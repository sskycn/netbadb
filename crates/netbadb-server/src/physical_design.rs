use std::error::Error;
use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver, Sender, SyncSender, TryRecvError};
use std::sync::{Arc, Weak};

use netbadb_core::{
    Database, DatabaseError, ExecutionFeedbackReport, PhysicalColumnarCandidate,
    PhysicalColumnarDesignApplyError, PhysicalColumnarDesignApplyOutcome,
    PhysicalColumnarDesignLocationState, PhysicalColumnarDesignMode,
    PhysicalColumnarDesignProposal, PhysicalColumnarDesignProposalError,
    PhysicalDesignAdvisorError, PhysicalDesignAdvisorPolicy, PhysicalDesignAdvisorReport,
    PhysicalDesignEvidenceEpoch, PhysicalDesignEvidenceLimits, PhysicalDesignEvidenceRecordError,
    PhysicalDesignEvidenceRecordOutcome, PhysicalDesignEvidenceSummary,
    PhysicalDesignEvidenceWindow, PhysicalDesignEvidenceWindowInspection,
    PhysicalDesignNoActionReason, PhysicalIndexCandidate, PhysicalIndexDesignApplyError,
    PhysicalIndexDesignApplyOutcome, PhysicalIndexDesignApplyReport, PhysicalIndexDesignNameState,
    PhysicalIndexDesignProposal, PhysicalIndexDesignProposalError,
};
use netbadb_schema::SchemaFingerprint;
use netbadb_types::{
    ChangeStreamGeneration, ColumnarProjectionId, DatabaseCommitSeq, IndexId, IndexName,
    SchemaGeneration, StorageId, TableSchemaVersion,
};

use crate::adaptive_driver::ServerAdaptiveHostConfig;
#[cfg(test)]
use crate::physical_design_receipts::TestJournalIoFailure;
use crate::physical_design_receipts::{
    MutationReceiptTarget, ServerPhysicalDesignMutationReceiptConfig,
    ServerPhysicalDesignMutationReceiptControlError, ServerPhysicalDesignMutationReceiptCursor,
    ServerPhysicalDesignMutationReceiptId, ServerPhysicalDesignMutationReceiptJournal,
    ServerPhysicalDesignMutationReceiptOutcome, ServerPhysicalDesignMutationReceiptPage,
    ServerPhysicalDesignMutationReceiptReference, ServerPhysicalDesignMutationReceiptScopedPage,
    ServerPhysicalDesignMutationReceiptStartupError, ServerPhysicalDesignMutationReceiptStatus,
    ServerPhysicalDesignMutationSource,
};

pub(crate) struct ServerHostObservationConfig {
    pub(crate) adaptive: ServerAdaptiveHostConfig,
    pub(crate) physical_design_controls: Receiver<ServerPhysicalDesignControlRequest>,
}

/// Explicit, programmatic-only configuration for Server physical-design advice.
///
/// This value intentionally has no `Default`: evidence bounds and recommendation
/// thresholds are separate operator choices and must both be visible at opt-in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServerPhysicalDesignAdvisorConfig {
    evidence_limits: PhysicalDesignEvidenceLimits,
    advisor_policy: PhysicalDesignAdvisorPolicy,
}

impl ServerPhysicalDesignAdvisorConfig {
    #[must_use]
    pub const fn new(
        evidence_limits: PhysicalDesignEvidenceLimits,
        advisor_policy: PhysicalDesignAdvisorPolicy,
    ) -> Self {
        Self {
            evidence_limits,
            advisor_policy,
        }
    }

    #[must_use]
    pub const fn evidence_limits(self) -> PhysicalDesignEvidenceLimits {
        self.evidence_limits
    }

    #[must_use]
    pub const fn advisor_policy(self) -> PhysicalDesignAdvisorPolicy {
        self.advisor_policy
    }
}

/// Programmatic host policy for placing managed Columnar projections.
///
/// The root is canonicalized during construction and must already exist. This
/// type deliberately has no `Default`: the host must approve at least one
/// exact maintenance mode and provision the namespace itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerPhysicalColumnarApplyConfig {
    root: PathBuf,
    allow_snapshot: bool,
    allow_incremental: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ServerPhysicalColumnarApplyCapabilities {
    pub(crate) allow_snapshot: bool,
    pub(crate) allow_incremental: bool,
}

impl ServerPhysicalColumnarApplyConfig {
    pub fn new(
        root: impl AsRef<Path>,
        allow_snapshot: bool,
        allow_incremental: bool,
    ) -> Result<Self, ServerPhysicalColumnarApplyConfigError> {
        if !allow_snapshot && !allow_incremental {
            return Err(ServerPhysicalColumnarApplyConfigError::NoModesEnabled);
        }
        let supplied = root.as_ref();
        let canonical = fs::canonicalize(supplied).map_err(|source| {
            ServerPhysicalColumnarApplyConfigError::RootResolution {
                path: supplied.to_path_buf(),
                source,
            }
        })?;
        if !canonical.is_dir() {
            return Err(ServerPhysicalColumnarApplyConfigError::RootNotDirectory {
                path: canonical,
            });
        }
        Ok(Self {
            root: canonical,
            allow_snapshot,
            allow_incremental,
        })
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    #[must_use]
    pub const fn allow_snapshot(&self) -> bool {
        self.allow_snapshot
    }

    #[must_use]
    pub const fn allow_incremental(&self) -> bool {
        self.allow_incremental
    }

    pub(crate) const fn capabilities(&self) -> ServerPhysicalColumnarApplyCapabilities {
        ServerPhysicalColumnarApplyCapabilities {
            allow_snapshot: self.allow_snapshot,
            allow_incremental: self.allow_incremental,
        }
    }

    #[must_use]
    pub const fn allows(&self, mode: PhysicalColumnarDesignMode) -> bool {
        match mode {
            PhysicalColumnarDesignMode::Snapshot => self.allow_snapshot,
            PhysicalColumnarDesignMode::Incremental => self.allow_incremental,
        }
    }

    fn resolve(
        &self,
        placement: &ServerPhysicalColumnarPlacementKey,
    ) -> Result<PathBuf, ServerPhysicalColumnarPlacementRootError> {
        self.revalidate()?;
        Ok(self.root.join(placement.as_str()))
    }

    pub(crate) fn revalidate(&self) -> Result<(), ServerPhysicalColumnarPlacementRootError> {
        let current = fs::canonicalize(&self.root).map_err(|source| {
            ServerPhysicalColumnarPlacementRootError::Resolution {
                path: self.root.clone(),
                source,
            }
        })?;
        if !current.is_dir() {
            return Err(ServerPhysicalColumnarPlacementRootError::NotDirectory { path: current });
        }
        if current != self.root {
            return Err(ServerPhysicalColumnarPlacementRootError::Changed {
                expected: self.root.clone(),
                actual: current,
            });
        }
        Ok(())
    }
}

#[derive(Debug)]
pub enum ServerPhysicalColumnarApplyConfigError {
    NoModesEnabled,
    RootResolution { path: PathBuf, source: io::Error },
    RootNotDirectory { path: PathBuf },
}

impl fmt::Display for ServerPhysicalColumnarApplyConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoModesEnabled => formatter
                .write_str("physical-columnar apply must allow Snapshot, Incremental, or both"),
            Self::RootResolution { path, source } => write!(
                formatter,
                "failed to resolve physical-columnar placement root {}: {source}",
                path.display()
            ),
            Self::RootNotDirectory { path } => write!(
                formatter,
                "physical-columnar placement root {} is not a directory",
                path.display()
            ),
        }
    }
}

impl Error for ServerPhysicalColumnarApplyConfigError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::RootResolution { source, .. } => Some(source),
            Self::NoModesEnabled | Self::RootNotDirectory { .. } => None,
        }
    }
}

#[derive(Debug)]
pub enum ServerPhysicalColumnarPlacementRootError {
    Resolution { path: PathBuf, source: io::Error },
    NotDirectory { path: PathBuf },
    Changed { expected: PathBuf, actual: PathBuf },
}

impl fmt::Display for ServerPhysicalColumnarPlacementRootError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Resolution { path, source } => write!(
                formatter,
                "physical-columnar placement root {} is unavailable: {source}",
                path.display()
            ),
            Self::NotDirectory { path } => write!(
                formatter,
                "physical-columnar placement root {} is no longer a directory",
                path.display()
            ),
            Self::Changed { expected, actual } => write!(
                formatter,
                "physical-columnar placement root changed from {} to {}",
                expected.display(),
                actual.display()
            ),
        }
    }
}

impl Error for ServerPhysicalColumnarPlacementRootError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Resolution { source, .. } => Some(source),
            Self::NotDirectory { .. } | Self::Changed { .. } => None,
        }
    }
}

#[derive(Debug)]
pub enum ServerPhysicalColumnarApplyStartupError {
    PhysicalDesignRequired,
    OperatorPolicyMismatch,
    PlacementRoot(ServerPhysicalColumnarPlacementRootError),
}

impl fmt::Display for ServerPhysicalColumnarApplyStartupError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PhysicalDesignRequired => formatter
                .write_str("physical-columnar apply requires a configured physical-design advisor"),
            Self::OperatorPolicyMismatch => formatter.write_str(
                "operator-authorized physical-columnar apply cannot use a different builder placement policy",
            ),
            Self::PlacementRoot(error) => error.fmt(formatter),
        }
    }
}

impl Error for ServerPhysicalColumnarApplyStartupError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::PhysicalDesignRequired => None,
            Self::OperatorPolicyMismatch => None,
            Self::PlacementRoot(error) => Some(error),
        }
    }
}

/// One validated logical child in the Server-owned Columnar namespace.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ServerPhysicalColumnarPlacementKey(String);

impl ServerPhysicalColumnarPlacementKey {
    pub fn new(value: impl Into<String>) -> Result<Self, ServerPhysicalColumnarPlacementKeyError> {
        let value = value.into();
        if value.is_empty() {
            return Err(ServerPhysicalColumnarPlacementKeyError::Empty);
        }
        if value.len() > 128 {
            return Err(ServerPhysicalColumnarPlacementKeyError::TooLong { bytes: value.len() });
        }
        for (index, byte) in value.bytes().enumerate() {
            let valid =
                byte.is_ascii_alphanumeric() || (index != 0 && matches!(byte, b'-' | b'_' | b'.'));
            if !valid {
                return Err(ServerPhysicalColumnarPlacementKeyError::InvalidCharacter {
                    byte_index: index,
                });
            }
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ServerPhysicalColumnarPlacementKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerPhysicalColumnarPlacementKeyError {
    Empty,
    TooLong { bytes: usize },
    InvalidCharacter { byte_index: usize },
}

impl fmt::Display for ServerPhysicalColumnarPlacementKeyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => formatter.write_str("physical-columnar placement key is empty"),
            Self::TooLong { bytes } => write!(
                formatter,
                "physical-columnar placement key is {bytes} bytes; maximum is 128"
            ),
            Self::InvalidCharacter { byte_index } => write!(
                formatter,
                "physical-columnar placement key has an invalid byte at offset {byte_index}"
            ),
        }
    }
}

impl Error for ServerPhysicalColumnarPlacementKeyError {}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ServerPhysicalDesignDiagnostics {
    pub eligible_query_count: u64,
    pub record_success_count: u64,
    pub record_error_count: u64,
    pub schema_rotation_count: u64,
    pub capacity_rejection_count: u64,
    pub incomplete_report_count: u64,
    pub counter_overflowed: bool,
    pub last_record_outcome: Option<PhysicalDesignEvidenceRecordOutcome>,
    pub last_record_error: Option<PhysicalDesignEvidenceRecordError>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServerPhysicalDesignStatus {
    pub diagnostics: ServerPhysicalDesignDiagnostics,
    pub evidence: PhysicalDesignEvidenceWindowInspection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServerPhysicalDesignRotationReport {
    pub previous_epoch: PhysicalDesignEvidenceEpoch,
    pub new_epoch: PhysicalDesignEvidenceEpoch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ServerApprovedPhysicalIndexApplyOutcome {
    Created { index_id: IndexId },
    AlreadyApplied { index_id: IndexId },
    AlreadyCovered,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ServerApprovedPhysicalColumnarApplyOutcome {
    Created { projection_id: ColumnarProjectionId },
    AlreadyApplied { projection_id: ColumnarProjectionId },
    AlreadyCovered,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ServerApprovedPhysicalColumnarApplyReport {
    pub(crate) candidate: PhysicalColumnarCandidate,
    pub(crate) mode: PhysicalColumnarDesignMode,
    pub(crate) placement: ServerPhysicalColumnarPlacementKey,
    pub(crate) outcome: ServerApprovedPhysicalColumnarApplyOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ServerApprovedPhysicalIndexApplyReport {
    pub(crate) candidate: PhysicalIndexCandidate,
    pub(crate) index_name: IndexName,
    pub(crate) outcome: ServerApprovedPhysicalIndexApplyOutcome,
}

#[derive(Debug)]
pub(crate) struct ServerPhysicalDesignMutationControlReply<T, E> {
    pub(crate) receipt: Option<ServerPhysicalDesignMutationReceiptReference>,
    pub(crate) result: Result<T, E>,
}

impl<T, E> ServerPhysicalDesignMutationControlReply<T, E> {
    fn without_receipt(result: Result<T, E>) -> Self {
        Self {
            receipt: None,
            result,
        }
    }
}

struct ServerPhysicalDesignRuntimeIdentity;

/// A runtime-local physical-index proposal produced by this Server worker.
///
/// The proposal does not keep the worker alive. Apply revalidates both this
/// runtime provenance and all durable/current-state invariants in Core.
#[derive(Clone)]
pub struct ServerPhysicalIndexDesignProposal {
    origin: Weak<ServerPhysicalDesignRuntimeIdentity>,
    proposal: PhysicalIndexDesignProposal,
}

impl ServerPhysicalIndexDesignProposal {
    #[must_use]
    pub const fn candidate(&self) -> PhysicalIndexCandidate {
        self.proposal.candidate()
    }

    #[must_use]
    pub const fn evidence_epoch(&self) -> PhysicalDesignEvidenceEpoch {
        self.proposal.evidence_epoch()
    }

    #[must_use]
    pub const fn evidence_schema_generation(&self) -> SchemaGeneration {
        self.proposal.evidence_schema_generation()
    }

    #[must_use]
    pub const fn first_global_commit_seq(&self) -> DatabaseCommitSeq {
        self.proposal.first_global_commit_seq()
    }

    #[must_use]
    pub const fn last_global_commit_seq(&self) -> DatabaseCommitSeq {
        self.proposal.last_global_commit_seq()
    }

    #[must_use]
    pub const fn proposed_at_global_commit_seq(&self) -> DatabaseCommitSeq {
        self.proposal.proposed_at_global_commit_seq()
    }

    #[must_use]
    pub const fn table_schema_version(&self) -> TableSchemaVersion {
        self.proposal.table_schema_version()
    }

    #[must_use]
    pub fn table_fingerprint(&self) -> &SchemaFingerprint {
        self.proposal.table_fingerprint()
    }

    #[must_use]
    pub const fn storage_id(&self) -> StorageId {
        self.proposal.storage_id()
    }

    #[must_use]
    pub const fn point_report_count(&self) -> u64 {
        self.proposal.point_report_count()
    }

    #[must_use]
    pub const fn range_report_count(&self) -> u64 {
        self.proposal.range_report_count()
    }

    #[must_use]
    pub const fn evidence(&self) -> PhysicalDesignEvidenceSummary {
        self.proposal.evidence()
    }
}

impl fmt::Debug for ServerPhysicalIndexDesignProposal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ServerPhysicalIndexDesignProposal")
            .field("candidate", &self.candidate())
            .field("evidence_epoch", &self.evidence_epoch())
            .field(
                "evidence_schema_generation",
                &self.evidence_schema_generation(),
            )
            .field("first_global_commit_seq", &self.first_global_commit_seq())
            .field("last_global_commit_seq", &self.last_global_commit_seq())
            .field(
                "proposed_at_global_commit_seq",
                &self.proposed_at_global_commit_seq(),
            )
            .field("table_schema_version", &self.table_schema_version())
            .field("table_fingerprint", self.table_fingerprint())
            .field("storage_id", &self.storage_id())
            .field("point_report_count", &self.point_report_count())
            .field("range_report_count", &self.range_report_count())
            .field("evidence", &self.evidence())
            .finish()
    }
}

/// A same-runtime Server approval for one Core Columnar proposal and logical
/// placement key. The weak origin deliberately cannot retain the Server.
#[derive(Clone)]
pub struct ServerPhysicalColumnarDesignProposal {
    origin: Weak<ServerPhysicalDesignRuntimeIdentity>,
    placement: ServerPhysicalColumnarPlacementKey,
    proposal: PhysicalColumnarDesignProposal,
}

impl ServerPhysicalColumnarDesignProposal {
    #[must_use]
    pub fn candidate(&self) -> &PhysicalColumnarCandidate {
        self.proposal.candidate()
    }

    #[must_use]
    pub const fn mode(&self) -> PhysicalColumnarDesignMode {
        self.proposal.mode()
    }

    #[must_use]
    pub fn placement(&self) -> &ServerPhysicalColumnarPlacementKey {
        &self.placement
    }

    #[must_use]
    pub const fn evidence_epoch(&self) -> PhysicalDesignEvidenceEpoch {
        self.proposal.evidence_epoch()
    }

    #[must_use]
    pub const fn evidence_schema_generation(&self) -> SchemaGeneration {
        self.proposal.evidence_schema_generation()
    }

    #[must_use]
    pub const fn first_global_commit_seq(&self) -> DatabaseCommitSeq {
        self.proposal.first_global_commit_seq()
    }

    #[must_use]
    pub const fn last_global_commit_seq(&self) -> DatabaseCommitSeq {
        self.proposal.last_global_commit_seq()
    }

    #[must_use]
    pub const fn proposed_at_global_commit_seq(&self) -> DatabaseCommitSeq {
        self.proposal.proposed_at_global_commit_seq()
    }

    #[must_use]
    pub const fn table_schema_version(&self) -> TableSchemaVersion {
        self.proposal.table_schema_version()
    }

    #[must_use]
    pub fn table_fingerprint(&self) -> &SchemaFingerprint {
        self.proposal.table_fingerprint()
    }

    #[must_use]
    pub const fn storage_id(&self) -> StorageId {
        self.proposal.storage_id()
    }

    #[must_use]
    pub const fn change_stream_generation(&self) -> Option<ChangeStreamGeneration> {
        self.proposal.change_stream_generation()
    }

    #[must_use]
    pub const fn evidence(&self) -> PhysicalDesignEvidenceSummary {
        self.proposal.evidence()
    }
}

impl fmt::Debug for ServerPhysicalColumnarDesignProposal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ServerPhysicalColumnarDesignProposal")
            .field("candidate", self.candidate())
            .field("mode", &self.mode())
            .field("placement", self.placement())
            .field("evidence_epoch", &self.evidence_epoch())
            .field(
                "evidence_schema_generation",
                &self.evidence_schema_generation(),
            )
            .field("first_global_commit_seq", &self.first_global_commit_seq())
            .field("last_global_commit_seq", &self.last_global_commit_seq())
            .field(
                "proposed_at_global_commit_seq",
                &self.proposed_at_global_commit_seq(),
            )
            .field("table_schema_version", &self.table_schema_version())
            .field("table_fingerprint", self.table_fingerprint())
            .field("storage_id", &self.storage_id())
            .field("change_stream_generation", &self.change_stream_generation())
            .field("evidence", &self.evidence())
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerPhysicalColumnarDesignApplyReport {
    pub candidate: PhysicalColumnarCandidate,
    pub mode: PhysicalColumnarDesignMode,
    pub placement: ServerPhysicalColumnarPlacementKey,
    pub evidence_epoch: PhysicalDesignEvidenceEpoch,
    pub global_commit_seq_before: DatabaseCommitSeq,
    pub global_commit_seq_after: DatabaseCommitSeq,
    pub schema_generation: SchemaGeneration,
    pub outcome: PhysicalColumnarDesignApplyOutcome,
}

#[derive(Debug)]
pub enum ServerPhysicalColumnarDesignControlError {
    PhysicalDesignNotEnabled,
    ColumnarApplyNotEnabled,
    ModeNotAllowed(PhysicalColumnarDesignMode),
    PlacementRootUnavailable(ServerPhysicalColumnarPlacementRootError),
    PlacementOccupied {
        placement: ServerPhysicalColumnarPlacementKey,
    },
    LocationConflict {
        placement: ServerPhysicalColumnarPlacementKey,
    },
    PlacementInspection {
        placement: ServerPhysicalColumnarPlacementKey,
        source: io::Error,
    },
    LocationInspection(DatabaseError),
    Proposal(Box<PhysicalColumnarDesignProposalError>),
    Apply(Box<PhysicalColumnarDesignApplyError>),
    MutationReceipt(ServerPhysicalDesignMutationReceiptControlError),
    MutationRecoveryRequired(ServerPhysicalDesignMutationReceiptControlError),
    EvidenceEpochChanged {
        expected: PhysicalDesignEvidenceEpoch,
        actual: PhysicalDesignEvidenceEpoch,
    },
    PhysicalDesignRuntimeChanged,
    ProposalRuntimeChanged,
    PlacementInvariantViolated,
    ServerStopped,
    MutationOutcomeUncertain,
    PostBeginMutationOutcomeUncertain(Box<ServerPhysicalColumnarDesignControlError>),
}

impl fmt::Display for ServerPhysicalColumnarDesignControlError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PhysicalDesignNotEnabled => {
                formatter.write_str("server physical-design advisor is not enabled")
            }
            Self::ColumnarApplyNotEnabled => {
                formatter.write_str("server physical-columnar apply is not enabled")
            }
            Self::ModeNotAllowed(mode) => write!(
                formatter,
                "physical-columnar mode {mode:?} is not allowed by this server"
            ),
            Self::PlacementRootUnavailable(error) => error.fmt(formatter),
            Self::PlacementOccupied { placement } => write!(
                formatter,
                "physical-columnar placement `{placement}` is occupied by an unregistered filesystem object"
            ),
            Self::LocationConflict { placement } => write!(
                formatter,
                "physical-columnar placement `{placement}` conflicts with a registered projection"
            ),
            Self::PlacementInspection { placement, source } => write!(
                formatter,
                "failed to inspect physical-columnar placement `{placement}`: {source}"
            ),
            Self::LocationInspection(error) => error.fmt(formatter),
            Self::Proposal(error) => error.fmt(formatter),
            Self::Apply(error) => error.fmt(formatter),
            Self::MutationReceipt(error) => error.fmt(formatter),
            Self::MutationRecoveryRequired(error) => error.fmt(formatter),
            Self::EvidenceEpochChanged { expected, actual } => write!(
                formatter,
                "physical-design evidence epoch changed from expected {} to {}",
                expected.0, actual.0
            ),
            Self::PhysicalDesignRuntimeChanged => formatter
                .write_str("physical-columnar approval belongs to another operator/design runtime"),
            Self::ProposalRuntimeChanged => formatter.write_str(
                "physical-columnar proposal belongs to another server physical-design runtime",
            ),
            Self::PlacementInvariantViolated => formatter.write_str(
                "physical-columnar proposal directory does not match the approved placement",
            ),
            Self::ServerStopped => {
                formatter.write_str("server physical-columnar control is stopped")
            }
            Self::MutationOutcomeUncertain => formatter.write_str(
                "server physical-columnar control stopped after accepting the mutation command",
            ),
            Self::PostBeginMutationOutcomeUncertain(_) => formatter.write_str(
                "physical-columnar mutation outcome is uncertain after its durable receipt Begin",
            ),
        }
    }
}

impl Error for ServerPhysicalColumnarDesignControlError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::PlacementRootUnavailable(error) => Some(error),
            Self::PlacementInspection { source, .. } => Some(source),
            Self::LocationInspection(error) => Some(error),
            Self::Proposal(error) => Some(error.as_ref()),
            Self::Apply(error) => Some(error.as_ref()),
            Self::MutationReceipt(error) => Some(error),
            Self::MutationRecoveryRequired(error) => Some(error),
            Self::PostBeginMutationOutcomeUncertain(source) => Some(source.as_ref()),
            Self::PhysicalDesignNotEnabled
            | Self::ColumnarApplyNotEnabled
            | Self::ModeNotAllowed(_)
            | Self::PlacementOccupied { .. }
            | Self::LocationConflict { .. }
            | Self::EvidenceEpochChanged { .. }
            | Self::PhysicalDesignRuntimeChanged
            | Self::ProposalRuntimeChanged
            | Self::PlacementInvariantViolated
            | Self::ServerStopped
            | Self::MutationOutcomeUncertain => None,
        }
    }
}

#[derive(Debug)]
pub enum ServerPhysicalDesignControlError {
    PhysicalDesignNotEnabled,
    EvidenceEpochChanged {
        expected: PhysicalDesignEvidenceEpoch,
        actual: PhysicalDesignEvidenceEpoch,
    },
    EvidenceRotation(PhysicalDesignEvidenceRecordError),
    Advisor(PhysicalDesignAdvisorError),
    Proposal(Box<PhysicalIndexDesignProposalError>),
    Apply(Box<PhysicalIndexDesignApplyError>),
    MutationReceipt(ServerPhysicalDesignMutationReceiptControlError),
    MutationRecoveryRequired(ServerPhysicalDesignMutationReceiptControlError),
    ProposalRuntimeChanged,
    PhysicalDesignRuntimeChanged,
    PhysicalIndexNameConflict(IndexName),
    ServerStopped,
    MutationOutcomeUncertain,
    PostBeginMutationOutcomeUncertain(Box<ServerPhysicalDesignControlError>),
}

impl fmt::Display for ServerPhysicalDesignControlError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PhysicalDesignNotEnabled => {
                formatter.write_str("server physical-design advisor is not enabled")
            }
            Self::EvidenceEpochChanged { expected, actual } => write!(
                formatter,
                "physical-design evidence epoch changed from expected {} to {}",
                expected.0, actual.0
            ),
            Self::EvidenceRotation(error) => error.fmt(formatter),
            Self::Advisor(error) => error.fmt(formatter),
            Self::Proposal(error) => error.fmt(formatter),
            Self::Apply(error) => error.fmt(formatter),
            Self::MutationReceipt(error) => error.fmt(formatter),
            Self::MutationRecoveryRequired(error) => error.fmt(formatter),
            Self::ProposalRuntimeChanged => formatter.write_str(
                "physical-index proposal belongs to another server physical-design runtime",
            ),
            Self::PhysicalDesignRuntimeChanged => formatter
                .write_str("physical-index approval belongs to another operator/design runtime"),
            Self::PhysicalIndexNameConflict(name) => {
                write!(formatter, "physical-index name `{name}` is already in use")
            }
            Self::ServerStopped => formatter.write_str("server physical-design control is stopped"),
            Self::MutationOutcomeUncertain => formatter.write_str(
                "server physical-design control stopped after accepting the mutation command",
            ),
            Self::PostBeginMutationOutcomeUncertain(_) => formatter.write_str(
                "physical-index mutation outcome is uncertain after its durable receipt Begin",
            ),
        }
    }
}

impl Error for ServerPhysicalDesignControlError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::EvidenceRotation(error) => Some(error),
            Self::Advisor(error) => Some(error),
            Self::Proposal(error) => Some(error),
            Self::Apply(error) => Some(error),
            Self::MutationReceipt(error) => Some(error),
            Self::MutationRecoveryRequired(error) => Some(error),
            Self::PostBeginMutationOutcomeUncertain(source) => Some(source.as_ref()),
            Self::PhysicalDesignNotEnabled
            | Self::EvidenceEpochChanged { .. }
            | Self::ProposalRuntimeChanged
            | Self::PhysicalDesignRuntimeChanged
            | Self::PhysicalIndexNameConflict(_)
            | Self::ServerStopped
            | Self::MutationOutcomeUncertain => None,
        }
    }
}

/// Synchronous controls share one outstanding request across all handle clones.
/// Concurrent callers wait before allocating a reply or copying a proposal;
/// the permit is released after the reply or a stopped-worker error.
#[derive(Clone)]
pub struct ServerPhysicalDesignControlHandle {
    requests: Sender<ServerPhysicalDesignControlRequest>,
    admission: crate::control_admission::ControlAdmission,
}

impl ServerPhysicalDesignControlHandle {
    pub(crate) fn new(requests: Sender<ServerPhysicalDesignControlRequest>) -> Self {
        Self {
            requests,
            admission: crate::control_admission::ControlAdmission::default(),
        }
    }

    pub fn status(&self) -> Result<ServerPhysicalDesignStatus, ServerPhysicalDesignControlError> {
        let _permit = self.admission.enter();
        let (reply, response) = mpsc::sync_channel(1);
        self.requests
            .send(ServerPhysicalDesignControlRequest::Status { reply })
            .map_err(|_| ServerPhysicalDesignControlError::ServerStopped)?;
        response
            .recv()
            .map_err(|_| ServerPhysicalDesignControlError::ServerStopped)?
    }

    /// Reads one bounded page in the current journal's local receipt-ID order.
    /// Persisted cursors should use [`Self::mutation_receipts_scoped`] so a
    /// replacement journal cannot silently reinterpret a numeric receipt ID.
    pub fn mutation_receipts(
        &self,
        after: Option<ServerPhysicalDesignMutationReceiptId>,
        limit: u32,
    ) -> Result<
        ServerPhysicalDesignMutationReceiptPage,
        ServerPhysicalDesignMutationReceiptControlError,
    > {
        let _permit = self.admission.enter();
        let (reply, response) = mpsc::sync_channel(1);
        self.requests
            .send(ServerPhysicalDesignControlRequest::MutationReceipts {
                after,
                limit,
                reply,
            })
            .map_err(|_| ServerPhysicalDesignMutationReceiptControlError::ServerStopped)?;
        response
            .recv()
            .map_err(|_| ServerPhysicalDesignMutationReceiptControlError::ServerStopped)?
    }

    /// Reads one bounded page under an explicit durable journal namespace.
    pub fn mutation_receipts_scoped(
        &self,
        after: Option<ServerPhysicalDesignMutationReceiptCursor>,
        limit: u32,
    ) -> Result<
        ServerPhysicalDesignMutationReceiptScopedPage,
        ServerPhysicalDesignMutationReceiptControlError,
    > {
        let _permit = self.admission.enter();
        let (reply, response) = mpsc::sync_channel(1);
        self.requests
            .send(ServerPhysicalDesignControlRequest::MutationReceiptsScoped {
                after,
                limit,
                reply,
            })
            .map_err(|_| ServerPhysicalDesignMutationReceiptControlError::ServerStopped)?;
        response
            .recv()
            .map_err(|_| ServerPhysicalDesignMutationReceiptControlError::ServerStopped)?
    }

    /// Reports the current durable receipt namespace and bounded local state.
    pub fn mutation_receipt_status(
        &self,
    ) -> Result<
        ServerPhysicalDesignMutationReceiptStatus,
        ServerPhysicalDesignMutationReceiptControlError,
    > {
        let _permit = self.admission.enter();
        let (reply, response) = mpsc::sync_channel(1);
        self.requests
            .send(ServerPhysicalDesignControlRequest::MutationReceiptStatus { reply })
            .map_err(|_| ServerPhysicalDesignMutationReceiptControlError::ServerStopped)?;
        response
            .recv()
            .map_err(|_| ServerPhysicalDesignMutationReceiptControlError::ServerStopped)?
    }

    pub fn recommendations(
        &self,
    ) -> Result<PhysicalDesignAdvisorReport, ServerPhysicalDesignControlError> {
        let _permit = self.admission.enter();
        let (reply, response) = mpsc::sync_channel(1);
        self.requests
            .send(ServerPhysicalDesignControlRequest::Recommendations { reply })
            .map_err(|_| ServerPhysicalDesignControlError::ServerStopped)?;
        response
            .recv()
            .map_err(|_| ServerPhysicalDesignControlError::ServerStopped)?
    }

    /// Proposes one exact current index candidate without mutating Database or
    /// evidence state.
    pub fn propose_index(
        &self,
        candidate: PhysicalIndexCandidate,
    ) -> Result<ServerPhysicalIndexDesignProposal, ServerPhysicalDesignControlError> {
        let _permit = self.admission.enter();
        let (reply, response) = mpsc::sync_channel(1);
        self.requests
            .send(ServerPhysicalDesignControlRequest::ProposeIndex { candidate, reply })
            .map_err(|_| ServerPhysicalDesignControlError::ServerStopped)?;
        response
            .recv()
            .map_err(|_| ServerPhysicalDesignControlError::ServerStopped)?
    }

    /// Applies a proposal from this exact worker runtime through Core's named
    /// index transaction. The caller retains its proposal for typed retries.
    pub fn apply_index(
        &self,
        proposal: &ServerPhysicalIndexDesignProposal,
        index_name: IndexName,
    ) -> Result<PhysicalIndexDesignApplyReport, ServerPhysicalDesignControlError> {
        let _permit = self.admission.enter();
        let (reply, response) = mpsc::sync_channel(1);
        self.requests
            .send(ServerPhysicalDesignControlRequest::ApplyIndex {
                proposal: Box::new(proposal.clone()),
                index_name,
                reply,
            })
            .map_err(|_| ServerPhysicalDesignControlError::ServerStopped)?;
        response
            .recv()
            .map_err(|_| ServerPhysicalDesignControlError::MutationOutcomeUncertain)?
    }

    /// Proposes one current Columnar candidate at one host-approved logical
    /// placement. Proposal creation never creates or reserves the directory.
    pub fn propose_columnar(
        &self,
        candidate: PhysicalColumnarCandidate,
        mode: PhysicalColumnarDesignMode,
        placement: ServerPhysicalColumnarPlacementKey,
    ) -> Result<ServerPhysicalColumnarDesignProposal, ServerPhysicalColumnarDesignControlError>
    {
        let _permit = self.admission.enter();
        let (reply, response) = mpsc::sync_channel(1);
        self.requests
            .send(ServerPhysicalDesignControlRequest::ProposeColumnar {
                candidate,
                mode,
                placement,
                reply,
            })
            .map_err(|_| ServerPhysicalColumnarDesignControlError::ServerStopped)?;
        response
            .recv()
            .map_err(|_| ServerPhysicalColumnarDesignControlError::ServerStopped)?
    }

    /// Applies a proposal from this exact worker runtime through Core's Phase
    /// 27 authority. The caller retains its proposal for typed retries.
    pub fn apply_columnar(
        &self,
        proposal: &ServerPhysicalColumnarDesignProposal,
    ) -> Result<ServerPhysicalColumnarDesignApplyReport, ServerPhysicalColumnarDesignControlError>
    {
        let _permit = self.admission.enter();
        let (reply, response) = mpsc::sync_channel(1);
        self.requests
            .send(ServerPhysicalDesignControlRequest::ApplyColumnar {
                proposal: Box::new(proposal.clone()),
                reply,
            })
            .map_err(|_| ServerPhysicalColumnarDesignControlError::ServerStopped)?;
        response
            .recv()
            .map_err(|_| ServerPhysicalColumnarDesignControlError::MutationOutcomeUncertain)?
    }

    /// Forwards one externally approved logical candidate as one atomic worker
    /// command. The listener has already checked deployment permission and the
    /// current opaque runtime token; Core still owns all database decisions.
    pub(crate) fn apply_approved_index(
        &self,
        runtime_token_matches: bool,
        expected_evidence_epoch: PhysicalDesignEvidenceEpoch,
        candidate: PhysicalIndexCandidate,
        index_name: IndexName,
    ) -> ServerPhysicalDesignMutationControlReply<
        ServerApprovedPhysicalIndexApplyReport,
        ServerPhysicalDesignControlError,
    > {
        let _permit = self.admission.enter();
        let (reply, response) = mpsc::sync_channel(1);
        if self
            .requests
            .send(ServerPhysicalDesignControlRequest::ApplyApprovedIndex {
                runtime_token_matches,
                expected_evidence_epoch,
                candidate,
                index_name,
                reply,
            })
            .is_err()
        {
            return ServerPhysicalDesignMutationControlReply::without_receipt(Err(
                ServerPhysicalDesignControlError::ServerStopped,
            ));
        }
        response.recv().unwrap_or_else(|_| {
            ServerPhysicalDesignMutationControlReply::without_receipt(Err(
                ServerPhysicalDesignControlError::MutationOutcomeUncertain,
            ))
        })
    }

    pub(crate) fn apply_approved_columnar(
        &self,
        runtime_token_matches: bool,
        expected_evidence_epoch: PhysicalDesignEvidenceEpoch,
        candidate: PhysicalColumnarCandidate,
        mode: PhysicalColumnarDesignMode,
        placement: ServerPhysicalColumnarPlacementKey,
    ) -> ServerPhysicalDesignMutationControlReply<
        ServerApprovedPhysicalColumnarApplyReport,
        ServerPhysicalColumnarDesignControlError,
    > {
        let _permit = self.admission.enter();
        let (reply, response) = mpsc::sync_channel(1);
        if self
            .requests
            .send(ServerPhysicalDesignControlRequest::ApplyApprovedColumnar {
                runtime_token_matches,
                expected_evidence_epoch,
                candidate,
                mode,
                placement,
                reply,
            })
            .is_err()
        {
            return ServerPhysicalDesignMutationControlReply::without_receipt(Err(
                ServerPhysicalColumnarDesignControlError::ServerStopped,
            ));
        }
        response.recv().unwrap_or_else(|_| {
            ServerPhysicalDesignMutationControlReply::without_receipt(Err(
                ServerPhysicalColumnarDesignControlError::MutationOutcomeUncertain,
            ))
        })
    }

    /// Compares and rotates as one command in the sole Database worker.
    pub fn rotate_evidence_if_epoch(
        &self,
        expected: PhysicalDesignEvidenceEpoch,
    ) -> Result<ServerPhysicalDesignRotationReport, ServerPhysicalDesignControlError> {
        let _permit = self.admission.enter();
        let (reply, response) = mpsc::sync_channel(1);
        self.requests
            .send(ServerPhysicalDesignControlRequest::RotateEvidenceIfEpoch { expected, reply })
            .map_err(|_| ServerPhysicalDesignControlError::ServerStopped)?;
        response
            .recv()
            .map_err(|_| ServerPhysicalDesignControlError::ServerStopped)?
    }
}

pub(crate) enum ServerPhysicalDesignControlRequest {
    Status {
        reply: SyncSender<Result<ServerPhysicalDesignStatus, ServerPhysicalDesignControlError>>,
    },
    MutationReceipts {
        after: Option<ServerPhysicalDesignMutationReceiptId>,
        limit: u32,
        reply: SyncSender<
            Result<
                ServerPhysicalDesignMutationReceiptPage,
                ServerPhysicalDesignMutationReceiptControlError,
            >,
        >,
    },
    MutationReceiptsScoped {
        after: Option<ServerPhysicalDesignMutationReceiptCursor>,
        limit: u32,
        reply: SyncSender<
            Result<
                ServerPhysicalDesignMutationReceiptScopedPage,
                ServerPhysicalDesignMutationReceiptControlError,
            >,
        >,
    },
    MutationReceiptStatus {
        reply: SyncSender<
            Result<
                ServerPhysicalDesignMutationReceiptStatus,
                ServerPhysicalDesignMutationReceiptControlError,
            >,
        >,
    },
    Recommendations {
        reply: SyncSender<Result<PhysicalDesignAdvisorReport, ServerPhysicalDesignControlError>>,
    },
    ProposeIndex {
        candidate: PhysicalIndexCandidate,
        reply:
            SyncSender<Result<ServerPhysicalIndexDesignProposal, ServerPhysicalDesignControlError>>,
    },
    ApplyIndex {
        proposal: Box<ServerPhysicalIndexDesignProposal>,
        index_name: IndexName,
        reply: SyncSender<Result<PhysicalIndexDesignApplyReport, ServerPhysicalDesignControlError>>,
    },
    ProposeColumnar {
        candidate: PhysicalColumnarCandidate,
        mode: PhysicalColumnarDesignMode,
        placement: ServerPhysicalColumnarPlacementKey,
        reply: SyncSender<
            Result<ServerPhysicalColumnarDesignProposal, ServerPhysicalColumnarDesignControlError>,
        >,
    },
    ApplyColumnar {
        proposal: Box<ServerPhysicalColumnarDesignProposal>,
        reply: SyncSender<
            Result<
                ServerPhysicalColumnarDesignApplyReport,
                ServerPhysicalColumnarDesignControlError,
            >,
        >,
    },
    ApplyApprovedIndex {
        runtime_token_matches: bool,
        expected_evidence_epoch: PhysicalDesignEvidenceEpoch,
        candidate: PhysicalIndexCandidate,
        index_name: IndexName,
        reply: SyncSender<
            ServerPhysicalDesignMutationControlReply<
                ServerApprovedPhysicalIndexApplyReport,
                ServerPhysicalDesignControlError,
            >,
        >,
    },
    ApplyApprovedColumnar {
        runtime_token_matches: bool,
        expected_evidence_epoch: PhysicalDesignEvidenceEpoch,
        candidate: PhysicalColumnarCandidate,
        mode: PhysicalColumnarDesignMode,
        placement: ServerPhysicalColumnarPlacementKey,
        reply: SyncSender<
            ServerPhysicalDesignMutationControlReply<
                ServerApprovedPhysicalColumnarApplyReport,
                ServerPhysicalColumnarDesignControlError,
            >,
        >,
    },
    RotateEvidenceIfEpoch {
        expected: PhysicalDesignEvidenceEpoch,
        reply: SyncSender<
            Result<ServerPhysicalDesignRotationReport, ServerPhysicalDesignControlError>,
        >,
    },
}

pub(crate) enum ServerPhysicalDesignWorkerCommand {
    Status {
        reply: SyncSender<Result<ServerPhysicalDesignStatus, ServerPhysicalDesignControlError>>,
    },
    MutationReceipts {
        after: Option<ServerPhysicalDesignMutationReceiptId>,
        limit: u32,
        reply: SyncSender<
            Result<
                ServerPhysicalDesignMutationReceiptPage,
                ServerPhysicalDesignMutationReceiptControlError,
            >,
        >,
    },
    MutationReceiptsScoped {
        after: Option<ServerPhysicalDesignMutationReceiptCursor>,
        limit: u32,
        reply: SyncSender<
            Result<
                ServerPhysicalDesignMutationReceiptScopedPage,
                ServerPhysicalDesignMutationReceiptControlError,
            >,
        >,
    },
    MutationReceiptStatus {
        reply: SyncSender<
            Result<
                ServerPhysicalDesignMutationReceiptStatus,
                ServerPhysicalDesignMutationReceiptControlError,
            >,
        >,
    },
    Recommendations {
        reply: SyncSender<Result<PhysicalDesignAdvisorReport, ServerPhysicalDesignControlError>>,
    },
    ProposeIndex {
        candidate: PhysicalIndexCandidate,
        reply:
            SyncSender<Result<ServerPhysicalIndexDesignProposal, ServerPhysicalDesignControlError>>,
    },
    ApplyIndex {
        proposal: Box<ServerPhysicalIndexDesignProposal>,
        index_name: IndexName,
        reply: SyncSender<Result<PhysicalIndexDesignApplyReport, ServerPhysicalDesignControlError>>,
    },
    ProposeColumnar {
        candidate: PhysicalColumnarCandidate,
        mode: PhysicalColumnarDesignMode,
        placement: ServerPhysicalColumnarPlacementKey,
        reply: SyncSender<
            Result<ServerPhysicalColumnarDesignProposal, ServerPhysicalColumnarDesignControlError>,
        >,
    },
    ApplyColumnar {
        proposal: Box<ServerPhysicalColumnarDesignProposal>,
        reply: SyncSender<
            Result<
                ServerPhysicalColumnarDesignApplyReport,
                ServerPhysicalColumnarDesignControlError,
            >,
        >,
    },
    ApplyApprovedIndex {
        runtime_token_matches: bool,
        expected_evidence_epoch: PhysicalDesignEvidenceEpoch,
        candidate: PhysicalIndexCandidate,
        index_name: IndexName,
        reply: SyncSender<
            ServerPhysicalDesignMutationControlReply<
                ServerApprovedPhysicalIndexApplyReport,
                ServerPhysicalDesignControlError,
            >,
        >,
    },
    ApplyApprovedColumnar {
        runtime_token_matches: bool,
        expected_evidence_epoch: PhysicalDesignEvidenceEpoch,
        candidate: PhysicalColumnarCandidate,
        mode: PhysicalColumnarDesignMode,
        placement: ServerPhysicalColumnarPlacementKey,
        reply: SyncSender<
            ServerPhysicalDesignMutationControlReply<
                ServerApprovedPhysicalColumnarApplyReport,
                ServerPhysicalColumnarDesignControlError,
            >,
        >,
    },
    RotateEvidenceIfEpoch {
        expected: PhysicalDesignEvidenceEpoch,
        reply: SyncSender<
            Result<ServerPhysicalDesignRotationReport, ServerPhysicalDesignControlError>,
        >,
    },
}

/// Independent runtime owned beside, never inside, the adaptive runtime.
pub(crate) struct ServerPhysicalDesignRuntime {
    identity: Arc<ServerPhysicalDesignRuntimeIdentity>,
    evidence: PhysicalDesignEvidenceWindow,
    policy: PhysicalDesignAdvisorPolicy,
    columnar_apply: Option<ServerPhysicalColumnarApplyConfig>,
    mutation_receipts: Option<ServerPhysicalDesignMutationReceiptJournal>,
    diagnostics: ServerPhysicalDesignDiagnostics,
}

impl ServerPhysicalDesignRuntime {
    #[cfg(test)]
    pub(crate) fn new(config: ServerPhysicalDesignAdvisorConfig) -> Self {
        Self::new_with_columnar(config, None)
    }

    pub(crate) fn new_with_columnar(
        config: ServerPhysicalDesignAdvisorConfig,
        columnar_apply: Option<ServerPhysicalColumnarApplyConfig>,
    ) -> Self {
        Self {
            identity: Arc::new(ServerPhysicalDesignRuntimeIdentity),
            evidence: PhysicalDesignEvidenceWindow::new(config.evidence_limits()),
            policy: config.advisor_policy(),
            columnar_apply,
            mutation_receipts: None,
            diagnostics: ServerPhysicalDesignDiagnostics {
                eligible_query_count: 0,
                record_success_count: 0,
                record_error_count: 0,
                schema_rotation_count: 0,
                capacity_rejection_count: 0,
                incomplete_report_count: 0,
                counter_overflowed: false,
                last_record_outcome: None,
                last_record_error: None,
            },
        }
    }

    pub(crate) fn new_with_mutation_receipts(
        config: ServerPhysicalDesignAdvisorConfig,
        columnar_apply: Option<ServerPhysicalColumnarApplyConfig>,
        mutation_receipts: Option<ServerPhysicalDesignMutationReceiptConfig>,
        database: &Database,
    ) -> Result<Self, ServerPhysicalDesignMutationReceiptStartupError> {
        let mutation_receipts = if let Some(receipt_config) = mutation_receipts {
            let identity = database
                .physical_design_database_identity()
                .map_err(ServerPhysicalDesignMutationReceiptStartupError::DatabaseIdentity)?;
            Some(
                ServerPhysicalDesignMutationReceiptJournal::open(
                    receipt_config,
                    identity,
                    database,
                )
                .map_err(ServerPhysicalDesignMutationReceiptStartupError::Journal)?,
            )
        } else {
            None
        };
        let mut runtime = Self::new_with_columnar(config, columnar_apply);
        runtime.mutation_receipts = mutation_receipts;
        Ok(runtime)
    }

    #[cfg(test)]
    fn fail_next_receipt_begin_before_write(&mut self) {
        self.mutation_receipts
            .as_mut()
            .expect("test runtime has mutation receipts")
            .fail_next_begin_before_write();
    }

    #[cfg(test)]
    fn fail_next_receipt_outcome_before_write(&mut self) {
        self.mutation_receipts
            .as_mut()
            .expect("test runtime has mutation receipts")
            .fail_next_outcome_before_write();
    }

    #[cfg(test)]
    fn fail_receipt_io_at(&mut self, failure: TestJournalIoFailure) {
        self.mutation_receipts
            .as_mut()
            .expect("test runtime has mutation receipts")
            .fail_io_at(failure);
    }

    pub(crate) fn status(&self) -> ServerPhysicalDesignStatus {
        ServerPhysicalDesignStatus {
            diagnostics: self.diagnostics,
            evidence: self.evidence.inspection(),
        }
    }

    fn increment(&mut self, counter: fn(&mut ServerPhysicalDesignDiagnostics) -> &mut u64) {
        let value = counter(&mut self.diagnostics);
        match value.checked_add(1) {
            Some(next) => *value = next,
            None => self.diagnostics.counter_overflowed = true,
        }
    }

    pub(crate) fn record_successful_query(&mut self, report: &ExecutionFeedbackReport) {
        self.increment(|diagnostics| &mut diagnostics.eligible_query_count);
        if report.incomplete || report.overflowed {
            self.increment(|diagnostics| &mut diagnostics.incomplete_report_count);
        }
        match self.evidence.record_execution_feedback(report) {
            Ok(outcome) => {
                self.increment(|diagnostics| &mut diagnostics.record_success_count);
                self.diagnostics.last_record_outcome = Some(outcome);
                self.diagnostics.last_record_error = None;
                if matches!(
                    outcome,
                    PhysicalDesignEvidenceRecordOutcome::SchemaRotated
                        | PhysicalDesignEvidenceRecordOutcome::SchemaRotatedWithCapacityRejection
                ) {
                    self.increment(|diagnostics| &mut diagnostics.schema_rotation_count);
                }
                if matches!(
                    outcome,
                    PhysicalDesignEvidenceRecordOutcome::RecordedWithCapacityRejection
                        | PhysicalDesignEvidenceRecordOutcome::SchemaRotatedWithCapacityRejection
                ) {
                    self.increment(|diagnostics| &mut diagnostics.capacity_rejection_count);
                }
            }
            Err(error) => {
                self.increment(|diagnostics| &mut diagnostics.record_error_count);
                self.diagnostics.last_record_outcome = None;
                self.diagnostics.last_record_error = Some(error);
            }
        }
    }

    pub(crate) fn record_missing_report(&mut self) {
        self.increment(|diagnostics| &mut diagnostics.record_error_count);
    }

    pub(crate) fn handle(
        &mut self,
        database: &mut Database,
        command: ServerPhysicalDesignWorkerCommand,
    ) {
        match command {
            ServerPhysicalDesignWorkerCommand::Status { reply } => {
                let _ = reply.send(Ok(self.status()));
            }
            ServerPhysicalDesignWorkerCommand::MutationReceipts {
                after,
                limit,
                reply,
            } => {
                let result = self
                    .mutation_receipts
                    .as_ref()
                    .ok_or(ServerPhysicalDesignMutationReceiptControlError::NotEnabled)
                    .and_then(|journal| journal.page(after, limit));
                let _ = reply.send(result);
            }
            ServerPhysicalDesignWorkerCommand::MutationReceiptsScoped {
                after,
                limit,
                reply,
            } => {
                let result = self
                    .mutation_receipts
                    .as_ref()
                    .ok_or(ServerPhysicalDesignMutationReceiptControlError::NotEnabled)
                    .and_then(|journal| journal.scoped_page(after, limit));
                let _ = reply.send(result);
            }
            ServerPhysicalDesignWorkerCommand::MutationReceiptStatus { reply } => {
                let result = self
                    .mutation_receipts
                    .as_ref()
                    .ok_or(ServerPhysicalDesignMutationReceiptControlError::NotEnabled)
                    .map(ServerPhysicalDesignMutationReceiptJournal::status);
                let _ = reply.send(result);
            }
            ServerPhysicalDesignWorkerCommand::Recommendations { reply } => {
                let result = database
                    .advise_physical_design(&self.evidence, self.policy)
                    .map_err(ServerPhysicalDesignControlError::Advisor);
                let _ = reply.send(result);
            }
            ServerPhysicalDesignWorkerCommand::ProposeIndex { candidate, reply } => {
                let result = database
                    .propose_physical_index_design(&self.evidence, self.policy, candidate)
                    .map(|proposal| ServerPhysicalIndexDesignProposal {
                        origin: Arc::downgrade(&self.identity),
                        proposal,
                    })
                    .map_err(|error| ServerPhysicalDesignControlError::Proposal(Box::new(error)));
                let _ = reply.send(result);
            }
            ServerPhysicalDesignWorkerCommand::ApplyIndex {
                proposal,
                index_name,
                reply,
            } => {
                let receipt_id = self.begin_index_receipt(
                    ServerPhysicalDesignMutationSource::Programmatic,
                    proposal.evidence_epoch(),
                    proposal.candidate(),
                    index_name.clone(),
                );
                let result = receipt_id.and_then(|receipt_id| {
                    let result = match proposal.origin.upgrade() {
                        Some(origin) if Arc::ptr_eq(&origin, &self.identity) => database
                            .apply_physical_index_design(
                                &self.evidence,
                                &proposal.proposal,
                                index_name,
                            )
                            .map_err(|error| {
                                ServerPhysicalDesignControlError::Apply(Box::new(error))
                            }),
                        Some(_) | None => {
                            Err(ServerPhysicalDesignControlError::ProposalRuntimeChanged)
                        }
                    };
                    self.finish_index_result(receipt_id, result, |report| match report.outcome {
                        PhysicalIndexDesignApplyOutcome::Created { index_id } => {
                            ServerPhysicalDesignMutationReceiptOutcome::CreatedIndex { index_id }
                        }
                        PhysicalIndexDesignApplyOutcome::AlreadyApplied { index_id } => {
                            ServerPhysicalDesignMutationReceiptOutcome::AlreadyAppliedIndex {
                                index_id,
                            }
                        }
                        PhysicalIndexDesignApplyOutcome::AlreadyCovered => {
                            ServerPhysicalDesignMutationReceiptOutcome::AlreadyCovered
                        }
                    })
                });
                let _ = reply.send(result);
            }
            ServerPhysicalDesignWorkerCommand::ProposeColumnar {
                candidate,
                mode,
                placement,
                reply,
            } => {
                let result = self.propose_columnar(database, candidate, mode, placement);
                let _ = reply.send(result);
            }
            ServerPhysicalDesignWorkerCommand::ApplyColumnar { proposal, reply } => {
                let result = self.apply_columnar(database, &proposal);
                let _ = reply.send(result);
            }
            ServerPhysicalDesignWorkerCommand::ApplyApprovedIndex {
                runtime_token_matches,
                expected_evidence_epoch,
                candidate,
                index_name,
                reply,
            } => {
                let result = self.apply_approved_index(
                    database,
                    runtime_token_matches,
                    expected_evidence_epoch,
                    candidate,
                    index_name,
                );
                let _ = reply.send(result);
            }
            ServerPhysicalDesignWorkerCommand::ApplyApprovedColumnar {
                runtime_token_matches,
                expected_evidence_epoch,
                candidate,
                mode,
                placement,
                reply,
            } => {
                let result = self.apply_approved_columnar(
                    database,
                    runtime_token_matches,
                    expected_evidence_epoch,
                    candidate,
                    mode,
                    placement,
                );
                let _ = reply.send(result);
            }
            ServerPhysicalDesignWorkerCommand::RotateEvidenceIfEpoch { expected, reply } => {
                let actual = self.evidence.epoch();
                let result = if actual == expected {
                    self.evidence
                        .rotate_window()
                        .map(|new_epoch| ServerPhysicalDesignRotationReport {
                            previous_epoch: actual,
                            new_epoch,
                        })
                        .map_err(ServerPhysicalDesignControlError::EvidenceRotation)
                } else {
                    Err(ServerPhysicalDesignControlError::EvidenceEpochChanged { expected, actual })
                };
                let _ = reply.send(result);
            }
        }
    }

    fn begin_index_receipt(
        &mut self,
        source: ServerPhysicalDesignMutationSource,
        evidence_epoch: PhysicalDesignEvidenceEpoch,
        candidate: PhysicalIndexCandidate,
        index_name: IndexName,
    ) -> Result<Option<ServerPhysicalDesignMutationReceiptId>, ServerPhysicalDesignControlError>
    {
        self.mutation_receipts
            .as_mut()
            .map(|journal| {
                journal.begin(
                    source,
                    evidence_epoch,
                    MutationReceiptTarget::Index {
                        candidate,
                        index_name,
                    },
                )
            })
            .transpose()
            .map_err(ServerPhysicalDesignControlError::MutationReceipt)
    }

    fn finish_index_result<T>(
        &mut self,
        receipt_id: Option<ServerPhysicalDesignMutationReceiptId>,
        result: Result<T, ServerPhysicalDesignControlError>,
        success_outcome: impl FnOnce(&T) -> ServerPhysicalDesignMutationReceiptOutcome,
    ) -> Result<T, ServerPhysicalDesignControlError> {
        let Some(receipt_id) = receipt_id else {
            return result;
        };
        match result {
            Ok(report) => {
                let outcome = success_outcome(&report);
                self.mutation_receipts
                    .as_mut()
                    .ok_or(ServerPhysicalDesignMutationReceiptControlError::NotEnabled)
                    .and_then(|journal| journal.finish(receipt_id, outcome))
                    .map_err(ServerPhysicalDesignControlError::MutationRecoveryRequired)?;
                Ok(report)
            }
            Err(error) => {
                let Some(outcome) = index_error_receipt_outcome(&error) else {
                    if let Some(journal) = self.mutation_receipts.as_mut() {
                        journal.mark_recovery_required();
                    }
                    return Err(
                        ServerPhysicalDesignControlError::PostBeginMutationOutcomeUncertain(
                            Box::new(error),
                        ),
                    );
                };
                self.mutation_receipts
                    .as_mut()
                    .ok_or(ServerPhysicalDesignMutationReceiptControlError::NotEnabled)
                    .and_then(|journal| journal.finish(receipt_id, outcome))
                    .map_err(ServerPhysicalDesignControlError::MutationRecoveryRequired)?;
                Err(error)
            }
        }
    }

    fn begin_columnar_receipt(
        &mut self,
        source: ServerPhysicalDesignMutationSource,
        evidence_epoch: PhysicalDesignEvidenceEpoch,
        candidate: PhysicalColumnarCandidate,
        mode: PhysicalColumnarDesignMode,
        placement: ServerPhysicalColumnarPlacementKey,
        directory: PathBuf,
    ) -> Result<
        Option<ServerPhysicalDesignMutationReceiptId>,
        ServerPhysicalColumnarDesignControlError,
    > {
        self.mutation_receipts
            .as_mut()
            .map(|journal| {
                journal.begin(
                    source,
                    evidence_epoch,
                    MutationReceiptTarget::Columnar {
                        candidate,
                        mode,
                        placement,
                        directory,
                    },
                )
            })
            .transpose()
            .map_err(ServerPhysicalColumnarDesignControlError::MutationReceipt)
    }

    fn finish_columnar_result<T>(
        &mut self,
        receipt_id: Option<ServerPhysicalDesignMutationReceiptId>,
        result: Result<T, ServerPhysicalColumnarDesignControlError>,
        success_outcome: impl FnOnce(&T) -> ServerPhysicalDesignMutationReceiptOutcome,
    ) -> Result<T, ServerPhysicalColumnarDesignControlError> {
        let Some(receipt_id) = receipt_id else {
            return result;
        };
        match result {
            Ok(report) => {
                let outcome = success_outcome(&report);
                self.mutation_receipts
                    .as_mut()
                    .ok_or(ServerPhysicalDesignMutationReceiptControlError::NotEnabled)
                    .and_then(|journal| journal.finish(receipt_id, outcome))
                    .map_err(ServerPhysicalColumnarDesignControlError::MutationRecoveryRequired)?;
                Ok(report)
            }
            Err(error) => {
                let Some(outcome) = columnar_error_receipt_outcome(&error) else {
                    if let Some(journal) = self.mutation_receipts.as_mut() {
                        journal.mark_recovery_required();
                    }
                    return Err(
                        ServerPhysicalColumnarDesignControlError::PostBeginMutationOutcomeUncertain(
                            Box::new(error),
                        ),
                    );
                };
                self.mutation_receipts
                    .as_mut()
                    .ok_or(ServerPhysicalDesignMutationReceiptControlError::NotEnabled)
                    .and_then(|journal| journal.finish(receipt_id, outcome))
                    .map_err(ServerPhysicalColumnarDesignControlError::MutationRecoveryRequired)?;
                Err(error)
            }
        }
    }

    fn propose_columnar(
        &self,
        database: &Database,
        candidate: PhysicalColumnarCandidate,
        mode: PhysicalColumnarDesignMode,
        placement: ServerPhysicalColumnarPlacementKey,
    ) -> Result<ServerPhysicalColumnarDesignProposal, ServerPhysicalColumnarDesignControlError>
    {
        let config = self
            .columnar_apply
            .as_ref()
            .ok_or(ServerPhysicalColumnarDesignControlError::ColumnarApplyNotEnabled)?;
        if !config.allows(mode) {
            return Err(ServerPhysicalColumnarDesignControlError::ModeNotAllowed(
                mode,
            ));
        }
        let directory = config
            .resolve(&placement)
            .map_err(ServerPhysicalColumnarDesignControlError::PlacementRootUnavailable)?;
        let proposal = database
            .propose_physical_columnar_design(
                &self.evidence,
                self.policy,
                candidate,
                mode,
                &directory,
            )
            .map_err(|error| ServerPhysicalColumnarDesignControlError::Proposal(Box::new(error)))?;
        if proposal.directory() != directory {
            return Err(ServerPhysicalColumnarDesignControlError::PlacementInvariantViolated);
        }
        require_unoccupied(&directory, &placement)?;
        Ok(ServerPhysicalColumnarDesignProposal {
            origin: Arc::downgrade(&self.identity),
            placement,
            proposal,
        })
    }

    fn apply_columnar(
        &mut self,
        database: &mut Database,
        proposal: &ServerPhysicalColumnarDesignProposal,
    ) -> Result<ServerPhysicalColumnarDesignApplyReport, ServerPhysicalColumnarDesignControlError>
    {
        let directory = proposal.proposal.directory().to_path_buf();
        let receipt_id = self.begin_columnar_receipt(
            ServerPhysicalDesignMutationSource::Programmatic,
            proposal.evidence_epoch(),
            proposal.candidate().clone(),
            proposal.mode(),
            proposal.placement().clone(),
            directory,
        )?;
        let result = self.apply_columnar_unreceipted(database, proposal);
        self.finish_columnar_result(receipt_id, result, |report| match report.outcome {
            PhysicalColumnarDesignApplyOutcome::Created { projection_id } => {
                ServerPhysicalDesignMutationReceiptOutcome::CreatedColumnar { projection_id }
            }
            PhysicalColumnarDesignApplyOutcome::AlreadyApplied { projection_id } => {
                ServerPhysicalDesignMutationReceiptOutcome::AlreadyAppliedColumnar { projection_id }
            }
            PhysicalColumnarDesignApplyOutcome::AlreadyCovered => {
                ServerPhysicalDesignMutationReceiptOutcome::AlreadyCovered
            }
        })
    }

    fn apply_columnar_unreceipted(
        &mut self,
        database: &mut Database,
        proposal: &ServerPhysicalColumnarDesignProposal,
    ) -> Result<ServerPhysicalColumnarDesignApplyReport, ServerPhysicalColumnarDesignControlError>
    {
        let Some(origin) = proposal.origin.upgrade() else {
            return Err(ServerPhysicalColumnarDesignControlError::ProposalRuntimeChanged);
        };
        if !Arc::ptr_eq(&origin, &self.identity) {
            return Err(ServerPhysicalColumnarDesignControlError::ProposalRuntimeChanged);
        }
        let config = self
            .columnar_apply
            .as_ref()
            .ok_or(ServerPhysicalColumnarDesignControlError::ColumnarApplyNotEnabled)?;
        if !config.allows(proposal.mode()) {
            return Err(ServerPhysicalColumnarDesignControlError::ModeNotAllowed(
                proposal.mode(),
            ));
        }
        let directory = config
            .resolve(proposal.placement())
            .map_err(ServerPhysicalColumnarDesignControlError::PlacementRootUnavailable)?;
        if proposal.proposal.directory() != directory {
            return Err(ServerPhysicalColumnarDesignControlError::PlacementInvariantViolated);
        }
        let location = database
            .inspect_physical_columnar_design_location(
                proposal.candidate(),
                proposal.mode(),
                &directory,
            )
            .map_err(ServerPhysicalColumnarDesignControlError::LocationInspection)?;
        if location == PhysicalColumnarDesignLocationState::Available {
            require_unoccupied(&directory, proposal.placement())?;
        }
        let report = database
            .apply_physical_columnar_design(&self.evidence, &proposal.proposal)
            .map_err(|error| ServerPhysicalColumnarDesignControlError::Apply(Box::new(error)))?;
        Ok(ServerPhysicalColumnarDesignApplyReport {
            candidate: report.candidate,
            mode: report.mode,
            placement: proposal.placement.clone(),
            evidence_epoch: report.evidence_epoch,
            global_commit_seq_before: report.global_commit_seq_before,
            global_commit_seq_after: report.global_commit_seq_after,
            schema_generation: report.schema_generation,
            outcome: report.outcome,
        })
    }

    fn apply_approved_index(
        &mut self,
        database: &mut Database,
        runtime_token_matches: bool,
        expected_evidence_epoch: PhysicalDesignEvidenceEpoch,
        candidate: PhysicalIndexCandidate,
        index_name: IndexName,
    ) -> ServerPhysicalDesignMutationControlReply<
        ServerApprovedPhysicalIndexApplyReport,
        ServerPhysicalDesignControlError,
    > {
        let receipt_id = match self.begin_index_receipt(
            ServerPhysicalDesignMutationSource::LocalOperator,
            expected_evidence_epoch,
            candidate,
            index_name.clone(),
        ) {
            Ok(receipt_id) => receipt_id,
            Err(error) => {
                return ServerPhysicalDesignMutationControlReply::without_receipt(Err(error));
            }
        };
        let receipt = receipt_id.and_then(|receipt_id| {
            self.mutation_receipts
                .as_ref()
                .map(|journal| journal.reference(receipt_id))
        });
        let result = self.apply_approved_index_unreceipted(
            database,
            runtime_token_matches,
            expected_evidence_epoch,
            candidate,
            index_name,
        );
        let result = self.finish_index_result(receipt_id, result, |report| match report.outcome {
            ServerApprovedPhysicalIndexApplyOutcome::Created { index_id } => {
                ServerPhysicalDesignMutationReceiptOutcome::CreatedIndex { index_id }
            }
            ServerApprovedPhysicalIndexApplyOutcome::AlreadyApplied { index_id } => {
                ServerPhysicalDesignMutationReceiptOutcome::AlreadyAppliedIndex { index_id }
            }
            ServerApprovedPhysicalIndexApplyOutcome::AlreadyCovered => {
                ServerPhysicalDesignMutationReceiptOutcome::AlreadyCovered
            }
        });
        ServerPhysicalDesignMutationControlReply { receipt, result }
    }

    fn apply_approved_index_unreceipted(
        &mut self,
        database: &mut Database,
        runtime_token_matches: bool,
        expected_evidence_epoch: PhysicalDesignEvidenceEpoch,
        candidate: PhysicalIndexCandidate,
        index_name: IndexName,
    ) -> Result<ServerApprovedPhysicalIndexApplyReport, ServerPhysicalDesignControlError> {
        match database.inspect_physical_index_design_name(candidate, &index_name) {
            PhysicalIndexDesignNameState::AlreadyApplied { index_id } => {
                return Ok(ServerApprovedPhysicalIndexApplyReport {
                    candidate,
                    index_name,
                    outcome: ServerApprovedPhysicalIndexApplyOutcome::AlreadyApplied { index_id },
                });
            }
            PhysicalIndexDesignNameState::Conflict => {
                return Err(ServerPhysicalDesignControlError::PhysicalIndexNameConflict(
                    index_name,
                ));
            }
            PhysicalIndexDesignNameState::Available => {}
        }
        if !runtime_token_matches {
            return Err(ServerPhysicalDesignControlError::PhysicalDesignRuntimeChanged);
        }
        let actual = self.evidence.epoch();
        if actual != expected_evidence_epoch {
            return Err(ServerPhysicalDesignControlError::EvidenceEpochChanged {
                expected: expected_evidence_epoch,
                actual,
            });
        }

        let proposal =
            match database.propose_physical_index_design(&self.evidence, self.policy, candidate) {
                Ok(proposal) => proposal,
                Err(PhysicalIndexDesignProposalError::CandidateNotRecommended {
                    reason: PhysicalDesignNoActionReason::ExistingDesignCovers,
                    ..
                }) => {
                    return Ok(ServerApprovedPhysicalIndexApplyReport {
                        candidate,
                        index_name,
                        outcome: ServerApprovedPhysicalIndexApplyOutcome::AlreadyCovered,
                    });
                }
                Err(error) => {
                    return Err(ServerPhysicalDesignControlError::Proposal(Box::new(error)));
                }
            };
        let report = database
            .apply_physical_index_design(&self.evidence, &proposal, index_name.clone())
            .map_err(|error| ServerPhysicalDesignControlError::Apply(Box::new(error)))?;
        let outcome = match report.outcome {
            PhysicalIndexDesignApplyOutcome::Created { index_id } => {
                ServerApprovedPhysicalIndexApplyOutcome::Created { index_id }
            }
            PhysicalIndexDesignApplyOutcome::AlreadyApplied { index_id } => {
                ServerApprovedPhysicalIndexApplyOutcome::AlreadyApplied { index_id }
            }
            PhysicalIndexDesignApplyOutcome::AlreadyCovered => {
                ServerApprovedPhysicalIndexApplyOutcome::AlreadyCovered
            }
        };
        Ok(ServerApprovedPhysicalIndexApplyReport {
            candidate,
            index_name,
            outcome,
        })
    }

    fn apply_approved_columnar(
        &mut self,
        database: &mut Database,
        runtime_token_matches: bool,
        expected_evidence_epoch: PhysicalDesignEvidenceEpoch,
        candidate: PhysicalColumnarCandidate,
        mode: PhysicalColumnarDesignMode,
        placement: ServerPhysicalColumnarPlacementKey,
    ) -> ServerPhysicalDesignMutationControlReply<
        ServerApprovedPhysicalColumnarApplyReport,
        ServerPhysicalColumnarDesignControlError,
    > {
        let Some(columnar_apply) = self.columnar_apply.as_ref() else {
            return ServerPhysicalDesignMutationControlReply::without_receipt(Err(
                ServerPhysicalColumnarDesignControlError::ColumnarApplyNotEnabled,
            ));
        };
        let directory = columnar_apply.root().join(placement.as_str());
        let receipt_id = match self.begin_columnar_receipt(
            ServerPhysicalDesignMutationSource::LocalOperator,
            expected_evidence_epoch,
            candidate.clone(),
            mode,
            placement.clone(),
            directory,
        ) {
            Ok(receipt_id) => receipt_id,
            Err(error) => {
                return ServerPhysicalDesignMutationControlReply::without_receipt(Err(error));
            }
        };
        let receipt = receipt_id.and_then(|receipt_id| {
            self.mutation_receipts
                .as_ref()
                .map(|journal| journal.reference(receipt_id))
        });
        let result = self.apply_approved_columnar_unreceipted(
            database,
            runtime_token_matches,
            expected_evidence_epoch,
            candidate,
            mode,
            placement,
        );
        let result =
            self.finish_columnar_result(receipt_id, result, |report| match report.outcome {
                ServerApprovedPhysicalColumnarApplyOutcome::Created { projection_id } => {
                    ServerPhysicalDesignMutationReceiptOutcome::CreatedColumnar { projection_id }
                }
                ServerApprovedPhysicalColumnarApplyOutcome::AlreadyApplied { projection_id } => {
                    ServerPhysicalDesignMutationReceiptOutcome::AlreadyAppliedColumnar {
                        projection_id,
                    }
                }
                ServerApprovedPhysicalColumnarApplyOutcome::AlreadyCovered => {
                    ServerPhysicalDesignMutationReceiptOutcome::AlreadyCovered
                }
            });
        ServerPhysicalDesignMutationControlReply { receipt, result }
    }

    fn apply_approved_columnar_unreceipted(
        &mut self,
        database: &mut Database,
        runtime_token_matches: bool,
        expected_evidence_epoch: PhysicalDesignEvidenceEpoch,
        candidate: PhysicalColumnarCandidate,
        mode: PhysicalColumnarDesignMode,
        placement: ServerPhysicalColumnarPlacementKey,
    ) -> Result<ServerApprovedPhysicalColumnarApplyReport, ServerPhysicalColumnarDesignControlError>
    {
        let config = self
            .columnar_apply
            .as_ref()
            .ok_or(ServerPhysicalColumnarDesignControlError::ColumnarApplyNotEnabled)?;
        let directory = config
            .resolve(&placement)
            .map_err(ServerPhysicalColumnarDesignControlError::PlacementRootUnavailable)?;
        match database
            .inspect_physical_columnar_design_location(&candidate, mode, &directory)
            .map_err(ServerPhysicalColumnarDesignControlError::LocationInspection)?
        {
            PhysicalColumnarDesignLocationState::AlreadyApplied { projection_id } => {
                return Ok(ServerApprovedPhysicalColumnarApplyReport {
                    candidate,
                    mode,
                    placement,
                    outcome: ServerApprovedPhysicalColumnarApplyOutcome::AlreadyApplied {
                        projection_id,
                    },
                });
            }
            PhysicalColumnarDesignLocationState::Conflict { .. } => {
                return Err(ServerPhysicalColumnarDesignControlError::LocationConflict {
                    placement,
                });
            }
            PhysicalColumnarDesignLocationState::Available => {}
        }
        if !config.allows(mode) {
            return Err(ServerPhysicalColumnarDesignControlError::ModeNotAllowed(
                mode,
            ));
        }
        if !runtime_token_matches {
            return Err(ServerPhysicalColumnarDesignControlError::PhysicalDesignRuntimeChanged);
        }
        let actual = self.evidence.epoch();
        if actual != expected_evidence_epoch {
            return Err(
                ServerPhysicalColumnarDesignControlError::EvidenceEpochChanged {
                    expected: expected_evidence_epoch,
                    actual,
                },
            );
        }
        require_unoccupied(&directory, &placement)?;
        let proposal = match database.propose_physical_columnar_design(
            &self.evidence,
            self.policy,
            candidate.clone(),
            mode,
            &directory,
        ) {
            Ok(proposal) => proposal,
            Err(PhysicalColumnarDesignProposalError::CandidateNotRecommended {
                reason: PhysicalDesignNoActionReason::ExistingDesignCovers,
                ..
            }) => {
                return Ok(ServerApprovedPhysicalColumnarApplyReport {
                    candidate,
                    mode,
                    placement,
                    outcome: ServerApprovedPhysicalColumnarApplyOutcome::AlreadyCovered,
                });
            }
            Err(error) => {
                return Err(ServerPhysicalColumnarDesignControlError::Proposal(
                    Box::new(error),
                ));
            }
        };
        let report = database
            .apply_physical_columnar_design(&self.evidence, &proposal)
            .map_err(|error| ServerPhysicalColumnarDesignControlError::Apply(Box::new(error)))?;
        let outcome = match report.outcome {
            PhysicalColumnarDesignApplyOutcome::Created { projection_id } => {
                ServerApprovedPhysicalColumnarApplyOutcome::Created { projection_id }
            }
            PhysicalColumnarDesignApplyOutcome::AlreadyApplied { projection_id } => {
                ServerApprovedPhysicalColumnarApplyOutcome::AlreadyApplied { projection_id }
            }
            PhysicalColumnarDesignApplyOutcome::AlreadyCovered => {
                ServerApprovedPhysicalColumnarApplyOutcome::AlreadyCovered
            }
        };
        Ok(ServerApprovedPhysicalColumnarApplyReport {
            candidate,
            mode,
            placement,
            outcome,
        })
    }
}

fn index_error_receipt_outcome(
    error: &ServerPhysicalDesignControlError,
) -> Option<ServerPhysicalDesignMutationReceiptOutcome> {
    match error {
        ServerPhysicalDesignControlError::Apply(error)
            if matches!(
                error.as_ref(),
                PhysicalIndexDesignApplyError::Database(_)
                    | PhysicalIndexDesignApplyError::Advisor(PhysicalDesignAdvisorError::Database(
                        _,
                    ))
            ) =>
        {
            None
        }
        ServerPhysicalDesignControlError::Apply(_) => {
            Some(ServerPhysicalDesignMutationReceiptOutcome::Rejected)
        }
        ServerPhysicalDesignControlError::MutationReceipt(_)
        | ServerPhysicalDesignControlError::MutationRecoveryRequired(_)
        | ServerPhysicalDesignControlError::PostBeginMutationOutcomeUncertain(_)
        | ServerPhysicalDesignControlError::MutationOutcomeUncertain => None,
        _ => Some(ServerPhysicalDesignMutationReceiptOutcome::Rejected),
    }
}

fn columnar_error_receipt_outcome(
    error: &ServerPhysicalColumnarDesignControlError,
) -> Option<ServerPhysicalDesignMutationReceiptOutcome> {
    match error {
        ServerPhysicalColumnarDesignControlError::Apply(error)
            if matches!(
                error.as_ref(),
                PhysicalColumnarDesignApplyError::Database(_)
                    | PhysicalColumnarDesignApplyError::Advisor(
                        PhysicalDesignAdvisorError::Database(_),
                    )
            ) =>
        {
            None
        }
        ServerPhysicalColumnarDesignControlError::Apply(_) => {
            Some(ServerPhysicalDesignMutationReceiptOutcome::Rejected)
        }
        ServerPhysicalColumnarDesignControlError::MutationReceipt(_)
        | ServerPhysicalColumnarDesignControlError::MutationRecoveryRequired(_)
        | ServerPhysicalColumnarDesignControlError::PostBeginMutationOutcomeUncertain(_)
        | ServerPhysicalColumnarDesignControlError::MutationOutcomeUncertain => None,
        _ => Some(ServerPhysicalDesignMutationReceiptOutcome::Rejected),
    }
}

fn require_unoccupied(
    directory: &Path,
    placement: &ServerPhysicalColumnarPlacementKey,
) -> Result<(), ServerPhysicalColumnarDesignControlError> {
    match fs::symlink_metadata(directory) {
        Ok(_) => Err(
            ServerPhysicalColumnarDesignControlError::PlacementOccupied {
                placement: placement.clone(),
            },
        ),
        Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(
            ServerPhysicalColumnarDesignControlError::PlacementInspection {
                placement: placement.clone(),
                source,
            },
        ),
    }
}

pub(crate) fn forward_physical_design_control_requests<F>(
    requests: &Receiver<ServerPhysicalDesignControlRequest>,
    mut submit: F,
) where
    F: FnMut(ServerPhysicalDesignWorkerCommand) -> Result<(), ()>,
{
    // Bound listener work even when a caller immediately submits its next control.
    let request = match requests.try_recv() {
        Ok(request) => request,
        Err(TryRecvError::Empty | TryRecvError::Disconnected) => return,
    };
    match request {
        ServerPhysicalDesignControlRequest::Status { reply } => {
            let fallback = reply.clone();
            if submit(ServerPhysicalDesignWorkerCommand::Status { reply }).is_err() {
                let _ = fallback.send(Err(ServerPhysicalDesignControlError::ServerStopped));
            }
        }
        ServerPhysicalDesignControlRequest::MutationReceipts {
            after,
            limit,
            reply,
        } => {
            let fallback = reply.clone();
            if submit(ServerPhysicalDesignWorkerCommand::MutationReceipts {
                after,
                limit,
                reply,
            })
            .is_err()
            {
                let _ = fallback.send(Err(
                    ServerPhysicalDesignMutationReceiptControlError::ServerStopped,
                ));
            }
        }
        ServerPhysicalDesignControlRequest::MutationReceiptsScoped {
            after,
            limit,
            reply,
        } => {
            let fallback = reply.clone();
            if submit(ServerPhysicalDesignWorkerCommand::MutationReceiptsScoped {
                after,
                limit,
                reply,
            })
            .is_err()
            {
                let _ = fallback.send(Err(
                    ServerPhysicalDesignMutationReceiptControlError::ServerStopped,
                ));
            }
        }
        ServerPhysicalDesignControlRequest::MutationReceiptStatus { reply } => {
            let fallback = reply.clone();
            if submit(ServerPhysicalDesignWorkerCommand::MutationReceiptStatus { reply }).is_err() {
                let _ = fallback.send(Err(
                    ServerPhysicalDesignMutationReceiptControlError::ServerStopped,
                ));
            }
        }
        ServerPhysicalDesignControlRequest::Recommendations { reply } => {
            let fallback = reply.clone();
            if submit(ServerPhysicalDesignWorkerCommand::Recommendations { reply }).is_err() {
                let _ = fallback.send(Err(ServerPhysicalDesignControlError::ServerStopped));
            }
        }
        ServerPhysicalDesignControlRequest::ProposeIndex { candidate, reply } => {
            let fallback = reply.clone();
            if submit(ServerPhysicalDesignWorkerCommand::ProposeIndex { candidate, reply }).is_err()
            {
                let _ = fallback.send(Err(ServerPhysicalDesignControlError::ServerStopped));
            }
        }
        ServerPhysicalDesignControlRequest::ApplyIndex {
            proposal,
            index_name,
            reply,
        } => {
            let fallback = reply.clone();
            if submit(ServerPhysicalDesignWorkerCommand::ApplyIndex {
                proposal,
                index_name,
                reply,
            })
            .is_err()
            {
                let _ = fallback.send(Err(ServerPhysicalDesignControlError::ServerStopped));
            }
        }
        ServerPhysicalDesignControlRequest::ProposeColumnar {
            candidate,
            mode,
            placement,
            reply,
        } => {
            let fallback = reply.clone();
            if submit(ServerPhysicalDesignWorkerCommand::ProposeColumnar {
                candidate,
                mode,
                placement,
                reply,
            })
            .is_err()
            {
                let _ = fallback.send(Err(ServerPhysicalColumnarDesignControlError::ServerStopped));
            }
        }
        ServerPhysicalDesignControlRequest::ApplyColumnar { proposal, reply } => {
            let fallback = reply.clone();
            if submit(ServerPhysicalDesignWorkerCommand::ApplyColumnar { proposal, reply }).is_err()
            {
                let _ = fallback.send(Err(ServerPhysicalColumnarDesignControlError::ServerStopped));
            }
        }
        ServerPhysicalDesignControlRequest::ApplyApprovedIndex {
            runtime_token_matches,
            expected_evidence_epoch,
            candidate,
            index_name,
            reply,
        } => {
            let fallback = reply.clone();
            if submit(ServerPhysicalDesignWorkerCommand::ApplyApprovedIndex {
                runtime_token_matches,
                expected_evidence_epoch,
                candidate,
                index_name,
                reply,
            })
            .is_err()
            {
                let _ = fallback.send(ServerPhysicalDesignMutationControlReply::without_receipt(
                    Err(ServerPhysicalDesignControlError::ServerStopped),
                ));
            }
        }
        ServerPhysicalDesignControlRequest::ApplyApprovedColumnar {
            runtime_token_matches,
            expected_evidence_epoch,
            candidate,
            mode,
            placement,
            reply,
        } => {
            let fallback = reply.clone();
            if submit(ServerPhysicalDesignWorkerCommand::ApplyApprovedColumnar {
                runtime_token_matches,
                expected_evidence_epoch,
                candidate,
                mode,
                placement,
                reply,
            })
            .is_err()
            {
                let _ = fallback.send(ServerPhysicalDesignMutationControlReply::without_receipt(
                    Err(ServerPhysicalColumnarDesignControlError::ServerStopped),
                ));
            }
        }
        ServerPhysicalDesignControlRequest::RotateEvidenceIfEpoch { expected, reply } => {
            let fallback = reply.clone();
            if submit(ServerPhysicalDesignWorkerCommand::RotateEvidenceIfEpoch { expected, reply })
                .is_err()
            {
                let _ = fallback.send(Err(ServerPhysicalDesignControlError::ServerStopped));
            }
        }
    }
}

pub(crate) fn handle_disabled_physical_design_worker_command(
    command: ServerPhysicalDesignWorkerCommand,
) {
    match command {
        ServerPhysicalDesignWorkerCommand::Status { reply } => {
            let _ = reply.send(Err(
                ServerPhysicalDesignControlError::PhysicalDesignNotEnabled,
            ));
        }
        ServerPhysicalDesignWorkerCommand::MutationReceipts { reply, .. } => {
            let _ = reply.send(Err(
                ServerPhysicalDesignMutationReceiptControlError::NotEnabled,
            ));
        }
        ServerPhysicalDesignWorkerCommand::MutationReceiptsScoped { reply, .. } => {
            let _ = reply.send(Err(
                ServerPhysicalDesignMutationReceiptControlError::NotEnabled,
            ));
        }
        ServerPhysicalDesignWorkerCommand::MutationReceiptStatus { reply } => {
            let _ = reply.send(Err(
                ServerPhysicalDesignMutationReceiptControlError::NotEnabled,
            ));
        }
        ServerPhysicalDesignWorkerCommand::Recommendations { reply } => {
            let _ = reply.send(Err(
                ServerPhysicalDesignControlError::PhysicalDesignNotEnabled,
            ));
        }
        ServerPhysicalDesignWorkerCommand::ProposeIndex { reply, .. } => {
            let _ = reply.send(Err(
                ServerPhysicalDesignControlError::PhysicalDesignNotEnabled,
            ));
        }
        ServerPhysicalDesignWorkerCommand::ApplyIndex { reply, .. } => {
            let _ = reply.send(Err(
                ServerPhysicalDesignControlError::PhysicalDesignNotEnabled,
            ));
        }
        ServerPhysicalDesignWorkerCommand::ProposeColumnar { reply, .. } => {
            let _ = reply.send(Err(
                ServerPhysicalColumnarDesignControlError::PhysicalDesignNotEnabled,
            ));
        }
        ServerPhysicalDesignWorkerCommand::ApplyColumnar { reply, .. } => {
            let _ = reply.send(Err(
                ServerPhysicalColumnarDesignControlError::PhysicalDesignNotEnabled,
            ));
        }
        ServerPhysicalDesignWorkerCommand::ApplyApprovedIndex { reply, .. } => {
            let _ = reply.send(ServerPhysicalDesignMutationControlReply::without_receipt(
                Err(ServerPhysicalDesignControlError::PhysicalDesignNotEnabled),
            ));
        }
        ServerPhysicalDesignWorkerCommand::ApplyApprovedColumnar { reply, .. } => {
            let _ = reply.send(ServerPhysicalDesignMutationControlReply::without_receipt(
                Err(ServerPhysicalColumnarDesignControlError::PhysicalDesignNotEnabled),
            ));
        }
        ServerPhysicalDesignWorkerCommand::RotateEvidenceIfEpoch { reply, .. } => {
            let _ = reply.send(Err(
                ServerPhysicalDesignControlError::PhysicalDesignNotEnabled,
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    use netbadb_core::{
        DatabaseCoordinatorConfig, PhysicalColumnarDesignApplyOutcome, PhysicalColumnarDesignMode,
        PhysicalDesignAdvisorError, PhysicalDesignRecommendationPolicy,
        PhysicalIndexDesignApplyOutcome, PhysicalIndexDesignProposalError,
        PreparedExecutionFeedback, ProjectionCatalogError, TableStorageCreateSpec,
    };
    use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
    use netbadb_types::{ColumnId, ColumnarProjectionId, PhysicalType, TableId};

    use super::*;

    static NEXT_PATH: AtomicU64 = AtomicU64::new(1);
    const TABLE_ID: TableId = TableId(61_240);
    const CANDIDATE: PhysicalIndexCandidate = PhysicalIndexCandidate {
        table_id: TABLE_ID,
        column_id: ColumnId(2),
    };

    fn columnar_candidate() -> PhysicalColumnarCandidate {
        PhysicalColumnarCandidate {
            table_id: TABLE_ID,
            columns: vec![ColumnId(1)],
        }
    }

    fn assert_programmatic_index_channel_result(
        proposal: &ServerPhysicalIndexDesignProposal,
        drop_receiver_before_send: bool,
    ) -> ServerPhysicalDesignControlError {
        let (requests, receiver) = std::sync::mpsc::channel();
        if drop_receiver_before_send {
            drop(receiver);
            return ServerPhysicalDesignControlHandle::new(requests)
                .apply_index(proposal, IndexName::new("programmatic_idx").unwrap())
                .unwrap_err();
        }
        let worker = std::thread::spawn(move || drop(receiver.recv().unwrap()));
        let result = ServerPhysicalDesignControlHandle::new(requests)
            .apply_index(proposal, IndexName::new("programmatic_idx").unwrap())
            .unwrap_err();
        worker.join().unwrap();
        result
    }

    #[test]
    fn resource_control_permit_lasts_until_reply_and_shutdown_releases_it() {
        let (send, receive) = mpsc::channel();
        let handle = ServerPhysicalDesignControlHandle::new(send);
        let caller = handle.clone();
        let join = std::thread::spawn(move || caller.status());
        let request = receive.recv().unwrap();
        assert!(handle.admission.is_occupied());
        assert!(matches!(receive.try_recv(), Err(TryRecvError::Empty)));
        // A lost worker reply must release admission and preserve the typed error.
        drop(request);
        assert!(matches!(
            join.join().unwrap(),
            Err(ServerPhysicalDesignControlError::ServerStopped)
        ));
        assert!(!handle.admission.is_occupied());
        drop(receive);
        assert!(matches!(
            handle.status(),
            Err(ServerPhysicalDesignControlError::ServerStopped)
        ));
    }

    #[test]
    fn programmatic_index_send_failure_is_definite() {
        let mut fixture = Fixture::create("programmatic-index-send-failure");
        let mut runtime = ServerPhysicalDesignRuntime::new(config());
        record_candidate(&mut fixture.database, &mut runtime);
        let proposal = propose(&mut fixture.database, &mut runtime).unwrap();
        assert!(matches!(
            assert_programmatic_index_channel_result(&proposal, true),
            ServerPhysicalDesignControlError::ServerStopped
        ));
        fixture.close();
    }

    #[test]
    fn programmatic_index_reply_loss_is_uncertain() {
        let mut fixture = Fixture::create("programmatic-index-reply-loss");
        let mut runtime = ServerPhysicalDesignRuntime::new(config());
        record_candidate(&mut fixture.database, &mut runtime);
        let proposal = propose(&mut fixture.database, &mut runtime).unwrap();
        assert!(matches!(
            assert_programmatic_index_channel_result(&proposal, false),
            ServerPhysicalDesignControlError::MutationOutcomeUncertain
        ));
        fixture.close();
    }

    fn assert_programmatic_columnar_channel_result(
        proposal: &ServerPhysicalColumnarDesignProposal,
        drop_receiver_before_send: bool,
    ) -> ServerPhysicalColumnarDesignControlError {
        let (requests, receiver) = std::sync::mpsc::channel();
        if drop_receiver_before_send {
            drop(receiver);
            return ServerPhysicalDesignControlHandle::new(requests)
                .apply_columnar(proposal)
                .unwrap_err();
        }
        let worker = std::thread::spawn(move || drop(receiver.recv().unwrap()));
        let result = ServerPhysicalDesignControlHandle::new(requests)
            .apply_columnar(proposal)
            .unwrap_err();
        worker.join().unwrap();
        result
    }

    #[test]
    fn programmatic_columnar_send_failure_is_definite() {
        let mut fixture = Fixture::create("programmatic-columnar-send-failure");
        let placements = fixture.root.join("placements");
        fs::create_dir(&placements).unwrap();
        let mut runtime = columnar_runtime(&placements);
        record_columnar_candidate(&mut fixture.database, &mut runtime);
        let proposal = propose_columnar(
            &mut fixture.database,
            &mut runtime,
            PhysicalColumnarDesignMode::Snapshot,
            "programmatic-columnar",
        )
        .unwrap();
        assert!(matches!(
            assert_programmatic_columnar_channel_result(&proposal, true),
            ServerPhysicalColumnarDesignControlError::ServerStopped
        ));
        fixture.close();
    }

    #[test]
    fn programmatic_columnar_reply_loss_is_uncertain() {
        let mut fixture = Fixture::create("programmatic-columnar-reply-loss");
        let placements = fixture.root.join("placements");
        fs::create_dir(&placements).unwrap();
        let mut runtime = columnar_runtime(&placements);
        record_columnar_candidate(&mut fixture.database, &mut runtime);
        let proposal = propose_columnar(
            &mut fixture.database,
            &mut runtime,
            PhysicalColumnarDesignMode::Snapshot,
            "programmatic-columnar",
        )
        .unwrap();
        assert!(matches!(
            assert_programmatic_columnar_channel_result(&proposal, false),
            ServerPhysicalColumnarDesignControlError::MutationOutcomeUncertain
        ));
        fixture.close();
    }

    #[test]
    fn approved_columnar_control_distinguishes_not_sent_from_uncertain_reply_loss() {
        let (requests, receiver) = std::sync::mpsc::channel();
        drop(receiver);
        let stopped = ServerPhysicalDesignControlHandle::new(requests).apply_approved_columnar(
            true,
            PhysicalDesignEvidenceEpoch(1),
            columnar_candidate(),
            PhysicalColumnarDesignMode::Snapshot,
            ServerPhysicalColumnarPlacementKey::new("not-sent").unwrap(),
        );
        assert!(matches!(
            stopped.result,
            Err(ServerPhysicalColumnarDesignControlError::ServerStopped)
        ));

        let (requests, receiver) = std::sync::mpsc::channel();
        let control = ServerPhysicalDesignControlHandle::new(requests);
        let worker = std::thread::spawn(move || {
            let request = receiver.recv().unwrap();
            assert!(matches!(
                request,
                ServerPhysicalDesignControlRequest::ApplyApprovedColumnar { .. }
            ));
            drop(request);
        });
        let uncertain = control.apply_approved_columnar(
            true,
            PhysicalDesignEvidenceEpoch(1),
            columnar_candidate(),
            PhysicalColumnarDesignMode::Snapshot,
            ServerPhysicalColumnarPlacementKey::new("reply-lost").unwrap(),
        );
        worker.join().unwrap();
        assert!(matches!(
            uncertain.result,
            Err(ServerPhysicalColumnarDesignControlError::MutationOutcomeUncertain)
        ));
    }

    struct Fixture {
        root: PathBuf,
        database: Database,
    }

    impl Fixture {
        fn create(name: &str) -> Self {
            let suffix = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
            let root = std::env::temp_dir().join(format!(
                "netbadb-server-physical-design-{name}-{}-{suffix}",
                std::process::id()
            ));
            fs::create_dir_all(&root).expect("create physical-design fixture root");
            let table = TableDef::new(
                TABLE_ID,
                "events",
                vec![
                    ColumnDef::new(ColumnId(1), "id", TypeSpec::Physical(PhysicalType::Int64)),
                    ColumnDef::new(
                        ColumnId(2),
                        "category",
                        TypeSpec::Physical(PhysicalType::Int64),
                    ),
                ],
            );
            let mut database = Database::create_catalog(
                root.join("catalog"),
                vec![TableStorageCreateSpec::heap(root.join("events"), table)],
                Some(
                    DatabaseCoordinatorConfig::new(root.join("coordinator"))
                        .with_global_visibility(),
                ),
            )
            .expect("create physical-design fixture");
            database
                .execute("INSERT INTO events (id, category) VALUES (1, 7)")
                .expect("seed physical-design fixture");
            Self { root, database }
        }

        fn close(self) {
            self.database
                .close()
                .expect("close physical-design fixture");
            fs::remove_dir_all(self.root).expect("remove physical-design fixture root");
        }
    }

    fn config() -> ServerPhysicalDesignAdvisorConfig {
        let recommendation = PhysicalDesignRecommendationPolicy {
            minimum_reports: 1,
            minimum_distinct_query_shapes: 1,
            minimum_actual_scan_work_units: 0,
            max_recommendations: 8,
        };
        ServerPhysicalDesignAdvisorConfig::new(
            PhysicalDesignEvidenceLimits::default(),
            PhysicalDesignAdvisorPolicy {
                index: recommendation,
                columnar: recommendation,
            },
        )
    }

    fn record_candidate(
        database: &mut Database,
        runtime: &mut ServerPhysicalDesignRuntime,
    ) -> ExecutionFeedbackReport {
        let prepared = database
            .prepare_statement("SELECT id FROM events WHERE category = 7", &[])
            .expect("prepare candidate query");
        let execution = database
            .execute_prepared_with_feedback(&prepared, &[])
            .expect("execute candidate query with feedback");
        let PreparedExecutionFeedback::Query(report) = execution.feedback else {
            panic!("candidate query must return feedback");
        };
        runtime.record_successful_query(&report);
        *report
    }

    fn record_columnar_candidate(
        database: &mut Database,
        runtime: &mut ServerPhysicalDesignRuntime,
    ) -> ExecutionFeedbackReport {
        let prepared = database
            .prepare_statement("SELECT id FROM events", &[])
            .expect("prepare Columnar candidate query");
        let execution = database
            .execute_prepared_with_feedback(&prepared, &[])
            .expect("execute Columnar candidate query with feedback");
        let PreparedExecutionFeedback::Query(report) = execution.feedback else {
            panic!("Columnar candidate query must return feedback");
        };
        runtime.record_successful_query(&report);
        *report
    }

    fn columnar_runtime(root: &Path) -> ServerPhysicalDesignRuntime {
        ServerPhysicalDesignRuntime::new_with_columnar(
            config(),
            Some(
                ServerPhysicalColumnarApplyConfig::new(root, true, true)
                    .expect("valid Columnar apply config"),
            ),
        )
    }

    fn propose_columnar(
        database: &mut Database,
        runtime: &mut ServerPhysicalDesignRuntime,
        mode: PhysicalColumnarDesignMode,
        placement: &str,
    ) -> Result<ServerPhysicalColumnarDesignProposal, ServerPhysicalColumnarDesignControlError>
    {
        let (reply, response) = mpsc::sync_channel(1);
        runtime.handle(
            database,
            ServerPhysicalDesignWorkerCommand::ProposeColumnar {
                candidate: columnar_candidate(),
                mode,
                placement: ServerPhysicalColumnarPlacementKey::new(placement)
                    .expect("valid placement key"),
                reply,
            },
        );
        response.recv().expect("Columnar proposal response")
    }

    fn apply_columnar(
        database: &mut Database,
        runtime: &mut ServerPhysicalDesignRuntime,
        proposal: ServerPhysicalColumnarDesignProposal,
    ) -> Result<ServerPhysicalColumnarDesignApplyReport, ServerPhysicalColumnarDesignControlError>
    {
        let (reply, response) = mpsc::sync_channel(1);
        runtime.handle(
            database,
            ServerPhysicalDesignWorkerCommand::ApplyColumnar {
                proposal: Box::new(proposal),
                reply,
            },
        );
        response.recv().expect("Columnar apply response")
    }

    fn propose(
        database: &mut Database,
        runtime: &mut ServerPhysicalDesignRuntime,
    ) -> Result<ServerPhysicalIndexDesignProposal, ServerPhysicalDesignControlError> {
        let (reply, response) = mpsc::sync_channel(1);
        runtime.handle(
            database,
            ServerPhysicalDesignWorkerCommand::ProposeIndex {
                candidate: CANDIDATE,
                reply,
            },
        );
        response.recv().expect("proposal response")
    }

    fn apply(
        database: &mut Database,
        runtime: &mut ServerPhysicalDesignRuntime,
        proposal: ServerPhysicalIndexDesignProposal,
        name: &str,
    ) -> Result<PhysicalIndexDesignApplyReport, ServerPhysicalDesignControlError> {
        let (reply, response) = mpsc::sync_channel(1);
        runtime.handle(
            database,
            ServerPhysicalDesignWorkerCommand::ApplyIndex {
                proposal: Box::new(proposal),
                index_name: IndexName::new(name).expect("valid index name"),
                reply,
            },
        );
        response.recv().expect("apply response")
    }

    fn apply_approved(
        database: &mut Database,
        runtime: &mut ServerPhysicalDesignRuntime,
        runtime_token_matches: bool,
        expected_evidence_epoch: PhysicalDesignEvidenceEpoch,
        name: &str,
    ) -> Result<ServerApprovedPhysicalIndexApplyReport, ServerPhysicalDesignControlError> {
        let (reply, response) = mpsc::sync_channel(1);
        runtime.handle(
            database,
            ServerPhysicalDesignWorkerCommand::ApplyApprovedIndex {
                runtime_token_matches,
                expected_evidence_epoch,
                candidate: CANDIDATE,
                index_name: IndexName::new(name).expect("valid index name"),
                reply,
            },
        );
        response.recv().expect("approved apply response").result
    }

    fn receipts(
        database: &mut Database,
        runtime: &mut ServerPhysicalDesignRuntime,
        after: Option<ServerPhysicalDesignMutationReceiptId>,
        limit: u32,
    ) -> Result<
        ServerPhysicalDesignMutationReceiptPage,
        ServerPhysicalDesignMutationReceiptControlError,
    > {
        let (reply, response) = mpsc::sync_channel(1);
        runtime.handle(
            database,
            ServerPhysicalDesignWorkerCommand::MutationReceipts {
                after,
                limit,
                reply,
            },
        );
        response.recv().expect("receipt response")
    }

    fn scoped_receipts(
        database: &mut Database,
        runtime: &mut ServerPhysicalDesignRuntime,
        after: Option<ServerPhysicalDesignMutationReceiptCursor>,
        limit: u32,
    ) -> Result<
        ServerPhysicalDesignMutationReceiptScopedPage,
        ServerPhysicalDesignMutationReceiptControlError,
    > {
        let (reply, response) = mpsc::sync_channel(1);
        runtime.handle(
            database,
            ServerPhysicalDesignWorkerCommand::MutationReceiptsScoped {
                after,
                limit,
                reply,
            },
        );
        response.recv().expect("scoped receipt response")
    }

    fn receipt_status(
        database: &mut Database,
        runtime: &mut ServerPhysicalDesignRuntime,
    ) -> Result<
        ServerPhysicalDesignMutationReceiptStatus,
        ServerPhysicalDesignMutationReceiptControlError,
    > {
        let (reply, response) = mpsc::sync_channel(1);
        runtime.handle(
            database,
            ServerPhysicalDesignWorkerCommand::MutationReceiptStatus { reply },
        );
        response.recv().expect("receipt status response")
    }

    fn current_commit_seq(database: &Database) -> DatabaseCommitSeq {
        database
            .current_database_snapshot()
            .expect("inspect database snapshot")
            .expect("global database snapshot")
            .commit_seq()
    }

    #[test]
    fn proposal_and_apply_delegate_purity_conflict_retry_and_coverage_to_core() {
        let mut fixture = Fixture::create("apply");
        let mut runtime = ServerPhysicalDesignRuntime::new(config());
        record_candidate(&mut fixture.database, &mut runtime);
        let status_before = runtime.status();
        let commit_before = current_commit_seq(&fixture.database);
        let proposal = propose(&mut fixture.database, &mut runtime).expect("create proposal");
        assert_eq!(runtime.status(), status_before);
        assert_eq!(current_commit_seq(&fixture.database), commit_before);
        assert_eq!(proposal.candidate(), CANDIDATE);
        assert_eq!(proposal.evidence_epoch(), status_before.evidence.epoch);
        assert_eq!(proposal.point_report_count(), 1);
        assert_eq!(proposal.range_report_count(), 0);
        let debug = format!("{proposal:?}");
        assert!(!debug.contains("origin"));
        assert!(!debug.contains("incarnation"));
        assert!(!debug.contains("0x"));

        fixture
            .database
            .create_named_index(
                IndexName::new("occupied_name").expect("valid occupied name"),
                TABLE_ID,
                ColumnId(1),
            )
            .expect("create conflicting named index");
        let before_conflict = current_commit_seq(&fixture.database);
        assert!(matches!(
            apply(
                &mut fixture.database,
                &mut runtime,
                proposal.clone(),
                "occupied_name"
            ),
            Err(ServerPhysicalDesignControlError::Apply(
                error
            ))
                if matches!(
                    error.as_ref(),
                    PhysicalIndexDesignApplyError::IndexNameConflict(_)
                )
        ));
        assert_eq!(current_commit_seq(&fixture.database), before_conflict);

        record_candidate(&mut fixture.database, &mut runtime);
        let status_before_apply = runtime.status();
        let created = apply(
            &mut fixture.database,
            &mut runtime,
            proposal.clone(),
            "events_category_idx",
        )
        .expect("apply proposal with newer same-epoch evidence");
        assert!(matches!(
            created.outcome,
            PhysicalIndexDesignApplyOutcome::Created { .. }
        ));
        assert_eq!(runtime.status(), status_before_apply);

        let epoch = runtime.evidence.epoch();
        runtime.evidence.rotate_window().expect("rotate evidence");
        assert_ne!(runtime.evidence.epoch(), epoch);
        let repeated = apply(
            &mut fixture.database,
            &mut runtime,
            proposal.clone(),
            "events_category_idx",
        )
        .expect("same-name retry after rotation");
        assert!(matches!(
            repeated.outcome,
            PhysicalIndexDesignApplyOutcome::AlreadyApplied { .. }
        ));
        assert_eq!(
            repeated.global_commit_seq_after,
            created.global_commit_seq_after
        );

        let covered = apply(
            &mut fixture.database,
            &mut runtime,
            proposal,
            "events_category_idx_other",
        )
        .expect("different-name retry observes current coverage");
        assert_eq!(
            covered.outcome,
            PhysicalIndexDesignApplyOutcome::AlreadyCovered
        );
        assert_eq!(
            covered.global_commit_seq_after,
            created.global_commit_seq_after
        );
        assert!(matches!(
            fixture
                .database
                .execute("SELECT id FROM events WHERE category = 7")
                .expect("query through current index"),
            netbadb_core::ExecutionResult::Query(_)
        ));
        fixture.close();
    }

    #[test]
    fn proposal_runtime_identity_rejects_live_and_dropped_other_runtime() {
        let mut fixture = Fixture::create("runtime-identity");
        let mut origin = ServerPhysicalDesignRuntime::new(config());
        let report = record_candidate(&mut fixture.database, &mut origin);
        let proposal = propose(&mut fixture.database, &mut origin).expect("create proposal");
        assert_eq!(Arc::strong_count(&origin.identity), 1);
        assert_eq!(Arc::weak_count(&origin.identity), 1);

        let mut other = ServerPhysicalDesignRuntime::new(config());
        other.record_successful_query(&report);
        let before = current_commit_seq(&fixture.database);
        assert!(matches!(
            apply(
                &mut fixture.database,
                &mut other,
                proposal.clone(),
                "events_category_idx"
            ),
            Err(ServerPhysicalDesignControlError::ProposalRuntimeChanged)
        ));
        assert_eq!(current_commit_seq(&fixture.database), before);
        assert_eq!(other.status().evidence.epoch, proposal.evidence_epoch());

        drop(origin);
        assert!(matches!(
            apply(
                &mut fixture.database,
                &mut other,
                proposal,
                "events_category_idx"
            ),
            Err(ServerPhysicalDesignControlError::ProposalRuntimeChanged)
        ));
        assert_eq!(current_commit_seq(&fixture.database), before);
        fixture.close();
    }

    #[test]
    fn empty_and_disabled_programmatic_commands_are_typed_and_side_effect_free() {
        let mut fixture = Fixture::create("disabled");
        let mut runtime = ServerPhysicalDesignRuntime::new(config());
        assert!(matches!(
            propose(&mut fixture.database, &mut runtime),
            Err(ServerPhysicalDesignControlError::Proposal(
                error
            ))
                if matches!(
                    error.as_ref(),
                    PhysicalIndexDesignProposalError::Advisor(
                        PhysicalDesignAdvisorError::NoEvidence
                    )
                )
        ));

        let report = record_candidate(&mut fixture.database, &mut runtime);
        assert!(matches!(
            propose_columnar(
                &mut fixture.database,
                &mut runtime,
                PhysicalColumnarDesignMode::Snapshot,
                "disabled"
            ),
            Err(ServerPhysicalColumnarDesignControlError::ColumnarApplyNotEnabled)
        ));
        let proposal = propose(&mut fixture.database, &mut runtime).expect("create proposal");
        let before = current_commit_seq(&fixture.database);
        let (reply, response) = mpsc::sync_channel(1);
        handle_disabled_physical_design_worker_command(
            ServerPhysicalDesignWorkerCommand::ProposeIndex {
                candidate: CANDIDATE,
                reply,
            },
        );
        assert!(matches!(
            response.recv().expect("disabled proposal response"),
            Err(ServerPhysicalDesignControlError::PhysicalDesignNotEnabled)
        ));
        let (reply, response) = mpsc::sync_channel(1);
        handle_disabled_physical_design_worker_command(
            ServerPhysicalDesignWorkerCommand::MutationReceipts {
                after: None,
                limit: 1,
                reply,
            },
        );
        assert!(matches!(
            response.recv().expect("disabled receipt response"),
            Err(ServerPhysicalDesignMutationReceiptControlError::NotEnabled)
        ));
        let (reply, response) = mpsc::sync_channel(1);
        handle_disabled_physical_design_worker_command(
            ServerPhysicalDesignWorkerCommand::MutationReceiptsScoped {
                after: None,
                limit: 1,
                reply,
            },
        );
        assert!(matches!(
            response.recv().expect("disabled scoped receipt response"),
            Err(ServerPhysicalDesignMutationReceiptControlError::NotEnabled)
        ));
        let (reply, response) = mpsc::sync_channel(1);
        handle_disabled_physical_design_worker_command(
            ServerPhysicalDesignWorkerCommand::MutationReceiptStatus { reply },
        );
        assert!(matches!(
            response.recv().expect("disabled receipt status response"),
            Err(ServerPhysicalDesignMutationReceiptControlError::NotEnabled)
        ));
        let (reply, response) = mpsc::sync_channel(1);
        handle_disabled_physical_design_worker_command(
            ServerPhysicalDesignWorkerCommand::ProposeColumnar {
                candidate: columnar_candidate(),
                mode: PhysicalColumnarDesignMode::Snapshot,
                placement: ServerPhysicalColumnarPlacementKey::new("disabled").unwrap(),
                reply,
            },
        );
        assert!(matches!(
            response.recv().expect("disabled Columnar response"),
            Err(ServerPhysicalColumnarDesignControlError::PhysicalDesignNotEnabled)
        ));
        let (reply, response) = mpsc::sync_channel(1);
        handle_disabled_physical_design_worker_command(
            ServerPhysicalDesignWorkerCommand::ApplyIndex {
                proposal: Box::new(proposal),
                index_name: IndexName::new("events_category_idx").expect("valid index name"),
                reply,
            },
        );
        assert!(matches!(
            response.recv().expect("disabled apply response"),
            Err(ServerPhysicalDesignControlError::PhysicalDesignNotEnabled)
        ));
        assert_eq!(current_commit_seq(&fixture.database), before);
        assert_eq!(report.anchor.global_commit_seq, Some(before));
        fixture.close();
    }

    #[test]
    fn proposal_and_apply_preserve_core_no_action_capacity_and_epoch_errors() {
        let mut fixture = Fixture::create("typed-errors");
        let mut runtime = ServerPhysicalDesignRuntime::new(config());
        let report = record_candidate(&mut fixture.database, &mut runtime);

        let (reply, response) = mpsc::sync_channel(1);
        runtime.handle(
            &mut fixture.database,
            ServerPhysicalDesignWorkerCommand::ProposeIndex {
                candidate: PhysicalIndexCandidate {
                    table_id: TABLE_ID,
                    column_id: ColumnId(1),
                },
                reply,
            },
        );
        assert!(matches!(
            response.recv().expect("unobserved proposal response"),
            Err(ServerPhysicalDesignControlError::Proposal(error))
                if matches!(
                    error.as_ref(),
                    PhysicalIndexDesignProposalError::CandidateNotObserved(_)
                )
        ));

        let recommendation = PhysicalDesignRecommendationPolicy {
            minimum_reports: 2,
            minimum_distinct_query_shapes: 1,
            minimum_actual_scan_work_units: 0,
            max_recommendations: 8,
        };
        let mut below_threshold =
            ServerPhysicalDesignRuntime::new(ServerPhysicalDesignAdvisorConfig::new(
                PhysicalDesignEvidenceLimits::default(),
                PhysicalDesignAdvisorPolicy {
                    index: recommendation,
                    columnar: recommendation,
                },
            ));
        below_threshold.record_successful_query(&report);
        assert!(matches!(
            propose(&mut fixture.database, &mut below_threshold),
            Err(ServerPhysicalDesignControlError::Proposal(error))
                if matches!(
                    error.as_ref(),
                    PhysicalIndexDesignProposalError::CandidateNotRecommended { .. }
                )
        ));

        let mut capacity_limited =
            ServerPhysicalDesignRuntime::new(ServerPhysicalDesignAdvisorConfig::new(
                PhysicalDesignEvidenceLimits {
                    max_index_candidates: 0,
                    ..PhysicalDesignEvidenceLimits::default()
                },
                config().advisor_policy(),
            ));
        capacity_limited.record_successful_query(&report);
        assert!(capacity_limited.status().evidence.truncated);
        assert!(matches!(
            propose(&mut fixture.database, &mut capacity_limited),
            Err(ServerPhysicalDesignControlError::Proposal(error))
                if matches!(
                    error.as_ref(),
                    PhysicalIndexDesignProposalError::Advisor(
                        PhysicalDesignAdvisorError::InconclusiveCapacity { .. }
                    )
                )
        ));

        let proposal = propose(&mut fixture.database, &mut runtime).expect("create proposal");
        runtime.evidence.rotate_window().expect("rotate evidence");
        let before = current_commit_seq(&fixture.database);
        assert!(matches!(
            apply(
                &mut fixture.database,
                &mut runtime,
                proposal,
                "events_category_idx"
            ),
            Err(ServerPhysicalDesignControlError::Apply(error))
                if matches!(
                    error.as_ref(),
                    PhysicalIndexDesignApplyError::EvidenceEpochChanged { .. }
                )
        ));
        assert_eq!(current_commit_seq(&fixture.database), before);
        fixture.close();
    }

    #[test]
    fn operator_approval_is_one_worker_command_with_retry_precedence() {
        let mut fixture = Fixture::create("operator-approval");
        let mut runtime = ServerPhysicalDesignRuntime::new(config());
        record_candidate(&mut fixture.database, &mut runtime);
        let epoch = runtime.status().evidence.epoch;
        let before = current_commit_seq(&fixture.database);

        assert!(matches!(
            apply_approved(
                &mut fixture.database,
                &mut runtime,
                false,
                epoch,
                "stale_runtime_absent"
            ),
            Err(ServerPhysicalDesignControlError::PhysicalDesignRuntimeChanged)
        ));
        assert_eq!(current_commit_seq(&fixture.database), before);
        assert!(fixture.database.indexes(TABLE_ID).unwrap().is_empty());

        assert!(matches!(
            apply_approved(
                &mut fixture.database,
                &mut runtime,
                true,
                PhysicalDesignEvidenceEpoch(epoch.0 + 1),
                "stale_epoch_absent"
            ),
            Err(ServerPhysicalDesignControlError::EvidenceEpochChanged { .. })
        ));
        assert_eq!(current_commit_seq(&fixture.database), before);

        let created = apply_approved(
            &mut fixture.database,
            &mut runtime,
            true,
            epoch,
            "approved_category_idx",
        )
        .expect("create approved index");
        let index_id = match created.outcome {
            ServerApprovedPhysicalIndexApplyOutcome::Created { index_id } => index_id,
            other => panic!("unexpected approved outcome: {other:?}"),
        };
        assert_eq!(
            index_id,
            IndexId(1),
            "rejected approvals must not burn an ID"
        );
        let after = current_commit_seq(&fixture.database);
        assert!(after > before);

        runtime.evidence.rotate_window().expect("rotate evidence");
        let retry = apply_approved(
            &mut fixture.database,
            &mut runtime,
            false,
            epoch,
            "approved_category_idx",
        )
        .expect("exact durable name wins over stale runtime and epoch");
        assert_eq!(
            retry.outcome,
            ServerApprovedPhysicalIndexApplyOutcome::AlreadyApplied { index_id }
        );
        assert_eq!(current_commit_seq(&fixture.database), after);

        fixture
            .database
            .create_named_index(
                IndexName::new("conflicting_approval_name").unwrap(),
                TABLE_ID,
                ColumnId(1),
            )
            .unwrap();
        let before_conflict = current_commit_seq(&fixture.database);
        assert!(matches!(
            apply_approved(
                &mut fixture.database,
                &mut runtime,
                false,
                epoch,
                "conflicting_approval_name"
            ),
            Err(ServerPhysicalDesignControlError::PhysicalIndexNameConflict(
                _
            ))
        ));
        assert_eq!(current_commit_seq(&fixture.database), before_conflict);
        fixture.close();
    }

    #[test]
    fn columnar_config_and_placement_key_freeze_the_direct_child_namespace() {
        let root = std::env::temp_dir().join(format!(
            "netbadb-server-columnar-config-{}-{}",
            std::process::id(),
            NEXT_PATH.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&root).unwrap();
        let nested = root.join("nested");
        fs::create_dir(&nested).unwrap();
        let config = ServerPhysicalColumnarApplyConfig::new(&nested, true, false).unwrap();
        assert!(config.root().is_absolute());
        assert_eq!(config.root(), fs::canonicalize(&nested).unwrap());
        assert!(config.allow_snapshot());
        assert!(!config.allow_incremental());

        for valid in ["events_amount", "events-amount", "events.v1", "A1"] {
            let key = ServerPhysicalColumnarPlacementKey::new(valid).unwrap();
            let resolved = config.resolve(&key).unwrap();
            assert_eq!(resolved.parent(), Some(config.root()));
            assert_eq!(
                resolved.file_name().and_then(|name| name.to_str()),
                Some(valid)
            );
        }
        assert!(matches!(
            ServerPhysicalColumnarPlacementKey::new(""),
            Err(ServerPhysicalColumnarPlacementKeyError::Empty)
        ));
        for invalid in [".", "..", ".hidden", "a/b", "a\\b", "../x", "x y"] {
            assert!(matches!(
                ServerPhysicalColumnarPlacementKey::new(invalid),
                Err(ServerPhysicalColumnarPlacementKeyError::InvalidCharacter { .. })
            ));
        }
        assert!(matches!(
            ServerPhysicalColumnarPlacementKey::new("a".repeat(129)),
            Err(ServerPhysicalColumnarPlacementKeyError::TooLong { bytes: 129 })
        ));
        assert!(matches!(
            ServerPhysicalColumnarApplyConfig::new(&nested, false, false),
            Err(ServerPhysicalColumnarApplyConfigError::NoModesEnabled)
        ));
        assert!(matches!(
            ServerPhysicalColumnarApplyConfig::new(root.join("missing"), true, false),
            Err(ServerPhysicalColumnarApplyConfigError::RootResolution { .. })
        ));
        let file = root.join("file");
        fs::write(&file, b"not a directory").unwrap();
        assert!(matches!(
            ServerPhysicalColumnarApplyConfig::new(&file, true, false),
            Err(ServerPhysicalColumnarApplyConfigError::RootNotDirectory { .. })
        ));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn columnar_control_error_preserves_recovery_required_source_chain() {
        let error = ServerPhysicalColumnarDesignControlError::Apply(Box::new(
            PhysicalColumnarDesignApplyError::Database(DatabaseError::from(
                ProjectionCatalogError::RecoveryRequired {
                    projection_id: ColumnarProjectionId(7),
                    operation: "publish test artifact",
                    detail: "ambiguous test result".into(),
                },
            )),
        ));
        let core = error.source().expect("Server wraps Core apply error");
        let database = core.source().expect("Core wraps Database error");
        let catalog = database
            .source()
            .expect("Database wraps Projection Catalog error");
        assert!(matches!(
            catalog.downcast_ref::<ProjectionCatalogError>(),
            Some(ProjectionCatalogError::RecoveryRequired { .. })
        ));
    }

    #[test]
    fn snapshot_columnar_proposal_apply_retry_and_occupancy_delegate_to_core() {
        let mut fixture = Fixture::create("columnar-snapshot");
        let placements = fixture.root.join("placements");
        fs::create_dir(&placements).unwrap();
        let mut runtime = columnar_runtime(&placements);
        record_columnar_candidate(&mut fixture.database, &mut runtime);
        let status_before = runtime.status();
        let commit_before = current_commit_seq(&fixture.database);
        let proposal = propose_columnar(
            &mut fixture.database,
            &mut runtime,
            PhysicalColumnarDesignMode::Snapshot,
            "events.v1",
        )
        .expect("create Snapshot proposal");
        let covered_proposal = propose_columnar(
            &mut fixture.database,
            &mut runtime,
            PhysicalColumnarDesignMode::Snapshot,
            "events-v2",
        )
        .expect("create second unreserved proposal");
        assert!(!placements.join("events.v1").exists());
        assert_eq!(runtime.status(), status_before);
        assert_eq!(current_commit_seq(&fixture.database), commit_before);
        assert_eq!(proposal.candidate(), &columnar_candidate());
        assert_eq!(proposal.placement().as_str(), "events.v1");
        assert_eq!(proposal.change_stream_generation(), None);
        let debug = format!("{proposal:?}");
        assert!(!debug.contains(placements.to_string_lossy().as_ref()));
        assert!(!debug.contains("origin"));
        assert!(!debug.contains("incarnation"));

        fs::write(placements.join("occupied"), b"unregistered").unwrap();
        assert!(matches!(
            propose_columnar(
                &mut fixture.database,
                &mut runtime,
                PhysicalColumnarDesignMode::Snapshot,
                "occupied"
            ),
            Err(ServerPhysicalColumnarDesignControlError::PlacementOccupied { .. })
        ));
        fs::create_dir(placements.join("occupied-dir")).unwrap();
        assert!(matches!(
            propose_columnar(
                &mut fixture.database,
                &mut runtime,
                PhysicalColumnarDesignMode::Snapshot,
                "occupied-dir"
            ),
            Err(ServerPhysicalColumnarDesignControlError::PlacementOccupied { .. })
        ));

        let created = apply_columnar(&mut fixture.database, &mut runtime, proposal.clone())
            .expect("apply Snapshot proposal");
        let projection_id = match created.outcome {
            PhysicalColumnarDesignApplyOutcome::Created { projection_id } => projection_id,
            other => panic!("unexpected apply outcome: {other:?}"),
        };
        assert_eq!(created.placement.as_str(), "events.v1");
        assert_eq!(current_commit_seq(&fixture.database), commit_before);
        assert_eq!(runtime.status(), status_before);
        assert_eq!(fixture.database.inspect_columnar_projections().len(), 1);

        let retry = apply_columnar(&mut fixture.database, &mut runtime, proposal)
            .expect("retry exact Snapshot proposal");
        assert_eq!(
            retry.outcome,
            PhysicalColumnarDesignApplyOutcome::AlreadyApplied { projection_id }
        );
        assert_eq!(
            apply_columnar(&mut fixture.database, &mut runtime, covered_proposal)
                .expect("current coverage wins over second placement")
                .outcome,
            PhysicalColumnarDesignApplyOutcome::AlreadyCovered
        );
        assert!(!placements.join("events-v2").exists());
        fixture.close();
    }

    #[test]
    fn columnar_apply_rechecks_occupancy_mode_root_and_runtime_before_core() {
        let mut fixture = Fixture::create("columnar-revalidation");
        let placements = fixture.root.join("placements");
        fs::create_dir(&placements).unwrap();
        let snapshot_only =
            ServerPhysicalColumnarApplyConfig::new(&placements, true, false).unwrap();
        let next_projection_id = fixture
            .database
            .inspect_columnar_projection_catalog()
            .next_projection_id;
        let mut origin =
            ServerPhysicalDesignRuntime::new_with_columnar(config(), Some(snapshot_only.clone()));
        record_columnar_candidate(&mut fixture.database, &mut origin);
        assert!(matches!(
            propose_columnar(
                &mut fixture.database,
                &mut origin,
                PhysicalColumnarDesignMode::Incremental,
                "incremental"
            ),
            Err(ServerPhysicalColumnarDesignControlError::ModeNotAllowed(
                PhysicalColumnarDesignMode::Incremental
            ))
        ));
        let proposal = propose_columnar(
            &mut fixture.database,
            &mut origin,
            PhysicalColumnarDesignMode::Snapshot,
            "appeared",
        )
        .unwrap();
        fs::create_dir(placements.join("appeared")).unwrap();
        assert!(matches!(
            apply_columnar(&mut fixture.database, &mut origin, proposal.clone()),
            Err(ServerPhysicalColumnarDesignControlError::PlacementOccupied { .. })
        ));
        assert!(fixture.database.inspect_columnar_projections().is_empty());

        fs::remove_dir(placements.join("appeared")).unwrap();
        fs::remove_dir(&placements).unwrap();
        assert!(matches!(
            apply_columnar(&mut fixture.database, &mut origin, proposal.clone()),
            Err(ServerPhysicalColumnarDesignControlError::PlacementRootUnavailable(_))
        ));
        fs::create_dir(&placements).unwrap();

        let mut other =
            ServerPhysicalDesignRuntime::new_with_columnar(config(), Some(snapshot_only));
        assert!(matches!(
            apply_columnar(&mut fixture.database, &mut other, proposal.clone()),
            Err(ServerPhysicalColumnarDesignControlError::ProposalRuntimeChanged)
        ));
        drop(origin);
        assert!(matches!(
            apply_columnar(&mut fixture.database, &mut other, proposal),
            Err(ServerPhysicalColumnarDesignControlError::ProposalRuntimeChanged)
        ));
        assert_eq!(
            fixture
                .database
                .inspect_columnar_projection_catalog()
                .next_projection_id,
            next_projection_id
        );
        fixture.close();
    }

    #[test]
    fn incremental_columnar_apply_uses_existing_stream_without_maintaining_it() {
        let mut fixture = Fixture::create("columnar-incremental");
        let cursor = fixture
            .database
            .enable_change_stream(TABLE_ID)
            .expect("explicitly enable Change Stream");
        let placements = fixture.root.join("placements");
        fs::create_dir(&placements).unwrap();
        let mut runtime = columnar_runtime(&placements);
        record_columnar_candidate(&mut fixture.database, &mut runtime);
        let status_before = runtime.status();
        let proposal = propose_columnar(
            &mut fixture.database,
            &mut runtime,
            PhysicalColumnarDesignMode::Incremental,
            "events-incremental",
        )
        .expect("create Incremental proposal");
        let conflicting = propose_columnar(
            &mut fixture.database,
            &mut runtime,
            PhysicalColumnarDesignMode::Snapshot,
            "events-incremental",
        )
        .expect("create same-placement Snapshot proposal before mutation");
        assert_eq!(proposal.change_stream_generation(), Some(cursor.generation));
        assert!(!placements.join("events-incremental").exists());
        let report = apply_columnar(&mut fixture.database, &mut runtime, proposal)
            .expect("apply Incremental proposal");
        assert!(matches!(
            report.outcome,
            PhysicalColumnarDesignApplyOutcome::Created { .. }
        ));
        assert_eq!(runtime.status(), status_before);
        assert_eq!(fixture.database.inspect_columnar_projections().len(), 1);
        assert!(matches!(
            apply_columnar(&mut fixture.database, &mut runtime, conflicting),
            Err(ServerPhysicalColumnarDesignControlError::Apply(error))
                if matches!(
                    error.as_ref(),
                    PhysicalColumnarDesignApplyError::ProjectionLocationConflict { .. }
                )
        ));
        fixture
            .database
            .execute("INSERT INTO events (id, category) VALUES (2, 8)")
            .expect("commit DML without automatic projection advance");
        assert!(matches!(
            fixture.database.inspect_columnar_projections()[0].health,
            netbadb_core::ColumnarProjectionHealth::Lagging
        ));
        fixture.close();
    }

    #[cfg(unix)]
    #[test]
    fn columnar_proposal_rejects_registered_and_dangling_symlink_targets() {
        use std::os::unix::fs::symlink;

        let mut fixture = Fixture::create("columnar-symlink");
        let placements = fixture.root.join("placements");
        fs::create_dir(&placements).unwrap();
        let mut runtime = columnar_runtime(&placements);
        record_columnar_candidate(&mut fixture.database, &mut runtime);
        fs::create_dir(fixture.root.join("elsewhere")).unwrap();
        symlink(fixture.root.join("elsewhere"), placements.join("symlink")).unwrap();
        assert!(matches!(
            propose_columnar(
                &mut fixture.database,
                &mut runtime,
                PhysicalColumnarDesignMode::Snapshot,
                "symlink"
            ),
            Err(ServerPhysicalColumnarDesignControlError::PlacementOccupied { .. })
        ));
        symlink(
            fixture.root.join("missing-target"),
            placements.join("dangling"),
        )
        .unwrap();
        assert!(matches!(
            propose_columnar(
                &mut fixture.database,
                &mut runtime,
                PhysicalColumnarDesignMode::Snapshot,
                "dangling"
            ),
            Err(ServerPhysicalColumnarDesignControlError::PlacementOccupied { .. })
        ));
        fixture.close();
    }

    #[test]
    fn index_receipts_cover_programmatic_retry_and_local_operator_sources() {
        let mut fixture = Fixture::create("index-receipts");
        let receipt_config = ServerPhysicalDesignMutationReceiptConfig::new(
            fixture.root.join("physical-design.nbmr"),
            1_000_000,
        )
        .unwrap();
        let mut runtime = ServerPhysicalDesignRuntime::new_with_mutation_receipts(
            config(),
            None,
            Some(receipt_config),
            &fixture.database,
        )
        .unwrap();
        record_candidate(&mut fixture.database, &mut runtime);
        let status_before = runtime.status();
        let proposal = propose(&mut fixture.database, &mut runtime).unwrap();
        let created = apply(
            &mut fixture.database,
            &mut runtime,
            proposal.clone(),
            "events_category_idx",
        )
        .unwrap();
        let PhysicalIndexDesignApplyOutcome::Created { index_id } = created.outcome else {
            panic!("expected created index");
        };
        let repeated = apply(
            &mut fixture.database,
            &mut runtime,
            proposal,
            "events_category_idx",
        )
        .unwrap();
        assert_eq!(
            repeated.outcome,
            PhysicalIndexDesignApplyOutcome::AlreadyApplied { index_id }
        );
        let evidence_epoch = runtime.evidence.epoch();
        let operator = apply_approved(
            &mut fixture.database,
            &mut runtime,
            true,
            evidence_epoch,
            "events_category_idx_other",
        )
        .unwrap();
        assert_eq!(
            operator.outcome,
            ServerApprovedPhysicalIndexApplyOutcome::AlreadyCovered
        );

        let page = receipts(&mut fixture.database, &mut runtime, None, 128).unwrap();
        assert_eq!(page.receipts.len(), 3);
        assert_eq!(page.receipts[0].id.0, 1);
        assert_eq!(page.receipts[1].id.0, 2);
        assert_eq!(page.receipts[2].id.0, 3);
        assert_eq!(
            page.receipts[0].source,
            ServerPhysicalDesignMutationSource::Programmatic
        );
        assert_eq!(
            page.receipts[0].outcome,
            ServerPhysicalDesignMutationReceiptOutcome::CreatedIndex { index_id }
        );
        assert_eq!(
            page.receipts[1].outcome,
            ServerPhysicalDesignMutationReceiptOutcome::AlreadyAppliedIndex { index_id }
        );
        assert_eq!(
            page.receipts[2].source,
            ServerPhysicalDesignMutationSource::LocalOperator
        );
        assert_eq!(
            page.receipts[2].outcome,
            ServerPhysicalDesignMutationReceiptOutcome::AlreadyCovered
        );
        assert_eq!(runtime.status(), status_before);
        assert!(matches!(
            page.receipts[0].target,
            crate::ServerPhysicalDesignMutationTarget::Index { .. }
        ));
        fixture.close();
    }

    #[test]
    fn receipt_namespace_survives_evidence_rotation_and_runtime_recreation() {
        let mut fixture = Fixture::create("receipt-namespace-lifetime");
        let receipt_config = ServerPhysicalDesignMutationReceiptConfig::new(
            fixture.root.join("physical-design.nbmr"),
            1_000_000,
        )
        .unwrap();
        let database_before = current_commit_seq(&fixture.database);
        let mut runtime = ServerPhysicalDesignRuntime::new_with_mutation_receipts(
            config(),
            None,
            Some(receipt_config.clone()),
            &fixture.database,
        )
        .unwrap();
        let old_runtime = Arc::downgrade(&runtime.identity);
        let before = receipt_status(&mut fixture.database, &mut runtime).unwrap();
        let epoch = runtime.evidence.epoch();
        runtime.evidence.rotate_window().unwrap();
        assert_ne!(runtime.evidence.epoch(), epoch);
        assert_eq!(
            receipt_status(&mut fixture.database, &mut runtime)
                .unwrap()
                .journal_incarnation,
            before.journal_incarnation
        );
        drop(runtime);
        assert!(old_runtime.upgrade().is_none());

        let mut reopened = ServerPhysicalDesignRuntime::new_with_mutation_receipts(
            config(),
            None,
            Some(receipt_config),
            &fixture.database,
        )
        .unwrap();
        assert_eq!(
            receipt_status(&mut fixture.database, &mut reopened)
                .unwrap()
                .journal_incarnation,
            before.journal_incarnation
        );
        assert_eq!(current_commit_seq(&fixture.database), database_before);
        drop(reopened);
        fixture.close();
    }

    #[test]
    fn columnar_receipts_cover_both_modes_without_exposing_recovery_paths() {
        for (name, mode) in [
            ("receipt-snapshot", PhysicalColumnarDesignMode::Snapshot),
            (
                "receipt-incremental",
                PhysicalColumnarDesignMode::Incremental,
            ),
        ] {
            let mut fixture = Fixture::create(name);
            let placements = fixture.root.join("private-placements");
            fs::create_dir(&placements).unwrap();
            let receipt_config = ServerPhysicalDesignMutationReceiptConfig::new(
                fixture.root.join("physical-design.nbmr"),
                1_000_000,
            )
            .unwrap();
            let columnar = ServerPhysicalColumnarApplyConfig::new(&placements, true, true).unwrap();
            let mut runtime = ServerPhysicalDesignRuntime::new_with_mutation_receipts(
                config(),
                Some(columnar),
                Some(receipt_config),
                &fixture.database,
            )
            .unwrap();
            if mode == PhysicalColumnarDesignMode::Incremental {
                fixture.database.enable_change_stream(TABLE_ID).unwrap();
            }
            record_columnar_candidate(&mut fixture.database, &mut runtime);
            let proposal = propose_columnar(
                &mut fixture.database,
                &mut runtime,
                mode,
                "events-reporting",
            )
            .unwrap();
            let report = apply_columnar(&mut fixture.database, &mut runtime, proposal).unwrap();
            let projection_id = match report.outcome {
                PhysicalColumnarDesignApplyOutcome::Created { projection_id } => projection_id,
                _ => panic!("expected created Columnar projection"),
            };
            let page = receipts(&mut fixture.database, &mut runtime, None, 1).unwrap();
            assert_eq!(page.receipts.len(), 1);
            assert_eq!(
                page.receipts[0].outcome,
                ServerPhysicalDesignMutationReceiptOutcome::CreatedColumnar { projection_id }
            );
            assert!(matches!(
                &page.receipts[0].target,
                crate::ServerPhysicalDesignMutationTarget::Columnar {
                    mode: receipt_mode,
                    placement,
                    ..
                } if *receipt_mode == mode && placement.as_str() == "events-reporting"
            ));
            assert!(!format!("{:?}", page.receipts[0]).contains("private-placements"));
            fixture.close();
        }
    }

    #[test]
    fn receipt_reads_are_bounded_paginated_and_side_effect_free() {
        let mut fixture = Fixture::create("receipt-pages");
        let receipt_path = fixture.root.join("physical-design.nbmr");
        let receipt_config =
            ServerPhysicalDesignMutationReceiptConfig::new(&receipt_path, 1_000_000).unwrap();
        let mut runtime = ServerPhysicalDesignRuntime::new_with_mutation_receipts(
            config(),
            None,
            Some(receipt_config),
            &fixture.database,
        )
        .unwrap();
        record_candidate(&mut fixture.database, &mut runtime);
        let proposal = propose(&mut fixture.database, &mut runtime).unwrap();
        for suffix in 0..130 {
            apply(
                &mut fixture.database,
                &mut runtime,
                proposal.clone(),
                &format!("events_category_idx_{suffix}"),
            )
            .unwrap();
        }
        let bytes_before = fs::read(&receipt_path).unwrap();
        let first = receipts(&mut fixture.database, &mut runtime, None, 128).unwrap();
        assert_eq!(first.receipts.len(), 128);
        assert_eq!(first.receipts[0].id.0, 1);
        assert_eq!(first.next_after, Some(first.receipts[127].id));
        assert_eq!(
            receipts(&mut fixture.database, &mut runtime, None, 128).unwrap(),
            first
        );
        let second = receipts(&mut fixture.database, &mut runtime, first.next_after, 128).unwrap();
        assert_eq!(second.receipts.len(), 2);
        assert_eq!(second.receipts[0].id.0, 129);
        assert_eq!(second.receipts[1].id.0, 130);
        assert_eq!(second.next_after, None);
        assert_eq!(fs::read(&receipt_path).unwrap(), bytes_before);
        assert!(matches!(
            receipts(&mut fixture.database, &mut runtime, None, 129),
            Err(ServerPhysicalDesignMutationReceiptControlError::InvalidLimit { .. })
        ));
        fixture.close();
    }

    #[test]
    fn begin_failure_prevents_mutation_without_burning_a_receipt_id() {
        let mut fixture = Fixture::create("receipt-begin-failure");
        let receipt_config = ServerPhysicalDesignMutationReceiptConfig::new(
            fixture.root.join("physical-design.nbmr"),
            1_000_000,
        )
        .unwrap();
        let mut runtime = ServerPhysicalDesignRuntime::new_with_mutation_receipts(
            config(),
            None,
            Some(receipt_config),
            &fixture.database,
        )
        .unwrap();
        record_candidate(&mut fixture.database, &mut runtime);
        let proposal = propose(&mut fixture.database, &mut runtime).unwrap();
        let before = current_commit_seq(&fixture.database);
        runtime.fail_next_receipt_begin_before_write();
        assert!(matches!(
            apply(
                &mut fixture.database,
                &mut runtime,
                proposal.clone(),
                "events_category_idx"
            ),
            Err(ServerPhysicalDesignControlError::MutationReceipt(
                ServerPhysicalDesignMutationReceiptControlError::Journal(_)
            ))
        ));
        assert_eq!(current_commit_seq(&fixture.database), before);
        assert_eq!(
            fixture.database.inspect_physical_index_design_name(
                CANDIDATE,
                &IndexName::new("events_category_idx").unwrap()
            ),
            PhysicalIndexDesignNameState::Available
        );
        assert!(
            receipts(&mut fixture.database, &mut runtime, None, 1)
                .unwrap()
                .receipts
                .is_empty()
        );
        assert!(matches!(
            apply(
                &mut fixture.database,
                &mut runtime,
                proposal,
                "events_category_idx"
            ),
            Err(ServerPhysicalDesignControlError::MutationReceipt(
                ServerPhysicalDesignMutationReceiptControlError::RecoveryRequired
            ))
        ));
        assert_eq!(current_commit_seq(&fixture.database), before);
        fixture.close();
    }

    #[test]
    fn ambiguous_index_database_error_after_begin_is_typed_uncertainty() {
        let mut fixture = Fixture::create("ambiguous-index-database-error");
        let receipt_config = ServerPhysicalDesignMutationReceiptConfig::new(
            fixture.root.join("physical-design.nbmr"),
            1_000_000,
        )
        .unwrap();
        let mut runtime = ServerPhysicalDesignRuntime::new_with_mutation_receipts(
            config(),
            None,
            Some(receipt_config),
            &fixture.database,
        )
        .unwrap();
        let receipt_id = runtime
            .begin_index_receipt(
                ServerPhysicalDesignMutationSource::LocalOperator,
                PhysicalDesignEvidenceEpoch(1),
                CANDIDATE,
                IndexName::new("events_category_idx").unwrap(),
            )
            .unwrap();
        let result: Result<(), _> = runtime.finish_index_result(
            receipt_id,
            Err(ServerPhysicalDesignControlError::Apply(Box::new(
                PhysicalIndexDesignApplyError::Database(DatabaseError::UndefinedIndex),
            ))),
            |_| ServerPhysicalDesignMutationReceiptOutcome::Rejected,
        );
        assert!(matches!(
            result,
            Err(ServerPhysicalDesignControlError::PostBeginMutationOutcomeUncertain(source))
                if matches!(
                    *source,
                    ServerPhysicalDesignControlError::Apply(ref error)
                        if matches!(error.as_ref(), PhysicalIndexDesignApplyError::Database(DatabaseError::UndefinedIndex))
                )
        ));
        assert!(
            receipt_status(&mut fixture.database, &mut runtime)
                .unwrap()
                .recovery_required
        );
        fixture.close();
    }

    #[test]
    fn ambiguous_columnar_database_error_after_begin_is_typed_uncertainty() {
        let mut fixture = Fixture::create("ambiguous-columnar-database-error");
        let placements = fixture.root.join("placements");
        fs::create_dir(&placements).unwrap();
        let receipt_config = ServerPhysicalDesignMutationReceiptConfig::new(
            fixture.root.join("physical-design.nbmr"),
            1_000_000,
        )
        .unwrap();
        let columnar = ServerPhysicalColumnarApplyConfig::new(&placements, true, false).unwrap();
        let mut runtime = ServerPhysicalDesignRuntime::new_with_mutation_receipts(
            config(),
            Some(columnar),
            Some(receipt_config),
            &fixture.database,
        )
        .unwrap();
        let candidate = columnar_candidate();
        let receipt_id = runtime
            .begin_columnar_receipt(
                ServerPhysicalDesignMutationSource::LocalOperator,
                PhysicalDesignEvidenceEpoch(1),
                candidate,
                PhysicalColumnarDesignMode::Snapshot,
                ServerPhysicalColumnarPlacementKey::new("events").unwrap(),
                placements.join("events"),
            )
            .unwrap();
        let result: Result<(), _> = runtime.finish_columnar_result(
            receipt_id,
            Err(ServerPhysicalColumnarDesignControlError::Apply(Box::new(
                PhysicalColumnarDesignApplyError::Database(
                    DatabaseError::ColumnarProjectionNotFound(ColumnarProjectionId(99)),
                ),
            ))),
            |_| ServerPhysicalDesignMutationReceiptOutcome::Rejected,
        );
        assert!(matches!(
            result,
            Err(ServerPhysicalColumnarDesignControlError::PostBeginMutationOutcomeUncertain(source))
                if matches!(
                    *source,
                    ServerPhysicalColumnarDesignControlError::Apply(ref error)
                        if matches!(error.as_ref(), PhysicalColumnarDesignApplyError::Database(DatabaseError::ColumnarProjectionNotFound(ColumnarProjectionId(99))))
                )
        ));
        assert!(
            receipt_status(&mut fixture.database, &mut runtime)
                .unwrap()
                .recovery_required
        );
        fixture.close();
    }

    #[test]
    fn begin_full_write_sync_failure_never_calls_core_and_reconciles_not_applied() {
        let mut fixture = Fixture::create("receipt-begin-full-write-sync-failure");
        let receipt_config = ServerPhysicalDesignMutationReceiptConfig::new(
            fixture.root.join("physical-design.nbmr"),
            1_000_000,
        )
        .unwrap();
        let mut runtime = ServerPhysicalDesignRuntime::new_with_mutation_receipts(
            config(),
            None,
            Some(receipt_config.clone()),
            &fixture.database,
        )
        .unwrap();
        record_candidate(&mut fixture.database, &mut runtime);
        let proposal = propose(&mut fixture.database, &mut runtime).unwrap();
        let commit_before = current_commit_seq(&fixture.database);
        runtime.fail_receipt_io_at(TestJournalIoFailure::BeginAfterFullWriteBeforeSync);
        assert!(matches!(
            apply(
                &mut fixture.database,
                &mut runtime,
                proposal,
                "events_category_idx"
            ),
            Err(ServerPhysicalDesignControlError::MutationReceipt(
                ServerPhysicalDesignMutationReceiptControlError::Journal(_)
            ))
        ));
        assert_eq!(current_commit_seq(&fixture.database), commit_before);
        assert!(
            receipt_status(&mut fixture.database, &mut runtime)
                .unwrap()
                .recovery_required
        );
        drop(runtime);

        let mut reopened = ServerPhysicalDesignRuntime::new_with_mutation_receipts(
            config(),
            None,
            Some(receipt_config),
            &fixture.database,
        )
        .unwrap();
        let page = receipts(&mut fixture.database, &mut reopened, None, 1).unwrap();
        assert_eq!(page.receipts.len(), 1);
        assert_eq!(
            page.receipts[0].outcome,
            ServerPhysicalDesignMutationReceiptOutcome::RecoveredNotApplied
        );
        fixture.close();
    }

    #[test]
    fn outcome_partial_write_failure_truncates_tail_and_reconciles_without_reapply() {
        let mut fixture = Fixture::create("receipt-outcome-partial-write");
        let receipt_config = ServerPhysicalDesignMutationReceiptConfig::new(
            fixture.root.join("physical-design.nbmr"),
            1_000_000,
        )
        .unwrap();
        let mut runtime = ServerPhysicalDesignRuntime::new_with_mutation_receipts(
            config(),
            None,
            Some(receipt_config.clone()),
            &fixture.database,
        )
        .unwrap();
        record_candidate(&mut fixture.database, &mut runtime);
        let proposal = propose(&mut fixture.database, &mut runtime).unwrap();
        runtime.fail_receipt_io_at(TestJournalIoFailure::OutcomeAfterPartialWrite);
        assert!(matches!(
            apply(
                &mut fixture.database,
                &mut runtime,
                proposal,
                "events_category_idx"
            ),
            Err(ServerPhysicalDesignControlError::MutationRecoveryRequired(
                _
            ))
        ));
        let index_id = match fixture.database.inspect_physical_index_design_name(
            CANDIDATE,
            &IndexName::new("events_category_idx").unwrap(),
        ) {
            PhysicalIndexDesignNameState::AlreadyApplied { index_id } => index_id,
            state => panic!("expected applied index, got {state:?}"),
        };
        let commit_after_apply = current_commit_seq(&fixture.database);
        drop(runtime);
        let mut reopened = ServerPhysicalDesignRuntime::new_with_mutation_receipts(
            config(),
            None,
            Some(receipt_config),
            &fixture.database,
        )
        .unwrap();
        assert_eq!(current_commit_seq(&fixture.database), commit_after_apply);
        let page = receipts(&mut fixture.database, &mut reopened, None, 2).unwrap();
        assert_eq!(page.receipts.len(), 1);
        assert_eq!(
            page.receipts[0].outcome,
            ServerPhysicalDesignMutationReceiptOutcome::RecoveredAppliedIndex { index_id }
        );
        fixture.close();
    }

    #[test]
    fn outcome_full_write_sync_failure_reopens_existing_outcome_without_duplicate() {
        let mut fixture = Fixture::create("receipt-outcome-full-write-sync-failure");
        let receipt_config = ServerPhysicalDesignMutationReceiptConfig::new(
            fixture.root.join("physical-design.nbmr"),
            1_000_000,
        )
        .unwrap();
        let mut runtime = ServerPhysicalDesignRuntime::new_with_mutation_receipts(
            config(),
            None,
            Some(receipt_config.clone()),
            &fixture.database,
        )
        .unwrap();
        record_candidate(&mut fixture.database, &mut runtime);
        let proposal = propose(&mut fixture.database, &mut runtime).unwrap();
        runtime.fail_receipt_io_at(TestJournalIoFailure::OutcomeAfterFullWriteBeforeSync);
        assert!(matches!(
            apply(
                &mut fixture.database,
                &mut runtime,
                proposal,
                "events_category_idx"
            ),
            Err(ServerPhysicalDesignControlError::MutationRecoveryRequired(
                _
            ))
        ));
        assert!(
            receipt_status(&mut fixture.database, &mut runtime)
                .unwrap()
                .recovery_required
        );
        let bytes_after_failure = fs::read(receipt_config.path()).unwrap();
        drop(runtime);
        let mut reopened = ServerPhysicalDesignRuntime::new_with_mutation_receipts(
            config(),
            None,
            Some(receipt_config.clone()),
            &fixture.database,
        )
        .unwrap();
        let page = receipts(&mut fixture.database, &mut reopened, None, 2).unwrap();
        assert_eq!(page.receipts.len(), 1);
        assert!(matches!(
            page.receipts[0].outcome,
            ServerPhysicalDesignMutationReceiptOutcome::CreatedIndex { .. }
        ));
        assert_eq!(
            fs::read(receipt_config.path()).unwrap(),
            bytes_after_failure
        );
        fixture.close();
    }

    #[test]
    fn outcome_failure_gates_mutations_and_restart_reconciles_created_index() {
        let mut fixture = Fixture::create("receipt-outcome-failure");
        let receipt_config = ServerPhysicalDesignMutationReceiptConfig::new(
            fixture.root.join("physical-design.nbmr"),
            1_000_000,
        )
        .unwrap();
        let mut runtime = ServerPhysicalDesignRuntime::new_with_mutation_receipts(
            config(),
            None,
            Some(receipt_config.clone()),
            &fixture.database,
        )
        .unwrap();
        let journal_incarnation = receipt_status(&mut fixture.database, &mut runtime)
            .unwrap()
            .journal_incarnation;
        record_candidate(&mut fixture.database, &mut runtime);
        let proposal = propose(&mut fixture.database, &mut runtime).unwrap();
        runtime.fail_next_receipt_outcome_before_write();
        assert!(matches!(
            apply(
                &mut fixture.database,
                &mut runtime,
                proposal.clone(),
                "events_category_idx"
            ),
            Err(ServerPhysicalDesignControlError::MutationRecoveryRequired(
                ServerPhysicalDesignMutationReceiptControlError::Journal(_)
            ))
        ));
        let index_id = match fixture.database.inspect_physical_index_design_name(
            CANDIDATE,
            &IndexName::new("events_category_idx").unwrap(),
        ) {
            PhysicalIndexDesignNameState::AlreadyApplied { index_id } => index_id,
            state => panic!("expected created index after receipt failure, got {state:?}"),
        };
        assert_eq!(
            receipts(&mut fixture.database, &mut runtime, None, 1)
                .unwrap()
                .receipts[0]
                .outcome,
            ServerPhysicalDesignMutationReceiptOutcome::Pending
        );
        let bytes_before_reads = fs::read(receipt_config.path()).unwrap();
        let status = receipt_status(&mut fixture.database, &mut runtime).unwrap();
        assert_eq!(status.journal_incarnation, journal_incarnation);
        assert!(status.recovery_required);
        assert_eq!(
            status.latest_receipt_id,
            Some(ServerPhysicalDesignMutationReceiptId(1))
        );
        let scoped = scoped_receipts(&mut fixture.database, &mut runtime, None, 1).unwrap();
        assert_eq!(scoped.journal_incarnation, journal_incarnation);
        assert_eq!(
            scoped.receipts[0].outcome,
            ServerPhysicalDesignMutationReceiptOutcome::Pending
        );
        assert_eq!(fs::read(receipt_config.path()).unwrap(), bytes_before_reads);
        assert!(matches!(
            apply(
                &mut fixture.database,
                &mut runtime,
                proposal,
                "events_category_idx"
            ),
            Err(ServerPhysicalDesignControlError::MutationReceipt(
                ServerPhysicalDesignMutationReceiptControlError::RecoveryRequired
            ))
        ));
        drop(runtime);

        let mut reopened = ServerPhysicalDesignRuntime::new_with_mutation_receipts(
            config(),
            None,
            Some(receipt_config),
            &fixture.database,
        )
        .unwrap();
        let page = receipts(&mut fixture.database, &mut reopened, None, 1).unwrap();
        let reopened_status = receipt_status(&mut fixture.database, &mut reopened).unwrap();
        assert_eq!(reopened_status.journal_incarnation, journal_incarnation);
        assert!(!reopened_status.recovery_required);
        assert_eq!(
            page.receipts[0].outcome,
            ServerPhysicalDesignMutationReceiptOutcome::RecoveredAppliedIndex { index_id }
        );
        fixture.close();
    }

    #[cfg(unix)]
    #[test]
    fn nbop_outcome_before_write_failure_returns_uncertainty_and_restart_guidance() {
        use std::io::{Read, Write};
        use std::os::unix::net::UnixStream;

        let (design_tx, design_rx) = std::sync::mpsc::channel();
        let design_control = ServerPhysicalDesignControlHandle::new(design_tx);
        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
        let worker = std::thread::spawn(move || {
            let mut fixture = Fixture::create("nbop-receipt-outcome-failure");
            let receipt_config = ServerPhysicalDesignMutationReceiptConfig::new(
                fixture.root.join("physical-design.nbmr"),
                1_000_000,
            )
            .unwrap();
            let mut runtime = ServerPhysicalDesignRuntime::new_with_mutation_receipts(
                config(),
                None,
                Some(receipt_config),
                &fixture.database,
            )
            .unwrap();
            record_candidate(&mut fixture.database, &mut runtime);
            runtime.fail_next_receipt_outcome_before_write();
            ready_tx.send(runtime.evidence.epoch().0).unwrap();
            let request = design_rx.recv().unwrap();
            let ServerPhysicalDesignControlRequest::ApplyApprovedIndex {
                runtime_token_matches,
                expected_evidence_epoch,
                candidate,
                index_name,
                reply,
            } = request
            else {
                panic!("expected one approved Index command");
            };
            runtime.handle(
                &mut fixture.database,
                ServerPhysicalDesignWorkerCommand::ApplyApprovedIndex {
                    runtime_token_matches,
                    expected_evidence_epoch,
                    candidate,
                    index_name: index_name.clone(),
                    reply,
                },
            );
            assert!(matches!(
                fixture
                    .database
                    .inspect_physical_index_design_name(candidate, &index_name),
                PhysicalIndexDesignNameState::AlreadyApplied { .. }
            ));
            assert!(
                runtime
                    .mutation_receipts
                    .as_ref()
                    .unwrap()
                    .status()
                    .recovery_required
            );
            fixture.close();
        });
        let evidence_epoch = ready_rx.recv().unwrap();

        let (adaptive_tx, _adaptive_rx) = std::sync::mpsc::channel();
        let adaptive = crate::ServerAdaptiveControlHandle::new(adaptive_tx);
        let (mut client, mut server) = UnixStream::pair().unwrap();
        let listener = std::thread::spawn(move || {
            crate::operator::serve_operator_connection_with_capabilities(
                &mut server,
                &adaptive,
                &design_control,
                crate::operator::OperatorListenerPolicy::new(
                    true,
                    false,
                    false,
                    None,
                    Some(
                        crate::operator::OperatorPhysicalDesignRuntimeToken::from_bytes([0x11; 16]),
                    ),
                ),
            )
            .unwrap();
        });
        let payload = format!(
            "{{\"request_id\":1,\"operation\":{{\"type\":\"apply_physical_index\",\"expected_runtime_token\":\"{}\",\"expected_evidence_epoch\":{},\"table_id\":{},\"column_id\":2,\"index_name\":\"events_category_idx\"}}}}",
            "11".repeat(16),
            evidence_epoch,
            TABLE_ID.0
        );
        let mut frame = b"NBOP\0\x05\0\0".to_vec();
        frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        frame.extend_from_slice(payload.as_bytes());
        client.write_all(&frame).unwrap();
        let mut header = [0_u8; 12];
        client.read_exact(&mut header).unwrap();
        assert_eq!(&header[..8], b"NBOP\0\x05\0\0");
        let length = u32::from_be_bytes(header[8..12].try_into().unwrap()) as usize;
        let mut response = vec![0_u8; length];
        client.read_exact(&mut response).unwrap();
        let response: serde_json::Value = serde_json::from_slice(&response).unwrap();
        assert_eq!(response["outcome"], "error");
        assert_eq!(
            response["error"]["code"],
            "physical_design_mutation_outcome_uncertain"
        );
        assert!(response["error"]["receipt"].is_object());
        assert!(
            response["error"]["message"]
                .as_str()
                .unwrap()
                .contains("restart/reopen")
        );
        assert!(!response.to_string().contains("physical-design.nbmr"));
        listener.join().unwrap();
        worker.join().unwrap();
    }

    #[test]
    fn columnar_outcome_failure_reconciles_after_database_reopen() {
        let mut fixture = Fixture::create("columnar-receipt-outcome-failure");
        let placements = fixture.root.join("private-placements");
        fs::create_dir(&placements).unwrap();
        let receipt_config = ServerPhysicalDesignMutationReceiptConfig::new(
            fixture.root.join("physical-design.nbmr"),
            1_000_000,
        )
        .unwrap();
        let columnar = ServerPhysicalColumnarApplyConfig::new(&placements, true, false).unwrap();
        let mut runtime = ServerPhysicalDesignRuntime::new_with_mutation_receipts(
            config(),
            Some(columnar.clone()),
            Some(receipt_config.clone()),
            &fixture.database,
        )
        .unwrap();
        record_columnar_candidate(&mut fixture.database, &mut runtime);
        let proposal = propose_columnar(
            &mut fixture.database,
            &mut runtime,
            PhysicalColumnarDesignMode::Snapshot,
            "events-reporting",
        )
        .unwrap();
        runtime.fail_next_receipt_outcome_before_write();
        assert!(matches!(
            apply_columnar(&mut fixture.database, &mut runtime, proposal),
            Err(
                ServerPhysicalColumnarDesignControlError::MutationRecoveryRequired(
                    ServerPhysicalDesignMutationReceiptControlError::Journal(_)
                )
            )
        ));
        let projection_id = fixture.database.inspect_columnar_projections()[0]
            .projection_id
            .unwrap();
        drop(runtime);
        let Fixture { root, database } = fixture;
        database.close().unwrap();

        let mut database = Database::open_catalog(root.join("catalog")).unwrap();
        let mut runtime = ServerPhysicalDesignRuntime::new_with_mutation_receipts(
            config(),
            Some(columnar),
            Some(receipt_config),
            &database,
        )
        .unwrap();
        let page = receipts(&mut database, &mut runtime, None, 1).unwrap();
        assert_eq!(
            page.receipts[0].outcome,
            ServerPhysicalDesignMutationReceiptOutcome::RecoveredAppliedColumnar { projection_id }
        );
        drop(runtime);
        database.close().unwrap();
        fs::remove_dir_all(root).unwrap();
    }
}
