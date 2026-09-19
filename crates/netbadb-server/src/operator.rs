use std::error::Error;
use std::fmt;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc::Sender;
use std::time::Duration;

use netbadb_core::{
    AdaptiveEvidencePoolHealth, AdaptiveEvidenceRecordError, AdaptiveEvidenceRecordOutcome,
    AdaptiveEvidenceRotationError, AdaptiveEvidenceRotationReport, AdaptiveEvidenceWindowEpoch,
    AutomaticEvidenceRenewalReason, AutomaticOrchestrationStopReason, AutomaticSchedulerDelayClass,
    AutomaticSchedulerFault, AutomaticSchedulerGate, DatabaseError, PhysicalColumnarCandidate,
    PhysicalColumnarDesignApplyError, PhysicalColumnarDesignMode,
    PhysicalColumnarDesignProposalError, PhysicalColumnarDesignProposalStaleReason,
    PhysicalColumnarRecommendationInspection, PhysicalDesignAdvisorError,
    PhysicalDesignAdvisorReport, PhysicalDesignCandidateDecision,
    PhysicalDesignEvidenceRecordError, PhysicalDesignEvidenceRecordOutcome,
    PhysicalDesignEvidenceSummary, PhysicalDesignNoActionReason, PhysicalIndexCandidate,
    PhysicalIndexDesignApplyError, PhysicalIndexDesignProposalError,
    PhysicalIndexRecommendationInspection, ProjectionCatalogError,
};
use netbadb_types::{ColumnId, IndexName, TableId};
use serde::{Deserialize, Serialize};

use crate::physical_design::{
    ServerApprovedPhysicalColumnarApplyOutcome, ServerApprovedPhysicalColumnarApplyReport,
    ServerApprovedPhysicalIndexApplyOutcome, ServerApprovedPhysicalIndexApplyReport,
    ServerPhysicalColumnarApplyCapabilities,
};
use crate::{
    ServerAdaptiveControlError, ServerAdaptiveControlHandle, ServerAdaptiveMode,
    ServerAdaptiveStatus, ServerPhysicalColumnarDesignControlError,
    ServerPhysicalDesignControlError, ServerPhysicalDesignControlHandle,
    ServerPhysicalDesignRotationReport, ServerPhysicalDesignStatus,
};
use crate::{
    ServerPhysicalDesignMutationReceipt, ServerPhysicalDesignMutationReceiptControlError,
    ServerPhysicalDesignMutationReceiptCursor, ServerPhysicalDesignMutationReceiptJournalError,
    ServerPhysicalDesignMutationReceiptJournalIncarnation,
    ServerPhysicalDesignMutationReceiptOutcome, ServerPhysicalDesignMutationReceiptReference,
    ServerPhysicalDesignMutationReceiptScopedPage, ServerPhysicalDesignMutationReceiptStatus,
    ServerPhysicalDesignMutationSource, ServerPhysicalDesignMutationTarget,
};

pub const OPERATOR_PROTOCOL_VERSION: u16 = 5;
pub const MAX_OPERATOR_PAYLOAD_BYTES: u32 = 64 * 1024;

const OPERATOR_MAGIC: [u8; 4] = *b"NBOP";
const OPERATOR_HEADER_BYTES: usize = 12;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OperatorPhysicalDesignRuntimeToken([u8; 16]);

impl OperatorPhysicalDesignRuntimeToken {
    #[cfg(test)]
    pub(crate) const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    fn generate() -> Result<Self, getrandom::Error> {
        let mut bytes = [0_u8; 16];
        getrandom::getrandom(&mut bytes)?;
        Ok(Self(bytes))
    }

    fn parse(value: &str) -> Option<Self> {
        decode_lower_hex_16(value).map(Self)
    }

    fn encode(self) -> String {
        encode_lower_hex_16(self.0)
    }
}

fn decode_lower_hex_16(value: &str) -> Option<[u8; 16]> {
    if value.len() != 32
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return None;
    }
    let mut bytes = [0_u8; 16];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        bytes[index] = hex_nibble(pair[0])? << 4 | hex_nibble(pair[1])?;
    }
    Some(bytes)
}

fn encode_lower_hex_16(bytes: [u8; 16]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(32);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

const fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

/// Runtime-ready local operator configuration resolved from the deployment
/// manifest. The socket path is absolute and its parent already exists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerOperatorConfig {
    unix_socket: PathBuf,
    io_timeout: Duration,
    allow_physical_index_apply: bool,
    allow_physical_columnar_apply: bool,
    allow_physical_design_receipt_read: bool,
}

impl ServerOperatorConfig {
    #[cfg(test)]
    pub(crate) fn new(
        unix_socket: PathBuf,
        io_timeout: Duration,
        allow_physical_index_apply: bool,
    ) -> Result<Self, ServerOperatorConfigError> {
        Self::new_with_columnar(unix_socket, io_timeout, allow_physical_index_apply, false)
    }

    #[cfg(test)]
    pub(crate) fn new_with_columnar(
        unix_socket: PathBuf,
        io_timeout: Duration,
        allow_physical_index_apply: bool,
        allow_physical_columnar_apply: bool,
    ) -> Result<Self, ServerOperatorConfigError> {
        Self::new_with_receipt_read(
            unix_socket,
            io_timeout,
            allow_physical_index_apply,
            allow_physical_columnar_apply,
            false,
        )
    }

    pub(crate) fn new_with_receipt_read(
        unix_socket: PathBuf,
        io_timeout: Duration,
        allow_physical_index_apply: bool,
        allow_physical_columnar_apply: bool,
        allow_physical_design_receipt_read: bool,
    ) -> Result<Self, ServerOperatorConfigError> {
        if io_timeout.is_zero() {
            return Err(ServerOperatorConfigError::ZeroIoTimeout);
        }
        Ok(Self {
            unix_socket,
            io_timeout,
            allow_physical_index_apply,
            allow_physical_columnar_apply,
            allow_physical_design_receipt_read,
        })
    }

    #[must_use]
    pub fn unix_socket(&self) -> &Path {
        &self.unix_socket
    }

    #[must_use]
    pub const fn io_timeout(&self) -> Duration {
        self.io_timeout
    }

    #[must_use]
    pub const fn allow_physical_index_apply(&self) -> bool {
        self.allow_physical_index_apply
    }

    #[must_use]
    pub const fn allow_physical_columnar_apply(&self) -> bool {
        self.allow_physical_columnar_apply
    }

    #[must_use]
    pub const fn allow_physical_design_receipt_read(&self) -> bool {
        self.allow_physical_design_receipt_read
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerOperatorConfigError {
    ZeroIoTimeout,
}

impl fmt::Display for ServerOperatorConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroIoTimeout => formatter.write_str("operator I/O timeout must be nonzero"),
        }
    }
}

impl Error for ServerOperatorConfigError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorAdaptiveModeV5 {
    FeedbackOnly,
    Driven,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorEvidencePoolHealthV5 {
    Healthy,
    RotationRecommended,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorEvidenceRecordOutcomeV5 {
    Recorded,
    SchemaRotated,
    RecordedWithCapacityRejection,
    SchemaRotatedWithCapacityRejection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorEvidenceRecordErrorV5 {
    GlobalVisibilityRequired,
    StaleSchemaEvidence,
    OutOfOrderVisibility,
    StaleTargetGenerationEvidence,
    StaleTargetIdentityEvidence,
    RetiredTargetEvidence,
    StaleCalibrationEpochEvidence,
    EvidenceWindowEpochExhausted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorSchedulerDelayClassV5 {
    Normal,
    Idle,
    NoProgress,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorEvidenceRenewalReasonV5 {
    ColumnarPhysicalStateChanged,
    ColumnarEligibilityChanged,
    AuthoritativeLsmLayoutChanged,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorSchedulerFaultV5 {
    MaintenanceEnvelopeExceeded,
    StepFailed,
    ConsumptionOverflow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum OperatorSchedulerGateV5 {
    Open {
        delay_class: OperatorSchedulerDelayClassV5,
    },
    AwaitingTrialProgress {
        window_epoch: u64,
        schema_generation: Option<u64>,
        recorded_reports: u64,
    },
    AwaitingEvidenceRenewal {
        blocked_window_epoch: u64,
        renewal_reason: OperatorEvidenceRenewalReasonV5,
    },
    Faulted {
        fault: OperatorSchedulerFaultV5,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorOrchestrationStopReasonV5 {
    NoReadyWork,
    StepLimitReached,
    ActiveTrial,
    TrialBoundaryResolved,
    EvidenceRenewalRecommended,
    SelectedCandidateDidNotProgress,
    MaintenanceEnvelopeExceeded,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorFeedbackStatusV5 {
    pub eligible_query_count: u64,
    pub record_success_count: u64,
    pub record_error_count: u64,
    pub capacity_rejection_count: u64,
    pub schema_rotation_count: u64,
    pub incomplete_report_count: u64,
    pub counter_overflowed: bool,
    pub last_record_outcome: Option<OperatorEvidenceRecordOutcomeV5>,
    pub last_record_error: Option<OperatorEvidenceRecordErrorV5>,
    pub window_epoch: u64,
    pub schema_generation: Option<u64>,
    pub recorded_reports: u64,
    pub pool_health: OperatorEvidencePoolHealthV5,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorDriverStatusV5 {
    pub scheduler_last_observed_tick: Option<u64>,
    pub scheduler_last_run_tick: Option<u64>,
    pub scheduler_gate: OperatorSchedulerGateV5,
    pub last_submitted_logical_tick: Option<u64>,
    pub tick_pending: bool,
    pub driver_tick_count: u64,
    pub scheduler_tick_count: u64,
    pub scheduler_ran_count: u64,
    pub scheduler_held_count: u64,
    pub scheduler_error_count: u64,
    pub last_orchestration_stop_reason: Option<OperatorOrchestrationStopReasonV5>,
    pub host_clock_exhausted: bool,
    pub counter_overflowed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorAdaptiveStatusV5 {
    pub mode: OperatorAdaptiveModeV5,
    pub feedback: OperatorFeedbackStatusV5,
    pub driver: Option<OperatorDriverStatusV5>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorStatusV5 {
    pub adaptive: Option<OperatorAdaptiveStatusV5>,
    pub physical_design: Option<OperatorPhysicalDesignStatusV5>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalIndexApplyStatusV5 {
    pub enabled: bool,
    pub runtime_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalColumnarApplyStatusV5 {
    pub enabled: bool,
    pub allow_snapshot: bool,
    pub allow_incremental: bool,
    pub runtime_token: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalIndexApplyCapabilityV5 {
    pub enabled: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalColumnarApplyCapabilityV5 {
    pub enabled: bool,
    pub allow_snapshot: bool,
    pub allow_incremental: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorPhysicalDesignRecordOutcomeV5 {
    Recorded,
    SchemaRotated,
    RecordedWithCapacityRejection,
    SchemaRotatedWithCapacityRejection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorPhysicalDesignRecordErrorV5 {
    GlobalVisibilityRequired,
    StaleSchemaEvidence,
    OutOfOrderVisibility,
    EvidenceWindowEpochExhausted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalDesignDiagnosticsV5 {
    pub eligible_query_count: u64,
    pub record_success_count: u64,
    pub record_error_count: u64,
    pub schema_rotation_count: u64,
    pub capacity_rejection_count: u64,
    pub incomplete_report_count: u64,
    pub counter_overflowed: bool,
    pub last_record_outcome: Option<OperatorPhysicalDesignRecordOutcomeV5>,
    pub last_record_error: Option<OperatorPhysicalDesignRecordErrorV5>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalDesignEvidenceLimitsV5 {
    pub max_index_candidates: u64,
    pub max_columnar_candidates: u64,
    pub max_query_shapes_per_candidate: u64,
    pub max_columnar_columns_per_candidate: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalDesignEvidenceStatusV5 {
    pub limits: OperatorPhysicalDesignEvidenceLimitsV5,
    pub epoch: u64,
    pub schema_generation: Option<u64>,
    pub first_global_commit_seq: Option<u64>,
    pub last_global_commit_seq: Option<u64>,
    pub ordering_high_water: Option<u64>,
    pub recorded_reports: u64,
    pub index_candidate_count: u64,
    pub columnar_candidate_count: u64,
    pub capacity_rejections: u64,
    pub discarded_incomplete_reports: u64,
    pub overflowed: bool,
    pub incomplete: bool,
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalDesignStatusV5 {
    pub diagnostics: OperatorPhysicalDesignDiagnosticsV5,
    pub evidence: OperatorPhysicalDesignEvidenceStatusV5,
    pub physical_index_apply: OperatorPhysicalIndexApplyStatusV5,
    pub physical_columnar_apply: OperatorPhysicalColumnarApplyStatusV5,
    pub physical_design_mutation_receipts: OperatorPhysicalDesignMutationReceiptCapabilityV5,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalDesignEvidenceSummaryV5 {
    pub report_count: u64,
    pub distinct_query_shapes: u64,
    pub total_actual_scan_work_units: u64,
    pub total_rows_examined: u64,
    pub overflowed: bool,
    pub incomplete: bool,
    pub truncated: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorPhysicalDesignNoActionReasonV5 {
    BelowMinimumReports,
    BelowMinimumShapeDiversity,
    BelowMinimumActualWork,
    ExistingDesignCovers,
    UnsupportedCurrentLayout,
    IncompleteEvidence,
    CurrentProjectionUnavailable,
    RecommendationLimitReached,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum OperatorPhysicalDesignDecisionV5 {
    Recommend {},
    NoAction {
        reason: OperatorPhysicalDesignNoActionReasonV5,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalIndexCandidateV5 {
    pub table_id: u64,
    pub column_id: u32,
    pub point_report_count: u64,
    pub range_report_count: u64,
    pub evidence: OperatorPhysicalDesignEvidenceSummaryV5,
    pub decision: OperatorPhysicalDesignDecisionV5,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalColumnarCandidateV5 {
    pub table_id: u64,
    pub columns: Vec<u32>,
    pub evidence: OperatorPhysicalDesignEvidenceSummaryV5,
    pub decision: OperatorPhysicalDesignDecisionV5,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalDesignAdvisorReportV5 {
    pub evidence_epoch: u64,
    pub schema_generation: u64,
    pub first_global_commit_seq: u64,
    pub last_global_commit_seq: u64,
    pub recorded_reports: u64,
    pub discarded_incomplete_reports: u64,
    pub overflowed: bool,
    pub incomplete: bool,
    pub index_candidates: Vec<OperatorPhysicalIndexCandidateV5>,
    pub columnar_candidates: Vec<OperatorPhysicalColumnarCandidateV5>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalDesignRecommendationsV5 {
    pub runtime_token: Option<String>,
    pub report: OperatorPhysicalDesignAdvisorReportV5,
}

impl std::ops::Deref for OperatorPhysicalDesignRecommendationsV5 {
    type Target = OperatorPhysicalDesignAdvisorReportV5;

    fn deref(&self) -> &Self::Target {
        &self.report
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
pub enum OperatorPhysicalIndexApplyOutcomeV5 {
    Created { index_id: u64 },
    AlreadyApplied { index_id: u64 },
    AlreadyCovered,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorPhysicalColumnarDesignModeV5 {
    Snapshot,
    Incremental,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum OperatorPhysicalColumnarApplyOutcomeV5 {
    Created { projection_id: u64 },
    AlreadyApplied { projection_id: u64 },
    AlreadyCovered,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalColumnarApplyResultV5 {
    pub table_id: u64,
    pub columns: Vec<u32>,
    pub mode: OperatorPhysicalColumnarDesignModeV5,
    pub placement_key: String,
    pub outcome: OperatorPhysicalColumnarApplyOutcomeV5,
    pub receipt: Option<OperatorPhysicalDesignMutationReceiptRefV5>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalIndexApplyResultV5 {
    pub table_id: u64,
    pub column_id: u32,
    pub index_name: String,
    pub outcome: OperatorPhysicalIndexApplyOutcomeV5,
    pub receipt: Option<OperatorPhysicalDesignMutationReceiptRefV5>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalDesignMutationReceiptRefV5 {
    pub journal_incarnation: String,
    pub receipt_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalDesignMutationReceiptCursorV5 {
    pub journal_incarnation: String,
    pub receipt_id: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalDesignMutationReceiptCapabilityV5 {
    pub read_enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalDesignMutationReceiptStatusV5 {
    pub journal_incarnation: String,
    pub recovery_required: bool,
    pub latest_receipt_id: Option<u64>,
    pub max_receipts_per_read: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorPhysicalDesignMutationReceiptSourceV5 {
    Programmatic,
    LocalOperator,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum OperatorPhysicalDesignMutationReceiptTargetV5 {
    Index {
        table_id: u64,
        column_id: u32,
        index_name: String,
    },
    Columnar {
        table_id: u64,
        columns: Vec<u32>,
        mode: OperatorPhysicalColumnarDesignModeV5,
        placement_key: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
pub enum OperatorPhysicalDesignMutationReceiptOutcomeV5 {
    Pending,
    CreatedIndex { index_id: u64 },
    CreatedColumnar { projection_id: u64 },
    AlreadyAppliedIndex { index_id: u64 },
    AlreadyAppliedColumnar { projection_id: u64 },
    AlreadyCovered,
    Rejected,
    Failed,
    RecoveredAppliedIndex { index_id: u64 },
    RecoveredAppliedColumnar { projection_id: u64 },
    RecoveredNotApplied,
    RecoveredConflict,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalDesignMutationReceiptV5 {
    pub receipt: OperatorPhysicalDesignMutationReceiptRefV5,
    pub source: OperatorPhysicalDesignMutationReceiptSourceV5,
    pub evidence_epoch: u64,
    pub target: OperatorPhysicalDesignMutationReceiptTargetV5,
    pub outcome: OperatorPhysicalDesignMutationReceiptOutcomeV5,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalDesignMutationReceiptPageV5 {
    pub journal_incarnation: String,
    pub receipts: Vec<OperatorPhysicalDesignMutationReceiptV5>,
    pub next_after: Option<OperatorPhysicalDesignMutationReceiptCursorV5>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalDesignRotationV5 {
    pub previous_epoch: u64,
    pub new_epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorEvidenceRotationV5 {
    pub previous_window_epoch: u64,
    pub new_window_epoch: u64,
    pub schema_generation: Option<u64>,
    pub ordering_high_water: Option<u64>,
    pub discarded_target_window_count: u64,
    pub discarded_calibration_epoch_count: u64,
    pub discarded_query_shape_count: u64,
    pub was_incomplete: bool,
    pub was_truncated: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorErrorCodeV5 {
    AdaptiveNotEnabled,
    DriverNotEnabled,
    SchedulerNotFaulted,
    EvidenceWindowChanged,
    EvidenceWindowEpochExhausted,
    PhysicalDesignNotEnabled,
    PhysicalDesignEvidenceEpochChanged,
    PhysicalDesignEvidenceEpochExhausted,
    PhysicalDesignNoEvidence,
    PhysicalDesignStaleSchema,
    PhysicalDesignInconclusiveCapacity,
    PhysicalIndexApplyNotEnabled,
    PhysicalDesignRuntimeChanged,
    InvalidIndexName,
    PhysicalIndexCandidateNotObserved,
    PhysicalIndexNotRecommended,
    PhysicalIndexNameConflict,
    PhysicalIndexApplyFailed,
    PhysicalColumnarApplyNotEnabled,
    InvalidPhysicalColumnarPlacementKey,
    PhysicalColumnarModeNotAllowed,
    PhysicalColumnarPlacementUnavailable,
    PhysicalColumnarPlacementOccupied,
    PhysicalColumnarLocationConflict,
    PhysicalColumnarCandidateNotObserved,
    PhysicalColumnarNotRecommended,
    PhysicalColumnarChangeStreamNotEnabled,
    PhysicalColumnarChangeStreamUnavailable,
    PhysicalColumnarChangeStreamChanged,
    PhysicalColumnarRecoveryRequired,
    PhysicalColumnarApplyFailed,
    PhysicalDesignMutationOutcomeUncertain,
    PhysicalDesignMutationReceiptCapacityExceeded,
    PhysicalDesignMutationReceiptUnavailable,
    PhysicalDesignMutationReceiptReadNotAllowed,
    PhysicalDesignMutationReceiptsNotEnabled,
    InvalidPhysicalDesignMutationReceiptCursor,
    PhysicalDesignMutationReceiptJournalChanged,
    InvalidPhysicalDesignMutationReceiptLimit,
    PhysicalDesignMutationReceiptReadFailed,
    ResponseTooLarge,
    ServerStopped,
    MalformedRequest,
    UnsupportedProtocolVersion,
    RequestTooLarge,
    Internal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorRemoteErrorV5 {
    pub code: OperatorErrorCodeV5,
    pub message: String,
    pub receipt: Option<OperatorPhysicalDesignMutationReceiptRefV5>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OperatorRequestV5 {
    request_id: u64,
    operation: OperatorOperationV5,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum OperatorOperationV5 {
    Status {},
    PhysicalDesignMutationReceiptStatus {},
    PhysicalDesignMutationReceipts {
        after: Option<OperatorPhysicalDesignMutationReceiptCursorV5>,
        limit: u32,
    },
    RotateEvidence {
        expected_window_epoch: u64,
    },
    ResetFaultedScheduler {},
    PhysicalDesignRecommendations {},
    RotatePhysicalDesignEvidence {
        expected_evidence_epoch: u64,
    },
    ApplyPhysicalIndex {
        expected_runtime_token: String,
        expected_evidence_epoch: u64,
        table_id: u64,
        column_id: u32,
        index_name: String,
    },
    ApplyPhysicalColumnar {
        expected_runtime_token: String,
        expected_evidence_epoch: u64,
        table_id: u64,
        columns: Vec<u32>,
        mode: OperatorPhysicalColumnarDesignModeV5,
        placement_key: String,
    },
}

struct OperatorPhysicalIndexApplyInput {
    expected_runtime_token: String,
    expected_evidence_epoch: u64,
    table_id: u64,
    column_id: u32,
    index_name: String,
}

struct OperatorPhysicalColumnarApplyInput {
    expected_runtime_token: String,
    expected_evidence_epoch: u64,
    table_id: u64,
    columns: Vec<u32>,
    mode: OperatorPhysicalColumnarDesignModeV5,
    placement_key: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
enum OperatorResponseV5 {
    Ok {
        request_id: u64,
        result: OperatorResultV5,
    },
    Error {
        request_id: u64,
        error: OperatorRemoteErrorV5,
    },
}

impl OperatorResponseV5 {
    const fn request_id(&self) -> u64 {
        match self {
            Self::Ok { request_id, .. } | Self::Error { request_id, .. } => *request_id,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum OperatorResultV5 {
    Status {
        status: Box<OperatorStatusV5>,
    },
    EvidenceRotated {
        rotation: OperatorEvidenceRotationV5,
    },
    SchedulerReset {},
    PhysicalDesignRecommendations {
        runtime_token: Option<String>,
        report: OperatorPhysicalDesignAdvisorReportV5,
    },
    PhysicalDesignEvidenceRotated {
        rotation: OperatorPhysicalDesignRotationV5,
    },
    PhysicalIndexApplied {
        apply: OperatorPhysicalIndexApplyResultV5,
    },
    PhysicalColumnarApplied {
        apply: OperatorPhysicalColumnarApplyResultV5,
    },
    PhysicalDesignMutationReceiptStatus {
        status: OperatorPhysicalDesignMutationReceiptStatusV5,
    },
    PhysicalDesignMutationReceipts {
        page: OperatorPhysicalDesignMutationReceiptPageV5,
    },
}

#[derive(Debug)]
pub enum OperatorProtocolError {
    Io(io::Error),
    TruncatedHeader,
    TruncatedPayload,
    WrongMagic,
    UnsupportedVersion(u16),
    NonzeroReserved(u16),
    RequestTooLarge(u32),
    PayloadTooLarge(usize),
    InvalidJson(serde_json::Error),
}

impl fmt::Display for OperatorProtocolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => error.fmt(formatter),
            Self::TruncatedHeader => formatter.write_str("truncated NBOP frame header"),
            Self::TruncatedPayload => formatter.write_str("truncated NBOP frame payload"),
            Self::WrongMagic => formatter.write_str("invalid NBOP frame magic"),
            Self::UnsupportedVersion(version) => {
                write!(formatter, "unsupported NBOP protocol version {version}")
            }
            Self::NonzeroReserved(value) => {
                write!(formatter, "NBOP reserved header field is nonzero ({value})")
            }
            Self::RequestTooLarge(length) => write!(
                formatter,
                "NBOP payload length {length} exceeds {MAX_OPERATOR_PAYLOAD_BYTES} bytes"
            ),
            Self::PayloadTooLarge(length) => {
                write!(
                    formatter,
                    "NBOP response payload length {length} is too large"
                )
            }
            Self::InvalidJson(error) => write!(formatter, "invalid NBOP JSON payload: {error}"),
        }
    }
}

impl Error for OperatorProtocolError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::InvalidJson(error) => Some(error),
            _ => None,
        }
    }
}

fn read_frame<T: for<'de> Deserialize<'de>>(
    reader: &mut impl Read,
) -> Result<T, OperatorProtocolError> {
    let mut header = [0_u8; OPERATOR_HEADER_BYTES];
    read_exact_frame(reader, &mut header, OperatorProtocolError::TruncatedHeader)?;
    if header[..4] != OPERATOR_MAGIC {
        return Err(OperatorProtocolError::WrongMagic);
    }
    let version = u16::from_be_bytes([header[4], header[5]]);
    if version != OPERATOR_PROTOCOL_VERSION {
        return Err(OperatorProtocolError::UnsupportedVersion(version));
    }
    let reserved = u16::from_be_bytes([header[6], header[7]]);
    if reserved != 0 {
        return Err(OperatorProtocolError::NonzeroReserved(reserved));
    }
    let length = u32::from_be_bytes([header[8], header[9], header[10], header[11]]);
    if length > MAX_OPERATOR_PAYLOAD_BYTES {
        return Err(OperatorProtocolError::RequestTooLarge(length));
    }
    let mut payload = vec![0_u8; length as usize];
    read_exact_frame(
        reader,
        &mut payload,
        OperatorProtocolError::TruncatedPayload,
    )?;
    serde_json::from_slice(&payload).map_err(OperatorProtocolError::InvalidJson)
}

fn read_exact_frame(
    reader: &mut impl Read,
    bytes: &mut [u8],
    truncated: OperatorProtocolError,
) -> Result<(), OperatorProtocolError> {
    reader.read_exact(bytes).map_err(|error| {
        if error.kind() == io::ErrorKind::UnexpectedEof {
            truncated
        } else {
            OperatorProtocolError::Io(error)
        }
    })
}

fn write_frame<T: Serialize>(
    writer: &mut impl Write,
    value: &T,
) -> Result<(), OperatorProtocolError> {
    let frame = encode_frame(value)?;
    writer
        .write_all(&frame)
        .and_then(|()| writer.flush())
        .map_err(OperatorProtocolError::Io)
}

fn encode_frame<T: Serialize>(value: &T) -> Result<Vec<u8>, OperatorProtocolError> {
    let payload = serde_json::to_vec(value).map_err(OperatorProtocolError::InvalidJson)?;
    if payload.len() > MAX_OPERATOR_PAYLOAD_BYTES as usize {
        return Err(OperatorProtocolError::PayloadTooLarge(payload.len()));
    }
    let length = u32::try_from(payload.len())
        .map_err(|_| OperatorProtocolError::PayloadTooLarge(payload.len()))?;
    let mut header = [0_u8; OPERATOR_HEADER_BYTES];
    header[..4].copy_from_slice(&OPERATOR_MAGIC);
    header[4..6].copy_from_slice(&OPERATOR_PROTOCOL_VERSION.to_be_bytes());
    header[8..12].copy_from_slice(&length.to_be_bytes());
    let mut frame = Vec::with_capacity(OPERATOR_HEADER_BYTES + payload.len());
    frame.extend_from_slice(&header);
    frame.extend_from_slice(&payload);
    Ok(frame)
}

#[derive(Debug)]
pub enum OperatorClientError {
    OperatorNotConfigured,
    UnsupportedPlatform,
    Connect {
        path: PathBuf,
        source: io::Error,
    },
    Configure(io::Error),
    Protocol(OperatorProtocolError),
    RequestIdMismatch {
        expected: u64,
        received: u64,
    },
    UnexpectedResult,
    Remote(OperatorRemoteErrorV5),
    MutationOutcomeUncertain {
        recovery_required: bool,
        receipt: Option<OperatorPhysicalDesignMutationReceiptRefV5>,
        source: Box<OperatorClientError>,
    },
}

impl fmt::Display for OperatorClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OperatorNotConfigured => {
                formatter.write_str("operator plane is not configured in the manifest")
            }
            Self::UnsupportedPlatform => {
                formatter.write_str("the local operator plane requires a Unix platform")
            }
            Self::Connect { path, source } => write!(
                formatter,
                "failed to connect to operator socket `{}`: {source}",
                path.display()
            ),
            Self::Configure(error) => {
                write!(formatter, "failed to configure operator socket: {error}")
            }
            Self::Protocol(error) => error.fmt(formatter),
            Self::RequestIdMismatch { expected, received } => write!(
                formatter,
                "operator response request ID {received} does not match {expected}"
            ),
            Self::UnexpectedResult => formatter.write_str("operator returned an unexpected result"),
            Self::Remote(error) => write!(formatter, "operator request failed: {}", error.message),
            Self::MutationOutcomeUncertain {
                recovery_required, ..
            } => {
                if *recovery_required {
                    formatter.write_str("operator mutation outcome is uncertain because its receipt Outcome may not be durable; restart/reopen the daemon, wait for startup reconciliation, then inspect or retry the same exact logical approval if still needed")
                } else {
                    formatter.write_str("operator mutation outcome is uncertain after command dispatch; the same exact approval may be retried to discover the idempotent outcome")
                }
            }
        }
    }
}

impl Error for OperatorClientError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Connect { source, .. } | Self::Configure(source) => Some(source),
            Self::Protocol(error) => Some(error),
            Self::MutationOutcomeUncertain { source, .. } => Some(source.as_ref()),
            _ => None,
        }
    }
}

pub struct ServerOperatorClient<'a> {
    config: &'a ServerOperatorConfig,
}

impl<'a> ServerOperatorClient<'a> {
    #[must_use]
    pub const fn new(config: &'a ServerOperatorConfig) -> Self {
        Self { config }
    }

    pub fn status(&self) -> Result<OperatorStatusV5, OperatorClientError> {
        match self.exchange(OperatorOperationV5::Status {})? {
            OperatorResultV5::Status { status } => Ok(*status),
            _ => Err(OperatorClientError::UnexpectedResult),
        }
    }

    pub fn rotate_evidence(
        &self,
        expected_window_epoch: u64,
    ) -> Result<OperatorEvidenceRotationV5, OperatorClientError> {
        match self.exchange(OperatorOperationV5::RotateEvidence {
            expected_window_epoch,
        })? {
            OperatorResultV5::EvidenceRotated { rotation } => Ok(rotation),
            _ => Err(OperatorClientError::UnexpectedResult),
        }
    }

    pub fn reset_faulted_scheduler(&self) -> Result<(), OperatorClientError> {
        match self.exchange(OperatorOperationV5::ResetFaultedScheduler {})? {
            OperatorResultV5::SchedulerReset {} => Ok(()),
            _ => Err(OperatorClientError::UnexpectedResult),
        }
    }

    pub fn physical_design_recommendations(
        &self,
    ) -> Result<OperatorPhysicalDesignRecommendationsV5, OperatorClientError> {
        match self.exchange(OperatorOperationV5::PhysicalDesignRecommendations {})? {
            OperatorResultV5::PhysicalDesignRecommendations {
                runtime_token,
                report,
            } => Ok(OperatorPhysicalDesignRecommendationsV5 {
                runtime_token,
                report,
            }),
            _ => Err(OperatorClientError::UnexpectedResult),
        }
    }

    pub fn physical_design_mutation_receipt_status(
        &self,
    ) -> Result<OperatorPhysicalDesignMutationReceiptStatusV5, OperatorClientError> {
        match self.exchange(OperatorOperationV5::PhysicalDesignMutationReceiptStatus {})? {
            OperatorResultV5::PhysicalDesignMutationReceiptStatus { status } => Ok(status),
            _ => Err(OperatorClientError::UnexpectedResult),
        }
    }

    pub fn physical_design_mutation_receipts(
        &self,
        after: Option<OperatorPhysicalDesignMutationReceiptCursorV5>,
        limit: u32,
    ) -> Result<OperatorPhysicalDesignMutationReceiptPageV5, OperatorClientError> {
        match self.exchange(OperatorOperationV5::PhysicalDesignMutationReceipts { after, limit })? {
            OperatorResultV5::PhysicalDesignMutationReceipts { page } => Ok(page),
            _ => Err(OperatorClientError::UnexpectedResult),
        }
    }

    pub fn rotate_physical_design_evidence(
        &self,
        expected_evidence_epoch: u64,
    ) -> Result<OperatorPhysicalDesignRotationV5, OperatorClientError> {
        match self.exchange(OperatorOperationV5::RotatePhysicalDesignEvidence {
            expected_evidence_epoch,
        })? {
            OperatorResultV5::PhysicalDesignEvidenceRotated { rotation } => Ok(rotation),
            _ => Err(OperatorClientError::UnexpectedResult),
        }
    }

    pub fn apply_physical_index(
        &self,
        expected_runtime_token: impl Into<String>,
        expected_evidence_epoch: u64,
        table_id: u64,
        column_id: u32,
        index_name: impl Into<String>,
    ) -> Result<OperatorPhysicalIndexApplyResultV5, OperatorClientError> {
        let result = self.exchange_mutating(OperatorOperationV5::ApplyPhysicalIndex {
            expected_runtime_token: expected_runtime_token.into(),
            expected_evidence_epoch,
            table_id,
            column_id,
            index_name: index_name.into(),
        })?;
        match result {
            OperatorResultV5::PhysicalIndexApplied { apply } => Ok(apply),
            _ => Err(classify_mutating_client_error(
                OperatorClientError::UnexpectedResult,
            )),
        }
    }

    pub fn apply_physical_columnar(
        &self,
        expected_runtime_token: impl Into<String>,
        expected_evidence_epoch: u64,
        table_id: u64,
        columns: Vec<u32>,
        mode: OperatorPhysicalColumnarDesignModeV5,
        placement_key: impl Into<String>,
    ) -> Result<OperatorPhysicalColumnarApplyResultV5, OperatorClientError> {
        let result = self.exchange_mutating(OperatorOperationV5::ApplyPhysicalColumnar {
            expected_runtime_token: expected_runtime_token.into(),
            expected_evidence_epoch,
            table_id,
            columns,
            mode,
            placement_key: placement_key.into(),
        })?;
        match result {
            OperatorResultV5::PhysicalColumnarApplied { apply } => Ok(apply),
            _ => Err(classify_mutating_client_error(
                OperatorClientError::UnexpectedResult,
            )),
        }
    }

    #[cfg(unix)]
    fn exchange_mutating(
        &self,
        operation: OperatorOperationV5,
    ) -> Result<OperatorResultV5, OperatorClientError> {
        use std::os::unix::net::UnixStream;

        let request_id = 1;
        let frame = encode_frame(&OperatorRequestV5 {
            request_id,
            operation,
        })
        .map_err(OperatorClientError::Protocol)?;
        let mut stream = UnixStream::connect(self.config.unix_socket()).map_err(|source| {
            OperatorClientError::Connect {
                path: self.config.unix_socket().to_path_buf(),
                source,
            }
        })?;
        stream
            .set_read_timeout(Some(self.config.io_timeout()))
            .and_then(|()| stream.set_write_timeout(Some(self.config.io_timeout())))
            .map_err(OperatorClientError::Configure)?;
        stream
            .write_all(&frame)
            .and_then(|()| stream.flush())
            .map_err(|error| {
                classify_mutating_client_error(OperatorClientError::Protocol(
                    OperatorProtocolError::Io(error),
                ))
            })?;
        let response: OperatorResponseV5 = read_frame(&mut stream)
            .map_err(OperatorClientError::Protocol)
            .map_err(classify_mutating_client_error)?;
        let result = match response {
            OperatorResponseV5::Ok {
                request_id: received,
                result,
            } => {
                verify_request_id(request_id, received).map_err(classify_mutating_client_error)?;
                Ok(result)
            }
            OperatorResponseV5::Error {
                request_id: received,
                error,
            } => {
                if received != 0 {
                    verify_request_id(request_id, received)
                        .map_err(classify_mutating_client_error)?;
                }
                Err(OperatorClientError::Remote(error))
            }
        };
        result.map_err(classify_mutating_client_error)
    }

    #[cfg(not(unix))]
    fn exchange_mutating(
        &self,
        _operation: OperatorOperationV5,
    ) -> Result<OperatorResultV5, OperatorClientError> {
        Err(OperatorClientError::UnsupportedPlatform)
    }

    #[cfg(unix)]
    fn exchange(
        &self,
        operation: OperatorOperationV5,
    ) -> Result<OperatorResultV5, OperatorClientError> {
        use std::os::unix::net::UnixStream;

        let mut stream = UnixStream::connect(self.config.unix_socket()).map_err(|source| {
            OperatorClientError::Connect {
                path: self.config.unix_socket().to_path_buf(),
                source,
            }
        })?;
        stream
            .set_read_timeout(Some(self.config.io_timeout()))
            .and_then(|()| stream.set_write_timeout(Some(self.config.io_timeout())))
            .map_err(OperatorClientError::Configure)?;
        let request_id = 1;
        write_frame(
            &mut stream,
            &OperatorRequestV5 {
                request_id,
                operation,
            },
        )
        .map_err(OperatorClientError::Protocol)?;
        let response: OperatorResponseV5 =
            read_frame(&mut stream).map_err(OperatorClientError::Protocol)?;
        match response {
            OperatorResponseV5::Ok {
                request_id: received,
                result,
            } => {
                verify_request_id(request_id, received)?;
                Ok(result)
            }
            OperatorResponseV5::Error {
                request_id: received,
                error,
            } => {
                if received != 0 {
                    verify_request_id(request_id, received)?;
                }
                Err(OperatorClientError::Remote(error))
            }
        }
    }

    #[cfg(not(unix))]
    fn exchange(
        &self,
        _operation: OperatorOperationV5,
    ) -> Result<OperatorResultV5, OperatorClientError> {
        Err(OperatorClientError::UnsupportedPlatform)
    }
}

fn classify_mutating_client_error(error: OperatorClientError) -> OperatorClientError {
    let (recovery_required, receipt) = match &error {
        OperatorClientError::Remote(remote)
            if remote.code == OperatorErrorCodeV5::PhysicalDesignMutationOutcomeUncertain =>
        {
            (remote.receipt.is_some(), remote.receipt.clone())
        }
        OperatorClientError::Protocol(_)
        | OperatorClientError::RequestIdMismatch { .. }
        | OperatorClientError::UnexpectedResult => (false, None),
        _ => return error,
    };
    OperatorClientError::MutationOutcomeUncertain {
        recovery_required,
        receipt,
        source: Box::new(error),
    }
}

fn verify_request_id(expected: u64, received: u64) -> Result<(), OperatorClientError> {
    if expected == received {
        Ok(())
    } else {
        Err(OperatorClientError::RequestIdMismatch { expected, received })
    }
}

#[derive(Debug)]
pub enum ServerOperatorError {
    UnsupportedPlatform,
    Randomness(getrandom::Error),
    PathExists(PathBuf),
    Bind { path: PathBuf, source: io::Error },
    Configure { path: PathBuf, source: io::Error },
    ThreadSpawn(io::Error),
    Accept(io::Error),
    SocketPathReplaced(PathBuf),
    Cleanup { path: PathBuf, source: io::Error },
    ThreadPanicked,
}

impl fmt::Display for ServerOperatorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedPlatform => {
                formatter.write_str("configured operator plane requires a Unix platform")
            }
            Self::Randomness(_) => {
                formatter.write_str("failed to generate the operator physical-design runtime token")
            }
            Self::PathExists(path) => write!(
                formatter,
                "operator socket path `{}` already exists; verify the old daemon is stopped and remove it explicitly",
                path.display()
            ),
            Self::Bind { path, source } => write!(
                formatter,
                "failed to bind operator socket `{}`: {source}",
                path.display()
            ),
            Self::Configure { path, source } => write!(
                formatter,
                "failed to configure operator socket `{}`: {source}",
                path.display()
            ),
            Self::ThreadSpawn(error) => {
                write!(formatter, "failed to spawn operator listener: {error}")
            }
            Self::Accept(error) => write!(formatter, "operator socket accept failed: {error}"),
            Self::SocketPathReplaced(path) => write!(
                formatter,
                "operator socket path `{}` was replaced and was not removed",
                path.display()
            ),
            Self::Cleanup { path, source } => write!(
                formatter,
                "failed to clean up operator socket `{}`: {source}",
                path.display()
            ),
            Self::ThreadPanicked => formatter.write_str("operator listener thread panicked"),
        }
    }
}

impl Error for ServerOperatorError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Bind { source, .. }
            | Self::Configure { source, .. }
            | Self::Cleanup { source, .. }
            | Self::ThreadSpawn(source)
            | Self::Accept(source) => Some(source),
            _ => None,
        }
    }
}

pub(crate) struct ServerOperatorPlane {
    shutdown: Sender<()>,
    join: Option<std::thread::JoinHandle<Result<(), ServerOperatorError>>>,
}

impl ServerOperatorPlane {
    #[cfg(test)]
    #[cfg(unix)]
    pub(crate) fn start(
        config: ServerOperatorConfig,
        adaptive_control: ServerAdaptiveControlHandle,
        physical_design_control: ServerPhysicalDesignControlHandle,
        failure_notification: Sender<()>,
    ) -> Result<Self, ServerOperatorError> {
        Self::start_with_capabilities(
            config,
            adaptive_control,
            physical_design_control,
            failure_notification,
            None,
        )
    }

    #[cfg(unix)]
    pub(crate) fn start_with_capabilities(
        config: ServerOperatorConfig,
        adaptive_control: ServerAdaptiveControlHandle,
        physical_design_control: ServerPhysicalDesignControlHandle,
        failure_notification: Sender<()>,
        columnar_capabilities: Option<ServerPhysicalColumnarApplyCapabilities>,
    ) -> Result<Self, ServerOperatorError> {
        use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
        use std::os::unix::net::UnixListener;
        use std::sync::mpsc;

        let runtime_token = (config.allow_physical_index_apply()
            || config.allow_physical_columnar_apply())
        .then(OperatorPhysicalDesignRuntimeToken::generate)
        .transpose()
        .map_err(ServerOperatorError::Randomness)?;
        let path = config.unix_socket().to_path_buf();
        match std::fs::symlink_metadata(&path) {
            Ok(_) => return Err(ServerOperatorError::PathExists(path)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(source) => {
                return Err(ServerOperatorError::Bind { path, source });
            }
        }
        let listener = UnixListener::bind(&path).map_err(|source| ServerOperatorError::Bind {
            path: path.clone(),
            source,
        })?;
        let metadata =
            std::fs::symlink_metadata(&path).map_err(|source| ServerOperatorError::Configure {
                path: path.clone(),
                source,
            })?;
        if !metadata.file_type().is_socket() {
            return Err(ServerOperatorError::SocketPathReplaced(path));
        }
        let owned = OwnedSocketPath {
            path: path.clone(),
            device: metadata.dev(),
            inode: metadata.ino(),
            armed: true,
        };
        if let Err(source) = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
        {
            return Err(ServerOperatorError::Configure { path, source });
        }
        match std::fs::symlink_metadata(&path) {
            Ok(current)
                if current.file_type().is_socket()
                    && current.dev() == owned.device
                    && current.ino() == owned.inode => {}
            Ok(_) => return Err(ServerOperatorError::SocketPathReplaced(path)),
            Err(source) => return Err(ServerOperatorError::Configure { path, source }),
        }
        if let Err(source) = listener.set_nonblocking(true) {
            return Err(ServerOperatorError::Configure { path, source });
        }
        let (shutdown, shutdown_rx) = mpsc::channel();
        let listener_policy = OperatorListenerPolicy::new(
            config.allow_physical_index_apply(),
            config.allow_physical_columnar_apply(),
            config.allow_physical_design_receipt_read(),
            columnar_capabilities,
            runtime_token,
        );
        let join = std::thread::Builder::new()
            .name("netbadb-operator-listener".into())
            .spawn(move || {
                let mut failure = OperatorFailureNotification::new(failure_notification);
                let run = run_operator_listener(
                    &listener,
                    &shutdown_rx,
                    config.io_timeout(),
                    &adaptive_control,
                    &physical_design_control,
                    listener_policy,
                );
                let result = run.and(owned.cleanup());
                if result.is_ok() {
                    failure.disarm();
                }
                result
            })
            .map_err(ServerOperatorError::ThreadSpawn)?;
        Ok(Self {
            shutdown,
            join: Some(join),
        })
    }

    #[cfg(test)]
    #[cfg(not(unix))]
    pub(crate) fn start(
        _config: ServerOperatorConfig,
        _adaptive_control: ServerAdaptiveControlHandle,
        _physical_design_control: ServerPhysicalDesignControlHandle,
        _failure_notification: Sender<()>,
    ) -> Result<Self, ServerOperatorError> {
        Err(ServerOperatorError::UnsupportedPlatform)
    }

    #[cfg(not(unix))]
    pub(crate) fn start_with_capabilities(
        config: ServerOperatorConfig,
        adaptive_control: ServerAdaptiveControlHandle,
        physical_design_control: ServerPhysicalDesignControlHandle,
        failure_notification: Sender<()>,
        columnar_capabilities: Option<ServerPhysicalColumnarApplyCapabilities>,
    ) -> Result<Self, ServerOperatorError> {
        let _ = (
            config,
            adaptive_control,
            physical_design_control,
            failure_notification,
            columnar_capabilities,
        );
        Err(ServerOperatorError::UnsupportedPlatform)
    }

    pub(crate) fn shutdown(mut self) -> Result<(), ServerOperatorError> {
        let _ = self.shutdown.send(());
        let join = self
            .join
            .take()
            .ok_or(ServerOperatorError::ThreadPanicked)?;
        join.join()
            .map_err(|_| ServerOperatorError::ThreadPanicked)?
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.join
            .as_ref()
            .is_none_or(std::thread::JoinHandle::is_finished)
    }
}

struct OperatorFailureNotification {
    sender: Sender<()>,
    armed: bool,
}

impl OperatorFailureNotification {
    const fn new(sender: Sender<()>) -> Self {
        Self {
            sender,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for OperatorFailureNotification {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.sender.send(());
        }
    }
}

#[cfg(unix)]
struct OwnedSocketPath {
    path: PathBuf,
    device: u64,
    inode: u64,
    armed: bool,
}

#[cfg(unix)]
impl OwnedSocketPath {
    fn cleanup(mut self) -> Result<(), ServerOperatorError> {
        use std::os::unix::fs::{FileTypeExt, MetadataExt};

        match std::fs::symlink_metadata(&self.path) {
            Ok(metadata)
                if metadata.file_type().is_socket()
                    && metadata.dev() == self.device
                    && metadata.ino() == self.inode =>
            {
                std::fs::remove_file(&self.path).map_err(|source| {
                    ServerOperatorError::Cleanup {
                        path: self.path.clone(),
                        source,
                    }
                })?;
                self.armed = false;
                Ok(())
            }
            Ok(_) => {
                self.armed = false;
                Err(ServerOperatorError::SocketPathReplaced(self.path.clone()))
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                self.armed = false;
                Ok(())
            }
            Err(source) => {
                self.armed = false;
                Err(ServerOperatorError::Cleanup {
                    path: self.path.clone(),
                    source,
                })
            }
        }
    }
}

#[cfg(unix)]
impl Drop for OwnedSocketPath {
    fn drop(&mut self) {
        use std::os::unix::fs::{FileTypeExt, MetadataExt};

        if !self.armed {
            return;
        }
        if let Ok(metadata) = std::fs::symlink_metadata(&self.path) {
            if metadata.file_type().is_socket()
                && metadata.dev() == self.device
                && metadata.ino() == self.inode
            {
                let _ = std::fs::remove_file(&self.path);
            }
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct OperatorListenerPolicy {
    allow_physical_index_apply: bool,
    allow_physical_columnar_apply: bool,
    allow_physical_design_receipt_read: bool,
    columnar_capabilities: Option<ServerPhysicalColumnarApplyCapabilities>,
    runtime_token: Option<OperatorPhysicalDesignRuntimeToken>,
}

impl OperatorListenerPolicy {
    pub(crate) const fn new(
        allow_physical_index_apply: bool,
        allow_physical_columnar_apply: bool,
        allow_physical_design_receipt_read: bool,
        columnar_capabilities: Option<ServerPhysicalColumnarApplyCapabilities>,
        runtime_token: Option<OperatorPhysicalDesignRuntimeToken>,
    ) -> Self {
        Self {
            allow_physical_index_apply,
            allow_physical_columnar_apply,
            allow_physical_design_receipt_read,
            columnar_capabilities,
            runtime_token,
        }
    }
}

#[cfg(unix)]
fn run_operator_listener(
    listener: &std::os::unix::net::UnixListener,
    shutdown: &std::sync::mpsc::Receiver<()>,
    io_timeout: Duration,
    adaptive_control: &ServerAdaptiveControlHandle,
    physical_design_control: &ServerPhysicalDesignControlHandle,
    policy: OperatorListenerPolicy,
) -> Result<(), ServerOperatorError> {
    use std::sync::mpsc::TryRecvError;

    loop {
        match shutdown.try_recv() {
            Ok(()) | Err(TryRecvError::Disconnected) => return Ok(()),
            Err(TryRecvError::Empty) => {}
        }
        match listener.accept() {
            Ok((mut stream, _)) => {
                if stream
                    .set_nonblocking(false)
                    .and_then(|()| stream.set_read_timeout(Some(io_timeout)))
                    .and_then(|()| stream.set_write_timeout(Some(io_timeout)))
                    .is_ok()
                {
                    let _ = serve_operator_connection_with_capabilities(
                        &mut stream,
                        adaptive_control,
                        physical_design_control,
                        policy,
                    );
                }
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(error) => return Err(ServerOperatorError::Accept(error)),
        }
    }
}

#[cfg(unix)]
#[cfg(test)]
fn serve_operator_connection(
    stream: &mut std::os::unix::net::UnixStream,
    adaptive_control: &ServerAdaptiveControlHandle,
    physical_design_control: &ServerPhysicalDesignControlHandle,
    allow_physical_index_apply: bool,
    runtime_token: Option<OperatorPhysicalDesignRuntimeToken>,
) -> Result<(), OperatorProtocolError> {
    serve_operator_connection_with_capabilities(
        stream,
        adaptive_control,
        physical_design_control,
        OperatorListenerPolicy::new(
            allow_physical_index_apply,
            false,
            false,
            None,
            runtime_token,
        ),
    )
}

#[cfg(unix)]
pub(crate) fn serve_operator_connection_with_capabilities(
    stream: &mut std::os::unix::net::UnixStream,
    adaptive_control: &ServerAdaptiveControlHandle,
    physical_design_control: &ServerPhysicalDesignControlHandle,
    policy: OperatorListenerPolicy,
) -> Result<(), OperatorProtocolError> {
    let request = match read_frame::<OperatorRequestV5>(stream) {
        Ok(request) => request,
        Err(error) => {
            let response = OperatorResponseV5::Error {
                request_id: 0,
                error: protocol_remote_error(&error),
            };
            let _ = write_frame(stream, &response);
            return Err(error);
        }
    };
    let response = execute_operator_request_with_capabilities(
        request,
        adaptive_control,
        physical_design_control,
        policy,
    );
    write_operator_response(stream, &response)
}

fn write_operator_response(
    writer: &mut impl Write,
    response: &OperatorResponseV5,
) -> Result<(), OperatorProtocolError> {
    let request_id = response.request_id();
    match write_frame(writer, response) {
        Err(OperatorProtocolError::PayloadTooLarge(_)) => write_frame(
            writer,
            &OperatorResponseV5::Error {
                request_id,
                error: response_too_large_error(),
            },
        ),
        result => result,
    }
}

#[cfg(test)]
fn execute_operator_request(
    request: OperatorRequestV5,
    adaptive_control: &ServerAdaptiveControlHandle,
    physical_design_control: &ServerPhysicalDesignControlHandle,
    allow_physical_index_apply: bool,
    runtime_token: Option<OperatorPhysicalDesignRuntimeToken>,
) -> OperatorResponseV5 {
    execute_operator_request_with_capabilities(
        request,
        adaptive_control,
        physical_design_control,
        OperatorListenerPolicy::new(
            allow_physical_index_apply,
            false,
            false,
            None,
            runtime_token,
        ),
    )
}

fn execute_operator_request_with_capabilities(
    request: OperatorRequestV5,
    adaptive_control: &ServerAdaptiveControlHandle,
    physical_design_control: &ServerPhysicalDesignControlHandle,
    policy: OperatorListenerPolicy,
) -> OperatorResponseV5 {
    let result = match request.operation {
        OperatorOperationV5::Status {} => operator_status_with_capabilities(
            adaptive_control,
            physical_design_control,
            policy.allow_physical_index_apply,
            policy.allow_physical_columnar_apply,
            policy.allow_physical_design_receipt_read,
            policy.columnar_capabilities,
            policy.runtime_token,
        )
        .map(|status| OperatorResultV5::Status {
            status: Box::new(status),
        }),
        OperatorOperationV5::PhysicalDesignMutationReceiptStatus {} => {
            execute_physical_design_mutation_receipt_status(
                physical_design_control,
                policy.allow_physical_design_receipt_read,
            )
        }
        OperatorOperationV5::PhysicalDesignMutationReceipts { after, limit } => {
            execute_physical_design_mutation_receipts(
                physical_design_control,
                policy.allow_physical_design_receipt_read,
                after,
                limit,
            )
        }
        OperatorOperationV5::RotateEvidence {
            expected_window_epoch,
        } => adaptive_control
            .rotate_evidence_if_window(AdaptiveEvidenceWindowEpoch(expected_window_epoch))
            .map(operator_rotation)
            .map(|rotation| OperatorResultV5::EvidenceRotated { rotation })
            .map_err(control_remote_error),
        OperatorOperationV5::ResetFaultedScheduler {} => adaptive_control
            .reset_faulted_scheduler()
            .map(|()| OperatorResultV5::SchedulerReset {})
            .map_err(control_remote_error),
        OperatorOperationV5::PhysicalDesignRecommendations {} => physical_design_control
            .recommendations()
            .map(operator_physical_design_report)
            .map(|report| OperatorResultV5::PhysicalDesignRecommendations {
                runtime_token: policy
                    .runtime_token
                    .map(OperatorPhysicalDesignRuntimeToken::encode),
                report,
            })
            .map_err(physical_design_remote_error),
        OperatorOperationV5::RotatePhysicalDesignEvidence {
            expected_evidence_epoch,
        } => physical_design_control
            .rotate_evidence_if_epoch(netbadb_core::PhysicalDesignEvidenceEpoch(
                expected_evidence_epoch,
            ))
            .map(operator_physical_design_rotation)
            .map(|rotation| OperatorResultV5::PhysicalDesignEvidenceRotated { rotation })
            .map_err(physical_design_remote_error),
        OperatorOperationV5::ApplyPhysicalIndex {
            expected_runtime_token,
            expected_evidence_epoch,
            table_id,
            column_id,
            index_name,
        } => execute_physical_index_apply(
            physical_design_control,
            policy.allow_physical_index_apply,
            policy.runtime_token,
            OperatorPhysicalIndexApplyInput {
                expected_runtime_token,
                expected_evidence_epoch,
                table_id,
                column_id,
                index_name,
            },
        ),
        OperatorOperationV5::ApplyPhysicalColumnar {
            expected_runtime_token,
            expected_evidence_epoch,
            table_id,
            columns,
            mode,
            placement_key,
        } => execute_physical_columnar_apply(
            physical_design_control,
            policy.allow_physical_columnar_apply,
            policy.runtime_token,
            OperatorPhysicalColumnarApplyInput {
                expected_runtime_token,
                expected_evidence_epoch,
                table_id,
                columns,
                mode,
                placement_key,
            },
        ),
    };
    match result {
        Ok(result) => OperatorResponseV5::Ok {
            request_id: request.request_id,
            result,
        },
        Err(error) => OperatorResponseV5::Error {
            request_id: request.request_id,
            error,
        },
    }
}

fn receipt_read_not_allowed_error() -> OperatorRemoteErrorV5 {
    OperatorRemoteErrorV5 {
        code: OperatorErrorCodeV5::PhysicalDesignMutationReceiptReadNotAllowed,
        message: "operator physical-design receipt read is not allowed by the manifest".into(),
        receipt: None,
    }
}

fn execute_physical_design_mutation_receipt_status(
    physical_design_control: &ServerPhysicalDesignControlHandle,
    allowed: bool,
) -> Result<OperatorResultV5, OperatorRemoteErrorV5> {
    if !allowed {
        return Err(receipt_read_not_allowed_error());
    }
    physical_design_control
        .mutation_receipt_status()
        .map(operator_mutation_receipt_status)
        .map(|status| OperatorResultV5::PhysicalDesignMutationReceiptStatus { status })
        .map_err(receipt_read_remote_error)
}

fn execute_physical_design_mutation_receipts(
    physical_design_control: &ServerPhysicalDesignControlHandle,
    allowed: bool,
    after: Option<OperatorPhysicalDesignMutationReceiptCursorV5>,
    limit: u32,
) -> Result<OperatorResultV5, OperatorRemoteErrorV5> {
    if !allowed {
        return Err(receipt_read_not_allowed_error());
    }
    let after = after
        .map(operator_receipt_cursor)
        .transpose()
        .map_err(|()| OperatorRemoteErrorV5 {
            code: OperatorErrorCodeV5::InvalidPhysicalDesignMutationReceiptCursor,
            message: "receipt cursor requires exactly 32 lowercase hexadecimal incarnation characters and a nonzero receipt ID".into(),
            receipt: None,
        })?;
    physical_design_control
        .mutation_receipts_scoped(after, limit)
        .map(operator_mutation_receipt_page)
        .map(|page| OperatorResultV5::PhysicalDesignMutationReceipts { page })
        .map_err(receipt_read_remote_error)
}

fn operator_receipt_cursor(
    cursor: OperatorPhysicalDesignMutationReceiptCursorV5,
) -> Result<ServerPhysicalDesignMutationReceiptCursor, ()> {
    let incarnation = decode_lower_hex_16(&cursor.journal_incarnation)
        .and_then(|bytes| ServerPhysicalDesignMutationReceiptJournalIncarnation::new(bytes).ok())
        .ok_or(())?;
    ServerPhysicalDesignMutationReceiptCursor::new(
        incarnation,
        crate::ServerPhysicalDesignMutationReceiptId(cursor.receipt_id),
    )
    .map_err(|_| ())
}

fn operator_receipt_reference(
    reference: ServerPhysicalDesignMutationReceiptReference,
) -> OperatorPhysicalDesignMutationReceiptRefV5 {
    OperatorPhysicalDesignMutationReceiptRefV5 {
        journal_incarnation: encode_lower_hex_16(*reference.journal_incarnation().as_bytes()),
        receipt_id: reference.receipt_id().0,
    }
}

fn operator_mutation_receipt_status(
    status: ServerPhysicalDesignMutationReceiptStatus,
) -> OperatorPhysicalDesignMutationReceiptStatusV5 {
    OperatorPhysicalDesignMutationReceiptStatusV5 {
        journal_incarnation: encode_lower_hex_16(*status.journal_incarnation.as_bytes()),
        recovery_required: status.recovery_required,
        latest_receipt_id: status.latest_receipt_id.map(|id| id.0),
        max_receipts_per_read: status.max_receipts_per_read,
    }
}

fn operator_mutation_receipt_page(
    page: ServerPhysicalDesignMutationReceiptScopedPage,
) -> OperatorPhysicalDesignMutationReceiptPageV5 {
    let incarnation = page.journal_incarnation;
    OperatorPhysicalDesignMutationReceiptPageV5 {
        journal_incarnation: encode_lower_hex_16(*incarnation.as_bytes()),
        receipts: page
            .receipts
            .into_iter()
            .map(|receipt| operator_mutation_receipt(incarnation, receipt))
            .collect(),
        next_after: page
            .next_after
            .map(|cursor| OperatorPhysicalDesignMutationReceiptCursorV5 {
                journal_incarnation: encode_lower_hex_16(*cursor.journal_incarnation().as_bytes()),
                receipt_id: cursor.receipt_id().0,
            }),
    }
}

fn operator_mutation_receipt(
    incarnation: ServerPhysicalDesignMutationReceiptJournalIncarnation,
    receipt: ServerPhysicalDesignMutationReceipt,
) -> OperatorPhysicalDesignMutationReceiptV5 {
    OperatorPhysicalDesignMutationReceiptV5 {
        receipt: OperatorPhysicalDesignMutationReceiptRefV5 {
            journal_incarnation: encode_lower_hex_16(*incarnation.as_bytes()),
            receipt_id: receipt.id.0,
        },
        source: match receipt.source {
            ServerPhysicalDesignMutationSource::Programmatic => {
                OperatorPhysicalDesignMutationReceiptSourceV5::Programmatic
            }
            ServerPhysicalDesignMutationSource::LocalOperator => {
                OperatorPhysicalDesignMutationReceiptSourceV5::LocalOperator
            }
        },
        evidence_epoch: receipt.evidence_epoch.0,
        target: match receipt.target {
            ServerPhysicalDesignMutationTarget::Index {
                table_id,
                column_id,
                index_name,
            } => OperatorPhysicalDesignMutationReceiptTargetV5::Index {
                table_id: table_id.0,
                column_id: column_id.0,
                index_name: index_name.as_str().to_owned(),
            },
            ServerPhysicalDesignMutationTarget::Columnar {
                table_id,
                columns,
                mode,
                placement,
            } => OperatorPhysicalDesignMutationReceiptTargetV5::Columnar {
                table_id: table_id.0,
                columns: columns.into_iter().map(|column| column.0).collect(),
                mode: match mode {
                    PhysicalColumnarDesignMode::Snapshot => {
                        OperatorPhysicalColumnarDesignModeV5::Snapshot
                    }
                    PhysicalColumnarDesignMode::Incremental => {
                        OperatorPhysicalColumnarDesignModeV5::Incremental
                    }
                },
                placement_key: placement.as_str().to_owned(),
            },
        },
        outcome: match receipt.outcome {
            ServerPhysicalDesignMutationReceiptOutcome::Pending => {
                OperatorPhysicalDesignMutationReceiptOutcomeV5::Pending
            }
            ServerPhysicalDesignMutationReceiptOutcome::CreatedIndex { index_id } => {
                OperatorPhysicalDesignMutationReceiptOutcomeV5::CreatedIndex {
                    index_id: index_id.0,
                }
            }
            ServerPhysicalDesignMutationReceiptOutcome::CreatedColumnar { projection_id } => {
                OperatorPhysicalDesignMutationReceiptOutcomeV5::CreatedColumnar {
                    projection_id: projection_id.0,
                }
            }
            ServerPhysicalDesignMutationReceiptOutcome::AlreadyAppliedIndex { index_id } => {
                OperatorPhysicalDesignMutationReceiptOutcomeV5::AlreadyAppliedIndex {
                    index_id: index_id.0,
                }
            }
            ServerPhysicalDesignMutationReceiptOutcome::AlreadyAppliedColumnar {
                projection_id,
            } => OperatorPhysicalDesignMutationReceiptOutcomeV5::AlreadyAppliedColumnar {
                projection_id: projection_id.0,
            },
            ServerPhysicalDesignMutationReceiptOutcome::AlreadyCovered => {
                OperatorPhysicalDesignMutationReceiptOutcomeV5::AlreadyCovered
            }
            ServerPhysicalDesignMutationReceiptOutcome::Rejected => {
                OperatorPhysicalDesignMutationReceiptOutcomeV5::Rejected
            }
            ServerPhysicalDesignMutationReceiptOutcome::Failed => {
                OperatorPhysicalDesignMutationReceiptOutcomeV5::Failed
            }
            ServerPhysicalDesignMutationReceiptOutcome::RecoveredAppliedIndex { index_id } => {
                OperatorPhysicalDesignMutationReceiptOutcomeV5::RecoveredAppliedIndex {
                    index_id: index_id.0,
                }
            }
            ServerPhysicalDesignMutationReceiptOutcome::RecoveredAppliedColumnar {
                projection_id,
            } => OperatorPhysicalDesignMutationReceiptOutcomeV5::RecoveredAppliedColumnar {
                projection_id: projection_id.0,
            },
            ServerPhysicalDesignMutationReceiptOutcome::RecoveredNotApplied => {
                OperatorPhysicalDesignMutationReceiptOutcomeV5::RecoveredNotApplied
            }
            ServerPhysicalDesignMutationReceiptOutcome::RecoveredConflict => {
                OperatorPhysicalDesignMutationReceiptOutcomeV5::RecoveredConflict
            }
        },
    }
}

fn receipt_read_remote_error(
    error: ServerPhysicalDesignMutationReceiptControlError,
) -> OperatorRemoteErrorV5 {
    let (code, message) = match error {
        ServerPhysicalDesignMutationReceiptControlError::NotEnabled => (
            OperatorErrorCodeV5::PhysicalDesignMutationReceiptsNotEnabled,
            "physical-design mutation receipts are not enabled",
        ),
        ServerPhysicalDesignMutationReceiptControlError::InvalidLimit { .. } => (
            OperatorErrorCodeV5::InvalidPhysicalDesignMutationReceiptLimit,
            "receipt read limit must be between 1 and 128",
        ),
        ServerPhysicalDesignMutationReceiptControlError::JournalChanged { .. } => (
            OperatorErrorCodeV5::PhysicalDesignMutationReceiptJournalChanged,
            "receipt cursor belongs to a different journal incarnation",
        ),
        ServerPhysicalDesignMutationReceiptControlError::ServerStopped => (
            OperatorErrorCodeV5::ServerStopped,
            "server physical-design control is stopped",
        ),
        ServerPhysicalDesignMutationReceiptControlError::RecoveryRequired
        | ServerPhysicalDesignMutationReceiptControlError::Journal(_) => (
            OperatorErrorCodeV5::PhysicalDesignMutationReceiptReadFailed,
            "physical-design mutation receipt read failed",
        ),
    };
    OperatorRemoteErrorV5 {
        code,
        message: message.into(),
        receipt: None,
    }
}

fn execute_physical_index_apply(
    physical_design_control: &ServerPhysicalDesignControlHandle,
    allow_physical_index_apply: bool,
    runtime_token: Option<OperatorPhysicalDesignRuntimeToken>,
    input: OperatorPhysicalIndexApplyInput,
) -> Result<OperatorResultV5, OperatorRemoteErrorV5> {
    if !allow_physical_index_apply {
        return Err(OperatorRemoteErrorV5 {
            code: OperatorErrorCodeV5::PhysicalIndexApplyNotEnabled,
            message: "operator physical-index apply is not enabled by the manifest".into(),
            receipt: None,
        });
    }
    let expected = OperatorPhysicalDesignRuntimeToken::parse(&input.expected_runtime_token)
        .ok_or_else(|| OperatorRemoteErrorV5 {
            code: OperatorErrorCodeV5::MalformedRequest,
            message: "expected_runtime_token must be exactly 32 lowercase hexadecimal characters"
                .into(),
            receipt: None,
        })?;
    let runtime_token_matches = runtime_token == Some(expected);
    let index_name = IndexName::new(input.index_name).map_err(|_| OperatorRemoteErrorV5 {
        code: OperatorErrorCodeV5::InvalidIndexName,
        message: "index_name must be nonempty and at most 255 bytes".into(),
        receipt: None,
    })?;
    let reply = physical_design_control.apply_approved_index(
        runtime_token_matches,
        netbadb_core::PhysicalDesignEvidenceEpoch(input.expected_evidence_epoch),
        PhysicalIndexCandidate {
            table_id: TableId(input.table_id),
            column_id: ColumnId(input.column_id),
        },
        index_name,
    );
    let receipt = reply.receipt.map(operator_receipt_reference);
    match reply.result {
        Ok(report) => Ok(OperatorResultV5::PhysicalIndexApplied {
            apply: operator_physical_index_apply_result(report, receipt),
        }),
        Err(error) => Err(physical_design_remote_error_with_receipt(error, receipt)),
    }
}

fn operator_physical_index_apply_result(
    report: ServerApprovedPhysicalIndexApplyReport,
    receipt: Option<OperatorPhysicalDesignMutationReceiptRefV5>,
) -> OperatorPhysicalIndexApplyResultV5 {
    let outcome = match report.outcome {
        ServerApprovedPhysicalIndexApplyOutcome::Created { index_id } => {
            OperatorPhysicalIndexApplyOutcomeV5::Created {
                index_id: index_id.0,
            }
        }
        ServerApprovedPhysicalIndexApplyOutcome::AlreadyApplied { index_id } => {
            OperatorPhysicalIndexApplyOutcomeV5::AlreadyApplied {
                index_id: index_id.0,
            }
        }
        ServerApprovedPhysicalIndexApplyOutcome::AlreadyCovered => {
            OperatorPhysicalIndexApplyOutcomeV5::AlreadyCovered
        }
    };
    OperatorPhysicalIndexApplyResultV5 {
        table_id: report.candidate.table_id.0,
        column_id: report.candidate.column_id.0,
        index_name: report.index_name.as_str().to_owned(),
        outcome,
        receipt,
    }
}

fn execute_physical_columnar_apply(
    physical_design_control: &ServerPhysicalDesignControlHandle,
    allow_physical_columnar_apply: bool,
    runtime_token: Option<OperatorPhysicalDesignRuntimeToken>,
    input: OperatorPhysicalColumnarApplyInput,
) -> Result<OperatorResultV5, OperatorRemoteErrorV5> {
    if !allow_physical_columnar_apply {
        return Err(OperatorRemoteErrorV5 {
            code: OperatorErrorCodeV5::PhysicalColumnarApplyNotEnabled,
            message: "operator physical-columnar apply is not enabled by the manifest".into(),
            receipt: None,
        });
    }
    if input.columns.is_empty() {
        return Err(OperatorRemoteErrorV5 {
            code: OperatorErrorCodeV5::MalformedRequest,
            message: "columns must contain at least one column".into(),
            receipt: None,
        });
    }
    let expected = OperatorPhysicalDesignRuntimeToken::parse(&input.expected_runtime_token)
        .ok_or_else(|| OperatorRemoteErrorV5 {
            code: OperatorErrorCodeV5::MalformedRequest,
            message: "expected_runtime_token must be exactly 32 lowercase hexadecimal characters"
                .into(),
            receipt: None,
        })?;
    let runtime_token_matches = runtime_token == Some(expected);
    let placement =
        crate::ServerPhysicalColumnarPlacementKey::new(input.placement_key).map_err(|_| {
            OperatorRemoteErrorV5 {
                code: OperatorErrorCodeV5::InvalidPhysicalColumnarPlacementKey,
                message: "placement_key must be one direct-child ASCII namespace key".into(),
                receipt: None,
            }
        })?;
    let reply = physical_design_control.apply_approved_columnar(
        runtime_token_matches,
        netbadb_core::PhysicalDesignEvidenceEpoch(input.expected_evidence_epoch),
        PhysicalColumnarCandidate {
            table_id: TableId(input.table_id),
            columns: input.columns.into_iter().map(ColumnId).collect(),
        },
        match input.mode {
            OperatorPhysicalColumnarDesignModeV5::Snapshot => PhysicalColumnarDesignMode::Snapshot,
            OperatorPhysicalColumnarDesignModeV5::Incremental => {
                PhysicalColumnarDesignMode::Incremental
            }
        },
        placement,
    );
    let receipt = reply.receipt.map(operator_receipt_reference);
    match reply.result {
        Ok(report) => Ok(OperatorResultV5::PhysicalColumnarApplied {
            apply: operator_physical_columnar_apply_result(report, receipt),
        }),
        Err(error) => Err(physical_columnar_remote_error_with_receipt(error, receipt)),
    }
}

fn operator_physical_columnar_apply_result(
    report: ServerApprovedPhysicalColumnarApplyReport,
    receipt: Option<OperatorPhysicalDesignMutationReceiptRefV5>,
) -> OperatorPhysicalColumnarApplyResultV5 {
    let outcome = match report.outcome {
        ServerApprovedPhysicalColumnarApplyOutcome::Created { projection_id } => {
            OperatorPhysicalColumnarApplyOutcomeV5::Created {
                projection_id: projection_id.0,
            }
        }
        ServerApprovedPhysicalColumnarApplyOutcome::AlreadyApplied { projection_id } => {
            OperatorPhysicalColumnarApplyOutcomeV5::AlreadyApplied {
                projection_id: projection_id.0,
            }
        }
        ServerApprovedPhysicalColumnarApplyOutcome::AlreadyCovered => {
            OperatorPhysicalColumnarApplyOutcomeV5::AlreadyCovered
        }
    };
    OperatorPhysicalColumnarApplyResultV5 {
        table_id: report.candidate.table_id.0,
        columns: report
            .candidate
            .columns
            .into_iter()
            .map(|column| column.0)
            .collect(),
        mode: match report.mode {
            PhysicalColumnarDesignMode::Snapshot => OperatorPhysicalColumnarDesignModeV5::Snapshot,
            PhysicalColumnarDesignMode::Incremental => {
                OperatorPhysicalColumnarDesignModeV5::Incremental
            }
        },
        placement_key: report.placement.as_str().to_owned(),
        outcome,
        receipt,
    }
}

fn operator_status_with_capabilities(
    adaptive_control: &ServerAdaptiveControlHandle,
    physical_design_control: &ServerPhysicalDesignControlHandle,
    allow_physical_index_apply: bool,
    allow_physical_columnar_apply: bool,
    allow_physical_design_receipt_read: bool,
    columnar_capabilities: Option<ServerPhysicalColumnarApplyCapabilities>,
    runtime_token: Option<OperatorPhysicalDesignRuntimeToken>,
) -> Result<OperatorStatusV5, OperatorRemoteErrorV5> {
    let adaptive = match adaptive_control.status() {
        Ok(status) if status.mode == ServerAdaptiveMode::Disabled => None,
        Ok(status) => Some(operator_adaptive_status(status).map_err(control_remote_error)?),
        Err(ServerAdaptiveControlError::AdaptiveNotEnabled) => None,
        Err(error) => return Err(control_remote_error(error)),
    };
    let physical_design = match physical_design_control.status() {
        Ok(status) => Some(operator_physical_design_status_with_capabilities(
            status,
            allow_physical_index_apply,
            allow_physical_columnar_apply,
            allow_physical_design_receipt_read,
            columnar_capabilities,
            runtime_token,
        )),
        Err(ServerPhysicalDesignControlError::PhysicalDesignNotEnabled) => None,
        Err(error) => return Err(physical_design_remote_error(error)),
    };
    if adaptive.is_none() && physical_design.is_none() {
        return Err(OperatorRemoteErrorV5 {
            code: OperatorErrorCodeV5::Internal,
            message: "operator plane has no managed runtime".into(),
            receipt: None,
        });
    }
    Ok(OperatorStatusV5 {
        adaptive,
        physical_design,
    })
}

fn operator_adaptive_status(
    status: ServerAdaptiveStatus,
) -> Result<OperatorAdaptiveStatusV5, ServerAdaptiveControlError> {
    let mode = match status.mode {
        ServerAdaptiveMode::FeedbackOnly => OperatorAdaptiveModeV5::FeedbackOnly,
        ServerAdaptiveMode::Driven => OperatorAdaptiveModeV5::Driven,
        ServerAdaptiveMode::Disabled => return Err(ServerAdaptiveControlError::AdaptiveNotEnabled),
    };
    let feedback = status
        .feedback
        .ok_or(ServerAdaptiveControlError::AdaptiveNotEnabled)?;
    let progress = feedback.evidence_progress;
    let feedback = OperatorFeedbackStatusV5 {
        eligible_query_count: feedback.eligible_query_count,
        record_success_count: feedback.record_success_count,
        record_error_count: feedback.record_error_count,
        capacity_rejection_count: feedback.capacity_rejection_count,
        schema_rotation_count: feedback.schema_rotation_count,
        incomplete_report_count: feedback.incomplete_report_count,
        counter_overflowed: feedback.counter_overflowed,
        last_record_outcome: feedback.last_record_outcome.map(record_outcome),
        last_record_error: feedback.last_record_error.map(record_error),
        window_epoch: progress.window_epoch.0,
        schema_generation: progress.schema_generation.map(|generation| generation.0),
        recorded_reports: progress.recorded_reports,
        pool_health: match feedback.pool_health {
            AdaptiveEvidencePoolHealth::Healthy => OperatorEvidencePoolHealthV5::Healthy,
            AdaptiveEvidencePoolHealth::RotationRecommended => {
                OperatorEvidencePoolHealthV5::RotationRecommended
            }
        },
    };
    let driver = status.driver.map(|driver| OperatorDriverStatusV5 {
        scheduler_last_observed_tick: driver.scheduler_state.last_observed_tick.map(|tick| tick.0),
        scheduler_last_run_tick: driver.scheduler_state.last_run_tick.map(|tick| tick.0),
        scheduler_gate: scheduler_gate(driver.scheduler_state.gate),
        last_submitted_logical_tick: driver.last_submitted_logical_tick.map(|tick| tick.0),
        tick_pending: driver.tick_pending,
        driver_tick_count: driver.driver_tick_count,
        scheduler_tick_count: driver.scheduler_tick_count,
        scheduler_ran_count: driver.scheduler_ran_count,
        scheduler_held_count: driver.scheduler_held_count,
        scheduler_error_count: driver.scheduler_error_count,
        last_orchestration_stop_reason: driver
            .last_orchestration_stop_reason
            .map(orchestration_stop_reason),
        host_clock_exhausted: driver.host_clock_exhausted,
        counter_overflowed: driver.counter_overflowed,
    });
    Ok(OperatorAdaptiveStatusV5 {
        mode,
        feedback,
        driver,
    })
}

fn record_outcome(outcome: AdaptiveEvidenceRecordOutcome) -> OperatorEvidenceRecordOutcomeV5 {
    match outcome {
        AdaptiveEvidenceRecordOutcome::Recorded => OperatorEvidenceRecordOutcomeV5::Recorded,
        AdaptiveEvidenceRecordOutcome::SchemaRotated => {
            OperatorEvidenceRecordOutcomeV5::SchemaRotated
        }
        AdaptiveEvidenceRecordOutcome::RecordedWithCapacityRejection => {
            OperatorEvidenceRecordOutcomeV5::RecordedWithCapacityRejection
        }
        AdaptiveEvidenceRecordOutcome::SchemaRotatedWithCapacityRejection => {
            OperatorEvidenceRecordOutcomeV5::SchemaRotatedWithCapacityRejection
        }
    }
}

fn record_error(error: AdaptiveEvidenceRecordError) -> OperatorEvidenceRecordErrorV5 {
    match error {
        AdaptiveEvidenceRecordError::GlobalVisibilityRequired => {
            OperatorEvidenceRecordErrorV5::GlobalVisibilityRequired
        }
        AdaptiveEvidenceRecordError::StaleSchemaEvidence { .. } => {
            OperatorEvidenceRecordErrorV5::StaleSchemaEvidence
        }
        AdaptiveEvidenceRecordError::OutOfOrderVisibility { .. } => {
            OperatorEvidenceRecordErrorV5::OutOfOrderVisibility
        }
        AdaptiveEvidenceRecordError::StaleTargetGenerationEvidence { .. } => {
            OperatorEvidenceRecordErrorV5::StaleTargetGenerationEvidence
        }
        AdaptiveEvidenceRecordError::StaleTargetIdentityEvidence { .. } => {
            OperatorEvidenceRecordErrorV5::StaleTargetIdentityEvidence
        }
        AdaptiveEvidenceRecordError::RetiredTargetEvidence { .. } => {
            OperatorEvidenceRecordErrorV5::RetiredTargetEvidence
        }
        AdaptiveEvidenceRecordError::StaleCalibrationEpochEvidence { .. } => {
            OperatorEvidenceRecordErrorV5::StaleCalibrationEpochEvidence
        }
        AdaptiveEvidenceRecordError::EvidenceWindowEpochExhausted => {
            OperatorEvidenceRecordErrorV5::EvidenceWindowEpochExhausted
        }
    }
}

fn scheduler_gate(gate: AutomaticSchedulerGate) -> OperatorSchedulerGateV5 {
    match gate {
        AutomaticSchedulerGate::Open { delay } => OperatorSchedulerGateV5::Open {
            delay_class: match delay {
                AutomaticSchedulerDelayClass::Normal => OperatorSchedulerDelayClassV5::Normal,
                AutomaticSchedulerDelayClass::Idle => OperatorSchedulerDelayClassV5::Idle,
                AutomaticSchedulerDelayClass::NoProgress => {
                    OperatorSchedulerDelayClassV5::NoProgress
                }
            },
        },
        AutomaticSchedulerGate::AwaitingTrialProgress { evidence } => {
            OperatorSchedulerGateV5::AwaitingTrialProgress {
                window_epoch: evidence.window_epoch.0,
                schema_generation: evidence.schema_generation.map(|generation| generation.0),
                recorded_reports: evidence.recorded_reports,
            }
        }
        AutomaticSchedulerGate::AwaitingEvidenceRenewal {
            blocked_window_epoch,
            recommendation,
        } => OperatorSchedulerGateV5::AwaitingEvidenceRenewal {
            blocked_window_epoch: blocked_window_epoch.0,
            renewal_reason: match recommendation.reason {
                AutomaticEvidenceRenewalReason::ColumnarPhysicalStateChanged => {
                    OperatorEvidenceRenewalReasonV5::ColumnarPhysicalStateChanged
                }
                AutomaticEvidenceRenewalReason::ColumnarEligibilityChanged => {
                    OperatorEvidenceRenewalReasonV5::ColumnarEligibilityChanged
                }
                AutomaticEvidenceRenewalReason::AuthoritativeLsmLayoutChanged => {
                    OperatorEvidenceRenewalReasonV5::AuthoritativeLsmLayoutChanged
                }
            },
        },
        AutomaticSchedulerGate::Faulted(fault) => OperatorSchedulerGateV5::Faulted {
            fault: match fault {
                AutomaticSchedulerFault::MaintenanceEnvelopeExceeded => {
                    OperatorSchedulerFaultV5::MaintenanceEnvelopeExceeded
                }
                AutomaticSchedulerFault::StepFailed => OperatorSchedulerFaultV5::StepFailed,
                AutomaticSchedulerFault::ConsumptionOverflow => {
                    OperatorSchedulerFaultV5::ConsumptionOverflow
                }
            },
        },
    }
}

fn orchestration_stop_reason(
    reason: AutomaticOrchestrationStopReason,
) -> OperatorOrchestrationStopReasonV5 {
    match reason {
        AutomaticOrchestrationStopReason::NoReadyWork => {
            OperatorOrchestrationStopReasonV5::NoReadyWork
        }
        AutomaticOrchestrationStopReason::StepLimitReached => {
            OperatorOrchestrationStopReasonV5::StepLimitReached
        }
        AutomaticOrchestrationStopReason::ActiveTrial(_) => {
            OperatorOrchestrationStopReasonV5::ActiveTrial
        }
        AutomaticOrchestrationStopReason::TrialBoundaryResolved => {
            OperatorOrchestrationStopReasonV5::TrialBoundaryResolved
        }
        AutomaticOrchestrationStopReason::EvidenceRenewalRecommended(_) => {
            OperatorOrchestrationStopReasonV5::EvidenceRenewalRecommended
        }
        AutomaticOrchestrationStopReason::SelectedCandidateDidNotProgress => {
            OperatorOrchestrationStopReasonV5::SelectedCandidateDidNotProgress
        }
        AutomaticOrchestrationStopReason::MaintenanceEnvelopeExceeded { .. } => {
            OperatorOrchestrationStopReasonV5::MaintenanceEnvelopeExceeded
        }
    }
}

fn operator_rotation(report: AdaptiveEvidenceRotationReport) -> OperatorEvidenceRotationV5 {
    OperatorEvidenceRotationV5 {
        previous_window_epoch: report.previous_window_epoch.0,
        new_window_epoch: report.new_window_epoch.0,
        schema_generation: report.schema_generation.map(|generation| generation.0),
        ordering_high_water: report.ordering_high_water.map(|sequence| sequence.0),
        discarded_target_window_count: report.discarded_target_window_count,
        discarded_calibration_epoch_count: report.discarded_calibration_epoch_count,
        discarded_query_shape_count: report.discarded_query_shape_count,
        was_incomplete: report.was_incomplete,
        was_truncated: report.was_truncated,
    }
}

#[cfg(test)]
fn operator_physical_design_status(
    status: ServerPhysicalDesignStatus,
    allow_physical_index_apply: bool,
    runtime_token: Option<OperatorPhysicalDesignRuntimeToken>,
) -> OperatorPhysicalDesignStatusV5 {
    operator_physical_design_status_with_capabilities(
        status,
        allow_physical_index_apply,
        false,
        false,
        None,
        runtime_token,
    )
}

fn operator_physical_design_status_with_capabilities(
    status: ServerPhysicalDesignStatus,
    allow_physical_index_apply: bool,
    allow_physical_columnar_apply: bool,
    allow_physical_design_receipt_read: bool,
    columnar_capabilities: Option<ServerPhysicalColumnarApplyCapabilities>,
    runtime_token: Option<OperatorPhysicalDesignRuntimeToken>,
) -> OperatorPhysicalDesignStatusV5 {
    let diagnostics = status.diagnostics;
    let evidence = status.evidence;
    let columnar_apply = operator_physical_columnar_apply_capability(
        allow_physical_columnar_apply,
        columnar_capabilities,
    );
    OperatorPhysicalDesignStatusV5 {
        diagnostics: OperatorPhysicalDesignDiagnosticsV5 {
            eligible_query_count: diagnostics.eligible_query_count,
            record_success_count: diagnostics.record_success_count,
            record_error_count: diagnostics.record_error_count,
            schema_rotation_count: diagnostics.schema_rotation_count,
            capacity_rejection_count: diagnostics.capacity_rejection_count,
            incomplete_report_count: diagnostics.incomplete_report_count,
            counter_overflowed: diagnostics.counter_overflowed,
            last_record_outcome: diagnostics
                .last_record_outcome
                .map(physical_design_record_outcome),
            last_record_error: diagnostics
                .last_record_error
                .map(physical_design_record_error),
        },
        evidence: OperatorPhysicalDesignEvidenceStatusV5 {
            limits: OperatorPhysicalDesignEvidenceLimitsV5 {
                max_index_candidates: evidence.limits.max_index_candidates,
                max_columnar_candidates: evidence.limits.max_columnar_candidates,
                max_query_shapes_per_candidate: evidence.limits.max_query_shapes_per_candidate,
                max_columnar_columns_per_candidate: evidence
                    .limits
                    .max_columnar_columns_per_candidate,
            },
            epoch: evidence.epoch.0,
            schema_generation: evidence.schema_generation.map(|generation| generation.0),
            first_global_commit_seq: evidence.first_global_commit_seq.map(|sequence| sequence.0),
            last_global_commit_seq: evidence.last_global_commit_seq.map(|sequence| sequence.0),
            ordering_high_water: evidence.ordering_high_water.map(|sequence| sequence.0),
            recorded_reports: evidence.recorded_reports,
            index_candidate_count: evidence.index_candidate_count,
            columnar_candidate_count: evidence.columnar_candidate_count,
            capacity_rejections: evidence.capacity_rejections,
            discarded_incomplete_reports: evidence.discarded_incomplete_reports,
            overflowed: evidence.overflowed,
            incomplete: evidence.incomplete,
            truncated: evidence.truncated,
        },
        physical_index_apply: OperatorPhysicalIndexApplyStatusV5 {
            enabled: allow_physical_index_apply,
            runtime_token: allow_physical_index_apply
                .then(|| runtime_token.map(OperatorPhysicalDesignRuntimeToken::encode))
                .flatten(),
        },
        physical_columnar_apply: OperatorPhysicalColumnarApplyStatusV5 {
            enabled: columnar_apply.enabled,
            allow_snapshot: columnar_apply.allow_snapshot,
            allow_incremental: columnar_apply.allow_incremental,
            runtime_token: allow_physical_columnar_apply
                .then(|| runtime_token.map(OperatorPhysicalDesignRuntimeToken::encode))
                .flatten(),
        },
        physical_design_mutation_receipts: OperatorPhysicalDesignMutationReceiptCapabilityV5 {
            read_enabled: allow_physical_design_receipt_read,
        },
    }
}

fn operator_physical_columnar_apply_capability(
    enabled: bool,
    capabilities: Option<ServerPhysicalColumnarApplyCapabilities>,
) -> OperatorPhysicalColumnarApplyCapabilityV5 {
    OperatorPhysicalColumnarApplyCapabilityV5 {
        enabled,
        allow_snapshot: enabled
            && capabilities.is_some_and(|capabilities| capabilities.allow_snapshot),
        allow_incremental: enabled
            && capabilities.is_some_and(|capabilities| capabilities.allow_incremental),
    }
}

const fn physical_design_record_outcome(
    outcome: PhysicalDesignEvidenceRecordOutcome,
) -> OperatorPhysicalDesignRecordOutcomeV5 {
    match outcome {
        PhysicalDesignEvidenceRecordOutcome::Recorded => {
            OperatorPhysicalDesignRecordOutcomeV5::Recorded
        }
        PhysicalDesignEvidenceRecordOutcome::SchemaRotated => {
            OperatorPhysicalDesignRecordOutcomeV5::SchemaRotated
        }
        PhysicalDesignEvidenceRecordOutcome::RecordedWithCapacityRejection => {
            OperatorPhysicalDesignRecordOutcomeV5::RecordedWithCapacityRejection
        }
        PhysicalDesignEvidenceRecordOutcome::SchemaRotatedWithCapacityRejection => {
            OperatorPhysicalDesignRecordOutcomeV5::SchemaRotatedWithCapacityRejection
        }
    }
}

const fn physical_design_record_error(
    error: PhysicalDesignEvidenceRecordError,
) -> OperatorPhysicalDesignRecordErrorV5 {
    match error {
        PhysicalDesignEvidenceRecordError::GlobalVisibilityRequired => {
            OperatorPhysicalDesignRecordErrorV5::GlobalVisibilityRequired
        }
        PhysicalDesignEvidenceRecordError::StaleSchemaEvidence { .. } => {
            OperatorPhysicalDesignRecordErrorV5::StaleSchemaEvidence
        }
        PhysicalDesignEvidenceRecordError::OutOfOrderVisibility { .. } => {
            OperatorPhysicalDesignRecordErrorV5::OutOfOrderVisibility
        }
        PhysicalDesignEvidenceRecordError::EvidenceWindowEpochExhausted => {
            OperatorPhysicalDesignRecordErrorV5::EvidenceWindowEpochExhausted
        }
    }
}

fn operator_physical_design_report(
    report: PhysicalDesignAdvisorReport,
) -> OperatorPhysicalDesignAdvisorReportV5 {
    OperatorPhysicalDesignAdvisorReportV5 {
        evidence_epoch: report.evidence_epoch.0,
        schema_generation: report.schema_generation.0,
        first_global_commit_seq: report.first_global_commit_seq.0,
        last_global_commit_seq: report.last_global_commit_seq.0,
        recorded_reports: report.recorded_reports,
        discarded_incomplete_reports: report.discarded_incomplete_reports,
        overflowed: report.overflowed,
        incomplete: report.incomplete,
        index_candidates: report
            .index_candidates
            .into_iter()
            .map(operator_index_candidate)
            .collect(),
        columnar_candidates: report
            .columnar_candidates
            .into_iter()
            .map(operator_columnar_candidate)
            .collect(),
    }
}

fn operator_index_candidate(
    inspection: PhysicalIndexRecommendationInspection,
) -> OperatorPhysicalIndexCandidateV5 {
    OperatorPhysicalIndexCandidateV5 {
        table_id: inspection.candidate.table_id.0,
        column_id: inspection.candidate.column_id.0,
        point_report_count: inspection.point_report_count,
        range_report_count: inspection.range_report_count,
        evidence: operator_evidence_summary(inspection.evidence),
        decision: operator_design_decision(inspection.decision),
    }
}

fn operator_columnar_candidate(
    inspection: PhysicalColumnarRecommendationInspection,
) -> OperatorPhysicalColumnarCandidateV5 {
    OperatorPhysicalColumnarCandidateV5 {
        table_id: inspection.candidate.table_id.0,
        columns: inspection
            .candidate
            .columns
            .into_iter()
            .map(|column| column.0)
            .collect(),
        evidence: operator_evidence_summary(inspection.evidence),
        decision: operator_design_decision(inspection.decision),
    }
}

const fn operator_evidence_summary(
    evidence: PhysicalDesignEvidenceSummary,
) -> OperatorPhysicalDesignEvidenceSummaryV5 {
    OperatorPhysicalDesignEvidenceSummaryV5 {
        report_count: evidence.report_count,
        distinct_query_shapes: evidence.distinct_query_shapes,
        total_actual_scan_work_units: evidence.total_actual_scan_work_units,
        total_rows_examined: evidence.total_rows_examined,
        overflowed: evidence.overflowed,
        incomplete: evidence.incomplete,
        truncated: evidence.truncated,
    }
}

const fn operator_design_decision(
    decision: PhysicalDesignCandidateDecision,
) -> OperatorPhysicalDesignDecisionV5 {
    match decision {
        PhysicalDesignCandidateDecision::Recommend => {
            OperatorPhysicalDesignDecisionV5::Recommend {}
        }
        PhysicalDesignCandidateDecision::NoAction(reason) => {
            OperatorPhysicalDesignDecisionV5::NoAction {
                reason: operator_no_action_reason(reason),
            }
        }
    }
}

const fn operator_no_action_reason(
    reason: PhysicalDesignNoActionReason,
) -> OperatorPhysicalDesignNoActionReasonV5 {
    match reason {
        PhysicalDesignNoActionReason::BelowMinimumReports => {
            OperatorPhysicalDesignNoActionReasonV5::BelowMinimumReports
        }
        PhysicalDesignNoActionReason::BelowMinimumShapeDiversity => {
            OperatorPhysicalDesignNoActionReasonV5::BelowMinimumShapeDiversity
        }
        PhysicalDesignNoActionReason::BelowMinimumActualWork => {
            OperatorPhysicalDesignNoActionReasonV5::BelowMinimumActualWork
        }
        PhysicalDesignNoActionReason::ExistingDesignCovers => {
            OperatorPhysicalDesignNoActionReasonV5::ExistingDesignCovers
        }
        PhysicalDesignNoActionReason::UnsupportedCurrentLayout => {
            OperatorPhysicalDesignNoActionReasonV5::UnsupportedCurrentLayout
        }
        PhysicalDesignNoActionReason::IncompleteEvidence => {
            OperatorPhysicalDesignNoActionReasonV5::IncompleteEvidence
        }
        PhysicalDesignNoActionReason::CurrentProjectionUnavailable => {
            OperatorPhysicalDesignNoActionReasonV5::CurrentProjectionUnavailable
        }
        PhysicalDesignNoActionReason::RecommendationLimitReached => {
            OperatorPhysicalDesignNoActionReasonV5::RecommendationLimitReached
        }
    }
}

const fn operator_physical_design_rotation(
    report: ServerPhysicalDesignRotationReport,
) -> OperatorPhysicalDesignRotationV5 {
    OperatorPhysicalDesignRotationV5 {
        previous_epoch: report.previous_epoch.0,
        new_epoch: report.new_epoch.0,
    }
}

fn control_remote_error(error: ServerAdaptiveControlError) -> OperatorRemoteErrorV5 {
    let code = match error {
        ServerAdaptiveControlError::AdaptiveNotEnabled => OperatorErrorCodeV5::AdaptiveNotEnabled,
        ServerAdaptiveControlError::DriverNotEnabled => OperatorErrorCodeV5::DriverNotEnabled,
        ServerAdaptiveControlError::SchedulerNotFaulted => OperatorErrorCodeV5::SchedulerNotFaulted,
        ServerAdaptiveControlError::EvidenceWindowChanged { .. } => {
            OperatorErrorCodeV5::EvidenceWindowChanged
        }
        ServerAdaptiveControlError::EvidenceRotation(
            AdaptiveEvidenceRotationError::EvidenceWindowEpochExhausted,
        ) => OperatorErrorCodeV5::EvidenceWindowEpochExhausted,
        ServerAdaptiveControlError::ServerStopped => OperatorErrorCodeV5::ServerStopped,
    };
    let message = match code {
        OperatorErrorCodeV5::AdaptiveNotEnabled => "adaptive runtime is not enabled",
        OperatorErrorCodeV5::DriverNotEnabled => "adaptive driver is not enabled",
        OperatorErrorCodeV5::SchedulerNotFaulted => "adaptive scheduler is not faulted",
        OperatorErrorCodeV5::EvidenceWindowChanged => "adaptive evidence window changed",
        OperatorErrorCodeV5::EvidenceWindowEpochExhausted => {
            "adaptive evidence window epoch is exhausted"
        }
        OperatorErrorCodeV5::ServerStopped => "server adaptive control is stopped",
        _ => "operator request failed",
    };
    OperatorRemoteErrorV5 {
        code,
        message: message.into(),
        receipt: None,
    }
}

#[cfg(test)]
fn physical_columnar_remote_error(
    error: ServerPhysicalColumnarDesignControlError,
) -> OperatorRemoteErrorV5 {
    physical_columnar_remote_error_with_receipt(error, None)
}

fn physical_columnar_remote_error_with_receipt(
    error: ServerPhysicalColumnarDesignControlError,
    receipt: Option<OperatorPhysicalDesignMutationReceiptRefV5>,
) -> OperatorRemoteErrorV5 {
    if matches!(
        &error,
        ServerPhysicalColumnarDesignControlError::MutationRecoveryRequired(_)
            | ServerPhysicalColumnarDesignControlError::PostBeginMutationOutcomeUncertain(_)
    ) {
        return mutation_outcome_uncertain_remote_error(receipt);
    }
    if let ServerPhysicalColumnarDesignControlError::MutationReceipt(ref error) = error {
        let code = mutation_receipt_begin_error_code(error);
        return OperatorRemoteErrorV5 {
            code,
            message: mutation_receipt_begin_error_message(error).into(),
            receipt: None,
        };
    }
    if matches!(
        &error,
        ServerPhysicalColumnarDesignControlError::MutationOutcomeUncertain
            | ServerPhysicalColumnarDesignControlError::UnjournaledMutationOutcomeUncertain(_)
    ) {
        return mutation_outcome_uncertain_remote_error(None);
    }
    let code = match error {
        ServerPhysicalColumnarDesignControlError::PhysicalDesignNotEnabled => {
            OperatorErrorCodeV5::PhysicalDesignNotEnabled
        }
        ServerPhysicalColumnarDesignControlError::ColumnarApplyNotEnabled => {
            OperatorErrorCodeV5::PhysicalColumnarApplyNotEnabled
        }
        ServerPhysicalColumnarDesignControlError::ModeNotAllowed(_) => {
            OperatorErrorCodeV5::PhysicalColumnarModeNotAllowed
        }
        ServerPhysicalColumnarDesignControlError::PlacementRootUnavailable(_) => {
            OperatorErrorCodeV5::PhysicalColumnarPlacementUnavailable
        }
        ServerPhysicalColumnarDesignControlError::PlacementOccupied { .. } => {
            OperatorErrorCodeV5::PhysicalColumnarPlacementOccupied
        }
        ServerPhysicalColumnarDesignControlError::LocationConflict { .. } => {
            OperatorErrorCodeV5::PhysicalColumnarLocationConflict
        }
        ServerPhysicalColumnarDesignControlError::PlacementInspection { .. }
        | ServerPhysicalColumnarDesignControlError::LocationInspection(_) => {
            OperatorErrorCodeV5::PhysicalColumnarPlacementUnavailable
        }
        ServerPhysicalColumnarDesignControlError::EvidenceEpochChanged { .. } => {
            OperatorErrorCodeV5::PhysicalDesignEvidenceEpochChanged
        }
        ServerPhysicalColumnarDesignControlError::PhysicalDesignRuntimeChanged => {
            OperatorErrorCodeV5::PhysicalDesignRuntimeChanged
        }
        ServerPhysicalColumnarDesignControlError::Proposal(error) => {
            physical_columnar_proposal_code(*error)
        }
        ServerPhysicalColumnarDesignControlError::Apply(error) => {
            physical_columnar_apply_code(*error)
        }
        ServerPhysicalColumnarDesignControlError::MutationRecoveryRequired(_)
        | ServerPhysicalColumnarDesignControlError::MutationOutcomeUncertain
        | ServerPhysicalColumnarDesignControlError::PostBeginMutationOutcomeUncertain(_)
        | ServerPhysicalColumnarDesignControlError::UnjournaledMutationOutcomeUncertain(_) => {
            OperatorErrorCodeV5::PhysicalColumnarApplyFailed
        }
        ServerPhysicalColumnarDesignControlError::MutationReceipt(_) => {
            OperatorErrorCodeV5::PhysicalColumnarApplyFailed
        }
        ServerPhysicalColumnarDesignControlError::ProposalRuntimeChanged => {
            OperatorErrorCodeV5::PhysicalDesignRuntimeChanged
        }
        ServerPhysicalColumnarDesignControlError::PlacementInvariantViolated => {
            OperatorErrorCodeV5::PhysicalColumnarApplyFailed
        }
        ServerPhysicalColumnarDesignControlError::ServerStopped => {
            OperatorErrorCodeV5::ServerStopped
        }
    };
    let message = match code {
        OperatorErrorCodeV5::PhysicalDesignNotEnabled => "physical-design advisor is not enabled",
        OperatorErrorCodeV5::PhysicalColumnarApplyNotEnabled => {
            "physical-columnar apply is not enabled"
        }
        OperatorErrorCodeV5::PhysicalColumnarModeNotAllowed => {
            "physical-columnar mode is not allowed"
        }
        OperatorErrorCodeV5::PhysicalColumnarPlacementUnavailable => {
            "physical-columnar placement is unavailable"
        }
        OperatorErrorCodeV5::PhysicalColumnarPlacementOccupied => {
            "physical-columnar placement is occupied"
        }
        OperatorErrorCodeV5::PhysicalColumnarLocationConflict => {
            "physical-columnar placement conflicts with a registered projection"
        }
        OperatorErrorCodeV5::PhysicalColumnarCandidateNotObserved => {
            "physical-columnar candidate was not observed in current evidence"
        }
        OperatorErrorCodeV5::PhysicalColumnarNotRecommended => {
            "physical-columnar candidate is not currently recommended"
        }
        OperatorErrorCodeV5::PhysicalColumnarChangeStreamNotEnabled => {
            "incremental physical-columnar apply requires an enabled change stream"
        }
        OperatorErrorCodeV5::PhysicalColumnarChangeStreamUnavailable => {
            "incremental physical-columnar change stream is unavailable"
        }
        OperatorErrorCodeV5::PhysicalColumnarChangeStreamChanged => {
            "incremental physical-columnar change stream changed"
        }
        OperatorErrorCodeV5::PhysicalColumnarRecoveryRequired => {
            "physical-columnar publication is ambiguous; restart/reopen the daemon before retrying the exact approval"
        }
        OperatorErrorCodeV5::PhysicalColumnarApplyFailed => {
            "physical-columnar apply failed without creating a new projection"
        }
        OperatorErrorCodeV5::PhysicalDesignEvidenceEpochChanged => {
            "physical-design evidence epoch changed"
        }
        OperatorErrorCodeV5::PhysicalDesignRuntimeChanged => {
            "physical-design approval belongs to a previous daemon/operator runtime"
        }
        OperatorErrorCodeV5::ServerStopped => "server physical-design control is stopped",
        _ => "operator request failed",
    };
    OperatorRemoteErrorV5 {
        code,
        message: message.into(),
        receipt,
    }
}

fn physical_columnar_proposal_code(
    error: PhysicalColumnarDesignProposalError,
) -> OperatorErrorCodeV5 {
    match error {
        PhysicalColumnarDesignProposalError::Advisor(PhysicalDesignAdvisorError::NoEvidence) => {
            OperatorErrorCodeV5::PhysicalDesignNoEvidence
        }
        PhysicalColumnarDesignProposalError::Advisor(PhysicalDesignAdvisorError::StaleSchema {
            ..
        }) => OperatorErrorCodeV5::PhysicalDesignStaleSchema,
        PhysicalColumnarDesignProposalError::Advisor(
            PhysicalDesignAdvisorError::InconclusiveCapacity { .. },
        ) => OperatorErrorCodeV5::PhysicalDesignInconclusiveCapacity,
        PhysicalColumnarDesignProposalError::CandidateNotObserved(_) => {
            OperatorErrorCodeV5::PhysicalColumnarCandidateNotObserved
        }
        PhysicalColumnarDesignProposalError::CandidateNotRecommended { .. } => {
            OperatorErrorCodeV5::PhysicalColumnarNotRecommended
        }
        PhysicalColumnarDesignProposalError::IncrementalChangeStreamNotEnabled { .. } => {
            OperatorErrorCodeV5::PhysicalColumnarChangeStreamNotEnabled
        }
        PhysicalColumnarDesignProposalError::IncrementalChangeStreamUnavailable { .. } => {
            OperatorErrorCodeV5::PhysicalColumnarChangeStreamUnavailable
        }
        PhysicalColumnarDesignProposalError::ProjectionLocationConflict { .. } => {
            OperatorErrorCodeV5::PhysicalColumnarLocationConflict
        }
        PhysicalColumnarDesignProposalError::GlobalVisibilityRequired
        | PhysicalColumnarDesignProposalError::DurableCatalogRequired
        | PhysicalColumnarDesignProposalError::Advisor(PhysicalDesignAdvisorError::Database(_))
        | PhysicalColumnarDesignProposalError::Database(_) => {
            OperatorErrorCodeV5::PhysicalColumnarApplyFailed
        }
    }
}

fn physical_columnar_apply_code(error: PhysicalColumnarDesignApplyError) -> OperatorErrorCodeV5 {
    match error {
        PhysicalColumnarDesignApplyError::EvidenceEpochChanged { .. } => {
            OperatorErrorCodeV5::PhysicalDesignEvidenceEpochChanged
        }
        PhysicalColumnarDesignApplyError::CandidateNotObserved(_) => {
            OperatorErrorCodeV5::PhysicalColumnarCandidateNotObserved
        }
        PhysicalColumnarDesignApplyError::RecommendationNoLongerValid(_) => {
            OperatorErrorCodeV5::PhysicalColumnarNotRecommended
        }
        PhysicalColumnarDesignApplyError::StaleProposal(reason) => match reason {
            PhysicalColumnarDesignProposalStaleReason::ChangeStreamDisabled => {
                OperatorErrorCodeV5::PhysicalColumnarChangeStreamNotEnabled
            }
            PhysicalColumnarDesignProposalStaleReason::ChangeStreamUnavailable => {
                OperatorErrorCodeV5::PhysicalColumnarChangeStreamUnavailable
            }
            PhysicalColumnarDesignProposalStaleReason::ChangeStreamGenerationChanged { .. } => {
                OperatorErrorCodeV5::PhysicalColumnarChangeStreamChanged
            }
            _ => OperatorErrorCodeV5::PhysicalColumnarApplyFailed,
        },
        PhysicalColumnarDesignApplyError::ProjectionLocationConflict { .. } => {
            OperatorErrorCodeV5::PhysicalColumnarLocationConflict
        }
        PhysicalColumnarDesignApplyError::Advisor(PhysicalDesignAdvisorError::NoEvidence) => {
            OperatorErrorCodeV5::PhysicalDesignNoEvidence
        }
        PhysicalColumnarDesignApplyError::Advisor(PhysicalDesignAdvisorError::StaleSchema {
            ..
        }) => OperatorErrorCodeV5::PhysicalDesignStaleSchema,
        PhysicalColumnarDesignApplyError::Advisor(
            PhysicalDesignAdvisorError::InconclusiveCapacity { .. },
        ) => OperatorErrorCodeV5::PhysicalDesignInconclusiveCapacity,
        PhysicalColumnarDesignApplyError::Database(DatabaseError::ProjectionCatalog(
            ProjectionCatalogError::RecoveryRequired { .. },
        )) => OperatorErrorCodeV5::PhysicalColumnarRecoveryRequired,
        PhysicalColumnarDesignApplyError::DatabaseIdentityChanged
        | PhysicalColumnarDesignApplyError::Advisor(PhysicalDesignAdvisorError::Database(_))
        | PhysicalColumnarDesignApplyError::Database(_) => {
            OperatorErrorCodeV5::PhysicalColumnarApplyFailed
        }
    }
}

fn physical_design_remote_error(error: ServerPhysicalDesignControlError) -> OperatorRemoteErrorV5 {
    physical_design_remote_error_with_receipt(error, None)
}

fn physical_design_remote_error_with_receipt(
    error: ServerPhysicalDesignControlError,
    receipt: Option<OperatorPhysicalDesignMutationReceiptRefV5>,
) -> OperatorRemoteErrorV5 {
    if matches!(
        &error,
        ServerPhysicalDesignControlError::MutationRecoveryRequired(_)
            | ServerPhysicalDesignControlError::PostBeginMutationOutcomeUncertain(_)
    ) {
        return mutation_outcome_uncertain_remote_error(receipt);
    }
    if let ServerPhysicalDesignControlError::MutationReceipt(ref error) = error {
        let code = mutation_receipt_begin_error_code(error);
        return OperatorRemoteErrorV5 {
            code,
            message: mutation_receipt_begin_error_message(error).into(),
            receipt: None,
        };
    }
    if matches!(
        &error,
        ServerPhysicalDesignControlError::MutationOutcomeUncertain
            | ServerPhysicalDesignControlError::UnjournaledMutationOutcomeUncertain(_)
    ) {
        return mutation_outcome_uncertain_remote_error(None);
    }
    let code = match error {
        ServerPhysicalDesignControlError::PhysicalDesignNotEnabled => {
            OperatorErrorCodeV5::PhysicalDesignNotEnabled
        }
        ServerPhysicalDesignControlError::EvidenceEpochChanged { .. } => {
            OperatorErrorCodeV5::PhysicalDesignEvidenceEpochChanged
        }
        ServerPhysicalDesignControlError::EvidenceRotation(
            PhysicalDesignEvidenceRecordError::EvidenceWindowEpochExhausted,
        ) => OperatorErrorCodeV5::PhysicalDesignEvidenceEpochExhausted,
        ServerPhysicalDesignControlError::EvidenceRotation(_) => OperatorErrorCodeV5::Internal,
        ServerPhysicalDesignControlError::Advisor(PhysicalDesignAdvisorError::NoEvidence) => {
            OperatorErrorCodeV5::PhysicalDesignNoEvidence
        }
        ServerPhysicalDesignControlError::Advisor(PhysicalDesignAdvisorError::StaleSchema {
            ..
        }) => OperatorErrorCodeV5::PhysicalDesignStaleSchema,
        ServerPhysicalDesignControlError::Advisor(
            PhysicalDesignAdvisorError::InconclusiveCapacity { .. },
        ) => OperatorErrorCodeV5::PhysicalDesignInconclusiveCapacity,
        ServerPhysicalDesignControlError::Advisor(PhysicalDesignAdvisorError::Database(_)) => {
            OperatorErrorCodeV5::Internal
        }
        ServerPhysicalDesignControlError::Proposal(error) => match *error {
            PhysicalIndexDesignProposalError::Advisor(PhysicalDesignAdvisorError::NoEvidence) => {
                OperatorErrorCodeV5::PhysicalDesignNoEvidence
            }
            PhysicalIndexDesignProposalError::Advisor(
                PhysicalDesignAdvisorError::StaleSchema { .. },
            ) => OperatorErrorCodeV5::PhysicalDesignStaleSchema,
            PhysicalIndexDesignProposalError::Advisor(
                PhysicalDesignAdvisorError::InconclusiveCapacity { .. },
            ) => OperatorErrorCodeV5::PhysicalDesignInconclusiveCapacity,
            PhysicalIndexDesignProposalError::CandidateNotObserved(_) => {
                OperatorErrorCodeV5::PhysicalIndexCandidateNotObserved
            }
            PhysicalIndexDesignProposalError::CandidateNotRecommended { .. } => {
                OperatorErrorCodeV5::PhysicalIndexNotRecommended
            }
            PhysicalIndexDesignProposalError::GlobalVisibilityRequired
            | PhysicalIndexDesignProposalError::DurableCatalogRequired
            | PhysicalIndexDesignProposalError::Advisor(PhysicalDesignAdvisorError::Database(_))
            | PhysicalIndexDesignProposalError::Database(_) => {
                OperatorErrorCodeV5::PhysicalIndexApplyFailed
            }
        },
        ServerPhysicalDesignControlError::Apply(error) => match *error {
            PhysicalIndexDesignApplyError::EvidenceEpochChanged { .. } => {
                OperatorErrorCodeV5::PhysicalDesignEvidenceEpochChanged
            }
            PhysicalIndexDesignApplyError::CandidateNotObserved(_) => {
                OperatorErrorCodeV5::PhysicalIndexCandidateNotObserved
            }
            PhysicalIndexDesignApplyError::RecommendationNoLongerValid(_) => {
                OperatorErrorCodeV5::PhysicalIndexNotRecommended
            }
            PhysicalIndexDesignApplyError::Advisor(PhysicalDesignAdvisorError::NoEvidence) => {
                OperatorErrorCodeV5::PhysicalDesignNoEvidence
            }
            PhysicalIndexDesignApplyError::Advisor(PhysicalDesignAdvisorError::StaleSchema {
                ..
            }) => OperatorErrorCodeV5::PhysicalDesignStaleSchema,
            PhysicalIndexDesignApplyError::Advisor(
                PhysicalDesignAdvisorError::InconclusiveCapacity { .. },
            ) => OperatorErrorCodeV5::PhysicalDesignInconclusiveCapacity,
            PhysicalIndexDesignApplyError::IndexNameConflict(_) => {
                OperatorErrorCodeV5::PhysicalIndexNameConflict
            }
            PhysicalIndexDesignApplyError::DatabaseIdentityChanged
            | PhysicalIndexDesignApplyError::StaleProposal(_)
            | PhysicalIndexDesignApplyError::Advisor(PhysicalDesignAdvisorError::Database(_))
            | PhysicalIndexDesignApplyError::Database(_) => {
                OperatorErrorCodeV5::PhysicalIndexApplyFailed
            }
        },
        ServerPhysicalDesignControlError::PhysicalDesignRuntimeChanged
        | ServerPhysicalDesignControlError::ProposalRuntimeChanged => {
            OperatorErrorCodeV5::PhysicalDesignRuntimeChanged
        }
        ServerPhysicalDesignControlError::PhysicalIndexNameConflict(_) => {
            OperatorErrorCodeV5::PhysicalIndexNameConflict
        }
        ServerPhysicalDesignControlError::ServerStopped => OperatorErrorCodeV5::ServerStopped,
        ServerPhysicalDesignControlError::MutationOutcomeUncertain
        | ServerPhysicalDesignControlError::UnjournaledMutationOutcomeUncertain(_)
        | ServerPhysicalDesignControlError::MutationRecoveryRequired(_)
        | ServerPhysicalDesignControlError::PostBeginMutationOutcomeUncertain(_)
        | ServerPhysicalDesignControlError::MutationReceipt(_) => {
            OperatorErrorCodeV5::PhysicalIndexApplyFailed
        }
    };
    let message = match code {
        OperatorErrorCodeV5::PhysicalDesignNotEnabled => "physical-design advisor is not enabled",
        OperatorErrorCodeV5::PhysicalDesignEvidenceEpochChanged => {
            "physical-design evidence epoch changed"
        }
        OperatorErrorCodeV5::PhysicalDesignEvidenceEpochExhausted => {
            "physical-design evidence epoch is exhausted"
        }
        OperatorErrorCodeV5::PhysicalDesignNoEvidence => "physical-design evidence window is empty",
        OperatorErrorCodeV5::PhysicalDesignStaleSchema => {
            "physical-design evidence schema is stale"
        }
        OperatorErrorCodeV5::PhysicalDesignInconclusiveCapacity => {
            "physical-design evidence is capacity-truncated"
        }
        OperatorErrorCodeV5::PhysicalDesignRuntimeChanged => {
            "physical-design approval belongs to a previous daemon/operator runtime"
        }
        OperatorErrorCodeV5::PhysicalIndexCandidateNotObserved => {
            "physical-index candidate was not observed in current evidence"
        }
        OperatorErrorCodeV5::PhysicalIndexNotRecommended => {
            "physical-index candidate is not currently recommended"
        }
        OperatorErrorCodeV5::PhysicalIndexNameConflict => {
            "physical-index name is already bound to another target"
        }
        OperatorErrorCodeV5::PhysicalIndexApplyFailed => {
            "physical-index apply failed without creating a new index"
        }
        OperatorErrorCodeV5::ServerStopped => "server physical-design control is stopped",
        _ => "operator request failed",
    };
    OperatorRemoteErrorV5 {
        code,
        message: message.into(),
        receipt,
    }
}

fn mutation_receipt_begin_error_code(
    error: &ServerPhysicalDesignMutationReceiptControlError,
) -> OperatorErrorCodeV5 {
    match error {
        ServerPhysicalDesignMutationReceiptControlError::Journal(
            ServerPhysicalDesignMutationReceiptJournalError::CapacityExceeded,
        ) => OperatorErrorCodeV5::PhysicalDesignMutationReceiptCapacityExceeded,
        _ => OperatorErrorCodeV5::PhysicalDesignMutationReceiptUnavailable,
    }
}

const fn mutation_receipt_begin_error_message(
    error: &ServerPhysicalDesignMutationReceiptControlError,
) -> &'static str {
    match error {
        ServerPhysicalDesignMutationReceiptControlError::Journal(
            ServerPhysicalDesignMutationReceiptJournalError::CapacityExceeded,
        ) => "physical-design mutation receipt journal capacity is exhausted; no mutation occurred",
        ServerPhysicalDesignMutationReceiptControlError::RecoveryRequired
        | ServerPhysicalDesignMutationReceiptControlError::Journal(
            ServerPhysicalDesignMutationReceiptJournalError::Io { .. },
        ) => {
            "physical-design mutation receipt journal is unavailable; no mutation occurred for this request; restart/reopen is required before another receipted mutation"
        }
        _ => "physical-design mutation receipt journal is unavailable; no mutation occurred",
    }
}

fn mutation_outcome_uncertain_remote_error(
    receipt: Option<OperatorPhysicalDesignMutationReceiptRefV5>,
) -> OperatorRemoteErrorV5 {
    let message = if receipt.is_some() {
        "physical-design mutation outcome is uncertain; restart/reopen the daemon, allow NBMR startup reconciliation to complete, then inspect the referenced receipt before deciding whether any retry is needed"
    } else {
        "physical-design mutation outcome is uncertain; no receipt reference was observed; the same exact approval may be used for idempotent discovery"
    };
    OperatorRemoteErrorV5 {
        code: OperatorErrorCodeV5::PhysicalDesignMutationOutcomeUncertain,
        message: message.into(),
        receipt,
    }
}

fn response_too_large_error() -> OperatorRemoteErrorV5 {
    OperatorRemoteErrorV5 {
        code: OperatorErrorCodeV5::ResponseTooLarge,
        message: "operator response exceeds the NBOP payload limit".into(),
        receipt: None,
    }
}

fn protocol_remote_error(error: &OperatorProtocolError) -> OperatorRemoteErrorV5 {
    let (code, message) = match error {
        OperatorProtocolError::UnsupportedVersion(_) => (
            OperatorErrorCodeV5::UnsupportedProtocolVersion,
            "unsupported operator protocol version",
        ),
        OperatorProtocolError::RequestTooLarge(_) | OperatorProtocolError::PayloadTooLarge(_) => (
            OperatorErrorCodeV5::RequestTooLarge,
            "operator request is too large",
        ),
        OperatorProtocolError::TruncatedHeader | OperatorProtocolError::TruncatedPayload => (
            OperatorErrorCodeV5::MalformedRequest,
            "truncated operator request",
        ),
        OperatorProtocolError::WrongMagic => (
            OperatorErrorCodeV5::MalformedRequest,
            "invalid operator request magic",
        ),
        OperatorProtocolError::NonzeroReserved(_) => (
            OperatorErrorCodeV5::MalformedRequest,
            "operator reserved field is nonzero",
        ),
        OperatorProtocolError::InvalidJson(_) => (
            OperatorErrorCodeV5::MalformedRequest,
            "invalid operator request JSON",
        ),
        OperatorProtocolError::Io(_) => (
            OperatorErrorCodeV5::MalformedRequest,
            "operator request I/O failed",
        ),
    };
    OperatorRemoteErrorV5 {
        code,
        message: message.into(),
        receipt: None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;
    use crate::adaptive_driver::ServerAdaptiveControlRequest;
    use crate::physical_design::ServerPhysicalDesignControlRequest;

    static NEXT_PATH: AtomicU64 = AtomicU64::new(1);

    #[cfg(unix)]
    fn socket_fixture(name: &str) -> (PathBuf, ServerOperatorConfig) {
        let sequence = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        let directory =
            PathBuf::from("/tmp").join(format!("nbop-{name}-{}-{sequence}", std::process::id()));
        std::fs::create_dir_all(&directory).unwrap();
        let config = ServerOperatorConfig::new(
            directory.join("operator.sock"),
            Duration::from_millis(100),
            false,
        )
        .unwrap();
        (directory, config)
    }

    #[cfg(unix)]
    fn serve_one_wrong_mutation_result(path: PathBuf) -> std::thread::JoinHandle<()> {
        use std::os::unix::net::UnixListener;

        let listener = UnixListener::bind(path).unwrap();
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let request: OperatorRequestV5 = read_frame(&mut stream).unwrap();
            write_frame(
                &mut stream,
                &OperatorResponseV5::Ok {
                    request_id: request.request_id,
                    result: OperatorResultV5::SchedulerReset {},
                },
            )
            .unwrap();
        })
    }

    #[cfg(unix)]
    fn serve_one_lost_mutation_response(path: PathBuf) -> std::thread::JoinHandle<()> {
        use std::os::unix::net::UnixListener;

        let listener = UnixListener::bind(path).unwrap();
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let _: OperatorRequestV5 = read_frame(&mut stream).unwrap();
        })
    }

    #[cfg(unix)]
    #[test]
    fn index_unexpected_result_is_uncertain() {
        let (directory, config) = socket_fixture("index-unexpected-result");
        let server = serve_one_wrong_mutation_result(config.unix_socket().to_path_buf());
        let error = ServerOperatorClient::new(&config)
            .apply_physical_index("11".repeat(16), 1, 2, 3, "events_idx")
            .unwrap_err();
        assert!(matches!(
            error,
            OperatorClientError::MutationOutcomeUncertain {
                recovery_required: false,
                receipt: None,
                source,
            } if matches!(*source, OperatorClientError::UnexpectedResult)
        ));
        server.join().unwrap();
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn columnar_unexpected_result_is_uncertain() {
        let (directory, config) = socket_fixture("columnar-unexpected-result");
        let server = serve_one_wrong_mutation_result(config.unix_socket().to_path_buf());
        let error = ServerOperatorClient::new(&config)
            .apply_physical_columnar(
                "11".repeat(16),
                1,
                2,
                vec![3],
                OperatorPhysicalColumnarDesignModeV5::Snapshot,
                "events",
            )
            .unwrap_err();
        assert!(matches!(
            error,
            OperatorClientError::MutationOutcomeUncertain {
                recovery_required: false,
                receipt: None,
                source,
            } if matches!(*source, OperatorClientError::UnexpectedResult)
        ));
        server.join().unwrap();
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn index_lost_response_after_dispatch_is_uncertain_without_receipt() {
        let (directory, config) = socket_fixture("index-lost-response");
        let server = serve_one_lost_mutation_response(config.unix_socket().to_path_buf());
        let error = ServerOperatorClient::new(&config)
            .apply_physical_index("11".repeat(16), 1, 2, 3, "events_idx")
            .unwrap_err();
        assert!(matches!(
            error,
            OperatorClientError::MutationOutcomeUncertain {
                recovery_required: false,
                receipt: None,
                source,
            } if matches!(
                *source,
                OperatorClientError::Protocol(OperatorProtocolError::TruncatedHeader)
            )
        ));
        server.join().unwrap();
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn mutating_request_encoding_failure_is_definitely_pre_dispatch() {
        let (directory, config) = socket_fixture("predispatch-size");
        let error = ServerOperatorClient::new(&config)
            .apply_physical_index("11".repeat(16), 1, 2, 3, "x".repeat(70_000))
            .unwrap_err();
        assert!(matches!(
            error,
            OperatorClientError::Protocol(OperatorProtocolError::PayloadTooLarge(_))
        ));
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[cfg(unix)]
    fn idle_plane(
        config: ServerOperatorConfig,
    ) -> Result<ServerOperatorPlane, ServerOperatorError> {
        let (control_tx, _control_rx) = std::sync::mpsc::channel();
        let (design_tx, _design_rx) = std::sync::mpsc::channel();
        let (server_shutdown, _server_shutdown_rx) = std::sync::mpsc::channel();
        ServerOperatorPlane::start(
            config,
            ServerAdaptiveControlHandle::new(control_tx),
            ServerPhysicalDesignControlHandle::new(design_tx),
            server_shutdown,
        )
    }

    #[test]
    fn frame_header_is_exact_big_endian_nbop_v5() {
        let request = OperatorRequestV5 {
            request_id: 42,
            operation: OperatorOperationV5::Status {},
        };
        let mut bytes = Vec::new();
        write_frame(&mut bytes, &request).unwrap();
        assert_eq!(&bytes[..4], b"NBOP");
        assert_eq!(&bytes[4..6], &[0, 5]);
        assert_eq!(&bytes[6..8], &[0, 0]);
        assert_eq!(
            u32::from_be_bytes(bytes[8..12].try_into().unwrap()) as usize,
            bytes.len() - OPERATOR_HEADER_BYTES
        );
        let decoded: OperatorRequestV5 = read_frame(&mut bytes.as_slice()).unwrap();
        assert_eq!(decoded.request_id, 42);
        assert!(matches!(decoded.operation, OperatorOperationV5::Status {}));
    }

    #[test]
    fn response_frame_and_request_id_echo_are_stable() {
        let response = OperatorResponseV5::Ok {
            request_id: 42,
            result: OperatorResultV5::SchedulerReset {},
        };
        let mut bytes = Vec::new();
        write_frame(&mut bytes, &response).unwrap();
        let payload = br#"{"outcome":"ok","request_id":42,"result":{"type":"scheduler_reset"}}"#;
        assert_eq!(&bytes[..4], b"NBOP");
        assert_eq!(&bytes[4..8], &[0, 5, 0, 0]);
        assert_eq!(&bytes[8..12], &(payload.len() as u32).to_be_bytes());
        assert_eq!(&bytes[12..], payload);
    }

    #[test]
    fn phase30_recommendation_payload_remains_shape_compatible_inside_v5() {
        #[derive(Debug, Deserialize)]
        #[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
        enum FrozenResponse {
            Ok {
                request_id: u64,
                result: FrozenResult,
            },
            Error {
                request_id: u64,
                error: OperatorRemoteErrorV5,
            },
        }

        #[derive(Debug, Deserialize)]
        #[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
        enum FrozenResult {
            PhysicalDesignRecommendations {
                runtime_token: Option<String>,
                report: OperatorPhysicalDesignAdvisorReportV5,
            },
        }

        let payload = br#"{"outcome":"ok","request_id":30,"result":{"type":"physical_design_recommendations","runtime_token":"00112233445566778899aabbccddeeff","report":{"evidence_epoch":1,"schema_generation":2,"first_global_commit_seq":3,"last_global_commit_seq":4,"recorded_reports":5,"discarded_incomplete_reports":0,"overflowed":false,"incomplete":false,"index_candidates":[],"columnar_candidates":[]}}}"#;
        let mut frozen_frame = Vec::new();
        frozen_frame.extend_from_slice(b"NBOP");
        frozen_frame.extend_from_slice(&OPERATOR_PROTOCOL_VERSION.to_be_bytes());
        frozen_frame.extend_from_slice(&0_u16.to_be_bytes());
        frozen_frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        frozen_frame.extend_from_slice(payload);
        assert!(matches!(
            read_frame::<OperatorResponseV5>(&mut frozen_frame.as_slice()).unwrap(),
            OperatorResponseV5::Ok {
                request_id: 30,
                result: OperatorResultV5::PhysicalDesignRecommendations {
                    runtime_token: Some(_),
                    ..
                }
            }
        ));

        let response = OperatorResponseV5::Ok {
            request_id: 31,
            result: OperatorResultV5::PhysicalDesignRecommendations {
                runtime_token: None,
                report: OperatorPhysicalDesignAdvisorReportV5 {
                    evidence_epoch: 1,
                    schema_generation: 2,
                    first_global_commit_seq: 3,
                    last_global_commit_seq: 4,
                    recorded_reports: 5,
                    discarded_incomplete_reports: 0,
                    overflowed: false,
                    incomplete: false,
                    index_candidates: Vec::new(),
                    columnar_candidates: Vec::new(),
                },
            },
        };
        let mut current_frame = Vec::new();
        write_frame(&mut current_frame, &response).unwrap();
        let frozen: FrozenResponse = read_frame(&mut current_frame.as_slice()).unwrap();
        match frozen {
            FrozenResponse::Ok {
                request_id,
                result:
                    FrozenResult::PhysicalDesignRecommendations {
                        runtime_token,
                        report,
                    },
            } => {
                assert_eq!(request_id, 31);
                assert_eq!(runtime_token, None);
                assert_eq!(report.evidence_epoch, 1);
            }
            FrozenResponse::Error { request_id, error } => {
                panic!("unexpected frozen error {request_id}: {error:?}")
            }
        }
        assert_eq!(
            serde_json::from_str::<OperatorErrorCodeV5>(
                "\"physical_design_mutation_outcome_uncertain\""
            )
            .unwrap(),
            OperatorErrorCodeV5::PhysicalDesignMutationOutcomeUncertain
        );
        assert_eq!(
            serde_json::from_str::<OperatorErrorCodeV5>("\"internal\"").unwrap(),
            OperatorErrorCodeV5::Internal
        );
    }

    #[test]
    fn nbop_v5_error_set_is_exact() {
        fn frozen_wire_name(code: OperatorErrorCodeV5) -> &'static str {
            match code {
                OperatorErrorCodeV5::AdaptiveNotEnabled => "adaptive_not_enabled",
                OperatorErrorCodeV5::DriverNotEnabled => "driver_not_enabled",
                OperatorErrorCodeV5::SchedulerNotFaulted => "scheduler_not_faulted",
                OperatorErrorCodeV5::EvidenceWindowChanged => "evidence_window_changed",
                OperatorErrorCodeV5::EvidenceWindowEpochExhausted => {
                    "evidence_window_epoch_exhausted"
                }
                OperatorErrorCodeV5::PhysicalDesignNotEnabled => "physical_design_not_enabled",
                OperatorErrorCodeV5::PhysicalDesignEvidenceEpochChanged => {
                    "physical_design_evidence_epoch_changed"
                }
                OperatorErrorCodeV5::PhysicalDesignEvidenceEpochExhausted => {
                    "physical_design_evidence_epoch_exhausted"
                }
                OperatorErrorCodeV5::PhysicalDesignNoEvidence => "physical_design_no_evidence",
                OperatorErrorCodeV5::PhysicalDesignStaleSchema => "physical_design_stale_schema",
                OperatorErrorCodeV5::PhysicalDesignInconclusiveCapacity => {
                    "physical_design_inconclusive_capacity"
                }
                OperatorErrorCodeV5::PhysicalIndexApplyNotEnabled => {
                    "physical_index_apply_not_enabled"
                }
                OperatorErrorCodeV5::PhysicalDesignRuntimeChanged => {
                    "physical_design_runtime_changed"
                }
                OperatorErrorCodeV5::InvalidIndexName => "invalid_index_name",
                OperatorErrorCodeV5::PhysicalIndexCandidateNotObserved => {
                    "physical_index_candidate_not_observed"
                }
                OperatorErrorCodeV5::PhysicalIndexNotRecommended => {
                    "physical_index_not_recommended"
                }
                OperatorErrorCodeV5::PhysicalIndexNameConflict => "physical_index_name_conflict",
                OperatorErrorCodeV5::PhysicalIndexApplyFailed => "physical_index_apply_failed",
                OperatorErrorCodeV5::PhysicalColumnarApplyNotEnabled => {
                    "physical_columnar_apply_not_enabled"
                }
                OperatorErrorCodeV5::InvalidPhysicalColumnarPlacementKey => {
                    "invalid_physical_columnar_placement_key"
                }
                OperatorErrorCodeV5::PhysicalColumnarModeNotAllowed => {
                    "physical_columnar_mode_not_allowed"
                }
                OperatorErrorCodeV5::PhysicalColumnarPlacementUnavailable => {
                    "physical_columnar_placement_unavailable"
                }
                OperatorErrorCodeV5::PhysicalColumnarPlacementOccupied => {
                    "physical_columnar_placement_occupied"
                }
                OperatorErrorCodeV5::PhysicalColumnarLocationConflict => {
                    "physical_columnar_location_conflict"
                }
                OperatorErrorCodeV5::PhysicalColumnarCandidateNotObserved => {
                    "physical_columnar_candidate_not_observed"
                }
                OperatorErrorCodeV5::PhysicalColumnarNotRecommended => {
                    "physical_columnar_not_recommended"
                }
                OperatorErrorCodeV5::PhysicalColumnarChangeStreamNotEnabled => {
                    "physical_columnar_change_stream_not_enabled"
                }
                OperatorErrorCodeV5::PhysicalColumnarChangeStreamUnavailable => {
                    "physical_columnar_change_stream_unavailable"
                }
                OperatorErrorCodeV5::PhysicalColumnarChangeStreamChanged => {
                    "physical_columnar_change_stream_changed"
                }
                OperatorErrorCodeV5::PhysicalColumnarRecoveryRequired => {
                    "physical_columnar_recovery_required"
                }
                OperatorErrorCodeV5::PhysicalColumnarApplyFailed => {
                    "physical_columnar_apply_failed"
                }
                OperatorErrorCodeV5::PhysicalDesignMutationOutcomeUncertain => {
                    "physical_design_mutation_outcome_uncertain"
                }
                OperatorErrorCodeV5::PhysicalDesignMutationReceiptCapacityExceeded => {
                    "physical_design_mutation_receipt_capacity_exceeded"
                }
                OperatorErrorCodeV5::PhysicalDesignMutationReceiptUnavailable => {
                    "physical_design_mutation_receipt_unavailable"
                }
                OperatorErrorCodeV5::PhysicalDesignMutationReceiptReadNotAllowed => {
                    "physical_design_mutation_receipt_read_not_allowed"
                }
                OperatorErrorCodeV5::PhysicalDesignMutationReceiptsNotEnabled => {
                    "physical_design_mutation_receipts_not_enabled"
                }
                OperatorErrorCodeV5::InvalidPhysicalDesignMutationReceiptCursor => {
                    "invalid_physical_design_mutation_receipt_cursor"
                }
                OperatorErrorCodeV5::PhysicalDesignMutationReceiptJournalChanged => {
                    "physical_design_mutation_receipt_journal_changed"
                }
                OperatorErrorCodeV5::InvalidPhysicalDesignMutationReceiptLimit => {
                    "invalid_physical_design_mutation_receipt_limit"
                }
                OperatorErrorCodeV5::PhysicalDesignMutationReceiptReadFailed => {
                    "physical_design_mutation_receipt_read_failed"
                }
                OperatorErrorCodeV5::ResponseTooLarge => "response_too_large",
                OperatorErrorCodeV5::ServerStopped => "server_stopped",
                OperatorErrorCodeV5::MalformedRequest => "malformed_request",
                OperatorErrorCodeV5::UnsupportedProtocolVersion => "unsupported_protocol_version",
                OperatorErrorCodeV5::RequestTooLarge => "request_too_large",
                OperatorErrorCodeV5::Internal => "internal",
            }
        }

        let frozen_codes = [
            OperatorErrorCodeV5::AdaptiveNotEnabled,
            OperatorErrorCodeV5::DriverNotEnabled,
            OperatorErrorCodeV5::SchedulerNotFaulted,
            OperatorErrorCodeV5::EvidenceWindowChanged,
            OperatorErrorCodeV5::EvidenceWindowEpochExhausted,
            OperatorErrorCodeV5::PhysicalDesignNotEnabled,
            OperatorErrorCodeV5::PhysicalDesignEvidenceEpochChanged,
            OperatorErrorCodeV5::PhysicalDesignEvidenceEpochExhausted,
            OperatorErrorCodeV5::PhysicalDesignNoEvidence,
            OperatorErrorCodeV5::PhysicalDesignStaleSchema,
            OperatorErrorCodeV5::PhysicalDesignInconclusiveCapacity,
            OperatorErrorCodeV5::PhysicalIndexApplyNotEnabled,
            OperatorErrorCodeV5::PhysicalDesignRuntimeChanged,
            OperatorErrorCodeV5::InvalidIndexName,
            OperatorErrorCodeV5::PhysicalIndexCandidateNotObserved,
            OperatorErrorCodeV5::PhysicalIndexNotRecommended,
            OperatorErrorCodeV5::PhysicalIndexNameConflict,
            OperatorErrorCodeV5::PhysicalIndexApplyFailed,
            OperatorErrorCodeV5::PhysicalColumnarApplyNotEnabled,
            OperatorErrorCodeV5::InvalidPhysicalColumnarPlacementKey,
            OperatorErrorCodeV5::PhysicalColumnarModeNotAllowed,
            OperatorErrorCodeV5::PhysicalColumnarPlacementUnavailable,
            OperatorErrorCodeV5::PhysicalColumnarPlacementOccupied,
            OperatorErrorCodeV5::PhysicalColumnarLocationConflict,
            OperatorErrorCodeV5::PhysicalColumnarCandidateNotObserved,
            OperatorErrorCodeV5::PhysicalColumnarNotRecommended,
            OperatorErrorCodeV5::PhysicalColumnarChangeStreamNotEnabled,
            OperatorErrorCodeV5::PhysicalColumnarChangeStreamUnavailable,
            OperatorErrorCodeV5::PhysicalColumnarChangeStreamChanged,
            OperatorErrorCodeV5::PhysicalColumnarRecoveryRequired,
            OperatorErrorCodeV5::PhysicalColumnarApplyFailed,
            OperatorErrorCodeV5::PhysicalDesignMutationOutcomeUncertain,
            OperatorErrorCodeV5::PhysicalDesignMutationReceiptCapacityExceeded,
            OperatorErrorCodeV5::PhysicalDesignMutationReceiptUnavailable,
            OperatorErrorCodeV5::PhysicalDesignMutationReceiptReadNotAllowed,
            OperatorErrorCodeV5::PhysicalDesignMutationReceiptsNotEnabled,
            OperatorErrorCodeV5::InvalidPhysicalDesignMutationReceiptCursor,
            OperatorErrorCodeV5::PhysicalDesignMutationReceiptJournalChanged,
            OperatorErrorCodeV5::InvalidPhysicalDesignMutationReceiptLimit,
            OperatorErrorCodeV5::PhysicalDesignMutationReceiptReadFailed,
            OperatorErrorCodeV5::ResponseTooLarge,
            OperatorErrorCodeV5::ServerStopped,
            OperatorErrorCodeV5::MalformedRequest,
            OperatorErrorCodeV5::UnsupportedProtocolVersion,
            OperatorErrorCodeV5::RequestTooLarge,
            OperatorErrorCodeV5::Internal,
        ];
        assert_eq!(frozen_codes.len(), 46);
        for code in frozen_codes {
            assert_eq!(
                serde_json::to_string(&code).unwrap(),
                format!("\"{}\"", frozen_wire_name(code))
            );
        }
    }

    #[test]
    fn v5_columnar_request_and_response_json_contract_is_stable() {
        let request = OperatorRequestV5 {
            request_id: 7,
            operation: OperatorOperationV5::ApplyPhysicalColumnar {
                expected_runtime_token: "00112233445566778899aabbccddeeff".into(),
                expected_evidence_epoch: 3,
                table_id: 5,
                columns: vec![2, 4],
                mode: OperatorPhysicalColumnarDesignModeV5::Snapshot,
                placement_key: "users-v1".into(),
            },
        };
        assert_eq!(
            serde_json::to_value(request).unwrap(),
            serde_json::json!({
                "request_id": 7,
                "operation": {
                    "type": "apply_physical_columnar",
                    "expected_runtime_token": "00112233445566778899aabbccddeeff",
                    "expected_evidence_epoch": 3,
                    "table_id": 5,
                    "columns": [2, 4],
                    "mode": "snapshot",
                    "placement_key": "users-v1"
                }
            })
        );

        let response = OperatorResponseV5::Ok {
            request_id: 7,
            result: OperatorResultV5::PhysicalColumnarApplied {
                apply: OperatorPhysicalColumnarApplyResultV5 {
                    table_id: 5,
                    columns: vec![2, 4],
                    mode: OperatorPhysicalColumnarDesignModeV5::Snapshot,
                    placement_key: "users-v1".into(),
                    outcome: OperatorPhysicalColumnarApplyOutcomeV5::Created { projection_id: 9 },
                    receipt: None,
                },
            },
        };
        assert_eq!(
            serde_json::to_value(response).unwrap(),
            serde_json::json!({
                "outcome": "ok",
                "request_id": 7,
                "result": {
                    "type": "physical_columnar_applied",
                    "apply": {
                        "table_id": 5,
                        "columns": [2, 4],
                        "mode": "snapshot",
                        "placement_key": "users-v1",
                        "outcome": {"kind": "created", "projection_id": 9},
                        "receipt": null
                    }
                }
            })
        );
        assert_eq!(
            serde_json::to_string(&OperatorErrorCodeV5::PhysicalColumnarApplyNotEnabled).unwrap(),
            "\"physical_columnar_apply_not_enabled\""
        );
    }

    #[test]
    fn receipt_request_json_is_scoped_strict_and_has_no_bare_cursor() {
        let status = OperatorRequestV5 {
            request_id: 32,
            operation: OperatorOperationV5::PhysicalDesignMutationReceiptStatus {},
        };
        assert_eq!(
            serde_json::to_value(status).unwrap(),
            serde_json::json!({
                "request_id": 32,
                "operation": {"type": "physical_design_mutation_receipt_status"}
            })
        );

        let initial = OperatorRequestV5 {
            request_id: 33,
            operation: OperatorOperationV5::PhysicalDesignMutationReceipts {
                after: None,
                limit: 32,
            },
        };
        assert_eq!(
            serde_json::to_value(initial).unwrap(),
            serde_json::json!({
                "request_id": 33,
                "operation": {
                    "type": "physical_design_mutation_receipts",
                    "after": null,
                    "limit": 32
                }
            })
        );

        let continuation = OperatorRequestV5 {
            request_id: 34,
            operation: OperatorOperationV5::PhysicalDesignMutationReceipts {
                after: Some(OperatorPhysicalDesignMutationReceiptCursorV5 {
                    journal_incarnation: "00112233445566778899aabbccddeeff".into(),
                    receipt_id: 41,
                }),
                limit: 32,
            },
        };
        assert_eq!(
            serde_json::to_value(continuation).unwrap()["operation"]["after"],
            serde_json::json!({
                "journal_incarnation": "00112233445566778899aabbccddeeff",
                "receipt_id": 41
            })
        );

        for operation in [
            serde_json::json!({
                "type": "physical_design_mutation_receipts",
                "after": null,
                "limit": 1,
                "unknown": true
            }),
            serde_json::json!({
                "type": "physical_design_mutation_receipts",
                "after_receipt_id": 41,
                "limit": 1
            }),
        ] {
            assert!(
                serde_json::from_value::<OperatorRequestV5>(serde_json::json!({
                    "request_id": 1,
                    "operation": operation
                }))
                .is_err()
            );
        }
    }

    #[test]
    fn receipt_cursor_encoding_is_exact_and_invalid_forms_never_reach_the_worker() {
        let (adaptive_tx, _adaptive_rx) = std::sync::mpsc::channel();
        let adaptive = ServerAdaptiveControlHandle::new(adaptive_tx);
        for incarnation in [
            "00112233445566778899AABBCCDDEEFF",
            "00112233445566778899aabbccddee",
            "00112233-4455-6677-8899-aabbccddeeff",
            "0x00112233445566778899aabbccddeeff",
            " 00112233445566778899aabbccddeeff",
        ] {
            let (design_tx, design_rx) = std::sync::mpsc::channel();
            let response = execute_operator_request_with_capabilities(
                OperatorRequestV5 {
                    request_id: 1,
                    operation: OperatorOperationV5::PhysicalDesignMutationReceipts {
                        after: Some(OperatorPhysicalDesignMutationReceiptCursorV5 {
                            journal_incarnation: incarnation.into(),
                            receipt_id: 1,
                        }),
                        limit: 1,
                    },
                },
                &adaptive,
                &ServerPhysicalDesignControlHandle::new(design_tx),
                OperatorListenerPolicy::new(false, false, true, None, None),
            );
            assert!(matches!(
                response,
                OperatorResponseV5::Error {
                    error: OperatorRemoteErrorV5 {
                        code: OperatorErrorCodeV5::InvalidPhysicalDesignMutationReceiptCursor,
                        receipt: None,
                        ..
                    },
                    ..
                }
            ));
            assert!(design_rx.try_recv().is_err());
        }

        let (design_tx, design_rx) = std::sync::mpsc::channel();
        let response = execute_operator_request_with_capabilities(
            OperatorRequestV5 {
                request_id: 2,
                operation: OperatorOperationV5::PhysicalDesignMutationReceipts {
                    after: Some(OperatorPhysicalDesignMutationReceiptCursorV5 {
                        journal_incarnation: "00112233445566778899aabbccddeeff".into(),
                        receipt_id: 0,
                    }),
                    limit: 1,
                },
            },
            &adaptive,
            &ServerPhysicalDesignControlHandle::new(design_tx),
            OperatorListenerPolicy::new(false, false, true, None, None),
        );
        assert!(matches!(
            response,
            OperatorResponseV5::Error {
                error: OperatorRemoteErrorV5 {
                    code: OperatorErrorCodeV5::InvalidPhysicalDesignMutationReceiptCursor,
                    ..
                },
                ..
            }
        ));
        assert!(design_rx.try_recv().is_err());
    }

    #[test]
    fn receipt_read_permission_precedes_worker_forwarding() {
        let (adaptive_tx, _adaptive_rx) = std::sync::mpsc::channel();
        let (design_tx, design_rx) = std::sync::mpsc::channel();
        let response = execute_operator_request_with_capabilities(
            OperatorRequestV5 {
                request_id: 1,
                operation: OperatorOperationV5::PhysicalDesignMutationReceiptStatus {},
            },
            &ServerAdaptiveControlHandle::new(adaptive_tx),
            &ServerPhysicalDesignControlHandle::new(design_tx),
            OperatorListenerPolicy::new(true, true, false, None, None),
        );
        assert!(matches!(
            response,
            OperatorResponseV5::Error {
                error: OperatorRemoteErrorV5 {
                    code: OperatorErrorCodeV5::PhysicalDesignMutationReceiptReadNotAllowed,
                    receipt: None,
                    ..
                },
                ..
            }
        ));
        assert!(design_rx.try_recv().is_err());
    }

    #[test]
    fn receipt_status_and_scoped_page_forward_exactly_once() {
        let incarnation =
            ServerPhysicalDesignMutationReceiptJournalIncarnation::new([0x11; 16]).unwrap();
        let cursor = ServerPhysicalDesignMutationReceiptCursor::new(
            incarnation,
            crate::ServerPhysicalDesignMutationReceiptId(1),
        )
        .unwrap();
        let (design_tx, design_rx) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            let ServerPhysicalDesignControlRequest::MutationReceiptStatus { reply } =
                design_rx.recv().unwrap()
            else {
                panic!("expected receipt status request");
            };
            reply
                .send(Ok(ServerPhysicalDesignMutationReceiptStatus {
                    journal_incarnation: incarnation,
                    recovery_required: true,
                    latest_receipt_id: Some(crate::ServerPhysicalDesignMutationReceiptId(1)),
                    max_receipts_per_read: 128,
                }))
                .unwrap();
            let ServerPhysicalDesignControlRequest::MutationReceiptsScoped {
                after,
                limit,
                reply,
            } = design_rx.recv().unwrap()
            else {
                panic!("expected scoped receipt read");
            };
            assert_eq!(after, Some(cursor));
            assert_eq!(limit, 32);
            reply
                .send(Ok(ServerPhysicalDesignMutationReceiptScopedPage {
                    journal_incarnation: incarnation,
                    receipts: vec![ServerPhysicalDesignMutationReceipt {
                        id: crate::ServerPhysicalDesignMutationReceiptId(2),
                        source: ServerPhysicalDesignMutationSource::Programmatic,
                        evidence_epoch: netbadb_core::PhysicalDesignEvidenceEpoch(7),
                        target: ServerPhysicalDesignMutationTarget::Index {
                            table_id: TableId(1),
                            column_id: ColumnId(3),
                            index_name: IndexName::new("idx_users_email").unwrap(),
                        },
                        outcome: ServerPhysicalDesignMutationReceiptOutcome::Failed,
                    }],
                    next_after: Some(
                        ServerPhysicalDesignMutationReceiptCursor::new(
                            incarnation,
                            crate::ServerPhysicalDesignMutationReceiptId(2),
                        )
                        .unwrap(),
                    ),
                }))
                .unwrap();
        });
        let (adaptive_tx, _adaptive_rx) = std::sync::mpsc::channel();
        let adaptive = ServerAdaptiveControlHandle::new(adaptive_tx);
        let design = ServerPhysicalDesignControlHandle::new(design_tx);

        let status = execute_operator_request_with_capabilities(
            OperatorRequestV5 {
                request_id: 1,
                operation: OperatorOperationV5::PhysicalDesignMutationReceiptStatus {},
            },
            &adaptive,
            &design,
            OperatorListenerPolicy::new(false, false, true, None, None),
        );
        assert!(matches!(
            status,
            OperatorResponseV5::Ok {
                result: OperatorResultV5::PhysicalDesignMutationReceiptStatus {
                    status: OperatorPhysicalDesignMutationReceiptStatusV5 {
                        recovery_required: true,
                        latest_receipt_id: Some(1),
                        max_receipts_per_read: 128,
                        ..
                    }
                },
                ..
            }
        ));

        let page = execute_operator_request_with_capabilities(
            OperatorRequestV5 {
                request_id: 2,
                operation: OperatorOperationV5::PhysicalDesignMutationReceipts {
                    after: Some(OperatorPhysicalDesignMutationReceiptCursorV5 {
                        journal_incarnation: "11111111111111111111111111111111".into(),
                        receipt_id: 1,
                    }),
                    limit: 32,
                },
            },
            &adaptive,
            &design,
            OperatorListenerPolicy::new(false, false, true, None, None),
        );
        let OperatorResponseV5::Ok {
            result: OperatorResultV5::PhysicalDesignMutationReceipts { page },
            ..
        } = page
        else {
            panic!("expected receipt page");
        };
        assert_eq!(page.journal_incarnation, "11".repeat(16));
        assert_eq!(page.receipts.len(), 1);
        assert_eq!(page.receipts[0].receipt.receipt_id, 2);
        assert_eq!(page.next_after.unwrap().receipt_id, 2);
        assert_eq!(
            page.receipts[0].outcome,
            OperatorPhysicalDesignMutationReceiptOutcomeV5::Failed
        );
        worker.join().unwrap();
    }

    #[test]
    fn receipt_wire_mapping_covers_every_source_target_and_outcome() {
        let incarnation =
            ServerPhysicalDesignMutationReceiptJournalIncarnation::new([0x22; 16]).unwrap();
        let outcomes = [
            ServerPhysicalDesignMutationReceiptOutcome::Pending,
            ServerPhysicalDesignMutationReceiptOutcome::CreatedIndex {
                index_id: netbadb_types::IndexId(1),
            },
            ServerPhysicalDesignMutationReceiptOutcome::CreatedColumnar {
                projection_id: netbadb_types::ColumnarProjectionId(2),
            },
            ServerPhysicalDesignMutationReceiptOutcome::AlreadyAppliedIndex {
                index_id: netbadb_types::IndexId(3),
            },
            ServerPhysicalDesignMutationReceiptOutcome::AlreadyAppliedColumnar {
                projection_id: netbadb_types::ColumnarProjectionId(4),
            },
            ServerPhysicalDesignMutationReceiptOutcome::AlreadyCovered,
            ServerPhysicalDesignMutationReceiptOutcome::Rejected,
            ServerPhysicalDesignMutationReceiptOutcome::Failed,
            ServerPhysicalDesignMutationReceiptOutcome::RecoveredAppliedIndex {
                index_id: netbadb_types::IndexId(5),
            },
            ServerPhysicalDesignMutationReceiptOutcome::RecoveredAppliedColumnar {
                projection_id: netbadb_types::ColumnarProjectionId(6),
            },
            ServerPhysicalDesignMutationReceiptOutcome::RecoveredNotApplied,
            ServerPhysicalDesignMutationReceiptOutcome::RecoveredConflict,
        ];
        let expected_outcomes = [
            OperatorPhysicalDesignMutationReceiptOutcomeV5::Pending,
            OperatorPhysicalDesignMutationReceiptOutcomeV5::CreatedIndex { index_id: 1 },
            OperatorPhysicalDesignMutationReceiptOutcomeV5::CreatedColumnar { projection_id: 2 },
            OperatorPhysicalDesignMutationReceiptOutcomeV5::AlreadyAppliedIndex { index_id: 3 },
            OperatorPhysicalDesignMutationReceiptOutcomeV5::AlreadyAppliedColumnar {
                projection_id: 4,
            },
            OperatorPhysicalDesignMutationReceiptOutcomeV5::AlreadyCovered,
            OperatorPhysicalDesignMutationReceiptOutcomeV5::Rejected,
            OperatorPhysicalDesignMutationReceiptOutcomeV5::Failed,
            OperatorPhysicalDesignMutationReceiptOutcomeV5::RecoveredAppliedIndex { index_id: 5 },
            OperatorPhysicalDesignMutationReceiptOutcomeV5::RecoveredAppliedColumnar {
                projection_id: 6,
            },
            OperatorPhysicalDesignMutationReceiptOutcomeV5::RecoveredNotApplied,
            OperatorPhysicalDesignMutationReceiptOutcomeV5::RecoveredConflict,
        ];
        for (offset, (outcome, expected_outcome)) in
            outcomes.into_iter().zip(expected_outcomes).enumerate()
        {
            let mapped = operator_mutation_receipt(
                incarnation,
                ServerPhysicalDesignMutationReceipt {
                    id: crate::ServerPhysicalDesignMutationReceiptId(offset as u64 + 1),
                    source: if offset % 2 == 0 {
                        ServerPhysicalDesignMutationSource::Programmatic
                    } else {
                        ServerPhysicalDesignMutationSource::LocalOperator
                    },
                    evidence_epoch: netbadb_core::PhysicalDesignEvidenceEpoch(9),
                    target: ServerPhysicalDesignMutationTarget::Index {
                        table_id: TableId(1),
                        column_id: ColumnId(3),
                        index_name: IndexName::new("idx_users_email").unwrap(),
                    },
                    outcome,
                },
            );
            assert_eq!(mapped.receipt.journal_incarnation, "22".repeat(16));
            assert_eq!(mapped.receipt.receipt_id, offset as u64 + 1);
            assert_eq!(mapped.evidence_epoch, 9);
            assert_eq!(mapped.outcome, expected_outcome);
            assert_eq!(
                mapped.source,
                if offset % 2 == 0 {
                    OperatorPhysicalDesignMutationReceiptSourceV5::Programmatic
                } else {
                    OperatorPhysicalDesignMutationReceiptSourceV5::LocalOperator
                }
            );
            assert!(matches!(
                mapped.target,
                OperatorPhysicalDesignMutationReceiptTargetV5::Index {
                    table_id: 1,
                    column_id: 3,
                    ..
                }
            ));
        }

        let columnar = operator_mutation_receipt(
            incarnation,
            ServerPhysicalDesignMutationReceipt {
                id: crate::ServerPhysicalDesignMutationReceiptId(20),
                source: ServerPhysicalDesignMutationSource::LocalOperator,
                evidence_epoch: netbadb_core::PhysicalDesignEvidenceEpoch(10),
                target: ServerPhysicalDesignMutationTarget::Columnar {
                    table_id: TableId(1),
                    columns: vec![ColumnId(2), ColumnId(3), ColumnId(4)],
                    mode: PhysicalColumnarDesignMode::Incremental,
                    placement: crate::ServerPhysicalColumnarPlacementKey::new("users-analytics-v1")
                        .unwrap(),
                },
                outcome: ServerPhysicalDesignMutationReceiptOutcome::CreatedColumnar {
                    projection_id: netbadb_types::ColumnarProjectionId(7),
                },
            },
        );
        assert!(matches!(
            columnar.target,
            OperatorPhysicalDesignMutationReceiptTargetV5::Columnar {
                table_id: 1,
                mode: OperatorPhysicalColumnarDesignModeV5::Incremental,
                ..
            }
        ));
        let encoded = serde_json::to_string(&columnar).unwrap();
        for private in [
            "/private/",
            "database_incarnation",
            "runtime_token",
            "sql",
            "principal",
            "session",
            "127.0.0.1",
        ] {
            assert!(!encoded.contains(private));
        }
    }

    #[test]
    fn receipt_errors_are_stable_bounded_and_path_private() {
        for (error, expected) in [
            (
                ServerPhysicalDesignMutationReceiptControlError::NotEnabled,
                OperatorErrorCodeV5::PhysicalDesignMutationReceiptsNotEnabled,
            ),
            (
                ServerPhysicalDesignMutationReceiptControlError::InvalidLimit {
                    supplied: 0,
                    maximum: 128,
                },
                OperatorErrorCodeV5::InvalidPhysicalDesignMutationReceiptLimit,
            ),
            (
                ServerPhysicalDesignMutationReceiptControlError::JournalChanged {
                    expected: ServerPhysicalDesignMutationReceiptJournalIncarnation::new([1; 16])
                        .unwrap(),
                    actual: ServerPhysicalDesignMutationReceiptJournalIncarnation::new([2; 16])
                        .unwrap(),
                },
                OperatorErrorCodeV5::PhysicalDesignMutationReceiptJournalChanged,
            ),
        ] {
            let mapped = receipt_read_remote_error(error);
            assert_eq!(mapped.code, expected);
            assert_eq!(mapped.receipt, None);
        }

        let mapped =
            receipt_read_remote_error(ServerPhysicalDesignMutationReceiptControlError::Journal(
                ServerPhysicalDesignMutationReceiptJournalError::Io {
                    operation: "read",
                    path: PathBuf::from("/private/receipts.nbmr"),
                    source: io::Error::other("secret raw I/O"),
                },
            ));
        assert_eq!(
            mapped.code,
            OperatorErrorCodeV5::PhysicalDesignMutationReceiptReadFailed
        );
        let encoded = serde_json::to_string(&mapped).unwrap();
        assert!(!encoded.contains("/private/receipts.nbmr"));
        assert!(!encoded.contains("secret raw I/O"));

        assert_eq!(
            mutation_receipt_begin_error_code(
                &ServerPhysicalDesignMutationReceiptControlError::Journal(
                    ServerPhysicalDesignMutationReceiptJournalError::CapacityExceeded,
                )
            ),
            OperatorErrorCodeV5::PhysicalDesignMutationReceiptCapacityExceeded
        );
        assert_eq!(
            mutation_receipt_begin_error_code(
                &ServerPhysicalDesignMutationReceiptControlError::Journal(
                    ServerPhysicalDesignMutationReceiptJournalError::Corrupt("private")
                )
            ),
            OperatorErrorCodeV5::PhysicalDesignMutationReceiptUnavailable
        );
    }

    #[test]
    fn one_maximum_public_receipt_fits_and_large_pages_fail_without_truncation() {
        let incarnation =
            ServerPhysicalDesignMutationReceiptJournalIncarnation::new([0x33; 16]).unwrap();
        let receipt = operator_mutation_receipt(
            incarnation,
            ServerPhysicalDesignMutationReceipt {
                id: crate::ServerPhysicalDesignMutationReceiptId(1),
                source: ServerPhysicalDesignMutationSource::LocalOperator,
                evidence_epoch: netbadb_core::PhysicalDesignEvidenceEpoch(u64::MAX),
                target: ServerPhysicalDesignMutationTarget::Columnar {
                    table_id: TableId(u64::MAX),
                    columns: vec![ColumnId(u32::MAX); 4_096],
                    mode: PhysicalColumnarDesignMode::Incremental,
                    placement: crate::ServerPhysicalColumnarPlacementKey::new("p".repeat(128))
                        .unwrap(),
                },
                outcome: ServerPhysicalDesignMutationReceiptOutcome::Failed,
            },
        );
        let single = OperatorResponseV5::Ok {
            request_id: 1,
            result: OperatorResultV5::PhysicalDesignMutationReceipts {
                page: OperatorPhysicalDesignMutationReceiptPageV5 {
                    journal_incarnation: "33".repeat(16),
                    receipts: vec![receipt.clone()],
                    next_after: None,
                },
            },
        };
        let single_payload = serde_json::to_vec(&single).unwrap();
        assert!(single_payload.len() <= MAX_OPERATOR_PAYLOAD_BYTES as usize);
        assert_eq!(
            single_payload
                .windows(b"4294967295".len())
                .filter(|window| *window == b"4294967295")
                .count(),
            4_096
        );

        let oversized = OperatorResponseV5::Ok {
            request_id: 2,
            result: OperatorResultV5::PhysicalDesignMutationReceipts {
                page: OperatorPhysicalDesignMutationReceiptPageV5 {
                    journal_incarnation: "33".repeat(16),
                    receipts: vec![receipt.clone(), receipt],
                    next_after: None,
                },
            },
        };
        let mut bytes = Vec::new();
        write_operator_response(&mut bytes, &oversized).unwrap();
        assert!(matches!(
            read_frame::<OperatorResponseV5>(&mut bytes.as_slice()).unwrap(),
            OperatorResponseV5::Error {
                request_id: 2,
                error: OperatorRemoteErrorV5 {
                    code: OperatorErrorCodeV5::ResponseTooLarge,
                    receipt: None,
                    ..
                }
            }
        ));
    }

    #[test]
    fn physical_design_status_and_report_use_explicit_stable_dtos() {
        let status = operator_physical_design_status(
            ServerPhysicalDesignStatus {
                diagnostics: crate::ServerPhysicalDesignDiagnostics {
                    eligible_query_count: 1,
                    record_success_count: 2,
                    record_error_count: 3,
                    schema_rotation_count: 4,
                    capacity_rejection_count: 5,
                    incomplete_report_count: 6,
                    counter_overflowed: true,
                    last_record_outcome: Some(
                        PhysicalDesignEvidenceRecordOutcome::SchemaRotatedWithCapacityRejection,
                    ),
                    last_record_error: Some(
                        PhysicalDesignEvidenceRecordError::OutOfOrderVisibility {
                            previous: netbadb_types::DatabaseCommitSeq(9),
                            received: netbadb_types::DatabaseCommitSeq(8),
                        },
                    ),
                },
                evidence: netbadb_core::PhysicalDesignEvidenceWindowInspection {
                    limits: netbadb_core::PhysicalDesignEvidenceLimits {
                        max_index_candidates: 10,
                        max_columnar_candidates: 11,
                        max_query_shapes_per_candidate: 12,
                        max_columnar_columns_per_candidate: 13,
                    },
                    epoch: netbadb_core::PhysicalDesignEvidenceEpoch(14),
                    schema_generation: Some(netbadb_types::SchemaGeneration(15)),
                    first_global_commit_seq: Some(netbadb_types::DatabaseCommitSeq(16)),
                    last_global_commit_seq: Some(netbadb_types::DatabaseCommitSeq(17)),
                    ordering_high_water: Some(netbadb_types::DatabaseCommitSeq(18)),
                    recorded_reports: 19,
                    index_candidate_count: 20,
                    columnar_candidate_count: 21,
                    capacity_rejections: 22,
                    discarded_incomplete_reports: 23,
                    overflowed: true,
                    incomplete: true,
                    truncated: true,
                },
            },
            false,
            None,
        );
        assert_eq!(status.diagnostics.eligible_query_count, 1);
        assert_eq!(
            status.diagnostics.last_record_outcome,
            Some(OperatorPhysicalDesignRecordOutcomeV5::SchemaRotatedWithCapacityRejection)
        );
        assert_eq!(
            status.diagnostics.last_record_error,
            Some(OperatorPhysicalDesignRecordErrorV5::OutOfOrderVisibility)
        );
        assert_eq!(
            status.evidence.limits.max_columnar_columns_per_candidate,
            13
        );
        assert_eq!(status.evidence.epoch, 14);
        assert!(!status.physical_index_apply.enabled);
        assert!(status.physical_index_apply.runtime_token.is_none());
        assert_eq!(status.evidence.ordering_high_water, Some(18));
        assert_eq!(status.evidence.discarded_incomplete_reports, 23);

        for (reason, expected) in [
            (
                PhysicalDesignNoActionReason::BelowMinimumReports,
                OperatorPhysicalDesignNoActionReasonV5::BelowMinimumReports,
            ),
            (
                PhysicalDesignNoActionReason::BelowMinimumShapeDiversity,
                OperatorPhysicalDesignNoActionReasonV5::BelowMinimumShapeDiversity,
            ),
            (
                PhysicalDesignNoActionReason::BelowMinimumActualWork,
                OperatorPhysicalDesignNoActionReasonV5::BelowMinimumActualWork,
            ),
            (
                PhysicalDesignNoActionReason::ExistingDesignCovers,
                OperatorPhysicalDesignNoActionReasonV5::ExistingDesignCovers,
            ),
            (
                PhysicalDesignNoActionReason::UnsupportedCurrentLayout,
                OperatorPhysicalDesignNoActionReasonV5::UnsupportedCurrentLayout,
            ),
            (
                PhysicalDesignNoActionReason::IncompleteEvidence,
                OperatorPhysicalDesignNoActionReasonV5::IncompleteEvidence,
            ),
            (
                PhysicalDesignNoActionReason::CurrentProjectionUnavailable,
                OperatorPhysicalDesignNoActionReasonV5::CurrentProjectionUnavailable,
            ),
            (
                PhysicalDesignNoActionReason::RecommendationLimitReached,
                OperatorPhysicalDesignNoActionReasonV5::RecommendationLimitReached,
            ),
        ] {
            assert_eq!(operator_no_action_reason(reason), expected);
        }
    }

    #[test]
    fn runtime_token_encoding_is_exact_canonical_and_permission_precedes_forwarding() {
        let token = OperatorPhysicalDesignRuntimeToken([
            0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd,
            0xee, 0xff,
        ]);
        let encoded = token.encode();
        assert_eq!(encoded, "00112233445566778899aabbccddeeff");
        assert_eq!(
            OperatorPhysicalDesignRuntimeToken::parse(&encoded),
            Some(token)
        );
        for invalid in [
            "00112233445566778899AABBCCDDEEFF",
            "0x00112233445566778899aabbccddeeff",
            "00112233-4455-6677-8899-aabbccddeeff",
            "0011223344556677",
            "ABEiM0RVZneImaq7zN3u/w==",
        ] {
            assert!(OperatorPhysicalDesignRuntimeToken::parse(invalid).is_none());
        }

        let (adaptive_tx, _adaptive_rx) = std::sync::mpsc::channel();
        let (design_tx, design_rx) = std::sync::mpsc::channel();
        let response = execute_operator_request(
            OperatorRequestV5 {
                request_id: 91,
                operation: OperatorOperationV5::ApplyPhysicalIndex {
                    expected_runtime_token: "invalid".into(),
                    expected_evidence_epoch: 0,
                    table_id: 1,
                    column_id: 2,
                    index_name: String::new(),
                },
            },
            &ServerAdaptiveControlHandle::new(adaptive_tx),
            &ServerPhysicalDesignControlHandle::new(design_tx),
            false,
            None,
        );
        assert!(matches!(
            response,
            OperatorResponseV5::Error {
                error: OperatorRemoteErrorV5 {
                    code: OperatorErrorCodeV5::PhysicalIndexApplyNotEnabled,
                    ..
                },
                ..
            }
        ));
        assert!(matches!(
            design_rx.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty | std::sync::mpsc::TryRecvError::Disconnected)
        ));

        let (adaptive_tx, _adaptive_rx) = std::sync::mpsc::channel();
        let (design_tx, design_rx) = std::sync::mpsc::channel();
        let response = execute_operator_request(
            OperatorRequestV5 {
                request_id: 92,
                operation: OperatorOperationV5::ApplyPhysicalIndex {
                    expected_runtime_token: encoded,
                    expected_evidence_epoch: 0,
                    table_id: 1,
                    column_id: 2,
                    index_name: String::new(),
                },
            },
            &ServerAdaptiveControlHandle::new(adaptive_tx),
            &ServerPhysicalDesignControlHandle::new(design_tx),
            true,
            Some(token),
        );
        assert!(matches!(
            response,
            OperatorResponseV5::Error {
                error: OperatorRemoteErrorV5 {
                    code: OperatorErrorCodeV5::InvalidIndexName,
                    ..
                },
                ..
            }
        ));
        assert!(matches!(
            design_rx.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty | std::sync::mpsc::TryRecvError::Disconnected)
        ));

        let (adaptive_tx, _adaptive_rx) = std::sync::mpsc::channel();
        let (design_tx, design_rx) = std::sync::mpsc::channel();
        let response = execute_operator_request(
            OperatorRequestV5 {
                request_id: 93,
                operation: OperatorOperationV5::ApplyPhysicalIndex {
                    expected_runtime_token: "00112233445566778899AABBCCDDEEFF".into(),
                    expected_evidence_epoch: 0,
                    table_id: 1,
                    column_id: 2,
                    index_name: "users_name_idx".into(),
                },
            },
            &ServerAdaptiveControlHandle::new(adaptive_tx),
            &ServerPhysicalDesignControlHandle::new(design_tx),
            true,
            Some(token),
        );
        assert!(matches!(
            response,
            OperatorResponseV5::Error {
                error: OperatorRemoteErrorV5 {
                    code: OperatorErrorCodeV5::MalformedRequest,
                    ..
                },
                ..
            }
        ));
        assert!(matches!(
            design_rx.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty | std::sync::mpsc::TryRecvError::Disconnected)
        ));
    }

    #[cfg(unix)]
    #[test]
    fn raw_v5_empty_column_list_is_malformed_before_worker_forwarding() {
        use std::os::unix::net::UnixStream;

        let token = OperatorPhysicalDesignRuntimeToken([0x11; 16]);
        let payload = br#"{"request_id":1,"operation":{"type":"apply_physical_columnar","expected_runtime_token":"11111111111111111111111111111111","expected_evidence_epoch":1,"table_id":1,"columns":[],"mode":"snapshot","placement_key":"test"}}"#;
        let mut frame = b"NBOP\0\x05\0\0".to_vec();
        frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        frame.extend_from_slice(payload);

        let (mut client, mut server) = UnixStream::pair().unwrap();
        let (adaptive_tx, _adaptive_rx) = std::sync::mpsc::channel();
        let (design_tx, design_rx) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            serve_operator_connection_with_capabilities(
                &mut server,
                &ServerAdaptiveControlHandle::new(adaptive_tx),
                &ServerPhysicalDesignControlHandle::new(design_tx),
                OperatorListenerPolicy::new(
                    false,
                    true,
                    false,
                    Some(ServerPhysicalColumnarApplyCapabilities {
                        allow_snapshot: true,
                        allow_incremental: false,
                    }),
                    Some(token),
                ),
            )
            .unwrap();
        });

        client.write_all(&frame).unwrap();
        assert!(matches!(
            read_frame::<OperatorResponseV5>(&mut client).unwrap(),
            OperatorResponseV5::Error {
                request_id: 1,
                error: OperatorRemoteErrorV5 {
                    code: OperatorErrorCodeV5::MalformedRequest,
                    ref message,
                    receipt: None,
                },
            } if message == "columns must contain at least one column"
        ));
        worker.join().unwrap();
        assert!(matches!(
            design_rx.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty | std::sync::mpsc::TryRecvError::Disconnected)
        ));
    }

    #[test]
    fn columnar_server_stop_mapping_preserves_definite_and_uncertain_outcomes() {
        let stopped =
            physical_columnar_remote_error(ServerPhysicalColumnarDesignControlError::ServerStopped);
        assert_eq!(stopped.code, OperatorErrorCodeV5::ServerStopped);
        assert!(!stopped.message.contains("without creating"));

        let uncertain = physical_columnar_remote_error(
            ServerPhysicalColumnarDesignControlError::MutationOutcomeUncertain,
        );
        assert_eq!(
            uncertain.code,
            OperatorErrorCodeV5::PhysicalDesignMutationOutcomeUncertain
        );
        assert!(uncertain.message.contains("same exact approval"));
        assert!(!uncertain.message.contains("without creating"));

        let reference = operator_receipt_reference(
            ServerPhysicalDesignMutationReceiptReference::new(
                ServerPhysicalDesignMutationReceiptJournalIncarnation::new([0x44; 16]).unwrap(),
                crate::ServerPhysicalDesignMutationReceiptId(9),
            )
            .unwrap(),
        );
        let receipt_uncertain = physical_columnar_remote_error_with_receipt(
            ServerPhysicalColumnarDesignControlError::MutationRecoveryRequired(
                crate::ServerPhysicalDesignMutationReceiptControlError::RecoveryRequired,
            ),
            Some(reference.clone()),
        );
        assert_eq!(
            receipt_uncertain.code,
            OperatorErrorCodeV5::PhysicalDesignMutationOutcomeUncertain
        );
        assert!(receipt_uncertain.message.contains("restart/reopen"));
        assert_eq!(receipt_uncertain.receipt, Some(reference.clone()));
        let ambiguous_columnar = physical_columnar_remote_error_with_receipt(
            ServerPhysicalColumnarDesignControlError::PostBeginMutationOutcomeUncertain(Box::new(
                ServerPhysicalColumnarDesignControlError::Apply(Box::new(
                    PhysicalColumnarDesignApplyError::Database(
                        DatabaseError::ColumnarProjectionNotFound(
                            netbadb_types::ColumnarProjectionId(77),
                        ),
                    ),
                )),
            )),
            Some(reference.clone()),
        );
        assert_eq!(
            ambiguous_columnar.code,
            OperatorErrorCodeV5::PhysicalDesignMutationOutcomeUncertain
        );
        assert_eq!(ambiguous_columnar.receipt, Some(reference.clone()));
        let index_receipt_uncertain = physical_design_remote_error_with_receipt(
            ServerPhysicalDesignControlError::MutationRecoveryRequired(
                crate::ServerPhysicalDesignMutationReceiptControlError::RecoveryRequired,
            ),
            Some(reference.clone()),
        );
        assert_eq!(
            index_receipt_uncertain.code,
            OperatorErrorCodeV5::PhysicalDesignMutationOutcomeUncertain
        );
        assert!(index_receipt_uncertain.message.contains("restart/reopen"));
        let ambiguous_index = physical_design_remote_error_with_receipt(
            ServerPhysicalDesignControlError::PostBeginMutationOutcomeUncertain(Box::new(
                ServerPhysicalDesignControlError::Apply(Box::new(
                    PhysicalIndexDesignApplyError::Database(DatabaseError::UndefinedIndex),
                )),
            )),
            Some(reference.clone()),
        );
        assert_eq!(
            ambiguous_index.code,
            OperatorErrorCodeV5::PhysicalDesignMutationOutcomeUncertain
        );
        assert_eq!(ambiguous_index.receipt, Some(reference.clone()));
        let gated_request = physical_design_remote_error_with_receipt(
            ServerPhysicalDesignControlError::MutationReceipt(
                ServerPhysicalDesignMutationReceiptControlError::RecoveryRequired,
            ),
            None,
        );
        assert_eq!(
            gated_request.code,
            OperatorErrorCodeV5::PhysicalDesignMutationReceiptUnavailable
        );
        assert_eq!(gated_request.receipt, None);
        assert!(gated_request.message.contains("no mutation occurred"));
        assert!(gated_request.message.contains("restart/reopen"));

        let reply_loss = classify_mutating_client_error(OperatorClientError::Protocol(
            OperatorProtocolError::TruncatedPayload,
        ));
        assert!(matches!(
            &reply_loss,
            OperatorClientError::MutationOutcomeUncertain {
                recovery_required: false,
                receipt: None,
                ..
            }
        ));
        assert!(reply_loss.to_string().contains("same exact approval"));
        let recovery =
            classify_mutating_client_error(OperatorClientError::Remote(receipt_uncertain));
        assert!(matches!(
            &recovery,
            OperatorClientError::MutationOutcomeUncertain {
                recovery_required: true,
                receipt: Some(receipt),
                ..
            } if receipt == &reference
        ));
        assert!(recovery.to_string().contains("restart/reopen"));
        let no_receipt_remote = mutation_outcome_uncertain_remote_error(None);
        assert!(matches!(
            classify_mutating_client_error(OperatorClientError::Remote(no_receipt_remote)),
            OperatorClientError::MutationOutcomeUncertain {
                recovery_required: false,
                receipt: None,
                ..
            }
        ));
        assert!(matches!(
            classify_mutating_client_error(OperatorClientError::Remote(stopped)),
            OperatorClientError::Remote(OperatorRemoteErrorV5 {
                code: OperatorErrorCodeV5::ServerStopped,
                ..
            })
        ));
    }

    #[test]
    fn v5_worker_reply_loss_is_uncertain_without_a_receipt() {
        let token = OperatorPhysicalDesignRuntimeToken::from_bytes([0x51; 16]);
        let encoded_token = token.encode();
        let (adaptive_tx, _adaptive_rx) = std::sync::mpsc::channel();

        let (index_tx, index_rx) = std::sync::mpsc::channel();
        let index_worker = std::thread::spawn(move || drop(index_rx.recv().unwrap()));
        let index = execute_operator_request_with_capabilities(
            OperatorRequestV5 {
                request_id: 1,
                operation: OperatorOperationV5::ApplyPhysicalIndex {
                    expected_runtime_token: encoded_token.clone(),
                    expected_evidence_epoch: 1,
                    table_id: 2,
                    column_id: 3,
                    index_name: "events_idx".into(),
                },
            },
            &ServerAdaptiveControlHandle::new(adaptive_tx.clone()),
            &ServerPhysicalDesignControlHandle::new(index_tx),
            OperatorListenerPolicy::new(true, false, false, None, Some(token)),
        );
        index_worker.join().unwrap();
        assert!(matches!(
            index,
            OperatorResponseV5::Error {
                error: OperatorRemoteErrorV5 {
                    code: OperatorErrorCodeV5::PhysicalDesignMutationOutcomeUncertain,
                    receipt: None,
                    ..
                },
                ..
            }
        ));

        let (columnar_tx, columnar_rx) = std::sync::mpsc::channel();
        let columnar_worker = std::thread::spawn(move || drop(columnar_rx.recv().unwrap()));
        let columnar = execute_operator_request_with_capabilities(
            OperatorRequestV5 {
                request_id: 2,
                operation: OperatorOperationV5::ApplyPhysicalColumnar {
                    expected_runtime_token: encoded_token,
                    expected_evidence_epoch: 1,
                    table_id: 2,
                    columns: vec![3],
                    mode: OperatorPhysicalColumnarDesignModeV5::Snapshot,
                    placement_key: "events".into(),
                },
            },
            &ServerAdaptiveControlHandle::new(adaptive_tx),
            &ServerPhysicalDesignControlHandle::new(columnar_tx),
            OperatorListenerPolicy::new(
                false,
                true,
                false,
                Some(ServerPhysicalColumnarApplyCapabilities {
                    allow_snapshot: true,
                    allow_incremental: false,
                }),
                Some(token),
            ),
        );
        columnar_worker.join().unwrap();
        assert!(matches!(
            columnar,
            OperatorResponseV5::Error {
                error: OperatorRemoteErrorV5 {
                    code: OperatorErrorCodeV5::PhysicalDesignMutationOutcomeUncertain,
                    receipt: None,
                    ..
                },
                ..
            }
        ));
    }

    #[test]
    fn oversized_success_becomes_bounded_response_too_large_error() {
        let candidate = OperatorPhysicalIndexCandidateV5 {
            table_id: 1,
            column_id: 2,
            point_report_count: 3,
            range_report_count: 4,
            evidence: OperatorPhysicalDesignEvidenceSummaryV5 {
                report_count: 5,
                distinct_query_shapes: 6,
                total_actual_scan_work_units: 7,
                total_rows_examined: 8,
                overflowed: false,
                incomplete: false,
                truncated: false,
            },
            decision: OperatorPhysicalDesignDecisionV5::Recommend {},
        };
        let response = OperatorResponseV5::Ok {
            request_id: 77,
            result: OperatorResultV5::PhysicalDesignRecommendations {
                runtime_token: None,
                report: OperatorPhysicalDesignAdvisorReportV5 {
                    evidence_epoch: 1,
                    schema_generation: 2,
                    first_global_commit_seq: 3,
                    last_global_commit_seq: 4,
                    recorded_reports: 5,
                    discarded_incomplete_reports: 0,
                    overflowed: false,
                    incomplete: false,
                    index_candidates: vec![candidate; 1_000],
                    columnar_candidates: Vec::new(),
                },
            },
        };
        let mut bytes = Vec::new();
        write_operator_response(&mut bytes, &response).unwrap();
        assert!(bytes.len() <= OPERATOR_HEADER_BYTES + MAX_OPERATOR_PAYLOAD_BYTES as usize);
        assert!(matches!(
            read_frame::<OperatorResponseV5>(&mut bytes.as_slice()).unwrap(),
            OperatorResponseV5::Error {
                request_id: 77,
                error: OperatorRemoteErrorV5 {
                    code: OperatorErrorCodeV5::ResponseTooLarge,
                    ..
                }
            }
        ));
    }

    #[test]
    fn reset_and_conditional_rotation_forward_typed_worker_results() {
        let (requests, controls) = std::sync::mpsc::channel();
        let control = ServerAdaptiveControlHandle::new(requests);
        let (design_requests, _design_controls) = std::sync::mpsc::channel();
        let design_control = ServerPhysicalDesignControlHandle::new(design_requests);
        let worker = std::thread::spawn(move || {
            match controls.recv().unwrap() {
                ServerAdaptiveControlRequest::ResetFaultedScheduler { reply } => {
                    reply.send(Ok(())).unwrap();
                }
                _ => panic!("unexpected first operator control request"),
            }
            match controls.recv().unwrap() {
                ServerAdaptiveControlRequest::RotateEvidenceIfWindow { expected, reply } => {
                    assert_eq!(expected, AdaptiveEvidenceWindowEpoch(7));
                    reply
                        .send(Err(ServerAdaptiveControlError::EvidenceWindowChanged {
                            expected,
                            actual: AdaptiveEvidenceWindowEpoch(8),
                        }))
                        .unwrap();
                }
                _ => panic!("unexpected second operator control request"),
            }
        });

        let reset = execute_operator_request(
            OperatorRequestV5 {
                request_id: 11,
                operation: OperatorOperationV5::ResetFaultedScheduler {},
            },
            &control,
            &design_control,
            false,
            None,
        );
        assert!(matches!(
            reset,
            OperatorResponseV5::Ok {
                request_id: 11,
                result: OperatorResultV5::SchedulerReset {}
            }
        ));

        let stale = execute_operator_request(
            OperatorRequestV5 {
                request_id: 12,
                operation: OperatorOperationV5::RotateEvidence {
                    expected_window_epoch: 7,
                },
            },
            &control,
            &design_control,
            false,
            None,
        );
        assert!(matches!(
            stale,
            OperatorResponseV5::Error {
                request_id: 12,
                error: OperatorRemoteErrorV5 {
                    code: OperatorErrorCodeV5::EvidenceWindowChanged,
                    ..
                }
            }
        ));
        worker.join().unwrap();
    }

    #[test]
    fn codec_rejects_header_and_payload_violations() {
        let mut wrong_magic = [0_u8; OPERATOR_HEADER_BYTES];
        wrong_magic[..4].copy_from_slice(b"NOPE");
        wrong_magic[4..6].copy_from_slice(&3_u16.to_be_bytes());
        assert!(matches!(
            read_frame::<OperatorRequestV5>(&mut wrong_magic.as_slice()),
            Err(OperatorProtocolError::WrongMagic)
        ));

        let mut wrong_version = [0_u8; OPERATOR_HEADER_BYTES];
        wrong_version[..4].copy_from_slice(b"NBOP");
        wrong_version[4..6].copy_from_slice(&4_u16.to_be_bytes());
        assert!(matches!(
            read_frame::<OperatorRequestV5>(&mut wrong_version.as_slice()),
            Err(OperatorProtocolError::UnsupportedVersion(4))
        ));

        let mut reserved = [0_u8; OPERATOR_HEADER_BYTES];
        reserved[..4].copy_from_slice(b"NBOP");
        reserved[4..6].copy_from_slice(&5_u16.to_be_bytes());
        reserved[6..8].copy_from_slice(&1_u16.to_be_bytes());
        assert!(matches!(
            read_frame::<OperatorRequestV5>(&mut reserved.as_slice()),
            Err(OperatorProtocolError::NonzeroReserved(1))
        ));

        let mut oversized = [0_u8; OPERATOR_HEADER_BYTES];
        oversized[..4].copy_from_slice(b"NBOP");
        oversized[4..6].copy_from_slice(&5_u16.to_be_bytes());
        oversized[8..12].copy_from_slice(&(MAX_OPERATOR_PAYLOAD_BYTES + 1).to_be_bytes());
        assert!(matches!(
            read_frame::<OperatorRequestV5>(&mut oversized.as_slice()),
            Err(OperatorProtocolError::RequestTooLarge(_))
        ));
        assert!(matches!(
            read_frame::<OperatorRequestV5>(&mut b"NB".as_slice()),
            Err(OperatorProtocolError::TruncatedHeader)
        ));

        let truncated_payload = b"NBOP\0\x05\0\0\0\0\0\x02{".to_vec();
        assert!(matches!(
            read_frame::<OperatorRequestV5>(&mut truncated_payload.as_slice()),
            Err(OperatorProtocolError::TruncatedPayload)
        ));
    }

    #[cfg(unix)]
    #[test]
    fn v3_client_receives_explicit_unsupported_protocol_version() {
        use std::os::unix::net::UnixStream;

        let (mut client, mut server) = UnixStream::pair().unwrap();
        let (adaptive_tx, _adaptive_rx) = std::sync::mpsc::channel();
        let (design_tx, _design_rx) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            assert!(matches!(
                serve_operator_connection(
                    &mut server,
                    &ServerAdaptiveControlHandle::new(adaptive_tx),
                    &ServerPhysicalDesignControlHandle::new(design_tx),
                    false,
                    None,
                ),
                Err(OperatorProtocolError::UnsupportedVersion(3))
            ));
        });
        let mut v3_header = [0_u8; OPERATOR_HEADER_BYTES];
        v3_header[..4].copy_from_slice(b"NBOP");
        v3_header[4..6].copy_from_slice(&3_u16.to_be_bytes());
        client.write_all(&v3_header).unwrap();
        assert!(matches!(
            read_frame::<OperatorResponseV5>(&mut client).unwrap(),
            OperatorResponseV5::Error {
                request_id: 0,
                error: OperatorRemoteErrorV5 {
                    code: OperatorErrorCodeV5::UnsupportedProtocolVersion,
                    ..
                }
            }
        ));
        worker.join().unwrap();
    }

    #[test]
    fn strict_request_json_rejects_unknown_fields_and_operations() {
        fn framed(payload: &[u8]) -> Vec<u8> {
            let mut bytes = Vec::from(b"NBOP\0\x05\0\0".as_slice());
            bytes.extend_from_slice(&(payload.len() as u32).to_be_bytes());
            bytes.extend_from_slice(payload);
            bytes
        }
        for payload in [
            br#"{"request_id":1,"operation":{"type":"status","extra":true}}"#.as_slice(),
            br#"{"request_id":1,"operation":{"type":"unknown"}}"#.as_slice(),
            br#"{"request_id":1,"operation":{"type":"apply_physical_columnar"}}"#.as_slice(),
            br#"{"request_id":1,"operation":{"type":"apply_physical_index","index_name":"idx"}}"#
                .as_slice(),
            &[0xff][..],
        ] {
            assert!(matches!(
                read_frame::<OperatorRequestV5>(&mut framed(payload).as_slice()),
                Err(OperatorProtocolError::InvalidJson(_))
            ));
        }
    }

    #[test]
    fn armed_failure_notification_reports_abnormal_listener_exit() {
        let (sender, receiver) = std::sync::mpsc::channel();
        drop(OperatorFailureNotification::new(sender));
        assert_eq!(receiver.recv().unwrap(), ());

        let (sender, receiver) = std::sync::mpsc::channel();
        let mut notification = OperatorFailureNotification::new(sender);
        notification.disarm();
        drop(notification);
        assert!(receiver.recv().is_err());
    }

    #[cfg(unix)]
    #[test]
    fn listener_forwards_only_one_active_control_request() {
        use std::os::unix::net::UnixStream;
        use std::sync::mpsc::TryRecvError;

        fn reset_frame(request_id: u64) -> Vec<u8> {
            let mut bytes = Vec::new();
            write_frame(
                &mut bytes,
                &OperatorRequestV5 {
                    request_id,
                    operation: OperatorOperationV5::ResetFaultedScheduler {},
                },
            )
            .unwrap();
            bytes
        }

        let (directory, mut config) = socket_fixture("serial");
        config.io_timeout = Duration::from_secs(1);
        let path = config.unix_socket().to_path_buf();
        let (control_tx, control_rx) = std::sync::mpsc::channel();
        let (design_tx, _design_rx) = std::sync::mpsc::channel();
        let (failure_tx, _failure_rx) = std::sync::mpsc::channel();
        let plane = ServerOperatorPlane::start(
            config,
            ServerAdaptiveControlHandle::new(control_tx),
            ServerPhysicalDesignControlHandle::new(design_tx),
            failure_tx,
        )
        .unwrap();

        let mut first = UnixStream::connect(&path).unwrap();
        let mut second = UnixStream::connect(&path).unwrap();
        first.write_all(&reset_frame(1)).unwrap();
        second.write_all(&reset_frame(2)).unwrap();

        let first_reply = match control_rx.recv_timeout(Duration::from_secs(1)).unwrap() {
            ServerAdaptiveControlRequest::ResetFaultedScheduler { reply } => reply,
            _ => panic!("unexpected first operator control request"),
        };
        assert!(matches!(control_rx.try_recv(), Err(TryRecvError::Empty)));
        first_reply.send(Ok(())).unwrap();
        assert!(matches!(
            read_frame::<OperatorResponseV5>(&mut first).unwrap(),
            OperatorResponseV5::Ok { request_id: 1, .. }
        ));

        let second_reply = match control_rx.recv_timeout(Duration::from_secs(1)).unwrap() {
            ServerAdaptiveControlRequest::ResetFaultedScheduler { reply } => reply,
            _ => panic!("unexpected second operator control request"),
        };
        second_reply
            .send(Err(ServerAdaptiveControlError::SchedulerNotFaulted))
            .unwrap();
        assert!(matches!(
            read_frame::<OperatorResponseV5>(&mut second).unwrap(),
            OperatorResponseV5::Error {
                request_id: 2,
                error: OperatorRemoteErrorV5 {
                    code: OperatorErrorCodeV5::SchedulerNotFaulted,
                    ..
                }
            }
        ));

        plane.shutdown().unwrap();
        assert!(!path.exists());
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn shutdown_with_an_incomplete_request_is_bounded_by_io_timeout() {
        use std::os::unix::net::UnixStream;

        let (directory, mut config) = socket_fixture("partial");
        config.io_timeout = Duration::from_millis(20);
        let path = config.unix_socket().to_path_buf();
        let plane = idle_plane(config).unwrap();
        let _incomplete = UnixStream::connect(&path).unwrap();
        plane.shutdown().unwrap();
        assert!(!path.exists());
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn socket_mode_cleanup_and_replacement_identity_are_exact() {
        use std::os::unix::fs::PermissionsExt;

        let (directory, config) = socket_fixture("lifecycle");
        let path = config.unix_socket().to_path_buf();
        let plane = idle_plane(config.clone()).unwrap();
        assert!(!plane.is_finished());
        let mode = std::fs::symlink_metadata(&path)
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
        plane.shutdown().unwrap();
        assert!(!path.exists());

        let plane = idle_plane(config).unwrap();
        std::fs::remove_file(&path).unwrap();
        std::fs::write(&path, b"replacement").unwrap();
        assert!(matches!(
            plane.shutdown(),
            Err(ServerOperatorError::SocketPathReplaced(actual)) if actual == path
        ));
        assert_eq!(std::fs::read(&path).unwrap(), b"replacement");
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn every_existing_path_kind_and_stale_socket_is_preserved() {
        use std::os::unix::fs::symlink;
        use std::os::unix::net::UnixListener;

        for kind in ["file", "symlink", "directory", "socket"] {
            let (directory, config) = socket_fixture(kind);
            let path = config.unix_socket().to_path_buf();
            match kind {
                "file" => std::fs::write(&path, b"owned elsewhere").unwrap(),
                "symlink" => symlink(directory.join("missing-target"), &path).unwrap(),
                "directory" => std::fs::create_dir(&path).unwrap(),
                "socket" => {
                    let stale = UnixListener::bind(&path).unwrap();
                    drop(stale);
                }
                _ => unreachable!(),
            }
            assert!(matches!(
                idle_plane(config),
                Err(ServerOperatorError::PathExists(actual)) if actual == path
            ));
            assert!(std::fs::symlink_metadata(&path).is_ok());
            std::fs::remove_dir_all(directory).unwrap();
        }
    }
}
