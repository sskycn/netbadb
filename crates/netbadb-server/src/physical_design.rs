use std::error::Error;
use std::fmt;
use std::sync::mpsc::{self, Receiver, Sender, SyncSender, TryRecvError};
use std::sync::{Arc, Weak};

use netbadb_core::{
    Database, ExecutionFeedbackReport, PhysicalDesignAdvisorError, PhysicalDesignAdvisorPolicy,
    PhysicalDesignAdvisorReport, PhysicalDesignEvidenceEpoch, PhysicalDesignEvidenceLimits,
    PhysicalDesignEvidenceRecordError, PhysicalDesignEvidenceRecordOutcome,
    PhysicalDesignEvidenceSummary, PhysicalDesignEvidenceWindow,
    PhysicalDesignEvidenceWindowInspection, PhysicalIndexCandidate, PhysicalIndexDesignApplyError,
    PhysicalIndexDesignApplyReport, PhysicalIndexDesignProposal, PhysicalIndexDesignProposalError,
};
use netbadb_schema::SchemaFingerprint;
use netbadb_types::{
    DatabaseCommitSeq, IndexName, SchemaGeneration, StorageId, TableSchemaVersion,
};

use crate::adaptive_driver::ServerAdaptiveHostConfig;

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
    ProposalRuntimeChanged,
    ServerStopped,
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
            Self::ProposalRuntimeChanged => formatter.write_str(
                "physical-index proposal belongs to another server physical-design runtime",
            ),
            Self::ServerStopped => formatter.write_str("server physical-design control is stopped"),
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
            Self::PhysicalDesignNotEnabled
            | Self::EvidenceEpochChanged { .. }
            | Self::ProposalRuntimeChanged
            | Self::ServerStopped => None,
        }
    }
}

#[derive(Clone)]
pub struct ServerPhysicalDesignControlHandle {
    requests: Sender<ServerPhysicalDesignControlRequest>,
}

impl ServerPhysicalDesignControlHandle {
    pub(crate) const fn new(requests: Sender<ServerPhysicalDesignControlRequest>) -> Self {
        Self { requests }
    }

    pub fn status(&self) -> Result<ServerPhysicalDesignStatus, ServerPhysicalDesignControlError> {
        let (reply, response) = mpsc::sync_channel(1);
        self.requests
            .send(ServerPhysicalDesignControlRequest::Status { reply })
            .map_err(|_| ServerPhysicalDesignControlError::ServerStopped)?;
        response
            .recv()
            .map_err(|_| ServerPhysicalDesignControlError::ServerStopped)?
    }

    pub fn recommendations(
        &self,
    ) -> Result<PhysicalDesignAdvisorReport, ServerPhysicalDesignControlError> {
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
            .map_err(|_| ServerPhysicalDesignControlError::ServerStopped)?
    }

    /// Compares and rotates as one command in the sole Database worker.
    pub fn rotate_evidence_if_epoch(
        &self,
        expected: PhysicalDesignEvidenceEpoch,
    ) -> Result<ServerPhysicalDesignRotationReport, ServerPhysicalDesignControlError> {
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
    diagnostics: ServerPhysicalDesignDiagnostics,
}

impl ServerPhysicalDesignRuntime {
    pub(crate) fn new(config: ServerPhysicalDesignAdvisorConfig) -> Self {
        Self {
            identity: Arc::new(ServerPhysicalDesignRuntimeIdentity),
            evidence: PhysicalDesignEvidenceWindow::new(config.evidence_limits()),
            policy: config.advisor_policy(),
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
                let result = match proposal.origin.upgrade() {
                    Some(origin) if Arc::ptr_eq(&origin, &self.identity) => database
                        .apply_physical_index_design(&self.evidence, &proposal.proposal, index_name)
                        .map_err(|error| ServerPhysicalDesignControlError::Apply(Box::new(error))),
                    Some(_) | None => Err(ServerPhysicalDesignControlError::ProposalRuntimeChanged),
                };
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
}

pub(crate) fn forward_physical_design_control_requests<F>(
    requests: &Receiver<ServerPhysicalDesignControlRequest>,
    mut submit: F,
) where
    F: FnMut(ServerPhysicalDesignWorkerCommand) -> Result<(), ()>,
{
    loop {
        let request = match requests.try_recv() {
            Ok(request) => request,
            Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
        };
        match request {
            ServerPhysicalDesignControlRequest::Status { reply } => {
                let fallback = reply.clone();
                if submit(ServerPhysicalDesignWorkerCommand::Status { reply }).is_err() {
                    let _ = fallback.send(Err(ServerPhysicalDesignControlError::ServerStopped));
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
                if submit(ServerPhysicalDesignWorkerCommand::ProposeIndex { candidate, reply })
                    .is_err()
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
            ServerPhysicalDesignControlRequest::RotateEvidenceIfEpoch { expected, reply } => {
                let fallback = reply.clone();
                if submit(ServerPhysicalDesignWorkerCommand::RotateEvidenceIfEpoch {
                    expected,
                    reply,
                })
                .is_err()
                {
                    let _ = fallback.send(Err(ServerPhysicalDesignControlError::ServerStopped));
                }
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
        DatabaseCoordinatorConfig, PhysicalDesignAdvisorError, PhysicalDesignRecommendationPolicy,
        PhysicalIndexDesignApplyOutcome, PhysicalIndexDesignProposalError,
        PreparedExecutionFeedback, TableStorageCreateSpec,
    };
    use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
    use netbadb_types::{ColumnId, PhysicalType, TableId};

    use super::*;

    static NEXT_PATH: AtomicU64 = AtomicU64::new(1);
    const TABLE_ID: TableId = TableId(61_240);
    const CANDIDATE: PhysicalIndexCandidate = PhysicalIndexCandidate {
        table_id: TABLE_ID,
        column_id: ColumnId(2),
    };

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
}
