use std::error::Error;
use std::fmt;
use std::sync::mpsc::{self, Receiver, Sender, SyncSender, TryRecvError};

use netbadb_core::{
    Database, ExecutionFeedbackReport, PhysicalDesignAdvisorError, PhysicalDesignAdvisorPolicy,
    PhysicalDesignAdvisorReport, PhysicalDesignEvidenceEpoch, PhysicalDesignEvidenceLimits,
    PhysicalDesignEvidenceRecordError, PhysicalDesignEvidenceRecordOutcome,
    PhysicalDesignEvidenceWindow, PhysicalDesignEvidenceWindowInspection,
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

#[derive(Debug)]
pub enum ServerPhysicalDesignControlError {
    PhysicalDesignNotEnabled,
    EvidenceEpochChanged {
        expected: PhysicalDesignEvidenceEpoch,
        actual: PhysicalDesignEvidenceEpoch,
    },
    EvidenceRotation(PhysicalDesignEvidenceRecordError),
    Advisor(PhysicalDesignAdvisorError),
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
            Self::ServerStopped => formatter.write_str("server physical-design control is stopped"),
        }
    }
}

impl Error for ServerPhysicalDesignControlError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::EvidenceRotation(error) => Some(error),
            Self::Advisor(error) => Some(error),
            Self::PhysicalDesignNotEnabled
            | Self::EvidenceEpochChanged { .. }
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
    RotateEvidenceIfEpoch {
        expected: PhysicalDesignEvidenceEpoch,
        reply: SyncSender<
            Result<ServerPhysicalDesignRotationReport, ServerPhysicalDesignControlError>,
        >,
    },
}

/// Independent runtime owned beside, never inside, the adaptive runtime.
pub(crate) struct ServerPhysicalDesignRuntime {
    evidence: PhysicalDesignEvidenceWindow,
    policy: PhysicalDesignAdvisorPolicy,
    diagnostics: ServerPhysicalDesignDiagnostics,
}

impl ServerPhysicalDesignRuntime {
    pub(crate) const fn new(config: ServerPhysicalDesignAdvisorConfig) -> Self {
        Self {
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
        database: &Database,
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
        ServerPhysicalDesignWorkerCommand::RotateEvidenceIfEpoch { reply, .. } => {
            let _ = reply.send(Err(
                ServerPhysicalDesignControlError::PhysicalDesignNotEnabled,
            ));
        }
    }
}
