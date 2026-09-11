use std::collections::HashSet;
use std::error::Error;
use std::fmt;
use std::sync::mpsc::{self, Receiver, Sender, SyncSender, TryRecvError};
use std::time::{Duration, Instant};

use netbadb_core::{
    AdaptiveEvidencePoolHealth, AdaptiveEvidenceProgressToken, AdaptiveEvidenceRecordError,
    AdaptiveEvidenceRecordOutcome, AdaptiveEvidenceRotationError, AdaptiveEvidenceRotationReport,
    AdaptiveEvidenceWindowEpoch, AutomaticAdmissionScope, AutomaticCrossLaneServicePolicy,
    AutomaticMultiSafeModePolicy, AutomaticOrchestrationEnvelope, AutomaticOrchestrationInput,
    AutomaticOrchestrationStopReason, AutomaticScheduler, AutomaticSchedulerGate,
    AutomaticSchedulerPolicy, AutomaticSchedulerState, AutomaticSchedulerTick,
    AutomaticSchedulerTickOutcome, Database, MAX_AUTOMATIC_ORCHESTRATION_STEPS,
    PlannerCalibrationClass,
};
use netbadb_types::TableId;

use crate::adaptive_feedback::{
    ServerAdaptiveFeedbackConfig, ServerAdaptiveFeedbackInspection, ServerAdaptiveFeedbackRuntime,
};

/// Explicit configuration for the Server-owned host-time scheduling bridge.
///
/// The scope is owned because this value outlives each borrowed Core
/// `AutomaticAdmissionScope` passed to one scheduler invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerAdaptiveDriverConfig {
    feedback: ServerAdaptiveFeedbackConfig,
    tick_interval: Duration,
    scheduler_policy: AutomaticSchedulerPolicy,
    orchestration_envelope: AutomaticOrchestrationEnvelope,
    automatic_policy: AutomaticMultiSafeModePolicy,
    table_ids: Vec<TableId>,
    calibration_classes: Vec<PlannerCalibrationClass>,
}

impl ServerAdaptiveDriverConfig {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        feedback: ServerAdaptiveFeedbackConfig,
        tick_interval: Duration,
        scheduler_policy: AutomaticSchedulerPolicy,
        orchestration_envelope: AutomaticOrchestrationEnvelope,
        automatic_policy: AutomaticMultiSafeModePolicy,
        table_ids: Vec<TableId>,
        calibration_classes: Vec<PlannerCalibrationClass>,
    ) -> Result<Self, ServerAdaptiveDriverConfigError> {
        if tick_interval.is_zero() {
            return Err(ServerAdaptiveDriverConfigError::ZeroTickInterval);
        }
        if orchestration_envelope.max_steps == 0 {
            return Err(ServerAdaptiveDriverConfigError::ZeroOrchestrationSteps);
        }
        if orchestration_envelope.max_steps > MAX_AUTOMATIC_ORCHESTRATION_STEPS {
            return Err(
                ServerAdaptiveDriverConfigError::OrchestrationStepLimitExceeded {
                    requested: orchestration_envelope.max_steps,
                    maximum: MAX_AUTOMATIC_ORCHESTRATION_STEPS,
                },
            );
        }
        if automatic_policy.allow_columnar_compaction
            && !automatic_policy.columnar_compaction_policy.is_valid()
        {
            return Err(ServerAdaptiveDriverConfigError::InvalidColumnarCompactionPolicy);
        }
        if automatic_policy.allow_change_stream_gc
            && !automatic_policy.change_stream_gc_policy.is_valid()
        {
            return Err(ServerAdaptiveDriverConfigError::InvalidChangeStreamGcPolicy);
        }
        if matches!(
            automatic_policy.cross_lane_service,
            AutomaticCrossLaneServicePolicy::BoundedColumnarBurst {
                max_consecutive_columnar_admissions: 0
            }
        ) {
            return Err(ServerAdaptiveDriverConfigError::InvalidCrossLaneServicePolicy);
        }
        if !automatic_policy
            .safe_mode
            .planner_calibration_policy
            .is_valid()
        {
            return Err(ServerAdaptiveDriverConfigError::InvalidPlannerCalibrationPolicy);
        }
        let table_count = u64::try_from(table_ids.len()).unwrap_or(u64::MAX);
        if table_count > automatic_policy.max_candidate_tables {
            return Err(ServerAdaptiveDriverConfigError::TableScopeTooLarge {
                received: table_count,
                maximum: automatic_policy.max_candidate_tables,
            });
        }
        let calibration_count = u64::try_from(calibration_classes.len()).unwrap_or(u64::MAX);
        if calibration_count > automatic_policy.max_calibration_classes {
            return Err(ServerAdaptiveDriverConfigError::CalibrationScopeTooLarge {
                received: calibration_count,
                maximum: automatic_policy.max_calibration_classes,
            });
        }
        let mut unique_tables = HashSet::with_capacity(table_ids.len());
        for table_id in &table_ids {
            if !unique_tables.insert(*table_id) {
                return Err(ServerAdaptiveDriverConfigError::DuplicateTableId(*table_id));
            }
        }
        let mut unique_classes = HashSet::with_capacity(calibration_classes.len());
        for class in &calibration_classes {
            if !unique_classes.insert(*class) {
                return Err(ServerAdaptiveDriverConfigError::DuplicateCalibrationClass(
                    *class,
                ));
            }
        }
        Ok(Self {
            feedback,
            tick_interval,
            scheduler_policy,
            orchestration_envelope,
            automatic_policy,
            table_ids,
            calibration_classes,
        })
    }

    #[must_use]
    pub const fn feedback(&self) -> ServerAdaptiveFeedbackConfig {
        self.feedback
    }

    #[must_use]
    pub const fn tick_interval(&self) -> Duration {
        self.tick_interval
    }

    #[must_use]
    pub const fn scheduler_policy(&self) -> AutomaticSchedulerPolicy {
        self.scheduler_policy
    }

    #[must_use]
    pub const fn orchestration_envelope(&self) -> AutomaticOrchestrationEnvelope {
        self.orchestration_envelope
    }

    #[must_use]
    pub const fn automatic_policy(&self) -> AutomaticMultiSafeModePolicy {
        self.automatic_policy
    }

    #[must_use]
    pub fn table_ids(&self) -> &[TableId] {
        &self.table_ids
    }

    #[must_use]
    pub fn calibration_classes(&self) -> &[PlannerCalibrationClass] {
        &self.calibration_classes
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerAdaptiveDriverConfigError {
    ZeroTickInterval,
    ZeroOrchestrationSteps,
    OrchestrationStepLimitExceeded { requested: u32, maximum: u32 },
    DuplicateTableId(TableId),
    DuplicateCalibrationClass(PlannerCalibrationClass),
    TableScopeTooLarge { received: u64, maximum: u64 },
    CalibrationScopeTooLarge { received: u64, maximum: u64 },
    InvalidColumnarCompactionPolicy,
    InvalidChangeStreamGcPolicy,
    InvalidCrossLaneServicePolicy,
    InvalidPlannerCalibrationPolicy,
    UnknownTableId(TableId),
}

impl fmt::Display for ServerAdaptiveDriverConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroTickInterval => formatter.write_str("adaptive tick interval must be nonzero"),
            Self::ZeroOrchestrationSteps => {
                formatter.write_str("adaptive orchestration requires at least one step")
            }
            Self::OrchestrationStepLimitExceeded { requested, maximum } => write!(
                formatter,
                "adaptive orchestration requested {requested} steps, exceeding {maximum}"
            ),
            Self::DuplicateTableId(table_id) => {
                write!(formatter, "adaptive scope repeats table {}", table_id.0)
            }
            Self::DuplicateCalibrationClass(class) => {
                write!(
                    formatter,
                    "adaptive scope repeats calibration class {class:?}"
                )
            }
            Self::TableScopeTooLarge { received, maximum } => write!(
                formatter,
                "adaptive table scope has {received} entries, exceeding {maximum}"
            ),
            Self::CalibrationScopeTooLarge { received, maximum } => write!(
                formatter,
                "adaptive calibration scope has {received} entries, exceeding {maximum}"
            ),
            Self::InvalidColumnarCompactionPolicy => {
                formatter.write_str("adaptive columnar compaction policy is invalid")
            }
            Self::InvalidChangeStreamGcPolicy => {
                formatter.write_str("adaptive change-stream GC policy is invalid")
            }
            Self::InvalidCrossLaneServicePolicy => {
                formatter.write_str("adaptive cross-lane service policy is invalid")
            }
            Self::InvalidPlannerCalibrationPolicy => {
                formatter.write_str("adaptive planner calibration policy is invalid")
            }
            Self::UnknownTableId(table_id) => write!(
                formatter,
                "adaptive scope references unknown committed table {}",
                table_id.0
            ),
        }
    }
}

impl Error for ServerAdaptiveDriverConfigError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerAdaptiveMode {
    Disabled,
    FeedbackOnly,
    Driven,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServerAdaptiveFeedbackStatus {
    pub eligible_query_count: u64,
    pub record_success_count: u64,
    pub record_error_count: u64,
    pub capacity_rejection_count: u64,
    pub schema_rotation_count: u64,
    pub incomplete_report_count: u64,
    pub counter_overflowed: bool,
    pub last_record_outcome: Option<AdaptiveEvidenceRecordOutcome>,
    pub last_record_error: Option<AdaptiveEvidenceRecordError>,
    pub evidence_progress: AdaptiveEvidenceProgressToken,
    pub pool_health: AdaptiveEvidencePoolHealth,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServerAdaptiveDriverStatus {
    pub scheduler_state: AutomaticSchedulerState,
    pub last_submitted_logical_tick: Option<AutomaticSchedulerTick>,
    pub tick_pending: bool,
    pub driver_tick_count: u64,
    pub scheduler_tick_count: u64,
    pub scheduler_ran_count: u64,
    pub scheduler_held_count: u64,
    pub scheduler_error_count: u64,
    pub last_orchestration_stop_reason: Option<AutomaticOrchestrationStopReason>,
    pub host_clock_exhausted: bool,
    pub counter_overflowed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServerAdaptiveStatus {
    pub mode: ServerAdaptiveMode,
    pub feedback: Option<ServerAdaptiveFeedbackStatus>,
    pub driver: Option<ServerAdaptiveDriverStatus>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerAdaptiveControlError {
    AdaptiveNotEnabled,
    DriverNotEnabled,
    SchedulerNotFaulted,
    EvidenceWindowChanged {
        expected: AdaptiveEvidenceWindowEpoch,
        actual: AdaptiveEvidenceWindowEpoch,
    },
    EvidenceRotation(AdaptiveEvidenceRotationError),
    ServerStopped,
}

impl fmt::Display for ServerAdaptiveControlError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AdaptiveNotEnabled => formatter.write_str("adaptive feedback is not enabled"),
            Self::DriverNotEnabled => formatter.write_str("adaptive driver is not enabled"),
            Self::SchedulerNotFaulted => formatter.write_str("adaptive scheduler is not faulted"),
            Self::EvidenceWindowChanged { expected, actual } => write!(
                formatter,
                "adaptive evidence window changed from expected {} to {}",
                expected.0, actual.0
            ),
            Self::EvidenceRotation(error) => error.fmt(formatter),
            Self::ServerStopped => formatter.write_str("server adaptive control is stopped"),
        }
    }
}

impl Error for ServerAdaptiveControlError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::EvidenceRotation(error) => Some(error),
            _ => None,
        }
    }
}

#[derive(Clone)]
pub struct ServerAdaptiveControlHandle {
    requests: Sender<ServerAdaptiveControlRequest>,
}

impl ServerAdaptiveControlHandle {
    pub(crate) const fn new(requests: Sender<ServerAdaptiveControlRequest>) -> Self {
        Self { requests }
    }

    pub fn status(&self) -> Result<ServerAdaptiveStatus, ServerAdaptiveControlError> {
        let (reply, response) = mpsc::sync_channel(1);
        self.requests
            .send(ServerAdaptiveControlRequest::Status { reply })
            .map_err(|_| ServerAdaptiveControlError::ServerStopped)?;
        response
            .recv()
            .map_err(|_| ServerAdaptiveControlError::ServerStopped)?
    }

    pub fn rotate_evidence(
        &self,
    ) -> Result<AdaptiveEvidenceRotationReport, ServerAdaptiveControlError> {
        let (reply, response) = mpsc::sync_channel(1);
        self.requests
            .send(ServerAdaptiveControlRequest::RotateEvidence { reply })
            .map_err(|_| ServerAdaptiveControlError::ServerStopped)?;
        response
            .recv()
            .map_err(|_| ServerAdaptiveControlError::ServerStopped)?
    }

    /// Rotates the caller-owned evidence pool only when its current window
    /// still equals `expected`. Comparison and mutation execute as one command
    /// in the sole Database worker.
    pub fn rotate_evidence_if_window(
        &self,
        expected: AdaptiveEvidenceWindowEpoch,
    ) -> Result<AdaptiveEvidenceRotationReport, ServerAdaptiveControlError> {
        let (reply, response) = mpsc::sync_channel(1);
        self.requests
            .send(ServerAdaptiveControlRequest::RotateEvidenceIfWindow { expected, reply })
            .map_err(|_| ServerAdaptiveControlError::ServerStopped)?;
        response
            .recv()
            .map_err(|_| ServerAdaptiveControlError::ServerStopped)?
    }

    pub fn reset_faulted_scheduler(&self) -> Result<(), ServerAdaptiveControlError> {
        let (reply, response) = mpsc::sync_channel(1);
        self.requests
            .send(ServerAdaptiveControlRequest::ResetFaultedScheduler { reply })
            .map_err(|_| ServerAdaptiveControlError::ServerStopped)?;
        response
            .recv()
            .map_err(|_| ServerAdaptiveControlError::ServerStopped)?
    }
}

pub(crate) enum ServerAdaptiveControlRequest {
    Status {
        reply: SyncSender<Result<ServerAdaptiveStatus, ServerAdaptiveControlError>>,
    },
    RotateEvidence {
        reply: SyncSender<Result<AdaptiveEvidenceRotationReport, ServerAdaptiveControlError>>,
    },
    RotateEvidenceIfWindow {
        expected: AdaptiveEvidenceWindowEpoch,
        reply: SyncSender<Result<AdaptiveEvidenceRotationReport, ServerAdaptiveControlError>>,
    },
    ResetFaultedScheduler {
        reply: SyncSender<Result<(), ServerAdaptiveControlError>>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ServerAdaptiveStartupMode {
    Disabled,
    FeedbackOnly(ServerAdaptiveFeedbackConfig),
    Driven(Box<ServerAdaptiveDriverConfig>),
}

pub(crate) struct ServerAdaptiveHostConfig {
    pub(crate) tick_interval: Option<Duration>,
    pub(crate) controls: Receiver<ServerAdaptiveControlRequest>,
    pub(crate) operator_failures: Receiver<()>,
}

impl ServerAdaptiveStartupMode {
    pub(crate) const fn mode(&self) -> ServerAdaptiveMode {
        match self {
            Self::Disabled => ServerAdaptiveMode::Disabled,
            Self::FeedbackOnly(_) => ServerAdaptiveMode::FeedbackOnly,
            Self::Driven(_) => ServerAdaptiveMode::Driven,
        }
    }

    pub(crate) const fn tick_interval(&self) -> Option<Duration> {
        match self {
            Self::Driven(config) => Some(config.tick_interval()),
            Self::Disabled | Self::FeedbackOnly(_) => None,
        }
    }
}

pub(crate) enum ServerAdaptiveWorkerCommand {
    Tick {
        tick: AutomaticSchedulerTick,
        reply: SyncSender<()>,
    },
    Inspect {
        host: Option<ServerAdaptiveHostSnapshot>,
        reply: SyncSender<Result<ServerAdaptiveStatus, ServerAdaptiveControlError>>,
    },
    RotateEvidence {
        reply: SyncSender<Result<AdaptiveEvidenceRotationReport, ServerAdaptiveControlError>>,
    },
    RotateEvidenceIfWindow {
        expected: AdaptiveEvidenceWindowEpoch,
        reply: SyncSender<Result<AdaptiveEvidenceRotationReport, ServerAdaptiveControlError>>,
    },
    ResetFaultedScheduler {
        reply: SyncSender<Result<(), ServerAdaptiveControlError>>,
    },
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct ServerAdaptiveDriverDiagnostics {
    scheduler_tick_count: u64,
    scheduler_ran_count: u64,
    scheduler_held_count: u64,
    scheduler_error_count: u64,
    last_orchestration_stop_reason: Option<AutomaticOrchestrationStopReason>,
    counter_overflowed: bool,
}

struct ServerAdaptiveDriverRuntime {
    scheduler: AutomaticScheduler,
    table_ids: Vec<TableId>,
    calibration_classes: Vec<PlannerCalibrationClass>,
    orchestration_envelope: AutomaticOrchestrationEnvelope,
    automatic_policy: AutomaticMultiSafeModePolicy,
    diagnostics: ServerAdaptiveDriverDiagnostics,
}

pub(crate) struct ServerAdaptiveWorkerRuntime {
    feedback: ServerAdaptiveFeedbackRuntime,
    driver: Option<ServerAdaptiveDriverRuntime>,
}

impl ServerAdaptiveWorkerRuntime {
    pub(crate) fn new(
        mode: ServerAdaptiveStartupMode,
        database: &Database,
    ) -> Result<Option<Self>, ServerAdaptiveDriverConfigError> {
        match mode {
            ServerAdaptiveStartupMode::Disabled => Ok(None),
            ServerAdaptiveStartupMode::FeedbackOnly(config) => Ok(Some(Self {
                feedback: ServerAdaptiveFeedbackRuntime::new(config),
                driver: None,
            })),
            ServerAdaptiveStartupMode::Driven(config) => {
                for table_id in config.table_ids() {
                    if !database
                        .schema()
                        .tables()
                        .iter()
                        .any(|table| table.id == *table_id)
                    {
                        return Err(ServerAdaptiveDriverConfigError::UnknownTableId(*table_id));
                    }
                }
                let feedback = ServerAdaptiveFeedbackRuntime::new(config.feedback());
                let driver = ServerAdaptiveDriverRuntime {
                    scheduler: AutomaticScheduler::new(config.scheduler_policy()),
                    table_ids: config.table_ids,
                    calibration_classes: config.calibration_classes,
                    orchestration_envelope: config.orchestration_envelope,
                    automatic_policy: config.automatic_policy,
                    diagnostics: ServerAdaptiveDriverDiagnostics::default(),
                };
                Ok(Some(Self {
                    feedback,
                    driver: Some(driver),
                }))
            }
        }
    }

    pub(crate) fn feedback_mut(&mut self) -> &mut ServerAdaptiveFeedbackRuntime {
        &mut self.feedback
    }

    pub(crate) fn handle(&mut self, database: &mut Database, command: ServerAdaptiveWorkerCommand) {
        match command {
            ServerAdaptiveWorkerCommand::Tick { tick, reply } => {
                if let Some(driver) = self.driver.as_mut() {
                    driver.increment(|diagnostics| &mut diagnostics.scheduler_tick_count);
                    let input = AutomaticOrchestrationInput {
                        scope: AutomaticAdmissionScope {
                            table_ids: &driver.table_ids,
                            calibration_classes: &driver.calibration_classes,
                        },
                        envelope: driver.orchestration_envelope,
                    };
                    match driver.scheduler.tick(
                        database,
                        self.feedback.pool(),
                        tick,
                        input,
                        driver.automatic_policy,
                    ) {
                        Ok(report) => match report.outcome {
                            AutomaticSchedulerTickOutcome::Held(_) => driver
                                .increment(|diagnostics| &mut diagnostics.scheduler_held_count),
                            AutomaticSchedulerTickOutcome::Ran(orchestration) => {
                                driver
                                    .increment(|diagnostics| &mut diagnostics.scheduler_ran_count);
                                driver.diagnostics.last_orchestration_stop_reason =
                                    Some(orchestration.stop_reason);
                            }
                        },
                        Err(_) => {
                            driver.increment(|diagnostics| &mut diagnostics.scheduler_error_count);
                        }
                    }
                }
                let _ = reply.send(());
            }
            ServerAdaptiveWorkerCommand::Inspect { host, reply } => {
                let _ = reply.send(Ok(self.status(host)));
            }
            ServerAdaptiveWorkerCommand::RotateEvidence { reply } => {
                let result = self
                    .feedback
                    .rotate_window()
                    .map_err(ServerAdaptiveControlError::EvidenceRotation);
                let _ = reply.send(result);
            }
            ServerAdaptiveWorkerCommand::RotateEvidenceIfWindow { expected, reply } => {
                let actual = self.feedback.pool().progress_token().window_epoch;
                let result = if actual == expected {
                    self.feedback
                        .rotate_window()
                        .map_err(ServerAdaptiveControlError::EvidenceRotation)
                } else {
                    Err(ServerAdaptiveControlError::EvidenceWindowChanged { expected, actual })
                };
                let _ = reply.send(result);
            }
            ServerAdaptiveWorkerCommand::ResetFaultedScheduler { reply } => {
                let result = match self.driver.as_mut() {
                    None => Err(ServerAdaptiveControlError::DriverNotEnabled),
                    Some(driver)
                        if !matches!(
                            driver.scheduler.state().gate,
                            AutomaticSchedulerGate::Faulted(_)
                        ) =>
                    {
                        Err(ServerAdaptiveControlError::SchedulerNotFaulted)
                    }
                    Some(driver) => {
                        driver.scheduler = AutomaticScheduler::new(driver.scheduler.policy());
                        Ok(())
                    }
                };
                let _ = reply.send(result);
            }
        }
    }

    fn status(&self, host: Option<ServerAdaptiveHostSnapshot>) -> ServerAdaptiveStatus {
        let feedback = feedback_status(self.feedback.inspection());
        let driver = self.driver.as_ref().map(|driver| {
            let host = host.unwrap_or_default();
            ServerAdaptiveDriverStatus {
                scheduler_state: driver.scheduler.state(),
                last_submitted_logical_tick: host.last_submitted_logical_tick,
                tick_pending: host.tick_pending,
                driver_tick_count: host.driver_tick_count,
                scheduler_tick_count: driver.diagnostics.scheduler_tick_count,
                scheduler_ran_count: driver.diagnostics.scheduler_ran_count,
                scheduler_held_count: driver.diagnostics.scheduler_held_count,
                scheduler_error_count: driver.diagnostics.scheduler_error_count,
                last_orchestration_stop_reason: driver.diagnostics.last_orchestration_stop_reason,
                host_clock_exhausted: host.host_clock_exhausted,
                counter_overflowed: host.counter_overflowed
                    || driver.diagnostics.counter_overflowed,
            }
        });
        ServerAdaptiveStatus {
            mode: if driver.is_some() {
                ServerAdaptiveMode::Driven
            } else {
                ServerAdaptiveMode::FeedbackOnly
            },
            feedback: Some(feedback),
            driver,
        }
    }
}

impl ServerAdaptiveDriverRuntime {
    fn increment(&mut self, counter: fn(&mut ServerAdaptiveDriverDiagnostics) -> &mut u64) {
        let value = counter(&mut self.diagnostics);
        match value.checked_add(1) {
            Some(next) => *value = next,
            None => self.diagnostics.counter_overflowed = true,
        }
    }
}

fn feedback_status(inspection: ServerAdaptiveFeedbackInspection) -> ServerAdaptiveFeedbackStatus {
    ServerAdaptiveFeedbackStatus {
        eligible_query_count: inspection.diagnostics.eligible_query_count,
        record_success_count: inspection.diagnostics.record_success_count,
        record_error_count: inspection.diagnostics.record_error_count,
        capacity_rejection_count: inspection.diagnostics.capacity_rejection_count,
        schema_rotation_count: inspection.diagnostics.schema_rotation_count,
        incomplete_report_count: inspection.diagnostics.incomplete_report_count,
        counter_overflowed: inspection.diagnostics.counter_overflowed,
        last_record_outcome: inspection.diagnostics.last_record_outcome,
        last_record_error: inspection.diagnostics.last_record_error,
        evidence_progress: inspection.progress,
        pool_health: inspection.health,
    }
}

pub(crate) fn disabled_status() -> ServerAdaptiveStatus {
    ServerAdaptiveStatus {
        mode: ServerAdaptiveMode::Disabled,
        feedback: None,
        driver: None,
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct ServerAdaptiveHostSnapshot {
    last_submitted_logical_tick: Option<AutomaticSchedulerTick>,
    tick_pending: bool,
    driver_tick_count: u64,
    host_clock_exhausted: bool,
    counter_overflowed: bool,
}

#[derive(Debug)]
struct ServerAdaptiveCadence {
    interval: Duration,
    next_due: Duration,
    next_tick: u64,
    pending: bool,
    last_submitted_logical_tick: Option<AutomaticSchedulerTick>,
    driver_tick_count: u64,
    exhausted: bool,
    counter_overflowed: bool,
}

impl ServerAdaptiveCadence {
    fn new(interval: Duration) -> Self {
        Self {
            interval,
            next_due: interval,
            next_tick: 1,
            pending: false,
            last_submitted_logical_tick: None,
            driver_tick_count: 0,
            exhausted: false,
            counter_overflowed: false,
        }
    }

    fn offer(&mut self, elapsed: Duration) -> Option<AutomaticSchedulerTick> {
        if self.pending || self.exhausted || elapsed < self.next_due {
            return None;
        }
        let tick = AutomaticSchedulerTick(self.next_tick);
        match self.next_tick.checked_add(1) {
            Some(next) => self.next_tick = next,
            None => self.exhausted = true,
        }
        self.pending = true;
        self.last_submitted_logical_tick = Some(tick);
        match self.driver_tick_count.checked_add(1) {
            Some(next) => self.driver_tick_count = next,
            None => self.counter_overflowed = true,
        }
        self.next_due = elapsed.checked_add(self.interval).unwrap_or(Duration::MAX);
        Some(tick)
    }

    fn complete(&mut self) {
        self.pending = false;
    }

    fn snapshot(&self) -> ServerAdaptiveHostSnapshot {
        ServerAdaptiveHostSnapshot {
            last_submitted_logical_tick: self.last_submitted_logical_tick,
            tick_pending: self.pending,
            driver_tick_count: self.driver_tick_count,
            host_clock_exhausted: self.exhausted,
            counter_overflowed: self.counter_overflowed,
        }
    }
}

pub(crate) struct ServerAdaptiveHostDriver {
    started_at: Instant,
    cadence: ServerAdaptiveCadence,
    pending_reply: Option<Receiver<()>>,
}

impl ServerAdaptiveHostDriver {
    pub(crate) fn new(interval: Duration) -> Self {
        Self {
            started_at: Instant::now(),
            cadence: ServerAdaptiveCadence::new(interval),
            pending_reply: None,
        }
    }

    pub(crate) fn poll<F>(&mut self, mut submit: F)
    where
        F: FnMut(ServerAdaptiveWorkerCommand) -> Result<(), ()>,
    {
        if let Some(reply) = self.pending_reply.as_ref() {
            match reply.try_recv() {
                Ok(()) | Err(TryRecvError::Disconnected) => {
                    self.pending_reply = None;
                    self.cadence.complete();
                }
                Err(TryRecvError::Empty) => {}
            }
        }
        let Some(tick) = self.cadence.offer(self.started_at.elapsed()) else {
            return;
        };
        let (reply, response) = mpsc::sync_channel(1);
        if submit(ServerAdaptiveWorkerCommand::Tick { tick, reply }).is_ok() {
            self.pending_reply = Some(response);
        } else {
            self.cadence.complete();
        }
    }

    pub(crate) fn snapshot(&self) -> ServerAdaptiveHostSnapshot {
        self.cadence.snapshot()
    }
}

pub(crate) fn forward_control_requests<F>(
    requests: &Receiver<ServerAdaptiveControlRequest>,
    host: Option<ServerAdaptiveHostSnapshot>,
    mut submit: F,
) where
    F: FnMut(ServerAdaptiveWorkerCommand) -> Result<(), ()>,
{
    loop {
        let request = match requests.try_recv() {
            Ok(request) => request,
            Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
        };
        match request {
            ServerAdaptiveControlRequest::Status { reply } => {
                let fallback = reply.clone();
                if submit(ServerAdaptiveWorkerCommand::Inspect { host, reply }).is_err() {
                    let _ = fallback.send(Err(ServerAdaptiveControlError::ServerStopped));
                }
            }
            ServerAdaptiveControlRequest::RotateEvidence { reply } => {
                let fallback = reply.clone();
                if submit(ServerAdaptiveWorkerCommand::RotateEvidence { reply }).is_err() {
                    let _ = fallback.send(Err(ServerAdaptiveControlError::ServerStopped));
                }
            }
            ServerAdaptiveControlRequest::RotateEvidenceIfWindow { expected, reply } => {
                let fallback = reply.clone();
                if submit(ServerAdaptiveWorkerCommand::RotateEvidenceIfWindow { expected, reply })
                    .is_err()
                {
                    let _ = fallback.send(Err(ServerAdaptiveControlError::ServerStopped));
                }
            }
            ServerAdaptiveControlRequest::ResetFaultedScheduler { reply } => {
                let fallback = reply.clone();
                if submit(ServerAdaptiveWorkerCommand::ResetFaultedScheduler { reply }).is_err() {
                    let _ = fallback.send(Err(ServerAdaptiveControlError::ServerStopped));
                }
            }
        }
    }
}

pub(crate) fn handle_disabled_worker_command(command: ServerAdaptiveWorkerCommand) {
    match command {
        ServerAdaptiveWorkerCommand::Tick { reply, .. } => {
            let _ = reply.send(());
        }
        ServerAdaptiveWorkerCommand::Inspect { reply, .. } => {
            let _ = reply.send(Ok(disabled_status()));
        }
        ServerAdaptiveWorkerCommand::RotateEvidence { reply } => {
            let _ = reply.send(Err(ServerAdaptiveControlError::AdaptiveNotEnabled));
        }
        ServerAdaptiveWorkerCommand::RotateEvidenceIfWindow { reply, .. } => {
            let _ = reply.send(Err(ServerAdaptiveControlError::AdaptiveNotEnabled));
        }
        ServerAdaptiveWorkerCommand::ResetFaultedScheduler { reply } => {
            let _ = reply.send(Err(ServerAdaptiveControlError::DriverNotEnabled));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    use netbadb_core::{
        AdaptiveChangeStreamGcPolicy, AdaptiveEvidencePoolLimits, MaintenanceBudget,
        TableStorageCreateSpec,
    };
    use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
    use netbadb_types::{ColumnId, PhysicalType};

    use super::*;

    static NEXT_PATH: AtomicU64 = AtomicU64::new(1);

    fn envelope(max_steps: u32) -> AutomaticOrchestrationEnvelope {
        let budget = MaintenanceBudget::new(u64::MAX, u64::MAX, u64::MAX, max_steps);
        AutomaticOrchestrationEnvelope {
            max_steps,
            per_step_maintenance_budget: budget,
            run_maintenance_budget: budget,
        }
    }

    fn config(
        table_ids: Vec<TableId>,
    ) -> Result<ServerAdaptiveDriverConfig, ServerAdaptiveDriverConfigError> {
        ServerAdaptiveDriverConfig::new(
            ServerAdaptiveFeedbackConfig::new(AdaptiveEvidencePoolLimits::default()),
            Duration::from_millis(100),
            AutomaticSchedulerPolicy::new(1, 2, 2, 3).expect("valid scheduler policy"),
            envelope(4),
            AutomaticMultiSafeModePolicy::default(),
            table_ids,
            vec![PlannerCalibrationClass::SeqScan],
        )
    }

    fn fixture(name: &str) -> (PathBuf, Database) {
        let suffix = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "netbadb-server-driver-{name}-{}-{suffix}",
            std::process::id()
        ));
        fs::create_dir_all(&root).expect("create driver fixture root");
        let table = TableDef::new(
            TableId(61_601),
            "events",
            vec![ColumnDef::new(
                ColumnId(1),
                "id",
                TypeSpec::Physical(PhysicalType::Int64),
            )],
        );
        let database = Database::create_catalog(
            root.join("catalog"),
            vec![TableStorageCreateSpec::heap(root.join("events.ndb"), table)],
            None,
        )
        .expect("create driver fixture");
        (root, database)
    }

    #[test]
    fn cadence_delays_first_tick_and_coalesces_missed_intervals() {
        let interval = Duration::from_millis(100);
        let mut cadence = ServerAdaptiveCadence::new(interval);
        assert_eq!(cadence.offer(Duration::ZERO), None);
        assert_eq!(cadence.offer(Duration::from_millis(99)), None);
        assert_eq!(
            cadence.offer(Duration::from_millis(100)),
            Some(AutomaticSchedulerTick(1))
        );
        assert_eq!(cadence.offer(Duration::from_millis(400)), None);
        cadence.complete();
        assert_eq!(
            cadence.offer(Duration::from_millis(400)),
            Some(AutomaticSchedulerTick(2))
        );
        cadence.complete();
        assert_eq!(cadence.offer(Duration::from_millis(499)), None);
        assert_eq!(
            cadence.offer(Duration::from_millis(500)),
            Some(AutomaticSchedulerTick(3))
        );
    }

    #[test]
    fn cadence_exhaustion_never_wraps() {
        let mut cadence = ServerAdaptiveCadence::new(Duration::from_nanos(1));
        cadence.next_tick = u64::MAX;
        assert_eq!(
            cadence.offer(Duration::from_nanos(1)),
            Some(AutomaticSchedulerTick(u64::MAX))
        );
        assert!(cadence.snapshot().host_clock_exhausted);
        cadence.complete();
        assert_eq!(cadence.offer(Duration::from_nanos(2)), None);
    }

    #[test]
    fn config_rejects_zero_duplicate_oversized_and_unknown_scope() {
        let base = config(vec![TableId(1)]).expect("valid base driver config");
        assert_eq!(
            ServerAdaptiveDriverConfig::new(
                base.feedback(),
                Duration::ZERO,
                base.scheduler_policy(),
                base.orchestration_envelope(),
                base.automatic_policy(),
                base.table_ids().to_vec(),
                base.calibration_classes().to_vec(),
            ),
            Err(ServerAdaptiveDriverConfigError::ZeroTickInterval)
        );
        assert_eq!(
            config(vec![TableId(1), TableId(1)]),
            Err(ServerAdaptiveDriverConfigError::DuplicateTableId(TableId(
                1
            )))
        );
        assert_eq!(
            ServerAdaptiveDriverConfig::new(
                base.feedback(),
                base.tick_interval(),
                base.scheduler_policy(),
                base.orchestration_envelope(),
                base.automatic_policy(),
                vec![TableId(1)],
                vec![
                    PlannerCalibrationClass::SeqScan,
                    PlannerCalibrationClass::SeqScan,
                ],
            ),
            Err(ServerAdaptiveDriverConfigError::DuplicateCalibrationClass(
                PlannerCalibrationClass::SeqScan
            ))
        );
        assert_eq!(
            config((0..17).map(TableId).collect()),
            Err(ServerAdaptiveDriverConfigError::TableScopeTooLarge {
                received: 17,
                maximum: 16,
            })
        );
        let mut invalid_planner = base.automatic_policy();
        invalid_planner
            .safe_mode
            .planner_calibration_policy
            .maximum_step_up_ratio = netbadb_core::CalibrationRatio::HALF;
        assert_eq!(
            ServerAdaptiveDriverConfig::new(
                base.feedback(),
                base.tick_interval(),
                base.scheduler_policy(),
                base.orchestration_envelope(),
                invalid_planner,
                base.table_ids().to_vec(),
                base.calibration_classes().to_vec(),
            ),
            Err(ServerAdaptiveDriverConfigError::InvalidPlannerCalibrationPolicy)
        );

        let (root, database) = fixture("unknown-scope");
        let result = ServerAdaptiveWorkerRuntime::new(
            ServerAdaptiveStartupMode::Driven(Box::new(
                config(vec![TableId(99)]).expect("structurally valid config"),
            )),
            &database,
        );
        assert!(matches!(
            result,
            Err(ServerAdaptiveDriverConfigError::UnknownTableId(TableId(99)))
        ));
        database.close().expect("close driver fixture");
        fs::remove_dir_all(root).expect("remove driver fixture");
    }

    #[test]
    fn worker_tick_runs_scheduler_once_and_controls_are_explicit() {
        let (root, mut database) = fixture("worker");
        let mut runtime = ServerAdaptiveWorkerRuntime::new(
            ServerAdaptiveStartupMode::Driven(Box::new(
                config(vec![TableId(61_601)]).expect("valid driver config"),
            )),
            &database,
        )
        .expect("validate driver scope")
        .expect("driven runtime");

        let (reply, response) = mpsc::sync_channel(1);
        runtime.handle(
            &mut database,
            ServerAdaptiveWorkerCommand::Tick {
                tick: AutomaticSchedulerTick(1),
                reply,
            },
        );
        response.recv().expect("tick completion");
        let status = runtime.status(Some(ServerAdaptiveHostSnapshot {
            last_submitted_logical_tick: Some(AutomaticSchedulerTick(1)),
            tick_pending: false,
            driver_tick_count: 1,
            ..ServerAdaptiveHostSnapshot::default()
        }));
        let driver = status.driver.expect("driver status");
        assert_eq!(driver.scheduler_tick_count, 1);
        assert_eq!(driver.scheduler_ran_count, 1);
        assert_eq!(driver.scheduler_held_count, 0);
        assert_eq!(driver.scheduler_error_count, 0);

        let (reply, response) = mpsc::sync_channel(1);
        runtime.handle(
            &mut database,
            ServerAdaptiveWorkerCommand::ResetFaultedScheduler { reply },
        );
        assert_eq!(
            response.recv().expect("reset response"),
            Err(ServerAdaptiveControlError::SchedulerNotFaulted)
        );

        let (reply, response) = mpsc::sync_channel(1);
        runtime.handle(
            &mut database,
            ServerAdaptiveWorkerCommand::RotateEvidence { reply },
        );
        let rotation = response
            .recv()
            .expect("rotation response")
            .expect("explicit rotation succeeds");
        assert_eq!(rotation.new_window_epoch.0, 1);
        assert_eq!(
            runtime
                .status(None)
                .feedback
                .expect("feedback status")
                .evidence_progress
                .window_epoch
                .0,
            1
        );

        let (reply, response) = mpsc::sync_channel(1);
        runtime.handle(
            &mut database,
            ServerAdaptiveWorkerCommand::RotateEvidenceIfWindow {
                expected: AdaptiveEvidenceWindowEpoch(0),
                reply,
            },
        );
        assert_eq!(
            response.recv().expect("conditional rotation response"),
            Err(ServerAdaptiveControlError::EvidenceWindowChanged {
                expected: AdaptiveEvidenceWindowEpoch(0),
                actual: AdaptiveEvidenceWindowEpoch(1),
            })
        );
        assert_eq!(
            runtime
                .status(None)
                .feedback
                .expect("feedback status")
                .evidence_progress
                .window_epoch,
            AdaptiveEvidenceWindowEpoch(1)
        );

        let (reply, response) = mpsc::sync_channel(1);
        runtime.handle(
            &mut database,
            ServerAdaptiveWorkerCommand::RotateEvidenceIfWindow {
                expected: AdaptiveEvidenceWindowEpoch(1),
                reply,
            },
        );
        assert_eq!(
            response
                .recv()
                .expect("conditional rotation response")
                .expect("matching conditional rotation succeeds")
                .new_window_epoch,
            AdaptiveEvidenceWindowEpoch(2)
        );

        database.close().expect("close driver fixture");
        fs::remove_dir_all(root).expect("remove driver fixture");
    }

    #[test]
    fn feedback_only_supports_rotation_but_has_no_scheduler() {
        let (root, mut database) = fixture("feedback-only");
        let mut runtime = ServerAdaptiveWorkerRuntime::new(
            ServerAdaptiveStartupMode::FeedbackOnly(ServerAdaptiveFeedbackConfig::new(
                AdaptiveEvidencePoolLimits::default(),
            )),
            &database,
        )
        .expect("feedback-only startup")
        .expect("feedback runtime");
        assert_eq!(runtime.status(None).mode, ServerAdaptiveMode::FeedbackOnly);

        let (reply, response) = mpsc::sync_channel(1);
        runtime.handle(
            &mut database,
            ServerAdaptiveWorkerCommand::RotateEvidence { reply },
        );
        assert_eq!(
            response
                .recv()
                .expect("rotation response")
                .expect("rotation succeeds")
                .new_window_epoch
                .0,
            1
        );
        let (reply, response) = mpsc::sync_channel(1);
        runtime.handle(
            &mut database,
            ServerAdaptiveWorkerCommand::ResetFaultedScheduler { reply },
        );
        assert_eq!(
            response.recv().expect("reset response"),
            Err(ServerAdaptiveControlError::DriverNotEnabled)
        );

        database.close().expect("close driver fixture");
        fs::remove_dir_all(root).expect("remove driver fixture");
    }

    #[test]
    fn disabled_commands_report_typed_mode_and_control_errors() {
        let (reply, response) = mpsc::sync_channel(1);
        handle_disabled_worker_command(ServerAdaptiveWorkerCommand::Inspect { host: None, reply });
        assert_eq!(
            response.recv().expect("status response"),
            Ok(disabled_status())
        );

        let (reply, response) = mpsc::sync_channel(1);
        handle_disabled_worker_command(ServerAdaptiveWorkerCommand::RotateEvidence { reply });
        assert_eq!(
            response.recv().expect("rotation response"),
            Err(ServerAdaptiveControlError::AdaptiveNotEnabled)
        );

        let (reply, response) = mpsc::sync_channel(1);
        handle_disabled_worker_command(ServerAdaptiveWorkerCommand::ResetFaultedScheduler {
            reply,
        });
        assert_eq!(
            response.recv().expect("reset response"),
            Err(ServerAdaptiveControlError::DriverNotEnabled)
        );
    }

    #[test]
    fn scheduler_fault_is_isolated_and_only_explicit_reset_reopens_it() {
        let (root, mut database) = fixture("fault-reset");
        // Public construction rejects this structural error. Inject it only
        // through this private seam to exercise the background fault path.
        let mut injected = config(vec![TableId(61_601)]).expect("base config");
        injected.automatic_policy.allow_change_stream_gc = true;
        injected.automatic_policy.change_stream_gc_policy = AdaptiveChangeStreamGcPolicy::new(0, 0);
        let mut runtime = ServerAdaptiveWorkerRuntime::new(
            ServerAdaptiveStartupMode::Driven(Box::new(injected)),
            &database,
        )
        .expect("scope remains valid")
        .expect("driven runtime");
        let evidence_before = runtime.feedback.pool().progress_token();

        let (reply, response) = mpsc::sync_channel(1);
        runtime.handle(
            &mut database,
            ServerAdaptiveWorkerCommand::Tick {
                tick: AutomaticSchedulerTick(1),
                reply,
            },
        );
        response.recv().expect("faulted tick completion");
        let faulted = runtime.status(None).driver.expect("driver status");
        assert!(matches!(
            faulted.scheduler_state.gate,
            AutomaticSchedulerGate::Faulted(_)
        ));
        assert_eq!(faulted.scheduler_error_count, 1);

        let (reply, response) = mpsc::sync_channel(1);
        runtime.handle(
            &mut database,
            ServerAdaptiveWorkerCommand::ResetFaultedScheduler { reply },
        );
        assert_eq!(response.recv().expect("reset response"), Ok(()));
        let reset = runtime.status(None).driver.expect("driver status");
        assert_eq!(
            reset.scheduler_state,
            AutomaticScheduler::new(
                AutomaticSchedulerPolicy::new(1, 2, 2, 3).expect("valid scheduler policy")
            )
            .state()
        );
        assert_eq!(reset.scheduler_tick_count, 1);
        assert_eq!(runtime.feedback.pool().progress_token(), evidence_before);

        database.close().expect("close driver fixture");
        fs::remove_dir_all(root).expect("remove driver fixture");
    }
}
