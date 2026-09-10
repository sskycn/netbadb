use std::error::Error;
use std::fmt;

use netbadb_planner::{CalibrationRatio, PlannerCalibrationClass, PlannerCalibrationEpoch};
use netbadb_types::{ColumnarGeneration, ColumnarProjectionId, SchemaGeneration, TableId};

use crate::planner_calibration::{aggregate_calibration_evidence, replay_calibration_ratio_errors};
use crate::{
    AdaptiveChangeStreamGcDecision, AdaptiveChangeStreamGcExecutionReport,
    AdaptiveChangeStreamGcNoActionReason, AdaptiveChangeStreamGcOutcome,
    AdaptiveChangeStreamGcPolicy, AdaptiveChangeStreamGcProposal,
    AdaptiveColumnarCompactionDecision, AdaptiveColumnarCompactionError,
    AdaptiveColumnarCompactionExecutionReport, AdaptiveColumnarCompactionNoActionReason,
    AdaptiveColumnarCompactionObservation, AdaptiveColumnarCompactionOutcome,
    AdaptiveColumnarCompactionPolicy, AdaptiveColumnarCompactionProposal, AdaptiveCycleReport,
    AdaptiveDecision, AdaptiveError, AdaptiveEvidencePool, AdaptiveLsmCompactionPolicy,
    AdaptiveLsmFlushPolicy, AdaptiveLsmMaintenanceDecision, AdaptiveLsmMaintenanceError,
    AdaptiveLsmMaintenanceExecutionReport, AdaptiveLsmMaintenanceNoActionReason,
    AdaptiveLsmMaintenanceOutcome, AdaptiveLsmMaintenanceProposal, AdaptiveMaintenanceOutcome,
    AdaptiveNoActionReason, AdaptivePolicy, AdaptiveWorkloadEvaluationReport,
    AdaptiveWorkloadLimits, AdaptiveWorkloadOutcome, AdaptiveWorkloadPolicy,
    AdaptiveWorkloadStaleReason, AdaptiveWorkloadTarget, AdaptiveWorkloadWindow, Database,
    DatabaseError, MaintenanceAction, MaintenanceBudget, PlannerCalibrationAdvisorError,
    PlannerCalibrationDecision, PlannerCalibrationEvidence, PlannerCalibrationMutationError,
    PlannerCalibrationNoAction, PlannerCalibrationPolicy, PlannerCalibrationProposal,
    PlannerCalibrationReceipt, PlannerCalibrationShadowDecision, PlannerCalibrationShadowReport,
};
use netbadb_types::{StorageDataVersion, StorageId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AutomaticCalibrationTrialPolicy {
    pub minimum_samples: u64,
    pub minimum_actual_work_units: u64,
    pub minimum_distinct_visibility_points: u64,
    pub minimum_distinct_query_shapes: u64,
    pub minimum_keep_error_improvement_work_units: u64,
    pub maximum_tolerated_error_regression_work_units: u64,
}

impl Default for AutomaticCalibrationTrialPolicy {
    fn default() -> Self {
        Self {
            minimum_samples: 8,
            minimum_actual_work_units: 1,
            minimum_distinct_visibility_points: 2,
            minimum_distinct_query_shapes: 3,
            minimum_keep_error_improvement_work_units: 1,
            maximum_tolerated_error_regression_work_units: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AutomaticSafeModePolicy {
    pub allow_columnar_maintenance: bool,
    pub allow_planner_calibration: bool,
    pub adaptive_policy: AdaptivePolicy,
    pub workload_policy: AdaptiveWorkloadPolicy,
    pub planner_calibration_policy: PlannerCalibrationPolicy,
    pub calibration_trial_policy: AutomaticCalibrationTrialPolicy,
}

#[derive(Debug, Clone, Copy)]
pub struct AutomaticSafeModeInput<'a> {
    pub columnar_table_id: Option<TableId>,
    pub workload_window: Option<&'a AdaptiveWorkloadWindow>,
    pub calibration_class: Option<PlannerCalibrationClass>,
    pub maintenance_budget: MaintenanceBudget,
}

impl<'a> AutomaticSafeModeInput<'a> {
    #[must_use]
    pub const fn new(maintenance_budget: MaintenanceBudget) -> Self {
        Self {
            columnar_table_id: None,
            workload_window: None,
            calibration_class: None,
            maintenance_budget,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AutomaticColumnarTrial {
    target: AdaptiveWorkloadTarget,
}

impl AutomaticColumnarTrial {
    #[must_use]
    pub const fn target(self) -> AdaptiveWorkloadTarget {
        self.target
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AutomaticPlannerCalibrationTrial {
    calibration_class: PlannerCalibrationClass,
    schema_generation: SchemaGeneration,
    previous_epoch: PlannerCalibrationEpoch,
    applied_epoch: PlannerCalibrationEpoch,
    previous_ratio: CalibrationRatio,
    applied_ratio: CalibrationRatio,
}

impl AutomaticPlannerCalibrationTrial {
    #[must_use]
    pub const fn calibration_class(self) -> PlannerCalibrationClass {
        self.calibration_class
    }

    #[must_use]
    pub const fn schema_generation(self) -> SchemaGeneration {
        self.schema_generation
    }

    #[must_use]
    pub const fn previous_epoch(self) -> PlannerCalibrationEpoch {
        self.previous_epoch
    }

    #[must_use]
    pub const fn applied_epoch(self) -> PlannerCalibrationEpoch {
        self.applied_epoch
    }

    #[must_use]
    pub const fn previous_ratio(self) -> CalibrationRatio {
        self.previous_ratio
    }

    #[must_use]
    pub const fn applied_ratio(self) -> CalibrationRatio {
        self.applied_ratio
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutomaticSafeTrial {
    Columnar(AutomaticColumnarTrial),
    PlannerCalibration(AutomaticPlannerCalibrationTrial),
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AutomaticSafeModeState {
    active_trial: Option<AutomaticSafeTrial>,
    cross_lane_service: AutomaticCrossLaneServiceState,
}

impl AutomaticSafeModeState {
    #[must_use]
    pub const fn active_trial(self) -> Option<AutomaticSafeTrial> {
        self.active_trial
    }

    #[must_use]
    pub const fn cross_lane_service(self) -> AutomaticCrossLaneServiceState {
        self.cross_lane_service
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutomaticSafeModeLane {
    None,
    ActiveColumnarTrial,
    ActivePlannerCalibrationTrial,
    ColumnarMaintenance,
    ChangeStreamReclamation,
    AuthoritativeMaintenance,
    PlannerCalibration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutomaticSafeModeMutation {
    ColumnarAdvance {
        projection_id: ColumnarProjectionId,
    },
    ColumnarCompaction {
        projection_id: ColumnarProjectionId,
        old_generation: ColumnarGeneration,
        new_generation: ColumnarGeneration,
    },
    ColumnarSuppression {
        target: AdaptiveWorkloadTarget,
    },
    ChangeStreamReclamation {
        storage_id: StorageId,
        new_earliest_frontier: StorageDataVersion,
    },
    LsmMaintenance {
        storage_id: StorageId,
        action: crate::AdaptiveLsmMaintenanceAction,
    },
    PlannerCalibrationApply {
        calibration_class: PlannerCalibrationClass,
        applied_epoch: PlannerCalibrationEpoch,
    },
    PlannerCalibrationRevert {
        calibration_class: PlannerCalibrationClass,
        reverted_epoch: PlannerCalibrationEpoch,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutomaticTrialAwaitingReason {
    MissingWorkloadWindow,
    WorkloadTargetMismatch,
    EvidenceSchemaMismatch,
    InsufficientEvidence,
    IncompleteEvidence,
    ArithmeticUnavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutomaticCalibrationTrialStaleReason {
    SchemaChanged,
    CalibrationEpochChanged,
    CalibrationRatioChanged,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutomaticSafeModeNoAction {
    AutomaticActionsDisabled,
    ColumnarInputUnavailable,
    CalibrationInputUnavailable,
    ColumnarNoAction,
    CalibrationAdvisorNoAction(PlannerCalibrationNoAction),
    CalibrationShadowRejected(PlannerCalibrationNoAction),
    ChangeStreamGcNoAction(AdaptiveChangeStreamGcNoActionReason),
    LsmMaintenanceNoAction(AdaptiveLsmMaintenanceNoActionReason),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutomaticSafeModeOutcome {
    NoAction(AutomaticSafeModeNoAction),
    ColumnarMutationCompleted,
    ColumnarCompactionCompleted,
    ColumnarCompactionRevertedInsufficientMeasuredBenefit,
    ColumnarCompactionAborted,
    ColumnarCompactionInconclusive,
    ColumnarTrialValidatedKeep,
    ColumnarTrialReverted,
    ColumnarTrialResolvedSuppressed,
    ColumnarTrialHeld,
    ColumnarTrialAwaiting(AutomaticTrialAwaitingReason),
    ColumnarTrialStale(AdaptiveWorkloadStaleReason),
    PlannerCalibrationApplied,
    PlannerCalibrationTrialValidatedKeep,
    PlannerCalibrationTrialReverted,
    PlannerCalibrationTrialHeld,
    PlannerCalibrationTrialAwaiting(AutomaticTrialAwaitingReason),
    PlannerCalibrationTrialStale(AutomaticCalibrationTrialStaleReason),
    ChangeStreamGcCompleted,
    ChangeStreamGcAborted,
    ChangeStreamGcInconclusive,
    LsmMaintenanceCompleted,
    LsmMaintenanceAborted,
    LsmMaintenanceInconclusive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AutomaticCalibrationTrialEvaluationReport {
    pub calibration_class: PlannerCalibrationClass,
    pub calibration_epoch: PlannerCalibrationEpoch,
    pub sample_count: u64,
    pub total_actual_work_units: u64,
    pub distinct_visibility_points: u64,
    pub distinct_query_shapes: u64,
    pub previous_ratio_error_work_units: u64,
    pub applied_ratio_error_work_units: u64,
    pub overflowed: bool,
    pub incomplete: bool,
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AutomaticSafeModeReport {
    pub trial_before: Option<AutomaticSafeTrial>,
    pub selected_lane: AutomaticSafeModeLane,
    pub mutation: Option<AutomaticSafeModeMutation>,
    pub columnar_cycle: Option<Box<AdaptiveCycleReport>>,
    pub columnar_compaction: Option<Box<AdaptiveColumnarCompactionExecutionReport>>,
    pub change_stream_gc: Option<Box<AdaptiveChangeStreamGcExecutionReport>>,
    pub lsm_maintenance: Option<Box<AdaptiveLsmMaintenanceExecutionReport>>,
    pub workload_evaluation: Option<AdaptiveWorkloadEvaluationReport>,
    pub calibration_decision: Option<PlannerCalibrationDecision>,
    pub calibration_shadow: Option<PlannerCalibrationShadowDecision>,
    pub calibration_trial_evaluation: Option<AutomaticCalibrationTrialEvaluationReport>,
    pub outcome: AutomaticSafeModeOutcome,
    pub trial_after: Option<AutomaticSafeTrial>,
}

#[derive(Debug, Clone, Copy)]
pub struct AutomaticAdmissionScope<'a> {
    pub table_ids: &'a [TableId],
    pub calibration_classes: &'a [PlannerCalibrationClass],
}

#[derive(Debug, Clone, Copy)]
pub struct AutomaticMultiSafeModeInput<'a> {
    pub scope: AutomaticAdmissionScope<'a>,
    pub maintenance_budget: MaintenanceBudget,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AutomaticMultiSafeModePolicy {
    pub safe_mode: AutomaticSafeModePolicy,
    pub allow_columnar_compaction: bool,
    pub allow_change_stream_gc: bool,
    pub allow_lsm_flush: bool,
    pub allow_lsm_compaction: bool,
    pub change_stream_gc_policy: AdaptiveChangeStreamGcPolicy,
    pub lsm_flush_policy: AdaptiveLsmFlushPolicy,
    pub lsm_compaction_policy: AdaptiveLsmCompactionPolicy,
    pub columnar_compaction_policy: AdaptiveColumnarCompactionPolicy,
    pub cross_lane_service: AutomaticCrossLaneServicePolicy,
    pub max_candidate_tables: u64,
    pub max_calibration_classes: u64,
    pub max_fairness_entries: u64,
}

impl Default for AutomaticMultiSafeModePolicy {
    fn default() -> Self {
        Self {
            safe_mode: AutomaticSafeModePolicy::default(),
            allow_columnar_compaction: false,
            allow_change_stream_gc: false,
            allow_lsm_flush: false,
            allow_lsm_compaction: false,
            change_stream_gc_policy: AdaptiveChangeStreamGcPolicy::default(),
            lsm_flush_policy: AdaptiveLsmFlushPolicy::default(),
            lsm_compaction_policy: AdaptiveLsmCompactionPolicy::default(),
            columnar_compaction_policy: AdaptiveColumnarCompactionPolicy::default(),
            cross_lane_service: AutomaticCrossLaneServicePolicy::StrictPhysicalPriority,
            max_candidate_tables: 16,
            max_calibration_classes: 4,
            max_fairness_entries: 64,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum AutomaticCrossLaneServicePolicy {
    #[default]
    StrictPhysicalPriority,
    BoundedColumnarBurst {
        max_consecutive_columnar_admissions: u64,
    },
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AutomaticCrossLaneServiceState {
    pub consecutive_columnar_admissions: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutomaticLaneSelectionReason {
    ActiveTrial,
    StrictPhysicalPriority,
    ColumnarBurstAvailable,
    CalibrationServiceDue,
    OnlyColumnarReady,
    OnlyCalibrationReady,
    OnlyReclamationReady,
    OnlyAuthoritativeMaintenanceReady,
    ReclamationPriority,
    AuthoritativeMaintenancePriority,
    NoReadyCandidates,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum AutomaticCandidateKey {
    Columnar {
        table_id: TableId,
        projection_id: Option<ColumnarProjectionId>,
    },
    ColumnarCompaction {
        table_id: TableId,
        projection_id: ColumnarProjectionId,
    },
    PlannerCalibration {
        calibration_class: PlannerCalibrationClass,
    },
    ChangeStreamGc {
        table_id: TableId,
        storage_id: StorageId,
    },
    LsmFlush {
        table_id: TableId,
        storage_id: StorageId,
    },
    LsmCompaction {
        table_id: TableId,
        storage_id: StorageId,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutomaticCandidateReadiness {
    Ready,
    ColumnarBlocked(AdaptiveNoActionReason),
    ColumnarCompactionBlocked(AdaptiveColumnarCompactionNoActionReason),
    CalibrationBlocked(PlannerCalibrationNoAction),
    CalibrationEvidenceUnavailable,
    ChangeStreamGcBlocked(AdaptiveChangeStreamGcNoActionReason),
    LsmMaintenanceBlocked(AdaptiveLsmMaintenanceNoActionReason),
    StaleEvidence,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AutomaticCandidateRankEvidence {
    pub ready_age: u64,
    pub expected_benefit_work_units: Option<u64>,
    pub maintenance_work_units: Option<u64>,
    pub read_bytes: Option<u64>,
    pub write_bytes: Option<u64>,
    pub delta_segment_count: Option<u64>,
    pub delta_bytes: Option<u64>,
    pub delta_mutations: Option<u64>,
    pub suppressed_versions: Option<u64>,
    pub shadow_improvement_work_units: Option<u64>,
    pub reclaimable_batches: Option<u64>,
    pub reclaimable_mutations: Option<u64>,
    pub reclaimable_bytes: Option<u64>,
    pub safe_reclaim_frontier: Option<StorageDataVersion>,
    pub lsm_memtable_entries: Option<u64>,
    pub lsm_memtable_bytes: Option<u64>,
    pub lsm_flush_threshold_bytes: Option<u64>,
    pub lsm_compaction_input_bytes: Option<u64>,
    pub lsm_compaction_work_units: Option<u64>,
    pub distinct_query_shapes: Option<u64>,
    pub sample_count: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AutomaticCandidateInspection {
    pub key: AutomaticCandidateKey,
    pub lane: AutomaticSafeModeLane,
    pub readiness: AutomaticCandidateReadiness,
    pub rank: AutomaticCandidateRankEvidence,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AutomaticCandidateInspectionReport {
    pub candidates: Vec<AutomaticCandidateInspection>,
    pub blocked_by_active_trial: Option<AutomaticSafeTrial>,
    pub cross_lane_service_state: AutomaticCrossLaneServiceState,
    pub preferred_lane: AutomaticSafeModeLane,
    pub lane_selection_reason: AutomaticLaneSelectionReason,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AutomaticMultiSafeModeReport {
    pub candidates: Vec<AutomaticCandidateInspection>,
    pub selected_candidate: Option<AutomaticCandidateKey>,
    pub evidence_window_epoch: crate::AdaptiveEvidenceWindowEpoch,
    pub cross_lane_service_before: AutomaticCrossLaneServiceState,
    pub cross_lane_service_after: AutomaticCrossLaneServiceState,
    pub lane_selection_reason: AutomaticLaneSelectionReason,
    pub action: AutomaticSafeModeReport,
}

#[derive(Debug)]
pub enum AutomaticSafeModeError {
    Adaptive(AdaptiveError),
    ColumnarCompaction(AdaptiveColumnarCompactionError),
    LsmMaintenance(AdaptiveLsmMaintenanceError),
    CalibrationAdvisor(PlannerCalibrationAdvisorError),
    CalibrationMutation(PlannerCalibrationMutationError),
    MissingColumnarMeasurement,
    AdmissionScopeTooLarge,
    InvalidCrossLaneServicePolicy,
    InvalidColumnarCompactionPolicy,
    InvalidChangeStreamGcPolicy,
    Database(DatabaseError),
}

impl fmt::Display for AutomaticSafeModeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Adaptive(error) => error.fmt(formatter),
            Self::ColumnarCompaction(error) => error.fmt(formatter),
            Self::LsmMaintenance(error) => error.fmt(formatter),
            Self::CalibrationAdvisor(error) => error.fmt(formatter),
            Self::CalibrationMutation(error) => error.fmt(formatter),
            Self::MissingColumnarMeasurement => formatter.write_str(
                "a kept automatic Columnar mutation did not return its measured target state",
            ),
            Self::AdmissionScopeTooLarge => {
                formatter.write_str("automatic admission scope exceeds the configured bound")
            }
            Self::InvalidCrossLaneServicePolicy => formatter
                .write_str("bounded cross-lane service requires at least one Columnar admission"),
            Self::InvalidColumnarCompactionPolicy => formatter.write_str(
                "automatic Columnar compaction requires at least one non-zero pressure threshold",
            ),
            Self::InvalidChangeStreamGcPolicy => formatter.write_str(
                "automatic change-stream GC requires a non-zero batch or byte threshold",
            ),
            Self::Database(error) => error.fmt(formatter),
        }
    }
}

impl Error for AutomaticSafeModeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Adaptive(error) => Some(error),
            Self::ColumnarCompaction(error) => Some(error),
            Self::LsmMaintenance(error) => Some(error),
            Self::CalibrationAdvisor(error) => Some(error),
            Self::CalibrationMutation(error) => Some(error),
            Self::Database(error) => Some(error),
            Self::MissingColumnarMeasurement
            | Self::AdmissionScopeTooLarge
            | Self::InvalidCrossLaneServicePolicy
            | Self::InvalidColumnarCompactionPolicy
            | Self::InvalidChangeStreamGcPolicy => None,
        }
    }
}

impl From<AdaptiveError> for AutomaticSafeModeError {
    fn from(error: AdaptiveError) -> Self {
        Self::Adaptive(error)
    }
}

impl From<AdaptiveColumnarCompactionError> for AutomaticSafeModeError {
    fn from(error: AdaptiveColumnarCompactionError) -> Self {
        Self::ColumnarCompaction(error)
    }
}

impl From<AdaptiveLsmMaintenanceError> for AutomaticSafeModeError {
    fn from(error: AdaptiveLsmMaintenanceError) -> Self {
        Self::LsmMaintenance(error)
    }
}

impl From<PlannerCalibrationAdvisorError> for AutomaticSafeModeError {
    fn from(error: PlannerCalibrationAdvisorError) -> Self {
        Self::CalibrationAdvisor(error)
    }
}

impl From<PlannerCalibrationMutationError> for AutomaticSafeModeError {
    fn from(error: PlannerCalibrationMutationError) -> Self {
        Self::CalibrationMutation(error)
    }
}

impl From<DatabaseError> for AutomaticSafeModeError {
    fn from(error: DatabaseError) -> Self {
        Self::Database(error)
    }
}

#[derive(Debug, Clone, Copy)]
enum ActiveAutomaticTrial {
    Columnar {
        target: AdaptiveWorkloadTarget,
    },
    PlannerCalibration {
        receipt: PlannerCalibrationReceipt,
        schema_generation: SchemaGeneration,
    },
}

impl ActiveAutomaticTrial {
    fn summary(self) -> AutomaticSafeTrial {
        match self {
            Self::Columnar { target } => {
                AutomaticSafeTrial::Columnar(AutomaticColumnarTrial { target })
            }
            Self::PlannerCalibration {
                receipt,
                schema_generation,
            } => AutomaticSafeTrial::PlannerCalibration(AutomaticPlannerCalibrationTrial {
                calibration_class: receipt.calibration_class(),
                schema_generation,
                previous_epoch: receipt.previous_epoch(),
                applied_epoch: receipt.applied_epoch(),
                previous_ratio: receipt.previous_ratio(),
                applied_ratio: receipt.applied_ratio(),
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CandidateAdmissionEntry {
    key: AutomaticCandidateKey,
    ready_age: u64,
}

#[derive(Debug, Default)]
pub(crate) struct AutomaticSafeModeRuntimeState {
    active_trial: Option<ActiveAutomaticTrial>,
    admission: Vec<CandidateAdmissionEntry>,
    cross_lane_service: AutomaticCrossLaneServiceState,
}

#[derive(Debug, Clone)]
enum AutomaticCandidateAuthority {
    Columnar(Box<ColumnarCandidateAuthority>),
    ColumnarCompaction(Box<ColumnarCompactionCandidateAuthority>),
    PlannerCalibration(Box<CalibrationCandidateAuthority>),
    ChangeStreamGc(Box<ChangeStreamGcCandidateAuthority>),
    LsmMaintenance(Box<LsmMaintenanceCandidateAuthority>),
}

#[derive(Debug, Clone)]
struct ColumnarCandidateAuthority {
    observation: crate::AdaptiveObservation,
    proposal: crate::AdaptiveMaintenanceProposal,
}

#[derive(Debug, Clone)]
struct ColumnarCompactionCandidateAuthority {
    proposal: AdaptiveColumnarCompactionProposal,
}

#[derive(Debug, Clone)]
struct CalibrationCandidateAuthority {
    decision: PlannerCalibrationDecision,
    proposal: PlannerCalibrationProposal,
    shadow: PlannerCalibrationShadowDecision,
    accepted_shadow: PlannerCalibrationShadowReport,
}

#[derive(Debug, Clone)]
struct ChangeStreamGcCandidateAuthority {
    proposal: AdaptiveChangeStreamGcProposal,
}

#[derive(Debug, Clone)]
struct LsmMaintenanceCandidateAuthority {
    proposal: AdaptiveLsmMaintenanceProposal,
}

#[derive(Debug, Clone)]
struct DiscoveredAutomaticCandidate {
    inspection: AutomaticCandidateInspection,
    authority: Option<AutomaticCandidateAuthority>,
}

impl Database {
    #[must_use]
    pub fn automatic_safe_mode_state(&self) -> AutomaticSafeModeState {
        AutomaticSafeModeState {
            active_trial: self
                .automatic_safe_mode
                .active_trial
                .map(ActiveAutomaticTrial::summary),
            cross_lane_service: self.automatic_safe_mode.cross_lane_service,
        }
    }

    /// Ends probation without changing projection eligibility, calibration, or data.
    pub fn abandon_automatic_safe_trial(&mut self) -> Option<AutomaticSafeTrial> {
        self.automatic_safe_mode
            .active_trial
            .take()
            .map(ActiveAutomaticTrial::summary)
    }

    /// Discovers every candidate in the explicit operator scope without
    /// changing evidence, fairness age, trial state, G, or physical state.
    pub fn inspect_automatic_candidates(
        &self,
        pool: &AdaptiveEvidencePool,
        input: AutomaticMultiSafeModeInput<'_>,
        policy: AutomaticMultiSafeModePolicy,
    ) -> Result<AutomaticCandidateInspectionReport, AutomaticSafeModeError> {
        validate_multi_scope(input.scope, policy)?;
        let service_state = self.automatic_safe_mode.cross_lane_service;
        if let Some(active) = self.automatic_safe_mode_state().active_trial() {
            let preferred_lane = match active {
                AutomaticSafeTrial::Columnar(_) => AutomaticSafeModeLane::ActiveColumnarTrial,
                AutomaticSafeTrial::PlannerCalibration(_) => {
                    AutomaticSafeModeLane::ActivePlannerCalibrationTrial
                }
            };
            return Ok(AutomaticCandidateInspectionReport {
                candidates: Vec::new(),
                blocked_by_active_trial: Some(active),
                cross_lane_service_state: service_state,
                preferred_lane,
                lane_selection_reason: AutomaticLaneSelectionReason::ActiveTrial,
            });
        }
        let columnar = self.discover_columnar_candidates(
            input.scope.table_ids,
            input.maintenance_budget,
            policy,
        )?;
        let reclamation = self.discover_change_stream_gc_candidates(
            input.scope.table_ids,
            input.maintenance_budget,
            policy,
        )?;
        let authoritative = self.discover_lsm_maintenance_candidates(
            input.scope.table_ids,
            input.maintenance_budget,
            policy,
        )?;
        validate_candidate_count(
            columnar.len(),
            reclamation.len(),
            authoritative.len(),
            0,
            policy,
        )?;
        let calibration = self.discover_calibration_candidates(
            pool,
            input.scope.calibration_classes,
            policy.safe_mode,
        )?;
        validate_candidate_count(
            columnar.len(),
            reclamation.len(),
            authoritative.len(),
            calibration.len(),
            policy,
        )?;
        let (preferred_lane, lane_selection_reason) = select_ready_lane(
            &columnar,
            &reclamation,
            &authoritative,
            &calibration,
            policy.cross_lane_service,
            service_state,
        );
        Ok(AutomaticCandidateInspectionReport {
            candidates: columnar
                .into_iter()
                .chain(reclamation)
                .chain(authoritative)
                .chain(calibration)
                .map(|candidate| candidate.inspection)
                .collect(),
            blocked_by_active_trial: None,
            cross_lane_service_state: service_state,
            preferred_lane,
            lane_selection_reason,
        })
    }

    /// Performs one multi-target admission round. Active probation owns the
    /// call; otherwise exactly one ready candidate can reach an existing
    /// Phase 1 or Phase 4 mutation authority.
    pub fn automatic_safe_step_multi(
        &mut self,
        pool: &AdaptiveEvidencePool,
        input: AutomaticMultiSafeModeInput<'_>,
        policy: AutomaticMultiSafeModePolicy,
    ) -> Result<AutomaticMultiSafeModeReport, AutomaticSafeModeError> {
        validate_multi_scope(input.scope, policy)?;
        let trial_before = self.automatic_safe_mode_state().active_trial();
        let service_before = self.automatic_safe_mode.cross_lane_service;
        if let Some(trial) = self.automatic_safe_mode.active_trial {
            let action = match trial {
                ActiveAutomaticTrial::Columnar { target } => {
                    if let Some(window) = pool.target_window(target) {
                        self.evaluate_automatic_columnar_trial(
                            target,
                            Some(window),
                            policy.safe_mode.workload_policy,
                            trial_before,
                        )?
                    } else if let Some(reason) = self.adaptive_workload_stale_reason(target) {
                        self.automatic_safe_mode.active_trial = None;
                        self.finish_automatic_report(
                            trial_before,
                            AutomaticSafeModeLane::ActiveColumnarTrial,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            AutomaticSafeModeOutcome::ColumnarTrialStale(reason),
                        )
                    } else {
                        self.finish_automatic_report(
                            trial_before,
                            AutomaticSafeModeLane::ActiveColumnarTrial,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            AutomaticSafeModeOutcome::ColumnarTrialAwaiting(
                                AutomaticTrialAwaitingReason::MissingWorkloadWindow,
                            ),
                        )
                    }
                }
                ActiveAutomaticTrial::PlannerCalibration {
                    receipt,
                    schema_generation,
                } => {
                    let evidence = pool.calibration_evidence(
                        receipt.calibration_class(),
                        receipt.applied_epoch(),
                        0,
                    );
                    let schema_matches = pool
                        .schema_generation()
                        .is_none_or(|schema| schema == schema_generation);
                    self.evaluate_automatic_calibration_trial_evidence(
                        receipt,
                        schema_generation,
                        evidence,
                        schema_matches,
                        policy.safe_mode.calibration_trial_policy,
                        trial_before,
                    )?
                }
            };
            return Ok(AutomaticMultiSafeModeReport {
                candidates: Vec::new(),
                selected_candidate: None,
                evidence_window_epoch: pool.window_epoch(),
                cross_lane_service_before: service_before,
                cross_lane_service_after: self.automatic_safe_mode.cross_lane_service,
                lane_selection_reason: AutomaticLaneSelectionReason::ActiveTrial,
                action,
            });
        }

        let mut columnar = self.discover_columnar_candidates(
            input.scope.table_ids,
            input.maintenance_budget,
            policy,
        )?;
        let mut reclamation = self.discover_change_stream_gc_candidates(
            input.scope.table_ids,
            input.maintenance_budget,
            policy,
        )?;
        let mut authoritative = self.discover_lsm_maintenance_candidates(
            input.scope.table_ids,
            input.maintenance_budget,
            policy,
        )?;
        validate_candidate_count(
            columnar.len(),
            reclamation.len(),
            authoritative.len(),
            0,
            policy,
        )?;
        let mut calibration = self.discover_calibration_candidates(
            pool,
            input.scope.calibration_classes,
            policy.safe_mode,
        )?;
        validate_candidate_count(
            columnar.len(),
            reclamation.len(),
            authoritative.len(),
            calibration.len(),
            policy,
        )?;
        let mut inspections = columnar
            .iter()
            .map(|candidate| candidate.inspection)
            .collect::<Vec<_>>();
        inspections.extend(reclamation.iter().map(|candidate| candidate.inspection));
        inspections.extend(authoritative.iter().map(|candidate| candidate.inspection));
        inspections.extend(calibration.iter().map(|candidate| candidate.inspection));
        let (selected_lane, lane_selection_reason) = select_ready_lane(
            &columnar,
            &reclamation,
            &authoritative,
            &calibration,
            policy.cross_lane_service,
            service_before,
        );
        let selected = match selected_lane {
            AutomaticSafeModeLane::ColumnarMaintenance => {
                select_columnar_candidate(&columnar).map(|index| columnar.remove(index))
            }
            AutomaticSafeModeLane::ChangeStreamReclamation => {
                select_reclamation_candidate(&reclamation).map(|index| reclamation.remove(index))
            }
            AutomaticSafeModeLane::AuthoritativeMaintenance => {
                select_lsm_candidate(&authoritative).map(|index| authoritative.remove(index))
            }
            AutomaticSafeModeLane::PlannerCalibration => {
                select_calibration_candidate(&calibration).map(|index| calibration.remove(index))
            }
            _ => None,
        };
        if let Some(selected) = selected {
            let key = selected.inspection.key;
            self.advance_fairness(&inspections, key, policy.max_fairness_entries);
            self.record_cross_lane_admission(selected_lane);
            let action =
                self.execute_multi_candidate(selected, input.maintenance_budget, trial_before)?;
            return Ok(AutomaticMultiSafeModeReport {
                candidates: inspections,
                selected_candidate: Some(key),
                evidence_window_epoch: pool.window_epoch(),
                cross_lane_service_before: service_before,
                cross_lane_service_after: self.automatic_safe_mode.cross_lane_service,
                lane_selection_reason,
                action,
            });
        }

        self.automatic_safe_mode.admission.clear();
        let no_action = if !policy.safe_mode.allow_columnar_maintenance
            && !policy.allow_columnar_compaction
            && !policy.allow_change_stream_gc
            && !policy.allow_lsm_flush
            && !policy.allow_lsm_compaction
            && !policy.safe_mode.allow_planner_calibration
        {
            AutomaticSafeModeNoAction::AutomaticActionsDisabled
        } else {
            AutomaticSafeModeNoAction::ColumnarNoAction
        };
        Ok(AutomaticMultiSafeModeReport {
            candidates: inspections,
            selected_candidate: None,
            evidence_window_epoch: pool.window_epoch(),
            cross_lane_service_before: service_before,
            cross_lane_service_after: self.automatic_safe_mode.cross_lane_service,
            lane_selection_reason,
            action: self.finish_automatic_report(
                trial_before,
                AutomaticSafeModeLane::None,
                None,
                None,
                None,
                None,
                None,
                None,
                AutomaticSafeModeOutcome::NoAction(no_action),
            ),
        })
    }

    /// Performs one explicit synchronous safe-mode control step. A live trial
    /// owns the step, and every path performs at most one control mutation.
    pub fn automatic_safe_step(
        &mut self,
        input: AutomaticSafeModeInput<'_>,
        policy: AutomaticSafeModePolicy,
    ) -> Result<AutomaticSafeModeReport, AutomaticSafeModeError> {
        let trial_before = self.automatic_safe_mode_state().active_trial();
        if let Some(trial) = self.automatic_safe_mode.active_trial {
            return match trial {
                ActiveAutomaticTrial::Columnar { target } => self
                    .evaluate_automatic_columnar_trial(
                        target,
                        input.workload_window,
                        policy.workload_policy,
                        trial_before,
                    ),
                ActiveAutomaticTrial::PlannerCalibration {
                    receipt,
                    schema_generation,
                } => self.evaluate_automatic_calibration_trial(
                    receipt,
                    schema_generation,
                    input.workload_window,
                    policy.calibration_trial_policy,
                    trial_before,
                ),
            };
        }

        let mut columnar_cycle = None;
        let mut no_action = AutomaticSafeModeNoAction::AutomaticActionsDisabled;
        if policy.allow_columnar_maintenance {
            if let Some(table_id) = input.columnar_table_id {
                let cycle = self.adaptive_columnar_step(
                    table_id,
                    policy.adaptive_policy,
                    input.maintenance_budget,
                )?;
                if matches!(cycle.decision, AdaptiveDecision::Proposal(_)) {
                    let mutation = cycle.execution.as_ref().and_then(|execution| {
                        (execution.consumed.actions != 0).then_some(
                            AutomaticSafeModeMutation::ColumnarAdvance {
                                projection_id: execution.proposal.projection_id,
                            },
                        )
                    });
                    if cycle.execution.as_ref().is_some_and(|execution| {
                        execution.outcome == AdaptiveMaintenanceOutcome::Kept
                    }) {
                        let execution = cycle
                            .execution
                            .as_ref()
                            .ok_or(AutomaticSafeModeError::MissingColumnarMeasurement)?;
                        let measurement = execution
                            .measurement
                            .as_ref()
                            .ok_or(AutomaticSafeModeError::MissingColumnarMeasurement)?;
                        self.automatic_safe_mode.active_trial =
                            Some(ActiveAutomaticTrial::Columnar {
                                target: AdaptiveWorkloadTarget {
                                    table_id: execution.proposal.table_id,
                                    storage_id: execution.proposal.storage_id,
                                    projection_id: execution.proposal.projection_id,
                                    generation: measurement.after_outcome.projection_generation,
                                    schema_generation: measurement.schema_generation_after,
                                },
                            });
                    }
                    return Ok(self.finish_automatic_report(
                        trial_before,
                        AutomaticSafeModeLane::ColumnarMaintenance,
                        mutation,
                        Some(Box::new(cycle)),
                        None,
                        None,
                        None,
                        None,
                        AutomaticSafeModeOutcome::ColumnarMutationCompleted,
                    ));
                }
                no_action = AutomaticSafeModeNoAction::ColumnarNoAction;
                columnar_cycle = Some(Box::new(cycle));
            } else {
                no_action = AutomaticSafeModeNoAction::ColumnarInputUnavailable;
            }
        }

        if policy.allow_planner_calibration {
            let (Some(window), Some(class)) = (input.workload_window, input.calibration_class)
            else {
                return Ok(self.finish_automatic_report(
                    trial_before,
                    AutomaticSafeModeLane::None,
                    None,
                    columnar_cycle,
                    None,
                    None,
                    None,
                    None,
                    AutomaticSafeModeOutcome::NoAction(
                        AutomaticSafeModeNoAction::CalibrationInputUnavailable,
                    ),
                ));
            };
            let decision =
                self.advise_planner_calibration(window, class, policy.planner_calibration_policy)?;
            let proposal = match &decision {
                PlannerCalibrationDecision::NoAction(reason) => {
                    return Ok(self.finish_automatic_report(
                        trial_before,
                        AutomaticSafeModeLane::PlannerCalibration,
                        None,
                        columnar_cycle,
                        None,
                        Some(decision.clone()),
                        None,
                        None,
                        AutomaticSafeModeOutcome::NoAction(
                            AutomaticSafeModeNoAction::CalibrationAdvisorNoAction(*reason),
                        ),
                    ));
                }
                PlannerCalibrationDecision::Proposal(proposal) => proposal,
            };
            let shadow = self.shadow_planner_calibration(proposal);
            let shadow_report = match &shadow {
                PlannerCalibrationShadowDecision::Accepted(report) => report,
                PlannerCalibrationShadowDecision::Rejected { reason, .. } => {
                    return Ok(self.finish_automatic_report(
                        trial_before,
                        AutomaticSafeModeLane::PlannerCalibration,
                        None,
                        columnar_cycle,
                        None,
                        Some(decision.clone()),
                        Some(shadow.clone()),
                        None,
                        AutomaticSafeModeOutcome::NoAction(
                            AutomaticSafeModeNoAction::CalibrationShadowRejected(*reason),
                        ),
                    ));
                }
            };
            let schema_generation = self.schema_generation();
            let receipt = self.apply_planner_calibration(proposal, shadow_report)?;
            self.automatic_safe_mode.active_trial =
                Some(ActiveAutomaticTrial::PlannerCalibration {
                    receipt,
                    schema_generation,
                });
            return Ok(self.finish_automatic_report(
                trial_before,
                AutomaticSafeModeLane::PlannerCalibration,
                Some(AutomaticSafeModeMutation::PlannerCalibrationApply {
                    calibration_class: receipt.calibration_class(),
                    applied_epoch: receipt.applied_epoch(),
                }),
                columnar_cycle,
                None,
                Some(decision),
                Some(shadow),
                None,
                AutomaticSafeModeOutcome::PlannerCalibrationApplied,
            ));
        }

        Ok(self.finish_automatic_report(
            trial_before,
            AutomaticSafeModeLane::None,
            None,
            columnar_cycle,
            None,
            None,
            None,
            None,
            AutomaticSafeModeOutcome::NoAction(no_action),
        ))
    }

    fn discover_columnar_candidates(
        &self,
        table_ids: &[TableId],
        budget: MaintenanceBudget,
        policy: AutomaticMultiSafeModePolicy,
    ) -> Result<Vec<DiscoveredAutomaticCandidate>, AutomaticSafeModeError> {
        if !policy.safe_mode.allow_columnar_maintenance && !policy.allow_columnar_compaction {
            return Ok(Vec::new());
        }
        let mut output = Vec::new();
        for table_id in stable_unique_tables(table_ids) {
            if policy.safe_mode.allow_columnar_maintenance {
                let observation = self.observe_adaptive_columnar(table_id)?;
                for decision in observation.decisions(policy.safe_mode.adaptive_policy, budget) {
                    match decision {
                        AdaptiveDecision::Proposal(proposal) => {
                            let key = AutomaticCandidateKey::Columnar {
                                table_id,
                                projection_id: Some(proposal.projection_id),
                            };
                            output.push(DiscoveredAutomaticCandidate {
                                inspection: AutomaticCandidateInspection {
                                    key,
                                    lane: AutomaticSafeModeLane::ColumnarMaintenance,
                                    readiness: AutomaticCandidateReadiness::Ready,
                                    rank: AutomaticCandidateRankEvidence {
                                        ready_age: self.ready_age(key),
                                        expected_benefit_work_units: Some(
                                            proposal.expected_planner.benefit_work_units,
                                        ),
                                        maintenance_work_units: Some(
                                            proposal.estimated_cost.work_units,
                                        ),
                                        read_bytes: Some(proposal.estimated_cost.read_bytes),
                                        write_bytes: Some(proposal.estimated_cost.write_bytes),
                                        ..AutomaticCandidateRankEvidence::default()
                                    },
                                },
                                authority: Some(AutomaticCandidateAuthority::Columnar(Box::new(
                                    ColumnarCandidateAuthority {
                                        observation: observation.clone(),
                                        proposal,
                                    },
                                ))),
                            });
                        }
                        AdaptiveDecision::NoAction(no_action) => {
                            let key = AutomaticCandidateKey::Columnar {
                                table_id,
                                projection_id: no_action.projection_id,
                            };
                            output.push(DiscoveredAutomaticCandidate {
                                inspection: AutomaticCandidateInspection {
                                    key,
                                    lane: AutomaticSafeModeLane::ColumnarMaintenance,
                                    readiness: AutomaticCandidateReadiness::ColumnarBlocked(
                                        no_action.reason,
                                    ),
                                    rank: AutomaticCandidateRankEvidence::default(),
                                },
                                authority: None,
                            });
                        }
                    }
                }
            }
            if policy.allow_columnar_compaction {
                for observation in self.observe_adaptive_columnar_compactions(table_id, budget)? {
                    let MaintenanceAction::CompactColumnar { projection_id } =
                        observation.maintenance_candidate.action
                    else {
                        continue;
                    };
                    let key = AutomaticCandidateKey::ColumnarCompaction {
                        table_id,
                        projection_id,
                    };
                    let mut rank =
                        compaction_rank_evidence(self.ready_age(key), projection_id, &observation);
                    match observation.decide(
                        policy.columnar_compaction_policy,
                        policy.safe_mode.adaptive_policy,
                    )? {
                        AdaptiveColumnarCompactionDecision::Proposal(proposal) => {
                            rank.expected_benefit_work_units =
                                Some(proposal.expected_planner.benefit_work_units);
                            output.push(DiscoveredAutomaticCandidate {
                                inspection: AutomaticCandidateInspection {
                                    key,
                                    lane: AutomaticSafeModeLane::ColumnarMaintenance,
                                    readiness: AutomaticCandidateReadiness::Ready,
                                    rank,
                                },
                                authority: Some(AutomaticCandidateAuthority::ColumnarCompaction(
                                    Box::new(ColumnarCompactionCandidateAuthority { proposal }),
                                )),
                            });
                        }
                        AdaptiveColumnarCompactionDecision::NoAction(no_action) => {
                            output.push(DiscoveredAutomaticCandidate {
                                inspection: AutomaticCandidateInspection {
                                    key,
                                    lane: AutomaticSafeModeLane::ColumnarMaintenance,
                                    readiness:
                                        AutomaticCandidateReadiness::ColumnarCompactionBlocked(
                                            no_action.reason,
                                        ),
                                    rank,
                                },
                                authority: None,
                            });
                        }
                    }
                }
            }
        }
        Ok(output)
    }

    fn discover_calibration_candidates(
        &self,
        pool: &AdaptiveEvidencePool,
        classes: &[PlannerCalibrationClass],
        policy: AutomaticSafeModePolicy,
    ) -> Result<Vec<DiscoveredAutomaticCandidate>, AutomaticSafeModeError> {
        if !policy.allow_planner_calibration {
            return Ok(Vec::new());
        }
        let mut output = Vec::new();
        for class in stable_unique_classes(classes) {
            let key = AutomaticCandidateKey::PlannerCalibration {
                calibration_class: class,
            };
            let Some(evidence) = pool.calibration_evidence(
                class,
                self.planner_calibration.epoch,
                policy.planner_calibration_policy.error_deadband_work_units,
            ) else {
                output.push(DiscoveredAutomaticCandidate {
                    inspection: AutomaticCandidateInspection {
                        key,
                        lane: AutomaticSafeModeLane::PlannerCalibration,
                        readiness: AutomaticCandidateReadiness::CalibrationEvidenceUnavailable,
                        rank: AutomaticCandidateRankEvidence::default(),
                    },
                    authority: None,
                });
                continue;
            };
            let decision = match self
                .advise_planner_calibration_evidence(evidence, policy.planner_calibration_policy)
            {
                Ok(decision) => decision,
                Err(PlannerCalibrationAdvisorError::SchemaChanged { .. }) => {
                    output.push(DiscoveredAutomaticCandidate {
                        inspection: AutomaticCandidateInspection {
                            key,
                            lane: AutomaticSafeModeLane::PlannerCalibration,
                            readiness: AutomaticCandidateReadiness::StaleEvidence,
                            rank: AutomaticCandidateRankEvidence::default(),
                        },
                        authority: None,
                    });
                    continue;
                }
                Err(error) => return Err(error.into()),
            };
            let proposal = match &decision {
                PlannerCalibrationDecision::NoAction(reason) => {
                    output.push(DiscoveredAutomaticCandidate {
                        inspection: AutomaticCandidateInspection {
                            key,
                            lane: AutomaticSafeModeLane::PlannerCalibration,
                            readiness: AutomaticCandidateReadiness::CalibrationBlocked(*reason),
                            rank: AutomaticCandidateRankEvidence::default(),
                        },
                        authority: None,
                    });
                    continue;
                }
                PlannerCalibrationDecision::Proposal(proposal) => proposal.as_ref().clone(),
            };
            let shadow = self.shadow_planner_calibration(&proposal);
            let accepted_shadow = match &shadow {
                PlannerCalibrationShadowDecision::Accepted(report) => report.clone(),
                PlannerCalibrationShadowDecision::Rejected { reason, .. } => {
                    output.push(DiscoveredAutomaticCandidate {
                        inspection: AutomaticCandidateInspection {
                            key,
                            lane: AutomaticSafeModeLane::PlannerCalibration,
                            readiness: AutomaticCandidateReadiness::CalibrationBlocked(*reason),
                            rank: AutomaticCandidateRankEvidence::default(),
                        },
                        authority: None,
                    });
                    continue;
                }
            };
            let evidence = proposal.evidence();
            output.push(DiscoveredAutomaticCandidate {
                inspection: AutomaticCandidateInspection {
                    key,
                    lane: AutomaticSafeModeLane::PlannerCalibration,
                    readiness: AutomaticCandidateReadiness::Ready,
                    rank: AutomaticCandidateRankEvidence {
                        ready_age: self.ready_age(key),
                        shadow_improvement_work_units: Some(
                            accepted_shadow.improvement_work_units(),
                        ),
                        distinct_query_shapes: Some(evidence.distinct_query_shapes),
                        sample_count: Some(evidence.sample_count),
                        ..AutomaticCandidateRankEvidence::default()
                    },
                },
                authority: Some(AutomaticCandidateAuthority::PlannerCalibration(Box::new(
                    CalibrationCandidateAuthority {
                        decision,
                        proposal,
                        shadow,
                        accepted_shadow,
                    },
                ))),
            });
        }
        Ok(output)
    }

    fn discover_change_stream_gc_candidates(
        &self,
        table_ids: &[TableId],
        budget: MaintenanceBudget,
        policy: AutomaticMultiSafeModePolicy,
    ) -> Result<Vec<DiscoveredAutomaticCandidate>, AutomaticSafeModeError> {
        if !policy.allow_change_stream_gc {
            return Ok(Vec::new());
        }
        let mut output = Vec::new();
        for table_id in stable_unique_tables(table_ids) {
            let observation = self.observe_change_stream_reclamation(table_id)?;
            let key = AutomaticCandidateKey::ChangeStreamGc {
                table_id,
                storage_id: observation.storage_id,
            };
            let mut rank = AutomaticCandidateRankEvidence {
                ready_age: self.ready_age(key),
                safe_reclaim_frontier: observation.safe_reclaim_through.map(|safe| safe.frontier()),
                ..AutomaticCandidateRankEvidence::default()
            };
            if let Some(prefix) = observation.reclaimable_prefix {
                rank.reclaimable_batches = Some(prefix.batches);
                rank.reclaimable_mutations = Some(prefix.mutations);
                rank.reclaimable_bytes = Some(prefix.reclaimed_file_bytes);
                rank.maintenance_work_units = Some(prefix.rewrite_work_units);
                rank.read_bytes = Some(observation.file_bytes);
                rank.write_bytes = Some(prefix.rewrite_bytes);
            }
            match self.advise_change_stream_reclamation(
                &observation,
                policy.change_stream_gc_policy,
                budget,
            ) {
                Ok(AdaptiveChangeStreamGcDecision::Proposal(proposal)) => {
                    output.push(DiscoveredAutomaticCandidate {
                        inspection: AutomaticCandidateInspection {
                            key,
                            lane: AutomaticSafeModeLane::ChangeStreamReclamation,
                            readiness: AutomaticCandidateReadiness::Ready,
                            rank,
                        },
                        authority: Some(AutomaticCandidateAuthority::ChangeStreamGc(Box::new(
                            ChangeStreamGcCandidateAuthority {
                                proposal: *proposal,
                            },
                        ))),
                    });
                }
                Ok(AdaptiveChangeStreamGcDecision::NoAction(reason)) => {
                    output.push(DiscoveredAutomaticCandidate {
                        inspection: AutomaticCandidateInspection {
                            key,
                            lane: AutomaticSafeModeLane::ChangeStreamReclamation,
                            readiness: AutomaticCandidateReadiness::ChangeStreamGcBlocked(reason),
                            rank,
                        },
                        authority: None,
                    });
                }
                Err(_) => return Err(AutomaticSafeModeError::InvalidChangeStreamGcPolicy),
            }
        }
        Ok(output)
    }

    fn discover_lsm_maintenance_candidates(
        &self,
        table_ids: &[TableId],
        budget: MaintenanceBudget,
        policy: AutomaticMultiSafeModePolicy,
    ) -> Result<Vec<DiscoveredAutomaticCandidate>, AutomaticSafeModeError> {
        if !policy.allow_lsm_flush && !policy.allow_lsm_compaction {
            return Ok(Vec::new());
        }
        let mut output = Vec::new();
        for table_id in stable_unique_tables(table_ids) {
            for observation in self.observe_adaptive_lsm_maintenance(table_id, budget)? {
                if policy.allow_lsm_flush {
                    let key = AutomaticCandidateKey::LsmFlush {
                        table_id,
                        storage_id: observation.storage_id,
                    };
                    let rank = AutomaticCandidateRankEvidence {
                        ready_age: self.ready_age(key),
                        maintenance_work_units: observation
                            .maintenance
                            .flush_conservative_bound
                            .map(|bound| bound.work_units),
                        read_bytes: observation
                            .maintenance
                            .flush_conservative_bound
                            .map(|bound| bound.read_bytes),
                        write_bytes: observation
                            .maintenance
                            .flush_conservative_bound
                            .map(|bound| bound.write_bytes),
                        lsm_memtable_entries: Some(observation.maintenance.memtable_entry_count),
                        lsm_memtable_bytes: Some(observation.maintenance.memtable_bytes),
                        lsm_flush_threshold_bytes: Some(
                            observation.maintenance.memtable_flush_threshold_bytes,
                        ),
                        ..AutomaticCandidateRankEvidence::default()
                    };
                    output.push(lsm_discovered_candidate(
                        key,
                        rank,
                        observation.decide_flush(policy.lsm_flush_policy, budget),
                    ));
                }
                if policy.allow_lsm_compaction {
                    let key = AutomaticCandidateKey::LsmCompaction {
                        table_id,
                        storage_id: observation.storage_id,
                    };
                    let plan = observation.maintenance.next_compaction.as_ref();
                    let rank = AutomaticCandidateRankEvidence {
                        ready_age: self.ready_age(key),
                        maintenance_work_units: plan.map(|plan| plan.conservative_bound.work_units),
                        read_bytes: plan.map(|plan| plan.conservative_bound.read_bytes),
                        write_bytes: plan.map(|plan| plan.conservative_bound.write_bytes),
                        lsm_memtable_entries: Some(observation.maintenance.memtable_entry_count),
                        lsm_memtable_bytes: Some(observation.maintenance.memtable_bytes),
                        lsm_flush_threshold_bytes: Some(
                            observation.maintenance.memtable_flush_threshold_bytes,
                        ),
                        lsm_compaction_input_bytes: plan.map(|plan| plan.input_bytes),
                        lsm_compaction_work_units: plan.map(|plan| plan.input_entries),
                        ..AutomaticCandidateRankEvidence::default()
                    };
                    output.push(lsm_discovered_candidate(
                        key,
                        rank,
                        observation.decide_compaction(policy.lsm_compaction_policy, budget),
                    ));
                }
            }
        }
        Ok(output)
    }

    fn ready_age(&self, key: AutomaticCandidateKey) -> u64 {
        self.automatic_safe_mode
            .admission
            .iter()
            .find(|entry| entry.key == key)
            .map_or(0, |entry| entry.ready_age)
    }

    fn advance_fairness(
        &mut self,
        candidates: &[AutomaticCandidateInspection],
        selected: AutomaticCandidateKey,
        maximum_entries: u64,
    ) {
        let maximum = usize::try_from(maximum_entries).unwrap_or(usize::MAX);
        let ready = candidates
            .iter()
            .filter(|candidate| candidate.readiness == AutomaticCandidateReadiness::Ready)
            .map(|candidate| candidate.key)
            .collect::<Vec<_>>();
        self.automatic_safe_mode
            .admission
            .retain(|entry| ready.contains(&entry.key));
        self.automatic_safe_mode.admission.truncate(maximum);
        for key in ready.into_iter().take(maximum) {
            if let Some(entry) = self
                .automatic_safe_mode
                .admission
                .iter_mut()
                .find(|entry| entry.key == key)
            {
                entry.ready_age = if key == selected {
                    0
                } else {
                    entry.ready_age.saturating_add(1)
                };
            } else {
                self.automatic_safe_mode
                    .admission
                    .push(CandidateAdmissionEntry {
                        key,
                        ready_age: u64::from(key != selected),
                    });
            }
        }
    }

    fn record_cross_lane_admission(&mut self, lane: AutomaticSafeModeLane) {
        match lane {
            AutomaticSafeModeLane::ColumnarMaintenance => {
                let consecutive = &mut self
                    .automatic_safe_mode
                    .cross_lane_service
                    .consecutive_columnar_admissions;
                *consecutive = consecutive.saturating_add(1);
            }
            AutomaticSafeModeLane::PlannerCalibration => {
                self.automatic_safe_mode
                    .cross_lane_service
                    .consecutive_columnar_admissions = 0;
            }
            AutomaticSafeModeLane::ChangeStreamReclamation => {}
            AutomaticSafeModeLane::AuthoritativeMaintenance => {}
            AutomaticSafeModeLane::None
            | AutomaticSafeModeLane::ActiveColumnarTrial
            | AutomaticSafeModeLane::ActivePlannerCalibrationTrial => {}
        }
    }

    fn execute_multi_candidate(
        &mut self,
        candidate: DiscoveredAutomaticCandidate,
        budget: MaintenanceBudget,
        trial_before: Option<AutomaticSafeTrial>,
    ) -> Result<AutomaticSafeModeReport, AutomaticSafeModeError> {
        match candidate.authority {
            Some(AutomaticCandidateAuthority::Columnar(authority)) => {
                let ColumnarCandidateAuthority {
                    observation,
                    proposal,
                } = *authority;
                let execution = self.execute_adaptive_columnar(&proposal, budget)?;
                let mutation = (execution.consumed.actions != 0).then_some(
                    AutomaticSafeModeMutation::ColumnarAdvance {
                        projection_id: proposal.projection_id,
                    },
                );
                if execution.outcome == AdaptiveMaintenanceOutcome::Kept {
                    let measurement = execution
                        .measurement
                        .as_ref()
                        .ok_or(AutomaticSafeModeError::MissingColumnarMeasurement)?;
                    self.automatic_safe_mode.active_trial = Some(ActiveAutomaticTrial::Columnar {
                        target: AdaptiveWorkloadTarget {
                            table_id: proposal.table_id,
                            storage_id: proposal.storage_id,
                            projection_id: proposal.projection_id,
                            generation: measurement.after_outcome.projection_generation,
                            schema_generation: measurement.schema_generation_after,
                        },
                    });
                }
                let cycle = AdaptiveCycleReport {
                    observation,
                    decision: AdaptiveDecision::Proposal(proposal),
                    execution: Some(execution),
                };
                Ok(self.finish_automatic_report(
                    trial_before,
                    AutomaticSafeModeLane::ColumnarMaintenance,
                    mutation,
                    Some(Box::new(cycle)),
                    None,
                    None,
                    None,
                    None,
                    AutomaticSafeModeOutcome::ColumnarMutationCompleted,
                ))
            }
            Some(AutomaticCandidateAuthority::ColumnarCompaction(authority)) => {
                let ColumnarCompactionCandidateAuthority { proposal } = *authority;
                let execution = self.execute_adaptive_columnar_compaction(&proposal, budget)?;
                let mutation = execution
                    .physical
                    .as_ref()
                    .filter(|physical| physical.compacted)
                    .map(|physical| AutomaticSafeModeMutation::ColumnarCompaction {
                        projection_id: physical.projection_id,
                        old_generation: physical.old_generation,
                        new_generation: physical.new_generation,
                    });
                if execution.outcome == AdaptiveColumnarCompactionOutcome::Completed {
                    let measurement = execution
                        .measurement
                        .as_ref()
                        .ok_or(AutomaticSafeModeError::MissingColumnarMeasurement)?;
                    let physical = execution
                        .physical
                        .as_ref()
                        .ok_or(AutomaticSafeModeError::MissingColumnarMeasurement)?;
                    self.automatic_safe_mode.active_trial = Some(ActiveAutomaticTrial::Columnar {
                        target: AdaptiveWorkloadTarget {
                            table_id: proposal.table_id,
                            storage_id: proposal.storage_id,
                            projection_id: proposal.projection_id,
                            generation: physical.new_generation,
                            schema_generation: measurement.schema_generation_after,
                        },
                    });
                }
                let outcome = match execution.outcome {
                    AdaptiveColumnarCompactionOutcome::Completed => {
                        AutomaticSafeModeOutcome::ColumnarCompactionCompleted
                    }
                    AdaptiveColumnarCompactionOutcome::RevertedInsufficientMeasuredBenefit => {
                        AutomaticSafeModeOutcome::ColumnarCompactionRevertedInsufficientMeasuredBenefit
                    }
                    AdaptiveColumnarCompactionOutcome::Aborted(_) => {
                        AutomaticSafeModeOutcome::ColumnarCompactionAborted
                    }
                    AdaptiveColumnarCompactionOutcome::InconclusiveNoWork
                    | AdaptiveColumnarCompactionOutcome::InconclusiveCostBoundExceeded
                    | AdaptiveColumnarCompactionOutcome::InconclusivePostconditionsChanged => {
                        AutomaticSafeModeOutcome::ColumnarCompactionInconclusive
                    }
                };
                let mut report = self.finish_automatic_report(
                    trial_before,
                    AutomaticSafeModeLane::ColumnarMaintenance,
                    mutation,
                    None,
                    None,
                    None,
                    None,
                    None,
                    outcome,
                );
                report.columnar_compaction = Some(Box::new(execution));
                Ok(report)
            }
            Some(AutomaticCandidateAuthority::ChangeStreamGc(authority)) => {
                let ChangeStreamGcCandidateAuthority { proposal } = *authority;
                let execution = self.execute_change_stream_reclamation(&proposal, budget)?;
                let mutation = execution.actual.as_ref().and_then(|actual| {
                    (execution.outcome == AdaptiveChangeStreamGcOutcome::Completed).then_some(
                        AutomaticSafeModeMutation::ChangeStreamReclamation {
                            storage_id: actual.storage_id,
                            new_earliest_frontier: actual.new_earliest_frontier,
                        },
                    )
                });
                let outcome = match execution.outcome {
                    AdaptiveChangeStreamGcOutcome::Completed => {
                        AutomaticSafeModeOutcome::ChangeStreamGcCompleted
                    }
                    AdaptiveChangeStreamGcOutcome::Aborted(_) => {
                        AutomaticSafeModeOutcome::ChangeStreamGcAborted
                    }
                    AdaptiveChangeStreamGcOutcome::InconclusiveAlreadyReclaimed
                    | AdaptiveChangeStreamGcOutcome::InconclusiveNoWork
                    | AdaptiveChangeStreamGcOutcome::InconclusiveCostBoundExceeded
                    | AdaptiveChangeStreamGcOutcome::InconclusivePostconditionsChanged => {
                        AutomaticSafeModeOutcome::ChangeStreamGcInconclusive
                    }
                };
                let mut report = self.finish_automatic_report(
                    trial_before,
                    AutomaticSafeModeLane::ChangeStreamReclamation,
                    mutation,
                    None,
                    None,
                    None,
                    None,
                    None,
                    outcome,
                );
                report.change_stream_gc = Some(Box::new(execution));
                Ok(report)
            }
            Some(AutomaticCandidateAuthority::LsmMaintenance(authority)) => {
                let LsmMaintenanceCandidateAuthority { proposal } = *authority;
                let execution = self.execute_adaptive_lsm_maintenance(&proposal, budget)?;
                let mutation = (execution.outcome == AdaptiveLsmMaintenanceOutcome::Completed)
                    .then_some(AutomaticSafeModeMutation::LsmMaintenance {
                        storage_id: match &proposal {
                            AdaptiveLsmMaintenanceProposal::Flush(proposal) => proposal.storage_id,
                            AdaptiveLsmMaintenanceProposal::CompactOne(proposal) => {
                                proposal.storage_id
                            }
                        },
                        action: proposal.action(),
                    });
                let outcome = match execution.outcome {
                    AdaptiveLsmMaintenanceOutcome::Completed => {
                        AutomaticSafeModeOutcome::LsmMaintenanceCompleted
                    }
                    AdaptiveLsmMaintenanceOutcome::Aborted(_) => {
                        AutomaticSafeModeOutcome::LsmMaintenanceAborted
                    }
                    AdaptiveLsmMaintenanceOutcome::InconclusiveNoWork => {
                        AutomaticSafeModeOutcome::LsmMaintenanceInconclusive
                    }
                };
                let mut report = self.finish_automatic_report(
                    trial_before,
                    AutomaticSafeModeLane::AuthoritativeMaintenance,
                    mutation,
                    None,
                    None,
                    None,
                    None,
                    None,
                    outcome,
                );
                report.lsm_maintenance = Some(Box::new(execution));
                Ok(report)
            }
            Some(AutomaticCandidateAuthority::PlannerCalibration(authority)) => {
                let CalibrationCandidateAuthority {
                    decision,
                    proposal,
                    shadow,
                    accepted_shadow,
                } = *authority;
                let schema_generation = self.schema_generation();
                let receipt = self.apply_planner_calibration(&proposal, &accepted_shadow)?;
                self.automatic_safe_mode.active_trial =
                    Some(ActiveAutomaticTrial::PlannerCalibration {
                        receipt,
                        schema_generation,
                    });
                Ok(self.finish_automatic_report(
                    trial_before,
                    AutomaticSafeModeLane::PlannerCalibration,
                    Some(AutomaticSafeModeMutation::PlannerCalibrationApply {
                        calibration_class: receipt.calibration_class(),
                        applied_epoch: receipt.applied_epoch(),
                    }),
                    None,
                    None,
                    Some(decision),
                    Some(shadow),
                    None,
                    AutomaticSafeModeOutcome::PlannerCalibrationApplied,
                ))
            }
            None => Ok(self.finish_automatic_report(
                trial_before,
                AutomaticSafeModeLane::None,
                None,
                None,
                None,
                None,
                None,
                None,
                AutomaticSafeModeOutcome::NoAction(AutomaticSafeModeNoAction::ColumnarNoAction),
            )),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn finish_automatic_report(
        &self,
        trial_before: Option<AutomaticSafeTrial>,
        selected_lane: AutomaticSafeModeLane,
        mutation: Option<AutomaticSafeModeMutation>,
        columnar_cycle: Option<Box<AdaptiveCycleReport>>,
        workload_evaluation: Option<AdaptiveWorkloadEvaluationReport>,
        calibration_decision: Option<PlannerCalibrationDecision>,
        calibration_shadow: Option<PlannerCalibrationShadowDecision>,
        calibration_trial_evaluation: Option<AutomaticCalibrationTrialEvaluationReport>,
        outcome: AutomaticSafeModeOutcome,
    ) -> AutomaticSafeModeReport {
        AutomaticSafeModeReport {
            trial_before,
            selected_lane,
            mutation,
            columnar_cycle,
            columnar_compaction: None,
            change_stream_gc: None,
            lsm_maintenance: None,
            workload_evaluation,
            calibration_decision,
            calibration_shadow,
            calibration_trial_evaluation,
            outcome,
            trial_after: self.automatic_safe_mode_state().active_trial(),
        }
    }

    fn evaluate_automatic_columnar_trial(
        &mut self,
        target: AdaptiveWorkloadTarget,
        window: Option<&AdaptiveWorkloadWindow>,
        policy: AdaptiveWorkloadPolicy,
        trial_before: Option<AutomaticSafeTrial>,
    ) -> Result<AutomaticSafeModeReport, AutomaticSafeModeError> {
        let empty_window;
        let (evaluation_window, supplied_matches) = match window {
            Some(window) if window.target == target => (window, true),
            _ => {
                empty_window =
                    AdaptiveWorkloadWindow::new(target, AdaptiveWorkloadLimits::default());
                (&empty_window, false)
            }
        };
        let evaluation = self.evaluate_adaptive_workload(evaluation_window, policy)?;
        let (outcome, mutation, clear) = match evaluation.outcome {
            AdaptiveWorkloadOutcome::StaleWindow(reason) => (
                AutomaticSafeModeOutcome::ColumnarTrialStale(reason),
                None,
                true,
            ),
            _ if !supplied_matches => (
                AutomaticSafeModeOutcome::ColumnarTrialAwaiting(if window.is_some() {
                    AutomaticTrialAwaitingReason::WorkloadTargetMismatch
                } else {
                    AutomaticTrialAwaitingReason::MissingWorkloadWindow
                }),
                None,
                false,
            ),
            AdaptiveWorkloadOutcome::ValidatedKeep => (
                AutomaticSafeModeOutcome::ColumnarTrialValidatedKeep,
                None,
                true,
            ),
            AdaptiveWorkloadOutcome::RevertedMeasuredRegression => (
                AutomaticSafeModeOutcome::ColumnarTrialReverted,
                Some(AutomaticSafeModeMutation::ColumnarSuppression { target }),
                true,
            ),
            AdaptiveWorkloadOutcome::HeldSuppressed => (
                AutomaticSafeModeOutcome::ColumnarTrialResolvedSuppressed,
                None,
                true,
            ),
            AdaptiveWorkloadOutcome::HeldWithinHysteresisBand => {
                (AutomaticSafeModeOutcome::ColumnarTrialHeld, None, false)
            }
            AdaptiveWorkloadOutcome::Inconclusive => (
                AutomaticSafeModeOutcome::ColumnarTrialAwaiting(
                    AutomaticTrialAwaitingReason::InsufficientEvidence,
                ),
                None,
                false,
            ),
        };
        if clear {
            self.automatic_safe_mode.active_trial = None;
        }
        Ok(self.finish_automatic_report(
            trial_before,
            AutomaticSafeModeLane::ActiveColumnarTrial,
            mutation,
            None,
            Some(evaluation),
            None,
            None,
            None,
            outcome,
        ))
    }

    fn evaluate_automatic_calibration_trial(
        &mut self,
        receipt: PlannerCalibrationReceipt,
        schema_generation: SchemaGeneration,
        window: Option<&AdaptiveWorkloadWindow>,
        policy: AutomaticCalibrationTrialPolicy,
        trial_before: Option<AutomaticSafeTrial>,
    ) -> Result<AutomaticSafeModeReport, AutomaticSafeModeError> {
        let evidence = window.map(|window| {
            aggregate_calibration_evidence(
                window,
                receipt.calibration_class(),
                receipt.applied_epoch(),
                0,
            )
        });
        let evidence_schema_matches =
            window.is_none_or(|window| window.target.schema_generation == schema_generation);
        self.evaluate_automatic_calibration_trial_evidence(
            receipt,
            schema_generation,
            evidence,
            evidence_schema_matches,
            policy,
            trial_before,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn evaluate_automatic_calibration_trial_evidence(
        &mut self,
        receipt: PlannerCalibrationReceipt,
        schema_generation: SchemaGeneration,
        evidence: Option<PlannerCalibrationEvidence>,
        evidence_schema_matches: bool,
        policy: AutomaticCalibrationTrialPolicy,
        trial_before: Option<AutomaticSafeTrial>,
    ) -> Result<AutomaticSafeModeReport, AutomaticSafeModeError> {
        let current = self.planner_calibration_profile();
        let stale = if self.schema_generation() != schema_generation {
            Some(AutomaticCalibrationTrialStaleReason::SchemaChanged)
        } else if current.epoch != receipt.applied_epoch() {
            Some(AutomaticCalibrationTrialStaleReason::CalibrationEpochChanged)
        } else if current.ratio(receipt.calibration_class()) != receipt.applied_ratio() {
            Some(AutomaticCalibrationTrialStaleReason::CalibrationRatioChanged)
        } else {
            None
        };
        if let Some(reason) = stale {
            self.automatic_safe_mode.active_trial = None;
            return Ok(self.finish_automatic_report(
                trial_before,
                AutomaticSafeModeLane::ActivePlannerCalibrationTrial,
                None,
                None,
                None,
                None,
                None,
                None,
                AutomaticSafeModeOutcome::PlannerCalibrationTrialStale(reason),
            ));
        }
        let Some(evidence) = evidence else {
            return Ok(self.finish_automatic_report(
                trial_before,
                AutomaticSafeModeLane::ActivePlannerCalibrationTrial,
                None,
                None,
                None,
                None,
                None,
                None,
                AutomaticSafeModeOutcome::PlannerCalibrationTrialAwaiting(
                    AutomaticTrialAwaitingReason::MissingWorkloadWindow,
                ),
            ));
        };
        if !evidence_schema_matches || evidence.schema_generation != schema_generation {
            return Ok(self.finish_automatic_report(
                trial_before,
                AutomaticSafeModeLane::ActivePlannerCalibrationTrial,
                None,
                None,
                None,
                None,
                None,
                None,
                AutomaticSafeModeOutcome::PlannerCalibrationTrialAwaiting(
                    AutomaticTrialAwaitingReason::EvidenceSchemaMismatch,
                ),
            ));
        }
        let replay = replay_calibration_ratio_errors(
            &evidence,
            receipt.previous_ratio(),
            receipt.applied_ratio(),
        );
        let report = AutomaticCalibrationTrialEvaluationReport {
            calibration_class: evidence.calibration_class,
            calibration_epoch: evidence.calibration_epoch,
            sample_count: evidence.sample_count,
            total_actual_work_units: evidence.total_actual_work_units,
            distinct_visibility_points: evidence.distinct_visibility_points,
            distinct_query_shapes: evidence.distinct_query_shapes,
            previous_ratio_error_work_units: replay.old_error_work_units,
            applied_ratio_error_work_units: replay.new_error_work_units,
            overflowed: evidence.overflowed,
            incomplete: evidence.incomplete || replay.incomplete,
            truncated: evidence.truncated,
        };
        let awaiting = if evidence.overflowed || evidence.incomplete || evidence.truncated {
            Some(AutomaticTrialAwaitingReason::IncompleteEvidence)
        } else if replay.incomplete {
            Some(AutomaticTrialAwaitingReason::ArithmeticUnavailable)
        } else if evidence.sample_count < policy.minimum_samples
            || evidence.total_actual_work_units < policy.minimum_actual_work_units
            || evidence.distinct_visibility_points < policy.minimum_distinct_visibility_points
            || evidence.distinct_query_shapes < policy.minimum_distinct_query_shapes
        {
            Some(AutomaticTrialAwaitingReason::InsufficientEvidence)
        } else {
            None
        };
        if let Some(reason) = awaiting {
            return Ok(self.finish_automatic_report(
                trial_before,
                AutomaticSafeModeLane::ActivePlannerCalibrationTrial,
                None,
                None,
                None,
                None,
                None,
                Some(report),
                AutomaticSafeModeOutcome::PlannerCalibrationTrialAwaiting(reason),
            ));
        }

        let old_error = replay.old_error_work_units;
        let new_error = replay.new_error_work_units;
        let (outcome, mutation, clear) = if old_error >= new_error
            && old_error - new_error >= policy.minimum_keep_error_improvement_work_units
        {
            (
                AutomaticSafeModeOutcome::PlannerCalibrationTrialValidatedKeep,
                None,
                true,
            )
        } else if new_error > old_error
            && new_error - old_error > policy.maximum_tolerated_error_regression_work_units
        {
            let reverted = self.revert_planner_calibration(receipt)?;
            (
                AutomaticSafeModeOutcome::PlannerCalibrationTrialReverted,
                Some(AutomaticSafeModeMutation::PlannerCalibrationRevert {
                    calibration_class: receipt.calibration_class(),
                    reverted_epoch: reverted.applied_epoch(),
                }),
                true,
            )
        } else {
            (
                AutomaticSafeModeOutcome::PlannerCalibrationTrialHeld,
                None,
                false,
            )
        };
        if clear {
            self.automatic_safe_mode.active_trial = None;
        }
        Ok(self.finish_automatic_report(
            trial_before,
            AutomaticSafeModeLane::ActivePlannerCalibrationTrial,
            mutation,
            None,
            None,
            None,
            None,
            Some(report),
            outcome,
        ))
    }
}

fn validate_multi_scope(
    scope: AutomaticAdmissionScope<'_>,
    policy: AutomaticMultiSafeModePolicy,
) -> Result<(), AutomaticSafeModeError> {
    if policy.allow_columnar_compaction && !policy.columnar_compaction_policy.is_valid() {
        return Err(AutomaticSafeModeError::InvalidColumnarCompactionPolicy);
    }
    if policy.allow_change_stream_gc && !policy.change_stream_gc_policy.is_valid() {
        return Err(AutomaticSafeModeError::InvalidChangeStreamGcPolicy);
    }
    if matches!(
        policy.cross_lane_service,
        AutomaticCrossLaneServicePolicy::BoundedColumnarBurst {
            max_consecutive_columnar_admissions: 0
        }
    ) {
        return Err(AutomaticSafeModeError::InvalidCrossLaneServicePolicy);
    }
    if u64::try_from(scope.table_ids.len())
        .map_or(true, |count| count > policy.max_candidate_tables)
        || u64::try_from(scope.calibration_classes.len())
            .map_or(true, |count| count > policy.max_calibration_classes)
    {
        return Err(AutomaticSafeModeError::AdmissionScopeTooLarge);
    }
    Ok(())
}

fn validate_candidate_count(
    columnar: usize,
    reclamation: usize,
    authoritative: usize,
    calibration: usize,
    policy: AutomaticMultiSafeModePolicy,
) -> Result<(), AutomaticSafeModeError> {
    if u64::try_from(
        columnar
            .saturating_add(reclamation)
            .saturating_add(authoritative)
            .saturating_add(calibration),
    )
    .map_or(true, |count| count > policy.max_fairness_entries)
    {
        return Err(AutomaticSafeModeError::AdmissionScopeTooLarge);
    }
    Ok(())
}

fn lsm_discovered_candidate(
    key: AutomaticCandidateKey,
    rank: AutomaticCandidateRankEvidence,
    decision: AdaptiveLsmMaintenanceDecision,
) -> DiscoveredAutomaticCandidate {
    match decision {
        AdaptiveLsmMaintenanceDecision::Proposal(proposal) => DiscoveredAutomaticCandidate {
            inspection: AutomaticCandidateInspection {
                key,
                lane: AutomaticSafeModeLane::AuthoritativeMaintenance,
                readiness: AutomaticCandidateReadiness::Ready,
                rank,
            },
            authority: Some(AutomaticCandidateAuthority::LsmMaintenance(Box::new(
                LsmMaintenanceCandidateAuthority {
                    proposal: *proposal,
                },
            ))),
        },
        AdaptiveLsmMaintenanceDecision::NoAction(reason) => DiscoveredAutomaticCandidate {
            inspection: AutomaticCandidateInspection {
                key,
                lane: AutomaticSafeModeLane::AuthoritativeMaintenance,
                readiness: AutomaticCandidateReadiness::LsmMaintenanceBlocked(reason),
                rank,
            },
            authority: None,
        },
    }
}

fn select_ready_lane(
    columnar: &[DiscoveredAutomaticCandidate],
    reclamation: &[DiscoveredAutomaticCandidate],
    authoritative: &[DiscoveredAutomaticCandidate],
    calibration: &[DiscoveredAutomaticCandidate],
    policy: AutomaticCrossLaneServicePolicy,
    state: AutomaticCrossLaneServiceState,
) -> (AutomaticSafeModeLane, AutomaticLaneSelectionReason) {
    let columnar_ready = columnar
        .iter()
        .any(|candidate| candidate.inspection.readiness == AutomaticCandidateReadiness::Ready);
    let calibration_ready = calibration
        .iter()
        .any(|candidate| candidate.inspection.readiness == AutomaticCandidateReadiness::Ready);
    let reclamation_ready = reclamation
        .iter()
        .any(|candidate| candidate.inspection.readiness == AutomaticCandidateReadiness::Ready);
    let authoritative_ready = authoritative
        .iter()
        .any(|candidate| candidate.inspection.readiness == AutomaticCandidateReadiness::Ready);
    if columnar_ready && calibration_ready {
        return match policy {
            AutomaticCrossLaneServicePolicy::BoundedColumnarBurst {
                max_consecutive_columnar_admissions,
            } if state.consecutive_columnar_admissions >= max_consecutive_columnar_admissions => (
                AutomaticSafeModeLane::PlannerCalibration,
                AutomaticLaneSelectionReason::CalibrationServiceDue,
            ),
            AutomaticCrossLaneServicePolicy::BoundedColumnarBurst { .. } => (
                AutomaticSafeModeLane::ColumnarMaintenance,
                AutomaticLaneSelectionReason::ColumnarBurstAvailable,
            ),
            AutomaticCrossLaneServicePolicy::StrictPhysicalPriority => (
                AutomaticSafeModeLane::ColumnarMaintenance,
                AutomaticLaneSelectionReason::StrictPhysicalPriority,
            ),
        };
    }
    if columnar_ready {
        return (
            AutomaticSafeModeLane::ColumnarMaintenance,
            AutomaticLaneSelectionReason::OnlyColumnarReady,
        );
    }
    if reclamation_ready {
        return (
            AutomaticSafeModeLane::ChangeStreamReclamation,
            if calibration_ready || authoritative_ready {
                AutomaticLaneSelectionReason::ReclamationPriority
            } else {
                AutomaticLaneSelectionReason::OnlyReclamationReady
            },
        );
    }
    if authoritative_ready {
        return (
            AutomaticSafeModeLane::AuthoritativeMaintenance,
            if calibration_ready {
                AutomaticLaneSelectionReason::AuthoritativeMaintenancePriority
            } else {
                AutomaticLaneSelectionReason::OnlyAuthoritativeMaintenanceReady
            },
        );
    }
    if calibration_ready {
        return (
            AutomaticSafeModeLane::PlannerCalibration,
            AutomaticLaneSelectionReason::OnlyCalibrationReady,
        );
    }
    (
        AutomaticSafeModeLane::None,
        AutomaticLaneSelectionReason::NoReadyCandidates,
    )
}

fn compaction_rank_evidence(
    ready_age: u64,
    projection_id: ColumnarProjectionId,
    observation: &AdaptiveColumnarCompactionObservation,
) -> AutomaticCandidateRankEvidence {
    let projection = observation
        .observation
        .projections
        .iter()
        .find(|target| target.projection.projection_id == Some(projection_id));
    AutomaticCandidateRankEvidence {
        ready_age,
        maintenance_work_units: Some(observation.maintenance_candidate.estimate.work_units),
        read_bytes: Some(observation.maintenance_candidate.estimate.read_bytes),
        write_bytes: Some(observation.maintenance_candidate.estimate.write_bytes),
        delta_segment_count: projection.and_then(|target| target.projection.delta_segment_count),
        delta_bytes: projection.and_then(|target| target.projection.delta_bytes),
        delta_mutations: projection.and_then(|target| target.projection.delta_mutations),
        suppressed_versions: projection.and_then(|target| target.projection.suppressed_versions),
        ..AutomaticCandidateRankEvidence::default()
    }
}

fn stable_unique_tables(table_ids: &[TableId]) -> Vec<TableId> {
    let mut output = Vec::new();
    for table_id in table_ids {
        if !output.contains(table_id) {
            output.push(*table_id);
        }
    }
    output
}

fn stable_unique_classes(classes: &[PlannerCalibrationClass]) -> Vec<PlannerCalibrationClass> {
    let mut output = Vec::new();
    for class in classes {
        if !output.contains(class) {
            output.push(*class);
        }
    }
    output
}

fn select_columnar_candidate(candidates: &[DiscoveredAutomaticCandidate]) -> Option<usize> {
    candidates
        .iter()
        .enumerate()
        .filter(|(_, candidate)| {
            candidate.inspection.readiness == AutomaticCandidateReadiness::Ready
        })
        .max_by(|(_, left), (_, right)| compare_columnar_rank(&left.inspection, &right.inspection))
        .map(|(index, _)| index)
}

fn compare_columnar_rank(
    left: &AutomaticCandidateInspection,
    right: &AutomaticCandidateInspection,
) -> std::cmp::Ordering {
    left.rank
        .ready_age
        .cmp(&right.rank.ready_age)
        .then_with(|| columnar_class_priority(left.key).cmp(&columnar_class_priority(right.key)))
        .then_with(|| match (left.key, right.key) {
            (AutomaticCandidateKey::Columnar { .. }, AutomaticCandidateKey::Columnar { .. }) => {
                left.rank
                    .expected_benefit_work_units
                    .cmp(&right.rank.expected_benefit_work_units)
                    .then_with(|| {
                        right
                            .rank
                            .maintenance_work_units
                            .cmp(&left.rank.maintenance_work_units)
                    })
                    .then_with(|| right.rank.read_bytes.cmp(&left.rank.read_bytes))
                    .then_with(|| right.rank.write_bytes.cmp(&left.rank.write_bytes))
            }
            (
                AutomaticCandidateKey::ColumnarCompaction { .. },
                AutomaticCandidateKey::ColumnarCompaction { .. },
            ) => left
                .rank
                .delta_bytes
                .cmp(&right.rank.delta_bytes)
                .then_with(|| {
                    left.rank
                        .delta_segment_count
                        .cmp(&right.rank.delta_segment_count)
                })
                .then_with(|| {
                    left.rank
                        .suppressed_versions
                        .cmp(&right.rank.suppressed_versions)
                })
                .then_with(|| left.rank.delta_mutations.cmp(&right.rank.delta_mutations))
                .then_with(|| {
                    right
                        .rank
                        .maintenance_work_units
                        .cmp(&left.rank.maintenance_work_units)
                })
                .then_with(|| right.rank.read_bytes.cmp(&left.rank.read_bytes))
                .then_with(|| right.rank.write_bytes.cmp(&left.rank.write_bytes)),
            _ => std::cmp::Ordering::Equal,
        })
        .then_with(|| right.key.cmp(&left.key))
}

const fn columnar_class_priority(key: AutomaticCandidateKey) -> u8 {
    match key {
        AutomaticCandidateKey::Columnar { .. } => 1,
        AutomaticCandidateKey::ColumnarCompaction { .. } => 0,
        AutomaticCandidateKey::PlannerCalibration { .. }
        | AutomaticCandidateKey::ChangeStreamGc { .. }
        | AutomaticCandidateKey::LsmFlush { .. }
        | AutomaticCandidateKey::LsmCompaction { .. } => 0,
    }
}

fn select_lsm_candidate(candidates: &[DiscoveredAutomaticCandidate]) -> Option<usize> {
    candidates
        .iter()
        .enumerate()
        .filter(|(_, candidate)| {
            candidate.inspection.readiness == AutomaticCandidateReadiness::Ready
        })
        .max_by(|(_, left), (_, right)| compare_lsm_rank(&left.inspection, &right.inspection))
        .map(|(index, _)| index)
}

fn compare_lsm_rank(
    left: &AutomaticCandidateInspection,
    right: &AutomaticCandidateInspection,
) -> std::cmp::Ordering {
    left.rank
        .ready_age
        .cmp(&right.rank.ready_age)
        .then_with(|| lsm_class_priority(left.key).cmp(&lsm_class_priority(right.key)))
        .then_with(|| match (left.key, right.key) {
            (AutomaticCandidateKey::LsmFlush { .. }, AutomaticCandidateKey::LsmFlush { .. }) => {
                left.rank
                    .lsm_memtable_bytes
                    .cmp(&right.rank.lsm_memtable_bytes)
                    .then_with(|| {
                        left.rank
                            .lsm_memtable_entries
                            .cmp(&right.rank.lsm_memtable_entries)
                    })
                    .then_with(|| {
                        right
                            .rank
                            .maintenance_work_units
                            .cmp(&left.rank.maintenance_work_units)
                    })
                    .then_with(|| right.rank.write_bytes.cmp(&left.rank.write_bytes))
            }
            (
                AutomaticCandidateKey::LsmCompaction { .. },
                AutomaticCandidateKey::LsmCompaction { .. },
            ) => left
                .rank
                .lsm_compaction_input_bytes
                .cmp(&right.rank.lsm_compaction_input_bytes)
                .then_with(|| {
                    left.rank
                        .lsm_compaction_work_units
                        .cmp(&right.rank.lsm_compaction_work_units)
                })
                .then_with(|| right.rank.write_bytes.cmp(&left.rank.write_bytes)),
            _ => std::cmp::Ordering::Equal,
        })
        .then_with(|| right.key.cmp(&left.key))
}

const fn lsm_class_priority(key: AutomaticCandidateKey) -> u8 {
    match key {
        AutomaticCandidateKey::LsmFlush { .. } => 1,
        AutomaticCandidateKey::LsmCompaction { .. } => 0,
        _ => 0,
    }
}

fn select_reclamation_candidate(candidates: &[DiscoveredAutomaticCandidate]) -> Option<usize> {
    candidates
        .iter()
        .enumerate()
        .filter(|(_, candidate)| {
            candidate.inspection.readiness == AutomaticCandidateReadiness::Ready
        })
        .max_by(|(_, left), (_, right)| {
            left.inspection
                .rank
                .ready_age
                .cmp(&right.inspection.rank.ready_age)
                .then_with(|| {
                    left.inspection
                        .rank
                        .reclaimable_bytes
                        .cmp(&right.inspection.rank.reclaimable_bytes)
                })
                .then_with(|| {
                    left.inspection
                        .rank
                        .reclaimable_batches
                        .cmp(&right.inspection.rank.reclaimable_batches)
                })
                .then_with(|| {
                    right
                        .inspection
                        .rank
                        .write_bytes
                        .cmp(&left.inspection.rank.write_bytes)
                })
                .then_with(|| right.inspection.key.cmp(&left.inspection.key))
        })
        .map(|(index, _)| index)
}

fn select_calibration_candidate(candidates: &[DiscoveredAutomaticCandidate]) -> Option<usize> {
    candidates
        .iter()
        .enumerate()
        .filter(|(_, candidate)| {
            candidate.inspection.readiness == AutomaticCandidateReadiness::Ready
        })
        .max_by(|(_, left), (_, right)| {
            left.inspection
                .rank
                .ready_age
                .cmp(&right.inspection.rank.ready_age)
                .then_with(|| {
                    left.inspection
                        .rank
                        .shadow_improvement_work_units
                        .cmp(&right.inspection.rank.shadow_improvement_work_units)
                })
                .then_with(|| {
                    left.inspection
                        .rank
                        .distinct_query_shapes
                        .cmp(&right.inspection.rank.distinct_query_shapes)
                })
                .then_with(|| {
                    left.inspection
                        .rank
                        .sample_count
                        .cmp(&right.inspection.rank.sample_count)
                })
                .then_with(|| right.inspection.key.cmp(&left.inspection.key))
        })
        .map(|(index, _)| index)
}

#[cfg(test)]
mod admission_tests {
    use super::*;

    fn columnar(
        table: u64,
        projection: u64,
        age: u64,
        benefit: u64,
        work: u64,
    ) -> DiscoveredAutomaticCandidate {
        DiscoveredAutomaticCandidate {
            inspection: AutomaticCandidateInspection {
                key: AutomaticCandidateKey::Columnar {
                    table_id: TableId(table),
                    projection_id: Some(ColumnarProjectionId(projection)),
                },
                lane: AutomaticSafeModeLane::ColumnarMaintenance,
                readiness: AutomaticCandidateReadiness::Ready,
                rank: AutomaticCandidateRankEvidence {
                    ready_age: age,
                    expected_benefit_work_units: Some(benefit),
                    maintenance_work_units: Some(work),
                    read_bytes: Some(10),
                    write_bytes: Some(10),
                    ..AutomaticCandidateRankEvidence::default()
                },
            },
            authority: None,
        }
    }

    fn compaction(
        table: u64,
        projection: u64,
        age: u64,
        delta_bytes: u64,
        delta_segments: u64,
    ) -> DiscoveredAutomaticCandidate {
        DiscoveredAutomaticCandidate {
            inspection: AutomaticCandidateInspection {
                key: AutomaticCandidateKey::ColumnarCompaction {
                    table_id: TableId(table),
                    projection_id: ColumnarProjectionId(projection),
                },
                lane: AutomaticSafeModeLane::ColumnarMaintenance,
                readiness: AutomaticCandidateReadiness::Ready,
                rank: AutomaticCandidateRankEvidence {
                    ready_age: age,
                    maintenance_work_units: Some(20),
                    read_bytes: Some(30),
                    write_bytes: Some(40),
                    delta_segment_count: Some(delta_segments),
                    delta_bytes: Some(delta_bytes),
                    delta_mutations: Some(10),
                    suppressed_versions: Some(2),
                    ..AutomaticCandidateRankEvidence::default()
                },
            },
            authority: None,
        }
    }

    fn calibration(
        class: PlannerCalibrationClass,
        age: u64,
        improvement: u64,
    ) -> DiscoveredAutomaticCandidate {
        DiscoveredAutomaticCandidate {
            inspection: AutomaticCandidateInspection {
                key: AutomaticCandidateKey::PlannerCalibration {
                    calibration_class: class,
                },
                lane: AutomaticSafeModeLane::PlannerCalibration,
                readiness: AutomaticCandidateReadiness::Ready,
                rank: AutomaticCandidateRankEvidence {
                    ready_age: age,
                    shadow_improvement_work_units: Some(improvement),
                    distinct_query_shapes: Some(3),
                    sample_count: Some(10),
                    ..AutomaticCandidateRankEvidence::default()
                },
            },
            authority: None,
        }
    }

    fn reclamation(table: u64, storage: u64, age: u64, bytes: u64) -> DiscoveredAutomaticCandidate {
        DiscoveredAutomaticCandidate {
            inspection: AutomaticCandidateInspection {
                key: AutomaticCandidateKey::ChangeStreamGc {
                    table_id: TableId(table),
                    storage_id: StorageId(storage),
                },
                lane: AutomaticSafeModeLane::ChangeStreamReclamation,
                readiness: AutomaticCandidateReadiness::Ready,
                rank: AutomaticCandidateRankEvidence {
                    ready_age: age,
                    reclaimable_batches: Some(2),
                    reclaimable_bytes: Some(bytes),
                    write_bytes: Some(10),
                    ..AutomaticCandidateRankEvidence::default()
                },
            },
            authority: None,
        }
    }

    fn lsm(
        table: u64,
        storage: u64,
        flush: bool,
        age: u64,
        bytes: u64,
    ) -> DiscoveredAutomaticCandidate {
        DiscoveredAutomaticCandidate {
            inspection: AutomaticCandidateInspection {
                key: if flush {
                    AutomaticCandidateKey::LsmFlush {
                        table_id: TableId(table),
                        storage_id: StorageId(storage),
                    }
                } else {
                    AutomaticCandidateKey::LsmCompaction {
                        table_id: TableId(table),
                        storage_id: StorageId(storage),
                    }
                },
                lane: AutomaticSafeModeLane::AuthoritativeMaintenance,
                readiness: AutomaticCandidateReadiness::Ready,
                rank: AutomaticCandidateRankEvidence {
                    ready_age: age,
                    write_bytes: Some(bytes),
                    lsm_memtable_entries: flush.then_some(2),
                    lsm_memtable_bytes: flush.then_some(bytes),
                    lsm_compaction_input_bytes: (!flush).then_some(bytes),
                    lsm_compaction_work_units: (!flush).then_some(2),
                    ..AutomaticCandidateRankEvidence::default()
                },
            },
            authority: None,
        }
    }

    #[test]
    fn ready_age_precedes_merit_and_never_makes_blocked_ready() {
        let high = columnar(1, 1, 0, 10_000, 1);
        let low_aged = columnar(2, 2, 1, 1, 100);
        let mut blocked = columnar(3, 3, u64::MAX, u64::MAX, 0);
        blocked.inspection.readiness = AutomaticCandidateReadiness::ColumnarBlocked(
            AdaptiveNoActionReason::InsufficientBudgetEstimate,
        );
        let candidates = vec![high, low_aged, blocked];
        assert_eq!(select_columnar_candidate(&candidates), Some(1));
    }

    #[test]
    fn columnar_merit_and_identity_ties_are_deterministic() {
        let high_cost = columnar(1, 1, 0, 100, 10);
        let low_cost_later = columnar(2, 2, 0, 100, 5);
        assert_eq!(
            select_columnar_candidate(&[high_cost, low_cost_later]),
            Some(1)
        );
        let later_id = columnar(2, 2, 0, 100, 5);
        let earlier_id = columnar(1, 1, 0, 100, 5);
        assert_eq!(select_columnar_candidate(&[later_id, earlier_id]), Some(1));
    }

    #[test]
    fn calibration_rank_uses_age_then_shadow_improvement() {
        let columnar = calibration(PlannerCalibrationClass::Columnar, 0, 100);
        let seq = calibration(PlannerCalibrationClass::SeqScan, 0, 500);
        assert_eq!(select_calibration_candidate(&[columnar, seq]), Some(1));
        let aged = calibration(PlannerCalibrationClass::Columnar, 1, 1);
        let stronger = calibration(PlannerCalibrationClass::SeqScan, 0, 500);
        assert_eq!(select_calibration_candidate(&[aged, stronger]), Some(0));
    }

    #[test]
    fn columnar_rank_uses_age_then_class_then_class_local_evidence() {
        let catch_up = columnar(1, 1, 0, 10, 1);
        let compact = compaction(2, 2, 0, 10_000, 8);
        assert_eq!(select_columnar_candidate(&[catch_up, compact]), Some(0));

        let catch_up = columnar(1, 1, 0, 10_000, 1);
        let aged_compaction = compaction(2, 2, 1, 1, 1);
        assert_eq!(
            select_columnar_candidate(&[catch_up, aged_compaction]),
            Some(1)
        );

        let small = compaction(1, 1, 0, 100, 10);
        let large = compaction(2, 2, 0, 200, 1);
        assert_eq!(select_columnar_candidate(&[small, large]), Some(1));
    }

    #[test]
    fn lsm_rank_uses_age_then_flush_priority_then_real_pressure() {
        let flush = lsm(1, 1, true, 0, 10);
        let compact = lsm(2, 2, false, 0, 10_000);
        assert_eq!(select_lsm_candidate(&[flush, compact]), Some(0));

        let flush = lsm(1, 1, true, 0, 10_000);
        let aged_compact = lsm(2, 2, false, 1, 1);
        assert_eq!(select_lsm_candidate(&[flush, aged_compact]), Some(1));

        let small = lsm(1, 1, false, 0, 10);
        let large = lsm(2, 2, false, 0, 100);
        assert_eq!(select_lsm_candidate(&[small, large]), Some(1));
    }

    #[test]
    fn authoritative_lane_follows_reclamation_and_precedes_calibration() {
        let authoritative = vec![lsm(1, 1, true, 0, 10)];
        let calibration = vec![calibration(PlannerCalibrationClass::Columnar, 0, 1)];
        assert_eq!(
            select_ready_lane(
                &[],
                &[],
                &authoritative,
                &calibration,
                AutomaticCrossLaneServicePolicy::StrictPhysicalPriority,
                AutomaticCrossLaneServiceState::default(),
            ),
            (
                AutomaticSafeModeLane::AuthoritativeMaintenance,
                AutomaticLaneSelectionReason::AuthoritativeMaintenancePriority,
            )
        );
        let reclamation = vec![reclamation(2, 2, 0, 20)];
        assert_eq!(
            select_ready_lane(
                &[],
                &reclamation,
                &authoritative,
                &calibration,
                AutomaticCrossLaneServicePolicy::StrictPhysicalPriority,
                AutomaticCrossLaneServiceState::default(),
            )
            .0,
            AutomaticSafeModeLane::ChangeStreamReclamation
        );
    }

    #[test]
    fn cross_lane_service_is_opt_in_and_cannot_override_readiness() {
        assert_eq!(
            AutomaticMultiSafeModePolicy::default().cross_lane_service,
            AutomaticCrossLaneServicePolicy::StrictPhysicalPriority
        );
        let columnar = vec![columnar(1, 1, 0, 1, 1)];
        let calibration = vec![calibration(PlannerCalibrationClass::Columnar, 0, 1)];
        let state = AutomaticCrossLaneServiceState {
            consecutive_columnar_admissions: 2,
        };
        assert_eq!(
            select_ready_lane(
                &columnar,
                &[],
                &[],
                &calibration,
                AutomaticCrossLaneServicePolicy::StrictPhysicalPriority,
                state,
            ),
            (
                AutomaticSafeModeLane::ColumnarMaintenance,
                AutomaticLaneSelectionReason::StrictPhysicalPriority,
            )
        );
        assert_eq!(
            select_ready_lane(
                &columnar,
                &[],
                &[],
                &calibration,
                AutomaticCrossLaneServicePolicy::BoundedColumnarBurst {
                    max_consecutive_columnar_admissions: 2,
                },
                state,
            ),
            (
                AutomaticSafeModeLane::PlannerCalibration,
                AutomaticLaneSelectionReason::CalibrationServiceDue,
            )
        );
        let mut blocked = calibration[0].clone();
        blocked.inspection.readiness = AutomaticCandidateReadiness::CalibrationEvidenceUnavailable;
        assert_eq!(
            select_ready_lane(
                &columnar,
                &[],
                &[],
                &[blocked],
                AutomaticCrossLaneServicePolicy::BoundedColumnarBurst {
                    max_consecutive_columnar_admissions: 2,
                },
                state,
            ),
            (
                AutomaticSafeModeLane::ColumnarMaintenance,
                AutomaticLaneSelectionReason::OnlyColumnarReady,
            )
        );
    }

    #[test]
    fn reclamation_has_explicit_priority_without_reinterpreting_bounded_burst() {
        let columnar = vec![columnar(1, 1, 0, 1, 1)];
        let reclamation = vec![reclamation(2, 2, 0, 100)];
        let calibration = vec![calibration(PlannerCalibrationClass::Columnar, 0, 1)];
        let state = AutomaticCrossLaneServiceState {
            consecutive_columnar_admissions: 2,
        };
        assert_eq!(
            select_ready_lane(
                &columnar,
                &reclamation,
                &[],
                &calibration,
                AutomaticCrossLaneServicePolicy::StrictPhysicalPriority,
                state,
            ),
            (
                AutomaticSafeModeLane::ColumnarMaintenance,
                AutomaticLaneSelectionReason::StrictPhysicalPriority,
            )
        );
        assert_eq!(
            select_ready_lane(
                &[],
                &reclamation,
                &[],
                &calibration,
                AutomaticCrossLaneServicePolicy::StrictPhysicalPriority,
                state,
            ),
            (
                AutomaticSafeModeLane::ChangeStreamReclamation,
                AutomaticLaneSelectionReason::ReclamationPriority,
            )
        );
        assert_eq!(
            select_ready_lane(
                &columnar,
                &reclamation,
                &[],
                &calibration,
                AutomaticCrossLaneServicePolicy::BoundedColumnarBurst {
                    max_consecutive_columnar_admissions: 2,
                },
                state,
            ),
            (
                AutomaticSafeModeLane::PlannerCalibration,
                AutomaticLaneSelectionReason::CalibrationServiceDue,
            )
        );
    }

    #[test]
    fn reclamation_ranking_is_age_then_pressure_cost_and_identity() {
        let large = reclamation(2, 2, 0, 200);
        let small_aged = reclamation(3, 3, 1, 1);
        assert_eq!(
            select_reclamation_candidate(&[large.clone(), small_aged]),
            Some(1)
        );
        let small = reclamation(1, 1, 0, 100);
        assert_eq!(select_reclamation_candidate(&[small, large]), Some(1));
    }

    #[test]
    fn zero_burst_is_a_typed_policy_error() {
        let policy = AutomaticMultiSafeModePolicy {
            cross_lane_service: AutomaticCrossLaneServicePolicy::BoundedColumnarBurst {
                max_consecutive_columnar_admissions: 0,
            },
            ..AutomaticMultiSafeModePolicy::default()
        };
        assert!(matches!(
            validate_multi_scope(
                AutomaticAdmissionScope {
                    table_ids: &[],
                    calibration_classes: &[],
                },
                policy,
            ),
            Err(AutomaticSafeModeError::InvalidCrossLaneServicePolicy)
        ));
    }

    #[test]
    fn enabled_compaction_requires_a_nonzero_pressure_threshold() {
        let policy = AutomaticMultiSafeModePolicy {
            allow_columnar_compaction: true,
            columnar_compaction_policy: AdaptiveColumnarCompactionPolicy::new(0, 0),
            ..AutomaticMultiSafeModePolicy::default()
        };
        assert!(matches!(
            validate_multi_scope(
                AutomaticAdmissionScope {
                    table_ids: &[],
                    calibration_classes: &[],
                },
                policy,
            ),
            Err(AutomaticSafeModeError::InvalidColumnarCompactionPolicy)
        ));
    }
}
