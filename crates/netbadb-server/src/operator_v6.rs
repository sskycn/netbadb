//! Frozen public NBOP v6 data-transfer objects.
//!
//! These shapes are historical compatibility references only. The listener and
//! client accept only the current protocol version.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorAdaptiveModeV6 {
    FeedbackOnly,
    Driven,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorEvidencePoolHealthV6 {
    Healthy,
    RotationRecommended,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorEvidenceRecordOutcomeV6 {
    Recorded,
    SchemaRotated,
    RecordedWithCapacityRejection,
    SchemaRotatedWithCapacityRejection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorEvidenceRecordErrorV6 {
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
pub enum OperatorSchedulerDelayClassV6 {
    Normal,
    Idle,
    NoProgress,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorEvidenceRenewalReasonV6 {
    ColumnarPhysicalStateChanged,
    ColumnarEligibilityChanged,
    AuthoritativeLsmLayoutChanged,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorSchedulerFaultV6 {
    MaintenanceEnvelopeExceeded,
    StepFailed,
    ConsumptionOverflow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum OperatorSchedulerGateV6 {
    Open {
        delay_class: OperatorSchedulerDelayClassV6,
    },
    AwaitingTrialProgress {
        window_epoch: u64,
        schema_generation: Option<u64>,
        recorded_reports: u64,
    },
    AwaitingEvidenceRenewal {
        blocked_window_epoch: u64,
        renewal_reason: OperatorEvidenceRenewalReasonV6,
    },
    Faulted {
        fault: OperatorSchedulerFaultV6,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorOrchestrationStopReasonV6 {
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
pub struct OperatorFeedbackStatusV6 {
    pub eligible_query_count: u64,
    pub record_success_count: u64,
    pub record_error_count: u64,
    pub capacity_rejection_count: u64,
    pub schema_rotation_count: u64,
    pub incomplete_report_count: u64,
    pub counter_overflowed: bool,
    pub last_record_outcome: Option<OperatorEvidenceRecordOutcomeV6>,
    pub last_record_error: Option<OperatorEvidenceRecordErrorV6>,
    pub window_epoch: u64,
    pub schema_generation: Option<u64>,
    pub recorded_reports: u64,
    pub pool_health: OperatorEvidencePoolHealthV6,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorDriverStatusV6 {
    pub scheduler_last_observed_tick: Option<u64>,
    pub scheduler_last_run_tick: Option<u64>,
    pub scheduler_gate: OperatorSchedulerGateV6,
    pub last_submitted_logical_tick: Option<u64>,
    pub tick_pending: bool,
    pub driver_tick_count: u64,
    pub scheduler_tick_count: u64,
    pub scheduler_ran_count: u64,
    pub scheduler_held_count: u64,
    pub scheduler_error_count: u64,
    pub last_orchestration_stop_reason: Option<OperatorOrchestrationStopReasonV6>,
    pub host_clock_exhausted: bool,
    pub counter_overflowed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorAdaptiveStatusV6 {
    pub mode: OperatorAdaptiveModeV6,
    pub feedback: OperatorFeedbackStatusV6,
    pub driver: Option<OperatorDriverStatusV6>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorStatusV6 {
    pub adaptive: Option<OperatorAdaptiveStatusV6>,
    pub physical_design: Option<OperatorPhysicalDesignStatusV6>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalIndexApplyStatusV6 {
    pub admission: OperatorPhysicalDesignMutationAdmissionModeV6,
    pub enabled: bool,
    pub runtime_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalColumnarApplyStatusV6 {
    pub snapshot_admission: OperatorPhysicalDesignMutationAdmissionModeV6,
    pub incremental_admission: OperatorPhysicalDesignMutationAdmissionModeV6,
    pub enabled: bool,
    pub allow_snapshot: bool,
    pub allow_incremental: bool,
    pub runtime_token: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalIndexApplyCapabilityV6 {
    pub enabled: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalColumnarApplyCapabilityV6 {
    pub enabled: bool,
    pub allow_snapshot: bool,
    pub allow_incremental: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorPhysicalDesignRecordOutcomeV6 {
    Recorded,
    SchemaRotated,
    RecordedWithCapacityRejection,
    SchemaRotatedWithCapacityRejection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorPhysicalDesignRecordErrorV6 {
    GlobalVisibilityRequired,
    StaleSchemaEvidence,
    OutOfOrderVisibility,
    EvidenceWindowEpochExhausted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalDesignDiagnosticsV6 {
    pub eligible_query_count: u64,
    pub record_success_count: u64,
    pub record_error_count: u64,
    pub schema_rotation_count: u64,
    pub capacity_rejection_count: u64,
    pub incomplete_report_count: u64,
    pub counter_overflowed: bool,
    pub last_record_outcome: Option<OperatorPhysicalDesignRecordOutcomeV6>,
    pub last_record_error: Option<OperatorPhysicalDesignRecordErrorV6>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalDesignEvidenceLimitsV6 {
    pub max_index_candidates: u64,
    pub max_columnar_candidates: u64,
    pub max_query_shapes_per_candidate: u64,
    pub max_columnar_columns_per_candidate: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalDesignEvidenceStatusV6 {
    pub limits: OperatorPhysicalDesignEvidenceLimitsV6,
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
pub struct OperatorPhysicalDesignStatusV6 {
    pub diagnostics: OperatorPhysicalDesignDiagnosticsV6,
    pub evidence: OperatorPhysicalDesignEvidenceStatusV6,
    pub physical_index_apply: OperatorPhysicalIndexApplyStatusV6,
    pub physical_columnar_apply: OperatorPhysicalColumnarApplyStatusV6,
    pub physical_design_mutation_receipts: OperatorPhysicalDesignMutationReceiptCapabilityV6,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalDesignEvidenceSummaryV6 {
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
pub enum OperatorPhysicalDesignNoActionReasonV6 {
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
pub enum OperatorPhysicalDesignDecisionV6 {
    Recommend {},
    NoAction {
        reason: OperatorPhysicalDesignNoActionReasonV6,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalIndexCandidateV6 {
    pub table_id: u64,
    pub column_id: u32,
    pub point_report_count: u64,
    pub range_report_count: u64,
    pub evidence: OperatorPhysicalDesignEvidenceSummaryV6,
    pub decision: OperatorPhysicalDesignDecisionV6,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalColumnarCandidateV6 {
    pub table_id: u64,
    pub columns: Vec<u32>,
    pub evidence: OperatorPhysicalDesignEvidenceSummaryV6,
    pub decision: OperatorPhysicalDesignDecisionV6,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalDesignAdvisorReportV6 {
    pub evidence_epoch: u64,
    pub schema_generation: u64,
    pub first_global_commit_seq: u64,
    pub last_global_commit_seq: u64,
    pub recorded_reports: u64,
    pub discarded_incomplete_reports: u64,
    pub overflowed: bool,
    pub incomplete: bool,
    pub index_candidates: Vec<OperatorPhysicalIndexCandidateV6>,
    pub columnar_candidates: Vec<OperatorPhysicalColumnarCandidateV6>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalDesignRecommendationsV6 {
    pub runtime_token: Option<String>,
    pub report: OperatorPhysicalDesignAdvisorReportV6,
}

impl std::ops::Deref for OperatorPhysicalDesignRecommendationsV6 {
    type Target = OperatorPhysicalDesignAdvisorReportV6;

    fn deref(&self) -> &Self::Target {
        &self.report
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
pub enum OperatorPhysicalIndexApplyOutcomeV6 {
    Created { index_id: u64 },
    AlreadyApplied { index_id: u64 },
    AlreadyCovered,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorPhysicalColumnarDesignModeV6 {
    Snapshot,
    Incremental,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum OperatorPhysicalColumnarApplyOutcomeV6 {
    Created { projection_id: u64 },
    AlreadyApplied { projection_id: u64 },
    AlreadyCovered,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalColumnarApplyResultV6 {
    pub table_id: u64,
    pub columns: Vec<u32>,
    pub mode: OperatorPhysicalColumnarDesignModeV6,
    pub placement_key: String,
    pub outcome: OperatorPhysicalColumnarApplyOutcomeV6,
    pub receipt: Option<OperatorPhysicalDesignMutationReceiptRefV6>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalIndexApplyResultV6 {
    pub table_id: u64,
    pub column_id: u32,
    pub index_name: String,
    pub outcome: OperatorPhysicalIndexApplyOutcomeV6,
    pub receipt: Option<OperatorPhysicalDesignMutationReceiptRefV6>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalDesignMutationReceiptRefV6 {
    pub journal_incarnation: String,
    pub receipt_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalDesignMutationReceiptCursorV6 {
    pub journal_incarnation: String,
    pub receipt_id: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalDesignMutationReceiptCapabilityV6 {
    pub read_enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalDesignMutationReceiptStatusV6 {
    pub journal_incarnation: String,
    pub recovery_required: bool,
    pub latest_receipt_id: Option<u64>,
    pub max_receipts_per_read: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorPhysicalDesignMutationReceiptSourceV6 {
    Programmatic,
    LocalOperator,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum OperatorPhysicalDesignMutationReceiptTargetV6 {
    Index {
        table_id: u64,
        column_id: u32,
        index_name: String,
    },
    Columnar {
        table_id: u64,
        columns: Vec<u32>,
        mode: OperatorPhysicalColumnarDesignModeV6,
        placement_key: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
pub enum OperatorPhysicalDesignMutationReceiptOutcomeV6 {
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
pub struct OperatorPhysicalDesignMutationReceiptV6 {
    pub receipt: OperatorPhysicalDesignMutationReceiptRefV6,
    pub source: OperatorPhysicalDesignMutationReceiptSourceV6,
    pub evidence_epoch: u64,
    pub target: OperatorPhysicalDesignMutationReceiptTargetV6,
    pub outcome: OperatorPhysicalDesignMutationReceiptOutcomeV6,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalDesignMutationReceiptPageV6 {
    pub journal_incarnation: String,
    pub receipts: Vec<OperatorPhysicalDesignMutationReceiptV6>,
    pub next_after: Option<OperatorPhysicalDesignMutationReceiptCursorV6>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalDesignRotationV6 {
    pub previous_epoch: u64,
    pub new_epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorEvidenceRotationV6 {
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

/// Status presentation only. Apply requests never accept component limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum OperatorPhysicalDesignMutationAdmissionConstraintV6 {
    Unconstrained {},
    AtMost { maximum: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalDesignMutationAdmissionPolicyV6 {
    pub source_work_units: OperatorPhysicalDesignMutationAdmissionConstraintV6,
    pub source_read_bytes: OperatorPhysicalDesignMutationAdmissionConstraintV6,
    pub prerequisite_work_units: OperatorPhysicalDesignMutationAdmissionConstraintV6,
    pub prerequisite_read_bytes: OperatorPhysicalDesignMutationAdmissionConstraintV6,
    pub prerequisite_write_bytes: OperatorPhysicalDesignMutationAdmissionConstraintV6,
    pub output_write_bytes: OperatorPhysicalDesignMutationAdmissionConstraintV6,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum OperatorPhysicalDesignMutationAdmissionModeV6 {
    Unadmitted {},
    ComponentLimits {
        policy: OperatorPhysicalDesignMutationAdmissionPolicyV6,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorPhysicalDesignMutationAdmissionDimensionV6 {
    SourceWorkUnits,
    SourceReadBytes,
    PrerequisiteWorkUnits,
    PrerequisiteReadBytes,
    PrerequisiteWriteBytes,
    OutputWriteBytes,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum OperatorPhysicalDesignMutationAdmissionRejectionV6 {
    RequiredBoundNotProven {
        dimension: OperatorPhysicalDesignMutationAdmissionDimensionV6,
    },
    LimitExceeded {
        dimension: OperatorPhysicalDesignMutationAdmissionDimensionV6,
        conservative_bound: u64,
        maximum: u64,
    },
    InspectionFailed {},
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorErrorCodeV6 {
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
    PhysicalDesignMutationAdmissionRejected,
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
pub struct OperatorRemoteErrorV6 {
    pub admission: Option<OperatorPhysicalDesignMutationAdmissionRejectionV6>,
    pub code: OperatorErrorCodeV6,
    pub message: String,
    pub receipt: Option<OperatorPhysicalDesignMutationReceiptRefV6>,
}

#[cfg(test)]
mod tests {
    use super::{
        OperatorErrorCodeV6, OperatorPhysicalDesignMutationAdmissionDimensionV6,
        OperatorPhysicalDesignMutationAdmissionRejectionV6,
        OperatorPhysicalDesignMutationReceiptRefV6, OperatorRemoteErrorV6,
    };

    fn frozen_admission_kind(
        rejection: OperatorPhysicalDesignMutationAdmissionRejectionV6,
    ) -> &'static str {
        match rejection {
            OperatorPhysicalDesignMutationAdmissionRejectionV6::RequiredBoundNotProven {
                ..
            } => "required_bound_not_proven",
            OperatorPhysicalDesignMutationAdmissionRejectionV6::LimitExceeded { .. } => {
                "limit_exceeded"
            }
            OperatorPhysicalDesignMutationAdmissionRejectionV6::InspectionFailed {} => {
                "inspection_failed"
            }
        }
    }

    #[test]
    fn frozen_nbop_v6_error_set_is_exact() {
        fn frozen_wire_name(code: OperatorErrorCodeV6) -> &'static str {
            match code {
                OperatorErrorCodeV6::AdaptiveNotEnabled => "adaptive_not_enabled",
                OperatorErrorCodeV6::DriverNotEnabled => "driver_not_enabled",
                OperatorErrorCodeV6::SchedulerNotFaulted => "scheduler_not_faulted",
                OperatorErrorCodeV6::EvidenceWindowChanged => "evidence_window_changed",
                OperatorErrorCodeV6::EvidenceWindowEpochExhausted => {
                    "evidence_window_epoch_exhausted"
                }
                OperatorErrorCodeV6::PhysicalDesignNotEnabled => "physical_design_not_enabled",
                OperatorErrorCodeV6::PhysicalDesignEvidenceEpochChanged => {
                    "physical_design_evidence_epoch_changed"
                }
                OperatorErrorCodeV6::PhysicalDesignEvidenceEpochExhausted => {
                    "physical_design_evidence_epoch_exhausted"
                }
                OperatorErrorCodeV6::PhysicalDesignNoEvidence => "physical_design_no_evidence",
                OperatorErrorCodeV6::PhysicalDesignStaleSchema => "physical_design_stale_schema",
                OperatorErrorCodeV6::PhysicalDesignInconclusiveCapacity => {
                    "physical_design_inconclusive_capacity"
                }
                OperatorErrorCodeV6::PhysicalIndexApplyNotEnabled => {
                    "physical_index_apply_not_enabled"
                }
                OperatorErrorCodeV6::PhysicalDesignRuntimeChanged => {
                    "physical_design_runtime_changed"
                }
                OperatorErrorCodeV6::InvalidIndexName => "invalid_index_name",
                OperatorErrorCodeV6::PhysicalIndexCandidateNotObserved => {
                    "physical_index_candidate_not_observed"
                }
                OperatorErrorCodeV6::PhysicalIndexNotRecommended => {
                    "physical_index_not_recommended"
                }
                OperatorErrorCodeV6::PhysicalIndexNameConflict => "physical_index_name_conflict",
                OperatorErrorCodeV6::PhysicalIndexApplyFailed => "physical_index_apply_failed",
                OperatorErrorCodeV6::PhysicalColumnarApplyNotEnabled => {
                    "physical_columnar_apply_not_enabled"
                }
                OperatorErrorCodeV6::InvalidPhysicalColumnarPlacementKey => {
                    "invalid_physical_columnar_placement_key"
                }
                OperatorErrorCodeV6::PhysicalColumnarModeNotAllowed => {
                    "physical_columnar_mode_not_allowed"
                }
                OperatorErrorCodeV6::PhysicalColumnarPlacementUnavailable => {
                    "physical_columnar_placement_unavailable"
                }
                OperatorErrorCodeV6::PhysicalColumnarPlacementOccupied => {
                    "physical_columnar_placement_occupied"
                }
                OperatorErrorCodeV6::PhysicalColumnarLocationConflict => {
                    "physical_columnar_location_conflict"
                }
                OperatorErrorCodeV6::PhysicalColumnarCandidateNotObserved => {
                    "physical_columnar_candidate_not_observed"
                }
                OperatorErrorCodeV6::PhysicalColumnarNotRecommended => {
                    "physical_columnar_not_recommended"
                }
                OperatorErrorCodeV6::PhysicalColumnarChangeStreamNotEnabled => {
                    "physical_columnar_change_stream_not_enabled"
                }
                OperatorErrorCodeV6::PhysicalColumnarChangeStreamUnavailable => {
                    "physical_columnar_change_stream_unavailable"
                }
                OperatorErrorCodeV6::PhysicalColumnarChangeStreamChanged => {
                    "physical_columnar_change_stream_changed"
                }
                OperatorErrorCodeV6::PhysicalColumnarRecoveryRequired => {
                    "physical_columnar_recovery_required"
                }
                OperatorErrorCodeV6::PhysicalColumnarApplyFailed => {
                    "physical_columnar_apply_failed"
                }
                OperatorErrorCodeV6::PhysicalDesignMutationOutcomeUncertain => {
                    "physical_design_mutation_outcome_uncertain"
                }
                OperatorErrorCodeV6::PhysicalDesignMutationReceiptCapacityExceeded => {
                    "physical_design_mutation_receipt_capacity_exceeded"
                }
                OperatorErrorCodeV6::PhysicalDesignMutationReceiptUnavailable => {
                    "physical_design_mutation_receipt_unavailable"
                }
                OperatorErrorCodeV6::PhysicalDesignMutationReceiptReadNotAllowed => {
                    "physical_design_mutation_receipt_read_not_allowed"
                }
                OperatorErrorCodeV6::PhysicalDesignMutationReceiptsNotEnabled => {
                    "physical_design_mutation_receipts_not_enabled"
                }
                OperatorErrorCodeV6::InvalidPhysicalDesignMutationReceiptCursor => {
                    "invalid_physical_design_mutation_receipt_cursor"
                }
                OperatorErrorCodeV6::PhysicalDesignMutationReceiptJournalChanged => {
                    "physical_design_mutation_receipt_journal_changed"
                }
                OperatorErrorCodeV6::InvalidPhysicalDesignMutationReceiptLimit => {
                    "invalid_physical_design_mutation_receipt_limit"
                }
                OperatorErrorCodeV6::PhysicalDesignMutationReceiptReadFailed => {
                    "physical_design_mutation_receipt_read_failed"
                }
                OperatorErrorCodeV6::ResponseTooLarge => "response_too_large",
                OperatorErrorCodeV6::ServerStopped => "server_stopped",
                OperatorErrorCodeV6::MalformedRequest => "malformed_request",
                OperatorErrorCodeV6::UnsupportedProtocolVersion => "unsupported_protocol_version",
                OperatorErrorCodeV6::RequestTooLarge => "request_too_large",
                OperatorErrorCodeV6::PhysicalDesignMutationAdmissionRejected => {
                    "physical_design_mutation_admission_rejected"
                }
                OperatorErrorCodeV6::Internal => "internal",
            }
        }

        let frozen_codes = [
            OperatorErrorCodeV6::AdaptiveNotEnabled,
            OperatorErrorCodeV6::DriverNotEnabled,
            OperatorErrorCodeV6::SchedulerNotFaulted,
            OperatorErrorCodeV6::EvidenceWindowChanged,
            OperatorErrorCodeV6::EvidenceWindowEpochExhausted,
            OperatorErrorCodeV6::PhysicalDesignNotEnabled,
            OperatorErrorCodeV6::PhysicalDesignEvidenceEpochChanged,
            OperatorErrorCodeV6::PhysicalDesignEvidenceEpochExhausted,
            OperatorErrorCodeV6::PhysicalDesignNoEvidence,
            OperatorErrorCodeV6::PhysicalDesignStaleSchema,
            OperatorErrorCodeV6::PhysicalDesignInconclusiveCapacity,
            OperatorErrorCodeV6::PhysicalIndexApplyNotEnabled,
            OperatorErrorCodeV6::PhysicalDesignRuntimeChanged,
            OperatorErrorCodeV6::InvalidIndexName,
            OperatorErrorCodeV6::PhysicalIndexCandidateNotObserved,
            OperatorErrorCodeV6::PhysicalIndexNotRecommended,
            OperatorErrorCodeV6::PhysicalIndexNameConflict,
            OperatorErrorCodeV6::PhysicalIndexApplyFailed,
            OperatorErrorCodeV6::PhysicalColumnarApplyNotEnabled,
            OperatorErrorCodeV6::InvalidPhysicalColumnarPlacementKey,
            OperatorErrorCodeV6::PhysicalColumnarModeNotAllowed,
            OperatorErrorCodeV6::PhysicalColumnarPlacementUnavailable,
            OperatorErrorCodeV6::PhysicalColumnarPlacementOccupied,
            OperatorErrorCodeV6::PhysicalColumnarLocationConflict,
            OperatorErrorCodeV6::PhysicalColumnarCandidateNotObserved,
            OperatorErrorCodeV6::PhysicalColumnarNotRecommended,
            OperatorErrorCodeV6::PhysicalColumnarChangeStreamNotEnabled,
            OperatorErrorCodeV6::PhysicalColumnarChangeStreamUnavailable,
            OperatorErrorCodeV6::PhysicalColumnarChangeStreamChanged,
            OperatorErrorCodeV6::PhysicalColumnarRecoveryRequired,
            OperatorErrorCodeV6::PhysicalColumnarApplyFailed,
            OperatorErrorCodeV6::PhysicalDesignMutationAdmissionRejected,
            OperatorErrorCodeV6::PhysicalDesignMutationOutcomeUncertain,
            OperatorErrorCodeV6::PhysicalDesignMutationReceiptCapacityExceeded,
            OperatorErrorCodeV6::PhysicalDesignMutationReceiptUnavailable,
            OperatorErrorCodeV6::PhysicalDesignMutationReceiptReadNotAllowed,
            OperatorErrorCodeV6::PhysicalDesignMutationReceiptsNotEnabled,
            OperatorErrorCodeV6::InvalidPhysicalDesignMutationReceiptCursor,
            OperatorErrorCodeV6::PhysicalDesignMutationReceiptJournalChanged,
            OperatorErrorCodeV6::InvalidPhysicalDesignMutationReceiptLimit,
            OperatorErrorCodeV6::PhysicalDesignMutationReceiptReadFailed,
            OperatorErrorCodeV6::ResponseTooLarge,
            OperatorErrorCodeV6::ServerStopped,
            OperatorErrorCodeV6::MalformedRequest,
            OperatorErrorCodeV6::UnsupportedProtocolVersion,
            OperatorErrorCodeV6::RequestTooLarge,
            OperatorErrorCodeV6::Internal,
        ];
        assert_eq!(frozen_codes.len(), 47);
        for code in frozen_codes {
            assert_eq!(
                serde_json::to_string(&code).unwrap(),
                format!("\"{}\"", frozen_wire_name(code))
            );
        }
    }

    #[test]
    fn frozen_nbop_v6_admission_set_is_exact_and_rejects_recovery_required() {
        assert_eq!(
            frozen_admission_kind(
                OperatorPhysicalDesignMutationAdmissionRejectionV6::RequiredBoundNotProven {
                    dimension: OperatorPhysicalDesignMutationAdmissionDimensionV6::OutputWriteBytes,
                },
            ),
            "required_bound_not_proven"
        );
        assert_eq!(
            frozen_admission_kind(
                OperatorPhysicalDesignMutationAdmissionRejectionV6::LimitExceeded {
                    dimension: OperatorPhysicalDesignMutationAdmissionDimensionV6::SourceReadBytes,
                    conservative_bound: 12_345,
                    maximum: 10_000,
                },
            ),
            "limit_exceeded"
        );
        assert_eq!(
            frozen_admission_kind(
                OperatorPhysicalDesignMutationAdmissionRejectionV6::InspectionFailed {},
            ),
            "inspection_failed"
        );
        assert!(
            serde_json::from_str::<OperatorPhysicalDesignMutationAdmissionRejectionV6>(
                r#"{"kind":"recovery_required"}"#,
            )
            .is_err()
        );
    }

    #[test]
    fn frozen_nbop_v6_remote_error_json_is_exact_with_nullable_receipt() {
        let without_receipt = OperatorRemoteErrorV6 {
            admission: Some(
                OperatorPhysicalDesignMutationAdmissionRejectionV6::InspectionFailed {},
            ),
            code: OperatorErrorCodeV6::PhysicalDesignMutationAdmissionRejected,
            message: "current mutation-work inspection failed before mutation".to_owned(),
            receipt: None,
        };
        assert_eq!(
            serde_json::to_string(&without_receipt).unwrap(),
            r#"{"admission":{"kind":"inspection_failed"},"code":"physical_design_mutation_admission_rejected","message":"current mutation-work inspection failed before mutation","receipt":null}"#
        );

        let with_receipt = OperatorRemoteErrorV6 {
            receipt: Some(OperatorPhysicalDesignMutationReceiptRefV6 {
                journal_incarnation: "00112233445566778899aabbccddeeff".to_owned(),
                receipt_id: 41,
            }),
            ..without_receipt
        };
        assert_eq!(
            serde_json::to_string(&with_receipt).unwrap(),
            r#"{"admission":{"kind":"inspection_failed"},"code":"physical_design_mutation_admission_rejected","message":"current mutation-work inspection failed before mutation","receipt":{"journal_incarnation":"00112233445566778899aabbccddeeff","receipt_id":41}}"#
        );
    }
}
