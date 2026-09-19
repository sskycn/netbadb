//! Frozen public Rust DTOs from NBOP v5. Only v6 is accepted on the wire.

use serde::{Deserialize, Serialize};

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn historical_v5_dtos_preserve_exact_shapes_without_admission() {
        let error = OperatorRemoteErrorV5 {
            code: OperatorErrorCodeV5::Internal,
            message: "failed".into(),
            receipt: None,
        };
        assert_eq!(
            serde_json::to_value(&error).unwrap(),
            serde_json::json!({"code":"internal","message":"failed","receipt":null})
        );
        let index = OperatorPhysicalIndexApplyStatusV5 {
            enabled: true,
            runtime_token: Some("11".repeat(16)),
        };
        assert_eq!(
            serde_json::to_value(&index).unwrap(),
            serde_json::json!({"enabled":true,"runtime_token":"11".repeat(16)})
        );
        let columnar = OperatorPhysicalColumnarApplyStatusV5 {
            enabled: true,
            allow_snapshot: true,
            allow_incremental: false,
            runtime_token: None,
        };
        assert_eq!(
            serde_json::to_value(&columnar).unwrap(),
            serde_json::json!({"enabled":true,"allow_snapshot":true,"allow_incremental":false,"runtime_token":null})
        );
        let mut invalid = serde_json::to_value(error).unwrap();
        invalid["admission"] = serde_json::json!(null);
        assert!(serde_json::from_value::<OperatorRemoteErrorV5>(invalid).is_err());
        assert!(
            serde_json::from_value::<OperatorErrorCodeV5>(serde_json::json!(
                "physical_design_mutation_admission_rejected"
            ))
            .is_err()
        );
    }
}
