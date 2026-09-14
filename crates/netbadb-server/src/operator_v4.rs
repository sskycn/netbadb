//! Frozen public Rust DTOs from NBOP v4. The current wire protocol remains v5.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorAdaptiveModeV4 {
    FeedbackOnly,
    Driven,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorEvidencePoolHealthV4 {
    Healthy,
    RotationRecommended,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorEvidenceRecordOutcomeV4 {
    Recorded,
    SchemaRotated,
    RecordedWithCapacityRejection,
    SchemaRotatedWithCapacityRejection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorEvidenceRecordErrorV4 {
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
pub enum OperatorSchedulerDelayClassV4 {
    Normal,
    Idle,
    NoProgress,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorEvidenceRenewalReasonV4 {
    ColumnarPhysicalStateChanged,
    ColumnarEligibilityChanged,
    AuthoritativeLsmLayoutChanged,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorSchedulerFaultV4 {
    MaintenanceEnvelopeExceeded,
    StepFailed,
    ConsumptionOverflow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum OperatorSchedulerGateV4 {
    Open {
        delay_class: OperatorSchedulerDelayClassV4,
    },
    AwaitingTrialProgress {
        window_epoch: u64,
        schema_generation: Option<u64>,
        recorded_reports: u64,
    },
    AwaitingEvidenceRenewal {
        blocked_window_epoch: u64,
        renewal_reason: OperatorEvidenceRenewalReasonV4,
    },
    Faulted {
        fault: OperatorSchedulerFaultV4,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorOrchestrationStopReasonV4 {
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
pub struct OperatorFeedbackStatusV4 {
    pub eligible_query_count: u64,
    pub record_success_count: u64,
    pub record_error_count: u64,
    pub capacity_rejection_count: u64,
    pub schema_rotation_count: u64,
    pub incomplete_report_count: u64,
    pub counter_overflowed: bool,
    pub last_record_outcome: Option<OperatorEvidenceRecordOutcomeV4>,
    pub last_record_error: Option<OperatorEvidenceRecordErrorV4>,
    pub window_epoch: u64,
    pub schema_generation: Option<u64>,
    pub recorded_reports: u64,
    pub pool_health: OperatorEvidencePoolHealthV4,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorDriverStatusV4 {
    pub scheduler_last_observed_tick: Option<u64>,
    pub scheduler_last_run_tick: Option<u64>,
    pub scheduler_gate: OperatorSchedulerGateV4,
    pub last_submitted_logical_tick: Option<u64>,
    pub tick_pending: bool,
    pub driver_tick_count: u64,
    pub scheduler_tick_count: u64,
    pub scheduler_ran_count: u64,
    pub scheduler_held_count: u64,
    pub scheduler_error_count: u64,
    pub last_orchestration_stop_reason: Option<OperatorOrchestrationStopReasonV4>,
    pub host_clock_exhausted: bool,
    pub counter_overflowed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorAdaptiveStatusV4 {
    pub mode: OperatorAdaptiveModeV4,
    pub feedback: OperatorFeedbackStatusV4,
    pub driver: Option<OperatorDriverStatusV4>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorStatusV4 {
    pub adaptive: Option<OperatorAdaptiveStatusV4>,
    pub physical_design: Option<OperatorPhysicalDesignStatusV4>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalIndexApplyStatusV4 {
    pub enabled: bool,
    pub runtime_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalColumnarApplyStatusV4 {
    pub enabled: bool,
    pub allow_snapshot: bool,
    pub allow_incremental: bool,
    pub runtime_token: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalIndexApplyCapabilityV4 {
    pub enabled: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalColumnarApplyCapabilityV4 {
    pub enabled: bool,
    pub allow_snapshot: bool,
    pub allow_incremental: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorPhysicalDesignRecordOutcomeV4 {
    Recorded,
    SchemaRotated,
    RecordedWithCapacityRejection,
    SchemaRotatedWithCapacityRejection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorPhysicalDesignRecordErrorV4 {
    GlobalVisibilityRequired,
    StaleSchemaEvidence,
    OutOfOrderVisibility,
    EvidenceWindowEpochExhausted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalDesignDiagnosticsV4 {
    pub eligible_query_count: u64,
    pub record_success_count: u64,
    pub record_error_count: u64,
    pub schema_rotation_count: u64,
    pub capacity_rejection_count: u64,
    pub incomplete_report_count: u64,
    pub counter_overflowed: bool,
    pub last_record_outcome: Option<OperatorPhysicalDesignRecordOutcomeV4>,
    pub last_record_error: Option<OperatorPhysicalDesignRecordErrorV4>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalDesignEvidenceLimitsV4 {
    pub max_index_candidates: u64,
    pub max_columnar_candidates: u64,
    pub max_query_shapes_per_candidate: u64,
    pub max_columnar_columns_per_candidate: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalDesignEvidenceStatusV4 {
    pub limits: OperatorPhysicalDesignEvidenceLimitsV4,
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
pub struct OperatorPhysicalDesignStatusV4 {
    pub diagnostics: OperatorPhysicalDesignDiagnosticsV4,
    pub evidence: OperatorPhysicalDesignEvidenceStatusV4,
    pub physical_index_apply: OperatorPhysicalIndexApplyStatusV4,
    pub physical_columnar_apply: OperatorPhysicalColumnarApplyStatusV4,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalDesignEvidenceSummaryV4 {
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
pub enum OperatorPhysicalDesignNoActionReasonV4 {
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
pub enum OperatorPhysicalDesignDecisionV4 {
    Recommend {},
    NoAction {
        reason: OperatorPhysicalDesignNoActionReasonV4,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalIndexCandidateV4 {
    pub table_id: u64,
    pub column_id: u32,
    pub point_report_count: u64,
    pub range_report_count: u64,
    pub evidence: OperatorPhysicalDesignEvidenceSummaryV4,
    pub decision: OperatorPhysicalDesignDecisionV4,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalColumnarCandidateV4 {
    pub table_id: u64,
    pub columns: Vec<u32>,
    pub evidence: OperatorPhysicalDesignEvidenceSummaryV4,
    pub decision: OperatorPhysicalDesignDecisionV4,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalDesignAdvisorReportV4 {
    pub evidence_epoch: u64,
    pub schema_generation: u64,
    pub first_global_commit_seq: u64,
    pub last_global_commit_seq: u64,
    pub recorded_reports: u64,
    pub discarded_incomplete_reports: u64,
    pub overflowed: bool,
    pub incomplete: bool,
    pub index_candidates: Vec<OperatorPhysicalIndexCandidateV4>,
    pub columnar_candidates: Vec<OperatorPhysicalColumnarCandidateV4>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalDesignRecommendationsV4 {
    pub runtime_token: Option<String>,
    pub report: OperatorPhysicalDesignAdvisorReportV4,
}

impl std::ops::Deref for OperatorPhysicalDesignRecommendationsV4 {
    type Target = OperatorPhysicalDesignAdvisorReportV4;

    fn deref(&self) -> &Self::Target {
        &self.report
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
pub enum OperatorPhysicalIndexApplyOutcomeV4 {
    Created { index_id: u64 },
    AlreadyApplied { index_id: u64 },
    AlreadyCovered,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorPhysicalColumnarDesignModeV4 {
    Snapshot,
    Incremental,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum OperatorPhysicalColumnarApplyOutcomeV4 {
    Created { projection_id: u64 },
    AlreadyApplied { projection_id: u64 },
    AlreadyCovered,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalColumnarApplyResultV4 {
    pub table_id: u64,
    pub columns: Vec<u32>,
    pub mode: OperatorPhysicalColumnarDesignModeV4,
    pub placement_key: String,
    pub outcome: OperatorPhysicalColumnarApplyOutcomeV4,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalIndexApplyResultV4 {
    pub table_id: u64,
    pub column_id: u32,
    pub index_name: String,
    pub outcome: OperatorPhysicalIndexApplyOutcomeV4,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalDesignRotationV4 {
    pub previous_epoch: u64,
    pub new_epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorEvidenceRotationV4 {
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
pub enum OperatorErrorCodeV4 {
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
    ResponseTooLarge,
    ServerStopped,
    MalformedRequest,
    UnsupportedProtocolVersion,
    RequestTooLarge,
    Internal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorRemoteErrorV4 {
    pub code: OperatorErrorCodeV4,
    pub message: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v4_public_apply_fixture_is_frozen() {
        let value = OperatorPhysicalIndexApplyResultV4 {
            table_id: 7,
            column_id: 3,
            index_name: "events_category_idx".into(),
            outcome: OperatorPhysicalIndexApplyOutcomeV4::Created { index_id: 11 },
        };
        let json = r#"{"table_id":7,"column_id":3,"index_name":"events_category_idx","outcome":{"outcome":"created","index_id":11}}"#;
        assert_eq!(serde_json::to_string(&value).unwrap(), json);
        assert_eq!(
            serde_json::from_str::<OperatorPhysicalIndexApplyResultV4>(json).unwrap(),
            value
        );
    }

    #[test]
    fn v4_public_error_fixture_is_frozen_without_v5_receipt_field() {
        let value = OperatorRemoteErrorV4 {
            code: OperatorErrorCodeV4::PhysicalColumnarApplyFailed,
            message: "apply failed".into(),
        };
        let json = r#"{"code":"physical_columnar_apply_failed","message":"apply failed"}"#;
        assert_eq!(serde_json::to_string(&value).unwrap(), json);
        assert_eq!(
            serde_json::from_str::<OperatorRemoteErrorV4>(json).unwrap(),
            value
        );
        assert!(serde_json::from_str::<OperatorRemoteErrorV4>(
            r#"{"code":"physical_columnar_apply_failed","message":"apply failed","receipt":null}"#
        )
        .is_err());
    }
}
