use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorAdaptiveModeV3 {
    FeedbackOnly,
    Driven,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorEvidencePoolHealthV3 {
    Healthy,
    RotationRecommended,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorEvidenceRecordOutcomeV3 {
    Recorded,
    SchemaRotated,
    RecordedWithCapacityRejection,
    SchemaRotatedWithCapacityRejection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorEvidenceRecordErrorV3 {
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
pub enum OperatorSchedulerDelayClassV3 {
    Normal,
    Idle,
    NoProgress,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorEvidenceRenewalReasonV3 {
    ColumnarPhysicalStateChanged,
    ColumnarEligibilityChanged,
    AuthoritativeLsmLayoutChanged,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorSchedulerFaultV3 {
    MaintenanceEnvelopeExceeded,
    StepFailed,
    ConsumptionOverflow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum OperatorSchedulerGateV3 {
    Open {
        delay_class: OperatorSchedulerDelayClassV3,
    },
    AwaitingTrialProgress {
        window_epoch: u64,
        schema_generation: Option<u64>,
        recorded_reports: u64,
    },
    AwaitingEvidenceRenewal {
        blocked_window_epoch: u64,
        renewal_reason: OperatorEvidenceRenewalReasonV3,
    },
    Faulted {
        fault: OperatorSchedulerFaultV3,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorOrchestrationStopReasonV3 {
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
pub struct OperatorFeedbackStatusV3 {
    pub eligible_query_count: u64,
    pub record_success_count: u64,
    pub record_error_count: u64,
    pub capacity_rejection_count: u64,
    pub schema_rotation_count: u64,
    pub incomplete_report_count: u64,
    pub counter_overflowed: bool,
    pub last_record_outcome: Option<OperatorEvidenceRecordOutcomeV3>,
    pub last_record_error: Option<OperatorEvidenceRecordErrorV3>,
    pub window_epoch: u64,
    pub schema_generation: Option<u64>,
    pub recorded_reports: u64,
    pub pool_health: OperatorEvidencePoolHealthV3,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorDriverStatusV3 {
    pub scheduler_last_observed_tick: Option<u64>,
    pub scheduler_last_run_tick: Option<u64>,
    pub scheduler_gate: OperatorSchedulerGateV3,
    pub last_submitted_logical_tick: Option<u64>,
    pub tick_pending: bool,
    pub driver_tick_count: u64,
    pub scheduler_tick_count: u64,
    pub scheduler_ran_count: u64,
    pub scheduler_held_count: u64,
    pub scheduler_error_count: u64,
    pub last_orchestration_stop_reason: Option<OperatorOrchestrationStopReasonV3>,
    pub host_clock_exhausted: bool,
    pub counter_overflowed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorAdaptiveStatusV3 {
    pub mode: OperatorAdaptiveModeV3,
    pub feedback: OperatorFeedbackStatusV3,
    pub driver: Option<OperatorDriverStatusV3>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorStatusV3 {
    pub adaptive: Option<OperatorAdaptiveStatusV3>,
    pub physical_design: Option<OperatorPhysicalDesignStatusV3>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalIndexApplyStatusV3 {
    pub enabled: bool,
    pub runtime_token: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorPhysicalDesignRecordOutcomeV3 {
    Recorded,
    SchemaRotated,
    RecordedWithCapacityRejection,
    SchemaRotatedWithCapacityRejection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorPhysicalDesignRecordErrorV3 {
    GlobalVisibilityRequired,
    StaleSchemaEvidence,
    OutOfOrderVisibility,
    EvidenceWindowEpochExhausted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalDesignDiagnosticsV3 {
    pub eligible_query_count: u64,
    pub record_success_count: u64,
    pub record_error_count: u64,
    pub schema_rotation_count: u64,
    pub capacity_rejection_count: u64,
    pub incomplete_report_count: u64,
    pub counter_overflowed: bool,
    pub last_record_outcome: Option<OperatorPhysicalDesignRecordOutcomeV3>,
    pub last_record_error: Option<OperatorPhysicalDesignRecordErrorV3>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalDesignEvidenceLimitsV3 {
    pub max_index_candidates: u64,
    pub max_columnar_candidates: u64,
    pub max_query_shapes_per_candidate: u64,
    pub max_columnar_columns_per_candidate: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalDesignEvidenceStatusV3 {
    pub limits: OperatorPhysicalDesignEvidenceLimitsV3,
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
pub struct OperatorPhysicalDesignStatusV3 {
    pub diagnostics: OperatorPhysicalDesignDiagnosticsV3,
    pub evidence: OperatorPhysicalDesignEvidenceStatusV3,
    pub physical_index_apply: OperatorPhysicalIndexApplyStatusV3,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalDesignEvidenceSummaryV3 {
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
pub enum OperatorPhysicalDesignNoActionReasonV3 {
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
pub enum OperatorPhysicalDesignDecisionV3 {
    Recommend {},
    NoAction {
        reason: OperatorPhysicalDesignNoActionReasonV3,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalIndexCandidateV3 {
    pub table_id: u64,
    pub column_id: u32,
    pub point_report_count: u64,
    pub range_report_count: u64,
    pub evidence: OperatorPhysicalDesignEvidenceSummaryV3,
    pub decision: OperatorPhysicalDesignDecisionV3,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalColumnarCandidateV3 {
    pub table_id: u64,
    pub columns: Vec<u32>,
    pub evidence: OperatorPhysicalDesignEvidenceSummaryV3,
    pub decision: OperatorPhysicalDesignDecisionV3,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalDesignAdvisorReportV3 {
    pub evidence_epoch: u64,
    pub schema_generation: u64,
    pub first_global_commit_seq: u64,
    pub last_global_commit_seq: u64,
    pub recorded_reports: u64,
    pub discarded_incomplete_reports: u64,
    pub overflowed: bool,
    pub incomplete: bool,
    pub index_candidates: Vec<OperatorPhysicalIndexCandidateV3>,
    pub columnar_candidates: Vec<OperatorPhysicalColumnarCandidateV3>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalDesignRecommendationsV3 {
    pub runtime_token: Option<String>,
    pub report: OperatorPhysicalDesignAdvisorReportV3,
}

impl std::ops::Deref for OperatorPhysicalDesignRecommendationsV3 {
    type Target = OperatorPhysicalDesignAdvisorReportV3;

    fn deref(&self) -> &Self::Target {
        &self.report
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
pub enum OperatorPhysicalIndexApplyOutcomeV3 {
    Created { index_id: u64 },
    AlreadyApplied { index_id: u64 },
    AlreadyCovered,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalIndexApplyResultV3 {
    pub table_id: u64,
    pub column_id: u32,
    pub index_name: String,
    pub outcome: OperatorPhysicalIndexApplyOutcomeV3,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorPhysicalDesignRotationV3 {
    pub previous_epoch: u64,
    pub new_epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorEvidenceRotationV3 {
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
pub enum OperatorErrorCodeV3 {
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
    ResponseTooLarge,
    ServerStopped,
    MalformedRequest,
    UnsupportedProtocolVersion,
    RequestTooLarge,
    Internal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorRemoteErrorV3 {
    pub code: OperatorErrorCodeV3,
    pub message: String,
}

#[cfg(test)]
mod tests {
    use std::any::TypeId;

    use super::*;

    fn phase_28_physical_design_status() -> serde_json::Value {
        serde_json::json!({
            "diagnostics": {
                "eligible_query_count": 0,
                "record_success_count": 0,
                "record_error_count": 0,
                "schema_rotation_count": 0,
                "capacity_rejection_count": 0,
                "incomplete_report_count": 0,
                "counter_overflowed": false,
                "last_record_outcome": null,
                "last_record_error": null
            },
            "evidence": {
                "limits": {
                    "max_index_candidates": 1,
                    "max_columnar_candidates": 1,
                    "max_query_shapes_per_candidate": 1,
                    "max_columnar_columns_per_candidate": 1
                },
                "epoch": 0,
                "schema_generation": null,
                "first_global_commit_seq": null,
                "last_global_commit_seq": null,
                "ordering_high_water": null,
                "recorded_reports": 0,
                "index_candidate_count": 0,
                "columnar_candidate_count": 0,
                "capacity_rejections": 0,
                "discarded_incomplete_reports": 0,
                "overflowed": false,
                "incomplete": false,
                "truncated": false
            },
            "physical_index_apply": {
                "enabled": false,
                "runtime_token": null
            }
        })
    }

    #[test]
    fn phase_28_v3_status_shape_is_frozen_and_distinct_from_current() {
        assert_ne!(
            TypeId::of::<OperatorStatusV3>(),
            TypeId::of::<crate::OperatorStatusV5>()
        );
        let value = phase_28_physical_design_status();
        let status: OperatorPhysicalDesignStatusV3 =
            serde_json::from_value(value.clone()).expect("Phase 28 V3 status must decode");
        let encoded = serde_json::to_value(status).expect("V3 status must encode");
        assert!(encoded.get("physical_columnar_apply").is_none());

        let mut phase_29_shape = value;
        phase_29_shape
            .as_object_mut()
            .expect("status object")
            .insert(
                "physical_columnar_apply".into(),
                serde_json::json!({
                    "enabled": true,
                    "allow_snapshot": true,
                    "allow_incremental": false,
                    "runtime_token": "00112233445566778899aabbccddeeff"
                }),
            );
        assert!(
            serde_json::from_value::<OperatorPhysicalDesignStatusV3>(phase_29_shape).is_err(),
            "V3 must reject the Phase 29 status field"
        );
    }

    #[test]
    fn phase_28_v3_error_codes_reject_phase_29_columnar_variants() {
        assert!(
            serde_json::from_str::<OperatorErrorCodeV3>("\"physical_columnar_apply_not_enabled\"")
                .is_err()
        );
        assert!(
            serde_json::from_str::<OperatorErrorCodeV3>("\"mutation_outcome_uncertain\"").is_err()
        );
    }
}
