use crate::{AdaptiveError, Database};

pub use netbadb_query_feedback::{
    AdaptivePlanVariantAggregate, AdaptiveQueryShapeAggregate, AdaptiveWorkloadEvaluationReport,
    AdaptiveWorkloadLimits, AdaptiveWorkloadOutcome, AdaptiveWorkloadPolicy,
    AdaptiveWorkloadRecordError, AdaptiveWorkloadRecordOutcome, AdaptiveWorkloadStaleReason,
    AdaptiveWorkloadTarget, AdaptiveWorkloadWindow, AggregatedCalibrationEvidence,
    CalibrationVisibilityEvidence, record_calibration_report,
};

impl Database {
    pub(crate) fn adaptive_workload_stale_reason(
        &self,
        target: AdaptiveWorkloadTarget,
    ) -> Option<AdaptiveWorkloadStaleReason> {
        if self.schema_generation() != target.schema_generation {
            return Some(AdaptiveWorkloadStaleReason::SchemaChanged);
        }
        let Some(projection) = self
            .projections
            .iter()
            .find(|entry| entry.identity.id == target.projection_id)
            .and_then(|entry| entry.projection.as_ref())
        else {
            return Some(AdaptiveWorkloadStaleReason::TargetIdentityChanged);
        };
        let metadata = projection.metadata();
        if metadata.table_id != target.table_id || metadata.source_storage_id != target.storage_id {
            return Some(AdaptiveWorkloadStaleReason::TargetIdentityChanged);
        }
        (metadata.generation != target.generation)
            .then_some(AdaptiveWorkloadStaleReason::TargetGenerationChanged)
    }

    /// Applies deterministic workload hysteresis to one caller-owned window.
    /// Current G is intentionally not compared with historical sample Gs.
    pub fn evaluate_adaptive_workload(
        &mut self,
        window: &AdaptiveWorkloadWindow,
        policy: AdaptiveWorkloadPolicy,
    ) -> Result<AdaptiveWorkloadEvaluationReport, AdaptiveError> {
        self.current_database_snapshot()?
            .ok_or(AdaptiveError::GlobalVisibilityRequired)?;
        let target = window.target;
        let stale_reason = self.adaptive_workload_stale_reason(target);
        let suppressed = self
            .adaptive_runtime
            .is_suppressed(target.projection_id, target.generation);
        let report = netbadb_query_feedback::evaluate_workload_window(
            window,
            policy,
            suppressed,
            stale_reason,
        );
        if report.outcome == AdaptiveWorkloadOutcome::RevertedMeasuredRegression {
            self.adaptive_runtime
                .suppress(target.projection_id, target.generation);
        }
        Ok(report)
    }
}
