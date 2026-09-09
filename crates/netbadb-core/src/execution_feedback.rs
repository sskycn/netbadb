use netbadb_executor::{
    ExecutionAccessKind, ExecutionAccessSample, ExecutionFilterSample, ExecutionStatistics,
};
use netbadb_planner::{
    PlanVariant, PlannerAccessEstimate, PlannerAccessKind, PlannerActualAccessEvidence,
    PlannerCalibrationEpoch, PlannerCalibrationSample, PlannerColumnarExecutionEvidence,
    evaluate_actual_access_work,
};
use netbadb_rel::LogicalQueryShape;
use netbadb_types::{
    ColumnarGeneration, ColumnarProjectionId, DatabaseCommitSeq, SchemaGeneration, StorageId,
    TableId,
};

use crate::{AdaptiveError, Database, DatabaseError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionFeedbackAnchor {
    pub global_commit_seq: Option<DatabaseCommitSeq>,
    pub schema_generation: SchemaGeneration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccessExecutionFeedback {
    pub planner: Option<PlannerAccessEstimate>,
    pub actual: ExecutionAccessSample,
    pub calibration: Option<PlannerCalibrationSample>,
}

/// One runtime-only query report. It is never written to a catalog, WAL, or
/// inspection contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionFeedbackReport {
    pub anchor: ExecutionFeedbackAnchor,
    pub calibration_epoch: PlannerCalibrationEpoch,
    pub query_shape: LogicalQueryShape,
    pub plan_variant: PlanVariant,
    pub accesses: Vec<AccessExecutionFeedback>,
    pub filters: Vec<ExecutionFilterSample>,
    pub overflowed: bool,
    pub incomplete: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionFeedbackPolicy {
    pub minimum_actual_samples: u64,
    pub minimum_actual_work_units: u64,
    pub maximum_allowed_regression_work_units: u64,
}

impl ExecutionFeedbackPolicy {
    #[must_use]
    pub const fn new(
        minimum_actual_samples: u64,
        minimum_actual_work_units: u64,
        maximum_allowed_regression_work_units: u64,
    ) -> Self {
        Self {
            minimum_actual_samples,
            minimum_actual_work_units,
            maximum_allowed_regression_work_units,
        }
    }
}

impl Default for ExecutionFeedbackPolicy {
    fn default() -> Self {
        Self::new(3, 1, 0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdaptiveExecutionFeedbackOutcome {
    ValidatedKeep,
    RevertedMeasuredRegression,
    Inconclusive,
    StaleFeedback,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdaptiveExecutionFeedbackReport {
    pub projection_id: ColumnarProjectionId,
    pub generation: ColumnarGeneration,
    pub table_id: TableId,
    pub storage_id: StorageId,
    pub sample_count: u64,
    pub total_estimated_work_units: u64,
    pub total_actual_work_units: u64,
    pub total_source_alternative_work_units: u64,
    pub overflowed: bool,
    pub incomplete: bool,
    pub outcome: AdaptiveExecutionFeedbackOutcome,
}

pub(crate) fn correlate_execution_feedback(
    anchor: ExecutionFeedbackAnchor,
    calibration_epoch: PlannerCalibrationEpoch,
    query_shape: LogicalQueryShape,
    plan_variant: PlanVariant,
    estimates: &[PlannerAccessEstimate],
    statistics: ExecutionStatistics,
) -> ExecutionFeedbackReport {
    let accesses = statistics
        .accesses
        .into_iter()
        .map(|actual| {
            let planner = estimates
                .iter()
                .find(|estimate| access_matches(estimate, &actual))
                .cloned();
            let calibration = planner.as_ref().and_then(|estimate| {
                let estimated = estimate.estimated_work_units?;
                let actual_work = evaluate_actual_access_work(estimate, planner_evidence(&actual));
                Some(PlannerCalibrationSample::with_effective(
                    estimated,
                    estimate.effective_work_units,
                    estimate.calibration_epoch,
                    actual_work,
                ))
            });
            AccessExecutionFeedback {
                planner,
                actual,
                calibration,
            }
        })
        .collect();
    ExecutionFeedbackReport {
        anchor,
        calibration_epoch,
        query_shape,
        plan_variant,
        accesses,
        filters: statistics.filters,
        overflowed: statistics.overflowed,
        incomplete: statistics.incomplete,
    }
}

fn access_matches(estimate: &PlannerAccessEstimate, actual: &ExecutionAccessSample) -> bool {
    estimate.node == actual.node
        && estimate.table_id == actual.table_id
        && estimate
            .storage_id
            .is_none_or(|storage_id| storage_id == actual.storage_id)
        && planner_kind(actual.kind) == estimate.kind
        && estimate.access_path == actual.access_path
}

fn planner_kind(kind: ExecutionAccessKind) -> PlannerAccessKind {
    match kind {
        ExecutionAccessKind::SeqScan => PlannerAccessKind::SeqScan,
        ExecutionAccessKind::IndexPoint => PlannerAccessKind::IndexPoint,
        ExecutionAccessKind::IndexRange => PlannerAccessKind::IndexRange,
        ExecutionAccessKind::Columnar => PlannerAccessKind::Columnar,
        ExecutionAccessKind::PartitionedSeqScan => PlannerAccessKind::PartitionedSeqScan,
        ExecutionAccessKind::PartitionedIndexPoint => PlannerAccessKind::PartitionedIndexPoint,
        ExecutionAccessKind::PartitionedIndexRange => PlannerAccessKind::PartitionedIndexRange,
    }
}

fn planner_evidence(actual: &ExecutionAccessSample) -> PlannerActualAccessEvidence {
    let columnar = actual
        .work
        .columnar
        .as_ref()
        .map(|columnar| PlannerColumnarExecutionEvidence {
            row_groups_read: columnar.scan.row_groups_read,
            rows_read: columnar.scan.rows_read,
            physical_block_reads: columnar.scan.physical_block_reads,
            physical_bytes_read: columnar.scan.physical_bytes_read,
            decoded_column_chunks: columnar.scan.decoded_column_chunks,
            decoded_version_blocks: columnar.scan.decoded_version_blocks,
            delta_segments: columnar.scan.delta_segments,
            delta_bytes_read: columnar.scan.delta_bytes_read,
            delta_mutations: columnar.scan.delta_mutations,
            delta_live_rows: columnar.scan.delta_live_rows,
            suppressed_version_rows: columnar.scan.base_rows_suppressed,
            merged_rows: columnar.scan.merged_rows,
        });
    PlannerActualAccessEvidence {
        rows_examined: actual.work.rows_examined,
        index_point_probes: actual.work.index_point_probes,
        index_range_probes: actual.work.index_range_probes,
        index_candidates_examined: actual.work.index_candidates_examined,
        columnar,
        incomplete: actual.work.incomplete,
    }
}

impl Database {
    /// Evaluates a slice of runtime samples for exactly one existing Columnar
    /// generation. The only mutation available to a regression outcome is
    /// runtime suppression of that derived generation.
    pub fn evaluate_adaptive_execution_feedback(
        &mut self,
        projection_id: ColumnarProjectionId,
        generation: ColumnarGeneration,
        feedback: &[ExecutionFeedbackReport],
        policy: ExecutionFeedbackPolicy,
    ) -> Result<AdaptiveExecutionFeedbackReport, AdaptiveError> {
        let projection = self
            .projections
            .iter()
            .find(|entry| entry.identity.id == projection_id)
            .and_then(|entry| entry.projection.as_ref())
            .ok_or(DatabaseError::ColumnarProjectionNotFound(projection_id))?;
        let metadata = projection.metadata();
        let table_id = metadata.table_id;
        let storage_id = metadata.source_storage_id;
        let current_snapshot = self
            .current_database_snapshot()?
            .ok_or(AdaptiveError::GlobalVisibilityRequired)?;
        let current_anchor = ExecutionFeedbackAnchor {
            global_commit_seq: Some(current_snapshot.commit_seq()),
            schema_generation: self.schema_generation(),
        };
        let stale = metadata.generation != generation
            || feedback
                .iter()
                .any(|report| report.anchor != current_anchor);
        if stale {
            return Ok(adaptive_report(
                projection_id,
                generation,
                table_id,
                storage_id,
                0,
                0,
                0,
                0,
                false,
                false,
                AdaptiveExecutionFeedbackOutcome::StaleFeedback,
            ));
        }

        let mut samples = 0_u64;
        let mut estimated = 0_u64;
        let mut actual = 0_u64;
        let mut alternative = 0_u64;
        let mut overflowed = false;
        let mut incomplete = false;
        for report in feedback {
            overflowed |= report.overflowed;
            incomplete |= report.incomplete;
            for access in &report.accesses {
                let Some(planner) = &access.planner else {
                    continue;
                };
                if planner.projection_id != Some(projection_id)
                    || planner.projection_generation != Some(generation)
                    || planner.table_id != table_id
                    || planner.storage_id != Some(storage_id)
                {
                    continue;
                }
                let Some(calibration) = access.calibration else {
                    incomplete = true;
                    continue;
                };
                let (Some(actual_work), Some(source_work)) = (
                    calibration.actual_work_units,
                    planner.source_alternative_work_units,
                ) else {
                    incomplete = true;
                    continue;
                };
                overflowed |= accumulate(&mut samples, 1);
                overflowed |= accumulate(&mut estimated, calibration.estimated_work_units);
                overflowed |= accumulate(&mut actual, actual_work);
                overflowed |= accumulate(&mut alternative, source_work);
            }
        }
        incomplete |= overflowed;
        let outcome = if overflowed
            || incomplete
            || samples < policy.minimum_actual_samples
            || actual < policy.minimum_actual_work_units
        {
            AdaptiveExecutionFeedbackOutcome::Inconclusive
        } else if actual > alternative.saturating_add(policy.maximum_allowed_regression_work_units)
        {
            self.adaptive_runtime.suppress(projection_id, generation);
            AdaptiveExecutionFeedbackOutcome::RevertedMeasuredRegression
        } else {
            self.adaptive_runtime.keep(projection_id, generation);
            AdaptiveExecutionFeedbackOutcome::ValidatedKeep
        };
        Ok(adaptive_report(
            projection_id,
            generation,
            table_id,
            storage_id,
            samples,
            estimated,
            actual,
            alternative,
            overflowed,
            incomplete,
            outcome,
        ))
    }
}

fn accumulate(total: &mut u64, value: u64) -> bool {
    match total.checked_add(value) {
        Some(sum) => {
            *total = sum;
            false
        }
        None => {
            *total = u64::MAX;
            true
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn adaptive_report(
    projection_id: ColumnarProjectionId,
    generation: ColumnarGeneration,
    table_id: TableId,
    storage_id: StorageId,
    sample_count: u64,
    total_estimated_work_units: u64,
    total_actual_work_units: u64,
    total_source_alternative_work_units: u64,
    overflowed: bool,
    incomplete: bool,
    outcome: AdaptiveExecutionFeedbackOutcome,
) -> AdaptiveExecutionFeedbackReport {
    AdaptiveExecutionFeedbackReport {
        projection_id,
        generation,
        table_id,
        storage_id,
        sample_count,
        total_estimated_work_units,
        total_actual_work_units,
        total_source_alternative_work_units,
        overflowed,
        incomplete,
        outcome,
    }
}
