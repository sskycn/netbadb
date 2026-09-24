//! Typed, database-independent execution feedback correlation and evaluation.

use std::error::Error;
use std::fmt;

use netbadb_planner::{
    PlanNodeOrdinal, PlanVariant, PlannerAccessEstimate, PlannerAccessKind,
    PlannerActualAccessEvidence, PlannerCalibrationClass, PlannerCalibrationEpoch,
    PlannerCalibrationSample, PlannerColumnarExecutionEvidence, PlannerEstimateDirection,
    evaluate_actual_access_work,
};
use netbadb_rel::LogicalQueryShape;
use netbadb_storage_api::ColumnarScanStatistics;
use netbadb_types::{
    AccessPathId, ColumnarGeneration, ColumnarProjectionId, DatabaseCommitSeq, PartitionId,
    RelationBindingId, SchemaGeneration, StorageId, TableId,
};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ColumnarExecutionStatistics {
    pub projection_id: Option<ColumnarProjectionId>,
    pub generation: Option<ColumnarGeneration>,
    pub table_id: Option<TableId>,
    pub storage_id: Option<StorageId>,
    pub scan: ColumnarScanStatistics,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionAccessKind {
    SeqScan,
    IndexPoint,
    IndexRange,
    Columnar,
    PartitionedSeqScan,
    PartitionedIndexPoint,
    PartitionedIndexRange,
}

/// Raw counters measured by execution. They are deliberately not planner work
/// units and never cause query execution to fail.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExecutionWork {
    pub rows_examined: u64,
    pub rows_output: u64,
    pub filter_rows_evaluated: u64,
    pub filter_rows_passed: u64,
    pub filter_rows_rejected: u64,
    pub index_point_probes: u64,
    pub index_range_probes: u64,
    pub index_candidates_examined: u64,
    pub columnar: Option<ColumnarExecutionStatistics>,
    pub overflowed: bool,
    pub incomplete: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionAccessSample {
    pub node: PlanNodeOrdinal,
    pub binding_id: RelationBindingId,
    pub table_id: TableId,
    pub storage_id: StorageId,
    pub partition_id: Option<PartitionId>,
    pub kind: ExecutionAccessKind,
    pub access_path: Option<AccessPathId>,
    pub work: ExecutionWork,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionFilterSample {
    pub node: PlanNodeOrdinal,
    pub work: ExecutionWork,
}

/// Opt-in execution telemetry. Normal execution constructs none of these
/// vectors and retains its existing API and fast paths.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExecutionStatistics {
    pub accesses: Vec<ExecutionAccessSample>,
    pub filters: Vec<ExecutionFilterSample>,
    pub overflowed: bool,
    pub incomplete: bool,
}

impl ExecutionWork {
    /// Saturating counter update. Overflow degrades telemetry, never query results.
    pub fn add_rows_examined(&mut self, value: u64) {
        match self.rows_examined.checked_add(value) {
            Some(sum) => self.rows_examined = sum,
            None => {
                self.rows_examined = u64::MAX;
                self.overflowed = true;
                self.incomplete = true;
            }
        }
    }
}

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

pub fn correlate_execution_feedback(
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

/// Identity and current visibility supplied by the database owner for one assessment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColumnarFeedbackTarget {
    pub projection_id: ColumnarProjectionId,
    pub generation: ColumnarGeneration,
    pub current_generation: ColumnarGeneration,
    pub table_id: TableId,
    pub storage_id: StorageId,
    pub current_anchor: ExecutionFeedbackAnchor,
}

/// Pure assessment of runtime samples for one existing derived generation.
#[must_use]
pub fn evaluate_columnar_feedback(
    target: ColumnarFeedbackTarget,
    feedback: &[ExecutionFeedbackReport],
    policy: ExecutionFeedbackPolicy,
) -> AdaptiveExecutionFeedbackReport {
    let ColumnarFeedbackTarget {
        projection_id,
        generation,
        current_generation,
        table_id,
        storage_id,
        current_anchor,
    } = target;
    let stale = current_generation != generation
        || feedback
            .iter()
            .any(|report| report.anchor != current_anchor);
    if stale {
        return adaptive_report(
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
        );
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
    } else if actual > alternative.saturating_add(policy.maximum_allowed_regression_work_units) {
        AdaptiveExecutionFeedbackOutcome::RevertedMeasuredRegression
    } else {
        AdaptiveExecutionFeedbackOutcome::ValidatedKeep
    };
    adaptive_report(
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
    )
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

pub const MAX_CALIBRATION_EPOCH_CLASS_GROUPS: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdaptiveWorkloadTarget {
    pub table_id: TableId,
    pub storage_id: StorageId,
    pub projection_id: ColumnarProjectionId,
    pub generation: ColumnarGeneration,
    pub schema_generation: SchemaGeneration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdaptiveWorkloadLimits {
    pub max_query_shapes: u64,
    pub max_plan_variants_per_shape: u64,
}

impl AdaptiveWorkloadLimits {
    #[must_use]
    pub const fn new(max_query_shapes: u64, max_plan_variants_per_shape: u64) -> Self {
        Self {
            max_query_shapes,
            max_plan_variants_per_shape,
        }
    }
}

impl Default for AdaptiveWorkloadLimits {
    fn default() -> Self {
        Self::new(64, 8)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregatedCalibrationEvidence {
    pub calibration_class: PlannerCalibrationClass,
    pub calibration_epoch: PlannerCalibrationEpoch,
    pub sample_count: u64,
    pub total_base_estimated_work_units: u64,
    pub total_effective_estimated_work_units: u64,
    pub total_actual_work_units: u64,
    pub total_base_absolute_error_work_units: u64,
    pub total_effective_absolute_error_work_units: u64,
    pub base_exact_samples: u64,
    pub base_underestimated_samples: u64,
    pub base_overestimated_samples: u64,
    pub effective_exact_samples: u64,
    pub effective_underestimated_samples: u64,
    pub effective_overestimated_samples: u64,
    pub overflowed: bool,
    pub incomplete: bool,
}

impl AggregatedCalibrationEvidence {
    pub fn new(
        calibration_class: PlannerCalibrationClass,
        calibration_epoch: PlannerCalibrationEpoch,
    ) -> Self {
        Self {
            calibration_class,
            calibration_epoch,
            sample_count: 0,
            total_base_estimated_work_units: 0,
            total_effective_estimated_work_units: 0,
            total_actual_work_units: 0,
            total_base_absolute_error_work_units: 0,
            total_effective_absolute_error_work_units: 0,
            base_exact_samples: 0,
            base_underestimated_samples: 0,
            base_overestimated_samples: 0,
            effective_exact_samples: 0,
            effective_underestimated_samples: 0,
            effective_overestimated_samples: 0,
            overflowed: false,
            incomplete: false,
        }
    }

    fn record(&mut self, sample: PlannerCalibrationSample) {
        let (
            Some(effective),
            Some(actual),
            Some(base_absolute_error),
            Some(effective_absolute_error),
        ) = (
            sample.effective_estimated_work_units,
            sample.actual_work_units,
            sample.absolute_error_work_units,
            sample.effective_absolute_error_work_units,
        )
        else {
            self.incomplete = true;
            return;
        };
        let mut overflowed = false;
        overflowed |= checked_accumulate(&mut self.sample_count, 1);
        overflowed |= checked_accumulate(
            &mut self.total_base_estimated_work_units,
            sample.estimated_work_units,
        );
        overflowed |= checked_accumulate(&mut self.total_effective_estimated_work_units, effective);
        overflowed |= checked_accumulate(&mut self.total_actual_work_units, actual);
        overflowed |= checked_accumulate(
            &mut self.total_base_absolute_error_work_units,
            base_absolute_error,
        );
        overflowed |= checked_accumulate(
            &mut self.total_effective_absolute_error_work_units,
            effective_absolute_error,
        );
        overflowed |= record_direction(
            sample.direction,
            &mut self.base_exact_samples,
            &mut self.base_underestimated_samples,
            &mut self.base_overestimated_samples,
        );
        overflowed |= record_direction(
            sample.effective_direction,
            &mut self.effective_exact_samples,
            &mut self.effective_underestimated_samples,
            &mut self.effective_overestimated_samples,
        );
        self.overflowed |= overflowed;
        self.incomplete |= overflowed;
    }
}

fn record_direction(
    direction: PlannerEstimateDirection,
    exact: &mut u64,
    underestimated: &mut u64,
    overestimated: &mut u64,
) -> bool {
    let total = match direction {
        PlannerEstimateDirection::Exact => Some(exact),
        PlannerEstimateDirection::Underestimated => Some(underestimated),
        PlannerEstimateDirection::Overestimated => Some(overestimated),
        PlannerEstimateDirection::ActualUnavailable => None,
    };
    total.is_some_and(|total| checked_accumulate(total, 1))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CalibrationVisibilityEvidence {
    pub calibration_class: PlannerCalibrationClass,
    pub calibration_epoch: PlannerCalibrationEpoch,
    pub first_global_commit_seq: DatabaseCommitSeq,
    pub last_global_commit_seq: DatabaseCommitSeq,
    pub distinct_visibility_points: u64,
    pub overflowed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdaptivePlanVariantAggregate {
    pub plan_variant: PlanVariant,
    pub report_count: u64,
    pub target_query_samples: u64,
    pub target_access_count: u64,
    /// Phase 3 target base estimate; calibration never changes this meaning.
    pub total_estimated_work_units: u64,
    pub total_actual_work_units: u64,
    pub total_source_alternative_work_units: u64,
    pub target_overflowed: bool,
    pub target_incomplete: bool,
    pub calibration: Vec<AggregatedCalibrationEvidence>,
    pub calibration_truncated: bool,
}

impl AdaptivePlanVariantAggregate {
    fn new(plan_variant: PlanVariant) -> Self {
        Self {
            plan_variant,
            report_count: 0,
            target_query_samples: 0,
            target_access_count: 0,
            total_estimated_work_units: 0,
            total_actual_work_units: 0,
            total_source_alternative_work_units: 0,
            target_overflowed: false,
            target_incomplete: false,
            calibration: Vec::new(),
            calibration_truncated: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdaptiveQueryShapeAggregate {
    pub query_shape: LogicalQueryShape,
    pub plan_variants: Vec<AdaptivePlanVariantAggregate>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdaptiveWorkloadRecordOutcome {
    RecordedRelevant { target_access_count: u64 },
    RecordedNotRelevant,
    Truncated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdaptiveWorkloadRecordError {
    GlobalVisibilityRequired,
    SchemaChanged {
        expected: SchemaGeneration,
        actual: SchemaGeneration,
    },
    OutOfOrderVisibility {
        previous: DatabaseCommitSeq,
        received: DatabaseCommitSeq,
    },
}

impl fmt::Display for AdaptiveWorkloadRecordError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::GlobalVisibilityRequired => {
                formatter.write_str("adaptive workload samples require global visibility")
            }
            Self::SchemaChanged { expected, actual } => write!(
                formatter,
                "adaptive workload schema changed from generation {} to {}",
                expected.0, actual.0
            ),
            Self::OutOfOrderVisibility { previous, received } => write!(
                formatter,
                "adaptive workload visibility {} follows newer visibility {}",
                received.0, previous.0
            ),
        }
    }
}

impl Error for AdaptiveWorkloadRecordError {}

/// Caller-owned, runtime-only evidence window for one exact Columnar target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdaptiveWorkloadWindow {
    pub target: AdaptiveWorkloadTarget,
    pub limits: AdaptiveWorkloadLimits,
    pub first_global_commit_seq: Option<DatabaseCommitSeq>,
    pub last_global_commit_seq: Option<DatabaseCommitSeq>,
    pub distinct_visibility_points: u64,
    pub total_samples: u64,
    pub total_target_accesses: u64,
    /// Phase 3 target base estimate; retained for physical hysteresis.
    pub total_estimated_work_units: u64,
    pub total_actual_work_units: u64,
    pub total_source_alternative_work_units: u64,
    pub query_shapes: Vec<AdaptiveQueryShapeAggregate>,
    pub calibration_visibility: Vec<CalibrationVisibilityEvidence>,
    pub calibration_truncated: bool,
    pub overflowed: bool,
    pub incomplete: bool,
    pub truncated: bool,
    last_recorded_global_commit_seq: Option<DatabaseCommitSeq>,
    last_relevant_global_commit_seq: Option<DatabaseCommitSeq>,
}

impl AdaptiveWorkloadWindow {
    #[must_use]
    pub const fn new(target: AdaptiveWorkloadTarget, limits: AdaptiveWorkloadLimits) -> Self {
        Self {
            target,
            limits,
            first_global_commit_seq: None,
            last_global_commit_seq: None,
            distinct_visibility_points: 0,
            total_samples: 0,
            total_target_accesses: 0,
            total_estimated_work_units: 0,
            total_actual_work_units: 0,
            total_source_alternative_work_units: 0,
            query_shapes: Vec::new(),
            calibration_visibility: Vec::new(),
            calibration_truncated: false,
            overflowed: false,
            incomplete: false,
            truncated: false,
            last_recorded_global_commit_seq: None,
            last_relevant_global_commit_seq: None,
        }
    }

    /// Records one report in caller order. G is a timeline coordinate, not an
    /// expiration token; only schema mismatch rejects cross-report grouping.
    pub fn record(
        &mut self,
        report: &ExecutionFeedbackReport,
    ) -> Result<AdaptiveWorkloadRecordOutcome, AdaptiveWorkloadRecordError> {
        let global_commit_seq = report
            .anchor
            .global_commit_seq
            .ok_or(AdaptiveWorkloadRecordError::GlobalVisibilityRequired)?;
        if report.anchor.schema_generation != self.target.schema_generation {
            return Err(AdaptiveWorkloadRecordError::SchemaChanged {
                expected: self.target.schema_generation,
                actual: report.anchor.schema_generation,
            });
        }
        if let Some(previous) = self.last_recorded_global_commit_seq {
            if global_commit_seq < previous {
                return Err(AdaptiveWorkloadRecordError::OutOfOrderVisibility {
                    previous,
                    received: global_commit_seq,
                });
            }
        }

        let shape_index = self
            .query_shapes
            .iter()
            .position(|group| group.query_shape == report.query_shape);
        let shape_index = match shape_index {
            Some(index) => index,
            None => {
                if u64::try_from(self.query_shapes.len())
                    .map_or(true, |count| count >= self.limits.max_query_shapes)
                {
                    self.mark_truncated(global_commit_seq);
                    return Ok(AdaptiveWorkloadRecordOutcome::Truncated);
                }
                self.query_shapes.push(AdaptiveQueryShapeAggregate {
                    query_shape: report.query_shape.clone(),
                    plan_variants: Vec::new(),
                });
                self.query_shapes.len() - 1
            }
        };
        let variant_index = self.query_shapes[shape_index]
            .plan_variants
            .iter()
            .position(|group| group.plan_variant == report.plan_variant);
        let variant_index = match variant_index {
            Some(index) => index,
            None => {
                let variants = &mut self.query_shapes[shape_index].plan_variants;
                if u64::try_from(variants.len()).map_or(true, |count| {
                    count >= self.limits.max_plan_variants_per_shape
                }) {
                    self.mark_truncated(global_commit_seq);
                    return Ok(AdaptiveWorkloadRecordOutcome::Truncated);
                }
                variants.push(AdaptivePlanVariantAggregate::new(
                    report.plan_variant.clone(),
                ));
                variants.len() - 1
            }
        };

        self.last_recorded_global_commit_seq = Some(global_commit_seq);
        let target = collect_target_query_evidence(report, self.target);
        self.record_calibration_visibility(report, global_commit_seq);
        let group = &mut self.query_shapes[shape_index].plan_variants[variant_index];
        let report_count_overflow = checked_accumulate(&mut group.report_count, 1);
        if report_count_overflow {
            self.overflowed = true;
            self.incomplete = true;
        }
        record_calibration(group, report);

        if target.access_count == 0 {
            return Ok(AdaptiveWorkloadRecordOutcome::RecordedNotRelevant);
        }
        record_target_group(group, &target);
        self.record_target_totals(global_commit_seq, &target);
        Ok(AdaptiveWorkloadRecordOutcome::RecordedRelevant {
            target_access_count: target.access_count,
        })
    }

    fn record_calibration_visibility(
        &mut self,
        report: &ExecutionFeedbackReport,
        global_commit_seq: DatabaseCommitSeq,
    ) {
        record_calibration_visibility_parts(
            &mut self.calibration_visibility,
            &mut self.calibration_truncated,
            report,
            global_commit_seq,
            MAX_CALIBRATION_EPOCH_CLASS_GROUPS,
        );
    }

    fn mark_truncated(&mut self, global_commit_seq: DatabaseCommitSeq) {
        self.last_recorded_global_commit_seq = Some(global_commit_seq);
        self.truncated = true;
        self.incomplete = true;
    }

    fn record_target_totals(
        &mut self,
        global_commit_seq: DatabaseCommitSeq,
        target: &TargetQueryEvidence,
    ) {
        let mut overflowed = target.overflowed;
        overflowed |= checked_accumulate(&mut self.total_samples, 1);
        overflowed |= checked_accumulate(&mut self.total_target_accesses, target.access_count);
        overflowed |= checked_accumulate(
            &mut self.total_estimated_work_units,
            target.estimated_work_units,
        );
        overflowed |=
            checked_accumulate(&mut self.total_actual_work_units, target.actual_work_units);
        overflowed |= checked_accumulate(
            &mut self.total_source_alternative_work_units,
            target.source_alternative_work_units,
        );
        if self.first_global_commit_seq.is_none() {
            self.first_global_commit_seq = Some(global_commit_seq);
        }
        self.last_global_commit_seq = Some(global_commit_seq);
        if self.last_relevant_global_commit_seq != Some(global_commit_seq) {
            overflowed |= checked_accumulate(&mut self.distinct_visibility_points, 1);
            self.last_relevant_global_commit_seq = Some(global_commit_seq);
        }
        self.overflowed |= overflowed;
        self.incomplete |= target.incomplete || overflowed;
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct TargetQueryEvidence {
    access_count: u64,
    estimated_work_units: u64,
    actual_work_units: u64,
    source_alternative_work_units: u64,
    overflowed: bool,
    incomplete: bool,
}

fn collect_target_query_evidence(
    report: &ExecutionFeedbackReport,
    target: AdaptiveWorkloadTarget,
) -> TargetQueryEvidence {
    let mut evidence = TargetQueryEvidence::default();
    for access in &report.accesses {
        let Some(planner) = &access.planner else {
            continue;
        };
        if planner.projection_id != Some(target.projection_id)
            || planner.projection_generation != Some(target.generation)
            || planner.table_id != target.table_id
            || planner.storage_id != Some(target.storage_id)
        {
            continue;
        }
        let mut overflowed = checked_accumulate(&mut evidence.access_count, 1);
        let exact_actual_target = access.actual.table_id == target.table_id
            && access.actual.storage_id == target.storage_id
            && access
                .actual
                .work
                .columnar
                .as_ref()
                .is_some_and(|columnar| {
                    columnar.projection_id == Some(target.projection_id)
                        && columnar.generation == Some(target.generation)
                        && columnar.table_id == Some(target.table_id)
                        && columnar.storage_id == Some(target.storage_id)
                });
        let Some(calibration) = access.calibration else {
            evidence.incomplete = true;
            continue;
        };
        let (Some(actual), Some(alternative)) = (
            calibration.actual_work_units,
            planner.source_alternative_work_units,
        ) else {
            evidence.incomplete = true;
            continue;
        };
        if access.actual.work.incomplete || !exact_actual_target {
            evidence.incomplete = true;
            continue;
        }
        overflowed |= checked_accumulate(
            &mut evidence.estimated_work_units,
            calibration.estimated_work_units,
        );
        overflowed |= checked_accumulate(&mut evidence.actual_work_units, actual);
        overflowed |= checked_accumulate(&mut evidence.source_alternative_work_units, alternative);
        evidence.overflowed |= overflowed;
        evidence.incomplete |= overflowed;
    }
    evidence
}

fn record_target_group(group: &mut AdaptivePlanVariantAggregate, target: &TargetQueryEvidence) {
    let mut overflowed = target.overflowed;
    overflowed |= checked_accumulate(&mut group.target_query_samples, 1);
    overflowed |= checked_accumulate(&mut group.target_access_count, target.access_count);
    overflowed |= checked_accumulate(
        &mut group.total_estimated_work_units,
        target.estimated_work_units,
    );
    overflowed |= checked_accumulate(&mut group.total_actual_work_units, target.actual_work_units);
    overflowed |= checked_accumulate(
        &mut group.total_source_alternative_work_units,
        target.source_alternative_work_units,
    );
    group.target_overflowed |= overflowed;
    group.target_incomplete |= target.incomplete || overflowed;
}

pub fn record_calibration(
    group: &mut AdaptivePlanVariantAggregate,
    report: &ExecutionFeedbackReport,
) {
    for access in &report.accesses {
        let (Some(planner), Some(calibration)) = (&access.planner, access.calibration) else {
            continue;
        };
        let class = planner.kind.calibration_class();
        let index = group
            .calibration
            .iter()
            .position(|aggregate| {
                aggregate.calibration_class == class
                    && aggregate.calibration_epoch == calibration.calibration_epoch
            })
            .unwrap_or_else(|| {
                if group.calibration.len() >= MAX_CALIBRATION_EPOCH_CLASS_GROUPS {
                    group.calibration_truncated = true;
                    return group.calibration.len();
                }
                group.calibration.push(AggregatedCalibrationEvidence::new(
                    class,
                    calibration.calibration_epoch,
                ));
                group.calibration.len() - 1
            });
        if let Some(aggregate) = group.calibration.get_mut(index) {
            aggregate.record(calibration);
            aggregate.incomplete |= calibration.calibration_epoch != report.calibration_epoch;
        }
    }
}

/// Records one report into calibration-only Q/V storage. Phase 3 and the
/// Phase 6 global pool share this path so one report has one aggregation
/// meaning regardless of its number of adaptive targets.
pub fn record_calibration_report(
    query_shapes: &mut Vec<AdaptiveQueryShapeAggregate>,
    calibration_visibility: &mut Vec<CalibrationVisibilityEvidence>,
    calibration_truncated: &mut bool,
    limits: AdaptiveWorkloadLimits,
    report: &ExecutionFeedbackReport,
    global_commit_seq: DatabaseCommitSeq,
) -> bool {
    let shape_index = query_shapes
        .iter()
        .position(|group| group.query_shape == report.query_shape);
    let shape_index = match shape_index {
        Some(index) => index,
        None => {
            if u64::try_from(query_shapes.len())
                .map_or(true, |count| count >= limits.max_query_shapes)
            {
                *calibration_truncated = true;
                return false;
            }
            query_shapes.push(AdaptiveQueryShapeAggregate {
                query_shape: report.query_shape.clone(),
                plan_variants: Vec::new(),
            });
            query_shapes.len() - 1
        }
    };
    let variant_index = query_shapes[shape_index]
        .plan_variants
        .iter()
        .position(|group| group.plan_variant == report.plan_variant);
    let variant_index = match variant_index {
        Some(index) => index,
        None => {
            let variants = &mut query_shapes[shape_index].plan_variants;
            if u64::try_from(variants.len())
                .map_or(true, |count| count >= limits.max_plan_variants_per_shape)
            {
                *calibration_truncated = true;
                return false;
            }
            variants.push(AdaptivePlanVariantAggregate::new(
                report.plan_variant.clone(),
            ));
            variants.len() - 1
        }
    };

    record_calibration_visibility_parts(
        calibration_visibility,
        calibration_truncated,
        report,
        global_commit_seq,
        MAX_CALIBRATION_EPOCH_CLASS_GROUPS,
    );
    let group = &mut query_shapes[shape_index].plan_variants[variant_index];
    let overflowed = checked_accumulate(&mut group.report_count, 1);
    group.calibration_truncated |= overflowed;
    record_calibration(group, report);
    true
}

pub fn record_calibration_visibility_parts(
    calibration_visibility: &mut Vec<CalibrationVisibilityEvidence>,
    calibration_truncated: &mut bool,
    report: &ExecutionFeedbackReport,
    global_commit_seq: DatabaseCommitSeq,
    max_groups: usize,
) {
    let mut seen = Vec::new();
    for access in &report.accesses {
        let (Some(planner), Some(sample)) = (&access.planner, access.calibration) else {
            continue;
        };
        let key = (planner.kind.calibration_class(), sample.calibration_epoch);
        if seen.contains(&key) {
            continue;
        }
        seen.push(key);
        let index = calibration_visibility.iter().position(|evidence| {
            evidence.calibration_class == key.0 && evidence.calibration_epoch == key.1
        });
        match index {
            Some(index) => {
                let evidence = &mut calibration_visibility[index];
                if evidence.last_global_commit_seq != global_commit_seq {
                    let overflowed =
                        checked_accumulate(&mut evidence.distinct_visibility_points, 1);
                    evidence.overflowed |= overflowed;
                    evidence.last_global_commit_seq = global_commit_seq;
                }
            }
            None if calibration_visibility.len() < max_groups => {
                calibration_visibility.push(CalibrationVisibilityEvidence {
                    calibration_class: key.0,
                    calibration_epoch: key.1,
                    first_global_commit_seq: global_commit_seq,
                    last_global_commit_seq: global_commit_seq,
                    distinct_visibility_points: 1,
                    overflowed: false,
                });
            }
            None => *calibration_truncated = true,
        }
    }
}

fn checked_accumulate(total: &mut u64, value: u64) -> bool {
    let Some(sum) = total.checked_add(value) else {
        return true;
    };
    *total = sum;
    false
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdaptiveWorkloadPolicy {
    pub minimum_samples: u64,
    pub minimum_actual_work_units: u64,
    pub minimum_distinct_visibility_points: u64,
    pub minimum_keep_improvement_work_units: u64,
    pub maximum_tolerated_regression_work_units: u64,
}

impl AdaptiveWorkloadPolicy {
    #[must_use]
    pub const fn new(
        minimum_samples: u64,
        minimum_actual_work_units: u64,
        minimum_distinct_visibility_points: u64,
        minimum_keep_improvement_work_units: u64,
        maximum_tolerated_regression_work_units: u64,
    ) -> Self {
        Self {
            minimum_samples,
            minimum_actual_work_units,
            minimum_distinct_visibility_points,
            minimum_keep_improvement_work_units,
            maximum_tolerated_regression_work_units,
        }
    }
}

impl Default for AdaptiveWorkloadPolicy {
    fn default() -> Self {
        Self::new(3, 1, 2, 1, 0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdaptiveWorkloadStaleReason {
    SchemaChanged,
    TargetGenerationChanged,
    TargetIdentityChanged,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdaptiveWorkloadOutcome {
    ValidatedKeep,
    RevertedMeasuredRegression,
    HeldWithinHysteresisBand,
    HeldSuppressed,
    Inconclusive,
    StaleWindow(AdaptiveWorkloadStaleReason),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdaptiveWorkloadEvaluationReport {
    pub target: AdaptiveWorkloadTarget,
    pub sample_count: u64,
    pub distinct_visibility_points: u64,
    /// Phase 3 target base estimate, not the effective calibration overlay.
    pub total_estimated_work_units: u64,
    pub total_actual_work_units: u64,
    pub total_source_alternative_work_units: u64,
    pub overflowed: bool,
    pub incomplete: bool,
    pub truncated: bool,
    pub outcome: AdaptiveWorkloadOutcome,
}

pub fn workload_report(
    window: &AdaptiveWorkloadWindow,
    outcome: AdaptiveWorkloadOutcome,
) -> AdaptiveWorkloadEvaluationReport {
    AdaptiveWorkloadEvaluationReport {
        target: window.target,
        sample_count: window.total_samples,
        distinct_visibility_points: window.distinct_visibility_points,
        total_estimated_work_units: window.total_estimated_work_units,
        total_actual_work_units: window.total_actual_work_units,
        total_source_alternative_work_units: window.total_source_alternative_work_units,
        overflowed: window.overflowed,
        incomplete: window.incomplete,
        truncated: window.truncated,
        outcome,
    }
}

/// Evaluates one bounded caller-owned feedback window without mutating a database.
#[must_use]
pub fn evaluate_workload_window(
    window: &AdaptiveWorkloadWindow,
    policy: AdaptiveWorkloadPolicy,
    suppressed: bool,
    stale_reason: Option<AdaptiveWorkloadStaleReason>,
) -> AdaptiveWorkloadEvaluationReport {
    if let Some(reason) = stale_reason {
        return workload_report(window, AdaptiveWorkloadOutcome::StaleWindow(reason));
    }
    let insufficient = window.overflowed
        || window.incomplete
        || window.truncated
        || window.total_samples < policy.minimum_samples
        || window.total_actual_work_units < policy.minimum_actual_work_units
        || window.distinct_visibility_points < policy.minimum_distinct_visibility_points;
    if insufficient {
        return workload_report(window, AdaptiveWorkloadOutcome::Inconclusive);
    }
    let actual = window.total_actual_work_units;
    let source = window.total_source_alternative_work_units;
    let outcome = if source >= actual
        && source - actual >= policy.minimum_keep_improvement_work_units
    {
        if suppressed {
            AdaptiveWorkloadOutcome::HeldSuppressed
        } else {
            AdaptiveWorkloadOutcome::ValidatedKeep
        }
    } else if actual > source && actual - source > policy.maximum_tolerated_regression_work_units {
        AdaptiveWorkloadOutcome::RevertedMeasuredRegression
    } else if suppressed {
        AdaptiveWorkloadOutcome::HeldSuppressed
    } else {
        AdaptiveWorkloadOutcome::HeldWithinHysteresisBand
    };
    workload_report(window, outcome)
}

#[cfg(test)]
mod tests {
    use super::*;
    use netbadb_planner::PlannerWorkModel;

    fn anchor() -> ExecutionFeedbackAnchor {
        ExecutionFeedbackAnchor {
            global_commit_seq: Some(DatabaseCommitSeq(7)),
            schema_generation: SchemaGeneration(2),
        }
    }

    fn estimate() -> PlannerAccessEstimate {
        PlannerAccessEstimate {
            node: PlanNodeOrdinal(1),
            binding_id: RelationBindingId(1),
            table_id: TableId(1),
            storage_id: Some(StorageId(3)),
            kind: PlannerAccessKind::Columnar,
            access_path: None,
            projection_id: Some(ColumnarProjectionId(4)),
            projection_generation: Some(ColumnarGeneration(5)),
            estimated_work_units: Some(4),
            effective_work_units: None,
            calibration_epoch: PlannerCalibrationEpoch(6),
            source_alternative_work_units: Some(0),
            effective_source_alternative_work_units: None,
            work_model: PlannerWorkModel::Columnar {
                projected_column_count: 1,
            },
        }
    }

    fn report() -> ExecutionFeedbackReport {
        let estimate = estimate();
        let actual = ExecutionAccessSample {
            node: estimate.node,
            binding_id: estimate.binding_id,
            table_id: estimate.table_id,
            storage_id: StorageId(3),
            partition_id: None,
            kind: ExecutionAccessKind::Columnar,
            access_path: None,
            work: ExecutionWork {
                columnar: Some(ColumnarExecutionStatistics {
                    projection_id: Some(ColumnarProjectionId(4)),
                    generation: Some(ColumnarGeneration(5)),
                    table_id: Some(TableId(1)),
                    storage_id: Some(StorageId(3)),
                    scan: ColumnarScanStatistics {
                        rows_read: 10,
                        ..Default::default()
                    },
                }),
                ..Default::default()
            },
        };
        correlate_execution_feedback(
            anchor(),
            PlannerCalibrationEpoch(6),
            LogicalQueryShape::OneRow,
            PlanVariant::OneRow,
            &[estimate],
            ExecutionStatistics {
                accesses: vec![actual],
                ..Default::default()
            },
        )
    }

    fn assess(reports: &[ExecutionFeedbackReport]) -> AdaptiveExecutionFeedbackReport {
        evaluate_columnar_feedback(
            ColumnarFeedbackTarget {
                projection_id: ColumnarProjectionId(4),
                generation: ColumnarGeneration(5),
                current_generation: ColumnarGeneration(5),
                table_id: TableId(1),
                storage_id: StorageId(3),
                current_anchor: anchor(),
            },
            reports,
            ExecutionFeedbackPolicy::new(1, 1, 0),
        )
    }

    #[test]
    fn matches_typed_estimate_and_preserves_missing_actual() {
        let report = report();
        assert!(report.accesses[0].planner.is_some());
        assert!(report.accesses[0].calibration.is_some());
        let mut missing = report.clone();
        missing.accesses[0].actual.work.columnar = None;
        missing.accesses[0].calibration = None;
        assert_eq!(
            assess(&[missing]).outcome,
            AdaptiveExecutionFeedbackOutcome::Inconclusive
        );
        assert_eq!(assess(std::slice::from_ref(&report)).sample_count, 1);
        let mut wrong_storage = report;
        wrong_storage.accesses[0].actual.storage_id = StorageId(9);
        let unmatched = correlate_execution_feedback(
            anchor(),
            PlannerCalibrationEpoch(6),
            LogicalQueryShape::OneRow,
            PlanVariant::OneRow,
            &[estimate()],
            ExecutionStatistics {
                accesses: vec![wrong_storage.accesses.remove(0).actual],
                ..Default::default()
            },
        );
        assert!(unmatched.accesses[0].planner.is_none());
    }

    #[test]
    fn stale_and_overflow_are_inconclusive_or_rejected() {
        let report = report();
        assert_eq!(
            assess(std::slice::from_ref(&report)).outcome,
            AdaptiveExecutionFeedbackOutcome::RevertedMeasuredRegression
        );
        let mut overflow = report.clone();
        overflow.overflowed = true;
        assert_eq!(
            assess(&[overflow]).outcome,
            AdaptiveExecutionFeedbackOutcome::Inconclusive
        );
        let mut stale = report;
        stale.anchor.schema_generation = SchemaGeneration(3);
        assert_eq!(
            assess(&[stale]).outcome,
            AdaptiveExecutionFeedbackOutcome::StaleFeedback
        );
    }

    #[test]
    fn window_groups_typed_shapes_and_rejects_stale_or_missing_evidence() {
        let target = AdaptiveWorkloadTarget {
            table_id: TableId(1),
            storage_id: StorageId(3),
            projection_id: ColumnarProjectionId(4),
            generation: ColumnarGeneration(5),
            schema_generation: SchemaGeneration(2),
        };
        let mut window = AdaptiveWorkloadWindow::new(target, AdaptiveWorkloadLimits::new(2, 2));
        let policy = AdaptiveWorkloadPolicy::new(1, 1, 1, 1, 0);
        assert_eq!(
            evaluate_workload_window(&window, policy, false, None).outcome,
            AdaptiveWorkloadOutcome::Inconclusive
        );
        let one = report();
        assert!(matches!(
            window.record(&one),
            Ok(AdaptiveWorkloadRecordOutcome::RecordedRelevant { .. })
        ));
        assert_eq!(window.query_shapes.len(), 1);
        assert_eq!(window.total_samples, 1);
        let stale = evaluate_workload_window(
            &window,
            policy,
            false,
            Some(AdaptiveWorkloadStaleReason::TargetGenerationChanged),
        );
        assert_eq!(
            stale.outcome,
            AdaptiveWorkloadOutcome::StaleWindow(
                AdaptiveWorkloadStaleReason::TargetGenerationChanged
            )
        );
        let mut wrong_schema = one;
        wrong_schema.anchor.schema_generation = SchemaGeneration(3);
        assert!(matches!(
            window.record(&wrong_schema),
            Err(AdaptiveWorkloadRecordError::SchemaChanged { .. })
        ));
        window.overflowed = true;
        assert_eq!(
            evaluate_workload_window(&window, policy, false, None).outcome,
            AdaptiveWorkloadOutcome::Inconclusive
        );
    }
}
