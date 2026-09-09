use std::error::Error;
use std::fmt;

use netbadb_planner::{PlanVariant, PlannerAccessKind, PlannerEstimateDirection};
use netbadb_rel::LogicalQueryShape;
use netbadb_types::{
    ColumnarGeneration, ColumnarProjectionId, DatabaseCommitSeq, SchemaGeneration, StorageId,
    TableId,
};

use crate::{AdaptiveError, Database, ExecutionFeedbackReport};

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
    pub access_kind: PlannerAccessKind,
    pub sample_count: u64,
    pub total_estimated_work_units: u64,
    pub total_actual_work_units: u64,
    pub total_absolute_error_work_units: u64,
    pub exact_samples: u64,
    pub underestimated_samples: u64,
    pub overestimated_samples: u64,
    pub overflowed: bool,
    pub incomplete: bool,
}

impl AggregatedCalibrationEvidence {
    fn new(access_kind: PlannerAccessKind) -> Self {
        Self {
            access_kind,
            sample_count: 0,
            total_estimated_work_units: 0,
            total_actual_work_units: 0,
            total_absolute_error_work_units: 0,
            exact_samples: 0,
            underestimated_samples: 0,
            overestimated_samples: 0,
            overflowed: false,
            incomplete: false,
        }
    }

    fn record(
        &mut self,
        estimated: u64,
        actual: u64,
        absolute_error: u64,
        direction: PlannerEstimateDirection,
    ) {
        let mut overflowed = false;
        overflowed |= checked_accumulate(&mut self.sample_count, 1);
        overflowed |= checked_accumulate(&mut self.total_estimated_work_units, estimated);
        overflowed |= checked_accumulate(&mut self.total_actual_work_units, actual);
        overflowed |= checked_accumulate(&mut self.total_absolute_error_work_units, absolute_error);
        let direction_total = match direction {
            PlannerEstimateDirection::Exact => Some(&mut self.exact_samples),
            PlannerEstimateDirection::Underestimated => Some(&mut self.underestimated_samples),
            PlannerEstimateDirection::Overestimated => Some(&mut self.overestimated_samples),
            PlannerEstimateDirection::ActualUnavailable => None,
        };
        if let Some(total) = direction_total {
            overflowed |= checked_accumulate(total, 1);
        }
        self.overflowed |= overflowed;
        self.incomplete |= overflowed;
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdaptivePlanVariantAggregate {
    pub plan_variant: PlanVariant,
    pub report_count: u64,
    pub target_query_samples: u64,
    pub target_access_count: u64,
    pub total_estimated_work_units: u64,
    pub total_actual_work_units: u64,
    pub total_source_alternative_work_units: u64,
    pub target_overflowed: bool,
    pub target_incomplete: bool,
    pub calibration: Vec<AggregatedCalibrationEvidence>,
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
    pub total_estimated_work_units: u64,
    pub total_actual_work_units: u64,
    pub total_source_alternative_work_units: u64,
    pub query_shapes: Vec<AdaptiveQueryShapeAggregate>,
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

fn record_calibration(group: &mut AdaptivePlanVariantAggregate, report: &ExecutionFeedbackReport) {
    for access in &report.accesses {
        let (Some(planner), Some(calibration)) = (&access.planner, access.calibration) else {
            continue;
        };
        let (Some(actual), Some(absolute_error)) = (
            calibration.actual_work_units,
            calibration.absolute_error_work_units,
        ) else {
            continue;
        };
        let index = group
            .calibration
            .iter()
            .position(|aggregate| aggregate.access_kind == planner.kind)
            .unwrap_or_else(|| {
                group
                    .calibration
                    .push(AggregatedCalibrationEvidence::new(planner.kind));
                group.calibration.len() - 1
            });
        group.calibration[index].record(
            calibration.estimated_work_units,
            actual,
            absolute_error,
            calibration.direction,
        );
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
    pub total_estimated_work_units: u64,
    pub total_actual_work_units: u64,
    pub total_source_alternative_work_units: u64,
    pub overflowed: bool,
    pub incomplete: bool,
    pub truncated: bool,
    pub outcome: AdaptiveWorkloadOutcome,
}

impl Database {
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
        if self.schema_generation() != target.schema_generation {
            return Ok(workload_report(
                window,
                AdaptiveWorkloadOutcome::StaleWindow(AdaptiveWorkloadStaleReason::SchemaChanged),
            ));
        }
        let Some(projection) = self
            .projections
            .iter()
            .find(|entry| entry.identity.id == target.projection_id)
            .and_then(|entry| entry.projection.as_ref())
        else {
            return Ok(workload_report(
                window,
                AdaptiveWorkloadOutcome::StaleWindow(
                    AdaptiveWorkloadStaleReason::TargetIdentityChanged,
                ),
            ));
        };
        let metadata = projection.metadata();
        if metadata.table_id != target.table_id || metadata.source_storage_id != target.storage_id {
            return Ok(workload_report(
                window,
                AdaptiveWorkloadOutcome::StaleWindow(
                    AdaptiveWorkloadStaleReason::TargetIdentityChanged,
                ),
            ));
        }
        if metadata.generation != target.generation {
            return Ok(workload_report(
                window,
                AdaptiveWorkloadOutcome::StaleWindow(
                    AdaptiveWorkloadStaleReason::TargetGenerationChanged,
                ),
            ));
        }

        let insufficient = window.overflowed
            || window.incomplete
            || window.truncated
            || window.total_samples < policy.minimum_samples
            || window.total_actual_work_units < policy.minimum_actual_work_units
            || window.distinct_visibility_points < policy.minimum_distinct_visibility_points;
        if insufficient {
            return Ok(workload_report(
                window,
                AdaptiveWorkloadOutcome::Inconclusive,
            ));
        }

        let suppressed = self
            .adaptive_runtime
            .is_suppressed(target.projection_id, target.generation);
        let actual = window.total_actual_work_units;
        let source = window.total_source_alternative_work_units;
        let outcome =
            if source >= actual && source - actual >= policy.minimum_keep_improvement_work_units {
                if suppressed {
                    AdaptiveWorkloadOutcome::HeldSuppressed
                } else {
                    AdaptiveWorkloadOutcome::ValidatedKeep
                }
            } else if actual > source
                && actual - source > policy.maximum_tolerated_regression_work_units
            {
                self.adaptive_runtime
                    .suppress(target.projection_id, target.generation);
                AdaptiveWorkloadOutcome::RevertedMeasuredRegression
            } else if suppressed {
                AdaptiveWorkloadOutcome::HeldSuppressed
            } else {
                AdaptiveWorkloadOutcome::HeldWithinHysteresisBand
            };
        Ok(workload_report(window, outcome))
    }
}

fn workload_report(
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
