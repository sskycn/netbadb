//! Physical planning kept separate from logical relational meaning.

use std::cmp::Ordering;

use netbadb_index::{IndexBound, IndexRange, IndexStatistics, TableStatistics, compare_values};
use netbadb_rel::{
    AggregateInput, AggregateOutput, Assignment, BinaryOp, ColumnRef, Expr, ExprKind, JoinKind,
    LogicalPlan, LogicalStatement, OutputField, ProjectedExpr, SortKey,
};
use netbadb_types::{
    AccessPathId, ColumnId, ColumnarGeneration, ColumnarProjectionId, PartitionId,
    RelationBindingId, ScalarValue, StorageId, TableId,
};

mod calibration;
#[cfg(test)]
mod calibration_integration_tests;
use calibration::effective_planning_cost;
pub use calibration::{
    CalibrationRatio, CalibrationRatioError, PlannerCalibrationClass, PlannerCalibrationEpoch,
    PlannerCalibrationProfile, apply_calibration_ratio,
};
mod plan_variant;
pub use plan_variant::{
    PlanIndexBoundShape, PlanIndexRangeShape, PlanPartitionAccessVariant, PlanPartitionVariant,
    PlanScalarShape, PlanVariant, PlanVariantError,
};

/// Executable operations advertised by one physical access path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AccessPathCapabilities {
    pub point_lookup: bool,
    pub range_lookup: bool,
    /// Results are ordered by the access column and stable row identity.
    pub ordered: bool,
}

/// Storage-neutral integer costs supplied by an access method.
///
/// Storage-supplied weights in neutral integer planning-work units.
///
/// They share the scale of a table's managed sequential-page cost; they are
/// not elapsed time. The point base is fixed access-method work, point I/O is
/// expected candidate-source reads, range startup precedes returned
/// candidates, and the sequential unit weights each returned candidate row.
/// Engines may derive the weights from trees, levels, filters, or another
/// persistent layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AccessCostHints {
    pub point_probe_base_cost: u32,
    pub expected_point_io: u32,
    pub range_startup_cost: u32,
    pub sequential_unit_cost: u32,
}

/// One registered single-column access capability available to physical planning.
///
/// Callers preserve registration order in the slice. The planner receives
/// immutable domain snapshots and an opaque table-scoped identity, never
/// storage objects, B+Tree handles, catalog pages, or future engine internals.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccessPath {
    pub table_id: TableId,
    pub column_id: ColumnId,
    pub id: AccessPathId,
    pub capabilities: AccessPathCapabilities,
    pub statistics: Option<IndexStatistics>,
    pub cost_hints: Option<AccessCostHints>,
}

/// Optional optimizer snapshot for one table visible to physical planning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableAccessStatistics {
    pub table_id: TableId,
    pub statistics: Option<TableStatistics>,
}

/// Immutable, storage-independent optimizer input for one physical partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionPlanningSnapshot {
    pub partition_id: PartitionId,
    pub storage_id: StorageId,
    pub lower: Option<ScalarValue>,
    pub upper: Option<ScalarValue>,
    pub statistics: Option<TableStatistics>,
    pub access_paths: Vec<AccessPath>,
}

/// Exact range metadata for one logical table. Bounds are canonical and
/// ordered by the core before planning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeTablePlanningSnapshot {
    pub table_id: TableId,
    pub partition_key: ColumnId,
    pub partitions: Vec<PartitionPlanningSnapshot>,
}

/// Immutable optimizer metadata for one validated, fresh derived projection.
/// Core supplies only projections whose table, source storage, schema,
/// generation, token, and transaction context are eligible.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnarProjectionPlanningSnapshot {
    pub projection_id: ColumnarProjectionId,
    pub generation: ColumnarGeneration,
    pub table_id: TableId,
    pub source_storage_id: StorageId,
    pub projected_columns: Vec<ColumnId>,
    pub row_count: u64,
    pub row_group_count: u64,
    pub segment_bytes: u64,
    pub delta_segment_count: u64,
    pub delta_bytes: u64,
    pub delta_mutation_count: u64,
    pub delta_live_row_count: u64,
    pub suppressed_version_count: u64,
    pub row_groups: Vec<ColumnarRowGroupPlanningSnapshot>,
}

/// Storage-neutral work comparison for one concrete columnar projection.
///
/// This is immutable planner evidence, not a maintenance recommendation. Core
/// may use it to evaluate an already-existing derived projection without
/// copying the planner's cost formula or allowing planning to mutate storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColumnarPlanningCost {
    pub source_work_units: u64,
    pub projection_work_units: u64,
}

/// Deterministic, runtime-only identity assigned by physical-plan preorder.
///
/// Ordinals are meaningful only with the exact plan that produced them. They
/// are deliberately neither persistent identities nor pointer-derived values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PlanNodeOrdinal(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlannerAccessKind {
    SeqScan,
    IndexPoint,
    IndexRange,
    Columnar,
    PartitionedSeqScan,
    PartitionedIndexPoint,
    PartitionedIndexRange,
}

impl PlannerAccessKind {
    #[must_use]
    pub const fn calibration_class(self) -> PlannerCalibrationClass {
        match self {
            Self::SeqScan | Self::PartitionedSeqScan => PlannerCalibrationClass::SeqScan,
            Self::IndexPoint | Self::PartitionedIndexPoint => PlannerCalibrationClass::IndexPoint,
            Self::IndexRange | Self::PartitionedIndexRange => PlannerCalibrationClass::IndexRange,
            Self::Columnar => PlannerCalibrationClass::Columnar,
        }
    }
}

/// Parameters retained from the canonical planning formula so actual evidence
/// can be evaluated in the same unit system without consulting newer stats.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlannerWorkModel {
    SequentialRows,
    Index {
        startup_work_units: u64,
        candidate_work_units: u64,
    },
    Columnar {
        projected_column_count: u64,
    },
}

/// Planning-time evidence for one selected physical access operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannerAccessEstimate {
    pub node: PlanNodeOrdinal,
    pub binding_id: RelationBindingId,
    pub table_id: TableId,
    pub storage_id: Option<StorageId>,
    pub kind: PlannerAccessKind,
    pub access_path: Option<AccessPathId>,
    pub projection_id: Option<ColumnarProjectionId>,
    pub projection_generation: Option<ColumnarGeneration>,
    /// Uncalibrated estimate produced by the canonical base cost model.
    pub estimated_work_units: Option<u64>,
    /// Calibration overlay applied to `estimated_work_units` for this plan.
    pub effective_work_units: Option<u64>,
    pub calibration_epoch: PlannerCalibrationEpoch,
    /// Selected Columnar scans retain the authoritative source alternative
    /// used at planning time. Other access kinds have no Phase 2 alternative.
    pub source_alternative_work_units: Option<u64>,
    /// Calibrated diagnostic counterpart. Phase 2/3 physical regression
    /// continues to use the uncalibrated source alternative above.
    pub effective_source_alternative_work_units: Option<u64>,
    pub work_model: PlannerWorkModel,
}

/// Storage-independent actual evidence accepted by the planner's pure work
/// evaluator. These are raw counters, not work units.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PlannerActualAccessEvidence {
    pub rows_examined: u64,
    pub index_point_probes: u64,
    pub index_range_probes: u64,
    pub index_candidates_examined: u64,
    pub columnar: Option<PlannerColumnarExecutionEvidence>,
    pub incomplete: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PlannerColumnarExecutionEvidence {
    pub row_groups_read: u64,
    pub rows_read: u64,
    pub physical_block_reads: u64,
    pub physical_bytes_read: u64,
    pub decoded_column_chunks: u64,
    pub decoded_version_blocks: u64,
    pub delta_segments: u64,
    pub delta_bytes_read: u64,
    pub delta_mutations: u64,
    pub delta_live_rows: u64,
    pub suppressed_version_rows: u64,
    pub merged_rows: u64,
}

/// One estimate/actual pair. Signed arithmetic is represented without lossy
/// casts: direction and absolute error remain correct over the full `u64`
/// domain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlannerCalibrationSample {
    /// Base model estimate; this preserves the Phase 2 field semantics.
    pub estimated_work_units: u64,
    pub effective_estimated_work_units: Option<u64>,
    pub calibration_epoch: PlannerCalibrationEpoch,
    pub actual_work_units: Option<u64>,
    /// Direction and error of the base estimate.
    pub direction: PlannerEstimateDirection,
    pub absolute_error_work_units: Option<u64>,
    pub effective_direction: PlannerEstimateDirection,
    pub effective_absolute_error_work_units: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlannerEstimateDirection {
    Exact,
    Underestimated,
    Overestimated,
    ActualUnavailable,
}

impl PlannerCalibrationSample {
    #[must_use]
    pub fn new(estimated_work_units: u64, actual_work_units: Option<u64>) -> Self {
        Self::with_effective(
            estimated_work_units,
            Some(estimated_work_units),
            PlannerCalibrationEpoch(0),
            actual_work_units,
        )
    }

    #[must_use]
    pub fn with_effective(
        estimated_work_units: u64,
        effective_estimated_work_units: Option<u64>,
        calibration_epoch: PlannerCalibrationEpoch,
        actual_work_units: Option<u64>,
    ) -> Self {
        let (direction, absolute_error_work_units) =
            estimate_error(estimated_work_units, actual_work_units);
        let (effective_direction, effective_absolute_error_work_units) =
            match effective_estimated_work_units {
                Some(effective) => estimate_error(effective, actual_work_units),
                None => (PlannerEstimateDirection::ActualUnavailable, None),
            };
        Self {
            estimated_work_units,
            effective_estimated_work_units,
            calibration_epoch,
            actual_work_units,
            direction,
            absolute_error_work_units,
            effective_direction,
            effective_absolute_error_work_units,
        }
    }
}

fn estimate_error(
    estimated_work_units: u64,
    actual_work_units: Option<u64>,
) -> (PlannerEstimateDirection, Option<u64>) {
    match actual_work_units {
        None => (PlannerEstimateDirection::ActualUnavailable, None),
        Some(actual) if actual == estimated_work_units => {
            (PlannerEstimateDirection::Exact, Some(0))
        }
        Some(actual) if actual > estimated_work_units => (
            PlannerEstimateDirection::Underestimated,
            Some(actual - estimated_work_units),
        ),
        Some(actual) => (
            PlannerEstimateDirection::Overestimated,
            Some(estimated_work_units - actual),
        ),
    }
}

impl ColumnarPlanningCost {
    #[must_use]
    pub const fn benefit_work_units(self) -> u64 {
        self.source_work_units
            .saturating_sub(self.projection_work_units)
    }

    #[must_use]
    pub const fn projection_is_preferred(self) -> bool {
        self.projection_work_units <= self.source_work_units
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnarRowGroupPlanningSnapshot {
    pub rows: u32,
    pub columns: Vec<ColumnarZoneMapPlanningSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnarZoneMapPlanningSnapshot {
    pub column_id: ColumnId,
    pub null_count: u64,
    pub minimum: Option<ScalarValue>,
    pub maximum: Option<ScalarValue>,
    pub encoded_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionAccessPlan {
    SeqScan,
    IndexScan {
        index_column: ColumnRef,
        access_path: AccessPathId,
        key: ScalarValue,
    },
    RangeIndexScan {
        index_column: ColumnRef,
        access_path: AccessPathId,
        range: IndexRange,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionScanPlan {
    pub partition_id: PartitionId,
    pub storage_id: StorageId,
    pub access: PartitionAccessPlan,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PhysicalPlan {
    OneRow,
    SeqScan {
        binding_id: RelationBindingId,
        table_id: TableId,
        table_name: String,
        columns: Vec<ColumnRef>,
    },
    ColumnarScan {
        binding_id: RelationBindingId,
        table_id: TableId,
        table_name: String,
        columns: Vec<ColumnRef>,
        projection_id: ColumnarProjectionId,
        generation: ColumnarGeneration,
        source_storage_id: StorageId,
    },
    IndexScan {
        binding_id: RelationBindingId,
        table_id: TableId,
        table_name: String,
        columns: Vec<ColumnRef>,
        index_column: ColumnRef,
        access_path: AccessPathId,
        key: ScalarValue,
    },
    RangeIndexScan {
        binding_id: RelationBindingId,
        table_id: TableId,
        table_name: String,
        columns: Vec<ColumnRef>,
        index_column: ColumnRef,
        access_path: AccessPathId,
        range: IndexRange,
    },
    PartitionedScan {
        binding_id: RelationBindingId,
        table_id: TableId,
        table_name: String,
        columns: Vec<ColumnRef>,
        partition_key: ColumnId,
        total_partitions: usize,
        partitions: Vec<PartitionScanPlan>,
    },
    NestedLoopJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        kind: JoinKind,
        predicate: Expr,
        columns: Vec<ColumnRef>,
    },
    IndexNestedLoopJoin {
        left: Box<PhysicalPlan>,
        right_binding_id: RelationBindingId,
        right_table_id: TableId,
        right_table_name: String,
        right_columns: Vec<ColumnRef>,
        kind: JoinKind,
        left_key: ColumnRef,
        right_key: ColumnRef,
        right_access_path: AccessPathId,
        predicate: Expr,
        columns: Vec<ColumnRef>,
    },
    HashJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        kind: JoinKind,
        left_key: ColumnRef,
        right_key: ColumnRef,
        predicate: Expr,
        columns: Vec<ColumnRef>,
    },
    Filter {
        input: Box<PhysicalPlan>,
        predicate: Expr,
    },
    Sort {
        input: Box<PhysicalPlan>,
        keys: Vec<SortKey>,
    },
    Project {
        input: Box<PhysicalPlan>,
        columns: Vec<ColumnRef>,
    },
    ScalarProject {
        input: Box<PhysicalPlan>,
        expressions: Vec<ProjectedExpr>,
    },
    Aggregate {
        input: Box<PhysicalPlan>,
        group_keys: Vec<ColumnRef>,
        outputs: Vec<AggregateOutput>,
    },
    Limit {
        input: Box<PhysicalPlan>,
        limit: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PhysicalStatement {
    Query(PhysicalPlan),
    Insert {
        table_id: TableId,
        table_name: String,
        values: Vec<Expr>,
    },
    Update {
        input: PhysicalPlan,
        table_id: TableId,
        assignments: Vec<Assignment>,
    },
    Delete {
        input: PhysicalPlan,
        table_id: TableId,
    },
}

impl PhysicalPlan {
    #[must_use]
    pub fn output_fields(&self) -> Vec<OutputField> {
        match self {
            Self::OneRow => Vec::new(),
            Self::SeqScan { columns, .. }
            | Self::ColumnarScan { columns, .. }
            | Self::IndexScan { columns, .. }
            | Self::RangeIndexScan { columns, .. }
            | Self::PartitionedScan { columns, .. }
            | Self::NestedLoopJoin { columns, .. }
            | Self::IndexNestedLoopJoin { columns, .. }
            | Self::HashJoin { columns, .. }
            | Self::Project { columns, .. } => {
                columns.iter().cloned().map(OutputField::Source).collect()
            }
            Self::Aggregate { outputs, .. } => {
                outputs.iter().map(AggregateOutput::output_field).collect()
            }
            Self::ScalarProject { expressions, .. } => expressions
                .iter()
                .map(|expression| OutputField::Derived(expression.output.clone()))
                .collect(),
            Self::Filter { input, .. } | Self::Sort { input, .. } | Self::Limit { input, .. } => {
                input.output_fields()
            }
        }
    }
}

#[must_use]
pub fn plan(logical: &netbadb_rel::LogicalPlan) -> PhysicalPlan {
    plan_with_access_paths(logical, &[])
}

/// Selects physical operators from logical meaning and an ordered snapshot of
/// registered single-column access paths.
#[must_use]
pub fn plan_with_access_paths(logical: &LogicalPlan, access_paths: &[AccessPath]) -> PhysicalPlan {
    plan_with_statistics(logical, &[], access_paths)
}

/// Selects physical operators using ordered access paths and optional explicit
/// `ANALYZE` snapshots. Missing table statistics preserve the Phase 4E rule.
#[must_use]
pub fn plan_with_statistics(
    logical: &LogicalPlan,
    table_statistics: &[TableAccessStatistics],
    access_paths: &[AccessPath],
) -> PhysicalPlan {
    plan_with_partition_snapshots(logical, table_statistics, access_paths, &[])
}

/// Plans one logical relation while treating range partitioning strictly as a
/// physical concern. Exact bounds drive pruning before each selected
/// partition performs its own access-path choice.
#[must_use]
pub fn plan_with_partition_snapshots(
    logical: &LogicalPlan,
    table_statistics: &[TableAccessStatistics],
    access_paths: &[AccessPath],
    range_tables: &[RangeTablePlanningSnapshot],
) -> PhysicalPlan {
    plan_with_partition_snapshots_and_calibration(
        logical,
        table_statistics,
        access_paths,
        range_tables,
        &PlannerCalibrationProfile::IDENTITY,
    )
}

/// Calibration-aware variant. The immutable profile is used exactly once at
/// each canonical access-cost comparison.
#[must_use]
pub fn plan_with_partition_snapshots_and_calibration(
    logical: &LogicalPlan,
    table_statistics: &[TableAccessStatistics],
    access_paths: &[AccessPath],
    range_tables: &[RangeTablePlanningSnapshot],
    calibration: &PlannerCalibrationProfile,
) -> PhysicalPlan {
    let raw = plan_raw_with_statistics(
        logical,
        table_statistics,
        access_paths,
        range_tables,
        calibration,
    );
    let required = raw
        .output_fields()
        .into_iter()
        .filter_map(|field| field.source_column().map(SourceIdentity::from))
        .collect::<Vec<_>>();
    prune_required_columns(raw, &required)
}

/// Plans a query with columnar projections kept separate from access paths.
#[must_use]
pub fn plan_with_columnar_snapshots(
    logical: &LogicalPlan,
    table_statistics: &[TableAccessStatistics],
    access_paths: &[AccessPath],
    range_tables: &[RangeTablePlanningSnapshot],
    projections: &[ColumnarProjectionPlanningSnapshot],
) -> PhysicalPlan {
    plan_with_columnar_snapshots_and_calibration(
        logical,
        table_statistics,
        access_paths,
        range_tables,
        projections,
        &PlannerCalibrationProfile::IDENTITY,
    )
}

#[must_use]
pub fn plan_with_columnar_snapshots_and_calibration(
    logical: &LogicalPlan,
    table_statistics: &[TableAccessStatistics],
    access_paths: &[AccessPath],
    range_tables: &[RangeTablePlanningSnapshot],
    projections: &[ColumnarProjectionPlanningSnapshot],
    calibration: &PlannerCalibrationProfile,
) -> PhysicalPlan {
    let plan = plan_with_partition_snapshots_and_calibration(
        logical,
        table_statistics,
        access_paths,
        range_tables,
        calibration,
    );
    select_columnar_scans(plan, table_statistics, projections, calibration, true, &[])
}

/// Evaluates one existing projection with the same storage-neutral work model
/// used by physical scan selection. The projection's complete declared column
/// set is used because Adaptive Operations Phase 1 has no workload model.
#[must_use]
pub fn evaluate_columnar_projection_cost(
    statistics: Option<TableStatistics>,
    projection: &ColumnarProjectionPlanningSnapshot,
) -> ColumnarPlanningCost {
    ColumnarPlanningCost {
        source_work_units: source_scan_work_units(statistics, projection.row_count),
        projection_work_units: columnar_work_units(projection, &projection.projected_columns, &[]),
    }
}

#[derive(Debug, Clone)]
struct ColumnarPlanningConstraint {
    column_id: ColumnId,
    lower: Option<(ScalarValue, bool)>,
    upper: Option<(ScalarValue, bool)>,
}

fn select_columnar_scans(
    plan: PhysicalPlan,
    table_statistics: &[TableAccessStatistics],
    projections: &[ColumnarProjectionPlanningSnapshot],
    calibration: &PlannerCalibrationProfile,
    eligible_context: bool,
    constraints: &[ColumnarPlanningConstraint],
) -> PhysicalPlan {
    match plan {
        PhysicalPlan::SeqScan {
            binding_id,
            table_id,
            table_name,
            columns,
        } if eligible_context => {
            let required = columns
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>();
            let source_statistics = table_statistics
                .iter()
                .find(|entry| entry.table_id == table_id)
                .and_then(|entry| entry.statistics);
            let fallback_rows = projections
                .iter()
                .filter(|projection| projection.table_id == table_id)
                .map(|projection| projection.row_count)
                .max()
                .unwrap_or(0);
            let source_work = source_scan_work_units(source_statistics, fallback_rows);
            let effective_source_work = effective_planning_cost(
                u128::from(source_work),
                calibration.ratio(PlannerCalibrationClass::SeqScan),
            );
            let selected = projections
                .iter()
                .filter(|projection| {
                    projection.table_id == table_id
                        && required
                            .iter()
                            .all(|column| projection.projected_columns.contains(column))
                })
                .min_by_key(|projection| {
                    effective_planning_cost(
                        u128::from(columnar_work_units(projection, &required, constraints)),
                        calibration.ratio(PlannerCalibrationClass::Columnar),
                    )
                });
            if let Some(projection) = selected {
                let columnar_work = columnar_work_units(projection, &required, constraints);
                let effective_columnar_work = effective_planning_cost(
                    u128::from(columnar_work),
                    calibration.ratio(PlannerCalibrationClass::Columnar),
                );
                if effective_columnar_work <= effective_source_work {
                    return PhysicalPlan::ColumnarScan {
                        binding_id,
                        table_id,
                        table_name,
                        columns,
                        projection_id: projection.projection_id,
                        generation: projection.generation,
                        source_storage_id: projection.source_storage_id,
                    };
                }
            }
            PhysicalPlan::SeqScan {
                binding_id,
                table_id,
                table_name,
                columns,
            }
        }
        PhysicalPlan::Filter { input, predicate } => PhysicalPlan::Filter {
            input: Box::new({
                let mut pushed = constraints.to_vec();
                collect_columnar_constraints(&predicate, &mut pushed);
                select_columnar_scans(
                    *input,
                    table_statistics,
                    projections,
                    calibration,
                    eligible_context,
                    &pushed,
                )
            }),
            predicate,
        },
        PhysicalPlan::Project { input, columns } => PhysicalPlan::Project {
            input: Box::new(select_columnar_scans(
                *input,
                table_statistics,
                projections,
                calibration,
                eligible_context,
                constraints,
            )),
            columns,
        },
        PhysicalPlan::ScalarProject { input, expressions } => PhysicalPlan::ScalarProject {
            input: Box::new(select_columnar_scans(
                *input,
                table_statistics,
                projections,
                calibration,
                eligible_context,
                constraints,
            )),
            expressions,
        },
        PhysicalPlan::Aggregate {
            input,
            group_keys,
            outputs,
        } => PhysicalPlan::Aggregate {
            input: Box::new(select_columnar_scans(
                *input,
                table_statistics,
                projections,
                calibration,
                eligible_context,
                constraints,
            )),
            group_keys,
            outputs,
        },
        PhysicalPlan::Limit { input, limit } => PhysicalPlan::Limit {
            input: Box::new(select_columnar_scans(
                *input,
                table_statistics,
                projections,
                calibration,
                eligible_context,
                constraints,
            )),
            limit,
        },
        PhysicalPlan::Sort { input, keys } => PhysicalPlan::Sort {
            input: Box::new(select_columnar_scans(
                *input,
                table_statistics,
                projections,
                calibration,
                false,
                &[],
            )),
            keys,
        },
        PhysicalPlan::NestedLoopJoin {
            left,
            right,
            kind,
            predicate,
            columns,
        } => PhysicalPlan::NestedLoopJoin {
            left: Box::new(select_columnar_scans(
                *left,
                table_statistics,
                projections,
                calibration,
                false,
                &[],
            )),
            right: Box::new(select_columnar_scans(
                *right,
                table_statistics,
                projections,
                calibration,
                false,
                &[],
            )),
            kind,
            predicate,
            columns,
        },
        PhysicalPlan::HashJoin {
            left,
            right,
            kind,
            left_key,
            right_key,
            predicate,
            columns,
        } => PhysicalPlan::HashJoin {
            left: Box::new(select_columnar_scans(
                *left,
                table_statistics,
                projections,
                calibration,
                false,
                &[],
            )),
            right: Box::new(select_columnar_scans(
                *right,
                table_statistics,
                projections,
                calibration,
                false,
                &[],
            )),
            kind,
            left_key,
            right_key,
            predicate,
            columns,
        },
        PhysicalPlan::IndexNestedLoopJoin {
            left,
            right_binding_id,
            right_table_id,
            right_table_name,
            right_columns,
            kind,
            left_key,
            right_key,
            right_access_path,
            predicate,
            columns,
        } => PhysicalPlan::IndexNestedLoopJoin {
            left: Box::new(select_columnar_scans(
                *left,
                table_statistics,
                projections,
                calibration,
                false,
                &[],
            )),
            right_binding_id,
            right_table_id,
            right_table_name,
            right_columns,
            kind,
            left_key,
            right_key,
            right_access_path,
            predicate,
            columns,
        },
        other => other,
    }
}

fn source_scan_work_units(statistics: Option<TableStatistics>, fallback_rows: u64) -> u64 {
    match statistics {
        Some(statistics) => statistics.managed_page_count.max(1),
        None => 1_u64.saturating_add(fallback_rows / 32),
    }
}

fn columnar_work_units(
    projection: &ColumnarProjectionPlanningSnapshot,
    required: &[ColumnId],
    constraints: &[ColumnarPlanningConstraint],
) -> u64 {
    let mut selected_groups = 0_u64;
    let mut selected_rows = 0_u64;
    let mut required_bytes = 0_u64;
    for group in &projection.row_groups {
        if constraints
            .iter()
            .any(|constraint| planning_group_cannot_match(group, constraint))
        {
            continue;
        }
        selected_groups = selected_groups.saturating_add(1);
        selected_rows = selected_rows.saturating_add(u64::from(group.rows));
        required_bytes = required_bytes.saturating_add(
            group
                .columns
                .iter()
                .filter(|column| required.contains(&column.column_id))
                .fold(0_u64, |bytes, column| {
                    bytes.saturating_add(column.encoded_bytes)
                }),
        );
    }
    let required_columns = u64::try_from(required.len()).unwrap_or(u64::MAX);
    let decoded_values = selected_rows.saturating_mul(required_columns);
    // Startup, row-group dispatch, encoded byte pages, and vector value work
    // all use the same storage-neutral integer work units as managed scans.
    2_u64
        .saturating_add(selected_groups)
        .saturating_add(required_bytes.div_ceil(4096))
        .saturating_add(decoded_values.div_ceil(256))
        .saturating_add(projection.delta_segment_count)
        .saturating_add(projection.delta_bytes.div_ceil(4096))
        .saturating_add(projection.delta_mutation_count.div_ceil(256))
        .saturating_add(
            projection
                .delta_live_row_count
                .saturating_add(projection.suppressed_version_count)
                .div_ceil(256),
        )
}

/// Captures access estimates from the same immutable snapshots used to create
/// `plan`. Callers must invoke this before execution and retain the result;
/// this function intentionally has no access to storage or mutable statistics.
#[must_use]
pub fn estimate_execution_accesses(
    plan: &PhysicalPlan,
    table_statistics: &[TableAccessStatistics],
    access_paths: &[AccessPath],
    range_tables: &[RangeTablePlanningSnapshot],
    projections: &[ColumnarProjectionPlanningSnapshot],
) -> Vec<PlannerAccessEstimate> {
    estimate_execution_accesses_with_calibration(
        plan,
        table_statistics,
        access_paths,
        range_tables,
        projections,
        &PlannerCalibrationProfile::IDENTITY,
    )
}

#[must_use]
pub fn estimate_execution_accesses_with_calibration(
    plan: &PhysicalPlan,
    table_statistics: &[TableAccessStatistics],
    access_paths: &[AccessPath],
    range_tables: &[RangeTablePlanningSnapshot],
    projections: &[ColumnarProjectionPlanningSnapshot],
    calibration: &PlannerCalibrationProfile,
) -> Vec<PlannerAccessEstimate> {
    let mut estimates = Vec::new();
    let mut next_node = 0_u32;
    collect_access_estimates(
        plan,
        table_statistics,
        access_paths,
        range_tables,
        projections,
        calibration,
        &[],
        &mut next_node,
        &mut estimates,
    );
    estimates
}

#[allow(clippy::too_many_arguments)]
fn collect_access_estimates(
    plan: &PhysicalPlan,
    table_statistics: &[TableAccessStatistics],
    access_paths: &[AccessPath],
    range_tables: &[RangeTablePlanningSnapshot],
    projections: &[ColumnarProjectionPlanningSnapshot],
    calibration: &PlannerCalibrationProfile,
    constraints: &[ColumnarPlanningConstraint],
    next_node: &mut u32,
    output: &mut Vec<PlannerAccessEstimate>,
) {
    let node = PlanNodeOrdinal(*next_node);
    *next_node = next_node.saturating_add(1);
    let table_stats = |table_id| {
        table_statistics
            .iter()
            .find(|entry| entry.table_id == table_id)
            .and_then(|entry| entry.statistics)
    };
    match plan {
        PhysicalPlan::SeqScan {
            binding_id,
            table_id,
            ..
        } => {
            let fallback_rows = projections
                .iter()
                .filter(|projection| projection.table_id == *table_id)
                .map(|projection| projection.row_count)
                .max()
                .unwrap_or(0);
            let base = source_scan_work_units(table_stats(*table_id), fallback_rows);
            output.push(PlannerAccessEstimate {
                node,
                binding_id: *binding_id,
                table_id: *table_id,
                storage_id: None,
                kind: PlannerAccessKind::SeqScan,
                access_path: None,
                projection_id: None,
                projection_generation: None,
                estimated_work_units: Some(base),
                effective_work_units: apply_calibration_ratio(
                    base,
                    calibration.ratio(PlannerCalibrationClass::SeqScan),
                ),
                calibration_epoch: calibration.epoch,
                source_alternative_work_units: None,
                effective_source_alternative_work_units: None,
                work_model: PlannerWorkModel::SequentialRows,
            });
        }
        PhysicalPlan::IndexScan {
            binding_id,
            table_id,
            access_path,
            key,
            ..
        } => output.push(index_access_estimate(
            node,
            *binding_id,
            *table_id,
            None,
            PlannerAccessKind::IndexPoint,
            *access_path,
            Some(key),
            None,
            table_stats(*table_id),
            access_paths,
            calibration,
        )),
        PhysicalPlan::RangeIndexScan {
            binding_id,
            table_id,
            access_path,
            range,
            ..
        } => output.push(index_access_estimate(
            node,
            *binding_id,
            *table_id,
            None,
            PlannerAccessKind::IndexRange,
            *access_path,
            None,
            Some(range),
            table_stats(*table_id),
            access_paths,
            calibration,
        )),
        PhysicalPlan::ColumnarScan {
            binding_id,
            table_id,
            columns,
            projection_id,
            generation,
            source_storage_id,
            ..
        } => {
            let projection = projections.iter().find(|projection| {
                projection.projection_id == *projection_id
                    && projection.generation == *generation
                    && projection.source_storage_id == *source_storage_id
            });
            let required = columns
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>();
            let base = projection
                .map(|projection| columnar_work_units(projection, &required, constraints));
            let source_base = projection.map(|projection| {
                source_scan_work_units(table_stats(*table_id), projection.row_count)
            });
            output.push(PlannerAccessEstimate {
                node,
                binding_id: *binding_id,
                table_id: *table_id,
                storage_id: Some(*source_storage_id),
                kind: PlannerAccessKind::Columnar,
                access_path: None,
                projection_id: Some(*projection_id),
                projection_generation: Some(*generation),
                estimated_work_units: base,
                effective_work_units: base.and_then(|base| {
                    apply_calibration_ratio(
                        base,
                        calibration.ratio(PlannerCalibrationClass::Columnar),
                    )
                }),
                calibration_epoch: calibration.epoch,
                source_alternative_work_units: source_base,
                effective_source_alternative_work_units: source_base.and_then(|base| {
                    apply_calibration_ratio(
                        base,
                        calibration.ratio(PlannerCalibrationClass::SeqScan),
                    )
                }),
                work_model: PlannerWorkModel::Columnar {
                    projected_column_count: u64::try_from(required.len()).unwrap_or(u64::MAX),
                },
            });
        }
        PhysicalPlan::PartitionedScan {
            binding_id,
            table_id,
            partitions,
            ..
        } => {
            let range = range_tables
                .iter()
                .find(|range| range.table_id == *table_id);
            for partition in partitions {
                let snapshot = range.and_then(|range| {
                    range
                        .partitions
                        .iter()
                        .find(|candidate| candidate.storage_id == partition.storage_id)
                });
                match &partition.access {
                    PartitionAccessPlan::SeqScan => {
                        let base = source_scan_work_units(
                            snapshot.and_then(|snapshot| snapshot.statistics),
                            0,
                        );
                        output.push(PlannerAccessEstimate {
                            node,
                            binding_id: *binding_id,
                            table_id: *table_id,
                            storage_id: Some(partition.storage_id),
                            kind: PlannerAccessKind::PartitionedSeqScan,
                            access_path: None,
                            projection_id: None,
                            projection_generation: None,
                            estimated_work_units: Some(base),
                            effective_work_units: apply_calibration_ratio(
                                base,
                                calibration.ratio(PlannerCalibrationClass::SeqScan),
                            ),
                            calibration_epoch: calibration.epoch,
                            source_alternative_work_units: None,
                            effective_source_alternative_work_units: None,
                            work_model: PlannerWorkModel::SequentialRows,
                        })
                    }
                    PartitionAccessPlan::IndexScan {
                        access_path, key, ..
                    } => output.push(index_access_estimate(
                        node,
                        *binding_id,
                        *table_id,
                        Some(partition.storage_id),
                        PlannerAccessKind::PartitionedIndexPoint,
                        *access_path,
                        Some(key),
                        None,
                        snapshot.and_then(|snapshot| snapshot.statistics),
                        snapshot.map_or(&[], |snapshot| snapshot.access_paths.as_slice()),
                        calibration,
                    )),
                    PartitionAccessPlan::RangeIndexScan {
                        access_path, range, ..
                    } => output.push(index_access_estimate(
                        node,
                        *binding_id,
                        *table_id,
                        Some(partition.storage_id),
                        PlannerAccessKind::PartitionedIndexRange,
                        *access_path,
                        None,
                        Some(range),
                        snapshot.and_then(|snapshot| snapshot.statistics),
                        snapshot.map_or(&[], |snapshot| snapshot.access_paths.as_slice()),
                        calibration,
                    )),
                }
            }
        }
        PhysicalPlan::Filter { input, predicate } => {
            let mut pushed = constraints.to_vec();
            collect_columnar_constraints(predicate, &mut pushed);
            collect_access_estimates(
                input,
                table_statistics,
                access_paths,
                range_tables,
                projections,
                calibration,
                &pushed,
                next_node,
                output,
            );
        }
        PhysicalPlan::NestedLoopJoin { left, right, .. }
        | PhysicalPlan::HashJoin { left, right, .. } => {
            collect_access_estimates(
                left,
                table_statistics,
                access_paths,
                range_tables,
                projections,
                calibration,
                &[],
                next_node,
                output,
            );
            collect_access_estimates(
                right,
                table_statistics,
                access_paths,
                range_tables,
                projections,
                calibration,
                &[],
                next_node,
                output,
            );
        }
        PhysicalPlan::IndexNestedLoopJoin {
            left,
            right_binding_id,
            right_table_id,
            right_access_path,
            ..
        } => {
            let path = access_paths
                .iter()
                .find(|path| path.table_id == *right_table_id && path.id == *right_access_path);
            let right_statistics = table_stats(*right_table_id);
            let left_rows = physical_access_table(left)
                .and_then(table_stats)
                .map(|statistics| statistics.row_count);
            let estimated_work_units = path
                .zip(right_statistics.as_ref())
                .and_then(|(path, table)| {
                    let index = path.statistics.as_ref()?;
                    let matches = estimate_non_null_point_rows(table, index)?;
                    let point = point_lookup_cost(index, path.cost_hints.as_ref(), matches)?;
                    point.checked_mul(u128::from(left_rows?))
                })
                .and_then(|work| u64::try_from(work).ok());
            let effective_work_units = estimated_work_units.and_then(|base| {
                apply_calibration_ratio(
                    base,
                    calibration.ratio(PlannerCalibrationClass::IndexPoint),
                )
            });
            output.push(PlannerAccessEstimate {
                node,
                binding_id: *right_binding_id,
                table_id: *right_table_id,
                storage_id: None,
                kind: PlannerAccessKind::IndexPoint,
                access_path: Some(*right_access_path),
                projection_id: None,
                projection_generation: None,
                estimated_work_units,
                effective_work_units,
                calibration_epoch: calibration.epoch,
                source_alternative_work_units: None,
                effective_source_alternative_work_units: None,
                work_model: path
                    .and_then(|path| index_work_model(path, PlannerAccessKind::IndexPoint))
                    .unwrap_or(PlannerWorkModel::Index {
                        startup_work_units: 0,
                        candidate_work_units: 1,
                    }),
            });
            collect_access_estimates(
                left,
                table_statistics,
                access_paths,
                range_tables,
                projections,
                calibration,
                constraints,
                next_node,
                output,
            );
        }
        PhysicalPlan::Sort { input: left, .. }
        | PhysicalPlan::Project { input: left, .. }
        | PhysicalPlan::ScalarProject { input: left, .. }
        | PhysicalPlan::Aggregate { input: left, .. }
        | PhysicalPlan::Limit { input: left, .. } => collect_access_estimates(
            left,
            table_statistics,
            access_paths,
            range_tables,
            projections,
            calibration,
            constraints,
            next_node,
            output,
        ),
        PhysicalPlan::OneRow => {}
    }
}

fn physical_access_table(plan: &PhysicalPlan) -> Option<TableId> {
    match plan {
        PhysicalPlan::SeqScan { table_id, .. }
        | PhysicalPlan::ColumnarScan { table_id, .. }
        | PhysicalPlan::IndexScan { table_id, .. }
        | PhysicalPlan::RangeIndexScan { table_id, .. }
        | PhysicalPlan::PartitionedScan { table_id, .. } => Some(*table_id),
        PhysicalPlan::Filter { input, .. }
        | PhysicalPlan::Sort { input, .. }
        | PhysicalPlan::Project { input, .. }
        | PhysicalPlan::ScalarProject { input, .. }
        | PhysicalPlan::Aggregate { input, .. }
        | PhysicalPlan::Limit { input, .. } => physical_access_table(input),
        _ => None,
    }
}

#[allow(clippy::too_many_arguments)]
fn index_access_estimate(
    node: PlanNodeOrdinal,
    binding_id: RelationBindingId,
    table_id: TableId,
    storage_id: Option<StorageId>,
    kind: PlannerAccessKind,
    access_path_id: AccessPathId,
    point: Option<&ScalarValue>,
    range: Option<&IndexRange>,
    table: Option<TableStatistics>,
    access_paths: &[AccessPath],
    calibration: &PlannerCalibrationProfile,
) -> PlannerAccessEstimate {
    let path = access_paths
        .iter()
        .find(|path| path.id == access_path_id && path.table_id == table_id);
    let model = path
        .and_then(|path| index_work_model(path, kind))
        .unwrap_or(PlannerWorkModel::Index {
            startup_work_units: 0,
            candidate_work_units: 1,
        });
    let estimated_work_units = table
        .as_ref()
        .zip(path)
        .and_then(|(table, path)| {
            let index = path.statistics.as_ref()?;
            let lookup = match (point, range) {
                (Some(key), None) => IndexLookupCandidate::Point { key: key.clone() },
                (None, Some(range)) => IndexLookupCandidate::Range {
                    range: range.clone(),
                    possible_integer_keys: estimated_integer_key_count(range)?,
                },
                _ => return None,
            };
            candidate_cost(table, index, path.cost_hints.as_ref(), &lookup)
        })
        .and_then(|work| u64::try_from(work).ok());
    PlannerAccessEstimate {
        node,
        binding_id,
        table_id,
        storage_id,
        kind,
        access_path: Some(access_path_id),
        projection_id: None,
        projection_generation: None,
        estimated_work_units,
        effective_work_units: estimated_work_units.and_then(|base| {
            apply_calibration_ratio(base, calibration.ratio(kind.calibration_class()))
        }),
        calibration_epoch: calibration.epoch,
        source_alternative_work_units: None,
        effective_source_alternative_work_units: None,
        work_model: model,
    }
}

fn index_work_model(path: &AccessPath, kind: PlannerAccessKind) -> Option<PlannerWorkModel> {
    if let Some(hints) = path.cost_hints {
        let startup = if matches!(
            kind,
            PlannerAccessKind::IndexPoint | PlannerAccessKind::PartitionedIndexPoint
        ) {
            u64::from(hints.point_probe_base_cost)
                .saturating_add(u64::from(hints.expected_point_io))
        } else {
            u64::from(hints.range_startup_cost)
        };
        return Some(PlannerWorkModel::Index {
            startup_work_units: startup,
            candidate_work_units: u64::from(hints.sequential_unit_cost),
        });
    }
    let height = path.statistics.as_ref()?.tree_height;
    Some(PlannerWorkModel::Index {
        startup_work_units: 1_u64.saturating_add(u64::from(height)),
        candidate_work_units: 1,
    })
}

/// Converts raw execution evidence to canonical planner work units.
/// Incomplete evidence never participates in calibration or adaptive policy.
#[must_use]
pub fn evaluate_actual_access_work(
    estimate: &PlannerAccessEstimate,
    actual: PlannerActualAccessEvidence,
) -> Option<u64> {
    if actual.incomplete {
        return None;
    }
    match estimate.work_model {
        PlannerWorkModel::SequentialRows => Some(1_u64.saturating_add(actual.rows_examined / 32)),
        PlannerWorkModel::Index {
            startup_work_units,
            candidate_work_units,
        } => {
            let probes = match estimate.kind {
                PlannerAccessKind::IndexPoint | PlannerAccessKind::PartitionedIndexPoint => {
                    actual.index_point_probes
                }
                PlannerAccessKind::IndexRange | PlannerAccessKind::PartitionedIndexRange => {
                    actual.index_range_probes
                }
                _ => return None,
            };
            startup_work_units
                .checked_mul(probes)?
                .checked_add(candidate_work_units.checked_mul(actual.index_candidates_examined)?)
        }
        PlannerWorkModel::Columnar {
            projected_column_count,
        } => {
            let columnar = actual.columnar?;
            let decoded_values = columnar
                .rows_read
                .checked_mul(projected_column_count)?
                .div_ceil(256);
            let version_rows = columnar
                .delta_live_rows
                .checked_add(columnar.suppressed_version_rows)?
                .div_ceil(256);
            2_u64
                .checked_add(columnar.row_groups_read)?
                .checked_add(columnar.physical_block_reads)?
                .checked_add(columnar.physical_bytes_read.div_ceil(4096))?
                .checked_add(columnar.decoded_column_chunks)?
                .checked_add(columnar.decoded_version_blocks)?
                .checked_add(decoded_values)?
                .checked_add(columnar.delta_segments)?
                .checked_add(columnar.delta_bytes_read.div_ceil(4096))?
                .checked_add(columnar.delta_mutations.div_ceil(256))?
                .checked_add(version_rows)?
                .checked_add(columnar.merged_rows.div_ceil(256))
        }
    }
}

fn planning_group_cannot_match(
    group: &ColumnarRowGroupPlanningSnapshot,
    constraint: &ColumnarPlanningConstraint,
) -> bool {
    let Some(zone) = group
        .columns
        .iter()
        .find(|zone| zone.column_id == constraint.column_id)
    else {
        return false;
    };
    let (Some(minimum), Some(maximum)) = (&zone.minimum, &zone.maximum) else {
        return zone.null_count == u64::from(group.rows);
    };
    if let Some((lower, inclusive)) = &constraint.lower {
        let ordering = compare_values(maximum, lower);
        if ordering == Ordering::Less || (!inclusive && ordering == Ordering::Equal) {
            return true;
        }
    }
    if let Some((upper, inclusive)) = &constraint.upper {
        let ordering = compare_values(minimum, upper);
        if ordering == Ordering::Greater || (!inclusive && ordering == Ordering::Equal) {
            return true;
        }
    }
    false
}

fn collect_columnar_constraints(expression: &Expr, output: &mut Vec<ColumnarPlanningConstraint>) {
    let ExprKind::Binary {
        operator,
        left,
        right,
    } = &expression.kind
    else {
        return;
    };
    if *operator == BinaryOp::And {
        collect_columnar_constraints(left, output);
        collect_columnar_constraints(right, output);
        return;
    }
    let (column, value, operator) = match (&left.kind, &right.kind) {
        (ExprKind::Column(column), ExprKind::Literal(value)) => (column, value, *operator),
        (ExprKind::Literal(value), ExprKind::Column(column)) => {
            let reversed = match operator {
                BinaryOp::Lt => BinaryOp::Gt,
                BinaryOp::LtEq => BinaryOp::GtEq,
                BinaryOp::Gt => BinaryOp::Lt,
                BinaryOp::GtEq => BinaryOp::LtEq,
                other => *other,
            };
            (column, value, reversed)
        }
        _ => return,
    };
    if matches!(value, ScalarValue::Null) {
        return;
    }
    let (lower, upper) = match operator {
        BinaryOp::Eq => (Some((value.clone(), true)), Some((value.clone(), true))),
        BinaryOp::Gt => (Some((value.clone(), false)), None),
        BinaryOp::GtEq => (Some((value.clone(), true)), None),
        BinaryOp::Lt => (None, Some((value.clone(), false))),
        BinaryOp::LtEq => (None, Some((value.clone(), true))),
        BinaryOp::NotEq | BinaryOp::And | BinaryOp::Or => return,
    };
    output.push(ColumnarPlanningConstraint {
        column_id: column.column_id,
        lower,
        upper,
    });
}

fn plan_raw_with_statistics(
    logical: &LogicalPlan,
    table_statistics: &[TableAccessStatistics],
    access_paths: &[AccessPath],
    range_tables: &[RangeTablePlanningSnapshot],
    calibration: &PlannerCalibrationProfile,
) -> PhysicalPlan {
    match logical {
        LogicalPlan::OneRow => PhysicalPlan::OneRow,
        LogicalPlan::Scan {
            binding_id,
            table_id,
            table_name,
            columns,
        } => range_tables
            .iter()
            .find(|placement| placement.table_id == *table_id)
            .map(|placement| {
                build_partitioned_scan(
                    None,
                    *binding_id,
                    *table_id,
                    table_name,
                    columns,
                    placement,
                    calibration,
                )
            })
            .unwrap_or_else(|| PhysicalPlan::SeqScan {
                binding_id: *binding_id,
                table_id: *table_id,
                table_name: table_name.clone(),
                columns: columns.clone(),
            }),
        LogicalPlan::Join {
            left,
            right,
            kind,
            predicate,
            columns,
        } => {
            let physical_left = Box::new(plan_raw_with_statistics(
                left,
                table_statistics,
                access_paths,
                range_tables,
                calibration,
            ));
            match choose_direct_inner_join(
                *kind,
                left,
                right,
                predicate,
                table_statistics,
                access_paths,
                range_tables,
                calibration,
            ) {
                DirectInnerJoin::Index {
                    left_key,
                    right_key,
                    right_access_path,
                } => {
                    let LogicalPlan::Scan {
                        binding_id,
                        table_id,
                        table_name,
                        columns: right_columns,
                    } = right.as_ref()
                    else {
                        return PhysicalPlan::NestedLoopJoin {
                            left: physical_left,
                            right: Box::new(plan_raw_with_statistics(
                                right,
                                table_statistics,
                                access_paths,
                                range_tables,
                                calibration,
                            )),
                            kind: *kind,
                            predicate: predicate.clone(),
                            columns: columns.clone(),
                        };
                    };
                    PhysicalPlan::IndexNestedLoopJoin {
                        left: physical_left,
                        right_binding_id: *binding_id,
                        right_table_id: *table_id,
                        right_table_name: table_name.clone(),
                        right_columns: right_columns.clone(),
                        kind: *kind,
                        left_key,
                        right_key,
                        right_access_path,
                        predicate: predicate.clone(),
                        columns: columns.clone(),
                    }
                }
                DirectInnerJoin::Hash {
                    left_key,
                    right_key,
                } => PhysicalPlan::HashJoin {
                    left: physical_left,
                    right: Box::new(plan_raw_with_statistics(
                        right,
                        table_statistics,
                        access_paths,
                        range_tables,
                        calibration,
                    )),
                    kind: *kind,
                    left_key,
                    right_key,
                    predicate: predicate.clone(),
                    columns: columns.clone(),
                },
                DirectInnerJoin::NestedLoop => PhysicalPlan::NestedLoopJoin {
                    left: physical_left,
                    right: Box::new(plan_raw_with_statistics(
                        right,
                        table_statistics,
                        access_paths,
                        range_tables,
                        calibration,
                    )),
                    kind: *kind,
                    predicate: predicate.clone(),
                    columns: columns.clone(),
                },
            }
        }
        LogicalPlan::Filter { input, predicate } => {
            let input = match input.as_ref() {
                LogicalPlan::Scan {
                    binding_id,
                    table_id,
                    table_name,
                    columns,
                } => range_tables
                    .iter()
                    .find(|placement| placement.table_id == *table_id)
                    .map(|placement| {
                        build_partitioned_scan(
                            Some(predicate),
                            *binding_id,
                            *table_id,
                            table_name,
                            columns,
                            placement,
                            calibration,
                        )
                    })
                    .or_else(|| {
                        choose_index_access(
                            predicate,
                            *binding_id,
                            *table_id,
                            table_name,
                            columns,
                            table_statistics,
                            access_paths,
                            calibration,
                        )
                    })
                    .unwrap_or_else(|| {
                        plan_raw_with_statistics(
                            input,
                            table_statistics,
                            access_paths,
                            range_tables,
                            calibration,
                        )
                    }),
                _ => plan_raw_with_statistics(
                    input,
                    table_statistics,
                    access_paths,
                    range_tables,
                    calibration,
                ),
            };
            PhysicalPlan::Filter {
                input: Box::new(input),
                predicate: predicate.clone(),
            }
        }
        LogicalPlan::Sort { input, keys } => PhysicalPlan::Sort {
            input: Box::new(plan_raw_with_statistics(
                input,
                table_statistics,
                access_paths,
                range_tables,
                calibration,
            )),
            keys: keys.clone(),
        },
        LogicalPlan::Project { input, columns } => PhysicalPlan::Project {
            input: Box::new(plan_raw_with_statistics(
                input,
                table_statistics,
                access_paths,
                range_tables,
                calibration,
            )),
            columns: columns.clone(),
        },
        LogicalPlan::ScalarProject { input, expressions } => PhysicalPlan::ScalarProject {
            input: Box::new(plan_raw_with_statistics(
                input,
                table_statistics,
                access_paths,
                range_tables,
                calibration,
            )),
            expressions: expressions.clone(),
        },
        LogicalPlan::Aggregate {
            input,
            group_keys,
            outputs,
        } => PhysicalPlan::Aggregate {
            input: Box::new(plan_raw_with_statistics(
                input,
                table_statistics,
                access_paths,
                range_tables,
                calibration,
            )),
            group_keys: group_keys.clone(),
            outputs: outputs.clone(),
        },
        LogicalPlan::Limit { input, limit } => PhysicalPlan::Limit {
            input: Box::new(plan_raw_with_statistics(
                input,
                table_statistics,
                access_paths,
                range_tables,
                calibration,
            )),
            limit: *limit,
        },
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SourceIdentity {
    binding_id: RelationBindingId,
    column_id: ColumnId,
}

impl From<&ColumnRef> for SourceIdentity {
    fn from(column: &ColumnRef) -> Self {
        Self {
            binding_id: column.binding_id,
            column_id: column.column_id,
        }
    }
}

fn add_required(required: &mut Vec<SourceIdentity>, column: &ColumnRef) {
    let identity = SourceIdentity::from(column);
    if !required.contains(&identity) {
        required.push(identity);
    }
}

fn collect_expression_columns(expression: &Expr, required: &mut Vec<SourceIdentity>) {
    match &expression.kind {
        ExprKind::Column(column) => add_required(required, column),
        ExprKind::Literal(_) | ExprKind::Parameter(_) => {}
        ExprKind::Binary { left, right, .. } => {
            collect_expression_columns(left, required);
            collect_expression_columns(right, required);
        }
        ExprKind::Cast { expression }
        | ExprKind::Unary { expression, .. }
        | ExprKind::IsNull { expression, .. } => {
            collect_expression_columns(expression, required);
        }
    }
}

fn prune_required_columns(plan: PhysicalPlan, parent_required: &[SourceIdentity]) -> PhysicalPlan {
    match plan {
        PhysicalPlan::OneRow => PhysicalPlan::OneRow,
        PhysicalPlan::SeqScan {
            binding_id,
            table_id,
            table_name,
            columns,
        } => PhysicalPlan::SeqScan {
            binding_id,
            table_id,
            table_name,
            columns: prune_columns(columns, parent_required),
        },
        PhysicalPlan::ColumnarScan {
            binding_id,
            table_id,
            table_name,
            columns,
            projection_id,
            generation,
            source_storage_id,
        } => PhysicalPlan::ColumnarScan {
            binding_id,
            table_id,
            table_name,
            columns: prune_columns(columns, parent_required),
            projection_id,
            generation,
            source_storage_id,
        },
        PhysicalPlan::IndexScan {
            binding_id,
            table_id,
            table_name,
            columns,
            index_column,
            access_path,
            key,
        } => PhysicalPlan::IndexScan {
            binding_id,
            table_id,
            table_name,
            columns: prune_columns(columns, parent_required),
            index_column,
            access_path,
            key,
        },
        PhysicalPlan::RangeIndexScan {
            binding_id,
            table_id,
            table_name,
            columns,
            index_column,
            access_path,
            range,
        } => PhysicalPlan::RangeIndexScan {
            binding_id,
            table_id,
            table_name,
            columns: prune_columns(columns, parent_required),
            index_column,
            access_path,
            range,
        },
        PhysicalPlan::PartitionedScan {
            binding_id,
            table_id,
            table_name,
            columns,
            partition_key,
            total_partitions,
            partitions,
        } => PhysicalPlan::PartitionedScan {
            binding_id,
            table_id,
            table_name,
            columns: prune_columns(columns, parent_required),
            partition_key,
            total_partitions,
            partitions,
        },
        PhysicalPlan::Filter { input, predicate } => {
            let mut required = parent_required.to_vec();
            collect_expression_columns(&predicate, &mut required);
            PhysicalPlan::Filter {
                input: Box::new(prune_required_columns(*input, &required)),
                predicate,
            }
        }
        PhysicalPlan::Sort { input, keys } => {
            let mut required = parent_required.to_vec();
            for key in &keys {
                add_required(&mut required, &key.column);
            }
            PhysicalPlan::Sort {
                input: Box::new(prune_required_columns(*input, &required)),
                keys,
            }
        }
        PhysicalPlan::Project { input, columns } => {
            let mut required = Vec::new();
            for column in &columns {
                add_required(&mut required, column);
            }
            PhysicalPlan::Project {
                input: Box::new(prune_required_columns(*input, &required)),
                columns,
            }
        }
        PhysicalPlan::ScalarProject { input, expressions } => {
            let mut required = Vec::new();
            for expression in &expressions {
                collect_expression_columns(&expression.expression, &mut required);
            }
            PhysicalPlan::ScalarProject {
                input: Box::new(prune_required_columns(*input, &required)),
                expressions,
            }
        }
        PhysicalPlan::Aggregate {
            input,
            group_keys,
            outputs,
        } => {
            let mut required = Vec::new();
            for column in &group_keys {
                add_required(&mut required, column);
            }
            for output in &outputs {
                if let AggregateOutput::Aggregate(aggregate) = output {
                    if let AggregateInput::Column(column) = &aggregate.input {
                        add_required(&mut required, column);
                    }
                }
            }
            PhysicalPlan::Aggregate {
                input: Box::new(prune_required_columns(*input, &required)),
                group_keys,
                outputs,
            }
        }
        PhysicalPlan::Limit { input, limit } => PhysicalPlan::Limit {
            input: Box::new(prune_required_columns(*input, parent_required)),
            limit,
        },
        PhysicalPlan::NestedLoopJoin {
            left,
            right,
            kind,
            predicate,
            columns,
        } => prune_join(
            *left,
            *right,
            kind,
            predicate,
            columns,
            None,
            parent_required,
        ),
        PhysicalPlan::IndexNestedLoopJoin {
            left,
            right_binding_id,
            right_table_id,
            right_table_name,
            right_columns,
            kind,
            left_key,
            right_key,
            right_access_path,
            predicate,
            columns,
        } => prune_index_join(
            *left,
            right_binding_id,
            right_table_id,
            right_table_name,
            right_columns,
            kind,
            left_key,
            right_key,
            right_access_path,
            predicate,
            columns,
            parent_required,
        ),
        PhysicalPlan::HashJoin {
            left,
            right,
            kind,
            left_key,
            right_key,
            predicate,
            columns,
        } => prune_join(
            *left,
            *right,
            kind,
            predicate,
            columns,
            Some((left_key, right_key)),
            parent_required,
        ),
    }
}

#[allow(clippy::too_many_arguments)]
fn prune_index_join(
    left: PhysicalPlan,
    right_binding_id: RelationBindingId,
    right_table_id: TableId,
    right_table_name: String,
    right_columns: Vec<ColumnRef>,
    kind: JoinKind,
    left_key: ColumnRef,
    right_key: ColumnRef,
    right_access_path: AccessPathId,
    predicate: Expr,
    columns: Vec<ColumnRef>,
    parent_required: &[SourceIdentity],
) -> PhysicalPlan {
    let mut required = parent_required.to_vec();
    collect_expression_columns(&predicate, &mut required);
    add_required(&mut required, &left_key);
    add_required(&mut required, &right_key);
    let left_outputs = left.output_fields();
    let left_required = required
        .iter()
        .copied()
        .filter(|identity| output_contains(&left_outputs, *identity))
        .collect::<Vec<_>>();
    let right_columns = prune_columns(right_columns, &required);
    let columns = prune_columns(columns, &required);
    PhysicalPlan::IndexNestedLoopJoin {
        left: Box::new(prune_required_columns(left, &left_required)),
        right_binding_id,
        right_table_id,
        right_table_name,
        right_columns,
        kind,
        left_key,
        right_key,
        right_access_path,
        predicate,
        columns,
    }
}

fn prune_columns(columns: Vec<ColumnRef>, required: &[SourceIdentity]) -> Vec<ColumnRef> {
    columns
        .into_iter()
        .filter(|column| required.contains(&SourceIdentity::from(column)))
        .collect()
}

fn prune_join(
    left: PhysicalPlan,
    right: PhysicalPlan,
    kind: JoinKind,
    predicate: Expr,
    columns: Vec<ColumnRef>,
    hash_keys: Option<(ColumnRef, ColumnRef)>,
    parent_required: &[SourceIdentity],
) -> PhysicalPlan {
    let mut required = parent_required.to_vec();
    collect_expression_columns(&predicate, &mut required);
    if let Some((left_key, right_key)) = &hash_keys {
        add_required(&mut required, left_key);
        add_required(&mut required, right_key);
    }
    let left_outputs = left.output_fields();
    let right_outputs = right.output_fields();
    let left_required = required
        .iter()
        .copied()
        .filter(|identity| output_contains(&left_outputs, *identity))
        .collect::<Vec<_>>();
    let right_required = required
        .iter()
        .copied()
        .filter(|identity| output_contains(&right_outputs, *identity))
        .collect::<Vec<_>>();
    let columns = columns
        .into_iter()
        .filter(|column| required.contains(&SourceIdentity::from(column)))
        .collect();
    let left = Box::new(prune_required_columns(left, &left_required));
    let right = Box::new(prune_required_columns(right, &right_required));
    match hash_keys {
        None => PhysicalPlan::NestedLoopJoin {
            left,
            right,
            kind,
            predicate,
            columns,
        },
        Some((left_key, right_key)) => PhysicalPlan::HashJoin {
            left,
            right,
            kind,
            left_key,
            right_key,
            predicate,
            columns,
        },
    }
}

fn output_contains(fields: &[OutputField], identity: SourceIdentity) -> bool {
    fields.iter().any(|field| {
        field
            .source_column()
            .is_some_and(|column| SourceIdentity::from(column) == identity)
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DirectInnerJoin {
    NestedLoop,
    Hash {
        left_key: ColumnRef,
        right_key: ColumnRef,
    },
    Index {
        left_key: ColumnRef,
        right_key: ColumnRef,
        right_access_path: AccessPathId,
    },
}

#[allow(clippy::too_many_arguments)]
fn choose_direct_inner_join(
    kind: JoinKind,
    left: &LogicalPlan,
    right: &LogicalPlan,
    predicate: &Expr,
    table_statistics: &[TableAccessStatistics],
    access_paths: &[AccessPath],
    range_tables: &[RangeTablePlanningSnapshot],
    calibration: &PlannerCalibrationProfile,
) -> DirectInnerJoin {
    if !matches!(kind, JoinKind::Inner) {
        return DirectInnerJoin::NestedLoop;
    }
    let Some((left_key, right_key)) = find_hash_equality(predicate, left, right) else {
        return DirectInnerJoin::NestedLoop;
    };
    let Some(left_table_id) = direct_scan_table_id(left) else {
        return DirectInnerJoin::NestedLoop;
    };
    let (Some(left_statistics), Some(right_statistics)) = (
        direct_scan_statistics(left, table_statistics),
        direct_scan_statistics(right, table_statistics),
    ) else {
        return DirectInnerJoin::NestedLoop;
    };
    let left_rows = left_statistics.row_count;
    let right_rows = right_statistics.row_count;
    let Some(nested_loop_work) = u128::from(left_rows).checked_mul(u128::from(right_rows)) else {
        return DirectInnerJoin::NestedLoop;
    };
    let Some(hash_join_work) = u128::from(left_rows).checked_add(u128::from(right_rows)) else {
        return DirectInnerJoin::NestedLoop;
    };
    // Point access and SeqScan must stay in the storage-neutral units already
    // used by Filter planning. Row count remains the existing Hash-vs-Nested
    // CPU-work comparison, but is not a full-scan access cost.
    let hash_join_right_work = effective_planning_cost(
        seq_scan_cost(right_statistics),
        calibration.ratio(PlannerCalibrationClass::SeqScan),
    );

    if let Some((right_table_id, right_access_path, point_cost)) = eligible_right_point_access(
        right,
        &right_key,
        table_statistics,
        access_paths,
        range_tables,
    ) {
        if left_table_id != right_table_id
            && !range_tables
                .iter()
                .any(|placement| placement.table_id == left_table_id)
        {
            if let Some(index_join_inner_base_work) = u128::from(left_rows).checked_mul(point_cost)
            {
                let index_join_inner_work = effective_planning_cost(
                    index_join_inner_base_work,
                    calibration.ratio(PlannerCalibrationClass::IndexPoint),
                );
                if index_join_inner_work < nested_loop_work
                    && index_join_inner_work < hash_join_right_work
                {
                    return DirectInnerJoin::Index {
                        left_key,
                        right_key,
                        right_access_path,
                    };
                }
            }
        }
    }

    if hash_join_work < nested_loop_work {
        DirectInnerJoin::Hash {
            left_key,
            right_key,
        }
    } else {
        DirectInnerJoin::NestedLoop
    }
}

fn direct_scan_table_id(plan: &LogicalPlan) -> Option<TableId> {
    let LogicalPlan::Scan { table_id, .. } = plan else {
        return None;
    };
    Some(*table_id)
}

fn eligible_right_point_access(
    right: &LogicalPlan,
    right_key: &ColumnRef,
    table_statistics: &[TableAccessStatistics],
    access_paths: &[AccessPath],
    range_tables: &[RangeTablePlanningSnapshot],
) -> Option<(TableId, AccessPathId, u128)> {
    let LogicalPlan::Scan { table_id, .. } = right else {
        return None;
    };
    if range_tables
        .iter()
        .any(|placement| placement.table_id == *table_id)
    {
        return None;
    }
    let table = table_statistics
        .iter()
        .find(|candidate| candidate.table_id == *table_id)?
        .statistics
        .as_ref()?;
    access_paths
        .iter()
        .filter(|path| {
            path.table_id == *table_id
                && path.column_id == right_key.column_id
                && path.capabilities.point_lookup
                && path.capabilities.ordered
        })
        .filter_map(|path| {
            let index = path.statistics.as_ref()?;
            let matches = estimate_non_null_point_rows(table, index)?;
            let cost = point_lookup_cost(index, path.cost_hints.as_ref(), matches)?;
            Some((path.id, cost))
        })
        .min_by_key(|(_, cost)| *cost)
        .map(|(access_path, cost)| (*table_id, access_path, cost))
}

fn direct_scan_statistics<'a>(
    plan: &LogicalPlan,
    table_statistics: &'a [TableAccessStatistics],
) -> Option<&'a TableStatistics> {
    let LogicalPlan::Scan { table_id, .. } = plan else {
        return None;
    };
    table_statistics
        .iter()
        .find(|candidate| candidate.table_id == *table_id)?
        .statistics
        .as_ref()
}

fn find_hash_equality(
    predicate: &Expr,
    left: &LogicalPlan,
    right: &LogicalPlan,
) -> Option<(ColumnRef, ColumnRef)> {
    match &predicate.kind {
        ExprKind::Binary {
            operator: BinaryOp::And,
            left: first,
            right: second,
        } => find_hash_equality(first, left, right)
            .or_else(|| find_hash_equality(second, left, right)),
        ExprKind::Binary {
            operator: BinaryOp::Eq,
            left: first,
            right: second,
        } => {
            let (ExprKind::Column(first), ExprKind::Column(second)) = (&first.kind, &second.kind)
            else {
                return None;
            };
            if !first.data_type.is_compatible_with(&second.data_type) {
                return None;
            }
            if scan_contains_column(left, first) && scan_contains_column(right, second) {
                Some((first.clone(), second.clone()))
            } else if scan_contains_column(left, second) && scan_contains_column(right, first) {
                Some((second.clone(), first.clone()))
            } else {
                None
            }
        }
        _ => None,
    }
}

fn scan_contains_column(plan: &LogicalPlan, column: &ColumnRef) -> bool {
    let LogicalPlan::Scan { columns, .. } = plan else {
        return false;
    };
    columns.iter().any(|candidate| {
        candidate.binding_id == column.binding_id && candidate.column_id == column.column_id
    })
}

#[derive(Debug)]
enum IndexLookupCandidate {
    Point {
        key: ScalarValue,
    },
    Range {
        range: IndexRange,
        possible_integer_keys: u128,
    },
}

#[derive(Debug)]
struct IndexCandidate<'a> {
    access_path: &'a AccessPath,
    index_column: ColumnRef,
    lookup: IndexLookupCandidate,
}

#[derive(Debug, Default)]
struct PartitionConstraint {
    lower: Option<IndexBound>,
    upper: Option<IndexBound>,
    unsafe_boolean: bool,
}

fn build_partitioned_scan(
    predicate: Option<&Expr>,
    binding_id: RelationBindingId,
    table_id: TableId,
    table_name: &str,
    columns: &[ColumnRef],
    placement: &RangeTablePlanningSnapshot,
    calibration: &PlannerCalibrationProfile,
) -> PhysicalPlan {
    let constraint = predicate.map(|predicate| {
        let mut constraint = PartitionConstraint::default();
        collect_partition_constraint(
            predicate,
            binding_id,
            table_id,
            placement.partition_key,
            &mut constraint,
        );
        constraint
    });
    let partitions = placement
        .partitions
        .iter()
        .filter(|partition| {
            constraint
                .as_ref()
                .is_none_or(|constraint| partition_may_match(partition, constraint))
        })
        .map(|partition| {
            let local_statistics = [TableAccessStatistics {
                table_id,
                statistics: partition.statistics,
            }];
            let selected = predicate.and_then(|predicate| {
                choose_index_access(
                    predicate,
                    binding_id,
                    table_id,
                    table_name,
                    columns,
                    &local_statistics,
                    &partition.access_paths,
                    calibration,
                )
            });
            let access = match selected {
                Some(PhysicalPlan::IndexScan {
                    index_column,
                    access_path,
                    key,
                    ..
                }) => PartitionAccessPlan::IndexScan {
                    index_column,
                    access_path,
                    key,
                },
                Some(PhysicalPlan::RangeIndexScan {
                    index_column,
                    access_path,
                    range,
                    ..
                }) => PartitionAccessPlan::RangeIndexScan {
                    index_column,
                    access_path,
                    range,
                },
                _ => PartitionAccessPlan::SeqScan,
            };
            PartitionScanPlan {
                partition_id: partition.partition_id,
                storage_id: partition.storage_id,
                access,
            }
        })
        .collect();
    PhysicalPlan::PartitionedScan {
        binding_id,
        table_id,
        table_name: table_name.to_owned(),
        columns: columns.to_vec(),
        partition_key: placement.partition_key,
        total_partitions: placement.partitions.len(),
        partitions,
    }
}

fn collect_partition_constraint(
    expression: &Expr,
    binding_id: RelationBindingId,
    table_id: TableId,
    column_id: ColumnId,
    constraint: &mut PartitionConstraint,
) {
    let ExprKind::Binary {
        operator,
        left,
        right,
    } = &expression.kind
    else {
        if matches!(expression.kind, ExprKind::Unary { .. }) {
            constraint.unsafe_boolean = true;
        }
        return;
    };
    match operator {
        BinaryOp::And => {
            collect_partition_constraint(left, binding_id, table_id, column_id, constraint);
            collect_partition_constraint(right, binding_id, table_id, column_id, constraint);
        }
        BinaryOp::Or => constraint.unsafe_boolean = true,
        BinaryOp::Eq => {
            let equality = point_equality(left, right, binding_id, table_id, column_id)
                .or_else(|| point_equality(right, left, binding_id, table_id, column_id));
            if let Some((_, value)) = equality {
                tighten_lower(&mut constraint.lower, IndexBound::Included(value.clone()));
                tighten_upper(&mut constraint.upper, IndexBound::Included(value));
            }
        }
        BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::Gt | BinaryOp::GtEq => {
            if let Some((_, bound, is_lower)) =
                comparison_bound(*operator, left, right, binding_id, table_id, column_id)
            {
                if is_lower {
                    tighten_lower(&mut constraint.lower, bound);
                } else {
                    tighten_upper(&mut constraint.upper, bound);
                }
            }
        }
        BinaryOp::NotEq => {}
    }
}

fn partition_may_match(
    partition: &PartitionPlanningSnapshot,
    constraint: &PartitionConstraint,
) -> bool {
    if constraint.unsafe_boolean {
        return true;
    }
    let query_lower = constraint.lower.as_ref().and_then(bound_value);
    let query_upper = constraint.upper.as_ref().and_then(bound_value);
    let partition_lower = partition.lower.as_ref().map(|value| (value, true));
    let partition_upper = partition.upper.as_ref().map(|value| (value, false));
    let lower = strongest_lower(query_lower, partition_lower);
    let upper = strongest_upper(query_upper, partition_upper);
    integer_interval_nonempty(lower, upper)
}

fn strongest_lower<'a>(
    left: Option<(&'a ScalarValue, bool)>,
    right: Option<(&'a ScalarValue, bool)>,
) -> Option<(&'a ScalarValue, bool)> {
    match (left, right) {
        (None, value) | (value, None) => value,
        (Some(left), Some(right)) => match scalar_order(left.0, right.0)? {
            Ordering::Less => Some(right),
            Ordering::Greater => Some(left),
            Ordering::Equal => Some((left.0, left.1 && right.1)),
        },
    }
}

fn strongest_upper<'a>(
    left: Option<(&'a ScalarValue, bool)>,
    right: Option<(&'a ScalarValue, bool)>,
) -> Option<(&'a ScalarValue, bool)> {
    match (left, right) {
        (None, value) | (value, None) => value,
        (Some(left), Some(right)) => match scalar_order(left.0, right.0)? {
            Ordering::Less => Some(left),
            Ordering::Greater => Some(right),
            Ordering::Equal => Some((left.0, left.1 && right.1)),
        },
    }
}

fn integer_interval_nonempty(
    lower: Option<(&ScalarValue, bool)>,
    upper: Option<(&ScalarValue, bool)>,
) -> bool {
    match (lower, upper) {
        (
            Some((ScalarValue::Int64(lower), lower_included)),
            Some((ScalarValue::Int64(upper), upper_included)),
        ) => {
            let minimum = if lower_included {
                Some(*lower)
            } else {
                lower.checked_add(1)
            };
            let maximum = if upper_included {
                Some(*upper)
            } else {
                upper.checked_sub(1)
            };
            minimum
                .zip(maximum)
                .is_some_and(|(minimum, maximum)| minimum <= maximum)
        }
        (
            Some((ScalarValue::UInt64(lower), lower_included)),
            Some((ScalarValue::UInt64(upper), upper_included)),
        ) => {
            let minimum = if lower_included {
                Some(*lower)
            } else {
                lower.checked_add(1)
            };
            let maximum = if upper_included {
                Some(*upper)
            } else {
                upper.checked_sub(1)
            };
            minimum
                .zip(maximum)
                .is_some_and(|(minimum, maximum)| minimum <= maximum)
        }
        (Some((ScalarValue::Int64(value), false)), None) => value.checked_add(1).is_some(),
        (Some((ScalarValue::UInt64(value), false)), None) => value.checked_add(1).is_some(),
        (None, Some((ScalarValue::Int64(value), false))) => value.checked_sub(1).is_some(),
        (None, Some((ScalarValue::UInt64(value), false))) => value.checked_sub(1).is_some(),
        (Some((ScalarValue::Int64(_), true)), None)
        | (Some((ScalarValue::UInt64(_), true)), None)
        | (None, Some((ScalarValue::Int64(_), true)))
        | (None, Some((ScalarValue::UInt64(_), true)))
        | (None, None) => true,
        _ => true,
    }
}

fn scalar_order(left: &ScalarValue, right: &ScalarValue) -> Option<Ordering> {
    match (left, right) {
        (ScalarValue::Int64(left), ScalarValue::Int64(right)) => Some(left.cmp(right)),
        (ScalarValue::UInt64(left), ScalarValue::UInt64(right)) => Some(left.cmp(right)),
        _ => None,
    }
}

#[allow(clippy::too_many_arguments)]
fn choose_index_access(
    predicate: &Expr,
    binding_id: RelationBindingId,
    table_id: TableId,
    table_name: &str,
    columns: &[ColumnRef],
    table_statistics: &[TableAccessStatistics],
    access_paths: &[AccessPath],
    calibration: &PlannerCalibrationProfile,
) -> Option<PhysicalPlan> {
    let mut eligible = Vec::new();
    for access_path in access_paths {
        if access_path.table_id != table_id {
            continue;
        }
        if access_path.capabilities.point_lookup {
            if let Some((index_column, key)) =
                find_point_constraint(predicate, binding_id, table_id, access_path.column_id)
            {
                eligible.push(IndexCandidate {
                    access_path,
                    index_column,
                    lookup: IndexLookupCandidate::Point { key },
                });
            }
        }
        if access_path.capabilities.range_lookup {
            if let Some((index_column, range, possible_integer_keys)) =
                find_range_constraint(predicate, binding_id, table_id, access_path.column_id)
            {
                eligible.push(IndexCandidate {
                    access_path,
                    index_column,
                    lookup: IndexLookupCandidate::Range {
                        range,
                        possible_integer_keys,
                    },
                });
            }
        }
    }
    let first_point = eligible
        .iter()
        .find(|candidate| matches!(candidate.lookup, IndexLookupCandidate::Point { .. }));
    let analyzed_table = table_statistics
        .iter()
        .find(|entry| entry.table_id == table_id)
        .and_then(|entry| entry.statistics.as_ref());
    let selected = match analyzed_table {
        None => first_point?,
        Some(table) => {
            let mut best: Option<(&IndexCandidate<'_>, u128)> = None;
            for candidate in &eligible {
                let Some(index) = candidate.access_path.statistics.as_ref() else {
                    continue;
                };
                let Some(base_cost) = candidate_cost(
                    table,
                    index,
                    candidate.access_path.cost_hints.as_ref(),
                    &candidate.lookup,
                ) else {
                    continue;
                };
                let class = match &candidate.lookup {
                    IndexLookupCandidate::Point { .. } => PlannerCalibrationClass::IndexPoint,
                    IndexLookupCandidate::Range { .. } => PlannerCalibrationClass::IndexRange,
                };
                let cost = effective_planning_cost(base_cost, calibration.ratio(class));
                if best.is_none_or(|(_, best_cost)| cost < best_cost) {
                    best = Some((candidate, cost));
                }
            }
            let Some((candidate, cost)) = best else {
                return first_point.map(|candidate| {
                    build_index_scan(candidate, binding_id, table_id, table_name, columns)
                });
            };
            let seq_cost = effective_planning_cost(
                seq_scan_cost(table),
                calibration.ratio(PlannerCalibrationClass::SeqScan),
            );
            if cost >= seq_cost {
                return None;
            }
            candidate
        }
    };
    Some(build_index_scan(
        selected, binding_id, table_id, table_name, columns,
    ))
}

fn build_index_scan(
    candidate: &IndexCandidate<'_>,
    binding_id: RelationBindingId,
    table_id: TableId,
    table_name: &str,
    columns: &[ColumnRef],
) -> PhysicalPlan {
    match &candidate.lookup {
        IndexLookupCandidate::Point { key } => PhysicalPlan::IndexScan {
            binding_id,
            table_id,
            table_name: table_name.to_owned(),
            columns: columns.to_vec(),
            index_column: candidate.index_column.clone(),
            access_path: candidate.access_path.id,
            key: key.clone(),
        },
        IndexLookupCandidate::Range { range, .. } => PhysicalPlan::RangeIndexScan {
            binding_id,
            table_id,
            table_name: table_name.to_owned(),
            columns: columns.to_vec(),
            index_column: candidate.index_column.clone(),
            access_path: candidate.access_path.id,
            range: range.clone(),
        },
    }
}

fn seq_scan_cost(statistics: &TableStatistics) -> u128 {
    u128::from(statistics.managed_page_count)
}

fn candidate_cost(
    table: &TableStatistics,
    index: &IndexStatistics,
    hints: Option<&AccessCostHints>,
    candidate: &IndexLookupCandidate,
) -> Option<u128> {
    let estimated_matches = match candidate {
        IndexLookupCandidate::Point { key } => estimate_point_rows(table, index, key)?,
        IndexLookupCandidate::Range {
            possible_integer_keys,
            ..
        } => estimate_range_rows(table, index, *possible_integer_keys)?,
    };
    if matches!(candidate, IndexLookupCandidate::Point { .. }) {
        return point_lookup_cost(index, hints, estimated_matches);
    }
    if let Some(hints) = hints {
        return u128::from(hints.range_startup_cost)
            .checked_add(estimated_matches.checked_mul(u128::from(hints.sequential_unit_cost))?);
    }
    Some(1 + u128::from(index.tree_height) + estimated_matches)
}

fn point_lookup_cost(
    index: &IndexStatistics,
    hints: Option<&AccessCostHints>,
    estimated_matches: u128,
) -> Option<u128> {
    if let Some(hints) = hints {
        return u128::from(hints.point_probe_base_cost)
            .checked_add(u128::from(hints.expected_point_io))?
            .checked_add(estimated_matches.checked_mul(u128::from(hints.sequential_unit_cost))?);
    }
    u128::from(index.tree_height)
        .checked_add(1)?
        .checked_add(estimated_matches)
}

fn estimate_range_rows(
    table: &TableStatistics,
    index: &IndexStatistics,
    possible_integer_keys: u128,
) -> Option<u128> {
    let non_null_rows = table.row_count.checked_sub(index.null_count)?;
    if non_null_rows == 0 {
        return Some(0);
    }
    if index.distinct_non_null_keys == 0 {
        return None;
    }
    let quotient = non_null_rows / index.distinct_non_null_keys;
    let remainder = non_null_rows % index.distinct_non_null_keys;
    let average_duplicates = u128::from(quotient) + u128::from(remainder != 0);
    let estimated = possible_integer_keys.checked_mul(average_duplicates)?;
    Some(estimated.min(u128::from(non_null_rows)))
}

fn estimate_point_rows(
    table: &TableStatistics,
    index: &IndexStatistics,
    key: &ScalarValue,
) -> Option<u128> {
    if matches!(key, ScalarValue::Null) {
        return Some(u128::from(index.null_count));
    }
    estimate_non_null_point_rows(table, index)
}

fn estimate_non_null_point_rows(table: &TableStatistics, index: &IndexStatistics) -> Option<u128> {
    let non_null_rows = table.row_count.checked_sub(index.null_count)?;
    if non_null_rows == 0 {
        return Some(0);
    }
    if index.distinct_non_null_keys == 0 {
        return None;
    }
    let quotient = non_null_rows / index.distinct_non_null_keys;
    let remainder = non_null_rows % index.distinct_non_null_keys;
    Some(u128::from(quotient) + u128::from(remainder != 0))
}

fn find_point_constraint(
    predicate: &Expr,
    binding_id: RelationBindingId,
    table_id: TableId,
    column_id: ColumnId,
) -> Option<(ColumnRef, ScalarValue)> {
    match &predicate.kind {
        ExprKind::Binary {
            operator: BinaryOp::And,
            left,
            right,
        } => find_point_constraint(left, binding_id, table_id, column_id)
            .or_else(|| find_point_constraint(right, binding_id, table_id, column_id)),
        ExprKind::Binary {
            operator: BinaryOp::Eq,
            left,
            right,
        } => point_equality(left, right, binding_id, table_id, column_id)
            .or_else(|| point_equality(right, left, binding_id, table_id, column_id)),
        ExprKind::IsNull {
            expression,
            negated: false,
        } => match &expression.kind {
            ExprKind::Column(column)
                if column_matches(column, binding_id, table_id, column_id) && column.nullable =>
            {
                Some((column.clone(), ScalarValue::Null))
            }
            _ => None,
        },
        _ => None,
    }
}

fn find_range_constraint(
    predicate: &Expr,
    binding_id: RelationBindingId,
    table_id: TableId,
    column_id: ColumnId,
) -> Option<(ColumnRef, IndexRange, u128)> {
    let mut column = None;
    let mut lower = None;
    let mut upper = None;
    collect_range_bounds(
        predicate,
        binding_id,
        table_id,
        column_id,
        &mut column,
        &mut lower,
        &mut upper,
    );
    let column = column?;
    if !matches!(
        column.data_type.physical,
        netbadb_types::PhysicalType::Int64 | netbadb_types::PhysicalType::UInt64
    ) {
        return None;
    }
    let range = IndexRange {
        lower: lower?,
        upper: upper?,
    };
    let possible_integer_keys = estimated_integer_key_count(&range)?;
    Some((column, range, possible_integer_keys))
}

fn collect_range_bounds(
    predicate: &Expr,
    binding_id: RelationBindingId,
    table_id: TableId,
    column_id: ColumnId,
    column: &mut Option<ColumnRef>,
    lower: &mut Option<IndexBound>,
    upper: &mut Option<IndexBound>,
) {
    let ExprKind::Binary {
        operator,
        left,
        right,
    } = &predicate.kind
    else {
        return;
    };
    if *operator == BinaryOp::And {
        collect_range_bounds(left, binding_id, table_id, column_id, column, lower, upper);
        collect_range_bounds(right, binding_id, table_id, column_id, column, lower, upper);
        return;
    }
    let Some((matched_column, bound, is_lower)) =
        comparison_bound(*operator, left, right, binding_id, table_id, column_id)
    else {
        return;
    };
    *column = Some(matched_column);
    if is_lower {
        tighten_lower(lower, bound);
    } else {
        tighten_upper(upper, bound);
    }
}

fn comparison_bound(
    operator: BinaryOp,
    left: &Expr,
    right: &Expr,
    binding_id: RelationBindingId,
    table_id: TableId,
    column_id: ColumnId,
) -> Option<(ColumnRef, IndexBound, bool)> {
    comparison_bound_ordered(operator, left, right, binding_id, table_id, column_id).or_else(|| {
        comparison_bound_ordered(
            reverse_comparison(operator)?,
            right,
            left,
            binding_id,
            table_id,
            column_id,
        )
    })
}

fn comparison_bound_ordered(
    operator: BinaryOp,
    column: &Expr,
    literal: &Expr,
    binding_id: RelationBindingId,
    table_id: TableId,
    column_id: ColumnId,
) -> Option<(ColumnRef, IndexBound, bool)> {
    let ExprKind::Column(column) = &column.kind else {
        return None;
    };
    let ExprKind::Literal(value) = &literal.kind else {
        return None;
    };
    if matches!(value, ScalarValue::Null)
        || !column_matches(column, binding_id, table_id, column_id)
    {
        return None;
    }
    let (bound, is_lower) = match operator {
        BinaryOp::Gt => (IndexBound::Excluded(value.clone()), true),
        BinaryOp::GtEq => (IndexBound::Included(value.clone()), true),
        BinaryOp::Lt => (IndexBound::Excluded(value.clone()), false),
        BinaryOp::LtEq => (IndexBound::Included(value.clone()), false),
        BinaryOp::Eq | BinaryOp::NotEq | BinaryOp::And | BinaryOp::Or => return None,
    };
    Some((column.clone(), bound, is_lower))
}

const fn reverse_comparison(operator: BinaryOp) -> Option<BinaryOp> {
    match operator {
        BinaryOp::Lt => Some(BinaryOp::Gt),
        BinaryOp::LtEq => Some(BinaryOp::GtEq),
        BinaryOp::Gt => Some(BinaryOp::Lt),
        BinaryOp::GtEq => Some(BinaryOp::LtEq),
        BinaryOp::Eq | BinaryOp::NotEq | BinaryOp::And | BinaryOp::Or => None,
    }
}

fn tighten_lower(current: &mut Option<IndexBound>, candidate: IndexBound) {
    let replace = current
        .as_ref()
        .is_none_or(|existing| compare_bounds(existing, &candidate, true) == Ordering::Less);
    if replace {
        *current = Some(candidate);
    }
}

fn tighten_upper(current: &mut Option<IndexBound>, candidate: IndexBound) {
    let replace = current
        .as_ref()
        .is_none_or(|existing| compare_bounds(existing, &candidate, false) == Ordering::Greater);
    if replace {
        *current = Some(candidate);
    }
}

fn compare_bounds(left: &IndexBound, right: &IndexBound, lower: bool) -> Ordering {
    let Some((left_value, left_included)) = bound_value(left) else {
        return Ordering::Equal;
    };
    let Some((right_value, right_included)) = bound_value(right) else {
        return Ordering::Equal;
    };
    compare_values(left_value, right_value).then_with(|| {
        if left_included == right_included {
            Ordering::Equal
        } else if lower {
            left_included.cmp(&right_included).reverse()
        } else {
            left_included.cmp(&right_included)
        }
    })
}

fn bound_value(bound: &IndexBound) -> Option<(&ScalarValue, bool)> {
    match bound {
        IndexBound::Included(value) => Some((value, true)),
        IndexBound::Excluded(value) => Some((value, false)),
        IndexBound::Unbounded => None,
    }
}

fn estimated_integer_key_count(range: &IndexRange) -> Option<u128> {
    match (&range.lower, &range.upper) {
        (IndexBound::Included(ScalarValue::Int64(lower)), _) => {
            let lower = i128::from(*lower);
            let upper = match &range.upper {
                IndexBound::Included(ScalarValue::Int64(value)) => i128::from(*value) + 1,
                IndexBound::Excluded(ScalarValue::Int64(value)) => i128::from(*value),
                _ => return None,
            };
            if upper <= lower {
                Some(0)
            } else {
                u128::try_from(upper - lower).ok()
            }
        }
        (IndexBound::Excluded(ScalarValue::Int64(lower)), _) => {
            let lower = i128::from(*lower) + 1;
            let upper = match &range.upper {
                IndexBound::Included(ScalarValue::Int64(value)) => i128::from(*value) + 1,
                IndexBound::Excluded(ScalarValue::Int64(value)) => i128::from(*value),
                _ => return None,
            };
            if upper <= lower {
                Some(0)
            } else {
                u128::try_from(upper - lower).ok()
            }
        }
        (IndexBound::Included(ScalarValue::UInt64(lower)), _) => {
            let lower = u128::from(*lower);
            let upper = match &range.upper {
                IndexBound::Included(ScalarValue::UInt64(value)) => {
                    u128::from(*value).checked_add(1)?
                }
                IndexBound::Excluded(ScalarValue::UInt64(value)) => u128::from(*value),
                _ => return None,
            };
            Some(upper.saturating_sub(lower))
        }
        (IndexBound::Excluded(ScalarValue::UInt64(lower)), _) => {
            let lower = u128::from(*lower).checked_add(1)?;
            let upper = match &range.upper {
                IndexBound::Included(ScalarValue::UInt64(value)) => {
                    u128::from(*value).checked_add(1)?
                }
                IndexBound::Excluded(ScalarValue::UInt64(value)) => u128::from(*value),
                _ => return None,
            };
            Some(upper.saturating_sub(lower))
        }
        _ => None,
    }
}

fn point_equality(
    column: &Expr,
    literal: &Expr,
    binding_id: RelationBindingId,
    table_id: TableId,
    column_id: ColumnId,
) -> Option<(ColumnRef, ScalarValue)> {
    let ExprKind::Column(column) = &column.kind else {
        return None;
    };
    let ExprKind::Literal(value) = &literal.kind else {
        return None;
    };
    if matches!(value, ScalarValue::Null)
        || !column_matches(column, binding_id, table_id, column_id)
    {
        return None;
    }
    Some((column.clone(), value.clone()))
}

fn column_matches(
    column: &ColumnRef,
    binding_id: RelationBindingId,
    table_id: TableId,
    column_id: ColumnId,
) -> bool {
    column.binding_id == binding_id && column.table_id == table_id && column.column_id == column_id
}

#[must_use]
pub fn plan_statement(logical: &LogicalStatement) -> PhysicalStatement {
    plan_statement_with_access_paths(logical, &[])
}

/// Plans one statement using access paths in caller-provided priority order.
#[must_use]
pub fn plan_statement_with_access_paths(
    logical: &LogicalStatement,
    access_paths: &[AccessPath],
) -> PhysicalStatement {
    plan_statement_with_statistics(logical, &[], access_paths)
}

/// Plans one statement with the same access-path statistics context used for
/// queries, UPDATE, and DELETE.
#[must_use]
pub fn plan_statement_with_statistics(
    logical: &LogicalStatement,
    table_statistics: &[TableAccessStatistics],
    access_paths: &[AccessPath],
) -> PhysicalStatement {
    plan_statement_with_partition_snapshots(logical, table_statistics, access_paths, &[])
}

#[must_use]
pub fn plan_statement_with_partition_snapshots(
    logical: &LogicalStatement,
    table_statistics: &[TableAccessStatistics],
    access_paths: &[AccessPath],
    range_tables: &[RangeTablePlanningSnapshot],
) -> PhysicalStatement {
    plan_statement_with_partition_snapshots_and_calibration(
        logical,
        table_statistics,
        access_paths,
        range_tables,
        &PlannerCalibrationProfile::IDENTITY,
    )
}

#[must_use]
pub fn plan_statement_with_partition_snapshots_and_calibration(
    logical: &LogicalStatement,
    table_statistics: &[TableAccessStatistics],
    access_paths: &[AccessPath],
    range_tables: &[RangeTablePlanningSnapshot],
    calibration: &PlannerCalibrationProfile,
) -> PhysicalStatement {
    match logical {
        LogicalStatement::Query(query) => {
            PhysicalStatement::Query(plan_with_partition_snapshots_and_calibration(
                query,
                table_statistics,
                access_paths,
                range_tables,
                calibration,
            ))
        }
        LogicalStatement::Insert {
            table_id,
            table_name,
            values,
        } => PhysicalStatement::Insert {
            table_id: *table_id,
            table_name: table_name.clone(),
            values: values.clone(),
        },
        LogicalStatement::Update {
            input,
            table_id,
            assignments,
        } => PhysicalStatement::Update {
            input: plan_raw_with_statistics(
                input,
                table_statistics,
                access_paths,
                range_tables,
                calibration,
            ),
            table_id: *table_id,
            assignments: assignments.clone(),
        },
        LogicalStatement::Delete { input, table_id } => PhysicalStatement::Delete {
            input: plan_raw_with_statistics(
                input,
                table_statistics,
                access_paths,
                range_tables,
                calibration,
            ),
            table_id: *table_id,
        },
    }
}

#[must_use]
pub fn plan_statement_with_columnar_snapshots(
    logical: &LogicalStatement,
    table_statistics: &[TableAccessStatistics],
    access_paths: &[AccessPath],
    range_tables: &[RangeTablePlanningSnapshot],
    projections: &[ColumnarProjectionPlanningSnapshot],
) -> PhysicalStatement {
    plan_statement_with_columnar_snapshots_and_calibration(
        logical,
        table_statistics,
        access_paths,
        range_tables,
        projections,
        &PlannerCalibrationProfile::IDENTITY,
    )
}

#[must_use]
pub fn plan_statement_with_columnar_snapshots_and_calibration(
    logical: &LogicalStatement,
    table_statistics: &[TableAccessStatistics],
    access_paths: &[AccessPath],
    range_tables: &[RangeTablePlanningSnapshot],
    projections: &[ColumnarProjectionPlanningSnapshot],
    calibration: &PlannerCalibrationProfile,
) -> PhysicalStatement {
    match logical {
        LogicalStatement::Query(query) => {
            PhysicalStatement::Query(plan_with_columnar_snapshots_and_calibration(
                query,
                table_statistics,
                access_paths,
                range_tables,
                projections,
                calibration,
            ))
        }
        _ => plan_statement_with_partition_snapshots_and_calibration(
            logical,
            table_statistics,
            access_paths,
            range_tables,
            calibration,
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AccessCostHints, AccessPath, AccessPathCapabilities, ColumnarPlanningConstraint,
        ColumnarProjectionPlanningSnapshot, ColumnarRowGroupPlanningSnapshot,
        ColumnarZoneMapPlanningSnapshot, PhysicalPlan, PhysicalStatement,
        PlannerActualAccessEvidence, PlannerColumnarExecutionEvidence, RangeTablePlanningSnapshot,
        TableAccessStatistics, columnar_work_units, estimate_execution_accesses,
        evaluate_actual_access_work, plan, plan_statement, plan_statement_with_access_paths,
        plan_statement_with_statistics, plan_with_access_paths, plan_with_columnar_snapshots,
        plan_with_partition_snapshots, plan_with_statistics, point_lookup_cost,
    };
    use netbadb_index::{IndexBound, IndexRange, IndexStatistics, TableStatistics};
    use netbadb_rel::{BinaryOp, ColumnRef, Expr, ExprKind, LogicalPlan, LogicalStatement};
    use netbadb_types::{
        AccessPathId, ColumnId, ColumnarGeneration, ColumnarProjectionId, ExprType, PhysicalType,
        RelationBindingId, ScalarValue, SemanticType, StorageId, TableId,
    };

    fn columnar_snapshot(
        projected_columns: Vec<ColumnId>,
        groups: Vec<ColumnarRowGroupPlanningSnapshot>,
    ) -> ColumnarProjectionPlanningSnapshot {
        ColumnarProjectionPlanningSnapshot {
            projection_id: ColumnarProjectionId(1),
            generation: ColumnarGeneration(1),
            table_id: TableId(1),
            source_storage_id: StorageId(1),
            projected_columns,
            row_count: groups.iter().map(|group| u64::from(group.rows)).sum(),
            row_group_count: u64::try_from(groups.len()).expect("group count"),
            segment_bytes: groups
                .iter()
                .flat_map(|group| &group.columns)
                .map(|column| column.encoded_bytes)
                .sum(),
            delta_segment_count: 0,
            delta_bytes: 0,
            delta_mutation_count: 0,
            delta_live_row_count: 0,
            suppressed_version_count: 0,
            row_groups: groups,
        }
    }

    fn planning_group(
        rows: u32,
        columns: &[ColumnId],
        bytes_per_column: u64,
        minimum: i64,
        maximum: i64,
    ) -> ColumnarRowGroupPlanningSnapshot {
        ColumnarRowGroupPlanningSnapshot {
            rows,
            columns: columns
                .iter()
                .map(|column_id| ColumnarZoneMapPlanningSnapshot {
                    column_id: *column_id,
                    null_count: 0,
                    minimum: Some(ScalarValue::Int64(minimum)),
                    maximum: Some(ScalarValue::Int64(maximum)),
                    encoded_bytes: bytes_per_column,
                })
                .collect(),
        }
    }

    #[test]
    fn columnar_cost_uses_projected_bytes_and_keeps_index_precedence() {
        let id = test_column(1, "id", false);
        let payload = test_column(2, "payload", false);
        let point = LogicalPlan::Project {
            input: Box::new(filtered_scan(
                binary(
                    BinaryOp::Eq,
                    column_expr(&id),
                    literal(ScalarValue::Int64(7)),
                ),
                vec![id.clone(), payload.clone()],
            )),
            columns: vec![payload.clone()],
        };
        let projection = columnar_snapshot(
            vec![ColumnId(1), ColumnId(2)],
            vec![planning_group(
                10_000,
                &[ColumnId(1), ColumnId(2)],
                80_000,
                0,
                9_999,
            )],
        );
        let planned = plan_with_columnar_snapshots(
            &point,
            &[analyzed_table(10_000, 1_000)],
            &[analyzed_path(1, 40, 10_000, 0, 2)],
            &[],
            std::slice::from_ref(&projection),
        );
        assert!(matches!(
            base_plan(&planned),
            PhysicalPlan::IndexScan { .. }
        ));

        let small_table_scan = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Scan {
                binding_id: RelationBindingId(7),
                table_id: TableId(1),
                table_name: "users".into(),
                columns: vec![id.clone(), payload.clone()],
            }),
            columns: vec![id, payload],
        };
        let costly_projection = columnar_snapshot(
            vec![ColumnId(1), ColumnId(2)],
            vec![
                planning_group(128, &[ColumnId(1), ColumnId(2)], 4_096, 0, 127),
                planning_group(128, &[ColumnId(1), ColumnId(2)], 4_096, 128, 255),
            ],
        );
        let planned = plan_with_columnar_snapshots(
            &small_table_scan,
            &[analyzed_table(256, 4)],
            &[],
            &[],
            &[costly_projection],
        );
        assert!(matches!(base_plan(&planned), PhysicalPlan::SeqScan { .. }));
    }

    #[test]
    fn wide_projection_with_narrow_required_subset_can_beat_authoritative_scan() {
        let required = [ColumnId(1), ColumnId(2), ColumnId(3)];
        let projected = (1..=128).map(ColumnId).collect::<Vec<_>>();
        let groups = (0..16)
            .map(|group| planning_group(512, &projected, 4_096, group * 512, group * 512 + 511))
            .collect();
        let projection = columnar_snapshot(projected, groups);
        let columns = required
            .iter()
            .enumerate()
            .map(|(position, id)| test_column(id.0, &format!("c{position}"), false))
            .collect::<Vec<_>>();
        let logical = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Scan {
                binding_id: RelationBindingId(7),
                table_id: TableId(1),
                table_name: "wide".into(),
                columns: columns.clone(),
            }),
            columns,
        };
        let planned = plan_with_columnar_snapshots(
            &logical,
            &[analyzed_table(8_192, 1_000)],
            &[],
            &[],
            &[projection],
        );
        assert!(matches!(
            base_plan(&planned),
            PhysicalPlan::ColumnarScan { .. }
        ));
    }

    #[test]
    fn zone_map_pruning_reduces_work_independently_of_predicate_selectivity() {
        let column = ColumnId(1);
        let projected = vec![column];
        let friendly = columnar_snapshot(
            projected.clone(),
            (0..8)
                .map(|group| planning_group(100, &projected, 800, group * 100, group * 100 + 99))
                .collect(),
        );
        let hostile = columnar_snapshot(
            projected,
            (0..8)
                .map(|_| planning_group(100, &[column], 800, 0, 799))
                .collect(),
        );
        let constraints = [ColumnarPlanningConstraint {
            column_id: column,
            lower: Some((ScalarValue::Int64(100), true)),
            upper: Some((ScalarValue::Int64(199), true)),
        }];
        let friendly_work = columnar_work_units(&friendly, &[column], &constraints);
        let hostile_work = columnar_work_units(&hostile, &[column], &constraints);
        assert!(friendly_work < hostile_work);
        assert_eq!(friendly_work, 5);
        assert_eq!(hostile_work, 16);
    }

    #[test]
    fn delta_cost_is_structural_and_never_receives_base_zone_map_credit() {
        let column = ColumnId(1);
        let mut small = columnar_snapshot(
            vec![column],
            vec![planning_group(1_000, &[column], 8_000, 0, 999)],
        );
        small.delta_segment_count = 1;
        small.delta_bytes = 4_096;
        small.delta_mutation_count = 64;
        small.delta_live_row_count = 32;
        small.suppressed_version_count = 32;
        let mut large = small.clone();
        large.delta_segment_count = 128;
        large.delta_bytes = 64 * 1024 * 1024;
        large.delta_mutation_count = 500_000;
        large.delta_live_row_count = 250_000;
        large.suppressed_version_count = 250_000;
        let impossible = [ColumnarPlanningConstraint {
            column_id: column,
            lower: Some((ScalarValue::Int64(2_000), true)),
            upper: None,
        }];
        let base = columnar_snapshot(
            vec![column],
            vec![planning_group(1_000, &[column], 8_000, 0, 999)],
        );
        let base_pruned = columnar_work_units(&base, &[column], &impossible);
        let small_pruned = columnar_work_units(&small, &[column], &impossible);
        let large_pruned = columnar_work_units(&large, &[column], &impossible);
        assert!(small_pruned > base_pruned);
        assert!(large_pruned > small_pruned);
        assert_eq!(
            small_pruned.saturating_sub(base_pruned),
            columnar_work_units(&small, &[], &impossible).saturating_sub(columnar_work_units(
                &base,
                &[],
                &impossible
            )),
            "base pruning cannot discount delta work"
        );

        let source = test_column(1, "value", false);
        let logical = LogicalPlan::Scan {
            binding_id: RelationBindingId(7),
            table_id: TableId(1),
            table_name: "values".into(),
            columns: vec![source],
        };
        let planned = plan_with_columnar_snapshots(
            &logical,
            &[analyzed_table(1_000, 10)],
            &[],
            &[],
            &[large],
        );
        assert!(
            matches!(base_plan(&planned), PhysicalPlan::SeqScan { .. }),
            "large delta state must be able to make the authoritative scan cheaper"
        );
        let compacted =
            plan_with_columnar_snapshots(&logical, &[analyzed_table(1_000, 10)], &[], &[], &[base]);
        assert!(
            matches!(base_plan(&compacted), PhysicalPlan::ColumnarScan { .. }),
            "resetting structural Delta work after compaction must naturally restore eligibility"
        );
    }

    #[test]
    fn projection_missing_a_required_column_is_never_a_candidate() {
        let id = test_column(1, "id", false);
        let payload = test_column(2, "payload", false);
        let logical = LogicalPlan::Scan {
            binding_id: RelationBindingId(7),
            table_id: TableId(1),
            table_name: "users".into(),
            columns: vec![id, payload],
        };
        let projection = columnar_snapshot(
            vec![ColumnId(1)],
            vec![planning_group(1_000, &[ColumnId(1)], 8_000, 0, 999)],
        );
        let planned = plan_with_columnar_snapshots(
            &logical,
            &[analyzed_table(1_000, 10_000)],
            &[],
            &[],
            &[projection],
        );
        assert!(matches!(planned, PhysicalPlan::SeqScan { .. }));
    }

    #[test]
    fn creates_a_sequence_scan_physical_plan() {
        let column = ColumnRef {
            binding_id: RelationBindingId(0),
            table_id: TableId(1),
            column_id: ColumnId(1),
            relation_name: "users".into(),
            name: "id".into(),
            data_type: SemanticType::physical(PhysicalType::Int64),
            nullable: false,
        };
        let logical = LogicalPlan::Scan {
            binding_id: RelationBindingId(0),
            table_id: TableId(1),
            table_name: "users".into(),
            columns: vec![column],
        };
        assert!(matches!(plan(&logical), PhysicalPlan::SeqScan { .. }));
    }

    #[test]
    fn preserves_a_dml_operator_above_its_physical_input() {
        let logical = LogicalStatement::Delete {
            input: LogicalPlan::Scan {
                binding_id: RelationBindingId(0),
                table_id: TableId(1),
                table_name: "users".into(),
                columns: Vec::new(),
            },
            table_id: TableId(1),
        };
        assert!(matches!(
            plan_statement(&logical),
            PhysicalStatement::Delete {
                input: PhysicalPlan::SeqScan { .. },
                table_id: TableId(1),
            }
        ));
    }

    #[test]
    fn lowers_logical_join_directly_to_nested_loop_join() {
        let logical = LogicalPlan::Join {
            left: Box::new(LogicalPlan::Scan {
                binding_id: RelationBindingId(0),
                table_id: TableId(1),
                table_name: "employees".into(),
                columns: Vec::new(),
            }),
            right: Box::new(LogicalPlan::Scan {
                binding_id: RelationBindingId(1),
                table_id: TableId(1),
                table_name: "employees".into(),
                columns: Vec::new(),
            }),
            kind: netbadb_rel::JoinKind::Inner,
            predicate: netbadb_rel::Expr {
                kind: netbadb_rel::ExprKind::Literal(netbadb_types::ScalarValue::Bool(true)),
                expr_type: netbadb_types::ExprType {
                    data_type: SemanticType::physical(PhysicalType::Bool),
                    nullable: false,
                },
            },
            columns: Vec::new(),
        };
        assert!(matches!(
            plan(&logical),
            PhysicalPlan::NestedLoopJoin { .. }
        ));
    }

    fn join_column_ref(
        binding_id: u32,
        table_id: u64,
        column_id: u32,
        name: &str,
        data_type: SemanticType,
        nullable: bool,
    ) -> ColumnRef {
        ColumnRef {
            binding_id: RelationBindingId(binding_id),
            table_id: TableId(table_id),
            column_id: ColumnId(column_id),
            relation_name: format!("r{binding_id}"),
            name: name.into(),
            data_type,
            nullable,
        }
    }

    fn join_scan(binding_id: u32, table_id: u64, columns: Vec<ColumnRef>) -> LogicalPlan {
        LogicalPlan::Scan {
            binding_id: RelationBindingId(binding_id),
            table_id: TableId(table_id),
            table_name: format!("table_{table_id}"),
            columns,
        }
    }

    fn join_expr(column: &ColumnRef) -> Expr {
        Expr {
            kind: ExprKind::Column(column.clone()),
            expr_type: ExprType {
                data_type: column.data_type.clone(),
                nullable: column.nullable,
            },
        }
    }

    fn join_literal(value: ScalarValue, physical: PhysicalType) -> Expr {
        Expr {
            kind: ExprKind::Literal(value.clone()),
            expr_type: ExprType {
                data_type: SemanticType::physical(physical),
                nullable: matches!(value, ScalarValue::Null),
            },
        }
    }

    fn join_binary(operator: BinaryOp, left: Expr, right: Expr) -> Expr {
        Expr {
            kind: ExprKind::Binary {
                operator,
                left: Box::new(left),
                right: Box::new(right),
            },
            expr_type: ExprType {
                data_type: SemanticType::physical(PhysicalType::Bool),
                nullable: true,
            },
        }
    }

    fn logical_join(left: LogicalPlan, right: LogicalPlan, predicate: Expr) -> LogicalPlan {
        let mut columns = left
            .output_fields()
            .into_iter()
            .filter_map(|field| match field {
                netbadb_rel::OutputField::Source(column) => Some(column),
                netbadb_rel::OutputField::Derived(_) => None,
            })
            .collect::<Vec<_>>();
        columns.extend(
            right
                .output_fields()
                .into_iter()
                .filter_map(|field| match field {
                    netbadb_rel::OutputField::Source(column) => Some(column),
                    netbadb_rel::OutputField::Derived(_) => None,
                }),
        );
        LogicalPlan::Join {
            left: Box::new(left),
            right: Box::new(right),
            kind: netbadb_rel::JoinKind::Inner,
            predicate,
            columns,
        }
    }

    fn join_table_statistics(table_id: u64, row_count: u64) -> TableAccessStatistics {
        join_table_statistics_with_pages(table_id, row_count, row_count.max(1))
    }

    fn join_table_statistics_with_pages(
        table_id: u64,
        row_count: u64,
        managed_page_count: u64,
    ) -> TableAccessStatistics {
        TableAccessStatistics {
            table_id: TableId(table_id),
            statistics: Some(TableStatistics {
                row_count,
                managed_page_count,
            }),
        }
    }

    fn join_index_path(table_id: u64, column_id: u32, distinct_non_null_keys: u64) -> AccessPath {
        AccessPath {
            table_id: TableId(table_id),
            column_id: ColumnId(column_id),
            id: AccessPathId(72),
            capabilities: AccessPathCapabilities {
                point_lookup: true,
                range_lookup: true,
                ordered: true,
            },
            statistics: Some(IndexStatistics {
                distinct_non_null_keys,
                null_count: 0,
                tree_height: 2,
            }),
            cost_hints: None,
        }
    }

    fn simple_join_fixture(
        left_type: SemanticType,
        right_type: SemanticType,
    ) -> (LogicalPlan, LogicalPlan, ColumnRef, ColumnRef) {
        let left_key = join_column_ref(10, 1, 1, "key", left_type, true);
        let right_key = join_column_ref(20, 2, 1, "key", right_type, true);
        (
            join_scan(10, 1, vec![left_key.clone()]),
            join_scan(20, 2, vec![right_key.clone()]),
            left_key,
            right_key,
        )
    }

    #[test]
    fn analyzed_direct_equality_uses_hash_join_and_normalizes_orientation() {
        let (left, right, left_key, right_key) = simple_join_fixture(
            SemanticType::physical(PhysicalType::Int64),
            SemanticType::physical(PhysicalType::Int64),
        );
        let statistics = [join_table_statistics(1, 500), join_table_statistics(2, 500)];
        for predicate in [
            join_binary(BinaryOp::Eq, join_expr(&left_key), join_expr(&right_key)),
            join_binary(BinaryOp::Eq, join_expr(&right_key), join_expr(&left_key)),
        ] {
            assert!(matches!(
                plan_with_statistics(
                    &logical_join(left.clone(), right.clone(), predicate),
                    &statistics,
                    &[],
                ),
                PhysicalPlan::HashJoin {
                    left_key: actual_left,
                    right_key: actual_right,
                    ..
                } if actual_left == left_key && actual_right == right_key
            ));
        }
    }

    #[test]
    fn hash_join_requires_both_statistics_and_strictly_cheaper_work() {
        let (left, right, left_key, right_key) = simple_join_fixture(
            SemanticType::physical(PhysicalType::Int64),
            SemanticType::physical(PhysicalType::Int64),
        );
        let predicate = join_binary(BinaryOp::Eq, join_expr(&left_key), join_expr(&right_key));
        let logical = logical_join(left, right, predicate);
        for statistics in [
            Vec::new(),
            vec![join_table_statistics(1, 500)],
            vec![join_table_statistics(2, 500)],
        ] {
            assert!(matches!(
                plan_with_statistics(&logical, &statistics, &[]),
                PhysicalPlan::NestedLoopJoin { .. }
            ));
        }
        for (left_rows, right_rows, hash_expected) in [(1, 100, false), (2, 2, false), (2, 3, true)]
        {
            let statistics = [
                join_table_statistics(1, left_rows),
                join_table_statistics(2, right_rows),
            ];
            assert_eq!(
                matches!(
                    plan_with_statistics(&logical, &statistics, &[]),
                    PhysicalPlan::HashJoin { .. }
                ),
                hash_expected
            );
        }
    }

    #[test]
    fn costed_right_point_index_wins_only_when_strictly_cheaper() {
        let (left, right, left_key, right_key) = simple_join_fixture(
            SemanticType::physical(PhysicalType::Int64),
            SemanticType::physical(PhysicalType::Int64),
        );
        let predicate = join_binary(BinaryOp::Eq, join_expr(&left_key), join_expr(&right_key));
        let logical = logical_join(left, right, predicate);
        let access = [join_index_path(2, 1, 4_096)];

        let small = [
            join_table_statistics_with_pages(1, 8, 1),
            join_table_statistics_with_pages(2, 4_096, 128),
        ];
        assert!(matches!(
            plan_with_statistics(&logical, &small, &access),
            PhysicalPlan::IndexNestedLoopJoin {
                right_table_id: TableId(2),
                right_access_path: AccessPathId(72),
                left_key: actual_left,
                right_key: actual_right,
                ..
            } if actual_left == left_key && actual_right == right_key
        ));
        let mut second_equal_index = join_index_path(2, 1, 4_096);
        second_equal_index.id = AccessPathId(73);
        assert!(matches!(
            plan_with_statistics(&logical, &small, &[second_equal_index, access[0].clone()],),
            PhysicalPlan::IndexNestedLoopJoin {
                right_access_path: AccessPathId(73),
                ..
            }
        ));

        let tie = [
            join_table_statistics_with_pages(1, 8, 1),
            join_table_statistics_with_pages(2, 4_096, 32),
        ];
        assert!(matches!(
            plan_with_statistics(&logical, &tie, &access),
            PhysicalPlan::HashJoin { .. }
        ));

        let scan_cheaper = [
            join_table_statistics_with_pages(1, 8, 1),
            join_table_statistics_with_pages(2, 4_096, 16),
        ];
        assert!(matches!(
            plan_with_statistics(&logical, &scan_cheaper, &access),
            PhysicalPlan::HashJoin { .. }
        ));

        let duplicate_access = [join_index_path(2, 1, 64)];
        let duplicate_outer = [
            join_table_statistics(1, 64),
            join_table_statistics(2, 4_096),
        ];
        assert!(matches!(
            plan_with_statistics(&logical, &duplicate_outer, &duplicate_access),
            PhysicalPlan::HashJoin { .. }
        ));
    }

    #[test]
    fn index_join_uses_shared_point_units_against_right_scan_units() {
        let (left, right, left_key, right_key) = simple_join_fixture(
            SemanticType::physical(PhysicalType::Int64),
            SemanticType::physical(PhysicalType::Int64),
        );
        let logical = logical_join(
            left,
            right,
            join_binary(BinaryOp::Eq, join_expr(&left_key), join_expr(&right_key)),
        );
        let hints = AccessCostHints {
            point_probe_base_cost: 8,
            expected_point_io: 2,
            range_startup_cost: 2,
            sequential_unit_cost: 1,
        };
        let mut access = join_index_path(2, 1, 4_096);
        access.cost_hints = Some(hints);
        let index = access.statistics.as_ref().expect("index statistics");
        assert_eq!(point_lookup_cost(index, Some(&hints), 1), Some(11));

        let index_cheaper = [
            join_table_statistics_with_pages(1, 8, 1),
            join_table_statistics_with_pages(2, 4_096, 89),
        ];
        assert!(matches!(
            plan_with_statistics(&logical, &index_cheaper, &[access.clone()]),
            PhysicalPlan::IndexNestedLoopJoin { .. }
        ));

        let exact_tie = [
            join_table_statistics_with_pages(1, 8, 1),
            join_table_statistics_with_pages(2, 4_096, 88),
        ];
        assert!(matches!(
            plan_with_statistics(&logical, &exact_tie, &[access]),
            PhysicalPlan::HashJoin { .. }
        ));
    }

    #[test]
    fn index_join_requires_complete_statistics_ordered_right_index_and_distinct_tables() {
        let (left, right, left_key, right_key) = simple_join_fixture(
            SemanticType::physical(PhysicalType::Int64),
            SemanticType::physical(PhysicalType::Int64),
        );
        let predicate = join_binary(BinaryOp::Eq, join_expr(&left_key), join_expr(&right_key));
        let logical = logical_join(left.clone(), right, predicate);
        let statistics = [join_table_statistics(1, 8), join_table_statistics(2, 4_096)];
        let mut missing_index_statistics = join_index_path(2, 1, 4_096);
        missing_index_statistics.statistics = None;
        let mut unordered = join_index_path(2, 1, 4_096);
        unordered.capabilities.ordered = false;
        for access in [
            Vec::new(),
            vec![missing_index_statistics],
            vec![unordered],
            vec![join_index_path(1, 1, 4_096)],
        ] {
            assert!(matches!(
                plan_with_statistics(&logical, &statistics, &access),
                PhysicalPlan::HashJoin { .. }
            ));
        }
        assert!(matches!(
            plan_with_statistics(
                &logical,
                &[join_table_statistics(1, 8)],
                &[join_index_path(2, 1, 4_096)],
            ),
            PhysicalPlan::NestedLoopJoin { .. }
        ));

        let self_right_key = join_column_ref(
            20,
            1,
            1,
            "key",
            SemanticType::physical(PhysicalType::Int64),
            true,
        );
        let self_join = logical_join(
            left,
            join_scan(20, 1, vec![self_right_key.clone()]),
            join_binary(
                BinaryOp::Eq,
                join_expr(&left_key),
                join_expr(&self_right_key),
            ),
        );
        assert!(!matches!(
            plan_with_statistics(
                &self_join,
                &[join_table_statistics(1, 4_096)],
                &[join_index_path(1, 1, 4_096)],
            ),
            PhysicalPlan::IndexNestedLoopJoin { .. }
        ));

        let partitioned_left = [RangeTablePlanningSnapshot {
            table_id: TableId(1),
            partition_key: ColumnId(1),
            partitions: Vec::new(),
        }];
        assert!(!matches!(
            plan_with_partition_snapshots(
                &logical,
                &statistics,
                &[join_index_path(2, 1, 4_096)],
                &partitioned_left,
            ),
            PhysicalPlan::IndexNestedLoopJoin { .. }
        ));
    }

    #[test]
    fn hash_equality_extraction_is_necessary_typed_and_deterministic() {
        let left_a = join_column_ref(
            10,
            1,
            1,
            "a",
            SemanticType::physical(PhysicalType::Int64),
            false,
        );
        let left_b = join_column_ref(
            10,
            1,
            2,
            "b",
            SemanticType::physical(PhysicalType::Int64),
            false,
        );
        let left_active = join_column_ref(
            10,
            1,
            3,
            "active",
            SemanticType::physical(PhysicalType::Bool),
            false,
        );
        let right_a = join_column_ref(
            20,
            2,
            1,
            "a",
            SemanticType::physical(PhysicalType::Int64),
            false,
        );
        let right_b = join_column_ref(
            20,
            2,
            2,
            "b",
            SemanticType::physical(PhysicalType::Int64),
            false,
        );
        let left = join_scan(
            10,
            1,
            vec![left_a.clone(), left_b.clone(), left_active.clone()],
        );
        let right = join_scan(20, 2, vec![right_a.clone(), right_b.clone()]);
        let statistics = [join_table_statistics(1, 500), join_table_statistics(2, 500)];
        let first_equality = join_binary(BinaryOp::Eq, join_expr(&left_a), join_expr(&right_a));
        let second_equality = join_binary(BinaryOp::Eq, join_expr(&left_b), join_expr(&right_b));
        let nested = join_binary(
            BinaryOp::And,
            join_binary(
                BinaryOp::Eq,
                join_expr(&left_active),
                join_literal(ScalarValue::Bool(true), PhysicalType::Bool),
            ),
            join_binary(
                BinaryOp::And,
                first_equality.clone(),
                second_equality.clone(),
            ),
        );
        assert!(matches!(
            plan_with_statistics(
                &logical_join(left.clone(), right.clone(), nested.clone()),
                &statistics,
                &[],
            ),
            PhysicalPlan::HashJoin {
                left_key,
                right_key,
                predicate,
                ..
            } if left_key == left_a && right_key == right_a && predicate == nested
        ));

        let rejected = [
            join_binary(
                BinaryOp::Or,
                first_equality.clone(),
                join_expr(&left_active),
            ),
            Expr {
                kind: ExprKind::Unary {
                    operator: netbadb_rel::UnaryOp::Not,
                    expression: Box::new(first_equality.clone()),
                },
                expr_type: ExprType {
                    data_type: SemanticType::physical(PhysicalType::Bool),
                    nullable: false,
                },
            },
            join_binary(BinaryOp::NotEq, join_expr(&left_a), join_expr(&right_a)),
            join_binary(BinaryOp::Lt, join_expr(&left_a), join_expr(&right_a)),
            join_binary(BinaryOp::LtEq, join_expr(&left_a), join_expr(&right_a)),
            join_binary(BinaryOp::Gt, join_expr(&left_a), join_expr(&right_a)),
            join_binary(BinaryOp::GtEq, join_expr(&left_a), join_expr(&right_a)),
            join_binary(BinaryOp::Eq, join_expr(&left_a), join_expr(&left_b)),
            join_binary(
                BinaryOp::Eq,
                join_expr(&left_a),
                join_literal(ScalarValue::Int64(42), PhysicalType::Int64),
            ),
        ];
        for predicate in rejected {
            assert!(matches!(
                plan_with_statistics(
                    &logical_join(left.clone(), right.clone(), predicate),
                    &statistics,
                    &[],
                ),
                PhysicalPlan::NestedLoopJoin { .. }
            ));
        }

        let (nominal_left, nominal_right, nominal_left_key, nominal_right_key) =
            simple_join_fixture(
                SemanticType::named("UserId", PhysicalType::UInt64),
                SemanticType::named("TeamId", PhysicalType::UInt64),
            );
        assert!(matches!(
            plan_with_statistics(
                &logical_join(
                    nominal_left,
                    nominal_right,
                    join_binary(
                        BinaryOp::Eq,
                        join_expr(&nominal_left_key),
                        join_expr(&nominal_right_key),
                    ),
                ),
                &statistics,
                &[],
            ),
            PhysicalPlan::NestedLoopJoin { .. }
        ));
    }

    #[test]
    fn only_the_current_direct_scan_join_is_hash_eligible() {
        let (left, right, left_key, right_key) = simple_join_fixture(
            SemanticType::physical(PhysicalType::Int64),
            SemanticType::physical(PhysicalType::Int64),
        );
        let predicate = join_binary(BinaryOp::Eq, join_expr(&left_key), join_expr(&right_key));
        let statistics = [
            join_table_statistics(1, 500),
            join_table_statistics(2, 500),
            join_table_statistics(3, 500),
        ];
        let filtered_left = LogicalPlan::Filter {
            input: Box::new(left.clone()),
            predicate: join_literal(ScalarValue::Bool(true), PhysicalType::Bool),
        };
        assert!(matches!(
            plan_with_statistics(
                &logical_join(filtered_left, right.clone(), predicate.clone()),
                &statistics,
                &[],
            ),
            PhysicalPlan::NestedLoopJoin { .. }
        ));

        let inner = logical_join(left, right, predicate);
        let third_key = join_column_ref(
            30,
            3,
            1,
            "key",
            SemanticType::physical(PhysicalType::Int64),
            false,
        );
        let outer = logical_join(
            inner,
            join_scan(30, 3, vec![third_key.clone()]),
            join_binary(BinaryOp::Eq, join_expr(&left_key), join_expr(&third_key)),
        );
        assert!(matches!(
            plan_with_statistics(&outer, &statistics, &[]),
            PhysicalPlan::NestedLoopJoin { left, .. }
                if matches!(*left, PhysicalPlan::HashJoin { .. })
        ));
    }

    #[test]
    fn lowers_sort_without_changing_output_columns() {
        let column = ColumnRef {
            binding_id: RelationBindingId(0),
            table_id: TableId(1),
            column_id: ColumnId(1),
            relation_name: "users".into(),
            name: "id".into(),
            data_type: SemanticType::physical(PhysicalType::Int64),
            nullable: false,
        };
        let logical = LogicalPlan::Sort {
            input: Box::new(LogicalPlan::Scan {
                binding_id: RelationBindingId(0),
                table_id: TableId(1),
                table_name: "users".into(),
                columns: vec![column.clone()],
            }),
            keys: vec![netbadb_rel::SortKey {
                column: column.clone(),
                direction: netbadb_rel::SortDirection::Desc,
                null_order: netbadb_rel::NullOrder::Last,
            }],
        };
        assert_eq!(
            logical.output_fields(),
            vec![netbadb_rel::OutputField::Source(column)]
        );
        assert!(matches!(
            plan(&logical),
            PhysicalPlan::Sort { keys, .. }
                if keys[0].direction == netbadb_rel::SortDirection::Desc
        ));
    }

    #[test]
    fn lowers_global_aggregate_with_derived_output_fields() {
        let column = ColumnRef {
            binding_id: RelationBindingId(0),
            table_id: TableId(1),
            column_id: ColumnId(1),
            relation_name: "users".into(),
            name: "score".into(),
            data_type: SemanticType::physical(PhysicalType::Int64),
            nullable: true,
        };
        let logical = LogicalPlan::Aggregate {
            input: Box::new(LogicalPlan::Scan {
                binding_id: RelationBindingId(0),
                table_id: TableId(1),
                table_name: "users".into(),
                columns: vec![column.clone()],
            }),
            group_keys: Vec::new(),
            outputs: vec![netbadb_rel::AggregateOutput::Aggregate(
                netbadb_rel::AggregateExpr {
                    function: netbadb_rel::AggregateFunction::Min,
                    input: netbadb_rel::AggregateInput::Column(column),
                    output: netbadb_rel::DerivedField {
                        name: "MIN(score)".into(),
                        data_type: SemanticType::physical(PhysicalType::Int64),
                        nullable: true,
                    },
                },
            )],
        };
        let physical = plan(&logical);
        assert!(matches!(
            physical.output_fields().as_slice(),
            [netbadb_rel::OutputField::Derived(field)] if field.name == "MIN(score)"
        ));
        assert!(matches!(physical, PhysicalPlan::Aggregate { .. }));
    }

    #[test]
    fn preserves_group_keys_and_interleaved_aggregate_outputs() {
        let column = ColumnRef {
            binding_id: RelationBindingId(0),
            table_id: TableId(1),
            column_id: ColumnId(1),
            relation_name: "users".into(),
            name: "team_id".into(),
            data_type: SemanticType::physical(PhysicalType::UInt64),
            nullable: true,
        };
        let count = netbadb_rel::AggregateExpr {
            function: netbadb_rel::AggregateFunction::Count,
            input: netbadb_rel::AggregateInput::All,
            output: netbadb_rel::DerivedField {
                name: "COUNT(*)".into(),
                data_type: SemanticType::physical(PhysicalType::UInt64),
                nullable: false,
            },
        };
        let logical = LogicalPlan::Aggregate {
            input: Box::new(LogicalPlan::Scan {
                binding_id: RelationBindingId(0),
                table_id: TableId(1),
                table_name: "users".into(),
                columns: vec![column.clone()],
            }),
            group_keys: vec![column.clone()],
            outputs: vec![
                netbadb_rel::AggregateOutput::Aggregate(count.clone()),
                netbadb_rel::AggregateOutput::GroupKey(column),
                netbadb_rel::AggregateOutput::Aggregate(count),
            ],
        };
        let physical = plan(&logical);
        assert!(matches!(
            physical.output_fields().as_slice(),
            [
                netbadb_rel::OutputField::Derived(_),
                netbadb_rel::OutputField::Source(_),
                netbadb_rel::OutputField::Derived(_)
            ]
        ));
        assert!(matches!(
            physical,
            PhysicalPlan::Aggregate {
                group_keys,
                outputs,
                ..
            } if group_keys.len() == 1 && outputs.len() == 3
        ));
    }

    fn test_column(column_id: u32, name: &str, nullable: bool) -> ColumnRef {
        typed_column(column_id, name, PhysicalType::Int64, nullable)
    }

    fn typed_column(
        column_id: u32,
        name: &str,
        physical: PhysicalType,
        nullable: bool,
    ) -> ColumnRef {
        ColumnRef {
            binding_id: RelationBindingId(7),
            table_id: TableId(1),
            column_id: ColumnId(column_id),
            relation_name: "u".into(),
            name: name.into(),
            data_type: SemanticType::physical(physical),
            nullable,
        }
    }

    fn column_expr(column: &ColumnRef) -> Expr {
        Expr {
            kind: ExprKind::Column(column.clone()),
            expr_type: ExprType {
                data_type: column.data_type.clone(),
                nullable: column.nullable,
            },
        }
    }

    fn cast_expr(expression: Expr, target: PhysicalType) -> Expr {
        let nullable = expression.expr_type.nullable;
        Expr {
            kind: ExprKind::Cast {
                expression: Box::new(expression),
            },
            expr_type: ExprType {
                data_type: SemanticType::physical(target),
                nullable,
            },
        }
    }

    fn literal(value: ScalarValue) -> Expr {
        let nullable = matches!(value, ScalarValue::Null);
        Expr {
            kind: ExprKind::Literal(value),
            expr_type: ExprType {
                data_type: SemanticType::physical(PhysicalType::Int64),
                nullable,
            },
        }
    }

    fn binary(operator: BinaryOp, left: Expr, right: Expr) -> Expr {
        Expr {
            kind: ExprKind::Binary {
                operator,
                left: Box::new(left),
                right: Box::new(right),
            },
            expr_type: ExprType {
                data_type: SemanticType::physical(PhysicalType::Bool),
                nullable: true,
            },
        }
    }

    fn filtered_scan(predicate: Expr, columns: Vec<ColumnRef>) -> LogicalPlan {
        LogicalPlan::Filter {
            input: Box::new(LogicalPlan::Scan {
                binding_id: RelationBindingId(7),
                table_id: TableId(1),
                table_name: "users".into(),
                columns,
            }),
            predicate,
        }
    }

    #[test]
    fn cross_physical_cast_does_not_create_index_zone_or_partition_constraints() {
        let mut legacy = test_column(1, "legacy", false);
        legacy.data_type = SemanticType::physical(PhysicalType::Text);
        let predicate = binary(
            BinaryOp::Eq,
            cast_expr(column_expr(&legacy), PhysicalType::Int64),
            literal(ScalarValue::Int64(42)),
        );

        assert!(
            super::find_point_constraint(
                &predicate,
                legacy.binding_id,
                legacy.table_id,
                legacy.column_id,
            )
            .is_none()
        );
        let mut columnar = Vec::new();
        super::collect_columnar_constraints(&predicate, &mut columnar);
        assert!(columnar.is_empty());

        let mut partition = super::PartitionConstraint::default();
        super::collect_partition_constraint(
            &predicate,
            legacy.binding_id,
            legacy.table_id,
            legacy.column_id,
            &mut partition,
        );
        assert!(partition.lower.is_none());
        assert!(partition.upper.is_none());
    }

    fn access_path(column_id: u32, page_id: u64) -> AccessPath {
        AccessPath {
            table_id: TableId(1),
            column_id: ColumnId(column_id),
            id: AccessPathId(page_id),
            capabilities: AccessPathCapabilities {
                point_lookup: true,
                range_lookup: true,
                ordered: true,
            },
            statistics: None,
            cost_hints: None,
        }
    }

    fn analyzed_path(
        column_id: u32,
        page_id: u64,
        distinct_non_null_keys: u64,
        null_count: u64,
        tree_height: u32,
    ) -> AccessPath {
        let mut path = access_path(column_id, page_id);
        path.statistics = Some(IndexStatistics {
            distinct_non_null_keys,
            null_count,
            tree_height,
        });
        path
    }

    fn analyzed_table(row_count: u64, managed_page_count: u64) -> TableAccessStatistics {
        TableAccessStatistics {
            table_id: TableId(1),
            statistics: Some(TableStatistics {
                row_count,
                managed_page_count,
            }),
        }
    }

    fn index_scan_input(plan: &PhysicalPlan) -> Option<&PhysicalPlan> {
        match plan {
            PhysicalPlan::Filter { input, .. } => Some(input),
            _ => None,
        }
    }

    fn bounded(column: &ColumnRef, lower: i64, upper: i64) -> Expr {
        binary(
            BinaryOp::And,
            binary(
                BinaryOp::GtEq,
                column_expr(column),
                literal(ScalarValue::Int64(lower)),
            ),
            binary(
                BinaryOp::Lt,
                column_expr(column),
                literal(ScalarValue::Int64(upper)),
            ),
        )
    }

    #[test]
    fn bounded_integer_ranges_require_statistics_and_compare_costs() {
        let id = test_column(1, "id", false);
        let access = [analyzed_path(1, 40, 10_000, 0, 2)];
        let table = [analyzed_table(10_000, 1_000)];
        let narrow = plan_with_statistics(
            &filtered_scan(bounded(&id, 5_000, 5_100), vec![id.clone()]),
            &table,
            &access,
        );
        assert!(matches!(
            index_scan_input(&narrow),
            Some(PhysicalPlan::RangeIndexScan {
                range: IndexRange {
                    lower: IndexBound::Included(ScalarValue::Int64(5_000)),
                    upper: IndexBound::Excluded(ScalarValue::Int64(5_100)),
                },
                ..
            })
        ));

        let wide = plan_with_statistics(
            &filtered_scan(bounded(&id, 2_500, 7_500), vec![id.clone()]),
            &table,
            &access,
        );
        assert!(matches!(
            index_scan_input(&wide),
            Some(PhysicalPlan::SeqScan { .. })
        ));
        let without_statistics = plan_with_access_paths(
            &filtered_scan(bounded(&id, 5_000, 5_100), vec![id]),
            &[access_path(1, 40)],
        );
        assert!(matches!(
            index_scan_input(&without_statistics),
            Some(PhysicalPlan::SeqScan { .. })
        ));
    }

    #[test]
    fn range_extraction_reverses_and_tightens_nested_bounds() {
        let id = test_column(1, "id", false);
        let predicate = binary(
            BinaryOp::And,
            binary(
                BinaryOp::And,
                binary(
                    BinaryOp::LtEq,
                    literal(ScalarValue::Int64(100)),
                    column_expr(&id),
                ),
                binary(
                    BinaryOp::Gt,
                    column_expr(&id),
                    literal(ScalarValue::Int64(200)),
                ),
            ),
            binary(
                BinaryOp::And,
                binary(
                    BinaryOp::Gt,
                    literal(ScalarValue::Int64(1_000)),
                    column_expr(&id),
                ),
                binary(
                    BinaryOp::LtEq,
                    column_expr(&id),
                    literal(ScalarValue::Int64(900)),
                ),
            ),
        );
        let physical = plan_with_statistics(
            &filtered_scan(predicate, vec![id]),
            &[analyzed_table(10_000, 2_000)],
            &[analyzed_path(1, 40, 10_000, 0, 2)],
        );
        assert!(matches!(
            index_scan_input(&physical),
            Some(PhysicalPlan::RangeIndexScan {
                range: IndexRange {
                    lower: IndexBound::Excluded(ScalarValue::Int64(200)),
                    upper: IndexBound::Included(ScalarValue::Int64(900)),
                },
                ..
            })
        ));
    }

    #[test]
    fn unsupported_range_shapes_remain_sequence_scans() {
        let analyzed = [analyzed_table(10_000, 2_000)];
        for physical in [PhysicalType::Int64, PhysicalType::Text, PhysicalType::Bool] {
            let column = typed_column(1, "value", physical, false);
            let (lower, upper) = match physical {
                PhysicalType::Int64 => (ScalarValue::Int64(10), ScalarValue::Int64(20)),
                PhysicalType::Text => {
                    (ScalarValue::Text("a".into()), ScalarValue::Text("b".into()))
                }
                PhysicalType::Bool => (ScalarValue::Bool(false), ScalarValue::Bool(true)),
                PhysicalType::Int8
                | PhysicalType::Int16
                | PhysicalType::Int32
                | PhysicalType::Int128
                | PhysicalType::UInt8
                | PhysicalType::UInt16
                | PhysicalType::UInt32
                | PhysicalType::UInt64
                | PhysicalType::UInt128
                | PhysicalType::Float32
                | PhysicalType::Float64
                | PhysicalType::Bytes => continue,
            };
            let predicate = binary(
                BinaryOp::And,
                binary(BinaryOp::GtEq, column_expr(&column), literal(lower)),
                binary(BinaryOp::Lt, column_expr(&column), literal(upper)),
            );
            let plan = plan_with_statistics(
                &filtered_scan(predicate, vec![column]),
                &analyzed,
                &[analyzed_path(1, 40, 10_000, 0, 2)],
            );
            if physical == PhysicalType::Int64 {
                assert!(matches!(
                    index_scan_input(&plan),
                    Some(PhysicalPlan::RangeIndexScan { .. })
                ));
            } else {
                assert!(matches!(
                    index_scan_input(&plan),
                    Some(PhysicalPlan::SeqScan { .. })
                ));
            }
        }

        let id = test_column(1, "id", false);
        let one_sided = plan_with_statistics(
            &filtered_scan(
                binary(
                    BinaryOp::GtEq,
                    column_expr(&id),
                    literal(ScalarValue::Int64(10)),
                ),
                vec![id.clone()],
            ),
            &analyzed,
            &[analyzed_path(1, 40, 10_000, 0, 2)],
        );
        assert!(matches!(
            index_scan_input(&one_sided),
            Some(PhysicalPlan::SeqScan { .. })
        ));
        let disjunction = plan_with_statistics(
            &filtered_scan(
                binary(
                    BinaryOp::Or,
                    binary(
                        BinaryOp::Lt,
                        column_expr(&id),
                        literal(ScalarValue::Int64(10)),
                    ),
                    binary(
                        BinaryOp::Gt,
                        column_expr(&id),
                        literal(ScalarValue::Int64(9_000)),
                    ),
                ),
                vec![id],
            ),
            &analyzed,
            &[analyzed_path(1, 40, 10_000, 0, 2)],
        );
        assert!(matches!(
            index_scan_input(&disjunction),
            Some(PhysicalPlan::SeqScan { .. })
        ));
    }

    #[test]
    fn integer_key_count_is_checked_at_signed_and_unsigned_boundaries() {
        assert_eq!(
            super::estimated_integer_key_count(&IndexRange {
                lower: IndexBound::Included(ScalarValue::Int64(i64::MIN)),
                upper: IndexBound::Included(ScalarValue::Int64(i64::MAX)),
            }),
            Some(1_u128 << 64)
        );
        assert_eq!(
            super::estimated_integer_key_count(&IndexRange {
                lower: IndexBound::Included(ScalarValue::UInt64(0)),
                upper: IndexBound::Included(ScalarValue::UInt64(u64::MAX)),
            }),
            Some(1_u128 << 64)
        );
        assert_eq!(
            super::estimated_integer_key_count(&IndexRange {
                lower: IndexBound::Excluded(ScalarValue::Int64(10)),
                upper: IndexBound::Excluded(ScalarValue::Int64(5)),
            }),
            Some(0)
        );
    }

    #[test]
    fn point_range_and_contradiction_candidates_share_one_costed_choice() {
        let id = test_column(1, "id", false);
        let bucket = test_column(2, "bucket", false);
        let point_and_range = binary(
            BinaryOp::And,
            binary(
                BinaryOp::Eq,
                column_expr(&id),
                literal(ScalarValue::Int64(42)),
            ),
            bounded(&id, 0, 100),
        );
        let point = plan_with_statistics(
            &filtered_scan(point_and_range, vec![id.clone()]),
            &[analyzed_table(10_000, 2_000)],
            &[analyzed_path(1, 40, 10_000, 0, 2)],
        );
        assert!(matches!(
            index_scan_input(&point),
            Some(PhysicalPlan::IndexScan {
                key: ScalarValue::Int64(42),
                ..
            })
        ));

        let contradiction = plan_with_statistics(
            &filtered_scan(
                binary(
                    BinaryOp::And,
                    binary(
                        BinaryOp::Gt,
                        column_expr(&id),
                        literal(ScalarValue::Int64(100)),
                    ),
                    binary(
                        BinaryOp::Lt,
                        column_expr(&id),
                        literal(ScalarValue::Int64(100)),
                    ),
                ),
                vec![id.clone()],
            ),
            &[analyzed_table(10_000, 2_000)],
            &[analyzed_path(1, 40, 10_000, 0, 2)],
        );
        assert!(matches!(
            index_scan_input(&contradiction),
            Some(PhysicalPlan::RangeIndexScan { .. })
        ));

        let mixed = plan_with_statistics(
            &filtered_scan(
                binary(
                    BinaryOp::And,
                    bounded(&id, 0, 100),
                    binary(
                        BinaryOp::Eq,
                        column_expr(&bucket),
                        literal(ScalarValue::Int64(7)),
                    ),
                ),
                vec![id, bucket],
            ),
            &[analyzed_table(10_000, 2_000)],
            &[
                analyzed_path(1, 40, 10_000, 0, 2),
                analyzed_path(2, 50, 10_000, 0, 2),
            ],
        );
        assert!(matches!(
            index_scan_input(&mixed),
            Some(PhysicalPlan::IndexScan {
                access_path: AccessPathId(50),
                ..
            })
        ));
    }

    #[test]
    fn bounded_uint64_range_is_costable() {
        let id = typed_column(1, "id", PhysicalType::UInt64, false);
        let predicate = binary(
            BinaryOp::And,
            binary(
                BinaryOp::GtEq,
                column_expr(&id),
                literal(ScalarValue::UInt64(u64::MAX - 9)),
            ),
            binary(
                BinaryOp::LtEq,
                column_expr(&id),
                literal(ScalarValue::UInt64(u64::MAX)),
            ),
        );
        let physical = plan_with_statistics(
            &filtered_scan(predicate, vec![id]),
            &[analyzed_table(10_000, 2_000)],
            &[analyzed_path(1, 40, 10_000, 0, 2)],
        );
        assert!(matches!(
            index_scan_input(&physical),
            Some(PhysicalPlan::RangeIndexScan { .. })
        ));
    }

    #[test]
    fn point_equality_and_commuted_equality_retain_the_full_filter() {
        let id = test_column(1, "id", false);
        let path = access_path(1, 40);
        for predicate in [
            binary(
                BinaryOp::Eq,
                column_expr(&id),
                literal(ScalarValue::Int64(42)),
            ),
            binary(
                BinaryOp::Eq,
                literal(ScalarValue::Int64(42)),
                column_expr(&id),
            ),
        ] {
            let logical = filtered_scan(predicate.clone(), vec![id.clone()]);
            let physical = plan_with_access_paths(&logical, std::slice::from_ref(&path));
            assert!(matches!(
                &physical,
                PhysicalPlan::Filter { predicate: actual, .. } if actual == &predicate
            ));
            assert!(matches!(
                index_scan_input(&physical),
                Some(PhysicalPlan::IndexScan {
                    binding_id: RelationBindingId(7),
                    access_path,
                    key: ScalarValue::Int64(42),
                    index_column,
                    ..
                }) if *access_path == path.id && index_column.relation_name == "u"
            ));
            assert_eq!(
                physical.output_fields(),
                vec![netbadb_rel::OutputField::Source(id.clone())]
            );
        }
    }

    #[test]
    fn access_path_capabilities_gate_physical_lookup_selection() {
        let id = test_column(1, "id", false);
        let logical = filtered_scan(
            binary(
                BinaryOp::Eq,
                column_expr(&id),
                literal(ScalarValue::Int64(42)),
            ),
            vec![id],
        );
        let mut path = access_path(1, 40);
        path.capabilities.point_lookup = false;
        let physical = plan_with_access_paths(&logical, &[path]);
        assert!(matches!(
            index_scan_input(&physical),
            Some(PhysicalPlan::SeqScan { .. })
        ));
    }

    #[test]
    fn null_and_non_point_predicates_only_use_safe_access_paths() {
        let nullable = test_column(1, "value", true);
        let required = test_column(2, "required", false);
        let path_nullable = access_path(1, 40);
        let path_required = access_path(2, 41);

        let is_null = Expr {
            kind: ExprKind::IsNull {
                expression: Box::new(column_expr(&nullable)),
                negated: false,
            },
            expr_type: ExprType {
                data_type: SemanticType::physical(PhysicalType::Bool),
                nullable: false,
            },
        };
        let physical = plan_with_access_paths(
            &filtered_scan(is_null, vec![nullable.clone(), required.clone()]),
            &[path_nullable.clone(), path_required.clone()],
        );
        assert!(matches!(
            index_scan_input(&physical),
            Some(PhysicalPlan::IndexScan {
                key: ScalarValue::Null,
                ..
            })
        ));

        let unsupported = [
            binary(
                BinaryOp::Eq,
                column_expr(&nullable),
                literal(ScalarValue::Null),
            ),
            Expr {
                kind: ExprKind::IsNull {
                    expression: Box::new(column_expr(&required)),
                    negated: false,
                },
                expr_type: ExprType {
                    data_type: SemanticType::physical(PhysicalType::Bool),
                    nullable: false,
                },
            },
            Expr {
                kind: ExprKind::IsNull {
                    expression: Box::new(column_expr(&nullable)),
                    negated: true,
                },
                expr_type: ExprType {
                    data_type: SemanticType::physical(PhysicalType::Bool),
                    nullable: false,
                },
            },
            binary(
                BinaryOp::Lt,
                column_expr(&nullable),
                literal(ScalarValue::Int64(42)),
            ),
            binary(
                BinaryOp::Or,
                binary(
                    BinaryOp::Eq,
                    column_expr(&nullable),
                    literal(ScalarValue::Int64(1)),
                ),
                binary(
                    BinaryOp::Eq,
                    column_expr(&nullable),
                    literal(ScalarValue::Int64(2)),
                ),
            ),
        ];
        for predicate in unsupported {
            let physical = plan_with_access_paths(
                &filtered_scan(predicate, vec![nullable.clone(), required.clone()]),
                &[path_nullable.clone(), path_required.clone()],
            );
            assert!(matches!(
                index_scan_input(&physical),
                Some(PhysicalPlan::SeqScan { .. })
            ));
        }
    }

    #[test]
    fn and_uses_first_eligible_registered_path_not_predicate_order() {
        let team = test_column(2, "team_id", false);
        let name = test_column(3, "name", false);
        let predicate = binary(
            BinaryOp::And,
            binary(
                BinaryOp::Eq,
                column_expr(&name),
                literal(ScalarValue::Int64(9)),
            ),
            binary(
                BinaryOp::Eq,
                column_expr(&team),
                literal(ScalarValue::Int64(10)),
            ),
        );
        let physical = plan_with_access_paths(
            &filtered_scan(predicate, vec![team, name]),
            &[access_path(2, 50), access_path(3, 60)],
        );
        assert!(matches!(
            index_scan_input(&physical),
            Some(PhysicalPlan::IndexScan {
                access_path: AccessPathId(50),
                key: ScalarValue::Int64(10),
                ..
            })
        ));
    }

    #[test]
    fn analyzed_costs_compare_point_indexes_with_sequence_scan() {
        let id = test_column(1, "id", false);
        let predicate = binary(
            BinaryOp::Eq,
            column_expr(&id),
            literal(ScalarValue::Int64(42)),
        );
        let logical = filtered_scan(predicate, vec![id]);

        let small = plan_with_statistics(
            &logical,
            &[analyzed_table(1, 3)],
            &[analyzed_path(1, 40, 1, 0, 1)],
        );
        assert!(matches!(
            index_scan_input(&small),
            Some(PhysicalPlan::SeqScan { .. })
        ));

        let selective = plan_with_statistics(
            &logical,
            &[analyzed_table(1_000, 100)],
            &[analyzed_path(1, 40, 1_000, 0, 1)],
        );
        assert!(matches!(
            index_scan_input(&selective),
            Some(PhysicalPlan::IndexScan { .. })
        ));

        let duplicate_heavy = plan_with_statistics(
            &logical,
            &[analyzed_table(1_000, 10)],
            &[analyzed_path(1, 40, 2, 0, 1)],
        );
        assert!(matches!(
            index_scan_input(&duplicate_heavy),
            Some(PhysicalPlan::SeqScan { .. })
        ));
    }

    #[test]
    fn storage_neutral_cost_hints_adjust_point_probe_cost() {
        let id = test_column(1, "id", false);
        let logical = filtered_scan(
            binary(
                BinaryOp::Eq,
                column_expr(&id),
                literal(ScalarValue::Int64(7)),
            ),
            vec![id],
        );
        let table = [analyzed_table(100, 5)];
        let mut path = analyzed_path(1, 40, 100, 0, 10);
        assert!(matches!(
            index_scan_input(&plan_with_statistics(&logical, &table, &[path.clone()])),
            Some(PhysicalPlan::SeqScan { .. })
        ));
        path.cost_hints = Some(AccessCostHints {
            point_probe_base_cost: 1,
            expected_point_io: 1,
            range_startup_cost: 2,
            sequential_unit_cost: 1,
        });
        assert!(matches!(
            index_scan_input(&plan_with_statistics(&logical, &table, &[path])),
            Some(PhysicalPlan::IndexScan { .. })
        ));
    }

    #[test]
    fn analyzed_null_cost_uses_null_count() {
        let nullable = test_column(2, "team_id", true);
        let logical = filtered_scan(
            Expr {
                kind: ExprKind::IsNull {
                    expression: Box::new(column_expr(&nullable)),
                    negated: false,
                },
                expr_type: ExprType {
                    data_type: SemanticType::physical(PhysicalType::Bool),
                    nullable: false,
                },
            },
            vec![nullable],
        );
        let sparse = plan_with_statistics(
            &logical,
            &[analyzed_table(1_000, 100)],
            &[analyzed_path(2, 40, 999, 1, 1)],
        );
        assert!(matches!(
            index_scan_input(&sparse),
            Some(PhysicalPlan::IndexScan { .. })
        ));
        let dense = plan_with_statistics(
            &logical,
            &[analyzed_table(1_000, 20)],
            &[analyzed_path(2, 40, 900, 100, 1)],
        );
        assert!(matches!(
            index_scan_input(&dense),
            Some(PhysicalPlan::SeqScan { .. })
        ));
    }

    #[test]
    fn analyzed_candidates_use_cost_then_registration_order_and_ignore_unknowns() {
        let team = test_column(2, "team_id", false);
        let name = test_column(3, "name", false);
        let logical = filtered_scan(
            binary(
                BinaryOp::And,
                binary(
                    BinaryOp::Eq,
                    column_expr(&team),
                    literal(ScalarValue::Int64(10)),
                ),
                binary(
                    BinaryOp::Eq,
                    column_expr(&name),
                    literal(ScalarValue::Int64(9)),
                ),
            ),
            vec![team, name],
        );
        let table = [analyzed_table(1_000, 100)];
        let cheaper_later = plan_with_statistics(
            &logical,
            &table,
            &[
                analyzed_path(2, 50, 2, 0, 1),
                analyzed_path(3, 60, 1_000, 0, 1),
            ],
        );
        assert!(matches!(
            index_scan_input(&cheaper_later),
            Some(PhysicalPlan::IndexScan {
                access_path: AccessPathId(60),
                ..
            })
        ));

        let tied = plan_with_statistics(
            &logical,
            &table,
            &[
                analyzed_path(2, 50, 1_000, 0, 1),
                analyzed_path(3, 60, 1_000, 0, 1),
            ],
        );
        assert!(matches!(
            index_scan_input(&tied),
            Some(PhysicalPlan::IndexScan {
                access_path: AccessPathId(50),
                ..
            })
        ));

        let unknown_first = access_path(2, 50);
        let known_second = analyzed_path(3, 60, 1_000, 0, 1);
        let partial = plan_with_statistics(&logical, &table, &[unknown_first, known_second]);
        assert!(matches!(
            index_scan_input(&partial),
            Some(PhysicalPlan::IndexScan {
                access_path: AccessPathId(60),
                ..
            })
        ));

        let only_unknown = plan_with_statistics(&logical, &table, &[access_path(2, 50)]);
        assert!(matches!(
            index_scan_input(&only_unknown),
            Some(PhysicalPlan::IndexScan {
                access_path: AccessPathId(50),
                ..
            })
        ));
    }

    #[test]
    fn dml_uses_the_same_costed_access_path_context() {
        let id = test_column(1, "id", false);
        let logical = LogicalStatement::Delete {
            input: filtered_scan(
                binary(
                    BinaryOp::Eq,
                    column_expr(&id),
                    literal(ScalarValue::Int64(42)),
                ),
                vec![id],
            ),
            table_id: TableId(1),
        };
        assert!(matches!(
            plan_statement_with_statistics(
                &logical,
                &[analyzed_table(1_000, 3)],
                &[analyzed_path(1, 40, 1_000, 0, 1)],
            ),
            PhysicalStatement::Delete {
                input: PhysicalPlan::Filter { input, .. },
                ..
            } if matches!(*input, PhysicalPlan::SeqScan { .. })
        ));
    }

    #[test]
    fn statement_planning_uses_access_paths_for_dml_inputs_only() {
        let id = test_column(1, "id", false);
        let input = filtered_scan(
            binary(
                BinaryOp::Eq,
                column_expr(&id),
                literal(ScalarValue::Int64(42)),
            ),
            vec![id],
        );
        let logical = LogicalStatement::Delete {
            input,
            table_id: TableId(1),
        };
        assert!(matches!(
            plan_statement_with_access_paths(&logical, &[access_path(1, 40)]),
            PhysicalStatement::Delete {
                input: PhysicalPlan::Filter { input, .. },
                ..
            } if matches!(*input, PhysicalPlan::IndexScan { .. })
        ));
        assert!(matches!(
            plan_statement(&logical),
            PhysicalStatement::Delete {
                input: PhysicalPlan::Filter { input, .. },
                ..
            } if matches!(*input, PhysicalPlan::SeqScan { .. })
        ));
    }

    #[test]
    fn enclosing_sort_and_aggregate_preserve_the_point_scan_without_elision() {
        let id = test_column(1, "id", false);
        let filtered = filtered_scan(
            binary(
                BinaryOp::Eq,
                column_expr(&id),
                literal(ScalarValue::Int64(42)),
            ),
            vec![id.clone()],
        );
        let sorted = LogicalPlan::Sort {
            input: Box::new(filtered.clone()),
            keys: vec![netbadb_rel::SortKey {
                column: id.clone(),
                direction: netbadb_rel::SortDirection::Desc,
                null_order: netbadb_rel::NullOrder::Last,
            }],
        };
        let sorted = plan_with_access_paths(&sorted, &[access_path(1, 40)]);
        let PhysicalPlan::Sort { input, .. } = sorted else {
            panic!("expected sort");
        };
        assert!(matches!(
            index_scan_input(&input),
            Some(PhysicalPlan::IndexScan { .. })
        ));

        let aggregate = LogicalPlan::Aggregate {
            input: Box::new(filtered),
            group_keys: Vec::new(),
            outputs: vec![netbadb_rel::AggregateOutput::Aggregate(
                netbadb_rel::AggregateExpr {
                    function: netbadb_rel::AggregateFunction::Count,
                    input: netbadb_rel::AggregateInput::All,
                    output: netbadb_rel::DerivedField {
                        name: "COUNT(*)".into(),
                        data_type: SemanticType::physical(PhysicalType::UInt64),
                        nullable: false,
                    },
                },
            )],
        };
        let aggregate = plan_with_access_paths(&aggregate, &[access_path(1, 40)]);
        let PhysicalPlan::Aggregate { input, .. } = aggregate else {
            panic!("expected aggregate");
        };
        assert!(matches!(
            index_scan_input(&input),
            Some(PhysicalPlan::IndexScan { .. })
        ));
    }

    fn base_columns(plan: &PhysicalPlan) -> &[ColumnRef] {
        match plan {
            PhysicalPlan::SeqScan { columns, .. }
            | PhysicalPlan::ColumnarScan { columns, .. }
            | PhysicalPlan::IndexScan { columns, .. }
            | PhysicalPlan::RangeIndexScan { columns, .. }
            | PhysicalPlan::PartitionedScan { columns, .. } => columns,
            PhysicalPlan::Filter { input, .. }
            | PhysicalPlan::Sort { input, .. }
            | PhysicalPlan::Project { input, .. }
            | PhysicalPlan::ScalarProject { input, .. }
            | PhysicalPlan::Aggregate { input, .. }
            | PhysicalPlan::Limit { input, .. } => base_columns(input),
            PhysicalPlan::OneRow
            | PhysicalPlan::NestedLoopJoin { .. }
            | PhysicalPlan::IndexNestedLoopJoin { .. }
            | PhysicalPlan::HashJoin { .. } => {
                panic!("expected one base scan")
            }
        }
    }

    fn base_plan(plan: &PhysicalPlan) -> &PhysicalPlan {
        match plan {
            PhysicalPlan::Filter { input, .. }
            | PhysicalPlan::Sort { input, .. }
            | PhysicalPlan::Project { input, .. }
            | PhysicalPlan::Aggregate { input, .. }
            | PhysicalPlan::Limit { input, .. } => base_plan(input),
            _ => plan,
        }
    }

    fn column_ids(columns: &[ColumnRef]) -> Vec<ColumnId> {
        columns.iter().map(|column| column.column_id).collect()
    }

    #[test]
    fn query_pruning_preserves_projection_filter_sort_and_aggregate_requirements() {
        let id = test_column(1, "id", false);
        let active = test_column(2, "active", false);
        let payload = test_column(3, "payload", false);
        let scan = || LogicalPlan::Scan {
            binding_id: RelationBindingId(0),
            table_id: TableId(1),
            table_name: "items".into(),
            columns: vec![id.clone(), active.clone(), payload.clone()],
        };

        let duplicate_project = LogicalPlan::Project {
            input: Box::new(scan()),
            columns: vec![id.clone(), id.clone()],
        };
        let physical = plan(&duplicate_project);
        assert_eq!(column_ids(base_columns(&physical)), vec![ColumnId(1)]);
        assert_eq!(physical.output_fields().len(), 2);

        let filtered = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Filter {
                input: Box::new(scan()),
                predicate: binary(
                    BinaryOp::Eq,
                    column_expr(&active),
                    literal(ScalarValue::Int64(1)),
                ),
            }),
            columns: vec![id.clone()],
        };
        assert_eq!(
            column_ids(base_columns(&plan(&filtered))),
            vec![ColumnId(1), ColumnId(2)]
        );

        let sorted = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Sort {
                input: Box::new(scan()),
                keys: vec![netbadb_rel::SortKey {
                    column: payload.clone(),
                    direction: netbadb_rel::SortDirection::Asc,
                    null_order: netbadb_rel::NullOrder::First,
                }],
            }),
            columns: vec![id.clone()],
        };
        assert_eq!(
            column_ids(base_columns(&plan(&sorted))),
            vec![ColumnId(1), ColumnId(3)]
        );

        let count = |input| {
            netbadb_rel::AggregateOutput::Aggregate(netbadb_rel::AggregateExpr {
                function: netbadb_rel::AggregateFunction::Count,
                input,
                output: netbadb_rel::DerivedField {
                    name: "count".into(),
                    data_type: SemanticType::physical(PhysicalType::UInt64),
                    nullable: false,
                },
            })
        };
        let count_star = LogicalPlan::Aggregate {
            input: Box::new(scan()),
            group_keys: Vec::new(),
            outputs: vec![count(netbadb_rel::AggregateInput::All)],
        };
        assert!(base_columns(&plan(&count_star)).is_empty());
        let count_payload = LogicalPlan::Aggregate {
            input: Box::new(scan()),
            group_keys: vec![active.clone()],
            outputs: vec![
                netbadb_rel::AggregateOutput::GroupKey(active),
                count(netbadb_rel::AggregateInput::Column(payload)),
            ],
        };
        assert_eq!(
            column_ids(base_columns(&plan(&count_payload))),
            vec![ColumnId(2), ColumnId(3)]
        );
    }

    #[test]
    fn point_range_reads_prune_columns_but_dml_inputs_remain_complete() {
        let id = test_column(1, "id", false);
        let payload = test_column(2, "payload", false);
        let extra = test_column(3, "extra", false);
        let columns = vec![id.clone(), payload.clone(), extra.clone()];
        let point_input = filtered_scan(
            binary(
                BinaryOp::Eq,
                column_expr(&id),
                literal(ScalarValue::Int64(42)),
            ),
            columns.clone(),
        );
        let point = LogicalPlan::Project {
            input: Box::new(point_input.clone()),
            columns: vec![payload.clone()],
        };
        let point = plan_with_access_paths(&point, &[access_path(1, 40)]);
        assert_eq!(
            column_ids(base_columns(&point)),
            vec![ColumnId(1), ColumnId(2)]
        );
        assert!(matches!(base_plan(&point), PhysicalPlan::IndexScan { .. }));

        let range_input = filtered_scan(
            binary(
                BinaryOp::And,
                binary(
                    BinaryOp::GtEq,
                    column_expr(&id),
                    literal(ScalarValue::Int64(10)),
                ),
                binary(
                    BinaryOp::Lt,
                    column_expr(&id),
                    literal(ScalarValue::Int64(20)),
                ),
            ),
            columns.clone(),
        );
        let range = LogicalPlan::Project {
            input: Box::new(range_input),
            columns: vec![payload],
        };
        let range = plan_with_statistics(
            &range,
            &[analyzed_table(1_000, 100)],
            &[analyzed_path(1, 40, 1_000, 0, 1)],
        );
        assert_eq!(
            column_ids(base_columns(&range)),
            vec![ColumnId(1), ColumnId(2)]
        );
        assert!(matches!(
            base_plan(&range),
            PhysicalPlan::RangeIndexScan { .. }
        ));

        let delete = plan_statement_with_access_paths(
            &LogicalStatement::Delete {
                input: point_input,
                table_id: TableId(1),
            },
            &[access_path(1, 40)],
        );
        let PhysicalStatement::Delete { input, .. } = delete else {
            panic!("expected delete")
        };
        assert_eq!(
            column_ids(base_columns(&input)),
            vec![ColumnId(1), ColumnId(2), ColumnId(3)]
        );
    }

    #[test]
    fn join_pruning_splits_self_join_bindings_and_keeps_hash_residual_columns() {
        let left_id = join_column_ref(
            10,
            1,
            1,
            "id",
            SemanticType::physical(PhysicalType::Int64),
            false,
        );
        let left_key = join_column_ref(
            10,
            1,
            2,
            "key",
            SemanticType::physical(PhysicalType::Int64),
            false,
        );
        let left_extra = join_column_ref(
            10,
            1,
            3,
            "extra",
            SemanticType::physical(PhysicalType::Bool),
            false,
        );
        let right_id = join_column_ref(
            20,
            1,
            1,
            "id",
            SemanticType::physical(PhysicalType::Int64),
            false,
        );
        let right_key = join_column_ref(
            20,
            1,
            2,
            "key",
            SemanticType::physical(PhysicalType::Int64),
            false,
        );
        let right_active = join_column_ref(
            20,
            1,
            3,
            "active",
            SemanticType::physical(PhysicalType::Bool),
            false,
        );
        let predicate = join_binary(
            BinaryOp::And,
            join_binary(BinaryOp::Eq, join_expr(&left_key), join_expr(&right_key)),
            join_binary(
                BinaryOp::Eq,
                join_expr(&right_active),
                join_literal(ScalarValue::Bool(true), PhysicalType::Bool),
            ),
        );
        let joined = logical_join(
            join_scan(10, 1, vec![left_id.clone(), left_key.clone(), left_extra]),
            join_scan(
                20,
                1,
                vec![right_id, right_key.clone(), right_active.clone()],
            ),
            predicate,
        );
        let physical = plan_with_statistics(
            &LogicalPlan::Project {
                input: Box::new(joined),
                columns: vec![left_id.clone()],
            },
            &[join_table_statistics(1, 500)],
            &[],
        );
        let PhysicalPlan::Project { input, .. } = physical else {
            panic!("expected project")
        };
        let PhysicalPlan::HashJoin {
            left,
            right,
            columns,
            ..
        } = *input
        else {
            panic!("expected hash join")
        };
        assert_eq!(
            column_ids(base_columns(&left)),
            vec![ColumnId(1), ColumnId(2)]
        );
        assert_eq!(
            column_ids(base_columns(&right)),
            vec![ColumnId(2), ColumnId(3)]
        );
        assert_eq!(
            columns
                .iter()
                .map(|column| (column.binding_id, column.column_id))
                .collect::<Vec<_>>(),
            vec![
                (RelationBindingId(10), ColumnId(1)),
                (RelationBindingId(10), ColumnId(2)),
                (RelationBindingId(20), ColumnId(2)),
                (RelationBindingId(20), ColumnId(3)),
            ]
        );
    }

    #[test]
    fn chained_join_propagates_outer_predicate_columns_through_the_inner_join() {
        let a_id = join_column_ref(
            10,
            1,
            1,
            "id",
            SemanticType::physical(PhysicalType::Int64),
            false,
        );
        let b_id = join_column_ref(
            20,
            2,
            1,
            "id",
            SemanticType::physical(PhysicalType::Int64),
            false,
        );
        let b_a_id = join_column_ref(
            20,
            2,
            2,
            "a_id",
            SemanticType::physical(PhysicalType::Int64),
            false,
        );
        let c_b_id = join_column_ref(
            30,
            3,
            1,
            "b_id",
            SemanticType::physical(PhysicalType::Int64),
            false,
        );
        let c_name = join_column_ref(
            30,
            3,
            2,
            "name",
            SemanticType::physical(PhysicalType::Text),
            false,
        );
        let inner = logical_join(
            join_scan(10, 1, vec![a_id.clone()]),
            join_scan(20, 2, vec![b_id.clone(), b_a_id.clone()]),
            join_binary(BinaryOp::Eq, join_expr(&a_id), join_expr(&b_a_id)),
        );
        let outer = logical_join(
            inner,
            join_scan(30, 3, vec![c_b_id.clone(), c_name.clone()]),
            join_binary(BinaryOp::Eq, join_expr(&b_id), join_expr(&c_b_id)),
        );
        let physical = plan(&LogicalPlan::Project {
            input: Box::new(outer),
            columns: vec![a_id, c_name],
        });
        let PhysicalPlan::Project { input, .. } = physical else {
            panic!("expected project")
        };
        let PhysicalPlan::NestedLoopJoin { left, right, .. } = *input else {
            panic!("expected outer join")
        };
        assert_eq!(
            column_ids(base_columns(&right)),
            vec![ColumnId(1), ColumnId(2)]
        );
        let PhysicalPlan::NestedLoopJoin {
            left,
            right,
            columns,
            ..
        } = *left
        else {
            panic!("expected inner join")
        };
        assert_eq!(column_ids(base_columns(&left)), vec![ColumnId(1)]);
        assert_eq!(
            column_ids(base_columns(&right)),
            vec![ColumnId(1), ColumnId(2)]
        );
        assert_eq!(
            columns
                .iter()
                .map(|column| (column.binding_id, column.column_id))
                .collect::<Vec<_>>(),
            vec![
                (RelationBindingId(10), ColumnId(1)),
                (RelationBindingId(20), ColumnId(1)),
                (RelationBindingId(20), ColumnId(2)),
            ]
        );
    }

    #[test]
    fn execution_estimate_is_frozen_from_the_supplied_planning_snapshot() {
        let column = test_column(1, "id", false);
        let plan = PhysicalPlan::ColumnarScan {
            binding_id: column.binding_id,
            table_id: column.table_id,
            table_name: "items".to_owned(),
            columns: vec![column],
            projection_id: ColumnarProjectionId(1),
            generation: ColumnarGeneration(1),
            source_storage_id: StorageId(1),
        };
        let initial = columnar_snapshot(
            vec![ColumnId(1)],
            vec![planning_group(256, &[ColumnId(1)], 4_096, 0, 255)],
        );
        let estimates = estimate_execution_accesses(
            &plan,
            &[TableAccessStatistics {
                table_id: TableId(1),
                statistics: Some(TableStatistics {
                    row_count: 256,
                    managed_page_count: 100,
                }),
            }],
            &[],
            &[],
            std::slice::from_ref(&initial),
        );
        let frozen = estimates[0].estimated_work_units;

        let mut latest = initial;
        latest.delta_segment_count = 10_000;
        let recomputed = estimate_execution_accesses(
            &plan,
            &[TableAccessStatistics {
                table_id: TableId(1),
                statistics: Some(TableStatistics {
                    row_count: 256,
                    managed_page_count: 1,
                }),
            }],
            &[],
            &[],
            &[latest],
        );
        assert_eq!(estimates[0].estimated_work_units, frozen);
        assert_ne!(
            estimates[0].estimated_work_units,
            recomputed[0].estimated_work_units
        );
    }

    #[test]
    fn planner_evaluates_raw_columnar_evidence_in_canonical_work_units() {
        let column = test_column(1, "id", false);
        let plan = PhysicalPlan::ColumnarScan {
            binding_id: column.binding_id,
            table_id: column.table_id,
            table_name: "items".to_owned(),
            columns: vec![column],
            projection_id: ColumnarProjectionId(1),
            generation: ColumnarGeneration(1),
            source_storage_id: StorageId(1),
        };
        let projection = columnar_snapshot(
            vec![ColumnId(1)],
            vec![planning_group(256, &[ColumnId(1)], 4_096, 0, 255)],
        );
        let estimate = estimate_execution_accesses(&plan, &[], &[], &[], &[projection]).remove(0);
        assert_eq!(
            evaluate_actual_access_work(
                &estimate,
                PlannerActualAccessEvidence {
                    columnar: Some(PlannerColumnarExecutionEvidence {
                        row_groups_read: 1,
                        rows_read: 256,
                        physical_bytes_read: 4_096,
                        ..PlannerColumnarExecutionEvidence::default()
                    }),
                    ..PlannerActualAccessEvidence::default()
                }
            ),
            Some(5)
        );
        assert_eq!(
            evaluate_actual_access_work(
                &estimate,
                PlannerActualAccessEvidence {
                    incomplete: true,
                    ..PlannerActualAccessEvidence::default()
                }
            ),
            None
        );
        assert_eq!(
            evaluate_actual_access_work(
                &estimate,
                PlannerActualAccessEvidence {
                    columnar: Some(PlannerColumnarExecutionEvidence {
                        row_groups_read: u64::MAX,
                        ..PlannerColumnarExecutionEvidence::default()
                    }),
                    ..PlannerActualAccessEvidence::default()
                }
            ),
            None
        );
    }
}
