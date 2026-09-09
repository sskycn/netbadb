use std::error::Error;
use std::fmt;

use netbadb_index::{IndexBound, IndexRange};
use netbadb_rel::{
    AggregateOutputShape, JoinKind, QueryColumnShape, QueryExpressionShape,
    QueryProjectedExpressionShape, QueryShapeCanonicalizer, QueryShapeError, QuerySortKeyShape,
};
use netbadb_types::{
    AccessPathId, ColumnId, ColumnarProjectionId, PartitionId, ScalarValue, SemanticType,
    StorageId, TableId,
};

use crate::{PartitionAccessPlan, PhysicalPlan};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PlanScalarShape {
    pub data_type: SemanticType,
    pub is_null: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PlanIndexBoundShape {
    Unbounded,
    Included(PlanScalarShape),
    Excluded(PlanScalarShape),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PlanIndexRangeShape {
    pub lower: PlanIndexBoundShape,
    pub upper: PlanIndexBoundShape,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PlanPartitionAccessVariant {
    SeqScan,
    IndexPoint {
        index_column: QueryColumnShape,
        access_path: AccessPathId,
        key: PlanScalarShape,
    },
    IndexRange {
        index_column: QueryColumnShape,
        access_path: AccessPathId,
        range: PlanIndexRangeShape,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PlanPartitionVariant {
    pub partition_id: PartitionId,
    pub storage_id: StorageId,
    pub access: PlanPartitionAccessVariant,
}

/// Typed physical strategy identity for workload grouping.
///
/// Runtime node ordinals, names, literal payloads, and Columnar generations
/// are deliberately absent. Exact generation remains execution-target
/// evidence rather than long-lived strategy identity.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PlanVariant {
    OneRow,
    SeqScan {
        binding: netbadb_rel::CanonicalBindingOrdinal,
        table_id: TableId,
        columns: Vec<QueryColumnShape>,
    },
    ColumnarScan {
        binding: netbadb_rel::CanonicalBindingOrdinal,
        table_id: TableId,
        columns: Vec<QueryColumnShape>,
        projection_id: ColumnarProjectionId,
        source_storage_id: StorageId,
    },
    IndexPoint {
        binding: netbadb_rel::CanonicalBindingOrdinal,
        table_id: TableId,
        columns: Vec<QueryColumnShape>,
        index_column: QueryColumnShape,
        access_path: AccessPathId,
        key: PlanScalarShape,
    },
    IndexRange {
        binding: netbadb_rel::CanonicalBindingOrdinal,
        table_id: TableId,
        columns: Vec<QueryColumnShape>,
        index_column: QueryColumnShape,
        access_path: AccessPathId,
        range: PlanIndexRangeShape,
    },
    PartitionedAccess {
        binding: netbadb_rel::CanonicalBindingOrdinal,
        table_id: TableId,
        columns: Vec<QueryColumnShape>,
        partition_key: ColumnId,
        total_partitions: u64,
        partitions: Vec<PlanPartitionVariant>,
    },
    NestedLoopJoin {
        left: Box<PlanVariant>,
        right: Box<PlanVariant>,
        kind: JoinKind,
        predicate: QueryExpressionShape,
        columns: Vec<QueryColumnShape>,
    },
    IndexNestedLoopJoin {
        left: Box<PlanVariant>,
        right_binding: netbadb_rel::CanonicalBindingOrdinal,
        right_table_id: TableId,
        right_columns: Vec<QueryColumnShape>,
        kind: JoinKind,
        left_key: QueryColumnShape,
        right_key: QueryColumnShape,
        right_access_path: AccessPathId,
        predicate: QueryExpressionShape,
        columns: Vec<QueryColumnShape>,
    },
    HashJoin {
        left: Box<PlanVariant>,
        right: Box<PlanVariant>,
        kind: JoinKind,
        left_key: QueryColumnShape,
        right_key: QueryColumnShape,
        predicate: QueryExpressionShape,
        columns: Vec<QueryColumnShape>,
    },
    Filter {
        input: Box<PlanVariant>,
        predicate: QueryExpressionShape,
    },
    Sort {
        input: Box<PlanVariant>,
        keys: Vec<QuerySortKeyShape>,
    },
    Project {
        input: Box<PlanVariant>,
        columns: Vec<QueryColumnShape>,
    },
    ScalarProject {
        input: Box<PlanVariant>,
        expressions: Vec<QueryProjectedExpressionShape>,
    },
    Aggregate {
        input: Box<PlanVariant>,
        group_keys: Vec<QueryColumnShape>,
        outputs: Vec<AggregateOutputShape>,
    },
    Limit {
        input: Box<PlanVariant>,
        limit: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanVariantError {
    QueryShape(QueryShapeError),
    PartitionCountOverflow,
}

impl fmt::Display for PlanVariantError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::QueryShape(error) => error.fmt(formatter),
            Self::PartitionCountOverflow => {
                formatter.write_str("physical plan partition count exceeds u64")
            }
        }
    }
}

impl Error for PlanVariantError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::QueryShape(error) => Some(error),
            Self::PartitionCountOverflow => None,
        }
    }
}

impl From<QueryShapeError> for PlanVariantError {
    fn from(error: QueryShapeError) -> Self {
        Self::QueryShape(error)
    }
}

impl PlanVariant {
    /// Derives a strategy key directly from a typed physical plan.
    pub fn from_plan(plan: &PhysicalPlan) -> Result<Self, PlanVariantError> {
        PlanVariantBuilder::default().plan(plan)
    }
}

#[derive(Default)]
struct PlanVariantBuilder {
    canonical: QueryShapeCanonicalizer,
}

impl PlanVariantBuilder {
    fn plan(&mut self, plan: &PhysicalPlan) -> Result<PlanVariant, PlanVariantError> {
        Ok(match plan {
            PhysicalPlan::OneRow => PlanVariant::OneRow,
            PhysicalPlan::SeqScan {
                binding_id,
                table_id,
                columns,
                ..
            } => PlanVariant::SeqScan {
                binding: self.canonical.register_binding(*binding_id)?,
                table_id: *table_id,
                columns: self.canonical.columns(columns)?,
            },
            PhysicalPlan::ColumnarScan {
                binding_id,
                table_id,
                columns,
                projection_id,
                source_storage_id,
                ..
            } => PlanVariant::ColumnarScan {
                binding: self.canonical.register_binding(*binding_id)?,
                table_id: *table_id,
                columns: self.canonical.columns(columns)?,
                projection_id: *projection_id,
                source_storage_id: *source_storage_id,
            },
            PhysicalPlan::IndexScan {
                binding_id,
                table_id,
                columns,
                index_column,
                access_path,
                key,
                ..
            } => PlanVariant::IndexPoint {
                binding: self.canonical.register_binding(*binding_id)?,
                table_id: *table_id,
                columns: self.canonical.columns(columns)?,
                index_column: self.canonical.column(index_column)?,
                access_path: *access_path,
                key: scalar_shape(key, &index_column.data_type),
            },
            PhysicalPlan::RangeIndexScan {
                binding_id,
                table_id,
                columns,
                index_column,
                access_path,
                range,
                ..
            } => PlanVariant::IndexRange {
                binding: self.canonical.register_binding(*binding_id)?,
                table_id: *table_id,
                columns: self.canonical.columns(columns)?,
                index_column: self.canonical.column(index_column)?,
                access_path: *access_path,
                range: range_shape(range, &index_column.data_type),
            },
            PhysicalPlan::PartitionedScan {
                binding_id,
                table_id,
                columns,
                partition_key,
                total_partitions,
                partitions,
                ..
            } => {
                let binding = self.canonical.register_binding(*binding_id)?;
                let columns = self.canonical.columns(columns)?;
                let total_partitions = u64::try_from(*total_partitions)
                    .map_err(|_| PlanVariantError::PartitionCountOverflow)?;
                let partitions = partitions
                    .iter()
                    .map(|partition| self.partition(partition))
                    .collect::<Result<_, _>>()?;
                PlanVariant::PartitionedAccess {
                    binding,
                    table_id: *table_id,
                    columns,
                    partition_key: *partition_key,
                    total_partitions,
                    partitions,
                }
            }
            PhysicalPlan::NestedLoopJoin {
                left,
                right,
                kind,
                predicate,
                columns,
            } => {
                let left = self.plan(left)?;
                let right = self.plan(right)?;
                PlanVariant::NestedLoopJoin {
                    left: Box::new(left),
                    right: Box::new(right),
                    kind: *kind,
                    predicate: self.canonical.expression(predicate)?,
                    columns: self.canonical.columns(columns)?,
                }
            }
            PhysicalPlan::IndexNestedLoopJoin {
                left,
                right_binding_id,
                right_table_id,
                right_columns,
                kind,
                left_key,
                right_key,
                right_access_path,
                predicate,
                columns,
                ..
            } => {
                let left = self.plan(left)?;
                let right_binding = self.canonical.register_binding(*right_binding_id)?;
                PlanVariant::IndexNestedLoopJoin {
                    left: Box::new(left),
                    right_binding,
                    right_table_id: *right_table_id,
                    right_columns: self.canonical.columns(right_columns)?,
                    kind: *kind,
                    left_key: self.canonical.column(left_key)?,
                    right_key: self.canonical.column(right_key)?,
                    right_access_path: *right_access_path,
                    predicate: self.canonical.expression(predicate)?,
                    columns: self.canonical.columns(columns)?,
                }
            }
            PhysicalPlan::HashJoin {
                left,
                right,
                kind,
                left_key,
                right_key,
                predicate,
                columns,
            } => {
                let left = self.plan(left)?;
                let right = self.plan(right)?;
                PlanVariant::HashJoin {
                    left: Box::new(left),
                    right: Box::new(right),
                    kind: *kind,
                    left_key: self.canonical.column(left_key)?,
                    right_key: self.canonical.column(right_key)?,
                    predicate: self.canonical.expression(predicate)?,
                    columns: self.canonical.columns(columns)?,
                }
            }
            PhysicalPlan::Filter { input, predicate } => PlanVariant::Filter {
                input: Box::new(self.plan(input)?),
                predicate: self.canonical.expression(predicate)?,
            },
            PhysicalPlan::Sort { input, keys } => PlanVariant::Sort {
                input: Box::new(self.plan(input)?),
                keys: keys
                    .iter()
                    .map(|key| self.canonical.sort_key(key))
                    .collect::<Result<_, _>>()?,
            },
            PhysicalPlan::Project { input, columns } => PlanVariant::Project {
                input: Box::new(self.plan(input)?),
                columns: self.canonical.columns(columns)?,
            },
            PhysicalPlan::ScalarProject { input, expressions } => PlanVariant::ScalarProject {
                input: Box::new(self.plan(input)?),
                expressions: expressions
                    .iter()
                    .map(|expression| self.canonical.projected_expression(expression))
                    .collect::<Result<_, _>>()?,
            },
            PhysicalPlan::Aggregate {
                input,
                group_keys,
                outputs,
            } => PlanVariant::Aggregate {
                input: Box::new(self.plan(input)?),
                group_keys: self.canonical.columns(group_keys)?,
                outputs: outputs
                    .iter()
                    .map(|output| self.canonical.aggregate_output(output))
                    .collect::<Result<_, _>>()?,
            },
            PhysicalPlan::Limit { input, limit } => PlanVariant::Limit {
                input: Box::new(self.plan(input)?),
                limit: *limit,
            },
        })
    }

    fn partition(
        &self,
        partition: &crate::PartitionScanPlan,
    ) -> Result<PlanPartitionVariant, PlanVariantError> {
        let access = match &partition.access {
            PartitionAccessPlan::SeqScan => PlanPartitionAccessVariant::SeqScan,
            PartitionAccessPlan::IndexScan {
                index_column,
                access_path,
                key,
            } => PlanPartitionAccessVariant::IndexPoint {
                index_column: self.canonical.column(index_column)?,
                access_path: *access_path,
                key: scalar_shape(key, &index_column.data_type),
            },
            PartitionAccessPlan::RangeIndexScan {
                index_column,
                access_path,
                range,
            } => PlanPartitionAccessVariant::IndexRange {
                index_column: self.canonical.column(index_column)?,
                access_path: *access_path,
                range: range_shape(range, &index_column.data_type),
            },
        };
        Ok(PlanPartitionVariant {
            partition_id: partition.partition_id,
            storage_id: partition.storage_id,
            access,
        })
    }
}

fn scalar_shape(value: &ScalarValue, data_type: &SemanticType) -> PlanScalarShape {
    PlanScalarShape {
        data_type: data_type.clone(),
        is_null: matches!(value, ScalarValue::Null),
    }
}

fn bound_shape(bound: &IndexBound, data_type: &SemanticType) -> PlanIndexBoundShape {
    match bound {
        IndexBound::Unbounded => PlanIndexBoundShape::Unbounded,
        IndexBound::Included(value) => {
            PlanIndexBoundShape::Included(scalar_shape(value, data_type))
        }
        IndexBound::Excluded(value) => {
            PlanIndexBoundShape::Excluded(scalar_shape(value, data_type))
        }
    }
}

fn range_shape(range: &IndexRange, data_type: &SemanticType) -> PlanIndexRangeShape {
    PlanIndexRangeShape {
        lower: bound_shape(&range.lower, data_type),
        upper: bound_shape(&range.upper, data_type),
    }
}
