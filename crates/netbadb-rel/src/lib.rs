//! Typed logical relational IR shared by the compiler, planner, and executor.

use netbadb_types::{ColumnId, ExprType, RelationBindingId, ScalarValue, SemanticType, TableId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnRef {
    pub binding_id: RelationBindingId,
    pub table_id: TableId,
    pub column_id: ColumnId,
    pub relation_name: String,
    pub name: String,
    pub data_type: SemanticType,
    pub nullable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Metadata for a computed result field that has no catalog column identity.
pub struct DerivedField {
    pub name: String,
    pub data_type: SemanticType,
    pub nullable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// A plan output is either a resolvable source column or a derived value.
pub enum OutputField {
    Source(ColumnRef),
    Derived(DerivedField),
}

impl OutputField {
    #[must_use]
    pub fn name(&self) -> &str {
        match self {
            Self::Source(column) => &column.name,
            Self::Derived(field) => &field.name,
        }
    }

    #[must_use]
    pub fn data_type(&self) -> &SemanticType {
        match self {
            Self::Source(column) => &column.data_type,
            Self::Derived(field) => &field.data_type,
        }
    }

    #[must_use]
    pub const fn nullable(&self) -> bool {
        match self {
            Self::Source(column) => column.nullable,
            Self::Derived(field) => field.nullable,
        }
    }

    #[must_use]
    pub const fn source_column(&self) -> Option<&ColumnRef> {
        match self {
            Self::Source(column) => Some(column),
            Self::Derived(_) => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NullOrder {
    First,
    Last,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortKey {
    pub column: ColumnRef,
    pub direction: SortDirection,
    pub null_order: NullOrder,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFunction {
    Count,
    Sum,
    Min,
    Max,
}

impl AggregateFunction {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Count => "COUNT",
            Self::Sum => "SUM",
            Self::Min => "MIN",
            Self::Max => "MAX",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AggregateInput {
    All,
    Column(ColumnRef),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregateExpr {
    pub function: AggregateFunction,
    pub input: AggregateInput,
    pub output: DerivedField,
}

/// One projected field produced by an aggregate operator. Group identity is
/// defined separately by the operator's `group_keys`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AggregateOutput {
    GroupKey(ColumnRef),
    Aggregate(AggregateExpr),
}

impl AggregateOutput {
    #[must_use]
    pub fn output_field(&self) -> OutputField {
        match self {
            Self::GroupKey(column) => OutputField::Source(column.clone()),
            Self::Aggregate(aggregate) => OutputField::Derived(aggregate.output.clone()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    And,
    Or,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Not,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Expr {
    pub kind: ExprKind,
    pub expr_type: ExprType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExprKind {
    Column(ColumnRef),
    Literal(ScalarValue),
    Binary {
        operator: BinaryOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    Unary {
        operator: UnaryOp,
        expression: Box<Expr>,
    },
    IsNull {
        expression: Box<Expr>,
        negated: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogicalPlan {
    Scan {
        binding_id: RelationBindingId,
        table_id: TableId,
        table_name: String,
        columns: Vec<ColumnRef>,
    },
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        kind: JoinKind,
        predicate: Expr,
        columns: Vec<ColumnRef>,
    },
    Filter {
        input: Box<LogicalPlan>,
        predicate: Expr,
    },
    Sort {
        input: Box<LogicalPlan>,
        keys: Vec<SortKey>,
    },
    Project {
        input: Box<LogicalPlan>,
        columns: Vec<ColumnRef>,
    },
    Aggregate {
        input: Box<LogicalPlan>,
        group_keys: Vec<ColumnRef>,
        outputs: Vec<AggregateOutput>,
    },
    Limit {
        input: Box<LogicalPlan>,
        limit: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Assignment {
    pub column: ColumnRef,
    pub value: Expr,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogicalStatement {
    Query(LogicalPlan),
    Insert {
        table_id: TableId,
        table_name: String,
        values: Vec<Expr>,
    },
    Update {
        input: LogicalPlan,
        table_id: TableId,
        assignments: Vec<Assignment>,
    },
    Delete {
        input: LogicalPlan,
        table_id: TableId,
    },
}

impl LogicalPlan {
    #[must_use]
    pub fn output_fields(&self) -> Vec<OutputField> {
        match self {
            Self::Scan { columns, .. }
            | Self::Join { columns, .. }
            | Self::Project { columns, .. } => {
                columns.iter().cloned().map(OutputField::Source).collect()
            }
            Self::Aggregate { outputs, .. } => {
                outputs.iter().map(AggregateOutput::output_field).collect()
            }
            Self::Filter { input, .. } | Self::Sort { input, .. } | Self::Limit { input, .. } => {
                input.output_fields()
            }
        }
    }
}
