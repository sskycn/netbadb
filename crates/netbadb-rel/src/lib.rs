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
    pub fn output_columns(&self) -> &[ColumnRef] {
        match self {
            Self::Scan { columns, .. }
            | Self::Join { columns, .. }
            | Self::Project { columns, .. } => columns,
            Self::Filter { input, .. } | Self::Sort { input, .. } | Self::Limit { input, .. } => {
                input.output_columns()
            }
        }
    }
}
