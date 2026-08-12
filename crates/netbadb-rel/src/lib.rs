//! Typed logical relational IR shared by the compiler, planner, and executor.

use netbadb_types::{ColumnId, ScalarValue, SemanticType, TableId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnRef {
    pub table_id: TableId,
    pub column_id: ColumnId,
    pub name: String,
    pub data_type: SemanticType,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expr {
    Column(ColumnRef),
    Literal(ScalarValue),
    Binary {
        operator: BinaryOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogicalPlan {
    Scan {
        table_id: TableId,
        table_name: String,
        columns: Vec<ColumnRef>,
    },
    Filter {
        input: Box<LogicalPlan>,
        predicate: Expr,
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

impl LogicalPlan {
    #[must_use]
    pub fn output_columns(&self) -> &[ColumnRef] {
        match self {
            Self::Scan { columns, .. } | Self::Project { columns, .. } => columns,
            Self::Filter { input, .. } | Self::Limit { input, .. } => input.output_columns(),
        }
    }
}
