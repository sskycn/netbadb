//! Physical planning kept separate from logical relational meaning.

use netbadb_rel::{Assignment, ColumnRef, Expr, JoinKind, LogicalStatement, SortKey};
use netbadb_types::{RelationBindingId, TableId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PhysicalPlan {
    SeqScan {
        binding_id: RelationBindingId,
        table_id: TableId,
        table_name: String,
        columns: Vec<ColumnRef>,
    },
    NestedLoopJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        kind: JoinKind,
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
    pub fn output_columns(&self) -> &[ColumnRef] {
        match self {
            Self::SeqScan { columns, .. }
            | Self::NestedLoopJoin { columns, .. }
            | Self::Project { columns, .. } => columns,
            Self::Filter { input, .. } | Self::Sort { input, .. } | Self::Limit { input, .. } => {
                input.output_columns()
            }
        }
    }
}

#[must_use]
pub fn plan(logical: &netbadb_rel::LogicalPlan) -> PhysicalPlan {
    match logical {
        netbadb_rel::LogicalPlan::Scan {
            binding_id,
            table_id,
            table_name,
            columns,
        } => PhysicalPlan::SeqScan {
            binding_id: *binding_id,
            table_id: *table_id,
            table_name: table_name.clone(),
            columns: columns.clone(),
        },
        netbadb_rel::LogicalPlan::Join {
            left,
            right,
            kind,
            predicate,
            columns,
        } => PhysicalPlan::NestedLoopJoin {
            left: Box::new(plan(left)),
            right: Box::new(plan(right)),
            kind: *kind,
            predicate: predicate.clone(),
            columns: columns.clone(),
        },
        netbadb_rel::LogicalPlan::Filter { input, predicate } => PhysicalPlan::Filter {
            input: Box::new(plan(input)),
            predicate: predicate.clone(),
        },
        netbadb_rel::LogicalPlan::Sort { input, keys } => PhysicalPlan::Sort {
            input: Box::new(plan(input)),
            keys: keys.clone(),
        },
        netbadb_rel::LogicalPlan::Project { input, columns } => PhysicalPlan::Project {
            input: Box::new(plan(input)),
            columns: columns.clone(),
        },
        netbadb_rel::LogicalPlan::Limit { input, limit } => PhysicalPlan::Limit {
            input: Box::new(plan(input)),
            limit: *limit,
        },
    }
}

#[must_use]
pub fn plan_statement(logical: &LogicalStatement) -> PhysicalStatement {
    match logical {
        LogicalStatement::Query(query) => PhysicalStatement::Query(plan(query)),
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
            input: plan(input),
            table_id: *table_id,
            assignments: assignments.clone(),
        },
        LogicalStatement::Delete { input, table_id } => PhysicalStatement::Delete {
            input: plan(input),
            table_id: *table_id,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::{PhysicalPlan, PhysicalStatement, plan, plan_statement};
    use netbadb_rel::ColumnRef;
    use netbadb_rel::{LogicalPlan, LogicalStatement};
    use netbadb_types::{ColumnId, PhysicalType, RelationBindingId, SemanticType, TableId};

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
        assert_eq!(logical.output_columns(), &[column]);
        assert!(matches!(
            plan(&logical),
            PhysicalPlan::Sort { keys, .. }
                if keys[0].direction == netbadb_rel::SortDirection::Desc
        ));
    }
}
