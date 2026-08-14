//! Physical planning kept separate from logical relational meaning.

use netbadb_index::{BTreeHandle, IndexStatistics, TableStatistics};
use netbadb_rel::{
    AggregateOutput, Assignment, BinaryOp, ColumnRef, Expr, ExprKind, JoinKind, LogicalPlan,
    LogicalStatement, OutputField, SortKey,
};
use netbadb_types::{ColumnId, RelationBindingId, ScalarValue, TableId};

/// One registered point-lookup capability available to physical planning.
///
/// Callers preserve registration order in the slice. The planner receives
/// immutable domain snapshots, never storage objects or catalog pages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexAccessPath {
    pub table_id: TableId,
    pub column_id: ColumnId,
    pub handle: BTreeHandle,
    pub statistics: Option<IndexStatistics>,
}

/// Optional optimizer snapshot for one table visible to physical planning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableAccessStatistics {
    pub table_id: TableId,
    pub statistics: Option<TableStatistics>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PhysicalPlan {
    SeqScan {
        binding_id: RelationBindingId,
        table_id: TableId,
        table_name: String,
        columns: Vec<ColumnRef>,
    },
    IndexScan {
        binding_id: RelationBindingId,
        table_id: TableId,
        table_name: String,
        columns: Vec<ColumnRef>,
        index_column: ColumnRef,
        handle: BTreeHandle,
        key: ScalarValue,
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
            Self::SeqScan { columns, .. }
            | Self::IndexScan { columns, .. }
            | Self::NestedLoopJoin { columns, .. }
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

#[must_use]
pub fn plan(logical: &netbadb_rel::LogicalPlan) -> PhysicalPlan {
    plan_with_access_paths(logical, &[])
}

/// Selects physical operators from logical meaning and an ordered snapshot of
/// registered point-lookup access paths.
#[must_use]
pub fn plan_with_access_paths(
    logical: &LogicalPlan,
    access_paths: &[IndexAccessPath],
) -> PhysicalPlan {
    plan_with_statistics(logical, &[], access_paths)
}

/// Selects physical operators using ordered access paths and optional explicit
/// `ANALYZE` snapshots. Missing table statistics preserve the Phase 4E rule.
#[must_use]
pub fn plan_with_statistics(
    logical: &LogicalPlan,
    table_statistics: &[TableAccessStatistics],
    access_paths: &[IndexAccessPath],
) -> PhysicalPlan {
    match logical {
        LogicalPlan::Scan {
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
        LogicalPlan::Join {
            left,
            right,
            kind,
            predicate,
            columns,
        } => PhysicalPlan::NestedLoopJoin {
            left: Box::new(plan_with_statistics(left, table_statistics, access_paths)),
            right: Box::new(plan_with_statistics(right, table_statistics, access_paths)),
            kind: *kind,
            predicate: predicate.clone(),
            columns: columns.clone(),
        },
        LogicalPlan::Filter { input, predicate } => {
            let input = match input.as_ref() {
                LogicalPlan::Scan {
                    binding_id,
                    table_id,
                    table_name,
                    columns,
                } => choose_point_index(
                    predicate,
                    *binding_id,
                    *table_id,
                    table_name,
                    columns,
                    table_statistics,
                    access_paths,
                )
                .unwrap_or_else(|| plan_with_statistics(input, table_statistics, access_paths)),
                _ => plan_with_statistics(input, table_statistics, access_paths),
            };
            PhysicalPlan::Filter {
                input: Box::new(input),
                predicate: predicate.clone(),
            }
        }
        LogicalPlan::Sort { input, keys } => PhysicalPlan::Sort {
            input: Box::new(plan_with_statistics(input, table_statistics, access_paths)),
            keys: keys.clone(),
        },
        LogicalPlan::Project { input, columns } => PhysicalPlan::Project {
            input: Box::new(plan_with_statistics(input, table_statistics, access_paths)),
            columns: columns.clone(),
        },
        LogicalPlan::Aggregate {
            input,
            group_keys,
            outputs,
        } => PhysicalPlan::Aggregate {
            input: Box::new(plan_with_statistics(input, table_statistics, access_paths)),
            group_keys: group_keys.clone(),
            outputs: outputs.clone(),
        },
        LogicalPlan::Limit { input, limit } => PhysicalPlan::Limit {
            input: Box::new(plan_with_statistics(input, table_statistics, access_paths)),
            limit: *limit,
        },
    }
}

fn choose_point_index(
    predicate: &Expr,
    binding_id: RelationBindingId,
    table_id: TableId,
    table_name: &str,
    columns: &[ColumnRef],
    table_statistics: &[TableAccessStatistics],
    access_paths: &[IndexAccessPath],
) -> Option<PhysicalPlan> {
    let eligible = access_paths
        .iter()
        .filter_map(|access_path| {
            if access_path.table_id != table_id {
                return None;
            }
            let (index_column, key) =
                find_point_constraint(predicate, binding_id, table_id, access_path.column_id)?;
            Some((access_path, index_column, key))
        })
        .collect::<Vec<_>>();
    let first = eligible.first()?;
    let analyzed_table = table_statistics
        .iter()
        .find(|entry| entry.table_id == table_id)
        .and_then(|entry| entry.statistics.as_ref());
    let selected = match analyzed_table {
        None => first,
        Some(table) => {
            let mut best: Option<(&(&IndexAccessPath, ColumnRef, ScalarValue), u128)> = None;
            for candidate in &eligible {
                let Some(index) = candidate.0.statistics.as_ref() else {
                    continue;
                };
                let Some(cost) = index_scan_cost(table, index, &candidate.2) else {
                    continue;
                };
                if best.is_none_or(|(_, best_cost)| cost < best_cost) {
                    best = Some((candidate, cost));
                }
            }
            let Some((candidate, cost)) = best else {
                return build_index_scan(first, binding_id, table_id, table_name, columns);
            };
            if cost >= seq_scan_cost(table) {
                return None;
            }
            candidate
        }
    };
    build_index_scan(selected, binding_id, table_id, table_name, columns)
}

fn build_index_scan(
    candidate: &(&IndexAccessPath, ColumnRef, ScalarValue),
    binding_id: RelationBindingId,
    table_id: TableId,
    table_name: &str,
    columns: &[ColumnRef],
) -> Option<PhysicalPlan> {
    Some(PhysicalPlan::IndexScan {
        binding_id,
        table_id,
        table_name: table_name.to_owned(),
        columns: columns.to_vec(),
        index_column: candidate.1.clone(),
        handle: candidate.0.handle,
        key: candidate.2.clone(),
    })
}

fn seq_scan_cost(statistics: &TableStatistics) -> u128 {
    u128::from(statistics.managed_page_count)
}

fn index_scan_cost(
    table: &TableStatistics,
    index: &IndexStatistics,
    key: &ScalarValue,
) -> Option<u128> {
    let estimated_matches = estimate_point_rows(table, index, key)?;
    Some(1 + u128::from(index.tree_height) + estimated_matches)
}

fn estimate_point_rows(
    table: &TableStatistics,
    index: &IndexStatistics,
    key: &ScalarValue,
) -> Option<u128> {
    if matches!(key, ScalarValue::Null) {
        return Some(u128::from(index.null_count));
    }
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
    access_paths: &[IndexAccessPath],
) -> PhysicalStatement {
    plan_statement_with_statistics(logical, &[], access_paths)
}

/// Plans one statement with the same access-path statistics context used for
/// queries, UPDATE, and DELETE.
#[must_use]
pub fn plan_statement_with_statistics(
    logical: &LogicalStatement,
    table_statistics: &[TableAccessStatistics],
    access_paths: &[IndexAccessPath],
) -> PhysicalStatement {
    match logical {
        LogicalStatement::Query(query) => {
            PhysicalStatement::Query(plan_with_statistics(query, table_statistics, access_paths))
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
            input: plan_with_statistics(input, table_statistics, access_paths),
            table_id: *table_id,
            assignments: assignments.clone(),
        },
        LogicalStatement::Delete { input, table_id } => PhysicalStatement::Delete {
            input: plan_with_statistics(input, table_statistics, access_paths),
            table_id: *table_id,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::{
        IndexAccessPath, PhysicalPlan, PhysicalStatement, TableAccessStatistics, plan,
        plan_statement, plan_statement_with_access_paths, plan_statement_with_statistics,
        plan_with_access_paths, plan_with_statistics,
    };
    use netbadb_index::{BTreeHandle, IndexStatistics, TableStatistics};
    use netbadb_rel::{BinaryOp, ColumnRef, Expr, ExprKind, LogicalPlan, LogicalStatement};
    use netbadb_types::{
        ColumnId, ExprType, PageId, PhysicalType, RelationBindingId, ScalarValue, SemanticType,
        TableId,
    };

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
        ColumnRef {
            binding_id: RelationBindingId(7),
            table_id: TableId(1),
            column_id: ColumnId(column_id),
            relation_name: "u".into(),
            name: name.into(),
            data_type: SemanticType::physical(PhysicalType::Int64),
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

    fn access_path(column_id: u32, page_id: u64) -> IndexAccessPath {
        IndexAccessPath {
            table_id: TableId(1),
            column_id: ColumnId(column_id),
            handle: BTreeHandle {
                meta_page: PageId(page_id),
            },
            statistics: None,
        }
    }

    fn analyzed_path(
        column_id: u32,
        page_id: u64,
        distinct_non_null_keys: u64,
        null_count: u64,
        tree_height: u32,
    ) -> IndexAccessPath {
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
                    handle,
                    key: ScalarValue::Int64(42),
                    index_column,
                    ..
                }) if *handle == path.handle && index_column.relation_name == "u"
            ));
            assert_eq!(
                physical.output_fields(),
                vec![netbadb_rel::OutputField::Source(id.clone())]
            );
        }
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
                handle: BTreeHandle {
                    meta_page: PageId(50)
                },
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
                handle: BTreeHandle {
                    meta_page: PageId(60)
                },
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
                handle: BTreeHandle {
                    meta_page: PageId(50)
                },
                ..
            })
        ));

        let unknown_first = access_path(2, 50);
        let known_second = analyzed_path(3, 60, 1_000, 0, 1);
        let partial = plan_with_statistics(&logical, &table, &[unknown_first, known_second]);
        assert!(matches!(
            index_scan_input(&partial),
            Some(PhysicalPlan::IndexScan {
                handle: BTreeHandle {
                    meta_page: PageId(60)
                },
                ..
            })
        ));

        let only_unknown = plan_with_statistics(&logical, &table, &[access_path(2, 50)]);
        assert!(matches!(
            index_scan_input(&only_unknown),
            Some(PhysicalPlan::IndexScan {
                handle: BTreeHandle {
                    meta_page: PageId(50)
                },
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
}
