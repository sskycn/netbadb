use crate::DatabaseError;
use crate::registry::TablePlacement;
use crate::registry::{PhysicalBindings, StorageRegistry};
use netbadb_index::IndexBound;
use netbadb_inspect::{
    AggregateFunctionInspection, AggregateInputInspection, AggregateOutputInspection,
    AssignmentInspection, BinaryOpInspection, CatalogInspection, ColumnInspection,
    ColumnReferenceInspection, ExpressionInspection, ExpressionKindInspection, IndexInspection,
    IndexKindInspection, IndexRangeInspection, IndexStatisticsInspection, JoinKindInspection,
    NullOrderInspection, PartitionAccessInspection, PartitionScanInspection, PlanNodeInspection,
    RangeBoundInspection, RangePartitionInspection, ResultFieldInspection, SortDirectionInspection,
    SortKeyInspection, SourceColumnInspection, StatementAccessInspection, StatementInspection,
    StatementKind, StatementPlanInspection, StatementResultInspection, TableInspection,
    TablePlacementInspection, TableStatisticsInspection, UnaryOpInspection,
};
use netbadb_planner::{PartitionAccessPlan, PhysicalPlan, PhysicalStatement};
use netbadb_rel::{
    AggregateFunction, AggregateInput, AggregateOutput, Assignment, BinaryOp, ColumnRef, Expr,
    ExprKind, JoinKind, LogicalStatement, NullOrder, OutputField, SortDirection, SortKey, UnaryOp,
};
use netbadb_schema::Schema;

pub(crate) fn catalog(
    schema: &Schema,
    bindings: &PhysicalBindings,
    registry: &StorageRegistry,
) -> Result<CatalogInspection, DatabaseError> {
    let mut tables = Vec::with_capacity(schema.tables().len());
    for table in schema.tables() {
        let placement = bindings.placement(table.id)?;
        let single_storage = match placement {
            TablePlacement::Single { storage_id, .. } => Some(
                registry
                    .get(*storage_id)
                    .ok_or(DatabaseError::InspectionStorageMissing { table_id: table.id })?,
            ),
            TablePlacement::RangePartitioned { .. } => None,
        };
        let mut indexes =
            Vec::with_capacity(single_storage.map_or(0, |storage| storage.indexes().len()));
        for (position, definition) in single_storage
            .into_iter()
            .flat_map(|storage| storage.indexes())
            .enumerate()
        {
            let registration_order = u32::try_from(position).map_err(|_| {
                DatabaseError::InspectionRegistrationOrderOverflow {
                    table_id: table.id,
                    position,
                }
            })?;
            let column = table.column_by_id(definition.column_id).ok_or(
                DatabaseError::InspectionIndexColumnMissing {
                    table_id: table.id,
                    column_id: definition.column_id,
                },
            )?;
            indexes.push(IndexInspection {
                table_id: table.id,
                column_id: definition.column_id,
                column_name: column.name.clone(),
                kind: IndexKindInspection::BTree,
                unique: false,
                registration_order,
                statistics: single_storage
                    .and_then(|storage| storage.index_statistics(definition.column_id))
                    .map(|statistics| IndexStatisticsInspection {
                        distinct_non_null_keys: statistics.distinct_non_null_keys,
                        null_count: statistics.null_count,
                        tree_height: statistics.tree_height,
                    }),
            });
        }
        tables.push(TableInspection {
            table_id: table.id,
            name: table.name.clone(),
            fingerprint: table.fingerprint()?,
            columns: table
                .columns
                .iter()
                .map(|column| ColumnInspection {
                    column_id: column.id,
                    name: column.name.clone(),
                    data_type: column.semantic_type(),
                    nullable: column.nullable,
                    primary_key: column.primary_key,
                })
                .collect(),
            indexes,
            statistics: single_storage
                .and_then(|storage| storage.table_statistics())
                .map(|statistics| TableStatisticsInspection {
                    row_count: statistics.row_count,
                    managed_page_count: statistics.managed_page_count,
                }),
            placement: match placement {
                TablePlacement::Single { .. } => TablePlacementInspection::Single,
                TablePlacement::RangePartitioned {
                    partition_key,
                    partitions,
                    ..
                } => TablePlacementInspection::RangePartitioned {
                    partition_key: *partition_key,
                    partitions: partitions
                        .iter()
                        .map(|partition| RangePartitionInspection {
                            partition_id: partition.partition_id,
                            lower: partition.lower.clone(),
                            upper: partition.upper.clone(),
                        })
                        .collect(),
                },
            },
        });
    }
    Ok(CatalogInspection { tables })
}

pub(crate) fn statement(
    logical: &LogicalStatement,
    physical: &PhysicalStatement,
) -> StatementInspection {
    StatementInspection {
        kind: statement_kind(logical),
        access: StatementAccessInspection {
            read_tables: logical.read_tables(),
            write_tables: logical.write_tables(),
        },
        result: statement_result(physical),
        plan: statement_plan(physical),
    }
}

fn statement_kind(statement: &LogicalStatement) -> StatementKind {
    match statement {
        LogicalStatement::Query(_) => StatementKind::Query,
        LogicalStatement::Insert { .. } => StatementKind::Insert,
        LogicalStatement::Update { .. } => StatementKind::Update,
        LogicalStatement::Delete { .. } => StatementKind::Delete,
    }
}

fn statement_result(statement: &PhysicalStatement) -> StatementResultInspection {
    match statement {
        PhysicalStatement::Query(plan) => StatementResultInspection::Query {
            columns: plan.output_fields().iter().map(result_field).collect(),
        },
        PhysicalStatement::Insert { .. }
        | PhysicalStatement::Update { .. }
        | PhysicalStatement::Delete { .. } => StatementResultInspection::AffectedRows,
    }
}

fn result_field(field: &OutputField) -> ResultFieldInspection {
    ResultFieldInspection {
        name: field.name().to_owned(),
        data_type: field.data_type().clone(),
        nullable: field.nullable(),
        source: field.source_column().map(source_column),
    }
}

fn source_column(column: &ColumnRef) -> SourceColumnInspection {
    SourceColumnInspection {
        binding_id: column.binding_id,
        table_id: column.table_id,
        column_id: column.column_id,
        relation_name: column.relation_name.clone(),
        name: column.name.clone(),
    }
}

fn column_reference(column: &ColumnRef) -> ColumnReferenceInspection {
    ColumnReferenceInspection {
        binding_id: column.binding_id,
        table_id: column.table_id,
        column_id: column.column_id,
        relation_name: column.relation_name.clone(),
        name: column.name.clone(),
        data_type: column.data_type.clone(),
        nullable: column.nullable,
    }
}

fn expression(expression: &Expr) -> ExpressionInspection {
    let kind = match &expression.kind {
        ExprKind::Column(column) => ExpressionKindInspection::Column(column_reference(column)),
        ExprKind::Literal(value) => ExpressionKindInspection::Literal(value.clone()),
        ExprKind::Parameter(id) => ExpressionKindInspection::Parameter(*id),
        ExprKind::Cast { expression } => return self::expression(expression),
        ExprKind::Binary {
            operator,
            left,
            right,
        } => ExpressionKindInspection::Binary {
            operator: binary_operator(*operator),
            left: Box::new(self::expression(left)),
            right: Box::new(self::expression(right)),
        },
        ExprKind::Unary {
            operator,
            expression,
        } => ExpressionKindInspection::Unary {
            operator: unary_operator(*operator),
            expression: Box::new(self::expression(expression)),
        },
        ExprKind::IsNull {
            expression,
            negated,
        } => ExpressionKindInspection::IsNull {
            expression: Box::new(self::expression(expression)),
            negated: *negated,
        },
    };
    ExpressionInspection {
        kind,
        data_type: expression.expr_type.data_type.clone(),
        nullable: expression.expr_type.nullable,
    }
}

fn binary_operator(operator: BinaryOp) -> BinaryOpInspection {
    match operator {
        BinaryOp::Eq => BinaryOpInspection::Eq,
        BinaryOp::NotEq => BinaryOpInspection::NotEq,
        BinaryOp::Lt => BinaryOpInspection::Lt,
        BinaryOp::LtEq => BinaryOpInspection::LtEq,
        BinaryOp::Gt => BinaryOpInspection::Gt,
        BinaryOp::GtEq => BinaryOpInspection::GtEq,
        BinaryOp::And => BinaryOpInspection::And,
        BinaryOp::Or => BinaryOpInspection::Or,
    }
}

fn unary_operator(operator: UnaryOp) -> UnaryOpInspection {
    match operator {
        UnaryOp::Not => UnaryOpInspection::Not,
    }
}

fn inspect_plan(plan: &PhysicalPlan) -> PlanNodeInspection {
    match plan {
        PhysicalPlan::OneRow => PlanNodeInspection::OneRow,
        PhysicalPlan::SeqScan {
            binding_id,
            table_id,
            table_name,
            columns,
        } => PlanNodeInspection::SeqScan {
            binding_id: *binding_id,
            table_id: *table_id,
            table_name: table_name.clone(),
            columns: columns.iter().map(column_reference).collect(),
        },
        PhysicalPlan::IndexScan {
            binding_id,
            table_id,
            table_name,
            columns,
            index_column,
            access_path: _,
            key,
        } => PlanNodeInspection::IndexScan {
            binding_id: *binding_id,
            table_id: *table_id,
            table_name: table_name.clone(),
            columns: columns.iter().map(column_reference).collect(),
            index_column: column_reference(index_column),
            key: key.clone(),
        },
        PhysicalPlan::RangeIndexScan {
            binding_id,
            table_id,
            table_name,
            columns,
            index_column,
            access_path: _,
            range,
        } => PlanNodeInspection::RangeIndexScan {
            binding_id: *binding_id,
            table_id: *table_id,
            table_name: table_name.clone(),
            columns: columns.iter().map(column_reference).collect(),
            index_column: column_reference(index_column),
            range: IndexRangeInspection {
                lower: range_bound(&range.lower),
                upper: range_bound(&range.upper),
            },
        },
        PhysicalPlan::PartitionedScan {
            binding_id,
            table_id,
            table_name,
            columns,
            partition_key,
            total_partitions,
            partitions,
        } => PlanNodeInspection::PartitionedScan {
            binding_id: *binding_id,
            table_id: *table_id,
            table_name: table_name.clone(),
            columns: columns.iter().map(column_reference).collect(),
            partition_key: *partition_key,
            total_partitions: *total_partitions,
            partitions: partitions
                .iter()
                .map(|partition| PartitionScanInspection {
                    partition_id: partition.partition_id,
                    access: match &partition.access {
                        PartitionAccessPlan::SeqScan => PartitionAccessInspection::SeqScan,
                        PartitionAccessPlan::IndexScan { index_column, .. } => {
                            PartitionAccessInspection::IndexScan {
                                column: column_reference(index_column),
                            }
                        }
                        PartitionAccessPlan::RangeIndexScan {
                            index_column,
                            range,
                            ..
                        } => PartitionAccessInspection::RangeIndexScan {
                            column: column_reference(index_column),
                            range: IndexRangeInspection {
                                lower: range_bound(&range.lower),
                                upper: range_bound(&range.upper),
                            },
                        },
                    },
                })
                .collect(),
        },
        PhysicalPlan::NestedLoopJoin {
            left,
            right,
            kind,
            predicate,
            columns: _,
        } => PlanNodeInspection::NestedLoopJoin {
            kind: join_kind(*kind),
            predicate: expression(predicate),
            left: Box::new(inspect_plan(left)),
            right: Box::new(inspect_plan(right)),
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
        } => PlanNodeInspection::IndexNestedLoopJoin {
            kind: join_kind(*kind),
            left_key: column_reference(left_key),
            right_key: column_reference(right_key),
            right_binding_id: *right_binding_id,
            right_table_id: *right_table_id,
            right_table_name: right_table_name.clone(),
            right_access_path: *right_access_path,
            right_columns: right_columns.iter().map(column_reference).collect(),
            columns: columns.iter().map(column_reference).collect(),
            predicate: expression(predicate),
            left: Box::new(inspect_plan(left)),
        },
        PhysicalPlan::HashJoin {
            left,
            right,
            kind,
            left_key,
            right_key,
            predicate,
            columns: _,
        } => PlanNodeInspection::HashJoin {
            kind: join_kind(*kind),
            left_key: column_reference(left_key),
            right_key: column_reference(right_key),
            predicate: expression(predicate),
            left: Box::new(inspect_plan(left)),
            right: Box::new(inspect_plan(right)),
        },
        PhysicalPlan::Filter { input, predicate } => PlanNodeInspection::Filter {
            predicate: expression(predicate),
            input: Box::new(inspect_plan(input)),
        },
        PhysicalPlan::Sort { input, keys } => PlanNodeInspection::Sort {
            keys: keys.iter().map(sort_key).collect(),
            input: Box::new(inspect_plan(input)),
        },
        PhysicalPlan::Project { input, columns } => PlanNodeInspection::Project {
            columns: columns.iter().map(column_reference).collect(),
            input: Box::new(inspect_plan(input)),
        },
        PhysicalPlan::ScalarProject { input, expressions } => PlanNodeInspection::ScalarProject {
            expressions: expressions
                .iter()
                .map(|projected| expression(&projected.expression))
                .collect(),
            input: Box::new(inspect_plan(input)),
        },
        PhysicalPlan::Aggregate {
            input,
            group_keys,
            outputs,
        } => PlanNodeInspection::Aggregate {
            group_keys: group_keys.iter().map(column_reference).collect(),
            outputs: outputs.iter().map(aggregate_output).collect(),
            input: Box::new(inspect_plan(input)),
        },
        PhysicalPlan::Limit { input, limit } => PlanNodeInspection::Limit {
            limit: *limit,
            input: Box::new(inspect_plan(input)),
        },
    }
}

fn range_bound(bound: &IndexBound) -> RangeBoundInspection {
    match bound {
        IndexBound::Unbounded => RangeBoundInspection::Unbounded,
        IndexBound::Included(value) => RangeBoundInspection::Included(value.clone()),
        IndexBound::Excluded(value) => RangeBoundInspection::Excluded(value.clone()),
    }
}

fn join_kind(kind: JoinKind) -> JoinKindInspection {
    match kind {
        JoinKind::Inner => JoinKindInspection::Inner,
    }
}

fn sort_key(key: &SortKey) -> SortKeyInspection {
    SortKeyInspection {
        column: column_reference(&key.column),
        direction: match key.direction {
            SortDirection::Asc => SortDirectionInspection::Asc,
            SortDirection::Desc => SortDirectionInspection::Desc,
        },
        null_order: match key.null_order {
            NullOrder::First => NullOrderInspection::First,
            NullOrder::Last => NullOrderInspection::Last,
        },
    }
}

fn aggregate_output(output: &AggregateOutput) -> AggregateOutputInspection {
    match output {
        AggregateOutput::GroupKey(column) => {
            AggregateOutputInspection::GroupKey(column_reference(column))
        }
        AggregateOutput::Aggregate(aggregate) => AggregateOutputInspection::Aggregate {
            function: aggregate_function(aggregate.function),
            input: match &aggregate.input {
                AggregateInput::All => AggregateInputInspection::All,
                AggregateInput::Column(column) => {
                    AggregateInputInspection::Column(column_reference(column))
                }
            },
            output: ResultFieldInspection {
                name: aggregate.output.name.clone(),
                data_type: aggregate.output.data_type.clone(),
                nullable: aggregate.output.nullable,
                source: None,
            },
        },
    }
}

fn aggregate_function(function: AggregateFunction) -> AggregateFunctionInspection {
    match function {
        AggregateFunction::Count => AggregateFunctionInspection::Count,
        AggregateFunction::Sum => AggregateFunctionInspection::Sum,
        AggregateFunction::Min => AggregateFunctionInspection::Min,
        AggregateFunction::Max => AggregateFunctionInspection::Max,
    }
}

fn assignment(assignment: &Assignment) -> AssignmentInspection {
    AssignmentInspection {
        column: column_reference(&assignment.column),
        value: expression(&assignment.value),
    }
}

fn statement_plan(statement: &PhysicalStatement) -> StatementPlanInspection {
    match statement {
        PhysicalStatement::Query(root) => StatementPlanInspection::Query {
            root: inspect_plan(root),
        },
        PhysicalStatement::Insert {
            table_id,
            table_name,
            values,
        } => StatementPlanInspection::Insert {
            table_id: *table_id,
            table_name: table_name.clone(),
            values: values.iter().map(expression).collect(),
        },
        PhysicalStatement::Update {
            input,
            table_id,
            assignments,
        } => StatementPlanInspection::Update {
            table_id: *table_id,
            input: inspect_plan(input),
            assignments: assignments.iter().map(assignment).collect(),
        },
        PhysicalStatement::Delete { input, table_id } => StatementPlanInspection::Delete {
            table_id: *table_id,
            input: inspect_plan(input),
        },
    }
}
