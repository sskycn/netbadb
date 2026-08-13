//! Query compilation from source text to typed HIR and logical relational IR.

use std::error::Error;
use std::fmt;

use netbadb_hir::{
    AggregateFunction as HirAggregateFunction, ColumnRef as HirColumnRef, HirError,
    NullOrder as HirNullOrder, SortDirection as HirSortDirection, TypedAggregate,
    TypedAggregateInput, TypedExpr, TypedExprKind, TypedProjectionItem, TypedQuery, TypedRelation,
    TypedStatement,
};
use netbadb_parser::{ParseError, parse, parse_statement};
use netbadb_rel::{
    AggregateExpr, AggregateFunction, AggregateInput, AggregateOutput, Assignment, BinaryOp,
    ColumnRef, DerivedField, Expr, ExprKind, JoinKind, LogicalPlan, LogicalStatement, NullOrder,
    SortDirection, SortKey, UnaryOp,
};
use netbadb_schema::Schema;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledQuery {
    pub hir: TypedQuery,
    pub logical_plan: LogicalPlan,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledStatement {
    pub hir: TypedStatement,
    pub logical_statement: LogicalStatement,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompileError {
    Parse(ParseError),
    Hir(HirError),
}

impl fmt::Display for CompileError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Parse(error) => error.fmt(formatter),
            Self::Hir(error) => error.fmt(formatter),
        }
    }
}

impl Error for CompileError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Parse(error) => Some(error),
            Self::Hir(error) => Some(error),
        }
    }
}

impl From<ParseError> for CompileError {
    fn from(error: ParseError) -> Self {
        Self::Parse(error)
    }
}

impl From<HirError> for CompileError {
    fn from(error: HirError) -> Self {
        Self::Hir(error)
    }
}

pub fn compile(schema: &Schema, source: &str) -> Result<CompiledQuery, CompileError> {
    let ast = parse(source)?;
    let hir = netbadb_hir::lower_query(schema, &ast)?;
    let logical_plan = lower_query_plan(&hir);

    Ok(CompiledQuery { hir, logical_plan })
}

pub fn compile_statement(schema: &Schema, source: &str) -> Result<CompiledStatement, CompileError> {
    let ast = parse_statement(source)?;
    let hir = netbadb_hir::lower_statement(schema, &ast)?;
    let logical_statement = lower_statement(&hir);
    Ok(CompiledStatement {
        hir,
        logical_statement,
    })
}

fn lower_statement(statement: &TypedStatement) -> LogicalStatement {
    match statement {
        TypedStatement::Select(query) => LogicalStatement::Query(lower_query_plan(query)),
        TypedStatement::Insert(insert) => LogicalStatement::Insert {
            table_id: insert.table_id,
            table_name: insert.table_name.clone(),
            values: insert.values.iter().map(lower_expr).collect(),
        },
        TypedStatement::Update(update) => LogicalStatement::Update {
            input: scan_and_filter(
                update.table_id,
                &update.table_name,
                &update.columns,
                update.selection.as_ref(),
            ),
            table_id: update.table_id,
            assignments: update
                .assignments
                .iter()
                .map(|assignment| Assignment {
                    column: column_ref_from_hir(&assignment.column),
                    value: lower_expr(&assignment.value),
                })
                .collect(),
        },
        TypedStatement::Delete(delete) => LogicalStatement::Delete {
            input: scan_and_filter(
                delete.table_id,
                &delete.table_name,
                &delete.columns,
                delete.selection.as_ref(),
            ),
            table_id: delete.table_id,
        },
    }
}

fn lower_query_plan(query: &TypedQuery) -> LogicalPlan {
    let mut plan = lower_scan(&query.from);
    let mut visible_columns = query
        .from
        .columns
        .iter()
        .map(column_ref_from_hir)
        .collect::<Vec<_>>();
    for join in &query.joins {
        let right = lower_scan(&join.right);
        visible_columns.extend(join.right.columns.iter().map(column_ref_from_hir));
        plan = LogicalPlan::Join {
            left: Box::new(plan),
            right: Box::new(right),
            kind: JoinKind::Inner,
            predicate: lower_expr(&join.predicate),
            columns: visible_columns.clone(),
        };
    }
    if let Some(predicate) = &query.selection {
        plan = LogicalPlan::Filter {
            input: Box::new(plan),
            predicate: lower_expr(predicate),
        };
    }
    let is_grouping = !query.group_by.is_empty()
        || query
            .projection
            .iter()
            .any(|item| matches!(item, TypedProjectionItem::Aggregate(_)));
    if is_grouping {
        plan = LogicalPlan::Aggregate {
            input: Box::new(plan),
            group_keys: query.group_by.iter().map(column_ref_from_hir).collect(),
            outputs: query
                .projection
                .iter()
                .map(|item| match item {
                    TypedProjectionItem::Column(column) => {
                        AggregateOutput::GroupKey(column_ref_from_hir(column))
                    }
                    TypedProjectionItem::Aggregate(aggregate) => {
                        AggregateOutput::Aggregate(lower_aggregate(aggregate))
                    }
                })
                .collect(),
        };
    } else {
        if !query.order_by.is_empty() {
            plan = LogicalPlan::Sort {
                input: Box::new(plan),
                keys: query
                    .order_by
                    .iter()
                    .map(|key| SortKey {
                        column: column_ref_from_hir(&key.column),
                        direction: match key.direction {
                            HirSortDirection::Asc => SortDirection::Asc,
                            HirSortDirection::Desc => SortDirection::Desc,
                        },
                        null_order: match key.null_order {
                            HirNullOrder::First => NullOrder::First,
                            HirNullOrder::Last => NullOrder::Last,
                        },
                    })
                    .collect(),
            };
        }
        plan = LogicalPlan::Project {
            input: Box::new(plan),
            columns: query
                .projection
                .iter()
                .filter_map(|item| item.source_column().map(column_ref_from_hir))
                .collect(),
        };
    }
    if let Some(limit) = query.limit {
        plan = LogicalPlan::Limit {
            input: Box::new(plan),
            limit,
        };
    }
    plan
}

fn lower_aggregate(aggregate: &TypedAggregate) -> AggregateExpr {
    AggregateExpr {
        function: match aggregate.function {
            HirAggregateFunction::Count => AggregateFunction::Count,
            HirAggregateFunction::Sum => AggregateFunction::Sum,
            HirAggregateFunction::Min => AggregateFunction::Min,
            HirAggregateFunction::Max => AggregateFunction::Max,
        },
        input: match &aggregate.input {
            TypedAggregateInput::All => AggregateInput::All,
            TypedAggregateInput::Column(column) => {
                AggregateInput::Column(column_ref_from_hir(column))
            }
        },
        output: DerivedField {
            name: aggregate.output_name.clone(),
            data_type: aggregate.expr_type.data_type.clone(),
            nullable: aggregate.expr_type.nullable,
        },
    }
}

fn lower_scan(relation: &TypedRelation) -> LogicalPlan {
    LogicalPlan::Scan {
        binding_id: relation.binding_id,
        table_id: relation.table_id,
        table_name: relation.table_name.clone(),
        columns: relation.columns.iter().map(column_ref_from_hir).collect(),
    }
}

fn scan_and_filter(
    table_id: netbadb_types::TableId,
    table_name: &str,
    columns: &[HirColumnRef],
    selection: Option<&TypedExpr>,
) -> LogicalPlan {
    let mut plan = LogicalPlan::Scan {
        binding_id: columns
            .first()
            .map_or(netbadb_types::RelationBindingId(0), |column| {
                column.binding_id
            }),
        table_id,
        table_name: table_name.to_owned(),
        columns: columns.iter().map(column_ref_from_hir).collect(),
    };
    if let Some(predicate) = selection {
        plan = LogicalPlan::Filter {
            input: Box::new(plan),
            predicate: lower_expr(predicate),
        };
    }
    plan
}

fn column_ref_from_hir(column: &HirColumnRef) -> ColumnRef {
    ColumnRef {
        binding_id: column.binding_id,
        table_id: column.table_id,
        column_id: column.column_id,
        relation_name: column.relation_name.clone(),
        name: column.name.clone(),
        data_type: column.data_type.clone(),
        nullable: column.nullable,
    }
}

fn lower_expr(expression: &TypedExpr) -> Expr {
    let kind = match &expression.kind {
        TypedExprKind::Column(column) => ExprKind::Column(column_ref_from_hir(column)),
        TypedExprKind::Literal(value) => ExprKind::Literal(value.clone()),
        TypedExprKind::Binary {
            operator,
            left,
            right,
        } => ExprKind::Binary {
            operator: match operator {
                netbadb_hir::BinaryOp::Eq => BinaryOp::Eq,
                netbadb_hir::BinaryOp::NotEq => BinaryOp::NotEq,
                netbadb_hir::BinaryOp::Lt => BinaryOp::Lt,
                netbadb_hir::BinaryOp::LtEq => BinaryOp::LtEq,
                netbadb_hir::BinaryOp::Gt => BinaryOp::Gt,
                netbadb_hir::BinaryOp::GtEq => BinaryOp::GtEq,
                netbadb_hir::BinaryOp::And => BinaryOp::And,
                netbadb_hir::BinaryOp::Or => BinaryOp::Or,
            },
            left: Box::new(lower_expr(left)),
            right: Box::new(lower_expr(right)),
        },
        TypedExprKind::Unary {
            operator,
            expression,
        } => ExprKind::Unary {
            operator: match operator {
                netbadb_hir::UnaryOp::Not => UnaryOp::Not,
            },
            expression: Box::new(lower_expr(expression)),
        },
        TypedExprKind::IsNull {
            expression,
            negated,
        } => ExprKind::IsNull {
            expression: Box::new(lower_expr(expression)),
            negated: *negated,
        },
    };
    Expr {
        kind,
        expr_type: expression.expr_type.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::{compile, compile_statement};
    use netbadb_rel::{
        AggregateFunction, ExprKind, LogicalPlan, LogicalStatement, NullOrder, OutputField,
        SortDirection,
    };
    use netbadb_schema::{ColumnDef, Schema, TableDef, TypeSpec};
    use netbadb_types::{ColumnId, PhysicalType, RelationBindingId, TableId};

    #[test]
    fn compiles_source_to_a_logical_plan() {
        let schema = Schema::new(vec![TableDef::new(
            TableId(1),
            "users",
            vec![
                ColumnDef::new(ColumnId(1), "id", TypeSpec::Physical(PhysicalType::Int64)),
                ColumnDef::new(ColumnId(2), "name", TypeSpec::Physical(PhysicalType::Text)),
            ],
        )])
        .expect("valid test schema");
        let compiled =
            compile(&schema, "SELECT name FROM users WHERE id >= 2 LIMIT 1").expect("compile");
        assert_eq!(compiled.logical_plan.output_fields().len(), 1);
        assert_eq!(compiled.hir.limit, Some(1));
    }

    #[test]
    fn preserves_null_predicates_in_logical_ir() {
        let schema = Schema::new(vec![TableDef::new(
            TableId(1),
            "users",
            vec![
                ColumnDef::new(ColumnId(1), "id", TypeSpec::Physical(PhysicalType::Int64)),
                ColumnDef::new(
                    ColumnId(2),
                    "nickname",
                    TypeSpec::Physical(PhysicalType::Text),
                )
                .nullable(true),
            ],
        )])
        .expect("valid test schema");
        let compiled =
            compile(&schema, "SELECT id FROM users WHERE nickname IS NOT NULL").expect("compile");
        let LogicalPlan::Project { input, .. } = compiled.logical_plan else {
            panic!("expected project");
        };
        let LogicalPlan::Filter { predicate, .. } = *input else {
            panic!("expected filter");
        };
        assert!(matches!(
            predicate.kind,
            ExprKind::IsNull { negated: true, .. }
        ));
        assert!(!predicate.expr_type.nullable);
    }

    #[test]
    fn places_sort_before_projection_and_limit_for_unprojected_keys() {
        let schema = Schema::new(vec![TableDef::new(
            TableId(1),
            "users",
            vec![
                ColumnDef::new(ColumnId(1), "id", TypeSpec::Physical(PhysicalType::Int64)),
                ColumnDef::new(ColumnId(2), "name", TypeSpec::Physical(PhysicalType::Text)),
            ],
        )])
        .expect("valid test schema");
        let compiled = compile(
            &schema,
            "SELECT name FROM users WHERE id > 0 ORDER BY id DESC LIMIT 2",
        )
        .expect("compile ORDER BY");
        let LogicalPlan::Limit { input, limit: 2 } = compiled.logical_plan else {
            panic!("expected limit");
        };
        let LogicalPlan::Project { input, columns } = *input else {
            panic!("expected project");
        };
        assert_eq!(columns.len(), 1);
        assert_eq!(columns[0].name, "name");
        let LogicalPlan::Sort { input, keys } = *input else {
            panic!("expected sort");
        };
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].column.name, "id");
        assert_eq!(keys[0].direction, SortDirection::Desc);
        assert_eq!(keys[0].null_order, NullOrder::First);
        assert!(matches!(*input, LogicalPlan::Filter { .. }));
    }

    #[test]
    fn places_global_aggregate_after_filter_and_before_limit() {
        let schema = Schema::new(vec![TableDef::new(
            TableId(1),
            "users",
            vec![
                ColumnDef::new(ColumnId(1), "id", TypeSpec::Physical(PhysicalType::Int64)),
                ColumnDef::new(
                    ColumnId(2),
                    "score",
                    TypeSpec::Physical(PhysicalType::Int64),
                )
                .nullable(true),
            ],
        )])
        .expect("valid test schema");
        let compiled = compile(
            &schema,
            "SELECT COUNT(*), SUM(score) FROM users WHERE id > 0 LIMIT 0",
        )
        .expect("compile aggregate");
        assert!(matches!(
            compiled.logical_plan.output_fields().as_slice(),
            [OutputField::Derived(count), OutputField::Derived(sum)]
                if count.name == "COUNT(*)" && !count.nullable
                    && sum.name == "SUM(score)" && sum.nullable
        ));
        let LogicalPlan::Limit { input, limit: 0 } = compiled.logical_plan else {
            panic!("expected limit");
        };
        let LogicalPlan::Aggregate {
            input,
            group_keys,
            outputs,
        } = *input
        else {
            panic!("expected aggregate");
        };
        assert!(group_keys.is_empty());
        assert_eq!(outputs.len(), 2);
        assert!(matches!(
            outputs[0],
            netbadb_rel::AggregateOutput::Aggregate(ref aggregate)
                if aggregate.function == AggregateFunction::Count
        ));
        assert!(matches!(
            outputs[1],
            netbadb_rel::AggregateOutput::Aggregate(ref aggregate)
                if aggregate.function == AggregateFunction::Sum
        ));
        assert!(matches!(*input, LogicalPlan::Filter { .. }));
    }

    #[test]
    fn grouped_plan_separates_keys_from_projection_order() {
        let schema = Schema::new(vec![TableDef::new(
            TableId(1),
            "users",
            vec![
                ColumnDef::new(
                    ColumnId(1),
                    "team_id",
                    TypeSpec::Physical(PhysicalType::UInt64),
                ),
                ColumnDef::new(
                    ColumnId(2),
                    "score",
                    TypeSpec::Physical(PhysicalType::Int64),
                ),
            ],
        )])
        .expect("valid schema");
        let compiled = compile(
            &schema,
            "SELECT COUNT(*), team_id, MAX(score) FROM users GROUP BY team_id",
        )
        .expect("compile grouped query");
        let LogicalPlan::Aggregate {
            group_keys,
            outputs,
            ..
        } = compiled.logical_plan
        else {
            panic!("expected grouped aggregate");
        };
        assert_eq!(group_keys.len(), 1);
        assert!(matches!(
            outputs.as_slice(),
            [
                netbadb_rel::AggregateOutput::Aggregate(_),
                netbadb_rel::AggregateOutput::GroupKey(group_key),
                netbadb_rel::AggregateOutput::Aggregate(_)
            ] if group_key.name == "team_id"
        ));
    }

    #[test]
    fn compiles_typed_dml_statements() {
        let schema = Schema::new(vec![TableDef::new(
            TableId(1),
            "users",
            vec![
                ColumnDef::new(ColumnId(1), "id", TypeSpec::Physical(PhysicalType::Int64)),
                ColumnDef::new(ColumnId(2), "name", TypeSpec::Physical(PhysicalType::Text)),
                ColumnDef::new(
                    ColumnId(3),
                    "nickname",
                    TypeSpec::Physical(PhysicalType::Text),
                )
                .nullable(true),
            ],
        )])
        .expect("valid test schema");
        assert!(matches!(
            compile_statement(&schema, "INSERT INTO users (id, name) VALUES (1, 'Ada')")
                .expect("compile insert")
                .logical_statement,
            LogicalStatement::Insert { .. }
        ));
        assert!(matches!(
            compile_statement(&schema, "UPDATE users SET nickname = name WHERE id = 1")
                .expect("compile update")
                .logical_statement,
            LogicalStatement::Update { .. }
        ));
        assert!(matches!(
            compile_statement(&schema, "DELETE FROM users WHERE nickname IS NULL")
                .expect("compile delete")
                .logical_statement,
            LogicalStatement::Delete { .. }
        ));
    }

    #[test]
    fn compiles_left_associative_join_plans_with_binding_aware_scans() {
        let schema = Schema::new(vec![
            TableDef::new(
                TableId(1),
                "a",
                vec![ColumnDef::new(
                    ColumnId(1),
                    "id",
                    TypeSpec::Physical(PhysicalType::Int64),
                )],
            ),
            TableDef::new(
                TableId(2),
                "b",
                vec![
                    ColumnDef::new(ColumnId(1), "id", TypeSpec::Physical(PhysicalType::Int64)),
                    ColumnDef::new(ColumnId(2), "a_id", TypeSpec::Physical(PhysicalType::Int64)),
                ],
            ),
            TableDef::new(
                TableId(3),
                "c",
                vec![ColumnDef::new(
                    ColumnId(1),
                    "b_id",
                    TypeSpec::Physical(PhysicalType::Int64),
                )],
            ),
        ])
        .expect("valid test schema");
        let compiled = compile(
            &schema,
            "SELECT a.id, c.b_id FROM a JOIN b ON a.id = b.a_id \
             JOIN c ON b.id = c.b_id",
        )
        .expect("compile joins");
        let LogicalPlan::Project { input, .. } = compiled.logical_plan else {
            panic!("expected project");
        };
        let LogicalPlan::Join { left, right, .. } = *input else {
            panic!("expected outer join");
        };
        assert!(matches!(
            *right,
            LogicalPlan::Scan {
                binding_id: RelationBindingId(2),
                ..
            }
        ));
        assert!(matches!(*left, LogicalPlan::Join { .. }));
    }
}
