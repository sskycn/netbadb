//! Query compilation from source text to typed HIR and logical relational IR.

use std::error::Error;
use std::fmt;

use netbadb_hir::{
    ColumnRef as HirColumnRef, HirError, TypedExpr, TypedExprKind, TypedQuery, TypedStatement,
};
use netbadb_parser::{ParseError, parse, parse_statement};
use netbadb_rel::{
    Assignment, BinaryOp, ColumnRef, Expr, ExprKind, LogicalPlan, LogicalStatement, UnaryOp,
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
    let table = schema.table(&hir.table_name).ok_or_else(|| {
        CompileError::Hir(HirError::UnknownTable {
            name: hir.table_name.clone(),
            span: netbadb_parser::Span { start: 0, end: 0 },
        })
    })?;
    let scan_columns = table
        .columns
        .iter()
        .map(|column| column_ref(hir.table_id, column))
        .collect();
    let mut logical_plan = LogicalPlan::Scan {
        table_id: hir.table_id,
        table_name: hir.table_name.clone(),
        columns: scan_columns,
    };

    if let Some(predicate) = &hir.selection {
        logical_plan = LogicalPlan::Filter {
            input: Box::new(logical_plan),
            predicate: lower_expr(predicate),
        };
    }
    logical_plan = LogicalPlan::Project {
        input: Box::new(logical_plan),
        columns: hir.projection.iter().map(column_ref_from_hir).collect(),
    };
    if let Some(limit) = hir.limit {
        logical_plan = LogicalPlan::Limit {
            input: Box::new(logical_plan),
            limit,
        };
    }

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
    let mut plan = LogicalPlan::Scan {
        table_id: query.table_id,
        table_name: query.table_name.clone(),
        columns: query.columns.iter().map(column_ref_from_hir).collect(),
    };
    if let Some(predicate) = &query.selection {
        plan = LogicalPlan::Filter {
            input: Box::new(plan),
            predicate: lower_expr(predicate),
        };
    }
    plan = LogicalPlan::Project {
        input: Box::new(plan),
        columns: query.projection.iter().map(column_ref_from_hir).collect(),
    };
    if let Some(limit) = query.limit {
        plan = LogicalPlan::Limit {
            input: Box::new(plan),
            limit,
        };
    }
    plan
}

fn scan_and_filter(
    table_id: netbadb_types::TableId,
    table_name: &str,
    columns: &[HirColumnRef],
    selection: Option<&TypedExpr>,
) -> LogicalPlan {
    let mut plan = LogicalPlan::Scan {
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

fn column_ref(table_id: netbadb_types::TableId, column: &netbadb_schema::ColumnDef) -> ColumnRef {
    ColumnRef {
        table_id,
        column_id: column.id,
        name: column.name.clone(),
        data_type: column.semantic_type(),
        nullable: column.nullable,
    }
}

fn column_ref_from_hir(column: &HirColumnRef) -> ColumnRef {
    ColumnRef {
        table_id: column.table_id,
        column_id: column.column_id,
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
    use netbadb_rel::{ExprKind, LogicalPlan, LogicalStatement};
    use netbadb_schema::{ColumnDef, Schema, TableDef, TypeSpec};
    use netbadb_types::{ColumnId, PhysicalType, TableId};

    #[test]
    fn compiles_source_to_a_logical_plan() {
        let schema = Schema::new(vec![TableDef::new(
            TableId(1),
            "users",
            vec![
                ColumnDef::new(ColumnId(1), "id", TypeSpec::Physical(PhysicalType::Int64)),
                ColumnDef::new(ColumnId(2), "name", TypeSpec::Physical(PhysicalType::Text)),
            ],
        )]);
        let compiled =
            compile(&schema, "SELECT name FROM users WHERE id >= 2 LIMIT 1").expect("compile");
        assert_eq!(compiled.logical_plan.output_columns().len(), 1);
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
        )]);
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
        )]);
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
}
