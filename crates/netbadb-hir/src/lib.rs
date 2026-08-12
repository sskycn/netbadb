//! Typed High-level IR produced after name resolution and type checking.

use std::error::Error;
use std::fmt;

use netbadb_parser::{BinaryOp as AstBinaryOp, Expr as AstExpr, Ident, Literal, Query, Span};
use netbadb_schema::{Schema, TableDef};
use netbadb_types::{ColumnId, PhysicalType, ScalarValue, SemanticType, TableId};

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
pub struct TypedExpr {
    pub kind: TypedExprKind,
    pub data_type: SemanticType,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TypedExprKind {
    Column(ColumnRef),
    Literal(ScalarValue),
    Binary {
        operator: BinaryOp,
        left: Box<TypedExpr>,
        right: Box<TypedExpr>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypedQuery {
    pub table_id: TableId,
    pub table_name: String,
    pub projection: Vec<ColumnRef>,
    pub selection: Option<TypedExpr>,
    pub limit: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HirError {
    UnknownTable {
        name: String,
        span: Span,
    },
    UnknownColumn {
        table: String,
        name: String,
        span: Span,
    },
    TypeMismatch {
        expected: SemanticType,
        actual: SemanticType,
        span: Span,
    },
    IncompatibleComparison {
        left: SemanticType,
        right: SemanticType,
        span: Span,
    },
    UnsupportedLiteral {
        span: Span,
    },
}

impl fmt::Display for HirError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownTable { name, .. } => write!(formatter, "unknown table `{name}`"),
            Self::UnknownColumn { table, name, .. } => {
                write!(formatter, "unknown column `{table}.{name}`")
            }
            Self::TypeMismatch {
                expected, actual, ..
            } => write!(formatter, "expected {expected}, found {actual}"),
            Self::IncompatibleComparison { left, right, .. } => {
                write!(formatter, "cannot compare {left} with {right}")
            }
            Self::UnsupportedLiteral { .. } => {
                formatter.write_str("NULL is not typed in the initial query subset")
            }
        }
    }
}

impl Error for HirError {}

pub fn lower_query(schema: &Schema, query: &Query) -> Result<TypedQuery, HirError> {
    let table = schema
        .table(&query.from.name)
        .ok_or_else(|| HirError::UnknownTable {
            name: query.from.name.clone(),
            span: query.from.span,
        })?;

    let projection = query
        .projection
        .iter()
        .try_fold(Vec::new(), |mut columns, item| {
            match item {
                netbadb_parser::SelectItem::Wildcard(_) => {
                    columns.extend(
                        table
                            .columns
                            .iter()
                            .map(|column| column_ref(table.id, column)),
                    );
                }
                netbadb_parser::SelectItem::Column(column) => {
                    columns.push(resolve_column(table, column)?);
                }
            }
            Ok::<_, HirError>(columns)
        })?;

    let selection = query
        .selection
        .as_ref()
        .map(|expression| lower_expr(table, expression))
        .transpose()?;
    if let Some(predicate) = &selection {
        let bool_type = SemanticType::physical(PhysicalType::Bool);
        if predicate.data_type != bool_type {
            return Err(HirError::TypeMismatch {
                expected: bool_type,
                actual: predicate.data_type.clone(),
                span: predicate.span,
            });
        }
    }

    Ok(TypedQuery {
        table_id: table.id,
        table_name: table.name.clone(),
        projection,
        selection,
        limit: query.limit,
    })
}

fn lower_expr(table: &TableDef, expression: &AstExpr) -> Result<TypedExpr, HirError> {
    match expression {
        AstExpr::Column(column) => {
            let resolved = resolve_column(table, column)?;
            Ok(TypedExpr {
                data_type: resolved.data_type.clone(),
                kind: TypedExprKind::Column(resolved),
                span: column.span,
            })
        }
        AstExpr::Literal { value, span } => {
            let (value, data_type) = match value {
                Literal::Bool(value) => (
                    ScalarValue::Bool(*value),
                    SemanticType::physical(PhysicalType::Bool),
                ),
                Literal::Int(value) => (
                    ScalarValue::Int64(*value),
                    SemanticType::physical(PhysicalType::Int64),
                ),
                Literal::String(value) => (
                    ScalarValue::Text(value.clone()),
                    SemanticType::physical(PhysicalType::Text),
                ),
                Literal::Null => return Err(HirError::UnsupportedLiteral { span: *span }),
            };
            Ok(TypedExpr {
                kind: TypedExprKind::Literal(value),
                data_type,
                span: *span,
            })
        }
        AstExpr::Binary {
            left,
            operator,
            right,
            span,
        } => {
            let left = lower_expr(table, left)?;
            let right = lower_expr(table, right)?;
            let operator = lower_operator(*operator);
            let bool_type = SemanticType::physical(PhysicalType::Bool);
            match operator {
                BinaryOp::And | BinaryOp::Or => {
                    if left.data_type != bool_type {
                        return Err(HirError::TypeMismatch {
                            expected: bool_type.clone(),
                            actual: left.data_type,
                            span: left.span,
                        });
                    }
                    if right.data_type != bool_type {
                        return Err(HirError::TypeMismatch {
                            expected: bool_type.clone(),
                            actual: right.data_type,
                            span: right.span,
                        });
                    }
                }
                BinaryOp::Eq
                | BinaryOp::NotEq
                | BinaryOp::Lt
                | BinaryOp::LtEq
                | BinaryOp::Gt
                | BinaryOp::GtEq => {
                    if !left.data_type.is_compatible_with(&right.data_type) {
                        return Err(HirError::IncompatibleComparison {
                            left: left.data_type,
                            right: right.data_type,
                            span: *span,
                        });
                    }
                }
            }
            Ok(TypedExpr {
                kind: TypedExprKind::Binary {
                    operator,
                    left: Box::new(left),
                    right: Box::new(right),
                },
                data_type: bool_type,
                span: *span,
            })
        }
    }
}

fn resolve_column(table: &TableDef, column: &Ident) -> Result<ColumnRef, HirError> {
    table
        .column(&column.name)
        .map(|column_def| column_ref(table.id, column_def))
        .ok_or_else(|| HirError::UnknownColumn {
            table: table.name.clone(),
            name: column.name.clone(),
            span: column.span,
        })
}

fn column_ref(table_id: TableId, column: &netbadb_schema::ColumnDef) -> ColumnRef {
    ColumnRef {
        table_id,
        column_id: column.id,
        name: column.name.clone(),
        data_type: column.semantic_type(),
    }
}

fn lower_operator(operator: AstBinaryOp) -> BinaryOp {
    match operator {
        AstBinaryOp::Eq => BinaryOp::Eq,
        AstBinaryOp::NotEq => BinaryOp::NotEq,
        AstBinaryOp::Lt => BinaryOp::Lt,
        AstBinaryOp::LtEq => BinaryOp::LtEq,
        AstBinaryOp::Gt => BinaryOp::Gt,
        AstBinaryOp::GtEq => BinaryOp::GtEq,
        AstBinaryOp::And => BinaryOp::And,
        AstBinaryOp::Or => BinaryOp::Or,
    }
}

#[cfg(test)]
mod tests {
    use super::{HirError, TypedExprKind, lower_query};
    use netbadb_parser::parse;
    use netbadb_schema::{ColumnDef, Schema, TableDef, TypeSpec};
    use netbadb_types::{ColumnId, PhysicalType, TableId};

    fn schema() -> Schema {
        Schema::new(vec![TableDef::new(
            TableId(1),
            "users",
            vec![
                ColumnDef::new(
                    ColumnId(1),
                    "id",
                    TypeSpec::Semantic {
                        name: "UserId".into(),
                        physical: PhysicalType::UInt64,
                    },
                ),
                ColumnDef::new(ColumnId(2), "name", TypeSpec::Physical(PhysicalType::Text)),
                ColumnDef::new(
                    ColumnId(3),
                    "active",
                    TypeSpec::Physical(PhysicalType::Bool),
                ),
            ],
        )])
    }

    #[test]
    fn lowers_and_types_a_query() {
        let query = parse("SELECT id, name FROM users WHERE active = true LIMIT 2").expect("parse");
        let typed = lower_query(&schema(), &query).expect("lower");
        assert_eq!(typed.projection.len(), 2);
        assert!(matches!(
            typed.selection.expect("predicate").kind,
            TypedExprKind::Binary { .. }
        ));
    }

    #[test]
    fn rejects_comparison_of_distinct_nominal_types() {
        let query = parse("SELECT id FROM users WHERE id = 1").expect("parse");
        let error = lower_query(&schema(), &query).expect_err("raw integer cannot be a UserId");
        assert!(matches!(error, HirError::IncompatibleComparison { .. }));
    }
}
