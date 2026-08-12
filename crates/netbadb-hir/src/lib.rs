//! Typed High-level IR produced after name resolution and type checking.

use std::collections::HashSet;
use std::error::Error;
use std::fmt;

use netbadb_parser::{
    BinaryOp as AstBinaryOp, Expr as AstExpr, Ident, Literal, Query, Span,
    Statement as AstStatement, UnaryOp as AstUnaryOp,
};
use netbadb_schema::{Schema, TableDef};
use netbadb_types::{ColumnId, ExprType, PhysicalType, ScalarValue, SemanticType, TableId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnRef {
    pub table_id: TableId,
    pub column_id: ColumnId,
    pub name: String,
    pub data_type: SemanticType,
    pub nullable: bool,
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
pub struct TypedExpr {
    pub kind: TypedExprKind,
    pub expr_type: ExprType,
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
    Unary {
        operator: UnaryOp,
        expression: Box<TypedExpr>,
    },
    IsNull {
        expression: Box<TypedExpr>,
        negated: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypedQuery {
    pub table_id: TableId,
    pub table_name: String,
    pub columns: Vec<ColumnRef>,
    pub projection: Vec<ColumnRef>,
    pub selection: Option<TypedExpr>,
    pub limit: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TypedStatement {
    Select(TypedQuery),
    Insert(TypedInsert),
    Update(TypedUpdate),
    Delete(TypedDelete),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypedInsert {
    pub table_id: TableId,
    pub table_name: String,
    /// Values are ordered by the canonical table column order.
    pub values: Vec<TypedExpr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypedAssignment {
    pub column: ColumnRef,
    pub value: TypedExpr,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypedUpdate {
    pub table_id: TableId,
    pub table_name: String,
    pub columns: Vec<ColumnRef>,
    pub assignments: Vec<TypedAssignment>,
    pub selection: Option<TypedExpr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypedDelete {
    pub table_id: TableId,
    pub table_name: String,
    pub columns: Vec<ColumnRef>,
    pub selection: Option<TypedExpr>,
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
    CannotInferNullType {
        span: Span,
    },
    DuplicateColumn {
        name: String,
        span: Span,
    },
    ValueCountMismatch {
        columns: usize,
        values: usize,
        span: Span,
    },
    MissingRequiredColumn {
        name: String,
        span: Span,
    },
    NullNotAllowed {
        name: String,
        span: Span,
    },
    InsertValueReferencesColumn {
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
            Self::CannotInferNullType { .. } => {
                formatter.write_str("cannot infer the type of NULL in this expression")
            }
            Self::DuplicateColumn { name, .. } => {
                write!(formatter, "column `{name}` is specified more than once")
            }
            Self::ValueCountMismatch {
                columns, values, ..
            } => write!(
                formatter,
                "INSERT specifies {columns} columns but provides {values} values"
            ),
            Self::MissingRequiredColumn { name, .. } => {
                write!(formatter, "required column `{name}` is missing")
            }
            Self::NullNotAllowed { name, .. } => {
                write!(formatter, "column `{name}` is not nullable")
            }
            Self::InsertValueReferencesColumn { .. } => {
                formatter.write_str("INSERT VALUES expressions cannot reference table columns")
            }
        }
    }
}

impl Error for HirError {}

pub fn lower_statement(
    schema: &Schema,
    statement: &AstStatement,
) -> Result<TypedStatement, HirError> {
    match statement {
        AstStatement::Select(query) => lower_query(schema, query).map(TypedStatement::Select),
        AstStatement::Insert(insert) => lower_insert(schema, insert).map(TypedStatement::Insert),
        AstStatement::Update(update) => lower_update(schema, update).map(TypedStatement::Update),
        AstStatement::Delete(delete) => lower_delete(schema, delete).map(TypedStatement::Delete),
    }
}

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

    let bool_type = SemanticType::physical(PhysicalType::Bool);
    let selection = query
        .selection
        .as_ref()
        .map(|expression| lower_expr(table, expression, Some(&bool_type)))
        .transpose()?;
    if let Some(predicate) = &selection {
        if predicate.expr_type.data_type != bool_type {
            return Err(HirError::TypeMismatch {
                expected: bool_type,
                actual: predicate.expr_type.data_type.clone(),
                span: predicate.span,
            });
        }
    }

    Ok(TypedQuery {
        table_id: table.id,
        table_name: table.name.clone(),
        columns: table
            .columns
            .iter()
            .map(|column| column_ref(table.id, column))
            .collect(),
        projection,
        selection,
        limit: query.limit,
    })
}

fn lower_insert(
    schema: &Schema,
    insert: &netbadb_parser::InsertStatement,
) -> Result<TypedInsert, HirError> {
    let table = resolve_table(schema, &insert.table)?;
    if insert.columns.len() != insert.values.len() {
        return Err(HirError::ValueCountMismatch {
            columns: insert.columns.len(),
            values: insert.values.len(),
            span: insert.span,
        });
    }
    let mut seen = HashSet::new();
    let mut values = vec![None; table.columns.len()];
    for (target, value) in insert.columns.iter().zip(&insert.values) {
        let column = resolve_column(table, target)?;
        if !seen.insert(column.column_id) {
            return Err(HirError::DuplicateColumn {
                name: target.name.clone(),
                span: target.span,
            });
        }
        if expression_references_column(value) {
            return Err(HirError::InsertValueReferencesColumn {
                span: ast_expr_span(value),
            });
        }
        let position = table
            .columns
            .iter()
            .position(|candidate| candidate.id == column.column_id)
            .ok_or_else(|| HirError::UnknownColumn {
                table: table.name.clone(),
                name: target.name.clone(),
                span: target.span,
            })?;
        values[position] = Some(lower_value_for_column(table, value, &column)?);
    }

    let values = table
        .columns
        .iter()
        .zip(values)
        .map(|(column, value)| match value {
            Some(value) => Ok(value),
            None if column.nullable => Ok(TypedExpr {
                kind: TypedExprKind::Literal(ScalarValue::Null),
                expr_type: ExprType {
                    data_type: column.semantic_type(),
                    nullable: true,
                },
                span: insert.table.span,
            }),
            None => Err(HirError::MissingRequiredColumn {
                name: column.name.clone(),
                span: insert.span,
            }),
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(TypedInsert {
        table_id: table.id,
        table_name: table.name.clone(),
        values,
    })
}

fn lower_update(
    schema: &Schema,
    update: &netbadb_parser::UpdateStatement,
) -> Result<TypedUpdate, HirError> {
    let table = resolve_table(schema, &update.table)?;
    let mut seen = HashSet::new();
    let assignments = update
        .assignments
        .iter()
        .map(|assignment| {
            let column = resolve_column(table, &assignment.column)?;
            if !seen.insert(column.column_id) {
                return Err(HirError::DuplicateColumn {
                    name: assignment.column.name.clone(),
                    span: assignment.column.span,
                });
            }
            let value = lower_value_for_column(table, &assignment.value, &column)?;
            Ok(TypedAssignment { column, value })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let selection = lower_selection(table, update.selection.as_ref())?;
    Ok(TypedUpdate {
        table_id: table.id,
        table_name: table.name.clone(),
        columns: table
            .columns
            .iter()
            .map(|column| column_ref(table.id, column))
            .collect(),
        assignments,
        selection,
    })
}

fn lower_delete(
    schema: &Schema,
    delete: &netbadb_parser::DeleteStatement,
) -> Result<TypedDelete, HirError> {
    let table = resolve_table(schema, &delete.table)?;
    Ok(TypedDelete {
        table_id: table.id,
        table_name: table.name.clone(),
        columns: table
            .columns
            .iter()
            .map(|column| column_ref(table.id, column))
            .collect(),
        selection: lower_selection(table, delete.selection.as_ref())?,
    })
}

fn resolve_table<'a>(schema: &'a Schema, table: &Ident) -> Result<&'a TableDef, HirError> {
    schema
        .table(&table.name)
        .ok_or_else(|| HirError::UnknownTable {
            name: table.name.clone(),
            span: table.span,
        })
}

fn lower_selection(
    table: &TableDef,
    selection: Option<&AstExpr>,
) -> Result<Option<TypedExpr>, HirError> {
    let bool_type = SemanticType::physical(PhysicalType::Bool);
    let selection = selection
        .map(|expression| lower_expr(table, expression, Some(&bool_type)))
        .transpose()?;
    if let Some(predicate) = &selection {
        require_type(predicate, &bool_type)?;
    }
    Ok(selection)
}

fn lower_value_for_column(
    table: &TableDef,
    expression: &AstExpr,
    column: &ColumnRef,
) -> Result<TypedExpr, HirError> {
    let mut value = lower_expr(table, expression, Some(&column.data_type))?;
    if matches!(&value.kind, TypedExprKind::Literal(_))
        && value.expr_type.data_type.physical == column.data_type.physical
    {
        value.expr_type.data_type = column.data_type.clone();
    }
    if !value
        .expr_type
        .data_type
        .is_compatible_with(&column.data_type)
    {
        return Err(HirError::TypeMismatch {
            expected: column.data_type.clone(),
            actual: value.expr_type.data_type,
            span: value.span,
        });
    }
    if !column.nullable && value.expr_type.nullable {
        return Err(HirError::NullNotAllowed {
            name: column.name.clone(),
            span: value.span,
        });
    }
    Ok(value)
}

fn expression_references_column(expression: &AstExpr) -> bool {
    match expression {
        AstExpr::Column(_) => true,
        AstExpr::Literal { .. } => false,
        AstExpr::Binary { left, right, .. } => {
            expression_references_column(left) || expression_references_column(right)
        }
        AstExpr::Unary { expression, .. } | AstExpr::IsNull { expression, .. } => {
            expression_references_column(expression)
        }
    }
}

fn ast_expr_span(expression: &AstExpr) -> Span {
    match expression {
        AstExpr::Column(column) => column.span,
        AstExpr::Literal { span, .. }
        | AstExpr::Binary { span, .. }
        | AstExpr::Unary { span, .. }
        | AstExpr::IsNull { span, .. } => *span,
    }
}

fn lower_expr(
    table: &TableDef,
    expression: &AstExpr,
    expected: Option<&SemanticType>,
) -> Result<TypedExpr, HirError> {
    match expression {
        AstExpr::Column(column) => {
            let resolved = resolve_column(table, column)?;
            Ok(TypedExpr {
                expr_type: ExprType {
                    data_type: resolved.data_type.clone(),
                    nullable: resolved.nullable,
                },
                kind: TypedExprKind::Column(resolved),
                span: column.span,
            })
        }
        AstExpr::Literal { value, span } => {
            let (value, data_type, nullable) = match value {
                Literal::Bool(value) => (
                    ScalarValue::Bool(*value),
                    SemanticType::physical(PhysicalType::Bool),
                    false,
                ),
                Literal::Int(value) => (
                    ScalarValue::Int64(*value),
                    SemanticType::physical(PhysicalType::Int64),
                    false,
                ),
                Literal::String(value) => (
                    ScalarValue::Text(value.clone()),
                    SemanticType::physical(PhysicalType::Text),
                    false,
                ),
                Literal::Null => (
                    ScalarValue::Null,
                    expected
                        .cloned()
                        .ok_or(HirError::CannotInferNullType { span: *span })?,
                    true,
                ),
            };
            Ok(TypedExpr {
                kind: TypedExprKind::Literal(value),
                expr_type: ExprType {
                    data_type,
                    nullable,
                },
                span: *span,
            })
        }
        AstExpr::Binary {
            left,
            operator,
            right,
            span,
        } => {
            let operator = lower_operator(*operator);
            let bool_type = SemanticType::physical(PhysicalType::Bool);
            let (left, right) = match operator {
                BinaryOp::And | BinaryOp::Or => {
                    let left = lower_expr(table, left, Some(&bool_type))?;
                    let right = lower_expr(table, right, Some(&bool_type))?;
                    require_type(&left, &bool_type)?;
                    require_type(&right, &bool_type)?;
                    (left, right)
                }
                BinaryOp::Eq
                | BinaryOp::NotEq
                | BinaryOp::Lt
                | BinaryOp::LtEq
                | BinaryOp::Gt
                | BinaryOp::GtEq => {
                    let (left, right) = lower_comparison_operands(table, left, right)?;
                    if !left
                        .expr_type
                        .data_type
                        .is_compatible_with(&right.expr_type.data_type)
                    {
                        return Err(HirError::IncompatibleComparison {
                            left: left.expr_type.data_type,
                            right: right.expr_type.data_type,
                            span: *span,
                        });
                    }
                    (left, right)
                }
            };
            let nullable = left.expr_type.nullable || right.expr_type.nullable;
            Ok(TypedExpr {
                kind: TypedExprKind::Binary {
                    operator,
                    left: Box::new(left),
                    right: Box::new(right),
                },
                expr_type: ExprType {
                    data_type: bool_type,
                    nullable,
                },
                span: *span,
            })
        }
        AstExpr::Unary {
            operator,
            expression,
            span,
        } => {
            let bool_type = SemanticType::physical(PhysicalType::Bool);
            let expression = lower_expr(table, expression, Some(&bool_type))?;
            require_type(&expression, &bool_type)?;
            Ok(TypedExpr {
                expr_type: ExprType {
                    data_type: bool_type,
                    nullable: expression.expr_type.nullable,
                },
                kind: TypedExprKind::Unary {
                    operator: match operator {
                        AstUnaryOp::Not => UnaryOp::Not,
                    },
                    expression: Box::new(expression),
                },
                span: *span,
            })
        }
        AstExpr::IsNull {
            expression,
            negated,
            span,
        } => {
            let bool_type = SemanticType::physical(PhysicalType::Bool);
            let expression = if is_null_literal(expression) {
                lower_expr(table, expression, Some(&bool_type))?
            } else {
                lower_expr(table, expression, None)?
            };
            Ok(TypedExpr {
                expr_type: ExprType {
                    data_type: bool_type,
                    nullable: false,
                },
                kind: TypedExprKind::IsNull {
                    expression: Box::new(expression),
                    negated: *negated,
                },
                span: *span,
            })
        }
    }
}

fn lower_comparison_operands(
    table: &TableDef,
    left: &AstExpr,
    right: &AstExpr,
) -> Result<(TypedExpr, TypedExpr), HirError> {
    match (is_null_literal(left), is_null_literal(right)) {
        (true, true) => {
            // With no operand context, BOOL is a deterministic carrier type;
            // both runtime values remain NULL and comparison yields UNKNOWN.
            let carrier = SemanticType::physical(PhysicalType::Bool);
            Ok((
                lower_expr(table, left, Some(&carrier))?,
                lower_expr(table, right, Some(&carrier))?,
            ))
        }
        (true, false) => {
            let right = lower_expr(table, right, None)?;
            let left = lower_expr(table, left, Some(&right.expr_type.data_type))?;
            Ok((left, right))
        }
        (false, true) => {
            let left = lower_expr(table, left, None)?;
            let right = lower_expr(table, right, Some(&left.expr_type.data_type))?;
            Ok((left, right))
        }
        (false, false) => Ok((
            lower_expr(table, left, None)?,
            lower_expr(table, right, None)?,
        )),
    }
}

fn is_null_literal(expression: &AstExpr) -> bool {
    matches!(
        expression,
        AstExpr::Literal {
            value: Literal::Null,
            ..
        }
    )
}

fn require_type(expression: &TypedExpr, expected: &SemanticType) -> Result<(), HirError> {
    if expression.expr_type.data_type == *expected {
        Ok(())
    } else {
        Err(HirError::TypeMismatch {
            expected: expected.clone(),
            actual: expression.expr_type.data_type.clone(),
            span: expression.span,
        })
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
        nullable: column.nullable,
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
    use super::{HirError, TypedExprKind, TypedStatement, UnaryOp, lower_query, lower_statement};
    use netbadb_parser::{parse, parse_statement};
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
                )
                .nullable(true),
                ColumnDef::new(
                    ColumnId(4),
                    "nickname",
                    TypeSpec::Physical(PhysicalType::Text),
                )
                .nullable(true),
                ColumnDef::new(
                    ColumnId(5),
                    "team_id",
                    TypeSpec::Semantic {
                        name: "TeamId".into(),
                        physical: PhysicalType::UInt64,
                    },
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

    #[test]
    fn contextually_types_null_and_tracks_expression_nullability() {
        let query = parse("SELECT nickname FROM users WHERE nickname = NULL").expect("parse");
        let typed = lower_query(&schema(), &query).expect("lower");
        let predicate = typed.selection.expect("predicate");
        assert_eq!(predicate.expr_type.data_type.physical, PhysicalType::Bool);
        assert!(predicate.expr_type.nullable);
        let TypedExprKind::Binary { right, .. } = predicate.kind else {
            panic!("expected comparison");
        };
        assert_eq!(right.expr_type.data_type.physical, PhysicalType::Text);
        assert!(right.expr_type.nullable);
    }

    #[test]
    fn is_null_is_non_nullable_and_not_preserves_nullability() {
        let is_null = lower_query(
            &schema(),
            &parse("SELECT id FROM users WHERE nickname IS NULL").expect("parse"),
        )
        .expect("lower")
        .selection
        .expect("predicate");
        assert!(!is_null.expr_type.nullable);
        assert!(matches!(is_null.kind, TypedExprKind::IsNull { .. }));

        let not = lower_query(
            &schema(),
            &parse("SELECT id FROM users WHERE NOT active").expect("parse"),
        )
        .expect("lower")
        .selection
        .expect("predicate");
        assert!(not.expr_type.nullable);
        assert!(matches!(
            not.kind,
            TypedExprKind::Unary {
                operator: UnaryOp::Not,
                ..
            }
        ));
    }

    #[test]
    fn where_null_is_a_nullable_boolean() {
        let typed = lower_query(
            &schema(),
            &parse("SELECT id FROM users WHERE NULL").expect("parse"),
        )
        .expect("lower");
        let predicate = typed.selection.expect("predicate");
        assert_eq!(predicate.expr_type.data_type.physical, PhysicalType::Bool);
        assert!(predicate.expr_type.nullable);
    }

    #[test]
    fn null_does_not_weaken_nominal_comparisons() {
        let null_query = parse("SELECT id FROM users WHERE id = NULL").expect("parse");
        assert!(lower_query(&schema(), &null_query).is_ok());

        let incompatible = parse("SELECT id FROM users WHERE id = team_id").expect("parse");
        assert!(matches!(
            lower_query(&schema(), &incompatible),
            Err(HirError::IncompatibleComparison { .. })
        ));
    }

    #[test]
    fn rejects_unknown_columns_non_boolean_where_and_invalid_not() {
        let unknown = parse("SELECT id FROM users WHERE missing = 1").expect("parse");
        assert!(matches!(
            lower_query(&schema(), &unknown),
            Err(HirError::UnknownColumn { .. })
        ));

        let non_boolean = parse("SELECT id FROM users WHERE nickname").expect("parse");
        assert!(matches!(
            lower_query(&schema(), &non_boolean),
            Err(HirError::TypeMismatch { .. })
        ));

        let invalid_not = parse("SELECT id FROM users WHERE NOT nickname").expect("parse");
        assert!(matches!(
            lower_query(&schema(), &invalid_not),
            Err(HirError::TypeMismatch { .. })
        ));
    }

    #[test]
    fn types_insert_and_fills_missing_nullable_columns() {
        let statement = parse_statement("INSERT INTO users (name) VALUES ('Ada')").expect("parse");
        let error = lower_statement(&schema(), &statement).expect_err("id is required");
        assert!(matches!(error, HirError::MissingRequiredColumn { name, .. } if name == "id"));

        let statement =
            parse_statement("INSERT INTO users (id, name, active) VALUES (NULL, 'Ada', true)")
                .expect("parse");
        assert!(matches!(
            lower_statement(&schema(), &statement),
            Err(HirError::NullNotAllowed { name, .. }) if name == "id"
        ));
    }

    #[test]
    fn rejects_duplicate_dml_targets_and_value_count_mismatch() {
        let duplicate =
            parse_statement("INSERT INTO users (name, name) VALUES ('a', 'b')").expect("parse");
        assert!(matches!(
            lower_statement(&schema(), &duplicate),
            Err(HirError::DuplicateColumn { name, .. }) if name == "name"
        ));

        let mismatch =
            parse_statement("INSERT INTO users (id, name) VALUES (NULL)").expect("parse");
        assert!(matches!(
            lower_statement(&schema(), &mismatch),
            Err(HirError::ValueCountMismatch { .. })
        ));

        let update =
            parse_statement("UPDATE users SET nickname = 'a', nickname = 'b'").expect("parse");
        assert!(matches!(
            lower_statement(&schema(), &update),
            Err(HirError::DuplicateColumn { name, .. }) if name == "nickname"
        ));
    }

    #[test]
    fn types_update_delete_and_preserves_nominal_assignment_safety() {
        let update =
            parse_statement("UPDATE users SET nickname = NULL WHERE active").expect("parse");
        assert!(matches!(
            lower_statement(&schema(), &update).expect("lower update"),
            TypedStatement::Update(update)
                if update.assignments[0].value.expr_type.nullable
                    && update.selection.is_some()
        ));

        let incompatible = parse_statement("UPDATE users SET id = team_id").expect("parse");
        assert!(matches!(
            lower_statement(&schema(), &incompatible),
            Err(HirError::TypeMismatch { .. })
        ));

        let delete = parse_statement("DELETE FROM users WHERE nickname IS NULL").expect("parse");
        assert!(matches!(
            lower_statement(&schema(), &delete).expect("lower delete"),
            TypedStatement::Delete(delete) if delete.selection.is_some()
        ));
    }
}
