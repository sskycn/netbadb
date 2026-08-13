//! Typed High-level IR produced after name resolution and type checking.

use std::collections::HashSet;
use std::error::Error;
use std::fmt;

use netbadb_parser::{
    BinaryOp as AstBinaryOp, ColumnName, Expr as AstExpr, FromItem, Ident, Literal, Query, Span,
    Statement as AstStatement, UnaryOp as AstUnaryOp,
};
use netbadb_schema::{Schema, TableDef};
use netbadb_types::{
    ColumnId, ExprType, PhysicalType, RelationBindingId, ScalarValue, SemanticType, TableId,
};

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
pub struct TypedRelation {
    pub binding_id: RelationBindingId,
    pub table_id: TableId,
    pub table_name: String,
    pub exposed_name: String,
    pub columns: Vec<ColumnRef>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypedJoin {
    pub kind: JoinKind,
    pub right: TypedRelation,
    pub predicate: TypedExpr,
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
    pub from: TypedRelation,
    pub joins: Vec<TypedJoin>,
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
    UnknownRelationQualifier {
        name: String,
        span: Span,
    },
    DuplicateRelationName {
        name: String,
        span: Span,
    },
    AmbiguousColumn {
        name: String,
        span: Span,
    },
    TooManyRelations {
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
            Self::UnknownRelationQualifier { name, .. } => {
                write!(formatter, "unknown relation qualifier `{name}`")
            }
            Self::DuplicateRelationName { name, .. } => {
                write!(
                    formatter,
                    "relation name `{name}` is exposed more than once"
                )
            }
            Self::AmbiguousColumn { name, .. } => {
                write!(formatter, "column `{name}` is ambiguous")
            }
            Self::TooManyRelations { .. } => {
                formatter.write_str("query contains too many relation bindings")
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
    let mut scope = RelationScope::default();
    let from = scope.add(schema, &query.from)?;
    let bool_type = SemanticType::physical(PhysicalType::Bool);
    let mut joins = Vec::with_capacity(query.joins.len());
    for join in &query.joins {
        let right = scope.add(schema, &join.right)?;
        let predicate = lower_expr_in_scope(&scope, &join.condition, Some(&bool_type))?;
        require_type(&predicate, &bool_type)?;
        joins.push(TypedJoin {
            kind: JoinKind::Inner,
            right,
            predicate,
        });
    }

    let projection = query
        .projection
        .iter()
        .try_fold(Vec::new(), |mut columns, item| {
            match item {
                netbadb_parser::SelectItem::Wildcard(_) => {
                    columns.extend(scope.columns());
                }
                netbadb_parser::SelectItem::Column(column) => {
                    columns.push(scope.resolve(column)?);
                }
            }
            Ok::<_, HirError>(columns)
        })?;

    let selection = query
        .selection
        .as_ref()
        .map(|expression| lower_expr_in_scope(&scope, expression, Some(&bool_type)))
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
        from,
        joins,
        columns: scope.columns(),
        projection,
        selection,
        limit: query.limit,
    })
}

#[derive(Default)]
struct RelationScope<'a> {
    bindings: Vec<ScopeBinding<'a>>,
}

struct ScopeBinding<'a> {
    binding_id: RelationBindingId,
    exposed_name: String,
    table: &'a TableDef,
}

impl<'a> RelationScope<'a> {
    fn single(table: &'a TableDef) -> Self {
        Self {
            bindings: vec![ScopeBinding {
                binding_id: RelationBindingId(0),
                exposed_name: table.name.clone(),
                table,
            }],
        }
    }

    fn add(&mut self, schema: &'a Schema, item: &FromItem) -> Result<TypedRelation, HirError> {
        let table = schema
            .table(&item.table.name)
            .ok_or_else(|| HirError::UnknownTable {
                name: item.table.name.clone(),
                span: item.table.span,
            })?;
        let exposed = item.alias.as_ref().unwrap_or(&item.table);
        if self
            .bindings
            .iter()
            .any(|binding| binding.exposed_name == exposed.name)
        {
            return Err(HirError::DuplicateRelationName {
                name: exposed.name.clone(),
                span: exposed.span,
            });
        }
        let ordinal = u32::try_from(self.bindings.len())
            .map_err(|_| HirError::TooManyRelations { span: item.span })?;
        let binding_id = RelationBindingId(ordinal);
        let binding = ScopeBinding {
            binding_id,
            exposed_name: exposed.name.clone(),
            table,
        };
        let relation = binding.typed_relation();
        self.bindings.push(binding);
        Ok(relation)
    }

    fn resolve(&self, column: &ColumnName) -> Result<ColumnRef, HirError> {
        if let Some(qualifier) = &column.qualifier {
            let binding = self
                .bindings
                .iter()
                .find(|binding| binding.exposed_name == qualifier.name)
                .ok_or_else(|| HirError::UnknownRelationQualifier {
                    name: qualifier.name.clone(),
                    span: qualifier.span,
                })?;
            return binding
                .column(&column.name)
                .ok_or_else(|| HirError::UnknownColumn {
                    table: binding.exposed_name.clone(),
                    name: column.name.name.clone(),
                    span: column.name.span,
                });
        }

        let mut matches = self
            .bindings
            .iter()
            .filter_map(|binding| binding.column(&column.name));
        let resolved = matches.next().ok_or_else(|| HirError::UnknownColumn {
            table: "query scope".into(),
            name: column.name.name.clone(),
            span: column.name.span,
        })?;
        if matches.next().is_some() {
            return Err(HirError::AmbiguousColumn {
                name: column.name.name.clone(),
                span: column.span,
            });
        }
        Ok(resolved)
    }

    fn columns(&self) -> Vec<ColumnRef> {
        self.bindings
            .iter()
            .flat_map(ScopeBinding::columns)
            .collect()
    }
}

impl ScopeBinding<'_> {
    fn column(&self, name: &Ident) -> Option<ColumnRef> {
        self.table.column(&name.name).map(|column| ColumnRef {
            binding_id: self.binding_id,
            table_id: self.table.id,
            column_id: column.id,
            relation_name: self.exposed_name.clone(),
            name: column.name.clone(),
            data_type: column.semantic_type(),
            nullable: column.nullable,
        })
    }

    fn columns(&self) -> impl Iterator<Item = ColumnRef> + '_ {
        self.table.columns.iter().map(|column| ColumnRef {
            binding_id: self.binding_id,
            table_id: self.table.id,
            column_id: column.id,
            relation_name: self.exposed_name.clone(),
            name: column.name.clone(),
            data_type: column.semantic_type(),
            nullable: column.nullable,
        })
    }

    fn typed_relation(&self) -> TypedRelation {
        TypedRelation {
            binding_id: self.binding_id,
            table_id: self.table.id,
            table_name: self.table.name.clone(),
            exposed_name: self.exposed_name.clone(),
            columns: self.columns().collect(),
        }
    }
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
            .map(|column| column_ref(table, column))
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
            .map(|column| column_ref(table, column))
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
    lower_expr_in_scope(&RelationScope::single(table), expression, expected)
}

fn lower_expr_in_scope(
    scope: &RelationScope<'_>,
    expression: &AstExpr,
    expected: Option<&SemanticType>,
) -> Result<TypedExpr, HirError> {
    match expression {
        AstExpr::Column(column) => {
            let resolved = scope.resolve(column)?;
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
                    let left = lower_expr_in_scope(scope, left, Some(&bool_type))?;
                    let right = lower_expr_in_scope(scope, right, Some(&bool_type))?;
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
                    let (left, right) = lower_comparison_operands(scope, left, right)?;
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
            let expression = lower_expr_in_scope(scope, expression, Some(&bool_type))?;
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
                lower_expr_in_scope(scope, expression, Some(&bool_type))?
            } else {
                lower_expr_in_scope(scope, expression, None)?
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
    scope: &RelationScope<'_>,
    left: &AstExpr,
    right: &AstExpr,
) -> Result<(TypedExpr, TypedExpr), HirError> {
    match (is_null_literal(left), is_null_literal(right)) {
        (true, true) => {
            // With no operand context, BOOL is a deterministic carrier type;
            // both runtime values remain NULL and comparison yields UNKNOWN.
            let carrier = SemanticType::physical(PhysicalType::Bool);
            Ok((
                lower_expr_in_scope(scope, left, Some(&carrier))?,
                lower_expr_in_scope(scope, right, Some(&carrier))?,
            ))
        }
        (true, false) => {
            let right = lower_expr_in_scope(scope, right, None)?;
            let left = lower_expr_in_scope(scope, left, Some(&right.expr_type.data_type))?;
            Ok((left, right))
        }
        (false, true) => {
            let left = lower_expr_in_scope(scope, left, None)?;
            let right = lower_expr_in_scope(scope, right, Some(&left.expr_type.data_type))?;
            Ok((left, right))
        }
        (false, false) => Ok((
            lower_expr_in_scope(scope, left, None)?,
            lower_expr_in_scope(scope, right, None)?,
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
        .map(|column_def| column_ref(table, column_def))
        .ok_or_else(|| HirError::UnknownColumn {
            table: table.name.clone(),
            name: column.name.clone(),
            span: column.span,
        })
}

fn column_ref(table: &TableDef, column: &netbadb_schema::ColumnDef) -> ColumnRef {
    ColumnRef {
        binding_id: RelationBindingId(0),
        table_id: table.id,
        column_id: column.id,
        relation_name: table.name.clone(),
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
    use netbadb_types::{ColumnId, PhysicalType, RelationBindingId, TableId};

    fn schema() -> Schema {
        Schema::new(vec![
            TableDef::new(
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
                    )
                    .nullable(true),
                ],
            ),
            TableDef::new(
                TableId(2),
                "teams",
                vec![
                    ColumnDef::new(
                        ColumnId(1),
                        "id",
                        TypeSpec::Semantic {
                            name: "TeamId".into(),
                            physical: PhysicalType::UInt64,
                        },
                    ),
                    ColumnDef::new(
                        ColumnId(2),
                        "owner_id",
                        TypeSpec::Semantic {
                            name: "UserId".into(),
                            physical: PhysicalType::UInt64,
                        },
                    ),
                    ColumnDef::new(ColumnId(3), "name", TypeSpec::Physical(PhysicalType::Text)),
                ],
            ),
            TableDef::new(
                TableId(3),
                "employees",
                vec![
                    ColumnDef::new(
                        ColumnId(1),
                        "id",
                        TypeSpec::Semantic {
                            name: "EmployeeId".into(),
                            physical: PhysicalType::UInt64,
                        },
                    ),
                    ColumnDef::new(
                        ColumnId(2),
                        "manager_id",
                        TypeSpec::Semantic {
                            name: "EmployeeId".into(),
                            physical: PhysicalType::UInt64,
                        },
                    )
                    .nullable(true),
                    ColumnDef::new(ColumnId(3), "name", TypeSpec::Physical(PhysicalType::Text)),
                ],
            ),
        ])
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

    #[test]
    fn resolves_qualified_and_unique_unqualified_join_columns() {
        let typed = lower_query(
            &schema(),
            &parse(
                "SELECT u.active, t.owner_id FROM users AS u JOIN teams AS t \
                 ON u.team_id = t.id WHERE active IS NULL",
            )
            .expect("parse"),
        )
        .expect("lower");
        assert_eq!(typed.from.binding_id, RelationBindingId(0));
        assert_eq!(typed.joins[0].right.binding_id, RelationBindingId(1));
        assert_eq!(typed.projection[0].relation_name, "u");
        assert!(typed.joins[0].predicate.expr_type.nullable);
    }

    #[test]
    fn rejects_ambiguous_unknown_and_duplicate_relation_names() {
        let ambiguous =
            parse("SELECT id FROM users u JOIN teams t ON u.team_id = t.id").expect("parse");
        assert!(matches!(
            lower_query(&schema(), &ambiguous),
            Err(HirError::AmbiguousColumn { name, .. }) if name == "id"
        ));

        let unknown_qualifier = parse("SELECT x.id FROM users u").expect("parse unknown qualifier");
        assert!(matches!(
            lower_query(&schema(), &unknown_qualifier),
            Err(HirError::UnknownRelationQualifier { name, .. }) if name == "x"
        ));

        let hidden_table = parse("SELECT users.id FROM users AS u").expect("parse hidden table");
        assert!(matches!(
            lower_query(&schema(), &hidden_table),
            Err(HirError::UnknownRelationQualifier { name, .. }) if name == "users"
        ));

        let duplicate = parse("SELECT x.id FROM users x JOIN teams x ON x.team_id = x.id")
            .expect("parse duplicate alias");
        assert!(matches!(
            lower_query(&schema(), &duplicate),
            Err(HirError::DuplicateRelationName { name, .. }) if name == "x"
        ));
    }

    #[test]
    fn join_on_scope_does_not_include_future_relations() {
        let query = parse(
            "SELECT u.id FROM users u JOIN teams t ON e.id = t.id \
             JOIN employees e ON e.id = u.id",
        )
        .expect("parse");
        assert!(matches!(
            lower_query(&schema(), &query),
            Err(HirError::UnknownRelationQualifier { name, .. }) if name == "e"
        ));
    }

    #[test]
    fn self_join_uses_distinct_bindings_and_nominal_predicate_types() {
        let typed = lower_query(
            &schema(),
            &parse(
                "SELECT e.name, m.name FROM employees e JOIN employees m \
                 ON e.manager_id = m.id",
            )
            .expect("parse"),
        )
        .expect("self join lowers");
        assert_eq!(typed.projection[0].table_id, typed.projection[1].table_id);
        assert_ne!(
            typed.projection[0].binding_id,
            typed.projection[1].binding_id
        );

        let nominal_mismatch =
            parse("SELECT u.id FROM users u JOIN teams t ON u.id = t.id").expect("parse mismatch");
        assert!(matches!(
            lower_query(&schema(), &nominal_mismatch),
            Err(HirError::IncompatibleComparison { .. })
        ));

        let non_boolean =
            parse("SELECT u.id FROM users u JOIN teams t ON t.id").expect("parse non-bool");
        assert!(matches!(
            lower_query(&schema(), &non_boolean),
            Err(HirError::TypeMismatch { .. })
        ));

        let is_null = parse("SELECT u.id FROM users u JOIN teams t ON u.team_id IS NULL")
            .expect("parse IS NULL join");
        let typed = lower_query(&schema(), &is_null).expect("IS NULL is a valid ON expression");
        assert!(!typed.joins[0].predicate.expr_type.nullable);
    }
}
