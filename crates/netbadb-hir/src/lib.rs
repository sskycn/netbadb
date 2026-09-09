//! Typed High-level IR produced after name resolution and type checking.

use std::collections::HashSet;
use std::error::Error;
use std::fmt;

use netbadb_parser::{
    AggregateArgument as AstAggregateArgument, AggregateCall as AstAggregateCall,
    AggregateFunction as AstAggregateFunction, AlterTableAction as AstAlterTableAction,
    AlterTableStatement as AstAlterTableStatement, BinaryOp as AstBinaryOp, ColumnName,
    CreateIndexStatement as AstCreateIndexStatement, DropTableStatement as AstDropTableStatement,
    Expr as AstExpr, FromItem, Ident, Literal, NullOrder as AstNullOrder, Query,
    SortDirection as AstSortDirection, Span, SqlTypeName as AstSqlTypeName,
    Statement as AstStatement, UnaryOp as AstUnaryOp,
};
use netbadb_schema::{DropTableTarget, Schema, TableDef};
use netbadb_types::{
    ColumnId, ExprType, Float32Value, Float64Value, IndexId, IndexName, ParameterId, PhysicalType,
    RelationBindingId, ScalarValue, SemanticType, TableId,
};

/// Checked logical columns in declaration order; identities are allocated at Execute.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypedCreateTable {
    pub name: String,
    pub columns: Vec<TypedCreateColumn>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypedCreateColumn {
    pub name: String,
    pub data_type: SemanticType,
    pub nullable: bool,
    pub span: Span,
    pub type_span: Span,
}

/// Exact table identity available in the current schema view at compile time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableIdentityBinding {
    pub target: DropTableTarget,
}

/// Prepared DROP TABLE payload. The source name is diagnostic-only; execution
/// consumes `target` and never performs a second name lookup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypedDropTable {
    pub name: String,
    pub target: DropTableTarget,
    pub name_span: Span,
    pub span: Span,
}

/// Exact prepared table identity plus one frontend-neutral logical rewrite.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypedAlterTable {
    pub table_name: String,
    pub target: DropTableTarget,
    pub operation: TypedAlterTableOperation,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TypedAlterTableOperation {
    RenameTable {
        new_name: String,
    },
    RenameColumn {
        column_id: ColumnId,
        new_name: String,
    },
    AddNullableColumn {
        name: String,
        data_type: SemanticType,
    },
    DropColumn {
        column_id: ColumnId,
    },
    SetNotNull {
        column_id: ColumnId,
    },
    DropNotNull {
        column_id: ColumnId,
    },
}

pub fn lower_drop_table(
    schema: &Schema,
    statement: &AstDropTableStatement,
    tables: &[TableIdentityBinding],
) -> Result<TypedDropTable, HirError> {
    let table = schema
        .table(&statement.name.name)
        .ok_or_else(|| HirError::UnknownTable {
            name: statement.name.name.clone(),
            span: statement.name.span,
        })?;
    let target = tables
        .iter()
        .find(|binding| binding.target.table_id == table.id)
        .map(|binding| binding.target)
        .ok_or_else(|| HirError::UnknownTable {
            name: statement.name.name.clone(),
            span: statement.name.span,
        })?;
    Ok(TypedDropTable {
        name: statement.name.name.clone(),
        target,
        name_span: statement.name.span,
        span: statement.span,
    })
}

pub fn lower_alter_table(
    schema: &Schema,
    statement: &AstAlterTableStatement,
    tables: &[TableIdentityBinding],
) -> Result<TypedAlterTable, HirError> {
    let table = schema
        .table(&statement.table.name)
        .ok_or_else(|| HirError::UnknownTable {
            name: statement.table.name.clone(),
            span: statement.table.span,
        })?;
    let target = tables
        .iter()
        .find(|binding| binding.target.table_id == table.id)
        .map(|binding| binding.target)
        .ok_or_else(|| HirError::UnknownTable {
            name: statement.table.name.clone(),
            span: statement.table.span,
        })?;
    let resolve_column = |name: &Ident| {
        table
            .column(&name.name)
            .map(|column| column.id)
            .ok_or_else(|| HirError::UnknownColumn {
                table: table.name.clone(),
                name: name.name.clone(),
                span: name.span,
            })
    };
    let operation = match &statement.action {
        AstAlterTableAction::RenameTable { new_name } => {
            if schema.table(&new_name.name).is_some() {
                return Err(HirError::DuplicateTable {
                    name: new_name.name.clone(),
                    span: new_name.span,
                });
            }
            TypedAlterTableOperation::RenameTable {
                new_name: new_name.name.clone(),
            }
        }
        AstAlterTableAction::RenameColumn { old_name, new_name } => {
            let column_id = resolve_column(old_name)?;
            if table.column(&new_name.name).is_some() {
                return Err(HirError::DuplicateColumn {
                    name: new_name.name.clone(),
                    span: new_name.span,
                });
            }
            TypedAlterTableOperation::RenameColumn {
                column_id,
                new_name: new_name.name.clone(),
            }
        }
        AstAlterTableAction::AddColumn { column } => {
            if table.column(&column.name.name).is_some() {
                return Err(HirError::DuplicateColumn {
                    name: column.name.name.clone(),
                    span: column.name.span,
                });
            }
            TypedAlterTableOperation::AddNullableColumn {
                name: column.name.name.clone(),
                data_type: resolve_declared_type(&column.data_type)?,
            }
        }
        AstAlterTableAction::DropColumn { name } => TypedAlterTableOperation::DropColumn {
            column_id: resolve_column(name)?,
        },
        AstAlterTableAction::SetNotNull { column_name } => TypedAlterTableOperation::SetNotNull {
            column_id: resolve_column(column_name)?,
        },
        AstAlterTableAction::DropNotNull { column_name } => TypedAlterTableOperation::DropNotNull {
            column_id: resolve_column(column_name)?,
        },
    };
    Ok(TypedAlterTable {
        table_name: table.name.clone(),
        target,
        operation,
        span: statement.span,
    })
}

/// Resolve a declaration, without an existing-table lookup or nominal inference.
pub fn lower_create_table(
    statement: &netbadb_parser::CreateTableStatement,
) -> Result<TypedCreateTable, HirError> {
    let mut seen = HashSet::new();
    let mut columns = Vec::with_capacity(statement.columns.len());
    for column in &statement.columns {
        if !seen.insert(&column.name.name) {
            return Err(HirError::DuplicateColumn {
                name: column.name.name.clone(),
                span: column.name.span,
            });
        }
        let data_type = resolve_declared_type(&column.data_type)?;
        columns.push(TypedCreateColumn {
            name: column.name.name.clone(),
            data_type,
            nullable: column.nullability.is_none_or(|n| n.nullable),
            span: column.span,
            type_span: column.data_type.span,
        });
    }
    Ok(TypedCreateTable {
        name: statement.name.name.clone(),
        columns,
        span: statement.span,
    })
}

fn resolve_declared_type(name: &Ident) -> Result<SemanticType, HirError> {
    let physical = match name.name.to_ascii_uppercase().as_str() {
        "BOOL" | "BOOLEAN" => PhysicalType::Bool,
        "TINYINT" => PhysicalType::Int8,
        "SMALLINT" | "INT2" | "INT16" => PhysicalType::Int16,
        "INTEGER" | "INT" | "INT4" | "INT32" => PhysicalType::Int32,
        "BIGINT" | "INT64" | "INT8" => PhysicalType::Int64,
        "INT128" => PhysicalType::Int128,
        "UINT8" => PhysicalType::UInt8,
        "UINT16" => PhysicalType::UInt16,
        "UINT32" => PhysicalType::UInt32,
        "TEXT" | "VARCHAR" => PhysicalType::Text,
        "UINT64" => PhysicalType::UInt64,
        "UINT128" => PhysicalType::UInt128,
        "REAL" | "FLOAT4" | "FLOAT32" => PhysicalType::Float32,
        "DOUBLE" | "FLOAT8" | "FLOAT64" => PhysicalType::Float64,
        "BYTES" | "BYTEA" => PhysicalType::Bytes,
        "NUMERIC" | "DECIMAL" | "FLOAT" | "DATE" | "TIME" | "TIMESTAMP" | "TIMESTAMPTZ"
        | "INTERVAL" | "JSON" | "JSONB" | "UUID" | "ARRAY" | "SERIAL" | "BIGSERIAL"
        | "SMALLSERIAL" | "CHAR" | "CHARACTER" => {
            return Err(HirError::UnsupportedType {
                name: name.name.clone(),
                span: name.span,
            });
        }
        _ => {
            return Err(HirError::UnknownType {
                name: name.name.clone(),
                span: name.span,
            });
        }
    };
    Ok(SemanticType::physical(physical))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypedCreateIndex {
    pub name: IndexName,
    pub target: DropTableTarget,
    pub table_name: String,
    pub column_id: ColumnId,
    pub column_name: String,
    pub if_not_exists: bool,
}

/// Resolved generic identity, independent of PostgreSQL naming and storage pages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DropIndexTarget {
    pub table_id: TableId,
    pub index_id: IndexId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexNameBinding {
    pub name: IndexName,
    pub target: DropIndexTarget,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypedDropIndex {
    pub name: Option<IndexName>,
    pub target: Option<DropIndexTarget>,
    pub if_exists: bool,
}

pub fn lower_drop_index(
    statement: &netbadb_parser::DropIndexStatement,
    indexes: &[IndexNameBinding],
) -> Result<TypedDropIndex, HirError> {
    if statement
        .name
        .qualifier
        .as_ref()
        .is_some_and(|schema| schema.name != "public")
    {
        return Err(HirError::InvalidIndexDefinition {
            message: "only the public index namespace is supported",
            span: statement.name.span,
        });
    }
    let name = IndexName::new(statement.name.name.name.clone()).map_err(|_| {
        HirError::InvalidIndexDefinition {
            message: "invalid index name",
            span: statement.name.span,
        }
    })?;
    let target = indexes
        .iter()
        .find(|binding| binding.name == name)
        .map(|binding| binding.target);
    Ok(TypedDropIndex {
        name: Some(name),
        target,
        if_exists: statement.if_exists,
    })
}

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
pub struct TypedOrderKey {
    pub column: ColumnRef,
    pub direction: SortDirection,
    pub null_order: NullOrder,
    pub span: Span,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFunction {
    Count,
    Sum,
    Min,
    Max,
}

impl AggregateFunction {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Count => "COUNT",
            Self::Sum => "SUM",
            Self::Min => "MIN",
            Self::Max => "MAX",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TypedAggregateInput {
    All,
    Column(ColumnRef),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypedAggregate {
    pub function: AggregateFunction,
    pub input: TypedAggregateInput,
    pub expr_type: ExprType,
    pub output_name: String,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TypedProjectionItem {
    Column(ColumnRef),
    Aggregate(TypedAggregate),
    Expression {
        expression: TypedExpr,
        output_name: String,
    },
}

impl TypedProjectionItem {
    #[must_use]
    pub const fn source_column(&self) -> Option<&ColumnRef> {
        match self {
            Self::Column(column) => Some(column),
            Self::Aggregate(_) | Self::Expression { .. } => None,
        }
    }
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
    Parameter(ParameterId),
    Cast {
        expression: Box<TypedExpr>,
    },
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
    pub from: Option<TypedRelation>,
    pub joins: Vec<TypedJoin>,
    pub columns: Vec<ColumnRef>,
    pub projection: Vec<TypedProjectionItem>,
    pub selection: Option<TypedExpr>,
    pub group_by: Vec<ColumnRef>,
    pub order_by: Vec<TypedOrderKey>,
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
    UnknownType {
        name: String,
        span: Span,
    },
    UnsupportedType {
        name: String,
        span: Span,
    },
    InvalidTableDefinition {
        message: &'static str,
        span: Span,
    },
    InvalidIndexDefinition {
        message: &'static str,
        span: Span,
    },
    UnknownTable {
        name: String,
        span: Span,
    },
    DuplicateTable {
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
    UnsupportedCast {
        source: PhysicalType,
        target: PhysicalType,
        span: Span,
    },
    CannotInferNullType {
        span: Span,
    },
    InvalidLiteral {
        message: &'static str,
        span: Span,
    },
    CannotInferParameterType {
        id: ParameterId,
        span: Span,
    },
    ParameterTypeConflict {
        id: ParameterId,
        previous: SemanticType,
        required: SemanticType,
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
    UngroupedColumn {
        name: String,
        span: Span,
    },
    WildcardNotSupportedWithGroupBy {
        span: Span,
    },
    OrderByNotSupportedWithGrouping {
        span: Span,
    },
    InvalidAggregateArgument {
        function: AggregateFunction,
        span: Span,
    },
    InvalidAggregateType {
        function: AggregateFunction,
        actual: SemanticType,
        span: Span,
    },
}

impl HirError {
    /// Returns the exact UTF-8 byte span supplied by the parsed source node.
    #[must_use]
    pub const fn span(&self) -> Span {
        match self {
            Self::UnknownType { span, .. }
            | Self::UnsupportedType { span, .. }
            | Self::InvalidTableDefinition { span, .. }
            | Self::InvalidIndexDefinition { span, .. }
            | Self::UnknownTable { span, .. }
            | Self::DuplicateTable { span, .. }
            | Self::UnknownColumn { span, .. }
            | Self::UnknownRelationQualifier { span, .. }
            | Self::DuplicateRelationName { span, .. }
            | Self::AmbiguousColumn { span, .. }
            | Self::TooManyRelations { span }
            | Self::TypeMismatch { span, .. }
            | Self::IncompatibleComparison { span, .. }
            | Self::UnsupportedCast { span, .. }
            | Self::CannotInferNullType { span }
            | Self::InvalidLiteral { span, .. }
            | Self::CannotInferParameterType { span, .. }
            | Self::ParameterTypeConflict { span, .. }
            | Self::DuplicateColumn { span, .. }
            | Self::ValueCountMismatch { span, .. }
            | Self::MissingRequiredColumn { span, .. }
            | Self::NullNotAllowed { span, .. }
            | Self::InsertValueReferencesColumn { span }
            | Self::UngroupedColumn { span, .. }
            | Self::WildcardNotSupportedWithGroupBy { span }
            | Self::OrderByNotSupportedWithGrouping { span }
            | Self::InvalidAggregateArgument { span, .. }
            | Self::InvalidAggregateType { span, .. } => *span,
        }
    }
}

impl fmt::Display for HirError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownType { name, .. } => write!(formatter, "unknown type `{name}`"),
            Self::UnsupportedType { name, .. } => {
                write!(formatter, "type `{name}` is not supported")
            }
            Self::InvalidTableDefinition { message, .. } => formatter.write_str(message),
            Self::InvalidIndexDefinition { message, .. } => formatter.write_str(message),
            Self::UnknownTable { name, .. } => write!(formatter, "unknown table `{name}`"),
            Self::DuplicateTable { name, .. } => write!(formatter, "table `{name}` already exists"),
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
            } => write!(
                formatter,
                "expected {}, found {}",
                expected.sql_name(),
                actual.sql_name()
            ),
            Self::IncompatibleComparison { left, right, .. } => {
                write!(
                    formatter,
                    "cannot compare {} with {}",
                    left.sql_name(),
                    right.sql_name()
                )
            }
            Self::UnsupportedCast { source, target, .. } => {
                write!(
                    formatter,
                    "cannot cast {} to {}",
                    source.sql_name(),
                    target.sql_name()
                )
            }
            Self::CannotInferNullType { .. } => {
                formatter.write_str("cannot infer the type of NULL in this expression")
            }
            Self::InvalidLiteral { message, .. } => formatter.write_str(message),
            Self::CannotInferParameterType { id, .. } => {
                write!(
                    formatter,
                    "cannot infer the type of parameter ${}",
                    id.0 + 1
                )
            }
            Self::ParameterTypeConflict {
                id,
                previous,
                required,
                ..
            } => write!(
                formatter,
                "parameter ${} is constrained as both {} and {}",
                id.0 + 1,
                previous.sql_name(),
                required.sql_name()
            ),
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
            Self::UngroupedColumn { name, .. } => write!(
                formatter,
                "column `{name}` must appear in GROUP BY or be aggregated"
            ),
            Self::WildcardNotSupportedWithGroupBy { .. } => {
                formatter.write_str("wildcard projection is not supported with GROUP BY")
            }
            Self::OrderByNotSupportedWithGrouping { .. } => {
                formatter.write_str("ORDER BY is not supported for grouped queries yet")
            }
            Self::InvalidAggregateArgument { function, .. } => {
                write!(formatter, "{} requires a source column", function.as_str())
            }
            Self::InvalidAggregateType {
                function, actual, ..
            } => write!(
                formatter,
                "{} does not support {}",
                function.as_str(),
                actual.sql_name()
            ),
        }
    }
}

impl Error for HirError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParameterMetadata {
    pub id: ParameterId,
    pub data_type: SemanticType,
}

/// Frontend-supplied information used while resolving a parameter.
///
/// Exact constraints are database type declarations and must agree with SQL
/// context. Fallbacks describe a frontend representation to use only when SQL
/// provides no contextual target, such as a PostgreSQL parameter OID.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParameterTypeHint {
    Exact(PhysicalType),
    Fallback(PhysicalType),
}

struct ParameterContext {
    hints: Vec<Option<ParameterTypeHint>>,
    inferred: Vec<Option<SemanticType>>,
    spans: Vec<Option<Span>>,
}

impl ParameterContext {
    fn new(hints: &[Option<ParameterTypeHint>]) -> Self {
        Self {
            hints: hints.to_vec(),
            inferred: vec![None; hints.len()],
            spans: vec![None; hints.len()],
        }
    }

    fn resolve(
        &mut self,
        id: ParameterId,
        expected: Option<&SemanticType>,
        span: Span,
    ) -> Result<SemanticType, HirError> {
        let index =
            usize::try_from(id.0).map_err(|_| HirError::CannotInferParameterType { id, span })?;
        if self.inferred.len() <= index {
            self.inferred.resize(index + 1, None);
            self.hints.resize(index + 1, None);
            self.spans.resize(index + 1, None);
        }
        self.spans[index].get_or_insert(span);
        let exact = match self.hints[index] {
            Some(ParameterTypeHint::Exact(physical)) => Some(SemanticType::physical(physical)),
            Some(ParameterTypeHint::Fallback(_)) | None => None,
        };
        let fallback = match self.hints[index] {
            Some(ParameterTypeHint::Fallback(physical)) => Some(SemanticType::physical(physical)),
            Some(ParameterTypeHint::Exact(_)) | None => None,
        };
        let required = match (expected, exact.as_ref(), fallback) {
            (Some(expected), Some(exact), _) if expected.physical != exact.physical => {
                return Err(HirError::ParameterTypeConflict {
                    id,
                    previous: exact.clone(),
                    required: expected.clone(),
                    span,
                });
            }
            (Some(expected), _, _) => expected.clone(),
            (None, Some(exact), _) => exact.clone(),
            (None, None, Some(fallback)) => fallback,
            (None, None, None) => self.inferred[index]
                .clone()
                .ok_or(HirError::CannotInferParameterType { id, span })?,
        };
        if let Some(previous) = &self.inferred[index] {
            if !previous.is_compatible_with(&required) {
                return Err(HirError::ParameterTypeConflict {
                    id,
                    previous: previous.clone(),
                    required,
                    span,
                });
            }
            return Ok(previous.clone());
        }
        self.inferred[index] = Some(required.clone());
        Ok(required)
    }

    fn has_source_type(&self, id: ParameterId) -> bool {
        usize::try_from(id.0).ok().is_some_and(|index| {
            self.hints.get(index).is_some_and(Option::is_some)
                || self.inferred.get(index).is_some_and(Option::is_some)
        })
    }

    fn finish(self) -> Result<Vec<ParameterMetadata>, HirError> {
        self.inferred
            .into_iter()
            .enumerate()
            .map(|(index, data_type)| {
                let id = ParameterId(u32::try_from(index).unwrap_or(u32::MAX));
                Ok(ParameterMetadata {
                    id,
                    data_type: data_type.ok_or(HirError::CannotInferParameterType {
                        id,
                        span: self.spans[index].unwrap_or(Span { start: 0, end: 0 }),
                    })?,
                })
            })
            .collect()
    }
}

pub fn lower_statement(
    schema: &Schema,
    statement: &AstStatement,
) -> Result<TypedStatement, HirError> {
    lower_statement_with_parameters(schema, statement, &[]).map(|(statement, _)| statement)
}

pub fn lower_statement_with_parameters(
    schema: &Schema,
    statement: &AstStatement,
    declared: &[Option<PhysicalType>],
) -> Result<(TypedStatement, Vec<ParameterMetadata>), HirError> {
    let hints = declared
        .iter()
        .map(|physical| physical.map(ParameterTypeHint::Exact))
        .collect::<Vec<_>>();
    lower_statement_with_parameter_hints(schema, statement, &hints)
}

pub fn lower_statement_with_parameter_hints(
    schema: &Schema,
    statement: &AstStatement,
    hints: &[Option<ParameterTypeHint>],
) -> Result<(TypedStatement, Vec<ParameterMetadata>), HirError> {
    let mut parameters = ParameterContext::new(hints);
    let statement = match statement {
        AstStatement::Select(query) => {
            lower_query_with_context(schema, query, &mut parameters).map(TypedStatement::Select)
        }
        AstStatement::Insert(insert) => {
            lower_insert(schema, insert, &mut parameters).map(TypedStatement::Insert)
        }
        AstStatement::Update(update) => {
            lower_update(schema, update, &mut parameters).map(TypedStatement::Update)
        }
        AstStatement::Delete(delete) => {
            lower_delete(schema, delete, &mut parameters).map(TypedStatement::Delete)
        }
        AstStatement::CreateTable(create) => {
            return Err(HirError::InvalidTableDefinition {
                message: "CREATE TABLE requires the schema-mutation compiler boundary",
                span: create.span,
            });
        }
        AstStatement::DropTable(drop) => {
            return Err(HirError::InvalidTableDefinition {
                message: "DROP TABLE requires the schema-mutation compiler boundary",
                span: drop.span,
            });
        }
        AstStatement::AlterTable(alter) => {
            return Err(HirError::InvalidTableDefinition {
                message: "ALTER TABLE requires the schema-mutation compiler boundary",
                span: alter.span,
            });
        }
        AstStatement::DropIndex(drop) => {
            return Err(HirError::InvalidIndexDefinition {
                message: "DROP INDEX requires the schema-mutation compiler boundary",
                span: drop.span,
            });
        }
        AstStatement::CreateIndex(create) => {
            return Err(HirError::InvalidIndexDefinition {
                message: "CREATE INDEX requires the schema-mutation compiler boundary",
                span: create.span,
            });
        }
    }?;
    Ok((statement, parameters.finish()?))
}

pub fn lower_create_index(
    schema: &Schema,
    statement: &AstCreateIndexStatement,
    tables: &[TableIdentityBinding],
) -> Result<TypedCreateIndex, HirError> {
    if statement
        .table
        .qualifier
        .as_ref()
        .is_some_and(|schema| schema.name != "public")
    {
        return Err(HirError::UnknownTable {
            name: format!(
                "{}.{}",
                statement
                    .table
                    .qualifier
                    .as_ref()
                    .map_or("", |value| value.name.as_str()),
                statement.table.name.name
            ),
            span: statement.table.span,
        });
    }
    let table = resolve_table(schema, &statement.table.name)?;
    let target = tables
        .iter()
        .find(|binding| binding.target.table_id == table.id)
        .map(|binding| binding.target)
        .ok_or_else(|| HirError::UnknownTable {
            name: statement.table.name.name.clone(),
            span: statement.table.span,
        })?;
    let column = table
        .columns
        .iter()
        .find(|column| column.name == statement.column.name)
        .ok_or_else(|| HirError::UnknownColumn {
            table: table.name.clone(),
            name: statement.column.name.clone(),
            span: statement.column.span,
        })?;
    let name = IndexName::new(statement.name.name.clone()).map_err(|_| {
        HirError::InvalidIndexDefinition {
            message: "index name is empty or exceeds the generic identifier bound",
            span: statement.name.span,
        }
    })?;
    Ok(TypedCreateIndex {
        name,
        target,
        table_name: table.name.clone(),
        column_id: column.id,
        column_name: column.name.clone(),
        if_not_exists: statement.if_not_exists,
    })
}

pub fn lower_query(schema: &Schema, query: &Query) -> Result<TypedQuery, HirError> {
    let mut parameters = ParameterContext::new(&[]);
    let query = lower_query_with_context(schema, query, &mut parameters)?;
    let _ = parameters.finish()?;
    Ok(query)
}

fn lower_query_with_context(
    schema: &Schema,
    query: &Query,
    parameters: &mut ParameterContext,
) -> Result<TypedQuery, HirError> {
    let mut scope = RelationScope::default();
    let from = query
        .from
        .as_ref()
        .map(|from| scope.add(schema, from))
        .transpose()?;
    let bool_type = SemanticType::physical(PhysicalType::Bool);
    let mut joins = Vec::with_capacity(query.joins.len());
    for join in &query.joins {
        let right = scope.add(schema, &join.right)?;
        let predicate = lower_expr_in_scope(&scope, &join.condition, Some(&bool_type), parameters)?;
        require_type(&predicate, &bool_type)?;
        joins.push(TypedJoin {
            kind: JoinKind::Inner,
            right,
            predicate,
        });
    }

    let has_aggregate = query
        .projection
        .iter()
        .any(|item| matches!(item, netbadb_parser::SelectItem::Aggregate(_)));
    let group_by = query
        .group_by
        .iter()
        .map(|column| scope.resolve(column))
        .collect::<Result<Vec<_>, _>>()?;
    let is_grouping = has_aggregate || !group_by.is_empty();
    if is_grouping && !query.order_by.is_empty() {
        return Err(HirError::OrderByNotSupportedWithGrouping {
            span: query.order_by[0].span,
        });
    }

    // Predicates often provide the strongest contextual parameter types. Type
    // them before projection so a repeated slot can be used in `SELECT $1`
    // after being constrained by a source column in WHERE.
    let selection = query
        .selection
        .as_ref()
        .map(|expression| lower_expr_in_scope(&scope, expression, Some(&bool_type), parameters))
        .transpose()?;
    if let Some(predicate) = &selection {
        if predicate.expr_type.data_type != bool_type {
            return Err(HirError::TypeMismatch {
                expected: bool_type.clone(),
                actual: predicate.expr_type.data_type.clone(),
                span: predicate.span,
            });
        }
    }

    let projection = query
        .projection
        .iter()
        .try_fold(Vec::new(), |mut projection, item| {
            match item {
                netbadb_parser::SelectItem::Wildcard(_) => {
                    if !group_by.is_empty() {
                        return Err(HirError::WildcardNotSupportedWithGroupBy {
                            span: select_item_span(item),
                        });
                    }
                    projection.extend(scope.columns().into_iter().map(TypedProjectionItem::Column))
                }
                netbadb_parser::SelectItem::Column(column) => {
                    let resolved = scope.resolve(column)?;
                    if is_grouping
                        && !group_by
                            .iter()
                            .any(|group_key| same_source_column(group_key, &resolved))
                    {
                        return Err(HirError::UngroupedColumn {
                            name: column.name.name.clone(),
                            span: column.span,
                        });
                    }
                    projection.push(TypedProjectionItem::Column(resolved));
                }
                netbadb_parser::SelectItem::Aggregate(aggregate) => projection.push(
                    TypedProjectionItem::Aggregate(lower_aggregate(&scope, aggregate)?),
                ),
                netbadb_parser::SelectItem::Expression {
                    expression, alias, ..
                } => {
                    if is_grouping {
                        return Err(HirError::InvalidAggregateArgument {
                            function: AggregateFunction::Count,
                            span: ast_expr_span(expression),
                        });
                    }
                    // PostgreSQL and the native frontend both allow a standalone
                    // `NULL` projection. With no surrounding expression there is
                    // no contextual carrier type, so use TEXT as the deterministic
                    // wire-visible carrier while preserving the value as SQL NULL.
                    let null_carrier = matches!(
                        expression,
                        netbadb_parser::Expr::Literal {
                            value: netbadb_parser::Literal::Null,
                            ..
                        }
                    )
                    .then(|| SemanticType::physical(PhysicalType::Text));
                    let expression =
                        lower_expr_in_scope(&scope, expression, null_carrier.as_ref(), parameters)?;
                    let output_name = alias.as_ref().map_or_else(
                        || expression_output_name(&expression),
                        |alias| alias.name.clone(),
                    );
                    projection.push(TypedProjectionItem::Expression {
                        expression,
                        output_name,
                    });
                }
            }
            Ok::<_, HirError>(projection)
        })?;

    let order_by = query
        .order_by
        .iter()
        .map(|item| {
            let direction = match item.direction.unwrap_or(AstSortDirection::Asc) {
                AstSortDirection::Asc => SortDirection::Asc,
                AstSortDirection::Desc => SortDirection::Desc,
            };
            let null_order = match item.null_order {
                Some(AstNullOrder::First) => NullOrder::First,
                Some(AstNullOrder::Last) => NullOrder::Last,
                None if direction == SortDirection::Asc => NullOrder::Last,
                None => NullOrder::First,
            };
            Ok(TypedOrderKey {
                column: scope.resolve(&item.column)?,
                direction,
                null_order,
                span: item.span,
            })
        })
        .collect::<Result<Vec<_>, HirError>>()?;

    Ok(TypedQuery {
        from,
        joins,
        columns: scope.columns(),
        projection,
        selection,
        group_by,
        order_by,
        limit: query.limit,
    })
}

fn same_source_column(left: &ColumnRef, right: &ColumnRef) -> bool {
    left.binding_id == right.binding_id && left.column_id == right.column_id
}

fn lower_aggregate(
    scope: &RelationScope<'_>,
    aggregate: &AstAggregateCall,
) -> Result<TypedAggregate, HirError> {
    let function = match aggregate.function {
        AstAggregateFunction::Count => AggregateFunction::Count,
        AstAggregateFunction::Sum => AggregateFunction::Sum,
        AstAggregateFunction::Min => AggregateFunction::Min,
        AstAggregateFunction::Max => AggregateFunction::Max,
    };
    let input = match &aggregate.argument {
        AstAggregateArgument::Star(_) if function == AggregateFunction::Count => {
            TypedAggregateInput::All
        }
        AstAggregateArgument::Star(_) => {
            return Err(HirError::InvalidAggregateArgument {
                function,
                span: aggregate.span,
            });
        }
        AstAggregateArgument::Column(column) => TypedAggregateInput::Column(scope.resolve(column)?),
    };
    let expr_type = match function {
        AggregateFunction::Count => ExprType {
            data_type: SemanticType::physical(PhysicalType::UInt64),
            nullable: false,
        },
        AggregateFunction::Sum => {
            let TypedAggregateInput::Column(column) = &input else {
                return Err(HirError::InvalidAggregateArgument {
                    function,
                    span: aggregate.span,
                });
            };
            if column.data_type.physical.is_numeric() {
                ExprType {
                    data_type: SemanticType::physical(column.data_type.physical),
                    nullable: true,
                }
            } else {
                return Err(HirError::InvalidAggregateType {
                    function,
                    actual: column.data_type.clone(),
                    span: aggregate.span,
                });
            }
        }
        AggregateFunction::Min | AggregateFunction::Max => {
            let TypedAggregateInput::Column(column) = &input else {
                return Err(HirError::InvalidAggregateArgument {
                    function,
                    span: aggregate.span,
                });
            };
            ExprType {
                data_type: column.data_type.clone(),
                nullable: true,
            }
        }
    };
    Ok(TypedAggregate {
        function,
        input,
        expr_type,
        output_name: aggregate_output_name(aggregate),
        span: aggregate.span,
    })
}

fn aggregate_output_name(aggregate: &AstAggregateCall) -> String {
    let argument = match &aggregate.argument {
        AstAggregateArgument::Star(_) => "*".to_owned(),
        AstAggregateArgument::Column(column) => match &column.qualifier {
            Some(qualifier) => format!("{}.{}", qualifier.name, column.name.name),
            None => column.name.name.clone(),
        },
    };
    format!(
        "{}({argument})",
        match aggregate.function {
            AstAggregateFunction::Count => "COUNT",
            AstAggregateFunction::Sum => "SUM",
            AstAggregateFunction::Min => "MIN",
            AstAggregateFunction::Max => "MAX",
        }
    )
}

fn select_item_span(item: &netbadb_parser::SelectItem) -> Span {
    match item {
        netbadb_parser::SelectItem::Wildcard(span) => *span,
        netbadb_parser::SelectItem::Column(column) => column.span,
        netbadb_parser::SelectItem::Aggregate(aggregate) => aggregate.span,
        netbadb_parser::SelectItem::Expression { span, .. } => *span,
    }
}

fn expression_output_name(expression: &TypedExpr) -> String {
    match &expression.kind {
        TypedExprKind::Parameter(id) => format!("${}", id.0 + 1),
        _ => "?column?".into(),
    }
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
    parameters: &mut ParameterContext,
) -> Result<TypedInsert, HirError> {
    let table = resolve_table(schema, &insert.table)?;
    let implicit_columns;
    let targets = if insert.columns.is_empty() {
        implicit_columns = table
            .columns
            .iter()
            .map(|c| Ident {
                name: c.name.clone(),
                span: insert.table.span,
            })
            .collect::<Vec<_>>();
        &implicit_columns
    } else {
        &insert.columns
    };
    if targets.len() != insert.values.len() {
        return Err(HirError::ValueCountMismatch {
            columns: targets.len(),
            values: insert.values.len(),
            span: insert.span,
        });
    }
    let mut seen = HashSet::new();
    let mut values = vec![None; table.columns.len()];
    for (target, value) in targets.iter().zip(&insert.values) {
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
        values[position] = Some(lower_value_for_column(table, value, &column, parameters)?);
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
    parameters: &mut ParameterContext,
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
            let value = lower_value_for_column(table, &assignment.value, &column, parameters)?;
            Ok(TypedAssignment { column, value })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let selection = lower_selection(table, update.selection.as_ref(), parameters)?;
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
    parameters: &mut ParameterContext,
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
        selection: lower_selection(table, delete.selection.as_ref(), parameters)?,
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
    parameters: &mut ParameterContext,
) -> Result<Option<TypedExpr>, HirError> {
    let bool_type = SemanticType::physical(PhysicalType::Bool);
    let selection = selection
        .map(|expression| lower_expr(table, expression, Some(&bool_type), parameters))
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
    parameters: &mut ParameterContext,
) -> Result<TypedExpr, HirError> {
    let mut value = lower_expr(table, expression, Some(&column.data_type), parameters)?;
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
    if !column.nullable && value.expr_type.nullable && !is_typed_parameter_expression(&value) {
        return Err(HirError::NullNotAllowed {
            name: column.name.clone(),
            span: value.span,
        });
    }
    Ok(value)
}

fn is_typed_parameter_expression(expression: &TypedExpr) -> bool {
    match &expression.kind {
        TypedExprKind::Parameter(_) => true,
        TypedExprKind::Cast { expression } => is_typed_parameter_expression(expression),
        _ => false,
    }
}

fn expression_references_column(expression: &AstExpr) -> bool {
    match expression {
        AstExpr::Column(_) => true,
        AstExpr::Literal { .. } | AstExpr::Parameter { .. } => false,
        AstExpr::Cast { expression, .. } => expression_references_column(expression),
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
        | AstExpr::Parameter { span, .. }
        | AstExpr::Cast { span, .. }
        | AstExpr::Binary { span, .. }
        | AstExpr::Unary { span, .. }
        | AstExpr::IsNull { span, .. } => *span,
    }
}

fn lower_expr(
    table: &TableDef,
    expression: &AstExpr,
    expected: Option<&SemanticType>,
    parameters: &mut ParameterContext,
) -> Result<TypedExpr, HirError> {
    lower_expr_in_scope(
        &RelationScope::single(table),
        expression,
        expected,
        parameters,
    )
}

fn lower_expr_in_scope(
    scope: &RelationScope<'_>,
    expression: &AstExpr,
    expected: Option<&SemanticType>,
    parameters: &mut ParameterContext,
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
            let (value, data_type, nullable) = lower_literal(value, expected, *span)?;
            Ok(TypedExpr {
                kind: TypedExprKind::Literal(value),
                expr_type: ExprType {
                    data_type,
                    nullable,
                },
                span: *span,
            })
        }
        AstExpr::Parameter { id, span } => {
            let data_type = parameters.resolve(*id, expected, *span)?;
            Ok(TypedExpr {
                kind: TypedExprKind::Parameter(*id),
                expr_type: ExprType {
                    data_type,
                    nullable: true,
                },
                span: *span,
            })
        }
        AstExpr::Cast {
            expression,
            data_type,
            span,
        } => {
            let physical = match data_type {
                AstSqlTypeName::Bool => PhysicalType::Bool,
                AstSqlTypeName::Int8 => PhysicalType::Int8,
                AstSqlTypeName::Int16 => PhysicalType::Int16,
                AstSqlTypeName::Int32 => PhysicalType::Int32,
                AstSqlTypeName::Int64 => PhysicalType::Int64,
                AstSqlTypeName::Int128 => PhysicalType::Int128,
                AstSqlTypeName::UInt8 => PhysicalType::UInt8,
                AstSqlTypeName::UInt16 => PhysicalType::UInt16,
                AstSqlTypeName::UInt32 => PhysicalType::UInt32,
                AstSqlTypeName::UInt64 => PhysicalType::UInt64,
                AstSqlTypeName::UInt128 => PhysicalType::UInt128,
                AstSqlTypeName::Float32 => PhysicalType::Float32,
                AstSqlTypeName::Float64 => PhysicalType::Float64,
                AstSqlTypeName::Text => PhysicalType::Text,
                AstSqlTypeName::Bytes => PhysicalType::Bytes,
            };
            // A same-physical cast may adopt an already-resolved nominal
            // context. Cross-physical admissibility is determined solely by
            // the resolved physical source and target pair.
            let target = expected
                .filter(|expected| expected.physical == physical)
                .cloned()
                .unwrap_or_else(|| SemanticType::physical(physical));
            let source_context = match expression.as_ref() {
                AstExpr::Literal {
                    value: Literal::Null,
                    ..
                } => Some(&target),
                AstExpr::Parameter { id, .. } if !parameters.has_source_type(*id) => Some(&target),
                _ => None,
            };
            let expression = lower_expr_in_scope(scope, expression, source_context, parameters)?;
            let source = expression.expr_type.data_type.physical;
            if !source.supports_explicit_cast_to(physical) {
                return Err(HirError::UnsupportedCast {
                    source,
                    target: physical,
                    span: *span,
                });
            }
            if source == physical {
                require_type(&expression, &target)?;
            }
            Ok(TypedExpr {
                expr_type: ExprType {
                    data_type: target,
                    nullable: expression.expr_type.nullable,
                },
                kind: TypedExprKind::Cast {
                    expression: Box::new(expression),
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
                    let left = lower_expr_in_scope(scope, left, Some(&bool_type), parameters)?;
                    let right = lower_expr_in_scope(scope, right, Some(&bool_type), parameters)?;
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
                    let (left, right) = lower_comparison_operands(scope, left, right, parameters)?;
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
            let expression = lower_expr_in_scope(scope, expression, Some(&bool_type), parameters)?;
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
                lower_expr_in_scope(scope, expression, Some(&bool_type), parameters)?
            } else {
                lower_expr_in_scope(scope, expression, None, parameters)?
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

fn lower_literal(
    literal: &Literal,
    expected: Option<&SemanticType>,
    span: Span,
) -> Result<(ScalarValue, SemanticType, bool), HirError> {
    // Context chooses a literal's physical width, but it must never mint a
    // nominal application identity such as UserId from raw SQL syntax.
    let typed = SemanticType::physical;
    match literal {
        Literal::Bool(value) => Ok((ScalarValue::Bool(*value), typed(PhysicalType::Bool), false)),
        Literal::Int(value) => {
            let (value, physical) = parse_integer_literal(&value.to_string(), expected, span)?;
            Ok((value, typed(physical), false))
        }
        Literal::LargeInteger(value) => {
            let (value, physical) = parse_integer_literal(value, expected, span)?;
            Ok((value, typed(physical), false))
        }
        Literal::Float(value) => {
            let parsed = value.parse::<f64>().map_err(|_| HirError::InvalidLiteral {
                message: "invalid floating-point literal",
                span,
            })?;
            if !parsed.is_finite() {
                return Err(HirError::InvalidLiteral {
                    message: "FLOAT64 literal is out of range",
                    span,
                });
            }
            if expected.is_some_and(|expected| expected.physical == PhysicalType::Float32) {
                let narrowed = parsed as f32;
                if parsed.is_finite() && !narrowed.is_finite() {
                    return Err(HirError::InvalidLiteral {
                        message: "FLOAT32 literal is out of range",
                        span,
                    });
                }
                Ok((
                    ScalarValue::Float32(Float32Value::new(narrowed)),
                    typed(PhysicalType::Float32),
                    false,
                ))
            } else {
                Ok((
                    ScalarValue::Float64(Float64Value::new(parsed)),
                    typed(PhysicalType::Float64),
                    false,
                ))
            }
        }
        Literal::String(value) => Ok((
            ScalarValue::Text(value.clone()),
            typed(PhysicalType::Text),
            false,
        )),
        Literal::Bytes(value) => Ok((
            ScalarValue::Bytes(value.clone()),
            typed(PhysicalType::Bytes),
            false,
        )),
        Literal::Null => Ok((
            ScalarValue::Null,
            expected
                .cloned()
                .ok_or(HirError::CannotInferNullType { span })?,
            true,
        )),
    }
}

fn parse_integer_literal(
    value: &str,
    expected: Option<&SemanticType>,
    span: Span,
) -> Result<(ScalarValue, PhysicalType), HirError> {
    macro_rules! signed {
        ($kind:ident, $type:ty) => {
            value
                .parse::<$type>()
                .map(|value| (ScalarValue::$kind(value), PhysicalType::$kind))
                .map_err(|_| HirError::InvalidLiteral {
                    message: concat!(stringify!($kind), " literal is out of range"),
                    span,
                })
        };
    }
    macro_rules! unsigned {
        ($kind:ident, $type:ty) => {
            value
                .parse::<$type>()
                .map(|value| (ScalarValue::$kind(value), PhysicalType::$kind))
                .map_err(|_| HirError::InvalidLiteral {
                    message: concat!(stringify!($kind), " literal is out of range"),
                    span,
                })
        };
    }
    match expected.map(|expected| expected.physical) {
        Some(PhysicalType::Int8) => signed!(Int8, i8),
        Some(PhysicalType::Int16) => signed!(Int16, i16),
        Some(PhysicalType::Int32) => signed!(Int32, i32),
        Some(PhysicalType::Int64) => signed!(Int64, i64),
        Some(PhysicalType::Int128) => signed!(Int128, i128),
        Some(PhysicalType::UInt8) => unsigned!(UInt8, u8),
        Some(PhysicalType::UInt16) => unsigned!(UInt16, u16),
        Some(PhysicalType::UInt32) => unsigned!(UInt32, u32),
        Some(PhysicalType::UInt64) => unsigned!(UInt64, u64),
        Some(PhysicalType::UInt128) => unsigned!(UInt128, u128),
        _ => {
            if let Ok(value) = value.parse::<i64>() {
                Ok((ScalarValue::Int64(value), PhysicalType::Int64))
            } else if let Ok(value) = value.parse::<i128>() {
                Ok((ScalarValue::Int128(value), PhysicalType::Int128))
            } else if let Ok(value) = value.parse::<u128>() {
                Ok((ScalarValue::UInt128(value), PhysicalType::UInt128))
            } else {
                Err(HirError::InvalidLiteral {
                    message: "integer literal is out of UInt128 range",
                    span,
                })
            }
        }
    }
}

fn lower_comparison_operands(
    scope: &RelationScope<'_>,
    left: &AstExpr,
    right: &AstExpr,
    parameters: &mut ParameterContext,
) -> Result<(TypedExpr, TypedExpr), HirError> {
    if is_literal_expression(left) && !is_literal_expression(right) {
        let right = lower_expr_in_scope(scope, right, None, parameters)?;
        let left = lower_expr_in_scope(scope, left, Some(&right.expr_type.data_type), parameters)?;
        return Ok((left, right));
    }
    if !is_literal_expression(left) && is_literal_expression(right) {
        let left = lower_expr_in_scope(scope, left, None, parameters)?;
        let right = lower_expr_in_scope(scope, right, Some(&left.expr_type.data_type), parameters)?;
        return Ok((left, right));
    }
    if is_parameter_expression(left) && !is_parameter_expression(right) {
        let right = lower_expr_in_scope(scope, right, None, parameters)?;
        let left = lower_expr_in_scope(scope, left, Some(&right.expr_type.data_type), parameters)?;
        return Ok((left, right));
    }
    if !is_parameter_expression(left) && is_parameter_expression(right) {
        let left = lower_expr_in_scope(scope, left, None, parameters)?;
        let right = lower_expr_in_scope(scope, right, Some(&left.expr_type.data_type), parameters)?;
        return Ok((left, right));
    }
    match (is_null_literal(left), is_null_literal(right)) {
        (true, true) => {
            // With no operand context, BOOL is a deterministic carrier type;
            // both runtime values remain NULL and comparison yields UNKNOWN.
            let carrier = SemanticType::physical(PhysicalType::Bool);
            Ok((
                lower_expr_in_scope(scope, left, Some(&carrier), parameters)?,
                lower_expr_in_scope(scope, right, Some(&carrier), parameters)?,
            ))
        }
        (true, false) => {
            let right = lower_expr_in_scope(scope, right, None, parameters)?;
            let left =
                lower_expr_in_scope(scope, left, Some(&right.expr_type.data_type), parameters)?;
            Ok((left, right))
        }
        (false, true) => {
            let left = lower_expr_in_scope(scope, left, None, parameters)?;
            let right =
                lower_expr_in_scope(scope, right, Some(&left.expr_type.data_type), parameters)?;
            Ok((left, right))
        }
        (false, false) => Ok((
            lower_expr_in_scope(scope, left, None, parameters)?,
            lower_expr_in_scope(scope, right, None, parameters)?,
        )),
    }
}

fn is_literal_expression(expression: &AstExpr) -> bool {
    matches!(expression, AstExpr::Literal { .. })
}

fn is_parameter_expression(expression: &AstExpr) -> bool {
    match expression {
        AstExpr::Parameter { .. } => true,
        AstExpr::Cast { expression, .. } => is_parameter_expression(expression),
        _ => false,
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
    use super::{
        AggregateFunction, HirError, NullOrder, SortDirection, TypedAggregateInput, TypedExprKind,
        TypedProjectionItem, TypedStatement, UnaryOp, lower_query, lower_statement,
    };
    use netbadb_parser::{parse, parse_statement};
    use netbadb_schema::{ColumnDef, Schema, TableDef, TypeSpec};
    use netbadb_types::{ColumnId, PhysicalType, RelationBindingId, ScalarValue, TableId};

    fn only_literal(query: &super::TypedQuery) -> &ScalarValue {
        let TypedProjectionItem::Expression { expression, .. } = &query.projection[0] else {
            panic!("expected expression projection");
        };
        let TypedExprKind::Cast { expression } = &expression.kind else {
            panic!("expected typed cast");
        };
        let TypedExprKind::Literal(value) = &expression.kind else {
            panic!("expected literal below cast");
        };
        value
    }

    #[test]
    fn explicit_cast_literals_preserve_uncontextualized_source_width() {
        let empty = Schema::new(vec![]).expect("empty schema");
        let cases = [
            ("SELECT -128::TINYINT", ScalarValue::Int64(-128)),
            ("SELECT 255::UINT8", ScalarValue::Int64(255)),
            (
                "SELECT 170141183460469231731687303715884105727::INT128",
                ScalarValue::Int128(i128::MAX),
            ),
            (
                "SELECT 340282366920938463463374607431768211455::UINT128",
                ScalarValue::UInt128(u128::MAX),
            ),
        ];
        for (sql, expected) in cases {
            let typed =
                lower_query(&empty, &parse(sql).expect("parse boundary")).expect("lower boundary");
            assert_eq!(only_literal(&typed), &expected, "{sql}");
        }
        for sql in [
            "SELECT 340282366920938463463374607431768211456::UINT128",
            "SELECT 1e400::FLOAT64",
        ] {
            assert!(
                matches!(
                    lower_query(&empty, &parse(sql).expect("parse overflow")),
                    Err(HirError::InvalidLiteral { .. })
                ),
                "{sql}"
            );
        }
    }

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
                    ColumnDef::new(
                        ColumnId(6),
                        "score",
                        TypeSpec::Physical(PhysicalType::Int64),
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
        .expect("valid test schema")
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
        assert_eq!(
            typed.from.as_ref().expect("FROM").binding_id,
            RelationBindingId(0)
        );
        assert_eq!(typed.joins[0].right.binding_id, RelationBindingId(1));
        assert_eq!(
            typed.projection[0]
                .source_column()
                .expect("source projection")
                .relation_name,
            "u"
        );
        assert!(typed.joins[0].predicate.expr_type.nullable);
    }

    #[test]
    fn resolves_typed_order_keys_with_explicit_null_defaults() {
        let typed = lower_query(
            &schema(),
            &parse(
                "SELECT u.name FROM users u JOIN teams t ON u.team_id = t.id \
                 ORDER BY t.name, u.nickname DESC, u.id ASC NULLS FIRST",
            )
            .expect("parse"),
        )
        .expect("lower ORDER BY");
        assert_eq!(typed.projection.len(), 1);
        assert_eq!(typed.order_by.len(), 3);
        assert_eq!(typed.order_by[0].column.relation_name, "t");
        assert_eq!(typed.order_by[0].direction, SortDirection::Asc);
        assert_eq!(typed.order_by[0].null_order, NullOrder::Last);
        assert_eq!(typed.order_by[1].direction, SortDirection::Desc);
        assert_eq!(typed.order_by[1].null_order, NullOrder::First);
        assert!(typed.order_by[1].column.nullable);
        assert_eq!(typed.order_by[2].null_order, NullOrder::First);
        assert_eq!(
            typed.order_by[2].column.data_type.name.as_deref(),
            Some("UserId")
        );
    }

    #[test]
    fn types_global_aggregates_and_preserves_only_value_semantics() {
        let typed = lower_query(
            &schema(),
            &parse("SELECT COUNT(*), COUNT(score), SUM(id), MIN(id), MAX(u.score) FROM users u")
                .expect("parse aggregates"),
        )
        .expect("lower aggregates");
        let aggregates = typed
            .projection
            .iter()
            .map(|item| match item {
                TypedProjectionItem::Aggregate(aggregate) => aggregate,
                TypedProjectionItem::Column(_) | TypedProjectionItem::Expression { .. } => {
                    panic!("expected only aggregates")
                }
            })
            .collect::<Vec<_>>();
        assert!(matches!(aggregates[0].input, TypedAggregateInput::All));
        assert_eq!(aggregates[0].function, AggregateFunction::Count);
        assert_eq!(aggregates[0].output_name, "COUNT(*)");
        assert_eq!(
            aggregates[0].expr_type.data_type.physical,
            PhysicalType::UInt64
        );
        assert!(!aggregates[0].expr_type.nullable);
        assert_eq!(aggregates[1].output_name, "COUNT(score)");
        assert!(!aggregates[1].expr_type.nullable);
        assert_eq!(aggregates[2].output_name, "SUM(id)");
        assert_eq!(
            aggregates[2].expr_type.data_type.physical,
            PhysicalType::UInt64
        );
        assert_eq!(aggregates[2].expr_type.data_type.name, None);
        assert!(aggregates[2].expr_type.nullable);
        assert_eq!(
            aggregates[3].expr_type.data_type.name.as_deref(),
            Some("UserId")
        );
        assert!(aggregates[3].expr_type.nullable);
        assert_eq!(aggregates[4].output_name, "MAX(u.score)");
    }

    #[test]
    fn aggregate_resolution_and_global_restrictions_are_typed_errors() {
        let qualified = lower_query(
            &schema(),
            &parse("SELECT COUNT(u.id), MIN(t.name) FROM users u JOIN teams t ON u.team_id = t.id")
                .expect("parse qualified aggregates"),
        )
        .expect("qualified aggregates lower");
        assert_eq!(qualified.projection.len(), 2);

        for source in [
            "SELECT SUM(name) FROM users",
            "SELECT SUM(active) FROM users",
        ] {
            assert!(matches!(
                lower_query(&schema(), &parse(source).expect("parse invalid SUM")),
                Err(HirError::InvalidAggregateType {
                    function: AggregateFunction::Sum,
                    ..
                })
            ));
        }
        for source in [
            "SELECT id, COUNT(*) FROM users",
            "SELECT COUNT(*), name FROM users",
        ] {
            assert!(matches!(
                lower_query(&schema(), &parse(source).expect("parse mixed projection")),
                Err(HirError::UngroupedColumn { .. })
            ));
        }
        assert!(matches!(
            lower_query(
                &schema(),
                &parse("SELECT COUNT(*) FROM users ORDER BY id").expect("parse aggregate order")
            ),
            Err(HirError::OrderByNotSupportedWithGrouping { .. })
        ));
        assert!(matches!(
            lower_query(
                &schema(),
                &parse(
                    "SELECT COUNT(id) FROM users u JOIN teams t ON u.team_id = t.id"
                )
                .expect("parse ambiguous aggregate")
            ),
            Err(HirError::AmbiguousColumn { name, .. }) if name == "id"
        ));
        assert!(matches!(
            lower_query(
                &schema(),
                &parse("SELECT COUNT(x.id) FROM users u").expect("parse unknown qualifier")
            ),
            Err(HirError::UnknownRelationQualifier { name, .. }) if name == "x"
        ));
        assert!(matches!(
            lower_query(
                &schema(),
                &parse("SELECT MAX(u.missing) FROM users u").expect("parse unknown column")
            ),
            Err(HirError::UnknownColumn { name, .. }) if name == "missing"
        ));
    }

    #[test]
    fn resolves_group_keys_and_enforces_grouped_projection_semantics() {
        let typed = lower_query(
            &schema(),
            &parse(
                "SELECT COUNT(*), u.team_id, MAX(score) FROM users u \
                 GROUP BY u.team_id, active",
            )
            .expect("parse grouped query"),
        )
        .expect("lower grouped query");
        assert_eq!(typed.group_by.len(), 2);
        assert_eq!(typed.group_by[0].name, "team_id");
        assert_eq!(typed.group_by[0].relation_name, "u");
        assert_eq!(typed.projection.len(), 3);

        for source in [
            "SELECT id, COUNT(*) FROM users GROUP BY team_id",
            "SELECT team_id, id FROM users GROUP BY team_id",
            "SELECT id FROM users GROUP BY team_id",
            "SELECT COUNT(*), name FROM users",
        ] {
            let error = lower_query(&schema(), &parse(source).expect("parse ungrouped column"))
                .expect_err("ungrouped source column must fail");
            assert!(matches!(
                error,
                HirError::UngroupedColumn { name, .. } if name == "id" || name == "name"
            ));
        }

        let source = "SELECT COUNT(*), name FROM users";
        let error = lower_query(&schema(), &parse(source).expect("parse span case"))
            .expect_err("ungrouped name must fail");
        let HirError::UngroupedColumn { name, span } = error else {
            panic!("expected ungrouped column");
        };
        assert_eq!(name, "name");
        assert_eq!(&source[span.start..span.end], "name");

        for source in [
            "SELECT team_id FROM users GROUP BY team_id",
            "SELECT COUNT(*) FROM users GROUP BY team_id",
            "SELECT team_id, COUNT(*) FROM users GROUP BY team_id, active",
        ] {
            assert!(
                lower_query(&schema(), &parse(source).expect("parse valid grouping")).is_ok(),
                "{source}"
            );
        }

        assert!(matches!(
            lower_query(
                &schema(),
                &parse("SELECT * FROM users GROUP BY team_id").expect("parse wildcard grouping")
            ),
            Err(HirError::WildcardNotSupportedWithGroupBy { .. })
        ));
        assert!(matches!(
            lower_query(
                &schema(),
                &parse("SELECT team_id FROM users GROUP BY team_id ORDER BY team_id")
                    .expect("parse grouped order")
            ),
            Err(HirError::OrderByNotSupportedWithGrouping { .. })
        ));
    }

    #[test]
    fn group_by_reuses_binding_aware_scope_resolution() {
        let qualified = lower_query(
            &schema(),
            &parse(
                "SELECT u.id, COUNT(*) FROM users u JOIN teams t ON u.team_id = t.id \
                 GROUP BY u.id",
            )
            .expect("parse qualified group"),
        )
        .expect("qualified group resolves");
        assert_eq!(qualified.group_by[0].binding_id, RelationBindingId(0));

        assert!(matches!(
            lower_query(
                &schema(),
                &parse(
                    "SELECT COUNT(*) FROM users u JOIN teams t ON u.team_id = t.id GROUP BY id"
                )
                .expect("parse ambiguous group")
            ),
            Err(HirError::AmbiguousColumn { name, .. }) if name == "id"
        ));
        assert!(matches!(
            lower_query(
                &schema(),
                &parse("SELECT COUNT(*) FROM users u GROUP BY x.id")
                    .expect("parse unknown qualifier")
            ),
            Err(HirError::UnknownRelationQualifier { name, .. }) if name == "x"
        ));

        let employee_group = lower_query(
            &schema(),
            &parse(
                "SELECT e.id, COUNT(*) FROM employees e JOIN employees m \
                 ON e.manager_id = m.id GROUP BY e.id",
            )
            .expect("parse employee self-join group"),
        )
        .expect("employee self-join group resolves");
        let manager_group = lower_query(
            &schema(),
            &parse(
                "SELECT m.id, COUNT(*) FROM employees e JOIN employees m \
                 ON e.manager_id = m.id GROUP BY m.id",
            )
            .expect("parse manager self-join group"),
        )
        .expect("manager self-join group resolves");
        assert_ne!(
            employee_group.group_by[0].binding_id,
            manager_group.group_by[0].binding_id
        );
    }

    #[test]
    fn order_by_reuses_scope_ambiguity_and_qualifier_errors() {
        let ambiguous =
            parse("SELECT u.name FROM users u JOIN teams t ON u.team_id = t.id ORDER BY id")
                .expect("parse ambiguous key");
        assert!(matches!(
            lower_query(&schema(), &ambiguous),
            Err(HirError::AmbiguousColumn { name, .. }) if name == "id"
        ));

        let unknown = parse("SELECT name FROM users u ORDER BY x.id").expect("parse qualifier");
        assert!(matches!(
            lower_query(&schema(), &unknown),
            Err(HirError::UnknownRelationQualifier { name, .. }) if name == "x"
        ));

        let unknown_column =
            parse("SELECT name FROM users ORDER BY missing").expect("parse unknown column");
        assert!(matches!(
            lower_query(&schema(), &unknown_column),
            Err(HirError::UnknownColumn { name, .. }) if name == "missing"
        ));
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
                 ON e.manager_id = m.id ORDER BY m.name, e.id DESC",
            )
            .expect("parse"),
        )
        .expect("self join lowers");
        let employee = typed.projection[0]
            .source_column()
            .expect("employee projection");
        let manager = typed.projection[1]
            .source_column()
            .expect("manager projection");
        assert_eq!(employee.table_id, manager.table_id);
        assert_ne!(employee.binding_id, manager.binding_id);
        assert_eq!(typed.order_by[0].column.binding_id, manager.binding_id);
        assert_eq!(typed.order_by[1].column.binding_id, employee.binding_id);

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

#[cfg(test)]
mod alter_table_tests;
#[cfg(test)]
mod create_table_tests;
#[cfg(test)]
mod drop_table_tests;
