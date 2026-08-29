//! Query compilation from source text to typed HIR and logical relational IR.

use std::error::Error;
use std::fmt;

use netbadb_hir::{
    AggregateFunction as HirAggregateFunction, ColumnRef as HirColumnRef, HirError,
    NullOrder as HirNullOrder, ParameterMetadata, SortDirection as HirSortDirection,
    TypedAggregate, TypedAggregateInput, TypedCreateIndex, TypedExpr, TypedExprKind,
    TypedProjectionItem, TypedQuery, TypedRelation, TypedStatement,
};
use netbadb_parser::{ParseError, parse, parse_statement};
use netbadb_rel::{
    AggregateExpr, AggregateFunction, AggregateInput, AggregateOutput, Assignment, BinaryOp,
    ColumnRef, DerivedField, Expr, ExprKind, JoinKind, LogicalPlan, LogicalStatement, NullOrder,
    ProjectedExpr, SortDirection, SortKey, UnaryOp,
};
use netbadb_schema::Schema;
use netbadb_types::{ParameterId, PhysicalType, ScalarValue, SemanticType};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledQuery {
    pub hir: TypedQuery,
    pub logical_plan: LogicalPlan,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledStatement {
    pub hir: TypedStatement,
    pub logical_statement: LogicalStatement,
    pub parameters: Vec<PreparedParameter>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedParameter {
    pub id: ParameterId,
    pub data_type: SemanticType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompiledDdlStatement {
    CreateIndex(TypedCreateIndex),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BindError {
    ParameterCount {
        expected: usize,
        actual: usize,
    },
    ParameterType {
        id: ParameterId,
        expected: SemanticType,
        actual: Option<PhysicalType>,
    },
}

impl fmt::Display for BindError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ParameterCount { expected, actual } => {
                write!(formatter, "expected {expected} parameters, found {actual}")
            }
            Self::ParameterType {
                id,
                expected,
                actual,
            } => write!(
                formatter,
                "parameter ${} expects {expected}, found {}",
                id.0 + 1,
                actual.map_or_else(|| "NULL".into(), |actual| actual.to_string())
            ),
        }
    }
}

impl Error for BindError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompileError {
    Parse(ParseError),
    Hir(HirError),
}

/// Frontend-neutral semantic category for a failed compilation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompileErrorKind {
    Syntax,
    UndefinedTable,
    UndefinedColumn,
    AmbiguousColumn,
    DatatypeMismatch,
    IndeterminateDatatype,
    NotNullViolation,
    FeatureNotSupported,
}

impl CompileError {
    #[must_use]
    pub const fn kind(&self) -> CompileErrorKind {
        match self {
            Self::Parse(_) => CompileErrorKind::Syntax,
            Self::Hir(error) => match error {
                HirError::UnknownTable { .. } => CompileErrorKind::UndefinedTable,
                HirError::UnknownColumn { .. } | HirError::UnknownRelationQualifier { .. } => {
                    CompileErrorKind::UndefinedColumn
                }
                HirError::AmbiguousColumn { .. } | HirError::DuplicateRelationName { .. } => {
                    CompileErrorKind::AmbiguousColumn
                }
                HirError::NullNotAllowed { .. } | HirError::MissingRequiredColumn { .. } => {
                    CompileErrorKind::NotNullViolation
                }
                HirError::WildcardNotSupportedWithGroupBy { .. }
                | HirError::OrderByNotSupportedWithGrouping { .. } => {
                    CompileErrorKind::FeatureNotSupported
                }
                HirError::InvalidIndexDefinition { .. } => CompileErrorKind::FeatureNotSupported,
                HirError::TooManyRelations { .. }
                | HirError::TypeMismatch { .. }
                | HirError::IncompatibleComparison { .. }
                | HirError::CannotInferNullType { .. }
                | HirError::ParameterTypeConflict { .. }
                | HirError::DuplicateColumn { .. }
                | HirError::ValueCountMismatch { .. }
                | HirError::InsertValueReferencesColumn { .. }
                | HirError::UngroupedColumn { .. }
                | HirError::InvalidAggregateArgument { .. }
                | HirError::InvalidAggregateType { .. } => CompileErrorKind::DatatypeMismatch,
                HirError::CannotInferParameterType { .. } => {
                    CompileErrorKind::IndeterminateDatatype
                }
            },
        }
    }

    #[must_use]
    pub const fn span(&self) -> netbadb_parser::Span {
        match self {
            Self::Parse(error) => error.span,
            Self::Hir(error) => error.span(),
        }
    }
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
    compile_statement_with_parameters(schema, source, &[])
}

pub fn compile_statement_with_parameters(
    schema: &Schema,
    source: &str,
    declared: &[Option<PhysicalType>],
) -> Result<CompiledStatement, CompileError> {
    let ast = parse_statement(source)?;
    let (hir, parameters) = netbadb_hir::lower_statement_with_parameters(schema, &ast, declared)?;
    let logical_statement = lower_statement(&hir);
    Ok(CompiledStatement {
        hir,
        logical_statement,
        parameters: parameters.into_iter().map(prepared_parameter).collect(),
    })
}

pub fn compile_ddl_statement(
    schema: &Schema,
    source: &str,
) -> Result<CompiledDdlStatement, CompileError> {
    match parse_statement(source)? {
        netbadb_parser::Statement::CreateIndex(statement) => {
            netbadb_hir::lower_create_index(schema, &statement)
                .map(CompiledDdlStatement::CreateIndex)
                .map_err(CompileError::from)
        }
        statement => Err(CompileError::Hir(HirError::InvalidIndexDefinition {
            message: "statement is not generic DDL",
            span: match statement {
                netbadb_parser::Statement::Select(value) => value.span,
                netbadb_parser::Statement::Insert(value) => value.span,
                netbadb_parser::Statement::Update(value) => value.span,
                netbadb_parser::Statement::Delete(value) => value.span,
                netbadb_parser::Statement::CreateIndex(value) => value.span,
            },
        })),
    }
}

fn prepared_parameter(parameter: ParameterMetadata) -> PreparedParameter {
    PreparedParameter {
        id: parameter.id,
        data_type: parameter.data_type,
    }
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
    let mut plan = query.from.as_ref().map_or(LogicalPlan::OneRow, lower_scan);
    let mut visible_columns = query.from.as_ref().map_or_else(Vec::new, |from| {
        from.columns.iter().map(column_ref_from_hir).collect()
    });
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
                .filter_map(|item| match item {
                    TypedProjectionItem::Column(column) => {
                        Some(AggregateOutput::GroupKey(column_ref_from_hir(column)))
                    }
                    TypedProjectionItem::Aggregate(aggregate) => {
                        Some(AggregateOutput::Aggregate(lower_aggregate(aggregate)))
                    }
                    TypedProjectionItem::Expression { .. } => None,
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
        if query
            .projection
            .iter()
            .any(|item| matches!(item, TypedProjectionItem::Expression { .. }))
        {
            plan = LogicalPlan::ScalarProject {
                input: Box::new(plan),
                expressions: query
                    .projection
                    .iter()
                    .filter_map(|item| match item {
                        TypedProjectionItem::Column(column) => Some(ProjectedExpr {
                            expression: Expr {
                                kind: ExprKind::Column(column_ref_from_hir(column)),
                                expr_type: netbadb_types::ExprType {
                                    data_type: column.data_type.clone(),
                                    nullable: column.nullable,
                                },
                            },
                            output: DerivedField {
                                name: column.name.clone(),
                                data_type: column.data_type.clone(),
                                nullable: column.nullable,
                            },
                        }),
                        TypedProjectionItem::Expression {
                            expression,
                            output_name,
                        } => Some(ProjectedExpr {
                            expression: lower_expr(expression),
                            output: DerivedField {
                                name: output_name.clone(),
                                data_type: expression.expr_type.data_type.clone(),
                                nullable: expression.expr_type.nullable,
                            },
                        }),
                        TypedProjectionItem::Aggregate(_) => None,
                    })
                    .collect(),
            };
        } else {
            plan = LogicalPlan::Project {
                input: Box::new(plan),
                columns: query
                    .projection
                    .iter()
                    .filter_map(|item| item.source_column().map(column_ref_from_hir))
                    .collect(),
            };
        }
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
        TypedExprKind::Parameter(id) => ExprKind::Parameter(*id),
        TypedExprKind::Cast { expression } => ExprKind::Cast {
            expression: Box::new(lower_expr(expression)),
        },
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

/// Binds owned scalar values into a compiled parameterized statement without
/// reparsing or resolving names. The returned statement is ephemeral and may
/// be optimized and planned using the concrete values.
pub fn bind_statement(
    compiled: &CompiledStatement,
    values: &[ScalarValue],
) -> Result<LogicalStatement, BindError> {
    if compiled.parameters.len() != values.len() {
        return Err(BindError::ParameterCount {
            expected: compiled.parameters.len(),
            actual: values.len(),
        });
    }
    for (parameter, value) in compiled.parameters.iter().zip(values) {
        if !value.matches_type(&parameter.data_type) {
            return Err(BindError::ParameterType {
                id: parameter.id,
                expected: parameter.data_type.clone(),
                actual: value.physical_type(),
            });
        }
    }
    let mut statement = compiled.logical_statement.clone();
    bind_logical_statement(&mut statement, values);
    Ok(statement)
}

fn bind_logical_statement(statement: &mut LogicalStatement, values: &[ScalarValue]) {
    match statement {
        LogicalStatement::Query(plan) => bind_plan(plan, values),
        LogicalStatement::Insert { values: exprs, .. } => {
            exprs.iter_mut().for_each(|expr| bind_expr(expr, values));
        }
        LogicalStatement::Update {
            input, assignments, ..
        } => {
            bind_plan(input, values);
            assignments
                .iter_mut()
                .for_each(|assignment| bind_expr(&mut assignment.value, values));
        }
        LogicalStatement::Delete { input, .. } => bind_plan(input, values),
    }
}

fn bind_plan(plan: &mut LogicalPlan, values: &[ScalarValue]) {
    match plan {
        LogicalPlan::OneRow | LogicalPlan::Scan { .. } => {}
        LogicalPlan::Join {
            left,
            right,
            predicate,
            ..
        } => {
            bind_plan(left, values);
            bind_plan(right, values);
            bind_expr(predicate, values);
        }
        LogicalPlan::Filter { input, predicate } => {
            bind_plan(input, values);
            bind_expr(predicate, values);
        }
        LogicalPlan::Sort { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Limit { input, .. } => bind_plan(input, values),
        LogicalPlan::ScalarProject { input, expressions } => {
            bind_plan(input, values);
            expressions
                .iter_mut()
                .for_each(|projected| bind_expr(&mut projected.expression, values));
        }
    }
}

fn bind_expr(expression: &mut Expr, values: &[ScalarValue]) {
    match &mut expression.kind {
        ExprKind::Parameter(id) => {
            if let Some(value) = values.get(id.0 as usize) {
                expression.kind = ExprKind::Literal(value.clone());
            }
        }
        ExprKind::Binary { left, right, .. } => {
            bind_expr(left, values);
            bind_expr(right, values);
        }
        ExprKind::Cast { expression: inner } => {
            bind_expr(inner, values);
            *expression = (**inner).clone();
        }
        ExprKind::Unary { expression, .. } | ExprKind::IsNull { expression, .. } => {
            bind_expr(expression, values);
        }
        ExprKind::Column(_) | ExprKind::Literal(_) => {}
    }
}

#[cfg(test)]
mod tests {
    use super::{bind_statement, compile, compile_statement, compile_statement_with_parameters};
    use netbadb_rel::{
        AggregateFunction, ExprKind, LogicalPlan, LogicalStatement, NullOrder, OutputField,
        SortDirection,
    };
    use netbadb_schema::{ColumnDef, Schema, TableDef, TypeSpec};
    use netbadb_types::{
        ColumnId, ParameterId, PhysicalType, RelationBindingId, ScalarValue, TableId,
    };

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

    #[test]
    fn infers_nominal_parameters_and_rejects_conflicting_reuse() {
        let schema = Schema::new(vec![TableDef::new(
            TableId(1),
            "links",
            vec![
                ColumnDef::new(
                    ColumnId(1),
                    "user_id",
                    TypeSpec::Semantic {
                        name: "UserId".into(),
                        physical: PhysicalType::Int64,
                    },
                ),
                ColumnDef::new(
                    ColumnId(2),
                    "parent_id",
                    TypeSpec::Semantic {
                        name: "UserId".into(),
                        physical: PhysicalType::Int64,
                    },
                ),
                ColumnDef::new(
                    ColumnId(3),
                    "team_id",
                    TypeSpec::Semantic {
                        name: "TeamId".into(),
                        physical: PhysicalType::Int64,
                    },
                ),
            ],
        )])
        .expect("schema");
        let prepared = compile_statement_with_parameters(
            &schema,
            "SELECT user_id FROM links WHERE user_id = $1 OR parent_id = $1",
            &[],
        )
        .expect("infer repeated UserId");
        assert_eq!(prepared.parameters.len(), 1);
        assert_eq!(prepared.parameters[0].id, ParameterId(0));
        assert_eq!(
            prepared.parameters[0].data_type.name.as_deref(),
            Some("UserId")
        );
        assert!(matches!(
            compile_statement_with_parameters(
                &schema,
                "SELECT user_id FROM links WHERE user_id = $1 OR team_id = $1",
                &[],
            ),
            Err(super::CompileError::Hir(
                netbadb_hir::HirError::ParameterTypeConflict { .. }
            ))
        ));
    }

    #[test]
    fn typed_casts_preserve_contextual_nominal_parameter_types() {
        let schema = Schema::new(vec![TableDef::new(
            TableId(1),
            "users",
            vec![
                ColumnDef::new(
                    ColumnId(1),
                    "id",
                    TypeSpec::Semantic {
                        name: "UserId".into(),
                        physical: PhysicalType::Int64,
                    },
                ),
                ColumnDef::new(ColumnId(2), "name", TypeSpec::Physical(PhysicalType::Text)),
            ],
        )])
        .expect("schema");
        for source in [
            "SELECT id FROM users WHERE id = $1::BIGINT",
            "INSERT INTO users (id, name) VALUES ($1::INT8, $2::VARCHAR)",
        ] {
            let prepared = compile_statement_with_parameters(&schema, source, &[])
                .unwrap_or_else(|error| panic!("{source}: {error}"));
            assert_eq!(
                prepared.parameters[0].data_type.name.as_deref(),
                Some("UserId"),
                "{source}"
            );
            let values = if source.starts_with("SELECT") {
                vec![ScalarValue::Int64(7)]
            } else {
                vec![ScalarValue::Int64(7), ScalarValue::Text("Ada".into())]
            };
            bind_statement(&prepared, &values)
                .unwrap_or_else(|error| panic!("bind {source}: {error}"));
        }
        assert!(
            compile_statement_with_parameters(
                &schema,
                "SELECT id FROM users WHERE id = $1::TEXT",
                &[],
            )
            .is_err()
        );
    }

    #[test]
    fn binds_values_without_reparsing_and_supports_fromless_select() {
        let schema = Schema::new(vec![TableDef::new(
            TableId(1),
            "users",
            vec![ColumnDef::new(
                ColumnId(1),
                "id",
                TypeSpec::Physical(PhysicalType::Int64),
            )],
        )])
        .expect("schema");
        let prepared = compile_statement_with_parameters(
            &schema,
            "SELECT $1 AS one",
            &[Some(PhysicalType::Int64)],
        )
        .expect("prepare scalar SELECT");
        assert_eq!(prepared.parameters.len(), 1);
        let bound = bind_statement(&prepared, &[ScalarValue::Int64(42)]).expect("bind");
        let LogicalStatement::Query(LogicalPlan::ScalarProject { expressions, input }) = bound
        else {
            panic!("expected scalar projection");
        };
        assert!(matches!(*input, LogicalPlan::OneRow));
        assert!(matches!(
            expressions[0].expression.kind,
            ExprKind::Literal(ScalarValue::Int64(42))
        ));
    }

    #[test]
    fn parameters_cover_select_and_typed_dml() {
        let schema = Schema::new(vec![TableDef::new(
            TableId(1),
            "users",
            vec![
                ColumnDef::new(ColumnId(1), "id", TypeSpec::Physical(PhysicalType::Int64)),
                ColumnDef::new(ColumnId(2), "name", TypeSpec::Physical(PhysicalType::Text)),
            ],
        )])
        .expect("schema");
        for source in [
            "SELECT id FROM users WHERE id = $1",
            "INSERT INTO users (id, name) VALUES ($1, $2)",
            "UPDATE users SET name = $1 WHERE id = $2",
            "DELETE FROM users WHERE id = $1",
        ] {
            let prepared = compile_statement_with_parameters(&schema, source, &[])
                .unwrap_or_else(|error| panic!("{source}: {error}"));
            assert!(!prepared.parameters.is_empty(), "{source}");
        }
        assert!(matches!(
            compile_statement_with_parameters(&schema, "SELECT $1", &[]),
            Err(super::CompileError::Hir(
                netbadb_hir::HirError::CannotInferParameterType { .. }
            ))
        ));
    }
}
