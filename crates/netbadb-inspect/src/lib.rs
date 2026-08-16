//! Stable, read-only inspection values and deterministic human-readable text.
//!
//! These values describe catalog and planner decisions without exposing the
//! internal planner IR, storage handles, or persistent page identities. They
//! are observation results only and never drive execution.

use std::fmt::{self, Write};

use netbadb_schema::SchemaFingerprint;
use netbadb_types::{ColumnId, RelationBindingId, ScalarValue, SemanticType, TableId};

/// One declaration-ordered snapshot of the visible canonical catalog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogInspection {
    /// Tables in canonical schema declaration order.
    pub tables: Vec<TableInspection>,
}

/// Canonical table metadata plus registered indexes and cached statistics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableInspection {
    pub table_id: TableId,
    pub name: String,
    pub fingerprint: SchemaFingerprint,
    /// Columns in canonical schema declaration order.
    pub columns: Vec<ColumnInspection>,
    /// Indexes in persistent registration order.
    pub indexes: Vec<IndexInspection>,
    /// Last explicit `ANALYZE` snapshot. It may be stale.
    pub statistics: Option<TableStatisticsInspection>,
}

/// One canonical column in declaration order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnInspection {
    pub column_id: ColumnId,
    pub name: String,
    pub data_type: SemanticType,
    pub nullable: bool,
    pub primary_key: bool,
}

/// One registered single-column index without its physical tree handle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexInspection {
    pub column_id: ColumnId,
    pub column_name: String,
    /// Zero-based persistent registration position.
    pub registration_order: u32,
    /// Last explicit `ANALYZE` snapshot. It may be stale.
    pub statistics: Option<IndexStatisticsInspection>,
}

/// Last-`ANALYZE` table snapshot used by the planner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableStatisticsInspection {
    pub row_count: u64,
    pub managed_page_count: u64,
}

/// Last-`ANALYZE` index snapshot used by the planner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexStatisticsInspection {
    pub distinct_non_null_keys: u64,
    pub null_count: u64,
    pub tree_height: u32,
}

/// Stable top-level SQL statement classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatementKind {
    Query,
    Insert,
    Update,
    Delete,
}

/// Access, result, and chosen physical-plan inspection for one compilation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatementInspection {
    pub kind: StatementKind,
    pub access: StatementAccessInspection,
    pub result: StatementResultInspection,
    pub plan: StatementPlanInspection,
}

/// Canonical table access in typed logical first-occurrence order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatementAccessInspection {
    /// Read tables in typed logical first-occurrence order.
    pub read_tables: Vec<TableId>,
    /// Write tables in typed logical first-occurrence order.
    pub write_tables: Vec<TableId>,
}

/// Result shape promised by the inspected statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatementResultInspection {
    Query { columns: Vec<ResultFieldInspection> },
    AffectedRows,
}

/// One query result field with optional source-column provenance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResultFieldInspection {
    pub name: String,
    pub data_type: SemanticType,
    pub nullable: bool,
    /// Present only when the result retains one source-column identity.
    pub source: Option<SourceColumnInspection>,
}

/// Stable source identity retained by a projected base or group-key field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceColumnInspection {
    pub binding_id: RelationBindingId,
    pub table_id: TableId,
    pub column_id: ColumnId,
    pub relation_name: String,
    pub name: String,
}

/// Fully typed, binding-aware column identity used inside expressions/plans.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnReferenceInspection {
    pub binding_id: RelationBindingId,
    pub table_id: TableId,
    pub column_id: ColumnId,
    pub relation_name: String,
    pub name: String,
    pub data_type: SemanticType,
    pub nullable: bool,
}

/// One typed expression with explicit possible-NULL metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpressionInspection {
    pub kind: ExpressionKindInspection,
    pub data_type: SemanticType,
    pub nullable: bool,
}

/// Stable structural expression variants; no debug-formatted internal IR.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExpressionKindInspection {
    Column(ColumnReferenceInspection),
    Literal(ScalarValue),
    Binary {
        operator: BinaryOpInspection,
        left: Box<ExpressionInspection>,
        right: Box<ExpressionInspection>,
    },
    Unary {
        operator: UnaryOpInspection,
        expression: Box<ExpressionInspection>,
    },
    IsNull {
        expression: Box<ExpressionInspection>,
        negated: bool,
    },
}

/// Stable binary-expression operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOpInspection {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    And,
    Or,
}

/// Stable unary-expression operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOpInspection {
    Not,
}

/// Chosen physical operator tree stripped of executor/storage handles.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanNodeInspection {
    SeqScan {
        binding_id: RelationBindingId,
        table_id: TableId,
        table_name: String,
        columns: Vec<ColumnReferenceInspection>,
    },
    IndexScan {
        binding_id: RelationBindingId,
        table_id: TableId,
        table_name: String,
        columns: Vec<ColumnReferenceInspection>,
        index_column: ColumnReferenceInspection,
        key: ScalarValue,
    },
    NestedLoopJoin {
        kind: JoinKindInspection,
        predicate: ExpressionInspection,
        left: Box<PlanNodeInspection>,
        right: Box<PlanNodeInspection>,
    },
    Filter {
        predicate: ExpressionInspection,
        input: Box<PlanNodeInspection>,
    },
    Sort {
        keys: Vec<SortKeyInspection>,
        input: Box<PlanNodeInspection>,
    },
    Project {
        columns: Vec<ColumnReferenceInspection>,
        input: Box<PlanNodeInspection>,
    },
    Aggregate {
        group_keys: Vec<ColumnReferenceInspection>,
        outputs: Vec<AggregateOutputInspection>,
        input: Box<PlanNodeInspection>,
    },
    Limit {
        limit: u64,
        input: Box<PlanNodeInspection>,
    },
}

/// Stable join semantic represented by a physical join node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKindInspection {
    Inner,
}

/// One fully explicit physical sort key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortKeyInspection {
    pub column: ColumnReferenceInspection,
    pub direction: SortDirectionInspection,
    pub null_order: NullOrderInspection,
}

/// Sort direction selected by the typed query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirectionInspection {
    Asc,
    Desc,
}

/// Explicit placement of NULL values for one sort key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NullOrderInspection {
    First,
    Last,
}

/// Stable aggregate function classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFunctionInspection {
    Count,
    Sum,
    Min,
    Max,
}

/// Aggregate input identity, including the distinct `*` case.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AggregateInputInspection {
    All,
    Column(ColumnReferenceInspection),
}

/// Projection-ordered output of a physical aggregate node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AggregateOutputInspection {
    GroupKey(ColumnReferenceInspection),
    Aggregate {
        function: AggregateFunctionInspection,
        input: AggregateInputInspection,
        output: ResultFieldInspection,
    },
}

/// One typed UPDATE assignment evaluated against the original row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssignmentInspection {
    pub column: ColumnReferenceInspection,
    pub value: ExpressionInspection,
}

/// Chosen query or DML physical statement shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatementPlanInspection {
    Query {
        root: PlanNodeInspection,
    },
    Insert {
        table_id: TableId,
        table_name: String,
        values: Vec<ExpressionInspection>,
    },
    Update {
        table_id: TableId,
        input: PlanNodeInspection,
        assignments: Vec<AssignmentInspection>,
    },
    Delete {
        table_id: TableId,
        input: PlanNodeInspection,
    },
}

/// Renders deterministic human-readable catalog text. This is not a machine
/// format or a permanent serialization contract.
#[must_use]
pub fn render_catalog(catalog: &CatalogInspection) -> String {
    let mut renderer = Renderer::default();
    renderer.line(0, format_args!("Catalog"));
    for table in &catalog.tables {
        renderer.line(
            0,
            format_args!("Table {} #{}", escape_text(&table.name), table.table_id.0),
        );
        renderer.line(1, format_args!("fingerprint: {}", table.fingerprint));
        renderer.line(1, format_args!("columns:"));
        for column in &table.columns {
            renderer.column(2, column);
        }
        renderer.line(1, format_args!("indexes:"));
        if table.indexes.is_empty() {
            renderer.line(2, format_args!("none"));
        }
        for index in &table.indexes {
            renderer.line(
                2,
                format_args!(
                    "[{}] column #{} {}",
                    index.registration_order,
                    index.column_id.0,
                    escape_text(&index.column_name)
                ),
            );
            match index.statistics {
                Some(statistics) => renderer.line(
                    3,
                    format_args!(
                        "statistics: distinct_non_null_keys={} null_count={} tree_height={}",
                        statistics.distinct_non_null_keys,
                        statistics.null_count,
                        statistics.tree_height
                    ),
                ),
                None => renderer.line(3, format_args!("statistics: none")),
            }
        }
        match table.statistics {
            Some(statistics) => {
                renderer.line(1, format_args!("statistics:"));
                renderer.line(2, format_args!("rows: {}", statistics.row_count));
                renderer.line(
                    2,
                    format_args!("managed_pages: {}", statistics.managed_page_count),
                );
            }
            None => renderer.line(1, format_args!("statistics: none")),
        }
    }
    renderer.finish()
}

/// Renders deterministic human-readable statement and chosen-plan text. This
/// is not SQL and is not a machine serialization contract.
#[must_use]
pub fn render_statement(statement: &StatementInspection) -> String {
    let mut renderer = Renderer::default();
    renderer.line(0, format_args!("{}", statement_kind(statement.kind)));
    renderer.line(
        1,
        format_args!(
            "access: read={} write={}",
            table_ids(&statement.access.read_tables),
            table_ids(&statement.access.write_tables)
        ),
    );
    match &statement.result {
        StatementResultInspection::Query { columns } => {
            renderer.line(1, format_args!("result:"));
            for (position, column) in columns.iter().enumerate() {
                let source = column
                    .source
                    .as_ref()
                    .map(source_column_text)
                    .unwrap_or_else(|| "derived".to_owned());
                renderer.line(
                    2,
                    format_args!(
                        "[{}] {} {} {} source={}",
                        position,
                        escape_text(&column.name),
                        semantic_type_text(&column.data_type),
                        nullability(column.nullable),
                        source
                    ),
                );
            }
        }
        StatementResultInspection::AffectedRows => {
            renderer.line(1, format_args!("result: AffectedRows"));
        }
    }
    renderer.line(1, format_args!("plan:"));
    match &statement.plan {
        StatementPlanInspection::Query { root } => renderer.plan(2, root),
        StatementPlanInspection::Insert {
            table_id,
            table_name,
            values,
        } => renderer.line(
            2,
            format_args!(
                "Insert table={}#{} values={}",
                escape_text(table_name),
                table_id.0,
                expressions_text(values)
            ),
        ),
        StatementPlanInspection::Update {
            table_id,
            input,
            assignments,
        } => {
            renderer.line(2, format_args!("Update table=#{}", table_id.0));
            renderer.line(3, format_args!("assignments:"));
            for assignment in assignments {
                renderer.line(
                    4,
                    format_args!(
                        "{} = {}",
                        column_reference_text(&assignment.column),
                        expression_text(&assignment.value)
                    ),
                );
            }
            renderer.line(3, format_args!("input:"));
            renderer.plan(4, input);
        }
        StatementPlanInspection::Delete { table_id, input } => {
            renderer.line(2, format_args!("Delete table=#{}", table_id.0));
            renderer.line(3, format_args!("input:"));
            renderer.plan(4, input);
        }
    }
    renderer.finish()
}

#[derive(Default)]
struct Renderer {
    output: String,
}

impl Renderer {
    fn line(&mut self, depth: usize, arguments: fmt::Arguments<'_>) {
        for _ in 0..depth {
            self.output.push_str("  ");
        }
        self.output
            .write_fmt(arguments)
            .expect("writing inspection text to String is infallible");
        self.output.push('\n');
    }

    fn column(&mut self, depth: usize, column: &ColumnInspection) {
        let primary_key = if column.primary_key {
            " PRIMARY KEY"
        } else {
            ""
        };
        self.line(
            depth,
            format_args!(
                "#{} {} {} {}{}",
                column.column_id.0,
                escape_text(&column.name),
                semantic_type_text(&column.data_type),
                nullability(column.nullable),
                primary_key
            ),
        );
    }

    fn plan(&mut self, depth: usize, plan: &PlanNodeInspection) {
        match plan {
            PlanNodeInspection::SeqScan {
                binding_id,
                table_id,
                table_name,
                columns,
            } => self.line(
                depth,
                format_args!(
                    "SeqScan table={}#{} binding=#{} columns={}",
                    escape_text(table_name),
                    table_id.0,
                    binding_id.0,
                    columns_text(columns)
                ),
            ),
            PlanNodeInspection::IndexScan {
                binding_id,
                table_id,
                table_name,
                columns,
                index_column,
                key,
            } => self.line(
                depth,
                format_args!(
                    "IndexScan table={}#{} binding=#{} columns={} index={}#{} key={}",
                    escape_text(table_name),
                    table_id.0,
                    binding_id.0,
                    columns_text(columns),
                    escape_text(&index_column.name),
                    index_column.column_id.0,
                    scalar_text(key)
                ),
            ),
            PlanNodeInspection::NestedLoopJoin {
                kind,
                predicate,
                left,
                right,
            } => {
                self.line(
                    depth,
                    format_args!(
                        "NestedLoopJoin kind={} predicate={}",
                        join_kind(*kind),
                        expression_text(predicate)
                    ),
                );
                self.line(depth + 1, format_args!("left:"));
                self.plan(depth + 2, left);
                self.line(depth + 1, format_args!("right:"));
                self.plan(depth + 2, right);
            }
            PlanNodeInspection::Filter { predicate, input } => {
                self.line(
                    depth,
                    format_args!("Filter predicate={}", expression_text(predicate)),
                );
                self.plan(depth + 1, input);
            }
            PlanNodeInspection::Sort { keys, input } => {
                self.line(depth, format_args!("Sort keys={}", sort_keys_text(keys)));
                self.plan(depth + 1, input);
            }
            PlanNodeInspection::Project { columns, input } => {
                self.line(
                    depth,
                    format_args!("Project columns={}", columns_text(columns)),
                );
                self.plan(depth + 1, input);
            }
            PlanNodeInspection::Aggregate {
                group_keys,
                outputs,
                input,
            } => {
                self.line(
                    depth,
                    format_args!(
                        "Aggregate group_keys={} outputs={}",
                        columns_text(group_keys),
                        aggregate_outputs_text(outputs)
                    ),
                );
                self.plan(depth + 1, input);
            }
            PlanNodeInspection::Limit { limit, input } => {
                self.line(depth, format_args!("Limit value={limit}"));
                self.plan(depth + 1, input);
            }
        }
    }

    fn finish(self) -> String {
        self.output
    }
}

fn statement_kind(kind: StatementKind) -> &'static str {
    match kind {
        StatementKind::Query => "Query",
        StatementKind::Insert => "Insert",
        StatementKind::Update => "Update",
        StatementKind::Delete => "Delete",
    }
}

fn nullability(nullable: bool) -> &'static str {
    if nullable { "NULL" } else { "NOT NULL" }
}

fn join_kind(kind: JoinKindInspection) -> &'static str {
    match kind {
        JoinKindInspection::Inner => "Inner",
    }
}

fn binary_operator(operator: BinaryOpInspection) -> &'static str {
    match operator {
        BinaryOpInspection::Eq => "Eq",
        BinaryOpInspection::NotEq => "NotEq",
        BinaryOpInspection::Lt => "Lt",
        BinaryOpInspection::LtEq => "LtEq",
        BinaryOpInspection::Gt => "Gt",
        BinaryOpInspection::GtEq => "GtEq",
        BinaryOpInspection::And => "And",
        BinaryOpInspection::Or => "Or",
    }
}

fn unary_operator(operator: UnaryOpInspection) -> &'static str {
    match operator {
        UnaryOpInspection::Not => "Not",
    }
}

fn sort_direction(direction: SortDirectionInspection) -> &'static str {
    match direction {
        SortDirectionInspection::Asc => "Asc",
        SortDirectionInspection::Desc => "Desc",
    }
}

fn null_order(order: NullOrderInspection) -> &'static str {
    match order {
        NullOrderInspection::First => "First",
        NullOrderInspection::Last => "Last",
    }
}

fn aggregate_function(function: AggregateFunctionInspection) -> &'static str {
    match function {
        AggregateFunctionInspection::Count => "Count",
        AggregateFunctionInspection::Sum => "Sum",
        AggregateFunctionInspection::Min => "Min",
        AggregateFunctionInspection::Max => "Max",
    }
}

fn semantic_type_text(data_type: &SemanticType) -> String {
    match &data_type.name {
        Some(name) => format!("{}({})", escape_text(name), data_type.physical),
        None => data_type.physical.to_string(),
    }
}

fn table_ids(ids: &[TableId]) -> String {
    let values = ids
        .iter()
        .map(|id| format!("#{}", id.0))
        .collect::<Vec<_>>()
        .join(", ");
    format!("[{values}]")
}

fn source_column_text(column: &SourceColumnInspection) -> String {
    format!(
        "{}#{}.{}#{}@table#{}",
        escape_text(&column.relation_name),
        column.binding_id.0,
        escape_text(&column.name),
        column.column_id.0,
        column.table_id.0
    )
}

fn column_reference_text(column: &ColumnReferenceInspection) -> String {
    format!(
        "{}#{}.{}#{}@table#{}",
        escape_text(&column.relation_name),
        column.binding_id.0,
        escape_text(&column.name),
        column.column_id.0,
        column.table_id.0
    )
}

fn columns_text(columns: &[ColumnReferenceInspection]) -> String {
    let values = columns
        .iter()
        .map(column_reference_text)
        .collect::<Vec<_>>()
        .join(", ");
    format!("[{values}]")
}

fn expressions_text(expressions: &[ExpressionInspection]) -> String {
    let values = expressions
        .iter()
        .map(expression_text)
        .collect::<Vec<_>>()
        .join(", ");
    format!("[{values}]")
}

fn expression_text(expression: &ExpressionInspection) -> String {
    match &expression.kind {
        ExpressionKindInspection::Column(column) => column_reference_text(column),
        ExpressionKindInspection::Literal(value) => scalar_text(value),
        ExpressionKindInspection::Binary {
            operator,
            left,
            right,
        } => format!(
            "{}({}, {})",
            binary_operator(*operator),
            expression_text(left),
            expression_text(right)
        ),
        ExpressionKindInspection::Unary {
            operator,
            expression,
        } => format!(
            "{}({})",
            unary_operator(*operator),
            expression_text(expression)
        ),
        ExpressionKindInspection::IsNull {
            expression,
            negated,
        } => format!(
            "{}({})",
            if *negated { "IsNotNull" } else { "IsNull" },
            expression_text(expression)
        ),
    }
}

fn scalar_text(value: &ScalarValue) -> String {
    match value {
        ScalarValue::Null => "NULL".to_owned(),
        ScalarValue::Bool(value) => format!("BOOL({value})"),
        ScalarValue::Int64(value) => format!("INT64({value})"),
        ScalarValue::UInt64(value) => format!("UINT64({value})"),
        ScalarValue::Text(value) => format!("TEXT(\"{}\")", escape_text(value)),
    }
}

fn escape_text(value: &str) -> String {
    let mut escaped = String::new();
    for character in value.chars() {
        match character {
            '\"' => escaped.push_str("\\\""),
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            character if character.is_control() => {
                escaped.push_str(&format!("\\u{{{:x}}}", u32::from(character)));
            }
            character => escaped.push(character),
        }
    }
    escaped
}

fn sort_keys_text(keys: &[SortKeyInspection]) -> String {
    let values = keys
        .iter()
        .map(|key| {
            format!(
                "{} {} Nulls{}",
                column_reference_text(&key.column),
                sort_direction(key.direction),
                null_order(key.null_order)
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("[{values}]")
}

fn aggregate_input_text(input: &AggregateInputInspection) -> String {
    match input {
        AggregateInputInspection::All => "All".to_owned(),
        AggregateInputInspection::Column(column) => column_reference_text(column),
    }
}

fn aggregate_outputs_text(outputs: &[AggregateOutputInspection]) -> String {
    let values = outputs
        .iter()
        .map(|output| match output {
            AggregateOutputInspection::GroupKey(column) => {
                format!("GroupKey({})", column_reference_text(column))
            }
            AggregateOutputInspection::Aggregate {
                function,
                input,
                output,
            } => format!(
                "{}({}) -> {}:{}:{}",
                aggregate_function(*function),
                aggregate_input_text(input),
                escape_text(&output.name),
                semantic_type_text(&output.data_type),
                nullability(output.nullable)
            ),
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("[{values}]")
}

#[cfg(test)]
mod tests;
