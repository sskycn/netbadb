use netbadb_sdk::inspection::{
    AggregateFunctionInspection, AggregateInputInspection, AggregateOutputInspection,
    AssignmentInspection, BinaryOpInspection, CatalogInspection, ColumnInspection,
    ColumnReferenceInspection, ExpressionInspection, ExpressionKindInspection, IndexInspection,
    IndexStatisticsInspection, JoinKindInspection, NullOrderInspection, PlanNodeInspection,
    ResultFieldInspection, SortDirectionInspection, SortKeyInspection, SourceColumnInspection,
    StatementAccessInspection, StatementInspection, StatementKind, StatementPlanInspection,
    StatementResultInspection, TableInspection, TableStatisticsInspection, UnaryOpInspection,
};
use netbadb_sdk::{PhysicalType, ScalarValue, SemanticType};
use serde::Serialize;

pub(crate) const INSPECTION_JSON_VERSION: u32 = 1;
const INSPECTION_JSON_FORMAT: &str = "netbadb-inspection";

pub(crate) fn render_catalog(catalog: &CatalogInspection) -> Result<String, serde_json::Error> {
    let envelope = CatalogEnvelope {
        format: INSPECTION_JSON_FORMAT,
        version: INSPECTION_JSON_VERSION,
        kind: "catalog",
        catalog: CatalogJson::from(catalog),
    };
    pretty(&envelope)
}

pub(crate) fn render_statement(
    statement: &StatementInspection,
) -> Result<String, serde_json::Error> {
    let envelope = StatementEnvelope {
        format: INSPECTION_JSON_FORMAT,
        version: INSPECTION_JSON_VERSION,
        kind: "statement",
        statement: StatementJson::from(statement),
    };
    pretty(&envelope)
}

fn pretty(value: &impl Serialize) -> Result<String, serde_json::Error> {
    let mut output = serde_json::to_string_pretty(value)?;
    output.push('\n');
    Ok(output)
}

#[derive(Serialize)]
struct CatalogEnvelope<'a> {
    format: &'static str,
    version: u32,
    kind: &'static str,
    catalog: CatalogJson<'a>,
}

#[derive(Serialize)]
struct CatalogJson<'a> {
    tables: Vec<TableJson<'a>>,
}

impl<'a> From<&'a CatalogInspection> for CatalogJson<'a> {
    fn from(catalog: &'a CatalogInspection) -> Self {
        Self {
            tables: catalog.tables.iter().map(TableJson::from).collect(),
        }
    }
}

#[derive(Serialize)]
struct TableJson<'a> {
    table_id: u64,
    name: &'a str,
    fingerprint: String,
    columns: Vec<ColumnJson<'a>>,
    indexes: Vec<IndexJson<'a>>,
    statistics: Option<TableStatisticsJson>,
}

impl<'a> From<&'a TableInspection> for TableJson<'a> {
    fn from(table: &'a TableInspection) -> Self {
        Self {
            table_id: table.table_id.0,
            name: &table.name,
            fingerprint: table.fingerprint.to_string(),
            columns: table.columns.iter().map(ColumnJson::from).collect(),
            indexes: table.indexes.iter().map(IndexJson::from).collect(),
            statistics: table.statistics.map(TableStatisticsJson::from),
        }
    }
}

#[derive(Serialize)]
struct ColumnJson<'a> {
    column_id: u32,
    name: &'a str,
    data_type: SemanticTypeJson<'a>,
    nullable: bool,
    primary_key: bool,
}

impl<'a> From<&'a ColumnInspection> for ColumnJson<'a> {
    fn from(column: &'a ColumnInspection) -> Self {
        Self {
            column_id: column.column_id.0,
            name: &column.name,
            data_type: SemanticTypeJson::from(&column.data_type),
            nullable: column.nullable,
            primary_key: column.primary_key,
        }
    }
}

#[derive(Serialize)]
struct IndexJson<'a> {
    column_id: u32,
    column_name: &'a str,
    registration_order: u32,
    statistics: Option<IndexStatisticsJson>,
}

impl<'a> From<&'a IndexInspection> for IndexJson<'a> {
    fn from(index: &'a IndexInspection) -> Self {
        Self {
            column_id: index.column_id.0,
            column_name: &index.column_name,
            registration_order: index.registration_order,
            statistics: index.statistics.map(IndexStatisticsJson::from),
        }
    }
}

#[derive(Serialize)]
struct TableStatisticsJson {
    row_count: u64,
    managed_page_count: u64,
}

impl From<TableStatisticsInspection> for TableStatisticsJson {
    fn from(statistics: TableStatisticsInspection) -> Self {
        Self {
            row_count: statistics.row_count,
            managed_page_count: statistics.managed_page_count,
        }
    }
}

#[derive(Serialize)]
struct IndexStatisticsJson {
    distinct_non_null_keys: u64,
    null_count: u64,
    tree_height: u32,
}

impl From<IndexStatisticsInspection> for IndexStatisticsJson {
    fn from(statistics: IndexStatisticsInspection) -> Self {
        Self {
            distinct_non_null_keys: statistics.distinct_non_null_keys,
            null_count: statistics.null_count,
            tree_height: statistics.tree_height,
        }
    }
}

#[derive(Serialize)]
struct StatementEnvelope<'a> {
    format: &'static str,
    version: u32,
    kind: &'static str,
    statement: StatementJson<'a>,
}

#[derive(Serialize)]
struct StatementJson<'a> {
    kind: &'static str,
    access: StatementAccessJson,
    result: StatementResultJson<'a>,
    plan: StatementPlanJson<'a>,
}

impl<'a> From<&'a StatementInspection> for StatementJson<'a> {
    fn from(statement: &'a StatementInspection) -> Self {
        Self {
            kind: statement_kind(statement.kind),
            access: StatementAccessJson::from(&statement.access),
            result: StatementResultJson::from(&statement.result),
            plan: StatementPlanJson::from(&statement.plan),
        }
    }
}

fn statement_kind(kind: StatementKind) -> &'static str {
    match kind {
        StatementKind::Query => "query",
        StatementKind::Insert => "insert",
        StatementKind::Update => "update",
        StatementKind::Delete => "delete",
    }
}

#[derive(Serialize)]
struct StatementAccessJson {
    read_tables: Vec<u64>,
    write_tables: Vec<u64>,
}

impl From<&StatementAccessInspection> for StatementAccessJson {
    fn from(access: &StatementAccessInspection) -> Self {
        Self {
            read_tables: access.read_tables.iter().map(|id| id.0).collect(),
            write_tables: access.write_tables.iter().map(|id| id.0).collect(),
        }
    }
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum StatementResultJson<'a> {
    Query { columns: Vec<ResultFieldJson<'a>> },
    AffectedRows,
}

impl<'a> From<&'a StatementResultInspection> for StatementResultJson<'a> {
    fn from(result: &'a StatementResultInspection) -> Self {
        match result {
            StatementResultInspection::Query { columns } => Self::Query {
                columns: columns.iter().map(ResultFieldJson::from).collect(),
            },
            StatementResultInspection::AffectedRows => Self::AffectedRows,
        }
    }
}

#[derive(Serialize)]
struct ResultFieldJson<'a> {
    name: &'a str,
    data_type: SemanticTypeJson<'a>,
    nullable: bool,
    source: Option<SourceColumnJson<'a>>,
}

impl<'a> From<&'a ResultFieldInspection> for ResultFieldJson<'a> {
    fn from(field: &'a ResultFieldInspection) -> Self {
        Self {
            name: &field.name,
            data_type: SemanticTypeJson::from(&field.data_type),
            nullable: field.nullable,
            source: field.source.as_ref().map(SourceColumnJson::from),
        }
    }
}

#[derive(Serialize)]
struct SourceColumnJson<'a> {
    binding_id: u32,
    table_id: u64,
    column_id: u32,
    relation_name: &'a str,
    name: &'a str,
}

impl<'a> From<&'a SourceColumnInspection> for SourceColumnJson<'a> {
    fn from(column: &'a SourceColumnInspection) -> Self {
        Self {
            binding_id: column.binding_id.0,
            table_id: column.table_id.0,
            column_id: column.column_id.0,
            relation_name: &column.relation_name,
            name: &column.name,
        }
    }
}

#[derive(Serialize)]
struct ColumnReferenceJson<'a> {
    binding_id: u32,
    table_id: u64,
    column_id: u32,
    relation_name: &'a str,
    name: &'a str,
    data_type: SemanticTypeJson<'a>,
    nullable: bool,
}

impl<'a> From<&'a ColumnReferenceInspection> for ColumnReferenceJson<'a> {
    fn from(column: &'a ColumnReferenceInspection) -> Self {
        Self {
            binding_id: column.binding_id.0,
            table_id: column.table_id.0,
            column_id: column.column_id.0,
            relation_name: &column.relation_name,
            name: &column.name,
            data_type: SemanticTypeJson::from(&column.data_type),
            nullable: column.nullable,
        }
    }
}

#[derive(Serialize)]
struct SemanticTypeJson<'a> {
    physical: &'static str,
    semantic_name: Option<&'a str>,
}

impl<'a> From<&'a SemanticType> for SemanticTypeJson<'a> {
    fn from(data_type: &'a SemanticType) -> Self {
        Self {
            physical: physical_type(data_type.physical),
            semantic_name: data_type.name.as_deref(),
        }
    }
}

fn physical_type(physical: PhysicalType) -> &'static str {
    match physical {
        PhysicalType::Bool => "bool",
        PhysicalType::Int64 => "int64",
        PhysicalType::UInt64 => "uint64",
        PhysicalType::Text => "text",
    }
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum StatementPlanJson<'a> {
    Query {
        root: PlanJson<'a>,
    },
    Insert {
        table_id: u64,
        table_name: &'a str,
        values: Vec<ExpressionJson<'a>>,
    },
    Update {
        table_id: u64,
        input: PlanJson<'a>,
        assignments: Vec<AssignmentJson<'a>>,
    },
    Delete {
        table_id: u64,
        input: PlanJson<'a>,
    },
}

impl<'a> From<&'a StatementPlanInspection> for StatementPlanJson<'a> {
    fn from(plan: &'a StatementPlanInspection) -> Self {
        match plan {
            StatementPlanInspection::Query { root } => Self::Query {
                root: PlanJson::from(root),
            },
            StatementPlanInspection::Insert {
                table_id,
                table_name,
                values,
            } => Self::Insert {
                table_id: table_id.0,
                table_name,
                values: values.iter().map(ExpressionJson::from).collect(),
            },
            StatementPlanInspection::Update {
                table_id,
                input,
                assignments,
            } => Self::Update {
                table_id: table_id.0,
                input: PlanJson::from(input),
                assignments: assignments.iter().map(AssignmentJson::from).collect(),
            },
            StatementPlanInspection::Delete { table_id, input } => Self::Delete {
                table_id: table_id.0,
                input: PlanJson::from(input),
            },
        }
    }
}

#[derive(Serialize)]
#[serde(tag = "operator", rename_all = "snake_case")]
enum PlanJson<'a> {
    SeqScan {
        binding_id: u32,
        table_id: u64,
        table_name: &'a str,
        columns: Vec<ColumnReferenceJson<'a>>,
    },
    IndexScan {
        binding_id: u32,
        table_id: u64,
        table_name: &'a str,
        columns: Vec<ColumnReferenceJson<'a>>,
        index_column: ColumnReferenceJson<'a>,
        key: ScalarJson<'a>,
    },
    NestedLoopJoin {
        kind: &'static str,
        predicate: ExpressionJson<'a>,
        left: Box<PlanJson<'a>>,
        right: Box<PlanJson<'a>>,
    },
    Filter {
        predicate: ExpressionJson<'a>,
        input: Box<PlanJson<'a>>,
    },
    Sort {
        keys: Vec<SortKeyJson<'a>>,
        input: Box<PlanJson<'a>>,
    },
    Project {
        columns: Vec<ColumnReferenceJson<'a>>,
        input: Box<PlanJson<'a>>,
    },
    Aggregate {
        group_keys: Vec<ColumnReferenceJson<'a>>,
        outputs: Vec<AggregateOutputJson<'a>>,
        input: Box<PlanJson<'a>>,
    },
    Limit {
        limit: u64,
        input: Box<PlanJson<'a>>,
    },
}

impl<'a> From<&'a PlanNodeInspection> for PlanJson<'a> {
    fn from(plan: &'a PlanNodeInspection) -> Self {
        match plan {
            PlanNodeInspection::SeqScan {
                binding_id,
                table_id,
                table_name,
                columns,
            } => Self::SeqScan {
                binding_id: binding_id.0,
                table_id: table_id.0,
                table_name,
                columns: columns.iter().map(ColumnReferenceJson::from).collect(),
            },
            PlanNodeInspection::IndexScan {
                binding_id,
                table_id,
                table_name,
                columns,
                index_column,
                key,
            } => Self::IndexScan {
                binding_id: binding_id.0,
                table_id: table_id.0,
                table_name,
                columns: columns.iter().map(ColumnReferenceJson::from).collect(),
                index_column: ColumnReferenceJson::from(index_column),
                key: ScalarJson::from(key),
            },
            PlanNodeInspection::NestedLoopJoin {
                kind,
                predicate,
                left,
                right,
            } => Self::NestedLoopJoin {
                kind: join_kind(*kind),
                predicate: ExpressionJson::from(predicate),
                left: Box::new(PlanJson::from(left.as_ref())),
                right: Box::new(PlanJson::from(right.as_ref())),
            },
            PlanNodeInspection::Filter { predicate, input } => Self::Filter {
                predicate: ExpressionJson::from(predicate),
                input: Box::new(PlanJson::from(input.as_ref())),
            },
            PlanNodeInspection::Sort { keys, input } => Self::Sort {
                keys: keys.iter().map(SortKeyJson::from).collect(),
                input: Box::new(PlanJson::from(input.as_ref())),
            },
            PlanNodeInspection::Project { columns, input } => Self::Project {
                columns: columns.iter().map(ColumnReferenceJson::from).collect(),
                input: Box::new(PlanJson::from(input.as_ref())),
            },
            PlanNodeInspection::Aggregate {
                group_keys,
                outputs,
                input,
            } => Self::Aggregate {
                group_keys: group_keys.iter().map(ColumnReferenceJson::from).collect(),
                outputs: outputs.iter().map(AggregateOutputJson::from).collect(),
                input: Box::new(PlanJson::from(input.as_ref())),
            },
            PlanNodeInspection::Limit { limit, input } => Self::Limit {
                limit: *limit,
                input: Box::new(PlanJson::from(input.as_ref())),
            },
        }
    }
}

fn join_kind(kind: JoinKindInspection) -> &'static str {
    match kind {
        JoinKindInspection::Inner => "inner",
    }
}

#[derive(Serialize)]
struct SortKeyJson<'a> {
    column: ColumnReferenceJson<'a>,
    direction: &'static str,
    null_order: &'static str,
}

impl<'a> From<&'a SortKeyInspection> for SortKeyJson<'a> {
    fn from(key: &'a SortKeyInspection) -> Self {
        Self {
            column: ColumnReferenceJson::from(&key.column),
            direction: match key.direction {
                SortDirectionInspection::Asc => "asc",
                SortDirectionInspection::Desc => "desc",
            },
            null_order: match key.null_order {
                NullOrderInspection::First => "first",
                NullOrderInspection::Last => "last",
            },
        }
    }
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum AggregateInputJson<'a> {
    All,
    Column { column: ColumnReferenceJson<'a> },
}

impl<'a> From<&'a AggregateInputInspection> for AggregateInputJson<'a> {
    fn from(input: &'a AggregateInputInspection) -> Self {
        match input {
            AggregateInputInspection::All => Self::All,
            AggregateInputInspection::Column(column) => Self::Column {
                column: ColumnReferenceJson::from(column),
            },
        }
    }
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum AggregateOutputJson<'a> {
    GroupKey {
        column: ColumnReferenceJson<'a>,
    },
    Aggregate {
        function: &'static str,
        input: AggregateInputJson<'a>,
        output: ResultFieldJson<'a>,
    },
}

impl<'a> From<&'a AggregateOutputInspection> for AggregateOutputJson<'a> {
    fn from(output: &'a AggregateOutputInspection) -> Self {
        match output {
            AggregateOutputInspection::GroupKey(column) => Self::GroupKey {
                column: ColumnReferenceJson::from(column),
            },
            AggregateOutputInspection::Aggregate {
                function,
                input,
                output,
            } => Self::Aggregate {
                function: aggregate_function(*function),
                input: AggregateInputJson::from(input),
                output: ResultFieldJson::from(output),
            },
        }
    }
}

fn aggregate_function(function: AggregateFunctionInspection) -> &'static str {
    match function {
        AggregateFunctionInspection::Count => "count",
        AggregateFunctionInspection::Sum => "sum",
        AggregateFunctionInspection::Min => "min",
        AggregateFunctionInspection::Max => "max",
    }
}

#[derive(Serialize)]
struct AssignmentJson<'a> {
    column: ColumnReferenceJson<'a>,
    value: ExpressionJson<'a>,
}

impl<'a> From<&'a AssignmentInspection> for AssignmentJson<'a> {
    fn from(assignment: &'a AssignmentInspection) -> Self {
        Self {
            column: ColumnReferenceJson::from(&assignment.column),
            value: ExpressionJson::from(&assignment.value),
        }
    }
}

#[derive(Serialize)]
struct ExpressionJson<'a> {
    #[serde(flatten)]
    kind: ExpressionKindJson<'a>,
    data_type: SemanticTypeJson<'a>,
    nullable: bool,
}

impl<'a> From<&'a ExpressionInspection> for ExpressionJson<'a> {
    fn from(expression: &'a ExpressionInspection) -> Self {
        Self {
            kind: ExpressionKindJson::from(&expression.kind),
            data_type: SemanticTypeJson::from(&expression.data_type),
            nullable: expression.nullable,
        }
    }
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ExpressionKindJson<'a> {
    Column {
        column: ColumnReferenceJson<'a>,
    },
    Literal {
        value: ScalarJson<'a>,
    },
    Binary {
        operator: &'static str,
        left: Box<ExpressionJson<'a>>,
        right: Box<ExpressionJson<'a>>,
    },
    Unary {
        operator: &'static str,
        expression: Box<ExpressionJson<'a>>,
    },
    IsNull {
        expression: Box<ExpressionJson<'a>>,
        negated: bool,
    },
}

impl<'a> From<&'a ExpressionKindInspection> for ExpressionKindJson<'a> {
    fn from(kind: &'a ExpressionKindInspection) -> Self {
        match kind {
            ExpressionKindInspection::Column(column) => Self::Column {
                column: ColumnReferenceJson::from(column),
            },
            ExpressionKindInspection::Literal(value) => Self::Literal {
                value: ScalarJson::from(value),
            },
            ExpressionKindInspection::Binary {
                operator,
                left,
                right,
            } => Self::Binary {
                operator: binary_operator(*operator),
                left: Box::new(ExpressionJson::from(left.as_ref())),
                right: Box::new(ExpressionJson::from(right.as_ref())),
            },
            ExpressionKindInspection::Unary {
                operator,
                expression,
            } => Self::Unary {
                operator: unary_operator(*operator),
                expression: Box::new(ExpressionJson::from(expression.as_ref())),
            },
            ExpressionKindInspection::IsNull {
                expression,
                negated,
            } => Self::IsNull {
                expression: Box::new(ExpressionJson::from(expression.as_ref())),
                negated: *negated,
            },
        }
    }
}

fn binary_operator(operator: BinaryOpInspection) -> &'static str {
    match operator {
        BinaryOpInspection::Eq => "eq",
        BinaryOpInspection::NotEq => "not_eq",
        BinaryOpInspection::Lt => "lt",
        BinaryOpInspection::LtEq => "lt_eq",
        BinaryOpInspection::Gt => "gt",
        BinaryOpInspection::GtEq => "gt_eq",
        BinaryOpInspection::And => "and",
        BinaryOpInspection::Or => "or",
    }
}

fn unary_operator(operator: UnaryOpInspection) -> &'static str {
    match operator {
        UnaryOpInspection::Not => "not",
    }
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ScalarJson<'a> {
    Null,
    Bool {
        value: bool,
    },
    Int64 {
        value: i64,
    },
    #[serde(rename = "uint64")]
    UInt64 {
        value: u64,
    },
    Text {
        value: &'a str,
    },
}

impl<'a> From<&'a ScalarValue> for ScalarJson<'a> {
    fn from(value: &'a ScalarValue) -> Self {
        match value {
            ScalarValue::Null => Self::Null,
            ScalarValue::Bool(value) => Self::Bool { value: *value },
            ScalarValue::Int64(value) => Self::Int64 { value: *value },
            ScalarValue::UInt64(value) => Self::UInt64 { value: *value },
            ScalarValue::Text(value) => Self::Text { value },
        }
    }
}

#[cfg(test)]
mod tests {
    use netbadb_sdk::inspection::{
        AggregateInputInspection, AggregateOutputInspection, BinaryOpInspection, CatalogInspection,
        ColumnInspection, ColumnReferenceInspection, ExpressionInspection,
        ExpressionKindInspection, IndexInspection, IndexStatisticsInspection, PlanNodeInspection,
        ResultFieldInspection, SourceColumnInspection, StatementAccessInspection,
        StatementInspection, StatementKind, StatementPlanInspection, StatementResultInspection,
        TableInspection, TableStatisticsInspection,
    };
    use netbadb_sdk::{
        ColumnId, PhysicalType, RelationBindingId, ScalarValue, SchemaFingerprint, SemanticType,
        TableId,
    };

    use super::{render_catalog, render_statement};

    fn column(id: u32, name: &str, physical: PhysicalType) -> ColumnReferenceInspection {
        ColumnReferenceInspection {
            binding_id: RelationBindingId(0),
            table_id: TableId(1),
            column_id: ColumnId(id),
            relation_name: "users".into(),
            name: name.into(),
            data_type: SemanticType::physical(physical),
            nullable: false,
        }
    }

    fn expression(kind: ExpressionKindInspection, physical: PhysicalType) -> ExpressionInspection {
        ExpressionInspection {
            kind,
            data_type: SemanticType::physical(physical),
            nullable: false,
        }
    }

    fn literal(value: ScalarValue, physical: PhysicalType) -> ExpressionInspection {
        expression(ExpressionKindInspection::Literal(value), physical)
    }

    fn statement(
        root: PlanNodeInspection,
        result: Vec<ResultFieldInspection>,
    ) -> StatementInspection {
        StatementInspection {
            kind: StatementKind::Query,
            access: StatementAccessInspection {
                read_tables: vec![TableId(1)],
                write_tables: Vec::new(),
            },
            result: StatementResultInspection::Query { columns: result },
            plan: StatementPlanInspection::Query { root },
        }
    }

    fn result(column: &ColumnReferenceInspection) -> ResultFieldInspection {
        ResultFieldInspection {
            name: column.name.clone(),
            data_type: column.data_type.clone(),
            nullable: column.nullable,
            source: Some(SourceColumnInspection {
                binding_id: column.binding_id,
                table_id: column.table_id,
                column_id: column.column_id,
                relation_name: column.relation_name.clone(),
                name: column.name.clone(),
            }),
        }
    }

    #[test]
    fn catalog_json_v1_matches_the_golden_contract() {
        let catalog = CatalogInspection {
            tables: vec![TableInspection {
                table_id: TableId(1),
                name: "users".into(),
                fingerprint: SchemaFingerprint::from_bytes([0xab; 32]),
                columns: vec![ColumnInspection {
                    column_id: ColumnId(1),
                    name: "id".into(),
                    data_type: SemanticType::named("UserId", PhysicalType::UInt64),
                    nullable: false,
                    primary_key: true,
                }],
                indexes: vec![IndexInspection {
                    column_id: ColumnId(1),
                    column_name: "id".into(),
                    registration_order: 0,
                    statistics: Some(IndexStatisticsInspection {
                        distinct_non_null_keys: 8,
                        null_count: 0,
                        tree_height: 1,
                    }),
                }],
                statistics: Some(TableStatisticsInspection {
                    row_count: 8,
                    managed_page_count: 2,
                }),
            }],
        };
        assert_eq!(
            render_catalog(&catalog).unwrap(),
            include_str!("../tests/golden/catalog-v1.json")
        );
    }

    #[test]
    fn seq_scan_statement_json_v1_matches_the_golden_contract() {
        let id = column(1, "id", PhysicalType::UInt64);
        let inspection = statement(
            PlanNodeInspection::Project {
                columns: vec![id.clone()],
                input: Box::new(PlanNodeInspection::SeqScan {
                    binding_id: RelationBindingId(0),
                    table_id: TableId(1),
                    table_name: "users".into(),
                    columns: vec![id.clone()],
                }),
            },
            vec![result(&id)],
        );
        assert_eq!(
            render_statement(&inspection).unwrap(),
            include_str!("../tests/golden/statement-seq-scan-v1.json")
        );
    }

    #[test]
    fn index_filter_statement_json_v1_matches_the_golden_contract() {
        let id = column(1, "id", PhysicalType::UInt64);
        let predicate = expression(
            ExpressionKindInspection::Binary {
                operator: BinaryOpInspection::Eq,
                left: Box::new(expression(
                    ExpressionKindInspection::Column(id.clone()),
                    PhysicalType::UInt64,
                )),
                right: Box::new(literal(ScalarValue::UInt64(42), PhysicalType::UInt64)),
            },
            PhysicalType::Bool,
        );
        let inspection = statement(
            PlanNodeInspection::Filter {
                predicate,
                input: Box::new(PlanNodeInspection::IndexScan {
                    binding_id: RelationBindingId(0),
                    table_id: TableId(1),
                    table_name: "users".into(),
                    columns: vec![id.clone()],
                    index_column: id.clone(),
                    key: ScalarValue::UInt64(42),
                }),
            },
            vec![result(&id)],
        );
        assert_eq!(
            render_statement(&inspection).unwrap(),
            include_str!("../tests/golden/statement-index-filter-v1.json")
        );
    }

    #[test]
    fn aggregate_statement_json_v1_matches_the_golden_contract() {
        let team_id = column(2, "team_id", PhysicalType::UInt64);
        let count = ResultFieldInspection {
            name: "count(*)".into(),
            data_type: SemanticType::physical(PhysicalType::UInt64),
            nullable: false,
            source: None,
        };
        let inspection = statement(
            PlanNodeInspection::Aggregate {
                group_keys: vec![team_id.clone()],
                outputs: vec![
                    AggregateOutputInspection::GroupKey(team_id.clone()),
                    AggregateOutputInspection::Aggregate {
                        function: netbadb_sdk::inspection::AggregateFunctionInspection::Count,
                        input: AggregateInputInspection::All,
                        output: count.clone(),
                    },
                ],
                input: Box::new(PlanNodeInspection::SeqScan {
                    binding_id: RelationBindingId(0),
                    table_id: TableId(1),
                    table_name: "users".into(),
                    columns: vec![team_id.clone()],
                }),
            },
            vec![result(&team_id), count],
        );
        assert_eq!(
            render_statement(&inspection).unwrap(),
            include_str!("../tests/golden/statement-aggregate-v1.json")
        );
    }

    #[test]
    fn dml_statement_json_v1_matches_the_golden_contract() {
        let inspection = StatementInspection {
            kind: StatementKind::Insert,
            access: StatementAccessInspection {
                read_tables: Vec::new(),
                write_tables: vec![TableId(1)],
            },
            result: StatementResultInspection::AffectedRows,
            plan: StatementPlanInspection::Insert {
                table_id: TableId(1),
                table_name: "users".into(),
                values: vec![
                    literal(ScalarValue::Null, PhysicalType::Text),
                    literal(ScalarValue::Bool(true), PhysicalType::Bool),
                    literal(ScalarValue::Int64(-1), PhysicalType::Int64),
                    literal(ScalarValue::UInt64(42), PhysicalType::UInt64),
                    literal(ScalarValue::Text("Ada".into()), PhysicalType::Text),
                ],
            },
        };
        assert_eq!(
            render_statement(&inspection).unwrap(),
            include_str!("../tests/golden/statement-dml-v1.json")
        );
    }
}
