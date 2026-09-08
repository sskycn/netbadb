use netbadb_sdk::inspection::{
    AggregateFunctionInspection, AggregateInputInspection, AggregateOutputInspection,
    AssignmentInspection, BinaryOpInspection, CatalogInspection, ColumnInspection,
    ColumnReferenceInspection, ExpressionInspection, ExpressionKindInspection, IndexInspection,
    IndexStatisticsInspection, JoinKindInspection, NullOrderInspection, PartitionAccessInspection,
    PlanNodeInspection, RangeBoundInspection, ResultFieldInspection, SortDirectionInspection,
    SortKeyInspection, SourceColumnInspection, StatementAccessInspection, StatementInspection,
    StatementKind, StatementPlanInspection, StatementResultInspection, TableInspection,
    TablePlacementInspection, TableStatisticsInspection, UnaryOpInspection,
};
use netbadb_sdk::{PhysicalType, ScalarValue, SemanticType};
use serde::Serialize;

const BASE_INSPECTION_JSON_VERSION: u32 = 3;
const PARTITION_INSPECTION_JSON_VERSION: u32 = 4;
const INDEX_JOIN_INSPECTION_JSON_VERSION: u32 = 5;
const COLUMNAR_INSPECTION_JSON_VERSION: u32 = 6;
pub(crate) const INSPECTION_JSON_VERSION: u32 = 7;
const INSPECTION_JSON_FORMAT: &str = "netbadb-inspection";

pub(crate) fn render_catalog(catalog: &CatalogInspection) -> Result<String, serde_json::Error> {
    let envelope = CatalogEnvelope {
        format: INSPECTION_JSON_FORMAT,
        version: if catalog_uses_physical_types_v2(catalog) {
            INSPECTION_JSON_VERSION
        } else if catalog.tables.iter().any(|table| {
            matches!(
                table.placement,
                TablePlacementInspection::RangePartitioned { .. }
            )
        }) {
            PARTITION_INSPECTION_JSON_VERSION
        } else {
            BASE_INSPECTION_JSON_VERSION
        },
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
        version: statement_json_version(statement),
        kind: "statement",
        statement: StatementJson::from(statement),
    };
    pretty(&envelope)
}

fn statement_json_version(statement: &StatementInspection) -> u32 {
    fn plan_has_columnar(plan: &PlanNodeInspection) -> bool {
        match plan {
            PlanNodeInspection::ColumnarScan { .. } => true,
            PlanNodeInspection::NestedLoopJoin { left, right, .. }
            | PlanNodeInspection::HashJoin { left, right, .. } => {
                plan_has_columnar(left) || plan_has_columnar(right)
            }
            PlanNodeInspection::IndexNestedLoopJoin { left, .. }
            | PlanNodeInspection::Filter { input: left, .. }
            | PlanNodeInspection::Sort { input: left, .. }
            | PlanNodeInspection::Project { input: left, .. }
            | PlanNodeInspection::ScalarProject { input: left, .. }
            | PlanNodeInspection::Aggregate { input: left, .. }
            | PlanNodeInspection::Limit { input: left, .. } => plan_has_columnar(left),
            PlanNodeInspection::OneRow
            | PlanNodeInspection::SeqScan { .. }
            | PlanNodeInspection::IndexScan { .. }
            | PlanNodeInspection::RangeIndexScan { .. }
            | PlanNodeInspection::PartitionedScan { .. } => false,
        }
    }
    fn plan_has_index_join(plan: &PlanNodeInspection) -> bool {
        match plan {
            PlanNodeInspection::IndexNestedLoopJoin { .. } => true,
            PlanNodeInspection::NestedLoopJoin { left, right, .. }
            | PlanNodeInspection::HashJoin { left, right, .. } => {
                plan_has_index_join(left) || plan_has_index_join(right)
            }
            PlanNodeInspection::Filter { input, .. }
            | PlanNodeInspection::Sort { input, .. }
            | PlanNodeInspection::Project { input, .. }
            | PlanNodeInspection::ScalarProject { input, .. }
            | PlanNodeInspection::Aggregate { input, .. }
            | PlanNodeInspection::Limit { input, .. } => plan_has_index_join(input),
            PlanNodeInspection::OneRow
            | PlanNodeInspection::SeqScan { .. }
            | PlanNodeInspection::ColumnarScan { .. }
            | PlanNodeInspection::IndexScan { .. }
            | PlanNodeInspection::RangeIndexScan { .. }
            | PlanNodeInspection::PartitionedScan { .. } => false,
        }
    }
    let has_index_join = match &statement.plan {
        StatementPlanInspection::Query { root } => plan_has_index_join(root),
        StatementPlanInspection::Update { input, .. }
        | StatementPlanInspection::Delete { input, .. } => plan_has_index_join(input),
        StatementPlanInspection::Insert { .. } => false,
    };
    let has_columnar = match &statement.plan {
        StatementPlanInspection::Query { root } => plan_has_columnar(root),
        StatementPlanInspection::Update { input, .. }
        | StatementPlanInspection::Delete { input, .. } => plan_has_columnar(input),
        StatementPlanInspection::Insert { .. } => false,
    };
    let structural = if has_columnar {
        COLUMNAR_INSPECTION_JSON_VERSION
    } else if has_index_join {
        INDEX_JOIN_INSPECTION_JSON_VERSION
    } else if statement_has_partitions(statement) {
        PARTITION_INSPECTION_JSON_VERSION
    } else {
        BASE_INSPECTION_JSON_VERSION
    };
    if statement_uses_physical_types_v2(statement) {
        structural.max(INSPECTION_JSON_VERSION)
    } else {
        structural
    }
}

fn is_physical_types_v2(physical: PhysicalType) -> bool {
    !matches!(
        physical,
        PhysicalType::Bool | PhysicalType::Int64 | PhysicalType::UInt64 | PhysicalType::Text
    )
}

fn scalar_uses_physical_types_v2(value: &ScalarValue) -> bool {
    value.physical_type().is_some_and(is_physical_types_v2)
}

fn column_uses_physical_types_v2(column: &ColumnReferenceInspection) -> bool {
    is_physical_types_v2(column.data_type.physical)
}

fn catalog_uses_physical_types_v2(catalog: &CatalogInspection) -> bool {
    catalog.tables.iter().any(|table| {
        table
            .columns
            .iter()
            .any(|column| is_physical_types_v2(column.data_type.physical))
            || match &table.placement {
                TablePlacementInspection::Single => false,
                TablePlacementInspection::RangePartitioned { partitions, .. } => {
                    partitions.iter().any(|partition| {
                        partition
                            .lower
                            .as_ref()
                            .is_some_and(scalar_uses_physical_types_v2)
                            || partition
                                .upper
                                .as_ref()
                                .is_some_and(scalar_uses_physical_types_v2)
                    })
                }
            }
    })
}

fn expression_uses_physical_types_v2(expression: &ExpressionInspection) -> bool {
    is_physical_types_v2(expression.data_type.physical)
        || match &expression.kind {
            ExpressionKindInspection::Column(column) => column_uses_physical_types_v2(column),
            ExpressionKindInspection::Literal(value) => scalar_uses_physical_types_v2(value),
            ExpressionKindInspection::Parameter(_) => false,
            ExpressionKindInspection::Binary { left, right, .. } => {
                expression_uses_physical_types_v2(left) || expression_uses_physical_types_v2(right)
            }
            ExpressionKindInspection::Unary { expression, .. }
            | ExpressionKindInspection::IsNull { expression, .. } => {
                expression_uses_physical_types_v2(expression)
            }
        }
}

fn bound_uses_physical_types_v2(bound: &RangeBoundInspection) -> bool {
    match bound {
        RangeBoundInspection::Unbounded => false,
        RangeBoundInspection::Included(value) | RangeBoundInspection::Excluded(value) => {
            scalar_uses_physical_types_v2(value)
        }
    }
}

fn plan_uses_physical_types_v2(plan: &PlanNodeInspection) -> bool {
    let columns_use_v2 =
        |columns: &[ColumnReferenceInspection]| columns.iter().any(column_uses_physical_types_v2);
    match plan {
        PlanNodeInspection::OneRow => false,
        PlanNodeInspection::SeqScan { columns, .. }
        | PlanNodeInspection::ColumnarScan { columns, .. } => columns_use_v2(columns),
        PlanNodeInspection::IndexScan {
            columns,
            index_column,
            key,
            ..
        } => {
            columns_use_v2(columns)
                || column_uses_physical_types_v2(index_column)
                || scalar_uses_physical_types_v2(key)
        }
        PlanNodeInspection::RangeIndexScan {
            columns,
            index_column,
            range,
            ..
        } => {
            columns_use_v2(columns)
                || column_uses_physical_types_v2(index_column)
                || bound_uses_physical_types_v2(&range.lower)
                || bound_uses_physical_types_v2(&range.upper)
        }
        PlanNodeInspection::PartitionedScan {
            columns,
            partitions,
            ..
        } => {
            columns_use_v2(columns)
                || partitions.iter().any(|partition| match &partition.access {
                    PartitionAccessInspection::SeqScan => false,
                    PartitionAccessInspection::IndexScan { column } => {
                        column_uses_physical_types_v2(column)
                    }
                    PartitionAccessInspection::RangeIndexScan { column, range } => {
                        column_uses_physical_types_v2(column)
                            || bound_uses_physical_types_v2(&range.lower)
                            || bound_uses_physical_types_v2(&range.upper)
                    }
                })
        }
        PlanNodeInspection::NestedLoopJoin {
            predicate,
            left,
            right,
            ..
        } => {
            expression_uses_physical_types_v2(predicate)
                || plan_uses_physical_types_v2(left)
                || plan_uses_physical_types_v2(right)
        }
        PlanNodeInspection::IndexNestedLoopJoin {
            left_key,
            right_key,
            right_columns,
            columns,
            predicate,
            left,
            ..
        } => {
            column_uses_physical_types_v2(left_key)
                || column_uses_physical_types_v2(right_key)
                || columns_use_v2(right_columns)
                || columns_use_v2(columns)
                || expression_uses_physical_types_v2(predicate)
                || plan_uses_physical_types_v2(left)
        }
        PlanNodeInspection::HashJoin {
            left_key,
            right_key,
            predicate,
            left,
            right,
            ..
        } => {
            column_uses_physical_types_v2(left_key)
                || column_uses_physical_types_v2(right_key)
                || expression_uses_physical_types_v2(predicate)
                || plan_uses_physical_types_v2(left)
                || plan_uses_physical_types_v2(right)
        }
        PlanNodeInspection::Filter { predicate, input } => {
            expression_uses_physical_types_v2(predicate) || plan_uses_physical_types_v2(input)
        }
        PlanNodeInspection::Sort { keys, input } => {
            keys.iter()
                .any(|key| column_uses_physical_types_v2(&key.column))
                || plan_uses_physical_types_v2(input)
        }
        PlanNodeInspection::Project { columns, input } => {
            columns_use_v2(columns) || plan_uses_physical_types_v2(input)
        }
        PlanNodeInspection::ScalarProject { expressions, input } => {
            expressions.iter().any(expression_uses_physical_types_v2)
                || plan_uses_physical_types_v2(input)
        }
        PlanNodeInspection::Aggregate {
            group_keys,
            outputs,
            input,
        } => {
            columns_use_v2(group_keys)
                || outputs.iter().any(|output| match output {
                    AggregateOutputInspection::GroupKey(column) => {
                        column_uses_physical_types_v2(column)
                    }
                    AggregateOutputInspection::Aggregate { input, output, .. } => {
                        matches!(
                            input,
                            AggregateInputInspection::Column(column)
                                if column_uses_physical_types_v2(column)
                        ) || is_physical_types_v2(output.data_type.physical)
                    }
                })
                || plan_uses_physical_types_v2(input)
        }
        PlanNodeInspection::Limit { input, .. } => plan_uses_physical_types_v2(input),
    }
}

fn statement_uses_physical_types_v2(statement: &StatementInspection) -> bool {
    let result_uses_v2 = match &statement.result {
        StatementResultInspection::AffectedRows => false,
        StatementResultInspection::Query { columns } => columns
            .iter()
            .any(|column| is_physical_types_v2(column.data_type.physical)),
    };
    result_uses_v2
        || match &statement.plan {
            StatementPlanInspection::Query { root } => plan_uses_physical_types_v2(root),
            StatementPlanInspection::Insert { values, .. } => {
                values.iter().any(expression_uses_physical_types_v2)
            }
            StatementPlanInspection::Update {
                input, assignments, ..
            } => {
                plan_uses_physical_types_v2(input)
                    || assignments.iter().any(|assignment| {
                        column_uses_physical_types_v2(&assignment.column)
                            || expression_uses_physical_types_v2(&assignment.value)
                    })
            }
            StatementPlanInspection::Delete { input, .. } => plan_uses_physical_types_v2(input),
        }
}

fn statement_has_partitions(statement: &StatementInspection) -> bool {
    fn plan_has_partitions(plan: &PlanNodeInspection) -> bool {
        match plan {
            PlanNodeInspection::PartitionedScan { .. } => true,
            PlanNodeInspection::NestedLoopJoin { left, right, .. }
            | PlanNodeInspection::HashJoin { left, right, .. } => {
                plan_has_partitions(left) || plan_has_partitions(right)
            }
            PlanNodeInspection::IndexNestedLoopJoin { left, .. } => plan_has_partitions(left),
            PlanNodeInspection::Filter { input, .. }
            | PlanNodeInspection::Sort { input, .. }
            | PlanNodeInspection::Project { input, .. }
            | PlanNodeInspection::ScalarProject { input, .. }
            | PlanNodeInspection::Aggregate { input, .. }
            | PlanNodeInspection::Limit { input, .. } => plan_has_partitions(input),
            PlanNodeInspection::OneRow
            | PlanNodeInspection::SeqScan { .. }
            | PlanNodeInspection::ColumnarScan { .. }
            | PlanNodeInspection::IndexScan { .. }
            | PlanNodeInspection::RangeIndexScan { .. } => false,
        }
    }
    match &statement.plan {
        StatementPlanInspection::Query { root } => plan_has_partitions(root),
        StatementPlanInspection::Update { input, .. }
        | StatementPlanInspection::Delete { input, .. } => plan_has_partitions(input),
        StatementPlanInspection::Insert { .. } => false,
    }
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
    #[serde(skip_serializing_if = "Option::is_none")]
    placement: Option<TablePlacementJson<'a>>,
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum TablePlacementJson<'a> {
    RangePartitioned {
        partition_key: u32,
        partitions: Vec<RangePartitionJson<'a>>,
    },
}

#[derive(Serialize)]
struct RangePartitionJson<'a> {
    partition_id: u64,
    lower: Option<ScalarJson<'a>>,
    upper: Option<ScalarJson<'a>>,
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
            placement: match &table.placement {
                TablePlacementInspection::Single => None,
                TablePlacementInspection::RangePartitioned {
                    partition_key,
                    partitions,
                } => Some(TablePlacementJson::RangePartitioned {
                    partition_key: partition_key.0,
                    partitions: partitions
                        .iter()
                        .map(|partition| RangePartitionJson {
                            partition_id: partition.partition_id.0,
                            lower: partition.lower.as_ref().map(ScalarJson::from),
                            upper: partition.upper.as_ref().map(ScalarJson::from),
                        })
                        .collect(),
                }),
            },
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
        PhysicalType::Int8 => "int8",
        PhysicalType::Int16 => "int16",
        PhysicalType::Int32 => "int32",
        PhysicalType::Int64 => "int64",
        PhysicalType::Int128 => "int128",
        PhysicalType::UInt8 => "uint8",
        PhysicalType::UInt16 => "uint16",
        PhysicalType::UInt32 => "uint32",
        PhysicalType::UInt64 => "uint64",
        PhysicalType::UInt128 => "uint128",
        PhysicalType::Float32 => "float32",
        PhysicalType::Float64 => "float64",
        PhysicalType::Text => "text",
        PhysicalType::Bytes => "bytes",
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
    OneRow,
    SeqScan {
        binding_id: u32,
        table_id: u64,
        table_name: &'a str,
        columns: Vec<ColumnReferenceJson<'a>>,
    },
    ColumnarScan {
        binding_id: u32,
        table_id: u64,
        table_name: &'a str,
        columns: Vec<ColumnReferenceJson<'a>>,
        projection_id: u64,
        generation: u64,
        source_storage_id: u64,
    },
    IndexScan {
        binding_id: u32,
        table_id: u64,
        table_name: &'a str,
        columns: Vec<ColumnReferenceJson<'a>>,
        index_column: ColumnReferenceJson<'a>,
        key: ScalarJson<'a>,
    },
    RangeIndexScan {
        binding_id: u32,
        table_id: u64,
        table_name: &'a str,
        columns: Vec<ColumnReferenceJson<'a>>,
        index_column: ColumnReferenceJson<'a>,
        lower_bound: RangeBoundJson<'a>,
        upper_bound: RangeBoundJson<'a>,
    },
    PartitionedScan {
        binding_id: u32,
        table_id: u64,
        table_name: &'a str,
        columns: Vec<ColumnReferenceJson<'a>>,
        partition_key: u32,
        total_partitions: usize,
        partitions: Vec<PartitionScanJson<'a>>,
    },
    NestedLoopJoin {
        kind: &'static str,
        predicate: ExpressionJson<'a>,
        left: Box<PlanJson<'a>>,
        right: Box<PlanJson<'a>>,
    },
    IndexNestedLoopJoin {
        kind: &'static str,
        left_key: ColumnReferenceJson<'a>,
        right_key: ColumnReferenceJson<'a>,
        right_binding_id: u32,
        right_table_id: u64,
        right_table_name: &'a str,
        right_access_path: u64,
        right_columns: Vec<ColumnReferenceJson<'a>>,
        columns: Vec<ColumnReferenceJson<'a>>,
        predicate: ExpressionJson<'a>,
        left: Box<PlanJson<'a>>,
    },
    HashJoin {
        kind: &'static str,
        left_key: ColumnReferenceJson<'a>,
        right_key: ColumnReferenceJson<'a>,
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
    ScalarProject {
        expressions: Vec<ExpressionJson<'a>>,
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

#[derive(Serialize)]
struct PartitionScanJson<'a> {
    partition_id: u64,
    access: PartitionAccessJson<'a>,
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum PartitionAccessJson<'a> {
    #[serde(rename = "seq_scan")]
    Seq,
    #[serde(rename = "index_scan")]
    Index { column: ColumnReferenceJson<'a> },
    #[serde(rename = "range_index_scan")]
    RangeIndex {
        column: ColumnReferenceJson<'a>,
        lower_bound: RangeBoundJson<'a>,
        upper_bound: RangeBoundJson<'a>,
    },
}

impl<'a> From<&'a PlanNodeInspection> for PlanJson<'a> {
    fn from(plan: &'a PlanNodeInspection) -> Self {
        match plan {
            PlanNodeInspection::OneRow => Self::OneRow,
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
            PlanNodeInspection::ColumnarScan {
                binding_id,
                table_id,
                table_name,
                columns,
                projection_id,
                generation,
                source_storage_id,
            } => Self::ColumnarScan {
                binding_id: binding_id.0,
                table_id: table_id.0,
                table_name,
                columns: columns.iter().map(ColumnReferenceJson::from).collect(),
                projection_id: projection_id.0,
                generation: generation.0,
                source_storage_id: source_storage_id.0,
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
            PlanNodeInspection::RangeIndexScan {
                binding_id,
                table_id,
                table_name,
                columns,
                index_column,
                range,
            } => Self::RangeIndexScan {
                binding_id: binding_id.0,
                table_id: table_id.0,
                table_name,
                columns: columns.iter().map(ColumnReferenceJson::from).collect(),
                index_column: ColumnReferenceJson::from(index_column),
                lower_bound: RangeBoundJson::from(&range.lower),
                upper_bound: RangeBoundJson::from(&range.upper),
            },
            PlanNodeInspection::PartitionedScan {
                binding_id,
                table_id,
                table_name,
                columns,
                partition_key,
                total_partitions,
                partitions,
            } => Self::PartitionedScan {
                binding_id: binding_id.0,
                table_id: table_id.0,
                table_name,
                columns: columns.iter().map(ColumnReferenceJson::from).collect(),
                partition_key: partition_key.0,
                total_partitions: *total_partitions,
                partitions: partitions
                    .iter()
                    .map(|partition| PartitionScanJson {
                        partition_id: partition.partition_id.0,
                        access: match &partition.access {
                            PartitionAccessInspection::SeqScan => PartitionAccessJson::Seq,
                            PartitionAccessInspection::IndexScan { column } => {
                                PartitionAccessJson::Index {
                                    column: ColumnReferenceJson::from(column),
                                }
                            }
                            PartitionAccessInspection::RangeIndexScan { column, range } => {
                                PartitionAccessJson::RangeIndex {
                                    column: ColumnReferenceJson::from(column),
                                    lower_bound: RangeBoundJson::from(&range.lower),
                                    upper_bound: RangeBoundJson::from(&range.upper),
                                }
                            }
                        },
                    })
                    .collect(),
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
            PlanNodeInspection::IndexNestedLoopJoin {
                kind,
                left_key,
                right_key,
                right_binding_id,
                right_table_id,
                right_table_name,
                right_access_path,
                right_columns,
                columns,
                predicate,
                left,
            } => Self::IndexNestedLoopJoin {
                kind: join_kind(*kind),
                left_key: ColumnReferenceJson::from(left_key),
                right_key: ColumnReferenceJson::from(right_key),
                right_binding_id: right_binding_id.0,
                right_table_id: right_table_id.0,
                right_table_name,
                right_access_path: right_access_path.0,
                right_columns: right_columns
                    .iter()
                    .map(ColumnReferenceJson::from)
                    .collect(),
                columns: columns.iter().map(ColumnReferenceJson::from).collect(),
                predicate: ExpressionJson::from(predicate),
                left: Box::new(PlanJson::from(left.as_ref())),
            },
            PlanNodeInspection::HashJoin {
                kind,
                left_key,
                right_key,
                predicate,
                left,
                right,
            } => Self::HashJoin {
                kind: join_kind(*kind),
                left_key: ColumnReferenceJson::from(left_key),
                right_key: ColumnReferenceJson::from(right_key),
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
            PlanNodeInspection::ScalarProject { expressions, input } => Self::ScalarProject {
                expressions: expressions.iter().map(ExpressionJson::from).collect(),
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

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum RangeBoundJson<'a> {
    Unbounded,
    Included { value: ScalarJson<'a> },
    Excluded { value: ScalarJson<'a> },
}

impl<'a> From<&'a RangeBoundInspection> for RangeBoundJson<'a> {
    fn from(bound: &'a RangeBoundInspection) -> Self {
        match bound {
            RangeBoundInspection::Unbounded => Self::Unbounded,
            RangeBoundInspection::Included(value) => Self::Included {
                value: ScalarJson::from(value),
            },
            RangeBoundInspection::Excluded(value) => Self::Excluded {
                value: ScalarJson::from(value),
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
    Parameter {
        id: u32,
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
            ExpressionKindInspection::Parameter(id) => Self::Parameter { id: id.0 },
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
    Int8 {
        value: i8,
    },
    Int16 {
        value: i16,
    },
    Int32 {
        value: i32,
    },
    Int128 {
        value: String,
    },
    UInt8 {
        value: u8,
    },
    UInt16 {
        value: u16,
    },
    UInt32 {
        value: u32,
    },
    #[serde(rename = "uint64")]
    UInt64 {
        value: u64,
    },
    UInt128 {
        value: String,
    },
    Float32 {
        bits: String,
    },
    Float64 {
        bits: String,
    },
    Text {
        value: &'a str,
    },
    Bytes {
        hex: String,
    },
}

impl<'a> From<&'a ScalarValue> for ScalarJson<'a> {
    fn from(value: &'a ScalarValue) -> Self {
        match value {
            ScalarValue::Null => Self::Null,
            ScalarValue::Bool(value) => Self::Bool { value: *value },
            ScalarValue::Int8(value) => Self::Int8 { value: *value },
            ScalarValue::Int16(value) => Self::Int16 { value: *value },
            ScalarValue::Int32(value) => Self::Int32 { value: *value },
            ScalarValue::Int64(value) => Self::Int64 { value: *value },
            ScalarValue::Int128(value) => Self::Int128 {
                value: value.to_string(),
            },
            ScalarValue::UInt8(value) => Self::UInt8 { value: *value },
            ScalarValue::UInt16(value) => Self::UInt16 { value: *value },
            ScalarValue::UInt32(value) => Self::UInt32 { value: *value },
            ScalarValue::UInt64(value) => Self::UInt64 { value: *value },
            ScalarValue::UInt128(value) => Self::UInt128 {
                value: value.to_string(),
            },
            ScalarValue::Float32(value) => Self::Float32 {
                bits: format!("{:08x}", value.to_bits()),
            },
            ScalarValue::Float64(value) => Self::Float64 {
                bits: format!("{:016x}", value.to_bits()),
            },
            ScalarValue::Text(value) => Self::Text { value },
            ScalarValue::Bytes(value) => Self::Bytes {
                hex: value.iter().map(|byte| format!("{byte:02x}")).collect(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use netbadb_sdk::inspection::{
        AggregateInputInspection, AggregateOutputInspection, BinaryOpInspection, CatalogInspection,
        ColumnInspection, ColumnReferenceInspection, ExpressionInspection,
        ExpressionKindInspection, IndexInspection, IndexKindInspection, IndexRangeInspection,
        IndexStatisticsInspection, PartitionAccessInspection, PartitionScanInspection,
        PlanNodeInspection, RangeBoundInspection, RangePartitionInspection, ResultFieldInspection,
        SourceColumnInspection, StatementAccessInspection, StatementInspection, StatementKind,
        StatementPlanInspection, StatementResultInspection, TableInspection,
        TablePlacementInspection, TableStatisticsInspection,
    };
    use netbadb_sdk::{
        AccessPathId, ColumnId, ColumnarGeneration, ColumnarProjectionId, PartitionId,
        PhysicalType, RelationBindingId, ScalarValue, SchemaFingerprint, SemanticType, StorageId,
        TableId,
    };

    use super::{render_catalog, render_statement};

    fn column(id: u32, name: &str, physical: PhysicalType) -> ColumnReferenceInspection {
        bound_column(0, 1, "users", id, name, physical)
    }

    fn bound_column(
        binding_id: u32,
        table_id: u64,
        relation_name: &str,
        id: u32,
        name: &str,
        physical: PhysicalType,
    ) -> ColumnReferenceInspection {
        ColumnReferenceInspection {
            binding_id: RelationBindingId(binding_id),
            table_id: TableId(table_id),
            column_id: ColumnId(id),
            relation_name: relation_name.into(),
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
    fn catalog_json_v3_matches_the_golden_contract() {
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
                    name: None,
                    table_id: TableId(1),
                    column_id: ColumnId(1),
                    column_name: "id".into(),
                    kind: IndexKindInspection::BTree,
                    unique: false,
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
                placement: TablePlacementInspection::Single,
            }],
        };
        assert_eq!(
            render_catalog(&catalog).unwrap(),
            include_str!("../tests/golden/catalog-v3.json")
        );
    }

    #[test]
    fn catalog_with_physical_types_v2_uses_json_v7() {
        let catalog = CatalogInspection {
            tables: vec![TableInspection {
                table_id: TableId(1),
                name: "measurements".into(),
                fingerprint: SchemaFingerprint::from_bytes([0x17; 32]),
                columns: vec![ColumnInspection {
                    column_id: ColumnId(1),
                    name: "value".into(),
                    data_type: SemanticType::physical(PhysicalType::Int8),
                    nullable: false,
                    primary_key: false,
                }],
                indexes: Vec::new(),
                statistics: None,
                placement: TablePlacementInspection::Single,
            }],
        };
        let json = render_catalog(&catalog).unwrap();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["version"], 7);
        assert_eq!(
            value["catalog"]["tables"][0]["columns"][0]["data_type"]["physical"],
            "int8"
        );
        assert!(json.ends_with('\n'));
    }

    #[test]
    fn partition_catalog_and_plan_use_json_v4_without_changing_v3() {
        let catalog = CatalogInspection {
            tables: vec![TableInspection {
                table_id: TableId(1),
                name: "events".into(),
                fingerprint: SchemaFingerprint::from_bytes([0xcd; 32]),
                columns: vec![ColumnInspection {
                    column_id: ColumnId(1),
                    name: "key".into(),
                    data_type: SemanticType::physical(PhysicalType::Int64),
                    nullable: false,
                    primary_key: false,
                }],
                indexes: Vec::new(),
                statistics: None,
                placement: TablePlacementInspection::RangePartitioned {
                    partition_key: ColumnId(1),
                    partitions: vec![RangePartitionInspection {
                        partition_id: PartitionId(7),
                        lower: Some(ScalarValue::Int64(0)),
                        upper: Some(ScalarValue::Int64(10)),
                    }],
                },
            }],
        };
        let catalog_json: serde_json::Value =
            serde_json::from_str(&render_catalog(&catalog).unwrap()).unwrap();
        assert_eq!(catalog_json["version"], 4);
        assert_eq!(
            catalog_json["catalog"]["tables"][0]["placement"]["kind"],
            "range_partitioned"
        );
        assert_eq!(
            catalog_json["catalog"]["tables"][0]["placement"]["partitions"][0]["partition_id"],
            7
        );

        let key = bound_column(0, 1, "events", 1, "key", PhysicalType::Int64);
        let inspection = statement(
            PlanNodeInspection::PartitionedScan {
                binding_id: RelationBindingId(0),
                table_id: TableId(1),
                table_name: "events".into(),
                columns: vec![key.clone()],
                partition_key: ColumnId(1),
                total_partitions: 3,
                partitions: vec![PartitionScanInspection {
                    partition_id: PartitionId(7),
                    access: PartitionAccessInspection::IndexScan {
                        column: key.clone(),
                    },
                }],
            },
            vec![result(&key)],
        );
        let statement_json: serde_json::Value =
            serde_json::from_str(&render_statement(&inspection).unwrap()).unwrap();
        assert_eq!(statement_json["version"], 4);
        let root = &statement_json["statement"]["plan"]["root"];
        assert_eq!(root["operator"], "partitioned_scan");
        assert_eq!(root["total_partitions"], 3);
        assert_eq!(root["partitions"][0]["partition_id"], 7);
        assert_eq!(root["partitions"][0]["access"]["kind"], "index_scan");
    }

    #[test]
    fn seq_scan_statement_json_v3_matches_the_golden_contract() {
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
            include_str!("../tests/golden/statement-seq-scan-v3.json")
        );
    }

    #[test]
    fn columnar_scan_advances_statement_json_to_v6_with_stable_identities() {
        let id = column(1, "id", PhysicalType::UInt64);
        let inspection = statement(
            PlanNodeInspection::ColumnarScan {
                binding_id: RelationBindingId(0),
                table_id: TableId(1),
                table_name: "users".into(),
                columns: vec![id.clone()],
                projection_id: ColumnarProjectionId(11),
                generation: ColumnarGeneration(3),
                source_storage_id: StorageId(7),
            },
            vec![result(&id)],
        );
        let json: serde_json::Value =
            serde_json::from_str(&render_statement(&inspection).unwrap()).unwrap();
        assert_eq!(json["version"], 6);
        let root = &json["statement"]["plan"]["root"];
        assert_eq!(root["operator"], "columnar_scan");
        assert_eq!(root["projection_id"], 11);
        assert_eq!(root["generation"], 3);
        assert_eq!(root["source_storage_id"], 7);
    }

    #[test]
    fn physical_types_v2_raise_statement_structural_versions_to_v7() {
        for (scalar, physical, expected_shape) in [
            (
                ScalarValue::Float64(netbadb_sdk::Float64Value::new(-0.0)),
                PhysicalType::Float64,
                serde_json::json!({"kind":"float64","bits":"0000000000000000"}),
            ),
            (
                ScalarValue::Int128(i128::MIN),
                PhysicalType::Int128,
                serde_json::json!({"kind":"int128","value":i128::MIN.to_string()}),
            ),
            (
                ScalarValue::Bytes(vec![0, 0xff, 0x80]),
                PhysicalType::Bytes,
                serde_json::json!({"kind":"bytes","hex":"00ff80"}),
            ),
        ] {
            let inspection = statement(
                PlanNodeInspection::ScalarProject {
                    expressions: vec![literal(scalar, physical)],
                    input: Box::new(PlanNodeInspection::OneRow),
                },
                vec![ResultFieldInspection {
                    name: "value".into(),
                    data_type: SemanticType::physical(physical),
                    nullable: false,
                    source: None,
                }],
            );
            let value: serde_json::Value =
                serde_json::from_str(&render_statement(&inspection).unwrap()).unwrap();
            assert_eq!(value["version"], 7);
            assert_eq!(
                value["statement"]["plan"]["root"]["expressions"][0]["value"],
                expected_shape
            );
        }

        let bytes = column(1, "payload", PhysicalType::Bytes);
        let inspection = statement(
            PlanNodeInspection::ColumnarScan {
                binding_id: RelationBindingId(0),
                table_id: TableId(1),
                table_name: "events".into(),
                columns: vec![bytes.clone()],
                projection_id: ColumnarProjectionId(12),
                generation: ColumnarGeneration(4),
                source_storage_id: StorageId(8),
            },
            vec![result(&bytes)],
        );
        let value: serde_json::Value =
            serde_json::from_str(&render_statement(&inspection).unwrap()).unwrap();
        assert_eq!(value["version"], 7);
    }

    fn join_statement(hash: bool) -> StatementInspection {
        let left = bound_column(0, 1, "e", 1, "id", PhysicalType::Int64);
        let right = bound_column(1, 1, "m", 1, "id", PhysicalType::Int64);
        let predicate = expression(
            ExpressionKindInspection::Binary {
                operator: BinaryOpInspection::Eq,
                left: Box::new(expression(
                    ExpressionKindInspection::Column(left.clone()),
                    PhysicalType::Int64,
                )),
                right: Box::new(expression(
                    ExpressionKindInspection::Column(right.clone()),
                    PhysicalType::Int64,
                )),
            },
            PhysicalType::Bool,
        );
        let left_scan = PlanNodeInspection::SeqScan {
            binding_id: RelationBindingId(0),
            table_id: TableId(1),
            table_name: "employees".into(),
            columns: vec![left.clone()],
        };
        let right_scan = PlanNodeInspection::SeqScan {
            binding_id: RelationBindingId(1),
            table_id: TableId(1),
            table_name: "employees".into(),
            columns: vec![right.clone()],
        };
        let root = if hash {
            PlanNodeInspection::HashJoin {
                kind: netbadb_sdk::inspection::JoinKindInspection::Inner,
                left_key: left.clone(),
                right_key: right,
                predicate,
                left: Box::new(left_scan),
                right: Box::new(right_scan),
            }
        } else {
            PlanNodeInspection::NestedLoopJoin {
                kind: netbadb_sdk::inspection::JoinKindInspection::Inner,
                predicate,
                left: Box::new(left_scan),
                right: Box::new(right_scan),
            }
        };
        statement(root, vec![result(&left)])
    }

    #[test]
    fn nested_loop_join_statement_json_v3_matches_the_golden_contract() {
        assert_eq!(
            render_statement(&join_statement(false)).unwrap(),
            include_str!("../tests/golden/statement-nested-loop-join-v3.json")
        );
    }

    #[test]
    fn hash_join_statement_json_v3_matches_the_golden_contract() {
        assert_eq!(
            render_statement(&join_statement(true)).unwrap(),
            include_str!("../tests/golden/statement-hash-join-v3.json")
        );
    }

    #[test]
    fn index_nested_loop_join_advances_statement_json_to_v5() {
        let left = bound_column(10, 1, "l", 2, "join_key", PhysicalType::Int64);
        let right = bound_column(20, 2, "r", 2, "join_key", PhysicalType::Int64);
        let predicate = expression(
            ExpressionKindInspection::Binary {
                operator: BinaryOpInspection::Eq,
                left: Box::new(expression(
                    ExpressionKindInspection::Column(left.clone()),
                    PhysicalType::Int64,
                )),
                right: Box::new(expression(
                    ExpressionKindInspection::Column(right.clone()),
                    PhysicalType::Int64,
                )),
            },
            PhysicalType::Bool,
        );
        let inspection = statement(
            PlanNodeInspection::IndexNestedLoopJoin {
                kind: netbadb_sdk::inspection::JoinKindInspection::Inner,
                left_key: left.clone(),
                right_key: right.clone(),
                right_binding_id: RelationBindingId(20),
                right_table_id: TableId(2),
                right_table_name: "right_rows".into(),
                right_access_path: AccessPathId(72),
                right_columns: vec![right.clone()],
                columns: vec![left.clone(), right],
                predicate,
                left: Box::new(PlanNodeInspection::SeqScan {
                    binding_id: RelationBindingId(10),
                    table_id: TableId(1),
                    table_name: "left_rows".into(),
                    columns: vec![left.clone()],
                }),
            },
            vec![result(&left)],
        );
        let json: serde_json::Value =
            serde_json::from_str(&render_statement(&inspection).unwrap()).unwrap();
        assert_eq!(json["version"], 5);
        let root = &json["statement"]["plan"]["root"];
        assert_eq!(root["operator"], "index_nested_loop_join");
        assert_eq!(root["right_table_id"], 2);
        assert_eq!(root["right_access_path"], 72);
        assert_eq!(root["left"]["operator"], "seq_scan");
    }

    #[test]
    fn index_filter_statement_json_v3_matches_the_golden_contract() {
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
            include_str!("../tests/golden/statement-index-filter-v3.json")
        );
    }

    #[test]
    fn range_index_statement_json_v3_matches_the_golden_contract() {
        let id = column(1, "id", PhysicalType::Int64);
        let inspection = statement(
            PlanNodeInspection::RangeIndexScan {
                binding_id: RelationBindingId(0),
                table_id: TableId(1),
                table_name: "users".into(),
                columns: vec![id.clone()],
                index_column: id.clone(),
                range: IndexRangeInspection {
                    lower: RangeBoundInspection::Included(ScalarValue::Int64(5_000)),
                    upper: RangeBoundInspection::Excluded(ScalarValue::Int64(5_100)),
                },
            },
            vec![result(&id)],
        );
        assert_eq!(
            render_statement(&inspection).unwrap(),
            include_str!("../tests/golden/statement-range-index-v3.json")
        );
    }

    #[test]
    fn aggregate_statement_json_v3_matches_the_golden_contract() {
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
            include_str!("../tests/golden/statement-aggregate-v3.json")
        );
    }

    #[test]
    fn dml_statement_json_v3_matches_the_golden_contract() {
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
            include_str!("../tests/golden/statement-dml-v3.json")
        );
    }
}
