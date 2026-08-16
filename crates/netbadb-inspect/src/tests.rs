use netbadb_schema::SchemaFingerprint;
use netbadb_types::{
    ColumnId, PhysicalType, RelationBindingId, ScalarValue, SemanticType, TableId,
};

use super::*;

fn column(
    binding: u32,
    table: u64,
    relation: &str,
    id: u32,
    name: &str,
    physical: PhysicalType,
) -> ColumnReferenceInspection {
    ColumnReferenceInspection {
        binding_id: RelationBindingId(binding),
        table_id: TableId(table),
        column_id: ColumnId(id),
        relation_name: relation.into(),
        name: name.into(),
        data_type: SemanticType::physical(physical),
        nullable: false,
    }
}

fn literal(value: ScalarValue, physical: PhysicalType) -> ExpressionInspection {
    ExpressionInspection {
        kind: ExpressionKindInspection::Literal(value),
        data_type: SemanticType::physical(physical),
        nullable: false,
    }
}

fn column_expression(column: &ColumnReferenceInspection) -> ExpressionInspection {
    ExpressionInspection {
        kind: ExpressionKindInspection::Column(column.clone()),
        data_type: column.data_type.clone(),
        nullable: column.nullable,
    }
}

fn query_statement(root: PlanNodeInspection) -> StatementInspection {
    StatementInspection {
        kind: StatementKind::Query,
        access: StatementAccessInspection {
            read_tables: vec![TableId(1)],
            write_tables: Vec::new(),
        },
        result: StatementResultInspection::Query {
            columns: vec![ResultFieldInspection {
                name: "id".into(),
                data_type: SemanticType::named("UserId", PhysicalType::Int64),
                nullable: false,
                source: Some(SourceColumnInspection {
                    binding_id: RelationBindingId(0),
                    table_id: TableId(1),
                    column_id: ColumnId(1),
                    relation_name: "users".into(),
                    name: "id".into(),
                }),
            }],
        },
        plan: StatementPlanInspection::Query { root },
    }
}

#[test]
fn catalog_renderer_is_explicit_and_deterministic() {
    let catalog = CatalogInspection {
        tables: vec![TableInspection {
            table_id: TableId(1),
            name: "users".into(),
            fingerprint: SchemaFingerprint::from_bytes([0xab; 32]),
            columns: vec![
                ColumnInspection {
                    column_id: ColumnId(1),
                    name: "id".into(),
                    data_type: SemanticType::named("UserId", PhysicalType::Int64),
                    nullable: false,
                    primary_key: true,
                },
                ColumnInspection {
                    column_id: ColumnId(2),
                    name: "name".into(),
                    data_type: SemanticType::physical(PhysicalType::Text),
                    nullable: true,
                    primary_key: false,
                },
            ],
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
    let expected = concat!(
        "Catalog\n",
        "Table users #1\n",
        "  fingerprint: abababababababababababababababababababababababababababababababab\n",
        "  columns:\n",
        "    #1 id UserId(INT64) NOT NULL PRIMARY KEY\n",
        "    #2 name TEXT NULL\n",
        "  indexes:\n",
        "    [0] column #1 id\n",
        "      statistics: distinct_non_null_keys=8 null_count=0 tree_height=1\n",
        "  statistics:\n",
        "    rows: 8\n",
        "    managed_pages: 2\n",
    );
    assert_eq!(render_catalog(&catalog), expected);
    assert_eq!(render_catalog(&catalog), render_catalog(&catalog));
}

#[test]
fn statement_renderer_covers_seq_and_index_filter_plans() {
    let id = column(0, 1, "users", 1, "id", PhysicalType::Int64);
    let seq = query_statement(PlanNodeInspection::Project {
        columns: vec![id.clone()],
        input: Box::new(PlanNodeInspection::SeqScan {
            binding_id: RelationBindingId(0),
            table_id: TableId(1),
            table_name: "users".into(),
            columns: vec![id.clone()],
        }),
    });
    assert_eq!(
        render_statement(&seq),
        concat!(
            "Query\n",
            "  access: read=[#1] write=[]\n",
            "  result:\n",
            "    [0] id UserId(INT64) NOT NULL source=users#0.id#1@table#1\n",
            "  plan:\n",
            "    Project columns=[users#0.id#1@table#1]\n",
            "      SeqScan table=users#1 binding=#0 columns=[users#0.id#1@table#1]\n",
        )
    );

    let predicate = ExpressionInspection {
        kind: ExpressionKindInspection::Binary {
            operator: BinaryOpInspection::Eq,
            left: Box::new(column_expression(&id)),
            right: Box::new(literal(ScalarValue::Int64(42), PhysicalType::Int64)),
        },
        data_type: SemanticType::physical(PhysicalType::Bool),
        nullable: false,
    };
    let indexed = query_statement(PlanNodeInspection::Filter {
        predicate,
        input: Box::new(PlanNodeInspection::IndexScan {
            binding_id: RelationBindingId(0),
            table_id: TableId(1),
            table_name: "users".into(),
            columns: vec![id.clone()],
            index_column: id,
            key: ScalarValue::Int64(42),
        }),
    });
    assert_eq!(
        render_statement(&indexed),
        concat!(
            "Query\n",
            "  access: read=[#1] write=[]\n",
            "  result:\n",
            "    [0] id UserId(INT64) NOT NULL source=users#0.id#1@table#1\n",
            "  plan:\n",
            "    Filter predicate=Eq(users#0.id#1@table#1, INT64(42))\n",
            "      IndexScan table=users#1 binding=#0 columns=[users#0.id#1@table#1] index=id#1 key=INT64(42)\n",
        )
    );
}

#[test]
fn statement_renderer_covers_join_aggregate_and_dml() {
    let employee = column(0, 1, "e", 1, "id", PhysicalType::Int64);
    let manager = column(1, 1, "m", 1, "id", PhysicalType::Int64);
    let predicate = ExpressionInspection {
        kind: ExpressionKindInspection::Binary {
            operator: BinaryOpInspection::Eq,
            left: Box::new(column_expression(&employee)),
            right: Box::new(column_expression(&manager)),
        },
        data_type: SemanticType::physical(PhysicalType::Bool),
        nullable: false,
    };
    let join = PlanNodeInspection::NestedLoopJoin {
        kind: JoinKindInspection::Inner,
        predicate,
        left: Box::new(PlanNodeInspection::SeqScan {
            binding_id: RelationBindingId(0),
            table_id: TableId(1),
            table_name: "employees".into(),
            columns: vec![employee.clone()],
        }),
        right: Box::new(PlanNodeInspection::SeqScan {
            binding_id: RelationBindingId(1),
            table_id: TableId(1),
            table_name: "employees".into(),
            columns: vec![manager],
        }),
    };
    let join_text = render_statement(&query_statement(join));
    assert_eq!(
        join_text,
        concat!(
            "Query\n",
            "  access: read=[#1] write=[]\n",
            "  result:\n",
            "    [0] id UserId(INT64) NOT NULL source=users#0.id#1@table#1\n",
            "  plan:\n",
            "    NestedLoopJoin kind=Inner predicate=Eq(e#0.id#1@table#1, m#1.id#1@table#1)\n",
            "      left:\n",
            "        SeqScan table=employees#1 binding=#0 columns=[e#0.id#1@table#1]\n",
            "      right:\n",
            "        SeqScan table=employees#1 binding=#1 columns=[m#1.id#1@table#1]\n",
        )
    );

    let aggregate = PlanNodeInspection::Aggregate {
        group_keys: vec![employee.clone()],
        outputs: vec![
            AggregateOutputInspection::GroupKey(employee.clone()),
            AggregateOutputInspection::Aggregate {
                function: AggregateFunctionInspection::Count,
                input: AggregateInputInspection::All,
                output: ResultFieldInspection {
                    name: "count(*)".into(),
                    data_type: SemanticType::physical(PhysicalType::UInt64),
                    nullable: false,
                    source: None,
                },
            },
        ],
        input: Box::new(PlanNodeInspection::SeqScan {
            binding_id: RelationBindingId(0),
            table_id: TableId(1),
            table_name: "employees".into(),
            columns: vec![employee],
        }),
    };
    let aggregate_text = render_statement(&query_statement(aggregate));
    assert_eq!(
        aggregate_text,
        concat!(
            "Query\n",
            "  access: read=[#1] write=[]\n",
            "  result:\n",
            "    [0] id UserId(INT64) NOT NULL source=users#0.id#1@table#1\n",
            "  plan:\n",
            "    Aggregate group_keys=[e#0.id#1@table#1] outputs=[GroupKey(e#0.id#1@table#1), Count(All) -> count(*):UINT64:NOT NULL]\n",
            "      SeqScan table=employees#1 binding=#0 columns=[e#0.id#1@table#1]\n",
        )
    );

    let insert = StatementInspection {
        kind: StatementKind::Insert,
        access: StatementAccessInspection {
            read_tables: Vec::new(),
            write_tables: vec![TableId(1)],
        },
        result: StatementResultInspection::AffectedRows,
        plan: StatementPlanInspection::Insert {
            table_id: TableId(1),
            table_name: "values".into(),
            values: vec![
                literal(ScalarValue::Null, PhysicalType::Text),
                literal(ScalarValue::Bool(true), PhysicalType::Bool),
                literal(ScalarValue::Int64(-1), PhysicalType::Int64),
                literal(ScalarValue::UInt64(42), PhysicalType::UInt64),
                literal(
                    ScalarValue::Text("quote\" slash\\ line\n\ttab".into()),
                    PhysicalType::Text,
                ),
            ],
        },
    };
    assert_eq!(
        render_statement(&insert),
        concat!(
            "Insert\n",
            "  access: read=[] write=[#1]\n",
            "  result: AffectedRows\n",
            "  plan:\n",
            "    Insert table=values#1 values=[NULL, BOOL(true), INT64(-1), UINT64(42), TEXT(\"quote\\\" slash\\\\ line\\n\\ttab\")]\n",
        )
    );
}

#[test]
fn renderers_escape_free_form_names_without_injecting_lines_or_controls() {
    let catalog = CatalogInspection {
        tables: vec![TableInspection {
            table_id: TableId(1),
            name: "users\n\u{1b}[31m".into(),
            fingerprint: SchemaFingerprint::from_bytes([0; 32]),
            columns: vec![ColumnInspection {
                column_id: ColumnId(1),
                name: "id\t\\\"".into(),
                data_type: SemanticType::named("User\rId", PhysicalType::Int64),
                nullable: false,
                primary_key: false,
            }],
            indexes: vec![IndexInspection {
                column_id: ColumnId(1),
                column_name: "id\t\\\"".into(),
                registration_order: 0,
                statistics: None,
            }],
            statistics: None,
        }],
    };
    let catalog_text = render_catalog(&catalog);
    assert_eq!(catalog_text.lines().count(), 9);
    assert!(catalog_text.contains("Table users\\n\\u{1b}[31m #1"));
    assert!(catalog_text.contains("#1 id\\t\\\\\\\" User\\rId(INT64) NOT NULL"));
    assert!(catalog_text.contains("[0] column #1 id\\t\\\\\\\""));
    assert!(!catalog_text.contains('\u{1b}'));

    let inspected_column = column(
        0,
        1,
        "users\n\u{1b}[31m",
        1,
        "id\t\\\"",
        PhysicalType::Int64,
    );
    let statement = StatementInspection {
        kind: StatementKind::Insert,
        access: StatementAccessInspection {
            read_tables: Vec::new(),
            write_tables: vec![TableId(1)],
        },
        result: StatementResultInspection::Query {
            columns: vec![ResultFieldInspection {
                name: "result\nname".into(),
                data_type: SemanticType::named("User\rId", PhysicalType::Int64),
                nullable: false,
                source: Some(SourceColumnInspection {
                    binding_id: inspected_column.binding_id,
                    table_id: inspected_column.table_id,
                    column_id: inspected_column.column_id,
                    relation_name: inspected_column.relation_name.clone(),
                    name: inspected_column.name.clone(),
                }),
            }],
        },
        plan: StatementPlanInspection::Insert {
            table_id: TableId(1),
            table_name: "users\n\u{1b}[31m".into(),
            values: vec![column_expression(&inspected_column)],
        },
    };
    let statement_text = render_statement(&statement);
    assert_eq!(statement_text.lines().count(), 6);
    assert!(statement_text.contains("result\\nname User\\rId(INT64)"));
    assert!(statement_text.contains("users\\n\\u{1b}[31m#0.id\\t\\\\\\\"#1@table#1"));
    assert!(statement_text.contains("Insert table=users\\n\\u{1b}[31m#1"));
    assert!(!statement_text.contains('\u{1b}'));
}
