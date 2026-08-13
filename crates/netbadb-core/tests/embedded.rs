use netbadb_core::{Database, DatabaseError, ExecutionResult, TransactionState};
use netbadb_executor::ExecutionError;
use netbadb_schema::{ColumnDef, SchemaError, TableDef, TypeSpec};
use netbadb_storage::{PageError, StorageError};
use netbadb_types::{ColumnId, PhysicalType, ScalarValue, TableId};

fn users() -> TableDef {
    TableDef::new(
        TableId(1),
        "users",
        vec![
            ColumnDef::new(ColumnId(1), "id", TypeSpec::Physical(PhysicalType::Int64)),
            ColumnDef::new(ColumnId(2), "name", TypeSpec::Physical(PhysicalType::Text)),
        ],
    )
}

fn nullable_users() -> TableDef {
    TableDef::new(
        TableId(2),
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
            ColumnDef::new(
                ColumnId(4),
                "active",
                TypeSpec::Physical(PhysicalType::Bool),
            )
            .nullable(true),
        ],
    )
}

fn pairs() -> TableDef {
    TableDef::new(
        TableId(3),
        "pairs",
        vec![
            ColumnDef::new(ColumnId(1), "id", TypeSpec::Physical(PhysicalType::Int64)),
            ColumnDef::new(ColumnId(2), "a", TypeSpec::Physical(PhysicalType::Int64)),
            ColumnDef::new(ColumnId(3), "b", TypeSpec::Physical(PhysicalType::Int64)),
        ],
    )
}

fn variable_rows() -> TableDef {
    TableDef::new(
        TableId(4),
        "variable_rows",
        vec![
            ColumnDef::new(ColumnId(1), "id", TypeSpec::Physical(PhysicalType::Int64)),
            ColumnDef::new(
                ColumnId(2),
                "target",
                TypeSpec::Physical(PhysicalType::Text),
            ),
            ColumnDef::new(
                ColumnId(3),
                "source",
                TypeSpec::Physical(PhysicalType::Text),
            ),
        ],
    )
}

fn affected(result: ExecutionResult) -> u64 {
    match result {
        ExecutionResult::AffectedRows(rows) => rows,
        ExecutionResult::Query(_) => panic!("expected affected rows"),
    }
}

fn cleanup(path: &std::path::Path) {
    let _ = std::fs::remove_file(path);
    let wal = netbadb_storage::wal_path(path);
    let _ = std::fs::remove_file(netbadb_storage::wal_alternate_path(&wal));
    let _ = std::fs::remove_file(wal);
}

#[test]
fn public_embedded_api_round_trips_rows_and_queries() {
    let path = std::env::temp_dir().join(format!("netbadb-integration-{}", std::process::id()));
    let mut database = Database::create(&path, users()).expect("create database");
    database
        .insert(&[ScalarValue::Int64(1), ScalarValue::Text("Ada".into())])
        .expect("insert");

    let result = database
        .query("SELECT id, name FROM users WHERE id = 1 LIMIT 1")
        .expect("query");
    assert_eq!(
        result.rows,
        vec![vec![ScalarValue::Int64(1), ScalarValue::Text("Ada".into())]]
    );
    database.close().expect("close database");
    cleanup(&path);
}

#[test]
fn order_by_unprojected_nullable_and_semantic_keys_survives_reopen() {
    let path = std::env::temp_dir().join(format!(
        "netbadb-order-by-{}-{:?}.db",
        std::process::id(),
        std::thread::current().id()
    ));
    let table = TableDef::new(
        TableId(91),
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
                "score",
                TypeSpec::Physical(PhysicalType::Int64),
            )
            .nullable(true),
        ],
    );
    let mut database = Database::create(&path, table.clone()).expect("create database");
    for row in [
        vec![
            ScalarValue::UInt64(1),
            ScalarValue::Text("A".into()),
            ScalarValue::Null,
        ],
        vec![
            ScalarValue::UInt64(2),
            ScalarValue::Text("B".into()),
            ScalarValue::Int64(20),
        ],
        vec![
            ScalarValue::UInt64(3),
            ScalarValue::Text("C".into()),
            ScalarValue::Int64(10),
        ],
        vec![
            ScalarValue::UInt64(4),
            ScalarValue::Text("D".into()),
            ScalarValue::Null,
        ],
        vec![
            ScalarValue::UInt64(5),
            ScalarValue::Text("E".into()),
            ScalarValue::Int64(20),
        ],
    ] {
        database.insert(&row).expect("insert row");
    }

    let source = "SELECT name FROM users ORDER BY score DESC NULLS LAST, id ASC LIMIT 4";
    let result = database.query(source).expect("ordered query");
    assert_eq!(result.columns.len(), 1);
    assert_eq!(result.columns[0].name, "name");
    assert_eq!(result.columns[0].data_type.physical, PhysicalType::Text);
    assert_eq!(
        result.rows,
        ["B", "E", "C", "A"]
            .map(|name| vec![ScalarValue::Text(name.into())])
            .to_vec()
    );

    let by_id_source = "SELECT name FROM users ORDER BY id DESC LIMIT 2";
    let by_id = database.query(by_id_source).expect("unprojected ID sort");
    assert_eq!(
        by_id.rows,
        ["E", "D"]
            .map(|name| vec![ScalarValue::Text(name.into())])
            .to_vec()
    );

    let semantic = database
        .query("SELECT id FROM users ORDER BY id DESC LIMIT 1")
        .expect("semantic key query");
    assert_eq!(
        semantic.columns[0].data_type.name.as_deref(),
        Some("UserId")
    );
    assert_eq!(semantic.rows, vec![vec![ScalarValue::UInt64(5)]]);
    database.close().expect("close database");

    let mut reopened = Database::open(&path, table).expect("reopen database");
    assert_eq!(
        reopened
            .query(source)
            .expect("ordered query after reopen")
            .rows,
        result.rows
    );
    assert_eq!(
        reopened
            .query(by_id_source)
            .expect("unprojected ID sort after reopen")
            .rows,
        by_id.rows
    );
    reopened.close().expect("close reopened database");
    cleanup(&path);
}

#[test]
fn typed_global_aggregates_cover_null_empty_types_limit_and_reopen() {
    let path = std::env::temp_dir().join(format!(
        "netbadb-aggregate-{}-{:?}.db",
        std::process::id(),
        std::thread::current().id()
    ));
    let table = TableDef::new(
        TableId(94),
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
            ColumnDef::new(
                ColumnId(2),
                "score",
                TypeSpec::Physical(PhysicalType::Int64),
            )
            .nullable(true),
            ColumnDef::new(ColumnId(3), "name", TypeSpec::Physical(PhysicalType::Text)),
            ColumnDef::new(
                ColumnId(4),
                "active",
                TypeSpec::Physical(PhysicalType::Bool),
            ),
        ],
    );
    let mut database = Database::create(&path, table.clone()).expect("create aggregate database");
    for row in [
        vec![
            ScalarValue::UInt64(1),
            ScalarValue::Null,
            ScalarValue::Text("A".into()),
            ScalarValue::Bool(true),
        ],
        vec![
            ScalarValue::UInt64(2),
            ScalarValue::Int64(20),
            ScalarValue::Text("B".into()),
            ScalarValue::Bool(true),
        ],
        vec![
            ScalarValue::UInt64(3),
            ScalarValue::Int64(10),
            ScalarValue::Text("C".into()),
            ScalarValue::Bool(false),
        ],
        vec![
            ScalarValue::UInt64(4),
            ScalarValue::Null,
            ScalarValue::Text("D".into()),
            ScalarValue::Bool(true),
        ],
        vec![
            ScalarValue::UInt64(5),
            ScalarValue::Int64(20),
            ScalarValue::Text("E".into()),
            ScalarValue::Bool(true),
        ],
    ] {
        database.insert(&row).expect("insert aggregate row");
    }

    let all_source = "SELECT COUNT(*), COUNT(score), SUM(score), MIN(score), MAX(score) FROM users";
    let all = database.query(all_source).expect("aggregate all rows");
    assert_eq!(
        all.rows,
        vec![vec![
            ScalarValue::UInt64(5),
            ScalarValue::UInt64(3),
            ScalarValue::Int64(50),
            ScalarValue::Int64(10),
            ScalarValue::Int64(20),
        ]]
    );
    assert_eq!(
        all.columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        [
            "COUNT(*)",
            "COUNT(score)",
            "SUM(score)",
            "MIN(score)",
            "MAX(score)"
        ]
    );
    assert!(!all.columns[0].nullable);
    assert!(!all.columns[1].nullable);
    assert!(all.columns[2..].iter().all(|column| column.nullable));

    let filtered_source = "SELECT COUNT(*), COUNT(score), SUM(score), MIN(score), MAX(score) \
                           FROM users WHERE active = true";
    let filtered = database.query(filtered_source).expect("filtered aggregate");
    assert_eq!(
        filtered.rows,
        vec![vec![
            ScalarValue::UInt64(4),
            ScalarValue::UInt64(2),
            ScalarValue::Int64(40),
            ScalarValue::Int64(20),
            ScalarValue::Int64(20),
        ]]
    );

    let empty = database
        .query(
            "SELECT COUNT(*), COUNT(score), SUM(score), MIN(score), MAX(score) \
             FROM users WHERE name = 'missing'",
        )
        .expect("empty aggregate input");
    assert_eq!(
        empty.rows,
        vec![vec![
            ScalarValue::UInt64(0),
            ScalarValue::UInt64(0),
            ScalarValue::Null,
            ScalarValue::Null,
            ScalarValue::Null,
        ]]
    );
    let limited = database
        .query("SELECT COUNT(*) FROM users LIMIT 0")
        .expect("limit aggregate output");
    assert_eq!(limited.columns[0].name, "COUNT(*)");
    assert!(limited.rows.is_empty());
    assert_eq!(
        database
            .query("SELECT COUNT(*) FROM users LIMIT 1")
            .expect("aggregate before limit")
            .rows,
        vec![vec![ScalarValue::UInt64(5)]]
    );
    assert_eq!(
        database
            .query(
                "SELECT COUNT(score), SUM(score), MIN(score), MAX(score) \
                 FROM users WHERE name = 'A'",
            )
            .expect("all-null aggregate input")
            .rows,
        vec![vec![
            ScalarValue::UInt64(0),
            ScalarValue::Null,
            ScalarValue::Null,
            ScalarValue::Null,
        ]]
    );

    let typed = database
        .query("SELECT SUM(id), MIN(id), MAX(id), MIN(name), MAX(name), MIN(active), MAX(active) FROM users")
        .expect("typed aggregate outputs");
    assert_eq!(typed.columns[0].data_type.physical, PhysicalType::UInt64);
    assert_eq!(typed.columns[0].data_type.name, None);
    assert_eq!(typed.columns[1].data_type.name.as_deref(), Some("UserId"));
    assert_eq!(typed.columns[2].data_type.name.as_deref(), Some("UserId"));
    assert_eq!(
        typed.rows,
        vec![vec![
            ScalarValue::UInt64(15),
            ScalarValue::UInt64(1),
            ScalarValue::UInt64(5),
            ScalarValue::Text("A".into()),
            ScalarValue::Text("E".into()),
            ScalarValue::Bool(false),
            ScalarValue::Bool(true),
        ]]
    );

    database.close().expect("close aggregate database");
    let mut reopened = Database::open(&path, table).expect("reopen aggregate database");
    assert_eq!(
        reopened
            .query(filtered_source)
            .expect("aggregate after reopen"),
        filtered
    );
    reopened.close().expect("close reopened aggregate database");
    cleanup(&path);
}

#[test]
fn typed_grouped_aggregates_preserve_null_groups_projection_and_reopen() {
    let path = std::env::temp_dir().join(format!(
        "netbadb-grouped-{}-{:?}.db",
        std::process::id(),
        std::thread::current().id()
    ));
    let table = TableDef::new(
        TableId(95),
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
            ColumnDef::new(
                ColumnId(2),
                "team_id",
                TypeSpec::Semantic {
                    name: "TeamId".into(),
                    physical: PhysicalType::UInt64,
                },
            )
            .nullable(true),
            ColumnDef::new(
                ColumnId(3),
                "score",
                TypeSpec::Physical(PhysicalType::Int64),
            )
            .nullable(true),
            ColumnDef::new(
                ColumnId(4),
                "active",
                TypeSpec::Physical(PhysicalType::Bool),
            ),
            ColumnDef::new(
                ColumnId(5),
                "country",
                TypeSpec::Physical(PhysicalType::Text),
            ),
        ],
    );
    let mut database = Database::create(&path, table.clone()).expect("create grouped database");
    for row in [
        vec![
            ScalarValue::UInt64(1),
            ScalarValue::UInt64(10),
            ScalarValue::Int64(10),
            ScalarValue::Bool(true),
            ScalarValue::Text("US".into()),
        ],
        vec![
            ScalarValue::UInt64(2),
            ScalarValue::UInt64(10),
            ScalarValue::Int64(20),
            ScalarValue::Bool(true),
            ScalarValue::Text("US".into()),
        ],
        vec![
            ScalarValue::UInt64(3),
            ScalarValue::UInt64(20),
            ScalarValue::Null,
            ScalarValue::Bool(true),
            ScalarValue::Text("CA".into()),
        ],
        vec![
            ScalarValue::UInt64(4),
            ScalarValue::UInt64(20),
            ScalarValue::Int64(30),
            ScalarValue::Bool(false),
            ScalarValue::Text("CA".into()),
        ],
        vec![
            ScalarValue::UInt64(5),
            ScalarValue::Null,
            ScalarValue::Int64(5),
            ScalarValue::Bool(true),
            ScalarValue::Text("US".into()),
        ],
        vec![
            ScalarValue::UInt64(6),
            ScalarValue::Null,
            ScalarValue::Null,
            ScalarValue::Bool(true),
            ScalarValue::Text("US".into()),
        ],
    ] {
        database.insert(&row).expect("insert grouped row");
    }

    let source = "SELECT COUNT(*), team_id, COUNT(score), SUM(score), MIN(score), MAX(score) \
                  FROM users GROUP BY team_id";
    let grouped = database.query(source).expect("grouped aggregate");
    assert_eq!(
        grouped
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        [
            "COUNT(*)",
            "team_id",
            "COUNT(score)",
            "SUM(score)",
            "MIN(score)",
            "MAX(score)"
        ]
    );
    assert_eq!(grouped.columns[1].data_type.name.as_deref(), Some("TeamId"));
    assert!(grouped.columns[1].nullable);
    assert_eq!(
        grouped.rows,
        vec![
            vec![
                ScalarValue::UInt64(2),
                ScalarValue::UInt64(10),
                ScalarValue::UInt64(2),
                ScalarValue::Int64(30),
                ScalarValue::Int64(10),
                ScalarValue::Int64(20),
            ],
            vec![
                ScalarValue::UInt64(2),
                ScalarValue::UInt64(20),
                ScalarValue::UInt64(1),
                ScalarValue::Int64(30),
                ScalarValue::Int64(30),
                ScalarValue::Int64(30),
            ],
            vec![
                ScalarValue::UInt64(2),
                ScalarValue::Null,
                ScalarValue::UInt64(1),
                ScalarValue::Int64(5),
                ScalarValue::Int64(5),
                ScalarValue::Int64(5),
            ],
        ]
    );

    let hidden = database
        .query("SELECT COUNT(*) FROM users GROUP BY team_id")
        .expect("hidden group key");
    assert_eq!(hidden.columns.len(), 1);
    assert_eq!(hidden.rows.len(), 3);
    assert_eq!(
        hidden.rows,
        vec![
            vec![ScalarValue::UInt64(2)],
            vec![ScalarValue::UInt64(2)],
            vec![ScalarValue::UInt64(2)]
        ]
    );
    assert_eq!(
        database
            .query("SELECT team_id FROM users GROUP BY team_id")
            .expect("group-only distinct output")
            .rows,
        vec![
            vec![ScalarValue::UInt64(10)],
            vec![ScalarValue::UInt64(20)],
            vec![ScalarValue::Null]
        ]
    );
    assert_eq!(
        database
            .query(
                "SELECT active, COUNT(*), COUNT(score), SUM(score), MIN(score), MAX(score) \
                 FROM users WHERE score IS NULL GROUP BY active",
            )
            .expect("all-null aggregate group")
            .rows,
        vec![vec![
            ScalarValue::Bool(true),
            ScalarValue::UInt64(2),
            ScalarValue::UInt64(0),
            ScalarValue::Null,
            ScalarValue::Null,
            ScalarValue::Null,
        ]]
    );
    assert_eq!(
        database
            .query("SELECT score, COUNT(*) FROM users GROUP BY score")
            .expect("signed integer group key")
            .rows
            .len(),
        5
    );
    assert_eq!(
        database
            .query("SELECT country, team_id, COUNT(*) FROM users GROUP BY country, team_id")
            .expect("multi-key grouping")
            .rows,
        vec![
            vec![
                ScalarValue::Text("US".into()),
                ScalarValue::UInt64(10),
                ScalarValue::UInt64(2)
            ],
            vec![
                ScalarValue::Text("CA".into()),
                ScalarValue::UInt64(20),
                ScalarValue::UInt64(2)
            ],
            vec![
                ScalarValue::Text("US".into()),
                ScalarValue::Null,
                ScalarValue::UInt64(2)
            ],
        ]
    );
    assert_eq!(
        database
            .query(
                "SELECT country, score, COUNT(*) FROM users WHERE score IS NULL \
                 GROUP BY country, score",
            )
            .expect("multi-key NULL grouping")
            .rows,
        vec![
            vec![
                ScalarValue::Text("CA".into()),
                ScalarValue::Null,
                ScalarValue::UInt64(1)
            ],
            vec![
                ScalarValue::Text("US".into()),
                ScalarValue::Null,
                ScalarValue::UInt64(1)
            ],
        ]
    );

    let grouped_empty = database
        .query("SELECT team_id, COUNT(*) FROM users WHERE country = 'missing' GROUP BY team_id")
        .expect("empty grouped input");
    assert!(grouped_empty.rows.is_empty());
    assert_eq!(grouped_empty.columns.len(), 2);
    assert_eq!(
        database
            .query("SELECT COUNT(*) FROM users WHERE country = 'missing'")
            .expect("empty global input")
            .rows,
        vec![vec![ScalarValue::UInt64(0)]]
    );
    let limited = database
        .query("SELECT team_id, COUNT(*) FROM users GROUP BY team_id LIMIT 1")
        .expect("limit grouped results");
    assert_eq!(limited.rows.len(), 1);
    assert_eq!(limited.rows[0][1], ScalarValue::UInt64(2));

    let semantic = database
        .query("SELECT team_id, SUM(id), MIN(id), MAX(id) FROM users GROUP BY team_id")
        .expect("grouped semantic outputs");
    assert_eq!(semantic.columns[1].data_type.name, None);
    assert_eq!(
        semantic.columns[2].data_type.name.as_deref(),
        Some("UserId")
    );
    assert_eq!(
        semantic.columns[3].data_type.name.as_deref(),
        Some("UserId")
    );

    database.close().expect("close grouped database");
    let mut reopened = Database::open(&path, table).expect("reopen grouped database");
    assert_eq!(reopened.query(source).expect("group after reopen"), grouped);
    reopened.close().expect("close reopened grouped database");
    cleanup(&path);
}

#[test]
fn public_query_pipeline_obeys_null_and_three_valued_logic_after_reopen() {
    let path =
        std::env::temp_dir().join(format!("netbadb-null-integration-{}", std::process::id()));
    let mut database = Database::create(&path, nullable_users()).expect("create database");
    assert!(matches!(
        database.insert(&[
            ScalarValue::Null,
            ScalarValue::Text("invalid".into()),
            ScalarValue::Null,
            ScalarValue::Null,
        ]),
        Err(DatabaseError::Storage(StorageError::NullNotAllowed { column })) if column == "id"
    ));
    for row in [
        vec![
            ScalarValue::Int64(1),
            ScalarValue::Text("Ada".into()),
            ScalarValue::Null,
            ScalarValue::Bool(true),
        ],
        vec![
            ScalarValue::Int64(2),
            ScalarValue::Text("Lin".into()),
            ScalarValue::Text("lin".into()),
            ScalarValue::Null,
        ],
        vec![
            ScalarValue::Int64(3),
            ScalarValue::Text("Bo".into()),
            ScalarValue::Null,
            ScalarValue::Bool(false),
        ],
    ] {
        database.insert(&row).expect("insert row");
    }
    database.close().expect("close database");

    let mut database = Database::open(&path, nullable_users()).expect("reopen database");
    let ids = |database: &mut Database, predicate: &str| {
        database
            .query(&format!("SELECT id FROM users WHERE {predicate}"))
            .expect("query")
            .rows
    };
    assert_eq!(
        ids(&mut database, "nickname IS NULL"),
        vec![vec![ScalarValue::Int64(1)], vec![ScalarValue::Int64(3)]]
    );
    assert_eq!(
        ids(&mut database, "nickname IS NOT NULL"),
        vec![vec![ScalarValue::Int64(2)]]
    );
    assert_eq!(
        ids(&mut database, "nickname = 'lin'"),
        vec![vec![ScalarValue::Int64(2)]]
    );
    assert_eq!(
        ids(&mut database, "active"),
        vec![vec![ScalarValue::Int64(1)]]
    );
    assert_eq!(
        ids(&mut database, "active AND true"),
        vec![vec![ScalarValue::Int64(1)]]
    );
    assert!(ids(&mut database, "active AND false").is_empty());
    assert_eq!(
        ids(&mut database, "active OR true"),
        vec![
            vec![ScalarValue::Int64(1)],
            vec![ScalarValue::Int64(2)],
            vec![ScalarValue::Int64(3)],
        ]
    );
    assert_eq!(
        ids(&mut database, "active OR false"),
        vec![vec![ScalarValue::Int64(1)]]
    );
    assert_eq!(
        ids(&mut database, "NOT active"),
        vec![vec![ScalarValue::Int64(3)]]
    );
    assert!(ids(&mut database, "NULL").is_empty());
    assert!(ids(&mut database, "nickname = NULL").is_empty());
    assert!(ids(&mut database, "NULL = NULL").is_empty());
    database.close().expect("close reopened database");
    cleanup(&path);
}

#[test]
fn typed_sql_dml_reports_affected_rows_and_survives_reopen() {
    let path = std::env::temp_dir().join(format!("netbadb-dml-{}", std::process::id()));
    let mut database = Database::create(&path, nullable_users()).expect("create database");

    assert_eq!(
        affected(
            database
                .execute("INSERT INTO users (id, name) VALUES (1, 'Ada')")
                .expect("insert Ada"),
        ),
        1
    );
    assert_eq!(
        database
            .query("SELECT nickname, active FROM users WHERE id = 1")
            .expect("omitted nullable columns")
            .rows,
        vec![vec![ScalarValue::Null, ScalarValue::Null]]
    );
    assert!(matches!(
        database
            .execute("SELECT name FROM users WHERE id = 1")
            .expect("execute SELECT"),
        ExecutionResult::Query(result)
            if result.rows == vec![vec![ScalarValue::Text("Ada".into())]]
    ));
    assert_eq!(
        affected(
            database
                .execute(
                    "INSERT INTO users (id, name, nickname, active) VALUES (2, 'Lin', 'lin', true)"
                )
                .expect("insert Lin"),
        ),
        1
    );
    assert_eq!(
        affected(
            database
                .execute("UPDATE users SET nickname = 'ada', active = false WHERE nickname IS NULL")
                .expect("update NULL nickname"),
        ),
        1
    );
    assert_eq!(
        affected(
            database
                .execute("UPDATE users SET name = name WHERE id = 1")
                .expect("same-value update"),
        ),
        1
    );
    assert_eq!(
        affected(
            database
                .execute("DELETE FROM users WHERE active = NULL")
                .expect("unknown predicate deletes nothing"),
        ),
        0
    );
    assert!(matches!(
        database.query("DELETE FROM users WHERE id = 2"),
        Err(DatabaseError::ExpectedQuery)
    ));
    assert_eq!(
        affected(
            database
                .execute("DELETE FROM users WHERE id = 2")
                .expect("delete Lin"),
        ),
        1
    );
    database.close().expect("close database");

    let mut reopened = Database::open(&path, nullable_users()).expect("reopen database");
    assert_eq!(
        reopened
            .query("SELECT id, name, nickname, active FROM users")
            .expect("query final row")
            .rows,
        vec![vec![
            ScalarValue::Int64(1),
            ScalarValue::Text("Ada".into()),
            ScalarValue::Text("ada".into()),
            ScalarValue::Bool(false),
        ]]
    );
    reopened.close().expect("close reopened database");
    cleanup(&path);
}

#[test]
fn update_assignments_are_simultaneous_and_unfiltered_dml_targets_all_rows() {
    let path = std::env::temp_dir().join(format!("netbadb-dml-all-{}", std::process::id()));
    let mut database = Database::create(&path, pairs()).expect("create database");
    database
        .execute("INSERT INTO pairs (id, a, b) VALUES (1, 10, 20)")
        .expect("insert first");
    database
        .execute("INSERT INTO pairs (id, a, b) VALUES (2, 30, 40)")
        .expect("insert second");

    assert_eq!(
        affected(
            database
                .execute("UPDATE pairs SET a = b, b = a")
                .expect("swap all rows"),
        ),
        2
    );
    assert_eq!(
        database
            .query("SELECT id, a, b FROM pairs")
            .expect("query swapped rows")
            .rows,
        vec![
            vec![
                ScalarValue::Int64(1),
                ScalarValue::Int64(20),
                ScalarValue::Int64(10),
            ],
            vec![
                ScalarValue::Int64(2),
                ScalarValue::Int64(40),
                ScalarValue::Int64(30),
            ],
        ]
    );
    assert_eq!(
        affected(database.execute("DELETE FROM pairs").expect("delete all")),
        2
    );
    assert!(
        database
            .query("SELECT id FROM pairs")
            .expect("query empty table")
            .rows
            .is_empty()
    );
    database.close().expect("close database");
    cleanup(&path);
}

#[test]
fn explicit_transaction_mixed_dml_rolls_back_or_commits_as_one_unit() {
    let path = std::env::temp_dir().join(format!("netbadb-dml-txn-{}", std::process::id()));
    let mut database = Database::create(&path, nullable_users()).expect("create database");
    database
        .execute("INSERT INTO users (id, name) VALUES (1, 'base')")
        .expect("insert baseline");

    let mut rollback = database.begin_transaction().expect("begin rollback");
    database
        .execute_in(
            &mut rollback,
            "INSERT INTO users (id, name) VALUES (2, 'temporary')",
        )
        .expect("insert temporary");
    database
        .execute_in(
            &mut rollback,
            "UPDATE users SET name = 'changed' WHERE id = 1",
        )
        .expect("update baseline");
    database
        .execute_in(&mut rollback, "DELETE FROM users WHERE id = 2")
        .expect("delete temporary");
    rollback.rollback().expect("rollback mixed DML");
    assert_eq!(rollback.state(), TransactionState::RolledBack);
    assert_eq!(
        database
            .query("SELECT id, name FROM users")
            .expect("query rollback state")
            .rows,
        vec![vec![
            ScalarValue::Int64(1),
            ScalarValue::Text("base".into()),
        ]]
    );

    let mut commit = database.begin_transaction().expect("begin commit");
    database
        .execute_in(
            &mut commit,
            "INSERT INTO users (id, name) VALUES (2, 'committed')",
        )
        .expect("insert committed");
    database
        .execute_in(
            &mut commit,
            "UPDATE users SET name = 'updated' WHERE id = 1",
        )
        .expect("update committed");
    database
        .execute_in(&mut commit, "DELETE FROM users WHERE id = 2")
        .expect("delete committed insert");
    commit.commit().expect("commit mixed DML");
    database.close().expect("close database");

    let mut reopened = Database::open(&path, nullable_users()).expect("reopen database");
    assert_eq!(
        reopened
            .query("SELECT id, name FROM users")
            .expect("query committed state")
            .rows,
        vec![vec![
            ScalarValue::Int64(1),
            ScalarValue::Text("updated".into()),
        ]]
    );
    reopened.close().expect("close reopened database");
    cleanup(&path);
}

#[test]
fn overflowing_multi_row_update_rolls_back_every_prior_replacement() {
    let path = std::env::temp_dir().join(format!("netbadb-dml-atomic-{}", std::process::id()));
    let mut database = Database::create(&path, variable_rows()).expect("create database");
    let small_source = "a".repeat(100);
    let large_source = "b".repeat(1_500);
    let filler = "c".repeat(2_250);
    for sql in [
        format!("INSERT INTO variable_rows (id, target, source) VALUES (1, 'x', '{small_source}')"),
        format!("INSERT INTO variable_rows (id, target, source) VALUES (2, 'y', '{large_source}')"),
        format!("INSERT INTO variable_rows (id, target, source) VALUES (3, 'z', '{filler}')"),
    ] {
        database.execute(&sql).expect("insert variable row");
    }

    let mut transaction = database.begin_transaction().expect("begin failed update");
    assert!(matches!(
        database.execute_in(&mut transaction, "UPDATE variable_rows SET target = source"),
        Err(DatabaseError::Execution(ExecutionError::Storage(
            StorageError::Page(PageError::UpdateWouldOverflowPage { .. })
        )))
    ));
    assert_eq!(transaction.state(), TransactionState::RolledBack);
    assert_eq!(
        database
            .query("SELECT id, target FROM variable_rows")
            .expect("query rolled-back rows")
            .rows,
        vec![
            vec![ScalarValue::Int64(1), ScalarValue::Text("x".into())],
            vec![ScalarValue::Int64(2), ScalarValue::Text("y".into())],
            vec![ScalarValue::Int64(3), ScalarValue::Text("z".into())],
        ]
    );
    assert!(matches!(
        database.execute("UPDATE variable_rows SET target = source"),
        Err(DatabaseError::Execution(ExecutionError::Storage(
            StorageError::Page(PageError::UpdateWouldOverflowPage { .. })
        )))
    ));
    assert_eq!(
        database
            .query("SELECT id, target FROM variable_rows")
            .expect("query implicit rollback rows")
            .rows,
        vec![
            vec![ScalarValue::Int64(1), ScalarValue::Text("x".into())],
            vec![ScalarValue::Int64(2), ScalarValue::Text("y".into())],
            vec![ScalarValue::Int64(3), ScalarValue::Text("z".into())],
        ]
    );
    database.close().expect("close database");
    cleanup(&path);
}

#[test]
fn checkpoint_preserves_mixed_dml_across_wal_rotation_and_reopen() {
    let path = std::env::temp_dir().join(format!("netbadb-dml-checkpoint-{}", std::process::id()));
    let mut database = Database::create(&path, nullable_users()).expect("create database");
    database
        .execute("INSERT INTO users (id, name) VALUES (1, 'before')")
        .expect("insert before checkpoint");
    database
        .execute("INSERT INTO users (id, name) VALUES (2, 'delete-me')")
        .expect("insert delete target");
    database
        .execute("UPDATE users SET name = 'checkpointed' WHERE id = 1")
        .expect("update before checkpoint");
    database
        .execute("DELETE FROM users WHERE id = 2")
        .expect("delete before checkpoint");
    database.checkpoint().expect("checkpoint mixed DML");
    database
        .execute("INSERT INTO users (id, name) VALUES (3, 'after')")
        .expect("insert after checkpoint");
    database.close().expect("close database");

    let mut reopened = Database::open(&path, nullable_users()).expect("reopen database");
    assert_eq!(
        reopened
            .query("SELECT id, name FROM users")
            .expect("query checkpoint state")
            .rows,
        vec![
            vec![
                ScalarValue::Int64(1),
                ScalarValue::Text("checkpointed".into()),
            ],
            vec![ScalarValue::Int64(3), ScalarValue::Text("after".into()),],
        ]
    );
    reopened.close().expect("close reopened database");
    cleanup(&path);
}

#[test]
fn foreign_transaction_is_rejected_without_rolling_back_its_owner() {
    let first_path =
        std::env::temp_dir().join(format!("netbadb-dml-foreign-a-{}", std::process::id()));
    let second_path =
        std::env::temp_dir().join(format!("netbadb-dml-foreign-b-{}", std::process::id()));
    let mut first = Database::create(&first_path, nullable_users()).expect("create first");
    let mut second = Database::create(&second_path, nullable_users()).expect("create second");
    let mut transaction = first.begin_transaction().expect("begin first transaction");
    first
        .execute_in(
            &mut transaction,
            "INSERT INTO users (id, name) VALUES (1, 'temporary')",
        )
        .expect("write in owner");

    assert!(matches!(
        second.execute_in(
            &mut transaction,
            "UPDATE users SET name = 'wrong database' WHERE id = 999"
        ),
        Err(DatabaseError::Storage(StorageError::Transaction(_)))
    ));
    assert_eq!(transaction.state(), TransactionState::Active);
    transaction.rollback().expect("owner can still roll back");
    assert!(
        first
            .query("SELECT id FROM users")
            .expect("owner rollback state")
            .rows
            .is_empty()
    );

    first.close().expect("close first");
    second.close().expect("close second");
    cleanup(&first_path);
    cleanup(&second_path);
}

#[test]
fn failed_multi_table_create_removes_only_files_created_by_that_call() {
    let base = std::env::temp_dir().join(format!(
        "netbadb-core-create-rollback-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let first_path = base.with_extension("first.db");
    let existing_path = base.with_extension("existing.db");
    let first = TableDef::new(
        TableId(41),
        "first",
        vec![ColumnDef::new(
            ColumnId(1),
            "id",
            TypeSpec::Physical(PhysicalType::Int64),
        )],
    );
    let existing = TableDef::new(
        TableId(42),
        "existing",
        vec![ColumnDef::new(
            ColumnId(1),
            "id",
            TypeSpec::Physical(PhysicalType::Int64),
        )],
    );
    cleanup(&first_path);
    cleanup(&existing_path);
    Database::create(&existing_path, existing.clone())
        .expect("create existing target")
        .close()
        .expect("close existing target");

    assert!(
        Database::create_tables(vec![
            (first_path.clone(), first),
            (existing_path.clone(), existing.clone()),
        ])
        .is_err()
    );
    assert!(!first_path.exists());
    let first_wal = netbadb_storage::wal_path(&first_path);
    assert!(!first_wal.exists());
    assert!(!netbadb_storage::wal_alternate_path(&first_wal).exists());

    Database::open(&existing_path, existing)
        .expect("pre-existing target remains intact")
        .close()
        .expect("close pre-existing target");
    cleanup(&first_path);
    cleanup(&existing_path);
}

#[test]
fn invalid_multi_table_schema_is_rejected_before_any_storage_is_created() {
    let base = std::env::temp_dir().join(format!(
        "netbadb-core-invalid-schema-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let users_path = base.with_extension("users.db");
    let teams_path = base.with_extension("teams.db");
    cleanup(&users_path);
    cleanup(&teams_path);
    let invalid_teams = TableDef::new(
        TableId(82),
        "teams",
        vec![
            ColumnDef::new(ColumnId(1), "id", TypeSpec::Physical(PhysicalType::UInt64)),
            ColumnDef::new(
                ColumnId(1),
                "owner_id",
                TypeSpec::Physical(PhysicalType::UInt64),
            ),
        ],
    );
    assert!(matches!(
        Database::create_tables(vec![
            (
                users_path.clone(),
                TableDef::new(TableId(81), "users", Vec::new())
            ),
            (teams_path.clone(), invalid_teams),
        ]),
        Err(DatabaseError::Schema(SchemaError::DuplicateColumnId {
            table,
            column_id: ColumnId(1)
        })) if table == "teams"
    ));
    for path in [&users_path, &teams_path] {
        assert!(!path.exists());
        let wal = netbadb_storage::wal_path(path);
        assert!(!wal.exists());
        assert!(!netbadb_storage::wal_alternate_path(&wal).exists());
    }
}

#[test]
fn sql_keyword_name_is_accepted_by_canonical_schema_and_storage() {
    let path = std::env::temp_dir().join(format!(
        "netbadb-core-frontend-independent-name-{}-{:?}.db",
        std::process::id(),
        std::thread::current().id()
    ));
    cleanup(&path);
    let table = TableDef::new(
        TableId(83),
        "order",
        vec![
            ColumnDef::new(
                ColumnId(1),
                "group",
                TypeSpec::Physical(PhysicalType::UInt64),
            ),
            ColumnDef::new(ColumnId(2), "nulls", TypeSpec::Physical(PhysicalType::Text)),
        ],
    );

    Database::create(&path, table.clone())
        .expect("SQL keywords are valid canonical names")
        .close()
        .expect("close database");
    Database::open(&path, table)
        .expect("reopen database with SQL keyword name")
        .close()
        .expect("close reopened database");
    cleanup(&path);
}

#[test]
fn reopen_rejects_swapped_nominal_types_with_identical_physical_layout() {
    let path = std::env::temp_dir().join(format!(
        "netbadb-core-schema-fingerprint-{}-{:?}.db",
        std::process::id(),
        std::thread::current().id()
    ));
    cleanup(&path);
    let users = TableDef::new(
        TableId(91),
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
            ColumnDef::new(
                ColumnId(2),
                "team_id",
                TypeSpec::Semantic {
                    name: "TeamId".into(),
                    physical: PhysicalType::UInt64,
                },
            ),
        ],
    );
    let mut database = Database::create(&path, users.clone()).expect("create database");
    database
        .insert(&[ScalarValue::UInt64(1), ScalarValue::UInt64(7)])
        .expect("insert nominal row");
    database.close().expect("close database");

    let mut swapped = users.clone();
    swapped.columns[0].type_spec = TypeSpec::Semantic {
        name: "TeamId".into(),
        physical: PhysicalType::UInt64,
    };
    swapped.columns[1].type_spec = TypeSpec::Semantic {
        name: "UserId".into(),
        physical: PhysicalType::UInt64,
    };
    assert!(matches!(
        Database::open(&path, swapped),
        Err(DatabaseError::Storage(StorageError::SchemaMismatch { .. }))
    ));

    let mut reopened = Database::open(&path, users).expect("matching schema still opens");
    assert_eq!(
        reopened
            .query("SELECT id, team_id FROM users")
            .expect("read preserved row")
            .rows,
        vec![vec![ScalarValue::UInt64(1), ScalarValue::UInt64(7)]]
    );
    reopened.close().expect("close matching database");
    cleanup(&path);
}

#[test]
fn table_bound_transaction_rejects_cross_table_writes_without_rollback() {
    let base = std::env::temp_dir().join(format!(
        "netbadb-core-table-transaction-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let users_path = base.with_extension("users.db");
    let teams_path = base.with_extension("teams.db");
    let users = users();
    let teams = TableDef::new(
        TableId(2),
        "teams",
        vec![
            ColumnDef::new(ColumnId(1), "id", TypeSpec::Physical(PhysicalType::Int64)),
            ColumnDef::new(ColumnId(2), "name", TypeSpec::Physical(PhysicalType::Text)),
        ],
    );
    let mut database = Database::create_tables(vec![
        (users_path.clone(), users),
        (teams_path.clone(), teams),
    ])
    .expect("create catalog");
    let mut transaction = database
        .begin_transaction_for(TableId(1))
        .expect("begin users transaction");
    database
        .insert_into_in(
            TableId(1),
            &mut transaction,
            &[ScalarValue::Int64(1), ScalarValue::Text("temporary".into())],
        )
        .expect("insert into owning table");

    assert!(matches!(
        database.insert_into_in(
            TableId(2),
            &mut transaction,
            &[
                ScalarValue::Int64(1),
                ScalarValue::Text("wrong table".into())
            ],
        ),
        Err(DatabaseError::Storage(StorageError::Transaction(_)))
    ));
    assert_eq!(transaction.state(), TransactionState::Active);
    transaction.rollback().expect("owner can still roll back");
    assert!(
        database
            .query("SELECT id FROM users")
            .expect("query owner table")
            .rows
            .is_empty()
    );

    database.close().expect("close catalog");
    cleanup(&users_path);
    cleanup(&teams_path);
}

#[test]
fn typed_inner_join_runs_across_heaps_with_null_where_star_limit_and_duplicates() {
    let base = std::env::temp_dir().join(format!(
        "netbadb-core-join-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let users_path = base.with_extension("users.db");
    let teams_path = base.with_extension("teams.db");
    let users = TableDef::new(
        TableId(11),
        "users",
        vec![
            ColumnDef::new(ColumnId(1), "id", TypeSpec::Physical(PhysicalType::Int64)),
            ColumnDef::new(ColumnId(2), "name", TypeSpec::Physical(PhysicalType::Text)),
            ColumnDef::new(
                ColumnId(3),
                "team_id",
                TypeSpec::Physical(PhysicalType::Int64),
            )
            .nullable(true),
        ],
    );
    let teams = TableDef::new(
        TableId(12),
        "teams",
        vec![
            ColumnDef::new(ColumnId(1), "id", TypeSpec::Physical(PhysicalType::Int64))
                .nullable(true),
            ColumnDef::new(ColumnId(2), "name", TypeSpec::Physical(PhysicalType::Text)),
        ],
    );
    let mut database = Database::create_tables(vec![
        (users_path.clone(), users.clone()),
        (teams_path.clone(), teams.clone()),
    ])
    .expect("create catalog");
    for values in [
        vec![
            ScalarValue::Int64(1),
            ScalarValue::Text("Ada".into()),
            ScalarValue::Int64(10),
        ],
        vec![
            ScalarValue::Int64(2),
            ScalarValue::Text("Lin".into()),
            ScalarValue::Int64(20),
        ],
        vec![
            ScalarValue::Int64(3),
            ScalarValue::Text("Bo".into()),
            ScalarValue::Null,
        ],
    ] {
        database
            .insert_into(TableId(11), &values)
            .expect("insert user");
    }
    assert!(
        database
            .query("SELECT * FROM users u JOIN teams t ON u.team_id = t.id")
            .expect("right-empty join")
            .rows
            .is_empty()
    );
    assert_eq!(
        database
            .query("SELECT COUNT(*) FROM users u JOIN teams t ON u.team_id = t.id")
            .expect("aggregate empty join")
            .rows,
        vec![vec![ScalarValue::UInt64(0)]]
    );
    for values in [
        vec![ScalarValue::Int64(10), ScalarValue::Text("Core".into())],
        vec![ScalarValue::Int64(10), ScalarValue::Text("Core-2".into())],
        vec![ScalarValue::Int64(20), ScalarValue::Text("Tools".into())],
        vec![ScalarValue::Null, ScalarValue::Text("No team".into())],
    ] {
        database
            .insert_into(TableId(12), &values)
            .expect("insert team");
    }

    let joined = database
        .query(
            "SELECT u.name, t.name FROM users AS u JOIN teams AS t \
             ON u.team_id = t.id",
        )
        .expect("join");
    assert_eq!(
        joined.rows,
        vec![
            vec![
                ScalarValue::Text("Ada".into()),
                ScalarValue::Text("Core".into())
            ],
            vec![
                ScalarValue::Text("Ada".into()),
                ScalarValue::Text("Core-2".into())
            ],
            vec![
                ScalarValue::Text("Lin".into()),
                ScalarValue::Text("Tools".into())
            ],
        ]
    );
    assert_eq!(joined.columns[0].name, "name");
    assert_eq!(joined.columns[1].name, "name");
    assert_eq!(
        database
            .query(
                "SELECT t.name, COUNT(*) FROM users u JOIN teams t ON u.team_id = t.id \
                 GROUP BY t.name",
            )
            .expect("group joined rows")
            .rows,
        vec![
            vec![ScalarValue::Text("Core".into()), ScalarValue::UInt64(1)],
            vec![ScalarValue::Text("Core-2".into()), ScalarValue::UInt64(1)],
            vec![ScalarValue::Text("Tools".into()), ScalarValue::UInt64(1)],
        ]
    );
    assert_eq!(
        database
            .query(
                "SELECT u.team_id, COUNT(*) FROM users u JOIN teams t ON u.team_id = t.id \
                 GROUP BY u.team_id",
            )
            .expect("group duplicate join rows")
            .rows,
        vec![
            vec![ScalarValue::Int64(10), ScalarValue::UInt64(2)],
            vec![ScalarValue::Int64(20), ScalarValue::UInt64(1)],
        ]
    );
    assert_eq!(
        database
            .query(
                "SELECT COUNT(*), COUNT(t.id), MIN(t.name) FROM users u \
                 JOIN teams t ON u.team_id = t.id",
            )
            .expect("aggregate joined duplicates")
            .rows,
        vec![vec![
            ScalarValue::UInt64(3),
            ScalarValue::UInt64(3),
            ScalarValue::Text("Core".into()),
        ]]
    );

    let filtered = database
        .query(
            "SELECT u.name FROM users u JOIN teams t ON u.team_id = t.id \
             WHERE t.name = 'Core' LIMIT 1",
        )
        .expect("filtered join");
    assert_eq!(filtered.rows, vec![vec![ScalarValue::Text("Ada".into())]]);

    let star = database
        .query("SELECT * FROM users u JOIN teams t ON u.team_id = t.id LIMIT 1")
        .expect("star join");
    assert_eq!(
        star.rows[0],
        vec![
            ScalarValue::Int64(1),
            ScalarValue::Text("Ada".into()),
            ScalarValue::Int64(10),
            ScalarValue::Int64(10),
            ScalarValue::Text("Core".into()),
        ]
    );
    assert_eq!(star.columns.len(), 5);

    database.close().expect("close catalog");
    let mut reopened = Database::open_tables(vec![
        (users_path.clone(), users),
        (teams_path.clone(), teams),
    ])
    .expect("reopen catalog");
    assert_eq!(
        reopened
            .query("SELECT u.name FROM users u JOIN teams t ON u.team_id = t.id")
            .expect("join after reopen")
            .rows
            .len(),
        3
    );
    reopened.close().expect("close reopened catalog");
    cleanup(&users_path);
    cleanup(&teams_path);
}

#[test]
fn order_by_join_columns_uses_binding_identity_and_multiple_keys() {
    let base = std::env::temp_dir().join(format!(
        "netbadb-order-join-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let users_path = base.with_extension("users.db");
    let teams_path = base.with_extension("teams.db");
    let users = TableDef::new(
        TableId(92),
        "users",
        vec![
            ColumnDef::new(ColumnId(1), "id", TypeSpec::Physical(PhysicalType::Int64)),
            ColumnDef::new(
                ColumnId(2),
                "team_id",
                TypeSpec::Physical(PhysicalType::Int64),
            ),
        ],
    );
    let teams = TableDef::new(
        TableId(93),
        "teams",
        vec![
            ColumnDef::new(ColumnId(1), "id", TypeSpec::Physical(PhysicalType::Int64)),
            ColumnDef::new(ColumnId(2), "name", TypeSpec::Physical(PhysicalType::Text)),
        ],
    );
    let mut database = Database::create_tables(vec![
        (users_path.clone(), users),
        (teams_path.clone(), teams),
    ])
    .expect("create catalog");
    for (id, team_id) in [(1, 2), (2, 1), (3, 1)] {
        database
            .insert_into(
                TableId(92),
                &[ScalarValue::Int64(id), ScalarValue::Int64(team_id)],
            )
            .expect("insert user");
    }
    for (id, name) in [(1, "alpha"), (2, "beta")] {
        database
            .insert_into(
                TableId(93),
                &[ScalarValue::Int64(id), ScalarValue::Text(name.into())],
            )
            .expect("insert team");
    }

    let result = database
        .query(
            "SELECT u.id, t.name FROM users u JOIN teams t ON u.team_id = t.id \
             ORDER BY t.name ASC, u.id DESC",
        )
        .expect("ordered join");
    assert_eq!(
        result.rows,
        vec![
            vec![ScalarValue::Int64(3), ScalarValue::Text("alpha".into())],
            vec![ScalarValue::Int64(2), ScalarValue::Text("alpha".into())],
            vec![ScalarValue::Int64(1), ScalarValue::Text("beta".into())],
        ]
    );
    database.close().expect("close catalog");
    cleanup(&users_path);
    cleanup(&teams_path);
}

#[test]
fn chained_join_executes_left_associatively_and_handles_an_empty_left_side() {
    let base = std::env::temp_dir().join(format!(
        "netbadb-core-multi-join-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let a_path = base.with_extension("a.db");
    let b_path = base.with_extension("b.db");
    let c_path = base.with_extension("c.db");
    let a = TableDef::new(
        TableId(31),
        "a",
        vec![ColumnDef::new(
            ColumnId(1),
            "id",
            TypeSpec::Physical(PhysicalType::Int64),
        )],
    );
    let b = TableDef::new(
        TableId(32),
        "b",
        vec![
            ColumnDef::new(ColumnId(1), "id", TypeSpec::Physical(PhysicalType::Int64)),
            ColumnDef::new(ColumnId(2), "a_id", TypeSpec::Physical(PhysicalType::Int64)),
        ],
    );
    let c = TableDef::new(
        TableId(33),
        "c",
        vec![
            ColumnDef::new(ColumnId(1), "b_id", TypeSpec::Physical(PhysicalType::Int64)),
            ColumnDef::new(ColumnId(2), "name", TypeSpec::Physical(PhysicalType::Text)),
        ],
    );
    let mut database = Database::create_tables(vec![
        (a_path.clone(), a),
        (b_path.clone(), b),
        (c_path.clone(), c),
    ])
    .expect("create three-table catalog");
    database
        .insert_into(
            TableId(32),
            &[ScalarValue::Int64(20), ScalarValue::Int64(10)],
        )
        .expect("insert b");
    database
        .insert_into(
            TableId(33),
            &[ScalarValue::Int64(20), ScalarValue::Text("match".into())],
        )
        .expect("insert c");
    assert!(
        database
            .query(
                "SELECT c.name FROM a JOIN b ON a.id = b.a_id \
                 JOIN c ON b.id = c.b_id",
            )
            .expect("left-empty multi join")
            .rows
            .is_empty()
    );
    database
        .insert_into(TableId(31), &[ScalarValue::Int64(10)])
        .expect("insert a");
    assert_eq!(
        database
            .query(
                "SELECT a.id, c.name FROM a JOIN b ON a.id = b.a_id \
                 JOIN c ON b.id = c.b_id",
            )
            .expect("multi join")
            .rows,
        vec![vec![
            ScalarValue::Int64(10),
            ScalarValue::Text("match".into()),
        ]]
    );
    database.close().expect("close three-table catalog");
    cleanup(&a_path);
    cleanup(&b_path);
    cleanup(&c_path);
}

#[test]
fn self_join_uses_independent_relation_bindings() {
    let path = std::env::temp_dir().join(format!(
        "netbadb-core-self-join-{}-{:?}.db",
        std::process::id(),
        std::thread::current().id()
    ));
    let employees = TableDef::new(
        TableId(21),
        "employees",
        vec![
            ColumnDef::new(ColumnId(1), "id", TypeSpec::Physical(PhysicalType::Int64)),
            ColumnDef::new(
                ColumnId(2),
                "manager_id",
                TypeSpec::Physical(PhysicalType::Int64),
            )
            .nullable(true),
            ColumnDef::new(ColumnId(3), "name", TypeSpec::Physical(PhysicalType::Text)),
        ],
    );
    let mut database = Database::create(&path, employees).expect("create employees");
    for row in [
        vec![
            ScalarValue::Int64(1),
            ScalarValue::Null,
            ScalarValue::Text("CEO".into()),
        ],
        vec![
            ScalarValue::Int64(2),
            ScalarValue::Int64(1),
            ScalarValue::Text("Ada".into()),
        ],
        vec![
            ScalarValue::Int64(3),
            ScalarValue::Int64(1),
            ScalarValue::Text("Lin".into()),
        ],
    ] {
        database.insert(&row).expect("insert employee");
    }
    let result = database
        .query(
            "SELECT e.name, m.name FROM employees e JOIN employees m \
             ON e.manager_id = m.id",
        )
        .expect("self join");
    assert_eq!(
        result.rows,
        vec![
            vec![
                ScalarValue::Text("Ada".into()),
                ScalarValue::Text("CEO".into())
            ],
            vec![
                ScalarValue::Text("Lin".into()),
                ScalarValue::Text("CEO".into())
            ],
        ]
    );
    database.close().expect("close employees");
    cleanup(&path);
}
