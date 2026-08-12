use netbadb_core::{Database, DatabaseError};
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_storage::StorageError;
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
