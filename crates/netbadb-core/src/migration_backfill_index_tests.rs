use super::*;
use std::path::PathBuf;

fn test_root(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "netbadb-round33-{name}-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    std::fs::create_dir(&path).unwrap();
    path
}

#[test]
fn post_backfill_create_index_uses_final_schema_and_one_staged_storage() {
    let root = test_root("index");
    let mut db = Database::create_catalog(
        root.join("catalog"),
        vec![TableStorageCreateSpec::heap(
            root.join("seed.heap"),
            netbadb_schema::TableDef::new(
                TableId(1),
                "seed",
                vec![netbadb_schema::ColumnDef::new(
                    ColumnId(1),
                    "id",
                    netbadb_schema::TypeSpec::Physical(PhysicalType::Int64),
                )],
            ),
        )],
        Some(DatabaseCoordinatorConfig::new(root.join("coordinator"))),
    )
    .unwrap();
    db.execute("CREATE TABLE users (id BIGINT NOT NULL, name TEXT)")
        .unwrap();
    db.execute("INSERT INTO users VALUES (1, 'one')").unwrap();
    db.execute("INSERT INTO users VALUES (2, 'two')").unwrap();

    let users = db.schema().table("users").unwrap().id;
    let old_storage = db.bindings.resolve_single(users).unwrap();
    let mut transaction = db.begin_transaction().unwrap();
    db.execute_in(
        &mut transaction,
        "ALTER TABLE users ADD COLUMN normalized_name TEXT",
    )
    .unwrap();
    db.execute_in(
        &mut transaction,
        "UPDATE users SET normalized_name = 'filled'",
    )
    .unwrap();
    db.execute_in(
        &mut transaction,
        "INSERT INTO users VALUES (3, 'three', 'filled')",
    )
    .unwrap();
    db.execute_in(&mut transaction, "DELETE FROM users WHERE id = 2")
        .unwrap();
    db.execute_in(
        &mut transaction,
        "ALTER TABLE users ALTER COLUMN normalized_name SET NOT NULL",
    )
    .unwrap();
    let staged_storage = transaction.staged_binding(users).unwrap();
    assert_ne!(staged_storage, old_storage);
    assert!(matches!(
        transaction.schema_composition,
        schema_composition::SchemaCompositionState::Refining(_)
    ));

    db.execute_in(
        &mut transaction,
        "CREATE INDEX users_normalized_name_idx ON users(normalized_name)",
    )
    .unwrap();
    assert!(matches!(
        transaction.schema_composition,
        schema_composition::SchemaCompositionState::IndexFinalizing(_)
    ));
    assert_eq!(transaction.staged_binding(users), Some(staged_storage));
    assert!(matches!(
        db.execute_in(&mut transaction, "SELECT id FROM users"),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::MigrationDataAccessAfterRefinement
        ))
    ));

    db.commit_transaction(&mut transaction).unwrap();
    let indexes = db.indexes(users).unwrap();
    assert_eq!(indexes.len(), 1);
    assert_eq!(indexes[0].id, netbadb_types::IndexId(1));
    assert_eq!(
        indexes[0].column_id,
        db.schema()
            .table("users")
            .unwrap()
            .column("normalized_name")
            .unwrap()
            .id
    );
    assert_eq!(db.bindings.resolve_single(users).unwrap(), staged_storage);
    assert_eq!(
        db.query("SELECT id, normalized_name FROM users ORDER BY id")
            .unwrap()
            .rows,
        vec![
            vec![ScalarValue::Int64(1), ScalarValue::Text("filled".into())],
            vec![ScalarValue::Int64(3), ScalarValue::Text("filled".into())]
        ]
    );
    db.close().unwrap();
}

#[test]
fn post_backfill_drop_and_recreate_burns_id_without_a_transient_tree() {
    let root = test_root("recreate");
    let mut db = Database::create_catalog(
        root.join("catalog"),
        vec![TableStorageCreateSpec::heap(
            root.join("seed.heap"),
            netbadb_schema::TableDef::new(
                TableId(1),
                "seed",
                vec![netbadb_schema::ColumnDef::new(
                    ColumnId(1),
                    "id",
                    netbadb_schema::TypeSpec::Physical(PhysicalType::Int64),
                )],
            ),
        )],
        Some(DatabaseCoordinatorConfig::new(root.join("coordinator"))),
    )
    .unwrap();
    db.execute("CREATE TABLE users (id BIGINT NOT NULL)")
        .unwrap();
    db.execute("INSERT INTO users VALUES (1)").unwrap();
    db.execute("CREATE INDEX users_id_idx ON users(id)")
        .unwrap();
    let users = db.schema().table("users").unwrap().id;
    let old_id = db.indexes(users).unwrap()[0].id;
    let mut transaction = db.begin_transaction().unwrap();
    db.execute_in(&mut transaction, "ALTER TABLE users ADD COLUMN marker TEXT")
        .unwrap();
    db.execute_in(&mut transaction, "UPDATE users SET marker = 'x'")
        .unwrap();
    db.execute_in(
        &mut transaction,
        "CREATE INDEX users_marker_idx ON users(marker)",
    )
    .unwrap();
    db.execute_in(&mut transaction, "DROP INDEX users_marker_idx")
        .unwrap();
    db.execute_in(
        &mut transaction,
        "CREATE INDEX users_marker_idx ON users(marker)",
    )
    .unwrap();
    db.commit_transaction(&mut transaction).unwrap();
    let indexes = db.indexes(users).unwrap();
    assert_eq!(indexes.len(), 2);
    assert!(indexes.iter().any(|index| index.id.0 > old_id.0));
    db.close().unwrap();
}
