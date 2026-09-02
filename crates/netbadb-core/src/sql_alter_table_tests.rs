use super::*;
use netbadb_schema::{ColumnDef, TypeSpec};
use netbadb_types::{IndexName, SemanticType};
use std::path::{Path, PathBuf};

fn root(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "netbadb-round26-{name}-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    std::fs::create_dir(&path).unwrap();
    path
}

fn seed(root: &Path) -> Database {
    let mut db = Database::create_catalog(
        root.join("catalog"),
        vec![TableStorageCreateSpec::heap(
            root.join("seed.heap"),
            TableDef::new(
                TableId(1),
                "seed",
                vec![ColumnDef::new(
                    ColumnId(1),
                    "id",
                    TypeSpec::Physical(PhysicalType::Int64),
                )],
            ),
        )],
        Some(DatabaseCoordinatorConfig::new(root.join("coordinator"))),
    )
    .unwrap();
    db.execute("CREATE TABLE projects (id BIGINT NOT NULL, name TEXT)")
        .unwrap();
    db
}

fn files(path: &Path) -> Vec<(PathBuf, Vec<u8>)> {
    let mut result = Vec::new();
    for entry in std::fs::read_dir(path).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            result.extend(files(&path));
        } else {
            result.push((path.clone(), std::fs::read(path).unwrap()));
        }
    }
    result.sort();
    result
}

#[test]
fn sql_alter_prepare_is_pure_exact_and_schema_write_only() {
    let root = root("prepare-pure");
    let db = seed(&root);
    let expected = db.resolve_alter_table("projects").unwrap();
    let before = files(&root);
    let prepared = db
        .prepare_ddl_statement("ALTER TABLE projects ADD COLUMN active BOOL")
        .unwrap();
    assert!(prepared.is_table_alter());
    assert_eq!(prepared.alter_table_target(), Some(expected.into()));
    assert!(prepared.access().schema_write());
    assert_eq!(prepared.access().schema_tables(), [TableId(2)]);
    assert!(prepared.access().read_tables().is_empty());
    assert!(prepared.access().write_tables().is_empty());
    assert_eq!(
        prepared.created_column_types().cloned().collect::<Vec<_>>(),
        [SemanticType::physical(PhysicalType::Bool)]
    );
    assert_eq!(files(&root), before);
    assert_eq!(db.next_storage_id(), Some(StorageId(3)));
    assert_eq!(db.next_column_id(TableId(2)), Some(ColumnId(3)));
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn six_sql_alters_use_one_rewrite_lifecycle_and_preserve_logical_identities() {
    let root = root("six-operations");
    let mut db = seed(&root);
    let table_id = db.schema().table("projects").unwrap().id;
    db.execute("INSERT INTO projects VALUES (1, 'one')")
        .unwrap();
    db.execute("INSERT INTO projects VALUES (2, 'two')")
        .unwrap();
    let index = db
        .create_named_index(
            IndexName::new("projects_name_idx").unwrap(),
            table_id,
            ColumnId(2),
        )
        .unwrap();
    let initial_storage = db.bindings.resolve_single(table_id).unwrap();
    let initial_generation = db.schema_generation();

    let mut add = db.begin_transaction().unwrap();
    assert_eq!(
        db.execute_in(&mut add, "ALTER TABLE projects ADD COLUMN active BOOLEAN")
            .unwrap(),
        ExecutionResult::AffectedRows(0)
    );
    assert_eq!(
        db.execute_in(
            &mut add,
            "SELECT id, name, active FROM projects ORDER BY id"
        )
        .unwrap(),
        ExecutionResult::Query(QueryResult {
            columns: vec![
                ResultColumn {
                    name: "id".into(),
                    data_type: SemanticType::physical(PhysicalType::Int64),
                    nullable: false,
                },
                ResultColumn {
                    name: "name".into(),
                    data_type: SemanticType::physical(PhysicalType::Text),
                    nullable: true,
                },
                ResultColumn {
                    name: "active".into(),
                    data_type: SemanticType::physical(PhysicalType::Bool),
                    nullable: true,
                },
            ],
            rows: vec![
                vec![
                    ScalarValue::Int64(1),
                    ScalarValue::Text("one".into()),
                    ScalarValue::Null
                ],
                vec![
                    ScalarValue::Int64(2),
                    ScalarValue::Text("two".into()),
                    ScalarValue::Null
                ],
            ],
        })
    );
    db.execute_in(&mut add, "INSERT INTO projects VALUES (3, 'three', true)")
        .unwrap();
    db.commit_transaction(&mut add).unwrap();
    drop(add);

    for sql in [
        "ALTER TABLE projects RENAME COLUMN name TO title",
        "ALTER TABLE projects ALTER COLUMN title SET NOT NULL",
        "ALTER TABLE projects ALTER COLUMN title DROP NOT NULL",
        "ALTER TABLE projects DROP COLUMN active",
        "ALTER TABLE projects RENAME TO work",
    ] {
        assert_eq!(db.execute(sql).unwrap(), ExecutionResult::AffectedRows(0));
    }

    let table = db.schema().table("work").unwrap();
    assert_eq!(table.id, table_id);
    assert_eq!(table.columns[0].id, ColumnId(1));
    assert_eq!(table.columns[1].id, ColumnId(2));
    assert_eq!(table.columns[1].name, "title");
    assert!(table.columns[1].nullable);
    assert_eq!(db.indexes(table_id).unwrap()[0].id, index.id);
    assert_eq!(db.indexes(table_id).unwrap()[0].name, index.name);
    assert_eq!(db.indexes(table_id).unwrap()[0].column_id, ColumnId(2));
    assert_eq!(
        db.bindings.resolve_single(table_id).unwrap(),
        StorageId(initial_storage.0 + 6)
    );
    assert_eq!(
        db.table_schema_version(table_id),
        Some(TableSchemaVersion(7))
    );
    assert_eq!(
        db.schema_generation(),
        SchemaGeneration(initial_generation.0 + 6)
    );
    assert_eq!(
        db.query("SELECT id, title FROM work ORDER BY id")
            .unwrap()
            .rows,
        vec![
            vec![ScalarValue::Int64(1), ScalarValue::Text("one".into())],
            vec![ScalarValue::Int64(2), ScalarValue::Text("two".into())],
            vec![ScalarValue::Int64(3), ScalarValue::Text("three".into())],
        ]
    );
    let retired = db.inspect_replacement_retired_heaps();
    assert_eq!(retired.len(), 6);
    assert_eq!(retired[0].old_storage_id, initial_storage);
    assert_eq!(
        db.gc_replacement_retired_heap(&retired[0]).unwrap().state,
        RetiredHeapGcState::Deleted
    );
    db.close().unwrap();
    for _ in 0..3 {
        let mut reopened = Database::open_catalog(root.join("catalog")).unwrap();
        assert_eq!(reopened.schema().table("work").unwrap().id, table_id);
        assert_eq!(
            reopened.table_schema_version(table_id),
            Some(TableSchemaVersion(7))
        );
        assert_eq!(reopened.indexes(table_id).unwrap()[0].id, index.id);
        assert_eq!(
            reopened
                .query("SELECT id, title FROM work ORDER BY id")
                .unwrap()
                .rows
                .len(),
            3
        );
        reopened.close().unwrap();
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn prepared_alter_is_exact_but_ignores_data_and_index_only_revisions() {
    let root = root("prepared-exact");
    let mut db = seed(&root);
    db.execute("INSERT INTO projects VALUES (1, 'one')")
        .unwrap();
    let add = db
        .prepare_ddl_statement("ALTER TABLE projects ADD COLUMN active BOOL")
        .unwrap();
    db.execute("INSERT INTO projects VALUES (2, 'two')")
        .unwrap();
    db.create_named_index(
        IndexName::new("projects_name_idx").unwrap(),
        TableId(2),
        ColumnId(2),
    )
    .unwrap();
    assert_eq!(db.execute_ddl(&add).unwrap(), DdlOutcome::Altered);
    assert_eq!(
        db.query("SELECT id, active FROM projects ORDER BY id")
            .unwrap()
            .rows,
        vec![
            vec![ScalarValue::Int64(1), ScalarValue::Null],
            vec![ScalarValue::Int64(2), ScalarValue::Null],
        ]
    );
    let storage_after = db.next_storage_id();
    assert_eq!(
        db.execute_ddl(&add).unwrap_err().kind(),
        DatabaseErrorKind::TransactionState
    );
    assert_eq!(db.next_storage_id(), storage_after);

    let stale = db
        .prepare_ddl_statement("ALTER TABLE projects RENAME TO work")
        .unwrap();
    db.execute("ALTER TABLE projects RENAME COLUMN name TO title")
        .unwrap();
    let before = db.next_storage_id();
    assert_eq!(
        db.execute_ddl(&stale).unwrap_err().kind(),
        DatabaseErrorKind::TransactionState
    );
    assert_eq!(db.next_storage_id(), before);
    assert!(db.schema().table("projects").is_some());
    assert!(db.schema().table("work").is_none());
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn prepared_alter_never_rebinds_after_drop_and_same_name_recreate() {
    let root = root("drop-recreate-exact");
    let mut db = seed(&root);
    let prepared = db
        .prepare_ddl_statement("ALTER TABLE projects RENAME TO work")
        .unwrap();
    let original = prepared.alter_table_target().unwrap();
    db.execute("DROP TABLE projects").unwrap();
    db.execute("CREATE TABLE projects (id BIGINT NOT NULL, replacement TEXT)")
        .unwrap();
    let replacement = db.schema().table("projects").unwrap().id;
    assert_ne!(replacement, original.table_id);
    let storage = db.bindings.resolve_single(replacement).unwrap();
    let next_storage = db.next_storage_id();
    assert_eq!(
        db.execute_ddl(&prepared).unwrap_err().kind(),
        DatabaseErrorKind::UndefinedTable
    );
    assert_eq!(db.schema().table("projects").unwrap().id, replacement);
    assert!(db.schema().table("work").is_none());
    assert_eq!(db.bindings.resolve_single(replacement).unwrap(), storage);
    assert_eq!(db.next_storage_id(), next_storage);
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn prepared_drop_column_revalidates_new_index_and_pristine_admission() {
    let root = root("late-index-pristine");
    let mut db = seed(&root);
    let drop_name = db
        .prepare_ddl_statement("ALTER TABLE projects DROP COLUMN name")
        .unwrap();
    db.create_named_index(
        IndexName::new("projects_name_idx").unwrap(),
        TableId(2),
        ColumnId(2),
    )
    .unwrap();
    let before = db.next_storage_id();
    assert_eq!(
        db.execute_ddl(&drop_name).unwrap_err().kind(),
        DatabaseErrorKind::DependentObjects
    );
    assert_eq!(db.next_storage_id(), before);
    assert!(
        db.schema()
            .table("projects")
            .unwrap()
            .column("name")
            .is_some()
    );

    let prepared = db
        .prepare_ddl_statement("ALTER TABLE projects ADD COLUMN active BOOL")
        .unwrap();
    let mut transaction = db.begin_transaction().unwrap();
    db.execute_in(&mut transaction, "SELECT * FROM seed")
        .unwrap();
    assert_eq!(
        db.execute_ddl_in(&mut transaction, &prepared)
            .unwrap_err()
            .kind(),
        DatabaseErrorKind::TransactionState
    );
    assert_eq!(db.next_storage_id(), before);
    transaction.rollback().unwrap();
    drop(transaction);
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn sql_alter_rollback_burns_ids_but_restores_schema_and_rows() {
    let root = root("rollback");
    let mut db = seed(&root);
    db.execute("INSERT INTO projects VALUES (1, 'one')")
        .unwrap();
    let generation = db.schema_generation();
    let storage = db.bindings.resolve_single(TableId(2)).unwrap();
    let mut transaction = db.begin_transaction().unwrap();
    db.execute_in(
        &mut transaction,
        "ALTER TABLE projects ADD COLUMN active BOOLEAN",
    )
    .unwrap();
    db.execute_in(
        &mut transaction,
        "INSERT INTO projects VALUES (2, 'two', true)",
    )
    .unwrap();
    transaction.rollback().unwrap();
    drop(transaction);
    assert!(
        db.schema()
            .table("projects")
            .unwrap()
            .column("active")
            .is_none()
    );
    assert_eq!(db.query("SELECT * FROM projects").unwrap().rows.len(), 1);
    assert_eq!(db.schema_generation(), generation);
    assert_eq!(db.bindings.resolve_single(TableId(2)).unwrap(), storage);
    assert_eq!(db.next_storage_id(), Some(StorageId(storage.0 + 2)));
    assert_eq!(db.next_column_id(TableId(2)), Some(ColumnId(4)));
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn sql_alter_crash_child() {
    let Ok(root) = std::env::var("NETBADB_SQL_ALTER_CHILD") else {
        return;
    };
    let mut db = Database::open_catalog(Path::new(&root).join("catalog")).unwrap();
    db.execute("ALTER TABLE projects ADD COLUMN active BOOLEAN")
        .unwrap();
    panic!("configured SQL ALTER crash hook was not reached");
}

#[test]
fn sql_driven_alter_loser_and_winner_recover_three_times_without_reparse() {
    for (point, winner) in [
        ("before-coordinator-decision", false),
        ("coordinator-durable", true),
    ] {
        let root = root(&format!("crash-{point}"));
        let mut seeded = seed(&root);
        seeded
            .execute("INSERT INTO projects VALUES (1, 'one')")
            .unwrap();
        seeded.close().unwrap();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "sql_alter_table_tests::sql_alter_crash_child",
                "--nocapture",
            ])
            .env("NETBADB_SQL_ALTER_CHILD", &root)
            .env("NETBADB_REWRITE_CRASH_POINT", point)
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(90),
            "{point}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        for _ in 0..3 {
            let mut db = Database::open_catalog(root.join("catalog")).unwrap();
            let table = db.schema().table("projects").unwrap();
            assert_eq!(table.id, TableId(2));
            assert_eq!(
                db.schema_generation(),
                SchemaGeneration(if winner { 3 } else { 2 })
            );
            assert_eq!(
                db.table_schema_version(table.id),
                Some(TableSchemaVersion(if winner { 2 } else { 1 }))
            );
            assert_eq!(
                db.bindings.resolve_single(table.id).unwrap(),
                StorageId(if winner { 3 } else { 2 })
            );
            assert_eq!(db.next_storage_id(), Some(StorageId(4)));
            assert_eq!(db.next_column_id(table.id), Some(ColumnId(4)));
            assert_eq!(table.column("active").is_some(), winner);
            if winner {
                assert_eq!(
                    db.query("SELECT id, active FROM projects").unwrap().rows,
                    [vec![ScalarValue::Int64(1), ScalarValue::Null]]
                );
                assert_eq!(db.inspect_replacement_retired_heaps().len(), 1);
            } else {
                assert!(db.inspect_replacement_retired_heaps().is_empty());
            }
            db.close().unwrap();
        }
        std::fs::remove_dir_all(root).unwrap();
    }
}
