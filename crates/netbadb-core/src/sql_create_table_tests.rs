use super::*;
use netbadb_schema::{ColumnDef, TypeSpec};
use netbadb_types::{ColumnId, SemanticType};
use std::path::{Path, PathBuf};

const CREATE: &str =
    "CREATE TABLE projects (id BIGINT NOT NULL, name TEXT, active BOOLEAN NOT NULL)";

fn root(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("netbadb-round19-{name}-{}", std::process::id()));
    std::fs::create_dir(&path).unwrap();
    path
}
fn seed(root: &Path) -> Database {
    Database::create_catalog(
        root.join("catalog"),
        vec![
            TableStorageCreateSpec::heap(
                root.join("users"),
                TableDef::new(
                    TableId(1),
                    "users",
                    vec![ColumnDef::new(
                        ColumnId(1),
                        "id",
                        TypeSpec::Physical(PhysicalType::Int64),
                    )],
                ),
            ),
            TableStorageCreateSpec::heap(
                root.join("teams"),
                TableDef::new(
                    TableId(2),
                    "teams",
                    vec![ColumnDef::new(
                        ColumnId(1),
                        "id",
                        TypeSpec::Physical(PhysicalType::Int64),
                    )],
                ),
            ),
        ],
        None,
    )
    .unwrap()
}
fn rows(result: ExecutionResult) -> Vec<Vec<ScalarValue>> {
    let ExecutionResult::Query(q) = result else {
        panic!()
    };
    q.rows
}
fn expected() -> Vec<Vec<ScalarValue>> {
    vec![vec![
        ScalarValue::Int64(10),
        ScalarValue::Text("demo".into()),
        ScalarValue::Bool(true),
    ]]
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
fn prepare_and_invalid_sql_are_pure_then_execute_rechecks_duplicate() {
    let root = root("pure");
    let mut db = seed(&root);
    let before = files(&root);
    let prepared = db.prepare_ddl_statement(CREATE).unwrap();
    assert!(prepared.access().schema_write());
    assert!(prepared.access().read_tables().is_empty());
    assert!(prepared.access().write_tables().is_empty());
    assert_eq!(db.next_table_id(), Some(TableId(3)));
    assert_eq!(db.next_storage_id(), Some(StorageId(3)));
    assert_eq!(db.schema_generation(), SchemaGeneration(1));
    for sql in [
        "CREATE TABLE bad (id BIGINT PRIMARY KEY)",
        "CREATE TABLE bad (a TEXT, a TEXT)",
        "CREATE TABLE bad (a MAGIC)",
        "CREATE TABLE bad (a INTEGER)",
        "CREATE TABLE bad (\"\" TEXT)",
        "CREATE TABLE $1 (a TEXT)",
    ] {
        assert!(db.execute(sql).is_err(), "{sql}");
    }
    assert_eq!(files(&root), before);
    // A prepared declaration must not hold schema-writer admission.
    let other = db.begin_transaction().unwrap();
    assert_eq!(
        db.execute_ddl(&prepared).unwrap_err().kind(),
        DatabaseErrorKind::SchemaBusy
    );
    drop(other);
    assert_eq!(db.execute_ddl(&prepared).unwrap(), DdlOutcome::Created);
    assert_eq!(
        db.execute_ddl(&prepared).unwrap_err().kind(),
        DatabaseErrorKind::DuplicateObject
    );
    assert_eq!(db.next_table_id(), Some(TableId(4)));
    assert_eq!(db.schema_generation(), SchemaGeneration(2));
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn sql_rollback_commit_overlay_identity_generation_and_catalog_only_reopen() {
    let root = root("lifecycle");
    let mut db = seed(&root);
    db.execute("INSERT INTO users VALUES (1)").unwrap();
    let old = db.prepare_statement("SELECT id FROM users", &[]).unwrap();
    let mut txn = db.begin_transaction().unwrap();
    db.execute_in(&mut txn, CREATE).unwrap();
    assert!(txn.owns_staged_table(TableId(3)));
    assert!(db.prepare_statement("SELECT * FROM projects", &[]).is_err());
    let insert = db
        .prepare_statement_in(&txn, "INSERT INTO projects VALUES ($1, $2, $3)", &[])
        .unwrap();
    db.execute_prepared_in(&mut txn, &insert, &expected()[0])
        .unwrap();
    assert_eq!(
        rows(
            db.execute_in(&mut txn, "SELECT * FROM projects WHERE id = 10")
                .unwrap()
        ),
        expected()
    );
    txn.rollback().unwrap();
    assert!(!txn.owns_staged_table(TableId(3)));
    assert!(db.execute_prepared(&insert, &expected()[0]).is_err());
    drop(txn);
    assert!(db.schema().table("projects").is_none());
    assert_eq!(db.schema_generation(), SchemaGeneration(1));
    assert_eq!(db.catalog_generation(), 0);
    assert_eq!(db.next_table_id(), Some(TableId(4)));
    assert_eq!(db.next_storage_id(), Some(StorageId(4)));
    let mut txn = db.begin_transaction().unwrap();
    db.execute_in(&mut txn, "UPDATE users SET id = 2").unwrap();
    db.execute_in(&mut txn, CREATE).unwrap();
    db.execute_in(
        &mut txn,
        "INSERT INTO projects (id, name, active) VALUES (10, 'demo', true)",
    )
    .unwrap();
    db.commit_transaction(&mut txn).unwrap();
    drop(txn);
    assert_eq!(
        rows(db.execute_prepared(&old, &[]).unwrap()),
        vec![vec![ScalarValue::Int64(2)]]
    );
    let table = db.schema().table("projects").unwrap().clone();
    assert_eq!(table.id, TableId(4));
    assert_eq!(db.bindings.resolve_single(table.id).unwrap(), StorageId(4));
    assert_eq!(db.schema_generation(), SchemaGeneration(2));
    assert_eq!(db.catalog_generation(), 1);
    assert_eq!(
        db.table_schema_version(table.id),
        Some(TableSchemaVersion(1))
    );
    db.close().unwrap();
    for _ in 0..3 {
        let mut db = Database::open_catalog(root.join("catalog")).unwrap();
        assert_eq!(db.schema().table("projects"), Some(&table));
        assert_eq!(db.query("SELECT * FROM projects").unwrap().rows, expected());
        assert_eq!(db.schema_generation(), SchemaGeneration(2));
        assert_eq!(
            db.table_schema_version(table.id),
            Some(TableSchemaVersion(1))
        );
        assert_eq!(db.bindings.resolve_single(table.id).unwrap(), StorageId(4));
        db.close().unwrap();
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn sql_and_direct_core_creation_have_identical_schema_and_placement() {
    let sql_root = root("equiv-sql");
    let direct_root = root("equiv-direct");
    let mut sql_db = seed(&sql_root);
    let mut direct_db = seed(&direct_root);
    sql_db.execute(CREATE).unwrap();
    let mut txn = direct_db.begin_transaction().unwrap();
    direct_db
        .create_heap_table_in(
            &mut txn,
            CreateTableSpec::new(
                "projects",
                vec![
                    CreateColumnSpec::new("id", SemanticType::physical(PhysicalType::Int64), false),
                    CreateColumnSpec::new("name", SemanticType::physical(PhysicalType::Text), true),
                    CreateColumnSpec::new(
                        "active",
                        SemanticType::physical(PhysicalType::Bool),
                        false,
                    ),
                ],
            ),
        )
        .unwrap();
    direct_db.commit_transaction(&mut txn).unwrap();
    drop(txn);
    assert_eq!(sql_db.schema(), direct_db.schema());
    assert_eq!(
        sql_db
            .schema()
            .table("projects")
            .unwrap()
            .fingerprint()
            .unwrap(),
        direct_db
            .schema()
            .table("projects")
            .unwrap()
            .fingerprint()
            .unwrap()
    );
    assert_eq!(
        sql_db.inspect_catalog().unwrap(),
        direct_db.inspect_catalog().unwrap()
    );
    sql_db.close().unwrap();
    direct_db.close().unwrap();
    std::fs::remove_dir_all(sql_root).unwrap();
    std::fs::remove_dir_all(direct_root).unwrap();
}

#[test]
fn sql_rejects_multiple_creates_and_index_mixing_and_enforces_not_null() {
    let root = root("mixing");
    let mut db = seed(&root);
    let mut txn = db.begin_transaction().unwrap();
    db.execute_in(&mut txn, CREATE).unwrap();
    for sql in [
        "CREATE TABLE other (id INT64)",
        "CREATE INDEX i ON projects (id)",
        "DROP INDEX missing",
    ] {
        assert_eq!(
            db.execute_in(&mut txn, sql).unwrap_err().kind(),
            DatabaseErrorKind::FeatureNotSupported
        );
    }
    assert!(
        db.execute_in(&mut txn, "INSERT INTO projects VALUES (NULL, 'bad', true)")
            .is_err()
    );
    txn.rollback().unwrap();
    drop(txn);
    db.execute("CREATE INDEX existing ON users (id)").unwrap();
    let mut txn = db.begin_transaction().unwrap();
    db.execute_in(&mut txn, "DROP INDEX existing").unwrap();
    assert_eq!(
        db.execute_in(&mut txn, CREATE).unwrap_err().kind(),
        DatabaseErrorKind::FeatureNotSupported
    );
    txn.rollback().unwrap();
    drop(txn);
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn sql_staging_failure_consumes_reservations_only_after_execute() {
    let root = root("failure");
    let mut db = seed(&root);
    let prepared = db.prepare_ddl_statement(CREATE).unwrap();
    let mut txn = db.begin_transaction().unwrap();
    let snapshot = crate::schema_catalog_file::load(&root.join("catalog")).unwrap();
    let stage = crate::schema_catalog_file::resolve(
        &root.join("catalog"),
        &crate::schema_mutation_journal::stage_locator(
            &root.join("catalog"),
            snapshot.incarnation,
            txn.id(),
            StorageId(3),
        )
        .unwrap(),
    );
    crate::schema_mutation::ensure_parent(&stage).unwrap();
    std::fs::write(
        crate::schema_catalog_file::suffix(&stage, ".owner"),
        b"injected collision",
    )
    .unwrap();
    assert!(db.execute_ddl_in(&mut txn, &prepared).is_err());
    assert_eq!(txn.state(), TransactionState::RollbackRequired);
    assert!(db.commit_transaction(&mut txn).is_err());
    txn.rollback().unwrap();
    drop(txn);
    assert_eq!(db.next_table_id(), Some(TableId(4)));
    assert_eq!(db.next_storage_id(), Some(StorageId(4)));
    assert_eq!(db.schema_generation(), SchemaGeneration(1));
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn sql_create_crash_child() {
    let Ok(root) = std::env::var("NETBADB_SQL_CREATE_CHILD") else {
        return;
    };
    let mut db = Database::open_catalog(Path::new(&root).join("catalog")).unwrap();
    let mut txn = db.begin_transaction().unwrap();
    db.execute_in(&mut txn, CREATE).unwrap();
    db.execute_in(&mut txn, "INSERT INTO projects VALUES (10, 'demo', true)")
        .unwrap();
    db.commit_transaction(&mut txn).unwrap();
    panic!("crash point not reached");
}

#[test]
fn sql_driven_crash_loser_and_winner_recover_from_catalog() {
    for (point, winner) in [
        ("before-coordinator-decision", false),
        ("coordinator-durable", true),
        ("during-nbsc-publication", true),
    ] {
        let root = root(&format!("crash-{point}"));
        seed(&root).close().unwrap();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "sql_create_table_tests::sql_create_crash_child",
                "--nocapture",
            ])
            .env("NETBADB_SQL_CREATE_CHILD", &root)
            .env("NETBADB_CREATE_CRASH_POINT", point)
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(90),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        for _ in 0..3 {
            let mut db = Database::open_catalog(root.join("catalog")).unwrap();
            assert_eq!(db.schema().table("projects").is_some(), winner);
            assert_eq!(db.next_table_id(), Some(TableId(4)));
            assert_eq!(db.next_storage_id(), Some(StorageId(4)));
            assert_eq!(
                db.schema_generation(),
                SchemaGeneration(if winner { 2 } else { 1 })
            );
            if winner {
                assert_eq!(db.query("SELECT * FROM projects").unwrap().rows, expected());
            }
            db.close().unwrap();
        }
        std::fs::remove_dir_all(root).unwrap();
    }
}
