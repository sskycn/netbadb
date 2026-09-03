use super::*;
use netbadb_schema::{ColumnDef, TypeSpec};
use netbadb_types::{ColumnId, IndexName};
use std::path::{Path, PathBuf};

const DROP: &str = "DROP TABLE projects;";

fn root(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "netbadb-round21-{name}-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    std::fs::create_dir(&path).unwrap();
    path
}

fn seed(root: &Path) -> Database {
    let mut db = Database::create_catalog(
        root.join("catalog"),
        vec![],
        Some(DatabaseCoordinatorConfig::new(root.join("coordinator"))),
    )
    .unwrap();
    db.execute("CREATE TABLE projects (id BIGINT NOT NULL)")
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
fn sql_prepare_is_pure_and_access_carries_only_the_exact_schema_target() {
    let root = root("prepare-pure");
    let db = seed(&root);
    let target = db.resolve_drop_table("projects").unwrap();
    let before = files(&root);
    let prepared = db.prepare_ddl_statement(DROP).unwrap();
    assert_eq!(prepared.drop_table_target(), Some(target));
    assert!(prepared.access().schema_write());
    assert_eq!(prepared.access().schema_tables(), [TableId(1)]);
    assert!(prepared.access().read_tables().is_empty());
    assert!(prepared.access().write_tables().is_empty());
    let PreparedSqlStatement::Ddl(generic) = db.prepare_sql_statement(DROP, &[]).unwrap() else {
        panic!("generic DDL")
    };
    assert_eq!(generic.drop_table_target(), Some(target));
    assert_eq!(files(&root), before);
    assert_eq!(db.schema_generation(), SchemaGeneration(2));
    assert_eq!(db.next_table_id(), Some(TableId(2)));
    assert_eq!(db.next_storage_id(), Some(StorageId(2)));
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn sql_drop_rollback_then_commit_preserves_overlay_indexes_retirement_and_reopen() {
    let root = root("lifecycle");
    let mut db = seed(&root);
    db.execute("INSERT INTO projects VALUES (7)").unwrap();
    db.create_named_index(
        IndexName::new("projects_id_idx").unwrap(),
        TableId(1),
        ColumnId(1),
    )
    .unwrap();
    let target = db.resolve_drop_table("projects").unwrap();
    let high_waters = (db.next_table_id(), db.next_storage_id());
    let mut transaction = db.begin_transaction().unwrap();
    assert_eq!(
        db.execute_in(&mut transaction, DROP).unwrap(),
        ExecutionResult::AffectedRows(0)
    );
    assert!(
        db.prepare_sql_statement_in(&transaction, "SELECT * FROM projects", &[])
            .is_err()
    );
    assert!(db.schema().table("projects").is_some());
    assert!(db.inspect_retired_table_resources().is_empty());
    transaction.rollback().unwrap();
    drop(transaction);
    assert_eq!(db.query("SELECT * FROM projects").unwrap().rows.len(), 1);
    assert_eq!(db.indexes(TableId(1)).unwrap().len(), 1);
    assert_eq!((db.next_table_id(), db.next_storage_id()), high_waters);

    let mut transaction = db.begin_transaction().unwrap();
    db.execute_in(&mut transaction, DROP).unwrap();
    db.commit_transaction(&mut transaction).unwrap();
    drop(transaction);
    assert!(db.schema().table("projects").is_none());
    assert_eq!(db.schema_generation(), SchemaGeneration(3));
    assert_eq!((db.next_table_id(), db.next_storage_id()), high_waters);
    let retired = db.inspect_retired_table_resources();
    assert_eq!(retired.len(), 1);
    assert_eq!(retired[0].table_id, target.table_id);
    assert_eq!(retired[0].fingerprint, target.fingerprint);
    let retired_path =
        crate::schema_catalog_file::resolve(&root.join("catalog"), &retired[0].relative_locator);
    assert!(retired_path.is_file());
    db.close().unwrap();
    for _ in 0..3 {
        let reopened = Database::open_catalog(root.join("catalog")).unwrap();
        assert!(reopened.schema().table("projects").is_none());
        assert_eq!(reopened.inspect_retired_table_resources(), retired);
        reopened.close().unwrap();
    }
    let retired_heap = netbadb_storage::TableStorage::open_heap(
        retired_path,
        TableDef::new(
            TableId(1),
            "projects",
            vec![ColumnDef::new(
                ColumnId(1),
                "id",
                TypeSpec::Physical(PhysicalType::Int64),
            )],
        ),
    )
    .unwrap();
    assert_eq!(retired_heap.indexes().len(), 1);
    retired_heap.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn prepared_sql_drop_never_rebinds_to_a_same_name_replacement() {
    let root = root("same-name");
    let mut db = seed(&root);
    db.execute("INSERT INTO projects VALUES (1)").unwrap();
    db.create_named_index(
        IndexName::new("projects_id_idx").unwrap(),
        TableId(1),
        ColumnId(1),
    )
    .unwrap();
    let prepared = db.prepare_ddl_statement(DROP).unwrap();
    let old_target = prepared.drop_table_target().unwrap();
    let old_storage = db.bindings.resolve_single(old_target.table_id).unwrap();
    assert_eq!(
        (old_target.table_id, old_storage),
        (TableId(1), StorageId(1))
    );
    assert_eq!(db.execute_ddl(&prepared).unwrap(), DdlOutcome::Dropped);
    assert_eq!(
        db.execute("CREATE TABLE projects (id BIGINT NOT NULL)")
            .unwrap(),
        ExecutionResult::AffectedRows(0)
    );
    let replacement = db.schema().table("projects").unwrap().id;
    let replacement_storage = db.bindings.resolve_single(replacement).unwrap();
    assert_eq!(
        (replacement, replacement_storage),
        (TableId(2), StorageId(2))
    );
    assert_eq!(
        db.table_schema_version(replacement),
        Some(TableSchemaVersion(1))
    );
    assert_eq!(db.schema_generation(), SchemaGeneration(4));
    assert_eq!(db.next_table_id(), Some(TableId(3)));
    assert_eq!(db.next_storage_id(), Some(StorageId(3)));
    assert_eq!(db.indexes(replacement).unwrap(), []);
    assert_eq!(db.inspect_retired_table_resources().len(), 1);
    assert_eq!(
        db.inspect_retired_table_resources()[0].storage_id,
        old_storage
    );
    assert_eq!(
        db.execute_ddl(&prepared).unwrap_err().kind(),
        DatabaseErrorKind::UndefinedTable
    );
    assert_eq!(db.schema().table("projects").unwrap().id, replacement);
    assert!(db.query("SELECT * FROM projects").unwrap().rows.is_empty());
    let retired = &db.inspect_retired_table_resources()[0];
    assert!(
        crate::schema_catalog_file::resolve(&root.join("catalog"), &retired.relative_locator,)
            .is_file()
    );
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn prepared_sql_drop_survives_index_and_data_changes() {
    let root = root("nonschema-changes");
    let mut db = seed(&root);
    let prepared = db.prepare_ddl_statement(DROP).unwrap();
    db.create_named_index(
        IndexName::new("projects_id_idx").unwrap(),
        TableId(1),
        ColumnId(1),
    )
    .unwrap();
    db.execute("INSERT INTO projects VALUES (11)").unwrap();
    assert_eq!(db.execute_ddl(&prepared).unwrap(), DdlOutcome::Dropped);
    assert!(db.schema().table("projects").is_none());
    assert_eq!(db.inspect_retired_table_resources().len(), 1);
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn sql_drop_reports_missing_table_without_allocating_or_mutating_catalog() {
    let root = root("missing");
    let mut db = seed(&root);
    let before = files(&root);
    let high_waters = (db.next_table_id(), db.next_storage_id());
    assert_eq!(
        db.execute("DROP TABLE missing").unwrap_err().kind(),
        DatabaseErrorKind::UndefinedTable
    );
    assert_eq!(files(&root), before);
    assert_eq!((db.next_table_id(), db.next_storage_id()), high_waters);
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}
