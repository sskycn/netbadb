use netbadb_core::{
    ChangeStreamError, Database, DatabaseCoordinatorConfig, DatabaseError, StorageError,
};
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_types::{ColumnId, PhysicalType, ScalarValue, TableId};

fn table(id: u64, name: &str) -> TableDef {
    TableDef::new(
        TableId(id),
        name,
        vec![ColumnDef::new(
            ColumnId(1),
            "id",
            TypeSpec::Physical(PhysicalType::Int64),
        )],
    )
}

#[test]
fn coordinator_correlates_local_batches_without_creating_global_order() {
    let root = std::env::temp_dir().join(format!(
        "netbadb-core-change-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir(&root).expect("create root");
    let first_path = root.join("first.db");
    let second_path = root.join("second.db");
    let coordinator = root.join("coordinator");
    let first_table = table(1, "first");
    let second_table = table(2, "second");

    let mut database = Database::create_tables_with_coordinator(
        vec![
            (first_path, first_table.clone()),
            (second_path, second_table.clone()),
        ],
        DatabaseCoordinatorConfig::new(coordinator),
    )
    .expect("create database");
    let first_cursor = database
        .enable_change_stream(TableId(1))
        .expect("enable first");
    let second_cursor = database
        .enable_change_stream(TableId(2))
        .expect("enable second");

    let mut transaction = database
        .begin_transaction_for(TableId(1))
        .expect("begin transaction");
    let database_txn_id = transaction.id();
    database
        .insert_into_in(TableId(1), &mut transaction, &[ScalarValue::Int64(1)])
        .expect("insert first");
    database
        .insert_into_in(TableId(2), &mut transaction, &[ScalarValue::Int64(2)])
        .expect("insert second");
    transaction
        .commit()
        .expect("commit coordinated transaction");
    drop(transaction);

    let first = database
        .read_changes(TableId(1), first_cursor, 10, 1_000_000)
        .expect("read first");
    let second = database
        .read_changes(TableId(2), second_cursor, 10, 1_000_000)
        .expect("read second");
    assert_eq!(first.batches.len(), 1);
    assert_eq!(second.batches.len(), 1);
    assert_eq!(first.batches[0].database_txn_id, Some(database_txn_id));
    assert_eq!(second.batches[0].database_txn_id, Some(database_txn_id));
    assert_ne!(first.batches[0].storage_id, second.batches[0].storage_id);
    assert!(matches!(
        database.read_changes(TableId(1), second_cursor, 10, 1_000_000),
        Err(DatabaseError::Storage(StorageError::ChangeStream(
            ChangeStreamError::ContextMismatch
        )))
    ));

    database.close().expect("close database");
    let _ = std::fs::remove_dir_all(root);
}
