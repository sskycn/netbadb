use super::*;
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_storage::{
    ChangeStreamError, ChangeStreamStatus, StorageChange, StorageError, TableStorage,
};
use netbadb_types::{ColumnId, PhysicalType, StorageDataVersion, StorageId, TableId};
use std::path::{Path, PathBuf};

fn root(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "netbadb-round51-{name}-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    std::fs::create_dir_all(&path).unwrap();
    path
}

fn seed(path: &Path) -> Database {
    let mut database = Database::create_catalog(
        path.join("catalog"),
        vec![TableStorageCreateSpec::heap(
            path.join("seed.heap"),
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
        Some(DatabaseCoordinatorConfig::new(path.join("coordinator"))),
    )
    .unwrap();
    database
        .execute("CREATE TABLE users (id BIGINT NOT NULL, legacy TEXT, flag BOOLEAN)")
        .unwrap();
    for statement in [
        "INSERT INTO users VALUES (1, 'one', true)",
        "INSERT INTO users VALUES (2, 'two', false)",
        "INSERT INTO users VALUES (3, 'three', true)",
    ] {
        database.execute(statement).unwrap();
    }
    database
}

fn users(database: &Database) -> TableId {
    database.schema().table("users").unwrap().id
}

fn assert_disabled(error: DatabaseError) {
    assert!(matches!(
        error,
        DatabaseError::Storage(StorageError::ChangeStream(ChangeStreamError::Disabled))
    ));
}

fn execute_round50_replacement(database: &mut Database) -> (DatabaseTxnId, StorageId) {
    let table = users(database);
    let source = database.bindings.resolve_single(table).unwrap();
    let mut transaction = database.begin_transaction().unwrap();
    for statement in [
        "UPDATE users SET legacy = 'updated' WHERE id = 1",
        "INSERT INTO users VALUES (4, 'four', false)",
        "DELETE FROM users WHERE id = 2",
        "ALTER TABLE users ADD COLUMN marker TEXT",
        "UPDATE users SET marker = legacy",
        "ALTER TABLE users ALTER COLUMN marker SET NOT NULL",
        "CREATE INDEX users_marker_idx ON users(marker)",
    ] {
        database.execute_in(&mut transaction, statement).unwrap();
    }
    let transaction_id = transaction.id();
    database.commit_transaction(&mut transaction).unwrap();
    drop(transaction);
    assert_ne!(database.bindings.resolve_single(table).unwrap(), source);
    (transaction_id, source)
}

#[test]
fn current_round50_replacement_commits_an_unreachable_final_s1_batch_then_gc_removes_it() {
    let root = root("current-silent-abandonment");
    let mut database = seed(&root);
    let table = users(&database);
    let old_table = database.schema().table("users").unwrap().clone();
    let old_fingerprint = old_table.fingerprint().unwrap();
    let old_cursor = database.enable_change_stream(table).unwrap();
    let (database_txn_id, source) = execute_round50_replacement(&mut database);
    let target = database.bindings.resolve_single(table).unwrap();

    assert_eq!(source, old_cursor.storage_id);
    assert_ne!(target, source);
    let target_stream = database.inspect_change_stream(table).unwrap();
    assert_eq!(target_stream.storage_id, target);
    assert_eq!(target_stream.status, ChangeStreamStatus::Disabled);
    assert_disabled(
        database
            .read_changes(table, old_cursor, 16, 1_000_000)
            .unwrap_err(),
    );

    let retired = database
        .inspect_replacement_retired_heaps()
        .into_iter()
        .find(|resource| resource.old_storage_id == source)
        .unwrap();
    let inspection = database
        .inspect_replacement_retired_heap_gc(&retired)
        .unwrap();
    assert!(inspection.eligible(), "{:?}", inspection.blockers);
    for kind in [
        RetiredHeapGcComponentKind::ChangeLog,
        RetiredHeapGcComponentKind::ChangeStreamGuard,
    ] {
        assert!(
            inspection
                .components
                .iter()
                .any(|component| component.kind == kind && component.present),
            "missing retained {kind:?}"
        );
    }
    let old_heap = inspection
        .components
        .iter()
        .find(|component| component.kind == RetiredHeapGcComponentKind::Main)
        .unwrap()
        .path
        .clone();
    let retained_paths = inspection
        .components
        .iter()
        .filter(|component| {
            matches!(
                component.kind,
                RetiredHeapGcComponentKind::ChangeLog
                    | RetiredHeapGcComponentKind::ChangeStreamGuard
            )
        })
        .map(|component| component.path.clone())
        .collect::<Vec<_>>();

    let old_storage = TableStorage::open_heap(&old_heap, old_table).unwrap();
    let changes = old_storage.read_changes(old_cursor, 16, 1_000_000).unwrap();
    assert_eq!(changes.batches.len(), 1);
    let batch = &changes.batches[0];
    assert_eq!(batch.database_txn_id, Some(database_txn_id));
    assert_eq!(batch.storage_id, source);
    assert_eq!(batch.schema_fingerprint, old_fingerprint);
    assert_eq!(batch.before, old_cursor.frontier);
    assert_eq!(batch.after, StorageDataVersion(old_cursor.frontier.0 + 1));
    assert_eq!(changes.current_frontier, batch.after);
    assert_eq!(batch.mutations.len(), 3);
    for mutation in &batch.mutations {
        match mutation {
            StorageChange::Insert { new_version, after } => {
                assert_eq!(new_version.storage_id(), source);
                assert_eq!(after.len(), 3);
            }
            StorageChange::Update {
                old_version,
                new_version,
                after,
            } => {
                assert_eq!(old_version.storage_id(), source);
                assert_eq!(new_version.storage_id(), source);
                assert_eq!(after.len(), 3);
            }
            StorageChange::Delete { old_version } => {
                assert_eq!(old_version.storage_id(), source);
            }
        }
    }
    old_storage.close().unwrap();

    database.gc_replacement_retired_heap(&retired).unwrap();
    assert!(retained_paths.iter().all(|path| !path.exists()));
    assert_eq!(
        database
            .inspect_replacement_retired_heap_gc(&retired)
            .unwrap()
            .state,
        RetiredHeapGcState::Deleted
    );
    database.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn explicit_disable_replacement_enable_anchor_starts_a_distinct_s2_history() {
    let root = root("explicit-rebaseline");
    let mut database = seed(&root);
    let table = users(&database);
    let old_cursor = database.enable_change_stream(table).unwrap();
    database
        .execute("UPDATE users SET legacy = 'before-disable' WHERE id = 1")
        .unwrap();
    database.disable_change_stream(table).unwrap();
    assert_eq!(
        database.inspect_change_stream(table).unwrap().status,
        ChangeStreamStatus::Disabled
    );

    database
        .execute("ALTER TABLE users ADD COLUMN marker TEXT")
        .unwrap();
    let replacement = database.bindings.resolve_single(table).unwrap();
    assert_ne!(replacement, old_cursor.storage_id);
    assert_eq!(
        database.inspect_change_stream(table).unwrap().status,
        ChangeStreamStatus::Disabled
    );
    let new_cursor = database.enable_change_stream(table).unwrap();
    let anchor = database.committed_read_anchor(table).unwrap();
    assert_eq!(anchor.cursor, new_cursor);
    assert_eq!(new_cursor.storage_id, replacement);
    assert_ne!(new_cursor.storage_id, old_cursor.storage_id);
    let baseline = database.inspect_change_stream(table).unwrap();
    assert_eq!(baseline.baseline_data_version, Some(new_cursor.frontier));
    assert_eq!(baseline.current_data_version, new_cursor.frontier);

    assert!(matches!(
        database
            .read_changes(table, old_cursor, 16, 1_000_000)
            .unwrap_err(),
        DatabaseError::Storage(StorageError::ChangeStream(
            ChangeStreamError::ContextMismatch
        ))
    ));
    database
        .execute("UPDATE users SET marker = legacy WHERE id = 1")
        .unwrap();
    let changes = database
        .read_changes(table, anchor.cursor, 16, 1_000_000)
        .unwrap();
    assert_eq!(changes.batches.len(), 1);
    assert_eq!(changes.batches[0].before, anchor.cursor.frontier);
    assert_eq!(changes.batches[0].mutations.len(), 1);
    assert!(
        changes.batches[0]
            .mutations
            .iter()
            .all(|mutation| match mutation {
                StorageChange::Insert { new_version, .. } =>
                    new_version.storage_id() == replacement,
                StorageChange::Update {
                    old_version,
                    new_version,
                    ..
                } => {
                    old_version.storage_id() == replacement
                        && new_version.storage_id() == replacement
                }
                StorageChange::Delete { old_version } => old_version.storage_id() == replacement,
            })
    );

    let retired = database
        .inspect_replacement_retired_heaps()
        .into_iter()
        .find(|resource| resource.old_storage_id == old_cursor.storage_id)
        .unwrap();
    let inspection = database
        .inspect_replacement_retired_heap_gc(&retired)
        .unwrap();
    assert!(inspection.components.iter().any(|component| {
        component.kind == RetiredHeapGcComponentKind::ChangeLog && component.present
    }));
    assert!(inspection.components.iter().any(|component| {
        component.kind == RetiredHeapGcComponentKind::ChangeStreamGuard && !component.present
    }));
    database.gc_replacement_retired_heap(&retired).unwrap();

    database.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn enabled_stream_survives_same_s1_index_only_and_global_noop_paths() {
    let root = root("same-s1");
    let mut database = seed(&root);
    let table = users(&database);
    let source = database.bindings.resolve_single(table).unwrap();
    let cursor = database.enable_change_stream(table).unwrap();
    let mut transaction = database.begin_transaction().unwrap();
    for statement in [
        "UPDATE users SET legacy = 'index-only' WHERE id = 1",
        "ALTER TABLE users RENAME COLUMN legacy TO temporary_name",
        "ALTER TABLE users RENAME COLUMN temporary_name TO legacy",
        "CREATE INDEX users_legacy_idx ON users(legacy)",
    ] {
        database.execute_in(&mut transaction, statement).unwrap();
    }
    database.commit_transaction(&mut transaction).unwrap();
    drop(transaction);
    assert_eq!(database.bindings.resolve_single(table), Ok(source));
    let first = database.read_changes(table, cursor, 16, 1_000_000).unwrap();
    assert_eq!(first.batches.len(), 1);
    assert_eq!(first.batches[0].mutations.len(), 1);
    let after_index_only = database.inspect_change_stream(table).unwrap();
    assert_eq!(after_index_only.status, ChangeStreamStatus::Enabled);
    assert_eq!(after_index_only.generation, Some(cursor.generation));

    for statement in [
        "CREATE INDEX users_flag_idx ON users(flag)",
        "DROP INDEX users_flag_idx",
    ] {
        database.execute(statement).unwrap();
        assert_eq!(
            database
                .inspect_change_stream(table)
                .unwrap()
                .current_data_version,
            after_index_only.current_data_version
        );
    }
    database.analyze(table).unwrap();
    database.vacuum(table).unwrap();
    assert_eq!(
        database
            .inspect_change_stream(table)
            .unwrap()
            .current_data_version,
        after_index_only.current_data_version
    );

    let second_cursor = ChangeStreamCursor {
        storage_id: source,
        generation: cursor.generation,
        frontier: after_index_only.current_data_version,
    };
    let mut transaction = database.begin_transaction().unwrap();
    for statement in [
        "UPDATE users SET flag = false WHERE id = 1",
        "ALTER TABLE users ADD COLUMN transient TEXT",
        "ALTER TABLE users DROP COLUMN transient",
        "ALTER TABLE users ALTER COLUMN legacy SET NOT NULL",
        "ALTER TABLE users ALTER COLUMN legacy DROP NOT NULL",
        "CREATE INDEX users_transient_idx ON users(flag)",
        "DROP INDEX users_transient_idx",
    ] {
        database.execute_in(&mut transaction, statement).unwrap();
    }
    database.commit_transaction(&mut transaction).unwrap();
    drop(transaction);
    assert_eq!(database.bindings.resolve_single(table), Ok(source));
    let second = database
        .read_changes(table, second_cursor, 16, 1_000_000)
        .unwrap();
    assert_eq!(second.batches.len(), 1);
    assert_eq!(second.batches[0].mutations.len(), 1);
    assert_eq!(
        database.inspect_change_stream(table).unwrap().generation,
        Some(cursor.generation)
    );

    database.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn active_stream_is_currently_abandoned_by_each_effective_schema_rewrite_shape() {
    for (name, statement) in [
        ("add", "ALTER TABLE users ADD COLUMN note TEXT"),
        (
            "rename",
            "ALTER TABLE users RENAME COLUMN legacy TO description",
        ),
        (
            "set-not-null",
            "ALTER TABLE users ALTER COLUMN legacy SET NOT NULL",
        ),
        ("drop", "ALTER TABLE users DROP COLUMN flag"),
    ] {
        let root = root(name);
        let mut database = seed(&root);
        let table = users(&database);
        let cursor = database.enable_change_stream(table).unwrap();
        database.execute(statement).unwrap();
        assert_ne!(
            database.bindings.resolve_single(table).unwrap(),
            cursor.storage_id,
            "{name}"
        );
        assert_eq!(
            database.inspect_change_stream(table).unwrap().status,
            ChangeStreamStatus::Disabled,
            "{name}"
        );
        assert_disabled(
            database
                .read_changes(table, cursor, 16, 1_000_000)
                .unwrap_err(),
        );
        database.close().unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn drop_first_and_direct_core_rewrite_paths_have_the_same_current_abandonment() {
    let drop_first_root = root("drop-first");
    let mut database = seed(&drop_first_root);
    let table = users(&database);
    database
        .execute("CREATE INDEX users_legacy_idx ON users(legacy)")
        .unwrap();
    let cursor = database.enable_change_stream(table).unwrap();
    let mut transaction = database.begin_transaction().unwrap();
    for statement in [
        "DROP INDEX users_legacy_idx",
        "UPDATE users SET legacy = 'drop-first' WHERE id = 1",
        "ALTER TABLE users ALTER COLUMN legacy SET NOT NULL",
        "CREATE INDEX users_legacy_idx ON users(legacy)",
    ] {
        database.execute_in(&mut transaction, statement).unwrap();
    }
    assert!(transaction.schema_composition.source_backfill().is_some());
    database.commit_transaction(&mut transaction).unwrap();
    drop(transaction);
    assert_ne!(
        database.bindings.resolve_single(table).unwrap(),
        cursor.storage_id
    );
    assert_disabled(
        database
            .read_changes(table, cursor, 16, 1_000_000)
            .unwrap_err(),
    );
    database.close().unwrap();
    std::fs::remove_dir_all(drop_first_root).unwrap();

    let direct_root = root("direct-core");
    let mut database = seed(&direct_root);
    let table = users(&database);
    let cursor = database.enable_change_stream(table).unwrap();
    let target = database.resolve_alter_table("users").unwrap();
    let mut transaction = database.begin_transaction().unwrap();
    database
        .rewrite_heap_table_schema_legacy_in(
            &mut transaction,
            AlterTableSpec::new(
                target,
                AlterTableOperation::AddNullableColumn {
                    name: "note".into(),
                    data_type: netbadb_types::SemanticType::physical(PhysicalType::Text),
                },
            ),
        )
        .unwrap();
    database.commit_transaction(&mut transaction).unwrap();
    drop(transaction);
    assert_ne!(
        database.bindings.resolve_single(table).unwrap(),
        cursor.storage_id
    );
    assert_disabled(
        database
            .read_changes(table, cursor, 16, 1_000_000)
            .unwrap_err(),
    );
    database.close().unwrap();
    std::fs::remove_dir_all(direct_root).unwrap();
}

#[test]
fn round51_crash_child() {
    let Ok(root) = std::env::var("NETBADB_ROUND51_CRASH_ROOT") else {
        return;
    };
    let mut database = Database::open_catalog(Path::new(&root).join("catalog")).unwrap();
    execute_round50_replacement(&mut database);
    panic!("configured Round 51 crash hook was not reached");
}

#[test]
fn active_s1_stream_recovery_hides_pre_cord_batch_and_repairs_post_cord_winner() {
    for (point, winner) in [
        ("after-all-prepares", false),
        ("after-durable-decision", true),
    ] {
        let root = root(&format!("crash-{point}"));
        let mut database = seed(&root);
        let table = users(&database);
        let old_table = database.schema().table("users").unwrap().clone();
        let source = database.bindings.resolve_single(table).unwrap();
        let cursor = database.enable_change_stream(table).unwrap();
        database.close().unwrap();

        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "change_stream_schema_replacement_audit_tests::round51_crash_child",
                "--nocapture",
            ])
            .env("NETBADB_ROUND51_CRASH_ROOT", &root);
        crate::coordinator_crash::configure_child(&mut command, point, &root, point);
        let output = command.output().unwrap();
        assert_eq!(
            output.status.code(),
            Some(crate::coordinator_crash::EXIT_CODE),
            "{point}: {} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );

        let database = Database::open_catalog(root.join("catalog")).unwrap();
        if winner {
            assert_ne!(database.bindings.resolve_single(table).unwrap(), source);
            assert_eq!(
                database.inspect_change_stream(table).unwrap().status,
                ChangeStreamStatus::Disabled
            );
            assert_disabled(
                database
                    .read_changes(table, cursor, 16, 1_000_000)
                    .unwrap_err(),
            );
            let retired = database
                .inspect_replacement_retired_heaps()
                .into_iter()
                .find(|resource| resource.old_storage_id == source)
                .unwrap();
            let inspection = database
                .inspect_replacement_retired_heap_gc(&retired)
                .unwrap();
            let old_heap = inspection
                .components
                .iter()
                .find(|component| component.kind == RetiredHeapGcComponentKind::Main)
                .unwrap()
                .path
                .clone();
            database.close().unwrap();
            let old_storage = TableStorage::open_heap(old_heap, old_table).unwrap();
            let changes = old_storage.read_changes(cursor, 16, 1_000_000).unwrap();
            assert_eq!(changes.batches.len(), 1);
            assert!(changes.batches[0].database_txn_id.is_some());
            assert_eq!(changes.batches[0].mutations.len(), 3);
            old_storage.close().unwrap();
        } else {
            assert_eq!(database.bindings.resolve_single(table), Ok(source));
            assert_eq!(
                database.inspect_change_stream(table).unwrap().status,
                ChangeStreamStatus::Enabled
            );
            let changes = database.read_changes(table, cursor, 16, 1_000_000).unwrap();
            assert!(changes.batches.is_empty());
            assert_eq!(changes.current_frontier, cursor.frontier);
            assert!(database.inspect_replacement_retired_heaps().is_empty());
            database.close().unwrap();
        }
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn unavailable_source_stream_is_currently_abandoned_by_schema_only_replacement() {
    let root = root("unavailable");
    let mut database = seed(&root);
    let table = users(&database);
    let source = database.bindings.resolve_single(table).unwrap();
    database.enable_change_stream(table).unwrap();
    database.close().unwrap();

    let snapshot = schema_catalog_file::load(&root.join("catalog")).unwrap();
    let locator = snapshot
        .storages
        .iter()
        .find(|entry| entry.id == source)
        .unwrap()
        .locator
        .clone();
    let heap = schema_catalog_file::resolve(&root.join("catalog"), &locator);
    std::fs::remove_file(netbadb_storage::heap_change_log_path(&heap)).unwrap();

    let mut database = Database::open_catalog(root.join("catalog")).unwrap();
    assert_eq!(
        database.inspect_change_stream(table).unwrap().status,
        ChangeStreamStatus::Unavailable
    );
    database
        .execute("ALTER TABLE users ADD COLUMN note TEXT")
        .unwrap();
    assert_ne!(database.bindings.resolve_single(table).unwrap(), source);
    assert_eq!(
        database.inspect_change_stream(table).unwrap().status,
        ChangeStreamStatus::Disabled
    );
    database.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}
