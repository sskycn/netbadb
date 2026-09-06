use super::*;
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_storage::{ChangeStreamError, ChangeStreamStatus, StorageChange, StorageError};
use netbadb_types::{ColumnId, PhysicalType, StorageId, TableId};
use std::path::{Path, PathBuf};

fn root(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "netbadb-round52-{name}-{}-{:?}",
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

fn assert_replacement_blocked(
    error: DatabaseError,
    table_id: TableId,
    storage_id: StorageId,
    status: ChangeStreamStatus,
) {
    assert_eq!(error.kind(), DatabaseErrorKind::FeatureNotSupported);
    let message = error.to_string();
    assert!(message.contains("authoritative storage replacement"));
    assert!(message.contains("explicitly disabling the source change stream"));
    assert!(message.contains(&format!("table {}", table_id.0)));
    assert!(message.contains(&format!("storage {}", storage_id.0)));
    assert!(message.contains(&format!("{status:?}")));
    assert!(matches!(
        error,
        DatabaseError::SchemaMutation(
            SchemaMutationError::ActiveChangeStreamBlocksReplacement {
                table_id: actual_table,
                storage_id: actual_storage,
                status: actual_status,
            }
        ) if actual_table == table_id
            && actual_storage == storage_id
            && actual_status == status
    ));
}

fn assert_no_replacement_artifacts(
    root: &Path,
    incarnation: [u8; 16],
    transaction_id: DatabaseTxnId,
    target_storage: StorageId,
) {
    let catalog = root.join("catalog");
    let stage = schema_catalog_file::resolve(
        &catalog,
        &schema_mutation_journal::stage_locator(
            &catalog,
            incarnation,
            transaction_id,
            target_storage,
        )
        .unwrap(),
    );
    let final_heap = schema_catalog_file::resolve(
        &catalog,
        &schema_mutation_journal::final_locator(&catalog, incarnation, target_storage).unwrap(),
    );
    let prepared = schema_catalog_file::resolve(
        &catalog,
        &schema_mutation_journal::prepared_locator(&catalog, incarnation, transaction_id).unwrap(),
    );
    for path in [
        stage.clone(),
        schema_catalog_file::suffix(&stage, ".owner"),
        netbadb_storage::txn_status_path(&stage),
        netbadb_storage::heap_change_log_path(&stage),
        netbadb_storage::change_stream_guard_path(netbadb_storage::heap_change_log_path(&stage)),
        final_heap.clone(),
        schema_catalog_file::suffix(&final_heap, ".owner"),
        netbadb_storage::txn_status_path(&final_heap),
        netbadb_storage::heap_change_log_path(&final_heap),
        netbadb_storage::change_stream_guard_path(netbadb_storage::heap_change_log_path(
            &final_heap,
        )),
        prepared,
    ] {
        assert!(
            !path.exists(),
            "unexpected replacement artifact: {}",
            path.display()
        );
    }
    assert!(
        !stage.parent().is_some_and(Path::exists),
        "replacement staging directory was created"
    );
}

#[test]
fn round50_replacement_is_blocked_before_storage_allocation_and_remains_rollbackable() {
    let root = root("round50-blocked");
    let mut database = seed(&root);
    let table = users(&database);
    let cursor = database.enable_change_stream(table).unwrap();
    let source = cursor.storage_id;
    let storage_floor = database.next_storage_id();
    let target_storage = storage_floor.unwrap();
    let incarnation = schema_catalog_file::load(&root.join("catalog"))
        .unwrap()
        .incarnation;
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
    let error = database.commit_transaction(&mut transaction).unwrap_err();
    assert_replacement_blocked(error, table, source, ChangeStreamStatus::Enabled);
    assert_eq!(transaction.state(), TransactionState::RollbackRequired);
    assert!(transaction.schema_composition.plan().is_some());
    assert_eq!(database.next_storage_id(), storage_floor);
    assert_eq!(database.bindings.resolve_single(table), Ok(source));
    let inspection = database.inspect_change_stream(table).unwrap();
    assert_eq!(inspection.status, ChangeStreamStatus::Enabled);
    assert_eq!(inspection.generation, Some(cursor.generation));
    assert_eq!(inspection.current_data_version, cursor.frontier);
    let journal = database.mutation_journal.as_ref().unwrap().borrow();
    assert!(
        !journal
            .source_backfill_intents
            .contains_key(&transaction_id)
    );
    assert!(!journal.stage_intents.contains_key(&transaction_id));
    let record = &journal.compositions[&transaction_id];
    assert!(record.index_intent.is_none());
    assert!(record.table_intent.is_none());
    drop(journal);
    assert_no_replacement_artifacts(&root, incarnation, transaction_id, target_storage);
    transaction.rollback().unwrap();
    assert_eq!(transaction.state(), TransactionState::RolledBack);
    drop(transaction);
    let journal = database.mutation_journal.as_ref().unwrap().borrow();
    let record = &journal.compositions[&transaction_id];
    assert_eq!(record.reservations.len(), 1, "ColumnId burn must remain");
    assert_eq!(
        record.index_reservations.len() + record.migration_index_reservations.len(),
        1,
        "IndexId burn must remain"
    );
    assert_eq!(
        record.resolution,
        Some(schema_mutation_journal::CompositionResolution::Loser)
    );
    drop(journal);
    assert_eq!(database.next_storage_id(), storage_floor);
    assert_eq!(database.bindings.resolve_single(table), Ok(source));
    assert!(
        database
            .read_changes(table, cursor, 16, 1_000_000)
            .unwrap()
            .batches
            .is_empty()
    );
    assert!(
        database
            .schema()
            .table("users")
            .unwrap()
            .column("marker")
            .is_none()
    );
    assert!(database.inspect_replacement_retired_heaps().is_empty());
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
    let old_changes = database
        .read_changes(table, old_cursor, 16, 1_000_000)
        .unwrap();
    assert_eq!(old_changes.batches.len(), 1);
    assert_eq!(old_changes.batches[0].mutations.len(), 1);
    database.disable_change_stream(table).unwrap();
    assert_eq!(
        database.inspect_change_stream(table).unwrap().status,
        ChangeStreamStatus::Disabled
    );

    let mut migration = database.begin_transaction().unwrap();
    for statement in [
        "UPDATE users SET legacy = 'migration-window' WHERE id = 1",
        "ALTER TABLE users ADD COLUMN marker TEXT",
        "UPDATE users SET marker = legacy",
        "ALTER TABLE users ALTER COLUMN marker SET NOT NULL",
        "CREATE INDEX users_marker_idx ON users(marker)",
    ] {
        database.execute_in(&mut migration, statement).unwrap();
    }
    database.commit_transaction(&mut migration).unwrap();
    drop(migration);
    let replacement = database.bindings.resolve_single(table).unwrap();
    assert_ne!(replacement, old_cursor.storage_id);
    assert_eq!(
        database.inspect_change_stream(table).unwrap().status,
        ChangeStreamStatus::Disabled
    );
    assert_disabled(
        database
            .read_changes(table, old_cursor, 16, 1_000_000)
            .unwrap_err(),
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
        .execute("UPDATE users SET marker = 'after-anchor' WHERE id = 1")
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
fn enabled_stream_blocks_each_effective_schema_rewrite_shape_without_a_storage_burn() {
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
        (
            "drop-not-null",
            "ALTER TABLE users ALTER COLUMN id DROP NOT NULL",
        ),
        ("drop", "ALTER TABLE users DROP COLUMN flag"),
    ] {
        let root = root(name);
        let mut database = seed(&root);
        let table = users(&database);
        let cursor = database.enable_change_stream(table).unwrap();
        let storage_floor = database.next_storage_id();
        let mut transaction = database.begin_transaction().unwrap();
        database.execute_in(&mut transaction, statement).unwrap();
        let error = database.commit_transaction(&mut transaction).unwrap_err();
        assert_replacement_blocked(error, table, cursor.storage_id, ChangeStreamStatus::Enabled);
        assert_eq!(
            database.bindings.resolve_single(table).unwrap(),
            cursor.storage_id,
            "{name}"
        );
        assert_eq!(database.next_storage_id(), storage_floor, "{name}");
        let stream = database.inspect_change_stream(table).unwrap();
        assert_eq!(stream.status, ChangeStreamStatus::Enabled, "{name}");
        assert_eq!(stream.generation, Some(cursor.generation), "{name}");
        assert_eq!(stream.current_data_version, cursor.frontier, "{name}");
        assert!(database.inspect_replacement_retired_heaps().is_empty());
        transaction.rollback().unwrap();
        drop(transaction);
        database.close().unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn drop_first_and_direct_core_rewrite_paths_use_the_same_production_guard() {
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
    let storage_floor = database.next_storage_id();
    let error = database.commit_transaction(&mut transaction).unwrap_err();
    assert_replacement_blocked(error, table, cursor.storage_id, ChangeStreamStatus::Enabled);
    assert_eq!(database.next_storage_id(), storage_floor);
    assert_eq!(
        database.bindings.resolve_single(table),
        Ok(cursor.storage_id)
    );
    let journal = database.mutation_journal.as_ref().unwrap().borrow();
    assert!(
        !journal
            .source_backfill_intents
            .contains_key(&transaction.id())
    );
    assert!(!journal.stage_intents.contains_key(&transaction.id()));
    let prelude = journal.compositions[&transaction.id()]
        .index_intent
        .as_ref()
        .unwrap();
    assert!(prelude.tables.iter().all(|plan| !matches!(
        plan,
        crate::schema_mutation_journal::SchemaIndexTablePlan::RewriteHeap { .. }
    )));
    drop(journal);
    transaction.rollback().unwrap();
    drop(transaction);
    database.close().unwrap();
    std::fs::remove_dir_all(drop_first_root).unwrap();

    let direct_root = root("direct-core-production");
    let mut database = seed(&direct_root);
    let table = users(&database);
    let cursor = database.enable_change_stream(table).unwrap();
    let storage_floor = database.next_storage_id();
    let target = database.resolve_alter_table("users").unwrap();
    let mut transaction = database.begin_transaction().unwrap();
    database
        .rewrite_heap_table_schema_in(
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
    let error = database.commit_transaction(&mut transaction).unwrap_err();
    assert_replacement_blocked(error, table, cursor.storage_id, ChangeStreamStatus::Enabled);
    assert_eq!(transaction.state(), TransactionState::RollbackRequired);
    assert_eq!(database.next_storage_id(), storage_floor);
    assert_eq!(
        database.bindings.resolve_single(table),
        Ok(cursor.storage_id)
    );
    transaction.rollback().unwrap();
    drop(transaction);
    database.close().unwrap();
    std::fs::remove_dir_all(direct_root).unwrap();

    let legacy_root = root("direct-core-legacy");
    let mut database = seed(&legacy_root);
    let table = users(&database);
    let cursor = database.enable_change_stream(table).unwrap();
    let storage_floor = database.next_storage_id();
    let target = database.resolve_alter_table("users").unwrap();
    let mut transaction = database.begin_transaction().unwrap();
    let error = database
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
        .unwrap_err();
    assert_replacement_blocked(error, table, cursor.storage_id, ChangeStreamStatus::Enabled);
    assert_eq!(transaction.state(), TransactionState::Active);
    assert_eq!(database.next_storage_id(), storage_floor);
    assert_eq!(
        database.bindings.resolve_single(table),
        Ok(cursor.storage_id)
    );
    transaction.rollback().unwrap();
    drop(transaction);
    database.close().unwrap();
    std::fs::remove_dir_all(legacy_root).unwrap();
}

#[test]
fn unavailable_source_stream_blocks_replacement_and_can_be_explicitly_abandoned() {
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
    let storage_floor = database.next_storage_id();
    let mut transaction = database.begin_transaction().unwrap();
    database
        .execute_in(&mut transaction, "ALTER TABLE users ADD COLUMN note TEXT")
        .unwrap();
    let error = database.commit_transaction(&mut transaction).unwrap_err();
    assert_replacement_blocked(error, table, source, ChangeStreamStatus::Unavailable);
    assert_eq!(database.next_storage_id(), storage_floor);
    assert_eq!(database.bindings.resolve_single(table), Ok(source));
    transaction.rollback().unwrap();
    drop(transaction);
    database.disable_change_stream(table).unwrap();
    assert_eq!(
        database.inspect_change_stream(table).unwrap().status,
        ChangeStreamStatus::Disabled
    );
    database
        .execute("ALTER TABLE users ADD COLUMN note TEXT")
        .unwrap();
    assert_ne!(database.bindings.resolve_single(table).unwrap(), source);
    database.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn disabled_and_never_enabled_sources_replace_while_create_and_drop_remain_unaffected() {
    for (name, enable_then_disable) in [("never-enabled", false), ("disabled", true)] {
        let root = root(name);
        let mut database = seed(&root);
        let table = users(&database);
        let source = database.bindings.resolve_single(table).unwrap();
        if enable_then_disable {
            database.enable_change_stream(table).unwrap();
            database.disable_change_stream(table).unwrap();
        }
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

    let root = root("create-drop");
    let mut database = seed(&root);
    let users = users(&database);
    database.enable_change_stream(users).unwrap();
    database
        .execute("CREATE TABLE transient (id BIGINT NOT NULL)")
        .unwrap();
    let transient = database.schema().table("transient").unwrap().id;
    database.enable_change_stream(transient).unwrap();
    database.execute("DROP TABLE transient").unwrap();
    assert!(database.schema().table("transient").is_none());
    let retired = database
        .inspect_retired_table_resources()
        .into_iter()
        .find(|resource| resource.table_id == transient)
        .unwrap();
    let inspection = database.inspect_retired_heap_gc(&retired).unwrap();
    for kind in [
        RetiredHeapGcComponentKind::ChangeLog,
        RetiredHeapGcComponentKind::ChangeStreamGuard,
    ] {
        assert!(
            inspection
                .components
                .iter()
                .any(|component| component.kind == kind && component.present)
        );
    }
    database.gc_retired_heap(&retired).unwrap();
    assert_eq!(
        database.inspect_retired_heap_gc(&retired).unwrap().state,
        RetiredHeapGcState::Deleted
    );
    assert_eq!(
        database.inspect_change_stream(users).unwrap().status,
        ChangeStreamStatus::Enabled
    );
    database.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn incremental_projection_stays_on_s1_when_blocked_and_rebaselines_explicitly_on_s2() {
    let root = root("incremental-rebaseline");
    let mut database = seed(&root);
    let table = users(&database);
    let source = database.bindings.resolve_single(table).unwrap();
    let cursor = database.enable_change_stream(table).unwrap();
    let source_columns = database
        .schema()
        .table("users")
        .unwrap()
        .columns
        .iter()
        .map(|column| column.id)
        .collect::<Vec<_>>();
    let old_projection = database
        .build_incremental_columnar_projection(ColumnarProjectionSpec::new(
            table,
            root.join("projection-s1"),
            source_columns,
        ))
        .unwrap();
    let inspection = database.inspect_columnar_projections();
    let old = inspection
        .iter()
        .find(|projection| projection.projection_id == Some(old_projection))
        .unwrap();
    assert_eq!(old.health, ColumnarProjectionHealth::Fresh);
    assert_eq!(old.source_storage_id, Some(source));
    assert_eq!(old.stream_generation, Some(cursor.generation));

    let mut blocked = database.begin_transaction().unwrap();
    database
        .execute_in(&mut blocked, "ALTER TABLE users ADD COLUMN marker TEXT")
        .unwrap();
    let error = database.commit_transaction(&mut blocked).unwrap_err();
    assert_replacement_blocked(error, table, source, ChangeStreamStatus::Enabled);
    blocked.rollback().unwrap();
    drop(blocked);
    let old = database
        .inspect_columnar_projections()
        .into_iter()
        .find(|projection| projection.projection_id == Some(old_projection))
        .unwrap();
    assert_eq!(old.health, ColumnarProjectionHealth::Fresh);
    assert_eq!(old.source_storage_id, Some(source));
    assert_eq!(old.stream_generation, Some(cursor.generation));

    database.disable_change_stream(table).unwrap();
    assert_eq!(
        database
            .inspect_columnar_projections()
            .into_iter()
            .find(|projection| projection.projection_id == Some(old_projection))
            .unwrap()
            .health,
        ColumnarProjectionHealth::RebuildRequired
    );
    database
        .execute("ALTER TABLE users ADD COLUMN marker TEXT")
        .unwrap();
    let replacement = database.bindings.resolve_single(table).unwrap();
    assert_ne!(replacement, source);
    let old_after_replacement = database
        .inspect_columnar_projections()
        .into_iter()
        .find(|projection| projection.projection_id == Some(old_projection))
        .unwrap();
    assert_ne!(
        old_after_replacement.health,
        ColumnarProjectionHealth::Fresh
    );
    assert_eq!(old_after_replacement.source_storage_id, Some(source));

    let new_cursor = database.enable_change_stream(table).unwrap();
    assert_eq!(new_cursor.storage_id, replacement);
    let replacement_columns = database
        .schema()
        .table("users")
        .unwrap()
        .columns
        .iter()
        .map(|column| column.id)
        .collect::<Vec<_>>();
    let new_projection = database
        .build_incremental_columnar_projection(ColumnarProjectionSpec::new(
            table,
            root.join("projection-s2"),
            replacement_columns,
        ))
        .unwrap();
    let new = database
        .inspect_columnar_projections()
        .into_iter()
        .find(|projection| projection.projection_id == Some(new_projection))
        .unwrap();
    assert_eq!(new.health, ColumnarProjectionHealth::Fresh);
    assert_eq!(new.source_storage_id, Some(replacement));
    assert_eq!(new.stream_generation, Some(new_cursor.generation));

    database
        .execute("UPDATE users SET marker = 'delta' WHERE id = 1")
        .unwrap();
    assert_eq!(
        database
            .inspect_columnar_projections()
            .into_iter()
            .find(|projection| projection.projection_id == Some(new_projection))
            .unwrap()
            .health,
        ColumnarProjectionHealth::Lagging
    );
    let report = database
        .advance_columnar_projection(new_projection, ColumnarAdvanceBudget::new(16, 1_000_000))
        .unwrap();
    assert!(report.caught_up);
    assert_eq!(report.batches_applied, 1);
    assert_eq!(
        database
            .inspect_columnar_projections()
            .into_iter()
            .find(|projection| projection.projection_id == Some(new_projection))
            .unwrap()
            .health,
        ColumnarProjectionHealth::Fresh
    );
    assert_eq!(
        database
            .query("SELECT marker FROM users WHERE id = 1")
            .unwrap()
            .rows,
        vec![vec![ScalarValue::Text("delta".into())]]
    );
    database.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}
