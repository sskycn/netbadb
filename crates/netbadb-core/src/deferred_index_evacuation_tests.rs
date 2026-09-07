//! Round 58 production coverage for terminal logical index evacuation.
//!
//! Statements enter through the ordinary Database execution API. The
//! source Heap and its indexes remain physical authority until finalization.

use super::*;
use crate::schema_composition::SchemaCompositionState;
use crate::schema_mutation_journal::{SchemaIndexTablePlan, heap_rewrite_indexes_digest};
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_storage::{ChangeStreamStatus, HeapRewriteIndex};
use netbadb_types::{
    ColumnId, DatabaseCommitSeq, IndexId, PhysicalType, ScalarValue, StorageId, TableId,
};
use std::path::{Path, PathBuf};

const USERS: TableId = TableId(2);
const ID: ColumnId = ColumnId(1);
const OLD_LEGACY: ColumnId = ColumnId(2);
const FLAG: ColumnId = ColumnId(3);
const SHADOW: ColumnId = ColumnId(4);

fn root(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "netbadb-round58-{name}-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_dir_all(&path);
    std::fs::create_dir_all(&path).unwrap();
    path
}

fn seed_with_config(path: &Path, global: bool, second_index: bool) -> Database {
    let mut config = DatabaseCoordinatorConfig::new(path.join("coordinator"));
    if global {
        config = config.with_global_visibility();
    }
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
        Some(config),
    )
    .unwrap();
    database
        .execute("CREATE TABLE users (id BIGINT NOT NULL, legacy TEXT, flag BOOLEAN)")
        .unwrap();
    for statement in [
        "INSERT INTO users VALUES (1, 'old1', true)",
        "INSERT INTO users VALUES (2, 'old2', false)",
        "INSERT INTO users VALUES (3, NULL, NULL)",
        "CREATE INDEX users_legacy_idx ON users(legacy)",
    ] {
        database.execute(statement).unwrap();
    }
    if second_index {
        database
            .execute("CREATE INDEX users_flag_idx ON users(flag)")
            .unwrap();
    }
    database
}

fn seed(path: &Path) -> Database {
    seed_with_config(path, false, false)
}

fn adopt_shadow(database: &mut Database) -> Transaction {
    let mut transaction = database.begin_transaction().unwrap();
    for statement in [
        "UPDATE users SET legacy = 'updated1' WHERE id = 1",
        "INSERT INTO users VALUES (4, 'inserted4', true)",
        "DELETE FROM users WHERE id = 2",
        "ALTER TABLE users ADD COLUMN shadow TEXT",
    ] {
        database.execute_in(&mut transaction, statement).unwrap();
    }
    transaction
}

fn adopt_noop_shadow(database: &mut Database) -> Transaction {
    let mut transaction = database.begin_transaction().unwrap();
    for statement in [
        "UPDATE users SET legacy = legacy WHERE id = 999",
        "ALTER TABLE users ADD COLUMN shadow TEXT",
        "UPDATE users SET shadow = legacy WHERE id = 999",
    ] {
        database.execute_in(&mut transaction, statement).unwrap();
    }
    transaction
}

fn prepare_shadow(database: &mut Database, transaction: &mut Transaction) {
    for statement in [
        "UPDATE users SET shadow = legacy WHERE legacy IS NOT NULL",
        "UPDATE users SET shadow = 'missing' WHERE shadow IS NULL",
        "ALTER TABLE users ALTER COLUMN shadow SET NOT NULL",
    ] {
        database.execute_in(transaction, statement).unwrap();
    }
    assert!(matches!(
        transaction.schema_composition,
        SchemaCompositionState::AdoptedSourceBackfilling(_)
    ));
}

fn prepared_ddl(database: &Database, transaction: &Transaction, sql: &str) -> PreparedDdlStatement {
    let PreparedSqlStatement::Ddl(prepared) = database
        .prepare_sql_statement_in(transaction, sql, &[])
        .unwrap()
    else {
        panic!("expected DDL")
    };
    prepared
}

fn evacuate(
    database: &mut Database,
    transaction: &mut Transaction,
    prepared: &PreparedDdlStatement,
) -> Result<DdlOutcome, DatabaseError> {
    database.execute_ddl_in(transaction, prepared)
}

fn source_digest(database: &mut Database, storage: StorageId) -> [u8; 32] {
    heap_rewrite_indexes_digest(
        &database
            .registry
            .get_mut(storage)
            .unwrap()
            .heap_rewrite_indexes()
            .unwrap(),
    )
    .unwrap()
}

fn physical_index(database: &mut Database, storage: StorageId, index: IndexId) -> HeapRewriteIndex {
    database
        .registry
        .get_mut(storage)
        .unwrap()
        .heap_rewrite_indexes()
        .unwrap()
        .active
        .into_iter()
        .find(|candidate| candidate.id == index)
        .unwrap()
}

fn assert_physical_lookup(
    database: &mut Database,
    storage: StorageId,
    index: IndexId,
    key: &str,
    expected_id: i64,
) {
    let storage = database.registry.get_mut(storage).unwrap();
    let column = storage
        .heap_rewrite_indexes()
        .unwrap()
        .active
        .iter()
        .find(|candidate| candidate.id == index)
        .unwrap()
        .column_id;
    let access_path = storage
        .access_paths()
        .into_iter()
        .find(|candidate| candidate.column_id == column)
        .unwrap()
        .id;
    let view = storage.read_view().unwrap();
    let rows = storage
        .point_lookup_columns_with_view(access_path, &ScalarValue::Text(key.into()), &[ID], &view)
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1, vec![ScalarValue::Int64(expected_id)]);
}

fn logical_indexes(transaction: &Transaction) -> Vec<HeapRewriteIndex> {
    transaction
        .schema_composition
        .plan()
        .unwrap()
        .touched
        .get(&USERS)
        .unwrap()
        .indexes
        .active
        .clone()
}

fn old_index(database: &Database) -> HeapRewriteIndex {
    database
        .indexes(USERS)
        .unwrap()
        .iter()
        .find(|index| {
            index
                .name
                .as_ref()
                .is_some_and(|name| name.as_str() == "users_legacy_idx")
        })
        .map(|index| HeapRewriteIndex {
            id: index.id,
            name: index.name.clone(),
            column_id: index.column_id,
        })
        .unwrap()
}

fn finish_shadow_swap(database: &mut Database, transaction: &mut Transaction) -> IndexId {
    for statement in [
        "ALTER TABLE users DROP COLUMN legacy",
        "ALTER TABLE users RENAME COLUMN shadow TO legacy",
    ] {
        database.execute_in(transaction, statement).unwrap();
    }
    let create = prepared_ddl(
        database,
        transaction,
        "CREATE INDEX IF NOT EXISTS users_legacy_idx ON users(legacy)",
    );
    assert_eq!(
        database.execute_ddl_in(transaction, &create).unwrap(),
        DdlOutcome::Created
    );
    assert!(matches!(
        transaction.schema_composition,
        SchemaCompositionState::AdoptedSourceIndexFinalizing(_)
    ));
    database
        .index_name_bindings(Some(transaction))
        .into_iter()
        .find(|binding| binding.name.as_str() == "users_legacy_idx")
        .unwrap()
        .target
        .index_id
}

#[test]
fn production_drop_enters_final_refining_and_keeps_terminal_structure_open() {
    let path = root("production-route");
    let mut database = seed(&path);
    let mut transaction = adopt_shadow(&mut database);
    prepare_shadow(&mut database, &mut transaction);
    let drop = prepared_ddl(&database, &transaction, "DROP INDEX users_legacy_idx");
    assert_eq!(
        database.execute_ddl_in(&mut transaction, &drop).unwrap(),
        DdlOutcome::Dropped
    );
    assert!(matches!(
        transaction.schema_composition,
        SchemaCompositionState::AdoptedSourceFinalRefining(_)
    ));
    assert_eq!(
        database
            .execute_in(&mut transaction, "ALTER TABLE users DROP COLUMN legacy")
            .unwrap(),
        ExecutionResult::AffectedRows(0)
    );
    transaction.rollback().unwrap();
    database.close().unwrap();
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn full_indexed_swap_is_logical_until_one_s2_finalization() {
    let path = root("full-indexed-swap");
    let mut database = seed(&path);
    let source = database.bindings.resolve_single(USERS).unwrap();
    let target_floor = database.next_storage_id().unwrap();
    let old = old_index(&database);
    assert_eq!((old.id, old.column_id), (IndexId(1), OLD_LEGACY));
    let digest = source_digest(&mut database, source);
    let public_table = database.schema().table("users").unwrap().clone();
    let mut transaction = adopt_shadow(&mut database);
    let prepared_drop = prepared_ddl(&database, &transaction, "DROP INDEX users_legacy_idx");
    prepare_shadow(&mut database, &mut transaction);
    let action_count = transaction
        .schema_composition
        .plan()
        .unwrap()
        .action_count();

    assert_eq!(
        evacuate(&mut database, &mut transaction, &prepared_drop).unwrap(),
        DdlOutcome::Dropped
    );
    assert!(matches!(
        transaction.schema_composition,
        SchemaCompositionState::AdoptedSourceFinalRefining(_)
    ));
    assert_eq!(
        transaction
            .schema_composition
            .plan()
            .unwrap()
            .action_count(),
        action_count + 1
    );
    assert_eq!(database.next_storage_id(), Some(target_floor));
    assert_eq!(source_digest(&mut database, source), digest);
    assert_eq!(physical_index(&mut database, source, old.id), old);
    assert_physical_lookup(&mut database, source, old.id, "old1", 1);
    assert!(logical_indexes(&transaction).is_empty());
    assert_eq!(database.schema().table("users"), Some(&public_table));
    assert_eq!(old_index(&database), old);

    let new = finish_shadow_swap(&mut database, &mut transaction);
    assert_eq!(new, IndexId(2));
    assert_ne!(new, old.id);
    assert_eq!(source_digest(&mut database, source), digest);
    assert_eq!(physical_index(&mut database, source, old.id), old);
    database.finalize_adopted_source(&mut transaction).unwrap();
    let materialized = transaction.schema_composition.source_backfill().unwrap();
    assert_eq!(
        (
            materialized.source_copy_passes,
            materialized.source_rows_copied
        ),
        (1, 3)
    );
    let SchemaIndexTablePlan::RewriteHeap {
        replacement,
        base_indexes,
        final_indexes,
    } = &materialized.intent.tables[0]
    else {
        panic!("expected one Heap rewrite")
    };
    assert_eq!(replacement.new_storage(), target_floor);
    assert_eq!(base_indexes.active, vec![old.clone()]);
    assert_eq!(final_indexes.active.len(), 1);
    assert_eq!(
        (
            final_indexes.active[0].id,
            final_indexes.active[0].column_id
        ),
        (new, SHADOW)
    );

    database.commit_transaction(&mut transaction).unwrap();
    assert_eq!(database.bindings.resolve_single(USERS), Ok(target_floor));
    assert_eq!(
        database.next_storage_id(),
        Some(StorageId(target_floor.0 + 1))
    );
    for _ in 0..3 {
        let table = database.schema().table("users").unwrap();
        assert_eq!(table.column("legacy").unwrap().id, SHADOW);
        assert!(table.column_by_id(OLD_LEGACY).is_none());
        let indexes = database.indexes(USERS).unwrap();
        assert_eq!(indexes.len(), 1);
        assert_eq!((indexes[0].id, indexes[0].column_id), (new, SHADOW));
        for (key, id) in [("updated1", 1), ("missing", 3), ("inserted4", 4)] {
            assert_physical_lookup(&mut database, target_floor, new, key, id);
        }
        database.close().unwrap();
        database = Database::open_catalog(path.join("catalog")).unwrap();
    }
    database.close().unwrap();
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn effective_and_unchanged_drop_phase_transitions_are_exact() {
    let path = root("drop-outcomes");
    let mut database = seed_with_config(&path, false, true);
    database
        .execute("CREATE INDEX seed_idx ON seed(id)")
        .unwrap();
    let mut transaction = adopt_shadow(&mut database);
    let exact = prepared_ddl(&database, &transaction, "DROP INDEX users_legacy_idx");
    let exact_if_exists = prepared_ddl(
        &database,
        &transaction,
        "DROP INDEX IF EXISTS users_legacy_idx",
    );
    prepare_shadow(&mut database, &mut transaction);

    let absent = prepared_ddl(
        &database,
        &transaction,
        "DROP INDEX IF EXISTS index_that_is_not_here",
    );
    assert_eq!(
        evacuate(&mut database, &mut transaction, &absent).unwrap(),
        DdlOutcome::Unchanged
    );
    assert!(matches!(
        transaction.schema_composition,
        SchemaCompositionState::AdoptedSourceBackfilling(_)
    ));
    database
        .execute_in(
            &mut transaction,
            "UPDATE users SET shadow = shadow WHERE id = 999",
        )
        .unwrap();

    let cross = prepared_ddl(&database, &transaction, "DROP INDEX seed_idx");
    assert!(matches!(
        evacuate(&mut database, &mut transaction, &cross),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::UnsupportedBackfillRefinement(
                crate::schema_mutation::BackfillRefinementReason::CrossTableAccess
            )
        ))
    ));
    assert!(matches!(
        transaction.schema_composition,
        SchemaCompositionState::AdoptedSourceBackfilling(_)
    ));

    assert_eq!(
        evacuate(&mut database, &mut transaction, &exact).unwrap(),
        DdlOutcome::Dropped
    );
    assert_eq!(
        evacuate(&mut database, &mut transaction, &exact_if_exists).unwrap(),
        DdlOutcome::Unchanged
    );
    assert!(matches!(
        evacuate(&mut database, &mut transaction, &exact),
        Err(DatabaseError::UndefinedIndex)
    ));
    assert!(matches!(
        transaction.schema_composition,
        SchemaCompositionState::AdoptedSourceFinalRefining(_)
    ));
    assert!(matches!(
        database.execute_in(
            &mut transaction,
            "UPDATE users SET shadow = shadow WHERE id = 999"
        ),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::MigrationDataAccessAfterRefinement
        ))
    ));
    assert!(
        database
            .execute_in(
                &mut transaction,
                "ALTER TABLE users ALTER COLUMN shadow DROP NOT NULL"
            )
            .is_err()
    );

    let already_indexed = prepared_ddl(
        &database,
        &transaction,
        "CREATE INDEX another_flag_idx ON users(flag)",
    );
    assert!(
        database
            .execute_ddl_in(&mut transaction, &already_indexed)
            .is_err()
    );
    assert!(matches!(
        transaction.schema_composition,
        SchemaCompositionState::AdoptedSourceFinalRefining(_)
    ));
    assert_eq!(logical_indexes(&transaction)[0].column_id, FLAG);
    transaction.rollback().unwrap();
    database.close().unwrap();
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn evacuation_requires_backfilling_but_any_effective_same_table_drop_qualifies() {
    let path = root("bounded-eligibility");
    let mut database = seed_with_config(&path, false, true);
    let mut transaction = adopt_shadow(&mut database);
    let early = prepared_ddl(&database, &transaction, "DROP INDEX users_legacy_idx");
    assert_eq!(
        evacuate(&mut database, &mut transaction, &early).unwrap(),
        DdlOutcome::Dropped
    );
    assert!(matches!(
        transaction.schema_composition,
        SchemaCompositionState::AdoptedSourceIndexFinalizing(_)
    ));
    transaction.rollback().unwrap();
    std::mem::drop(transaction);

    let mut transaction = adopt_shadow(&mut database);
    prepare_shadow(&mut database, &mut transaction);
    let unrelated = prepared_ddl(&database, &transaction, "DROP INDEX users_flag_idx");
    assert_eq!(
        evacuate(&mut database, &mut transaction, &unrelated).unwrap(),
        DdlOutcome::Dropped
    );
    assert!(matches!(
        transaction.schema_composition,
        SchemaCompositionState::AdoptedSourceFinalRefining(_)
    ));
    assert_eq!(logical_indexes(&transaction), vec![old_index(&database)]);
    transaction.rollback().unwrap();
    database.close().unwrap();
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn final_refining_allows_more_evacuations_and_preserves_unrelated_index() {
    let path = root("rename-table-multiple");
    let mut database = seed_with_config(&path, false, true);
    let source = database.bindings.resolve_single(USERS).unwrap();
    let old = old_index(&database);
    let retained = database
        .indexes(USERS)
        .unwrap()
        .iter()
        .find(|index| index.column_id == FLAG)
        .unwrap()
        .id;
    let mut transaction = adopt_shadow(&mut database);
    prepare_shadow(&mut database, &mut transaction);
    database
        .execute_in(&mut transaction, "ALTER TABLE users RENAME TO people")
        .unwrap();
    let drop = prepared_ddl(&database, &transaction, "DROP INDEX users_legacy_idx");
    assert_eq!(
        evacuate(&mut database, &mut transaction, &drop).unwrap(),
        DdlOutcome::Dropped
    );
    assert!(matches!(
        transaction.schema_composition,
        SchemaCompositionState::AdoptedSourceFinalRefining(_)
    ));
    for statement in [
        "ALTER TABLE people DROP COLUMN legacy",
        "ALTER TABLE people RENAME COLUMN shadow TO legacy",
        "CREATE INDEX people_legacy_idx ON people(legacy)",
    ] {
        database.execute_in(&mut transaction, statement).unwrap();
    }
    database.finalize_adopted_source(&mut transaction).unwrap();
    let materialized = transaction.schema_composition.source_backfill().unwrap();
    assert_eq!(materialized.source_copy_passes, 1);
    let SchemaIndexTablePlan::RewriteHeap { final_indexes, .. } = &materialized.intent.tables[0]
    else {
        panic!("expected rewrite")
    };
    assert_eq!(final_indexes.active.len(), 2);
    assert!(
        final_indexes
            .active
            .iter()
            .any(|index| index.id == retained && index.column_id == FLAG)
    );
    let replacement = final_indexes
        .active
        .iter()
        .find(|index| {
            index
                .name
                .as_ref()
                .is_some_and(|name| name.as_str() == "people_legacy_idx")
        })
        .unwrap();
    assert!(replacement.id > retained);
    assert_eq!(replacement.column_id, SHADOW);
    assert_eq!(physical_index(&mut database, source, old.id), old);
    database.commit_transaction(&mut transaction).unwrap();
    assert!(database.schema().table("users").is_none());
    assert_eq!(
        database
            .schema()
            .table("people")
            .unwrap()
            .column("legacy")
            .unwrap()
            .id,
        SHADOW
    );
    database.close().unwrap();
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn table_noop_index_delta_stays_on_s1_and_is_change_stream_safe() {
    for enabled in [false, true] {
        let path = root(if enabled {
            "index-only-stream"
        } else {
            "index-only"
        });
        let mut database = seed_with_config(&path, true, false);
        if enabled {
            database.enable_change_stream(USERS).unwrap();
        }
        let visibility_before = database.inspect_global_visibility().unwrap();
        let snapshot_before = database.current_database_snapshot().unwrap().unwrap();
        let stream_before = database.inspect_change_stream(USERS).unwrap();
        let source = database.bindings.resolve_single(USERS).unwrap();
        let storage_floor = database.next_storage_id().unwrap();
        let old = old_index(&database);
        let digest = source_digest(&mut database, source);
        let mut transaction = adopt_noop_shadow(&mut database);
        database
            .execute_in(&mut transaction, "ALTER TABLE users DROP COLUMN shadow")
            .unwrap();
        let drop = prepared_ddl(&database, &transaction, "DROP INDEX users_legacy_idx");
        assert_eq!(
            evacuate(&mut database, &mut transaction, &drop).unwrap(),
            DdlOutcome::Dropped
        );
        assert_eq!(source_digest(&mut database, source), digest);
        database.finalize_adopted_source(&mut transaction).unwrap();
        let materialized = transaction.schema_composition.materialized_index().unwrap();
        assert_eq!(
            (
                materialized.source_copy_passes,
                materialized.source_rows_copied
            ),
            (0, 0)
        );
        assert!(materialized.target.is_none());
        assert!(materialized.staged.is_empty());
        let SchemaIndexTablePlan::InPlaceIndexDelta {
            storage,
            base_indexes,
            final_indexes,
            ..
        } = &materialized.intent.tables[0]
        else {
            panic!("expected same-S1 index delta")
        };
        assert_eq!(*storage, source);
        assert_eq!(base_indexes.active, vec![old]);
        assert!(final_indexes.active.is_empty());
        database.commit_transaction(&mut transaction).unwrap();
        let snapshot_after = database.current_database_snapshot().unwrap().unwrap();
        assert_eq!(
            snapshot_after.commit_seq(),
            DatabaseCommitSeq(snapshot_before.commit_seq().0 + 1)
        );
        assert!(snapshot_after.boundary(source).is_some());
        let visibility_after = database.inspect_global_visibility().unwrap();
        assert_eq!(
            visibility_after.published_commit_seq,
            Some(snapshot_after.commit_seq())
        );
        assert_eq!(
            visibility_after.decision_sync_count,
            visibility_before.decision_sync_count + 1
        );
        assert_eq!(
            visibility_after.combined_pipeline_sync_count,
            visibility_before.combined_pipeline_sync_count
        );
        assert_eq!(
            visibility_after.checkpoint_sync_count,
            visibility_before.checkpoint_sync_count + 1
        );
        assert_eq!(visibility_after.pending_complete_count, 0);
        assert_eq!(
            visibility_after.last_synced_complete,
            Some(snapshot_after.commit_seq())
        );
        assert_eq!(database.bindings.resolve_single(USERS), Ok(source));
        assert_eq!(database.next_storage_id(), Some(storage_floor));
        assert!(database.indexes(USERS).unwrap().is_empty());
        let stream_after = database.inspect_change_stream(USERS).unwrap();
        assert_eq!(
            stream_after.status,
            if enabled {
                ChangeStreamStatus::Enabled
            } else {
                ChangeStreamStatus::Disabled
            }
        );
        assert_eq!(stream_after.generation, stream_before.generation);
        assert_eq!(
            stream_after.stream_origin_frontier,
            stream_before.stream_origin_frontier
        );
        assert_eq!(
            stream_after.current_data_version,
            stream_before.current_data_version
        );
        database.close().unwrap();
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn indexed_terminal_migration_keeps_maintenance_busy_and_non_authoritative() {
    let path = root("maintenance");
    let projection_path = path.join("projection");
    let mut database = seed(&path);
    database.enable_change_stream(USERS).unwrap();
    database
        .build_incremental_columnar_projection(ColumnarProjectionSpec::new(
            USERS,
            &projection_path,
            vec![ID, OLD_LEGACY],
        ))
        .unwrap();
    database
        .execute("UPDATE users SET legacy = 'pending' WHERE id = 1")
        .unwrap();
    let manifest = projection_path.join("projection.nbcmanifest");
    let before = std::fs::read(&manifest).unwrap();
    let mut transaction = adopt_shadow(&mut database);
    prepare_shadow(&mut database, &mut transaction);
    let drop = prepared_ddl(&database, &transaction, "DROP INDEX users_legacy_idx");
    evacuate(&mut database, &mut transaction, &drop).unwrap();

    let budget = MaintenanceBudget::new(1 << 20, 1 << 30, 1 << 20, 16);
    let inspection = database.inspect_maintenance(budget).unwrap();
    assert!(inspection.decision.is_none());
    assert!(inspection.candidates.iter().any(|candidate| {
        matches!(candidate.action, MaintenanceAction::AdvanceColumnar { .. })
            && candidate.blocker == Some(MaintenanceBlocker::Busy)
    }));
    assert!(matches!(
        database.maintenance_step(budget).unwrap().outcome,
        MaintenanceOutcome::NoWork
    ));
    assert_eq!(std::fs::read(&manifest).unwrap(), before);
    transaction.rollback().unwrap();
    database.close().unwrap();
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn same_definition_recreation_is_effective_because_index_identity_changes() {
    let path = root("same-definition-new-identity");
    let mut database = seed(&path);
    let source = database.bindings.resolve_single(USERS).unwrap();
    let old = old_index(&database);
    let mut transaction = adopt_noop_shadow(&mut database);
    database
        .execute_in(&mut transaction, "ALTER TABLE users DROP COLUMN shadow")
        .unwrap();
    let drop = prepared_ddl(&database, &transaction, "DROP INDEX users_legacy_idx");
    evacuate(&mut database, &mut transaction, &drop).unwrap();
    let create = prepared_ddl(
        &database,
        &transaction,
        "CREATE INDEX users_legacy_idx ON users(legacy)",
    );
    assert_eq!(
        database.execute_ddl_in(&mut transaction, &create).unwrap(),
        DdlOutcome::Created
    );
    let replacement = database
        .index_name_bindings(Some(&transaction))
        .into_iter()
        .find(|binding| binding.name.as_str() == "users_legacy_idx")
        .unwrap()
        .target
        .index_id;
    assert_eq!((old.id, replacement), (IndexId(1), IndexId(2)));
    database.finalize_adopted_source(&mut transaction).unwrap();
    let materialized = transaction.schema_composition.materialized_index().unwrap();
    let SchemaIndexTablePlan::InPlaceIndexDelta {
        base_indexes,
        final_indexes,
        ..
    } = &materialized.intent.tables[0]
    else {
        panic!("identity replacement must be an effective same-S1 delta")
    };
    assert_eq!(base_indexes.active[0].id, old.id);
    assert_eq!(final_indexes.active[0].id, replacement);
    database.commit_transaction(&mut transaction).unwrap();
    assert_eq!(database.bindings.resolve_single(USERS), Ok(source));
    assert_eq!(database.indexes(USERS).unwrap()[0].id, replacement);
    database.close().unwrap();
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn enabled_stream_blocks_effective_replacement_without_publication() {
    let path = root("stream-block");
    let mut database = seed_with_config(&path, true, false);
    let source = database.enable_change_stream(USERS).unwrap().storage_id;
    let before = database.current_database_snapshot().unwrap().unwrap();
    let storage_floor = database.next_storage_id();
    let old = old_index(&database);
    let mut transaction = adopt_shadow(&mut database);
    prepare_shadow(&mut database, &mut transaction);
    let drop = prepared_ddl(&database, &transaction, "DROP INDEX users_legacy_idx");
    evacuate(&mut database, &mut transaction, &drop).unwrap();
    finish_shadow_swap(&mut database, &mut transaction);
    assert!(matches!(
        database.commit_transaction(&mut transaction),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::ActiveChangeStreamBlocksReplacement {
                table_id: USERS,
                storage_id,
                status: ChangeStreamStatus::Enabled,
            }
        )) if storage_id == source
    ));
    assert_eq!(database.current_database_snapshot().unwrap(), Some(before));
    assert_eq!(database.next_storage_id(), storage_floor);
    transaction.rollback().unwrap();
    assert_eq!(database.bindings.resolve_single(USERS), Ok(source));
    assert_eq!(old_index(&database), old);
    database.close().unwrap();
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn structural_swap_checkpoints_a_prior_phase3b_complete_and_publishes_one_g() {
    let path = root("global-pipeline");
    let mut database = seed_with_config(&path, true, false);
    let baseline = database.inspect_global_visibility().unwrap();
    let before = database.current_database_snapshot().unwrap().unwrap();
    database
        .execute("UPDATE users SET flag = false WHERE id = 1")
        .unwrap();
    let pure = database.inspect_global_visibility().unwrap();
    assert_eq!(
        pure.published_commit_seq,
        Some(DatabaseCommitSeq(before.commit_seq().0 + 1))
    );
    assert_eq!(pure.pending_complete_count, 1);
    assert_eq!(pure.decision_sync_count, baseline.decision_sync_count + 1);
    assert_eq!(pure.checkpoint_sync_count, baseline.checkpoint_sync_count);

    let structural_before = database.current_database_snapshot().unwrap().unwrap();
    let source = database.bindings.resolve_single(USERS).unwrap();
    let target = database.next_storage_id().unwrap();
    assert!(structural_before.boundary(source).is_some());
    assert!(structural_before.boundary(target).is_none());
    let mut transaction = adopt_shadow(&mut database);
    prepare_shadow(&mut database, &mut transaction);
    let drop = prepared_ddl(&database, &transaction, "DROP INDEX users_legacy_idx");
    evacuate(&mut database, &mut transaction, &drop).unwrap();
    finish_shadow_swap(&mut database, &mut transaction);
    assert_eq!(
        database.current_database_snapshot().unwrap(),
        Some(structural_before.clone())
    );
    database.commit_transaction(&mut transaction).unwrap();

    let final_snapshot = database.current_database_snapshot().unwrap().unwrap();
    assert_eq!(
        final_snapshot.commit_seq().0,
        structural_before.commit_seq().0 + 1
    );
    assert!(final_snapshot.boundary(source).is_none());
    assert!(final_snapshot.boundary(target).is_some());
    let structural = database.inspect_global_visibility().unwrap();
    assert_eq!(
        structural.published_commit_seq,
        Some(final_snapshot.commit_seq())
    );
    assert_eq!(structural.decision_sync_count, pure.decision_sync_count + 1);
    assert_eq!(
        structural.combined_pipeline_sync_count,
        pure.combined_pipeline_sync_count + 1
    );
    assert_eq!(
        structural.checkpoint_sync_count,
        pure.checkpoint_sync_count + 1
    );
    assert_eq!(structural.pending_complete_count, 0);
    assert_eq!(
        structural.last_synced_complete,
        Some(final_snapshot.commit_seq())
    );
    database.close().unwrap();
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn rollback_needs_no_physical_restore_and_index_id_burns_remain_monotonic() {
    for reserve_replacement in [false, true] {
        let path = root(if reserve_replacement {
            "rollback-burn"
        } else {
            "rollback-no-burn"
        });
        let mut database = seed(&path);
        let source = database.bindings.resolve_single(USERS).unwrap();
        let old = old_index(&database);
        let digest = source_digest(&mut database, source);
        let mut transaction = adopt_shadow(&mut database);
        prepare_shadow(&mut database, &mut transaction);
        let drop = prepared_ddl(&database, &transaction, "DROP INDEX users_legacy_idx");
        evacuate(&mut database, &mut transaction, &drop).unwrap();
        if reserve_replacement {
            finish_shadow_swap(&mut database, &mut transaction);
        }
        transaction.rollback().unwrap();
        assert_eq!(database.bindings.resolve_single(USERS), Ok(source));
        assert_eq!(
            database
                .schema()
                .table("users")
                .unwrap()
                .column("legacy")
                .unwrap()
                .id,
            OLD_LEGACY
        );
        assert_eq!(old_index(&database), old);
        assert_eq!(source_digest(&mut database, source), digest);
        std::mem::drop(transaction);

        database
            .execute("CREATE INDEX users_flag_idx ON users(flag)")
            .unwrap();
        let allocated = database
            .indexes(USERS)
            .unwrap()
            .iter()
            .find(|index| index.column_id == FLAG)
            .unwrap()
            .id;
        assert_eq!(
            allocated,
            if reserve_replacement {
                IndexId(3)
            } else {
                IndexId(2)
            }
        );
        database.close().unwrap();
        let reopened = Database::open_catalog(path.join("catalog")).unwrap();
        assert_eq!(
            reopened
                .indexes(USERS)
                .unwrap()
                .iter()
                .find(|index| index.column_id == FLAG)
                .unwrap()
                .id,
            allocated
        );
        reopened.close().unwrap();
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn round58_indexed_swap_crash_child() {
    let Ok(path) = std::env::var("NETBADB_ROUND58_CRASH_ROOT") else {
        return;
    };
    let mut database = Database::open_catalog(Path::new(&path).join("catalog")).unwrap();
    if std::env::var_os("NETBADB_ROUND58_PRIOR_PENDING").is_some() {
        database
            .execute("UPDATE users SET flag = false WHERE id = 1")
            .unwrap();
        assert_eq!(
            database
                .inspect_global_visibility()
                .unwrap()
                .pending_complete_count,
            1
        );
        if let Some(arm_file) = std::env::var_os("NETBADB_COORDINATOR_CRASH_ARM_FILE") {
            std::fs::write(arm_file, b"armed").unwrap();
        }
    }
    let mut transaction = adopt_shadow(&mut database);
    prepare_shadow(&mut database, &mut transaction);
    let drop = prepared_ddl(&database, &transaction, "DROP INDEX users_legacy_idx");
    evacuate(&mut database, &mut transaction, &drop).unwrap();
    finish_shadow_swap(&mut database, &mut transaction);
    database.commit_transaction(&mut transaction).unwrap();
    panic!("configured Round 58 crash hook was not reached");
}

fn assert_no_stage(path: &Path) {
    for entry in std::fs::read_dir(path).unwrap() {
        let entry = entry.unwrap();
        assert!(!entry.file_name().to_string_lossy().contains(".stage"));
        if entry.file_type().unwrap().is_dir() {
            assert_no_stage(&entry.path());
        }
    }
}

fn assert_recovered_indexed_swap(database: &mut Database, source: StorageId, winner: bool) {
    let active = database.bindings.resolve_single(USERS).unwrap();
    let table = database.schema().table("users").unwrap();
    let indexes = database.indexes(USERS).unwrap();
    assert_eq!(indexes.len(), 1);
    if winner {
        assert_ne!(active, source);
        assert_eq!(table.column("legacy").unwrap().id, SHADOW);
        assert!(table.column_by_id(OLD_LEGACY).is_none());
        assert_eq!((indexes[0].id, indexes[0].column_id), (IndexId(2), SHADOW));
        assert_eq!(
            database
                .query("SELECT id, legacy FROM users ORDER BY id")
                .unwrap()
                .rows,
            vec![
                vec![ScalarValue::Int64(1), ScalarValue::Text("updated1".into())],
                vec![ScalarValue::Int64(3), ScalarValue::Text("missing".into())],
                vec![ScalarValue::Int64(4), ScalarValue::Text("inserted4".into())],
            ]
        );
        assert_physical_lookup(database, active, IndexId(2), "updated1", 1);
    } else {
        assert_eq!(active, source);
        assert_eq!(table.column("legacy").unwrap().id, OLD_LEGACY);
        assert!(table.column_by_id(SHADOW).is_none());
        assert_eq!(
            (indexes[0].id, indexes[0].column_id),
            (IndexId(1), OLD_LEGACY)
        );
        assert_eq!(
            database
                .query("SELECT id, legacy FROM users ORDER BY id")
                .unwrap()
                .rows,
            vec![
                vec![ScalarValue::Int64(1), ScalarValue::Text("old1".into())],
                vec![ScalarValue::Int64(2), ScalarValue::Text("old2".into())],
                vec![ScalarValue::Int64(3), ScalarValue::Null],
            ]
        );
        assert_physical_lookup(database, source, IndexId(1), "old1", 1);
    }
}

#[test]
fn indexed_swap_pre_and_post_cord_matrix_converges_on_three_reopens() {
    let cases = [
        (
            "round58-after-logical-index-evacuation",
            false,
            false,
            false,
        ),
        ("round56-after-terminal-drop", false, false, false),
        ("round56-after-terminal-rename", false, false, false),
        (
            "adopted-final-index-reservation-durable",
            false,
            false,
            false,
        ),
        ("composition-before-intent", false, false, false),
        ("composition-intent-durable", false, false, false),
        ("source-backfill-intent-durable", false, false, false),
        ("source-backfill-stage-intent-durable", false, false, false),
        ("composition-stage-first-file", false, false, false),
        ("source-backfill-mid-copy", false, false, false),
        ("source-backfill-final-indexes-built", false, false, false),
        ("before-first-prepare", true, false, false),
        ("after-prepare-1", true, false, false),
        ("after-all-prepares", true, false, false),
        ("after-durable-decision", true, true, false),
        ("after-commit-1", true, true, false),
        ("after-commit-1", true, true, true),
        ("after-all-commits", true, true, false),
    ];
    for (point, coordinator, winner, reverse) in cases {
        let path = root(&format!("crash-{point}-{reverse}"));
        let database = seed(&path);
        let source = database.bindings.resolve_single(USERS).unwrap();
        database.close().unwrap();
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "deferred_index_evacuation_tests::round58_indexed_swap_crash_child",
                "--nocapture",
            ])
            .env("NETBADB_ROUND58_CRASH_ROOT", &path);
        if reverse {
            command.env("NETBADB_REVERSE_PARTICIPANT_COMMIT", "1");
        }
        if coordinator {
            crate::coordinator_crash::configure_child(&mut command, point, &path, point);
        } else {
            command.env("NETBADB_BACKFILL_CRASH_POINT", point);
        }
        let output = command.output().unwrap();
        assert_eq!(
            output.status.code(),
            Some(if coordinator { 87 } else { 90 }),
            "{point} {reverse}: {} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
        for _ in 0..3 {
            let mut reopened = Database::open_catalog(path.join("catalog")).unwrap();
            assert_recovered_indexed_swap(&mut reopened, source, winner);
            reopened.close().unwrap();
        }
        assert_no_stage(&path);
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn prior_pending_complete_and_structural_crash_preserve_g_order() {
    for (point, winner) in [
        ("before-first-prepare", false),
        ("after-durable-decision", true),
        ("after-durable-complete-before-publication", true),
    ] {
        let path = root(&format!("pending-crash-{point}"));
        let database = seed_with_config(&path, true, false);
        let before = database.current_database_snapshot().unwrap().unwrap();
        let source = database.bindings.resolve_single(USERS).unwrap();
        database.close().unwrap();

        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "deferred_index_evacuation_tests::round58_indexed_swap_crash_child",
                "--nocapture",
            ])
            .env("NETBADB_ROUND58_CRASH_ROOT", &path)
            .env("NETBADB_ROUND58_PRIOR_PENDING", "1")
            .env(
                "NETBADB_COORDINATOR_CRASH_ARM_FILE",
                path.join("arm-structural-crash"),
            );
        crate::coordinator_crash::configure_child(&mut command, point, &path, point);
        let output = command.output().unwrap();
        assert_eq!(
            output.status.code(),
            Some(87),
            "{point}: {} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );

        for _ in 0..3 {
            let mut reopened = Database::open_catalog(path.join("catalog")).unwrap();
            let snapshot = reopened.current_database_snapshot().unwrap().unwrap();
            assert_eq!(
                snapshot.commit_seq().0,
                before.commit_seq().0 + if winner { 2 } else { 1 }
            );
            assert_recovered_indexed_swap(&mut reopened, source, winner);
            let visibility = reopened.inspect_global_visibility().unwrap();
            assert_eq!(visibility.published_commit_seq, Some(snapshot.commit_seq()));
            assert_eq!(visibility.pending_complete_count, 0);
            assert_eq!(visibility.last_synced_complete, Some(snapshot.commit_seq()));
            reopened.close().unwrap();
        }
        assert_no_stage(&path);
        std::fs::remove_dir_all(path).unwrap();
    }
}
