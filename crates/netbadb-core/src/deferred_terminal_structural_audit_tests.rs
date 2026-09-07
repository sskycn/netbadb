use super::*;
use crate::deferred_backfill::audit_final_output_projection;
use crate::schema_mutation_journal::SchemaIndexTablePlan;
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_storage::ChangeStreamStatus;
use netbadb_types::{ColumnId, PhysicalType, ScalarValue, StorageId, TableId};
use std::path::{Path, PathBuf};
use std::time::Instant;

fn root(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "netbadb-round55-{name}-{}-{:?}",
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
        "INSERT INTO users VALUES (1, 'old1', true)",
        "INSERT INTO users VALUES (2, 'old2', false)",
        "INSERT INTO users VALUES (3, NULL, NULL)",
    ] {
        database.execute(statement).unwrap();
    }
    database
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

fn adopt_shadow_without_physical_change(database: &mut Database) -> Transaction {
    let mut transaction = database.begin_transaction().unwrap();
    for statement in [
        "UPDATE users SET legacy = legacy WHERE id = 999",
        "ALTER TABLE users ADD COLUMN shadow TEXT",
    ] {
        database.execute_in(&mut transaction, statement).unwrap();
    }
    transaction
}

fn affected(result: ExecutionResult) -> u64 {
    let ExecutionResult::AffectedRows(rows) = result else {
        panic!("expected affected rows")
    };
    rows
}

fn prepare_shadow(database: &mut Database, transaction: &mut Transaction) {
    assert_eq!(
        affected(
            database
                .execute_in(
                    transaction,
                    "UPDATE users SET shadow = legacy WHERE legacy IS NOT NULL",
                )
                .unwrap(),
        ),
        2
    );
    assert_eq!(
        affected(
            database
                .execute_in(
                    transaction,
                    "UPDATE users SET shadow = 'missing' WHERE shadow IS NULL",
                )
                .unwrap(),
        ),
        1
    );
    database
        .execute_in(
            transaction,
            "ALTER TABLE users ALTER COLUMN shadow SET NOT NULL",
        )
        .unwrap();
}

fn prepared_ddl(database: &Database, transaction: &Transaction, sql: &str) -> PreparedDdlStatement {
    match database
        .prepare_sql_statement_in(transaction, sql, &[])
        .unwrap()
    {
        PreparedSqlStatement::Ddl(prepared) => prepared,
        PreparedSqlStatement::Relational(_) => panic!("expected DDL"),
    }
}

fn audit_terminal_prepared(
    database: &mut Database,
    transaction: &mut Transaction,
    prepared: &PreparedDdlStatement,
) -> Result<(), DatabaseError> {
    let CompiledDdlStatement::AlterTable(statement) = &prepared.compiled else {
        return Err(DatabaseError::UnsupportedDdlCombination);
    };
    database.audit_apply_terminal_alter_in(transaction, AlterTableSpec::from(statement))
}

fn audit_terminal(
    database: &mut Database,
    transaction: &mut Transaction,
    sql: &str,
) -> Result<(), DatabaseError> {
    let prepared = prepared_ddl(database, transaction, sql);
    audit_terminal_prepared(database, transaction, &prepared)
}

fn assert_physical_index_key(
    database: &mut Database,
    storage: StorageId,
    column: ColumnId,
    key: &str,
    expected_id: i64,
) {
    let storage = database.registry.get_mut(storage).unwrap();
    let index = storage
        .access_paths()
        .into_iter()
        .find(|path| path.column_id == column)
        .unwrap();
    let view = storage.read_view().unwrap();
    let rows = storage
        .point_lookup_columns_with_view(
            index.id,
            &ScalarValue::Text(key.into()),
            &[ColumnId(1)],
            &view,
        )
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1, vec![ScalarValue::Int64(expected_id)]);
}

#[test]
fn production_terminal_structural_alter_remains_closed() {
    for statement in [
        "ALTER TABLE users DROP COLUMN legacy",
        "ALTER TABLE users RENAME COLUMN shadow TO replacement",
        "ALTER TABLE users RENAME TO people",
    ] {
        let path = root("production-closed");
        let mut database = seed(&path);
        let source = database.bindings.resolve_single(TableId(2)).unwrap();
        let storage_floor = database.next_storage_id().unwrap();
        let mut transaction = adopt_shadow(&mut database);
        database
            .execute_in(
                &mut transaction,
                "UPDATE users SET shadow = legacy WHERE id = 999",
            )
            .unwrap();
        assert!(matches!(
            database.execute_in(&mut transaction, statement),
            Err(DatabaseError::SchemaMutation(
                SchemaMutationError::SchemaMutationAfterMaterialization
            ))
        ));
        assert_eq!(database.bindings.resolve_single(TableId(2)), Ok(source));
        assert_eq!(database.next_storage_id().unwrap(), storage_floor);
        assert_eq!(
            database.registry.get(source).unwrap().table().columns.len(),
            3
        );
        transaction.rollback().unwrap();
        database.close().unwrap();
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn frozen_evaluation_schema_materializes_atomic_shadow_swap_once() {
    let path = root("full");
    let mut database = seed(&path);
    database.enable_change_stream(TableId(2)).unwrap();
    database.disable_change_stream(TableId(2)).unwrap();
    assert_eq!(
        database.inspect_change_stream(TableId(2)).unwrap().status,
        ChangeStreamStatus::Disabled
    );
    let projection = database
        .build_columnar_projection(ColumnarProjectionSpec::new(
            TableId(2),
            path.join("projection"),
            vec![ColumnId(1), ColumnId(2)],
        ))
        .unwrap();
    let source = database.bindings.resolve_single(TableId(2)).unwrap();
    let target_floor = database.next_storage_id().unwrap();
    let mut transaction = adopt_shadow(&mut database);
    prepare_shadow(&mut database, &mut transaction);
    let program_digests = (0..2)
        .map(|index| {
            transaction
                .schema_composition
                .plan()
                .unwrap()
                .deferred_backfill
                .semantic_digest(index)
                .unwrap()
        })
        .collect::<Vec<_>>();
    let program_positions = (0..2)
        .map(|index| {
            transaction
                .schema_composition
                .plan()
                .unwrap()
                .deferred_backfill
                .action_cached_positions(index)
                .unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        transaction
            .schema_composition
            .plan()
            .unwrap()
            .deferred_backfill
            .evaluation_column_ids(),
        Some(vec![ColumnId(1), ColumnId(2), ColumnId(3), ColumnId(4)])
    );

    audit_terminal(
        &mut database,
        &mut transaction,
        "ALTER TABLE users DROP COLUMN legacy",
    )
    .unwrap();
    audit_terminal(
        &mut database,
        &mut transaction,
        "ALTER TABLE users RENAME COLUMN shadow TO legacy",
    )
    .unwrap();
    assert_eq!(
        (0..2)
            .map(|index| {
                transaction
                    .schema_composition
                    .plan()
                    .unwrap()
                    .deferred_backfill
                    .semantic_digest(index)
                    .unwrap()
            })
            .collect::<Vec<_>>(),
        program_digests
    );
    assert_eq!(
        (0..2)
            .map(|index| {
                transaction
                    .schema_composition
                    .plan()
                    .unwrap()
                    .deferred_backfill
                    .action_cached_positions(index)
                    .unwrap()
            })
            .collect::<Vec<_>>(),
        program_positions
    );
    assert_eq!(
        transaction
            .schema_composition
            .plan()
            .unwrap()
            .deferred_backfill
            .evaluation_column_ids(),
        Some(vec![ColumnId(1), ColumnId(2), ColumnId(3), ColumnId(4)])
    );
    let final_table = transaction
        .schema_composition
        .plan()
        .unwrap()
        .overlay
        .schema
        .table("users")
        .unwrap();
    assert_eq!(
        final_table
            .columns
            .iter()
            .map(|column| (column.id, column.name.as_str(), column.nullable))
            .collect::<Vec<_>>(),
        vec![
            (ColumnId(1), "id", false),
            (ColumnId(3), "flag", true),
            (ColumnId(4), "legacy", false),
        ]
    );
    assert_eq!(
        database
            .registry
            .get(source)
            .unwrap()
            .table()
            .columns
            .iter()
            .map(|column| column.id)
            .collect::<Vec<_>>(),
        vec![ColumnId(1), ColumnId(2), ColumnId(3)]
    );
    database
        .execute_in(
            &mut transaction,
            "CREATE INDEX users_legacy_idx ON users(legacy)",
        )
        .unwrap();
    database.finalize_adopted_source(&mut transaction).unwrap();
    let target = {
        let materialized = transaction.schema_composition.materialized_index().unwrap();
        assert_eq!(materialized.source_copy_passes, 1);
        assert_eq!(materialized.source_rows_copied, 3);
        let SchemaIndexTablePlan::RewriteHeap {
            replacement,
            final_indexes,
            ..
        } = &materialized.intent.tables[0]
        else {
            panic!("expected one Heap rewrite")
        };
        assert_eq!(final_indexes.active.len(), 1);
        assert_eq!(final_indexes.active[0].column_id, ColumnId(4));
        let target = replacement.new_storage();
        assert_eq!(target, target_floor);
        assert_eq!(materialized.staged[&target].table().columns.len(), 3);
        target
    };
    database.commit_transaction(&mut transaction).unwrap();
    assert_ne!(target, source);
    assert_eq!(
        database.next_storage_id().unwrap(),
        StorageId(target_floor.0 + 1)
    );
    {
        let storage = database.registry.get_mut(target).unwrap();
        assert_eq!(
            storage
                .table()
                .columns
                .iter()
                .map(|column| column.id)
                .collect::<Vec<_>>(),
            vec![ColumnId(1), ColumnId(3), ColumnId(4)]
        );
        let view = storage.read_view().unwrap();
        let physical_rows = storage
            .scan_columns_with_view(&[ColumnId(1), ColumnId(3), ColumnId(4)], &view)
            .unwrap();
        assert_eq!(physical_rows.len(), 3);
        assert!(physical_rows.iter().all(|(_, values)| values.len() == 3));
    }
    assert_ne!(
        database
            .inspect_columnar_projections()
            .into_iter()
            .find(|entry| entry.projection_id == Some(projection))
            .unwrap()
            .health,
        ColumnarProjectionHealth::Fresh
    );

    for _ in 0..3 {
        database.close().unwrap();
        database = Database::open_catalog(path.join("catalog")).unwrap();
        let table = database.schema().table("users").unwrap();
        assert_eq!(table.column("legacy").unwrap().id, ColumnId(4));
        assert!(table.column_by_id(ColumnId(2)).is_none());
        assert_eq!(table.columns.len(), 3);
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
        assert_physical_index_key(&mut database, target, ColumnId(4), "updated1", 1);
        assert_physical_index_key(&mut database, target, ColumnId(4), "missing", 3);
        assert_physical_index_key(&mut database, target, ColumnId(4), "inserted4", 4);
    }
    database.close().unwrap();
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn dropped_late_intermediate_remains_an_evaluation_only_dependency() {
    let path = root("intermediate");
    let mut database = seed(&path);
    let mut transaction = database.begin_transaction().unwrap();
    for statement in [
        "UPDATE users SET legacy = 'updated1' WHERE id = 1",
        "INSERT INTO users VALUES (4, 'inserted4', true)",
        "DELETE FROM users WHERE id = 2",
        "ALTER TABLE users ADD COLUMN marker TEXT",
        "ALTER TABLE users ADD COLUMN normalized TEXT",
        "UPDATE users SET marker = legacy WHERE legacy IS NOT NULL",
        "UPDATE users SET marker = 'missing' WHERE marker IS NULL",
        "UPDATE users SET normalized = marker WHERE marker IS NOT NULL",
        "ALTER TABLE users ALTER COLUMN normalized SET NOT NULL",
    ] {
        database.execute_in(&mut transaction, statement).unwrap();
    }
    assert_eq!(
        transaction
            .schema_composition
            .plan()
            .unwrap()
            .deferred_backfill
            .evaluation_column_ids(),
        Some(vec![
            ColumnId(1),
            ColumnId(2),
            ColumnId(3),
            ColumnId(4),
            ColumnId(5),
        ])
    );
    for statement in [
        "ALTER TABLE users DROP COLUMN legacy",
        "ALTER TABLE users DROP COLUMN marker",
        "ALTER TABLE users RENAME COLUMN normalized TO legacy",
    ] {
        audit_terminal(&mut database, &mut transaction, statement).unwrap();
    }
    database
        .execute_in(
            &mut transaction,
            "CREATE INDEX users_legacy_idx ON users(legacy)",
        )
        .unwrap();
    database.commit_transaction(&mut transaction).unwrap();
    let table = database.schema().table("users").unwrap();
    assert!(table.column_by_id(ColumnId(2)).is_none());
    assert!(table.column_by_id(ColumnId(4)).is_none());
    assert_eq!(table.column("legacy").unwrap().id, ColumnId(5));
    assert_eq!(table.columns.len(), 3);
    assert_eq!(
        database.indexes(TableId(2)).unwrap()[0].column_id,
        ColumnId(5)
    );
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
    database.close().unwrap();
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn zero_row_first_action_still_freezes_evaluation_schema() {
    let path = root("zero-freeze");
    let mut database = seed(&path);
    let mut transaction = adopt_shadow(&mut database);
    assert_eq!(
        affected(
            database
                .execute_in(
                    &mut transaction,
                    "UPDATE users SET shadow = legacy WHERE id = 999",
                )
                .unwrap(),
        ),
        0
    );
    assert_eq!(
        transaction
            .schema_composition
            .plan()
            .unwrap()
            .deferred_backfill
            .evaluation_column_ids(),
        Some(vec![ColumnId(1), ColumnId(2), ColumnId(3), ColumnId(4)])
    );
    audit_terminal(
        &mut database,
        &mut transaction,
        "ALTER TABLE users DROP COLUMN legacy",
    )
    .unwrap();
    assert_eq!(
        transaction
            .schema_composition
            .plan()
            .unwrap()
            .deferred_backfill
            .evaluation_column_ids(),
        Some(vec![ColumnId(1), ColumnId(2), ColumnId(3), ColumnId(4)])
    );
    transaction.rollback().unwrap();
    database.close().unwrap();
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn terminal_phase_seals_updates_and_preserves_prepared_dependency_order() {
    let path = root("prepared");
    let mut database = seed(&path);
    let mut transaction = adopt_shadow(&mut database);
    database
        .execute_in(
            &mut transaction,
            "UPDATE users SET shadow = legacy WHERE legacy IS NOT NULL",
        )
        .unwrap();
    let prepared_update = match database
        .prepare_sql_statement_in(
            &transaction,
            "UPDATE users SET shadow = 'late' WHERE id = 1",
            &[],
        )
        .unwrap()
    {
        PreparedSqlStatement::Relational(prepared) => prepared,
        PreparedSqlStatement::Ddl(_) => panic!("expected UPDATE"),
    };
    let prepared_rename = prepared_ddl(
        &database,
        &transaction,
        "ALTER TABLE users RENAME COLUMN shadow TO replacement",
    );
    audit_terminal(
        &mut database,
        &mut transaction,
        "ALTER TABLE users DROP COLUMN legacy",
    )
    .unwrap();
    assert!(matches!(
        database.execute_prepared_in(&mut transaction, &prepared_update, &[]),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::StalePreparedStatement
        ))
    ));
    assert!(matches!(
        audit_terminal_prepared(&mut database, &mut transaction, &prepared_rename),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::StaleSchemaDependency
        ))
    ));
    let fresh_update = match database
        .prepare_sql_statement_in(
            &transaction,
            "UPDATE users SET shadow = 'late' WHERE id = 1",
            &[],
        )
        .unwrap()
    {
        PreparedSqlStatement::Relational(prepared) => prepared,
        PreparedSqlStatement::Ddl(_) => panic!("expected UPDATE"),
    };
    assert!(matches!(
        database.execute_prepared_in(&mut transaction, &fresh_update, &[]),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::MigrationDataAccessAfterRefinement
        ))
    ));
    let add = prepared_ddl(
        &database,
        &transaction,
        "ALTER TABLE users ADD COLUMN too_late TEXT",
    );
    assert!(matches!(
        audit_terminal_prepared(&mut database, &mut transaction, &add),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::SchemaMutationAfterMaterialization
        ))
    ));
    transaction.rollback().unwrap();
    database.close().unwrap();
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn indexed_old_column_is_outside_the_round56_baseline() {
    let path = root("indexed-old");
    let mut database = seed(&path);
    database
        .execute("CREATE INDEX users_old_idx ON users(legacy)")
        .unwrap();
    let mut transaction = adopt_shadow(&mut database);
    prepare_shadow(&mut database, &mut transaction);
    assert!(matches!(
        audit_terminal(
            &mut database,
            &mut transaction,
            "ALTER TABLE users DROP COLUMN legacy",
        ),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::IndexedColumn(ColumnId(2))
        ))
    ));
    assert_eq!(
        database.indexes(TableId(2)).unwrap()[0].column_id,
        ColumnId(2)
    );
    transaction.rollback().unwrap();
    database.close().unwrap();
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn terminal_table_rename_composes_with_column_swap() {
    let path = root("rename-table");
    let mut database = seed(&path);
    let mut transaction = adopt_shadow(&mut database);
    prepare_shadow(&mut database, &mut transaction);
    for statement in [
        "ALTER TABLE users RENAME TO people",
        "ALTER TABLE people DROP COLUMN legacy",
        "ALTER TABLE people RENAME COLUMN shadow TO legacy",
    ] {
        audit_terminal(&mut database, &mut transaction, statement).unwrap();
    }
    database
        .execute_in(
            &mut transaction,
            "CREATE INDEX people_legacy_idx ON people(legacy)",
        )
        .unwrap();
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
        ColumnId(4)
    );
    assert_eq!(
        database
            .query("SELECT id, legacy FROM people ORDER BY id")
            .unwrap()
            .rows,
        vec![
            vec![ScalarValue::Int64(1), ScalarValue::Text("updated1".into())],
            vec![ScalarValue::Int64(3), ScalarValue::Text("missing".into())],
            vec![ScalarValue::Int64(4), ScalarValue::Text("inserted4".into())],
        ]
    );
    database.close().unwrap();
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn active_terminal_migration_keeps_maintenance_busy_and_non_authoritative() {
    let path = root("maintenance");
    let projection_path = path.join("projection");
    let mut database = seed(&path);
    database.enable_change_stream(TableId(2)).unwrap();
    database
        .build_incremental_columnar_projection(ColumnarProjectionSpec::new(
            TableId(2),
            &projection_path,
            vec![ColumnId(1), ColumnId(2)],
        ))
        .unwrap();
    database
        .execute("UPDATE users SET legacy = 'pending' WHERE id = 1")
        .unwrap();
    let manifest = projection_path.join("projection.nbcmanifest");
    let before = std::fs::read(&manifest).unwrap();
    let mut transaction = adopt_shadow_without_physical_change(&mut database);
    prepare_terminal_swap(&mut database, &mut transaction);
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

fn digest_case(name: &str, swap: bool) -> (Vec<[u8; 32]>, [u8; 32], [u8; 32], [u8; 32]) {
    let path = root(name);
    let mut database = seed(&path);
    let mut transaction = adopt_shadow(&mut database);
    prepare_shadow(&mut database, &mut transaction);
    let semantic = (0..2)
        .map(|index| {
            transaction
                .schema_composition
                .plan()
                .unwrap()
                .deferred_backfill
                .semantic_digest(index)
                .unwrap()
        })
        .collect::<Vec<_>>();
    if swap {
        audit_terminal(
            &mut database,
            &mut transaction,
            "ALTER TABLE users DROP COLUMN legacy",
        )
        .unwrap();
        audit_terminal(
            &mut database,
            &mut transaction,
            "ALTER TABLE users RENAME COLUMN shadow TO legacy",
        )
        .unwrap();
    }
    database.finalize_adopted_source(&mut transaction).unwrap();
    let materialized = transaction.schema_composition.materialized_index().unwrap();
    let action = materialized.intent.action_digest;
    let snapshot = materialized.intent.snapshot_digest;
    let clone = database
        .mutation_journal
        .as_ref()
        .unwrap()
        .borrow()
        .source_backfill_intents
        .get(&transaction.id())
        .unwrap()
        .clone_plan_digest;
    transaction.rollback().unwrap();
    database.close().unwrap();
    std::fs::remove_dir_all(path).unwrap();
    (semantic, action, snapshot, clone)
}

#[test]
fn terminal_schema_changes_bind_snapshot_action_and_clone_plan_not_program_bytes() {
    let keep = digest_case("digest-keep", false);
    let swap = digest_case("digest-swap", true);
    assert_eq!(keep.0, swap.0);
    assert_ne!(keep.1, swap.1);
    assert_ne!(keep.2, swap.2);
    assert_ne!(keep.3, swap.3);
}

#[test]
fn final_output_projection_cost_is_linear_in_rows_and_final_width() {
    for (width, dropped) in [(4_usize, 1_usize), (16, 4), (64, 16), (128, 32)] {
        let evaluation = TableDef::new(
            TableId(1),
            "wide",
            (0..width)
                .map(|position| {
                    ColumnDef::new(
                        ColumnId(u32::try_from(position + 1).unwrap()),
                        format!("c{position}"),
                        TypeSpec::Physical(PhysicalType::UInt64),
                    )
                })
                .collect(),
        );
        let final_table = TableDef::new(TableId(1), "wide", evaluation.columns[dropped..].to_vec());
        let row = (0..width)
            .map(|value| ScalarValue::UInt64(u64::try_from(value).unwrap()))
            .collect::<Vec<_>>();
        let rows = 10_000;
        let started = Instant::now();
        let cost = audit_final_output_projection(&evaluation, &final_table, &row, rows).unwrap();
        let elapsed = started.elapsed();
        assert_eq!(cost.rows, rows);
        assert_eq!(cost.evaluation_width, width);
        assert_eq!(cost.final_width, width - dropped);
        assert_eq!(cost.dropped_columns, dropped);
        assert_eq!(cost.output_allocations, rows);
        assert_eq!(cost.copied_values, rows * (width - dropped));
        eprintln!(
            "round55-final-projection width={width} dropped={dropped} rows={rows} elapsed_us={} allocations={} copied_values={}",
            elapsed.as_micros(),
            cost.output_allocations,
            cost.copied_values,
        );
    }
}

#[test]
fn terminal_audit_rollback_restores_s1_and_discards_e_and_f() {
    let path = root("rollback");
    let mut database = seed(&path);
    let source = database.bindings.resolve_single(TableId(2)).unwrap();
    let storage_floor = database.next_storage_id().unwrap();
    let mut transaction = adopt_shadow(&mut database);
    prepare_shadow(&mut database, &mut transaction);
    audit_terminal(
        &mut database,
        &mut transaction,
        "ALTER TABLE users DROP COLUMN legacy",
    )
    .unwrap();
    audit_terminal(
        &mut database,
        &mut transaction,
        "ALTER TABLE users RENAME COLUMN shadow TO legacy",
    )
    .unwrap();
    transaction.rollback().unwrap();
    assert_eq!(database.bindings.resolve_single(TableId(2)), Ok(source));
    assert_eq!(database.next_storage_id().unwrap(), storage_floor);
    let table = database.schema().table("users").unwrap();
    assert_eq!(table.column("legacy").unwrap().id, ColumnId(2));
    assert!(table.column_by_id(ColumnId(4)).is_none());
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
    database.close().unwrap();
    std::fs::remove_dir_all(path).unwrap();
}

fn assert_stream_block(
    error: DatabaseError,
    table: TableId,
    storage: StorageId,
    status: ChangeStreamStatus,
) {
    assert!(matches!(
        error,
        DatabaseError::SchemaMutation(
            SchemaMutationError::ActiveChangeStreamBlocksReplacement {
                table_id,
                storage_id,
                status: actual,
            }
        ) if table_id == table && storage_id == storage && actual == status
    ));
}

fn prepare_terminal_swap(database: &mut Database, transaction: &mut Transaction) {
    prepare_shadow(database, transaction);
    audit_terminal(
        database,
        transaction,
        "ALTER TABLE users DROP COLUMN legacy",
    )
    .unwrap();
    audit_terminal(
        database,
        transaction,
        "ALTER TABLE users RENAME COLUMN shadow TO legacy",
    )
    .unwrap();
}

#[test]
fn enabled_and_unavailable_streams_still_block_terminal_replacement() {
    let enabled_path = root("stream-enabled");
    let mut database = seed(&enabled_path);
    let table = TableId(2);
    let source = database.enable_change_stream(table).unwrap().storage_id;
    let storage_floor = database.next_storage_id();
    let mut transaction = adopt_shadow(&mut database);
    prepare_terminal_swap(&mut database, &mut transaction);
    let error = database.commit_transaction(&mut transaction).unwrap_err();
    assert_stream_block(error, table, source, ChangeStreamStatus::Enabled);
    assert_eq!(database.next_storage_id(), storage_floor);
    transaction.rollback().unwrap();
    assert_eq!(database.bindings.resolve_single(table), Ok(source));
    database.close().unwrap();
    std::fs::remove_dir_all(enabled_path).unwrap();

    let unavailable_path = root("stream-unavailable");
    let mut database = seed(&unavailable_path);
    let source = database.enable_change_stream(table).unwrap().storage_id;
    database.close().unwrap();
    let snapshot = crate::schema_catalog_file::load(&unavailable_path.join("catalog")).unwrap();
    let locator = snapshot
        .storages
        .iter()
        .find(|entry| entry.id == source)
        .unwrap()
        .locator
        .clone();
    let heap = crate::schema_catalog_file::resolve(&unavailable_path.join("catalog"), &locator);
    std::fs::remove_file(netbadb_storage::heap_change_log_path(&heap)).unwrap();
    let mut database = Database::open_catalog(unavailable_path.join("catalog")).unwrap();
    assert_eq!(
        database.inspect_change_stream(table).unwrap().status,
        ChangeStreamStatus::Unavailable
    );
    let storage_floor = database.next_storage_id();
    let mut transaction = adopt_shadow_without_physical_change(&mut database);
    prepare_terminal_swap(&mut database, &mut transaction);
    let error = database.commit_transaction(&mut transaction).unwrap_err();
    assert_stream_block(error, table, source, ChangeStreamStatus::Unavailable);
    assert_eq!(database.next_storage_id(), storage_floor);
    transaction.rollback().unwrap();
    assert_eq!(database.bindings.resolve_single(table), Ok(source));
    database.close().unwrap();
    std::fs::remove_dir_all(unavailable_path).unwrap();
}

#[test]
fn round55_crash_child() {
    let Ok(root) = std::env::var("NETBADB_ROUND55_CRASH_ROOT") else {
        return;
    };
    let mut database = Database::open_catalog(Path::new(&root).join("catalog")).unwrap();
    let mut transaction = adopt_shadow(&mut database);
    prepare_terminal_swap(&mut database, &mut transaction);
    database
        .execute_in(
            &mut transaction,
            "CREATE INDEX users_legacy_idx ON users(legacy)",
        )
        .unwrap();
    database.commit_transaction(&mut transaction).unwrap();
    panic!("configured Round 55 crash hook was not reached");
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

#[test]
fn terminal_structural_crash_matrix_converges_without_replaying_e() {
    let cases = [
        ("round55-after-terminal-drop", false, false, false),
        ("round55-after-terminal-rename", false, false, false),
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
        let source = database.bindings.resolve_single(TableId(2)).unwrap();
        database.close().unwrap();
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "deferred_terminal_structural_audit_tests::round55_crash_child",
                "--nocapture",
            ])
            .env("NETBADB_ROUND55_CRASH_ROOT", &path);
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
            if winner {
                let target = reopened.bindings.resolve_single(TableId(2)).unwrap();
                assert_ne!(target, source);
                let table = reopened.schema().table("users").unwrap();
                assert_eq!(table.column("legacy").unwrap().id, ColumnId(4));
                assert!(table.column_by_id(ColumnId(2)).is_none());
                assert_eq!(table.columns.len(), 3);
                assert_eq!(
                    reopened.indexes(TableId(2)).unwrap()[0].column_id,
                    ColumnId(4)
                );
                assert_eq!(
                    reopened
                        .query("SELECT id, legacy FROM users ORDER BY id")
                        .unwrap()
                        .rows,
                    vec![
                        vec![ScalarValue::Int64(1), ScalarValue::Text("updated1".into()),],
                        vec![ScalarValue::Int64(3), ScalarValue::Text("missing".into())],
                        vec![ScalarValue::Int64(4), ScalarValue::Text("inserted4".into()),],
                    ]
                );
                assert_physical_index_key(&mut reopened, target, ColumnId(4), "updated1", 1);
            } else {
                assert_eq!(reopened.bindings.resolve_single(TableId(2)), Ok(source));
                let table = reopened.schema().table("users").unwrap();
                assert_eq!(table.column("legacy").unwrap().id, ColumnId(2));
                assert!(table.column_by_id(ColumnId(4)).is_none());
                assert_eq!(
                    reopened
                        .query("SELECT id, legacy FROM users ORDER BY id")
                        .unwrap()
                        .rows,
                    vec![
                        vec![ScalarValue::Int64(1), ScalarValue::Text("old1".into())],
                        vec![ScalarValue::Int64(2), ScalarValue::Text("old2".into())],
                        vec![ScalarValue::Int64(3), ScalarValue::Null],
                    ]
                );
            }
            reopened.close().unwrap();
        }
        assert_no_stage(&path);
        std::fs::remove_dir_all(path).unwrap();
    }
}
