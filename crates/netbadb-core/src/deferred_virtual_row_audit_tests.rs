use super::*;
use crate::deferred_backfill::{
    VirtualAuditExecution, audit_execute_virtual_adopted_update, audit_replay_virtual_rows,
};
use crate::schema_composition::SchemaCompositionState;
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_storage::ChangeStreamStatus;
use netbadb_types::{ColumnId, PhysicalType, ScalarValue, StorageId, TableId};
use std::path::{Path, PathBuf};
use std::time::Instant;

fn root(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "netbadb-round53-{name}-{}-{:?}",
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

fn adopt(database: &mut Database) -> Transaction {
    let mut transaction = database.begin_transaction().unwrap();
    for statement in [
        "UPDATE users SET legacy = 'updated1' WHERE id = 1",
        "INSERT INTO users VALUES (4, 'inserted4', true)",
        "DELETE FROM users WHERE id = 2",
        "ALTER TABLE users ADD COLUMN marker TEXT",
        "ALTER TABLE users ADD COLUMN normalized TEXT",
    ] {
        database.execute_in(&mut transaction, statement).unwrap();
    }
    assert!(matches!(
        transaction.schema_composition,
        SchemaCompositionState::AdoptedSourceRefining(_)
    ));
    transaction
}

fn relational(prepared: PreparedSqlStatement) -> Box<PreparedStatement> {
    match prepared {
        PreparedSqlStatement::Relational(prepared) => prepared,
        PreparedSqlStatement::Ddl(_) => panic!("expected relational statement"),
    }
}

fn virtual_prepared(
    database: &mut Database,
    transaction: &mut Transaction,
    prepared: &PreparedStatement,
    values: &[ScalarValue],
) -> Result<VirtualAuditExecution, DatabaseError> {
    database.validate_transaction(transaction)?;
    database.validate_prepared_dependencies(prepared, Some(transaction))?;
    let logical = bind_statement(&prepared.compiled, values)?;
    audit_execute_virtual_adopted_update(database, transaction, &logical)?
        .ok_or_else(|| SchemaMutationError::MigrationDataAccessAfterRefinement.into())
}

fn virtual_sql(
    database: &mut Database,
    transaction: &mut Transaction,
    sql: &str,
) -> Result<VirtualAuditExecution, DatabaseError> {
    let prepared = relational(database.prepare_sql_statement_in(transaction, sql, &[])?);
    virtual_prepared(database, transaction, &prepared, &[])
}

fn assert_physical_index_key(
    database: &mut Database,
    storage: StorageId,
    column: ColumnId,
    key: &ScalarValue,
    expected_ids: &[i64],
) {
    let storage = database.registry.get_mut(storage).unwrap();
    let index = storage
        .access_paths()
        .into_iter()
        .find(|path| path.column_id == column)
        .unwrap()
        .id;
    let view = storage.read_view().unwrap();
    let mut ids = storage
        .point_lookup_columns_with_view(index, key, &[ColumnId(1)], &view)
        .unwrap()
        .into_iter()
        .map(|(_, values)| match values[0] {
            ScalarValue::Int64(id) => id,
            _ => panic!("expected BIGINT id"),
        })
        .collect::<Vec<_>>();
    ids.sort_unstable();
    assert_eq!(ids, expected_ids);
}

#[test]
fn virtual_prefix_full_migration_repairs_copies_and_builds_one_s2() {
    let root = root("full");
    let mut database = seed(&root);
    let mut transaction = adopt(&mut database);
    let source = transaction
        .schema_composition
        .adopted_source()
        .unwrap()
        .source_storage;
    let next_storage = database.next_storage_id().unwrap();
    let source_index_digest = crate::schema_mutation_journal::heap_rewrite_indexes_digest(
        &database
            .registry
            .get_mut(source)
            .unwrap()
            .heap_rewrite_indexes()
            .unwrap(),
    )
    .unwrap();

    let first = virtual_sql(
        &mut database,
        &mut transaction,
        "UPDATE users SET marker = legacy WHERE marker IS NULL AND legacy IS NOT NULL",
    )
    .unwrap();
    assert_eq!(first.affected_rows, 2);
    assert_eq!(first.source_scans, 1);
    assert_eq!(first.source_rows, 3);
    assert_eq!(first.prior_action_evaluations, 0);
    assert!(matches!(
        transaction.schema_composition,
        SchemaCompositionState::AdoptedSourceBackfilling(_)
    ));
    assert!(matches!(
        database.execute_in(
            &mut transaction,
            "ALTER TABLE users ALTER COLUMN marker SET NOT NULL"
        ),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::NotNullViolation(ColumnId(4))
        ))
    ));

    let repair = virtual_sql(
        &mut database,
        &mut transaction,
        "UPDATE users SET marker = 'missing' WHERE marker IS NULL",
    )
    .unwrap();
    assert_eq!(repair.affected_rows, 1);
    assert_eq!(repair.prior_action_evaluations, 3);
    database
        .execute_in(
            &mut transaction,
            "ALTER TABLE users ALTER COLUMN marker SET NOT NULL",
        )
        .unwrap();

    let copy = virtual_sql(
        &mut database,
        &mut transaction,
        "UPDATE users SET normalized = marker WHERE marker IS NOT NULL",
    )
    .unwrap();
    assert_eq!(copy.affected_rows, 3);
    assert_eq!(copy.prior_action_evaluations, 6);
    let before_rejected = transaction
        .schema_composition
        .plan()
        .unwrap()
        .deferred_backfill
        .len();
    let nullable_write = relational(
        database
            .prepare_sql_statement_in(
                &transaction,
                "UPDATE users SET marker = $1 WHERE id = 1",
                &[Some(PhysicalType::Text)],
            )
            .unwrap(),
    );
    let not_null_error = virtual_prepared(
        &mut database,
        &mut transaction,
        &nullable_write,
        &[ScalarValue::Null],
    )
    .unwrap_err();
    assert!(
        matches!(
            not_null_error,
            DatabaseError::SchemaMutation(SchemaMutationError::NotNullViolation(ColumnId(4)))
        ),
        "{not_null_error:?}"
    );
    assert_eq!(
        transaction
            .schema_composition
            .plan()
            .unwrap()
            .deferred_backfill
            .len(),
        before_rejected
    );
    let zero = virtual_sql(
        &mut database,
        &mut transaction,
        "UPDATE users SET normalized = 'never' WHERE marker = 'absent'",
    )
    .unwrap();
    assert_eq!(zero.affected_rows, 0);
    database
        .execute_in(
            &mut transaction,
            "ALTER TABLE users ALTER COLUMN normalized SET NOT NULL",
        )
        .unwrap();
    database
        .execute_in(
            &mut transaction,
            "CREATE INDEX users_normalized_idx ON users(normalized)",
        )
        .unwrap();

    assert_eq!(database.next_storage_id().unwrap(), next_storage);
    assert_eq!(
        database
            .registry
            .get_mut(source)
            .unwrap()
            .table()
            .columns
            .len(),
        3
    );
    assert_eq!(
        crate::schema_mutation_journal::heap_rewrite_indexes_digest(
            &database
                .registry
                .get_mut(source)
                .unwrap()
                .heap_rewrite_indexes()
                .unwrap(),
        )
        .unwrap(),
        source_index_digest
    );
    database.finalize_adopted_source(&mut transaction).unwrap();
    let materialized = transaction.schema_composition.materialized_index().unwrap();
    assert_eq!(materialized.source_copy_passes, 1);
    assert_eq!(materialized.source_rows_copied, 3);
    let crate::schema_mutation_journal::SchemaIndexTablePlan::RewriteHeap { replacement, .. } =
        &materialized.intent.tables[0]
    else {
        panic!("expected one Heap replacement")
    };
    let target = replacement.new_storage();
    assert_eq!(target, next_storage);
    database.commit_transaction(&mut transaction).unwrap();
    assert_eq!(database.bindings.resolve_single(TableId(2)), Ok(target));

    for _ in 0..3 {
        database.close().unwrap();
        database = Database::open_catalog(root.join("catalog")).unwrap();
        assert_eq!(
            database
                .query("SELECT id, marker, normalized FROM users ORDER BY id")
                .unwrap()
                .rows,
            vec![
                vec![
                    ScalarValue::Int64(1),
                    ScalarValue::Text("updated1".into()),
                    ScalarValue::Text("updated1".into()),
                ],
                vec![
                    ScalarValue::Int64(3),
                    ScalarValue::Text("missing".into()),
                    ScalarValue::Text("missing".into()),
                ],
                vec![
                    ScalarValue::Int64(4),
                    ScalarValue::Text("inserted4".into()),
                    ScalarValue::Text("inserted4".into()),
                ],
            ]
        );
        assert_physical_index_key(
            &mut database,
            target,
            ColumnId(5),
            &ScalarValue::Text("missing".into()),
            &[3],
        );
    }
    database.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn synthesized_null_simultaneous_and_swap_semantics_use_pre_action_rows() {
    let simultaneous_root = root("simultaneous");
    let mut database = seed(&simultaneous_root);
    let mut transaction = adopt(&mut database);
    assert_eq!(
        virtual_sql(
            &mut database,
            &mut transaction,
            "UPDATE users SET marker = 'initial' WHERE marker IS NULL"
        )
        .unwrap()
        .affected_rows,
        3
    );
    virtual_sql(
        &mut database,
        &mut transaction,
        "UPDATE users SET normalized = 'old-normalized' WHERE id = 1",
    )
    .unwrap();
    virtual_sql(
        &mut database,
        &mut transaction,
        "UPDATE users SET marker = 'new-marker', normalized = marker WHERE marker = 'initial' AND id = 1",
    )
    .unwrap();
    database.commit_transaction(&mut transaction).unwrap();
    assert_eq!(
        database
            .query("SELECT marker, normalized FROM users WHERE id = 1")
            .unwrap()
            .rows,
        vec![vec![
            ScalarValue::Text("new-marker".into()),
            ScalarValue::Text("initial".into()),
        ]]
    );
    database.close().unwrap();
    std::fs::remove_dir_all(simultaneous_root).unwrap();

    let swap_root = root("swap");
    let mut database = seed(&swap_root);
    let mut transaction = adopt(&mut database);
    for statement in [
        "UPDATE users SET marker = 'old-marker' WHERE id = 1",
        "UPDATE users SET normalized = 'old-normalized' WHERE id = 1",
        "UPDATE users SET marker = normalized, normalized = marker WHERE id = 1",
    ] {
        virtual_sql(&mut database, &mut transaction, statement).unwrap();
    }
    database.commit_transaction(&mut transaction).unwrap();
    assert_eq!(
        database
            .query("SELECT marker, normalized FROM users WHERE id = 1")
            .unwrap()
            .rows,
        vec![vec![
            ScalarValue::Text("old-normalized".into()),
            ScalarValue::Text("old-marker".into()),
        ]]
    );
    database.close().unwrap();
    std::fs::remove_dir_all(swap_root).unwrap();
}

#[test]
fn virtual_layout_maps_shifted_and_renamed_columns_by_stable_identity() {
    let root = root("identity-layout");
    let mut database = seed(&root);
    let mut transaction = database.begin_transaction().unwrap();
    for statement in [
        "UPDATE users SET legacy = 'updated1' WHERE id = 1",
        "ALTER TABLE users DROP COLUMN id",
        "ALTER TABLE users RENAME COLUMN legacy TO contact",
        "ALTER TABLE users ADD COLUMN marker TEXT",
        "ALTER TABLE users RENAME COLUMN marker TO renamed_marker",
    ] {
        database.execute_in(&mut transaction, statement).unwrap();
    }
    assert_eq!(
        virtual_sql(
            &mut database,
            &mut transaction,
            "UPDATE users SET renamed_marker = contact WHERE contact IS NOT NULL"
        )
        .unwrap()
        .affected_rows,
        2
    );
    assert!(matches!(
        database.execute_in(
            &mut transaction,
            "ALTER TABLE users ADD COLUMN structurally_too_late TEXT"
        ),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::SchemaMutationAfterMaterialization
        ))
    ));
    database.commit_transaction(&mut transaction).unwrap();
    assert_eq!(
        database
            .query("SELECT contact, renamed_marker FROM users WHERE contact = 'updated1'")
            .unwrap()
            .rows,
        vec![vec![
            ScalarValue::Text("updated1".into()),
            ScalarValue::Text("updated1".into()),
        ]]
    );
    database.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn prepared_late_reader_tracks_program_values_but_schema_changes_stale_it() {
    let root = root("prepared");
    let mut database = seed(&root);
    let mut transaction = adopt(&mut database);
    let consumer = relational(
        database
            .prepare_sql_statement_in(
                &transaction,
                "UPDATE users SET normalized = marker WHERE marker IS NOT NULL",
                &[],
            )
            .unwrap(),
    );
    virtual_sql(
        &mut database,
        &mut transaction,
        "UPDATE users SET marker = legacy WHERE marker IS NULL AND legacy IS NOT NULL",
    )
    .unwrap();
    virtual_sql(
        &mut database,
        &mut transaction,
        "UPDATE users SET marker = 'missing' WHERE marker IS NULL",
    )
    .unwrap();
    assert_eq!(
        virtual_prepared(&mut database, &mut transaction, &consumer, &[])
            .unwrap()
            .affected_rows,
        3
    );

    let parameterized = relational(
        database
            .prepare_sql_statement_in(
                &transaction,
                "UPDATE users SET normalized = $1 WHERE marker = $2",
                &[Some(PhysicalType::Text), Some(PhysicalType::Text)],
            )
            .unwrap(),
    );
    let first_index = transaction
        .schema_composition
        .plan()
        .unwrap()
        .deferred_backfill
        .len();
    virtual_prepared(
        &mut database,
        &mut transaction,
        &parameterized,
        &[
            ScalarValue::Text("owned-one".into()),
            ScalarValue::Text("updated1".into()),
        ],
    )
    .unwrap();
    virtual_prepared(
        &mut database,
        &mut transaction,
        &parameterized,
        &[
            ScalarValue::Text("owned-four".into()),
            ScalarValue::Text("inserted4".into()),
        ],
    )
    .unwrap();
    let program = &transaction
        .schema_composition
        .plan()
        .unwrap()
        .deferred_backfill;
    assert_ne!(
        program.semantic_digest(first_index),
        program.semantic_digest(first_index + 1)
    );

    let stale = relational(
        database
            .prepare_sql_statement_in(
                &transaction,
                "UPDATE users SET normalized = marker WHERE marker IS NOT NULL",
                &[],
            )
            .unwrap(),
    );
    database
        .execute_in(
            &mut transaction,
            "ALTER TABLE users ALTER COLUMN marker SET NOT NULL",
        )
        .unwrap();
    assert!(matches!(
        virtual_prepared(&mut database, &mut transaction, &stale, &[]),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::StalePreparedStatement
        ))
    ));
    transaction.rollback().unwrap();
    database.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

fn digest_scenario(name: &str, statements: &[&str]) -> (Vec<[u8; 32]>, [u8; 32]) {
    let root = root(name);
    let mut database = seed(&root);
    let mut transaction = adopt(&mut database);
    for statement in statements {
        virtual_sql(&mut database, &mut transaction, statement).unwrap();
    }
    let plan = transaction.schema_composition.plan().unwrap();
    let semantic = (0..plan.deferred_backfill.len())
        .map(|index| plan.deferred_backfill.semantic_digest(index).unwrap())
        .collect();
    let whole = plan.action_digest();
    transaction.rollback().unwrap();
    database.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
    (semantic, whole)
}

#[test]
fn late_read_digests_bind_column_predicate_prefix_value_and_order() {
    let (marker_read, _) = digest_scenario(
        "digest-marker",
        &["UPDATE users SET normalized = marker WHERE marker IS NULL"],
    );
    let (normalized_read, _) = digest_scenario(
        "digest-normalized",
        &["UPDATE users SET normalized = normalized WHERE marker IS NULL"],
    );
    let (not_null, _) = digest_scenario(
        "digest-not-null",
        &["UPDATE users SET normalized = marker WHERE marker IS NOT NULL"],
    );
    assert_ne!(marker_read[0], normalized_read[0]);
    assert_ne!(marker_read[0], not_null[0]);

    let statements = [
        "UPDATE users SET marker = 'producer-a' WHERE id = 1",
        "UPDATE users SET normalized = marker WHERE marker IS NOT NULL",
    ];
    let (producer_a, whole_a) = digest_scenario("producer-a", &statements);
    let (producer_b, whole_b) = digest_scenario(
        "producer-b",
        &[
            "UPDATE users SET marker = 'producer-b' WHERE id = 1",
            statements[1],
        ],
    );
    assert_eq!(producer_a[1], producer_b[1]);
    assert_ne!(whole_a, whole_b);

    let (_, forward) = digest_scenario("order-forward", &statements);
    let (_, reverse) = digest_scenario("order-reverse", &[statements[1], statements[0]]);
    assert_ne!(forward, reverse);
}

#[test]
fn production_dispatch_keeps_late_rhs_and_predicate_closed() {
    for statement in [
        "UPDATE users SET normalized = marker",
        "UPDATE users SET normalized = 'x' WHERE marker IS NULL",
    ] {
        let root = root("production-negative");
        let mut database = seed(&root);
        let mut transaction = adopt(&mut database);
        let source = transaction
            .schema_composition
            .adopted_source()
            .unwrap()
            .source_storage;
        assert!(matches!(
            database.execute_in(&mut transaction, statement),
            Err(DatabaseError::SchemaMutation(
                SchemaMutationError::MigrationDataAccessAfterRefinement
            ))
        ));
        assert!(
            transaction
                .schema_composition
                .plan()
                .unwrap()
                .deferred_backfill
                .is_empty()
        );
        assert_eq!(database.bindings.resolve_single(TableId(2)), Ok(source));
        transaction.rollback().unwrap();
        database.close().unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn virtual_observation_mismatch_is_corrupt_and_unpublished() {
    let root = root("mismatch");
    let mut database = seed(&root);
    let source = database.bindings.resolve_single(TableId(2)).unwrap();
    let mut transaction = adopt(&mut database);
    virtual_sql(
        &mut database,
        &mut transaction,
        "UPDATE users SET marker = legacy WHERE marker IS NULL AND legacy IS NOT NULL",
    )
    .unwrap();
    transaction
        .schema_composition
        .adopted_source_mut()
        .unwrap()
        .logical
        .deferred_backfill
        .corrupt_expected_digest(0);
    assert!(matches!(
        database.commit_transaction(&mut transaction),
        Err(DatabaseError::SchemaMutation(SchemaMutationError::Corrupt(
            "deferred backfill Execute/finalization mismatch"
        )))
    ));
    assert_eq!(database.bindings.resolve_single(TableId(2)), Ok(source));
    transaction.rollback().unwrap();
    database.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

fn prepare_complete_program(database: &mut Database, transaction: &mut Transaction) {
    for statement in [
        "UPDATE users SET marker = legacy WHERE marker IS NULL AND legacy IS NOT NULL",
        "UPDATE users SET marker = 'missing' WHERE marker IS NULL",
    ] {
        virtual_sql(database, transaction, statement).unwrap();
    }
    database
        .execute_in(
            transaction,
            "ALTER TABLE users ALTER COLUMN marker SET NOT NULL",
        )
        .unwrap();
    virtual_sql(
        database,
        transaction,
        "UPDATE users SET normalized = marker WHERE marker IS NOT NULL",
    )
    .unwrap();
    database
        .execute_in(
            transaction,
            "ALTER TABLE users ALTER COLUMN normalized SET NOT NULL",
        )
        .unwrap();
    database
        .execute_in(
            transaction,
            "CREATE INDEX users_normalized_idx ON users(normalized)",
        )
        .unwrap();
}

fn assert_replacement_blocked(
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

#[test]
fn enabled_stream_still_blocks_virtual_program_replacement() {
    let enabled_root = root("stream-enabled");
    let mut database = seed(&enabled_root);
    let table = TableId(2);
    let source = database.enable_change_stream(table).unwrap().storage_id;
    let storage_floor = database.next_storage_id();
    let mut transaction = adopt(&mut database);
    prepare_complete_program(&mut database, &mut transaction);
    let error = database.commit_transaction(&mut transaction).unwrap_err();
    assert_replacement_blocked(error, table, source, ChangeStreamStatus::Enabled);
    assert_eq!(database.next_storage_id(), storage_floor);
    assert_eq!(database.bindings.resolve_single(table), Ok(source));
    transaction.rollback().unwrap();
    database.close().unwrap();
    std::fs::remove_dir_all(enabled_root).unwrap();
}

#[test]
fn virtual_prefix_replay_cost_curve_is_explicit_and_row_bounded() {
    let root = root("cost");
    let mut database = seed(&root);
    let mut transaction = adopt(&mut database);
    for _ in 0..32 {
        virtual_sql(
            &mut database,
            &mut transaction,
            "UPDATE users SET marker = legacy WHERE id = 999",
        )
        .unwrap();
    }
    let source = [
        ScalarValue::Int64(1),
        ScalarValue::Text("updated1".into()),
        ScalarValue::Bool(true),
    ];
    for rows in [1_000, 10_000, 100_000] {
        for actions in [1, 4, 8, 16, 32] {
            let started = Instant::now();
            let cost = audit_replay_virtual_rows(&transaction, &source, rows, actions).unwrap();
            let elapsed = started.elapsed();
            assert_eq!(cost.rows, rows);
            assert_eq!(cost.actions, actions);
            assert_eq!(cost.action_evaluations, (rows * actions) as u64);
            assert!(cost.resident_metadata_bytes_estimate > 0);
            eprintln!(
                "round53-cost rows={rows} actions={actions} evals={} elapsed_us={} metadata_bytes={}",
                cost.action_evaluations,
                elapsed.as_micros(),
                cost.resident_metadata_bytes_estimate
            );
        }
    }
    transaction.rollback().unwrap();
    database.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn round53_crash_child() {
    let Ok(root) = std::env::var("NETBADB_ROUND53_CRASH_ROOT") else {
        return;
    };
    let root = Path::new(&root);
    let mut database = Database::open_catalog(root.join("catalog")).unwrap();
    let mut transaction = adopt(&mut database);
    prepare_complete_program(&mut database, &mut transaction);
    database.commit_transaction(&mut transaction).unwrap();
    panic!("configured Round 53 crash hook was not reached");
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
fn virtual_program_recovery_uses_cord_and_never_expression_replay() {
    let cases = [
        ("deferred-backfill-accepted", false, false, false),
        ("composition-intent-durable", false, false, false),
        ("source-backfill-intent-durable", false, false, false),
        ("source-backfill-stage-intent-durable", false, false, false),
        ("source-backfill-mid-copy", false, false, false),
        ("source-backfill-final-indexes-built", false, false, false),
        ("after-prepare-1", true, false, false),
        ("after-all-prepares", true, false, false),
        ("after-durable-decision", true, true, false),
        ("after-commit-1", true, true, false),
        ("after-commit-1", true, true, true),
        ("after-all-commits", true, true, false),
    ];
    for (point, coordinator, winner, reverse) in cases {
        let root = root(&format!("crash-{point}-{reverse}"));
        let database = seed(&root);
        let source = database.bindings.resolve_single(TableId(2)).unwrap();
        database.close().unwrap();
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "deferred_virtual_row_audit_tests::round53_crash_child",
                "--nocapture",
            ])
            .env("NETBADB_ROUND53_CRASH_ROOT", &root);
        if reverse {
            command.env("NETBADB_REVERSE_PARTICIPANT_COMMIT", "1");
        }
        if coordinator {
            crate::coordinator_crash::configure_child(&mut command, point, &root, point);
        } else {
            command.env("NETBADB_BACKFILL_CRASH_POINT", point);
        }
        let output = command.output().unwrap();
        assert_eq!(
            output.status.code(),
            Some(if coordinator { 87 } else { 90 }),
            "{point} {reverse}: {} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        for _ in 0..3 {
            let mut reopened = Database::open_catalog(root.join("catalog")).unwrap();
            if winner {
                assert_ne!(
                    reopened.bindings.resolve_single(TableId(2)).unwrap(),
                    source
                );
                assert_eq!(reopened.indexes(TableId(2)).unwrap().len(), 1);
                assert_eq!(
                    reopened
                        .query("SELECT id, marker, normalized FROM users ORDER BY id")
                        .unwrap()
                        .rows,
                    vec![
                        vec![
                            ScalarValue::Int64(1),
                            ScalarValue::Text("updated1".into()),
                            ScalarValue::Text("updated1".into()),
                        ],
                        vec![
                            ScalarValue::Int64(3),
                            ScalarValue::Text("missing".into()),
                            ScalarValue::Text("missing".into()),
                        ],
                        vec![
                            ScalarValue::Int64(4),
                            ScalarValue::Text("inserted4".into()),
                            ScalarValue::Text("inserted4".into()),
                        ],
                    ]
                );
            } else {
                assert_eq!(reopened.bindings.resolve_single(TableId(2)), Ok(source));
                assert!(
                    reopened
                        .schema()
                        .table("users")
                        .unwrap()
                        .column("marker")
                        .is_none()
                );
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
        assert_no_stage(&root);
        std::fs::remove_dir_all(root).unwrap();
    }
}
