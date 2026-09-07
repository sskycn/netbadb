use super::*;
use crate::deferred_backfill::audit_replay_virtual_rows;
use crate::schema_composition::SchemaCompositionState;
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_storage::ChangeStreamStatus;
use netbadb_types::{ColumnId, PhysicalType, ScalarValue, StorageId, TableId};
use std::path::{Path, PathBuf};
use std::time::Instant;

fn root(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "netbadb-round54-{name}-{}-{:?}",
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

fn production_prepared(
    database: &mut Database,
    transaction: &mut Transaction,
    prepared: &PreparedStatement,
    values: &[ScalarValue],
) -> Result<u64, DatabaseError> {
    match database.execute_prepared_in(transaction, prepared, values)? {
        ExecutionResult::AffectedRows(rows) => Ok(rows),
        ExecutionResult::Query(_) => Err(DatabaseError::ExpectedQuery),
    }
}

fn production_sql(
    database: &mut Database,
    transaction: &mut Transaction,
    sql: &str,
) -> Result<u64, DatabaseError> {
    match database.execute_in(transaction, sql)? {
        ExecutionResult::AffectedRows(rows) => Ok(rows),
        ExecutionResult::Query(_) => Err(DatabaseError::ExpectedQuery),
    }
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

    let first = production_sql(
        &mut database,
        &mut transaction,
        "UPDATE users SET marker = legacy WHERE marker IS NULL AND legacy IS NOT NULL",
    )
    .unwrap();
    assert_eq!(first, 2);
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

    let repair = production_sql(
        &mut database,
        &mut transaction,
        "UPDATE users SET marker = 'missing' WHERE marker IS NULL",
    )
    .unwrap();
    assert_eq!(repair, 1);
    database
        .execute_in(
            &mut transaction,
            "ALTER TABLE users ALTER COLUMN marker SET NOT NULL",
        )
        .unwrap();

    let copy = production_sql(
        &mut database,
        &mut transaction,
        "UPDATE users SET normalized = marker WHERE marker IS NOT NULL",
    )
    .unwrap();
    assert_eq!(copy, 3);
    let before_rejected = transaction
        .schema_composition
        .plan()
        .unwrap()
        .deferred_backfill
        .len();
    let evidence_before_rejected = transaction
        .schema_composition
        .plan()
        .unwrap()
        .action_evidence
        .clone();
    let digest_before_rejected = transaction
        .schema_composition
        .plan()
        .unwrap()
        .action_digest();
    let nullable_write = relational(
        database
            .prepare_sql_statement_in(
                &transaction,
                "UPDATE users SET marker = $1 WHERE id = 1",
                &[Some(PhysicalType::Text)],
            )
            .unwrap(),
    );
    let not_null_error = production_prepared(
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
    assert_eq!(
        transaction
            .schema_composition
            .plan()
            .unwrap()
            .action_evidence,
        evidence_before_rejected
    );
    assert_eq!(
        transaction
            .schema_composition
            .plan()
            .unwrap()
            .action_digest(),
        digest_before_rejected
    );
    assert!(matches!(
        transaction.schema_composition,
        SchemaCompositionState::AdoptedSourceBackfilling(_)
    ));
    assert_eq!(database.next_storage_id().unwrap(), next_storage);
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
    let zero = production_sql(
        &mut database,
        &mut transaction,
        "UPDATE users SET normalized = 'never' WHERE marker = 'absent'",
    )
    .unwrap();
    assert_eq!(zero, 0);
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
            &ScalarValue::Text("updated1".into()),
            &[1],
        );
        assert_physical_index_key(
            &mut database,
            target,
            ColumnId(5),
            &ScalarValue::Text("missing".into()),
            &[3],
        );
        assert_physical_index_key(
            &mut database,
            target,
            ColumnId(5),
            &ScalarValue::Text("inserted4".into()),
            &[4],
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
        production_sql(
            &mut database,
            &mut transaction,
            "UPDATE users SET marker = 'initial' WHERE marker IS NULL"
        )
        .unwrap(),
        3
    );
    production_sql(
        &mut database,
        &mut transaction,
        "UPDATE users SET normalized = 'old-normalized' WHERE id = 1",
    )
    .unwrap();
    production_sql(
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
        production_sql(&mut database, &mut transaction, statement).unwrap();
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
        production_sql(
            &mut database,
            &mut transaction,
            "UPDATE users SET renamed_marker = contact WHERE contact IS NOT NULL"
        )
        .unwrap(),
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
fn deferred_execute_reads_authoritative_s1_not_a_columnar_projection() {
    let root = root("columnar-authority");
    let mut database = seed(&root);
    let projection = database
        .build_columnar_projection(ColumnarProjectionSpec::new(
            TableId(2),
            root.join("projection"),
            vec![ColumnId(1), ColumnId(2)],
        ))
        .unwrap();
    assert_eq!(
        database
            .inspect_columnar_projections()
            .into_iter()
            .find(|entry| entry.projection_id == Some(projection))
            .unwrap()
            .health,
        ColumnarProjectionHealth::Fresh
    );

    let mut transaction = adopt(&mut database);
    assert_eq!(
        production_sql(
            &mut database,
            &mut transaction,
            "UPDATE users SET marker = legacy WHERE marker IS NULL AND legacy IS NOT NULL"
        )
        .unwrap(),
        2
    );
    database.commit_transaction(&mut transaction).unwrap();
    assert_eq!(
        database
            .query("SELECT id, marker FROM users ORDER BY id")
            .unwrap()
            .rows,
        vec![
            vec![ScalarValue::Int64(1), ScalarValue::Text("updated1".into())],
            vec![ScalarValue::Int64(3), ScalarValue::Null],
            vec![ScalarValue::Int64(4), ScalarValue::Text("inserted4".into())],
        ]
    );
    assert_ne!(
        database
            .inspect_columnar_projections()
            .into_iter()
            .find(|entry| entry.projection_id == Some(projection))
            .unwrap()
            .health,
        ColumnarProjectionHealth::Fresh
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
    production_sql(
        &mut database,
        &mut transaction,
        "UPDATE users SET marker = legacy WHERE marker IS NULL AND legacy IS NOT NULL",
    )
    .unwrap();
    production_sql(
        &mut database,
        &mut transaction,
        "UPDATE users SET marker = 'missing' WHERE marker IS NULL",
    )
    .unwrap();
    assert_eq!(
        production_prepared(&mut database, &mut transaction, &consumer, &[]).unwrap(),
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
    production_prepared(
        &mut database,
        &mut transaction,
        &parameterized,
        &[
            ScalarValue::Text("owned-one".into()),
            ScalarValue::Text("updated1".into()),
        ],
    )
    .unwrap();
    production_prepared(
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
        production_prepared(&mut database, &mut transaction, &stale, &[]),
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
        production_sql(&mut database, &mut transaction, statement).unwrap();
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

fn digest_hex(digest: [u8; 32]) -> String {
    digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>()
}

#[test]
fn production_late_read_action_has_stable_v1_golden_digest() {
    let (digests, _) = digest_scenario(
        "late-read-golden",
        &["UPDATE users SET normalized = marker WHERE marker IS NOT NULL"],
    );
    assert_eq!(
        digest_hex(digests[0]),
        "ba8201334bcc39194e0ca6e2136626f1e0f16059fc00421c297f07af1d65fdef"
    );
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
    let (marker_target, _) = digest_scenario(
        "digest-marker-target",
        &["UPDATE users SET marker = marker WHERE marker IS NULL"],
    );
    let (not_null, _) = digest_scenario(
        "digest-not-null",
        &["UPDATE users SET normalized = marker WHERE marker IS NOT NULL"],
    );
    assert_ne!(marker_read[0], normalized_read[0]);
    assert_ne!(marker_read[0], marker_target[0]);
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

fn materialized_program_digests(name: &str, producer: &str) -> ([u8; 32], [u8; 32]) {
    let root = root(name);
    let mut database = seed(&root);
    let mut transaction = adopt(&mut database);
    production_sql(&mut database, &mut transaction, producer).unwrap();
    production_sql(
        &mut database,
        &mut transaction,
        "UPDATE users SET normalized = marker WHERE marker IS NOT NULL",
    )
    .unwrap();
    database.finalize_adopted_source(&mut transaction).unwrap();
    let tag25_action_digest = transaction
        .schema_composition
        .materialized_index()
        .unwrap()
        .intent
        .action_digest;
    let tag35_clone_plan_digest = database
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
    std::fs::remove_dir_all(root).unwrap();
    (tag25_action_digest, tag35_clone_plan_digest)
}

#[test]
fn producer_values_bind_whole_program_tag25_and_tag35_evidence() {
    let first = materialized_program_digests(
        "whole-program-a",
        "UPDATE users SET marker = 'a' WHERE id = 1",
    );
    let second = materialized_program_digests(
        "whole-program-b",
        "UPDATE users SET marker = 'b' WHERE id = 1",
    );
    assert_ne!(first.0, second.0);
    assert_ne!(first.1, second.1);
}

#[test]
fn production_dispatch_keeps_base_writes_and_relational_access_closed() {
    for statement in [
        "UPDATE users SET legacy = marker",
        "UPDATE users SET legacy = marker, normalized = marker",
        "SELECT marker FROM users",
        "INSERT INTO users (id, legacy, flag, marker, normalized) VALUES (9, 'x', true, 'm', 'n')",
        "DELETE FROM users WHERE marker IS NULL",
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
fn first_zero_row_action_is_accepted_and_burned_dropped_identity_is_not_readable() {
    let root = root("zero-and-dropped");
    let mut database = seed(&root);
    let mut transaction = database.begin_transaction().unwrap();
    database
        .execute_in(
            &mut transaction,
            "UPDATE users SET legacy = legacy WHERE id = 1",
        )
        .unwrap();
    database
        .execute_in(&mut transaction, "ALTER TABLE users ADD COLUMN tmp TEXT")
        .unwrap();
    let dropped_reader = relational(
        database
            .prepare_sql_statement_in(
                &transaction,
                "UPDATE users SET tmp = tmp WHERE tmp IS NULL",
                &[],
            )
            .unwrap(),
    );
    database
        .execute_in(&mut transaction, "ALTER TABLE users DROP COLUMN tmp")
        .unwrap();
    database
        .execute_in(&mut transaction, "ALTER TABLE users ADD COLUMN marker TEXT")
        .unwrap();
    assert!(matches!(
        production_prepared(&mut database, &mut transaction, &dropped_reader, &[]),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::StalePreparedStatement
        ))
    ));
    assert_eq!(
        production_sql(
            &mut database,
            &mut transaction,
            "UPDATE users SET marker = 'never' WHERE legacy = 'absent'"
        )
        .unwrap(),
        0
    );
    assert!(matches!(
        transaction.schema_composition,
        SchemaCompositionState::AdoptedSourceBackfilling(_)
    ));
    assert_eq!(
        transaction
            .schema_composition
            .plan()
            .unwrap()
            .deferred_backfill
            .len(),
        1
    );
    transaction.rollback().unwrap();
    database.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn virtual_observation_mismatch_is_corrupt_and_unpublished() {
    let root = root("mismatch");
    let mut database = seed(&root);
    let source = database.bindings.resolve_single(TableId(2)).unwrap();
    let mut transaction = adopt(&mut database);
    production_sql(
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

#[test]
fn production_virtual_row_rollback_matrix_restores_s1() {
    for stage in 0..7 {
        let root = root(&format!("rollback-{stage}"));
        let mut database = seed(&root);
        let source = database.bindings.resolve_single(TableId(2)).unwrap();
        let base_generation = database.schema_generation();
        let base_version = database.table_schema_version(TableId(2)).unwrap();
        let storage_floor = database.next_storage_id().unwrap();
        let source_index_digest = crate::schema_mutation_journal::heap_rewrite_indexes_digest(
            &database
                .registry
                .get_mut(source)
                .unwrap()
                .heap_rewrite_indexes()
                .unwrap(),
        )
        .unwrap();
        let mut transaction = adopt(&mut database);
        production_sql(
            &mut database,
            &mut transaction,
            "UPDATE users SET marker = legacy WHERE marker IS NULL AND legacy IS NOT NULL",
        )
        .unwrap();

        if stage == 3 {
            assert!(matches!(
                database.execute_in(
                    &mut transaction,
                    "ALTER TABLE users ALTER COLUMN marker SET NOT NULL"
                ),
                Err(DatabaseError::SchemaMutation(
                    SchemaMutationError::NotNullViolation(ColumnId(4))
                ))
            ));
        } else if stage >= 1 {
            production_sql(
                &mut database,
                &mut transaction,
                "UPDATE users SET marker = 'missing' WHERE marker IS NULL",
            )
            .unwrap();
            if stage >= 2 {
                production_sql(
                    &mut database,
                    &mut transaction,
                    "UPDATE users SET normalized = marker WHERE marker IS NOT NULL",
                )
                .unwrap();
            }
            if stage >= 4 {
                for statement in [
                    "ALTER TABLE users ALTER COLUMN marker SET NOT NULL",
                    "ALTER TABLE users ALTER COLUMN normalized SET NOT NULL",
                ] {
                    database.execute_in(&mut transaction, statement).unwrap();
                }
            }
            if stage >= 5 {
                database
                    .execute_in(
                        &mut transaction,
                        "CREATE INDEX users_normalized_idx ON users(normalized)",
                    )
                    .unwrap();
            }
        }
        let staged_target = if stage == 6 {
            database.finalize_adopted_source(&mut transaction).unwrap();
            let materialized = transaction.schema_composition.materialized_index().unwrap();
            let crate::schema_mutation_journal::SchemaIndexTablePlan::RewriteHeap {
                replacement,
                ..
            } = &materialized.intent.tables[0]
            else {
                panic!("expected staged replacement")
            };
            Some(replacement.new_storage())
        } else {
            None
        };

        transaction.rollback().unwrap();
        assert_eq!(database.schema_generation(), base_generation);
        assert_eq!(
            database.table_schema_version(TableId(2)),
            Some(base_version)
        );
        assert_eq!(database.bindings.resolve_single(TableId(2)), Ok(source));
        assert!(
            database
                .schema()
                .table("users")
                .unwrap()
                .column("marker")
                .is_none()
        );
        assert_eq!(database.next_column_id(TableId(2)), Some(ColumnId(6)));
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
        assert_eq!(
            database.next_storage_id(),
            Some(StorageId(storage_floor.0 + u64::from(stage == 6)))
        );
        if let Some(target) = staged_target {
            assert!(database.registry.get_mut(target).is_none());
        }
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
        std::fs::remove_dir_all(root).unwrap();
    }
}

fn prepare_complete_program(database: &mut Database, transaction: &mut Transaction) {
    for statement in [
        "UPDATE users SET marker = legacy WHERE marker IS NULL AND legacy IS NOT NULL",
        "UPDATE users SET marker = 'missing' WHERE marker IS NULL",
    ] {
        production_sql(database, transaction, statement).unwrap();
    }
    database
        .execute_in(
            transaction,
            "ALTER TABLE users ALTER COLUMN marker SET NOT NULL",
        )
        .unwrap();
    production_sql(
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
    let cursor = database.enable_change_stream(table).unwrap();
    let source = cursor.storage_id;
    let storage_floor = database.next_storage_id();
    let mut transaction = adopt(&mut database);
    prepare_complete_program(&mut database, &mut transaction);
    assert_eq!(
        transaction
            .schema_composition
            .plan()
            .unwrap()
            .deferred_backfill
            .len(),
        3
    );
    assert_eq!(
        database
            .inspect_change_stream(table)
            .unwrap()
            .current_data_version,
        cursor.frontier
    );
    let error = database.commit_transaction(&mut transaction).unwrap_err();
    assert_replacement_blocked(error, table, source, ChangeStreamStatus::Enabled);
    assert_eq!(database.next_storage_id(), storage_floor);
    assert_eq!(database.bindings.resolve_single(table), Ok(source));
    transaction.rollback().unwrap();
    assert_eq!(
        database
            .inspect_change_stream(table)
            .unwrap()
            .current_data_version,
        cursor.frontier
    );
    database.close().unwrap();
    std::fs::remove_dir_all(enabled_root).unwrap();
}

#[test]
fn unavailable_stream_accepts_logical_late_reads_but_blocks_replacement() {
    let root = root("stream-unavailable");
    let mut database = seed(&root);
    let table = TableId(2);
    let source = database.enable_change_stream(table).unwrap().storage_id;
    database.close().unwrap();
    let snapshot = crate::schema_catalog_file::load(&root.join("catalog")).unwrap();
    let locator = snapshot
        .storages
        .iter()
        .find(|entry| entry.id == source)
        .unwrap()
        .locator
        .clone();
    let heap = crate::schema_catalog_file::resolve(&root.join("catalog"), &locator);
    std::fs::remove_file(netbadb_storage::heap_change_log_path(&heap)).unwrap();

    let mut database = Database::open_catalog(root.join("catalog")).unwrap();
    assert_eq!(
        database.inspect_change_stream(table).unwrap().status,
        ChangeStreamStatus::Unavailable
    );
    let storage_floor = database.next_storage_id();
    let mut transaction = database.begin_transaction().unwrap();
    for statement in [
        "UPDATE users SET legacy = legacy WHERE id = 999",
        "ALTER TABLE users ADD COLUMN marker TEXT",
        "ALTER TABLE users ADD COLUMN normalized TEXT",
    ] {
        database.execute_in(&mut transaction, statement).unwrap();
    }
    assert!(matches!(
        transaction.schema_composition,
        SchemaCompositionState::AdoptedSourceRefining(_)
    ));
    prepare_complete_program(&mut database, &mut transaction);
    let error = database.commit_transaction(&mut transaction).unwrap_err();
    assert_replacement_blocked(error, table, source, ChangeStreamStatus::Unavailable);
    assert_eq!(database.next_storage_id(), storage_floor);
    assert_eq!(database.bindings.resolve_single(table), Ok(source));
    transaction.rollback().unwrap();
    database.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn explicitly_disabled_stream_allows_late_read_rebaseline_flow() {
    let root = root("stream-disabled");
    let mut database = seed(&root);
    let table = TableId(2);
    let source = database.enable_change_stream(table).unwrap().storage_id;
    database.disable_change_stream(table).unwrap();
    let mut transaction = adopt(&mut database);
    prepare_complete_program(&mut database, &mut transaction);
    database.commit_transaction(&mut transaction).unwrap();
    let replacement = database.bindings.resolve_single(table).unwrap();
    assert_ne!(replacement, source);
    let cursor = database.enable_change_stream(table).unwrap();
    assert_eq!(cursor.storage_id, replacement);
    assert_eq!(
        cursor,
        database.committed_read_anchor(table).unwrap().cursor
    );
    database.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn virtual_prefix_replay_cost_curve_is_explicit_and_row_bounded() {
    let root = root("cost");
    let mut database = seed(&root);
    let mut transaction = adopt(&mut database);
    for _ in 0..32 {
        production_sql(
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
                "round54-cost rows={rows} actions={actions} evals={} elapsed_us={} metadata_bytes={}",
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
fn round54_crash_child() {
    let Ok(root) = std::env::var("NETBADB_ROUND54_CRASH_ROOT") else {
        return;
    };
    let root = Path::new(&root);
    let mut database = Database::open_catalog(root.join("catalog")).unwrap();
    let mut transaction = adopt(&mut database);
    prepare_complete_program(&mut database, &mut transaction);
    database.commit_transaction(&mut transaction).unwrap();
    panic!("configured Round 54 crash hook was not reached");
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
                "deferred_virtual_row_tests::round54_crash_child",
                "--nocapture",
            ])
            .env("NETBADB_ROUND54_CRASH_ROOT", &root);
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
