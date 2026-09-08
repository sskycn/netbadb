//! Round 59 executable architecture probe.
//!
//! Production SQL/HIR remains closed. Tests replace the RHS of an otherwise
//! normally compiled UPDATE with manually constructed typed relational IR and
//! opt into the non-default executor conversion semantics.

use super::*;
use crate::deferred_backfill::try_execute_adopted_update_for_round59_audit;
use crate::schema_composition::SchemaCompositionState;
use crate::schema_mutation_journal::{SchemaIndexTablePlan, heap_rewrite_indexes_digest};
use netbadb_rel::{ColumnRef, Expr, ExprKind, LogicalPlan, LogicalStatement};
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_types::{
    ColumnId, ExprType, IndexId, PhysicalType, ScalarValue, SemanticType, StorageId, TableId,
};
use std::path::{Path, PathBuf};

const USERS: TableId = TableId(2);
const ID: ColumnId = ColumnId(1);
const OLD_LEGACY: ColumnId = ColumnId(2);
const SHADOW: ColumnId = ColumnId(4);

fn root(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "netbadb-round59-{name}-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_dir_all(&path);
    std::fs::create_dir_all(&path).unwrap();
    path
}

fn seed(path: &Path, global: bool, invalid: &str) -> Database {
    let mut coordinator = DatabaseCoordinatorConfig::new(path.join("coordinator"));
    if global {
        coordinator = coordinator.with_global_visibility();
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
        Some(coordinator),
    )
    .unwrap();
    database
        .execute("CREATE TABLE users (id BIGINT NOT NULL, legacy TEXT NOT NULL, flag BOOLEAN)")
        .unwrap();
    for statement in [
        "INSERT INTO users VALUES (1, '42', true)".to_owned(),
        "INSERT INTO users VALUES (2, '-7', false)".to_owned(),
        format!("INSERT INTO users VALUES (3, '{invalid}', NULL)"),
        "CREATE INDEX users_legacy_idx ON users(legacy)".to_owned(),
    ] {
        database.execute(&statement).unwrap();
    }
    database
}

fn adopt(database: &mut Database) -> Transaction {
    let mut transaction = database.begin_transaction().unwrap();
    for statement in [
        "UPDATE users SET legacy = '43' WHERE id = 1",
        "INSERT INTO users VALUES (4, '99', true)",
        "DELETE FROM users WHERE id = 2",
        "ALTER TABLE users ADD COLUMN shadow BIGINT",
    ] {
        database.execute_in(&mut transaction, statement).unwrap();
    }
    transaction
}

fn source_column(plan: &LogicalPlan, column_id: ColumnId) -> ColumnRef {
    match plan {
        LogicalPlan::Scan { columns, .. } => columns
            .iter()
            .find(|column| column.column_id == column_id)
            .cloned()
            .unwrap(),
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::ScalarProject { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Limit { input, .. } => source_column(input, column_id),
        LogicalPlan::Join { .. } | LogicalPlan::OneRow => panic!("expected one-table UPDATE"),
    }
}

fn audit_update(
    database: &Database,
    transaction: &Transaction,
    template: &str,
    source: ColumnId,
    target: PhysicalType,
) -> LogicalStatement {
    let PreparedSqlStatement::Relational(prepared) = database
        .prepare_sql_statement_in(transaction, template, &[])
        .unwrap()
    else {
        panic!("expected relational UPDATE")
    };
    let mut statement = prepared.compiled.logical_statement.clone();
    let LogicalStatement::Update {
        input, assignments, ..
    } = &mut statement
    else {
        panic!("expected UPDATE")
    };
    let child = source_column(input, source);
    assignments[0].value = Expr {
        kind: ExprKind::Cast {
            expression: Box::new(Expr {
                kind: ExprKind::Column(child.clone()),
                expr_type: ExprType {
                    data_type: child.data_type,
                    nullable: child.nullable,
                },
            }),
        },
        expr_type: ExprType {
            data_type: SemanticType::physical(target),
            nullable: child.nullable,
        },
    };
    statement
}

fn convert_legacy(
    database: &mut Database,
    transaction: &mut Transaction,
) -> Result<u64, DatabaseError> {
    let statement = audit_update(
        database,
        transaction,
        "UPDATE users SET shadow = 0 WHERE shadow IS NULL AND legacy IS NOT NULL",
        OLD_LEGACY,
        PhysicalType::Int64,
    );
    try_execute_adopted_update_for_round59_audit(database, transaction, &statement)?
        .ok_or(SchemaMutationError::Corrupt("Round 59 audit UPDATE was not accepted").into())
}

fn source_index_digest(database: &mut Database, storage: StorageId) -> [u8; 32] {
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

fn finish_swap(database: &mut Database, transaction: &mut Transaction) -> IndexId {
    for statement in [
        "DROP INDEX users_legacy_idx",
        "ALTER TABLE users DROP COLUMN legacy",
        "ALTER TABLE users RENAME COLUMN shadow TO legacy",
        "CREATE INDEX users_legacy_idx ON users(legacy)",
    ] {
        database.execute_in(transaction, statement).unwrap();
    }
    database
        .index_name_bindings(Some(transaction))
        .into_iter()
        .find(|binding| binding.name.as_str() == "users_legacy_idx")
        .unwrap()
        .target
        .index_id
}

fn assert_int64_lookup(
    database: &mut Database,
    storage: StorageId,
    index: IndexId,
    key: i64,
    id: i64,
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
        .point_lookup_columns_with_view(access_path, &ScalarValue::Int64(key), &[ID], &view)
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1, vec![ScalarValue::Int64(id)]);
}

#[test]
fn text_to_bigint_shadow_conversion_repairs_then_publishes_one_indexed_s2() {
    let path = root("full");
    let mut database = seed(&path, true, "bad");
    let source = database.bindings.resolve_single(USERS).unwrap();
    let old_index = database.indexes(USERS).unwrap()[0].clone();
    assert_eq!(
        (old_index.id, old_index.column_id),
        (IndexId(1), OLD_LEGACY)
    );
    let old_digest = source_index_digest(&mut database, source);
    let target = database.next_storage_id().unwrap();
    let before_global = database.current_database_snapshot().unwrap().unwrap();
    let mut transaction = adopt(&mut database);
    let prepared_old = match database
        .prepare_sql_statement_in(&transaction, "SELECT legacy FROM users", &[])
        .unwrap()
    {
        PreparedSqlStatement::Relational(prepared) => prepared,
        PreparedSqlStatement::Ddl(_) => panic!("expected SELECT"),
    };
    let plan = transaction.schema_composition.plan().unwrap();
    let actions_before = plan.action_count();
    let evidence_before = plan.action_evidence.len();
    assert_eq!(plan.deferred_backfill.len(), 0);

    assert!(matches!(
        convert_legacy(&mut database, &mut transaction),
        Err(DatabaseError::Execution(
            netbadb_executor::ExecutionError::InvalidCastText {
                target: PhysicalType::Int64
            }
        ))
    ));
    let plan = transaction.schema_composition.plan().unwrap();
    assert_eq!(plan.action_count(), actions_before);
    assert_eq!(plan.action_evidence.len(), evidence_before);
    assert_eq!(plan.deferred_backfill.len(), 0);
    assert!(!plan.deferred_backfill.has_evaluation_schema());
    assert!(matches!(
        transaction.schema_composition,
        SchemaCompositionState::AdoptedSourceRefining(_)
    ));
    assert_eq!(database.bindings.resolve_single(USERS), Ok(source));
    assert_eq!(database.next_storage_id(), Some(target));
    assert_eq!(source_index_digest(&mut database, source), old_digest);
    let source_table = database.registry.get_mut(source).unwrap().table();
    assert_eq!(
        source_table
            .column_by_id(OLD_LEGACY)
            .unwrap()
            .semantic_type()
            .physical,
        PhysicalType::Text
    );
    assert!(source_table.column_by_id(SHADOW).is_none());

    assert_eq!(
        database
            .execute_in(
                &mut transaction,
                "UPDATE users SET shadow = 0 WHERE legacy = 'bad' AND shadow IS NULL",
            )
            .unwrap(),
        ExecutionResult::AffectedRows(1)
    );
    assert_eq!(convert_legacy(&mut database, &mut transaction).unwrap(), 2);
    let plan = transaction.schema_composition.plan().unwrap();
    assert_eq!(plan.deferred_backfill.len(), 2);
    assert!(plan.deferred_backfill.has_evaluation_schema());
    database
        .execute_in(
            &mut transaction,
            "ALTER TABLE users ALTER COLUMN shadow SET NOT NULL",
        )
        .unwrap();
    let new_index = finish_swap(&mut database, &mut transaction);
    assert_eq!((new_index, SHADOW), (IndexId(2), SHADOW));
    assert_eq!(source_index_digest(&mut database, source), old_digest);
    assert_eq!(database.next_storage_id(), Some(target));

    database.finalize_adopted_source(&mut transaction).unwrap();
    let materialized = transaction.schema_composition.materialized_index().unwrap();
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
        panic!("expected one Heap replacement")
    };
    assert_eq!(replacement.new_storage(), target);
    assert_eq!(base_indexes.active[0].id, old_index.id);
    assert_eq!(
        (
            final_indexes.active[0].id,
            final_indexes.active[0].column_id
        ),
        (new_index, SHADOW)
    );
    database.commit_transaction(&mut transaction).unwrap();

    assert!(matches!(
        database.execute_prepared(&prepared_old, &[]),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::StalePreparedStatement
        ))
    ));
    let after_global = database.current_database_snapshot().unwrap().unwrap();
    assert_eq!(
        after_global.commit_seq().0,
        before_global.commit_seq().0 + 1
    );
    assert_eq!(database.bindings.resolve_single(USERS), Ok(target));
    assert_eq!(database.next_storage_id(), Some(StorageId(target.0 + 1)));
    assert_eq!(
        database
            .query("SELECT id, legacy FROM users ORDER BY id")
            .unwrap()
            .rows,
        vec![
            vec![ScalarValue::Int64(1), ScalarValue::Int64(43)],
            vec![ScalarValue::Int64(3), ScalarValue::Int64(0)],
            vec![ScalarValue::Int64(4), ScalarValue::Int64(99)],
        ]
    );
    let final_table = database.schema().table("users").unwrap();
    assert!(final_table.column_by_id(OLD_LEGACY).is_none());
    let final_column = final_table.column("legacy").unwrap();
    assert_eq!(
        (final_column.id, final_column.semantic_type().physical),
        (SHADOW, PhysicalType::Int64)
    );
    for (key, id) in [(43, 1), (0, 3), (99, 4)] {
        assert_int64_lookup(&mut database, target, new_index, key, id);
    }
    for _ in 0..3 {
        database.close().unwrap();
        database = Database::open_catalog(path.join("catalog")).unwrap();
        assert_eq!(database.bindings.resolve_single(USERS), Ok(target));
        assert_int64_lookup(&mut database, target, new_index, 43, 1);
    }
    database.close().unwrap();
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn failed_conversion_after_prefix_keeps_program_and_range_error_distinct() {
    let path = root("prefix-failure");
    let mut database = seed(&path, false, "9223372036854775808");
    let mut transaction = adopt(&mut database);
    assert_eq!(
        database
            .execute_in(&mut transaction, "UPDATE users SET shadow = 1 WHERE id = 1")
            .unwrap(),
        ExecutionResult::AffectedRows(1)
    );
    let before = transaction.schema_composition.plan().unwrap();
    let actions = before.action_count();
    let evidence = before.action_evidence.clone();
    assert_eq!(before.deferred_backfill.len(), 1);
    assert!(matches!(
        convert_legacy(&mut database, &mut transaction),
        Err(DatabaseError::Execution(
            netbadb_executor::ExecutionError::CastOutOfRange {
                source: PhysicalType::Text,
                target: PhysicalType::Int64
            }
        ))
    ));
    let after = transaction.schema_composition.plan().unwrap();
    assert_eq!(after.action_count(), actions);
    assert_eq!(after.action_evidence, evidence);
    assert_eq!(after.deferred_backfill.len(), 1);
    assert!(matches!(
        transaction.schema_composition,
        SchemaCompositionState::AdoptedSourceBackfilling(_)
    ));
    transaction.rollback().unwrap();
    database.close().unwrap();
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn converted_result_observation_mismatch_blocks_publication() {
    let path = root("observation-corruption");
    let mut database = seed(&path, false, "8");
    let source = database.bindings.resolve_single(USERS).unwrap();
    let target = database.next_storage_id().unwrap();
    let mut transaction = adopt(&mut database);
    assert_eq!(convert_legacy(&mut database, &mut transaction).unwrap(), 3);
    transaction
        .schema_composition
        .adopted_source_mut()
        .unwrap()
        .logical
        .deferred_backfill
        .corrupt_expected_digest(0);
    database
        .execute_in(
            &mut transaction,
            "ALTER TABLE users ALTER COLUMN shadow SET NOT NULL",
        )
        .unwrap();
    finish_swap(&mut database, &mut transaction);
    assert!(matches!(
        database.finalize_adopted_source(&mut transaction),
        Err(DatabaseError::SchemaMutation(SchemaMutationError::Corrupt(
            "deferred backfill Execute/finalization mismatch"
        )))
    ));
    assert_eq!(database.bindings.resolve_single(USERS), Ok(source));
    assert_eq!(database.next_storage_id(), Some(StorageId(target.0 + 1)));
    transaction.rollback().unwrap();
    database.close().unwrap();
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn later_conversion_action_reads_prior_converted_virtual_value() {
    let path = root("ordered-prefix");
    let mut database = seed(&path, false, "8");
    let mut transaction = adopt(&mut database);
    database
        .execute_in(&mut transaction, "ALTER TABLE users ADD COLUMN echo TEXT")
        .unwrap();
    assert_eq!(convert_legacy(&mut database, &mut transaction).unwrap(), 3);
    let echo = transaction
        .visible_schema(database.schema())
        .table("users")
        .unwrap()
        .column("echo")
        .unwrap()
        .id;
    let statement = audit_update(
        &database,
        &transaction,
        "UPDATE users SET echo = '' WHERE echo IS NULL AND shadow IS NOT NULL",
        SHADOW,
        PhysicalType::Text,
    );
    assert_eq!(
        try_execute_adopted_update_for_round59_audit(&mut database, &mut transaction, &statement,)
            .unwrap(),
        Some(3)
    );
    assert_eq!(
        transaction
            .schema_composition
            .plan()
            .unwrap()
            .deferred_backfill
            .len(),
        2
    );
    assert_ne!(echo, SHADOW);
    transaction.rollback().unwrap();
    database.close().unwrap();
    std::fs::remove_dir_all(path).unwrap();
}

fn digest_case(
    path: &Path,
    target_sql: &str,
    target: PhysicalType,
    source: ColumnId,
    predicate_ids: &[i64],
) -> (Vec<[u8; 32]>, [u8; 32]) {
    let mut database = Database::create_catalog(
        path.join("catalog"),
        vec![TableStorageCreateSpec::heap(
            path.join("seed.heap"),
            TableDef::new(
                TableId(1),
                "seed",
                vec![ColumnDef::new(
                    ID,
                    "id",
                    TypeSpec::Physical(PhysicalType::Int64),
                )],
            ),
        )],
        Some(DatabaseCoordinatorConfig::new(path.join("coordinator"))),
    )
    .unwrap();
    database
        .execute("CREATE TABLE sources (id BIGINT, source_a TEXT, source_b TEXT)")
        .unwrap();
    database
        .execute("INSERT INTO sources VALUES (1, '1', '1')")
        .unwrap();
    let mut transaction = database.begin_transaction().unwrap();
    database
        .execute_in(
            &mut transaction,
            "UPDATE sources SET source_a = source_a WHERE id = 999",
        )
        .unwrap();
    database
        .execute_in(
            &mut transaction,
            &format!("ALTER TABLE sources ADD COLUMN shadow {target_sql}"),
        )
        .unwrap();
    for predicate_id in predicate_ids {
        let template = format!(
            "UPDATE sources SET shadow = {} WHERE id = {predicate_id}",
            if target == PhysicalType::Bool {
                "false"
            } else {
                "0"
            }
        );
        let statement = audit_update(&database, &transaction, &template, source, target);
        assert_eq!(
            try_execute_adopted_update_for_round59_audit(
                &mut database,
                &mut transaction,
                &statement,
            )
            .unwrap(),
            Some(0)
        );
    }
    let plan = transaction.schema_composition.plan().unwrap();
    let semantic = (0..plan.deferred_backfill.len())
        .map(|index| plan.deferred_backfill.semantic_digest(index).unwrap())
        .collect();
    let whole = plan.action_digest();
    transaction.rollback().unwrap();
    database.close().unwrap();
    (semantic, whole)
}

#[test]
fn conversion_target_source_literal_and_action_order_change_digests() {
    let cases = [
        (
            "int",
            "BIGINT",
            PhysicalType::Int64,
            OLD_LEGACY,
            vec![998, 999],
        ),
        (
            "bool",
            "BOOLEAN",
            PhysicalType::Bool,
            OLD_LEGACY,
            vec![998, 999],
        ),
        (
            "source",
            "BIGINT",
            PhysicalType::Int64,
            ColumnId(3),
            vec![998, 999],
        ),
        (
            "literal",
            "BIGINT",
            PhysicalType::Int64,
            OLD_LEGACY,
            vec![997, 999],
        ),
        (
            "order",
            "BIGINT",
            PhysicalType::Int64,
            OLD_LEGACY,
            vec![999, 998],
        ),
    ];
    let mut results = Vec::new();
    for (name, sql, target, source, predicates) in cases {
        let path = root(&format!("digest-{name}"));
        results.push(digest_case(&path, sql, target, source, &predicates));
        std::fs::remove_dir_all(path).unwrap();
    }
    let baseline = &results[0];
    assert_ne!(baseline.0[0], results[1].0[0]);
    assert_ne!(baseline.0[0], results[2].0[0]);
    assert_ne!(baseline.0[0], results[3].0[0]);
    assert_ne!(baseline.1, results[4].1);
}

#[test]
fn round59_conversion_crash_child() {
    let Ok(path) = std::env::var("NETBADB_ROUND59_CRASH_ROOT") else {
        return;
    };
    let mut database = Database::open_catalog(Path::new(&path).join("catalog")).unwrap();
    let mut transaction = adopt(&mut database);
    database
        .execute_in(
            &mut transaction,
            "UPDATE users SET shadow = 0 WHERE legacy = 'bad' AND shadow IS NULL",
        )
        .unwrap();
    assert_eq!(convert_legacy(&mut database, &mut transaction).unwrap(), 2);
    database
        .execute_in(
            &mut transaction,
            "ALTER TABLE users ALTER COLUMN shadow SET NOT NULL",
        )
        .unwrap();
    finish_swap(&mut database, &mut transaction);
    database.commit_transaction(&mut transaction).unwrap();
    panic!("configured Round 59 crash point was not reached");
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

fn assert_recovered_conversion(database: &mut Database, source: StorageId, winner: bool) {
    let active = database.bindings.resolve_single(USERS).unwrap();
    let table = database.schema().table("users").unwrap();
    let indexes = database.indexes(USERS).unwrap();
    assert_eq!(indexes.len(), 1);
    if winner {
        assert_ne!(active, source);
        assert!(table.column_by_id(OLD_LEGACY).is_none());
        assert_eq!(
            (
                table.column("legacy").unwrap().id,
                table.column("legacy").unwrap().semantic_type().physical,
            ),
            (SHADOW, PhysicalType::Int64)
        );
        assert_eq!((indexes[0].id, indexes[0].column_id), (IndexId(2), SHADOW));
        assert_eq!(
            database
                .query("SELECT id, legacy FROM users ORDER BY id")
                .unwrap()
                .rows,
            vec![
                vec![ScalarValue::Int64(1), ScalarValue::Int64(43)],
                vec![ScalarValue::Int64(3), ScalarValue::Int64(0)],
                vec![ScalarValue::Int64(4), ScalarValue::Int64(99)],
            ]
        );
        assert_int64_lookup(database, active, IndexId(2), 43, 1);
    } else {
        assert_eq!(active, source);
        assert_eq!(
            (
                table.column("legacy").unwrap().id,
                table.column("legacy").unwrap().semantic_type().physical,
            ),
            (OLD_LEGACY, PhysicalType::Text)
        );
        assert!(table.column_by_id(SHADOW).is_none());
        assert_eq!(
            (indexes[0].id, indexes[0].column_id),
            (IndexId(1), OLD_LEGACY)
        );
    }
}

#[test]
fn conversion_crash_matrix_reopens_without_replaying_text_conversion() {
    let cases = [
        ("deferred-backfill-accepted", false, false),
        ("round59-conversion-accepted", false, false),
        ("round58-after-logical-index-evacuation", false, false),
        ("round56-after-terminal-drop", false, false),
        ("round56-after-terminal-rename", false, false),
        ("adopted-final-index-reservation-durable", false, false),
        ("composition-intent-durable", false, false),
        ("source-backfill-intent-durable", false, false),
        ("source-backfill-mid-copy", false, false),
        ("after-all-prepares", true, false),
        ("after-durable-decision", true, true),
        ("after-durable-complete-before-publication", true, true),
    ];
    for (point, coordinator, winner) in cases {
        let path = root(&format!("crash-{point}"));
        let database = seed(&path, true, "bad");
        let source = database.bindings.resolve_single(USERS).unwrap();
        database.close().unwrap();
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "deferred_type_conversion_audit_tests::round59_conversion_crash_child",
                "--nocapture",
            ])
            .env("NETBADB_ROUND59_CRASH_ROOT", &path);
        if coordinator {
            crate::coordinator_crash::configure_child(&mut command, point, &path, point);
        } else {
            command.env("NETBADB_BACKFILL_CRASH_POINT", point);
        }
        let output = command.output().unwrap();
        assert_eq!(
            output.status.code(),
            Some(if coordinator { 87 } else { 90 }),
            "{point}: {} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        for _ in 0..3 {
            let mut reopened = Database::open_catalog(path.join("catalog")).unwrap();
            assert_recovered_conversion(&mut reopened, source, winner);
            reopened.close().unwrap();
        }
        assert_no_stage(&path);
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn cord_v4_checkpoint_precedes_one_structural_conversion_tail() {
    let path = root("cord-v4");
    let mut database = Database::create_catalog(
        path.join("catalog"),
        vec![TableStorageCreateSpec::heap(
            path.join("seed.heap"),
            TableDef::new(
                TableId(1),
                "seed",
                vec![ColumnDef::new(
                    ID,
                    "id",
                    TypeSpec::Physical(PhysicalType::Int64),
                )],
            ),
        )],
        Some(DatabaseCoordinatorConfig::new(path.join("coordinator")).with_global_visibility()),
    )
    .unwrap();
    for id in 1..=10 {
        database
            .execute(&format!("INSERT INTO seed VALUES ({id})"))
            .unwrap();
    }
    let report = database.compact_coordinator_log().unwrap();
    assert!(report.compacted);
    database.close().unwrap();
    database = Database::open_catalog(path.join("catalog")).unwrap();
    assert_eq!(
        database
            .inspect_global_visibility()
            .unwrap()
            .checkpointed_through,
        Some(report.checkpointed_through)
    );

    database
        .execute("CREATE TABLE users (id BIGINT NOT NULL, legacy TEXT NOT NULL, flag BOOLEAN)")
        .unwrap();
    for statement in [
        "INSERT INTO users VALUES (1, '42', true)",
        "INSERT INTO users VALUES (2, '-7', false)",
        "INSERT INTO users VALUES (3, 'bad', NULL)",
        "CREATE INDEX users_legacy_idx ON users(legacy)",
    ] {
        database.execute(statement).unwrap();
    }
    let source = database.bindings.resolve_single(USERS).unwrap();
    let before = database.current_database_snapshot().unwrap().unwrap();
    let mut transaction = adopt(&mut database);
    database
        .execute_in(
            &mut transaction,
            "UPDATE users SET shadow = 0 WHERE legacy = 'bad' AND shadow IS NULL",
        )
        .unwrap();
    assert_eq!(convert_legacy(&mut database, &mut transaction).unwrap(), 2);
    database
        .execute_in(
            &mut transaction,
            "ALTER TABLE users ALTER COLUMN shadow SET NOT NULL",
        )
        .unwrap();
    finish_swap(&mut database, &mut transaction);
    database.commit_transaction(&mut transaction).unwrap();
    drop(transaction);
    let after = database.current_database_snapshot().unwrap().unwrap();
    assert_eq!(after.commit_seq().0, before.commit_seq().0 + 1);
    let visibility = database.inspect_global_visibility().unwrap();
    assert_eq!(
        visibility.checkpointed_through,
        Some(report.checkpointed_through)
    );
    assert!(!visibility.compaction_possible);
    let compaction_error = database.compact_coordinator_log().unwrap_err();
    assert!(
        matches!(
            compaction_error,
            DatabaseError::Transaction(
                CoordinatorError::CoordinatorCompactionStructuralHistoryRequired
            )
        ),
        "{compaction_error:?}"
    );
    database.close().unwrap();
    let mut reopened = Database::open_catalog(path.join("catalog")).unwrap();
    assert_eq!(
        reopened
            .inspect_global_visibility()
            .unwrap()
            .checkpointed_through,
        Some(report.checkpointed_through)
    );
    assert_recovered_conversion(&mut reopened, source, true);
    reopened.close().unwrap();
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn cord_v4_checkpoint_and_conversion_decision_crash_preserve_both_frontiers() {
    let path = root("cord-v4-crash");
    let mut database = Database::create_catalog(
        path.join("catalog"),
        vec![TableStorageCreateSpec::heap(
            path.join("seed.heap"),
            TableDef::new(
                TableId(1),
                "seed",
                vec![ColumnDef::new(
                    ID,
                    "id",
                    TypeSpec::Physical(PhysicalType::Int64),
                )],
            ),
        )],
        Some(DatabaseCoordinatorConfig::new(path.join("coordinator")).with_global_visibility()),
    )
    .unwrap();
    for id in 1..=10 {
        database
            .execute(&format!("INSERT INTO seed VALUES ({id})"))
            .unwrap();
    }
    let checkpoint = database
        .compact_coordinator_log()
        .unwrap()
        .checkpointed_through;
    database
        .execute("CREATE TABLE users (id BIGINT NOT NULL, legacy TEXT NOT NULL, flag BOOLEAN)")
        .unwrap();
    for statement in [
        "INSERT INTO users VALUES (1, '42', true)",
        "INSERT INTO users VALUES (2, '-7', false)",
        "INSERT INTO users VALUES (3, 'bad', NULL)",
        "CREATE INDEX users_legacy_idx ON users(legacy)",
    ] {
        database.execute(statement).unwrap();
    }
    let before = database.current_database_snapshot().unwrap().unwrap();
    let source = database.bindings.resolve_single(USERS).unwrap();
    database.close().unwrap();

    let mut command = std::process::Command::new(std::env::current_exe().unwrap());
    command
        .args([
            "--exact",
            "deferred_type_conversion_audit_tests::round59_conversion_crash_child",
            "--nocapture",
        ])
        .env("NETBADB_ROUND59_CRASH_ROOT", &path);
    crate::coordinator_crash::configure_child(
        &mut command,
        "round59-cord-v4-tail",
        &path,
        "after-durable-decision",
    );
    let output = command.output().unwrap();
    assert_eq!(
        output.status.code(),
        Some(87),
        "{} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    for _ in 0..3 {
        let mut reopened = Database::open_catalog(path.join("catalog")).unwrap();
        assert_recovered_conversion(&mut reopened, source, true);
        let snapshot = reopened.current_database_snapshot().unwrap().unwrap();
        assert_eq!(snapshot.commit_seq().0, before.commit_seq().0 + 1);
        assert_eq!(
            reopened
                .inspect_global_visibility()
                .unwrap()
                .checkpointed_through,
            Some(checkpoint)
        );
        reopened.close().unwrap();
    }
    assert_no_stage(&path);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn enabled_change_stream_blocks_conversion_replacement_before_s2() {
    let path = root("change-stream");
    let mut database = seed(&path, true, "bad");
    database.enable_change_stream(USERS).unwrap();
    let source = database.bindings.resolve_single(USERS).unwrap();
    let target = database.next_storage_id().unwrap();
    let mut transaction = adopt(&mut database);
    database
        .execute_in(
            &mut transaction,
            "UPDATE users SET shadow = 0 WHERE legacy = 'bad' AND shadow IS NULL",
        )
        .unwrap();
    assert_eq!(convert_legacy(&mut database, &mut transaction).unwrap(), 2);
    database
        .execute_in(
            &mut transaction,
            "ALTER TABLE users ALTER COLUMN shadow SET NOT NULL",
        )
        .unwrap();
    finish_swap(&mut database, &mut transaction);
    assert!(matches!(
        database.finalize_adopted_source(&mut transaction),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::ActiveChangeStreamBlocksReplacement { .. }
        ))
    ));
    assert_eq!(database.bindings.resolve_single(USERS), Ok(source));
    assert_eq!(database.next_storage_id(), Some(target));
    transaction.rollback().unwrap();
    database.close().unwrap();
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn production_hir_negatives_remain_exact_and_do_not_append_a_program() {
    let path = root("production-negatives");
    let mut database = seed(&path, false, "bad");
    let error = database
        .prepare_sql_statement("SELECT '42'::BIGINT", &[])
        .unwrap_err();
    assert_eq!(error.kind(), DatabaseErrorKind::DatatypeMismatch);
    assert_eq!(error.to_string(), "expected INT64, found TEXT");

    let mut transaction = adopt(&mut database);
    let before = transaction
        .schema_composition
        .plan()
        .unwrap()
        .action_evidence
        .clone();
    let error = database
        .execute_in(
            &mut transaction,
            "UPDATE users SET shadow = legacy::BIGINT WHERE shadow IS NULL",
        )
        .unwrap_err();
    assert_eq!(error.kind(), DatabaseErrorKind::DatatypeMismatch);
    assert_eq!(error.to_string(), "expected INT64, found TEXT");
    let plan = transaction.schema_composition.plan().unwrap();
    assert_eq!(plan.action_evidence, before);
    assert_eq!(plan.deferred_backfill.len(), 0);
    assert!(matches!(
        transaction.schema_composition,
        SchemaCompositionState::AdoptedSourceRefining(_)
    ));
    transaction.rollback().unwrap();
    database.close().unwrap();
    std::fs::remove_dir_all(path).unwrap();
}
