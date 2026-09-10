//! Round 61 test-only synthetic ALTER COLUMN TYPE ... USING architecture audit.

use super::*;
use crate::schema_composition::{
    AlterTypeUsingAuditSpec, SchemaCompositionState, TypeConversionSource,
};
use crate::schema_mutation_journal::SchemaIndexTablePlan;
use netbadb_rel::{Expr, ExprKind, LogicalPlan, LogicalStatement};
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_types::{
    ColumnId, ExprType, IndexId, PhysicalType, ScalarValue, SemanticType, StorageId, TableId,
};
use std::path::{Path, PathBuf};

const USERS: TableId = TableId(2);
const ID: ColumnId = ColumnId(1);
const LEGACY: ColumnId = ColumnId(2);
const FLAG: ColumnId = ColumnId(3);
const REPLACEMENT: ColumnId = ColumnId(4);

fn root(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "netbadb-round61-{name}-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_dir_all(&path);
    std::fs::create_dir_all(&path).unwrap();
    path
}

fn seed(path: &Path, invalid: bool, nullable: bool) -> Database {
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
        Some(DatabaseCoordinatorConfig::new(path.join("coordinator")).with_global_visibility()),
    )
    .unwrap();
    let nullability = if nullable { "" } else { " NOT NULL" };
    database
        .execute(&format!(
            "CREATE TABLE users (id BIGINT NOT NULL, legacy TEXT{nullability}, flag BOOLEAN)"
        ))
        .unwrap();
    let third = if invalid { "bad" } else { "99" };
    for statement in [
        "INSERT INTO users VALUES (1, '43', true)".to_owned(),
        "INSERT INTO users VALUES (2, '0', false)".to_owned(),
        format!("INSERT INTO users VALUES (3, '{third}', true)"),
        "CREATE INDEX users_legacy_idx ON users(legacy)".to_owned(),
        "CREATE INDEX users_flag_idx ON users(flag)".to_owned(),
    ] {
        database.execute(&statement).unwrap();
    }
    database
}

fn dependency(database: &Database) -> SchemaDependency {
    let table = database.schema().table("users").unwrap();
    let lineage = database
        .committed
        .tables
        .iter()
        .find(|lineage| lineage.table_id == table.id)
        .unwrap();
    SchemaDependency {
        table_id: table.id,
        table_version: lineage.version,
        fingerprint: table.fingerprint().unwrap(),
    }
}

fn query_expression(database: &Database, transaction: Option<&Transaction>, source: &str) -> Expr {
    let prepared = match transaction {
        Some(transaction) => database
            .prepare_sql_statement_in(transaction, source, &[])
            .unwrap(),
        None => database.prepare_sql_statement(source, &[]).unwrap(),
    };
    let PreparedSqlStatement::Relational(prepared) = prepared else {
        panic!("expected relational USING carrier")
    };
    let LogicalStatement::Query(plan) = &prepared.compiled.logical_statement else {
        panic!("expected query USING carrier")
    };
    match plan {
        LogicalPlan::ScalarProject { expressions, .. } => {
            assert_eq!(expressions.len(), 1);
            expressions[0].expression.clone()
        }
        LogicalPlan::Project { columns, .. } => {
            assert_eq!(columns.len(), 1);
            let column = columns[0].clone();
            Expr {
                kind: ExprKind::Column(column.clone()),
                expr_type: ExprType {
                    data_type: column.data_type,
                    nullable: column.nullable,
                },
            }
        }
        other => panic!("unexpected USING carrier plan: {other:?}"),
    }
}

fn spec(database: &Database, using: Expr) -> AlterTypeUsingAuditSpec {
    AlterTypeUsingAuditSpec {
        target: dependency(database),
        column_id: LEGACY,
        target_type: SemanticType::physical(PhysicalType::Int64),
        using,
        hidden_name_seed: None,
    }
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
        .into_iter()
        .find(|definition| definition.id == index)
        .unwrap()
        .column_id;
    let access_path = storage
        .access_paths()
        .into_iter()
        .find(|path| path.column_id == column)
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
fn pristine_synthetic_plan_preserves_order_and_replaces_one_index_in_one_s2() {
    let path = root("pristine");
    let mut database = seed(&path, false, false);
    let source_storage = database.bindings.resolve_single(USERS).unwrap();
    let target_storage = database.next_storage_id().unwrap();
    let old_indexes = database.indexes(USERS).unwrap();
    assert_eq!(
        old_indexes
            .iter()
            .map(|index| (index.id, index.column_id))
            .collect::<Vec<_>>(),
        vec![(IndexId(1), LEGACY), (IndexId(2), FLAG)]
    );
    let prepared_old = match database
        .prepare_sql_statement("SELECT legacy FROM users", &[])
        .unwrap()
    {
        PreparedSqlStatement::Relational(prepared) => prepared,
        PreparedSqlStatement::Ddl(_) => panic!("expected old query"),
    };
    let using = query_expression(&database, None, "SELECT legacy::BIGINT FROM users");
    let mut transaction = database.begin_transaction().unwrap();
    let outcome = database
        .audit_alter_type_using(&mut transaction, spec(&database, using))
        .unwrap();
    assert_eq!(
        (
            outcome.new_column_id,
            outcome.new_index_id,
            outcome.affected_rows,
            outcome.adopted_source,
            outcome.validation_scans,
        ),
        (REPLACEMENT, Some(IndexId(3)), 3, false, 1)
    );
    assert_eq!(transaction.write_participant_count(), 0);
    assert_eq!(database.next_storage_id(), Some(target_storage));
    assert!(matches!(
        transaction.schema_composition,
        SchemaCompositionState::TypeConversionReady(_)
    ));
    let plan = transaction.schema_composition.plan().unwrap();
    assert_eq!(plan.deferred_backfill.len(), 1);
    assert_eq!(
        plan.deferred_backfill.evaluation_column_ids().unwrap(),
        vec![ID, LEGACY, FLAG, REPLACEMENT]
    );
    let private = plan.overlay.schema.table("users").unwrap();
    assert_eq!(
        private
            .columns
            .iter()
            .map(|column| column.id)
            .collect::<Vec<_>>(),
        vec![ID, REPLACEMENT, FLAG]
    );
    assert!(!private.column("legacy").unwrap().nullable);
    assert!(private.column(&outcome.hidden_evaluation_name).is_none());
    let private_indexes = &plan.touched[&USERS].indexes.active;
    assert_eq!(
        private_indexes
            .iter()
            .map(|index| (index.id, index.column_id, index.name.clone()))
            .collect::<Vec<_>>(),
        vec![
            (
                IndexId(2),
                FLAG,
                Some(IndexName::new("users_flag_idx").unwrap())
            ),
            (
                IndexId(3),
                REPLACEMENT,
                Some(IndexName::new("users_legacy_idx").unwrap())
            ),
        ]
    );
    assert_eq!(
        database
            .schema()
            .table("users")
            .unwrap()
            .column("legacy")
            .unwrap()
            .id,
        LEGACY
    );
    assert_eq!(
        database
            .registry
            .get(source_storage)
            .unwrap()
            .table()
            .column("legacy")
            .unwrap()
            .id,
        LEGACY
    );
    assert!(matches!(
        database.execute_in(&mut transaction, "SELECT * FROM users"),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::MigrationDataAccessAfterRefinement
        ))
    ));
    assert!(matches!(
        database.execute_in(&mut transaction, "UPDATE users SET flag = flag"),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::MigrationDataAccessAfterRefinement
        ))
    ));
    assert!(matches!(
        database.execute_in(
            &mut transaction,
            "ALTER TABLE users ADD COLUMN later BIGINT"
        ),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::SchemaMutationAfterMaterialization
        ))
    ));

    let whole_action_digest = transaction
        .schema_composition
        .plan()
        .unwrap()
        .action_digest();
    database
        .ensure_schema_materialized(&mut transaction)
        .unwrap();
    let materialized = transaction.schema_composition.materialized_index().unwrap();
    assert_eq!(materialized.intent.action_count, 1);
    assert_eq!(materialized.intent.action_digest, whole_action_digest);
    assert_eq!(
        materialized.logical.deferred_backfill.semantic_digest(0),
        Some(outcome.semantic_digest)
    );
    assert!(
        !database
            .mutation_journal
            .as_ref()
            .unwrap()
            .borrow()
            .source_backfill_intents
            .contains_key(&transaction.id())
    );
    let SchemaIndexTablePlan::RewriteHeap {
        replacement,
        base_indexes,
        final_indexes,
    } = &materialized.intent.tables[0]
    else {
        panic!("expected one Heap rewrite")
    };
    assert_eq!(replacement.old_storage(), source_storage);
    assert_eq!(replacement.new_storage(), target_storage);
    assert_eq!(base_indexes.active[0].id, IndexId(1));
    assert_eq!(final_indexes.active[0].id, IndexId(2));
    assert_eq!(final_indexes.active[1].id, IndexId(3));
    database.commit_transaction(&mut transaction).unwrap();

    assert_eq!(database.bindings.resolve_single(USERS), Ok(target_storage));
    assert_eq!(
        database.next_storage_id(),
        Some(StorageId(target_storage.0 + 1))
    );
    let final_table = database.schema().table("users").unwrap();
    assert_eq!(
        final_table
            .columns
            .iter()
            .map(|column| column.id)
            .collect::<Vec<_>>(),
        vec![ID, REPLACEMENT, FLAG]
    );
    assert_eq!(
        database
            .query("SELECT id, legacy FROM users ORDER BY id")
            .unwrap()
            .rows,
        vec![
            vec![ScalarValue::Int64(1), ScalarValue::Int64(43)],
            vec![ScalarValue::Int64(2), ScalarValue::Int64(0)],
            vec![ScalarValue::Int64(3), ScalarValue::Int64(99)],
        ]
    );
    assert!(matches!(
        database.execute_prepared(&prepared_old, &[]),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::StalePreparedStatement
        ))
    ));
    for (key, id) in [(43, 1), (0, 2), (99, 3)] {
        assert_int64_lookup(&mut database, target_storage, IndexId(3), key, id);
    }
    for _ in 0..3 {
        database.close().unwrap();
        database = Database::open_catalog(path.join("catalog")).unwrap();
        assert_eq!(database.bindings.resolve_single(USERS), Ok(target_storage));
        assert_int64_lookup(&mut database, target_storage, IndexId(3), 43, 1);
    }
    database.close().unwrap();
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn post_dml_source_reads_transaction_visible_repair_and_emits_tag35() {
    let path = root("adopted");
    let mut database = seed(&path, true, false);
    let target_storage = database.next_storage_id().unwrap();
    let mut transaction = database.begin_transaction().unwrap();
    assert_eq!(
        database
            .execute_in(
                &mut transaction,
                "UPDATE users SET legacy = '0' WHERE legacy = 'bad'",
            )
            .unwrap(),
        ExecutionResult::AffectedRows(1)
    );
    let using = query_expression(
        &database,
        Some(&transaction),
        "SELECT legacy::BIGINT FROM users",
    );
    let outcome = database
        .audit_alter_type_using(&mut transaction, spec(&database, using))
        .unwrap();
    assert!(outcome.adopted_source);
    let SchemaCompositionState::TypeConversionReady(conversion) = &transaction.schema_composition
    else {
        panic!("expected sealed type conversion")
    };
    assert!(matches!(
        conversion.source,
        TypeConversionSource::Adopted(_)
    ));
    assert_eq!(database.next_storage_id(), Some(target_storage));
    database
        .ensure_schema_materialized(&mut transaction)
        .unwrap();
    let materialized = transaction.schema_composition.materialized_index().unwrap();
    assert_eq!(
        (
            materialized.source_copy_passes,
            materialized.source_rows_copied
        ),
        (1, 3)
    );
    assert!(
        database
            .mutation_journal
            .as_ref()
            .unwrap()
            .borrow()
            .source_backfill_intents
            .contains_key(&transaction.id())
    );
    database.commit_transaction(&mut transaction).unwrap();
    assert_eq!(database.bindings.resolve_single(USERS), Ok(target_storage));
    assert_eq!(
        database
            .query("SELECT id, legacy FROM users ORDER BY id")
            .unwrap()
            .rows,
        vec![
            vec![ScalarValue::Int64(1), ScalarValue::Int64(43)],
            vec![ScalarValue::Int64(2), ScalarValue::Int64(0)],
            vec![ScalarValue::Int64(3), ScalarValue::Int64(0)],
        ]
    );
    assert_int64_lookup(&mut database, target_storage, IndexId(3), 43, 1);
    database.close().unwrap();
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn failed_validation_does_not_burn_column_or_index_identity() {
    let path = root("invalid");
    let mut database = seed(&path, true, false);
    let using = query_expression(&database, None, "SELECT legacy::BIGINT FROM users");
    let mut failed = database.begin_transaction().unwrap();
    assert!(matches!(
        database.audit_alter_type_using(&mut failed, spec(&database, using)),
        Err(DatabaseError::Execution(
            netbadb_executor::ExecutionError::InvalidCastText {
                target: PhysicalType::Int64
            }
        ))
    ));
    assert!(failed.schema_composition.is_none());
    assert_eq!(failed.write_participant_count(), 0);
    failed.rollback().unwrap();
    drop(failed);

    let using = query_expression(&database, None, "SELECT 0::BIGINT");
    let mut accepted = database.begin_transaction().unwrap();
    let outcome = database
        .audit_alter_type_using(&mut accepted, spec(&database, using))
        .unwrap();
    assert_eq!(outcome.new_column_id, REPLACEMENT);
    assert_eq!(outcome.new_index_id, Some(IndexId(3)));
    accepted.rollback().unwrap();
    assert_eq!(
        database
            .schema()
            .table("users")
            .unwrap()
            .column("legacy")
            .unwrap()
            .id,
        LEGACY
    );
    database.close().unwrap();
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn using_other_column_and_constant_do_not_require_old_to_target_cast() {
    for (name, sql, expected) in [
        ("other", "SELECT id FROM users", vec![1_i64, 2, 3]),
        ("constant", "SELECT 7::BIGINT", vec![7_i64, 7, 7]),
    ] {
        let path = root(name);
        let mut database = seed(&path, false, false);
        let using = query_expression(&database, None, sql);
        let mut transaction = database.begin_transaction().unwrap();
        database
            .audit_alter_type_using(&mut transaction, spec(&database, using))
            .unwrap();
        database.commit_transaction(&mut transaction).unwrap();
        assert_eq!(
            database
                .query("SELECT legacy FROM users ORDER BY id")
                .unwrap()
                .rows,
            expected
                .into_iter()
                .map(|value| vec![ScalarValue::Int64(value)])
                .collect::<Vec<_>>()
        );
        database.close().unwrap();
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn hidden_name_is_not_semantic_and_nullable_result_obeys_final_contract() {
    let mut digests = Vec::new();
    for hidden in ["__first", "legacy"] {
        let path = root(hidden);
        let mut database = seed(&path, false, false);
        let using = query_expression(&database, None, "SELECT legacy::BIGINT FROM users");
        let mut request = spec(&database, using);
        request.hidden_name_seed = Some(hidden.to_owned());
        let mut transaction = database.begin_transaction().unwrap();
        let outcome = database
            .audit_alter_type_using(&mut transaction, request)
            .unwrap();
        assert_ne!(outcome.hidden_evaluation_name, "legacy");
        digests.push(outcome.semantic_digest);
        transaction.rollback().unwrap();
        database.close().unwrap();
        std::fs::remove_dir_all(path).unwrap();
    }
    assert_eq!(digests[0], digests[1]);

    let path = root("not-null");
    let mut database = seed(&path, false, false);
    let using = Expr {
        kind: ExprKind::Literal(ScalarValue::Null),
        expr_type: ExprType {
            data_type: SemanticType::physical(PhysicalType::Int64),
            nullable: true,
        },
    };
    let mut transaction = database.begin_transaction().unwrap();
    assert!(matches!(
        database.audit_alter_type_using(&mut transaction, spec(&database, using)),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::NotNullViolation(REPLACEMENT)
        ))
    ));
    assert!(transaction.schema_composition.is_none());
    transaction.rollback().unwrap();
    database.close().unwrap();
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn closed_language_and_structural_admission_boundaries_remain_exact() {
    let path = root("boundaries");
    let mut database = seed(&path, false, false);
    let error = database
        .prepare_sql_statement(
            "ALTER TABLE users ALTER COLUMN legacy TYPE BIGINT USING legacy::BIGINT",
            &[],
        )
        .unwrap_err();
    assert_eq!(error.kind(), DatabaseErrorKind::FeatureNotSupported);

    let text_using = query_expression(&database, None, "SELECT legacy FROM users");
    let mut mismatch = database.begin_transaction().unwrap();
    assert!(matches!(
        database.audit_alter_type_using(&mut mismatch, spec(&database, text_using)),
        Err(DatabaseError::Execution(
            netbadb_executor::ExecutionError::TypeMismatch
        ))
    ));
    mismatch.rollback().unwrap();
    drop(mismatch);

    assert_eq!(
        database
            .prepare_sql_statement("SELECT true::BIGINT", &[])
            .unwrap_err()
            .kind(),
        DatabaseErrorKind::CannotCoerce
    );

    let mut group = database.begin_group_commit().unwrap();
    let mut member = database.begin_group_member(&group).unwrap();
    let using = query_expression(&database, None, "SELECT legacy::BIGINT FROM users");
    assert!(matches!(
        database.audit_alter_type_using(&mut member, spec(&database, using)),
        Err(DatabaseError::Transaction(
            CoordinatorError::GroupCommitStructuralMutation
        ))
    ));
    assert!(member.schema_composition.is_none());
    member.rollback().unwrap();
    database.abort_group(&mut group).unwrap();
    database.close().unwrap();
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn enabled_change_stream_blocks_before_scan_or_identity_reservation() {
    let path = root("stream");
    let mut database = seed(&path, false, false);
    database.enable_change_stream(USERS).unwrap();
    let projection = database
        .build_incremental_columnar_projection(ColumnarProjectionSpec::new(
            USERS,
            path.join("projection"),
            vec![ID, LEGACY, FLAG],
        ))
        .unwrap();
    for statement in [
        "INSERT INTO users VALUES (4, '4', false)",
        "INSERT INTO users VALUES (5, '5', true)",
    ] {
        database.execute(statement).unwrap();
    }
    database
        .advance_columnar_projection(projection, ColumnarAdvanceBudget::new(10, 1 << 30))
        .unwrap();
    let reclaimed = database.gc_change_stream(USERS).unwrap();
    assert!(reclaimed.batches_removed > 0);
    let stream_before = database.inspect_change_stream(USERS).unwrap();
    let using = query_expression(&database, None, "SELECT legacy::BIGINT FROM users");
    let mut transaction = database.begin_transaction().unwrap();
    assert!(matches!(
        database.audit_alter_type_using(&mut transaction, spec(&database, using)),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::ActiveChangeStreamBlocksReplacement { .. }
        ))
    ));
    assert!(transaction.schema_composition.is_none());
    assert_eq!(
        database.inspect_change_stream(USERS).unwrap(),
        stream_before
    );
    transaction.rollback().unwrap();
    drop(transaction);
    database.disable_change_stream(USERS).unwrap();
    let using = query_expression(&database, None, "SELECT legacy::BIGINT FROM users");
    let mut accepted = database.begin_transaction().unwrap();
    let outcome = database
        .audit_alter_type_using(&mut accepted, spec(&database, using))
        .unwrap();
    assert_eq!(outcome.new_column_id, REPLACEMENT);
    accepted.rollback().unwrap();
    database.close().unwrap();
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn nullable_values_survive_and_zero_row_dml_is_real_adopted_authority() {
    let path = root("nullable");
    let mut database = seed(&path, false, true);
    database
        .execute("UPDATE users SET legacy = NULL WHERE id = 2")
        .unwrap();
    let using = query_expression(&database, None, "SELECT legacy::BIGINT FROM users");
    let mut transaction = database.begin_transaction().unwrap();
    let outcome = database
        .audit_alter_type_using(&mut transaction, spec(&database, using))
        .unwrap();
    assert!(!outcome.adopted_source);
    database.commit_transaction(&mut transaction).unwrap();
    assert!(
        database
            .schema()
            .table("users")
            .unwrap()
            .column("legacy")
            .unwrap()
            .nullable
    );
    assert_eq!(
        database
            .query("SELECT legacy FROM users ORDER BY id")
            .unwrap()
            .rows,
        vec![
            vec![ScalarValue::Int64(43)],
            vec![ScalarValue::Null],
            vec![ScalarValue::Int64(99)],
        ]
    );
    database.close().unwrap();
    std::fs::remove_dir_all(path).unwrap();

    let path = root("zero-row-dml");
    let mut database = seed(&path, false, false);
    let mut transaction = database.begin_transaction().unwrap();
    assert_eq!(
        database
            .execute_in(
                &mut transaction,
                "UPDATE users SET legacy = legacy WHERE id = 999",
            )
            .unwrap(),
        ExecutionResult::AffectedRows(0)
    );
    let using = query_expression(
        &database,
        Some(&transaction),
        "SELECT legacy::BIGINT FROM users",
    );
    let outcome = database
        .audit_alter_type_using(&mut transaction, spec(&database, using))
        .unwrap();
    assert!(outcome.adopted_source);
    transaction.rollback().unwrap();
    database.close().unwrap();
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn primary_key_same_physical_and_prior_read_boundaries_are_closed() {
    let path = root("same-physical");
    let mut database = seed(&path, false, false);
    let using = query_expression(&database, None, "SELECT legacy FROM users");
    let mut request = spec(&database, using);
    request.target_type = SemanticType::physical(PhysicalType::Text);
    let mut transaction = database.begin_transaction().unwrap();
    assert!(matches!(
        database.audit_alter_type_using(&mut transaction, request),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::UnsupportedSchemaEvolution
        ))
    ));
    transaction.rollback().unwrap();
    drop(transaction);

    let mut prior_read = database.begin_transaction().unwrap();
    database
        .execute_in(&mut prior_read, "SELECT id FROM users")
        .unwrap();
    let using = query_expression(
        &database,
        Some(&prior_read),
        "SELECT legacy::BIGINT FROM users",
    );
    assert!(matches!(
        database.audit_alter_type_using(&mut prior_read, spec(&database, using)),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::UnsupportedBackfillRefinement(_)
        ))
    ));
    prior_read.rollback().unwrap();
    database.close().unwrap();
    std::fs::remove_dir_all(path).unwrap();

    let path = root("primary-key");
    let mut database = Database::create_catalog(
        path.join("catalog"),
        vec![
            TableStorageCreateSpec::heap(
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
            ),
            TableStorageCreateSpec::heap(
                path.join("users.heap"),
                TableDef::new(
                    USERS,
                    "users",
                    vec![
                        ColumnDef::new(ID, "id", TypeSpec::Physical(PhysicalType::Int64)),
                        ColumnDef::new(LEGACY, "legacy", TypeSpec::Physical(PhysicalType::Text))
                            .primary_key(true),
                        ColumnDef::new(FLAG, "flag", TypeSpec::Physical(PhysicalType::Bool)),
                    ],
                ),
            ),
        ],
        Some(DatabaseCoordinatorConfig::new(path.join("coordinator"))),
    )
    .unwrap();
    let using = query_expression(&database, None, "SELECT 0::BIGINT");
    let mut transaction = database.begin_transaction().unwrap();
    assert!(matches!(
        database.audit_alter_type_using(&mut transaction, spec(&database, using)),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::PrimaryKeyColumn(LEGACY)
        ))
    ));
    assert!(transaction.schema_composition.is_none());
    transaction.rollback().unwrap();
    database.close().unwrap();
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn round61_crash_child() {
    let Ok(path) = std::env::var("NETBADB_ROUND61_CRASH_ROOT") else {
        return;
    };
    let adopted = std::env::var_os("NETBADB_ROUND61_ADOPTED").is_some();
    let mut database = Database::open_catalog(Path::new(&path).join("catalog")).unwrap();
    let mut transaction = database.begin_transaction().unwrap();
    if adopted {
        database
            .execute_in(
                &mut transaction,
                "UPDATE users SET legacy = legacy WHERE id = 1",
            )
            .unwrap();
    }
    let using = query_expression(
        &database,
        adopted.then_some(&transaction),
        "SELECT legacy::BIGINT FROM users",
    );
    database
        .audit_alter_type_using(&mut transaction, spec(&database, using))
        .unwrap();
    database.commit_transaction(&mut transaction).unwrap();
    panic!("configured Round 61 crash point was not reached");
}

fn assert_recovered(database: &mut Database, source: StorageId, winner: bool) {
    let storage = database.bindings.resolve_single(USERS).unwrap();
    let table = database.schema().table("users").unwrap();
    let indexes = database.indexes(USERS).unwrap();
    assert!(
        table
            .columns
            .iter()
            .all(|column| !column.name.starts_with("__netbadb_alter_type_"))
    );
    if winner {
        assert_ne!(storage, source);
        assert_eq!(
            table
                .columns
                .iter()
                .map(|column| column.id)
                .collect::<Vec<_>>(),
            vec![ID, REPLACEMENT, FLAG]
        );
        assert_eq!(
            indexes
                .iter()
                .map(|index| (index.id, index.column_id))
                .collect::<Vec<_>>(),
            vec![(IndexId(2), FLAG), (IndexId(3), REPLACEMENT)]
        );
        assert_int64_lookup(database, storage, IndexId(3), 43, 1);
    } else {
        assert_eq!(storage, source);
        assert_eq!(
            table
                .columns
                .iter()
                .map(|column| column.id)
                .collect::<Vec<_>>(),
            vec![ID, LEGACY, FLAG]
        );
        assert_eq!(
            indexes
                .iter()
                .map(|index| (index.id, index.column_id))
                .collect::<Vec<_>>(),
            vec![(IndexId(1), LEGACY), (IndexId(2), FLAG)]
        );
    }
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
fn pristine_and_adopted_crash_matrices_converge_without_using_replay() {
    let cases = [
        ("round61-column-reservation-durable", false, false, false),
        ("round61-index-reservation-durable", false, false, false),
        ("round61-logical-plan-sealed", false, false, false),
        ("composition-intent-durable", false, false, false),
        ("composition-table-copy-complete", false, false, false),
        ("source-backfill-intent-durable", true, false, false),
        ("source-backfill-mid-copy", true, false, false),
        ("after-all-prepares", false, true, false),
        ("after-durable-decision", false, true, true),
        (
            "after-durable-complete-before-publication",
            true,
            true,
            true,
        ),
    ];
    for (point, adopted, coordinator, winner) in cases {
        let path = root(&format!("crash-{point}-{adopted}"));
        let database = seed(&path, false, false);
        let source = database.bindings.resolve_single(USERS).unwrap();
        database.close().unwrap();
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "alter_type_using_audit_tests::round61_crash_child",
                "--nocapture",
            ])
            .env("NETBADB_ROUND61_CRASH_ROOT", &path);
        if adopted {
            command.env("NETBADB_ROUND61_ADOPTED", "1");
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
            "{point}: {} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        for _ in 0..3 {
            let mut reopened = Database::open_catalog(path.join("catalog")).unwrap();
            assert_recovered(&mut reopened, source, winner);
            reopened.close().unwrap();
        }
        assert_no_stage(&path);
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
#[ignore = "manual Round 61 10K/100K cost observation"]
fn alter_type_using_cost_probe() {
    for rows in [10_000_usize, 100_000] {
        let path = root(&format!("cost-{rows}"));
        let database = seed(&path, false, false);
        let base = database.schema().table("users").unwrap().clone();
        let base_version = dependency(&database).table_version;
        let using = query_expression(&database, None, "SELECT legacy::BIGINT FROM users");
        database.close().unwrap();

        let evaluation_version = TableSchemaVersion(base_version.0 + 1);
        let mut evaluation = base.clone();
        evaluation.columns.push(
            ColumnDef::new(
                REPLACEMENT,
                "__round61_cost_shadow",
                TypeSpec::Physical(PhysicalType::Int64),
            )
            .nullable(true),
        );
        let mut final_table = base.clone();
        final_table.columns[1] = ColumnDef::new(
            REPLACEMENT,
            "legacy",
            TypeSpec::Physical(PhysicalType::Int64),
        );
        let cost = crate::deferred_backfill::audit_synthetic_deferred_cost(
            using,
            &base,
            base_version,
            &evaluation,
            evaluation_version,
            &final_table,
            REPLACEMENT,
            rows,
            |row| {
                let id = i64::try_from(row + 1).unwrap();
                vec![
                    ScalarValue::Int64(id),
                    ScalarValue::Text(id.to_string()),
                    ScalarValue::Bool(row % 2 == 0),
                ]
            },
        )
        .unwrap();
        println!(
            "round61 rows={rows} validation_us={} finalization_us={} known_program_vec_allocations={} metadata_bytes={}",
            cost.validation.as_micros(),
            cost.finalization.as_micros(),
            cost.transient_vector_allocations,
            cost.resident_metadata_bytes,
        );
        std::fs::remove_dir_all(path).unwrap();
    }
}
