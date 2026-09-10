//! Round 62 production ALTER COLUMN TYPE ... USING coverage.

use super::*;
use crate::schema_composition::{SchemaCompositionState, TypeConversionSource};
use crate::schema_mutation_journal::SchemaIndexTablePlan;
use netbadb_rel::{Expr, ExprKind, LogicalPlan, LogicalStatement};
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_types::{ColumnId, ExprType, IndexId, PhysicalType, ScalarValue, StorageId, TableId};
use std::path::{Path, PathBuf};

const USERS: TableId = TableId(2);
const ID: ColumnId = ColumnId(1);
const LEGACY: ColumnId = ColumnId(2);
const FLAG: ColumnId = ColumnId(3);
const REPLACEMENT: ColumnId = ColumnId(4);
const ALTER_TYPE_SQL: &str =
    "ALTER TABLE users ALTER COLUMN legacy TYPE BIGINT USING legacy::BIGINT";

fn root(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "netbadb-round62-{name}-{}-{:?}",
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

fn prepare_ddl(database: &Database, source: &str) -> PreparedDdlStatement {
    let PreparedSqlStatement::Ddl(prepared) = database.prepare_sql_statement(source, &[]).unwrap()
    else {
        panic!("expected DDL")
    };
    prepared
}

fn digest_hex(digest: [u8; 32]) -> String {
    digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>()
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
    let ddl = prepare_ddl(&database, ALTER_TYPE_SQL);
    let access = ddl.access();
    assert_eq!(access.read_tables(), &[USERS]);
    assert_eq!(access.write_tables(), &[USERS]);
    assert_eq!(access.schema_tables(), &[USERS]);
    assert!(access.schema_write());
    let mut transaction = database.begin_transaction().unwrap();
    assert_eq!(
        database.execute_ddl_in(&mut transaction, &ddl).unwrap(),
        DdlOutcome::Altered
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
    assert!(
        private
            .columns
            .iter()
            .all(|column| !column.name.starts_with("__netbadb_alter_type_"))
    );
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
        database.prepare_sql_statement_in(&transaction, "SELECT * FROM users", &[]),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::MigrationDataAccessAfterRefinement
        ))
    ));
    assert!(matches!(
        database.prepare_sql_statement_in(
            &transaction,
            "ALTER TABLE users ADD COLUMN later BIGINT",
            &[]
        ),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::SchemaMutationAfterMaterialization
        ))
    ));
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
    let semantic_digest = materialized
        .logical
        .deferred_backfill
        .semantic_digest(0)
        .unwrap();
    assert_eq!(
        digest_hex(semantic_digest),
        "0b5b8ee84bbdc8370cfc6d9a55707e8ce31cc5e33f3ae4a19e8a70f0f66a50a0"
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
    assert_eq!(
        database
            .execute_in(&mut transaction, ALTER_TYPE_SQL)
            .unwrap(),
        ExecutionResult::AffectedRows(0)
    );
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
    let mut failed = database.begin_transaction().unwrap();
    assert!(matches!(
        database.execute_in(&mut failed, ALTER_TYPE_SQL),
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

    database
        .execute("UPDATE users SET legacy = '128' WHERE id = 3")
        .unwrap();
    let mut ranged = database.begin_transaction().unwrap();
    assert!(matches!(
        database.execute_in(
            &mut ranged,
            "ALTER TABLE users ALTER COLUMN legacy TYPE TINYINT USING legacy::TINYINT",
        ),
        Err(DatabaseError::Execution(
            netbadb_executor::ExecutionError::CastOutOfRange {
                target: PhysicalType::Int8,
                ..
            }
        ))
    ));
    assert!(ranged.schema_composition.is_none());
    ranged.rollback().unwrap();
    drop(ranged);

    let mut accepted = database.begin_transaction().unwrap();
    assert_eq!(
        database
            .execute_in(
                &mut accepted,
                "ALTER TABLE users ALTER COLUMN legacy TYPE BIGINT USING 0::BIGINT",
            )
            .unwrap(),
        ExecutionResult::AffectedRows(0)
    );
    let plan = accepted.schema_composition.plan().unwrap();
    assert_eq!(
        plan.overlay
            .schema
            .table("users")
            .unwrap()
            .column("legacy")
            .unwrap()
            .id,
        REPLACEMENT
    );
    assert!(
        plan.touched[&USERS]
            .indexes
            .active
            .iter()
            .any(|index| index.id == IndexId(3) && index.column_id == REPLACEMENT)
    );
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
    for (name, using, expected) in [
        ("other", "id", vec![1_i64, 2, 3]),
        ("constant", "7::BIGINT", vec![7_i64, 7, 7]),
        ("qualified", "users.id", vec![1_i64, 2, 3]),
        (
            "chained",
            "legacy::BIGINT::TEXT::BIGINT",
            vec![43_i64, 0, 99],
        ),
    ] {
        let path = root(name);
        let mut database = seed(&path, false, false);
        database
            .execute(&format!(
                "ALTER TABLE users ALTER COLUMN legacy TYPE BIGINT USING {using}"
            ))
            .unwrap();
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

    let path = root("multiple");
    let mut database = seed(&path, false, false);
    database
        .execute(
            "ALTER TABLE users ALTER COLUMN legacy TYPE BOOLEAN \
             USING flag AND legacy IS NOT NULL",
        )
        .unwrap();
    assert_eq!(
        database
            .query("SELECT legacy FROM users ORDER BY id")
            .unwrap()
            .rows,
        vec![
            vec![ScalarValue::Bool(true)],
            vec![ScalarValue::Bool(false)],
            vec![ScalarValue::Bool(true)],
        ]
    );
    database.close().unwrap();
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn hidden_name_is_not_semantic_and_nullable_result_obeys_final_contract() {
    let path = root("hidden-digest");
    let database = seed(&path, false, false);
    let base = database.schema().table("users").unwrap().clone();
    let using = query_expression(&database, None, "SELECT legacy::BIGINT FROM users");
    let mut digests = Vec::new();
    for hidden in ["__first", "__second"] {
        let mut evaluation = base.clone();
        evaluation.columns.push(
            ColumnDef::new(REPLACEMENT, hidden, TypeSpec::Physical(PhysicalType::Int64))
                .nullable(true),
        );
        digests.push(
            crate::deferred_backfill::build_synthetic_assignment(
                using.clone(),
                &base,
                &evaluation,
                REPLACEMENT,
            )
            .unwrap()
            .semantic_digest(),
        );
    }
    assert_eq!(digests[0], digests[1]);
    database.close().unwrap();
    std::fs::remove_dir_all(path).unwrap();

    let path = root("not-null");
    let mut database = seed(&path, false, false);
    let mut transaction = database.begin_transaction().unwrap();
    assert!(matches!(
        database.execute_in(
            &mut transaction,
            "ALTER TABLE users ALTER COLUMN legacy TYPE BIGINT USING NULL::BIGINT",
        ),
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
    assert!(prepare_ddl(&database, ALTER_TYPE_SQL).is_table_alter());
    for unsupported in [
        "ALTER TABLE users ALTER COLUMN legacy TYPE BIGINT",
        "ALTER TABLE users ALTER COLUMN legacy SET DATA TYPE BIGINT USING legacy::BIGINT",
        "ALTER TABLE users ALTER COLUMN legacy TYPE BIGINT(8) USING legacy::BIGINT",
    ] {
        assert_eq!(
            database
                .prepare_sql_statement(unsupported, &[])
                .unwrap_err()
                .kind(),
            DatabaseErrorKind::FeatureNotSupported,
            "{unsupported}"
        );
    }
    assert_eq!(
        database
            .prepare_sql_statement(
                "ALTER TABLE users ALTER COLUMN legacy TYPE BIGINT USING $1",
                &[Some(PhysicalType::Int64)],
            )
            .unwrap_err()
            .kind(),
        DatabaseErrorKind::FeatureNotSupported
    );
    assert_eq!(
        database
            .prepare_sql_statement(
                "ALTER TABLE users ALTER COLUMN legacy TYPE BIGINT USING legacy",
                &[],
            )
            .unwrap_err()
            .kind(),
        DatabaseErrorKind::DatatypeMismatch
    );

    assert_eq!(
        database
            .prepare_sql_statement("SELECT true::BIGINT", &[])
            .unwrap_err()
            .kind(),
        DatabaseErrorKind::CannotCoerce
    );

    let mut group = database.begin_group_commit().unwrap();
    let mut member = database.begin_group_member(&group).unwrap();
    assert!(matches!(
        database.execute_in(&mut member, ALTER_TYPE_SQL),
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
    let mut transaction = database.begin_transaction().unwrap();
    assert!(matches!(
        database.execute_in(&mut transaction, ALTER_TYPE_SQL),
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
    let mut accepted = database.begin_transaction().unwrap();
    database.execute_in(&mut accepted, ALTER_TYPE_SQL).unwrap();
    assert_eq!(
        accepted
            .schema_composition
            .plan()
            .unwrap()
            .overlay
            .schema
            .table("users")
            .unwrap()
            .column("legacy")
            .unwrap()
            .id,
        REPLACEMENT
    );
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
    let mut transaction = database.begin_transaction().unwrap();
    database
        .execute_in(&mut transaction, ALTER_TYPE_SQL)
        .unwrap();
    let SchemaCompositionState::TypeConversionReady(conversion) = &transaction.schema_composition
    else {
        panic!("expected sealed type conversion")
    };
    assert!(matches!(
        conversion.source,
        TypeConversionSource::Committed(_)
    ));
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
    database
        .execute_in(&mut transaction, ALTER_TYPE_SQL)
        .unwrap();
    let SchemaCompositionState::TypeConversionReady(conversion) = &transaction.schema_composition
    else {
        panic!("expected sealed type conversion")
    };
    assert!(matches!(
        conversion.source,
        TypeConversionSource::Adopted(_)
    ));
    transaction.rollback().unwrap();
    database.close().unwrap();
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn primary_key_same_physical_and_prior_read_boundaries_are_closed() {
    let path = root("same-physical");
    let mut database = seed(&path, false, false);
    let mut transaction = database.begin_transaction().unwrap();
    assert!(matches!(
        database.execute_in(
            &mut transaction,
            "ALTER TABLE users ALTER COLUMN legacy TYPE TEXT USING legacy",
        ),
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
    assert!(matches!(
        database.execute_in(&mut prior_read, ALTER_TYPE_SQL),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::UnsupportedBackfillRefinement(_)
        ))
    ));
    prior_read.rollback().unwrap();

    let mut other_table_dml = database.begin_transaction().unwrap();
    database
        .execute_in(&mut other_table_dml, "UPDATE seed SET id = id")
        .unwrap();
    assert!(matches!(
        database.execute_in(&mut other_table_dml, ALTER_TYPE_SQL),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::UnsupportedPlacement
        ))
    ));
    other_table_dml.rollback().unwrap();
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
    let mut transaction = database.begin_transaction().unwrap();
    assert!(matches!(
        database.execute_in(
            &mut transaction,
            "ALTER TABLE users ALTER COLUMN legacy TYPE BIGINT USING 0::BIGINT",
        ),
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
fn unnamed_source_index_shape_is_rejected() {
    let path = root("unnamed-index");
    let mut database = seed(&path, false, false);
    database.execute("DROP INDEX users_legacy_idx").unwrap();
    database.create_index(USERS, LEGACY).unwrap();
    let mut transaction = database.begin_transaction().unwrap();
    assert!(matches!(
        database.execute_in(&mut transaction, ALTER_TYPE_SQL),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::UnsupportedSchemaEvolution
        ))
    ));
    assert!(transaction.schema_composition.is_none());
    assert_eq!(transaction.write_participant_count(), 0);
    transaction.rollback().unwrap();
    database.close().unwrap();
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn prepared_dependencies_survive_rollback_and_stale_after_commit() {
    let path = root("prepared-lifecycle");
    let mut database = seed(&path, false, false);
    let PreparedSqlStatement::Relational(old_query) = database
        .prepare_sql_statement("SELECT legacy FROM users ORDER BY id", &[])
        .unwrap()
    else {
        panic!("expected relational statement")
    };
    let ddl = prepare_ddl(&database, ALTER_TYPE_SQL);
    let stale_ddl = ddl.clone();

    let mut rolled_back = database.begin_transaction().unwrap();
    assert_eq!(
        database.execute_ddl_in(&mut rolled_back, &ddl).unwrap(),
        DdlOutcome::Altered
    );
    rolled_back.rollback().unwrap();
    drop(rolled_back);
    let ExecutionResult::Query(result) = database.execute_prepared(&old_query, &[]).unwrap() else {
        panic!("expected query result")
    };
    assert_eq!(
        result.rows,
        vec![
            vec![ScalarValue::Text("43".into())],
            vec![ScalarValue::Text("0".into())],
            vec![ScalarValue::Text("99".into())],
        ]
    );

    assert_eq!(database.execute_ddl(&ddl).unwrap(), DdlOutcome::Altered);
    let storage_after_winner = database.next_storage_id();
    assert!(matches!(
        database.execute_prepared(&old_query, &[]),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::StalePreparedStatement
        ))
    ));
    assert!(matches!(
        database.execute_ddl(&stale_ddl),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::StaleSchemaDependency
        ))
    ));
    assert_eq!(database.next_storage_id(), storage_after_winner);
    database.close().unwrap();
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn alter_type_crash_child() {
    let Ok(path) = std::env::var("NETBADB_ROUND62_CRASH_ROOT") else {
        return;
    };
    let adopted = std::env::var_os("NETBADB_ROUND62_ADOPTED").is_some();
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
    database
        .execute_in(&mut transaction, ALTER_TYPE_SQL)
        .unwrap();
    database.commit_transaction(&mut transaction).unwrap();
    panic!("configured Round 62 crash point was not reached");
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
        ("alter-type-column-reservation-durable", false, false, false),
        ("alter-type-index-reservation-durable", false, false, false),
        ("alter-type-logical-plan-sealed", false, false, false),
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
                "alter_type_using_tests::alter_type_crash_child",
                "--nocapture",
            ])
            .env("NETBADB_ROUND62_CRASH_ROOT", &path);
        if adopted {
            command.env("NETBADB_ROUND62_ADOPTED", "1");
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
#[ignore = "manual Round 62 10K/100K cost observation"]
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
                "__round62_cost_shadow",
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
        let cost = crate::deferred_backfill::synthetic_deferred_cost(
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
            "round62 rows={rows} validation_us={} finalization_us={} known_program_vec_allocations={} metadata_bytes={}",
            cost.validation.as_micros(),
            cost.finalization.as_micros(),
            cost.transient_vector_allocations,
            cost.resident_metadata_bytes,
        );
        std::fs::remove_dir_all(path).unwrap();
    }
}
