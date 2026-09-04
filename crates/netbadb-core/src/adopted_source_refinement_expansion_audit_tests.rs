use std::path::{Path, PathBuf};

use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_types::{ColumnId, IndexId, PhysicalType, ScalarValue, StorageId, TableId};

use super::*;
use crate::schema_composition::SchemaCompositionState;

fn root(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "netbadb-round45-{name}-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    std::fs::create_dir(&path).unwrap();
    path
}

fn seed(root: &Path, email_not_null: bool, with_email_index: bool) -> Database {
    let mut db = Database::create_catalog(
        root.join("catalog"),
        vec![TableStorageCreateSpec::heap(
            root.join("seed.heap"),
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
        Some(DatabaseCoordinatorConfig::new(root.join("coordinator"))),
    )
    .unwrap();
    let nullability = if email_not_null { " NOT NULL" } else { "" };
    db.execute(&format!(
        "CREATE TABLE users (id BIGINT NOT NULL, legacy TEXT, email TEXT{nullability})"
    ))
    .unwrap();
    let first_email = if email_not_null {
        "'one@example.test'"
    } else {
        "NULL"
    };
    db.execute(&format!(
        "INSERT INTO users VALUES (1, 'old-one', {first_email})"
    ))
    .unwrap();
    db.execute("INSERT INTO users VALUES (2, 'old-two', 'two@example.test')")
        .unwrap();
    db.execute("INSERT INTO users VALUES (3, 'old-three', 'three@example.test')")
        .unwrap();
    if with_email_index {
        db.execute("CREATE INDEX users_email_idx ON users(email)")
            .unwrap();
    }
    db
}

fn alter_spec(db: &Database, transaction: Option<&Transaction>, sql: &str) -> AlterTableSpec {
    let prepared = transaction.map_or_else(
        || db.prepare_sql_statement(sql, &[]),
        |transaction| db.prepare_sql_statement_in(transaction, sql, &[]),
    );
    let PreparedSqlStatement::Ddl(prepared) = prepared.unwrap() else {
        panic!("expected prepared DDL");
    };
    let CompiledDdlStatement::AlterTable(statement) = prepared.compiled else {
        panic!("expected prepared ALTER TABLE");
    };
    AlterTableSpec::from(&statement)
}

fn create_index_statement(db: &Database, transaction: &Transaction, sql: &str) -> TypedCreateIndex {
    let PreparedSqlStatement::Ddl(prepared) =
        db.prepare_sql_statement_in(transaction, sql, &[]).unwrap()
    else {
        panic!("expected prepared DDL");
    };
    let CompiledDdlStatement::CreateIndex(statement) = prepared.compiled else {
        panic!("expected prepared CREATE INDEX");
    };
    statement
}

fn journal_bytes(db: &Database) -> Vec<u8> {
    db.mutation_journal
        .as_ref()
        .map(|journal| journal.borrow().encode().unwrap())
        .unwrap_or_default()
}

fn resource_bytes(path: &Path) -> u64 {
    std::fs::read_dir(path)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                resource_bytes(&entry.path())
            } else {
                entry.metadata().unwrap().len()
            }
        })
        .sum()
}

#[test]
fn candidate_a_transaction_visible_set_not_null_matrix() {
    for (name, setup, succeeds) in [
        (
            "own-update-repairs-null",
            "UPDATE users SET email = 'filled@example.test' WHERE email IS NULL",
            true,
        ),
        (
            "own-insert-introduces-null",
            "INSERT INTO users VALUES (4, 'old-four', NULL)",
            false,
        ),
        (
            "own-delete-removes-null",
            "DELETE FROM users WHERE email IS NULL",
            true,
        ),
        (
            "zero-row-leaves-null",
            "UPDATE users SET email = email WHERE id = -1",
            false,
        ),
        ("empty-visible-table", "DELETE FROM users", true),
    ] {
        let root = root(name);
        let mut db = seed(&root, false, true);
        if name == "own-insert-introduces-null" {
            db.execute("UPDATE users SET email = 'one@example.test' WHERE email IS NULL")
                .unwrap();
        }
        let spec = alter_spec(
            &db,
            None,
            "ALTER TABLE users ALTER COLUMN email SET NOT NULL",
        );
        let mut transaction = db.begin_transaction().unwrap();
        db.execute_in(&mut transaction, setup).unwrap();
        let result = db.audit_apply_adopted_source_nullability(&mut transaction, spec);
        if succeeds {
            result.unwrap();
            db.commit_transaction(&mut transaction).unwrap();
            assert!(
                !db.schema()
                    .table("users")
                    .unwrap()
                    .column("email")
                    .unwrap()
                    .nullable
            );
        } else {
            assert!(matches!(
                result,
                Err(DatabaseError::SchemaMutation(
                    SchemaMutationError::NotNullViolation(ColumnId(3))
                ))
            ));
            assert_eq!(transaction.state(), TransactionState::Active);
            assert!(transaction.schema_composition.is_none());
            assert!(db.schema_writer.get().is_none());
            transaction.rollback().unwrap();
        }
        db.close().unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    let root = root("all-visible-non-null");
    let mut db = seed(&root, false, true);
    db.execute("UPDATE users SET email = 'one@example.test' WHERE email IS NULL")
        .unwrap();
    let spec = alter_spec(
        &db,
        None,
        "ALTER TABLE users ALTER COLUMN email SET NOT NULL",
    );
    let mut transaction = db.begin_transaction().unwrap();
    db.execute_in(&mut transaction, "UPDATE users SET email = email")
        .unwrap();
    db.audit_apply_adopted_source_nullability(&mut transaction, spec)
        .unwrap();
    db.commit_transaction(&mut transaction).unwrap();
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn candidate_a_first_failure_is_pre_writer_and_native_repair_retry_works() {
    let root = root("repair-retry");
    let mut db = seed(&root, false, true);
    let spec = alter_spec(
        &db,
        None,
        "ALTER TABLE users ALTER COLUMN email SET NOT NULL",
    );
    let mut transaction = db.begin_transaction().unwrap();
    db.execute_in(
        &mut transaction,
        "UPDATE users SET email = 'one@example.test' WHERE email IS NULL",
    )
    .unwrap();
    db.execute_in(
        &mut transaction,
        "INSERT INTO users VALUES (4, 'old-four', NULL)",
    )
    .unwrap();
    let journal = journal_bytes(&db);
    assert!(matches!(
        db.audit_apply_adopted_source_nullability(&mut transaction, spec.clone()),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::NotNullViolation(ColumnId(3))
        ))
    ));
    assert_eq!(journal_bytes(&db), journal);
    assert_eq!(transaction.state(), TransactionState::Active);
    assert!(transaction.schema_composition.is_none());
    assert!(db.schema_writer.get().is_none());
    db.execute_in(
        &mut transaction,
        "UPDATE users SET email = 'four@example.test' WHERE id = 4",
    )
    .unwrap();
    db.audit_apply_adopted_source_nullability(&mut transaction, spec)
        .unwrap();
    db.commit_transaction(&mut transaction).unwrap();
    assert_eq!(
        db.query("SELECT email FROM users WHERE id = 4")
            .unwrap()
            .rows,
        [vec![ScalarValue::Text("four@example.test".into())]]
    );
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn candidate_a_subsequent_validation_keeps_dml_closed_but_allows_ddl_retry() {
    let root = root("subsequent-validation");
    let mut db = seed(&root, false, true);
    let mut transaction = db.begin_transaction().unwrap();
    db.execute_in(&mut transaction, "UPDATE users SET legacy = legacy")
        .unwrap();
    db.execute_in(&mut transaction, "ALTER TABLE users ADD COLUMN marker TEXT")
        .unwrap();
    let set = alter_spec(
        &db,
        Some(&transaction),
        "ALTER TABLE users ALTER COLUMN email SET NOT NULL",
    );
    assert!(matches!(
        db.audit_apply_adopted_source_nullability(&mut transaction, set),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::NotNullViolation(ColumnId(3))
        ))
    ));
    assert!(matches!(
        db.execute_in(&mut transaction, "UPDATE users SET email = 'filled'"),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::MigrationDataAccessAfterRefinement
        ))
    ));
    db.execute_in(
        &mut transaction,
        "ALTER TABLE users RENAME COLUMN legacy TO old_value",
    )
    .unwrap();
    transaction.rollback().unwrap();
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn candidate_a_subsequent_set_uses_one_final_projection_and_rejects_new_column() {
    let root = root("subsequent-success");
    let mut db = seed(&root, false, true);
    let table = db.schema().table("users").unwrap().id;
    let target = db.next_storage_id().unwrap();
    let mut transaction = db.begin_transaction().unwrap();
    db.execute_in(
        &mut transaction,
        "UPDATE users SET email = 'filled@example.test' WHERE email IS NULL",
    )
    .unwrap();
    db.execute_in(&mut transaction, "ALTER TABLE users ADD COLUMN marker TEXT")
        .unwrap();
    let marker_set = alter_spec(
        &db,
        Some(&transaction),
        "ALTER TABLE users ALTER COLUMN marker SET NOT NULL",
    );
    assert!(matches!(
        db.audit_apply_adopted_source_nullability(&mut transaction, marker_set),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::UnsupportedBackfillRefinement(_)
        ))
    ));
    let email_set = alter_spec(
        &db,
        Some(&transaction),
        "ALTER TABLE users ALTER COLUMN email SET NOT NULL",
    );
    db.audit_apply_adopted_source_nullability(&mut transaction, email_set)
        .unwrap();
    db.finalize_adopted_source(&mut transaction).unwrap();
    let materialized = transaction.schema_composition.source_backfill().unwrap();
    assert_eq!(materialized.source_copy_passes, 1);
    assert_eq!(materialized.source_rows_copied, 3);
    db.commit_transaction(&mut transaction).unwrap();
    assert_eq!(db.bindings.resolve_single(table), Ok(target));
    assert!(
        !db.schema()
            .table("users")
            .unwrap()
            .column("email")
            .unwrap()
            .nullable
    );
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn candidate_a_indexed_and_unindexed_survivors_preserve_identity() {
    for (name, column, column_id, indexed) in [
        ("indexed", "email", ColumnId(3), true),
        ("unindexed", "legacy", ColumnId(2), false),
    ] {
        let root = root(name);
        let mut db = seed(&root, false, true);
        let table = db.schema().table("users").unwrap().id;
        let index = indexed.then(|| db.indexes(table).unwrap()[0].clone());
        let sql = format!("ALTER TABLE users ALTER COLUMN {column} SET NOT NULL");
        let spec = alter_spec(&db, None, &sql);
        assert!(matches!(
            spec.operation,
            AlterTableOperation::SetNotNull { column_id: id } if id == column_id
        ));
        let mut transaction = db.begin_transaction().unwrap();
        db.execute_in(
            &mut transaction,
            &format!("UPDATE users SET {column} = 'filled' WHERE {column} IS NULL"),
        )
        .unwrap();
        db.audit_apply_adopted_source_nullability(&mut transaction, spec)
            .unwrap();
        db.commit_transaction(&mut transaction).unwrap();
        if let Some(index) = index {
            let final_index = &db.indexes(table).unwrap()[0];
            assert_eq!(final_index.id, index.id);
            assert_eq!(final_index.column_id, index.column_id);
            assert_eq!(final_index.name, index.name);
        }
        db.close().unwrap();
        let reopened = Database::open_catalog(root.join("catalog")).unwrap();
        assert_eq!(
            reopened
                .schema()
                .table("users")
                .unwrap()
                .column(column)
                .unwrap()
                .id,
            column_id
        );
        reopened.close().unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    let root = root("indexed-drop-not-null");
    let mut db = seed(&root, true, true);
    let table = db.schema().table("users").unwrap().id;
    let index = db.indexes(table).unwrap()[0].clone();
    let spec = alter_spec(
        &db,
        None,
        "ALTER TABLE users ALTER COLUMN email DROP NOT NULL",
    );
    let mut transaction = db.begin_transaction().unwrap();
    db.execute_in(&mut transaction, "UPDATE users SET email = email")
        .unwrap();
    db.audit_apply_adopted_source_nullability(&mut transaction, spec)
        .unwrap();
    db.commit_transaction(&mut transaction).unwrap();
    assert!(
        db.schema()
            .table("users")
            .unwrap()
            .column("email")
            .unwrap()
            .nullable
    );
    assert_eq!(db.indexes(table).unwrap()[0].id, index.id);
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn candidate_a_nullability_round_trips_are_clean_s1_noops() {
    for (name, base_not_null, first, second) in [
        (
            "set-drop",
            false,
            "ALTER TABLE users ALTER COLUMN email SET NOT NULL",
            "ALTER TABLE users ALTER COLUMN email DROP NOT NULL",
        ),
        (
            "drop-set",
            true,
            "ALTER TABLE users ALTER COLUMN email DROP NOT NULL",
            "ALTER TABLE users ALTER COLUMN email SET NOT NULL",
        ),
    ] {
        let root = root(name);
        let mut db = seed(&root, base_not_null, true);
        let table = db.schema().table("users").unwrap().id;
        let source = db.bindings.resolve_single(table).unwrap();
        let storage_floor = db.next_storage_id();
        let version = db.table_schema_version(table);
        let generation = db.schema_generation();
        let revision = db.catalog_generation();
        let mut transaction = db.begin_transaction().unwrap();
        db.execute_in(
            &mut transaction,
            "UPDATE users SET email = 'filled@example.test' WHERE email IS NULL",
        )
        .unwrap();
        let first = alter_spec(&db, Some(&transaction), first);
        db.audit_apply_adopted_source_nullability(&mut transaction, first)
            .unwrap();
        let second = alter_spec(&db, Some(&transaction), second);
        db.audit_apply_adopted_source_nullability(&mut transaction, second)
            .unwrap();
        db.finalize_adopted_source(&mut transaction).unwrap();
        assert!(matches!(
            transaction.schema_composition,
            SchemaCompositionState::SealedNoEffectiveChange(_)
        ));
        db.commit_transaction(&mut transaction).unwrap();
        assert_eq!(db.bindings.resolve_single(table), Ok(source));
        assert_eq!(db.next_storage_id(), storage_floor);
        assert_eq!(db.table_schema_version(table), version);
        assert_eq!(db.schema_generation(), generation);
        assert_eq!(db.catalog_generation(), revision);
        db.close().unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn candidate_a_prepared_target_is_exact_and_production_remains_closed() {
    let root = root("prepared");
    let mut db = seed(&root, false, true);
    let prepared = db
        .prepare_sql_statement("ALTER TABLE users ALTER COLUMN email SET NOT NULL", &[])
        .unwrap();
    let PreparedSqlStatement::Ddl(prepared) = prepared else {
        panic!("expected DDL");
    };
    let CompiledDdlStatement::AlterTable(statement) = &prepared.compiled else {
        panic!("expected ALTER");
    };
    let exact = AlterTableSpec::from(statement);
    assert_eq!(exact.target.table_id, TableId(2));
    assert!(matches!(
        exact.operation,
        AlterTableOperation::SetNotNull {
            column_id: ColumnId(3)
        }
    ));
    let mut transaction = db.begin_transaction().unwrap();
    db.execute_in(
        &mut transaction,
        "UPDATE users SET email = 'filled@example.test' WHERE email IS NULL",
    )
    .unwrap();
    assert!(matches!(
        db.execute_ddl_in(&mut transaction, &prepared),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::TransactionNotPristine
        ))
    ));
    db.audit_apply_adopted_source_nullability(&mut transaction, exact)
        .unwrap();
    db.commit_transaction(&mut transaction).unwrap();
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn candidate_b_index_reservation_is_durable_but_table_noop_is_not_a_source_rewrite() {
    let root = root("index-reservation");
    let mut db = seed(&root, false, false);
    let table = db.schema().table("users").unwrap().id;
    let mut transaction = db.begin_transaction().unwrap();
    db.execute_in(&mut transaction, "UPDATE users SET legacy = legacy")
        .unwrap();
    db.execute_in(
        &mut transaction,
        "ALTER TABLE users RENAME COLUMN email TO contact",
    )
    .unwrap();
    db.execute_in(
        &mut transaction,
        "ALTER TABLE users RENAME COLUMN contact TO email",
    )
    .unwrap();
    let statement = create_index_statement(
        &db,
        &transaction,
        "CREATE INDEX users_legacy_idx ON users(legacy)",
    );
    db.audit_apply_adopted_source_create_index(&mut transaction, &statement)
        .unwrap();
    let (reserved, floor) = {
        let adopted = match &transaction.schema_composition {
            SchemaCompositionState::AdoptedSourceRefining(adopted) => adopted,
            _ => panic!("expected adopted state"),
        };
        let touched = &adopted.logical.touched[&table];
        let record = adopted.logical.journal.borrow();
        let reservation = record.compositions[&transaction.id()].index_reservations[0].clone();
        assert!(
            record.compositions[&transaction.id()]
                .index_intent
                .is_none()
        );
        assert_eq!(reservation.table, table);
        assert_eq!(reservation.index, IndexId(1));
        let encoded = record.encode().unwrap();
        let decoded =
            crate::schema_mutation_journal::SchemaMutationJournal::decode(&encoded).unwrap();
        assert_eq!(
            decoded.compositions[&transaction.id()].index_reservations,
            std::slice::from_ref(&reservation)
        );
        (reservation.index, touched.base_indexes.next_index_id)
    };
    let finalization_error = db.finalize_adopted_source(&mut transaction).unwrap_err();
    println!("ROUND45_INDEX_ONLY_ERROR {finalization_error:?}");
    assert!(matches!(
        finalization_error,
        DatabaseError::SchemaMutation(SchemaMutationError::Corrupt(_))
    ));
    transaction.rollback().unwrap();
    assert_eq!(
        db.mutation_journal
            .as_ref()
            .unwrap()
            .borrow()
            .effective_index(table, floor),
        Some(IndexId(reserved.0 + 1))
    );
    db.close().unwrap();
    let reopened = Database::open_catalog(root.join("catalog")).unwrap();
    assert_eq!(
        reopened
            .mutation_journal
            .as_ref()
            .unwrap()
            .borrow()
            .effective_index(table, floor),
        Some(IndexId(reserved.0 + 1))
    );
    reopened.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn candidate_b_new_column_index_is_logically_and_physically_feasible_with_s2() {
    let root = root("new-column-index");
    let mut db = seed(&root, false, false);
    let table = db.schema().table("users").unwrap().id;
    let target = db.next_storage_id().unwrap();
    let mut transaction = db.begin_transaction().unwrap();
    db.execute_in(&mut transaction, "UPDATE users SET legacy = legacy")
        .unwrap();
    db.execute_in(&mut transaction, "ALTER TABLE users ADD COLUMN marker TEXT")
        .unwrap();
    let marker = transaction
        .visible_schema(db.schema())
        .table("users")
        .unwrap()
        .column("marker")
        .unwrap()
        .id;
    let statement = create_index_statement(
        &db,
        &transaction,
        "CREATE INDEX users_marker_idx ON users(marker)",
    );
    assert_eq!(statement.column_id, marker);
    db.audit_apply_adopted_source_create_index(&mut transaction, &statement)
        .unwrap();
    db.finalize_adopted_source(&mut transaction).unwrap();
    let materialized = transaction.schema_composition.source_backfill().unwrap();
    assert_eq!(
        (
            materialized.source_copy_passes,
            materialized.source_rows_copied
        ),
        (1, 3)
    );
    db.commit_transaction(&mut transaction).unwrap();
    assert_eq!(db.bindings.resolve_single(table), Ok(target));
    let index = db
        .indexes(table)
        .unwrap()
        .iter()
        .find(|index| index.column_id == marker)
        .unwrap();
    assert_eq!(index.id, IndexId(1));
    assert_eq!(index.name.as_ref().unwrap().as_str(), "users_marker_idx");
    assert_eq!(
        db.query("SELECT id FROM users WHERE marker IS NULL ORDER BY id")
            .unwrap()
            .rows
            .len(),
        3
    );
    db.close().unwrap();
    let reopened = Database::open_catalog(root.join("catalog")).unwrap();
    assert_eq!(reopened.indexes(table).unwrap()[0].id, IndexId(1));
    reopened.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn candidate_c_probes_show_rename_compatibility_and_layout_mismatches() {
    for (name, alter, insert, expected_overlay_width, expected_source_width) in [
        (
            "rename-only",
            "ALTER TABLE users RENAME COLUMN legacy TO old_value",
            "INSERT INTO users VALUES (9, 'nine', NULL)",
            3,
            3,
        ),
        (
            "add-width",
            "ALTER TABLE users ADD COLUMN marker TEXT",
            "INSERT INTO users VALUES (9, 'nine', NULL, NULL)",
            4,
            3,
        ),
        (
            "drop-width",
            "ALTER TABLE users DROP COLUMN legacy",
            "INSERT INTO users VALUES (9, NULL)",
            2,
            3,
        ),
    ] {
        let root = root(name);
        let mut db = seed(&root, false, false);
        let table = db.schema().table("users").unwrap().id;
        let source = db.bindings.resolve_single(table).unwrap();
        let source_ids = db
            .registry
            .get(source)
            .unwrap()
            .table()
            .columns
            .iter()
            .map(|column| column.id)
            .collect::<Vec<_>>();
        let mut transaction = db.begin_transaction().unwrap();
        db.execute_in(&mut transaction, "UPDATE users SET legacy = legacy")
            .unwrap();
        db.execute_in(&mut transaction, alter).unwrap();
        let overlay = transaction
            .visible_schema(db.schema())
            .table("users")
            .unwrap();
        let source_table = db.registry.get(source).unwrap().table();
        assert_eq!(overlay.columns.len(), expected_overlay_width);
        assert_eq!(source_table.columns.len(), expected_source_width);
        if name == "rename-only" {
            assert_eq!(
                overlay
                    .columns
                    .iter()
                    .map(|column| column.id)
                    .collect::<Vec<_>>(),
                source_ids
            );
            assert_eq!(
                overlay
                    .columns
                    .iter()
                    .map(|column| column.semantic_type().physical)
                    .collect::<Vec<_>>(),
                source_table
                    .columns
                    .iter()
                    .map(|column| column.semantic_type().physical)
                    .collect::<Vec<_>>()
            );
        }
        assert!(matches!(
            db.execute_in(&mut transaction, insert),
            Err(DatabaseError::SchemaMutation(
                SchemaMutationError::MigrationDataAccessAfterRefinement
            ))
        ));
        transaction.rollback().unwrap();
        db.close().unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    let root = root("nullability-mismatch");
    let mut db = seed(&root, false, false);
    let table = db.schema().table("users").unwrap().id;
    let source = db.bindings.resolve_single(table).unwrap();
    let mut transaction = db.begin_transaction().unwrap();
    db.execute_in(
        &mut transaction,
        "UPDATE users SET email = 'filled' WHERE email IS NULL",
    )
    .unwrap();
    let spec = alter_spec(
        &db,
        Some(&transaction),
        "ALTER TABLE users ALTER COLUMN email SET NOT NULL",
    );
    db.audit_apply_adopted_source_nullability(&mut transaction, spec)
        .unwrap();
    assert!(
        db.registry
            .get(source)
            .unwrap()
            .table()
            .column("email")
            .unwrap()
            .nullable
    );
    assert!(
        !transaction
            .visible_schema(db.schema())
            .table("users")
            .unwrap()
            .column("email")
            .unwrap()
            .nullable
    );
    transaction.rollback().unwrap();
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn production_round45_surfaces_stay_negative_and_round44_round42_stay_positive() {
    for (name, statements) in [
        (
            "set-negative",
            vec![
                "UPDATE users SET email = email",
                "ALTER TABLE users ALTER COLUMN email SET NOT NULL",
            ],
        ),
        (
            "drop-negative",
            vec![
                "UPDATE users SET email = email",
                "ALTER TABLE users ALTER COLUMN email DROP NOT NULL",
            ],
        ),
    ] {
        let root = root(name);
        let mut db = seed(&root, false, true);
        let mut transaction = db.begin_transaction().unwrap();
        db.execute_in(&mut transaction, statements[0]).unwrap();
        let error = db.execute_in(&mut transaction, statements[1]).unwrap_err();
        assert!(matches!(
            error,
            DatabaseError::SchemaMutation(SchemaMutationError::TransactionNotPristine)
        ));
        transaction.rollback().unwrap();
        db.close().unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    let negative_root = root("index-and-dml-negative");
    let mut db = seed(&negative_root, false, false);
    let mut transaction = db.begin_transaction().unwrap();
    db.execute_in(&mut transaction, "UPDATE users SET legacy = legacy")
        .unwrap();
    db.execute_in(&mut transaction, "ALTER TABLE users ADD COLUMN marker TEXT")
        .unwrap();
    assert!(
        db.execute_in(
            &mut transaction,
            "CREATE INDEX users_marker_idx ON users(marker)"
        )
        .is_err()
    );
    for statement in [
        "SELECT id FROM users",
        "INSERT INTO users VALUES (4, 'four', 'four@example.test', NULL)",
        "UPDATE users SET legacy = legacy",
        "DELETE FROM users WHERE id = 1",
    ] {
        assert!(matches!(
            db.execute_in(&mut transaction, statement),
            Err(DatabaseError::SchemaMutation(
                SchemaMutationError::MigrationDataAccessAfterRefinement
            ))
        ));
    }
    transaction.rollback().unwrap();
    db.close().unwrap();
    std::fs::remove_dir_all(negative_root).unwrap();

    for (name, alter) in [
        ("round44-add", "ALTER TABLE users ADD COLUMN marker TEXT"),
        ("round44-drop", "ALTER TABLE users DROP COLUMN legacy"),
        (
            "round44-rename-table",
            "ALTER TABLE users RENAME TO accounts",
        ),
        (
            "round44-rename-column",
            "ALTER TABLE users RENAME COLUMN legacy TO old_value",
        ),
    ] {
        let root = root(name);
        let mut db = seed(&root, false, false);
        let mut transaction = db.begin_transaction().unwrap();
        db.execute_in(&mut transaction, "UPDATE users SET legacy = legacy")
            .unwrap();
        db.execute_in(&mut transaction, alter).unwrap();
        db.commit_transaction(&mut transaction).unwrap();
        db.close().unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    let root = root("round42");
    let mut db = seed(&root, false, true);
    let mut transaction = db.begin_transaction().unwrap();
    for statement in [
        "DROP INDEX users_email_idx",
        "UPDATE users SET email = 'filled' WHERE email IS NULL",
        "ALTER TABLE users ALTER COLUMN email SET NOT NULL",
        "ALTER TABLE users ADD COLUMN marker TEXT",
        "CREATE INDEX users_email_idx ON users(email)",
    ] {
        db.execute_in(&mut transaction, statement).unwrap();
    }
    db.commit_transaction(&mut transaction).unwrap();
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[derive(Debug)]
struct Observation {
    source_bytes: u64,
    peak_bytes: u64,
    final_bytes: u64,
    target: StorageId,
    passes: u64,
    rows: u64,
}

fn observe(name: &str, mode: &str) -> Observation {
    let root = root(name);
    let mut db = seed(&root, false, false);
    let target = db.next_storage_id().unwrap();
    let mut transaction = db.begin_transaction().unwrap();
    db.execute_in(
        &mut transaction,
        "UPDATE users SET email = 'filled' WHERE email IS NULL",
    )
    .unwrap();
    let source_bytes = resource_bytes(&root);
    match mode {
        "baseline" => {
            db.execute_in(&mut transaction, "ALTER TABLE users ADD COLUMN marker TEXT")
                .unwrap();
        }
        "nullability" => {
            let spec = alter_spec(
                &db,
                Some(&transaction),
                "ALTER TABLE users ALTER COLUMN email SET NOT NULL",
            );
            db.audit_apply_adopted_source_nullability(&mut transaction, spec)
                .unwrap();
        }
        "index" => {
            db.execute_in(&mut transaction, "ALTER TABLE users ADD COLUMN marker TEXT")
                .unwrap();
            let statement = create_index_statement(
                &db,
                &transaction,
                "CREATE INDEX users_marker_idx ON users(marker)",
            );
            db.audit_apply_adopted_source_create_index(&mut transaction, &statement)
                .unwrap();
        }
        _ => panic!("unknown observation mode"),
    }
    db.finalize_adopted_source(&mut transaction).unwrap();
    let materialized = transaction.schema_composition.source_backfill().unwrap();
    let passes = materialized.source_copy_passes;
    let rows = materialized.source_rows_copied;
    let peak_bytes = resource_bytes(&root);
    db.commit_transaction(&mut transaction).unwrap();
    db.close().unwrap();
    let final_bytes = resource_bytes(&root);
    std::fs::remove_dir_all(root).unwrap();
    Observation {
        source_bytes,
        peak_bytes,
        final_bytes,
        target,
        passes,
        rows,
    }
}

#[test]
fn fixed_fixture_observes_baseline_nullability_and_new_column_index() {
    let baseline = observe("observation-baseline", "baseline");
    let nullability = observe("observation-nullability", "nullability");
    let index = observe("observation-index", "index");
    for observation in [&baseline, &nullability, &index] {
        assert_eq!((observation.passes, observation.rows), (1, 3));
        assert_eq!(observation.target, StorageId(3));
        assert!(observation.peak_bytes >= observation.source_bytes);
        assert!(observation.final_bytes >= observation.source_bytes);
    }
    println!("ROUND45_COST baseline={baseline:?} nullability={nullability:?} index={index:?}");
}

fn execute_candidate_a_crash_transaction(db: &mut Database) {
    let mut transaction = db.begin_transaction().unwrap();
    db.execute_in(
        &mut transaction,
        "UPDATE users SET email = 'filled@example.test' WHERE email IS NULL",
    )
    .unwrap();
    let spec = alter_spec(
        db,
        Some(&transaction),
        "ALTER TABLE users ALTER COLUMN email SET NOT NULL",
    );
    db.audit_apply_adopted_source_nullability(&mut transaction, spec)
        .unwrap();
    db.commit_transaction(&mut transaction).unwrap();
}

#[test]
fn candidate_a_crash_child() {
    let Ok(root) = std::env::var("NETBADB_ROUND45_CRASH_ROOT") else {
        return;
    };
    let mut db = Database::open_catalog(Path::new(&root).join("catalog")).unwrap();
    execute_candidate_a_crash_transaction(&mut db);
    panic!("configured Round 45 crash hook was not reached");
}

fn assert_candidate_a_outcome(root: &Path, winner: bool, expected_storage: StorageId) {
    for _ in 0..3 {
        let mut db = Database::open_catalog(root.join("catalog")).unwrap();
        let table = db.schema().table("users").unwrap().id;
        assert_eq!(db.bindings.resolve_single(table), Ok(expected_storage));
        let email = db.schema().table("users").unwrap().column("email").unwrap();
        assert_eq!(email.nullable, !winner);
        assert_eq!(db.indexes(table).unwrap()[0].id, IndexId(1));
        assert_eq!(
            db.query("SELECT email FROM users WHERE id = 1")
                .unwrap()
                .rows,
            [vec![if winner {
                ScalarValue::Text("filled@example.test".into())
            } else {
                ScalarValue::Null
            }]]
        );
        db.close().unwrap();
    }
}

#[test]
fn candidate_a_reuses_round44_pre_and_post_cord_recovery() {
    for point in [
        "composition-intent-durable",
        "source-backfill-intent-durable",
        "source-backfill-mid-copy",
    ] {
        let root = root(point);
        let db = seed(&root, false, true);
        let source = db.bindings.resolve_single(TableId(2)).unwrap();
        db.close().unwrap();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "adopted_source_refinement_expansion_audit_tests::candidate_a_crash_child",
                "--nocapture",
            ])
            .env("NETBADB_ROUND45_CRASH_ROOT", &root)
            .env("NETBADB_BACKFILL_CRASH_POINT", point)
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(90), "{point}");
        assert_candidate_a_outcome(&root, false, source);
        std::fs::remove_dir_all(root).unwrap();
    }

    for (name, point, reverse) in [
        ("both-prepared", "after-durable-decision", false),
        ("source-committed", "after-commit-1", false),
        ("target-committed", "after-commit-1", true),
        ("both-committed", "after-all-commits", false),
    ] {
        let root = root(name);
        let db = seed(&root, false, true);
        let target = db.next_storage_id().unwrap();
        db.close().unwrap();
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "adopted_source_refinement_expansion_audit_tests::candidate_a_crash_child",
                "--nocapture",
            ])
            .env("NETBADB_ROUND45_CRASH_ROOT", &root);
        if reverse {
            command.env("NETBADB_REVERSE_PARTICIPANT_COMMIT", "1");
        }
        coordinator_crash::configure_child(&mut command, name, &root, point);
        let output = command.output().unwrap();
        assert_eq!(
            output.status.code(),
            Some(coordinator_crash::EXIT_CODE),
            "{name}"
        );
        assert_candidate_a_outcome(&root, true, target);
        std::fs::remove_dir_all(root).unwrap();
    }
}
