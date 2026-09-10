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
    AlterTableSpec::try_from(&statement).unwrap()
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
        let mut transaction = db.begin_transaction().unwrap();
        db.execute_in(&mut transaction, setup).unwrap();
        let result = db.execute_in(
            &mut transaction,
            "ALTER TABLE users ALTER COLUMN email SET NOT NULL",
        );
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
    let mut transaction = db.begin_transaction().unwrap();
    db.execute_in(&mut transaction, "UPDATE users SET email = email")
        .unwrap();
    db.execute_in(
        &mut transaction,
        "ALTER TABLE users ALTER COLUMN email SET NOT NULL",
    )
    .unwrap();
    db.commit_transaction(&mut transaction).unwrap();
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn candidate_a_first_failure_is_pre_writer_and_native_repair_retry_works() {
    let root = root("repair-retry");
    let mut db = seed(&root, false, true);
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
    let storage_floor = db.next_storage_id();
    assert!(matches!(
        db.execute_in(
            &mut transaction,
            "ALTER TABLE users ALTER COLUMN email SET NOT NULL"
        ),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::NotNullViolation(ColumnId(3))
        ))
    ));
    assert_eq!(journal_bytes(&db), journal);
    assert_eq!(transaction.state(), TransactionState::Active);
    assert!(transaction.schema_composition.is_none());
    assert!(db.schema_writer.get().is_none());
    assert_eq!(db.next_storage_id(), storage_floor);
    db.execute_in(
        &mut transaction,
        "UPDATE users SET email = 'four@example.test' WHERE id = 4",
    )
    .unwrap();
    db.execute_in(
        &mut transaction,
        "ALTER TABLE users ALTER COLUMN email SET NOT NULL",
    )
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
    assert!(matches!(
        db.execute_in(
            &mut transaction,
            "ALTER TABLE users ALTER COLUMN email SET NOT NULL"
        ),
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
    assert!(matches!(
        db.execute_in(
            &mut transaction,
            "ALTER TABLE users ALTER COLUMN marker SET NOT NULL"
        ),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::TransactionNotPristine
        ))
    ));
    db.execute_in(
        &mut transaction,
        "ALTER TABLE users ALTER COLUMN email SET NOT NULL",
    )
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
        db.execute_in(&mut transaction, &sql).unwrap();
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
    let mut transaction = db.begin_transaction().unwrap();
    db.execute_in(&mut transaction, "UPDATE users SET email = email")
        .unwrap();
    db.execute_in(
        &mut transaction,
        "ALTER TABLE users ALTER COLUMN email DROP NOT NULL",
    )
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
        db.execute_in(&mut transaction, first).unwrap();
        db.execute_in(&mut transaction, second).unwrap();
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
fn candidate_a_prepared_set_target_is_exact_and_executes_in_production() {
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
    let exact = AlterTableSpec::try_from(statement).unwrap();
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
    db.execute_ddl_in(&mut transaction, &prepared).unwrap();
    db.commit_transaction(&mut transaction).unwrap();
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn round46_effective_set_and_drop_publish_once_and_rebuild_final_index_spec() {
    for (name, base_not_null, setup, alter, final_nullable, expected_scans) in [
        (
            "effective-set",
            false,
            "UPDATE users SET email = 'filled@example.test' WHERE email IS NULL",
            "ALTER TABLE users ALTER COLUMN email SET NOT NULL",
            false,
            1,
        ),
        (
            "effective-drop",
            true,
            "UPDATE users SET email = email WHERE id = 1",
            "ALTER TABLE users ALTER COLUMN email DROP NOT NULL",
            true,
            0,
        ),
    ] {
        let root = root(name);
        let catalog = root.join("catalog");
        let mut db = seed(&root, base_not_null, true);
        let table = db.schema().table("users").unwrap().id;
        let source = db.bindings.resolve_single(table).unwrap();
        let target = db.next_storage_id().unwrap();
        let storage_floor = db.next_storage_id().unwrap();
        let version = db.table_schema_version(table).unwrap();
        let generation = db.schema_generation();
        let epoch = schema_catalog_file::load(&catalog).unwrap().epoch;
        let revision = db.catalog_generation();
        let old_index = db.indexes(table).unwrap()[0].clone();
        crate::schema_composition::reset_source_not_null_validation_count();
        let mut transaction = db.begin_transaction().unwrap();
        db.execute_in(&mut transaction, setup).unwrap();
        db.execute_in(&mut transaction, alter).unwrap();
        assert_eq!(db.next_storage_id(), Some(storage_floor));
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
        assert_eq!(
            crate::schema_composition::source_not_null_validation_count(),
            expected_scans
        );
        assert_eq!(db.bindings.resolve_single(table), Ok(target));
        assert_ne!(source, target);
        assert_eq!(db.next_storage_id(), Some(StorageId(storage_floor.0 + 1)));
        assert_eq!(
            db.table_schema_version(table),
            Some(TableSchemaVersion(version.0 + 1))
        );
        assert_eq!(db.schema_generation(), SchemaGeneration(generation.0 + 1));
        assert_eq!(
            schema_catalog_file::load(&catalog).unwrap().epoch,
            epoch + 1
        );
        assert_eq!(db.catalog_generation(), revision + 1);
        let final_column = db.schema().table("users").unwrap().column("email").unwrap();
        assert_eq!(
            (final_column.id, final_column.nullable),
            (ColumnId(3), final_nullable)
        );
        let final_index = &db.indexes(table).unwrap()[0];
        assert_eq!(
            (final_index.id, final_index.column_id, &final_index.name),
            (old_index.id, old_index.column_id, &old_index.name)
        );
        assert_ne!(final_index.handle, old_index.handle);
        db.close().unwrap();

        for _ in 0..3 {
            let mut reopened = Database::open_catalog(&catalog).unwrap();
            let final_table = reopened.schema().table("users").unwrap().clone();
            let final_storage = reopened.bindings.resolve_single(table).unwrap();
            let final_indexes = reopened
                .registry
                .get_mut(final_storage)
                .unwrap()
                .heap_rewrite_indexes()
                .unwrap();
            reopened
                .registry
                .get_mut(final_storage)
                .unwrap()
                .validate_heap_rewrite_index_inventory(&final_table, &final_indexes)
                .unwrap();
            assert_eq!(reopened.indexes(table).unwrap()[0].id, old_index.id);
            reopened.close().unwrap();
        }
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn round46_indexed_nullability_round_trips_are_physical_s1_noops() {
    for (name, base_not_null, first, second, expected_scans) in [
        (
            "indexed-set-drop",
            false,
            "ALTER TABLE users ALTER COLUMN email SET NOT NULL",
            "ALTER TABLE users ALTER COLUMN email DROP NOT NULL",
            1,
        ),
        (
            "indexed-drop-set",
            true,
            "ALTER TABLE users ALTER COLUMN email DROP NOT NULL",
            "ALTER TABLE users ALTER COLUMN email SET NOT NULL",
            1,
        ),
    ] {
        let root = root(name);
        let catalog = root.join("catalog");
        let mut db = seed(&root, base_not_null, true);
        let table = db.schema().table("users").unwrap().id;
        let source = db.bindings.resolve_single(table).unwrap();
        let storage_floor = db.next_storage_id();
        let version = db.table_schema_version(table);
        let generation = db.schema_generation();
        let epoch = schema_catalog_file::load(&catalog).unwrap().epoch;
        let revision = db.catalog_generation();
        let old_index = db.indexes(table).unwrap()[0].clone();
        crate::schema_composition::reset_source_not_null_validation_count();
        let mut transaction = db.begin_transaction().unwrap();
        db.execute_in(
            &mut transaction,
            "UPDATE users SET email = 'filled@example.test' WHERE email IS NULL",
        )
        .unwrap();
        db.execute_in(&mut transaction, first).unwrap();
        db.execute_in(&mut transaction, second).unwrap();
        db.finalize_adopted_source(&mut transaction).unwrap();
        assert!(matches!(
            transaction.schema_composition,
            SchemaCompositionState::SealedNoEffectiveChange(_)
        ));
        db.commit_transaction(&mut transaction).unwrap();
        assert_eq!(
            crate::schema_composition::source_not_null_validation_count(),
            expected_scans
        );
        assert_eq!(db.bindings.resolve_single(table), Ok(source));
        assert_eq!(db.next_storage_id(), storage_floor);
        assert_eq!(db.table_schema_version(table), version);
        assert_eq!(db.schema_generation(), generation);
        assert_eq!(schema_catalog_file::load(&catalog).unwrap().epoch, epoch);
        assert_eq!(db.catalog_generation(), revision);
        assert_eq!(db.indexes(table).unwrap()[0], old_index);
        db.close().unwrap();
        let reopened = Database::open_catalog(&catalog).unwrap();
        assert_eq!(reopened.indexes(table).unwrap()[0], old_index);
        reopened.close().unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn round46_composes_survivor_nullability_with_layout_and_combined_dml() {
    for (name, before, after, final_table, final_column) in [
        (
            "rename-then-set",
            "ALTER TABLE users RENAME COLUMN email TO contact",
            "ALTER TABLE users ALTER COLUMN contact SET NOT NULL",
            "users",
            "contact",
        ),
        (
            "set-then-rename",
            "ALTER TABLE users ALTER COLUMN email SET NOT NULL",
            "ALTER TABLE users RENAME COLUMN email TO contact",
            "users",
            "contact",
        ),
        (
            "add-then-set",
            "ALTER TABLE users ADD COLUMN marker TEXT",
            "ALTER TABLE users ALTER COLUMN email SET NOT NULL",
            "users",
            "email",
        ),
        (
            "drop-then-set",
            "ALTER TABLE users DROP COLUMN legacy",
            "ALTER TABLE users ALTER COLUMN email SET NOT NULL",
            "users",
            "email",
        ),
    ] {
        let root = root(name);
        let mut db = seed(&root, false, true);
        let table = db.schema().table("users").unwrap().id;
        let index = db.indexes(table).unwrap()[0].clone();
        let mut transaction = db.begin_transaction().unwrap();
        for statement in [
            "UPDATE users SET email = 'filled@example.test' WHERE id = 1",
            "INSERT INTO users VALUES (4, 'old-four', 'four@example.test')",
            "DELETE FROM users WHERE id = 2",
            before,
            after,
        ] {
            db.execute_in(&mut transaction, statement).unwrap();
        }
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
        let final_def = db.schema().table(final_table).unwrap();
        assert_eq!(final_def.column(final_column).unwrap().id, ColumnId(3));
        assert!(!final_def.column(final_column).unwrap().nullable);
        if name == "add-then-set" {
            assert!(final_def.column("marker").unwrap().nullable);
        }
        if name == "drop-then-set" {
            assert!(final_def.column("legacy").is_none());
        }
        let final_index = &db.indexes(table).unwrap()[0];
        assert_eq!(
            (final_index.id, final_index.column_id, &final_index.name),
            (index.id, ColumnId(3), &index.name)
        );
        assert_eq!(
            db.query(&format!(
                "SELECT id, {final_column} FROM {final_table} ORDER BY id"
            ))
            .unwrap()
            .rows,
            vec![
                vec![
                    ScalarValue::Int64(1),
                    ScalarValue::Text("filled@example.test".into())
                ],
                vec![
                    ScalarValue::Int64(3),
                    ScalarValue::Text("three@example.test".into())
                ],
                vec![
                    ScalarValue::Int64(4),
                    ScalarValue::Text("four@example.test".into())
                ],
            ]
        );
        assert_eq!(
            db.query(&format!(
                "SELECT id FROM {final_table} WHERE {final_column} = 'four@example.test'"
            ))
            .unwrap()
            .rows,
            [vec![ScalarValue::Int64(4)]]
        );
        db.close().unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn round46_prepared_drop_and_overlay_dependency_remain_exact() {
    let drop_root = root("prepared-drop");
    let mut db = seed(&drop_root, true, true);
    let PreparedSqlStatement::Ddl(drop_not_null) = db
        .prepare_sql_statement("ALTER TABLE users ALTER COLUMN email DROP NOT NULL", &[])
        .unwrap()
    else {
        panic!("expected DDL");
    };
    let mut transaction = db.begin_transaction().unwrap();
    db.execute_in(
        &mut transaction,
        "UPDATE users SET email = email WHERE id = 1",
    )
    .unwrap();
    db.execute_ddl_in(&mut transaction, &drop_not_null).unwrap();
    db.commit_transaction(&mut transaction).unwrap();
    assert!(
        db.schema()
            .table("users")
            .unwrap()
            .column("email")
            .unwrap()
            .nullable
    );
    db.close().unwrap();
    std::fs::remove_dir_all(drop_root).unwrap();

    let root = root("prepared-overlay-stale");
    let mut db = seed(&root, false, true);
    let PreparedSqlStatement::Ddl(old_set) = db
        .prepare_sql_statement("ALTER TABLE users ALTER COLUMN email SET NOT NULL", &[])
        .unwrap()
    else {
        panic!("expected DDL");
    };
    let mut transaction = db.begin_transaction().unwrap();
    db.execute_in(
        &mut transaction,
        "UPDATE users SET email = 'filled' WHERE email IS NULL",
    )
    .unwrap();
    db.execute_in(&mut transaction, "ALTER TABLE users ADD COLUMN marker TEXT")
        .unwrap();
    assert!(matches!(
        db.execute_ddl_in(&mut transaction, &old_set),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::StaleSchemaDependency
        ))
    ));
    assert!(matches!(
        transaction.schema_composition,
        SchemaCompositionState::AdoptedSourceRefining(_)
    ));
    let PreparedSqlStatement::Ddl(new_set) = db
        .prepare_sql_statement_in(
            &transaction,
            "ALTER TABLE users ALTER COLUMN email SET NOT NULL",
            &[],
        )
        .unwrap()
    else {
        panic!("expected DDL");
    };
    let CompiledDdlStatement::AlterTable(statement) = &new_set.compiled else {
        panic!("expected ALTER");
    };
    assert!(matches!(
        statement.operation,
        netbadb_compiler::TypedAlterTableOperation::SetNotNull {
            column_id: ColumnId(3)
        }
    ));
    db.execute_ddl_in(&mut transaction, &new_set).unwrap();
    db.commit_transaction(&mut transaction).unwrap();
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
fn round46_new_column_nullability_and_dml_remain_closed() {
    for operation in ["SET NOT NULL", "DROP NOT NULL"] {
        let root = root(if operation.starts_with("SET") {
            "cnew-set"
        } else {
            "cnew-drop"
        });
        let mut db = seed(&root, false, true);
        let mut transaction = db.begin_transaction().unwrap();
        db.execute_in(&mut transaction, "UPDATE users SET legacy = legacy")
            .unwrap();
        db.execute_in(&mut transaction, "ALTER TABLE users ADD COLUMN marker TEXT")
            .unwrap();
        let result = db.execute_in(
            &mut transaction,
            &format!("ALTER TABLE users ALTER COLUMN marker {operation}"),
        );
        if operation.starts_with("SET") {
            assert!(
                matches!(
                    result,
                    Err(DatabaseError::SchemaMutation(
                        SchemaMutationError::TransactionNotPristine
                    ))
                ),
                "{operation}: {result:?}"
            );
        } else {
            assert!(
                matches!(
                    result,
                    Err(DatabaseError::SchemaMutation(
                        SchemaMutationError::InvalidSchemaEvolution("column is already nullable")
                    ))
                ),
                "{operation}: {result:?}"
            );
        }
        transaction.rollback().unwrap();
        db.close().unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    for (name, base_not_null, alter) in [
        (
            "closed-set",
            false,
            "ALTER TABLE users ALTER COLUMN email SET NOT NULL",
        ),
        (
            "closed-drop",
            true,
            "ALTER TABLE users ALTER COLUMN email DROP NOT NULL",
        ),
    ] {
        let root = root(name);
        let mut db = seed(&root, base_not_null, true);
        let mut transaction = db.begin_transaction().unwrap();
        db.execute_in(
            &mut transaction,
            "UPDATE users SET email = 'filled' WHERE email IS NULL",
        )
        .unwrap();
        db.execute_in(&mut transaction, alter).unwrap();
        for statement in [
            "SELECT id FROM users",
            "INSERT INTO users VALUES (4, 'four', 'four@example.test')",
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
        for statement in [
            "CREATE INDEX users_legacy_idx ON users(legacy)",
            "DROP INDEX users_email_idx",
        ] {
            db.execute_in(&mut transaction, statement).unwrap();
            assert!(matches!(
                transaction.schema_composition,
                SchemaCompositionState::AdoptedSourceIndexFinalizing(_)
            ));
        }
        transaction.rollback().unwrap();
        db.close().unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn round46_writer_contention_fails_before_validation_and_retries_cleanly() {
    let root = root("writer-contention");
    let mut db = seed(&root, false, true);
    let journal = journal_bytes(&db);
    let mut writer = db.begin_transaction().unwrap();
    db.execute_in(
        &mut writer,
        "UPDATE users SET email = 'filled' WHERE email IS NULL",
    )
    .unwrap();
    let mut blocker = db.begin_transaction().unwrap();
    db.execute_in(&mut blocker, "SELECT id FROM users").unwrap();
    crate::schema_composition::reset_source_not_null_validation_count();
    assert!(matches!(
        db.execute_in(
            &mut writer,
            "ALTER TABLE users ALTER COLUMN email SET NOT NULL"
        ),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::SchemaBusy
        ))
    ));
    assert_eq!(
        crate::schema_composition::source_not_null_validation_count(),
        0
    );
    assert_eq!(writer.state(), TransactionState::Active);
    assert!(writer.schema_composition.is_none());
    assert!(db.schema_writer.get().is_none());
    assert_eq!(journal_bytes(&db), journal);
    blocker.rollback().unwrap();
    drop(blocker);
    db.execute_in(
        &mut writer,
        "ALTER TABLE users ALTER COLUMN email SET NOT NULL",
    )
    .unwrap();
    assert_eq!(
        crate::schema_composition::source_not_null_validation_count(),
        1
    );
    db.commit_transaction(&mut writer).unwrap();
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn round46_retains_cross_table_read_only_pending_index_and_pristine_routing() {
    let root = root("eligibility-boundaries");
    let mut db = seed(&root, false, true);
    db.execute("CREATE TABLE teams (id BIGINT NOT NULL, name TEXT)")
        .unwrap();
    db.execute("INSERT INTO teams VALUES (1, 'one')").unwrap();

    let mut read_only = db.begin_transaction().unwrap();
    db.execute_in(&mut read_only, "SELECT id FROM users")
        .unwrap();
    assert_eq!(
        db.execute_in(
            &mut read_only,
            "ALTER TABLE users ALTER COLUMN legacy SET NOT NULL"
        )
        .unwrap_err()
        .kind(),
        DatabaseErrorKind::TransactionState
    );
    assert!(read_only.schema_composition.is_none());
    read_only.rollback().unwrap();
    drop(read_only);

    let mut cross_table = db.begin_transaction().unwrap();
    db.execute_in(
        &mut cross_table,
        "UPDATE users SET email = 'filled' WHERE email IS NULL",
    )
    .unwrap();
    db.execute_in(&mut cross_table, "SELECT id FROM teams")
        .unwrap();
    assert_eq!(
        db.execute_in(
            &mut cross_table,
            "ALTER TABLE users ALTER COLUMN email SET NOT NULL"
        )
        .unwrap_err()
        .kind(),
        DatabaseErrorKind::FeatureNotSupported
    );
    assert!(cross_table.schema_composition.is_none());
    cross_table.rollback().unwrap();
    drop(cross_table);

    let mut pending_index = db.begin_transaction().unwrap();
    db.execute_in(
        &mut pending_index,
        "UPDATE users SET email = 'filled' WHERE email IS NULL",
    )
    .unwrap();
    db.execute_in(
        &mut pending_index,
        "CREATE INDEX users_legacy_idx ON users(legacy)",
    )
    .unwrap();
    assert_eq!(
        db.execute_in(
            &mut pending_index,
            "ALTER TABLE users ALTER COLUMN email SET NOT NULL"
        )
        .unwrap_err()
        .kind(),
        DatabaseErrorKind::FeatureNotSupported
    );
    assert!(pending_index.schema_composition.is_none());
    pending_index.rollback().unwrap();
    drop(pending_index);

    db.execute("ALTER TABLE users ALTER COLUMN legacy SET NOT NULL")
        .unwrap();
    assert!(
        !db.schema()
            .table("users")
            .unwrap()
            .column("legacy")
            .unwrap()
            .nullable
    );
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn round46_set_and_drop_rollback_restore_exact_s1() {
    for (name, base_not_null, alter) in [
        (
            "rollback-set",
            false,
            "ALTER TABLE users ALTER COLUMN email SET NOT NULL",
        ),
        (
            "rollback-drop",
            true,
            "ALTER TABLE users ALTER COLUMN email DROP NOT NULL",
        ),
    ] {
        let root = root(name);
        let mut db = seed(&root, base_not_null, true);
        let table = db.schema().table("users").unwrap().id;
        let source = db.bindings.resolve_single(table).unwrap();
        let schema = db.schema().clone();
        let indexes = db.indexes(table).unwrap().to_vec();
        let storage_floor = db.next_storage_id();
        let mut transaction = db.begin_transaction().unwrap();
        db.execute_in(
            &mut transaction,
            "UPDATE users SET email = 'filled' WHERE email IS NULL",
        )
        .unwrap();
        db.execute_in(&mut transaction, alter).unwrap();
        transaction.rollback().unwrap();
        assert!(db.schema_writer.get().is_none());
        assert_eq!(db.schema(), &schema);
        assert_eq!(db.bindings.resolve_single(table), Ok(source));
        assert_eq!(db.indexes(table).unwrap(), indexes);
        assert_eq!(db.next_storage_id(), storage_floor);
        assert_eq!(
            db.query("SELECT email FROM users WHERE id = 1")
                .unwrap()
                .rows,
            [vec![if base_not_null {
                ScalarValue::Text("one@example.test".into())
            } else {
                ScalarValue::Null
            }]]
        );
        db.close().unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn final_index_reservation_is_durable_and_table_noop_uses_in_place_delta() {
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
    db.compose_create_index_in(&mut transaction, &statement)
        .unwrap();
    let (reserved, floor) = {
        let adopted = match &transaction.schema_composition {
            SchemaCompositionState::AdoptedSourceIndexFinalizing(adopted) => adopted,
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
    db.finalize_adopted_source(&mut transaction).unwrap();
    assert!(matches!(
        transaction.schema_composition,
        SchemaCompositionState::MaterializedIndex(_)
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
    db.compose_create_index_in(&mut transaction, &statement)
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
    db.execute_in(
        &mut transaction,
        "ALTER TABLE users ALTER COLUMN email SET NOT NULL",
    )
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
fn production_round46_nullability_and_round44_round42_stay_positive() {
    for (name, base_not_null, setup, alter) in [
        (
            "set-positive",
            false,
            "UPDATE users SET email = 'filled' WHERE email IS NULL",
            "ALTER TABLE users ALTER COLUMN email SET NOT NULL",
        ),
        (
            "drop-positive",
            true,
            "UPDATE users SET email = email",
            "ALTER TABLE users ALTER COLUMN email DROP NOT NULL",
        ),
    ] {
        let root = root(name);
        let mut db = seed(&root, base_not_null, true);
        let mut transaction = db.begin_transaction().unwrap();
        db.execute_in(&mut transaction, setup).unwrap();
        db.execute_in(&mut transaction, alter).unwrap();
        db.commit_transaction(&mut transaction).unwrap();
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
    db.execute_in(
        &mut transaction,
        "CREATE INDEX users_marker_idx ON users(marker)",
    )
    .unwrap();
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
    validation_scans: u64,
}

fn observe(name: &str, mode: &str) -> Observation {
    let root = root(name);
    let mut db = seed(&root, mode == "drop", false);
    let target = db.next_storage_id().unwrap();
    crate::schema_composition::reset_source_not_null_validation_count();
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
            db.execute_in(
                &mut transaction,
                "ALTER TABLE users ALTER COLUMN email SET NOT NULL",
            )
            .unwrap();
        }
        "drop" => {
            db.execute_in(
                &mut transaction,
                "ALTER TABLE users ALTER COLUMN email DROP NOT NULL",
            )
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
            db.compose_create_index_in(&mut transaction, &statement)
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
    let validation_scans = crate::schema_composition::source_not_null_validation_count();
    std::fs::remove_dir_all(root).unwrap();
    Observation {
        source_bytes,
        peak_bytes,
        final_bytes,
        target,
        passes,
        rows,
        validation_scans,
    }
}

#[test]
fn fixed_fixture_observes_baseline_nullability_and_new_column_index() {
    let baseline = observe("observation-baseline", "baseline");
    let nullability = observe("observation-nullability", "nullability");
    let drop = observe("observation-drop-nullability", "drop");
    let index = observe("observation-index", "index");
    for observation in [&baseline, &nullability, &drop, &index] {
        assert_eq!((observation.passes, observation.rows), (1, 3));
        assert_eq!(observation.target, StorageId(3));
        assert!(observation.peak_bytes >= observation.source_bytes);
        assert!(observation.final_bytes >= observation.source_bytes);
    }
    assert_eq!(nullability.validation_scans, 1);
    assert_eq!(drop.validation_scans, 0);
    println!(
        "ROUND46_COST baseline={baseline:?} set={nullability:?} drop={drop:?} index={index:?}"
    );
}

fn execute_candidate_a_crash_transaction(db: &mut Database) {
    let mut transaction = db.begin_transaction().unwrap();
    db.execute_in(
        &mut transaction,
        "UPDATE users SET email = 'filled@example.test' WHERE email IS NULL",
    )
    .unwrap();
    db.execute_in(
        &mut transaction,
        "ALTER TABLE users ALTER COLUMN email SET NOT NULL",
    )
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
        "post-dml-adoption-preflight-complete",
        "post-dml-not-null-validation-complete",
        "post-dml-adopted-source-installed",
        "post-dml-first-refinement-accepted",
        "composition-intent-durable",
        "source-backfill-intent-durable",
        "source-backfill-stage-intent-durable",
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
