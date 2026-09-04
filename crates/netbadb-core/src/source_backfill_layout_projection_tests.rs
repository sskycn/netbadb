use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_types::{ColumnId, PhysicalType, ScalarValue, SemanticType, StorageId, TableId};

use super::*;
use crate::schema_composition::{RowProjection, RowProjectionEntry, SchemaCompositionState};
use crate::schema_mutation_journal::CompositionColumnReservation;

fn root(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "netbadb-round41-{name}-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    std::fs::create_dir(&path).unwrap();
    path
}

fn seed(root: &Path, rows: bool) -> Database {
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
    db.execute("CREATE TABLE users (id BIGINT NOT NULL, legacy TEXT, email TEXT)")
        .unwrap();
    if rows {
        db.execute("INSERT INTO users VALUES (1, 'old-one', NULL)")
            .unwrap();
        db.execute("INSERT INTO users VALUES (2, 'old-two', 'two@example.test')")
            .unwrap();
        db.execute("INSERT INTO users VALUES (3, 'old-three', 'three@example.test')")
            .unwrap();
    }
    db.execute("CREATE INDEX users_legacy_idx ON users(legacy)")
        .unwrap();
    db.execute("CREATE INDEX users_email_idx ON users(email)")
        .unwrap();
    db
}

fn drop_source_indexes(db: &mut Database, transaction: &mut Transaction) {
    db.execute_in(transaction, "DROP INDEX users_legacy_idx")
        .unwrap();
    db.execute_in(transaction, "DROP INDEX users_email_idx")
        .unwrap();
}

fn apply_layout(
    db: &mut Database,
    transaction: &mut Transaction,
    operation: AlterTableOperation,
) -> Result<(), DatabaseError> {
    let target = db.resolve_alter_table_in(transaction, "users")?;
    db.apply_source_backfill_layout_refinement(transaction, AlterTableSpec::new(target, operation))
}

#[test]
fn production_projection_is_column_id_checked_target_ordered_and_null_synthesizing() {
    let source = TableDef::new(
        TableId(41),
        "rows",
        vec![
            ColumnDef::new(ColumnId(1), "id", TypeSpec::Physical(PhysicalType::Int64)),
            ColumnDef::new(
                ColumnId(2),
                "legacy",
                TypeSpec::Physical(PhysicalType::Text),
            ),
            ColumnDef::new(ColumnId(3), "email", TypeSpec::Physical(PhysicalType::Text)),
        ],
    );
    let target = TableDef::new(
        TableId(41),
        "rows",
        vec![
            source.columns[0].clone(),
            ColumnDef::new(
                ColumnId(3),
                "canonical_email",
                TypeSpec::Physical(PhysicalType::Text),
            ),
            ColumnDef::new(
                ColumnId(4),
                "legacy",
                TypeSpec::Physical(PhysicalType::Text),
            ),
        ],
    );
    let projection = RowProjection::build(
        &source,
        TableSchemaVersion(1),
        &target,
        TableSchemaVersion(2),
        &BTreeSet::from([ColumnId(4)]),
    )
    .unwrap();
    assert_eq!(
        projection.target_entries,
        vec![
            RowProjectionEntry::Source {
                column_id: ColumnId(1),
                source_position: 0,
                target_nullable: false,
            },
            RowProjectionEntry::Source {
                column_id: ColumnId(3),
                source_position: 2,
                target_nullable: false,
            },
            RowProjectionEntry::SynthesizedNull {
                column_id: ColumnId(4),
                target_nullable: false,
            },
        ]
    );
    assert!(matches!(
        projection.project(&[
            ScalarValue::Int64(7),
            ScalarValue::Text("must-not-leak".into()),
            ScalarValue::Text("seven@example.test".into()),
        ]),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::NotNullViolation(ColumnId(4))
        ))
    ));

    let incompatible = TableDef::new(
        TableId(41),
        "rows",
        vec![ColumnDef::new(
            ColumnId(1),
            "id",
            TypeSpec::Physical(PhysicalType::Text),
        )],
    );
    assert!(matches!(
        RowProjection::build(
            &source,
            TableSchemaVersion(1),
            &incompatible,
            TableSchemaVersion(2),
            &BTreeSet::new(),
        ),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::UnsupportedSchemaEvolution
        ))
    ));
}

#[test]
fn core_layout_refinement_projects_own_writes_once_and_preserves_identity_rules() {
    let root = root("end-to-end");
    let mut db = seed(&root, true);
    let catalog_path = root.join("catalog");
    let table = db.schema().table("users").unwrap().id;
    let source_storage = db.bindings.resolve_single(table).unwrap();
    let source_version = db.table_schema_version(table).unwrap();
    let source_generation = db.schema_generation();
    let source_epoch = schema_catalog_file::load(&catalog_path).unwrap().epoch;
    let source_revision = db.catalog_generation();
    let first_new_column = db.next_column_id(table).unwrap();
    assert_eq!(first_new_column, ColumnId(4));
    let target_storage = db.next_storage_id().unwrap();
    let prepared_before = db
        .prepare_statement("SELECT legacy FROM users", &[])
        .unwrap();

    let mut transaction = db.begin_transaction().unwrap();
    drop_source_indexes(&mut db, &mut transaction);
    db.execute_in(
        &mut transaction,
        "UPDATE users SET email = 'filled@example.test' WHERE email IS NULL",
    )
    .unwrap();
    db.execute_in(
        &mut transaction,
        "INSERT INTO users VALUES (4, 'old-four', 'four@example.test')",
    )
    .unwrap();
    db.execute_in(&mut transaction, "DELETE FROM users WHERE id = 2")
        .unwrap();
    apply_layout(
        &mut db,
        &mut transaction,
        AlterTableOperation::DropColumn {
            column_id: ColumnId(2),
        },
    )
    .unwrap();
    apply_layout(
        &mut db,
        &mut transaction,
        AlterTableOperation::AddNullableColumn {
            name: "legacy".into(),
            data_type: SemanticType::physical(PhysicalType::Text),
        },
    )
    .unwrap();
    assert!(matches!(
        db.execute_in(&mut transaction, "DELETE FROM users WHERE id = 3"),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::MigrationDataAccessAfterRefinement
        ))
    ));
    assert_eq!(db.next_column_id(table), Some(ColumnId(5)));
    assert_eq!(db.next_storage_id(), Some(target_storage));
    db.execute_in(
        &mut transaction,
        "ALTER TABLE users ALTER COLUMN email SET NOT NULL",
    )
    .unwrap();
    db.execute_in(
        &mut transaction,
        "ALTER TABLE users RENAME COLUMN email TO canonical_email",
    )
    .unwrap();
    db.execute_in(
        &mut transaction,
        "CREATE INDEX users_email_idx ON users(canonical_email)",
    )
    .unwrap();

    db.finalize_source_backfill(&mut transaction).unwrap();
    let materialized = transaction.schema_composition.source_backfill().unwrap();
    assert_eq!(materialized.source_copy_passes, 1);
    assert_eq!(materialized.source_rows_copied, 3);
    assert_eq!(db.next_storage_id(), Some(StorageId(target_storage.0 + 1)));
    db.commit_transaction(&mut transaction).unwrap();

    let final_table = db.schema().table("users").unwrap();
    assert_eq!(final_table.id, table);
    assert_eq!(
        final_table
            .columns
            .iter()
            .map(|column| column.id)
            .collect::<Vec<_>>(),
        vec![ColumnId(1), ColumnId(3), ColumnId(4)]
    );
    assert_eq!(final_table.column("legacy").unwrap().id, ColumnId(4));
    assert_eq!(db.bindings.resolve_single(table), Ok(target_storage));
    assert_ne!(target_storage, source_storage);
    assert_eq!(
        db.table_schema_version(table),
        Some(TableSchemaVersion(source_version.0 + 1))
    );
    assert_eq!(db.schema_generation().0, source_generation.0 + 1);
    assert_eq!(
        schema_catalog_file::load(&catalog_path).unwrap().epoch,
        source_epoch + 1
    );
    assert_eq!(db.catalog_generation(), source_revision + 1);
    assert!(matches!(
        db.validate_prepared_dependencies(&prepared_before, None),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::StalePreparedStatement
        ))
    ));
    assert_eq!(db.indexes(table).unwrap().len(), 1);
    assert_eq!(
        db.query("SELECT id, canonical_email, legacy FROM users ORDER BY id")
            .unwrap()
            .rows,
        vec![
            vec![
                ScalarValue::Int64(1),
                ScalarValue::Text("filled@example.test".into()),
                ScalarValue::Null,
            ],
            vec![
                ScalarValue::Int64(3),
                ScalarValue::Text("three@example.test".into()),
                ScalarValue::Null,
            ],
            vec![
                ScalarValue::Int64(4),
                ScalarValue::Text("four@example.test".into()),
                ScalarValue::Null,
            ],
        ]
    );
    db.close().unwrap();
    for _ in 0..3 {
        let mut reopened = Database::open_catalog(&catalog_path).unwrap();
        assert_eq!(reopened.bindings.resolve_single(table), Ok(target_storage));
        assert_eq!(reopened.next_column_id(table), Some(ColumnId(5)));
        assert_eq!(
            reopened
                .query("SELECT legacy FROM users")
                .unwrap()
                .rows
                .len(),
            3
        );
        reopened.close().unwrap();
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn late_add_drop_noop_burns_column_without_allocating_a_target_heap() {
    let root = root("noop");
    let mut db = seed(&root, true);
    let table = db.schema().table("users").unwrap().id;
    let source_storage = db.bindings.resolve_single(table).unwrap();
    let version = db.table_schema_version(table).unwrap();
    let generation = db.schema_generation();
    let revision = db.catalog_generation();
    let storage_floor = db.next_storage_id();
    let column = db.next_column_id(table).unwrap();
    let mut transaction = db.begin_transaction().unwrap();
    let transaction_id = transaction.id();
    drop_source_indexes(&mut db, &mut transaction);
    db.execute_in(
        &mut transaction,
        "UPDATE users SET email = 'changed@example.test' WHERE id = 3",
    )
    .unwrap();
    db.execute_in(
        &mut transaction,
        "ALTER TABLE users ADD COLUMN temporary BOOLEAN",
    )
    .unwrap();
    db.execute_in(&mut transaction, "ALTER TABLE users DROP COLUMN temporary")
        .unwrap();
    db.finalize_source_backfill(&mut transaction).unwrap();
    assert!(matches!(
        transaction.schema_composition,
        SchemaCompositionState::MaterializedIndex(_)
    ));
    assert_eq!(db.next_storage_id(), storage_floor);
    let journal = db.mutation_journal.as_ref().unwrap().borrow();
    assert!(!journal.stage_intents.contains_key(&transaction_id));
    assert!(
        !journal
            .source_backfill_intents
            .contains_key(&transaction_id)
    );
    assert!(
        !journal
            .migration_finalization_intents
            .contains_key(&transaction_id)
    );
    drop(journal);
    db.commit_transaction(&mut transaction).unwrap();
    assert_eq!(db.bindings.resolve_single(table), Ok(source_storage));
    assert_eq!(db.next_storage_id(), storage_floor);
    assert_eq!(db.next_column_id(table), Some(ColumnId(column.0 + 1)));
    assert_eq!(db.table_schema_version(table), Some(version));
    assert_eq!(db.schema_generation(), generation);
    // The prerequisite durable DROP-only index transaction publishes its own
    // index change. The net-no-op layout adds no second runtime revision.
    assert_eq!(db.catalog_generation(), revision + 1);
    assert_eq!(
        db.query("SELECT email FROM users WHERE id = 3")
            .unwrap()
            .rows,
        vec![vec![ScalarValue::Text("changed@example.test".into())]]
    );
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn public_sql_rollback_matrix_preserves_source_layout_and_column_burns() {
    for (name, statements, burned_columns) in [
        (
            "rollback-add",
            &["ALTER TABLE users ADD COLUMN first_loser TEXT"] as &[&str],
            1,
        ),
        (
            "rollback-drop",
            &["ALTER TABLE users DROP COLUMN legacy"] as &[&str],
            0,
        ),
        (
            "rollback-same-name",
            &[
                "ALTER TABLE users DROP COLUMN legacy",
                "ALTER TABLE users ADD COLUMN legacy TEXT",
            ] as &[&str],
            1,
        ),
        (
            "rollback-multiple-add",
            &[
                "ALTER TABLE users ADD COLUMN first_loser TEXT",
                "ALTER TABLE users ADD COLUMN second_loser TEXT",
            ] as &[&str],
            2,
        ),
    ] {
        let root = root(name);
        let mut db = seed(&root, true);
        let table = db.schema().table("users").unwrap().id;
        let source_storage = db.bindings.resolve_single(table).unwrap();
        let first = db.next_column_id(table).unwrap();
        let storage_floor = db.next_storage_id();
        let mut transaction = db.begin_transaction().unwrap();
        drop_source_indexes(&mut db, &mut transaction);
        db.execute_in(&mut transaction, "DELETE FROM users WHERE id = 2")
            .unwrap();
        for statement in statements {
            db.execute_in(&mut transaction, statement).unwrap();
        }
        assert_eq!(
            db.next_column_id(table),
            Some(ColumnId(first.0 + burned_columns))
        );
        transaction.rollback().unwrap();
        drop(transaction);
        db.close().unwrap();

        let mut reopened = Database::open_catalog(root.join("catalog")).unwrap();
        assert_eq!(reopened.bindings.resolve_single(table), Ok(source_storage));
        assert_eq!(reopened.next_storage_id(), storage_floor);
        assert_eq!(
            reopened.next_column_id(table),
            Some(ColumnId(first.0 + burned_columns))
        );
        let users = reopened.schema().table("users").unwrap();
        assert_eq!(users.column("legacy").unwrap().id, ColumnId(2));
        assert!(users.column("first_loser").is_none());
        assert!(users.column("second_loser").is_none());
        assert_eq!(
            reopened
                .query("SELECT id FROM users ORDER BY id")
                .unwrap()
                .rows
                .len(),
            3
        );
        reopened.close().unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn policy_b_allows_empty_new_not_null_but_rejects_nonempty_during_projection() {
    for rows in [false, true] {
        let root = root(if rows {
            "policy-b-nonempty"
        } else {
            "policy-b-empty"
        });
        let mut db = seed(&root, rows);
        let table = db.schema().table("users").unwrap().id;
        let new_column = db.next_column_id(table).unwrap();
        let mut transaction = db.begin_transaction().unwrap();
        drop_source_indexes(&mut db, &mut transaction);
        db.execute_in(
            &mut transaction,
            "UPDATE users SET email = email WHERE id = -1",
        )
        .unwrap();
        apply_layout(
            &mut db,
            &mut transaction,
            AlterTableOperation::AddNullableColumn {
                name: "required_later".into(),
                data_type: SemanticType::physical(PhysicalType::Text),
            },
        )
        .unwrap();
        apply_layout(
            &mut db,
            &mut transaction,
            AlterTableOperation::SetNotNull {
                column_id: new_column,
            },
        )
        .unwrap();
        if rows {
            assert!(matches!(
                db.finalize_source_backfill(&mut transaction),
                Err(DatabaseError::SchemaMutation(
                    SchemaMutationError::NotNullViolation(column)
                )) if column == new_column
            ));
            transaction.rollback().unwrap();
            drop(transaction);
            db.close().unwrap();
            let reopened = Database::open_catalog(root.join("catalog")).unwrap();
            assert!(
                reopened
                    .schema()
                    .table("users")
                    .unwrap()
                    .column("required_later")
                    .is_none()
            );
            assert_eq!(
                reopened.next_column_id(table),
                Some(ColumnId(new_column.0 + 1))
            );
            reopened.close().unwrap();
        } else {
            db.commit_transaction(&mut transaction).unwrap();
            let column = db
                .schema()
                .table("users")
                .unwrap()
                .column("required_later")
                .unwrap();
            assert_eq!(column.id, new_column);
            assert!(!column.nullable);
            db.close().unwrap();
        }
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn public_layout_sql_adopts_ordinary_dml_but_keeps_index_and_new_column_restrictions() {
    let adopted_root = root("public-adopted-add");
    let mut db = seed(&adopted_root, true);
    let mut transaction = db.begin_transaction().unwrap();
    db.execute_in(
        &mut transaction,
        "UPDATE users SET legacy = 'changed' WHERE id = 1",
    )
    .unwrap();
    db.execute_in(
        &mut transaction,
        "ALTER TABLE users ADD COLUMN public_add TEXT",
    )
    .unwrap();
    transaction.rollback().unwrap();
    db.close().unwrap();
    std::fs::remove_dir_all(adopted_root).unwrap();

    let indexed_root = root("public-indexed-drop");
    let mut db = seed(&indexed_root, true);
    let mut transaction = db.begin_transaction().unwrap();
    db.execute_in(
        &mut transaction,
        "UPDATE users SET legacy = 'changed' WHERE id = 1",
    )
    .unwrap();
    assert!(matches!(
        db.execute_in(&mut transaction, "ALTER TABLE users DROP COLUMN legacy"),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::IndexedColumn(ColumnId(2))
        ))
    ));
    transaction.rollback().unwrap();
    db.close().unwrap();
    std::fs::remove_dir_all(indexed_root).unwrap();

    let index_root = root("new-index-negative");
    let mut db = seed(&index_root, true);
    let mut transaction = db.begin_transaction().unwrap();
    drop_source_indexes(&mut db, &mut transaction);
    db.execute_in(
        &mut transaction,
        "UPDATE users SET email = email WHERE id = -1",
    )
    .unwrap();
    db.execute_in(
        &mut transaction,
        "ALTER TABLE users ADD COLUMN unindexed TEXT",
    )
    .unwrap();
    assert!(matches!(
        db.execute_in(
            &mut transaction,
            "CREATE INDEX users_unindexed_idx ON users(unindexed)",
        ),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::UnsupportedBackfillRefinement(_)
        ))
    ));
    transaction.rollback().unwrap();
    db.close().unwrap();
    std::fs::remove_dir_all(index_root).unwrap();

    let not_null_root = root("new-not-null-negative");
    let mut db = seed(&not_null_root, true);
    let mut transaction = db.begin_transaction().unwrap();
    drop_source_indexes(&mut db, &mut transaction);
    db.execute_in(
        &mut transaction,
        "UPDATE users SET email = email WHERE id = 1",
    )
    .unwrap();
    db.execute_in(
        &mut transaction,
        "ALTER TABLE users ADD COLUMN nullable_only TEXT",
    )
    .unwrap();
    assert!(matches!(
        db.execute_in(
            &mut transaction,
            "ALTER TABLE users ALTER COLUMN nullable_only SET NOT NULL",
        ),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::UnsupportedBackfillRefinement(_)
        ))
    ));
    transaction.rollback().unwrap();
    db.close().unwrap();
    std::fs::remove_dir_all(not_null_root).unwrap();
}

#[test]
fn public_sql_routes_drop_add_and_compatible_refinements_through_one_late_clone() {
    let root = root("public-end-to-end");
    let mut db = seed(&root, true);
    let catalog_path = root.join("catalog");
    let table = db.schema().table("users").unwrap().id;
    let source_storage = db.bindings.resolve_single(table).unwrap();
    let target_storage = db.next_storage_id().unwrap();
    let base_version = db.table_schema_version(table).unwrap();
    let base_generation = db.schema_generation();
    let base_epoch = schema_catalog_file::load(&catalog_path).unwrap().epoch;
    let base_revision = db.catalog_generation();
    let mut transaction = db.begin_transaction().unwrap();
    for source in [
        "DROP INDEX users_legacy_idx",
        "DROP INDEX users_email_idx",
        "UPDATE users SET email = 'filled@example.test' WHERE email IS NULL",
        "INSERT INTO users VALUES (4, 'old-four', 'four@example.test')",
        "DELETE FROM users WHERE id = 2",
        "ALTER TABLE users DROP COLUMN legacy",
        "ALTER TABLE users ADD COLUMN legacy TEXT",
        "ALTER TABLE users ALTER COLUMN email SET NOT NULL",
        "ALTER TABLE users RENAME COLUMN email TO canonical_email",
        "CREATE INDEX users_email_idx ON users(canonical_email)",
    ] {
        db.execute_in(&mut transaction, source)
            .unwrap_or_else(|error| panic!("{source}: {error:?}"));
    }
    assert!(matches!(
        db.execute_in(&mut transaction, "SELECT id FROM users"),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::MigrationDataAccessAfterRefinement
        ))
    ));
    db.finalize_source_backfill(&mut transaction).unwrap();
    let materialized = transaction.schema_composition.source_backfill().unwrap();
    assert_eq!(materialized.source_copy_passes, 1);
    assert_eq!(materialized.source_rows_copied, 3);
    db.commit_transaction(&mut transaction).unwrap();

    let final_table = db.schema().table("users").unwrap();
    assert_eq!(final_table.id, table);
    assert_eq!(
        final_table
            .columns
            .iter()
            .map(|column| column.id)
            .collect::<Vec<_>>(),
        [ColumnId(1), ColumnId(3), ColumnId(4)]
    );
    assert_eq!(final_table.column("legacy").unwrap().id, ColumnId(4));
    assert_eq!(db.bindings.resolve_single(table), Ok(target_storage));
    assert_ne!(source_storage, target_storage);
    assert_eq!(
        db.table_schema_version(table),
        Some(TableSchemaVersion(base_version.0 + 1))
    );
    assert_eq!(db.schema_generation().0, base_generation.0 + 1);
    assert_eq!(
        schema_catalog_file::load(&catalog_path).unwrap().epoch,
        base_epoch + 1
    );
    assert_eq!(db.catalog_generation(), base_revision + 1);
    assert_eq!(db.next_storage_id(), Some(StorageId(target_storage.0 + 1)));
    assert_eq!(
        db.query("SELECT id, canonical_email, legacy FROM users ORDER BY id")
            .unwrap()
            .rows,
        [
            vec![
                ScalarValue::Int64(1),
                ScalarValue::Text("filled@example.test".into()),
                ScalarValue::Null,
            ],
            vec![
                ScalarValue::Int64(3),
                ScalarValue::Text("three@example.test".into()),
                ScalarValue::Null,
            ],
            vec![
                ScalarValue::Int64(4),
                ScalarValue::Text("four@example.test".into()),
                ScalarValue::Null,
            ],
        ]
    );
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn public_sql_multiple_adds_share_one_version_heap_and_source_scan() {
    let root = root("public-multiple-add");
    let mut db = seed(&root, true);
    let table = db.schema().table("users").unwrap().id;
    let version = db.table_schema_version(table).unwrap();
    let target_storage = db.next_storage_id().unwrap();
    let mut transaction = db.begin_transaction().unwrap();
    drop_source_indexes(&mut db, &mut transaction);
    db.execute_in(
        &mut transaction,
        "UPDATE users SET email = email WHERE id = 1",
    )
    .unwrap();
    db.execute_in(&mut transaction, "ALTER TABLE users ADD COLUMN marker TEXT")
        .unwrap();
    db.execute_in(
        &mut transaction,
        "ALTER TABLE users ADD COLUMN score BIGINT",
    )
    .unwrap();
    assert_eq!(db.next_column_id(table), Some(ColumnId(6)));
    db.finalize_source_backfill(&mut transaction).unwrap();
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
        db.table_schema_version(table),
        Some(TableSchemaVersion(version.0 + 1))
    );
    assert_eq!(db.bindings.resolve_single(table), Ok(target_storage));
    assert_eq!(
        db.schema()
            .table("users")
            .unwrap()
            .column("marker")
            .unwrap()
            .id,
        ColumnId(4)
    );
    assert_eq!(
        db.schema()
            .table("users")
            .unwrap()
            .column("score")
            .unwrap()
            .id,
        ColumnId(5)
    );
    assert!(
        db.query("SELECT marker, score FROM users")
            .unwrap()
            .rows
            .iter()
            .all(|row| row == &[ScalarValue::Null, ScalarValue::Null])
    );
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn prepared_public_drop_and_add_are_pure_exact_and_reserve_once_on_execute() {
    let root = root("prepared-public-layout");
    let mut db = seed(&root, true);
    let table = db.schema().table("users").unwrap().id;
    let old_legacy = db
        .schema()
        .table("users")
        .unwrap()
        .column("legacy")
        .unwrap()
        .id;
    let column_floor = db.next_column_id(table).unwrap();
    let storage_floor = db.next_storage_id();
    let drop_legacy = db
        .prepare_ddl_statement("ALTER TABLE users DROP COLUMN legacy")
        .unwrap();
    let add_marker = db
        .prepare_ddl_statement("ALTER TABLE users ADD COLUMN marker TEXT")
        .unwrap();
    assert_eq!(db.next_column_id(table), Some(column_floor));
    assert_eq!(db.next_storage_id(), storage_floor);

    let mut transaction = db.begin_transaction().unwrap();
    drop_source_indexes(&mut db, &mut transaction);
    db.execute_in(
        &mut transaction,
        "UPDATE users SET email = email WHERE id = 1",
    )
    .unwrap();
    assert_eq!(
        db.execute_ddl_in(&mut transaction, &drop_legacy).unwrap(),
        DdlOutcome::Altered
    );
    assert_eq!(db.next_column_id(table), Some(column_floor));
    assert_eq!(
        db.execute_ddl_in(&mut transaction, &add_marker).unwrap(),
        DdlOutcome::Altered
    );
    db.execute_in(&mut transaction, "ALTER TABLE users ADD COLUMN legacy TEXT")
        .unwrap();
    assert_eq!(db.next_column_id(table), Some(ColumnId(column_floor.0 + 2)));
    db.commit_transaction(&mut transaction).unwrap();

    let replacement = db
        .schema()
        .table("users")
        .unwrap()
        .column("legacy")
        .unwrap();
    assert_eq!(replacement.id, ColumnId(column_floor.0 + 1));
    assert_ne!(replacement.id, old_legacy);
    assert_eq!(
        db.schema()
            .table("users")
            .unwrap()
            .column("marker")
            .unwrap()
            .id,
        column_floor
    );
    assert!(
        db.query("SELECT legacy FROM users")
            .unwrap()
            .rows
            .iter()
            .all(|row| row == &[ScalarValue::Null])
    );
    let next_column = db.next_column_id(table);
    let next_storage = db.next_storage_id();
    assert!(db.execute_ddl(&add_marker).is_err());
    assert!(db.execute_ddl(&drop_legacy).is_err());
    assert_eq!(db.next_column_id(table), next_column);
    assert_eq!(db.next_storage_id(), next_storage);
    assert_eq!(
        db.schema()
            .table("users")
            .unwrap()
            .column("legacy")
            .unwrap()
            .id,
        ColumnId(column_floor.0 + 1)
    );
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn late_tag16_api_accepts_only_the_exact_drop_only_source_authority() {
    let root = root("late-tag16-authority");
    let mut db = seed(&root, true);
    let table = db.schema().table("users").unwrap().id;
    let column = db.next_column_id(table).unwrap();
    let mut transaction = db.begin_transaction().unwrap();
    drop_source_indexes(&mut db, &mut transaction);
    db.execute_in(
        &mut transaction,
        "UPDATE users SET email = email WHERE id = 1",
    )
    .unwrap();
    let reservation = CompositionColumnReservation {
        transaction: transaction.id(),
        table,
        column,
        next_column_id: Some(ColumnId(column.0 + 1)),
    };
    let journal = db.mutation_journal.as_ref().unwrap().clone();
    assert!(
        journal
            .borrow()
            .prepare_source_backfill_column_reservation(&reservation, column)
            .is_ok()
    );
    assert!(
        journal
            .borrow()
            .prepare_source_backfill_column_reservation(&reservation, ColumnId(column.0 + 1))
            .is_err()
    );
    let mut wrong_table = reservation.clone();
    wrong_table.table = TableId(table.0 + 1);
    assert!(
        journal
            .borrow()
            .prepare_source_backfill_column_reservation(&wrong_table, column)
            .is_err()
    );
    apply_layout(
        &mut db,
        &mut transaction,
        AlterTableOperation::AddNullableColumn {
            name: "durably_reserved".into(),
            data_type: SemanticType::physical(PhysicalType::Text),
        },
    )
    .unwrap();
    assert!(
        journal
            .borrow()
            .prepare_source_backfill_column_reservation(&reservation, column)
            .is_err()
    );
    let bytes = journal.borrow().encode().unwrap();
    let decoded = crate::schema_mutation_journal::SchemaMutationJournal::decode(&bytes).unwrap();
    assert_eq!(
        decoded.effective_column(table, Some(column)),
        Some(ColumnId(column.0 + 1))
    );
    assert!(
        decoded.compositions[&transaction.id()]
            .index_intent
            .is_some()
    );
    transaction.rollback().unwrap();
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

fn execute_crash_transaction(db: &mut Database) {
    let mut transaction = db.begin_transaction().unwrap();
    drop_source_indexes(db, &mut transaction);
    db.execute_in(
        &mut transaction,
        "UPDATE users SET email = 'filled@example.test' WHERE email IS NULL",
    )
    .unwrap();
    db.execute_in(&mut transaction, "ALTER TABLE users ADD COLUMN marker TEXT")
        .unwrap();
    db.commit_transaction(&mut transaction).unwrap();
}

fn assert_crash_base(root: &Path, source_storage: StorageId) {
    for _ in 0..3 {
        let mut reopened = Database::open_catalog(root.join("catalog")).unwrap();
        let table = reopened.schema().table("users").unwrap().id;
        assert_eq!(reopened.bindings.resolve_single(table), Ok(source_storage));
        assert!(
            reopened
                .schema()
                .table("users")
                .unwrap()
                .column("marker")
                .is_none()
        );
        assert_eq!(reopened.next_column_id(table), Some(ColumnId(5)));
        assert_eq!(reopened.indexes(table).unwrap().len(), 2);
        assert_eq!(
            reopened
                .query("SELECT id FROM users ORDER BY id")
                .unwrap()
                .rows
                .len(),
            3
        );
        reopened.close().unwrap();
    }
}

fn assert_crash_winner(root: &Path, target_storage: StorageId) {
    for _ in 0..3 {
        let mut reopened = Database::open_catalog(root.join("catalog")).unwrap();
        let table = reopened.schema().table("users").unwrap().id;
        assert_eq!(reopened.bindings.resolve_single(table), Ok(target_storage));
        assert_eq!(
            reopened
                .schema()
                .table("users")
                .unwrap()
                .column("marker")
                .unwrap()
                .id,
            ColumnId(4)
        );
        assert_eq!(reopened.next_column_id(table), Some(ColumnId(5)));
        assert_eq!(reopened.indexes(table).unwrap().len(), 0);
        assert_eq!(
            reopened
                .query("SELECT id, marker FROM users ORDER BY id")
                .unwrap()
                .rows,
            vec![
                vec![ScalarValue::Int64(1), ScalarValue::Null],
                vec![ScalarValue::Int64(2), ScalarValue::Null],
                vec![ScalarValue::Int64(3), ScalarValue::Null],
            ]
        );
        reopened.close().unwrap();
    }
}

#[test]
fn source_backfill_layout_crash_child() {
    let Ok(root) = std::env::var("NETBADB_ROUND41_CRASH_ROOT") else {
        return;
    };
    let mut db = Database::open_catalog(Path::new(&root).join("catalog")).unwrap();
    execute_crash_transaction(&mut db);
    panic!("configured Round 41 crash hook was not reached");
}

#[test]
fn layout_predecision_crashes_restore_base_and_preserve_column_burn() {
    for point in [
        "source-backfill-column-reservation-durable",
        "source-backfill-target-reserved",
        "source-backfill-mid-copy",
        "source-backfill-final-indexes-built",
    ] {
        let root = root(point);
        let db = seed(&root, true);
        let table = db.schema().table("users").unwrap().id;
        let source_storage = db.bindings.resolve_single(table).unwrap();
        db.close().unwrap();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "source_backfill_layout_projection_tests::source_backfill_layout_crash_child",
                "--nocapture",
            ])
            .env("NETBADB_ROUND41_CRASH_ROOT", &root)
            .env("NETBADB_BACKFILL_CRASH_POINT", point)
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(90),
            "{point}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_crash_base(&root, source_storage);
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn layout_cord_partial_commit_matrix_converges_to_one_winner() {
    for (name, point, reverse) in [
        ("both-prepared", "after-durable-decision", false),
        ("source-committed", "after-commit-1", false),
        ("target-committed", "after-commit-1", true),
        ("both-committed", "after-all-commits", false),
    ] {
        let root = root(name);
        let db = seed(&root, true);
        let target_storage = db.next_storage_id().unwrap();
        db.close().unwrap();
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "source_backfill_layout_projection_tests::source_backfill_layout_crash_child",
                "--nocapture",
            ])
            .env("NETBADB_ROUND41_CRASH_ROOT", &root);
        if reverse {
            command.env("NETBADB_REVERSE_PARTICIPANT_COMMIT", "1");
        }
        coordinator_crash::configure_child(&mut command, name, &root, point);
        let output = command.output().unwrap();
        assert_eq!(
            output.status.code(),
            Some(coordinator_crash::EXIT_CODE),
            "{name}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_crash_winner(&root, target_storage);
        std::fs::remove_dir_all(root).unwrap();
    }
}
