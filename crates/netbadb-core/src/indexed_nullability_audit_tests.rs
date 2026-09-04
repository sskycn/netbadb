use super::*;
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use schema_mutation_journal::SchemaIndexTablePlan;
use std::path::{Path, PathBuf};

fn root(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "netbadb-round34-{name}-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    std::fs::create_dir(&path).unwrap();
    path
}

fn seed(root: &Path) -> Database {
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
    db.execute("CREATE TABLE users (id BIGINT NOT NULL, email TEXT)")
        .unwrap();
    db.execute("INSERT INTO users VALUES (1, NULL)").unwrap();
    db.execute("INSERT INTO users VALUES (2, 'two@example.test')")
        .unwrap();
    db.execute("CREATE INDEX users_email_idx ON users(email)")
        .unwrap();
    db
}

#[test]
fn direct_index_drop_then_dml_uses_the_committed_heap_participant() {
    let root = root("direct-index-drop-dml");
    let mut db = seed(&root);
    let users = db.schema().table("users").unwrap().id;
    let storage = db.bindings.resolve_single(users).unwrap();
    let index = db.indexes(users).unwrap()[0].clone();
    let base_generation = db.schema_generation();
    let base_revision = db.catalog_generation;

    let prepared = db.prepare_drop_index(users, index.id, false).unwrap();
    let mut transaction = db.begin_transaction().unwrap();
    assert_eq!(
        db.execute_ddl_in(&mut transaction, &prepared).unwrap(),
        DdlOutcome::Dropped
    );
    assert!(matches!(
        transaction.schema_composition,
        schema_composition::SchemaCompositionState::Composing(_)
    ));
    assert_eq!(transaction.write_participant(), None);
    assert_eq!(transaction.participant_count(), 0);
    assert_eq!(transaction.staged_binding(users), None);
    assert_eq!(db.schema_writer.get(), Some(transaction.id()));
    let plan = transaction.schema_composition.plan().unwrap();
    assert!(plan.touched[&users].indexes.active.is_empty());
    assert!(
        !plan
            .journal
            .borrow()
            .stage_intents
            .contains_key(&transaction.id())
    );

    assert_eq!(
        db.execute_in(
            &mut transaction,
            "UPDATE users SET email = 'filled@example.test' WHERE email IS NULL",
        )
        .unwrap(),
        ExecutionResult::AffectedRows(1)
    );
    assert!(matches!(
        transaction.schema_composition,
        schema_composition::SchemaCompositionState::MaterializedIndex(_)
    ));
    assert_eq!(transaction.write_participant(), Some(storage));
    assert_eq!(transaction.participant_count(), 1);
    assert_eq!(transaction.staged_binding(users), None);
    let materialized = transaction.schema_composition.materialized_index().unwrap();
    assert!(materialized.target.is_none());
    assert!(materialized.reference.is_none());
    assert!(materialized.staged.is_empty());
    assert!(!materialized.backfill);
    assert!(matches!(
        materialized.intent.tables.as_slice(),
        [SchemaIndexTablePlan::InPlaceIndexDelta {
            storage: participant,
            base_indexes,
            final_indexes,
            ..
        }] if *participant == storage
            && base_indexes.active.len() == 1
            && base_indexes.active[0].id == index.id
            && base_indexes.active[0].name == index.name
            && base_indexes.active[0].column_id == index.column_id
            && final_indexes.active.is_empty()
    ));
    assert_eq!(materialized.publications.len(), 1);
    assert_eq!(materialized.publications[0].storage, storage);
    assert_eq!(materialized.publications[0].drops, vec![index.id]);
    assert!(materialized.publications[0].creates.is_empty());
    assert!(
        !materialized
            .logical
            .journal
            .borrow()
            .stage_intents
            .contains_key(&transaction.id())
    );

    transaction.rollback().unwrap();
    assert_eq!(db.schema_generation(), base_generation);
    assert_eq!(db.catalog_generation, base_revision);
    assert_eq!(db.bindings.resolve_single(users), Ok(storage));
    assert_eq!(db.indexes(users).unwrap(), std::slice::from_ref(&index));
    assert_eq!(
        db.query("SELECT email FROM users WHERE id = 1")
            .unwrap()
            .rows,
        vec![vec![ScalarValue::Null]]
    );
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn sql_primary_sequence_is_sealed_after_s1_dml_and_rejects_refinement() {
    let root = root("sql-primary-sequence");
    let mut db = seed(&root);
    let users = db.schema().table("users").unwrap().id;
    let storage = db.bindings.resolve_single(users).unwrap();
    let mut transaction = db.begin_transaction().unwrap();

    db.execute_in(&mut transaction, "DROP INDEX users_email_idx")
        .unwrap();
    db.execute_in(
        &mut transaction,
        "UPDATE users SET email = 'filled@example.test' WHERE email IS NULL",
    )
    .unwrap();
    let error = db
        .execute_in(
            &mut transaction,
            "ALTER TABLE users ALTER COLUMN email SET NOT NULL",
        )
        .unwrap_err();
    assert!(matches!(
        &error,
        DatabaseError::SchemaMutation(SchemaMutationError::SchemaMutationAfterMaterialization)
    ));
    assert_eq!(error.kind(), DatabaseErrorKind::TransactionState);
    assert_eq!(transaction.state(), TransactionState::Active);
    assert!(matches!(
        transaction.schema_composition,
        schema_composition::SchemaCompositionState::MaterializedIndex(_)
    ));
    assert_eq!(transaction.write_participant(), Some(storage));
    assert_eq!(transaction.staged_binding(users), None);

    transaction.rollback().unwrap();
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn staged_sql_drop_keeps_history_but_physically_evacuates_before_refinement() {
    let root = root("staged-historical-guard");
    let mut db = seed(&root);
    let users = db.schema().table("users").unwrap().id;
    let email = db
        .schema()
        .table("users")
        .unwrap()
        .column("email")
        .unwrap()
        .id;
    let old_index = db.indexes(users).unwrap()[0].clone();
    let base_storage = db.bindings.resolve_single(users).unwrap();
    let mut transaction = db.begin_transaction().unwrap();

    db.execute_in(&mut transaction, "ALTER TABLE users ADD COLUMN marker TEXT")
        .unwrap();
    db.execute_in(
        &mut transaction,
        "UPDATE users SET marker = 'ready', email = 'filled@example.test'",
    )
    .unwrap();
    let staged_storage = transaction.staged_binding(users).unwrap();
    assert_ne!(staged_storage, base_storage);
    let materialized = transaction.schema_composition.materialized().unwrap();
    assert!(materialized.backfill_indexed_columns.contains(&email));
    let staged_old = &materialized.staged[&staged_storage].indexes()[0];
    assert_eq!(staged_old.id, old_index.id);
    assert_eq!(staged_old.name, old_index.name);
    assert_eq!(staged_old.column_id, old_index.column_id);

    db.execute_in(&mut transaction, "DROP INDEX users_email_idx")
        .unwrap();
    let materialized = transaction.schema_composition.materialized().unwrap();
    assert!(
        materialized.logical.touched[&users]
            .indexes
            .active
            .is_empty()
    );
    assert!(materialized.backfill_indexed_columns.contains(&email));
    assert!(materialized.staged[&staged_storage].indexes().is_empty());
    assert!(materialized.staged_indexes[&users].active.is_empty());
    assert!(matches!(
        transaction.schema_composition,
        schema_composition::SchemaCompositionState::IndexEvacuating(_)
    ));

    db.execute_in(
        &mut transaction,
        "ALTER TABLE users ALTER COLUMN email SET NOT NULL",
    )
    .unwrap();
    assert!(matches!(
        transaction.schema_composition,
        schema_composition::SchemaCompositionState::RefiningAfterEvacuation(_)
    ));
    assert!(
        transaction
            .schema_composition
            .materialized()
            .unwrap()
            .backfill_indexed_columns
            .contains(&email)
    );

    transaction.rollback().unwrap();
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn existing_rewrite_plan_can_omit_a_preparatory_drop_but_cannot_open_backfill() {
    let root = root("rewrite-inventory-excludes-drop");
    let mut db = seed(&root);
    let users = db.schema().table("users").unwrap().id;
    let base_storage = db.bindings.resolve_single(users).unwrap();
    let mut transaction = db.begin_transaction().unwrap();

    db.execute_in(&mut transaction, "DROP INDEX users_email_idx")
        .unwrap();
    db.execute_in(&mut transaction, "ALTER TABLE users ADD COLUMN marker TEXT")
        .unwrap();
    db.execute_in(&mut transaction, "UPDATE users SET marker = 'ready'")
        .unwrap();

    let staged_storage = transaction.staged_binding(users).unwrap();
    assert_ne!(staged_storage, base_storage);
    let materialized = transaction.schema_composition.materialized_index().unwrap();
    assert!(!materialized.backfill);
    assert!(materialized.staged[&staged_storage].indexes().is_empty());
    assert!(matches!(
        materialized.intent.tables.as_slice(),
        [SchemaIndexTablePlan::RewriteHeap { final_indexes, .. }]
            if final_indexes.active.is_empty()
    ));
    assert!(
        !materialized
            .logical
            .journal
            .borrow()
            .stage_intents
            .contains_key(&transaction.id())
    );
    assert!(matches!(
        db.execute_in(
            &mut transaction,
            "ALTER TABLE users ALTER COLUMN marker SET NOT NULL",
        ),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::SchemaMutationAfterMaterialization
        ))
    ));

    transaction.rollback().unwrap();
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn prepared_old_drop_cannot_retire_a_same_name_replacement() {
    let root = root("prepared-old-drop");
    let mut db = seed(&root);
    let users = db.schema().table("users").unwrap().id;
    let old = db.indexes(users).unwrap()[0].clone();
    let prepared = db.prepare_drop_index(users, old.id, true).unwrap();

    db.execute("DROP INDEX users_email_idx").unwrap();
    db.execute("CREATE INDEX users_email_idx ON users(email)")
        .unwrap();
    let replacement = db.indexes(users).unwrap()[0].clone();
    assert_ne!(replacement.id, old.id);
    assert!(replacement.id > old.id);

    let mut transaction = db.begin_transaction().unwrap();
    assert_eq!(
        db.execute_ddl_in(&mut transaction, &prepared).unwrap(),
        DdlOutcome::Unchanged
    );
    db.commit_transaction(&mut transaction).unwrap();
    assert_eq!(
        db.indexes(users).unwrap(),
        std::slice::from_ref(&replacement)
    );

    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}
