use super::*;
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_types::IndexId;
use std::path::{Path, PathBuf};

fn root(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "netbadb-round35-{name}-{}-{:?}",
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
    db.execute("CREATE INDEX users_id_idx ON users(id)")
        .unwrap();
    db.execute("CREATE INDEX users_email_idx ON users(email)")
        .unwrap();
    db
}

fn seed_multiple(root: &Path) -> Database {
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
    db.execute("CREATE TABLE users (id BIGINT NOT NULL, email TEXT, nickname TEXT)")
        .unwrap();
    db.execute("INSERT INTO users VALUES (1, NULL, NULL)")
        .unwrap();
    db.execute("INSERT INTO users VALUES (2, 'two@example.test', 'two')")
        .unwrap();
    db.execute("CREATE INDEX users_id_idx ON users(id)")
        .unwrap();
    db.execute("CREATE INDEX users_email_idx ON users(email)")
        .unwrap();
    db.execute("CREATE INDEX users_nickname_idx ON users(nickname)")
        .unwrap();
    db
}

#[test]
fn evacuation_refinement_and_replacement_reuse_one_staged_heap() {
    let root = root("lifecycle");
    let mut db = seed(&root);
    let users = db.schema().table("users").unwrap().id;
    let email = db
        .schema()
        .table("users")
        .unwrap()
        .column("email")
        .unwrap()
        .id;
    let base_storage = db.bindings.resolve_single(users).unwrap();
    let base_version = db.table_schema_version(users).unwrap();
    let base_generation = db.schema_generation();
    let base_epoch = crate::schema_catalog_file::load(&root.join("catalog"))
        .unwrap()
        .epoch;
    let base_revision = db.catalog_generation();
    let old_email = db
        .indexes(users)
        .unwrap()
        .iter()
        .find(|index| index.column_id == email)
        .unwrap()
        .clone();
    let surviving = db
        .indexes(users)
        .unwrap()
        .iter()
        .find(|index| index.column_id != email)
        .unwrap()
        .clone();
    assert_eq!(users, TableId(2));
    assert_eq!(base_storage, StorageId(2));
    assert_eq!(base_version, TableSchemaVersion(1));
    assert_eq!(surviving.id, IndexId(1));
    assert_eq!(old_email.id, IndexId(2));

    let mut transaction = db.begin_transaction().unwrap();
    db.execute_in(&mut transaction, "ALTER TABLE users ADD COLUMN marker TEXT")
        .unwrap();
    db.execute_in(
        &mut transaction,
        "UPDATE users SET marker = 'ready', email = 'filled@example.test' WHERE email IS NULL",
    )
    .unwrap();
    let staged_storage = transaction.staged_binding(users).unwrap();
    assert_ne!(staged_storage, base_storage);
    assert_eq!(staged_storage, StorageId(3));

    db.evacuate_staged_backfill_index_in(
        &mut transaction,
        DropIndexTarget {
            table_id: users,
            index_id: old_email.id,
        },
    )
    .unwrap();
    assert!(matches!(
        transaction.schema_composition,
        schema_composition::SchemaCompositionState::IndexEvacuating(_)
    ));
    assert_eq!(transaction.participant_count(), 1);
    assert_eq!(transaction.write_participant(), Some(staged_storage));
    assert!(matches!(
        db.execute_in(&mut transaction, "SELECT id FROM users"),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::MigrationDataAccessAfterRefinement
        ))
    ));
    let materialized = transaction.schema_composition.materialized().unwrap();
    assert_eq!(
        materialized.staged_indexes[&users].active,
        vec![netbadb_storage::HeapRewriteIndex {
            id: surviving.id,
            name: surviving.name.clone(),
            column_id: surviving.column_id,
        }]
    );
    let staged_surviving = &materialized.staged[&staged_storage].indexes()[0];
    assert_eq!(staged_surviving.id, surviving.id);
    assert_eq!(staged_surviving.name, surviving.name);
    assert_eq!(staged_surviving.column_id, surviving.column_id);

    db.execute_in(
        &mut transaction,
        "ALTER TABLE users ALTER COLUMN email SET NOT NULL",
    )
    .unwrap();
    assert!(matches!(
        transaction.schema_composition,
        schema_composition::SchemaCompositionState::RefiningAfterEvacuation(_)
    ));
    db.execute_in(
        &mut transaction,
        "CREATE INDEX users_email_idx ON users(email)",
    )
    .unwrap();
    assert!(matches!(
        transaction.schema_composition,
        schema_composition::SchemaCompositionState::IndexFinalizing(_)
    ));
    db.commit_transaction(&mut transaction).unwrap();

    assert_eq!(db.bindings.resolve_single(users), Ok(staged_storage));
    let replacement = db
        .indexes(users)
        .unwrap()
        .iter()
        .find(|index| index.column_id == email)
        .unwrap();
    assert_eq!(replacement.id, IndexId(3));
    assert_eq!(db.next_storage_id(), Some(StorageId(4)));
    assert_eq!(
        db.registry
            .get_mut(staged_storage)
            .unwrap()
            .heap_rewrite_indexes()
            .unwrap()
            .next_index_id,
        IndexId(4)
    );
    assert_eq!(db.table_schema_version(users), Some(TableSchemaVersion(2)));
    assert_eq!(db.schema_generation().0, base_generation.0 + 1);
    assert_eq!(
        crate::schema_catalog_file::load(&root.join("catalog"))
            .unwrap()
            .epoch,
        base_epoch + 1
    );
    assert_eq!(db.catalog_generation(), base_revision + 1);
    assert!(
        !db.schema()
            .table("users")
            .unwrap()
            .column("email")
            .unwrap()
            .nullable
    );
    assert_eq!(
        db.query("SELECT id, email FROM users ORDER BY id")
            .unwrap()
            .rows,
        vec![
            vec![
                ScalarValue::Int64(1),
                ScalarValue::Text("filled@example.test".into())
            ],
            vec![
                ScalarValue::Int64(2),
                ScalarValue::Text("two@example.test".into())
            ]
        ]
    );
    db.close().unwrap();

    for _ in 0..3 {
        let reopened = Database::open_catalog(root.join("catalog")).unwrap();
        let indexes = reopened.indexes(users).unwrap();
        assert_eq!(indexes.len(), 2);
        assert!(indexes.iter().any(|index| index.id == surviving.id));
        assert!(indexes.iter().any(|index| index.id > old_email.id));
        assert!(
            !reopened
                .schema()
                .table("users")
                .unwrap()
                .column("email")
                .unwrap()
                .nullable
        );
        reopened.close().unwrap();
    }

    let mut reopened = Database::open_catalog(root.join("catalog")).unwrap();
    let retired = reopened
        .inspect_replacement_retired_heaps()
        .into_iter()
        .find(|resource| resource.old_storage_id == base_storage)
        .unwrap();
    let before_gc = reopened
        .inspect_replacement_retired_heap_gc(&retired)
        .unwrap();
    let gc = reopened.gc_replacement_retired_heap(&retired).unwrap();
    assert_eq!(gc.bytes_deleted, before_gc.total_present_bytes);
    assert_eq!(reopened.bindings.resolve_single(users), Ok(staged_storage));
    let indexes = reopened.indexes(users).unwrap();
    assert_eq!(indexes.len(), 2);
    assert!(indexes.iter().any(|index| index.id == surviving.id));
    assert!(indexes.iter().any(|index| index.id > old_email.id));
    assert_eq!(
        reopened
            .query("SELECT id, email FROM users ORDER BY id")
            .unwrap()
            .rows,
        vec![
            vec![
                ScalarValue::Int64(1),
                ScalarValue::Text("filled@example.test".into())
            ],
            vec![
                ScalarValue::Int64(2),
                ScalarValue::Text("two@example.test".into())
            ]
        ]
    );
    reopened.close().unwrap();

    for _ in 0..3 {
        let reopened = Database::open_catalog(root.join("catalog")).unwrap();
        assert_eq!(reopened.bindings.resolve_single(users), Ok(staged_storage));
        assert_eq!(reopened.indexes(users).unwrap().len(), 2);
        reopened.close().unwrap();
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn evacuation_requires_successful_refinement_before_commit() {
    let root = root("commit-gate");
    let mut db = seed(&root);
    let users = db.schema().table("users").unwrap().id;
    let email = db
        .schema()
        .table("users")
        .unwrap()
        .column("email")
        .unwrap()
        .id;
    let old = db
        .indexes(users)
        .unwrap()
        .iter()
        .find(|index| index.column_id == email)
        .unwrap()
        .clone();
    let mut transaction = db.begin_transaction().unwrap();
    db.execute_in(&mut transaction, "ALTER TABLE users ADD COLUMN marker TEXT")
        .unwrap();
    db.execute_in(&mut transaction, "UPDATE users SET marker = 'ready'")
        .unwrap();
    db.evacuate_staged_backfill_index_in(
        &mut transaction,
        DropIndexTarget {
            table_id: users,
            index_id: old.id,
        },
    )
    .unwrap();

    let error = db.commit_transaction(&mut transaction).unwrap_err();
    assert!(matches!(
        error,
        DatabaseError::SchemaMutation(SchemaMutationError::EvacuationRequiresRefinement)
    ));
    assert_eq!(error.kind(), DatabaseErrorKind::TransactionState);
    assert!(matches!(
        transaction.schema_composition,
        schema_composition::SchemaCompositionState::IndexEvacuating(_)
    ));
    let error = db
        .execute_in(
            &mut transaction,
            "ALTER TABLE users ALTER COLUMN email SET NOT NULL",
        )
        .unwrap_err();
    assert!(matches!(
        error,
        DatabaseError::SchemaMutation(SchemaMutationError::NotNullViolation(column))
            if column == email
    ));
    assert!(matches!(
        transaction.schema_composition,
        schema_composition::SchemaCompositionState::IndexEvacuating(_)
    ));
    transaction.rollback().unwrap();
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn exact_target_failure_is_a_noop_and_refined_evacuation_needs_no_replacement() {
    let root = root("exact-and-no-replacement");
    let mut db = seed(&root);
    let users = db.schema().table("users").unwrap().id;
    let email = db
        .schema()
        .table("users")
        .unwrap()
        .column("email")
        .unwrap()
        .id;
    let old = db
        .indexes(users)
        .unwrap()
        .iter()
        .find(|index| index.column_id == email)
        .unwrap()
        .clone();
    let mut transaction = db.begin_transaction().unwrap();
    db.execute_in(&mut transaction, "ALTER TABLE users ADD COLUMN marker TEXT")
        .unwrap();
    db.execute_in(
        &mut transaction,
        "UPDATE users SET marker = 'ready', email = 'filled@example.test' WHERE email IS NULL",
    )
    .unwrap();
    let staged_storage = transaction.staged_binding(users).unwrap();
    let before = transaction
        .schema_composition
        .materialized()
        .unwrap()
        .staged_indexes[&users]
        .clone();

    assert!(matches!(
        db.evacuate_staged_backfill_index_in(
            &mut transaction,
            DropIndexTarget {
                table_id: users,
                index_id: IndexId(old.id.0 + 10_000),
            },
        ),
        Err(DatabaseError::UndefinedIndex)
    ));
    assert!(matches!(
        transaction.schema_composition,
        schema_composition::SchemaCompositionState::BackfillOpen(_)
    ));
    assert_eq!(
        transaction
            .schema_composition
            .materialized()
            .unwrap()
            .staged_indexes[&users],
        before
    );

    db.evacuate_staged_backfill_index_in(
        &mut transaction,
        DropIndexTarget {
            table_id: users,
            index_id: old.id,
        },
    )
    .unwrap();
    db.execute_in(
        &mut transaction,
        "ALTER TABLE users ALTER COLUMN email SET NOT NULL",
    )
    .unwrap();
    db.commit_transaction(&mut transaction).unwrap();

    assert_eq!(db.bindings.resolve_single(users), Ok(staged_storage));
    assert!(
        db.indexes(users)
            .unwrap()
            .iter()
            .all(|index| index.id != old.id)
    );
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
fn multiple_evacuations_preserve_a_compatible_index_across_rename() {
    let root = root("multiple");
    let mut db = seed_multiple(&root);
    let users = db.schema().table("users").unwrap().id;
    let table = db.schema().table("users").unwrap();
    let id = table.column("id").unwrap().id;
    let email = table.column("email").unwrap().id;
    let nickname = table.column("nickname").unwrap().id;
    let indexes = db.indexes(users).unwrap().to_vec();
    let surviving = indexes
        .iter()
        .find(|index| index.column_id == id)
        .unwrap()
        .clone();
    let evacuated = indexes
        .iter()
        .filter(|index| index.column_id == email || index.column_id == nickname)
        .cloned()
        .collect::<Vec<_>>();
    let mut transaction = db.begin_transaction().unwrap();
    db.execute_in(&mut transaction, "ALTER TABLE users ADD COLUMN marker TEXT")
        .unwrap();
    db.execute_in(
        &mut transaction,
        "UPDATE users SET marker = 'ready', email = 'filled@example.test', nickname = 'filled'",
    )
    .unwrap();
    for index in &evacuated {
        db.evacuate_staged_backfill_index_in(
            &mut transaction,
            DropIndexTarget {
                table_id: users,
                index_id: index.id,
            },
        )
        .unwrap();
        assert!(matches!(
            transaction.schema_composition,
            schema_composition::SchemaCompositionState::IndexEvacuating(_)
        ));
    }
    db.execute_in(
        &mut transaction,
        "ALTER TABLE users RENAME COLUMN id TO member_id",
    )
    .unwrap();
    db.execute_in(
        &mut transaction,
        "ALTER TABLE users ALTER COLUMN email SET NOT NULL",
    )
    .unwrap();
    db.execute_in(
        &mut transaction,
        "ALTER TABLE users ALTER COLUMN nickname SET NOT NULL",
    )
    .unwrap();
    db.commit_transaction(&mut transaction).unwrap();

    let final_indexes = db.indexes(users).unwrap();
    assert_eq!(final_indexes.len(), 1);
    assert_eq!(final_indexes[0].id, surviving.id);
    assert_eq!(final_indexes[0].column_id, id);
    let table = db.schema().table("users").unwrap();
    assert_eq!(table.column("member_id").unwrap().id, id);
    assert!(!table.column("email").unwrap().nullable);
    assert!(!table.column("nickname").unwrap().nullable);
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn evacuation_crash_child() {
    let Ok(root) = std::env::var("NETBADB_ROUND35_CRASH_ROOT") else {
        return;
    };
    let mut db = Database::open_catalog(Path::new(&root).join("catalog")).unwrap();
    let users = db.schema().table("users").unwrap().id;
    let email = db
        .schema()
        .table("users")
        .unwrap()
        .column("email")
        .unwrap()
        .id;
    let old = db
        .indexes(users)
        .unwrap()
        .iter()
        .find(|index| index.column_id == email)
        .unwrap()
        .id;
    let mut transaction = db.begin_transaction().unwrap();
    db.execute_in(&mut transaction, "ALTER TABLE users ADD COLUMN marker TEXT")
        .unwrap();
    db.execute_in(
        &mut transaction,
        "UPDATE users SET marker = 'ready', email = 'filled@example.test' WHERE email IS NULL",
    )
    .unwrap();
    db.evacuate_staged_backfill_index_in(
        &mut transaction,
        DropIndexTarget {
            table_id: users,
            index_id: old,
        },
    )
    .unwrap();
    db.execute_in(
        &mut transaction,
        "ALTER TABLE users ALTER COLUMN email SET NOT NULL",
    )
    .unwrap();
    db.execute_in(
        &mut transaction,
        "CREATE INDEX users_email_idx ON users(email)",
    )
    .unwrap();
    db.commit_transaction(&mut transaction).unwrap();
}

#[test]
fn evacuation_crashes_have_one_s1_or_s2_winner_on_three_reopens() {
    for (point, final_winner) in [
        ("backfill-evacuation-before-drop", false),
        ("backfill-evacuation-physical-drop-durable", false),
        ("backfill-evacuation-inventory-updated", false),
        ("backfill-evacuation-compatibility-validated", false),
        ("backfill-evacuation-refinement-accepted", false),
        ("backfill-before-retarget", false),
        ("backfill-after-heap-retarget", false),
        ("backfill-after-owner-retarget", false),
        ("backfill-index-delta-durable", false),
        ("backfill-before-index-finalization-intent", false),
        ("backfill-index-finalization-intent-durable", false),
        ("composition-prepared-catalog-durable", false),
        ("backfill-participants-prepared", false),
        ("backfill-before-coordinator-decision", false),
        ("coordinator-durable", true),
        ("staged-heap-committed", true),
    ] {
        let root = root(&format!("crash-{point}"));
        let db = seed(&root);
        let users = db.schema().table("users").unwrap().id;
        let email_index = db
            .indexes(users)
            .unwrap()
            .iter()
            .find(|index| index.column_id == ColumnId(2))
            .unwrap()
            .clone();
        db.close().unwrap();

        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("staged_index_evacuation_tests::evacuation_crash_child")
            .arg("--nocapture")
            .env("NETBADB_ROUND35_CRASH_ROOT", &root)
            .env("NETBADB_BACKFILL_CRASH_POINT", point)
            .output()
            .unwrap();
        assert!(!output.status.success(), "crash point {point} returned");

        for _ in 0..3 {
            let mut reopened = Database::open_catalog(root.join("catalog")).unwrap();
            if final_winner {
                assert!(
                    reopened
                        .schema()
                        .table("users")
                        .unwrap()
                        .column("marker")
                        .is_some()
                );
                assert!(
                    !reopened
                        .schema()
                        .table("users")
                        .unwrap()
                        .column("email")
                        .unwrap()
                        .nullable
                );
                let replacement = reopened
                    .indexes(users)
                    .unwrap()
                    .iter()
                    .find(|index| index.column_id == ColumnId(2))
                    .unwrap();
                assert!(replacement.id > email_index.id);
                assert_eq!(
                    reopened
                        .query("SELECT email FROM users WHERE id = 1")
                        .unwrap()
                        .rows,
                    vec![vec![ScalarValue::Text("filled@example.test".into())]]
                );
            } else {
                assert!(
                    reopened
                        .schema()
                        .table("users")
                        .unwrap()
                        .column("marker")
                        .is_none()
                );
                assert!(
                    reopened
                        .schema()
                        .table("users")
                        .unwrap()
                        .column("email")
                        .unwrap()
                        .nullable
                );
                assert!(
                    reopened
                        .indexes(users)
                        .unwrap()
                        .iter()
                        .any(|index| index.id == email_index.id)
                );
                assert_eq!(
                    reopened
                        .query("SELECT email FROM users WHERE id = 1")
                        .unwrap()
                        .rows,
                    vec![vec![ScalarValue::Null]]
                );
            }
            reopened.close().unwrap();
        }
        std::fs::remove_dir_all(root).unwrap();
    }
}
