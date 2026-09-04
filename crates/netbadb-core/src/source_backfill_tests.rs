use super::*;
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_types::{ColumnId, PhysicalType, ScalarValue, StorageId, TableId};
use std::path::{Path, PathBuf};

fn root(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "netbadb-round38-{name}-{}-{:?}",
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
    db.execute("INSERT INTO users VALUES (3, 'three@example.test')")
        .unwrap();
    db.execute("CREATE INDEX users_email_idx ON users(email)")
        .unwrap();
    db
}

fn execute_foundation_transaction(db: &mut Database) {
    let users = db.schema().table("users").unwrap().id;
    let email = db
        .schema()
        .table("users")
        .unwrap()
        .column("email")
        .unwrap()
        .id;
    let mut transaction = db.begin_transaction().unwrap();
    db.execute_in(&mut transaction, "DROP INDEX users_email_idx")
        .unwrap();
    db.open_source_backfill(&mut transaction).unwrap();
    db.execute_in(
        &mut transaction,
        "UPDATE users SET email = 'filled@example.test' WHERE email IS NULL",
    )
    .unwrap();
    db.execute_in(
        &mut transaction,
        "INSERT INTO users VALUES (4, 'four@example.test')",
    )
    .unwrap();
    db.execute_in(&mut transaction, "DELETE FROM users WHERE id = 2")
        .unwrap();
    let dependency = transaction
        .schema_composition
        .plan()
        .unwrap()
        .dependency(users)
        .unwrap();
    db.apply_source_backfill_refinement(
        &mut transaction,
        AlterTableSpec::new(
            dependency,
            AlterTableOperation::SetNotNull { column_id: email },
        ),
    )
    .unwrap();
    db.execute_in(
        &mut transaction,
        "CREATE INDEX users_email_idx ON users(email)",
    )
    .unwrap();
    db.commit_transaction(&mut transaction).unwrap();
}

fn assert_recovered_winner(root: &Path, expected_target: StorageId) {
    for _ in 0..3 {
        let mut reopened = Database::open_catalog(root.join("catalog"))
            .unwrap_or_else(|error| panic!("reopen {root:?}: {error:?}"));
        let users = reopened.schema().table("users").unwrap();
        assert_eq!(
            reopened.bindings.resolve_single(users.id),
            Ok(expected_target)
        );
        assert!(!users.column("email").unwrap().nullable);
        assert_eq!(reopened.indexes(users.id).unwrap().len(), 1);
        assert_eq!(
            reopened
                .query("SELECT id, email FROM users ORDER BY id")
                .unwrap()
                .rows,
            vec![
                vec![
                    ScalarValue::Int64(1),
                    ScalarValue::Text("filled@example.test".into()),
                ],
                vec![
                    ScalarValue::Int64(3),
                    ScalarValue::Text("three@example.test".into()),
                ],
                vec![
                    ScalarValue::Int64(4),
                    ScalarValue::Text("four@example.test".into()),
                ],
            ]
        );
        assert!(
            reopened
                .inspect_replacement_retired_heaps()
                .iter()
                .any(|retired| retired.old_storage_id != expected_target)
        );
        reopened.close().unwrap();
    }
}

fn assert_recovered_base(root: &Path, source_storage: StorageId) {
    for _ in 0..3 {
        let mut reopened = Database::open_catalog(root.join("catalog"))
            .unwrap_or_else(|error| panic!("reopen {root:?}: {error:?}"));
        let users = reopened.schema().table("users").unwrap();
        assert_eq!(
            reopened.bindings.resolve_single(users.id),
            Ok(source_storage)
        );
        assert!(users.column("email").unwrap().nullable);
        assert_eq!(reopened.indexes(users.id).unwrap().len(), 1);
        assert_eq!(
            reopened
                .query("SELECT id, email FROM users ORDER BY id")
                .unwrap()
                .rows,
            vec![
                vec![ScalarValue::Int64(1), ScalarValue::Null],
                vec![
                    ScalarValue::Int64(2),
                    ScalarValue::Text("two@example.test".into()),
                ],
                vec![
                    ScalarValue::Int64(3),
                    ScalarValue::Text("three@example.test".into()),
                ],
            ]
        );
        reopened.close().unwrap();
    }
}

fn tree_bytes(path: &Path) -> u64 {
    std::fs::read_dir(path)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            let metadata = entry.metadata().unwrap();
            if metadata.is_dir() {
                tree_bytes(&entry.path())
            } else {
                metadata.len()
            }
        })
        .sum()
}

fn resource_family_bytes(path: &Path) -> u64 {
    let prefix = path.file_name().unwrap().to_string_lossy();
    std::fs::read_dir(path.parent().unwrap())
        .unwrap()
        .map(|entry| entry.unwrap())
        .filter(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with(prefix.as_ref())
        })
        .map(|entry| entry.metadata().unwrap().len())
        .sum()
}

#[test]
fn source_backfill_physical_cost_is_delayed_and_uses_one_copy_pass() {
    let root = root("physical-cost");
    let mut db = seed(&root);
    let catalog_path = root.join("catalog");
    let users = db.schema().table("users").unwrap().id;
    let email = db
        .schema()
        .table("users")
        .unwrap()
        .column("email")
        .unwrap()
        .id;
    let source_storage = db.bindings.resolve_single(users).unwrap();
    let initial = schema_catalog_file::load(&catalog_path).unwrap();
    let source_locator = &initial
        .storages
        .iter()
        .find(|storage| storage.id == source_storage)
        .unwrap()
        .locator;
    let source_path = schema_catalog_file::resolve(&catalog_path, source_locator);
    let source_before = resource_family_bytes(&source_path);
    let target_floor = initial.committed.next_storage_id.unwrap();

    let mut transaction = db.begin_transaction().unwrap();
    db.execute_in(&mut transaction, "DROP INDEX users_email_idx")
        .unwrap();
    db.open_source_backfill(&mut transaction).unwrap();
    db.execute_in(
        &mut transaction,
        "UPDATE users SET email = 'filled@example.test' WHERE email IS NULL",
    )
    .unwrap();
    db.execute_in(
        &mut transaction,
        "INSERT INTO users VALUES (4, 'four@example.test')",
    )
    .unwrap();
    db.execute_in(&mut transaction, "DELETE FROM users WHERE id = 2")
        .unwrap();
    let source_during = resource_family_bytes(&source_path);
    let before_target = tree_bytes(&root);
    assert_eq!(db.next_storage_id(), Some(target_floor));
    let dependency = transaction
        .schema_composition
        .plan()
        .unwrap()
        .dependency(users)
        .unwrap();
    db.apply_source_backfill_refinement(
        &mut transaction,
        AlterTableSpec::new(
            dependency,
            AlterTableOperation::SetNotNull { column_id: email },
        ),
    )
    .unwrap();
    db.execute_in(
        &mut transaction,
        "CREATE INDEX users_email_idx ON users(email)",
    )
    .unwrap();
    assert_eq!(db.next_storage_id(), Some(target_floor));

    db.finalize_source_backfill(&mut transaction).unwrap();
    let materialized = transaction.schema_composition.source_backfill().unwrap();
    assert_eq!(materialized.source_copy_passes, 1);
    assert_eq!(materialized.source_rows_copied, 3);
    let intent = db
        .mutation_journal
        .as_ref()
        .unwrap()
        .borrow()
        .source_backfill_intents
        .get(&transaction.id())
        .unwrap()
        .clone();
    let target_storage = intent.target_storage;
    assert_eq!(target_storage, target_floor);
    assert_eq!(db.next_storage_id().unwrap().0, target_floor.0 + 1);
    let target_stage = schema_catalog_file::resolve(&catalog_path, &intent.target_stage_locator);
    let target_staged = resource_family_bytes(&target_stage);
    let peak = tree_bytes(&root);
    assert!(target_staged > 0);
    assert!(peak > before_target);

    db.commit_transaction(&mut transaction).unwrap();
    let committed = schema_catalog_file::load(&catalog_path).unwrap();
    let target_locator = &committed
        .storages
        .iter()
        .find(|storage| storage.id == target_storage)
        .unwrap()
        .locator;
    let target_final =
        resource_family_bytes(&schema_catalog_file::resolve(&catalog_path, target_locator));
    eprintln!(
        "ROUND38_PHYSICAL_COST source_before={source_before} source_during={source_during} source_growth={} before_target={before_target} target_staged={target_staged} target_final={target_final} peak={peak} copy_passes=1 rows_copied=3 target_ids=1",
        source_during.saturating_sub(source_before)
    );
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn source_backfill_clones_transaction_visible_rows_once_into_final_heap() {
    let root = root("primary");
    let mut db = seed(&root);
    let users = db.schema().table("users").unwrap().id;
    let email = db
        .schema()
        .table("users")
        .unwrap()
        .column("email")
        .unwrap()
        .id;
    let source_storage = db.bindings.resolve_single(users).unwrap();
    let source_version = db.table_schema_version(users).unwrap();
    let source_generation = db.schema_generation();
    let old_index = db.indexes(users).unwrap()[0].id;

    let mut transaction = db.begin_transaction().unwrap();
    db.execute_in(&mut transaction, "DROP INDEX users_email_idx")
        .unwrap();
    db.open_source_backfill(&mut transaction).unwrap();
    assert!(matches!(
        transaction.schema_composition,
        schema_composition::SchemaCompositionState::SourceBackfillOpen(_)
    ));
    assert_eq!(transaction.participant_count(), 1);
    assert_eq!(transaction.write_participant(), Some(source_storage));
    db.execute_in(
        &mut transaction,
        "UPDATE users SET email = 'filled@example.test' WHERE email IS NULL",
    )
    .unwrap();
    db.execute_in(
        &mut transaction,
        "INSERT INTO users VALUES (4, 'four@example.test')",
    )
    .unwrap();
    db.execute_in(&mut transaction, "DELETE FROM users WHERE id = 2")
        .unwrap();

    let dependency = transaction
        .schema_composition
        .plan()
        .unwrap()
        .dependency(users)
        .unwrap();
    db.apply_source_backfill_refinement(
        &mut transaction,
        AlterTableSpec::new(
            dependency,
            AlterTableOperation::SetNotNull { column_id: email },
        ),
    )
    .unwrap();
    assert!(matches!(
        db.execute_in(&mut transaction, "SELECT id FROM users"),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::MigrationDataAccessAfterRefinement
        ))
    ));
    db.execute_in(
        &mut transaction,
        "CREATE INDEX users_email_idx ON users(email)",
    )
    .unwrap();
    let transaction_id = transaction.id();
    db.commit_transaction(&mut transaction).unwrap();

    let target_storage = db.bindings.resolve_single(users).unwrap();
    assert_ne!(target_storage, source_storage);
    assert_eq!(
        db.table_schema_version(users).unwrap().0,
        source_version.0 + 1
    );
    assert_eq!(db.schema_generation().0, source_generation.0 + 1);
    let indexes = db.indexes(users).unwrap();
    assert_eq!(indexes.len(), 1);
    assert_ne!(indexes[0].id, old_index);
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
                ScalarValue::Int64(3),
                ScalarValue::Text("three@example.test".into())
            ],
            vec![
                ScalarValue::Int64(4),
                ScalarValue::Text("four@example.test".into())
            ],
        ]
    );
    let decision = transaction
        .coordinator_decisions()
        .unwrap()
        .into_iter()
        .find(|decision| decision.database_txn_id == transaction_id)
        .unwrap();
    assert!(decision.complete);
    assert_eq!(decision.participants.len(), 2);
    assert!(
        decision
            .participants
            .iter()
            .any(|participant| { participant.storage_id == source_storage })
    );
    assert!(
        decision
            .participants
            .iter()
            .any(|participant| { participant.storage_id == target_storage })
    );
    let journal = db.mutation_journal.as_ref().unwrap().borrow();
    assert!(
        journal
            .source_backfill_intents
            .contains_key(&transaction_id)
    );
    assert!(journal.stage_intents.contains_key(&transaction_id));
    assert!(
        journal
            .migration_finalization_intents
            .contains_key(&transaction_id)
    );
    drop(journal);
    db.close().unwrap();
    for _ in 0..3 {
        let mut reopened = Database::open_catalog(root.join("catalog")).unwrap();
        assert_eq!(reopened.bindings.resolve_single(users), Ok(target_storage));
        assert_eq!(
            reopened
                .query("SELECT id, email FROM users ORDER BY id")
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
fn source_backfill_crash_child() {
    let Ok(root) = std::env::var("NETBADB_ROUND38_CRASH_ROOT") else {
        return;
    };
    let mut db = Database::open_catalog(Path::new(&root).join("catalog")).unwrap();
    execute_foundation_transaction(&mut db);
    panic!("configured Round 38 crash hook was not reached");
}

#[test]
fn source_backfill_partial_winner_matrix_converges_on_three_reopens() {
    for (name, point, reverse) in [
        ("both-prepared", "after-durable-decision", false),
        ("source-committed", "after-commit-1", false),
        ("target-committed", "after-commit-1", true),
        ("both-committed", "after-all-commits", false),
    ] {
        let root = root(name);
        seed(&root).close().unwrap();
        let expected_target = schema_catalog_file::load(&root.join("catalog"))
            .unwrap()
            .committed
            .next_storage_id
            .unwrap();
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "source_backfill_tests::source_backfill_crash_child",
                "--nocapture",
            ])
            .env("NETBADB_ROUND38_CRASH_ROOT", &root);
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
        assert_recovered_winner(&root, expected_target);
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn source_backfill_postdecision_publication_crashes_converge() {
    for point in [
        "coordinator-durable",
        "staged-heap-committed",
        "composition-after-promotion-1",
        "composition-after-retirement-1",
        "composition-before-nbsc-publication",
        "composition-nbsc-durable",
        "composition-after-cord-complete",
        "composition-before-winner-resolution",
        "composition-after-winner-resolution",
        "composition-before-memory-publication",
        "composition-memory-published",
        "composition-before-api-return",
    ] {
        let root = root(point);
        seed(&root).close().unwrap();
        let expected_target = schema_catalog_file::load(&root.join("catalog"))
            .unwrap()
            .committed
            .next_storage_id
            .unwrap();
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "source_backfill_tests::source_backfill_crash_child",
                "--nocapture",
            ])
            .env("NETBADB_ROUND38_CRASH_ROOT", &root)
            .env("NETBADB_BACKFILL_CRASH_POINT", point);
        let output = command.output().unwrap();
        assert_eq!(
            output.status.code(),
            Some(90),
            "{point}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_recovered_winner(&root, expected_target);
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn source_backfill_winner_rejects_wrong_source_transaction_and_missing_target() {
    for corrupt_source_transaction in [true, false] {
        let root = root(if corrupt_source_transaction {
            "wrong-source-transaction"
        } else {
            "missing-target"
        });
        seed(&root).close().unwrap();
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "source_backfill_tests::source_backfill_crash_child",
                "--nocapture",
            ])
            .env("NETBADB_ROUND38_CRASH_ROOT", &root);
        coordinator_crash::configure_child(
            &mut command,
            "source-authority-negative",
            &root,
            "after-durable-decision",
        );
        let output = command.output().unwrap();
        assert_eq!(output.status.code(), Some(coordinator_crash::EXIT_CODE));
        let catalog = root.join("catalog");
        let incarnation = schema_catalog_file::marker(&catalog)
            .unwrap()
            .unwrap()
            .incarnation;
        let mut journal =
            schema_mutation_journal::SchemaMutationJournal::open(&catalog, incarnation)
                .unwrap()
                .unwrap();
        if corrupt_source_transaction {
            let source = journal.source_backfill_intents.values_mut().next().unwrap();
            source.source_physical_txn_id.0 += 1;
            let bytes = journal.encode().unwrap();
            std::fs::write(&journal.path, bytes).unwrap();
            assert!(matches!(
                Database::open_catalog(&catalog),
                Err(DatabaseError::SchemaMutation(SchemaMutationError::Corrupt(
                    "source-backfill coordinator participant mismatch"
                )))
            ));
        } else {
            let source = journal.source_backfill_intents.values().next().unwrap();
            let stage = schema_catalog_file::resolve(&catalog, &source.target_stage_locator);
            std::fs::remove_file(stage).unwrap();
            assert!(Database::open_catalog(&catalog).is_err());
        }
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn mutated_source_is_replacement_retired_and_existing_gc_preserves_target() {
    let root = root("mutated-source-gc");
    let mut db = seed(&root);
    let users = db.schema().table("users").unwrap().id;
    let source_storage = db.bindings.resolve_single(users).unwrap();
    execute_foundation_transaction(&mut db);
    let target_storage = db.bindings.resolve_single(users).unwrap();
    let retired = db
        .inspect_replacement_retired_heaps()
        .into_iter()
        .find(|retired| retired.old_storage_id == source_storage)
        .unwrap();
    let inspection = db.inspect_replacement_retired_heap_gc(&retired).unwrap();
    assert!(inspection.eligible(), "{:?}", inspection.blockers);
    db.gc_replacement_retired_heap(&retired).unwrap();
    assert_eq!(
        db.inspect_replacement_retired_heap_gc(&retired)
            .unwrap()
            .state,
        RetiredHeapGcState::Deleted
    );
    assert_eq!(db.bindings.resolve_single(users), Ok(target_storage));
    assert_eq!(db.indexes(users).unwrap().len(), 1);
    assert_eq!(
        db.query("SELECT id FROM users ORDER BY id").unwrap().rows,
        vec![
            vec![ScalarValue::Int64(1)],
            vec![ScalarValue::Int64(3)],
            vec![ScalarValue::Int64(4)],
        ]
    );
    db.close().unwrap();
    assert_recovered_winner(&root, target_storage);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn source_backfill_predecision_crash_matrix_restores_the_base() {
    for point in [
        "source-backfill-target-reserved",
        "source-backfill-intent-durable",
        "source-backfill-stage-intent-durable",
        "composition-stage-first-file",
        "source-backfill-mid-copy",
        "composition-table-copy-complete",
        "source-backfill-final-indexes-built",
        "source-backfill-target-validated",
        "source-backfill-index-finalization-intent-durable",
        "after-prepare-1",
        "after-prepare-2",
        "after-all-prepares",
        "composition-prepared-catalog-durable",
        "before-coordinator-decision",
    ] {
        let root = root(point);
        let seeded = seed(&root);
        let users = seeded.schema().table("users").unwrap().id;
        let source_storage = seeded.bindings.resolve_single(users).unwrap();
        seeded.close().unwrap();
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "source_backfill_tests::source_backfill_crash_child",
                "--nocapture",
            ])
            .env("NETBADB_ROUND38_CRASH_ROOT", &root);
        let expected_exit = if point.starts_with("after-") {
            coordinator_crash::configure_child(&mut command, point, &root, point);
            coordinator_crash::EXIT_CODE
        } else {
            command.env("NETBADB_BACKFILL_CRASH_POINT", point);
            90
        };
        let output = command.output().unwrap();
        assert_eq!(
            output.status.code(),
            Some(expected_exit),
            "{point}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_recovered_base(&root, source_storage);
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn failed_source_validation_can_be_backfilled_and_retried_without_early_target() {
    let root = root("native-retry");
    let mut db = seed(&root);
    let users = db.schema().table("users").unwrap().id;
    let email = db
        .schema()
        .table("users")
        .unwrap()
        .column("email")
        .unwrap()
        .id;
    let source_storage = db.bindings.resolve_single(users).unwrap();
    let target_floor = db.next_storage_id();
    let mut transaction = db.begin_transaction().unwrap();
    db.execute_in(&mut transaction, "DROP INDEX users_email_idx")
        .unwrap();
    db.open_source_backfill(&mut transaction).unwrap();
    let dependency = transaction
        .schema_composition
        .plan()
        .unwrap()
        .dependency(users)
        .unwrap();
    assert!(matches!(
        db.apply_source_backfill_refinement(
            &mut transaction,
            AlterTableSpec::new(
                dependency,
                AlterTableOperation::SetNotNull { column_id: email },
            ),
        ),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::NotNullViolation(column)
        )) if column == email
    ));
    assert!(matches!(
        transaction.schema_composition,
        schema_composition::SchemaCompositionState::SourceBackfillOpen(_)
    ));
    assert_eq!(db.next_storage_id(), target_floor);
    assert!(
        db.mutation_journal
            .as_ref()
            .unwrap()
            .borrow()
            .source_backfill_intents
            .is_empty()
    );
    db.execute_in(
        &mut transaction,
        "UPDATE users SET email = 'filled@example.test' WHERE email IS NULL",
    )
    .unwrap();
    let dependency = transaction
        .schema_composition
        .plan()
        .unwrap()
        .dependency(users)
        .unwrap();
    db.apply_source_backfill_refinement(
        &mut transaction,
        AlterTableSpec::new(
            dependency,
            AlterTableOperation::SetNotNull { column_id: email },
        ),
    )
    .unwrap();
    db.commit_transaction(&mut transaction).unwrap();
    assert_ne!(db.bindings.resolve_single(users), Ok(source_storage));
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn no_alter_and_schema_net_noop_never_allocate_a_target() {
    for net_noop in [false, true] {
        let root = root(if net_noop { "net-noop" } else { "no-alter" });
        let mut db = seed(&root);
        let users = db.schema().table("users").unwrap().id;
        let email = db
            .schema()
            .table("users")
            .unwrap()
            .column("email")
            .unwrap()
            .id;
        let source_storage = db.bindings.resolve_single(users).unwrap();
        let source_version = db.table_schema_version(users).unwrap();
        let source_generation = db.schema_generation();
        let target_floor = db.next_storage_id();
        let mut transaction = db.begin_transaction().unwrap();
        db.execute_in(&mut transaction, "DROP INDEX users_email_idx")
            .unwrap();
        db.open_source_backfill(&mut transaction).unwrap();
        db.execute_in(
            &mut transaction,
            "UPDATE users SET email = 'filled@example.test' WHERE email IS NULL",
        )
        .unwrap();
        if net_noop {
            let dependency = transaction
                .schema_composition
                .plan()
                .unwrap()
                .dependency(users)
                .unwrap();
            db.apply_source_backfill_refinement(
                &mut transaction,
                AlterTableSpec::new(
                    dependency,
                    AlterTableOperation::RenameColumn {
                        column_id: email,
                        new_name: "contact".into(),
                    },
                ),
            )
            .unwrap();
            let dependency = transaction
                .schema_composition
                .plan()
                .unwrap()
                .dependency(users)
                .unwrap();
            db.apply_source_backfill_refinement(
                &mut transaction,
                AlterTableSpec::new(
                    dependency,
                    AlterTableOperation::RenameColumn {
                        column_id: email,
                        new_name: "email".into(),
                    },
                ),
            )
            .unwrap();
        }
        let transaction_id = transaction.id();
        db.commit_transaction(&mut transaction).unwrap();
        assert_eq!(db.bindings.resolve_single(users), Ok(source_storage));
        assert_eq!(db.table_schema_version(users), Some(source_version));
        assert_eq!(db.schema_generation(), source_generation);
        assert_eq!(db.next_storage_id(), target_floor);
        assert!(db.indexes(users).unwrap().is_empty());
        let journal = db.mutation_journal.as_ref().unwrap().borrow();
        assert!(
            !journal
                .source_backfill_intents
                .contains_key(&transaction_id)
        );
        assert!(!journal.stage_intents.contains_key(&transaction_id));
        assert!(
            !journal
                .migration_finalization_intents
                .contains_key(&transaction_id)
        );
        drop(journal);
        db.close().unwrap();
        for _ in 0..3 {
            Database::open_catalog(root.join("catalog"))
                .unwrap()
                .close()
                .unwrap();
        }
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
#[ignore = "explicit deterministic Round 38 fuzz corpus generation"]
fn write_source_backfill_fuzz_corpus() {
    let output = PathBuf::from(
        std::env::var("NETBADB_ROUND38_CORPUS").expect("explicit Round 38 corpus output directory"),
    );
    std::fs::create_dir_all(output.join("schema_mutation_decode")).unwrap();
    std::fs::create_dir_all(output.join("coordinator_log_decode")).unwrap();
    let root = root("corpus");
    let mut db = seed(&root);
    execute_foundation_transaction(&mut db);
    let journal = db
        .mutation_journal
        .as_ref()
        .unwrap()
        .borrow()
        .encode()
        .unwrap();
    std::fs::write(
        output.join("schema_mutation_decode/source-backfill-winner-v1"),
        &journal,
    )
    .unwrap();
    std::fs::write(
        output.join("schema_mutation_decode/source-backfill-truncated-v1"),
        &journal[..journal.len() - 17],
    )
    .unwrap();
    std::fs::write(
        output.join("coordinator_log_decode/source-backfill-two-participant-v2"),
        std::fs::read(root.join("coordinator")).unwrap(),
    )
    .unwrap();
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}
