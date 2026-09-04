use std::path::{Path, PathBuf};

use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_types::{ColumnId, PhysicalType, ScalarValue, StorageId, TableId};

use super::*;
use crate::schema_composition::SchemaCompositionState;
use crate::schema_mutation::BackfillRefinementReason;

fn root(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "netbadb-round43-{name}-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    std::fs::create_dir(&path).unwrap();
    path
}

fn seed(root: &Path, teams: bool) -> Database {
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
    db.execute("INSERT INTO users VALUES (1, 'old-one', NULL)")
        .unwrap();
    db.execute("INSERT INTO users VALUES (2, 'old-two', 'two@example.test')")
        .unwrap();
    db.execute("INSERT INTO users VALUES (3, 'old-three', 'three@example.test')")
        .unwrap();
    db.execute("CREATE INDEX users_email_idx ON users(email)")
        .unwrap();
    if teams {
        db.execute("CREATE TABLE teams (id BIGINT NOT NULL, name TEXT)")
            .unwrap();
        db.execute("INSERT INTO teams VALUES (1, 'one')").unwrap();
    }
    db
}

fn users_target(db: &Database) -> SchemaDependency {
    db.resolve_alter_table("users").unwrap()
}

fn journal_bytes(db: &Database) -> Vec<u8> {
    db.mutation_journal
        .as_ref()
        .map(|journal| journal.borrow().encode().unwrap())
        .unwrap_or_default()
}

fn resource_bytes(path: &Path) -> u64 {
    let mut total = 0_u64;
    for entry in std::fs::read_dir(path).unwrap() {
        let entry = entry.unwrap();
        let metadata = entry.metadata().unwrap();
        if metadata.is_dir() {
            total += resource_bytes(&entry.path());
        } else {
            total += metadata.len();
        }
    }
    total
}

fn assert_source_adoption_rejected(error: DatabaseError) {
    assert!(matches!(
        error,
        DatabaseError::SchemaMutation(SchemaMutationError::UnsupportedBackfillRefinement(
            BackfillRefinementReason::CrossTableAccess
                | BackfillRefinementReason::UnsupportedOperation
        )) | DatabaseError::SchemaMutation(SchemaMutationError::UnsupportedPlacement)
            | DatabaseError::SchemaMutation(SchemaMutationError::StaleSchemaDependency)
    ));
}

#[test]
fn production_update_then_alter_surface_remains_closed() {
    for (name, alter) in [
        ("negative-add", "ALTER TABLE users ADD COLUMN marker TEXT"),
        ("negative-drop", "ALTER TABLE users DROP COLUMN legacy"),
        (
            "negative-rename",
            "ALTER TABLE users RENAME COLUMN email TO contact",
        ),
        (
            "negative-not-null",
            "ALTER TABLE users ALTER COLUMN email SET NOT NULL",
        ),
    ] {
        let root = root(name);
        let mut db = seed(&root, false);
        let mut transaction = db.begin_transaction().unwrap();
        db.execute_in(
            &mut transaction,
            "UPDATE users SET email = email WHERE id = 1",
        )
        .unwrap();
        assert!(matches!(
            db.execute_in(&mut transaction, alter),
            Err(DatabaseError::SchemaMutation(
                SchemaMutationError::TransactionNotPristine
            ))
        ));
        transaction.rollback().unwrap();
        db.close().unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn schema_is_stable_and_late_writer_requires_exclusive_transaction_ownership() {
    let root = root("writer-exclusivity");
    let mut db = seed(&root, false);
    let target = users_target(&db);
    let mut writer = db.begin_transaction().unwrap();
    db.execute_in(&mut writer, "UPDATE users SET email = email WHERE id = 1")
        .unwrap();
    let mut reader = db.begin_transaction().unwrap();
    db.execute_in(&mut reader, "SELECT id FROM users").unwrap();
    let journal = journal_bytes(&db);
    assert!(matches!(
        db.audit_post_dml_source_adoption_evidence(&writer, &target),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::SchemaBusy
        ))
    ));
    assert!(db.schema_writer.get().is_none());
    assert_eq!(journal_bytes(&db), journal);
    assert_eq!(writer.state(), TransactionState::Active);

    reader.rollback().unwrap();
    drop(reader);
    let evidence = db
        .audit_adopt_post_dml_source(&mut writer, &target)
        .unwrap();
    assert_eq!(evidence.table, TableId(2));
    assert_eq!(evidence.table_version, target.table_version);
    assert_eq!(evidence.fingerprint, target.fingerprint);
    assert_eq!(evidence.base_generation, db.schema_generation());
    assert_eq!(
        evidence.base_epoch,
        schema_catalog_file::load(&root.join("catalog"))
            .unwrap()
            .epoch
    );
    assert_eq!(evidence.storage, writer.write_participant().unwrap());
    assert_eq!(
        Some(evidence.physical_txn_id),
        writer.physical_transaction_id(evidence.storage)
    );
    assert_eq!(db.schema_writer.get(), Some(writer.id()));
    assert!(matches!(
        db.begin_transaction(),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::SchemaBusy
        ))
    ));
    writer.rollback().unwrap();
    assert!(db.schema_writer.get().is_none());
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn zero_row_insert_update_and_delete_all_provide_the_same_source_identity() {
    for (name, dml) in [
        (
            "zero-row-update",
            "UPDATE users SET email = email WHERE id = -1",
        ),
        (
            "insert-first",
            "INSERT INTO users VALUES (4, 'old-four', 'four@example.test')",
        ),
        (
            "update-first",
            "UPDATE users SET legacy = 'changed' WHERE id = 1",
        ),
        ("delete-first", "DELETE FROM users WHERE id = 1"),
    ] {
        let root = root(name);
        let mut db = seed(&root, false);
        let target = users_target(&db);
        let mut transaction = db.begin_transaction().unwrap();
        db.execute_in(&mut transaction, dml).unwrap();
        let evidence = db
            .audit_post_dml_source_adoption_evidence(&transaction, &target)
            .unwrap();
        assert_eq!(transaction.participant_count(), 1);
        assert_eq!(transaction.write_participant(), Some(evidence.storage));
        assert!(
            transaction
                .physical_transaction_id(evidence.storage)
                .is_some()
        );
        assert!(!evidence.source_locator.is_empty());
        assert_ne!(evidence.index_digest, [0; 32]);
        transaction.rollback().unwrap();
        db.close().unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn strict_one_table_policy_rejects_cross_table_reads_writes_and_targets() {
    for (name, setup, target_name) in [
        (
            "two-writes",
            &[
                "UPDATE users SET email = email WHERE id = 1",
                "UPDATE teams SET name = name WHERE id = 1",
            ] as &[&str],
            "users",
        ),
        (
            "cross-read",
            &[
                "SELECT id FROM teams",
                "UPDATE users SET email = email WHERE id = 1",
            ] as &[&str],
            "users",
        ),
        (
            "wrong-target",
            &["UPDATE users SET email = email WHERE id = 1"] as &[&str],
            "teams",
        ),
    ] {
        let root = root(name);
        let mut db = seed(&root, true);
        let target = db.resolve_alter_table(target_name).unwrap();
        let mut transaction = db.begin_transaction().unwrap();
        for statement in setup {
            db.execute_in(&mut transaction, statement).unwrap();
        }
        let error = db
            .audit_post_dml_source_adoption_evidence(&transaction, &target)
            .unwrap_err();
        assert_source_adoption_rejected(error);
        assert!(db.schema_writer.get().is_none());
        transaction.rollback().unwrap();
        db.close().unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn pending_index_writes_are_not_misclassified_as_row_source_provenance() {
    for (name, index_ddl) in [
        (
            "pending-create",
            "CREATE INDEX users_legacy_idx ON users(legacy)",
        ),
        ("pending-drop", "DROP INDEX users_email_idx"),
    ] {
        let root = root(name);
        let mut db = seed(&root, false);
        let target = users_target(&db);
        let mut transaction = db.begin_transaction().unwrap();
        db.execute_in(
            &mut transaction,
            "UPDATE users SET legacy = legacy WHERE id = 1",
        )
        .unwrap();
        db.execute_in(&mut transaction, index_ddl).unwrap();
        assert!(transaction.schema_composition.is_none());
        assert!(transaction.has_pending_index_creations() || transaction.has_pending_index_drops());
        let error = db
            .audit_post_dml_source_adoption_evidence(&transaction, &target)
            .unwrap_err();
        assert_source_adoption_rejected(error);
        transaction.rollback().unwrap();
        db.close().unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn candidate_a_identity_index_intent_is_rejected_without_journal_mutation() {
    let root = root("candidate-a");
    let mut db = seed(&root, false);
    let target = users_target(&db);
    let mut transaction = db.begin_transaction().unwrap();
    db.execute_in(
        &mut transaction,
        "UPDATE users SET email = email WHERE id = 1",
    )
    .unwrap();
    db.audit_adopt_post_dml_source(&mut transaction, &target)
        .unwrap();
    let before = journal_bytes(&db);
    assert!(matches!(
        db.audit_candidate_a_identity_intent(&transaction),
        Err(DatabaseError::SchemaMutation(SchemaMutationError::Corrupt(
            "invalid in-place index delta"
        )))
    ));
    assert_eq!(journal_bytes(&db), before);
    transaction.rollback().unwrap();
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn candidate_b_reservation_before_intent_rolls_back_schema_but_burns_column() {
    let root = root("candidate-b-rollback");
    let mut db = seed(&root, false);
    let target = users_target(&db);
    let table = target.table_id;
    let source = db.bindings.resolve_single(table).unwrap();
    let storage_floor = db.next_storage_id();
    let column = db.next_column_id(table).unwrap();
    let mut transaction = db.begin_transaction().unwrap();
    db.execute_in(
        &mut transaction,
        "UPDATE users SET legacy = 'loser' WHERE id = 1",
    )
    .unwrap();
    db.audit_adopt_post_dml_source(&mut transaction, &target)
        .unwrap();
    db.execute_in(&mut transaction, "ALTER TABLE users ADD COLUMN marker TEXT")
        .unwrap();
    assert_eq!(db.next_column_id(table), Some(ColumnId(column.0 + 1)));
    {
        let journal = db.mutation_journal.as_ref().unwrap().borrow();
        let record = &journal.compositions[&transaction.id()];
        assert_eq!(record.reservations.len(), 1);
        assert!(record.index_intent.is_none());
    }
    transaction.rollback().unwrap();
    drop(transaction);
    db.close().unwrap();

    let mut reopened = Database::open_catalog(root.join("catalog")).unwrap();
    assert_eq!(reopened.bindings.resolve_single(table), Ok(source));
    assert_eq!(reopened.next_storage_id(), storage_floor);
    assert_eq!(reopened.next_column_id(table), Some(ColumnId(column.0 + 1)));
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
            .query("SELECT legacy FROM users WHERE id = 1")
            .unwrap()
            .rows,
        [vec![ScalarValue::Text("old-one".into())]]
    );
    reopened.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn candidate_b_add_drop_noop_commits_dml_on_s1_without_schema_publication() {
    let root = root("candidate-b-noop");
    let mut db = seed(&root, false);
    let catalog = root.join("catalog");
    let target = users_target(&db);
    let table = target.table_id;
    let source = db.bindings.resolve_single(table).unwrap();
    let version = db.table_schema_version(table).unwrap();
    let generation = db.schema_generation();
    let epoch = schema_catalog_file::load(&catalog).unwrap().epoch;
    let revision = db.catalog_generation();
    let storage_floor = db.next_storage_id();
    let column = db.next_column_id(table).unwrap();
    let mut transaction = db.begin_transaction().unwrap();
    db.execute_in(
        &mut transaction,
        "UPDATE users SET legacy = 'winner' WHERE id = 1",
    )
    .unwrap();
    db.audit_adopt_post_dml_source(&mut transaction, &target)
        .unwrap();
    db.execute_in(
        &mut transaction,
        "ALTER TABLE users ADD COLUMN temporary TEXT",
    )
    .unwrap();
    db.execute_in(&mut transaction, "ALTER TABLE users DROP COLUMN temporary")
        .unwrap();
    db.audit_finalize_adopted_source(&mut transaction).unwrap();
    assert!(matches!(
        transaction.schema_composition,
        SchemaCompositionState::SealedNoEffectiveChange(_)
    ));
    let journal = db.mutation_journal.as_ref().unwrap().borrow();
    let record = &journal.compositions[&transaction.id()];
    assert_eq!(record.reservations.len(), 1);
    assert!(record.index_intent.is_none());
    assert!(
        !journal
            .source_backfill_intents
            .contains_key(&transaction.id())
    );
    assert!(!journal.stage_intents.contains_key(&transaction.id()));
    assert!(
        !journal
            .migration_finalization_intents
            .contains_key(&transaction.id())
    );
    drop(journal);
    db.commit_transaction(&mut transaction).unwrap();
    assert_eq!(db.bindings.resolve_single(table), Ok(source));
    assert_eq!(db.table_schema_version(table), Some(version));
    assert_eq!(db.schema_generation(), generation);
    assert_eq!(schema_catalog_file::load(&catalog).unwrap().epoch, epoch);
    assert_eq!(db.catalog_generation(), revision);
    assert_eq!(db.next_storage_id(), storage_floor);
    assert_eq!(db.next_column_id(table), Some(ColumnId(column.0 + 1)));
    assert_eq!(
        db.query("SELECT legacy FROM users WHERE id = 1")
            .unwrap()
            .rows,
        [vec![ScalarValue::Text("winner".into())]]
    );
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn candidate_b_effective_change_uses_transaction_view_one_s2_and_one_pass() {
    let root = root("candidate-b-effective");
    let mut db = seed(&root, false);
    let target = users_target(&db);
    let table = target.table_id;
    let source = db.bindings.resolve_single(table).unwrap();
    let target_storage = db.next_storage_id().unwrap();
    let version = db.table_schema_version(table).unwrap();
    let prepared_add = db
        .prepare_ddl_statement("ALTER TABLE users ADD COLUMN marker TEXT")
        .unwrap();
    let mut transaction = db.begin_transaction().unwrap();
    for statement in [
        "UPDATE users SET email = 'filled@example.test' WHERE email IS NULL",
        "INSERT INTO users VALUES (4, 'old-four', 'four@example.test')",
        "DELETE FROM users WHERE id = 2",
    ] {
        db.execute_in(&mut transaction, statement).unwrap();
    }
    let evidence = db
        .audit_adopt_post_dml_source(&mut transaction, &target)
        .unwrap();
    assert_eq!(evidence.storage, source);
    db.execute_ddl_in(&mut transaction, &prepared_add).unwrap();
    db.audit_finalize_adopted_source(&mut transaction).unwrap();
    let materialized = transaction.schema_composition.source_backfill().unwrap();
    assert_eq!(materialized.source_copy_passes, 1);
    assert_eq!(materialized.source_rows_copied, 3);
    assert_eq!(transaction.participant_count(), 2);
    db.commit_transaction(&mut transaction).unwrap();

    assert_eq!(db.bindings.resolve_single(table), Ok(target_storage));
    assert_eq!(
        db.table_schema_version(table),
        Some(TableSchemaVersion(version.0 + 1))
    );
    assert_eq!(db.next_storage_id(), Some(StorageId(target_storage.0 + 1)));
    assert_eq!(db.indexes(table).unwrap().len(), 1);
    assert_eq!(
        db.query("SELECT id, email, marker FROM users ORDER BY id")
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
fn candidate_b_transaction_prepared_alter_remains_exact_after_dml() {
    let root = root("candidate-b-transaction-prepare");
    let mut db = seed(&root, false);
    let target = users_target(&db);
    let mut transaction = db.begin_transaction().unwrap();
    let PreparedSqlStatement::Ddl(prepared_rename) = db
        .prepare_sql_statement_in(
            &transaction,
            "ALTER TABLE users RENAME COLUMN email TO contact",
            &[],
        )
        .unwrap()
    else {
        panic!("expected prepared ALTER");
    };
    db.execute_in(
        &mut transaction,
        "UPDATE users SET email = email WHERE id = 1",
    )
    .unwrap();
    db.audit_adopt_post_dml_source(&mut transaction, &target)
        .unwrap();
    db.execute_ddl_in(&mut transaction, &prepared_rename)
        .unwrap();
    db.audit_finalize_adopted_source(&mut transaction).unwrap();
    db.commit_transaction(&mut transaction).unwrap();
    assert!(
        db.schema()
            .table("users")
            .unwrap()
            .column("email")
            .is_none()
    );
    assert_eq!(
        db.schema()
            .table("users")
            .unwrap()
            .column("contact")
            .unwrap()
            .id,
        ColumnId(3)
    );
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn candidate_b_prepared_drop_keeps_exact_column_identity() {
    let root = root("candidate-b-prepared-drop");
    let mut db = seed(&root, false);
    let target = users_target(&db);
    let table = target.table_id;
    let old = db
        .schema()
        .table("users")
        .unwrap()
        .column("legacy")
        .unwrap()
        .id;
    let prepared_drop = db
        .prepare_ddl_statement("ALTER TABLE users DROP COLUMN legacy")
        .unwrap();
    let mut transaction = db.begin_transaction().unwrap();
    db.execute_in(
        &mut transaction,
        "UPDATE users SET email = email WHERE id = 1",
    )
    .unwrap();
    db.audit_adopt_post_dml_source(&mut transaction, &target)
        .unwrap();
    db.execute_ddl_in(&mut transaction, &prepared_drop).unwrap();
    db.execute_in(&mut transaction, "ALTER TABLE users ADD COLUMN legacy TEXT")
        .unwrap();
    db.audit_finalize_adopted_source(&mut transaction).unwrap();
    db.commit_transaction(&mut transaction).unwrap();
    let replacement = db
        .schema()
        .table("users")
        .unwrap()
        .column("legacy")
        .unwrap();
    assert_eq!(old, ColumnId(2));
    assert_eq!(replacement.id, ColumnId(4));
    assert_ne!(replacement.id, old);
    assert_eq!(db.next_column_id(table), Some(ColumnId(5)));
    assert!(
        db.query("SELECT legacy FROM users")
            .unwrap()
            .rows
            .iter()
            .all(|row| row == &[ScalarValue::Null])
    );
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

fn execute_candidate_b_crash_transaction(db: &mut Database) {
    let target = users_target(db);
    let mut transaction = db.begin_transaction().unwrap();
    db.execute_in(
        &mut transaction,
        "UPDATE users SET email = 'filled@example.test' WHERE email IS NULL",
    )
    .unwrap();
    db.audit_adopt_post_dml_source(&mut transaction, &target)
        .unwrap();
    db.execute_in(&mut transaction, "ALTER TABLE users ADD COLUMN marker TEXT")
        .unwrap();
    db.audit_finalize_adopted_source(&mut transaction).unwrap();
    db.commit_transaction(&mut transaction).unwrap();
}

fn assert_candidate_b_loser(root: &Path, source: StorageId) {
    for _ in 0..3 {
        let mut db = Database::open_catalog(root.join("catalog")).unwrap();
        let table = db.schema().table("users").unwrap().id;
        assert_eq!(db.bindings.resolve_single(table), Ok(source));
        assert!(
            db.schema()
                .table("users")
                .unwrap()
                .column("marker")
                .is_none()
        );
        assert_eq!(db.next_column_id(table), Some(ColumnId(5)));
        assert_eq!(
            db.query("SELECT email FROM users WHERE id = 1")
                .unwrap()
                .rows,
            [vec![ScalarValue::Null]]
        );
        db.close().unwrap();
    }
}

fn assert_candidate_b_winner(root: &Path, target: StorageId) {
    for _ in 0..3 {
        let mut db = Database::open_catalog(root.join("catalog")).unwrap();
        let table = db.schema().table("users").unwrap().id;
        assert_eq!(db.bindings.resolve_single(table), Ok(target));
        assert_eq!(
            db.schema()
                .table("users")
                .unwrap()
                .column("marker")
                .unwrap()
                .id,
            ColumnId(4)
        );
        assert_eq!(db.indexes(table).unwrap().len(), 1);
        assert_eq!(
            db.query("SELECT email, marker FROM users WHERE id = 1")
                .unwrap()
                .rows,
            [vec![
                ScalarValue::Text("filled@example.test".into()),
                ScalarValue::Null,
            ]]
        );
        db.close().unwrap();
    }
}

#[test]
fn candidate_b_crash_child() {
    let Ok(root) = std::env::var("NETBADB_ROUND43_CRASH_ROOT") else {
        return;
    };
    let mut db = Database::open_catalog(Path::new(&root).join("catalog")).unwrap();
    execute_candidate_b_crash_transaction(&mut db);
    panic!("configured Round 43 crash hook was not reached");
}

#[test]
fn candidate_b_pre_cord_crashes_are_losers_and_keep_allocator_history() {
    for point in [
        "composition-intent-durable",
        "source-backfill-intent-durable",
        "source-backfill-stage-intent-durable",
        "source-backfill-mid-copy",
    ] {
        let root = root(point);
        let db = seed(&root, false);
        let source = db.bindings.resolve_single(TableId(2)).unwrap();
        db.close().unwrap();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "post_dml_source_adoption_audit_tests::candidate_b_crash_child",
                "--nocapture",
            ])
            .env("NETBADB_ROUND43_CRASH_ROOT", &root)
            .env("NETBADB_BACKFILL_CRASH_POINT", point)
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(90),
            "{point}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_candidate_b_loser(&root, source);
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn candidate_b_post_cord_partial_commits_reuse_existing_winner_recovery() {
    for (name, point, reverse) in [
        ("both-prepared", "after-durable-decision", false),
        ("source-committed", "after-commit-1", false),
        ("target-committed", "after-commit-1", true),
        ("both-committed", "after-all-commits", false),
    ] {
        let root = root(name);
        let db = seed(&root, false);
        let target = db.next_storage_id().unwrap();
        db.close().unwrap();
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "post_dml_source_adoption_audit_tests::candidate_b_crash_child",
                "--nocapture",
            ])
            .env("NETBADB_ROUND43_CRASH_ROOT", &root);
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
        assert_candidate_b_winner(&root, target);
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[derive(Debug)]
struct CostObservation {
    source_after_dml: u64,
    peak_observed: u64,
    final_bytes: u64,
    target_storage: StorageId,
    source_passes: u64,
    copied_rows: u64,
}

fn cost_observation(name: &str, adopted: bool) -> CostObservation {
    let root = root(name);
    let mut db = seed(&root, false);
    let target = users_target(&db);
    let table = target.table_id;
    let target_storage = db.next_storage_id().unwrap();
    let mut transaction = db.begin_transaction().unwrap();
    if !adopted {
        db.execute_in(&mut transaction, "DROP INDEX users_email_idx")
            .unwrap();
    }
    db.execute_in(
        &mut transaction,
        "UPDATE users SET email = 'filled@example.test' WHERE email IS NULL",
    )
    .unwrap();
    let source_after_dml = resource_bytes(&root);
    if adopted {
        db.audit_adopt_post_dml_source(&mut transaction, &target)
            .unwrap();
    }
    db.execute_in(&mut transaction, "ALTER TABLE users ADD COLUMN marker TEXT")
        .unwrap();
    if !adopted {
        db.execute_in(
            &mut transaction,
            "CREATE INDEX users_email_idx ON users(email)",
        )
        .unwrap();
        db.finalize_source_backfill(&mut transaction).unwrap();
    } else {
        db.audit_finalize_adopted_source(&mut transaction).unwrap();
    }
    let materialized = transaction.schema_composition.source_backfill().unwrap();
    let source_passes = materialized.source_copy_passes;
    let copied_rows = materialized.source_rows_copied;
    let peak_observed = resource_bytes(&root);
    db.commit_transaction(&mut transaction).unwrap();
    assert_eq!(db.bindings.resolve_single(table), Ok(target_storage));
    assert_eq!(db.indexes(table).unwrap().len(), 1);
    db.close().unwrap();
    let final_bytes = resource_bytes(&root);
    std::fs::remove_dir_all(root).unwrap();
    CostObservation {
        source_after_dml,
        peak_observed,
        final_bytes,
        target_storage,
        source_passes,
        copied_rows,
    }
}

#[test]
fn candidate_b_physical_cost_retains_one_s1_to_s2_pass_without_s3() {
    let current = cost_observation("cost-current", false);
    let adopted = cost_observation("cost-adopted", true);
    assert_eq!((current.source_passes, current.copied_rows), (1, 3));
    assert_eq!((adopted.source_passes, adopted.copied_rows), (1, 3));
    assert_eq!(current.target_storage, adopted.target_storage);
    assert!(current.peak_observed >= current.source_after_dml);
    assert!(adopted.peak_observed >= adopted.source_after_dml);
    assert!(current.final_bytes >= current.source_after_dml);
    assert!(adopted.final_bytes >= adopted.source_after_dml);
    println!("ROUND43_COST current={current:?} adopted={adopted:?}");
}

#[test]
fn round42_drop_first_source_authority_remains_positive() {
    let root = root("round42-positive");
    let mut db = seed(&root, false);
    let mut transaction = db.begin_transaction().unwrap();
    for statement in [
        "DROP INDEX users_email_idx",
        "UPDATE users SET email = email WHERE id = 1",
        "ALTER TABLE users ADD COLUMN marker TEXT",
    ] {
        db.execute_in(&mut transaction, statement).unwrap();
    }
    db.commit_transaction(&mut transaction).unwrap();
    assert_eq!(
        db.schema()
            .table("users")
            .unwrap()
            .column("marker")
            .unwrap()
            .id,
        ColumnId(4)
    );
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}
