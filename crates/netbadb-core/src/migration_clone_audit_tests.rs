use super::*;
use netbadb_schema::{ColumnDef, Schema, TableDef, TypeSpec};
use netbadb_storage::{HeapRewriteIndex, HeapRewriteIndexes, TableStorage};
use netbadb_types::{ColumnId, IndexId, IndexName, PhysicalType, ScalarValue, StorageId, TableId};
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::rc::Rc;

fn root(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "netbadb-round37-{name}-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    std::fs::create_dir(&path).unwrap();
    path
}

fn seed_users(root: &Path) -> Database {
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

fn rows(result: ExecutionResult) -> Vec<Vec<ScalarValue>> {
    match result {
        ExecutionResult::Query(result) => result.rows,
        ExecutionResult::AffectedRows(_) => panic!("expected query"),
    }
}

#[test]
fn drop_update_commit_preserves_the_existing_in_place_contract() {
    let root = root("drop-update-commit");
    let mut db = seed_users(&root);
    let users = db.schema().table("users").unwrap().id;
    let storage = db.bindings.resolve_single(users).unwrap();
    let old_index = db.indexes(users).unwrap()[0].clone();
    let base_version = db.table_schema_version(users).unwrap();
    let base_fingerprint = db.schema().table("users").unwrap().fingerprint().unwrap();
    let base_generation = db.schema_generation();
    let base_epoch = schema_catalog_file::load(&root.join("catalog"))
        .unwrap()
        .epoch;
    let base_revision = db.catalog_generation();

    let mut transaction = db.begin_transaction().unwrap();
    db.execute_in(&mut transaction, "DROP INDEX users_email_idx")
        .unwrap();
    assert!(matches!(
        transaction.schema_composition,
        schema_composition::SchemaCompositionState::Composing(_)
    ));
    assert_eq!(transaction.participant_count(), 0);
    assert_eq!(transaction.write_participant(), None);
    assert_eq!(transaction.staged_binding(users), None);
    assert_eq!(db.schema_writer.get(), Some(transaction.id()));

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
    assert_eq!(transaction.participant_count(), 1);
    assert_eq!(transaction.write_participant(), Some(storage));
    assert_eq!(
        transaction.participant_mode(storage),
        Some(ParticipantMode::Write)
    );
    assert_eq!(transaction.staged_binding(users), None);
    // The committed registry deliberately continues to expose the old inventory
    // until the source participant commits; the materialized plan below is the
    // transaction-local index truth.
    assert_eq!(
        rows(
            db.execute_in(&mut transaction, "SELECT id, email FROM users ORDER BY id")
                .unwrap()
        )[0],
        vec![
            ScalarValue::Int64(1),
            ScalarValue::Text("filled@example.test".into())
        ]
    );
    assert!(matches!(
        db.begin_transaction(),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::SchemaBusy
        ))
    ));

    let materialized = transaction.schema_composition.materialized_index().unwrap();
    let journal = Rc::clone(&materialized.logical.journal);
    assert!(materialized.target.is_none());
    assert!(materialized.reference.is_none());
    assert!(materialized.staged.is_empty());
    assert!(journal.borrow().stage_intents.is_empty());
    let transaction_id = transaction.id();

    db.commit_transaction(&mut transaction).unwrap();
    assert_eq!(db.bindings.resolve_single(users), Ok(storage));
    assert_eq!(db.table_schema_version(users), Some(base_version));
    assert_eq!(
        db.schema().table("users").unwrap().fingerprint().unwrap(),
        base_fingerprint
    );
    assert_eq!(db.schema_generation(), base_generation);
    assert_eq!(
        schema_catalog_file::load(&root.join("catalog"))
            .unwrap()
            .epoch,
        base_epoch
    );
    assert_eq!(db.catalog_generation(), base_revision + 1);
    assert!(db.indexes(users).unwrap().is_empty());
    assert_eq!(
        db.query("SELECT id, email FROM users ORDER BY id")
            .unwrap()
            .rows[0],
        vec![
            ScalarValue::Int64(1),
            ScalarValue::Text("filled@example.test".into())
        ]
    );
    let decision = transaction
        .coordinator_decisions()
        .unwrap()
        .into_iter()
        .find(|decision| decision.database_txn_id == transaction_id)
        .unwrap();
    assert!(decision.complete);
    assert!(decision.schema.is_none());
    assert_eq!(decision.participants.len(), 1);
    assert_eq!(decision.participants[0].storage_id, storage);
    let record = &journal.borrow().compositions[&transaction_id];
    assert_eq!(
        record.resolution,
        Some(schema_mutation_journal::CompositionResolution::Winner)
    );
    assert!(record.index_intent.is_some());
    assert!(journal.borrow().stage_intents.is_empty());
    assert_ne!(old_index.id, IndexId(0));

    db.close().unwrap();
    let reopened = Database::open_catalog(root.join("catalog")).unwrap();
    assert_eq!(reopened.bindings.resolve_single(users), Ok(storage));
    assert!(reopened.indexes(users).unwrap().is_empty());
    reopened.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn source_transaction_view_streams_exact_rows_directly_into_final_schema() {
    let root = root("source-view-copy");
    let mut db = seed_users(&root);
    let users = db.schema().table("users").unwrap().id;
    let source_storage = db.bindings.resolve_single(users).unwrap();
    let email = db
        .schema()
        .table("users")
        .unwrap()
        .column("email")
        .unwrap()
        .id;
    let old_index = db.indexes(users).unwrap()[0].clone();
    let mut transaction = db.begin_transaction().unwrap();
    db.execute_in(&mut transaction, "DROP INDEX users_email_idx")
        .unwrap();
    db.execute_in(
        &mut transaction,
        "UPDATE users SET email = email WHERE id = 3",
    )
    .unwrap();

    let partial = transaction
        .begin_read_view(&[source_storage], &mut db.registry)
        .unwrap();
    let partial_view = partial
        .iter()
        .find_map(|(id, view)| (id == source_storage).then_some(view))
        .unwrap();
    assert!(
        db.registry
            .get_mut(source_storage)
            .unwrap()
            .scan_columns_with_view(&[email], partial_view)
            .unwrap()
            .iter()
            .any(|(_, values)| matches!(values.as_slice(), [ScalarValue::Null]))
    );

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
    let visible = transaction
        .begin_read_view(&[source_storage], &mut db.registry)
        .unwrap();
    let source_view = visible
        .iter()
        .find_map(|(id, view)| (id == source_storage).then_some(view))
        .unwrap();
    assert!(
        db.registry
            .get_mut(source_storage)
            .unwrap()
            .scan_columns_with_view(&[email], source_view)
            .unwrap()
            .iter()
            .all(|(_, values)| !matches!(values.as_slice(), [ScalarValue::Null]))
    );

    let mut final_table = db.schema().table("users").unwrap().clone();
    final_table
        .columns
        .iter_mut()
        .find(|column| column.id == email)
        .unwrap()
        .nullable = false;
    let target_storage = StorageId(900);
    let target_path = root.join("final-s2.heap");
    let mut target = TableStorage::create_heap_with_storage_id(
        &target_path,
        final_table.clone(),
        target_storage,
    )
    .unwrap();
    let mut target_transaction = target.begin_transaction().unwrap();
    let final_index = HeapRewriteIndex {
        id: IndexId(old_index.id.0 + 1),
        name: Some(IndexName::new("users_email_idx").unwrap()),
        column_id: email,
    };
    let final_indexes = HeapRewriteIndexes {
        active: vec![final_index.clone()],
        next_index_id: IndexId(final_index.id.0 + 1),
    };
    target
        .install_heap_rewrite_indexes_in(&mut target_transaction, &final_indexes)
        .unwrap();

    let source_columns = db
        .schema()
        .table("users")
        .unwrap()
        .columns
        .iter()
        .map(|column| column.id)
        .collect::<Vec<_>>();
    let mut copied = 0_u64;
    let flow = db
        .registry
        .get_mut(source_storage)
        .unwrap()
        .visit_rows_with_view_control::<StorageError, _>(
            &source_columns,
            source_view,
            |_row, values| {
                target.insert_in(&mut target_transaction, &values)?;
                copied += 1;
                Ok(ControlFlow::Continue(()))
            },
        )
        .unwrap();
    assert_eq!(flow, ControlFlow::Continue(()));
    assert_eq!(copied, 3);
    target_transaction.commit().unwrap();
    assert_eq!(target.table(), &final_table);
    assert_eq!(target.indexes().len(), 1);
    assert_eq!(target.indexes()[0].id, final_index.id);
    assert_ne!(target.indexes()[0].id, old_index.id);
    let target_view = target.read_view().unwrap();
    let mut copied_rows = target
        .scan_columns_with_view(&[ColumnId(1), email], &target_view)
        .unwrap()
        .into_iter()
        .map(|(_, values)| values)
        .collect::<Vec<_>>();
    copied_rows.sort_by_key(|values| match values.first() {
        Some(ScalarValue::Int64(id)) => *id,
        _ => panic!("audit fixture row lacks its integer key"),
    });
    assert_eq!(
        copied_rows,
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

    target.close().unwrap();
    transaction.rollback().unwrap();
    assert_eq!(db.indexes(users).unwrap(), std::slice::from_ref(&old_index));
    assert_eq!(
        db.query("SELECT id, email FROM users ORDER BY id")
            .unwrap()
            .rows,
        vec![
            vec![ScalarValue::Int64(1), ScalarValue::Null],
            vec![
                ScalarValue::Int64(2),
                ScalarValue::Text("two@example.test".into())
            ],
            vec![
                ScalarValue::Int64(3),
                ScalarValue::Text("three@example.test".into())
            ],
        ]
    );
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn placement_only_nbsc_accepts_same_logical_identity_and_prepared_rebinds() {
    let root = root("placement-only");
    let catalog = root.join("catalog");
    let table = TableDef::new(
        TableId(1),
        "users",
        vec![ColumnDef::new(
            ColumnId(1),
            "id",
            TypeSpec::Physical(PhysicalType::Int64),
        )],
    );
    let expectation = Schema::new(vec![table.clone()]).unwrap();
    let mut db = Database::create_catalog(
        &catalog,
        vec![TableStorageCreateSpec::heap(
            root.join("s1.heap"),
            table.clone(),
        )],
        None,
    )
    .unwrap();
    db.execute("INSERT INTO users VALUES (7)").unwrap();
    let prepared = db.prepare_statement("SELECT id FROM users", &[]).unwrap();
    let base = schema_catalog_file::load(&catalog).unwrap();
    let base_storage = db.bindings.resolve_single(TableId(1)).unwrap();
    let new_storage = base.committed.next_storage_id.unwrap();
    let new_path = root.join("s2.heap");
    let mut target =
        TableStorage::create_heap_with_storage_id(&new_path, table.clone(), new_storage).unwrap();
    let source_view = db.registry.get(base_storage).unwrap().read_view().unwrap();
    let mut target_transaction = target.begin_transaction().unwrap();
    let copied = db
        .registry
        .get_mut(base_storage)
        .unwrap()
        .visit_rows_with_view_control::<StorageError, _>(
            &[ColumnId(1)],
            &source_view,
            |_row, values| {
                target.insert_in(&mut target_transaction, &values)?;
                Ok(ControlFlow::Continue(()))
            },
        )
        .unwrap();
    assert_eq!(copied, ControlFlow::Continue(()));
    target_transaction.commit().unwrap();
    target.close().unwrap();
    schema_catalog_file::write_link(&new_path, &catalog, base.incarnation).unwrap();

    let mut replacement = base.clone();
    replacement.epoch += 1;
    replacement.committed.next_storage_id = Some(StorageId(new_storage.0 + 1));
    replacement.storages[0].id = new_storage;
    replacement.storages[0].locator = "s2.heap".into();
    replacement.placements.tables[0].placement = TablePlacement::Single {
        table_id: TableId(1),
        storage_id: new_storage,
    };
    replacement.validate().unwrap();
    let bytes = replacement.encode().unwrap();
    assert_eq!(
        schema_catalog::SchemaCatalogSnapshot::decode(&bytes).unwrap(),
        replacement
    );
    let prepared_path = root.join("placement-only.prepared.nbsc");
    schema_catalog_file::atomic_write(&prepared_path, &bytes, false).unwrap();
    assert_eq!(
        schema_catalog::SchemaCatalogSnapshot::decode(
            &schema_catalog_file::read(&prepared_path).unwrap()
        )
        .unwrap(),
        replacement
    );

    db.close().unwrap();
    schema_catalog_file::publish_runtime(&catalog, &replacement).unwrap();
    let mut reopened =
        Database::open_catalog_with_expectation(&catalog, Some(&expectation)).unwrap();
    assert_eq!(reopened.schema_generation(), base.committed.generation);
    assert_eq!(
        reopened.table_schema_version(TableId(1)),
        Some(base.committed.tables[0].version)
    );
    assert_eq!(
        reopened
            .schema()
            .table("users")
            .unwrap()
            .fingerprint()
            .unwrap(),
        table.fingerprint().unwrap()
    );
    assert_eq!(
        reopened.bindings.resolve_single(TableId(1)),
        Ok(new_storage)
    );
    assert_eq!(
        rows(reopened.execute_prepared(&prepared, &[]).unwrap()),
        vec![vec![ScalarValue::Int64(7)]]
    );
    reopened.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn same_identity_stage_and_tag34_are_rejected_without_replacement_authority() {
    let root = root("same-identity-stage-tag34");
    let catalog = root.join("catalog");
    let mut db = seed_users(&root);
    let users = db.schema().table("users").unwrap().id;
    let source_storage = db.bindings.resolve_single(users).unwrap();
    let mut transaction = db.begin_transaction().unwrap();
    db.execute_in(&mut transaction, "DROP INDEX users_email_idx")
        .unwrap();
    db.execute_in(&mut transaction, "UPDATE users SET email = email")
        .unwrap();
    let transaction_id = transaction.id();
    let materialized = transaction.schema_composition.materialized_index().unwrap();
    let journal = Rc::clone(&materialized.logical.journal);
    let base = schema_catalog_file::load(&catalog).unwrap();
    let target_storage = base.committed.next_storage_id.unwrap();
    let stage_locator = schema_mutation_journal::stage_locator(
        &catalog,
        base.incarnation,
        transaction_id,
        target_storage,
    )
    .unwrap();
    let final_locator =
        schema_mutation_journal::final_locator(&catalog, base.incarnation, target_storage).unwrap();

    let mut provisional = base.clone();
    let user = provisional.committed.schema.table("users").unwrap().clone();
    provisional.committed.schema = Schema::new(vec![user.clone()]).unwrap();
    provisional
        .committed
        .tables
        .retain(|lineage| lineage.table_id == users);
    provisional
        .placements
        .tables
        .retain(|entry| entry.table_id == users);
    provisional
        .storages
        .retain(|entry| entry.id == source_storage);
    provisional.storages[0].id = target_storage;
    provisional.storages[0].locator.clone_from(&stage_locator);
    provisional.placements.tables[0].placement = TablePlacement::Single {
        table_id: users,
        storage_id: target_storage,
    };
    provisional.committed.next_storage_id = Some(StorageId(target_storage.0 + 1));
    provisional.partition_evidence = None;
    provisional.validate().unwrap();
    assert_eq!(
        provisional.committed.tables[0].version,
        base.committed
            .tables
            .iter()
            .find(|lineage| lineage.table_id == users)
            .unwrap()
            .version
    );
    assert_eq!(
        user.fingerprint().unwrap(),
        db.schema().table("users").unwrap().fingerprint().unwrap()
    );

    let provisional_digest = schema_mutation::digest(&provisional.encode().unwrap());
    let final_snapshot_digest = schema_mutation::digest(b"round37-same-vf-final-snapshot");
    let final_indexes = HeapRewriteIndexes {
        active: Vec::new(),
        next_index_id: IndexId(2),
    };
    let stage_intent = schema_mutation_journal::StageResourceIntent {
        transaction: transaction_id,
        table: users,
        storage: target_storage,
        base_generation: base.committed.generation,
        base_epoch: base.epoch,
        provisional,
        stage_locator: stage_locator.clone(),
        final_locator: final_locator.clone(),
        digest: provisional_digest,
    };
    let mut stage_probe = journal.borrow().clone();
    let stage_error = stage_probe
        .stage_resource_intent(stage_intent.clone())
        .unwrap_err();
    assert!(matches!(
        stage_error,
        SchemaMutationError::Corrupt("backfill stage target is absent from composition")
    ));
    let mut tag34_probe = journal.borrow().clone();
    tag34_probe
        .stage_intents
        .insert(transaction_id, stage_intent);
    let tag34_error = tag34_probe
        .migration_finalization_intent(schema_mutation_journal::MigrationIndexFinalizationIntent {
            transaction: transaction_id,
            table: users,
            storage: target_storage,
            final_table_version: db.table_schema_version(users).unwrap(),
            final_fingerprint: user.fingerprint().unwrap(),
            final_snapshot_digest,
            stage_locator,
            final_locator,
            final_indexes,
            digest: schema_mutation::digest(b"round37-same-vf-tag34"),
        })
        .unwrap_err();
    assert!(matches!(
        tag34_error,
        SchemaMutationError::Corrupt("backfill stage target is absent from composition")
    ));

    let durable = schema_mutation_journal::SchemaMutationJournal::open(&catalog, base.incarnation)
        .unwrap()
        .unwrap();
    assert!(!durable.stage_intents.contains_key(&transaction_id));
    assert!(
        !durable
            .migration_finalization_intents
            .contains_key(&transaction_id)
    );
    let plan = &durable.compositions[&transaction_id]
        .index_intent
        .as_ref()
        .unwrap()
        .tables[0];
    assert!(matches!(
        plan,
        schema_mutation_journal::SchemaIndexTablePlan::InPlaceIndexDelta {
            storage,
            ..
        } if *storage == source_storage
    ));

    transaction.rollback().unwrap();
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

fn stage_two_same_table_write_participants(
    db: &mut Database,
    transaction: &mut Transaction,
) -> (TableId, StorageId, StorageId) {
    let users = db.schema().table("users").unwrap().id;
    let old_storage = db.bindings.resolve_single(users).unwrap();
    let old_index = db.indexes(users).unwrap()[0].id;
    let target = db.resolve_alter_table("users").unwrap();
    let email = db.resolve_alter_column(&target, "email").unwrap();
    db.rewrite_heap_table_schema_legacy_in(
        transaction,
        AlterTableSpec::new(
            target,
            AlterTableOperation::RenameColumn {
                column_id: email,
                new_name: "contact".into(),
            },
        ),
    )
    .unwrap();
    let new_storage = transaction
        .schema_mutation
        .as_ref()
        .unwrap()
        .reservation
        .storage;
    transaction
        .with_write_storage(old_storage, &mut db.registry, |storage, context| {
            storage.drop_index_in(context, old_index)?;
            storage
                .insert_in(
                    context,
                    &[
                        ScalarValue::Int64(4),
                        ScalarValue::Text("four@example.test".into()),
                    ],
                )
                .map(|_| ())
        })
        .unwrap();
    transaction
        .with_staged_write(|storage, context| {
            storage
                .insert_in(
                    context,
                    &[
                        ScalarValue::Int64(4),
                        ScalarValue::Text("four@example.test".into()),
                    ],
                )
                .map(|_| ())
        })
        .unwrap();
    assert_eq!(transaction.participant_count(), 2);
    assert_eq!(transaction.write_participant(), Some(old_storage));
    assert_eq!(
        transaction.participant_mode(old_storage),
        Some(ParticipantMode::Write)
    );
    assert_eq!(
        transaction.participant_mode(new_storage),
        Some(ParticipantMode::Write)
    );
    (users, old_storage, new_storage)
}

#[test]
fn same_table_source_and_target_can_commit_then_retire_and_gc_mutated_source() {
    let root = root("two-participant-live");
    let mut db = seed_users(&root);
    let mut transaction = db.begin_transaction().unwrap();
    let (users, old_storage, new_storage) =
        stage_two_same_table_write_participants(&mut db, &mut transaction);
    let transaction_id = transaction.id();
    db.commit_transaction(&mut transaction).unwrap();

    let decision = transaction
        .coordinator_decisions()
        .unwrap()
        .into_iter()
        .find(|decision| decision.database_txn_id == transaction_id)
        .unwrap();
    assert!(decision.complete);
    assert!(decision.schema.is_some());
    assert_eq!(
        decision
            .participants
            .iter()
            .map(|participant| participant.storage_id)
            .collect::<Vec<_>>(),
        vec![old_storage, new_storage]
    );
    assert_eq!(db.bindings.resolve_single(users), Ok(new_storage));
    assert_eq!(
        db.query("SELECT id, contact FROM users ORDER BY id")
            .unwrap()
            .rows
            .last(),
        Some(&vec![
            ScalarValue::Int64(4),
            ScalarValue::Text("four@example.test".into())
        ])
    );
    let retired = db
        .inspect_replacement_retired_heaps()
        .into_iter()
        .find(|retired| retired.old_storage_id == old_storage)
        .unwrap();
    drop(transaction);
    let inspection = db.inspect_replacement_retired_heap_gc(&retired).unwrap();
    assert!(inspection.eligible(), "{:?}", inspection.blockers);
    assert_eq!(inspection.coordinator_horizon, Some(transaction_id));
    db.gc_replacement_retired_heap(&retired).unwrap();
    assert_eq!(
        db.inspect_replacement_retired_heap_gc(&retired)
            .unwrap()
            .state,
        RetiredHeapGcState::Deleted
    );
    db.close().unwrap();
    let mut reopened = Database::open_catalog(root.join("catalog")).unwrap();
    assert_eq!(reopened.bindings.resolve_single(users), Ok(new_storage));
    assert_eq!(
        reopened
            .query("SELECT id, contact FROM users ORDER BY id")
            .unwrap()
            .rows
            .len(),
        4
    );
    reopened.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn two_participant_schema_crash_child() {
    let Ok(root) = std::env::var("NETBADB_ROUND37_CRASH_ROOT") else {
        return;
    };
    let mut db = Database::open_catalog(Path::new(&root).join("catalog")).unwrap();
    let mut transaction = db.begin_transaction().unwrap();
    stage_two_same_table_write_participants(&mut db, &mut transaction);
    db.commit_transaction(&mut transaction)
        .expect("commit must reach configured crash point");
    panic!("configured Round 37 crash hook was not reached");
}

#[test]
fn partial_same_table_cord_winner_pins_the_current_recovery_blocker() {
    let root = root("two-participant-crash");
    seed_users(&root).close().unwrap();
    let mut command = std::process::Command::new(std::env::current_exe().unwrap());
    command
        .args([
            "--exact",
            "migration_clone_audit_tests::two_participant_schema_crash_child",
            "--nocapture",
        ])
        .env("NETBADB_ROUND37_CRASH_ROOT", &root);
    coordinator_crash::configure_child(
        &mut command,
        "round37-two-participant",
        &root,
        "after-commit-1",
    );
    let output = command.output().unwrap();
    assert_eq!(
        output.status.code(),
        Some(coordinator_crash::EXIT_CODE),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let expected_target = schema_catalog_file::load(&root.join("catalog"))
        .unwrap()
        .committed
        .next_storage_id
        .unwrap();
    let error = match Database::open_catalog(root.join("catalog")) {
        Ok(_) => panic!("current recovery unexpectedly accepted the omitted source participant"),
        Err(error) => error,
    };
    assert!(
        matches!(error, DatabaseError::MissingCommitParticipant { storage_id, .. } if storage_id == expected_target),
        "{error:?}"
    );
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn no_effective_schema_change_keeps_the_source_storage_path() {
    let root = root("net-no-op");
    let mut db = seed_users(&root);
    let users = db.schema().table("users").unwrap().id;
    let source_storage = db.bindings.resolve_single(users).unwrap();
    let source_version = db.table_schema_version(users).unwrap();
    let source_fingerprint = db.schema().table("users").unwrap().fingerprint().unwrap();
    let mut transaction = db.begin_transaction().unwrap();
    db.execute_in(&mut transaction, "DROP INDEX users_email_idx")
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
    db.execute_in(&mut transaction, "UPDATE users SET email = email")
        .unwrap();
    db.commit_transaction(&mut transaction).unwrap();
    assert_eq!(db.bindings.resolve_single(users), Ok(source_storage));
    assert_eq!(db.table_schema_version(users), Some(source_version));
    assert_eq!(
        db.schema().table("users").unwrap().fingerprint().unwrap(),
        source_fingerprint
    );
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn transaction_visible_view_is_bound_to_the_source_physical_transaction() {
    let root = root("source-view-owner");
    let mut db = seed_users(&root);
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
    let physical_txn = transaction
        .write_context(storage, &mut db.registry)
        .unwrap()
        .id();
    let view = transaction
        .begin_read_view(&[storage], &mut db.registry)
        .unwrap();
    assert_eq!(view.transaction_id(), Some(transaction.id()));
    assert_ne!(physical_txn.0, 0);
    transaction.rollback().unwrap();
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}
