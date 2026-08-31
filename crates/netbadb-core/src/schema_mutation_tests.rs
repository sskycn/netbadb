use std::path::{Path, PathBuf};
use std::process::Command;

use netbadb_schema::{ColumnDef, Schema, TableDef, TypeSpec};
use netbadb_types::{ColumnId, PhysicalType, ScalarValue, SemanticType, StorageId, TableId};

use crate::{
    CreateColumnSpec, CreateTableSpec, Database, DatabaseCoordinatorConfig, ExecutionResult,
    SchemaGeneration, SchemaMutationError, TableSchemaVersion, TableStorageCreateSpec,
    TransactionState,
};

fn root(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "netbadb-round18-{name}-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    std::fs::create_dir(&path).expect("fresh isolated test directory");
    path
}
fn old_table(id: u64, name: &str) -> TableDef {
    TableDef::new(
        TableId(id),
        name,
        vec![ColumnDef::new(
            ColumnId(1),
            "id",
            TypeSpec::Physical(PhysicalType::Int64),
        )],
    )
}
fn seed(path: &Path, coordinator: bool) -> Database {
    let specs = vec![
        TableStorageCreateSpec::heap(path.join("users.heap"), old_table(1, "users")),
        TableStorageCreateSpec::heap(path.join("teams.heap"), old_table(2, "teams")),
    ];
    let config = coordinator.then(|| DatabaseCoordinatorConfig::new(path.join("coordinator")));
    let mut db = Database::create_catalog(path.join("catalog"), specs, config).unwrap();
    db.execute("INSERT INTO users (id) VALUES (1)").unwrap();
    db.execute("INSERT INTO teams (id) VALUES (2)").unwrap();
    db
}
fn spec(name: &str) -> CreateTableSpec {
    CreateTableSpec::new(
        name,
        vec![
            CreateColumnSpec::new(
                "id",
                SemanticType::named("ProjectId", PhysicalType::Int64),
                false,
            ),
            CreateColumnSpec::new(
                "name",
                SemanticType::named("ProjectName", PhysicalType::Text),
                false,
            ),
            CreateColumnSpec::new("active", SemanticType::physical(PhysicalType::Bool), false),
            CreateColumnSpec::new("score", SemanticType::physical(PhysicalType::Int64), true),
            CreateColumnSpec::new("label", SemanticType::physical(PhysicalType::Text), true),
        ],
    )
}
fn rows(result: ExecutionResult) -> Vec<Vec<ScalarValue>> {
    match result {
        ExecutionResult::Query(q) => q.rows,
        _ => panic!("expected query"),
    }
}
fn insert(db: &mut Database, txn: &mut crate::Transaction) {
    let insert = db
        .prepare_statement_in(
            txn,
            "INSERT INTO projects (id, name, active, score, label) VALUES ($1, $2, $3, $4, $5)",
            &[],
        )
        .unwrap();
    assert_eq!(
        db.execute_prepared_in(
            txn,
            &insert,
            &[
                ScalarValue::Int64(10),
                ScalarValue::Text("demo".into()),
                ScalarValue::Bool(true),
                ScalarValue::Null,
                ScalarValue::Null
            ]
        )
        .unwrap(),
        ExecutionResult::AffectedRows(1)
    );
}
fn expected() -> Vec<Vec<ScalarValue>> {
    vec![vec![
        ScalarValue::Int64(10),
        ScalarValue::Text("demo".into()),
        ScalarValue::Bool(true),
        ScalarValue::Null,
        ScalarValue::Null,
    ]]
}

#[test]
fn core_create_same_transaction_dml_publication_dependencies_and_catalog_only_reopen() {
    let root = root("basic");
    let mut db = seed(&root, true);
    let existing = db.prepare_statement("SELECT id FROM users", &[]).unwrap();
    let old_catalog = std::fs::read(root.join("catalog")).unwrap();
    let old_marker = std::fs::read(root.join("catalog.state")).unwrap();
    let mut txn = db.begin_transaction().unwrap();
    db.execute_in(&mut txn, "UPDATE users SET id = 7").unwrap();
    db.execute_in(&mut txn, "UPDATE teams SET id = 8").unwrap();
    assert_eq!(
        db.create_heap_table_in(&mut txn, spec("projects")).unwrap(),
        TableId(3)
    );
    assert_eq!(db.next_table_id(), Some(TableId(4)));
    assert_eq!(db.next_storage_id(), Some(StorageId(4)));
    assert_eq!(db.next_partition_id(), Some(netbadb_types::PartitionId(1)));
    assert!(db.schema().table("projects").is_none());
    assert_eq!(db.registry.len(), 2);
    assert!(db.prepare_statement("SELECT * FROM projects", &[]).is_err());
    assert!(
        db.inspect_catalog()
            .unwrap()
            .tables
            .iter()
            .all(|t| t.name != "projects")
    );
    assert!(matches!(
        db.begin_transaction(),
        Err(crate::DatabaseError::SchemaMutation(
            SchemaMutationError::SchemaBusy
        ))
    ));
    assert!(db.execute("UPDATE users SET id = 99").is_err());
    assert_eq!(std::fs::read(root.join("catalog")).unwrap(), old_catalog);
    assert_eq!(
        std::fs::read(root.join("catalog.state")).unwrap(),
        old_marker
    );
    insert(&mut db, &mut txn);
    let select = db
        .prepare_statement_in(&txn, "SELECT * FROM projects", &[])
        .unwrap();
    assert_eq!(
        rows(db.execute_prepared_in(&mut txn, &select, &[]).unwrap()),
        expected()
    );
    assert!(db.execute_prepared(&select, &[]).is_err());
    assert!(txn.commit().is_err());
    db.commit_transaction(&mut txn).unwrap();
    assert_eq!(txn.state(), TransactionState::Committed);
    assert_eq!(db.schema_generation(), SchemaGeneration(2));
    assert_eq!(db.catalog_generation(), 1);
    assert_eq!(
        db.table_schema_version(TableId(3)),
        Some(TableSchemaVersion(1))
    );
    assert_eq!(
        db.table_schema_version(TableId(1)),
        Some(TableSchemaVersion(1))
    );
    assert_eq!(db.next_column_id(TableId(3)), Some(ColumnId(6)));
    assert_eq!(
        rows(db.execute_prepared(&existing, &[]).unwrap()),
        vec![vec![ScalarValue::Int64(7)]]
    );
    assert_eq!(
        rows(db.execute("SELECT * FROM projects").unwrap()),
        expected()
    );
    assert!(db.execute_prepared(&select, &[]).is_err());
    let table = db.schema().table("projects").unwrap().clone();
    assert!(table.columns.iter().all(|c| !c.primary_key));
    assert_eq!(
        table.columns.iter().map(|c| c.id).collect::<Vec<_>>(),
        (1..=5).map(ColumnId).collect::<Vec<_>>()
    );
    drop(txn);
    db.close().unwrap();
    let bytes = std::fs::read(root.join("catalog")).unwrap();
    let marker = std::fs::read(root.join("catalog.state")).unwrap();
    for _ in 0..3 {
        let mut db = Database::open_catalog(root.join("catalog")).unwrap();
        assert_eq!(db.schema().table("projects"), Some(&table));
        assert_eq!(
            db.bindings.resolve_single(TableId(3)).unwrap(),
            StorageId(3)
        );
        assert_eq!(
            rows(db.execute("SELECT * FROM projects").unwrap()),
            expected()
        );
        assert_eq!(db.schema_generation(), SchemaGeneration(2));
        db.close().unwrap();
        assert_eq!(std::fs::read(root.join("catalog")).unwrap(), bytes);
        assert_eq!(std::fs::read(root.join("catalog.state")).unwrap(), marker);
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn rollback_burns_ids_name_reuse_and_transaction_prepared_scope() {
    let root = root("rollback");
    let mut db = seed(&root, false);
    let mut txn = db.begin_transaction().unwrap();
    let rolled = db.create_heap_table_in(&mut txn, spec("projects")).unwrap();
    assert_eq!(rolled, TableId(3));
    insert(&mut db, &mut txn);
    let select = db
        .prepare_statement_in(&txn, "SELECT * FROM projects", &[])
        .unwrap();
    txn.rollback().unwrap();
    assert_eq!(txn.state(), TransactionState::RolledBack);
    assert_eq!(db.schema_generation(), SchemaGeneration(1));
    assert_eq!(db.catalog_generation(), 0);
    assert!(db.schema().table("projects").is_none());
    assert!(db.execute_prepared_in(&mut txn, &select, &[]).is_err());
    drop(txn);
    db.close().unwrap();
    let mut db = Database::open_catalog(root.join("catalog")).unwrap();
    assert_eq!(db.next_table_id(), Some(TableId(4)));
    assert_eq!(db.next_storage_id(), Some(StorageId(4)));
    let mut txn = db.begin_transaction().unwrap();
    assert_eq!(
        db.create_heap_table_in(&mut txn, spec("projects")).unwrap(),
        TableId(4)
    );
    assert!(db.execute_prepared_in(&mut txn, &select, &[]).is_err());
    db.commit_transaction(&mut txn).unwrap();
    assert_eq!(
        db.bindings.resolve_single(TableId(4)).unwrap(),
        StorageId(4)
    );
    assert!(db.query("SELECT * FROM projects").unwrap().rows.is_empty());
    drop(txn);
    db.close().unwrap();
    for _ in 0..3 {
        let db = Database::open_catalog(root.join("catalog")).unwrap();
        assert_eq!(db.schema().table("projects").unwrap().id, TableId(4));
        db.close().unwrap();
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn exclusive_retained_handle_admission_validation_and_mixing() {
    let root = root("admission");
    let mut db = seed(&root, true);
    let retained = db.begin_transaction().unwrap();
    let mut txn = db.begin_transaction().unwrap();
    assert!(matches!(
        db.create_heap_table_in(&mut txn, spec("projects")),
        Err(crate::DatabaseError::SchemaMutation(
            SchemaMutationError::SchemaBusy
        ))
    ));
    drop(retained);
    for invalid in [
        spec("users"),
        spec(""),
        CreateTableSpec::new(
            "projects",
            vec![spec("x").columns[0].clone(), spec("x").columns[0].clone()],
        ),
    ] {
        assert!(db.create_heap_table_in(&mut txn, invalid).is_err());
        assert_eq!(db.next_table_id(), Some(TableId(3)));
        assert_eq!(db.next_storage_id(), Some(StorageId(3)));
    }
    db.create_heap_table_in(&mut txn, spec("projects")).unwrap();
    assert!(matches!(
        db.create_heap_table_in(&mut txn, spec("other")),
        Err(crate::DatabaseError::SchemaMutation(
            SchemaMutationError::MultipleCreatesUnsupported
        ))
    ));
    let ddl = db
        .prepare_ddl_statement("CREATE INDEX user_id ON users (id)")
        .unwrap();
    assert!(db.execute_ddl_in(&mut txn, &ddl).is_err());
    assert!(db.create_index(TableId(1), ColumnId(1)).is_err());
    assert!(
        db.prepare_ddl_statement("CREATE INDEX project_id ON projects (id)")
            .is_err()
    );
    let null = db
        .prepare_statement_in(
            &txn,
            "INSERT INTO projects (id, name, active, score, label) VALUES ($1, $2, $3, $4, $5)",
            &[],
        )
        .unwrap();
    assert!(
        db.execute_prepared_in(
            &mut txn,
            &null,
            &[
                ScalarValue::Null,
                ScalarValue::Text("bad".into()),
                ScalarValue::Bool(true),
                ScalarValue::Null,
                ScalarValue::Null
            ]
        )
        .is_err()
    );
    if txn.state() == TransactionState::Active {
        txn.rollback().unwrap();
    }
    assert!(db.schema().table("projects").is_none());
    drop(txn);
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn empty_catalog_empty_columns_and_multiple_commits_preserve_high_waters() {
    let root = root("empty");
    let mut db = Database::create_catalog(root.join("catalog"), vec![], None).unwrap();
    for n in 1..=3 {
        let mut txn = db.begin_transaction().unwrap();
        let id = db
            .create_heap_table_in(&mut txn, CreateTableSpec::new(format!("table{n}"), vec![]))
            .unwrap();
        assert_eq!(id, TableId(n));
        db.commit_transaction(&mut txn).unwrap();
        drop(txn);
    }
    assert_eq!(db.schema_generation(), SchemaGeneration(4));
    db.close().unwrap();
    let db = Database::open_catalog(root.join("catalog")).unwrap();
    assert_eq!(db.schema().tables().len(), 3);
    assert_eq!(db.next_table_id(), Some(TableId(4)));
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn commit_sync_failures_remain_retry_only_and_cleanup_failure_is_rollback_pending() {
    for fault in [
        "decision-append",
        "decision-sync",
        "complete-append",
        "complete-sync",
    ] {
        let root = root(fault);
        let mut db = seed(&root, true);
        let mut txn = db.begin_transaction().unwrap();
        db.create_heap_table_in(&mut txn, spec("projects")).unwrap();
        insert(&mut db, &mut txn);
        {
            let mut log = db.coordinator.as_ref().unwrap().borrow_mut();
            match fault {
                "decision-append" => log.inject_decision_append_failure(),
                "decision-sync" => log.inject_decision_sync_failure(),
                "complete-append" => log.inject_complete_append_failure(),
                _ => log.inject_complete_sync_failure(),
            }
        }
        assert!(db.commit_transaction(&mut txn).is_err());
        assert!(matches!(
            txn.state(),
            TransactionState::DecisionPending | TransactionState::FinalizePending
        ));
        assert!(txn.rollback().is_err());
        assert!(db.schema().table("projects").is_none());
        db.commit_transaction(&mut txn).unwrap();
        assert_eq!(
            rows(db.execute("SELECT * FROM projects").unwrap()),
            expected()
        );
        drop(txn);
        db.close().unwrap();
        Database::open_catalog(root.join("catalog"))
            .unwrap()
            .close()
            .unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
    let root = root("cleanup-pending");
    let mut db = seed(&root, true);
    let mut txn = db.begin_transaction().unwrap();
    db.create_heap_table_in(&mut txn, spec("projects")).unwrap();
    let mutation = txn.schema_mutation.as_ref().unwrap();
    let path = crate::schema_catalog_file::resolve(
        &mutation.catalog,
        &crate::schema_mutation_journal::prepared_locator(
            &mutation.catalog,
            mutation.target.incarnation,
            txn.id(),
        )
        .unwrap(),
    );
    std::fs::create_dir(&path).unwrap();
    assert!(txn.rollback().is_err());
    assert_eq!(txn.state(), TransactionState::RollbackPending);
    assert!(db.begin_transaction().is_err());
    std::fs::remove_dir(path).unwrap();
    txn.rollback().unwrap();
    drop(txn);
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn create_crash_child() {
    let Ok(root) = std::env::var("NETBADB_CREATE_CHILD_ROOT") else {
        return;
    };
    let mut db = Database::open_catalog(Path::new(&root).join("catalog")).unwrap();
    let mut txn = db.begin_transaction().unwrap();
    db.execute_in(&mut txn, "UPDATE users SET id = 7").unwrap();
    if std::env::var_os("NETBADB_CREATE_SKIP_SECOND_WRITE").is_none() {
        db.execute_in(&mut txn, "UPDATE teams SET id = 8").unwrap();
    }
    db.create_heap_table_in(&mut txn, spec("projects")).unwrap();
    insert(&mut db, &mut txn);
    assert_eq!(
        rows(db.execute_in(&mut txn, "SELECT * FROM projects").unwrap()),
        expected()
    );
    if std::env::var("NETBADB_CREATE_CRASH_POINT")
        .unwrap_or_default()
        .starts_with("rollback-")
    {
        txn.rollback().unwrap();
    } else {
        db.commit_transaction(&mut txn).unwrap();
    }
    panic!("configured crash hook was not reached");
}
fn spawn(root: &Path, point: &str) {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args([
            "--exact",
            "schema_mutation_tests::create_crash_child",
            "--nocapture",
        ])
        .env("NETBADB_CREATE_CHILD_ROOT", root)
        .env("NETBADB_CREATE_CRASH_POINT", point);
    if point == "during-decision-append" || point == "after-decision-append" {
        crate::coordinator_crash::configure_child(&mut command, "schema-create", root, point);
    }
    let output = command.output().unwrap();
    assert_eq!(
        output.status.code(),
        Some(if point.contains("decision-append") {
            87
        } else {
            90
        }),
        "{point}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}
fn outcome(root: &Path, winner: bool) {
    for _ in 0..3 {
        let mut db = Database::open_catalog(root.join("catalog")).unwrap();
        assert_eq!(
            db.schema_generation(),
            SchemaGeneration(if winner { 2 } else { 1 })
        );
        assert_eq!(db.next_table_id(), Some(TableId(4)));
        assert_eq!(db.next_storage_id(), Some(StorageId(4)));
        assert_eq!(db.registry.len(), if winner { 3 } else { 2 });
        assert_eq!(
            db.query("SELECT id FROM users").unwrap().rows,
            vec![vec![ScalarValue::Int64(if winner { 7 } else { 1 })]]
        );
        assert_eq!(
            db.query("SELECT id FROM teams").unwrap().rows,
            vec![vec![ScalarValue::Int64(if winner { 8 } else { 2 })]]
        );
        if winner {
            let table = db.schema().table("projects").unwrap();
            assert_eq!(table.id, TableId(3));
            assert_eq!(
                table.columns.iter().map(|c| c.id).collect::<Vec<_>>(),
                (1..=5).map(ColumnId).collect::<Vec<_>>()
            );
            assert_eq!(
                db.table_schema_version(table.id),
                Some(TableSchemaVersion(1))
            );
            assert_eq!(db.next_column_id(table.id), Some(ColumnId(6)));
            assert_eq!(db.bindings.resolve_single(table.id).unwrap(), StorageId(3));
            assert_eq!(db.query("SELECT * FROM projects").unwrap().rows, expected());
        } else {
            assert!(db.schema().table("projects").is_none());
        }
        db.close().unwrap();
    }
}
#[test]
fn subprocess_create_crash_matrix_reopens_three_times_with_exact_outcomes() {
    for (point, winner) in [
        ("reservation-durable", false),
        ("intent-durable", false),
        ("stage-first-file", false),
        ("stage-synced", false),
        ("participants-prepared", false),
        ("prepared-catalog-written", false),
        ("prepared-catalog-durable", false),
        ("before-coordinator-decision", false),
        ("during-decision-append", false),
        ("after-decision-append", true),
        ("coordinator-durable", true),
        ("staged-heap-committed", true),
        ("promotion-partial", true),
        ("promotion-complete", true),
        ("before-nbsc-publication", true),
        ("during-nbsc-publication", true),
        ("nbsc-state-durable", true),
        ("before-memory-publish", true),
        ("after-memory-publish", true),
        ("before-api-return", true),
        ("rollback-participants-durable", false),
        ("rollback-cleanup", false),
    ] {
        let root = root(point);
        seed(&root, true).close().unwrap();
        spawn(&root, point);
        outcome(&root, winner);
        if !winner {
            let mut db = Database::open_catalog(root.join("catalog")).unwrap();
            let mut txn = db.begin_transaction().unwrap();
            assert_eq!(
                db.create_heap_table_in(&mut txn, spec("projects")).unwrap(),
                TableId(4)
            );
            db.commit_transaction(&mut txn).unwrap();
            assert_eq!(
                db.bindings.resolve_single(TableId(4)).unwrap(),
                StorageId(4)
            );
            drop(txn);
            db.close().unwrap();
        }
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn initialized_journal_and_winner_corruption_fail_closed() {
    for case in [
        "missing-journal",
        "corrupt-journal",
        "missing-heap",
        "corrupt-owner",
        "missing-prepared",
        "bad-digest",
        "final-collision",
    ] {
        let root = root(case);
        seed(&root, true).close().unwrap();
        spawn(&root, "coordinator-durable");
        let marker = crate::schema_catalog_file::marker(&root.join("catalog"))
            .unwrap()
            .unwrap();
        let journal = crate::schema_mutation_journal::SchemaMutationJournal::open(
            &root.join("catalog"),
            marker.incarnation,
        )
        .unwrap()
        .unwrap();
        let r = journal.reservations.values().next().unwrap();
        let stage = crate::schema_catalog_file::resolve(
            &root.join("catalog"),
            &crate::schema_mutation_journal::stage_locator(
                &root.join("catalog"),
                marker.incarnation,
                r.transaction,
                r.storage,
            )
            .unwrap(),
        );
        let prepared = crate::schema_catalog_file::resolve(
            &root.join("catalog"),
            &crate::schema_mutation_journal::prepared_locator(
                &root.join("catalog"),
                marker.incarnation,
                r.transaction,
            )
            .unwrap(),
        );
        match case {
            "missing-journal" => std::fs::remove_file(root.join("catalog.mutations")).unwrap(),
            "corrupt-journal" => std::fs::write(root.join("catalog.mutations"), b"bad").unwrap(),
            "missing-heap" => std::fs::remove_file(&stage).unwrap(),
            "corrupt-owner" => {
                std::fs::write(crate::schema_catalog_file::suffix(&stage, ".owner"), b"bad")
                    .unwrap()
            }
            "missing-prepared" => std::fs::remove_file(&prepared).unwrap(),
            "bad-digest" => {
                let mut b = std::fs::read(&prepared).unwrap();
                b[20] ^= 1;
                std::fs::write(&prepared, b).unwrap();
            }
            _ => {
                let destination = crate::schema_catalog_file::resolve(
                    &root.join("catalog"),
                    &r.intent.as_ref().unwrap().fragment.storages[0].locator,
                );
                crate::schema_mutation::ensure_parent(&destination).unwrap();
                std::fs::copy(&stage, destination).unwrap();
            }
        }
        for _ in 0..3 {
            assert!(
                Database::open_catalog(root.join("catalog")).is_err(),
                "{case}"
            );
        }
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn journal_codec_roundtrip_truncation_duplicates_and_incarnation() {
    let root = root("codec");
    let mut db = seed(&root, true);
    let mut txn = db.begin_transaction().unwrap();
    db.create_heap_table_in(&mut txn, spec("projects")).unwrap();
    txn.rollback().unwrap();
    let journal = db.mutation_journal.as_ref().unwrap().borrow();
    let bytes = journal.encode().unwrap();
    let decoded = crate::schema_mutation_journal::SchemaMutationJournal::decode(&bytes).unwrap();
    assert_eq!(decoded.encode().unwrap(), bytes);
    for n in 0..bytes.len() {
        assert!(
            crate::schema_mutation_journal::SchemaMutationJournal::decode(&bytes[..n]).is_err()
        );
    }
    let mut duplicate = decoded.clone();
    let mut r = duplicate.reservations.values().next().unwrap().clone();
    r.transaction.0 += 1;
    duplicate.reservations.insert(r.transaction, r);
    assert!(duplicate.encode().is_err());
    assert!(
        crate::schema_mutation_journal::SchemaMutationJournal::open(&root.join("catalog"), [1; 16])
            .is_err()
    );
    drop(journal);
    drop(txn);
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn exact_expectation_accepts_added_table_after_runtime_commit() {
    let root = root("expectation");
    let mut db = seed(&root, true);
    let expected = Schema::new(db.schema().tables().to_vec()).unwrap();
    let mut txn = db.begin_transaction().unwrap();
    db.create_heap_table_in(&mut txn, spec("projects")).unwrap();
    db.commit_transaction(&mut txn).unwrap();
    drop(txn);
    db.close().unwrap();
    let db =
        Database::open_catalog_with_expectation(root.join("catalog"), Some(&expected)).unwrap();
    assert_eq!(db.schema().tables().len(), 3);
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn dropped_dirty_participant_blocks_schema_admission_without_reserving() {
    let root = root("dropped-dirty");
    let mut db = seed(&root, true);
    let mut dirty = db.begin_transaction().unwrap();
    db.execute_in(&mut dirty, "UPDATE users SET id = 4")
        .unwrap();
    drop(dirty);
    let mut txn = db.begin_transaction().unwrap();
    assert!(db.create_heap_table_in(&mut txn, spec("projects")).is_err());
    assert_eq!(db.next_table_id(), Some(TableId(3)));
    txn.rollback().unwrap();
    drop(txn);
    drop(db);
    let db = Database::open_catalog(root.join("catalog")).unwrap();
    assert!(db.schema().table("projects").is_none());
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn failed_staging_requires_rollback_and_never_decides_commit() {
    let root = root("stage-failure");
    let mut db = seed(&root, true);
    let mut txn = db.begin_transaction().unwrap();
    let snapshot = crate::schema_catalog_file::load(&root.join("catalog")).unwrap();
    let stage = crate::schema_catalog_file::resolve(
        &root.join("catalog"),
        &crate::schema_mutation_journal::stage_locator(
            &root.join("catalog"),
            snapshot.incarnation,
            txn.id(),
            StorageId(3),
        )
        .unwrap(),
    );
    crate::schema_mutation::ensure_parent(&stage).unwrap();
    std::fs::write(
        crate::schema_catalog_file::suffix(&stage, ".owner"),
        b"injected create-new collision",
    )
    .unwrap();
    assert!(db.create_heap_table_in(&mut txn, spec("projects")).is_err());
    assert_eq!(txn.state(), TransactionState::RollbackRequired);
    assert!(db.commit_transaction(&mut txn).is_err());
    assert_eq!(
        db.coordinator
            .as_ref()
            .unwrap()
            .borrow()
            .decisions()
            .count(),
        0
    );
    txn.rollback().unwrap();
    assert_eq!(db.next_table_id(), Some(TableId(4)));
    assert_eq!(db.next_storage_id(), Some(StorageId(4)));
    drop(txn);
    db.close().unwrap();
    Database::open_catalog(root.join("catalog"))
        .unwrap()
        .close()
        .unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn existing_lsm_and_range_participants_commit_with_new_heap() {
    for range in [false, true] {
        let root = root(if range { "range-create" } else { "lsm-create" });
        let mut db = if range {
            Database::create_catalog_with_placements(
                root.join("catalog"),
                vec![crate::TablePlacementSpec::range_partitioned(
                    old_table(1, "users"),
                    ColumnId(1),
                    vec![
                        crate::RangePartitionSpec::new(
                            netbadb_types::PartitionId(9),
                            root.join("lower"),
                            None,
                            Some(ScalarValue::Int64(0)),
                        ),
                        crate::RangePartitionSpec::new(
                            netbadb_types::PartitionId(10),
                            root.join("upper"),
                            Some(ScalarValue::Int64(0)),
                            None,
                        ),
                    ],
                )],
                crate::PartitionCatalogConfig::new(
                    root.join("partitions"),
                    root.join("coordinator"),
                ),
            )
            .unwrap()
        } else {
            Database::create_catalog(
                root.join("catalog"),
                vec![TableStorageCreateSpec::lsm(
                    root.join("lsm"),
                    old_table(1, "users"),
                    ColumnId(1),
                )],
                Some(DatabaseCoordinatorConfig::new(root.join("coordinator"))),
            )
            .unwrap()
        };
        let next_partition = db.next_partition_id();
        let next_storage = db.next_storage_id().unwrap();
        let mut txn = db.begin_transaction().unwrap();
        db.execute_in(&mut txn, "INSERT INTO users (id) VALUES (1)")
            .unwrap();
        let id = db.create_heap_table_in(&mut txn, spec("projects")).unwrap();
        insert(&mut db, &mut txn);
        db.commit_transaction(&mut txn).unwrap();
        drop(txn);
        db.close().unwrap();
        for _ in 0..3 {
            let mut db = Database::open_catalog(root.join("catalog")).unwrap();
            assert_eq!(db.schema_generation(), SchemaGeneration(2));
            assert_eq!(db.next_partition_id(), next_partition);
            assert_eq!(db.bindings.resolve_single(id).unwrap(), next_storage);
            assert_eq!(
                db.query("SELECT id FROM users").unwrap().rows,
                vec![vec![ScalarValue::Int64(1)]]
            );
            assert_eq!(db.query("SELECT * FROM projects").unwrap().rows, expected());
            db.close().unwrap();
        }
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn generation_and_identity_exhaustion_are_checked_before_staging() {
    for field in ["generation", "epoch", "revision", "table", "storage"] {
        let root = root(field);
        let mut db = seed(&root, true);
        let mut snapshot = crate::schema_catalog_file::load(&root.join("catalog")).unwrap();
        match field {
            "generation" => snapshot.committed.generation.0 = u64::MAX,
            "epoch" => snapshot.epoch = u64::MAX,
            "revision" => db.catalog_generation = u64::MAX,
            "table" => snapshot.committed.next_table_id = None,
            _ => snapshot.committed.next_storage_id = None,
        }
        // Test-only fixture replacement keeps the v1 marker CRC valid.
        let bytes = snapshot.encode().unwrap();
        std::fs::write(root.join("catalog"), &bytes).unwrap();
        let mut marker = std::fs::read(root.join("catalog.state")).unwrap();
        marker[36..44].copy_from_slice(&snapshot.epoch.to_le_bytes());
        marker[44..48].copy_from_slice(&crc32c::crc32c(&bytes).to_le_bytes());
        marker[12..16].fill(0);
        let crc = crc32c::crc32c_append(crc32c::crc32c(&marker[..12]), &marker[16..]);
        marker[12..16].copy_from_slice(&crc.to_le_bytes());
        std::fs::write(root.join("catalog.state"), marker).unwrap();
        db.committed = snapshot.committed;
        let mut txn = db.begin_transaction().unwrap();
        assert!(matches!(
            db.create_heap_table_in(&mut txn, spec("projects")),
            Err(crate::DatabaseError::SchemaMutation(
                SchemaMutationError::IdentityExhausted(_)
            ))
        ));
        assert!(!root.join("catalog.mutations").exists());
        txn.rollback().unwrap();
        drop(txn);
        db.close().unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
#[ignore = "explicit deterministic fuzz corpus generation"]
fn write_schema_mutation_fuzz_corpus() {
    let output = PathBuf::from(
        std::env::var("NETBADB_ROUND18_CORPUS").expect("explicit corpus output directory"),
    );
    std::fs::create_dir_all(output.join("schema_mutation_decode")).unwrap();
    std::fs::create_dir_all(output.join("coordinator_log_decode")).unwrap();
    let root = root("fuzz-corpus");
    let mut db = seed(&root, true);
    let mut txn = db.begin_transaction().unwrap();
    db.create_heap_table_in(&mut txn, spec("projects")).unwrap();
    let mut journal = db.mutation_journal.as_ref().unwrap().borrow().clone();
    journal.incarnation = [7; 16];
    journal.coordinator = "coordinator".into();
    for reservation in journal.reservations.values_mut() {
        let intent = reservation.intent.as_mut().unwrap();
        intent.fragment.incarnation = [7; 16];
        intent.fragment.coordinator = Some("coordinator".into());
        intent.fragment.storages[0].locator = "resources/storage/3.heap".into();
        intent.snapshot_digest = [9; 32];
    }
    for (name, intent, outcome) in [
        ("reservation-v1", false, None),
        ("intent-v1", true, None),
        ("abort-v1", true, Some(false)),
        ("commit-v1", true, Some(true)),
    ] {
        let mut sample = journal.clone();
        for r in sample.reservations.values_mut() {
            if !intent {
                r.intent = None;
            }
            r.resolved = outcome;
        }
        let bytes = sample.encode().unwrap();
        std::fs::write(output.join("schema_mutation_decode").join(name), bytes).unwrap();
    }
    let reference = crate::coordinator_log::SchemaParticipantReference {
        incarnation: [7; 16],
        target_epoch: 2,
        digest: [9; 32],
    };
    let log_path = root.join("seed-coordinator");
    let mut log = crate::CoordinatorLog::create(&log_path).unwrap();
    log.commit_schema_decision(
        netbadb_types::DatabaseTxnId(3),
        &[
            crate::coordinator_log::CoordinatorParticipant {
                storage_id: StorageId(1),
                physical_txn_id: netbadb_types::TxnId(7),
            },
            crate::coordinator_log::CoordinatorParticipant {
                storage_id: StorageId(3),
                physical_txn_id: netbadb_types::TxnId(1),
            },
        ],
        Some(&reference),
    )
    .unwrap();
    let bytes = std::fs::read(&log_path).unwrap();
    std::fs::write(
        output.join("coordinator_log_decode/schema-decision-v2"),
        &bytes,
    )
    .unwrap();
    std::fs::write(
        output.join("coordinator_log_decode/schema-decision-v2-truncated"),
        &bytes[..bytes.len() - 5],
    )
    .unwrap();
    log.complete(netbadb_types::DatabaseTxnId(3)).unwrap();
    std::fs::write(
        output.join("coordinator_log_decode/schema-complete-v2"),
        std::fs::read(&log_path).unwrap(),
    )
    .unwrap();
    drop(log);
    txn.rollback().unwrap();
    drop(txn);
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn subsequent_winner_recovers_past_prior_completed_journal_history() {
    let root = root("second-winner");
    let mut db = seed(&root, true);
    let mut txn = db.begin_transaction().unwrap();
    db.create_heap_table_in(&mut txn, CreateTableSpec::new("first", vec![]))
        .unwrap();
    db.commit_transaction(&mut txn).unwrap();
    drop(txn);
    db.close().unwrap();
    spawn(&root, "during-nbsc-publication");
    for _ in 0..3 {
        let mut db = Database::open_catalog(root.join("catalog")).unwrap();
        assert_eq!(db.schema_generation(), SchemaGeneration(3));
        assert_eq!(db.schema().table("projects").unwrap().id, TableId(4));
        assert_eq!(db.query("SELECT * FROM projects").unwrap().rows, expected());
        db.close().unwrap();
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn dropped_schema_handle_requires_recovery_and_consumes_ids() {
    let root = root("dropped-schema");
    let mut db = seed(&root, true);
    let mut txn = db.begin_transaction().unwrap();
    db.create_heap_table_in(&mut txn, spec("projects")).unwrap();
    insert(&mut db, &mut txn);
    drop(txn);
    assert!(matches!(
        db.begin_transaction(),
        Err(crate::DatabaseError::SchemaMutation(
            SchemaMutationError::RecoveryRequired
        ))
    ));
    drop(db);
    outcome(&root, false);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn automatic_coordinator_recovers_preexisting_writes_on_both_outcomes() {
    for winner in [false, true] {
        let root = root(if winner {
            "automatic-winner"
        } else {
            "automatic-loser"
        });
        seed(&root, false).close().unwrap();
        let status = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "schema_mutation_tests::create_crash_child"])
            .env("NETBADB_CREATE_CHILD_ROOT", &root)
            .env("NETBADB_CREATE_SKIP_SECOND_WRITE", "1")
            .env(
                "NETBADB_CREATE_CRASH_POINT",
                if winner {
                    "coordinator-durable"
                } else {
                    "participants-prepared"
                },
            )
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(90));
        for _ in 0..3 {
            let mut db = Database::open_catalog(root.join("catalog")).unwrap();
            assert_eq!(
                db.schema_generation(),
                SchemaGeneration(if winner { 2 } else { 1 })
            );
            assert_eq!(
                db.query("SELECT id FROM users").unwrap().rows,
                vec![vec![ScalarValue::Int64(if winner { 7 } else { 1 })]]
            );
            assert_eq!(db.next_table_id(), Some(TableId(4)));
            assert_eq!(db.schema().table("projects").is_some(), winner);
            if winner {
                assert_eq!(db.query("SELECT * FROM projects").unwrap().rows, expected());
            }
            db.close().unwrap();
        }
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn uncertain_rollback_journal_sync_requires_recovery_without_publishing() {
    let root = root("rollback-sync");
    let mut db = seed(&root, true);
    let mut txn = db.begin_transaction().unwrap();
    db.create_heap_table_in(&mut txn, spec("projects")).unwrap();
    insert(&mut db, &mut txn);
    db.mutation_journal
        .as_ref()
        .unwrap()
        .borrow_mut()
        .inject_sync_failure();
    assert!(txn.rollback().is_err());
    assert_eq!(txn.state(), TransactionState::RollbackPending);
    assert!(db.schema().table("projects").is_none());
    assert_eq!(db.schema_generation(), SchemaGeneration(1));
    assert!(txn.rollback().is_err());
    drop(txn);
    drop(db);
    outcome(&root, false);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn empty_journal_reopen_completes_activation_before_reservation() {
    let root = root("activation-reopen");
    let db = seed(&root, true);
    let catalog = root.join("catalog");
    let snapshot = crate::schema_catalog_file::load(&catalog).unwrap();
    crate::schema_mutation_journal::SchemaMutationJournal::initialize(
        &catalog,
        snapshot.incarnation,
        "coordinator".into(),
    )
    .unwrap();
    // Exact durable state of a crash between empty-journal and witness writes.
    std::fs::remove_file(root.join("catalog.mutations.state")).unwrap();
    db.close().unwrap();
    for _ in 0..3 {
        let db = Database::open_catalog(&catalog).unwrap();
        assert_eq!(db.next_table_id(), Some(TableId(3)));
        assert!(!root.join("catalog.mutations.state").exists());
        db.close().unwrap();
    }
    let mut db = Database::open_catalog(&catalog).unwrap();
    let mut txn = db.begin_transaction().unwrap();
    assert_eq!(
        db.create_heap_table_in(&mut txn, spec("projects")).unwrap(),
        TableId(3)
    );
    assert!(root.join("catalog.mutations.state").is_file());
    insert(&mut db, &mut txn);
    db.commit_transaction(&mut txn).unwrap();
    drop(txn);
    db.close().unwrap();
    for _ in 0..3 {
        let mut db = Database::open_catalog(&catalog).unwrap();
        assert_eq!(db.query("SELECT * FROM projects").unwrap().rows, expected());
        db.close().unwrap();
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn journal_capacity_rejects_before_reservation_and_leaves_resolution_room() {
    let root = root("journal-capacity");
    let db = seed(&root, true);
    let catalog = root.join("catalog");
    let snapshot = crate::schema_catalog_file::load(&catalog).unwrap();
    let mut journal = crate::schema_mutation_journal::SchemaMutationJournal::initialize(
        &catalog,
        snapshot.incarnation,
        "coordinator".into(),
    )
    .unwrap();
    // 65,534 valid records leave room for reserve + intent but not resolution.
    // These are durable aborts before intent, so no physical artifacts exist.
    for id in 1..=32767 {
        let transaction = netbadb_types::DatabaseTxnId(id);
        journal.reservations.insert(
            transaction,
            crate::schema_mutation_journal::Reservation {
                transaction,
                table: TableId(id + 2),
                storage: StorageId(id + 2),
                base_generation: SchemaGeneration(1),
                base_epoch: 1,
                intent: None,
                resolved: Some(false),
            },
        );
    }
    let bytes = journal.encode().unwrap();
    std::fs::write(root.join("catalog.mutations"), &bytes).unwrap();
    db.close().unwrap();
    let mut db = Database::open_catalog(&catalog).unwrap();
    let mut txn = db.begin_transaction().unwrap();
    assert!(db.create_heap_table_in(&mut txn, spec("projects")).is_err());
    assert_eq!(txn.state(), TransactionState::Active);
    assert_eq!(db.next_table_id(), Some(TableId(32770)));
    assert_eq!(db.next_storage_id(), Some(StorageId(32770)));
    assert_eq!(
        std::fs::read(root.join("catalog.mutations")).unwrap(),
        bytes
    );
    assert!(db.schema_writer.get().is_none());
    txn.rollback().unwrap();
    drop(txn);
    db.close().unwrap();
    let db = Database::open_catalog(&catalog).unwrap();
    assert_eq!(db.next_table_id(), Some(TableId(32770)));
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}
