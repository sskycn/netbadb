use netbadb_core::{
    Database, DatabaseCoordinatorConfig, ExecutionResult, TableStorageCreateSpec,
    TableStorageOpenSpec,
};
use netbadb_inspect::{PlanNodeInspection, StatementPlanInspection};
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_types::{ColumnId, PhysicalType, ScalarValue, TableId};

fn table(id: u64, name: &str) -> TableDef {
    TableDef::new(
        TableId(id),
        name,
        vec![
            ColumnDef::new(ColumnId(1), "id", TypeSpec::Physical(PhysicalType::Int64)),
            ColumnDef::new(ColumnId(2), "value", TypeSpec::Physical(PhysicalType::Text))
                .nullable(true),
        ],
    )
}

fn paths(case: &str) -> (std::path::PathBuf, std::path::PathBuf, std::path::PathBuf) {
    let base = std::env::temp_dir().join(format!(
        "netbadb-core-lsm-{case}-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    (
        base.with_extension("heap"),
        base.with_extension("lsm"),
        base.with_extension("coordinator"),
    )
}

fn cleanup(heap: &std::path::Path, lsm: &std::path::Path, coordinator: &std::path::Path) {
    let wal = netbadb_storage::wal_path(heap);
    let _ = std::fs::remove_file(netbadb_storage::wal_alternate_path(&wal));
    let _ = std::fs::remove_file(wal);
    let _ = std::fs::remove_file(netbadb_storage::txn_status_path(heap));
    let _ = std::fs::remove_file(heap);
    let _ = std::fs::remove_dir_all(lsm);
    let _ = std::fs::remove_file(coordinator);
}

#[cfg(unix)]
#[test]
fn completed_mixed_create_journal_reopens_with_lsm_participant() {
    use std::os::unix::fs::MetadataExt;
    let root = std::env::temp_dir().join(format!(
        "netbadb-mixed-create-reopen-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let mut database = Database::create_catalog(
        root.join("catalog"),
        vec![TableStorageCreateSpec::lsm(
            root.join("items.lsm"),
            table(1, "items"),
            ColumnId(1),
        )],
        Some(DatabaseCoordinatorConfig::new(root.join("coordinator"))),
    )
    .unwrap();
    database
        .execute("CREATE TABLE projects (id BIGINT NOT NULL)")
        .unwrap();
    database.close().unwrap();
    let carriers = ["catalog.core-owner", "coordinator.core-owner"].map(|name| {
        let metadata = std::fs::metadata(root.join(name)).unwrap();
        (metadata.dev(), metadata.ino())
    });
    let before = ["catalog", "catalog.mutations", "coordinator"]
        .map(|name| std::fs::read(root.join(name)).unwrap());
    let holder =
        netbadb_storage::TableStorage::open_lsm(root.join("items.lsm"), table(1, "items")).unwrap();
    assert!(Database::open_catalog(root.join("catalog")).is_err());
    for (name, expected) in ["catalog", "catalog.mutations", "coordinator"]
        .into_iter()
        .zip(before)
    {
        assert_eq!(std::fs::read(root.join(name)).unwrap(), expected);
    }
    holder.close().unwrap();
    let mut reopened = Database::open_catalog(root.join("catalog")).unwrap();
    reopened
        .execute("INSERT INTO projects (id) VALUES (7)")
        .unwrap();
    assert_eq!(
        reopened
            .query("SELECT id FROM projects")
            .unwrap()
            .rows
            .len(),
        1
    );
    reopened.close().unwrap();
    for (name, expected) in ["catalog.core-owner", "coordinator.core-owner"]
        .into_iter()
        .zip(carriers)
    {
        let metadata = std::fs::metadata(root.join(name)).unwrap();
        assert_eq!((metadata.dev(), metadata.ino()), expected);
    }
    std::fs::remove_dir_all(root).unwrap();
}

fn root(plan: &StatementPlanInspection) -> &PlanNodeInspection {
    match plan {
        StatementPlanInspection::Query { root } => root,
        _ => panic!("query plan expected"),
    }
}

fn contains_index(plan: &PlanNodeInspection) -> bool {
    match plan {
        PlanNodeInspection::IndexScan { .. }
        | PlanNodeInspection::RangeIndexScan { .. }
        | PlanNodeInspection::IndexNestedLoopJoin { .. } => true,
        PlanNodeInspection::Filter { input, .. }
        | PlanNodeInspection::Sort { input, .. }
        | PlanNodeInspection::Project { input, .. }
        | PlanNodeInspection::ScalarProject { input, .. }
        | PlanNodeInspection::Aggregate { input, .. }
        | PlanNodeInspection::Limit { input, .. } => contains_index(input),
        PlanNodeInspection::NestedLoopJoin { left, right, .. }
        | PlanNodeInspection::HashJoin { left, right, .. } => {
            contains_index(left) || contains_index(right)
        }
        PlanNodeInspection::SeqScan { .. }
        | PlanNodeInspection::ColumnarScan { .. }
        | PlanNodeInspection::PartitionedScan { .. }
        | PlanNodeInspection::OneRow => false,
    }
}

#[cfg(unix)]
#[test]
fn mixed_admission_holder_child() {
    use std::io::Read;
    let Ok(root) = std::env::var("NETBADB_MIXED_HOLDER_ROOT") else {
        return;
    };
    let root = std::path::PathBuf::from(root);
    let storage = match std::env::var("NETBADB_MIXED_HOLDER_KIND").unwrap().as_str() {
        "heap" => {
            netbadb_storage::TableStorage::open_heap(root.join("a.heap"), table(1, "heap_items"))
                .unwrap()
        }
        "lsm" => netbadb_storage::TableStorage::open_lsm(root.join("z.lsm"), table(2, "lsm_items"))
            .unwrap(),
        other => panic!("unknown holder kind {other}"),
    };
    std::fs::write(root.join("ready"), b"ready").unwrap();
    let mut release = [0_u8; 1];
    std::io::stdin().read_exact(&mut release).unwrap();
    storage.close().unwrap();
}

#[cfg(unix)]
#[test]
fn mixed_admission_rejects_busy_participant_before_recovery() {
    use std::io::Write;
    use std::process::{Command, Stdio};
    use std::time::{Duration, Instant};

    for held in ["lsm", "heap"] {
        let root = std::env::temp_dir().join(format!(
            "netbadb-mixed-admission-{held}-{}-{:?}",
            std::process::id(),
            std::thread::current().id(),
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let heap = root.join("a.heap");
        let lsm = root.join("z.lsm");
        let coordinator = root.join("coordinator");
        let specs = vec![
            TableStorageCreateSpec::heap(&heap, table(1, "heap_items")),
            TableStorageCreateSpec::lsm(&lsm, table(2, "lsm_items"), ColumnId(1)),
        ];
        Database::create_storages_with_coordinator(
            specs,
            DatabaseCoordinatorConfig::new(&coordinator),
        )
        .unwrap()
        .close()
        .unwrap();
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "mixed_admission_holder_child", "--nocapture"])
            .env("NETBADB_MIXED_HOLDER_ROOT", &root)
            .env("NETBADB_MIXED_HOLDER_KIND", held)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while !root.join("ready").exists() {
            assert!(Instant::now() < deadline, "holder did not become ready");
            assert!(
                child.try_wait().unwrap().is_none(),
                "holder exited before readiness"
            );
            std::thread::yield_now();
        }
        let heap_wal = netbadb_storage::wal_path(&heap);
        let lsm_wal = lsm.join("wal-00000000000000000001.nblw");
        let orphan = lsm.join("wal-00000000000000000002.nblw");
        let orphan_sst = lsm.join("sst").join("sst-00000000000000000999-l0.nbls");
        if held == "heap" {
            std::fs::write(&orphan, b"unpublished orphan").unwrap();
            std::fs::write(&orphan_sst, b"unpublished sstable").unwrap();
        }
        let manifest_before = std::fs::read(lsm.join("MANIFEST")).unwrap();
        let entries_before = std::fs::read_dir(&lsm)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<std::collections::BTreeSet<_>>();
        let sst_entries_before = std::fs::read_dir(lsm.join("sst"))
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<std::collections::BTreeSet<_>>();
        let pending = if held == "lsm" { &heap_wal } else { &lsm_wal };
        std::fs::OpenOptions::new()
            .append(true)
            .open(pending)
            .unwrap()
            .write_all(b"W")
            .unwrap();
        std::fs::OpenOptions::new()
            .append(true)
            .open(&coordinator)
            .unwrap()
            .write_all(b"C")
            .unwrap();
        let pending_before = std::fs::read(pending).unwrap();
        let coordinator_before = std::fs::read(&coordinator).unwrap();
        let mut specs = vec![
            TableStorageOpenSpec::heap(&heap, table(1, "heap_items")),
            TableStorageOpenSpec::lsm(&lsm, table(2, "lsm_items")),
        ];
        if held == "heap" {
            specs.reverse();
        }
        assert!(
            Database::open_storages_with_coordinator(
                specs.clone(),
                DatabaseCoordinatorConfig::new(&coordinator)
            )
            .is_err()
        );
        assert_eq!(std::fs::read(pending).unwrap(), pending_before);
        assert_eq!(std::fs::read(&coordinator).unwrap(), coordinator_before);
        assert_eq!(
            std::fs::read(lsm.join("MANIFEST")).unwrap(),
            manifest_before
        );
        assert_eq!(
            std::fs::read_dir(&lsm)
                .unwrap()
                .map(|entry| entry.unwrap().file_name())
                .collect::<std::collections::BTreeSet<_>>(),
            entries_before,
        );
        assert_eq!(
            std::fs::read_dir(lsm.join("sst"))
                .unwrap()
                .map(|entry| entry.unwrap().file_name())
                .collect::<std::collections::BTreeSet<_>>(),
            sst_entries_before,
        );
        if held == "heap" {
            assert_eq!(std::fs::read(&orphan).unwrap(), b"unpublished orphan");
            assert_eq!(std::fs::read(&orphan_sst).unwrap(), b"unpublished sstable");
        }
        child.stdin.take().unwrap().write_all(b"x").unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while child.try_wait().unwrap().is_none() {
            assert!(Instant::now() < deadline, "holder did not exit");
            std::thread::yield_now();
        }
        assert!(child.wait().unwrap().success());
        Database::open_storages_with_coordinator(
            specs,
            DatabaseCoordinatorConfig::new(&coordinator),
        )
        .unwrap()
        .close()
        .unwrap();
        assert_eq!(
            std::fs::metadata(pending).unwrap().len(),
            pending_before.len() as u64 - 1
        );
        assert_eq!(
            std::fs::metadata(&coordinator).unwrap().len(),
            coordinator_before.len() as u64 - 1
        );
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[cfg(unix)]
#[test]
fn mixed_invalid_participant_rejects_before_coordinator_tail_repair() {
    use std::io::{Seek, Write};

    let root = std::env::temp_dir().join(format!(
        "netbadb-mixed-invalid-recovery-{}-{:?}",
        std::process::id(),
        std::thread::current().id(),
    ));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let heap = root.join("a.heap");
    let lsm = root.join("z.lsm");
    let coordinator = root.join("coordinator");
    Database::create_storages_with_coordinator(
        vec![
            TableStorageCreateSpec::heap(&heap, table(1, "heap_items")),
            TableStorageCreateSpec::lsm(&lsm, table(2, "lsm_items"), ColumnId(1)),
        ],
        DatabaseCoordinatorConfig::new(&coordinator),
    )
    .unwrap()
    .close()
    .unwrap();
    let lsm_wal = lsm.join("wal-00000000000000000001.nblw");
    let mut wal = std::fs::OpenOptions::new()
        .write(true)
        .open(&lsm_wal)
        .unwrap();
    wal.rewind().unwrap();
    wal.write_all(b"X").unwrap();
    drop(wal);
    std::fs::OpenOptions::new()
        .append(true)
        .open(&coordinator)
        .unwrap()
        .write_all(b"C")
        .unwrap();
    let coordinator_before = std::fs::read(&coordinator).unwrap();
    let heap_wal = netbadb_storage::wal_path(&heap);
    let heap_before = std::fs::read(&heap_wal).unwrap();
    assert!(
        Database::open_storages_with_coordinator(
            vec![
                TableStorageOpenSpec::heap(&heap, table(1, "heap_items")),
                TableStorageOpenSpec::lsm(&lsm, table(2, "lsm_items")),
            ],
            DatabaseCoordinatorConfig::new(&coordinator),
        )
        .is_err()
    );
    assert_eq!(std::fs::read(&coordinator).unwrap(), coordinator_before);
    assert_eq!(std::fs::read(&heap_wal).unwrap(), heap_before);
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(unix)]
#[test]
fn mixed_without_coordinator_checks_all_recovery_inputs_before_open() {
    use std::io::{Seek, Write};

    let root = std::env::temp_dir().join(format!(
        "netbadb-mixed-no-coordinator-{}-{:?}",
        std::process::id(),
        std::thread::current().id(),
    ));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let heap = root.join("a.heap");
    let lsm = root.join("z.lsm");
    Database::create_storages(vec![
        TableStorageCreateSpec::heap(&heap, table(1, "heap_items")),
        TableStorageCreateSpec::lsm(&lsm, table(2, "lsm_items"), ColumnId(1)),
    ])
    .unwrap()
    .close()
    .unwrap();
    let heap_wal = netbadb_storage::wal_path(&heap);
    std::fs::OpenOptions::new()
        .append(true)
        .open(&heap_wal)
        .unwrap()
        .write_all(b"W")
        .unwrap();
    let heap_before = std::fs::read(&heap_wal).unwrap();
    let lsm_wal = lsm.join("wal-00000000000000000001.nblw");
    let mut wal = std::fs::OpenOptions::new()
        .write(true)
        .open(&lsm_wal)
        .unwrap();
    wal.rewind().unwrap();
    wal.write_all(b"X").unwrap();
    drop(wal);
    let specs = vec![
        TableStorageOpenSpec::heap(&heap, table(1, "heap_items")),
        TableStorageOpenSpec::lsm(&lsm, table(2, "lsm_items")),
    ];
    assert!(Database::open_storages(specs).is_err());
    assert_eq!(std::fs::read(&heap_wal).unwrap(), heap_before);
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(unix)]
#[test]
fn shared_catalog_rejects_second_core_with_subset_expectation() {
    let root = std::env::temp_dir().join(format!(
        "netbadb-mixed-metadata-{}-{:?}",
        std::process::id(),
        std::thread::current().id(),
    ));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let catalog = root.join("catalog");
    let coordinator = root.join("coordinator");
    let heap = root.join("a.heap");
    let lsm = root.join("z.lsm");
    let database = Database::create_catalog(
        &catalog,
        vec![
            TableStorageCreateSpec::heap(&heap, table(1, "heap_items")),
            TableStorageCreateSpec::lsm(&lsm, table(2, "lsm_items"), ColumnId(1)),
        ],
        Some(DatabaseCoordinatorConfig::new(&coordinator)),
    )
    .unwrap();
    let catalog_before = std::fs::read(&catalog).unwrap();
    let coordinator_before = std::fs::read(&coordinator).unwrap();
    let error = match Database::open_storages_with_coordinator(
        vec![TableStorageOpenSpec::lsm(&lsm, table(2, "lsm_items"))],
        DatabaseCoordinatorConfig::new(&coordinator),
    ) {
        Ok(_) => panic!("second Core instance acquired shared metadata"),
        Err(error) => error,
    };
    assert!(
        error.to_string().contains("claim catalog ownership"),
        "{error}"
    );
    assert_eq!(std::fs::read(&catalog).unwrap(), catalog_before);
    assert_eq!(std::fs::read(&coordinator).unwrap(), coordinator_before);
    database.close().unwrap();
    Database::open_catalog(&catalog).unwrap().close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(unix)]
#[test]
fn metadata_ownership_survives_database_drop_with_live_transaction() {
    let root = std::env::temp_dir().join(format!(
        "netbadb-mixed-metadata-lifetime-{}-{:?}",
        std::process::id(),
        std::thread::current().id(),
    ));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let catalog = root.join("catalog");
    let mut database = Database::create_catalog(
        &catalog,
        vec![
            TableStorageCreateSpec::heap(root.join("a.heap"), table(1, "heap_items")),
            TableStorageCreateSpec::lsm(root.join("z.lsm"), table(2, "lsm_items"), ColumnId(1)),
        ],
        Some(DatabaseCoordinatorConfig::new(root.join("coordinator"))),
    )
    .expect("S1 create database");
    let catalog_before = std::fs::read(&catalog).unwrap();
    let coordinator = root.join("coordinator");
    let coordinator_before = std::fs::read(&coordinator).unwrap();
    let transaction = database.begin_transaction().expect("S2 begin transaction");
    drop(database);
    let error = match Database::open_catalog(&catalog) {
        Ok(_) => panic!("S4 catalog owner was lost while transaction remained live"),
        Err(error) => error,
    };
    match error {
        netbadb_core::DatabaseError::SchemaCatalog(netbadb_core::SchemaCatalogError::Io {
            operation: "claim catalog ownership",
            path,
            source,
        }) => {
            assert_eq!(path, catalog.canonicalize().unwrap());
            assert_eq!(source.kind(), std::io::ErrorKind::WouldBlock);
            assert_eq!(source.raw_os_error(), Some(libc::EWOULDBLOCK));
        }
        other => panic!("S4 unexpected ownership error: {other:?}"),
    }
    assert_eq!(std::fs::read(&catalog).unwrap(), catalog_before);
    assert_eq!(std::fs::read(&coordinator).unwrap(), coordinator_before);
    drop(transaction);
    let reopened = Database::open_catalog(&catalog).expect("S6 reopen after final transaction");
    reopened.close().expect("S7 close reopened database");
    std::fs::remove_dir_all(root).expect("S8 remove test resources");
}

#[test]
fn lsm_sql_dml_access_paths_aggregates_and_reopen() {
    let (heap, lsm, coordinator) = paths("sql");
    cleanup(&heap, &lsm, &coordinator);
    let events = table(2, "events");
    let mut database = Database::create_storages(vec![TableStorageCreateSpec::lsm(
        &lsm,
        events.clone(),
        ColumnId(1),
    )])
    .expect("create LSM catalog");
    for sql in [
        "INSERT INTO events (id, value) VALUES (10, 'a')",
        "INSERT INTO events (id, value) VALUES (10, 'b')",
        "INSERT INTO events (id, value) VALUES (20, NULL)",
    ] {
        database.execute(sql).expect("insert");
    }
    database.analyze(TableId(2)).expect("analyze");
    assert!(contains_index(root(
        &database
            .inspect_statement("SELECT value FROM events WHERE id = 10")
            .expect("inspect point")
            .plan
    )));
    assert!(contains_index(root(
        &database
            .inspect_statement("SELECT value FROM events WHERE id >= 10 AND id < 11")
            .expect("inspect range")
            .plan
    )));
    assert_eq!(
        database
            .query("SELECT value FROM events WHERE id = 10 ORDER BY value")
            .expect("duplicates")
            .rows,
        vec![
            vec![ScalarValue::Text("a".into())],
            vec![ScalarValue::Text("b".into())]
        ]
    );
    assert_eq!(
        database
            .execute("UPDATE events SET id = 30 WHERE value = 'a'")
            .expect("key move"),
        ExecutionResult::AffectedRows(1)
    );
    assert_eq!(
        database
            .execute("DELETE FROM events WHERE value = 'b'")
            .expect("delete"),
        ExecutionResult::AffectedRows(1)
    );
    let aggregates = database
        .query("SELECT COUNT(*), COUNT(value), SUM(id), MIN(id), MAX(id) FROM events")
        .expect("aggregates");
    assert_eq!(
        aggregates.rows,
        vec![vec![
            ScalarValue::UInt64(2),
            ScalarValue::UInt64(1),
            ScalarValue::Int64(50),
            ScalarValue::Int64(20),
            ScalarValue::Int64(30)
        ]]
    );
    database.checkpoint().expect("flush checkpoint");
    database.compact(TableId(2)).expect("compact");
    database.close().expect("close");
    let mut reopened =
        Database::open_storages(vec![TableStorageOpenSpec::lsm(&lsm, events)]).expect("reopen");
    assert_eq!(
        reopened
            .query("SELECT id, value FROM events ORDER BY id")
            .expect("rows")
            .rows,
        vec![
            vec![ScalarValue::Int64(20), ScalarValue::Null],
            vec![ScalarValue::Int64(30), ScalarValue::Text("a".into())]
        ]
    );
    reopened.close().expect("close reopened");
    cleanup(&heap, &lsm, &coordinator);
}

#[test]
fn heap_and_lsm_share_atomic_coordinator_and_rollback_semantics() {
    let (heap, lsm, coordinator) = paths("mixed");
    let lsm_peer = lsm.with_extension("lsm-peer");
    cleanup(&heap, &lsm, &coordinator);
    let _ = std::fs::remove_dir_all(&lsm_peer);
    let heap_table = table(1, "heap_items");
    let lsm_table = table(2, "lsm_items");
    let lsm_peer_table = table(3, "lsm_peer_items");
    let specs = vec![
        TableStorageCreateSpec::heap(&heap, heap_table.clone()),
        TableStorageCreateSpec::lsm(&lsm, lsm_table.clone(), ColumnId(1)),
        TableStorageCreateSpec::lsm(&lsm_peer, lsm_peer_table.clone(), ColumnId(1)),
    ];
    let config = DatabaseCoordinatorConfig::new(&coordinator);
    let mut database =
        Database::create_storages_with_coordinator(specs, config.clone()).expect("create mixed");

    let mut rollback = database
        .begin_transaction_for(TableId(1))
        .expect("begin rollback");
    database
        .execute_in(
            &mut rollback,
            "INSERT INTO heap_items (id, value) VALUES (1, 'heap-rollback')",
        )
        .expect("heap write");
    database
        .execute_in(
            &mut rollback,
            "INSERT INTO lsm_items (id, value) VALUES (1, 'lsm-rollback')",
        )
        .expect("LSM write");
    rollback.rollback().expect("rollback");
    assert!(
        database
            .query("SELECT id FROM heap_items")
            .expect("heap empty")
            .rows
            .is_empty()
    );
    assert!(
        database
            .query("SELECT id FROM lsm_items")
            .expect("LSM empty")
            .rows
            .is_empty()
    );

    let mut commit = database
        .begin_transaction_for(TableId(1))
        .expect("begin commit");
    database
        .execute_in(
            &mut commit,
            "INSERT INTO heap_items (id, value) VALUES (2, 'heap')",
        )
        .expect("heap write");
    database
        .execute_in(
            &mut commit,
            "INSERT INTO lsm_items (id, value) VALUES (2, 'lsm')",
        )
        .expect("LSM write");
    database
        .execute_in(
            &mut commit,
            "INSERT INTO lsm_peer_items (id, value) VALUES (2, 'lsm-peer')",
        )
        .expect("second LSM write");
    commit.commit().expect("atomic commit");
    database.close().expect("close");

    let mut reopened = Database::open_storages_with_coordinator(
        vec![
            TableStorageOpenSpec::lsm(&lsm_peer, lsm_peer_table),
            TableStorageOpenSpec::lsm(&lsm, lsm_table),
            TableStorageOpenSpec::heap(&heap, heap_table),
        ],
        config,
    )
    .expect("reopen reversed");
    assert_eq!(
        reopened
            .query("SELECT value FROM heap_items")
            .expect("heap row")
            .rows,
        vec![vec![ScalarValue::Text("heap".into())]]
    );
    assert_eq!(
        reopened
            .query("SELECT value FROM lsm_items")
            .expect("LSM row")
            .rows,
        vec![vec![ScalarValue::Text("lsm".into())]]
    );
    assert_eq!(reopened.query("SELECT heap_items.id FROM heap_items JOIN lsm_items ON heap_items.id = lsm_items.id").expect("join").rows,
        vec![vec![ScalarValue::Int64(2)]]);
    assert_eq!(
        reopened
            .query(
                "SELECT lsm_items.id FROM lsm_items JOIN heap_items ON lsm_items.id = heap_items.id"
            )
            .expect("reverse join")
            .rows,
        vec![vec![ScalarValue::Int64(2)]]
    );
    assert_eq!(
        reopened
            .query("SELECT a.id FROM lsm_items a JOIN lsm_items b ON a.id = b.id")
            .expect("LSM self join")
            .rows,
        vec![vec![ScalarValue::Int64(2)]]
    );
    assert_eq!(
        reopened
            .query("SELECT lsm_items.id FROM lsm_items JOIN lsm_peer_items ON lsm_items.id = lsm_peer_items.id")
            .expect("LSM to LSM join")
            .rows,
        vec![vec![ScalarValue::Int64(2)]]
    );
    assert_eq!(
        reopened
            .query("SELECT value, COUNT(*), MIN(id), MAX(id) FROM lsm_items GROUP BY value")
            .expect("LSM grouped aggregates")
            .rows,
        vec![vec![
            ScalarValue::Text("lsm".into()),
            ScalarValue::UInt64(1),
            ScalarValue::Int64(2),
            ScalarValue::Int64(2),
        ]]
    );
    reopened.close().expect("close reopened");
    cleanup(&heap, &lsm, &coordinator);
    let _ = std::fs::remove_dir_all(&lsm_peer);
}
