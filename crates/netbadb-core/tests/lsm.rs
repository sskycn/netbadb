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

fn root(plan: &StatementPlanInspection) -> &PlanNodeInspection {
    match plan {
        StatementPlanInspection::Query { root } => root,
        _ => panic!("query plan expected"),
    }
}

fn contains_index(plan: &PlanNodeInspection) -> bool {
    match plan {
        PlanNodeInspection::IndexScan { .. } | PlanNodeInspection::RangeIndexScan { .. } => true,
        PlanNodeInspection::Filter { input, .. }
        | PlanNodeInspection::Sort { input, .. }
        | PlanNodeInspection::Project { input, .. }
        | PlanNodeInspection::Aggregate { input, .. }
        | PlanNodeInspection::Limit { input, .. } => contains_index(input),
        PlanNodeInspection::NestedLoopJoin { left, right, .. }
        | PlanNodeInspection::HashJoin { left, right, .. } => {
            contains_index(left) || contains_index(right)
        }
        PlanNodeInspection::SeqScan { .. } | PlanNodeInspection::PartitionedScan { .. } => false,
    }
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
