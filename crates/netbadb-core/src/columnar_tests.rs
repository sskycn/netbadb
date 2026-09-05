use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use netbadb_inspect::{PlanNodeInspection, StatementPlanInspection};
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_types::{ColumnId, PhysicalType, ScalarValue, TableId};

use crate::{
    ColumnarProjectionHealth, ColumnarProjectionSpec, Database, DatabaseError, ExecutionResult,
    TableStorageCreateSpec, cleanup_created_table_files,
};

static NEXT_PATH: AtomicU64 = AtomicU64::new(1);

fn path(name: &str) -> PathBuf {
    let suffix = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "netbadb-columnar-core-{name}-{}-{suffix}",
        std::process::id()
    ))
}

fn table() -> TableDef {
    TableDef::new(
        TableId(1),
        "events",
        vec![
            ColumnDef::new(ColumnId(1), "id", TypeSpec::Physical(PhysicalType::Int64)),
            ColumnDef::new(
                ColumnId(2),
                "amount",
                TypeSpec::Physical(PhysicalType::Int64),
            )
            .nullable(true),
            ColumnDef::new(
                ColumnId(3),
                "active",
                TypeSpec::Physical(PhysicalType::Bool),
            ),
            ColumnDef::new(ColumnId(4), "label", TypeSpec::Physical(PhysicalType::Text)),
        ],
    )
}

fn insert_rows(database: &mut Database, count: i64) {
    for id in 0..count {
        database
            .insert(&[
                ScalarValue::Int64(id),
                if id % 5 == 0 {
                    ScalarValue::Null
                } else {
                    ScalarValue::Int64(id * 2)
                },
                ScalarValue::Bool(id % 2 == 0),
                ScalarValue::Text(format!("event-{id}")),
            ])
            .expect("insert fixture row");
    }
}

fn plan_contains_columnar(plan: &PlanNodeInspection) -> bool {
    match plan {
        PlanNodeInspection::ColumnarScan { .. } => true,
        PlanNodeInspection::Filter { input, .. }
        | PlanNodeInspection::Sort { input, .. }
        | PlanNodeInspection::Project { input, .. }
        | PlanNodeInspection::ScalarProject { input, .. }
        | PlanNodeInspection::Aggregate { input, .. }
        | PlanNodeInspection::Limit { input, .. } => plan_contains_columnar(input),
        PlanNodeInspection::NestedLoopJoin { left, right, .. }
        | PlanNodeInspection::HashJoin { left, right, .. } => {
            plan_contains_columnar(left) || plan_contains_columnar(right)
        }
        PlanNodeInspection::IndexNestedLoopJoin { left, .. } => plan_contains_columnar(left),
        PlanNodeInspection::OneRow
        | PlanNodeInspection::SeqScan { .. }
        | PlanNodeInspection::IndexScan { .. }
        | PlanNodeInspection::RangeIndexScan { .. }
        | PlanNodeInspection::PartitionedScan { .. } => false,
    }
}

fn statement_uses_columnar(database: &Database, sql: &str) -> bool {
    let inspection = database.inspect_statement(sql).expect("inspect statement");
    match inspection.plan {
        StatementPlanInspection::Query { root } => plan_contains_columnar(&root),
        _ => false,
    }
}

fn plan_contains_index(plan: &PlanNodeInspection) -> bool {
    match plan {
        PlanNodeInspection::IndexScan { .. }
        | PlanNodeInspection::RangeIndexScan { .. }
        | PlanNodeInspection::IndexNestedLoopJoin { .. } => true,
        PlanNodeInspection::Filter { input, .. }
        | PlanNodeInspection::Sort { input, .. }
        | PlanNodeInspection::Project { input, .. }
        | PlanNodeInspection::ScalarProject { input, .. }
        | PlanNodeInspection::Aggregate { input, .. }
        | PlanNodeInspection::Limit { input, .. } => plan_contains_index(input),
        PlanNodeInspection::NestedLoopJoin { left, right, .. }
        | PlanNodeInspection::HashJoin { left, right, .. } => {
            plan_contains_index(left) || plan_contains_index(right)
        }
        PlanNodeInspection::OneRow
        | PlanNodeInspection::SeqScan { .. }
        | PlanNodeInspection::ColumnarScan { .. }
        | PlanNodeInspection::PartitionedScan { .. } => false,
    }
}

fn cleanup(heap: &PathBuf, projection: &PathBuf) {
    cleanup_created_table_files(std::slice::from_ref(heap));
    let _ = fs::remove_dir_all(projection);
}

#[test]
fn heap_columnar_build_executes_vector_filter_projection_and_aggregate() {
    let heap = path("heap");
    let projection = path("heap-projection");
    cleanup(&heap, &projection);
    let mut database = Database::create(&heap, table()).expect("create heap database");
    insert_rows(&mut database, 512);
    let id = database
        .build_columnar_projection(
            ColumnarProjectionSpec::new(
                TableId(1),
                &projection,
                vec![ColumnId(1), ColumnId(2), ColumnId(3), ColumnId(4)],
            )
            .with_row_group_rows(64),
        )
        .expect("build projection");
    assert_eq!(
        database.inspect_columnar_projections()[0].health,
        ColumnarProjectionHealth::Fresh
    );
    let sql = "SELECT active, COUNT(*), SUM(amount), MIN(id), MAX(id) FROM events WHERE id >= 256 GROUP BY active";
    assert!(statement_uses_columnar(&database, sql));
    let (columnar, scan) = database
        .query_with_columnar_statistics(sql)
        .expect("columnar query");
    assert_eq!(scan.projection_id, Some(id));
    assert!(scan.scan.row_groups_pruned >= 4);
    assert!(scan.scan.row_groups_read < scan.scan.row_groups_total);

    let null_sql = "SELECT COUNT(*), MIN(id), MAX(id) FROM events WHERE amount IS NULL";
    assert!(statement_uses_columnar(&database, null_sql));
    let columnar_nulls = database.query(null_sql).expect("columnar NULL predicate");
    let mut read_only = database
        .begin_transaction_with_isolation(crate::IsolationLevel::RepeatableRead)
        .expect("begin authoritative comparison transaction");
    let ExecutionResult::Query(authoritative_nulls) = database
        .execute_in(&mut read_only, null_sql)
        .expect("authoritative NULL predicate")
    else {
        panic!("query result expected");
    };
    read_only
        .rollback()
        .expect("rollback read-only transaction");
    assert_eq!(columnar_nulls, authoritative_nulls);

    database
        .execute("UPDATE events SET label = 'changed' WHERE id = 0")
        .expect("commit update");
    assert_eq!(
        database.inspect_columnar_projections()[0].health,
        ColumnarProjectionHealth::Stale
    );
    assert!(!statement_uses_columnar(&database, sql));
    let heap_result = database.query(sql).expect("heap fallback query");
    assert_eq!(columnar, heap_result);

    let generation = database
        .refresh_columnar_projection(id)
        .expect("refresh projection");
    assert_eq!(generation.0, 2);
    assert!(statement_uses_columnar(&database, sql));
    assert_eq!(database.query(sql).expect("refreshed query"), heap_result);

    database
        .execute("DELETE FROM events WHERE id = 510")
        .expect("commit delete");
    assert_eq!(
        database.inspect_columnar_projections()[0].health,
        ColumnarProjectionHealth::Stale
    );
    assert!(!statement_uses_columnar(&database, sql));
    assert_eq!(
        database
            .refresh_columnar_projection(id)
            .expect("refresh after delete")
            .0,
        3
    );

    let mut transaction = database.begin_transaction().expect("begin transaction");
    database
        .execute_in(&mut transaction, "DELETE FROM events WHERE id = 511")
        .expect("stage delete");
    transaction.rollback().expect("rollback delete");
    assert_eq!(
        database.inspect_columnar_projections()[0].health,
        ColumnarProjectionHealth::Fresh
    );
    database
        .drop_columnar_projection(id)
        .expect("drop projection");
    assert!(!projection.join("projection.nbcmanifest").exists());
    database.close().expect("close database");
    cleanup(&heap, &projection);
}

#[test]
fn point_lookup_keeps_btree_precedence_over_a_fresh_projection() {
    let heap = path("point");
    let projection = path("point-projection");
    cleanup(&heap, &projection);
    let mut database = Database::create(&heap, table()).expect("create heap database");
    insert_rows(&mut database, 512);
    database
        .create_index(TableId(1), ColumnId(1))
        .expect("create point index");
    database
        .build_columnar_projection(ColumnarProjectionSpec::new(
            TableId(1),
            &projection,
            vec![ColumnId(1), ColumnId(2), ColumnId(3), ColumnId(4)],
        ))
        .expect("build projection");
    let inspection = database
        .inspect_statement("SELECT label FROM events WHERE id = 400")
        .expect("inspect point lookup");
    let StatementPlanInspection::Query { root } = inspection.plan else {
        panic!("query plan expected");
    };
    assert!(plan_contains_index(&root));
    assert!(!plan_contains_columnar(&root));
    database.close().expect("close database");
    cleanup(&heap, &projection);
}

#[test]
fn missing_columns_and_explicit_transaction_writes_fall_back_authoritative() {
    let heap = path("eligibility");
    let projection = path("eligibility-projection");
    cleanup(&heap, &projection);
    let mut database = Database::create(&heap, table()).expect("create heap database");
    insert_rows(&mut database, 512);
    let partial = database
        .build_columnar_projection(ColumnarProjectionSpec::new(
            TableId(1),
            &projection,
            vec![ColumnId(1), ColumnId(2)],
        ))
        .expect("build partial projection");
    assert!(!statement_uses_columnar(
        &database,
        "SELECT label FROM events WHERE id >= 0"
    ));
    database
        .drop_columnar_projection(partial)
        .expect("drop partial projection");

    let full = database
        .build_columnar_projection(ColumnarProjectionSpec::new(
            TableId(1),
            &projection,
            vec![ColumnId(1), ColumnId(2), ColumnId(3), ColumnId(4)],
        ))
        .expect("build full projection");
    let mut transaction = database
        .begin_transaction_with_isolation(crate::IsolationLevel::RepeatableRead)
        .expect("begin repeatable-read transaction");
    database
        .execute_in(
            &mut transaction,
            "INSERT INTO events (id, amount, active, label) VALUES (900, 1, TRUE, 'own')",
        )
        .expect("stage own insert");
    let ExecutionResult::Query(inside) = database
        .execute_in(&mut transaction, "SELECT COUNT(*) FROM events")
        .expect("read own write")
    else {
        panic!("query result expected");
    };
    assert_eq!(inside.rows, vec![vec![ScalarValue::UInt64(513)]]);
    transaction.rollback().expect("rollback own write");
    assert_eq!(
        database.inspect_columnar_projections()[0].health,
        ColumnarProjectionHealth::Fresh
    );

    database
        .execute(
            "INSERT INTO events (id, amount, active, label) VALUES (901, 2, FALSE, 'committed')",
        )
        .expect("commit insert");
    assert_eq!(
        database.inspect_columnar_projections()[0].health,
        ColumnarProjectionHealth::Stale
    );
    assert!(!statement_uses_columnar(
        &database,
        "SELECT COUNT(*) FROM events"
    ));
    assert_eq!(
        database
            .query("SELECT COUNT(*) FROM events")
            .expect("authoritative fallback")
            .rows,
        vec![vec![ScalarValue::UInt64(513)]]
    );
    database
        .refresh_columnar_projection(full)
        .expect("refresh after insert");
    assert!(statement_uses_columnar(
        &database,
        "SELECT COUNT(*) FROM events"
    ));
    database.close().expect("close database");
    cleanup(&heap, &projection);
}

#[test]
fn lsm_projection_builds_and_reopens_by_explicit_attachment() {
    let lsm = path("lsm");
    let projection = path("lsm-projection");
    let _ = fs::remove_dir_all(&lsm);
    let _ = fs::remove_dir_all(&projection);
    let mut database = Database::create_storages(vec![TableStorageCreateSpec::lsm(
        &lsm,
        table(),
        ColumnId(1),
    )])
    .expect("create LSM database");
    insert_rows(&mut database, 512);
    let id = database
        .build_columnar_projection(ColumnarProjectionSpec::new(
            TableId(1),
            &projection,
            vec![ColumnId(1), ColumnId(2), ColumnId(3), ColumnId(4)],
        ))
        .expect("build LSM projection");
    let sql = "SELECT COUNT(*), SUM(id) FROM events WHERE id >= 100";
    assert!(statement_uses_columnar(&database, sql));
    let expected = database.query(sql).expect("query LSM projection");
    database.close().expect("close LSM database");

    let mut reopened =
        Database::open_storages(vec![crate::TableStorageOpenSpec::lsm(&lsm, table())])
            .expect("reopen LSM database");
    assert!(!statement_uses_columnar(&reopened, sql));
    assert_eq!(
        reopened
            .attach_columnar_projection(&projection, TableId(1))
            .expect("attach projection"),
        id
    );
    assert_eq!(
        reopened.inspect_columnar_projections()[0].health,
        ColumnarProjectionHealth::Fresh,
        "reattached metadata: {:?}; current={:?}",
        reopened.inspect_columnar_projections(),
        reopened.registry.iter().next().map(|entry| entry
            .storage
            .current_snapshot_token()
            .map(|token| token.diagnostic()))
    );
    assert!(statement_uses_columnar(&reopened, sql));
    assert_eq!(
        reopened.query(sql).expect("query attached projection"),
        expected
    );
    reopened
        .execute("DELETE FROM events WHERE id = 511")
        .expect("commit LSM delete");
    assert_eq!(
        reopened.inspect_columnar_projections()[0].health,
        ColumnarProjectionHealth::Stale
    );
    reopened
        .compact_full(TableId(1))
        .expect("compact LSM tombstone");
    assert_eq!(
        reopened.inspect_columnar_projections()[0].health,
        ColumnarProjectionHealth::Stale,
        "compaction must not make a pre-delete token equal again"
    );
    reopened
        .refresh_columnar_projection(id)
        .expect("refresh LSM projection");
    let mut transaction = reopened
        .begin_transaction_with_isolation(crate::IsolationLevel::RepeatableRead)
        .expect("begin LSM rollback transaction");
    reopened
        .execute_in(&mut transaction, "DELETE FROM events WHERE id = 510")
        .expect("stage LSM delete");
    transaction.rollback().expect("rollback LSM delete");
    assert_eq!(
        reopened.inspect_columnar_projections()[0].health,
        ColumnarProjectionHealth::Fresh
    );
    reopened.close().expect("close reopened LSM");
    let _ = fs::remove_dir_all(lsm);
    let _ = fs::remove_dir_all(projection);
}

#[test]
fn build_aborts_if_source_commits_during_the_build_window() {
    let heap = path("race");
    let projection = path("race-projection");
    cleanup(&heap, &projection);
    let mut database = Database::create(&heap, table()).expect("create database");
    insert_rows(&mut database, 4);
    let result = database.build_columnar_projection_with(
        ColumnarProjectionSpec::new(
            TableId(1),
            &projection,
            vec![ColumnId(1), ColumnId(2), ColumnId(3), ColumnId(4)],
        ),
        |database| {
            database
                .execute(
                    "INSERT INTO events (id, amount, active, label) VALUES (9, 9, TRUE, 'race')",
                )
                .map(|_| ())
        },
    );
    assert!(matches!(
        result,
        Err(DatabaseError::ColumnarBuildSourceChanged { .. })
    ));
    assert!(!projection.join("projection.nbcmanifest").exists());
    assert!(database.inspect_columnar_projections().is_empty());
    assert_eq!(
        fs::read_dir(&projection)
            .expect("projection staging directory")
            .count(),
        0,
        "aborted build must remove synced temporary generation files"
    );
    database.close().expect("close database");
    cleanup(&heap, &projection);
}

#[test]
fn corrupt_projection_is_unavailable_while_authoritative_query_remains_usable() {
    let heap = path("corrupt");
    let projection = path("corrupt-projection");
    cleanup(&heap, &projection);
    let mut database = Database::create(&heap, table()).expect("create database");
    insert_rows(&mut database, 8);
    database
        .build_columnar_projection(ColumnarProjectionSpec::new(
            TableId(1),
            &projection,
            vec![ColumnId(1), ColumnId(2), ColumnId(3), ColumnId(4)],
        ))
        .expect("build projection");
    database.close().expect("close database");
    let segment = fs::read_dir(&projection)
        .expect("read projection directory")
        .map(|entry| entry.expect("directory entry").path())
        .find(|path| {
            path.extension()
                .is_some_and(|extension| extension == "nbcs")
        })
        .expect("segment path");
    let mut bytes = fs::read(&segment).expect("read segment");
    bytes.truncate(bytes.len() / 2);
    fs::write(&segment, bytes).expect("truncate segment");

    let mut reopened = Database::open(&heap, table()).expect("reopen database");
    assert!(
        reopened
            .attach_columnar_projection(&projection, TableId(1))
            .is_err()
    );
    assert_eq!(
        Database::inspect_columnar_projection_path(&projection, &table()).health,
        ColumnarProjectionHealth::Unavailable
    );
    assert_eq!(
        reopened
            .query("SELECT COUNT(*) FROM events")
            .expect("authoritative query")
            .rows,
        vec![vec![ScalarValue::UInt64(8)]]
    );
    reopened.close().expect("close reopened database");
    cleanup(&heap, &projection);
}

#[test]
fn attach_rejects_projection_from_a_different_physical_storage() {
    let source_heap = path("source-identity");
    let first_target_heap = path("target-first");
    let second_target_heap = path("target-second");
    let projection = path("source-identity-projection");
    cleanup(&source_heap, &projection);
    cleanup_created_table_files(&[first_target_heap.clone(), second_target_heap.clone()]);

    let mut source = Database::create(&source_heap, table()).expect("create source database");
    insert_rows(&mut source, 2);
    source
        .build_columnar_projection(ColumnarProjectionSpec::new(
            TableId(1),
            &projection,
            vec![ColumnId(1), ColumnId(2), ColumnId(3), ColumnId(4)],
        ))
        .expect("build source projection");
    source.close().expect("close source database");

    let other_table = TableDef::new(
        TableId(2),
        "other",
        vec![ColumnDef::new(
            ColumnId(20),
            "id",
            TypeSpec::Physical(PhysicalType::Int64),
        )],
    );
    let mut target = Database::create_storages(vec![
        TableStorageCreateSpec::heap(&first_target_heap, other_table),
        TableStorageCreateSpec::heap(&second_target_heap, table()),
    ])
    .expect("create target database with different storage identity");
    assert!(
        target
            .attach_columnar_projection(&projection, TableId(1))
            .is_err(),
        "a projection bound to source storage 1 must not attach to storage 2"
    );
    assert!(target.inspect_columnar_projections().is_empty());
    target.close().expect("close target database");

    cleanup(&source_heap, &projection);
    cleanup_created_table_files(&[first_target_heap, second_target_heap]);
}
