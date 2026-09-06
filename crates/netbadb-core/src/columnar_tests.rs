use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

use netbadb_inspect::{PlanNodeInspection, StatementPlanInspection};
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_types::{ColumnId, PhysicalType, ScalarValue, TableId};

use crate::{
    ColumnarAdvanceBudget, ColumnarProjectionHealth, ColumnarProjectionSpec, Database,
    DatabaseError, ExecutionResult, ProjectionCatalogError, TableStorageCreateSpec,
    cleanup_created_table_files,
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

fn authoritative(database: &mut Database, sql: &str) -> crate::QueryResult {
    let mut transaction = database
        .begin_transaction_with_isolation(crate::IsolationLevel::RepeatableRead)
        .expect("begin authoritative transaction");
    let ExecutionResult::Query(result) = database
        .execute_in(&mut transaction, sql)
        .expect("authoritative query")
    else {
        panic!("query result expected");
    };
    transaction.rollback().expect("rollback read transaction");
    result
}

#[test]
fn incremental_heap_merge_is_predicate_safe_and_reopens() {
    let root = path("incremental-heap-root");
    let catalog = root.join("catalog");
    let heap = root.join("heap");
    let projection = root.join("projection");
    fs::create_dir_all(&root).expect("create root");
    let mut database = Database::create_catalog(
        &catalog,
        vec![TableStorageCreateSpec::heap(&heap, table())],
        None,
    )
    .expect("create managed database");
    insert_rows(&mut database, 512);
    database
        .enable_change_stream(TableId(1))
        .expect("enable stream explicitly");
    let id = database
        .build_incremental_columnar_projection(
            ColumnarProjectionSpec::new(
                TableId(1),
                &projection,
                vec![ColumnId(1), ColumnId(2), ColumnId(3), ColumnId(4)],
            )
            .with_row_group_rows(256),
        )
        .expect("build incremental projection");
    assert_eq!(
        database.inspect_columnar_projections()[0].health,
        ColumnarProjectionHealth::Fresh
    );
    let manifest_before_dml = fs::read(projection.join("projection.nbcmanifest"))
        .expect("read incremental manifest before DML");
    let files_before_dml = fs::read_dir(&projection)
        .expect("read projection directory")
        .map(|entry| entry.expect("projection entry").file_name())
        .collect::<Vec<_>>();

    database
        .execute("UPDATE events SET amount = 0 WHERE id = 100")
        .expect("update out of predicate");
    database
        .execute("UPDATE events SET amount = 1000 WHERE id = 1")
        .expect("update into predicate");
    database
        .execute("DELETE FROM events WHERE id = 2")
        .expect("delete base row");
    database
        .execute("INSERT INTO events (id, amount, active, label) VALUES (900, 700, TRUE, 'delta')")
        .expect("insert delta row");
    for amount in [7, 8, 9] {
        database
            .execute(&format!("UPDATE events SET amount = {amount} WHERE id = 3"))
            .expect("advance update chain");
    }
    database
        .execute("INSERT INTO events (id, amount, active, label) VALUES (901, 800, TRUE, 'gone')")
        .expect("insert transient row");
    database
        .execute("DELETE FROM events WHERE id = 901")
        .expect("delete transient row");
    assert_eq!(
        fs::read(projection.join("projection.nbcmanifest")).expect("read manifest after DML"),
        manifest_before_dml,
        "authoritative commit must not synchronously update NBCM"
    );
    assert_eq!(
        fs::read_dir(&projection)
            .expect("read projection directory after DML")
            .map(|entry| entry.expect("projection entry").file_name())
            .collect::<Vec<_>>(),
        files_before_dml,
        "authoritative commit must not create NBCD files"
    );

    let lagging = &database.inspect_columnar_projections()[0];
    assert_eq!(lagging.health, ColumnarProjectionHealth::Lagging);
    assert!(!statement_uses_columnar(
        &database,
        "SELECT id FROM events WHERE amount > 50"
    ));
    let report = database
        .advance_columnar_projection(id, ColumnarAdvanceBudget::new(100, 64 * 1024 * 1024))
        .expect("advance delta");
    assert!(report.caught_up);
    assert_eq!(report.batches_applied, 9);
    assert!(report.delta_segment_created);
    let fresh = &database.inspect_columnar_projections()[0];
    assert_eq!(fresh.health, ColumnarProjectionHealth::Fresh);
    assert_eq!(fresh.delta_live_rows, Some(4));
    assert!(fresh.suppressed_versions.is_some_and(|count| count >= 7));

    for sql in [
        "SELECT id FROM events WHERE amount > 50",
        "SELECT id FROM events WHERE amount IS NULL",
        "SELECT COUNT(*), COUNT(amount), SUM(amount), MIN(amount), MAX(amount) FROM events",
        "SELECT active, COUNT(*), COUNT(amount), SUM(amount), MIN(amount), MAX(amount) FROM events GROUP BY active",
    ] {
        let expected = authoritative(&mut database, sql);
        assert!(statement_uses_columnar(&database, sql), "{sql}");
        let (actual, statistics) = database
            .query_with_columnar_statistics(sql)
            .expect("merged columnar query");
        assert_eq!(actual, expected, "{sql}");
        assert_eq!(statistics.scan.delta_live_rows, 4);
    }

    database.close().expect("close database");
    let mut reopened = Database::open_catalog(&catalog).expect("reopen managed database");
    assert_eq!(
        reopened.inspect_columnar_projections()[0].health,
        ColumnarProjectionHealth::Fresh
    );
    let sql = "SELECT id FROM events WHERE amount > 50";
    assert!(statement_uses_columnar(&reopened, sql));
    let expected = authoritative(&mut reopened, sql);
    assert_eq!(
        reopened.query(sql).expect("reopened merged query"),
        expected
    );
    reopened.close().expect("close reopened database");
    let delta = fs::read_dir(&projection)
        .expect("read projection directory")
        .map(|entry| entry.expect("projection entry").path())
        .find(|path| {
            path.extension()
                .is_some_and(|extension| extension == "nbcd")
        })
        .expect("delta segment");
    fs::remove_file(delta).expect("remove delta segment");
    let mut degraded = Database::open_catalog(&catalog).expect("authoritative reopen");
    assert_eq!(
        degraded.inspect_columnar_projections()[0].health,
        ColumnarProjectionHealth::Unavailable
    );
    assert_eq!(
        degraded
            .query("SELECT COUNT(*) FROM events")
            .expect("authoritative fallback")
            .rows,
        vec![vec![ScalarValue::UInt64(512)]]
    );
    degraded.close().expect("close degraded database");
    fs::remove_dir_all(root).expect("remove fixture");
}

#[test]
fn incremental_heap_null_chains_and_duplicate_values_merge_by_version_identity() {
    let heap = path("incremental-identity-heap");
    let projection = path("incremental-identity-projection");
    cleanup(&heap, &projection);
    let mut database = Database::create(&heap, table()).expect("create heap database");
    insert_rows(&mut database, 512);
    for values in [
        "(1001, NULL, TRUE, 'null-to-value')",
        "(1002, 20, FALSE, 'value-to-null')",
        "(1003, NULL, TRUE, 'deleted-null')",
        "(1009, 10, TRUE, 'duplicate-a')",
        "(1009, 10, TRUE, 'duplicate-b')",
    ] {
        database
            .execute(&format!(
                "INSERT INTO events (id, amount, active, label) VALUES {values}"
            ))
            .expect("insert identity fixture");
    }
    database
        .enable_change_stream(TableId(1))
        .expect("enable stream");
    let id = database
        .build_incremental_columnar_projection(ColumnarProjectionSpec::new(
            TableId(1),
            &projection,
            vec![ColumnId(1), ColumnId(2), ColumnId(3)],
        ))
        .expect("build incremental projection");

    database
        .execute("UPDATE events SET amount = 42 WHERE id = 1001")
        .expect("NULL to value");
    database
        .execute("UPDATE events SET amount = NULL WHERE id = 1002")
        .expect("value to NULL");
    database
        .execute("DELETE FROM events WHERE id = 1003")
        .expect("delete NULL base row");
    database.vacuum(TableId(1)).expect("vacuum deleted slot");
    database
        .execute(
            "INSERT INTO events (id, amount, active, label) VALUES (1004, NULL, FALSE, 'reused-slot')",
        )
        .expect("insert after slot retirement");
    database
        .execute(
            "INSERT INTO events (id, amount, active, label) VALUES (1005, NULL, TRUE, 'delta-chain')",
        )
        .expect("insert delta chain");
    database
        .execute("UPDATE events SET amount = 5 WHERE id = 1005")
        .expect("first delta update");
    database
        .execute("UPDATE events SET amount = NULL WHERE id = 1005")
        .expect("second delta update");
    database
        .execute(
            "INSERT INTO events (id, amount, active, label) VALUES (1006, NULL, TRUE, 'transient')",
        )
        .expect("insert transient NULL");
    database
        .execute("DELETE FROM events WHERE id = 1006")
        .expect("delete transient NULL");
    database
        .execute("UPDATE events SET amount = 11 WHERE label = 'duplicate-a'")
        .expect("update one duplicate-valued version");
    database
        .execute("DELETE FROM events WHERE label = 'duplicate-b'")
        .expect("delete the other duplicate-valued version");

    database
        .advance_columnar_projection(id, ColumnarAdvanceBudget::new(100, 64 * 1024 * 1024))
        .expect("advance identity delta");
    for sql in [
        "SELECT id, amount, active FROM events WHERE id >= 1000",
        "SELECT id FROM events WHERE id >= 1000 AND amount IS NULL",
        "SELECT id FROM events WHERE id >= 1000 AND amount IS NOT NULL",
        "SELECT COUNT(*), COUNT(amount), SUM(amount), MIN(amount), MAX(amount) FROM events WHERE id >= 1000",
        "SELECT active, COUNT(*), COUNT(amount), SUM(amount), MIN(amount), MAX(amount) FROM events WHERE id >= 1000 GROUP BY active",
    ] {
        let expected = authoritative(&mut database, sql);
        assert!(statement_uses_columnar(&database, sql), "{sql}");
        assert_eq!(
            database.query(sql).expect("columnar query"),
            expected,
            "{sql}"
        );
    }
    assert_eq!(
        database
            .query("SELECT id, amount FROM events WHERE id = 1009")
            .expect("query surviving duplicate")
            .rows,
        vec![vec![ScalarValue::Int64(1009), ScalarValue::Int64(11)]],
        "equal business values must not cause cross-row suppression"
    );
    database.close().expect("close database");
    cleanup(&heap, &projection);
}

#[test]
fn incremental_build_accepts_concurrent_commits_and_requires_same_stream() {
    let heap = path("incremental-race-heap");
    let projection = path("incremental-race-projection");
    cleanup(&heap, &projection);
    let mut database = Database::create(&heap, table()).expect("create heap database");
    insert_rows(&mut database, 512);
    database
        .enable_change_stream(TableId(1))
        .expect("enable stream");
    let id = database
        .build_incremental_columnar_projection_with(
            ColumnarProjectionSpec::new(
                TableId(1),
                &projection,
                vec![ColumnId(1), ColumnId(2), ColumnId(3), ColumnId(4)],
            ),
            |database| {
                database.execute("INSERT INTO events (id, amount, active, label) VALUES (700, 70, TRUE, 'race-a')")?;
                database.execute("INSERT INTO events (id, amount, active, label) VALUES (701, 71, TRUE, 'race-b')")?;
                Ok(())
            },
        )
        .expect("build survives commits after anchor");
    assert_eq!(
        database.inspect_columnar_projections()[0].health,
        ColumnarProjectionHealth::Lagging
    );
    database
        .advance_columnar_projection(id, ColumnarAdvanceBudget::new(1, 64 * 1024 * 1024))
        .expect("partial advance");
    assert_eq!(
        database.inspect_columnar_projections()[0].health,
        ColumnarProjectionHealth::Lagging
    );
    database
        .advance_columnar_projection(id, ColumnarAdvanceBudget::new(10, 64 * 1024 * 1024))
        .expect("catch up");
    assert_eq!(
        database.inspect_columnar_projections()[0].health,
        ColumnarProjectionHealth::Fresh
    );

    database
        .disable_change_stream(TableId(1))
        .expect("disable stream");
    database
        .enable_change_stream(TableId(1))
        .expect("re-enable stream");
    assert_eq!(
        database.inspect_columnar_projections()[0].health,
        ColumnarProjectionHealth::RebuildRequired
    );
    assert!(
        database
            .advance_columnar_projection(id, ColumnarAdvanceBudget::new(10, 64 * 1024 * 1024))
            .is_err()
    );
    assert!(!statement_uses_columnar(
        &database,
        "SELECT COUNT(*) FROM events"
    ));
    database.close().expect("close database");
    cleanup(&heap, &projection);
}

#[test]
fn incremental_lsm_key_move_and_maintenance_preserve_freshness() {
    let root = path("incremental-lsm-root");
    let catalog = root.join("catalog");
    let lsm = root.join("lsm");
    let projection = root.join("projection");
    fs::create_dir_all(&root).expect("create root");
    let mut database = Database::create_catalog(
        &catalog,
        vec![TableStorageCreateSpec::lsm(&lsm, table(), ColumnId(1))],
        None,
    )
    .expect("create managed LSM database");
    insert_rows(&mut database, 512);
    database
        .enable_change_stream(TableId(1))
        .expect("enable stream");
    let id = database
        .build_incremental_columnar_projection(
            ColumnarProjectionSpec::new(
                TableId(1),
                &projection,
                vec![ColumnId(1), ColumnId(2), ColumnId(3), ColumnId(4)],
            )
            .with_row_group_rows(256),
        )
        .expect("build incremental LSM projection");
    database
        .execute("UPDATE events SET id = 700, amount = 777 WHERE id = 100")
        .expect("move LSM clustering key");
    database
        .advance_columnar_projection(id, ColumnarAdvanceBudget::new(10, 64 * 1024 * 1024))
        .expect("advance LSM delta");
    let sql = "SELECT id, amount, active, label FROM events WHERE id >= 695";
    let expected = authoritative(&mut database, sql);
    assert!(statement_uses_columnar(&database, sql));
    assert_eq!(database.query(sql).expect("merged LSM query"), expected);
    database.flush().expect("flush LSM");
    database.compact_full(TableId(1)).expect("compact LSM");
    assert_eq!(
        database.inspect_columnar_projections()[0].health,
        ColumnarProjectionHealth::Fresh,
        "physical LSM maintenance must not alter logical data frontier"
    );
    database.close().expect("close LSM database");
    let mut reopened = Database::open_catalog(&catalog).expect("reopen LSM database");
    assert_eq!(
        reopened.inspect_columnar_projections()[0].health,
        ColumnarProjectionHealth::Fresh
    );
    assert!(statement_uses_columnar(&reopened, sql));
    assert_eq!(reopened.query(sql).expect("reopened LSM query"), expected);
    let generation = reopened
        .refresh_columnar_projection(id)
        .expect("rebaseline incremental projection");
    assert_eq!(generation, netbadb_types::ColumnarGeneration(2));
    let refreshed = &reopened.inspect_columnar_projections()[0];
    assert_eq!(refreshed.health, ColumnarProjectionHealth::Fresh);
    assert_eq!(refreshed.delta_mutations, Some(0));
    reopened.close().expect("close refreshed database");
    fs::remove_dir_all(root).expect("remove fixture");
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
fn lsm_projection_is_discovered_automatically_after_reopen() {
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
    assert_eq!(reopened.inspect_columnar_projections().len(), 1);
    assert_eq!(
        reopened.inspect_columnar_projections()[0].projection_id,
        Some(id)
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
fn managed_catalog_discovers_projections_and_never_reuses_dropped_ids() {
    let root = path("managed-identity-root");
    let catalog = root.join("catalog");
    let heap = root.join("heap");
    let projection_a = root.join("projection-a");
    let projection_b = root.join("projection-b");
    let projection_c = root.join("projection-c");
    let projection_d = root.join("projection-d");
    fs::create_dir_all(&root).expect("create root");
    let mut database = Database::create_catalog(
        &catalog,
        vec![TableStorageCreateSpec::heap(&heap, table())],
        None,
    )
    .expect("create managed database");
    insert_rows(&mut database, 512);
    let build = |database: &mut Database, directory: &PathBuf| {
        database
            .build_columnar_projection(
                ColumnarProjectionSpec::new(
                    TableId(1),
                    directory,
                    vec![ColumnId(1), ColumnId(2), ColumnId(3)],
                )
                .with_row_group_rows(64),
            )
            .expect("build managed projection")
    };
    let a = build(&mut database, &projection_a);
    assert!(
        database
            .build_columnar_projection(ColumnarProjectionSpec::new(
                TableId(1),
                &projection_a,
                vec![ColumnId(1)],
            ))
            .is_err(),
        "one managed location cannot be registered twice"
    );
    let b = build(&mut database, &projection_b);
    assert_eq!((a.0, b.0), (1, 2));
    database.close().expect("close database");

    let mut reopened = Database::open_catalog(&catalog).expect("reopen managed database");
    assert_eq!(
        reopened
            .inspect_columnar_projections()
            .iter()
            .map(|entry| entry.projection_id.expect("managed identity").0)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert!(statement_uses_columnar(
        &reopened,
        "SELECT COUNT(*) FROM events"
    ));
    let c = build(&mut reopened, &projection_c);
    assert_eq!(c.0, 3);
    reopened
        .drop_columnar_projection(b)
        .expect("drop projection B");
    reopened
        .drop_columnar_projection(b)
        .expect("repeat logical drop is idempotent");
    reopened.close().expect("close reopened database");

    let mut reopened = Database::open_catalog(&catalog).expect("reopen after drop");
    let d = build(&mut reopened, &projection_d);
    assert_eq!(d.0, 4);
    assert_eq!(
        reopened
            .inspect_columnar_projection_catalog()
            .next_projection_id
            .expect("next identity")
            .0,
        5
    );
    reopened.close().expect("close final database");
    fs::remove_dir_all(root).expect("remove managed fixture");
}

#[test]
fn missing_projection_segment_is_unavailable_but_inventory_and_database_survive() {
    let root = path("missing-managed-segment-root");
    let catalog = root.join("catalog");
    let heap = root.join("heap");
    let projection = root.join("projection");
    fs::create_dir_all(&root).expect("create root");
    let mut database = Database::create_catalog(
        &catalog,
        vec![TableStorageCreateSpec::heap(&heap, table())],
        None,
    )
    .expect("create database");
    insert_rows(&mut database, 8);
    let id = database
        .build_columnar_projection(ColumnarProjectionSpec::new(
            TableId(1),
            &projection,
            vec![ColumnId(1), ColumnId(2)],
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
    fs::remove_file(segment).expect("remove segment");

    let mut reopened = Database::open_catalog(&catalog).expect("authoritative reopen");
    let inventory = reopened.inspect_columnar_projections();
    assert_eq!(inventory.len(), 1);
    assert_eq!(inventory[0].projection_id, Some(id));
    assert_eq!(inventory[0].health, ColumnarProjectionHealth::Unavailable);
    assert!(
        inventory[0]
            .detail
            .as_deref()
            .is_some_and(|detail| detail.contains("I/O"))
    );
    assert_eq!(
        reopened
            .query("SELECT COUNT(*) FROM events")
            .expect("authoritative query")
            .rows,
        vec![vec![ScalarValue::UInt64(8)]]
    );
    let replacement = reopened
        .build_columnar_projection(ColumnarProjectionSpec::new(
            TableId(1),
            root.join("replacement"),
            vec![ColumnId(1)],
        ))
        .expect("build replacement identity");
    assert_eq!(replacement.0, 2);
    reopened.close().expect("close database");
    fs::remove_dir_all(root).expect("remove fixture");
}

#[test]
fn corrupt_projection_catalog_degrades_only_the_projection_subsystem() {
    let root = path("corrupt-managed-catalog-root");
    let catalog = root.join("catalog");
    let heap = root.join("heap");
    fs::create_dir_all(&root).expect("create root");
    let mut database = Database::create_catalog(
        &catalog,
        vec![TableStorageCreateSpec::heap(&heap, table())],
        None,
    )
    .expect("create database");
    insert_rows(&mut database, 4);
    database
        .build_columnar_projection(ColumnarProjectionSpec::new(
            TableId(1),
            root.join("projection"),
            vec![ColumnId(1)],
        ))
        .expect("build projection");
    database.close().expect("close database");
    let projection_catalog = root.join("catalog.projections");
    let mut bytes = fs::read(&projection_catalog).expect("read projection catalog");
    bytes[20] ^= 0x40;
    fs::write(&projection_catalog, bytes).expect("corrupt projection catalog");

    let mut reopened = Database::open_catalog(&catalog).expect("authoritative reopen");
    let inspection = reopened.inspect_columnar_projection_catalog();
    assert!(inspection.managed);
    assert!(!inspection.available);
    assert!(
        inspection
            .detail
            .as_deref()
            .is_some_and(|detail| detail.contains("checksum"))
    );
    assert_eq!(
        reopened
            .query("SELECT COUNT(*) FROM events")
            .expect("authoritative query")
            .rows,
        vec![vec![ScalarValue::UInt64(4)]]
    );
    assert!(matches!(
        reopened.build_columnar_projection(ColumnarProjectionSpec::new(
            TableId(1),
            root.join("blocked"),
            vec![ColumnId(1)],
        )),
        Err(DatabaseError::ProjectionCatalog(
            ProjectionCatalogError::Unavailable(_)
        ))
    ));
    reopened.close().expect("close database");
    fs::remove_dir_all(root).expect("remove fixture");
}

#[test]
fn missing_projection_catalog_with_durable_marker_degrades_only_projections() {
    let root = path("missing-managed-catalog-root");
    let catalog = root.join("catalog");
    fs::create_dir_all(&root).expect("create root");
    let mut database = Database::create_catalog(
        &catalog,
        vec![TableStorageCreateSpec::heap(root.join("heap"), table())],
        None,
    )
    .expect("create database");
    insert_rows(&mut database, 2);
    database
        .build_columnar_projection(ColumnarProjectionSpec::new(
            TableId(1),
            root.join("projection"),
            vec![ColumnId(1)],
        ))
        .expect("build projection");
    database.close().expect("close database");
    fs::remove_file(root.join("catalog.projections")).expect("remove projection catalog");

    let mut reopened = Database::open_catalog(&catalog).expect("authoritative reopen");
    let inspection = reopened.inspect_columnar_projection_catalog();
    assert!(!inspection.available);
    assert!(
        inspection
            .detail
            .as_deref()
            .is_some_and(|detail| detail.contains("catalog is missing"))
    );
    assert_eq!(
        reopened
            .query("SELECT COUNT(*) FROM events")
            .expect("authoritative query")
            .rows,
        vec![vec![ScalarValue::UInt64(2)]]
    );
    reopened.close().expect("close database");
    fs::remove_dir_all(root).expect("remove fixture");
}

#[test]
fn schema_rewrite_and_drop_table_invalidate_managed_projection_identity() {
    let root = path("schema-invalidation-root");
    let catalog = root.join("catalog");
    fs::create_dir_all(&root).expect("create root");
    let mut database = Database::create_catalog(
        &catalog,
        vec![],
        Some(crate::DatabaseCoordinatorConfig::new(
            root.join("coordinator"),
        )),
    )
    .expect("create database");
    database
        .execute(
            "CREATE TABLE events (id BIGINT NOT NULL, amount BIGINT, active BOOLEAN NOT NULL, label TEXT NOT NULL)",
        )
        .expect("create runtime table");
    insert_rows(&mut database, 4);
    let id = database
        .build_columnar_projection(ColumnarProjectionSpec::new(
            TableId(1),
            root.join("projection"),
            vec![ColumnId(1)],
        ))
        .expect("build projection");
    database
        .execute("ALTER TABLE events ADD COLUMN note TEXT")
        .expect("rewrite schema");
    assert_eq!(
        database.inspect_columnar_projections()[0].health,
        ColumnarProjectionHealth::Stale
    );
    assert!(!statement_uses_columnar(
        &database,
        "SELECT COUNT(*) FROM events"
    ));
    database.close().expect("close rewritten database");

    let mut reopened = Database::open_catalog(&catalog).expect("reopen rewritten database");
    assert_eq!(
        reopened.inspect_columnar_projections()[0].projection_id,
        Some(id)
    );
    assert_eq!(
        reopened.inspect_columnar_projections()[0].health,
        ColumnarProjectionHealth::Unavailable
    );
    reopened.execute("DROP TABLE events").expect("drop table");
    reopened.close().expect("close dropped database");
    let reopened = Database::open_catalog(&catalog).expect("reopen dropped database");
    let inventory = reopened.inspect_columnar_projections();
    assert_eq!(inventory.len(), 1);
    assert_eq!(inventory[0].projection_id, Some(id));
    assert_eq!(inventory[0].health, ColumnarProjectionHealth::Unavailable);
    drop(reopened);
    fs::remove_dir_all(root).expect("remove fixture");
}

#[test]
fn deferred_backfill_uses_s1_and_stales_the_old_columnar_identity() {
    let root = path("deferred-backfill");
    let catalog = root.join("catalog");
    fs::create_dir_all(&root).expect("create root");
    let mut database = Database::create_catalog(
        &catalog,
        vec![],
        Some(crate::DatabaseCoordinatorConfig::new(
            root.join("coordinator"),
        )),
    )
    .expect("create database");
    database
        .execute("CREATE TABLE events (id BIGINT NOT NULL, label TEXT)")
        .expect("create events");
    for id in 0..256 {
        database
            .execute(&format!("INSERT INTO events VALUES ({id}, 'old-{id}')"))
            .expect("insert event");
    }
    let table = database.schema().table("events").unwrap().id;
    let projection = database
        .build_columnar_projection(ColumnarProjectionSpec::new(
            table,
            root.join("projection"),
            vec![ColumnId(1), ColumnId(2)],
        ))
        .expect("build projection");
    let analytical = "SELECT COUNT(*), MIN(id), MAX(id) FROM events WHERE id >= 0";
    assert!(statement_uses_columnar(&database, analytical));

    let mut transaction = database.begin_transaction().expect("begin migration");
    database
        .execute_in(
            &mut transaction,
            "UPDATE events SET label = 'own-write' WHERE id = 1",
        )
        .expect("update S1");
    database
        .execute_in(
            &mut transaction,
            "ALTER TABLE events ADD COLUMN marker TEXT",
        )
        .expect("add marker");
    assert_eq!(
        database
            .execute_in(
                &mut transaction,
                "UPDATE events SET marker = label WHERE id = 1",
            )
            .expect("observe S1"),
        ExecutionResult::AffectedRows(1)
    );
    database
        .commit_transaction(&mut transaction)
        .expect("commit migration");
    assert_eq!(
        database
            .query("SELECT marker FROM events WHERE id = 1")
            .expect("query marker")
            .rows,
        vec![vec![ScalarValue::Text("own-write".into())]]
    );
    assert_eq!(
        database.inspect_columnar_projections()[0].health,
        ColumnarProjectionHealth::Stale
    );
    let (_, stale_stats) = database
        .query_with_columnar_statistics(analytical)
        .expect("authoritative fallback");
    assert_eq!(stale_stats.projection_id, None);

    let (_, marker_stats) = database
        .query_with_columnar_statistics("SELECT COUNT(*) FROM events WHERE marker IS NOT NULL")
        .expect("marker query fallback");
    assert_eq!(marker_stats.projection_id, None);
    assert_eq!(
        database.inspect_columnar_projections()[0].projection_id,
        Some(projection)
    );
    assert!(matches!(
        database.refresh_columnar_projection(projection),
        Err(DatabaseError::ProjectionCatalog(
            ProjectionCatalogError::Corrupt("replacement projection identity changed")
        ))
    ));
    assert!(!statement_uses_columnar(&database, analytical));

    database.close().expect("close database");
    fs::remove_dir_all(root).expect("remove fixture");
}

#[test]
fn catalog_entry_for_partitioned_table_stays_unavailable() {
    let root = path("partitioned-catalog-entry-root");
    let catalog = root.join("catalog");
    fs::create_dir_all(&root).expect("create root");
    let partitions = vec![
        crate::RangePartitionSpec::new(
            netbadb_types::PartitionId(1),
            root.join("low"),
            None,
            Some(ScalarValue::Int64(0)),
        ),
        crate::RangePartitionSpec::new(
            netbadb_types::PartitionId(2),
            root.join("high"),
            Some(ScalarValue::Int64(0)),
            None,
        ),
    ];
    Database::create_catalog_with_placements(
        &catalog,
        vec![crate::TablePlacementSpec::range_partitioned(
            table(),
            ColumnId(1),
            partitions,
        )],
        crate::PartitionCatalogConfig::new(root.join("placements"), root.join("coordinator")),
    )
    .expect("create partitioned database")
    .close()
    .expect("close partitioned database");
    let snapshot = crate::schema_catalog_file::load(&catalog).expect("load schema catalog");
    let mut projection_catalog = crate::projection_catalog::ProjectionCatalog::open_or_initialize(
        &catalog,
        snapshot.incarnation,
    )
    .expect("open projection catalog");
    let projection_path = root.join("unsupported-projection");
    let locator = projection_catalog
        .locator(&projection_path)
        .expect("projection locator");
    projection_catalog
        .insert(crate::projection_catalog::ProjectionCatalogEntry {
            id: netbadb_types::ColumnarProjectionId(1),
            table_id: TableId(1),
            source_storage_id: netbadb_types::StorageId(1),
            generation: netbadb_types::ColumnarGeneration(1),
            schema_fingerprint: table().fingerprint().expect("table fingerprint"),
            locator,
        })
        .expect("publish synthetic unsupported entry");
    drop(projection_catalog);

    let reopened = Database::open_catalog(&catalog).expect("reopen partitioned database");
    let inventory = reopened.inspect_columnar_projections();
    assert_eq!(inventory.len(), 1);
    assert_eq!(inventory[0].health, ColumnarProjectionHealth::Unavailable);
    assert!(
        inventory[0]
            .detail
            .as_deref()
            .is_some_and(|detail| detail.contains("partitioned"))
    );
    drop(reopened);
    fs::remove_dir_all(root).expect("remove fixture");
}

#[test]
fn projection_lifecycle_crash_child() {
    let Ok(root) = std::env::var("NETBADB_COLUMNAR_CRASH_ROOT") else {
        return;
    };
    let operation =
        std::env::var("NETBADB_COLUMNAR_CRASH_OPERATION").expect("crash child operation");
    let root = PathBuf::from(root);
    let mut database = Database::open_catalog(root.join("catalog")).expect("child open database");
    match operation.as_str() {
        "build" => {
            database
                .build_columnar_projection(ColumnarProjectionSpec::new(
                    TableId(1),
                    root.join("crashing-projection"),
                    vec![ColumnId(1), ColumnId(2)],
                ))
                .expect("crash point should terminate build");
        }
        "refresh" => {
            database
                .insert(&[
                    ScalarValue::Int64(99),
                    ScalarValue::Int64(198),
                    ScalarValue::Bool(false),
                    ScalarValue::Text("refresh".into()),
                ])
                .expect("make projection stale");
            database
                .refresh_columnar_projection(netbadb_types::ColumnarProjectionId(1))
                .expect("crash point should terminate refresh");
        }
        "drop" => database
            .drop_columnar_projection(netbadb_types::ColumnarProjectionId(1))
            .expect("crash point should terminate drop"),
        _ => panic!("unknown crash child operation"),
    }
    panic!("configured crash point was not reached");
}

fn run_crash_child(root: &PathBuf, operation: &str, point: &str) {
    let status = Command::new(std::env::current_exe().expect("current test executable"))
        .args([
            "--exact",
            "columnar_tests::projection_lifecycle_crash_child",
            "--nocapture",
        ])
        .env("NETBADB_COLUMNAR_CRASH_ROOT", root)
        .env("NETBADB_COLUMNAR_CRASH_OPERATION", operation)
        .env("NETBADB_PROJECTION_CATALOG_CRASH_POINT", point)
        .status()
        .expect("run crash child");
    assert_eq!(status.code(), Some(88), "crash point {point}");
}

fn seed_crash_database(root: &PathBuf, projection: bool) {
    fs::create_dir_all(root).expect("create crash root");
    let mut database = Database::create_catalog(
        root.join("catalog"),
        vec![TableStorageCreateSpec::heap(root.join("heap"), table())],
        None,
    )
    .expect("create crash database");
    insert_rows(&mut database, 8);
    if projection {
        assert_eq!(
            database
                .build_columnar_projection(ColumnarProjectionSpec::new(
                    TableId(1),
                    root.join("projection"),
                    vec![ColumnId(1), ColumnId(2)],
                ))
                .expect("seed projection")
                .0,
            1
        );
    }
    database.close().expect("close crash seed");
}

#[test]
fn projection_catalog_build_refresh_and_drop_crash_boundaries_reopen_deterministically() {
    let reservation_points = [
        ("catalog-mid-write", 1),
        ("catalog-temp-written", 1),
        ("catalog-temp-synced", 1),
        ("catalog-before-rename", 1),
        ("catalog-after-rename", 2),
        ("catalog-renamed", 2),
        ("id-reserved", 2),
        ("build-files-synced", 2),
        ("build-manifest-published", 2),
    ];
    for (point, expected_next) in reservation_points {
        let root = path(&format!("crash-build-{point}"));
        seed_crash_database(&root, false);
        run_crash_child(&root, "build", point);
        let mut reopened =
            Database::open_catalog(root.join("catalog")).expect("reopen build crash");
        assert!(reopened.inspect_columnar_projections().is_empty());
        let id = reopened
            .build_columnar_projection(ColumnarProjectionSpec::new(
                TableId(1),
                root.join("after-crash"),
                vec![ColumnId(1)],
            ))
            .expect("build after crash");
        assert_eq!(id.0, expected_next, "crash point {point}");
        reopened.close().expect("close build crash database");
        fs::remove_dir_all(root).expect("remove build crash fixture");
    }

    let root = path("crash-build-registry-publish");
    seed_crash_database(&root, false);
    run_crash_child(&root, "build", "before-registry-publish");
    let reopened = Database::open_catalog(root.join("catalog")).expect("reopen registry crash");
    assert_eq!(reopened.inspect_columnar_projections().len(), 1);
    assert_eq!(
        reopened.inspect_columnar_projections()[0].projection_id,
        Some(netbadb_types::ColumnarProjectionId(1))
    );
    drop(reopened);
    fs::remove_dir_all(root).expect("remove registry crash fixture");

    for point in [
        "refresh-files-synced",
        "refresh-manifest-published",
        "refresh-catalog-updated",
        "refresh-old-retired",
    ] {
        let root = path(&format!("crash-refresh-{point}"));
        seed_crash_database(&root, true);
        run_crash_child(&root, "refresh", point);
        let reopened = Database::open_catalog(root.join("catalog")).expect("reopen refresh crash");
        let inspection = &reopened.inspect_columnar_projections()[0];
        let expected_generation = if point == "refresh-files-synced" {
            1
        } else {
            2
        };
        assert_eq!(
            inspection.generation,
            Some(netbadb_types::ColumnarGeneration(expected_generation)),
            "crash point {point}"
        );
        assert_eq!(
            inspection.health,
            if expected_generation == 1 {
                ColumnarProjectionHealth::Stale
            } else {
                ColumnarProjectionHealth::Fresh
            },
            "crash point {point}"
        );
        drop(reopened);
        fs::remove_dir_all(root).expect("remove refresh crash fixture");
    }

    for point in [
        "drop-catalog-removal",
        "drop-registry-removal",
        "drop-physical-cleanup",
    ] {
        let root = path(&format!("crash-drop-{point}"));
        seed_crash_database(&root, true);
        run_crash_child(&root, "drop", point);
        let reopened = Database::open_catalog(root.join("catalog")).expect("reopen drop crash");
        assert!(
            reopened.inspect_columnar_projections().is_empty(),
            "crash point {point}"
        );
        drop(reopened);
        fs::remove_dir_all(root).expect("remove drop crash fixture");
    }
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
