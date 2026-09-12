use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

use netbadb_inspect::{PlanNodeInspection, StatementPlanInspection};
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_types::{ColumnId, PhysicalType, ScalarValue, TableId};

use crate::{
    ColumnarAdvanceBudget, ColumnarProjectionHealth, ColumnarProjectionSpec, Database,
    DatabaseCoordinatorConfig, DatabaseError, ExecutionResult, IsolationLevel,
    ProjectionCatalogError, TableStorageCreateSpec, cleanup_created_table_files,
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
fn compaction_rebaselines_only_applied_state_and_lagging_projection_catches_up() {
    let root = path("phase2c-compaction");
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
    insert_rows(&mut database, 100);
    database
        .enable_change_stream(TableId(1))
        .expect("enable stream");
    let id = database
        .build_incremental_columnar_projection(ColumnarProjectionSpec::new(
            TableId(1),
            &projection,
            vec![ColumnId(1), ColumnId(2), ColumnId(3), ColumnId(4)],
        ))
        .expect("build projection");
    database
        .execute("UPDATE events SET amount = 1000 WHERE id = 1")
        .expect("update into predicate");
    database
        .execute("UPDATE events SET amount = 0 WHERE id = 30")
        .expect("update out of predicate");
    database
        .execute("DELETE FROM events WHERE id = 2")
        .expect("delete base row");
    database
        .execute("INSERT INTO events (id, amount, active, label) VALUES (900, 700, TRUE, 'delta')")
        .expect("insert live row");
    for amount in [7, 8, 9] {
        database
            .execute(&format!("UPDATE events SET amount = {amount} WHERE id = 3"))
            .expect("extend version chain");
    }
    database
        .execute("INSERT INTO events (id, amount, active, label) VALUES (901, NULL, TRUE, 'gone')")
        .expect("insert transient row");
    database
        .execute("DELETE FROM events WHERE id = 901")
        .expect("delete transient row");
    database
        .advance_columnar_projection(id, ColumnarAdvanceBudget::new(100, 64 * 1024 * 1024))
        .expect("advance mixed delta");
    let equivalence_queries = [
        "SELECT id FROM events WHERE amount > 50",
        "SELECT id FROM events WHERE amount IS NULL",
        "SELECT id FROM events WHERE amount IS NOT NULL",
        "SELECT COUNT(*), COUNT(amount), SUM(amount), MIN(amount), MAX(amount) FROM events",
        "SELECT active, COUNT(*), COUNT(amount), SUM(amount), MIN(amount), MAX(amount) FROM events GROUP BY active",
    ];
    for sql in equivalence_queries {
        let expected = authoritative(&mut database, sql);
        assert_eq!(
            database.query(sql).expect("query before compaction"),
            expected
        );
    }
    let first_compaction = database
        .compact_columnar_projection(id)
        .expect("compact mixed delta");
    assert!(first_compaction.compacted);
    assert!(first_compaction.delta_mutations_consumed >= 9);
    for sql in equivalence_queries {
        let expected = authoritative(&mut database, sql);
        assert_eq!(
            database.query(sql).expect("query after compaction"),
            expected
        );
    }

    database
        .execute("UPDATE events SET amount = 2000 WHERE id = 4")
        .expect("first source advance beyond compacted frontier");
    database
        .execute("UPDATE events SET amount = 3000 WHERE id = 6")
        .expect("second source advance beyond compacted frontier");
    database
        .advance_columnar_projection(id, ColumnarAdvanceBudget::new(1, 64 * 1024 * 1024))
        .expect("advance one batch and remain lagging");
    let lagging_before = database.inspect_columnar_projections()[0].clone();
    assert_eq!(lagging_before.health, ColumnarProjectionHealth::Lagging);
    let applied = lagging_before.applied_frontier.expect("applied frontier");
    let source_current = lagging_before
        .current_source_frontier
        .expect("source frontier");
    let report = database
        .compact_columnar_projection(id)
        .expect("compact lagging projection");
    assert!(report.compacted);
    assert_eq!(report.old_generation.0 + 1, report.new_generation.0);
    assert_eq!(report.compacted_frontier, applied);
    assert!(report.delta_segments_consumed > 0);
    let lagging_after = &database.inspect_columnar_projections()[0];
    assert_eq!(lagging_after.health, ColumnarProjectionHealth::Lagging);
    assert_eq!(lagging_after.base_frontier, Some(applied));
    assert_eq!(lagging_after.applied_frontier, Some(applied));
    assert_eq!(lagging_after.current_source_frontier, Some(source_current));
    assert_eq!(lagging_after.delta_segment_count, Some(0));
    assert_eq!(lagging_after.delta_mutations, Some(0));
    assert_eq!(lagging_after.suppressed_versions, Some(0));
    assert_eq!(lagging_after.delta_live_rows, Some(0));
    assert_eq!(lagging_after.compaction_possible, Some(false));
    database
        .advance_columnar_projection(id, ColumnarAdvanceBudget::new(10, 64 * 1024 * 1024))
        .expect("catch up after compaction");
    assert_eq!(
        database.inspect_columnar_projections()[0].health,
        ColumnarProjectionHealth::Fresh
    );
    let sql = "SELECT id FROM events WHERE amount > 500";
    let expected = authoritative(&mut database, sql);
    assert_eq!(database.query(sql).expect("query after catch-up"), expected);
    let no_op = database
        .compact_columnar_projection(id)
        .expect("compact new delta");
    assert!(no_op.compacted);
    let second_no_op = database
        .compact_columnar_projection(id)
        .expect("base-only no-op");
    assert!(!second_no_op.compacted);
    database.close().expect("close database");
    let reopened = Database::open_catalog(&catalog).expect("reopen compacted database");
    let inspection = &reopened.inspect_columnar_projections()[0];
    assert_eq!(inspection.health, ColumnarProjectionHealth::Fresh);
    assert_eq!(inspection.delta_segment_count, Some(0));
    reopened.close().expect("close reopened database");
    fs::remove_dir_all(root).expect("remove fixture");
}

#[test]
fn managed_projection_minimum_frontier_controls_change_stream_gc() {
    let root = path("phase2c-retention");
    let catalog = root.join("catalog");
    let heap = root.join("heap");
    let projection_one = root.join("projection-one");
    let projection_two = root.join("projection-two");
    fs::create_dir_all(&root).expect("create root");
    let mut database = Database::create_catalog(
        &catalog,
        vec![TableStorageCreateSpec::heap(&heap, table())],
        None,
    )
    .expect("create database");
    insert_rows(&mut database, 20);
    let origin = database
        .enable_change_stream(TableId(1))
        .expect("enable stream");
    assert!(matches!(
        database.gc_change_stream(TableId(1)),
        Err(DatabaseError::ChangeStreamGcNoRetentionConsumer(_))
    ));
    let first = database
        .build_incremental_columnar_projection(ColumnarProjectionSpec::new(
            TableId(1),
            &projection_one,
            vec![ColumnId(1), ColumnId(2)],
        ))
        .expect("build first projection");
    let second = database
        .build_incremental_columnar_projection(ColumnarProjectionSpec::new(
            TableId(1),
            &projection_two,
            vec![ColumnId(1), ColumnId(2)],
        ))
        .expect("build second projection");
    database
        .execute("UPDATE events SET amount = 101 WHERE id = 1")
        .expect("first update");
    database
        .execute("UPDATE events SET amount = 202 WHERE id = 2")
        .expect("second update");
    database
        .advance_columnar_projection(first, ColumnarAdvanceBudget::new(10, 64 * 1024 * 1024))
        .expect("advance first fully");
    database
        .advance_columnar_projection(second, ColumnarAdvanceBudget::new(1, 64 * 1024 * 1024))
        .expect("advance second once");
    let inspections = database.inspect_columnar_projections();
    let second_frontier = inspections
        .iter()
        .find(|inspection| inspection.projection_id == Some(second))
        .and_then(|inspection| inspection.applied_frontier)
        .expect("second frontier");
    let report = database
        .gc_change_stream(TableId(1))
        .expect("GC to limiting projection");
    assert_eq!(report.new_earliest_frontier, second_frontier);
    assert_eq!(report.limiting_projection_ids, vec![second]);
    assert_eq!(report.batches_removed, 1);
    assert!(matches!(
        database.read_changes(TableId(1), origin, 10, 1_000_000),
        Err(DatabaseError::Storage(crate::StorageError::ChangeStream(
            crate::ChangeStreamError::HistoryUnavailable
        )))
    ));
    database
        .advance_columnar_projection(second, ColumnarAdvanceBudget::new(10, 64 * 1024 * 1024))
        .expect("advance limiter fully");
    let report = database
        .gc_change_stream(TableId(1))
        .expect("GC through current frontier");
    assert_eq!(report.current_frontier, report.new_earliest_frontier);
    assert_eq!(report.limiting_projection_ids, vec![first, second]);
    database.close().expect("close database");

    fs::remove_file(projection_two.join("projection.nbcmanifest"))
        .expect("remove managed manifest");
    let mut reopened =
        Database::open_catalog(&catalog).expect("reopen with unavailable projection");
    assert!(matches!(
        reopened.gc_change_stream(TableId(1)),
        Err(DatabaseError::ChangeStreamGcUnsafe { .. })
    ));
    reopened.close().expect("close reopened database");
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
    let compacted = database
        .compact_columnar_projection(id)
        .expect("compact LSM key-move delta");
    assert!(compacted.compacted);
    assert_eq!(compacted.new_generation.0, compacted.old_generation.0 + 1);
    assert_eq!(database.query(sql).expect("compacted LSM query"), expected);
    database.flush().expect("flush LSM");
    database.compact_full(TableId(1)).expect("compact LSM");
    let gc = database
        .gc_change_stream(TableId(1))
        .expect("GC LSM history after physical maintenance");
    assert_eq!(gc.new_earliest_frontier, gc.current_frontier);
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
    assert_eq!(generation, netbadb_types::ColumnarGeneration(3));
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
fn lazy_block_corruption_is_quarantined_and_same_autocommit_query_retries_authoritative() {
    let heap = path("lazy-corruption-heap");
    let projection = path("lazy-corruption-projection");
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
        .expect("build lazy projection");
    let sql = "SELECT COUNT(*), SUM(id) FROM events";
    assert!(statement_uses_columnar(&database, sql));
    let expected = authoritative(&mut database, sql);
    let segment = projection.join(format!("projection-{}-g1.nbcs", id.0));
    let mut bytes = fs::read(&segment).expect("read lazy segment");
    bytes[132] ^= 0x80;
    fs::write(&segment, bytes).expect("corrupt first selected payload block");

    assert_eq!(
        database.query(sql).expect("safe authoritative retry"),
        expected
    );
    let inspection = &database.inspect_columnar_projections()[0];
    assert_eq!(inspection.health, ColumnarProjectionHealth::Unavailable);
    assert!(
        inspection
            .detail
            .as_deref()
            .is_some_and(|detail| detail.contains("checksum mismatch"))
    );
    assert!(!statement_uses_columnar(&database, sql));
    assert_eq!(database.query(sql).expect("future fallback"), expected);
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
        "incremental-build" => {
            database
                .build_incremental_columnar_projection(ColumnarProjectionSpec::new(
                    TableId(1),
                    root.join("crashing-projection"),
                    vec![ColumnId(1), ColumnId(2)],
                ))
                .expect("crash point should terminate incremental build");
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
        "compact" => {
            database
                .compact_columnar_projection(netbadb_types::ColumnarProjectionId(1))
                .expect("crash point should terminate compaction");
        }
        "drop" => database
            .drop_columnar_projection(netbadb_types::ColumnarProjectionId(1))
            .expect("crash point should terminate drop"),
        "recover" => {}
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
        .env("NETBADB_COLUMNAR_DELTA_CRASH_POINT", point)
        .status()
        .expect("run crash child");
    assert!(
        matches!(status.code(), Some(88 | 89)),
        "crash point {point}: {status:?}"
    );
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

fn seed_incremental_build_crash_database(root: &PathBuf) {
    seed_crash_database(root, false);
    let mut database = Database::open_catalog(root.join("catalog")).expect("reopen crash seed");
    database
        .enable_change_stream(TableId(1))
        .expect("enable crash fixture stream");
    database.close().expect("close incremental crash seed");
}

fn seed_recovered_planner_database(root: &PathBuf, incremental: bool) {
    fs::create_dir_all(root).expect("create recovered planner root");
    let mut database = Database::create_catalog(
        root.join("catalog"),
        vec![TableStorageCreateSpec::heap(root.join("heap"), table())],
        None,
    )
    .expect("create recovered planner database");
    insert_rows(&mut database, 512);
    if incremental {
        database
            .enable_change_stream(TableId(1))
            .expect("enable recovered planner stream");
    }
    database.close().expect("close recovered planner seed");
}

fn seed_compaction_crash_database(root: &PathBuf) {
    fs::create_dir_all(root).expect("create compaction crash root");
    let mut database = Database::create_catalog(
        root.join("catalog"),
        vec![TableStorageCreateSpec::heap(root.join("heap"), table())],
        None,
    )
    .expect("create compaction crash database");
    insert_rows(&mut database, 8);
    database
        .enable_change_stream(TableId(1))
        .expect("enable compaction crash stream");
    let id = database
        .build_incremental_columnar_projection(ColumnarProjectionSpec::new(
            TableId(1),
            root.join("projection"),
            vec![ColumnId(1), ColumnId(2)],
        ))
        .expect("build compaction crash projection");
    database
        .execute("UPDATE events SET amount = 999 WHERE id = 1")
        .expect("create compaction crash delta");
    database
        .advance_columnar_projection(id, ColumnarAdvanceBudget::new(10, 1 << 20))
        .expect("publish compaction crash delta");
    database.close().expect("close compaction crash seed");
}

#[test]
fn compaction_catalog_registry_and_retirement_crashes_reopen_published_generation() {
    for point in [
        "compact-manifest-published",
        "compact-catalog-updated-before-registry",
        "compact-registry-swapped",
        "compact-old-retired",
    ] {
        let root = path(&format!("crash-compact-{point}"));
        seed_compaction_crash_database(&root);
        run_crash_child(&root, "compact", point);
        let mut reopened =
            Database::open_catalog(root.join("catalog")).expect("reopen compaction crash");
        let inspection = &reopened.inspect_columnar_projections()[0];
        assert_eq!(
            inspection.generation,
            Some(netbadb_types::ColumnarGeneration(2)),
            "crash point {point}"
        );
        assert_eq!(inspection.delta_segment_count, Some(0));
        assert_eq!(inspection.health, ColumnarProjectionHealth::Fresh);
        assert_eq!(
            reopened
                .query("SELECT id FROM events WHERE amount > 500")
                .expect("query compacted generation")
                .rows,
            vec![vec![ScalarValue::Int64(1)]],
            "crash point {point}"
        );
        reopened.close().expect("close compaction crash database");
        fs::remove_dir_all(root).expect("remove compaction crash fixture");
    }
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
        ("build-intent-durable", 2),
        ("build-files-synced", 2),
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

    for operation in ["build", "incremental-build"] {
        let root = path(&format!("crash-{operation}-manifest-published"));
        seed_recovered_planner_database(&root, operation == "incremental-build");
        let point = if operation == "incremental-build" {
            "incremental-build-manifest-published"
        } else {
            "build-manifest-published"
        };
        run_crash_child(&root, operation, point);
        let mut reopened = Database::open_catalog(root.join("catalog"))
            .expect("promote manifest-published pending build");
        let inventory = reopened.inspect_columnar_projections();
        assert_eq!(inventory.len(), 1);
        assert_eq!(
            inventory[0].projection_id,
            Some(netbadb_types::ColumnarProjectionId(1))
        );
        assert_eq!(
            inventory[0].mode,
            Some(operation.strip_suffix("-build").unwrap_or("snapshot"))
        );
        assert_eq!(
            reopened
                .inspect_columnar_projection_catalog()
                .next_projection_id,
            Some(netbadb_types::ColumnarProjectionId(2))
        );
        assert!(statement_uses_columnar(
            &reopened,
            "SELECT id FROM events WHERE amount > 0"
        ));
        if operation == "incremental-build" {
            let retention = reopened
                .observe_change_stream_reclamation(TableId(1))
                .expect("inspect recovered retention authority");
            assert!(retention.consumers.iter().any(|consumer| matches!(
                consumer,
                crate::ChangeStreamRetentionConsumer::Columnar {
                    projection_id: netbadb_types::ColumnarProjectionId(1),
                    ..
                }
            )));
            reopened
                .execute("UPDATE events SET amount = 500 WHERE id = 1")
                .expect("write after incremental recovery");
            let advanced = reopened
                .advance_columnar_projection(
                    netbadb_types::ColumnarProjectionId(1),
                    ColumnarAdvanceBudget::new(8, 1 << 20),
                )
                .expect("advance recovered incremental projection");
            assert!(advanced.caught_up);
        }
        drop(reopened);
        let reopened_again = Database::open_catalog(root.join("catalog"))
            .expect("repeat recovered projection reopen");
        assert_eq!(reopened_again.inspect_columnar_projections().len(), 1);
        drop(reopened_again);
        fs::remove_dir_all(root).expect("remove manifest recovery fixture");
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

    for point in ["catalog-mid-write", "catalog-after-rename"] {
        let root = path(&format!("crash-build-active-transition-{point}"));
        seed_crash_database(&root, false);
        run_crash_child(&root, "build", "build-manifest-published");
        run_crash_child(&root, "recover", point);
        let reopened = Database::open_catalog(root.join("catalog"))
            .expect("finish interrupted pending-to-active recovery");
        assert_eq!(reopened.inspect_columnar_projections().len(), 1);
        assert_eq!(
            reopened.inspect_columnar_projections()[0].projection_id,
            Some(netbadb_types::ColumnarProjectionId(1))
        );
        drop(reopened);
        let reopened_again = Database::open_catalog(root.join("catalog"))
            .expect("repeat active-transition recovery reopen");
        assert_eq!(reopened_again.inspect_columnar_projections().len(), 1);
        drop(reopened_again);
        fs::remove_dir_all(root).expect("remove active-transition fixture");
    }

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
fn managed_source_change_aborts_pending_build_and_burns_its_identity() {
    let root = path("managed-source-change");
    fs::create_dir_all(&root).expect("create managed source-change root");
    let mut database = Database::create_catalog(
        root.join("catalog"),
        vec![TableStorageCreateSpec::heap(root.join("heap"), table())],
        None,
    )
    .expect("create managed database");
    insert_rows(&mut database, 4);
    let failed = root.join("failed-projection");
    let result = database.build_columnar_projection_with(
        ColumnarProjectionSpec::new(TableId(1), &failed, vec![ColumnId(1), ColumnId(2)]),
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
    assert_eq!(
        database
            .inspect_columnar_projection_catalog()
            .next_projection_id,
        Some(netbadb_types::ColumnarProjectionId(2))
    );
    assert_eq!(
        fs::read_dir(&failed)
            .expect("retained caller directory")
            .count(),
        0
    );
    let next = database
        .build_columnar_projection(ColumnarProjectionSpec::new(
            TableId(1),
            root.join("next-projection"),
            vec![ColumnId(1)],
        ))
        .expect("build after clean abort");
    assert_eq!(next, netbadb_types::ColumnarProjectionId(2));
    database.close().expect("close managed database");
    fs::remove_dir_all(root).expect("remove managed source-change fixture");
}

#[test]
fn v1_managed_inventory_migrates_without_rewriting_projection_files_or_adopting_orphans() {
    let root = path("v1-managed-migration");
    fs::create_dir_all(&root).expect("create v1 migration root");
    let catalog = root.join("catalog");
    let active_directory = root.join("active-projection");
    let mut database = Database::create_catalog(
        &catalog,
        vec![TableStorageCreateSpec::heap(root.join("heap"), table())],
        None,
    )
    .expect("create migration database");
    insert_rows(&mut database, 8);
    let active = database
        .build_columnar_projection(ColumnarProjectionSpec::new(
            TableId(1),
            &active_directory,
            vec![ColumnId(1), ColumnId(2)],
        ))
        .expect("build active projection");
    let burned = database
        .build_columnar_projection(ColumnarProjectionSpec::new(
            TableId(1),
            root.join("burned-projection"),
            vec![ColumnId(1)],
        ))
        .expect("build projection to drop");
    database
        .drop_columnar_projection(burned)
        .expect("drop while preserving high-water gap");
    database.close().expect("close v2 database");

    let before = fs::read_dir(&active_directory)
        .expect("read active projection files")
        .map(|entry| {
            let path = entry.expect("projection file").path();
            let bytes = fs::read(&path).expect("read projection file bytes");
            (path.file_name().unwrap().to_owned(), bytes)
        })
        .collect::<Vec<_>>();
    let orphan = detached_artifact(vec![ColumnId(1)], false);
    let orphan_destination = root.join("historical-orphan");
    fs::rename(orphan, &orphan_destination).expect("install historical orphan");
    crate::projection_catalog::write_v1_fixture_for_test(&catalog)
        .expect("rewrite historical v1 fixture");

    let reopened = Database::open_catalog(&catalog).expect("migrate managed v1 inventory");
    let inventory = reopened.inspect_columnar_projections();
    assert_eq!(inventory.len(), 1);
    assert_eq!(inventory[0].projection_id, Some(active));
    assert_eq!(
        reopened
            .inspect_columnar_projection_catalog()
            .next_projection_id,
        Some(netbadb_types::ColumnarProjectionId(3))
    );
    assert!(orphan_destination.join("projection.nbcmanifest").is_file());
    drop(reopened);
    for (name, bytes) in before {
        assert_eq!(
            fs::read(active_directory.join(name)).expect("read migrated projection file"),
            bytes
        );
    }
    let catalog_bytes =
        fs::read(crate::projection_catalog::catalog_path(&catalog)).expect("read migrated catalog");
    assert_eq!(
        u16::from_le_bytes(catalog_bytes[4..6].try_into().unwrap()),
        2
    );
    fs::remove_dir_all(root).expect("remove v1 migration fixture");
}

fn detached_artifact(columns: Vec<ColumnId>, incremental: bool) -> PathBuf {
    let heap = path("detached-artifact-heap");
    let projection = path("detached-artifact-projection");
    let mut database = Database::create(&heap, table()).expect("create detached artifact source");
    insert_rows(&mut database, 4);
    if incremental {
        database
            .enable_change_stream(TableId(1))
            .expect("enable detached artifact stream");
        database
            .build_incremental_columnar_projection(ColumnarProjectionSpec::new(
                TableId(1),
                &projection,
                columns,
            ))
            .expect("build detached incremental artifact");
    } else {
        database
            .build_columnar_projection(ColumnarProjectionSpec::new(
                TableId(1),
                &projection,
                columns,
            ))
            .expect("build detached snapshot artifact");
    }
    database.close().expect("close detached artifact source");
    cleanup_created_table_files(&[heap]);
    projection
}

#[test]
fn pending_recovery_fails_closed_for_wrong_columns_and_mode() {
    for (name, intent_incremental, columns, artifact_incremental) in [
        ("columns", false, vec![ColumnId(1), ColumnId(3)], false),
        ("column-order", false, vec![ColumnId(2), ColumnId(1)], false),
        (
            "snapshot-intent-incremental-artifact",
            false,
            vec![ColumnId(1), ColumnId(2)],
            true,
        ),
        (
            "incremental-intent-snapshot-artifact",
            true,
            vec![ColumnId(1), ColumnId(2)],
            false,
        ),
    ] {
        let root = path(&format!("pending-mismatch-{name}"));
        if intent_incremental {
            seed_incremental_build_crash_database(&root);
            run_crash_child(&root, "incremental-build", "build-intent-durable");
        } else {
            seed_crash_database(&root, false);
            run_crash_child(&root, "build", "build-intent-durable");
        }
        let artifact = detached_artifact(columns, artifact_incremental);
        let destination = root.join("crashing-projection");
        fs::rename(&artifact, &destination).expect("install mismatched final artifact");
        assert!(matches!(
            Database::open_catalog(root.join("catalog")),
            Err(DatabaseError::ProjectionCatalog(
                ProjectionCatalogError::PendingBuildMismatch(_)
            ))
        ));
        assert!(destination.join("projection.nbcmanifest").is_file());
        fs::remove_dir_all(root).expect("remove mismatch fixture");
    }
}

#[test]
fn pending_recovery_fails_closed_for_corrupt_manifest_and_missing_segment() {
    for missing_segment in [false, true] {
        let root = path(if missing_segment {
            "pending-missing-segment"
        } else {
            "pending-corrupt-manifest"
        });
        seed_crash_database(&root, false);
        run_crash_child(&root, "build", "build-intent-durable");
        let artifact = detached_artifact(vec![ColumnId(1), ColumnId(2)], false);
        let destination = root.join("crashing-projection");
        fs::rename(&artifact, &destination).expect("install final artifact");
        if missing_segment {
            fs::remove_file(destination.join("projection-1-g1.nbcs"))
                .expect("remove referenced segment");
        } else {
            fs::write(destination.join("projection.nbcmanifest"), b"NBCM")
                .expect("corrupt final manifest");
        }
        assert!(matches!(
            Database::open_catalog(root.join("catalog")),
            Err(DatabaseError::ProjectionCatalog(
                ProjectionCatalogError::PendingBuildCorrupt(_)
            ))
        ));
        assert!(destination.join("projection.nbcmanifest").is_file());
        fs::remove_dir_all(root).expect("remove corrupt pending fixture");
    }
}

#[test]
fn recovery_cleans_exact_final_segment_without_manifest_and_retains_unknown_files() {
    let root = path("pending-final-segment");
    seed_crash_database(&root, false);
    run_crash_child(&root, "build", "build-intent-durable");
    let projection = root.join("crashing-projection");
    fs::create_dir_all(&projection).expect("create pending projection directory");
    fs::write(projection.join("projection-1-g1.nbcs"), b"partial")
        .expect("write final unpublished segment");
    fs::write(projection.join("notes.txt"), b"operator-owned").expect("write unrelated file");

    let mut reopened = Database::open_catalog(root.join("catalog"))
        .expect("recover manifest-absent pending build");
    assert!(!projection.join("projection-1-g1.nbcs").exists());
    assert!(projection.join("notes.txt").is_file());
    assert!(reopened.inspect_columnar_projections().is_empty());
    let id = reopened
        .build_columnar_projection(ColumnarProjectionSpec::new(
            TableId(1),
            root.join("after-recovery"),
            vec![ColumnId(1)],
        ))
        .expect("build after pending cleanup");
    assert_eq!(id, netbadb_types::ColumnarProjectionId(2));
    reopened.close().expect("close recovered database");
    fs::remove_dir_all(root).expect("remove final-segment recovery fixture");
}

#[test]
fn ambiguous_catalog_commit_requires_reopen_and_blocks_managed_mutation_and_gc() {
    let root = path("ambiguous-catalog-commit");
    fs::create_dir_all(&root).expect("create ambiguous commit root");
    let catalog = root.join("catalog");
    let mut database = Database::create_catalog(
        &catalog,
        vec![TableStorageCreateSpec::heap(root.join("heap"), table())],
        None,
    )
    .expect("create managed database");
    insert_rows(&mut database, 8);
    database
        .enable_change_stream(TableId(1))
        .expect("enable stream");
    let active = database
        .build_incremental_columnar_projection(ColumnarProjectionSpec::new(
            TableId(1),
            root.join("active-projection"),
            vec![ColumnId(1), ColumnId(2)],
        ))
        .expect("build active incremental projection");
    let catalog_shadow = crate::schema_catalog_file::suffix(
        &crate::projection_catalog::catalog_path(&catalog),
        ".next",
    );
    let failed = root.join("pending-projection");
    let result = database.build_columnar_projection_with(
        ColumnarProjectionSpec::new(TableId(1), &failed, vec![ColumnId(1)]),
        |_| {
            fs::create_dir(&catalog_shadow).expect("block catalog shadow publication");
            Ok(())
        },
    );
    assert!(matches!(
        result,
        Err(DatabaseError::ProjectionCatalog(
            ProjectionCatalogError::RecoveryRequired { .. }
        ))
    ));
    assert!(failed.join("projection.nbcmanifest").is_file());
    assert!(!database.inspect_columnar_projection_catalog().available);
    assert!(matches!(
        database.drop_columnar_projection(active),
        Err(DatabaseError::ProjectionCatalog(
            ProjectionCatalogError::RecoveryRequired { .. }
        ))
    ));
    assert!(matches!(
        database.refresh_columnar_projection(active),
        Err(DatabaseError::ProjectionCatalog(
            ProjectionCatalogError::RecoveryRequired { .. }
        ))
    ));
    assert!(matches!(
        database.advance_columnar_projection(active, ColumnarAdvanceBudget::new(1, 1024)),
        Err(DatabaseError::ProjectionCatalog(
            ProjectionCatalogError::RecoveryRequired { .. }
        ))
    ));
    assert!(matches!(
        database.compact_columnar_projection(active),
        Err(DatabaseError::ProjectionCatalog(
            ProjectionCatalogError::RecoveryRequired { .. }
        ))
    ));
    assert!(matches!(
        database.gc_change_stream(TableId(1)),
        Err(DatabaseError::ProjectionCatalog(
            ProjectionCatalogError::RecoveryRequired { .. }
        ))
    ));
    fs::remove_dir(&catalog_shadow).expect("unblock catalog publication");
    drop(database);

    let reopened = Database::open_catalog(&catalog).expect("recover durable artifact");
    let inventory = reopened.inspect_columnar_projections();
    assert_eq!(inventory.len(), 2);
    assert_eq!(
        inventory[1].projection_id,
        Some(netbadb_types::ColumnarProjectionId(2))
    );
    drop(reopened);
    let reopened_again = Database::open_catalog(&catalog).expect("repeat recovery reopen");
    assert_eq!(reopened_again.inspect_columnar_projections().len(), 2);
    drop(reopened_again);
    fs::remove_dir_all(root).expect("remove ambiguous commit fixture");
}

#[test]
fn ambiguous_artifact_publish_requires_reopen_and_aborts_when_manifest_is_absent() {
    let root = path("ambiguous-artifact-publish");
    fs::create_dir_all(&root).expect("create ambiguous artifact root");
    let catalog = root.join("catalog");
    let mut database = Database::create_catalog(
        &catalog,
        vec![TableStorageCreateSpec::heap(root.join("heap"), table())],
        None,
    )
    .expect("create managed database");
    insert_rows(&mut database, 4);
    let projection = root.join("projection");
    let final_segment = projection.join("projection-1-g1.nbcs");
    let result = database.build_columnar_projection_with(
        ColumnarProjectionSpec::new(TableId(1), &projection, vec![ColumnId(1)]),
        |_| {
            fs::create_dir(&final_segment).expect("block final segment rename");
            Ok(())
        },
    );
    assert!(matches!(
        result,
        Err(DatabaseError::ProjectionCatalog(
            ProjectionCatalogError::RecoveryRequired { .. }
        ))
    ));
    assert!(matches!(
        database.build_columnar_projection(ColumnarProjectionSpec::new(
            TableId(1),
            root.join("blocked"),
            vec![ColumnId(1)],
        )),
        Err(DatabaseError::ProjectionCatalog(
            ProjectionCatalogError::RecoveryRequired { .. }
        ))
    ));
    fs::remove_dir(&final_segment).expect("remove rename blocker");
    drop(database);

    let mut reopened = Database::open_catalog(&catalog).expect("abort absent-manifest build");
    assert!(reopened.inspect_columnar_projections().is_empty());
    let id = reopened
        .build_columnar_projection(ColumnarProjectionSpec::new(
            TableId(1),
            root.join("after-reopen"),
            vec![ColumnId(1)],
        ))
        .expect("build after reopen");
    assert_eq!(id, netbadb_types::ColumnarProjectionId(2));
    reopened.close().expect("close recovered database");
    fs::remove_dir_all(root).expect("remove ambiguous artifact fixture");
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

#[test]
fn global_snapshot_with_pending_complete_keeps_old_rr_on_authoritative_history() {
    let root = path("global-snapshot-eligibility");
    let catalog = root.join("catalog");
    let heap = root.join("heap");
    let coordinator = root.join("coordinator");
    let projection = root.join("projection");
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(&root).expect("create root");
    let mut database = Database::create_catalog(
        &catalog,
        vec![TableStorageCreateSpec::heap(&heap, table())],
        Some(DatabaseCoordinatorConfig::new(&coordinator).with_global_visibility()),
    )
    .expect("create global columnar database");
    let mut seed = database.begin_transaction().expect("begin seed");
    for id in 0..512_i64 {
        database
            .insert_in(
                &mut seed,
                &[
                    ScalarValue::Int64(id),
                    ScalarValue::Int64(id),
                    ScalarValue::Bool(id % 2 == 0),
                    ScalarValue::Text(format!("row-{id}")),
                ],
            )
            .expect("seed row");
    }
    seed.commit().expect("publish seed G1");
    let schema_generation = database.schema_generation();
    let published_commit_seq = database
        .inspect_global_visibility()
        .expect("inspect G before projection build")
        .published_commit_seq;
    let projection_id = database
        .build_columnar_projection(ColumnarProjectionSpec::new(
            TableId(1),
            &projection,
            vec![ColumnId(1), ColumnId(2)],
        ))
        .expect("build G1 projection");
    assert_eq!(database.schema_generation(), schema_generation);
    assert_eq!(
        database
            .inspect_global_visibility()
            .expect("inspect G after projection build")
            .published_commit_seq,
        published_commit_seq
    );

    let mut old = database
        .begin_transaction_with_isolation(IsolationLevel::RepeatableRead)
        .expect("begin old RR");
    database
        .execute_in(&mut old, "SELECT id FROM events WHERE id = 0")
        .expect("pin RR at G1");

    let mut writer = database.begin_transaction().expect("begin G2 writer");
    database
        .insert_in(
            &mut writer,
            &[
                ScalarValue::Int64(999),
                ScalarValue::Int64(999),
                ScalarValue::Bool(true),
                ScalarValue::Text("new".into()),
            ],
        )
        .expect("write G2 row");
    database
        .coordinator
        .as_ref()
        .expect("coordinator")
        .borrow_mut()
        .inject_complete_sync_failure();
    writer
        .commit()
        .expect("publish G2 before its Complete checkpoint sync");
    database
        .refresh_columnar_projection(projection_id)
        .expect("refresh at published G2");
    assert!(database.flush().is_err(), "Complete checkpoint sync fails");
    database.flush().expect("retry Complete checkpoint sync");

    let (latest, statistics) = database
        .query_with_columnar_statistics("SELECT id FROM events WHERE id >= 0")
        .expect("latest columnar query");
    assert_eq!(latest.rows.len(), 513);
    assert_eq!(statistics.projection_id, Some(projection_id));
    let ExecutionResult::Query(old_result) = database
        .execute_in(&mut old, "SELECT id FROM events WHERE id >= 0")
        .expect("old RR authoritative query")
    else {
        panic!("query result expected");
    };
    assert_eq!(old_result.rows.len(), 512);
    old.rollback().expect("finish old RR");
    database.close().expect("close global columnar database");
    let _ = fs::remove_dir_all(root);
}
