use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_types::{ColumnId, PhysicalType, ScalarValue, TableId};

use crate::{
    ColumnarAdvanceBudget, ColumnarProjectionHealth, ColumnarProjectionSpec, Database,
    MaintenanceAction, MaintenanceActionReport, MaintenanceBlocker, MaintenanceBudget,
    MaintenanceOutcome, TableStorageCreateSpec, cleanup_created_table_files,
};

static NEXT_PATH: AtomicU64 = AtomicU64::new(1);

fn root(name: &str) -> PathBuf {
    let suffix = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "netbadb-maintenance-{name}-{}-{suffix}",
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
            ),
        ],
    )
}

fn budget(work: u64) -> MaintenanceBudget {
    MaintenanceBudget::new(work, 1 << 30, 1 << 30, 1)
}

fn cleanup(root: &Path, heap: &Path) {
    cleanup_created_table_files(&[heap.to_owned()]);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn bounded_steps_advance_then_gc_then_compact_without_touching_dml() {
    let root = root("columnar-e2e");
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
    for id in 0..8 {
        database
            .insert(&[ScalarValue::Int64(id), ScalarValue::Int64(id * 10)])
            .expect("insert base row");
    }
    database
        .enable_change_stream(TableId(1))
        .expect("enable stream");
    let projection_id = database
        .build_incremental_columnar_projection(
            ColumnarProjectionSpec::new(TableId(1), &projection, vec![ColumnId(1), ColumnId(2)])
                .with_row_group_rows(4),
        )
        .expect("build projection");
    let manifest_before =
        fs::read(projection.join("projection.nbcmanifest")).expect("read manifest before DML");
    for id in 0..3 {
        database
            .execute(&format!(
                "UPDATE events SET amount = {} WHERE id = {id}",
                100 + id
            ))
            .expect("update source");
    }
    assert_eq!(
        fs::read(projection.join("projection.nbcmanifest")).expect("read manifest after DML"),
        manifest_before,
        "foreground DML must not invoke maintenance"
    );

    let mut transaction = database
        .begin_transaction()
        .expect("begin transaction that makes maintenance busy");
    let busy = database
        .inspect_maintenance(budget(1 << 20))
        .expect("inspect busy maintenance");
    assert!(busy.decision.is_none());
    assert!(busy.candidates.iter().any(|candidate| {
        matches!(candidate.action, MaintenanceAction::AdvanceColumnar { .. })
            && candidate.blocker == Some(MaintenanceBlocker::Busy)
    }));
    transaction.rollback().expect("rollback busy transaction");
    drop(transaction);

    let tiny = MaintenanceBudget::new(1, 1 << 30, 0, 1);
    let blocked = database
        .inspect_maintenance(tiny)
        .expect("inspect tiny budget");
    assert!(blocked.decision.is_none());
    assert!(blocked.candidates.iter().any(|candidate| {
        matches!(
            candidate.action,
            MaintenanceAction::AdvanceColumnar { projection_id: id, .. } if id == projection_id
        ) && candidate.blocker == Some(MaintenanceBlocker::WriteBudgetExceeded)
    }));
    assert!(matches!(
        database
            .maintenance_step(tiny)
            .expect("tiny maintenance step")
            .outcome,
        MaintenanceOutcome::NoWork
    ));

    let first = database
        .maintenance_step(budget(1))
        .expect("first bounded advance");
    assert!(matches!(
        first.decision.expect("first decision").action,
        MaintenanceAction::AdvanceColumnar {
            projection_id: id,
            max_batches: 1,
            ..
        } if id == projection_id
    ));
    assert_eq!(first.consumed.work_units, 1);
    assert!(first.more_work_remaining);
    let lagging = database.inspect_columnar_projections().remove(0);
    assert_eq!(lagging.health, ColumnarProjectionHealth::Lagging);
    assert!(lagging.delta_segment_count.unwrap_or(0) > 0);
    let dependency = database
        .inspect_maintenance(budget(1 << 20))
        .expect("inspect advance dependency");
    assert!(matches!(
        dependency.decision.expect("advance decision").action,
        MaintenanceAction::AdvanceColumnar { .. }
    ));
    assert!(dependency.candidates.iter().any(|candidate| {
        matches!(candidate.action, MaintenanceAction::CompactColumnar { .. })
            && candidate.blocker == Some(MaintenanceBlocker::ProjectionLagging)
    }));

    let generous = budget(1 << 20);
    let mut saw_gc = false;
    let mut saw_compaction = false;
    for _ in 0..16 {
        let report = database
            .maintenance_step(generous)
            .expect("drive bounded maintenance");
        match report.outcome {
            MaintenanceOutcome::NoWork => break,
            MaintenanceOutcome::Completed(MaintenanceActionReport::GcChangeStream(_)) => {
                saw_gc = true;
            }
            MaintenanceOutcome::Completed(MaintenanceActionReport::CompactColumnar(_)) => {
                saw_compaction = true;
            }
            MaintenanceOutcome::Completed(_) => {}
        }
    }
    assert!(saw_gc, "catch-up must unblock a later independent GC step");
    assert!(
        saw_compaction,
        "fresh structural Delta must compact when admitted"
    );
    let final_projection = database.inspect_columnar_projections().remove(0);
    assert_eq!(final_projection.health, ColumnarProjectionHealth::Fresh);
    assert_eq!(final_projection.delta_segment_count, Some(0));
    let stream = database
        .inspect_change_stream(TableId(1))
        .expect("inspect retained stream");
    assert_eq!(
        stream.earliest_available_frontier,
        Some(stream.current_data_version)
    );
    assert_eq!(
        database
            .query("SELECT id, amount FROM events ORDER BY id")
            .expect("query maintained state")
            .rows,
        vec![
            vec![ScalarValue::Int64(0), ScalarValue::Int64(100)],
            vec![ScalarValue::Int64(1), ScalarValue::Int64(101)],
            vec![ScalarValue::Int64(2), ScalarValue::Int64(102)],
            vec![ScalarValue::Int64(3), ScalarValue::Int64(30)],
            vec![ScalarValue::Int64(4), ScalarValue::Int64(40)],
            vec![ScalarValue::Int64(5), ScalarValue::Int64(50)],
            vec![ScalarValue::Int64(6), ScalarValue::Int64(60)],
            vec![ScalarValue::Int64(7), ScalarValue::Int64(70)],
        ]
    );
    assert!(
        database
            .inspect_maintenance(generous)
            .expect("inspect settled database")
            .decision
            .is_none()
    );
    database
        .disable_change_stream(TableId(1))
        .expect("disable stream");
    let rebuild = database
        .inspect_maintenance(generous)
        .expect("inspect rebuild-required projection");
    assert!(rebuild.candidates.iter().any(|candidate| {
        matches!(candidate.action, MaintenanceAction::AdvanceColumnar { .. })
            && candidate.blocker == Some(MaintenanceBlocker::RebuildRequired)
    }));
    database
        .enable_change_stream(TableId(1))
        .expect("enable new stream incarnation");
    database
        .projections
        .quarantine(projection_id, "injected unavailable projection".into());
    let unavailable = database
        .inspect_maintenance(generous)
        .expect("inspect unavailable projection");
    assert!(unavailable.candidates.iter().any(|candidate| {
        matches!(candidate.action, MaintenanceAction::AdvanceColumnar { .. })
            && candidate.blocker == Some(MaintenanceBlocker::Unavailable)
    }));
    database.close().expect("close database");
    cleanup(&root, &heap);
}

#[test]
fn lsm_flush_and_one_compaction_are_budget_gated_and_preserve_frontier() {
    let root = root("lsm");
    let catalog = root.join("catalog");
    let lsm = root.join("lsm");
    fs::create_dir_all(&root).expect("create root");
    let mut database = Database::create_catalog(
        &catalog,
        vec![TableStorageCreateSpec::lsm(&lsm, table(), ColumnId(1))],
        None,
    )
    .expect("create LSM database");
    database
        .enable_change_stream(TableId(1))
        .expect("enable stream");

    for id in 0..2 {
        database
            .insert(&[ScalarValue::Int64(id), ScalarValue::Int64(id)])
            .expect("insert first run");
    }
    let frontier = database
        .inspect_change_stream(TableId(1))
        .expect("inspect first frontier")
        .current_data_version;
    let too_small = MaintenanceBudget::new(1, 1, 1, 1);
    let blocked = database
        .inspect_maintenance(too_small)
        .expect("inspect blocked flush");
    assert!(blocked.decision.is_none());
    assert!(blocked.candidates.iter().any(|candidate| {
        matches!(candidate.action, MaintenanceAction::FlushLsm { .. })
            && candidate.blocker.is_some()
    }));
    let flushed = database
        .maintenance_step(budget(1 << 20))
        .expect("flush first run");
    assert!(matches!(
        flushed.outcome,
        MaintenanceOutcome::Completed(MaintenanceActionReport::FlushLsm(_))
    ));
    assert_eq!(
        database
            .inspect_change_stream(TableId(1))
            .expect("frontier after flush")
            .current_data_version,
        frontier
    );

    for id in 2..4 {
        database
            .insert(&[ScalarValue::Int64(id), ScalarValue::Int64(id)])
            .expect("insert second run");
    }
    database
        .maintenance_step(budget(1 << 20))
        .expect("flush second run");
    let before_compact = database
        .inspect_change_stream(TableId(1))
        .expect("frontier before compact")
        .current_data_version;
    let compacted = database
        .maintenance_step(budget(1 << 20))
        .expect("compact one LSM plan");
    assert!(matches!(
        compacted.outcome,
        MaintenanceOutcome::Completed(MaintenanceActionReport::CompactLsm(_))
    ));
    assert_eq!(
        database
            .inspect_change_stream(TableId(1))
            .expect("frontier after compact")
            .current_data_version,
        before_compact
    );
    assert_eq!(
        database
            .query("SELECT id, amount FROM events ORDER BY id")
            .expect("query compacted LSM")
            .rows
            .len(),
        4
    );
    database.close().expect("close LSM database");
    let _ = fs::remove_dir_all(root);
}

#[test]
fn maintenance_compaction_crash_child() {
    if std::env::var("NETBADB_MAINTENANCE_CRASH_CHILD").as_deref() != Ok("1") {
        return;
    }
    let catalog = PathBuf::from(
        std::env::var("NETBADB_MAINTENANCE_CRASH_CATALOG").expect("crash catalog path"),
    );
    let mut database = Database::open_catalog(catalog).expect("open crash database");
    database
        .maintenance_step(budget(1 << 20))
        .expect("maintenance must reach configured crash point");
    panic!("configured maintenance crash point did not terminate child");
}

#[test]
fn compaction_crash_reopens_durable_state_and_next_step_continues() {
    let root = root("compaction-crash");
    let catalog = root.join("catalog");
    let heap = root.join("heap");
    let projection = root.join("projection");
    fs::create_dir_all(&root).expect("create root");
    let mut database = Database::create_catalog(
        &catalog,
        vec![TableStorageCreateSpec::heap(&heap, table())],
        None,
    )
    .expect("create crash database");
    for id in 0..4 {
        database
            .insert(&[ScalarValue::Int64(id), ScalarValue::Int64(id)])
            .expect("insert crash row");
    }
    database
        .enable_change_stream(TableId(1))
        .expect("enable crash stream");
    let id = database
        .build_incremental_columnar_projection(ColumnarProjectionSpec::new(
            TableId(1),
            &projection,
            vec![ColumnId(1), ColumnId(2)],
        ))
        .expect("build crash projection");
    database
        .execute("UPDATE events SET amount = 99 WHERE id = 1")
        .expect("create crash delta");
    database
        .advance_columnar_projection(id, ColumnarAdvanceBudget::new(10, 1 << 20))
        .expect("publish crash delta");
    database
        .gc_change_stream(TableId(1))
        .expect("remove GC dependency");
    database.close().expect("close crash seed");

    let status = Command::new(std::env::current_exe().expect("current test executable"))
        .arg("--exact")
        .arg("maintenance_tests::maintenance_compaction_crash_child")
        .arg("--nocapture")
        .env("NETBADB_MAINTENANCE_CRASH_CHILD", "1")
        .env("NETBADB_MAINTENANCE_CRASH_CATALOG", &catalog)
        .env(
            "NETBADB_PROJECTION_CATALOG_CRASH_POINT",
            "compact-manifest-published",
        )
        .status()
        .expect("run maintenance crash child");
    assert_eq!(status.code(), Some(88));

    let mut reopened = Database::open_catalog(&catalog).expect("reopen after crash");
    let recovered = reopened.inspect_columnar_projections().remove(0);
    assert_eq!(recovered.projection_id, Some(id));
    assert_eq!(recovered.delta_segment_count, Some(0));
    assert!(matches!(
        reopened
            .maintenance_step(budget(1 << 20))
            .expect("continue maintenance after recovered compaction")
            .outcome,
        MaintenanceOutcome::NoWork
    ));
    assert_eq!(
        reopened
            .query("SELECT amount FROM events WHERE id = 1")
            .expect("query after retried compaction")
            .rows,
        vec![vec![ScalarValue::Int64(99)]]
    );
    reopened.close().expect("close recovered database");
    cleanup(&root, &heap);
}
