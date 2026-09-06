use std::error::Error;
use std::time::Instant;

use netbadb_core::{
    ColumnarProjectionSpec, Database, MaintenanceAction, MaintenanceBudget, MaintenanceOutcome,
    TableStorageCreateSpec,
};
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_types::{ColumnId, PhysicalType, ScalarValue, TableId};

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

fn action_name(action: MaintenanceAction) -> &'static str {
    match action {
        MaintenanceAction::AdvanceColumnar { .. } => "advance-columnar",
        MaintenanceAction::CompactColumnar { .. } => "compact-columnar",
        MaintenanceAction::GcChangeStream { .. } => "gc-change-stream",
        MaintenanceAction::FlushLsm { .. } => "flush-lsm",
        MaintenanceAction::CompactLsm { .. } => "compact-lsm",
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let rows = std::env::var("NETBADB_MAINTENANCE_ROWS")
        .unwrap_or_else(|_| "1000".into())
        .parse::<i64>()?;
    let mutations = std::env::var("NETBADB_MAINTENANCE_MUTATIONS")
        .unwrap_or_else(|_| "20".into())
        .parse::<i64>()?;
    let batches_per_step = std::env::var("NETBADB_MAINTENANCE_BATCHES_PER_STEP")
        .unwrap_or_else(|_| "5".into())
        .parse::<u64>()?;
    let root = std::env::temp_dir().join(format!(
        "netbadb-maintenance-phase2e-{rows}-{mutations}-{}",
        std::process::id()
    ));
    let catalog = root.join("catalog");
    let heap = root.join("heap");
    let projection = root.join("projection");
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root)?;
    let mut database = Database::create_catalog(
        &catalog,
        vec![TableStorageCreateSpec::heap(&heap, table())],
        None,
    )?;
    for id in 0..rows {
        database.insert(&[ScalarValue::Int64(id), ScalarValue::Int64(id)])?;
    }
    database.enable_change_stream(TableId(1))?;
    database.build_incremental_columnar_projection(
        ColumnarProjectionSpec::new(TableId(1), &projection, vec![ColumnId(1), ColumnId(2)])
            .with_row_group_rows(4096),
    )?;
    let manifest_before_dml = std::fs::read(projection.join("projection.nbcmanifest"))?;
    let nbcl_before_dml = database.inspect_change_stream(TableId(1))?.file_bytes;
    for id in 0..mutations.min(rows) {
        database.execute(&format!(
            "UPDATE events SET amount = {} WHERE id = {id}",
            rows + id
        ))?;
    }
    let foreground_manifest_unchanged =
        std::fs::read(projection.join("projection.nbcmanifest"))? == manifest_before_dml;
    let nbcl_after_dml = database.inspect_change_stream(TableId(1))?.file_bytes;
    println!(
        "scenario,step,action,frontier_before,frontier_after,work,read_bytes,write_bytes,elapsed_ns,detail"
    );
    println!(
        "foreground,0,dml-only,0,0,0,{nbcl_before_dml},{nbcl_after_dml},0,projection_unchanged={foreground_manifest_unchanged}"
    );

    let tiny = MaintenanceBudget::new(u64::MAX, u64::MAX, 0, 1);
    let tiny_inspection = database.inspect_maintenance(tiny)?;
    println!(
        "budget-too-small,0,{},0,0,0,0,0,0,selected={} candidates={}",
        tiny_inspection
            .candidates
            .iter()
            .find_map(|candidate| matches!(
                candidate.action,
                MaintenanceAction::CompactColumnar { .. }
            )
            .then_some("compact-columnar"))
            .unwrap_or("none"),
        tiny_inspection.decision.is_some(),
        tiny_inspection.candidates.len()
    );

    let step_budget = MaintenanceBudget::new(batches_per_step, 1 << 30, 1 << 30, 1);
    for step in 1..=mutations.saturating_add(8) {
        let before = database
            .inspect_columnar_projections()
            .first()
            .and_then(|projection| projection.applied_frontier)
            .map_or(0, |frontier| frontier.0);
        let started = Instant::now();
        let report = database.maintenance_step(step_budget)?;
        let elapsed = started.elapsed().as_nanos();
        let after = database
            .inspect_columnar_projections()
            .first()
            .and_then(|projection| projection.applied_frontier)
            .map_or(0, |frontier| frontier.0);
        let action = report
            .decision
            .map_or("none", |decision| action_name(decision.action));
        println!(
            "bounded,{step},{action},{before},{after},{},{},{},{elapsed},more={}",
            report.consumed.work_units,
            report.consumed.read_bytes,
            report.consumed.write_bytes,
            report.more_work_remaining
        );
        if matches!(report.outcome, MaintenanceOutcome::NoWork) {
            break;
        }
    }
    let compact_tiny = database.inspect_maintenance(MaintenanceBudget::new(
        rows.saturating_add(mutations) as u64,
        1,
        1,
        1,
    ))?;
    let compact_blocker = compact_tiny
        .candidates
        .iter()
        .find(|candidate| matches!(candidate.action, MaintenanceAction::CompactColumnar { .. }))
        .and_then(|candidate| candidate.blocker);
    println!(
        "compact-budget,0,compact-columnar,0,0,0,1,1,0,selected={} blocker={compact_blocker:?}",
        compact_tiny.decision.is_some()
    );
    let compact_budget = MaintenanceBudget::new(
        rows.saturating_add(mutations).saturating_add(1) as u64,
        1 << 30,
        1 << 30,
        1,
    );
    let started = Instant::now();
    let compact = database.maintenance_step(compact_budget)?;
    println!(
        "compact,0,{},0,0,{},{},{},{},more={}",
        compact
            .decision
            .map_or("none", |decision| action_name(decision.action)),
        compact.consumed.work_units,
        compact.consumed.read_bytes,
        compact.consumed.write_bytes,
        started.elapsed().as_nanos(),
        compact.more_work_remaining
    );
    let final_stream = database.inspect_change_stream(TableId(1))?;
    let final_projection = database.inspect_columnar_projections().remove(0);
    println!(
        "final,0,settled,{},{},0,{},{},0,health={:?} delta_segments={} earliest={}",
        final_projection.applied_frontier.map_or(0, |value| value.0),
        final_stream.current_data_version.0,
        nbcl_after_dml,
        final_stream.file_bytes,
        final_projection.health,
        final_projection.delta_segment_count.unwrap_or(0),
        final_stream
            .earliest_available_frontier
            .map_or(0, |value| value.0)
    );
    database.close()?;
    let _ = std::fs::remove_dir_all(root);
    Ok(())
}
