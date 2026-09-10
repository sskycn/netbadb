use std::error::Error;
use std::path::Path;
use std::time::Instant;

use netbadb_core::{
    Database, DatabaseCoordinatorConfig, ExtendedGroupCommitOptions,
    GroupChangeStreamDurabilityMode, GroupChangeStreamFinalizeMode, GroupCommitOptions,
    GroupPrepareMode, PreparedRuntimeInspection, TableStorageCreateSpec,
};
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_types::{ColumnId, PhysicalType, ScalarValue, TableId};

fn table(id: u64, name: &str) -> TableDef {
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

fn file_bytes(path: &Path) -> u64 {
    std::fs::metadata(path).map_or(0, |metadata| metadata.len())
}

fn lsm_wal_bytes(root: &Path) -> u64 {
    std::fs::read_dir(root).map_or(0, |entries| {
        entries
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().ends_with(".nblw"))
            .map(|entry| file_bytes(&entry.path()))
            .sum()
    })
}

fn mode_name(mode: GroupChangeStreamFinalizeMode) -> &'static str {
    match mode {
        GroupChangeStreamFinalizeMode::ImmediateBarrier => "immediate-barrier",
        GroupChangeStreamFinalizeMode::PipelinedCheckpoint => "pipelined-checkpoint",
    }
}

fn total(
    heap: &Option<PreparedRuntimeInspection>,
    lsm: &Option<PreparedRuntimeInspection>,
    field: fn(&PreparedRuntimeInspection) -> u64,
) -> u64 {
    heap.as_ref().map_or(0, field) + lsm.as_ref().map_or(0, field)
}

fn run(
    engine: &str,
    transactions: usize,
    group_size: usize,
    finalize_mode: GroupChangeStreamFinalizeMode,
) -> Result<(), Box<dyn Error>> {
    let root = std::env::temp_dir().join(format!(
        "netbadb-phase3g-{engine}-{}-{transactions}-{group_size}-{}",
        mode_name(finalize_mode),
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root)?;
    let heap = root.join("heap.db");
    let lsm = root.join("lsm");
    let coordinator = root.join("coordinator.nbco");
    let specs = match engine {
        "heap" => vec![TableStorageCreateSpec::heap(&heap, table(1, "heap_items"))],
        "lsm" => vec![TableStorageCreateSpec::lsm(
            &lsm,
            table(2, "lsm_items"),
            ColumnId(1),
        )],
        "heap+lsm" => vec![
            TableStorageCreateSpec::heap(&heap, table(1, "heap_items")),
            TableStorageCreateSpec::lsm(&lsm, table(2, "lsm_items"), ColumnId(1)),
        ],
        _ => return Err("unknown benchmark engine".into()),
    };
    let mut database = Database::create_storages_with_coordinator(
        specs,
        DatabaseCoordinatorConfig::new(&coordinator).with_global_visibility(),
    )?;
    let heap_cursor = (engine != "lsm")
        .then(|| database.enable_change_stream(TableId(1)))
        .transpose()?;
    let lsm_cursor = (engine != "heap")
        .then(|| database.enable_change_stream(TableId(2)))
        .transpose()?;

    let started = Instant::now();
    let mut next = 0_usize;
    let mut groups = 0_usize;
    while next < transactions {
        let mut group =
            database.begin_group_commit_with_extended_options(ExtendedGroupCommitOptions {
                group: GroupCommitOptions {
                    prepare_mode: GroupPrepareMode::BatchedBarrier,
                    change_stream_durability_mode: GroupChangeStreamDurabilityMode::BatchedBarriers,
                },
                change_stream_finalize_mode: finalize_mode,
            })?;
        let end = transactions.min(next + group_size);
        for number in next..end {
            let mut member = database.begin_group_member(&group)?;
            let row = [ScalarValue::Int64(number as i64)];
            if engine != "lsm" {
                database.insert_into_in(TableId(1), &mut member, &row)?;
            }
            if engine != "heap" {
                database.insert_into_in(TableId(2), &mut member, &row)?;
            }
            database.stage_group_member(&mut group, member)?;
        }
        let report = database.commit_group(&mut group)?;
        assert_eq!(report.change_stream_finalize_mode, finalize_mode);
        groups += 1;
        next = end;
    }
    let elapsed = started.elapsed();
    let global = database.inspect_global_visibility()?;
    let heap_runtime = (engine != "lsm")
        .then(|| database.inspect_prepared_runtime(TableId(1)))
        .transpose()?;
    let lsm_runtime = (engine != "heap")
        .then(|| database.inspect_prepared_runtime(TableId(2)))
        .transpose()?;
    let storage_count = if engine == "heap+lsm" { 2_u64 } else { 1 };
    let authoritative_prepare = total(&heap_runtime, &lsm_runtime, |runtime| {
        runtime.group_prepare_barrier_sync_count
    });
    let authoritative_commit = total(&heap_runtime, &lsm_runtime, |runtime| {
        runtime.group_commit_barrier_sync_count
    });
    let nbcl_prepare = total(&heap_runtime, &lsm_runtime, |runtime| {
        runtime.change_stream_group_prepare_barrier_sync_count
    });
    let immediate_finalize = total(&heap_runtime, &lsm_runtime, |runtime| {
        runtime.change_stream_group_finalize_barrier_sync_count
    });
    let combined = total(&heap_runtime, &lsm_runtime, |runtime| {
        runtime.change_stream_combined_finalize_prepare_sync_count
    });
    let foreground_total = total(&heap_runtime, &lsm_runtime, |runtime| {
        runtime.change_stream_sync_count
    });
    assert_eq!(authoritative_prepare, groups as u64 * storage_count);
    assert_eq!(authoritative_commit, groups as u64 * storage_count);
    assert_eq!(global.group_decision_sync_count, groups as u64);
    assert_eq!(nbcl_prepare, groups as u64 * storage_count);
    match finalize_mode {
        GroupChangeStreamFinalizeMode::ImmediateBarrier => {
            assert_eq!(immediate_finalize, groups as u64 * storage_count);
            assert_eq!(combined, 0);
            assert_eq!(foreground_total, groups as u64 * storage_count * 2);
        }
        GroupChangeStreamFinalizeMode::PipelinedCheckpoint => {
            assert_eq!(immediate_finalize, 0);
            assert_eq!(combined, groups.saturating_sub(1) as u64 * storage_count);
            assert_eq!(foreground_total, groups as u64 * storage_count);
        }
    }
    assert_eq!(
        global.published_commit_seq.map(|sequence| sequence.0),
        Some(transactions as u64)
    );
    let heap_changes = heap_cursor
        .map(|cursor| database.read_changes(TableId(1), cursor, usize::MAX, u64::MAX))
        .transpose()?;
    let lsm_changes = lsm_cursor
        .map(|cursor| database.read_changes(TableId(2), cursor, usize::MAX, u64::MAX))
        .transpose()?;
    if let Some(changes) = &heap_changes {
        assert_eq!(changes.batches.len(), transactions);
    }
    if let Some(changes) = &lsm_changes {
        assert_eq!(changes.batches.len(), transactions);
    }
    let heap_frontier = heap_changes.map_or(0, |changes| changes.current_frontier.0);
    let lsm_frontier = lsm_changes.map_or(0, |changes| changes.current_frontier.0);
    assert!(engine == "lsm" || heap_frontier == transactions as u64);
    assert!(engine == "heap" || lsm_frontier == transactions as u64);
    let heap_checkpointed_before_flush = if engine == "lsm" {
        0
    } else {
        database
            .inspect_change_stream(TableId(1))?
            .finalize_checkpointed_through
            .map_or(0, |frontier| frontier.0)
    };
    let lsm_checkpointed_before_flush = if engine == "heap" {
        0
    } else {
        database
            .inspect_change_stream(TableId(2))?
            .finalize_checkpointed_through
            .map_or(0, |frontier| frontier.0)
    };
    let close_checkpoint_syncs = database.flush_change_stream_checkpoints()?;
    let heap_after = (engine != "lsm")
        .then(|| database.inspect_prepared_runtime(TableId(1)))
        .transpose()?;
    let lsm_after = (engine != "heap")
        .then(|| database.inspect_prepared_runtime(TableId(2)))
        .transpose()?;
    let explicit_checkpoints = total(&heap_after, &lsm_after, |runtime| {
        runtime.change_stream_explicit_finalize_checkpoint_sync_count
    });
    let total_including_close = total(&heap_after, &lsm_after, |runtime| {
        runtime.change_stream_sync_count
    });
    match finalize_mode {
        GroupChangeStreamFinalizeMode::ImmediateBarrier => {
            assert_eq!(close_checkpoint_syncs, 0);
            assert_eq!(explicit_checkpoints, 0);
            assert!(engine == "lsm" || heap_checkpointed_before_flush == transactions as u64);
            assert!(engine == "heap" || lsm_checkpointed_before_flush == transactions as u64);
        }
        GroupChangeStreamFinalizeMode::PipelinedCheckpoint => {
            assert_eq!(close_checkpoint_syncs, storage_count);
            assert_eq!(explicit_checkpoints, storage_count);
            assert!(engine == "lsm" || heap_checkpointed_before_flush < transactions as u64);
            assert!(engine == "heap" || lsm_checkpointed_before_flush < transactions as u64);
        }
    }
    assert_eq!(
        total_including_close,
        foreground_total + close_checkpoint_syncs
    );
    if engine == "heap"
        && transactions == 100
        && group_size == 10
        && finalize_mode == GroupChangeStreamFinalizeMode::PipelinedCheckpoint
    {
        assert_eq!(groups, 10);
        assert_eq!(foreground_total, 10);
        assert_eq!(immediate_finalize, 0);
        assert!(close_checkpoint_syncs <= 1);
    }
    let nbcl_bytes = if engine == "lsm" {
        file_bytes(&netbadb_storage::lsm_change_log_path(&lsm))
    } else if engine == "heap" {
        file_bytes(&netbadb_storage::heap_change_log_path(&heap))
    } else {
        file_bytes(&netbadb_storage::heap_change_log_path(&heap))
            + file_bytes(&netbadb_storage::lsm_change_log_path(&lsm))
    };
    println!(
        "{engine},{},{transactions},{group_size},{groups},{nbcl_prepare},{immediate_finalize},{combined},{explicit_checkpoints},{foreground_total},{total_including_close},{authoritative_prepare},{},{authoritative_commit},{},{},{nbcl_bytes},{},{},{},{},{heap_frontier},{lsm_frontier},{heap_checkpointed_before_flush},{lsm_checkpointed_before_flush}",
        mode_name(finalize_mode),
        global.group_decision_sync_count,
        elapsed.as_nanos(),
        elapsed.as_nanos() / transactions as u128,
        if engine == "lsm" {
            0
        } else {
            file_bytes(&netbadb_storage::wal_path(&heap))
        },
        if engine == "heap" {
            0
        } else {
            lsm_wal_bytes(&lsm)
        },
        global.coordinator_bytes,
        global.published_commit_seq.map_or(0, |sequence| sequence.0),
    );
    database.close()?;
    let _ = std::fs::remove_dir_all(root);
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    println!(
        "engine,finalize_mode,transactions,group_size,groups,nbcl_prepare_syncs,nbcl_immediate_finalize_syncs,nbcl_combined_prior_finalize_prepare_syncs,nbcl_explicit_close_checkpoint_syncs,nbcl_total_foreground_syncs,nbcl_total_syncs_including_close,authoritative_prepare_barriers,coordinator_group_syncs,authoritative_commit_barriers,total_elapsed_ns,mean_transaction_ns,nbcl_bytes,heap_wal_bytes,lsm_wal_bytes,coordinator_bytes,published_g,heap_frontier,lsm_frontier,heap_checkpointed_frontier_before_close,lsm_checkpointed_frontier_before_close"
    );
    for engine in ["heap", "lsm", "heap+lsm"] {
        for transactions in [100, 1_000] {
            for group_size in [1, 4, 8, 10, 16, 32] {
                for finalize_mode in [
                    GroupChangeStreamFinalizeMode::ImmediateBarrier,
                    GroupChangeStreamFinalizeMode::PipelinedCheckpoint,
                ] {
                    run(engine, transactions, group_size, finalize_mode)?;
                }
            }
        }
    }
    Ok(())
}
