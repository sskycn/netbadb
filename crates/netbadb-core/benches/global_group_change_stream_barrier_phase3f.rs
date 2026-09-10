use std::error::Error;
use std::path::Path;
use std::time::Instant;

use netbadb_core::{
    Database, DatabaseCoordinatorConfig, GroupChangeStreamDurabilityMode, GroupCommitOptions,
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

fn mode_name(mode: GroupChangeStreamDurabilityMode) -> &'static str {
    match mode {
        GroupChangeStreamDurabilityMode::PerMember => "per-member",
        GroupChangeStreamDurabilityMode::BatchedBarriers => "batched-barriers",
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
    mode: GroupChangeStreamDurabilityMode,
) -> Result<(), Box<dyn Error>> {
    let root = std::env::temp_dir().join(format!(
        "netbadb-phase3f-{engine}-{}-{transactions}-{group_size}-{}",
        mode_name(mode),
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
        let mut group = database.begin_group_commit_with_options(GroupCommitOptions {
            prepare_mode: GroupPrepareMode::BatchedBarrier,
            change_stream_durability_mode: mode,
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
        assert_eq!(report.change_stream_durability_mode, mode);
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
    let storage_count = if engine == "heap+lsm" { 2 } else { 1 };
    let authoritative_prepare = total(&heap_runtime, &lsm_runtime, |runtime| {
        runtime.group_prepare_barrier_sync_count
    });
    let authoritative_commit = total(&heap_runtime, &lsm_runtime, |runtime| {
        runtime.group_commit_barrier_sync_count
    });
    let member_prepare = total(&heap_runtime, &lsm_runtime, |runtime| {
        runtime.change_stream_member_prepare_sync_count
    });
    let group_prepare = total(&heap_runtime, &lsm_runtime, |runtime| {
        runtime.change_stream_group_prepare_barrier_sync_count
    });
    let member_finalize = total(&heap_runtime, &lsm_runtime, |runtime| {
        runtime.change_stream_member_finalize_sync_count
    });
    let group_finalize = total(&heap_runtime, &lsm_runtime, |runtime| {
        runtime.change_stream_group_finalize_barrier_sync_count
    });
    let nbcl_total = total(&heap_runtime, &lsm_runtime, |runtime| {
        runtime.change_stream_sync_count
    });
    assert_eq!(authoritative_prepare, groups as u64 * storage_count);
    assert_eq!(authoritative_commit, groups as u64 * storage_count);
    assert_eq!(global.group_decision_sync_count, groups as u64);
    match mode {
        GroupChangeStreamDurabilityMode::PerMember => {
            assert_eq!(member_prepare, transactions as u64 * storage_count);
            assert_eq!(member_finalize, transactions as u64 * storage_count);
            assert_eq!(group_prepare, 0);
            assert_eq!(group_finalize, 0);
        }
        GroupChangeStreamDurabilityMode::BatchedBarriers => {
            assert_eq!(member_prepare, 0);
            assert_eq!(member_finalize, 0);
            assert_eq!(group_prepare, groups as u64 * storage_count);
            assert_eq!(group_finalize, groups as u64 * storage_count);
        }
    }
    assert_eq!(
        nbcl_total,
        member_prepare + group_prepare + member_finalize + group_finalize
    );
    if engine == "heap"
        && transactions == 100
        && group_size == 10
        && mode == GroupChangeStreamDurabilityMode::BatchedBarriers
    {
        assert_eq!((group_prepare, group_finalize, nbcl_total), (10, 10, 20));
    }
    assert_eq!(
        global.published_commit_seq.map(|sequence| sequence.0),
        Some(transactions as u64)
    );
    let heap_frontier = heap_cursor
        .map(|cursor| database.read_changes(TableId(1), cursor, usize::MAX, u64::MAX))
        .transpose()?
        .map_or(0, |changes| changes.current_frontier.0);
    let lsm_frontier = lsm_cursor
        .map(|cursor| database.read_changes(TableId(2), cursor, usize::MAX, u64::MAX))
        .transpose()?
        .map_or(0, |changes| changes.current_frontier.0);
    let nbcl_bytes = if engine == "lsm" {
        file_bytes(&netbadb_storage::lsm_change_log_path(&lsm))
    } else if engine == "heap" {
        file_bytes(&netbadb_storage::heap_change_log_path(&heap))
    } else {
        file_bytes(&netbadb_storage::heap_change_log_path(&heap))
            + file_bytes(&netbadb_storage::lsm_change_log_path(&lsm))
    };
    println!(
        "{engine},{},{transactions},{group_size},{groups},{authoritative_prepare},{},{authoritative_commit},{member_prepare},{group_prepare},{member_finalize},{group_finalize},{nbcl_total},{},{},{nbcl_bytes},{},{},{},{},{heap_frontier},{lsm_frontier}",
        mode_name(mode),
        global.group_decision_sync_count,
        elapsed.as_nanos(),
        elapsed.as_nanos() / transactions as u128,
        global.published_commit_seq.map_or(0, |sequence| sequence.0),
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
    );
    database.close()?;
    let _ = std::fs::remove_dir_all(root);
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    println!(
        "engine,change_stream_mode,transactions,group_size,groups,authoritative_prepare_barrier_syncs,coordinator_group_syncs,authoritative_commit_barrier_syncs,nbcl_member_prepare_syncs,nbcl_group_prepare_barrier_syncs,nbcl_member_finalize_syncs,nbcl_group_finalize_barrier_syncs,nbcl_total_syncs,total_elapsed_ns,mean_transaction_ns,nbcl_bytes,heap_wal_bytes,lsm_wal_bytes,coordinator_bytes,published_g,heap_frontier,lsm_frontier"
    );
    for engine in ["heap", "lsm", "heap+lsm"] {
        for transactions in [100, 1_000] {
            for group_size in [1, 4, 8, 10, 16, 32] {
                for mode in [
                    GroupChangeStreamDurabilityMode::PerMember,
                    GroupChangeStreamDurabilityMode::BatchedBarriers,
                ] {
                    run(engine, transactions, group_size, mode)?;
                }
            }
        }
    }
    Ok(())
}
