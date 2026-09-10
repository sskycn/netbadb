use std::error::Error;
use std::path::Path;
use std::time::Instant;

use netbadb_core::{Database, DatabaseCoordinatorConfig, GroupPrepareMode, TableStorageCreateSpec};
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

fn mode_name(mode: GroupPrepareMode) -> &'static str {
    match mode {
        GroupPrepareMode::DurablePerMember => "durable-per-member",
        GroupPrepareMode::BatchedBarrier => "batched-barrier",
    }
}

fn run(
    engine: &str,
    transactions: usize,
    group_size: usize,
    mode: GroupPrepareMode,
    change_stream: bool,
) -> Result<(), Box<dyn Error>> {
    let root = std::env::temp_dir().join(format!(
        "netbadb-phase3e-{engine}-{}-{transactions}-{group_size}-{change_stream}-{}",
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
    if change_stream {
        if engine != "lsm" {
            database.enable_change_stream(TableId(1))?;
        }
        if engine != "heap" {
            database.enable_change_stream(TableId(2))?;
        }
    }

    let started = Instant::now();
    let mut next = 0_usize;
    let mut groups = 0_usize;
    while next < transactions {
        let mut group = database.begin_group_commit()?;
        let end = transactions.min(next + group_size);
        for number in next..end {
            let mut member = database.begin_group_member(&group)?;
            let value = [ScalarValue::Int64(number as i64)];
            if engine != "lsm" {
                database.insert_into_in(TableId(1), &mut member, &value)?;
            }
            if engine != "heap" {
                database.insert_into_in(TableId(2), &mut member, &value)?;
            }
            match mode {
                GroupPrepareMode::DurablePerMember => {
                    database.park_group_member(&mut group, member)?;
                }
                GroupPrepareMode::BatchedBarrier => {
                    database.stage_group_member(&mut group, member)?;
                }
            }
        }
        let report = database.commit_group(&mut group)?;
        assert_eq!(report.prepare_mode, mode);
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
    let get = |runtime: &Option<netbadb_core::PreparedRuntimeInspection>,
               field: fn(&netbadb_core::PreparedRuntimeInspection) -> u64| {
        runtime.as_ref().map_or(0, field)
    };
    let heap_member_prepare = get(&heap_runtime, |value| value.prepare_sync_count);
    let heap_group_prepare = get(&heap_runtime, |value| {
        value.group_prepare_barrier_sync_count
    });
    let heap_group_commit = get(&heap_runtime, |value| value.group_commit_barrier_sync_count);
    let lsm_member_prepare = get(&lsm_runtime, |value| value.prepare_sync_count);
    let lsm_group_prepare = get(&lsm_runtime, |value| value.group_prepare_barrier_sync_count);
    let lsm_group_commit = get(&lsm_runtime, |value| value.group_commit_barrier_sync_count);
    let nbcl_syncs = get(&heap_runtime, |value| value.change_stream_sync_count)
        .saturating_add(get(&lsm_runtime, |value| value.change_stream_sync_count));
    assert_eq!(global.group_decision_sync_count, groups as u64);
    let storage_count = if engine == "heap+lsm" { 2 } else { 1 };
    match mode {
        GroupPrepareMode::DurablePerMember => {
            assert_eq!(
                heap_member_prepare + lsm_member_prepare,
                transactions as u64 * storage_count
            );
            assert_eq!(heap_group_prepare + lsm_group_prepare, 0);
        }
        GroupPrepareMode::BatchedBarrier => {
            assert_eq!(heap_member_prepare + lsm_member_prepare, 0);
            assert_eq!(
                heap_group_prepare + lsm_group_prepare,
                groups as u64 * storage_count
            );
        }
    }
    assert_eq!(
        heap_group_commit + lsm_group_commit,
        groups as u64 * storage_count
    );
    if change_stream {
        assert_eq!(nbcl_syncs, transactions as u64 * 2 * storage_count);
    } else {
        assert_eq!(nbcl_syncs, 0);
    }
    assert_eq!(
        global.published_commit_seq.map(|sequence| sequence.0),
        Some(transactions as u64)
    );
    println!(
        "{engine},{},{transactions},{group_size},{groups},{change_stream},{heap_member_prepare},{heap_group_prepare},{heap_group_commit},{lsm_member_prepare},{lsm_group_prepare},{lsm_group_commit},{},{nbcl_syncs},{},{},{},{},{},{}",
        mode_name(mode),
        global.group_decision_sync_count,
        elapsed.as_nanos(),
        elapsed.as_nanos() / transactions as u128,
        global.published_commit_seq.map_or(0, |sequence| sequence.0),
        global.coordinator_bytes,
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
    );
    database.close()?;
    let _ = std::fs::remove_dir_all(root);
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    println!(
        "engine,prepare_mode,transactions,group_size,groups,change_stream,heap_member_prepare_syncs,heap_group_prepare_barrier_syncs,heap_group_commit_barrier_syncs,lsm_member_prepare_syncs,lsm_group_prepare_barrier_syncs,lsm_group_commit_barrier_syncs,coordinator_group_syncs,nbcl_syncs,total_elapsed_ns,mean_transaction_ns,published_g,coordinator_bytes,heap_wal_bytes,lsm_wal_bytes"
    );
    for engine in ["heap", "lsm", "heap+lsm"] {
        for transactions in [100, 1_000] {
            for group_size in [1, 4, 8, 10, 16, 32] {
                for mode in [
                    GroupPrepareMode::DurablePerMember,
                    GroupPrepareMode::BatchedBarrier,
                ] {
                    run(engine, transactions, group_size, mode, false)?;
                }
            }
        }
    }
    run("heap", 100, 10, GroupPrepareMode::BatchedBarrier, true)?;
    Ok(())
}
