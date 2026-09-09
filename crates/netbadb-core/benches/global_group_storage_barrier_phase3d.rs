use std::error::Error;
use std::path::Path;
use std::time::Instant;

use netbadb_core::{Database, DatabaseCoordinatorConfig, TableStorageCreateSpec};
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

fn run(
    engine: &str,
    transactions: usize,
    group_size: usize,
    change_stream: bool,
) -> Result<(), Box<dyn Error>> {
    let root = std::env::temp_dir().join(format!(
        "netbadb-phase3d-{engine}-{transactions}-{group_size}-{change_stream}-{}",
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
            database.park_group_member(&mut group, member)?;
        }
        database.commit_group(&mut group)?;
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
    let heap_prepare = heap_runtime
        .as_ref()
        .map_or(0, |value| value.prepare_sync_count);
    let heap_single = heap_runtime
        .as_ref()
        .map_or(0, |value| value.single_commit_sync_count);
    let heap_group = heap_runtime
        .as_ref()
        .map_or(0, |value| value.group_commit_barrier_sync_count);
    let lsm_prepare = lsm_runtime
        .as_ref()
        .map_or(0, |value| value.prepare_sync_count);
    let lsm_single = lsm_runtime
        .as_ref()
        .map_or(0, |value| value.single_commit_sync_count);
    let lsm_group = lsm_runtime
        .as_ref()
        .map_or(0, |value| value.group_commit_barrier_sync_count);
    let nbcl_syncs = heap_runtime
        .as_ref()
        .map_or(0, |value| value.change_stream_sync_count)
        .saturating_add(
            lsm_runtime
                .as_ref()
                .map_or(0, |value| value.change_stream_sync_count),
        );
    let storage_commit_syncs = heap_group.saturating_add(lsm_group);
    assert_eq!(global.group_decision_sync_count, groups as u64);
    assert_eq!(
        global.published_commit_seq.map(|sequence| sequence.0),
        Some(transactions as u64)
    );
    if engine != "lsm" {
        assert_eq!(heap_prepare, transactions as u64);
        assert_eq!(heap_single, 0);
        assert_eq!(heap_group, groups as u64);
        assert_eq!(
            database.query("SELECT id FROM heap_items")?.rows.len(),
            transactions
        );
    }
    if engine != "heap" {
        assert_eq!(lsm_prepare, transactions as u64);
        assert_eq!(lsm_single, 0);
        assert_eq!(lsm_group, groups as u64);
        assert_eq!(
            database.query("SELECT id FROM lsm_items")?.rows.len(),
            transactions
        );
    }
    if change_stream {
        let storage_count = u64::from(engine == "heap+lsm") + 1;
        assert_eq!(nbcl_syncs, transactions as u64 * 2 * storage_count);
    } else {
        assert_eq!(nbcl_syncs, 0);
    }
    println!(
        "{engine},{transactions},{group_size},{groups},{change_stream},{},{heap_prepare},{heap_single},{heap_group},{lsm_prepare},{lsm_single},{lsm_group},{nbcl_syncs},{:.3},{},{},{},{},{},{}",
        global.group_decision_sync_count,
        transactions as f64 / storage_commit_syncs.max(1) as f64,
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
        "engine,transactions,group_size,groups,change_stream,coordinator_group_syncs,heap_prepare_syncs,heap_single_commit_syncs,heap_group_commit_barrier_syncs,lsm_prepare_syncs,lsm_single_commit_syncs,lsm_group_commit_barrier_syncs,nbcl_syncs,transactions_per_storage_commit_sync,total_elapsed_ns,mean_transaction_ns,published_g,coordinator_bytes,heap_wal_bytes,lsm_wal_bytes"
    );
    for engine in ["heap", "lsm", "heap+lsm"] {
        for transactions in [100, 1_000] {
            for group_size in [1, 4, 8, 10, 16, 32] {
                run(engine, transactions, group_size, false)?;
            }
        }
    }
    run("heap", 100, 10, true)?;
    Ok(())
}
