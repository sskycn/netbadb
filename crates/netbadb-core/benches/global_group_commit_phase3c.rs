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

fn run(engine: &str, transactions: usize, group_size: usize) -> Result<(), Box<dyn Error>> {
    let root = std::env::temp_dir().join(format!(
        "netbadb-phase3c-{engine}-{transactions}-{group_size}-{}",
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
    let inspection = database.inspect_global_visibility()?;
    let heap_wal_bytes = if engine == "lsm" {
        0
    } else {
        file_bytes(&netbadb_storage::wal_path(&heap))
    };
    let lsm_wal_bytes = if engine == "heap" {
        0
    } else {
        lsm_wal_bytes(&lsm)
    };
    println!(
        "{engine},{transactions},{group_size},{groups},{},{},{:.3},{},{},{},{},{},{},{}",
        inspection.group_decision_sync_count,
        inspection.decision_sync_count,
        transactions as f64 / inspection.group_decision_sync_count.max(1) as f64,
        elapsed.as_nanos(),
        elapsed.as_nanos() / transactions as u128,
        inspection.coordinator_bytes,
        inspection
            .published_commit_seq
            .map_or(0, |sequence| sequence.0),
        heap_wal_bytes,
        lsm_wal_bytes,
        inspection.combined_group_pipeline_sync_count,
    );
    database.close()?;
    let _ = std::fs::remove_dir_all(root);
    Ok(())
}

fn run_conflict(engine: &str) -> Result<(), Box<dyn Error>> {
    let root = std::env::temp_dir().join(format!(
        "netbadb-phase3c-conflict-{engine}-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root)?;
    let heap = root.join("heap.db");
    let lsm = root.join("lsm");
    let coordinator = root.join("coordinator.nbco");
    let (spec, table_id, table_name) = match engine {
        "heap" => (
            TableStorageCreateSpec::heap(&heap, table(1, "heap_items")),
            TableId(1),
            "heap_items",
        ),
        "lsm" => (
            TableStorageCreateSpec::lsm(&lsm, table(2, "lsm_items"), ColumnId(1)),
            TableId(2),
            "lsm_items",
        ),
        _ => return Err("unknown conflict benchmark engine".into()),
    };
    let mut database = Database::create_storages_with_coordinator(
        vec![spec],
        DatabaseCoordinatorConfig::new(&coordinator).with_global_visibility(),
    )?;
    database.execute(&format!("INSERT INTO {table_name} (id) VALUES (1)"))?;

    let mut group = database.begin_group_commit()?;
    let mut first = database.begin_group_member(&group)?;
    database.execute_in(
        &mut first,
        &format!("UPDATE {table_name} SET id = 2 WHERE id = 1"),
    )?;
    database.park_group_member(&mut group, first)?;
    let mut second = database.begin_group_member(&group)?;
    let conflict = database
        .execute_in(
            &mut second,
            &format!("UPDATE {table_name} SET id = 3 WHERE id = 1"),
        )
        .is_err();
    drop(second);
    let report = database.commit_group(&mut group)?;
    let correct = database
        .query(&format!("SELECT id FROM {table_name}"))?
        .rows
        == vec![vec![ScalarValue::Int64(2)]];
    println!(
        "conflict,{engine},{},{},{},{conflict},{correct}",
        table_id.0, report.member_count, report.last_commit_seq.0
    );
    drop(group);
    database.close()?;
    let _ = std::fs::remove_dir_all(root);
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    println!(
        "engine,transactions,group_size,groups,coordinator_group_syncs,coordinator_single_syncs,transactions_per_coordinator_sync,total_elapsed_ns,mean_per_transaction_ns,coordinator_bytes,published_g,heap_wal_bytes,lsm_wal_bytes,combined_prior_complete_group_syncs"
    );
    for engine in ["heap", "lsm", "heap+lsm"] {
        for transactions in [100, 1_000] {
            for group_size in [1, 4, 8, 10, 16, 32] {
                run(engine, transactions, group_size)?;
            }
        }
    }
    println!(
        "scenario,engine,table_id,committed_members,published_g,conflict_detected,database_correct"
    );
    for engine in ["heap", "lsm"] {
        run_conflict(engine)?;
    }
    Ok(())
}
