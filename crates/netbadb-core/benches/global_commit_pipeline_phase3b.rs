use std::error::Error;
use std::path::Path;
use std::time::Instant;

use netbadb_core::{Database, DatabaseCoordinatorConfig, TableStorageCreateSpec};
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_types::{ColumnId, PhysicalType, ScalarValue, TableId};

fn heap_table() -> TableDef {
    TableDef::new(
        TableId(1),
        "heap_items",
        vec![
            ColumnDef::new(ColumnId(1), "id", TypeSpec::Physical(PhysicalType::Int64)),
            ColumnDef::new(
                ColumnId(2),
                "payload",
                TypeSpec::Physical(PhysicalType::Text),
            ),
        ],
    )
}

fn lsm_table() -> TableDef {
    TableDef::new(
        TableId(2),
        "lsm_items",
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

fn root(scenario: &str, engine: &str, size: i64) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "netbadb-phase3b-{scenario}-{engine}-{size}-{}",
        std::process::id()
    ))
}

fn values(engine: &str, id: i64) -> Vec<ScalarValue> {
    if engine == "heap" {
        vec![ScalarValue::Int64(id), ScalarValue::Text("v".into())]
    } else {
        vec![ScalarValue::Int64(id)]
    }
}

fn run_engine(
    scenario: &str,
    engine: &str,
    transactions: i64,
    rows_per_tx: i64,
) -> Result<(), Box<dyn Error>> {
    let root = root(scenario, engine, transactions * rows_per_tx);
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root)?;
    let heap = root.join("heap.db");
    let lsm = root.join("lsm");
    let coordinator = root.join("coordinator.nbco");
    let (spec, table_id) = if engine == "heap" {
        (
            TableStorageCreateSpec::heap(&heap, heap_table()),
            TableId(1),
        )
    } else {
        (
            TableStorageCreateSpec::lsm(&lsm, lsm_table(), ColumnId(1)),
            TableId(2),
        )
    };
    let mut database = Database::create_storages_with_coordinator(
        vec![spec],
        DatabaseCoordinatorConfig::new(&coordinator).with_global_visibility(),
    )?;

    let started = Instant::now();
    for transaction_number in 0..transactions {
        let mut transaction = database.begin_transaction_for(table_id)?;
        for row in 0..rows_per_tx {
            let id = transaction_number * rows_per_tx + row;
            database.insert_into_in(table_id, &mut transaction, &values(engine, id))?;
        }
        transaction.commit()?;
    }
    let foreground_ns = started.elapsed().as_nanos();
    let foreground = database.inspect_global_visibility()?;
    let close_checkpoint_syncs = u64::from(foreground.pending_complete_count != 0);
    let authoritative_wal_bytes = if engine == "heap" {
        file_bytes(&netbadb_storage::wal_path(&heap))
    } else {
        lsm_wal_bytes(&lsm)
    };
    database.close()?;
    println!(
        "{scenario},{engine},{transactions},{rows_per_tx},{},{foreground_ns},{},{},{close_checkpoint_syncs},{},{},{}",
        foreground_ns / transactions as u128,
        foreground.coordinator_bytes,
        foreground.decision_sync_count,
        foreground.combined_pipeline_sync_count,
        foreground
            .published_commit_seq
            .map_or(0, |sequence| sequence.0),
        authoritative_wal_bytes,
    );
    let _ = std::fs::remove_dir_all(root);
    Ok(())
}

fn run_cross_storage() -> Result<(), Box<dyn Error>> {
    let root = root("single", "heap+lsm", 200);
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root)?;
    let heap = root.join("heap.db");
    let lsm = root.join("lsm");
    let coordinator = root.join("coordinator.nbco");
    let mut database = Database::create_storages_with_coordinator(
        vec![
            TableStorageCreateSpec::heap(&heap, heap_table()),
            TableStorageCreateSpec::lsm(&lsm, lsm_table(), ColumnId(1)),
        ],
        DatabaseCoordinatorConfig::new(&coordinator).with_global_visibility(),
    )?;
    let started = Instant::now();
    let mut transaction = database.begin_transaction()?;
    for id in 0..100_i64 {
        database.insert_into_in(TableId(1), &mut transaction, &values("heap", id))?;
        database.insert_into_in(TableId(2), &mut transaction, &values("lsm", id))?;
    }
    transaction.commit()?;
    let foreground_ns = started.elapsed().as_nanos();
    let foreground = database.inspect_global_visibility()?;
    let close_checkpoint_syncs = u64::from(foreground.pending_complete_count != 0);
    let authoritative_wal_bytes =
        file_bytes(&netbadb_storage::wal_path(&heap)) + lsm_wal_bytes(&lsm);
    database.close()?;
    println!(
        "single,heap+lsm,1,200,{foreground_ns},{foreground_ns},{},{},{},{},{},{}",
        foreground.coordinator_bytes,
        foreground.decision_sync_count,
        close_checkpoint_syncs,
        foreground.combined_pipeline_sync_count,
        foreground
            .published_commit_seq
            .map_or(0, |sequence| sequence.0),
        authoritative_wal_bytes,
    );
    let _ = std::fs::remove_dir_all(root);
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    println!(
        "scenario,engine,transactions,rows_per_tx,mean_commit_ns,total_commit_ns,coordinator_bytes,decision_syncs,close_checkpoint_syncs,combined_pipeline_syncs,published_g,authoritative_wal_bytes"
    );
    for engine in ["heap", "lsm"] {
        for rows in [1, 100, 1_000] {
            run_engine("single", engine, 1, rows)?;
        }
        for transactions in [10, 100, 1_000] {
            run_engine("sequential", engine, transactions, 1)?;
        }
    }
    run_cross_storage()?;
    Ok(())
}
