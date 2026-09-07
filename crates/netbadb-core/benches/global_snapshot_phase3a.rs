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

fn run_single(kind: &str, global: bool, rows: i64) -> Result<(), Box<dyn Error>> {
    let root = std::env::temp_dir().join(format!(
        "netbadb-global-snapshot-bench-{kind}-{global}-{rows}-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root)?;
    let heap = root.join("heap.db");
    let lsm = root.join("lsm");
    let coordinator = root.join("coordinator.nbco");
    let spec = if kind == "heap" {
        TableStorageCreateSpec::heap(&heap, heap_table())
    } else {
        TableStorageCreateSpec::lsm(&lsm, lsm_table(), ColumnId(1))
    };
    let mut database = if global {
        Database::create_storages_with_coordinator(
            vec![spec],
            DatabaseCoordinatorConfig::new(&coordinator).with_global_visibility(),
        )?
    } else {
        Database::create_storages(vec![spec])?
    };
    let table_id = if kind == "heap" {
        TableId(1)
    } else {
        TableId(2)
    };
    let started = Instant::now();
    if rows == 1 {
        let values = if kind == "heap" {
            vec![ScalarValue::Int64(0), ScalarValue::Text("v".into())]
        } else {
            vec![ScalarValue::Int64(0)]
        };
        database.insert_into(table_id, &values)?;
    } else {
        let mut transaction = database.begin_transaction_for(table_id)?;
        for id in 0..rows {
            let values = if kind == "heap" {
                vec![ScalarValue::Int64(id), ScalarValue::Text("v".into())]
            } else {
                vec![ScalarValue::Int64(id)]
            };
            database.insert_into_in(table_id, &mut transaction, &values)?;
        }
        transaction.commit()?;
    }
    let elapsed = started.elapsed().as_nanos();
    let sync_count = database.inspect_global_visibility()?.decision_sync_count;
    database.flush()?;
    let sequence = database
        .current_database_snapshot()?
        .map_or(0, |snapshot| snapshot.commit_seq().0);
    let coordinator_bytes = file_bytes(&coordinator);
    let wal_bytes = if kind == "heap" {
        file_bytes(&netbadb_storage::wal_path(&heap))
    } else {
        lsm_wal_bytes(&lsm)
    };
    println!(
        "single,{kind},{},{rows},{elapsed},{coordinator_bytes},{sync_count},{wal_bytes},0,{sequence}",
        if global { "global" } else { "local" }
    );
    database.close()?;
    let _ = std::fs::remove_dir_all(root);
    Ok(())
}

fn run_cross_storage() -> Result<(), Box<dyn Error>> {
    let root = std::env::temp_dir().join(format!(
        "netbadb-global-snapshot-bench-cross-{}",
        std::process::id()
    ));
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
        database.insert_into_in(
            TableId(1),
            &mut transaction,
            &[ScalarValue::Int64(id), ScalarValue::Text("v".into())],
        )?;
        database.insert_into_in(TableId(2), &mut transaction, &[ScalarValue::Int64(id)])?;
    }
    transaction.commit()?;
    let commit_elapsed = started.elapsed().as_nanos();
    let sync_count = database.inspect_global_visibility()?.decision_sync_count;
    let read_started = Instant::now();
    let rows = database
        .query("SELECT h.id FROM heap_items h JOIN lsm_items l ON h.id = l.id")?
        .rows
        .len();
    let read_elapsed = read_started.elapsed().as_nanos();
    database.flush()?;
    let sequence = database
        .current_database_snapshot()?
        .expect("global snapshot")
        .commit_seq()
        .0;
    println!(
        "cross,heap+lsm,global,200,{commit_elapsed},{},{sync_count},{},0,{sequence};join_rows={rows};join_ns={read_elapsed}",
        file_bytes(&coordinator),
        file_bytes(&netbadb_storage::wal_path(&heap)) + lsm_wal_bytes(&lsm)
    );
    database.close()?;
    let _ = std::fs::remove_dir_all(root);
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    println!(
        "scenario,engine,mode,rows,commit_ns,coordinator_bytes,coordinator_syncs,authoritative_wal_bytes,nbcl_bytes,database_commit_seq"
    );
    for kind in ["heap", "lsm"] {
        for rows in [1, 100, 1_000] {
            run_single(kind, false, rows)?;
            run_single(kind, true, rows)?;
        }
    }
    run_cross_storage()?;
    Ok(())
}
