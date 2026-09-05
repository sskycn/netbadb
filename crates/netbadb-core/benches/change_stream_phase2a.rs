use std::error::Error;
use std::time::Instant;

use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_storage::{TableStorage, heap_change_log_path, lsm_change_log_path, wal_path};
use netbadb_types::{ColumnId, PhysicalType, ScalarValue, StorageId, TableId};

fn table() -> TableDef {
    TableDef::new(
        TableId(1),
        "change_bench",
        vec![
            ColumnDef::new(ColumnId(1), "key", TypeSpec::Physical(PhysicalType::Int64)),
            ColumnDef::new(ColumnId(2), "value", TypeSpec::Physical(PhysicalType::Text)),
        ],
    )
}

fn row(index: usize, phase: &str) -> Vec<ScalarValue> {
    vec![
        ScalarValue::Int64(index as i64),
        ScalarValue::Text(format!("{phase}-{index:08}")),
    ]
}

fn file_bytes(path: &std::path::Path) -> u64 {
    std::fs::metadata(path).map_or(0, |metadata| metadata.len())
}

fn run_case(engine: &str, enabled: bool, rows: usize) -> Result<(), Box<dyn Error>> {
    let root = std::env::temp_dir().join(format!(
        "netbadb-change-bench-{engine}-{enabled}-{rows}-{}",
        std::process::id()
    ));
    let heap_path = root.with_extension("db");
    let mut storage = if engine == "heap" {
        TableStorage::create_heap_with_storage_id(&heap_path, table(), StorageId(1))?
    } else {
        let _ = std::fs::remove_dir_all(&root);
        TableStorage::create_lsm_with_storage_id(&root, table(), ColumnId(1), StorageId(1))?
    };
    if enabled {
        storage.enable_change_stream()?;
    }

    let started = Instant::now();
    if rows == 1 {
        storage.insert(&row(0, "insert"))?;
    } else {
        let mut transaction = storage.begin_transaction()?;
        for index in 0..rows {
            storage.insert_in(&mut transaction, &row(index, "insert"))?;
        }
        transaction.commit()?;
    }
    let insert_elapsed = started.elapsed();

    let view = storage.read_view()?;
    let handles = storage
        .scan_columns_with_view(&[ColumnId(1)], &view)?
        .into_iter()
        .map(|(handle, _)| handle)
        .collect::<Vec<_>>();
    drop(view);
    let started = Instant::now();
    let mut transaction = storage.begin_transaction()?;
    for (index, handle) in handles.into_iter().enumerate() {
        storage.update_in(&mut transaction, handle, &row(index, "update"))?;
    }
    transaction.commit()?;
    let update_elapsed = started.elapsed();

    let view = storage.read_view()?;
    let handles = storage
        .scan_columns_with_view(&[ColumnId(1)], &view)?
        .into_iter()
        .map(|(handle, _)| handle)
        .collect::<Vec<_>>();
    drop(view);
    let started = Instant::now();
    let mut transaction = storage.begin_transaction()?;
    for handle in handles {
        storage.delete_in(&mut transaction, handle)?;
    }
    transaction.commit()?;
    let delete_elapsed = started.elapsed();

    let inspection = storage.inspect_change_stream();
    let change_path = if engine == "heap" {
        heap_change_log_path(&heap_path)
    } else {
        lsm_change_log_path(&root)
    };
    let authoritative_wal_bytes = if engine == "heap" {
        file_bytes(&wal_path(&heap_path))
    } else {
        std::fs::read_dir(&root)?
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().starts_with("wal-"))
            .map(|entry| file_bytes(&entry.path()))
            .sum()
    };
    let changing_transactions = 3_u64;
    println!(
        "{engine},{enabled},{rows},{},{},{},{},{},{},{},{}",
        insert_elapsed.as_nanos(),
        update_elapsed.as_nanos(),
        delete_elapsed.as_nanos(),
        inspection.committed_batch_count,
        inspection.committed_mutation_count,
        file_bytes(&change_path),
        authoritative_wal_bytes,
        if enabled {
            changing_transactions * 2
        } else {
            0
        }
    );
    storage.close()?;
    if engine == "heap" {
        for component in netbadb_storage::heap_resource_components(&heap_path) {
            let _ = std::fs::remove_file(component.path);
        }
    } else {
        let _ = std::fs::remove_dir_all(root);
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    println!(
        "engine,enabled,rows,insert_ns,update_ns,delete_ns,batches,mutations,change_bytes,wal_bytes,change_syncs"
    );
    for engine in ["heap", "lsm"] {
        for enabled in [false, true] {
            for rows in [1, 100, 1_000] {
                run_case(engine, enabled, rows)?;
            }
        }
    }
    Ok(())
}
