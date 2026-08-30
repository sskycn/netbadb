#![no_main]

use libfuzzer_sys::fuzz_target;
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_storage::{HeapStorage, wal_alternate_path, wal_path};
use netbadb_types::{ColumnId, PhysicalType, TableId};

const MAX_INPUT_SIZE: usize = 64 * 1024;

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT_SIZE {
        return;
    }

    let database_path =
        std::env::temp_dir().join(format!("netbadb-wal-recovery-fuzz-{}", std::process::id()));
    cleanup(&database_path);
    let table = fuzz_table();
    let storage = HeapStorage::create(&database_path, table.clone())
        .expect("create isolated WAL fuzz fixture");
    drop(storage);

    let wal_file = wal_path(&database_path);
    if std::fs::write(&wal_file, data).is_ok() {
        // HeapStorage::open invokes the crate-private recovery WAL decoder,
        // including its partial-final-record path, through a production API.
        if let Ok(mut storage) = HeapStorage::open(&database_path, table) {
            let _ = storage.inspect_index_reclaim();
        }
    }
    cleanup(&database_path);
});

fn fuzz_table() -> TableDef {
    TableDef::new(
        TableId(1),
        "fuzz_rows",
        vec![
            ColumnDef::new(ColumnId(1), "id", TypeSpec::Physical(PhysicalType::UInt64))
                .primary_key(true),
        ],
    )
}

fn cleanup(database_path: &std::path::Path) {
    let wal_file = wal_path(database_path);
    let _ = std::fs::remove_file(wal_alternate_path(&wal_file));
    let _ = std::fs::remove_file(wal_file);
    let _ = std::fs::remove_file(database_path);
    let mut status_path = database_path.as_os_str().to_os_string();
    status_path.push("-txn-status");
    let _ = std::fs::remove_file(status_path);
}
