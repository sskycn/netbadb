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
    let Ok(storage) = HeapStorage::create(&database_path, table.clone()) else {
        cleanup(&database_path);
        return;
    };
    drop(storage);

    let wal_file = wal_path(&database_path);
    if std::fs::write(&wal_file, data).is_ok() {
        // HeapStorage::open invokes the crate-private recovery WAL decoder,
        // including its partial-final-record path, through a production API.
        let _ = HeapStorage::open(&database_path, table);
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
}
