#![no_main]

use libfuzzer_sys::fuzz_target;
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_storage::{HeapStorage, wal_alternate_path, wal_path};
use netbadb_types::{ColumnId, PhysicalType, TableId};

// Retained no-checkpoint transition histories include both complete heap and WAL.
const MAX_INPUT_SIZE: usize = 256 * 1024;

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT_SIZE {
        return;
    }

    // NBRF is a fuzz-fixture envelope, not a database format. It supplies a
    // checkpointed file plus selected WAL so intent old/new length paths are
    // actually reachable; ordinary seeds remain raw WAL bytes.
    let fixture = if data.starts_with(b"NBRF") {
        if data.len() < 12 {
            return;
        }
        let heap_length = u32::from_le_bytes(data[4..8].try_into().unwrap()) as usize;
        let wal_length = u32::from_le_bytes(data[8..12].try_into().unwrap()) as usize;
        let Some(end) = 12_usize.checked_add(heap_length) else {
            return;
        };
        if end.checked_add(wal_length) != Some(data.len()) {
            return;
        }
        Some((&data[12..end], &data[end..]))
    } else {
        None
    };
    let database_path =
        std::env::temp_dir().join(format!("netbadb-wal-recovery-fuzz-{}", std::process::id()));
    cleanup(&database_path);
    let table = fuzz_table();
    let storage = HeapStorage::create(&database_path, table.clone())
        .expect("create isolated WAL fuzz fixture");
    drop(storage);

    let wal_file = wal_path(&database_path);
    let wal_bytes = if let Some((heap, wal)) = fixture {
        std::fs::write(&database_path, heap).expect("install isolated heap fixture");
        wal
    } else {
        data
    };
    if std::fs::write(&wal_file, wal_bytes).is_ok() {
        // HeapStorage::open invokes the crate-private recovery WAL decoder,
        // including its partial-final-record path, through a production API.
        if let Ok(mut storage) = HeapStorage::open(&database_path, table) {
            // Corrupt dormant pages can be rejected by the lazy full-file scan
            // even when registry open succeeds. A successful scan must agree
            // with candidate generation/owner classification, not merely open.
            if let Ok(ownership) = storage.inspect_index_reclaim() {
                let reuse = storage
                    .inspect_reusable_pages()
                    .expect("validated candidate scan");
                assert_eq!(reuse.file_pages, ownership.database_pages);
                assert_eq!(reuse.pending_owners, ownership.pending_reclaim_indexes);
                assert!(
                    reuse
                        .candidates
                        .windows(2)
                        .all(|pair| pair[0].page_ref.page_id < pair[1].page_ref.page_id)
                );
                for candidate in reuse.candidates {
                    assert!(candidate.page_ref.generation.0 > 0);
                    assert!(candidate.retired_index_id < ownership.next_index_id);
                    assert!(
                        ownership
                            .allocations
                            .iter()
                            .any(|page| page.page_ref == candidate.page_ref
                                && page.owner == candidate.retired_index_id)
                    );
                }
                for pending in ownership.pending {
                    assert!(pending.index_id.0 > 0 && pending.index_id < ownership.next_index_id);
                }
            }
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
