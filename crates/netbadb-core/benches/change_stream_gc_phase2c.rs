use std::error::Error;
use std::time::Instant;

use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_storage::{ChangeStreamCursor, TableStorage};
use netbadb_types::{ColumnId, PhysicalType, ScalarValue, StorageId, TableId};

fn table() -> TableDef {
    TableDef::new(
        TableId(1),
        "events",
        vec![ColumnDef::new(
            ColumnId(1),
            "id",
            TypeSpec::Physical(PhysicalType::Int64),
        )],
    )
}

fn main() -> Result<(), Box<dyn Error>> {
    println!(
        "mutations,batch_rows,batches_before,earliest_before,current,gc_frontier,gc_ns,bytes_before,bytes_after,bytes_reclaimed,batches_retained,reopen_current,next_commit_before,next_commit_after,next_sequence"
    );
    for mutations in [10_000_usize, 100_000] {
        let path = std::env::temp_dir().join(format!(
            "netbadb-change-gc-bench-{mutations}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&path);
        let mut storage =
            TableStorage::create_lsm_with_storage_id(&path, table(), ColumnId(1), StorageId(1))?;
        let origin = storage.enable_change_stream()?;
        for start in (0..mutations).step_by(1_000) {
            let mut transaction = storage.begin_transaction()?;
            for index in start..(start + 1_000).min(mutations) {
                storage.insert_in(&mut transaction, &[ScalarValue::Int64(index as i64)])?;
            }
            transaction.commit()?;
        }
        let before = storage.inspect_change_stream();
        let current = before.current_data_version;
        let gc_frontier = netbadb_types::StorageDataVersion(current.0 / 2);
        let started = Instant::now();
        let gc = storage.gc_change_stream(gc_frontier)?;
        let gc_ns = started.elapsed().as_nanos();
        let retained = storage.inspect_change_stream().committed_batch_count;
        storage.close()?;
        let mut reopened = TableStorage::open_lsm(&path, table())?;
        let reopen_current = reopened.inspect_change_stream().current_data_version;
        reopened.insert(&[ScalarValue::Int64(mutations as i64)])?;
        let cursor = ChangeStreamCursor {
            frontier: current,
            ..origin
        };
        let next = reopened.read_changes(cursor, 1, 1_000_000)?;
        let batch = &next.batches[0];
        println!(
            "{mutations},1000,{},{},{},{},{gc_ns},{},{},{},{retained},{},{},{},{}",
            before.committed_batch_count,
            before
                .earliest_available_frontier
                .expect("enabled earliest")
                .0,
            current.0,
            gc_frontier.0,
            gc.bytes_before,
            gc.bytes_after,
            gc.bytes_reclaimed,
            reopen_current.0,
            batch.before.0,
            batch.after.0,
            batch.sequence,
        );
        reopened.close()?;
        let _ = std::fs::remove_dir_all(&path);
    }
    Ok(())
}
