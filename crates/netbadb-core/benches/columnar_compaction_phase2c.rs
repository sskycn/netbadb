use std::error::Error;
use std::hint::black_box;
use std::time::Instant;

use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_storage::{
    ChangeBatch, ChangeStreamCursor, ColumnarProjection, StorageChange, StorageVersionKey,
    TableStorage,
};
use netbadb_types::{
    ChangeStreamGeneration, ColumnId, ColumnarGeneration, ColumnarProjectionId, PageId,
    PhysicalType, RowId, ScalarValue, StorageDataVersion, StorageId, TableId, TxnId,
};

fn table() -> TableDef {
    TableDef::new(
        TableId(1),
        "events",
        vec![
            ColumnDef::new(ColumnId(1), "id", TypeSpec::Physical(PhysicalType::Int64)),
            ColumnDef::new(
                ColumnId(2),
                "amount",
                TypeSpec::Physical(PhysicalType::Int64),
            ),
        ],
    )
}

fn key(storage_id: StorageId, row: usize, generation: u32) -> StorageVersionKey {
    StorageVersionKey::Heap {
        storage_id,
        row_id: RowId {
            page: PageId((row / 256 + 1) as u64),
            slot: (row % 256) as u16,
            generation,
        },
    }
}

fn row(row: usize, amount: i64) -> Vec<ScalarValue> {
    vec![ScalarValue::Int64(row as i64), ScalarValue::Int64(amount)]
}

fn main() -> Result<(), Box<dyn Error>> {
    let row_counts = std::env::var("NETBADB_COLUMNAR_COMPACTION_ROWS")
        .unwrap_or_else(|_| "100000,1000000".into())
        .split(',')
        .map(str::parse::<usize>)
        .collect::<Result<Vec<_>, _>>()?;
    println!(
        "base_rows,workload,delta_basis_points,delta_segments,delta_mutations,delta_bytes,suppressed,live_delta,query_before_ns,compaction_ns,query_after_ns,old_bytes,new_bytes,bytes_reclaimed,rows_after,plan_before,plan_after"
    );
    for rows in row_counts {
        for workload in ["update", "mixed"] {
            for ratio in [100_usize, 500, 1_000] {
                let root = std::env::temp_dir().join(format!(
                    "netbadb-columnar-compaction-{rows}-{workload}-{ratio}-{}",
                    std::process::id()
                ));
                let heap = root.join("anchor.db");
                let projection_path = root.join("projection");
                let _ = std::fs::remove_dir_all(&root);
                std::fs::create_dir_all(&root)?;
                let storage_id = StorageId(1);
                let storage =
                    TableStorage::create_heap_with_storage_id(&heap, table(), storage_id)?;
                let token = storage.current_snapshot_token()?;
                storage.close()?;
                let base = (0..rows)
                    .map(|index| (key(storage_id, index, 1), row(index, index as i64)))
                    .collect::<Vec<_>>();
                let projection = ColumnarProjection::prepare_incremental(
                    &projection_path,
                    ColumnarProjectionId(1),
                    ColumnarGeneration(1),
                    &table(),
                    storage_id,
                    token,
                    ChangeStreamCursor {
                        storage_id,
                        generation: ChangeStreamGeneration(1),
                        frontier: StorageDataVersion(0),
                    },
                    &[ColumnId(1), ColumnId(2)],
                    &base,
                    Some(4_096),
                )?
                .publish()?;
                let count = rows.saturating_mul(ratio) / 10_000;
                let mut mutations = (0..count)
                    .map(|index| StorageChange::Update {
                        old_version: key(storage_id, index, 1),
                        new_version: key(storage_id, index, 2),
                        after: row(index, -(index as i64)),
                    })
                    .collect::<Vec<_>>();
                if workload == "mixed" {
                    for index in 0..count / 4 {
                        mutations.push(StorageChange::Insert {
                            new_version: key(storage_id, rows + index, 1),
                            after: row(rows + index, index as i64),
                        });
                    }
                }
                let batch = ChangeBatch {
                    sequence: 1,
                    physical_txn_id: TxnId(1),
                    database_txn_id: None,
                    table_id: TableId(1),
                    storage_id,
                    schema_fingerprint: table().fingerprint()?,
                    before: StorageDataVersion(0),
                    after: StorageDataVersion(1),
                    mutations,
                };
                let projection = projection.prepare_advance(&table(), &[batch])?.publish()?;
                let incremental = projection
                    .metadata()
                    .incremental
                    .as_ref()
                    .expect("incremental metadata");
                let delta_segments = incremental.delta_segments.len();
                let delta_mutations = incremental.delta_mutation_count;
                let delta_bytes = incremental.delta_bytes;
                let suppressed = incremental.suppressed_version_count;
                let live_delta = incremental.delta_live_row_count;
                let old_bytes = projection
                    .metadata()
                    .segment_bytes
                    .saturating_add(delta_bytes);
                let started = Instant::now();
                black_box(projection.scan(&[ColumnId(1), ColumnId(2)], &[])?);
                let query_before = started.elapsed().as_nanos();
                let started = Instant::now();
                let compacted = projection
                    .prepare_compaction(&table(), ColumnarGeneration(2))?
                    .publish()?;
                let compaction = started.elapsed().as_nanos();
                let started = Instant::now();
                black_box(compacted.scan(&[ColumnId(1), ColumnId(2)], &[])?);
                let query_after = started.elapsed().as_nanos();
                let new_bytes = compacted.metadata().segment_bytes;
                println!(
                    "{rows},{workload},{ratio},{delta_segments},{delta_mutations},{delta_bytes},{suppressed},{live_delta},{query_before},{compaction},{query_after},{old_bytes},{new_bytes},{},{},direct-columnar,direct-columnar",
                    old_bytes.saturating_sub(new_bytes),
                    compacted.metadata().row_count
                );
                compacted.drop_files()?;
                for component in netbadb_storage::heap_resource_components(&heap) {
                    let _ = std::fs::remove_file(component.path);
                }
                let _ = std::fs::remove_dir_all(root);
            }
        }
    }
    Ok(())
}
