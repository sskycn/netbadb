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
            ColumnDef::new(
                ColumnId(3),
                "payload",
                TypeSpec::Physical(PhysicalType::Text),
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
    vec![
        ScalarValue::Int64(row as i64),
        ScalarValue::Int64(amount),
        ScalarValue::Text(format!("payload-{row:08}")),
    ]
}

fn mutations(
    workload: &str,
    rows: usize,
    count: usize,
    storage_id: StorageId,
) -> Vec<StorageChange> {
    let insert = |index: usize| StorageChange::Insert {
        new_version: key(storage_id, rows + index, 1),
        after: row(rows + index, index as i64),
    };
    let update = |index: usize| StorageChange::Update {
        old_version: key(storage_id, index, 1),
        new_version: key(storage_id, index, 2),
        after: row(index, -(index as i64)),
    };
    let delete = |index: usize| StorageChange::Delete {
        old_version: key(storage_id, index, 1),
    };
    match workload {
        "insert" => (0..count).map(insert).collect(),
        "update" => (0..count).map(update).collect(),
        "delete" => (0..count).map(delete).collect(),
        "mixed" => {
            let inserts = count.saturating_mul(4) / 10;
            let updates = count.saturating_mul(4) / 10;
            let deletes = count.saturating_sub(inserts).saturating_sub(updates);
            (0..inserts)
                .map(insert)
                .chain((0..updates).map(update))
                .chain((updates..updates + deletes).map(delete))
                .collect()
        }
        _ => Vec::new(),
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let row_counts = std::env::var("NETBADB_COLUMNAR_DELTA_ROWS")
        .unwrap_or_else(|_| "100000,1000000".into())
        .split(',')
        .map(str::parse::<usize>)
        .collect::<Result<Vec<_>, _>>()?;
    let ratios = [0_u64, 1, 10, 100, 500, 1_000];
    println!(
        "base_rows,workload,delta_basis_points,delta_mutations,nbcd_bytes,advance_ns,query_ns,base_bytes,delta_bytes,suppressed,live_delta,selected_plan"
    );
    for rows in row_counts {
        for workload in ["insert", "update", "delete", "mixed"] {
            for ratio in ratios {
                let root = std::env::temp_dir().join(format!(
                    "netbadb-columnar-delta-bench-{rows}-{ratio}-{}",
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
                    .map(|index| {
                        let mut values = row(index, index as i64);
                        values.truncate(2);
                        (key(storage_id, index, 1), values)
                    })
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
                let mutation_count = rows.saturating_mul(ratio as usize) / 10_000;
                let started = Instant::now();
                let projection = if mutation_count == 0 {
                    projection
                } else {
                    let mutations = mutations(workload, rows, mutation_count, storage_id);
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
                    projection.prepare_advance(&table(), &[batch])?.publish()?
                };
                let advance_ns = started.elapsed().as_nanos();
                let started = Instant::now();
                let (_, scan) = projection.scan(&[ColumnId(1), ColumnId(2)], &[])?;
                black_box(&scan);
                let query_ns = started.elapsed().as_nanos();
                let metadata = projection.metadata();
                let incremental = metadata.incremental.as_ref().expect("incremental metadata");
                println!(
                    "{rows},{workload},{ratio},{},{},{advance_ns},{query_ns},{},{},{},{},direct-columnar",
                    incremental.delta_mutation_count,
                    incremental.delta_bytes,
                    metadata.segment_bytes,
                    incremental.delta_bytes,
                    incremental.suppressed_version_count,
                    incremental.delta_live_row_count,
                );
                projection.drop_files()?;
                for component in netbadb_storage::heap_resource_components(&heap) {
                    let _ = std::fs::remove_file(component.path);
                }
                let _ = std::fs::remove_dir_all(root);
            }
        }
    }
    Ok(())
}
