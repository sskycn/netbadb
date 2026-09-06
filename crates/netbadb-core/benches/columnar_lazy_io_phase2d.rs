use std::error::Error;
use std::hint::black_box;
use std::time::Instant;

use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_storage::{
    ChangeBatch, ChangeStreamCursor, ColumnarConstraint, ColumnarProjection, StorageChange,
    StorageVersionKey, TableStorage,
};
use netbadb_types::{
    ChangeStreamGeneration, ColumnId, ColumnarGeneration, ColumnarProjectionId, PageId,
    PhysicalType, RowId, ScalarValue, StorageDataVersion, StorageId, TableId, TxnId,
};

const DEFAULT_ROWS: &[u64] = &[100_000, 1_000_000];
const DEFAULT_WIDTHS: &[u32] = &[4, 16, 64, 128];
const ROW_GROUP_ROWS: usize = 4_096;

fn parse_list<T>(name: &str, default: &[T]) -> Result<Vec<T>, Box<dyn Error>>
where
    T: std::str::FromStr + Copy,
    T::Err: Error + 'static,
{
    match std::env::var(name) {
        Ok(value) => value
            .split(',')
            .map(|part| part.trim().parse::<T>().map_err(Into::into))
            .collect(),
        Err(_) => Ok(default.to_vec()),
    }
}

fn table(width: u32) -> Result<TableDef, Box<dyn Error>> {
    if !DEFAULT_WIDTHS.contains(&width) {
        return Err(format!("width must be one of 4,16,64,128; got {width}").into());
    }
    Ok(TableDef::new(
        TableId(1),
        "events",
        (1..=width)
            .map(|column| {
                let physical = if column <= 4 {
                    PhysicalType::Int64
                } else if column <= 84 {
                    PhysicalType::Text
                } else {
                    PhysicalType::UInt64
                };
                ColumnDef::new(
                    ColumnId(column),
                    format!("c{column}"),
                    TypeSpec::Physical(physical),
                )
            })
            .collect(),
    ))
}

fn row(row: u64, width: u32) -> Vec<ScalarValue> {
    (1..=width)
        .map(|column| {
            if column <= 4 {
                ScalarValue::Int64(i64::try_from(row + u64::from(column)).unwrap_or(i64::MAX))
            } else if column <= 84 {
                ScalarValue::Text("t".into())
            } else {
                ScalarValue::UInt64(row + u64::from(column))
            }
        })
        .collect()
}

fn numeric_table(width: u32) -> TableDef {
    TableDef::new(
        TableId(2),
        "delta_events",
        (1..=width)
            .map(|column| {
                ColumnDef::new(
                    ColumnId(column),
                    format!("c{column}"),
                    TypeSpec::Physical(PhysicalType::Int64),
                )
            })
            .collect(),
    )
}

fn numeric_row(row: u64, width: u32) -> Vec<ScalarValue> {
    (1..=width)
        .map(|column| ScalarValue::Int64((row + u64::from(column)) as i64))
        .collect()
}

fn heap_version(storage_id: StorageId, row: u64, generation: u32) -> StorageVersionKey {
    StorageVersionKey::Heap {
        storage_id,
        row_id: RowId {
            page: PageId(row + 1),
            slot: 0,
            generation,
        },
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let rows = parse_list("NETBADB_COLUMNAR_LAZY_ROWS", DEFAULT_ROWS)?;
    let widths = parse_list("NETBADB_COLUMNAR_LAZY_WIDTHS", DEFAULT_WIDTHS)?;
    let iterations = std::env::var("NETBADB_COLUMNAR_LAZY_ITERATIONS")
        .map_or(Ok(3_usize), |value| value.parse())?;
    if iterations == 0 {
        return Err("NETBADB_COLUMNAR_LAZY_ITERATIONS must be positive".into());
    }
    println!(
        "rows,width,queried_columns,row_group_rows,build_ms,open_us,segment_bytes,resident_metadata_bytes,resident_value_bytes,base_index_bytes,base_blocks,row_groups_total,row_groups_read,row_groups_pruned,logical_bytes_selected,physical_base_data_bytes,physical_version_bytes,physical_delta_data_bytes,blocks_verified,column_chunks_decoded,version_chunks_decoded,mean_query_us"
    );
    for row_count in rows {
        for width in &widths {
            let root = std::env::temp_dir().join(format!(
                "netbadb-columnar-lazy-phase2d-{}-{row_count}-{width}",
                std::process::id()
            ));
            let _ = std::fs::remove_dir_all(&root);
            std::fs::create_dir_all(&root)?;
            let source_path = root.join("source.ndb");
            let projection_path = root.join("projection");
            let definition = table(*width)?;
            let source = TableStorage::create_heap_with_storage_id(
                &source_path,
                definition.clone(),
                StorageId(1),
            )?;
            let token = source.current_snapshot_token()?;
            source.close()?;
            let build_started = Instant::now();
            let projection = ColumnarProjection::prepare_streaming(
                &projection_path,
                ColumnarProjectionId(1),
                ColumnarGeneration(1),
                &definition,
                StorageId(1),
                token,
                &(1..=*width).map(ColumnId).collect::<Vec<_>>(),
                (0..row_count).map(|value| row(value, *width)),
                Some(ROW_GROUP_ROWS),
            )?
            .publish()?;
            let build_ms = build_started.elapsed().as_millis();
            let segment_bytes = projection.metadata().segment_bytes;
            drop(projection);
            let open_started = Instant::now();
            let projection = ColumnarProjection::open(&projection_path, &definition)?;
            let open_us = open_started.elapsed().as_micros();
            let representation = projection.representation_statistics();
            let lower = row_count / 2;
            let upper = lower.saturating_add(100).min(row_count);
            let constraints = [ColumnarConstraint {
                column_id: ColumnId(2),
                lower: Some((ScalarValue::Int64(lower as i64 + 2), true)),
                upper: Some((ScalarValue::Int64(upper as i64 + 2), false)),
            }];
            let mut last = None;
            let query_started = Instant::now();
            for _ in 0..iterations {
                last = Some(black_box(
                    projection.scan(&[ColumnId(2), ColumnId(3), ColumnId(4)], &constraints)?,
                ));
            }
            let mean_query_us = query_started.elapsed().as_micros() / iterations as u128;
            let (_, statistics) = last.ok_or("benchmark iterations must be positive")?;
            println!(
                "{row_count},{width},3,{ROW_GROUP_ROWS},{build_ms},{open_us},{segment_bytes},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
                representation.resident_metadata_bytes,
                representation.resident_payload_bytes,
                representation.base_index_bytes,
                representation.base_block_count,
                statistics.row_groups_total,
                statistics.row_groups_read,
                statistics.row_groups_pruned,
                statistics.bytes_read,
                statistics.base_data_bytes_read,
                statistics.base_version_key_bytes_read,
                statistics.delta_data_bytes_read,
                statistics.blocks_verified,
                statistics.decoded_column_chunks,
                statistics.decoded_version_blocks,
                mean_query_us,
            );
            projection.drop_files()?;
            for component in netbadb_storage::heap_resource_components(&source_path) {
                let _ = std::fs::remove_file(component.path);
            }
            let _ = std::fs::remove_dir_all(root);
        }
    }
    if std::env::var("NETBADB_COLUMNAR_LAZY_DELTA").as_deref() != Ok("0") {
        const BASE_ROWS: u64 = 1_000_000;
        const DELTA_ROWS: u64 = 100_000;
        const WIDTH: u32 = 64;
        let root = std::env::temp_dir().join(format!(
            "netbadb-columnar-lazy-phase2d-delta-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root)?;
        let source_path = root.join("source.ndb");
        let projection_path = root.join("projection");
        let definition = numeric_table(WIDTH);
        let storage_id = StorageId(2);
        let source = TableStorage::create_heap_with_storage_id(
            &source_path,
            definition.clone(),
            storage_id,
        )?;
        let token = source.current_snapshot_token()?;
        source.close()?;
        let columns = (1..=WIDTH).map(ColumnId).collect::<Vec<_>>();
        let base = ColumnarProjection::prepare_incremental_streaming(
            &projection_path,
            ColumnarProjectionId(2),
            ColumnarGeneration(1),
            &definition,
            storage_id,
            token,
            ChangeStreamCursor {
                storage_id,
                generation: ChangeStreamGeneration(1),
                frontier: StorageDataVersion(1),
            },
            &columns,
            (0..BASE_ROWS).map(|value| {
                (
                    heap_version(storage_id, value, 1),
                    numeric_row(value, WIDTH),
                )
            }),
            Some(ROW_GROUP_ROWS),
        )?
        .publish()?;
        let fingerprint = definition.fingerprint()?;
        let mutations = (0..DELTA_ROWS)
            .map(|value| StorageChange::Update {
                old_version: heap_version(storage_id, value, 1),
                new_version: heap_version(storage_id, value, 2),
                after: numeric_row(BASE_ROWS.saturating_mul(2).saturating_add(value), WIDTH),
            })
            .collect::<Vec<_>>();
        let advanced = base
            .prepare_advance(
                &definition,
                &[ChangeBatch {
                    sequence: 1,
                    physical_txn_id: TxnId(1),
                    database_txn_id: None,
                    table_id: definition.id,
                    storage_id,
                    schema_fingerprint: fingerprint,
                    before: StorageDataVersion(1),
                    after: StorageDataVersion(2),
                    mutations,
                }],
            )?
            .publish()?;
        drop(advanced);
        let open_started = Instant::now();
        let projection = ColumnarProjection::open(&projection_path, &definition)?;
        let open_us = open_started.elapsed().as_micros();
        let representation = projection.representation_statistics();
        let lower = BASE_ROWS.saturating_mul(2).saturating_add(50_000);
        let upper = lower.saturating_add(100);
        let constraints = [ColumnarConstraint {
            column_id: ColumnId(1),
            lower: Some((ScalarValue::Int64(lower as i64 + 1), true)),
            upper: Some((ScalarValue::Int64(upper as i64 + 1), false)),
        }];
        let query_started = Instant::now();
        let (_, statistics) =
            black_box(projection.scan(&[ColumnId(2), ColumnId(3)], &constraints)?);
        let query_us = query_started.elapsed().as_micros();
        println!(
            "delta_base_rows,delta_rows,width,queried_columns,open_us,resident_metadata_bytes,resident_suppressed_version_bytes,resident_delta_row_reference_bytes,resident_after_image_bytes,delta_descriptor_bytes,delta_row_refs,groups_total,groups_read,groups_pruned,base_data_bytes,base_version_bytes,delta_data_bytes,blocks_verified,chunks_decoded,query_us"
        );
        println!(
            "{BASE_ROWS},{DELTA_ROWS},{WIDTH},2,{open_us},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            representation.resident_metadata_bytes,
            representation.resident_suppressed_version_bytes,
            representation.resident_delta_row_reference_bytes,
            representation.resident_payload_bytes,
            representation.delta_descriptor_bytes,
            representation.indexed_delta_row_references,
            statistics.row_groups_total,
            statistics.row_groups_read,
            statistics.row_groups_pruned,
            statistics.base_data_bytes_read,
            statistics.base_version_key_bytes_read,
            statistics.delta_data_bytes_read,
            statistics.blocks_verified,
            statistics.decoded_column_chunks,
            query_us,
        );
        projection.drop_files()?;
        for component in netbadb_storage::heap_resource_components(&source_path) {
            let _ = std::fs::remove_file(component.path);
        }
        let _ = std::fs::remove_dir_all(root);
    }
    Ok(())
}
