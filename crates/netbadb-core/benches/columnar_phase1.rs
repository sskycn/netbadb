use std::error::Error;
use std::fs;
use std::hint::black_box;
use std::path::Path;
use std::time::{Duration, Instant};

use netbadb_core::{ColumnarProjectionSpec, Database, TableStorageCreateSpec};
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_storage::{HeapResourceComponent, heap_resource_components};
use netbadb_types::{ColumnId, PhysicalType, ScalarValue, TableId};

const ROWS: i64 = 2_048;
const ITERATIONS: usize = 5;

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
                "active",
                TypeSpec::Physical(PhysicalType::Bool),
            ),
            ColumnDef::new(
                ColumnId(4),
                "payload",
                TypeSpec::Physical(PhysicalType::Text),
            ),
        ],
    )
}

fn load(database: &mut Database) -> Result<(), Box<dyn Error>> {
    for id in 0..ROWS {
        database.insert(&[
            ScalarValue::Int64(id),
            ScalarValue::Int64(id * 3),
            ScalarValue::Bool(id % 2 == 0),
            ScalarValue::Text(format!("payload-{id:08}")),
        ])?;
    }
    Ok(())
}

fn measure(
    mut operation: impl FnMut() -> Result<(), Box<dyn Error>>,
) -> Result<Duration, Box<dyn Error>> {
    operation()?;
    let start = Instant::now();
    for _ in 0..ITERATIONS {
        operation()?;
    }
    Ok(start.elapsed() / u32::try_from(ITERATIONS)?)
}

fn cleanup_heap(path: &Path) {
    for HeapResourceComponent { path, .. } in heap_resource_components(path) {
        let _ = fs::remove_file(path);
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let root = std::env::temp_dir().join(format!("netbadb-columnar-bench-{}", std::process::id()));
    let heap_path = root.join("heap.ndb");
    let lsm_path = root.join("lsm");
    let projection_path = root.join("projection");
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(&root)?;

    let mut heap = Database::create(&heap_path, table())?;
    load(&mut heap)?;
    let analytical = "SELECT active, COUNT(*), SUM(amount), MIN(id), MAX(id) FROM events WHERE id >= 1024 GROUP BY active";
    let heap_scan = measure(|| {
        black_box(heap.query(analytical)?);
        Ok(())
    })?;

    heap.create_index(TableId(1), ColumnId(1))?;
    let point = measure(|| {
        black_box(heap.query("SELECT payload FROM events WHERE id = 1536")?);
        Ok(())
    })?;
    heap.build_columnar_projection(
        ColumnarProjectionSpec::new(
            TableId(1),
            &projection_path,
            vec![ColumnId(1), ColumnId(2), ColumnId(3), ColumnId(4)],
        )
        .with_row_group_rows(128),
    )?;
    let (_, scan) = heap.query_with_columnar_statistics(analytical)?;
    if scan.projection_id.is_none() {
        return Err("fresh analytical projection was not selected".into());
    }
    let columnar = measure(|| {
        black_box(heap.query_with_columnar_statistics(analytical)?);
        Ok(())
    })?;

    heap.execute("UPDATE events SET amount = 6142 WHERE id = 2047")?;
    let (_, stale_scan) = heap.query_with_columnar_statistics(analytical)?;
    if stale_scan.projection_id.is_some() {
        return Err("stale projection remained eligible".into());
    }
    let stale_fallback = measure(|| {
        black_box(heap.query_with_columnar_statistics(analytical)?);
        Ok(())
    })?;

    let mut lsm = Database::create_storages(vec![TableStorageCreateSpec::lsm(
        &lsm_path,
        table(),
        ColumnId(1),
    )])?;
    load(&mut lsm)?;
    let lsm_scan = measure(|| {
        black_box(lsm.query(analytical)?);
        Ok(())
    })?;

    println!(
        "scenario,rows,mean_us,bytes_scanned,column_chunks,row_groups_total,row_groups_read,row_groups_pruned"
    );
    println!(
        "heap-seq,{ROWS},{},unreported,unreported,0,0,0",
        heap_scan.as_micros()
    );
    println!(
        "heap-btree-point,1,{},unreported,unreported,0,0,0",
        point.as_micros()
    );
    println!(
        "lsm-scan,{ROWS},{},unreported,unreported,0,0,0",
        lsm_scan.as_micros()
    );
    println!(
        "columnar,{ROWS},{},{},{},{},{},{}",
        columnar.as_micros(),
        scan.scan.bytes_read,
        scan.scan.column_chunks_read,
        scan.scan.row_groups_total,
        scan.scan.row_groups_read,
        scan.scan.row_groups_pruned
    );
    println!(
        "stale-authoritative-fallback,{ROWS},{},unreported,unreported,0,0,0",
        stale_fallback.as_micros()
    );

    heap.close()?;
    lsm.close()?;
    cleanup_heap(&heap_path);
    let _ = fs::remove_dir_all(&lsm_path);
    let _ = fs::remove_dir_all(&projection_path);
    let _ = fs::remove_dir(&root);
    Ok(())
}
