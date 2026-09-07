use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::hint::black_box;
use std::path::Path;
use std::time::{Duration, Instant};

use netbadb_core::{ColumnarProjectionSpec, Database, TableStorageCreateSpec};
use netbadb_inspect::{PlanNodeInspection, StatementPlanInspection};
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_storage::{
    ColumnarConstraint, ColumnarProjection, ColumnarScanStatistics, ColumnarVector, TableStorage,
};
use netbadb_types::{
    ColumnId, ColumnarGeneration, ColumnarProjectionId, PhysicalType, ScalarValue, StorageId,
    TableId,
};

const DEFAULT_ROWS: u64 = 10_000;
const DEFAULT_WIDTH: u32 = 4;
const DEFAULT_ROW_GROUP_ROWS: usize = 1_024;
const DEFAULT_SELECTIVITIES: &[u64] = &[1, 10, 100, 1_000, 5_000, 10_000];

#[derive(Clone, Copy)]
enum Distribution {
    Friendly,
    Hostile,
}

impl Distribution {
    const fn label(self) -> &'static str {
        match self {
            Self::Friendly => "zone-friendly",
            Self::Hostile => "zone-hostile",
        }
    }
}

struct Measurement {
    mean: Duration,
    chosen_plan: String,
    result_rows: usize,
    row_groups_total: u64,
    row_groups_read: u64,
    row_groups_pruned: u64,
    bytes_scanned: u64,
}

struct CsvRecord<'a> {
    engine: &'a str,
    workload: &'a str,
    distribution: &'a str,
    rows: u64,
    columns_total: u32,
    columns_read: u32,
    row_group_rows: usize,
    selectivity_basis_points: u64,
    measurement: &'a Measurement,
    logical_bytes: u64,
    segment_bytes: u64,
}

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

fn distributions() -> Result<Vec<Distribution>, Box<dyn Error>> {
    let value = std::env::var("NETBADB_COLUMNAR_DISTRIBUTIONS")
        .unwrap_or_else(|_| "friendly,hostile".into());
    value
        .split(',')
        .map(|part| match part.trim() {
            "friendly" => Ok(Distribution::Friendly),
            "hostile" => Ok(Distribution::Hostile),
            _ => Err(format!("invalid distribution {part:?}").into()),
        })
        .collect()
}

fn table(width: u32) -> Result<TableDef, Box<dyn Error>> {
    if ![4, 16, 64, 128].contains(&width) {
        return Err(format!("column width must be one of 4,16,64,128; got {width}").into());
    }
    let mut columns = Vec::with_capacity(width as usize);
    for position in 1..=width {
        let (physical, nullable) = if position <= 4 {
            (PhysicalType::Int64, false)
        } else {
            match position % 5 {
                0 => (PhysicalType::Text, false),
                1 => (PhysicalType::UInt64, false),
                2 => (PhysicalType::Bool, false),
                3 => (PhysicalType::Text, true),
                _ => (PhysicalType::Int64, true),
            }
        };
        columns.push(
            ColumnDef::new(
                ColumnId(position),
                format!("c{position}"),
                TypeSpec::Physical(physical),
            )
            .nullable(nullable),
        );
    }
    Ok(TableDef::new(TableId(1), "events", columns))
}

fn row_values(
    row: u64,
    rows: u64,
    width: u32,
    groups: u64,
    distribution: Distribution,
) -> Vec<ScalarValue> {
    let hostile_ts = if rows == 0 {
        0
    } else {
        row.wrapping_mul(48_271) % rows
    };
    let ts = match distribution {
        Distribution::Friendly => row,
        Distribution::Hostile => hostile_ts,
    };
    let mut values = Vec::with_capacity(width as usize);
    for position in 1..=width {
        let value = match position {
            1 => ScalarValue::Int64(i64::try_from(row).unwrap_or(i64::MAX)),
            2 => ScalarValue::Int64(i64::try_from(ts).unwrap_or(i64::MAX)),
            3 => ScalarValue::Int64(i64::try_from(row % groups.max(1)).unwrap_or(i64::MAX)),
            4 => ScalarValue::Int64(i64::try_from(row.wrapping_mul(3)).unwrap_or(i64::MAX)),
            _ if position % 5 == 0 => ScalarValue::Text(format!("text-{position}-{row:012}")),
            _ if position % 5 == 1 => ScalarValue::UInt64(row.wrapping_add(u64::from(position))),
            _ if position % 5 == 2 => ScalarValue::Bool((row + u64::from(position)) % 2 == 0),
            _ if position % 5 == 3 && row % 7 == 0 => ScalarValue::Null,
            _ if position % 5 == 3 => ScalarValue::Text(format!("nullable-{row:012}")),
            _ if row % 11 == 0 => ScalarValue::Null,
            _ => ScalarValue::Int64(i64::try_from(row).unwrap_or(i64::MAX)),
        };
        values.push(value);
    }
    values
}

fn logical_bytes(values: &[ScalarValue]) -> u64 {
    values
        .iter()
        .map(|value| match value {
            ScalarValue::Null => 0,
            ScalarValue::Bool(_) => 1,
            ScalarValue::Int8(_) | ScalarValue::UInt8(_) => 1,
            ScalarValue::Int16(_) | ScalarValue::UInt16(_) => 2,
            ScalarValue::Int32(_) | ScalarValue::UInt32(_) | ScalarValue::Float32(_) => 4,
            ScalarValue::Int64(_) | ScalarValue::UInt64(_) | ScalarValue::Float64(_) => 8,
            ScalarValue::Int128(_) | ScalarValue::UInt128(_) => 16,
            ScalarValue::Text(value) => u64::try_from(value.len()).unwrap_or(u64::MAX),
            ScalarValue::Bytes(value) => u64::try_from(value.len()).unwrap_or(u64::MAX),
        })
        .sum()
}

fn load(
    database: &mut Database,
    rows: u64,
    width: u32,
    groups: u64,
    distribution: Distribution,
) -> Result<u64, Box<dyn Error>> {
    let mut transaction = database.begin_transaction_for(TableId(1))?;
    let mut bytes = 0_u64;
    for row in 0..rows {
        let values = row_values(row, rows, width, groups, distribution);
        bytes = bytes.saturating_add(logical_bytes(&values));
        database.insert_in(&mut transaction, &values)?;
    }
    database.commit_transaction(&mut transaction)?;
    Ok(bytes)
}

fn plan_name(plan: &PlanNodeInspection) -> Option<&'static str> {
    match plan {
        PlanNodeInspection::ColumnarScan { .. } => Some("columnar"),
        PlanNodeInspection::IndexScan { .. } => Some("btree-point"),
        PlanNodeInspection::RangeIndexScan { .. } => Some("btree-range"),
        PlanNodeInspection::SeqScan { .. } => Some("sequential"),
        PlanNodeInspection::PartitionedScan { .. } => Some("partitioned"),
        PlanNodeInspection::Filter { input, .. }
        | PlanNodeInspection::Sort { input, .. }
        | PlanNodeInspection::Project { input, .. }
        | PlanNodeInspection::ScalarProject { input, .. }
        | PlanNodeInspection::Aggregate { input, .. }
        | PlanNodeInspection::Limit { input, .. } => plan_name(input),
        PlanNodeInspection::NestedLoopJoin { left, right, .. }
        | PlanNodeInspection::HashJoin { left, right, .. } => {
            plan_name(left).or_else(|| plan_name(right))
        }
        PlanNodeInspection::IndexNestedLoopJoin { .. } => Some("btree-join"),
        PlanNodeInspection::OneRow => Some("one-row"),
    }
}

fn measure(
    database: &mut Database,
    sql: &str,
    iterations: usize,
) -> Result<Measurement, Box<dyn Error>> {
    let inspection = database.inspect_statement(sql)?;
    let chosen_plan = match &inspection.plan {
        StatementPlanInspection::Query { root } => plan_name(root).unwrap_or("unknown"),
        _ => "non-query",
    }
    .to_owned();
    black_box(database.query_with_columnar_statistics(sql)?);
    let start = Instant::now();
    let mut last = None;
    for _ in 0..iterations {
        last = Some(black_box(database.query_with_columnar_statistics(sql)?));
    }
    let elapsed = start.elapsed() / u32::try_from(iterations)?;
    let (result, statistics) = last.ok_or("benchmark iterations must be positive")?;
    Ok(Measurement {
        mean: elapsed,
        chosen_plan,
        result_rows: result.rows.len(),
        row_groups_total: statistics.scan.row_groups_total,
        row_groups_read: statistics.scan.row_groups_read,
        row_groups_pruned: statistics.scan.row_groups_pruned,
        bytes_scanned: statistics.scan.bytes_read,
    })
}

fn print_record(record: CsvRecord<'_>) {
    println!(
        "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
        record.engine,
        record.workload,
        record.distribution,
        record.rows,
        record.columns_total,
        record.columns_read,
        record.row_group_rows,
        record.selectivity_basis_points,
        record.measurement.chosen_plan,
        record.measurement.row_groups_total,
        record.measurement.row_groups_read,
        record.measurement.row_groups_pruned,
        record.measurement.bytes_scanned,
        record.logical_bytes,
        record.segment_bytes,
        record.measurement.result_rows,
        record.measurement.mean.as_micros(),
    );
}

fn selectivity_bounds(rows: u64, selectivity_basis_points: u64) -> (u64, u64) {
    let selected = rows
        .saturating_mul(selectivity_basis_points)
        .div_ceil(10_000)
        .max(1)
        .min(rows);
    let lower = rows.saturating_sub(selected) / 2;
    (lower, lower.saturating_add(selected))
}

fn analytical_sql(rows: u64, selectivity_basis_points: u64) -> String {
    let (lower, upper) = selectivity_bounds(rows, selectivity_basis_points);
    format!("SELECT c3, SUM(c4) FROM events WHERE c2 >= {lower} AND c2 < {upper} GROUP BY c3")
}

fn cleanup(root: &Path) {
    let _ = fs::remove_dir_all(root);
}

struct Case {
    rows: u64,
    width: u32,
    row_group_rows: usize,
    groups: u64,
    distribution: Distribution,
    selectivities: Vec<u64>,
    iterations: usize,
    engines: Vec<String>,
}

fn engine_enabled(case: &Case, engine: &str) -> bool {
    case.engines.iter().any(|candidate| candidate == engine)
}

fn execute_direct_aggregate(
    projection: &ColumnarProjection,
    lower: i64,
    upper: i64,
    constraints: &[ColumnarConstraint],
) -> Result<(usize, ColumnarScanStatistics), Box<dyn Error>> {
    let (batches, statistics) =
        projection.scan(&[ColumnId(2), ColumnId(3), ColumnId(4)], constraints)?;
    let mut groups = HashMap::<i64, i64>::new();
    for batch in batches {
        let [timestamp, region, amount] = batch.columns.as_slice() else {
            return Err("direct columnar scan returned an unexpected shape".into());
        };
        let ColumnarVector::Int64 {
            values: timestamps, ..
        } = &timestamp.values
        else {
            return Err("timestamp vector is not Int64".into());
        };
        let ColumnarVector::Int64 {
            values: regions, ..
        } = &region.values
        else {
            return Err("region vector is not Int64".into());
        };
        let ColumnarVector::Int64 {
            values: amounts, ..
        } = &amount.values
        else {
            return Err("amount vector is not Int64".into());
        };
        for ((timestamp, region), amount) in timestamps.iter().zip(regions).zip(amounts) {
            if *timestamp >= lower && *timestamp < upper {
                let total = groups.entry(*region).or_default();
                *total = total.checked_add(*amount).ok_or("aggregate overflow")?;
            }
        }
    }
    Ok((black_box(groups).len(), statistics))
}

fn run_direct_columnar(case: &Case, root: &Path) -> Result<(), Box<dyn Error>> {
    let definition = table(case.width)?;
    let mut logical_size = 0_u64;
    let mut rows = Vec::with_capacity(usize::try_from(case.rows)?);
    for row in 0..case.rows {
        let values = row_values(row, case.rows, case.width, case.groups, case.distribution);
        logical_size = logical_size.saturating_add(logical_bytes(&values));
        rows.push(values);
    }
    let directory = root.join("direct-projection");
    let token_source = TableStorage::create_heap_with_storage_id(
        root.join("direct-source.ndb"),
        definition.clone(),
        StorageId(1),
    )?;
    let source_token = token_source.current_snapshot_token()?;
    drop(token_source);
    let projection = ColumnarProjection::prepare(
        &directory,
        ColumnarProjectionId(1),
        ColumnarGeneration(1),
        &definition,
        StorageId(1),
        source_token,
        &(1..=case.width).map(ColumnId).collect::<Vec<_>>(),
        &rows,
        Some(case.row_group_rows),
    )?
    .publish()?;
    drop(rows);
    let segment_bytes = projection.metadata().segment_bytes;
    for selectivity in &case.selectivities {
        let (lower, upper) = selectivity_bounds(case.rows, *selectivity);
        let constraints = [ColumnarConstraint {
            column_id: ColumnId(2),
            lower: Some((
                ScalarValue::Int64(i64::try_from(lower).unwrap_or(i64::MAX)),
                true,
            )),
            upper: Some((
                ScalarValue::Int64(i64::try_from(upper).unwrap_or(i64::MAX)),
                false,
            )),
        }];
        black_box(execute_direct_aggregate(
            &projection,
            i64::try_from(lower).unwrap_or(i64::MAX),
            i64::try_from(upper).unwrap_or(i64::MAX),
            &constraints,
        )?);
        let start = Instant::now();
        let mut last = None;
        for _ in 0..case.iterations {
            last = Some(black_box(execute_direct_aggregate(
                &projection,
                i64::try_from(lower).unwrap_or(i64::MAX),
                i64::try_from(upper).unwrap_or(i64::MAX),
                &constraints,
            )?));
        }
        let mean = start.elapsed() / u32::try_from(case.iterations)?;
        let (result_rows, statistics) = last.ok_or("benchmark iterations must be positive")?;
        let measurement = Measurement {
            mean,
            chosen_plan: "direct-columnar".into(),
            result_rows,
            row_groups_total: statistics.row_groups_total,
            row_groups_read: statistics.row_groups_read,
            row_groups_pruned: statistics.row_groups_pruned,
            bytes_scanned: statistics.bytes_read,
        };
        print_record(CsvRecord {
            engine: "columnar",
            workload: "direct-columnar-aggregate",
            distribution: case.distribution.label(),
            rows: case.rows,
            columns_total: case.width,
            columns_read: 3,
            row_group_rows: case.row_group_rows,
            selectivity_basis_points: *selectivity,
            measurement: &measurement,
            logical_bytes: logical_size,
            segment_bytes,
        });
    }
    drop(projection);
    fs::remove_dir_all(directory)?;
    Ok(())
}

fn run_case(case: &Case) -> Result<(), Box<dyn Error>> {
    let root = std::env::temp_dir().join(format!(
        "netbadb-columnar-scale-{}-{}-{}-{}-{}-{}",
        std::process::id(),
        case.rows,
        case.width,
        case.row_group_rows,
        case.groups,
        case.distribution.label()
    ));
    cleanup(&root);
    fs::create_dir_all(&root)?;
    if engine_enabled(case, "direct-columnar") {
        run_direct_columnar(case, &root)?;
        if case.engines.len() == 1 {
            cleanup(&root);
            return Ok(());
        }
    }
    let definition = table(case.width)?;
    let mut heap = Database::create_catalog(
        root.join("heap.catalog"),
        vec![TableStorageCreateSpec::heap(
            root.join("heap.ndb"),
            definition.clone(),
        )],
        None,
    )?;
    let logical_bytes = load(
        &mut heap,
        case.rows,
        case.width,
        case.groups,
        case.distribution,
    )?;
    for selectivity in &case.selectivities {
        let sql = analytical_sql(case.rows, *selectivity);
        let measurement = measure(&mut heap, &sql, case.iterations)?;
        print_record(CsvRecord {
            engine: "heap",
            workload: "analytical-range-group",
            distribution: case.distribution.label(),
            rows: case.rows,
            columns_total: case.width,
            columns_read: 3,
            row_group_rows: case.row_group_rows,
            selectivity_basis_points: *selectivity,
            measurement: &measurement,
            logical_bytes,
            segment_bytes: 0,
        });
    }
    let full_sql = "SELECT COUNT(*), SUM(c4), MIN(c1), MAX(c1) FROM events";
    let measurement = measure(&mut heap, full_sql, case.iterations)?;
    print_record(CsvRecord {
        engine: "heap",
        workload: "full-scan-aggregate",
        distribution: case.distribution.label(),
        rows: case.rows,
        columns_total: case.width,
        columns_read: 2,
        row_group_rows: case.row_group_rows,
        selectivity_basis_points: 10_000,
        measurement: &measurement,
        logical_bytes,
        segment_bytes: 0,
    });

    heap.create_index(TableId(1), ColumnId(1))?;
    let point_sql = format!("SELECT c4 FROM events WHERE c1 = {}", case.rows / 2);
    let measurement = measure(&mut heap, &point_sql, case.iterations)?;
    print_record(CsvRecord {
        engine: "heap-btree",
        workload: "point",
        distribution: case.distribution.label(),
        rows: case.rows,
        columns_total: case.width,
        columns_read: 2,
        row_group_rows: case.row_group_rows,
        selectivity_basis_points: 1,
        measurement: &measurement,
        logical_bytes,
        segment_bytes: 0,
    });
    let upper = case.rows.div_ceil(100).max(1);
    let range_sql = format!("SELECT c1, c4 FROM events WHERE c1 >= 0 AND c1 < {upper}");
    let measurement = measure(&mut heap, &range_sql, case.iterations)?;
    print_record(CsvRecord {
        engine: "heap-btree",
        workload: "narrow-range",
        distribution: case.distribution.label(),
        rows: case.rows,
        columns_total: case.width,
        columns_read: 2,
        row_group_rows: case.row_group_rows,
        selectivity_basis_points: 100,
        measurement: &measurement,
        logical_bytes,
        segment_bytes: 0,
    });

    heap.build_columnar_projection(
        ColumnarProjectionSpec::new(
            TableId(1),
            root.join("projection"),
            (1..=case.width).map(ColumnId).collect(),
        )
        .with_row_group_rows(case.row_group_rows),
    )?;
    let segment_bytes = heap
        .inspect_columnar_projections()
        .first()
        .and_then(|inspection| inspection.segment_bytes)
        .unwrap_or(0);
    for selectivity in &case.selectivities {
        let sql = analytical_sql(case.rows, *selectivity);
        let measurement = measure(&mut heap, &sql, case.iterations)?;
        print_record(CsvRecord {
            engine: "columnar-candidate",
            workload: "analytical-range-group",
            distribution: case.distribution.label(),
            rows: case.rows,
            columns_total: case.width,
            columns_read: 3,
            row_group_rows: case.row_group_rows,
            selectivity_basis_points: *selectivity,
            measurement: &measurement,
            logical_bytes,
            segment_bytes,
        });
    }
    let measurement = measure(&mut heap, full_sql, case.iterations)?;
    print_record(CsvRecord {
        engine: "columnar-candidate",
        workload: "full-scan-aggregate",
        distribution: case.distribution.label(),
        rows: case.rows,
        columns_total: case.width,
        columns_read: 2,
        row_group_rows: case.row_group_rows,
        selectivity_basis_points: 10_000,
        measurement: &measurement,
        logical_bytes,
        segment_bytes,
    });

    if engine_enabled(case, "lsm") {
        let mut lsm = Database::create_catalog(
            root.join("lsm.catalog"),
            vec![TableStorageCreateSpec::lsm(
                root.join("lsm"),
                definition,
                ColumnId(1),
            )],
            None,
        )?;
        let lsm_logical_bytes = load(
            &mut lsm,
            case.rows,
            case.width,
            case.groups,
            case.distribution,
        )?;
        for selectivity in &case.selectivities {
            let sql = analytical_sql(case.rows, *selectivity);
            let measurement = measure(&mut lsm, &sql, case.iterations)?;
            print_record(CsvRecord {
                engine: "lsm",
                workload: "analytical-range-group",
                distribution: case.distribution.label(),
                rows: case.rows,
                columns_total: case.width,
                columns_read: 3,
                row_group_rows: case.row_group_rows,
                selectivity_basis_points: *selectivity,
                measurement: &measurement,
                logical_bytes: lsm_logical_bytes,
                segment_bytes: 0,
            });
        }
        lsm.close()?;
    }
    heap.close()?;
    cleanup(&root);
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let rows = parse_list("NETBADB_COLUMNAR_ROWS", &[DEFAULT_ROWS])?;
    let widths = parse_list("NETBADB_COLUMNAR_WIDTHS", &[DEFAULT_WIDTH])?;
    let row_groups = parse_list("NETBADB_COLUMNAR_ROW_GROUPS", &[DEFAULT_ROW_GROUP_ROWS])?;
    let selectivities = parse_list("NETBADB_COLUMNAR_SELECTIVITIES", DEFAULT_SELECTIVITIES)?;
    let group_counts = parse_list("NETBADB_COLUMNAR_GROUPS", &[100_u64])?;
    let iterations =
        std::env::var("NETBADB_COLUMNAR_ITERATIONS").map_or(Ok(3_usize), |value| value.parse())?;
    let engines = std::env::var("NETBADB_COLUMNAR_ENGINES")
        .unwrap_or_else(|_| "heap,btree,columnar,lsm".into())
        .split(',')
        .map(|value| value.trim().to_owned())
        .collect::<Vec<_>>();
    if engines.is_empty()
        || engines.iter().any(|engine| {
            !["heap", "btree", "columnar", "direct-columnar", "lsm"].contains(&engine.as_str())
        })
    {
        return Err(
            "NETBADB_COLUMNAR_ENGINES must contain heap,btree,columnar,direct-columnar,lsm".into(),
        );
    }
    if iterations == 0 {
        return Err("NETBADB_COLUMNAR_ITERATIONS must be positive".into());
    }
    for value in &selectivities {
        if !(1..=10_000).contains(value) {
            return Err(format!("selectivity basis points must be 1..=10000; got {value}").into());
        }
    }
    println!(
        "engine_path,workload,distribution,rows,columns_total,columns_read,row_group_rows,predicate_selectivity_basis_points,chosen_plan,row_groups_total,row_groups_read,row_groups_pruned,bytes_scanned,total_logical_bytes,segment_bytes,result_rows,mean_us"
    );
    for row_count in rows {
        for width in &widths {
            for row_group_rows in &row_groups {
                if ![128, 512, 1_024, 2_048, 4_096, 8_192].contains(row_group_rows) {
                    return Err(format!(
                        "row group size must be one of 128,512,1024,2048,4096,8192; got {row_group_rows}"
                    )
                    .into());
                }
                for groups in &group_counts {
                    for distribution in distributions()? {
                        run_case(&Case {
                            rows: row_count,
                            width: *width,
                            row_group_rows: *row_group_rows,
                            groups: *groups,
                            distribution,
                            selectivities: selectivities.clone(),
                            iterations,
                            engines: engines.clone(),
                        })?;
                    }
                }
            }
        }
    }
    Ok(())
}
