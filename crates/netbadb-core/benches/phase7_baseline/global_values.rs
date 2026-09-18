//! NULL density, Bytes ownership, Float64 and batch-boundary controls.

use super::*;

pub(super) fn run() -> BenchResult<()> {
    for lsm in [false, true] {
        for percent in [0, 10, 50, 100] {
            fixture(lsm, 1_000, 128, percent)?;
        }
        for rows in [255, 256, 257] {
            for width in [8, 128, 1024] {
                fixture(lsm, rows, width, 10)?;
            }
        }
        statistics_fixture(lsm)?;
    }
    Ok(())
}

fn statistics_fixture(lsm: bool) -> BenchResult<()> {
    let engine = if lsm { "lsm" } else { "heap" };
    let directory = Directory::new(&format!("stats-{engine}"))?;
    let mut database = directory.create(lsm)?;
    load(&mut database, 1_000, 8)?;
    if !lsm {
        database.create_index(ITEMS_TABLE_ID, ID_COLUMN_ID)?;
    }
    let sql = "SELECT id FROM items WHERE id >= 500";
    for (state, rows) in [
        ("fresh", 1_000_u64),
        ("analyzed", 1_000),
        ("stale", 2_000),
        ("refreshed", 2_000),
    ] {
        if state == "stale" {
            let mut transaction = database.begin_transaction_for(ITEMS_TABLE_ID)?;
            for id in 1_000..2_000 {
                database.insert_into_in(ITEMS_TABLE_ID, &mut transaction, &row(id, 2_000, 8)?)?;
            }
            transaction.commit()?;
        }
        if state == "analyzed" || state == "refreshed" {
            let start = Instant::now();
            database.analyze(ITEMS_TABLE_ID)?;
            report(
                &format!("stats_{engine}_{state}_analyze"),
                rows,
                8,
                &[start.elapsed()],
                None,
                "explicit-ANALYZE",
            )?;
        }
        let name = format!("stats_{engine}_{state}_range");
        let plan = inspect_plan(&database, &name, sql, &[], &[])?;
        let operator = match plan.as_str() {
            "Project>Filter>SeqScan" => Operator::SeqScan,
            "Project>Filter>RangeIndexScan" => Operator::RangeIndexScan,
            _ => {
                return Err(message_error(format!(
                    "unexpected statistics control plan: {plan}"
                )));
            }
        };
        let expected = (500..rows)
            .map(|id| vec![ScalarValue::Int64(id as i64)])
            .collect::<Vec<_>>();
        query(&mut database, &name, rows, 8, sql, &expected, operator)?;
    }
    database.close()?;
    Ok(())
}

fn fixture(lsm: bool, rows: u64, width: usize, null_percent: u64) -> BenchResult<()> {
    let engine = if lsm { "lsm" } else { "heap" };
    let prefix = format!("values_{engine}_n{rows}_w{width}_null{null_percent}");
    let directory = Directory::new(&prefix)?;
    let table = TableDef::new(
        TableId(1),
        "vals",
        vec![
            ColumnDef::new(ColumnId(1), "id", TypeSpec::Physical(PhysicalType::Int64)),
            ColumnDef::new(ColumnId(2), "k", TypeSpec::Physical(PhysicalType::Int64))
                .nullable(true),
            ColumnDef::new(ColumnId(3), "t", TypeSpec::Physical(PhysicalType::Text)).nullable(true),
            ColumnDef::new(ColumnId(4), "b", TypeSpec::Physical(PhysicalType::Bytes))
                .nullable(true),
            ColumnDef::new(ColumnId(5), "f", TypeSpec::Physical(PhysicalType::Float64))
                .nullable(true),
        ],
    );
    let path = directory.0.join("data");
    let spec = if lsm {
        TableStorageCreateSpec::lsm(&path, table.clone(), ColumnId(1))
    } else {
        TableStorageCreateSpec::heap(&path, table.clone())
    };
    let mut database = Database::create_storages(vec![spec])?;
    let input = (0..rows)
        .map(|id| {
            let value = i64::try_from(id)?;
            let mut row = vec![ScalarValue::Int64(value)];
            if id % 100 < null_percent {
                row.extend([
                    ScalarValue::Null,
                    ScalarValue::Null,
                    ScalarValue::Null,
                    ScalarValue::Null,
                ]);
            } else {
                let text = format!("{id:0width$}");
                row.extend([
                    ScalarValue::Int64(value),
                    ScalarValue::Text(text.clone()),
                    ScalarValue::Bytes(text.into_bytes()),
                    ScalarValue::Float64((value as f64 * 0.5).into()),
                ]);
            }
            Ok(row)
        })
        .collect::<BenchResult<Vec<_>>>()?;
    let scan_order = if lsm {
        let mut transaction = database.begin_transaction_for(TableId(1))?;
        for row in &input {
            database.insert_into_in(TableId(1), &mut transaction, row)?;
        }
        transaction.commit()?;
        (0..rows).collect::<Vec<_>>()
    } else {
        // Small NULL rows can fit on earlier pages rejected by wider rows.
        // Derive physical scan order from insert locators, independently of SQL
        // execution; never assume insertion order or use query output as oracle.
        // Close each owner before opening the next owner of this StorageId.
        database.close()?;
        let mut storage = HeapStorage::open(&path, table.clone())?;
        let mut transaction = storage.begin_transaction()?;
        let mut placements = Vec::new();
        for (id, row) in input.iter().enumerate() {
            let location = storage.insert_in(&mut transaction, row)?;
            placements.push((location.page.0, location.slot, id as u64));
        }
        transaction.commit()?;
        drop(transaction);
        storage.close()?;
        database = Database::open(&path, table)?;
        placements.sort_unstable();
        placements.into_iter().map(|(_, _, id)| id).collect()
    };
    // HashJoin admission requires statistics on both inputs, including aliases
    // of the same table. Keep ANALYZE explicit and outside all query timers.
    database.analyze(TableId(1))?;
    let live = scan_order
        .iter()
        .copied()
        .filter(|id| id % 100 >= null_percent)
        .collect::<Vec<_>>();
    let nulls = scan_order
        .iter()
        .copied()
        .filter(|id| id % 100 < null_percent)
        .collect::<Vec<_>>();
    let ids = |ids: &[u64]| -> Vec<Vec<ScalarValue>> {
        ids.iter()
            .map(|id| vec![ScalarValue::Int64(*id as i64)])
            .collect()
    };
    let mut measure = |suffix: &str, sql: &str, expected: &[Vec<ScalarValue>], operator| {
        let owned_bytes = expected
            .iter()
            .flatten()
            .map(|v| match v {
                ScalarValue::Bytes(bytes) => bytes.len(),
                _ => 0,
            })
            .sum::<usize>();
        println!("audit_owned_bytes,{prefix}_{suffix},{owned_bytes}");
        query(
            &mut database,
            &format!("{prefix}_{suffix}"),
            rows,
            width,
            sql,
            expected,
            operator,
        )
    };
    let filtered = live
        .iter()
        .copied()
        .filter(|id| *id >= rows / 2)
        .collect::<Vec<_>>();
    measure(
        "filter",
        &format!(
            "SELECT id FROM vals WHERE k >= {} AND (t = t OR k < 0)",
            rows / 2
        ),
        &ids(&filtered),
        Operator::SeqScan,
    )?;
    measure(
        "bytes_filter",
        "SELECT id FROM vals WHERE b = b",
        &ids(&live),
        Operator::SeqScan,
    )?;
    let sum = live.iter().copied().sum::<u64>();
    let mut sorted_live = live.clone();
    sorted_live.sort_unstable();
    let min = sorted_live
        .first()
        .map_or(ScalarValue::Null, |id| input[*id as usize][2].clone());
    let max = sorted_live
        .last()
        .map_or(ScalarValue::Null, |id| input[*id as usize][2].clone());
    measure(
        "aggregate",
        "SELECT COUNT(*), COUNT(k), SUM(k), MIN(t), MAX(t) FROM vals",
        &[vec![
            ScalarValue::UInt64(rows),
            ScalarValue::UInt64(live.len() as u64),
            if live.is_empty() {
                ScalarValue::Null
            } else {
                ScalarValue::Int64(sum as i64)
            },
            min,
            max,
        ]],
        Operator::Aggregate,
    )?;
    let mut groups = Vec::new();
    for &id in &scan_order {
        if id % 100 >= null_percent {
            groups.push(vec![ScalarValue::Int64(id as i64), ScalarValue::UInt64(1)]);
        } else if Some(&id) == nulls.first() {
            groups.push(vec![
                ScalarValue::Null,
                ScalarValue::UInt64(nulls.len() as u64),
            ]);
        }
    }
    measure(
        "group",
        "SELECT k, COUNT(*) FROM vals GROUP BY k",
        &groups,
        Operator::Aggregate,
    )?;
    let descending_null_first = nulls
        .iter()
        .copied()
        .chain(sorted_live.iter().copied().rev())
        .collect::<Vec<_>>();
    measure(
        "sort",
        "SELECT id FROM vals ORDER BY k DESC NULLS FIRST",
        &ids(&descending_null_first),
        Operator::Sort,
    )?;
    measure(
        "top_n",
        "SELECT id FROM vals ORDER BY k DESC NULLS FIRST LIMIT 20",
        &ids(&descending_null_first[..20]),
        Operator::Sort,
    )?;
    let descending_null_last = sorted_live
        .iter()
        .copied()
        .rev()
        .chain(nulls.iter().copied())
        .collect::<Vec<_>>();
    measure(
        "float_sort",
        "SELECT id FROM vals ORDER BY f DESC NULLS LAST",
        &ids(&descending_null_last),
        Operator::Sort,
    )?;
    let ascending_null_last = sorted_live
        .iter()
        .copied()
        .chain(nulls.iter().copied())
        .collect::<Vec<_>>();
    measure(
        "bytes_sort",
        "SELECT id FROM vals ORDER BY b ASC NULLS LAST",
        &ids(&ascending_null_last),
        Operator::Sort,
    )?;
    let duplicate = scan_order
        .iter()
        .map(|id| {
            vec![
                input[*id as usize][3].clone(),
                input[*id as usize][3].clone(),
            ]
        })
        .collect::<Vec<_>>();
    measure(
        "bytes_duplicate",
        "SELECT b, b FROM vals",
        &duplicate,
        Operator::SeqScan,
    )?;
    measure(
        "join",
        "SELECT l.id FROM vals l JOIN vals r ON l.k = r.k",
        &ids(&live),
        Operator::HashJoin,
    )?;
    database.close()?;
    Ok(())
}
