//! Additional whole-database attribution; shares the historical benchmark's
//! fixtures, observation gates and release target, never production dispatch.

use super::*;
use std::fs;

#[path = "global_values.rs"]
mod values;

pub(super) fn requested() -> BenchResult<bool> {
    match env::var("NETBADB_BENCH_SUITE") {
        Err(env::VarError::NotPresent) => Ok(false),
        Ok(value) if value == "historical" => Ok(false),
        Ok(value)
            if [
                "global",
                "reads",
                "writes",
                "lifecycle",
                "partitions",
                "joins",
                "values",
                "pages",
            ]
            .contains(&value.as_str()) =>
        {
            Ok(true)
        }
        value => Err(message_error(format!(
            "invalid NETBADB_BENCH_SUITE: {value:?}"
        ))),
    }
}

struct Directory(PathBuf);

impl Directory {
    fn new(label: &str) -> BenchResult<Self> {
        let sequence = FIXTURE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = env::temp_dir().join(format!(
            "netbadb-global-{label}-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir(&path)?;
        Ok(Self(path))
    }

    fn create(&self, lsm: bool) -> BenchResult<Database> {
        let path = self.0.join("items");
        let spec = if lsm {
            TableStorageCreateSpec::lsm(path, items_table(), ID_COLUMN_ID)
        } else {
            TableStorageCreateSpec::heap(path, items_table())
        };
        Ok(Database::create_catalog(
            self.0.join("schema"),
            vec![spec],
            None,
        )?)
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_dir_all(&self.0) {
            eprintln!("benchmark cleanup {}: {error}", self.0.display());
        }
    }
}

fn sizes() -> BenchResult<Vec<u64>> {
    let value = env::var("NETBADB_AUDIT_ROWS").unwrap_or_else(|_| "1000,10000,100000".into());
    let rows = value
        .split(',')
        .map(str::parse)
        .collect::<Result<Vec<u64>, _>>()?;
    if rows.is_empty() || rows.iter().any(|n| *n == 0 || *n > 100_000) {
        return Err(message_error("audit rows must be within 1..=100000"));
    }
    Ok(rows)
}

pub(super) fn run() -> BenchResult<()> {
    let suite = env::var("NETBADB_BENCH_SUITE")?;
    println!(
        "audit_csv,scenario,source_rows,text_width,iterations,min_ns,median_ns,max_ns,output_rows,output_scalar_slots,output_text_bytes,plan"
    );
    if suite == "global" || suite == "reads" {
        for rows in sizes()? {
            // 100K wide Heap fixture construction itself performs a very large
            // first-fit traversal. The 1K/10K width sweep and 100K narrow scale
            // are explicit, rather than silently reducing a requested size.
            let widths: &[usize] = if rows > 10_000 { &[8] } else { &[8, 128, 1024] };
            for &width in widths {
                for lsm in [false, true] {
                    if rows > 10_000 && !lsm {
                        eprintln!(
                            "audit coverage: {rows}-row Heap omitted; baseline first-fit setup is quadratic; 100K scale uses LSM"
                        );
                        continue;
                    }
                    read_fixture(rows, width, lsm)?;
                }
            }
        }
    }
    if suite == "global" || suite == "writes" {
        write_matrix()?;
    }
    if suite == "global" || suite == "partitions" {
        let mut measurements = Vec::new();
        let mut settings = BenchProfile::Full.settings();
        settings.phase66_partition_rows = 1_024;
        run_phase66_partitioned_scenarios(settings, &mut measurements)?;
        print_report(BenchProfile::Full, settings, &measurements)?;
        run_partition_correctness_scenarios()?;
        let (mut database, paths) = partitioned_items_fixture("audit-move", 1000, 4)?;
        let ids = (0..100).collect::<Vec<_>>();
        let mut pruned = Vec::new();
        measure_phase66_partition_query(
            &mut database,
            "audit_partition_pruned_p4",
            1000,
            1,
            "SELECT id FROM partitioned_items WHERE id >= 0 AND id < 100",
            &[Operator::Filter, Operator::SeqScan],
            expected_ids_observation(&ids)?,
            settings,
            |result| ordered_ids_observation(result, &ids),
            &mut pruned,
        )?;
        print_report(BenchProfile::Full, settings, &pruned)?;
        let expected = (0..1000)
            .map(|id| vec![ScalarValue::Int64(id)])
            .collect::<Vec<_>>();
        query(
            &mut database,
            "partition_join_p4",
            1000,
            0,
            "SELECT l.id FROM partitioned_items l JOIN partitioned_items r ON l.id = r.id",
            &expected,
            Operator::NestedLoopJoin,
        )?;
        let start = Instant::now();
        let result = database.execute("UPDATE partitioned_items SET id = 1001 WHERE id = 0")?;
        let elapsed = start.elapsed();
        if result != ExecutionResult::AffectedRows(1) {
            return Err(message_error("partition movement affected rows differ"));
        }
        let moved = database.query("SELECT id FROM partitioned_items WHERE id = 1001")?;
        if moved.rows != vec![vec![ScalarValue::Int64(1001)]] {
            return Err(message_error("partition destination differs"));
        }
        report(
            "partition_move_p4",
            1000,
            0,
            &[elapsed],
            None,
            "cross-StorageId-atomic-update",
        )?;
        database.close()?;
        paths.cleanup()?;
    }
    if suite == "global" || suite == "lifecycle" {
        lifecycle()?;
    }
    if suite == "global" || suite == "joins" {
        join_matrix()?;
    }
    if suite == "global" || suite == "values" {
        values::run()?;
    }
    if suite == "global" || suite == "pages" {
        page_work()?;
    }
    Ok(())
}

fn page_work() -> BenchResult<()> {
    use netbadb_storage::{
        PAGE_HEADER_SIZE, PAGE_SIZE, Page, PageError, PageType, SLOT_SIZE, StorageError,
    };
    use netbadb_types::{PageId, SlotId};
    for slots in [1_u16, 16, 128, 256] {
        let mut full = Page::new(PageId(1), PageType::Heap);
        for _ in 1..slots {
            full.insert_record(b"x")?;
        }
        let remaining =
            PAGE_SIZE - PAGE_HEADER_SIZE - usize::from(slots) * SLOT_SIZE - usize::from(slots - 1);
        full.insert_record(&vec![7; remaining])?;
        if full.header()?.free_space() != 0 {
            return Err(message_error("page fixture is not full"));
        }
        for operation in ["full_reject", "replace", "delete", "reuse"] {
            let mut initial = full.clone();
            if operation == "reuse" {
                initial.delete_record(SlotId(0))?;
            }
            let mut times = Vec::new();
            for iteration in 0..103 {
                let mut page = initial.clone();
                let start = Instant::now();
                match operation {
                    "full_reject" => {
                        if !matches!(page.insert_record(black_box(b"x")), Err(StorageError::Page(PageError::PageFull { required, available: 0 })) if required == SLOT_SIZE + 1)
                        {
                            return Err(message_error(
                                "full page did not reject with the exact fit error",
                            ));
                        }
                    }
                    "replace" => page.replace_record(SlotId(0), black_box(b"x"))?,
                    "delete" => page.delete_record(SlotId(0))?,
                    "reuse" => {
                        let inserted = page.insert_record(black_box(b"x"))?;
                        if inserted.slot != SlotId(0) || inserted.generation != 2 {
                            return Err(message_error("tombstone reuse differs"));
                        }
                    }
                    _ => return Err(message_error("unknown page operation")),
                }
                let elapsed = start.elapsed();
                if operation == "full_reject" && page.bytes() != initial.bytes() {
                    return Err(message_error("rejected insert changed page bytes"));
                }
                if operation == "replace" || operation == "reuse" {
                    if page.read_record(SlotId(0))? != b"x" {
                        return Err(message_error("page replacement payload differs"));
                    }
                } else if operation == "delete" && !page.is_slot_deleted(SlotId(0))? {
                    return Err(message_error("page delete did not retain tombstone"));
                }
                if page.header()?.slot_count != slots {
                    return Err(message_error("page operation renumbered slots"));
                }
                if iteration >= 3 {
                    times.push(elapsed);
                }
            }
            report(
                &format!("page_s{slots}_{operation}"),
                u64::from(slots),
                0,
                &times,
                None,
                "validated-Heap-page-operation-clone-excluded",
            )?;
        }
    }
    Ok(())
}

fn row(id: u64, rows: u64, width: usize) -> BenchResult<Vec<ScalarValue>> {
    let mut values = item_row(id, 4, NullDistribution::Low)?;
    values[2] = ScalarValue::Int64(i64::try_from(id % (rows / 100).max(1))?);
    values[5] = ScalarValue::Text(sort_text_payload(id, rows, width)?);
    Ok(values)
}

fn load(database: &mut Database, rows: u64, width: usize) -> BenchResult<()> {
    for start in (0..rows).step_by(4096) {
        let mut transaction = database.begin_transaction_for(ITEMS_TABLE_ID)?;
        for id in start..(start + 4096).min(rows) {
            database.insert_into_in(ITEMS_TABLE_ID, &mut transaction, &row(id, rows, width)?)?;
        }
        transaction.commit()?;
    }
    Ok(())
}

fn report(
    name: &str,
    rows: u64,
    width: usize,
    times: &[Duration],
    result: Option<&QueryResult>,
    plan: &str,
) -> BenchResult<()> {
    let stats = Statistics::from_durations(times, 1)?;
    let mut slots = 0;
    let mut bytes = 0;
    if let Some(result) = result {
        for row in &result.rows {
            slots += row.len();
            for value in row {
                if let ScalarValue::Text(text) = value {
                    bytes += text.len();
                }
            }
        }
    }
    let max = times
        .iter()
        .map(Duration::as_nanos)
        .max()
        .ok_or_else(|| message_error("empty timings"))?;
    println!(
        "audit_csv,{name},{rows},{width},{},{},{},{max},{},{slots},{bytes},{plan}",
        times.len(),
        stats.min_ns_per_op,
        stats.median_ns_per_op,
        result.map_or(0, |r| r.rows.len())
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn query(
    database: &mut Database,
    name: &str,
    rows: u64,
    width: usize,
    sql: &str,
    expected: &[Vec<ScalarValue>],
    required: Operator,
) -> BenchResult<()> {
    let plan = inspect_plan(database, name, sql, &[required], &[])?;
    if name.ends_with("_range_large")
        && plan != "Project>Filter>SeqScan"
        && plan != "Project>Filter>RangeIndexScan"
    {
        return Err(message_error(format!(
            "{name}: unsupported large-range plan {plan}"
        )));
    }
    let (observed, feedback) = database.query_with_feedback(sql)?;
    if observed.rows != expected {
        return Err(message_error(format!("{name}: feedback result differs")));
    }
    println!(
        "audit_work,{name},accesses={:?},filters={:?},incomplete={}",
        feedback
            .accesses
            .iter()
            .map(|a| (&a.actual.kind, &a.actual.work))
            .collect::<Vec<_>>(),
        feedback.filters,
        feedback.incomplete
    );
    let mut times = Vec::new();
    for iteration in 0..14 {
        let start = Instant::now();
        let result = database.query(black_box(sql))?;
        let duration = start.elapsed();
        if result.rows != expected {
            return Err(message_error(format!("{name}: exact result differs")));
        }
        black_box(&result);
        if iteration >= 3 {
            times.push(duration);
        }
    }
    report(name, rows, width, &times, Some(&observed), &plan)
}

fn projected(
    ids: impl IntoIterator<Item = u64>,
    rows: u64,
    width: usize,
    positions: &[usize],
) -> BenchResult<Vec<Vec<ScalarValue>>> {
    ids.into_iter()
        .map(|id| {
            let values = row(id, rows, width)?;
            Ok(positions.iter().map(|p| values[*p].clone()).collect())
        })
        .collect()
}

fn read_fixture(rows: u64, width: usize, lsm: bool) -> BenchResult<()> {
    let engine = if lsm { "lsm" } else { "heap" };
    let prefix = format!("{engine}_n{rows}_w{width}");
    eprintln!("audit fixture {prefix}");
    let directory = Directory::new(&prefix)?;
    let mut database = directory.create(lsm)?;
    let started = Instant::now();
    load(&mut database, rows, width)?;
    report(
        &format!("{prefix}_load"),
        rows,
        width,
        &[started.elapsed()],
        None,
        "explicit-batches-4096-including-commit",
    )?;
    let mut cases = vec![
        (
            "id",
            "SELECT id FROM items".to_owned(),
            (0..rows).collect::<Vec<_>>(),
            vec![0],
        ),
        (
            "identity",
            "SELECT id, team_id, bucket_id, nullable_key, active, payload FROM items".to_owned(),
            (0..rows).collect(),
            vec![0, 1, 2, 3, 4, 5],
        ),
        (
            "subset",
            "SELECT payload FROM items".to_owned(),
            (0..rows).collect(),
            vec![5],
        ),
        (
            "reorder",
            "SELECT payload, id FROM items".to_owned(),
            (0..rows).collect(),
            vec![5, 0],
        ),
        (
            "duplicate",
            "SELECT payload, payload FROM items".to_owned(),
            (0..rows).collect(),
            vec![5, 5],
        ),
        (
            "filter_half",
            format!("SELECT id FROM items WHERE id < {}", rows / 2),
            (0..rows / 2).collect(),
            vec![0],
        ),
        (
            "filter_zero",
            "SELECT id FROM items WHERE active = true AND active = false".to_owned(),
            vec![],
            vec![0],
        ),
        (
            "filter_all",
            "SELECT id FROM items WHERE active = true OR active = false".to_owned(),
            (0..rows).collect(),
            vec![0],
        ),
        (
            "filter_null",
            "SELECT id FROM items WHERE nullable_key IS NULL".to_owned(),
            (0..rows).filter(|id| id % 100 == 0).collect(),
            vec![0],
        ),
        (
            "filter_text",
            format!(
                "SELECT id FROM items WHERE payload = '{}'",
                sort_text_payload(rows / 2, rows, width)?
            ),
            vec![rows / 2],
            vec![0],
        ),
        (
            "sort_hidden_text",
            "SELECT id FROM items ORDER BY payload".to_owned(),
            (0..rows).rev().collect(),
            vec![0],
        ),
        (
            "sort_retained_text",
            "SELECT id, payload FROM items ORDER BY payload".to_owned(),
            (0..rows).rev().collect(),
            vec![0, 5],
        ),
        (
            "sort_primitive",
            "SELECT id FROM items ORDER BY id DESC".to_owned(),
            (0..rows).rev().collect(),
            vec![0],
        ),
    ];
    let mut multi = (0..rows).collect::<Vec<_>>();
    multi.sort_by_key(|id| (id % 4, std::cmp::Reverse(*id)));
    cases.push((
        "sort_multi",
        "SELECT id FROM items ORDER BY team_id, id DESC".into(),
        multi.clone(),
        vec![0],
    ));
    multi.truncate(20);
    cases.push((
        "top_n",
        "SELECT id FROM items ORDER BY team_id, id DESC LIMIT 20".into(),
        multi,
        vec![0],
    ));
    for (label, sql, ids, columns) in cases {
        let expected = projected(ids, rows, width, &columns)?;
        let required = if label.starts_with("sort") || label == "top_n" {
            Operator::Sort
        } else {
            Operator::SeqScan
        };
        query(
            &mut database,
            &format!("{prefix}_{label}"),
            rows,
            width,
            &sql,
            &expected,
            required,
        )?;
    }
    if width == 8 {
        for (descending, nulls_first, direction, nulls) in [
            (false, false, "ASC", "LAST"),
            (false, true, "ASC", "FIRST"),
            (true, false, "DESC", "LAST"),
            (true, true, "DESC", "FIRST"),
        ] {
            let ids = expected_nullable_sort_ids(rows, descending, nulls_first);
            for limited in [false, true] {
                let count = if limited { 20 } else { ids.len() };
                let expected = projected(ids.iter().copied().take(count), rows, width, &[0])?;
                let limit = if limited { " LIMIT 20" } else { "" };
                query(
                    &mut database,
                    &format!("{prefix}_nullable_{direction}_{nulls}_top{limited}"),
                    rows,
                    width,
                    &format!(
                        "SELECT id FROM items ORDER BY nullable_key {direction} NULLS {nulls}{limit}"
                    ),
                    &expected,
                    Operator::Sort,
                )?;
            }
        }
    }
    query(
        &mut database,
        &format!("{prefix}_count"),
        rows,
        width,
        "SELECT COUNT(*), COUNT(nullable_key) FROM items",
        &[vec![
            ScalarValue::UInt64(rows),
            ScalarValue::UInt64(rows - (rows - 1) / 100 - 1),
        ]],
        Operator::Aggregate,
    )?;
    query(
        &mut database,
        &format!("{prefix}_aggregate"),
        rows,
        width,
        "SELECT SUM(id), MIN(payload), MAX(payload) FROM items",
        &[vec![
            ScalarValue::Int64(i64::try_from(arithmetic_sum(rows))?),
            ScalarValue::Text(sort_text_payload(rows - 1, rows, width)?),
            ScalarValue::Text(sort_text_payload(0, rows, width)?),
        ]],
        Operator::Aggregate,
    )?;
    for (column, groups) in [
        ("team_id", 4),
        ("bucket_id", (rows / 100).max(1)),
        ("id", rows),
    ] {
        let expected = (0..groups.min(rows))
            .map(|id| {
                Ok(vec![
                    ScalarValue::Int64(i64::try_from(id)?),
                    ScalarValue::UInt64((rows - 1 - id) / groups + 1),
                ])
            })
            .collect::<BenchResult<Vec<_>>>()?;
        query(
            &mut database,
            &format!("{prefix}_group_{column}"),
            rows,
            width,
            &format!("SELECT {column}, COUNT(*) FROM items GROUP BY {column}"),
            &expected,
            Operator::Aggregate,
        )?;
    }
    if !lsm {
        database.create_index(ITEMS_TABLE_ID, ID_COLUMN_ID)?;
    }
    database.analyze(ITEMS_TABLE_ID)?;
    access_queries(&mut database, &prefix, rows, width)?;
    if lsm {
        database.flush()?;
        println!(
            "audit_lsm,{prefix}_flushed,{:?}",
            database.inspect_lsm_storage(ITEMS_TABLE_ID)?
        );
        access_queries(&mut database, &format!("{prefix}_flushed"), rows, width)?;
        // Disjoint short transactions and explicit flushes create overlapping
        // L0 inputs without changing the queried row set (updates are identity).
        for id in 0..8 {
            database.execute(&format!(
                "UPDATE items SET active = {} WHERE id = {id}",
                id % 3 == 0
            ))?;
            database.flush()?;
        }
        println!(
            "audit_lsm,{prefix}_multi_l0,{:?}",
            database.inspect_lsm_storage(ITEMS_TABLE_ID)?
        );
        access_queries(&mut database, &format!("{prefix}_multi_l0"), rows, width)?;
        database.compact(ITEMS_TABLE_ID)?;
        println!(
            "audit_lsm,{prefix}_compacted,{:?}",
            database.inspect_lsm_storage(ITEMS_TABLE_ID)?
        );
        access_queries(&mut database, &format!("{prefix}_compacted"), rows, width)?;
    }
    for (label, sql) in [
        (
            "dml_update",
            format!(
                "UPDATE items SET payload = '{}' WHERE id = 0",
                sort_text_payload(0, rows, width)?
            ),
        ),
        ("dml_delete", "DELETE FROM items WHERE id = 0".into()),
    ] {
        let inspection = database.inspect_statement(&sql)?;
        let start = Instant::now();
        let result = database.execute(&sql)?;
        let elapsed = start.elapsed();
        if result != ExecutionResult::AffectedRows(1) {
            return Err(message_error("engine DML affected rows differ"));
        }
        report(
            &format!("{prefix}_{label}"),
            rows,
            width,
            &[elapsed],
            None,
            &format!("{:?}", inspection.plan),
        )?;
    }
    let replacement = row(0, rows, width)?;
    let start = Instant::now();
    database.insert_into(ITEMS_TABLE_ID, &replacement)?;
    report(
        &format!("{prefix}_dml_insert"),
        rows,
        width,
        &[start.elapsed()],
        None,
        "autocommit-direct-insert",
    )?;
    let sql = "SELECT id FROM items WHERE id = 1";
    let mut compile = Vec::new();
    let mut plan = Vec::new();
    for _ in 0..101 {
        let start = Instant::now();
        black_box(database.prepare_statement(sql, &[])?);
        compile.push(start.elapsed());
        let start = Instant::now();
        black_box(database.inspect_statement(sql)?);
        plan.push(start.elapsed());
    }
    report(
        &format!("{prefix}_compile"),
        rows,
        width,
        &compile[1..],
        None,
        "parse-resolve-type-HIR-Rel",
    )?;
    report(
        &format!("{prefix}_compile_plan"),
        rows,
        width,
        &plan[1..],
        None,
        "compile-plan-inspection",
    )?;
    database.close()?;
    let mut reopen = Vec::new();
    for _ in 0..3 {
        let start = Instant::now();
        let mut reopened = Database::open_catalog(directory.0.join("schema"))?;
        reopen.push(start.elapsed());
        if count_observation(&reopened.query("SELECT COUNT(*) FROM items")?)?.checksum
            != u128::from(rows)
        {
            return Err(message_error("reopen count differs"));
        }
        reopened.close()?;
    }
    report(
        &format!("{prefix}_reopen"),
        rows,
        width,
        &reopen,
        None,
        "fresh-handle-warm-OS-cache",
    )?;
    let mut reopened = Database::open_catalog(directory.0.join("schema"))?;
    reopened.checkpoint()?;
    reopened.close()?;
    let mut checkpointed = Vec::new();
    for _ in 0..3 {
        let start = Instant::now();
        let reopened = Database::open_catalog(directory.0.join("schema"))?;
        checkpointed.push(start.elapsed());
        reopened.close()?;
    }
    report(
        &format!("{prefix}_reopen_checkpointed"),
        rows,
        width,
        &checkpointed,
        None,
        "fresh-handle-warm-OS-cache-checkpointed",
    )?;
    Ok(())
}

fn access_queries(
    database: &mut Database,
    prefix: &str,
    rows: u64,
    width: usize,
) -> BenchResult<()> {
    for (name, sql, ids, required) in [
        (
            "point",
            "SELECT id FROM items WHERE id = 1".into(),
            vec![1],
            Operator::IndexScan,
        ),
        (
            "point_miss",
            format!("SELECT id FROM items WHERE id = {}", rows + 1),
            vec![],
            Operator::IndexScan,
        ),
        (
            "range_small",
            "SELECT id FROM items WHERE id >= 1 AND id <= 3".into(),
            vec![1, 2, 3],
            Operator::RangeIndexScan,
        ),
        (
            "range_large",
            format!("SELECT id FROM items WHERE id >= 0 AND id < {rows}"),
            (0..rows).collect(),
            Operator::Filter,
        ),
    ] {
        let expected = projected(ids, rows, width, &[0])?;
        inspect_base_scan_columns(database, name, &sql, &[ID_COLUMN_ID])?;
        query(
            database,
            &format!("{prefix}_{name}"),
            rows,
            width,
            &sql,
            &expected,
            required,
        )?;
    }
    Ok(())
}

fn join_matrix() -> BenchResult<()> {
    let mut settings = BenchProfile::Quick.settings();
    settings.phase65_probe_rows = 1_024;
    settings.join_iterations = 11;
    let mut measurements = Vec::new();
    run_phase65_hash_join_scenarios(settings, &mut measurements)?;
    run_phase68_hash_join_scenarios(settings, &mut measurements)?;
    run_phase70_hash_join_scenarios(settings, &mut measurements)?;
    for (label, cardinality, offset, operator, sql) in [
        (
            "unique",
            300,
            0,
            Operator::HashJoin,
            "SELECT l.id FROM left_rows l JOIN right_rows r ON l.join_key = r.join_key",
        ),
        (
            "duplicate",
            30,
            0,
            Operator::HashJoin,
            "SELECT l.id FROM left_rows l JOIN right_rows r ON l.join_key = r.join_key",
        ),
        (
            "none",
            300,
            300,
            Operator::HashJoin,
            "SELECT l.id FROM left_rows l JOIN right_rows r ON l.join_key = r.join_key",
        ),
        (
            "non_equi_none",
            300,
            300,
            Operator::NestedLoopJoin,
            "SELECT l.id FROM left_rows l JOIN right_rows r ON l.join_key > r.join_key",
        ),
    ] {
        run_join_query(
            JoinScenario {
                name: &format!("audit_join_{label}"),
                rows: 300,
                cardinality,
                left_key_offset: 0,
                right_key_offset: offset,
                sql,
                expected: if offset == 0 {
                    expected_join(300, cardinality)
                } else {
                    Observation {
                        rows: 0,
                        checksum: 0,
                    }
                },
                operator,
                wide: false,
            },
            settings,
            &mut measurements,
        )?;
    }
    run_text_join_query("audit_non_equi_text", 300, settings, &mut measurements)?;
    for matching in [true, false] {
        run_phase73_join_pair(
            Phase73JoinScenario {
                family: if matching {
                    "audit_match"
                } else {
                    "audit_none"
                },
                left_rows: 8,
                right_rows: 1_024,
                right_cardinality: 1_024,
                matching,
                text_width: None,
            },
            settings,
            &mut measurements,
        )?;
    }
    run_phase73_lsm_index_join_control(settings, &mut measurements)?;
    print_report(BenchProfile::Quick, settings, &measurements)
}

fn write_matrix() -> BenchResult<()> {
    for index_count in [0, 1, 4, 8] {
        for batch in [1_u64, 1000] {
            let directory = Directory::new("write")?;
            let table = TableDef::new(
                TableId(1),
                "writes",
                (1..=9)
                    .map(|i| {
                        ColumnDef::new(
                            ColumnId(i),
                            format!("c{i}"),
                            TypeSpec::Physical(PhysicalType::Int64),
                        )
                    })
                    .collect(),
            );
            let mut database = Database::create(directory.0.join("data"), table)?;
            for index in 1..=index_count {
                database.create_index(TableId(1), ColumnId(index))?;
            }
            let prefix = format!("heap_write_i{index_count}_batch{batch}");
            let mut execute = Vec::new();
            let mut commit = Vec::new();
            for sample in 0..5 {
                let values = (0..batch)
                    .map(|id| {
                        vec![
                            ScalarValue::Int64(
                                i64::try_from(sample * batch + id).expect("bounded benchmark ID")
                            );
                            9
                        ]
                    })
                    .collect::<Vec<_>>();
                let mut tx = database.begin_transaction_for(TableId(1))?;
                let start = Instant::now();
                for value in &values {
                    database.insert_into_in(TableId(1), &mut tx, value)?;
                }
                execute.push(start.elapsed());
                let start = Instant::now();
                tx.commit()?;
                commit.push(start.elapsed());
            }
            report(
                &format!("{prefix}_execute"),
                batch,
                0,
                &execute,
                None,
                "insert-with-index-maintenance-WAL",
            )?;
            report(
                &format!("{prefix}_commit"),
                batch,
                0,
                &commit,
                None,
                "durable-commit",
            )?;
            let inspection = database.inspect_prepared_runtime(TableId(1))?;
            println!(
                "audit_durability,{prefix},{inspection:?},wal_bytes={}",
                fs::metadata(netbadb_storage::wal_path(directory.0.join("data")))?.len()
            );
            database.analyze(TableId(1))?;
            for (label, sql) in [
                (
                    "update_unindexed",
                    "UPDATE writes SET c9 = 777 WHERE c1 = 0",
                ),
                (
                    "update_indexed",
                    "UPDATE writes SET c1 = 888888 WHERE c1 = 0",
                ),
                ("delete", "DELETE FROM writes WHERE c1 = 888888"),
            ] {
                let inspection = database.inspect_statement(sql)?;
                let start = Instant::now();
                let result = database.execute(sql)?;
                let elapsed = start.elapsed();
                if result != ExecutionResult::AffectedRows(1) {
                    return Err(message_error("DML affected rows differ"));
                }
                report(
                    &format!("{prefix}_{label}"),
                    batch * 5,
                    0,
                    &[elapsed],
                    None,
                    &format!("{:?}", inspection.plan),
                )?;
            }
            let count = count_observation(&database.query("SELECT COUNT(*) FROM writes")?)?;
            if count.checksum != u128::from(batch * 5 - 1) {
                return Err(message_error("DML final count differs"));
            }
            database.close()?;
        }
    }
    Ok(())
}

fn lifecycle() -> BenchResult<()> {
    // Retained-history reopen versus checkpointed reopen. All measurements
    // include authoritative recovery; no cache purge or power-loss claim.
    for rows in [100_u64, 1000] {
        let directory = Directory::new("history")?;
        let mut database = directory.create(false)?;
        load(&mut database, rows, 8)?;
        database.close()?;
        for checkpoint in [false, true] {
            let mut times = Vec::new();
            for _ in 0..3 {
                let start = Instant::now();
                let mut database = Database::open_catalog(directory.0.join("schema"))?;
                times.push(start.elapsed());
                if count_observation(&database.query("SELECT COUNT(*) FROM items")?)?.checksum
                    != u128::from(rows)
                {
                    return Err(message_error("history reopen result differs"));
                }
                database.checkpoint()?;
                database.close()?;
                if !checkpoint {
                    break;
                }
            }
            report(
                &format!("recovery_n{rows}_checkpoint{checkpoint}"),
                rows,
                8,
                &times,
                None,
                "retained-history-vs-checkpoint",
            )?;
        }
    }
    Ok(())
}
