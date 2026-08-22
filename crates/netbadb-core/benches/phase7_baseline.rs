//! Reproducible warm-cache performance baselines for Phase 7.

use std::env;
use std::error::Error;
use std::hint::black_box;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use netbadb_core::{Database, ExecutionResult, QueryResult};
use netbadb_inspect::{PlanNodeInspection, StatementInspection, StatementPlanInspection};
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_storage::HeapStorage;
use netbadb_types::{ColumnId, PhysicalType, RowId, ScalarValue, TableId};

type BenchResult<T> = Result<T, Box<dyn Error>>;

const ITEMS_TABLE_ID: TableId = TableId(1);
const ID_COLUMN_ID: ColumnId = ColumnId(1);
const TEAM_COLUMN_ID: ColumnId = ColumnId(2);
const BUCKET_COLUMN_ID: ColumnId = ColumnId(3);
const NULLABLE_COLUMN_ID: ColumnId = ColumnId(4);
const ACTIVE_COLUMN_ID: ColumnId = ColumnId(5);
const PAYLOAD_COLUMN_ID: ColumnId = ColumnId(6);
const LEFT_TABLE_ID: TableId = TableId(11);
const RIGHT_TABLE_ID: TableId = TableId(12);
const CHECKSUM_FACTOR: u128 = 1_000_003;

static FIXTURE_SEQUENCE: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BenchProfile {
    Quick,
    Full,
}

impl BenchProfile {
    fn from_environment() -> BenchResult<Self> {
        match env::var("NETBADB_BENCH_PROFILE") {
            Ok(value) if value == "quick" => Ok(Self::Quick),
            Ok(value) if value == "full" => Ok(Self::Full),
            Ok(value) => Err(message_error(format!(
                "unknown NETBADB_BENCH_PROFILE `{value}`; expected `quick` or `full`"
            ))),
            Err(env::VarError::NotPresent) => Ok(Self::Quick),
            Err(error) => Err(message_error(format!(
                "failed to read NETBADB_BENCH_PROFILE: {error}"
            ))),
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Quick => "quick",
            Self::Full => "full",
        }
    }

    const fn settings(self) -> ProfileSettings {
        match self {
            Self::Quick => ProfileSettings {
                small_rows: 250,
                medium_rows: 1_000,
                query_iterations: 5,
                query_warmup: 2,
                planner_iterations: 50,
                insert_samples: 3,
                join_small: 100,
                join_large: 300,
                join_iterations: 3,
                update_rows: 100,
            },
            Self::Full => ProfileSettings {
                small_rows: 1_000,
                medium_rows: 10_000,
                query_iterations: 12,
                query_warmup: 3,
                planner_iterations: 500,
                insert_samples: 5,
                join_small: 500,
                join_large: 1_000,
                join_iterations: 5,
                update_rows: 500,
            },
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct ProfileSettings {
    small_rows: u64,
    medium_rows: u64,
    query_iterations: usize,
    query_warmup: usize,
    planner_iterations: usize,
    insert_samples: usize,
    join_small: u64,
    join_large: u64,
    join_iterations: usize,
    update_rows: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Observation {
    rows: u64,
    checksum: u128,
}

#[derive(Debug)]
struct Measurement {
    scenario: String,
    rows: String,
    plan: String,
    operations_per_iteration: u64,
    durations: Vec<Duration>,
}

#[derive(Debug, Clone, Copy)]
struct Statistics {
    min_ns_per_op: u128,
    median_ns_per_op: u128,
    p95_ns_per_op: u128,
}

impl Statistics {
    fn from_durations(durations: &[Duration], operations_per_iteration: u64) -> BenchResult<Self> {
        if durations.is_empty() {
            return Err(message_error("measurement contains no durations"));
        }
        if operations_per_iteration == 0 {
            return Err(message_error("operations per iteration must be nonzero"));
        }
        let mut values = durations.iter().map(Duration::as_nanos).collect::<Vec<_>>();
        values.sort_unstable();
        let median = if values.len() % 2 == 0 {
            let upper = values.len() / 2;
            values[upper - 1]
                .checked_add(values[upper])
                .ok_or_else(|| message_error("median duration overflow"))?
                / 2
        } else {
            values[values.len() / 2]
        };
        let nearest_rank = values
            .len()
            .checked_mul(95)
            .and_then(|value| value.checked_add(99))
            .ok_or_else(|| message_error("p95 rank overflow"))?
            / 100;
        let divisor = u128::from(operations_per_iteration);
        Ok(Self {
            min_ns_per_op: values[0] / divisor,
            median_ns_per_op: median / divisor,
            p95_ns_per_op: values[nearest_rank - 1] / divisor,
        })
    }
}

struct FixturePaths {
    paths: Vec<PathBuf>,
    cleaned: bool,
}

impl FixturePaths {
    fn new(scenario: &str, count: usize) -> Self {
        let sequence = FIXTURE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let base = format!(
            "netbadb-phase7-{scenario}-{}-{sequence}",
            std::process::id()
        );
        let paths = (0..count)
            .map(|index| env::temp_dir().join(format!("{base}-{index}.ndb")))
            .collect();
        Self {
            paths,
            cleaned: false,
        }
    }

    fn path(&self, index: usize) -> &Path {
        &self.paths[index]
    }

    fn cleanup(mut self) -> BenchResult<()> {
        cleanup_paths(&self.paths)?;
        self.cleaned = true;
        Ok(())
    }
}

impl Drop for FixturePaths {
    fn drop(&mut self) {
        if !self.cleaned {
            let _ = cleanup_paths(&self.paths);
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum NullDistribution {
    Low,
    High,
}

impl NullDistribution {
    const fn is_null(self, id: u64) -> bool {
        match self {
            Self::Low => id % 100 == 0,
            Self::High => id % 2 == 0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Operator {
    SeqScan,
    IndexScan,
    RangeIndexScan,
    NestedLoopJoin,
    HashJoin,
    Filter,
    Sort,
    Project,
    Aggregate,
    Limit,
}

impl Operator {
    const fn name(self) -> &'static str {
        match self {
            Self::SeqScan => "SeqScan",
            Self::IndexScan => "IndexScan",
            Self::RangeIndexScan => "RangeIndexScan",
            Self::NestedLoopJoin => "NestedLoopJoin",
            Self::HashJoin => "HashJoin",
            Self::Filter => "Filter",
            Self::Sort => "Sort",
            Self::Project => "Project",
            Self::Aggregate => "Aggregate",
            Self::Limit => "Limit",
        }
    }
}

fn main() -> BenchResult<()> {
    let profile = BenchProfile::from_environment()?;
    let settings = profile.settings();
    let mut measurements = Vec::new();

    run_direct_heap_scan_scenarios(settings, &mut measurements)?;
    run_projection_attribution_scenarios(settings, &mut measurements)?;
    run_point_and_shape_scenarios(settings, &mut measurements)?;
    run_join_scenarios(settings, &mut measurements)?;
    run_insert_scenarios(settings, &mut measurements)?;
    run_update_scenario(settings, &mut measurements)?;
    run_planner_scenario(settings, &mut measurements)?;

    print_report(profile, settings, &measurements)?;
    Ok(())
}

fn run_projection_attribution_scenarios(
    settings: ProfileSettings,
    measurements: &mut Vec<Measurement>,
) -> BenchResult<()> {
    let rows = settings.medium_rows;
    run_attribution_query(
        "projection_id_only",
        rows,
        "SELECT id FROM items",
        &[Operator::Project, Operator::SeqScan],
        &[ID_COLUMN_ID],
        Observation {
            rows,
            checksum: arithmetic_sum(rows),
        },
        settings,
        ids_observation,
        measurements,
    )?;
    run_attribution_query(
        "projection_id_payload",
        rows,
        "SELECT id, payload FROM items",
        &[Operator::Project, Operator::SeqScan],
        &[ID_COLUMN_ID, PAYLOAD_COLUMN_ID],
        Observation {
            rows,
            checksum: arithmetic_sum(rows),
        },
        settings,
        id_payload_observation,
        measurements,
    )?;
    run_attribution_query(
        "projection_payload_only",
        rows,
        "SELECT payload FROM items",
        &[Operator::Project, Operator::SeqScan],
        &[PAYLOAD_COLUMN_ID],
        Observation {
            rows,
            checksum: arithmetic_sum(rows),
        },
        settings,
        payload_observation,
        measurements,
    )?;
    run_attribution_query(
        "projection_payload_id_reordered",
        rows,
        "SELECT payload, id FROM items",
        &[Operator::Project, Operator::SeqScan],
        &[ID_COLUMN_ID, PAYLOAD_COLUMN_ID],
        Observation {
            rows,
            checksum: arithmetic_sum(rows),
        },
        settings,
        payload_id_observation,
        measurements,
    )?;
    run_attribution_query(
        "projection_payload_twice",
        rows,
        "SELECT payload, payload FROM items",
        &[Operator::Project, Operator::SeqScan],
        &[PAYLOAD_COLUMN_ID],
        Observation {
            rows,
            checksum: arithmetic_sum(rows),
        },
        settings,
        duplicate_payload_observation,
        measurements,
    )?;
    run_attribution_query(
        "aggregate_count_star",
        rows,
        "SELECT COUNT(*) FROM items",
        &[Operator::Aggregate, Operator::SeqScan],
        &[],
        Observation {
            rows: 1,
            checksum: u128::from(rows),
        },
        settings,
        count_observation,
        measurements,
    )?;
    run_attribution_query(
        "aggregate_count_id",
        rows,
        "SELECT COUNT(id) FROM items",
        &[Operator::Aggregate, Operator::SeqScan],
        &[ID_COLUMN_ID],
        Observation {
            rows: 1,
            checksum: u128::from(rows),
        },
        settings,
        count_observation,
        measurements,
    )?;
    run_attribution_query(
        "aggregate_count_nullable",
        rows,
        "SELECT COUNT(nullable_key) FROM items",
        &[Operator::Aggregate, Operator::SeqScan],
        &[NULLABLE_COLUMN_ID],
        Observation {
            rows: 1,
            checksum: u128::from(low_non_null_count(rows)),
        },
        settings,
        count_observation,
        measurements,
    )?;
    run_attribution_query(
        "aggregate_count_payload",
        rows,
        "SELECT COUNT(payload) FROM items",
        &[Operator::Aggregate, Operator::SeqScan],
        &[PAYLOAD_COLUMN_ID],
        Observation {
            rows: 1,
            checksum: u128::from(rows),
        },
        settings,
        count_observation,
        measurements,
    )?;
    run_attribution_query(
        "aggregate_count_pair_control",
        rows,
        "SELECT COUNT(id), COUNT(payload) FROM items",
        &[Operator::Aggregate, Operator::SeqScan],
        &[ID_COLUMN_ID, PAYLOAD_COLUMN_ID],
        Observation {
            rows: 1,
            checksum: u128::from(rows) * 2,
        },
        settings,
        |result| count_values_observation(result, &[rows, rows]),
        measurements,
    )?;
    run_attribution_query(
        "aggregate_count_duplicate_payload",
        rows,
        "SELECT COUNT(payload), COUNT(payload) FROM items",
        &[Operator::Aggregate, Operator::SeqScan],
        &[PAYLOAD_COLUMN_ID],
        Observation {
            rows: 1,
            checksum: u128::from(rows) * 2,
        },
        settings,
        |result| count_values_observation(result, &[rows, rows]),
        measurements,
    )?;
    let nullable_count = low_non_null_count(rows);
    run_attribution_query(
        "aggregate_count_mixed_nullable",
        rows,
        "SELECT COUNT(id), COUNT(nullable_key), COUNT(payload) FROM items",
        &[Operator::Aggregate, Operator::SeqScan],
        &[ID_COLUMN_ID, NULLABLE_COLUMN_ID, PAYLOAD_COLUMN_ID],
        Observation {
            rows: 1,
            checksum: u128::from(rows) * 2 + u128::from(nullable_count),
        },
        settings,
        |result| count_values_observation(result, &[rows, nullable_count, rows]),
        measurements,
    )?;
    run_attribution_query(
        "aggregate_count_star_payload",
        rows,
        "SELECT COUNT(*), COUNT(payload) FROM items",
        &[Operator::Aggregate, Operator::SeqScan],
        &[PAYLOAD_COLUMN_ID],
        Observation {
            rows: 1,
            checksum: u128::from(rows) * 2,
        },
        settings,
        |result| count_values_observation(result, &[rows, rows]),
        measurements,
    )?;
    run_attribution_query(
        "aggregate_count_output_order",
        rows,
        "SELECT COUNT(payload), COUNT(*), COUNT(nullable_key), COUNT(payload), COUNT(id) FROM items",
        &[Operator::Aggregate, Operator::SeqScan],
        &[ID_COLUMN_ID, NULLABLE_COLUMN_ID, PAYLOAD_COLUMN_ID],
        Observation {
            rows: 1,
            checksum: u128::from(rows) * 4 + u128::from(nullable_count),
        },
        settings,
        |result| count_values_observation(result, &[rows, rows, nullable_count, rows, rows]),
        measurements,
    )?;
    run_attribution_query(
        "aggregate_count_star_pair_control",
        rows,
        "SELECT COUNT(*), COUNT(*) FROM items",
        &[Operator::Aggregate, Operator::SeqScan],
        &[],
        Observation {
            rows: 1,
            checksum: u128::from(rows) * 2,
        },
        settings,
        |result| count_values_observation(result, &[rows, rows]),
        measurements,
    )?;
    run_attribution_query(
        "aggregate_count_payload_filter_control",
        rows,
        "SELECT COUNT(payload) FROM items WHERE active = true",
        &[Operator::Aggregate, Operator::Filter, Operator::SeqScan],
        &[ACTIVE_COLUMN_ID, PAYLOAD_COLUMN_ID],
        Observation {
            rows: 1,
            checksum: u128::from(active_count(rows)),
        },
        settings,
        count_observation,
        measurements,
    )?;
    let filtered_count = active_count(rows);
    run_attribution_query(
        "aggregate_count_pair_filter_control",
        rows,
        "SELECT COUNT(id), COUNT(payload) FROM items WHERE active = true",
        &[Operator::Aggregate, Operator::Filter, Operator::SeqScan],
        &[ID_COLUMN_ID, ACTIVE_COLUMN_ID, PAYLOAD_COLUMN_ID],
        Observation {
            rows: 1,
            checksum: u128::from(filtered_count) * 2,
        },
        settings,
        |result| count_values_observation(result, &[filtered_count, filtered_count]),
        measurements,
    )?;
    let middle = rows / 2;
    run_attribution_query(
        "hidden_filter_payload",
        rows,
        &format!("SELECT id FROM items WHERE payload = 'payload-{middle:016}'"),
        &[Operator::Filter, Operator::Project, Operator::SeqScan],
        &[ID_COLUMN_ID, PAYLOAD_COLUMN_ID],
        Observation {
            rows: 1,
            checksum: u128::from(middle),
        },
        settings,
        ids_observation,
        measurements,
    )
}

#[allow(clippy::too_many_arguments)]
fn run_attribution_query(
    scenario: &str,
    rows: u64,
    sql: &str,
    required: &[Operator],
    expected_base_columns: &[ColumnId],
    expected: Observation,
    settings: ProfileSettings,
    observe: impl Fn(&QueryResult) -> BenchResult<Observation>,
    measurements: &mut Vec<Measurement>,
) -> BenchResult<()> {
    let (mut database, paths) = items_fixture(scenario, rows, &[], NullDistribution::Low, 4)?;
    let plan = inspect_plan(&database, scenario, sql, required, &[])?;
    inspect_base_scan_columns(&database, scenario, sql, expected_base_columns)?;
    let durations = measure_checked(
        scenario,
        settings.query_warmup,
        settings.query_iterations,
        expected,
        || database.query(sql).map_err(Into::into),
        observe,
    )?;
    database.close()?;
    paths.cleanup()?;
    measurements.push(Measurement {
        scenario: scenario.to_owned(),
        rows: rows.to_string(),
        plan,
        operations_per_iteration: 1,
        durations,
    });
    Ok(())
}

fn run_direct_heap_scan_scenarios(
    settings: ProfileSettings,
    measurements: &mut Vec<Measurement>,
) -> BenchResult<()> {
    run_direct_heap_scan(
        "heap_scan_join_shape",
        settings.join_large,
        join_table(LEFT_TABLE_ID, "join_rows"),
        2,
        |id| {
            let value = i64::try_from(id).map_err(|_| message_error("join ID exceeds i64"))?;
            Ok(vec![ScalarValue::Int64(value), ScalarValue::Int64(value)])
        },
        settings,
        measurements,
    )?;
    run_direct_heap_scan(
        "heap_scan_item_shape",
        settings.medium_rows,
        items_table(),
        6,
        |id| item_row(id, 4, NullDistribution::Low),
        settings,
        measurements,
    )?;
    run_direct_heap_payload_scan(settings, measurements)
}

fn run_direct_heap_payload_scan(
    settings: ProfileSettings,
    measurements: &mut Vec<Measurement>,
) -> BenchResult<()> {
    let scenario = "heap_scan_payload_only";
    let rows = settings.medium_rows;
    let paths = FixturePaths::new(scenario, 1);
    let mut storage = HeapStorage::create(paths.path(0), items_table())?;
    let mut transaction = storage.begin_transaction()?;
    for id in 0..rows {
        storage.insert_in(&mut transaction, &item_row(id, 4, NullDistribution::Low)?)?;
    }
    transaction.commit()?;

    let expected = Observation {
        rows,
        checksum: arithmetic_sum(rows),
    };
    let durations = measure_checked(
        scenario,
        settings.query_warmup,
        settings.query_iterations,
        expected,
        || {
            storage
                .scan_columns(&[PAYLOAD_COLUMN_ID])
                .map_err(Into::into)
        },
        |result| heap_payload_observation(result),
    )?;
    storage.close()?;
    paths.cleanup()?;
    measurements.push(Measurement {
        scenario: scenario.to_owned(),
        rows: rows.to_string(),
        plan: "DirectProjectedHeapScan".to_owned(),
        operations_per_iteration: 1,
        durations,
    });
    Ok(())
}

fn run_direct_heap_scan(
    scenario: &str,
    rows: u64,
    table: TableDef,
    expected_columns: usize,
    mut row: impl FnMut(u64) -> BenchResult<Vec<ScalarValue>>,
    settings: ProfileSettings,
    measurements: &mut Vec<Measurement>,
) -> BenchResult<()> {
    let paths = FixturePaths::new(scenario, 1);
    let mut storage = HeapStorage::create(paths.path(0), table)?;
    let mut transaction = storage.begin_transaction()?;
    for id in 0..rows {
        storage.insert_in(&mut transaction, &row(id)?)?;
    }
    transaction.commit()?;

    let expected = Observation {
        rows,
        checksum: arithmetic_sum(rows),
    };
    let durations = measure_checked(
        scenario,
        settings.query_warmup,
        settings.query_iterations,
        expected,
        || storage.scan().map_err(Into::into),
        |result| heap_scan_observation(result, expected_columns),
    )?;
    storage.close()?;
    paths.cleanup()?;
    measurements.push(Measurement {
        scenario: scenario.to_owned(),
        rows: rows.to_string(),
        plan: "DirectHeapScan".to_owned(),
        operations_per_iteration: 1,
        durations,
    });
    Ok(())
}

fn run_point_and_shape_scenarios(
    settings: ProfileSettings,
    measurements: &mut Vec<Measurement>,
) -> BenchResult<()> {
    let rows = settings.medium_rows;
    let middle = rows / 2;
    let point_sql = format!("SELECT id FROM items WHERE id = {middle}");

    run_items_query(
        "point_seq",
        rows,
        &[],
        NullDistribution::Low,
        &point_sql,
        &[Operator::Filter, Operator::SeqScan],
        &[Operator::IndexScan, Operator::RangeIndexScan],
        Observation {
            rows: 1,
            checksum: u128::from(middle),
        },
        settings,
        ids_observation,
        measurements,
    )?;
    run_items_query(
        "point_index",
        rows,
        &[ID_COLUMN_ID],
        NullDistribution::Low,
        &point_sql,
        &[Operator::Filter, Operator::IndexScan],
        &[Operator::SeqScan],
        Observation {
            rows: 1,
            checksum: u128::from(middle),
        },
        settings,
        ids_observation,
        measurements,
    )?;

    let team_expected = expected_modulo_ids(rows, 4, 0);
    run_items_query(
        "duplicate_indexed_equality",
        rows,
        &[TEAM_COLUMN_ID],
        NullDistribution::Low,
        "SELECT id FROM items WHERE team_id = 0",
        &[Operator::Filter, Operator::SeqScan],
        &[Operator::IndexScan, Operator::RangeIndexScan],
        team_expected,
        settings,
        ids_observation,
        measurements,
    )?;
    run_items_query(
        "selective_secondary_index",
        rows,
        &[BUCKET_COLUMN_ID],
        NullDistribution::Low,
        &format!("SELECT id FROM items WHERE bucket_id = {middle}"),
        &[Operator::Filter, Operator::IndexScan],
        &[Operator::SeqScan],
        Observation {
            rows: 1,
            checksum: u128::from(middle),
        },
        settings,
        ids_observation,
        measurements,
    )?;

    for (scenario, distribution, expected_operator) in [
        (
            "is_null_low_rate",
            NullDistribution::Low,
            Operator::IndexScan,
        ),
        (
            "is_null_high_rate",
            NullDistribution::High,
            Operator::SeqScan,
        ),
    ] {
        let expected = expected_null_ids(rows, distribution);
        let forbidden = if expected_operator == Operator::IndexScan {
            [Operator::SeqScan]
        } else {
            [Operator::IndexScan]
        };
        run_items_query(
            scenario,
            rows,
            &[NULLABLE_COLUMN_ID],
            distribution,
            "SELECT id FROM items WHERE nullable_key IS NULL",
            &[Operator::Filter, expected_operator],
            &forbidden,
            expected,
            settings,
            ids_observation,
            measurements,
        )?;
    }

    let one_percent_start = rows / 2;
    let one_percent_end = one_percent_start + (rows / 100).max(1);
    run_items_query(
        "range_one_percent",
        rows,
        &[ID_COLUMN_ID],
        NullDistribution::Low,
        &format!("SELECT id FROM items WHERE id >= {one_percent_start} AND id < {one_percent_end}"),
        &[Operator::Filter, Operator::RangeIndexScan],
        &[Operator::SeqScan, Operator::IndexScan],
        expected_range_ids(one_percent_start, one_percent_end),
        settings,
        ids_observation,
        measurements,
    )?;
    let half_start = rows / 4;
    let half_end = half_start + rows / 2;
    run_items_query(
        "range_fifty_percent",
        rows,
        &[ID_COLUMN_ID],
        NullDistribution::Low,
        &format!("SELECT id FROM items WHERE id >= {half_start} AND id < {half_end}"),
        &[Operator::Filter, Operator::SeqScan],
        &[Operator::IndexScan, Operator::RangeIndexScan],
        expected_range_ids(half_start, half_end),
        settings,
        ids_observation,
        measurements,
    )?;
    run_items_query(
        "range_one_sided",
        rows,
        &[ID_COLUMN_ID],
        NullDistribution::Low,
        &format!("SELECT id FROM items WHERE id >= {one_percent_start}"),
        &[Operator::Filter, Operator::SeqScan],
        &[Operator::IndexScan, Operator::RangeIndexScan],
        expected_range_ids(one_percent_start, rows),
        settings,
        ids_observation,
        measurements,
    )?;

    let order_limit = rows.min(20);
    run_items_query(
        "order_by_limit",
        rows,
        &[],
        NullDistribution::Low,
        &format!("SELECT id FROM items ORDER BY team_id LIMIT {order_limit}"),
        &[Operator::SeqScan, Operator::Sort, Operator::Limit],
        &[],
        Observation {
            rows: order_limit,
            checksum: u128::from(order_limit),
        },
        settings,
        |result| ordered_limit_observation(result, rows, 4),
        measurements,
    )?;

    run_group_query("group_by_low_cardinality", rows, 4, settings, measurements)?;
    run_group_query(
        "group_by_higher_cardinality",
        rows,
        (rows / 100).max(10),
        settings,
        measurements,
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn run_items_query(
    scenario: &str,
    rows: u64,
    indexes: &[ColumnId],
    null_distribution: NullDistribution,
    sql: &str,
    required: &[Operator],
    forbidden: &[Operator],
    expected: Observation,
    settings: ProfileSettings,
    observe: impl Fn(&QueryResult) -> BenchResult<Observation>,
    measurements: &mut Vec<Measurement>,
) -> BenchResult<()> {
    let (mut database, paths) = items_fixture(scenario, rows, indexes, null_distribution, 4)?;
    let plan = inspect_plan(&database, scenario, sql, required, forbidden)?;
    let durations = measure_checked(
        scenario,
        settings.query_warmup,
        settings.query_iterations,
        expected,
        || database.query(sql).map_err(Into::into),
        observe,
    )?;
    database.close()?;
    paths.cleanup()?;
    measurements.push(Measurement {
        scenario: scenario.to_owned(),
        rows: rows.to_string(),
        plan,
        operations_per_iteration: 1,
        durations,
    });
    Ok(())
}

fn run_group_query(
    scenario: &str,
    rows: u64,
    cardinality: u64,
    settings: ProfileSettings,
    measurements: &mut Vec<Measurement>,
) -> BenchResult<()> {
    let (mut database, paths) =
        items_fixture(scenario, rows, &[], NullDistribution::Low, cardinality)?;
    let sql = "SELECT team_id, COUNT(*) FROM items GROUP BY team_id";
    let plan = inspect_plan(
        &database,
        scenario,
        sql,
        &[Operator::SeqScan, Operator::Aggregate],
        &[],
    )?;
    let expected = expected_groups(rows, cardinality);
    let durations = measure_checked(
        scenario,
        settings.query_warmup,
        settings.query_iterations,
        expected,
        || database.query(sql).map_err(Into::into),
        group_observation,
    )?;
    database.close()?;
    paths.cleanup()?;
    measurements.push(Measurement {
        scenario: scenario.to_owned(),
        rows: rows.to_string(),
        plan,
        operations_per_iteration: 1,
        durations,
    });
    Ok(())
}

fn run_join_scenarios(
    settings: ProfileSettings,
    measurements: &mut Vec<Measurement>,
) -> BenchResult<()> {
    for (scale_name, rows) in [
        ("small", settings.join_small),
        ("large", settings.join_large),
    ] {
        run_join_query(
            JoinScenario {
                name: &format!("join_unique_{scale_name}"),
                rows,
                cardinality: rows,
                left_key_offset: 0,
                right_key_offset: 0,
                sql: "SELECT l.id FROM left_rows l JOIN right_rows r ON l.join_key = r.join_key",
                expected: expected_join(rows, rows),
                operator: Operator::HashJoin,
                wide: false,
            },
            settings,
            measurements,
        )?;
        run_join_query(
            JoinScenario {
                name: &format!("join_duplicate_{scale_name}"),
                rows,
                cardinality: (rows / 10).max(1),
                left_key_offset: 0,
                right_key_offset: 0,
                sql: "SELECT l.id FROM left_rows l JOIN right_rows r ON l.join_key = r.join_key",
                expected: expected_join(rows, (rows / 10).max(1)),
                operator: Operator::HashJoin,
                wide: false,
            },
            settings,
            measurements,
        )?;
        run_join_query(
            JoinScenario {
                name: &format!("join_none_{scale_name}"),
                rows,
                cardinality: rows,
                left_key_offset: 0,
                right_key_offset: rows,
                sql: "SELECT l.id FROM left_rows l JOIN right_rows r ON l.join_key = r.join_key",
                expected: Observation {
                    rows: 0,
                    checksum: 0,
                },
                operator: Operator::HashJoin,
                wide: false,
            },
            settings,
            measurements,
        )?;
        run_join_query(
            JoinScenario {
                name: &format!("join_non_equi_none_{scale_name}"),
                rows,
                cardinality: rows,
                left_key_offset: 0,
                right_key_offset: rows,
                sql: "SELECT l.id FROM left_rows l JOIN right_rows r ON l.join_key > r.join_key",
                expected: Observation {
                    rows: 0,
                    checksum: 0,
                },
                operator: Operator::NestedLoopJoin,
                wide: false,
            },
            settings,
            measurements,
        )?;
        run_join_query(
            JoinScenario {
                name: &format!("join_non_equi_wide_none_{scale_name}"),
                rows,
                cardinality: rows,
                left_key_offset: 0,
                right_key_offset: rows,
                sql: "SELECT l.id FROM left_rows l JOIN right_rows r ON l.join_key > r.join_key",
                expected: Observation {
                    rows: 0,
                    checksum: 0,
                },
                operator: Operator::NestedLoopJoin,
                wide: true,
            },
            settings,
            measurements,
        )?;
        run_join_query(
            JoinScenario {
                name: &format!("join_non_equi_partial_none_{scale_name}"),
                rows,
                cardinality: rows,
                left_key_offset: 0,
                right_key_offset: rows / 2,
                sql: "SELECT l.id FROM left_rows l JOIN right_rows r ON l.join_key > r.join_key AND l.id < 0",
                expected: Observation {
                    rows: 0,
                    checksum: 0,
                },
                operator: Operator::NestedLoopJoin,
                wide: false,
            },
            settings,
            measurements,
        )?;
        run_join_query(
            JoinScenario {
                name: &format!("join_non_equi_no_prune_none_{scale_name}"),
                rows,
                cardinality: rows,
                left_key_offset: rows,
                right_key_offset: 0,
                sql: "SELECT l.id FROM left_rows l JOIN right_rows r ON l.join_key > r.join_key AND l.id < 0",
                expected: Observation {
                    rows: 0,
                    checksum: 0,
                },
                operator: Operator::NestedLoopJoin,
                wide: false,
            },
            settings,
            measurements,
        )?;
        run_join_query(
            JoinScenario {
                name: &format!("join_non_equi_dense_none_{scale_name}"),
                rows,
                cardinality: rows,
                left_key_offset: rows * 3 / 4,
                right_key_offset: 0,
                sql: "SELECT l.id FROM left_rows l JOIN right_rows r ON l.join_key > r.join_key AND l.id < 0",
                expected: Observation {
                    rows: 0,
                    checksum: 0,
                },
                operator: Operator::NestedLoopJoin,
                wide: false,
            },
            settings,
            measurements,
        )?;
        run_text_join_query(
            &format!("join_non_equi_text_none_{scale_name}"),
            rows,
            settings,
            measurements,
        )?;
    }
    Ok(())
}

struct JoinScenario<'a> {
    name: &'a str,
    rows: u64,
    cardinality: u64,
    left_key_offset: u64,
    right_key_offset: u64,
    sql: &'static str,
    expected: Observation,
    operator: Operator,
    wide: bool,
}

fn run_join_query(
    scenario: JoinScenario<'_>,
    settings: ProfileSettings,
    measurements: &mut Vec<Measurement>,
) -> BenchResult<()> {
    let paths = FixturePaths::new(scenario.name, 2);
    let table = if scenario.wide {
        wide_join_table
    } else {
        join_table
    };
    let tables = vec![
        (
            paths.path(0).to_path_buf(),
            table(LEFT_TABLE_ID, "left_rows"),
        ),
        (
            paths.path(1).to_path_buf(),
            table(RIGHT_TABLE_ID, "right_rows"),
        ),
    ];
    let mut database = Database::create_tables(tables)?;
    if scenario.wide {
        load_wide_join_rows(
            &mut database,
            LEFT_TABLE_ID,
            scenario.rows,
            scenario.cardinality,
            scenario.left_key_offset,
        )?;
        load_wide_join_rows(
            &mut database,
            RIGHT_TABLE_ID,
            scenario.rows,
            scenario.cardinality,
            scenario.right_key_offset,
        )?;
    } else {
        load_join_rows(
            &mut database,
            LEFT_TABLE_ID,
            scenario.rows,
            scenario.cardinality,
            scenario.left_key_offset,
        )?;
        load_join_rows(
            &mut database,
            RIGHT_TABLE_ID,
            scenario.rows,
            scenario.cardinality,
            scenario.right_key_offset,
        )?;
    }
    database.analyze(LEFT_TABLE_ID)?;
    database.analyze(RIGHT_TABLE_ID)?;
    let plan = inspect_plan(
        &database,
        scenario.name,
        scenario.sql,
        &[scenario.operator, Operator::SeqScan],
        &[Operator::IndexScan],
    )?;
    let durations = measure_checked(
        scenario.name,
        settings.query_warmup,
        settings.join_iterations,
        scenario.expected,
        || database.query(scenario.sql).map_err(Into::into),
        ids_observation,
    )?;
    database.close()?;
    paths.cleanup()?;
    measurements.push(Measurement {
        scenario: scenario.name.to_owned(),
        rows: format!("{}x{}", scenario.rows, scenario.rows),
        plan,
        operations_per_iteration: 1,
        durations,
    });
    Ok(())
}

fn run_text_join_query(
    scenario: &str,
    rows: u64,
    settings: ProfileSettings,
    measurements: &mut Vec<Measurement>,
) -> BenchResult<()> {
    let paths = FixturePaths::new(scenario, 2);
    let tables = vec![
        (
            paths.path(0).to_path_buf(),
            text_join_table(LEFT_TABLE_ID, "left_rows"),
        ),
        (
            paths.path(1).to_path_buf(),
            text_join_table(RIGHT_TABLE_ID, "right_rows"),
        ),
    ];
    let mut database = Database::create_tables(tables)?;
    load_text_join_rows(&mut database, LEFT_TABLE_ID, rows, 'L')?;
    load_text_join_rows(&mut database, RIGHT_TABLE_ID, rows, 'R')?;
    database.analyze(LEFT_TABLE_ID)?;
    database.analyze(RIGHT_TABLE_ID)?;
    let sql = "SELECT l.id FROM left_rows l JOIN right_rows r ON l.join_key > r.join_key";
    let plan = inspect_plan(
        &database,
        scenario,
        sql,
        &[Operator::NestedLoopJoin, Operator::SeqScan],
        &[Operator::HashJoin, Operator::IndexScan],
    )?;
    let expected = Observation {
        rows: 0,
        checksum: 0,
    };
    let durations = measure_checked(
        scenario,
        settings.query_warmup,
        settings.join_iterations,
        expected,
        || database.query(sql).map_err(Into::into),
        ids_observation,
    )?;
    database.close()?;
    paths.cleanup()?;
    measurements.push(Measurement {
        scenario: scenario.to_owned(),
        rows: format!("{rows}x{rows}"),
        plan,
        operations_per_iteration: 1,
        durations,
    });
    Ok(())
}

fn run_insert_scenarios(
    settings: ProfileSettings,
    measurements: &mut Vec<Measurement>,
) -> BenchResult<()> {
    for index_count in 0..=2_u32 {
        let scenario = format!("insert_{index_count}_indexes");
        let mut durations = Vec::with_capacity(settings.insert_samples);
        for sample in 0..settings.insert_samples {
            let sample_name = format!("{scenario}_{sample}");
            let paths = FixturePaths::new(&sample_name, 1);
            let mut database = Database::create(paths.path(0), items_table())?;
            if index_count >= 1 {
                database.create_index(ITEMS_TABLE_ID, ID_COLUMN_ID)?;
            }
            if index_count >= 2 {
                database.create_index(ITEMS_TABLE_ID, TEAM_COLUMN_ID)?;
            }

            let started = Instant::now();
            let mut transaction = database.begin_transaction_for(ITEMS_TABLE_ID)?;
            for id in 0..settings.small_rows {
                let row = item_row(id, 4, NullDistribution::Low)?;
                database.insert_into_in(ITEMS_TABLE_ID, &mut transaction, &row)?;
            }
            transaction.commit()?;
            let elapsed = started.elapsed();
            black_box(settings.small_rows);
            durations.push(elapsed);

            let count = database.query("SELECT COUNT(*) FROM items")?;
            let observation = count_observation(&count)?;
            require_observation(
                &scenario,
                Observation {
                    rows: 1,
                    checksum: u128::from(settings.small_rows),
                },
                observation,
            )?;
            database.close()?;
            paths.cleanup()?;
        }
        measurements.push(Measurement {
            scenario,
            rows: settings.small_rows.to_string(),
            plan: format!("DirectInsert/{index_count} indexes"),
            operations_per_iteration: settings.small_rows,
            durations,
        });
    }
    Ok(())
}

fn run_update_scenario(
    settings: ProfileSettings,
    measurements: &mut Vec<Measurement>,
) -> BenchResult<()> {
    let rows = settings.update_rows;
    let (mut database, paths) = items_fixture(
        "update_indexed_key",
        rows,
        &[ID_COLUMN_ID, BUCKET_COLUMN_ID],
        NullDistribution::Low,
        4,
    )?;
    let first_sql = format!("UPDATE items SET bucket_id = {} WHERE id = 0", rows);
    let plan = inspect_plan(
        &database,
        "update_indexed_key",
        &first_sql,
        &[Operator::Filter, Operator::IndexScan],
        &[Operator::SeqScan],
    )?;
    let statements = (0..rows)
        .map(|id| format!("UPDATE items SET bucket_id = {} WHERE id = {id}", rows + id))
        .collect::<Vec<_>>();
    let mut durations = Vec::with_capacity(statements.len());
    for statement in statements {
        let started = Instant::now();
        let result = database.execute(&statement)?;
        let elapsed = started.elapsed();
        let affected = match result {
            ExecutionResult::AffectedRows(affected) => affected,
            ExecutionResult::Query(_) => {
                return Err(message_error("UPDATE unexpectedly returned query rows"));
            }
        };
        if affected != 1 {
            return Err(message_error(format!(
                "UPDATE affected {affected} rows; expected 1"
            )));
        }
        black_box(affected);
        durations.push(elapsed);
    }
    let updated = database.query(&format!(
        "SELECT id FROM items WHERE bucket_id >= {rows} AND bucket_id < {}",
        rows * 2
    ))?;
    require_observation(
        "update_indexed_key",
        expected_range_ids(0, rows),
        ids_observation(&updated)?,
    )?;
    database.close()?;
    paths.cleanup()?;
    measurements.push(Measurement {
        scenario: "update_indexed_key".to_owned(),
        rows: rows.to_string(),
        plan,
        operations_per_iteration: 1,
        durations,
    });
    Ok(())
}

fn run_planner_scenario(
    settings: ProfileSettings,
    measurements: &mut Vec<Measurement>,
) -> BenchResult<()> {
    let paths = FixturePaths::new("inspect_compile_plan", 1);
    let database = Database::create(paths.path(0), items_table())?;
    let sql = "SELECT id FROM items WHERE id = 1";
    let plan = inspect_plan(
        &database,
        "inspect_compile_plan",
        sql,
        &[Operator::Filter, Operator::SeqScan],
        &[Operator::IndexScan],
    )?;
    let expected = inspect_observation(&database.inspect_statement(sql)?)?;
    let durations = measure_checked(
        "inspect_compile_plan",
        settings.query_warmup,
        settings.planner_iterations,
        expected,
        || database.inspect_statement(sql).map_err(Into::into),
        inspect_observation,
    )?;
    database.close()?;
    paths.cleanup()?;
    measurements.push(Measurement {
        scenario: "inspect_compile_plan".to_owned(),
        rows: "0".to_owned(),
        plan,
        operations_per_iteration: 1,
        durations,
    });
    Ok(())
}

fn measure_checked<T>(
    scenario: &str,
    warmup: usize,
    iterations: usize,
    expected: Observation,
    mut operation: impl FnMut() -> BenchResult<T>,
    mut observe: impl FnMut(&T) -> BenchResult<Observation>,
) -> BenchResult<Vec<Duration>> {
    for _ in 0..warmup {
        let result = operation().map_err(|error| scenario_error(scenario, error))?;
        let observed = observe(&result).map_err(|error| scenario_error(scenario, error))?;
        require_observation(scenario, expected, observed)?;
        black_box(observed);
    }
    let mut durations = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let started = Instant::now();
        let result = operation().map_err(|error| scenario_error(scenario, error))?;
        let elapsed = started.elapsed();
        let observed = observe(&result).map_err(|error| scenario_error(scenario, error))?;
        require_observation(scenario, expected, observed)?;
        black_box(observed);
        durations.push(elapsed);
    }
    Ok(durations)
}

fn items_fixture(
    scenario: &str,
    rows: u64,
    indexes: &[ColumnId],
    null_distribution: NullDistribution,
    team_cardinality: u64,
) -> BenchResult<(Database, FixturePaths)> {
    let paths = FixturePaths::new(scenario, 1);
    let mut database = Database::create(paths.path(0), items_table())?;
    load_item_rows(&mut database, rows, team_cardinality, null_distribution)?;
    for column_id in indexes {
        database.create_index(ITEMS_TABLE_ID, *column_id)?;
    }
    if !indexes.is_empty() {
        database.analyze(ITEMS_TABLE_ID)?;
    }
    Ok((database, paths))
}

fn items_table() -> TableDef {
    TableDef::new(
        ITEMS_TABLE_ID,
        "items",
        vec![
            ColumnDef::new(ID_COLUMN_ID, "id", TypeSpec::Physical(PhysicalType::Int64)),
            ColumnDef::new(
                TEAM_COLUMN_ID,
                "team_id",
                TypeSpec::Physical(PhysicalType::Int64),
            ),
            ColumnDef::new(
                BUCKET_COLUMN_ID,
                "bucket_id",
                TypeSpec::Physical(PhysicalType::Int64),
            ),
            ColumnDef::new(
                NULLABLE_COLUMN_ID,
                "nullable_key",
                TypeSpec::Physical(PhysicalType::Int64),
            )
            .nullable(true),
            ColumnDef::new(
                ColumnId(5),
                "active",
                TypeSpec::Physical(PhysicalType::Bool),
            ),
            ColumnDef::new(
                ColumnId(6),
                "payload",
                TypeSpec::Physical(PhysicalType::Text),
            ),
        ],
    )
}

fn join_table(table_id: TableId, name: &str) -> TableDef {
    TableDef::new(
        table_id,
        name,
        vec![
            ColumnDef::new(ID_COLUMN_ID, "id", TypeSpec::Physical(PhysicalType::Int64)),
            ColumnDef::new(
                ColumnId(2),
                "join_key",
                TypeSpec::Physical(PhysicalType::Int64),
            ),
        ],
    )
}

fn wide_join_table(table_id: TableId, name: &str) -> TableDef {
    let mut columns = Vec::with_capacity(8);
    columns.push(ColumnDef::new(
        ID_COLUMN_ID,
        "id",
        TypeSpec::Physical(PhysicalType::Int64),
    ));
    for column in 1_u32..=6 {
        columns.push(ColumnDef::new(
            ColumnId(column + 1),
            format!("pad{column}"),
            TypeSpec::Physical(PhysicalType::Int64),
        ));
    }
    columns.push(ColumnDef::new(
        ColumnId(8),
        "join_key",
        TypeSpec::Physical(PhysicalType::Int64),
    ));
    TableDef::new(table_id, name, columns)
}

fn text_join_table(table_id: TableId, name: &str) -> TableDef {
    TableDef::new(
        table_id,
        name,
        vec![
            ColumnDef::new(ID_COLUMN_ID, "id", TypeSpec::Physical(PhysicalType::Int64)),
            ColumnDef::new(
                ColumnId(2),
                "join_key",
                TypeSpec::Physical(PhysicalType::Text),
            ),
        ],
    )
}

fn load_item_rows(
    database: &mut Database,
    rows: u64,
    team_cardinality: u64,
    null_distribution: NullDistribution,
) -> BenchResult<()> {
    let mut transaction = database.begin_transaction_for(ITEMS_TABLE_ID)?;
    for id in 0..rows {
        let row = item_row(id, team_cardinality, null_distribution)?;
        database.insert_into_in(ITEMS_TABLE_ID, &mut transaction, &row)?;
    }
    transaction.commit()?;
    Ok(())
}

fn item_row(
    id: u64,
    team_cardinality: u64,
    null_distribution: NullDistribution,
) -> BenchResult<Vec<ScalarValue>> {
    let id_value = i64::try_from(id).map_err(|_| message_error("fixture ID exceeds i64"))?;
    let team = i64::try_from(id % team_cardinality)
        .map_err(|_| message_error("fixture team ID exceeds i64"))?;
    let nullable = if null_distribution.is_null(id) {
        ScalarValue::Null
    } else {
        ScalarValue::Int64(id_value)
    };
    Ok(vec![
        ScalarValue::Int64(id_value),
        ScalarValue::Int64(team),
        ScalarValue::Int64(id_value),
        nullable,
        ScalarValue::Bool(id % 3 == 0),
        ScalarValue::Text(format!("payload-{id:016}")),
    ])
}

fn load_join_rows(
    database: &mut Database,
    table_id: TableId,
    rows: u64,
    cardinality: u64,
    key_offset: u64,
) -> BenchResult<()> {
    let mut transaction = database.begin_transaction_for(table_id)?;
    for id in 0..rows {
        let id_value = i64::try_from(id).map_err(|_| message_error("join ID exceeds i64"))?;
        let key = id
            .checked_rem(cardinality)
            .and_then(|key| key.checked_add(key_offset))
            .ok_or_else(|| message_error("join key overflow"))?;
        let key = i64::try_from(key).map_err(|_| message_error("join key exceeds i64"))?;
        database.insert_into_in(
            table_id,
            &mut transaction,
            &[ScalarValue::Int64(id_value), ScalarValue::Int64(key)],
        )?;
    }
    transaction.commit()?;
    Ok(())
}

fn load_wide_join_rows(
    database: &mut Database,
    table_id: TableId,
    rows: u64,
    cardinality: u64,
    key_offset: u64,
) -> BenchResult<()> {
    let mut transaction = database.begin_transaction_for(table_id)?;
    for id in 0..rows {
        let id_value = i64::try_from(id).map_err(|_| message_error("join ID exceeds i64"))?;
        let key = id
            .checked_rem(cardinality)
            .and_then(|key| key.checked_add(key_offset))
            .ok_or_else(|| message_error("join key overflow"))?;
        let key = i64::try_from(key).map_err(|_| message_error("join key exceeds i64"))?;
        let mut values = vec![ScalarValue::Int64(id_value); 7];
        values.push(ScalarValue::Int64(key));
        database.insert_into_in(table_id, &mut transaction, &values)?;
    }
    transaction.commit()?;
    Ok(())
}

fn load_text_join_rows(
    database: &mut Database,
    table_id: TableId,
    rows: u64,
    prefix: char,
) -> BenchResult<()> {
    let mut transaction = database.begin_transaction_for(table_id)?;
    for id in 0..rows {
        let id_value = i64::try_from(id).map_err(|_| message_error("join ID exceeds i64"))?;
        database.insert_into_in(
            table_id,
            &mut transaction,
            &[
                ScalarValue::Int64(id_value),
                ScalarValue::Text(format!("{prefix}-{id:020}")),
            ],
        )?;
    }
    transaction.commit()?;
    Ok(())
}

fn inspect_plan(
    database: &Database,
    scenario: &str,
    sql: &str,
    required: &[Operator],
    forbidden: &[Operator],
) -> BenchResult<String> {
    let inspection = database.inspect_statement(sql)?;
    let root = statement_root(&inspection)?;
    for operator in required {
        if !contains_operator(root, *operator) {
            return Err(message_error(format!(
                "scenario `{scenario}` plan is missing required operator {}: {}",
                operator.name(),
                plan_label(root)
            )));
        }
    }
    for operator in forbidden {
        if contains_operator(root, *operator) {
            return Err(message_error(format!(
                "scenario `{scenario}` plan unexpectedly contains operator {}: {}",
                operator.name(),
                plan_label(root)
            )));
        }
    }
    Ok(plan_label(root))
}

fn inspect_base_scan_columns(
    database: &Database,
    scenario: &str,
    sql: &str,
    expected: &[ColumnId],
) -> BenchResult<()> {
    let inspection = database.inspect_statement(sql)?;
    let root = query_root(&inspection)?;
    let mut scans = Vec::new();
    collect_base_scan_columns(root, &mut scans);
    let [actual] = scans.as_slice() else {
        return Err(message_error(format!(
            "scenario `{scenario}` expected exactly one base scan, found {}",
            scans.len()
        )));
    };
    if actual.as_slice() != expected {
        return Err(message_error(format!(
            "scenario `{scenario}` base scan columns were {actual:?}; expected {expected:?}"
        )));
    }
    Ok(())
}

fn collect_base_scan_columns(plan: &PlanNodeInspection, scans: &mut Vec<Vec<ColumnId>>) {
    match plan {
        PlanNodeInspection::SeqScan { columns, .. }
        | PlanNodeInspection::IndexScan { columns, .. }
        | PlanNodeInspection::RangeIndexScan { columns, .. } => {
            scans.push(columns.iter().map(|column| column.column_id).collect());
        }
        PlanNodeInspection::NestedLoopJoin { left, right, .. }
        | PlanNodeInspection::HashJoin { left, right, .. } => {
            collect_base_scan_columns(left, scans);
            collect_base_scan_columns(right, scans);
        }
        PlanNodeInspection::Filter { input, .. }
        | PlanNodeInspection::Sort { input, .. }
        | PlanNodeInspection::Project { input, .. }
        | PlanNodeInspection::Aggregate { input, .. }
        | PlanNodeInspection::Limit { input, .. } => collect_base_scan_columns(input, scans),
    }
}

fn statement_root(inspection: &StatementInspection) -> BenchResult<&PlanNodeInspection> {
    match &inspection.plan {
        StatementPlanInspection::Query { root } => Ok(root),
        StatementPlanInspection::Update { input, .. }
        | StatementPlanInspection::Delete { input, .. } => Ok(input),
        StatementPlanInspection::Insert { .. } => {
            Err(message_error("INSERT inspection has no input plan"))
        }
    }
}

fn query_root(inspection: &StatementInspection) -> BenchResult<&PlanNodeInspection> {
    match &inspection.plan {
        StatementPlanInspection::Query { root } => Ok(root),
        StatementPlanInspection::Insert { .. }
        | StatementPlanInspection::Update { .. }
        | StatementPlanInspection::Delete { .. } => {
            Err(message_error("expected query inspection plan"))
        }
    }
}

fn contains_operator(plan: &PlanNodeInspection, target: Operator) -> bool {
    operator(plan) == target
        || match plan {
            PlanNodeInspection::NestedLoopJoin { left, right, .. }
            | PlanNodeInspection::HashJoin { left, right, .. } => {
                contains_operator(left, target) || contains_operator(right, target)
            }
            PlanNodeInspection::Filter { input, .. }
            | PlanNodeInspection::Sort { input, .. }
            | PlanNodeInspection::Project { input, .. }
            | PlanNodeInspection::Aggregate { input, .. }
            | PlanNodeInspection::Limit { input, .. } => contains_operator(input, target),
            PlanNodeInspection::SeqScan { .. }
            | PlanNodeInspection::IndexScan { .. }
            | PlanNodeInspection::RangeIndexScan { .. } => false,
        }
}

const fn operator(plan: &PlanNodeInspection) -> Operator {
    match plan {
        PlanNodeInspection::SeqScan { .. } => Operator::SeqScan,
        PlanNodeInspection::IndexScan { .. } => Operator::IndexScan,
        PlanNodeInspection::RangeIndexScan { .. } => Operator::RangeIndexScan,
        PlanNodeInspection::NestedLoopJoin { .. } => Operator::NestedLoopJoin,
        PlanNodeInspection::HashJoin { .. } => Operator::HashJoin,
        PlanNodeInspection::Filter { .. } => Operator::Filter,
        PlanNodeInspection::Sort { .. } => Operator::Sort,
        PlanNodeInspection::Project { .. } => Operator::Project,
        PlanNodeInspection::Aggregate { .. } => Operator::Aggregate,
        PlanNodeInspection::Limit { .. } => Operator::Limit,
    }
}

fn plan_label(plan: &PlanNodeInspection) -> String {
    let mut operators = Vec::new();
    collect_operators(plan, &mut operators);
    operators.join(">")
}

fn collect_operators(plan: &PlanNodeInspection, operators: &mut Vec<&'static str>) {
    operators.push(operator(plan).name());
    match plan {
        PlanNodeInspection::NestedLoopJoin { left, right, .. }
        | PlanNodeInspection::HashJoin { left, right, .. } => {
            collect_operators(left, operators);
            collect_operators(right, operators);
        }
        PlanNodeInspection::Filter { input, .. }
        | PlanNodeInspection::Sort { input, .. }
        | PlanNodeInspection::Project { input, .. }
        | PlanNodeInspection::Aggregate { input, .. }
        | PlanNodeInspection::Limit { input, .. } => collect_operators(input, operators),
        PlanNodeInspection::SeqScan { .. }
        | PlanNodeInspection::IndexScan { .. }
        | PlanNodeInspection::RangeIndexScan { .. } => {}
    }
}

fn ids_observation(result: &QueryResult) -> BenchResult<Observation> {
    let mut checksum = 0_u128;
    for row in &result.rows {
        let [ScalarValue::Int64(value)] = row.as_slice() else {
            return Err(message_error("expected one non-NULL Int64 result column"));
        };
        checksum = checksum
            .checked_add(u128::try_from(*value).map_err(|_| message_error("negative result ID"))?)
            .ok_or_else(|| message_error("ID checksum overflow"))?;
    }
    Ok(Observation {
        rows: u64::try_from(result.rows.len())
            .map_err(|_| message_error("result row count exceeds u64"))?,
        checksum,
    })
}

fn id_payload_observation(result: &QueryResult) -> BenchResult<Observation> {
    let mut checksum = 0_u128;
    for row in &result.rows {
        let [ScalarValue::Int64(id), ScalarValue::Text(payload)] = row.as_slice() else {
            return Err(message_error(
                "id/payload query must return non-NULL Int64 and Text columns",
            ));
        };
        let id = u64::try_from(*id).map_err(|_| message_error("negative result ID"))?;
        if payload != &format!("payload-{id:016}") {
            return Err(message_error(format!(
                "id/payload query returned unexpected payload shape for ID {id}"
            )));
        }
        checksum = checksum
            .checked_add(u128::from(id))
            .ok_or_else(|| message_error("ID checksum overflow"))?;
    }
    Ok(Observation {
        rows: u64::try_from(result.rows.len())
            .map_err(|_| message_error("result row count exceeds u64"))?,
        checksum,
    })
}

fn payload_observation(result: &QueryResult) -> BenchResult<Observation> {
    let mut checksum = 0_u128;
    for (id, row) in result.rows.iter().enumerate() {
        let [ScalarValue::Text(payload)] = row.as_slice() else {
            return Err(message_error(
                "payload query must return one non-NULL Text column",
            ));
        };
        validate_payload(id, payload)?;
        checksum = checksum
            .checked_add(id as u128)
            .ok_or_else(|| message_error("payload checksum overflow"))?;
    }
    Ok(Observation {
        rows: u64::try_from(result.rows.len())
            .map_err(|_| message_error("result row count exceeds u64"))?,
        checksum,
    })
}

fn payload_id_observation(result: &QueryResult) -> BenchResult<Observation> {
    let mut checksum = 0_u128;
    for row in &result.rows {
        let [ScalarValue::Text(payload), ScalarValue::Int64(id)] = row.as_slice() else {
            return Err(message_error(
                "reordered projection must return non-NULL Text and Int64 columns",
            ));
        };
        let id = usize::try_from(*id).map_err(|_| message_error("invalid result ID"))?;
        validate_payload(id, payload)?;
        checksum = checksum
            .checked_add(id as u128)
            .ok_or_else(|| message_error("reordered projection checksum overflow"))?;
    }
    Ok(Observation {
        rows: u64::try_from(result.rows.len())
            .map_err(|_| message_error("result row count exceeds u64"))?,
        checksum,
    })
}

fn duplicate_payload_observation(result: &QueryResult) -> BenchResult<Observation> {
    let mut checksum = 0_u128;
    for (id, row) in result.rows.iter().enumerate() {
        let [ScalarValue::Text(first), ScalarValue::Text(second)] = row.as_slice() else {
            return Err(message_error(
                "duplicate projection must return two non-NULL Text columns",
            ));
        };
        validate_payload(id, first)?;
        validate_payload(id, second)?;
        if first != second {
            return Err(message_error("duplicate projection values differ"));
        }
        checksum = checksum
            .checked_add(id as u128)
            .ok_or_else(|| message_error("duplicate projection checksum overflow"))?;
    }
    Ok(Observation {
        rows: u64::try_from(result.rows.len())
            .map_err(|_| message_error("result row count exceeds u64"))?,
        checksum,
    })
}

fn validate_payload(id: usize, payload: &str) -> BenchResult<()> {
    if payload == format!("payload-{id:016}") {
        Ok(())
    } else {
        Err(message_error(format!("unexpected payload for row ID {id}")))
    }
}

fn heap_payload_observation(result: &[(RowId, Vec<ScalarValue>)]) -> BenchResult<Observation> {
    let mut checksum = 0_u128;
    for (id, (_, values)) in result.iter().enumerate() {
        let [ScalarValue::Text(payload)] = values.as_slice() else {
            return Err(message_error(
                "projected Heap scan must return one non-NULL Text column",
            ));
        };
        validate_payload(id, payload)?;
        checksum = checksum
            .checked_add(id as u128)
            .ok_or_else(|| message_error("projected Heap checksum overflow"))?;
    }
    Ok(Observation {
        rows: u64::try_from(result.len())
            .map_err(|_| message_error("heap scan row count exceeds u64"))?,
        checksum,
    })
}

fn heap_scan_observation(
    result: &[(RowId, Vec<ScalarValue>)],
    expected_columns: usize,
) -> BenchResult<Observation> {
    let mut checksum = 0_u128;
    for (_, values) in result {
        if values.len() != expected_columns {
            return Err(message_error(format!(
                "heap scan returned {} columns; expected {expected_columns}",
                values.len()
            )));
        }
        let Some(ScalarValue::Int64(id)) = values.first() else {
            return Err(message_error(
                "heap scan row must begin with a non-NULL Int64 ID",
            ));
        };
        checksum = checksum
            .checked_add(u128::try_from(*id).map_err(|_| message_error("negative heap row ID"))?)
            .ok_or_else(|| message_error("heap scan checksum overflow"))?;
    }
    Ok(Observation {
        rows: u64::try_from(result.len())
            .map_err(|_| message_error("heap scan row count exceeds u64"))?,
        checksum,
    })
}

fn ordered_limit_observation(
    result: &QueryResult,
    fixture_rows: u64,
    team_cardinality: u64,
) -> BenchResult<Observation> {
    let mut seen = Vec::with_capacity(result.rows.len());
    for row in &result.rows {
        let [ScalarValue::Int64(id)] = row.as_slice() else {
            return Err(message_error(
                "ORDER BY query must return one non-NULL Int64 ID",
            ));
        };
        let id = u64::try_from(*id).map_err(|_| {
            message_error("ORDER BY query returned a negative ID outside the fixture")
        })?;
        if id >= fixture_rows || id % team_cardinality != 0 {
            return Err(message_error(format!(
                "ORDER BY query returned ID {id} outside the minimum team"
            )));
        }
        if seen.contains(&id) {
            return Err(message_error(format!(
                "ORDER BY query returned duplicate ID {id}"
            )));
        }
        seen.push(id);
    }
    let rows = u64::try_from(seen.len())
        .map_err(|_| message_error("ORDER BY result row count exceeds u64"))?;
    Ok(Observation {
        rows,
        checksum: u128::from(rows),
    })
}

fn count_observation(result: &QueryResult) -> BenchResult<Observation> {
    let [row] = result.rows.as_slice() else {
        return Err(message_error("COUNT query must return one row"));
    };
    let [ScalarValue::UInt64(count)] = row.as_slice() else {
        return Err(message_error("COUNT query must return one UInt64 column"));
    };
    Ok(Observation {
        rows: 1,
        checksum: u128::from(*count),
    })
}

fn count_values_observation(result: &QueryResult, expected: &[u64]) -> BenchResult<Observation> {
    let [row] = result.rows.as_slice() else {
        return Err(message_error("COUNT query must return one row"));
    };
    if row.len() != expected.len() {
        return Err(message_error(format!(
            "COUNT query returned {} columns; expected {}",
            row.len(),
            expected.len()
        )));
    }
    let mut checksum = 0_u128;
    for (index, (value, expected)) in row.iter().zip(expected).enumerate() {
        let ScalarValue::UInt64(value) = value else {
            return Err(message_error(format!(
                "COUNT output {index} must be UInt64"
            )));
        };
        if value != expected {
            return Err(message_error(format!(
                "COUNT output {index} was {value}; expected {expected}"
            )));
        }
        checksum = checksum
            .checked_add(u128::from(*value))
            .ok_or_else(|| message_error("COUNT checksum overflow"))?;
    }
    Ok(Observation { rows: 1, checksum })
}

const fn low_non_null_count(rows: u64) -> u64 {
    rows.saturating_sub(rows.saturating_add(99) / 100)
}

const fn active_count(rows: u64) -> u64 {
    rows.saturating_add(2) / 3
}

fn group_observation(result: &QueryResult) -> BenchResult<Observation> {
    let mut checksum = 0_u128;
    for row in &result.rows {
        let [ScalarValue::Int64(key), ScalarValue::UInt64(count)] = row.as_slice() else {
            return Err(message_error(
                "GROUP BY query must return Int64 key and UInt64 count",
            ));
        };
        let key = u128::try_from(*key).map_err(|_| message_error("negative group key"))?;
        checksum = checksum
            .checked_add(key * CHECKSUM_FACTOR + u128::from(*count))
            .ok_or_else(|| message_error("group checksum overflow"))?;
    }
    Ok(Observation {
        rows: u64::try_from(result.rows.len())
            .map_err(|_| message_error("group row count exceeds u64"))?,
        checksum,
    })
}

fn inspect_observation(inspection: &StatementInspection) -> BenchResult<Observation> {
    let root = query_root(inspection)?;
    let mut operators = Vec::new();
    collect_operators(root, &mut operators);
    let checksum = operators.iter().try_fold(0_u128, |checksum, name| {
        checksum
            .checked_mul(131)
            .and_then(|value| value.checked_add(name.len() as u128))
            .ok_or_else(|| message_error("inspection checksum overflow"))
    })?;
    Ok(Observation {
        rows: u64::try_from(operators.len())
            .map_err(|_| message_error("operator count exceeds u64"))?,
        checksum,
    })
}

fn expected_modulo_ids(rows: u64, modulus: u64, remainder: u64) -> Observation {
    let mut count = 0_u64;
    let mut checksum = 0_u128;
    for id in 0..rows {
        if id % modulus == remainder {
            count += 1;
            checksum += u128::from(id);
        }
    }
    Observation {
        rows: count,
        checksum,
    }
}

fn expected_null_ids(rows: u64, distribution: NullDistribution) -> Observation {
    let mut count = 0_u64;
    let mut checksum = 0_u128;
    for id in 0..rows {
        if distribution.is_null(id) {
            count += 1;
            checksum += u128::from(id);
        }
    }
    Observation {
        rows: count,
        checksum,
    }
}

const fn expected_range_ids(start: u64, end: u64) -> Observation {
    Observation {
        rows: end - start,
        checksum: arithmetic_sum(end) - arithmetic_sum(start),
    }
}

fn expected_groups(rows: u64, cardinality: u64) -> Observation {
    let groups = rows.min(cardinality);
    let mut checksum = 0_u128;
    for key in 0..groups {
        let count = (rows - 1 - key) / cardinality + 1;
        checksum += u128::from(key) * CHECKSUM_FACTOR + u128::from(count);
    }
    Observation {
        rows: groups,
        checksum,
    }
}

fn expected_join(rows: u64, cardinality: u64) -> Observation {
    let mut result_rows = 0_u64;
    let mut checksum = 0_u128;
    for left_id in 0..rows {
        let key = left_id % cardinality;
        let matches = (rows - 1 - key) / cardinality + 1;
        result_rows += matches;
        checksum += u128::from(left_id) * u128::from(matches);
    }
    Observation {
        rows: result_rows,
        checksum,
    }
}

const fn arithmetic_sum(end: u64) -> u128 {
    let end = end as u128;
    if end == 0 { 0 } else { end * (end - 1) / 2 }
}

const _: () = {
    assert!(arithmetic_sum(0) == 0);
    assert!(arithmetic_sum(1) == 0);
    assert!(arithmetic_sum(2) == 1);
    assert!(arithmetic_sum(10) == 45);

    let range = expected_range_ids(0, 10);
    assert!(range.rows == 10);
    assert!(range.checksum == 45);
};

fn require_observation(
    scenario: &str,
    expected: Observation,
    actual: Observation,
) -> BenchResult<()> {
    if expected == actual {
        Ok(())
    } else {
        Err(message_error(format!(
            "scenario `{scenario}` produced an incorrect result: expected {expected:?}, observed {actual:?}"
        )))
    }
}

fn print_report(
    profile: BenchProfile,
    settings: ProfileSettings,
    measurements: &[Measurement],
) -> BenchResult<()> {
    println!("NetbaDB Phase 7 reproducible performance baseline");
    println!("netbadb_version={}", env!("CARGO_PKG_VERSION"));
    println!("profile={}", profile.name());
    println!(
        "small_rows={} medium_rows={} warm_cache=true",
        settings.small_rows, settings.medium_rows
    );
    println!();
    println!(
        "{:<36} {:>12} {:>10} {:>14} {:>14} {:>14}  plan",
        "scenario", "rows", "iterations", "min_ns/op", "median_ns/op", "p95_ns/op"
    );
    for measurement in measurements {
        let statistics = Statistics::from_durations(
            &measurement.durations,
            measurement.operations_per_iteration,
        )?;
        println!(
            "{:<36} {:>12} {:>10} {:>14} {:>14} {:>14}  {}",
            measurement.scenario,
            measurement.rows,
            measurement.durations.len(),
            statistics.min_ns_per_op,
            statistics.median_ns_per_op,
            statistics.p95_ns_per_op,
            measurement.plan
        );
    }
    println!();
    println!("Known current planner limitations represented by this baseline:");
    println!("- costed bounded Int64/UInt64 RangeIndexScan;");
    println!("- one-sided and Text/Bool ranges remain SeqScan;");
    println!("- no index union/intersection;");
    println!("- costed direct Scan x Scan equi HashJoin, otherwise NestedLoopJoin;");
    println!("- explicit in-memory Sort;");
    println!("- in-memory Aggregate;");
    println!("- no join reorder.");
    Ok(())
}

fn cleanup_paths(paths: &[PathBuf]) -> BenchResult<()> {
    for path in paths {
        remove_if_present(path)?;
        let wal = netbadb_storage::wal_path(path);
        remove_if_present(&netbadb_storage::wal_alternate_path(&wal))?;
        remove_if_present(&wal)?;
    }
    Ok(())
}

fn remove_if_present(path: &Path) -> BenchResult<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(message_error(format!(
            "failed to remove benchmark fixture `{}`: {error}",
            path.display()
        ))),
    }
}

fn message_error(message: impl Into<String>) -> Box<dyn Error> {
    Box::new(io::Error::other(message.into()))
}

fn scenario_error(scenario: &str, error: Box<dyn Error>) -> Box<dyn Error> {
    message_error(format!("scenario `{scenario}` failed: {error}"))
}
