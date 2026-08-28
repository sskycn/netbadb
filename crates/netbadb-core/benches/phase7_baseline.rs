//! Reproducible warm-cache performance baselines for Phase 7.

use std::env;
use std::error::Error;
use std::hint::black_box;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use netbadb_core::{
    Database, DatabaseCoordinatorConfig, ExecutionResult, PartitionCatalogConfig, QueryResult,
    RangePartitionSpec, TablePlacementSpec, TableStorageCreateSpec,
};
use netbadb_inspect::{
    PartitionAccessInspection, PlanNodeInspection, StatementInspection, StatementPlanInspection,
};
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_storage::HeapStorage;
use netbadb_types::{ColumnId, PartitionId, PhysicalType, RowId, ScalarValue, TableId};

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
const PARTITIONED_ITEMS_TABLE_ID: TableId = TableId(70);
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
                phase65_probe_rows: 4_096,
                phase65_build_rows: 64,
                phase66_partition_rows: 1_024,
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
                phase65_probe_rows: 16_384,
                phase65_build_rows: 256,
                phase66_partition_rows: 2_048,
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
    phase65_probe_rows: u64,
    phase65_build_rows: u64,
    phase66_partition_rows: u64,
    update_rows: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Observation {
    rows: u64,
    checksum: u128,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct GroupAggregateExpected {
    key: u64,
    count: u64,
    sum: u64,
    min: u64,
    max: u64,
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

#[derive(Debug, Clone, Copy)]
enum TextComparisonShape {
    EarlyDifference,
    LongCommonPrefix,
    AllEqual,
}

impl TextComparisonShape {
    fn payload(self, id: u64) -> String {
        let payload = match self {
            Self::EarlyDifference => {
                let mut value = String::with_capacity(64);
                value.push(if id == 0 { 'a' } else { 'z' });
                value.push_str(&".".repeat(63));
                value
            }
            Self::LongCommonPrefix => {
                let mut value = "m".repeat(63);
                value.push(if id == 0 { 'a' } else { 'z' });
                value
            }
            Self::AllEqual => "m".repeat(64),
        };
        debug_assert_eq!(payload.len(), 64);
        payload
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
    run_top_n_attribution_scenarios(settings, &mut measurements)?;
    run_point_and_shape_scenarios(settings, &mut measurements)?;
    run_join_scenarios(settings, &mut measurements)?;
    run_insert_scenarios(settings, &mut measurements)?;
    run_update_scenario(settings, &mut measurements)?;
    run_planner_scenario(settings, &mut measurements)?;
    run_phase66_partitioned_scenarios(settings, &mut measurements)?;
    run_partition_correctness_scenarios()?;
    run_lsm_correctness_scenarios(settings, &mut measurements)?;

    print_report(profile, settings, &measurements)?;
    Ok(())
}

fn run_lsm_correctness_scenarios(
    settings: ProfileSettings,
    measurements: &mut Vec<Measurement>,
) -> BenchResult<()> {
    let rows = settings.small_rows.max(8);
    let fixture = FixturePaths::new("lsm-correctness", 1);
    let mut database = Database::create_storages(vec![TableStorageCreateSpec::lsm(
        fixture.path(0),
        items_table(),
        ID_COLUMN_ID,
    )])?;

    let started = Instant::now();
    load_item_rows(&mut database, rows, 4, NullDistribution::Low)?;
    measurements.push(Measurement {
        scenario: "lsm_insert".into(),
        rows: rows.to_string(),
        plan: "LSM transaction overlay → WAL".into(),
        operations_per_iteration: rows,
        durations: vec![started.elapsed()],
    });
    database.analyze(ITEMS_TABLE_ID)?;

    let expected_all = Observation {
        rows,
        checksum: arithmetic_sum(rows),
    };
    for (scenario, sql, expected, required) in [
        (
            "lsm_sequential_scan_memtable",
            "SELECT id FROM items",
            expected_all,
            Operator::SeqScan,
        ),
        (
            "lsm_point",
            "SELECT id FROM items WHERE id = 1",
            Observation {
                rows: 1,
                checksum: 1,
            },
            Operator::IndexScan,
        ),
        (
            "lsm_narrow_range",
            "SELECT id FROM items WHERE id >= 1 AND id <= 3",
            Observation {
                rows: 3,
                checksum: 6,
            },
            Operator::RangeIndexScan,
        ),
        (
            "lsm_wide_range",
            "SELECT id FROM items WHERE id >= 0",
            expected_all,
            Operator::SeqScan,
        ),
    ] {
        let plan = inspect_plan(&database, scenario, sql, &[required], &[])?;
        let durations = measure_checked(
            scenario,
            settings.query_warmup,
            settings.query_iterations,
            expected,
            || database.query(sql).map_err(Into::into),
            ids_observation,
        )?;
        measurements.push(Measurement {
            scenario: scenario.into(),
            rows: rows.to_string(),
            plan,
            operations_per_iteration: 1,
            durations,
        });
    }

    let scenario = "lsm_batch_filter_project_limit";
    let sql = "SELECT id FROM items WHERE active = true LIMIT 20";
    let plan = inspect_plan(
        &database,
        scenario,
        sql,
        &[
            Operator::Limit,
            Operator::Filter,
            Operator::Project,
            Operator::SeqScan,
        ],
        &[],
    )?;
    let expected = Observation {
        rows: 20,
        checksum: 570,
    };
    let durations = measure_checked(
        scenario,
        settings.query_warmup,
        settings.query_iterations,
        expected,
        || database.query(sql).map_err(Into::into),
        ids_observation,
    )?;
    measurements.push(Measurement {
        scenario: scenario.into(),
        rows: expected.rows.to_string(),
        plan,
        operations_per_iteration: 1,
        durations,
    });

    let scenario = "lsm_top_n_multi_key_k_20";
    let sql = "SELECT id FROM items ORDER BY team_id ASC, id DESC LIMIT 20";
    let mut expected_ids = (0..rows).collect::<Vec<_>>();
    expected_ids.sort_by(|left, right| (left % 4).cmp(&(right % 4)).then_with(|| right.cmp(left)));
    expected_ids.truncate(usize::try_from(rows.min(20))?);
    let plan = inspect_plan(
        &database,
        scenario,
        sql,
        &[
            Operator::Limit,
            Operator::Project,
            Operator::Sort,
            Operator::SeqScan,
        ],
        &[],
    )?;
    if plan != "Limit>Project>Sort>SeqScan" {
        return Err(message_error(format!(
            "scenario `{scenario}` plan was `{plan}`; expected `Limit>Project>Sort>SeqScan`"
        )));
    }
    inspect_base_scan_columns(&database, scenario, sql, &[ID_COLUMN_ID, TEAM_COLUMN_ID])?;
    let expected = expected_ids_observation(&expected_ids)?;
    let durations = measure_checked(
        scenario,
        settings.query_warmup,
        settings.query_iterations,
        expected,
        || database.query(sql).map_err(Into::into),
        |result| ordered_ids_observation(result, &expected_ids),
    )?;
    measurements.push(Measurement {
        scenario: scenario.into(),
        rows: format!("{rows}/k={}", expected_ids.len()),
        plan,
        operations_per_iteration: 1,
        durations,
    });

    let scenario = "lsm_stream_aggregate_sum";
    let sql = "SELECT SUM(id) FROM items";
    let plan = inspect_plan(
        &database,
        scenario,
        sql,
        &[Operator::Aggregate, Operator::SeqScan],
        &[],
    )?;
    let expected = Observation {
        rows: 1,
        checksum: arithmetic_sum(rows),
    };
    let durations = measure_checked(
        scenario,
        settings.query_warmup,
        settings.query_iterations,
        expected,
        || database.query(sql).map_err(Into::into),
        sum_observation,
    )?;
    measurements.push(Measurement {
        scenario: scenario.into(),
        rows: expected.rows.to_string(),
        plan,
        operations_per_iteration: 1,
        durations,
    });

    let scenario = "phase67_lsm_same_column_multi";
    let sql = "SELECT SUM(id), MIN(id), MAX(id) FROM items";
    let plan = inspect_plan(
        &database,
        scenario,
        sql,
        &[Operator::Aggregate, Operator::SeqScan],
        &[],
    )?;
    inspect_base_scan_columns(&database, scenario, sql, &[ID_COLUMN_ID])?;
    let durations = measure_checked(
        scenario,
        settings.query_warmup,
        settings.query_iterations,
        expected_global_multi(rows),
        || database.query(sql).map_err(Into::into),
        |result| global_multi_observation(result, rows),
    )?;
    measurements.push(Measurement {
        scenario: scenario.into(),
        rows: rows.to_string(),
        plan,
        operations_per_iteration: 1,
        durations,
    });

    for (scenario, sql) in [
        (
            "lsm_update",
            "UPDATE items SET payload = 'updated' WHERE id = 1",
        ),
        (
            "lsm_key_changing_update",
            "UPDATE items SET id = 1000000 WHERE id = 2",
        ),
        ("lsm_delete", "DELETE FROM items WHERE id = 3"),
    ] {
        let started = Instant::now();
        let result = database.execute(sql)?;
        let elapsed = started.elapsed();
        if result != ExecutionResult::AffectedRows(1) {
            return Err(message_error(format!(
                "scenario `{scenario}` affected-row count mismatch"
            )));
        }
        measurements.push(Measurement {
            scenario: scenario.into(),
            rows: "1".into(),
            plan: "LSM version/tombstone mutation".into(),
            operations_per_iteration: 1,
            durations: vec![elapsed],
        });
    }
    require_observation(
        "lsm_memtable_only_read",
        Observation {
            rows: rows - 1,
            checksum: arithmetic_sum(rows) - 2 - 3 + 1_000_000,
        },
        ids_observation(&database.query("SELECT id FROM items")?)?,
    )?;
    database.checkpoint()?;
    require_observation(
        "lsm_post_flush_read",
        Observation {
            rows: rows - 1,
            checksum: arithmetic_sum(rows) - 2 - 3 + 1_000_000,
        },
        ids_observation(&database.query("SELECT id FROM items")?)?,
    )?;
    database.compact(ITEMS_TABLE_ID)?;
    require_observation(
        "lsm_post_compaction_read",
        Observation {
            rows: rows - 1,
            checksum: arithmetic_sum(rows) - 2 - 3 + 1_000_000,
        },
        ids_observation(&database.query("SELECT id FROM items")?)?,
    )?;
    database.close()?;
    fixture.cleanup()?;

    let amplification = FixturePaths::new("lsm-amplification", 1);
    let mut database = Database::create_storages(vec![TableStorageCreateSpec::lsm(
        amplification.path(0),
        items_table(),
        ID_COLUMN_ID,
    )])?;
    let large_payload = "x".repeat(40_000);
    for batch in 0..4_u64 {
        let mut transaction = database.begin_transaction_for(ITEMS_TABLE_ID)?;
        for item in 0..10_u64 {
            let id = (batch * 10 + item) * 2;
            let mut values = item_row(id, 4, NullDistribution::Low)?;
            values[5] = ScalarValue::Text(large_payload.clone());
            database.insert_into_in(ITEMS_TABLE_ID, &mut transaction, &values)?;
        }
        transaction.commit()?;
        database.checkpoint()?;
    }
    let compaction_started = Instant::now();
    database.compact(ITEMS_TABLE_ID)?;
    let compaction_elapsed = compaction_started.elapsed();
    database.analyze(ITEMS_TABLE_ID)?;
    let structural = database
        .inspect_lsm_storage(ITEMS_TABLE_ID)?
        .ok_or_else(|| message_error("LSM amplification inspection missing"))?;
    if !structural.levels.iter().any(|level| level.level >= 2) {
        return Err(message_error(
            "LSM amplification fixture did not reach a deeper level",
        ));
    }
    let before = structural.read_amplification;
    let miss_started = Instant::now();
    require_observation(
        "lsm_multi_level_point_miss",
        Observation {
            rows: 0,
            checksum: 0,
        },
        ids_observation(&database.query("SELECT id FROM items WHERE id = 19")?)?,
    )?;
    let miss_elapsed = miss_started.elapsed();
    let miss = database
        .inspect_lsm_storage(ITEMS_TABLE_ID)?
        .ok_or_else(|| message_error("LSM miss inspection missing"))?
        .read_amplification;
    if miss.bloom_checks <= before.bloom_checks || miss.bloom_negatives <= before.bloom_negatives {
        return Err(message_error(
            "LSM point miss did not exercise a Bloom-negative path",
        ));
    }
    measurements.push(Measurement {
        scenario: "lsm_multi_level_point_miss".into(),
        rows: "0".into(),
        plan: format!(
            "Bloom checks={} negatives={} blocks={}",
            miss.bloom_checks - before.bloom_checks,
            miss.bloom_negatives - before.bloom_negatives,
            miss.data_blocks_read - before.data_blocks_read
        ),
        operations_per_iteration: 1,
        durations: vec![miss_elapsed],
    });
    for (scenario, sql, expected, required) in [
        (
            "lsm_multi_level_point_hit",
            "SELECT id FROM items WHERE id = 0",
            Observation {
                rows: 1,
                checksum: 0,
            },
            Operator::IndexScan,
        ),
        (
            "lsm_multi_level_narrow_range",
            "SELECT id FROM items WHERE id >= 10 AND id <= 20",
            Observation {
                rows: 6,
                checksum: 90,
            },
            Operator::RangeIndexScan,
        ),
        (
            "lsm_multi_level_full_scan",
            "SELECT id FROM items",
            Observation {
                rows: 40,
                checksum: 1_560,
            },
            Operator::SeqScan,
        ),
    ] {
        let plan = inspect_plan(&database, scenario, sql, &[required], &[])?;
        let durations = measure_checked(
            scenario,
            settings.query_warmup,
            settings.query_iterations,
            expected,
            || database.query(sql).map_err(Into::into),
            ids_observation,
        )?;
        measurements.push(Measurement {
            scenario: scenario.into(),
            rows: expected.rows.to_string(),
            plan,
            operations_per_iteration: 1,
            durations,
        });
    }
    let amplification_stats = database
        .inspect_lsm_storage(ITEMS_TABLE_ID)?
        .ok_or_else(|| message_error("LSM write amplification inspection missing"))?;
    measurements.push(Measurement {
        scenario: "lsm_leveled_write_amplification".into(),
        rows: "40".into(),
        plan: format!(
            "flush_in={} flush_out={} compact_in={} compact_out={} obsolete={}",
            amplification_stats.write_amplification.flush_input_bytes,
            amplification_stats.write_amplification.flush_output_bytes,
            amplification_stats
                .write_amplification
                .compaction_input_bytes,
            amplification_stats
                .write_amplification
                .compaction_output_bytes,
            amplification_stats.write_amplification.obsolete_bytes,
        ),
        operations_per_iteration: 40,
        durations: vec![compaction_elapsed],
    });
    database.close()?;
    amplification.cleanup()?;

    let mixed = FixturePaths::new("heap-lsm-atomic", 3);
    let heap_table = join_table(TableId(91), "heap_atomic");
    let lsm_table = join_table(TableId(92), "lsm_atomic");
    let mut database = Database::create_storages_with_coordinator(
        vec![
            TableStorageCreateSpec::heap(mixed.path(0), heap_table),
            TableStorageCreateSpec::lsm(mixed.path(1), lsm_table, ID_COLUMN_ID),
        ],
        DatabaseCoordinatorConfig::new(mixed.path(2)),
    )?;
    let started = Instant::now();
    let mut transaction = database.begin_transaction_for(TableId(91))?;
    database.execute_in(
        &mut transaction,
        "INSERT INTO heap_atomic (id, join_key) VALUES (1, 7)",
    )?;
    database.execute_in(
        &mut transaction,
        "INSERT INTO lsm_atomic (id, join_key) VALUES (1, 7)",
    )?;
    transaction.commit()?;
    let elapsed = started.elapsed();
    for name in ["heap_atomic", "lsm_atomic"] {
        let result = database.query(&format!("SELECT id FROM {name}"))?;
        require_observation(
            "heap_lsm_atomic_transaction",
            Observation {
                rows: 1,
                checksum: 1,
            },
            ids_observation(&result)?,
        )?;
    }
    measurements.push(Measurement {
        scenario: "heap_lsm_atomic_transaction".into(),
        rows: "2 participant writes".into(),
        plan: "Prepare → CommitDecision → engine commits".into(),
        operations_per_iteration: 2,
        durations: vec![elapsed],
    });
    database.close()?;
    mixed.cleanup()
}

fn run_phase66_partitioned_scenarios(
    settings: ProfileSettings,
    measurements: &mut Vec<Measurement>,
) -> BenchResult<()> {
    let rows = settings.phase66_partition_rows;
    for partition_count in [1_usize, 2, 4, 8] {
        let fixture_name = format!("phase66-partitioned-p{partition_count}");
        let (mut database, paths) =
            partitioned_items_fixture(&fixture_name, rows, partition_count)?;
        let all_ids = (0..rows).collect::<Vec<_>>();
        measure_phase66_partition_query(
            &mut database,
            &format!("partition_full_projection_p{partition_count}"),
            rows,
            partition_count,
            "SELECT id FROM partitioned_items",
            &[Operator::Project, Operator::SeqScan],
            expected_ids_observation(&all_ids)?,
            settings,
            |result| ordered_ids_observation(result, &all_ids),
            measurements,
        )?;

        if partition_count == 4 {
            let limited_ids = (0..rows.min(20)).collect::<Vec<_>>();
            measure_phase66_partition_query(
                &mut database,
                "partition_limit_20",
                rows,
                partition_count,
                "SELECT id FROM partitioned_items LIMIT 20",
                &[Operator::Limit, Operator::Project, Operator::SeqScan],
                expected_ids_observation(&limited_ids)?,
                settings,
                |result| ordered_ids_observation(result, &limited_ids),
                measurements,
            )?;
            measure_phase66_partition_query(
                &mut database,
                "partition_sum",
                rows,
                partition_count,
                "SELECT SUM(id) FROM partitioned_items",
                &[Operator::Aggregate, Operator::SeqScan],
                Observation {
                    rows: 1,
                    checksum: arithmetic_sum(rows),
                },
                settings,
                sum_observation,
                measurements,
            )?;
            let scenario = "phase67_partition_same_column_multi";
            let sql = "SELECT SUM(id), MIN(id), MAX(id) FROM partitioned_items";
            inspect_base_scan_columns(&database, scenario, sql, &[ID_COLUMN_ID])?;
            measure_phase66_partition_query(
                &mut database,
                scenario,
                rows,
                partition_count,
                sql,
                &[Operator::Aggregate, Operator::SeqScan],
                expected_global_multi(rows),
                settings,
                |result| global_multi_observation(result, rows),
                measurements,
            )?;
            measure_phase66_partition_query(
                &mut database,
                "partition_grouped_count",
                rows,
                partition_count,
                "SELECT team_id, COUNT(*) FROM partitioned_items GROUP BY team_id",
                &[Operator::Aggregate, Operator::SeqScan],
                expected_groups(rows, 4),
                settings,
                group_observation,
                measurements,
            )?;

            let mut top_n_ids = (0..rows).collect::<Vec<_>>();
            top_n_ids.sort_by_key(|id| (id % 4, *id));
            top_n_ids.truncate(usize::try_from(rows.min(20))?);
            measure_phase66_partition_query(
                &mut database,
                "partition_top_n_20",
                rows,
                partition_count,
                "SELECT id FROM partitioned_items ORDER BY team_id, id LIMIT 20",
                &[
                    Operator::Limit,
                    Operator::Project,
                    Operator::Sort,
                    Operator::SeqScan,
                ],
                expected_ids_observation(&top_n_ids)?,
                settings,
                |result| ordered_ids_observation(result, &top_n_ids),
                measurements,
            )?;

            let filtered_ids = (0..rows)
                .filter(|id| id % 3 == 0)
                .take(20)
                .collect::<Vec<_>>();
            measure_phase66_partition_query(
                &mut database,
                "partition_filter_limit_20",
                rows,
                partition_count,
                "SELECT id FROM partitioned_items WHERE active = true LIMIT 20",
                &[
                    Operator::Limit,
                    Operator::Project,
                    Operator::Filter,
                    Operator::SeqScan,
                ],
                expected_ids_observation(&filtered_ids)?,
                settings,
                |result| ordered_ids_observation(result, &filtered_ids),
                measurements,
            )?;
        }
        database.close()?;
        paths.cleanup()?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn measure_phase66_partition_query(
    database: &mut Database,
    scenario: &str,
    source_rows: u64,
    expected_partitions: usize,
    sql: &str,
    required: &[Operator],
    expected: Observation,
    settings: ProfileSettings,
    observe: impl FnMut(&QueryResult) -> BenchResult<Observation>,
    measurements: &mut Vec<Measurement>,
) -> BenchResult<()> {
    let inspection = database.inspect_statement(sql)?;
    let root = query_root(&inspection)?;
    for operator in required {
        if !contains_operator(root, *operator) {
            return Err(message_error(format!(
                "scenario `{scenario}` plan is missing required operator {}: {}",
                operator.name(),
                plan_label(root)
            )));
        }
    }
    let partitions = partitioned_scan_partitions(root)
        .ok_or_else(|| message_error(format!("scenario `{scenario}` is not PartitionedScan")))?;
    if partitions.len() != expected_partitions {
        return Err(message_error(format!(
            "scenario `{scenario}` selected {} partitions; expected {expected_partitions}",
            partitions.len()
        )));
    }
    if partitions
        .iter()
        .any(|partition| !matches!(partition.access, PartitionAccessInspection::SeqScan))
    {
        return Err(message_error(format!(
            "scenario `{scenario}` did not select all-SeqScan partition access: {partitions:?}"
        )));
    }
    let durations = measure_checked(
        scenario,
        settings.query_warmup,
        settings.query_iterations,
        expected,
        || database.query(sql).map_err(Into::into),
        observe,
    )?;
    measurements.push(Measurement {
        scenario: scenario.to_owned(),
        rows: format!("{source_rows}/p{expected_partitions}"),
        plan: format!(
            "{} [PartitionedScan partitions={expected_partitions} access=all-SeqScan]",
            plan_label(root)
        ),
        operations_per_iteration: 1,
        durations,
    });
    Ok(())
}

fn partitioned_scan_partitions(
    plan: &PlanNodeInspection,
) -> Option<&[netbadb_inspect::PartitionScanInspection]> {
    match plan {
        PlanNodeInspection::PartitionedScan { partitions, .. } => Some(partitions),
        PlanNodeInspection::Filter { input, .. }
        | PlanNodeInspection::Sort { input, .. }
        | PlanNodeInspection::Project { input, .. }
        | PlanNodeInspection::Aggregate { input, .. }
        | PlanNodeInspection::Limit { input, .. } => partitioned_scan_partitions(input),
        PlanNodeInspection::NestedLoopJoin { .. }
        | PlanNodeInspection::HashJoin { .. }
        | PlanNodeInspection::SeqScan { .. }
        | PlanNodeInspection::IndexScan { .. }
        | PlanNodeInspection::RangeIndexScan { .. } => None,
    }
}

fn partitioned_items_fixture(
    scenario: &str,
    rows: u64,
    partition_count: usize,
) -> BenchResult<(Database, FixturePaths)> {
    let paths = FixturePaths::new(scenario, partition_count + 2);
    let partition_count_u64 = u64::try_from(partition_count)?;
    let partitions = (0..partition_count)
        .map(|position| {
            let position_u64 = u64::try_from(position)?;
            let next_position_u64 = position_u64
                .checked_add(1)
                .ok_or_else(|| message_error("partition position overflow"))?;
            let lower = if position == 0 {
                None
            } else {
                Some(ScalarValue::Int64(i64::try_from(
                    rows.checked_mul(position_u64)
                        .ok_or_else(|| message_error("partition lower bound overflow"))?
                        / partition_count_u64,
                )?))
            };
            let upper = if position + 1 == partition_count {
                None
            } else {
                Some(ScalarValue::Int64(i64::try_from(
                    rows.checked_mul(next_position_u64)
                        .ok_or_else(|| message_error("partition upper bound overflow"))?
                        / partition_count_u64,
                )?))
            };
            Ok(RangePartitionSpec::new(
                PartitionId(u64::try_from(position + 1)?),
                paths.path(position),
                lower,
                upper,
            ))
        })
        .collect::<BenchResult<Vec<_>>>()?;
    let config =
        PartitionCatalogConfig::new(paths.path(partition_count), paths.path(partition_count + 1));
    let placements = vec![TablePlacementSpec::range_partitioned(
        partitioned_items_table(),
        ID_COLUMN_ID,
        partitions,
    )];
    let mut database = Database::create_with_placements(placements, config)?;
    let mut transaction = database.begin_transaction_for(PARTITIONED_ITEMS_TABLE_ID)?;
    for id in 0..rows {
        let id_value = i64::try_from(id)?;
        database.insert_into_in(
            PARTITIONED_ITEMS_TABLE_ID,
            &mut transaction,
            &[
                ScalarValue::Int64(id_value),
                ScalarValue::Int64(i64::try_from(id % 4)?),
                ScalarValue::Bool(id % 3 == 0),
            ],
        )?;
    }
    transaction.commit()?;
    Ok((database, paths))
}

fn partitioned_items_table() -> TableDef {
    TableDef::new(
        PARTITIONED_ITEMS_TABLE_ID,
        "partitioned_items",
        vec![
            ColumnDef::new(ID_COLUMN_ID, "id", TypeSpec::Physical(PhysicalType::Int64)),
            ColumnDef::new(
                TEAM_COLUMN_ID,
                "team_id",
                TypeSpec::Physical(PhysicalType::Int64),
            ),
            ColumnDef::new(
                ACTIVE_COLUMN_ID,
                "active",
                TypeSpec::Physical(PhysicalType::Bool),
            ),
        ],
    )
}

fn run_partition_correctness_scenarios() -> BenchResult<()> {
    let fixture = FixturePaths::new("partition-correctness", 5);
    let table = TableDef::new(
        TableId(70),
        "partition_items",
        vec![ColumnDef::new(
            ColumnId(1),
            "key",
            TypeSpec::Physical(PhysicalType::Int64),
        )],
    );
    let config = PartitionCatalogConfig::new(fixture.path(3), fixture.path(4));
    let specs = vec![TablePlacementSpec::range_partitioned(
        table,
        ColumnId(1),
        vec![
            RangePartitionSpec::new(
                PartitionId(1),
                fixture.path(0),
                None,
                Some(ScalarValue::Int64(0)),
            ),
            RangePartitionSpec::new(
                PartitionId(2),
                fixture.path(1),
                Some(ScalarValue::Int64(0)),
                Some(ScalarValue::Int64(200)),
            ),
            RangePartitionSpec::new(
                PartitionId(3),
                fixture.path(2),
                Some(ScalarValue::Int64(200)),
                None,
            ),
        ],
    )];
    let mut database = Database::create_with_placements(specs, config)?;
    database.create_partition_index(TableId(70), PartitionId(2), ColumnId(1))?;
    for value in [-1, 1, 201] {
        database.execute(&format!(
            "INSERT INTO partition_items (key) VALUES ({value})"
        ))?;
    }
    for (sql, expected) in [
        ("SELECT key FROM partition_items", 3),
        ("SELECT key FROM partition_items WHERE key = 1", 1),
        (
            "SELECT key FROM partition_items WHERE key >= 0 AND key < 300",
            2,
        ),
    ] {
        let inspection = database.inspect_statement(sql)?;
        let root = query_root(&inspection)?;
        let selected = selected_partition_count(root)
            .ok_or_else(|| message_error("partition benchmark expected PartitionedScan"))?;
        if selected != expected {
            return Err(message_error(format!(
                "partition benchmark selected {selected} partitions for `{sql}`, expected {expected}"
            )));
        }
        let _ = database.query(sql)?;
    }
    if database.execute("UPDATE partition_items SET key = 250 WHERE key = -1")?
        != ExecutionResult::AffectedRows(1)
    {
        return Err(message_error("cross-partition UPDATE count mismatch"));
    }
    if database.execute("DELETE FROM partition_items WHERE key >= 0")?
        != ExecutionResult::AffectedRows(3)
    {
        return Err(message_error("multi-partition DELETE count mismatch"));
    }
    database.close()?;
    fixture.cleanup()
}

fn selected_partition_count(plan: &PlanNodeInspection) -> Option<usize> {
    match plan {
        PlanNodeInspection::PartitionedScan { partitions, .. } => Some(partitions.len()),
        PlanNodeInspection::Filter { input, .. }
        | PlanNodeInspection::Sort { input, .. }
        | PlanNodeInspection::Project { input, .. }
        | PlanNodeInspection::Aggregate { input, .. }
        | PlanNodeInspection::Limit { input, .. } => selected_partition_count(input),
        PlanNodeInspection::NestedLoopJoin { left, right, .. }
        | PlanNodeInspection::HashJoin { left, right, .. } => {
            selected_partition_count(left).or_else(|| selected_partition_count(right))
        }
        PlanNodeInspection::SeqScan { .. }
        | PlanNodeInspection::IndexScan { .. }
        | PlanNodeInspection::RangeIndexScan { .. } => None,
    }
}

fn run_projection_attribution_scenarios(
    settings: ProfileSettings,
    measurements: &mut Vec<Measurement>,
) -> BenchResult<()> {
    let rows = settings.medium_rows;
    run_attribution_query(
        "batch_full_projected_scan",
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
        "batch_filter_project_bool",
        rows,
        "SELECT id FROM items WHERE active = true",
        &[Operator::Filter, Operator::Project, Operator::SeqScan],
        &[ID_COLUMN_ID, ACTIVE_COLUMN_ID],
        expected_modulo_ids(rows, 3, 0),
        settings,
        ids_observation,
        measurements,
    )?;
    let middle = rows / 2;
    run_attribution_query(
        "batch_filter_project_text",
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
    )?;
    run_attribution_query(
        "batch_early_limit",
        rows,
        "SELECT id FROM items LIMIT 20",
        &[Operator::Limit, Operator::Project, Operator::SeqScan],
        &[ID_COLUMN_ID],
        Observation {
            rows: 20,
            checksum: arithmetic_sum(20),
        },
        settings,
        ids_observation,
        measurements,
    )?;
    run_attribution_query(
        "batch_filter_limit",
        rows,
        "SELECT id FROM items WHERE active = true LIMIT 20",
        &[
            Operator::Limit,
            Operator::Filter,
            Operator::Project,
            Operator::SeqScan,
        ],
        &[ID_COLUMN_ID, ACTIVE_COLUMN_ID],
        Observation {
            rows: 20,
            checksum: 570,
        },
        settings,
        ids_observation,
        measurements,
    )?;
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
        "stream_aggregate_global_sum",
        rows,
        "SELECT SUM(id) FROM items",
        &[Operator::Aggregate, Operator::SeqScan],
        &[ID_COLUMN_ID],
        Observation {
            rows: 1,
            checksum: arithmetic_sum(rows),
        },
        settings,
        sum_observation,
        measurements,
    )?;
    run_attribution_query(
        "stream_aggregate_global_multi",
        rows,
        "SELECT SUM(id), MIN(id), MAX(id) FROM items",
        &[Operator::Aggregate, Operator::SeqScan],
        &[ID_COLUMN_ID],
        expected_global_multi(rows),
        settings,
        |result| global_multi_observation(result, rows),
        measurements,
    )?;
    let sum = i64::try_from(arithmetic_sum(rows))?;
    let duplicate_sum_values = vec![
        ScalarValue::Int64(sum),
        ScalarValue::Int64(sum),
        ScalarValue::Int64(sum),
    ];
    run_attribution_query(
        "phase67_aggregate_same_column_sum_triplicate",
        rows,
        "SELECT SUM(id), SUM(id), SUM(id) FROM items",
        &[Operator::Aggregate, Operator::SeqScan],
        &[ID_COLUMN_ID],
        primitive_values_expected(&duplicate_sum_values)?,
        settings,
        |result| primitive_values_observation(result, &duplicate_sum_values),
        measurements,
    )?;
    let three_column_values = vec![
        ScalarValue::Int64(sum),
        ScalarValue::Int64(0),
        ScalarValue::Int64(i64::try_from(rows.saturating_sub(1))?),
    ];
    run_attribution_query(
        "phase67_aggregate_three_primitive_columns",
        rows,
        "SELECT SUM(id), MIN(team_id), MAX(bucket_id) FROM items",
        &[Operator::Aggregate, Operator::SeqScan],
        &[ID_COLUMN_ID, TEAM_COLUMN_ID, BUCKET_COLUMN_ID],
        primitive_values_expected(&three_column_values)?,
        settings,
        |result| primitive_values_observation(result, &three_column_values),
        measurements,
    )?;
    let active_ids = (0..rows).filter(|id| id % 3 == 0).collect::<Vec<_>>();
    let active_sum = active_ids.iter().try_fold(0_u64, |sum, id| {
        sum.checked_add(*id)
            .ok_or_else(|| message_error("active fixture SUM overflow"))
    })?;
    let filtered_values = vec![
        ScalarValue::Int64(i64::try_from(active_sum)?),
        active_ids
            .first()
            .copied()
            .map(i64::try_from)
            .transpose()?
            .map_or(ScalarValue::Null, ScalarValue::Int64),
        active_ids
            .last()
            .copied()
            .map(i64::try_from)
            .transpose()?
            .map_or(ScalarValue::Null, ScalarValue::Int64),
    ];
    run_attribution_query(
        "phase67_aggregate_filtered_same_column_multi",
        rows,
        "SELECT SUM(id), MIN(id), MAX(id) FROM items WHERE active = true",
        &[Operator::Aggregate, Operator::Filter, Operator::SeqScan],
        &[ID_COLUMN_ID, ACTIVE_COLUMN_ID],
        primitive_values_expected(&filtered_values)?,
        settings,
        |result| primitive_values_observation(result, &filtered_values),
        measurements,
    )?;
    let nullable_ids = (0..rows)
        .filter(|id| !NullDistribution::Low.is_null(*id))
        .collect::<Vec<_>>();
    let nullable_values = vec![
        ScalarValue::UInt64(u64::try_from(nullable_ids.len())?),
        nullable_ids
            .first()
            .copied()
            .map(i64::try_from)
            .transpose()?
            .map_or(ScalarValue::Null, ScalarValue::Int64),
        nullable_ids
            .last()
            .copied()
            .map(i64::try_from)
            .transpose()?
            .map_or(ScalarValue::Null, ScalarValue::Int64),
    ];
    run_attribution_query(
        "phase67_aggregate_nullable_primitive_multi",
        rows,
        "SELECT COUNT(nullable_key), MIN(nullable_key), MAX(nullable_key) FROM items",
        &[Operator::Aggregate, Operator::SeqScan],
        &[NULLABLE_COLUMN_ID],
        primitive_values_expected(&nullable_values)?,
        settings,
        |result| primitive_values_observation(result, &nullable_values),
        measurements,
    )?;
    for (scenario, shape) in [
        (
            "text_compare_min_early_difference_64",
            TextComparisonShape::EarlyDifference,
        ),
        (
            "text_compare_min_long_common_prefix_64",
            TextComparisonShape::LongCommonPrefix,
        ),
        (
            "text_compare_min_all_equal_64",
            TextComparisonShape::AllEqual,
        ),
    ] {
        run_text_comparison_attribution_query(scenario, rows, shape, settings, measurements)?;
    }
    run_attribution_query(
        "stream_aggregate_text_min",
        rows,
        "SELECT MIN(payload) FROM items",
        &[Operator::Aggregate, Operator::SeqScan],
        &[PAYLOAD_COLUMN_ID],
        Observation {
            rows: 1,
            checksum: 0,
        },
        settings,
        |result| text_extreme_observation(result, rows, false, 1),
        measurements,
    )?;
    run_attribution_query(
        "stream_aggregate_text_max",
        rows,
        "SELECT MAX(payload) FROM items",
        &[Operator::Aggregate, Operator::SeqScan],
        &[PAYLOAD_COLUMN_ID],
        Observation {
            rows: 1,
            checksum: u128::from(rows.saturating_sub(1)),
        },
        settings,
        |result| text_extreme_observation(result, rows, true, 1),
        measurements,
    )?;
    run_attribution_query(
        "stream_aggregate_text_min_max",
        rows,
        "SELECT MIN(payload), MAX(payload) FROM items",
        &[Operator::Aggregate, Operator::SeqScan],
        &[PAYLOAD_COLUMN_ID],
        Observation {
            rows: 1,
            checksum: u128::from(rows.saturating_sub(1)),
        },
        settings,
        |result| text_min_max_observation(result, rows),
        measurements,
    )?;
    run_attribution_query(
        "stream_aggregate_text_max_duplicate",
        rows,
        "SELECT MAX(payload), MAX(payload) FROM items",
        &[Operator::Aggregate, Operator::SeqScan],
        &[PAYLOAD_COLUMN_ID],
        Observation {
            rows: 1,
            checksum: u128::from(rows.saturating_sub(1)) * 2,
        },
        settings,
        |result| text_extreme_observation(result, rows, true, 2),
        measurements,
    )?;
    run_attribution_query(
        "stream_aggregate_int_min",
        rows,
        "SELECT MIN(id) FROM items",
        &[Operator::Aggregate, Operator::SeqScan],
        &[ID_COLUMN_ID],
        Observation {
            rows: 1,
            checksum: 0,
        },
        settings,
        |result| integer_extreme_observation(result, rows, false, 1),
        measurements,
    )?;
    run_attribution_query(
        "stream_aggregate_int_max",
        rows,
        "SELECT MAX(id) FROM items",
        &[Operator::Aggregate, Operator::SeqScan],
        &[ID_COLUMN_ID],
        Observation {
            rows: 1,
            checksum: u128::from(rows.saturating_sub(1)),
        },
        settings,
        |result| integer_extreme_observation(result, rows, true, 1),
        measurements,
    )?;
    run_attribution_query(
        "stream_aggregate_int_min_max",
        rows,
        "SELECT MIN(id), MAX(id) FROM items",
        &[Operator::Aggregate, Operator::SeqScan],
        &[ID_COLUMN_ID],
        Observation {
            rows: 1,
            checksum: u128::from(rows.saturating_sub(1)),
        },
        settings,
        |result| integer_min_max_observation(result, rows),
        measurements,
    )?;
    run_attribution_query(
        "stream_aggregate_grouped_multi",
        rows,
        "SELECT team_id, COUNT(*), SUM(id), MIN(id), MAX(id) FROM items GROUP BY team_id",
        &[Operator::Aggregate, Operator::SeqScan],
        &[ID_COLUMN_ID, TEAM_COLUMN_ID],
        expected_grouped_aggregate(rows, 4, false),
        settings,
        |result| grouped_aggregate_observation(result, rows, 4, false),
        measurements,
    )?;
    run_attribution_query(
        "stream_aggregate_filtered_grouped",
        rows,
        "SELECT team_id, COUNT(*), SUM(id) FROM items WHERE active = true GROUP BY team_id",
        &[Operator::Aggregate, Operator::Filter, Operator::SeqScan],
        &[ID_COLUMN_ID, TEAM_COLUMN_ID, ACTIVE_COLUMN_ID],
        expected_grouped_aggregate(rows, 4, true),
        settings,
        |result| grouped_aggregate_observation(result, rows, 4, true),
        measurements,
    )?;
    for (scenario, cardinality) in [
        ("group_borrow_group_only_cardinality_1", 1),
        ("group_borrow_group_only_cardinality_4", 4),
    ] {
        run_group_lookup_attribution_query(
            scenario,
            rows,
            cardinality,
            "SELECT team_id FROM items GROUP BY team_id",
            &[Operator::Aggregate, Operator::SeqScan],
            &[TEAM_COLUMN_ID],
            Observation {
                rows: rows.min(cardinality),
                checksum: arithmetic_sum(rows.min(cardinality)),
            },
            settings,
            |result| group_only_observation(result, rows, cardinality),
            measurements,
        )?;
    }
    run_group_lookup_attribution_query(
        "group_borrow_sum_cardinality_1",
        rows,
        1,
        "SELECT team_id, SUM(id) FROM items GROUP BY team_id",
        &[Operator::Aggregate, Operator::SeqScan],
        &[ID_COLUMN_ID, TEAM_COLUMN_ID],
        Observation {
            rows: 1,
            checksum: arithmetic_sum(rows),
        },
        settings,
        |result| one_group_sum_observation(result, rows),
        measurements,
    )?;
    for (scenario, function, maximum) in [
        ("group_borrow_text_min_cardinality_1", "MIN", false),
        ("group_borrow_text_max_cardinality_1", "MAX", true),
    ] {
        run_group_lookup_attribution_query(
            scenario,
            rows,
            1,
            &format!("SELECT team_id, {function}(payload) FROM items GROUP BY team_id"),
            &[Operator::Aggregate, Operator::SeqScan],
            &[TEAM_COLUMN_ID, PAYLOAD_COLUMN_ID],
            Observation {
                rows: 1,
                checksum: u128::from(if maximum { rows.saturating_sub(1) } else { 0 }),
            },
            settings,
            |result| one_group_text_extreme_observation(result, rows, maximum),
            measurements,
        )?;
    }
    for (scenario, cardinality) in [
        ("group_lookup_int_cardinality_1", 1),
        ("group_lookup_int_cardinality_4", 4),
        (
            "group_lookup_int_cardinality_1_percent",
            (rows / 100).max(1),
        ),
        ("group_lookup_int_cardinality_half", (rows / 2).max(1)),
        ("group_lookup_int_cardinality_unique", rows.max(1)),
    ] {
        run_group_lookup_attribution_query(
            scenario,
            rows,
            cardinality,
            "SELECT team_id, COUNT(*) FROM items GROUP BY team_id",
            &[Operator::Aggregate, Operator::SeqScan],
            &[TEAM_COLUMN_ID],
            expected_groups(rows, cardinality),
            settings,
            |result| exact_group_observation(result, rows, cardinality),
            measurements,
        )?;
    }
    run_group_lookup_attribution_query(
        "group_lookup_two_primitive_keys",
        rows,
        4,
        "SELECT team_id, active, COUNT(*) FROM items GROUP BY team_id, active",
        &[Operator::Aggregate, Operator::SeqScan],
        &[TEAM_COLUMN_ID, ACTIVE_COLUMN_ID],
        expected_two_key_groups(rows, 4),
        settings,
        |result| two_key_group_observation(result, rows, 4),
        measurements,
    )?;
    run_group_lookup_attribution_query(
        "group_lookup_text_unique",
        rows,
        4,
        "SELECT payload, COUNT(*) FROM items GROUP BY payload",
        &[Operator::Aggregate, Operator::SeqScan],
        &[PAYLOAD_COLUMN_ID],
        Observation {
            rows,
            checksum: arithmetic_sum(rows),
        },
        settings,
        |result| unique_text_group_observation(result, rows),
        measurements,
    )?;
    for (scenario, sql, owners) in [
        (
            "group_move_text_key_only",
            "SELECT payload FROM items GROUP BY payload",
            1,
        ),
        (
            "group_move_text_max_overlap",
            "SELECT payload, MAX(payload) FROM items GROUP BY payload",
            2,
        ),
        (
            "group_move_text_duplicate_max_overlap",
            "SELECT payload, MAX(payload), MAX(payload) FROM items GROUP BY payload",
            3,
        ),
    ] {
        run_group_lookup_attribution_query(
            scenario,
            rows,
            4,
            sql,
            &[Operator::Aggregate, Operator::SeqScan],
            &[PAYLOAD_COLUMN_ID],
            Observation {
                rows,
                checksum: arithmetic_sum(rows),
            },
            settings,
            |result| unique_text_owner_observation(result, rows, owners),
            measurements,
        )?;
    }
    run_group_lookup_attribution_query(
        "group_move_wide_unique",
        rows,
        4,
        "SELECT id, payload, COUNT(*) FROM items GROUP BY id, payload",
        &[Operator::Aggregate, Operator::SeqScan],
        &[ID_COLUMN_ID, PAYLOAD_COLUMN_ID],
        Observation {
            rows,
            checksum: arithmetic_sum(rows),
        },
        settings,
        |result| unique_wide_group_observation(result, rows),
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
        "aggregate_count_star_triple",
        rows,
        "SELECT COUNT(*), COUNT(*), COUNT(*) FROM items",
        &[Operator::Aggregate, Operator::SeqScan],
        &[],
        Observation {
            rows: 1,
            checksum: u128::from(rows) * 3,
        },
        settings,
        |result| count_values_observation(result, &[rows, rows, rows]),
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
    run_attribution_query(
        "aggregate_count_payload_text_is_not_null_filter",
        rows,
        "SELECT COUNT(payload) FROM items WHERE payload IS NOT NULL",
        &[Operator::Aggregate, Operator::Filter, Operator::SeqScan],
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
        "aggregate_count_payload_id_is_not_null_filter",
        rows,
        "SELECT COUNT(payload) FROM items WHERE id IS NOT NULL",
        &[Operator::Aggregate, Operator::Filter, Operator::SeqScan],
        &[ID_COLUMN_ID, PAYLOAD_COLUMN_ID],
        Observation {
            rows: 1,
            checksum: u128::from(rows),
        },
        settings,
        count_observation,
        measurements,
    )?;
    let filtered_count = active_count(rows);
    run_attribution_query(
        "aggregate_count_id_filter",
        rows,
        "SELECT COUNT(id) FROM items WHERE active = true",
        &[Operator::Aggregate, Operator::Filter, Operator::SeqScan],
        &[ID_COLUMN_ID, ACTIVE_COLUMN_ID],
        Observation {
            rows: 1,
            checksum: u128::from(filtered_count),
        },
        settings,
        count_observation,
        measurements,
    )?;
    let filtered_nullable_count = active_non_null_count(rows, NullDistribution::Low);
    run_attribution_query(
        "aggregate_count_nullable_filter",
        rows,
        "SELECT COUNT(nullable_key) FROM items WHERE active = true",
        &[Operator::Aggregate, Operator::Filter, Operator::SeqScan],
        &[NULLABLE_COLUMN_ID, ACTIVE_COLUMN_ID],
        Observation {
            rows: 1,
            checksum: u128::from(filtered_nullable_count),
        },
        settings,
        count_observation,
        measurements,
    )?;
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
    run_attribution_query(
        "aggregate_count_star_payload_filter",
        rows,
        "SELECT COUNT(*), COUNT(payload) FROM items WHERE active = true",
        &[Operator::Aggregate, Operator::Filter, Operator::SeqScan],
        &[ACTIVE_COLUMN_ID, PAYLOAD_COLUMN_ID],
        Observation {
            rows: 1,
            checksum: u128::from(filtered_count) * 2,
        },
        settings,
        |result| count_values_observation(result, &[filtered_count, filtered_count]),
        measurements,
    )?;
    run_attribution_query(
        "aggregate_count_filtered_output_order",
        rows,
        "SELECT COUNT(payload), COUNT(*), COUNT(nullable_key), COUNT(payload), COUNT(id) FROM items WHERE active = true",
        &[Operator::Aggregate, Operator::Filter, Operator::SeqScan],
        &[
            ID_COLUMN_ID,
            NULLABLE_COLUMN_ID,
            ACTIVE_COLUMN_ID,
            PAYLOAD_COLUMN_ID,
        ],
        Observation {
            rows: 1,
            checksum: u128::from(filtered_count) * 4 + u128::from(filtered_nullable_count),
        },
        settings,
        |result| {
            count_values_observation(
                result,
                &[
                    filtered_count,
                    filtered_count,
                    filtered_nullable_count,
                    filtered_count,
                    filtered_count,
                ],
            )
        },
        measurements,
    )?;
    run_attribution_query(
        "aggregate_count_star_pair_filter_control",
        rows,
        "SELECT COUNT(*), COUNT(*) FROM items WHERE active = true",
        &[Operator::Aggregate, Operator::Filter, Operator::SeqScan],
        &[ACTIVE_COLUMN_ID],
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
        "aggregate_count_payload_id_filter",
        rows,
        &format!("SELECT COUNT(payload) FROM items WHERE id = {middle}"),
        &[Operator::Aggregate, Operator::Filter, Operator::SeqScan],
        &[ID_COLUMN_ID, PAYLOAD_COLUMN_ID],
        Observation {
            rows: 1,
            checksum: 1,
        },
        settings,
        count_observation,
        measurements,
    )?;
    run_attribution_query(
        "aggregate_count_payload_int_repeated_filter",
        rows,
        &format!("SELECT COUNT(payload) FROM items WHERE id >= {middle} AND id <= {middle}"),
        &[Operator::Aggregate, Operator::Filter, Operator::SeqScan],
        &[ID_COLUMN_ID, PAYLOAD_COLUMN_ID],
        Observation {
            rows: 1,
            checksum: 1,
        },
        settings,
        count_observation,
        measurements,
    )?;
    run_attribution_query(
        "aggregate_count_payload_lookup_wide_filter",
        rows,
        &format!(
            "SELECT COUNT(payload) FROM items WHERE id = id AND team_id = team_id AND bucket_id = bucket_id AND active = active AND id = {middle}"
        ),
        &[Operator::Aggregate, Operator::Filter, Operator::SeqScan],
        &[
            ID_COLUMN_ID,
            TEAM_COLUMN_ID,
            BUCKET_COLUMN_ID,
            ACTIVE_COLUMN_ID,
            PAYLOAD_COLUMN_ID,
        ],
        Observation {
            rows: 1,
            checksum: 1,
        },
        settings,
        count_observation,
        measurements,
    )?;
    run_attribution_query(
        "aggregate_count_payload_text_filter",
        rows,
        &format!("SELECT COUNT(payload) FROM items WHERE payload = 'payload-{middle:016}'"),
        &[Operator::Aggregate, Operator::Filter, Operator::SeqScan],
        &[PAYLOAD_COLUMN_ID],
        Observation {
            rows: 1,
            checksum: 1,
        },
        settings,
        count_observation,
        measurements,
    )?;
    run_attribution_query(
        "aggregate_count_payload_text_repeated_filter",
        rows,
        &format!(
            "SELECT COUNT(payload) FROM items WHERE payload >= 'payload-{middle:016}' AND payload <= 'payload-{middle:016}'"
        ),
        &[Operator::Aggregate, Operator::Filter, Operator::SeqScan],
        &[PAYLOAD_COLUMN_ID],
        Observation {
            rows: 1,
            checksum: 1,
        },
        settings,
        count_observation,
        measurements,
    )?;
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
    )?;
    run_attribution_query(
        "hidden_filter_id",
        rows,
        &format!("SELECT id FROM items WHERE id = {middle}"),
        &[Operator::Filter, Operator::Project, Operator::SeqScan],
        &[ID_COLUMN_ID],
        Observation {
            rows: 1,
            checksum: u128::from(middle),
        },
        settings,
        ids_observation,
        measurements,
    )?;
    run_attribution_query(
        "hidden_filter_payload_is_null",
        rows,
        "SELECT id FROM items WHERE payload IS NULL",
        &[Operator::Filter, Operator::Project, Operator::SeqScan],
        &[ID_COLUMN_ID, PAYLOAD_COLUMN_ID],
        Observation {
            rows: 0,
            checksum: 0,
        },
        settings,
        ids_observation,
        measurements,
    )?;
    run_attribution_query(
        "hidden_filter_id_is_null",
        rows,
        "SELECT id FROM items WHERE id IS NULL",
        &[Operator::Filter, Operator::Project, Operator::SeqScan],
        &[ID_COLUMN_ID],
        Observation {
            rows: 0,
            checksum: 0,
        },
        settings,
        ids_observation,
        measurements,
    )?;
    run_attribution_query(
        "hidden_filter_payload_is_not_null",
        rows,
        "SELECT id FROM items WHERE payload IS NOT NULL",
        &[Operator::Filter, Operator::Project, Operator::SeqScan],
        &[ID_COLUMN_ID, PAYLOAD_COLUMN_ID],
        Observation {
            rows,
            checksum: arithmetic_sum(rows),
        },
        settings,
        ids_observation,
        measurements,
    )?;
    run_attribution_query(
        "hidden_filter_payload_retained_all_true",
        rows,
        "SELECT payload FROM items WHERE payload IS NOT NULL",
        &[Operator::Filter, Operator::Project, Operator::SeqScan],
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
        "hidden_filter_id_is_not_null_all",
        rows,
        "SELECT id FROM items WHERE id IS NOT NULL",
        &[Operator::Filter, Operator::Project, Operator::SeqScan],
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
        "hidden_filter_owned_text_output",
        rows,
        &format!("SELECT payload FROM items WHERE id = {middle}"),
        &[Operator::Filter, Operator::Project, Operator::SeqScan],
        &[ID_COLUMN_ID, PAYLOAD_COLUMN_ID],
        Observation {
            rows: 1,
            checksum: u128::from(middle),
        },
        settings,
        |result| single_payload_observation(result, middle),
        measurements,
    )?;
    run_attribution_query(
        "hidden_filter_payload_repeated",
        rows,
        &format!(
            "SELECT id FROM items WHERE payload >= 'payload-{middle:016}' AND payload <= 'payload-{middle:016}'"
        ),
        &[Operator::Filter, Operator::Project, Operator::SeqScan],
        &[ID_COLUMN_ID, PAYLOAD_COLUMN_ID],
        Observation {
            rows: 1,
            checksum: u128::from(middle),
        },
        settings,
        ids_observation,
        measurements,
    )?;
    run_attribution_query(
        "hidden_filter_id_repeated",
        rows,
        &format!("SELECT id FROM items WHERE id >= {middle} AND id <= {middle}"),
        &[Operator::Filter, Operator::Project, Operator::SeqScan],
        &[ID_COLUMN_ID],
        Observation {
            rows: 1,
            checksum: u128::from(middle),
        },
        settings,
        ids_observation,
        measurements,
    )?;
    run_filter_position_attribution_query(
        "generic_filter_position_narrow",
        rows,
        &format!(
            "SELECT id, team_id, bucket_id, active FROM items WHERE id = id AND id = id AND id = id AND id = id AND id = {middle}"
        ),
        middle,
        settings,
        measurements,
    )?;
    run_filter_position_attribution_query(
        "generic_filter_position_wide",
        rows,
        &format!(
            "SELECT id, team_id, bucket_id, active FROM items WHERE id = id AND team_id = team_id AND bucket_id = bucket_id AND active = active AND id = {middle}"
        ),
        middle,
        settings,
        measurements,
    )?;
    run_attribution_query(
        "hidden_filter_lookup_wide_control",
        rows,
        &format!(
            "SELECT id FROM items WHERE id = id AND team_id = team_id AND bucket_id = bucket_id AND active = active AND id = {middle}"
        ),
        &[Operator::Filter, Operator::Project, Operator::SeqScan],
        &[
            ID_COLUMN_ID,
            TEAM_COLUMN_ID,
            BUCKET_COLUMN_ID,
            ACTIVE_COLUMN_ID,
        ],
        Observation {
            rows: 1,
            checksum: u128::from(middle),
        },
        settings,
        ids_observation,
        measurements,
    )
}

fn run_top_n_attribution_scenarios(
    settings: ProfileSettings,
    measurements: &mut Vec<Measurement>,
) -> BenchResult<()> {
    let rows = settings.medium_rows;
    let (mut database, paths) =
        items_fixture("top-n-attribution", rows, &[], NullDistribution::Low, 4)?;

    let mut expected = (0..rows).collect::<Vec<_>>();
    expected.sort_by_key(|id| (id % 4, *id));
    expected.truncate(usize::try_from(rows.min(1))?);
    measure_top_n_query(
        &mut database,
        "top_n_target_a_k_1",
        rows,
        "SELECT id FROM items ORDER BY team_id, id LIMIT 1",
        "Limit>Project>Sort>SeqScan",
        &[ID_COLUMN_ID, TEAM_COLUMN_ID],
        &expected,
        settings,
        measurements,
    )?;

    for limit in [1, 20, 256, 257, rows / 2, rows] {
        let mut expected = (0..rows).collect::<Vec<_>>();
        expected.sort_by_key(|id| id % 4);
        expected.truncate(usize::try_from(limit.min(rows))?);
        measure_top_n_query(
            &mut database,
            &format!("top_n_duplicate_k_{limit}"),
            rows,
            &format!("SELECT id FROM items ORDER BY team_id LIMIT {limit}"),
            "Limit>Project>Sort>SeqScan",
            &[ID_COLUMN_ID, TEAM_COLUMN_ID],
            &expected,
            settings,
            measurements,
        )?;
    }

    let mut expected = (0..rows).rev().collect::<Vec<_>>();
    expected.truncate(usize::try_from(rows.min(20))?);
    measure_top_n_query(
        &mut database,
        "top_n_unique_desc_k_20",
        rows,
        "SELECT id FROM items ORDER BY id DESC LIMIT 20",
        "Limit>Project>Sort>SeqScan",
        &[ID_COLUMN_ID],
        &expected,
        settings,
        measurements,
    )?;

    let mut expected = (0..rows).collect::<Vec<_>>();
    expected.sort_by(|left, right| (left % 4).cmp(&(right % 4)).then_with(|| right.cmp(left)));
    expected.truncate(usize::try_from(rows.min(20))?);
    measure_top_n_query(
        &mut database,
        "top_n_multi_key_k_20",
        rows,
        "SELECT id FROM items ORDER BY team_id ASC, id DESC LIMIT 20",
        "Limit>Project>Sort>SeqScan",
        &[ID_COLUMN_ID, TEAM_COLUMN_ID],
        &expected,
        settings,
        measurements,
    )?;

    let mut expected = (0..rows).filter(|id| id % 3 == 0).collect::<Vec<_>>();
    expected.sort_by_key(|id| id % 4);
    expected.truncate(usize::try_from(rows.min(20))?);
    measure_top_n_query(
        &mut database,
        "top_n_filtered_k_20",
        rows,
        "SELECT id FROM items WHERE active = true ORDER BY team_id LIMIT 20",
        "Limit>Project>Sort>Filter>SeqScan",
        &[ID_COLUMN_ID, TEAM_COLUMN_ID, ACTIVE_COLUMN_ID],
        &expected,
        settings,
        measurements,
    )?;

    let mut expected = (0..rows).rev().collect::<Vec<_>>();
    expected.truncate(usize::try_from(rows.min(20))?);
    measure_top_n_query(
        &mut database,
        "top_n_text_desc_k_20",
        rows,
        "SELECT id FROM items ORDER BY payload DESC LIMIT 20",
        "Limit>Project>Sort>SeqScan",
        &[ID_COLUMN_ID, PAYLOAD_COLUMN_ID],
        &expected,
        settings,
        measurements,
    )?;

    for (scenario, direction, null_order, nulls_first) in [
        ("top_n_nullable_asc_first", "ASC", "FIRST", true),
        ("top_n_nullable_asc_last", "ASC", "LAST", false),
        ("top_n_nullable_desc_first", "DESC", "FIRST", true),
        ("top_n_nullable_desc_last", "DESC", "LAST", false),
    ] {
        let descending = direction == "DESC";
        let mut expected = (0..rows).collect::<Vec<_>>();
        expected.sort_by(|left, right| {
            let left_null = NullDistribution::Low.is_null(*left);
            let right_null = NullDistribution::Low.is_null(*right);
            match (left_null, right_null) {
                (true, true) => left.cmp(right),
                (true, false) => {
                    if nulls_first {
                        std::cmp::Ordering::Less
                    } else {
                        std::cmp::Ordering::Greater
                    }
                }
                (false, true) => {
                    if nulls_first {
                        std::cmp::Ordering::Greater
                    } else {
                        std::cmp::Ordering::Less
                    }
                }
                (false, false) if descending => right.cmp(left),
                (false, false) => left.cmp(right),
            }
        });
        expected.truncate(usize::try_from(rows.min(20))?);
        measure_top_n_query(
            &mut database,
            scenario,
            rows,
            &format!(
                "SELECT id FROM items ORDER BY nullable_key {direction} NULLS {null_order} LIMIT 20"
            ),
            "Limit>Project>Sort>SeqScan",
            &[ID_COLUMN_ID, NULLABLE_COLUMN_ID],
            &expected,
            settings,
            measurements,
        )?;
    }

    let mut expected = (0..rows).collect::<Vec<_>>();
    expected.sort_by_key(|id| id % 4);
    measure_top_n_query(
        &mut database,
        "full_sort_duplicate_control",
        rows,
        "SELECT id FROM items ORDER BY team_id",
        "Project>Sort>SeqScan",
        &[ID_COLUMN_ID, TEAM_COLUMN_ID],
        &expected,
        settings,
        measurements,
    )?;

    database.close()?;
    paths.cleanup()
}

#[allow(clippy::too_many_arguments)]
fn measure_top_n_query(
    database: &mut Database,
    scenario: &str,
    fixture_rows: u64,
    sql: &str,
    expected_plan: &str,
    expected_base_columns: &[ColumnId],
    expected_ids: &[u64],
    settings: ProfileSettings,
    measurements: &mut Vec<Measurement>,
) -> BenchResult<()> {
    let plan = inspect_plan(
        database,
        scenario,
        sql,
        &[Operator::Project, Operator::Sort, Operator::SeqScan],
        &[],
    )?;
    if plan != expected_plan {
        return Err(message_error(format!(
            "scenario `{scenario}` plan was `{plan}`; expected `{expected_plan}`"
        )));
    }
    inspect_base_scan_columns(database, scenario, sql, expected_base_columns)?;
    let expected = expected_ids_observation(expected_ids)?;
    let durations = measure_checked(
        scenario,
        settings.query_warmup,
        settings.query_iterations,
        expected,
        || database.query(sql).map_err(Into::into),
        |result| ordered_ids_observation(result, expected_ids),
    )?;
    measurements.push(Measurement {
        scenario: scenario.to_owned(),
        rows: format!("{fixture_rows}/k={}", expected_ids.len()),
        plan,
        operations_per_iteration: 1,
        durations,
    });
    Ok(())
}

fn run_filter_position_attribution_query(
    scenario: &str,
    rows: u64,
    sql: &str,
    expected_id: u64,
    settings: ProfileSettings,
    measurements: &mut Vec<Measurement>,
) -> BenchResult<()> {
    let (mut database, paths) = items_fixture(scenario, rows, &[], NullDistribution::Low, 4)?;
    let plan = inspect_plan(
        &database,
        scenario,
        sql,
        &[Operator::Project, Operator::Filter, Operator::SeqScan],
        &[],
    )?;
    if plan != "Project>Filter>SeqScan" {
        return Err(message_error(format!(
            "scenario `{scenario}` plan was `{plan}`; expected `Project>Filter>SeqScan`"
        )));
    }
    inspect_base_scan_columns(
        &database,
        scenario,
        sql,
        &[
            ID_COLUMN_ID,
            TEAM_COLUMN_ID,
            BUCKET_COLUMN_ID,
            ACTIVE_COLUMN_ID,
        ],
    )?;
    let expected = Observation {
        rows: 1,
        checksum: u128::from(expected_id),
    };
    let durations = measure_checked(
        scenario,
        settings.query_warmup,
        settings.query_iterations,
        expected,
        || database.query(sql).map_err(Into::into),
        |result| primitive_item_observation(result, expected_id),
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
    run_group_lookup_attribution_query(
        scenario,
        rows,
        4,
        sql,
        required,
        expected_base_columns,
        expected,
        settings,
        observe,
        measurements,
    )
}

#[allow(clippy::too_many_arguments)]
fn run_group_lookup_attribution_query(
    scenario: &str,
    rows: u64,
    team_cardinality: u64,
    sql: &str,
    required: &[Operator],
    expected_base_columns: &[ColumnId],
    expected: Observation,
    settings: ProfileSettings,
    observe: impl Fn(&QueryResult) -> BenchResult<Observation>,
    measurements: &mut Vec<Measurement>,
) -> BenchResult<()> {
    let (mut database, paths) =
        items_fixture(scenario, rows, &[], NullDistribution::Low, team_cardinality)?;
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

fn run_text_comparison_attribution_query(
    scenario: &str,
    rows: u64,
    shape: TextComparisonShape,
    settings: ProfileSettings,
    measurements: &mut Vec<Measurement>,
) -> BenchResult<()> {
    let (mut database, paths) = text_comparison_fixture(scenario, rows, shape)?;
    let sql = "SELECT MIN(payload) FROM items";
    let plan = inspect_plan(
        &database,
        scenario,
        sql,
        &[Operator::Aggregate, Operator::SeqScan],
        &[],
    )?;
    inspect_base_scan_columns(&database, scenario, sql, &[PAYLOAD_COLUMN_ID])?;
    let expected_payload = shape.payload(0);
    let expected = Observation {
        rows: 1,
        checksum: 0,
    };
    let durations = measure_checked(
        scenario,
        settings.query_warmup,
        settings.query_iterations,
        expected,
        || database.query(sql).map_err(Into::into),
        |result| exact_text_extreme_observation(result, &expected_payload),
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
    run_phase66_range_attribution(settings, measurements)?;
    Ok(())
}

fn run_phase66_range_attribution(
    settings: ProfileSettings,
    measurements: &mut Vec<Measurement>,
) -> BenchResult<()> {
    let rows = settings.medium_rows;
    let (mut database, paths) = items_fixture(
        "phase66-range-attribution",
        rows,
        &[ID_COLUMN_ID],
        NullDistribution::Low,
        4,
    )?;
    let start = rows / 3;
    for requested_rows in [1_u64, 20, 255, 256, 257] {
        let end = start
            .checked_add(requested_rows)
            .ok_or_else(|| message_error("range attribution endpoint overflow"))?;
        if end > rows {
            continue;
        }
        let scenario = format!("range_cardinality_{requested_rows}");
        let sql = format!("SELECT id FROM items WHERE id >= {start} AND id < {end}");
        let plan = inspect_range_attribution_plan(&database, &scenario, &sql, requested_rows)?;
        let expected = expected_range_ids(start, end);
        let durations = measure_checked(
            &scenario,
            settings.query_warmup,
            settings.query_iterations,
            expected,
            || database.query(&sql).map_err(Into::into),
            ids_observation,
        )?;
        measurements.push(Measurement {
            scenario,
            rows: requested_rows.to_string(),
            plan,
            operations_per_iteration: 1,
            durations,
        });
    }

    let aggregate_rows = 257_u64.min(rows.saturating_sub(start));
    if aggregate_rows > 0 {
        let end = start
            .checked_add(aggregate_rows)
            .ok_or_else(|| message_error("range aggregate endpoint overflow"))?;
        let scenario = "range_aggregate_cardinality_257";
        let sql = format!("SELECT SUM(id) FROM items WHERE id >= {start} AND id < {end}");
        let plan = inspect_range_attribution_plan(&database, scenario, &sql, aggregate_rows)?;
        let expected = Observation {
            rows: 1,
            checksum: arithmetic_sum(end) - arithmetic_sum(start),
        };
        let durations = measure_checked(
            scenario,
            settings.query_warmup,
            settings.query_iterations,
            expected,
            || database.query(&sql).map_err(Into::into),
            sum_observation,
        )?;
        measurements.push(Measurement {
            scenario: scenario.to_owned(),
            rows: aggregate_rows.to_string(),
            plan,
            operations_per_iteration: 1,
            durations,
        });
    }
    database.close()?;
    paths.cleanup()
}

fn inspect_range_attribution_plan(
    database: &Database,
    scenario: &str,
    sql: &str,
    expected_rows: u64,
) -> BenchResult<String> {
    let inspection = database.inspect_statement(sql)?;
    let root = query_root(&inspection)?;
    if !contains_operator(root, Operator::Filter) {
        return Err(message_error(format!(
            "scenario `{scenario}` is missing its residual Filter: {}",
            plan_label(root)
        )));
    }
    let range = contains_operator(root, Operator::RangeIndexScan);
    let sequence = contains_operator(root, Operator::SeqScan);
    if range == sequence || contains_operator(root, Operator::IndexScan) {
        return Err(message_error(format!(
            "scenario `{scenario}` expected exactly one RangeIndexScan/SeqScan source: {}",
            plan_label(root)
        )));
    }
    let access = if range {
        "RangeIndexScan"
    } else {
        "SeqScan transition"
    };
    Ok(format!(
        "{} [planned result rows={expected_rows} access={access}]",
        plan_label(root)
    ))
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
    run_phase65_hash_join_scenarios(settings, measurements)?;
    run_phase68_hash_join_scenarios(settings, measurements)?;
    Ok(())
}

fn run_phase65_hash_join_scenarios(
    settings: ProfileSettings,
    measurements: &mut Vec<Measurement>,
) -> BenchResult<()> {
    let probe_rows = settings.phase65_probe_rows;
    let build_rows = settings.phase65_build_rows;
    let unique_ids = (0..build_rows).collect::<Vec<_>>();
    let duplicate_cardinality = (build_rows / 4).max(1);
    let duplicate_ids = (0..duplicate_cardinality)
        .flat_map(|id| std::iter::repeat_n(id, 4))
        .collect::<Vec<_>>();
    let residual_ids = (build_rows..build_rows * 2).collect::<Vec<_>>();
    let residual_sql = format!(
        "SELECT l.id FROM left_rows l JOIN right_rows r ON l.join_key = r.join_key AND l.id > r.id AND l.id < {}",
        build_rows * 2
    );

    for scenario in [
        AsymmetricHashJoinScenario {
            name: "hash_join_probe_large_build_small_none",
            left_rows: probe_rows,
            right_rows: build_rows,
            left_cardinality: probe_rows,
            right_cardinality: build_rows,
            left_key_offset: 0,
            right_key_offset: probe_rows,
            sql: "SELECT l.id FROM left_rows l JOIN right_rows r ON l.join_key = r.join_key",
            expected_ids: &[],
        },
        AsymmetricHashJoinScenario {
            name: "hash_join_probe_small_build_large_none",
            left_rows: build_rows,
            right_rows: probe_rows,
            left_cardinality: build_rows,
            right_cardinality: probe_rows,
            left_key_offset: 0,
            right_key_offset: build_rows,
            sql: "SELECT l.id FROM left_rows l JOIN right_rows r ON l.join_key = r.join_key",
            expected_ids: &[],
        },
        AsymmetricHashJoinScenario {
            name: "hash_join_probe_large_build_small_unique",
            left_rows: probe_rows,
            right_rows: build_rows,
            left_cardinality: probe_rows,
            right_cardinality: build_rows,
            left_key_offset: 0,
            right_key_offset: 0,
            sql: "SELECT l.id FROM left_rows l JOIN right_rows r ON l.join_key = r.join_key",
            expected_ids: &unique_ids,
        },
        AsymmetricHashJoinScenario {
            name: "hash_join_probe_large_build_small_duplicate",
            left_rows: probe_rows,
            right_rows: build_rows,
            left_cardinality: probe_rows,
            right_cardinality: duplicate_cardinality,
            left_key_offset: 0,
            right_key_offset: 0,
            sql: "SELECT l.id FROM left_rows l JOIN right_rows r ON l.join_key = r.join_key",
            expected_ids: &duplicate_ids,
        },
        AsymmetricHashJoinScenario {
            name: "hash_join_probe_large_build_small_residual",
            left_rows: probe_rows,
            right_rows: build_rows,
            left_cardinality: build_rows,
            right_cardinality: build_rows,
            left_key_offset: 0,
            right_key_offset: 0,
            sql: &residual_sql,
            expected_ids: &residual_ids,
        },
    ] {
        run_asymmetric_hash_join_query(scenario, settings, measurements)?;
    }
    Ok(())
}

struct AsymmetricHashJoinScenario<'a> {
    name: &'a str,
    left_rows: u64,
    right_rows: u64,
    left_cardinality: u64,
    right_cardinality: u64,
    left_key_offset: u64,
    right_key_offset: u64,
    sql: &'a str,
    expected_ids: &'a [u64],
}

fn run_asymmetric_hash_join_query(
    scenario: AsymmetricHashJoinScenario<'_>,
    settings: ProfileSettings,
    measurements: &mut Vec<Measurement>,
) -> BenchResult<()> {
    let paths = FixturePaths::new(scenario.name, 2);
    let mut database = Database::create_tables(vec![
        (
            paths.path(0).to_path_buf(),
            join_table(LEFT_TABLE_ID, "left_rows"),
        ),
        (
            paths.path(1).to_path_buf(),
            join_table(RIGHT_TABLE_ID, "right_rows"),
        ),
    ])?;
    load_join_rows(
        &mut database,
        LEFT_TABLE_ID,
        scenario.left_rows,
        scenario.left_cardinality,
        scenario.left_key_offset,
    )?;
    load_join_rows(
        &mut database,
        RIGHT_TABLE_ID,
        scenario.right_rows,
        scenario.right_cardinality,
        scenario.right_key_offset,
    )?;
    database.analyze(LEFT_TABLE_ID)?;
    database.analyze(RIGHT_TABLE_ID)?;
    let plan = inspect_plan(
        &database,
        scenario.name,
        scenario.sql,
        &[Operator::HashJoin, Operator::SeqScan],
        &[Operator::NestedLoopJoin, Operator::IndexScan],
    )?;
    let expected = expected_ids_observation(scenario.expected_ids)?;
    let durations = measure_checked(
        scenario.name,
        settings.query_warmup,
        settings.join_iterations,
        expected,
        || database.query(scenario.sql).map_err(Into::into),
        |result| ordered_ids_observation(result, scenario.expected_ids),
    )?;
    database.close()?;
    paths.cleanup()?;
    measurements.push(Measurement {
        scenario: scenario.name.to_owned(),
        rows: format!("{}x{}", scenario.left_rows, scenario.right_rows),
        plan,
        operations_per_iteration: 1,
        durations,
    });
    Ok(())
}

fn run_phase68_hash_join_scenarios(
    settings: ProfileSettings,
    measurements: &mut Vec<Measurement>,
) -> BenchResult<()> {
    let probe_rows = settings.phase65_build_rows;
    let build_rows = settings.phase65_probe_rows;
    let matching_ids = (0..probe_rows).collect::<Vec<_>>();
    let duplicate_build_rows = settings.phase65_build_rows;
    let duplicate_cardinality = 4_u64;
    let duplicate_probe_rows = duplicate_cardinality;
    let duplicate_step = usize::try_from(duplicate_cardinality)?;
    let duplicate_ids = (0..duplicate_probe_rows)
        .flat_map(|key| (key..duplicate_build_rows).step_by(duplicate_step))
        .collect::<Vec<_>>();

    for scenario in [
        Phase68TextHashJoinScenario {
            name: "phase68_hash_join_text_unique_short_none",
            left_rows: probe_rows,
            right_rows: build_rows,
            left_cardinality: probe_rows,
            right_cardinality: build_rows,
            left_key_offset: 0,
            right_key_offset: build_rows,
            key_width: 8,
            projects_right_id: false,
            sql: "SELECT l.id FROM left_rows l JOIN right_rows r ON l.join_key = r.join_key",
            expected_ids: &[],
        },
        Phase68TextHashJoinScenario {
            name: "phase68_hash_join_text_unique_long_none",
            left_rows: probe_rows,
            right_rows: build_rows,
            left_cardinality: probe_rows,
            right_cardinality: build_rows,
            left_key_offset: 0,
            right_key_offset: build_rows,
            key_width: 128,
            projects_right_id: false,
            sql: "SELECT l.id FROM left_rows l JOIN right_rows r ON l.join_key = r.join_key",
            expected_ids: &[],
        },
        Phase68TextHashJoinScenario {
            name: "phase68_hash_join_text_duplicate_none",
            left_rows: probe_rows,
            right_rows: build_rows,
            left_cardinality: probe_rows,
            right_cardinality: probe_rows,
            left_key_offset: 0,
            right_key_offset: build_rows,
            key_width: 128,
            projects_right_id: false,
            sql: "SELECT l.id FROM left_rows l JOIN right_rows r ON l.join_key = r.join_key",
            expected_ids: &[],
        },
        Phase68TextHashJoinScenario {
            name: "phase68_hash_join_text_matching",
            left_rows: probe_rows,
            right_rows: build_rows,
            left_cardinality: probe_rows,
            right_cardinality: build_rows,
            left_key_offset: 0,
            right_key_offset: 0,
            key_width: 128,
            projects_right_id: false,
            sql: "SELECT l.id FROM left_rows l JOIN right_rows r ON l.join_key = r.join_key",
            expected_ids: &matching_ids,
        },
        Phase68TextHashJoinScenario {
            name: "phase68_hash_join_text_duplicate_matching_order",
            left_rows: duplicate_probe_rows,
            right_rows: duplicate_build_rows,
            left_cardinality: duplicate_cardinality,
            right_cardinality: duplicate_cardinality,
            left_key_offset: 0,
            right_key_offset: 0,
            key_width: 8,
            projects_right_id: true,
            sql: "SELECT r.id FROM left_rows l JOIN right_rows r ON l.join_key = r.join_key",
            expected_ids: &duplicate_ids,
        },
    ] {
        run_phase68_text_hash_join_query(scenario, settings, measurements)?;
    }
    Ok(())
}

struct Phase68TextHashJoinScenario<'a> {
    name: &'a str,
    left_rows: u64,
    right_rows: u64,
    left_cardinality: u64,
    right_cardinality: u64,
    left_key_offset: u64,
    right_key_offset: u64,
    key_width: usize,
    projects_right_id: bool,
    sql: &'a str,
    expected_ids: &'a [u64],
}

fn run_phase68_text_hash_join_query(
    scenario: Phase68TextHashJoinScenario<'_>,
    settings: ProfileSettings,
    measurements: &mut Vec<Measurement>,
) -> BenchResult<()> {
    let paths = FixturePaths::new(scenario.name, 2);
    let mut database = Database::create_tables(vec![
        (
            paths.path(0).to_path_buf(),
            text_join_table(LEFT_TABLE_ID, "left_rows"),
        ),
        (
            paths.path(1).to_path_buf(),
            text_join_table(RIGHT_TABLE_ID, "right_rows"),
        ),
    ])?;
    load_fixed_text_join_rows(
        &mut database,
        LEFT_TABLE_ID,
        scenario.left_rows,
        scenario.left_cardinality,
        scenario.left_key_offset,
        scenario.key_width,
    )?;
    load_fixed_text_join_rows(
        &mut database,
        RIGHT_TABLE_ID,
        scenario.right_rows,
        scenario.right_cardinality,
        scenario.right_key_offset,
        scenario.key_width,
    )?;
    database.analyze(LEFT_TABLE_ID)?;
    database.analyze(RIGHT_TABLE_ID)?;
    let plan = inspect_plan(
        &database,
        scenario.name,
        scenario.sql,
        &[Operator::HashJoin, Operator::SeqScan],
        &[Operator::NestedLoopJoin, Operator::IndexScan],
    )?;
    inspect_phase68_hash_join_shape(
        &database,
        scenario.name,
        scenario.sql,
        scenario.projects_right_id,
    )?;
    let expected = expected_ids_observation(scenario.expected_ids)?;
    let durations = measure_checked(
        scenario.name,
        settings.query_warmup,
        settings.join_iterations,
        expected,
        || database.query(scenario.sql).map_err(Into::into),
        |result| ordered_ids_observation(result, scenario.expected_ids),
    )?;
    database.close()?;
    paths.cleanup()?;
    measurements.push(Measurement {
        scenario: scenario.name.to_owned(),
        rows: format!("{}x{}", scenario.left_rows, scenario.right_rows),
        plan: format!("{plan} [right-build Text/{}]", scenario.key_width),
        operations_per_iteration: 1,
        durations,
    });
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

fn text_comparison_fixture(
    scenario: &str,
    rows: u64,
    shape: TextComparisonShape,
) -> BenchResult<(Database, FixturePaths)> {
    let paths = FixturePaths::new(scenario, 1);
    let mut database = Database::create(paths.path(0), items_table())?;
    let mut transaction = database.begin_transaction_for(ITEMS_TABLE_ID)?;
    for id in 0..rows {
        let mut row = item_row(id, 4, NullDistribution::Low)?;
        row[5] = ScalarValue::Text(shape.payload(id));
        database.insert_into_in(ITEMS_TABLE_ID, &mut transaction, &row)?;
    }
    transaction.commit()?;
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

fn load_fixed_text_join_rows(
    database: &mut Database,
    table_id: TableId,
    rows: u64,
    cardinality: u64,
    key_offset: u64,
    key_width: usize,
) -> BenchResult<()> {
    if cardinality == 0 {
        return Err(message_error("Text join cardinality must be nonzero"));
    }
    let mut transaction = database.begin_transaction_for(table_id)?;
    for id in 0..rows {
        let id_value = i64::try_from(id).map_err(|_| message_error("join ID exceeds i64"))?;
        let key = id
            .checked_rem(cardinality)
            .and_then(|key| key.checked_add(key_offset))
            .ok_or_else(|| message_error("Text join key overflow"))?;
        let digits = key.to_string();
        if digits.len() > key_width {
            return Err(message_error("Text join key exceeds fixed width"));
        }
        let mut key_value = String::with_capacity(key_width);
        key_value.extend(std::iter::repeat_n('0', key_width - digits.len()));
        key_value.push_str(&digits);
        database.insert_into_in(
            table_id,
            &mut transaction,
            &[ScalarValue::Int64(id_value), ScalarValue::Text(key_value)],
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

fn inspect_phase68_hash_join_shape(
    database: &Database,
    scenario: &str,
    sql: &str,
    projects_right_id: bool,
) -> BenchResult<()> {
    let inspection = database.inspect_statement(sql)?;
    let root = query_root(&inspection)?;
    let join = find_hash_join(root)
        .ok_or_else(|| message_error(format!("scenario `{scenario}` is not HashJoin")))?;
    let PlanNodeInspection::HashJoin {
        left_key,
        right_key,
        left,
        right,
        ..
    } = join
    else {
        return Err(message_error(format!(
            "scenario `{scenario}` is not HashJoin"
        )));
    };
    let join_key_column = ColumnId(2);
    let text_type = netbadb_types::SemanticType::physical(PhysicalType::Text);
    if left_key.table_id != LEFT_TABLE_ID
        || left_key.column_id != join_key_column
        || left_key.data_type != text_type
        || right_key.table_id != RIGHT_TABLE_ID
        || right_key.column_id != join_key_column
        || right_key.data_type != text_type
    {
        return Err(message_error(format!(
            "scenario `{scenario}` HashJoin keys do not preserve left-probe/right-build Text provenance"
        )));
    }
    let expected_left_columns = if projects_right_id {
        vec![join_key_column]
    } else {
        vec![ID_COLUMN_ID, join_key_column]
    };
    let expected_right_columns = if projects_right_id {
        vec![ID_COLUMN_ID, join_key_column]
    } else {
        vec![join_key_column]
    };
    if !direct_seq_scan_matches(left, LEFT_TABLE_ID, &expected_left_columns)
        || !direct_seq_scan_matches(right, RIGHT_TABLE_ID, &expected_right_columns)
    {
        return Err(message_error(format!(
            "scenario `{scenario}` does not use the expected direct left-probe/right-build SeqScan columns"
        )));
    }
    Ok(())
}

fn find_hash_join(plan: &PlanNodeInspection) -> Option<&PlanNodeInspection> {
    match plan {
        PlanNodeInspection::HashJoin { .. } => Some(plan),
        PlanNodeInspection::Filter { input, .. }
        | PlanNodeInspection::Sort { input, .. }
        | PlanNodeInspection::Project { input, .. }
        | PlanNodeInspection::Aggregate { input, .. }
        | PlanNodeInspection::Limit { input, .. } => find_hash_join(input),
        PlanNodeInspection::SeqScan { .. }
        | PlanNodeInspection::IndexScan { .. }
        | PlanNodeInspection::RangeIndexScan { .. }
        | PlanNodeInspection::PartitionedScan { .. }
        | PlanNodeInspection::NestedLoopJoin { .. } => None,
    }
}

fn direct_seq_scan_matches(
    plan: &PlanNodeInspection,
    expected_table: TableId,
    expected_columns: &[ColumnId],
) -> bool {
    let PlanNodeInspection::SeqScan {
        table_id, columns, ..
    } = plan
    else {
        return false;
    };
    *table_id == expected_table
        && columns
            .iter()
            .map(|column| column.column_id)
            .eq(expected_columns.iter().copied())
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
        PlanNodeInspection::PartitionedScan { columns, .. } => {
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
            PlanNodeInspection::PartitionedScan { .. } => false,
        }
}

const fn operator(plan: &PlanNodeInspection) -> Operator {
    match plan {
        PlanNodeInspection::SeqScan { .. } => Operator::SeqScan,
        PlanNodeInspection::IndexScan { .. } => Operator::IndexScan,
        PlanNodeInspection::RangeIndexScan { .. } => Operator::RangeIndexScan,
        PlanNodeInspection::PartitionedScan { .. } => Operator::SeqScan,
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
        PlanNodeInspection::PartitionedScan { .. } => {}
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

fn expected_ids_observation(ids: &[u64]) -> BenchResult<Observation> {
    Ok(Observation {
        rows: u64::try_from(ids.len())
            .map_err(|_| message_error("expected result row count exceeds u64"))?,
        checksum: ids.iter().try_fold(0_u128, |checksum, id| {
            checksum
                .checked_add(u128::from(*id))
                .ok_or_else(|| message_error("expected ID checksum overflow"))
        })?,
    })
}

fn ordered_ids_observation(result: &QueryResult, expected_ids: &[u64]) -> BenchResult<Observation> {
    let actual = result
        .rows
        .iter()
        .map(|row| {
            let [ScalarValue::Int64(id)] = row.as_slice() else {
                return Err(message_error(
                    "ordered query must return one non-NULL Int64 ID column",
                ));
            };
            u64::try_from(*id).map_err(|_| message_error("ordered query returned a negative ID"))
        })
        .collect::<BenchResult<Vec<_>>>()?;
    if actual != expected_ids {
        let mismatch = actual
            .iter()
            .zip(expected_ids)
            .position(|(actual, expected)| actual != expected)
            .unwrap_or(actual.len().min(expected_ids.len()));
        return Err(message_error(format!(
            "ordered query diverged at row {mismatch}: actual={:?}, expected={:?}",
            actual.get(mismatch),
            expected_ids.get(mismatch)
        )));
    }
    expected_ids_observation(&actual)
}

fn primitive_item_observation(result: &QueryResult, expected_id: u64) -> BenchResult<Observation> {
    let [row] = result.rows.as_slice() else {
        return Err(message_error("expected exactly one primitive item row"));
    };
    let [
        ScalarValue::Int64(id),
        ScalarValue::Int64(team_id),
        ScalarValue::Int64(bucket_id),
        ScalarValue::Bool(active),
    ] = row.as_slice()
    else {
        return Err(message_error(
            "primitive item query must return Int64, Int64, Int64, and Bool columns",
        ));
    };
    let expected_id = i64::try_from(expected_id)
        .map_err(|_| message_error("expected primitive item ID exceeds i64"))?;
    let expected_team_id = expected_id % 4;
    let expected_active = expected_id % 3 == 0;
    if (*id, *team_id, *bucket_id, *active)
        != (expected_id, expected_team_id, expected_id, expected_active)
    {
        return Err(message_error(
            "primitive item row values did not match fixture",
        ));
    }
    Ok(Observation {
        rows: 1,
        checksum: u128::try_from(expected_id)
            .map_err(|_| message_error("negative primitive item ID"))?,
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

fn single_payload_observation(result: &QueryResult, expected_id: u64) -> BenchResult<Observation> {
    let [row] = result.rows.as_slice() else {
        return Err(message_error("expected exactly one payload row"));
    };
    let [ScalarValue::Text(payload)] = row.as_slice() else {
        return Err(message_error(
            "single payload query must return one non-NULL Text column",
        ));
    };
    let expected_id = usize::try_from(expected_id)
        .map_err(|_| message_error("expected payload ID exceeds usize"))?;
    validate_payload(expected_id, payload)?;
    Ok(Observation {
        rows: 1,
        checksum: expected_id as u128,
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

fn sum_observation(result: &QueryResult) -> BenchResult<Observation> {
    let [row] = result.rows.as_slice() else {
        return Err(message_error("SUM query must return one row"));
    };
    let [ScalarValue::Int64(sum)] = row.as_slice() else {
        return Err(message_error(
            "SUM query must return one non-NULL Int64 column",
        ));
    };
    Ok(Observation {
        rows: 1,
        checksum: u128::try_from(*sum).map_err(|_| message_error("negative SUM result"))?,
    })
}

fn expected_global_multi(rows: u64) -> Observation {
    Observation {
        rows: 1,
        checksum: arithmetic_sum(rows) + u128::from(rows.saturating_sub(1)),
    }
}

fn global_multi_observation(result: &QueryResult, fixture_rows: u64) -> BenchResult<Observation> {
    let [row] = result.rows.as_slice() else {
        return Err(message_error("global multi aggregate must return one row"));
    };
    let [
        ScalarValue::Int64(sum),
        ScalarValue::Int64(min),
        ScalarValue::Int64(max),
    ] = row.as_slice()
    else {
        return Err(message_error(
            "global multi aggregate must return three non-NULL Int64 columns",
        ));
    };
    let expected_sum = i64::try_from(arithmetic_sum(fixture_rows))
        .map_err(|_| message_error("fixture SUM exceeds i64"))?;
    let expected_max = i64::try_from(fixture_rows.saturating_sub(1))
        .map_err(|_| message_error("fixture MAX exceeds i64"))?;
    if (*sum, *min, *max) != (expected_sum, 0, expected_max) {
        return Err(message_error(format!(
            "global multi aggregate returned ({sum}, {min}, {max}), expected ({expected_sum}, 0, {expected_max})"
        )));
    }
    Ok(expected_global_multi(fixture_rows))
}

fn primitive_values_expected(values: &[ScalarValue]) -> BenchResult<Observation> {
    Ok(Observation {
        rows: 1,
        checksum: primitive_values_checksum(values)?,
    })
}

fn primitive_values_observation(
    result: &QueryResult,
    expected: &[ScalarValue],
) -> BenchResult<Observation> {
    let [row] = result.rows.as_slice() else {
        return Err(message_error(
            "primitive aggregate query must return one row",
        ));
    };
    if row != expected {
        return Err(message_error(format!(
            "primitive aggregate returned {row:?}; expected {expected:?}"
        )));
    }
    primitive_values_expected(row)
}

fn primitive_values_checksum(values: &[ScalarValue]) -> BenchResult<u128> {
    values.iter().try_fold(0_u128, |checksum, value| {
        let value = match value {
            ScalarValue::Null => 0,
            ScalarValue::Bool(value) => u128::from(*value),
            ScalarValue::Int64(value) => u128::try_from(*value)
                .map_err(|_| message_error("negative primitive aggregate result"))?,
            ScalarValue::UInt64(value) => u128::from(*value),
            ScalarValue::Text(_) => {
                return Err(message_error(
                    "primitive aggregate result unexpectedly contained Text",
                ));
            }
        };
        checksum
            .checked_add(value)
            .ok_or_else(|| message_error("primitive aggregate checksum overflow"))
    })
}

fn text_min_max_observation(result: &QueryResult, fixture_rows: u64) -> BenchResult<Observation> {
    let [row] = result.rows.as_slice() else {
        return Err(message_error("Text MIN/MAX must return one row"));
    };
    let [ScalarValue::Text(min), ScalarValue::Text(max)] = row.as_slice() else {
        return Err(message_error(
            "Text MIN/MAX must return two non-NULL Text columns",
        ));
    };
    let expected_min = "payload-0000000000000000";
    let expected_max = format!("payload-{:016}", fixture_rows.saturating_sub(1));
    if min != expected_min || max != &expected_max {
        return Err(message_error(format!(
            "Text MIN/MAX returned ({min}, {max}), expected ({expected_min}, {expected_max})"
        )));
    }
    Ok(Observation {
        rows: 1,
        checksum: u128::from(fixture_rows.saturating_sub(1)),
    })
}

fn exact_text_extreme_observation(
    result: &QueryResult,
    expected: &str,
) -> BenchResult<Observation> {
    let [row] = result.rows.as_slice() else {
        return Err(message_error("Text comparison MIN must return one row"));
    };
    let [ScalarValue::Text(actual)] = row.as_slice() else {
        return Err(message_error(
            "Text comparison MIN must return one non-NULL Text column",
        ));
    };
    if actual != expected {
        return Err(message_error(format!(
            "Text comparison MIN returned {actual}; expected {expected}"
        )));
    }
    Ok(Observation {
        rows: 1,
        checksum: 0,
    })
}

fn text_extreme_observation(
    result: &QueryResult,
    fixture_rows: u64,
    maximum: bool,
    copies: usize,
) -> BenchResult<Observation> {
    let [row] = result.rows.as_slice() else {
        return Err(message_error("Text extreme query must return one row"));
    };
    if row.len() != copies {
        return Err(message_error(format!(
            "Text extreme query returned {} columns; expected {copies}",
            row.len()
        )));
    }
    let endpoint = if maximum {
        fixture_rows.saturating_sub(1)
    } else {
        0
    };
    let expected = format!("payload-{endpoint:016}");
    for (position, value) in row.iter().enumerate() {
        let ScalarValue::Text(value) = value else {
            return Err(message_error(format!(
                "Text extreme column {position} was not Text"
            )));
        };
        if value != &expected {
            return Err(message_error(format!(
                "Text extreme column {position} was {value}; expected {expected}"
            )));
        }
    }
    Ok(Observation {
        rows: 1,
        checksum: u128::from(endpoint) * copies as u128,
    })
}

fn integer_extreme_observation(
    result: &QueryResult,
    fixture_rows: u64,
    maximum: bool,
    copies: usize,
) -> BenchResult<Observation> {
    let [row] = result.rows.as_slice() else {
        return Err(message_error("integer extreme query must return one row"));
    };
    if row.len() != copies {
        return Err(message_error(format!(
            "integer extreme query returned {} columns; expected {copies}",
            row.len()
        )));
    }
    let endpoint = if maximum {
        fixture_rows.saturating_sub(1)
    } else {
        0
    };
    let expected = i64::try_from(endpoint).map_err(|_| message_error("endpoint exceeds i64"))?;
    for (position, value) in row.iter().enumerate() {
        if value != &ScalarValue::Int64(expected) {
            return Err(message_error(format!(
                "integer extreme column {position} was {value:?}; expected {expected}"
            )));
        }
    }
    Ok(Observation {
        rows: 1,
        checksum: u128::from(endpoint) * copies as u128,
    })
}

fn integer_min_max_observation(
    result: &QueryResult,
    fixture_rows: u64,
) -> BenchResult<Observation> {
    let [row] = result.rows.as_slice() else {
        return Err(message_error("integer MIN/MAX must return one row"));
    };
    let [ScalarValue::Int64(min), ScalarValue::Int64(max)] = row.as_slice() else {
        return Err(message_error(
            "integer MIN/MAX must return two non-NULL Int64 columns",
        ));
    };
    let expected_max = i64::try_from(fixture_rows.saturating_sub(1))
        .map_err(|_| message_error("fixture MAX exceeds i64"))?;
    if (*min, *max) != (0, expected_max) {
        return Err(message_error(format!(
            "integer MIN/MAX returned ({min}, {max}); expected (0, {expected_max})"
        )));
    }
    Ok(Observation {
        rows: 1,
        checksum: u128::from(fixture_rows.saturating_sub(1)),
    })
}

fn expected_group_aggregate_values(
    rows: u64,
    cardinality: u64,
    filtered: bool,
) -> Vec<GroupAggregateExpected> {
    let mut groups = Vec::<GroupAggregateExpected>::new();
    for id in 0..rows {
        if filtered && id % 3 != 0 {
            continue;
        }
        let key = id % cardinality;
        if let Some(group) = groups.iter_mut().find(|group| group.key == key) {
            group.count += 1;
            group.sum += id;
            group.min = group.min.min(id);
            group.max = group.max.max(id);
        } else {
            groups.push(GroupAggregateExpected {
                key,
                count: 1,
                sum: id,
                min: id,
                max: id,
            });
        }
    }
    groups
}

fn group_aggregate_checksum(group: GroupAggregateExpected, filtered: bool) -> u128 {
    let base = u128::from(group.key) * CHECKSUM_FACTOR
        + u128::from(group.count)
        + u128::from(group.sum) * 17;
    if filtered {
        base
    } else {
        base + u128::from(group.min) * 31 + u128::from(group.max) * 47
    }
}

fn expected_grouped_aggregate(rows: u64, cardinality: u64, filtered: bool) -> Observation {
    let groups = expected_group_aggregate_values(rows, cardinality, filtered);
    Observation {
        rows: groups.len() as u64,
        checksum: groups
            .into_iter()
            .map(|group| group_aggregate_checksum(group, filtered))
            .sum(),
    }
}

fn grouped_aggregate_observation(
    result: &QueryResult,
    fixture_rows: u64,
    cardinality: u64,
    filtered: bool,
) -> BenchResult<Observation> {
    let expected = expected_group_aggregate_values(fixture_rows, cardinality, filtered);
    if result.rows.len() != expected.len() {
        return Err(message_error(format!(
            "grouped aggregate returned {} groups; expected {}",
            result.rows.len(),
            expected.len()
        )));
    }
    let mut checksum = 0_u128;
    for (index, (row, expected)) in result.rows.iter().zip(expected).enumerate() {
        let actual = if filtered {
            let [
                ScalarValue::Int64(key),
                ScalarValue::UInt64(count),
                ScalarValue::Int64(sum),
            ] = row.as_slice()
            else {
                return Err(message_error(
                    "filtered grouped aggregate result shape mismatch",
                ));
            };
            (*key, *count, *sum, None, None)
        } else {
            let [
                ScalarValue::Int64(key),
                ScalarValue::UInt64(count),
                ScalarValue::Int64(sum),
                ScalarValue::Int64(min),
                ScalarValue::Int64(max),
            ] = row.as_slice()
            else {
                return Err(message_error("grouped aggregate result shape mismatch"));
            };
            (*key, *count, *sum, Some(*min), Some(*max))
        };
        let expected_tuple = (
            i64::try_from(expected.key).map_err(|_| message_error("group key exceeds i64"))?,
            expected.count,
            i64::try_from(expected.sum).map_err(|_| message_error("group SUM exceeds i64"))?,
            (!filtered)
                .then(|| i64::try_from(expected.min))
                .transpose()
                .map_err(|_| message_error("group MIN exceeds i64"))?,
            (!filtered)
                .then(|| i64::try_from(expected.max))
                .transpose()
                .map_err(|_| message_error("group MAX exceeds i64"))?,
        );
        if actual != expected_tuple {
            return Err(message_error(format!(
                "grouped aggregate row {index} was {actual:?}; expected {expected_tuple:?}"
            )));
        }
        checksum = checksum
            .checked_add(group_aggregate_checksum(expected, filtered))
            .ok_or_else(|| message_error("grouped aggregate checksum overflow"))?;
    }
    Ok(Observation {
        rows: result.rows.len() as u64,
        checksum,
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

fn active_non_null_count(rows: u64, distribution: NullDistribution) -> u64 {
    (0..rows)
        .filter(|id| id % 3 == 0 && !distribution.is_null(*id))
        .count()
        .try_into()
        .expect("fixture row count fits u64")
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

fn group_only_observation(
    result: &QueryResult,
    fixture_rows: u64,
    cardinality: u64,
) -> BenchResult<Observation> {
    let expected_groups = fixture_rows.min(cardinality);
    if result.rows.len() as u64 != expected_groups {
        return Err(message_error(format!(
            "group-only query returned {} groups; expected {expected_groups}",
            result.rows.len()
        )));
    }
    for (position, row) in result.rows.iter().enumerate() {
        let [ScalarValue::Int64(key)] = row.as_slice() else {
            return Err(message_error(
                "group-only query must return one non-NULL Int64 key",
            ));
        };
        if *key != i64::try_from(position)? {
            return Err(message_error(format!(
                "group-only row {position} returned key {key}"
            )));
        }
    }
    Ok(Observation {
        rows: expected_groups,
        checksum: arithmetic_sum(expected_groups),
    })
}

fn one_group_sum_observation(result: &QueryResult, fixture_rows: u64) -> BenchResult<Observation> {
    let [row] = result.rows.as_slice() else {
        return Err(message_error("one-group SUM must return one row"));
    };
    let [ScalarValue::Int64(0), ScalarValue::Int64(sum)] = row.as_slice() else {
        return Err(message_error(
            "one-group SUM must return Int64 key and Int64 SUM",
        ));
    };
    let expected = i64::try_from(arithmetic_sum(fixture_rows))
        .map_err(|_| message_error("fixture SUM exceeds i64"))?;
    if *sum != expected {
        return Err(message_error(format!(
            "one-group SUM returned {sum}; expected {expected}"
        )));
    }
    Ok(Observation {
        rows: 1,
        checksum: arithmetic_sum(fixture_rows),
    })
}

fn one_group_text_extreme_observation(
    result: &QueryResult,
    fixture_rows: u64,
    maximum: bool,
) -> BenchResult<Observation> {
    let [row] = result.rows.as_slice() else {
        return Err(message_error("one-group Text extreme must return one row"));
    };
    let [ScalarValue::Int64(0), ScalarValue::Text(payload)] = row.as_slice() else {
        return Err(message_error(
            "one-group Text extreme must return Int64 key and Text value",
        ));
    };
    let endpoint = if maximum {
        fixture_rows.saturating_sub(1)
    } else {
        0
    };
    validate_payload(usize::try_from(endpoint)?, payload)?;
    Ok(Observation {
        rows: 1,
        checksum: u128::from(endpoint),
    })
}

fn exact_group_observation(
    result: &QueryResult,
    fixture_rows: u64,
    cardinality: u64,
) -> BenchResult<Observation> {
    let expected_group_count = fixture_rows.min(cardinality);
    if result.rows.len() as u64 != expected_group_count {
        return Err(message_error(format!(
            "GROUP BY returned {} groups; expected {expected_group_count}",
            result.rows.len()
        )));
    }
    for (position, row) in result.rows.iter().enumerate() {
        let [ScalarValue::Int64(key), ScalarValue::UInt64(count)] = row.as_slice() else {
            return Err(message_error(
                "GROUP BY cardinality query must return Int64 key and UInt64 count",
            ));
        };
        let expected_key = position as u64;
        let expected_count = (fixture_rows - 1 - expected_key) / cardinality + 1;
        if *key != i64::try_from(expected_key)? || *count != expected_count {
            return Err(message_error(format!(
                "GROUP BY row {position} was ({key}, {count}); expected ({expected_key}, {expected_count})"
            )));
        }
    }
    Ok(expected_groups(fixture_rows, cardinality))
}

fn expected_two_key_group_values(rows: u64, cardinality: u64) -> Vec<(u64, bool, u64)> {
    let mut groups = Vec::<(u64, bool, u64)>::new();
    for id in 0..rows {
        let key = (id % cardinality, id % 3 == 0);
        if let Some((_, _, count)) = groups
            .iter_mut()
            .find(|(team, active, _)| (*team, *active) == key)
        {
            *count += 1;
        } else {
            groups.push((key.0, key.1, 1));
        }
    }
    groups
}

fn two_key_group_checksum(team: u64, active: bool, count: u64) -> u128 {
    u128::from(team) * CHECKSUM_FACTOR + u128::from(active) * 101 + u128::from(count)
}

fn expected_two_key_groups(rows: u64, cardinality: u64) -> Observation {
    let groups = expected_two_key_group_values(rows, cardinality);
    Observation {
        rows: groups.len() as u64,
        checksum: groups
            .into_iter()
            .map(|(team, active, count)| two_key_group_checksum(team, active, count))
            .sum(),
    }
}

fn two_key_group_observation(
    result: &QueryResult,
    fixture_rows: u64,
    cardinality: u64,
) -> BenchResult<Observation> {
    let expected = expected_two_key_group_values(fixture_rows, cardinality);
    if result.rows.len() != expected.len() {
        return Err(message_error(format!(
            "two-key GROUP BY returned {} groups; expected {}",
            result.rows.len(),
            expected.len()
        )));
    }
    let mut checksum = 0_u128;
    for (position, (row, expected)) in result.rows.iter().zip(expected).enumerate() {
        let [
            ScalarValue::Int64(team),
            ScalarValue::Bool(active),
            ScalarValue::UInt64(count),
        ] = row.as_slice()
        else {
            return Err(message_error(
                "two-key GROUP BY must return Int64, Bool, and UInt64",
            ));
        };
        let actual_team = u64::try_from(*team).map_err(|_| message_error("negative team key"))?;
        if (actual_team, *active, *count) != expected {
            return Err(message_error(format!(
                "two-key GROUP BY row {position} was ({actual_team}, {active}, {count}); expected {expected:?}"
            )));
        }
        checksum = checksum
            .checked_add(two_key_group_checksum(expected.0, expected.1, expected.2))
            .ok_or_else(|| message_error("two-key GROUP BY checksum overflow"))?;
    }
    Ok(Observation {
        rows: result.rows.len() as u64,
        checksum,
    })
}

fn unique_text_group_observation(
    result: &QueryResult,
    fixture_rows: u64,
) -> BenchResult<Observation> {
    if result.rows.len() as u64 != fixture_rows {
        return Err(message_error(format!(
            "Text GROUP BY returned {} groups; expected {fixture_rows}",
            result.rows.len()
        )));
    }
    for (id, row) in result.rows.iter().enumerate() {
        let [ScalarValue::Text(payload), ScalarValue::UInt64(1)] = row.as_slice() else {
            return Err(message_error(
                "Text GROUP BY must return a Text key with COUNT(*) = 1",
            ));
        };
        validate_payload(id, payload)?;
    }
    Ok(Observation {
        rows: fixture_rows,
        checksum: arithmetic_sum(fixture_rows),
    })
}

fn unique_text_owner_observation(
    result: &QueryResult,
    fixture_rows: u64,
    owners: usize,
) -> BenchResult<Observation> {
    if result.rows.len() as u64 != fixture_rows {
        return Err(message_error(format!(
            "Text ownership GROUP BY returned {} groups; expected {fixture_rows}",
            result.rows.len()
        )));
    }
    for (id, row) in result.rows.iter().enumerate() {
        if row.len() != owners {
            return Err(message_error(format!(
                "Text ownership GROUP BY returned {} values; expected {owners}",
                row.len()
            )));
        }
        let Some(ScalarValue::Text(payload)) = row.first() else {
            return Err(message_error(
                "Text ownership GROUP BY key must be non-NULL Text",
            ));
        };
        validate_payload(id, payload)?;
        if row
            .iter()
            .skip(1)
            .any(|value| !matches!(value, ScalarValue::Text(candidate) if candidate == payload))
        {
            return Err(message_error(
                "Text ownership GROUP BY aggregate owner differs from its unique key",
            ));
        }
    }
    Ok(Observation {
        rows: fixture_rows,
        checksum: arithmetic_sum(fixture_rows),
    })
}

fn unique_wide_group_observation(
    result: &QueryResult,
    fixture_rows: u64,
) -> BenchResult<Observation> {
    if result.rows.len() as u64 != fixture_rows {
        return Err(message_error(format!(
            "wide GROUP BY returned {} groups; expected {fixture_rows}",
            result.rows.len()
        )));
    }
    for (id, row) in result.rows.iter().enumerate() {
        let [
            ScalarValue::Int64(actual_id),
            ScalarValue::Text(payload),
            ScalarValue::UInt64(1),
        ] = row.as_slice()
        else {
            return Err(message_error(
                "wide GROUP BY must return Int64, Text, and COUNT(*) = 1",
            ));
        };
        if *actual_id != i64::try_from(id)? {
            return Err(message_error(format!(
                "wide GROUP BY row {id} returned ID {actual_id}"
            )));
        }
        validate_payload(id, payload)?;
    }
    Ok(Observation {
        rows: fixture_rows,
        checksum: arithmetic_sum(fixture_rows),
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
        remove_if_present(&netbadb_storage::txn_status_path(path))?;
    }
    Ok(())
}

fn remove_if_present(path: &Path) -> BenchResult<()> {
    if path.is_dir() {
        return std::fs::remove_dir_all(path).map_err(|error| {
            message_error(format!(
                "failed to remove benchmark fixture directory `{}`: {error}",
                path.display()
            ))
        });
    }
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
