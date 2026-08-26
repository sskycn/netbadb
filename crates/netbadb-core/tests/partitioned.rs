use std::path::{Path, PathBuf};

use netbadb_core::{
    Database, ExecutionResult, PartitionCatalogConfig, PartitionError, RangePartitionSpec,
    TablePlacementSpec,
};
use netbadb_inspect::{PartitionAccessInspection, PlanNodeInspection, StatementPlanInspection};
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_types::{ColumnId, PartitionId, PhysicalType, ScalarValue, TableId};

fn events() -> TableDef {
    TableDef::new(
        TableId(41),
        "events",
        vec![
            ColumnDef::new(ColumnId(1), "key", TypeSpec::Physical(PhysicalType::Int64)),
            ColumnDef::new(
                ColumnId(2),
                "payload",
                TypeSpec::Physical(PhysicalType::Text),
            ),
        ],
    )
}

struct Fixture {
    base: PathBuf,
    paths: Vec<PathBuf>,
    config: PartitionCatalogConfig,
}

impl Fixture {
    fn new(case: &str) -> Self {
        let base = std::env::temp_dir().join(format!(
            "netbadb-range-{case}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let paths = (0..3)
            .map(|position| base.with_extension(format!("p{position}.db")))
            .collect::<Vec<_>>();
        let config = PartitionCatalogConfig::new(
            base.with_extension("partitions"),
            base.with_extension("coordinator"),
        );
        let fixture = Self {
            base,
            paths,
            config,
        };
        fixture.cleanup();
        fixture
    }

    fn specs(&self) -> Vec<TablePlacementSpec> {
        vec![TablePlacementSpec::range_partitioned(
            events(),
            ColumnId(1),
            vec![
                RangePartitionSpec::new(
                    PartitionId(10),
                    &self.paths[0],
                    None,
                    Some(ScalarValue::Int64(0)),
                ),
                RangePartitionSpec::new(
                    PartitionId(20),
                    &self.paths[1],
                    Some(ScalarValue::Int64(0)),
                    Some(ScalarValue::Int64(100)),
                ),
                RangePartitionSpec::new(
                    PartitionId(30),
                    &self.paths[2],
                    Some(ScalarValue::Int64(200)),
                    None,
                ),
            ],
        )]
    }

    fn cleanup(&self) {
        for path in &self.paths {
            remove_storage(path);
        }
        for path in [
            self.config.catalog_path(),
            self.config.coordinator_log_path(),
        ] {
            let _ = std::fs::remove_file(path);
        }
        let _ = std::fs::remove_file(&self.base);
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.cleanup();
    }
}

fn remove_storage(path: &Path) {
    let wal = netbadb_storage::wal_path(path);
    for target in [
        path.to_owned(),
        wal.clone(),
        netbadb_storage::wal_alternate_path(&wal),
        netbadb_storage::txn_status_path(path),
    ] {
        let _ = std::fs::remove_file(target);
    }
}

fn partitioned_scan(plan: &PlanNodeInspection) -> &PlanNodeInspection {
    match plan {
        PlanNodeInspection::PartitionedScan { .. } => plan,
        PlanNodeInspection::Filter { input, .. }
        | PlanNodeInspection::Project { input, .. }
        | PlanNodeInspection::Aggregate { input, .. }
        | PlanNodeInspection::Sort { input, .. }
        | PlanNodeInspection::Limit { input, .. } => partitioned_scan(input),
        other => panic!("expected partitioned scan, got {other:?}"),
    }
}

fn inspected_scan(database: &Database, sql: &str) -> PlanNodeInspection {
    let inspection = database.inspect_statement(sql).expect("inspect statement");
    let root = match inspection.plan {
        StatementPlanInspection::Query { root }
        | StatementPlanInspection::Update { input: root, .. }
        | StatementPlanInspection::Delete { input: root, .. } => root,
        StatementPlanInspection::Insert { .. } => panic!("INSERT has no scan"),
    };
    partitioned_scan(&root).clone()
}

fn selected(plan: &PlanNodeInspection) -> Vec<(PartitionId, &'static str)> {
    let PlanNodeInspection::PartitionedScan { partitions, .. } = plan else {
        panic!("expected partitioned scan");
    };
    partitions
        .iter()
        .map(|partition| {
            let access = match partition.access {
                PartitionAccessInspection::SeqScan => "seq",
                PartitionAccessInspection::IndexScan { .. } => "point",
                PartitionAccessInspection::RangeIndexScan { .. } => "range",
            };
            (partition.partition_id, access)
        })
        .collect()
}

#[test]
fn range_pruning_routing_atomic_dml_and_reopen_path_reorder_work_end_to_end() {
    let fixture = Fixture::new("end-to-end");
    let mut database = Database::create_with_placements(fixture.specs(), fixture.config.clone())
        .expect("create range database");

    for (key, payload) in [(-5, "neg"), (0, "zero"), (99, "last"), (200, "high")] {
        database
            .execute(&format!(
                "INSERT INTO events (key, payload) VALUES ({key}, '{payload}')"
            ))
            .expect("route insert");
    }
    assert!(matches!(
        database.execute("INSERT INTO events (key, payload) VALUES (150, 'gap')"),
        Err(netbadb_core::DatabaseError::Partition(
            PartitionError::NoPartitionForValue(ScalarValue::Int64(150))
        ))
    ));

    assert_eq!(
        selected(&inspected_scan(
            &database,
            "SELECT key FROM events WHERE key >= 0 AND key < 100"
        )),
        vec![(PartitionId(20), "seq")]
    );
    assert_eq!(
        selected(&inspected_scan(
            &database,
            "SELECT key FROM events WHERE key >= -10 AND key < 250"
        )),
        vec![
            (PartitionId(10), "seq"),
            (PartitionId(20), "seq"),
            (PartitionId(30), "seq")
        ]
    );
    assert!(
        selected(&inspected_scan(
            &database,
            "SELECT key FROM events WHERE key > 100 AND key < 50"
        ))
        .is_empty()
    );
    assert!(
        selected(&inspected_scan(
            &database,
            "SELECT key FROM events WHERE key > 99 AND key < 100"
        ))
        .is_empty()
    );
    assert_eq!(
        selected(&inspected_scan(
            &database,
            "SELECT key FROM events WHERE key = -5 OR key = 200"
        ))
        .len(),
        3
    );

    assert_eq!(
        database
            .execute("UPDATE events SET key = 250 WHERE key = -5")
            .expect("cross partition update"),
        ExecutionResult::AffectedRows(1)
    );
    assert_eq!(
        database
            .query("SELECT key FROM events WHERE payload = 'neg'")
            .expect("query moved row")
            .rows,
        vec![vec![ScalarValue::Int64(250)]]
    );
    assert!(matches!(
        database.execute("UPDATE events SET key = 150 WHERE key = 0"),
        Err(netbadb_core::DatabaseError::Partition(
            PartitionError::NoPartitionForValue(ScalarValue::Int64(150))
        ))
    ));
    assert_eq!(
        database
            .query("SELECT payload FROM events WHERE key = 0")
            .expect("gap update made no mutation")
            .rows,
        vec![vec![ScalarValue::Text("zero".into())]]
    );

    let mut transaction = database
        .begin_transaction_for(TableId(41))
        .expect("begin explicit partition transaction");
    database
        .execute_in(
            &mut transaction,
            "INSERT INTO events (key, payload) VALUES (-10, 'left')",
        )
        .expect("insert first partition");
    database
        .execute_in(
            &mut transaction,
            "INSERT INTO events (key, payload) VALUES (220, 'right')",
        )
        .expect("insert second partition");
    transaction.commit().expect("atomic partition commit");

    assert_eq!(
        database
            .execute("DELETE FROM events WHERE key >= 99")
            .expect("multi partition delete"),
        ExecutionResult::AffectedRows(4)
    );
    database.close().expect("close range database");

    let mut reversed = fixture.paths.clone();
    reversed.reverse();
    let mut reopened =
        Database::open_with_placements(vec![events()], reversed, fixture.config.clone())
            .expect("reopen with reversed path order");
    assert_eq!(
        reopened
            .query("SELECT key FROM events")
            .expect("scan reopened partitions")
            .rows,
        vec![vec![ScalarValue::Int64(-10)], vec![ScalarValue::Int64(0)]]
    );
    reopened.close().expect("close reopened database");
}

#[test]
fn local_indexes_joins_aggregates_and_range_boundaries_preserve_logical_semantics() {
    let fixture = Fixture::new("logical");
    let mut database = Database::create_with_placements(fixture.specs(), fixture.config.clone())
        .expect("create range database");
    database
        .create_partition_index(TableId(41), PartitionId(20), ColumnId(1))
        .expect("create local index");
    for (key, payload) in [(-1, "a"), (0, "b"), (99, "c"), (100, "gap"), (200, "d")] {
        let result = database.execute(&format!(
            "INSERT INTO events (key, payload) VALUES ({key}, '{payload}')"
        ));
        if key == 100 {
            assert!(result.is_err());
        } else {
            result.expect("boundary route");
        }
    }
    assert_eq!(
        selected(&inspected_scan(
            &database,
            "SELECT payload FROM events WHERE 0 < key AND key = 99"
        )),
        vec![(PartitionId(20), "point")]
    );
    assert_eq!(
        database
            .query("SELECT COUNT(*), MIN(key), MAX(key) FROM events")
            .expect("aggregate all partitions")
            .rows,
        vec![vec![
            ScalarValue::UInt64(4),
            ScalarValue::Int64(-1),
            ScalarValue::Int64(200)
        ]]
    );
    assert_eq!(
        database
            .query("SELECT a.key FROM events a JOIN events b ON a.key = b.key")
            .expect("self join partitioned table")
            .rows
            .len(),
        4
    );
    database.close().expect("close logical fixture");
}

#[test]
fn invalid_partition_definitions_are_rejected_before_catalog_publication() {
    let fixture = Fixture::new("invalid");
    let nullable = TableDef::new(
        TableId(41),
        "events",
        vec![
            ColumnDef::new(ColumnId(1), "key", TypeSpec::Physical(PhysicalType::Int64))
                .nullable(true),
        ],
    );
    let result = Database::create_with_placements(
        vec![TablePlacementSpec::range_partitioned(
            nullable,
            ColumnId(1),
            vec![RangePartitionSpec::new(
                PartitionId(1),
                &fixture.paths[0],
                None,
                None,
            )],
        )],
        fixture.config.clone(),
    );
    assert!(matches!(
        result,
        Err(netbadb_core::DatabaseError::Partition(
            PartitionError::NullablePartitionKey { .. }
        ))
    ));
    assert!(!fixture.config.catalog_path().exists());
}

#[test]
fn uint64_min_max_and_half_open_boundary_route_without_overflow() {
    let fixture = Fixture::new("uint-boundaries");
    let table = TableDef::new(
        TableId(42),
        "counters",
        vec![ColumnDef::new(
            ColumnId(1),
            "key",
            TypeSpec::Physical(PhysicalType::UInt64),
        )],
    );
    let specs = vec![TablePlacementSpec::range_partitioned(
        table.clone(),
        ColumnId(1),
        vec![
            RangePartitionSpec::new(
                PartitionId(101),
                &fixture.paths[0],
                None,
                Some(ScalarValue::UInt64(10)),
            ),
            RangePartitionSpec::new(
                PartitionId(102),
                &fixture.paths[1],
                Some(ScalarValue::UInt64(10)),
                None,
            ),
        ],
    )];
    let mut database = Database::create_with_placements(specs, fixture.config.clone())
        .expect("create UInt64 partitions");
    for value in [0, 9, 10, u64::MAX] {
        database
            .insert_into(TableId(42), &[ScalarValue::UInt64(value)])
            .expect("route UInt64 boundary");
    }
    assert_eq!(
        database
            .query("SELECT COUNT(*) FROM counters")
            .expect("count UInt64 partitions")
            .rows,
        vec![vec![ScalarValue::UInt64(4)]]
    );
    database.close().expect("close UInt64 partitions");
}
