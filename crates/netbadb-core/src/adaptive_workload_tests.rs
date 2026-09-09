use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;

use netbadb_compiler::compile_statement;
use netbadb_planner::{PhysicalPlan, PlanVariant, PlannerCalibrationSample};
use netbadb_rel::{LogicalQueryShape, LogicalStatement};
use netbadb_schema::{ColumnDef, Schema, TableDef, TypeSpec};
use netbadb_types::{
    ColumnId, ColumnarGeneration, DatabaseCommitSeq, PhysicalType, ScalarValue, TableId,
};

use crate::execution_feedback_tests::{Fixture, NEXT_PATH, TABLE_ID};
use crate::{
    AdaptiveExecutionFeedbackOutcome, AdaptiveWorkloadLimits, AdaptiveWorkloadOutcome,
    AdaptiveWorkloadPolicy, AdaptiveWorkloadRecordError, AdaptiveWorkloadRecordOutcome,
    AdaptiveWorkloadStaleReason, AdaptiveWorkloadTarget, AdaptiveWorkloadWindow,
    ColumnarAdvanceBudget, ColumnarProjectionSpec, Database, DatabaseCoordinatorConfig,
    ExecutionAccessKind, ExecutionFeedbackPolicy, ExecutionFeedbackReport, TableStorageCreateSpec,
    cleanup_created_table_files,
};

const CLOCK_TABLE_ID: TableId = TableId(88_002);

pub(super) fn query_shape(schema: &Schema, sql: &str) -> LogicalQueryShape {
    let compiled = compile_statement(schema, sql).expect("compile shape query");
    let LogicalStatement::Query(plan) = compiled.logical_statement else {
        panic!("expected logical query")
    };
    LogicalQueryShape::from_plan(&plan).expect("derive logical query shape")
}

pub(super) fn workload_target(database: &Database) -> AdaptiveWorkloadTarget {
    let projection = database.inspect_columnar_projections().remove(0);
    AdaptiveWorkloadTarget {
        table_id: projection.table_id,
        storage_id: projection.source_storage_id.expect("source storage"),
        projection_id: projection.projection_id.expect("projection ID"),
        generation: projection.generation.expect("projection generation"),
        schema_generation: database.schema_generation(),
    }
}

pub(super) fn set_target_work(
    report: &mut ExecutionFeedbackReport,
    target: AdaptiveWorkloadTarget,
    estimated: u64,
    actual: u64,
    source: u64,
) {
    let access = report
        .accesses
        .iter_mut()
        .find(|access| {
            access
                .planner
                .as_ref()
                .is_some_and(|planner| planner.projection_id == Some(target.projection_id))
        })
        .expect("target access");
    let planner = access.planner.as_mut().expect("target planner evidence");
    planner.estimated_work_units = Some(estimated);
    planner.effective_work_units = Some(estimated);
    planner.source_alternative_work_units = Some(source);
    planner.effective_source_alternative_work_units = Some(source);
    planner.calibration_epoch = report.calibration_epoch;
    access.calibration = Some(PlannerCalibrationSample::with_effective(
        estimated,
        Some(estimated),
        report.calibration_epoch,
        Some(actual),
    ));
    access.actual.work.overflowed = false;
    access.actual.work.incomplete = false;
    report.overflowed = false;
    report.incomplete = false;
}

pub(super) struct TimelineFixture {
    pub(super) root: PathBuf,
    pub(super) event_path: PathBuf,
    pub(super) clock_path: PathBuf,
    pub(super) database: Database,
}

impl TimelineFixture {
    pub(super) fn create(name: &str) -> Self {
        let suffix = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "netbadb-adaptive-workload-{name}-{}-{suffix}",
            std::process::id()
        ));
        fs::create_dir_all(&root).expect("create timeline root");
        let event_path = root.join("events");
        let clock_path = root.join("clock");
        let events = TableDef::new(
            TABLE_ID,
            "events",
            vec![
                ColumnDef::new(ColumnId(1), "id", TypeSpec::Physical(PhysicalType::Int64)),
                ColumnDef::new(
                    ColumnId(2),
                    "category",
                    TypeSpec::Physical(PhysicalType::Int64),
                ),
                ColumnDef::new(
                    ColumnId(3),
                    "payload",
                    TypeSpec::Physical(PhysicalType::Text),
                ),
            ],
        );
        let clock = TableDef::new(
            CLOCK_TABLE_ID,
            "clock_rows",
            vec![ColumnDef::new(
                ColumnId(1),
                "id",
                TypeSpec::Physical(PhysicalType::Int64),
            )],
        );
        let mut database = Database::create_catalog(
            root.join("catalog"),
            vec![
                TableStorageCreateSpec::heap(&event_path, events),
                TableStorageCreateSpec::heap(&clock_path, clock),
            ],
            Some(DatabaseCoordinatorConfig::new(root.join("coordinator")).with_global_visibility()),
        )
        .expect("create timeline database");
        let mut transaction = database.begin_transaction().expect("begin timeline seed");
        for id in 0..512_i64 {
            database
                .insert_into_in(
                    TABLE_ID,
                    &mut transaction,
                    &[
                        ScalarValue::Int64(id),
                        ScalarValue::Int64(id % 8),
                        ScalarValue::Text(format!("payload-{id}")),
                    ],
                )
                .expect("insert timeline event");
        }
        transaction.commit().expect("commit timeline seed");
        database
            .enable_change_stream(TABLE_ID)
            .expect("enable timeline change stream");
        database
            .build_incremental_columnar_projection(ColumnarProjectionSpec::new(
                TABLE_ID,
                root.join("projection"),
                vec![ColumnId(1), ColumnId(2)],
            ))
            .expect("build timeline projection");
        Self {
            root,
            event_path,
            clock_path,
            database,
        }
    }

    pub(super) fn close(self) {
        self.database.close().expect("close timeline database");
        cleanup(&self.root, &[self.event_path, self.clock_path]);
    }
}

pub(super) fn cleanup(root: &Path, storage_paths: &[PathBuf]) {
    cleanup_created_table_files(storage_paths);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn columnar_raw_values_use_the_shared_production_cast_semantics() {
    let mut fixture = TimelineFixture::create("columnar-production-cast");
    let expected = fixture
        .database
        .query("SELECT id FROM events")
        .expect("authoritative Heap result")
        .rows
        .into_iter()
        .map(|row| match row.as_slice() {
            [ScalarValue::Int64(value)] => vec![ScalarValue::Text(value.to_string())],
            _ => panic!("unexpected Heap row"),
        })
        .collect::<Vec<_>>();
    let (casted, statistics) = fixture
        .database
        .query_with_columnar_statistics("SELECT id::TEXT FROM events")
        .expect("Columnar cast query");
    assert_eq!(casted.rows, expected);
    assert!(statistics.projection_id.is_some());
    assert!(statistics.scan.row_groups_read > 0);
    fixture.close();
}

#[test]
fn logical_query_shape_normalizes_literals_aliases_and_bindings_but_keeps_structure() {
    let schema = Schema::new(vec![
        TableDef::new(
            TableId(1),
            "users",
            vec![
                ColumnDef::new(ColumnId(1), "id", TypeSpec::Physical(PhysicalType::Int64)),
                ColumnDef::new(ColumnId(2), "age", TypeSpec::Physical(PhysicalType::Int64)),
            ],
        ),
        TableDef::new(
            TableId(2),
            "teams",
            vec![ColumnDef::new(
                ColumnId(1),
                "id",
                TypeSpec::Physical(PhysicalType::Int64),
            )],
        ),
    ])
    .expect("shape schema");
    assert_eq!(
        query_shape(&schema, "SELECT id FROM users WHERE id = 10"),
        query_shape(&schema, "SELECT id FROM users WHERE id = 11")
    );
    assert_eq!(
        query_shape(&schema, "SELECT a.id FROM users a WHERE a.id = 10"),
        query_shape(
            &schema,
            "SELECT renamed.id FROM users renamed WHERE renamed.id = 11"
        )
    );
    assert_ne!(
        query_shape(&schema, "SELECT id FROM users WHERE id = 10"),
        query_shape(&schema, "SELECT id FROM teams WHERE id = 10")
    );
    assert_ne!(
        query_shape(&schema, "SELECT id FROM users WHERE id = 10"),
        query_shape(&schema, "SELECT age FROM users WHERE age = 10")
    );
    assert_ne!(
        query_shape(&schema, "SELECT id FROM users WHERE id = 10"),
        query_shape(&schema, "SELECT id FROM users WHERE id > 10")
    );
    assert_ne!(
        query_shape(&schema, "SELECT id FROM users WHERE id = 10"),
        query_shape(&schema, "SELECT id FROM users WHERE id = $1")
    );
    assert_ne!(
        query_shape(&schema, "SELECT id FROM users WHERE id = 10"),
        query_shape(&schema, "SELECT id FROM users WHERE id = NULL")
    );
    assert_ne!(
        query_shape(&schema, "SELECT id FROM users ORDER BY id ASC"),
        query_shape(&schema, "SELECT id FROM users ORDER BY id DESC")
    );
    assert_ne!(
        query_shape(&schema, "SELECT id FROM users LIMIT 1"),
        query_shape(&schema, "SELECT id FROM users LIMIT 100")
    );
    let first = query_shape(
        &schema,
        "SELECT u.id FROM users u JOIN users manager ON u.id = manager.id",
    );
    let second = query_shape(
        &schema,
        "SELECT employee.id FROM users employee JOIN users boss ON employee.id = boss.id",
    );
    assert_eq!(first, second);
}

#[test]
fn plan_variant_erases_generation_and_index_payload_but_keeps_strategy() {
    let fixture = Fixture::create("variant", true);
    let target = workload_target(&fixture.database);
    let column = netbadb_rel::ColumnRef {
        binding_id: netbadb_types::RelationBindingId(7),
        table_id: TABLE_ID,
        column_id: ColumnId(1),
        relation_name: "ignored".to_owned(),
        name: "ignored".to_owned(),
        data_type: netbadb_types::SemanticType::physical(PhysicalType::Int64),
        nullable: false,
    };
    let columnar = |generation| PhysicalPlan::ColumnarScan {
        binding_id: column.binding_id,
        table_id: TABLE_ID,
        table_name: "ignored".to_owned(),
        columns: vec![column.clone()],
        projection_id: target.projection_id,
        generation,
        source_storage_id: target.storage_id,
    };
    assert_eq!(
        PlanVariant::from_plan(&columnar(target.generation)).expect("C8 variant"),
        PlanVariant::from_plan(&columnar(ColumnarGeneration(target.generation.0 + 1)))
            .expect("C9 variant")
    );
    let index = |key| PhysicalPlan::IndexScan {
        binding_id: column.binding_id,
        table_id: TABLE_ID,
        table_name: "ignored".to_owned(),
        columns: vec![column.clone()],
        index_column: column.clone(),
        access_path: netbadb_types::AccessPathId(9),
        key: ScalarValue::Int64(key),
    };
    assert_eq!(
        PlanVariant::from_plan(&index(10)).expect("index key 10"),
        PlanVariant::from_plan(&index(11)).expect("index key 11")
    );
    assert_ne!(
        PlanVariant::from_plan(&columnar(target.generation)).expect("columnar variant"),
        PlanVariant::from_plan(&index(10)).expect("index variant")
    );
    fixture.close();
}

#[test]
fn workload_crosses_real_g_advances_while_phase2_remains_strict() {
    let mut fixture = TimelineFixture::create("cross-g");
    let target = workload_target(&fixture.database);
    let sql = "SELECT id, category FROM events WHERE id >= 400";
    let (_, first) = fixture
        .database
        .query_with_feedback(sql)
        .expect("G first sample");
    fixture
        .database
        .execute("INSERT INTO events VALUES (512, 0, 'payload-512')")
        .expect("advance source and G with insert");
    fixture
        .database
        .advance_columnar_projection(target.projection_id, ColumnarAdvanceBudget::new(8, 1 << 20))
        .expect("catch up after insert");
    assert_eq!(
        workload_target(&fixture.database).generation,
        target.generation
    );
    let (_, second) = fixture
        .database
        .query_with_feedback(sql)
        .expect("G second sample");
    fixture
        .database
        .execute("UPDATE events SET category = 7 WHERE id = 511")
        .expect("advance source and G with update");
    fixture
        .database
        .advance_columnar_projection(target.projection_id, ColumnarAdvanceBudget::new(8, 1 << 20))
        .expect("catch up after update");
    assert_eq!(
        workload_target(&fixture.database).generation,
        target.generation
    );
    let (_, third) = fixture
        .database
        .query_with_feedback(sql)
        .expect("G third sample");
    let visibility = [first.anchor, second.anchor, third.anchor]
        .map(|anchor| anchor.global_commit_seq.expect("global G"));
    assert!(visibility[0] < visibility[1] && visibility[1] < visibility[2]);

    let phase2 = fixture
        .database
        .evaluate_adaptive_execution_feedback(
            target.projection_id,
            target.generation,
            std::slice::from_ref(&first),
            ExecutionFeedbackPolicy::new(1, 1, u64::MAX),
        )
        .expect("phase2 stale check");
    assert_eq!(
        phase2.outcome,
        AdaptiveExecutionFeedbackOutcome::StaleFeedback
    );

    let mut window = AdaptiveWorkloadWindow::new(target, AdaptiveWorkloadLimits::default());
    for report in [&first, &second, &third] {
        assert!(matches!(
            window.record(report).expect("record cross-G sample"),
            AdaptiveWorkloadRecordOutcome::RecordedRelevant { .. }
        ));
    }
    assert_eq!(window.total_samples, 3);
    assert_eq!(window.distinct_visibility_points, 3);
    assert_eq!(window.first_global_commit_seq, Some(visibility[0]));
    assert_eq!(window.last_global_commit_seq, Some(visibility[2]));
    let current_g = fixture
        .database
        .current_database_snapshot()
        .expect("current snapshot")
        .expect("global snapshot")
        .commit_seq();
    let outcome = fixture
        .database
        .evaluate_adaptive_workload(
            &window,
            AdaptiveWorkloadPolicy::new(3, 1, 3, u64::MAX, u64::MAX),
        )
        .expect("evaluate cross-G window");
    assert_eq!(
        outcome.outcome,
        AdaptiveWorkloadOutcome::HeldWithinHysteresisBand
    );
    assert_eq!(
        fixture
            .database
            .current_database_snapshot()
            .expect("unchanged snapshot")
            .expect("global snapshot")
            .commit_seq(),
        current_g
    );
    fixture.close();
}

#[test]
fn hysteresis_keeps_holds_reverts_and_never_unsuppresses_history() {
    let mut fixture = Fixture::create("hysteresis", true);
    let target = workload_target(&fixture.database);
    let (_, base) = fixture
        .database
        .query_with_feedback("SELECT id, category FROM events WHERE id >= 400")
        .expect("base hysteresis sample");
    let make_window = |actual: u64, source: u64| {
        let mut window = AdaptiveWorkloadWindow::new(target, AdaptiveWorkloadLimits::default());
        for _ in 0..3 {
            let mut report = base.clone();
            set_target_work(&mut report, target, 2_500, actual, source);
            window.record(&report).expect("record hysteresis sample");
        }
        window
    };
    let keep = make_window(2_000, 3_500);
    assert_eq!(
        fixture
            .database
            .evaluate_adaptive_workload(&keep, AdaptiveWorkloadPolicy::new(3, 1, 1, 3_000, 1_000),)
            .expect("keep outcome")
            .outcome,
        AdaptiveWorkloadOutcome::ValidatedKeep
    );
    let hold = make_window(3_600, 3_500);
    assert_eq!(
        fixture
            .database
            .evaluate_adaptive_workload(&hold, AdaptiveWorkloadPolicy::new(3, 1, 1, 3_000, 1_000),)
            .expect("hold outcome")
            .outcome,
        AdaptiveWorkloadOutcome::HeldWithinHysteresisBand
    );
    let regression = make_window(4_500, 3_500);
    let before_g = fixture
        .database
        .current_database_snapshot()
        .expect("snapshot before revert")
        .expect("global snapshot")
        .commit_seq();
    assert_eq!(
        fixture
            .database
            .evaluate_adaptive_workload(
                &regression,
                AdaptiveWorkloadPolicy::new(3, 1, 1, 3_000, 1_000),
            )
            .expect("revert outcome")
            .outcome,
        AdaptiveWorkloadOutcome::RevertedMeasuredRegression
    );
    assert_eq!(fixture.database.inspect_columnar_projections().len(), 1);
    let (_, fallback) = fixture
        .database
        .query_with_feedback("SELECT id, category FROM events WHERE id >= 400")
        .expect("fallback after workload revert");
    assert!(
        fallback
            .accesses
            .iter()
            .all(|access| access.actual.kind != ExecutionAccessKind::Columnar)
    );
    assert_eq!(
        fixture
            .database
            .evaluate_adaptive_workload(&keep, AdaptiveWorkloadPolicy::new(3, 1, 1, 3_000, 1_000),)
            .expect("historical keep after suppression")
            .outcome,
        AdaptiveWorkloadOutcome::HeldSuppressed
    );
    assert_eq!(
        fixture
            .database
            .current_database_snapshot()
            .expect("snapshot after revert")
            .expect("global snapshot")
            .commit_seq(),
        before_g
    );
    fixture.close();
}

#[test]
fn visibility_threshold_and_record_order_are_deterministic() {
    let mut fixture = Fixture::create("visibility", true);
    let target = workload_target(&fixture.database);
    let (_, base) = fixture
        .database
        .query_with_feedback("SELECT id, category FROM events WHERE id >= 400")
        .expect("visibility sample");
    let mut same_g = AdaptiveWorkloadWindow::new(target, AdaptiveWorkloadLimits::default());
    for _ in 0..100 {
        same_g.record(&base).expect("record same-G sample");
    }
    assert_eq!(same_g.distinct_visibility_points, 1);
    assert_eq!(
        fixture
            .database
            .evaluate_adaptive_workload(&same_g, AdaptiveWorkloadPolicy::new(1, 1, 2, 0, 0),)
            .expect("same-G threshold")
            .outcome,
        AdaptiveWorkloadOutcome::Inconclusive
    );

    let g = base.anchor.global_commit_seq.expect("base G");
    let mut newer = base.clone();
    newer.anchor.global_commit_seq = Some(DatabaseCommitSeq(g.0 + 2));
    let mut older = base;
    older.anchor.global_commit_seq = Some(DatabaseCommitSeq(g.0 + 1));
    let mut ordered = AdaptiveWorkloadWindow::new(target, AdaptiveWorkloadLimits::default());
    ordered.record(&newer).expect("record newer sample");
    let before = ordered.clone();
    assert_eq!(
        ordered.record(&older),
        Err(AdaptiveWorkloadRecordError::OutOfOrderVisibility {
            previous: DatabaseCommitSeq(g.0 + 2),
            received: DatabaseCommitSeq(g.0 + 1),
        })
    );
    assert_eq!(ordered, before);

    let mut missing_g = newer;
    missing_g.anchor.global_commit_seq = None;
    let empty = AdaptiveWorkloadWindow::new(target, AdaptiveWorkloadLimits::default());
    let mut rejected = empty.clone();
    assert_eq!(
        rejected.record(&missing_g),
        Err(AdaptiveWorkloadRecordError::GlobalVisibilityRequired)
    );
    assert_eq!(rejected, empty);
    fixture.close();
}

#[test]
fn schema_and_generation_changes_stale_windows_without_touching_new_target() {
    let mut generation_fixture = Fixture::create("generation-stale", true);
    let old_target = workload_target(&generation_fixture.database);
    let (_, feedback) = generation_fixture
        .database
        .query_with_feedback("SELECT id, category FROM events WHERE id >= 400")
        .expect("generation sample");
    let mut window = AdaptiveWorkloadWindow::new(old_target, AdaptiveWorkloadLimits::default());
    window.record(&feedback).expect("record generation sample");
    let new_generation = generation_fixture
        .database
        .refresh_columnar_projection(old_target.projection_id)
        .expect("refresh projection generation");
    assert_ne!(new_generation, old_target.generation);
    assert_eq!(
        generation_fixture
            .database
            .evaluate_adaptive_workload(&window, AdaptiveWorkloadPolicy::default())
            .expect("stale generation evaluation")
            .outcome,
        AdaptiveWorkloadOutcome::StaleWindow(AdaptiveWorkloadStaleReason::TargetGenerationChanged)
    );
    let (_, new_feedback) = generation_fixture
        .database
        .query_with_feedback("SELECT id, category FROM events WHERE id >= 400")
        .expect("new generation remains eligible");
    assert!(new_feedback.accesses.iter().any(|access| {
        access
            .actual
            .work
            .columnar
            .as_ref()
            .is_some_and(|columnar| columnar.generation == Some(new_generation))
    }));
    generation_fixture.close();

    let suffix = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
    let schema_root = std::env::temp_dir().join(format!(
        "netbadb-adaptive-workload-schema-stale-{}-{suffix}",
        std::process::id()
    ));
    fs::create_dir_all(&schema_root).expect("create schema fixture root");
    let mut schema_database = Database::create_catalog(
        schema_root.join("catalog"),
        vec![],
        Some(
            DatabaseCoordinatorConfig::new(schema_root.join("coordinator"))
                .with_global_visibility(),
        ),
    )
    .expect("create schema fixture database");
    schema_database
        .execute("CREATE TABLE events (id BIGINT, category BIGINT, payload TEXT)")
        .expect("create runtime events table");
    schema_database
        .enable_global_visibility()
        .expect("enable schema fixture global visibility");
    let mut transaction = schema_database
        .begin_transaction()
        .expect("begin schema fixture seed");
    for id in 0..512_i64 {
        schema_database
            .insert_into_in(
                TableId(1),
                &mut transaction,
                &[
                    ScalarValue::Int64(id),
                    ScalarValue::Int64(id % 8),
                    ScalarValue::Text(format!("payload-{id}")),
                ],
            )
            .expect("insert schema fixture row");
    }
    transaction.commit().expect("commit schema fixture seed");
    drop(transaction);
    schema_database
        .build_columnar_projection(ColumnarProjectionSpec::new(
            TableId(1),
            schema_root.join("projection"),
            vec![ColumnId(1), ColumnId(2)],
        ))
        .expect("build schema fixture projection");
    let old_target = workload_target(&schema_database);
    let (_, feedback) = schema_database
        .query_with_feedback("SELECT id, category FROM events WHERE id >= 400")
        .expect("schema sample");
    let mut window = AdaptiveWorkloadWindow::new(old_target, AdaptiveWorkloadLimits::default());
    window.record(&feedback).expect("record schema sample");
    schema_database
        .execute("ALTER TABLE events ADD COLUMN note TEXT")
        .expect("advance schema generation");
    assert_eq!(
        schema_database
            .evaluate_adaptive_workload(&window, AdaptiveWorkloadPolicy::default())
            .expect("stale schema evaluation")
            .outcome,
        AdaptiveWorkloadOutcome::StaleWindow(AdaptiveWorkloadStaleReason::SchemaChanged)
    );
    schema_database.close().expect("close schema fixture");
    let _ = fs::remove_dir_all(schema_root);
}

#[test]
fn window_is_bounded_overflow_safe_and_counts_queries_not_access_nodes() {
    let mut fixture = Fixture::create("bounded", true);
    let target = workload_target(&fixture.database);
    let (_, first) = fixture
        .database
        .query_with_feedback("SELECT id, category FROM events WHERE id >= 400")
        .expect("first bounded shape");
    let (_, second_shape) = fixture
        .database
        .query_with_feedback("SELECT id, category FROM events WHERE id > 400")
        .expect("second bounded shape");
    let mut bounded = AdaptiveWorkloadWindow::new(target, AdaptiveWorkloadLimits::new(1, 8));
    bounded.record(&first).expect("record first shape");
    assert_eq!(
        bounded
            .record(&second_shape)
            .expect("truncate second shape"),
        AdaptiveWorkloadRecordOutcome::Truncated
    );
    assert!(bounded.truncated && bounded.incomplete);
    assert_eq!(
        fixture
            .database
            .evaluate_adaptive_workload(&bounded, AdaptiveWorkloadPolicy::new(1, 1, 1, 0, 0),)
            .expect("truncated evaluation")
            .outcome,
        AdaptiveWorkloadOutcome::Inconclusive
    );

    let mut multiple_accesses = first.clone();
    let target_access = multiple_accesses
        .accesses
        .iter()
        .find(|access| {
            access
                .planner
                .as_ref()
                .is_some_and(|planner| planner.projection_id == Some(target.projection_id))
        })
        .expect("target access")
        .clone();
    multiple_accesses.accesses.push(target_access);
    let mut query_count = AdaptiveWorkloadWindow::new(target, AdaptiveWorkloadLimits::default());
    query_count
        .record(&multiple_accesses)
        .expect("record multi-access query");
    assert_eq!(query_count.total_samples, 1);
    assert_eq!(query_count.total_target_accesses, 2);

    let mut overflow = AdaptiveWorkloadWindow::new(target, AdaptiveWorkloadLimits::default());
    overflow.total_actual_work_units = u64::MAX;
    overflow.record(&first).expect("record overflow sample");
    assert!(overflow.overflowed && overflow.incomplete);
    assert_eq!(
        fixture
            .database
            .evaluate_adaptive_workload(&overflow, AdaptiveWorkloadPolicy::new(1, 1, 1, 0, 0),)
            .expect("overflow evaluation")
            .outcome,
        AdaptiveWorkloadOutcome::Inconclusive
    );
    fixture.close();
}

#[test]
fn one_shape_keeps_columnar_and_source_variants_with_calibration_diagnostics() {
    let mut fixture = Fixture::create("variant-groups", true);
    let target = workload_target(&fixture.database);
    let sql = "SELECT id, category FROM events WHERE id >= 400";
    let (_, columnar) = fixture
        .database
        .query_with_feedback(sql)
        .expect("columnar variant report");
    fixture
        .database
        .adaptive_runtime
        .suppress(target.projection_id, target.generation);
    fixture
        .database
        .create_index(TABLE_ID, ColumnId(1))
        .expect("create diagnostic index");
    fixture
        .database
        .analyze(TABLE_ID)
        .expect("analyze diagnostic index");
    let (_, source) = fixture
        .database
        .query_with_feedback(sql)
        .expect("source variant report");
    assert_eq!(columnar.query_shape, source.query_shape);
    assert_ne!(columnar.plan_variant, source.plan_variant);

    let mut window = AdaptiveWorkloadWindow::new(target, AdaptiveWorkloadLimits::default());
    assert!(matches!(
        window.record(&columnar).expect("record target variant"),
        AdaptiveWorkloadRecordOutcome::RecordedRelevant { .. }
    ));
    assert_eq!(
        window.record(&source).expect("record source diagnostics"),
        AdaptiveWorkloadRecordOutcome::RecordedNotRelevant
    );
    let (_, point) = fixture
        .database
        .query_with_feedback("SELECT id FROM events WHERE id = 17")
        .expect("point diagnostic report");
    let (_, range) = fixture
        .database
        .query_with_feedback("SELECT id FROM events WHERE id >= 17 AND id < 20")
        .expect("range diagnostic report");
    window.record(&point).expect("record point diagnostics");
    window.record(&range).expect("record range diagnostics");
    assert_eq!(window.total_samples, 1);
    assert_eq!(window.query_shapes.len(), 3);
    assert_eq!(window.query_shapes[0].plan_variants.len(), 2);
    let classes = window
        .query_shapes
        .iter()
        .flat_map(|shape| shape.plan_variants.iter())
        .flat_map(|variant| variant.calibration.iter())
        .map(|calibration| calibration.calibration_class)
        .collect::<Vec<_>>();
    assert!(classes.contains(&netbadb_planner::PlannerCalibrationClass::Columnar));
    assert!(classes.contains(&netbadb_planner::PlannerCalibrationClass::SeqScan));
    assert!(classes.contains(&netbadb_planner::PlannerCalibrationClass::IndexPoint));
    assert!(classes.contains(&netbadb_planner::PlannerCalibrationClass::IndexRange));
    fixture.close();
}
