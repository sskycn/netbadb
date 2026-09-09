use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_types::{ColumnId, PhysicalType, ScalarValue, TableId};

use crate::{
    AdaptiveExecutionFeedbackOutcome, AdaptiveMaintenanceOutcome, AdaptivePolicy,
    ColumnarProjectionSpec, Database, DatabaseCoordinatorConfig, ExecutionAccessKind,
    ExecutionFeedbackPolicy, MaintenanceBudget, TableStorageCreateSpec,
    cleanup_created_table_files,
};

pub(super) static NEXT_PATH: AtomicU64 = AtomicU64::new(1);
pub(super) const TABLE_ID: TableId = TableId(88_001);

pub(super) struct Fixture {
    pub(super) root: PathBuf,
    pub(super) source: PathBuf,
    pub(super) database: Database,
}

impl Fixture {
    pub(super) fn create(name: &str, projection: bool) -> Self {
        let suffix = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "netbadb-execution-feedback-{name}-{}-{suffix}",
            std::process::id()
        ));
        fs::create_dir_all(&root).expect("create feedback fixture root");
        let source = root.join("source");
        let mut database = Database::create_catalog(
            root.join("catalog"),
            vec![TableStorageCreateSpec::heap(&source, table())],
            Some(DatabaseCoordinatorConfig::new(root.join("coordinator")).with_global_visibility()),
        )
        .expect("create feedback database");
        let mut transaction = database.begin_transaction().expect("begin seed");
        for id in 0..512_i64 {
            database
                .insert_in(
                    &mut transaction,
                    &[
                        ScalarValue::Int64(id),
                        ScalarValue::Int64(id % 8),
                        ScalarValue::Text(format!("payload-{id}")),
                    ],
                )
                .expect("insert seed row");
        }
        transaction.commit().expect("commit seed");
        if projection {
            database
                .enable_change_stream(TABLE_ID)
                .expect("enable feedback stream");
            database
                .build_incremental_columnar_projection(
                    ColumnarProjectionSpec::new(
                        TABLE_ID,
                        root.join("projection"),
                        vec![ColumnId(1), ColumnId(2)],
                    )
                    .with_row_group_rows(256),
                )
                .expect("build feedback projection");
        }
        Self {
            root,
            source,
            database,
        }
    }

    pub(super) fn close(self) {
        self.database.close().expect("close feedback fixture");
        cleanup_root(&self.root, &self.source);
    }
}

fn table() -> TableDef {
    TableDef::new(
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
    )
}

fn cleanup_root(root: &Path, source: &Path) {
    cleanup_created_table_files(&[source.to_owned()]);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn feedback_is_transparent_and_measures_seq_filter_point_and_range_access() {
    let mut fixture = Fixture::create("accesses", false);
    let sql = "SELECT id FROM events WHERE category >= 5";
    let normal = fixture.database.query(sql).expect("normal seq query");
    let before = fixture
        .database
        .current_database_snapshot()
        .unwrap()
        .unwrap()
        .commit_seq();
    let (observed, feedback) = fixture
        .database
        .query_with_feedback(sql)
        .expect("observed seq query");
    assert_eq!(observed, normal);
    assert_eq!(
        feedback.calibration_epoch,
        netbadb_planner::PlannerCalibrationEpoch(0)
    );
    assert_eq!(feedback.accesses.len(), 1);
    assert_eq!(
        feedback.accesses[0].actual.kind,
        ExecutionAccessKind::SeqScan
    );
    assert_eq!(feedback.accesses[0].actual.work.rows_examined, 512);
    assert_eq!(feedback.accesses[0].actual.work.rows_output, 512);
    assert_eq!(feedback.filters.len(), 1);
    assert_eq!(feedback.filters[0].work.filter_rows_evaluated, 512);
    assert_eq!(feedback.filters[0].work.filter_rows_passed, 192);
    assert_eq!(feedback.filters[0].work.filter_rows_rejected, 320);
    let planner = feedback.accesses[0]
        .planner
        .as_ref()
        .expect("seq planner estimate");
    assert_eq!(planner.estimated_work_units, planner.effective_work_units);
    assert_eq!(
        planner.calibration_epoch,
        netbadb_planner::PlannerCalibrationEpoch(0)
    );
    let calibration = feedback.accesses[0].calibration.expect("seq calibration");
    assert_eq!(
        Some(calibration.estimated_work_units),
        calibration.effective_estimated_work_units
    );
    assert_eq!(calibration.direction, calibration.effective_direction);
    assert_eq!(
        calibration.absolute_error_work_units,
        calibration.effective_absolute_error_work_units
    );
    assert_eq!(
        fixture
            .database
            .current_database_snapshot()
            .unwrap()
            .unwrap()
            .commit_seq(),
        before
    );

    fixture
        .database
        .create_index(TABLE_ID, ColumnId(1))
        .expect("create feedback index");
    fixture.database.analyze(TABLE_ID).expect("analyze index");
    let (_, point) = fixture
        .database
        .query_with_feedback("SELECT id FROM events WHERE id = 17")
        .expect("point feedback");
    let point = point
        .accesses
        .iter()
        .find(|sample| sample.actual.kind == ExecutionAccessKind::IndexPoint)
        .expect("point access sample");
    assert_eq!(point.actual.work.index_point_probes, 1);
    assert_eq!(point.actual.work.index_candidates_examined, 1);
    assert_eq!(point.actual.work.rows_output, 1);
    assert!(point.actual.access_path.is_some());

    let (_, range) = fixture
        .database
        .query_with_feedback("SELECT id FROM events WHERE id >= 10 AND id < 15")
        .expect("range feedback");
    let range = range
        .accesses
        .iter()
        .find(|sample| sample.actual.kind == ExecutionAccessKind::IndexRange)
        .expect("range access sample");
    assert_eq!(range.actual.work.index_range_probes, 1);
    assert_eq!(range.actual.work.index_candidates_examined, 5);
    assert_eq!(range.actual.work.rows_output, 5);
    assert!(range.calibration.is_some());
    fixture.close();
}

#[test]
fn columnar_feedback_reuses_production_counters_and_validates_phase1_keep() {
    let mut fixture = Fixture::create("columnar", true);
    fixture
        .database
        .execute("UPDATE events SET category = 7 WHERE id = 3")
        .expect("make projection stale");
    let cycle = fixture
        .database
        .adaptive_columnar_step(
            TABLE_ID,
            AdaptivePolicy::default(),
            MaintenanceBudget::new(1 << 20, 1 << 30, 1 << 30, 1),
        )
        .expect("run phase1 catch-up");
    assert_eq!(
        cycle.execution.expect("phase1 execution").outcome,
        AdaptiveMaintenanceOutcome::Kept
    );
    let projection = fixture.database.inspect_columnar_projections().remove(0);
    let projection_id = projection.projection_id.expect("projection id");
    let generation = projection.generation.expect("projection generation");
    let sql = "SELECT id, category FROM events WHERE id >= 400";
    let normal = fixture.database.query(sql).expect("normal columnar query");
    let before = fixture
        .database
        .current_database_snapshot()
        .unwrap()
        .unwrap()
        .commit_seq();
    let (observed, feedback) = fixture
        .database
        .query_with_feedback(sql)
        .expect("columnar feedback query");
    assert_eq!(observed, normal);
    let access = feedback
        .accesses
        .iter()
        .find(|sample| sample.actual.kind == ExecutionAccessKind::Columnar)
        .expect("columnar access sample");
    let columnar = access.actual.work.columnar.as_ref().expect("columnar scan");
    assert_eq!(columnar.projection_id, Some(projection_id));
    assert_eq!(columnar.generation, Some(generation));
    assert!(columnar.scan.row_groups_total > 0);
    assert!(columnar.scan.row_groups_read > 0);
    assert!(columnar.scan.row_groups_pruned > 0);
    assert!(columnar.scan.row_groups_pruned_before_data_read > 0);
    assert_eq!(
        columnar.scan.row_groups_total,
        columnar.scan.row_groups_read + columnar.scan.row_groups_pruned
    );
    assert!(columnar.scan.rows_read > 0);
    assert!(columnar.scan.physical_bytes_read > 0);
    assert!(columnar.scan.delta_segments > 0);
    assert!(access.calibration.is_some());

    let reports = vec![feedback.clone(), feedback.clone(), feedback.clone()];
    let validation = fixture
        .database
        .evaluate_adaptive_execution_feedback(
            projection_id,
            generation,
            &reports,
            ExecutionFeedbackPolicy::new(3, 1, u64::MAX),
        )
        .expect("validate phase2 keep");
    assert_eq!(
        validation.outcome,
        AdaptiveExecutionFeedbackOutcome::ValidatedKeep
    );
    assert_eq!(validation.sample_count, 3);
    assert_eq!(
        fixture
            .database
            .current_database_snapshot()
            .unwrap()
            .unwrap()
            .commit_seq(),
        before
    );
    fixture.close();
}

#[test]
fn feedback_policy_is_bounded_stale_safe_and_overflow_inconclusive() {
    let mut fixture = Fixture::create("policy", true);
    let projection = fixture.database.inspect_columnar_projections().remove(0);
    let projection_id = projection.projection_id.expect("projection id");
    let generation = projection.generation.expect("projection generation");
    let (_, feedback) = fixture
        .database
        .query_with_feedback("SELECT id, category FROM events WHERE id >= 0")
        .expect("collect policy sample");

    let insufficient = fixture
        .database
        .evaluate_adaptive_execution_feedback(
            projection_id,
            generation,
            &[feedback.clone(), feedback.clone()],
            ExecutionFeedbackPolicy::new(3, 1, 0),
        )
        .expect("evaluate insufficient feedback");
    assert_eq!(
        insufficient.outcome,
        AdaptiveExecutionFeedbackOutcome::Inconclusive
    );

    let mut incomplete = feedback.clone();
    incomplete.overflowed = true;
    incomplete.incomplete = true;
    let overflow = fixture
        .database
        .evaluate_adaptive_execution_feedback(
            projection_id,
            generation,
            &[incomplete.clone(), incomplete.clone(), incomplete],
            ExecutionFeedbackPolicy::new(3, 1, 0),
        )
        .expect("evaluate overflow feedback");
    assert_eq!(
        overflow.outcome,
        AdaptiveExecutionFeedbackOutcome::Inconclusive
    );

    let stale = fixture
        .database
        .evaluate_adaptive_execution_feedback(
            projection_id,
            netbadb_types::ColumnarGeneration(generation.0 + 1),
            &[feedback.clone(), feedback.clone(), feedback.clone()],
            ExecutionFeedbackPolicy::new(3, 1, 0),
        )
        .expect("reject stale generation");
    assert_eq!(
        stale.outcome,
        AdaptiveExecutionFeedbackOutcome::StaleFeedback
    );

    let before = fixture
        .database
        .current_database_snapshot()
        .unwrap()
        .unwrap()
        .commit_seq();
    let regression = feedback;
    let columnar = regression
        .accesses
        .iter()
        .find(|access| access.actual.kind == ExecutionAccessKind::Columnar)
        .expect("real columnar regression sample");
    assert!(
        columnar
            .calibration
            .expect("columnar calibration")
            .actual_work_units
            .expect("actual columnar work")
            > columnar
                .planner
                .as_ref()
                .and_then(|planner| planner.source_alternative_work_units)
                .expect("planning-time source alternative")
    );
    let regressions = vec![regression.clone(), regression.clone(), regression];
    let reverted = fixture
        .database
        .evaluate_adaptive_execution_feedback(
            projection_id,
            generation,
            &regressions,
            ExecutionFeedbackPolicy::new(3, 1, 0),
        )
        .expect("evaluate measured regression");
    assert_eq!(
        reverted.outcome,
        AdaptiveExecutionFeedbackOutcome::RevertedMeasuredRegression
    );
    assert_eq!(
        fixture
            .database
            .current_database_snapshot()
            .unwrap()
            .unwrap()
            .commit_seq(),
        before
    );
    assert_eq!(fixture.database.inspect_columnar_projections().len(), 1);
    let (_, fallback) = fixture
        .database
        .query_with_feedback("SELECT id, category FROM events WHERE id >= 0")
        .expect("query after runtime suppression");
    assert!(
        fallback
            .accesses
            .iter()
            .all(|sample| sample.actual.kind != ExecutionAccessKind::Columnar)
    );
    fixture.close();
}

#[test]
fn telemetry_counter_overflow_saturates_without_an_execution_error() {
    let mut work = crate::ExecutionWork {
        rows_examined: u64::MAX,
        ..crate::ExecutionWork::default()
    };
    work.add_rows_examined(1);
    assert_eq!(work.rows_examined, u64::MAX);
    assert!(work.overflowed);
    assert!(work.incomplete);
}

#[test]
fn one_report_keeps_distinct_samples_for_multiple_access_nodes() {
    let suffix = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
    let root = std::env::temp_dir().join(format!(
        "netbadb-execution-feedback-join-{}-{suffix}",
        std::process::id()
    ));
    fs::create_dir_all(&root).expect("create join feedback root");
    let left_path = root.join("left");
    let right_path = root.join("right");
    let one_column_table = |id, name| {
        TableDef::new(
            id,
            name,
            vec![ColumnDef::new(
                ColumnId(1),
                "id",
                TypeSpec::Physical(PhysicalType::Int64),
            )],
        )
    };
    let mut database = Database::create_catalog(
        root.join("catalog"),
        vec![
            TableStorageCreateSpec::heap(
                &left_path,
                one_column_table(TableId(91_001), "left_rows"),
            ),
            TableStorageCreateSpec::heap(
                &right_path,
                one_column_table(TableId(91_002), "right_rows"),
            ),
        ],
        Some(DatabaseCoordinatorConfig::new(root.join("coordinator")).with_global_visibility()),
    )
    .expect("create join feedback database");
    for id in 0..3 {
        database
            .execute(&format!("INSERT INTO left_rows VALUES ({id})"))
            .expect("insert left row");
        database
            .execute(&format!("INSERT INTO right_rows VALUES ({id})"))
            .expect("insert right row");
    }
    let (_, feedback) = database
        .query_with_feedback("SELECT l.id FROM left_rows l JOIN right_rows r ON l.id = r.id")
        .expect("collect multi-access feedback");
    assert_eq!(feedback.accesses.len(), 2);
    assert_ne!(
        feedback.accesses[0].actual.node,
        feedback.accesses[1].actual.node
    );
    assert!(
        feedback
            .accesses
            .iter()
            .all(|sample| sample.actual.kind == ExecutionAccessKind::SeqScan)
    );
    assert_eq!(
        feedback
            .accesses
            .iter()
            .map(|sample| sample.actual.work.rows_examined)
            .collect::<Vec<_>>(),
        vec![3, 3]
    );
    database.close().expect("close join feedback database");
    cleanup_created_table_files(&[left_path, right_path]);
    let _ = fs::remove_dir_all(root);
}
