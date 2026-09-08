use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use netbadb_inspect::{PlanNodeInspection, StatementPlanInspection};
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_types::{ColumnId, PhysicalType, ScalarValue, TableId};

use crate::{
    AdaptiveAbortReason, AdaptiveColumnarAction, AdaptiveDecision, AdaptiveMaintenanceOutcome,
    AdaptiveNoActionReason, AdaptivePolicy, ColumnarProjectionHealth, ColumnarProjectionSpec,
    Database, DatabaseCoordinatorConfig, MaintenanceBudget, StorageKind, TableStorageCreateSpec,
    cleanup_created_table_files,
};

static NEXT_PATH: AtomicU64 = AtomicU64::new(1);
const TABLE_ID: TableId = TableId(1);

#[derive(Clone, Copy)]
enum SourceKind {
    Heap,
    Lsm,
}

struct Fixture {
    root: PathBuf,
    catalog: PathBuf,
    source: PathBuf,
    projection: PathBuf,
    database: Database,
}

impl Fixture {
    fn create(name: &str, kind: SourceKind, with_projection: bool) -> Self {
        let suffix = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "netbadb-adaptive-{name}-{}-{suffix}",
            std::process::id()
        ));
        let catalog = root.join("catalog");
        let coordinator = root.join("coordinator");
        let source = root.join("source");
        let projection = root.join("projection");
        fs::create_dir_all(&root).expect("create adaptive fixture root");
        let storage = match kind {
            SourceKind::Heap => TableStorageCreateSpec::heap(&source, table()),
            SourceKind::Lsm => TableStorageCreateSpec::lsm(&source, table(), ColumnId(1)),
        };
        let mut database = Database::create_catalog(
            &catalog,
            vec![storage],
            Some(DatabaseCoordinatorConfig::new(&coordinator).with_global_visibility()),
        )
        .expect("create globally visible adaptive fixture");
        let mut seed = database.begin_transaction().expect("begin adaptive seed");
        for id in 0..512_i64 {
            database
                .insert_in(
                    &mut seed,
                    &[
                        ScalarValue::Int64(id),
                        ScalarValue::Int64(id * 10),
                        ScalarValue::Text(format!("{}-{id}", "payload".repeat(64))),
                    ],
                )
                .expect("insert adaptive seed row");
        }
        seed.commit().expect("commit adaptive seed");
        database
            .enable_change_stream(TABLE_ID)
            .expect("enable adaptive change stream");
        if with_projection {
            database
                .build_incremental_columnar_projection(
                    ColumnarProjectionSpec::new(
                        TABLE_ID,
                        &projection,
                        vec![ColumnId(1), ColumnId(2)],
                    )
                    .with_row_group_rows(256),
                )
                .expect("build adaptive projection");
        }
        Self {
            root,
            catalog,
            source,
            projection,
            database,
        }
    }

    fn make_lagging(&mut self, amount: i64) {
        self.database
            .execute(&format!("UPDATE events SET amount = {amount} WHERE id = 7"))
            .expect("make adaptive projection lag");
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
                "amount",
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

fn generous_budget() -> MaintenanceBudget {
    MaintenanceBudget::new(1 << 20, 1 << 30, 1 << 30, 1)
}

fn proposal(decision: &AdaptiveDecision) -> crate::AdaptiveMaintenanceProposal {
    match decision {
        AdaptiveDecision::Proposal(proposal) => proposal.clone(),
        AdaptiveDecision::NoAction(no_action) => {
            panic!("expected proposal, got {:?}", no_action.reason)
        }
    }
}

fn no_action(decision: &AdaptiveDecision) -> AdaptiveNoActionReason {
    match decision {
        AdaptiveDecision::NoAction(no_action) => no_action.reason,
        AdaptiveDecision::Proposal(_) => panic!("expected typed no-action"),
    }
}

fn plan_contains_columnar(plan: &PlanNodeInspection) -> bool {
    match plan {
        PlanNodeInspection::ColumnarScan { .. } => true,
        PlanNodeInspection::Filter { input, .. }
        | PlanNodeInspection::Sort { input, .. }
        | PlanNodeInspection::Project { input, .. }
        | PlanNodeInspection::ScalarProject { input, .. }
        | PlanNodeInspection::Aggregate { input, .. }
        | PlanNodeInspection::Limit { input, .. } => plan_contains_columnar(input),
        PlanNodeInspection::NestedLoopJoin { left, right, .. }
        | PlanNodeInspection::HashJoin { left, right, .. } => {
            plan_contains_columnar(left) || plan_contains_columnar(right)
        }
        PlanNodeInspection::IndexNestedLoopJoin { left, .. } => plan_contains_columnar(left),
        PlanNodeInspection::OneRow
        | PlanNodeInspection::SeqScan { .. }
        | PlanNodeInspection::IndexScan { .. }
        | PlanNodeInspection::RangeIndexScan { .. }
        | PlanNodeInspection::PartitionedScan { .. } => false,
    }
}

fn statement_uses_columnar(database: &Database) -> bool {
    match database
        .inspect_statement("SELECT id, amount FROM events WHERE amount >= 0")
        .expect("inspect adaptive planner input")
        .plan
    {
        StatementPlanInspection::Query { root } => plan_contains_columnar(&root),
        _ => false,
    }
}

fn cleanup_root(root: &Path, source: &Path) {
    cleanup_created_table_files(&[source.to_owned()]);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn observation_and_no_action_are_typed_for_heap_lsm_fresh_missing_and_busy() {
    for (name, kind, expected_kind) in [
        ("observe-heap", SourceKind::Heap, StorageKind::Heap),
        ("observe-lsm", SourceKind::Lsm, StorageKind::Lsm),
    ] {
        let mut fixture = Fixture::create(name, kind, true);
        let before = fixture
            .database
            .observe_adaptive_columnar(TABLE_ID)
            .expect("observe fresh adaptive state");
        let source = before.source.as_ref().expect("source evidence");
        assert_eq!(source.storage_kind, expected_kind);
        assert_eq!(
            source.change_stream.current_data_version,
            source.data_version
        );
        assert!(source.change_stream.generation.is_some());
        assert_eq!(before.projections.len(), 1);
        assert_eq!(
            no_action(&before.decide(AdaptivePolicy::default(), generous_budget())),
            AdaptiveNoActionReason::AlreadyFresh
        );
        let projection_before = before.projections[0].projection.clone();
        let cycle = fixture
            .database
            .adaptive_columnar_step(TABLE_ID, AdaptivePolicy::default(), generous_budget())
            .expect("run fresh adaptive step");
        assert!(cycle.execution.is_none());
        assert_eq!(
            fixture
                .database
                .current_database_snapshot()
                .unwrap()
                .unwrap()
                .commit_seq(),
            before.anchor.global_commit_seq
        );
        let projection_after = fixture.database.inspect_columnar_projections().remove(0);
        assert_eq!(projection_after.generation, projection_before.generation);
        assert_eq!(
            projection_after.applied_frontier,
            projection_before.applied_frontier
        );

        let mut transaction = fixture
            .database
            .begin_transaction()
            .expect("begin busy txn");
        let busy = fixture
            .database
            .observe_adaptive_columnar(TABLE_ID)
            .expect("observe busy state");
        assert!(busy.active_transaction_handles > 0);
        assert_eq!(
            no_action(&busy.decide(AdaptivePolicy::default(), generous_budget())),
            AdaptiveNoActionReason::MaintenanceBusy
        );
        transaction.rollback().expect("rollback busy txn");
        drop(transaction);
        let root = fixture.root.clone();
        let source = fixture.source.clone();
        fixture.database.close().expect("close observation fixture");
        cleanup_root(&root, &source);
    }

    for (name, kind) in [
        ("observe-missing-heap", SourceKind::Heap),
        ("observe-missing-lsm", SourceKind::Lsm),
    ] {
        let mut missing = Fixture::create(name, kind, false);
        if matches!(kind, SourceKind::Heap) {
            missing
                .database
                .create_index(TABLE_ID, ColumnId(2))
                .expect("create observed access path");
        }
        missing
            .database
            .analyze(TABLE_ID)
            .expect("analyze observed source");
        let observation = missing
            .database
            .observe_adaptive_columnar(TABLE_ID)
            .expect("observe without projection");
        let source_evidence = observation.source.as_ref().expect("source evidence");
        assert!(source_evidence.statistics.is_some());
        assert_eq!(source_evidence.access_paths.len(), 1);
        assert!(source_evidence.access_paths[0].statistics.is_some());
        assert!(source_evidence.access_paths[0].capabilities.point_lookup);
        assert_eq!(
            no_action(&observation.decide(AdaptivePolicy::default(), generous_budget())),
            AdaptiveNoActionReason::NoColumnarProjection
        );
        let root = missing.root.clone();
        let source = missing.source.clone();
        missing.database.close().expect("close missing fixture");
        cleanup_root(&root, &source);
    }
}

#[test]
fn proposal_is_bound_and_revalidation_rejects_stale_stream_and_structural_work() {
    let mut fixture = Fixture::create("proposal-revalidation", SourceKind::Heap, true);
    fixture.make_lagging(7777);
    assert!(!statement_uses_columnar(&fixture.database));
    let observation = fixture
        .database
        .observe_adaptive_columnar(TABLE_ID)
        .expect("observe lagging projection");
    let source = observation.source.as_ref().expect("source evidence");
    let decision = observation.decide(AdaptivePolicy::default(), generous_budget());
    let stale = proposal(&decision);
    assert_eq!(stale.based_on, observation.anchor);
    assert_eq!(stale.storage_id, source.storage_id);
    assert_eq!(stale.storage_kind, source.storage_kind);
    assert_eq!(stale.expected_source_snapshot, source.snapshot_token);
    assert_eq!(stale.expected_source_data_version, source.data_version);
    assert_eq!(
        stale.expected_change_stream_generation,
        source.change_stream.generation.expect("stream generation")
    );
    assert_eq!(
        stale.expected_change_stream_frontier,
        source.change_stream.current_data_version
    );
    assert!(matches!(
        stale.action,
        AdaptiveColumnarAction::CatchUpExistingColumnar { .. }
    ));

    let manifest = fixture.projection.join("projection.nbcmanifest");
    let manifest_before = fs::read(&manifest).expect("read pre-stale manifest");
    fixture.make_lagging(8888);
    let source_after_dml = fixture
        .database
        .query("SELECT amount FROM events WHERE id = 7")
        .unwrap();
    let stale_report = fixture
        .database
        .execute_adaptive_columnar(&stale, generous_budget())
        .expect("reject stale proposal");
    assert_eq!(
        stale_report.outcome,
        AdaptiveMaintenanceOutcome::Aborted(AdaptiveAbortReason::StaleObservation)
    );
    assert_eq!(stale_report.consumed, Default::default());
    assert_eq!(fs::read(&manifest).unwrap(), manifest_before);
    assert_eq!(
        fixture
            .database
            .query("SELECT amount FROM events WHERE id = 7")
            .unwrap(),
        source_after_dml
    );

    let current = fixture
        .database
        .observe_adaptive_columnar(TABLE_ID)
        .expect("observe for budget check");
    let projection_before = current.projections[0].projection.clone();
    let tiny = MaintenanceBudget::new(0, 0, 0, 0);
    assert_eq!(
        no_action(&current.decide(AdaptivePolicy::default(), tiny)),
        AdaptiveNoActionReason::InsufficientBudgetEstimate
    );
    assert_eq!(
        fixture.database.inspect_columnar_projections()[0].applied_frontier,
        projection_before.applied_frontier
    );
    let current_proposal = proposal(&current.decide(AdaptivePolicy::default(), generous_budget()));
    let budget_abort = fixture
        .database
        .execute_adaptive_columnar(&current_proposal, tiny)
        .expect("reject insufficient execution budget");
    assert_eq!(
        budget_abort.outcome,
        AdaptiveMaintenanceOutcome::Aborted(AdaptiveAbortReason::BudgetInsufficient)
    );
    assert_eq!(budget_abort.consumed, Default::default());

    fixture
        .database
        .disable_change_stream(TABLE_ID)
        .expect("disable proposal stream");
    let stream_abort = fixture
        .database
        .execute_adaptive_columnar(&current_proposal, generous_budget())
        .expect("reject disabled stream");
    assert_eq!(
        stream_abort.outcome,
        AdaptiveMaintenanceOutcome::Aborted(AdaptiveAbortReason::PreconditionsChanged)
    );
    assert_eq!(stream_abort.consumed, Default::default());

    let root = fixture.root.clone();
    let source = fixture.source.clone();
    fixture
        .database
        .close()
        .expect("close revalidation fixture");
    cleanup_root(&root, &source);

    let mut structural_fixture = Fixture::create("structural", SourceKind::Heap, true);
    let mut structural = structural_fixture
        .database
        .begin_transaction()
        .expect("begin structural-owner transaction");
    // The writer token is the production busy boundary used by every schema
    // mutation path. This fixture's pre-created placement cannot enter DDL, so
    // install the same token directly to isolate the adaptive guard.
    structural_fixture
        .database
        .schema_writer
        .set(Some(structural.id()));
    let blocked = structural_fixture
        .database
        .observe_adaptive_columnar(TABLE_ID)
        .expect("observe structural mutation");
    assert!(blocked.structural_mutation_active);
    assert_eq!(
        no_action(&blocked.decide(AdaptivePolicy::default(), generous_budget())),
        AdaptiveNoActionReason::StructuralMutationActive
    );
    structural_fixture.database.schema_writer.set(None);
    structural.rollback().expect("rollback structural mutation");
    drop(structural);
    let root = structural_fixture.root.clone();
    let source = structural_fixture.source.clone();
    structural_fixture
        .database
        .close()
        .expect("close structural database");
    cleanup_root(&root, &source);
}

fn assert_complete_keep_and_reopen(name: &str, kind: SourceKind) {
    let mut fixture = Fixture::create(name, kind, true);
    fixture.make_lagging(9001);
    let source_truth = fixture
        .database
        .query("SELECT id, amount FROM events ORDER BY id")
        .expect("capture source truth");
    let before = fixture
        .database
        .observe_adaptive_columnar(TABLE_ID)
        .expect("observe before keep");
    let before_state = before.projections[0].projection.clone();
    assert_eq!(before_state.health, ColumnarProjectionHealth::Lagging);
    let manifest = fixture.projection.join("projection.nbcmanifest");
    let manifest_before_planning = fs::read(&manifest).expect("read manifest before planning");
    assert!(!statement_uses_columnar(&fixture.database));
    assert_eq!(
        fs::read(&manifest).expect("read manifest after planning"),
        manifest_before_planning,
        "planner inspection must remain read-only"
    );

    let cycle = fixture
        .database
        .adaptive_columnar_step(TABLE_ID, AdaptivePolicy::default(), generous_budget())
        .expect("complete adaptive keep loop");
    let execution = cycle.execution.expect("executed proposal");
    assert_eq!(execution.outcome, AdaptiveMaintenanceOutcome::Kept);
    assert!(execution.consumed.actions <= execution.budget_before.max_actions);
    assert!(execution.consumed.work_units <= execution.budget_before.max_work_units);
    assert!(execution.consumed.read_bytes <= execution.budget_before.max_read_bytes);
    assert!(execution.consumed.write_bytes <= execution.budget_before.max_write_bytes);
    let measurement = execution.measurement.expect("structured measurement");
    assert!(measurement.before.lag > 0);
    assert_eq!(measurement.after_change.lag, 0);
    assert_eq!(measurement.after_outcome.lag, 0);
    assert!(!measurement.before.planner.planner_eligible);
    assert!(measurement.after_outcome.planner.planner_eligible);
    assert_eq!(
        measurement.before.projection_generation, measurement.after_change.projection_generation,
        "ordinary delta publication retains the existing generation"
    );
    assert!(measurement.source_snapshot_unchanged);
    assert_eq!(
        measurement.global_commit_seq_before,
        measurement.global_commit_seq_after
    );
    assert_eq!(
        measurement.schema_generation_before,
        measurement.schema_generation_after
    );
    assert!(statement_uses_columnar(&fixture.database));
    assert_eq!(
        fixture
            .database
            .query("SELECT id, amount FROM events ORDER BY id")
            .expect("query kept projection"),
        source_truth
    );
    let after = fixture.database.inspect_columnar_projections().remove(0);
    assert_eq!(after.health, ColumnarProjectionHealth::Fresh);
    assert_ne!(after.applied_frontier, before_state.applied_frontier);
    let generation = after.generation;
    let frontier = after.applied_frontier;

    fixture.database.close().expect("close kept fixture");
    let mut reopened = Database::open_catalog(&fixture.catalog).expect("reopen kept fixture");
    let reopened_projection = reopened.inspect_columnar_projections().remove(0);
    assert_eq!(reopened_projection.health, ColumnarProjectionHealth::Fresh);
    assert_eq!(reopened_projection.generation, generation);
    assert_eq!(reopened_projection.applied_frontier, frontier);
    assert!(statement_uses_columnar(&reopened));
    assert_eq!(
        reopened
            .query("SELECT id, amount FROM events ORDER BY id")
            .expect("query reopened projection"),
        source_truth
    );
    reopened.close().expect("close reopened fixture");
    cleanup_root(&fixture.root, &fixture.source);
}

#[test]
fn complete_keep_loop_changes_real_planner_view_and_reopens_for_heap_and_lsm() {
    assert_complete_keep_and_reopen("keep-heap", SourceKind::Heap);
    assert_complete_keep_and_reopen("keep-lsm", SourceKind::Lsm);
}

#[test]
fn revert_suppresses_only_the_derived_generation_and_never_drops_the_projection() {
    let mut fixture = Fixture::create("revert", SourceKind::Heap, true);
    fixture.make_lagging(42);
    let source_truth = fixture
        .database
        .query("SELECT id, amount FROM events ORDER BY id")
        .expect("capture revert source truth");
    let commit_before = fixture
        .database
        .current_database_snapshot()
        .unwrap()
        .unwrap()
        .commit_seq();
    let policy = AdaptivePolicy::new(1, u64::MAX);
    let cycle = fixture
        .database
        .adaptive_columnar_step(TABLE_ID, policy, generous_budget())
        .expect("complete deterministic revert loop");
    let execution = cycle.execution.expect("execute revert candidate");
    assert_eq!(
        execution.outcome,
        AdaptiveMaintenanceOutcome::RevertedInsufficientMeasuredBenefit
    );
    assert!(execution.measurement.unwrap().source_snapshot_unchanged);
    assert_eq!(
        fixture
            .database
            .current_database_snapshot()
            .unwrap()
            .unwrap()
            .commit_seq(),
        commit_before
    );
    assert_eq!(
        fixture
            .database
            .query("SELECT id, amount FROM events ORDER BY id")
            .expect("query source after revert"),
        source_truth
    );
    assert!(!statement_uses_columnar(&fixture.database));
    let projections = fixture.database.inspect_columnar_projections();
    assert_eq!(projections.len(), 1);
    assert_eq!(projections[0].health, ColumnarProjectionHealth::Fresh);
    let suppressed = fixture
        .database
        .observe_adaptive_columnar(TABLE_ID)
        .expect("observe suppression guard");
    assert_eq!(
        no_action(&suppressed.decide(policy, generous_budget())),
        AdaptiveNoActionReason::SuppressedAfterRevert
    );

    fixture.database.close().expect("close reverted fixture");
    let reopened = Database::open_catalog(&fixture.catalog).expect("reopen reverted fixture");
    assert_eq!(reopened.inspect_columnar_projections().len(), 1);
    assert!(
        statement_uses_columnar(&reopened),
        "Phase 1 suppression is explicitly runtime-only"
    );
    reopened.close().expect("close reopened revert fixture");
    cleanup_root(&fixture.root, &fixture.source);
}
