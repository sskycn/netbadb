use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_types::{ColumnId, PhysicalType, ScalarValue, StorageDataVersion, TableId};

use crate::{
    AdaptiveChangeStreamGcAbortReason, AdaptiveChangeStreamGcDecision,
    AdaptiveChangeStreamGcOutcome, AdaptiveChangeStreamGcPolicy, AdaptiveEvidencePool,
    AutomaticAdmissionScope, AutomaticMultiSafeModeInput, AutomaticMultiSafeModePolicy,
    AutomaticSafeModeLane, AutomaticSafeModeMutation, AutomaticSafeModeOutcome, ChangeStreamCursor,
    ChangeStreamRetentionConsumer, ColumnarAdvanceBudget, ColumnarProjectionSpec, Database,
    MaintenanceBudget, TableStorageCreateSpec, cleanup_created_table_files,
};

static NEXT_PATH: AtomicU64 = AtomicU64::new(1);
const TABLE_ID: TableId = TableId(1);

struct Fixture {
    root: PathBuf,
    heap: PathBuf,
    database: Database,
    origin: ChangeStreamCursor,
    projection_id: netbadb_types::ColumnarProjectionId,
    second_projection_id: Option<netbadb_types::ColumnarProjectionId>,
}

impl Fixture {
    fn new(name: &str) -> Self {
        Self::with_projection_count(name, 1)
    }

    fn with_projection_count(name: &str, projection_count: usize) -> Self {
        let suffix = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "netbadb-adaptive-gc-{name}-{}-{suffix}",
            std::process::id()
        ));
        let catalog = root.join("catalog");
        let heap = root.join("heap");
        fs::create_dir_all(&root).expect("create fixture root");
        let table = TableDef::new(
            TABLE_ID,
            "events",
            vec![
                ColumnDef::new(ColumnId(1), "id", TypeSpec::Physical(PhysicalType::Int64)),
                ColumnDef::new(
                    ColumnId(2),
                    "amount",
                    TypeSpec::Physical(PhysicalType::Int64),
                ),
            ],
        );
        let mut database = Database::create_catalog(
            &catalog,
            vec![TableStorageCreateSpec::heap(&heap, table)],
            None,
        )
        .expect("create fixture database");
        let origin = database
            .enable_change_stream(TABLE_ID)
            .expect("enable change stream");
        let projection_ids = (0..projection_count)
            .map(|index| {
                database
                    .build_incremental_columnar_projection(ColumnarProjectionSpec::new(
                        TABLE_ID,
                        root.join(format!("projection-{index}")),
                        vec![ColumnId(1), ColumnId(2)],
                    ))
                    .expect("build incremental projection")
            })
            .collect::<Vec<_>>();
        let projection_id = projection_ids[0];
        for id in 0..3 {
            database
                .insert(&[ScalarValue::Int64(id), ScalarValue::Int64(id * 10)])
                .expect("insert stream row");
        }
        database
            .advance_columnar_projection(projection_id, ColumnarAdvanceBudget::new(2, 1 << 30))
            .expect("advance projection to D2");
        if let Some(second) = projection_ids.get(1).copied() {
            database
                .advance_columnar_projection(second, ColumnarAdvanceBudget::new(1, 1 << 30))
                .expect("advance second projection to D1");
        }
        Self {
            root,
            heap,
            database,
            origin,
            projection_id,
            second_projection_id: projection_ids.get(1).copied(),
        }
    }

    fn proposal(&self) -> crate::AdaptiveChangeStreamGcProposal {
        let observation = self
            .database
            .observe_change_stream_reclamation(TABLE_ID)
            .expect("observe retention");
        assert_eq!(
            observation.safe_reclaim_through.map(|safe| safe.frontier()),
            Some(StorageDataVersion(2))
        );
        match self
            .database
            .advise_change_stream_reclamation(
                &observation,
                AdaptiveChangeStreamGcPolicy::new(1, 0),
                generous_budget(),
            )
            .expect("advise GC")
        {
            AdaptiveChangeStreamGcDecision::Proposal(proposal) => *proposal,
            other => panic!("expected proposal, got {other:?}"),
        }
    }

    fn cleanup(self) {
        self.database.close().expect("close fixture database");
        cleanup_created_table_files(&[self.heap]);
        let _ = fs::remove_dir_all(self.root);
    }
}

fn generous_budget() -> MaintenanceBudget {
    MaintenanceBudget::new(u64::MAX, u64::MAX, u64::MAX, 1)
}

#[test]
fn retention_pin_is_explicit_forward_only_and_shared_by_manual_gc() {
    let mut fixture = Fixture::new("pin");
    let cursor = ChangeStreamCursor {
        frontier: StorageDataVersion(1),
        ..fixture.origin
    };
    let mut pin = fixture
        .database
        .pin_change_stream(TABLE_ID, cursor)
        .expect("pin D1");
    let observation = fixture
        .database
        .observe_change_stream_reclamation(TABLE_ID)
        .expect("observe pinned retention");
    assert_eq!(
        observation.safe_reclaim_through.map(|safe| safe.frontier()),
        Some(StorageDataVersion(1))
    );
    assert!(observation.consumers.iter().any(|consumer| matches!(
        consumer,
        ChangeStreamRetentionConsumer::RuntimePin {
            required_frontier: StorageDataVersion(1),
            ..
        }
    )));
    assert!(
        fixture
            .database
            .registry
            .get_mut(fixture.origin.storage_id)
            .expect("source storage")
            .gc_change_stream(StorageDataVersion(2))
            .is_err(),
        "the production storage writer is the final pin-safety guard"
    );
    assert!(
        fixture
            .database
            .advance_change_stream_retention_pin(TABLE_ID, &mut pin, StorageDataVersion(0))
            .is_err(),
        "a pin cannot reacquire older history"
    );
    fixture
        .database
        .advance_change_stream_retention_pin(TABLE_ID, &mut pin, StorageDataVersion(2))
        .expect("advance pin to D2");
    let report = fixture
        .database
        .gc_change_stream(TABLE_ID)
        .expect("manual GC honors pin and projection");
    assert_eq!(report.new_earliest_frontier, StorageDataVersion(2));
    drop(pin);
    fixture.cleanup();
}

#[test]
fn slowest_exact_projection_generation_controls_the_safe_frontier() {
    let fixture = Fixture::with_projection_count("two-projections", 2);
    let observation = fixture
        .database
        .observe_change_stream_reclamation(TABLE_ID)
        .expect("observe two consumers");
    assert_eq!(
        observation.safe_reclaim_through.map(|safe| safe.frontier()),
        Some(StorageDataVersion(1))
    );
    assert_eq!(observation.limiting_consumers.len(), 1);
    assert!(matches!(
        observation.limiting_consumers[0],
        ChangeStreamRetentionConsumer::Columnar {
            projection_id,
            required_frontier: StorageDataVersion(1),
            ..
        } if Some(projection_id) == fixture.second_projection_id
    ));
    fixture.cleanup();
}

#[test]
fn proposal_uses_dominance_revalidation_for_dml_consumer_and_new_pin() {
    let mut fixture = Fixture::new("dominance");
    let proposal = fixture.proposal();
    fixture
        .database
        .insert(&[ScalarValue::Int64(9), ScalarValue::Int64(90)])
        .expect("advance current frontier without changing safe frontier");
    fixture
        .database
        .advance_columnar_projection(
            fixture.projection_id,
            ColumnarAdvanceBudget::new(10, 1 << 30),
        )
        .expect("advance existing consumer beyond proposal frontier");
    let visibility_before = fixture
        .database
        .inspect_global_visibility()
        .expect("inspect G before GC");
    let schema_before = fixture.database.schema_generation();
    let rows_before = fixture
        .database
        .query("SELECT id, amount FROM events ORDER BY id")
        .expect("query before GC");
    let execution = fixture
        .database
        .execute_change_stream_reclamation(&proposal, generous_budget())
        .expect("execute proposal after DML");
    assert_eq!(execution.outcome, AdaptiveChangeStreamGcOutcome::Completed);
    let consumed = execution.consumed;
    let actual = execution.actual.expect("production report");
    assert_eq!(actual.new_earliest_frontier, StorageDataVersion(2));
    assert_eq!(actual.generation, fixture.origin.generation);
    assert!(actual.batches_removed > 0);
    assert!(actual.bytes_reclaimed > 0);
    assert_eq!(consumed.read_bytes, actual.bytes_before);
    assert_eq!(consumed.write_bytes, actual.bytes_after);
    assert!(actual.bytes_after > proposal.expected_prefix.rewrite_bytes);
    assert_eq!(fixture.database.schema_generation(), schema_before);
    assert_eq!(
        fixture
            .database
            .inspect_global_visibility()
            .expect("inspect G after GC"),
        visibility_before
    );
    assert_eq!(
        fixture
            .database
            .query("SELECT id, amount FROM events ORDER BY id")
            .expect("query after GC"),
        rows_before
    );
    assert!(
        fixture
            .database
            .read_changes(TABLE_ID, fixture.origin, 10, 1 << 30)
            .is_err(),
        "an unpinned stateless cursor may lose reclaimed history"
    );
    let retained = fixture
        .database
        .read_changes(
            TABLE_ID,
            ChangeStreamCursor {
                frontier: StorageDataVersion(2),
                ..fixture.origin
            },
            10,
            1 << 30,
        )
        .expect("retained consumer frontier remains readable");
    assert_eq!(retained.batches.len(), 2);
    fixture.cleanup();
}

#[test]
fn pressure_budget_generation_and_already_reclaimed_paths_do_not_mutate_unsafely() {
    let mut fixture = Fixture::new("gates");
    let observation = fixture
        .database
        .observe_change_stream_reclamation(TABLE_ID)
        .expect("observe gates");
    assert!(matches!(
        fixture
            .database
            .advise_change_stream_reclamation(
                &observation,
                AdaptiveChangeStreamGcPolicy::new(u64::MAX, 0),
                generous_budget(),
            )
            .expect("pressure decision"),
        AdaptiveChangeStreamGcDecision::NoAction(
            crate::AdaptiveChangeStreamGcNoActionReason::PressureBelowThreshold
        )
    ));
    assert!(matches!(
        fixture
            .database
            .advise_change_stream_reclamation(
                &observation,
                AdaptiveChangeStreamGcPolicy::new(1, 0),
                MaintenanceBudget::new(0, 0, 0, 0),
            )
            .expect("budget decision"),
        AdaptiveChangeStreamGcDecision::NoAction(
            crate::AdaptiveChangeStreamGcNoActionReason::CostBoundExceeded
        )
    ));
    let proposal = fixture.proposal();
    fixture
        .database
        .advance_columnar_projection(
            fixture.projection_id,
            ColumnarAdvanceBudget::new(10, 1 << 30),
        )
        .expect("advance consumer beyond proposed frontier");
    let earliest = fixture
        .database
        .gc_change_stream(TABLE_ID)
        .expect("manual GC wins race")
        .new_earliest_frontier;
    assert_eq!(earliest, StorageDataVersion(3));
    let already = fixture
        .database
        .execute_change_stream_reclamation(&proposal, generous_budget())
        .expect("old proposal sees manual GC");
    assert_eq!(
        already.outcome,
        AdaptiveChangeStreamGcOutcome::InconclusiveAlreadyReclaimed
    );

    fixture
        .database
        .disable_change_stream(TABLE_ID)
        .expect("disable old incarnation");
    fixture
        .database
        .enable_change_stream(TABLE_ID)
        .expect("enable new incarnation");
    let stale = fixture
        .database
        .execute_change_stream_reclamation(&proposal, generous_budget())
        .expect("reject old generation proposal");
    assert_eq!(
        stale.outcome,
        AdaptiveChangeStreamGcOutcome::Aborted(
            AdaptiveChangeStreamGcAbortReason::StreamGenerationChanged
        )
    );
    fixture.cleanup();
}

#[test]
fn older_pin_aborts_old_proposal_before_mutation_and_release_restores_safety() {
    let mut fixture = Fixture::new("older-pin");
    let proposal = fixture.proposal();
    let earliest_before = fixture
        .database
        .inspect_change_stream(TABLE_ID)
        .expect("inspect before pin")
        .earliest_available_frontier;
    let pin = fixture
        .database
        .pin_change_stream(
            TABLE_ID,
            ChangeStreamCursor {
                frontier: StorageDataVersion(1),
                ..fixture.origin
            },
        )
        .expect("pin older frontier");
    let blocked = fixture
        .database
        .execute_change_stream_reclamation(&proposal, generous_budget())
        .expect("revalidate old proposal");
    assert_eq!(
        blocked.outcome,
        AdaptiveChangeStreamGcOutcome::Aborted(
            AdaptiveChangeStreamGcAbortReason::PreconditionsChanged
        )
    );
    assert_eq!(
        fixture
            .database
            .inspect_change_stream(TABLE_ID)
            .expect("inspect non-mutation")
            .earliest_available_frontier,
        earliest_before
    );
    pin.release();
    let completed = fixture
        .database
        .execute_change_stream_reclamation(&proposal, generous_budget())
        .expect("execute after release");
    assert_eq!(completed.outcome, AdaptiveChangeStreamGcOutcome::Completed);
    fixture.cleanup();
}

#[test]
fn automatic_reclamation_is_opt_in_one_shot_and_starts_no_trial() {
    let mut fixture = Fixture::new("automatic");
    let pool = AdaptiveEvidencePool::default();
    let input = AutomaticMultiSafeModeInput {
        scope: AutomaticAdmissionScope {
            table_ids: &[TABLE_ID],
            calibration_classes: &[],
        },
        maintenance_budget: generous_budget(),
    };
    let disabled = fixture
        .database
        .automatic_safe_step_multi(&pool, input, AutomaticMultiSafeModePolicy::default())
        .expect("default safe step");
    assert_eq!(disabled.selected_candidate, None);
    assert_eq!(
        fixture.database.automatic_safe_mode_state().active_trial(),
        None
    );

    let policy = AutomaticMultiSafeModePolicy {
        allow_change_stream_gc: true,
        change_stream_gc_policy: AdaptiveChangeStreamGcPolicy::new(1, 0),
        ..AutomaticMultiSafeModePolicy::default()
    };
    let completed = fixture
        .database
        .automatic_safe_step_multi(&pool, input, policy)
        .expect("automatic GC step");
    assert_eq!(
        completed.action.selected_lane,
        AutomaticSafeModeLane::ChangeStreamReclamation
    );
    assert_eq!(
        completed.action.outcome,
        AutomaticSafeModeOutcome::ChangeStreamGcCompleted
    );
    assert!(matches!(
        completed.action.mutation,
        Some(AutomaticSafeModeMutation::ChangeStreamReclamation { .. })
    ));
    assert_eq!(
        fixture.database.automatic_safe_mode_state().active_trial(),
        None
    );
    assert_eq!(
        fixture
            .database
            .automatic_safe_mode_state()
            .cross_lane_service()
            .consecutive_columnar_admissions,
        0
    );
    assert_eq!(fixture.projection_id.0, 1);
    fixture.cleanup();
}
