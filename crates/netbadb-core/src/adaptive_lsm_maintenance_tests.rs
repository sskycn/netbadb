use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_types::{ColumnId, PhysicalType, ScalarValue, TableId};

use crate::{
    AdaptiveEvidencePool, AdaptiveLsmCompactionPolicy, AdaptiveLsmFlushPolicy,
    AdaptiveLsmMaintenanceAbortReason, AdaptiveLsmMaintenanceDecision,
    AdaptiveLsmMaintenanceNoActionReason, AdaptiveLsmMaintenanceOutcome,
    AdaptiveLsmMaintenanceProposal, AutomaticAdmissionScope, AutomaticCandidateKey,
    AutomaticCandidateReadiness, AutomaticEvidenceRenewalReason,
    AutomaticEvidenceRenewalRecommendation, AutomaticMultiSafeModeInput,
    AutomaticMultiSafeModePolicy, AutomaticSafeModeLane, AutomaticSafeModeMutation,
    AutomaticSafeModeOutcome, Database, DatabaseCoordinatorConfig, MaintenanceBlocker,
    MaintenanceBudget, TableStorageCreateSpec,
};

static NEXT_PATH: AtomicU64 = AtomicU64::new(1);
const EVENTS: TableId = TableId(1);
const OTHER: TableId = TableId(2);

struct Fixture {
    root: PathBuf,
    database: Database,
}

impl Fixture {
    fn create(name: &str, include_other: bool) -> Self {
        let suffix = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "netbadb-adaptive-lsm-{name}-{}-{suffix}",
            std::process::id()
        ));
        fs::create_dir_all(&root).expect("create fixture root");
        let mut storages = vec![TableStorageCreateSpec::lsm(
            root.join("events"),
            table(EVENTS, "events"),
            ColumnId(1),
        )];
        if include_other {
            storages.push(TableStorageCreateSpec::lsm(
                root.join("other"),
                table(OTHER, "other_events"),
                ColumnId(1),
            ));
        }
        let database = Database::create_catalog(
            root.join("catalog"),
            storages,
            Some(DatabaseCoordinatorConfig::new(root.join("coordinator")).with_global_visibility()),
        )
        .expect("create global LSM database");
        Self { root, database }
    }

    fn close(self) {
        self.database.close().expect("close fixture");
        let _ = fs::remove_dir_all(self.root);
    }

    fn create_mixed(name: &str) -> Self {
        let suffix = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "netbadb-adaptive-lsm-{name}-{}-{suffix}",
            std::process::id()
        ));
        fs::create_dir_all(&root).expect("create fixture root");
        let database = Database::create_catalog(
            root.join("catalog"),
            vec![
                TableStorageCreateSpec::lsm(
                    root.join("events"),
                    table(EVENTS, "events"),
                    ColumnId(1),
                ),
                TableStorageCreateSpec::heap(root.join("other"), table(OTHER, "other_events")),
            ],
            Some(DatabaseCoordinatorConfig::new(root.join("coordinator")).with_global_visibility()),
        )
        .expect("create mixed database");
        Self { root, database }
    }
}

fn table(id: TableId, name: &str) -> TableDef {
    TableDef::new(
        id,
        name,
        vec![
            ColumnDef::new(ColumnId(1), "id", TypeSpec::Physical(PhysicalType::Int64)),
            ColumnDef::new(
                ColumnId(2),
                "payload",
                TypeSpec::Physical(PhysicalType::Text),
            ),
        ],
    )
}

fn unlimited_budget() -> MaintenanceBudget {
    MaintenanceBudget::new(u64::MAX, u64::MAX, u64::MAX, 1)
}

fn automatic_input(tables: &[TableId]) -> AutomaticMultiSafeModeInput<'_> {
    AutomaticMultiSafeModeInput {
        scope: AutomaticAdmissionScope {
            table_ids: tables,
            calibration_classes: &[],
        },
        maintenance_budget: unlimited_budget(),
    }
}

fn lsm_policy(flush: bool, compact: bool) -> AutomaticMultiSafeModePolicy {
    AutomaticMultiSafeModePolicy {
        allow_lsm_flush: flush,
        allow_lsm_compaction: compact,
        lsm_flush_policy: AdaptiveLsmFlushPolicy::default(),
        lsm_compaction_policy: AdaptiveLsmCompactionPolicy::default(),
        ..AutomaticMultiSafeModePolicy::default()
    }
}

fn insert_run(database: &mut Database, table_id: TableId, start: i64, rows: i64, bytes: usize) {
    let payload = "x".repeat(bytes);
    let mut transaction = database.begin_transaction_for(table_id).expect("begin run");
    for id in start..start + rows {
        database
            .insert_into_in(
                table_id,
                &mut transaction,
                &[ScalarValue::Int64(id), ScalarValue::Text(payload.clone())],
            )
            .expect("insert run row");
    }
    transaction.commit().expect("commit run");
}

fn make_flush_ready(database: &mut Database, table_id: TableId) {
    insert_run(database, table_id, 0, 480, 9_000);
    let inspection = database
        .observe_adaptive_lsm_maintenance(table_id, unlimited_budget())
        .expect("inspect flush pressure")
        .remove(0);
    assert!(
        inspection.maintenance.memtable_bytes
            >= inspection.maintenance.memtable_flush_threshold_bytes
    );
}

fn manual_flush(database: &mut Database) {
    database.flush().expect("production maintenance flush");
}

fn make_compaction_ready(database: &mut Database) {
    insert_run(database, EVENTS, 0, 2, 8);
    manual_flush(database);
    insert_run(database, EVENTS, 10, 2, 8);
    manual_flush(database);
    let observation = database
        .observe_adaptive_lsm_maintenance(EVENTS, unlimited_budget())
        .expect("inspect compaction pressure")
        .remove(0);
    assert!(observation.maintenance.next_compaction.is_some());
    assert_eq!(observation.maintenance.memtable_entry_count, 0);
}

fn flush_proposal(database: &Database) -> crate::AdaptiveLsmMaintenanceProposal {
    let observation = database
        .observe_adaptive_lsm_maintenance(EVENTS, unlimited_budget())
        .expect("observe flush")
        .remove(0);
    match observation.decide_flush(AdaptiveLsmFlushPolicy::default(), unlimited_budget()) {
        AdaptiveLsmMaintenanceDecision::Proposal(proposal) => *proposal,
        decision => panic!("expected flush proposal, got {decision:?}"),
    }
}

fn compaction_proposal(database: &Database) -> crate::AdaptiveLsmMaintenanceProposal {
    let observation = database
        .observe_adaptive_lsm_maintenance(EVENTS, unlimited_budget())
        .expect("observe compaction")
        .remove(0);
    match observation.decide_compaction(AdaptiveLsmCompactionPolicy::default(), unlimited_budget())
    {
        AdaptiveLsmMaintenanceDecision::Proposal(proposal) => *proposal,
        decision => panic!("expected compaction proposal, got {decision:?}"),
    }
}

#[test]
fn automatic_lsm_is_default_disabled_and_flush_pressure_uses_production_threshold() {
    let mut fixture = Fixture::create("default-and-threshold", false);
    insert_run(&mut fixture.database, EVENTS, 0, 2, 8);
    let below = fixture
        .database
        .observe_adaptive_lsm_maintenance(EVENTS, unlimited_budget())
        .expect("below threshold")
        .remove(0);
    assert_eq!(
        below.decide_flush(AdaptiveLsmFlushPolicy::default(), unlimited_budget()),
        AdaptiveLsmMaintenanceDecision::NoAction(
            AdaptiveLsmMaintenanceNoActionReason::MemtableBelowAutomaticThreshold
        )
    );

    make_flush_ready(&mut fixture.database, EVENTS);
    let before = fixture
        .database
        .inspect_lsm_storage(EVENTS)
        .unwrap()
        .unwrap();
    let report = fixture
        .database
        .automatic_safe_step_multi(
            &AdaptiveEvidencePool::default(),
            automatic_input(&[EVENTS]),
            AutomaticMultiSafeModePolicy::default(),
        )
        .expect("default automatic step");
    assert_eq!(report.selected_candidate, None);
    assert_eq!(
        fixture
            .database
            .inspect_lsm_storage(EVENTS)
            .unwrap()
            .unwrap(),
        before
    );

    manual_flush(&mut fixture.database);
    insert_run(&mut fixture.database, EVENTS, 20_000, 2, 8);
    manual_flush(&mut fixture.database);
    let before = fixture
        .database
        .inspect_lsm_storage(EVENTS)
        .unwrap()
        .unwrap();
    assert!(
        fixture
            .database
            .observe_adaptive_lsm_maintenance(EVENTS, unlimited_budget())
            .unwrap()
            .remove(0)
            .maintenance
            .next_compaction
            .is_some()
    );
    let report = fixture
        .database
        .automatic_safe_step_multi(
            &AdaptiveEvidencePool::default(),
            automatic_input(&[EVENTS]),
            AutomaticMultiSafeModePolicy::default(),
        )
        .expect("default automatic compaction step");
    assert_eq!(report.selected_candidate, None);
    assert_eq!(
        fixture
            .database
            .inspect_lsm_storage(EVENTS)
            .unwrap()
            .unwrap(),
        before
    );
    fixture.close();
}

#[test]
fn flush_policy_can_only_raise_threshold_and_quiescence_is_shared() {
    let mut fixture = Fixture::create("flush-policy-quiescence", false);
    make_flush_ready(&mut fixture.database, EVENTS);
    let observation = fixture
        .database
        .observe_adaptive_lsm_maintenance(EVENTS, unlimited_budget())
        .unwrap()
        .remove(0);
    assert_eq!(
        observation.decide_flush(
            AdaptiveLsmFlushPolicy {
                minimum_memtable_bytes: observation.maintenance.memtable_bytes + 1,
            },
            unlimited_budget(),
        ),
        AdaptiveLsmMaintenanceDecision::NoAction(
            AdaptiveLsmMaintenanceNoActionReason::MemtableBelowAutomaticThreshold
        )
    );

    let mut transaction = fixture
        .database
        .begin_transaction()
        .expect("active transaction");
    let blocked = fixture
        .database
        .observe_adaptive_lsm_maintenance(EVENTS, unlimited_budget())
        .unwrap()
        .remove(0);
    assert!(matches!(
        blocked.decide_flush(AdaptiveLsmFlushPolicy::default(), unlimited_budget()),
        AdaptiveLsmMaintenanceDecision::NoAction(
            AdaptiveLsmMaintenanceNoActionReason::MaintenanceBlocked(MaintenanceBlocker::Busy)
        )
    ));
    transaction.rollback().expect("release transaction");
    drop(transaction);

    let storage_id = fixture.database.bindings.resolve_single(EVENTS).unwrap();
    let view = fixture
        .database
        .registry
        .get(storage_id)
        .unwrap()
        .read_view()
        .expect("retain read view");
    let blocked = fixture
        .database
        .observe_adaptive_lsm_maintenance(EVENTS, unlimited_budget())
        .unwrap()
        .remove(0);
    assert!(matches!(
        blocked.decide_flush(AdaptiveLsmFlushPolicy::default(), unlimited_budget()),
        AdaptiveLsmMaintenanceDecision::NoAction(
            AdaptiveLsmMaintenanceNoActionReason::MaintenanceBlocked(MaintenanceBlocker::Busy)
        )
    ));
    drop(view);
    fixture.close();
}

#[test]
fn automatic_flush_uses_production_writer_preserves_truth_and_has_no_trial() {
    let mut fixture = Fixture::create("automatic-flush", false);
    fixture.database.enable_change_stream(EVENTS).unwrap();
    make_flush_ready(&mut fixture.database, EVENTS);
    let report = fixture
        .database
        .automatic_safe_step_multi(
            &AdaptiveEvidencePool::default(),
            automatic_input(&[EVENTS]),
            lsm_policy(true, false),
        )
        .expect("automatic flush");
    assert_eq!(
        report.selected_candidate,
        Some(AutomaticCandidateKey::LsmFlush {
            table_id: EVENTS,
            storage_id: report
                .action
                .lsm_maintenance
                .as_ref()
                .expect("LSM report")
                .measurement
                .as_ref()
                .expect("measurement")
                .maintenance_before
                .anchor
                .storage_id,
        })
    );
    assert_eq!(
        report.action.selected_lane,
        AutomaticSafeModeLane::AuthoritativeMaintenance
    );
    assert_eq!(
        report.action.outcome,
        AutomaticSafeModeOutcome::LsmMaintenanceCompleted
    );
    assert!(matches!(
        report.action.mutation,
        Some(AutomaticSafeModeMutation::LsmMaintenance {
            action: crate::AdaptiveLsmMaintenanceAction::Flush,
            ..
        })
    ));
    assert_eq!(
        report.evidence_renewal_recommendation,
        Some(AutomaticEvidenceRenewalRecommendation {
            reason: AutomaticEvidenceRenewalReason::AuthoritativeLsmLayoutChanged,
        })
    );
    assert_eq!(report.action.trial_after, None);
    let execution = report.action.lsm_maintenance.expect("execution report");
    let measurement = execution.measurement.expect("physical measurement");
    assert_eq!(measurement.maintenance_after.memtable_entry_count, 0);
    assert_eq!(measurement.maintenance_after.memtable_bytes, 0);
    assert!(measurement.logical_rows_unchanged);
    assert_eq!(
        measurement.global_commit_seq_before,
        measurement.global_commit_seq_after
    );
    assert_eq!(
        measurement.schema_generation_before,
        measurement.schema_generation_after
    );
    assert_eq!(
        measurement.logical_data_version_before,
        measurement.logical_data_version_after
    );
    assert_eq!(
        measurement.change_stream_before,
        measurement.change_stream_after
    );
    assert!(execution.consumed.write_bytes <= execution.conservative_bound.write_bytes);
    fixture.close();
}

#[test]
fn four_lane_service_cannot_bypass_phase3e_staged_lsm_quiescence() {
    let mut fixture = Fixture::create("phase11-staged-lsm", false);
    make_compaction_ready(&mut fixture.database);
    let mut group = fixture.database.begin_group_commit().unwrap();
    let mut member = fixture.database.begin_group_member(&group).unwrap();
    fixture
        .database
        .insert_into_in(
            EVENTS,
            &mut member,
            &[
                ScalarValue::Int64(50_000),
                ScalarValue::Text("staged".into()),
            ],
        )
        .unwrap();
    fixture
        .database
        .stage_group_member(&mut group, member)
        .unwrap();

    let policy = AutomaticMultiSafeModePolicy {
        cross_lane_service: crate::AutomaticCrossLaneServicePolicy::BoundedFourLaneCycle,
        ..lsm_policy(false, true)
    };
    let before = fixture.database.automatic_safe_mode_state();
    let report = fixture
        .database
        .automatic_safe_step_multi(
            &AdaptiveEvidencePool::default(),
            automatic_input(&[EVENTS]),
            policy,
        )
        .expect("staged work is a typed blocker");
    assert_eq!(report.selected_candidate, None);
    assert!(
        report.candidates.iter().any(|candidate| {
            matches!(candidate.key, AutomaticCandidateKey::LsmCompaction { .. })
                && candidate.readiness
                    == AutomaticCandidateReadiness::LsmMaintenanceBlocked(
                        AdaptiveLsmMaintenanceNoActionReason::MaintenanceBlocked(
                            MaintenanceBlocker::Busy,
                        ),
                    )
        }),
        "unexpected staged LSM candidates: {:?}",
        report.candidates
    );
    assert_eq!(fixture.database.automatic_safe_mode_state(), before);
    fixture.database.abort_group(&mut group).unwrap();
    drop(group);
    fixture.close();
}

#[test]
fn stale_flush_proposals_abort_after_target_dml_or_manual_flush_but_not_unrelated_g() {
    let mut target = Fixture::create("stale-target", false);
    make_flush_ready(&mut target.database, EVENTS);
    let proposal = flush_proposal(&target.database);
    target
        .database
        .insert_into(
            EVENTS,
            &[ScalarValue::Int64(9_999), ScalarValue::Text("new".into())],
        )
        .unwrap();
    let stale = target
        .database
        .execute_adaptive_lsm_maintenance(&proposal, unlimited_budget())
        .unwrap();
    assert_eq!(
        stale.outcome,
        AdaptiveLsmMaintenanceOutcome::Aborted(
            AdaptiveLsmMaintenanceAbortReason::PreconditionsChanged
        )
    );
    assert_eq!(stale.consumed.actions, 0);
    target.close();

    let mut manual = Fixture::create("stale-manual", false);
    make_flush_ready(&mut manual.database, EVENTS);
    let proposal = flush_proposal(&manual.database);
    manual_flush(&mut manual.database);
    assert!(matches!(
        manual
            .database
            .execute_adaptive_lsm_maintenance(&proposal, unlimited_budget())
            .unwrap()
            .outcome,
        AdaptiveLsmMaintenanceOutcome::Aborted(_)
    ));
    manual.close();

    let mut unrelated = Fixture::create("unrelated-g", true);
    make_flush_ready(&mut unrelated.database, EVENTS);
    let proposal = flush_proposal(&unrelated.database);
    unrelated
        .database
        .insert_into(
            OTHER,
            &[ScalarValue::Int64(1), ScalarValue::Text("other".into())],
        )
        .unwrap();
    let completed = unrelated
        .database
        .execute_adaptive_lsm_maintenance(&proposal, unlimited_budget())
        .unwrap();
    assert_eq!(completed.outcome, AdaptiveLsmMaintenanceOutcome::Completed);
    unrelated.close();
}

#[test]
fn compaction_requires_empty_memtable_and_exact_plan_then_preserves_truth() {
    let mut fixture = Fixture::create("automatic-compaction", false);
    fixture.database.enable_change_stream(EVENTS).unwrap();
    make_compaction_ready(&mut fixture.database);
    insert_run(&mut fixture.database, EVENTS, 20, 1, 8);
    let blocked = fixture
        .database
        .observe_adaptive_lsm_maintenance(EVENTS, unlimited_budget())
        .unwrap()
        .remove(0);
    assert_eq!(
        blocked.decide_compaction(AdaptiveLsmCompactionPolicy::default(), unlimited_budget()),
        AdaptiveLsmMaintenanceDecision::NoAction(
            AdaptiveLsmMaintenanceNoActionReason::MemtableNotEmpty
        )
    );
    let manual = fixture
        .database
        .inspect_maintenance(unlimited_budget())
        .unwrap();
    assert!(manual.candidates.iter().any(|candidate| {
        matches!(
            candidate.action,
            crate::MaintenanceAction::CompactLsm { .. }
        ) && candidate.blocker == Some(MaintenanceBlocker::MemtableNotEmpty)
    }));
    manual_flush(&mut fixture.database);

    let observation = fixture
        .database
        .observe_adaptive_lsm_maintenance(EVENTS, unlimited_budget())
        .unwrap()
        .remove(0);
    let plan = observation
        .maintenance
        .next_compaction
        .as_ref()
        .expect("exact production compaction plan");
    assert_eq!(
        observation.decide_compaction(
            AdaptiveLsmCompactionPolicy {
                minimum_input_bytes: plan.input_bytes + 1,
            },
            unlimited_budget(),
        ),
        AdaptiveLsmMaintenanceDecision::NoAction(
            AdaptiveLsmMaintenanceNoActionReason::CompactionBelowAutomaticThreshold
        )
    );
    assert!(matches!(
        observation.decide_compaction(
            AdaptiveLsmCompactionPolicy::default(),
            MaintenanceBudget::new(
                u64::MAX,
                u64::MAX,
                plan.conservative_bound.write_bytes - 1,
                1,
            ),
        ),
        AdaptiveLsmMaintenanceDecision::NoAction(
            AdaptiveLsmMaintenanceNoActionReason::BudgetBlocked(
                MaintenanceBlocker::WriteBudgetExceeded
            )
        )
    ));

    let proposal = compaction_proposal(&fixture.database);
    let expected_plan = match &proposal {
        AdaptiveLsmMaintenanceProposal::CompactOne(proposal) => proposal.selected_plan.clone(),
        _ => panic!("expected compaction proposal"),
    };
    let report = fixture
        .database
        .execute_adaptive_lsm_maintenance(&proposal, unlimited_budget())
        .expect("execute compaction");
    assert_eq!(report.outcome, AdaptiveLsmMaintenanceOutcome::Completed);
    let measurement = report.measurement.expect("compaction measurement");
    assert!(measurement.logical_rows_unchanged);
    assert_eq!(
        measurement.change_stream_before,
        measurement.change_stream_after
    );
    assert_eq!(
        measurement.maintenance_before.anchor.wal_generation,
        measurement.maintenance_after.anchor.wal_generation
    );
    assert!(measurement.obsolete_bytes >= expected_plan.input_bytes);
    assert!(report.consumed.write_bytes <= report.conservative_bound.write_bytes);
    assert_eq!(
        fixture.database.automatic_safe_mode_state().active_trial(),
        None
    );
    fixture.close();
}

#[test]
fn manual_compaction_advances_layout_and_stales_exact_automatic_plan() {
    let mut fixture = Fixture::create("stale-manual-compaction", false);
    make_compaction_ready(&mut fixture.database);
    let proposal = compaction_proposal(&fixture.database);
    let manual = fixture
        .database
        .maintenance_step(unlimited_budget())
        .expect("manual compact_one");
    assert!(matches!(
        manual.decision.map(|decision| decision.action),
        Some(crate::MaintenanceAction::CompactLsm { .. })
    ));
    let stale = fixture
        .database
        .execute_adaptive_lsm_maintenance(&proposal, unlimited_budget())
        .expect("stale proposal report");
    assert_eq!(
        stale.outcome,
        AdaptiveLsmMaintenanceOutcome::Aborted(
            AdaptiveLsmMaintenanceAbortReason::PreconditionsChanged
        )
    );
    assert_eq!(stale.consumed.actions, 0);
    fixture.close();
}

#[test]
fn flush_then_compaction_requires_two_explicit_safe_steps() {
    let mut fixture = Fixture::create("two-steps", false);
    insert_run(&mut fixture.database, EVENTS, -10, 2, 8);
    manual_flush(&mut fixture.database);
    make_flush_ready(&mut fixture.database, EVENTS);
    let pool = AdaptiveEvidencePool::default();
    let policy = lsm_policy(true, true);
    let manual_cursor_before = fixture.database.maintenance_cursor;
    let service_before = fixture
        .database
        .automatic_safe_mode_state()
        .cross_lane_service();
    let first = fixture
        .database
        .automatic_safe_step_multi(&pool, automatic_input(&[EVENTS]), policy)
        .unwrap();
    assert!(matches!(
        first.selected_candidate,
        Some(AutomaticCandidateKey::LsmFlush { .. })
    ));
    assert_eq!(first.action.trial_after, None);
    assert_eq!(fixture.database.maintenance_cursor, manual_cursor_before);
    assert_eq!(
        fixture
            .database
            .automatic_safe_mode_state()
            .cross_lane_service(),
        service_before
    );
    let after_first = fixture
        .database
        .inspect_lsm_storage(EVENTS)
        .unwrap()
        .unwrap();
    assert_eq!(after_first.l0_count, 2, "same step must not compact");

    let second = fixture
        .database
        .automatic_safe_step_multi(&pool, automatic_input(&[EVENTS]), policy)
        .unwrap();
    assert!(matches!(
        second.selected_candidate,
        Some(AutomaticCandidateKey::LsmCompaction { .. })
    ));
    assert_eq!(
        second.action.outcome,
        AutomaticSafeModeOutcome::LsmMaintenanceCompleted
    );
    assert_eq!(
        second.evidence_renewal_recommendation,
        Some(AutomaticEvidenceRenewalRecommendation {
            reason: AutomaticEvidenceRenewalReason::AuthoritativeLsmLayoutChanged,
        })
    );
    fixture.close();
}

#[test]
fn heap_scope_is_ignored_while_each_lsm_storage_keeps_its_own_candidate_identity() {
    let mut fixture = Fixture::create_mixed("heap-ignored");
    make_flush_ready(&mut fixture.database, EVENTS);
    let report = fixture
        .database
        .inspect_automatic_candidates(
            &AdaptiveEvidencePool::default(),
            automatic_input(&[OTHER, EVENTS]),
            lsm_policy(true, true),
        )
        .expect("inspect mixed scope");
    assert!(!report.candidates.is_empty());
    assert!(
        report
            .candidates
            .iter()
            .all(|candidate| match candidate.key {
                AutomaticCandidateKey::LsmFlush { table_id, .. }
                | AutomaticCandidateKey::LsmCompaction { table_id, .. } => table_id == EVENTS,
                _ => false,
            })
    );
    fixture.close();
}

#[test]
fn automatic_candidate_inspection_is_read_only_and_exposes_typed_lsm_evidence() {
    let mut fixture = Fixture::create("inspection", false);
    make_flush_ready(&mut fixture.database, EVENTS);
    let pool = AdaptiveEvidencePool::default();
    let input = automatic_input(&[EVENTS]);
    let policy = lsm_policy(true, true);
    let first = fixture
        .database
        .inspect_automatic_candidates(&pool, input, policy)
        .unwrap();
    let second = fixture
        .database
        .inspect_automatic_candidates(&pool, input, policy)
        .unwrap();
    assert_eq!(first, second);
    let flush = first
        .candidates
        .iter()
        .find(|candidate| matches!(candidate.key, AutomaticCandidateKey::LsmFlush { .. }))
        .expect("flush candidate");
    assert_eq!(flush.readiness, AutomaticCandidateReadiness::Ready);
    assert!(flush.rank.lsm_memtable_bytes.unwrap() > 0);
    assert!(first.candidates.iter().any(|candidate| {
        matches!(candidate.key, AutomaticCandidateKey::LsmCompaction { .. })
            && candidate.readiness
                == AutomaticCandidateReadiness::LsmMaintenanceBlocked(
                    AdaptiveLsmMaintenanceNoActionReason::NoCompactionPlan,
                )
    }));
    fixture.close();
}
