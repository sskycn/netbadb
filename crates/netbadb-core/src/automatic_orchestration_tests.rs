use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use netbadb_planner::PlannerCalibrationClass;
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_types::{ColumnId, DatabaseCommitSeq, PhysicalType, ScalarValue, TableId};

use crate::adaptive_workload_tests::{TimelineFixture, set_target_work, workload_target};
use crate::automatic_orchestration::orchestration_stop_reason;
use crate::execution_feedback_tests::TABLE_ID;
use crate::planner_calibration_tests::permissive_policy;
use crate::{
    AdaptiveChangeStreamGcPolicy, AdaptiveEvidencePool, AdaptiveLsmCompactionPolicy,
    AdaptiveLsmFlushPolicy, AdaptivePolicy, AdaptiveWorkloadPolicy, AutomaticAdmissionScope,
    AutomaticCrossLaneServicePolicy, AutomaticCrossLaneServicePolicyKind,
    AutomaticEvidenceRenewalReason, AutomaticEvidenceRenewalRecommendation,
    AutomaticMultiSafeModePolicy, AutomaticOrchestrationEnvelope, AutomaticOrchestrationError,
    AutomaticOrchestrationInput, AutomaticOrchestrationInvalidEnvelope,
    AutomaticOrchestrationStepFailure, AutomaticOrchestrationStopReason, AutomaticProactiveLane,
    AutomaticSafeModeError, AutomaticSafeModeMutation, AutomaticSafeModeOutcome,
    AutomaticSafeModePolicy, AutomaticTrialAwaitingReason, ColumnarAdvanceBudget,
    ColumnarProjectionSpec, Database, DatabaseCoordinatorConfig, MAX_AUTOMATIC_ORCHESTRATION_STEPS,
    MaintenanceBudget, MaintenanceConsumption, TableStorageCreateSpec, cleanup_created_table_files,
};

static NEXT_PATH: AtomicU64 = AtomicU64::new(1);

fn budget(actions: u32) -> MaintenanceBudget {
    MaintenanceBudget::new(u64::MAX, u64::MAX, u64::MAX, actions)
}

fn envelope(
    max_steps: u32,
    per_step_actions: u32,
    run_actions: u32,
) -> AutomaticOrchestrationEnvelope {
    AutomaticOrchestrationEnvelope {
        max_steps,
        per_step_maintenance_budget: budget(per_step_actions),
        run_maintenance_budget: budget(run_actions),
    }
}

fn orchestration_input<'a>(
    table_ids: &'a [TableId],
    calibration_classes: &'a [PlannerCalibrationClass],
    envelope: AutomaticOrchestrationEnvelope,
) -> AutomaticOrchestrationInput<'a> {
    AutomaticOrchestrationInput {
        scope: AutomaticAdmissionScope {
            table_ids,
            calibration_classes,
        },
        envelope,
    }
}

fn columnar_policy() -> AutomaticMultiSafeModePolicy {
    AutomaticMultiSafeModePolicy {
        safe_mode: AutomaticSafeModePolicy {
            allow_columnar_maintenance: true,
            adaptive_policy: AdaptivePolicy::new(0, 0),
            workload_policy: AdaptiveWorkloadPolicy::new(3, 1, 3, 1, 0),
            ..AutomaticSafeModePolicy::default()
        },
        ..AutomaticMultiSafeModePolicy::default()
    }
}

#[test]
fn invalid_envelopes_are_typed_and_idle_run_retains_the_terminal_step() {
    let mut fixture = TimelineFixture::create("phase12-envelope-idle");
    let pool = AdaptiveEvidencePool::default();
    let state_before = fixture.database.automatic_safe_mode_state();
    let pool_before = pool.inspection();

    for (max_steps, expected) in [
        (0, AutomaticOrchestrationInvalidEnvelope::ZeroSteps),
        (
            MAX_AUTOMATIC_ORCHESTRATION_STEPS + 1,
            AutomaticOrchestrationInvalidEnvelope::StepLimitExceeded {
                requested: MAX_AUTOMATIC_ORCHESTRATION_STEPS + 1,
                maximum: MAX_AUTOMATIC_ORCHESTRATION_STEPS,
            },
        ),
    ] {
        let error = fixture
            .database
            .run_automatic_safe_orchestration(
                &pool,
                orchestration_input(&[], &[], envelope(max_steps, 1, 1)),
                AutomaticMultiSafeModePolicy::default(),
            )
            .expect_err("invalid envelope");
        assert!(matches!(
            error,
            AutomaticOrchestrationError::InvalidEnvelope(reason) if reason == expected
        ));
    }
    assert_eq!(fixture.database.automatic_safe_mode_state(), state_before);
    assert_eq!(pool.inspection(), pool_before);

    let idle = fixture
        .database
        .run_automatic_safe_orchestration(
            &pool,
            orchestration_input(&[], &[], envelope(4, 1, 4)),
            AutomaticMultiSafeModePolicy::default(),
        )
        .expect("idle run");
    assert_eq!(idle.steps.len(), 1);
    assert_eq!(
        idle.stop_reason,
        AutomaticOrchestrationStopReason::NoReadyWork
    );
    assert_eq!(idle.maintenance_consumed, MaintenanceConsumption::default());
    assert_eq!(idle.steps[0].step_index, 0);
    assert_eq!(idle.steps[0].maintenance_budget_before, budget(4));
    assert_eq!(idle.steps[0].maintenance_budget_granted, budget(1));
    assert_eq!(idle.steps[0].maintenance_budget_after, Some(budget(4)));

    let mut selected_without_mutation = idle.steps[0].report.clone();
    selected_without_mutation.selected_candidate = Some(crate::AutomaticCandidateKey::Columnar {
        table_id: TABLE_ID,
        projection_id: None,
    });
    assert_eq!(
        orchestration_stop_reason(
            &selected_without_mutation,
            false,
            budget(1),
            budget(4),
            MaintenanceConsumption::default(),
        ),
        Some(AutomaticOrchestrationStopReason::SelectedCandidateDidNotProgress)
    );
    fixture.close();
}

struct MultiGcFixture {
    root: PathBuf,
    heap_paths: Vec<PathBuf>,
    table_ids: Vec<TableId>,
    database: Database,
}

impl MultiGcFixture {
    fn create(name: &str, table_count: u64) -> Self {
        let suffix = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "netbadb-automatic-orchestration-{name}-{}-{suffix}",
            std::process::id()
        ));
        fs::create_dir_all(&root).expect("create orchestration fixture");
        let table_ids = (0..table_count)
            .map(|offset| TableId(12_000 + offset))
            .collect::<Vec<_>>();
        let heap_paths = table_ids
            .iter()
            .map(|table_id| root.join(format!("heap-{}", table_id.0)))
            .collect::<Vec<_>>();
        let storages = table_ids
            .iter()
            .zip(&heap_paths)
            .map(|(table_id, path)| {
                TableStorageCreateSpec::heap(
                    path,
                    TableDef::new(
                        *table_id,
                        format!("events_{}", table_id.0),
                        vec![
                            ColumnDef::new(
                                ColumnId(1),
                                "id",
                                TypeSpec::Physical(PhysicalType::Int64),
                            ),
                            ColumnDef::new(
                                ColumnId(2),
                                "value",
                                TypeSpec::Physical(PhysicalType::Int64),
                            ),
                        ],
                    ),
                )
            })
            .collect::<Vec<_>>();
        let mut database = Database::create_catalog(
            root.join("catalog"),
            storages,
            Some(DatabaseCoordinatorConfig::new(root.join("coordinator")).with_global_visibility()),
        )
        .expect("create orchestration database");
        for table_id in &table_ids {
            database
                .enable_change_stream(*table_id)
                .expect("enable stream");
            let projection_id = database
                .build_incremental_columnar_projection(ColumnarProjectionSpec::new(
                    *table_id,
                    root.join(format!("projection-{}", table_id.0)),
                    vec![ColumnId(1), ColumnId(2)],
                ))
                .expect("build retention authority");
            for value in 0..3 {
                database
                    .insert_into(
                        *table_id,
                        &[ScalarValue::Int64(value), ScalarValue::Int64(value * 10)],
                    )
                    .expect("insert stream row");
            }
            let advance = database
                .advance_columnar_projection(projection_id, ColumnarAdvanceBudget::new(2, u64::MAX))
                .expect("advance retention authority");
            assert_eq!(advance.batches_applied, 2);
        }
        Self {
            root,
            heap_paths,
            table_ids,
            database,
        }
    }

    fn close(self) {
        self.database.close().expect("close orchestration database");
        cleanup_created_table_files(&self.heap_paths);
        let _ = fs::remove_dir_all(self.root);
    }
}

fn gc_policy() -> AutomaticMultiSafeModePolicy {
    AutomaticMultiSafeModePolicy {
        allow_change_stream_gc: true,
        change_stream_gc_policy: AdaptiveChangeStreamGcPolicy::new(1, 0),
        cross_lane_service: AutomaticCrossLaneServicePolicy::BoundedFourLaneCycle,
        max_candidate_tables: 8,
        max_fairness_entries: 16,
        ..AutomaticMultiSafeModePolicy::default()
    }
}

#[test]
fn multiple_gc_steps_reuse_four_lane_service_and_account_run_budget() {
    let mut fixture = MultiGcFixture::create("multi-gc", 3);
    let pool = AdaptiveEvidencePool::default();
    let pool_before = pool.inspection();
    let report = fixture
        .database
        .run_automatic_safe_orchestration(
            &pool,
            orchestration_input(&fixture.table_ids, &[], envelope(8, 1, 8)),
            gc_policy(),
        )
        .expect("run three reclamations");
    assert_eq!(report.steps.len(), 4);
    assert_eq!(
        report.stop_reason,
        AutomaticOrchestrationStopReason::NoReadyWork
    );
    assert_eq!(report.maintenance_consumed.actions, 3);
    assert_eq!(
        report
            .maintenance_budget_remaining
            .expect("remaining")
            .max_actions,
        5
    );
    let mutations = report
        .steps
        .iter()
        .filter(|step| step.report.action.mutation.is_some())
        .count();
    assert_eq!(mutations, 3);
    assert!(mutations <= report.steps.len());
    for (index, step) in report.steps.iter().enumerate() {
        assert_eq!(
            step.step_index,
            u32::try_from(index).expect("bounded index")
        );
        assert!(step.maintenance_consumed.actions <= 1);
        if index < 3 {
            assert!(matches!(
                step.report.action.mutation,
                Some(AutomaticSafeModeMutation::ChangeStreamReclamation { .. })
            ));
            assert_eq!(step.maintenance_consumed.actions, 1);
            assert_eq!(
                step.report.cross_lane_service_after.active_policy,
                AutomaticCrossLaneServicePolicyKind::BoundedFourLaneCycle
            );
            assert_eq!(
                step.report
                    .cross_lane_service_after
                    .bounded_four_lane_cycle
                    .next_lane,
                AutomaticProactiveLane::AuthoritativeMaintenance
            );
            if index != 0 {
                assert_eq!(
                    step.report.cross_lane_service_before,
                    report.steps[index - 1].report.cross_lane_service_after
                );
            }
            assert_eq!(
                step.maintenance_budget_after
                    .expect("remaining")
                    .max_actions,
                7 - u32::try_from(index).expect("bounded index")
            );
        }
    }
    assert_eq!(pool.inspection(), pool_before);
    fixture.close();
}

#[test]
fn step_and_run_limits_are_independent_and_never_replenish_run_budget() {
    let mut per_step = MultiGcFixture::create("per-step-cap", 1);
    let blocked = per_step
        .database
        .run_automatic_safe_orchestration(
            &AdaptiveEvidencePool::default(),
            orchestration_input(&per_step.table_ids, &[], envelope(4, 0, 4)),
            gc_policy(),
        )
        .expect("per-step cap blocks GC");
    assert_eq!(blocked.steps.len(), 1);
    assert_eq!(
        blocked.stop_reason,
        AutomaticOrchestrationStopReason::NoReadyWork
    );
    assert_eq!(blocked.maintenance_consumed.actions, 0);
    per_step.close();

    let mut run_wide = MultiGcFixture::create("run-cap", 3);
    let bounded = run_wide
        .database
        .run_automatic_safe_orchestration(
            &AdaptiveEvidencePool::default(),
            orchestration_input(&run_wide.table_ids, &[], envelope(8, 8, 2)),
            gc_policy(),
        )
        .expect("run-wide cap");
    assert_eq!(bounded.steps.len(), 3);
    assert_eq!(
        bounded.stop_reason,
        AutomaticOrchestrationStopReason::NoReadyWork
    );
    assert_eq!(bounded.maintenance_consumed.actions, 2);
    assert_eq!(bounded.steps[2].maintenance_budget_granted.max_actions, 0);
    assert_eq!(
        bounded
            .maintenance_budget_remaining
            .expect("remaining")
            .max_actions,
        0
    );
    run_wide.close();

    let mut limited = MultiGcFixture::create("step-limit", 3);
    let limited_report = limited
        .database
        .run_automatic_safe_orchestration(
            &AdaptiveEvidencePool::default(),
            orchestration_input(&limited.table_ids, &[], envelope(2, 2, 3)),
            gc_policy(),
        )
        .expect("step limit");
    assert_eq!(limited_report.steps.len(), 2);
    assert_eq!(
        limited_report.stop_reason,
        AutomaticOrchestrationStopReason::StepLimitReached
    );
    assert_eq!(limited_report.maintenance_consumed.actions, 2);
    limited.close();
}

#[test]
fn columnar_renewal_and_trial_lifecycle_are_hard_run_boundaries() {
    let mut fixture = TimelineFixture::create("phase12-columnar-boundaries");
    fixture
        .database
        .execute("UPDATE events SET category = 7 WHERE id = 7")
        .expect("create Columnar lag");
    let pool = AdaptiveEvidencePool::default();
    let pool_before = pool.inspection();
    let tables = [TABLE_ID];
    let first = fixture
        .database
        .run_automatic_safe_orchestration(
            &pool,
            orchestration_input(&tables, &[], envelope(8, 1, 8)),
            columnar_policy(),
        )
        .expect("Columnar run");
    let recommendation = AutomaticEvidenceRenewalRecommendation {
        reason: AutomaticEvidenceRenewalReason::ColumnarPhysicalStateChanged,
    };
    assert_eq!(first.steps.len(), 1);
    assert_eq!(
        first.stop_reason,
        AutomaticOrchestrationStopReason::EvidenceRenewalRecommended(recommendation)
    );
    assert!(first.steps[0].report.action.trial_after.is_some());
    assert_eq!(pool.inspection(), pool_before);
    assert_eq!(
        first.steps[0].report.evidence_window_epoch,
        pool.window_epoch()
    );
    assert!(matches!(
        orchestration_stop_reason(
            &first.steps[0].report,
            true,
            budget(1),
            budget(8),
            first.steps[0].maintenance_consumed,
        ),
        Some(AutomaticOrchestrationStopReason::MaintenanceEnvelopeExceeded { .. })
    ));

    let waiting = fixture
        .database
        .run_automatic_safe_orchestration(
            &pool,
            orchestration_input(&tables, &[], envelope(8, 1, 8)),
            columnar_policy(),
        )
        .expect("trial wait run");
    assert_eq!(waiting.steps.len(), 1);
    assert!(matches!(
        waiting.stop_reason,
        AutomaticOrchestrationStopReason::ActiveTrial(_)
    ));
    assert_eq!(
        waiting.steps[0].report.action.outcome,
        AutomaticSafeModeOutcome::ColumnarTrialAwaiting(
            AutomaticTrialAwaitingReason::MissingWorkloadWindow
        )
    );

    let projection_id = fixture.database.inspect_columnar_projections()[0]
        .projection_id
        .expect("projection id");
    fixture
        .database
        .compact_columnar_projection(projection_id)
        .expect("manually stale trial target");
    let resolved = fixture
        .database
        .run_automatic_safe_orchestration(
            &pool,
            orchestration_input(&tables, &[], envelope(8, 1, 8)),
            columnar_policy(),
        )
        .expect("resolve stale trial");
    assert_eq!(resolved.steps.len(), 1);
    assert_eq!(
        resolved.stop_reason,
        AutomaticOrchestrationStopReason::TrialBoundaryResolved
    );
    fixture.close();
}

fn record_columnar_trial_evidence(
    fixture: &mut TimelineFixture,
    target: crate::AdaptiveWorkloadTarget,
    actual: u64,
    source: u64,
) -> AdaptiveEvidencePool {
    let mut pool = AdaptiveEvidencePool::default();
    for (offset, sql) in [
        "SELECT id FROM events",
        "SELECT id FROM events LIMIT 10",
        "SELECT id FROM events WHERE category = 1",
    ]
    .iter()
    .enumerate()
    {
        let (_, mut feedback) = fixture
            .database
            .query_with_feedback(sql)
            .expect("trial feedback");
        feedback.anchor.global_commit_seq = Some(DatabaseCommitSeq(
            30_000 + u64::try_from(offset).expect("bounded offset"),
        ));
        set_target_work(&mut feedback, target, 10, actual, source);
        pool.record_execution_feedback(&feedback)
            .expect("record trial evidence");
    }
    pool
}

#[test]
fn resolved_trial_stops_before_proactive_work_and_suppression_renewal_has_precedence() {
    let tables = [TABLE_ID];
    let empty = AdaptiveEvidencePool::default();

    let mut kept = TimelineFixture::create("phase12-trial-keep");
    kept.database
        .execute("UPDATE events SET category = 3 WHERE id = 3")
        .expect("create kept trial lag");
    kept.database
        .run_automatic_safe_orchestration(
            &empty,
            orchestration_input(&tables, &[], envelope(4, 1, 4)),
            columnar_policy(),
        )
        .expect("start kept trial");
    let Some(crate::AutomaticSafeTrial::Columnar(trial)) =
        kept.database.automatic_safe_mode_state().active_trial()
    else {
        panic!("expected Columnar trial")
    };
    let keep_pool = record_columnar_trial_evidence(&mut kept, trial.target(), 5, 20);
    let keep = kept
        .database
        .run_automatic_safe_orchestration(
            &keep_pool,
            orchestration_input(&tables, &[], envelope(4, 1, 4)),
            columnar_policy(),
        )
        .expect("resolve keep trial");
    assert_eq!(keep.steps.len(), 1);
    assert_eq!(
        keep.stop_reason,
        AutomaticOrchestrationStopReason::TrialBoundaryResolved
    );
    assert_eq!(
        keep.steps[0].report.action.outcome,
        AutomaticSafeModeOutcome::ColumnarTrialValidatedKeep
    );
    kept.close();

    let mut reverted = TimelineFixture::create("phase12-trial-suppression");
    reverted
        .database
        .execute("UPDATE events SET category = 4 WHERE id = 4")
        .expect("create regressing trial lag");
    reverted
        .database
        .run_automatic_safe_orchestration(
            &empty,
            orchestration_input(&tables, &[], envelope(4, 1, 4)),
            columnar_policy(),
        )
        .expect("start regressing trial");
    let Some(crate::AutomaticSafeTrial::Columnar(trial)) =
        reverted.database.automatic_safe_mode_state().active_trial()
    else {
        panic!("expected Columnar trial")
    };
    let mut regression_pool = record_columnar_trial_evidence(&mut reverted, trial.target(), 30, 20);
    let epoch_before = regression_pool.window_epoch();
    let suppression = reverted
        .database
        .run_automatic_safe_orchestration(
            &regression_pool,
            orchestration_input(&tables, &[], envelope(4, 1, 4)),
            columnar_policy(),
        )
        .expect("resolve suppression trial");
    assert_eq!(suppression.steps.len(), 1);
    assert_eq!(
        suppression.stop_reason,
        AutomaticOrchestrationStopReason::EvidenceRenewalRecommended(
            AutomaticEvidenceRenewalRecommendation {
                reason: AutomaticEvidenceRenewalReason::ColumnarEligibilityChanged,
            }
        )
    );
    assert_eq!(
        suppression.steps[0].report.action.outcome,
        AutomaticSafeModeOutcome::ColumnarTrialReverted
    );
    assert_eq!(regression_pool.window_epoch(), epoch_before);
    regression_pool
        .rotate_window()
        .expect("caller explicitly rotates after boundary");
    assert_ne!(regression_pool.window_epoch(), epoch_before);
    reverted.close();
}

#[test]
fn columnar_compaction_stops_after_one_existing_safe_step() {
    let mut fixture = TimelineFixture::create("phase12-columnar-compaction");
    let projection_id = fixture.database.inspect_columnar_projections()[0]
        .projection_id
        .expect("projection id");
    fixture
        .database
        .execute("UPDATE events SET category = 5 WHERE id = 5")
        .expect("create Delta input");
    fixture
        .database
        .advance_columnar_projection(projection_id, ColumnarAdvanceBudget::new(8, u64::MAX))
        .expect("publish fresh Delta");
    let policy = AutomaticMultiSafeModePolicy {
        allow_columnar_compaction: true,
        safe_mode: AutomaticSafeModePolicy {
            adaptive_policy: AdaptivePolicy::new(0, 0),
            ..AutomaticSafeModePolicy::default()
        },
        ..AutomaticMultiSafeModePolicy::default()
    };
    let tables = [TABLE_ID];
    let report = fixture
        .database
        .run_automatic_safe_orchestration(
            &AdaptiveEvidencePool::default(),
            orchestration_input(&tables, &[], envelope(8, 1, 8)),
            policy,
        )
        .expect("compact one Columnar generation");
    assert_eq!(report.steps.len(), 1);
    assert!(matches!(
        report.stop_reason,
        AutomaticOrchestrationStopReason::EvidenceRenewalRecommended(
            AutomaticEvidenceRenewalRecommendation {
                reason: AutomaticEvidenceRenewalReason::ColumnarPhysicalStateChanged,
            }
        )
    ));
    assert!(matches!(
        report.steps[0].report.action.mutation,
        Some(AutomaticSafeModeMutation::ColumnarCompaction { .. })
    ));
    fixture.close();
}

#[test]
fn calibration_can_run_with_zero_physical_budget_and_stops_at_its_trial() {
    let mut fixture = TimelineFixture::create("phase12-zero-budget-calibration");
    let target = workload_target(&fixture.database);
    let mut pool = AdaptiveEvidencePool::default();
    for (offset, sql) in [
        "SELECT id FROM events",
        "SELECT id FROM events LIMIT 10",
        "SELECT id FROM events WHERE category = 1",
    ]
    .iter()
    .enumerate()
    {
        let (_, mut feedback) = fixture
            .database
            .query_with_feedback(sql)
            .expect("calibration feedback");
        feedback.anchor.global_commit_seq = Some(DatabaseCommitSeq(
            20_000 + u64::try_from(offset).expect("bounded offset"),
        ));
        set_target_work(&mut feedback, target, 10, 20, 30);
        pool.record_execution_feedback(&feedback)
            .expect("record calibration evidence");
    }
    let pool_before = pool.inspection();
    let policy = AutomaticMultiSafeModePolicy {
        safe_mode: AutomaticSafeModePolicy {
            allow_planner_calibration: true,
            planner_calibration_policy: permissive_policy(),
            ..AutomaticSafeModePolicy::default()
        },
        ..AutomaticMultiSafeModePolicy::default()
    };
    let classes = [PlannerCalibrationClass::Columnar];
    let zero = MaintenanceBudget::new(0, 0, 0, 0);
    let report = fixture
        .database
        .run_automatic_safe_orchestration(
            &pool,
            orchestration_input(
                &[],
                &classes,
                AutomaticOrchestrationEnvelope {
                    max_steps: 8,
                    per_step_maintenance_budget: zero,
                    run_maintenance_budget: zero,
                },
            ),
            policy,
        )
        .expect("zero-budget calibration");
    assert_eq!(report.steps.len(), 1);
    assert!(matches!(
        report.stop_reason,
        AutomaticOrchestrationStopReason::ActiveTrial(_)
    ));
    assert_eq!(
        report.maintenance_consumed,
        MaintenanceConsumption::default()
    );
    assert!(matches!(
        report.steps[0].report.action.mutation,
        Some(AutomaticSafeModeMutation::PlannerCalibrationApply { .. })
    ));
    assert_eq!(pool.inspection(), pool_before);
    fixture.close();
}

#[test]
fn lsm_flush_stops_on_layout_renewal_without_rotating_evidence() {
    const LSM_TABLE: TableId = TableId(13_000);
    let suffix = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
    let root = std::env::temp_dir().join(format!(
        "netbadb-automatic-orchestration-lsm-{}-{suffix}",
        std::process::id()
    ));
    fs::create_dir_all(&root).expect("create LSM root");
    let table = TableDef::new(
        LSM_TABLE,
        "lsm_events",
        vec![
            ColumnDef::new(ColumnId(1), "id", TypeSpec::Physical(PhysicalType::Int64)),
            ColumnDef::new(
                ColumnId(2),
                "payload",
                TypeSpec::Physical(PhysicalType::Text),
            ),
        ],
    );
    let mut database = Database::create_catalog(
        root.join("catalog"),
        vec![TableStorageCreateSpec::lsm(
            root.join("lsm"),
            table,
            ColumnId(1),
        )],
        Some(DatabaseCoordinatorConfig::new(root.join("coordinator")).with_global_visibility()),
    )
    .expect("create LSM database");
    let payload = "x".repeat(9_000);
    let mut transaction = database
        .begin_transaction_for(LSM_TABLE)
        .expect("begin LSM run");
    for id in 0..480 {
        database
            .insert_into_in(
                LSM_TABLE,
                &mut transaction,
                &[ScalarValue::Int64(id), ScalarValue::Text(payload.clone())],
            )
            .expect("insert LSM row");
    }
    transaction.commit().expect("commit LSM run");
    drop(transaction);
    let pool = AdaptiveEvidencePool::default();
    let pool_before = pool.inspection();
    let policy = AutomaticMultiSafeModePolicy {
        allow_lsm_flush: true,
        lsm_flush_policy: AdaptiveLsmFlushPolicy::default(),
        ..AutomaticMultiSafeModePolicy::default()
    };
    let tables = [LSM_TABLE];
    let report = database
        .run_automatic_safe_orchestration(
            &pool,
            orchestration_input(&tables, &[], envelope(8, 1, 8)),
            policy,
        )
        .expect("automatic LSM flush");
    assert_eq!(report.steps.len(), 1);
    assert_eq!(
        report.stop_reason,
        AutomaticOrchestrationStopReason::EvidenceRenewalRecommended(
            AutomaticEvidenceRenewalRecommendation {
                reason: AutomaticEvidenceRenewalReason::AuthoritativeLsmLayoutChanged,
            }
        ),
        "unexpected candidates: {:#?}",
        report.steps[0].report.candidates
    );
    assert_eq!(report.maintenance_consumed.actions, 1);
    assert_eq!(pool.inspection(), pool_before);
    database.close().expect("close LSM database");
    let _ = fs::remove_dir_all(root);
}

#[test]
fn lsm_compact_one_stops_on_layout_renewal() {
    const LSM_TABLE: TableId = TableId(13_001);
    let suffix = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
    let root = std::env::temp_dir().join(format!(
        "netbadb-automatic-orchestration-lsm-compact-{}-{suffix}",
        std::process::id()
    ));
    fs::create_dir_all(&root).expect("create LSM compaction root");
    let table = TableDef::new(
        LSM_TABLE,
        "lsm_compaction_events",
        vec![
            ColumnDef::new(ColumnId(1), "id", TypeSpec::Physical(PhysicalType::Int64)),
            ColumnDef::new(
                ColumnId(2),
                "payload",
                TypeSpec::Physical(PhysicalType::Text),
            ),
        ],
    );
    let mut database = Database::create_catalog(
        root.join("catalog"),
        vec![TableStorageCreateSpec::lsm(
            root.join("lsm"),
            table,
            ColumnId(1),
        )],
        Some(DatabaseCoordinatorConfig::new(root.join("coordinator")).with_global_visibility()),
    )
    .expect("create LSM compaction database");
    for base in [0_i64, 10] {
        for id in base..base + 2 {
            database
                .insert_into(
                    LSM_TABLE,
                    &[
                        ScalarValue::Int64(id),
                        ScalarValue::Text(format!("value-{id}")),
                    ],
                )
                .expect("insert compaction row");
        }
        database.flush().expect("create one L0 SSTable");
    }
    let policy = AutomaticMultiSafeModePolicy {
        allow_lsm_compaction: true,
        lsm_compaction_policy: AdaptiveLsmCompactionPolicy::default(),
        ..AutomaticMultiSafeModePolicy::default()
    };
    let tables = [LSM_TABLE];
    let report = database
        .run_automatic_safe_orchestration(
            &AdaptiveEvidencePool::default(),
            orchestration_input(&tables, &[], envelope(8, 1, 8)),
            policy,
        )
        .expect("automatic compact_one");
    assert_eq!(report.steps.len(), 1);
    assert_eq!(
        report.stop_reason,
        AutomaticOrchestrationStopReason::EvidenceRenewalRecommended(
            AutomaticEvidenceRenewalRecommendation {
                reason: AutomaticEvidenceRenewalReason::AuthoritativeLsmLayoutChanged,
            }
        )
    );
    assert!(matches!(
        report.steps[0].report.action.mutation,
        Some(AutomaticSafeModeMutation::LsmMaintenance {
            action: crate::AdaptiveLsmMaintenanceAction::CompactOne,
            ..
        })
    ));
    database.close().expect("close LSM compaction database");
    let _ = fs::remove_dir_all(root);
}

#[test]
fn phase3e_staged_work_remains_blocked_and_orchestration_does_not_loop_around_it() {
    let mut fixture = TimelineFixture::create("phase12-phase3e");
    fixture
        .database
        .execute("UPDATE events SET category = 7 WHERE id = 7")
        .expect("create Columnar lag");
    let mut group = fixture.database.begin_group_commit().expect("begin group");
    let mut member = fixture
        .database
        .begin_group_member(&group)
        .expect("begin member");
    fixture
        .database
        .insert_into_in(
            TABLE_ID,
            &mut member,
            &[
                ScalarValue::Int64(90_000),
                ScalarValue::Int64(1),
                ScalarValue::Text("staged".into()),
            ],
        )
        .expect("stage row");
    fixture
        .database
        .stage_group_member(&mut group, member)
        .expect("prepare member");
    let state_before = fixture.database.automatic_safe_mode_state();
    let tables = [TABLE_ID];
    let report = fixture
        .database
        .run_automatic_safe_orchestration(
            &AdaptiveEvidencePool::default(),
            orchestration_input(&tables, &[], envelope(8, 1, 8)),
            columnar_policy(),
        )
        .expect("staged work stays blocked");
    assert_eq!(report.steps.len(), 1);
    assert_eq!(
        report.stop_reason,
        AutomaticOrchestrationStopReason::NoReadyWork
    );
    assert_eq!(report.steps[0].report.action.mutation, None);
    assert_eq!(fixture.database.automatic_safe_mode_state(), state_before);
    fixture
        .database
        .abort_group(&mut group)
        .expect("abort group");
    drop(group);
    fixture.close();
}

#[test]
fn consumption_helpers_are_componentwise_and_checked() {
    let capped =
        MaintenanceBudget::new(10, 20, 30, 4).capped_by(MaintenanceBudget::new(5, 25, 10, 8));
    assert_eq!(capped, MaintenanceBudget::new(5, 20, 10, 4));
    assert_eq!(
        MaintenanceConsumption {
            work_units: 1,
            read_bytes: 2,
            write_bytes: 3,
            actions: 1,
        }
        .checked_add(MaintenanceConsumption {
            work_units: 4,
            read_bytes: 5,
            write_bytes: 6,
            actions: 2,
        }),
        Some(MaintenanceConsumption {
            work_units: 5,
            read_bytes: 7,
            write_bytes: 9,
            actions: 3,
        })
    );
    assert_eq!(
        MaintenanceConsumption {
            work_units: u64::MAX,
            ..MaintenanceConsumption::default()
        }
        .checked_add(MaintenanceConsumption {
            work_units: 1,
            ..MaintenanceConsumption::default()
        }),
        None
    );
    assert_eq!(
        MaintenanceConsumption {
            actions: u32::MAX,
            ..MaintenanceConsumption::default()
        }
        .checked_add(MaintenanceConsumption {
            actions: 1,
            ..MaintenanceConsumption::default()
        }),
        None
    );
}

#[test]
fn orchestration_errors_expose_the_completed_prefix_without_rollback_claims() {
    let mut fixture = TimelineFixture::create("phase12-error-prefix-shape");
    let idle = fixture
        .database
        .run_automatic_safe_orchestration(
            &AdaptiveEvidencePool::default(),
            orchestration_input(&[], &[], envelope(1, 1, 1)),
            AutomaticMultiSafeModePolicy::default(),
        )
        .expect("one completed report");
    let state = fixture.database.automatic_safe_mode_state();
    let error =
        AutomaticOrchestrationError::StepFailed(Box::new(AutomaticOrchestrationStepFailure {
            step_index: 1,
            completed_steps: idle.steps.clone(),
            maintenance_budget_before: budget(1),
            maintenance_consumed: MaintenanceConsumption::default(),
            maintenance_budget_remaining_before_failed_step: budget(1),
            safe_mode_state_before: state,
            safe_mode_state_after: state,
            source: AutomaticSafeModeError::AdmissionScopeTooLarge,
        }));
    assert_eq!(error.completed_steps(), idle.steps.as_slice());
    assert_eq!(error.completed_steps().len(), 1);
    fixture.close();
}
