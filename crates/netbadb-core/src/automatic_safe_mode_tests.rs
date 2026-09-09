use netbadb_planner::{CalibrationRatio, PlannerCalibrationClass, PlannerCalibrationEpoch};
use netbadb_types::DatabaseCommitSeq;

use crate::adaptive_workload_tests::{TimelineFixture, cleanup, set_target_work, workload_target};
use crate::execution_feedback_tests::Fixture;
use crate::planner_calibration_tests::{permissive_policy, systematic_window};
use crate::{
    AdaptiveDecision, AdaptivePolicy, AdaptiveWorkloadLimits, AdaptiveWorkloadPolicy,
    AdaptiveWorkloadWindow, AutomaticCalibrationTrialPolicy, AutomaticCalibrationTrialStaleReason,
    AutomaticSafeModeInput, AutomaticSafeModeLane, AutomaticSafeModeMutation,
    AutomaticSafeModeNoAction, AutomaticSafeModeOutcome, AutomaticSafeModePolicy,
    AutomaticSafeTrial, AutomaticTrialAwaitingReason, Database, MaintenanceBudget,
    PlannerCalibrationDecision, PlannerCalibrationPolicy, PlannerCalibrationShadowDecision,
};

fn generous_budget() -> MaintenanceBudget {
    MaintenanceBudget::new(u64::MAX, u64::MAX, u64::MAX, 1)
}

fn automatic_policy() -> AutomaticSafeModePolicy {
    AutomaticSafeModePolicy {
        allow_columnar_maintenance: true,
        allow_planner_calibration: true,
        adaptive_policy: AdaptivePolicy::new(0, 0),
        workload_policy: AdaptiveWorkloadPolicy::new(3, 1, 3, 1, 0),
        planner_calibration_policy: permissive_policy(),
        calibration_trial_policy: AutomaticCalibrationTrialPolicy {
            minimum_samples: 3,
            minimum_actual_work_units: 1,
            minimum_distinct_visibility_points: 3,
            minimum_distinct_query_shapes: 3,
            minimum_keep_error_improvement_work_units: 1,
            maximum_tolerated_error_regression_work_units: 0,
        },
    }
}

fn make_columnar_lag(fixture: &mut TimelineFixture, id: i64) {
    fixture
        .database
        .execute(&format!("UPDATE events SET category = 7 WHERE id = {id}"))
        .expect("create projection lag");
}

fn start_columnar_trial(fixture: &mut TimelineFixture) -> crate::AdaptiveWorkloadTarget {
    make_columnar_lag(fixture, 7);
    let table_id = workload_target(&fixture.database).table_id;
    let report = fixture
        .database
        .automatic_safe_step(
            AutomaticSafeModeInput {
                columnar_table_id: Some(table_id),
                workload_window: None,
                calibration_class: None,
                maintenance_budget: generous_budget(),
            },
            automatic_policy(),
        )
        .expect("automatic Columnar step");
    assert_eq!(
        report.selected_lane,
        AutomaticSafeModeLane::ColumnarMaintenance
    );
    assert!(matches!(
        report.mutation,
        Some(AutomaticSafeModeMutation::ColumnarAdvance { .. })
    ));
    assert!(
        report
            .columnar_cycle
            .as_ref()
            .is_some_and(|cycle| matches!(cycle.decision, AdaptiveDecision::Proposal(_)))
    );
    let Some(AutomaticSafeTrial::Columnar(trial)) = report.trial_after else {
        panic!("kept Columnar mutation must start probation")
    };
    trial.target()
}

fn target_window(
    database: &mut Database,
    target: crate::AdaptiveWorkloadTarget,
    actual: u64,
    source: u64,
) -> AdaptiveWorkloadWindow {
    let mut window = AdaptiveWorkloadWindow::new(target, AdaptiveWorkloadLimits::default());
    for (index, sql) in [
        "SELECT id FROM events",
        "SELECT id FROM events LIMIT 10",
        "SELECT id FROM events WHERE category = 1",
    ]
    .iter()
    .enumerate()
    {
        let (_, mut report) = database.query_with_feedback(sql).expect("target feedback");
        report.anchor.global_commit_seq = Some(DatabaseCommitSeq(
            100 + u64::try_from(index).expect("visibility index"),
        ));
        set_target_work(&mut report, target, 10, actual, source);
        window.record(&report).expect("record target workload");
    }
    window
}

#[test]
fn idle_step_is_explicit_deterministic_and_has_no_background_state() {
    let mut fixture = TimelineFixture::create("phase5-idle");
    let before_profile = fixture.database.planner_calibration_profile();
    let before_projection = fixture.database.inspect_columnar_projections();
    let report = fixture
        .database
        .automatic_safe_step(
            AutomaticSafeModeInput::new(generous_budget()),
            AutomaticSafeModePolicy::default(),
        )
        .expect("idle step");
    assert_eq!(
        report.outcome,
        AutomaticSafeModeOutcome::NoAction(AutomaticSafeModeNoAction::AutomaticActionsDisabled)
    );
    assert_eq!(report.mutation, None);
    assert_eq!(
        fixture.database.automatic_safe_mode_state().active_trial(),
        None
    );
    assert_eq!(
        fixture.database.planner_calibration_profile(),
        before_profile
    );
    assert_eq!(
        fixture.database.inspect_columnar_projections(),
        before_projection
    );
    fixture
        .database
        .query("SELECT id FROM events LIMIT 1")
        .expect("ordinary query");
    assert_eq!(
        fixture.database.automatic_safe_mode_state().active_trial(),
        None
    );
    fixture.close();
}

#[test]
fn columnar_trial_has_priority_and_cross_g_keep_clears_probation() {
    let mut fixture = TimelineFixture::create("phase5-columnar-keep");
    let target = start_columnar_trial(&mut fixture);
    let awaiting = fixture
        .database
        .automatic_safe_step(
            AutomaticSafeModeInput {
                columnar_table_id: Some(target.table_id),
                workload_window: None,
                calibration_class: Some(PlannerCalibrationClass::Columnar),
                maintenance_budget: generous_budget(),
            },
            automatic_policy(),
        )
        .expect("await target evidence");
    assert_eq!(
        awaiting.outcome,
        AutomaticSafeModeOutcome::ColumnarTrialAwaiting(
            AutomaticTrialAwaitingReason::MissingWorkloadWindow
        )
    );
    assert_eq!(
        awaiting.selected_lane,
        AutomaticSafeModeLane::ActiveColumnarTrial
    );
    assert_eq!(awaiting.mutation, None);

    fixture
        .database
        .execute("INSERT INTO clock_rows (id) VALUES (1)")
        .expect("advance G outside the target table");
    let window = target_window(&mut fixture.database, target, 100, 1_000);
    assert_eq!(window.distinct_visibility_points, 3);
    let kept = fixture
        .database
        .automatic_safe_step(
            AutomaticSafeModeInput {
                columnar_table_id: Some(target.table_id),
                workload_window: Some(&window),
                calibration_class: Some(PlannerCalibrationClass::Columnar),
                maintenance_budget: generous_budget(),
            },
            automatic_policy(),
        )
        .expect("validate Columnar keep");
    assert_eq!(
        kept.outcome,
        AutomaticSafeModeOutcome::ColumnarTrialValidatedKeep
    );
    assert_eq!(kept.mutation, None);
    assert_eq!(kept.trial_after, None);
    fixture.close();
}

#[test]
fn columnar_regression_suppresses_exact_generation_and_manual_change_is_stale() {
    let mut fixture = TimelineFixture::create("phase5-columnar-revert");
    let target = start_columnar_trial(&mut fixture);
    let window = target_window(&mut fixture.database, target, 1_300, 1_000);
    let global_before = fixture
        .database
        .current_database_snapshot()
        .unwrap()
        .unwrap()
        .commit_seq();
    let reverted = fixture
        .database
        .automatic_safe_step(
            AutomaticSafeModeInput {
                columnar_table_id: None,
                workload_window: Some(&window),
                calibration_class: None,
                maintenance_budget: generous_budget(),
            },
            automatic_policy(),
        )
        .expect("revert target eligibility");
    assert_eq!(
        reverted.outcome,
        AutomaticSafeModeOutcome::ColumnarTrialReverted
    );
    assert_eq!(
        reverted.mutation,
        Some(AutomaticSafeModeMutation::ColumnarSuppression { target })
    );
    assert_eq!(
        fixture
            .database
            .current_database_snapshot()
            .unwrap()
            .unwrap()
            .commit_seq(),
        global_before
    );
    assert_eq!(reverted.trial_after, None);

    fixture
        .database
        .compact_columnar_projection(target.projection_id)
        .expect("publish a new unsuppressed generation");
    let newer = start_columnar_trial(&mut fixture);
    fixture
        .database
        .compact_columnar_projection(newer.projection_id)
        .expect("manual projection generation change");
    let stale = fixture
        .database
        .automatic_safe_step(
            AutomaticSafeModeInput::new(generous_budget()),
            automatic_policy(),
        )
        .expect("detect stale target trial");
    assert!(matches!(
        stale.outcome,
        AutomaticSafeModeOutcome::ColumnarTrialStale(_)
    ));
    assert_eq!(stale.mutation, None);
    assert_eq!(stale.trial_after, None);
    fixture.close();
}

#[test]
fn automatic_calibration_apply_firewall_and_new_epoch_keep_are_receipt_backed() {
    let mut fixture = TimelineFixture::create("phase5-calibration-keep");
    let target = workload_target(&fixture.database);
    let proposal_window = systematic_window(
        &mut fixture.database,
        target,
        PlannerCalibrationEpoch(0),
        1,
        1,
        100,
    );
    let validation_window = systematic_window(
        &mut fixture.database,
        target,
        PlannerCalibrationEpoch(1),
        1,
        100,
        100,
    );
    let mut policy = automatic_policy();
    policy.allow_columnar_maintenance = false;
    let global_before_apply = fixture
        .database
        .current_database_snapshot()
        .unwrap()
        .unwrap()
        .commit_seq();
    let applied = fixture
        .database
        .automatic_safe_step(
            AutomaticSafeModeInput {
                columnar_table_id: None,
                workload_window: Some(&proposal_window),
                calibration_class: Some(PlannerCalibrationClass::Columnar),
                maintenance_budget: generous_budget(),
            },
            policy,
        )
        .expect("apply calibration automatically");
    assert_eq!(
        applied.outcome,
        AutomaticSafeModeOutcome::PlannerCalibrationApplied
    );
    assert!(matches!(
        applied.mutation,
        Some(AutomaticSafeModeMutation::PlannerCalibrationApply {
            applied_epoch: PlannerCalibrationEpoch(1),
            ..
        })
    ));
    assert!(matches!(
        applied.trial_after,
        Some(AutomaticSafeTrial::PlannerCalibration(_))
    ));
    assert_eq!(
        fixture
            .database
            .current_database_snapshot()
            .unwrap()
            .unwrap()
            .commit_seq(),
        global_before_apply
    );

    fixture
        .database
        .execute("INSERT INTO clock_rows (id) VALUES (2)")
        .expect("ordinary G advance during calibration probation");
    let firewall = fixture
        .database
        .automatic_safe_step(
            AutomaticSafeModeInput {
                columnar_table_id: Some(target.table_id),
                workload_window: None,
                calibration_class: Some(PlannerCalibrationClass::Columnar),
                maintenance_budget: generous_budget(),
            },
            automatic_policy(),
        )
        .expect("trial firewall");
    assert_eq!(
        firewall.selected_lane,
        AutomaticSafeModeLane::ActivePlannerCalibrationTrial
    );
    assert_eq!(firewall.mutation, None);

    let kept = fixture
        .database
        .automatic_safe_step(
            AutomaticSafeModeInput {
                columnar_table_id: Some(target.table_id),
                workload_window: Some(&validation_window),
                calibration_class: Some(PlannerCalibrationClass::Columnar),
                maintenance_budget: generous_budget(),
            },
            automatic_policy(),
        )
        .expect("keep applied calibration");
    assert_eq!(
        kept.outcome,
        AutomaticSafeModeOutcome::PlannerCalibrationTrialValidatedKeep
    );
    assert_eq!(kept.trial_after, None);
    assert_eq!(
        fixture.database.planner_calibration_profile().epoch,
        PlannerCalibrationEpoch(1)
    );
    fixture.close();
}

#[test]
fn calibration_regression_reverts_once_while_insufficient_evidence_remains_active() {
    let mut fixture = TimelineFixture::create("phase5-calibration-revert");
    let target = workload_target(&fixture.database);
    let proposal_window = systematic_window(
        &mut fixture.database,
        target,
        PlannerCalibrationEpoch(0),
        1,
        1,
        100,
    );
    let regression = systematic_window(
        &mut fixture.database,
        target,
        PlannerCalibrationEpoch(1),
        1,
        100,
        1,
    );
    let mut incomplete_regression = regression.clone();
    incomplete_regression.incomplete = true;
    let mut policy = automatic_policy();
    policy.allow_columnar_maintenance = false;
    fixture
        .database
        .automatic_safe_step(
            AutomaticSafeModeInput {
                columnar_table_id: None,
                workload_window: Some(&proposal_window),
                calibration_class: Some(PlannerCalibrationClass::Columnar),
                maintenance_budget: generous_budget(),
            },
            policy,
        )
        .expect("apply calibration");

    let insufficient = AdaptiveWorkloadWindow::new(target, AdaptiveWorkloadLimits::default());
    let waiting = fixture
        .database
        .automatic_safe_step(
            AutomaticSafeModeInput {
                columnar_table_id: None,
                workload_window: Some(&insufficient),
                calibration_class: None,
                maintenance_budget: generous_budget(),
            },
            policy,
        )
        .expect("insufficient trial evidence");
    assert_eq!(
        waiting.outcome,
        AutomaticSafeModeOutcome::PlannerCalibrationTrialAwaiting(
            AutomaticTrialAwaitingReason::InsufficientEvidence
        )
    );
    assert!(waiting.trial_after.is_some());

    let incomplete = fixture
        .database
        .automatic_safe_step(
            AutomaticSafeModeInput {
                columnar_table_id: None,
                workload_window: Some(&incomplete_regression),
                calibration_class: None,
                maintenance_budget: generous_budget(),
            },
            policy,
        )
        .expect("incomplete trial evidence");
    assert_eq!(
        incomplete.outcome,
        AutomaticSafeModeOutcome::PlannerCalibrationTrialAwaiting(
            AutomaticTrialAwaitingReason::IncompleteEvidence
        )
    );
    assert_eq!(incomplete.mutation, None);
    assert!(incomplete.trial_after.is_some());

    let reverted = fixture
        .database
        .automatic_safe_step(
            AutomaticSafeModeInput {
                columnar_table_id: None,
                workload_window: Some(&regression),
                calibration_class: None,
                maintenance_budget: generous_budget(),
            },
            policy,
        )
        .expect("revert calibration");
    assert_eq!(
        reverted.outcome,
        AutomaticSafeModeOutcome::PlannerCalibrationTrialReverted
    );
    assert!(matches!(
        reverted.mutation,
        Some(AutomaticSafeModeMutation::PlannerCalibrationRevert {
            reverted_epoch: PlannerCalibrationEpoch(2),
            ..
        })
    ));
    assert_eq!(
        fixture.database.planner_calibration_profile().columnar,
        CalibrationRatio::IDENTITY
    );
    assert_eq!(reverted.trial_after, None);
    fixture.close();
}

#[test]
fn calibration_deadband_holds_and_manual_epoch_change_wins_the_attribution_race() {
    let mut fixture = TimelineFixture::create("phase5-calibration-hold-stale");
    let target = workload_target(&fixture.database);
    let proposal_window = systematic_window(
        &mut fixture.database,
        target,
        PlannerCalibrationEpoch(0),
        1,
        1,
        100,
    );
    let hold_window = systematic_window(
        &mut fixture.database,
        target,
        PlannerCalibrationEpoch(1),
        1,
        100,
        50,
    );
    let manual_window = systematic_window(
        &mut fixture.database,
        target,
        PlannerCalibrationEpoch(1),
        100,
        10_000,
        100,
    );
    let mut policy = automatic_policy();
    policy.allow_columnar_maintenance = false;
    policy
        .calibration_trial_policy
        .maximum_tolerated_error_regression_work_units = 3;
    fixture
        .database
        .automatic_safe_step(
            AutomaticSafeModeInput {
                columnar_table_id: None,
                workload_window: Some(&proposal_window),
                calibration_class: Some(PlannerCalibrationClass::Columnar),
                maintenance_budget: generous_budget(),
            },
            policy,
        )
        .expect("apply calibration");
    let held = fixture
        .database
        .automatic_safe_step(
            AutomaticSafeModeInput {
                columnar_table_id: None,
                workload_window: Some(&hold_window),
                calibration_class: None,
                maintenance_budget: generous_budget(),
            },
            policy,
        )
        .expect("hold calibration trial");
    assert_eq!(
        held.outcome,
        AutomaticSafeModeOutcome::PlannerCalibrationTrialHeld
    );
    assert_eq!(held.mutation, None);
    assert!(held.trial_after.is_some());

    let manual_proposal = match fixture
        .database
        .advise_planner_calibration(
            &manual_window,
            PlannerCalibrationClass::Columnar,
            permissive_policy(),
        )
        .expect("manual advisor")
    {
        PlannerCalibrationDecision::Proposal(proposal) => proposal,
        other => panic!("expected manual proposal, got {other:?}"),
    };
    let manual_shadow = match fixture
        .database
        .shadow_planner_calibration(&manual_proposal)
    {
        PlannerCalibrationShadowDecision::Accepted(report) => report,
        other => panic!("expected manual shadow acceptance, got {other:?}"),
    };
    fixture
        .database
        .apply_planner_calibration(&manual_proposal, &manual_shadow)
        .expect("manual calibration wins");
    assert_eq!(
        fixture.database.planner_calibration_profile().epoch,
        PlannerCalibrationEpoch(2)
    );
    let stale = fixture
        .database
        .automatic_safe_step(AutomaticSafeModeInput::new(generous_budget()), policy)
        .expect("detect manual epoch change");
    assert_eq!(
        stale.outcome,
        AutomaticSafeModeOutcome::PlannerCalibrationTrialStale(
            AutomaticCalibrationTrialStaleReason::CalibrationEpochChanged
        )
    );
    assert_eq!(stale.mutation, None);
    assert_eq!(stale.trial_after, None);
    assert_eq!(
        fixture.database.planner_calibration_profile().epoch,
        PlannerCalibrationEpoch(2)
    );
    fixture.close();
}

#[test]
fn wrong_columnar_window_waits_and_manual_target_change_clears_the_trial() {
    let mut fixture = Fixture::create("phase5-columnar-mismatch-schema", true);
    fixture
        .database
        .execute("UPDATE events SET category = 7 WHERE id = 7")
        .expect("create projection lag");
    let table_id = workload_target(&fixture.database).table_id;
    let started = fixture
        .database
        .automatic_safe_step(
            AutomaticSafeModeInput {
                columnar_table_id: Some(table_id),
                workload_window: None,
                calibration_class: None,
                maintenance_budget: generous_budget(),
            },
            automatic_policy(),
        )
        .expect("start Columnar trial");
    let Some(AutomaticSafeTrial::Columnar(trial)) = started.trial_after else {
        panic!("Columnar trial expected")
    };
    let target = trial.target();
    let mut wrong_target = target;
    wrong_target.generation.0 += 1;
    let wrong_window = AdaptiveWorkloadWindow::new(wrong_target, AdaptiveWorkloadLimits::default());
    let mismatch = fixture
        .database
        .automatic_safe_step(
            AutomaticSafeModeInput {
                columnar_table_id: None,
                workload_window: Some(&wrong_window),
                calibration_class: None,
                maintenance_budget: generous_budget(),
            },
            automatic_policy(),
        )
        .expect("wrong target evidence");
    assert_eq!(
        mismatch.outcome,
        AutomaticSafeModeOutcome::ColumnarTrialAwaiting(
            AutomaticTrialAwaitingReason::WorkloadTargetMismatch
        )
    );
    assert!(mismatch.trial_after.is_some());

    fixture
        .database
        .compact_columnar_projection(target.projection_id)
        .expect("manual generation change");
    let stale = fixture
        .database
        .automatic_safe_step(
            AutomaticSafeModeInput::new(generous_budget()),
            automatic_policy(),
        )
        .expect("detect generation change");
    assert!(matches!(
        stale.outcome,
        AutomaticSafeModeOutcome::ColumnarTrialStale(
            crate::AdaptiveWorkloadStaleReason::TargetGenerationChanged
        )
    ));
    assert_eq!(stale.mutation, None);
    assert_eq!(stale.trial_after, None);
    fixture.close();
}

#[test]
fn abandon_only_clears_probation_and_reopen_resets_runtime_state() {
    let mut fixture = TimelineFixture::create("phase5-reopen");
    let target = workload_target(&fixture.database);
    let proposal_window = systematic_window(
        &mut fixture.database,
        target,
        PlannerCalibrationEpoch(0),
        1,
        1,
        100,
    );
    let epoch_one = systematic_window(
        &mut fixture.database,
        target,
        PlannerCalibrationEpoch(1),
        100,
        10_000,
        100,
    );
    let mut policy = automatic_policy();
    policy.allow_columnar_maintenance = false;
    fixture
        .database
        .automatic_safe_step(
            AutomaticSafeModeInput {
                columnar_table_id: None,
                workload_window: Some(&proposal_window),
                calibration_class: Some(PlannerCalibrationClass::Columnar),
                maintenance_budget: generous_budget(),
            },
            policy,
        )
        .expect("start trial before abandon");
    assert!(matches!(
        fixture.database.abandon_automatic_safe_trial(),
        Some(AutomaticSafeTrial::PlannerCalibration(_))
    ));
    assert_eq!(
        fixture.database.planner_calibration_profile().epoch,
        PlannerCalibrationEpoch(1)
    );

    fixture
        .database
        .automatic_safe_step(
            AutomaticSafeModeInput {
                columnar_table_id: None,
                workload_window: Some(&epoch_one),
                calibration_class: Some(PlannerCalibrationClass::Columnar),
                maintenance_budget: generous_budget(),
            },
            policy,
        )
        .expect("start trial before reopen");
    assert!(
        fixture
            .database
            .automatic_safe_mode_state()
            .active_trial()
            .is_some()
    );
    let root = fixture.root.clone();
    let event_path = fixture.event_path.clone();
    let clock_path = fixture.clock_path.clone();
    fixture.database.close().expect("close before reopen");
    let reopened = Database::open_catalog(root.join("catalog")).expect("reopen catalog");
    assert_eq!(reopened.automatic_safe_mode_state().active_trial(), None);
    assert_eq!(
        reopened.planner_calibration_profile().epoch,
        PlannerCalibrationEpoch(0)
    );
    reopened.close().expect("close reopened database");
    cleanup(&root, &[event_path, clock_path]);
}

#[test]
fn columnar_no_action_permits_calibration_but_shadow_rejection_creates_no_trial() {
    let mut fixture = TimelineFixture::create("phase5-lanes");
    let target = workload_target(&fixture.database);
    let window = systematic_window(
        &mut fixture.database,
        target,
        PlannerCalibrationEpoch(0),
        1,
        1,
        100,
    );
    let mut planner_policy: PlannerCalibrationPolicy = permissive_policy();
    planner_policy.minimum_shadow_error_improvement_work_units = u64::MAX;
    let mut policy = automatic_policy();
    policy.planner_calibration_policy = planner_policy;
    make_columnar_lag(&mut fixture, 11);
    let report = fixture
        .database
        .automatic_safe_step(
            AutomaticSafeModeInput {
                columnar_table_id: Some(target.table_id),
                workload_window: Some(&window),
                calibration_class: Some(PlannerCalibrationClass::Columnar),
                maintenance_budget: MaintenanceBudget::new(0, 0, 0, 0),
            },
            policy,
        )
        .expect("lane selection");
    assert!(matches!(
        report.outcome,
        AutomaticSafeModeOutcome::NoAction(AutomaticSafeModeNoAction::CalibrationShadowRejected(_))
    ));
    assert!(report.columnar_cycle.is_some());
    assert!(matches!(
        report.columnar_cycle.as_deref().map(|cycle| &cycle.decision),
        Some(AdaptiveDecision::NoAction(no_action))
            if no_action.reason == crate::AdaptiveNoActionReason::InsufficientBudgetEstimate
    ));
    assert!(report.calibration_decision.is_some());
    assert_eq!(report.mutation, None);
    assert_eq!(report.trial_after, None);
    fixture.close();
}
