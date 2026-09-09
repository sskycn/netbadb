use netbadb_planner::{PlannerCalibrationClass, PlannerCalibrationEpoch};
use netbadb_types::DatabaseCommitSeq;

use crate::adaptive_workload_tests::{TimelineFixture, set_target_work, workload_target};
use crate::execution_feedback_tests::TABLE_ID;
use crate::planner_calibration_tests::permissive_policy;
use crate::{
    AdaptiveEvidencePool, AdaptivePolicy, AdaptiveWorkloadPolicy, AutomaticAdmissionScope,
    AutomaticCalibrationTrialPolicy, AutomaticMultiSafeModeInput, AutomaticMultiSafeModePolicy,
    AutomaticSafeModeLane, AutomaticSafeModeMutation, AutomaticSafeModeOutcome,
    AutomaticSafeModePolicy, AutomaticSafeTrial, AutomaticTrialAwaitingReason,
    ColumnarProjectionSpec, MaintenanceBudget,
};
use netbadb_types::ColumnId;

fn budget() -> MaintenanceBudget {
    MaintenanceBudget::new(u64::MAX, u64::MAX, u64::MAX, 1)
}

fn policy() -> AutomaticMultiSafeModePolicy {
    AutomaticMultiSafeModePolicy {
        safe_mode: AutomaticSafeModePolicy {
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
        },
        max_candidate_tables: 8,
        max_calibration_classes: 4,
        max_fairness_entries: 16,
    }
}

fn input<'a>(
    tables: &'a [crate::TableId],
    classes: &'a [PlannerCalibrationClass],
) -> AutomaticMultiSafeModeInput<'a> {
    AutomaticMultiSafeModeInput {
        scope: AutomaticAdmissionScope {
            table_ids: tables,
            calibration_classes: classes,
        },
        maintenance_budget: budget(),
    }
}

#[test]
fn inspection_is_pure_scope_is_deduplicated_and_active_trial_preempts_admission() {
    let mut fixture = TimelineFixture::create("phase6-inspection-firewall");
    fixture
        .database
        .execute("UPDATE events SET category = 7 WHERE id = 7")
        .expect("create lag");
    let pool = AdaptiveEvidencePool::default();
    let tables = [TABLE_ID, TABLE_ID];
    let first = fixture
        .database
        .inspect_automatic_candidates(&pool, input(&tables, &[]), policy())
        .expect("first inspection");
    let second = fixture
        .database
        .inspect_automatic_candidates(&pool, input(&tables, &[]), policy())
        .expect("second inspection");
    assert_eq!(first, second);
    assert_eq!(
        first
            .candidates
            .iter()
            .filter(|candidate| candidate.lane == AutomaticSafeModeLane::ColumnarMaintenance)
            .count(),
        1
    );

    let started = fixture
        .database
        .automatic_safe_step_multi(&pool, input(&tables, &[]), policy())
        .expect("start one trial");
    assert!(matches!(
        started.action.mutation,
        Some(AutomaticSafeModeMutation::ColumnarAdvance { .. })
    ));
    assert!(matches!(
        started.action.trial_after,
        Some(AutomaticSafeTrial::Columnar(_))
    ));

    let waiting = fixture
        .database
        .automatic_safe_step_multi(
            &pool,
            input(&tables, &[PlannerCalibrationClass::Columnar]),
            policy(),
        )
        .expect("active trial owns step");
    assert!(waiting.candidates.is_empty());
    assert_eq!(waiting.selected_candidate, None);
    assert_eq!(
        waiting.action.outcome,
        AutomaticSafeModeOutcome::ColumnarTrialAwaiting(
            AutomaticTrialAwaitingReason::MissingWorkloadWindow
        )
    );
    fixture.close();
}

#[test]
fn every_projection_is_a_candidate_and_stable_identity_breaks_equal_merit() {
    let mut fixture = TimelineFixture::create("phase6-multi-projection");
    fixture
        .database
        .build_incremental_columnar_projection(ColumnarProjectionSpec::new(
            TABLE_ID,
            fixture.root.join("projection-two"),
            vec![ColumnId(1), ColumnId(2)],
        ))
        .expect("second projection");
    fixture
        .database
        .execute("UPDATE events SET category = 5 WHERE id = 5")
        .expect("lag both projections");
    let pool = AdaptiveEvidencePool::default();
    let tables = [TABLE_ID];
    let inspected = fixture
        .database
        .inspect_automatic_candidates(&pool, input(&tables, &[]), policy())
        .expect("inspect projections");
    let mut projection_ids = inspected
        .candidates
        .iter()
        .filter_map(|candidate| match candidate.key {
            crate::AutomaticCandidateKey::Columnar {
                projection_id: Some(projection_id),
                ..
            } => Some(projection_id),
            _ => None,
        })
        .collect::<Vec<_>>();
    projection_ids.sort_unstable();
    assert_eq!(projection_ids.len(), 2);

    let selected = fixture
        .database
        .automatic_safe_step_multi(&pool, input(&tables, &[]), policy())
        .expect("select one projection");
    assert_eq!(
        selected.selected_candidate,
        Some(crate::AutomaticCandidateKey::Columnar {
            table_id: TABLE_ID,
            projection_id: Some(projection_ids[0]),
        })
    );
    fixture.close();
}

#[test]
fn active_columnar_trial_resolves_from_exact_pool_window_across_g() {
    let mut fixture = TimelineFixture::create("phase6-columnar-pool-trial");
    fixture
        .database
        .execute("UPDATE events SET category = 6 WHERE id = 6")
        .expect("create lag");
    let tables = [TABLE_ID];
    let empty = AdaptiveEvidencePool::default();
    fixture
        .database
        .automatic_safe_step_multi(&empty, input(&tables, &[]), policy())
        .expect("start trial");
    let Some(AutomaticSafeTrial::Columnar(trial)) =
        fixture.database.automatic_safe_mode_state().active_trial()
    else {
        panic!("expected Columnar trial")
    };
    let target = trial.target();
    let mut pool = AdaptiveEvidencePool::default();
    for (offset, sql) in [
        "SELECT id FROM events",
        "SELECT id FROM events LIMIT 10",
        "SELECT id FROM events WHERE category = 1",
    ]
    .iter()
    .enumerate()
    {
        let (_, mut report) = fixture
            .database
            .query_with_feedback(sql)
            .expect("trial feedback");
        report.anchor.global_commit_seq = Some(DatabaseCommitSeq(
            700 + u64::try_from(offset).expect("offset"),
        ));
        set_target_work(&mut report, target, 10, 5, 20);
        pool.record_execution_feedback(&report)
            .expect("pool evidence");
    }
    let resolved = fixture
        .database
        .automatic_safe_step_multi(&pool, input(&tables, &[]), policy())
        .expect("resolve from pool");
    assert_eq!(
        resolved.action.outcome,
        AutomaticSafeModeOutcome::ColumnarTrialValidatedKeep
    );
    assert_eq!(resolved.action.trial_after, None);
    fixture.close();
}

#[test]
fn calibration_candidate_uses_global_pool_and_starts_one_epoch_trial() {
    let mut fixture = TimelineFixture::create("phase6-calibration-admission");
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
        let (_, mut report) = fixture
            .database
            .query_with_feedback(sql)
            .expect("calibration feedback");
        report.anchor.global_commit_seq = Some(DatabaseCommitSeq(
            800 + u64::try_from(offset).expect("offset"),
        ));
        set_target_work(&mut report, target, 10, 20, 30);
        pool.record_execution_feedback(&report)
            .expect("record calibration");
    }
    let classes = [PlannerCalibrationClass::Columnar];
    let mut calibration_only = policy();
    calibration_only.safe_mode.allow_columnar_maintenance = false;
    let applied = fixture
        .database
        .automatic_safe_step_multi(&pool, input(&[], &classes), calibration_only)
        .expect("apply calibration");
    assert_eq!(
        applied.action.outcome,
        AutomaticSafeModeOutcome::PlannerCalibrationApplied
    );
    assert!(matches!(
        applied.action.mutation,
        Some(AutomaticSafeModeMutation::PlannerCalibrationApply {
            applied_epoch: PlannerCalibrationEpoch(1),
            ..
        })
    ));
    assert!(matches!(
        applied.action.trial_after,
        Some(AutomaticSafeTrial::PlannerCalibration(_))
    ));
    let old_epoch_only = fixture
        .database
        .automatic_safe_step_multi(&pool, input(&[], &classes), calibration_only)
        .expect("old epoch cannot resolve applied trial");
    assert_eq!(
        old_epoch_only.action.outcome,
        AutomaticSafeModeOutcome::PlannerCalibrationTrialAwaiting(
            AutomaticTrialAwaitingReason::MissingWorkloadWindow
        )
    );
    assert!(matches!(
        old_epoch_only.action.trial_after,
        Some(AutomaticSafeTrial::PlannerCalibration(_))
    ));
    fixture.close();
}

#[test]
fn ready_columnar_lane_precedes_stronger_calibration_candidate() {
    let mut fixture = TimelineFixture::create("phase6-lane-priority");
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
        let (_, mut report) = fixture
            .database
            .query_with_feedback(sql)
            .expect("calibration evidence");
        report.anchor.global_commit_seq = Some(DatabaseCommitSeq(
            900 + u64::try_from(offset).expect("offset"),
        ));
        set_target_work(&mut report, target, 10, 1_000, 1_500);
        pool.record_execution_feedback(&report)
            .expect("record evidence");
    }
    fixture
        .database
        .execute("UPDATE events SET category = 4 WHERE id = 4")
        .expect("create Columnar lag");
    let tables = [TABLE_ID];
    let classes = [PlannerCalibrationClass::Columnar];
    let report = fixture
        .database
        .automatic_safe_step_multi(&pool, input(&tables, &classes), policy())
        .expect("tiered admission");
    assert_eq!(
        report.action.selected_lane,
        AutomaticSafeModeLane::ColumnarMaintenance
    );
    assert!(matches!(
        report.action.mutation,
        Some(AutomaticSafeModeMutation::ColumnarAdvance { .. })
    ));
    assert_eq!(
        fixture.database.planner_calibration_profile().epoch,
        PlannerCalibrationEpoch(0)
    );
    fixture.close();
}
