use netbadb_planner::{PlannerCalibrationClass, PlannerCalibrationEpoch};
use netbadb_types::DatabaseCommitSeq;

use crate::adaptive_workload_tests::{TimelineFixture, set_target_work, workload_target};
use crate::execution_feedback_tests::TABLE_ID;
use crate::planner_calibration_tests::permissive_policy;
use crate::{
    AdaptiveColumnarCompactionPolicy, AdaptiveEvidencePool, AdaptiveEvidenceRecordError,
    AdaptivePolicy, AdaptiveWorkloadPolicy, AutomaticAdmissionScope,
    AutomaticCalibrationTrialPolicy, AutomaticMultiSafeModeInput, AutomaticMultiSafeModePolicy,
    AutomaticSafeModeLane, AutomaticSafeModeMutation, AutomaticSafeModeOutcome,
    AutomaticSafeModePolicy, AutomaticSafeTrial, AutomaticTrialAwaitingReason,
    ColumnarAdvanceBudget, ColumnarProjectionSpec, MaintenanceBudget,
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
        allow_columnar_compaction: false,
        allow_change_stream_gc: false,
        change_stream_gc_policy: crate::AdaptiveChangeStreamGcPolicy::default(),
        columnar_compaction_policy: crate::AdaptiveColumnarCompactionPolicy::default(),
        cross_lane_service: crate::AutomaticCrossLaneServicePolicy::StrictPhysicalPriority,
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

fn set_epoch(report: &mut crate::ExecutionFeedbackReport, epoch: PlannerCalibrationEpoch) {
    report.calibration_epoch = epoch;
    for access in &mut report.accesses {
        if let Some(planner) = &mut access.planner {
            planner.calibration_epoch = epoch;
        }
        if let Some(calibration) = &mut access.calibration {
            calibration.calibration_epoch = epoch;
        }
    }
}

fn create_fresh_delta(fixture: &mut TimelineFixture) -> crate::ColumnarProjectionId {
    let projection_id = fixture.database.inspect_columnar_projections()[0]
        .projection_id
        .expect("projection identity");
    fixture
        .database
        .execute("UPDATE events SET category = 4 WHERE id = 4")
        .expect("create source change");
    let advance = fixture
        .database
        .advance_columnar_projection(projection_id, ColumnarAdvanceBudget::new(16, 1 << 20))
        .expect("publish fresh Delta");
    assert!(advance.caught_up);
    projection_id
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
    pool.rotate_window().expect("renew empty evidence");
    let renewed_wait = fixture
        .database
        .automatic_safe_step_multi(&pool, input(&tables, &[]), policy())
        .expect("trial survives renewal");
    assert_eq!(
        renewed_wait.action.outcome,
        AutomaticSafeModeOutcome::ColumnarTrialAwaiting(
            AutomaticTrialAwaitingReason::MissingWorkloadWindow
        )
    );
    assert!(renewed_wait.action.trial_after.is_some());
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
    let mut validation_reports = Vec::new();
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
        validation_reports.push(report.clone());
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
    pool.rotate_window().expect("renew calibration evidence");
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
    for (offset, mut report) in validation_reports.into_iter().enumerate() {
        set_epoch(&mut report, PlannerCalibrationEpoch(1));
        report.anchor.global_commit_seq = Some(DatabaseCommitSeq(
            820 + u64::try_from(offset).expect("offset"),
        ));
        pool.record_execution_feedback(&report)
            .expect("record renewed epoch");
    }
    let resolved = fixture
        .database
        .automatic_safe_step_multi(&pool, input(&[], &classes), calibration_only)
        .expect("resolve renewed calibration trial");
    assert_eq!(
        resolved.action.outcome,
        AutomaticSafeModeOutcome::PlannerCalibrationTrialValidatedKeep
    );
    assert_eq!(resolved.action.trial_after, None);
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

#[test]
fn bounded_cross_lane_service_waits_for_ready_calibration_then_services_it() {
    let mut fixture = TimelineFixture::create("phase7-cross-lane-service");
    let tables = [TABLE_ID];
    let classes = [PlannerCalibrationClass::Columnar];
    let empty = AdaptiveEvidencePool::default();
    let mut bounded = policy();
    bounded.cross_lane_service = crate::AutomaticCrossLaneServicePolicy::BoundedColumnarBurst {
        max_consecutive_columnar_admissions: 2,
    };

    for expected in 1..=3 {
        fixture
            .database
            .execute("UPDATE events SET category = 3 WHERE id = 3")
            .expect("keep Columnar ready");
        let admitted = fixture
            .database
            .automatic_safe_step_multi(&empty, input(&tables, &classes), bounded)
            .expect("Columnar admission while calibration is blocked");
        assert_eq!(
            admitted.action.selected_lane,
            AutomaticSafeModeLane::ColumnarMaintenance
        );
        assert_eq!(
            admitted
                .cross_lane_service_after
                .consecutive_columnar_admissions,
            expected
        );
        fixture.database.abandon_automatic_safe_trial();
    }
    let idle = fixture
        .database
        .automatic_safe_step_multi(&empty, input(&tables, &classes), bounded)
        .expect("idle round");
    assert_eq!(idle.selected_candidate, None);
    assert_eq!(
        idle.cross_lane_service_after
            .consecutive_columnar_admissions,
        3
    );

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
            1_100 + u64::try_from(offset).expect("offset"),
        ));
        set_target_work(&mut report, target, 10, 20, 30);
        pool.record_execution_feedback(&report)
            .expect("record calibration");
    }
    fixture
        .database
        .execute("UPDATE events SET category = 2 WHERE id = 2")
        .expect("both lanes ready");
    let before = fixture.database.automatic_safe_mode_state();
    let inspected = fixture
        .database
        .inspect_automatic_candidates(&pool, input(&tables, &classes), bounded)
        .expect("pure service inspection");
    assert_eq!(
        inspected.lane_selection_reason,
        crate::AutomaticLaneSelectionReason::CalibrationServiceDue
    );
    assert_eq!(fixture.database.automatic_safe_mode_state(), before);

    let calibration = fixture
        .database
        .automatic_safe_step_multi(&pool, input(&tables, &classes), bounded)
        .expect("due calibration admission");
    assert_eq!(
        calibration.action.selected_lane,
        AutomaticSafeModeLane::PlannerCalibration
    );
    assert_eq!(
        calibration.lane_selection_reason,
        crate::AutomaticLaneSelectionReason::CalibrationServiceDue
    );
    assert_eq!(
        calibration
            .cross_lane_service_after
            .consecutive_columnar_admissions,
        0
    );

    let service_before_trial = fixture
        .database
        .automatic_safe_mode_state()
        .cross_lane_service();
    let awaiting = fixture
        .database
        .automatic_safe_step_multi(&empty, input(&tables, &classes), bounded)
        .expect("active trial remains exclusive");
    assert_eq!(
        awaiting.lane_selection_reason,
        crate::AutomaticLaneSelectionReason::ActiveTrial
    );
    assert_eq!(
        fixture
            .database
            .automatic_safe_mode_state()
            .cross_lane_service(),
        service_before_trial
    );
    fixture.close();
}

#[test]
fn reopen_resets_cross_lane_service_state() {
    let mut fixture = TimelineFixture::create("phase7-service-reopen");
    fixture
        .database
        .execute("UPDATE events SET category = 9 WHERE id = 9")
        .expect("create ready Columnar candidate");
    let pool = AdaptiveEvidencePool::default();
    let admitted = fixture
        .database
        .automatic_safe_step_multi(&pool, input(&[TABLE_ID], &[]), policy())
        .expect("record proactive admission");
    assert_eq!(
        admitted
            .cross_lane_service_after
            .consecutive_columnar_admissions,
        1
    );

    let root = fixture.root.clone();
    let event_path = fixture.event_path.clone();
    let clock_path = fixture.clock_path.clone();
    fixture.database.close().expect("close database");
    let reopened = crate::Database::open_catalog(root.join("catalog")).expect("reopen database");
    assert_eq!(
        reopened
            .automatic_safe_mode_state()
            .cross_lane_service()
            .consecutive_columnar_admissions,
        0
    );
    reopened.close().expect("close reopened database");
    crate::adaptive_workload_tests::cleanup(&root, &[event_path, clock_path]);
}

#[test]
fn automatic_compaction_is_opt_in_enters_existing_trial_and_rotates_evidence_naturally() {
    let mut fixture = TimelineFixture::create("phase8-automatic-compaction");
    let projection_id = create_fresh_delta(&mut fixture);
    let old_target = workload_target(&fixture.database);
    let (_, mut old_report) = fixture
        .database
        .query_with_feedback("SELECT id, category FROM events WHERE category = 4")
        .expect("old-generation feedback");
    set_target_work(&mut old_report, old_target, 10, 10, 20);
    let mut pool = AdaptiveEvidencePool::default();
    pool.record_execution_feedback(&old_report)
        .expect("record old generation");

    let tables = [TABLE_ID];
    let disabled = policy();
    let generation_before = fixture.database.inspect_columnar_projections()[0]
        .generation
        .expect("generation before disabled step");
    let disabled_inspection = fixture
        .database
        .inspect_automatic_candidates(&pool, input(&tables, &[]), disabled)
        .expect("default-compatible inspection");
    assert!(!disabled_inspection.candidates.iter().any(|candidate| {
        matches!(
            candidate.key,
            crate::AutomaticCandidateKey::ColumnarCompaction { .. }
        )
    }));
    let disabled_step = fixture
        .database
        .automatic_safe_step_multi(&pool, input(&tables, &[]), disabled)
        .expect("default-compatible step");
    assert_eq!(disabled_step.selected_candidate, None);
    assert_eq!(
        fixture.database.inspect_columnar_projections()[0].generation,
        Some(generation_before)
    );

    let mut enabled = policy();
    enabled.allow_columnar_compaction = true;
    enabled.columnar_compaction_policy = AdaptiveColumnarCompactionPolicy::new(1, 0);
    let database_state = fixture.database.automatic_safe_mode_state();
    let g_before = fixture
        .database
        .current_database_snapshot()
        .expect("snapshot before inspection")
        .expect("global visibility")
        .commit_seq();
    let first = fixture
        .database
        .inspect_automatic_candidates(&pool, input(&tables, &[]), enabled)
        .expect("inspect compaction");
    let second = fixture
        .database
        .inspect_automatic_candidates(&pool, input(&tables, &[]), enabled)
        .expect("repeat pure inspection");
    assert_eq!(first, second);
    assert_eq!(fixture.database.automatic_safe_mode_state(), database_state);
    let compaction = first
        .candidates
        .iter()
        .find(|candidate| {
            candidate.key
                == (crate::AutomaticCandidateKey::ColumnarCompaction {
                    table_id: TABLE_ID,
                    projection_id,
                })
        })
        .expect("compaction candidate");
    assert_eq!(
        compaction.readiness,
        crate::AutomaticCandidateReadiness::Ready
    );
    assert!(first.candidates.iter().any(|candidate| {
        candidate.key
            == (crate::AutomaticCandidateKey::Columnar {
                table_id: TABLE_ID,
                projection_id: Some(projection_id),
            })
            && candidate.readiness
                == crate::AutomaticCandidateReadiness::ColumnarBlocked(
                    crate::AdaptiveNoActionReason::AlreadyFresh,
                )
    }));
    assert!(compaction.rank.delta_segment_count.unwrap_or(0) > 0);
    assert!(compaction.rank.delta_bytes.unwrap_or(0) > 0);
    assert!(compaction.rank.delta_mutations.unwrap_or(0) > 0);
    assert!(compaction.rank.maintenance_work_units.is_some());

    let admitted = fixture
        .database
        .automatic_safe_step_multi(&pool, input(&tables, &[]), enabled)
        .expect("admit automatic compaction");
    assert_eq!(
        admitted.selected_candidate,
        Some(crate::AutomaticCandidateKey::ColumnarCompaction {
            table_id: TABLE_ID,
            projection_id,
        })
    );
    assert_eq!(
        admitted.action.outcome,
        AutomaticSafeModeOutcome::ColumnarCompactionCompleted
    );
    let Some(AutomaticSafeModeMutation::ColumnarCompaction {
        old_generation,
        new_generation,
        ..
    }) = admitted.action.mutation
    else {
        panic!("expected typed compaction mutation")
    };
    assert_eq!(old_generation, generation_before);
    assert!(new_generation > old_generation);
    assert_eq!(
        admitted
            .cross_lane_service_after
            .consecutive_columnar_admissions,
        1
    );
    let Some(AutomaticSafeTrial::Columnar(trial)) = admitted.action.trial_after else {
        panic!("compaction must start the existing Columnar trial")
    };
    let new_target = trial.target();
    assert_eq!(new_target.generation, new_generation);
    assert_eq!(new_target.projection_id, projection_id);
    assert_eq!(
        fixture
            .database
            .current_database_snapshot()
            .expect("snapshot after compaction")
            .expect("global visibility")
            .commit_seq(),
        g_before
    );

    let awaiting = fixture
        .database
        .automatic_safe_step_multi(
            &pool,
            input(&tables, &[PlannerCalibrationClass::Columnar]),
            enabled,
        )
        .expect("new generation initially lacks evidence");
    assert!(awaiting.candidates.is_empty());
    assert_eq!(
        awaiting.action.outcome,
        AutomaticSafeModeOutcome::ColumnarTrialAwaiting(
            AutomaticTrialAwaitingReason::MissingWorkloadWindow
        )
    );
    assert_eq!(
        awaiting
            .cross_lane_service_after
            .consecutive_columnar_admissions,
        1
    );

    for (offset, sql) in [
        "SELECT id FROM events",
        "SELECT id FROM events LIMIT 10",
        "SELECT id FROM events WHERE category = 4",
    ]
    .iter()
    .enumerate()
    {
        let (_, mut report) = fixture
            .database
            .query_with_feedback(sql)
            .expect("new-generation feedback");
        report.anchor.global_commit_seq = Some(DatabaseCommitSeq(
            10_000 + u64::try_from(offset).expect("offset"),
        ));
        set_target_work(&mut report, new_target, 10, 30, 10);
        pool.record_execution_feedback(&report)
            .expect("rotate lineage to new generation");
    }
    assert!(pool.target_window(new_target).is_some());
    let mut retired = old_report.clone();
    retired.anchor.global_commit_seq = Some(DatabaseCommitSeq(10_003));
    assert!(matches!(
        pool.record_execution_feedback(&retired),
        Err(AdaptiveEvidenceRecordError::RetiredTargetEvidence { target })
            if target == old_target
    ));

    let reverted = fixture
        .database
        .automatic_safe_step_multi(&pool, input(&tables, &[]), enabled)
        .expect("evaluate compacted generation regression");
    assert_eq!(
        reverted.action.outcome,
        AutomaticSafeModeOutcome::ColumnarTrialReverted
    );
    assert_eq!(reverted.action.trial_after, None);
    assert_eq!(
        fixture.database.inspect_columnar_projections()[0].generation,
        Some(new_generation),
        "regression must not restore the old generation"
    );
    let observed = fixture
        .database
        .observe_adaptive_columnar(TABLE_ID)
        .expect("observe exact suppression");
    let target = observed
        .projections
        .iter()
        .find(|target| target.projection.generation == Some(new_generation))
        .expect("current compacted generation");
    assert!(target.suppressed_after_revert);
    fixture.close();
}

#[test]
fn catch_up_and_compaction_are_never_ready_for_the_same_projection() {
    let mut fixture = TimelineFixture::create("phase8-mutually-exclusive-readiness");
    let projection_id = fixture.database.inspect_columnar_projections()[0]
        .projection_id
        .expect("projection identity");
    fixture
        .database
        .execute("UPDATE events SET category = 99 WHERE id = 5")
        .expect("make projection lagging");
    let mut enabled = policy();
    enabled.allow_columnar_compaction = true;
    enabled.columnar_compaction_policy = AdaptiveColumnarCompactionPolicy::new(1, 0);
    let pool = AdaptiveEvidencePool::default();
    let tables = [TABLE_ID];

    let lagging = fixture
        .database
        .inspect_automatic_candidates(&pool, input(&tables, &[]), enabled)
        .expect("inspect lagging projection");
    assert!(lagging.candidates.iter().any(|candidate| {
        candidate.key
            == (crate::AutomaticCandidateKey::Columnar {
                table_id: TABLE_ID,
                projection_id: Some(projection_id),
            })
            && candidate.readiness == crate::AutomaticCandidateReadiness::Ready
    }));
    assert!(lagging.candidates.iter().any(|candidate| {
        candidate.key
            == (crate::AutomaticCandidateKey::ColumnarCompaction {
                table_id: TABLE_ID,
                projection_id,
            })
            && candidate.readiness
                == crate::AutomaticCandidateReadiness::ColumnarCompactionBlocked(
                    crate::AdaptiveColumnarCompactionNoActionReason::MaintenanceBlocked(
                        crate::MaintenanceBlocker::ProjectionLagging,
                    ),
                )
    }));

    fixture
        .database
        .advance_columnar_projection(projection_id, ColumnarAdvanceBudget::new(16, 1 << 20))
        .expect("catch up into a fresh Delta");
    let fresh = fixture
        .database
        .inspect_automatic_candidates(&pool, input(&tables, &[]), enabled)
        .expect("inspect fresh projection");
    assert!(fresh.candidates.iter().any(|candidate| {
        candidate.key
            == (crate::AutomaticCandidateKey::Columnar {
                table_id: TABLE_ID,
                projection_id: Some(projection_id),
            })
            && candidate.readiness
                == crate::AutomaticCandidateReadiness::ColumnarBlocked(
                    crate::AdaptiveNoActionReason::AlreadyFresh,
                )
    }));
    assert!(fresh.candidates.iter().any(|candidate| {
        candidate.key
            == (crate::AutomaticCandidateKey::ColumnarCompaction {
                table_id: TABLE_ID,
                projection_id,
            })
            && candidate.readiness == crate::AutomaticCandidateReadiness::Ready
    }));
    fixture.close();
}

#[test]
fn one_step_compacts_only_one_of_multiple_ready_projections() {
    let mut fixture = TimelineFixture::create("phase8-one-of-many-compactions");
    let second_id = fixture
        .database
        .build_incremental_columnar_projection(ColumnarProjectionSpec::new(
            TABLE_ID,
            fixture.root.join("phase8-projection-two"),
            vec![ColumnId(1), ColumnId(2)],
        ))
        .expect("build second projection");
    fixture
        .database
        .execute("UPDATE events SET category = 98 WHERE id = 6")
        .expect("lag both projections");
    let projection_ids = fixture
        .database
        .inspect_columnar_projections()
        .iter()
        .map(|projection| projection.projection_id.expect("projection identity"))
        .collect::<Vec<_>>();
    for projection_id in &projection_ids {
        fixture
            .database
            .advance_columnar_projection(*projection_id, ColumnarAdvanceBudget::new(16, 1 << 20))
            .expect("make projection compactable");
    }
    assert!(projection_ids.contains(&second_id));
    let generations_before = fixture
        .database
        .inspect_columnar_projections()
        .iter()
        .map(|projection| {
            (
                projection.projection_id.expect("projection identity"),
                projection.generation.expect("projection generation"),
            )
        })
        .collect::<Vec<_>>();
    let mut enabled = policy();
    enabled.allow_columnar_compaction = true;
    enabled.columnar_compaction_policy = AdaptiveColumnarCompactionPolicy::new(1, 0);
    let pool = AdaptiveEvidencePool::default();
    let tables = [TABLE_ID];
    let inspected = fixture
        .database
        .inspect_automatic_candidates(&pool, input(&tables, &[]), enabled)
        .expect("inspect multiple compactable projections");
    assert_eq!(
        inspected
            .candidates
            .iter()
            .filter(|candidate| {
                matches!(
                    candidate.key,
                    crate::AutomaticCandidateKey::ColumnarCompaction { .. }
                ) && candidate.readiness == crate::AutomaticCandidateReadiness::Ready
            })
            .count(),
        2
    );

    let executed = fixture
        .database
        .automatic_safe_step_multi(&pool, input(&tables, &[]), enabled)
        .expect("execute exactly one compaction");
    let Some(AutomaticSafeModeMutation::ColumnarCompaction {
        projection_id: selected_id,
        ..
    }) = executed.action.mutation
    else {
        panic!("expected one typed compaction mutation")
    };
    let changed = fixture
        .database
        .inspect_columnar_projections()
        .iter()
        .filter(|projection| {
            generations_before
                .iter()
                .any(|(projection_id, generation)| {
                    Some(*projection_id) == projection.projection_id
                        && Some(*generation) != projection.generation
                })
        })
        .count();
    assert_eq!(changed, 1);
    assert_eq!(
        fixture
            .database
            .inspect_columnar_projections()
            .iter()
            .find(|projection| projection.projection_id == Some(selected_id))
            .expect("selected projection")
            .delta_segment_count,
        Some(0)
    );
    fixture.close();
}

#[test]
fn automatic_compaction_generation_reopens_but_probation_does_not() {
    let mut fixture = TimelineFixture::create("phase8-compaction-reopen");
    create_fresh_delta(&mut fixture);
    let mut enabled = policy();
    enabled.allow_columnar_compaction = true;
    enabled.columnar_compaction_policy = AdaptiveColumnarCompactionPolicy::new(1, 0);
    let report = fixture
        .database
        .automatic_safe_step_multi(
            &AdaptiveEvidencePool::default(),
            input(&[TABLE_ID], &[]),
            enabled,
        )
        .expect("automatic compaction");
    let Some(AutomaticSafeModeMutation::ColumnarCompaction { new_generation, .. }) =
        report.action.mutation
    else {
        panic!("expected compaction mutation")
    };
    assert!(report.action.trial_after.is_some());

    let root = fixture.root.clone();
    let event_path = fixture.event_path.clone();
    let clock_path = fixture.clock_path.clone();
    fixture.database.close().expect("close database");
    let reopened = crate::Database::open_catalog(root.join("catalog")).expect("reopen database");
    assert_eq!(
        reopened.inspect_columnar_projections()[0].generation,
        Some(new_generation)
    );
    assert_eq!(reopened.automatic_safe_mode_state().active_trial(), None);
    assert_eq!(
        reopened
            .automatic_safe_mode_state()
            .cross_lane_service()
            .consecutive_columnar_admissions,
        0
    );
    reopened.close().expect("close reopened database");
    crate::adaptive_workload_tests::cleanup(&root, &[event_path, clock_path]);
}

#[test]
fn compaction_counts_as_columnar_service_before_calibration_receives_the_lane() {
    let mut fixture = TimelineFixture::create("phase8-compaction-cross-lane");
    create_fresh_delta(&mut fixture);
    let target = workload_target(&fixture.database);
    let mut pool = AdaptiveEvidencePool::default();
    for (offset, sql) in [
        "SELECT id FROM events",
        "SELECT id FROM events LIMIT 10",
        "SELECT id FROM events WHERE category = 4",
    ]
    .iter()
    .enumerate()
    {
        let (_, mut report) = fixture
            .database
            .query_with_feedback(sql)
            .expect("calibration feedback");
        report.anchor.global_commit_seq = Some(DatabaseCommitSeq(
            20_000 + u64::try_from(offset).expect("offset"),
        ));
        set_target_work(&mut report, target, 10, 20, 30);
        pool.record_execution_feedback(&report)
            .expect("record calibration evidence");
    }

    let mut enabled = policy();
    enabled.allow_columnar_compaction = true;
    enabled.columnar_compaction_policy = AdaptiveColumnarCompactionPolicy::new(1, 0);
    enabled.cross_lane_service = crate::AutomaticCrossLaneServicePolicy::BoundedColumnarBurst {
        max_consecutive_columnar_admissions: 1,
    };
    let tables = [TABLE_ID];
    let classes = [PlannerCalibrationClass::Columnar];
    let input = input(&tables, &classes);
    let compaction = fixture
        .database
        .automatic_safe_step_multi(&pool, input, enabled)
        .expect("Columnar receives first burst opportunity");
    assert_eq!(
        compaction.action.outcome,
        AutomaticSafeModeOutcome::ColumnarCompactionCompleted
    );
    assert_eq!(
        compaction
            .cross_lane_service_after
            .consecutive_columnar_admissions,
        1
    );
    fixture.database.abandon_automatic_safe_trial();
    create_fresh_delta(&mut fixture);

    let calibration = fixture
        .database
        .automatic_safe_step_multi(&pool, input, enabled)
        .expect("calibration receives the due cross-lane opportunity");
    assert_eq!(
        calibration.action.selected_lane,
        AutomaticSafeModeLane::PlannerCalibration
    );
    assert_eq!(
        calibration.lane_selection_reason,
        crate::AutomaticLaneSelectionReason::CalibrationServiceDue
    );
    assert_eq!(
        calibration
            .cross_lane_service_after
            .consecutive_columnar_admissions,
        0
    );
    fixture.close();
}

#[test]
fn compacted_generation_uses_existing_hold_then_keep_workload_hysteresis() {
    let mut fixture = TimelineFixture::create("phase8-compaction-hold-keep");
    create_fresh_delta(&mut fixture);
    let mut enabled = policy();
    enabled.allow_columnar_compaction = true;
    enabled.columnar_compaction_policy = AdaptiveColumnarCompactionPolicy::new(1, 0);
    let tables = [TABLE_ID];
    let started = fixture
        .database
        .automatic_safe_step_multi(
            &AdaptiveEvidencePool::default(),
            input(&tables, &[]),
            enabled,
        )
        .expect("start compaction probation");
    let Some(AutomaticSafeTrial::Columnar(trial)) = started.action.trial_after else {
        panic!("expected existing Columnar trial")
    };
    let target = trial.target();
    let service_state = started.cross_lane_service_after;
    let mut pool = AdaptiveEvidencePool::default();
    for (offset, sql) in [
        "SELECT id FROM events",
        "SELECT id FROM events LIMIT 10",
        "SELECT id FROM events WHERE category = 4",
    ]
    .iter()
    .enumerate()
    {
        let (_, mut report) = fixture
            .database
            .query_with_feedback(sql)
            .expect("deadband feedback");
        report.anchor.global_commit_seq = Some(DatabaseCommitSeq(
            30_000 + u64::try_from(offset).expect("offset"),
        ));
        set_target_work(&mut report, target, 10, 10, 10);
        pool.record_execution_feedback(&report)
            .expect("record deadband evidence");
    }
    let held = fixture
        .database
        .automatic_safe_step_multi(&pool, input(&tables, &[]), enabled)
        .expect("hold compacted generation");
    assert_eq!(
        held.action.outcome,
        AutomaticSafeModeOutcome::ColumnarTrialHeld
    );
    assert!(held.action.trial_after.is_some());
    assert_eq!(held.cross_lane_service_after, service_state);

    for (offset, sql) in [
        "SELECT category FROM events",
        "SELECT category FROM events LIMIT 20",
        "SELECT category FROM events WHERE id = 4",
    ]
    .iter()
    .enumerate()
    {
        let (_, mut report) = fixture
            .database
            .query_with_feedback(sql)
            .expect("keep feedback");
        report.anchor.global_commit_seq = Some(DatabaseCommitSeq(
            40_000 + u64::try_from(offset).expect("offset"),
        ));
        set_target_work(&mut report, target, 10, 5, 30);
        pool.record_execution_feedback(&report)
            .expect("record keep evidence");
    }
    let kept = fixture
        .database
        .automatic_safe_step_multi(&pool, input(&tables, &[]), enabled)
        .expect("keep compacted generation");
    assert_eq!(
        kept.action.outcome,
        AutomaticSafeModeOutcome::ColumnarTrialValidatedKeep
    );
    assert_eq!(kept.action.trial_after, None);
    assert_eq!(kept.cross_lane_service_after, service_state);
    fixture.close();
}
