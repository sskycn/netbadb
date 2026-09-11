use netbadb_planner::{
    CalibrationRatio, PlannerCalibrationClass, PlannerCalibrationEpoch, PlannerCalibrationSample,
};
use netbadb_types::DatabaseCommitSeq;

use crate::adaptive_workload_tests::{TimelineFixture, cleanup, set_target_work, workload_target};
use crate::execution_feedback_tests::Fixture;
use crate::{
    AdaptiveWorkloadLimits, AdaptiveWorkloadOutcome, AdaptiveWorkloadPolicy,
    AdaptiveWorkloadRecordOutcome, AdaptiveWorkloadTarget, AdaptiveWorkloadWindow,
    ColumnarAdvanceBudget, Database, ExecutionFeedbackReport, PlannerCalibrationDecision,
    PlannerCalibrationMutationError, PlannerCalibrationNoAction, PlannerCalibrationPolicy,
    PlannerCalibrationShadowDecision,
};

pub(super) const SHAPES: [&str; 3] = [
    "SELECT id FROM events",
    "SELECT id FROM events LIMIT 10",
    "SELECT id FROM events WHERE category = 1",
];

pub(super) fn permissive_policy() -> PlannerCalibrationPolicy {
    PlannerCalibrationPolicy {
        minimum_samples: 3,
        minimum_actual_work_units: 1,
        minimum_distinct_visibility_points: 3,
        minimum_distinct_query_shapes: 3,
        minimum_directional_query_shape_margin: 2,
        error_deadband_work_units: 0,
        minimum_shadow_error_improvement_work_units: 1,
        global_min_ratio: CalibrationRatio::new(1, 100).expect("minimum ratio"),
        global_max_ratio: CalibrationRatio::new(100, 1).expect("maximum ratio"),
        maximum_step_up_ratio: CalibrationRatio::new(100, 1).expect("up step"),
        maximum_step_down_ratio: CalibrationRatio::new(100, 1).expect("down step"),
    }
}

#[test]
fn planner_policy_shared_validation_covers_ratio_relationships() {
    let valid = PlannerCalibrationPolicy::default();
    assert!(valid.is_valid());

    let mut inverted_bounds = valid;
    inverted_bounds.global_min_ratio = CalibrationRatio::new(3, 1).expect("ratio");
    assert!(!inverted_bounds.is_valid());

    let mut sub_identity_step_up = valid;
    sub_identity_step_up.maximum_step_up_ratio = CalibrationRatio::HALF;
    assert!(!sub_identity_step_up.is_valid());

    let mut sub_identity_step_down = valid;
    sub_identity_step_down.maximum_step_down_ratio = CalibrationRatio::HALF;
    assert!(!sub_identity_step_down.is_valid());
}

#[allow(clippy::too_many_arguments)]
pub(super) fn calibration_report(
    database: &mut Database,
    target: AdaptiveWorkloadTarget,
    sql: &str,
    global_commit_seq: u64,
    epoch: PlannerCalibrationEpoch,
    base: u64,
    effective: u64,
    actual: u64,
) -> ExecutionFeedbackReport {
    let (_, mut report) = database
        .query_with_feedback(sql)
        .expect("query calibration sample");
    report.anchor.global_commit_seq = Some(DatabaseCommitSeq(global_commit_seq));
    report.calibration_epoch = epoch;
    set_target_work(&mut report, target, base, actual, base.saturating_mul(2));
    let access = report
        .accesses
        .iter_mut()
        .find(|access| {
            access
                .planner
                .as_ref()
                .is_some_and(|planner| planner.projection_id == Some(target.projection_id))
        })
        .expect("columnar calibration access");
    let planner = access.planner.as_mut().expect("planner evidence");
    planner.effective_work_units = Some(effective);
    planner.calibration_epoch = epoch;
    access.calibration = Some(PlannerCalibrationSample::with_effective(
        base,
        Some(effective),
        epoch,
        Some(actual),
    ));
    report
}

pub(super) fn systematic_window(
    database: &mut Database,
    target: AdaptiveWorkloadTarget,
    epoch: PlannerCalibrationEpoch,
    base: u64,
    effective: u64,
    actual: u64,
) -> AdaptiveWorkloadWindow {
    let mut window = AdaptiveWorkloadWindow::new(target, AdaptiveWorkloadLimits::default());
    for (index, sql) in SHAPES.iter().enumerate() {
        let report = calibration_report(
            database,
            target,
            sql,
            100 + u64::try_from(index).expect("index"),
            epoch,
            base,
            effective,
            actual,
        );
        window.record(&report).expect("record calibration sample");
    }
    window
}

#[allow(clippy::too_many_arguments)]
fn class_report(
    database: &mut Database,
    sql: &str,
    global_commit_seq: u64,
    class: PlannerCalibrationClass,
    epoch: PlannerCalibrationEpoch,
    base: u64,
    effective: u64,
    actual: u64,
) -> ExecutionFeedbackReport {
    let (_, mut report) = database
        .query_with_feedback(sql)
        .expect("query class calibration sample");
    report.anchor.global_commit_seq = Some(DatabaseCommitSeq(global_commit_seq));
    report.calibration_epoch = epoch;
    let access = report
        .accesses
        .iter_mut()
        .find(|access| {
            access
                .planner
                .as_ref()
                .is_some_and(|planner| planner.kind.calibration_class() == class)
        })
        .expect("class access");
    let planner = access.planner.as_mut().expect("planner evidence");
    planner.estimated_work_units = Some(base);
    planner.effective_work_units = Some(effective);
    planner.calibration_epoch = epoch;
    access.calibration = Some(PlannerCalibrationSample::with_effective(
        base,
        Some(effective),
        epoch,
        Some(actual),
    ));
    report
}

fn proposal(
    database: &Database,
    window: &AdaptiveWorkloadWindow,
    policy: PlannerCalibrationPolicy,
) -> crate::PlannerCalibrationProposal {
    match database
        .advise_planner_calibration(window, PlannerCalibrationClass::Columnar, policy)
        .expect("advise calibration")
    {
        PlannerCalibrationDecision::Proposal(proposal) => *proposal,
        PlannerCalibrationDecision::NoAction(reason) => {
            panic!("expected proposal, got {reason:?}")
        }
    }
}

#[test]
fn advisor_shadow_apply_and_revert_are_explicit_forward_only_and_change_real_planning() {
    let mut fixture = TimelineFixture::create("phase4-apply-revert");
    let target = workload_target(&fixture.database);
    let baseline_rows = fixture
        .database
        .query(SHAPES[0])
        .expect("baseline query rows");
    let window = systematic_window(
        &mut fixture.database,
        target,
        PlannerCalibrationEpoch(0),
        1,
        1,
        100,
    );
    let proposal = proposal(&fixture.database, &window, permissive_policy());
    assert_eq!(proposal.based_on_epoch(), PlannerCalibrationEpoch(0));
    assert_eq!(
        proposal.target_ratio(),
        CalibrationRatio::new(100, 1).unwrap()
    );
    assert_eq!(proposal.proposed_ratio(), proposal.target_ratio());
    let shadow = match fixture.database.shadow_planner_calibration(&proposal) {
        PlannerCalibrationShadowDecision::Accepted(report) => report,
        PlannerCalibrationShadowDecision::Rejected { reason, .. } => {
            panic!("shadow unexpectedly rejected: {reason:?}")
        }
    };
    let stale_proposal = proposal.clone();
    let stale_shadow = shadow.clone();
    assert!(shadow.accepted());
    assert!(shadow.total_new_error_work_units() < shadow.total_old_error_work_units());

    let before_dml = fixture
        .database
        .current_database_snapshot()
        .expect("visibility")
        .expect("global visibility")
        .commit_seq();
    fixture
        .database
        .execute("INSERT INTO events (id, category, payload) VALUES (9000, 1, 'new')")
        .expect("ordinary DML between proposal and apply");
    let after_dml = fixture
        .database
        .current_database_snapshot()
        .expect("visibility")
        .expect("global visibility")
        .commit_seq();
    assert!(after_dml > before_dml);
    let schema_before_apply = fixture.database.schema_generation();
    let expected_rows_after_dml = fixture
        .database
        .query(SHAPES[0])
        .expect("rows after ordinary DML");
    let source_before_apply = fixture
        .database
        .observe_adaptive_columnar(target.table_id)
        .expect("source observation")
        .source
        .expect("source")
        .data_version;
    let receipt = fixture
        .database
        .apply_planner_calibration(&proposal, &shadow)
        .expect("ordinary G advance does not stale proposal");
    assert_eq!(
        fixture
            .database
            .apply_planner_calibration(&stale_proposal, &stale_shadow),
        Err(PlannerCalibrationMutationError::StaleCalibrationProposal)
    );
    assert_eq!(receipt.previous_epoch(), PlannerCalibrationEpoch(0));
    assert_eq!(receipt.applied_epoch(), PlannerCalibrationEpoch(1));
    assert_eq!(
        fixture.database.planner_calibration_profile().columnar,
        CalibrationRatio::new(100, 1).unwrap()
    );
    assert_eq!(fixture.database.schema_generation(), schema_before_apply);
    assert_eq!(
        fixture
            .database
            .current_database_snapshot()
            .unwrap()
            .unwrap()
            .commit_seq(),
        after_dml
    );
    assert_eq!(
        fixture
            .database
            .observe_adaptive_columnar(target.table_id)
            .unwrap()
            .source
            .unwrap()
            .data_version,
        source_before_apply
    );

    fixture
        .database
        .advance_columnar_projection(
            target.projection_id,
            ColumnarAdvanceBudget::new(usize::MAX, u64::MAX),
        )
        .expect("make projection current after DML");
    let (calibrated_rows, feedback) = fixture
        .database
        .query_with_feedback(SHAPES[0])
        .expect("calibrated query");
    assert_ne!(expected_rows_after_dml, baseline_rows);
    assert_eq!(calibrated_rows, expected_rows_after_dml);
    assert_eq!(feedback.calibration_epoch, PlannerCalibrationEpoch(1));
    assert!(
        feedback
            .accesses
            .iter()
            .all(|access| access.planner.as_ref().is_none_or(|planner| {
                planner.calibration_epoch == PlannerCalibrationEpoch(1)
                    && planner.estimated_work_units.is_some()
                    && planner.effective_work_units.is_some()
            }))
    );
    assert!(!matches!(
        feedback.plan_variant,
        netbadb_planner::PlanVariant::ColumnarScan { .. }
    ));

    let revert = fixture
        .database
        .revert_planner_calibration(receipt)
        .expect("revert calibration");
    assert_eq!(revert.previous_epoch(), PlannerCalibrationEpoch(1));
    assert_eq!(revert.applied_epoch(), PlannerCalibrationEpoch(2));
    assert_eq!(
        fixture.database.planner_calibration_profile().columnar,
        CalibrationRatio::IDENTITY
    );
    assert_eq!(
        fixture.database.revert_planner_calibration(receipt),
        Err(PlannerCalibrationMutationError::StaleCalibrationReceipt)
    );
    fixture.close();
}

#[test]
fn advisor_separates_epochs_enforces_diversity_consistency_deadband_and_clamps() {
    let mut fixture = TimelineFixture::create("phase4-advisor");
    let target = workload_target(&fixture.database);
    let mut window = systematic_window(
        &mut fixture.database,
        target,
        PlannerCalibrationEpoch(0),
        100,
        100,
        200,
    );
    let foreign_epoch = calibration_report(
        &mut fixture.database,
        target,
        SHAPES[0],
        103,
        PlannerCalibrationEpoch(1),
        1,
        1,
        10_000,
    );
    window.record(&foreign_epoch).expect("record future epoch");
    let mut bounded = permissive_policy();
    bounded.global_max_ratio = CalibrationRatio::DOUBLE;
    bounded.maximum_step_up_ratio = CalibrationRatio::NINE_EIGHTHS;
    let up_proposal = proposal(&fixture.database, &window, bounded);
    assert_eq!(up_proposal.evidence().sample_count, 3);
    assert_eq!(up_proposal.evidence().distinct_query_shapes, 3);
    assert_eq!(up_proposal.evidence().underestimated_query_shapes, 3);
    assert_eq!(up_proposal.target_ratio(), CalibrationRatio::DOUBLE);
    assert_eq!(
        up_proposal.globally_bounded_ratio(),
        CalibrationRatio::DOUBLE
    );
    assert_eq!(up_proposal.proposed_ratio(), CalibrationRatio::NINE_EIGHTHS);

    let overestimated = systematic_window(
        &mut fixture.database,
        target,
        PlannerCalibrationEpoch(0),
        100,
        100,
        50,
    );
    let mut down_bounded = permissive_policy();
    down_bounded.global_min_ratio = CalibrationRatio::HALF;
    down_bounded.global_max_ratio = CalibrationRatio::DOUBLE;
    down_bounded.maximum_step_down_ratio = CalibrationRatio::NINE_EIGHTHS;
    let down = proposal(&fixture.database, &overestimated, down_bounded);
    assert_eq!(down.target_ratio(), CalibrationRatio::HALF);
    assert_eq!(
        down.proposed_ratio(),
        CalibrationRatio::new(8, 9).expect("down step")
    );

    let mut insufficient = bounded;
    insufficient.minimum_distinct_query_shapes = 4;
    assert_eq!(
        fixture
            .database
            .advise_planner_calibration(&window, PlannerCalibrationClass::Columnar, insufficient,)
            .unwrap(),
        PlannerCalibrationDecision::NoAction(PlannerCalibrationNoAction::InsufficientEvidence)
    );

    let repeated = calibration_report(
        &mut fixture.database,
        target,
        SHAPES[0],
        800,
        PlannerCalibrationEpoch(0),
        100,
        100,
        200,
    );
    let mut one_shape = AdaptiveWorkloadWindow::new(target, AdaptiveWorkloadLimits::default());
    for offset in 0..10_u64 {
        let mut sample = repeated.clone();
        sample.anchor.global_commit_seq = Some(DatabaseCommitSeq(800 + offset));
        one_shape.record(&sample).expect("repeat one shape");
    }
    let mut diversity = permissive_policy();
    diversity.minimum_samples = 10;
    diversity.minimum_distinct_visibility_points = 10;
    assert_eq!(
        fixture
            .database
            .advise_planner_calibration(&one_shape, PlannerCalibrationClass::Columnar, diversity,)
            .unwrap(),
        PlannerCalibrationDecision::NoAction(PlannerCalibrationNoAction::InsufficientEvidence)
    );

    let deadband_window = systematic_window(
        &mut fixture.database,
        target,
        PlannerCalibrationEpoch(0),
        100,
        100,
        101,
    );
    let mut deadband = permissive_policy();
    deadband.error_deadband_work_units = 3;
    assert_eq!(
        fixture
            .database
            .advise_planner_calibration(
                &deadband_window,
                PlannerCalibrationClass::Columnar,
                deadband,
            )
            .unwrap(),
        PlannerCalibrationDecision::NoAction(PlannerCalibrationNoAction::WithinDeadband)
    );

    let mut inconsistent = AdaptiveWorkloadWindow::new(target, AdaptiveWorkloadLimits::default());
    let directions = [200, 200, 50, 50];
    let sql = [
        "SELECT id FROM events",
        "SELECT id FROM events LIMIT 1",
        "SELECT id FROM events LIMIT 2",
        "SELECT id FROM events WHERE category = 2",
    ];
    for index in 0..4 {
        let report = calibration_report(
            &mut fixture.database,
            target,
            sql[index],
            200 + u64::try_from(index).unwrap(),
            PlannerCalibrationEpoch(0),
            100,
            100,
            directions[index],
        );
        inconsistent.record(&report).unwrap();
    }
    let mut inconsistent_policy = permissive_policy();
    inconsistent_policy.minimum_samples = 4;
    inconsistent_policy.minimum_distinct_visibility_points = 4;
    inconsistent_policy.minimum_distinct_query_shapes = 4;
    assert_eq!(
        fixture
            .database
            .advise_planner_calibration(
                &inconsistent,
                PlannerCalibrationClass::Columnar,
                inconsistent_policy,
            )
            .unwrap(),
        PlannerCalibrationDecision::NoAction(PlannerCalibrationNoAction::InconsistentEvidence)
    );
    fixture.close();
}

#[test]
fn shadow_rejects_insufficient_improvement_and_schema_stales_apply() {
    let mut fixture = TimelineFixture::create("phase4-shadow-schema");
    let target = workload_target(&fixture.database);
    let window = systematic_window(
        &mut fixture.database,
        target,
        PlannerCalibrationEpoch(0),
        100,
        100,
        110,
    );
    let mut policy = permissive_policy();
    policy.minimum_shadow_error_improvement_work_units = 1_000;
    let weak = proposal(&fixture.database, &window, policy);
    assert!(matches!(
        fixture.database.shadow_planner_calibration(&weak),
        PlannerCalibrationShadowDecision::Rejected {
            reason: PlannerCalibrationNoAction::NoShadowImprovement,
            ..
        }
    ));

    let strong = proposal(&fixture.database, &window, permissive_policy());
    let shadow = match fixture.database.shadow_planner_calibration(&strong) {
        PlannerCalibrationShadowDecision::Accepted(report) => report,
        other => panic!("expected accepted shadow, got {other:?}"),
    };
    fixture
        .database
        .execute("CREATE TABLE phase4_added (id BIGINT)")
        .expect("schema mutation");
    assert_eq!(
        fixture.database.apply_planner_calibration(&strong, &shadow),
        Err(PlannerCalibrationMutationError::StaleCalibrationProposal)
    );
    assert_eq!(
        fixture.database.planner_calibration_profile().epoch,
        PlannerCalibrationEpoch(0)
    );
    fixture.close();
}

#[test]
fn phase3_hysteresis_uses_base_alternative_and_suppression_is_calibration_independent() {
    let mut fixture = TimelineFixture::create("phase4-phase3-regression");
    let target = workload_target(&fixture.database);
    let calibration_window = systematic_window(
        &mut fixture.database,
        target,
        PlannerCalibrationEpoch(0),
        100,
        100,
        120,
    );
    let mut physical_reports = Vec::new();
    for (index, sql) in SHAPES.iter().enumerate() {
        let (_, mut report) = fixture
            .database
            .query_with_feedback(sql)
            .expect("query physical evidence before calibration");
        report.anchor.global_commit_seq =
            Some(DatabaseCommitSeq(300 + u64::try_from(index).unwrap()));
        set_target_work(&mut report, target, 8_000, 13_000, 10_000);
        physical_reports.push(report);
    }
    let proposal = proposal(&fixture.database, &calibration_window, permissive_policy());
    let shadow = match fixture.database.shadow_planner_calibration(&proposal) {
        PlannerCalibrationShadowDecision::Accepted(report) => report,
        other => panic!("expected shadow acceptance, got {other:?}"),
    };
    let receipt = fixture
        .database
        .apply_planner_calibration(&proposal, &shadow)
        .expect("apply calibration");

    let mut physical_window =
        AdaptiveWorkloadWindow::new(target, AdaptiveWorkloadLimits::default());
    for report in &physical_reports {
        physical_window
            .record(report)
            .expect("record physical evidence");
    }
    let g_before = fixture
        .database
        .current_database_snapshot()
        .unwrap()
        .unwrap()
        .commit_seq();
    let outcome = fixture
        .database
        .evaluate_adaptive_workload(
            &physical_window,
            AdaptiveWorkloadPolicy::new(3, 1, 3, 1_000, 1_000),
        )
        .expect("evaluate Phase 3");
    assert_eq!(
        outcome.outcome,
        AdaptiveWorkloadOutcome::RevertedMeasuredRegression
    );
    fixture
        .database
        .revert_planner_calibration(receipt)
        .expect("revert calibration only");
    let (_, after_revert) = fixture
        .database
        .query_with_feedback(SHAPES[0])
        .expect("query suppressed generation");
    assert!(!matches!(
        after_revert.plan_variant,
        netbadb_planner::PlanVariant::ColumnarScan { .. }
    ));
    assert_eq!(
        fixture
            .database
            .current_database_snapshot()
            .unwrap()
            .unwrap()
            .commit_seq(),
        g_before
    );
    fixture.close();
}

#[test]
fn source_alternative_keeps_base_semantics_beside_effective_seq_overlay() {
    let mut fixture = TimelineFixture::create("phase4-source-alternative");
    let target = workload_target(&fixture.database);
    let sql = [
        "SELECT payload FROM events",
        "SELECT payload FROM events LIMIT 10",
        "SELECT payload FROM events WHERE id > 10",
    ];
    let mut window = AdaptiveWorkloadWindow::new(target, AdaptiveWorkloadLimits::default());
    for (index, sql) in sql.iter().enumerate() {
        let report = class_report(
            &mut fixture.database,
            sql,
            500 + u64::try_from(index).unwrap(),
            PlannerCalibrationClass::SeqScan,
            PlannerCalibrationEpoch(0),
            100,
            100,
            200,
        );
        assert_eq!(
            window.record(&report).expect("record SeqScan evidence"),
            AdaptiveWorkloadRecordOutcome::RecordedNotRelevant
        );
    }
    let seq_proposal = match fixture
        .database
        .advise_planner_calibration(
            &window,
            PlannerCalibrationClass::SeqScan,
            permissive_policy(),
        )
        .expect("advise SeqScan calibration")
    {
        PlannerCalibrationDecision::Proposal(proposal) => *proposal,
        other => panic!("expected SeqScan proposal, got {other:?}"),
    };
    assert_eq!(seq_proposal.proposed_ratio(), CalibrationRatio::DOUBLE);
    let shadow = match fixture.database.shadow_planner_calibration(&seq_proposal) {
        PlannerCalibrationShadowDecision::Accepted(report) => report,
        other => panic!("expected accepted SeqScan shadow, got {other:?}"),
    };
    fixture
        .database
        .apply_planner_calibration(&seq_proposal, &shadow)
        .expect("apply SeqScan calibration");
    let (_, feedback) = fixture
        .database
        .query_with_feedback("SELECT id FROM events")
        .expect("columnar feedback with SeqScan overlay");
    let planner = feedback
        .accesses
        .iter()
        .find_map(|access| access.planner.as_ref())
        .expect("columnar planner evidence");
    assert_eq!(
        planner.kind.calibration_class(),
        PlannerCalibrationClass::Columnar
    );
    let base = planner
        .source_alternative_work_units
        .expect("base source alternative");
    assert_eq!(
        planner.effective_source_alternative_work_units,
        netbadb_planner::apply_calibration_ratio(base, CalibrationRatio::DOUBLE)
    );
    assert_eq!(feedback.calibration_epoch, PlannerCalibrationEpoch(1));
    fixture.close();
}

#[test]
fn reopen_resets_profile_without_changing_durable_rows() {
    let mut fixture = Fixture::create("phase4-reopen", true);
    let target = workload_target(&fixture.database);
    let window = systematic_window(
        &mut fixture.database,
        target,
        PlannerCalibrationEpoch(0),
        10,
        10,
        20,
    );
    let proposal = proposal(&fixture.database, &window, permissive_policy());
    let shadow = match fixture.database.shadow_planner_calibration(&proposal) {
        PlannerCalibrationShadowDecision::Accepted(report) => report,
        other => panic!("expected accepted shadow, got {other:?}"),
    };
    fixture
        .database
        .apply_planner_calibration(&proposal, &shadow)
        .expect("apply before reopen");
    let rows = fixture
        .database
        .query(SHAPES[0])
        .expect("rows before close");
    let root = fixture.root.clone();
    let source = fixture.source.clone();
    fixture.database.close().expect("close calibrated database");
    let mut reopened = Database::open_catalog(root.join("catalog")).expect("reopen database");
    assert_eq!(
        reopened.planner_calibration_profile(),
        netbadb_planner::PlannerCalibrationProfile::IDENTITY
    );
    assert_eq!(reopened.query(SHAPES[0]).expect("rows after reopen"), rows);
    reopened.close().expect("close reopened database");
    cleanup(&root, &[source]);
}

#[test]
fn incomplete_calibration_aggregate_never_proposes() {
    let mut fixture = TimelineFixture::create("phase4-incomplete");
    let target = workload_target(&fixture.database);
    let first = calibration_report(
        &mut fixture.database,
        target,
        SHAPES[0],
        600,
        PlannerCalibrationEpoch(0),
        u64::MAX,
        u64::MAX,
        u64::MAX,
    );
    let mut second = first.clone();
    second.anchor.global_commit_seq = Some(DatabaseCommitSeq(601));
    let mut overflowed = AdaptiveWorkloadWindow::new(target, AdaptiveWorkloadLimits::default());
    overflowed.record(&first).expect("first maximum sample");
    overflowed.record(&second).expect("overflowing sample");
    assert!(overflowed.overflowed);
    assert_eq!(
        fixture
            .database
            .advise_planner_calibration(
                &overflowed,
                PlannerCalibrationClass::Columnar,
                permissive_policy(),
            )
            .unwrap(),
        PlannerCalibrationDecision::NoAction(PlannerCalibrationNoAction::IncompleteEvidence)
    );

    let mut truncated = AdaptiveWorkloadWindow::new(target, AdaptiveWorkloadLimits::new(1, 8));
    let first = calibration_report(
        &mut fixture.database,
        target,
        SHAPES[0],
        700,
        PlannerCalibrationEpoch(0),
        100,
        100,
        200,
    );
    let second = calibration_report(
        &mut fixture.database,
        target,
        SHAPES[1],
        701,
        PlannerCalibrationEpoch(0),
        100,
        100,
        200,
    );
    truncated.record(&first).expect("bounded first shape");
    truncated.record(&second).expect("truncate second shape");
    assert!(truncated.truncated);
    assert_eq!(
        fixture
            .database
            .advise_planner_calibration(
                &truncated,
                PlannerCalibrationClass::Columnar,
                permissive_policy(),
            )
            .unwrap(),
        PlannerCalibrationDecision::NoAction(PlannerCalibrationNoAction::IncompleteEvidence)
    );
    fixture.close();
}

#[test]
fn ratio_and_epoch_extremes_are_typed_or_non_disruptive() {
    assert!(CalibrationRatio::new(0, 1).is_err());
    assert!(CalibrationRatio::new(1, 0).is_err());
    assert_eq!(
        netbadb_planner::apply_calibration_ratio(u64::MAX, CalibrationRatio::DOUBLE),
        None
    );
    assert_eq!(PlannerCalibrationEpoch(u64::MAX).checked_next(), None);
}
