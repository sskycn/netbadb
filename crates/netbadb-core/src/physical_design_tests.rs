use std::fs;

use netbadb_types::{ColumnId, DatabaseCommitSeq, SchemaGeneration};

use crate::execution_feedback_tests::{Fixture, TABLE_ID};
use crate::{
    ColumnarProjectionSpec, PhysicalDesignAdvisorError, PhysicalDesignAdvisorPolicy,
    PhysicalDesignCandidateDecision, PhysicalDesignEvidenceEpoch, PhysicalDesignEvidenceLimits,
    PhysicalDesignEvidenceRecordError, PhysicalDesignEvidenceRecordOutcome,
    PhysicalDesignEvidenceWindow, PhysicalDesignNoActionReason, PhysicalDesignRecommendationPolicy,
    PhysicalIndexCandidate, PlanVariant, QueryExpressionShape, QueryExpressionShapeKind,
};

fn policy(
    minimum_reports: u64,
    minimum_shapes: u64,
    minimum_work: u64,
    maximum: u32,
) -> PhysicalDesignAdvisorPolicy {
    let lane = PhysicalDesignRecommendationPolicy {
        minimum_reports,
        minimum_distinct_query_shapes: minimum_shapes,
        minimum_actual_scan_work_units: minimum_work,
        max_recommendations: maximum,
    };
    PhysicalDesignAdvisorPolicy {
        index: lane,
        columnar: lane,
    }
}

fn feedback(fixture: &mut Fixture, sql: &str) -> crate::ExecutionFeedbackReport {
    fixture
        .database
        .query_with_feedback(sql)
        .expect("query feedback")
        .1
}

#[test]
fn point_range_and_columnar_evidence_are_exact_separate_and_ranked() {
    let mut fixture = Fixture::create("phase20-point-range", false);
    let mut window = PhysicalDesignEvidenceWindow::new(PhysicalDesignEvidenceLimits::default());
    for sql in [
        "SELECT id FROM events WHERE category = 3",
        "SELECT id FROM events WHERE category >= 3",
    ] {
        let report = feedback(&mut fixture, sql);
        assert_eq!(
            window.record_execution_feedback(&report),
            Ok(PhysicalDesignEvidenceRecordOutcome::Recorded)
        );
    }

    let report = fixture
        .database
        .advise_physical_design(&window, policy(2, 2, 1, 8))
        .expect("advise");
    let index = report
        .index_candidates
        .iter()
        .find(|inspection| {
            inspection.candidate
                == (PhysicalIndexCandidate {
                    table_id: TABLE_ID,
                    column_id: ColumnId(2),
                })
        })
        .expect("category candidate");
    assert_eq!(index.point_report_count, 1);
    assert_eq!(index.range_report_count, 1);
    assert_eq!(index.evidence.report_count, 2);
    assert_eq!(index.evidence.distinct_query_shapes, 2);
    assert!(index.evidence.total_actual_scan_work_units > 0);
    assert_eq!(index.decision, PhysicalDesignCandidateDecision::Recommend);

    let columnar = report
        .columnar_candidates
        .iter()
        .find(|inspection| inspection.candidate.table_id == TABLE_ID)
        .expect("columnar candidate");
    assert_eq!(columnar.candidate.columns, vec![ColumnId(1), ColumnId(2)]);
    assert_eq!(columnar.evidence.report_count, 2);
    assert_eq!(
        columnar.decision,
        PhysicalDesignCandidateDecision::Recommend
    );
    assert_eq!(
        fixture
            .database
            .advise_physical_design(&window, policy(2, 2, 1, 8))
            .expect("repeat advice"),
        report
    );
    fixture.close();
}

#[test]
fn prepared_parameter_literal_and_reversed_range_follow_structural_semantics() {
    let mut fixture = Fixture::create("phase20-parameter-reversed", false);
    let prepared = fixture
        .database
        .prepare_statement(
            "SELECT id FROM events WHERE $1 <= category",
            &[Some(netbadb_types::PhysicalType::Int64)],
        )
        .expect("prepare reversed range");
    let executed = fixture
        .database
        .execute_prepared_with_feedback(&prepared, &[netbadb_types::ScalarValue::Int64(3)])
        .expect("execute reversed range");
    let parameter = executed
        .feedback
        .query_report()
        .expect("parameter feedback")
        .clone();
    let literal = feedback(&mut fixture, "SELECT id FROM events WHERE 3 <= category");

    let mut window = PhysicalDesignEvidenceWindow::default();
    window
        .record_execution_feedback(&parameter)
        .expect("record parameter shape");
    window
        .record_execution_feedback(&literal)
        .expect("record literal shape");
    let advice = fixture
        .database
        .advise_physical_design(&window, policy(2, 2, 1, 8))
        .expect("reversed range advice");
    let candidate = advice
        .index_candidates
        .iter()
        .find(|candidate| candidate.candidate.column_id == ColumnId(2))
        .expect("range candidate");
    assert_eq!(candidate.point_report_count, 0);
    assert_eq!(candidate.range_report_count, 2);
    assert_eq!(candidate.evidence.distinct_query_shapes, 2);
    assert_eq!(
        candidate.decision,
        PhysicalDesignCandidateDecision::Recommend
    );
    fixture.close();
}

#[test]
fn conservative_predicate_subset_excludes_or_and_not_equal() {
    let mut fixture = Fixture::create("phase20-exclusions", false);
    for sql in [
        "SELECT id FROM events WHERE category != 3",
        "SELECT id FROM events WHERE category = 3 OR id = 4",
    ] {
        let mut window = PhysicalDesignEvidenceWindow::new(PhysicalDesignEvidenceLimits::default());
        let report = feedback(&mut fixture, sql);
        window
            .record_execution_feedback(&report)
            .expect("record excluded predicate");
        let advice = fixture
            .database
            .advise_physical_design(&window, policy(1, 1, 0, 8))
            .expect("inspect excluded predicate");
        assert!(advice.index_candidates.is_empty());
    }

    let mut cast = feedback(&mut fixture, "SELECT id FROM events WHERE category = 3");
    assert!(cast_first_filter_column(&mut cast.plan_variant));
    let mut cast_window = PhysicalDesignEvidenceWindow::default();
    cast_window
        .record_execution_feedback(&cast)
        .expect("record structural cast");
    assert!(
        fixture
            .database
            .advise_physical_design(&cast_window, policy(1, 1, 0, 8))
            .expect("cast advice")
            .index_candidates
            .is_empty()
    );
    fixture.close();
}

fn cast_first_filter_column(plan: &mut PlanVariant) -> bool {
    match plan {
        PlanVariant::Filter { input, predicate } => {
            let QueryExpressionShapeKind::Binary { left, right, .. } = &mut predicate.kind else {
                return cast_first_filter_column(input);
            };
            let target = if matches!(&left.kind, QueryExpressionShapeKind::Column(_)) {
                left
            } else if matches!(&right.kind, QueryExpressionShapeKind::Column(_)) {
                right
            } else {
                return false;
            };
            let original = target.as_ref().clone();
            **target = QueryExpressionShape {
                expr_type: original.expr_type.clone(),
                kind: QueryExpressionShapeKind::Cast {
                    expression: Box::new(original),
                },
            };
            true
        }
        PlanVariant::NestedLoopJoin { left, right, .. }
        | PlanVariant::HashJoin { left, right, .. } => {
            cast_first_filter_column(left) || cast_first_filter_column(right)
        }
        PlanVariant::IndexNestedLoopJoin { left, .. }
        | PlanVariant::Sort { input: left, .. }
        | PlanVariant::Project { input: left, .. }
        | PlanVariant::ScalarProject { input: left, .. }
        | PlanVariant::Aggregate { input: left, .. }
        | PlanVariant::Limit { input: left, .. } => cast_first_filter_column(left),
        PlanVariant::OneRow
        | PlanVariant::SeqScan { .. }
        | PlanVariant::ColumnarScan { .. }
        | PlanVariant::IndexPoint { .. }
        | PlanVariant::IndexRange { .. }
        | PlanVariant::PartitionedAccess { .. } => false,
    }
}

#[test]
fn self_join_and_already_indexed_access_do_not_create_missing_index_evidence() {
    let mut fixture = Fixture::create("phase20-self-indexed", false);
    let self_join = feedback(
        &mut fixture,
        "SELECT a.id FROM events a JOIN events b ON a.id = b.id WHERE a.category = 3",
    );
    let mut self_join_window = PhysicalDesignEvidenceWindow::default();
    self_join_window
        .record_execution_feedback(&self_join)
        .expect("record self join");
    let self_join_advice = fixture
        .database
        .advise_physical_design(&self_join_window, policy(1, 1, 0, 8))
        .expect("self join advice");
    assert!(self_join_advice.index_candidates.is_empty());
    assert!(self_join_advice.columnar_candidates.is_empty());

    fixture
        .database
        .create_index(TABLE_ID, ColumnId(2))
        .expect("create category index");
    let indexed = feedback(&mut fixture, "SELECT id FROM events WHERE category = 3");
    assert!(
        indexed
            .accesses
            .iter()
            .any(|access| { access.actual.kind == crate::ExecutionAccessKind::IndexPoint })
    );
    let mut indexed_window = PhysicalDesignEvidenceWindow::default();
    indexed_window
        .record_execution_feedback(&indexed)
        .expect("record indexed access");
    let indexed_advice = fixture
        .database
        .advise_physical_design(&indexed_window, policy(1, 1, 0, 8))
        .expect("indexed advice");
    assert!(indexed_advice.index_candidates.is_empty());
    assert!(indexed_advice.columnar_candidates.is_empty());
    fixture.close();
}

#[test]
fn and_yields_independent_single_column_candidates_and_limit_keeps_inspections() {
    let mut fixture = Fixture::create("phase20-and-limit", false);
    let report = feedback(
        &mut fixture,
        "SELECT payload FROM events WHERE id = 7 AND category >= 2",
    );
    let mut window = PhysicalDesignEvidenceWindow::new(PhysicalDesignEvidenceLimits::default());
    window
        .record_execution_feedback(&report)
        .expect("record AND feedback");
    let advice = fixture
        .database
        .advise_physical_design(&window, policy(1, 1, 0, 1))
        .expect("bounded advice");
    assert_eq!(advice.index_candidates.len(), 2);
    assert_eq!(
        advice
            .index_candidates
            .iter()
            .filter(|candidate| candidate.decision == PhysicalDesignCandidateDecision::Recommend)
            .count(),
        1
    );
    assert!(advice.index_candidates.iter().any(|candidate| {
        candidate.decision
            == PhysicalDesignCandidateDecision::NoAction(
                PhysicalDesignNoActionReason::RecommendationLimitReached,
            )
    }));
    fixture.close();
}

#[test]
fn current_index_and_projection_inventory_suppress_stale_missing_design_evidence() {
    let mut fixture = Fixture::create("phase20-current-inventory", false);
    let report = feedback(&mut fixture, "SELECT id FROM events WHERE category = 3");
    let mut window = PhysicalDesignEvidenceWindow::new(PhysicalDesignEvidenceLimits::default());
    window
        .record_execution_feedback(&report)
        .expect("record old scan evidence");

    fixture
        .database
        .create_index(TABLE_ID, ColumnId(2))
        .expect("create current index");
    fixture
        .database
        .build_columnar_projection(ColumnarProjectionSpec::new(
            TABLE_ID,
            fixture.root.join("phase20-projection"),
            vec![ColumnId(1), ColumnId(2), ColumnId(3)],
        ))
        .expect("build covering projection");

    let advice = fixture
        .database
        .advise_physical_design(&window, policy(1, 1, 0, 8))
        .expect("current inventory advice");
    assert_eq!(
        advice.index_candidates[0].decision,
        PhysicalDesignCandidateDecision::NoAction(
            PhysicalDesignNoActionReason::ExistingDesignCovers,
        )
    );
    assert_eq!(
        advice.columnar_candidates[0].decision,
        PhysicalDesignCandidateDecision::NoAction(
            PhysicalDesignNoActionReason::ExistingDesignCovers,
        )
    );
    fixture.close();
}

#[test]
fn lifecycle_requires_global_order_rotates_schema_and_retains_high_water() {
    let mut fixture = Fixture::create("phase20-lifecycle", false);
    let base = feedback(&mut fixture, "SELECT id FROM events WHERE category = 3");
    let mut window = PhysicalDesignEvidenceWindow::new(PhysicalDesignEvidenceLimits::default());

    let mut no_global = base.clone();
    no_global.anchor.global_commit_seq = None;
    assert_eq!(
        window.record_execution_feedback(&no_global),
        Err(PhysicalDesignEvidenceRecordError::GlobalVisibilityRequired)
    );

    for visibility in [10, 10, 11] {
        let mut ordered = base.clone();
        ordered.anchor.global_commit_seq = Some(DatabaseCommitSeq(visibility));
        window
            .record_execution_feedback(&ordered)
            .expect("nondecreasing G");
    }
    let mut older = base.clone();
    older.anchor.global_commit_seq = Some(DatabaseCommitSeq(10));
    assert_eq!(
        window.record_execution_feedback(&older),
        Err(PhysicalDesignEvidenceRecordError::OutOfOrderVisibility {
            previous: DatabaseCommitSeq(11),
            received: DatabaseCommitSeq(10),
        })
    );

    let base_schema = base.anchor.schema_generation;
    let mut newer = base.clone();
    newer.anchor.global_commit_seq = Some(DatabaseCommitSeq(12));
    newer.anchor.schema_generation = SchemaGeneration(base_schema.0 + 1);
    assert_eq!(
        window.record_execution_feedback(&newer),
        Ok(PhysicalDesignEvidenceRecordOutcome::SchemaRotated)
    );
    let rotated = window.inspection();
    assert_eq!(rotated.epoch, PhysicalDesignEvidenceEpoch(1));
    assert_eq!(rotated.recorded_reports, 1);
    assert_eq!(
        rotated.schema_generation,
        Some(newer.anchor.schema_generation)
    );

    let mut stale = base.clone();
    stale.anchor.global_commit_seq = Some(DatabaseCommitSeq(13));
    assert!(matches!(
        window.record_execution_feedback(&stale),
        Err(PhysicalDesignEvidenceRecordError::StaleSchemaEvidence { .. })
    ));

    assert_eq!(
        window.rotate_window().expect("explicit rotate"),
        PhysicalDesignEvidenceEpoch(2)
    );
    assert_eq!(window.inspection().recorded_reports, 0);
    let mut pre_rotation = newer;
    pre_rotation.anchor.global_commit_seq = Some(DatabaseCommitSeq(11));
    assert!(matches!(
        window.record_execution_feedback(&pre_rotation),
        Err(PhysicalDesignEvidenceRecordError::OutOfOrderVisibility { .. })
    ));
    fixture.close();
}

#[test]
fn incomplete_and_capacity_truncated_evidence_cannot_recommend() {
    let mut fixture = Fixture::create("phase20-incomplete-capacity", false);
    let report = feedback(
        &mut fixture,
        "SELECT payload FROM events WHERE id = 7 AND category = 2",
    );

    let mut incomplete_window =
        PhysicalDesignEvidenceWindow::new(PhysicalDesignEvidenceLimits::default());
    let mut incomplete = report.clone();
    incomplete.incomplete = true;
    incomplete_window
        .record_execution_feedback(&incomplete)
        .expect("record diagnostic only");
    let advice = fixture
        .database
        .advise_physical_design(&incomplete_window, policy(1, 1, 0, 8))
        .expect("empty positive evidence report");
    assert!(advice.index_candidates.is_empty());
    assert!(advice.columnar_candidates.is_empty());
    assert_eq!(advice.discarded_incomplete_reports, 1);

    let limits = PhysicalDesignEvidenceLimits {
        max_index_candidates: 1,
        ..PhysicalDesignEvidenceLimits::default()
    };
    let mut truncated = PhysicalDesignEvidenceWindow::new(limits);
    assert_eq!(
        truncated.record_execution_feedback(&report),
        Ok(PhysicalDesignEvidenceRecordOutcome::RecordedWithCapacityRejection)
    );
    assert!(matches!(
        fixture
            .database
            .advise_physical_design(&truncated, policy(1, 1, 0, 8)),
        Err(PhysicalDesignAdvisorError::InconclusiveCapacity { .. })
    ));
    fixture.close();
}

#[test]
fn shape_capacity_and_counter_overflow_are_fail_closed() {
    let mut fixture = Fixture::create("phase20-shape-overflow", false);
    let point = feedback(&mut fixture, "SELECT id FROM events WHERE category = 2");
    let range = feedback(&mut fixture, "SELECT id FROM events WHERE category >= 2");
    let limits = PhysicalDesignEvidenceLimits {
        max_query_shapes_per_candidate: 1,
        ..PhysicalDesignEvidenceLimits::default()
    };
    let mut shape_limited = PhysicalDesignEvidenceWindow::new(limits);
    shape_limited
        .record_execution_feedback(&point)
        .expect("record first shape");
    assert_eq!(
        shape_limited.record_execution_feedback(&range),
        Ok(PhysicalDesignEvidenceRecordOutcome::RecordedWithCapacityRejection)
    );
    assert!(matches!(
        fixture
            .database
            .advise_physical_design(&shape_limited, policy(1, 1, 0, 8)),
        Err(PhysicalDesignAdvisorError::InconclusiveCapacity { .. })
    ));

    let mut maximum = point.clone();
    for access in &mut maximum.accesses {
        if let Some(calibration) = &mut access.calibration {
            calibration.actual_work_units = Some(u64::MAX);
        }
    }
    let mut overflowed = PhysicalDesignEvidenceWindow::default();
    overflowed
        .record_execution_feedback(&maximum)
        .expect("record maximum work");
    overflowed
        .record_execution_feedback(&maximum)
        .expect("record overflowing work");
    let advice = fixture
        .database
        .advise_physical_design(&overflowed, policy(1, 1, 0, 8))
        .expect("overflow remains inspectable");
    assert!(advice.overflowed);
    assert!(advice.index_candidates.iter().all(|candidate| {
        candidate.decision
            == PhysicalDesignCandidateDecision::NoAction(
                PhysicalDesignNoActionReason::IncompleteEvidence,
            )
    }));
    fixture.close();
}

#[test]
fn empty_stale_and_full_row_evidence_are_fail_closed() {
    let mut fixture = Fixture::create("phase20-empty-stale-full", false);
    let empty = PhysicalDesignEvidenceWindow::default();
    assert!(matches!(
        fixture
            .database
            .advise_physical_design(&empty, policy(1, 1, 0, 8)),
        Err(PhysicalDesignAdvisorError::NoEvidence)
    ));

    let mut stale_report = feedback(&mut fixture, "SELECT id FROM events");
    stale_report.anchor.schema_generation =
        SchemaGeneration(stale_report.anchor.schema_generation.0.saturating_add(1));
    let mut stale = PhysicalDesignEvidenceWindow::default();
    stale
        .record_execution_feedback(&stale_report)
        .expect("record future schema evidence");
    assert!(matches!(
        fixture
            .database
            .advise_physical_design(&stale, policy(1, 1, 0, 8)),
        Err(PhysicalDesignAdvisorError::StaleSchema { .. })
    ));

    let full_row = feedback(&mut fixture, "SELECT id, category, payload FROM events");
    let mut full_row_window = PhysicalDesignEvidenceWindow::default();
    full_row_window
        .record_execution_feedback(&full_row)
        .expect("record full-row scan");
    let advice = fixture
        .database
        .advise_physical_design(&full_row_window, policy(1, 1, 0, 8))
        .expect("inspect full-row scan");
    assert_eq!(advice.columnar_candidates.len(), 1);
    assert_eq!(
        advice.columnar_candidates[0].decision,
        PhysicalDesignCandidateDecision::NoAction(
            PhysicalDesignNoActionReason::UnsupportedCurrentLayout,
        )
    );
    fixture.close();
}

#[test]
fn advising_is_read_only_and_creates_no_files() {
    let mut fixture = Fixture::create("phase20-purity", false);
    let report = feedback(&mut fixture, "SELECT id FROM events WHERE category = 3");
    let mut window = PhysicalDesignEvidenceWindow::new(PhysicalDesignEvidenceLimits::default());
    window
        .record_execution_feedback(&report)
        .expect("record evidence");

    let g_before = fixture
        .database
        .current_database_snapshot()
        .expect("snapshot")
        .expect("global")
        .commit_seq();
    let schema_before = fixture.database.schema_generation();
    let catalog_before = fixture.database.inspect_catalog().expect("catalog");
    let projections_before = fixture.database.inspect_columnar_projections();
    let projection_catalog_before = fixture.database.inspect_columnar_projection_catalog();
    let calibration_before = fixture.database.planner_calibration_profile();
    let safe_mode_before = fixture.database.automatic_safe_mode_state();
    let files_before = fs::read_dir(&fixture.root)
        .expect("read root")
        .map(|entry| entry.expect("entry").file_name())
        .collect::<Vec<_>>();

    fixture
        .database
        .advise_physical_design(&window, policy(1, 1, 0, 8))
        .expect("read-only advice");

    assert_eq!(fixture.database.schema_generation(), schema_before);
    assert_eq!(
        fixture
            .database
            .current_database_snapshot()
            .expect("snapshot after")
            .expect("global after")
            .commit_seq(),
        g_before
    );
    assert_eq!(
        fixture.database.inspect_catalog().expect("catalog after"),
        catalog_before
    );
    assert_eq!(
        fixture.database.inspect_columnar_projections(),
        projections_before
    );
    assert_eq!(
        fixture.database.inspect_columnar_projection_catalog(),
        projection_catalog_before
    );
    assert_eq!(
        fixture.database.planner_calibration_profile(),
        calibration_before
    );
    assert_eq!(
        fixture.database.automatic_safe_mode_state(),
        safe_mode_before
    );
    assert_eq!(
        fs::read_dir(&fixture.root)
            .expect("read root after")
            .map(|entry| entry.expect("entry after").file_name())
            .collect::<Vec<_>>(),
        files_before
    );
    fixture.close();
}
