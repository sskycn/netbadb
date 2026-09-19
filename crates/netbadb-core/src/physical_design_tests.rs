use std::fs;
use std::os::unix::fs::PermissionsExt;

use netbadb_types::{ColumnId, DatabaseCommitSeq, SchemaGeneration};

use crate::execution_feedback_tests::{Fixture, TABLE_ID};
use crate::{
    ColumnarAdvanceBudget, ColumnarProjectionHealth, ColumnarProjectionSpec, DatabaseError,
    PhysicalColumnarCandidate, PhysicalColumnarDesignApplyError,
    PhysicalColumnarDesignApplyOutcome, PhysicalColumnarDesignLocationState,
    PhysicalColumnarDesignMode, PhysicalColumnarDesignProposalError,
    PhysicalColumnarDesignProposalStaleReason, PhysicalDesignAdvisorError,
    PhysicalDesignAdvisorPolicy, PhysicalDesignCandidateDecision, PhysicalDesignEvidenceEpoch,
    PhysicalDesignEvidenceLimits, PhysicalDesignEvidenceRecordError,
    PhysicalDesignEvidenceRecordOutcome, PhysicalDesignEvidenceWindow,
    PhysicalDesignNoActionReason, PhysicalDesignRecommendationPolicy, PhysicalIndexCandidate,
    PhysicalIndexDesignApplyError, PhysicalIndexDesignApplyOutcome, PhysicalIndexDesignNameState,
    PhysicalIndexDesignProposalError, PlanVariant, ProjectionCatalogError, QueryExpressionShape,
    QueryExpressionShapeKind,
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

fn index_candidate() -> PhysicalIndexCandidate {
    PhysicalIndexCandidate {
        table_id: TABLE_ID,
        column_id: ColumnId(2),
    }
}

fn columnar_candidate() -> PhysicalColumnarCandidate {
    PhysicalColumnarCandidate {
        table_id: TABLE_ID,
        columns: vec![ColumnId(1), ColumnId(2)],
    }
}

fn columnar_window(fixture: &mut Fixture) -> PhysicalDesignEvidenceWindow {
    let mut window = PhysicalDesignEvidenceWindow::default();
    let report = feedback(
        fixture,
        "SELECT id, category FROM events WHERE category >= 3",
    );
    window
        .record_execution_feedback(&report)
        .expect("record columnar evidence");
    window
}

fn make_catalog_parent_read_only(database: &mut crate::Database) -> Result<(), DatabaseError> {
    let catalog = database
        .catalog_path
        .as_ref()
        .expect("managed schema catalog path");
    let parent = catalog.parent().expect("schema catalog parent");
    let mut permissions = fs::metadata(parent)
        .expect("catalog parent metadata")
        .permissions();
    permissions.set_mode(0o555);
    fs::set_permissions(parent, permissions).expect("make catalog parent read-only");
    Ok(())
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
    assert!(advice.columnar_candidates.is_empty());
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

#[test]
fn explicit_index_proposal_applies_through_named_create_and_is_idempotent() {
    let mut fixture = Fixture::create("phase23-create-retry", false);
    let report = feedback(&mut fixture, "SELECT id FROM events WHERE category = 3");
    let mut window = PhysicalDesignEvidenceWindow::default();
    window
        .record_execution_feedback(&report)
        .expect("record recommendation evidence");
    let candidate = index_candidate();
    let proposal = fixture
        .database
        .propose_physical_index_design(&window, policy(1, 1, 0, 8), candidate)
        .expect("proposal");
    let g_before = fixture
        .database
        .current_database_snapshot()
        .expect("snapshot")
        .expect("global visibility")
        .commit_seq();
    fixture
        .database
        .insert(&[
            netbadb_types::ScalarValue::Int64(513),
            netbadb_types::ScalarValue::Int64(3),
            netbadb_types::ScalarValue::Text("post-proposal-dml".into()),
        ])
        .expect("ordinary DML after proposal");
    let extra_report = feedback(&mut fixture, "SELECT id FROM events WHERE category = 3");
    window
        .record_execution_feedback(&extra_report)
        .expect("same-epoch evidence");
    let name = netbadb_types::IndexName::new("events_category_phase23_idx").unwrap();
    let applied = fixture
        .database
        .apply_physical_index_design(&window, &proposal, name.clone())
        .expect("apply");
    let index_id = match applied.outcome {
        PhysicalIndexDesignApplyOutcome::Created { index_id } => index_id,
        other => panic!("unexpected apply outcome: {other:?}"),
    };
    assert!(applied.global_commit_seq_after > g_before);
    assert_eq!(
        applied.schema_generation,
        fixture.database.schema_generation()
    );
    assert_eq!(fixture.database.indexes(TABLE_ID).unwrap().len(), 1);
    assert_eq!(
        fixture.database.indexes(TABLE_ID).unwrap()[0].name,
        Some(name.clone())
    );
    assert_eq!(
        fixture.database.indexes(TABLE_ID).unwrap()[0].column_id,
        ColumnId(2)
    );

    let retry = fixture
        .database
        .apply_physical_index_design(&window, &proposal, name)
        .expect("idempotent retry");
    assert_eq!(
        retry.outcome,
        PhysicalIndexDesignApplyOutcome::AlreadyApplied { index_id }
    );
    assert_eq!(
        retry.global_commit_seq_before,
        applied.global_commit_seq_after
    );
    assert_eq!(
        retry.global_commit_seq_after,
        applied.global_commit_seq_after
    );
    assert_eq!(fixture.database.indexes(TABLE_ID).unwrap().len(), 1);
    fixture.close();
}

#[test]
fn retained_proposal_recognizes_created_index_after_reopen() {
    let fixture = Fixture::create("phase23-reopen", false);
    let Fixture {
        root,
        source,
        mut database,
    } = fixture;
    let report = database
        .query_with_feedback("SELECT id FROM events WHERE category >= 3")
        .expect("query feedback")
        .1;
    let mut window = PhysicalDesignEvidenceWindow::default();
    window
        .record_execution_feedback(&report)
        .expect("record evidence");
    let proposal = database
        .propose_physical_index_design(&window, policy(1, 1, 0, 8), index_candidate())
        .expect("proposal");
    let name = netbadb_types::IndexName::new("events_category_reopen_idx").unwrap();
    let created = database
        .apply_physical_index_design(&window, &proposal, name.clone())
        .expect("create index");
    assert!(matches!(
        created.outcome,
        PhysicalIndexDesignApplyOutcome::Created { .. }
    ));
    database.close().expect("close database");

    let mut reopened = crate::Database::open_catalog(root.join("catalog")).expect("reopen");
    let retry = reopened
        .apply_physical_index_design(&window, &proposal, name)
        .expect("retry after reopen");
    assert!(matches!(
        retry.outcome,
        PhysicalIndexDesignApplyOutcome::AlreadyApplied { .. }
    ));
    reopened.close().expect("close reopened database");
    crate::cleanup_created_table_files(std::slice::from_ref(&source));
    let _ = fs::remove_dir_all(root);
}

#[test]
fn proposal_requires_recommendation_and_apply_revalidates_epoch_and_identity() {
    let mut fixture = Fixture::create("phase23-revalidation", false);
    let report = feedback(&mut fixture, "SELECT id FROM events WHERE category = 3");
    let mut window = PhysicalDesignEvidenceWindow::default();
    window
        .record_execution_feedback(&report)
        .expect("record evidence");
    let candidate = index_candidate();
    assert!(matches!(
        fixture.database.propose_physical_index_design(
            &window,
            policy(1, 1, 0, 8),
            PhysicalIndexCandidate {
                table_id: TABLE_ID,
                column_id: ColumnId(1),
            },
        ),
        Err(PhysicalIndexDesignProposalError::CandidateNotObserved(_))
    ));
    assert!(matches!(
        fixture
            .database
            .propose_physical_index_design(&window, policy(2, 1, 0, 8), candidate),
        Err(PhysicalIndexDesignProposalError::CandidateNotRecommended { .. })
    ));
    let proposal = fixture
        .database
        .propose_physical_index_design(&window, policy(1, 1, 0, 8), candidate)
        .expect("proposal");
    window.rotate_window().expect("rotate evidence");
    assert!(matches!(
        fixture.database.apply_physical_index_design(
            &window,
            &proposal,
            netbadb_types::IndexName::new("events_category_epoch_idx").unwrap(),
        ),
        Err(PhysicalIndexDesignApplyError::EvidenceEpochChanged { .. })
    ));
    fixture.close();
}

#[test]
fn apply_recognizes_coverage_and_name_conflicts_before_mutation() {
    let mut fixture = Fixture::create("phase23-preflight", false);
    let report = feedback(&mut fixture, "SELECT id FROM events WHERE category = 3");
    let mut window = PhysicalDesignEvidenceWindow::default();
    window
        .record_execution_feedback(&report)
        .expect("record evidence");
    let proposal = fixture
        .database
        .propose_physical_index_design(&window, policy(1, 1, 0, 8), index_candidate())
        .expect("proposal");
    fixture
        .database
        .create_named_index(
            netbadb_types::IndexName::new("events_id_conflict").unwrap(),
            TABLE_ID,
            ColumnId(1),
        )
        .expect("create conflicting name target");
    let before = fixture
        .database
        .current_database_snapshot()
        .unwrap()
        .unwrap()
        .commit_seq();
    assert!(matches!(
        fixture.database.apply_physical_index_design(
            &window,
            &proposal,
            netbadb_types::IndexName::new("events_id_conflict").unwrap(),
        ),
        Err(PhysicalIndexDesignApplyError::IndexNameConflict(_))
    ));
    assert_eq!(fixture.database.indexes(TABLE_ID).unwrap().len(), 1);
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
        .create_named_index(
            netbadb_types::IndexName::new("events_category_existing").unwrap(),
            TABLE_ID,
            ColumnId(2),
        )
        .expect("create covering design");
    let covered = fixture
        .database
        .apply_physical_index_design(
            &window,
            &proposal,
            netbadb_types::IndexName::new("events_category_new").unwrap(),
        )
        .expect("covered apply");
    assert_eq!(
        covered.outcome,
        PhysicalIndexDesignApplyOutcome::AlreadyCovered
    );
    assert_eq!(fixture.database.indexes(TABLE_ID).unwrap().len(), 2);
    fixture.close();
}

#[test]
fn current_physical_index_name_classification_is_active_only_and_pure() {
    let mut fixture = Fixture::create("phase25-name-classification", false);
    let candidate = index_candidate();
    let name = netbadb_types::IndexName::new("events_category_name_state").unwrap();
    let before = fixture
        .database
        .current_database_snapshot()
        .unwrap()
        .unwrap()
        .commit_seq();
    assert_eq!(
        fixture
            .database
            .inspect_physical_index_design_name(candidate, &name),
        PhysicalIndexDesignNameState::Available
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

    let definition = fixture
        .database
        .create_named_index(name.clone(), TABLE_ID, ColumnId(2))
        .unwrap();
    assert_eq!(
        fixture
            .database
            .inspect_physical_index_design_name(candidate, &name),
        PhysicalIndexDesignNameState::AlreadyApplied {
            index_id: definition.id
        }
    );
    assert_eq!(
        fixture.database.inspect_physical_index_design_name(
            PhysicalIndexCandidate {
                table_id: TABLE_ID,
                column_id: ColumnId(1),
            },
            &name,
        ),
        PhysicalIndexDesignNameState::Conflict
    );
    assert_eq!(
        fixture.database.inspect_physical_index_design_name(
            PhysicalIndexCandidate {
                table_id: netbadb_types::TableId(TABLE_ID.0 + 1),
                column_id: ColumnId(2),
            },
            &name,
        ),
        PhysicalIndexDesignNameState::Conflict
    );

    fixture
        .database
        .drop_index(TABLE_ID, definition.id)
        .unwrap();
    assert_eq!(
        fixture
            .database
            .inspect_physical_index_design_name(candidate, &name),
        PhysicalIndexDesignNameState::Available
    );
    fixture.close();
}

#[test]
fn proposal_is_bound_to_durable_database_incarnation() {
    let mut first = Fixture::create("phase23-identity-a", false);
    let mut second = Fixture::create("phase23-identity-b", false);
    let report = feedback(&mut first, "SELECT id FROM events WHERE category = 3");
    let mut window = PhysicalDesignEvidenceWindow::default();
    window
        .record_execution_feedback(&report)
        .expect("record evidence");
    let proposal = first
        .database
        .propose_physical_index_design(&window, policy(1, 1, 0, 8), index_candidate())
        .expect("proposal");
    let before = second
        .database
        .current_database_snapshot()
        .unwrap()
        .unwrap()
        .commit_seq();
    assert!(matches!(
        second.database.apply_physical_index_design(
            &window,
            &proposal,
            netbadb_types::IndexName::new("events_category_foreign_idx").unwrap(),
        ),
        Err(PhysicalIndexDesignApplyError::DatabaseIdentityChanged)
    ));
    assert!(second.database.indexes(TABLE_ID).unwrap().is_empty());
    assert_eq!(
        second
            .database
            .current_database_snapshot()
            .unwrap()
            .unwrap()
            .commit_seq(),
        before
    );
    first.close();
    second.close();
}

#[test]
fn columnar_proposals_are_pure_exact_and_mode_explicit() {
    let mut fixture = Fixture::create("phase27-proposal-purity", false);
    let window = columnar_window(&mut fixture);
    let candidate = columnar_candidate();
    let snapshot_directory = fixture.root.join("approved-snapshot");
    let incremental_directory = fixture.root.join("approved-incremental");
    let inspection_before = fixture.database.inspect_columnar_projection_catalog();
    let visibility_before = fixture
        .database
        .current_database_snapshot()
        .unwrap()
        .unwrap()
        .commit_seq();
    let schema_before = fixture.database.schema_generation();
    let evidence_before = window.clone();

    assert!(matches!(
        fixture.database.propose_physical_columnar_design(
            &window,
            policy(1, 1, 0, 8),
            PhysicalColumnarCandidate {
                table_id: TABLE_ID,
                columns: vec![ColumnId(2), ColumnId(1)],
            },
            PhysicalColumnarDesignMode::Snapshot,
            &snapshot_directory,
        ),
        Err(PhysicalColumnarDesignProposalError::CandidateNotObserved(_))
    ));
    assert!(matches!(
        fixture.database.propose_physical_columnar_design(
            &window,
            policy(2, 1, 0, 8),
            candidate.clone(),
            PhysicalColumnarDesignMode::Snapshot,
            &snapshot_directory,
        ),
        Err(PhysicalColumnarDesignProposalError::CandidateNotRecommended { .. })
    ));
    assert!(matches!(
        fixture.database.propose_physical_columnar_design(
            &window,
            policy(1, 1, 0, 8),
            candidate.clone(),
            PhysicalColumnarDesignMode::Incremental,
            &incremental_directory,
        ),
        Err(PhysicalColumnarDesignProposalError::IncrementalChangeStreamNotEnabled { .. })
    ));

    let snapshot = fixture
        .database
        .propose_physical_columnar_design(
            &window,
            policy(1, 1, 0, 8),
            candidate.clone(),
            PhysicalColumnarDesignMode::Snapshot,
            &snapshot_directory,
        )
        .expect("snapshot proposal without stream");
    assert_eq!(snapshot.candidate(), &candidate);
    assert_eq!(snapshot.mode(), PhysicalColumnarDesignMode::Snapshot);
    assert_eq!(
        snapshot.directory(),
        crate::schema_catalog_file::absolute(&snapshot_directory)
            .unwrap()
            .as_path()
    );
    assert!(snapshot.directory().is_absolute());
    assert_eq!(snapshot.change_stream_generation(), None);
    assert!(!snapshot_directory.exists());

    let cursor = fixture
        .database
        .enable_change_stream(TABLE_ID)
        .expect("explicitly enable stream");
    let incremental = fixture
        .database
        .propose_physical_columnar_design(
            &window,
            policy(1, 1, 0, 8),
            candidate,
            PhysicalColumnarDesignMode::Incremental,
            &incremental_directory,
        )
        .expect("incremental proposal with stream");
    assert_eq!(
        incremental.change_stream_generation(),
        Some(cursor.generation)
    );
    assert!(!incremental_directory.exists());
    assert_eq!(window, evidence_before);
    assert_eq!(fixture.database.schema_generation(), schema_before);
    assert_eq!(
        fixture
            .database
            .current_database_snapshot()
            .unwrap()
            .unwrap()
            .commit_seq(),
        visibility_before
    );
    assert_eq!(
        fixture
            .database
            .inspect_columnar_projection_catalog()
            .next_projection_id,
        inspection_before.next_projection_id
    );
    assert!(fixture.database.inspect_columnar_projections().is_empty());
    fixture.close();
}

#[test]
fn snapshot_columnar_apply_is_exact_idempotent_and_visibility_neutral() {
    let mut fixture = Fixture::create("phase27-snapshot-apply", false);
    let mut window = columnar_window(&mut fixture);
    let candidate = columnar_candidate();
    let directory = fixture.root.join("snapshot-design");
    let proposal = fixture
        .database
        .propose_physical_columnar_design(
            &window,
            policy(1, 1, 0, 8),
            candidate.clone(),
            PhysicalColumnarDesignMode::Snapshot,
            &directory,
        )
        .expect("snapshot proposal");
    fixture
        .database
        .execute("INSERT INTO events VALUES (8999, 2, 'before-apply')")
        .expect("ordinary DML before apply");
    let additional = feedback(
        &mut fixture,
        "SELECT id, category FROM events WHERE category >= 2",
    );
    window
        .record_execution_feedback(&additional)
        .expect("same-epoch additional evidence");
    assert_eq!(window.epoch(), proposal.evidence_epoch());
    let schema_before = fixture.database.schema_generation();
    let g_before = fixture
        .database
        .current_database_snapshot()
        .unwrap()
        .unwrap()
        .commit_seq();
    assert!(g_before > proposal.proposed_at_global_commit_seq());
    let created = fixture
        .database
        .apply_physical_columnar_design(&window, &proposal)
        .expect("apply snapshot proposal");
    let projection_id = match created.outcome {
        PhysicalColumnarDesignApplyOutcome::Created { projection_id } => projection_id,
        other => panic!("unexpected apply outcome: {other:?}"),
    };
    assert_eq!(created.global_commit_seq_before, g_before);
    assert_eq!(created.global_commit_seq_after, g_before);
    assert_eq!(created.schema_generation, schema_before);
    assert_eq!(fixture.database.schema_generation(), schema_before);
    let projection = &fixture.database.inspect_columnar_projections()[0];
    assert_eq!(projection.columns, candidate.columns);
    assert_eq!(projection.directory, proposal.directory());
    assert_eq!(projection.mode, Some("snapshot"));
    assert_eq!(projection.health, ColumnarProjectionHealth::Fresh);
    assert_eq!(
        fixture
            .database
            .inspect_physical_columnar_design_location(
                &candidate,
                PhysicalColumnarDesignMode::Snapshot,
                &directory,
            )
            .unwrap(),
        PhysicalColumnarDesignLocationState::AlreadyApplied { projection_id }
    );

    window.rotate_window().expect("rotate evidence");
    let retry = fixture
        .database
        .apply_physical_columnar_design(&window, &proposal)
        .expect("retry after evidence rotation");
    assert_eq!(
        retry.outcome,
        PhysicalColumnarDesignApplyOutcome::AlreadyApplied { projection_id }
    );
    assert_eq!(
        fixture
            .database
            .inspect_columnar_projection_catalog()
            .next_projection_id,
        Some(netbadb_types::ColumnarProjectionId(projection_id.0 + 1))
    );

    fixture
        .database
        .execute("INSERT INTO events VALUES (9000, 3, 'later')")
        .expect("committed DML");
    assert_eq!(
        fixture.database.inspect_columnar_projections()[0].health,
        ColumnarProjectionHealth::Stale
    );
    assert_eq!(
        fixture
            .database
            .inspect_physical_columnar_design_location(
                &candidate,
                PhysicalColumnarDesignMode::Snapshot,
                &directory,
            )
            .unwrap(),
        PhysicalColumnarDesignLocationState::AlreadyApplied { projection_id }
    );
    fixture.close();
}

#[test]
fn incremental_columnar_apply_revalidates_stream_lineage_and_uses_existing_advance() {
    let mut fixture = Fixture::create("phase27-incremental-apply", false);
    let window = columnar_window(&mut fixture);
    let candidate = columnar_candidate();
    let first_cursor = fixture
        .database
        .enable_change_stream(TABLE_ID)
        .expect("enable first stream");
    let stale_directory = fixture.root.join("stale-incremental-design");
    let stale = fixture
        .database
        .propose_physical_columnar_design(
            &window,
            policy(1, 1, 0, 8),
            candidate.clone(),
            PhysicalColumnarDesignMode::Incremental,
            &stale_directory,
        )
        .expect("first incremental proposal");
    assert_eq!(
        stale.change_stream_generation(),
        Some(first_cursor.generation)
    );
    fixture
        .database
        .disable_change_stream(TABLE_ID)
        .expect("disable first stream");
    let next_while_disabled = fixture
        .database
        .inspect_columnar_projection_catalog()
        .next_projection_id;
    assert!(matches!(
        fixture
            .database
            .apply_physical_columnar_design(&window, &stale),
        Err(PhysicalColumnarDesignApplyError::StaleProposal(
            PhysicalColumnarDesignProposalStaleReason::ChangeStreamDisabled
        ))
    ));
    assert_eq!(
        fixture
            .database
            .inspect_columnar_projection_catalog()
            .next_projection_id,
        next_while_disabled
    );
    let second_cursor = fixture
        .database
        .enable_change_stream(TABLE_ID)
        .expect("enable replacement stream");
    let next_before_rejection = fixture
        .database
        .inspect_columnar_projection_catalog()
        .next_projection_id;
    assert!(matches!(
        fixture
            .database
            .apply_physical_columnar_design(&window, &stale),
        Err(PhysicalColumnarDesignApplyError::StaleProposal(
            PhysicalColumnarDesignProposalStaleReason::ChangeStreamGenerationChanged {
                expected,
                actual,
            }
        )) if expected == first_cursor.generation && actual == second_cursor.generation
    ));
    assert_eq!(
        fixture
            .database
            .inspect_columnar_projection_catalog()
            .next_projection_id,
        next_before_rejection
    );
    assert!(!stale_directory.exists());

    let directory = fixture.root.join("incremental-design");
    let proposal = fixture
        .database
        .propose_physical_columnar_design(
            &window,
            policy(1, 1, 0, 8),
            candidate.clone(),
            PhysicalColumnarDesignMode::Incremental,
            &directory,
        )
        .expect("replacement-stream proposal");
    let g_before = fixture
        .database
        .current_database_snapshot()
        .unwrap()
        .unwrap()
        .commit_seq();
    let created = fixture
        .database
        .apply_physical_columnar_design(&window, &proposal)
        .expect("apply incremental proposal");
    let projection_id = match created.outcome {
        PhysicalColumnarDesignApplyOutcome::Created { projection_id } => projection_id,
        other => panic!("unexpected apply outcome: {other:?}"),
    };
    assert_eq!(created.global_commit_seq_before, g_before);
    assert_eq!(created.global_commit_seq_after, g_before);
    let projection = &fixture.database.inspect_columnar_projections()[0];
    assert_eq!(projection.mode, Some("incremental"));
    assert_eq!(projection.columns, candidate.columns);
    assert_eq!(projection.directory, proposal.directory());
    assert_eq!(projection.stream_generation, Some(second_cursor.generation));
    assert_eq!(projection.health, ColumnarProjectionHealth::Fresh);

    fixture
        .database
        .execute("INSERT INTO events VALUES (9001, 4, 'incremental-later')")
        .expect("streamed DML");
    assert_eq!(
        fixture.database.inspect_columnar_projections()[0].health,
        ColumnarProjectionHealth::Lagging
    );
    fixture
        .database
        .advance_columnar_projection(projection_id, ColumnarAdvanceBudget::new(8, 1 << 20))
        .expect("explicitly advance projection");
    assert_eq!(
        fixture.database.inspect_columnar_projections()[0].health,
        ColumnarProjectionHealth::Fresh
    );
    fixture.close();
}

#[test]
fn columnar_apply_conflicts_by_location_and_noops_for_other_coverage() {
    let mut fixture = Fixture::create("phase27-location-conflict", false);
    let window = columnar_window(&mut fixture);
    let candidate = columnar_candidate();
    fixture
        .database
        .enable_change_stream(TABLE_ID)
        .expect("enable stream");
    let directory = fixture.root.join("shared-location");
    let snapshot = fixture
        .database
        .propose_physical_columnar_design(
            &window,
            policy(1, 1, 0, 8),
            candidate.clone(),
            PhysicalColumnarDesignMode::Snapshot,
            &directory,
        )
        .expect("snapshot proposal");
    let incremental = fixture
        .database
        .propose_physical_columnar_design(
            &window,
            policy(1, 1, 0, 8),
            candidate.clone(),
            PhysicalColumnarDesignMode::Incremental,
            &directory,
        )
        .expect("incremental proposal");
    let created = fixture
        .database
        .apply_physical_columnar_design(&window, &snapshot)
        .expect("create snapshot");
    let projection_id = match created.outcome {
        PhysicalColumnarDesignApplyOutcome::Created { projection_id } => projection_id,
        other => panic!("unexpected apply outcome: {other:?}"),
    };
    assert!(matches!(
        fixture
            .database
            .apply_physical_columnar_design(&window, &incremental),
        Err(PhysicalColumnarDesignApplyError::ProjectionLocationConflict {
            projection_id: actual,
            ..
        }) if actual == projection_id
    ));
    fixture.close();

    let mut covered = Fixture::create("phase27-other-coverage", false);
    let window = columnar_window(&mut covered);
    let candidate = columnar_candidate();
    let proposal = covered
        .database
        .propose_physical_columnar_design(
            &window,
            policy(1, 1, 0, 8),
            candidate.clone(),
            PhysicalColumnarDesignMode::Snapshot,
            covered.root.join("approved-but-unused"),
        )
        .expect("proposal before coverage");
    covered
        .database
        .build_columnar_projection(ColumnarProjectionSpec::new(
            TABLE_ID,
            covered.root.join("other-covering-projection"),
            candidate.columns,
        ))
        .expect("create other covering projection");
    let next_before = covered
        .database
        .inspect_columnar_projection_catalog()
        .next_projection_id;
    let g_before = covered
        .database
        .current_database_snapshot()
        .unwrap()
        .unwrap()
        .commit_seq();
    let report = covered
        .database
        .apply_physical_columnar_design(&window, &proposal)
        .expect("coverage no-op");
    assert_eq!(
        report.outcome,
        PhysicalColumnarDesignApplyOutcome::AlreadyCovered
    );
    assert_eq!(report.global_commit_seq_before, g_before);
    assert_eq!(report.global_commit_seq_after, g_before);
    assert_eq!(
        covered
            .database
            .inspect_columnar_projection_catalog()
            .next_projection_id,
        next_before
    );
    assert!(!proposal.directory().exists());
    covered.close();
}

#[test]
fn columnar_apply_propagates_recovery_required_and_reopen_recovers_exact_retry() {
    let mut fixture = Fixture::create("phase27-recovery-retry", false);
    let window = columnar_window(&mut fixture);
    let candidate = columnar_candidate();
    let directory = fixture.root.join("ambiguous-publication");
    let proposal = fixture
        .database
        .propose_physical_columnar_design(
            &window,
            policy(1, 1, 0, 8),
            candidate,
            PhysicalColumnarDesignMode::Snapshot,
            &directory,
        )
        .expect("recovery proposal");
    fixture.database.columnar_build_after_scan = Some(make_catalog_parent_read_only);
    let failed = fixture
        .database
        .apply_physical_columnar_design(&window, &proposal);
    assert!(matches!(
        failed,
        Err(PhysicalColumnarDesignApplyError::Database(
            DatabaseError::ProjectionCatalog(ProjectionCatalogError::RecoveryRequired { .. })
        ))
    ));
    assert!(
        !fixture
            .database
            .inspect_columnar_projection_catalog()
            .available
    );
    assert!(fixture.database.inspect_columnar_projections().is_empty());

    let Fixture {
        root,
        source,
        database,
    } = fixture;
    let mut permissions = fs::metadata(&root).expect("fixture metadata").permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&root, permissions).expect("restore fixture permissions");
    drop(database);

    let mut reopened = crate::Database::open_catalog(root.join("catalog"))
        .expect("reopen promotes exact pending artifact");
    let recovered = &reopened.inspect_columnar_projections()[0];
    let projection_id = recovered.projection_id.expect("recovered projection id");
    assert_eq!(recovered.directory, proposal.directory());
    let retry = reopened
        .apply_physical_columnar_design(&window, &proposal)
        .expect("exact retry after recovery");
    assert_eq!(
        retry.outcome,
        PhysicalColumnarDesignApplyOutcome::AlreadyApplied { projection_id }
    );
    reopened.close().expect("close recovered database");
    crate::cleanup_created_table_files(std::slice::from_ref(&source));
    fs::remove_dir_all(root).expect("remove recovery fixture");
}

#[test]
fn columnar_proposal_is_bound_to_durable_database_incarnation() {
    let mut first = Fixture::create("phase27-identity-a", false);
    let second = Fixture::create("phase27-identity-b", false);
    let window = columnar_window(&mut first);
    let proposal = first
        .database
        .propose_physical_columnar_design(
            &window,
            policy(1, 1, 0, 8),
            columnar_candidate(),
            PhysicalColumnarDesignMode::Snapshot,
            first.root.join("foreign-design"),
        )
        .expect("first database proposal");
    let Fixture {
        root,
        source,
        mut database,
    } = second;
    let next_before = database
        .inspect_columnar_projection_catalog()
        .next_projection_id;
    assert!(matches!(
        database.apply_physical_columnar_design(&window, &proposal),
        Err(PhysicalColumnarDesignApplyError::DatabaseIdentityChanged)
    ));
    assert_eq!(
        database
            .inspect_columnar_projection_catalog()
            .next_projection_id,
        next_before
    );
    assert!(database.inspect_columnar_projections().is_empty());
    database.close().expect("close second database");
    crate::cleanup_created_table_files(std::slice::from_ref(&source));
    fs::remove_dir_all(root).expect("remove second fixture");
    first.close();
}

#[test]
fn physical_design_database_identity_is_stable_and_read_only() {
    let fixture = Fixture::create("phase30-database-identity", false);
    let catalog_path = fixture
        .database
        .catalog_path
        .as_ref()
        .expect("durable catalog")
        .clone();
    let catalog_before = fs::read(&catalog_path).expect("read catalog before identity");
    let snapshot_before = fixture
        .database
        .current_database_snapshot()
        .expect("database snapshot before identity");
    let schema_before = fixture.database.schema_generation();

    let first = fixture
        .database
        .physical_design_database_identity()
        .expect("first identity read");
    let second = fixture
        .database
        .physical_design_database_identity()
        .expect("second identity read");

    assert_eq!(first, second);
    assert_ne!(first.as_bytes(), &[0; 16]);
    assert_eq!(
        fixture
            .database
            .current_database_snapshot()
            .expect("database snapshot after identity"),
        snapshot_before
    );
    assert_eq!(fixture.database.schema_generation(), schema_before);
    assert_eq!(
        fs::read(catalog_path).expect("read catalog after identity"),
        catalog_before
    );
    fixture.close();
}

fn mutation_work_file_image(
    root: &std::path::Path,
) -> std::collections::BTreeMap<std::path::PathBuf, Vec<u8>> {
    let mut image = std::collections::BTreeMap::new();
    for entry in fs::read_dir(root).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            image.extend(mutation_work_file_image(&path));
        } else {
            image.insert(path.clone(), fs::read(path).unwrap());
        }
    }
    image
}

fn mutation_work_lsm_fixture(name: &str) -> Fixture {
    use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
    use netbadb_types::PhysicalType;
    let suffix = crate::execution_feedback_tests::NEXT_PATH
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let root = std::env::temp_dir().join(format!(
        "netbadb-phase33-{name}-{}-{suffix}",
        std::process::id()
    ));
    fs::create_dir_all(&root).unwrap();
    let source = root.join("lsm");
    let table = TableDef::new(
        TABLE_ID,
        "events",
        vec![
            ColumnDef::new(ColumnId(1), "id", TypeSpec::Physical(PhysicalType::Int64)),
            ColumnDef::new(
                ColumnId(2),
                "category",
                TypeSpec::Physical(PhysicalType::Int64),
            ),
        ],
    );
    let mut database = crate::Database::create_catalog(
        root.join("catalog"),
        vec![crate::TableStorageCreateSpec::lsm(
            &source,
            table,
            ColumnId(1),
        )],
        Some(
            crate::DatabaseCoordinatorConfig::new(root.join("coordinator"))
                .with_global_visibility(),
        ),
    )
    .unwrap();
    database
        .execute("INSERT INTO events VALUES (1, 1)")
        .unwrap();
    Fixture {
        root,
        source,
        database,
    }
}

#[test]
fn mutation_work_all_engine_modes_are_repeatable_pure_and_not_recommendation_gated() {
    use crate::{
        PhysicalColumnarMutationPrerequisiteInspection as Prerequisite,
        PhysicalDesignMutationConservativeBound as Bound,
    };
    use netbadb_storage::{
        StoragePhysicalDesignSourceInspection as Source,
        source_inspection_test_activity as activity,
    };
    for lsm in [false, true] {
        let mut fixture = if lsm {
            mutation_work_lsm_fixture("purity")
        } else {
            Fixture::create("phase33-purity", false)
        };
        fixture.database.enable_change_stream(TABLE_ID).unwrap();
        let window = columnar_window(&mut fixture);
        let window_before = window.clone();
        let pool = crate::AdaptiveEvidencePool::new(crate::AdaptiveEvidencePoolLimits::default());
        let pool_before = pool.progress_token();
        let scheduler = crate::AutomaticScheduler::new(
            crate::AutomaticSchedulerPolicy::new(1, 1, 1, 1).unwrap(),
        );
        let scheduler_before = scheduler.clone();
        let g = fixture
            .database
            .current_database_snapshot()
            .unwrap()
            .unwrap()
            .commit_seq();
        let schema = fixture.database.schema_generation();
        let catalog = fixture.database.inspect_catalog().unwrap();
        let projections = fixture.database.inspect_columnar_projection_catalog();
        let stream = fixture.database.inspect_change_stream(TABLE_ID).unwrap();
        let safe_mode = fixture.database.automatic_safe_mode_state();
        let calibration = fixture.database.planner_calibration_profile();
        let files = mutation_work_file_image(&fixture.root);
        activity::take();
        for mode in [
            PhysicalColumnarDesignMode::Snapshot,
            PhysicalColumnarDesignMode::Incremental,
        ] {
            let first = fixture
                .database
                .inspect_physical_columnar_design_mutation_work(&columnar_candidate(), mode)
                .unwrap();
            assert_eq!(first.candidate, columnar_candidate());
            assert_eq!(first.mode, mode);
            assert_eq!(first.schema_generation, schema);
            assert_eq!(
                Some(first.table_schema_version),
                fixture.database.table_schema_version(TABLE_ID)
            );
            assert!(matches!(
                first.bounds.output_write_bytes,
                Bound::Bounded(bytes) if bytes > 0
            ));
            match first.source {
                Source::Heap(heap) => {
                    assert!(!lsm);
                    assert_eq!(heap.storage_id, first.storage_id);
                    assert_eq!(
                        first.bounds.source_work_units,
                        Bound::Bounded(heap.managed_page_upper_bound)
                    );
                    assert_eq!(
                        first.bounds.source_read_bytes,
                        Bound::Bounded(heap.main_file_bytes_upper_bound)
                    );
                    assert_eq!(first.prerequisite, Prerequisite::None);
                    let index = fixture
                        .database
                        .inspect_physical_index_design_mutation_work(index_candidate())
                        .unwrap();
                    assert_eq!(index.candidate, index_candidate());
                    assert_eq!(index.source, first.source);
                    assert_eq!(
                        index.bounds.source_work_units,
                        Bound::Bounded(heap.index_backfill_page_upper_bound)
                    );
                    assert_eq!(
                        index.bounds.source_read_bytes,
                        Bound::Bounded(heap.index_backfill_bytes_upper_bound)
                    );
                    assert!(matches!(
                        index.bounds.output_write_bytes,
                        Bound::Bounded(bytes) if bytes > 0
                    ));
                    assert_eq!(index.table_fingerprint, first.table_fingerprint);
                }
                Source::Lsm(source) => {
                    assert!(lsm);
                    assert_eq!(source.memtable_entry_count, 1, "inspection cannot flush");
                    assert_eq!(source.sstable_count, 0);
                    assert_eq!(source.anchor.storage_id, first.storage_id);
                    assert_eq!(first.bounds.source_work_units, Bound::NotProven);
                    if mode == PhysicalColumnarDesignMode::Snapshot {
                        assert_eq!(
                            first.prerequisite,
                            Prerequisite::LsmFlush {
                                anchor: source.anchor,
                                conservative_bound: source.flush_conservative_bound.unwrap()
                            }
                        );
                        assert_eq!(
                            first.bounds.source_read_bytes,
                            Bound::Bounded(
                                source.total_sstable_bytes
                                    + source.flush_conservative_bound.unwrap().write_bytes
                            )
                        );
                    } else {
                        assert_eq!(first.prerequisite, Prerequisite::None);
                        assert_eq!(first.bounds.source_read_bytes, Bound::Bounded(0));
                    }
                }
            }
            for _ in 0..3 {
                assert_eq!(
                    fixture
                        .database
                        .inspect_physical_columnar_design_mutation_work(&columnar_candidate(), mode)
                        .unwrap(),
                    first
                );
            }
        }
        assert_eq!(
            activity::take(),
            activity::Activity::default(),
            "no source scan, ANALYZE, flush or cache access"
        );
        assert_eq!(
            fixture
                .database
                .current_database_snapshot()
                .unwrap()
                .unwrap()
                .commit_seq(),
            g
        );
        assert_eq!(fixture.database.schema_generation(), schema);
        assert_eq!(fixture.database.inspect_catalog().unwrap(), catalog);
        assert_eq!(
            fixture.database.inspect_columnar_projection_catalog(),
            projections
        );
        assert_eq!(
            fixture.database.inspect_change_stream(TABLE_ID).unwrap(),
            stream
        );
        assert_eq!(fixture.database.automatic_safe_mode_state(), safe_mode);
        assert_eq!(fixture.database.planner_calibration_profile(), calibration);
        assert_eq!(
            mutation_work_file_image(&fixture.root),
            files,
            "includes NBPC, schema/index high-waters, WAL, stream and all storage files"
        );
        assert_eq!(window, window_before);
        assert_eq!(pool.progress_token(), pool_before);
        assert_eq!(scheduler, scheduler_before);
        fixture.close();
    }
}

#[test]
fn mutation_work_rejects_impossible_targets_and_preserves_typed_sources() {
    use crate::PhysicalDesignMutationWorkInspectionError as WorkError;
    let mut fixture = Fixture::create("phase33-invalid", false);
    assert!(matches!(
        fixture
            .database
            .inspect_physical_index_design_mutation_work(PhysicalIndexCandidate {
                table_id: netbadb_types::TableId(u64::MAX),
                column_id: ColumnId(1)
            }),
        Err(WorkError::TableNotFound(_))
    ));
    assert!(matches!(
        fixture
            .database
            .inspect_physical_index_design_mutation_work(PhysicalIndexCandidate {
                table_id: TABLE_ID,
                column_id: ColumnId(99)
            }),
        Err(WorkError::ColumnNotFound { .. })
    ));
    assert!(matches!(
        fixture
            .database
            .inspect_physical_columnar_design_mutation_work(
                &columnar_candidate(),
                PhysicalColumnarDesignMode::Incremental
            ),
        Err(WorkError::IncrementalChangeStreamNotEnabled { .. })
    ));
    for (columns, duplicate) in [(vec![], false), (vec![ColumnId(1), ColumnId(1)], true)] {
        let error = fixture
            .database
            .inspect_physical_columnar_design_mutation_work(
                &PhysicalColumnarCandidate {
                    table_id: TABLE_ID,
                    columns,
                },
                PhysicalColumnarDesignMode::Snapshot,
            )
            .unwrap_err();
        assert!(matches!(
            (duplicate, error),
            (false, WorkError::EmptyColumnarColumns)
                | (true, WorkError::DuplicateColumnarColumn(_))
        ));
    }
    let file = fs::OpenOptions::new()
        .write(true)
        .open(&fixture.source)
        .unwrap();
    let length = file.metadata().unwrap().len();
    file.set_len(length + 1).unwrap();
    let storage_error = fixture
        .database
        .inspect_physical_index_design_mutation_work(index_candidate())
        .unwrap_err();
    assert!(matches!(
        &storage_error,
        WorkError::Database(DatabaseError::Storage(
            netbadb_storage::StorageError::InvalidFormat(_)
        ))
    ));
    assert!(
        std::error::Error::source(std::error::Error::source(&storage_error).unwrap())
            .unwrap()
            .downcast_ref::<netbadb_storage::StorageError>()
            .is_some()
    );
    file.set_len(length).unwrap();
    drop(file);
    fixture.database.projections.mark_recovery_required(
        netbadb_types::ColumnarProjectionId(1),
        "test",
        "test",
    );
    let error = fixture
        .database
        .inspect_physical_columnar_design_mutation_work(
            &columnar_candidate(),
            PhysicalColumnarDesignMode::Snapshot,
        )
        .unwrap_err();
    assert!(
        std::error::Error::source(&error)
            .unwrap()
            .downcast_ref::<DatabaseError>()
            .is_some()
    );
    assert!(matches!(
        error,
        WorkError::Database(DatabaseError::ProjectionCatalog(
            ProjectionCatalogError::RecoveryRequired { .. }
        ))
    ));
    fixture.close();
    let fixture = mutation_work_lsm_fixture("unsupported-index");
    assert!(matches!(
        fixture
            .database
            .inspect_physical_index_design_mutation_work(index_candidate()),
        Err(WorkError::UnsupportedIndexLayout)
    ));
    fixture.close();
}

#[test]
fn mutation_work_rejects_legacy_and_partitioned_targets() {
    use crate::PhysicalDesignMutationWorkInspectionError as WorkError;
    use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
    use netbadb_types::{PartitionId, PhysicalType};
    let fixture = Fixture::create("phase33-layout-paths", false);
    let table = TableDef::new(
        TABLE_ID,
        "events",
        vec![
            ColumnDef::new(ColumnId(1), "id", TypeSpec::Physical(PhysicalType::Int64)),
            ColumnDef::new(
                ColumnId(2),
                "category",
                TypeSpec::Physical(PhysicalType::Int64),
            ),
        ],
    );
    let mut partitioned = crate::Database::create_catalog_with_placements(
        fixture.root.join("range-catalog"),
        vec![crate::TablePlacementSpec::range_partitioned(
            table,
            ColumnId(1),
            vec![crate::RangePartitionSpec::new(
                PartitionId(1),
                fixture.root.join("range-heap"),
                None,
                None,
            )],
        )],
        crate::PartitionCatalogConfig::new(
            fixture.root.join("partitions"),
            fixture.root.join("range-coordinator"),
        ),
    )
    .unwrap();
    assert!(matches!(
        partitioned.inspect_physical_index_design_mutation_work(index_candidate()),
        Err(WorkError::GlobalVisibilityRequired)
    ));
    assert!(matches!(
        partitioned.inspect_physical_columnar_design_mutation_work(
            &columnar_candidate(),
            PhysicalColumnarDesignMode::Snapshot
        ),
        Err(WorkError::GlobalVisibilityRequired)
    ));
    partitioned.enable_global_visibility().unwrap();
    assert!(matches!(
        partitioned.inspect_physical_index_design_mutation_work(index_candidate()),
        Err(WorkError::RequiresSingleStorage(TABLE_ID))
    ));
    assert!(matches!(
        partitioned.inspect_physical_columnar_design_mutation_work(
            &columnar_candidate(),
            PhysicalColumnarDesignMode::Snapshot
        ),
        Err(WorkError::RequiresSingleStorage(TABLE_ID))
    ));
    partitioned.close().unwrap();
    fixture.close();
}

#[test]
fn mutation_work_follows_lsm_dml_and_flush_without_refreshing_analyze() {
    use crate::PhysicalColumnarMutationPrerequisiteInspection as Prerequisite;
    use netbadb_storage::StoragePhysicalDesignSourceInspection as Source;
    let mut fixture = mutation_work_lsm_fixture("stale-analyze");
    fixture.database.analyze(TABLE_ID).unwrap();
    let analyzed = fixture.database.inspect_catalog().unwrap();
    let before = fixture
        .database
        .inspect_physical_columnar_design_mutation_work(
            &columnar_candidate(),
            PhysicalColumnarDesignMode::Snapshot,
        )
        .unwrap();
    fixture
        .database
        .execute("INSERT INTO events VALUES (2, 2)")
        .unwrap();
    let after = fixture
        .database
        .inspect_physical_columnar_design_mutation_work(
            &columnar_candidate(),
            PhysicalColumnarDesignMode::Snapshot,
        )
        .unwrap();
    assert_ne!(before.source, after.source);
    assert_eq!(fixture.database.inspect_catalog().unwrap(), analyzed);
    let storage = fixture.database.registry.get(after.storage_id).unwrap();
    storage.flush().unwrap();
    let flushed = fixture
        .database
        .inspect_physical_columnar_design_mutation_work(
            &columnar_candidate(),
            PhysicalColumnarDesignMode::Snapshot,
        )
        .unwrap();
    assert_eq!(flushed.prerequisite, Prerequisite::None);
    let Source::Lsm(source) = flushed.source else {
        panic!("LSM source")
    };
    assert_eq!(source.memtable_entry_count, 0);
    assert!(source.total_sstable_bytes > 0);
    assert_eq!(fixture.database.inspect_catalog().unwrap(), analyzed);
    fixture.close();
}

#[test]
fn mutation_work_does_not_change_ordinary_index_or_columnar_builds_or_coverage() {
    use netbadb_storage::source_inspection_test_activity as activity;
    for lsm in [false, true] {
        for mode in [
            PhysicalColumnarDesignMode::Snapshot,
            PhysicalColumnarDesignMode::Incremental,
        ] {
            let mut results = Vec::new();
            for inspect in [false, true] {
                let mut fixture = if lsm {
                    mutation_work_lsm_fixture("build")
                } else {
                    Fixture::create("phase33-build", false)
                };
                fixture.database.enable_change_stream(TABLE_ID).unwrap();
                if inspect {
                    fixture
                        .database
                        .inspect_physical_columnar_design_mutation_work(&columnar_candidate(), mode)
                        .unwrap();
                }
                let g = fixture
                    .database
                    .current_database_snapshot()
                    .unwrap()
                    .unwrap()
                    .commit_seq();
                let schema = fixture.database.schema_generation();
                activity::take();
                let spec = ColumnarProjectionSpec::new(
                    TABLE_ID,
                    fixture.root.join("result"),
                    columnar_candidate().columns,
                );
                let id = match mode {
                    PhysicalColumnarDesignMode::Snapshot => {
                        fixture.database.build_columnar_projection(spec).unwrap()
                    }
                    PhysicalColumnarDesignMode::Incremental => fixture
                        .database
                        .build_incremental_columnar_projection(spec)
                        .unwrap(),
                };
                let actual = activity::take();
                assert_eq!(actual.scan_columns_calls, 1);
                assert_eq!(
                    actual.scan_versioned_columns_calls,
                    u64::from(mode == PhysicalColumnarDesignMode::Incremental)
                );
                assert_eq!(
                    actual.flush_calls,
                    u64::from(lsm && mode == PhysicalColumnarDesignMode::Snapshot)
                );
                assert_eq!(
                    fixture
                        .database
                        .current_database_snapshot()
                        .unwrap()
                        .unwrap()
                        .commit_seq(),
                    g
                );
                assert_eq!(fixture.database.schema_generation(), schema);
                fixture
                    .database
                    .inspect_physical_columnar_design_mutation_work(&columnar_candidate(), mode)
                    .expect("coverage never suppresses hypothetical footprint");
                if !lsm {
                    if inspect {
                        fixture
                            .database
                            .inspect_physical_index_design_mutation_work(index_candidate())
                            .unwrap();
                    }
                    fixture
                        .database
                        .create_named_index(
                            netbadb_types::IndexName::new("phase33_category").unwrap(),
                            TABLE_ID,
                            ColumnId(2),
                        )
                        .unwrap();
                    fixture
                        .database
                        .inspect_physical_index_design_mutation_work(index_candidate())
                        .expect("existing index does not return AlreadyCovered");
                }
                let rows = fixture
                    .database
                    .query("SELECT id, category FROM events ORDER BY id")
                    .unwrap();
                let indexes = fixture.database.indexes(TABLE_ID).unwrap().to_vec();
                results.push((
                    id,
                    rows,
                    indexes,
                    fixture
                        .database
                        .inspect_columnar_projection_catalog()
                        .next_projection_id,
                ));
                let Fixture {
                    root,
                    source: _,
                    database,
                } = fixture;
                database.close().unwrap();
                let reopened = crate::Database::open_catalog(root.join("catalog")).unwrap();
                reopened
                    .inspect_physical_columnar_design_mutation_work(&columnar_candidate(), mode)
                    .unwrap();
                assert_eq!(reopened.inspect_columnar_projections().len(), 1);
                reopened.close().unwrap();
                fs::remove_dir_all(root).unwrap();
            }
            assert_eq!(results[0], results[1]);
        }
    }
}

#[test]
fn mutation_work_incremental_rejects_unavailable_stream_without_repair() {
    use crate::PhysicalDesignMutationWorkInspectionError as WorkError;
    for lsm in [false, true] {
        let mut fixture = if lsm {
            mutation_work_lsm_fixture("unavailable")
        } else {
            Fixture::create("phase33-unavailable", false)
        };
        fixture.database.enable_change_stream(TABLE_ID).unwrap();
        let Fixture {
            root,
            source,
            database,
        } = fixture;
        database.close().unwrap();
        let log = if lsm {
            netbadb_storage::lsm_change_log_path(&source)
        } else {
            netbadb_storage::heap_change_log_path(&source)
        };
        fs::remove_file(log).unwrap();
        let database = crate::Database::open_catalog(root.join("catalog")).unwrap();
        let before = mutation_work_file_image(&root);
        assert!(matches!(
            database.inspect_physical_columnar_design_mutation_work(
                &columnar_candidate(),
                PhysicalColumnarDesignMode::Incremental
            ),
            Err(WorkError::IncrementalChangeStreamUnavailable { .. })
        ));
        database
            .inspect_physical_columnar_design_mutation_work(
                &columnar_candidate(),
                PhysicalColumnarDesignMode::Snapshot,
            )
            .unwrap();
        assert_eq!(mutation_work_file_image(&root), before);
        assert_eq!(
            database.inspect_change_stream(TABLE_ID).unwrap().status,
            netbadb_storage::ChangeStreamStatus::Unavailable
        );
        database.close().unwrap();
        fs::remove_dir_all(root).unwrap();
    }
}

mod admission {
    use super::*;
    use crate::{
        Database, PhysicalColumnarDesignAdmissionApplyError as ColumnarError,
        PhysicalColumnarDesignProposal,
        PhysicalColumnarMutationPrerequisiteInspection as Prerequisite,
        PhysicalDesignMutationAdmissionConstraint::{AtMost, Unconstrained},
        PhysicalDesignMutationAdmissionDimension as Dimension,
        PhysicalDesignMutationAdmissionError as AdmissionError,
        PhysicalDesignMutationAdmissionLimits as Limits,
        PhysicalDesignMutationAdmissionPolicy as Admission,
        PhysicalDesignMutationConservativeBound as Bound,
        PhysicalIndexDesignAdmissionApplyError as IndexError,
    };
    use netbadb_storage::source_inspection_test_activity as activity;
    use netbadb_storage::{
        StoragePhysicalDesignSourceInspection as Source,
        index_write_bound_test_activity as writer_activity,
    };
    use netbadb_types::IndexName;
    use std::error::Error;

    fn constraint(dimension: Dimension, maximum: u64) -> Admission {
        let mut limits = Limits {
            source_work_units: Unconstrained,
            source_read_bytes: Unconstrained,
            prerequisite_work_units: Unconstrained,
            prerequisite_read_bytes: Unconstrained,
            prerequisite_write_bytes: Unconstrained,
            output_write_bytes: Unconstrained,
        };
        match dimension {
            Dimension::SourceWorkUnits => limits.source_work_units = AtMost(maximum),
            Dimension::SourceReadBytes => limits.source_read_bytes = AtMost(maximum),
            Dimension::PrerequisiteWorkUnits => limits.prerequisite_work_units = AtMost(maximum),
            Dimension::PrerequisiteReadBytes => limits.prerequisite_read_bytes = AtMost(maximum),
            Dimension::PrerequisiteWriteBytes => limits.prerequisite_write_bytes = AtMost(maximum),
            Dimension::OutputWriteBytes => limits.output_write_bytes = AtMost(maximum),
        }
        Admission::new(limits).unwrap()
    }

    fn bound(value: Bound) -> u64 {
        let Bound::Bounded(value) = value else {
            panic!("expected proven bound")
        };
        value
    }

    fn name() -> IndexName {
        IndexName::new("phase34_category").unwrap()
    }

    fn evidence_window(fixture: &mut Fixture) -> PhysicalDesignEvidenceWindow {
        let mut window = PhysicalDesignEvidenceWindow::default();
        for sql in [
            "SELECT id FROM events WHERE category = 3",
            "SELECT id FROM events",
        ] {
            window
                .record_execution_feedback(&feedback(fixture, sql))
                .unwrap();
        }
        window
    }

    fn candidate() -> PhysicalColumnarCandidate {
        PhysicalColumnarCandidate {
            table_id: TABLE_ID,
            columns: vec![ColumnId(1)],
        }
    }

    fn columnar_proposal(
        fixture: &Fixture,
        window: &PhysicalDesignEvidenceWindow,
        mode: PhysicalColumnarDesignMode,
        placement: &str,
    ) -> PhysicalColumnarDesignProposal {
        fixture
            .database
            .propose_physical_columnar_design(
                window,
                policy(1, 1, 0, 8),
                candidate(),
                mode,
                fixture.root.join(placement),
            )
            .unwrap()
    }

    // Snapshot all authoritative persistent bytes, high-waters and observable
    // runtime state around each rejection, including LSM flush counters.
    fn pure_rejection(
        fixture: &mut Fixture,
        window: &PhysicalDesignEvidenceWindow,
        operation: impl FnOnce(&mut Database) -> AdmissionError,
    ) -> AdmissionError {
        let files = mutation_work_file_image(&fixture.root);
        let g = fixture
            .database
            .current_database_snapshot()
            .unwrap()
            .unwrap()
            .commit_seq();
        let schema = fixture.database.schema_generation();
        let catalog = fixture.database.inspect_catalog().unwrap();
        let projections = fixture.database.inspect_columnar_projection_catalog();
        let lsm = fixture.database.inspect_lsm_storage(TABLE_ID).unwrap();
        let stream = fixture.database.inspect_change_stream(TABLE_ID).unwrap();
        let evidence = window.clone();
        let calibration = fixture.database.planner_calibration_profile();
        let safe_mode = fixture.database.automatic_safe_mode_state();
        activity::take();
        let error = operation(&mut fixture.database);
        assert_eq!(activity::take(), activity::Activity::default());
        assert_eq!(mutation_work_file_image(&fixture.root), files);
        assert_eq!(
            fixture
                .database
                .current_database_snapshot()
                .unwrap()
                .unwrap()
                .commit_seq(),
            g
        );
        assert_eq!(fixture.database.schema_generation(), schema);
        assert_eq!(fixture.database.inspect_catalog().unwrap(), catalog);
        assert_eq!(
            fixture.database.inspect_columnar_projection_catalog(),
            projections
        );
        assert_eq!(fixture.database.inspect_lsm_storage(TABLE_ID).unwrap(), lsm);
        assert_eq!(
            fixture.database.inspect_change_stream(TABLE_ID).unwrap(),
            stream
        );
        assert_eq!(fixture.database.planner_calibration_profile(), calibration);
        assert_eq!(fixture.database.automatic_safe_mode_state(), safe_mode);
        assert_eq!(*window, evidence);
        error
    }

    fn columnar_rejection(
        fixture: &mut Fixture,
        window: &PhysicalDesignEvidenceWindow,
        proposal: &PhysicalColumnarDesignProposal,
        admission: Admission,
    ) -> AdmissionError {
        pure_rejection(fixture, window, |database| {
            let error = database
                .apply_physical_columnar_design_with_admission(window, proposal, admission)
                .unwrap_err();
            assert!(
                error
                    .source()
                    .unwrap()
                    .downcast_ref::<AdmissionError>()
                    .is_some()
            );
            let ColumnarError::Admission(error) = error else {
                panic!("expected admission error: {error:?}")
            };
            *error
        })
    }

    fn assert_limit(error: AdmissionError, dimension: Dimension, n: u64, maximum: u64) {
        assert!(
            matches!(error, AdmissionError::LimitExceeded { dimension: actual, conservative_bound, maximum: actual_maximum }
            if actual == dimension && conservative_bound == n && actual_maximum == maximum),
            "{error:?}"
        );
    }

    fn assert_unknown(error: AdmissionError, dimension: Dimension) {
        assert!(
            matches!(error, AdmissionError::RequiredBoundNotProven { dimension: actual } if actual == dimension),
            "{error:?}"
        );
    }

    #[test]
    fn admission_heap_index_bounds_reject_below_and_accept_equal() {
        for dimension in [
            Dimension::SourceWorkUnits,
            Dimension::SourceReadBytes,
            Dimension::OutputWriteBytes,
        ] {
            let mut fixture = Fixture::create("phase34-index", false);
            let window = evidence_window(&mut fixture);
            let proposal = fixture
                .database
                .propose_physical_index_design(&window, policy(1, 1, 0, 8), index_candidate())
                .unwrap();
            let inspection = fixture
                .database
                .inspect_physical_index_design_mutation_work(index_candidate())
                .unwrap();
            let n = bound(match dimension {
                Dimension::SourceWorkUnits => inspection.bounds.source_work_units,
                Dimension::SourceReadBytes => inspection.bounds.source_read_bytes,
                Dimension::OutputWriteBytes => inspection.bounds.output_write_bytes,
                _ => unreachable!(),
            });
            let error = pure_rejection(&mut fixture, &window, |database| {
                let error = database
                    .apply_physical_index_design_with_admission(
                        &window,
                        &proposal,
                        name(),
                        constraint(dimension, n - 1),
                    )
                    .unwrap_err();
                assert!(
                    error
                        .source()
                        .unwrap()
                        .downcast_ref::<AdmissionError>()
                        .is_some()
                );
                let IndexError::Admission(error) = error else {
                    panic!("admission error")
                };
                *error
            });
            assert_limit(error, dimension, n, n - 1);
            activity::take();
            let report = fixture
                .database
                .apply_physical_index_design_with_admission(
                    &window,
                    &proposal,
                    name(),
                    constraint(dimension, n),
                )
                .unwrap();
            assert!(matches!(
                report.outcome,
                PhysicalIndexDesignApplyOutcome::Created {
                    index_id: netbadb_types::IndexId(1)
                }
            ));
            let actual = activity::take();
            assert!(actual.heap_backfill_pages > 0);
            assert!(actual.heap_backfill_pages <= bound(inspection.bounds.source_work_units));
            assert_eq!(actual.heap_scan_pages, 0);
            assert_eq!(actual.scan_columns_calls, 0);
            assert_eq!(actual.analyze_calls, 0);
            fixture.close();
        }
    }

    #[test]
    fn global_index_actual_participant_output_fits_fresh_bound() {
        let mut fixture = Fixture::create("phase37-global-index-output", false);
        fixture.database.enable_change_stream(TABLE_ID).unwrap();
        let window = evidence_window(&mut fixture);
        let proposal = fixture
            .database
            .propose_physical_index_design(&window, policy(1, 1, 0, 8), index_candidate())
            .unwrap();
        let inspection = fixture
            .database
            .inspect_physical_index_design_mutation_work(index_candidate())
            .unwrap();
        let Source::Heap(heap) = inspection.source else {
            panic!("expected Heap source")
        };
        let writer_bound = heap.index_build_write_bound().unwrap();
        let output = bound(inspection.bounds.output_write_bytes);
        assert_eq!(output, writer_bound.total_write_bytes_upper_bound);

        writer_activity::take();
        let report = fixture
            .database
            .apply_physical_index_design_with_admission(
                &window,
                &proposal,
                name(),
                constraint(Dimension::OutputWriteBytes, u64::MAX),
            )
            .unwrap();
        assert!(matches!(
            report.outcome,
            PhysicalIndexDesignApplyOutcome::Created { .. }
        ));
        let actual = writer_activity::take();
        assert_eq!(actual.begin_records, 1);
        assert_eq!(actual.prepare_records, 1);
        assert_eq!(actual.commit_records, 1);
        assert_eq!(actual.abort_records, 0);
        assert_eq!(actual.rollback_complete_records, 0);
        assert_eq!(actual.staged_change_stream_rows, 0);
        assert_eq!(actual.published_page_images, actual.wal_page_image_records);
        assert!(actual.published_page_images <= writer_bound.total_page_image_upper_bound);
        assert!(
            actual.generation_reservation_records
                <= writer_bound.page_generation_reservation_upper_bound
        );
        assert!(actual.wal_appended_bytes <= writer_bound.heap_wal_write_bytes_upper_bound);
        assert_eq!(actual.committed_txn_status_records, 1);
        assert_eq!(
            actual.txn_status_appended_bytes,
            writer_bound.txn_status_write_bytes_upper_bound
        );
        let actual_output = actual.published_page_images * netbadb_storage::PAGE_SIZE as u64
            + actual.wal_appended_bytes
            + actual.txn_status_appended_bytes;
        assert!(actual_output <= output);
        fixture.close();
    }

    #[test]
    fn admission_heap_columnar_source_limits_and_zero_prerequisites_in_both_modes() {
        for mode in [
            PhysicalColumnarDesignMode::Snapshot,
            PhysicalColumnarDesignMode::Incremental,
        ] {
            for dimension in [
                Dimension::SourceWorkUnits,
                Dimension::SourceReadBytes,
                Dimension::OutputWriteBytes,
            ] {
                let mut fixture = Fixture::create("phase34-heap-columnar", false);
                fixture.database.enable_change_stream(TABLE_ID).unwrap();
                let window = evidence_window(&mut fixture);
                let proposal = columnar_proposal(&fixture, &window, mode, "admitted");
                let inspection = fixture
                    .database
                    .inspect_physical_columnar_design_mutation_work(&candidate(), mode)
                    .unwrap();
                let n = bound(match dimension {
                    Dimension::SourceWorkUnits => inspection.bounds.source_work_units,
                    Dimension::SourceReadBytes => inspection.bounds.source_read_bytes,
                    Dimension::OutputWriteBytes => inspection.bounds.output_write_bytes,
                    _ => unreachable!(),
                });
                let error = columnar_rejection(
                    &mut fixture,
                    &window,
                    &proposal,
                    constraint(dimension, n - 1),
                );
                assert_limit(error, dimension, n, n - 1);
                let mut limits = constraint(dimension, n).limits();
                if dimension != Dimension::OutputWriteBytes {
                    limits.prerequisite_work_units = AtMost(0);
                    limits.prerequisite_read_bytes = AtMost(0);
                    limits.prerequisite_write_bytes = AtMost(0);
                }
                activity::take();
                let report = fixture
                    .database
                    .apply_physical_columnar_design_with_admission(
                        &window,
                        &proposal,
                        Admission::new(limits).unwrap(),
                    )
                    .unwrap();
                assert!(matches!(
                    report.outcome,
                    PhysicalColumnarDesignApplyOutcome::Created {
                        projection_id: netbadb_types::ColumnarProjectionId(1)
                    }
                ));
                let actual = activity::take();
                assert_eq!(actual.scan_columns_calls, 1);
                assert_eq!(
                    actual.scan_versioned_columns_calls,
                    u64::from(mode == PhysicalColumnarDesignMode::Incremental)
                );
                assert_eq!(actual.flush_calls, 0);
                assert_eq!(actual.analyze_calls, 0);
                fixture.close();
            }
        }
    }

    #[test]
    fn admission_lsm_incremental_and_empty_snapshot_bound_sstables_only() {
        for mode in [
            PhysicalColumnarDesignMode::Snapshot,
            PhysicalColumnarDesignMode::Incremental,
        ] {
            let mut fixture = mutation_work_lsm_fixture("phase34-lsm-source");
            fixture.database.enable_change_stream(TABLE_ID).unwrap();
            let window = evidence_window(&mut fixture);
            let proposal = columnar_proposal(&fixture, &window, mode, "admitted");
            let storage_id = proposal.storage_id();
            fixture
                .database
                .registry
                .get(storage_id)
                .unwrap()
                .flush()
                .unwrap();
            // Incremental retains resident data but still never flushes.
            if mode == PhysicalColumnarDesignMode::Incremental {
                fixture
                    .database
                    .execute("INSERT INTO events VALUES (2, 2)")
                    .unwrap();
            }
            let inspection = fixture
                .database
                .inspect_physical_columnar_design_mutation_work(&candidate(), mode)
                .unwrap();
            let n = bound(inspection.bounds.source_read_bytes);
            assert!(n > 0);
            let error = columnar_rejection(
                &mut fixture,
                &window,
                &proposal,
                constraint(Dimension::SourceWorkUnits, u64::MAX),
            );
            assert_unknown(error, Dimension::SourceWorkUnits);
            let error = columnar_rejection(
                &mut fixture,
                &window,
                &proposal,
                constraint(Dimension::SourceReadBytes, n - 1),
            );
            assert_limit(error, Dimension::SourceReadBytes, n, n - 1);
            let output = bound(inspection.bounds.output_write_bytes);
            let error = columnar_rejection(
                &mut fixture,
                &window,
                &proposal,
                constraint(Dimension::OutputWriteBytes, output - 1),
            );
            assert_limit(error, Dimension::OutputWriteBytes, output, output - 1);
            let mut limits = constraint(Dimension::SourceReadBytes, n).limits();
            limits.output_write_bytes = AtMost(output);
            limits.prerequisite_work_units = AtMost(0);
            limits.prerequisite_read_bytes = AtMost(0);
            limits.prerequisite_write_bytes = AtMost(0);
            activity::take();
            fixture
                .database
                .apply_physical_columnar_design_with_admission(
                    &window,
                    &proposal,
                    Admission::new(limits).unwrap(),
                )
                .unwrap();
            let actual = activity::take();
            assert_eq!(actual.scan_columns_calls, 1);
            // Snapshot's ordinary flush is called even when its MemTable is empty.
            assert_eq!(
                actual.flush_calls,
                u64::from(mode == PhysicalColumnarDesignMode::Snapshot)
            );
            let source = fixture
                .database
                .inspect_lsm_storage(TABLE_ID)
                .unwrap()
                .unwrap();
            assert_eq!(
                source.memtable_entry_count,
                u64::from(mode == PhysicalColumnarDesignMode::Incremental)
            );
            fixture.close();
        }
    }

    #[test]
    fn admission_lsm_snapshot_partial_flush_limits_never_preflush_on_rejection() {
        for dimension in [
            Dimension::PrerequisiteWorkUnits,
            Dimension::PrerequisiteReadBytes,
            Dimension::PrerequisiteWriteBytes,
        ] {
            let mut fixture = mutation_work_lsm_fixture("phase34-lsm-flush");
            let window = evidence_window(&mut fixture);
            let proposal = columnar_proposal(
                &fixture,
                &window,
                PhysicalColumnarDesignMode::Snapshot,
                "admitted",
            );
            let inspection = fixture
                .database
                .inspect_physical_columnar_design_mutation_work(&candidate(), proposal.mode())
                .unwrap();
            let Prerequisite::LsmFlush {
                conservative_bound: flush,
                ..
            } = inspection.prerequisite
            else {
                panic!("flush prerequisite")
            };
            let error = columnar_rejection(
                &mut fixture,
                &window,
                &proposal,
                constraint(Dimension::SourceWorkUnits, u64::MAX),
            );
            assert_unknown(error, Dimension::SourceWorkUnits);
            for (d, component) in [
                (
                    Dimension::SourceReadBytes,
                    bound(inspection.bounds.source_read_bytes),
                ),
                (
                    Dimension::OutputWriteBytes,
                    bound(inspection.bounds.output_write_bytes),
                ),
            ] {
                let error = columnar_rejection(
                    &mut fixture,
                    &window,
                    &proposal,
                    constraint(d, component - 1),
                );
                assert_limit(error, d, component, component - 1);
            }
            let n = match dimension {
                Dimension::PrerequisiteWorkUnits => flush.work_units,
                Dimension::PrerequisiteReadBytes => flush.read_bytes,
                Dimension::PrerequisiteWriteBytes => flush.write_bytes,
                _ => unreachable!(),
            };
            if n > 0 {
                let error = columnar_rejection(
                    &mut fixture,
                    &window,
                    &proposal,
                    constraint(dimension, n - 1),
                );
                assert_limit(error, dimension, n, n - 1);
            } else {
                // The flush read component is exactly zero, so no u64 maximum
                // exists one below it. Equality at zero is the boundary test.
                assert_eq!(dimension, Dimension::PrerequisiteReadBytes);
            }
            activity::take();
            let mut limits = constraint(dimension, n).limits();
            limits.source_read_bytes = AtMost(bound(inspection.bounds.source_read_bytes));
            limits.output_write_bytes = AtMost(bound(inspection.bounds.output_write_bytes));
            fixture
                .database
                .apply_physical_columnar_design_with_admission(
                    &window,
                    &proposal,
                    Admission::new(limits).unwrap(),
                )
                .unwrap();
            let actual = activity::take();
            assert_eq!(actual.flush_calls, 1);
            assert_eq!(actual.scan_columns_calls, 1);
            assert_eq!(actual.analyze_calls, 0);
            let source = fixture
                .database
                .inspect_lsm_storage(TABLE_ID)
                .unwrap()
                .unwrap();
            assert_eq!(source.memtable_entry_count, 0);
            assert_eq!(source.sstable_count, 1);
            fixture.close();
        }
    }

    #[test]
    fn admission_recomputes_after_analyze_dml_and_ignores_retained_work_report() {
        let mut fixture = Fixture::create("phase34-fresh-index", false);
        fixture.database.analyze(TABLE_ID).unwrap();
        let window = evidence_window(&mut fixture);
        let proposal = fixture
            .database
            .propose_physical_index_design(&window, policy(1, 1, 0, 8), index_candidate())
            .unwrap();
        let old = fixture
            .database
            .inspect_physical_index_design_mutation_work(index_candidate())
            .unwrap();
        let catalog = fixture.database.inspect_catalog().unwrap();
        let mut transaction = fixture.database.begin_transaction().unwrap();
        for id in 512..1024 {
            fixture
                .database
                .insert_in(
                    &mut transaction,
                    &[
                        netbadb_types::ScalarValue::Int64(id),
                        netbadb_types::ScalarValue::Int64(3),
                        netbadb_types::ScalarValue::Text("x".repeat(256)),
                    ],
                )
                .unwrap();
        }
        transaction.commit().unwrap();
        assert_eq!(fixture.database.inspect_catalog().unwrap(), catalog);
        let current = fixture
            .database
            .inspect_physical_index_design_mutation_work(index_candidate())
            .unwrap();
        assert!(bound(current.bounds.source_work_units) > bound(old.bounds.source_work_units));
        let error = pure_rejection(&mut fixture, &window, |database| {
            let IndexError::Admission(error) = database
                .apply_physical_index_design_with_admission(
                    &window,
                    &proposal,
                    name(),
                    constraint(
                        Dimension::SourceWorkUnits,
                        bound(old.bounds.source_work_units),
                    ),
                )
                .unwrap_err()
            else {
                panic!("admission error")
            };
            *error
        });
        assert_limit(
            error,
            Dimension::SourceWorkUnits,
            bound(current.bounds.source_work_units),
            bound(old.bounds.source_work_units),
        );
        fixture.close();

        let mut fixture = mutation_work_lsm_fixture("phase34-fresh-columnar");
        fixture.database.analyze(TABLE_ID).unwrap();
        let window = evidence_window(&mut fixture);
        let proposal = columnar_proposal(
            &fixture,
            &window,
            PhysicalColumnarDesignMode::Snapshot,
            "admitted",
        );
        let old = fixture
            .database
            .inspect_physical_columnar_design_mutation_work(&candidate(), proposal.mode())
            .unwrap();
        let Prerequisite::LsmFlush {
            conservative_bound: old_flush,
            ..
        } = old.prerequisite
        else {
            panic!("flush")
        };
        fixture
            .database
            .execute("INSERT INTO events VALUES (2, 2)")
            .unwrap();
        let current = fixture
            .database
            .inspect_physical_columnar_design_mutation_work(&candidate(), proposal.mode())
            .unwrap();
        let Prerequisite::LsmFlush {
            conservative_bound: current_flush,
            ..
        } = current.prerequisite
        else {
            panic!("flush")
        };
        let error = columnar_rejection(
            &mut fixture,
            &window,
            &proposal,
            constraint(Dimension::PrerequisiteWorkUnits, old_flush.work_units),
        );
        assert_limit(
            error,
            Dimension::PrerequisiteWorkUnits,
            current_flush.work_units,
            old_flush.work_units,
        );
        fixture.close();
    }

    #[test]
    fn admission_exact_and_coverage_noops_precede_even_failing_work_inspection() {
        let mut fixture = Fixture::create("phase34-noops", false);
        let mut window = evidence_window(&mut fixture);
        let index = fixture
            .database
            .propose_physical_index_design(&window, policy(1, 1, 0, 8), index_candidate())
            .unwrap();
        let columnar = columnar_proposal(
            &fixture,
            &window,
            PhysicalColumnarDesignMode::Snapshot,
            "exact",
        );
        let covered = columnar_proposal(
            &fixture,
            &window,
            PhysicalColumnarDesignMode::Snapshot,
            "covered",
        );
        fixture
            .database
            .apply_physical_index_design(&window, &index, name())
            .unwrap();
        fixture
            .database
            .apply_physical_columnar_design(&window, &columnar)
            .unwrap();
        window.rotate_window().unwrap();
        // Metadata corruption would fail a fresh inspection. No-op paths must
        // never invoke it, even with impossible constrained output and work.
        let file = fs::OpenOptions::new()
            .write(true)
            .open(&fixture.source)
            .unwrap();
        let length = file.metadata().unwrap().len();
        file.set_len(length + 1).unwrap();
        assert!(
            fixture
                .database
                .inspect_physical_index_design_mutation_work(index_candidate())
                .is_err()
        );
        assert!(
            fixture
                .database
                .inspect_physical_columnar_design_mutation_work(&candidate(), columnar.mode())
                .is_err()
        );
        for d in [Dimension::SourceWorkUnits, Dimension::OutputWriteBytes] {
            let admission = constraint(d, 0);
            activity::take();
            assert!(matches!(
                fixture
                    .database
                    .apply_physical_index_design_with_admission(&window, &index, name(), admission)
                    .unwrap()
                    .outcome,
                PhysicalIndexDesignApplyOutcome::AlreadyApplied { .. }
            ));
            assert_eq!(
                fixture
                    .database
                    .apply_physical_index_design_with_admission(
                        &window,
                        &index,
                        IndexName::new("covered").unwrap(),
                        admission
                    )
                    .unwrap()
                    .outcome,
                PhysicalIndexDesignApplyOutcome::AlreadyCovered
            );
            assert!(matches!(
                fixture
                    .database
                    .apply_physical_columnar_design_with_admission(&window, &columnar, admission)
                    .unwrap()
                    .outcome,
                PhysicalColumnarDesignApplyOutcome::AlreadyApplied { .. }
            ));
            assert_eq!(
                fixture
                    .database
                    .apply_physical_columnar_design_with_admission(&window, &covered, admission)
                    .unwrap()
                    .outcome,
                PhysicalColumnarDesignApplyOutcome::AlreadyCovered
            );
            assert_eq!(activity::take(), activity::Activity::default());
        }
        file.set_len(length).unwrap();
        drop(file);
        fixture.close();
    }

    #[test]
    fn admission_preserves_preflight_errors_and_inspection_source_chains() {
        let mut fixture = Fixture::create("phase34-errors", false);
        fixture.database.enable_change_stream(TABLE_ID).unwrap();
        let mut window = evidence_window(&mut fixture);
        let index = fixture
            .database
            .propose_physical_index_design(&window, policy(1, 1, 0, 8), index_candidate())
            .unwrap();
        let columnar = columnar_proposal(
            &fixture,
            &window,
            PhysicalColumnarDesignMode::Incremental,
            "admitted",
        );
        let admission = constraint(Dimension::OutputWriteBytes, 0);
        let file = fs::OpenOptions::new()
            .write(true)
            .open(&fixture.source)
            .unwrap();
        let length = file.metadata().unwrap().len();
        file.set_len(length + 1).unwrap();
        let index_error = fixture
            .database
            .apply_physical_index_design_with_admission(&window, &index, name(), admission)
            .unwrap_err();
        let columnar_error = fixture
            .database
            .apply_physical_columnar_design_with_admission(&window, &columnar, admission)
            .unwrap_err();
        for error in [&index_error as &dyn Error, &columnar_error as &dyn Error] {
            let admission = error
                .source()
                .unwrap()
                .downcast_ref::<AdmissionError>()
                .unwrap();
            let AdmissionError::Inspection(inspection) = admission else {
                panic!("inspection error")
            };
            assert!(
                inspection
                    .source()
                    .unwrap()
                    .source()
                    .unwrap()
                    .downcast_ref::<netbadb_storage::StorageError>()
                    .is_some()
            );
        }
        file.set_len(length).unwrap();
        drop(file);
        window.rotate_window().unwrap();
        assert!(
            matches!(fixture.database.apply_physical_index_design_with_admission(&window, &index, name(), admission), Err(IndexError::Apply(error)) if matches!(*error, PhysicalIndexDesignApplyError::EvidenceEpochChanged { .. }))
        );
        assert!(
            matches!(fixture.database.apply_physical_columnar_design_with_admission(&window, &columnar, admission), Err(ColumnarError::Apply(error)) if matches!(*error, PhysicalColumnarDesignApplyError::EvidenceEpochChanged { .. }))
        );
        fixture.database.disable_change_stream(TABLE_ID).unwrap();
        assert!(
            matches!(fixture.database.apply_physical_columnar_design_with_admission(&window, &columnar, admission), Err(ColumnarError::Apply(error)) if matches!(*error, PhysicalColumnarDesignApplyError::StaleProposal(PhysicalColumnarDesignProposalStaleReason::ChangeStreamDisabled)))
        );
        fixture
            .database
            .create_named_index(name(), TABLE_ID, ColumnId(1))
            .unwrap();
        assert!(
            matches!(fixture.database.apply_physical_index_design_with_admission(&window, &index, name(), admission), Err(IndexError::Apply(error)) if matches!(*error, PhysicalIndexDesignApplyError::IndexNameConflict(_)))
        );
        fixture.close();
    }
}
