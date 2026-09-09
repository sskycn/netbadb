use netbadb_planner::{PlannerCalibrationClass, PlannerCalibrationEpoch};
use netbadb_types::{ColumnarGeneration, ColumnarProjectionId, DatabaseCommitSeq, StorageId};

use crate::adaptive_workload_tests::{TimelineFixture, set_target_work, workload_target};
use crate::{
    AdaptiveEvidencePool, AdaptiveEvidencePoolLimits, AdaptiveEvidenceRecordError,
    AdaptiveEvidenceRecordOutcome, AdaptiveTargetLineage, AdaptiveWorkloadLimits,
};

fn feedback(fixture: &mut TimelineFixture) -> crate::ExecutionFeedbackReport {
    let target = workload_target(&fixture.database);
    let (_, mut report) = fixture
        .database
        .query_with_feedback("SELECT id FROM events")
        .expect("query feedback");
    set_target_work(&mut report, target, 10, 12, 20);
    report
}

fn set_generation(report: &mut crate::ExecutionFeedbackReport, generation: ColumnarGeneration) {
    for access in &mut report.accesses {
        let Some(planner) = &mut access.planner else {
            continue;
        };
        if planner.projection_id.is_none() {
            continue;
        }
        planner.projection_generation = Some(generation);
        if let Some(columnar) = &mut access.actual.work.columnar {
            columnar.generation = Some(generation);
        }
    }
}

fn set_storage(report: &mut crate::ExecutionFeedbackReport, storage_id: StorageId) {
    for access in &mut report.accesses {
        let Some(planner) = &mut access.planner else {
            continue;
        };
        if planner.projection_id.is_none() {
            continue;
        }
        planner.storage_id = Some(storage_id);
        access.actual.storage_id = storage_id;
        if let Some(columnar) = &mut access.actual.work.columnar {
            columnar.storage_id = Some(storage_id);
        }
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

#[test]
fn ingestion_is_explicit_and_cross_g_is_one_timeline() {
    let mut fixture = TimelineFixture::create("phase6-explicit-cross-g");
    let pool = AdaptiveEvidencePool::default();
    let _ = fixture
        .database
        .query_with_feedback("SELECT id FROM events")
        .expect("unrecorded feedback");
    assert_eq!(pool.inspection().recorded_reports, 0);

    let mut pool = pool;
    let report = feedback(&mut fixture);
    for visibility in [100, 101, 105] {
        let mut sample = report.clone();
        sample.anchor.global_commit_seq = Some(DatabaseCommitSeq(visibility));
        pool.record_execution_feedback(&sample)
            .expect("ordered evidence");
    }
    let inspection = pool.inspection();
    assert_eq!(inspection.recorded_reports, 3);
    assert_eq!(inspection.target_windows.len(), 1);
    assert_eq!(inspection.target_windows[0].sample_count, 3);
    assert_eq!(inspection.target_windows[0].distinct_visibility_points, 3);
    assert_eq!(
        pool.calibration_evidence(
            PlannerCalibrationClass::Columnar,
            PlannerCalibrationEpoch(0),
            0,
        )
        .expect("calibration evidence")
        .sample_count,
        3
    );
    fixture.close();
}

#[test]
fn one_report_fans_out_targets_but_calibration_report_counts_once() {
    let mut fixture = TimelineFixture::create("phase6-multi-target-report");
    let mut report = feedback(&mut fixture);
    report.anchor.global_commit_seq = Some(DatabaseCommitSeq(200));
    let mut second = report.accesses[0].clone();
    let second_id = ColumnarProjectionId(9_999);
    second.planner.as_mut().expect("planner").projection_id = Some(second_id);
    second
        .actual
        .work
        .columnar
        .as_mut()
        .expect("columnar")
        .projection_id = Some(second_id);
    report.accesses.push(second);

    let mut pool = AdaptiveEvidencePool::default();
    let recorded = pool
        .record_execution_feedback(&report)
        .expect("multi-target record");
    assert_eq!(recorded.target_windows_recorded, 2);
    assert_eq!(recorded.calibration_reports_recorded, 1);
    assert_eq!(pool.inspection().target_windows.len(), 2);
    let calibration = pool
        .calibration_evidence(
            PlannerCalibrationClass::Columnar,
            PlannerCalibrationEpoch(0),
            0,
        )
        .expect("global calibration");
    assert_eq!(calibration.sample_count, 1);
    assert_eq!(calibration.total_actual_work_units, 24);
    fixture.close();
}

#[test]
fn schema_and_generation_rotation_are_forward_only() {
    let mut fixture = TimelineFixture::create("phase6-rotation");
    let mut initial = feedback(&mut fixture);
    initial.anchor.global_commit_seq = Some(DatabaseCommitSeq(300));
    let target = workload_target(&fixture.database);
    let mut pool = AdaptiveEvidencePool::default();
    pool.record_execution_feedback(&initial)
        .expect("initial evidence");

    let mut next = initial.clone();
    next.anchor.global_commit_seq = Some(DatabaseCommitSeq(301));
    set_generation(&mut next, ColumnarGeneration(target.generation.0 + 1));
    let rotated = pool
        .record_execution_feedback(&next)
        .expect("forward generation");
    assert_eq!(rotated.target_windows_rotated, 1);
    assert_eq!(
        pool.current_target_window(AdaptiveTargetLineage {
            table_id: target.table_id,
            projection_id: target.projection_id,
        })
        .expect("current target")
        .target
        .generation,
        ColumnarGeneration(target.generation.0 + 1)
    );
    let mut old = initial.clone();
    old.anchor.global_commit_seq = Some(DatabaseCommitSeq(302));
    assert!(matches!(
        pool.record_execution_feedback(&old),
        Err(AdaptiveEvidenceRecordError::StaleTargetGenerationEvidence { .. })
            | Err(AdaptiveEvidenceRecordError::RetiredTargetEvidence { .. })
    ));

    let mut next_storage = next.clone();
    next_storage.anchor.global_commit_seq = Some(DatabaseCommitSeq(303));
    set_storage(&mut next_storage, StorageId(target.storage_id.0 + 1));
    pool.record_execution_feedback(&next_storage)
        .expect("forward storage identity");
    let mut old_storage = next.clone();
    old_storage.anchor.global_commit_seq = Some(DatabaseCommitSeq(304));
    assert!(matches!(
        pool.record_execution_feedback(&old_storage),
        Err(AdaptiveEvidenceRecordError::StaleTargetIdentityEvidence { .. })
            | Err(AdaptiveEvidenceRecordError::RetiredTargetEvidence { .. })
    ));

    let mut new_schema = next_storage.clone();
    new_schema.anchor.schema_generation = crate::SchemaGeneration(target.schema_generation.0 + 1);
    new_schema.anchor.global_commit_seq = Some(DatabaseCommitSeq(400));
    let report = pool
        .record_execution_feedback(&new_schema)
        .expect("schema rotation");
    assert_eq!(report.outcome, AdaptiveEvidenceRecordOutcome::SchemaRotated);
    assert_eq!(
        pool.inspection().schema_generation,
        Some(new_schema.anchor.schema_generation)
    );
    assert!(matches!(
        pool.record_execution_feedback(&next),
        Err(AdaptiveEvidenceRecordError::StaleSchemaEvidence { .. })
    ));
    fixture.close();
}

#[test]
fn out_of_order_is_atomic_and_capacity_is_typed_incomplete() {
    let mut fixture = TimelineFixture::create("phase6-order-capacity");
    let mut report = feedback(&mut fixture);
    report.anchor.global_commit_seq = Some(DatabaseCommitSeq(500));
    let mut pool = AdaptiveEvidencePool::new(AdaptiveEvidencePoolLimits {
        max_target_windows: 1,
        workload_limits: AdaptiveWorkloadLimits::default(),
        ..AdaptiveEvidencePoolLimits::default()
    });
    pool.record_execution_feedback(&report)
        .expect("first record");
    let before = pool.inspection();
    let mut older = report.clone();
    older.anchor.global_commit_seq = Some(DatabaseCommitSeq(499));
    assert!(matches!(
        pool.record_execution_feedback(&older),
        Err(AdaptiveEvidenceRecordError::OutOfOrderVisibility { .. })
    ));
    assert_eq!(pool.inspection(), before);

    let mut new_target = report.clone();
    new_target.anchor.global_commit_seq = Some(DatabaseCommitSeq(501));
    let new_id = ColumnarProjectionId(8_888);
    for access in &mut new_target.accesses {
        if let Some(planner) = &mut access.planner {
            planner.projection_id = Some(new_id);
        }
        if let Some(columnar) = &mut access.actual.work.columnar {
            columnar.projection_id = Some(new_id);
        }
    }
    let capacity = pool
        .record_execution_feedback(&new_target)
        .expect("typed capacity result");
    assert_eq!(
        capacity.outcome,
        AdaptiveEvidenceRecordOutcome::RecordedWithCapacityRejection
    );
    assert!(pool.inspection().incomplete);
    fixture.close();
}

#[test]
fn calibration_epoch_retention_evicts_oldest_without_mutation() {
    let mut fixture = TimelineFixture::create("phase6-epoch-retention");
    let report = feedback(&mut fixture);
    let mut pool = AdaptiveEvidencePool::new(AdaptiveEvidencePoolLimits {
        max_calibration_epochs: 2,
        ..AdaptiveEvidencePoolLimits::default()
    });
    for epoch in 0..3 {
        let mut sample = report.clone();
        sample.anchor.global_commit_seq = Some(DatabaseCommitSeq(600 + epoch));
        set_epoch(&mut sample, PlannerCalibrationEpoch(epoch));
        pool.record_execution_feedback(&sample)
            .expect("epoch evidence");
    }
    let epochs = pool
        .inspection()
        .calibration_epochs
        .into_iter()
        .map(|entry| entry.epoch)
        .collect::<Vec<_>>();
    assert_eq!(
        epochs,
        vec![PlannerCalibrationEpoch(1), PlannerCalibrationEpoch(2)]
    );
    assert!(
        pool.calibration_evidence(
            PlannerCalibrationClass::Columnar,
            PlannerCalibrationEpoch(0),
            0,
        )
        .is_none()
    );
    assert_eq!(
        fixture.database.planner_calibration_profile().epoch,
        PlannerCalibrationEpoch(0)
    );
    fixture.close();
}
