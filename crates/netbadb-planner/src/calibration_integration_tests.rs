use netbadb_index::{IndexStatistics, TableStatistics};
use netbadb_rel::{BinaryOp, ColumnRef, Expr, ExprKind, JoinKind, LogicalPlan};
use netbadb_types::{
    AccessPathId, ColumnId, ColumnarGeneration, ColumnarProjectionId, ExprType, PhysicalType,
    RelationBindingId, SemanticType, StorageId, TableId,
};

use crate::{
    AccessPath, AccessPathCapabilities, CalibrationRatio, ColumnarProjectionPlanningSnapshot,
    ColumnarRowGroupPlanningSnapshot, ColumnarZoneMapPlanningSnapshot, PhysicalPlan,
    PlannerAccessKind, PlannerActualAccessEvidence, PlannerCalibrationClass,
    PlannerCalibrationEpoch, PlannerCalibrationProfile, TableAccessStatistics,
    estimate_execution_accesses, estimate_execution_accesses_with_calibration,
    evaluate_actual_access_work, plan_with_columnar_snapshots,
    plan_with_columnar_snapshots_and_calibration, plan_with_partition_snapshots_and_calibration,
};

fn column(table: u64, binding: u32) -> ColumnRef {
    ColumnRef {
        binding_id: RelationBindingId(binding),
        table_id: TableId(table),
        column_id: ColumnId(1),
        relation_name: format!("t{table}"),
        name: "id".to_owned(),
        data_type: SemanticType::physical(PhysicalType::Int64),
        nullable: false,
    }
}

fn scan(column: &ColumnRef) -> LogicalPlan {
    LogicalPlan::Scan {
        binding_id: column.binding_id,
        table_id: column.table_id,
        table_name: column.relation_name.clone(),
        columns: vec![column.clone()],
    }
}

#[test]
fn identity_preserves_plan_and_calibration_changes_real_columnar_choice() {
    let id = column(1, 1);
    let logical = scan(&id);
    let statistics = [TableAccessStatistics {
        table_id: TableId(1),
        statistics: Some(TableStatistics {
            row_count: 0,
            managed_page_count: 4,
        }),
    }];
    let projection = ColumnarProjectionPlanningSnapshot {
        projection_id: ColumnarProjectionId(1),
        generation: ColumnarGeneration(1),
        table_id: TableId(1),
        source_storage_id: StorageId(1),
        projected_columns: vec![ColumnId(1)],
        row_count: 0,
        row_group_count: 1,
        segment_bytes: 0,
        delta_segment_count: 0,
        delta_bytes: 0,
        delta_mutation_count: 0,
        delta_live_row_count: 0,
        suppressed_version_count: 0,
        row_groups: vec![ColumnarRowGroupPlanningSnapshot {
            rows: 0,
            columns: vec![ColumnarZoneMapPlanningSnapshot {
                column_id: ColumnId(1),
                null_count: 0,
                minimum: None,
                maximum: None,
                encoded_bytes: 0,
            }],
        }],
    };
    let baseline = plan_with_columnar_snapshots(
        &logical,
        &statistics,
        &[],
        &[],
        std::slice::from_ref(&projection),
    );
    let identity = plan_with_columnar_snapshots_and_calibration(
        &logical,
        &statistics,
        &[],
        &[],
        std::slice::from_ref(&projection),
        &PlannerCalibrationProfile::IDENTITY,
    );
    assert_eq!(identity, baseline);
    assert!(matches!(baseline, PhysicalPlan::ColumnarScan { .. }));

    let profile = PlannerCalibrationProfile::IDENTITY.with_ratio(
        PlannerCalibrationEpoch(1),
        PlannerCalibrationClass::Columnar,
        CalibrationRatio::DOUBLE,
    );
    let calibrated = plan_with_columnar_snapshots_and_calibration(
        &logical,
        &statistics,
        &[],
        &[],
        std::slice::from_ref(&projection),
        &profile,
    );
    assert!(matches!(calibrated, PhysicalPlan::SeqScan { .. }));

    let base_estimate = estimate_execution_accesses(
        &baseline,
        &statistics,
        &[],
        &[],
        std::slice::from_ref(&projection),
    )
    .remove(0);
    let calibrated_estimate = estimate_execution_accesses_with_calibration(
        &baseline,
        &statistics,
        &[],
        &[],
        std::slice::from_ref(&projection),
        &profile,
    )
    .remove(0);
    assert_eq!(
        base_estimate.estimated_work_units,
        calibrated_estimate.estimated_work_units
    );
    assert_eq!(base_estimate.effective_work_units, Some(3));
    assert_eq!(calibrated_estimate.effective_work_units, Some(6));
    let actual = PlannerActualAccessEvidence {
        columnar: Some(Default::default()),
        ..PlannerActualAccessEvidence::default()
    };
    assert_eq!(
        evaluate_actual_access_work(&base_estimate, actual),
        evaluate_actual_access_work(&calibrated_estimate, actual)
    );
}

#[test]
fn partition_classes_are_global_and_point_overlay_changes_index_join_choice() {
    assert_eq!(
        PlannerAccessKind::PartitionedSeqScan.calibration_class(),
        PlannerCalibrationClass::SeqScan
    );
    assert_eq!(
        PlannerAccessKind::PartitionedIndexPoint.calibration_class(),
        PlannerCalibrationClass::IndexPoint
    );
    assert_eq!(
        PlannerAccessKind::PartitionedIndexRange.calibration_class(),
        PlannerCalibrationClass::IndexRange
    );

    let left = column(1, 1);
    let right = column(2, 2);
    let predicate = Expr {
        kind: ExprKind::Binary {
            operator: BinaryOp::Eq,
            left: Box::new(Expr {
                kind: ExprKind::Column(left.clone()),
                expr_type: ExprType {
                    data_type: left.data_type.clone(),
                    nullable: false,
                },
            }),
            right: Box::new(Expr {
                kind: ExprKind::Column(right.clone()),
                expr_type: ExprType {
                    data_type: right.data_type.clone(),
                    nullable: false,
                },
            }),
        },
        expr_type: ExprType {
            data_type: SemanticType::physical(PhysicalType::Bool),
            nullable: false,
        },
    };
    let logical = LogicalPlan::Join {
        left: Box::new(scan(&left)),
        right: Box::new(scan(&right)),
        kind: JoinKind::Inner,
        predicate,
        columns: vec![left, right],
    };
    let statistics = [
        TableAccessStatistics {
            table_id: TableId(1),
            statistics: Some(TableStatistics {
                row_count: 1,
                managed_page_count: 1,
            }),
        },
        TableAccessStatistics {
            table_id: TableId(2),
            statistics: Some(TableStatistics {
                row_count: 100,
                managed_page_count: 10,
            }),
        },
    ];
    let paths = [AccessPath {
        table_id: TableId(2),
        column_id: ColumnId(1),
        id: AccessPathId(7),
        capabilities: AccessPathCapabilities {
            point_lookup: true,
            range_lookup: true,
            ordered: true,
        },
        statistics: Some(IndexStatistics {
            distinct_non_null_keys: 100,
            null_count: 0,
            tree_height: 1,
        }),
        cost_hints: None,
    }];
    let baseline = plan_with_partition_snapshots_and_calibration(
        &logical,
        &statistics,
        &paths,
        &[],
        &PlannerCalibrationProfile::IDENTITY,
    );
    assert!(matches!(baseline, PhysicalPlan::IndexNestedLoopJoin { .. }));
    let profile = PlannerCalibrationProfile::IDENTITY.with_ratio(
        PlannerCalibrationEpoch(1),
        PlannerCalibrationClass::IndexPoint,
        CalibrationRatio::new(4, 1).expect("ratio"),
    );
    let calibrated =
        plan_with_partition_snapshots_and_calibration(&logical, &statistics, &paths, &[], &profile);
    assert!(!matches!(
        calibrated,
        PhysicalPlan::IndexNestedLoopJoin { .. }
    ));
}
