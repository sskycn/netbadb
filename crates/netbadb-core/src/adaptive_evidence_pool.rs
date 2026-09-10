use std::error::Error;
use std::fmt;

use netbadb_planner::{PlannerCalibrationClass, PlannerCalibrationEpoch};
use netbadb_types::{
    ColumnarGeneration, ColumnarProjectionId, DatabaseCommitSeq, SchemaGeneration, StorageId,
    TableId,
};

use crate::adaptive_workload::{
    AdaptiveQueryShapeAggregate, CalibrationVisibilityEvidence, record_calibration_report,
};
use crate::planner_calibration::aggregate_calibration_evidence_parts;
use crate::{
    AdaptiveWorkloadLimits, AdaptiveWorkloadRecordError, AdaptiveWorkloadTarget,
    AdaptiveWorkloadWindow, ExecutionFeedbackReport, PlannerCalibrationEvidence,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdaptiveEvidencePoolLimits {
    pub max_target_windows: u64,
    pub workload_limits: AdaptiveWorkloadLimits,
    pub max_calibration_epochs: u64,
    pub max_calibration_query_shapes: u64,
    pub max_calibration_plan_variants_per_shape: u64,
}

impl Default for AdaptiveEvidencePoolLimits {
    fn default() -> Self {
        Self {
            max_target_windows: 16,
            workload_limits: AdaptiveWorkloadLimits::default(),
            max_calibration_epochs: 4,
            max_calibration_query_shapes: 64,
            max_calibration_plan_variants_per_shape: 8,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct AdaptiveTargetLineage {
    pub table_id: TableId,
    pub projection_id: ColumnarProjectionId,
}

/// Runtime aggregation lifetime. It is independent of schema, physical, and
/// planner-calibration generations.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct AdaptiveEvidenceWindowEpoch(pub u64);

/// Allocation-free progress coordinate for cooperative adaptive scheduling.
///
/// This token only indicates that the caller-owned aggregation state may have
/// changed. It is not evidence quality, freshness, or mutation authority.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct AdaptiveEvidenceProgressToken {
    pub window_epoch: AdaptiveEvidenceWindowEpoch,
    pub schema_generation: Option<SchemaGeneration>,
    pub recorded_reports: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdaptiveEvidencePoolHealth {
    Healthy,
    RotationRecommended,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdaptiveEvidenceRotationError {
    EvidenceWindowEpochExhausted,
}

impl fmt::Display for AdaptiveEvidenceRotationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EvidenceWindowEpochExhausted => {
                formatter.write_str("adaptive evidence window epoch is exhausted")
            }
        }
    }
}

impl Error for AdaptiveEvidenceRotationError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdaptiveEvidenceRotationReport {
    pub previous_window_epoch: AdaptiveEvidenceWindowEpoch,
    pub new_window_epoch: AdaptiveEvidenceWindowEpoch,
    pub schema_generation: Option<SchemaGeneration>,
    pub ordering_high_water: Option<DatabaseCommitSeq>,
    pub discarded_target_window_count: u64,
    pub discarded_calibration_epoch_count: u64,
    pub discarded_query_shape_count: u64,
    pub was_incomplete: bool,
    pub was_truncated: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdaptiveEvidenceRecordError {
    GlobalVisibilityRequired,
    StaleSchemaEvidence {
        current: SchemaGeneration,
        received: SchemaGeneration,
    },
    OutOfOrderVisibility {
        previous: DatabaseCommitSeq,
        received: DatabaseCommitSeq,
    },
    StaleTargetGenerationEvidence {
        lineage: AdaptiveTargetLineage,
        current: ColumnarGeneration,
        received: ColumnarGeneration,
    },
    StaleTargetIdentityEvidence {
        lineage: AdaptiveTargetLineage,
        current_storage_id: StorageId,
        received_storage_id: StorageId,
    },
    RetiredTargetEvidence {
        target: AdaptiveWorkloadTarget,
    },
    StaleCalibrationEpochEvidence {
        minimum: PlannerCalibrationEpoch,
        received: PlannerCalibrationEpoch,
    },
    EvidenceWindowEpochExhausted,
}

impl fmt::Display for AdaptiveEvidenceRecordError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::GlobalVisibilityRequired => {
                formatter.write_str("adaptive evidence requires global visibility")
            }
            Self::StaleSchemaEvidence { current, received } => write!(
                formatter,
                "schema evidence {} is older than current pool schema {}",
                received.0, current.0
            ),
            Self::OutOfOrderVisibility { previous, received } => write!(
                formatter,
                "evidence visibility {} follows newer visibility {}",
                received.0, previous.0
            ),
            Self::StaleTargetGenerationEvidence {
                lineage,
                current,
                received,
            } => write!(
                formatter,
                "target {}/{} generation {} is older than current generation {}",
                lineage.table_id.0, lineage.projection_id.0, received.0, current.0
            ),
            Self::StaleTargetIdentityEvidence {
                lineage,
                current_storage_id,
                received_storage_id,
            } => write!(
                formatter,
                "target {}/{} storage {} is older than current storage {}",
                lineage.table_id.0,
                lineage.projection_id.0,
                received_storage_id.0,
                current_storage_id.0
            ),
            Self::RetiredTargetEvidence { target } => write!(
                formatter,
                "evidence targets retired physical identity {}/{}/{}/{}",
                target.table_id.0, target.storage_id.0, target.projection_id.0, target.generation.0
            ),
            Self::StaleCalibrationEpochEvidence { minimum, received } => write!(
                formatter,
                "calibration epoch {} is older than the renewed window floor {}",
                received.0, minimum.0
            ),
            Self::EvidenceWindowEpochExhausted => {
                formatter.write_str("adaptive evidence window epoch is exhausted")
            }
        }
    }
}

impl Error for AdaptiveEvidenceRecordError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdaptiveEvidenceRecordOutcome {
    Recorded,
    SchemaRotated,
    RecordedWithCapacityRejection,
    SchemaRotatedWithCapacityRejection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdaptiveEvidenceRecordReport {
    pub outcome: AdaptiveEvidenceRecordOutcome,
    pub target_windows_recorded: u64,
    pub target_windows_rotated: u64,
    /// One means the report entered the global calibration accumulator once.
    pub calibration_reports_recorded: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TargetWindowEntry {
    lineage: AdaptiveTargetLineage,
    window: AdaptiveWorkloadWindow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TargetLineageState {
    lineage: AdaptiveTargetLineage,
    storage_id: StorageId,
    generation: ColumnarGeneration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CalibrationReportCount {
    class: PlannerCalibrationClass,
    epoch: PlannerCalibrationEpoch,
    reports: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdaptiveTargetWindowInspection {
    pub lineage: AdaptiveTargetLineage,
    pub target: AdaptiveWorkloadTarget,
    pub first_global_commit_seq: Option<DatabaseCommitSeq>,
    pub last_global_commit_seq: Option<DatabaseCommitSeq>,
    pub sample_count: u64,
    pub distinct_visibility_points: u64,
    pub overflowed: bool,
    pub incomplete: bool,
    pub truncated: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdaptiveCalibrationEpochInspection {
    pub epoch: PlannerCalibrationEpoch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdaptiveCalibrationGroupInspection {
    pub calibration_class: PlannerCalibrationClass,
    pub epoch: PlannerCalibrationEpoch,
    pub report_samples: u64,
    pub distinct_visibility_points: u64,
    pub overflowed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdaptiveEvidencePoolInspection {
    pub window_epoch: AdaptiveEvidenceWindowEpoch,
    pub health: AdaptiveEvidencePoolHealth,
    pub schema_generation: Option<SchemaGeneration>,
    pub first_global_commit_seq: Option<DatabaseCommitSeq>,
    pub last_global_commit_seq: Option<DatabaseCommitSeq>,
    pub recorded_reports: u64,
    pub target_windows: Vec<AdaptiveTargetWindowInspection>,
    pub calibration_epochs: Vec<AdaptiveCalibrationEpochInspection>,
    pub calibration_groups: Vec<AdaptiveCalibrationGroupInspection>,
    pub overflowed: bool,
    pub incomplete: bool,
    pub truncated: bool,
    pub target_capacity_rejections: u64,
}

/// Caller-owned, bounded, runtime-only multi-target evidence storage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdaptiveEvidencePool {
    limits: AdaptiveEvidencePoolLimits,
    window_epoch: AdaptiveEvidenceWindowEpoch,
    schema_generation: Option<SchemaGeneration>,
    first_global_commit_seq: Option<DatabaseCommitSeq>,
    last_global_commit_seq: Option<DatabaseCommitSeq>,
    ordering_high_water: Option<DatabaseCommitSeq>,
    recorded_reports: u64,
    target_lineages: Vec<TargetLineageState>,
    target_windows: Vec<TargetWindowEntry>,
    retired_targets: Vec<AdaptiveWorkloadTarget>,
    calibration_epochs: Vec<PlannerCalibrationEpoch>,
    calibration_epoch_high_water: Option<PlannerCalibrationEpoch>,
    calibration_epoch_floor: Option<PlannerCalibrationEpoch>,
    calibration_query_shapes: Vec<AdaptiveQueryShapeAggregate>,
    calibration_visibility: Vec<CalibrationVisibilityEvidence>,
    calibration_report_counts: Vec<CalibrationReportCount>,
    calibration_overflowed: bool,
    calibration_incomplete: bool,
    calibration_truncated: bool,
    target_capacity_rejections: u64,
}

impl AdaptiveEvidencePool {
    #[must_use]
    pub const fn new(limits: AdaptiveEvidencePoolLimits) -> Self {
        Self {
            limits,
            window_epoch: AdaptiveEvidenceWindowEpoch(0),
            schema_generation: None,
            first_global_commit_seq: None,
            last_global_commit_seq: None,
            ordering_high_water: None,
            recorded_reports: 0,
            target_lineages: Vec::new(),
            target_windows: Vec::new(),
            retired_targets: Vec::new(),
            calibration_epochs: Vec::new(),
            calibration_epoch_high_water: None,
            calibration_epoch_floor: None,
            calibration_query_shapes: Vec::new(),
            calibration_visibility: Vec::new(),
            calibration_report_counts: Vec::new(),
            calibration_overflowed: false,
            calibration_incomplete: false,
            calibration_truncated: false,
            target_capacity_rejections: 0,
        }
    }

    #[must_use]
    pub const fn limits(&self) -> AdaptiveEvidencePoolLimits {
        self.limits
    }

    #[must_use]
    pub const fn schema_generation(&self) -> Option<SchemaGeneration> {
        self.schema_generation
    }

    #[must_use]
    pub const fn window_epoch(&self) -> AdaptiveEvidenceWindowEpoch {
        self.window_epoch
    }

    /// Returns an O(1), read-only scheduling hint without materializing the
    /// target and calibration vectors returned by `inspection`.
    #[must_use]
    pub const fn progress_token(&self) -> AdaptiveEvidenceProgressToken {
        AdaptiveEvidenceProgressToken {
            window_epoch: self.window_epoch,
            schema_generation: self.schema_generation,
            recorded_reports: self.recorded_reports,
        }
    }

    pub fn clear(&mut self) {
        *self = Self::new(self.limits);
    }

    /// Starts an empty aggregation window while retaining same-schema
    /// ordering and physical-identity rollback guards.
    pub fn rotate_window(
        &mut self,
    ) -> Result<AdaptiveEvidenceRotationReport, AdaptiveEvidenceRotationError> {
        let next = self
            .window_epoch
            .0
            .checked_add(1)
            .map(AdaptiveEvidenceWindowEpoch)
            .ok_or(AdaptiveEvidenceRotationError::EvidenceWindowEpochExhausted)?;
        let report = self.rotation_report(next);
        self.window_epoch = next;
        self.calibration_epoch_floor = self.calibration_epoch_high_water;
        self.clear_aggregation_payload();
        Ok(report)
    }

    pub fn remove_target(&mut self, lineage: AdaptiveTargetLineage) -> bool {
        let before = self.target_windows.len();
        self.target_windows.retain(|entry| entry.lineage != lineage);
        before != self.target_windows.len()
    }

    #[must_use]
    pub fn target_window(&self, target: AdaptiveWorkloadTarget) -> Option<&AdaptiveWorkloadWindow> {
        self.target_windows
            .iter()
            .find(|entry| entry.window.target == target)
            .map(|entry| &entry.window)
    }

    #[must_use]
    pub fn current_target_window(
        &self,
        lineage: AdaptiveTargetLineage,
    ) -> Option<&AdaptiveWorkloadWindow> {
        self.target_windows
            .iter()
            .find(|entry| entry.lineage == lineage)
            .map(|entry| &entry.window)
    }

    #[must_use]
    pub fn calibration_evidence(
        &self,
        class: PlannerCalibrationClass,
        epoch: PlannerCalibrationEpoch,
        deadband: u64,
    ) -> Option<PlannerCalibrationEvidence> {
        let schema_generation = self.schema_generation?;
        let report_count = self
            .calibration_report_counts
            .iter()
            .find(|count| count.class == class && count.epoch == epoch)
            .map(|count| count.reports)?;
        let mut evidence = aggregate_calibration_evidence_parts(
            schema_generation,
            &self.calibration_query_shapes,
            &self.calibration_visibility,
            self.calibration_overflowed,
            self.calibration_incomplete,
            self.calibration_truncated,
            class,
            epoch,
            deadband,
        );
        evidence.sample_count = report_count;
        Some(evidence)
    }

    #[must_use]
    pub fn inspection(&self) -> AdaptiveEvidencePoolInspection {
        AdaptiveEvidencePoolInspection {
            window_epoch: self.window_epoch,
            health: if self.calibration_truncated
                || self.target_capacity_rejections != 0
                || self
                    .target_windows
                    .iter()
                    .any(|entry| entry.window.truncated)
            {
                AdaptiveEvidencePoolHealth::RotationRecommended
            } else {
                AdaptiveEvidencePoolHealth::Healthy
            },
            schema_generation: self.schema_generation,
            first_global_commit_seq: self.first_global_commit_seq,
            last_global_commit_seq: self.last_global_commit_seq,
            recorded_reports: self.recorded_reports,
            target_windows: self
                .target_windows
                .iter()
                .map(|entry| AdaptiveTargetWindowInspection {
                    lineage: entry.lineage,
                    target: entry.window.target,
                    first_global_commit_seq: entry.window.first_global_commit_seq,
                    last_global_commit_seq: entry.window.last_global_commit_seq,
                    sample_count: entry.window.total_samples,
                    distinct_visibility_points: entry.window.distinct_visibility_points,
                    overflowed: entry.window.overflowed,
                    incomplete: entry.window.incomplete,
                    truncated: entry.window.truncated,
                })
                .collect(),
            calibration_epochs: self
                .calibration_epochs
                .iter()
                .copied()
                .map(|epoch| AdaptiveCalibrationEpochInspection { epoch })
                .collect(),
            calibration_groups: self
                .calibration_report_counts
                .iter()
                .map(|count| {
                    let visibility = self.calibration_visibility.iter().find(|visibility| {
                        visibility.calibration_class == count.class
                            && visibility.calibration_epoch == count.epoch
                    });
                    AdaptiveCalibrationGroupInspection {
                        calibration_class: count.class,
                        epoch: count.epoch,
                        report_samples: count.reports,
                        distinct_visibility_points: visibility
                            .map_or(0, |visibility| visibility.distinct_visibility_points),
                        overflowed: visibility.is_some_and(|visibility| visibility.overflowed),
                    }
                })
                .collect(),
            overflowed: self.calibration_overflowed
                || self
                    .target_windows
                    .iter()
                    .any(|entry| entry.window.overflowed),
            incomplete: self.calibration_incomplete
                || self.target_capacity_rejections != 0
                || self
                    .target_windows
                    .iter()
                    .any(|entry| entry.window.incomplete),
            truncated: self.calibration_truncated
                || self.target_capacity_rejections != 0
                || self
                    .target_windows
                    .iter()
                    .any(|entry| entry.window.truncated),
            target_capacity_rejections: self.target_capacity_rejections,
        }
    }

    pub fn record_execution_feedback(
        &mut self,
        report: &ExecutionFeedbackReport,
    ) -> Result<AdaptiveEvidenceRecordReport, AdaptiveEvidenceRecordError> {
        let global_commit_seq = report
            .anchor
            .global_commit_seq
            .ok_or(AdaptiveEvidenceRecordError::GlobalVisibilityRequired)?;
        let schema_rotated = match self.schema_generation {
            Some(current) if report.anchor.schema_generation < current => {
                return Err(AdaptiveEvidenceRecordError::StaleSchemaEvidence {
                    current,
                    received: report.anchor.schema_generation,
                });
            }
            Some(current) if report.anchor.schema_generation > current => true,
            _ => false,
        };
        if !schema_rotated {
            if let Some(minimum) = self.calibration_epoch_floor {
                if report.calibration_epoch < minimum {
                    return Err(AdaptiveEvidenceRecordError::StaleCalibrationEpochEvidence {
                        minimum,
                        received: report.calibration_epoch,
                    });
                }
            }
        }
        if !schema_rotated {
            if let Some(previous) = self.ordering_high_water {
                if global_commit_seq < previous {
                    return Err(AdaptiveEvidenceRecordError::OutOfOrderVisibility {
                        previous,
                        received: global_commit_seq,
                    });
                }
            }
        }

        let targets = report_targets(report);
        if !schema_rotated {
            self.validate_target_forward_progress(&targets)?;
        }
        if schema_rotated {
            self.rotate_schema(report.anchor.schema_generation)?;
        } else if self.schema_generation.is_none() {
            self.schema_generation = Some(report.anchor.schema_generation);
        }

        if self.first_global_commit_seq.is_none() {
            self.first_global_commit_seq = Some(global_commit_seq);
        }
        self.last_global_commit_seq = Some(global_commit_seq);
        self.ordering_high_water = Some(global_commit_seq);
        self.calibration_epoch_high_water = Some(
            self.calibration_epoch_high_water
                .map_or(report.calibration_epoch, |current| {
                    current.max(report.calibration_epoch)
                }),
        );
        if self.calibration_epoch_floor.is_some() {
            self.calibration_epoch_floor = self.calibration_epoch_high_water;
        }
        if self.recorded_reports.checked_add(1).is_none() {
            self.calibration_overflowed = true;
            self.calibration_incomplete = true;
        } else {
            self.recorded_reports += 1;
        }

        self.retain_calibration_epoch(report.calibration_epoch);
        let calibration_recorded = if self.calibration_epochs.contains(&report.calibration_epoch) {
            let recorded = record_calibration_report(
                &mut self.calibration_query_shapes,
                &mut self.calibration_visibility,
                &mut self.calibration_truncated,
                AdaptiveWorkloadLimits::new(
                    self.limits.max_calibration_query_shapes,
                    self.limits.max_calibration_plan_variants_per_shape,
                ),
                report,
                global_commit_seq,
            );
            self.calibration_incomplete |= report.incomplete || report.overflowed || !recorded;
            self.calibration_overflowed |= report.overflowed;
            self.record_calibration_report_counts(report);
            u64::from(recorded)
        } else {
            self.calibration_incomplete = true;
            self.calibration_truncated = true;
            0
        };

        let mut recorded = 0_u64;
        let mut rotated = 0_u64;
        let mut capacity_rejected = false;
        for target in targets {
            let lineage = AdaptiveTargetLineage {
                table_id: target.table_id,
                projection_id: target.projection_id,
            };
            let existing = self
                .target_windows
                .iter()
                .position(|entry| entry.lineage == lineage);
            let index = match existing {
                Some(index) if self.target_windows[index].window.target == target => index,
                Some(index) => {
                    let old = self.target_windows[index].window.target;
                    self.remember_retired_target(old);
                    self.target_windows[index].window =
                        AdaptiveWorkloadWindow::new(target, self.limits.workload_limits);
                    rotated = rotated.saturating_add(1);
                    index
                }
                None => {
                    let known_lineage = self
                        .target_lineages
                        .iter()
                        .any(|state| state.lineage == lineage);
                    if !known_lineage
                        && u64::try_from(self.target_lineages.len())
                            .map_or(true, |count| count >= self.limits.max_target_windows)
                    {
                        if let Some(next) = self.target_capacity_rejections.checked_add(1) {
                            self.target_capacity_rejections = next;
                        } else {
                            self.calibration_overflowed = true;
                        }
                        self.calibration_incomplete = true;
                        self.calibration_truncated = true;
                        capacity_rejected = true;
                        continue;
                    }
                    self.target_windows.push(TargetWindowEntry {
                        lineage,
                        window: AdaptiveWorkloadWindow::new(target, self.limits.workload_limits),
                    });
                    self.target_windows.len() - 1
                }
            };
            self.update_target_lineage(target);
            match self.target_windows[index].window.record(report) {
                Ok(_) => recorded = recorded.saturating_add(1),
                Err(AdaptiveWorkloadRecordError::GlobalVisibilityRequired) => {
                    return Err(AdaptiveEvidenceRecordError::GlobalVisibilityRequired);
                }
                Err(AdaptiveWorkloadRecordError::SchemaChanged { .. }) => {
                    return Err(AdaptiveEvidenceRecordError::StaleSchemaEvidence {
                        current: report.anchor.schema_generation,
                        received: report.anchor.schema_generation,
                    });
                }
                Err(AdaptiveWorkloadRecordError::OutOfOrderVisibility { previous, received }) => {
                    return Err(AdaptiveEvidenceRecordError::OutOfOrderVisibility {
                        previous,
                        received,
                    });
                }
            }
        }
        let outcome = match (schema_rotated, capacity_rejected) {
            (false, false) => AdaptiveEvidenceRecordOutcome::Recorded,
            (true, false) => AdaptiveEvidenceRecordOutcome::SchemaRotated,
            (false, true) => AdaptiveEvidenceRecordOutcome::RecordedWithCapacityRejection,
            (true, true) => AdaptiveEvidenceRecordOutcome::SchemaRotatedWithCapacityRejection,
        };
        Ok(AdaptiveEvidenceRecordReport {
            outcome,
            target_windows_recorded: recorded,
            target_windows_rotated: rotated,
            calibration_reports_recorded: calibration_recorded,
        })
    }

    fn validate_target_forward_progress(
        &self,
        targets: &[AdaptiveWorkloadTarget],
    ) -> Result<(), AdaptiveEvidenceRecordError> {
        for target in targets {
            if self.retired_targets.contains(target) {
                return Err(AdaptiveEvidenceRecordError::RetiredTargetEvidence { target: *target });
            }
            let lineage = AdaptiveTargetLineage {
                table_id: target.table_id,
                projection_id: target.projection_id,
            };
            if let Some(current) = self
                .target_lineages
                .iter()
                .find(|state| state.lineage == lineage)
                .copied()
            {
                if target.generation < current.generation {
                    return Err(AdaptiveEvidenceRecordError::StaleTargetGenerationEvidence {
                        lineage,
                        current: current.generation,
                        received: target.generation,
                    });
                }
                if target.storage_id < current.storage_id {
                    return Err(AdaptiveEvidenceRecordError::StaleTargetIdentityEvidence {
                        lineage,
                        current_storage_id: current.storage_id,
                        received_storage_id: target.storage_id,
                    });
                }
            }
        }
        Ok(())
    }

    fn update_target_lineage(&mut self, target: AdaptiveWorkloadTarget) {
        let lineage = AdaptiveTargetLineage {
            table_id: target.table_id,
            projection_id: target.projection_id,
        };
        if let Some(current) = self
            .target_lineages
            .iter_mut()
            .find(|state| state.lineage == lineage)
        {
            current.storage_id = target.storage_id;
            current.generation = target.generation;
        } else {
            self.target_lineages.push(TargetLineageState {
                lineage,
                storage_id: target.storage_id,
                generation: target.generation,
            });
        }
    }

    fn rotation_report(
        &self,
        new_window_epoch: AdaptiveEvidenceWindowEpoch,
    ) -> AdaptiveEvidenceRotationReport {
        let target_query_shapes = self.target_windows.iter().fold(0_u64, |total, entry| {
            total.saturating_add(u64::try_from(entry.window.query_shapes.len()).unwrap_or(u64::MAX))
        });
        AdaptiveEvidenceRotationReport {
            previous_window_epoch: self.window_epoch,
            new_window_epoch,
            schema_generation: self.schema_generation,
            ordering_high_water: self.ordering_high_water,
            discarded_target_window_count: u64::try_from(self.target_windows.len())
                .unwrap_or(u64::MAX),
            discarded_calibration_epoch_count: u64::try_from(self.calibration_epochs.len())
                .unwrap_or(u64::MAX),
            discarded_query_shape_count: target_query_shapes.saturating_add(
                u64::try_from(self.calibration_query_shapes.len()).unwrap_or(u64::MAX),
            ),
            was_incomplete: self.inspection().incomplete,
            was_truncated: self.inspection().truncated,
        }
    }

    fn clear_aggregation_payload(&mut self) {
        self.first_global_commit_seq = None;
        self.last_global_commit_seq = None;
        self.recorded_reports = 0;
        self.target_windows.clear();
        self.calibration_epochs.clear();
        self.calibration_query_shapes.clear();
        self.calibration_visibility.clear();
        self.calibration_report_counts.clear();
        self.calibration_overflowed = false;
        self.calibration_incomplete = false;
        self.calibration_truncated = false;
        self.target_capacity_rejections = 0;
    }

    fn rotate_schema(
        &mut self,
        schema_generation: SchemaGeneration,
    ) -> Result<(), AdaptiveEvidenceRecordError> {
        let window_epoch = self
            .window_epoch
            .0
            .checked_add(1)
            .map(AdaptiveEvidenceWindowEpoch)
            .ok_or(AdaptiveEvidenceRecordError::EvidenceWindowEpochExhausted)?;
        let limits = self.limits;
        *self = Self::new(limits);
        self.window_epoch = window_epoch;
        self.schema_generation = Some(schema_generation);
        Ok(())
    }

    fn remember_retired_target(&mut self, target: AdaptiveWorkloadTarget) {
        if self.retired_targets.contains(&target) {
            return;
        }
        let maximum = usize::try_from(self.limits.max_target_windows).unwrap_or(usize::MAX);
        if maximum == 0 {
            return;
        }
        if self.retired_targets.len() == maximum {
            self.retired_targets.remove(0);
        }
        self.retired_targets.push(target);
    }

    fn retain_calibration_epoch(&mut self, epoch: PlannerCalibrationEpoch) {
        if self.calibration_epochs.contains(&epoch) {
            return;
        }
        let maximum = usize::try_from(self.limits.max_calibration_epochs).unwrap_or(usize::MAX);
        if maximum == 0 {
            return;
        }
        self.calibration_epochs.push(epoch);
        self.calibration_epochs.sort_unstable();
        if self.calibration_epochs.len() > maximum {
            let evicted = self.calibration_epochs.remove(0);
            for shape in &mut self.calibration_query_shapes {
                for variant in &mut shape.plan_variants {
                    variant
                        .calibration
                        .retain(|aggregate| aggregate.calibration_epoch != evicted);
                }
                shape.plan_variants.retain(|variant| {
                    !variant.calibration.is_empty() || variant.calibration_truncated
                });
            }
            self.calibration_query_shapes
                .retain(|shape| !shape.plan_variants.is_empty());
            self.calibration_visibility
                .retain(|visibility| visibility.calibration_epoch != evicted);
            self.calibration_report_counts
                .retain(|count| count.epoch != evicted);
        }
    }

    fn record_calibration_report_counts(&mut self, report: &ExecutionFeedbackReport) {
        let mut classes = Vec::new();
        for access in &report.accesses {
            let (Some(planner), Some(sample)) = (&access.planner, access.calibration) else {
                continue;
            };
            if sample.calibration_epoch != report.calibration_epoch {
                self.calibration_incomplete = true;
                continue;
            }
            let class = planner.kind.calibration_class();
            if !classes.contains(&class) {
                classes.push(class);
            }
        }
        for class in classes {
            if let Some(count) = self
                .calibration_report_counts
                .iter_mut()
                .find(|count| count.class == class && count.epoch == report.calibration_epoch)
            {
                if let Some(next) = count.reports.checked_add(1) {
                    count.reports = next;
                } else {
                    self.calibration_overflowed = true;
                    self.calibration_incomplete = true;
                }
            } else {
                self.calibration_report_counts.push(CalibrationReportCount {
                    class,
                    epoch: report.calibration_epoch,
                    reports: 1,
                });
            }
        }
        self.calibration_report_counts
            .sort_unstable_by_key(|count| (count.epoch, count.class));
    }
}

impl Default for AdaptiveEvidencePool {
    fn default() -> Self {
        Self::new(AdaptiveEvidencePoolLimits::default())
    }
}

fn report_targets(report: &ExecutionFeedbackReport) -> Vec<AdaptiveWorkloadTarget> {
    let mut targets = Vec::new();
    for access in &report.accesses {
        let Some(planner) = &access.planner else {
            continue;
        };
        let (Some(storage_id), Some(projection_id), Some(generation)) = (
            planner.storage_id,
            planner.projection_id,
            planner.projection_generation,
        ) else {
            continue;
        };
        let target = AdaptiveWorkloadTarget {
            table_id: planner.table_id,
            storage_id,
            projection_id,
            generation,
            schema_generation: report.anchor.schema_generation,
        };
        if !targets.contains(&target) {
            targets.push(target);
        }
    }
    targets.sort_unstable_by_key(|target| {
        (
            target.table_id.0,
            target.projection_id.0,
            target.generation.0,
            target.storage_id.0,
        )
    });
    targets
}
