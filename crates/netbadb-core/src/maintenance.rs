use std::cmp::Ordering;
use std::rc::Rc;

use netbadb_storage::{
    ChangeBatchMaintenanceInspection, ChangeStreamStatus, LsmMaintenanceCostInspection,
    LsmMaintenanceInspection, LsmMaintenanceSafetyBlocker, StorageKind,
};
use netbadb_types::{ColumnarProjectionId, StorageId, TableId};

use crate::{
    ChangeStreamGcReport, ColumnarAdvanceBudget, ColumnarAdvanceReport, ColumnarCompactionReport,
    ColumnarProjectionHealth, Database, DatabaseError, StorageRegistryError,
};

/// Structural resource limits for one caller-driven maintenance step.
///
/// These limits are deliberately not time deadlines. A step executes at most
/// one action even when `max_actions` is larger than one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaintenanceBudget {
    pub max_work_units: u64,
    pub max_read_bytes: u64,
    pub max_write_bytes: u64,
    pub max_actions: u32,
}

impl MaintenanceBudget {
    #[must_use]
    pub const fn new(
        max_work_units: u64,
        max_read_bytes: u64,
        max_write_bytes: u64,
        max_actions: u32,
    ) -> Self {
        Self {
            max_work_units,
            max_read_bytes,
            max_write_bytes,
            max_actions,
        }
    }

    /// Returns the component-wise minimum of two independent maintenance
    /// envelopes.
    #[must_use]
    pub const fn capped_by(self, other: Self) -> Self {
        Self {
            max_work_units: if self.max_work_units < other.max_work_units {
                self.max_work_units
            } else {
                other.max_work_units
            },
            max_read_bytes: if self.max_read_bytes < other.max_read_bytes {
                self.max_read_bytes
            } else {
                other.max_read_bytes
            },
            max_write_bytes: if self.max_write_bytes < other.max_write_bytes {
                self.max_write_bytes
            } else {
                other.max_write_bytes
            },
            max_actions: if self.max_actions < other.max_actions {
                self.max_actions
            } else {
                other.max_actions
            },
        }
    }

    pub(crate) const fn checked_remaining(self, consumed: MaintenanceConsumption) -> Option<Self> {
        if !self.contains(consumed) {
            return None;
        }
        Some(Self {
            max_work_units: self.max_work_units - consumed.work_units,
            max_read_bytes: self.max_read_bytes - consumed.read_bytes,
            max_write_bytes: self.max_write_bytes - consumed.write_bytes,
            max_actions: self.max_actions - consumed.actions,
        })
    }

    pub(crate) fn remaining(self, consumed: MaintenanceConsumption) -> Self {
        Self {
            max_work_units: self.max_work_units.saturating_sub(consumed.work_units),
            max_read_bytes: self.max_read_bytes.saturating_sub(consumed.read_bytes),
            max_write_bytes: self.max_write_bytes.saturating_sub(consumed.write_bytes),
            max_actions: self.max_actions.saturating_sub(consumed.actions),
        }
    }

    pub(crate) const fn admits(self, estimate: MaintenanceEstimate) -> bool {
        self.max_actions != 0
            && estimate.work_units <= self.max_work_units
            && estimate.read_bytes <= self.max_read_bytes
            && estimate.write_bytes <= self.max_write_bytes
    }

    pub(crate) const fn contains(self, consumed: MaintenanceConsumption) -> bool {
        consumed.actions <= self.max_actions
            && consumed.work_units <= self.max_work_units
            && consumed.read_bytes <= self.max_read_bytes
            && consumed.write_bytes <= self.max_write_bytes
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MaintenanceConsumption {
    pub work_units: u64,
    pub read_bytes: u64,
    pub write_bytes: u64,
    pub actions: u32,
}

impl MaintenanceConsumption {
    /// Adds independently measured maintenance consumption without hiding an
    /// overflow as a trustworthy total.
    #[must_use]
    pub const fn checked_add(self, other: Self) -> Option<Self> {
        let Some(work_units) = self.work_units.checked_add(other.work_units) else {
            return None;
        };
        let Some(read_bytes) = self.read_bytes.checked_add(other.read_bytes) else {
            return None;
        };
        let Some(write_bytes) = self.write_bytes.checked_add(other.write_bytes) else {
            return None;
        };
        let Some(actions) = self.actions.checked_add(other.actions) else {
            return None;
        };
        Some(Self {
            work_units,
            read_bytes,
            write_bytes,
            actions,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaintenanceEstimate {
    pub work_units: u64,
    pub read_bytes: u64,
    pub write_bytes: u64,
}

impl MaintenanceEstimate {
    const fn from_lsm(cost: LsmMaintenanceCostInspection) -> Self {
        Self {
            work_units: cost.work_units,
            read_bytes: cost.read_bytes,
            write_bytes: cost.write_bytes,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaintenanceBound {
    /// Batch count and encoded NBCL input bytes are hard bounded. Output bytes
    /// are an admission estimate because the existing NBCD writer is atomic.
    HardBoundedInput,
    /// The existing operation is atomic and starts only when its complete
    /// structural estimate fits the budget.
    EstimateGatedAtomic,
    /// The complete structural input and exact production rewrite output are
    /// known and admitted before an irreversible mutation begins.
    HardBoundedRewrite,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaintenanceReason {
    LsmMemtableFlush,
    ProjectionLag,
    ChangeHistoryReclaim,
    LsmCompactionPressure,
    ColumnarDeltaCost,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaintenanceAction {
    AdvanceColumnar {
        projection_id: ColumnarProjectionId,
        max_batches: u64,
        max_change_bytes: u64,
    },
    CompactColumnar {
        projection_id: ColumnarProjectionId,
    },
    GcChangeStream {
        table_id: TableId,
        storage_id: StorageId,
    },
    FlushLsm {
        table_id: TableId,
        storage_id: StorageId,
    },
    CompactLsm {
        table_id: TableId,
        storage_id: StorageId,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaintenanceBlocker {
    ActionBudgetExhausted,
    WorkBudgetExceeded,
    ReadBudgetExceeded,
    WriteBudgetExceeded,
    Busy,
    SnapshotProjection,
    ProjectionFresh,
    ProjectionLagging,
    RebuildRequired,
    Unavailable,
    NoDelta,
    NoRetentionConsumer,
    NoReclaimableHistory,
    RetentionUnsafe,
    HistoryUnavailable,
    RecoveryRequired,
    MemtableNotEmpty,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceCandidate {
    pub action: MaintenanceAction,
    pub reason: MaintenanceReason,
    pub bound: MaintenanceBound,
    pub estimate: MaintenanceEstimate,
    pub eligible: bool,
    pub blocker: Option<MaintenanceBlocker>,
}

impl MaintenanceCandidate {
    fn pending_work(&self) -> bool {
        matches!(
            self.blocker,
            None | Some(MaintenanceBlocker::ActionBudgetExhausted)
                | Some(MaintenanceBlocker::WorkBudgetExceeded)
                | Some(MaintenanceBlocker::ReadBudgetExceeded)
                | Some(MaintenanceBlocker::WriteBudgetExceeded)
                | Some(MaintenanceBlocker::Busy)
                | Some(MaintenanceBlocker::ProjectionLagging)
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaintenanceDecision {
    pub action: MaintenanceAction,
    pub reason: MaintenanceReason,
    pub bound: MaintenanceBound,
    pub estimated: MaintenanceEstimate,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceInspection {
    pub decision: Option<MaintenanceDecision>,
    pub candidates: Vec<MaintenanceCandidate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LsmMaintenanceReport {
    pub table_id: TableId,
    pub storage_id: StorageId,
    pub work_units: u64,
    pub read_bytes: u64,
    pub write_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MaintenanceActionReport {
    AdvanceColumnar(ColumnarAdvanceReport),
    CompactColumnar(ColumnarCompactionReport),
    GcChangeStream(ChangeStreamGcReport),
    FlushLsm(LsmMaintenanceReport),
    CompactLsm(LsmMaintenanceReport),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MaintenanceOutcome {
    NoWork,
    Completed(MaintenanceActionReport),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceStepReport {
    pub decision: Option<MaintenanceDecision>,
    pub outcome: MaintenanceOutcome,
    pub budget_before: MaintenanceBudget,
    pub consumed: MaintenanceConsumption,
    pub budget_remaining: MaintenanceBudget,
    pub more_work_remaining: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MaintenanceCursor {
    class: u8,
    target: u64,
}

#[derive(Debug)]
struct MaintenanceState {
    candidates: Vec<MaintenanceCandidate>,
}

impl Database {
    /// Inspects and ranks maintenance without mutating any storage subsystem.
    pub fn inspect_maintenance(
        &self,
        budget: MaintenanceBudget,
    ) -> Result<MaintenanceInspection, DatabaseError> {
        if self.inspect_group_commit().is_some() {
            return Err(crate::CoordinatorError::GroupCommitActive.into());
        }
        let state = self.maintenance_state(budget)?;
        Ok(plan_maintenance(state, self.maintenance_cursor))
    }

    /// Returns the production compaction candidate for every projection on one
    /// table without consulting or changing the manual-maintenance cursor.
    pub(crate) fn inspect_columnar_compaction_candidates(
        &self,
        table_id: TableId,
        budget: MaintenanceBudget,
    ) -> Vec<MaintenanceCandidate> {
        let busy = Rc::strong_count(&self.transaction_owner) != 1
            || self.inspect_group_commit().is_some()
            || self.schema_writer.get().is_some();
        self.inspect_columnar_projections()
            .into_iter()
            .filter(|projection| projection.table_id == table_id)
            .filter_map(|projection| {
                projection.projection_id.map(|projection_id| {
                    columnar_compaction_candidate(&projection, projection_id, busy, budget)
                })
            })
            .collect()
    }

    /// Executes at most one caller-driven synchronous maintenance action.
    pub fn maintenance_step(
        &mut self,
        budget: MaintenanceBudget,
    ) -> Result<MaintenanceStepReport, DatabaseError> {
        let inspection = self.inspect_maintenance(budget)?;
        let Some(decision) = inspection.decision else {
            return Ok(MaintenanceStepReport {
                decision: None,
                outcome: MaintenanceOutcome::NoWork,
                budget_before: budget,
                consumed: MaintenanceConsumption::default(),
                budget_remaining: budget,
                more_work_remaining: inspection
                    .candidates
                    .iter()
                    .any(MaintenanceCandidate::pending_work),
            });
        };

        let (report, consumed) = self.execute_maintenance_action(decision)?;
        self.maintenance_cursor = Some(action_cursor(decision.action));
        let follow_up = self.inspect_maintenance(budget)?;
        let more_work_remaining = follow_up.decision.is_some()
            || follow_up
                .candidates
                .iter()
                .any(MaintenanceCandidate::pending_work);
        Ok(MaintenanceStepReport {
            decision: Some(decision),
            outcome: MaintenanceOutcome::Completed(report),
            budget_before: budget,
            consumed,
            budget_remaining: budget.remaining(consumed),
            more_work_remaining,
        })
    }

    fn maintenance_state(
        &self,
        budget: MaintenanceBudget,
    ) -> Result<MaintenanceState, DatabaseError> {
        let busy = Rc::strong_count(&self.transaction_owner) != 1
            || self.inspect_group_commit().is_some()
            || self.schema_writer.get().is_some();
        let projection_inspections = self.inspect_columnar_projections();
        let stream_inspections = self
            .registry
            .iter()
            .map(|entry| {
                (
                    entry.id,
                    entry.storage.table().id,
                    entry.storage.inspect_change_stream_maintenance(),
                )
            })
            .collect::<Vec<_>>();
        let mut candidates = Vec::new();

        for projection in &projection_inspections {
            let Some(projection_id) = projection.projection_id else {
                continue;
            };
            let base_action = MaintenanceAction::AdvanceColumnar {
                projection_id,
                max_batches: 0,
                max_change_bytes: 0,
            };
            let (advance_action, advance_estimate, advance_blocker) = if projection.health
                == ColumnarProjectionHealth::Unavailable
            {
                (
                    base_action,
                    zero_estimate(),
                    Some(MaintenanceBlocker::Unavailable),
                )
            } else if projection.health == ColumnarProjectionHealth::RebuildRequired {
                (
                    base_action,
                    zero_estimate(),
                    Some(MaintenanceBlocker::RebuildRequired),
                )
            } else if projection.mode != Some("incremental") {
                (
                    base_action,
                    zero_estimate(),
                    Some(MaintenanceBlocker::SnapshotProjection),
                )
            } else {
                match projection.health {
                    ColumnarProjectionHealth::Fresh => (
                        base_action,
                        zero_estimate(),
                        Some(MaintenanceBlocker::ProjectionFresh),
                    ),
                    ColumnarProjectionHealth::RebuildRequired => (
                        base_action,
                        zero_estimate(),
                        Some(MaintenanceBlocker::RebuildRequired),
                    ),
                    ColumnarProjectionHealth::Unavailable => (
                        base_action,
                        zero_estimate(),
                        Some(MaintenanceBlocker::Unavailable),
                    ),
                    ColumnarProjectionHealth::Stale => (
                        base_action,
                        zero_estimate(),
                        Some(MaintenanceBlocker::SnapshotProjection),
                    ),
                    ColumnarProjectionHealth::Lagging => {
                        let batches = projection
                            .source_storage_id
                            .and_then(|storage_id| {
                                stream_inspections
                                    .iter()
                                    .find(|(id, _, _)| *id == storage_id)
                            })
                            .map(|(_, _, stream)| stream.batches.as_slice())
                            .unwrap_or(&[]);
                        bounded_advance(projection_id, projection.applied_frontier, batches, budget)
                    }
                }
            };
            candidates.push(candidate(
                advance_action,
                MaintenanceReason::ProjectionLag,
                MaintenanceBound::HardBoundedInput,
                advance_estimate,
                advance_blocker,
                busy,
                budget,
            ));

            candidates.push(columnar_compaction_candidate(
                projection,
                projection_id,
                busy,
                budget,
            ));
        }

        for (storage_id, table_id, maintenance) in &stream_inspections {
            if maintenance.stream.status == ChangeStreamStatus::Enabled {
                let assessment = self.assess_change_stream_retention(*table_id)?;
                candidates.push(change_stream_gc_candidate(
                    *table_id,
                    *storage_id,
                    &assessment,
                    busy,
                    budget,
                ));
            }
            let storage =
                self.registry
                    .get(*storage_id)
                    .ok_or(StorageRegistryError::UnknownStorageId {
                        storage_id: *storage_id,
                    })?;
            if storage.kind() != StorageKind::Lsm {
                continue;
            }
            let lsm = storage.lsm_maintenance_inspection()?.ok_or(
                StorageRegistryError::UnknownStorageId {
                    storage_id: *storage_id,
                },
            )?;
            candidates.extend(lsm_maintenance_candidates(
                *table_id,
                *storage_id,
                &lsm,
                busy,
                budget,
            ));
        }
        Ok(MaintenanceState { candidates })
    }

    fn execute_maintenance_action(
        &mut self,
        decision: MaintenanceDecision,
    ) -> Result<(MaintenanceActionReport, MaintenanceConsumption), DatabaseError> {
        match decision.action {
            MaintenanceAction::AdvanceColumnar {
                projection_id,
                max_batches,
                max_change_bytes,
            } => {
                let max_batches = usize::try_from(max_batches).unwrap_or(usize::MAX);
                let report = self.advance_columnar_projection(
                    projection_id,
                    ColumnarAdvanceBudget::new(max_batches, max_change_bytes),
                )?;
                let consumed = MaintenanceConsumption {
                    work_units: report.batches_applied,
                    read_bytes: decision.estimated.read_bytes,
                    write_bytes: report.bytes_written,
                    actions: 1,
                };
                Ok((MaintenanceActionReport::AdvanceColumnar(report), consumed))
            }
            MaintenanceAction::CompactColumnar { projection_id } => {
                let (report, consumed) =
                    self.execute_columnar_compaction(projection_id, decision.estimated)?;
                Ok((MaintenanceActionReport::CompactColumnar(report), consumed))
            }
            MaintenanceAction::GcChangeStream {
                table_id,
                storage_id: _,
            } => {
                let report = self.gc_change_stream(table_id)?;
                let consumed = MaintenanceConsumption {
                    work_units: decision.estimated.work_units,
                    read_bytes: report.bytes_before,
                    write_bytes: report.bytes_after,
                    actions: 1,
                };
                Ok((MaintenanceActionReport::GcChangeStream(report), consumed))
            }
            MaintenanceAction::FlushLsm {
                table_id,
                storage_id,
            } => {
                let before = self
                    .registry
                    .get(storage_id)
                    .and_then(|storage| storage.lsm_inspection())
                    .ok_or(StorageRegistryError::UnknownStorageId { storage_id })?;
                self.registry
                    .get_mut(storage_id)
                    .ok_or(StorageRegistryError::UnknownStorageId { storage_id })?
                    .flush()?;
                let after = self
                    .registry
                    .get(storage_id)
                    .and_then(|storage| storage.lsm_inspection())
                    .ok_or(StorageRegistryError::UnknownStorageId { storage_id })?;
                let report = LsmMaintenanceReport {
                    table_id,
                    storage_id,
                    work_units: decision.estimated.work_units,
                    read_bytes: after
                        .write_amplification
                        .flush_input_bytes
                        .saturating_sub(before.write_amplification.flush_input_bytes),
                    write_bytes: after
                        .write_amplification
                        .flush_output_bytes
                        .saturating_sub(before.write_amplification.flush_output_bytes),
                };
                let consumed = lsm_consumption(&report);
                Ok((MaintenanceActionReport::FlushLsm(report), consumed))
            }
            MaintenanceAction::CompactLsm {
                table_id,
                storage_id,
            } => {
                let before = self
                    .registry
                    .get(storage_id)
                    .and_then(|storage| storage.lsm_inspection())
                    .ok_or(StorageRegistryError::UnknownStorageId { storage_id })?;
                let compacted = self
                    .registry
                    .get_mut(storage_id)
                    .ok_or(StorageRegistryError::UnknownStorageId { storage_id })?
                    .compact_lsm_one()?;
                if !compacted {
                    return Ok((
                        MaintenanceActionReport::CompactLsm(LsmMaintenanceReport {
                            table_id,
                            storage_id,
                            work_units: 0,
                            read_bytes: 0,
                            write_bytes: 0,
                        }),
                        MaintenanceConsumption {
                            actions: 1,
                            ..MaintenanceConsumption::default()
                        },
                    ));
                }
                let after = self
                    .registry
                    .get(storage_id)
                    .and_then(|storage| storage.lsm_inspection())
                    .ok_or(StorageRegistryError::UnknownStorageId { storage_id })?;
                let report = LsmMaintenanceReport {
                    table_id,
                    storage_id,
                    work_units: decision.estimated.work_units,
                    read_bytes: after
                        .write_amplification
                        .compaction_input_bytes
                        .saturating_sub(before.write_amplification.compaction_input_bytes),
                    write_bytes: after
                        .write_amplification
                        .compaction_output_bytes
                        .saturating_sub(before.write_amplification.compaction_output_bytes),
                };
                let consumed = lsm_consumption(&report);
                Ok((MaintenanceActionReport::CompactLsm(report), consumed))
            }
        }
    }

    /// Executes one already-admitted exact Columnar compaction through the
    /// production writer. It intentionally does not update the manual cursor.
    pub(crate) fn execute_columnar_compaction(
        &mut self,
        projection_id: ColumnarProjectionId,
        estimated: MaintenanceEstimate,
    ) -> Result<(ColumnarCompactionReport, MaintenanceConsumption), DatabaseError> {
        let report = self.compact_columnar_projection(projection_id)?;
        let consumed = MaintenanceConsumption {
            work_units: estimated.work_units,
            read_bytes: report.bytes_before,
            write_bytes: report.bytes_after,
            actions: 1,
        };
        Ok((report, consumed))
    }
}

pub(crate) fn lsm_maintenance_candidates(
    table_id: TableId,
    storage_id: StorageId,
    inspection: &LsmMaintenanceInspection,
    core_busy: bool,
    budget: MaintenanceBudget,
) -> Vec<MaintenanceCandidate> {
    let safety_blocker = inspection.safety_blocker.map(|blocker| match blocker {
        LsmMaintenanceSafetyBlocker::RecoveryRequired => MaintenanceBlocker::RecoveryRequired,
        LsmMaintenanceSafetyBlocker::WriterActive { .. }
        | LsmMaintenanceSafetyBlocker::OutstandingTransactions { .. }
        | LsmMaintenanceSafetyBlocker::OutstandingReadViews { .. } => MaintenanceBlocker::Busy,
    });
    let mut candidates = Vec::new();
    if let Some(cost) = inspection.flush_cost {
        candidates.push(candidate(
            MaintenanceAction::FlushLsm {
                table_id,
                storage_id,
            },
            MaintenanceReason::LsmMemtableFlush,
            MaintenanceBound::EstimateGatedAtomic,
            MaintenanceEstimate::from_lsm(cost),
            safety_blocker,
            core_busy,
            budget,
        ));
    }
    if let Some(cost) = inspection.next_compaction_cost {
        let blocker = safety_blocker.or_else(|| {
            (inspection.memtable_entry_count != 0).then_some(MaintenanceBlocker::MemtableNotEmpty)
        });
        candidates.push(candidate(
            MaintenanceAction::CompactLsm {
                table_id,
                storage_id,
            },
            MaintenanceReason::LsmCompactionPressure,
            MaintenanceBound::EstimateGatedAtomic,
            MaintenanceEstimate::from_lsm(cost),
            blocker,
            core_busy,
            budget,
        ));
    }
    candidates
}

fn columnar_compaction_candidate(
    projection: &crate::ColumnarProjectionInspection,
    projection_id: ColumnarProjectionId,
    busy: bool,
    budget: MaintenanceBudget,
) -> MaintenanceCandidate {
    let action = MaintenanceAction::CompactColumnar { projection_id };
    let estimate = MaintenanceEstimate {
        work_units: projection
            .row_count
            .unwrap_or(0)
            .saturating_add(projection.delta_mutations.unwrap_or(0))
            .max(1),
        read_bytes: projection
            .segment_bytes
            .unwrap_or(0)
            .saturating_add(projection.delta_bytes.unwrap_or(0)),
        write_bytes: projection
            .segment_bytes
            .unwrap_or(0)
            .saturating_add(projection.delta_bytes.unwrap_or(0)),
    };
    let blocker = if projection.health == ColumnarProjectionHealth::Unavailable {
        Some(MaintenanceBlocker::Unavailable)
    } else if projection.health == ColumnarProjectionHealth::RebuildRequired {
        Some(MaintenanceBlocker::RebuildRequired)
    } else if projection.mode != Some("incremental") {
        Some(MaintenanceBlocker::SnapshotProjection)
    } else {
        match projection.health {
            ColumnarProjectionHealth::Fresh => (projection.delta_segment_count.unwrap_or(0) == 0)
                .then_some(MaintenanceBlocker::NoDelta),
            ColumnarProjectionHealth::Lagging => Some(MaintenanceBlocker::ProjectionLagging),
            ColumnarProjectionHealth::RebuildRequired => Some(MaintenanceBlocker::RebuildRequired),
            ColumnarProjectionHealth::Unavailable => Some(MaintenanceBlocker::Unavailable),
            ColumnarProjectionHealth::Stale => Some(MaintenanceBlocker::SnapshotProjection),
        }
    };
    candidate(
        action,
        MaintenanceReason::ColumnarDeltaCost,
        MaintenanceBound::EstimateGatedAtomic,
        estimate,
        blocker,
        busy,
        budget,
    )
}

fn bounded_advance(
    projection_id: ColumnarProjectionId,
    applied: Option<netbadb_types::StorageDataVersion>,
    batches: &[ChangeBatchMaintenanceInspection],
    budget: MaintenanceBudget,
) -> (
    MaintenanceAction,
    MaintenanceEstimate,
    Option<MaintenanceBlocker>,
) {
    let base = MaintenanceAction::AdvanceColumnar {
        projection_id,
        max_batches: 0,
        max_change_bytes: 0,
    };
    let Some(applied) = applied else {
        return (
            base,
            zero_estimate(),
            Some(MaintenanceBlocker::HistoryUnavailable),
        );
    };
    let Some(start) = batches.iter().position(|batch| batch.before == applied) else {
        return (
            base,
            zero_estimate(),
            Some(MaintenanceBlocker::HistoryUnavailable),
        );
    };
    let mut selected = 0_u64;
    let mut mutations = 0_u64;
    let mut bytes = 0_u64;
    for batch in &batches[start..] {
        let following_batches = selected.saturating_add(1);
        let following_bytes = bytes.saturating_add(batch.change_bytes);
        if following_batches > budget.max_work_units
            || following_bytes > budget.max_read_bytes
            || following_bytes > budget.max_write_bytes
        {
            break;
        }
        selected = following_batches;
        bytes = following_bytes;
        mutations = mutations.saturating_add(batch.mutation_count);
    }
    if selected == 0 {
        let first = batches[start];
        return (
            base,
            MaintenanceEstimate {
                work_units: 1,
                read_bytes: first.change_bytes,
                write_bytes: first.change_bytes,
            },
            None,
        );
    }
    let _ = mutations;
    (
        MaintenanceAction::AdvanceColumnar {
            projection_id,
            max_batches: selected,
            max_change_bytes: bytes,
        },
        MaintenanceEstimate {
            work_units: selected,
            read_bytes: bytes,
            write_bytes: bytes,
        },
        None,
    )
}

fn change_stream_gc_candidate(
    table_id: TableId,
    storage_id: StorageId,
    assessment: &crate::adaptive_change_stream_gc::ChangeStreamRetentionAssessment,
    busy: bool,
    budget: MaintenanceBudget,
) -> MaintenanceCandidate {
    let action = MaintenanceAction::GcChangeStream {
        table_id,
        storage_id,
    };
    let blocker = assessment.observation.blocker.map(|blocker| match blocker {
        crate::AdaptiveChangeStreamGcSafetyBlocker::PreparedChangesUnresolved => {
            MaintenanceBlocker::Busy
        }
        crate::AdaptiveChangeStreamGcSafetyBlocker::FinalizeCheckpointPending => {
            MaintenanceBlocker::Busy
        }
        crate::AdaptiveChangeStreamGcSafetyBlocker::NoRetentionConsumer => {
            MaintenanceBlocker::NoRetentionConsumer
        }
        crate::AdaptiveChangeStreamGcSafetyBlocker::NoReclaimableHistory => {
            MaintenanceBlocker::NoReclaimableHistory
        }
        crate::AdaptiveChangeStreamGcSafetyBlocker::HistoryUnavailable => {
            MaintenanceBlocker::HistoryUnavailable
        }
        crate::AdaptiveChangeStreamGcSafetyBlocker::StreamUnavailable
        | crate::AdaptiveChangeStreamGcSafetyBlocker::ProjectionCatalogUnavailable
        | crate::AdaptiveChangeStreamGcSafetyBlocker::ManagedProjectionUnavailable
        | crate::AdaptiveChangeStreamGcSafetyBlocker::UnmanagedIncrementalProjection
        | crate::AdaptiveChangeStreamGcSafetyBlocker::RetentionFrontierInvalid
        | crate::AdaptiveChangeStreamGcSafetyBlocker::ArithmeticOverflow => {
            MaintenanceBlocker::RetentionUnsafe
        }
    });
    let estimate = assessment
        .observation
        .reclaimable_prefix
        .map(|prefix| {
            crate::adaptive_change_stream_gc::exact_rewrite_estimate(
                &assessment.observation,
                prefix,
            )
        })
        .unwrap_or_else(zero_estimate);
    candidate(
        action,
        MaintenanceReason::ChangeHistoryReclaim,
        MaintenanceBound::HardBoundedRewrite,
        estimate,
        blocker,
        busy,
        budget,
    )
}

fn candidate(
    action: MaintenanceAction,
    reason: MaintenanceReason,
    bound: MaintenanceBound,
    estimate: MaintenanceEstimate,
    structural_blocker: Option<MaintenanceBlocker>,
    busy: bool,
    budget: MaintenanceBudget,
) -> MaintenanceCandidate {
    let blocker = structural_blocker.or_else(|| {
        if busy {
            Some(MaintenanceBlocker::Busy)
        } else {
            budget_blocker(estimate, budget)
        }
    });
    MaintenanceCandidate {
        action,
        reason,
        bound,
        estimate,
        eligible: blocker.is_none(),
        blocker,
    }
}

fn budget_blocker(
    estimate: MaintenanceEstimate,
    budget: MaintenanceBudget,
) -> Option<MaintenanceBlocker> {
    if budget.max_actions == 0 {
        Some(MaintenanceBlocker::ActionBudgetExhausted)
    } else if estimate.work_units > budget.max_work_units {
        Some(MaintenanceBlocker::WorkBudgetExceeded)
    } else if estimate.read_bytes > budget.max_read_bytes {
        Some(MaintenanceBlocker::ReadBudgetExceeded)
    } else if estimate.write_bytes > budget.max_write_bytes {
        Some(MaintenanceBlocker::WriteBudgetExceeded)
    } else {
        None
    }
}

fn plan_maintenance(
    mut state: MaintenanceState,
    cursor: Option<MaintenanceCursor>,
) -> MaintenanceInspection {
    state.candidates.sort_by(compare_candidates);
    let decision = state
        .candidates
        .iter()
        .filter(|candidate| candidate.eligible)
        .min_by(|left, right| compare_with_cursor(left, right, cursor))
        .map(|candidate| MaintenanceDecision {
            action: candidate.action,
            reason: candidate.reason,
            bound: candidate.bound,
            estimated: candidate.estimate,
        });
    MaintenanceInspection {
        decision,
        candidates: state.candidates,
    }
}

fn compare_candidates(left: &MaintenanceCandidate, right: &MaintenanceCandidate) -> Ordering {
    action_priority(left.action)
        .cmp(&action_priority(right.action))
        .then_with(|| action_sort_key(left.action).cmp(&action_sort_key(right.action)))
}

fn compare_with_cursor(
    left: &MaintenanceCandidate,
    right: &MaintenanceCandidate,
    cursor: Option<MaintenanceCursor>,
) -> Ordering {
    let priority = action_priority(left.action).cmp(&action_priority(right.action));
    if priority != Ordering::Equal {
        return priority;
    }
    let left_cursor = action_cursor(left.action);
    let right_cursor = action_cursor(right.action);
    if left_cursor.class != right_cursor.class {
        return left_cursor.class.cmp(&right_cursor.class);
    }
    let Some(cursor) = cursor.filter(|cursor| cursor.class == left_cursor.class) else {
        return left_cursor.target.cmp(&right_cursor.target);
    };
    let left_after = left_cursor.target > cursor.target;
    let right_after = right_cursor.target > cursor.target;
    right_after
        .cmp(&left_after)
        .then_with(|| left_cursor.target.cmp(&right_cursor.target))
}

const fn action_priority(action: MaintenanceAction) -> u8 {
    match action {
        MaintenanceAction::FlushLsm { .. } => 0,
        MaintenanceAction::AdvanceColumnar { .. } => 1,
        MaintenanceAction::GcChangeStream { .. } => 2,
        MaintenanceAction::CompactLsm { .. } => 3,
        MaintenanceAction::CompactColumnar { .. } => 4,
    }
}

const fn action_cursor(action: MaintenanceAction) -> MaintenanceCursor {
    match action {
        MaintenanceAction::FlushLsm { storage_id, .. } => MaintenanceCursor {
            class: 0,
            target: storage_id.0,
        },
        MaintenanceAction::AdvanceColumnar { projection_id, .. } => MaintenanceCursor {
            class: 1,
            target: projection_id.0,
        },
        MaintenanceAction::GcChangeStream { storage_id, .. } => MaintenanceCursor {
            class: 2,
            target: storage_id.0,
        },
        MaintenanceAction::CompactLsm { storage_id, .. } => MaintenanceCursor {
            class: 3,
            target: storage_id.0,
        },
        MaintenanceAction::CompactColumnar { projection_id } => MaintenanceCursor {
            class: 4,
            target: projection_id.0,
        },
    }
}

const fn action_sort_key(action: MaintenanceAction) -> (u8, u64) {
    let cursor = action_cursor(action);
    (cursor.class, cursor.target)
}

const fn zero_estimate() -> MaintenanceEstimate {
    MaintenanceEstimate {
        work_units: 0,
        read_bytes: 0,
        write_bytes: 0,
    }
}

fn lsm_consumption(report: &LsmMaintenanceReport) -> MaintenanceConsumption {
    MaintenanceConsumption {
        work_units: report.work_units,
        read_bytes: report.read_bytes,
        write_bytes: report.write_bytes,
        actions: 1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BUDGET: MaintenanceBudget = MaintenanceBudget::new(10, 1_000, 1_000, 1);

    fn eligible(action: MaintenanceAction, reason: MaintenanceReason) -> MaintenanceCandidate {
        candidate(
            action,
            reason,
            MaintenanceBound::EstimateGatedAtomic,
            MaintenanceEstimate {
                work_units: 1,
                read_bytes: 1,
                write_bytes: 1,
            },
            None,
            false,
            BUDGET,
        )
    }

    #[test]
    fn policy_is_lexicographic_and_stably_tied() {
        let state = MaintenanceState {
            candidates: vec![
                eligible(
                    MaintenanceAction::AdvanceColumnar {
                        projection_id: ColumnarProjectionId(9),
                        max_batches: 1,
                        max_change_bytes: 1,
                    },
                    MaintenanceReason::ProjectionLag,
                ),
                eligible(
                    MaintenanceAction::AdvanceColumnar {
                        projection_id: ColumnarProjectionId(4),
                        max_batches: 1,
                        max_change_bytes: 1,
                    },
                    MaintenanceReason::ProjectionLag,
                ),
                eligible(
                    MaintenanceAction::CompactColumnar {
                        projection_id: ColumnarProjectionId(1),
                    },
                    MaintenanceReason::ColumnarDeltaCost,
                ),
            ],
        };
        assert!(matches!(
            plan_maintenance(state, None).decision.unwrap().action,
            MaintenanceAction::AdvanceColumnar {
                projection_id: ColumnarProjectionId(4),
                ..
            }
        ));
    }

    #[test]
    fn fairness_cursor_rotates_equal_priority_targets() {
        let state = MaintenanceState {
            candidates: vec![
                eligible(
                    MaintenanceAction::AdvanceColumnar {
                        projection_id: ColumnarProjectionId(1),
                        max_batches: 1,
                        max_change_bytes: 1,
                    },
                    MaintenanceReason::ProjectionLag,
                ),
                eligible(
                    MaintenanceAction::AdvanceColumnar {
                        projection_id: ColumnarProjectionId(2),
                        max_batches: 1,
                        max_change_bytes: 1,
                    },
                    MaintenanceReason::ProjectionLag,
                ),
            ],
        };
        assert!(matches!(
            plan_maintenance(
                state,
                Some(MaintenanceCursor {
                    class: 1,
                    target: 1,
                }),
            )
            .decision
            .unwrap()
            .action,
            MaintenanceAction::AdvanceColumnar {
                projection_id: ColumnarProjectionId(2),
                ..
            }
        ));
    }

    #[test]
    fn atomic_action_that_exceeds_budget_is_not_selected() {
        let blocked = candidate(
            MaintenanceAction::CompactColumnar {
                projection_id: ColumnarProjectionId(1),
            },
            MaintenanceReason::ColumnarDeltaCost,
            MaintenanceBound::EstimateGatedAtomic,
            MaintenanceEstimate {
                work_units: 11,
                read_bytes: 1,
                write_bytes: 1,
            },
            None,
            false,
            BUDGET,
        );
        assert_eq!(
            blocked.blocker,
            Some(MaintenanceBlocker::WorkBudgetExceeded)
        );
        assert!(
            plan_maintenance(
                MaintenanceState {
                    candidates: vec![blocked]
                },
                None
            )
            .decision
            .is_none()
        );
    }
}
