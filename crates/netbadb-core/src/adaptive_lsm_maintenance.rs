use std::error::Error;
use std::fmt;
use std::rc::Rc;

use netbadb_storage::{LsmInspection, StorageKind, StorageVersionKey};
use netbadb_types::{StorageId, TableId};

use crate::maintenance::lsm_maintenance_candidates;
use crate::{
    Database, DatabaseError, MaintenanceAction, MaintenanceBudget, MaintenanceConsumption,
    MaintenanceEstimate, StorageRegistryError,
};

use netbadb_advisor::revalidate_lsm_proposal;
pub use netbadb_advisor::{
    AdaptiveLsmCompactionPolicy, AdaptiveLsmCompactionProposal, AdaptiveLsmFlushPolicy,
    AdaptiveLsmFlushProposal, AdaptiveLsmMaintenanceAbortReason, AdaptiveLsmMaintenanceAction,
    AdaptiveLsmMaintenanceDecision, AdaptiveLsmMaintenanceExecutionReport,
    AdaptiveLsmMaintenanceMeasurement, AdaptiveLsmMaintenanceNoActionReason,
    AdaptiveLsmMaintenanceObservation, AdaptiveLsmMaintenanceOutcome,
    AdaptiveLsmMaintenanceProposal,
};

#[derive(Debug)]
pub enum AdaptiveLsmMaintenanceError {
    Database(DatabaseError),
    AmplificationCounterUnavailable,
    ConservativeBoundViolated {
        bound: MaintenanceEstimate,
        actual: MaintenanceConsumption,
    },
    PostconditionFailed(&'static str),
}

impl fmt::Display for AdaptiveLsmMaintenanceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Database(error) => error.fmt(formatter),
            Self::AmplificationCounterUnavailable => formatter.write_str(
                "LSM write-amplification counters saturated during authoritative maintenance",
            ),
            Self::ConservativeBoundViolated { .. } => formatter
                .write_str("production LSM maintenance exceeded its conservative structural bound"),
            Self::PostconditionFailed(reason) => {
                write!(
                    formatter,
                    "authoritative LSM maintenance postcondition failed: {reason}"
                )
            }
        }
    }
}

impl Error for AdaptiveLsmMaintenanceError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Database(error) => Some(error),
            Self::AmplificationCounterUnavailable
            | Self::ConservativeBoundViolated { .. }
            | Self::PostconditionFailed(_) => None,
        }
    }
}

impl From<DatabaseError> for AdaptiveLsmMaintenanceError {
    fn from(error: DatabaseError) -> Self {
        Self::Database(error)
    }
}

impl From<StorageRegistryError> for AdaptiveLsmMaintenanceError {
    fn from(error: StorageRegistryError) -> Self {
        Self::Database(error.into())
    }
}

impl From<netbadb_storage::StorageError> for AdaptiveLsmMaintenanceError {
    fn from(error: netbadb_storage::StorageError) -> Self {
        Self::Database(error.into())
    }
}

impl Database {
    pub fn observe_adaptive_lsm_maintenance(
        &self,
        table_id: TableId,
        budget: MaintenanceBudget,
    ) -> Result<Vec<AdaptiveLsmMaintenanceObservation>, DatabaseError> {
        let observed_global_commit_seq = self
            .current_database_snapshot()?
            .map(|snapshot| snapshot.commit_seq());
        let core_busy = Rc::strong_count(&self.transaction_owner) != 1
            || self.inspect_group_commit().is_some()
            || self.schema_writer.get().is_some();
        let storage_ids = self
            .bindings
            .placement(table_id)?
            .storage_ids()
            .collect::<Vec<_>>();
        let mut observations = Vec::new();
        for storage_id in storage_ids {
            let storage = self
                .registry
                .get(storage_id)
                .ok_or(StorageRegistryError::UnknownStorageId { storage_id })?;
            if storage.kind() != StorageKind::Lsm {
                continue;
            }
            let maintenance = storage
                .lsm_maintenance_inspection()?
                .ok_or(StorageRegistryError::UnknownStorageId { storage_id })?;
            let candidates =
                lsm_maintenance_candidates(table_id, storage_id, &maintenance, core_busy, budget);
            observations.push(AdaptiveLsmMaintenanceObservation {
                observed_global_commit_seq,
                schema_generation: self.schema_generation(),
                table_id,
                storage_id,
                storage_kind: storage.kind(),
                storage_snapshot: storage.current_snapshot_token()?,
                logical_data_version: storage
                    .inspect_change_stream_maintenance()
                    .stream
                    .current_data_version,
                lsm: storage
                    .lsm_inspection()
                    .ok_or(StorageRegistryError::UnknownStorageId { storage_id })?,
                change_stream: storage.inspect_change_stream(),
                production_flush_candidate: candidates
                    .iter()
                    .find(|candidate| {
                        matches!(candidate.action, MaintenanceAction::FlushLsm { .. })
                    })
                    .cloned(),
                production_compaction_candidate: candidates
                    .iter()
                    .find(|candidate| {
                        matches!(candidate.action, MaintenanceAction::CompactLsm { .. })
                    })
                    .cloned(),
                maintenance,
            });
        }
        Ok(observations)
    }

    pub fn execute_adaptive_lsm_maintenance(
        &mut self,
        proposal: &AdaptiveLsmMaintenanceProposal,
        budget: MaintenanceBudget,
    ) -> Result<AdaptiveLsmMaintenanceExecutionReport, AdaptiveLsmMaintenanceError> {
        let (table_id, storage_id, estimated_cost) = match proposal {
            AdaptiveLsmMaintenanceProposal::Flush(proposal) => (
                proposal.table_id,
                proposal.storage_id,
                proposal.estimated_cost,
            ),
            AdaptiveLsmMaintenanceProposal::CompactOne(proposal) => (
                proposal.table_id,
                proposal.storage_id,
                proposal.estimated_cost,
            ),
        };
        let bound = proposal.conservative_bound();
        let current = self
            .observe_adaptive_lsm_maintenance(table_id, budget)?
            .into_iter()
            .find(|observation| observation.storage_id == storage_id);
        let Some(before) = current else {
            return Ok(aborted_report(proposal, budget, estimated_cost, bound));
        };
        let revalidated = revalidate_lsm_proposal(proposal, &before, budget);
        if !revalidated {
            return Ok(aborted_report(proposal, budget, estimated_cost, bound));
        }

        let rows_before = self.capture_lsm_rows(storage_id)?;
        let compacted = {
            let storage = self
                .registry
                .get_mut(storage_id)
                .ok_or(StorageRegistryError::UnknownStorageId { storage_id })?;
            match proposal.action() {
                AdaptiveLsmMaintenanceAction::Flush => {
                    storage.flush()?;
                    true
                }
                AdaptiveLsmMaintenanceAction::CompactOne => storage.compact_lsm_one()?,
            }
        };
        if !compacted {
            return Ok(AdaptiveLsmMaintenanceExecutionReport {
                proposal: proposal.clone(),
                action: proposal.action(),
                budget_before: budget,
                conservative_bound: bound,
                estimated_cost,
                consumed: MaintenanceConsumption {
                    actions: 1,
                    ..MaintenanceConsumption::default()
                },
                budget_remaining: budget.remaining(MaintenanceConsumption {
                    actions: 1,
                    ..MaintenanceConsumption::default()
                }),
                measurement: None,
                outcome: AdaptiveLsmMaintenanceOutcome::InconclusiveNoWork,
            });
        }
        let after = self
            .observe_adaptive_lsm_maintenance(table_id, budget)?
            .into_iter()
            .find(|observation| observation.storage_id == storage_id)
            .ok_or(StorageRegistryError::UnknownStorageId { storage_id })?;
        let rows_after = self.capture_lsm_rows(storage_id)?;
        let (consumed, obsolete_bytes) = measured_consumption(
            proposal.action(),
            &before.lsm,
            &after.lsm,
            estimated_cost.work_units,
        )?;
        if !budget.contains(consumed)
            || consumed.work_units > bound.work_units
            || consumed.read_bytes > bound.read_bytes
            || consumed.write_bytes > bound.write_bytes
        {
            return Err(AdaptiveLsmMaintenanceError::ConservativeBoundViolated {
                bound,
                actual: consumed,
            });
        }
        verify_postconditions(
            proposal.action(),
            &before,
            &after,
            &rows_before,
            &rows_after,
        )?;
        let measurement = AdaptiveLsmMaintenanceMeasurement {
            lsm_before: before.lsm,
            lsm_after: after.lsm,
            maintenance_before: before.maintenance,
            maintenance_after: after.maintenance,
            storage_snapshot_before: before.storage_snapshot,
            storage_snapshot_after: after.storage_snapshot,
            logical_data_version_before: before.logical_data_version,
            logical_data_version_after: after.logical_data_version,
            change_stream_before: before.change_stream,
            change_stream_after: after.change_stream,
            global_commit_seq_before: before.observed_global_commit_seq,
            global_commit_seq_after: after.observed_global_commit_seq,
            schema_generation_before: before.schema_generation,
            schema_generation_after: after.schema_generation,
            logical_rows_unchanged: true,
            obsolete_bytes,
        };
        Ok(AdaptiveLsmMaintenanceExecutionReport {
            proposal: proposal.clone(),
            action: proposal.action(),
            budget_before: budget,
            conservative_bound: bound,
            estimated_cost,
            consumed,
            budget_remaining: budget.remaining(consumed),
            measurement: Some(measurement),
            outcome: AdaptiveLsmMaintenanceOutcome::Completed,
        })
    }

    fn capture_lsm_rows(
        &mut self,
        storage_id: StorageId,
    ) -> Result<Vec<(StorageVersionKey, Vec<netbadb_types::ScalarValue>)>, DatabaseError> {
        let storage = self
            .registry
            .get_mut(storage_id)
            .ok_or(StorageRegistryError::UnknownStorageId { storage_id })?;
        let columns = storage
            .table()
            .columns
            .iter()
            .map(|column| column.id)
            .collect::<Vec<_>>();
        let view = storage.read_view()?;
        Ok(storage.scan_versioned_columns_with_view(&columns, &view)?)
    }
}

fn measured_consumption(
    action: AdaptiveLsmMaintenanceAction,
    before: &LsmInspection,
    after: &LsmInspection,
    work_units: u64,
) -> Result<(MaintenanceConsumption, u64), AdaptiveLsmMaintenanceError> {
    if before.write_amplification.overflowed || after.write_amplification.overflowed {
        return Err(AdaptiveLsmMaintenanceError::AmplificationCounterUnavailable);
    }
    let (read_bytes, write_bytes) = match action {
        AdaptiveLsmMaintenanceAction::Flush => (
            after
                .write_amplification
                .flush_input_bytes
                .checked_sub(before.write_amplification.flush_input_bytes),
            after
                .write_amplification
                .flush_output_bytes
                .checked_sub(before.write_amplification.flush_output_bytes),
        ),
        AdaptiveLsmMaintenanceAction::CompactOne => (
            after
                .write_amplification
                .compaction_input_bytes
                .checked_sub(before.write_amplification.compaction_input_bytes),
            after
                .write_amplification
                .compaction_output_bytes
                .checked_sub(before.write_amplification.compaction_output_bytes),
        ),
    };
    let obsolete_bytes = after
        .write_amplification
        .obsolete_bytes
        .checked_sub(before.write_amplification.obsolete_bytes)
        .ok_or(AdaptiveLsmMaintenanceError::AmplificationCounterUnavailable)?;
    let consumed = MaintenanceConsumption {
        work_units,
        read_bytes: read_bytes
            .ok_or(AdaptiveLsmMaintenanceError::AmplificationCounterUnavailable)?,
        write_bytes: write_bytes
            .ok_or(AdaptiveLsmMaintenanceError::AmplificationCounterUnavailable)?,
        actions: 1,
    };
    Ok((consumed, obsolete_bytes))
}

fn verify_postconditions(
    action: AdaptiveLsmMaintenanceAction,
    before: &AdaptiveLsmMaintenanceObservation,
    after: &AdaptiveLsmMaintenanceObservation,
    rows_before: &[(StorageVersionKey, Vec<netbadb_types::ScalarValue>)],
    rows_after: &[(StorageVersionKey, Vec<netbadb_types::ScalarValue>)],
) -> Result<(), AdaptiveLsmMaintenanceError> {
    if rows_before != rows_after {
        return Err(AdaptiveLsmMaintenanceError::PostconditionFailed(
            "logical rows changed",
        ));
    }
    if before.logical_data_version != after.logical_data_version {
        return Err(AdaptiveLsmMaintenanceError::PostconditionFailed(
            "logical data version changed",
        ));
    }
    if before.observed_global_commit_seq != after.observed_global_commit_seq {
        return Err(AdaptiveLsmMaintenanceError::PostconditionFailed(
            "database commit sequence changed",
        ));
    }
    if before.schema_generation != after.schema_generation {
        return Err(AdaptiveLsmMaintenanceError::PostconditionFailed(
            "schema generation changed",
        ));
    }
    if before.change_stream != after.change_stream {
        return Err(AdaptiveLsmMaintenanceError::PostconditionFailed(
            "change stream changed",
        ));
    }
    if before.maintenance.anchor.visible_commit_sequence
        != after.maintenance.anchor.visible_commit_sequence
    {
        return Err(AdaptiveLsmMaintenanceError::PostconditionFailed(
            "visible commit horizon changed",
        ));
    }
    if after.maintenance.anchor.manifest_generation <= before.maintenance.anchor.manifest_generation
    {
        return Err(AdaptiveLsmMaintenanceError::PostconditionFailed(
            "layout anchor did not advance",
        ));
    }
    match action {
        AdaptiveLsmMaintenanceAction::Flush => {
            if after.maintenance.memtable_entry_count != 0 || after.maintenance.memtable_bytes != 0
            {
                return Err(AdaptiveLsmMaintenanceError::PostconditionFailed(
                    "flush left MemTable entries",
                ));
            }
            if after.maintenance.anchor.wal_generation <= before.maintenance.anchor.wal_generation {
                return Err(AdaptiveLsmMaintenanceError::PostconditionFailed(
                    "flush WAL generation did not advance",
                ));
            }
        }
        AdaptiveLsmMaintenanceAction::CompactOne => {
            if after.maintenance.memtable_entry_count != 0 {
                return Err(AdaptiveLsmMaintenanceError::PostconditionFailed(
                    "compaction populated MemTable",
                ));
            }
            if after.maintenance.anchor.wal_generation != before.maintenance.anchor.wal_generation {
                return Err(AdaptiveLsmMaintenanceError::PostconditionFailed(
                    "compaction changed WAL generation",
                ));
            }
        }
    }
    Ok(())
}

fn aborted_report(
    proposal: &AdaptiveLsmMaintenanceProposal,
    budget: MaintenanceBudget,
    estimated_cost: MaintenanceEstimate,
    bound: MaintenanceEstimate,
) -> AdaptiveLsmMaintenanceExecutionReport {
    AdaptiveLsmMaintenanceExecutionReport {
        proposal: proposal.clone(),
        action: proposal.action(),
        budget_before: budget,
        conservative_bound: bound,
        estimated_cost,
        consumed: MaintenanceConsumption::default(),
        budget_remaining: budget,
        measurement: None,
        outcome: AdaptiveLsmMaintenanceOutcome::Aborted(
            AdaptiveLsmMaintenanceAbortReason::PreconditionsChanged,
        ),
    }
}
