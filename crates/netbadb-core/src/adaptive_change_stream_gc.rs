use std::error::Error;
use std::fmt;

use netbadb_storage::{
    ChangeStreamGcStorageReport, ChangeStreamMaintenanceInspection, ChangeStreamStatus, StorageKind,
};
use netbadb_types::{
    ChangeStreamGeneration, ColumnarGeneration, ColumnarProjectionId, SchemaGeneration,
    StorageDataVersion, StorageId, TableId,
};

use crate::{
    ChangeStreamGcReport, Database, DatabaseError, MaintenanceBound, MaintenanceBudget,
    MaintenanceConsumption, MaintenanceEstimate, StorageRegistryError,
};

/// Exact committed batch boundary through which the production NBCL rewrite
/// may delete history. Reclaim-through `F` removes batches whose `after <= F`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SafeReclaimThrough(StorageDataVersion);

impl SafeReclaimThrough {
    #[must_use]
    pub const fn frontier(self) -> StorageDataVersion {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeStreamRetentionConsumer {
    Columnar {
        projection_id: ColumnarProjectionId,
        projection_generation: ColumnarGeneration,
        source_storage_id: StorageId,
        stream_generation: ChangeStreamGeneration,
        required_frontier: StorageDataVersion,
        managed: bool,
    },
    RuntimePin {
        storage_id: StorageId,
        stream_generation: ChangeStreamGeneration,
        required_frontier: StorageDataVersion,
    },
}

impl ChangeStreamRetentionConsumer {
    #[must_use]
    pub const fn required_frontier(self) -> StorageDataVersion {
        match self {
            Self::Columnar {
                required_frontier, ..
            }
            | Self::RuntimePin {
                required_frontier, ..
            } => required_frontier,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChangeStreamReclaimPrefix {
    pub reclaim_through: SafeReclaimThrough,
    pub batches: u64,
    pub mutations: u64,
    /// Encoded prepared and finalize records removed from the prefix.
    pub record_bytes: u64,
    /// Exact size of the v2 file emitted by the production suffix rewrite.
    pub rewrite_bytes: u64,
    /// Exact file-length reduction expected from the rewrite.
    pub reclaimed_file_bytes: u64,
    /// One structural unit for the header plus one per retained batch.
    pub rewrite_work_units: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdaptiveChangeStreamGcSafetyBlocker {
    StreamUnavailable,
    PreparedChangesUnresolved,
    FinalizeCheckpointPending,
    ProjectionCatalogUnavailable,
    ManagedProjectionUnavailable,
    UnmanagedIncrementalProjection,
    RetentionFrontierInvalid,
    HistoryUnavailable,
    NoRetentionConsumer,
    NoReclaimableHistory,
    ArithmeticOverflow,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdaptiveChangeStreamGcObservation {
    pub schema_generation: SchemaGeneration,
    pub table_id: TableId,
    pub storage_id: StorageId,
    pub storage_kind: StorageKind,
    pub stream_generation: Option<ChangeStreamGeneration>,
    pub stream_origin_frontier: Option<StorageDataVersion>,
    pub earliest_available_frontier: Option<StorageDataVersion>,
    pub current_frontier: StorageDataVersion,
    pub prepared_unresolved_count: u64,
    pub pending_finalize_checkpoint_count: u64,
    pub batch_count: u64,
    pub file_bytes: u64,
    pub consumers: Vec<ChangeStreamRetentionConsumer>,
    pub limiting_consumers: Vec<ChangeStreamRetentionConsumer>,
    pub safe_reclaim_through: Option<SafeReclaimThrough>,
    pub reclaimable_prefix: Option<ChangeStreamReclaimPrefix>,
    pub blocker: Option<AdaptiveChangeStreamGcSafetyBlocker>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdaptiveChangeStreamGcPolicy {
    pub minimum_reclaimable_batches: u64,
    pub minimum_reclaimable_bytes: u64,
}

impl AdaptiveChangeStreamGcPolicy {
    #[must_use]
    pub const fn new(minimum_reclaimable_batches: u64, minimum_reclaimable_bytes: u64) -> Self {
        Self {
            minimum_reclaimable_batches,
            minimum_reclaimable_bytes,
        }
    }

    #[must_use]
    pub const fn is_valid(self) -> bool {
        self.minimum_reclaimable_batches != 0 || self.minimum_reclaimable_bytes != 0
    }

    const fn admits(self, prefix: ChangeStreamReclaimPrefix) -> bool {
        (self.minimum_reclaimable_batches != 0
            && prefix.batches >= self.minimum_reclaimable_batches)
            || (self.minimum_reclaimable_bytes != 0
                && prefix.reclaimed_file_bytes >= self.minimum_reclaimable_bytes)
    }
}

impl Default for AdaptiveChangeStreamGcPolicy {
    fn default() -> Self {
        Self::new(16, 1024 * 1024)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdaptiveChangeStreamGcNoActionReason {
    Safety(AdaptiveChangeStreamGcSafetyBlocker),
    PressureBelowThreshold,
    CostBoundExceeded,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdaptiveChangeStreamGcProposal {
    pub schema_generation: SchemaGeneration,
    pub table_id: TableId,
    pub storage_id: StorageId,
    pub storage_kind: StorageKind,
    pub stream_generation: ChangeStreamGeneration,
    pub observed_earliest_frontier: StorageDataVersion,
    pub observed_current_frontier: StorageDataVersion,
    pub safe_reclaim_through: SafeReclaimThrough,
    pub consumers: Vec<ChangeStreamRetentionConsumer>,
    pub limiting_consumers: Vec<ChangeStreamRetentionConsumer>,
    pub expected_prefix: ChangeStreamReclaimPrefix,
    pub bound: MaintenanceBound,
    pub estimated_cost: MaintenanceEstimate,
    pub policy: AdaptiveChangeStreamGcPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdaptiveChangeStreamGcDecision {
    NoAction(AdaptiveChangeStreamGcNoActionReason),
    Proposal(Box<AdaptiveChangeStreamGcProposal>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdaptiveChangeStreamGcAbortReason {
    SchemaChanged,
    TargetIdentityChanged,
    StreamGenerationChanged,
    PreconditionsChanged,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdaptiveChangeStreamGcOutcome {
    Completed,
    Aborted(AdaptiveChangeStreamGcAbortReason),
    InconclusiveAlreadyReclaimed,
    InconclusiveNoWork,
    InconclusiveCostBoundExceeded,
    InconclusivePostconditionsChanged,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdaptiveChangeStreamGcExecutionReport {
    pub proposal: AdaptiveChangeStreamGcProposal,
    pub current_safe_reclaim_through: Option<SafeReclaimThrough>,
    pub actual: Option<ChangeStreamGcReport>,
    pub consumed: MaintenanceConsumption,
    pub outcome: AdaptiveChangeStreamGcOutcome,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdaptiveChangeStreamGcAdvisorError {
    InvalidPolicy,
}

impl fmt::Display for AdaptiveChangeStreamGcAdvisorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPolicy => formatter.write_str(
                "automatic change-stream GC requires a non-zero batch or byte threshold",
            ),
        }
    }
}

impl Error for AdaptiveChangeStreamGcAdvisorError {}

#[derive(Debug, Clone)]
pub(crate) struct ChangeStreamRetentionAssessment {
    pub observation: AdaptiveChangeStreamGcObservation,
    pub maintenance: ChangeStreamMaintenanceInspection,
}

impl Database {
    /// Captures detached, payload-free retention evidence for one explicit
    /// table. It does not mutate the stream, pins, fairness, or trial state.
    pub fn observe_change_stream_reclamation(
        &self,
        table_id: TableId,
    ) -> Result<AdaptiveChangeStreamGcObservation, DatabaseError> {
        Ok(self.assess_change_stream_retention(table_id)?.observation)
    }

    pub fn advise_change_stream_reclamation(
        &self,
        observation: &AdaptiveChangeStreamGcObservation,
        policy: AdaptiveChangeStreamGcPolicy,
        budget: MaintenanceBudget,
    ) -> Result<AdaptiveChangeStreamGcDecision, AdaptiveChangeStreamGcAdvisorError> {
        if !policy.is_valid() {
            return Err(AdaptiveChangeStreamGcAdvisorError::InvalidPolicy);
        }
        if let Some(blocker) = observation.blocker {
            return Ok(AdaptiveChangeStreamGcDecision::NoAction(
                AdaptiveChangeStreamGcNoActionReason::Safety(blocker),
            ));
        }
        let Some(prefix) = observation.reclaimable_prefix else {
            return Ok(AdaptiveChangeStreamGcDecision::NoAction(
                AdaptiveChangeStreamGcNoActionReason::Safety(
                    AdaptiveChangeStreamGcSafetyBlocker::NoReclaimableHistory,
                ),
            ));
        };
        if !policy.admits(prefix) {
            return Ok(AdaptiveChangeStreamGcDecision::NoAction(
                AdaptiveChangeStreamGcNoActionReason::PressureBelowThreshold,
            ));
        }
        let estimate = exact_rewrite_estimate(observation, prefix);
        if !budget.admits(estimate) {
            return Ok(AdaptiveChangeStreamGcDecision::NoAction(
                AdaptiveChangeStreamGcNoActionReason::CostBoundExceeded,
            ));
        }
        let Some(stream_generation) = observation.stream_generation else {
            return Ok(AdaptiveChangeStreamGcDecision::NoAction(
                AdaptiveChangeStreamGcNoActionReason::Safety(
                    AdaptiveChangeStreamGcSafetyBlocker::StreamUnavailable,
                ),
            ));
        };
        let Some(observed_earliest_frontier) = observation.earliest_available_frontier else {
            return Ok(AdaptiveChangeStreamGcDecision::NoAction(
                AdaptiveChangeStreamGcNoActionReason::Safety(
                    AdaptiveChangeStreamGcSafetyBlocker::HistoryUnavailable,
                ),
            ));
        };
        Ok(AdaptiveChangeStreamGcDecision::Proposal(Box::new(
            AdaptiveChangeStreamGcProposal {
                schema_generation: observation.schema_generation,
                table_id: observation.table_id,
                storage_id: observation.storage_id,
                storage_kind: observation.storage_kind,
                stream_generation,
                observed_earliest_frontier,
                observed_current_frontier: observation.current_frontier,
                safe_reclaim_through: prefix.reclaim_through,
                consumers: observation.consumers.clone(),
                limiting_consumers: observation.limiting_consumers.clone(),
                expected_prefix: prefix,
                bound: MaintenanceBound::HardBoundedRewrite,
                estimated_cost: estimate,
                policy,
            },
        )))
    }

    /// Revalidates every current retention authority, then invokes the one
    /// production NBCL GC writer exactly once if the old proposal remains safe.
    pub fn execute_change_stream_reclamation(
        &mut self,
        proposal: &AdaptiveChangeStreamGcProposal,
        budget: MaintenanceBudget,
    ) -> Result<AdaptiveChangeStreamGcExecutionReport, DatabaseError> {
        let no_mutation = |outcome, safe| AdaptiveChangeStreamGcExecutionReport {
            proposal: proposal.clone(),
            current_safe_reclaim_through: safe,
            actual: None,
            consumed: MaintenanceConsumption::default(),
            outcome,
        };
        if self.schema_generation() != proposal.schema_generation {
            return Ok(no_mutation(
                AdaptiveChangeStreamGcOutcome::Aborted(
                    AdaptiveChangeStreamGcAbortReason::SchemaChanged,
                ),
                None,
            ));
        }
        let current = match self.assess_change_stream_retention(proposal.table_id) {
            Ok(current) => current,
            Err(DatabaseError::Bind(_)) | Err(DatabaseError::Registry(_)) => {
                return Ok(no_mutation(
                    AdaptiveChangeStreamGcOutcome::Aborted(
                        AdaptiveChangeStreamGcAbortReason::TargetIdentityChanged,
                    ),
                    None,
                ));
            }
            Err(error) => return Err(error),
        };
        let observation = &current.observation;
        if observation.storage_id != proposal.storage_id
            || observation.storage_kind != proposal.storage_kind
        {
            return Ok(no_mutation(
                AdaptiveChangeStreamGcOutcome::Aborted(
                    AdaptiveChangeStreamGcAbortReason::TargetIdentityChanged,
                ),
                observation.safe_reclaim_through,
            ));
        }
        if observation.stream_generation != Some(proposal.stream_generation) {
            return Ok(no_mutation(
                AdaptiveChangeStreamGcOutcome::Aborted(
                    AdaptiveChangeStreamGcAbortReason::StreamGenerationChanged,
                ),
                observation.safe_reclaim_through,
            ));
        }
        let Some(earliest) = observation.earliest_available_frontier else {
            return Ok(no_mutation(
                AdaptiveChangeStreamGcOutcome::Aborted(
                    AdaptiveChangeStreamGcAbortReason::PreconditionsChanged,
                ),
                observation.safe_reclaim_through,
            ));
        };
        let proposed = proposal.safe_reclaim_through.frontier();
        if earliest.0 >= proposed.0 {
            return Ok(no_mutation(
                AdaptiveChangeStreamGcOutcome::InconclusiveAlreadyReclaimed,
                observation.safe_reclaim_through,
            ));
        }
        if observation.blocker.is_some()
            || observation
                .safe_reclaim_through
                .is_none_or(|safe| safe.frontier().0 < proposed.0)
        {
            return Ok(no_mutation(
                AdaptiveChangeStreamGcOutcome::Aborted(
                    AdaptiveChangeStreamGcAbortReason::PreconditionsChanged,
                ),
                observation.safe_reclaim_through,
            ));
        }
        let prefix = match reclaim_prefix(&current.maintenance, proposed) {
            Ok(Some(prefix)) => prefix,
            Ok(None) => {
                return Ok(no_mutation(
                    AdaptiveChangeStreamGcOutcome::InconclusiveNoWork,
                    observation.safe_reclaim_through,
                ));
            }
            Err(_) => {
                return Ok(no_mutation(
                    AdaptiveChangeStreamGcOutcome::Aborted(
                        AdaptiveChangeStreamGcAbortReason::PreconditionsChanged,
                    ),
                    observation.safe_reclaim_through,
                ));
            }
        };
        let current_cost = exact_rewrite_estimate(observation, prefix);
        if !budget.admits(current_cost) {
            return Ok(no_mutation(
                AdaptiveChangeStreamGcOutcome::InconclusiveCostBoundExceeded,
                observation.safe_reclaim_through,
            ));
        }

        let storage_report = self.gc_change_stream_through(proposal.table_id, proposed)?;
        let consumed = MaintenanceConsumption {
            work_units: current_cost.work_units,
            read_bytes: storage_report.bytes_before,
            write_bytes: storage_report.bytes_after,
            actions: 1,
        };
        let after = self.assess_change_stream_retention(proposal.table_id)?;
        let valid = after.observation.stream_generation == Some(proposal.stream_generation)
            && after.observation.earliest_available_frontier == Some(proposed)
            && storage_report.generation == proposal.stream_generation
            && storage_report.previous_earliest_frontier == earliest
            && storage_report.new_earliest_frontier == proposed
            && storage_report.current_frontier == observation.current_frontier
            && after.observation.current_frontier == observation.current_frontier
            && after.observation.current_frontier.0 >= proposed.0
            && after.observation.schema_generation == proposal.schema_generation
            && after
                .observation
                .consumers
                .iter()
                .all(|consumer| consumer.required_frontier().0 >= proposed.0)
            && stream_is_contiguous(&after.maintenance);
        Ok(AdaptiveChangeStreamGcExecutionReport {
            proposal: proposal.clone(),
            current_safe_reclaim_through: observation.safe_reclaim_through,
            actual: Some(storage_report),
            consumed,
            outcome: if valid {
                AdaptiveChangeStreamGcOutcome::Completed
            } else {
                AdaptiveChangeStreamGcOutcome::InconclusivePostconditionsChanged
            },
        })
    }

    pub(crate) fn assess_change_stream_retention(
        &self,
        table_id: TableId,
    ) -> Result<ChangeStreamRetentionAssessment, DatabaseError> {
        let storage_id = self.bindings.resolve_single(table_id)?;
        let storage = self
            .registry
            .get(storage_id)
            .ok_or(StorageRegistryError::UnknownStorageId { storage_id })?;
        let maintenance = storage.inspect_change_stream_maintenance();
        let mut observation = AdaptiveChangeStreamGcObservation {
            schema_generation: self.schema_generation(),
            table_id,
            storage_id,
            storage_kind: storage.kind(),
            stream_generation: maintenance.stream.generation,
            stream_origin_frontier: maintenance.stream.stream_origin_frontier,
            earliest_available_frontier: maintenance.stream.earliest_available_frontier,
            current_frontier: maintenance.stream.current_data_version,
            prepared_unresolved_count: maintenance.stream.prepared_unresolved_count,
            pending_finalize_checkpoint_count: maintenance.stream.pending_finalize_checkpoint_count,
            batch_count: maintenance.stream.committed_batch_count,
            file_bytes: maintenance.stream.file_bytes,
            consumers: Vec::new(),
            limiting_consumers: Vec::new(),
            safe_reclaim_through: None,
            reclaimable_prefix: None,
            blocker: None,
        };
        if maintenance.stream.status != ChangeStreamStatus::Enabled {
            observation.blocker = Some(AdaptiveChangeStreamGcSafetyBlocker::StreamUnavailable);
            return Ok(ChangeStreamRetentionAssessment {
                observation,
                maintenance,
            });
        }
        let Some(generation) = maintenance.stream.generation else {
            observation.blocker = Some(AdaptiveChangeStreamGcSafetyBlocker::StreamUnavailable);
            return Ok(ChangeStreamRetentionAssessment {
                observation,
                maintenance,
            });
        };
        if maintenance.stream.prepared_unresolved_count != 0 {
            observation.blocker =
                Some(AdaptiveChangeStreamGcSafetyBlocker::PreparedChangesUnresolved);
            return Ok(ChangeStreamRetentionAssessment {
                observation,
                maintenance,
            });
        }
        if maintenance.stream.pending_finalize_checkpoint_count != 0 {
            observation.blocker =
                Some(AdaptiveChangeStreamGcSafetyBlocker::FinalizeCheckpointPending);
            return Ok(ChangeStreamRetentionAssessment {
                observation,
                maintenance,
            });
        }
        if self
            .projections
            .ensure_retention_catalog_available()
            .is_err()
        {
            observation.blocker =
                Some(AdaptiveChangeStreamGcSafetyBlocker::ProjectionCatalogUnavailable);
            return Ok(ChangeStreamRetentionAssessment {
                observation,
                maintenance,
            });
        }
        for entry in self
            .projections
            .iter()
            .filter(|entry| entry.identity.source_storage_id == storage_id)
        {
            let Some(projection) = &entry.projection else {
                if entry.managed {
                    observation.blocker =
                        Some(AdaptiveChangeStreamGcSafetyBlocker::ManagedProjectionUnavailable);
                }
                continue;
            };
            let Some(incremental) = &projection.metadata().incremental else {
                continue;
            };
            if !entry.managed {
                observation.blocker =
                    Some(AdaptiveChangeStreamGcSafetyBlocker::UnmanagedIncrementalProjection);
                continue;
            }
            if incremental.stream_generation != generation {
                continue;
            }
            observation
                .consumers
                .push(ChangeStreamRetentionConsumer::Columnar {
                    projection_id: entry.identity.id,
                    projection_generation: entry.identity.generation,
                    source_storage_id: storage_id,
                    stream_generation: generation,
                    required_frontier: incremental.applied_frontier,
                    managed: true,
                });
        }
        for pin in &maintenance.retention_pins {
            if pin.storage_id == storage_id && pin.generation == generation {
                observation
                    .consumers
                    .push(ChangeStreamRetentionConsumer::RuntimePin {
                        storage_id,
                        stream_generation: generation,
                        required_frontier: pin.frontier,
                    });
            }
        }
        if observation.blocker.is_some() {
            return Ok(ChangeStreamRetentionAssessment {
                observation,
                maintenance,
            });
        }
        let Some(earliest) = maintenance.stream.earliest_available_frontier else {
            observation.blocker = Some(AdaptiveChangeStreamGcSafetyBlocker::HistoryUnavailable);
            return Ok(ChangeStreamRetentionAssessment {
                observation,
                maintenance,
            });
        };
        if observation.consumers.is_empty() {
            observation.blocker = Some(AdaptiveChangeStreamGcSafetyBlocker::NoRetentionConsumer);
            return Ok(ChangeStreamRetentionAssessment {
                observation,
                maintenance,
            });
        }
        if observation.consumers.iter().any(|consumer| {
            let frontier = consumer.required_frontier();
            frontier.0 < earliest.0 || frontier.0 > observation.current_frontier.0
        }) {
            observation.blocker =
                Some(AdaptiveChangeStreamGcSafetyBlocker::RetentionFrontierInvalid);
            return Ok(ChangeStreamRetentionAssessment {
                observation,
                maintenance,
            });
        }
        let Some(safe) = observation
            .consumers
            .iter()
            .map(|consumer| consumer.required_frontier())
            .min_by_key(|frontier| frontier.0)
        else {
            observation.blocker = Some(AdaptiveChangeStreamGcSafetyBlocker::NoRetentionConsumer);
            return Ok(ChangeStreamRetentionAssessment {
                observation,
                maintenance,
            });
        };
        observation.limiting_consumers = observation
            .consumers
            .iter()
            .copied()
            .filter(|consumer| consumer.required_frontier() == safe)
            .collect();
        observation.safe_reclaim_through = Some(SafeReclaimThrough(safe));
        match reclaim_prefix(&maintenance, safe) {
            Ok(Some(prefix)) => observation.reclaimable_prefix = Some(prefix),
            Ok(None) => {
                observation.blocker =
                    Some(AdaptiveChangeStreamGcSafetyBlocker::NoReclaimableHistory);
            }
            Err(blocker) => observation.blocker = Some(blocker),
        }
        Ok(ChangeStreamRetentionAssessment {
            observation,
            maintenance,
        })
    }

    pub(crate) fn gc_change_stream_through(
        &mut self,
        table_id: TableId,
        frontier: StorageDataVersion,
    ) -> Result<ChangeStreamGcReport, DatabaseError> {
        let assessment = self.assess_change_stream_retention(table_id)?;
        let storage_id = assessment.observation.storage_id;
        let limiting_projection_ids = assessment
            .observation
            .limiting_consumers
            .iter()
            .filter_map(|consumer| match consumer {
                ChangeStreamRetentionConsumer::Columnar { projection_id, .. } => {
                    Some(*projection_id)
                }
                ChangeStreamRetentionConsumer::RuntimePin { .. } => None,
            })
            .collect::<Vec<_>>();
        let reclaimed: ChangeStreamGcStorageReport = self
            .registry
            .get_mut(storage_id)
            .ok_or(StorageRegistryError::UnknownStorageId { storage_id })?
            .gc_change_stream(frontier)?;
        Ok(ChangeStreamGcReport {
            storage_id: reclaimed.storage_id,
            generation: reclaimed.generation,
            previous_earliest_frontier: reclaimed.previous_earliest_frontier,
            new_earliest_frontier: reclaimed.new_earliest_frontier,
            current_frontier: reclaimed.current_frontier,
            limiting_projection_ids,
            batches_removed: reclaimed.batches_removed,
            mutations_removed: reclaimed.mutations_removed,
            bytes_before: reclaimed.bytes_before,
            bytes_after: reclaimed.bytes_after,
            bytes_reclaimed: reclaimed.bytes_reclaimed,
        })
    }
}

pub(crate) const fn exact_rewrite_estimate(
    observation: &AdaptiveChangeStreamGcObservation,
    prefix: ChangeStreamReclaimPrefix,
) -> MaintenanceEstimate {
    MaintenanceEstimate {
        work_units: prefix.rewrite_work_units,
        read_bytes: observation.file_bytes,
        write_bytes: prefix.rewrite_bytes,
    }
}

fn reclaim_prefix(
    maintenance: &ChangeStreamMaintenanceInspection,
    frontier: StorageDataVersion,
) -> Result<Option<ChangeStreamReclaimPrefix>, AdaptiveChangeStreamGcSafetyBlocker> {
    let earliest = maintenance
        .stream
        .earliest_available_frontier
        .ok_or(AdaptiveChangeStreamGcSafetyBlocker::HistoryUnavailable)?;
    if frontier == earliest {
        return Ok(None);
    }
    if frontier.0 < earliest.0 || frontier.0 > maintenance.stream.current_data_version.0 {
        return Err(AdaptiveChangeStreamGcSafetyBlocker::HistoryUnavailable);
    }
    let mut expected = earliest;
    let mut first_retained = None;
    for (index, batch) in maintenance.batches.iter().enumerate() {
        if batch.before != expected || batch.after.0 <= batch.before.0 {
            return Err(AdaptiveChangeStreamGcSafetyBlocker::HistoryUnavailable);
        }
        if batch.after.0 > frontier.0 {
            if batch.before != frontier {
                return Err(AdaptiveChangeStreamGcSafetyBlocker::HistoryUnavailable);
            }
            first_retained = Some(index);
            break;
        }
        expected = batch.after;
    }
    let first_retained = first_retained.unwrap_or(maintenance.batches.len());
    if first_retained == maintenance.batches.len()
        && frontier != maintenance.stream.current_data_version
    {
        return Err(AdaptiveChangeStreamGcSafetyBlocker::HistoryUnavailable);
    }
    let batches = u64::try_from(first_retained)
        .map_err(|_| AdaptiveChangeStreamGcSafetyBlocker::ArithmeticOverflow)?;
    let mutations = checked_sum(
        maintenance.batches[..first_retained]
            .iter()
            .map(|batch| batch.mutation_count),
    )?;
    let record_bytes = checked_sum(
        maintenance.batches[..first_retained]
            .iter()
            .map(|batch| batch.retained_file_bytes),
    )?;
    let retained_bytes = checked_sum(
        maintenance.batches[first_retained..]
            .iter()
            .map(|batch| batch.retained_file_bytes),
    )?;
    let rewrite_bytes = maintenance
        .rewrite_header_bytes
        .checked_add(retained_bytes)
        .ok_or(AdaptiveChangeStreamGcSafetyBlocker::ArithmeticOverflow)?;
    let reclaimed_file_bytes = maintenance
        .stream
        .file_bytes
        .checked_sub(rewrite_bytes)
        .ok_or(AdaptiveChangeStreamGcSafetyBlocker::ArithmeticOverflow)?;
    let retained_batch_count = u64::try_from(maintenance.batches.len() - first_retained)
        .map_err(|_| AdaptiveChangeStreamGcSafetyBlocker::ArithmeticOverflow)?;
    let rewrite_work_units = retained_batch_count
        .checked_add(1)
        .ok_or(AdaptiveChangeStreamGcSafetyBlocker::ArithmeticOverflow)?;
    Ok(Some(ChangeStreamReclaimPrefix {
        reclaim_through: SafeReclaimThrough(frontier),
        batches,
        mutations,
        record_bytes,
        rewrite_bytes,
        reclaimed_file_bytes,
        rewrite_work_units,
    }))
}

fn checked_sum(
    mut values: impl Iterator<Item = u64>,
) -> Result<u64, AdaptiveChangeStreamGcSafetyBlocker> {
    values.try_fold(0_u64, |total, value| {
        total
            .checked_add(value)
            .ok_or(AdaptiveChangeStreamGcSafetyBlocker::ArithmeticOverflow)
    })
}

fn stream_is_contiguous(maintenance: &ChangeStreamMaintenanceInspection) -> bool {
    let Some(mut expected) = maintenance.stream.earliest_available_frontier else {
        return false;
    };
    for batch in &maintenance.batches {
        if batch.before != expected || batch.after.0 <= batch.before.0 {
            return false;
        }
        expected = batch.after;
    }
    expected == maintenance.stream.current_data_version
}
