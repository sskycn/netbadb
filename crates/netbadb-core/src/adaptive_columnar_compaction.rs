use std::error::Error;
use std::fmt;

use netbadb_storage::{StorageKind, StorageSnapshotToken};
use netbadb_types::{
    ColumnarGeneration, ColumnarProjectionId, DatabaseCommitSeq, SchemaGeneration,
    StorageDataVersion, StorageId, TableId,
};

use crate::{
    AdaptiveColumnarState, AdaptiveError, AdaptiveObservation, AdaptiveObservationAnchor,
    AdaptivePlannerEvidence, AdaptivePolicy, ColumnarCompactionReport, Database, DatabaseError,
    MaintenanceAction, MaintenanceBlocker, MaintenanceBound, MaintenanceBudget,
    MaintenanceCandidate, MaintenanceConsumption, MaintenanceEstimate,
};

/// Automatic pressure gate layered above production maintenance eligibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdaptiveColumnarCompactionPolicy {
    pub minimum_delta_segments: u64,
    pub minimum_delta_bytes: u64,
}

impl AdaptiveColumnarCompactionPolicy {
    #[must_use]
    pub const fn new(minimum_delta_segments: u64, minimum_delta_bytes: u64) -> Self {
        Self {
            minimum_delta_segments,
            minimum_delta_bytes,
        }
    }

    #[must_use]
    pub const fn is_valid(self) -> bool {
        self.minimum_delta_segments != 0 || self.minimum_delta_bytes != 0
    }

    const fn admits(self, delta_segments: u64, delta_bytes: u64) -> bool {
        (self.minimum_delta_segments != 0 && delta_segments >= self.minimum_delta_segments)
            || (self.minimum_delta_bytes != 0 && delta_bytes >= self.minimum_delta_bytes)
    }
}

impl Default for AdaptiveColumnarCompactionPolicy {
    fn default() -> Self {
        Self::new(1, 0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdaptiveColumnarCompactionNoActionReason {
    MaintenanceBlocked(MaintenanceBlocker),
    BelowAutomaticCompactionThreshold,
    ProjectionEvidenceUnavailable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdaptiveColumnarCompactionNoAction {
    pub based_on: AdaptiveObservationAnchor,
    pub table_id: TableId,
    pub projection_id: Option<ColumnarProjectionId>,
    pub reason: AdaptiveColumnarCompactionNoActionReason,
}

/// Detached observation composed from Phase 1 source/projection evidence and
/// the exact production maintenance candidate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdaptiveColumnarCompactionObservation {
    pub observation: AdaptiveObservation,
    pub maintenance_candidate: MaintenanceCandidate,
}

/// Evidence-bound intent. Mutation authority remains current-state
/// revalidation followed by the production compaction writer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdaptiveColumnarCompactionProposal {
    pub based_on: AdaptiveObservationAnchor,
    pub table_id: TableId,
    pub storage_id: StorageId,
    pub storage_kind: StorageKind,
    pub expected_source_snapshot: StorageSnapshotToken,
    pub expected_source_data_version: StorageDataVersion,
    pub projection_id: ColumnarProjectionId,
    pub expected_projection_generation: ColumnarGeneration,
    pub expected_projection_frontier: StorageDataVersion,
    pub expected_source_current_frontier: StorageDataVersion,
    pub expected_delta_segment_count: u64,
    pub expected_delta_bytes: u64,
    pub expected_delta_mutations: u64,
    pub expected_suppressed_versions: u64,
    pub bound: MaintenanceBound,
    pub estimated_cost: MaintenanceEstimate,
    pub expected_planner: AdaptivePlannerEvidence,
    pub compaction_policy: AdaptiveColumnarCompactionPolicy,
    pub adaptive_policy: AdaptivePolicy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdaptiveColumnarCompactionDecision {
    NoAction(AdaptiveColumnarCompactionNoAction),
    Proposal(AdaptiveColumnarCompactionProposal),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdaptiveColumnarCompactionAbortReason {
    SchemaChanged,
    PreconditionsChanged,
    BudgetInsufficient,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdaptiveColumnarCompactionOutcome {
    Completed,
    RevertedInsufficientMeasuredBenefit,
    Aborted(AdaptiveColumnarCompactionAbortReason),
    InconclusiveNoWork,
    InconclusiveCostBoundExceeded,
    InconclusivePostconditionsChanged,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdaptiveColumnarCompactionMeasurement {
    pub before: AdaptiveColumnarState,
    pub after_change: AdaptiveColumnarState,
    pub after_outcome: AdaptiveColumnarState,
    pub observed_global_commit_seq: DatabaseCommitSeq,
    pub global_commit_seq_before: DatabaseCommitSeq,
    pub global_commit_seq_after: DatabaseCommitSeq,
    pub schema_generation_before: SchemaGeneration,
    pub schema_generation_after: SchemaGeneration,
    pub source_snapshot_before: StorageSnapshotToken,
    pub source_snapshot_after: StorageSnapshotToken,
    pub source_snapshot_unchanged: bool,
    pub source_data_version_unchanged: bool,
    pub projection_frontier_preserved: bool,
    pub estimated_cost: MaintenanceEstimate,
    pub consumed_cost: MaintenanceConsumption,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdaptiveColumnarCompactionExecutionReport {
    pub proposal: AdaptiveColumnarCompactionProposal,
    pub budget_before: MaintenanceBudget,
    pub consumed: MaintenanceConsumption,
    pub budget_remaining: MaintenanceBudget,
    pub physical: Option<ColumnarCompactionReport>,
    pub measurement: Option<AdaptiveColumnarCompactionMeasurement>,
    pub outcome: AdaptiveColumnarCompactionOutcome,
}

#[derive(Debug)]
pub enum AdaptiveColumnarCompactionError {
    InvalidPolicy,
    Adaptive(AdaptiveError),
}

impl fmt::Display for AdaptiveColumnarCompactionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPolicy => formatter.write_str(
                "automatic Columnar compaction requires at least one non-zero pressure threshold",
            ),
            Self::Adaptive(error) => error.fmt(formatter),
        }
    }
}

impl Error for AdaptiveColumnarCompactionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidPolicy => None,
            Self::Adaptive(error) => Some(error),
        }
    }
}

impl From<AdaptiveError> for AdaptiveColumnarCompactionError {
    fn from(error: AdaptiveError) -> Self {
        Self::Adaptive(error)
    }
}

impl From<DatabaseError> for AdaptiveColumnarCompactionError {
    fn from(error: DatabaseError) -> Self {
        Self::Adaptive(AdaptiveError::Database(error))
    }
}

impl AdaptiveColumnarCompactionObservation {
    pub fn decide(
        &self,
        compaction_policy: AdaptiveColumnarCompactionPolicy,
        adaptive_policy: AdaptivePolicy,
    ) -> Result<AdaptiveColumnarCompactionDecision, AdaptiveColumnarCompactionError> {
        if !compaction_policy.is_valid() {
            return Err(AdaptiveColumnarCompactionError::InvalidPolicy);
        }
        let MaintenanceAction::CompactColumnar { projection_id } =
            self.maintenance_candidate.action
        else {
            return Ok(no_action(
                self,
                None,
                AdaptiveColumnarCompactionNoActionReason::ProjectionEvidenceUnavailable,
            ));
        };
        if let Some(blocker) = self.maintenance_candidate.blocker {
            return Ok(no_action(
                self,
                Some(projection_id),
                AdaptiveColumnarCompactionNoActionReason::MaintenanceBlocked(blocker),
            ));
        }
        if !self.maintenance_candidate.eligible {
            return Ok(no_action(
                self,
                Some(projection_id),
                AdaptiveColumnarCompactionNoActionReason::ProjectionEvidenceUnavailable,
            ));
        }
        let Some(source) = self.observation.source.as_ref() else {
            return Ok(no_action(
                self,
                Some(projection_id),
                AdaptiveColumnarCompactionNoActionReason::ProjectionEvidenceUnavailable,
            ));
        };
        let Some(target) = self.observation.projections.iter().find(|target| {
            target.projection.projection_id == Some(projection_id)
                && target.projection.source_storage_id == Some(source.storage_id)
        }) else {
            return Ok(no_action(
                self,
                Some(projection_id),
                AdaptiveColumnarCompactionNoActionReason::ProjectionEvidenceUnavailable,
            ));
        };
        let projection = &target.projection;
        let (
            Some(generation),
            Some(frontier),
            Some(source_frontier),
            Some(delta_segment_count),
            Some(delta_bytes),
            Some(delta_mutations),
            Some(suppressed_versions),
            Some(planner),
        ) = (
            projection.generation,
            projection.applied_frontier,
            projection.current_source_frontier,
            projection.delta_segment_count,
            projection.delta_bytes,
            projection.delta_mutations,
            projection.suppressed_versions,
            target.planner,
        )
        else {
            return Ok(no_action(
                self,
                Some(projection_id),
                AdaptiveColumnarCompactionNoActionReason::ProjectionEvidenceUnavailable,
            ));
        };
        if !compaction_policy.admits(delta_segment_count, delta_bytes) {
            return Ok(no_action(
                self,
                Some(projection_id),
                AdaptiveColumnarCompactionNoActionReason::BelowAutomaticCompactionThreshold,
            ));
        }
        Ok(AdaptiveColumnarCompactionDecision::Proposal(
            AdaptiveColumnarCompactionProposal {
                based_on: self.observation.anchor,
                table_id: self.observation.table_id,
                storage_id: source.storage_id,
                storage_kind: source.storage_kind,
                expected_source_snapshot: source.snapshot_token,
                expected_source_data_version: source.data_version,
                projection_id,
                expected_projection_generation: generation,
                expected_projection_frontier: frontier,
                expected_source_current_frontier: source_frontier,
                expected_delta_segment_count: delta_segment_count,
                expected_delta_bytes: delta_bytes,
                expected_delta_mutations: delta_mutations,
                expected_suppressed_versions: suppressed_versions,
                bound: self.maintenance_candidate.bound,
                estimated_cost: self.maintenance_candidate.estimate,
                expected_planner: planner,
                compaction_policy,
                adaptive_policy,
            },
        ))
    }
}

impl Database {
    /// Captures Phase 1 observation plus exact production compaction
    /// eligibility for every projection on one table.
    pub fn observe_adaptive_columnar_compactions(
        &self,
        table_id: TableId,
        budget: MaintenanceBudget,
    ) -> Result<Vec<AdaptiveColumnarCompactionObservation>, AdaptiveColumnarCompactionError> {
        let observation = self.observe_adaptive_columnar(table_id)?;
        Ok(self
            .inspect_columnar_compaction_candidates(table_id, budget)
            .into_iter()
            .map(
                |maintenance_candidate| AdaptiveColumnarCompactionObservation {
                    observation: observation.clone(),
                    maintenance_candidate,
                },
            )
            .collect())
    }

    pub fn advise_adaptive_columnar_compaction(
        &self,
        observation: &AdaptiveColumnarCompactionObservation,
        compaction_policy: AdaptiveColumnarCompactionPolicy,
        adaptive_policy: AdaptivePolicy,
    ) -> Result<AdaptiveColumnarCompactionDecision, AdaptiveColumnarCompactionError> {
        observation.decide(compaction_policy, adaptive_policy)
    }

    /// Revalidates the exact source/projection/candidate state, then invokes
    /// the sole production Columnar compaction writer and measures the result.
    pub fn execute_adaptive_columnar_compaction(
        &mut self,
        proposal: &AdaptiveColumnarCompactionProposal,
        budget: MaintenanceBudget,
    ) -> Result<AdaptiveColumnarCompactionExecutionReport, AdaptiveColumnarCompactionError> {
        if !budget.admits(proposal.estimated_cost) {
            return Ok(aborted_report(
                proposal,
                budget,
                AdaptiveColumnarCompactionAbortReason::BudgetInsufficient,
            ));
        }
        if self.schema_generation() != proposal.based_on.schema_generation {
            return Ok(aborted_report(
                proposal,
                budget,
                AdaptiveColumnarCompactionAbortReason::SchemaChanged,
            ));
        }
        let current = self
            .observe_adaptive_columnar_compactions(proposal.table_id, budget)?
            .into_iter()
            .find(|observation| {
                observation.maintenance_candidate.action
                    == (MaintenanceAction::CompactColumnar {
                        projection_id: proposal.projection_id,
                    })
            });
        let Some(current) = current else {
            return Ok(aborted_report(
                proposal,
                budget,
                AdaptiveColumnarCompactionAbortReason::PreconditionsChanged,
            ));
        };
        let current_proposal =
            match current.decide(proposal.compaction_policy, proposal.adaptive_policy)? {
                AdaptiveColumnarCompactionDecision::Proposal(current) => current,
                AdaptiveColumnarCompactionDecision::NoAction(_) => {
                    return Ok(aborted_report(
                        proposal,
                        budget,
                        AdaptiveColumnarCompactionAbortReason::PreconditionsChanged,
                    ));
                }
            };
        if !same_preconditions(proposal, &current_proposal) {
            return Ok(aborted_report(
                proposal,
                budget,
                AdaptiveColumnarCompactionAbortReason::PreconditionsChanged,
            ));
        }
        let before = state_for(&current.observation, proposal.projection_id).ok_or(
            DatabaseError::ColumnarProjectionNotFound(proposal.projection_id),
        )?;
        let source_before = current.observation.source.as_ref().ok_or(
            DatabaseError::ColumnarProjectionRebuildRequired(proposal.projection_id),
        )?;
        let source_snapshot_before = source_before.snapshot_token;
        let global_commit_seq_before = current.observation.anchor.global_commit_seq;
        let schema_generation_before = current.observation.anchor.schema_generation;
        let (physical, consumed) = self
            .execute_columnar_compaction(proposal.projection_id, current_proposal.estimated_cost)?;
        let after_observation = self.observe_adaptive_columnar(proposal.table_id)?;
        let mut after_change = state_for(&after_observation, proposal.projection_id).ok_or(
            DatabaseError::ColumnarProjectionNotFound(proposal.projection_id),
        )?;
        let source_after = after_observation.source.as_ref().ok_or(
            DatabaseError::ColumnarProjectionRebuildRequired(proposal.projection_id),
        )?;
        let source_snapshot_after = source_after.snapshot_token;
        let source_snapshot_unchanged = source_snapshot_before == source_snapshot_after;
        let source_data_version_unchanged =
            before.source_data_version == after_change.source_data_version;
        let projection_frontier_preserved = before.projection_frontier
            == physical.compacted_frontier
            && physical.compacted_frontier == after_change.projection_frontier;
        let physical_postconditions_hold = physical.old_generation == before.projection_generation
            && physical.new_generation > physical.old_generation
            && physical.new_generation == after_change.projection_generation
            && projection_frontier_preserved
            && source_snapshot_unchanged
            && source_data_version_unchanged
            && global_commit_seq_before == after_observation.anchor.global_commit_seq
            && schema_generation_before == after_observation.anchor.schema_generation;
        let outcome = if !physical.compacted {
            AdaptiveColumnarCompactionOutcome::InconclusiveNoWork
        } else if !physical_postconditions_hold {
            self.adaptive_runtime
                .suppress(proposal.projection_id, after_change.projection_generation);
            AdaptiveColumnarCompactionOutcome::InconclusivePostconditionsChanged
        } else if !budget.contains(consumed) {
            self.adaptive_runtime
                .suppress(proposal.projection_id, after_change.projection_generation);
            AdaptiveColumnarCompactionOutcome::InconclusiveCostBoundExceeded
        } else if after_change.planner.benefit_work_units
            < proposal.adaptive_policy.minimum_keep_benefit_work_units
        {
            self.adaptive_runtime
                .suppress(proposal.projection_id, after_change.projection_generation);
            AdaptiveColumnarCompactionOutcome::RevertedInsufficientMeasuredBenefit
        } else {
            self.adaptive_runtime
                .keep(proposal.projection_id, after_change.projection_generation);
            AdaptiveColumnarCompactionOutcome::Completed
        };
        let mut after_outcome = after_change;
        after_outcome.planner.planner_eligible = !self
            .adaptive_runtime
            .is_suppressed(proposal.projection_id, after_change.projection_generation)
            && after_change.planner.planner_eligible;
        after_change.planner.planner_eligible = after_observation
            .projections
            .iter()
            .find(|entry| entry.projection.projection_id == Some(proposal.projection_id))
            .and_then(|entry| entry.planner)
            .is_some_and(|planner| planner.planner_eligible);
        let measurement = AdaptiveColumnarCompactionMeasurement {
            before,
            after_change,
            after_outcome,
            observed_global_commit_seq: proposal.based_on.global_commit_seq,
            global_commit_seq_before,
            global_commit_seq_after: after_observation.anchor.global_commit_seq,
            schema_generation_before,
            schema_generation_after: after_observation.anchor.schema_generation,
            source_snapshot_before,
            source_snapshot_after,
            source_snapshot_unchanged,
            source_data_version_unchanged,
            projection_frontier_preserved,
            estimated_cost: current_proposal.estimated_cost,
            consumed_cost: consumed,
        };
        Ok(AdaptiveColumnarCompactionExecutionReport {
            proposal: proposal.clone(),
            budget_before: budget,
            consumed,
            budget_remaining: budget.remaining(consumed),
            physical: Some(physical),
            measurement: Some(measurement),
            outcome,
        })
    }
}

fn no_action(
    observation: &AdaptiveColumnarCompactionObservation,
    projection_id: Option<ColumnarProjectionId>,
    reason: AdaptiveColumnarCompactionNoActionReason,
) -> AdaptiveColumnarCompactionDecision {
    AdaptiveColumnarCompactionDecision::NoAction(AdaptiveColumnarCompactionNoAction {
        based_on: observation.observation.anchor,
        table_id: observation.observation.table_id,
        projection_id,
        reason,
    })
}

fn same_preconditions(
    expected: &AdaptiveColumnarCompactionProposal,
    current: &AdaptiveColumnarCompactionProposal,
) -> bool {
    expected.based_on.schema_generation == current.based_on.schema_generation
        && expected.table_id == current.table_id
        && expected.storage_id == current.storage_id
        && expected.storage_kind == current.storage_kind
        && expected.expected_source_snapshot == current.expected_source_snapshot
        && expected.expected_source_data_version == current.expected_source_data_version
        && expected.projection_id == current.projection_id
        && expected.expected_projection_generation == current.expected_projection_generation
        && expected.expected_projection_frontier == current.expected_projection_frontier
        && expected.expected_source_current_frontier == current.expected_source_current_frontier
        && expected.expected_delta_segment_count == current.expected_delta_segment_count
        && expected.expected_delta_bytes == current.expected_delta_bytes
        && expected.expected_delta_mutations == current.expected_delta_mutations
        && expected.expected_suppressed_versions == current.expected_suppressed_versions
        && expected.bound == current.bound
        && expected.estimated_cost == current.estimated_cost
        && expected.expected_planner == current.expected_planner
        && expected.compaction_policy == current.compaction_policy
        && expected.adaptive_policy == current.adaptive_policy
}

fn state_for(
    observation: &AdaptiveObservation,
    projection_id: ColumnarProjectionId,
) -> Option<AdaptiveColumnarState> {
    let source = observation.source.as_ref()?;
    let projection = observation
        .projections
        .iter()
        .find(|entry| entry.projection.projection_id == Some(projection_id))?;
    Some(AdaptiveColumnarState {
        projection_generation: projection.projection.generation?,
        projection_frontier: projection.projection.applied_frontier?,
        source_data_version: source.data_version,
        lag: projection.projection.lag?,
        planner: projection.planner?,
    })
}

fn aborted_report(
    proposal: &AdaptiveColumnarCompactionProposal,
    budget: MaintenanceBudget,
    reason: AdaptiveColumnarCompactionAbortReason,
) -> AdaptiveColumnarCompactionExecutionReport {
    AdaptiveColumnarCompactionExecutionReport {
        proposal: proposal.clone(),
        budget_before: budget,
        consumed: MaintenanceConsumption::default(),
        budget_remaining: budget,
        physical: None,
        measurement: None,
        outcome: AdaptiveColumnarCompactionOutcome::Aborted(reason),
    }
}
