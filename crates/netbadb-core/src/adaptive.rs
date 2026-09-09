use std::error::Error;
use std::fmt;
use std::rc::Rc;

use netbadb_planner::{AccessPath, evaluate_columnar_projection_cost};
use netbadb_storage::{
    ChangeBatchMaintenanceInspection, ChangeStreamInspection, ChangeStreamStatus, StorageKind,
    StorageSnapshotToken,
};
use netbadb_types::{
    ChangeStreamGeneration, ColumnarGeneration, ColumnarProjectionId, DatabaseCommitSeq,
    SchemaGeneration, StorageDataVersion, StorageId, TableId,
};

use crate::registry::TablePlacement;
use crate::{
    ColumnarAdvanceBudget, ColumnarProjectionHealth, ColumnarProjectionInspection, Database,
    DatabaseError, MaintenanceBudget, MaintenanceConsumption, MaintenanceEstimate,
    StorageRegistryError, TableStatistics,
};

/// Logical state against which one immutable adaptive observation was taken.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdaptiveObservationAnchor {
    pub global_commit_seq: DatabaseCommitSeq,
    pub schema_generation: SchemaGeneration,
}

/// Immutable authoritative-source evidence. No storage or transaction handle
/// is retained by an observation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdaptiveSourceObservation {
    pub table_id: TableId,
    pub storage_id: StorageId,
    pub storage_kind: StorageKind,
    pub snapshot_token: StorageSnapshotToken,
    pub data_version: StorageDataVersion,
    pub statistics: Option<TableStatistics>,
    pub access_paths: Vec<AccessPath>,
    pub change_stream: ChangeStreamInspection,
    pub change_batches: Vec<ChangeBatchMaintenanceInspection>,
}

/// Work evidence produced by the planner's canonical columnar evaluator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdaptivePlannerEvidence {
    pub source_work_units: u64,
    pub projection_work_units: u64,
    pub benefit_work_units: u64,
    pub planner_eligible: bool,
}

/// Immutable evidence for one existing derived projection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdaptiveColumnarObservation {
    pub projection: ColumnarProjectionInspection,
    pub planner: Option<AdaptivePlannerEvidence>,
    pub suppressed_after_revert: bool,
}

/// One self-contained immutable Observe result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdaptiveObservation {
    pub anchor: AdaptiveObservationAnchor,
    pub table_id: TableId,
    pub source: Option<AdaptiveSourceObservation>,
    pub projections: Vec<AdaptiveColumnarObservation>,
    pub active_transaction_handles: u64,
    pub structural_mutation_active: bool,
    pub group_commit_active: bool,
}

/// The two thresholds are deliberately the complete Phase 1 policy surface.
/// They are planner work units, not elapsed time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdaptivePolicy {
    pub minimum_expected_benefit_work_units: u64,
    pub minimum_keep_benefit_work_units: u64,
}

impl AdaptivePolicy {
    #[must_use]
    pub const fn new(
        minimum_expected_benefit_work_units: u64,
        minimum_keep_benefit_work_units: u64,
    ) -> Self {
        Self {
            minimum_expected_benefit_work_units,
            minimum_keep_benefit_work_units,
        }
    }
}

impl Default for AdaptivePolicy {
    fn default() -> Self {
        Self::new(1, 1)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdaptiveNoActionReason {
    AlreadyFresh,
    NoColumnarProjection,
    ChangeStreamUnavailable,
    MaintenanceBusy,
    StructuralMutationActive,
    UnsupportedSource,
    UnsupportedProjectionMode,
    ProjectionUnavailable,
    RebuildRequired,
    InsufficientExpectedBenefit,
    InsufficientBudgetEstimate,
    SuppressedAfterRevert,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdaptiveNoAction {
    pub based_on: AdaptiveObservationAnchor,
    pub table_id: TableId,
    pub projection_id: Option<ColumnarProjectionId>,
    pub reason: AdaptiveNoActionReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdaptiveColumnarAction {
    CatchUpExistingColumnar {
        max_batches: u64,
        max_change_bytes: u64,
    },
}

/// A proposal is evidence-bound intent, never execution authority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdaptiveMaintenanceProposal {
    pub based_on: AdaptiveObservationAnchor,
    pub table_id: TableId,
    pub storage_id: StorageId,
    pub storage_kind: StorageKind,
    pub expected_source_snapshot: StorageSnapshotToken,
    pub expected_source_data_version: StorageDataVersion,
    pub projection_id: ColumnarProjectionId,
    pub expected_projection_generation: ColumnarGeneration,
    pub expected_projection_frontier: StorageDataVersion,
    pub expected_change_stream_generation: ChangeStreamGeneration,
    pub expected_change_stream_frontier: StorageDataVersion,
    pub expected_change_stream_earliest: StorageDataVersion,
    pub action: AdaptiveColumnarAction,
    pub estimated_cost: MaintenanceEstimate,
    pub expected_planner: AdaptivePlannerEvidence,
    pub policy: AdaptivePolicy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdaptiveDecision {
    NoAction(AdaptiveNoAction),
    Proposal(AdaptiveMaintenanceProposal),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdaptiveAbortReason {
    StaleObservation,
    PreconditionsChanged,
    BudgetInsufficient,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdaptiveMaintenanceOutcome {
    Kept,
    RevertedInsufficientMeasuredBenefit,
    Aborted(AdaptiveAbortReason),
    InconclusiveDidNotCatchUp,
    InconclusiveCostBoundExceeded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdaptiveColumnarState {
    pub projection_generation: ColumnarGeneration,
    pub projection_frontier: StorageDataVersion,
    pub source_data_version: StorageDataVersion,
    pub lag: u64,
    pub planner: AdaptivePlannerEvidence,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdaptiveColumnarMeasurement {
    pub before: AdaptiveColumnarState,
    pub after_change: AdaptiveColumnarState,
    pub after_outcome: AdaptiveColumnarState,
    pub global_commit_seq_before: DatabaseCommitSeq,
    pub global_commit_seq_after: DatabaseCommitSeq,
    pub schema_generation_before: SchemaGeneration,
    pub schema_generation_after: SchemaGeneration,
    pub source_snapshot_unchanged: bool,
    pub estimated_cost: MaintenanceEstimate,
    pub consumed_cost: MaintenanceConsumption,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdaptiveExecutionReport {
    pub proposal: AdaptiveMaintenanceProposal,
    pub budget_before: MaintenanceBudget,
    pub consumed: MaintenanceConsumption,
    pub budget_remaining: MaintenanceBudget,
    pub measurement: Option<AdaptiveColumnarMeasurement>,
    pub outcome: AdaptiveMaintenanceOutcome,
}

/// Complete runtime-only causal trace for one explicit closed-loop step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdaptiveCycleReport {
    pub observation: AdaptiveObservation,
    pub decision: AdaptiveDecision,
    pub execution: Option<AdaptiveExecutionReport>,
}

#[derive(Debug)]
pub enum AdaptiveError {
    GlobalVisibilityRequired,
    Database(DatabaseError),
}

impl fmt::Display for AdaptiveError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::GlobalVisibilityRequired => formatter
                .write_str("adaptive operations require a database-global visibility anchor"),
            Self::Database(error) => error.fmt(formatter),
        }
    }
}

impl Error for AdaptiveError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::GlobalVisibilityRequired => None,
            Self::Database(error) => Some(error),
        }
    }
}

impl From<DatabaseError> for AdaptiveError {
    fn from(error: DatabaseError) -> Self {
        Self::Database(error)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SuppressedProjection {
    projection_id: ColumnarProjectionId,
    generation: ColumnarGeneration,
}

#[derive(Debug, Default)]
pub(crate) struct AdaptiveRuntimeState {
    suppressed: Vec<SuppressedProjection>,
}

impl AdaptiveRuntimeState {
    pub(crate) fn is_suppressed(
        &self,
        projection_id: ColumnarProjectionId,
        generation: ColumnarGeneration,
    ) -> bool {
        self.suppressed
            .iter()
            .any(|entry| entry.projection_id == projection_id && entry.generation == generation)
    }

    pub(crate) fn suppress(
        &mut self,
        projection_id: ColumnarProjectionId,
        generation: ColumnarGeneration,
    ) {
        if !self.is_suppressed(projection_id, generation) {
            self.suppressed.push(SuppressedProjection {
                projection_id,
                generation,
            });
            self.suppressed
                .sort_unstable_by_key(|entry| (entry.projection_id.0, entry.generation.0));
        }
    }

    pub(crate) fn keep(
        &mut self,
        projection_id: ColumnarProjectionId,
        generation: ColumnarGeneration,
    ) {
        self.suppressed
            .retain(|entry| entry.projection_id != projection_id || entry.generation != generation);
    }
}

impl AdaptiveObservation {
    /// Pure read-only advisor. It consults only this immutable snapshot.
    #[must_use]
    pub fn decide(&self, policy: AdaptivePolicy, budget: MaintenanceBudget) -> AdaptiveDecision {
        let decisions = self.decisions(policy, budget);
        decisions
            .iter()
            .find(|decision| matches!(decision, AdaptiveDecision::Proposal(_)))
            .cloned()
            .or_else(|| decisions.into_iter().next())
            .unwrap_or_else(|| {
                adaptive_no_action(self, None, AdaptiveNoActionReason::NoColumnarProjection)
            })
    }

    /// Returns one pure decision per existing projection. Table-wide blockers
    /// remain one typed decision. Phase 1 keeps its first-proposal behavior;
    /// Phase 6 uses the complete list for read-only discovery.
    pub(crate) fn decisions(
        &self,
        policy: AdaptivePolicy,
        budget: MaintenanceBudget,
    ) -> Vec<AdaptiveDecision> {
        if self.structural_mutation_active {
            return vec![adaptive_no_action(
                self,
                None,
                AdaptiveNoActionReason::StructuralMutationActive,
            )];
        }
        if self.active_transaction_handles != 0 || self.group_commit_active {
            return vec![adaptive_no_action(
                self,
                None,
                AdaptiveNoActionReason::MaintenanceBusy,
            )];
        }
        let Some(source) = &self.source else {
            return vec![adaptive_no_action(
                self,
                None,
                AdaptiveNoActionReason::UnsupportedSource,
            )];
        };
        if self.projections.is_empty() {
            return vec![adaptive_no_action(
                self,
                None,
                AdaptiveNoActionReason::NoColumnarProjection,
            )];
        }
        self.projections
            .iter()
            .map(|target| decide_projection(self, source, target, policy, budget))
            .collect()
    }
}

fn adaptive_no_action(
    observation: &AdaptiveObservation,
    projection_id: Option<ColumnarProjectionId>,
    reason: AdaptiveNoActionReason,
) -> AdaptiveDecision {
    AdaptiveDecision::NoAction(AdaptiveNoAction {
        based_on: observation.anchor,
        table_id: observation.table_id,
        projection_id,
        reason,
    })
}

fn decide_projection(
    observation: &AdaptiveObservation,
    source: &AdaptiveSourceObservation,
    target: &AdaptiveColumnarObservation,
    policy: AdaptivePolicy,
    budget: MaintenanceBudget,
) -> AdaptiveDecision {
    let no_action = |projection_id, reason| adaptive_no_action(observation, projection_id, reason);
    let projection_id = target.projection.projection_id;
    if target.suppressed_after_revert {
        return no_action(projection_id, AdaptiveNoActionReason::SuppressedAfterRevert);
    }
    if target.projection.health == ColumnarProjectionHealth::Fresh {
        return no_action(projection_id, AdaptiveNoActionReason::AlreadyFresh);
    }
    if target.projection.health == ColumnarProjectionHealth::Unavailable {
        return no_action(projection_id, AdaptiveNoActionReason::ProjectionUnavailable);
    }
    if target.projection.health == ColumnarProjectionHealth::RebuildRequired {
        return no_action(projection_id, AdaptiveNoActionReason::RebuildRequired);
    }
    if target.projection.mode != Some("incremental") {
        return no_action(
            projection_id,
            AdaptiveNoActionReason::UnsupportedProjectionMode,
        );
    }
    if source.change_stream.status != ChangeStreamStatus::Enabled {
        return no_action(
            projection_id,
            AdaptiveNoActionReason::ChangeStreamUnavailable,
        );
    }
    let Some(id) = projection_id else {
        return no_action(None, AdaptiveNoActionReason::ProjectionUnavailable);
    };
    let (Some(generation), Some(applied), Some(stream_generation), Some(earliest)) = (
        target.projection.generation,
        target.projection.applied_frontier,
        target.projection.stream_generation,
        source.change_stream.earliest_available_frontier,
    ) else {
        return no_action(Some(id), AdaptiveNoActionReason::ChangeStreamUnavailable);
    };
    if source.change_stream.generation != Some(stream_generation)
        || applied.0 < earliest.0
        || applied.0 >= source.data_version.0
    {
        return no_action(Some(id), AdaptiveNoActionReason::ChangeStreamUnavailable);
    }
    let Some(planner) = target.planner else {
        return no_action(Some(id), AdaptiveNoActionReason::ProjectionUnavailable);
    };
    if planner.benefit_work_units < policy.minimum_expected_benefit_work_units {
        return no_action(
            Some(id),
            AdaptiveNoActionReason::InsufficientExpectedBenefit,
        );
    }
    let Some((max_batches, max_change_bytes, mutations)) =
        complete_change_range(applied, source.data_version, &source.change_batches)
    else {
        return no_action(Some(id), AdaptiveNoActionReason::ChangeStreamUnavailable);
    };
    let Some(write_bytes) = conservative_columnar_write_bound(
        max_change_bytes,
        max_batches,
        mutations,
        target.projection.columns.len(),
    ) else {
        return no_action(Some(id), AdaptiveNoActionReason::InsufficientBudgetEstimate);
    };
    let estimated_cost = MaintenanceEstimate {
        work_units: max_batches,
        read_bytes: max_change_bytes,
        write_bytes,
    };
    if !fits_budget(estimated_cost, budget) {
        return no_action(Some(id), AdaptiveNoActionReason::InsufficientBudgetEstimate);
    }
    AdaptiveDecision::Proposal(AdaptiveMaintenanceProposal {
        based_on: observation.anchor,
        table_id: observation.table_id,
        storage_id: source.storage_id,
        storage_kind: source.storage_kind,
        expected_source_snapshot: source.snapshot_token,
        expected_source_data_version: source.data_version,
        projection_id: id,
        expected_projection_generation: generation,
        expected_projection_frontier: applied,
        expected_change_stream_generation: stream_generation,
        expected_change_stream_frontier: source.change_stream.current_data_version,
        expected_change_stream_earliest: earliest,
        action: AdaptiveColumnarAction::CatchUpExistingColumnar {
            max_batches,
            max_change_bytes,
        },
        estimated_cost,
        expected_planner: planner,
        policy,
    })
}

impl Database {
    /// Captures a detached, immutable evidence snapshot for one table.
    pub fn observe_adaptive_columnar(
        &self,
        table_id: TableId,
    ) -> Result<AdaptiveObservation, AdaptiveError> {
        let snapshot = self
            .current_database_snapshot()?
            .ok_or(AdaptiveError::GlobalVisibilityRequired)?;
        let anchor = AdaptiveObservationAnchor {
            global_commit_seq: snapshot.commit_seq(),
            schema_generation: self.schema_generation(),
        };
        let planner_access_paths = self.planner_access_paths();
        let source = match self
            .bindings
            .placement(table_id)
            .map_err(DatabaseError::from)?
        {
            TablePlacement::Single { storage_id, .. } => {
                let storage = self
                    .registry
                    .get(*storage_id)
                    .ok_or(StorageRegistryError::UnknownStorageId {
                        storage_id: *storage_id,
                    })
                    .map_err(DatabaseError::from)?;
                let maintenance = storage.inspect_change_stream_maintenance();
                Some(AdaptiveSourceObservation {
                    table_id,
                    storage_id: *storage_id,
                    storage_kind: storage.kind(),
                    snapshot_token: storage
                        .current_snapshot_token()
                        .map_err(DatabaseError::from)?,
                    data_version: maintenance.stream.current_data_version,
                    statistics: storage.table_statistics(),
                    access_paths: planner_access_paths
                        .iter()
                        .filter(|path| path.table_id == table_id)
                        .cloned()
                        .collect(),
                    change_stream: maintenance.stream,
                    change_batches: maintenance.batches,
                })
            }
            TablePlacement::RangePartitioned { .. } => None,
        };
        let eligible = self.planner_columnar_projections();
        let projections = self
            .inspect_columnar_projections()
            .into_iter()
            .filter(|projection| projection.table_id == table_id)
            .map(|projection| {
                let raw = projection.projection_id.and_then(|id| {
                    self.projections
                        .iter()
                        .find(|entry| entry.identity.id == id)
                        .and_then(|entry| entry.projection.as_ref())
                        .map(crate::columnar_planning_snapshot)
                });
                let planner_eligible = projection.projection_id.is_some_and(|id| {
                    eligible
                        .iter()
                        .any(|candidate| candidate.projection_id == id)
                });
                let planner = raw.as_ref().map(|snapshot| {
                    let cost = evaluate_columnar_projection_cost(
                        source.as_ref().and_then(|source| source.statistics),
                        snapshot,
                    );
                    AdaptivePlannerEvidence {
                        source_work_units: cost.source_work_units,
                        projection_work_units: cost.projection_work_units,
                        benefit_work_units: cost.benefit_work_units(),
                        planner_eligible,
                    }
                });
                let suppressed_after_revert = projection
                    .projection_id
                    .zip(projection.generation)
                    .is_some_and(|(id, generation)| {
                        self.adaptive_runtime.is_suppressed(id, generation)
                    });
                AdaptiveColumnarObservation {
                    projection,
                    planner,
                    suppressed_after_revert,
                }
            })
            .collect();
        let active_transaction_handles =
            u64::try_from(Rc::strong_count(&self.transaction_owner).saturating_sub(1))
                .unwrap_or(u64::MAX);
        Ok(AdaptiveObservation {
            anchor,
            table_id,
            source,
            projections,
            active_transaction_handles,
            structural_mutation_active: self.schema_writer.get().is_some(),
            group_commit_active: self.inspect_group_commit().is_some(),
        })
    }

    /// Applies the pure advisor to an already-captured observation.
    #[must_use]
    pub fn advise_adaptive_columnar(
        &self,
        observation: &AdaptiveObservation,
        policy: AdaptivePolicy,
        budget: MaintenanceBudget,
    ) -> AdaptiveDecision {
        observation.decide(policy, budget)
    }

    /// Revalidates an evidence-bound proposal, executes the existing production
    /// Columnar advance, measures it, and explicitly keeps or suppresses it.
    pub fn execute_adaptive_columnar(
        &mut self,
        proposal: &AdaptiveMaintenanceProposal,
        budget: MaintenanceBudget,
    ) -> Result<AdaptiveExecutionReport, AdaptiveError> {
        if !fits_budget(proposal.estimated_cost, budget) {
            return Ok(aborted_report(
                proposal,
                budget,
                AdaptiveAbortReason::BudgetInsufficient,
            ));
        }
        let current = self.observe_adaptive_columnar(proposal.table_id)?;
        if current.anchor != proposal.based_on {
            return Ok(aborted_report(
                proposal,
                budget,
                AdaptiveAbortReason::StaleObservation,
            ));
        }
        if !current
            .decisions(proposal.policy, budget)
            .contains(&AdaptiveDecision::Proposal(proposal.clone()))
        {
            return Ok(aborted_report(
                proposal,
                budget,
                AdaptiveAbortReason::PreconditionsChanged,
            ));
        }
        let before = state_for(&current, proposal.projection_id).ok_or(
            DatabaseError::ColumnarProjectionNotFound(proposal.projection_id),
        )?;
        let AdaptiveColumnarAction::CatchUpExistingColumnar {
            max_batches,
            max_change_bytes,
        } = proposal.action;
        let max_batches = usize::try_from(max_batches).map_err(|_| {
            DatabaseError::ColumnarProjectionRebuildRequired(proposal.projection_id)
        })?;
        let advance = self.advance_columnar_projection(
            proposal.projection_id,
            ColumnarAdvanceBudget::new(max_batches, max_change_bytes),
        )?;
        let consumed = MaintenanceConsumption {
            work_units: advance.batches_applied,
            read_bytes: proposal.estimated_cost.read_bytes,
            write_bytes: advance.bytes_written,
            actions: 1,
        };
        let after_observation = self.observe_adaptive_columnar(proposal.table_id)?;
        let mut after_change = state_for(&after_observation, proposal.projection_id).ok_or(
            DatabaseError::ColumnarProjectionNotFound(proposal.projection_id),
        )?;
        let cost_bound_exceeded = !consumption_fits(consumed, budget);
        let outcome = if cost_bound_exceeded {
            self.adaptive_runtime
                .suppress(proposal.projection_id, after_change.projection_generation);
            AdaptiveMaintenanceOutcome::InconclusiveCostBoundExceeded
        } else if !advance.caught_up {
            AdaptiveMaintenanceOutcome::InconclusiveDidNotCatchUp
        } else if after_change.planner.benefit_work_units
            < proposal.policy.minimum_keep_benefit_work_units
        {
            self.adaptive_runtime
                .suppress(proposal.projection_id, after_change.projection_generation);
            AdaptiveMaintenanceOutcome::RevertedInsufficientMeasuredBenefit
        } else {
            self.adaptive_runtime
                .keep(proposal.projection_id, after_change.projection_generation);
            AdaptiveMaintenanceOutcome::Kept
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
        let source_after = after_observation.source.as_ref().ok_or(
            DatabaseError::ColumnarProjectionRebuildRequired(proposal.projection_id),
        )?;
        let measurement = AdaptiveColumnarMeasurement {
            before,
            after_change,
            after_outcome,
            global_commit_seq_before: proposal.based_on.global_commit_seq,
            global_commit_seq_after: after_observation.anchor.global_commit_seq,
            schema_generation_before: proposal.based_on.schema_generation,
            schema_generation_after: after_observation.anchor.schema_generation,
            source_snapshot_unchanged: source_after.snapshot_token
                == proposal.expected_source_snapshot,
            estimated_cost: proposal.estimated_cost,
            consumed_cost: consumed,
        };
        Ok(AdaptiveExecutionReport {
            proposal: proposal.clone(),
            budget_before: budget,
            consumed,
            budget_remaining: remaining_budget(budget, consumed),
            measurement: Some(measurement),
            outcome,
        })
    }

    /// Runs one explicit synchronous Observe→Decide→Change→Measure→Outcome loop.
    pub fn adaptive_columnar_step(
        &mut self,
        table_id: TableId,
        policy: AdaptivePolicy,
        budget: MaintenanceBudget,
    ) -> Result<AdaptiveCycleReport, AdaptiveError> {
        let observation = self.observe_adaptive_columnar(table_id)?;
        let decision = observation.decide(policy, budget);
        let execution = match &decision {
            AdaptiveDecision::NoAction(_) => None,
            AdaptiveDecision::Proposal(proposal) => {
                Some(self.execute_adaptive_columnar(proposal, budget)?)
            }
        };
        Ok(AdaptiveCycleReport {
            observation,
            decision,
            execution,
        })
    }
}

fn complete_change_range(
    applied: StorageDataVersion,
    current: StorageDataVersion,
    batches: &[ChangeBatchMaintenanceInspection],
) -> Option<(u64, u64, u64)> {
    let start = batches.iter().position(|batch| batch.before == applied)?;
    let mut frontier = applied;
    let mut batch_count = 0_u64;
    let mut change_bytes = 0_u64;
    let mut mutations = 0_u64;
    for batch in &batches[start..] {
        if batch.before != frontier || batch.after.0 <= batch.before.0 {
            return None;
        }
        batch_count = batch_count.checked_add(1)?;
        change_bytes = change_bytes.checked_add(batch.change_bytes)?;
        mutations = mutations.checked_add(batch.mutation_count)?;
        frontier = batch.after;
        if frontier == current {
            return Some((batch_count, change_bytes, mutations));
        }
        if frontier.0 > current.0 {
            return None;
        }
    }
    None
}

/// Conservative admission bound. Change payload contains every projected
/// after-image byte. The multiplier covers Base-compatible payload, min/max
/// zone-map copies and descriptor metadata; the remaining terms cover bounded
/// per-column, per-mutation, per-batch and file metadata. Checked arithmetic
/// rejects an unrepresentable estimate rather than silently saturating.
fn conservative_columnar_write_bound(
    change_bytes: u64,
    batches: u64,
    mutations: u64,
    columns: usize,
) -> Option<u64> {
    let columns = u64::try_from(columns).ok()?;
    change_bytes
        .checked_mul(3)?
        .checked_add(4096)?
        .checked_add(
            columns
                .checked_mul(mutations.checked_add(1)?)?
                .checked_mul(256)?,
        )?
        .checked_add(mutations.checked_mul(128)?)?
        .checked_add(batches.checked_mul(64)?)
}

fn fits_budget(estimate: MaintenanceEstimate, budget: MaintenanceBudget) -> bool {
    budget.max_actions != 0
        && estimate.work_units <= budget.max_work_units
        && estimate.read_bytes <= budget.max_read_bytes
        && estimate.write_bytes <= budget.max_write_bytes
}

fn consumption_fits(consumed: MaintenanceConsumption, budget: MaintenanceBudget) -> bool {
    consumed.actions <= budget.max_actions
        && consumed.work_units <= budget.max_work_units
        && consumed.read_bytes <= budget.max_read_bytes
        && consumed.write_bytes <= budget.max_write_bytes
}

fn remaining_budget(
    budget: MaintenanceBudget,
    consumed: MaintenanceConsumption,
) -> MaintenanceBudget {
    MaintenanceBudget::new(
        budget.max_work_units.saturating_sub(consumed.work_units),
        budget.max_read_bytes.saturating_sub(consumed.read_bytes),
        budget.max_write_bytes.saturating_sub(consumed.write_bytes),
        budget.max_actions.saturating_sub(consumed.actions),
    )
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
    proposal: &AdaptiveMaintenanceProposal,
    budget: MaintenanceBudget,
    reason: AdaptiveAbortReason,
) -> AdaptiveExecutionReport {
    AdaptiveExecutionReport {
        proposal: proposal.clone(),
        budget_before: budget,
        consumed: MaintenanceConsumption::default(),
        budget_remaining: budget,
        measurement: None,
        outcome: AdaptiveMaintenanceOutcome::Aborted(reason),
    }
}
