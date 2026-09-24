//! Pure maintenance admission, candidate ranking, and caller-owned scheduling policy.

use netbadb_change_stream::ChangeStreamInspection;
use netbadb_columnar::StorageSnapshotToken;
use netbadb_lsm::{
    LsmCompactionPlanInspection, LsmInspection, LsmMaintenanceAnchor, LsmMaintenanceInspection,
};
use netbadb_storage_api::StorageKind;
use netbadb_types::{
    ColumnarProjectionId, DatabaseCommitSeq, SchemaGeneration, StorageDataVersion, StorageId,
    TableId,
};
use std::cmp::Ordering;
use std::error::Error;
use std::fmt;

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

    pub const fn checked_remaining(self, consumed: MaintenanceConsumption) -> Option<Self> {
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

    pub fn remaining(self, consumed: MaintenanceConsumption) -> Self {
        Self {
            max_work_units: self.max_work_units.saturating_sub(consumed.work_units),
            max_read_bytes: self.max_read_bytes.saturating_sub(consumed.read_bytes),
            max_write_bytes: self.max_write_bytes.saturating_sub(consumed.write_bytes),
            max_actions: self.max_actions.saturating_sub(consumed.actions),
        }
    }

    pub const fn admits(self, estimate: MaintenanceEstimate) -> bool {
        self.max_actions != 0
            && estimate.work_units <= self.max_work_units
            && estimate.read_bytes <= self.max_read_bytes
            && estimate.write_bytes <= self.max_write_bytes
    }

    pub const fn contains(self, consumed: MaintenanceConsumption) -> bool {
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
    pub fn pending_work(&self) -> bool {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaintenanceCursor {
    pub class: u8,
    pub target: u64,
}

pub fn candidate(
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

pub fn plan_maintenance(
    mut candidates: Vec<MaintenanceCandidate>,
    cursor: Option<MaintenanceCursor>,
) -> MaintenanceInspection {
    candidates.sort_by(compare_candidates);
    let decision = candidates
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
        candidates,
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

pub const fn action_cursor(action: MaintenanceAction) -> MaintenanceCursor {
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

// The evidence token is an observation coordinate, never mutation authority.
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
pub enum AutomaticEvidenceRenewalReason {
    ColumnarPhysicalStateChanged,
    ColumnarEligibilityChanged,
    AuthoritativeLsmLayoutChanged,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AutomaticEvidenceRenewalRecommendation {
    pub reason: AutomaticEvidenceRenewalReason,
}

/// Caller-supplied progress coordinate for cooperative scheduling.
///
/// A scheduler tick is independent of every database, schema, storage,
/// calibration, and evidence generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct AutomaticSchedulerTick(pub u64);

/// Explicit logical-tick cadence for one caller-owned scheduler.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AutomaticSchedulerPolicy {
    minimum_ticks_between_runs: u64,
    idle_retry_ticks: u64,
    no_progress_retry_ticks: u64,
    trial_retry_ticks: u64,
}

impl AutomaticSchedulerPolicy {
    pub fn new(
        minimum_ticks_between_runs: u64,
        idle_retry_ticks: u64,
        no_progress_retry_ticks: u64,
        trial_retry_ticks: u64,
    ) -> Result<Self, AutomaticSchedulerPolicyError> {
        if minimum_ticks_between_runs == 0 {
            return Err(AutomaticSchedulerPolicyError::ZeroMinimumCadence);
        }
        if idle_retry_ticks < minimum_ticks_between_runs {
            return Err(AutomaticSchedulerPolicyError::IdleRetryBelowMinimum {
                minimum: minimum_ticks_between_runs,
                received: idle_retry_ticks,
            });
        }
        if no_progress_retry_ticks < minimum_ticks_between_runs {
            return Err(AutomaticSchedulerPolicyError::NoProgressRetryBelowMinimum {
                minimum: minimum_ticks_between_runs,
                received: no_progress_retry_ticks,
            });
        }
        if trial_retry_ticks < minimum_ticks_between_runs {
            return Err(AutomaticSchedulerPolicyError::TrialRetryBelowMinimum {
                minimum: minimum_ticks_between_runs,
                received: trial_retry_ticks,
            });
        }
        Ok(Self {
            minimum_ticks_between_runs,
            idle_retry_ticks,
            no_progress_retry_ticks,
            trial_retry_ticks,
        })
    }

    #[must_use]
    pub const fn minimum_ticks_between_runs(self) -> u64 {
        self.minimum_ticks_between_runs
    }

    #[must_use]
    pub const fn idle_retry_ticks(self) -> u64 {
        self.idle_retry_ticks
    }

    #[must_use]
    pub const fn no_progress_retry_ticks(self) -> u64 {
        self.no_progress_retry_ticks
    }

    #[must_use]
    pub const fn trial_retry_ticks(self) -> u64 {
        self.trial_retry_ticks
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutomaticSchedulerPolicyError {
    ZeroMinimumCadence,
    IdleRetryBelowMinimum { minimum: u64, received: u64 },
    NoProgressRetryBelowMinimum { minimum: u64, received: u64 },
    TrialRetryBelowMinimum { minimum: u64, received: u64 },
}

impl fmt::Display for AutomaticSchedulerPolicyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroMinimumCadence => {
                formatter.write_str("automatic scheduler minimum cadence must be at least one tick")
            }
            Self::IdleRetryBelowMinimum { minimum, received } => write!(
                formatter,
                "automatic scheduler idle retry {received} is below minimum cadence {minimum}"
            ),
            Self::NoProgressRetryBelowMinimum { minimum, received } => write!(
                formatter,
                "automatic scheduler no-progress retry {received} is below minimum cadence {minimum}"
            ),
            Self::TrialRetryBelowMinimum { minimum, received } => write!(
                formatter,
                "automatic scheduler trial retry {received} is below minimum cadence {minimum}"
            ),
        }
    }
}

impl Error for AutomaticSchedulerPolicyError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutomaticSchedulerDelayClass {
    Normal,
    Idle,
    NoProgress,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutomaticSchedulerFault {
    MaintenanceEnvelopeExceeded,
    StepFailed,
    ConsumptionOverflow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutomaticSchedulerGate {
    Open {
        delay: AutomaticSchedulerDelayClass,
    },
    AwaitingTrialProgress {
        evidence: AdaptiveEvidenceProgressToken,
    },
    AwaitingEvidenceRenewal {
        blocked_window_epoch: AdaptiveEvidenceWindowEpoch,
        recommendation: AutomaticEvidenceRenewalRecommendation,
    },
    Faulted(AutomaticSchedulerFault),
}

/// Fixed-size runtime state. Reports and tick history remain caller-owned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AutomaticSchedulerState {
    pub last_observed_tick: Option<AutomaticSchedulerTick>,
    pub last_run_tick: Option<AutomaticSchedulerTick>,
    pub gate: AutomaticSchedulerGate,
}

impl AutomaticSchedulerState {
    pub const INITIAL: Self = Self {
        last_observed_tick: None,
        last_run_tick: None,
        gate: AutomaticSchedulerGate::Open {
            delay: AutomaticSchedulerDelayClass::Normal,
        },
    };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutomaticSchedulerHoldReason {
    DuplicateTick,
    MinimumCadence {
        last_run_tick: AutomaticSchedulerTick,
        required_ticks: u64,
    },
    IdleBackoff {
        last_run_tick: AutomaticSchedulerTick,
        required_ticks: u64,
    },
    NoProgressBackoff {
        last_run_tick: AutomaticSchedulerTick,
        required_ticks: u64,
    },
    AwaitingTrialEvidenceOrRetry {
        last_run_tick: AutomaticSchedulerTick,
        retry_ticks: u64,
        evidence: AdaptiveEvidenceProgressToken,
    },
    AwaitingEvidenceRenewal {
        blocked_window_epoch: AdaptiveEvidenceWindowEpoch,
        observed_window_epoch: AdaptiveEvidenceWindowEpoch,
        recommendation: AutomaticEvidenceRenewalRecommendation,
    },
    Faulted(AutomaticSchedulerFault),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutomaticSchedulerInspection {
    WouldRunNow,
    Held(AutomaticSchedulerHoldReason),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchedulerTickOrderError {
    pub previous: AutomaticSchedulerTick,
    pub received: AutomaticSchedulerTick,
}

pub fn evaluate_scheduler_gate(
    state: AutomaticSchedulerState,
    policy: AutomaticSchedulerPolicy,
    evidence: AdaptiveEvidenceProgressToken,
    tick: AutomaticSchedulerTick,
) -> Result<AutomaticSchedulerInspection, SchedulerTickOrderError> {
    if let Some(previous) = state.last_observed_tick {
        if tick < previous {
            return Err(SchedulerTickOrderError {
                previous,
                received: tick,
            });
        }
        if tick == previous {
            return Ok(AutomaticSchedulerInspection::Held(
                AutomaticSchedulerHoldReason::DuplicateTick,
            ));
        }
    }

    let Some(last_run_tick) = state.last_run_tick else {
        return Ok(AutomaticSchedulerInspection::WouldRunNow);
    };
    let elapsed = tick.0 - last_run_tick.0;

    match state.gate {
        AutomaticSchedulerGate::Open { delay } => {
            let required_ticks = match delay {
                AutomaticSchedulerDelayClass::Normal => policy.minimum_ticks_between_runs,
                AutomaticSchedulerDelayClass::Idle => policy.idle_retry_ticks,
                AutomaticSchedulerDelayClass::NoProgress => policy.no_progress_retry_ticks,
            };
            if elapsed >= required_ticks {
                Ok(AutomaticSchedulerInspection::WouldRunNow)
            } else {
                let reason = match delay {
                    AutomaticSchedulerDelayClass::Normal => {
                        AutomaticSchedulerHoldReason::MinimumCadence {
                            last_run_tick,
                            required_ticks,
                        }
                    }
                    AutomaticSchedulerDelayClass::Idle => {
                        AutomaticSchedulerHoldReason::IdleBackoff {
                            last_run_tick,
                            required_ticks,
                        }
                    }
                    AutomaticSchedulerDelayClass::NoProgress => {
                        AutomaticSchedulerHoldReason::NoProgressBackoff {
                            last_run_tick,
                            required_ticks,
                        }
                    }
                };
                Ok(AutomaticSchedulerInspection::Held(reason))
            }
        }
        AutomaticSchedulerGate::AwaitingTrialProgress {
            evidence: previous_evidence,
        } => {
            if evidence != previous_evidence && elapsed >= policy.minimum_ticks_between_runs {
                return Ok(AutomaticSchedulerInspection::WouldRunNow);
            }
            if elapsed >= policy.trial_retry_ticks {
                return Ok(AutomaticSchedulerInspection::WouldRunNow);
            }
            if evidence != previous_evidence {
                return Ok(AutomaticSchedulerInspection::Held(
                    AutomaticSchedulerHoldReason::MinimumCadence {
                        last_run_tick,
                        required_ticks: policy.minimum_ticks_between_runs,
                    },
                ));
            }
            Ok(AutomaticSchedulerInspection::Held(
                AutomaticSchedulerHoldReason::AwaitingTrialEvidenceOrRetry {
                    last_run_tick,
                    retry_ticks: policy.trial_retry_ticks,
                    evidence: previous_evidence,
                },
            ))
        }
        AutomaticSchedulerGate::AwaitingEvidenceRenewal {
            blocked_window_epoch,
            recommendation,
        } => {
            if evidence.window_epoch <= blocked_window_epoch {
                return Ok(AutomaticSchedulerInspection::Held(
                    AutomaticSchedulerHoldReason::AwaitingEvidenceRenewal {
                        blocked_window_epoch,
                        observed_window_epoch: evidence.window_epoch,
                        recommendation,
                    },
                ));
            }
            if elapsed >= policy.minimum_ticks_between_runs {
                Ok(AutomaticSchedulerInspection::WouldRunNow)
            } else {
                Ok(AutomaticSchedulerInspection::Held(
                    AutomaticSchedulerHoldReason::MinimumCadence {
                        last_run_tick,
                        required_ticks: policy.minimum_ticks_between_runs,
                    },
                ))
            }
        }
        AutomaticSchedulerGate::Faulted(fault) => Ok(AutomaticSchedulerInspection::Held(
            AutomaticSchedulerHoldReason::Faulted(fault),
        )),
    }
}

// LSM observations are immutable DTOs authored by the storage engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdaptiveLsmMaintenanceAction {
    Flush,
    CompactOne,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AdaptiveLsmFlushPolicy {
    pub minimum_memtable_bytes: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AdaptiveLsmCompactionPolicy {
    pub minimum_input_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdaptiveLsmMaintenanceNoActionReason {
    NoMemtableEntries,
    MemtableBelowAutomaticThreshold,
    MemtableNotEmpty,
    NoCompactionPlan,
    CompactionBelowAutomaticThreshold,
    MaintenanceBlocked(MaintenanceBlocker),
    BudgetBlocked(MaintenanceBlocker),
    AutomaticBoundUnavailable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdaptiveLsmMaintenanceObservation {
    pub observed_global_commit_seq: Option<DatabaseCommitSeq>,
    pub schema_generation: SchemaGeneration,
    pub table_id: TableId,
    pub storage_id: StorageId,
    pub storage_kind: StorageKind,
    pub storage_snapshot: StorageSnapshotToken,
    pub logical_data_version: StorageDataVersion,
    pub lsm: LsmInspection,
    pub maintenance: LsmMaintenanceInspection,
    pub change_stream: ChangeStreamInspection,
    pub production_flush_candidate: Option<MaintenanceCandidate>,
    pub production_compaction_candidate: Option<MaintenanceCandidate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdaptiveLsmFlushProposal {
    pub observed_global_commit_seq: Option<DatabaseCommitSeq>,
    pub based_on_schema_generation: SchemaGeneration,
    pub table_id: TableId,
    pub storage_id: StorageId,
    pub expected_storage_snapshot: StorageSnapshotToken,
    pub expected_logical_data_version: StorageDataVersion,
    pub expected_layout_anchor: LsmMaintenanceAnchor,
    pub expected_memtable_entries: u64,
    pub expected_memtable_bytes: u64,
    pub expected_flush_threshold_bytes: u64,
    pub estimated_cost: MaintenanceEstimate,
    pub conservative_bound: MaintenanceEstimate,
    pub policy: AdaptiveLsmFlushPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdaptiveLsmCompactionProposal {
    pub observed_global_commit_seq: Option<DatabaseCommitSeq>,
    pub based_on_schema_generation: SchemaGeneration,
    pub table_id: TableId,
    pub storage_id: StorageId,
    pub expected_storage_snapshot: StorageSnapshotToken,
    pub expected_logical_data_version: StorageDataVersion,
    pub expected_layout_anchor: LsmMaintenanceAnchor,
    pub expected_memtable_entries: u64,
    pub selected_plan: LsmCompactionPlanInspection,
    pub estimated_cost: MaintenanceEstimate,
    pub conservative_bound: MaintenanceEstimate,
    pub policy: AdaptiveLsmCompactionPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdaptiveLsmMaintenanceProposal {
    Flush(AdaptiveLsmFlushProposal),
    CompactOne(AdaptiveLsmCompactionProposal),
}

impl AdaptiveLsmMaintenanceProposal {
    #[must_use]
    pub const fn action(&self) -> AdaptiveLsmMaintenanceAction {
        match self {
            Self::Flush(_) => AdaptiveLsmMaintenanceAction::Flush,
            Self::CompactOne(_) => AdaptiveLsmMaintenanceAction::CompactOne,
        }
    }

    #[must_use]
    pub const fn conservative_bound(&self) -> MaintenanceEstimate {
        match self {
            Self::Flush(proposal) => proposal.conservative_bound,
            Self::CompactOne(proposal) => proposal.conservative_bound,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdaptiveLsmMaintenanceDecision {
    Proposal(Box<AdaptiveLsmMaintenanceProposal>),
    NoAction(AdaptiveLsmMaintenanceNoActionReason),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdaptiveLsmMaintenanceAbortReason {
    PreconditionsChanged,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdaptiveLsmMaintenanceOutcome {
    Completed,
    Aborted(AdaptiveLsmMaintenanceAbortReason),
    InconclusiveNoWork,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdaptiveLsmMaintenanceMeasurement {
    pub lsm_before: LsmInspection,
    pub lsm_after: LsmInspection,
    pub maintenance_before: LsmMaintenanceInspection,
    pub maintenance_after: LsmMaintenanceInspection,
    pub storage_snapshot_before: StorageSnapshotToken,
    pub storage_snapshot_after: StorageSnapshotToken,
    pub logical_data_version_before: StorageDataVersion,
    pub logical_data_version_after: StorageDataVersion,
    pub change_stream_before: ChangeStreamInspection,
    pub change_stream_after: ChangeStreamInspection,
    pub global_commit_seq_before: Option<DatabaseCommitSeq>,
    pub global_commit_seq_after: Option<DatabaseCommitSeq>,
    pub schema_generation_before: SchemaGeneration,
    pub schema_generation_after: SchemaGeneration,
    pub logical_rows_unchanged: bool,
    pub obsolete_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdaptiveLsmMaintenanceExecutionReport {
    pub proposal: AdaptiveLsmMaintenanceProposal,
    pub action: AdaptiveLsmMaintenanceAction,
    pub budget_before: MaintenanceBudget,
    pub conservative_bound: MaintenanceEstimate,
    pub estimated_cost: MaintenanceEstimate,
    pub consumed: MaintenanceConsumption,
    pub budget_remaining: MaintenanceBudget,
    pub measurement: Option<AdaptiveLsmMaintenanceMeasurement>,
    pub outcome: AdaptiveLsmMaintenanceOutcome,
}

impl AdaptiveLsmMaintenanceObservation {
    #[must_use]
    pub fn decide_flush(
        &self,
        policy: AdaptiveLsmFlushPolicy,
        budget: MaintenanceBudget,
    ) -> AdaptiveLsmMaintenanceDecision {
        let Some(candidate) = self.production_flush_candidate.as_ref() else {
            return AdaptiveLsmMaintenanceDecision::NoAction(
                AdaptiveLsmMaintenanceNoActionReason::NoMemtableEntries,
            );
        };
        if let Some(reason) = production_blocker(candidate) {
            return AdaptiveLsmMaintenanceDecision::NoAction(reason);
        }
        let threshold = self
            .maintenance
            .memtable_flush_threshold_bytes
            .max(policy.minimum_memtable_bytes);
        if self.maintenance.memtable_bytes < threshold {
            return AdaptiveLsmMaintenanceDecision::NoAction(
                AdaptiveLsmMaintenanceNoActionReason::MemtableBelowAutomaticThreshold,
            );
        }
        if self.lsm.write_amplification.overflowed {
            return AdaptiveLsmMaintenanceDecision::NoAction(
                AdaptiveLsmMaintenanceNoActionReason::AutomaticBoundUnavailable,
            );
        }
        let Some(bound) = self.maintenance.flush_conservative_bound else {
            return AdaptiveLsmMaintenanceDecision::NoAction(
                AdaptiveLsmMaintenanceNoActionReason::AutomaticBoundUnavailable,
            );
        };
        let bound = maintenance_bound(bound.work_units, bound.read_bytes, bound.write_bytes);
        if let Some(blocker) = budget_blocker(bound, budget) {
            return AdaptiveLsmMaintenanceDecision::NoAction(
                AdaptiveLsmMaintenanceNoActionReason::BudgetBlocked(blocker),
            );
        }
        AdaptiveLsmMaintenanceDecision::Proposal(Box::new(AdaptiveLsmMaintenanceProposal::Flush(
            AdaptiveLsmFlushProposal {
                observed_global_commit_seq: self.observed_global_commit_seq,
                based_on_schema_generation: self.schema_generation,
                table_id: self.table_id,
                storage_id: self.storage_id,
                expected_storage_snapshot: self.storage_snapshot,
                expected_logical_data_version: self.logical_data_version,
                expected_layout_anchor: self.maintenance.anchor,
                expected_memtable_entries: self.maintenance.memtable_entry_count,
                expected_memtable_bytes: self.maintenance.memtable_bytes,
                expected_flush_threshold_bytes: self.maintenance.memtable_flush_threshold_bytes,
                estimated_cost: candidate.estimate,
                conservative_bound: bound,
                policy,
            },
        )))
    }

    #[must_use]
    pub fn decide_compaction(
        &self,
        policy: AdaptiveLsmCompactionPolicy,
        budget: MaintenanceBudget,
    ) -> AdaptiveLsmMaintenanceDecision {
        let Some(plan) = self.maintenance.next_compaction.as_ref() else {
            return AdaptiveLsmMaintenanceDecision::NoAction(
                AdaptiveLsmMaintenanceNoActionReason::NoCompactionPlan,
            );
        };
        if self.maintenance.memtable_entry_count != 0 {
            return AdaptiveLsmMaintenanceDecision::NoAction(
                AdaptiveLsmMaintenanceNoActionReason::MemtableNotEmpty,
            );
        }
        let Some(candidate) = self.production_compaction_candidate.as_ref() else {
            return AdaptiveLsmMaintenanceDecision::NoAction(
                AdaptiveLsmMaintenanceNoActionReason::NoCompactionPlan,
            );
        };
        if let Some(reason) = production_blocker(candidate) {
            return AdaptiveLsmMaintenanceDecision::NoAction(reason);
        }
        if plan.input_bytes < policy.minimum_input_bytes {
            return AdaptiveLsmMaintenanceDecision::NoAction(
                AdaptiveLsmMaintenanceNoActionReason::CompactionBelowAutomaticThreshold,
            );
        }
        if self.lsm.write_amplification.overflowed {
            return AdaptiveLsmMaintenanceDecision::NoAction(
                AdaptiveLsmMaintenanceNoActionReason::AutomaticBoundUnavailable,
            );
        }
        let bound = maintenance_bound(
            plan.conservative_bound.work_units,
            plan.conservative_bound.read_bytes,
            plan.conservative_bound.write_bytes,
        );
        if let Some(blocker) = budget_blocker(bound, budget) {
            return AdaptiveLsmMaintenanceDecision::NoAction(
                AdaptiveLsmMaintenanceNoActionReason::BudgetBlocked(blocker),
            );
        }
        AdaptiveLsmMaintenanceDecision::Proposal(Box::new(
            AdaptiveLsmMaintenanceProposal::CompactOne(AdaptiveLsmCompactionProposal {
                observed_global_commit_seq: self.observed_global_commit_seq,
                based_on_schema_generation: self.schema_generation,
                table_id: self.table_id,
                storage_id: self.storage_id,
                expected_storage_snapshot: self.storage_snapshot,
                expected_logical_data_version: self.logical_data_version,
                expected_layout_anchor: self.maintenance.anchor,
                expected_memtable_entries: self.maintenance.memtable_entry_count,
                selected_plan: plan.clone(),
                estimated_cost: candidate.estimate,
                conservative_bound: bound,
                policy,
            }),
        ))
    }
}

fn maintenance_bound(work_units: u64, read_bytes: u64, write_bytes: u64) -> MaintenanceEstimate {
    MaintenanceEstimate {
        work_units,
        read_bytes,
        write_bytes,
    }
}

fn production_blocker(
    candidate: &MaintenanceCandidate,
) -> Option<AdaptiveLsmMaintenanceNoActionReason> {
    candidate.blocker.map(|blocker| match blocker {
        MaintenanceBlocker::ActionBudgetExhausted
        | MaintenanceBlocker::WorkBudgetExceeded
        | MaintenanceBlocker::ReadBudgetExceeded
        | MaintenanceBlocker::WriteBudgetExceeded => {
            AdaptiveLsmMaintenanceNoActionReason::BudgetBlocked(blocker)
        }
        _ => AdaptiveLsmMaintenanceNoActionReason::MaintenanceBlocked(blocker),
    })
}

fn same_flush_authority(
    expected: &AdaptiveLsmFlushProposal,
    current: &AdaptiveLsmFlushProposal,
) -> bool {
    expected.based_on_schema_generation == current.based_on_schema_generation
        && expected.table_id == current.table_id
        && expected.storage_id == current.storage_id
        && expected.expected_storage_snapshot == current.expected_storage_snapshot
        && expected.expected_logical_data_version == current.expected_logical_data_version
        && expected.expected_layout_anchor == current.expected_layout_anchor
        && expected.expected_memtable_entries == current.expected_memtable_entries
        && expected.expected_memtable_bytes == current.expected_memtable_bytes
        && expected.expected_flush_threshold_bytes == current.expected_flush_threshold_bytes
        && expected.estimated_cost == current.estimated_cost
        && expected.conservative_bound == current.conservative_bound
        && expected.policy == current.policy
}

fn same_compaction_authority(
    expected: &AdaptiveLsmCompactionProposal,
    current: &AdaptiveLsmCompactionProposal,
) -> bool {
    expected.based_on_schema_generation == current.based_on_schema_generation
        && expected.table_id == current.table_id
        && expected.storage_id == current.storage_id
        && expected.expected_storage_snapshot == current.expected_storage_snapshot
        && expected.expected_logical_data_version == current.expected_logical_data_version
        && expected.expected_layout_anchor == current.expected_layout_anchor
        && expected.expected_memtable_entries == current.expected_memtable_entries
        && expected.selected_plan == current.selected_plan
        && expected.estimated_cost == current.estimated_cost
        && expected.conservative_bound == current.conservative_bound
        && expected.policy == current.policy
}

/// Re-evaluates a proposal against a fresh production observation and budget.
#[must_use]
pub fn revalidate_lsm_proposal(
    proposal: &AdaptiveLsmMaintenanceProposal,
    current: &AdaptiveLsmMaintenanceObservation,
    budget: MaintenanceBudget,
) -> bool {
    match proposal {
        AdaptiveLsmMaintenanceProposal::Flush(expected) => {
            match current.decide_flush(expected.policy, budget) {
                AdaptiveLsmMaintenanceDecision::Proposal(next) => match *next {
                    AdaptiveLsmMaintenanceProposal::Flush(next) => {
                        same_flush_authority(expected, &next)
                    }
                    AdaptiveLsmMaintenanceProposal::CompactOne(_) => false,
                },
                AdaptiveLsmMaintenanceDecision::NoAction(_) => false,
            }
        }
        AdaptiveLsmMaintenanceProposal::CompactOne(expected) => {
            match current.decide_compaction(expected.policy, budget) {
                AdaptiveLsmMaintenanceDecision::Proposal(next) => match *next {
                    AdaptiveLsmMaintenanceProposal::CompactOne(next) => {
                        same_compaction_authority(expected, &next)
                    }
                    AdaptiveLsmMaintenanceProposal::Flush(_) => false,
                },
                AdaptiveLsmMaintenanceDecision::NoAction(_) => false,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maintenance_admission_fails_closed_and_rank_is_stable() {
        let budget = MaintenanceBudget::new(10, 10, 10, 1);
        let estimate = MaintenanceEstimate {
            work_units: 5,
            read_bytes: 3,
            write_bytes: 4,
        };
        let flush = MaintenanceAction::FlushLsm {
            table_id: TableId(1),
            storage_id: StorageId(1),
        };
        let compact = MaintenanceAction::CompactLsm {
            table_id: TableId(1),
            storage_id: StorageId(1),
        };
        let admitted = candidate(
            flush,
            MaintenanceReason::LsmMemtableFlush,
            MaintenanceBound::EstimateGatedAtomic,
            estimate,
            None,
            false,
            budget,
        );
        let blocked = candidate(
            compact,
            MaintenanceReason::LsmCompactionPressure,
            MaintenanceBound::EstimateGatedAtomic,
            estimate,
            Some(MaintenanceBlocker::RecoveryRequired),
            false,
            budget,
        );
        assert_eq!(
            plan_maintenance(vec![blocked, admitted], None)
                .decision
                .unwrap()
                .action,
            flush
        );
        let unproven = candidate(
            flush,
            MaintenanceReason::LsmMemtableFlush,
            MaintenanceBound::EstimateGatedAtomic,
            estimate,
            Some(MaintenanceBlocker::Unavailable),
            false,
            budget,
        );
        assert!(plan_maintenance(vec![unproven], None).decision.is_none());
        assert_eq!(
            candidate(
                flush,
                MaintenanceReason::LsmMemtableFlush,
                MaintenanceBound::EstimateGatedAtomic,
                estimate,
                None,
                false,
                MaintenanceBudget::new(0, 10, 10, 1)
            )
            .blocker,
            Some(MaintenanceBlocker::WorkBudgetExceeded)
        );
        assert!(
            budget
                .checked_remaining(MaintenanceConsumption {
                    work_units: u64::MAX,
                    ..Default::default()
                })
                .is_none()
        );
    }

    #[test]
    fn logical_tick_gate_is_pure_and_requires_progress_or_retry() {
        let policy = AutomaticSchedulerPolicy::new(2, 5, 4, 9).unwrap();
        let evidence = AdaptiveEvidenceProgressToken {
            window_epoch: AdaptiveEvidenceWindowEpoch(2),
            schema_generation: Some(SchemaGeneration(1)),
            recorded_reports: 3,
        };
        let state = AutomaticSchedulerState {
            last_observed_tick: Some(AutomaticSchedulerTick(10)),
            last_run_tick: Some(AutomaticSchedulerTick(10)),
            gate: AutomaticSchedulerGate::AwaitingTrialProgress { evidence },
        };
        assert_eq!(
            evaluate_scheduler_gate(state, policy, evidence, AutomaticSchedulerTick(10)),
            Ok(AutomaticSchedulerInspection::Held(
                AutomaticSchedulerHoldReason::DuplicateTick
            ))
        );
        assert!(
            evaluate_scheduler_gate(state, policy, evidence, AutomaticSchedulerTick(9)).is_err()
        );
        assert!(matches!(
            evaluate_scheduler_gate(state, policy, evidence, AutomaticSchedulerTick(12)),
            Ok(AutomaticSchedulerInspection::Held(
                AutomaticSchedulerHoldReason::AwaitingTrialEvidenceOrRetry { .. }
            ))
        ));
        let changed = AdaptiveEvidenceProgressToken {
            recorded_reports: 4,
            ..evidence
        };
        assert_eq!(
            evaluate_scheduler_gate(state, policy, changed, AutomaticSchedulerTick(12)),
            Ok(AutomaticSchedulerInspection::WouldRunNow)
        );
        assert_eq!(
            evaluate_scheduler_gate(state, policy, evidence, AutomaticSchedulerTick(19)),
            Ok(AutomaticSchedulerInspection::WouldRunNow)
        );
        let renewal = AutomaticSchedulerState {
            gate: AutomaticSchedulerGate::AwaitingEvidenceRenewal {
                blocked_window_epoch: evidence.window_epoch,
                recommendation: AutomaticEvidenceRenewalRecommendation {
                    reason: AutomaticEvidenceRenewalReason::ColumnarPhysicalStateChanged,
                },
            },
            ..state
        };
        assert!(matches!(
            evaluate_scheduler_gate(renewal, policy, changed, AutomaticSchedulerTick(20)),
            Ok(AutomaticSchedulerInspection::Held(
                AutomaticSchedulerHoldReason::AwaitingEvidenceRenewal { .. }
            ))
        ));
    }

    #[test]
    fn retained_lsm_proposal_cannot_survive_a_layout_or_data_change() {
        let proposal = AdaptiveLsmFlushProposal {
            observed_global_commit_seq: Some(DatabaseCommitSeq(1)),
            based_on_schema_generation: SchemaGeneration(2),
            table_id: TableId(3),
            storage_id: StorageId(4),
            expected_storage_snapshot: StorageSnapshotToken::lsm(StorageId(4), 1, 2),
            expected_logical_data_version: StorageDataVersion(5),
            expected_layout_anchor: LsmMaintenanceAnchor {
                storage_id: StorageId(4),
                manifest_generation: 6,
                wal_generation: 7,
                visible_commit_sequence: 8,
            },
            expected_memtable_entries: 9,
            expected_memtable_bytes: 10,
            expected_flush_threshold_bytes: 10,
            estimated_cost: MaintenanceEstimate {
                work_units: 1,
                read_bytes: 2,
                write_bytes: 3,
            },
            conservative_bound: MaintenanceEstimate {
                work_units: 2,
                read_bytes: 4,
                write_bytes: 6,
            },
            policy: AdaptiveLsmFlushPolicy {
                minimum_memtable_bytes: 10,
            },
        };
        assert!(same_flush_authority(&proposal, &proposal));
        let mut changed = proposal.clone();
        changed.expected_logical_data_version = StorageDataVersion(6);
        assert!(!same_flush_authority(&proposal, &changed));
        let mut changed = proposal.clone();
        changed.expected_layout_anchor.manifest_generation += 1;
        assert!(!same_flush_authority(&proposal, &changed));
        let mut changed = proposal.clone();
        changed.conservative_bound.write_bytes -= 1;
        assert!(!same_flush_authority(&proposal, &changed));
    }
}
