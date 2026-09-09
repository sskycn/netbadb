use std::error::Error;
use std::fmt;

use netbadb_planner::{CalibrationRatio, PlannerCalibrationClass, PlannerCalibrationEpoch};
use netbadb_types::{ColumnarProjectionId, SchemaGeneration, TableId};

use crate::planner_calibration::{aggregate_calibration_evidence, replay_calibration_ratio_errors};
use crate::{
    AdaptiveCycleReport, AdaptiveDecision, AdaptiveError, AdaptiveMaintenanceOutcome,
    AdaptivePolicy, AdaptiveWorkloadEvaluationReport, AdaptiveWorkloadLimits,
    AdaptiveWorkloadOutcome, AdaptiveWorkloadPolicy, AdaptiveWorkloadStaleReason,
    AdaptiveWorkloadTarget, AdaptiveWorkloadWindow, Database, MaintenanceBudget,
    PlannerCalibrationAdvisorError, PlannerCalibrationDecision, PlannerCalibrationMutationError,
    PlannerCalibrationNoAction, PlannerCalibrationPolicy, PlannerCalibrationReceipt,
    PlannerCalibrationShadowDecision,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AutomaticCalibrationTrialPolicy {
    pub minimum_samples: u64,
    pub minimum_actual_work_units: u64,
    pub minimum_distinct_visibility_points: u64,
    pub minimum_distinct_query_shapes: u64,
    pub minimum_keep_error_improvement_work_units: u64,
    pub maximum_tolerated_error_regression_work_units: u64,
}

impl Default for AutomaticCalibrationTrialPolicy {
    fn default() -> Self {
        Self {
            minimum_samples: 8,
            minimum_actual_work_units: 1,
            minimum_distinct_visibility_points: 2,
            minimum_distinct_query_shapes: 3,
            minimum_keep_error_improvement_work_units: 1,
            maximum_tolerated_error_regression_work_units: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AutomaticSafeModePolicy {
    pub allow_columnar_maintenance: bool,
    pub allow_planner_calibration: bool,
    pub adaptive_policy: AdaptivePolicy,
    pub workload_policy: AdaptiveWorkloadPolicy,
    pub planner_calibration_policy: PlannerCalibrationPolicy,
    pub calibration_trial_policy: AutomaticCalibrationTrialPolicy,
}

#[derive(Debug, Clone, Copy)]
pub struct AutomaticSafeModeInput<'a> {
    pub columnar_table_id: Option<TableId>,
    pub workload_window: Option<&'a AdaptiveWorkloadWindow>,
    pub calibration_class: Option<PlannerCalibrationClass>,
    pub maintenance_budget: MaintenanceBudget,
}

impl<'a> AutomaticSafeModeInput<'a> {
    #[must_use]
    pub const fn new(maintenance_budget: MaintenanceBudget) -> Self {
        Self {
            columnar_table_id: None,
            workload_window: None,
            calibration_class: None,
            maintenance_budget,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AutomaticColumnarTrial {
    target: AdaptiveWorkloadTarget,
}

impl AutomaticColumnarTrial {
    #[must_use]
    pub const fn target(self) -> AdaptiveWorkloadTarget {
        self.target
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AutomaticPlannerCalibrationTrial {
    calibration_class: PlannerCalibrationClass,
    schema_generation: SchemaGeneration,
    previous_epoch: PlannerCalibrationEpoch,
    applied_epoch: PlannerCalibrationEpoch,
    previous_ratio: CalibrationRatio,
    applied_ratio: CalibrationRatio,
}

impl AutomaticPlannerCalibrationTrial {
    #[must_use]
    pub const fn calibration_class(self) -> PlannerCalibrationClass {
        self.calibration_class
    }

    #[must_use]
    pub const fn schema_generation(self) -> SchemaGeneration {
        self.schema_generation
    }

    #[must_use]
    pub const fn previous_epoch(self) -> PlannerCalibrationEpoch {
        self.previous_epoch
    }

    #[must_use]
    pub const fn applied_epoch(self) -> PlannerCalibrationEpoch {
        self.applied_epoch
    }

    #[must_use]
    pub const fn previous_ratio(self) -> CalibrationRatio {
        self.previous_ratio
    }

    #[must_use]
    pub const fn applied_ratio(self) -> CalibrationRatio {
        self.applied_ratio
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutomaticSafeTrial {
    Columnar(AutomaticColumnarTrial),
    PlannerCalibration(AutomaticPlannerCalibrationTrial),
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AutomaticSafeModeState {
    active_trial: Option<AutomaticSafeTrial>,
}

impl AutomaticSafeModeState {
    #[must_use]
    pub const fn active_trial(self) -> Option<AutomaticSafeTrial> {
        self.active_trial
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutomaticSafeModeLane {
    None,
    ActiveColumnarTrial,
    ActivePlannerCalibrationTrial,
    ColumnarMaintenance,
    PlannerCalibration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutomaticSafeModeMutation {
    ColumnarAdvance {
        projection_id: ColumnarProjectionId,
    },
    ColumnarSuppression {
        target: AdaptiveWorkloadTarget,
    },
    PlannerCalibrationApply {
        calibration_class: PlannerCalibrationClass,
        applied_epoch: PlannerCalibrationEpoch,
    },
    PlannerCalibrationRevert {
        calibration_class: PlannerCalibrationClass,
        reverted_epoch: PlannerCalibrationEpoch,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutomaticTrialAwaitingReason {
    MissingWorkloadWindow,
    WorkloadTargetMismatch,
    EvidenceSchemaMismatch,
    InsufficientEvidence,
    IncompleteEvidence,
    ArithmeticUnavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutomaticCalibrationTrialStaleReason {
    SchemaChanged,
    CalibrationEpochChanged,
    CalibrationRatioChanged,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutomaticSafeModeNoAction {
    AutomaticActionsDisabled,
    ColumnarInputUnavailable,
    CalibrationInputUnavailable,
    ColumnarNoAction,
    CalibrationAdvisorNoAction(PlannerCalibrationNoAction),
    CalibrationShadowRejected(PlannerCalibrationNoAction),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutomaticSafeModeOutcome {
    NoAction(AutomaticSafeModeNoAction),
    ColumnarMutationCompleted,
    ColumnarTrialValidatedKeep,
    ColumnarTrialReverted,
    ColumnarTrialResolvedSuppressed,
    ColumnarTrialHeld,
    ColumnarTrialAwaiting(AutomaticTrialAwaitingReason),
    ColumnarTrialStale(AdaptiveWorkloadStaleReason),
    PlannerCalibrationApplied,
    PlannerCalibrationTrialValidatedKeep,
    PlannerCalibrationTrialReverted,
    PlannerCalibrationTrialHeld,
    PlannerCalibrationTrialAwaiting(AutomaticTrialAwaitingReason),
    PlannerCalibrationTrialStale(AutomaticCalibrationTrialStaleReason),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AutomaticCalibrationTrialEvaluationReport {
    pub calibration_class: PlannerCalibrationClass,
    pub calibration_epoch: PlannerCalibrationEpoch,
    pub sample_count: u64,
    pub total_actual_work_units: u64,
    pub distinct_visibility_points: u64,
    pub distinct_query_shapes: u64,
    pub previous_ratio_error_work_units: u64,
    pub applied_ratio_error_work_units: u64,
    pub overflowed: bool,
    pub incomplete: bool,
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AutomaticSafeModeReport {
    pub trial_before: Option<AutomaticSafeTrial>,
    pub selected_lane: AutomaticSafeModeLane,
    pub mutation: Option<AutomaticSafeModeMutation>,
    pub columnar_cycle: Option<Box<AdaptiveCycleReport>>,
    pub workload_evaluation: Option<AdaptiveWorkloadEvaluationReport>,
    pub calibration_decision: Option<PlannerCalibrationDecision>,
    pub calibration_shadow: Option<PlannerCalibrationShadowDecision>,
    pub calibration_trial_evaluation: Option<AutomaticCalibrationTrialEvaluationReport>,
    pub outcome: AutomaticSafeModeOutcome,
    pub trial_after: Option<AutomaticSafeTrial>,
}

#[derive(Debug)]
pub enum AutomaticSafeModeError {
    Adaptive(AdaptiveError),
    CalibrationAdvisor(PlannerCalibrationAdvisorError),
    CalibrationMutation(PlannerCalibrationMutationError),
    MissingColumnarMeasurement,
}

impl fmt::Display for AutomaticSafeModeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Adaptive(error) => error.fmt(formatter),
            Self::CalibrationAdvisor(error) => error.fmt(formatter),
            Self::CalibrationMutation(error) => error.fmt(formatter),
            Self::MissingColumnarMeasurement => formatter.write_str(
                "a kept automatic Columnar mutation did not return its measured target state",
            ),
        }
    }
}

impl Error for AutomaticSafeModeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Adaptive(error) => Some(error),
            Self::CalibrationAdvisor(error) => Some(error),
            Self::CalibrationMutation(error) => Some(error),
            Self::MissingColumnarMeasurement => None,
        }
    }
}

impl From<AdaptiveError> for AutomaticSafeModeError {
    fn from(error: AdaptiveError) -> Self {
        Self::Adaptive(error)
    }
}

impl From<PlannerCalibrationAdvisorError> for AutomaticSafeModeError {
    fn from(error: PlannerCalibrationAdvisorError) -> Self {
        Self::CalibrationAdvisor(error)
    }
}

impl From<PlannerCalibrationMutationError> for AutomaticSafeModeError {
    fn from(error: PlannerCalibrationMutationError) -> Self {
        Self::CalibrationMutation(error)
    }
}

#[derive(Debug, Clone, Copy)]
enum ActiveAutomaticTrial {
    Columnar {
        target: AdaptiveWorkloadTarget,
    },
    PlannerCalibration {
        receipt: PlannerCalibrationReceipt,
        schema_generation: SchemaGeneration,
    },
}

impl ActiveAutomaticTrial {
    fn summary(self) -> AutomaticSafeTrial {
        match self {
            Self::Columnar { target } => {
                AutomaticSafeTrial::Columnar(AutomaticColumnarTrial { target })
            }
            Self::PlannerCalibration {
                receipt,
                schema_generation,
            } => AutomaticSafeTrial::PlannerCalibration(AutomaticPlannerCalibrationTrial {
                calibration_class: receipt.calibration_class(),
                schema_generation,
                previous_epoch: receipt.previous_epoch(),
                applied_epoch: receipt.applied_epoch(),
                previous_ratio: receipt.previous_ratio(),
                applied_ratio: receipt.applied_ratio(),
            }),
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct AutomaticSafeModeRuntimeState {
    active_trial: Option<ActiveAutomaticTrial>,
}

impl Database {
    #[must_use]
    pub fn automatic_safe_mode_state(&self) -> AutomaticSafeModeState {
        AutomaticSafeModeState {
            active_trial: self
                .automatic_safe_mode
                .active_trial
                .map(ActiveAutomaticTrial::summary),
        }
    }

    /// Ends probation without changing projection eligibility, calibration, or data.
    pub fn abandon_automatic_safe_trial(&mut self) -> Option<AutomaticSafeTrial> {
        self.automatic_safe_mode
            .active_trial
            .take()
            .map(ActiveAutomaticTrial::summary)
    }

    /// Performs one explicit synchronous safe-mode control step. A live trial
    /// owns the step, and every path performs at most one control mutation.
    pub fn automatic_safe_step(
        &mut self,
        input: AutomaticSafeModeInput<'_>,
        policy: AutomaticSafeModePolicy,
    ) -> Result<AutomaticSafeModeReport, AutomaticSafeModeError> {
        let trial_before = self.automatic_safe_mode_state().active_trial();
        if let Some(trial) = self.automatic_safe_mode.active_trial {
            return match trial {
                ActiveAutomaticTrial::Columnar { target } => self
                    .evaluate_automatic_columnar_trial(
                        target,
                        input.workload_window,
                        policy.workload_policy,
                        trial_before,
                    ),
                ActiveAutomaticTrial::PlannerCalibration {
                    receipt,
                    schema_generation,
                } => self.evaluate_automatic_calibration_trial(
                    receipt,
                    schema_generation,
                    input.workload_window,
                    policy.calibration_trial_policy,
                    trial_before,
                ),
            };
        }

        let mut columnar_cycle = None;
        let mut no_action = AutomaticSafeModeNoAction::AutomaticActionsDisabled;
        if policy.allow_columnar_maintenance {
            if let Some(table_id) = input.columnar_table_id {
                let cycle = self.adaptive_columnar_step(
                    table_id,
                    policy.adaptive_policy,
                    input.maintenance_budget,
                )?;
                if matches!(cycle.decision, AdaptiveDecision::Proposal(_)) {
                    let mutation = cycle.execution.as_ref().and_then(|execution| {
                        (execution.consumed.actions != 0).then_some(
                            AutomaticSafeModeMutation::ColumnarAdvance {
                                projection_id: execution.proposal.projection_id,
                            },
                        )
                    });
                    if cycle.execution.as_ref().is_some_and(|execution| {
                        execution.outcome == AdaptiveMaintenanceOutcome::Kept
                    }) {
                        let execution = cycle
                            .execution
                            .as_ref()
                            .ok_or(AutomaticSafeModeError::MissingColumnarMeasurement)?;
                        let measurement = execution
                            .measurement
                            .as_ref()
                            .ok_or(AutomaticSafeModeError::MissingColumnarMeasurement)?;
                        self.automatic_safe_mode.active_trial =
                            Some(ActiveAutomaticTrial::Columnar {
                                target: AdaptiveWorkloadTarget {
                                    table_id: execution.proposal.table_id,
                                    storage_id: execution.proposal.storage_id,
                                    projection_id: execution.proposal.projection_id,
                                    generation: measurement.after_outcome.projection_generation,
                                    schema_generation: measurement.schema_generation_after,
                                },
                            });
                    }
                    return Ok(self.finish_automatic_report(
                        trial_before,
                        AutomaticSafeModeLane::ColumnarMaintenance,
                        mutation,
                        Some(Box::new(cycle)),
                        None,
                        None,
                        None,
                        None,
                        AutomaticSafeModeOutcome::ColumnarMutationCompleted,
                    ));
                }
                no_action = AutomaticSafeModeNoAction::ColumnarNoAction;
                columnar_cycle = Some(Box::new(cycle));
            } else {
                no_action = AutomaticSafeModeNoAction::ColumnarInputUnavailable;
            }
        }

        if policy.allow_planner_calibration {
            let (Some(window), Some(class)) = (input.workload_window, input.calibration_class)
            else {
                return Ok(self.finish_automatic_report(
                    trial_before,
                    AutomaticSafeModeLane::None,
                    None,
                    columnar_cycle,
                    None,
                    None,
                    None,
                    None,
                    AutomaticSafeModeOutcome::NoAction(
                        AutomaticSafeModeNoAction::CalibrationInputUnavailable,
                    ),
                ));
            };
            let decision =
                self.advise_planner_calibration(window, class, policy.planner_calibration_policy)?;
            let proposal = match &decision {
                PlannerCalibrationDecision::NoAction(reason) => {
                    return Ok(self.finish_automatic_report(
                        trial_before,
                        AutomaticSafeModeLane::PlannerCalibration,
                        None,
                        columnar_cycle,
                        None,
                        Some(decision.clone()),
                        None,
                        None,
                        AutomaticSafeModeOutcome::NoAction(
                            AutomaticSafeModeNoAction::CalibrationAdvisorNoAction(*reason),
                        ),
                    ));
                }
                PlannerCalibrationDecision::Proposal(proposal) => proposal,
            };
            let shadow = self.shadow_planner_calibration(proposal);
            let shadow_report = match &shadow {
                PlannerCalibrationShadowDecision::Accepted(report) => report,
                PlannerCalibrationShadowDecision::Rejected { reason, .. } => {
                    return Ok(self.finish_automatic_report(
                        trial_before,
                        AutomaticSafeModeLane::PlannerCalibration,
                        None,
                        columnar_cycle,
                        None,
                        Some(decision.clone()),
                        Some(shadow.clone()),
                        None,
                        AutomaticSafeModeOutcome::NoAction(
                            AutomaticSafeModeNoAction::CalibrationShadowRejected(*reason),
                        ),
                    ));
                }
            };
            let schema_generation = self.schema_generation();
            let receipt = self.apply_planner_calibration(proposal, shadow_report)?;
            self.automatic_safe_mode.active_trial =
                Some(ActiveAutomaticTrial::PlannerCalibration {
                    receipt,
                    schema_generation,
                });
            return Ok(self.finish_automatic_report(
                trial_before,
                AutomaticSafeModeLane::PlannerCalibration,
                Some(AutomaticSafeModeMutation::PlannerCalibrationApply {
                    calibration_class: receipt.calibration_class(),
                    applied_epoch: receipt.applied_epoch(),
                }),
                columnar_cycle,
                None,
                Some(decision),
                Some(shadow),
                None,
                AutomaticSafeModeOutcome::PlannerCalibrationApplied,
            ));
        }

        Ok(self.finish_automatic_report(
            trial_before,
            AutomaticSafeModeLane::None,
            None,
            columnar_cycle,
            None,
            None,
            None,
            None,
            AutomaticSafeModeOutcome::NoAction(no_action),
        ))
    }

    #[allow(clippy::too_many_arguments)]
    fn finish_automatic_report(
        &self,
        trial_before: Option<AutomaticSafeTrial>,
        selected_lane: AutomaticSafeModeLane,
        mutation: Option<AutomaticSafeModeMutation>,
        columnar_cycle: Option<Box<AdaptiveCycleReport>>,
        workload_evaluation: Option<AdaptiveWorkloadEvaluationReport>,
        calibration_decision: Option<PlannerCalibrationDecision>,
        calibration_shadow: Option<PlannerCalibrationShadowDecision>,
        calibration_trial_evaluation: Option<AutomaticCalibrationTrialEvaluationReport>,
        outcome: AutomaticSafeModeOutcome,
    ) -> AutomaticSafeModeReport {
        AutomaticSafeModeReport {
            trial_before,
            selected_lane,
            mutation,
            columnar_cycle,
            workload_evaluation,
            calibration_decision,
            calibration_shadow,
            calibration_trial_evaluation,
            outcome,
            trial_after: self.automatic_safe_mode_state().active_trial(),
        }
    }

    fn evaluate_automatic_columnar_trial(
        &mut self,
        target: AdaptiveWorkloadTarget,
        window: Option<&AdaptiveWorkloadWindow>,
        policy: AdaptiveWorkloadPolicy,
        trial_before: Option<AutomaticSafeTrial>,
    ) -> Result<AutomaticSafeModeReport, AutomaticSafeModeError> {
        let empty_window;
        let (evaluation_window, supplied_matches) = match window {
            Some(window) if window.target == target => (window, true),
            _ => {
                empty_window =
                    AdaptiveWorkloadWindow::new(target, AdaptiveWorkloadLimits::default());
                (&empty_window, false)
            }
        };
        let evaluation = self.evaluate_adaptive_workload(evaluation_window, policy)?;
        let (outcome, mutation, clear) = match evaluation.outcome {
            AdaptiveWorkloadOutcome::StaleWindow(reason) => (
                AutomaticSafeModeOutcome::ColumnarTrialStale(reason),
                None,
                true,
            ),
            _ if !supplied_matches => (
                AutomaticSafeModeOutcome::ColumnarTrialAwaiting(if window.is_some() {
                    AutomaticTrialAwaitingReason::WorkloadTargetMismatch
                } else {
                    AutomaticTrialAwaitingReason::MissingWorkloadWindow
                }),
                None,
                false,
            ),
            AdaptiveWorkloadOutcome::ValidatedKeep => (
                AutomaticSafeModeOutcome::ColumnarTrialValidatedKeep,
                None,
                true,
            ),
            AdaptiveWorkloadOutcome::RevertedMeasuredRegression => (
                AutomaticSafeModeOutcome::ColumnarTrialReverted,
                Some(AutomaticSafeModeMutation::ColumnarSuppression { target }),
                true,
            ),
            AdaptiveWorkloadOutcome::HeldSuppressed => (
                AutomaticSafeModeOutcome::ColumnarTrialResolvedSuppressed,
                None,
                true,
            ),
            AdaptiveWorkloadOutcome::HeldWithinHysteresisBand => {
                (AutomaticSafeModeOutcome::ColumnarTrialHeld, None, false)
            }
            AdaptiveWorkloadOutcome::Inconclusive => (
                AutomaticSafeModeOutcome::ColumnarTrialAwaiting(
                    AutomaticTrialAwaitingReason::InsufficientEvidence,
                ),
                None,
                false,
            ),
        };
        if clear {
            self.automatic_safe_mode.active_trial = None;
        }
        Ok(self.finish_automatic_report(
            trial_before,
            AutomaticSafeModeLane::ActiveColumnarTrial,
            mutation,
            None,
            Some(evaluation),
            None,
            None,
            None,
            outcome,
        ))
    }

    fn evaluate_automatic_calibration_trial(
        &mut self,
        receipt: PlannerCalibrationReceipt,
        schema_generation: SchemaGeneration,
        window: Option<&AdaptiveWorkloadWindow>,
        policy: AutomaticCalibrationTrialPolicy,
        trial_before: Option<AutomaticSafeTrial>,
    ) -> Result<AutomaticSafeModeReport, AutomaticSafeModeError> {
        let current = self.planner_calibration_profile();
        let stale = if self.schema_generation() != schema_generation {
            Some(AutomaticCalibrationTrialStaleReason::SchemaChanged)
        } else if current.epoch != receipt.applied_epoch() {
            Some(AutomaticCalibrationTrialStaleReason::CalibrationEpochChanged)
        } else if current.ratio(receipt.calibration_class()) != receipt.applied_ratio() {
            Some(AutomaticCalibrationTrialStaleReason::CalibrationRatioChanged)
        } else {
            None
        };
        if let Some(reason) = stale {
            self.automatic_safe_mode.active_trial = None;
            return Ok(self.finish_automatic_report(
                trial_before,
                AutomaticSafeModeLane::ActivePlannerCalibrationTrial,
                None,
                None,
                None,
                None,
                None,
                None,
                AutomaticSafeModeOutcome::PlannerCalibrationTrialStale(reason),
            ));
        }
        let Some(window) = window else {
            return Ok(self.finish_automatic_report(
                trial_before,
                AutomaticSafeModeLane::ActivePlannerCalibrationTrial,
                None,
                None,
                None,
                None,
                None,
                None,
                AutomaticSafeModeOutcome::PlannerCalibrationTrialAwaiting(
                    AutomaticTrialAwaitingReason::MissingWorkloadWindow,
                ),
            ));
        };
        if window.target.schema_generation != schema_generation {
            return Ok(self.finish_automatic_report(
                trial_before,
                AutomaticSafeModeLane::ActivePlannerCalibrationTrial,
                None,
                None,
                None,
                None,
                None,
                None,
                AutomaticSafeModeOutcome::PlannerCalibrationTrialAwaiting(
                    AutomaticTrialAwaitingReason::EvidenceSchemaMismatch,
                ),
            ));
        }
        let evidence = aggregate_calibration_evidence(
            window,
            receipt.calibration_class(),
            receipt.applied_epoch(),
            0,
        );
        let replay = replay_calibration_ratio_errors(
            &evidence,
            receipt.previous_ratio(),
            receipt.applied_ratio(),
        );
        let report = AutomaticCalibrationTrialEvaluationReport {
            calibration_class: evidence.calibration_class,
            calibration_epoch: evidence.calibration_epoch,
            sample_count: evidence.sample_count,
            total_actual_work_units: evidence.total_actual_work_units,
            distinct_visibility_points: evidence.distinct_visibility_points,
            distinct_query_shapes: evidence.distinct_query_shapes,
            previous_ratio_error_work_units: replay.old_error_work_units,
            applied_ratio_error_work_units: replay.new_error_work_units,
            overflowed: evidence.overflowed,
            incomplete: evidence.incomplete || replay.incomplete,
            truncated: evidence.truncated,
        };
        let awaiting = if evidence.overflowed || evidence.incomplete || evidence.truncated {
            Some(AutomaticTrialAwaitingReason::IncompleteEvidence)
        } else if replay.incomplete {
            Some(AutomaticTrialAwaitingReason::ArithmeticUnavailable)
        } else if evidence.sample_count < policy.minimum_samples
            || evidence.total_actual_work_units < policy.minimum_actual_work_units
            || evidence.distinct_visibility_points < policy.minimum_distinct_visibility_points
            || evidence.distinct_query_shapes < policy.minimum_distinct_query_shapes
        {
            Some(AutomaticTrialAwaitingReason::InsufficientEvidence)
        } else {
            None
        };
        if let Some(reason) = awaiting {
            return Ok(self.finish_automatic_report(
                trial_before,
                AutomaticSafeModeLane::ActivePlannerCalibrationTrial,
                None,
                None,
                None,
                None,
                None,
                Some(report),
                AutomaticSafeModeOutcome::PlannerCalibrationTrialAwaiting(reason),
            ));
        }

        let old_error = replay.old_error_work_units;
        let new_error = replay.new_error_work_units;
        let (outcome, mutation, clear) = if old_error >= new_error
            && old_error - new_error >= policy.minimum_keep_error_improvement_work_units
        {
            (
                AutomaticSafeModeOutcome::PlannerCalibrationTrialValidatedKeep,
                None,
                true,
            )
        } else if new_error > old_error
            && new_error - old_error > policy.maximum_tolerated_error_regression_work_units
        {
            let reverted = self.revert_planner_calibration(receipt)?;
            (
                AutomaticSafeModeOutcome::PlannerCalibrationTrialReverted,
                Some(AutomaticSafeModeMutation::PlannerCalibrationRevert {
                    calibration_class: receipt.calibration_class(),
                    reverted_epoch: reverted.applied_epoch(),
                }),
                true,
            )
        } else {
            (
                AutomaticSafeModeOutcome::PlannerCalibrationTrialHeld,
                None,
                false,
            )
        };
        if clear {
            self.automatic_safe_mode.active_trial = None;
        }
        Ok(self.finish_automatic_report(
            trial_before,
            AutomaticSafeModeLane::ActivePlannerCalibrationTrial,
            mutation,
            None,
            None,
            None,
            None,
            Some(report),
            outcome,
        ))
    }
}
