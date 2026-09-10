use std::error::Error;
use std::fmt;

use crate::{
    AdaptiveEvidencePool, AutomaticAdmissionScope, AutomaticEvidenceRenewalRecommendation,
    AutomaticMultiSafeModeInput, AutomaticMultiSafeModePolicy, AutomaticMultiSafeModeReport,
    AutomaticSafeModeError, AutomaticSafeModeLane, AutomaticSafeModeReport, AutomaticSafeModeState,
    AutomaticSafeTrial, Database, MaintenanceBudget, MaintenanceConsumption,
};

/// Structural bound on the diagnostic vector produced by one orchestration
/// call. The caller's requested bound is rejected rather than silently
/// clamped.
pub const MAX_AUTOMATIC_ORCHESTRATION_STEPS: u32 = 64;

/// Explicit step and physical-maintenance limits for one synchronous run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AutomaticOrchestrationEnvelope {
    pub max_steps: u32,
    pub per_step_maintenance_budget: MaintenanceBudget,
    pub run_maintenance_budget: MaintenanceBudget,
}

/// Fixed operator scope and bounds for one orchestration run.
#[derive(Debug, Clone, Copy)]
pub struct AutomaticOrchestrationInput<'a> {
    pub scope: AutomaticAdmissionScope<'a>,
    pub envelope: AutomaticOrchestrationEnvelope,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutomaticOrchestrationInvalidEnvelope {
    ZeroSteps,
    StepLimitExceeded { requested: u32, maximum: u32 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutomaticOrchestrationStopReason {
    NoReadyWork,
    StepLimitReached,
    ActiveTrial(AutomaticSafeTrial),
    TrialBoundaryResolved,
    EvidenceRenewalRecommended(AutomaticEvidenceRenewalRecommendation),
    SelectedCandidateDidNotProgress,
    MaintenanceEnvelopeExceeded {
        granted: MaintenanceBudget,
        run_remaining_before: MaintenanceBudget,
        consumed: MaintenanceConsumption,
    },
}

/// Budget trace plus the unchanged causal report from one safe step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AutomaticOrchestrationStepReport {
    /// Zero-based position of this safe step within the current run.
    pub step_index: u32,
    pub maintenance_budget_before: MaintenanceBudget,
    pub maintenance_budget_granted: MaintenanceBudget,
    pub maintenance_consumed: MaintenanceConsumption,
    /// `None` means actual consumption exceeded the run-wide remaining
    /// envelope, so a nonnegative remaining budget cannot be stated.
    pub maintenance_budget_after: Option<MaintenanceBudget>,
    pub report: AutomaticMultiSafeModeReport,
}

/// Complete caller-owned trace for a bounded synchronous orchestration run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AutomaticOrchestrationReport {
    pub steps: Vec<AutomaticOrchestrationStepReport>,
    pub maintenance_budget_before: MaintenanceBudget,
    pub maintenance_consumed: MaintenanceConsumption,
    /// `None` only accompanies a `MaintenanceEnvelopeExceeded` stop where the
    /// actual physical consumption exceeded run-wide remaining budget.
    pub maintenance_budget_remaining: Option<MaintenanceBudget>,
    pub stop_reason: AutomaticOrchestrationStopReason,
}

/// Failure returned by an existing safe step after zero or more completed steps.
#[derive(Debug)]
pub struct AutomaticOrchestrationStepFailure {
    pub step_index: u32,
    pub completed_steps: Vec<AutomaticOrchestrationStepReport>,
    pub maintenance_budget_before: MaintenanceBudget,
    pub maintenance_consumed: MaintenanceConsumption,
    pub maintenance_budget_remaining_before_failed_step: MaintenanceBudget,
    pub safe_mode_state_before: AutomaticSafeModeState,
    pub safe_mode_state_after: AutomaticSafeModeState,
    pub source: AutomaticSafeModeError,
}

/// Checked aggregate-consumption overflow after a safe step completed.
#[derive(Debug)]
pub struct AutomaticOrchestrationConsumptionOverflow {
    pub step_index: u32,
    pub completed_steps: Vec<AutomaticOrchestrationStepReport>,
    pub maintenance_budget_before: MaintenanceBudget,
    pub maintenance_consumed_before_step: MaintenanceConsumption,
    pub maintenance_budget_remaining_before_step: MaintenanceBudget,
}

#[derive(Debug)]
pub enum AutomaticOrchestrationError {
    InvalidEnvelope(AutomaticOrchestrationInvalidEnvelope),
    StepFailed(Box<AutomaticOrchestrationStepFailure>),
    ConsumptionOverflow(Box<AutomaticOrchestrationConsumptionOverflow>),
}

impl AutomaticOrchestrationError {
    #[must_use]
    pub fn completed_steps(&self) -> &[AutomaticOrchestrationStepReport] {
        match self {
            Self::InvalidEnvelope(_) => &[],
            Self::StepFailed(failure) => &failure.completed_steps,
            Self::ConsumptionOverflow(overflow) => &overflow.completed_steps,
        }
    }
}

impl fmt::Display for AutomaticOrchestrationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidEnvelope(AutomaticOrchestrationInvalidEnvelope::ZeroSteps) => {
                formatter.write_str("automatic orchestration requires at least one safe step")
            }
            Self::InvalidEnvelope(AutomaticOrchestrationInvalidEnvelope::StepLimitExceeded {
                requested,
                maximum,
            }) => write!(
                formatter,
                "automatic orchestration requested {requested} steps, exceeding the structural maximum {maximum}"
            ),
            Self::StepFailed(failure) => write!(
                formatter,
                "automatic orchestration safe step {} failed: {}",
                failure.step_index, failure.source
            ),
            Self::ConsumptionOverflow(overflow) => write!(
                formatter,
                "automatic orchestration maintenance consumption overflowed at safe step {}",
                overflow.step_index
            ),
        }
    }
}

impl Error for AutomaticOrchestrationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::StepFailed(failure) => Some(&failure.source),
            Self::InvalidEnvelope(_) | Self::ConsumptionOverflow(_) => None,
        }
    }
}

impl AutomaticSafeModeReport {
    /// Returns physical maintenance consumption from the one selected safe
    /// lane. Calibration, eligibility suppression, and trial-only evaluation
    /// intentionally consume no `MaintenanceBudget` domain resources.
    #[must_use]
    pub fn maintenance_consumption(&self) -> MaintenanceConsumption {
        match self.selected_lane {
            AutomaticSafeModeLane::ColumnarMaintenance => self
                .columnar_cycle
                .as_ref()
                .and_then(|cycle| cycle.execution.as_ref())
                .map(|execution| execution.consumed)
                .or_else(|| {
                    self.columnar_compaction
                        .as_ref()
                        .map(|execution| execution.consumed)
                })
                .unwrap_or_default(),
            AutomaticSafeModeLane::ChangeStreamReclamation => self
                .change_stream_gc
                .as_ref()
                .map(|execution| execution.consumed)
                .unwrap_or_default(),
            AutomaticSafeModeLane::AuthoritativeMaintenance => self
                .lsm_maintenance
                .as_ref()
                .map(|execution| execution.consumed)
                .unwrap_or_default(),
            AutomaticSafeModeLane::None
            | AutomaticSafeModeLane::ActiveColumnarTrial
            | AutomaticSafeModeLane::ActivePlannerCalibrationTrial
            | AutomaticSafeModeLane::PlannerCalibration => MaintenanceConsumption::default(),
        }
    }
}

impl Database {
    /// Executes a bounded sequence of existing multi-target safe steps.
    ///
    /// The runner owns only the explicit run envelope. Candidate safety,
    /// ranking, four-lane service, ready age, and mutation authority remain in
    /// `automatic_safe_step_multi`. Completed steps are not transactional and
    /// are never rolled back when this run stops.
    pub fn run_automatic_safe_orchestration(
        &mut self,
        pool: &AdaptiveEvidencePool,
        input: AutomaticOrchestrationInput<'_>,
        policy: AutomaticMultiSafeModePolicy,
    ) -> Result<AutomaticOrchestrationReport, AutomaticOrchestrationError> {
        validate_envelope(input.envelope)?;

        let envelope = input.envelope;
        let mut steps = Vec::new();
        let mut consumed = MaintenanceConsumption::default();
        let mut remaining = envelope.run_maintenance_budget;

        for step_index in 0..envelope.max_steps {
            let granted = envelope.per_step_maintenance_budget.capped_by(remaining);
            let safe_mode_state_before = self.automatic_safe_mode_state();
            let report = match self.automatic_safe_step_multi(
                pool,
                AutomaticMultiSafeModeInput {
                    scope: input.scope,
                    maintenance_budget: granted,
                },
                policy,
            ) {
                Ok(report) => report,
                Err(source) => {
                    return Err(AutomaticOrchestrationError::StepFailed(Box::new(
                        AutomaticOrchestrationStepFailure {
                            step_index,
                            completed_steps: steps,
                            maintenance_budget_before: envelope.run_maintenance_budget,
                            maintenance_consumed: consumed,
                            maintenance_budget_remaining_before_failed_step: remaining,
                            safe_mode_state_before,
                            safe_mode_state_after: self.automatic_safe_mode_state(),
                            source,
                        },
                    )));
                }
            };
            let step_consumed = report.action.maintenance_consumption();
            let after = remaining.checked_remaining(step_consumed);
            let total = consumed.checked_add(step_consumed);
            let exceeded = !granted.contains(step_consumed) || after.is_none();
            let stop_reason =
                orchestration_stop_reason(&report, exceeded, granted, remaining, step_consumed);
            steps.push(AutomaticOrchestrationStepReport {
                step_index,
                maintenance_budget_before: remaining,
                maintenance_budget_granted: granted,
                maintenance_consumed: step_consumed,
                maintenance_budget_after: after,
                report,
            });
            let Some(total) = total else {
                return Err(AutomaticOrchestrationError::ConsumptionOverflow(Box::new(
                    AutomaticOrchestrationConsumptionOverflow {
                        step_index,
                        completed_steps: steps,
                        maintenance_budget_before: envelope.run_maintenance_budget,
                        maintenance_consumed_before_step: consumed,
                        maintenance_budget_remaining_before_step: remaining,
                    },
                )));
            };
            consumed = total;

            if let Some(stop_reason) = stop_reason {
                return Ok(AutomaticOrchestrationReport {
                    steps,
                    maintenance_budget_before: envelope.run_maintenance_budget,
                    maintenance_consumed: consumed,
                    maintenance_budget_remaining: after,
                    stop_reason,
                });
            }

            if let Some(next_remaining) = after {
                remaining = next_remaining;
            }
        }

        Ok(AutomaticOrchestrationReport {
            steps,
            maintenance_budget_before: envelope.run_maintenance_budget,
            maintenance_consumed: consumed,
            maintenance_budget_remaining: Some(remaining),
            stop_reason: AutomaticOrchestrationStopReason::StepLimitReached,
        })
    }
}

pub(crate) fn orchestration_stop_reason(
    report: &AutomaticMultiSafeModeReport,
    maintenance_envelope_exceeded: bool,
    granted: MaintenanceBudget,
    run_remaining_before: MaintenanceBudget,
    consumed: MaintenanceConsumption,
) -> Option<AutomaticOrchestrationStopReason> {
    if maintenance_envelope_exceeded {
        return Some(
            AutomaticOrchestrationStopReason::MaintenanceEnvelopeExceeded {
                granted,
                run_remaining_before,
                consumed,
            },
        );
    }
    if let Some(recommendation) = report.evidence_renewal_recommendation {
        return Some(AutomaticOrchestrationStopReason::EvidenceRenewalRecommended(recommendation));
    }
    if let Some(trial) = report.action.trial_after {
        return Some(AutomaticOrchestrationStopReason::ActiveTrial(trial));
    }
    if report.action.trial_before.is_some() {
        return Some(AutomaticOrchestrationStopReason::TrialBoundaryResolved);
    }
    if report.selected_candidate.is_none() {
        return Some(AutomaticOrchestrationStopReason::NoReadyWork);
    }
    if report.action.mutation.is_none() {
        return Some(AutomaticOrchestrationStopReason::SelectedCandidateDidNotProgress);
    }
    None
}

fn validate_envelope(
    envelope: AutomaticOrchestrationEnvelope,
) -> Result<(), AutomaticOrchestrationError> {
    if envelope.max_steps == 0 {
        return Err(AutomaticOrchestrationError::InvalidEnvelope(
            AutomaticOrchestrationInvalidEnvelope::ZeroSteps,
        ));
    }
    if envelope.max_steps > MAX_AUTOMATIC_ORCHESTRATION_STEPS {
        return Err(AutomaticOrchestrationError::InvalidEnvelope(
            AutomaticOrchestrationInvalidEnvelope::StepLimitExceeded {
                requested: envelope.max_steps,
                maximum: MAX_AUTOMATIC_ORCHESTRATION_STEPS,
            },
        ));
    }
    Ok(())
}
