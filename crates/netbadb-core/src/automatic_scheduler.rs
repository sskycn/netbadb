use std::error::Error;
use std::fmt;

use crate::{
    AdaptiveEvidencePool, AdaptiveEvidenceProgressToken, AutomaticMultiSafeModePolicy,
    AutomaticOrchestrationError, AutomaticOrchestrationInput, AutomaticOrchestrationReport,
    AutomaticOrchestrationStopReason, Database,
};

pub use netbadb_advisor::{
    AutomaticSchedulerDelayClass, AutomaticSchedulerFault, AutomaticSchedulerGate,
    AutomaticSchedulerHoldReason, AutomaticSchedulerInspection, AutomaticSchedulerPolicy,
    AutomaticSchedulerPolicyError, AutomaticSchedulerState, AutomaticSchedulerTick,
};
use netbadb_advisor::{SchedulerTickOrderError, evaluate_scheduler_gate};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AutomaticSchedulerTickOutcome {
    Held(AutomaticSchedulerHoldReason),
    Ran(AutomaticOrchestrationReport),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AutomaticSchedulerTickReport {
    pub tick: AutomaticSchedulerTick,
    pub state_before: AutomaticSchedulerState,
    pub state_after: AutomaticSchedulerState,
    pub evidence_progress: AdaptiveEvidenceProgressToken,
    pub outcome: AutomaticSchedulerTickOutcome,
}

#[derive(Debug)]
pub struct AutomaticSchedulerOrchestrationFailure {
    pub tick: AutomaticSchedulerTick,
    pub state_before: AutomaticSchedulerState,
    pub state_after: AutomaticSchedulerState,
    pub source: AutomaticOrchestrationError,
}

#[derive(Debug)]
pub enum AutomaticSchedulerError {
    OutOfOrderTick {
        previous: AutomaticSchedulerTick,
        received: AutomaticSchedulerTick,
    },
    Orchestration(Box<AutomaticSchedulerOrchestrationFailure>),
}

impl fmt::Display for AutomaticSchedulerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OutOfOrderTick { previous, received } => write!(
                formatter,
                "automatic scheduler tick {} follows newer tick {}",
                received.0, previous.0
            ),
            Self::Orchestration(failure) => write!(
                formatter,
                "automatic scheduler orchestration at tick {} failed: {}",
                failure.tick.0, failure.source
            ),
        }
    }
}

impl Error for AutomaticSchedulerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::OutOfOrderTick { .. } => None,
            Self::Orchestration(failure) => Some(&failure.source),
        }
    }
}

/// Caller-owned cooperative invocation gate for the Phase 12 runner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AutomaticScheduler {
    policy: AutomaticSchedulerPolicy,
    state: AutomaticSchedulerState,
}

impl AutomaticScheduler {
    #[must_use]
    pub const fn new(policy: AutomaticSchedulerPolicy) -> Self {
        Self {
            policy,
            state: AutomaticSchedulerState::INITIAL,
        }
    }

    #[must_use]
    pub const fn policy(&self) -> AutomaticSchedulerPolicy {
        self.policy
    }

    #[must_use]
    pub const fn state(&self) -> AutomaticSchedulerState {
        self.state
    }

    /// Evaluates one logical scheduling opportunity without changing any
    /// scheduler, database, or evidence-pool state.
    pub fn inspect_tick(
        &self,
        pool: &AdaptiveEvidencePool,
        tick: AutomaticSchedulerTick,
    ) -> Result<AutomaticSchedulerInspection, AutomaticSchedulerError> {
        evaluate_scheduler_gate(self.state, self.policy, pool.progress_token(), tick)
            .map_err(map_tick_order_error)
    }

    /// Evaluates one cooperative tick and invokes the Phase 12 runner at most
    /// once. The scheduler borrows, but never owns, database or evidence state.
    pub fn tick(
        &mut self,
        database: &mut Database,
        pool: &AdaptiveEvidencePool,
        tick: AutomaticSchedulerTick,
        input: AutomaticOrchestrationInput<'_>,
        automatic_policy: AutomaticMultiSafeModePolicy,
    ) -> Result<AutomaticSchedulerTickReport, AutomaticSchedulerError> {
        self.tick_with_runner(pool, tick, || {
            database.run_automatic_safe_orchestration(pool, input, automatic_policy)
        })
    }

    fn orchestration_error_without_run(
        &self,
        tick: AutomaticSchedulerTick,
        source: AutomaticOrchestrationError,
    ) -> AutomaticSchedulerError {
        AutomaticSchedulerError::Orchestration(Box::new(AutomaticSchedulerOrchestrationFailure {
            tick,
            state_before: self.state,
            state_after: self.state,
            source,
        }))
    }

    fn tick_with_runner<F>(
        &mut self,
        pool: &AdaptiveEvidencePool,
        tick: AutomaticSchedulerTick,
        runner: F,
    ) -> Result<AutomaticSchedulerTickReport, AutomaticSchedulerError>
    where
        F: FnOnce() -> Result<AutomaticOrchestrationReport, AutomaticOrchestrationError>,
    {
        let evidence_progress = pool.progress_token();
        let state_before = self.state;
        match evaluate_scheduler_gate(state_before, self.policy, evidence_progress, tick)
            .map_err(map_tick_order_error)?
        {
            AutomaticSchedulerInspection::Held(reason) => {
                if reason != AutomaticSchedulerHoldReason::DuplicateTick {
                    self.state.last_observed_tick = Some(tick);
                }
                Ok(AutomaticSchedulerTickReport {
                    tick,
                    state_before,
                    state_after: self.state,
                    evidence_progress,
                    outcome: AutomaticSchedulerTickOutcome::Held(reason),
                })
            }
            AutomaticSchedulerInspection::WouldRunNow => match runner() {
                Ok(report) => {
                    self.state.last_observed_tick = Some(tick);
                    self.state.last_run_tick = Some(tick);
                    self.state.gate = gate_after_report(&report, evidence_progress);
                    Ok(AutomaticSchedulerTickReport {
                        tick,
                        state_before,
                        state_after: self.state,
                        evidence_progress,
                        outcome: AutomaticSchedulerTickOutcome::Ran(report),
                    })
                }
                Err(source @ AutomaticOrchestrationError::InvalidEnvelope(_)) => {
                    Err(self.orchestration_error_without_run(tick, source))
                }
                Err(source) => {
                    self.state.last_observed_tick = Some(tick);
                    self.state.last_run_tick = Some(tick);
                    self.state.gate = AutomaticSchedulerGate::Faulted(match &source {
                        AutomaticOrchestrationError::StepFailed(_) => {
                            AutomaticSchedulerFault::StepFailed
                        }
                        AutomaticOrchestrationError::ConsumptionOverflow(_) => {
                            AutomaticSchedulerFault::ConsumptionOverflow
                        }
                        AutomaticOrchestrationError::InvalidEnvelope(_) => {
                            // Handled by the preceding match arm.
                            return Err(self.orchestration_error_without_run(tick, source));
                        }
                    });
                    Err(AutomaticSchedulerError::Orchestration(Box::new(
                        AutomaticSchedulerOrchestrationFailure {
                            tick,
                            state_before,
                            state_after: self.state,
                            source,
                        },
                    )))
                }
            },
        }
    }
}

fn map_tick_order_error(error: SchedulerTickOrderError) -> AutomaticSchedulerError {
    AutomaticSchedulerError::OutOfOrderTick {
        previous: error.previous,
        received: error.received,
    }
}

fn gate_after_report(
    report: &AutomaticOrchestrationReport,
    evidence: AdaptiveEvidenceProgressToken,
) -> AutomaticSchedulerGate {
    match report.stop_reason {
        AutomaticOrchestrationStopReason::NoReadyWork => AutomaticSchedulerGate::Open {
            delay: AutomaticSchedulerDelayClass::Idle,
        },
        AutomaticOrchestrationStopReason::StepLimitReached
        | AutomaticOrchestrationStopReason::TrialBoundaryResolved => AutomaticSchedulerGate::Open {
            delay: AutomaticSchedulerDelayClass::Normal,
        },
        AutomaticOrchestrationStopReason::SelectedCandidateDidNotProgress => {
            AutomaticSchedulerGate::Open {
                delay: AutomaticSchedulerDelayClass::NoProgress,
            }
        }
        AutomaticOrchestrationStopReason::ActiveTrial(_) => {
            AutomaticSchedulerGate::AwaitingTrialProgress { evidence }
        }
        AutomaticOrchestrationStopReason::EvidenceRenewalRecommended(recommendation) => {
            AutomaticSchedulerGate::AwaitingEvidenceRenewal {
                blocked_window_epoch: evidence.window_epoch,
                recommendation,
            }
        }
        AutomaticOrchestrationStopReason::MaintenanceEnvelopeExceeded { .. } => {
            AutomaticSchedulerGate::Faulted(AutomaticSchedulerFault::MaintenanceEnvelopeExceeded)
        }
    }
}

#[cfg(test)]
#[path = "automatic_scheduler_tests.rs"]
mod tests;
