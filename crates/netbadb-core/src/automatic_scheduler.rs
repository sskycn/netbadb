use std::error::Error;
use std::fmt;

use crate::{
    AdaptiveEvidencePool, AdaptiveEvidenceProgressToken, AdaptiveEvidenceWindowEpoch,
    AutomaticEvidenceRenewalRecommendation, AutomaticMultiSafeModePolicy,
    AutomaticOrchestrationError, AutomaticOrchestrationInput, AutomaticOrchestrationReport,
    AutomaticOrchestrationStopReason, Database,
};

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
    const INITIAL: Self = Self {
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
        match evaluate_scheduler_gate(state_before, self.policy, evidence_progress, tick)? {
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

fn evaluate_scheduler_gate(
    state: AutomaticSchedulerState,
    policy: AutomaticSchedulerPolicy,
    evidence: AdaptiveEvidenceProgressToken,
    tick: AutomaticSchedulerTick,
) -> Result<AutomaticSchedulerInspection, AutomaticSchedulerError> {
    if let Some(previous) = state.last_observed_tick {
        if tick < previous {
            return Err(AutomaticSchedulerError::OutOfOrderTick {
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
