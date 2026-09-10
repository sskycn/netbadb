use std::cell::Cell;

use netbadb_planner::PlannerCalibrationClass;
use netbadb_types::TableId;

use crate::adaptive_workload_tests::TimelineFixture;
use crate::automatic_orchestration_tests::{MultiGcFixture, gc_policy};
use crate::execution_feedback_tests::TABLE_ID;
use crate::{
    AdaptiveEvidencePool, AdaptiveEvidenceWindowEpoch, AdaptivePolicy, AdaptiveWorkloadPolicy,
    AutomaticAdmissionScope, AutomaticEvidenceRenewalReason,
    AutomaticEvidenceRenewalRecommendation, AutomaticMultiSafeModePolicy,
    AutomaticOrchestrationConsumptionOverflow, AutomaticOrchestrationEnvelope,
    AutomaticOrchestrationError, AutomaticOrchestrationInput,
    AutomaticOrchestrationInvalidEnvelope, AutomaticOrchestrationReport,
    AutomaticOrchestrationStepFailure, AutomaticOrchestrationStopReason, AutomaticSafeModeError,
    AutomaticSafeModePolicy, AutomaticScheduler, AutomaticSchedulerDelayClass,
    AutomaticSchedulerError, AutomaticSchedulerFault, AutomaticSchedulerGate,
    AutomaticSchedulerHoldReason, AutomaticSchedulerInspection, AutomaticSchedulerPolicy,
    AutomaticSchedulerPolicyError, AutomaticSchedulerTick, AutomaticSchedulerTickOutcome,
    MaintenanceBudget, MaintenanceConsumption,
};

fn scheduler_policy() -> AutomaticSchedulerPolicy {
    AutomaticSchedulerPolicy::new(2, 4, 3, 5).expect("valid scheduler policy")
}

fn zero_budget() -> MaintenanceBudget {
    MaintenanceBudget::new(0, 0, 0, 0)
}

fn report(stop_reason: AutomaticOrchestrationStopReason) -> AutomaticOrchestrationReport {
    AutomaticOrchestrationReport {
        steps: Vec::new(),
        maintenance_budget_before: zero_budget(),
        maintenance_consumed: MaintenanceConsumption::default(),
        maintenance_budget_remaining: Some(zero_budget()),
        stop_reason,
    }
}

fn input<'a>(
    table_ids: &'a [TableId],
    calibration_classes: &'a [PlannerCalibrationClass],
) -> AutomaticOrchestrationInput<'a> {
    AutomaticOrchestrationInput {
        scope: AutomaticAdmissionScope {
            table_ids,
            calibration_classes,
        },
        envelope: AutomaticOrchestrationEnvelope {
            max_steps: 8,
            per_step_maintenance_budget: MaintenanceBudget::new(u64::MAX, u64::MAX, u64::MAX, 1),
            run_maintenance_budget: MaintenanceBudget::new(u64::MAX, u64::MAX, u64::MAX, 8),
        },
    }
}

fn columnar_policy() -> AutomaticMultiSafeModePolicy {
    AutomaticMultiSafeModePolicy {
        safe_mode: AutomaticSafeModePolicy {
            allow_columnar_maintenance: true,
            adaptive_policy: AdaptivePolicy::new(0, 0),
            workload_policy: AdaptiveWorkloadPolicy::new(3, 1, 3, 1, 0),
            ..AutomaticSafeModePolicy::default()
        },
        ..AutomaticMultiSafeModePolicy::default()
    }
}

#[test]
fn policy_validation_rejects_every_invalid_cadence() {
    assert_eq!(
        AutomaticSchedulerPolicy::new(0, 1, 1, 1),
        Err(AutomaticSchedulerPolicyError::ZeroMinimumCadence)
    );
    assert_eq!(
        AutomaticSchedulerPolicy::new(2, 1, 2, 2),
        Err(AutomaticSchedulerPolicyError::IdleRetryBelowMinimum {
            minimum: 2,
            received: 1,
        })
    );
    assert_eq!(
        AutomaticSchedulerPolicy::new(2, 2, 1, 2),
        Err(AutomaticSchedulerPolicyError::NoProgressRetryBelowMinimum {
            minimum: 2,
            received: 1,
        })
    );
    assert_eq!(
        AutomaticSchedulerPolicy::new(2, 2, 2, 1),
        Err(AutomaticSchedulerPolicyError::TrialRetryBelowMinimum {
            minimum: 2,
            received: 1,
        })
    );
}

#[test]
fn first_tick_runs_inspection_is_pure_and_tick_order_is_typed() {
    let pool = AdaptiveEvidencePool::default();
    let pool_before = pool.clone();
    let mut scheduler = AutomaticScheduler::new(scheduler_policy());
    let initial = scheduler.state();
    for _ in 0..100 {
        assert!(matches!(
            scheduler.inspect_tick(&pool, AutomaticSchedulerTick(500)),
            Ok(AutomaticSchedulerInspection::WouldRunNow)
        ));
    }
    assert_eq!(scheduler.state(), initial);
    assert_eq!(pool, pool_before);

    let calls = Cell::new(0);
    let first = scheduler
        .tick_with_runner(&pool, AutomaticSchedulerTick(500), || {
            calls.set(calls.get() + 1);
            Ok(report(AutomaticOrchestrationStopReason::NoReadyWork))
        })
        .expect("first tick runs");
    assert_eq!(calls.get(), 1);
    assert!(matches!(
        first.outcome,
        AutomaticSchedulerTickOutcome::Ran(_)
    ));

    let duplicate = scheduler
        .tick_with_runner(&pool, AutomaticSchedulerTick(500), || {
            calls.set(calls.get() + 1);
            Ok(report(AutomaticOrchestrationStopReason::StepLimitReached))
        })
        .expect("duplicate tick is a hold");
    assert_eq!(calls.get(), 1);
    assert_eq!(
        duplicate.outcome,
        AutomaticSchedulerTickOutcome::Held(AutomaticSchedulerHoldReason::DuplicateTick)
    );

    let before_out_of_order = scheduler.state();
    let error = scheduler
        .tick_with_runner(&pool, AutomaticSchedulerTick(499), || {
            calls.set(calls.get() + 1);
            Ok(report(AutomaticOrchestrationStopReason::StepLimitReached))
        })
        .expect_err("lower tick is rejected");
    assert!(matches!(
        error,
        AutomaticSchedulerError::OutOfOrderTick {
            previous: AutomaticSchedulerTick(500),
            received: AutomaticSchedulerTick(499),
        }
    ));
    assert_eq!(calls.get(), 1);
    assert_eq!(scheduler.state(), before_out_of_order);

    let jump = scheduler
        .tick_with_runner(&pool, AutomaticSchedulerTick(1_000), || {
            calls.set(calls.get() + 1);
            Ok(report(AutomaticOrchestrationStopReason::StepLimitReached))
        })
        .expect("tick jumps are valid");
    assert_eq!(calls.get(), 2);
    assert!(matches!(
        jump.outcome,
        AutomaticSchedulerTickOutcome::Ran(_)
    ));
}

#[test]
fn terminal_reasons_select_distinct_backoffs_and_one_runner_call() {
    let pool = AdaptiveEvidencePool::default();

    for (stop_reason, hold_tick, run_tick, expected_delay, expected_hold) in [
        (
            AutomaticOrchestrationStopReason::NoReadyWork,
            3,
            4,
            AutomaticSchedulerDelayClass::Idle,
            AutomaticSchedulerHoldReason::IdleBackoff {
                last_run_tick: AutomaticSchedulerTick(0),
                required_ticks: 4,
            },
        ),
        (
            AutomaticOrchestrationStopReason::SelectedCandidateDidNotProgress,
            2,
            3,
            AutomaticSchedulerDelayClass::NoProgress,
            AutomaticSchedulerHoldReason::NoProgressBackoff {
                last_run_tick: AutomaticSchedulerTick(0),
                required_ticks: 3,
            },
        ),
        (
            AutomaticOrchestrationStopReason::StepLimitReached,
            1,
            2,
            AutomaticSchedulerDelayClass::Normal,
            AutomaticSchedulerHoldReason::MinimumCadence {
                last_run_tick: AutomaticSchedulerTick(0),
                required_ticks: 2,
            },
        ),
        (
            AutomaticOrchestrationStopReason::TrialBoundaryResolved,
            1,
            2,
            AutomaticSchedulerDelayClass::Normal,
            AutomaticSchedulerHoldReason::MinimumCadence {
                last_run_tick: AutomaticSchedulerTick(0),
                required_ticks: 2,
            },
        ),
    ] {
        let mut scheduler = AutomaticScheduler::new(scheduler_policy());
        let calls = Cell::new(0);
        scheduler
            .tick_with_runner(&pool, AutomaticSchedulerTick(0), || {
                calls.set(calls.get() + 1);
                Ok(report(stop_reason))
            })
            .expect("initial runner call");
        assert_eq!(
            scheduler.state().gate,
            AutomaticSchedulerGate::Open {
                delay: expected_delay,
            }
        );

        let held = scheduler
            .tick_with_runner(&pool, AutomaticSchedulerTick(hold_tick), || {
                calls.set(calls.get() + 1);
                Ok(report(AutomaticOrchestrationStopReason::StepLimitReached))
            })
            .expect("backoff hold");
        assert_eq!(
            held.outcome,
            AutomaticSchedulerTickOutcome::Held(expected_hold)
        );
        assert_eq!(calls.get(), 1);

        scheduler
            .tick_with_runner(&pool, AutomaticSchedulerTick(run_tick), || {
                calls.set(calls.get() + 1);
                Ok(report(AutomaticOrchestrationStopReason::StepLimitReached))
            })
            .expect("backoff boundary runs");
        assert_eq!(calls.get(), 2);
    }
}

#[test]
fn active_trial_wakes_on_evidence_and_periodically_resolves_external_staleness() {
    let mut fixture = TimelineFixture::create("phase13-trial-gate");
    fixture
        .database
        .execute("UPDATE events SET category = 7 WHERE id = 7")
        .expect("create Columnar lag");
    let mut pool = AdaptiveEvidencePool::default();
    let mut scheduler = AutomaticScheduler::new(scheduler_policy());
    let tables = [TABLE_ID];

    let first = scheduler
        .tick(
            &mut fixture.database,
            &pool,
            AutomaticSchedulerTick(10),
            input(&tables, &[]),
            columnar_policy(),
        )
        .expect("start Columnar trial");
    assert!(matches!(
        first.outcome,
        AutomaticSchedulerTickOutcome::Ran(AutomaticOrchestrationReport {
            stop_reason: AutomaticOrchestrationStopReason::EvidenceRenewalRecommended(_),
            ..
        })
    ));
    assert!(matches!(
        scheduler.state().gate,
        AutomaticSchedulerGate::AwaitingEvidenceRenewal { .. }
    ));

    pool.rotate_window().expect("caller renews evidence");
    let cadence = scheduler
        .tick(
            &mut fixture.database,
            &pool,
            AutomaticSchedulerTick(11),
            input(&tables, &[]),
            columnar_policy(),
        )
        .expect("normal cadence still applies");
    assert!(matches!(
        cadence.outcome,
        AutomaticSchedulerTickOutcome::Held(AutomaticSchedulerHoldReason::MinimumCadence { .. })
    ));

    let waiting = scheduler
        .tick(
            &mut fixture.database,
            &pool,
            AutomaticSchedulerTick(12),
            input(&tables, &[]),
            columnar_policy(),
        )
        .expect("evaluate active trial");
    assert!(matches!(
        waiting.outcome,
        AutomaticSchedulerTickOutcome::Ran(AutomaticOrchestrationReport {
            stop_reason: AutomaticOrchestrationStopReason::ActiveTrial(_),
            ..
        })
    ));

    let token_before_query = pool.progress_token();
    fixture
        .database
        .query("SELECT id FROM events LIMIT 1")
        .expect("ordinary query");
    let (_, feedback) = fixture
        .database
        .query_with_feedback("SELECT id FROM events")
        .expect("explicit feedback query");
    assert_eq!(pool.progress_token(), token_before_query);
    pool.record_execution_feedback(&feedback)
        .expect("caller explicitly records feedback");
    assert_ne!(pool.progress_token(), token_before_query);

    let early_cadence = scheduler
        .tick(
            &mut fixture.database,
            &pool,
            AutomaticSchedulerTick(13),
            input(&tables, &[]),
            columnar_policy(),
        )
        .expect("new evidence still honors minimum cadence");
    assert!(matches!(
        early_cadence.outcome,
        AutomaticSchedulerTickOutcome::Held(AutomaticSchedulerHoldReason::MinimumCadence { .. })
    ));
    let pool_before_scheduler = pool.clone();
    let early = scheduler
        .tick(
            &mut fixture.database,
            &pool,
            AutomaticSchedulerTick(14),
            input(&tables, &[]),
            columnar_policy(),
        )
        .expect("new evidence wakes the trial early");
    assert!(matches!(
        early.outcome,
        AutomaticSchedulerTickOutcome::Ran(AutomaticOrchestrationReport {
            stop_reason: AutomaticOrchestrationStopReason::ActiveTrial(_),
            ..
        })
    ));
    assert_eq!(pool, pool_before_scheduler);

    let projection_id = fixture.database.inspect_columnar_projections()[0]
        .projection_id
        .expect("projection id");
    fixture
        .database
        .compact_columnar_projection(projection_id)
        .expect("manual physical change makes trial stale");
    let before_retry = scheduler
        .tick(
            &mut fixture.database,
            &pool,
            AutomaticSchedulerTick(18),
            input(&tables, &[]),
            columnar_policy(),
        )
        .expect("unchanged evidence waits for periodic retry");
    assert!(matches!(
        before_retry.outcome,
        AutomaticSchedulerTickOutcome::Held(
            AutomaticSchedulerHoldReason::AwaitingTrialEvidenceOrRetry { .. }
        )
    ));
    let resolved = scheduler
        .tick(
            &mut fixture.database,
            &pool,
            AutomaticSchedulerTick(19),
            input(&tables, &[]),
            columnar_policy(),
        )
        .expect("periodic retry detects external staleness");
    assert!(matches!(
        resolved.outcome,
        AutomaticSchedulerTickOutcome::Ran(AutomaticOrchestrationReport {
            stop_reason: AutomaticOrchestrationStopReason::TrialBoundaryResolved,
            ..
        })
    ));
    fixture.close();
}

#[test]
fn renewal_requires_a_strict_window_advance_and_clear_does_not_acknowledge_it() {
    let mut fixture = TimelineFixture::create("phase13-renewal-gate");
    let mut pool = AdaptiveEvidencePool::default();
    for _ in 0..5 {
        pool.rotate_window().expect("advance to W5");
    }
    assert_eq!(pool.window_epoch(), AdaptiveEvidenceWindowEpoch(5));
    let recommendation = AutomaticEvidenceRenewalRecommendation {
        reason: AutomaticEvidenceRenewalReason::AuthoritativeLsmLayoutChanged,
    };
    let renewal =
        report(AutomaticOrchestrationStopReason::EvidenceRenewalRecommended(recommendation));
    let calls = Cell::new(0);
    let mut scheduler = AutomaticScheduler::new(scheduler_policy());
    scheduler
        .tick_with_runner(&pool, AutomaticSchedulerTick(100), || {
            calls.set(calls.get() + 1);
            Ok(renewal.clone())
        })
        .expect("enter renewal gate");
    assert_eq!(calls.get(), 1);

    let (_, feedback) = fixture
        .database
        .query_with_feedback("SELECT id FROM events")
        .expect("feedback report");
    pool.record_execution_feedback(&feedback)
        .expect("same-window report");
    scheduler
        .tick_with_runner(&pool, AutomaticSchedulerTick(10_000), || {
            calls.set(calls.get() + 1);
            Ok(report(AutomaticOrchestrationStopReason::StepLimitReached))
        })
        .expect("same-window evidence remains blocked");
    assert_eq!(calls.get(), 1);

    pool.clear();
    assert_eq!(pool.window_epoch(), AdaptiveEvidenceWindowEpoch(0));
    scheduler
        .tick_with_runner(&pool, AutomaticSchedulerTick(20_000), || {
            calls.set(calls.get() + 1);
            Ok(report(AutomaticOrchestrationStopReason::StepLimitReached))
        })
        .expect("clear remains blocked");
    assert_eq!(calls.get(), 1);

    for _ in 0..6 {
        pool.rotate_window().expect("advance beyond blocked epoch");
    }
    scheduler
        .tick_with_runner(&pool, AutomaticSchedulerTick(20_001), || {
            calls.set(calls.get() + 1);
            Ok(report(AutomaticOrchestrationStopReason::StepLimitReached))
        })
        .expect("strict window advance releases gate");
    assert_eq!(calls.get(), 2);
    fixture.close();
}

#[test]
fn every_hard_fault_blocks_future_runner_invocations() {
    let pool = AdaptiveEvidencePool::default();

    let mut envelope_scheduler = AutomaticScheduler::new(scheduler_policy());
    let envelope_calls = Cell::new(0);
    let exceeded = AutomaticOrchestrationStopReason::MaintenanceEnvelopeExceeded {
        granted: zero_budget(),
        run_remaining_before: zero_budget(),
        consumed: MaintenanceConsumption {
            actions: 1,
            ..MaintenanceConsumption::default()
        },
    };
    envelope_scheduler
        .tick_with_runner(&pool, AutomaticSchedulerTick(1), || {
            envelope_calls.set(envelope_calls.get() + 1);
            Ok(report(exceeded))
        })
        .expect("completed overrun returns its report");
    assert_eq!(
        envelope_scheduler.state().gate,
        AutomaticSchedulerGate::Faulted(AutomaticSchedulerFault::MaintenanceEnvelopeExceeded)
    );
    let held = envelope_scheduler
        .tick_with_runner(&pool, AutomaticSchedulerTick(u64::MAX), || {
            envelope_calls.set(envelope_calls.get() + 1);
            Ok(report(AutomaticOrchestrationStopReason::StepLimitReached))
        })
        .expect("fault remains held forever");
    assert_eq!(envelope_calls.get(), 1);
    assert_eq!(
        held.outcome,
        AutomaticSchedulerTickOutcome::Held(AutomaticSchedulerHoldReason::Faulted(
            AutomaticSchedulerFault::MaintenanceEnvelopeExceeded
        ))
    );

    let mut fixture = TimelineFixture::create("phase13-faults");
    let safe_state = fixture.database.automatic_safe_mode_state();
    let completed = fixture
        .database
        .run_automatic_safe_orchestration(
            &pool,
            input(&[], &[]),
            AutomaticMultiSafeModePolicy::default(),
        )
        .expect("one completed idle step");
    let completed_steps = completed.steps.clone();
    assert_eq!(completed_steps.len(), 1);
    let step_failure =
        AutomaticOrchestrationError::StepFailed(Box::new(AutomaticOrchestrationStepFailure {
            step_index: 1,
            completed_steps,
            maintenance_budget_before: zero_budget(),
            maintenance_consumed: MaintenanceConsumption::default(),
            maintenance_budget_remaining_before_failed_step: zero_budget(),
            safe_mode_state_before: safe_state,
            safe_mode_state_after: safe_state,
            source: AutomaticSafeModeError::AdmissionScopeTooLarge,
        }));
    let mut failed_scheduler = AutomaticScheduler::new(scheduler_policy());
    let error = failed_scheduler
        .tick_with_runner(&pool, AutomaticSchedulerTick(2), || Err(step_failure))
        .expect_err("step failure faults scheduler");
    let AutomaticSchedulerError::Orchestration(failure) = error else {
        panic!("expected orchestration failure")
    };
    assert!(matches!(
        &failure.source,
        AutomaticOrchestrationError::StepFailed(_)
    ));
    assert_eq!(failure.source.completed_steps(), completed.steps.as_slice());
    assert_eq!(
        failed_scheduler.state().gate,
        AutomaticSchedulerGate::Faulted(AutomaticSchedulerFault::StepFailed)
    );

    let overflow = AutomaticOrchestrationError::ConsumptionOverflow(Box::new(
        AutomaticOrchestrationConsumptionOverflow {
            step_index: 0,
            completed_steps: Vec::new(),
            maintenance_budget_before: zero_budget(),
            maintenance_consumed_before_step: MaintenanceConsumption::default(),
            maintenance_budget_remaining_before_step: zero_budget(),
        },
    ));
    let mut overflow_scheduler = AutomaticScheduler::new(scheduler_policy());
    overflow_scheduler
        .tick_with_runner(&pool, AutomaticSchedulerTick(3), || Err(overflow))
        .expect_err("consumption overflow faults scheduler");
    assert_eq!(
        overflow_scheduler.state().gate,
        AutomaticSchedulerGate::Faulted(AutomaticSchedulerFault::ConsumptionOverflow)
    );

    let before_invalid = overflow_scheduler.state();
    let invalid = AutomaticOrchestrationError::InvalidEnvelope(
        AutomaticOrchestrationInvalidEnvelope::ZeroSteps,
    );
    let mut invalid_scheduler = AutomaticScheduler::new(scheduler_policy());
    let invalid_before = invalid_scheduler.state();
    invalid_scheduler
        .tick_with_runner(&pool, AutomaticSchedulerTick(4), || Err(invalid))
        .expect_err("invalid envelope is configuration failure");
    assert_eq!(invalid_scheduler.state(), invalid_before);
    assert_eq!(overflow_scheduler.state(), before_invalid);
    fixture.close();
}

#[test]
fn scheduler_preserves_one_phase12_call_with_a_multi_step_gc_run() {
    let mut fixture = MultiGcFixture::create("phase13-multi-gc", 3);
    let pool = AdaptiveEvidencePool::default();
    let pool_before = pool.clone();
    let safe_mode_before = fixture.database.automatic_safe_mode_state();
    let mut scheduler = AutomaticScheduler::new(scheduler_policy());
    let tick = scheduler
        .tick(
            &mut fixture.database,
            &pool,
            AutomaticSchedulerTick(700),
            input(&fixture.table_ids, &[]),
            gc_policy(),
        )
        .expect("one scheduler invocation runs bounded Phase 12");
    let AutomaticSchedulerTickOutcome::Ran(orchestration) = tick.outcome else {
        panic!("first tick must run")
    };
    assert_eq!(orchestration.steps.len(), 4);
    assert_eq!(orchestration.maintenance_consumed.actions, 3);
    assert_eq!(
        orchestration.stop_reason,
        AutomaticOrchestrationStopReason::NoReadyWork
    );
    assert_eq!(pool, pool_before);
    assert_ne!(
        fixture.database.automatic_safe_mode_state(),
        safe_mode_before
    );
    assert_eq!(
        scheduler.state().gate,
        AutomaticSchedulerGate::Open {
            delay: AutomaticSchedulerDelayClass::Idle,
        }
    );
    fixture.close();
}

#[test]
fn invalid_phase12_envelope_is_exposed_without_scheduler_or_database_progress() {
    let mut fixture = TimelineFixture::create("phase13-invalid-envelope");
    let pool = AdaptiveEvidencePool::default();
    let mut scheduler = AutomaticScheduler::new(scheduler_policy());
    let scheduler_before = scheduler.state();
    let database_before = fixture.database.automatic_safe_mode_state();
    let invalid_input = AutomaticOrchestrationInput {
        scope: AutomaticAdmissionScope {
            table_ids: &[],
            calibration_classes: &[],
        },
        envelope: AutomaticOrchestrationEnvelope {
            max_steps: 0,
            per_step_maintenance_budget: zero_budget(),
            run_maintenance_budget: zero_budget(),
        },
    };
    let error = scheduler
        .tick(
            &mut fixture.database,
            &pool,
            AutomaticSchedulerTick(1),
            invalid_input,
            AutomaticMultiSafeModePolicy::default(),
        )
        .expect_err("invalid Phase 12 envelope");
    let AutomaticSchedulerError::Orchestration(failure) = error else {
        panic!("expected wrapped orchestration error")
    };
    assert!(matches!(
        failure.source,
        AutomaticOrchestrationError::InvalidEnvelope(
            AutomaticOrchestrationInvalidEnvelope::ZeroSteps
        )
    ));
    assert_eq!(failure.state_before, scheduler_before);
    assert_eq!(failure.state_after, scheduler_before);
    assert_eq!(scheduler.state(), scheduler_before);
    assert_eq!(
        fixture.database.automatic_safe_mode_state(),
        database_before
    );
    fixture.close();
}
