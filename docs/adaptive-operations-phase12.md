# Adaptive Operations Phase 12: caller-driven bounded orchestration

Phase 12 adds one synchronous upper envelope around the existing multi-target
safe step:

```text
explicit caller envelope
    -> automatic_safe_step_multi (at most one mutation)
    -> account physical maintenance
    -> evaluate a typed hard boundary
    -> optionally invoke the same safe step again
```

The runner answers how much work one explicit invocation may perform. A
scheduler answers when such an invocation should happen. Phase 12 implements
only the former: it adds no thread, timer, clock, daemon, async runtime, retry
policy, workload generation, or background scheduling.

## API and structural bounds

`Database::run_automatic_safe_orchestration` accepts a borrowed
`AdaptiveEvidencePool`, one fixed `AutomaticAdmissionScope`, one fixed
`AutomaticMultiSafeModePolicy`, and an `AutomaticOrchestrationEnvelope`.
The envelope contains:

- `max_steps`, which must be in `1..=64`;
- a `per_step_maintenance_budget`; and
- a separate `run_maintenance_budget`.

Invalid zero or oversized step limits are typed errors and execute no safe
step. The hard maximum bounds the returned diagnostic vector as well as work
performed. The run is stack-local and has no identity, resume token, WAL, or
catalog entry.

The runner is deliberately a small bounded loop. It never inspects candidates
itself and never directly calls a Columnar, Change Stream, LSM, or calibration
writer. Every iteration calls `automatic_safe_step_multi` exactly once. That
existing primitive remains the sole owner of candidate discovery, readiness,
lane-local ready age, four-lane service, formal revalidation, trial handling,
and the one-mutation-per-step theorem. The runner has no second service cursor
or fairness state.

## Per-step and run-wide maintenance accounting

Before each safe step, the granted budget is the component-wise minimum of the
fixed per-step budget and current run-wide remaining budget:

```text
granted = min(per_step, run_remaining)
```

The minimum covers work units, read bytes, write bytes, and actions. Run-wide
remaining is never replenished between steps. A zero physical budget is not an
entry-level stop: the existing safe step may still select planner calibration,
which has no `MaintenanceConsumption`.

`AutomaticSafeModeReport::maintenance_consumption` is the single mapping from
one safe action to physical consumption:

```text
Columnar catch-up     -> AdaptiveExecutionReport.consumed
Columnar compaction   -> AdaptiveColumnarCompactionExecutionReport.consumed
Change Stream GC     -> AdaptiveChangeStreamGcExecutionReport.consumed
LSM maintenance      -> AdaptiveLsmMaintenanceExecutionReport.consumed
Calibration          -> zero
Eligibility change   -> zero
Trial-only step      -> zero
```

Run totals use checked addition for all four components, including actions.
Overflow is an orchestration error and retains the completed step reports; it
is never saturated into a credible total. Normal remaining-budget subtraction
occurs only when actual consumption fits the prior run-wide remainder.

Existing actions retain their own Phase 1/8/9/10 admission theorems. Phase 12
does not claim that every atomic action's actual device I/O is universally
hard-bounded. If a completed action reports consumption beyond either its
granted step envelope or run-wide remainder, the mutation remains completed,
the step records the actual consumption, and the run stops immediately with
`MaintenanceEnvelopeExceeded`. No later step is attempted.

Each `AutomaticOrchestrationStepReport` records the step index, run budget
before the step, effective grant, actual consumption, optional valid remaining
budget, and the complete nested `AutomaticMultiSafeModeReport`. The top-level
report retains the bounded step vector, initial budget, checked total, valid
remaining budget when representable, and typed stop reason.

## Deterministic terminal precedence

After each successfully returned safe step, boundaries are evaluated in this
order:

```text
1. checked aggregate consumption; overflow returns an error
2. MaintenanceEnvelopeExceeded
3. EvidenceRenewalRecommended
4. ActiveTrial
5. TrialBoundaryResolved
6. NoReadyWork
7. SelectedCandidateDidNotProgress
8. after max_steps iterations, StepLimitReached
```

This ordering preserves the most specific safety information. A physical
overrun is not hidden by a simultaneous renewal recommendation. Renewal is not
hidden by a newly created trial. A trial that remains active is distinguished
from one that resolved. The final allowed step may therefore return a trial or
renewal boundary rather than the less specific step limit.

`NoReadyWork` retains the final safe-step report and its typed blocked
candidates. A selected candidate that becomes stale, aborts, or otherwise
returns without a state-changing mutation terminates with
`SelectedCandidateDidNotProgress`. The runner does not retry it or fall through
to another lane. Phase 11 may already have committed its service opportunity,
and the orchestration layer never rolls that state back.

## Trial and evidence boundaries

An orchestration run never crosses an experiment lifecycle boundary. A
Columnar catch-up or compaction that installs a trial ends the run. A planner
calibration apply likewise ends the run before the same evidence could be used
to evaluate its new epoch.

If a trial exists at run entry, the runner invokes the existing safe step once
to allow Keep, Hold, Await, Revert, or Stale processing. It then stops whether
the trial remains active or resolves. Hold and Await return `ActiveTrial`;
Keep, Stale, and a non-renewing resolution return `TrialBoundaryResolved`.
Exact Columnar suppression returns the higher-priority
`EvidenceRenewalRecommended(ColumnarEligibilityChanged)`.

Every Phase 11 evidence-renewal recommendation is a hard orchestration boundary
in this first version:

- Columnar catch-up or compaction stops after `ColumnarPhysicalStateChanged`;
- exact eligibility suppression stops after `ColumnarEligibilityChanged`; and
- LSM flush or `compact_one` stops after
  `AuthoritativeLsmLayoutChanged`.

Consequently a flush cannot be followed by `compact_one`, and Columnar
catch-up cannot be followed by compaction, inside the same run. The caller may
explicitly call `AdaptiveEvidencePool::rotate_window`, collect fresh feedback,
and invoke a new orchestration run. The runner only borrows the pool: it never
records feedback, removes targets, clears aggregation, rotates a window, runs a
query, or abandons a trial.

Change Stream GC creates neither a trial nor a renewal recommendation. Several
independent safe reclamations may therefore run consecutively until the step
limit, run budget, no-ready boundary, or an error stops the run. Four-lane
cursor movement and ready-age ranking naturally carry across those nested safe
steps through the existing Database runtime state.

## Errors, completed prefixes, and transaction boundary

An `AutomaticSafeModeError` terminates the run immediately as
`AutomaticOrchestrationError::StepFailed`. The error retains all previously
completed step reports, checked consumption and remaining budget before the
failed step, plus safe-mode state before and after the failing call. The latter
shows whether Phase 11 committed a service opportunity before the lower
authority returned its error. The runner never rewinds that state and never
swallows corruption or recovery errors as no progress.

Automatic orchestration is not a database transaction. A stop reason or later
error only prevents another safe-step invocation. Earlier GC, LSM, Columnar,
or calibration mutations remain governed by their existing publication and
recovery contracts and are never orchestration-rolled-back.

The runner publishes no `DatabaseCommitSeq`, does not invent G as a run ID, and
does not modify schema. Phase 3E prepared-state blockers remain visible to the
existing candidate authorities; the loop cannot work around a blocked staged
Columnar, Change Stream, or LSM action.

## Compatibility and future scheduling

The narrow safe-step API, multi-target safe-step API, inspection, state query,
trial abandonment, four-lane service, and caller-owned evidence APIs are
unchanged. Orchestration reports are embedded runtime values and are not added
to Inspection JSON v7.

Canonical Schema, Heap, BTree, Columnar, Change Stream v2, Coordinator, LSM
Manifest v2, LSM WAL v1, LSM SSTable v2, protocol v2, and Schema Spec v2 are
unchanged. There is no orchestration persistence, automatic physical design,
`compact_full`, heap maintenance, schema mutation, or repartitioning.

Any future scheduler should invoke this bounded runner as its only execution
envelope. It must not replace candidate safety, lane service, budget
accounting, trial boundaries, or renewal boundaries established by Phases
1–12.
