# Adaptive Operations Phase 13: cooperative scheduling policy

Phase 13 adds a caller-owned, synchronous invocation gate around the Phase 12
bounded runner:

```text
caller supplies one logical tick
    -> AutomaticScheduler decides Hold or RunNow
    -> at most one run_automatic_safe_orchestration call
    -> Phase 12 returns one typed terminal reason
    -> scheduler updates its fixed-size gate
```

Phase 12 answers how much automatic work one explicit run may perform. Phase
13 answers when a host should attempt such a run. Cadence and maintenance
budgets remain separate: logical ticks never become bytes, work units, or
actions, and the Phase 12 envelope is passed through unchanged.

This is cooperative scheduling, not concurrent or background execution. Core
remains synchronous and embeddable. The implementation adds no thread, async
function, channel, sleep, timer, wall clock, daemon, job queue, or second
database owner.

## Ownership and execution authority

`AutomaticScheduler` stores only an explicit `AutomaticSchedulerPolicy` and
an O(1) `AutomaticSchedulerState`. It owns neither `Database` nor
`AdaptiveEvidencePool`; `tick` borrows `&mut Database` and
`&AdaptiveEvidencePool` for one call. Reports remain detached caller-owned
values and the scheduler stores no history, SQL, candidates, or query results.

The scheduler's only production execution path is
`Database::run_automatic_safe_orchestration`. One tick makes zero or one call
to that Phase 12 runner. The runner may itself execute several bounded safe
steps, which does not create a second scheduler invocation. Phase 13 never
calls lane writers, `automatic_safe_step_multi`, query execution, evidence
ingestion, evidence rotation, or trial abandonment directly.

Consequently all earlier authorities remain unique:

- Phase 11 owns the four-lane cursor, lane readiness, and ready-age service;
- Phase 12 owns the structural step cap and per-step/run-wide resource bounds;
- Phases 1, 4, 5, 8, 9, and 10 own candidate revalidation, trials, and
  subsystem mutations; and
- the caller owns workload generation, feedback capture, evidence ingestion,
  and evidence-window rotation.

The scheduler does not discover or rank Columnar, Change Stream, LSM, or
calibration candidates. It reads only the Phase 12 terminal reason, logical
tick cadence, and an evidence progress token. It neither adjusts the caller's
four-lane policy nor changes the orchestration envelope.

## Logical ticks and explicit policy

`AutomaticSchedulerTick(u64)` is a caller-supplied scheduling progress
coordinate. The first tick may have any value and is immediately eligible to
run. Later ticks must be monotonic, but need not be consecutive. A lower tick
is a typed `OutOfOrderTick` error and changes neither scheduler nor database.
A duplicate tick returns `Held(DuplicateTick)` and cannot invoke Phase 12 a
second time. Cadence uses monotonic subtraction from `last_run_tick`, avoiding
`last + delay` overflow.

A scheduler tick is not `DatabaseCommitSeq`, `StorageDataVersion`, schema or
Columnar generation, a calibration epoch, or an evidence-window epoch. Core
does not interpret a tick as seconds or any other wall-clock unit. A host may
map ticks to event-loop opportunities or time outside Core, but that mapping
is not database truth and is not persisted.

`AutomaticSchedulerPolicy::new` requires four explicit logical-tick values:

- `minimum_ticks_between_runs`;
- `idle_retry_ticks`;
- `no_progress_retry_ticks`; and
- `trial_retry_ticks`.

The minimum must be at least one. Every retry value must be at least the
minimum. Invalid combinations are typed `AutomaticSchedulerPolicyError`
values, so an invalid policy cannot construct a scheduler. There is no
`Default`: only the caller knows what one logical tick means.

## Fixed-size state and cadence gates

`AutomaticSchedulerState` contains only:

```text
last_observed_tick: Option<AutomaticSchedulerTick>
last_run_tick:      Option<AutomaticSchedulerTick>
gate:               AutomaticSchedulerGate
```

The gate is one of:

- `Open { delay }`, with `Normal`, `Idle`, or `NoProgress` delay;
- `AwaitingTrialProgress { evidence }`;
- `AwaitingEvidenceRenewal { blocked_window_epoch, recommendation }`; or
- `Faulted(fault)`.

There are no vectors, maps, candidate ages, lane counters, or report histories
in scheduler state. `inspect_tick` and `tick` share the same pure gate
evaluation, so inspection cannot disagree with execution because of duplicate
cadence logic. Inspection does not mutate the scheduler, database, or pool and
never calls Phase 12.

Phase 12 terminal reasons map to gates as follows:

| Phase 12 terminal reason | Scheduler gate |
| --- | --- |
| `NoReadyWork` | `Open(Idle)` |
| `StepLimitReached` | `Open(Normal)` |
| `TrialBoundaryResolved` | `Open(Normal)` |
| `SelectedCandidateDidNotProgress` | `Open(NoProgress)` |
| `ActiveTrial` | `AwaitingTrialProgress` |
| `EvidenceRenewalRecommended` | `AwaitingEvidenceRenewal` |
| `MaintenanceEnvelopeExceeded` | hard fault |

`NoReadyWork` is a backoff rather than a permanent block because later DML may
create physical work without changing the evidence pool. Selected no-progress
uses its longer explicit backoff and is never retried in the same tick.
Step-limit and resolved-trial boundaries use normal cadence; a tick never
starts a second proactive run immediately after either boundary.

## Evidence-aware trial retries

`AdaptiveEvidencePool::progress_token` returns an allocation-free, read-only
`AdaptiveEvidenceProgressToken` containing:

```text
window_epoch
schema_generation
recorded_reports
```

The token avoids constructing the target and calibration vectors in the full
pool inspection on every tick. It is only an O(1) indication that aggregated
evidence may have changed. It is not a safety decision, evidence-quality
claim, target identity, or mutation authority.

When Phase 12 returns `ActiveTrial`, the scheduler saves the current token. A
changed token allows another Phase 12 evaluation after normal minimum cadence.
The new report may be unrelated to the exact trial; Phase 12 remains
responsible for that determination and may return `ActiveTrial` again, at
which point the scheduler saves the newer token.

Token change alone is insufficient for liveness. A manual Columnar compaction,
physical replacement, schema change, or other external mutation can make a
trial stale without adding evidence. Therefore unchanged evidence still gets
one bounded evaluation after `trial_retry_ticks`. The existing trial authority
then detects and clears stale state. This logical retry also prevents an
accidental token collision after a complete pool reset from stalling a trial
forever; it is not a workload polling loop.

Ordinary `query` and `query_with_feedback` remain non-ingesting operations.
Only an explicit caller call to `record_execution_feedback` changes the pool.
The scheduler never generates a query, records a report, or rotates a window.

## Evidence-renewal hard gate

`EvidenceRenewalRecommended` records the evidence window observed by the
Phase 12 run. No amount of elapsed logical time and no number of additional
reports in that same window can invoke the runner again. The gate is released
only when `pool.window_epoch()` is strictly greater than the blocked epoch;
normal minimum cadence still applies before the next run.

This makes renewal an observed fact rather than a caller acknowledgement.
`rotate_window`, or a valid schema advance that moves the existing evidence
epoch forward, naturally releases the gate. `clear()` resets the runtime pool
to W0 and therefore does not masquerade as renewal from a later blocked epoch.
The scheduler never calls either operation itself.

## Hard faults and errors

A completed Phase 12 action that returns `MaintenanceEnvelopeExceeded` moves
the scheduler to `Faulted(MaintenanceEnvelopeExceeded)`. Phase 12
`StepFailed` and `ConsumptionOverflow` errors move it to their corresponding
fault gates. Faulted schedulers hold indefinitely and never increase a budget
or retry automatically. The first version intentionally has no generic reset;
operator-controlled recovery can reconstruct a scheduler after understanding
the cause.

An invalid Phase 12 envelope is a configuration error, not a storage fault.
It is exposed as the original `AutomaticOrchestrationError::InvalidEnvelope`
with identical before/after scheduler state and no Phase 12 safe step.
`AutomaticSchedulerOrchestrationFailure` records the tick plus scheduler state
before and after an attempted run and owns the unchanged Phase 12 error. Thus a
`StepFailed` or `ConsumptionOverflow` retains its completed prefix and original
lower-level source.

Faulting scheduler state does not roll back the database. Any atomic action
completed by Phase 12 remains governed by its subsystem publication and
recovery contract. Scheduling and orchestration are not database transactions.

## Server boundary and future integration

Phase 13 does not modify `netbadb-server`, `netbadbd`, server metrics, protocol,
or deployment manifest. The current server uses connection threads that send
typed commands to one dedicated `netbadb-database-worker`, which exclusively
owns `Database`, session state, and transaction handles. A future host driver
must submit logical scheduling opportunities inside that same database-owner
domain:

```text
future host timer or event-loop opportunity
    -> existing database worker ownership domain
    -> Phase 13 logical tick
    -> Phase 12 bounded runner
```

It must not create a second maintenance thread or share `Database` through
`Arc<Mutex<_>>`. Server integration is deferred because server workload-to-
evidence capture is a separate explicit contract; Phase 13 does not silently
change feedback collection semantics.

The scheduler is unrelated to disjoint-`StorageId` writer scheduling. Phase 12
still executes safe steps serially, and active group commit, staged prepare,
schema writer, structural mutation, recovery-required, and other existing
authorities remain visible through the Phase 12 path. Phase 13 adds no parallel
maintenance or transaction execution.

## Runtime and compatibility

Scheduler state and logical ticks are runtime-only caller values. They are not
written to a catalog, WAL, coordinator log, or manifest. Reopening a database
does not mutate an external scheduler value; the recommended process-restart
behavior is to construct a new scheduler in canonical open state. Retaining an
old caller value cannot create storage authority because every later attempt
still passes through current-state Phase 12 revalidation.

Canonical Schema, Heap, BTree, Columnar, Change Stream v2, Coordinator, LSM
Manifest v2, LSM WAL v1, LSM SSTable v2, protocol v2, SDK Schema Spec v2,
Inspection JSON v7, and server manifest v4 are unchanged. There is no automatic
coordinator compaction, heap vacuum, checkpoint, physical design, schema
mutation, or persistence change.
