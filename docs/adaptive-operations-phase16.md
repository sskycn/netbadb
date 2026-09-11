# Adaptive Operations Phase 16: Server adaptive driver foundation

## Mission and clock boundary

Phase 13 is a clock-agnostic cooperative scheduler. It accepts a typed logical
`AutomaticSchedulerTick`, decides whether that opportunity is usable, and may
invoke the Phase 12 bounded runner once. Phase 16 supplies those opportunities
from real Server host time:

```text
Native or PostgreSQL accept loop
    -> elapsed host interval
    -> typed logical AdaptiveTick command
    -> existing Database worker
    -> AutomaticScheduler::tick
    -> at most one Phase 12 run
```

`Instant` and `Duration` live only in the private Server host driver. They mean
"it is time to offer another opportunity," never "maintenance is safe." Core
still sees only `AutomaticSchedulerTick`; it receives no timestamp, deadline,
or elapsed duration. A logical tick is not `DatabaseCommitSeq`, a schema or
storage generation, or a promise that a precise amount of time passed.

The accept loop is the timing boundary because it already owns the nonblocking
listener lifecycle and its short `WouldBlock` poll. It never calls Core
maintenance. It sends one typed worker command and continues accepting,
reaping, handling control requests, and observing shutdown without waiting for
adaptive execution.

## One execution owner

The current Native `netbadb-database-worker` or PostgreSQL
`netbadb-postgres-database-worker` remains the sole execution owner. Each
driven worker owns this composition:

```text
ServerAdaptiveWorkerRuntime
    + ServerAdaptiveFeedbackRuntime
        + AdaptiveEvidencePool
    + ServerAdaptiveDriverRuntime
        + AutomaticScheduler
        + owned TableId scope
        + owned PlannerCalibrationClass scope
        + Phase 12 envelope
        + Phase 11 automatic policy
```

The host driver owns only cadence and pending-command state. Connection threads
own neither driver state nor scheduling. There is no adaptive execution thread,
`Arc<Mutex<Database>>`, shared pool, query-triggered tick, DML-triggered tick,
or commit-triggered tick. Query, tick, rotation, reset, and shutdown commands
execute in worker FIFO order, so adaptive work cannot race a transaction or
migrate its handle.

On a tick the worker temporarily constructs the borrowed Core scope from the
configuration's owned vectors and calls exactly:

```rust,ignore
scheduler.tick(
    database,
    feedback.pool(),
    tick,
    AutomaticOrchestrationInput {
        scope: AutomaticAdmissionScope {
            table_ids: &driver.table_ids,
            calibration_classes: &driver.calibration_classes,
        },
        envelope: driver.orchestration_envelope,
    },
    driver.automatic_policy,
)
```

One accepted worker tick performs zero or one scheduler invocation. Phase 13
continues to guarantee at most one Phase 12 invocation, while Phase 12 may run
several safe steps within the configured step, per-step maintenance, and
run-wide budgets. Phase 11 remains the lane and fairness authority; Phases
1–10 remain the only mutation authorities.

## Explicit modes and configuration

Both server builders have three mutually exclusive internal startup modes:

```text
Disabled
FeedbackOnly(ServerAdaptiveFeedbackConfig)
Driven(ServerAdaptiveDriverConfig)
```

`TcpServer::new` and `PostgresTcpServer::new` select `Disabled`.
`with_adaptive_feedback` retains the Phase 15 capture-only behavior and starts
no host clock or scheduler. `with_adaptive_driver` selects `Driven`; its config
contains the Phase 15 feedback limits, nonzero host interval, Phase 13 policy,
Phase 12 envelope, Phase 11 policy, and owned table/calibration scopes. A
second feedback call is neither necessary nor combined with another pool.

`ServerAdaptiveDriverConfig` deliberately has no `Default`. Its validated
constructor rejects a zero interval, invalid structural step bounds, duplicate
scope entries, scope cardinality beyond Core policy bounds, and structurally
invalid enabled lane policies. After Database open and before worker readiness,
every configured `TableId` must exist in the authoritative committed schema.
Scope is operator policy; it is not inferred from authorization grants,
connected principals, session count, or current workload.

Driven mode does not call `enable_global_visibility` or create a coordinator.
LegacyLocal databases continue serving; feedback admission may report
`GlobalVisibilityRequired`, and Core may find no authorized automatic work.
Those outcomes are observed rather than strengthened or fabricated by Server.

## Cadence, coalescing, and exhaustion

The first opportunity is offered only after one complete `tick_interval`.
Accepted opportunities are numbered T1, T2, T3, and so on using checked
arithmetic. The interval is minimum host-time spacing between offers, not an
exact mapping between ticks and seconds.

The host records an opportunity as pending before another can be offered. Its
worker reply is polled asynchronously by the accept loop:

```text
interval due and no pending tick
    -> submit one tick
    -> continue host loop
while pending
    -> submit none
reply observed
    -> clear pending
```

If several intervals elapse while the worker handles a query or adaptive run,
they coalesce. Completion permits at most one later opportunity; no missed
interval is replayed and no burst or timer backlog is created. When the logical
counter submits `u64::MAX`, the host enters an observable exhausted state and
offers no further ticks. Foreground service and worker lifetime continue.

Cadence invariants are tested against synthetic elapsed `Duration` values; the
implementation needs no clock trait. Small real-loop integration tests prove
that each transport eventually delivers an opportunity to its worker.

## Evidence renewal, trials, and fault recovery

Foreground eligible queries synchronously feed the same worker-owned pool the
scheduler borrows. This naturally lets later host ticks observe active-trial
evidence progress. With no new evidence, continuing logical ticks retain Phase
13's `trial_retry_ticks` stale-trial checks.

An `EvidenceRenewalRecommended` stop remains a hard Phase 13 gate. Later ticks
continue arriving so the scheduler can observe a schema-driven window advance,
but same-window reports cannot release the gate. The background path never
calls `rotate_window`. Operators must call `rotate_evidence`, which is routed
to the worker and preserves the Phase 7 ordering, lineage, generation, storage,
and calibration guards while clearing only aggregation payload.

An adaptive execution error is counted and left in the scheduler's typed fault
gate. It is not a worker-fatal error, Native Error frame, PostgreSQL statement
error, failed transaction, or `awaiting_sync` transition. Foreground calls
continue under existing Database safety behavior.

`reset_faulted_scheduler` is explicit acknowledgement and succeeds only when
the Phase 13 gate is `Faulted`. It rebuilds `AutomaticScheduler` with the same
policy and does not mutate the pool, Database, active trial, or run a tick.
`AwaitingEvidenceRenewal` and normal cadence/backoff states are not faulted, so
reset cannot bypass renewal, trial, or scheduling gates.

## Programmatic operator control

`ServerHandle` and `PostgresServerHandle` expose the same
`ServerAdaptiveControlHandle` methods:

```text
status()
rotate_evidence()
reset_faulted_scheduler()
```

The public handle sends requests to the accept-loop boundary, which forwards
private typed commands. It never exposes or retains a raw worker sender, owns
no adaptive state, and cannot keep a stopped worker alive. Calls after shutdown
return a typed stopped error.

Status is a fixed-size snapshot: mode; Phase 15 counters, evidence progress and
pool health; scheduler state; last submitted tick; pending/exhaustion flags;
run/hold/error counters; and the last orchestration stop reason. It retains no
report history, candidate traces, SQL, parameters, rows, identities, session
IDs, addresses, or certificate data. Reading status changes no scheduler gate,
ready age, evidence, Database state, or global commit sequence. Diagnostics are
observability only and never participate in admission or safety decisions.

## Lifecycle, compatibility, and limitation

Shutdown stops new submissions, closes and joins connection threads, then
serializes worker shutdown after any already queued adaptive tick. A dropped
pending reply cannot detach the worker. Restart constructs a fresh empty W0
pool, initial scheduler, and T1 host clock; physical maintenance already
committed to persistent storage remains committed. Runtime reset is not
orchestration rollback.

Automatic runs are synchronous worker commands. They can increase latency for
foreground commands queued behind a bounded Phase 12 run. Phase 16 deliberately
does not introduce another Database owner, parallel automatic mutations,
disjoint-domain writer concurrency, async Core, load-aware budgets, dynamic
lane ordering, or automatic evidence sampling.

Server Manifest v4 stays strict and unchanged. `netbadbd` has no adaptive flag
or environment variable and therefore remains disabled. Protocol v2,
PostgreSQL wire, SQL syntax, Inspection JSON v7, SDK schema, and every durable
format are unchanged. Phase 17 can begin from the proven runtime API to design
deployment configuration and external observability without changing the
ownership or safety theorem established here.
