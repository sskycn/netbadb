# Adaptive Operations Phase 14: prepared execution feedback

Phase 14 closes the production prepared/parameterized execution-observability
gap. Embedded callers can explicitly observe an autocommit prepared execution
without creating a second execution architecture:

```text
PreparedStatement + ScalarValue parameters
    -> existing dependency validation
    -> existing parameter binder
    -> one concrete logical statement
    -> one physical plan from one immutable planning snapshot set
    -> one execution with opt-in executor counters
    -> PreparedExecutionWithFeedback
```

It adds no background collection, Server capture, scheduler driver, evidence
pool ownership, transaction-scoped feedback, protocol field, metric, or
manifest setting.

## Public contract and execution theorem

`Database::execute_prepared_with_feedback` is an additive synchronous
autocommit API. It returns `PreparedExecutionWithFeedback`, whose `result` is
the ordinary `ExecutionResult` and whose `feedback` is one of:

- `PreparedExecutionFeedback::Query(ExecutionFeedbackReport)` for a successful
  prepared query; or
- `PreparedExecutionFeedback::NotApplicable(Mutation)` for a successful
  INSERT, UPDATE, or DELETE.

Absence is typed because mutation completion is meaningful but does not
constitute query access-work evidence. A failed query or mutation returns the
existing `DatabaseError`; it never returns a successful wrapper with invented
or partial evidence.

The implementation has one validation, bind, plan, and execution path per
call. Dependency validation and `bind_statement` are the same production
operations used by `execute_prepared`, so parameter count/type errors and
stale prepared dependencies retain their existing categories. Binding creates
one ephemeral concrete logical statement. Core then collects one immutable set
of table statistics, access paths, partition snapshots, Columnar snapshots,
and the current calibration profile. That same set selects the physical plan
and produces its planner access estimates. The selected plan executes once.

Ordinary `execute_prepared` remains the disabled instrumentation path. It does
not construct `ExecutionStatistics`, feedback vectors, Query Shape, or planner
estimate vectors. Both APIs share the existing binder, planner inputs, query
executor, and the same private autocommit mutation helper. The mutation helper
owns the unchanged implicit transaction, commit, and rollback behavior, so the
feedback wrapper cannot execute DML twice or introduce a special durability
path.

`query_with_feedback(source)` keeps its Phase 2 SQL contract, including query
rejection and existing Columnar quarantine/retry behavior. It and the new
prepared API share the private planned-query feedback helper that creates the
anchor, runs the real instrumented executor, and correlates evidence. The
prepared path neither reconstructs nor reparses SQL and does not add a new
Columnar retry policy to ordinary prepared execution.

## Query identity and evidence authority

For parameterized execution, Query Shape comes from the original resolved and
typed prepared logical query, before scalar binding. `ParameterId` and its
`ExprType` therefore remain structural identity while parameter payload is
absent. Executions of `WHERE id = $1` with different values share the same
parameterized shape; they are not reclassified as literal queries. The bound
value is used only in the ephemeral concrete statement that the production
planner and executor consume. Neither reports nor `AdaptiveEvidencePool`
retain SQL, parameter values, literals, or result rows.

Plan Variant comes from the exact concrete physical plan. `PlanNodeOrdinal`
continues to be physical-preorder, in-memory, exact-plan-local correlation
identity. Core matches each executor sample to a planner estimate by ordinal,
access kind, table, storage, and access-path identity. It does not substitute
a prepared-statement ID, parameter ID, SQL hash, pointer, or durable identity.

Executor remains the authority for raw SeqScan, IndexPoint, IndexRange,
partition subscan, Filter, and Columnar counters. Core never derives work by
rescanning returned rows. Core converts matching raw evidence and invokes the
planner's existing `evaluate_actual_access_work`; there is no prepared-only
cost formula. Columnar reports retain the exact table, storage, relation
binding, projection, generation, and node identities. Counter overflow and
incompleteness retain Phase 2 semantics: evidence is marked incomplete without
changing query results, while a correlation or execution error remains an
error rather than a fabricated complete report.

The feedback anchor uses the autocommit read view's optional
`DatabaseCommitSeq` and current `SchemaGeneration`, exactly like source-string
feedback. A successful SELECT does not publish a new G. Feedback capture does
not flush WAL or Change Stream checkpoints, run maintenance, advance a Change
Stream frontier, resolve group state, or tick the scheduler.

## Explicit evidence ingestion and scheduling

The API returns a detached report. `AdaptiveEvidencePool` remains a
caller-owned runtime value and is never stored in `Database`:

```rust
let executed = database.execute_prepared_with_feedback(&prepared, &values)?;
if let Some(report) = executed.feedback.query_report() {
    pool.record_execution_feedback(report)?;
}
```

Execution alone leaves `AdaptiveEvidenceProgressToken` unchanged. Only the
explicit record call increments `recorded_reports` and can wake a Phase 13
`AwaitingTrialProgress` gate through its ordinary token-change rule. The
scheduler does not distinguish prepared reports from source-string reports.
Likewise, reports added to the same window cannot bypass
`AwaitingEvidenceRenewal`; only a strictly newer evidence-window epoch releases
that hard gate. No query path calls `rotate_window`, safe orchestration, or a
maintenance writer.

## Transaction-scope contract

Phase 14 deliberately does not add `execute_prepared_in_with_feedback`.
Explicit transactions may observe staged writes and transaction-local rows,
an older or statement-specific read snapshot, and multi-statement dependencies
whose physical state has not become database-global. Their observations can
also be affected by work that later rolls back. `AdaptiveEvidencePool` is
currently database-global runtime workload evidence used for calibration and
Columnar generation evaluation. Silently mixing those scopes would require a
new evidence-authority theorem defining at least:

- which transaction snapshot and staged-write provenance a report carries;
- whether and when commit, rollback, or retry makes the observation eligible;
- how transaction-local physical identities relate to published schema,
  storage, Columnar generations, and G ordering;
- whether dependent statements may be aggregated independently; and
- how global calibration avoids learning from uncommitted or nonrepresentative
  state.

Until that theorem exists, explicit-transaction execution remains the ordinary
`execute_prepared_in` path with no adaptive report. A future diagnostic-only
transaction report could be useful, but it would need a type and ownership
boundary that prevents admission to `AdaptiveEvidencePool`; Phase 14 does not
declare such a design safe or implement it.

## Server audit and deferred integration

The Native and PostgreSQL transports currently have separate worker
implementations; there is no single common Server worker path.

Native requests enter `DatabaseWorker`/`WorkerSession`. `WorkerSession` first
prepares and authorizes a relational statement, then `DatabaseSession` chooses
`execute_prepared` for autocommit or `execute_prepared_in` when it owns an
explicit transaction. A future Phase 15 Native connection point is therefore
the post-authorization autocommit relational-query branch inside the current
database-owner worker. Explicit transaction queries must keep the ordinary
path.

PostgreSQL uses the independent `PgDatabaseWorker`/`PgWorkerSession` path.
Simple Query prepares, authorizes, and executes through
`execute_statement`/`execute_prepared_core`. Extended Query stores Core's typed
artifact at Parse, decodes values at Bind, and first executes the portal through
`execute_to_portal`/`execute_prepared_core`. A future Phase 15 PostgreSQL
connection point is that successful, first autocommit, read-only Core execution;
portal resume must not execute or ingest again. Compatibility-catalog queries
and DDL are separate paths and are not Core workload feedback.

The recommended first Server eligibility rule is:

```text
successful autocommit read-only Core query -> capture eligible
explicit transaction query                -> ordinary execution only
DML or DDL                                -> no workload feedback
```

Authorization must still precede plan/execution/feedback. A bounded telemetry
pool rejecting evidence because of capacity, truncation, incompleteness, or
ordering must not turn an otherwise successful user query into a query failure;
execution correctness and telemetry admission are separate future Server
outcomes.

Phase 15 must decide pool lifetime and ingestion policy. If a worker is the
current unique `Database` execution owner, the pool should follow that
Database-owner runtime rather than a connection thread. This recommendation
does not assume one global worker forever: the Core API is synchronous, returns
owned detached values, and contains no worker-global pointer, `Arc`, or
`Mutex`, so a future explicit multi-owner architecture can assign each
authoritative domain its own correct runtime owner.

Phase 14 makes no Server code change. `SessionPolicy`, Server metrics, Native
protocol v2, PostgreSQL wire behavior, and deny-unknown-fields Server Manifest
v4 remain unchanged. It does not create Manifest v5 or choose a server-global,
per-worker, or per-transport evidence pool.

## Durability and compatibility

Prepared SELECT feedback is read-only observability and does not disturb Phase
3G pipelined Change Stream Finalize state. It neither flushes pending Finalize
checkpoints nor changes their retention blockers. Prepared DML uses the exact
ordinary autocommit transaction/WAL/Change Stream/G path and returns typed
`NotApplicable(Mutation)` only after successful completion.

`query`, `query_with_feedback`, `execute_prepared`, `execute_prepared_in`,
`PreparedStatement`, and `PreparedSqlStatement` keep their signatures and
meaning. Phases 1-13, caller-owned evidence, scheduler state, and foreground
synchronous behavior remain intact. Canonical Schema, Heap, BTree, LSM
Manifest v2/WAL v1/SSTable v2, Columnar, Change Stream v2, Coordinator,
protocol v2, SDK Schema Spec v2, Inspection JSON v7, and Server Manifest v4
formats are unchanged.

## Deliberate limitations and Phase 15 entry

Phase 14 does not expose transaction-local feedback, automatically ingest
evidence, persist feedback, collect latency, invent join/sort/aggregate costs,
drive orchestration, or wire either Server transport. Phase 15 can start by
adding transport-neutral autocommit eligibility at each of the two existing
worker execution points, keeping authorization and result semantics unchanged,
and explicitly handling telemetry-admission failure as non-query-fatal.
