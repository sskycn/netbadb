# Adaptive Operations Phase 15: Server workload evidence bridge

## Mission and boundary

Phase 15 lets an explicitly configured Server execution owner turn successful
autocommit Core queries into bounded workload evidence:

```text
authorized typed Core query
    -> autocommit eligibility
    -> Phase 14 prepared feedback execution
    -> unchanged user ExecutionResult
       + exact ExecutionFeedbackReport
    -> immediate AdaptiveEvidencePool admission in the same worker
```

This is capture only. It adds no scheduler, Phase 12 orchestration, automatic
maintenance, evidence-window rotation, timer, logical tick, sampling policy,
or second database worker. Phase 14 remains the unique prepared capture
primitive, so one dependency validation, bind, plan, and execution produce
both the user result and its report.

## Explicit programmatic opt-in

`ServerAdaptiveFeedbackConfig::new(AdaptiveEvidencePoolLimits)` is an explicit,
bounded runtime configuration. Native and PostgreSQL hosts opt in independently:

```rust
TcpServer::new(config).with_adaptive_feedback(adaptive_config);
PostgresTcpServer::new(config).with_adaptive_feedback(adaptive_config);
```

Both `new` constructors leave capture disabled. The disabled path constructs
neither `AdaptiveEvidencePool` nor executor feedback statistics. The config has
no milliseconds, scheduler cadence, maintenance budgets, four-lane policy, or
automatic-action flags.

Deployment `ServerConfig` and strict, deny-unknown-fields Server Manifest v4
are unchanged. Consequently `netbadbd`, which still constructs the default
server from Manifest v4, cannot enable Phase 15 capture. A future deployment
setting requires an explicit Manifest v5 design; Phase 15 productionizes only
the embedded programmatic Server opt-in.

## Worker ownership and lifetime

Each existing execution topology owns its own optional runtime:

```text
Native DatabaseWorkerState       PostgreSQL worker runtime
    + Database                       + Database
    + session map                    + PgWorkerSession map
    + AdaptiveEvidencePool           + AdaptiveEvidencePool
```

The worker is Core's caller and therefore satisfies the caller-owned evidence
theorem. There is one pool per current Database owner, not per connection or
session, and Native and PostgreSQL do not share a cross-transport pool because
they currently create independent workers and Databases. `Database`,
`SessionState`, `DatabaseSession`, connection threads, identities, and protocol
messages own no pool. No `Arc<Mutex<Database>>`, `Arc<Mutex<AdaptiveEvidencePool>>`,
global singleton, ingestion thread, or adaptive database thread is introduced.

`ServerAdaptiveFeedbackRuntime` is created with its worker, records evidence in
worker command order, and is dropped on worker shutdown. It is not flushed,
serialized, checkpointed, recovered, or persisted. A restart constructs an
empty W0 pool with fresh diagnostics; retained database state is unaffected.

## Shared capture bridge and eligibility

Native and PostgreSQL call the same internal
`execute_prepared_with_optional_server_feedback` helper after their existing
authorization checks. The helper owns only execution-path selection and
evidence ingestion. It does not authorize, frame Native responses, encode
PostgreSQL rows, or map protocol errors.

The typed execution-time predicate is:

```text
capture configured
AND DatabaseSession owns no explicit transaction
AND PreparedStatement::description().is_query
```

Disabled capture, an active explicit transaction, and relational DML all use
the ordinary `DatabaseSession::execute_prepared` path. That method remains the
authority that chooses `Database::execute_prepared_in` for an owned transaction
or `Database::execute_prepared` otherwise. DDL remains on its existing prepared
DDL path. No SQL prefix matching, reparsing, literal reconstruction, double
binding, or execute-ordinary-then-execute-feedback sequence exists.

Eligibility becomes an admitted workload sample only after Core successfully
returns `PreparedExecutionWithFeedback`. A Core execution error returns its
unchanged `DatabaseError` and admits no report. The query report is passed once
to `AdaptiveEvidencePool::record_execution_feedback` and then dropped; no raw
report history or QueryResult history is retained.

## Authorization and Native integration

Native `WorkerSession` preserves this order:

```text
prepare typed statement
    -> authorize StatementAccess
    -> shared feedback-aware execution bridge
    -> existing SessionState response construction
```

A denied query performs no planning, execution, or adaptive mutation. The
small response-path refactor keeps the existing `ExecutionResult`, row-limit,
database-error, transaction-state, and Protocol v2 mapping as the only Native
mapper. `BEGIN; SELECT ...; COMMIT` executes through the ordinary transaction
path and produces no database-global workload sample. Multiple authorized
Native sessions feed the one worker pool in serialized command order.

## PostgreSQL integration and portals

PostgreSQL connects the shared bridge inside `execute_prepared_core`, after
`authorize_access`. Simple Query and the first Extended Query portal Execute
therefore share the same policy. Parse, Bind, Describe, and Close do not execute
Core and cannot capture.

`execute_portal` invokes Core only while `portal.result.is_none()`. The first
Execute materializes and caches the complete result and records exactly one
report. Later Execute calls only emit remaining cached rows, so cursor resume
cannot plan, execute, or ingest again. Closing a portal before its first Execute
captures nothing.

`SHOW`, `version()`, `current_database()`, and typed `pg_catalog` compatibility
operations stay in their existing server-side compatibility paths. They do not
carry a Core `PreparedStatement`, never call the bridge, and are not NetbaDB
relation workload evidence. DDL and DML also remain excluded. PostgreSQL
transaction status is not used as a duplicate eligibility authority; the
`DatabaseSession` transaction handle is authoritative.

## Core-success boundary and failure isolation

Capture occurs when Core successfully completes the real query, before Native
result-row framing or PostgreSQL row encoding. A later row-limit or transport
encoding/delivery failure does not undo evidence, because the database work
already occurred. Conversely, a Core execution failure has no report and no
partial Server-created feedback.

Every typed evidence admission error—including `GlobalVisibilityRequired`,
stale schema/calibration/target evidence, out-of-order visibility, retired
identity, and exhausted window epoch—is telemetry failure only. The Server
drops that report, records a diagnostic, returns the successful query result,
and continues serving. It does not retry admission, clear the pool, rotate the
window, rewrite ordering, or kill a worker. A failed admission cannot emit a
Native Error, call PostgreSQL `record_error`, mark a transaction failed, or set
Extended Query `awaiting_sync`.

LegacyLocal mode is deliberately supported as best-effort capture: the Core
query succeeds, its report has no global G, admission returns
`GlobalVisibilityRequired`, diagnostics record that rejection, and the client
still receives success. Enabling capture never calls
`Database::enable_global_visibility` and never rejects Server startup.

Capacity outcomes (`RecordedWithCapacityRejection` and
`SchemaRotatedWithCapacityRejection`) are successful telemetry admissions and
cannot fail the query. They increment capacity diagnostics and may leave pool
health at `RotationRecommended`; the Server still performs no automatic
rotation. Executor overflow or incomplete reports similarly retain their typed
incomplete evidence semantics. The Server records their diagnostic count but
does not reject the query or invent stronger evidence authority.

## Diagnostics, ordering, and privacy

The private worker runtime keeps fixed-size saturating counters for eligible
successful queries, admission successes/errors, capacity rejection, schema
rotation, and incomplete reports, plus an overflow flag and the last typed
admission outcome/error. Tests can inspect those counters together with the
pool progress and health. Diagnostics never decide readiness, mutate evidence,
or affect query success; counter overflow saturates without panic. No public
metrics, protocol endpoint, PostgreSQL notice, or Inspection JSON field is
added.

Immediate synchronous admission preserves worker order and the pool's
nondecreasing `DatabaseCommitSeq` rule. Repeated reads at G100 are legal, a DML
may advance to G101 without producing evidence, and the next query records at
G101. A greater schema generation naturally invokes the pool's existing schema
cohort rotation during the next query admission; the Server does not rotate it
manually.

The runtime retains only the bounded typed aggregation already defined by
`AdaptiveEvidencePool`. It stores no SQL, literals, parameter payloads, result
rows, `SessionId`, username, certificate fingerprint, client address, request
ID, or principal label. Authorized queries from multiple principals may
contribute to database workload evidence, but the aggregation is never a
per-user profile.

## Compatibility and future entry

Phase 15 changes no Canonical Schema, Heap, BTree, LSM Manifest v2/WAL v1/
SSTable v2, Columnar, Change Stream v2, coordinator, Protocol v2, PostgreSQL
wire, SDK Schema Spec v2, Inspection JSON v7, SessionPolicy, authorization, or
Server Manifest v4 contract. Query result limits, transaction lifecycle, and
Phase 3G durability remain on their existing paths; SELECT capture does not
flush pending Change Stream Finalize, checkpoint, resolve group state, or
acquire a maintenance writer.

A future Server host-driver phase may define deployment configuration, logical
scheduling opportunities, and operator observability. It must submit those
opportunities inside the same Database-owner domain and preserve Phase 13's
caller-driven scheduler contract. Phase 15 creates the bounded workload
evidence that such a driver may later consume; it does not create the driver.
