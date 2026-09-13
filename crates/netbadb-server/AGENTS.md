# Server session rules

- The server depends on `netbadb-core` and the language-neutral protocol; core
  and lower layers MUST NOT depend on the server.
- `SessionState` owns the protocol transaction lifecycle. Failed commit or
  rollback attempts MUST retain retryable transaction handles, and disconnect
  handling MUST explicitly resolve or report an active transaction.
- Session and database execution remain synchronous. Future async networking
  MUST stay outside the synchronous database core. Synchronous execution does
  not imply that all future database work must remain globally single-threaded.
- The current server execution model uses one dedicated database worker thread.
  While this topology remains in use, that worker MUST exclusively own the
  `Database`, every `SessionState`, and every transaction handle. Connection
  threads own sockets and exchange only typed Send-safe commands and responses;
  transport threads or async tasks MUST NOT directly own or mutate database,
  session, or transaction state.
- The single database worker is a current implementation boundary, not a
  permanent global single-threading requirement. A future explicit concurrency
  architecture MAY replace it with multiple synchronous execution owners or
  workers only if all of the following invariants remain true:
  - core, transaction, and storage APIs remain synchronous and async-runtime
    types do not enter those layers;
  - each `SessionState` and transaction handle has exactly one active execution
    owner at a time;
  - each authoritative `StorageId` has at most one active mutation owner;
  - concurrent mutation owners may execute in parallel only when their
    mutation-domain sets are disjoint, under an explicit ownership/scheduling
    design; multi-domain mutation acquisition remains deterministic; and
  - durability batching, coordinator ordering, and global commit visibility
    remain correct and deterministic.
  This rule does not authorize that concurrency today; introducing additional
  database execution workers requires an explicit architecture decision and
  tests for ownership, transaction lifecycle, durability, recovery, and global
  visibility invariants.
- Plaintext TCP listeners MUST remain loopback-only. Non-loopback listeners
  MUST use mandatory mutual TLS, and TLS authentication MUST complete before a
  database session is admitted to an execution owner. A malformed frame is
  connection-fatal; do not guess a request identity or attempt to resynchronize
  the stream.
- Keep transport authentication in connection handling, principal admission
  and authorization in the database execution owner (currently the dedicated
  database worker), and SQL access extraction at the typed core/compiler
  boundary. `SessionState`, Protocol v2, and persistent database layers MUST
  remain identity- and policy-unaware.
- Authorization MUST run after handshake sequencing and successful SQL
  compilation but before planning, execution, writer acquisition, or ANALYZE.
  Commit, Rollback, session close, and shutdown MUST remain available for safe
  cleanup regardless of table grants.
- Disconnect and shutdown cleanup MUST fully resolve all connection handlers
  and database execution owners before shutdown completes. Under the current
  thread-based topology, this means joining connection and worker threads. A
  failed session rollback MUST be treated as fatal to continued database
  service and MUST NOT be discarded so the process can continue serving
  requests.
- Connection admission, socket timeouts, and runtime metrics belong at the TCP
  runtime boundary. The connection-vector length is the admission-limit truth;
  metrics MUST NOT decide correctness.
- Result-row policy belongs in transport-neutral `SessionState`, but is checked
  only after core execution materializes the complete `QueryResult`. Do not
  describe it as an executor memory limit or statement timeout.
- Explicit adaptive feedback capture runs only inside the current Database
  execution owner, after authorization, and only for eligible autocommit Core
  queries. Its bounded `AdaptiveEvidencePool` belongs to that worker, never a
  connection thread, `SessionState`, protocol message, or `Database`.
- Explicit Server physical-design evidence belongs to the same Database
  execution owner but remains an independent bounded runtime beside, never
  inside, `AdaptiveEvidencePool`. One successful eligible query may feed both
  concrete telemetry consumers, but it MUST be bound, planned, and executed
  only once.
- Physical-design recommendation requests are read-only evaluations against
  current Database inventory. They MUST remain explicit worker commands and
  MUST NOT invoke DDL, scheduling, maintenance, identity reservation, or
  automatic evidence rotation.
- Programmatic physical-index proposals MUST be bound to the exact lifetime of
  the worker-owned Physical Design runtime. Evidence epoch values are
  window-local and MUST NOT be used as cross-restart provenance. A proposal
  MUST hold only a weak runtime identity, and apply MUST reject a dead or
  different identity before reading destination evidence or calling Core.
- Programmatic physical-index apply MUST run in the sole Database worker and
  delegate current-state validation and mutation to Core's typed apply API and
  existing named-index transaction. Server MUST NOT duplicate coverage,
  incarnation, schema, storage, evidence, naming, allocation, WAL, retry, or
  recovery authority. It MUST NOT create an automatic apply path; an operator
  wire path must satisfy the explicit approval rules below.
- Optional Physical Design mutation receipts MUST remain a bounded,
  programmatic-only, Server-owned NBMR journal in the sole Database worker.
  A durable Begin precedes every Server Index or Columnar apply control, and a
  durable coarse Outcome follows each definitive result. Receipt failure MUST
  NOT roll back Database truth, mutate evidence, poison client protocol state,
  or create another writer; ambiguous outcome failure gates later receipt
  controls until startup reconciliation. Public receipts MUST expose only
  bounded logical targets and MUST NOT expose the journal path, absolute
  Columnar recovery path, database incarnation, runtime token, SQL, principal,
  session, or network address.
- Programmatic Server Columnar apply MUST use a configured Server-owned
  placement namespace. The control caller supplies only a validated logical
  placement key, never an arbitrary filesystem path, and the resolved
  directory MUST be one direct child of the configured canonical root. Server
  owns only placement policy and runtime provenance; database, schema, storage,
  Change Stream, evidence, ProjectionId, NBPC, artifact, and recovery decisions
  remain Core authority. A Server Columnar proposal MUST expire with the exact
  Physical Design runtime that issued it.
- Durable physical-index mutation from the local operator plane MUST require
  explicit deployment authorization in addition to filesystem access.
- Durable physical-Columnar mutation from the local operator plane MUST require
  both the explicit Manifest v9 `allow_physical_columnar_apply` permission and
  the complete `physical_design.columnar_apply` placement policy. The policy
  root MUST already exist, MUST be revalidated before each worker command, and
  MUST never cross the operator wire as a path.
- NBOP v4 Columnar approval MUST contain an exact runtime token and evidence
  epoch, table ID, ordered columns, explicit mode, and logical placement key.
  The listener MUST check permission before forwarding, while the sole
  Database worker MUST perform exact-location retry recognition, mode/token/
  epoch/occupancy revalidation, fresh Core proposal, and immediate Core apply
  in one typed command. No automatic Change Stream enablement, maintenance,
  scheduling, retry, or evidence rotation may be introduced.
- A wire approval MUST bind both an operator/runtime lifetime and an exact
  Physical Design evidence epoch. Numeric evidence epochs alone are
  insufficient across daemon restart.
- Wire clients MUST NOT construct or serialize Core or Server proposal objects
  as mutation authority. The Database worker MUST derive the Core proposal and
  apply it inside one typed worker command.
- A stale runtime approval MAY recognize an already-created exact named index
  as an idempotent success, but MUST NOT authorize a new mutation in the new
  runtime.
- Deployment and operator exposure of physical-design advice MUST remain a
  projection of the worker-owned runtime. Operator recommendations are
  observation only: they MUST NOT reserve identity, generate DDL, build
  physical state, or create another scheduling or mutation authority. When one
  operator response exposes multiple observation domains, each domain MUST
  remain semantically independent.
- Adaptive evidence admission failure is telemetry-only. It MUST NOT change an
  otherwise successful client result, protocol transaction state, or worker
  lifetime, and the Server MUST NOT retry, clear, rotate, schedule, or run
  maintenance in response.
- A Server host driver MAY use wall-clock time only to offer logical adaptive
  scheduling opportunities. Each opportunity MUST enter the existing Database
  execution owner as a typed command; only that owner may call
  `AutomaticScheduler` and the Phase 12 runner.
- At most one adaptive tick may be pending per worker. Missed host intervals
  MUST coalesce and MUST NOT become a maintenance backlog or catch-up burst.
- Adaptive scheduler failure MUST NOT corrupt Native or PostgreSQL protocol
  state, fail an unrelated foreground request, kill the worker, or create a
  second Database owner. Evidence-window renewal and faulted-scheduler reset
  remain explicit operator-controlled runtime actions.
- Deployment Adaptive configuration is versioned through the strict current
  Server Manifest. Manifest decoding may construct existing Server/Core policy
  values but MUST NOT duplicate their safety or scheduling authority.
- Wire responses MUST expose stable protocol domain values and errors, never
  internal Rust layouts, discriminants, debug strings, pages, or row locators.
- The live operator plane MUST remain outside the Database owner. Local
  operator requests may reach Adaptive runtime only through the existing typed
  Server adaptive control path.
- A mutating operator protocol action MUST have explicit retry and outcome
  semantics. An ambiguous transport retry MUST NOT cause a second evidence
  rotation.
- The first operator plane is local Unix-domain only and MUST NOT be exposed as
  Native or PostgreSQL SQL or database-protocol traffic.
- Unix process-signal ownership belongs to `netbadbd`, not this library.
  `TcpServer::run`, `PostgresTcpServer::run`, database workers, sessions, and
  the operator listener MUST remain signal-unaware.
- Server-handle `is_finished` is a pure owned-thread lifecycle observation, not
  a health check. When an operator listener is configured, its termination
  MUST be included so a daemon cannot outlive a failed control plane.
- Daemon readiness MUST follow successful startup of the database worker, TCP
  listener thread, and configured operator socket. It MUST NOT expose operator
  paths or authorization identities or create a protocol-level health claim.
