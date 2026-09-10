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
- Adaptive evidence admission failure is telemetry-only. It MUST NOT change an
  otherwise successful client result, protocol transaction state, or worker
  lifetime, and the Server MUST NOT retry, clear, rotate, schedule, or run
  maintenance in response.
- Wire responses MUST expose stable protocol domain values and errors, never
  internal Rust layouts, discriminants, debug strings, pages, or row locators.
