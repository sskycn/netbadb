# NetbaDB roadmap

## Phase 0 — Rust foundation (complete)

- Cargo workspace and dependency direction;
- canonical IDs, physical types, semantic types, and schema metadata;
- initial query parser;
- typed HIR with name resolution and nominal type checks;
- typed relational IR;
- logical-to-physical planner boundary;
- synchronous executor and error enums;
- tests for parser, type checking, planning, execution, and storage round trips.

## Phase 1 — Storage Foundation (complete)

- fixed 4 KiB pages with explicit version, page type, and slotted-page bounds;
- checked page allocation, offset arithmetic, raw page I/O, and heap metadata;
- bounded synchronous buffer pool with guards, pin/unpin, dirty writeback,
  flush, eviction, and pinned-page exhaustion errors;
- heap insert and scan through the buffer boundary, including multi-page and
  close/reopen behavior;
- deterministic page, buffer, heap, corruption, eviction, and vertical-slice
  tests.

The database file format remains experimental. The legacy `NBPG` container
marker is retained, while heap metadata and data-page layouts are explicitly
versioned. The pre-Foundation sequential `HEAP` data-page layout is not
migrated.

## Phase 2A — Transaction + WAL Core (complete)

- strong transaction IDs and LSNs plus explicit and implicit transaction APIs;
- active, commit-pending, committed, and aborted lifecycle states;
- separate retained WAL with versioned, bounded, little-endian Begin,
  PageUpdate, Commit, and Abort records;
- per-transaction prevLSN chains and full-page before/after images;
- page format version 2 with persistent pageLSN and explicit version 1
  rejection;
- append-versus-durable tracking and commit-record durability;
- WAL-before-data-page flush and eviction, including active-writer and I/O
  failure tests;
- clean close/reopen and multi-page transaction tests.

Phase 2A's original runtime was non-isolated: uncommitted changes were not
hidden and abort itself did not synchronously roll pages back. Its original WAL
format had no checksum; the current experimental WAL v3 adds integrity checks.

## Phase 2B — Crash Recovery (complete)

- synchronous startup recovery before buffer-pool exposure;
- analysis into Commit winners and incomplete/Abort losers;
- repeat-history redo in ascending LSN with pageLSN skipping;
- global descending-LSN loser undo through prevLSN chains and before-images;
- exact trailing-page allocation/removal without page-ID gaps;
- structurally valid incomplete-final-record truncation with hard errors for
  corruption, incompatible versions, broken chains, and malformed images;
- deterministic restart, idempotency, and interrupted redo/undo tests.
- single-writer enforcement and rejection of committed updates that depend on
  earlier loser contents in retained WAL.

Phase 2B intentionally had no MVCC, isolation, checkpoints, WAL recycling,
bounded WAL growth, or runtime full rollback guarantee.

## Phase 2B.1 — Single Writer + Runtime Rollback (complete)

- lazy first-write ownership with read-only transactions admitted concurrently;
- explicit Active, RollbackRequired, CommitPending, RollbackPending,
  Committed, and RolledBack states (RollbackRequired was added in Phase 4B);
- retryable durable commit and durable Abort followed by synchronous physical
  before-image undo;
- reverse-prevLSN rollback, including exact reverse removal of newly allocated
  trailing pages;
- durable RollbackComplete records after runtime or startup rollback pages are
  synchronized;
- recovery-safe interruption, failed commit/rollback writer retention, dirty
  writer Drop poisoning, and unresolved-writer close errors;
- regression tests preventing later winners from depending on loser page
  images.

This is a single-writer STEAL/NO-FORCE model. Reads are not isolated and may
observe an active writer. There is still no MVCC, checkpoint, WAL recycling,
bounded WAL growth, or concurrent-writer scheduling.

## Phase 2C — Checkpoint + WAL Lifecycle (complete)

- explicit zero-outstanding-transaction quiescent checkpoints;
- WAL format v2 generation metadata with logical base LSN, checkpoint boundary,
  and next-TxnId high-water mark;
- two-slot crash-safe generation selection and recycling with a previous
  generation retained only across an interrupted cleanup;
- bounded recovery input containing only post-checkpoint records;
- monotonic LSN/pageLSN behavior across repeated recycling;
- deterministic rotation-failure, generation-corruption, recovery-range,
  TxnId, close/reopen, and bounded-growth tests;
- deterministic subprocess termination coverage for STEAL/NO-FORCE recovery,
  commit/rollback boundaries, interrupted recovery, and WAL rotation windows.

Clean-shutdown metadata is intentionally omitted because recovery already scans
only one bounded generation, while safely invalidating a clean marker before
the next mutation would add another persistent state machine. Phase 2C remains
synchronous and explicit: there is no fuzzy checkpoint, background policy, WAL
archive, replication, or PITR. Subprocess termination tests model abrupt process
loss without Rust destructors, not machine or storage-device power loss.

## WAL integrity hardening (complete)

- WAL format v3 keeps the 48-byte generation header and adds a whole-header
  CRC32C in bytes 40..44;
- record format v2 replaces the redundant payload length with a whole-record
  CRC32C, preserving the 40-byte record header and existing record sizes;
- bounded framing and type-derived length checks run before allocation, while
  checksum verification precedes LSN, transaction-chain, and page-image
  semantics;
- physically incomplete, structurally valid final records remain recoverable
  crash tails, while complete checksum failures are hard corruption errors;
- golden vectors, semantic/payload mutation tests, generation corruption,
  truncation boundaries, and a file-level WAL recovery fuzz target cover the
  decoder.

WAL v2 and record v1 remain unsupported experimental formats. At this
WAL-integrity-hardening phase, heap metadata remained v2; the current heap
metadata format is v3. Canonical schema encoding remains v1.

## Data-page integrity hardening (complete)

- page format v4 expands the header from 24 to 28 bytes while preserving every
  existing semantic field offset through pageLSN;
- bytes 24..28 store a little-endian CRC32C over the expected little-endian
  PageId plus the complete 4096-byte page, with the checksum field zeroed;
- every successful semantic page mutation refreshes integrity, while failed
  mutations remain byte-for-byte unchanged and the all-zero new-page
  before-image remains a non-page WAL sentinel;
- recovery validates checksum before trusting a current pageLSN and reports a
  typed hard error rather than attempting repair from potentially recycled WAL;
- deterministic payload/header/PageId, post-checkpoint, retained-WAL, semantic
  corruption, rollback, and crash tests are complemented by a bounded public
  Page decoder fuzz target.

Page v5 retains this checksum unchanged while extending slot entries with a
generation; versions 1 through 4 are unsupported experimental formats. Page 0
now carries heap metadata v3 and remains outside data-page checksum coverage.
WAL v3, record v2, and canonical schema v1 are unchanged. Page CRC
detects persistent data-page corruption independently of WAL CRC after log
recycling; neither checksum repairs corruption nor authenticates malicious
changes.

## Phase 3A — Typed Expressions + NULL Semantics (complete)

- explicit `ScalarValue::Null` across storage and query execution;
- contextual typing of NULL literals plus expression nullability metadata;
- explicit `IS NULL`, `IS NOT NULL`, and unary `NOT` nodes through AST, typed
  HIR, relational IR, planning, and execution;
- complete SQL three-valued `AND`, `OR`, and `NOT` semantics;
- UNKNOWN-producing comparisons with NULL and TRUE-only `WHERE` filtering;
- preserved nominal type safety and independent heap-level NOT NULL
  enforcement;
- nullable row codec, close/reopen, parser, HIR, compiler, truth-table, and
  embedded end-to-end coverage.

Phase 3A does not expand projection to arbitrary expressions and does not add
DML, joins, sorting, aggregation, indexes, or explain output.

## Phase 3B — Typed DML (complete)

- typed statement AST, HIR, logical statements, and physical INSERT, UPDATE,
  and DELETE plans;
- explicit `AffectedRows(u64)` results and SELECT-compatible `execute`;
- stable-RowId page delete/replace primitives, version 3 tombstones, and
  deterministic page compaction (later superseded by generation-safe reuse);
- sequential target collection with shared three-valued predicates and
  simultaneous UPDATE assignments;
- implicit statement transactions and explicit multi-statement transaction
  integration, with whole-transaction rollback on mutating statement failure;
- unchanged full-page-image WAL, runtime rollback, startup recovery, and
  checkpoint machinery covering every DML mutation;
- parser, typing, page boundary, fault-injection, recovery, and embedded
  vertical integration tests.

Phase 3B originally preserved RowId and rejected same-page overflow; Phase 4B
later added relocation and returns the current RowId. INSERT remains one row
with an explicit column list; there are no defaults, RETURNING, UPSERT, or
subqueries.

## Phase 3C — Typed INNER JOIN (complete)

- query-local `RelationBindingId` values distinct from catalog `TableId`;
- qualified/unqualified name resolution, aliases, ambiguity rejection, and
  left-to-right JOIN scope construction;
- typed chained INNER JOIN predicates with nominal safety and nullable BOOL;
- logical Join and physical row-at-a-time NestedLoopJoin operators;
- binding-aware self joins, SQL NULL predicate semantics, duplicate
  preservation, and deterministic left-major/right-minor results;
- multi-heap core composition without page, WAL, recovery, checkpoint, or
  transaction format changes;
- parser, resolver, type, planner, executor, self-join, multi-join, and embedded
  close/reopen tests.

Phase 3C supports only INNER JOIN with `ON`. There is no outer join, `USING`,
join reordering, hash/merge/index join, or multi-table DML.

## Phase 3C.5 — Foundation Hardening (complete)

- unified typed validation for canonical schemas and table definitions;
- explicit canonical table-schema encoding version 1 and SHA-256 fingerprint;
- heap metadata format version 2 originally added persisted schema identity;
  at that phase metadata remained v2, while the current format is v3;
- pre-recovery rejection of table-ID and full-schema mismatches;
- deterministic golden, sensitivity, invalid-schema, and reopen tests.

Heap metadata versions 1 and 2 have no migration path and are rejected by the
current version 3 decoder. The experimental format may continue to change
between versions.

## Phase 3D — Aggregate + Sort (complete)

- typed multi-key source-column `ORDER BY` with qualified/unqualified
  resolution, explicit/default direction and NULL placement, stable in-memory
  execution, and sort-before-projection planning (complete);
- typed global `COUNT`, numeric `SUM`, and ordered-type `MIN`/`MAX`, including
  empty-input and NULL semantics, checked overflow, derived output metadata,
  and one-pass deterministic execution (complete);
- source-column `GROUP BY`, grouped COUNT/SUM/MIN/MAX, group-only distinct
  output, NULL/multi-key semantics, binding-aware validation, and hidden keys
  (complete);
- one-pass in-memory grouped logical/physical execution with projection-ordered
  outputs and deterministic first-seen implementation order (complete).

Phase 3D does not support sort expressions, projection aliases, ordinals,
aggregate aliases/expressions, DISTINCT, HAVING, aggregate-aware ordering,
GROUP BY expressions, GROUPING SETS, ROLLUP, or CUBE. No persistent page, heap
metadata, WAL record, recovery, checkpoint, or transaction format changed for
sorting or aggregation.

## Phase 4 — Indexing and planning

- Phase 4A complete: Page v5 stores a nonzero generation in every slot;
  tombstones retain it, deterministic reuse increments it without wrap, and
  stale RowIds are rejected before accessing a replacement occupant;
- Phase 4A recovery complete: full-page WAL redo preserves committed reuse and
  before-image undo restores the prior tombstone generation;
- Phase 4B complete: heap INSERT uses deterministic linear first-fit across
  data pages without a persistent FSM;
- Phase 4B complete: UPDATE prefers in-place replacement, otherwise relocates
  to the lowest accepting PageId or a new page and returns the current RowId;
- Phase 4B transaction safety complete: destination then source PageUpdates
  form one transaction chain, and partial compound mutations enter
  RollbackRequired so they cannot commit;
- Phase 4C1 complete: pure typed B+Tree node/ordering/codec crate plus
  transactional persistent create, arbitrary-height insert, and
  duplicate-preserving point lookup;
- Phase 4C1 durability complete: mixed Heap/B+Tree Page v5 kinds, stable
  metadata-page handles, deterministic full-page WAL splits, rollback-required
  partial failure handling, STEAL/NO-FORCE crash recovery, checkpoint/reopen,
  capacity-one traversal, corruption tests, and bounded decoder fuzzing;
- Phase 4C2 complete: exact `(key, RowId)` delete, deterministic merge-only
  encoded-byte rebalance, recursive parent compaction, root collapse, and
  rollback/STEAL/NO-FORCE crash durability;
- Phase 4C2 intentionally defers sibling redistribution and physical
  reclamation/reuse of orphan pages left by merge and root collapse;
- Phase 4D1 complete: Heap metadata v3 anchors an append-only persistent index
  registry, and one transaction creates, fully backfills, registers, commits,
  and exposes each single-column non-unique index for reopen discovery;
- Phase 4D1 keeps raw B+Trees unregistered and independent from table DML;
- Phase 4D2 complete: all Heap and SQL INSERT/UPDATE/DELETE operations maintain
  registered indexes in one transaction, propagate RowId relocation, preserve
  deterministic multi-index ordering, and recover runtime and crash failures
  under STEAL/NO-FORCE;
- Phase 4E complete: Core exposes registered indexes as an ordered read-only
  planner access-path snapshot; exact equality and nullable IS NULL predicates
  can select deterministic point IndexScan for SELECT/UPDATE/DELETE while the
  full SQL Filter remains responsible for truth semantics and Heap rows are
  fetched by generation-safe RowId;
- Phase 4F complete: explicit `ANALYZE` persists optional table/index optimizer
  snapshots in IndexCatalog v2 without DML maintenance; deterministic integer
  page-visit costs compare eligible point indexes with SeqScan, preserve the
  Phase 4E fallback when statistics are absent, and retain the full Filter so
  stale snapshots cannot change query semantics.

Phase 4 now provides baseline registered indexing, atomic DML maintenance,
point IndexScan execution, and point access-path cost planning. Histograms,
range scans/costing, index intersection/union, join ordering, index nested-loop
join, sort avoidance, and uniqueness enforcement remain advanced work.

## Phase 5 — Server and protocol

### Phase 5A — Protocol v1 and synchronous sessions (complete)

- explicit bounded `NDBP` binary frames and fixed client/server message tags;
- schema-fingerprint handshake and capability advertisement;
- streamed `QueryStart` / `QueryRow` / `QueryEnd` response batches;
- stable wire errors and transaction-state reporting;
- synchronous transport-neutral `SessionState` for query/DML, table-scoped
  explicit transactions, `ANALYZE`, ping, and fallible disconnect rollback;
- golden bytes, malformed-input coverage, protocol fuzzing, and real database
  session integration tests.

### Phase 5B — Network transport (complete)

- strict deployment manifest v1 and standalone `netbadbd` bootstrap;
- loopback-only blocking TCP listener and per-connection OS threads;
- dedicated synchronous database owner/worker with FIFO typed commands;
- multiple isolated SessionStates with one request completed at a time per
  connection;
- disconnect rollback, fatal rollback-failure policy, and graceful thread/
  database shutdown;
- real TCP handshake, query/DML, transaction, multi-client, malformed-frame,
  schema-mismatch, and shutdown integration tests.

### Phase 5C1 — Operational resource hardening (complete)

- strict deployment manifest v2 with bounded defaults;
- admitted connection/thread cap enforced before session and thread creation;
- blocking socket read-inactivity and write-delivery timeouts;
- SessionState response-row policy before wire-message expansion;
- standard-library atomic runtime metrics with read-only snapshots.

### Phase 5C2a — Secure remote transport (complete)

- mandatory mutual TLS with runtime-generated certificate integration tests;
- verified client leaf-certificate SHA-256 identity;
- secure non-loopback listening with loopback plaintext retained for local
  development;
- TLS handshake admission, timeouts, shutdown, and runtime metrics before
  worker session creation.

Protocol v1 has no authentication payload and remains byte-for-byte unchanged;
TLS establishes identity before Hello.

### Phase 5C2b — Per-client authorization (complete)

- required manifest v4 local-plaintext and certificate-fingerprint principals;
- explicit per-TableId read, write, transaction, and analyze scopes;
- typed compiler-resolved StatementAccess preflight before execution;
- authorization-filtered Hello table visibility and low-cardinality denial
  metrics;
- trusted-but-unlisted mTLS admission denial before Protocol Hello.

Protocol v1 remains byte-for-byte unchanged and maps operation denials to its
generic Database error code.

## Phase 6 — SDK and tooling

### Phase 6A — Go Protocol v1 client (complete)

- independent standard-library Go frame encoder and untrusted-server decoder;
- explicit scalar, semantic type, stable remote error, and transaction-state
  domains;
- loopback plaintext and verified mutual-TLS Dial with automatic Hello;
- required schema-fingerprint and capability gates;
- streaming Query, Exec, table-scoped transactions, Analyze, and Ping;
- independent Go/Rust golden bytes plus real plaintext and mutual-TLS
  `netbadbd` integration.

### Phase 6B — Generated typed Go SDK (complete)

- strict language-neutral SDK Schema Spec v1 converted through canonical Rust
  schema validation and fingerprinting;
- deterministic Rust-to-Go generation with selection by TableId, semantic
  nominal types, table/column IDs, embedded fingerprints, and stale-output CI;
- `Nullable[T]`, explicit full-row decoders, exact result-shape validation,
  typed row streams for both Client and Tx, and automatic generated schema
  gates;
- committed generated tests plus real Rust-server typed Go integration.

Phase 6B deliberately generates no CRUD or query-builder methods. Protocol v1
has no typed parameter binding, and primary-key metadata is not uniqueness
enforcement, so interpolating runtime values or promising one-row key lookup
would create false contracts.

### Phase 6C — Synchronous Rust remote client (complete)

- blocking `netbadb-client` transport reusing the authoritative
  `netbadb-protocol` codec, with resolved-peer loopback plaintext safety and
  mandatory verified mutual TLS;
- automatic Hello, capability and canonical schema-fingerprint gates, retained
  ServerInfo, checked request IDs, and no multiplexing, retry, or replay;
- borrowed streaming Rows with exact shape/type/nullability/count validation,
  explicit drain-on-close, and connection-closing unfinished Drop;
- borrowed table-scoped transactions whose wire error state controls local
  terminal state, with explicit commit/rollback and connection-closing active
  Drop preserving ambiguous network outcomes;
- `netbadb-sdk` default `embedded` feature plus optional `remote`, including a
  remote-only build with no core, executor, planner, or storage dependency;
- scripted protocol-state tests and real plaintext/mTLS/authorization/
  disconnect-rollback integration tests.

### Phase 6D1 — Structured inspection API (complete)

- low-level `netbadb-inspect` DTOs depend only on canonical schema and types;
- embedded catalog inspection reports declaration-ordered schema,
  registration-ordered indexes, fingerprints, and cached `ANALYZE` snapshots;
- statement inspection exposes typed access, result provenance, expressions,
  and the exact physical query/DML plan selected by the normal planner;
- exhaustive core conversion removes BTree/Page/WAL handles and performs no
  execution, transaction, writer acquisition, heap scan, or WAL mutation;
- explicit deterministic catalog and statement text rendering avoids `Debug`,
  serde, and accidental machine-format commitments.

### Phase 6D2 — Offline local inspection CLI (complete)

- standalone `netbadb inspect catalog|statement` commands reuse deployment
  manifest v4 and `netbadb-sdk` embedded inspection without depending directly
  on compiler, planner, executor, or storage internals;
- `--sql` and UTF-8 `--sql-file` support deterministic human text or explicit
  Inspection JSON v1, with stdout delayed until successful database close;
- the JSON contract explicitly tags statement, result, plan, expression,
  aggregate, semantic-type, and typed-scalar shapes without adding serde to
  inspection DTOs;
- local inspection requires offline exclusive ownership, uses normal startup
  recovery, ignores network-principal ACL filtering, and never executes the
  inspected SQL.

### Phase 6E1 — Shared diagnostics and diagnostics-only LSP (complete)

- SDK Schema Spec v1 has one strict `netbadb-schema-spec` parser shared by
  codegen and tooling while generated Go output remains byte-identical;
- `netbadb-tooling` exposes stable diagnostic codes, human messages, and exact
  UTF-8 byte spans through exhaustive parser/HIR error conversion;
- synchronous `netbadb-lsp --schema ...` provides stdio initialization, full
  document synchronization, versioned open/change/close diagnostics, and
  graceful shutdown without database or network access;
- the LSP adapter performs checked UTF-8 byte to UTF-16 line/character
  conversion and advertises no completion, hover, definition, formatting,
  semantic-token, or physical-plan capability.

### Phase 6E2 — MCP adapter (deferred)

Direct compilation probes found that official Rust MCP SDK releases supporting
the required MCP 2025-11-25 stdio tool surface require a Rust compiler newer
than NetbaDB's Rust 1.85 MSRV. Phase 6's implemented SDK and tooling scope is
complete, while this optional adapter remains deferred. Revisit it only when an
official release simultaneously provides MCP 2025-11-25 or newer, stdio tools,
and Rust 1.85 compatibility without a fork, patch, or NetbaDB MSRV increase.

No MCP placeholder crate or dependency is retained. There is currently no Go
`database/sql` driver, connection pool, ORM/query builder, prepared statement
support, typed parameter protocol, Rust generated query layer, SQL EXPLAIN
syntax, remote inspection protocol, cost explanation, or rejected-access-path
reporting.

## Phase 7 — Advanced optimization

### Phase 7A — Reproducible performance baseline (complete)

- dependency-free custom benchmark target using optimized Cargo bench builds,
  deterministic fixtures, warmup, and quick/full profiles;
- min, median, and nearest-rank p95 measurements without timing thresholds or
  committed machine-specific expected numbers;
- real planner inspection and correctness checks for point SeqScan/IndexScan,
  low-selectivity indexed equality, nullable point access, range predicates,
  sort, aggregate, and nested-loop join scenarios;
- direct INSERT maintenance scaling across zero, one, and two indexes, indexed
  SQL UPDATE, and compile/physical-plan inspection overhead;
- no optimizer, planner, executor, storage, protocol, or persistent-format
  behavior changes.

### Phase 7B — Costed bounded integer RangeIndexScan (complete)

- typed inclusive/exclusive index ranges and read-only B+Tree leaf-chain
  traversal, including duplicates spanning leaves and corruption checks;
- nested-AND extraction and tightening of two-sided Int64/UInt64 literal
  bounds, including reversed operands and empty ranges;
- integer-only cost comparison using existing ANALYZE snapshots and exact
  discrete bound cardinality, with narrow ranges eligible and wide ranges
  retaining SeqScan;
- complete residual Filter and generation-safe Heap fetch for SELECT, UPDATE,
  and DELETE, with DML targets materialized before index maintenance;
- deterministic inspection text and current Inspection JSON v2, while v1
  remains a historical documented/golden contract;
- no BTree payload, IndexCatalog, statistics, protocol, schema-spec, manifest,
  or other database persistent-format change.

Phase 7C must be selected from post-7B benchmark results. Candidate ranking is
therefore a measured follow-up, not a preselected hash-join or rewrite project.

### Later Phase 7 work

- histograms/MCVs and more sophisticated cost models;
- predicate rewrites and property inference;
- join ordering and algorithms;
- benchmarks before introducing complexity.
