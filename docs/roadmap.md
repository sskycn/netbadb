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
- explicit Active, CommitPending, RollbackPending, Committed, and RolledBack
  states;
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

WAL v2 and record v1 remain unsupported experimental formats. Heap metadata
remains v2 and canonical schema encoding remains v1.

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
retains heap metadata v2 and is outside data-page checksum coverage. WAL v3,
record v2, heap metadata v2, and canonical schema v1 are unchanged. Page CRC
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

UPDATE preserves RowId but does not relocate a row. A replacement that cannot
fit on its current page fails atomically. INSERT is currently one row with an
explicit column list; there are no defaults, RETURNING, UPSERT, or subqueries.

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
- heap metadata format version 2 with persisted schema identity;
- pre-recovery rejection of table-ID and full-schema mismatches;
- deterministic golden, sensitivity, invalid-schema, and reopen tests.

Heap metadata version 1 has no migration path and is rejected explicitly. The
experimental format may continue to change between versions.

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
- B+Tree index crate;
- index scan physical operator;
- catalog statistics;
- deterministic cost-based access-path selection.

## Phase 5 — Server and protocol

- `netbadbd` session layer;
- versioned language-neutral wire protocol;
- authentication, TLS, and connection management;
- protocol integration tests.

## Phase 6 — SDK and tooling

- generated Go schema/query SDK;
- Rust SDK stabilization;
- CLI, LSP, MCP, and plan/schema inspection APIs;
- shared diagnostics across compiler and tooling.

## Phase 7 — Advanced optimization

- statistics and cost model improvements;
- predicate rewrites and property inference;
- join ordering and algorithms;
- benchmarks before introducing complexity.
