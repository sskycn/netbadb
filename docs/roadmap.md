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
format had no checksum; WAL v3 added integrity checks and the current WAL v4
adds the durable Prepare record.

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

## Single-writer MVCC + snapshot read isolation (complete)

- strong `CommitSeq` and `CommandId` types, one canonical Snapshot/ReadView,
  per-statement Read Committed, and transaction-pinned Repeatable Read;
- heap metadata v4 and checked `NBMV` tuple v1 headers containing
  `xmin/xmax`, `cmin/cmax`, and an optional next-version RowId;
- durable checksummed `<database>-txn-status` v1 storage for committed and
  aborted decisions, with Commit WAL LSN as the monotonic commit sequence;
- commit ordering that syncs WAL before status publication and startup
  reconciliation of the intervening crash window;
- append-version UPDATE, logical DELETE, own-command visibility, dirty-read
  prevention, and aborted-version handling;
- one visibility implementation shared by sequential scans, selective and
  borrowed visitors, direct COUNT/presence paths, RowId reads, and point/range
  index scans;
- B+Trees as MVCC candidate generators, retaining old version entries until
  exact horizon-safe reclamation;
- explicit synchronous vacuum using the oldest pinned snapshot, with
  generation-safe Heap slot reuse and WAL-backed index/Heap cleanup;
- deterministic corruption, rollback, RC/RR, index-equivalence, vacuum,
  checkpoint/reopen, and abrupt-process recovery coverage.

This phase intentionally retains one physical writer, STEAL/NO-FORCE full-page
WAL, synchronous core execution, and quiescent checkpoints. Serializable,
multi-writer conflict detection, background vacuum, cross-process writer
coordination, replication, and distributed transactions remain deferred.

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

WAL versions 1 through 3 and record versions 1 through 2 are unsupported by the
current WAL v4/record v3 decoder. At this WAL-integrity-hardening phase, heap
metadata remained v2; the current heap metadata format is v5. Canonical schema
encoding remains v1.

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
carried heap metadata v3 at this phase and remains outside data-page checksum coverage.
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
  at that phase metadata remained v2, while the current format is v5;
- pre-recovery rejection of table-ID and full-schema mismatches;
- deterministic golden, sensitivity, invalid-schema, and reopen tests.

Heap metadata versions 1 through 4 have no migration path and are rejected by the
current version 5 decoder. The experimental format may continue to change
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

### PostgreSQL Compatibility Foundation — phase 1 (experimental)

- added a real `netbadb-pgwire` boundary with bounded PostgreSQL v3 startup and
  frontend codecs, typed backend messages, centralized OID/format adaptation,
  malformed/truncated input rejection, and no execution dependency;
- added `netbadbd --postgres` as an exclusive loopback listener mode without
  changing deployment manifest v4 or native Protocol v1;
- shared synchronous transaction execution and disconnect cleanup between the
  native and PostgreSQL session adapters while keeping PostgreSQL prepared,
  portal, Sync-recovery, and failed-transaction behavior in the adapter;
- completed Simple Query for the existing NetbaDB SQL subset, multi-statement
  transaction batches, RowDescription/DataRow/CommandComplete/ErrorResponse,
  text BOOL/INT8/TEXT/NULL, stable SQLSTATE mapping, and a small explicit set
  of connection compatibility SHOW/functions;
- completed named and unnamed zero-parameter Parse/Bind/Describe/Execute/Sync/
  Close/Flush lifecycle, including bounded portal suspension and replacement
  rules. Compiler-level typed `$n` parameters and all binary formats remain
  rejected explicitly rather than being implemented with SQL text replacement;
- added raw real-TCP startup, simple CRUD, transactions, failed transactions,
  extended query, portal/statement close, SSL refusal, CancelRequest, and
  compatibility-query integration coverage.

The phase is a foundation, not a general compatibility claim. P1 typed
parameters, simultaneous listeners, and TLS; P2 catalog-derived `pg_catalog`
and `information_schema`; and P3 broader PostgreSQL SQL remain open.

### PostgreSQL Compatibility Round 2 — typed Extended Query (complete)

- added frontend-neutral `ParameterId` expressions through parser, typed HIR,
  relational IR, compiler metadata, and logical binding; PostgreSQL OIDs remain
  confined to the pgwire/session adapter;
- Parse now compiles once with supplied, zero, omitted, or contextually inferred
  parameter types. Repeated nominal uses must agree and unresolved parameters
  fail with `42P18`;
- Bind validates PostgreSQL `0`/`1`/`N` format cardinality, decodes supported
  text/binary values once into typed `ScalarValue`s, and substitutes into a
  cloned logical statement without SQL interpolation or reparsing;
- added `ParameterDescription`, repeated named prepared execution, text/binary
  results for BOOL/INT8/TEXT, checked int2/int4/int8 inputs, and deterministic
  `08P01`, `22P02`, `22003`, and `42804` boundaries;
- added generic FROM-less scalar SELECT using typed `OneRow` and
  `ScalarProject` nodes rather than session query matching;
- verified parameterized CRUD, repeated binds, NULL, and transaction behavior
  through raw TCP tests and pgx v5.7.6, plus real psql 17.11 scalar queries.

### PostgreSQL Compatibility Round 3 — ORM reflection (complete)

- captured real psycopg 3.2.13 and SQLAlchemy 2.0.52 PostgreSQL-dialect startup,
  Core, reflection, autoload, and ORM traffic instead of guessing catalog
  surface area;
- added a bounded structured compatibility-operation layer for only the
  observed `pg_namespace`, `pg_class`, `pg_attribute`, `pg_type`, `pg_constraint`,
  `pg_index`, and `pg_description` projections;
- derives authorized table names, ordered columns, nullability, physical type
  projection, and primary keys from immutable Canonical Schema without a
  persistent PostgreSQL catalog or storage mutation;
- added deterministic high-range synthetic table OIDs with domain-separated
  hashing and collision checking, confined to the PostgreSQL server adapter;
- added generic typed postfix BOOL/INT64/TEXT casts and qualified projection
  aliases required by normal SQLAlchemy Core/ORM SQL while retaining nominal
  types inside HIR;
- added read-only savepoint recovery for psycopg's hstore capability probe,
  `DEALLOCATE` prepared-cache cleanup, a redacted opt-in protocol trace, and a
  reproducible real-client smoke script;
- verified schema/table/column/primary-key inspection, repeated multi-table
  autoload, reflected parameterized SELECT, Core CRUD and rollback, and ORM
  SELECT/CRUD against existing NetbaDB tables.

A complete `pg_catalog` or `information_schema`, DDL/migrations, simultaneous
listeners, PostgreSQL TLS/password authentication, actual cancellation, and
broader dialect support remain explicit later work.

### PostgreSQL Compatibility Round 4 — index reflection (complete)

- reused and strengthened the storage-neutral `CatalogInspection` Core boundary
  so registered indexes expose stable `(TableId, ColumnId)` logical identity,
  single-column order, BTree kind, and non-unique semantics without PageIds or
  storage handles;
- projects only real Heap registry entries; LSM clustering remains a table
  access path rather than a secondary index, while partition-local physical
  indexes are explicitly unsupported for logical reflection;
- added deterministic, bounded PostgreSQL-facing index names and domain-
  separated synthetic index OIDs with catalog-wide collision checking;
- implemented the captured SQLAlchemy `pg_index`/`pg_class`/`pg_attribute`/
  `pg_am` result shape, including array-typed column/opclass flags, without a
  general `pg_index` table or `pg_get_indexdef` implementation;
- verified two real secondary indexes (including a nullable column), a zero-
  index table, repeated reflection, multiple connections, reopen stability,
  autoloaded `Index` objects, authorization filtering, and no PK duplication;
- added an Alembic 1.16.5 read-only `compare_metadata` probe: matching metadata
  produces no diff, while omitting the two indexes produces two `remove_index`
  proposals. No migration operation or DDL is executed.

### PostgreSQL Compatibility Round 5 — psql describe catalogs (complete)

- captured the real psql 17.11 Simple Query sequence blocker by blocker with
  `-X -E`, then added structured operations for relation lookup/properties,
  columns, indexes, and truthful empty PostgreSQL-only metadata;
- added authorized `\dt` and `\di` projections, compatibility primary-key
  indexes, real Round 4 secondary indexes, and authenticated-session owner
  projection without creating a PostgreSQL role catalog;
- added bounded catalog-only psql pattern matching for relation and schema
  filters. It supports only anchored literals, `.`, and `.*`, uses
  non-backtracking dynamic programming, and rejects unsupported regex syntax;
- routed Simple Query catalog operations through the same read-only
  `CompatibilityStatement` evaluator used by Extended Query reflection;
- verified `\d users`, qualified/missing/wildcard variants, `\dt`, `\di`, and
  their required patterns with the real psql 17.11 executable.

This remains an existing-schema inspection profile. `\d+`, unrelated slash
commands, a general PostgreSQL regex engine/parser/catalog, migration DDL, and
PostgreSQL-only metadata absent from NetbaDB remain unsupported.

### PostgreSQL Compatibility Round 6 — index DDL lifecycle foundation (complete)

- added generic parsed and typed `CREATE INDEX` resolution into durable
  `IndexName`/`TableId`/`ColumnId` metadata without PostgreSQL types below the
  frontend;
- evolved IndexCatalog v2 to backward-readable v3 for optional bounded logical
  names, retaining synthetic reflection names for legacy entries;
- reused the transactional BTree create/backfill/catalog WAL path for implicit
  and explicit transactions, publishing planner/inspection state only after
  commit and proving rollback/crash/reopen behavior;
- verified psql, SQLAlchemy `Index.create()`, guarded Alembic add-index apply,
  immediate cross-connection reflection, planner discovery, and later DML
  maintenance;
- at Round 6, retained `DROP INDEX` as `0A000`; Round 7 below implements durable
  retirement while deferring physical reclamation.

### PostgreSQL Compatibility Round 7 — transactional index retirement (complete)

- implemented generic DROP AST/typed HIR/compiler resolution and Core DDL with
  stable registry-scoped IndexId, table write access, IF EXISTS, and commit-only
  active publication;
- introduced IndexCatalog v4 retained active/retired registrations, backward
  v2/v3 decoding and overflow-safe lazy upgrade, without changing Heap/Page/WAL,
  txn-status, CoordinatorLog, LSM, partition, BTree, or inspection JSON formats;
- added real process crash loser/winner tests, commit/rollback failure retention,
  checkpoint/reopen/idempotence, and create/drop/recreate with fresh trees/stats;
- verified point/range and Phase 73 IndexNestedLoopJoin paths disappear,
  prepared queries replan, DML stops touching retired pages, and ANALYZE/vacuum
  cannot revive the index;
- verified psql, SQLAlchemy Index.drop, named and legacy Alembic DropIndexOp apply,
  with empty subsequent comparisons and cross-client existing-connection refresh;
- retain retired definitions/physical ownership in storage inspection. No page
  free/reuse primitive exists; repeated create/drop grows catalog and database
  files. Quiescent reclamation and catalog compaction are intentionally deferred.

### Storage Lifecycle Round 8 — catalog compaction complete; physical reclaim deferred

- introduced IndexCatalog v5 root-only durable next_index_id; v2/v3 IDs retain
  metadata-page derivation, v4 IDs remain explicit, and v1 remains rejected;
- added explicit synchronous Heap/TableStorage/Core catalog compaction, using
  checkpoint admission and full-page WAL rollback/recovery rather than unlogged
  destructive writes; active identities, names, statistics and reflection stay
  unchanged, and repeated maintenance is a no-op;
- added root-to-leaf ownership enumeration with duplicate/cycle, alias, kind,
  bound and leaf-link validation for active, retired and raw trees;
- chose permanent abandonment of dropped-tree pages and obsolete continuation
  pages after compaction. No physical reclamation, file truncation or arbitrary
  PageId reuse is implemented; the database file and status history still grow;
- added deterministic CREATE/DROP stress, legacy expansion and process-crash
  tests, high-water corruption/exhaustion checks, Core/PG identity regressions,
  and v2/v3/v4/v5 catalog fuzz seeds.

### Storage Lifecycle Round 9 — durable ownership complete; physical reclaim deferred

- BTree v2 persists file-local IndexId on every new registered meta/internal/leaf
  page; handles/traversal require exact owners and splits inherit them;
- v1 raw and registered trees stay readable/writable and non-reclaimable;
- IndexCatalog v6 preserves v5 high-water and backward v2/v3/v4/v5 decoding,
  while compaction retains minimal pending owner/meta records;
- quiescent Heap/TableStorage/Core admin inventory validates complete page and
  node payloads, global reachability/disjointness, and merge-orphan ownership;
- unary internal deletion paths merge or rotate transactionally before leaf
  deletion; grow/delete/collapse and subprocess undo/redo tests cover this case;
- active-only inspection, planner paths, native Protocol v1 and PG CREATE/DROP
  semantics are unchanged; raw/historical legacy pages are never guessed free;
- Route B: owner mismatch protects cross-owner access, but rollback can repeat
  provisional identity and buffer/WAL still lack allocation generations. Tail
  reclaim, arbitrary reuse, free-list and global PageGeneration migration are
  deferred; 100-cycle tests report retained growth honestly.

### Storage Lifecycle Round 10 — generation-safe reference foundation

- Durable WAL reservation LSNs provide strongly typed PageGeneration without a
  second high-water. Reservations survive rollback/crash/checkpoint.
- Registered BTree v3 pages carry owner/generation; handle/root/child/leaf links
  persist full PageRef. Catalog v7 preserves active and pending refs.
- Exact buffer reads, pin checks and invalidation protect rollback/reappend;
  recovery checks allocation identity before pageLSN. Same-owner stale handles,
  multi-level references, dirty frames, and process-crash reuse are tested.
- Legacy BTree v1/v2 and catalogs v2-v6 remain supported with explicit legacy
  references. Raw v1 trees and Heap/RowId allocation reuse remain outside scope.
- No physical reclaim, tail reclamation, free-list or table DDL is added.

### Storage Lifecycle Round 11 — checkpoint-gated retired tail reclamation

- Core-first API selects the maximal suffix made of whole retired BTree v3 trees,
  including owned orphans. Middle-hole ownership stays pending.
- Shared quiescent admission, internal checkpoint and authoritative rescan precede
  a checksummed IndexCatalog v8 root intent (backward decode v2-v7).
- Clean/unpinned range invalidation, expected-count truncate and file sync precede
  transactional covered-record removal and intent clear. Open accepts only the
  exact old/new lengths and resolves interrupted intent before loading roots.
- Logging failures require reopen; normal mutations/checkpoint/catalog compaction
  are blocked. Root capacity is bounded; intent never adds pages into its target.
- Actual PageId reuse with fresh generation, stale-reference rejection, winner
  redo/loser undo, process-crash boundaries and whole-tree orphan tests are covered.
- Tail-friendly 100-cycle stress: initial/final 3 pages, peak 5, 200 pages reclaimed,
  0 pending, 99 repeated meta PageIds. Interleaving raw trees retains 200 retired
  pages, 100 pending owners and a 404-page file; general compaction is not claimed.

### Storage Lifecycle Round 12 — reusable capability and owner inventory (P0)

- Full allocator/page-kind audit selects retired-owner-derived inventory instead
  of a second durable free catalog. Only retired registered BTree v3 qualifies.
- IndexCatalog v9 adds owner-only pending records; v2-v8 remain readable and v8
  tail intents recover. Partial inventory survives loss of meta/root, including
  orphan-only remainders. Zero-owner cleanup uses ordinary catalog transactions.
- Quiescent Core/Heap candidate inspection validates the full file, excludes
  active/legacy/raw/Heap/catalog pages, and orders old PageRefs by lowest PageId.
- Production hole reuse is **not implemented**. Existing WAL rejects nonzero
  generation transitions; checkpoint alone cannot fix redo/undo identity rules.
- Measured interleaved fixture: 404 pages, 200 candidates, 100 retired owners.
  Another 100 CREATE/DROP operations still append 200 BTree pages (606 total,
  including catalog growth). This is a P0 safety gate, not bounded-growth success.
- Active-owner merge orphans and abandoned catalog pages remain excluded.

Next: **Generation-transition WAL/recovery and BTree-v3 reusable allocator**.
Prove physical before-image restoration, idempotent winner redo/loser undo,
clean-frame claims, and cache invalidation before enabling middle-hole allocation.
A new durable general free-list is not indicated by the inventory audit. Complete
P1's crash/stress gates before active-orphan reclamation or Heap/RowId migration;
do not jump to CREATE TABLE. These P1 gates are now completed by Round 13 below.
See [Round 12](page-reuse-round12.md).

### Storage Lifecycle Round 13 — generation transitions and middle-hole reuse

Implemented: explicit full-image WAL record v5/tag 8 for registered BTree-v3
allocation transitions; ordinary PageUpdate remains strict. Generation-first
redo/undo, certified superseded-history filtering, synced reservation authority,
exact rollback and retry semantics are tested without a checkpoint. A lazy
owner-derived cache revalidates candidates and skips pinned/dirty frames; CREATE,
backfill, leaf/internal split and DML consume holes before append. Owner-only
pending survives meta-first and orphan-only consumption; compaction cleans zero
owners. Page/BTree/Catalog/Heap and WAL container versions remain unchanged.

Real subprocess crash coverage and the 83-hole backfill preserve the 117-page
file; 500 CREATE/DROP cycles after a 404-page hole fixture remain at 404 pages,
with 1000 transitions and zero BTree appends. Active-owner orphans, Heap, Catalog,
raw/legacy and cross-kind reuse remain excluded in that round. Its next step,
explicit Active BTree Orphan Retirement, is implemented below.
See [Round 13](page-transition-round13.md).

### Storage Lifecycle Round 14 — explicit active-node retirement and reuse

Implemented: audited leaf/internal merge, root-collapse and unary-normalization
paths retire only explicitly unlinked generation-aware nodes. Independent NBTR
v1 payloads retain PageRef/owner and remove every outgoing structural field.
Same-allocation PageUpdate logs unlink before retirement; rollback restores exact
active bytes. Durable commit is the candidate-publication horizon; a transaction
cannot reuse its own new retirements even after steal/cache rebuild.

The existing allocator consumes whole-owner pages and independent markers in
PageId order. Same-owner generation transitions require a marker before image;
ordinary updates still reject generation, owner and kind changes. Marker reuse
rollback restores exact marker bytes. Page v5, active BTree v3, IndexCatalog v9,
WAL container/record layouts and Heap metadata v5 are unchanged.

The height-5 fixture shrinks from 83 active pages to two reachable pages plus
81 markers, with zero new unmarked orphans. At capacities 1/8, later same-index
DML consumes all 81 with fresh generations and no file growth. A 100-cycle
workload stays at 115 pages with 8,100 retirements and 8,100 reuse transitions.
Retirement and reuse have separate real-process winner/loser crash matrices.
See [Round 14](btree-orphan-round14.md).

### Storage Lifecycle Round 15 — quiescent historical orphan adoption

Implemented: explicit `Database::adopt_historical_btree_orphans(TableId)` for all
active registered v3 indexes in a single Heap. Existing maintenance admission,
internal checkpoint, complete post-checkpoint tree/file validation and exact
clean/unpinned candidate proofs precede one all-or-none PageUpdate transaction.
The checkpoint WAL generation becomes the recovery baseline; even deliberately
retained older structural WAL cannot reintroduce a reference to an adopted page.
NBTR, Page, BTree, Catalog and WAL formats are unchanged. Adoption preserves
PageRef/owner; subsequent same/different-owner reuse reserves fresh generations.

The historical fixture converts 81 ordinary orphans to 81 markers and consumes
all 81 without growing its 115-page file. A real 1,186-candidate fixture uses one
transaction. Runtime byte-exact rollback, process-loss winner/loser and partial
flush histories converge across three reopens. No startup repair, raw/v1/v2,
unknown-owner, Heap/Catalog adoption or NBTR tail truncation is implemented.
See [Round 15](historical-orphan-round15.md).

### Table Schema Lifecycle Round 16 — architecture audit

Completed audit/design only: [Round 16](table-schema-lifecycle-round16.md) traces
current schema authority, manifest startup, logical/physical identity,
fingerprints, transactions, prepared statements, authorization and SDK/PG
contracts. Targeted tests pin external-schema reopen behavior and the legacy
anonymous-index generation notification gap. No runtime schema writes, table
DDL, catalog format, or protocol change is implemented.

Chosen target: bootstrap once, then a persistent per-database SchemaCatalog is
the sole logical authority. SDK/application schemas are exact subset expectations.
Use durable independent ID reservations, immutable publication, transaction-local
schema overlays, a quiescent schema writer lease, and a versioned schema
participant with recoverable physical creation intents. DROP retires logically;
physical deletion waits for recovery-log retention gates.

### Runtime Schema Catalog Foundation Round 17

[Round 17](runtime-schema-catalog-round17.md) implements a database-level full
committed snapshot and independent durable installation discriminator. Fresh
create and explicit complete-inventory Heap/LSM/range legacy import install once;
ordinary reopen reconstructs Schema, placements and bindings without an external
schema authority. Semantic types, sparse IDs/order, fingerprints, schema/table
versions and four independent identity high-waters persist. Expectations permit
extra committed tables while requiring exact IDs/fingerprints for requested ones.
Missing/corrupt initialized catalogs fail; snapshot/marker shadows have explicit
crash winner/loser tests. The [v1 byte contract](schema-catalog-v1.md) is documented.

Manifest startup, native fingerprints and PG inspection remain projections or
expectations. Protocol v1, physical Heap/LSM/index/coordinator formats, PK enforcement
and prepared statements are unchanged. Runtime index revision saturation and the
anonymous-index notification gap remain follow-up work. Incomplete physical
creation and writable clone/downgrade workflows are not solved by catalog import.

### Core Transactional Heap CREATE TABLE Foundation — Round 18

[Round 18](core-create-table-round18.md) implements one Core Heap creation per
transaction, including same-transaction typed INSERT/SELECT and existing-table
DML. NBSC floors plus a bounded versioned reservation journal preserve identities
through rollback/crash. Private schema/bindings remain invisible globally until
coordinator-backed physical promotion and catalog publication complete. Recovery
resolves winners and losers before ordinary catalog open; subprocess crash tests
assert identities, rows, generation and repeated reopen. Prepared table dependencies
survive unrelated creation; transaction-local statements cannot execute globally.
PG/Protocol metadata remains a Core projection with existing authorization.

Round 19 below connects generic SQL CREATE TABLE to this Core API.
Reject PRIMARY KEY/UNIQUE/DEFAULT/CHECK/REFERENCES/GENERATED/PARTITION/CTAS/TEMP/
UNLOGGED/INHERITS/LIKE until their semantics exist. DROP/ALTER, non-Heap creation,
journal compaction, and general aborted-resource garbage collection remain deferred.

### Generic SQL CREATE TABLE — Round 19

[Round 19](sql-create-table-round19.md) implements parser declarations with spans,
typed HIR columns, compiled/prepared DDL and schema-write access metadata, with
no allocated IDs before execution. Native Protocol v1 and PG Simple/Extended Query
reuse Core Heap creation and transaction SchemaOverlay. Explicit default-deny
schema-admin authorization includes temporary creator access only while staged.
psql, psycopg and SQLAlchemy Table.create fixtures verify real transactional DML,
rollback, commit, unchanged manifests, catalog-only reopen and post-commit denial.
Constraints, DROP/ALTER, runtime LSM/range placement and ORM table migrations remain
unsupported. SQL parser bounds/property tests and existing storage crash/fuzz
regressions protect the new frontend boundary.

### Core Transactional DROP TABLE Foundation — Round 20

[Round 20](core-drop-table-round20.md) implements Core-only exact
TableId/version/fingerprint retirement for one active non-partitioned Heap. The
transaction overlay removes the table before commit and invalidates old prepared
dependencies without name rebinding. Coordinator winner recovery publishes NBSC
G+1, while retained NBSJ history durably accounts for the old StorageId/locator and
the complete Heap/index resource remains on disk. Rollback preserves exact identity,
data, indexes, generations and allocator floors. Same-name recreation receives new
TableId/StorageId and no old data/index/grant identity. Real subprocess crashes cover
intent through retirement/publication, including zero-participant DROP and
DML-before-DROP. LSM/range DROP and all physical deletion remain explicit deferred
work.

### Generic SQL DROP TABLE — Round 21

[Round 21](sql-drop-table-round21.md) implements the bounded `DROP TABLE name`
vertical slice. Parser AST keeps only name/spans; HIR and compiled DDL bind the exact
TableId/version/fingerprint from the transaction SchemaView. Native Protocol v1 and
PostgreSQL Simple/Extended execution invoke Round 20 Core, with schema-admin-only
DDL authorization, exact `DROP TABLE` completion, rollback/commit, stale same-name
protection, SQL-driven crash regression, and real psql/psycopg/SQLAlchemy probes.
No persistent format changes. IF EXISTS, CASCADE/RESTRICT, multi-table/qualified
forms, LSM/range DROP, physical deletion and Alembic table migration remain deferred.

### Retired Heap Physical Resource GC — Round 22

[Round 22](retired-heap-gc-round22.md) proves a durable recovery-retention horizon
from append-only completed Coordinator history and implements explicit GC for one
exact runtime-created Single Heap. NBSJ v1 GC intent/complete records make deletion
retry-only; startup resumes intents but never chooses candidates. Exact storage-owned
Heap/WAL/status plus Core owner/link files are removed without symlink following,
then the parent directory is synchronized. Same-name recreation, indexed Heaps,
12 crash points, terminal reappearance and 100 create/drop/GC cycles are covered.
LSM/partition/imported-orphan GC, automatic GC and journal/coordinator compaction
remain deferred.

### ALTER TABLE / schema-evolution architecture audit — Round 23

[Round 23](schema-evolution-round23.md) freezes the current row, MVCC, Heap/LSM,
partition, RowId, index, identity, prepared, SDK and PostgreSQL-reflection facts
without implementing ALTER. Rows have neither arity nor schema identity, and Heap
open requires an exact fingerprint, so the chosen first-version architecture uses
an offline staged copy-on-write replacement for every supported operation. TableId
and surviving ColumnIds remain stable; the table version/fingerprint advance; a new
StorageId is reserved; rows are streamed by ColumnId; all active indexes are rebuilt;
and exact old-schema/resource evidence is retained for later recovery-safe GC.

### Core Heap schema rewrite foundation — Round 24

[Round 24](core-heap-schema-rewrite-round24.md) implements the audited foundation
for one runtime-created Single Heap: exact typed transforms, durable StorageId and
ADD ColumnId reservations, same-TableId staged replacement, current-visible-row
streaming, stable IndexId/high-water rebuild, statistics reset, private target DML,
CORD v2 winner publication and distinct replacement-retirement evidence. NBSJ v1
adds bounded tags 11–15; recovery converges without recopying. The old Heap remains
physically retained and its exact replacement GC API is explicitly unsupported.

Next: integrate the Round 22 retention-horizon proof with replacement-retired Heap
history without treating the still-active TableId as dropped. SQL syntax remains a
later thin frontend. LSM/partition/imported ALTER, online mixed schemas, physical
conversions, defaults, column reorder and journal/log compaction remain deferred.

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
- deterministic inspection text and Inspection JSON v2, now retained with v1
  as a historical documented/golden contract;
- no BTree payload, IndexCatalog, statistics, protocol, schema-spec, manifest,
  or other database persistent-format change.

### Phase 7C — Predicate-first NestedLoopJoin (complete)

- the executor evaluates each typed join predicate through a private joined
  view over the materialized left and right child rows;
- rejected candidate pairs allocate no combined row and copy no scalar values;
  matching pairs retain normal left-then-right materialization, `row_id: None`,
  duplicate preservation, NULL semantics, and deterministic output order;
- PhysicalPlan::NestedLoopJoin, planner selection, binding-aware column lookup,
  and Inspection JSON v2 remain unchanged in this phase;
- controlled before/after benchmark runs cover unique-like, duplicate-key, and
  fully disjoint joins at 500×500 and 1,000×1,000; the disjoint cases isolate
  the eliminated rejected-pair materialization;
- this is a measured constant-factor improvement to the existing quadratic
  nested-loop algorithm, not a new join algorithm or cost model.

### Phase 7D — Costed simple equi HashJoin (complete)

- post-7C measurements showed that eliminating rejected-pair materialization
  left quadratic candidate enumeration and predicate evaluation as the
  dominant work in the fully disjoint million-pair join;
- analyzed direct Scan × Scan INNER JOINs can extract the first necessary,
  semantic-type-compatible cross-side column equality from an AND tree;
- checked integer work compares `left_rows * right_rows` with
  `left_rows + right_rows`, choosing HashJoin only when strictly cheaper and
  retaining NestedLoopJoin for missing statistics, ties, non-equi predicates,
  and unsupported child shapes;
- deterministic right-build HashJoin stores ordered right-row indices, skips
  NULL keys, probes in left order, and evaluates the complete typed residual
  predicate before materializing TRUE rows;
- duplicate multiplicity, self-join binding identity, nominal types, and the
  current left-major/right-minor executor order remain intact;
- deterministic inspection text and current Inspection JSON v3 expose the
  selected HashJoin while v1 and v2 remain historical contracts;
- controlled pre-feature and post-feature benchmark runs compare unique,
  duplicate, and equality no-match joins while a non-equi no-match join retains
  the measured NestedLoopJoin reference.

### Phase 7E — Validate-once Heap scan (complete)

- source inspection found that Heap scan's slot-state and record-read paths
  could repeat the complete `Page::header` checksum and structural validation
  `1 + 2N` times for a page with `N` live slots;
- a crate-private immutable `ValidatedPage<'_>` is created only by the existing
  authoritative full validation, borrows its `Page`, and stores the validated
  `PageHeader` without adding a trust bit or cache to mutable Page state;
- one combined live-record operation reads a slot once, preserves its RowId
  generation, represents tombstones as `None`, rejects invalid slots with a
  typed error, and obtains payloads through checked slices;
- `HeapStorage::scan` performs one full validation per Heap page per scan and
  reuses it only inside that immutable borrow; non-Heap single-payload
  validation is unchanged;
- the row codec, Text ownership, full-row materialization, buffer pool,
  PageManager, checksum algorithm, public Page APIs, planner, executor, and all
  persistent formats remain unchanged;
- direct two-column/1,000-row and six-column-with-Text/10,000-row storage scans
  were added before the implementation, then controlled full-profile pre/post
  runs were recorded three times each; query, range-plan, HashJoin, non-equi
  reference, and result gates remained intact.

### Phase 7F — Join predicate column-position prebinding (complete)

- a private executor-only bound expression borrows the original typed Expr and
  resolves every ColumnRef once through the existing
  `RelationBindingId + ColumnId` identity rule after concrete child output
  fields are known;
- NestedLoopJoin binds the complete `ON` expression before its candidate loop,
  and HashJoin binds the complete residual predicate before bucket probing;
  existing hash-key positions and right-build behavior are unchanged;
- bound evaluation accepts no OutputField slice, performs checked position
  access, and reuses the existing binary operations and three-valued truth
  semantics without changing owned ScalarValue/Text behavior or AND/OR
  evaluation order;
- semantic-equivalence tests cover every ExprKind, BinaryOp, ScalarValue kind,
  TRUE/FALSE/UNKNOWN, repeated columns, distinct self-join bindings, missing
  fields, and short rows; existing non-equi, complex residual, NULL, self-join,
  and chained-join integration tests remain green;
- Filter predicates, UPDATE assignments, Rel IR, PhysicalPlan, planner,
  inspection, and every public contract intentionally remain unchanged;
- narrow two-column and wide eight-Int64-column no-match non-equi scenarios
  were measured before and after in three serial full runs. The representative
  million-pair wide/narrow ratio contracted from about 1.70 to about 1.03,
  directly isolating removal of field-width-sensitive repeated lookup.

### Phase 7G — Borrowed Join predicate scalar evaluation (complete)

- an executor-private evaluated-scalar enum borrows bound Column and Literal
  leaves from the current row or typed predicate and owns only computed Binary,
  Unary, and IS NULL results;
- binary operations and `TruthValue` conversion use one reference-based
  semantic core, while the existing owned evaluator remains a thin wrapper;
  comparisons, NULL/UNKNOWN behavior, checked access, and errors are unchanged;
- NestedLoopJoin candidates and HashJoin residual candidates use the borrowed
  path; final TRUE-row materialization, hash keys and maps, and FALSE/UNKNOWN
  rejection remain unchanged;
- structural tests prove Int64/Text Column and Text Literal borrowing, and
  semantic tests cover all ScalarValue kinds, operators, three-valued logic,
  self joins, chained joins, short rows, and complex HashJoin residuals;
- a deterministic Text non-equi no-match pair was added at 500x500 and
  1,000x1,000 before implementation. Three serial full pre/post runs reduced
  the representative Text/narrow ratio from about 5.26 to 1.21 without a timing
  gate;
- AND/OR still has no short-circuit. Filter, UPDATE, Rel IR, PhysicalPlan,
  planner, inspection, public APIs, dependencies, and persistent formats are
  unchanged.

### Phase 7H — Exact inequality bound rejection (complete)

- NestedLoopJoin extracts the first left-to-right direct cross-side `<`, `<=`,
  `>`, or `>=` conjunct that is necessary under AND; reversed operands are
  normalized to child-relative positions, while OR, NOT, equality, literals,
  and same-side comparisons are ineligible;
- the materialized right rows provide a borrowed exact non-NULL minimum for
  `>`/`>=` or maximum for `<`/`<=`; no ScalarValue summary is cloned and no
  statistics or persistent metadata is involved;
- NULL right keys are ignored, all-NULL/empty right inputs return empty, and a
  NULL left key skips its right loop. Checked access, typed errors, and the
  authoritative scalar comparison semantics are retained;
- only a mathematically impossible left probe skips the inner loop. Possible
  probes preserve the original right order and evaluate the complete bound
  predicate, so output ordering, residual truth, duplicates, and UNKNOWN
  behavior are unchanged;
- 100%-reject narrow/wide/Text, approximately 50%-reject, and 0%-reject control
  scenarios were measured in three serial full pre/post runs. Representative
  large medians improved by about 76.4x, 21.4x, 38.8x, 2.01x, and 1.01x;
- PhysicalPlan and inspection remain NestedLoopJoin/v3. HashJoin, planner,
  storage, public APIs, dependencies, and persistent formats are unchanged.

### Phase 7I — Adaptive exact inequality candidate sweep (complete)

- the Phase 7H exact zero-candidate extreme check remains first, so fully
  impossible Int64, wide, and Text probes return before sorting;
- potential left probes and non-NULL right keys become deterministic
  key/index-sorted row-index auxiliaries that borrow current materialized row
  values; fallible ordering errors propagate as typed execution errors;
- a two-pointer pass counts exact `<`, `<=`, `>`, or `>=` candidate pairs,
  including strict duplicate boundaries. Checked `u128` arithmetic compares
  exact candidates plus integer sort/ordered-set work with the Phase 7H loop;
  only a strict win selects sweep, while ties and overflow fall back;
- an original-right-index `BTreeSet` grows or shrinks once per right row as left
  keys advance. The complete bound predicate still decides every candidate,
  and per-left buckets restore exact left-major/right-minor output order;
- auxiliary memory is O(left rows + right rows + output rows), never
  O(candidate pairs). NULL, duplicates, residual FALSE/UNKNOWN, self joins,
  chained joins, and borrowed scalar evaluation retain their semantics;
- three serial full pre/post runs kept 100%-reject near 0.125/0.123 ms, improved
  partial rejection from 11.697 to 3.266 ms (3.58x), and retained dense and
  no-prune fallback near 24 ms. All plan gates remain NestedLoopJoin;
- PhysicalPlan, planner, storage, inspection v3, public APIs, dependencies, and
  persistent formats are unchanged.

### Phase 7J — Required-column propagation and selective base-row decode (complete)

- raw physical planning remains responsible for access paths and join choices;
  one query-only top-down pass propagates binding-aware source requirements;
- Project, Filter, Sort, Aggregate, Limit, NestedLoopJoin, and HashJoin preserve
  every hidden semantic input while base Seq/point/range reads retain required
  columns in source order;
- join requirements split through `RelationBindingId + ColumnId`, including
  self joins, residual predicates, hash keys, and outer ON columns crossing a
  chained inner join;
- UPDATE and DELETE deliberately use raw full-row plans, preserving replacement
  and transaction behavior;
- typed Heap projected scan/point APIs preserve request order and duplicates;
  zero-column scans retain one RowId-bearing row per live tuple for COUNT(*);
- one borrowed decoder validates every persisted tag, length, bound, UTF-8
  string, physical type, NULL constraint, truncation, and trailing value, while
  only requested values become owned ScalarValues;
- sequential scans retain Phase 7E's once-per-page full validation and indexed
  reads retain generation-safe RowId fetches;
- attribution scenarios cover ID-only versus ID+Text, COUNT(*) versus
  COUNT(Text), hidden Text filtering, ORDER BY, GROUP BY, and narrow/wide joins;
  plans, rows, shapes, and checksums are hard gates without timing thresholds;
- PhysicalPlan operator variants, Inspection JSON v3, Protocol v1, SDK Schema
  Spec v1, Manifest v4, row encoding, dependencies, and persistent formats are
  unchanged.

### Phase 7K — Move-aware projection materialization (complete)

- one executor-private `ProjectionPlan` resolves binding-aware positions,
  identity shape, and per-input last use once per Project operator;
- identity projection moves complete input rows without rebuilding value Vecs;
- unique subset and reorder projections move selected owned ScalarValues;
- a source projected N times clones N - 1 times and moves its original value at
  last use, preserving independent owned duplicate outputs;
- every access remains checked, and RowId, output order, duplicates, types,
  nullability metadata, and fully owned QueryResult semantics are unchanged;
- join child candidate ownership, planner decisions, Phase 7J base columns,
  storage decode, and every public/machine/persistent contract are unchanged;
- allocation-pointer unit tests prove identity/reorder/subset moves and the
  duplicate minimum-clone invariant; SQL tests cover Text-only, reordered, and
  duplicate projections;
- full median-of-three Text-only and ID+Text improved about 34.2% and 36.0%,
  while direct projected Heap stayed within 0.5%; Text-only/direct contracted
  from 1.63x to 1.07x;
- duplicate Text improved about 16.0%, but duplicate/Text-only grew from 1.10x
  to 1.41x because its second String owner remains semantically required;
- all benchmark timing is observational; plan, base columns, rows, shapes,
  exact Text values, and checksums remain the hard gates.

### Phase 7L — Direct global COUNT(column) presence scan (complete)

- the only eligible runtime shape is one global COUNT(column), no GROUP BY,
  over a direct SeqScan whose single binding/column/table identity matches the
  aggregate input;
- the exact current Heap scan uses no catalog or index statistics, cache,
  persistence, WAL mutation, or transaction writer;
- each Heap page is authoritatively validated once, non-Heap single payloads
  remain validated, and only current live tuples participate across deletion,
  slot reuse, relocation, index creation, ANALYZE, and reopen;
- every persisted scalar still receives tag, bounds, UTF-8, physical-type,
  NULL-constraint, truncation, and trailing-value validation. The selected
  scalar is never owned; only its NULL presence is observed;
- storage accumulates an exact checked `u128`, and the executor retains the
  typed aggregate overflow error when converting the one final result to SQL
  COUNT's `u64`;
- no scanned row becomes a `ScalarValue` or `ExecutionRow`. COUNT(*), multiple
  outputs, grouping, Filter, Join, Sort, index scans, mismatched identities,
  and SUM/MIN/MAX retain the complete generic Aggregate executor;
- the physical plan stays `Aggregate → SeqScan[column]`; PhysicalPlan,
  Inspection JSON v3, Protocol v1, SDK Schema Spec v1, Manifest v4, row
  encoding, persistent formats, dependencies, and unsafe-code count are
  unchanged;
- three serial full pre/post runs improved median-of-three COUNT(id),
  COUNT(nullable_key), and COUNT(payload) from 0.927/0.925/1.157 ms to
  0.517/0.514/0.518 ms. COUNT(payload) improved 2.23x, payload/COUNT(*)
  contracted from 1.626x to 0.769x, and payload/ID contracted from 1.248x to
  1.003x;
- COUNT(*), multi-COUNT, filtered COUNT, direct projected Heap payload, and SQL
  payload projection remain controls with exact plan/result gates and no timing
  threshold.

### Phase 7M — Direct multi-COUNT presence summary (complete)

- one exact current Heap traversal produces checked `u128` live-row and
  source-order per-column non-NULL counts for multiple direct COUNT outputs;
- duplicate column counts reuse one summary slot, nullable counts remain exact,
  and mixed COUNT(*) outputs reuse the summary's live-row count;
- aggregate results are reconstructed in SQL output order and each checked
  `u64` conversion retains its own aggregate overflow metadata;
- the Phase 7L single-column API delegates to the generalized public low-level
  summary; zero and duplicate requests retain explicit ordered semantics;
- every page and persisted scalar remains fully validated, including
  unrequested Text/Bool/NULL/truncation/trailing-value corruption, without
  per-row presence allocation, ScalarValue ownership, or ExecutionRow
  materialization;
- single/all-star COUNT(*), grouping, Filter, Join, Sort, index scans,
  mismatched/unused scan columns, and mixed aggregate functions retain the
  generic executor;
- physical planning and Inspection JSON v3 remain unchanged; all persistent,
  protocol, schema-spec, manifest, dependency, and unsafe-code boundaries are
  unchanged;
- three serial full pre/post runs improved median-of-three pair, duplicate,
  mixed-nullable, star+column, and output-order cases from
  1.170/1.137/1.182/1.143/1.179 ms to
  0.657/0.646/0.685/0.622/0.686 ms, approximately 42–46%, with exact plan,
  source-column, result, and metadata gates and no timing threshold.

### Phase 7N — Streaming filtered COUNT presence consumer (complete)

- an executor-private specialization accepts only global all-COUNT output over
  direct `Filter → SeqScan`, with at least one COUNT(column); the physical plan
  and planner remain unchanged;
- source-order predicate columns become owned `ScalarValue` scratch, while
  unique source-order COUNT columns become NULL-presence scratch; overlap and
  duplicate/multi/mixed-star output mappings remain exact;
- one current Heap traversal validates each page once, validates non-Heap
  payloads, fully validates every scalar, and invokes the synchronous callback
  only after the whole live tuple is valid;
- storage knows no SQL `Expr`; executor retains the existing dynamic
  `evaluate_truth`, so TRUE qualifies and FALSE/UNKNOWN do not;
- count-only Text never becomes an owned String, predicate Text remains owned,
  and neither scanned nor filtered `ExecutionRow` collections are created;
- callback errors stop immediately, scratch containers are allocated outside
  the row loop, intermediate `u128` and final per-output `u64` overflow remain
  checked and attributed;
- all-star-only, grouped, mixed-function, nested Filter, Sort, Join, IndexScan,
  RangeIndexScan, missing/mismatched identity, and unused-scan-column shapes
  retain the generic executor;
- isolated serial full x3 reduced median-of-three filtered ID, nullable,
  payload, pair, star+payload, and output-order cases from
  0.969/0.979/1.253/1.267/1.263/1.305 ms to
  0.725/0.715/0.719/0.738/0.735/0.775 ms, with no timing threshold;
- filtered payload/direct payload contracted from 2.046x to 1.321x and filtered
  payload/filtered ID from 1.292x to 0.992x. The Text-predicate control remains
  1.868x the Bool-predicate target because predicate Text still owns.

### Phase 7O — Borrowed dynamic Filter predicate evaluation (complete)

- only the Phase 7N filtered-count callback uses the new private evaluator;
  generic Filter, UPDATE, and the Phase 7G prebound Join evaluator are
  unchanged;
- existing `EvaluatedScalar` is reused: dynamic Column and Literal leaves
  borrow, while Binary, Unary, and IsNull computed results remain owned;
- `find_source_position` remains dynamic and binding-aware, binary semantics
  remain centralized in `evaluate_binary_refs`, and AND/OR still evaluate both
  sides;
- storage still owns one String for predicate Text at the visitor value
  boundary; there is no Filter prebinding, borrowed persisted Text, or
  storage-to-executor zero-copy;
- semantic equivalence covers every ExprKind, BinaryOp, Bool/Int64/UInt64/Text/
  NULL, three-valued truth, repeated leaves, missing/short rows, and relation
  binding identity; pointer tests prove Text Column/Literal leaf borrowing;
- isolated serial full x3 reduced median-of-three single Text equality from
  1.462 to 1.037 ms and repeated Text from 2.013 to 1.163 ms, while Int64
  equality remained 0.767/0.766 ms and generic hidden Text Filter remained
  observational at 1.610/1.650 ms;
- Text/Int contracted from 1.906x to 1.353x, repeated/single Text from 1.377x
  to 1.122x, and Text/Bool from 1.889x to 1.305x, with no timing threshold.

### Phase 7P — Storage-to-executor borrowed predicate scalar views (complete)

- `netbadb-types::ScalarRef<'a>` is the single shared runtime scalar view;
  Bool/Int64/UInt64 copy by value, Text borrows `&str`, and NULL remains
  explicit. It is not a persistent, wire, schema, or SQL IR contract;
- Heap row decoding returns `ScalarRef` directly and still validates every
  persisted tag, width, Text length/bounds/UTF-8, physical type, NULL
  constraint, truncation, and trailing value before invoking a consumer;
- the new HRTB synchronous visitor scopes borrowed Text to the current
  validated-page callback. Page and record guards remain alive through that
  callback, references cannot escape in safe Rust, and per-page scratch avoids
  per-live-row vector allocation;
- request order, duplicate values, duplicate presence, value/presence overlap,
  zero-width projections, callback errors, tombstones, reuse, relocation,
  reopen, and non-Heap validation remain exact;
- the prior owned visitor is retained and delegates the borrowed traversal,
  converting requested views with `to_owned`; its public behavior is
  unchanged;
- `EvaluatedScalar::Borrowed` consumes `ScalarRef`, while computed values stay
  owned. One ScalarRef binary/comparison/truth core backs the retained
  ScalarValue wrappers and the Phase 7G bound evaluator adaptation;
- only the Phase 7N filtered-count callback switches to the borrowed visitor.
  Binding-aware dynamic lookup remains, literals still borrow from `Expr`, and
  generic Filter, UPDATE, INSERT, Join algorithms, planner, compiler, and
  inspection behavior are unchanged;
- isolated serial full x3 reduced median-of-three Text `IS NOT NULL` from
  0.969 to 0.646 ms and Text equality from 1.028 to 0.702 ms. Their equivalent
  Int64 controls changed from 0.710 to 0.624 ms and 0.775 to 0.672 ms;
- Text/Int `IS NOT NULL` contracted from 1.365x to 1.035x and Text/Int equality
  from 1.327x to 1.045x. Repeated/single Text changed from 1.178x to 1.251x,
  while generic hidden Filter remained observational at 1.641/1.510 ms. There
  is no timing threshold;
- row encoding and every persistent/machine contract remain unchanged; no
  dependency or unsafe code was added, and QueryResult stays fully owned.

### Phase 7Q — Filtered-count predicate position prebinding (complete)

- the Phase 7N specialization constructs source-order predicate fields once
  and calls the existing `bind_expression` before Heap traversal; no bound IR,
  expression bytecode, or PhysicalPlan variant was added;
- every bound Column stores the checked source position and diagnostic name.
  The callback evaluator receives only `BoundExpr + &[ScalarRef]`, never
  fields, so the row hot path cannot call `find_source_position`;
- one generalized BoundExpr recursive core accepts a ScalarRef getter. The
  existing Join wrapper adapts owned rows, while the filtered-count wrapper
  uses `values.get(position).copied()`;
- Column/Literal leaves remain borrowed, Binary/Unary/IsNull results remain
  owned, NULL truth is unchanged, and AND/OR continue evaluating both sides;
- binding tests cover repeated columns, multiple source-order positions,
  same-table/same-column self-binding identity, bind-time missing fields, and
  runtime short rows. ScalarRef tests cover every ExprKind, BinaryOp, scalar
  kind, TRUE/FALSE/UNKNOWN, Text pointer identity, and no short-circuit;
- only the specialized Aggregate → Filter → SeqScan COUNT path changes.
  Generic Filter, UPDATE, INSERT, Join algorithms, planner, compiler, storage,
  protocol, inspection, and fully owned QueryResult rows remain unchanged;
- Phase 7Q added repeated Int64 and wide primitive attribution before changing
  production. Isolated serial full x3 changed median-of-three single/repeated
  Int64 from 0.737/0.916 to 0.665/0.822 ms, single/repeated Text from
  0.774/0.948 to 0.696/0.839 ms, and wide lookup from 1.524 to 1.242 ms;
- repeated/single Int64 changed only 1.243x→1.235x and Text
  1.225x→1.204x because repeated predicates still perform extra semantic work.
  Wide/single Int64 contracted 2.068x→1.866x, the clearest lookup attribution;
- generic hidden Filter changed 1.641/1.494 ms and the direct/filtered controls
  also moved broadly. There is no timing threshold, so further Phase 7N local
  micro-tuning is not selected;
- no dependency or unsafe code was added, and all persistent/machine contracts
  and row encoding remain unchanged.

### Phase 7R — Direct COUNT(*) live-row specialization (complete)

- the existing direct-count eligibility no longer requires a COUNT(column).
  Pure single/pair/multi COUNT(*) is eligible only when its direct child is the
  planner's zero-column `SeqScan[]`; a nonempty all-star scan still falls back
  through the existing unused-column check;
- execution reuses Phase 7M's exact `scan_presence_counts([])` and its checked
  `live_rows: u128`. It performs one Heap traversal and creates only the final
  result row, with no scanned empty `ExecutionRow` or per-row `ScalarValue`;
- every output independently calls the existing checked SQL `u64` conversion
  with its exact `AggregateExpr`, preserving order, duplicate names, metadata,
  and precise overflow attribution;
- no storage counter, statistics access, cache, slot-header shortcut, or
  index-only path was added. Zero requested columns still decode and validate
  every persisted scalar; invalid Text remains an error rather than a count;
- tests cover empty and NULL-containing tables, single/pair/triple star
  eligibility, malformed nonempty-scan fallback, mixed and duplicate column
  paths, per-output overflow metadata, delete and slot reuse, index creation,
  stale ANALYZE statistics, reopen, and filtered all-star fallback. Existing
  storage tests retain relocation and mixed-page current-tuple coverage;
- per the requested limit, one isolated serial full pre/post run changed
  single/pair/triple star from 0.757/0.731/0.739 ms to
  0.558/0.550/0.545 ms. COUNT(*)/COUNT(id) changed 1.298x→0.859x and
  COUNT(*)/COUNT(payload) 1.303x→0.954x;
- pair/single remained 0.964x/0.986x and triple/single 0.976x/0.976x.
  Filtered all-star changed 0.948/0.968 ms and stays generic; there is no
  timing threshold;
- planner, PhysicalPlan, Inspection JSON v3, storage production, persistent
  formats, protocol, SDK contracts, ScalarValue, ScalarRef, and fully owned
  QueryResult rows are unchanged. No dependency or unsafe code was added.

### Phase 7S — Generic Filter borrowed-evaluator rollout (complete)

- generic `PhysicalPlan::Filter` now reuses the existing dynamic borrowed
  evaluator over its owned child `ExecutionRows`. Column leaves retain dynamic,
  binding-aware `find_source_position` lookup and borrow the selected row
  `ScalarValue`; Literal leaves borrow from `Expr`;
- computed Binary, Unary, and IsNull values remain owned. AND/OR evaluate both
  sides, TRUE moves the original row unchanged, and FALSE/UNKNOWN drop it;
- no generic Filter prebinding, storage borrowed visitor, streaming, predicate
  pushdown, planner/compiler/IR change, dependency, or unsafe code was added.
  QueryResult and every surviving row remain fully owned. DML selection uses
  the new predicate evaluator naturally; assignment and index-maintenance
  semantics are unchanged;
- one isolated serial full pre/post run changed hidden Text equality from
  1.619354 to 1.191853 ms, Text IS NULL from 1.358145 to 1.109500 ms, and
  repeated Text from 2.202375 to 1.335562 ms. Matching Int64 equality, IS NULL,
  and repeated controls changed 0.904979→0.912250, 0.866979→0.834645, and
  1.103104→1.059334 ms;
- Text/Int equality contracted 1.789x→1.306x, Text/Int IS NULL
  1.567x→1.329x, and repeated/single Text 1.360x→1.121x. Repeated/single
  Int64 changed 1.219x→1.161x. The wide lookup control remained
  1.698895/1.673500 ms, or 1.877x/1.834x single Int64;
- the Phase 7N specialized Text-equality control remained
  0.744437/0.726146 ms and direct COUNT(*)/COUNT(id)/COUNT(payload) remained
  0.575042/0.615562/0.612854 ms pre and
  0.549937/0.586416/0.585979 ms post. There is no timing threshold and no
  extra rerun was necessary.

### Phase 7T — Direct sequential Filter borrowed-row streaming (complete)

- storage exposes one authoritative row-aware borrowed visitor. It validates
  every scalar in every live persisted tuple, reports the current page, slot,
  and generation as `RowId`, and lends ordered `ScalarRef` values only for the
  synchronous callback. The older borrowed visitor is a thin RowId-ignoring
  wrapper and the owned visitor still delegates to borrowed traversal;
- the executor specializes only exact `Filter → SeqScan`. It conservatively
  rejects duplicate or mismatched scan identities and predicates whose
  binding/table/column identity is absent from the SeqScan output. Literal
  predicates are eligible; IndexScan, RangeIndexScan, Join, Sort, nested
  Filter, and malformed shapes retain the generic path;
- each live row is fully decoded and validated before dynamic three-valued
  predicate evaluation. FALSE and UNKNOWN create no owned scalar, row vector,
  or `ExecutionRow`; TRUE owns every SeqScan output value and preserves the
  exact current `RowId`. Dynamic `find_source_position` remains on every
  Column leaf and there is no expression prebinding or Project-aware shortcut;
- the first predicate error is saved while storage continues validation. A
  later storage error therefore retains the old child-first priority; after a
  successful scan the saved predicate error is returned. UPDATE and DELETE do
  not mutate until `execute_rows` succeeds, and assignment, index maintenance,
  transaction, Phase 7N filtered counts, and Phase 7Q/7R behavior are unchanged;
- one isolated serial full pre/post run changed hidden Text/Int equality from
  1.172708/0.891999 ms to 0.771625/0.700458 ms, and Text/Int IS NULL from
  1.089500/0.838041 ms to 0.695416/0.650563 ms. Repeated Text/Int changed
  1.351271/1.062854 ms to 0.962791/0.881395 ms; the wide lookup control changed
  1.652646 to 1.441604 ms;
- all-TRUE Text/Int controls changed 1.205125/0.740396 ms to
  1.296416/0.827771 ms. They intentionally still own complete SeqScan rows.
  The selective owned-Text-output control improved from 1.155479 to
  0.737333 ms. COUNT controls remained observational. There is no timing
  threshold and no extra full run was performed;
- planner, compiler, Rel IR, PhysicalPlan, inspection, protocol, SDK, and
  persistent formats are unchanged. QueryResult remains fully owned; no
  dependency or unsafe code was added.

### Phase 7U — Retained-column-aware Project/Filter streaming materialization (complete)

- the executor specializes only exact `Project → Filter → SeqScan` when at
  least one predicate-used scan column is not retained and every scan column
  is used by either Filter or Project. Malformed, unused, duplicate scan, and
  non-sequential shapes retain the generic path;
- Filter still evaluates dynamically over the complete validated borrowed
  SeqScan row. FALSE and UNKNOWN own nothing; TRUE owns only source positions
  retained by Project. Duplicate, reordered, and zero-width outputs preserve
  their exact row and ownership semantics;
- the Phase 7T visitor and predicate/storage error ordering are unchanged.
  Retained Text remains owned at the QueryResult boundary, and a shape with no
  predicate-only column intentionally falls back;
- planner, compiler, Rel IR, PhysicalPlan, inspection, protocol, SDK, and
  persistent formats are unchanged. No dependency, unsafe code, generic
  expression prebinding, or new physical operator was added.

One isolated full pre/post run changed the all-TRUE predicate-only Text target
from 1.292270 to 0.924291 ms, contracting its ratio to the all-TRUE Int64
control from 1.561x to 1.031x. The retained Text control changed from 0.952895
to 1.003125 ms and therefore did not share that target-level improvement.
Every benchmark correctness gate passed; there is no timing threshold and no
extra full run was performed.

### Phase 7V — Generic Filter position prebinding (complete in Phase 63)

- exact `Filter → SeqScan` and retained-column-aware
  `Project → Filter → SeqScan` build source-order fields and bind the existing
  `BoundExpr` once before borrowed storage traversal;
- their valid row callback evaluates `&BoundExpr + &[ScalarRef]` by checked
  position and receives no `Expr` or field slice. FALSE/UNKNOWN Text rows stay
  borrowed, while only selected output values become owned;
- the materialized legacy Filter binds once after child execution, covering
  IndexScan, RangeIndexScan, PartitionedScan, Join, and other ineligible
  streaming/batch children;
- binding failure keeps the dynamic evaluator as a malformed-plan compatibility
  fallback, preserving empty-child and row-dependent error timing. Existing
  three-valued logic and non-short-circuit AND/OR behavior remain shared;
- batch Filter, filtered COUNT, Join predicates, dispatch priority, plans,
  public APIs, dependencies, and persistent contracts are unchanged.

The Phase 63 narrow/wide pair holds output, scan columns, comparison count, AND
count, and result cardinality constant. Its quick wide/narrow ratio changed
only from 0.887x to 0.849x and remained below 1.0, while unrelated controls
moved from -71.2% to +17.7%. This does not establish a stable timing curve; the
structural removal of per-row identity lookup and the semantic tests are the
authoritative result.

### Later Phase 7 work

- histograms/MCVs and more sophisticated cost models;
- predicate rewrites and property inference;
- broader join ordering and algorithms;
- benchmarks before introducing complexity.

## Phase 8 — Multi-Storage Engine Foundation (complete)

- Core composes `Vec<TableStorage>` rather than `Vec<HeapStorage>`; the only
  implemented layout variant is `TableStorage::Heap`;
- B+Tree remains a registered Heap access method and is not modeled as a table
  storage engine;
- executor-facing projected scan, point/range access, borrowed visitor,
  presence/count summary, and mutation capabilities preserve every Phase 7
  Heap specialization through direct enum delegation;
- `StorageRowHandle` carries storage-owned mutation identity without exposing
  Heap PageId/SlotId to executor, planner, SQL, or relational IR. Heap RowId
  remains the checked generation-safe physical locator inside Heap and B+Tree;
- `StorageReadView` and `StorageTransaction` reserve database-facing context
  boundaries while delegating to the already implemented Heap MVCC and
  physical WAL transaction machinery;
- Planner consumes ordered `AccessPath` snapshots containing table/column IDs,
  opaque table-scoped `AccessPathId`, point/range capabilities, and optional
  statistics. Physical index plans no longer carry `BTreeHandle`;
- Heap point/range execution resolves opaque IDs against registered access
  methods and reuses the same MVCC candidate validation as SeqScan;
- architecture tests cover enum dispatch, capability planning, point/range
  execution, borrowed/presence fast paths, cross-storage context rejection,
  transaction rollback, and stale Heap locator protection.

This phase adds no LSM or Columnar implementation, fake placeholder, vectorized
executor, or new async runtime. It does not change Heap metadata v4, MVCC tuple
v1, transaction-status v1, Page v5, BTree/IndexCatalog payloads, WAL v3/record
v2, Protocol v1, SDK Schema Spec v1, manifest v4, Inspection JSON v3, or SQL
semantics. The prompt's earlier “no MVCC yet” premise was superseded by the
already completed single-writer MVCC phase; Phase 8 preserves that behavior and
only moves it behind storage-owned contexts.

The next storage-engine phase should add one concrete second layout only when
its real row identity, transaction context, scan capabilities, persistence, and
recovery model are specified. Batch/chunk execution remains a separate measured
executor phase rather than a prerequisite for this boundary.

## Phase 8B — Database Transaction Coordinator Foundation (complete)

- added strong `StorageId` physical identity and separate runtime-only
  `DatabaseTxnId`; neither changes Heap/WAL identities or persistent formats;
- Core owns deterministic `PhysicalBindings` and `StorageRegistry` boundaries.
  Current validated catalog order assigns StorageIds starting at one, while all
  routing resolves TableId → StorageId explicitly rather than treating a Vec
  position as identity;
- planner snapshots, inspection, SELECT/DML, index management, ANALYZE, vacuum,
  checkpoint, and executor storage/read-view lookup all route through these
  boundaries;
- `DatabaseTransaction` now owns SQL transaction identity, isolation,
  lifecycle, lazy participants, and the database-level read context.
  `StorageTransaction` is an engine participant containing the current Heap
  WAL/MVCC transaction;
- `DatabaseReadView` groups the StorageReadViews used by one logical statement.
  Explicit transactions support reads and joins across multiple StorageIds;
- participants have explicit Read/Write mode and support Read → Write upgrade.
  Multiple readers plus exactly one physical writer are supported;
- a second writer is rejected before physical mutation. Commit never attempts
  sequential multi-writer durability; rollback coordinates the unique writer
  and all read contexts. Participant failures leave the database transaction
  pending instead of falsely reporting a terminal state;
- Server `SessionState` owns `DatabaseTransaction`; BEGIN/QUERY/DML/COMMIT,
  ROLLBACK, and disconnect rollback retain their Protocol v1 lifecycle.
  Authorization remains based on compiler-resolved logical TableIds.

Architecture invariants:

```text
TableId is logical identity.
StorageId is physical storage identity.
TableId does not permanently imply exactly one physical storage.

DatabaseTransaction owns SQL transaction semantics.
StorageTransaction is one engine participant context.
```

This phase originally added no atomic multi-storage writes, prepare/2PC, or
coordinator log. Its StorageIds existed only for one opened composition; the
atomic commit phase below supersedes that limitation with metadata-persistent
identity.

## Atomic Multi-Storage Commit Foundation (complete)

- Heap metadata v5 persists a nonzero `StorageId`; reorder and restart no
  longer change physical recovery identity, and metadata versions 1–4 are
  rejected without migration;
- WAL v4 / record v3 adds a checksummed `Prepare(DatabaseTxnId)` record and a
  durable Prepared state that retains the storage writer;
- storage recovery classifies Prepared separately from winners and losers,
  requires an explicit typed resolution for standalone open, and idempotently
  commits or undoes exact physical transaction mappings;
- Core owns an independent append-only CoordinatorLog v1 with bounded,
  canonical CommitDecision participants and idempotent Complete records;
- the successful CommitDecision sync is the global commit point. Before it,
  presumed abort is legal; after it, every participant must eventually commit
  and rollback is rejected;
- coordinator-enabled create/open APIs take an explicit log path. Legacy APIs
  retain the one-write-storage boundary, while read-only and single-writer
  transactions retain their existing fast paths;
- startup validates coordinator decisions and all stable participant identities
  before physical recovery, rejects missing or mismatched participants, applies
  partial commits, and finishes incomplete Complete records;
- append/fsync retry tests, corruption/golden tests, bounded decoder fuzzing,
  repeated recovery, and 13 abrupt-process crash windows cover the durability
  boundary.

The CoordinatorLog is intentionally append-only and has no GC/checkpoint yet.
Protocol v1, SQL grammar/results, SDK Schema Spec v1, manifest v4, Inspection
JSON v3, Page v5, MVCC tuple v1, and transaction-status v1 are unchanged.

## Range Partition Foundation (complete)

- added strong, durable `PartitionId`, distinct from logical `TableId` and
  recoverable `StorageId`;
- evolved `PhysicalBindings` to `Single` and `RangePartitioned` placements,
  allowing several physical storages to belong to one logical table without
  weakening unique storage ownership;
- added immutable checksummed PartitionCatalog v1 with exact schema identity,
  typed half-open Int64/UInt64 bounds, bounded strict decoding, path-independent
  reopen, corruption tests, and a dedicated fuzz target;
- planner prunes exact AND comparison intervals before selecting a local access
  path per partition, keeps residual Filters, represents zero selected
  partitions, and preserves deterministic range order and required columns;
- INSERT routes typed values; UPDATE materializes and validates every
  destination before same-partition update or atomic delete+insert movement;
  DELETE materializes and writes every selected partition in one database
  transaction;
- logical SELECT, joins, self joins, and aggregates consume the concatenated
  relation. ANALYZE refreshes every partition independently; partial refresh is
  permitted because stale statistics affect cost only, never pruning or query
  semantics;
- inspection text and current JSON v4 expose the logical partitioned scan,
  exact selected PartitionIds, and per-partition access choices while v1/v2/v3
  remain historical contracts;
- subprocess tests cover pre/post-decision cross-partition UPDATE,
  multi-partition DELETE, and explicit multi-partition INSERT recovery.

Scope remains RANGE-only, single-column, NOT NULL, Int64/UInt64, local Heap
partitions and local indexes. Gaps are legal and have no DEFAULT fallback.
There is no SQL partition DDL, global index/uniqueness, split/merge, HASH/LIST,
heterogeneous storage, placement/sharding, replication, or distributed commit.

## LSM Storage MVP (complete)

- added `TableStorage::Lsm` without changing the executor/coordinator into
  storage-kind switches;
- added stable `LsmRowId` and storage-local `LsmCommitSeq`, duplicate-preserving
  clustering order, version visibility, tombstones, read-your-writes, and
  bounded single-writer transaction overlays;
- added checksummed little-endian Manifest v1, LSM WAL v1, and block-oriented
  immutable SSTable v1 formats with strict bounded decoding;
- added synchronous MemTable flush/WAL rotation and quiescent all-L0-plus-L1
  compaction with manifest-authoritative crash recovery and orphan cleanup;
- registered native LSM point/range capability metadata, projected reads,
  counts, persisted ANALYZE row/cardinality/min/max statistics, and SQL DML
  through the existing storage boundary;
- proved Heap+LSM prepare/commit/recovery through CoordinatorLog with reordered
  reopen and subprocess crash matrices.

The MVP intentionally remains one NOT NULL Int64/UInt64 clustering column,
duplicates ordered by `(clustering key, LsmRowId)`, one writer, synchronous
flush, L0 plus one L1, and quiescent compaction. It has no Bloom filter,
compression, background work, secondary LSM index, or LSM range partitions.
Range partition physical layout remains Heap-only; heterogeneous partitions
remain unsupported.

## LSM Hardening — Bloom Filters + Multi-Level Compaction (complete)

- upgraded experimental Manifest and SSTable formats to v2 while retaining LSM
  WAL v1 and explicitly rejecting old LSM files;
- added deterministic L0-L3 leveled compaction, stable overlap closure,
  clustering-group output splitting, and manifest-atomic multi-output publish;
- added stable checksummed per-SSTable clustering-key Bloom filters containing
  puts, tombstones, duplicates, and historical versions;
- replaced all-SST materialization with bounded block cursors and a streaming
  k-way merge, with binary-searched L1+ point/range routing;
- separated history-preserving regular compaction from quiescent full-history
  version/tombstone GC;
- exposed structural level/Bloom inspection, runtime read/write amplification
  counters, and storage-neutral integer access cost hints without planner
  storage-kind matching;
- retained manifest authority, canonical WAL retry, all-batch recovery
  validation, and truly read-only recovery inspection.

LSM Hardening selected **Vectorized Execution Foundation** as the next measured
phase because Heap and LSM then provided mature, distinct physical storage
paths and exposed a shared bottleneck above their boundary.

## Vectorized Execution Foundation (complete)

- added an executor-private 256-row owned `ExecutionBatch`; the capacity is a
  named bounded runtime starting point rather than an optimality claim;
- added one storage-neutral synchronous owned-row consumer with typed
  `ControlFlow` cancellation. Heap retains validated-once MVCC/page/codec
  traversal; LSM streams its ordered committed merge plus bounded transaction
  overlay without first materializing every visible base row;
- eligible complete trees contain one SeqScan and any existing Filter,
  Project, and Limit operators. Filter reuses `BoundExpr`, binds column
  positions once, and preserves TRUE/FALSE/UNKNOWN semantics. Project reuses
  move-aware identity/subset/reorder/duplicate behavior. Limit carries
  remaining state across batches and cancels upstream;
- QueryResult remains fully owned. PhysicalPlan, logical/typed IR, inspection,
  protocol, SDK, and every persistent format remain unchanged. Executor code
  does not branch on Heap versus LSM;
- exact standalone Filter and predicate-only Project/Filter shapes retain the
  measured borrowed Phase 7 streaming specialization for every scalar type;
  Filter pipelines with Limit use the bounded batch runtime. Direct COUNT
  specializations remain. Sort, grouped Aggregate, SUM/MIN/MAX, joins,
  index/range scans, partition scans, and DML deterministically use the legacy
  materialized implementation for the full tree;
- deterministic tests cover 0, 1, BATCH_SIZE−1, BATCH_SIZE, BATCH_SIZE+1,
  2×BATCH_SIZE, and 2×BATCH_SIZE+1 rows; zero-width scans/projects; Bool,
  Int64, UInt64, Text, NULL, nullable and duplicate values; Filter truth
  outcomes; move-aware projections; Limit boundaries; Heap/LSM equality; and
  test-only batch-versus-legacy result equality.

Deferred work remains typed column-oriented batches, SIMD, batch HashJoin,
Sort, index/range scans and partition scans, Columnar storage, Serializable
isolation, concurrent writers, and background execution.

## Batch Pipeline Composition + Streaming Aggregate (complete)

- refactored the Phase 55 batch loop into one executor-private bounded
  producer with a typed callback; the existing result path is now one consumer
  and preserves fully owned `QueryResult` rows;
- added an incremental Aggregate consumer for eligible SeqScan, Filter, and
  Project children. Group-key and aggregate-input positions bind once, while
  the existing COUNT/SUM/MIN/MAX transition and finalization logic remains the
  single semantic implementation for streaming and materialized fallback;
- preserved SQL NULL behavior, checked Int64/UInt64 SUM overflow attribution,
  runtime type errors, output-column order, repeated aggregate outputs, empty
  global/grouped results, first-seen group order, and multiple group keys;
- bounded global aggregate memory by one 256-row batch plus one state set.
  Grouped aggregate additionally retains one owned key and state set per
  distinct group; it never retains the full child `ExecutionRows`;
- Aggregate remains blocking and a Limit above it is applied only after all
  child batches are consumed. Existing direct and filtered COUNT
  specializations remain higher-priority dispatch paths;
- Heap and LSM continue through the same storage-neutral row consumer. No
  storage API, persistent format, PhysicalPlan, inspection, protocol, schema,
  SDK, or dependency changed;
- deterministic tests cover 0, 1, 255, 256, 257, 512, and 513 input rows;
  empty and all-NULL inputs; Int64, UInt64, Bool, Text, and NULL values;
  Project/Filter/SeqScan children; global, low/high-cardinality and multi-key
  grouping; cross-batch groups; output order; overflow/error attribution;
  Heap/LSM equality; fallback; and independent legacy-result equivalence.

## Move-Aware Aggregate Ownership (complete)

- changed streaming Aggregate to drain owned rows from each batch while
  retaining the batch allocation for reuse;
- MIN/MAX borrow candidates for NULL/type/comparison decisions, collect only
  states that actually replace, clone for all but one required owner, and move
  the original candidate into the final replacing state;
- retained the borrowed legacy Aggregate path and the existing authoritative
  comparison, NULL, overflow, and finalization semantics. Pure COUNT/SUM
  batches bypass replacement bookkeeping and continue borrowed inspection;
- reused `ProjectionPlan` last-use ownership during finalization so unique
  group/state values move, while duplicate outputs clone only before their
  final use;
- left `HashMap<Vec<ScalarValue>, usize>` and group-key lookup cloning intact;
  no storage API, PhysicalPlan, persistent format, protocol, SDK, dependency,
  unsafe code, or COUNT specialization changed;
- added pointer-identity structural tests plus empty/1/255/256/257/512/513,
  duplicate MIN/MAX, equal/alternating/NULL Text, Int64/UInt64/Bool,
  grouped/filtered/NULL-key, Heap/LSM, error, and legacy-equivalence coverage.

The quick attribution is noisy in absolute time, but the ascending Text
MAX/MIN ratio contracted from 1.271x to 0.928x and duplicate-MAX/MAX from
1.380x to 1.137x. That supports group-key ownership/hash attribution as the
Phase 58 target. Aggregate Text comparison and generic Filter prebinding follow;
typed column-oriented batches, SIMD, batch HashJoin/Sort, and
index/range/partition batch sources remain broader unselected candidates.

## Borrowed Group-Key Lookup + Owned-Key-on-Miss (complete)

- replaced `HashMap<Vec<ScalarValue>, usize>` with a private randomized
  hash-to-head lookup and a group-index collision chain;
- hashes group-key values directly from borrowed rows and exact-compares every
  candidate against the one durable key owned by `GroupState`; hash equality
  alone never merges SQL groups;
- existing-group hits allocate no `Vec<ScalarValue>` and clone no group-key
  values. A miss materializes exactly one durable key and stores only hash/index
  metadata in the lookup;
- preserved NULL grouping, ordered multi-key identity, deterministic first-seen
  output, Phase 57 MIN/MAX movement, direct COUNT dispatch, and the Heap/LSM
  storage boundary without dependencies or unsafe code;
- test-only local statistics prove 513 rows over four groups produce 509 hits,
  four misses, and four owned-key materializations; repeated Text produces 512
  hits and one materialization. A forced same-hash chain proves exact A/B hits
  and C miss independently of `RandomState` collisions.

Quick wall-clock controls moved broadly, but within-run grouping ratios favor
hit-heavy keys: one-group/unique changed from 0.852x to 0.442x and
four-group/unique from 0.984x to 0.683x. Unique Int64 grouping remains the
largest directly attributable miss-heavy residual. Phase 59 should therefore
first measure remaining miss ownership/hash metadata, followed by Aggregate
Text comparison and generic Filter prebinding. Typed column batches, HashJoin
batch integration, Sort/Top-N, and index/range/partition batch sources remain
lower-evidence candidates rather than selected work.

## Move-on-Miss Group-Key Ownership (complete)

- grouped batch Aggregate now drains owned rows even without MIN/MAX, while
  global COUNT/SUM retains its simpler borrowed batch path;
- separates the borrowed Phase 58 group probe from miss materialization, so
  hits still allocate, clone, and transfer no group key;
- prebinds group-key owner slots by source position, combines them on a miss
  with the current row's actual MIN/MAX replacement targets, and performs
  exactly `clone N-1 + move original once` for each owned source value;
- keeps borrowed legacy miss materialization, randomized hashing, exact
  collision checks, NULL grouping, multi-key order, first-seen output, direct
  COUNT priority, Heap/LSM boundaries, dependencies, and safe Rust unchanged;
- test-only counters prove 513 four-group rows perform four key moves and zero
  key clones, while 513 unique Int64 and Text groups perform 513 moves and zero
  clones. Pointer identity proves the original Text allocation reaches
  `GroupState`; key+MAX and key+duplicate-MAX require one and two clones.

Quick attribution moved pure unique Text key-only by -10.1%, unique Text+COUNT
by -11.2%, and unique `(Int64, Text)` by -17.9%, while Text key+MAX moved +1.8%
and unique Int64 +8.2%. Unrelated controls ranged from -27.1% to +23.5%, so the
structural ownership result is authoritative and the timings are directional.
Phase 60 should first isolate primitive group hashing and owned-row bookkeeping,
then compare Aggregate Text comparison and generic Filter prebinding. Column
batches, HashJoin integration, Sort/Top-N, and index/range/partition batch
sources remain unselected until those residuals are measured.

## Prehashed Group Bucket Lookup (complete)

- retained the executor-private `RandomState` as the only hash of SQL group-key
  width and ordered `ScalarValue` values;
- changed only `GroupLookup.bucket_heads` to use a private `PrehashedKey` and
  pass-through `BuildHasher`, so its already keyed/randomized `u64` selects the
  bucket without a second randomized hash;
- kept `GroupState.key_values` authoritative and traverses the unchanged exact
  collision chain before declaring a hit; equal prehashes never imply equal SQL
  groups;
- preserved Phase 58 allocation-free hits, Phase 59 move-on-miss ownership,
  NULL grouping, multi-key order, first-seen output, error timing, Heap/LSM
  neutrality, safe Rust, and all public and persistent contracts;
- added structural tests for exact pass-through boundary values, distinct
  bucket lookup, rejection of generic byte hashing, the `RandomState` outer
  boundary, and forced same-prehash/exact-key collisions.

One serial quick pre/post run moved one-group, four-group, unique Int64, two
primitive keys, unique Text, and wide unique by -21.7%, -29.3%, -29.7%, -38.7%,
-4.2%, and -26.4%. The much smaller unique-Text change relative to the cheap
primitive shapes supports the intended fixed-second-hash attribution, but the
wide result and unrelated controls ranging from -33.1% to +151.1% demonstrate
substantial machine/code-layout variance. The structural removal is therefore
authoritative; no stable throughput claim or timing gate is added.

Phase 61 should isolate selective owned-row bookkeeping. The grouped batch path
still drains every hit row into owned-row machinery even when no group-key or
MIN/MAX ownership transfer is needed. Aggregate Text comparison and generic
Filter position prebinding remain later focused candidates; typed
column-oriented batches, batch HashJoin, Sort/Top-N, and index/range/partition
batch sources remain unselected.

## Borrowed-First Grouped Batch Consumption (complete)

- keeps grouped rows inside `ExecutionBatch` and iterates them by mutable borrow;
  whole `ExecutionRow` values are no longer drained from the batch;
- lets group-only and COUNT/SUM hits perform only borrowed probe and transition
  work, and skips the scalar-transfer helper for MIN/MAX hits without an actual
  replacement;
- preserves Phase 59 miss ownership and actual extrema replacement behavior:
  group-key and replacement owners still share clone `N - 1` plus one move per
  selected source slot;
- clears the complete batch after success or error and retains its `Vec`
  capacity; global COUNT/SUM and the Phase 57 global MIN/MAX drain path remain
  unchanged;
- retains Phase 60 keyed prehashing, exact forced-collision comparison, NULL and
  multi-key grouping, first-seen order, Heap/LSM neutrality, safe Rust, and all
  public and persistent contracts.

Test-only accumulator counters give the structural result for 513 rows:

| workload | borrow-only hits | miss rows | transfer rows |
| --- | ---: | ---: | ---: |
| one-group COUNT | 512 | 1 | 1 |
| four-group COUNT | 509 | 4 | 4 |
| unique-group COUNT | 0 | 513 | 513 |
| one-group ascending Text MIN | 512 | 1 | 1 |
| one-group ascending Text MAX | 0 | 1 | 513 |

One serial quick pre/post run moved one-group group-only/COUNT/SUM by -34.4%,
-26.1%, and -7.1%. Mostly-borrowed Text MIN moved -50.2%, while transfer-heavy
Text MAX moved +15.3%, which is the clearest selective-bookkeeping-shaped signal.
Four-group group-only/COUNT moved +16.8%/+0.5%, unique Int64/Text moved
-8.6%/-8.2%, and unrelated controls ranged from -37.3% to +48.1%. Timings are
therefore directional; structural counters, cleanup tests, pointer identity,
and legacy equivalence are authoritative.

Phase 62 should investigate Aggregate Text comparison rather than continue
ownership micro-tuning. Primitive grouping did not show a consistent remaining
hash/lookup curve across one group, four groups, and two primitive keys, while
Phase 61 has now removed the identifiable whole-row bookkeeping. Generic Filter
position prebinding follows; typed column batches, HashJoin integration,
Sort/Top-N, and index/range/partition batch sources remain unselected.

## Typed MIN/MAX Extreme State + Direct Text Comparison (complete)

- binds every executor-private MIN/MAX state to Bool, Int64, UInt64, or Text
  from typed Aggregate output metadata during accumulator construction;
- compares candidates directly against the matching physical state, with Text
  borrowing `String` values as `&str` and using standard `str::cmp` ordering;
- preserves NULL-as-no-non-NULL-extreme state, defensive runtime type errors,
  and move-only finalization without changing the generic scalar comparator;
- retains Phase 57 clone `N - 1` plus one move, Phase 59 overlapping group/extreme
  ownership, and the complete Phase 61 grouped mutable-row path;
- adds same-length 64-byte early-difference, long-common-prefix, and all-equal
  Text MIN attribution fixtures plus typed/generic equivalence coverage for
  Bool, Int64, UInt64, ASCII, Unicode, empty, and long Text values.

The quick pre/post medians moved +3.1%, -46.0%, and +23.5% for early-difference,
long-common-prefix, and all-equal Text MIN. Their equal/early ratio changed from
0.834x to 1.000x and common-prefix/early from 0.841x to 0.441x. That is not a
credible monotonic lexical-cost curve: non-target controls ranged from -16.4%
to +49.4%, global Text MIN+MAX moved +100.3%, and primitive extrema moved in
conflicting directions. The implementation is retained for its stronger
typed-state invariant and removal of generic Aggregate dispatch, not as a
throughput claim.

Aggregate ownership and comparison micro-tuning stops here unless a new
structural issue is measured. Phase 63 follows with generic Filter position
prebinding.

## Generic Filter Position Prebinding (Phase 63, complete)

- reuses the sole executor-private `BoundExpr` for valid generic streaming and
  legacy materialized Filter paths, resolving every Column identity once per
  execution;
- keeps borrowed scalar evaluation and owns values only for qualified output;
- retains the dynamic evaluator only for malformed binding compatibility and
  other execution boundaries that have not selected prebinding;
- proves wide first/middle/last/repeated positions, bound/dynamic evaluation
  equivalence, projected streaming retention, batch Filter+Limit stability,
  and unchanged missing-column/type/boolean error behavior;
- adds a gated same-output, same-scan, five-comparison narrow/wide attribution
  pair and retains IndexScan Filter plus primitive/Text controls.

The quick pair did not reveal a stable position-cost signal: narrow moved
626,958→651,292 ns and wide moved 556,417→552,791 ns, changing wide/narrow from
0.887x to 0.849x. Broad non-target movement prevents a throughput claim or
timing gate. Phase 64 should not continue leaf-lookup micro-optimization.
Typed column-oriented batch representation, HashJoin batch producer/consumer
integration, Sort/Top-N, and index/range/partition batch sources should be
remeasured against current workloads before one is selected; AND/OR
short-circuiting remains a separate measured candidate.

## Bounded Top-N over Batch Producer (Phase 64, complete)

- recognizes only the existing `Limit -> Project -> Sort` physical tree when
  Sort's child is accepted by the SeqScan/Filter/Project batch producer;
- resolves sort and final projection positions once, consumes every child
  batch, validates every row even when K is zero or the candidate is discarded,
  and retains no more than `min(K, rows_seen)` complete candidates;
- uses a fallible worst-first heap and an input ordinal tie-break, then sorts
  the retained candidates by keys plus ordinal before reusing the move-aware
  ProjectionPlan, preserving hidden keys, duplicate outputs, NULL placement,
  direction, multi-key ordering, Text ordering, and stable ties;
- keeps malformed setup and unsupported sources on the legacy executor, and
  leaves full Sort without Limit unchanged;
- proves K and 256-row batch boundaries, all-equal ties, type/error behavior,
  full legacy result equivalence, and Heap/LSM equivalence; the benchmark adds
  exact-plan/base-column/order gates for K sweep, unique/duplicate/multi-key,
  Filter, Text, nullable, full-Sort control, and an LSM Top-N case.

Phase 64 adds no PhysicalPlan variant, public API, dependency, unsafe code,
storage format, or upstream cancellation. Typed column-oriented batches, batch
HashJoin, index/range/partition batch sources, full batch Sort, spilling, and a
costed K/N crossover remain future measured work.

## Streaming HashJoin Probe over Batch Producer (Phase 65, complete)

- recognizes only the planner's existing direct SeqScan × SeqScan INNER
  HashJoin shape; setup resolves compatible key positions, joined predicate
  positions, and output positions before child execution, while unsupported or
  malformed shapes retain the authoritative materialized fallback;
- leaves the right child as the fixed, fully materialized build side and keeps
  the existing `HashMap<ScalarValue, Vec<right index>>`, key cloning, right input
  order, and bucket insertion order unchanged;
- visits the left child through the shared 256-row producer, borrows each probe
  row for `hash_join_key`, evaluates the complete bound residual predicate for
  every bucket candidate, and owns values only for TRUE projected outputs;
- preserves NULL non-matching behavior, duplicates, left-major/right-minor
  order across batch boundaries, reordered and repeated outputs, zero-width
  output, runtime errors, self-join statement read views, and fully owned final
  `QueryResult` rows;
- proves 0/1/255/256/257/512/513 probe boundaries, a 17-row materialized build
  control, exact candidate/output counts, materialized legacy equivalence,
  Heap/LSM behavior, self joins, residuals, all physical key types, and
  conservative fallback/error behavior;
- adds plan-gated asymmetric no-match, unique, duplicate, and residual
  benchmarks while retaining all historical symmetric HashJoin and non-target
  controls.

The eligible input-side intermediate boundary is now
`O(right build input + hash metadata + probe batch)` instead of
`O(left input + right input + hash metadata)`, plus the unchanged fully owned
final output. Phase 65 adds no PhysicalPlan variant, planner policy, dynamic
build-side choice, public API, dependency, unsafe code, protocol change, or
persistent-format change. The next attribution work should prioritize the
concrete index/range/partition batch-source fallback gap before considering the
larger typed column-oriented representation; build-side HashJoin ownership,
full batch Sort, spilling, and AND/OR short-circuiting remain separate measured
candidates.

## Partitioned SeqScan Batch Source (Phase 66, complete)

- added plan-gated benchmark attribution for one logical range-partitioned
  table with 1, 2, 4, and 8 partitions. The real planner must produce
  PartitionedScan and every selected `PartitionAccessPlan` must be SeqScan;
  hand-built plans do not satisfy the benchmark gate;
- replaced the executor-private BatchPipeline's implicit single-table fields
  with a two-variant `BatchSource`: one SeqScan or one all-SeqScan
  PartitionedScan. No trait, boxed iterator, public API, or new physical plan
  was introduced;
- visits selected partitions in planner order through each storage's existing
  read view and `visit_rows_with_view_control`, validates the logical TableId,
  and carries one partial ExecutionBatch across partition boundaries. Only the
  final source exhaustion flushes a partial batch;
- propagates downstream `Break` through the current storage visitor and outer
  partition loop. Early Limit therefore skips later partitions, while the
  existing Aggregate and bounded Top-N consumers continue through every
  partition;
- accepts no mixed source. Any local IndexScan or RangeIndexScan makes the
  whole PartitionedScan use the retained materialized executor. Point/range
  storage capabilities still return owned vectors and are not pseudo-streamed
  by executor chunking;
- proves exact legacy row/order equivalence at 255/256/257/512/513 total rows,
  1/2/4/8 partitions, empty/single/unequal partitions, Heap and LSM, shared
  cross-partition batches, pending-error behavior, mixed point/range fallback,
  early Limit cancellation, and complete Aggregate/Top-N traversal;
- test-only statistics for 513 rows report every row seen, three delivered
  batches, and a maximum source batch of 256. A four-by-100-row `LIMIT 20`
  reports one partition visited and 20 rows seen; later partitions are not
  visited.

For eligible batch consumers, the PartitionedScan input intermediate changes
from `O(total partition rows)` to `O(256)` plus consumer state. Aggregate adds
group/state ownership and Top-N adds `O(K)` candidates. A full-output collector
still owns `O(N)` final QueryResult rows; Phase 66 removes only the extra source
intermediate. PhysicalPlan, planner policy, storage APIs and engines,
persistent formats, protocol, SDKs, dependencies, and unsafe-code usage remain
unchanged.

## Typed Primitive Aggregate Column-Batch Attribution (Phase 67, complete; pilot rejected)

- added plan- and result-gated attribution for global primitive aggregation:
  one SUM, same-column SUM/MIN/MAX, duplicate SUM, three distinct primitive
  columns, Filter plus same-column aggregates, nullable primitives, an
  all-SeqScan PartitionedScan, and LSM. Existing Text MIN/MAX and grouped
  aggregate cases remain non-target controls;
- evaluated an executor-private sidecar only for eligible global Bool/Int64/
  UInt64 aggregates. It deduplicated source positions, transposed each bounded
  row batch into typed value vectors plus explicit validity bits, and updated
  direct COUNT/SUM/MIN/MAX loops with checked arithmetic;
- verified during the pilot that 513 rows arrived as batches of 256, 256, and
  1, that three aggregates over one source created one typed column while three
  source positions created three columns, and that nullable and overflow
  behavior matched the row accumulator;
- rejected and removed the production pilot because the quick pre/post pair
  was internally inconsistent. Same-column SUM/MIN/MAX improved 12.4%, but
  duplicate SUM regressed 173.5%, Filter plus aggregate regressed 21.9%, the
  nullable target regressed 40.8%, and LSM changed by +4.4%. Broad controls also
  moved substantially, confirming material run-to-run noise;
- retains no typed sidecar, test-only sidecar statistics, new public API,
  dependency, unsafe code, planner policy, storage change, or persistent-format
  change. Production remains the row-owned `ExecutionBatch` plus
  `AggregateAccumulator`.

Phase 68 therefore selected the narrower HashJoin build-key ownership residual.
AND/OR short-circuiting, full batch Sort, and spilling remained separate
candidates. Any renewed column-layout work must first separate transposition
cost from consumption and demonstrate a consistent primitive-heavy benefit
across Heap, LSM, Filter, nullable, and partitioned inputs.

## Borrowed HashJoin Build Keys (Phase 68, complete)

- added real planner-produced right-build Text HashJoin attribution for unique
  8-byte and 128-byte keys, duplicate 128-byte keys, unique matching keys, and
  ordered duplicate matches. Every target requires direct SeqScan children,
  verifies left-probe/right-build key provenance and projected scan columns,
  forbids NestedLoopJoin/index access, and checks exact ordered results;
- changed the one shared production bucket builder used by streaming and
  materialized HashJoin from `HashMap<ScalarValue, Vec<usize>>` to
  `HashMap<&ScalarValue, Vec<usize>>`. The complete right rows remain separate
  immutable local owners throughout build and probe; there is no
  self-referential struct;
- continues standard-library RandomState and ScalarValue value Hash/Eq. Equal
  Text contents from distinct allocations share one logical bucket, NULL is
  excluded, and duplicate indices retain right input order;
- test-only statistics prove 513 unique Text rows produce 513 borrowed keys and
  513 indices, while 513 rows over four Text values produce four logical keys
  and 513 ordered indices. Both report zero owned build-key clones; pointer
  checks tie the stored map key to the first authoritative right-row String;
- retains the fixed fully materialized right build, streamed left probe,
  complete residual evaluation, owned output projection, fixed right-build
  policy, PhysicalPlan, storage APIs, dependencies, unsafe-code usage, and all
  persistent/public contracts.

The quick pair supports KEEP because the ownership result is exact and the
implementation is small: short unique Text improved 15.8%, matching Text
improved 36.7%, and Text/Int64 relative cost shrank, although long Text improved
only 2.3%, duplicate Text regressed 5.8%, and broad non-target movement confirms
substantial machine noise. HashJoin build-key ownership is closed here. Phase
69 should separately attribute AND/OR short-circuiting with runtime-error timing
audited first, or measure full Sort/spill and dynamic HashJoin build-side
materialization/choice. General column batches remain rejected absent new
evidence.

## Safe Filter AND/OR Short-Circuit (Phase 69, complete)

- added plan-, base-column-, and result-gated predicate-order pairs for
  Aggregate Filter, projected borrowed streaming Filter, filtered COUNT, and a
  primitive control. Equivalent pairs scan the same columns and return the
  same result while moving a selective primitive branch before or after
  repeated Text or primitive comparisons;
- introduced the executor-private `BoundFilterPredicate`, which first reuses
  `bind_expression` and then conservatively validates column identity and
  field metadata, literal type/nullability, logical/NOT/IS NULL Bool metadata,
  and comparison compatibility. Referenced source columns must additionally
  agree with the attached storage schema by identity, semantic type, and
  nullability. Successful binding with unsafe or unproven metadata stays on
  eager bound evaluation; binding failure retains the dynamic row-dependent
  fallback;
- recursively skips only the right side of `FALSE AND right` and
  `TRUE OR right`. TRUE AND, FALSE OR, and both UNKNOWN-left cases evaluate the
  right side and reuse the existing SQL `TruthValue` combination semantics;
- shares that boundary across batch Filter, direct and projected borrowed
  streaming Filter, materialized legacy Filter, and filtered COUNT. General
  scalar evaluation, DML, dynamic evaluation, NestedLoopJoin, and HashJoin
  residual predicates remain eager;
- proves all 18 AND/OR three-valued combinations against eager evaluation,
  exact right-branch access counts, recursive nested/NOT/IS NULL behavior,
  metadata-ineligible eager errors, missing-column timing, coordinated
  plan/predicate type lies against the runtime schema, wrong runtime types,
  Heap/LSM execution equivalence, and unchanged Join behavior;
- retains no public API, dependency, unsafe code, planner/compiler rewrite,
  PhysicalPlan or inspection change, protocol/SDK change, or persistent-format
  change.

The decision is **KEEP**. Deterministic structure reports zero right accesses
for FALSE AND and TRUE OR, one for TRUE AND, FALSE OR, UNKNOWN AND, and UNKNOWN
OR, and zero accesses to both right branches in nested decisive expressions.
Quick timing remains noisy: the Aggregate Text AND cheap/expensive median ratio
moved from 1.715x to 1.159x but remained inverse; Text OR moved from 1.713x to
0.397x, filtered COUNT AND was 0.723x post, primitive AND was 0.408x post, and
the projected streaming pair was neutral at 1.017x. The mixed absolute movement
precludes a throughput claim but does not systematically contradict the exact
skipped-work result.

## Statistics-Guided Smaller-Side HashJoin Build (Phase 70, complete)

- added exact ANALYZE and direct-plan gates to the asymmetric Int64 and Text
  HashJoin benchmarks before changing production, plus explicit 64×512 and
  512×64 no-match controls;
- added the executor-private `HashJoinBuildSide`. Eligible direct SeqScan ×
  SeqScan INNER HashJoin builds left only when both last-ANALYZE row counts
  exist and `left < right`; ties and either missing statistic build right;
- preserves logical PhysicalPlan left/right and the existing fixed-right
  materialized fallback. Unsupported metadata rejects specialization before
  execution, while errors after execution begins propagate without replay;
- BuildRight retains the historical materialized-right/stream-left output
  path. BuildLeft materializes left with the same borrowed-key RandomState
  buckets, streams right in at-most-256-row batches, evaluates the complete
  eager logical-left/logical-right predicate, and move-flattens per-left output
  buckets to preserve exact left-major/right-minor order;
- structural tests prove 64×4096 selects BuildLeft, 4096×64 and the 513×513 tie
  select BuildRight, missing statistics select BuildRight, and build keys still
  own zero clones. Equivalence covers batch boundaries, Bool/Int64/UInt64/Text,
  NULL, duplicates, residuals, projections, self joins, Heap/LSM, and stale
  statistics;
- retains PhysicalPlan, planner join-algorithm selection, inspection v3,
  public/storage/protocol/SDK APIs, persistent formats, dependencies, and safe
  Rust boundaries.

The decision is **KEEP**. The 64×4096 Int64 no-match median improved 20.8% and
the corresponding short/long Text cases improved 38.3%/41.5%. The asymmetric
4096-row ratio moved from 1.276x to 0.851x. Quick-run noise remains visible:
the unchanged 4096×64 right-build control moved +18.8% while its 512×64 control
moved -8.4%, and duplicate Text no-match moved +6.7%. The exact reduction from
4,096 owned build rows to 64 in the targeted orientation and broad semantic
coverage support KEEP without inventing a ratio threshold.

The choice follows the last ANALYZE `row_count`, not an exact runtime count;
stale statistics may choose the actually larger side without changing results.
HashJoin build-side structure closes here. Phase 71 should measure the full
Sort/spill boundary, or a storage-level range visitor only if new workloads
justify it, before selecting larger algorithmic work. Do not continue HashJoin
micro-tuning or reopen the rejected Phase 67 column-sidecar without new
evidence.

## Full Sort Memory & Spill Boundary Attribution (Phase 71, complete)

- added real-SQL, real-planner Full Sort benchmarks with exact physical-tree,
  base-scan-column, no-LIMIT, and complete ordered-result gates;
- sweeps narrow unique Int64, duplicate primitive keys, unique multi-key order,
  filtered input, all four explicit NULL direction/placement combinations,
  fixed 8-byte Text, fixed 128-byte retained and hidden Text keys, four-way
  PartitionedScan, Heap, and LSM controls;
- compares every important ordered shape with a no-ORDER-BY control returning
  the same final row count and width, while retaining Phase 64 Top-N as a
  separate small-K control;
- adds only `#[cfg(test)]` `FullSortStats`, populated by the production private
  sorting function. The 513-row boundary reports 513 rows before/after Sort;
  hidden 128-byte Text reports 1,026 scalar slots and 65,664 logical owned Text
  payload bytes before its one-column final projection;
- records that current stable ties are an external-run ordinal constraint and
  that every runtime key must still be validated before any future result is
  exposed;
- retains the production in-memory stable Sort, PhysicalPlan, fully owned
  QueryResult, public APIs, configuration, dependencies, safe Rust, and every
  protocol and persistent format.

The decision is **DEFER spill**. Across three serial quick runs, the median of
per-run medians at 4,096 rows was 754,167 ns for narrow unique Sort versus
765,917 ns without Sort (0.985x), 964,125 versus 1,043,166 ns for retained long
Text (0.924x), and 985,416 versus 764,125 ns for hidden long Text (1.290x).
Hidden and retained ordered long-Text medians differed by only 1.022x. Multi-key
primitive and partitioned Sort controls showed visible work at 1.698x and
2.273x relative to their controls, but they do not show that disk runs would
remove the dominant owned-result cost.

Full Sort input ownership remains `O(N input rows)`, while a fully owned result
returning N rows has an Ω(N final output) lower bound. Spill could remove some
wide hidden-key residency or sorting scratch; it cannot reduce total query
memory to `O(run_size)` under the current contract. Phase 72 should therefore
select another independently measured large feature rather than external Sort
infrastructure. A storage-level range visitor remains a candidate only if a new
real workload invalidates the Phase 66 selectivity boundary. If future evidence
specifically isolates hidden wide keys, first compare a compact indirect
key/index representation against disk spill.

## Costed Index Nested-Loop Join (Phase 72, complete)

- added pre-production real-SQL Heap attribution for matching 8×4,096 and
  disjoint 64×4,096 analyzed joins, with exact HashJoin/SeqScan and ordered
  result gates before the operator existed;
- added explicit `PhysicalPlan::IndexNestedLoopJoin` only for INNER direct
  Scan × Scan equality joins over distinct non-partitioned tables, with both
  table statistics and an analyzed ordered point access path on logical right;
- reuses the point lookup cardinality and storage-neutral startup/match cost,
  checked in `u128`, and compares it with the same `managed_page_count` SeqScan
  cost used by Filter planning as well as NestedLoop work. Ties and unsupported
  or incomplete metadata preserve the existing plan;
- streams logical left in at-most-256-row owned batches, skips probes for NULL,
  consumes one unchanged Heap/LSM point result at a time, evaluates the complete
  eager predicate, and preserves exact left-major/right-minor output;
- adds executor-private structural statistics and coverage for duplicates,
  residuals, projection, empty and NULL input, batch boundaries, Heap/LSM,
  malformed plans, and exact ordering;
- exposes a distinct inspection node with explicit logical keys, right table,
  opaque access path, required columns, and only the real left child. Statement
  Inspection JSON advances conditionally to v5; non-index plans remain v3 and
  partition plans remain v4;
- changes no storage API, persistent format, network protocol, deployment
  manifest, compiler semantics, dependency, unsafe boundary, or automatic
  analyze policy.

The decision is **KEEP** after review correction. The first row-count comparison
selected the measured-slower 256/512 and Text/64 point plans; using the existing
SeqScan managed-page work moves those cases back to HashJoin without a magic
outer-size threshold. This remains a deliberately narrow right-index
alternative, not join reordering, composite probing, an index-only join, a
partition-aware join, or a general optimizer framework. Stale statistics can
choose a slower operator but cannot alter results.

## IndexJoin Cost Calibration Attribution (Phase 73, complete; static calibration rejected)

- audited NestedLoop/Hash row-work units separately from the storage-neutral
  IndexJoin point-versus-sequential-access comparison;
- documented exact `StorageAccessCostHints` semantics and retained the checked
  shared Filter/Join fallback `1 + tree_height + estimated_matches`;
- added repeated Heap direct-probe, unique/no-match crossover, duplicate,
  inner-size, Text, Filter/range, and LSM controls without changing executor,
  PhysicalPlan, inspection, or lookup semantics;
- confirmed IndexJoin is consistently faster through the largest independently
  measured SQL point at outer 16, while outer 32 and above select Hash in both
  fixture states and therefore cannot supply a forced Index reference;
- rejected a Heap-only-page sequential-cost pilot because actual Heap SeqScan
  traverses colocated B+Tree/catalog pages; restored total managed-page work;
- leaves Heap hints as `None` and LSM dynamic hints unchanged. No static point
  constant simultaneously improves the conservative boundary, explains
  indexed-layout Hash cost, and preserves honest fixed-probe semantics.

The planner boundary for unique 4,096-row Heap inner input remains between
outer 16 and 32. Phase 74 may investigate a richer access/scan layout model
only if a real workload needs it; it should not add an outer-row threshold or
continue tuning this benchmark in isolation.
