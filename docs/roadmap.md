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

Phase 2A has no rollback or isolation: abort is a logged state transition, not
physical undo, and changes are not hidden from other operations. It also does
not replay WAL after a crash. The WAL has no checksum in this format version.

## Phase 2B — Recovery (next)

- analysis/redo/undo policy and WAL replay on open;
- crash-reopen guarantees for committed and incomplete transactions;
- explicit rollback behavior;
- checkpoints, WAL retention/truncation policy, and checksums;
- deterministic torn-write and recovery tests.

## Phase 3 — Query execution

- richer expression typing and null semantics;
- insert/update/delete plans;
- joins, sort, aggregate, and explain output;
- deterministic execution and result-shape diagnostics.

## Phase 4 — Indexing and planning

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
