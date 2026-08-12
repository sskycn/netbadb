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
migrated. Dirty writeback is not WAL ordering and does not provide crash-safe
transaction durability.

## Phase 2 — Transaction + WAL Foundation (next)

- transaction IDs and transaction API;
- transaction lifecycle and initial isolation model;
- versioned WAL records and LSN allocation;
- commit durability, checkpoints, recovery tests, and explicit rollback.

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
