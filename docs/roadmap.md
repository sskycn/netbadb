# NetbaDB roadmap

## Phase 0 — Rust foundation (current)

- Cargo workspace and dependency direction;
- canonical IDs, physical types, semantic types, and schema metadata;
- initial query parser;
- typed HIR with name resolution and nominal type checks;
- typed relational IR;
- logical-to-physical planner boundary;
- synchronous executor and error enums;
- tests for parser, type checking, planning, execution, and storage round trips.

## Phase 1 — Minimal storage

- database/catalog metadata beyond the single-table prototype;
- page validation and free-page management;
- heap insert and scan APIs for multiple tables;
- buffer manager with IDs, pinning, dirty tracking, and flush policy;
- integration tests for reopen and corruption detection.

## Phase 2 — Transactions and durability

- transaction IDs and transaction API;
- initial isolation model;
- WAL and commit records;
- checkpoints and recovery tests;
- explicit rollback behavior.

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
