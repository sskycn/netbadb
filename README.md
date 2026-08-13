# NetbaDB

NetbaDB is a strongly typed relational database core written in Rust.

The project separates application-language schemas from the database engine
through a language-independent Canonical Schema IR. Rust provides the native
embedded API. Go and future languages are intended to use generated SDKs or a
versioned NetbaDB protocol client rather than coupling the database core to an
application runtime.

> NetbaDB is experimental. The implemented subset is intentionally small, but
> it is a real Rust workspace with a parser-to-storage vertical slice.

## Architecture

The durable architectural boundary is:

```text
Application language schema
        ↓
Language frontend / SDK
        ↓
Canonical Schema IR
        ↓
Parser → HIR + type checking
        ↓
Typed Relational IR
        ↓
Optimizer / Planner
        ↓
Executor
        ↓
Transaction boundary
        ↓
Storage
```

The current embedded path is synchronous:

```text
Rust Schema API
    ↓
SELECT / INNER JOIN + typed INSERT / UPDATE / DELETE statement parser
    ↓
Typed HIR
    ↓
Logical query / DML statement plan
    ↓
Sequential scan + nested-loop join physical plan
    ↓
Join / filter / projection / limit + RowId mutation executor
    ↓
Heap
    ↓
Transaction lifecycle + versioned WAL
    ↓
Buffer pool (guards, pinning, dirty writeback)
    ↓
Slotted pages
    ↓
Page manager / database file
```

The core does not depend on Go, a network runtime, JSON execution IR, or
application-specific Rust structs.

## Strong types

Internal identifiers are newtypes such as `TableId`, `RelationBindingId`,
`ColumnId`, `PageId`, and `RowId`. A relation binding identifies one
query-local table occurrence, so two aliases of the same `TableId` remain
distinct in a self join. Schema columns preserve both a physical representation and an
optional nominal semantic type:

```text
physical: UINT64
semantic: UserId
```

`UserId` and `TeamId` therefore remain distinct even when their physical
representation is the same. The storage format encodes physical values; the
Canonical Schema remains the source of semantic meaning. Each validated table
also has a versioned canonical byte encoding and SHA-256 schema fingerprint.
Heap metadata persists that fingerprint, and reopen requires the caller's full
table identity—including semantic types and column order—to match.

## Repository layout

```text
netbadb/
├── Cargo.toml
├── rust-toolchain.toml
├── crates/
│   ├── netbadb-types/       shared IDs, physical and semantic types
│   ├── netbadb-schema/      language-independent Canonical Schema IR
│   ├── netbadb-parser/      small typed-query AST and parser
│   ├── netbadb-hir/         name resolution and type checking
│   ├── netbadb-rel/         typed logical relational IR
│   ├── netbadb-compiler/    AST → HIR → logical plan
│   ├── netbadb-planner/     logical plan → physical plan
│   ├── netbadb-storage/     transactions, WAL, pages, buffer pool, and heap
│   ├── netbadb-executor/    synchronous physical-plan execution
│   └── netbadb-core/        native embedded database API
├── sdk/
│   ├── rust/                Rust application-facing re-export surface
│   └── go/                  Go SDK / protocol direction and contract notes
├── docs/
├── examples/
└── tests/
```

In the dependency graph below, `A -> B` means that crate A depends on crate B.
The direction is acyclic:

```text
schema -> types
hir -> parser + schema + types
rel -> types
compiler -> hir + parser + rel + schema + types
planner -> rel + types
storage -> schema + types
executor -> planner + rel + storage + types
core -> compiler + planner + executor + storage + schema + types
Rust SDK -> core + executor + schema + types
```

Storage has no dependency on the planner or executor. The executor consumes a
physical plan and a safe storage API. The page layer currently uses no
`unsafe`; future binary-layout or mmap work must remain localized and audited.

## Implemented now

The current code genuinely supports:

- Cargo workspace compilation and unit/integration tests;
- Canonical schema definitions with nullable, primary-key, physical, and
  semantic type metadata, unified validation, and stable schema fingerprints;
- parser support for `SELECT`, qualified columns, `AS` and shorthand table
  aliases, chained `JOIN`/`INNER JOIN ... ON`, explicit-column single-row `INSERT`, `UPDATE`,
  `DELETE`, optional DML `WHERE`, `LIMIT`, wildcard projection,
  `AND`/`OR`/`NOT`, comparisons, `IS NULL`/`IS NOT NULL`, integer/string/
  boolean/NULL literals, and parentheses;
- name resolution and expression type checking with nominal semantic types and
  explicit nullability;
- typed query/DML HIR and logical relational IR;
- sequential-scan and left-major/right-minor nested-loop join physical planning;
- synchronous heap storage with fixed 4 KiB pages;
- version 3 slotted heap pages with persistent pageLSNs, explicit deleted-slot
  tombstones, and checked bounds;
- synchronous buffer-pool guards with pinning, dirty tracking, flush, and
  bounded eviction;
- versioned little-endian WAL records for begin, full-page update, commit,
  abort, and rollback completion, with strong LSNs and per-transaction prevLSN
  chains;
- explicit transaction handles plus implicit statement transactions;
- commit durability through WAL sync and WAL-before-data-page writeback;
- lazy single-writer admission and synchronous physical runtime rollback;
- synchronous startup recovery with analysis, repeat-history redo, and
  reverse-LSN undo of incomplete or aborted transactions;
- explicit quiescent checkpoints with bounded two-generation WAL retention,
  monotonic logical LSNs, and persistent transaction-ID high-water marks;
- RowId-based insert, update, delete, scan, file reopen, row encoding, and row
  decoding;
- executor support for INNER JOIN, filter, projection, limit, typed DML, affected-row
  results, SQL three-valued boolean logic, and NULL comparisons;
- a native embedded `netbadb-core::Database` API.

The experimental storage format uses versioned heap metadata and slotted pages.
Heap metadata version 2 adds the canonical table-schema fingerprint; version 1
is rejected rather than guessed or migrated. Phase 2A bumped
data pages from version 1 to version 2 to add pageLSN. Phase 3B bumps them to
version 3 because a formerly invalid slot encoding now means Deleted. Versions
1 and 2 are rejected rather than guessed or migrated. Files created by the
pre-Foundation sequential `HEAP` page prototype are likewise not migrated.

Each database uses two alternating WAL slots named `<database>-wal` and
`<database>-wal.next`. Creation uses create-new semantics and refuses to
overwrite an existing database or WAL slot. A successful checkpoint retains
only the current generation; at most one superseded slot can remain after an
interrupted rotation and is cleaned on open or the next checkpoint.
`Database::insert` runs as an implicit transaction. Call
`begin_transaction`, `insert_in`, and `Transaction::commit` when several
inserts must share one WAL chain, or call `Transaction::rollback` (equivalently
`abort`) to remove their physical effects. A successful commit means its
commit record has reached durable storage; heap pages may remain buffered until
eviction, `flush`, or `close`.

The current full-page-image model permits one writer per open database object.
Writer ownership is acquired lazily by the first write, so read-only
transactions do not reserve it. Commit releases ownership only after the
Commit record is durable. Rollback first makes Abort durable, follows the
transaction's prevLSN chain backward, installs and synchronizes each validated
before-image (or removes newly allocated trailing pages), then durably records
RollbackComplete and releases ownership. A failed commit or rollback remains
pending and retains the writer for retry.

Dropping an unfinished dirty writer does not silently release it: the open
storage becomes recovery-required for subsequent writes, and `close` reports
an error. `flush` remains legal during an active transaction because the engine
uses STEAL and WAL-orders each page write; flush success does not mean commit.
Readers are not isolated and may observe an active writer's buffered changes.

`Database::checkpoint` and `HeapStorage::checkpoint` are explicit synchronous
quiescent checkpoints. They return a typed error instead of waiting whenever a
transaction handle remains outstanding, a writer is active/pending, or runtime
health requires startup recovery. A successful checkpoint first flushes the
current WAL, WAL-orders and synchronizes every dirty page, then creates and
synchronizes the next WAL generation. Commit, rollback, and clean read-only
drop unregister their transaction handle; `close` also rejects any still-live
read-only transaction so it cannot invalidate that handle's prevLSN chain.

`Database::open` and `HeapStorage::open` synchronously recover before exposing
the buffer pool. Recovery classifies transactions with a Commit record as
winners, RollbackComplete transactions as already physically undone, and
incomplete or Abort-only transactions as losers. It redoes non-rolled-back page
updates in ascending LSN order while using pageLSN to skip installed images,
then undoes losers in global descending LSN order from full before-images.
After synchronizing physical undo, startup appends and flushes Abort when
needed plus RollbackComplete for each recovered loser. A crash before that
completion is durable safely repeats history and deterministic undo; later
opens skip transactions whose completion is durable.

An incomplete final WAL record caused by EOF is discarded at the recovery
boundary only when its available header bytes are structurally valid. Invalid
magic, versions, tags, lengths, transaction chains, middle records, and page
images remain hard errors. Existing data pages are fully validated before
their pageLSN can suppress redo. The active WAL generation currently has no
checksum. WAL header format v2 separates physical file offsets from logical
LSNs: for a record at physical offset `P`,
`LSN = base_lsn + (P - 48)`. A checkpoint chooses the old logical end as the
new base, so LSNs never move backward even though physical WAL bytes are
recycled and historical pageLSNs remain unchanged.

Startup validates both WAL slots and deterministically chooses the valid slot
with the greatest consistent generation. A truncated newly-created header is
an interrupted rotation and falls back to the last valid slot; a corrupt newer
complete generation is a hard error. Recovery scans only the selected
post-checkpoint generation. Clean shutdown markers are intentionally omitted:
the bounded current generation is scanned on open, avoiding a second persistent
state machine whose marker would need invalidation before writes.

Phase 2C still does not provide MVCC, reader isolation, fuzzy or background
checkpoints, concurrent writers, or cross-process writer coordination. A
successful explicit `close` rejects every outstanding transaction and then
WAL-orders and flushes dirty pages; WAL recycling remains an explicit
checkpoint operation.

The query language is a deliberately small native subset, not a claim of full
SQL compatibility. Database NULL is represented explicitly as
`ScalarValue::Null`, while Rust `Option` remains reserved for absent clauses or
metadata. Untyped NULL literals receive a semantic type from their expression
context. Comparisons with NULL evaluate to UNKNOWN; `IS NULL` and `IS NOT NULL`
are the explicit tests. `AND`, `OR`, and `NOT` use SQL three-valued logic, and a
`WHERE` filter retains only TRUE, rejecting both FALSE and UNKNOWN. Heap writes
independently enforce schema nullability, including through the embedded insert
API.

Typed DML uses the same compiler, transaction, full-page WAL, rollback, and
recovery path as heap writes. `Database::execute` returns either query rows or
an explicit `AffectedRows(u64)` result; `query` rejects mutating statements.
Single-row INSERT requires an explicit column list. Omitted nullable columns
become NULL, while omitted non-nullable columns are rejected. UPDATE evaluates
all right-hand sides against the original row, and UPDATE/DELETE reuse the
SELECT predicate evaluator, so FALSE and UNKNOWN do not mutate a row.

Mutation is located by an internal `RowId` (`PageId + SlotId`) that is never
exposed as a SQL column. DELETE compacts tuple bytes without renumbering slots;
the slot becomes an explicit tombstone and is not reused. UPDATE rebuilds the
current 4 KiB page while preserving the slot. If a larger replacement cannot
fit on that page, the statement returns `UpdateWouldOverflowPage` and rolls
back atomically; row relocation and forwarding pointers are not implemented.
Implicit DML owns one transaction. `execute_in` supports multiple statements
in an explicit transaction; until savepoints exist, an execution-time DML
failure rolls back that whole transaction.

Typed INNER JOIN resolution assigns deterministic `RelationBindingId` values
in source order. An alias hides the underlying table name. Qualified columns
resolve through the exposed relation name; unqualified columns are accepted
only when exactly one visible relation provides the name. Each `ON` expression
can see the complete left subtree and its current right relation, but not later
joins. HIR preserves nominal types and requires BOOL (nullable BOOL is valid).
At execution, only TRUE matches; FALSE and UNKNOWN, including `NULL = NULL`,
do not. `SELECT *` emits columns in left-to-right relation/schema order, and
the nested-loop operator preserves duplicates in deterministic left-major,
right-minor order.

The core composes multiple unchanged one-table heap files with
`Database::create_tables`/`open_tables`; `insert_into` targets a table for
embedded data loading. No page, heap, WAL, recovery, checkpoint, or transaction
format changed for JOIN. Multi-table write transactions remain unsupported.

## Go and protocol strategy

Go is no longer treated as the database implementation language. The intended
support boundary is:

```text
Rust: native core and embedded SDK
Go:   generated SDK and NetbaDB protocol client
```

The Go directory currently documents the boundary; no Go runtime implementation
or protocol wire format existed in the starting repository, so none is
pretended to be complete. A future `netbadbd` server must make the protocol
versioned and language-neutral before the Go client is generated around it.

## Development

The repository pins Rust 1.97.1 with `rustfmt` and `clippy` in
`rust-toolchain.toml`; rustup installs it automatically when needed. The
workspace MSRV is Rust 1.85.0. Run:

```bash
cargo fmt --all -- --check
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo +1.85.0 check --workspace --all-targets
```

Convenience targets are available through `make`:

```bash
make fmt-check
make check
make clippy
make test
make msrv-check
```

The Go SDK notes are under [`sdk/go`](sdk/go/README.md). Once executable Go
client code exists, it should be tested from that module with `go test ./...`.

## Roadmap

The implementation sequence is intentionally vertical:

1. Rust foundation — stable types, schema, parser, HIR, and relational IR.
2. Storage Foundation — versioned slotted pages, checked page decoding,
   bounded buffer pool, guards, dirty writeback, heap insert/scan, and reopen.
3. Transaction + WAL Core (Phase 2A) — transaction lifecycle, versioned WAL,
   LSN/pageLSN, durable commit, and WAL-ordered page writeback.
4. Recovery (Phase 2B) — startup analysis, repeat-history redo, reverse-LSN
   undo, crash-tail handling, and crash-reopen guarantees.
5. Single Writer + Runtime Rollback (Phase 2B.1) — lazy writer ownership,
   retryable commit/rollback states, physical before-image undo, and
   crash-during-rollback safety.
6. Checkpoint + WAL Lifecycle (Phase 2C) — quiescent recovery boundaries,
   monotonic logical LSNs, and crash-safe bounded WAL generation recycling.
7. Typed expressions + NULL semantics (Phase 3A) — contextual NULL typing,
   expression nullability, three-valued logic, and explicit NULL predicates.
8. Typed DML (Phase 3B) — typed insert/update/delete plans, stable-RowId page
   mutation, affected-row results, and atomic WAL-backed execution. Complete.
9. Join execution (Phase 3C) — qualified columns, aliases, typed INNER JOIN,
   self joins, nested-loop execution, and NULL-aware join predicates. Complete.
10. Aggregate + Sort (Phase 3D) — `ORDER BY`, aggregate functions, `GROUP BY`,
    and aggregate NULL semantics.
11. Indexing — B+Tree and planner access-path selection.
12. Server mode — protocol, sessions, and `netbadbd`.
13. SDKs and tooling — generated Go client, CLI, LSP, and MCP.
14. Advanced optimization — statistics, cost model, and rewrite rules.

Isolation/MVCC, B+Tree indexes, server networking, and Go wire-protocol code
are roadmap items, not implemented features in this slice.
See [`docs/architecture.md`](docs/architecture.md) and
[`docs/roadmap.md`](docs/roadmap.md) for the maintained design notes.

## License

NetbaDB is licensed under the [GNU Affero General Public License v3.0 or later](LICENSE).

This project is identified as `AGPL-3.0-or-later` in its Cargo package metadata.
