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
SELECT / FROM / WHERE / LIMIT parser
    ↓
Typed HIR
    ↓
Logical relational plan
    ↓
Sequential-scan physical plan
    ↓
Filter / projection / limit executor
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

Internal identifiers are newtypes such as `TableId`, `ColumnId`, `PageId`, and
`RowId`. Schema columns preserve both a physical representation and an
optional nominal semantic type:

```text
physical: UINT64
semantic: UserId
```

`UserId` and `TeamId` therefore remain distinct even when their physical
representation is the same. The storage format encodes physical values; the
Canonical Schema remains the source of semantic meaning.

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
  semantic type metadata;
- parser support for `SELECT`, `FROM`, `WHERE`, `LIMIT`, wildcard projection,
  boolean operators, comparisons, integer/string/boolean literals, and
  parentheses;
- name resolution and type checking with nominal semantic types;
- typed HIR and logical relational IR;
- sequential-scan physical planning;
- synchronous heap storage with fixed 4 KiB pages;
- version 2 slotted heap pages with persistent pageLSNs and checked bounds;
- synchronous buffer-pool guards with pinning, dirty tracking, flush, and
  bounded eviction;
- versioned little-endian WAL records for begin, full-page update, commit,
  abort, and rollback completion, with strong LSNs and per-transaction prevLSN
  chains;
- explicit transaction handles plus implicit single-insert transactions;
- commit durability through WAL sync and WAL-before-data-page writeback;
- lazy single-writer admission and synchronous physical runtime rollback;
- synchronous startup recovery with analysis, repeat-history redo, and
  reverse-LSN undo of incomplete or aborted transactions;
- insert, scan, file reopen, row encoding, and row decoding;
- executor support for filter, projection, and limit;
- a native embedded `netbadb-core::Database` API.

The experimental storage format uses versioned slotted pages. Phase 2A bumps
data pages from version 1 to version 2 to add pageLSN; old pages are rejected
rather than guessed or migrated. Files created by the pre-Foundation
sequential `HEAP` page prototype are likewise not migrated.

Each database file has a retained sibling WAL named `<database>-wal`.
Creation uses create-new semantics and refuses to overwrite either an existing
database file or an existing WAL path.
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

`Database::open` and `HeapStorage::open` synchronously recover before exposing
the buffer pool. Recovery classifies transactions with a Commit record as
winners, RollbackComplete transactions as already physically undone, and
incomplete or Abort-only transactions as losers. It redoes non-rolled-back page
updates in ascending LSN order while using pageLSN to skip installed images,
then undoes losers in global descending LSN order from full before-images.
Repeating recovery is idempotent even without compensation log records because
each restart first repeats the same history and then applies the same
deterministic undo.

An incomplete final WAL record caused by EOF is discarded at the recovery
boundary only when its available header bytes are structurally valid. Invalid
magic, versions, tags, lengths, transaction chains, middle records, and page
images remain hard errors. Existing data pages are fully validated before
their pageLSN can suppress redo. The retained WAL currently has no checksum.

Phase 2B.1 still does not provide MVCC, reader isolation, checkpoints, WAL
recycling, bounded WAL growth, concurrent writers, or cross-process writer
coordination. A successful explicit `close` rejects unresolved writers and then
WAL-orders and flushes dirty pages.

The query language is a deliberately small native subset, not a claim of SQL
compatibility. `NULL` parsing is recognized but rejected by the current type
checker because null semantics are not implemented yet.

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
6. Checkpoint + WAL Lifecycle (Phase 2C) — bounded recovery start points,
   clean-shutdown metadata, and WAL truncation/recycling.
7. Query execution — richer expressions, null semantics, and write commands.
8. Indexing — B+Tree and planner access-path selection.
9. Server mode — protocol, sessions, and `netbadbd`.
10. SDKs and tooling — generated Go client, CLI, LSP, and MCP.
11. Advanced optimization — statistics, cost model, joins, and rewrite rules.

Checkpoints, WAL recycling, isolation/MVCC, B+Tree indexes, server networking,
and Go wire-protocol code are roadmap items, not implemented features in this
slice.
See [`docs/architecture.md`](docs/architecture.md) and
[`docs/roadmap.md`](docs/roadmap.md) for the maintained design notes.

## License

TBD.
