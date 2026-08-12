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
Heap pages in a database file
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
│   ├── netbadb-storage/     pages, page manager, heap file
│   ├── netbadb-executor/    synchronous physical-plan execution
│   └── netbadb-core/        native embedded database API
├── sdk/
│   ├── rust/                Rust application-facing re-export surface
│   └── go/                  Go SDK / protocol direction and contract notes
├── docs/
├── examples/
└── tests/
```

The dependency direction is acyclic:

```text
types ──► schema
types ──► rel ──► planner
parser ──► hir ──► compiler ──► planner
schema ──► hir / compiler
schema + types ──► storage
planner + rel + storage ──► executor
compiler + planner + executor + storage ──► core
core ──► Rust SDK
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
- insert, scan, file reopen, row encoding, and row decoding;
- executor support for filter, projection, and limit;
- a native embedded `netbadb-core::Database` API.

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

Install a stable Rust toolchain with `rustfmt` and `clippy`, then run:

```bash
cargo fmt --check
cargo check --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Convenience targets are available through `make`:

```bash
make fmt-check
make check
make clippy
make test
```

The Go SDK notes are under [`sdk/go`](sdk/go/README.md). Once executable Go
client code exists, it should be tested from that module with `go test ./...`.

## Roadmap

The implementation sequence is intentionally vertical:

1. Rust foundation — stable types, schema, parser, HIR, and relational IR.
2. Minimal storage — database file, page manager, heap insert and scan.
3. Query execution — richer expressions, transactions, and write commands.
4. Indexing — B+Tree and planner access-path selection.
5. Durability — WAL, checkpoints, and recovery.
6. Server mode — protocol, sessions, and `netbadbd`.
7. SDKs and tooling — generated Go client, CLI, LSP, and MCP.
8. Advanced optimization — statistics, cost model, joins, and rewrite rules.

Transactions, MVCC, WAL, recovery, B+Tree indexes, server networking, and Go
wire-protocol code are roadmap items, not implemented features in this slice.
See [`docs/architecture.md`](docs/architecture.md) and
[`docs/roadmap.md`](docs/roadmap.md) for the maintained design notes.

## License

TBD.
