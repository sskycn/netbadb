# NetbaDB architecture

## Boundaries

NetbaDB keeps application language concerns at the frontend boundary. A Go,
Rust, or future schema frontend should produce the same Canonical Schema IR:
tables, columns, physical types, semantic types, nullability, keys, and
relationships. The core consumes that representation and does not inspect Go
types or Rust application structs.

In this graph, `A -> B` means A depends on B. The current crate graph is:

```text
netbadb-schema -> netbadb-types
netbadb-hir -> netbadb-parser + netbadb-schema + netbadb-types
netbadb-rel -> netbadb-types
netbadb-compiler -> netbadb-hir + netbadb-parser + netbadb-rel
                    + netbadb-schema + netbadb-types
netbadb-planner -> netbadb-rel + netbadb-types
netbadb-storage -> netbadb-schema + netbadb-types
netbadb-executor -> netbadb-planner + netbadb-rel + netbadb-storage
                    + netbadb-types
netbadb-core -> compiler + planner + executor + storage + schema + types
netbadb-sdk -> netbadb-core + netbadb-executor + netbadb-schema
               + netbadb-types
```

No lower layer depends on a higher layer. In particular, storage does not
depend on planner or executor, and executor does not depend on an SDK.

## Canonical Schema IR

`netbadb-schema` stores database meaning in explicit Rust structs that are
independent of any application language. A column has:

- stable `ColumnId`;
- a name;
- a `TypeSpec` containing physical type and optional semantic name;
- nullability;
- primary-key metadata.

The Rust types are an implementation of this in-memory IR, not a persistence
format. Serialization and compatibility rules can be added later without
making JSON the execution representation.

## Compiler and plans

The first query subset follows:

```text
source → AST → resolved/type-checked HIR → logical plan → physical plan
```

HIR owns source-level resolution and semantic type checking. Relational IR
owns relational meaning and column provenance. The planner only chooses the
currently available sequential scan. The executor evaluates typed expressions
against rows returned by storage.

The implementation uses IDs and owned values between layers. It does not keep
long-lived references to pages, frames, or tuples, leaving room for future
buffer management and concurrent execution without spreading lifetimes across
the whole system.

## Storage boundary

`netbadb-storage` currently provides:

- a fixed 4 KiB page abstraction;
- a synchronous `PageManager` over a database file;
- a heap file with a small header and data pages;
- physical scalar encoding/decoding;
- row validation against the supplied Canonical Schema.

The API is intentionally narrow: insert and scan return owned values and stable
`RowId`s. Page allocation, page I/O, and row encoding remain below the
executor. WAL, buffer eviction, transactions, indexes, and recovery can be
introduced behind this boundary.

## Embedded and server modes

`netbadb-core` is synchronous and embedded. A future server crate may add an
async network layer around the same core API, but async should not leak into
parser, compiler, planner, executor, page, or storage internals without a
measured need.

Rust applications use the native SDK. Go applications should use a generated
SDK over a versioned protocol once server mode exists. FFI is an optional
escape hatch, not the primary Go integration strategy.
