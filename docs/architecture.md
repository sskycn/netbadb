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

The synchronous storage path is now:

```text
Executor
    ↓
HeapStorage
    ↓
BufferPool + PageGuards
    ↓
PageManager
    ↓
Database file
```

`netbadb-storage` keeps the boundaries concrete and small:

- `PageManager` owns fixed-size file I/O, page allocation, checked page-offset
  arithmetic, and file sync. It does not interpret heap or index semantics.
- `BufferPool` owns a bounded set of raw page frames. It uses a simple
  round-robin eviction boundary, pins pages while guards are alive, refuses to
  evict pinned pages, writes dirty pages before reuse, and exposes explicit
  `flush_page`/`flush_all` operations.
- `Page` validates a versioned page header and explicit page type before
  exposing slotted-page operations. Heap pages use a slot directory at the
  front, free space in the middle, and tuple bytes packed from the end of the
  page backward.
- `HeapStorage` validates schema rows, encodes typed scalar values, chooses the
  last heap page or allocates a new one, and returns owned rows. Page guards do
  not escape these operations, so executor and query APIs carry no page
  lifetimes.

The experimental container retains the legacy `NBPG` file-root marker. Heap
metadata has its own `NBD1` marker and explicit version. Data pages use the
following little-endian layout:

```text
0..4    NBP1 page magic
4..6    u16 page format version
6       u8 page type (1 metadata, 2 heap)
7       reserved byte (zero)
8..10   u16 slot count
10..12  u16 free-space lower bound
12..14  u16 free-space upper bound
14..16  reserved bytes (zero)
16..    slot entries: u16 tuple offset + u16 tuple length
...     free space
...     tuple bytes, allocated from PAGE_SIZE backward
```

This is an intentional replacement of the pre-Foundation sequential `HEAP`
data-page layout. That earlier experimental format has no migration path; new
files use the versioned layout above.

All disk widths are explicit; no Rust struct layout, host-endian values,
pointers, or `usize` are persisted. Malformed headers, slot directories,
record ranges, row lengths, tags, and UTF-8 values return typed errors. The
current dirty-page writeback is not WAL ordering and is not crash-safe
transaction durability. WAL, transactions, MVCC, indexes, and recovery remain
future layers.

## Embedded and server modes

`netbadb-core` is synchronous and embedded. A future server crate may add an
async network layer around the same core API, but async should not leak into
parser, compiler, planner, executor, page, or storage internals without a
measured need.

Rust applications use the native SDK. Go applications should use a generated
SDK over a versioned protocol once server mode exists. FFI is an optional
escape hatch, not the primary Go integration strategy.
