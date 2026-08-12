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
TransactionManager + WAL
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
  evict pinned pages, and exposes explicit `flush_page`/`flush_all`
  operations. Before writing a dirty data page it makes the WAL durable
  through that page's pageLSN. The data-page write is not attempted if the WAL
  flush fails.
- `Page` validates a versioned page header and explicit page type before
  exposing slotted-page operations. Heap pages use a slot directory at the
  front, free space in the middle, and tuple bytes packed from the end of the
  page backward.
- `TransactionManager` allocates strong `TxnId` values and appends a `Begin`
  record. A transaction tracks `Active`, `CommitPending`, `Committed`, or
  `Aborted` and owns its last LSN. Commit appends exactly one commit record and
  becomes committed only after `flush_through(commit_lsn)` succeeds.
- `HeapStorage` validates and encodes rows, constructs a candidate after-image,
  appends its `PageUpdate`, and only then publishes the page to the buffer
  frame. It no longer flushes the entire buffer after each insert. Page guards
  do not escape these operations, so executor and query APIs carry no page
  lifetimes.

The experimental container retains the legacy `NBPG` file-root marker. Heap
metadata has its own `NBD1` marker and explicit version. Data pages use the
following version 2 little-endian layout:

```text
0..4    NBP1 page magic
4..6    u16 page format version (2)
6       u8 page type (2 heap; tag 1 remains reserved)
7       reserved byte (zero)
8..10   u16 slot count
10..12  u16 free-space lower bound
12..14  u16 free-space upper bound
14..16  reserved bytes (zero)
16..24  u64 pageLSN (zero means no WAL record)
24..    slot entries: u16 tuple offset + u16 tuple length
...     free space
...     tuple bytes, allocated from PAGE_SIZE backward
```

This is an intentional replacement of both the pre-Foundation sequential
`HEAP` layout and Phase 1 page version 1. Neither experimental format has a
migration path; the decoder returns an unsupported-version error for version
1 rather than interpreting it as version 2.

## Transaction and WAL boundary

The WAL is a separate `<database>-wal` file. Appending writes a complete record
to the file but does not imply durability. `WalManager` separately tracks the
highest written and durable LSN, and `flush_through` advances durability with
`sync_data`. LSN zero is reserved for “no LSN”; real LSNs are byte offsets and
start after the 16-byte WAL file header.

The WAL file header is:

```text
0..4    NBWL magic
4..6    u16 WAL format version (1)
6..8    u16 header size (16)
8..16   reserved bytes (zero)
```

Every record has a 40-byte fixed header followed by a bounded payload:

```text
0..4    WREC magic
4..6    u16 record format version (1)
6       u8 record type (Begin=1, PageUpdate=2, Commit=3, Abort=4)
7       reserved byte (zero)
8..12   u32 total record length
12..16  u32 payload length
16..24  u64 LSN (the record's file offset)
24..32  u64 transaction ID
32..40  u64 prevLSN (zero only for Begin)
40..    payload
```

`Begin`, `Commit`, and `Abort` have no payload. `PageUpdate` stores an explicit
u64 page ID, one 4 KiB before-image, and one 4 KiB after-image. Consequently,
the maximum accepted record is 8,240 bytes. The scanner validates magic,
versions, reserved bytes, lengths, truncation, stored LSN, transaction state,
and the prevLSN chain before exposing a record. The format does not yet include
a checksum; checksum selection remains an explicit future format decision.

The write ordering invariant is:

```text
construct after-image with pageLSN
    → append PageUpdate
    → publish dirty buffer frame
    → flush WAL through pageLSN
    → write data page
```

Commit uses `append Commit → flush_through(commitLSN) → Committed`. If the
flush fails, the handle remains `CommitPending`; retrying commit flushes the
same record and does not append a duplicate. A new-page update is also flushed
before extending the database file, because writing the allocator's zero page
is itself a data-file write that must not overtake its WAL record.

All disk widths are explicit; no Rust struct layout, host-endian values,
pointers, or `usize` are persisted. Malformed headers, slot directories,
record ranges, row lengths, tags, and UTF-8 values return typed errors.

## Startup recovery

Recovery is synchronous storage-layer work and completes before a `BufferPool`,
`HeapStorage`, or `Database` is exposed:

```text
Database::open
    │
    ▼
Open Data File + WAL
    │
    ▼
Analysis
    │
    ├── Winners (Commit exists)
    └── Losers (incomplete or Abort)
    │
    ▼
Redo every PageUpdate in ascending LSN
    │
    ▼
Undo losers in descending global LSN
```

Analysis builds transaction lastLSNs and an LSN lookup. Redo repeats history,
including loser updates: an existing page is skipped only when its pageLSN is
at least the update LSN, and that pageLSN is trusted only after full page
validation; otherwise the validated after-image is installed. A new page must
be exactly the next trailing page, so WAL cannot create page-ID gaps. Undo
follows each loser's prevLSN chain through a max-heap and installs PageUpdate
before-images in global descending LSN order. A zero before-image means the
loser allocated that page, which can only remove the exact trailing page. The
page file is synchronized before recovery returns.

Because full-page after-images can include another transaction's uncommitted
contents, the runtime currently permits one active writer. A dirty transaction
that aborts or is dropped, or a dropped commit-pending transaction, makes
further writes require reopen/recovery. Analysis also rejects a retained WAL
where a committed page update follows a loser update to the same page, before
recovery writes any page. This keeps unsupported dirty write dependencies from
being mistaken for successful recovery; it is a single-writer safety invariant,
not general isolation.

The algorithm intentionally has no compensation log records. A crash during
redo or undo is handled by rerunning complete repeat-history redo followed by
the same deterministic undo, making restart recovery idempotent for this
full-page-image model. The retained valid WAL is synchronized before any
recovery page write.

At startup only an incomplete final record whose available header is
structurally valid may be truncated at EOF. Corrupt middle records, invalid
magic/version/type/length fields, invalid transaction state, broken transaction
chains, malformed data pages, and malformed before/after page images fail open
with typed errors.

Phase 2B supplies crash recovery, not transaction visibility or isolation.
`abort` appends an abort record; its physical effects are removed during
restart. There is no MVCC, checkpoint, WAL recycling, bounded WAL growth, or
runtime full rollback guarantee.

## Embedded and server modes

`netbadb-core` is synchronous and embedded. A future server crate may add an
async network layer around the same core API, but async should not leak into
parser, compiler, planner, executor, page, or storage internals without a
measured need.

Rust applications use the native SDK. Go applications should use a generated
SDK over a versioned protocol once server mode exists. FFI is an optional
escape hatch, not the primary Go integration strategy.
