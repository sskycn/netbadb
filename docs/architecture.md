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

## Typed expressions and NULL

Database NULL is an explicit `ScalarValue::Null`; it is not represented by
Rust `Option<ScalarValue>`. `Option` continues to mean that syntax or metadata,
such as a `WHERE` clause, is absent. Parser NULL literals begin untyped. HIR
assigns them a semantic type from the surrounding boolean or comparison
context without granting NULL an arbitrary nominal identity.

HIR and relational expressions carry an expression type consisting of:

```text
SemanticType + nullable
```

Column nullability originates in Canonical Schema IR. Literal values other than
NULL are non-nullable. A comparison is nullable when either operand is
nullable, boolean `AND`/`OR`/`NOT` preserve possible UNKNOWN results, and
`IS NULL`/`IS NOT NULL` always produce a non-null BOOL. This expression
nullability is distinct from schema nullability: a nullable column may contain
NULL, while an expression over that column may or may not return NULL.

`IS NULL` and `IS NOT NULL` remain explicit AST, HIR, and relational nodes;
they are not lowered to equality with NULL. Ordinary `=`, `!=`, `<`, `<=`,
`>`, and `>=` comparisons return UNKNOWN if either operand is NULL, including
`NULL = NULL`. Nominal compatibility checks still apply to non-NULL operands,
so contextual NULL typing cannot make `UserId = TeamId` legal.

The executor centralizes boolean conversion as three truth values:

```text
Bool(true)  → TRUE
Bool(false) → FALSE
NULL        → UNKNOWN
```

`AND`, `OR`, and `NOT` use the SQL three-valued truth tables. A filter keeps a
row only when its predicate is TRUE; FALSE and UNKNOWN both reject the row.
Storage's existing scalar tag for NULL round-trips through heap pages, buffers,
the database file, and reopen. `HeapStorage` validates every embedded write and
returns `StorageError::NullNotAllowed` when NULL targets a non-nullable column,
independently of query compilation.

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
- `TransactionManager` allocates strong `TxnId` values, appends `Begin`, and
  owns the per-open-database writer/health state. A transaction tracks
  `Active`, `CommitPending`, `RollbackPending`, `Committed`, or `RolledBack`
  and owns its last LSN. Writer ownership is acquired lazily before the first
  heap mutation; read-only transactions do not reserve it.
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

The WAL uses two alternating files, `<database>-wal` and
`<database>-wal.next`. Appending writes a complete record to the selected
generation but does not imply durability. `WalManager` separately tracks the
highest written and durable logical LSN, and `flush_through` advances durability
with `sync_data`. LSN zero is reserved for “no LSN”. Physical WAL offsets and
logical LSNs are deliberately different:

```text
logical LSN = generation base_lsn + (physical record offset - 48)
```

The first generation starts at logical LSN 1. A new generation's base is the
old generation's logical end, which is strictly greater than every record LSN
that existed there. Physical offsets can therefore restart at byte 48 without
making historical pageLSNs incomparable or reusable.

The WAL file header is:

```text
0..4    NBWL magic
4..6    u16 WAL format version (2)
6..8    u16 header size (48)
8..16   u64 generation ID (starts at 1)
16..24  u64 base logical LSN (non-zero)
24..32  u64 checkpoint LSN (zero means no prior checkpoint)
32..40  u64 next transaction ID high-water mark (non-zero)
40..48  reserved bytes (zero)
```

WAL v1 is rejected explicitly; this experimental format has no migration
framework. The data-page layout remains version 2 because its existing u64
pageLSN already stores the logical value and its binary representation did not
change.

Every record has a 40-byte fixed header followed by a bounded payload:

```text
0..4    WREC magic
4..6    u16 record format version (1)
6       u8 record type (Begin=1, PageUpdate=2, Commit=3, Abort=4,
                        RollbackComplete=5)
7       reserved byte (zero)
8..12   u32 total record length
12..16  u32 payload length
16..24  u64 logical LSN
24..32  u64 transaction ID
32..40  u64 prevLSN (zero only for Begin)
40..    payload
```

`Begin`, `Commit`, `Abort`, and `RollbackComplete` have no payload.
`PageUpdate` stores an explicit u64 page ID, one 4 KiB before-image, and one 4
KiB after-image. Consequently, the maximum accepted record is 8,240 bytes. The
scanner validates magic, versions, reserved bytes, lengths, truncation, stored
LSN, transaction state, and the prevLSN chain before exposing a record. The
format does not yet include a checksum; checksum selection remains an explicit
future format decision.

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

Runtime rollback uses:

```text
Active
    → append + flush Abort
    → RollbackPending
    → follow this transaction's prevLSN chain backward
    → validate and install each full before-image
      (zero before-image removes the exact trailing page)
    → sync each affected rollback page or truncation
    → append + flush RollbackComplete
    → RolledBack
```

Before-images restore their historical pageLSN; rollback does not generate
ordinary PageUpdate records. A rollback error leaves `RollbackPending` and
retains writer ownership, so calling `rollback` again safely repeats the
idempotent physical undo. Only the affected rollback page is flushed per undo
step. Commit remains NO-FORCE; rollback synchronizes its physical changes before
reporting success.

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
    ├── Completed rollback (RollbackComplete exists)
    └── Losers (incomplete or Abort-only)
    │
    ▼
Redo non-rolled-back PageUpdates in ascending LSN
    │
    ▼
Undo losers in descending global LSN
    │
    ▼
Sync undo + durably finalize recovered losers
```

Analysis builds transaction lastLSNs and an LSN lookup. Redo repeats history,
including loser updates, but skips transactions with durable
RollbackComplete: an existing page is skipped only when its pageLSN is at least
the update LSN, and that pageLSN is trusted only after full page validation;
otherwise the validated after-image is installed. A new page must be exactly
the next trailing page, so WAL cannot create page-ID gaps. Undo follows each
incomplete or Abort-only loser's prevLSN chain through a max-heap and installs
PageUpdate before-images in global descending LSN order. A zero before-image
means the loser allocated that page, which can only remove the exact trailing
page. The page file is synchronized, then recovery appends Abort for any loser
that was still Active, appends RollbackComplete, and flushes those terminal
records before returning. This prevents a recovered loser from conflicting
with or overwriting a later winner on another restart.

Because full-page after-images can include another transaction's uncommitted
contents, the runtime permits one writer and acquires that ownership before any
heap page mutation or allocation. CommitPending and RollbackPending retain it.
Commit durability or completed physical rollback releases it. Dropping an
unfinished dirty writer marks the open storage recovery-required; later writes
and close fail, while read-only handles may still be created. Analysis also
rejects historical retained WAL where a committed page update follows an
unresolved loser update to the same page before recovery writes any page. This
is a single-writer safety invariant, not general isolation or cross-process
locking.

The algorithm intentionally has no compensation log records. During runtime
rollback, Abort is durable before physical undo and RollbackComplete becomes
durable only after all rollback page changes are synchronized. A crash before
completion therefore leaves an Abort-only loser: startup repeats its history
and deterministically undoes the whole prevLSN chain. Startup recovery uses the
same ordering when it finalizes a loser after undo. A durable completion record
means the already-synchronized transaction images must be skipped, so a later
committed winner is never overwritten by reapplying the old rollback. The
retained valid WAL is synchronized before any recovery page write.

At startup only an incomplete final record whose available header is
structurally valid may be truncated at EOF. Corrupt middle records, invalid
magic/version/type/length fields, invalid transaction state, broken transaction
chains, malformed data pages, and malformed before/after page images fail open
with typed errors.

The current model is single-writer, STEAL, NO-FORCE, WAL-protected, and supports
synchronous physical runtime rollback plus startup crash recovery. `abort` is
an alias for that rollback operation. Reads have no snapshot or visibility
isolation and may observe active-writer pages. There is no MVCC, fuzzy
checkpoint, B+Tree, concurrent writer queue, or cross-process writer lock.

## Checkpoint and WAL lifecycle

The first checkpoint model is intentionally quiescent. `TransactionManager`
registers every successful `Begin`, including read-only handles, and unregisters
exactly once after durable commit, completed rollback, or clean active drop.
Dropping a dirty/pending writer unregisters the vanished handle but changes
runtime health to `RecoveryRequired`. Checkpoint admission requires:

```text
writer = Idle
runtime health = Healthy
outstanding transaction handles = 0
```

It never waits or queues. Active, CommitPending, RollbackPending, read-only
active, and RecoveryRequired states return typed errors. Clean close enforces
the same no-outstanding-handle safety property so no live transaction can retain
a prevLSN into recycled history.

The checkpoint order is:

```text
verify quiescence
    → flush WAL through the highest written LSN
    → flush every dirty buffer frame (each preserves WAL-before-page)
    → synchronize the database file
    → capture old logical end + next TxnId
    → remove only the inactive older WAL slot
    → create the inactive slot with generation + 1 and the captured metadata
    → synchronize the new WAL file and its parent directory
    → switch the shared WalManager to that generation
    → delete the superseded slot and synchronize its directory entry
```

The synchronized data file is the checkpoint success foundation: every effect
represented by retired records is already durable before creation of the next
generation begins. `BufferPool`, `TransactionManager`, and `HeapStorage` retain
the same shared `WalManager`, so switching cannot split WAL ownership.

The two slots form a small crash-safe selection mechanism rather than a general
segment manager. Rotation never removes the currently selected valid slot.
Before the new header is complete, open ignores a truncated inactive header and
uses the old generation. Once a complete new header is durable, both files may
remain and open selects the greater generation after checking consecutive IDs,
base/checkpoint continuity, and the TxnId high-water mark. A malformed complete
newer candidate is a hard error. Open deletes a validated superseded generation
before exposing the selected manager. A successful checkpoint therefore keeps
one WAL file; a crash or cleanup failure may temporarily leave two, and the next
open or checkpoint deterministically removes the older one.

Recovery runs the existing analysis/redo/undo algorithm only over records in
the selected latest safe generation. Old pageLSNs are not reset. Post-checkpoint
updates receive logical LSNs above the generation base, so redo can compare them
directly against pages written before checkpoint. The header's `next_txn_id`
preserves transaction identity monotonicity after old records are recycled.

There is no clean-shutdown marker. Scanning one bounded active generation is
simple and deterministic; a marker would require a separate durable
clean-to-dirty invalidation state machine before the next mutation and does not
currently remove enough work to justify that risk.

## Embedded and server modes

`netbadb-core` is synchronous and embedded. A future server crate may add an
async network layer around the same core API, but async should not leak into
parser, compiler, planner, executor, page, or storage internals without a
measured need.

Rust applications use the native SDK. Go applications should use a generated
SDK over a versioned protocol once server mode exists. FFI is an optional
escape hatch, not the primary Go integration strategy.
