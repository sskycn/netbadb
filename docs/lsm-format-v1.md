# NetbaDB LSM persistent formats v1

> Historical experimental contract. Current code writes Manifest/SSTable v2
> and explicitly rejects v1 files; see [`lsm-format-v2.md`](lsm-format-v2.md).

NetbaDB's first LSM layout is a synchronous, single-process `TableStorage`
implementation. It is not a wrapper around Heap and does not use pages, Heap
WAL records, or B+Tree handles.

## Identity and key model

An LSM directory persists its nonzero `StorageId`, `TableId`, canonical schema
fingerprint, clustering `ColumnId`, and clustering physical type. Moving the
directory therefore does not change storage or row identity.

The clustering column must be `NOT NULL` and physically `Int64` or `UInt64`.
It is not a SQL uniqueness constraint. The physical user key is:

```text
(clustering value, LsmRowId)
```

`LsmRowId` is nonzero, monotonically reserved in durable ranges, stable across
restart, and never reused after deletion or compaction. Duplicate clustering
values remain legal.

All persistent integers use fixed-width little-endian encoding. All formats
have magic bytes, an explicit version, checked lengths/counts, and CRC32C.
Unknown versions and complete checksum failures are hard errors; there is no
guessed migration or automatic repair.

## Directory and authority

```text
table.lsm/
├── MANIFEST
├── MANIFEST.next
├── wal-<generation>.nblw
└── sst/
    ├── sst-<id>-l0.nbls
    └── sst-<id>-l1.nbls
```

Only `MANIFEST` is authoritative. A referenced missing file is corruption.
Unreferenced WAL/SSTable files and `.next` files are crash orphans and are
removed on open; they are never selected by filename recency.

## Manifest v1 (`NBLM`)

The checksummed manifest persists:

- format version and manifest generation;
- `StorageId`, `TableId`, and 32-byte schema fingerprint;
- clustering column and physical type;
- current WAL generation;
- reserved high-water marks for `LsmRowId`, physical `TxnId`, and
  storage-local `LsmCommitSeq`;
- next SSTable identity;
- optional last-`ANALYZE` live-row count, access cardinality/work, and
  clustering min/max statistics;
- a bounded list of SSTable identity, L0/L1 level, entry count, and min/max
  physical key.

Publishing writes and syncs `MANIFEST.next`, atomically renames it over
`MANIFEST`, and syncs the directory. Data files are never deleted before this
switch.

## LSM WAL v1 (`NBLW` / `NBLR`)

The WAL header binds a WAL generation to one `StorageId`. Each independently
checksummed bounded record is one of:

```text
MutationBatch(TxnId, [Put | Tombstone])
Prepare(TxnId, DatabaseTxnId)
Commit(TxnId, LsmCommitSeq)
Abort(TxnId)
```

Active changes live only in a bounded transaction overlay. Commit or prepare
canonicalizes the final row write set. Prepare syncs through `Prepare` and
retains writer ownership without publishing values. After the coordinator's
durable decision, `Commit` assigns a storage-local sequence and becomes the
visibility point. Retry reuses the same transaction, batch, and commit
sequence.

A structurally incomplete final record is a discardable crash tail. A complete
record with a bad checksum is corruption. Standalone open rejects a valid
unresolved prepare; only an explicit coordinator resolution may append Commit
or Abort.

## SSTable v1 (`NBLS` / `NBLB`)

An SSTable header binds the file to its `StorageId`, `TableId`, schema
fingerprint, immutable SSTable ID, L0/L1 level, clustering type, entry count,
block count, and min/max physical key. Entries are strictly ordered by:

```text
(clustering value, LsmRowId, LsmCommitSeq ascending)
```

Each entry contains a Put row payload or zero-length Tombstone. Put payloads
use the engine-neutral scalar row codec and preserve the original Heap bytes.
Every block records first/last key, bounded payload and entry counts, and a
CRC32C covering header plus payload. Open validates by streaming one bounded
block at a time and builds sparse block offsets. Point/range access prunes by
SSTable and block bounds and seeks only overlapping blocks; there is no
unbounded full-file read.

## MVCC, flush, and compaction

`StorageReadView::Lsm` captures an LSM-local visibility horizon. A database
read view still groups one opaque view per storage; no database-global
timestamp was introduced. The latest Put at or before the horizon is visible;
a latest Tombstone hides the key. A transaction overlay supplies
read-your-writes without exposing pending changes to other views.

Mutation handles bind `LsmRowId`, current clustering key, and the observed
committed version or pending revision. Stale handles are rejected. A key update
becomes a Tombstone for the old physical key plus a Put for the new key, with
the same row ID.

Commit means durable WAL state. Flush writes every committed MemTable version
to a new immutable L0, syncs a new WAL generation, publishes both in a new
manifest, then retires the old WAL. Retaining all versions keeps outstanding
snapshots valid.

Compaction is explicit, synchronous, and quiescent: no active/prepared
transaction or read view may exist. All L0 plus the old L1 become one canonical
L1. Only the latest committed Put remains; a latest Tombstone may be dropped
because every older SSTable participates. Output and manifest are synced
before obsolete files are deleted.

## MVP limits

- one `Int64`/`UInt64`, `NOT NULL` clustering column;
- duplicates through `(key, LsmRowId)` ordering;
- one writer per storage and multiple readers;
- bounded transaction overlays and MemTable lifecycle;
- synchronous flush and L0 plus one L1;
- quiescent synchronous compaction;
- no Bloom filters, compression, block cache, background threads, secondary
  LSM indexes, replication, or distributed placement.

Range-partition physical layout remains Heap-only. Heterogeneous partitions,
Protocol changes, and server-manifest changes remain out of scope.
