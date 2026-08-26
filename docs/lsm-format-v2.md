# NetbaDB LSM persistent formats v2

LSM v2 is a synchronous, single-process leveled layout. It preserves the v1
WAL transaction contract while replacing Manifest v1 and SSTable v1. This is
an experimental format change: v1 manifests and SSTables are rejected rather
than migrated implicitly.

## Identity, ordering, and authority

The physical user key remains `(clustering value, LsmRowId)` and the internal
key remains `(clustering value, LsmRowId, LsmCommitSeq ascending)`. Clustering
values are NOT NULL Int64 or UInt64 values; duplicates are legal.

Only `MANIFEST` selects live WAL and SSTable files. Unreferenced `.nbls`,
`.next`, and old WAL files are orphans and are never recovered by filename.
Every manifest-changing operation derives a candidate without modifying the
live state, syncs all output files, writes and syncs `MANIFEST.next`, renames it
over `MANIFEST`, and syncs the directory before installing the candidate in
memory. Inputs are deleted only after publication. A post-rename sync failure
poisons the live handle and requires reopen because either generation may be
selected after a crash.

## Manifest v2 (`NBLM`, version 2)

The fixed header retains storage/table/schema identity, generation, active WAL,
allocator high-water marks, and optional ANALYZE snapshots. Each bounded
SSTable descriptor now contains:

```text
SSTableId:       u64 little-endian
level:           u8, 0 <= level < 4
entry_count:     u64 little-endian
file_bytes:      u64 little-endian
bloom_bytes:     u64 little-endian
min_user_key:    typed clustering bits + LsmRowId
max_user_key:    typed clustering bits + LsmRowId
```

Descriptors are canonical: levels ascend; L0 files use ascending SSTable ID
(oldest to newest); L1+ use ascending physical minimum key. L0 ranges may
overlap. Within every L1+ level, `previous.max_user_key < next.min_user_key`.
Decode rejects unknown levels, duplicate IDs, stale SSTable ID high-water,
non-canonical order, overlap, reversed ranges, zero counts, and unbounded or
inconsistent sizes. The whole manifest is protected by CRC32C.

## SSTable v2 (`NBLS` / `NBLB`)

The v2 header binds the file to storage/table/schema/SSTable/level identity,
entry and block counts, physical min/max keys, exact file bytes, and one Bloom
descriptor. Header CRC32C covers those fields. The Bloom payload immediately
follows the fixed header and has its own CRC32C. Data blocks retain bounded
length/count, first/last physical keys, and per-block CRC32C.

A checksummed `NBLF` footer terminates the file. Its bounded sparse index stores
every block's exact offset, payload length, entry count, and first/last physical
key. The fixed trailer records the index byte length and checksum, so open can
seek from EOF and validate metadata without trusting an allocation-sized field
or discovering offsets by scanning data blocks. Index entries must cover the
packed block region exactly, with increasing offsets and canonical key ranges.
The format admits at most 262,144 blocks per SSTable, bounding the in-memory
index to 14 MiB before any allocation is attempted.

The per-SSTable Bloom filter is built from every represented clustering key,
including duplicate rows, old versions, and tombstones. Its persistent fields
are:

```text
algorithm:       1 (stable FNV-1a double hashing)
hash_count:      7
bit_count:       byte-aligned u64, bounded to 64 Mi bits
payload_length:  bit_count / 8
payload_crc32c:  CRC32C(bits)
bits:            payload bytes
```

Sizing is deterministic at ten bits per distinct clustering key, rounded up to
at least 64 bits. Hash input is one explicit physical-type tag followed by the
key's eight little-endian bits. Hash one uses FNV-1a offset
`0xcbf29ce484222325`; hash two uses offset `0x84222325cbf29ce4` and is forced
odd. Bit `i` is `(h1 + i*h2) mod bit_count`. Rust's randomized hashers are not
part of the format. Bloom corruption is a hard error; readers never treat a
corrupt filter as `maybe`.

## Leveled compaction

The bounded layout is L0 through L3. L0 compaction triggers at two files and
includes all current L0 files plus every overlapping L1 file. For L1 and L2,
the lowest overflowing level is selected first; targets are 256 KiB and 1 MiB
respectively, using a checked multiplier of four. The source file with the
lowest minimum key and then lowest ID is chosen. Source/target overlap expands
to a stable closure before merging.

Merge input is streamed one block per run. Normal compaction preserves all
versions and tombstones. Output targets 256 KiB and splits only between
clustering-key groups, so duplicate clustering values are never divided merely
to satisfy the target. A first streaming pass records only bounded output
descriptors; a second pass writes blocks and Bloom bits incrementally, so an
oversized duplicate-key group may exceed the target without becoming resident
as a whole. All outputs are synced and published in one manifest generation.
`compact()` drives until no current trigger remains.

`compact_full()` requires no writer, transaction, prepared transaction, or
read view. It merges every level into L3, retains only the newest Put for each
physical user key, and removes newest tombstones together with all superseded
history. This is safe because every possible older file participates and no
snapshot can observe the removed versions.

## Read paths and accounting

Point reads check MemTable, every overlapping L0 range, and binary-search the
canonical range run in each nonempty L1+ level. Bloom negatives avoid opening
data blocks. Range reads binary-search the first possible L1+ file and walk
only the overlapping run; Bloom is not consulted. Seq/range/point reads merge
sorted runs incrementally and retain only one current block per SSTable. Cursor
files are opened for one block read at a time, so a large legal manifest does
not require one simultaneously open file descriptor per selected SSTable.

Runtime read counters report candidate metadata checks, opened SSTables, data
blocks, Bloom checks, positives, and negatives. Runtime write counters report
logical MemTable bytes accepted by successful flush publication, physical
flush output bytes, physical compaction input/output bytes, and bytes made
obsolete by a successfully published compaction. Structural level/file/Bloom
bytes come directly from the authoritative manifest and opened SST metadata.

## Unchanged formats and scope

LSM WAL v1 (`NBLW` / `NBLR`) is unchanged. Canonical retry, validation of all
winner/loser/incomplete batches, and read-only recovery inspection remain
mandatory. Heap formats, CoordinatorLog, PartitionCatalog, Inspection JSON,
Protocol v1, and SDK schema formats are unchanged. There is no background
worker, async runtime, compression, block cache, secondary LSM index,
replication, or distributed placement.
