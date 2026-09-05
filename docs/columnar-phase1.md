# Columnar Phase 1

Columnar Phase 1 adds an immutable analytical projection while keeping Heap or
LSM as the only authoritative table storage. A projection is derived state: it
does not accept writes, participate in commit, replace `StorageRegistry`, or
appear as an index access path. Dropping it never changes table rows.

## Ownership and freshness

Core owns a `ProjectionRegistry` beside `PhysicalBindings` and
`StorageRegistry`. Each entry has a stable `ColumnarProjectionId`, a monotonic
`ColumnarGeneration`, one logical `TableId`, one exact source `StorageId`, a
canonical `SchemaFingerprint`, projected `ColumnId`s, and an equality-only
`StorageSnapshotToken` authored by the source engine.

Heap tokens contain that Heap's committed sequence. LSM tokens combine its WAL
generation with the exact maximum committed version across mutable and
immutable runs. A build or refresh first flushes an LSM source, making that
pair stable across a clean close/reopen. A later commit changes the maximum;
once flushed, the WAL generation also changes and prevents later tombstone
compaction from making an old token equal again. Allocator and general manifest
generations are excluded because they can advance on a rollback or metadata-only
rewrite. Tokens from different storage identities or engine kinds are never
ordered or compared.

`Database::build_columnar_projection` performs this sequence synchronously:

1. Resolve a Single placement, stabilize an LSM WAL generation when needed,
   and open one fixed committed read view `B`.
2. Scan the requested columns through `B`.
3. Write and sync immutable segment and manifest temporary files.
4. Release the scan, re-read the source token, and abort with temporary cleanup
   if it differs.
5. Atomically install the segment and manifest, sync the parent directory, and
   only then publish the in-memory registry entry.

The mutable `Database` borrow prevents a second operation through that handle
between the final check and publication. A deterministic test hook commits in
the scan/check window and proves that no manifest is published. Refresh repeats
the protocol with the same projection ID and the next generation, then retires
the previous immutable segment only after the new manifest is durable.

A committed INSERT, UPDATE, or DELETE changes the source token and makes the
projection stale. Rollback leaves it fresh. Planning omits stale entries, so
queries fall back to Heap or LSM. Explicit transactions conservatively use the
existing authoritative planning path in Phase 1.

## Persistent format

The experimental format has two independently checksummed little-endian files:

- `projection.nbcmanifest` starts with `NBCM`, version 1. It records projection,
  generation, table and source identities, schema fingerprint, opaque source
  token, row/row-group/segment counts, projected column types/nullability, the
  immutable segment filename, byte size, and segment CRC32C.
- `projection-<id>-g<generation>.nbcs` starts with `NBCS`, version 1. It repeats
  the identities and fingerprint, then stores one or more row groups. Every
  group stores each projected column as plain typed data plus a validity bitmap.
  Text uses checked `u32` offsets and one UTF-8 byte payload. Each column chunk
  records NULL count and optional typed minimum/maximum zone-map values.

Both files end with CRC32C over all preceding bytes. Readers bound file,
column, row-group, bitmap, offset, and payload sizes before allocation or
slicing; they reject bad magic/version, truncation, unknown tags, invalid UTF-8,
inconsistent counts, identity mismatches, and checksum failures. Publication
uses `create_new` temporary files, complete writes, `sync_data`, atomic rename,
and parent-directory `sync_all` for both the segment and manifest.

Managed databases now use the durable NBPC projection catalog described in
[Columnar Phase 1.5](columnar-phase1-5.md). Reopen automatically discovers its
registered locations and preserves a monotonic database-scoped identity
high-water. `Database::attach_columnar_projection` remains an explicit
adoption/repair path and validates table/schema/source identity and all segment
metadata. A failed or corrupt projection does not affect authoritative recovery
or queries. Inspection retains a registered unavailable identity with a
diagnostic.

## Planning and execution

Core converts only currently fresh, exact-context entries into
`ColumnarProjectionPlanningSnapshot`. This input is separate from BTree/LSM
`AccessPath`s. The planner first performs its existing index and partition
choices and column pruning. It may replace only a remaining `SeqScan` with an
independent `ColumnarScan` when every required column is present and transparent
integer work units are no greater than authoritative scan work. Existing point
and range index selections therefore keep precedence. Joins, sorts, partitions,
and explicit transactions retain their prior paths in Phase 1.

Columnar work includes fixed startup, row-group inventory, bytes for the
required columns only, typed decode work, and vector work for the estimated
rows. Safe literal constraints are compared with row-group zone maps before
costing, so groups that cannot match reduce estimated bytes and vector work.

The executor receives projection handles through a separate read-only context.
`ColumnarScan` produces storage-owned typed column vectors. Filter builds a
selection vector, Project selects or reorders vectors, and Aggregate consumes
selected vector values for COUNT, SUM, MIN, MAX, and GROUP BY. These operators
do not materialize an intermediate row collection. Scalar expressions and the
final `QueryResult` are explicit row boundaries. Simple literal comparison
conjuncts become safe zone-map constraints; every retained row still evaluates
the complete typed SQL predicate. `query_with_columnar_statistics` reports exact
row groups total/read/pruned and rows read.

## Embedded API

The Rust Core and embedded SDK expose:

- `ColumnarProjectionSpec::new` and optional row-group sizing;
- `build_columnar_projection`, `refresh_columnar_projection`,
  `attach_columnar_projection`, and `drop_columnar_projection`;
- `inspect_columnar_projections` and `inspect_columnar_projection_path`;
- `inspect_statement`, whose stable tree includes `ColumnarScan`;
- `query_with_columnar_statistics` for plan-linked scan counters.

Projection inspection reports the stored schema fingerprint, whether it matches
the current source schema, the opaque build token, health, stable identities,
projected columns, segment/row-group/row counts, bytes, and any unavailable-file
diagnostic.

Phase 1 intentionally omits authoritative columnar writes, CDC or incremental
maintenance, a database-global CSN,
partitioned projections, complex join/sort columnar pipelines, compression,
dictionary encoding, SIMD, spilling, protocol commands, PostgreSQL syntax, and
Go APIs.
