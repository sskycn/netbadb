# Adaptive Operations Phase 36 — Columnar artifact write bounds

Phase 36 proves two previously unknown current-state components without adding
an admission dimension or changing a persistent or wire format:

- application-level bytes written by the initial immutable Columnar base
  artifact writer (`NBCS` plus `NBCM`); and
- the prospective SSTable extent scanned by Snapshot Columnar after its
  ordinary nonempty-LSM flush.

The proof is storage-authored and metadata-only. Inspection does not scan or
sample values, decode Heap rows or SSTable data blocks, run ANALYZE, flush or
compact storage, allocate a `ColumnarProjectionId`, create a pending NBPC
intent, or cache a report.

## Output accounting

`ColumnarBaseArtifactMode` distinguishes Snapshot from Incremental and has no
default. `ColumnarBaseArtifactWriteBoundInspection` reports the source row and
row-group upper bounds, source scalar-payload upper bound, final NBCS extent,
NBCS application-write bytes, NBCM write bytes, and their total.

For one initial build the writer creates exactly one base NBCS and one NBCM.
The output component is:

```text
segment_write_bytes_upper_bound
    = segment_file_bytes_upper_bound + LAZY_SEGMENT_HEADER_BYTES

total_write_bytes_upper_bound
    = segment_write_bytes_upper_bound + manifest_write_bytes_upper_bound
```

The additional header term is required because production first writes a
zero-filled 132-byte NBCS header and later seeks back and writes the final
header. Payload blocks and the footer are written once. Temporary-file rename
does not write their payload again. Directory metadata, fsync/device
amplification, NBPC lifecycle publications, Change Stream bytes, database WAL,
memory, CPU, elapsed time and filesystem capacity are outside this component.

Storage derives the final segment extent from the existing v3 encoder layout:
fixed header, per-group payload blocks, incremental source-version blocks,
footer header, every group directory, block references, statistics and
checksum. Fixed-width values use their exact production widths; Bool is one
byte per row. Text and Bytes include one `u32` offset per row plus the group
terminator, validity bits and raw payload. Across disjoint groups, chosen
minimum payloads and chosen maximum payloads are each bounded by the source
column payload. The proof therefore charges one shared variable payload budget
for data and four statistic copies: min/max in the payload block and min/max in
the footer.

Incremental additionally charges the exact 24-byte production source-version
key for every possible row. Snapshot charges no source-version block. The
manifest bound uses the existing layout, column specs, fixed incremental
metadata when present, checksum, and the maximum decimal widths of a valid
projection ID and generation in `projection-<id>-g<generation>.nbcs`. Inspection
does not reserve or predict either identity.

Every operation uses checked arithmetic. Overflow is
`StorageError::ResourceBoundOverflow`, never saturation, wrapping, or
`NotProven`. Sizing is O(projected columns) for Heap and O(current SSTables plus
projected columns) for LSM. Row-group overhead is multiplied algebraically; no
prospective group, bitmap, block, row or vector is allocated.

## Source structure

For Heap, storage reports:

```text
row_upper_bound
    = current managed pages
      × maximum slot-directory entries representable by one valid Heap page

source_scalar_payload_bytes_upper_bound
    = current validated main-file extent
```

This intentionally counts non-Heap managed pages and every representable slot.
It is not a live-row count and does not decode a page.

For LSM:

```text
row_upper_bound
    = current SSTable physical entries + current MemTable physical entries
```

Versions and tombstones count. Incremental scalar payload is bounded by current
SSTable extents plus resident encoded MemTable payload accounting. Snapshot
with an empty MemTable uses current SSTable extents.

For a nonempty Snapshot MemTable, the existing production flush theorem is
reused unchanged. Flush adds at most one SSTable whose extent is at most
`LsmMaintenanceBoundInspection::write_bytes`; it does not compact or remove
the preexisting SSTables. Therefore:

```text
prospective_post_flush_sstable_bytes_upper_bound
    = current total_sstable_bytes + flush_bound.write_bytes
```

This value bounds both Snapshot's source persistent-read component and its
scalar-payload input to the artifact proof. The separate prerequisite
work/read/write components remain unchanged and are not added to source read.
LSM source work units remain `NotProven`.

## Admission and compatibility

Core maps the storage total to the existing
`bounds.output_write_bytes = Bounded(total)` for initial Heap/LSM Snapshot and
Incremental Columnar builds. Nonempty Snapshot LSM now reports prospective
source read as `Bounded`; Index output remains `NotProven` until a separate
BTree/WAL/catalog writer theorem exists.

Existing Phase 34 and Manifest v11/NBOP v6 policies consume this stronger
evidence. `AtMost(M)` means admit when the current engine proves the component
is at most M. It is not a permanent disable switch: deployments must use
`allow_physical_columnar_apply = false`, `allow_snapshot = false`, or
`allow_incremental = false` for that purpose. A policy constraining only output
is partial admission and says nothing about unconstrained source,
prerequisite, memory or whole-mutation cost.

No proposal, request, response, receipt or persistent field changes. Manifest
remains v11, NBOP v6, NBMR v3, Native Protocol v2 and PostgreSQL wire unchanged.
NBPC/NBPM v2, NBCM/NBCS/NBCD, Heap, BTree/Index Catalog, LSM
manifest/SSTable/WAL, Change Stream and database WAL bytes are unchanged.

Regression coverage ties fixed format helpers to production encoders, exercises
all frozen physical types and nullability, Text/Bytes and mixed projections,
zero rows, default row-group boundaries 1/255/256/257/512/513, Snapshot and
Incremental writers, manifest maximum identity widths, typed overflow, Heap
geometry, LSM prospective flush extent, admission equality/one-below behavior,
inspection purity and reopen/recovery suites. Actual application writes are
computed as final NBCS extent plus the second header write plus NBCM extent and
must not exceed the pre-build bound.

Still deferred are Index output writes, Columnar peak memory, whole-mutation
cost, CPU/time, filesystem capacity, cumulative quotas and automatic Physical
Design. A later phase should begin with a separately justified component proof,
not a universal format maximum or an inferred total.
