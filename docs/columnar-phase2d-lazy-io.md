# Columnar Phase 2D: lazy indexed I/O

Phase 2D changes the current Columnar representation from a resident decoded
copy into an indexed immutable reader. Heap and LSM remain authoritative, DML
still writes only the authoritative engine and NBCL, and explicit transactions
still avoid Columnar.

```text
NBCM v3
   |
   +-- NBCS v3 header + resident footer directory
   |       +-- zone maps and block references (resident)
   |       `-- source-version / projected-column blocks (lazy)
   `-- NBCD v2 header + resident descriptor/footer directory
           +-- suppressed keys and live key -> DeltaRowRef (resident)
           `-- projected after-image blocks (lazy)

ColumnarScan -> zone prune -> optional version blocks -> required chunks
             -> existing vector Filter / Project / Aggregate
```

## Format audit and compatibility

NBCS v1/v2 chunks contain lengths, but no directory that can be located without
parsing all preceding row groups. Their trailing whole-file CRC also requires a
complete read before any chunk is trusted. NBCD v1 similarly places descriptors
before one complete after-image column set and protects the whole file with one
CRC. Those formats cannot provide independently verified lazy reads without
changing their declared integrity semantics.

Readers therefore retain the existing eager paths for NBCM/NBCS v1 snapshots
and NBCM/NBCS v2 plus NBCD v1 incremental chains. New builds, refreshes,
advances, and compactions publish NBCM v3, NBCS v3, and NBCD v2. Compaction is
the explicit migration path from a legacy incremental chain to the indexed
representation; open never rewrites a legacy file.

NBCM v3 preserves the prior identity, source token, frontier, count, byte, and
inventory meanings. It adds explicit Base format, Delta format, and integrity
scheme fields. The manifest remains small and has its own trailing CRC32C.

NBCS v3 is byte-addressed (blocks require no alignment). Its fixed header
contains the repeated projection/source/schema identity, engine kind, counts,
footer offset and length, footer checksum, and a header checksum. Its
checksummed footer repeats identity/counts and holds, for every row group:

- row count and optional hidden source-version block reference;
- exact projected column identity/type/nullability;
- NULL count, typed min/max zone map, and logical encoded byte count;
- payload offset, length, and CRC32C.

NBCD v2 uses the same independently checksummed header/footer/block model. Its
footer retains batch boundaries and mutation descriptors. Each insert/update
descriptor names a global after-row index; open converts that index into an
immutable `DeltaRowRef` without decoding its values. After-images are split
into bounded row groups with one block per projected column.

Open validates header/footer checksums, repeated identities, format/mode,
frontier continuity, counts, bounded lengths, single-file names, unique Delta
inventory, data-area bounds, and non-overlap before exposing a reader. A data
block is CRC-verified again immediately before decode, and decoded chunk
identity/statistics must agree with the directory. An individual payload block
is capped at 64 MiB and an index/footer at 256 MiB.

## Runtime and memory invariants

The indexed representation owns file handles plus decoded directory metadata.
Base vectors and Delta after-image values are not retained. `live` maps exact
`StorageVersionKey` to `DeltaRowRef`; the suppression set remains exact physical
versions. A Base-only open therefore reports zero resident value bytes. These
are representation counters, not OS RSS or page-cache estimates.

Zone maps are evaluated before any Base data or source-version read. A snapshot
projection reads no hidden version blocks. An incremental projection also reads
none while its suppression set is empty; once suppression exists, it reads a
version block only for each Base group that survived zone pruning. Delta reads
are grouped by referenced after-image group and decode only SQL-required
columns. Base statistics are never attributed to Delta.

`ColumnarScanStatistics::bytes_read` keeps its existing logical selected-vector
meaning. Phase 2D adds physical Base-data, Base-version, and Delta-data byte
counters, physical block reads, verified blocks, decoded column/version chunks,
and groups pruned before data I/O. Projection inspection reports legacy versus
lazy representation, format versions, resident metadata/value bytes, index
bytes and entries, and descriptor bytes/counts.

## Generation ownership, compaction, and failure behavior

Each indexed reader holds immutable file handles through a generation lease.
Advance snapshots carry earlier same-generation leases forward. After a new
compacted generation and manifest are durable, retirement marks every lease in
the old generation; physical deletion occurs only after the last old reader
drops its handle. This preserves the active-reader guarantee without a mutex,
global registry, unsafe code, mmap, or cache.

Lazy compaction streams the old Base through the reader in physical order,
skips exact suppressed versions, then reads live Delta versions in deterministic
version-key order. It retains only one output row group while writing NBCS v3.
It does not scan Heap/LSM, read NBCL, or alter the applied frontier.

A selected-block checksum or decode failure returns no partial `QueryResult`.
For an autocommit query, Core quarantines the exact projection, replans the same
statement using the already captured authoritative read view, and returns the
authoritative result. Later planning omits the unavailable projection and
inspection retains its diagnostic. Explicit transactions never enter this path.

## Measured 2026-09-06 matrix

`columnar_lazy_io_phase2d` uses 4,096-row groups, 80 Text columns in the wide
shape, a three-numeric-column query, and a zone-friendly range that retains one
group. Times are observations, not assertions.

| rows | width | segment bytes | resident metadata | resident values | open us | blocks/decode | physical Base bytes | mean query us |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 100,000 | 4 | 3,261,684 | 7,296 | 0 | 39 | 3 / 3 | 99,978 | 51 |
| 100,000 | 16 | 9,442,884 | 25,392 | 0 | 79 | 3 / 3 | 99,978 | 50 |
| 100,000 | 64 | 34,167,684 | 97,776 | 0 | 165 | 3 / 3 | 99,978 | 42 |
| 100,000 | 128 | 80,342,884 | 200,888 | 0 | 275 | 3 / 3 | 99,978 | 42 |
| 1,000,000 | 4 | 32,612,884 | 68,016 | 0 | 89 | 3 / 3 | 99,978 | 44 |
| 1,000,000 | 16 | 94,418,644 | 244,512 | 0 | 309 | 3 / 3 | 99,978 | 44 |
| 1,000,000 | 64 | 341,641,684 | 950,496 | 0 | 1,268 | 3 / 3 | 99,978 | 45 |
| 1,000,000 | 128 | 803,358,644 | 1,956,488 | 0 | 2,239 | 3 / 3 | 99,978 | 46 |

For the required 1M x 128 case, 31,360 Base blocks are indexed, but the query
reads and decodes exactly three. Physical query bytes are determined by the
three requested chunks in one retained group, not the other 125 columns.

The same benchmark also builds a streaming 1M-row incremental Base with 64
numeric columns, advances it with 100K updates (10%), drops the first reader,
and reopens before scanning two projected Delta columns. NBCD uses 256-row
groups and deliberately performs no Delta zone-map pruning in this phase.

| Base rows | Delta rows | open us | resident metadata | suppression keys | Delta row refs | resident after-images | descriptor/index bytes | Delta data read | blocks/chunks | query us |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1,000,000 | 100,000 | 20,256 | 16,401,287 | 3,200,000 B | 4,800,000 B | 0 B | 7,356,424 | 1,660,972 B | 782 / 782 | 96,264 |

All 245 Base groups are pruned before data or hidden-version I/O. The 782
verified blocks are exactly two requested chunks across the 391 Delta groups
that contain live references; none of the other 62 columns is read or decoded.
The suppression and row-reference byte figures count their typed key/value
payloads and intentionally do not pretend to be allocator- or RSS-exact.

```bash
NETBADB_COLUMNAR_LAZY_ROWS=100000,1000000 \
NETBADB_COLUMNAR_LAZY_WIDTHS=4,16,64,128 \
NETBADB_COLUMNAR_LAZY_ITERATIONS=3 \
cargo bench -p netbadb-core --bench columnar_lazy_io_phase2d
```

Set `NETBADB_COLUMNAR_LAZY_DELTA=0` only when a quick Base-only development run
is desired; the default formal run includes the fixed 1M/100K Delta case.

## Work deferred beyond Phase 2D

Phase 2D deliberately adds no decoded chunk cache, mmap, unsafe zero-copy,
compression, dictionary encoding, SIMD, async prefetch, aggregate spill, global
CSN, `RowEntityId`, or authoritative Columnar placement. The matrix shows stable
three-block query work and does not establish decoded-block reuse as the
dominant bottleneck, so a cache is not justified here. [Phase
2E](columnar-phase2e-maintenance-controller.md) subsequently adds explicit,
caller-driven bounded maintenance selection; it does not add a background
worker or cache.
