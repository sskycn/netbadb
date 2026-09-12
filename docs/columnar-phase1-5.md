# Columnar Phase 1.5: scale, cost, and projection lifecycle

Phase 1.5 keeps Heap and LSM authoritative and hardens the immutable derived
Columnar projection. It adds a database-owned projection catalog, automatic
reopen discovery, durable monotonic identities, deterministic lifecycle crash
tests, structural planner costs, and a parameterized scale benchmark. It does
not add a writable column store or incremental maintenance.

## Ownership

```text
Schema Catalog ── tables/schema/placements ──┐
Storage Registry ── Heap/LSM identities ─────┼── Planner ── Executor
Projection Catalog ── inventory/location ────┤
Columnar NBCM ── current generation ──────────┘
       │
       └── immutable NBCS segment
```

The schema catalog remains the authority for `TableId`, schema fingerprints,
physical placement, `StorageId`, and database incarnation. `NBPC` is derived
metadata beside that schema catalog. It owns the database-scoped
`ColumnarProjectionId` allocator, registered inventory, relative location,
expected table/source/fingerprint, and the last observed generation. A dropped
ID is never reused.

`NBCM` is the sole current-generation authority. A refresh publishes and syncs
generation N+1 in NBCM before updating NBPC. Reopen accepts an NBCM generation
ahead of NBPC and forwards the catalog, which is the crash result after manifest
publication. It rejects an NBCM generation behind NBPC. NBCS contains immutable
physical row groups and repeats identity fields for validation.

Freshness remains an equality check between the source-authored
`StorageSnapshotToken` in NBCM and the current exact source storage. NBPC does
not duplicate the token or segment inventory.

## Historical NBPC v1

NBPC v1 is the original Phase 1.5 layout. Current binaries read it only as a
migration source and durably rewrite the catalog and marker as [NBPC
v2](projection-catalog-v2.md). Its active inventory, incarnation, locators,
generations, and exact next-ID high-water—including gaps from burned IDs—are
preserved. V1 had no pending-build authority, so unregistered historical NBC
artifacts remain unregistered and are never discovered by a filesystem scan.

The projection catalog file is `<schema-catalog>.projections` and its independent
publication marker is `<schema-catalog>.projections.state`.

- Magic: `NBPC`; marker magic: `NBPM`.
- Version: little-endian `u16` version 1 plus zero reserved header bytes.
- Identity: the 16-byte database incarnation.
- Allocator: `next_projection_id`; zero denotes exhausted `u64` space.
- Entry fields: projection, table, source storage and observed generation IDs,
  schema fingerprint, and a bounded UTF-8 location relative to NBPC.
- Integrity: CRC32C over every byte before the trailing checksum.
- Bounds: 64 MiB file, at most 1,048,576 entries, and at most 4,096 bytes per
  location. Decode uses checked slices and rejects trailing bytes.
- Validation: nonzero identities/generation, unique projection IDs and
  locations, non-absolute locations, and a high-water greater than every live
  ID.

Every allocator reservation publishes the incremented high-water through a
same-directory `.next` file, complete write, `sync_all`, atomic rename, parent
directory sync, and marker publication before returning the ID. Once that
rename is durable, a failed or abandoned build burns the ID. A shadow left
before rename is discarded on reopen and the unpublished ID remains available.

The marker distinguishes a new pre-Phase-1.5 database with no projection
catalog from a database whose initialized NBPC file was removed. A missing NBPC
with a surviving marker, a bad checksum, unsupported version, malformed entry,
or incarnation mismatch puts only the projection subsystem into an explicit
unavailable state. The authoritative database still opens and queries. Managed
projection mutation then returns `ProjectionCatalogError::Unavailable`.

## Reopen and lifecycle

On every managed `Database::open*`, Core opens NBPC and evaluates each entry
against the recovered authoritative schema and storage registry. A valid
projection is opened from NBCM/NBCS without an application attach call. A
missing table, partitioned placement, replaced source `StorageId`, changed
schema fingerprint, missing file, or corrupt file retains the catalog identity
and appears as `Unavailable`; it is never a planning candidate. A valid
projection with a changed source token appears as `Stale` and falls back to the
authoritative path.

`attach_columnar_projection` is now an explicit adoption/repair API. For a
known unavailable identity it must match the catalog location, table, and source.
For a new identity it may only adopt the manifest's exact ID at or above the
durable high-water; an already reserved ID, duplicate location, or identity
collision is rejected. The manifest ID is never silently rewritten.

The historical v1 build ordering was:

1. Reject an already registered canonical location.
2. Durably reserve the ID.
3. Capture one stable authoritative snapshot and build synced temporary files.
4. Recheck the source token.
5. Publish NBCS and NBCM.
6. Publish the NBPC entry.
7. Publish the in-memory registry entry.

An interruption before step 5 leaves only removable temporary files. Between
steps 5 and 6 it leaves an unregistered physical orphan and a burned ID. Between
steps 6 and 7 reopen discovers the complete projection.

Adaptive Operations Phase 26 replaces this new-build sequence with an NBPC v2
pending intent before physical publication. See
[`adaptive-operations-phase26.md`](adaptive-operations-phase26.md); refresh and
drop retain the lifecycle described below.

Refresh preserves the projection ID and orders synced N+1 files, NBCM
publication, NBPC observed-generation update, in-memory replacement, and old
segment retirement. Reopen therefore selects old N before NBCM publication or
new N+1 afterward. The projection object owns decoded immutable vectors. A
reader that already holds N continues scanning those vectors after N+1 is
published and N's file is retired; a dedicated storage test exercises this
interleaving.

Drop first removes and syncs the NBPC entry, then removes the in-memory entry,
then removes manifest/segment files. A failure in physical cleanup leaves an
unadvertised orphan, while logical DROP and the never-reuse high-water remain
durable. Repeating open is deterministic.

Schema rewrite changes the fingerprint and source `StorageId`, making the old
projection stale immediately and unavailable after reopen. `DROP TABLE` keeps
the derived catalog entry as unavailable evidence until an explicit projection
drop or repair; it cannot become healthy for a future table identity.

## Planner cost

Columnar remains a replacement only for an already selected sequential scan,
so a B+Tree point or range plan keeps precedence. For each eligible projection,
the planner uses integer planning-work units:

```text
2 startup
+ selected row groups
+ ceil(exact encoded bytes of required chunks / 4096)
+ ceil(selected rows × required columns / 256)
```

Zone maps determine selected groups before byte and decoded-value work is
charged. Predicate selectivity is not treated as row-group pruning. Exact
per-row-group encoded bytes are derived from column vectors, so a 128-column
projection queried for three columns pays for those three chunks. Costs contain
no elapsed time, device latency, CPU frequency, query text, or row-count
threshold.

Deterministic tests establish that a point lookup retains B+Tree, a small scan
reading a high byte fraction may retain Heap/LSM, a wide projection with a
narrow required subset may select Columnar, friendly zone maps reduce work,
hostile maps with the same predicate do not, and a missing column excludes the
projection. Core tests separately prove stale and unavailable entries never
reach planning.

## Scale benchmark

`columnar_scale` emits CSV and accepts comma-separated environment values:

```bash
NETBADB_COLUMNAR_ROWS=10000,100000,1000000,10000000 \
NETBADB_COLUMNAR_WIDTHS=4,16,64,128 \
NETBADB_COLUMNAR_ROW_GROUPS=128,512,1024,2048,4096,8192 \
NETBADB_COLUMNAR_SELECTIVITIES=1,10,100,1000,5000,10000 \
NETBADB_COLUMNAR_GROUPS=2,100,10000 \
NETBADB_COLUMNAR_DISTRIBUTIONS=friendly,hostile \
NETBADB_COLUMNAR_ENGINES=heap,btree,columnar,lsm \
cargo bench -p netbadb-core --bench columnar_scale
```

Selectivity is reported in basis points, so 1 is 0.01% and 10,000 is 100%.
The full database path covers Heap sequential scans, B+Tree point/range choices,
LSM scans, Columnar planning/execution, full aggregate, range/group aggregate,
and result cardinality. Its setup deliberately uses public transactional APIs.

`NETBADB_COLUMNAR_ENGINES=direct-columnar` is the storage scale path. It avoids
the intentionally durable row-at-a-time authoritative load when measuring 1M
and 10M immutable scans, but uses the same NBCM/NBCS builder, typed vectors,
zone maps, row groups, pruning, and byte counters. It performs the same
three-column range filter and grouped sum as the database workload. Its
`chosen_plan` is reported as `direct-columnar`, rather than pretending that
planner selection was run.

The CSV includes engine/path, workload, distribution, rows, table/read widths,
row-group size, selectivity, chosen plan, groups total/read/pruned, bytes scanned,
logical bytes, segment bytes, result rows, and mean microseconds. Timing is an
observation and is never a test assertion.

Representative measurements on the development host on 2026-09-05:

| rows | width | distribution | selectivity | row group | path | groups read/total | bytes | mean µs |
| ---: | ---: | --- | ---: | ---: | --- | ---: | ---: | ---: |
| 10,000 | 4 | friendly | 1% | 1,024 | Heap sequential | 0/0 | unreported | 1,480 |
| 10,000 | 4 | friendly | point | 1,024 | B+Tree point | 0/0 | unreported | 43 |
| 10,000 | 4 | friendly | 1% | 1,024 | Columnar | 1/10 | 24,960 | 174 |
| 10,000 | 4 | friendly | 1% | 1,024 | LSM sequential | 0/0 | unreported | 1,235 |
| 100,000 | 16 | friendly | 0.01% | 1,024 | direct Columnar | 1/98 | 24,960 | 1 |
| 100,000 | 16 | hostile | 0.01% | 1,024 | direct Columnar | 98/98 | 2,437,500 | 95 |
| 1,000,000 | 4 | friendly | 0.01% | 4,096 | direct Columnar | 1/245 | 99,840 | 7 |
| 1,000,000 | 4 | hostile | 0.01% | 4,096 | direct Columnar | 245/245 | 24,375,000 | 978 |
| 10,000,000 | 4 | friendly | 0.01% | 8,192 | direct Columnar | 1/1,221 | 199,680 | 17 |
| 10,000,000 | 4 | hostile | 0.01% | 8,192 | direct Columnar | 1,221/1,221 | 243,750,000 | 10,724 |

The 10M width-4 run completed. The full public Heap/LSM load is deliberately
not represented by the direct rows: a 10K full-engine fixture took about one
minute to load on this host, so extrapolating it to 1M or 10M would measure the
current durable insert path rather than analytical execution.

The complete 100K width-16 selectivity matrix at 0.01/0.1/1/10/50/100% read
1/1/2/11/50/98 of 98 friendly row groups. The hostile distribution read all 98
groups at every selectivity. This is the structural distinction used by the
planner: predicate selectivity alone never receives pruning credit.

At 100K rows and 1% selectivity, row groups 128/512/1024/2048/4096/8192 read
1,152/1,536/2,048/2,048/4,096/8,192 physical rows respectively. Mean aggregate
time was 14/10/9/9/10/12 µs. This supports retaining the current configurable
default: the best group size depends on pruning granularity and workload.

At 10K rows with a full scan and three required columns, widths 4/16/64/128 all
reported exactly 243,750 scanned bytes while total logical bytes rose from
320,000 to 13,865,055. This proves scan-time column pruning, including that
unqueried Text payload is not cloned into scan batches. That was the Phase 1.5
representation: it still decoded all chunks at open. [Columnar Phase
2D](columnar-phase2d-lazy-io.md) now uses indexed, independently verified lazy
chunks for new generations while retaining the eager reader for legacy files.

The 10K full-range GROUP BY matrix produced 2/100/10,000 result groups. At the
two endpoints, Heap measured 1,725/3,098 µs and Columnar measured 1,307/3,722
µs. High-cardinality aggregate state can therefore erase the scan advantage;
Phase 1.5 records this crossover and does not add spilling.

## Phase 2 boundary

Phase 1.5 does not implement a durable change stream, CDC, Columnar Delta,
base/delta merging, incremental refresh, a database-global commit sequence,
`RowEntityId`, writable Columnar storage, hybrid authoritative placement, or
background refresh/compaction.
