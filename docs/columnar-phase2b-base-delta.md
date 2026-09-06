# Columnar Phase 2B: incremental base and delta merge

Phase 2B keeps Heap and LSM authoritative while allowing an explicitly built
incremental Columnar projection to catch up from the durable per-storage NBCL
stream without rescanning its source.

```text
Heap / LSM
    ↓ committed DML
NBCL
    ↓ explicit bounded advance
immutable NBCD segments
    ↓ storage-owned version merge with NBCS base
ColumnBatch
    ↓
vector Filter / Project / Aggregate
```

Columnar remains derived state. DML commits write only the authoritative engine
and, when enabled, NBCL. They never write NBCD or advance NBCM. The synchronous
`Database::advance_columnar_projection` API performs maintenance later; there
is no worker or scheduler in this phase.

## Snapshot and incremental modes

`Database::build_columnar_projection` retains the Phase 1 snapshot contract.
Its NBCS v1 contains only projected SQL columns, and NBCM v1 uses exact
`StorageSnapshotToken` equality for freshness. Existing v1 files continue to
open and query without migration. Enabling a change stream does not convert a
v1 projection.

`Database::build_incremental_columnar_projection` is explicit and requires an
already enabled, healthy change stream. It captures one `CommittedReadAnchor`:
the storage's pinned read view and the matching `ChangeStreamCursor` F0. The
base scan through that view returns each projected row together with its exact
hidden `StorageVersionKey`. Commits after the anchor are legal: the base is
published at F0, reports `Lagging`, and catches up from NBCL on a later advance.

An explicit full refresh of an incremental projection repeats that anchor scan,
keeps the `ColumnarProjectionId`, publishes generation N+1, and resets Base and
applied frontiers to the new anchor with an empty Delta. This is the manual
rebaseline path for a large Delta.

## Persistent formats and authority

All formats use explicit little-endian fields, bounded counts and lengths, and
a trailing CRC32C.

- NBCS v1 is unchanged. NBCS v2 repeats projection generation, table, exact
  source `StorageId`, schema fingerprint, source engine kind, projected column
  chunks, and one hidden `StorageVersionKey` for every Base row. Heap keys keep
  the full generation-bearing `RowId`; LSM keys keep `LsmRowId` and nonzero
  committed `LsmCommitSeq`. Decoding rejects the wrong engine/storage, zero
  identities, missing keys, and row/key count disagreement.
- NBCM v1 is unchanged. NBCM v2 records incremental mode, stream generation,
  Base frontier, applied frontier, and the ordered NBCD inventory with per-file
  checksum, byte and mutation counts. It also records aggregate Delta bytes,
  live rows, and suppressed versions.
- NBCD v1 is immutable. One segment covers a nonempty contiguous `(Fa, Fb]`
  range and preserves every internal NBCL batch boundary. Mutation descriptors
  contain Insert, Update old-to-new, or Delete identities. After-images contain
  only requested projection columns in typed column vectors; deletes have no
  after row.

NBCM remains the sole current-generation and applied-frontier authority. NBPC
continues to own inventory, location, identity allocation, and the last
observed Columnar generation; advancing within a generation does not rewrite
NBPC. On reopen, NBCM order—not filenames—must prove
`Base F0 → Delta1 → ... → applied Fn`. A missing, corrupt, mismatched, or gapped
NBCD makes only that projection unavailable. A durable NBCD not referenced by
NBCM is an ignored orphan.

Advance publication is crash-safe:

```text
bounded contiguous NBCL read
    → write and sync temporary NBCD
    → write and sync replacement NBCM
    → rename NBCD and sync directory
    → atomically replace NBCM and sync directory
    → replace the in-memory immutable projection snapshot
```

The replacement NBCM may be fully prepared before publication, but its temp
file is not authority. NBCD is always renamed and directory-synced first; only
then can the NBCM rename make the new frontier visible.

A crash exposes either the old NBCM frontier or the new complete chain. It
cannot expose a manifest that names an unpublished Delta. One advance batches
all changes returned by its byte/batch budget into one NBCD and does not chase
a source frontier that continues moving.

## Merge and SQL correctness

Opening a projection decodes its immutable Base vectors and folds its NBCD
chain into an owned overlay containing only `suppressed` version keys and live
projected Delta rows. Base values are not copied into the overlay. Each update
suppresses/removes its old version and installs the new after-image; delete
suppresses/removes its old version; insert installs its new version. Thus
`V0→V1→V2→V3` leaves V0/V1/V2 suppressed and only V3 live, while an
insert followed by delete leaves no live row.

The storage projection scan applies operations in this order:

1. safely prune impossible Base row groups with Base-only zone maps;
2. suppress superseded Base versions in retained groups;
3. append every live Delta row (Delta gets no Base zone-map credit);
4. return `ColumnBatch` values to the existing vector pipeline;
5. evaluate the complete typed predicate and aggregates there.

This ordering fixes the critical case where Base `amount=100` is updated to
Delta `amount=0`: `WHERE amount > 50` cannot resurrect the suppressed Base row.
The inverse update emits the new Delta row. NULL validity and typed scalar
semantics use the same vectors as snapshot Columnar execution, including
COUNT, SUM, MIN, MAX, and GROUP BY.

## Freshness, planning, and observation

An incremental projection is eligible only when table, source `StorageId`,
schema fingerprint and stream generation match, its persisted chain is valid,
and `applied_frontier == current_data_version`. A newer commit immediately
makes it `Lagging` and queries fall back to Heap/LSM. Catch-up makes it `Fresh`
again. Disable/re-enable creates a new stream generation, so the old projection
becomes `RebuildRequired` and cannot advance. LSM flush, compaction, and WAL
rotation without logical DML do not change `StorageDataVersion` and therefore
do not stale a caught-up incremental projection.

Round 52 prevents an enabled projection source Heap from being silently
replaced. A blocked rewrite leaves the exact S1 projection identity unchanged.
Explicit S1 disable makes the old projection `RebuildRequired`; an S2 winner
does not retarget or refresh it. After explicit S2 stream enablement, callers
build a new S2 incremental projection and advance only from S2 NBCL. See
[change-stream-schema-replacement-round52](change-stream-schema-replacement-round52.md).

The planner still considers Columnar only after B+Tree/LSM access selection, so
point indexes retain precedence and explicit transactions retain authoritative
read-your-writes semantics. Columnar structural work adds Delta segment
startup, encoded-page work, mutation/vector work, and suppression/live merge
work to Base row-group, bytes, and vector work. There are no time/device
constants or ratio thresholds; a sufficiently large Delta can naturally lose
to the authoritative scan.

Inspection distinguishes `Fresh`, `Lagging`, `RebuildRequired`, and
`Unavailable`, and reports mode, stream generation, Base/applied/current
frontiers, lag, Delta segments/mutations/bytes, suppressed versions, and live
Delta rows. Scan statistics separately report Base groups/bytes/suppression,
Delta segments/mutations/bytes/live/emitted rows, and merged rows.

## Semantic boundaries

`StorageVersionKey` **IS NOT `RowEntityId`**. It identifies one physical
version only within one `StorageId` and layout lifecycle.

`StorageDataVersion` **IS NOT comparable across `StorageId`**. It is only a
per-storage logical frontier.

`DatabaseTxnId` **IS NOT a global commit sequence**. It correlates participants
but establishes no cross-storage snapshot order.

Columnar Delta **IS NOT an authoritative commit participant**. It never enters
the database coordinator and never blocks Heap/LSM COMMIT.

## Phase 2C continuation

Explicit Delta-to-Base compaction and managed-projection-aware NBCL retention
GC are implemented by [Columnar Phase 2C](columnar-phase2c-compaction-retention.md).
Lazy on-disk Columnar loading is implemented by [Columnar Phase
2D](columnar-phase2d-lazy-io.md). Automatic scheduling, generic CDC retention
leases, aggregate spill, database-global CSN, `RowEntityId`, and hybrid
authoritative Columnar storage remain deferred.
