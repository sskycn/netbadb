# Columnar Phase 2C: delta compaction and change-stream retention

Phase 2C adds two explicit, synchronous maintenance operations. It does not
change authoritative Heap/LSM commit behavior.

```text
NBCL -> explicit advance -> NBCD -> Base + Delta
                                      |
                               explicit compact
                                      v
                                new NBCS Base

managed projection applied frontiers -> minimum -> safe frontier -> NBCL GC
```

There is no worker, scheduler, threshold, network CDC consumer, global commit
sequence, or writable Columnar engine. Advance, compaction, and GC remain
separate administrator actions.

## Compaction state and protocol

For one incremental projection, compaction implements exactly:

```text
Base(F0) + Delta(F0, Fc] -> Base(Fc)
```

It iterates the owned NBCS v2 row groups in physical order, skips exact
`StorageVersionKey`s suppressed by the applied Delta chain, and emits bounded
row groups. It then emits live Delta versions in deterministic version-key
order. The writer never creates a whole-table `Vec<Vec<ScalarValue>>`; only one
row-group-sized row buffer is assembled before typed vectors and zone maps are
rebuilt. NBCS serialization and CRC calculation stream one encoded row group at
a time, so there is no second whole-segment byte buffer alongside the resident
replacement. The existing resident Base vectors and Delta overlay remain owned
by the old reader.

Compaction is not a source refresh. It does not scan Heap/LSM, read NBCL, catch
up, or alter the applied frontier. It preserves the projection ID, table,
source StorageId, schema fingerprint, source engine kind, stream generation,
and every surviving exact version key. A chain such as `V0 -> V1 -> V2 -> V3`
therefore leaves only V3; insert then delete leaves no row. NULL values use the
same validity-vector encoding as normal NBCS v2 writes.

The result is generation N+1 because a reader may already own generation N.
Publication is:

```text
write/sync new NBCS
-> rename NBCS and sync directory
-> publish/sync NBCM
-> update NBPC observed generation
-> swap ProjectionRegistry snapshot
-> retire old NBCS/NBCD files
```

NBCM remains the generation authority. NBPC remains inventory/location plus
observed generation; Phase 2C introduces no third authority. A crash before
NBCM publication reopens generation N. A crash after it reopens the complete
generation N+1. The manifest never names a missing Base or an old Delta
inventory. An old reader continues from owned immutable vectors after its
physical files are retired; new readers open N+1.

The compacted manifest sets `base_frontier = applied_frontier = Fc` and has an
empty Delta inventory and zero Delta bytes, mutations, live rows, and
suppressed versions. This naturally resets the existing structural planner
cost without a magic compaction threshold. If the source has advanced to Fnew
while Fc is compacted, the result remains Lagging at Fc and a later explicit
advance can consume `(Fc, Fnew]`.

`Database::compact_columnar_projection` reports generations, frontier, Base
rows before/after, consumed Delta segments/mutations, removed suppression,
folded live rows, and old/new/reclaimed bytes. A Base-only incremental
generation is a reported no-op. Snapshot projections and projections whose
stream incarnation no longer matches are rejected.

## Retention acknowledgement

For a managed incremental projection P, a durably published NBCM
`applied_frontier = Fp` acknowledges that P no longer needs committed NBCL
history at or before Fp for normal forward maintenance. It does not promise
reconstruction of corrupt projection files from an arbitrarily old Base; full
refresh/rebuild remains that recovery path. Compaction does not advance this
acknowledgement. Only successful NBCM publication by advance does.

Core computes retention policy because Storage cannot know all projections.
For one exact StorageId and the active `ChangeStreamGeneration`, it validates
every NBPC-owned projection and takes the minimum durable applied frontier:

```text
P1 applied F100
P2 applied F80
P3 applied F95
safe GC frontier = F80
```

Snapshot NBCM v1 projections do not participate. An incremental projection
bound to an old stream generation is already RebuildRequired and does not pin
the current generation. A Lagging projection on the current generation does
pin history. Missing/corrupt managed projection metadata fails closed because
Core cannot prove its dependency. An unmanaged incremental projection also
refuses managed GC. With no managed incremental consumer, the explicit API
returns `NoRetentionConsumer`; it never treats absence as permission to delete
everything. Explicit stream disable remains the abandonment API.

`read_changes` cursors are non-retaining cursors, not leases. After GC, a
cursor before the retained frontier returns `HistoryUnavailable`; a cursor at
the retained frontier reads the next batch normally. External consumer
registration, acknowledgement, leases, and network subscriptions remain
deferred.

## NBCL v2

NBCL v2 keeps the `NBCL` magic and checksummed records. Its 104-byte,
little-endian, checksummed header persists:

- active flag and Heap/LSM kind;
- StorageId, TableId, SchemaFingerprint, and ChangeStreamGeneration;
- `stream_origin_frontier`, where this incarnation was enabled;
- `earliest_retained_frontier`;
- a durable current-frontier checkpoint;
- a durable `next_sequence` high-water checkpoint.

Normal append/finalize retains the Phase 2A two-sync path. The header's current
and next-sequence values are rewrite checkpoints: later complete records may
advance beyond them and reopen derives the later effective values. GC writes
fresh checkpoints, which is what preserves F100 and the next diagnostic
sequence when every committed batch is removed. GC does not add work to DML.

Readers accept NBCL v1. A v1 header maps its baseline to origin, earliest, and
the initial current checkpoint; records derive the effective current and
sequence. The first actual GC rewrites that incarnation as v2. Database open
does not eagerly migrate logs. The `.active` guard accepts and writes both
versions and validates the stable storage/table/schema/generation identity.

For retained history, readers require:

```text
origin <= earliest <= current
first retained batch.before = earliest
each batch.after = next batch.before
last retained/effective batch reaches at least the durable current checkpoint
no retained batch => earliest = current
next_sequence != 0
```

## GC rewrite and recovery

`Database::gc_change_stream` resolves one exact storage, validates an enabled
healthy generation and a quiescent transaction boundary, rejects unresolved
prepared changes, computes the managed minimum, and asks Storage to rewrite
through that frontier. Storage writes only retained committed batches:

```text
write NBCL GC temporary
-> sync
-> atomic rename
-> sync parent directory
-> refresh active guard
-> replace in-memory handle/state
```

The stream generation and `StorageDataVersion` do not change. A rewrite from
origin F0, earliest F0, current F100 through F80 persists earliest F80 and
retains `F80 -> ... -> F100`. Rewriting through F100 leaves no committed batch,
but a later commit still forms `F100 -> F101`, and its diagnostic sequence does
not restart. Recovery of a prepared authoritative winner after GC uses the
same Phase 2A finalize repair and attaches after the retained current frontier.
GC failure cannot affect authoritative Heap/LSM rows.

LSM flush and compaction remain physical maintenance: they neither advance the
stream frontier nor change projection freshness. NBCL GC likewise does not
create a new stream generation.

## Inspection and benchmarks

Projection inspection now exposes exact Delta segment count and the factual
`compaction_possible = delta_segment_count > 0`. Change-stream inspection
separates origin, earliest available, and current frontier while preserving
`baseline_data_version` as an origin compatibility alias.

`columnar_compaction_phase2c` reports 100K/1M Base rows with 1/5/10% update and
mixed Delta, query time before/after, compaction time, Delta statistics, and
old/new bytes. `change_stream_gc_phase2c` reports 10K/100K mutations in batched
transactions, retained batches, rewrite time and bytes, reopen current, and
the next commit chain. Timings are observations, never assertions.

## Explicit non-goals

- Compaction is not a source refresh and does not advance applied frontier.
- NBCL GC does not advance StorageDataVersion or create a new stream generation.
- `read_changes` cursors are not retention leases.
- Projection applied frontier acknowledges only forward maintenance of that
  projection.
- `DatabaseTxnId` is not a global commit order.
- `StorageVersionKey` is not `RowEntityId`.
- Lazy NBCS/NBCD loading is implemented by [Columnar Phase
  2D](columnar-phase2d-lazy-io.md). Automatic/background catch-up, compaction,
  retention GC, generic CDC leases, aggregate spill, global CSN, RowEntityId,
  and hybrid authoritative storage remain deferred to a later design.
