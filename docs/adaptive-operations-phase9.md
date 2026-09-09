# Adaptive Operations Phase 9: safe Change Stream reclamation

Phase 9 adds an explicit synchronous reclamation path for committed NBCL
history. Reclamation differs from adaptive optimization: deleted history cannot
be measured and rolled back. Safety is therefore proved before the one
production rewrite, and successful GC never creates a workload trial.

```text
Observe retention state
        -> compute a safe batch boundary
        -> pressure and hard-budget admission
        -> revalidate every current authority
        -> one production GC rewrite
        -> verify retained state
```

## Retention authorities

The current concrete authorities are managed incremental Columnar projections
on the active stream incarnation and explicit runtime retention pins. Each
projection authority binds projection id and generation, source storage id,
stream generation, and applied frontier. Missing managed projection metadata,
an unavailable projection catalog, an unmanaged incremental projection, an
invalid frontier, or unresolved prepared stream state makes retention safety
unprovable and blocks reclamation. With no concrete consumer the established
`NoRetentionConsumer` behavior remains conservative; absence of a consumer is
not permission to delete all history.

`ChangeStreamCursor` remains a copyable position value. It is not registered
and does not retain history. Legal GC may therefore make an old unpinned cursor
return `HistoryUnavailable`.

`ChangeStreamRetentionPin` is a separate explicit runtime-only handle. Pin
acquisition validates storage id, stream generation, and that its frontier is
within the current `[earliest, current]` interval. Its frontier can advance but
cannot move backward. Explicit release or `Drop` unregisters the authority.
Both manual and automatic production GC honor it; the storage writer also
checks pins as a final safety guard. Pins have no WAL, catalog, or manifest and
disappear on process restart because an in-memory reader cannot survive that
restart.

## Safe frontier and exact prefix

For a consumer at frontier `F`, the next readable batch has `before == F`.
`SafeReclaimThrough(F)` therefore means deleting the contiguous prefix whose
`batch.after <= F`, while retaining a suffix whose first batch begins at `F`.
The value must be a real inspected batch boundary (or current frontier), never
an arbitrary integer. The safe frontier is the minimum required frontier of
all current authorities.

The detached observation binds SchemaGeneration, TableId, StorageId,
StorageKind, stream generation and origin, earliest/current frontiers,
unresolved count, payload-free batch metadata, all consumers, limiting
consumers, and the exact reclaimable prefix. All mutation, byte, and work
arithmetic used for admission is checked.

## Proposal, dominance, and budget

`AdaptiveChangeStreamGcPolicy` is only a pressure gate. Its non-zero batch and
byte thresholds use OR semantics. Safety is computed first; a large file never
implies permission to reclaim it.

An admitted proposal binds the exact identities, observed boundaries, safe
frontier, consumer evidence, expected prefix, policy, and a
`HardBoundedRewrite` maintenance estimate. The structural input is the current
NBCL file length. Rewrite output is exactly the v2 header plus the inspected
retained prepared/finalize record lengths. Work is one header unit plus one per
retained batch. These are structural budget quantities, not a claim about
physical device I/O.

A proposal is evidence, not mutation authority. Execution observes again and
requires the same schema, table/storage, and stream generation; no unresolved
state; an extant contiguous proposal prefix; and a newly computed safe frontier
that dominates the proposed boundary. A newly acquired slower pin or consumer
aborts before mutation. Existing consumers may advance. Ordinary DML may also
advance current frontier and append a suffix: neither fact alone makes the old
prefix unsafe, though the enlarged exact rewrite cost must still fit the
current budget. If manual GC has already passed the boundary, execution reports
already reclaimed/no work and never moves earliest backward.

The mutation calls the existing production `gc_change_stream` suffix rewrite.
Its report is reused directly. Postconditions require unchanged generation and
schema, the exact new earliest boundary, current at or after earliest, every
current authority at or after earliest, and a contiguous retained stream.
Reclamation publishes no database commit sequence and changes no table row,
index, source data version, or Columnar file.

## Automatic lane and compatibility

Multi-target Automatic Safe Mode adds the distinct
`ChangeStreamReclamation` lane and `ChangeStreamGc { table_id, storage_id }`
candidate identity. It is disabled by default and scans only the caller's
bounded table scope. Active Columnar or calibration probation still prevents
all candidate discovery and fairness updates.

With strict priority the proactive order is Columnar maintenance, Change
Stream reclamation, then planner calibration. The existing
`BoundedColumnarBurst` remains the same explicit Columnar-versus-calibration
service contract; Phase 9 does not reinterpret it as a generic three-lane
scheduler. Reclamation has no cross-lane starvation guarantee in this first
version. Within its lane, ready age precedes reclaimable bytes, reclaimable
batches, lower rewrite bytes, and stable table/storage identity. Blocked
evidence never becomes safe through age.

A selected reclamation candidate ends the step after completed, aborted,
stale, or inconclusive execution. It never falls through to a second action,
never starts a trial, never advances the manual maintenance cursor, and neither
increments nor resets the Phase 7 consecutive-Columnar counter. Existing
manual inspection, `maintenance_step`, and `gc_change_stream` share the same
retention theorem but remain independently caller-controlled.

## Durability boundary and exclusions

Phase 9 adds no background collector, thread, timer, LSM automation, physical
design, or schema mutation. Runtime pins, observations, proposals, fairness,
and outcomes are not persisted. It changes no Canonical Schema, Heap, LSM,
WAL, BTree, Columnar, Change Stream v2, coordinator, protocol v2, Schema Spec
v2, or Inspection JSON v7 contract.
