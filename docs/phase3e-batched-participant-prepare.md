# Phase 3E: batched participant Prepare barriers

Phase 3D reduced post-decision authoritative storage durability to one Commit
barrier per `StorageId`, but every member still synchronized its Prepare before
the next member could execute. Phase 3E adds an explicit alternative:

```text
stage member Prepare records
        ↓
park PreparePending and release the writer
        ↓
one Prepare barrier / StorageId
        ↓
all members durably Prepared
        ↓
unchanged CORD v5 GroupDecision
        ↓
Phase 3D Commit barriers / StorageId
        ↓
deferred Completes and final block publication
```

## API and mode compatibility

`Database::park_group_member` retains its Phase 3C contract: successful return
means every participant is durably prepared. It selects
`GroupPrepareMode::DurablePerMember` and continues to issue one authoritative
Prepare sync per member and storage.

`Database::stage_group_member` selects
`GroupPrepareMode::BatchedBarrier`. Successful return means the existing
Prepare record was appended, the transaction was frozen and consumed by the
batch, its parked conflict/reservation ownership is active, and the mutable
writer was released. It does **not** mean the authoritative storage Prepare is
durable. A group selects its mode when the first member joins; attempting to
mix the two paths returns the typed `GroupPrepareModeMismatch` error.

Core exposes the distinction as `ParkedPreparePending` and reports the selected
mode. Batched groups enter `PrepareBarrierPending` when commit starts. Once a
Prepare-barrier attempt begins, new members are rejected; the caller may retry
the same group or abort it before a GroupDecision exists.

## Heap protocol and WAL-before-page safety

Heap stages the existing NBCL evidence (when enabled), appends the existing
Heap Prepare WAL record, remembers its LSN, enters the one parked queue, and
releases the writer. The transaction retains B+Tree retirement and page-reuse
reservations. Pending entries participate in the same `xmax` dirty-write check
as durable parked entries.

For one Heap storage, Core supplies the exact parked queue prefix in global
member order. Heap validates the shared WAL/runtime, physical and database
transaction identities, queue order, and pending/durable states. It calls
`flush_through` once on the last Prepare LSN, then converts pending entries to
`ParkedPrepared`. A retry reuses every original Prepare LSN and does not append
another semantic Prepare.

Heap may steal a dirty page while a later Prepare marker is not durable. The
required invariant is narrower and sufficient: every PageUpdate WAL record
needed to redo or undo the page precedes page durability. Without a durable
GroupDecision, recovery treats a durable Prepare as abort-resolvable and an
incomplete transaction as a loser; global-LSN undo restores the baseline.
Runtime pre-decision abort likewise resolves members from the group tail, so
later same-page Heap and B+Tree before-images are undone before earlier ones.

## LSM protocol

LSM stages the existing mutation batch and Prepare record, retains the batch,
database transaction identity, and `(LsmRowId, base committed version)` parked
write intent, enters the same parked queue, and releases the writer. UPDATE,
DELETE, and clustering-key movement conflicts therefore remain excluded as
soon as staging succeeds.

The LSM Prepare barrier validates the exact parked prefix and shared runtime,
then issues one existing WAL `sync`. Only afterward are pending entries marked
`ParkedPrepared`. Sync retry uses the same mutation batch, Prepare record,
physical transaction identity, and NBCL reservation.

## Multi-storage failure and decision ordering

A member joins the group only after all of its participants stage successfully.
If one participant fails during staging, the member enters the existing prepare
resolution path; reverse rollback resolves all of that member's participants
before the group accepts more work. Previously staged members are unchanged.

Prepare barriers run serially in stable `StorageId` order (tests also reverse
the order). If S1 succeeds and S2 fails, no `DatabaseCommitSeq` is allocated,
no GroupDecision is appended, and nothing is published. S1's durable prepared
prefix is retained while S2 retries, or the whole group may still abort in
reverse global member order. Only after every storage barrier succeeds does
Core enter `DecisionPending` and invoke the unchanged CORD v5 decision path.

## Crash recovery

- Before any Prepare barrier, persisted mutation or Prepare bytes have no
  GroupDecision and every group transaction is aborted during recovery.
- After only some storage barriers, the same absence of GroupDecision makes
  both durable-prepared and incomplete participants abortable.
- After every Prepare barrier but before GroupDecision, recovery is the
  existing Phase 3C all-prepared/no-decision abort case.
- After a durable GroupDecision, rollback is forbidden and the unchanged Phase
  3D Commit-barrier recovery completes every member before final publication.

The WAL remains the sole recovery evidence. Heap WAL, NBLW, NBCL, NBCO and CORD
formats do not change, and there is no Prepare-batch sidecar.

## Change Stream, maintenance, and visibility

NBCL PreparedChange reservation and durability still happen per member before
the authoritative Prepare is staged. Prepare barriers do not advance the
committed Change Stream frontier. Abort abandons reservations from the tail;
commit finalizes them from the head. A staged member is counted as unresolved
prepared Change Stream evidence, so safe reclamation observations are blocked
by `PreparedChangesUnresolved`, and stale proposals fail revalidation without
mutation.

The active group barrier continues to reject checkpoints, close, coordinator
compaction, schema work, quiescent maintenance, LSM flush/compaction, and
Columnar maintenance. Readers remain on the old published database snapshot
until the final group sequence is published.

## Explicit non-goals

- `ParkedPreparePending` is not durable Prepared.
- Batched Prepare is not asynchronous commit.
- Batched Prepare does not allow multiple active writers.
- A Prepare barrier does not allocate `DatabaseCommitSeq` and is not a
  GroupDecision.
- `StorageVisibilityBoundary` is not `DatabaseCommitSeq`.
- Authoritative storage Prepare batching does not batch NBCL Prepare or
  finalize durability.
- Phase 3E adds no worker, timer, queue, Tokio task, parallel participant work,
  Serializable/SSI, `AS OF`, global CDC ordering, `RowEntityId`, or
  authoritative Columnar storage.

## Inspection and benchmark

`PreparedRuntimeInspection` separates durable-per-member Prepare syncs,
group Prepare-barrier syncs, ordinary Commit syncs, Phase 3D group Commit
barriers, and NBCL syncs. `GroupCommitReport` includes the prepare mode and one
`StoragePrepareBatchReport` per participating storage alongside the existing
Commit batch reports.

Run `cargo bench -p netbadb-core --bench global_group_prepare_barrier_phase3e`
to compare both modes across Heap, LSM,
and Heap+LSM at 100/1,000 transactions and group sizes 1/4/8/10/16/32. The
benchmark asserts structural sync counts rather than latency thresholds and
includes a 100-member Heap Change Stream case proving NBCL sync accounting is
unchanged.
