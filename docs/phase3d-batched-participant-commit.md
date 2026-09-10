# Phase 3D: batched participant commit barriers

Phase 3C reduced an explicit group's coordinator commit point to one durable
CORD v5 GroupDecision, but it still synchronized the same storage WAL once per
member after that decision. Phase 3D batches only this post-decision physical
durability work:

```text
durable member Prepares
        ↓
one GroupDecision sync
        ↓
per StorageId: stage existing Commit records in member order
        ↓
one storage WAL barrier
        ↓
finalize each local transaction in the same order
        ↓
all participating storages durable
        ↓
deferred Completes
        ↓
publish the final group G
```

## Boundaries and ordering

Every member still synchronizes its Prepare before parking and releasing the
single writer. Phase 3D introduces no `ParkedPreparePending`, concurrent active
writer, worker, timer, thread, Tokio task, or asynchronous commit. Ordinary
Phase 3B transactions retain their allocation-free single-transaction path.
Schema, index, backfill, replacement and other structural transactions remain
outside explicit group commit.

Core walks physical storages in canonical `StorageId` order. For each storage
it restricts the global member order to members that actually participate in
that storage. Storage validates that those physical transaction IDs are the
exact current parked-queue prefix; it never sorts them by physical or database
transaction identity and never permits a middle skip. Tests also execute the
storage batches in reverse StorageId order: correctness depends only on the
all-storage publication barrier, not on which storage becomes durable first.

A Storage Commit Barrier is not a GroupDecision. A local
`StorageVisibilityBoundary` is not a `DatabaseCommitSeq`. Sharing a WAL sync
does not merge transactions: every member retains its own physical TxnId,
local commit version, `DatabaseTxnId`, consecutive G, and Change Stream
transition.

## Heap protocol

For an exact parked prefix Heap validates state and database identity before
mutation. It appends the existing WAL Commit record for each member and keeps
its LSN as that transaction's distinct `CommitSeq`. A retry in `CommitPending`
reuses the already appended record. Heap then calls `flush_through` once on the
last Commit LSN. Only after this succeeds does it record each committed
TxnStatus, release the deque head, unregister the transaction and publish its
prepared NBCL change in the same order.

A mid-append or flush error is retry-only because GroupDecision is already
durable. No rollback or global publication occurs. A crash after the WAL
barrier but before some TxnStatus updates is recovered from the durable Commit
records; runtime finalization is not recovery authority.

## LSM protocol

LSM validates the same exact prefix. Each newly staged member reserves one
normal `LsmCommitSeq`, stores it in `pending_commit_seq`, and appends the
existing Commit record. A retry reuses that sequence and either finds or
re-appends the same record; sync uncertainty never allocates a replacement
sequence. One LSM WAL sync durabilizes the complete staged prefix.

After the barrier, LSM applies each durable mutation batch to the MemTable in
commit order, including clustering-key moves as their existing old-key
tombstone plus new-key put, advances local visibility, finalizes NBCL, releases
the parked write intent and removes the queue head. Crash recovery rebuilds the
same committed MemTable from WAL and requires no sidecar.

## Partial storage success and publication

Storage batches remain serial. If Heap S1 becomes durable and LSM S2 fails,
S1 is retained as successfully resolved and S2 is retried idempotently. The
group cannot roll back, maintenance and close barriers remain active, and the
published database snapshot remains at its old G. Only after every required
storage reports durable local commits does Core append the existing deferred
per-member Completes and atomically publish the final group sequence. It never
publishes an intermediate member G.

## Change Stream

NBCL is deliberately not batched in Phase 3D. Prepare reservations remain
durable per member, and authoritative commit finalization remains a separate
per-member sync in queue-head order. The logical chain therefore remains
`F0→F1`, `F1→F2`, rather than one merged `F0→Fn` transition. A finalize error
after storage commit durability cannot roll back the authoritative outcome;
existing unavailable/reopen recovery semantics apply.

## Reports and compatibility

`GroupCommitReport` includes one `StorageCommitBatchReport` per participating
storage and the total storage commit WAL barriers. Live prepared-runtime
inspection separates Prepare syncs, ordinary/single commit syncs, group commit
barrier syncs and NBCL prepare/finalize syncs. Counters reset on open and are
not inferred from elapsed time or persisted history.

No Heap WAL, LSM WAL, NBCO, CORD v1-v5, NBCL, schema, protocol, SDK or
inspection JSON format changes. Coordinator compaction continues to summarize
a group only after its Completes are durable.

The `global_group_storage_barrier_phase3d` benchmark covers Heap, LSM and
Heap+LSM at 100/1,000 transactions and group sizes 1/4/8/10/16/32, plus a
100-member Heap Change Stream case. It asserts no latency threshold; structural
sync counts and final data are the correctness result.

Prepare durability batching is implemented by the explicit opt-in successor
[Phase 3E](phase3e-batched-participant-prepare.md). Background/server group
formation, timers, parallel participant apply, NBCL finalize batching,
Serializable/SSI, historical snapshots, global CDC ordering, RowEntityId and
authoritative Columnar storage remain deferred.
