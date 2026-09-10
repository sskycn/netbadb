# Phase 3F: batched Change Stream durability barriers

Phase 3E reduced authoritative participant Prepare durability to one WAL
barrier per `StorageId`, but an enabled Change Stream still synchronized one
PreparedChange and one Finalize record for every changing member. Phase 3F
adds an explicit mode that shares those two NBCL barriers per storage:

```text
stage NBCL PreparedChange records
        ↓
one NBCL Prepare barrier / StorageId
        ↓
one authoritative Prepare barrier / StorageId
        ↓
unchanged CORD v5 GroupDecision
        ↓
one authoritative Commit barrier / StorageId
        ↓
stage existing NBCL Finalize records
        ↓
one NBCL Finalize barrier / StorageId
        ↓
promote independent batches, then publish the group
```

## Explicit API and compatibility

`GroupCommitOptions` fixes both `GroupPrepareMode` and
`GroupChangeStreamDurabilityMode` before the first member. The new
`BatchedBarrier + BatchedBarriers` pair is opt-in through
`Database::begin_group_commit_with_options`. `DurablePerMember +
BatchedBarriers` is rejected with the typed
`UnsupportedGroupDurabilityCombination` error.

`begin_group_commit`, `park_group_member`, and `stage_group_member` retain
their prior contracts. In particular, `stage_group_member` still selects
Phase 3E authoritative Prepare batching with per-member NBCL durability.
Ordinary transactions still return from Change Stream prepare/finalize only
after their respective sync, so a changing ordinary transaction still issues
two NBCL sync calls.

## Prepare staging and durability

The group-only path appends the existing `PREPARED_TAG` bytes, allocates the
diagnostic sequence and the exact `before -> after` frontier reservation, and
immediately inserts an unresolved runtime record. It does not synchronize.
The committed frontier therefore remains unchanged, while reclamation already
sees `PreparedChangesUnresolved`. Sequence values may be burned by a later
pre-decision abort.

Runtime-only `prepared_durable` state distinguishes an appended record from
explicitly synchronized evidence; NBCL bytes and headers are unchanged.
Complete persisted records reconstructed by `open` are durable recovery
evidence. The ordinary prepare path synchronizes an existing non-durable
record before returning, preserving its success-means-durable contract.

For each storage, the batch barrier validates stream identity, physical and
database transaction identities, exact sequence order, and the contiguous
frontier chain. Ordering comes from the PreparedChange sequence, never the
`TxnId` key of the unresolved map. One `sync_data` durabilizes the selected
prefix, after which those records are marked durable. Disabled streams produce
no record, report, allocation, or sync; unavailable streams fail closed.

The NBCL Prepare barrier deliberately precedes the authoritative Prepare
barrier. Core explicitly proves all required storage Change Stream barriers
completed before asking CORD to allocate any `DatabaseCommitSeq` or append a
GroupDecision. Thus any authoritative commit recoverable after a crash already
has durable logical change evidence.

## Finalize staging, durability, and promotion

After the unchanged Phase 3D authoritative Commit barrier, storage appends one
existing `FINALIZE_TAG` record for each changing transaction in frontier
order. Heap markers carry no LSM version. Every LSM marker binds that
transaction's exact `LsmCommitSeq`; members never share the group's last
version.

Runtime-only staged-Finalize state prevents a retry from appending a duplicate
marker. One `sync_data` durabilizes all markers for that storage. Until it
succeeds, unresolved records remain present, the committed batch vector and
effective current frontier remain unchanged, and destructive reclamation is
blocked. Only after the barrier are records promoted in the same contiguous
frontier order. One barrier therefore still yields N independent ChangeBatches
and N `StorageDataVersion` transitions; it never coalesces across transactions.

A partial append or uncertain sync poisons the live stream and requires reopen
rather than appending after a possibly torn tail. Since the GroupDecision is
already durable, rollback is forbidden and final database publication stays at
the previous G. Reopen truncates only a legal incomplete final record, consults
authoritative outcomes, and appends missing Finalizes. Complete-sized checksum
corruption remains a hard unavailable condition.

## Multi-storage failure and recovery

Prepare barriers execute per stable `StorageId`. If one storage completes both
its NBCL and authoritative Prepare barriers and a later storage fails, there is
no GroupDecision, no G allocation, and no publication. The group can retry or
reverse-abort; uncertain Change Stream I/O fails closed.

After a durable GroupDecision, partial authoritative Commit or NBCL Finalize
success is commit-only. A completed source-local frontier may temporarily lead
the global published G, but the final `DatabaseSnapshot` is not published until
every required Finalize barrier succeeds. Recovery uses the unchanged
authoritative participant outcomes to complete all missing Change Stream
Finalizes and then restores the final global publication.

Safe reclamation continues to use the existing retention pins and adaptive
policy. Staged-unsynced, staged-durable, and Finalize-pending records remain
unresolved, so observations are blocked and stale proposals abort revalidation
without mutation. Successful convergence restores ordinary reclamation and
rewrite behavior; batched records require no special persistent case.

## Sync accounting

`PreparedRuntimeInspection::change_stream_sync_count` remains the total number
of NBCL sync calls made by the live runtime. Separate member Prepare, group
Prepare, member Finalize, and group Finalize counters explain that total.
`GroupCommitReport` reports the selected mode, per-storage Prepare/Finalize
batches, bytes, frontiers, and actual sync calls.

For 100 changing transactions in ten-member groups on one storage:

```text
PerMember:       100 Prepare + 100 Finalize = 200 NBCL syncs
BatchedBarriers:  10 Prepare +  10 Finalize =  20 NBCL syncs
```

The `global_group_change_stream_barrier_phase3f` benchmark compares both modes
for Heap, LSM, and Heap+LSM at 100/1,000 transactions and group sizes
1/4/8/10/16/32. It asserts sync structure, rows, frontiers, and G publication;
elapsed time is observational only.

## Persistent compatibility and explicit non-goals

NBCL v1/v2, CORD v5, Heap WAL, and NBLW formats do not change. There is no
group identity, `DatabaseCommitSeq`, sidecar, new tag, or header rewrite in the
protocol. `DatabaseTxnId` remains correlation, not global CDC order.

- One NBCL sync does not merge ChangeBatches.
- `StorageDataVersion` is not `DatabaseCommitSeq`.
- Batched NBCL Prepare is not authoritative Prepare.
- Batched NBCL Finalize is not GroupDecision.
- Phase 3F does not defer NBCL Finalize beyond successful group publication.
- Phase 3F does not change ordinary transaction durability.
- Phase 3F introduces no background worker, timer, queue, async runtime,
  active multi-writer execution, or parallel participant barriers.
- Phase 3F adds no global CDC ordering, historical `AS OF`, Serializable/SSI,
  `RowEntityId`, or authoritative Columnar/hybrid storage.
