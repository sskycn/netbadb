# Phase 3G: pipelined Change Stream Finalize checkpoints

Phase 3G is an explicit synchronous group-commit optimization. It pipelines
the durability checkpoint for already appended NBCL Finalize markers into the
next NBCL sync. It does not pipeline PreparedChange durability, Finalize
append, authoritative commit, or database publication.

## Why Finalize is a recoverable checkpoint

The Phase 2A durability authority remains:

```text
durable full PreparedChange evidence
+
authoritative durable committed outcome
=>
the exact committed ChangeBatch can be recovered
```

The Finalize marker records that this recovery work has already been done. It
is a recovery checkpoint, not the transaction commit authority. Reopen already
consults the authoritative outcome for a durable PreparedChange whose Finalize
is missing. Phase 3G preserves that theorem and batches all missing recovery
Finalizes in frontier order behind one repair sync per NBCL file.

PreparedChange durability is still mandatory before GroupDecision:

```text
NBCL Prepare barrier
    -> authoritative Prepare barrier
    -> CORD v5 GroupDecision
```

No authoritative transaction can become irrevocably committed before its full
logical change evidence is durable.

## Explicit, source-compatible mode

Existing `GroupCommitOptions`, `begin_group_commit_with_options`, and Phase 3F
struct literals are unchanged. `ExtendedGroupCommitOptions` adds the explicit
`GroupChangeStreamFinalizeMode` selection:

```text
ImmediateBarrier
    Phase 3F: Finalize sync before publication

PipelinedCheckpoint
    Phase 3G: Finalize append and runtime promotion before publication;
              sync at the next NBCL durability boundary
```

The pipelined mode is supported only with
`GroupPrepareMode::BatchedBarrier` and
`GroupChangeStreamDurabilityMode::BatchedBarriers`. Other combinations are
rejected as `UnsupportedGroupDurabilityCombination`.

Ordinary transactions still perform one Prepared sync and one Finalize sync.
Phase 3F `BatchedBarriers` still performs its Finalize barrier before group
publication. No existing caller silently adopts Phase 3G.

## Commit ordering

The Phase 3G group path is:

```text
stage NBCL PreparedChange records
    -> one NBCL Prepare barrier per StorageId
    -> one authoritative Prepare barrier per StorageId
    -> one CORD v5 GroupDecision sync
    -> one authoritative Commit barrier per StorageId
    -> validate the complete Finalize batch
    -> append every existing per-transaction FINALIZE_TAG
    -> promote independent ChangeBatches in frontier order
    -> advance runtime current_data_version
    -> deferred coordinator Completes
    -> publish the DatabaseSnapshot
```

Finalize append remains a foreground requirement. An append error does not
publish the group; after GroupDecision it leaves commit/recovery-only state and
never permits rollback. All validation that can fail is performed before the
first marker append, so promotion after successful append is deterministic and
frontier ordered.

One sync never merges transactions. Every member retains its own ChangeBatch,
StorageDataVersion transition, physical TxnId, DatabaseTxnId correlation, Heap
RowId, and (for LSM) exact LsmCommitSeq.

## Runtime current and checkpoint frontier

`current_data_version` is the highest recoverably committed stream frontier in
the current process. In Phase 3G it may be ahead of
`finalize_checkpointed_through`:

```text
current_data_version             = F110
finalize_checkpointed_through    = F100
pending_finalize_checkpoint_count = 10
```

The gap is safe because the underlying PreparedChange evidence is durable and
the authoritative outcomes are committed. `read_changes` immediately exposes
the promoted F100->F110 batches. DatabaseSnapshot G110 is published only after
that promotion, preventing incremental Columnar freshness from treating an old
source frontier as current.

## Combining with the next sync

After Group 1 appends and promotes unsynced Finalizes, Group 2 appends its
Prepared records. Group 2's one Prepare sync durabilizes both byte ranges:

```text
Group 1 Finalizes
+
Group 2 PreparedChange records
    -> one file.sync_data()
```

Successful bookkeeping then advances Group 1's checkpoint frontier and marks
Group 2's Prepared records durable. The same rule applies to every successful
NBCL sync: ordinary Prepare, ordinary Finalize, Phase 3F immediate barriers,
and explicit checkpoint flushes all checkpoint earlier pending Finalizes. One
physical sync increments `change_stream_sync_count` exactly once even when it
has both effects.

## Flush and clean close

`Database::flush_change_stream_checkpoints` explicitly flushes only pending
NBCL Finalize checkpoints. `Database::flush`, `Database::checkpoint`, and clean
`Database::close` also flush the final pending checkpoint through the existing
storage durability boundary. A no-op issues no sync.

A checkpoint sync error is returned to the caller; close cannot report false
success. Already published transactions remain committed and cannot roll back.
Reopen recovers them from PreparedChange plus authoritative outcome. SELECT and
Columnar reads never trigger the checkpoint sync.

## Crash recovery

Crash recovery distinguishes groups by their independent transactions and
authoritative outcomes, not by sync boundaries:

- before GroupDecision, staged current-group Prepared records resolve aborted;
- after GroupDecision, every participant resolves committed;
- a legal torn Finalize tail is truncated and its missing marker is repaired;
- complete-sized checksum corruption remains hard/unavailable corruption;
- a prior pipelined group remains committed if the next group's Prepared
  records were appended but the next GroupDecision never existed;
- a crash after publication but before Finalize sync reconstructs the same
  batches, source frontier, and published G;
- multiple committed records missing Finalizes are appended in frontier order
  and repaired with one recovery sync per NBCL file.

CORD v5 remains the sole database-global decision authority. No new sidecar,
group identity, or DatabaseCommitSeq is stored in NBCL.

## Reclamation and retention

Promoted but uncheckpointed Finalizes require their Prepared evidence for
crash recovery. Therefore `pending_finalize_checkpoint_count > 0` is a distinct
`FinalizeCheckpointPending` safety blocker:

- the direct NBCL GC writer rejects rewrite/reclamation;
- adaptive advice reports the explicit blocker;
- stale proposal revalidation aborts without mutation;
- normal reclamation resumes after a successful checkpoint.

It is not reported as `PreparedChangesUnresolved`, because the authoritative
transactions are already committed. Existing retention pins,
`SafeReclaimThrough`, and adaptive scheduling priorities do not change.
Explicit stream disable remains the existing crash-safe history-abandonment
operation and may discard the whole incarnation, including its pending
checkpoint; ordinary GC/rewrite cannot.

## Columnar boundary

Runtime promotion precedes database publication, so an incremental Columnar
projection immediately observes the new source frontier as Lagging and can
consume `read_changes` to become Fresh. Columnar advance neither synchronizes
NBCL nor becomes an authority for its source. After crash/reopen the source
frontier is reconstructed exactly, and existing Columnar freshness rules apply.

## Sync structure

For 100 changing transactions, group size 10, one stream-enabled storage:

```text
Phase 3F ImmediateBarrier:
    10 Prepare syncs + 10 Finalize syncs = 20 foreground syncs

Phase 3G PipelinedCheckpoint:
    10 Prepare syncs
    9 of them also checkpoint the previous group's Finalizes
    0 immediate Finalize syncs
    = 10 foreground syncs
    + 1 final explicit/clean-close checkpoint sync
```

Heap+LSM applies this independently to each physical StorageId: 20 foreground
syncs plus two final checkpoint syncs, not one cross-storage fsync.

## Explicit non-goals

- Finalize checkpoint is not the ChangeBatch commit authority.
- Pipelined Finalize is not asynchronous authoritative commit.
- PreparedChange durability is not deferred.
- Finalize append is not deferred.
- StorageDataVersion is not DatabaseCommitSeq.
- DatabaseTxnId correlation is not global CDC ordering.
- Pending Finalize checkpoint is a GC retention blocker.
- Ordinary transaction durability is unchanged.
- Active multi-writer execution is not enabled.
- No timer, background flusher, request queue, or server automatic grouping is
  introduced.
- NBCL v1/v2, FINALIZE_TAG, CORD v5, authoritative Prepare/Commit barriers,
  Serializable/SSI scope, historical snapshots, RowEntityId, and Columnar
  authority are unchanged.
