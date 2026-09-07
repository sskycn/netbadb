# Phase 3B.5: coordinator checkpoint and log compaction

Phase 3B.5 bounds completed global coordinator history with an explicit,
synchronous administrative operation:

```rust
database.compact_coordinator_log()?;
```

Normal commits append to NBCO forever because every durable Decision and
Complete was retained. Startup consequently scanned the whole file and rebuilt
the whole decision map. Those completed data decisions are recovery proof only
until every participant is terminal and Complete is durable; they are not
historical snapshot support.

Compaction is never automatic. Commit, close, open, maintenance planning, and
maintenance steps do not call it. There is no queue, timer, thread, async
runtime, concurrent coordinator writer, or group commit.

## Dependency and high-water audit

The completed-history consumers were audited before removing entries:

- Heap and LSM recovery need an exact Decision only for a participant that is
  still Prepared. A terminal participant's WAL contains its durable Commit or
  rollback outcome and no longer needs a historical Decision.
- incomplete coordinator decisions name exact physical participants and cannot
  be compacted;
- schema mutation recovery, orphan adoption/cleanup, replacement/backfill
  recovery, and retired-Heap GC still inspect structural decisions and their
  `SchemaParticipantReference`. This release therefore rejects compaction when
  any retained structural decision exists;
- schema/catalog epochs and TableId/StorageId/ColumnId/index allocator floors
  are independently retained by NBSC/NBSJ and their catalogs;
- `DatabaseCommitSeq` was recovered solely from sequenced coordinator history,
  so the checkpoint retains both the published and last-decision frontier;
- `DatabaseTxnId` allocation used the maximum historical coordinator ID plus
  inspected prepared participants, so the checkpoint retains its high-water to
  prevent identity reuse after restart.

No coordinator record identity exists beyond the typed transaction and commit
sequence fields. No speculative high-water was added.

## Persistent representation

The NBCO file header remains version 1. A compacted file contains:

```text
NBCO v1 header
CORD v3 GlobalEnable
CORD v4 CoordinatorCheckpoint
```

The fixed CORD v4 payload is:

```text
published_commit_seq       u64, nonzero
last_sequenced_decision    u64, nonzero and equal to published
database_txn_id_high_water u64, nonzero
```

CRC32C covers the normal CORD header and payload. GlobalEnable remains the
explicit durable mode transition; the checkpoint cannot imply it. Older CORD
v1/v2/v3 logs remain readable and are never rewritten during open.

At runtime, the checkpoint is the completed prefix baseline and the decision
map contains only the post-checkpoint tail. Published G is the maximum of the
checkpoint and completed tail; next G follows the maximum sequenced decision.
The first tail decision must be checkpoint G + 1, every later decision remains
gap-free, and every tail `DatabaseTxnId` must exceed the retained high-water.

The checkpoint deliberately stores no `StorageId -> StorageVisibilityBoundary`
vector. Startup first resolves the coordinator tail, then rebuilds the latest
vector from each authoritative storage exactly as Phase 3A specified.

## Admission and rewrite protocol

The first implementation requires:

1. Global visibility mode;
2. no outstanding `DatabaseTransaction` handle, including read-only RR;
3. available schema/catalog state with no unresolved mutation recovery;
4. every storage reporting recovery-ready, with no Prepared participant;
5. no retained structural decision;
6. all coordinator decisions complete after pending Complete records are
   appended and synchronized.

An empty G0 history and an already compacted file with no tail are no-ops.
Otherwise compaction captures the current published frontier, writes the header,
GlobalEnable, and checkpoint to the fixed `.next` path, synchronizes the file,
releases the old handle, atomically replaces the primary, synchronizes the
parent directory, reopens and validates the primary, then installs the new
in-memory checkpoint and clears the compacted map.

An orphan `.next` is never authority and never masks corruption in the primary.
A failure before replacement preserves a usable old primary. Rename failure
reopens the old handle. Directory-sync uncertainty or inability to reopen after
publication leaves the in-process coordinator unavailable, so subsequent
global writes fail closed; a later database reopen validates whichever complete
primary the filesystem published.

Deterministic crash tests cover every boundary from before temp creation through
in-memory replacement. Reopen accepts only the old complete log or the new
complete checkpoint file, never a half checkpoint or a regressed G. Additional
tests cover malformed checkpoints, orphan `.next`, injected write/sync/rename
failures, deferred Complete recovery, a partial post-checkpoint Decision,
DatabaseTxnId non-reuse, and schema commit after a data-only checkpoint.

## Semantic non-effects

- `CoordinatorCheckpoint` is not a historical `DatabaseSnapshot`.
- It does not allocate or create a `DatabaseCommitSeq`.
- Compaction does not change published visibility or `StorageDataVersion`.
- It does not establish `AS OF` queries or historical schema.
- It does not enable multi-writer transactions.
- It is not group commit.
- A `DatabaseTxnId` must never be reused because its old Decision was compacted.

This phase comes before true group commit because it first makes coordinator
history bounded without changing the one-transaction publication protocol.
Phase 3C may separately evaluate a synchronous batch-commit primitive; it must
not be inferred from this checkpoint format.

## Inspection and benchmark

Global visibility inspection now reports `checkpointed_through`, retained tail
decision count, coordinator bytes, and whether current state admits compaction.
`CoordinatorCompactionReport` reports before/after bytes and decisions, reclaimed
counts, flushed pending Completes, the checkpoint frontier and transaction
high-water, and whether work occurred.

`coordinator_compaction_phase3b5` defaults to 1,000 sequential one-row global
transactions. `NETBADB_COORDINATOR_COMPACTION_TXNS=1000,10000` selects larger
runs. It prints log size, retained map size, open time before/after, compaction
time, continued G, and transaction-ID non-reuse diagnostics. It measures history
size and recovery metadata cost, not commit-throughput improvement.

One local optimized run on 2026-09-07 produced:

| transactions | bytes before | bytes after | retained before | retained after | open before | compact | open after |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1,000 | 96,048 | 104 | 1,000 | 0 | 59.02 ms | 7.91 ms | 33.08 ms |

It reopened at next G 1,001, allocated `DatabaseTxnId` 1,001 above checkpoint
high-water 1,000, and published G 1,001 on the next write. Timings are
observations from one run and carry no threshold or latency guarantee.
