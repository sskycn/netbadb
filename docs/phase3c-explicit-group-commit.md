# Phase 3C: explicit synchronous group commit

Phase 3C adds a caller-driven embedded batching API for databases that have
durable global visibility enabled. It does not add a worker, timer, async
commit, or a Local-mode compatibility path.

## API and ownership

```rust
let mut group = database.begin_group_commit()?;

let mut member = database.begin_group_member(&group)?;
database.insert_into_in(table_id, &mut member, &values)?;
database.park_group_member(&mut group, member)?;

let report = database.commit_group(&mut group)?;
```

`park_group_member` consumes the transaction handle. A successful park has
durably prepared every write participant, moved the handle into the ordered
batch, and released each storage's mutable writer lease. The old handle cannot
issue statements or writes. A batch and all its members are bound to the
`Database` instance that created them.

Only one group may be active. It captures the published `DatabaseSnapshot` at
begin and every member reads that same base plus its own writes. Members never
see another member's prepared work. Ordinary writers, schema work, coordinator
compaction, checkpoints, close, and quiescent maintenance are rejected while
the group barrier is active. The first version is data-DML only and rejects an
empty or read-only member.

If prepare/park cleanup is uncertain, the consumed member remains owned by the
batch in `PrepareResolutionRequired`. New members and commit are blocked until
`resolve_group_member_prepare` durably finishes its rollback; `abort_group`
also resolves that member before unwinding the already parked tail.

## Storage state and ordering

Ordinary `prepare` is unchanged and retains the writer. The group-only path is:

```text
Active -> PreparePending -> Prepared -> ParkedPrepared
```

Each authoritative storage holds a runtime-only ordered deque of parked
physical transaction IDs. The existing Prepare WAL record remains the sole
crash-recovery authority. Commit removes the deque head in member order;
pre-decision abort removes the tail in reverse member order. Middle removal is
rejected.

The reverse rule is essential for Heap: later transactions' before-images may
contain earlier parked physical mutations on the same Heap or B+Tree page.
Global-LSN reverse recovery and runtime tail abort restore the exact baseline.
Heap detects an `xmax` owned by another parked transaction and LSM retains
`(LsmRowId, base committed version)` write intents. Conflicting update/delete
pairs fail immediately with `PreparedWriteConflict`; this is dirty-write
exclusion, not predicate locking or serializable isolation.

NBCL may retain several prepared reservations. A new reservation uses the
previous unresolved reservation's `after` value; the committed frontier does
not advance until head-order finalization. Group abort discards reservations
from the tail. Thus committed batches remain gap-free without making parked
state a second durable file.

## Coordinator decision and publication

The NBCO file header remains v1. CORD v5 adds one bounded, checksummed group
record containing an ordered list of independent database transaction IDs and
their canonical participant lists. The record binds a consecutive commit
sequence range. Its single `sync_data` is the all-or-none commit point for the
entire group.

After that sync, rollback is forbidden. Core completes every member's local
participant commits in group order. Only after every participant is durable
does it append the existing per-transaction sequenced Complete checkpoints
without a foreground sync and publish one final storage boundary vector at the
last group sequence. A failure during participant apply retains the batch in a
commit-only retry state. Recovery
expands a valid group record into the same independent decision inventory used
by Phase 3A/3B. A missing or torn tail record supplies no decision, so prepared
participants abort; a complete valid record commits every member.

The group record is bounded to 1,024 members and 1,024 total participants.
CORD v1-v4 and ordinary Phase 3B single decisions remain readable and retain
their existing behavior.

## Explicit non-goals

- Parked Prepared is not an active concurrent writer; each storage still has
  at most one active mutable writer.
- Group commit is synchronous, not async commit, and a successful return means
  every participant commit is durable.
- Every member retains its own `DatabaseTxnId` and its own consecutive
  `DatabaseCommitSeq`; the group does not collapse either identity.
- The API provides neither serializable isolation nor historical `AS OF`
  snapshots. A GroupDecision records commit ordering, not a historical
  snapshot vector.
- Columnar projections remain derived and non-authoritative.

## Inspection and benchmarking

`inspect_group_commit` reports the active group ID, base G, member IDs,
participating storage count, and lifecycle state. Global visibility inspection
separates ordinary decision syncs, group decision syncs, transactions decided
by groups, and group syncs that also made prior Completes durable. Storage
runtime inspection reports the parked chain and dirty-write conflict count.

Run the structural matrix with:

```text
cargo bench -p netbadb-core --bench global_group_commit_phase3c
```

It covers Heap, LSM, and mixed Heap+LSM members at 100 and 1,000 transactions
with group sizes 1, 4, 8, 16, and 32. Output includes exact coordinator sync
counters, transactions per sync, elapsed time, coordinator bytes, published G,
and authoritative Heap/LSM WAL bytes. It also runs explicit Heap and LSM
same-row conflict scenarios and verifies the surviving group and final data.
