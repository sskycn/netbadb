# Phase 3A: global commit sequence and snapshot publication

> Historical baseline: Phase 3A introduced the two-sync protocol documented
> below. [Phase 3B](phase3b-global-commit-pipeline.md) now pipelines Complete
> synchronization for pure data transactions while retaining this conservative
> protocol for structural transactions.

Phase 3A adds an opt-in database-wide committed visibility order without
replacing either storage engine's MVCC domain. The database-global sequence is
publication metadata; Heap and LSM continue to own their physical histories.

```text
DatabaseCommitSeq (G0, G1, G2, ...)
                ↓
      published DatabaseSnapshot
                ↓
 sorted StorageVisibilityBoundary vector
                ↓
       Heap horizon / LSM horizon
                ↓
 authoritative reads; Columnar only when eligible
```

`DatabaseCommitSeq`, `DatabaseTxnId`, Heap `CommitSeq`, LSM `LsmCommitSeq`,
`StorageDataVersion`, and `StorageSnapshotToken` are intentionally different
types. Their numeric values are not interchangeable. G0 is the initial
database snapshot; committed writes publish non-zero, gap-free G values.

## Persistent mode and NBCO compatibility

Legacy databases remain in `LegacyLocal` mode and retain their existing
single-writer behavior. A coordinator-backed database can opt in at creation
with `DatabaseCoordinatorConfig::with_global_visibility()`, or can make a
quiescent one-way transition with `Database::enable_global_visibility()`.
There is no disable transition.

The NBCO file header remains v1. This deliberately avoids rewriting a durable
header during upgrade and keeps historical NBCO v1 decisions readable. A
checksummed, synced CORD v3 `GlobalEnable` record persists the mode. CORD v3
also carries sequenced decision and Complete records. CORD v1 data decisions
and CORD v2 schema-reference decisions retain their original codecs.

On open, the persisted record—not an open-time option—selects the mode. The
coordinator resolves every prepared decision before a `Database` is returned,
finishes missing Completes in sequence order, then reconstructs the latest
visibility vector from the current authoritative storage inventory.

## Historical Phase 3A commit and publication protocol

Every authoritative writer in global mode, including a single Heap or LSM
writer and a no-op writer already enlisted as a write participant, follows:

1. Prepare all physical write participants in canonical `StorageId` order.
2. Allocate the next G and append/sync one sequenced coordinator decision. The
   decision binds `DatabaseTxnId`, G, canonical physical participants, and an
   optional schema participant reference.
3. Commit every prepared participant. Participant commit order does not affect
   visibility.
4. Append and sync a sequenced Complete binding the same transaction and G.
5. Publish one in-memory `DatabaseSnapshot` containing G and the updated,
   sorted storage boundary vector.

In the Phase 3A baseline, Complete is the durable publication point. The
synchronous database worker does not serve another statement between durable
Complete and in-memory vector publication. A crash in that window reconstructs
the vector during recovery. Decision and Complete retries are idempotent: an
uncertain sync retries the same transaction and G. A later G cannot Complete
while an earlier sequenced decision is incomplete.

Read-only transactions allocate no G. The Phase 3A baseline intentionally pays
the coordinator decision and synchronization cost for a single writer;
bypassing the sequence would permit a reader to observe a state that no
published database snapshot names. Group commit and asynchronous batching are
deferred.

## Storage visibility boundaries

`StorageVisibilityBoundary` binds a `StorageId`, a `StorageKind`, and an opaque
non-zero encoded local horizon. Encoding value 1 represents an engine-local
horizon of zero, so value zero is always invalid. Storage rejects a boundary
from another identity or engine kind and rejects a future boundary.

Heap maps the boundary to its durable MVCC commit sequence. `read_view_at`
pins the Heap transaction-status horizon, so vacuum cannot remove versions
needed by the view. LSM maps the boundary to `LsmCommitSeq`; the existing
read-view lifetime protects the required version history from destructive
maintenance. Neither mapping creates an order between two storage engines.

## Read semantics

Autocommit and each Read Committed statement clone exactly one published
database snapshot, then open every participating storage at the boundary in
that vector. A statement therefore cannot combine one participant before G
with another participant after G.

Repeatable Read captures one database snapshot on its first statement and
pins every then-current authoritative storage boundary. Later access to a
previously untouched storage still opens at that original boundary. The pins
hold storage history, not decoded rows or a row cache. Transaction-local writes
remain visible through each engine's existing own-transaction semantics on top
of the committed base snapshot. Terminal commit or rollback releases all pins.

Schema replacement publishes a reconciled vector containing the new
authoritative identities and excluding retired identities. Private staged
storage remains transaction-local and is not inserted into a public snapshot
before schema publication.

## Columnar eligibility

Columnar remains derived and non-authoritative. Autocommit can use a fresh
projection only when planning verifies it against the current authoritative
storage state, which is also the latest published global snapshot. Explicit
transaction execution currently passes no Columnar projections, so an old RR
snapshot always falls back to Heap/LSM. A projection can never make data ahead
of the published vector query-visible.

## Inspection and scope

`Database::inspect_global_visibility()` reports mode, published G, next G, and
the sorted storage boundary diagnostics. Phase 3B extends that inspection with
the appended/synced Complete frontier, pending count, checkpoint error, bytes,
and runtime sync counters. `current_database_snapshot()` exposes the typed
snapshot for embedded coordination and tests.

This phase does not add `AS OF`, persistent historical G-to-vector lookup,
historical schema snapshots, Serializable isolation, group commit, async commit
batching, global CDC order, `RowEntityId`, authoritative Columnar storage, or
distributed consensus. Those remain later work.
