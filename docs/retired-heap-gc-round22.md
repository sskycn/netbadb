# Retired Heap physical GC — Round 22

Round 22 closes the physical lifecycle for an explicitly selected,
runtime-created, non-partitioned Heap. It adds no SQL and never scans for
candidates. The operator first inspects one exact durable
`RetiredTableResource`, then may pass that same complete identity back to Core.

## Recovery-retention proof

CoordinatorLog is an append-only decision history. A durable `Complete` record
means every participant is terminal, and current recovery already permits a
missing retired StorageId only for such a completed decision. For retired
storage `S`, the recovery horizon is the greatest DatabaseTxnId of the DROP
decision or any physical participant reference to `S`. Eligibility requires:

- the exact DROP schema decision exists and is Complete;
- every coordinator participant reference to `S` is Complete;
- the active NBSC, registry and bindings contain no `S`;
- no database transaction or schema-writer handle exists;
- the NBSJ create and drop histories, owner evidence, table identity,
  fingerprint, StorageId and generated locator all agree;
- Heap recovery inspection succeeds, and any prepared history is resolved by
  CoordinatorLog (or presumed abort when no decision exists) through a
  recovery-only Heap handle that is closed before intent;
- every required member of the exact supported bundle is present and is a
  regular non-symlink file.

The coordinator history is not compacted in this round. Its retained Complete
records are the durable horizon proof. A later reference to the retired
StorageId is impossible through normal Core routing and is treated as
corruption if it appears during retry.

## Exact resource bundle

`netbadb-storage` authoritatively lists the Heap main file, WAL, transaction
status file and optional alternate WAL. The main Heap contains IndexCatalog and
BTree pages. Core adds its exact `NBST` owner file, `NBSL` catalog discovery link
and optional atomic-write shadow. The bundle digest covers ordered component
kind, requiredness and database-relative generated path. No directory scan,
glob, recursive removal or table name is involved.

Only the generated locator
`<catalog>.resources-<incarnation>/storage/<StorageId>.heap` is supported.
Bootstrap/import locators, LSM and range partitions are reported as ineligible.
Ancestor and leaf symlinks, directories at file paths, ownership mismatch and
unknown identity are hard errors.

## Durable state machine

The states are:

```text
Active -> Retained -> Deleting -> Deleted
```

NBSJ v1 tag 9 is `RetiredHeapGcIntent`: DROP transaction, coordinator horizon
u64 and bundle SHA-256. Tag 10 is `RetiredHeapGcComplete`: DROP transaction.
The surrounding DROP fragment already persists incarnation, TableId,
TableSchemaVersion, fingerprint, StorageId, locator and retirement generation.
Before tag 9 is written, capacity is checked for both tags 9 and 10.

After durable intent, deletion is retry-only. Every exact leaf is revalidated
with `symlink_metadata`, present known files are unlinked, and the parent
directory is synchronized before Complete. Startup never selects a Retained
candidate; it only resumes an existing Deleting record or verifies a Deleted
record.

Recovery interpretation is strict:

- Retained + present: normal retained evidence;
- Retained + missing: unexplained loss, hard error;
- Intent + full/partial/absent bundle: retry exact known deletion, sync, Complete;
- Complete + absent: valid terminal state;
- Complete + any reappeared component: hard conflict;
- identity, horizon or manifest mismatch: hard corruption.

The active schema generation, runtime catalog revision and every identity
high-water remain unchanged by inspection or GC. Deleted StorageIds remain in
NBSJ history and are never reused. Same-name recreation obtains new TableId,
StorageId and locator, so deleting the old exact bundle cannot reach the new
table.

## Public Core API

`Database::inspect_retired_heap_gc(&RetiredTableResource)` returns the state,
horizon, digest, exact component paths/sizes and blockers without mutation.
`Database::gc_retired_heap(&RetiredTableResource)` performs the explicit
single-resource transition and returns deleted file/byte counts. Passing a
modified or stale resource token is rejected.

## Crash and growth coverage

Subprocess tests crash before intent, after durable intent, before the first
unlink, after each of seven component positions, after directory sync and after
durable Complete. Every case converges across three reopens; the pre-intent case
remains Retained until explicitly requested again. Additional tests cover
unexplained missing resources, symlink refusal, target mismatch, imported Heap
refusal, terminal reappearance conflict, same-name recreation, and an indexed
Heap whose index bytes are removed with its main file.

A deterministic 100-cycle create/drop/GC test proves monotonically increasing
TableId/StorageId, unchanged GC generation semantics, no remaining known
physical component per cycle, and three clean terminal reopens. NBSJ and CORD
history still grow by design; journal/coordinator compaction is deferred.

On the validated test fixture, one retired Heap reclaimed 5 present components
and 12,649 bytes. After 100 create/drop/GC cycles, known retired physical bytes
were 0; the retained NBSJ history was 105,643 bytes and the retained CORD
history was 25,616 bytes. These figures characterize the deterministic fixture,
not a fixed on-disk size guarantee.

## Compatibility and deferred work

NBSC, NBSM, NBST, NBSL, CORD, Heap, WAL, transaction-status, index, native
protocol and PostgreSQL wire versions do not change. NBSJ remains envelope
version 1 and gains tags 9/10; older binaries reject those tags, so downgrade
after GC intent is unsupported. SQL, prepared statement and frontend behavior
are unchanged because no GC SQL surface exists.

Deferred: LSM GC, partition GC, unknown-orphan cleanup, background/automatic
GC, startup candidate selection, journal/coordinator compaction, cross-process
writer exclusion, identity reuse and ALTER TABLE.
