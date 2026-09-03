# Staged incompatible-index evacuation lifecycle — Round 35

Round 35 implements a private Core migration foundation for an already-open
staged backfill. It does not expose indexed-nullability migration through SQL.

## 1. Commits / integration

Development starts from synchronized `main` in an isolated task worktree. The
final task report records commit, merge, push, containment, and cleanup evidence.

## 2. External-client status

Real psql 17.11 at `/opt/local/lib/pgsql/bin/psql`, with the required ICU
library path, passes the Round 32 migration, Round 33 final-index migration,
and Round 35 DROP-first negative probe. psycopg, SQLAlchemy, and Alembic remain
unavailable; nothing was installed, and internal tests are not reported as
external-client results.

## 3. Existing public indexed-nullability behavior

Public `DROP INDEX` keeps the Round 33 logical path. Direct DROP-first migration
still seals on S1 and cannot enter this private lifecycle.

## 4. Evacuation state machine

The typed path is `BackfillOpen -> IndexEvacuating ->
RefiningAfterEvacuation -> IndexFinalizing -> Finalized`. A migration with no
final CREATE goes from `RefiningAfterEvacuation` to `Finalized` at commit.

## 5. Internal evacuation API

The crate-private `evacuate_staged_backfill_index_in` primitive accepts the exact
typed `(TableId, IndexId)` `DropIndexTarget`; it never resolves a name.

## 6. Evacuation preconditions

Admission requires one managed Single Heap, one active private S2 backfill,
`BackfillOpen` or `IndexEvacuating`, and the same exact active index in logical
and actual staged inventories. No final refinement or CORD may exist.

## 7. DML closure

The first success enters `IndexEvacuating`. Reads and writes then fail with
`MigrationDataAccessAfterRefinement`, and DML never reopens.

## 8. Physical Iold drop

The existing WAL-backed Heap retirement runs immediately inside S2's long-lived
participant transaction. S2's private in-memory registry is updated only after
the catalog retirement succeeds; uncertain failure requires rollback.

## 9. Committed S1 isolation

Evacuation never resolves or mutates committed S1. Before CORD, S1 remains the
published winner with its original rows and indexes.

## 10. StageIndexInventory

`MaterializedSchemaTransaction::staged_indexes` is separate from base and final
logical inventories. It is initialized after physical S2 index installation,
sorted by IndexId, and updated after every exact evacuation.

## 11. Logical final index inventory

The same successful operation removes Iold from the transaction-local final
inventory and adds action evidence. Repeated exact evacuations remain ordered.

## 12. Physical compatibility predicate

The active S2 definitions and high-water must exactly equal StageIndexInventory.
Every persisted BTree `IndexSpec` must equal the semantic type and nullability
derived from the candidate final `TableDef`.

## 13. `backfill_indexed_columns` handling

The historical guard remains unchanged for ordinary Round 32/33 and public
paths. Only typed evacuation states bypass it, after the exact physical gate.

## 14. SET NOT NULL staged validation

After compatibility succeeds, `SET NOT NULL` scans the transaction-visible S2
view, including all preceding backfill DML. S1 rows are not consulted.

## 15. Failed validation semantics

A NULL violation leaves `IndexEvacuating`, keeps DML closed, and never rebuilds
Iold. The caller may only roll back or attempt another allowed refinement whose
preconditions independently hold.

## 16. RefiningAfterEvacuation

Successful ALTER enters `RefiningAfterEvacuation`. Later rename/nullability
refinements repeat the exact compatibility gate; layout and type changes remain
rejected.

## 17. Heap/owner retarget

Finalization validates remaining indexes, retargets Heap metadata, validates
again, then retargets the owner envelope. It copies no row or index page.

## 18. Replacement index reservation

CREATE after refinement reuses Round 33 durable reservation. Inew burns a fresh
IndexId and cannot inherit Iold's identity. The representative fixture preserves
the unrelated I1, evacuates I2, creates I3, and leaves the next floor at I4.

## 19. Final index construction

Inew is built only after Heap retarget, from current transaction-visible S2 rows
and the final schema's `IndexSpec`.

## 20. Final IndexCatalog

Before tag 34, Core validates exact final active definitions, high-water, and
persisted BTree specs against the final schema.

## 21. Avoided double-drop

Finalization diffs current StageIndexInventory against the logical final
inventory. Evacuated Iold is absent from both and is not retired twice.

## 22. Row/RowId stability

S2 is copied once at materialization. Evacuation, refinement, retarget, and final
index work preserve tuple bytes and RowIds.

## 23. BTree/PageRef lifecycle

Iold uses existing generation-safe retirement. Inew has a new IndexId and an
independent generation-bearing handle; numeric page reuse is not identity reuse.

## 24. StorageId

The representative fixture moves TableId 2 from StorageId 2 to StorageId 3;
the next floor is StorageId 4. It creates no second replacement and performs no
late clone or selective detach from S1.

## 25. TableVersion / Generation / epoch / revision

For TableId 2 the fixture records TableVersion 1 to 2, plus exactly one
SchemaGeneration increment, one NBSC epoch increment, and one runtime revision
increment.

## 26. Persistent journal

No NBSJ tag or decoder changes. Existing tags 1–34 retain exact byte semantics.

## 27. StageResourceIntent

Existing whole-S2 intent is sufficient before decision. Recovery needs no
per-index cleanup record because private S2 is disposable as one resource.

## 28. tag34 finalization

Existing tag 34 remains the immutable final schema/index inventory proof. It is
written after retarget, final index work, and exact physical validation.

## 29. Prepared NBSC

Exactly one final NBSC is prepared from the refined target/digest. No intermediate
nullable or indexless schema is published.

## 30. CORD

One CORD v2 schema-commit decision authorizes S2, the prepared NBSC, and the one
physical participant.

## 31. Commit protocol

Commit from `IndexEvacuating` fails with `EvacuationRequiresRefinement`. Commit
from `RefiningAfterEvacuation` is valid with or without replacement CREATE. CORD
follows physical work, tag 34, NBSC preparation, and S2 prepare.

## 32. Rollback after evacuation

Rollback discards the whole S2 bundle. It does not rebuild Iold because committed
S1 never changed.

## 33. Rollback after retarget

Before decision, rollback still discards S2 instead of reverse-retargeting Heap
metadata or the owner envelope.

## 34. Predecision process recovery

Abrupt exits before CORD resolve S1 as winner and clean exact staged resources
from durable StageResourceIntent evidence, without SQL replay.

## 35. Winner recovery

After durable CORD, recovery finishes the existing prepared S2/NBSC publication
path and opens the exact final catalog.

## 36. No rebuild after decision

CORD is impossible until final index construction and validation complete.
Winner recovery therefore never rebuilds an index.

## 37. Replacement retirement

Successful publication retires exactly one predecessor S1 through existing
composition and coordinator evidence.

## 38. Replacement GC

Existing retired-Heap GC applies to S1 as one owned bundle. No automatic or
index-specific deletion policy is added. The Round 35 lifecycle test explicitly
deletes S1, then verifies StorageId 3, I1/I3, rows, and three further reopens.

## 39. Multiple incompatible indexes

Multiple exact evacuations may occur in `IndexEvacuating`. Each updates actual
and logical inventories before refinement.

## 40. Unrelated surviving index

An unrelated compatible index stays active across evacuation, rename, retarget,
publication, and reopen after exact spec validation.

## 41. Commit without replacement index

A refined evacuation may intentionally leave the column unindexed. Finalization
publishes the smaller exact inventory and preserves its high-water.

## 42. Direct DROP-first negative

`DROP INDEX; UPDATE; ALTER ... SET NOT NULL` on an otherwise unchanged table
still uses S1 index-only materialization and fails at ALTER. Real psql 17.11
observes SQLSTATE `25000`; rollback restores the exact old index.

## 43. Public SQL exposure status

No SQL, HIR mode, server API, protocol capability, CLI, or SDK selects evacuation.
This is a private Core lifecycle foundation.

## 44. Physical cost

The 128-row physical fixture remains 77,824 bytes (19 x 4-KiB pages) with Iold,
after DROP, after retarget, and after Inew rebuild because retired pages are
reused. The source Heap row visitor runs once during S1-to-S2 materialization;
evacuation/refinement/finalization perform zero further row copies and create no S3.

## 45. Crash matrix

The 16-point subprocess matrix spans the boundary before the drop through the
S2 participant commit, including retirement, inventory publication,
compatibility validation, refinement, retarget, final delta, tag 34, NBSC,
participant preparation, and CORD decision edges. Every point reopens three
times: predecision crashes retain S1, while durable commit decisions recover S2.

## 46. Persistent formats

Unchanged: TableSchema v1; NBSC/NBSM/NBSJ/NBSA envelope v1 with NBSJ tags 1–34;
NBPC v1; NBCO v1 with CORD v1/v2; Heap v5; NBPG v5; NBMV v1; IndexCatalog v9;
BTree v3; NBTR v1; Heap WAL v4/current record v5; NBTS/TXST v1; LSM manifest
v2/WAL v1/SSTable v2; native protocol v1; PostgreSQL v3; SDK Schema Spec v1;
deployment manifest v4.

## 47. Tests

Native coverage includes replacement, exact-target no-op, commit gate, failed
validation, no-replacement commit, multiple evacuations, compatible rename,
DML closure, one S2 participant, replacement GC, and reopen/crash recovery.
Existing public negative tests remain unchanged. Full all-feature workspace
check, Clippy, and tests pass on the pinned toolchain; psql 17.11 passes the
three external probes.

## 48. Fuzz

No decoder or target is added. All 13 existing targets passed 1,000 iterations
with seed 35 using untracked temporary corpora and artifact directories.

## 49. Compatibility

Public SQL/protocol behavior, prepared dependencies, persistent bytes, Round
32/33 paths, index identity, and coordinator authority remain compatible.

## 50. Unsupported/deferred

Deferred: direct DROP-first and public indexed-nullability migration, type
conversion, ADD/DROP after DML, table DDL, multiple tables, LSM/partition/import,
savepoints, online/resumable work, defaults/constraints, detach, and S3 cloning.

## 51. NBSJ/CORD growth

NBSJ and CORD retain existing unbounded history. Checkpointing, pruning, and
bounded decision retention remain debt.

## 52. Completion workflow

Completion requires all-feature Rust validation, MSRV attempt, fuzz, diff review,
task commit, merge/push of main, remote containment proof, and clean task cleanup.

## 53. Round36 recommendation

Audit production orchestration before SQL exposure: authorization, prepared
targets, retry/error mapping, external clients, and whether DROP-first remains a
separate workflow. Do not broaden storage placement or recovery scope.
