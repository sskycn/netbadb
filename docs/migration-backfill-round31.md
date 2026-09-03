# Migration backfill / DDL–DML–DDL architecture audit — Round 31

Round 32 implements the selected controlled single-Heap slice described by
this audit. The current implementation details and exact remaining scope are
recorded in [`controlled-backfill-round32.md`](controlled-backfill-round32.md).

## 1. Commits / integration

Repository is `gostartkit/netbadb`. The audit began from `395c97c05ffafe93730430a965844071aff9b903` in isolated branch `codex/migration-backfill-round31` and worktree `netbadb-migration-backfill-round31`. Final task commit, merge, origin verification, and cleanup are recorded in the completion report after they occur.

## 2. Current seal behavior

`SchemaCompositionState::Composing` accepts ordered DDL. An eligible one-target
transaction enters `BackfillOpen` on its first relational statement and keeps
using the private staged Heap. Compatible rename/nullability DDL enters
`Refining`; data access then fails with a typed transaction-state error. Other
compositions retain the original `Materialized*` seal.

## 3. Current materialization protocol

Materialization freezes the complete logical overlay, calculates the target `G+1/E+1` snapshot and digest, reserves each final StorageId, writes and syncs the aggregate NBSJ intent, creates owner-bound private Heap resources, copies source rows, installs the final index inventory, and enlists every physical write participant. DML then writes the staged participant. Prepared NBSC and CORD decision are later commit steps, but the aggregate intent already claims a final target.

## 4. Aggregate intent finality

Current tag 27 `TableObjectChangeSetIntent` (or its ALTER-only predecessor) binds target generation/epoch, snapshot digest, final table plans, fingerprints, indexes, resources, and participant identities. A later `SET NOT NULL` changes `Fa` to `Fb`, invalidating the intent and snapshot digest. Rewriting that durable record would create ambiguous crash authority. CORD cannot repair this: it decides a participant set only after prepare and requires a single already-final NBSC reference.

## 5. Staged Heap state after materialization

For the primary sequence, committed state remains `T/V1/F1/S1`; private state is the same `TableId` at provisional `V2/Fa/S2`. Existing rows were copied to S2 with the added column encoded as `NULL`. S2 has its own Heap file, WAL, transaction-status file, StorageId, owner evidence, IndexCatalog root, and any BTree pages. It is enlisted as a write participant and substituted for S1 by transaction-local bindings.

## 6. Backfill read-your-writes

The direct Core test updates row 1 in S2, starts a new statement view from S2's already-enlisted participant context, and reads `[(1, "filled-one"), (2, NULL)]`. After updating row 2, the same path reads both new values. The query executor and the audit validator consume the identical transaction statement view; no committed-source scan is used.

## 7. SET NOT NULL afterbackfill validation

A `#[cfg(test)]` validator scans the candidate column in transaction-visible S2. Partial backfill returns exact `NotNullViolation(ColumnId(3))`; full backfill succeeds. Round 32 routes the same validation through the controlled refinement path: a failed statement leaves `BackfillOpen` unchanged, while a successful statement updates the private logical target and enters `Refining`.

## 8. Transaction-created table backfill

A direct test composes `CREATE TABLE imported`, materializes it on first INSERT, reads its own inserted row from the private CreateHeap, and accepts a compatible `SET NOT NULL` refinement before commit. Rollback still removes the private table.

## 9. Heap metadata binding

Heap metadata v5 occupies page 0 bytes 16–87: magic `NBD1`, version/reserved bytes, TableId, column count, 32-byte complete `TableDef` fingerprint, IndexCatalog root PageId, StorageId, and trailing reserved bytes. Row tuples are positional tagged scalars and contain no TableId, ColumnId, arity, or schema fingerprint. The owner file separately binds database transaction, TableId, StorageId, incarnation, and fingerprint. `.schema-link` is publication discovery metadata and is absent from a private staged target. WAL/status do not independently define the table schema.

## 10. Private metadata retarget experiment

Experiment Fa is a three-column table whose added text column is nullable; Fb keeps order, IDs, and physical types but makes it NOT NULL and renames the table. Opening the Fa Heap as Fb fails `SchemaMismatch` before retarget. A test-only page-0 rewrite changes the Heap metadata to Fb and syncs it. Bytes after page 0, WAL bytes, and transaction-status bytes remain identical; both RowIds and all row values remain exact; reopen under Fb succeeds. Therefore S2 itself can become the final unindexed Fb Heap without an S2→S3 row rewrite, provided owner evidence is updated in the same private finalization protocol.

## 11. Index/BTree impact

IndexCatalog v9 identifies IndexId/name/ColumnId/BTree handle but the BTree metadata page persists `IndexSpec { semantic/physical type, nullable }`. Changing indexed-column nullability in only the Heap header makes reopen fail `CatalogSpecMismatch(ColumnId(3))`. Table or column rename changes the Heap fingerprint but not the ColumnId-based BTree spec; the rename experiment reopens with the unchanged BTree pages and index. Indexed nullability/type retarget is therefore outside the first scope.

## 12. Candidate A

Keep S2 private, apply backfill DML there, accumulate only layout-compatible final logical refinements, validate them against S2, then at COMMIT retarget S2 once from Fa to final Fb before final intent/prepare. Cost is one base-to-target copy and peak S1+S2. It needs a private, crash-safe Heap/owner metadata retarget primitive and split durable stage/finalization authority.

## 13. Candidate B

After backfill, copy S2 into final S3 using Fb. This avoids metadata retarget but needs a second copy, peak S1+S2+S3, another scratch lifecycle, exact S2 read-participant/S3 write-participant coordination, and cleanup rules that do not manufacture replacement-retirement history. It remains the fallback if a future layout cannot be retargeted.

## 14. Candidate C

Keep DML as a durable delta over a provisional snapshot and apply it while building final storage. This requires a new typed durable mini-storage, read-view merge, bounded updates/deletes, replay-independent recovery, and conflict semantics. It is substantially broader than the primary backfill case.

## 15. Candidate D

Use a specialized sidecar for added-column values keyed by stable row identity. Current RowIds are storage-local and a rewrite changes them; deletes/updates/indexes and compaction would require a new identity/merge contract. It adds a second physical representation whose lifetime crosses planning, execution, recovery, and GC.

## 16. Comparison matrix

| Candidate | Copies | Peak main Heap set | New durable machinery | Read semantics | First-scope risk |
|---|---:|---|---|---|---|
| A: private retarget | 1 | S1 + S2 | stage/final intent + private metadata/owner retarget | existing S2 MVCC | lowest for proven layouts |
| B: second rewrite | 2 | S1 + S2 + S3 | scratch ownership/participants/cleanup | scan S2, write S3 | medium/high |
| C: DML delta | 1 final copy | S1 + delta + S2 | new delta format and merge engine | new merged view | high |
| D: sidecar | variable | S1 + sidecar + final | stable row key and sidecar lifecycle | new joined view | highest |

## 17. Chosen architecture

Choose Candidate A: **Controlled Backfill Phase + Private Heap Metadata Retarget**. It reuses the already-correct staged MVCC relation and preserves one final winner. This is a Round 32 design selection, not a Round 31 production claim.

## 18. Future phase state machine

```text
None
 -> Composing(DDL*)
 -> StageMaterializing
 -> BackfillOpen(SELECT/DML*)
 -> FinalRefining(layout-compatible DDL*)
 -> FinalRetargeting(one Fa->Fb pass per staged Heap)
 -> FinalIntentDurable
 -> PreparingParticipants
 -> DecisionDurable(commit|abort)
 -> PublishingOrCleanup
```

The first relational statement globally materializes all surviving plans. The first accepted refinement ends BackfillOpen; no later DML is accepted. Failed validation leaves the logical final schema unchanged and permits correction while still in BackfillOpen. COMMIT with no refinement may finalize Fa directly.

## 19. DML/read policy

BackfillOpen allows ordinary `SELECT`, `INSERT`, `UPDATE`, and `DELETE` only where every write target owns a private staged CreateHeap/RewriteHeap. Reads may include stable committed tables through the existing transaction view. After the first final refinement, user DML is rejected; this prevents repeated DDL–DML–DDL cycles and ensures validation cannot be invalidated. No SQL is stored or replayed.

## 20. Post-DML DDL compatibility matrix

| Action after DML | Round 32 decision | Reason |
|---|---|---|
| SET NOT NULL, non-indexed column | allow | staged scan + proven layout-compatible retarget |
| DROP NOT NULL, non-indexed column | allow | no data validation; same row layout |
| RENAME TABLE | allow | Heap/owner fingerprint only |
| RENAME COLUMN, indexed or not | allow | stable ColumnId; BTree spec unchanged |
| ADD/DROP COLUMN | reject | changes positional row shape after DML |
| physical/semantic type change | reject | codec and possibly BTree spec change |
| CREATE/DROP INDEX | reject | changes physical participant/final inventory after DML |
| CREATE/DROP TABLE | reject | changes global resource/participant plan after staging |

## 21. Layout compatibility matrix

| Change | Row bytes | Heap page 0 | owner | IndexCatalog | BTree metadata | First scope |
|---|---|---|---|---|---|---|
| non-indexed SET/DROP NOT NULL | unchanged | change | change | unchanged | n/a | yes |
| table rename | unchanged | change | change | unchanged | unchanged | yes |
| column rename | unchanged | change | change | unchanged | unchanged | yes |
| indexed nullability | unchanged | change | change | unchanged | **change required** | no |
| physical/semantic type | may reinterpret | change | change | unchanged | change if indexed | no |
| ADD/DROP/reorder column | changed positional shape | change | change | may change | may change | no |

## 22. Statement-time validation

`SET NOT NULL` resolves a stable ColumnId in the current private logical overlay and scans that column through the active S2 statement view. Any visible NULL rejects the statement before changing the final overlay. Full success records the refinement in memory; a later DML cannot invalidate it because DML is closed. COMMIT performs no surprise semantic rescan, though it rechecks state/fingerprint invariants.

## 23. Prepared dependency semantics

Statements prepared in BackfillOpen bind provisional Fa and transaction scope. Fb has a different fingerprint, as the direct test proves. Entering FinalRefining invalidates all Fa-bound private prepared statements; new DML preparation/execution is forbidden. Final published dependencies bind V2/Fb. No prepared statement silently reinterprets Fa as Fb.

## 24. TableSchemaVersion/generation

The transaction uses one provisional/final table version V2 throughout; refinements do not consume V3. The database publishes exactly one SchemaGeneration `G+1`, catalog epoch `E+1`, and runtime revision increment for the entire composition. Rollback and predecision crash publish none. Reservation floors still burn accepted IDs.

## 25. StorageId strategy

Candidate A assigns one final physical StorageId S2 per surviving CreateHeap/RewriteHeap at global materialization. The same S2 is backfilled and retargeted once; no S3 exists. In-place index-only plans cannot enter BackfillOpen because they do not own a private target.

## 26. Durable stage intent

Round 32 needs a new immutable NBSJ record, not reinterpretation of tag 27. `StageResourceIntent` authorizes reservations, exact private resource paths, owner identities, provisional Fa definitions, source identities, cleanup, and expected physical participant candidates. It explicitly does **not** claim final schema, final fingerprint, final NBSC digest, or final participant set.

## 27. Durable finalization intent

After all staged Heaps are durably retargeted to Fb, append and sync one immutable `FinalizationIntent` binding action digest, final schemas/fingerprints/index inventories, exact final resources/participants, target generation/epoch, and NBSC snapshot digest. There is no amendment chain. Absence of this record means the migration cannot commit.

## 28. Prepared NBSC

Prepare exactly one NBSC from FinalizationIntent after retarget durability. It contains V2/Fb and S2 for every surviving table. Fa is never an NBSC winner. Recovery verifies the prepared file against the finalization digest rather than recompiling or replaying migration SQL.

## 29. Coordinator

CORD v2 remains the sole global commit/abort authority and need not change format. A commit decision is legal only after FinalizationIntent, prepared NBSC, and all exact S2 participants are durably prepared. Its canonical TableId/StorageId participant set comes from FinalizationIntent. CORD never chooses between Fa and Fb.

## 30. Rollback/predecision crash

Rollback or crash before durable CORD commit makes base S1/V1/F1 the winner. Recovery uses StageResourceIntent to abort/resolve staged physical transactions, remove S2 Heap/WAL/status/owner artifacts, and discard prepared/final files if any. The Round 31 abrupt-exit test after full UPDATE reopens three times with base schema/data and burned allocator floors.

## 31. Winner recovery

After durable commit, recovery requires final S2/Fb metadata and owner evidence to match FinalizationIntent, resolves prepared S2 transactions as commit, publishes the one prepared NBSC, activates S2, and retires S1. Missing/mismatched final evidence is recovery-required corruption, never permission to fall back to Fa or replay SQL.

## 32. Scratch/staging cleanup

S2 is a private staged resource before decision and the active final resource after commit. Loser cleanup is exact-resource deletion, not replacement retirement. Winner cleanup removes transient prepared/owner staging evidence only after publication; the active Heap/WAL/status remain. Idempotent cleanup follows existing retained journal progress.

## 33. Retirement/GC

Only committed S1 becomes replacement-retired after the winner publication boundary. Existing Round 25 recovery horizons and GC apply. S2 is never retired as an intermediate version because no intermediate schema commits. Candidate B would require a distinct scratch class; Candidate A avoids it.

## 34. Multi-table backfill

Global materialization remains database-wide and TableId ordered. Backfill DML may touch several private S2 resources. Final refinement closes DML globally; COMMIT validates and retargets each affected Heap in TableId order, then emits one FinalizationIntent/NBSC/CORD decision. Any retarget failure before decision forces the whole transaction to rollback/recovery-required cleanup; no subset publishes.

## 35. Physical/WAL cost

Candidate A retains one base-to-S2 copy and peak S1+S2 main files; Candidate B adds a second copy and peak S1+S2+S3. Backfill DML already emits ordinary S2 WAL. The page-0 test rewrite itself changed neither row pages nor existing WAL/status bytes, but production Round 32 must provide an explicitly durable metadata/owner replacement protocol and fault injection rather than copying the raw test operation.

## 36. Alembic probe

The Round 31 probe recorded the pre-Round32 seal behavior: Alembic 1.16.5 generated `ALTER ... ADD`, deterministic supported `UPDATE`, and `ALTER ... SET NOT NULL`, after which the final ALTER returned SQLSTATE `25000` and the context rolled back. Round 32's native and server tests now cover the controlled success path; an updated external-client run remains environment-dependent.

## 37. psql

psql 17.11 previously observed the sealed Round 31 behavior and rollback. Round 32 preserves that failure behavior for non-eligible or unsupported cases while allowing the managed single-Heap compatible path; the external retest is deferred until the client dependency environment is available.

## 38. psycopg

psycopg 3.2.13 previously observed `InvalidTransactionState`, SQLSTATE `25000`, for the Round 31 seal. Round 32's prepared-statement semantics are covered by the native regression suite; the external compatibility retest remains pending.

## 39. SQLAlchemy

SQLAlchemy 2.0.52 previously carried SQLSTATE `25000` through the Round 31 `engine.begin()` context. Round 32's compatible refinement path is covered by the server protocol tests; the external ORM retest remains pending.

## 40. Persistent-format implications

Heap v5, Page v5, WAL v4/record v3, transaction status v1, BTree v3,
IndexCatalog v9, NBSC v1, and CORD v2 remain. NBSJ v1 now carries the new
stage/finalization records, and owner replacement uses the existing envelope
format with exact atomic replacement.

## 41. Crash matrix design

| Crash point | Required reopen winner | Required action |
|---|---|---|
| before/within stage intent | base | discard incomplete reservation/resource evidence safely |
| after stage intent, during copy/backfill | base | abort S2, exact cleanup |
| during final logical validation | base | no Fb durable authority exists |
| during Heap/owner retarget | base | detect/repair-or-delete private S2; never publish |
| after all retargets, before final intent | base | cleanup S2 using stage authority |
| after final intent/NBSC/prepare, before CORD commit | base | coordinator abort resolution + cleanup |
| after durable CORD commit, before publication | final | resolve S2 commit, publish V2/Fb, retire S1 |
| during/after GC | final | idempotently finish S1 retirement cleanup |

## 42. Tests

Round 32 adds Core tests for partial/full staged validation, staged SQL read-your-writes, multiple compatible refinements, transaction-created tables, global writer lifetime, typed boundary errors, intent target finality, Fa→Fb dependency change, rollback/reopen, and abrupt predecision cleanup. Storage tests cover baseline mismatch, raw private retarget, unchanged row pages/WAL/status/RowIds, indexed nullability mismatch, and indexed rename success. All helpers are `#[cfg(test)]` or example/script probes.

## 43. Fuzz

Existing parser/compiler/schema/catalog/journal/coordinator/storage fuzz targets remain applicable. Round 32 adds bounded decoding and validation for the new intent records plus explicit retarget crash points; broader fuzz corpus expansion remains follow-up work.

## 44. Compatibility

Existing Round 30 DDL-only behavior remains unchanged. Native controlled
backfill now permits the narrow compatible post-DML refinement phase; all other
post-materialization DDL remains rejected. No wire, SDK, manifest, schema-spec,
planner, or executor boundary changed.

## 45. Unsupported/deferred

Deferred: physical/semantic type conversion; ADD/DROP/reorder column after DML;
indexed-column nullability retarget; post-DML index/table creation/drop;
savepoints; online migration; cross-process writers; resumable backfill;
DEFAULT/backfill syntax; generated columns and broader constraints; arbitrary
expressions; LSM, partitioned, imported/external storage; repeated DDL–DML
cycles; and external-client acceptance runs for this round.

## 46. NBSJ/CORD compaction debt

NBSJ retains reservations, intents, resolutions, retirement, and GC progress indefinitely; CORD likewise retains decisions. Compaction needs an independent horizon and crash protocol. It must not be bundled with backfill or used to mutate an existing intent. CORD v2 capacity is not proof that a final participant set exists.

## 47. Completion workflow

The task runs formatting, all-target/all-feature check, clippy, tests, real-client regressions, fuzz, MSRV subsets, diff review, commit, fresh fetch, merge to main, push, origin verification, and task worktree/branch cleanup. Exact outcomes and environmental blockers belong in the final completion report.

## 48. Round32 recommendation

Implement exactly **Controlled Backfill Phase + Private Heap Metadata Retarget Foundation** for managed runtime Single Heap CreateHeap/RewriteHeap plans. Allow BackfillOpen reads and DML only on private write targets, then one DML-closing final refinement phase supporting non-indexed `SET/DROP NOT NULL` and table/column rename. Add immutable StageResourceIntent and FinalizationIntent, one crash-safe Heap+owner retarget at COMMIT, one prepared NBSC, and one CORD v2 decision. Do not add S3, delta/sidecar storage, indexed nullability, broader layout changes, savepoints, online/resumable migration, or LSM/partition/imported support.
