# Source-participant backfill and late final clone (Round 38)

Round 38 implements the crate-private foundation selected by the Round 37 audit.
Public SQL routing is deliberately unchanged: `DROP INDEX` followed by DML still
uses the efficient in-place source Heap path, while a later SQL `ALTER TABLE`
continues to fail with SQLSTATE `25000`. Tests enter the new lifecycle through a
crate-private transition only.

## State and admission

`SourceBackfillOpen` is entered only after a one-table, managed Single Heap,
drop-only index composition has materialized on the existing source StorageId
S1. S1 must already be the transaction's write participant. No target StorageId,
stage resource, or prepared NBSC exists at this point. Same-table SELECT, INSERT,
UPDATE, and DELETE continue through the ordinary planner, executor, and
`StorageTransaction`, preserving read-your-writes. Cross-table access is rejected.

The crate-private refinement accepts only `SET NOT NULL`, `DROP NOT NULL`, table
rename, and column rename. `SET NOT NULL` scans the transaction-visible S1 view.
An incompatible index must be both absent from the final logical inventory and
exactly inactive in this transaction's S1 publication delta. Failed validation
keeps `SourceBackfillOpen`, creates no S2, and permits native repair and retry.
The first successful refinement enters `SourceRefining`, assigns provisional
V2/F2 once, and closes all relational execution. Later compatible refinements
remain V2. Final logical CREATE/DROP INDEX actions use `SourceIndexFinalizing`;
new indexes reserve fresh IndexIds but are not built on S1.

At commit, a final TableDef equal to the base TableDef returns to the ordinary S1
index/DML commit path. It allocates no target, writes no source/stage/finalization
intent, and does not change generation, epoch, version, or placement. The same is
true when no refinement occurred.

## Late clone and durable evidence

For an effective final schema change, commit allocates exactly one S2 after DML
has closed and final schema/index truth is frozen. Ordering is:

1. replace the schema/index aggregate with the final one-table `RewriteHeap` plan;
2. sync NBSJ tag 35 `SourceBackfillIntent`;
3. sync the existing `StageResourceIntent`;
4. create S2 directly as V2/Ffinal;
5. stream the stable transaction-visible S1 view once into S2;
6. build only the final logical index inventory and validate it;
7. sync unchanged tag 34 final index truth;
8. prepare S1, S2, and one final NBSC;
9. write one canonical CORD schema COMMIT decision containing S1 and S2.

Tag 35 carries the incarnation, database transaction, TableId, source
StorageId/version/fingerprint/locator/physical TxnId, target
StorageId/version/fingerprint, base and target generation/epoch, target stage and
final locators, final-index digest, and clone-plan digest. NBSJ remains envelope
v1 and tags 1--34 retain their byte meanings. The decoder cross-validates tag 35
against the one-table replacement plan, stage intent, tag 34, and canonical
locators. It rejects aliased storages, bad version/generation/epoch steps,
truncation, digest disagreement, and mismatched final inventory.

The row visitor is bounded and streaming; it does not collect all rows. Round 38
is layout-compatible, so values keep their ColumnIds/order/physical types and are
copied directly. RowIds may change. Surviving logical indexes keep IndexIds;
dropped/recreated indexes get new IndexIds; burned allocator reservations remain
burned.

## Commit and recovery

CORD COMMIT makes both physical participants winners. S1 therefore commits even
though it is immediately replacement-retired; this preserves existing WAL and
two-phase participant semantics and requires no detach protocol. Only after both
participants are finished does recovery validate S2, record S1 retirement,
publish the prepared NBSC pointing to S2, mark CORD Complete, and record the NBSJ
Winner. Startup completes this sequence before client admission.

Participant recovery first uses the active registry where applicable, then exact
typed mutation authority. A late S2 is resolved only from tag 35 plus
`StageResourceIntent`, tag 34, the prepared NBSC reference, and the exact CORD
StorageId/physical TxnId. Existing promotion helpers handle stage/final split
states. S1 is resolved from the exact base fragment and source locator. Prepared
and already-Committed states finish idempotently; RolledBack or wrong physical
TxnId is corruption. Missing source/target resources and unexplained participants
remain hard errors. Recovery never reparses SQL, replays DML, reclones rows, or
rebuilds an index after CORD.

Before CORD, the source transaction is a loser and ordinary WAL recovery restores
the base S1 rows and dropped index; exact staged S2 resources are deleted. After
CORD, the four partial cases (both Prepared, only S1 Committed, only S2 Committed,
both Committed) converge to the same V2/F2/S2 winner. The mutated S1 uses the
existing `RetiredBySchemaRewrite` evidence, CORD horizon, and Round 25 GC path.

## Representative physical cost

The deterministic three-row fixture measured a 112,057-byte S1 resource family
before the transaction and 145,057 bytes after DROP plus UPDATE/INSERT/DELETE, a
33,000-byte increase from ordinary S1 WAL/index transaction work. No target was
allocated at that point. Finalization allocated one StorageId, made one bounded
source-view pass over three visible rows, produced a 103,152-byte staged S2
(103,321 bytes after commit/promotion), and reached a 264,801-byte whole-fixture
peak after S2 creation. The exact figures are fixture/build dependent; the
invariant is that S2 amplification is delayed until effective finalization and
there is one S1-to-S2 row stream, not a second rewrite.

## Formats and deferred work

NBSC v1, NBSM v1, Heap metadata/Page v5, NBMV v1, IndexCatalog v9, BTree v3,
Heap WAL v4/record v5, NBCO/CORD envelopes and schema reference, Protocol v1, PG
framing v3, Manifest v4, and SDK Schema Spec v1 are unchanged. NBSJ remains v1
and adds record tag 35 only. Older readers reject the unknown tag explicitly.

Public DROP-first ALTER routing, preparatory CREATE INDEX, multi-table source
backfill, layout/type changes, same-V/F physical replacement, participant detach,
savepoints, resumable migration, non-Heap placements, automatic GC, and NBSJ/CORD
compaction remain deferred.
