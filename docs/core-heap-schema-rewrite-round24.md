# Core Heap schema rewrite foundation — Round 24

Round 24 implements a frontend-neutral, transactional schema rewrite for one
runtime-created `Single` Heap. It deliberately adds no SQL `ALTER TABLE` grammar.
Every supported logical change keeps the `TableId`, creates a new `StorageId`,
rewrites current visible rows under a complete target `TableDef`, and retains the
old physical Heap as replacement history.

## Typed boundary and scope

`AlterTableSpec` binds an exact
`(TableId, TableSchemaVersion, SchemaFingerprint)` and one typed operation:

- rename the table;
- rename a column by `ColumnId`;
- add one nullable column with an explicit semantic/physical type;
- drop one non-primary-key, non-indexed column by `ColumnId`;
- set or drop `NOT NULL`;
- change only the nominal type while preserving the physical type.

`resolve_alter_table` and `resolve_alter_column` are side-effect-free helpers.
Execution never re-resolves a name. Missing/stale identities, no-ops, duplicate
names, a physical conversion, indexed/primary-key drop and arithmetic exhaustion
fail before durable reservation. Imported/bootstrap Heaps, LSM and range placement
are unsupported. Defaults, ADD NOT NULL, column position/reorder, physical type
conversion, multiple mutations and SQL ALTER are not representable.

The transaction must be pristine: no read snapshot, physical participant, prior
DML, schema mutation or index mutation. Exclusive schema-writer admission also
requires that no other database transaction handle can retain the source. The
source is consequently a stable committed view. Once the rewrite is staged, query
and DML against the target schema are allowed in the same transaction.

## Identity and target construction

Every admitted operation durably reserves a new `StorageId`. ADD also reserves the
current table-scoped `next_column_id`; the target appends that column in declaration
order. Reservations survive rollback and crash, so gaps are expected. No new
`TableId` is allocated. Surviving columns retain their `ColumnId`; a dropped ID is
never filled by a later ADD.

A successful rewrite checks and advances exactly:

```text
TableId              T -> T
TableSchemaVersion   V -> V + 1
SchemaGeneration     G -> G + 1
NBSC epoch            E -> E + 1
StorageId            S1 -> fresh S2
SchemaFingerprint    F1 -> canonical F2
```

The target locator remains the runtime-owned rule
`<catalog>.resources-<incarnation>/storage/<S2>.heap`. The complete target NBSC is
prepared separately; NBSJ carries bounded one-table base/target fragments as
recovery evidence, not as another active catalog.

## Streaming row rewrite and MVCC

The old Heap is never modified or opened with the target schema. Core obtains one
committed source read view and visits visible logical rows through the existing
Heap visitor. It owns at most one decoded source row and one transformed target row
at a time; there is no whole-table `Vec` and no raw page/MVCC clone.

Mapping is by target `ColumnId`, not vector position. Surviving values come from
the source declaration position for that ID, ADD supplies an explicit database
`NULL`, and DROP has no target field. `SET NOT NULL` rejects the first visible
`NULL`. Each transformed row is encoded and inserted as a complete target row with
new target-local RowIds. Only rows visible in the stable committed read view are
copied: dead update/delete history, old version-chain pointers and obsolete BTree
entries remain entirely in S1.

The implementation does not force target data/index pages after the copy. S2's
physical prepare synchronizes its WAL and transaction status before the CORD
decision. A winner that crashes immediately after that decision is therefore
recovered from the already-prepared S2 participant; it never rescans S1. Tests
also compare every storage-owned S1 bundle member byte-for-byte across failed
rewrite rollback and across active S2 `VACUUM`/`ANALYZE` maintenance.

## Indexes and statistics

Before row copy, storage snapshots every active logical index as
`(IndexId, optional IndexName, ColumnId)` plus the durable `next_index_id` high-water.
The target installs empty new B+Trees with exactly those logical identities. It
validates nonzero/sparse IDs, unique ID/name/column identity, target column presence
and the high-water before publishing any definition. Row insertion then maintains
the target trees normally, producing new RowIds and physical BTree handles.

Retired index ownership and old physical handles are S1 history and are not copied.
The target keeps the allocator high-water, so a post-rewrite index cannot reuse a
dropped IndexId. Table and index ANALYZE snapshots are reset to `None`; statistics
are physical observations and are not valid for the replacement Heap.

## Private overlay and prepared statements

The transaction overlay replaces the table schema, lineage, placement and access
paths with S2 while the committed database remains on S1. Preparation, planning,
query and DML in that transaction use the target fingerprint/version and rebuilt
index inventory. An old prepared statement for T is stale immediately in the
overlay; unrelated dependencies remain valid. Target DML is part of S2 and commits
or rolls back with the schema decision.

Publication is one synchronous ownership move: registry S1→S2, binding T→S2,
committed schema and runtime revision change together after durable publication.
No API callback can observe a mixed state.

## Journal, coordinator and commit order

NBSJ retains envelope version 1 and adds tags 11–15. A rewrite reservation contains
transaction, T, S2, optional ADD ColumnId, base generation and epoch. The intent
contains a typed operation, target NBSC digest, and exact base/target one-table NBSC
v1 fragments. Replacement-retained, loser and winner records close the state
machine. Replay mechanically reconstructs the requested target and validates
identity, placement, fingerprint, floors, checked `+1`, ordering and terminal state.

CORD remains version 2. S2 is an ordinary physical participant and the schema
reference binds the prepared NBSC digest/target epoch. Live winner order is:

1. validate scope, exact identities, operation and exclusive pristine admission;
2. synchronize NBSJ reservation and intent;
3. create S2 privately, install indexes, stream rows, expose the target overlay;
4. prepare S2 and synchronize the prepared NBSC;
5. synchronize the CORD schema commit decision;
6. commit S2, close its live handle and promote every exact staged component;
7. reopen/recover S2 from the decision, synchronize the final Heap and validate S1;
8. synchronize replacement-retirement evidence;
9. publish NBSC then NBSM;
10. synchronize Coordinator `Complete`, resolve the rewrite winner and remove
    prepared artifacts;
11. atomically replace the in-memory registry/binding/schema and release admission.

After the durable decision every failure is finalize/recovery-only; no path chooses
S1 as the final active storage. Recovery never copies rows again.

## Rollback and recovery

Before a decision, rollback durably aborts S2, removes only exact staged/prepared
artifacts, records a rewrite loser, discards the overlay and leaves S1 byte-for-byte
active. S2/ADD identities stay burned. A reservation-only crash is also made a
terminal loser during open.

Startup reads NBSJ and CORD before strict catalog publication. A loser cleans known
private S2 artifacts and verifies S1 remains active. A winner finishes partial
promotion, recovers prepared S2 from the coordinator decision, records missing
replacement retirement, publishes the exact prepared NBSC, completes the decision
and resolves the winner. Repeated open is idempotent and never re-runs the copy.
Active NBSC may contain T on S2 while retained rewrite evidence contains the same T
on S1; the StorageIds must differ and all exact physical identities must validate.

## Replacement retirement and compatibility

Replacement retirement is not DROP retirement: T remains active, its grants and
logical identity survive, and only `(V,F,S)` advances. It has a separate inspection
token, `ReplacementRetiredHeap`. S1 is retained physically and excluded from active
catalog inspection. Round 24 intentionally returns
`ReplacementRetirementGcUnsupported` for an exact token; it does not reuse the
Round 22 DROP GC proof because that proof assumes the logical TableId is retired.

NBSC v1, NBSM v1, Heap metadata v5, `NBMV` row headers v1, Page v5, IndexCatalog
v9, BTree v1/v2/v3, WAL, transaction status, CORD v2, Protocol v1 and PostgreSQL
wire framing are unchanged. NBSJ remains v1 but older binaries reject tags 11–15,
so downgrade after a rewrite is unsupported. Native protocol, PostgreSQL, generated
SDK and ORM clients gain no ALTER syntax in this round; their existing exact
fingerprint/prepared invalidation behavior applies after an embedded Core rewrite.

The existing bounded `schema_mutation_decode` target covers reviewed reservation,
intent, loser, winner and truncated rewrite histories. Subprocess tests cover
reservation, staging, copy, index, prepare, decision, promotion, retirement,
NBSC, Coordinator Complete, journal winner resolution and in-memory publication
windows, with three catalog-only reopens per outcome. The immediate post-decision
case additionally proves that S2 still contains a prepared physical transaction
which startup must recover.

Acceptance regressions reopen S1 independently using its retained base `TableDef`
and verify its current rows and logical indexes, while active catalog inspection
contains only S2. They also exercise point/range index scans, index nested-loop and
hash joins after rewrite, reset then repopulate target statistics, reject stale
manifest/SDK schema expectations, and prove unsupported replacement GC performs
zero filesystem mutation.

## Deferred work

Replacement-retired Heap GC integration is the next physical-lifecycle step. Also
deferred are SQL/PG ALTER syntax, protocol ALTER requests, LSM/range rewrite,
online/concurrent rewrite, defaults, ADD NOT NULL, physical conversion, column
reorder, multiple ALTER actions, journal/coordinator compaction and cross-process
writer exclusion.
