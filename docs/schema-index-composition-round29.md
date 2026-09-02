# Schema and index DDL composition — Round 29

Round 29 extends the Round 28 transaction aggregate so `ALTER TABLE`, `CREATE
INDEX`, and `DROP INDEX` compose as one ordered logical change set over
runtime-created Single Heap tables. Index statements do not run the standalone
index lifecycle while composition is logical. The final per-table state chooses
exactly one of no physical work, one replacement Heap, or one in-place Heap index
transaction.

This is the schema-plus-index subset needed by ordinary Alembic add-column/create-
index and drop-index/drop-column migrations. It is not general migration or table
object composition.

## Existing index lifecycle audit

Before this round, generic `CREATE INDEX` entered Core with a typed TableId and
ColumnId but not a table version or fingerprint. Heap read IndexCatalog v9,
selected its `next_index_id`, advanced the catalog floor in the same physical
transaction, created the owned B+Tree, backfilled a stable Heap view, and appended
the active registration last. Rollback undid all of those writes, so an unpublished
ID could be selected again. `DROP INDEX` was already prepared as exact
`DropIndexTarget { table_id, index_id }`; execution never needed a durable name.

IndexCatalog v9 remains the committed physical allocator and registration
authority. It retains active and retired definitions, persistent logical IndexId,
B+Tree ownership, and `next_index_id`. There is no second allocator file. A Heap
physical transaction can already contain multiple catalog/tree mutations and is
the correct participant boundary for a final index-only table delta.

Explicit index names are globally unique in the current database runtime, not
merely table-local. The transaction overlay preserves that rule across touched and
untouched tables. The planner historically read active committed Heap access paths;
Round 29 adds transaction-private final paths after materialization without making
fake B+Tree handles. PostgreSQL reflection reads physical state, so it triggers the
same global seal as any other user relation query.

The documented legacy explicit-storage behavior is retained. A pristine
catalog-managed Single Heap transaction uses composition. A transaction that has
already observed or changed data, or a legacy/imported explicit Heap outside the
managed locator namespace, continues through the existing standalone index path.
Such a path cannot later become a schema composition transaction. Existing
registration-last, retired-page, orphan, generation, WAL, and DML maintenance
contracts are unchanged.

## Typed preparation and logical execution

`TypedCreateIndex` now carries an exact table dependency:

```text
TableId + TableSchemaVersion + SchemaFingerprint
+ stable ColumnId + IndexName + supported index form
```

Preparation remains pure and allocates no IndexId. Preparation inside a composing
transaction resolves the current schema overlay, including newly added or renamed
columns. Execute rechecks the exact dependency before reservation. A stale prepared
CREATE therefore fails without consuming an ID.

Prepared DROP retains exact TableId/IndexId. Transaction-local name resolution may
bind a newly created logical index, but the prepared result is never rebound by
name. Dropping and recreating the same name produces a new IndexId, and an older
prepared DROP cannot affect it.

Each touched table records:

```text
base TableDef / lineage / StorageId
base active index inventory and next_index_id
current TableDef
current active index inventory and next_index_id
```

CREATE validates table dependency, final ColumnId, global name uniqueness,
single-column non-unique B+Tree support, duplicate indexed column, and aggregate
bounds. It then synchronizes an NBSJ IndexId reservation and only after that adds
the definition to the logical inventory. It creates no B+Tree, reserves no
StorageId, and writes no NBSC or CORD record. DROP removes one exact active IndexId
from the current inventory and performs no physical work.

Sequential semantics follow statement order. `DROP INDEX` then `DROP COLUMN`
succeeds because the dependency check sees the current inventory. The reverse
order fails with dependent-object semantics. CREATE then DROP then DROP COLUMN is
valid; the created IndexId remains consumed. CREATE then DROP then CREATE retains
only the newest definition and burns both accepted identities. An index remains
bound to its stable ColumnId across a later column rename.

The bounds are 128 total schema/index actions, 64 touched tables, 128 ColumnId
reservations, 128 IndexId reservations, and the existing aggregate, NBSC, and CORD
limits. The total action bound cannot be bypassed by mixing action kinds.

## Durable IndexId allocation

The effective allocation rule is:

```text
IndexCatalog v9 committed next_index_id
max checked successor in retained table-scoped NBSJ reservations
= effective next_index_id
```

Reservations are keyed by TableId and bind the accepted table version/fingerprint,
IndexId, and checked successor. They are append-only post-catalog allocator
evidence. Rollback, transient CREATE/DROP, and an early crash do not rewind them.
Reopen also advances DatabaseTxnId beyond composition-only history so a later
reservation cannot collide with a prior transaction record.

All production Core allocation paths consult this effective floor. The legacy
standalone Heap API receives the effective floor rather than deriving identity from
active registrations. A replacement Heap installs the exact final definitions and
the final floor. A table with a new TableId has a distinct namespace and does not
inherit another table's reservations. Exhaustion is checked before overlay change;
IDs never wrap.

## Final classification and materialization

The first post-DDL relational execution or COMMIT freezes the aggregate. Each
touched table is classified exactly once:

```text
final TableDef == base and final indexes == base  -> NoPhysicalChange
final TableDef != base                            -> RewriteHeap
final TableDef == base and final indexes != base  -> InPlaceIndexDelta
```

`RewriteHeap` allocates one new StorageId, creates one staged final Heap, installs
the final index inventory with exact IDs/floor, and streams the source once using
stable ColumnIds. It never creates or drops an index on the predecessor Heap. The
old Heap and every old tree are retired together through replacement retention.
Transient CREATE/DROP definitions absent from the final inventory create no tree.

`InPlaceIndexDelta` retains the StorageId and uses one physical transaction for the
table. It retires base-only IndexIds in ascending order, advances the high-water,
then builds final-only definitions in ascending reserved IndexId order. The
internal exact-create API validates the supplied identity and floor but cannot
select a replacement ID. Backfill precedes registration, preserving registration-
last semantics. Several index statements on one table still yield one participant.

Materialization installs real transaction-private access paths. A following DML or
SELECT plans against the final paths: created indexes are usable, dropped paths are
absent, and surviving indexes on a replacement Heap are usable. DML maintains
provisional created indexes and ignores provisionally dropped ones. Materialization
globally seals the aggregate; later ALTER/CREATE INDEX/DROP INDEX fails with
`SchemaMutationAfterMaterialization` (`25000`).

## Commit, rollback, and recovery

The v2 aggregate intent is synchronized before any staged Heap or index delta. Its
table plans are canonical by ascending TableId and bind exact base identity,
participant StorageId, base and final index inventories and floors, target schema
identity for replacements, and optional prepared NBSC digest. No physical B+Tree
page IDs or SQL are stored.

Schema-dirty transactions prepare one NBSC and advance SchemaGeneration/NBSC epoch
once. Index-only transactions carry no schema reference and write no NBSC. All
replacement and in-place participants enter one canonical CORD v2 decision.
CORD v2 already supports both storage-only and schema-referenced decisions, so its
format is unchanged.

After the durable decision, completion is retry-only: finish all participants,
promote replacement targets, validate final IndexCatalog inventories, retain every
replacement predecessor, publish NBSC only when present, append CORD Complete,
append the NBSJ winner, and atomically replace in-memory schema/bindings/access
paths. Runtime revision advances once for any effective schema or index result.

Before a decision, rollback restores every in-place Heap transaction and removes
only exact staged replacement/prepared artifacts. Logical reservations remain. A
no-effective-change transaction has no participant, CORD, NBSC, runtime revision,
generation, version, epoch, or StorageId change. After a decision, startup uses the
typed intent, CORD decision, Heap WAL/status, and prepared NBSC when present; it
never reparses SQL or recopies rows. Mixed partial completion is finished before a
database is served.

Replay validates canonical table/IndexId ordering; unique participants, active IDs,
columns and global names; nondecreasing floors; immutable surviving IndexId
definitions; exact table version/fingerprint and StorageId for in-place deltas;
exact base/final inventories; reservation lineage and target floor; schema-reference
presence; complete replacement retirement; and legal terminal ordering.

## Identity and publication matrix

| Final transaction | Schema G | table V | NBSC epoch | StorageId | runtime revision |
| --- | --- | --- | --- | --- | --- |
| effective index-only | same | same | same | same | +1 once |
| index-only no-op | same | same | same | same | same |
| ALTER + index | +1 once | +1 for schema-dirty table | +1 once | one new ID per rewrite | +1 once |
| schema net-no-op + effective index | same | same | same | same | +1 once |

## Persistent compatibility

NBSC v1, NBSM v1, Heap metadata v5, NBMV v1, Page v5, IndexCatalog v9,
B+Tree, WAL/status, CORD v2, Protocol v1, PostgreSQL wire, and Manifest v4 are
unchanged. NBSJ retains envelope v1. Round 28 tags 1–23 retain their exact bytes and
meaning. Tag 24 records a composition IndexId reservation; tag 25 records the new
schema/index aggregate. Existing generic composition terminal and per-replacement
GC tags are reused after their state checks were generalized without reinterpreting
tag 17. Older readers reject the new tags, so downgrade after Round 29 history is
explicitly unsupported.

NBSJ and CORD remain append-only and can grow with transaction history. Their
compaction is a separate project from physical replacement GC.

## Supported boundary and next step

Supported composition is limited to runtime-created catalog-managed Single Heaps.
CREATE/DROP TABLE composition, table-DDL mixing, savepoints, defaults/backfill,
ADD NOT NULL backfill, constraints/FKs/generated columns, physical conversion,
LSM/range/imported storage, online evolution, cross-process writers, automatic GC,
and NBSJ/CORD compaction remain deferred.

If validation remains green, Round 30 should add CREATE/DROP TABLE object-lifecycle
composition: private new TableIds, logical absence, same-name recreation,
CREATE-to-DROP elision with consumed identities, ALTER-to-DROP rewrite elision,
new-table final index inventories, grants/manifest behavior, and one final NBSC/CORD
decision. It should not proceed by weakening the exact IndexId or in-place recovery
model established here.
