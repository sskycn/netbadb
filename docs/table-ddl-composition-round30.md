# CREATE / DROP TABLE composition — Round 30

Round 30 moves runtime `CREATE TABLE` and `DROP TABLE` into the schema/index
transaction aggregate introduced in Rounds 28–29. The supported boundary is a
managed runtime Single-Heap catalog. LSM, range-partitioned, imported/bootstrap
tables, DDL after schema materialization, SAVEPOINT composition, backfill, and
physical type conversion remain outside this round.

## Lifecycle audit

The legacy CREATE writer reserved `TableId` and `StorageId` together, created a
private Heap immediately, wrote its own NBSC intent, and enlisted that Heap as a
participant. The legacy DROP writer wrote a standalone drop intent and installed
a private absent-table overlay immediately. Both decoders and recovery paths are
retained for tags 1–15, but production statement execution no longer calls
either writer. Historical tests call test-only legacy entry points explicitly.

The new lifecycle has two phases:

1. Statement acceptance validates against the transaction overlay. CREATE
   durably reserves only a `TableId`; DROP records only a logical final-absence
   action. ALTER and index DDL update the same overlay. No `StorageId`, Heap,
   BTree, NBSC, or CORD decision is created here.
2. The first DML that requires the overlay, or COMMIT, classifies every touched
   `TableId`, allocates only final physical identities, writes one aggregate
   intent and one prepared NBSC, and uses one CORD decision.

Materialization seals schema composition. Thus `CREATE; INSERT; ALTER` and
`CREATE/ALTER; DML; DROP` still fail at the final DDL statement; Round 30 does
not weaken the Round 28 global seal.

## Logical identities and overlay

Accepted CREATE appends NBSJ tag 26 with the schema transaction, reserved
`TableId`, and next-TableId floor. The reservation permanently burns that
identity even if the process crashes or the transaction rolls back. Committed
NBSC plus tag-26 history is the allocator authority; active-table maxima are
never used to reconstruct it.

A transaction-created table owns a private `TableDef`, lineage and index
inventory:

- initial `ColumnId`s are `1..N`, and the private next-column floor is `N+1`;
- ADD COLUMN advances that private namespace without a separate durable record,
  because it cannot escape or revive without its permanently unique `TableId`;
- new-table `IndexId`s are likewise private and start at 1;
- its final committed `TableSchemaVersion` is always 1, regardless of how many
  ALTER statements preceded materialization;
- creator access is transaction-local runtime state, not a durable grant.

DROP removes the exact `(TableId, version, fingerprint)` from the overlay and
removes all of its index-name bindings. A subsequent CREATE with the same name
gets a new `TableId`; old prepared statements and authorization never rebind by
name. CREATE→DROP leaves only the burned TableId reservation. DROP→CREATE with
the same name remains two distinct logical objects.

## Final physical classification

Each base/final `TableId` receives exactly one strategy, ordered canonically by
`TableId`:

| Base | Final | Strategy | New StorageId | Physical participant |
| --- | --- | --- | ---: | ---: |
| absent | present | `CreateHeap` | 1 | 1 |
| present | absent | `DropHeap` | 0 | 0 |
| present | schema changed | `RewriteHeap` | 1 | 1 |
| present | only indexes changed | `InPlaceIndexDelta` | 0 | 1 |
| equal/no surviving object | omitted | 0 | 0 |

`CreateHeap` contains the final V1 table fragment, locator, next-column floor,
final index inventory and next-index floor. It builds exactly one Heap and only
the surviving BTrees. `DropHeap` contains the exact old fragment needed for
recovery and GC but creates no fake storage participant. Consequently
ALTER→index→DROP creates neither a replacement Heap nor a transient BTree, and
CREATE→DROP allocates no StorageId at all.

StorageIds for CreateHeap and RewriteHeap are allocated at materialization in
TableId order. A mixed transaction can therefore contain CreateHeap,
RewriteHeap, InPlaceIndexDelta and DropHeap while producing one target NBSC and
one decision whose participants are only the three physical final-work plans.

## Durable protocol and formats

NBSJ remains v1. Existing tags 1–25 are unchanged and retain their historical
decoders. Round 30 adds:

- tag 26 — logical `TableId` reservation/floor;
- tag 27 — typed table-object aggregate (`CreateHeap`, `DropHeap`,
  `RewriteHeap`, `InPlaceIndexDelta`);
- tag 28 — typed rewrite/drop predecessor retirement;
- tags 29/30 — table-object retirement GC intent/completion.

The tag-27 record binds base/target generation and epoch, action count/digest,
one final NBSC digest, canonical table plans, exact fragments, final index
inventories, and physical participant identities. No SQL is persisted or
replayed. Decoding rejects noncanonical tables, duplicate participants or
predecessors, CreateHeap without its tag-26 reservation, version/floor errors,
old/new StorageId reuse, and out-of-order retirement/GC records.

NBSC v1, NBSM v1, Heap v5, NBMV v1, Page v5, IndexCatalog v9, BTree, WAL/status,
CORD v2, Protocol v1, PG wire and Manifest v4 are unchanged.

## Commit, rollback and recovery

For an effective schema change the order is: append/sync aggregate intent;
build final targets and index work; write/sync one prepared NBSC; prepare
physical participants; append/sync one CORD decision; finish/promote physical
participants; durably retire every rewrite/drop predecessor; publish NBSC;
complete CORD; mark one winner; publish memory.

A pure DropHeap decision has a schema reference and zero physical participants.
CREATE→DROP has no aggregate physical intent, prepared NBSC, CORD decision,
generation/epoch/revision change, or cleanup scan.

Before a decision, recovery chooses the base snapshot, restores in-place index
inventories, and deletes only exact staged CreateHeap/RewriteHeap artifacts.
After a decision, recovery uses the typed plans and prepared NBSC bytes: it
finishes/promotes targets, records all predecessor retirements before opening a
target that omits them, verifies final inventories, publishes once, completes
CORD and records the winner. It never reruns CREATE logic, SQL, or row-copy
planning. Reopening a terminal historical winner validates current physical
identity but does not demand its old final index inventory after legitimate
later index-only commits.

Rollback before materialization burns only TableIds (and any committed-table
ColumnId/IndexId reservations). Rollback after materialization also burns final
StorageIds and precisely removes staged artifacts. It never reuses an identity.

## Retirement and GC

RewriteHeap and DropHeap predecessor identities join the existing retired-Heap
theorem: exact table/version/fingerprint, StorageId, locator, retirement
transaction/generation, owner proof, coordinator horizon, and fixed component
manifest. DropHeap is exposed through `inspect_retired_table_resources`;
RewriteHeap remains in replacement inspection. Explicit GC uses tags 29/30 for
Round 30 retirements, is retry-only after its durable intent, and never touches
a same-name replacement. Commit still does not trigger automatic GC.

## Verified acceptance examples

The CREATE→ALTER→rename→index test observes one TableId, V1, three final
columns, IndexId 1, one StorageId allocated only at materialization, and one
Heap after three reopens. ALTER→index→DROP observes no StorageId advance, no
replacement retirement and one DropHeap for the original storage. CREATE→DROP
observes only a burned TableId. DROP→CREATE observes different TableIds and one
new StorageId, and GC of the old locator leaves the replacement unchanged.

The mixed-table test commits one CreateHeap, one RewriteHeap, one
InPlaceIndexDelta and one DropHeap using one schema decision and exactly three
physical participants. Subprocess matrices cover TableId-only early crashes,
materialization losers, winner publication boundaries, zero-participant DROP,
same-name recreation, ALTER→DROP, and every component deletion boundary for a
tag-29/30 DropHeap GC; each outcome reopens three times.

Real client acceptance uses unmodified PostgreSQL clients and the production
frontend: psql 17.11 covers CREATE/ALTER/INDEX, CREATE/DROP elision and
ALTER/DROP plus same-name recreation; psycopg 3.2.13 covers prepared DDL and a
parameterized INSERT materialization; SQLAlchemy 2.0.52 uses native
`Table.create`, `Index.create`, and `Table.drop`; Alembic 1.16.5 uses ordinary
`create_table`, `add_column`, `create_index`, `drop_index`, and `drop_table`
Operations. Every fixture is opened catalog-only three times after shutdown.
Rust Protocol v1, Go Protocol v1, Go-to-Rust integration, and generated SDK
`--check` also pass unchanged.

All thirteen registered fuzz targets pass 1,000 runs with seed 30. The generated
schema-mutation corpus includes tags 26--30 and is copied to a temporary corpus;
no random mutation is retained in the repository. Rust 1.85 checks pass for
types, schema, schema-spec, index, storage, parser, and HIR. Compiler, Core, and
server remain transitively blocked at the pre-existing planner let-chains on
lines 892 and 904; Round 30 does not change that unrelated code.

## Deferred work

Round 30 does not claim full PostgreSQL DDL or full Alembic migrations. Deferred
work includes DDL–DML–DDL/backfill, DEFAULT and constraints, physical type
conversion, generated/identity columns, LSM/range/imported composition,
SAVEPOINT schema composition, online/cross-process DDL, automatic GC, and
NBSJ/CORD compaction. The next architectural audit should study a controlled
migration-DML phase without simply removing the materialization seal.
