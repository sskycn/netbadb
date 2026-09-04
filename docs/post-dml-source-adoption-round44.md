# Post-DML source adoption (Round 44)

Round 44 productionizes the Candidate B architecture selected in Round 43. A
transaction may use its exact ordinary one-table DML Heap as the source for a
bounded later ALTER without first dropping an index:

```sql
BEGIN;
UPDATE users SET email = 'filled@example.test' WHERE email IS NULL;
ALTER TABLE users ADD COLUMN marker TEXT;
ALTER TABLE users RENAME COLUMN email TO contact;
COMMIT;
```

The public operation set is nullable ADD COLUMN, unindexed non-primary-key DROP
COLUMN, RENAME TABLE, and RENAME COLUMN. INSERT, UPDATE, DELETE, mixed DML, and
zero-row UPDATE/DELETE qualify equally. A read of the same S1 before its write
also qualifies. Read-only, cross-table, pending-index, LSM, partitioned, and
imported/bootstrap transactions do not.

## Adoption and writer ordering

Core holds an explicit private `AdoptedSourceTransaction`. It contains the
logical composition plan and captured S1 storage, physical P1 transaction,
table version/fingerprint, canonical locator, base generation/epoch, and index
definition digest. `DatabaseTransaction::is_only_participant` exposes only the
narrow predicate needed by this proof; the participant map remains private.

At the first eligible ALTER Execute, Core validates, in order: transaction
ownership and active state; bounded operation; empty schema and pending-index
state; participants and write participants both exactly `{S1}`; committed
T/V/F; Single(S1) placement; canonical managed Heap descriptor/locator;
registry T/F/S1 identity; P1 and index inventory; writer availability and
exclusive transaction ownership; then the logical source plan and operation.
Only after all preflight succeeds does Core acquire the writer, install
`AdoptedSourceRefining`, and accept the first refinement. Parse, Bind, Describe,
and DML never acquire the writer or reserve an identity.

Contention and ordinary eligibility failures leave the writer empty, journal
bytes unchanged, and the native DML transaction active. A native caller can
remove contention and retry. Failure after durable journal work follows the
existing rollback/recovery-required contract; Core does not release the writer
and continue ordinary DML.

## Refinement, identity, and no-op

Further nullable ADD, eligible DROP, and table/column rename actions compose
into the same final V+1 overlay. ADD uses the ordinary tag-16
`CompositionColumnReservation` at Execute. Multiple ADDs reserve consecutive
ColumnIds. DROP+ADD of the same name uses distinct IDs and synthesizes NULL for
the new column. Renames retain TableId/ColumnId, and existing indexes retain
their IndexId/ColumnId identity.

The first accepted refinement closes SELECT/INSERT/UPDATE/DELETE with
`MigrationDataAccessAfterRefinement` (`25000`). Generic SET/DROP NOT NULL and
CREATE/DROP INDEX remain closed on this route. Indexed DROP returns `2BP01` and
there is no implicit CASCADE. The older Round 39/42 DROP-first route remains
separate and retains its surviving-column nullability and final-index support.

If the final `TableDef` equals the base definition, Core seals
`NoEffectiveChange`: DML commits on S1, accepted ColumnIds stay burned, and no
rewrite intent, tag35, stage intent, tag34, prepared NBSC, S2, schema version,
generation, epoch, or runtime revision is produced. This covers ADD→DROP and a
rename followed by rename-back.

## Effective finalization and recovery

Before effective finalization, Core revalidates the captured S1/P1/T/V/F,
managed locator, placement, participant sets, base generation/epoch, Heap
identity, and index-definition digest. Drift is a hard invariant error; the
source is never reselected or recaptured.

Effective finalization calls the shared schema/index materializer with
`AdoptedSourceBackfill`. Unlike `SourceBackfill`, it creates the first real
rewrite intent rather than replacing an existing DROP-first index intent. The
common path records the existing schema/index intent and tag35, stages exactly
one S2, builds a target-ordered `RowProjection`, scans the transaction-visible
S1 once, rebuilds surviving final indexes once, records tag34 and prepared
NBSC, and commits S1+S2 through CORD v2. The winner publishes one V+1/G+1/E+1
catalog revision and retires S1; there is no S3.

Pre-CORD crashes retain the base schema, roll back S1 DML, remove S2, and keep
any durable tag-16 burn. Post-CORD source-first, target-first, both-prepared,
and both-committed states converge through existing recovery. Recovery does not
know that the source began as ordinary DML and never re-runs adoption, SQL,
DML, ALTER, reservation, projection, or copying.

## PostgreSQL and compatibility

The PostgreSQL adapter is unchanged. Simple and Extended Query continue normal
authorization, prepared dependency, command-tag, SQLSTATE, and failed-
transaction handling. Extended Parse/Bind/Describe are pure; Execute is the
first adoption and ADD-allocation point. A rejected statement in an explicit
transaction is followed by `25P02` until rollback.

No persistent or wire meaning changed: Canonical TableSchema v1, NBSC/NBSM/NBSJ
v1 tags 1--35, CORD v2, Heap metadata/Page v5, NBMV v1, IndexCatalog v9, BTree
v3, NBTR v1, Heap WAL v4, transaction status, PartitionCatalog, LSM formats,
Protocol v1, PostgreSQL framing v3, Manifest v4, SDK Schema Spec v1, and
inspection formats remain unchanged.

The measured three-row production fixture retained the Round 43 observation:
ordinary adoption used S2=3, one source pass, and three copied rows, with
142,793 bytes after DML, 248,399 bytes at the observed pre-commit peak, and
248,924 final bytes. This is fixture evidence, not a general performance claim.

Defaults, generated values, type/nominal conversion, USING, constraints,
CASCADE, post-refinement DML, new-column NOT NULL/indexes, generic post-DML
SET/DROP NOT NULL, cross-table adoption, savepoints, online/resumable migration,
participant detach, automatic GC, format compaction, and general Alembic
support remain out of scope.
