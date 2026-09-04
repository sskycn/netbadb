# Staged indexed-nullability SQL vertical slice — Round 36

Round 36 exposes the Round 35 staged-index evacuation lifecycle through ordinary
SQL only when the transaction already owns the one private managed Single-Heap
backfill target. It does not support DROP-first migration cloning.

## Supported ordering

```text
DDL compose
→ DML backfill*
→ preparatory DROP INDEX*
→ compatible ALTER+
→ final CREATE/DROP INDEX*
→ COMMIT
```

For example:

```sql
BEGIN;
ALTER TABLE users ADD COLUMN migration_marker TEXT;
UPDATE users SET email = 'filled@example.test' WHERE email IS NULL;
UPDATE users SET migration_marker = 'done';
DROP INDEX users_email_idx;
ALTER TABLE users ALTER COLUMN email SET NOT NULL;
CREATE INDEX users_email_idx ON users(email);
COMMIT;
```

The first relation execution materializes private S2 and enters `BackfillOpen`.
Executing the exact prepared DROP then physically retires Iold in S2 and enters
`IndexEvacuating`. From that statement onward every SELECT/INSERT/UPDATE/DELETE
fails with transaction-state error `25000`; all data changes must precede DROP.

## Exact execution-time route

`PreparedDdlStatement::DropIndex` first applies the existing exact target and
`IF EXISTS` rules. The new predicate then requires all of the following:

- active transaction in `BackfillOpen` or `IndexEvacuating`;
- exactly one logical migration target and the same target `TableId`;
- a distinct private staged StorageId backed by a managed Heap;
- exactly one staged resource plan and matching transaction binding;
- the exact `IndexId` active in both logical and physical staged inventories.

Only then does SQL call the existing `evacuate_staged_backfill_index_in`
primitive. Names are never re-resolved during execution. A stale prepared DROP
cannot target a same-name replacement, and `DROP INDEX IF EXISTS` returning
`Unchanged` does not change phase.

Autocommit DROP, ordinary explicit-transaction DROP, pre-backfill composition,
and logical DROP in `IndexFinalizing` retain their Round 29/30/33 behavior.
A cross-table DROP during staged evacuation is rejected as cross-table backfill
access and cannot enlist another participant.

## Refinement and final indexes

`IndexEvacuating` and `RefiningAfterEvacuation` reuse the Round 35 physical
proof. Before accepting nullability or rename refinement, Core validates the
actual S2 IndexCatalog and every surviving BTree `IndexSpec` against the
candidate final `TableDef`. `SET NOT NULL` also scans the transaction-visible S2
rows, including all preceding INSERT/UPDATE/DELETE effects. Remaining NULLs
produce `23502` in PostgreSQL; no old index is rebuilt in S2.

The first successful compatible ALTER enters `RefiningAfterEvacuation`.
Layout-changing ADD/DROP/type operations remain rejected. A replacement
`CREATE INDEX` reuses Round 33 durable reservation and enters `IndexFinalizing`;
Inew has a fresh IndexId and its BTree is built only during commit, after the
Heap and owner are retargeted to the final schema. Commit without replacement
is valid after refinement; commit directly from `IndexEvacuating` is rejected.

## Durability and compatibility

StageIndexInventory remains exact S2 physical truth and FinalIndexInventory
remains logical final truth. Finalization diffs those inventories, so the SQL
evacuation is never dropped twice. Round 35 tag 34, StageResourceIntent, one
prepared NBSC, and one CORD v2 decision are reused. Recovery does not know that
the evacuation originated in SQL and never replays SQL or rebuilds after CORD.

No persistent or wire format changes: TableSchema v1; NBSC/NBSM/NBSJ/NBSA v1
with NBSJ tags 1–34; NBPC v1; NBCO/CORD v1/v2; Heap/Page v5; NBMV v1;
IndexCatalog v9; BTree v3; NBTR v1; Heap WAL v4/current record v5; NBTS v1;
LSM manifest v2/WAL v1/SSTable v2; native protocol v1; PostgreSQL wire v3;
SDK Schema Spec v1; deployment manifest v4.

## Authorization, prepared, and Extended Query

Existing authorization remains ahead of execution: DROP, ALTER, and CREATE each
require their own normal schema/table checks. PostgreSQL maps denial to `42501`
and later commands in a failed transaction to `25P02`. Parse, Bind, and Describe
remain side-effect-free; physical evacuation occurs only at Execute and uses the
Execute-time transaction phase.

## External acceptance

The real `/opt/local/lib/pgsql/bin/psql` 17.11 acceptance covers the replacement
sequence, no-replacement commit, direct DROP-first `25000`, DML-after-evacuation
`25000`, partial-backfill `23502` followed by `25P02`, commit-before-refinement
`25000`, and three catalog-only reopens for each successful final state.
psycopg, SQLAlchemy, and Alembic were unavailable in the test environment.

## Explicit limitation

This still fails and rolls back with the original schema, rows, and Iold:

```sql
BEGIN;
DROP INDEX users_email_idx;
UPDATE users SET email = 'filled@example.test' WHERE email IS NULL;
ALTER TABLE users ALTER COLUMN email SET NOT NULL;
ROLLBACK;
```

Many migration tools, including common Alembic recipes, naturally emit this
DROP-first ordering. Round 36 must not be described as general Alembic indexed-
nullability support. MigrationCloneHeap, same-version/fingerprint physical
replacement, late S1 clone, and selective participant detach remain deferred.
