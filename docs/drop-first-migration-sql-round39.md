# DROP-first migration SQL vertical slice (Round 39)

Round 39 exposes the Round 38 source-participant late-clone lifecycle through
ordinary native SQL and PostgreSQL Simple/Extended Query. It adds routing only:
row streaming, index construction, tag 35, stage/finalization evidence, CORD,
recovery, replacement retirement, and GC remain Core-owned Round 38 machinery.

## Supported sequence and boundary

```sql
BEGIN;
DROP INDEX users_email_idx;
UPDATE users SET email = 'filled@example.test' WHERE email IS NULL;
ALTER TABLE users ALTER COLUMN email SET NOT NULL;
CREATE INDEX users_email_idx ON users(email);
COMMIT;
```

This is an offline, one-schema-writer, single-table migration for a managed
Single Heap. The prelude may only remove indexes. The compatible refinement set
is SET/DROP NOT NULL, RENAME COLUMN, and RENAME TABLE. ADD/DROP COLUMN, physical
or nominal type conversion, table creation/removal, cross-table work, LSM,
partitioned/imported storage, defaults, constraints, generated columns,
savepoints, online execution, and resumable migration remain unsupported.

There is no SQL look-ahead, buffered sequence recognition, migration flag, or
frontend orchestration. DROP and DML retain their existing behavior. The
transition is considered only when the exact typed ALTER reaches Core Execute.

## Execute-time routing authority

`Database::compose_heap_table_schema_in` calls the crate-private
`try_apply_source_backfill_refinement`. The predicate requires:

- an Active database transaction whose schema writer and composition intent
  belong to that exact transaction;
- `MaterializedIndex`, proving an earlier relation execution sealed the
  drop-only prelude on S1;
- exactly one touched TableId, no table action or transaction-created table,
  and the ALTER's exact V1/F1 dependency;
- the current binding and captured source to be the same managed Single Heap;
- one `InPlaceIndexDelta`, S1 as the sole write participant, no target snapshot,
  no prepared reference, and no staged S2;
- final prelude indexes to be a strict subset of the base inventory, with no
  create or replacement before refinement; and
- an allowed layout-compatible ALTER operation.

An ineligible operation falls through to the established staged-backfill or
sealed/error behavior. The PostgreSQL adapter remains unaware of Candidate B.
Authorization still runs independently for DROP, ALTER, and CREATE before Core
execution.

The preparatory DROP is an exact `(TableId, IndexId)` physical delta in S1's
`StorageTransaction`. A nullability refinement additionally proves that every
incompatible base index on the column is absent from final logical truth and is
listed as an exact S1 publication drop. The old Round 32--36
`backfill_indexed_columns` guard is unchanged because that path retargets S2;
Candidate B retires S1 instead.

## Source validation and phase closure

SET NOT NULL scans the current S1 transaction view. It observes UPDATE and
INSERT and excludes DELETE. Remaining NULL produces `NotNullViolation` / PG
`23502`, leaves native state `SourceBackfillOpen`, allocates no S2, and permits
native DML repair followed by retry. PostgreSQL follows its normal failed-
transaction rule, reporting `25P02` until ROLLBACK.

The first successful refinement moves to `SourceRefining`, assigns V2/F2 once,
and closes SELECT/INSERT/UPDATE/DELETE (`25000`). Further compatible ALTERs
update the same provisional V2/Ffinal. Final CREATE INDEX reserves a fresh
IndexId and moves to `SourceIndexFinalizing`; it changes logical final inventory
only and never builds Inew on S1. A prepared CREATE bound to V1/F1 is stale
before reservation. A prepared DROP remains bound to Iold and cannot delete a
same-name Inew. Final DROP operations are logical-only.

If the final TableDef equals the base, the transaction returns to the ordinary
S1 index/DML commit path: no StorageId, tag 35, stage intent, NBSC publication,
version, generation, epoch, or runtime-revision change occurs. With no ALTER,
the route is never entered and `DROP + UPDATE + COMMIT` has the same result.

## Late clone, commit, recovery, and cost

For an effective change, COMMIT freezes final truth, allocates one S2, records
tag 35 and `StageResourceIntent`, streams the S1 transaction view once directly
into V2/Ffinal/S2, builds only final indexes, records tag 34, and prepares one
NBSC plus the S1 and S2 physical participants. One CORD COMMIT decision makes
both participants winners. S1 commits before replacement retirement; S2 is the
only published placement.

The SQL-driven crash tests now use the public dispatcher for the complete
DROP/UPDATE/ALTER/CREATE sequence. Before CORD, S1 rolls back and staged S2 is
removed. After CORD, every prepared/partial-commit order finishes both winners,
retires S1, and publishes S2 before client admission. Recovery does not invoke
the parser, SQL executor, ALTER transform, row clone, or index builder.

The deterministic three-visible-row fixture retains the Round 38 measurements:
S1 grew from 112,057 to 145,057 bytes during DROP+DML; staged S2 was 103,152
bytes (103,321 after promotion); peak fixture bytes were 264,801; one target
StorageId, one source-view pass, and three copied rows were observed. These are
fixture/build measurements, not performance guarantees. Compared with the
Round 36 private-S2 path, S2's peak duration is shorter, while S1 carries the
pre-finalization WAL growth.

## PostgreSQL and prepared evidence

`scripts/test-drop-first-migration-sql.py` uses unmodified psql 17.11 and the
required `/opt/local/lib/icu/lib`. It covers replacement and no-replacement
commits, unchanged no-ALTER behavior, schema net-no-op, partial backfill,
post-refinement DML rejection, rollback, command tags, catalog nullability, and
three catalog-only reopens per case. Core/server tests cover Parse/Bind/Describe
purity, prepared ALTER execution against current S1 data, stale CREATE before
IndexId reservation, exact DROP identity, planner use of the rebuilt index,
and burned IndexId but unburned StorageId on pre-COMMIT rollback.

psycopg, SQLAlchemy, and Alembic were unavailable and were not installed.
Alembic remains unverified; this bounded ordering merely matches a common
DROP-index/backfill/nullability/recreate recipe more closely.

## Formats

No format changed. Canonical TableSchema v1, NBSC/NBSM/NBSJ v1, NBSJ tags
1--35, NBPC v1, CORD v2 schema commit, Heap metadata/Page v5, NBMV v1,
IndexCatalog v9, BTree v3, NBTR v1, Heap WAL v4/record v5, LSM formats, native
Protocol v1, PostgreSQL framing v3, Manifest v4, and SDK Schema Spec v1 remain
unchanged. NBSJ and CORD compaction remain separate work.

Round 40 audited layout-changing late-clone projection and Round 41 implemented
its Core-only foundation. [Round 42](drop-first-layout-migration-sql-round42.md)
now admits nullable ADD and exact-ID DROP only behind this document's exact
DROP-first source authority. The negative boundary moves to post-DML ADD/DROP
without that authority, which remains SQLSTATE `25000`; all other Round 39
activation and positive behavior remains unchanged.
