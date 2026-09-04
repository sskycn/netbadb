# DROP-first layout migration SQL (Round 42)

> Round 44 supersession: ordinary one-S1 INSERT/UPDATE/DELETE can now adopt
> that exact managed Heap for nullable ADD, eligible DROP, and table/column
> rename without a DROP-first prelude. This document remains authoritative for
> the distinct DROP-first route, including surviving-column SET/DROP NOT NULL
> and final CREATE INDEX.

Round 42 exposes the Round 41 late-clone row-projection foundation through
ordinary native SQL and PostgreSQL Simple/Extended Query. This is a routing
change, not generic ALTER-after-DML support.

## Supported transaction

One transaction may establish the exact Round 39 source authority, mutate S1,
then refine the final layout:

```sql
BEGIN;
DROP INDEX users_legacy_idx;
DROP INDEX users_email_idx;
UPDATE users SET email = 'filled@example.test' WHERE email IS NULL;
INSERT INTO users VALUES (4, 'old-four', 'four@example.test');
DELETE FROM users WHERE id = 2;
ALTER TABLE users DROP COLUMN legacy;
ALTER TABLE users ADD COLUMN legacy TEXT;
ALTER TABLE users ALTER COLUMN email SET NOT NULL;
ALTER TABLE users RENAME COLUMN email TO contact;
CREATE INDEX users_contact_idx ON users(contact);
COMMIT;
```

Activation remains exact: one managed Single Heap, one touched table, the same
transaction and exact table version/fingerprint, the sole S1 write participant,
and a durable strict DROP-only `InPlaceIndexDelta`. There may be no target/S2,
staged storage, table action, or cross-table work when the first refinement is
admitted. Round 44 separately permits bounded adoption after ordinary one-S1
DML; it does not use this DROP-first authority.

## Identity and projection

`ADD COLUMN` is compiled without an ID. Parse, Bind, and Describe neither write
files nor advance allocators. ALTER Execute reserves the next `ColumnId`
durably through the existing Round 41 tag-16 path. Rollback and predecision
crash preserve that burn. Multiple ADDs reserve consecutive IDs but share one
schema version, one source scan, and one S2.

`DROP COLUMN` carries the resolved exact `ColumnId`. A prepared DROP of C2 can
never bind to a later same-name C4. In `DROP C2; ADD same_name`, C4 is a new
nullable column and every surviving row receives `NULL`; C2 values are never
copied. Row projection remains target-ordered by ID: compatible survivors copy,
dropped IDs disappear, and reserved new IDs synthesize NULL.

`ADD; DROP` of the same new ID is a canonical layout no-op. It burns the ID but
allocates no S2 and publishes no layout version/generation change. The required
DROP-only index prelude retains its independent existing runtime revision.

## Phase boundary and indexes

The first successful layout refinement closes relational execution. Later
SELECT/INSERT/UPDATE/DELETE returns `MigrationDataAccessAfterRefinement`
(`25000` over PostgreSQL); the following PostgreSQL command returns `25P02`
until ROLLBACK.

Final indexes may target surviving base columns. An index on a newly added
column remains unsupported (`0A000`). Public `SET NOT NULL` or `DROP NOT NULL`
on a newly added late column also remains unsupported (`0A000`); Round 41 Policy
B is available only through the typed Core API. Indexed DROP remains `2BP01`.

## Prepared and PostgreSQL behavior

Prepared ADD/DROP retains the exact base table dependency. During the same
source refinement, that base dependency is accepted only for nullable ADD or
exact-ID DROP; all other prepared ALTER rules are unchanged. ADD reserves once
at Execute. Re-execution after publication is stale and does not burn another
ID. Exact DROP fails rather than selecting a same-name replacement.

The PostgreSQL adapter performs normal authorization, prepared-statement,
command-tag, SQLSTATE, and failed-transaction handling. It does not know about
S1/S2, source authority, projection, reservation, CORD, or recovery state.

| Failure | SQLSTATE |
| --- | --- |
| indexed DROP | `2BP01` |
| new-column CREATE INDEX | `0A000` |
| public new-column SET/DROP NOT NULL | `0A000` |
| post-refinement relational statement | `25000` |
| post-DML operation outside the Round 44 bounded adoption set | `25000` |
| command after an error in an explicit transaction | `25P02` |

## Physical and recovery boundary

An effective final layout allocates exactly one S2 at finalization, streams the
frozen transaction-visible S1 once, builds supported final indexes, and commits
S1+S2 under the existing CORD v2 decision. There is no S3. ADD Execute itself
does not allocate S2. The SQL-driven crash tests reuse the Round 41 tag-16,
projected-S2, and four-order partial-participant matrix. Recovery consumes only
durable schema/index/transaction evidence; it never parses SQL or reruns row
projection.

No persistent or wire meaning changed: Canonical TableSchema v1, NBSC/NBSM/NBSJ
v1 (tags 1--35), CORD v2, Heap metadata/Page v5, NBMV v1, IndexCatalog v9,
BTree v3, NBTR v1, Heap WAL v4/record v5, transaction status, PartitionCatalog,
LSM formats, Protocol v1, PostgreSQL framing v3, Manifest v4, SDK Schema Spec v1,
and inspection JSON remain unchanged.

Defaults, generated columns, conversions, constraints, indexes on new columns,
public new-column NOT NULL, ADD-followed-by-DML, multi-table/LSM/partitioned/
imported source migrations, online/resumable work, participant detach, automatic
GC, format compaction, and general Alembic migration support remain deferred.
