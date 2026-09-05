# Post-DML source nullability (Round 46)

Round 46 productionizes Round 45 Candidate A. One active transaction whose
only participant and write participant is an exact managed Single Heap S1 may
follow ordinary INSERT/UPDATE/DELETE with `SET NOT NULL` or `DROP NOT NULL` on
a surviving base `ColumnId`. Existing indexes on that column are supported.

```sql
BEGIN;
UPDATE users SET email = 'filled@example.test' WHERE email IS NULL;
ALTER TABLE users ALTER COLUMN email SET NOT NULL;
COMMIT;
```

The adopted-source allowlist is nullable ADD COLUMN, unindexed non-primary-key
DROP COLUMN, RENAME TABLE, RENAME COLUMN, and surviving-base-column SET/DROP
NOT NULL. Nullability of a column added in the same adopted overlay remains
unsupported. CREATE/DROP INDEX after adoption and all relational execution
after the first successful refinement also remain closed.

## Source identity and validation ordering

`apply_adopted_source_nullability` is the single production-private Candidate A
state machine. For the first SET it checks transaction ownership and Active
state, operation and empty schema/index state, exact `{S1}` participant and
write-participant sets, committed T/V/F, managed Single-Heap placement,
locator/registry/P1/index digest, writer availability and exclusive transaction
ownership, and captures `AdoptedSourceTransaction`. It then proves the target
`ColumnId` exists in the captured base table and calls the shared
`validate_source_view_not_null` against a transaction read view of S1. Only
after that scan succeeds does it acquire `schema_writer`, install
`AdoptedSourceRefining`, and apply the overlay mutation.

The validator scans the exact `ColumnId`; it observes the transaction's UPDATE
and INSERT and excludes its DELETE. A remaining visible NULL returns
`NotNullViolation(ColumnId)`. A first native failure leaves the transaction
Active, writer and composition empty, journal bytes and StorageId high-water
unchanged, so the caller can repair S1 and retry. PostgreSQL maps the same
failure to `23502` and applies its normal failed-transaction rule: subsequent
commands return `25P02` until ROLLBACK.

DROP uses the same full adoption preflight and surviving-base proof but performs
no row-validation scan. For a subsequent SET/DROP, the existing adopted state
must match the current overlay dependency and the ID must still occur in its
captured base table. SET scans that same frozen S1; failure retains the adopted
state and writer, keeps DML closed, permits compatible later ALTER under native
transaction semantics, and remains rollbackable. It never selects or captures
a new source.

## Indexes, publication, and no-ops

An S1 index stays frozen under the source schema and is never relabeled as the
final index. An effective change creates one S2 and rebuilds a fresh physical
BTree from the final `TableDef`, including final nullable metadata. Logical
`IndexId`, `ColumnId`, and index name remain unchanged. Reopening S2 validates
the persisted `IndexSpec` against the final table.

Effective SET and DROP each publish the same TableId with table version,
schema generation, catalog epoch, and runtime catalog revision incremented
exactly once. They allocate exactly one target StorageId, perform one final
transaction-visible `RowProjection` pass, and create no S3. SET performs one
additional validation scan; DROP performs zero. A combined UPDATE/INSERT/DELETE
fixture copies exactly its three visible winner rows and preserves indexed
lookups.

The fixed three-row fixture observed SET at 68,024 bytes after DML, 115,741 at
the pre-commit peak, and 116,248 after close; DROP observed 59,784, 107,501,
and 108,008 bytes respectively. Both used S2=`StorageId(3)`, one source-copy
pass, and three copied rows. SET recorded one validation scan and DROP zero.
The Round 45 Candidate A SET observation was exactly the same. Byte counts are
diagnostic fixture observations, not stable performance requirements.

SET→DROP from a nullable base and DROP→SET from a NOT NULL base are canonical
no-ops. DML commits directly on S1; the same physical index handle remains;
there is no S2, schema/index rewrite intent, stage/finalization intent, prepared
NBSC, version/generation/epoch/runtime publication, or identity allocation.

## Prepared execution, closure, and recovery

Prepared SET/DROP retain exact TableId, table version, fingerprint, and
ColumnId. Parse, Bind, and Describe are pure; Execute is the first validation
and adoption point. An old prepared statement becomes stale after an adopted
overlay changes its dependency. Preparing again against the transaction-visible
overlay resolves the surviving ID and succeeds; no name rebinding exception is
introduced.

After any successful adopted refinement, SELECT/INSERT/UPDATE/DELETE return
`MigrationDataAccessAfterRefinement` (`25000` through PostgreSQL). CREATE/DROP
INDEX also remain transaction-state errors. Rename→SET, SET→rename, nullable
ADD→surviving SET, and eligible DROP→surviving SET compose into the same one-S2
final projection.

The `post-dml-not-null-validation-complete` crash boundary lies after first SET
validation and before writer installation. Its DML is an ordinary loser: base
schema/S1 remain, no schema intent or S2 exists, and allocators do not advance.
Existing pre-CORD hooks retain that loser theorem. After the CORD decision,
both-prepared, source-first, target-first, and both-committed participant states
converge to the same S2 over repeated opens. Recovery consumes existing durable
rewrite evidence; it never reruns validation, DML, ALTER, source adoption,
projection, or SQL parsing.

## Compatibility and exclusions

No PostgreSQL production adapter code and no persistent or wire format changed:
Canonical TableSchema v1, NBSC/NBSM/NBSJ v1 tags 1–35, CORD v2, Heap metadata
and Page, NBMV, Heap WAL, transaction status, IndexCatalog v9, BTree v3,
PartitionCatalog, LSM formats, Protocol v1, PostgreSQL framing v3, Manifest v4,
SDK Schema Spec v1, and inspection formats are unchanged. Nullability allocates
no ColumnId or IndexId and adds no journal tag or recovery branch.

Still excluded are new-column SET/DROP NOT NULL, adopted CREATE/DROP INDEX,
post-refinement DML, defaults, generated expressions, type conversion/USING,
constraints/CASCADE, cross-table, LSM, partitioned, imported/bootstrap,
savepoints, online/resumable migration, participant detach, automatic GC, and
format compaction. The Round 39/42 DROP-first route and ordinary pristine
schema-first composition retain their distinct existing rules.
