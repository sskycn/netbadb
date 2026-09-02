# Round 26: generic SQL ALTER TABLE

Round 26 is a thin parser-to-Core vertical slice over the Round 24/25 Heap
schema-rewrite lifecycle. It adds no storage algorithm, durable identity
allocator, row copier, index rebuilder, recovery state machine, or garbage
collector to the SQL, compiler, server, or PostgreSQL layers.

Round 28 subsequently changed the Core execution target from one eager rewrite
to ALTER-only logical composition. The grammar and typed single-statement
contracts documented here remain unchanged; see
[Round 28](core-multi-alter-round28.md) for transaction behavior.

## Existing DDL pipeline audit

Before this change, CREATE TABLE, DROP TABLE, CREATE INDEX, and DROP INDEX used:

```text
generic tokenizer/parser AST
  -> typed HIR name/type resolution
  -> CompiledDdlStatement
  -> PreparedDdlStatement
  -> DatabaseSession
  -> native Protocol v1 or PostgreSQL Simple/Extended Query
  -> Core lifecycle API
```

ALTER follows the same path. PostgreSQL Parse stores the already compiled exact
DDL payload; Bind copies it with zero parameters; Describe reports an empty
ParameterDescription and NoData; Execute is the first mutating step; Sync restores
protocol synchronization. Native execution returns AffectedRows(0). PostgreSQL
returns CommandComplete `ALTER TABLE` for every supported operation.

## Supported grammar

The generic parser accepts one unquoted, unqualified, case-sensitive project
identifier and exactly one action:

```sql
ALTER TABLE t RENAME TO new_name;
ALTER TABLE t RENAME COLUMN old_name TO new_name;
ALTER TABLE t RENAME old_name TO new_name;
ALTER TABLE t ADD COLUMN c BIGINT;
ALTER TABLE t ADD COLUMN c BIGINT NULL;
ALTER TABLE t DROP COLUMN c;
ALTER TABLE t ALTER COLUMN c SET NOT NULL;
ALTER TABLE t ALTER COLUMN c DROP NOT NULL;
```

The optional-COLUMN rename spelling is a harmless PostgreSQL grammar alias. Real
Alembic 1.16.5 generated `ALTER TABLE projects RENAME name TO title`; both forms
lower to the same AST/HIR operation.

ADD shares CREATE TABLE's declaration resolver. BOOLEAN/BOOL maps to Bool;
BIGINT/INT64/INT8 to Int64; TEXT/VARCHAR to unbounded Text. Native generic SQL
also accepts UINT64. The PostgreSQL adapter rejects UINT64 before Core execution.
ADD is always nullable and allocates no ColumnId during parse, HIR, compilation,
PG Parse, Bind, or Describe.

## Syntax AST and exact HIR

`AlterTableStatement` contains only the table identifier, one action, and source
spans. Existing-column operations preserve spans for the old column; rename
preserves the new name; ADD preserves the type token. There is no TableId,
ColumnId, StorageId, schema version, fingerprint, PG OID, page, or row identity in
the AST.

HIR resolves the transaction-visible SchemaView to:

```text
TableId + TableSchemaVersion + SchemaFingerprint
```

Rename/drop/nullability operations also resolve the source name to a stable
ColumnId. ADD retains only `(name, SemanticType, nullable=true)`; the Round 24
durable allocator remains the only source of a new ColumnId. Missing names,
duplicate ADD/rename targets, and unknown types fail with their exact source span
before any writer admission or allocation.

## Compiler, access, and authorization

`CompiledDdlStatement::AlterTable` carries `TypedAlterTable`. Its
`StatementAccess` is `schema_write=true`, records the exact target TableId in
`schema_tables`, and has empty row read/write lists. Network execution therefore
requires `schema_admin` but no DML grant. ALTER never modifies grants or manifests;
the stable TableId means an existing external TableId grant remains applicable.
Same-transaction post-ALTER DML still requires a real DML grant, independently of
schema administration.

The Core conversion remains pure:

```text
TypedAlterTable -> AlterTableSpec -> compose_heap_table_schema_in
```

It maps the six SQL operations to RenameTable, RenameColumn,
AddNullableColumn, DropColumn, SetNotNull, and DropNotNull. It never exposes the
Core-only ChangeNominalType operation through SQL.

## Prepared identity and execution-time truth

Prepared ALTER never resolves its table name or source column name again.
DROP+same-name CREATE produces a new TableId, so the old ALTER fails as undefined
instead of retargeting. Another committed ALTER changes the table version and
fingerprint, so the old ALTER fails stale even though TableId is unchanged.
Rebinding an already successful Extended prepared ALTER likewise fails stale.

Index-only catalog revision and ordinary committed DML do not change the exact
schema dependency and therefore do not stale ALTER. Core snapshots the current
index catalog at Execute. A DROP COLUMN prepared while unindexed fails with
dependent_objects_still_exist if an index is created before Execute. Current rows,
including DML committed after preparation, are copied by the rewrite.

Parse/preparation does not make a transaction non-pristine. At Execute, the first
ALTER admits the composition writer; later ALTER statements resolve sequentially
against its overlay. The first executed user SELECT/DML, or COMMIT, materializes
and globally seals the aggregate. Later schema/index DDL is rejected. Table/index
mixing remains unsupported; the frontend never inserts implicit commits or splits
operations.

## Transactions and lifecycle

Session autocommit uses the shared DatabaseSession begin/execute/commit path.
Explicit transactions see the private replacement schema immediately. ADD writes
complete target rows with NULL in the new field for every old row; it does not rely
on missing-tail decoding. New DML uses the target schema and rebuilt indexes.

Rollback removes the private replacement and target DML while preserving the old
active schema/storage/generation/version. Reserved StorageId and ADD ColumnId are
not reused. Commit advances generation and version exactly once and installs a new
StorageId even for rename-only changes. TableId, surviving ColumnIds, logical
IndexIds/names, and grants remain stable. SET NOT NULL scans current visible rows
and returns a not-null violation without publishing the target if any value is
NULL. DROP of an indexed or primary-key column is rejected; no dependent index is
automatically removed.

The old Heap is retained as `RetiredBySchemaRewrite`. SQL does not invoke GC.
Round 25's explicit replacement-retired GC consumes the same durable evidence.
Startup recovery uses existing typed NBSJ/CORD/NBSC evidence and never stores or
reparses SQL.

## PostgreSQL errors

| Condition | SQLSTATE |
| --- | --- |
| malformed syntax | 42601 |
| missing table | 42P01 |
| rename table conflict | 42P07 |
| missing column | 42703 |
| duplicate column | 42701 |
| insufficient privilege | 42501 |
| SET NOT NULL row validation | 23502 |
| indexed/PK DROP dependency | 2BP01 |
| unsupported ALTER or DDL combination | 0A000 |
| schema writer busy | 55P03 |
| stale dependency / transaction not pristine | 25000 |
| command after explicit transaction failure | 25P02 |

Syntax, unsupported forms, permission denial, missing/duplicate names, stale exact
identity, transaction admission, and known DROP dependencies fail before a durable
rewrite reservation. Core remains the final authority for every check.

## Unsupported grammar and placement

Rejected rather than ignored: ALTER TABLE IF EXISTS/ONLY; qualified or quoted
table names; ADD NOT NULL, DEFAULT, PRIMARY KEY, UNIQUE, CHECK, REFERENCES, or
constraints; VARCHAR(n)/CHAR(n); DROP IF EXISTS/CASCADE/RESTRICT; ALTER TYPE,
SET DATA TYPE, USING, SET/DROP DEFAULT; ADD/DROP/RENAME CONSTRAINT; owner/schema/
tablespace/partition/enable/disable operations; comma-separated/multiple actions.

Core restrictions remain: physical or nominal SQL type alteration, indexed/PK
column DROP, imported/bootstrap Heap, LSM, range partition, online ALTER, multiple
schema mutations per transaction, automatic GC, and full multi-operation Alembic
migration transactions.

## Real client evidence

The dedicated fresh fixture uses psql 17.11, psycopg 3.2.13, SQLAlchemy 2.0.52,
and Alembic 1.16.5 without a custom dialect or protocol workaround.

- psql executes all six actions, private post-ADD SELECT/INSERT, rollback, and
  committed final reads.
- psycopg executes ADD with `prepare=True`, performs parameterized DML in the same
  transaction, and observes committed old/new rows.
- SQLAlchemy uses `exec_driver_sql` only as PostgreSQL transport and Inspector
  observes committed table names, columns, nullability, and indexes.
- Alembic executes six individually committed Operations. Its captured SQL is:

```text
ALTER TABLE projects ADD COLUMN active BOOLEAN
ALTER TABLE projects ALTER COLUMN name SET NOT NULL
ALTER TABLE projects ALTER COLUMN name DROP NOT NULL
ALTER TABLE projects RENAME name TO title
ALTER TABLE projects DROP COLUMN active
ALTER TABLE projects RENAME TO work
```

This is bounded single-operation Alembic apply, not support for a normal
multi-operation migration transaction.

## Persistence and compatibility

Round 26 introduces no persistent or wire format changes: NBSC v1, NBSM v1,
NBSJ v1 tags 1-15, Heap metadata v5, Page v5, IndexCatalog v9, BTree v1/v2/v3,
WAL/status, CORD v2, native Protocol v1, and PostgreSQL framing are unchanged.
SQL ALTER triggers the existing Round 24 journal, coordinator decision, prepared
catalog, replacement retirement, crash recovery, and optional Round 25 GC bytes.

Old generated SDK/manifest fingerprints remain exact expectations: an ALTER changes
the canonical fingerprint, so a new typed connection or stale manifest expectation
fails rather than receiving additive compatibility.

PostgreSQL synthetic table and index OIDs are currently keyed by the table
fingerprint as well as logical identities. They are therefore recomputed after an
ALTER even though the Core `TableId`, existing `ColumnId`, and `IndexId` remain
stable. Inspector column and index names always come from the current catalog.

## Next architecture question

Round 27 should audit schema-transaction composition before adding physical type
conversion: multiple mutations require overlay chaining, multiple reservations,
one final prepared NBSC, intermediate Heap cleanup, rollback/recovery ordering, and
replacement-retirement lineage. The current single-mutation API must not simply be
called in a loop.
