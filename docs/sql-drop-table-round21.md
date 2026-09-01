# Round 21: generic SQL DROP TABLE

Round 21 connects one bounded SQL form to the existing Round 20 Core lifecycle:

```sql
DROP TABLE projects;
```

The parser accepts exactly one unquoted, unqualified name and an optional trailing
semicolon. The AST contains only that logical name and UTF-8 byte spans. HIR resolves
the current transaction `SchemaView` to the exact shared `DropTableTarget`:
`TableId`, `TableSchemaVersion`, and canonical `SchemaFingerprint`. It contains no
`StorageId`, PostgreSQL OID, page, row, or filesystem identity. The compiler retains
that target in `CompiledDdlStatement::DropTable`; execution calls the existing Core
`drop_table_in` operation and never looks up the name again.

## Preparation, access, and authorization

Parse, HIR lowering, compilation, native preparation, PostgreSQL Parse, Bind, and
Describe have no schema-writer, identity-allocation, journal, catalog, or storage
effects. Extended Describe reports zero parameters and `NoData`. Execute is the
first lifecycle operation and returns native `AffectedRows(0)` or PostgreSQL
`CommandComplete("DROP TABLE")`.

`StatementAccess` marks a schema write and separately records the exact schema
target TableId. It does not classify DROP TABLE as row data write. Network execution
therefore requires `schema_admin` but no table DML grant; index DDL retains its
existing schema-admin plus target-write policy. Authorization runs after successful
compilation and before Core writer admission. Denial is `42501` and has no side
effects. Grants and manifests remain external, immutable TableId policies: dropping
a table neither edits them nor grants access to a same-name replacement.

## Transactions and prepared identity

Autocommit uses Core's normal schema transaction. Explicit transactions see the
table removed from their private schema immediately; global schema and inspection
remain unchanged until commit. Rollback preserves the exact table, data, indexes,
generation, versions, high-waters, and active storage. Commit advances schema
generation once, publishes logical absence, and retains the physical Heap and its
index pages for deferred garbage collection. Catalog-only reopen resolves the same
winner or loser outcome.

A prepared DROP remains bound to its original target. If another session drops and
recreates the same name, execution of the old statement fails as undefined/stale
(`42P01` through PostgreSQL) and the replacement survives. Index creation/removal,
ANALYZE/runtime catalog revision, and DML do not change the table schema identity,
so they do not stale a prepared DROP.

Any DDL error in an explicit PostgreSQL transaction enters failed state; subsequent
commands return `25P02` until rollback. A missing table maps to `42P01`, syntax to
`42601`, unsupported forms to `0A000`, permission denial to `42501`, and schema
writer contention to `55P03`.

## Supported and deliberately unsupported SQL

Supported: `DROP TABLE name` with an optional semicolon. Explicitly rejected:

- `IF EXISTS`, `CASCADE`, and `RESTRICT`;
- multiple, qualified, quoted, or `ONLY` targets;
- DROP VIEW, MATERIALIZED VIEW, SCHEMA, SEQUENCE, TYPE, and other object kinds;
- LSM and range-partitioned table retirement;
- multiple schema mutations or table/index DDL mixing in one transaction.

`IF EXISTS` remains deferred because name absence and a stale exact prepared target
are different states. No PostgreSQL-only target type or string-matching execution
path was added. Protocol v1 and every persistent format remain unchanged: NBSC v1,
NBSM v1, NBSJ v1, CORD v2, Heap, WAL/status, BTree/IndexCatalog, partition, LSM,
native Protocol v1, and PG wire encodings.

## Verification scope

Deterministic tests cover grammar/spans and parser mutation, HIR/compiler identity,
pure preparation, native Protocol v1, PostgreSQL Simple and Extended Query,
authorization, rollback/commit, failed transaction state, missing tables, indexes,
same-name recreation, unrelated data/index changes, physical retention, and three
catalog-only reopens. SQL-driven subprocess tests crash once before the coordinator
decision and once after its durable winner decision; the complete Round 20 crash
matrix remains enabled.

Real psql 17.11, psycopg 3.2.13 (including `prepare=True`), and SQLAlchemy 2.0.52
`Table.drop(connection, checkfirst=False)` probes cover rollback and commit. The
existing guarded Alembic index regression remains unchanged; `DropTableOp`,
`MetaData.drop_all`, general table migrations, physical resource deletion, and
security-catalog cleanup are not claimed.

Next recommended work is **physical-resource garbage-collection architecture**:
define safe retention horizons across coordinator history, open handles, prepared
state, and crash recovery before unlinking any retired Heap/WAL/index resource.
That correctness boundary is more important than adding thin DROP syntax variants.
