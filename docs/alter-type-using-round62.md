# Bounded production `ALTER COLUMN TYPE ... USING` (Round 62)

Round 62 productionizes the fresh-ColumnId synthetic lowering selected in
Round 61. The exact accepted SQL shape is:

```sql
ALTER TABLE table_name
ALTER COLUMN column_name TYPE target_type
USING same_table_expression;
```

`USING` is mandatory. `SET DATA TYPE`, type modifiers, parameters, multiple
actions, same-physical rewrites, primary-key conversion, cross-table
expressions, and unsupported scalar syntax remain rejected. The target type is
the existing NetbaDB physical/semantic declaration universe, and the USING
result must have that exact semantic type. A direct cast from the old column to
the target is not required: USING may instead reference another old column or a
typed constant. Explicit postfix casts retain the checked Round 60 conversion
and error rules.

## Typed lowering and authority

The parser retains the old column, target declaration, USING AST, and action
span. HIR resolves the exact old ColumnId and binds every expression reference
against the old table schema. The compiler lowers the typed expression to
`netbadb_rel::Expr`; Core never evaluates HIR or reparses SQL.

The prepared DDL access set names the target table in read, write, and schema
tables and requires schema-write authority. PostgreSQL and native Protocol v2
use this same prepared boundary; there is no frontend-specific bypass.

Core chooses one of two existing source authorities:

- a pristine transaction reads one committed S1 snapshot without creating a
  fake writer or participant;
- a transaction with same-table DML adopts the exact transaction-visible
  S1/P1 view, including a zero-row write operation.

Prior read-only access, other-table DML, group members, non-Single-Heap
placements, and an enabled Change Stream remain closed.

## Schema and index transform

For old schema B and old column `Cold`, Core constructs evaluation schema E by
appending fresh `Cnew` as a nullable evaluation slot. It installs one synthetic
deferred assignment whose RHS is the compiled USING expression. Final schema F
replaces `Cold` with `Cnew` at the same declaration ordinal and restores the old
public name and nullability. The evaluation-only name is derived internally,
does not enter the semantic digest, and never appears in F, inspection,
PostgreSQL catalogs, or persistent storage.

Validation performs one authoritative full scan and retains only bounded
action evidence, not converted rows. Invalid text, numeric range, unsupported
cast, result-type mismatch, or NOT NULL failure leaves the transaction
structurally unchanged and burns no ColumnId or IndexId. Only after validation
succeeds does Core durably reserve the fresh ColumnId and, when the old column
has one supported named single-column secondary index, a fresh replacement
IndexId. The old index is never retargeted. Unrelated indexes preserve their
identities. Primary-key, unnamed, unique, or multicolumn conversion shapes stay
unsupported.

Acceptance enters the production `TypeConversionReady` state. No StorageId or
S2 is allocated at statement success, S1 and its BTree remain unchanged, and
only COMMIT or ROLLBACK is then legal. Fresh relational preparation, fresh DDL
preparation, and execution of statements prepared before the conversion are
rejected while the transaction is sealed.

COMMIT reuses the ordinary deferred materializer: one S1-to-S2 pass evaluates
the same action, verifies the validation observation, projects E to F, and
builds the final index inventory. Publication advances TableSchemaVersion and
SchemaGeneration once. ROLLBACK publishes nothing; successful identity
reservations remain monotonically consumed.

## Persistence and recovery

Pristine conversion uses the ordinary tag-25 logical composition record and
does not manufacture tag 35. Adopted conversion retains the existing tag-25
plus tag-35 source-backfill authority. NBSJ remains tags 1–35, the deferred
semantic domain remains v1, and Canonical Schema, Heap, BTree, WAL, coordinator,
Change Stream, Columnar, Protocol v2, Schema Spec, inspection JSON, manifest,
and SDK format versions are unchanged.

Stable conversion failpoints are:

```text
alter-type-column-reservation-durable
alter-type-index-reservation-durable
alter-type-logical-plan-sealed
```

Before a durable coordinator Decision, recovery restores S1/Cold/Iold and
removes staged resources. After Decision, recovery publishes the already
materialized S2/F/Inew. Recovery never reparses or reevaluates USING. Both
pristine and adopted crash matrices converge identically across three reopens.

## Errors and scope

PostgreSQL success returns `ALTER TABLE`. Existing mappings are retained:

| Condition | SQLSTATE |
| --- | --- |
| invalid Text conversion | `22P02` |
| numeric out of range | `22003` |
| unsupported explicit cast | `42846` |
| USING result mismatch | `42804` |
| unsupported shape, same physical type, or active Change Stream | `0A000` |
| primary-key/dependent object | `2BP01` |
| permission denied | `42501` |
| command after explicit transaction failure | `25P02` |

This is not PostgreSQL-complete ALTER TYPE. It is not online or resumable and
does not implement implicit conversion, same-ColumnId physical mutation,
constraint/default/generated-column rewriting, LSM/partition/import migration,
cross-table USING, functions/subqueries, or multiple conversions in one
transaction. The explicit Round 60 shadow-column workflow remains available
when data needs staged repair or a shape is outside this bounded operation.

Reproduce the PostgreSQL 17.11 acceptance with:

```bash
DYLD_LIBRARY_PATH=/opt/local/lib/icu/lib \
  python3 scripts/test-alter-type-using-round62-sql.py
```
