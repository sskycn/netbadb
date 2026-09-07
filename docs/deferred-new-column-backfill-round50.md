# Production deferred new-column backfill (Round 50)

Round 50 productionizes the ordered typed program selected by the
[Round 49 audit](deferred-new-column-backfill-round49.md). It opens one bounded
relational operation after adopted-source refinement:

```sql
UPDATE same_adopted_table
SET late_reserved_column = deterministic_typed_expression
WHERE deterministic_typed_predicate;
```

Every assignment target must be a current column absent from the captured base
`TableDef` and backed by the transaction's durable tag-16 `ColumnId`
reservation. At this milestone every column read by the predicate or
right-hand side had to be a surviving base `ColumnId` from the same adopted
table. Literals, `NULL`, bound
scalar parameters, supported casts, comparisons, Boolean operations, and
`IS [NOT] NULL` reuse the existing typed executor evaluator. Functions,
arithmetic outside the existing language, subqueries, joins, aggregates,
late-column reads, base-column writes, and cross-table access remained closed.
Round 54 now adds the bounded late-read extension described in
[`deferred-virtual-row-round54.md`](deferred-virtual-row-round54.md), without
changing this Round 50 base-only semantic domain or golden digest.

This is bounded deferred migration UPDATE support. It is not general
post-refinement DML.

## Production authority and lifecycle

The private `deferred_backfill` Core module owns the program, actions,
assignments, evaluation layout, Execute observation, accumulator, canonical
digest, eligibility proof, row transform, and final verification. No type is a
public Core, SDK, protocol, or inspection API.

```text
AdoptedSourceRefining
    -> first accepted deferred UPDATE
AdoptedSourceBackfilling
    -> projected late-column SET NOT NULL, or more deferred UPDATEs
    -> first final CREATE/DROP INDEX
AdoptedSourceIndexFinalizing
    -> commit/finalization
```

`AdoptedSourceBackfilling` is still logical. It is deliberately excluded from
`is_sealed`: S1 remains the sole physical table and its transaction-visible
P1 is the source authority. Structural ALTER freezes after the first deferred
action. Surviving-base SET/DROP NOT NULL remains available. A nullable late
column may become NOT NULL only after at least one accepted deferred action and
only after a streaming validation of `RowProjection + ordered program` proves
that every projected value is non-NULL. Failure does not mutate metadata, so
the native transaction can accept another repair action and retry. Late-column
DROP NOT NULL keeps its earlier error.

The first final index operation reuses Round 48 and remains terminal. Further
same-table final CREATE/DROP operations may compose, while UPDATE and ALTER are
closed.

## Execute contract

`execute_prepared_in` first validates transaction ownership and exact prepared
dependencies, then binds parameters through the compiler. Bound parameter
nodes become owned `ScalarValue` literals before the deferred route sees the
statement. Parse, Prepare, Bind, and Describe do not scan S1, append actions,
change phase, touch the journal, or allocate storage.

An eligible Execute proves identities and expression bounds, constructs the
action, opens the exact transaction-visible S1/P1 read view, and streams every
visible row through the storage visitor. It computes the SQL affected-row count
and a deterministic result digest. Only after the complete scan succeeds does
Core append the semantic digest and action and enter Backfilling. Zero matching
rows are a valid accepted action and return `AffectedRows(0)`.

The hard limits are 32 actions, 32 assignments per action, 256 expression
nodes, depth 32, and the existing requirement that the plan-wide evidence
count remain below 128 before acceptance. Limit checks occur before the source scan where possible. A stale
prepared statement, bad parameter, ineligible expression, evaluation error, or
scan failure leaves the program, action evidence, phase, journal, S1, source
index digest, and storage allocation unchanged.

Ineligible UPDATE plus SELECT, INSERT, and DELETE fall through the existing
`MigrationDataAccessAfterRefinement` boundary (`25000` over PostgreSQL).

## Canonical evidence

Each action uses the exact domain:

```text
NetbaDB deferred backfill action v1\0
```

The digest binds the target `TableId`, assignment count and SQL order, each
target `ColumnId`, typed right-hand-side tree, and typed predicate presence and
tree. Expression and scalar kinds have explicit tags; integers and lengths use
defined little-endian encodings. Types bind physical type, nullability, and
semantic-name presence/value. A golden regression pins the byte contract.

Execute observations use a separate versioned domain and bind affected source
rows plus ordered target/value results. They are runtime transaction state and
are not persisted. Action semantic digests enter the existing `action_digest`,
which is already bound by the tag-25 aggregate intent and tag-35 clone-plan
digest.

## Finalization and recovery

The existing adopted-source finalizer remains the only finalizer. For each S1
row it performs:

```text
RowProjection without target constraints
    -> ordered deferred actions
    -> final target constraints
    -> one S2 insert and final index maintenance
```

It accumulates the same observation while streaming and rejects an
Execute/finalization mismatch as corruption before publication. A dirty table
allocates one S2, performs one source pass, and never creates S3 or a sidecar.
Deferred Execute itself allocates no target storage and does not modify S1 or
its indexes.

The program and typed expressions exist only in the active transaction. No
NBSJ tag, AST codec, replay record, or recovery branch was added. A crash after
action acceptance but before CORD loses the in-memory program and the existing
rollback path restores S1. Once CORD's durable commit decision exists, tag-25,
tag-35, prepared NBSC, and staged S2 contain all recovery authority; recovery
never replays SQL or the program. Source-first, target-first, reverse-order,
both-prepared, and both-committed cases converge through the existing theorem.

## PostgreSQL and Columnar behavior

The PostgreSQL adapter has no deferred-backfill special case. Simple Query and
Extended Query return ordinary `UPDATE n`, `ALTER TABLE`, `CREATE INDEX`, and
`COMMIT` tags. Projected NOT NULL failure maps to `23502`; the explicit session
then returns `25P02` until rollback. Existing authorization still requires
write permission for UPDATE and schema-admin permission for ALTER/index DDL.

Columnar projections remain derived read acceleration. Deferred Execute reads
the transaction-visible authoritative S1/P1 and observes its own prior UPDATE,
INSERT, and DELETE; it does not consult a possibly stale projection. After an
S1-to-S2 publication, old projection metadata remains tied to S1 and becomes
stale or unavailable, so planning falls back to authoritative storage. The
current refresh API rejects that old table identity even when its selected
columns survive; Round 50 does not weaken projection identity or automatically
rebuild a projection, and Columnar never becomes a CORD participant.

Round 52 now gates this final S1-to-S2 step when the S1 Change Stream is
`Enabled` or `Unavailable`. The deferred UPDATE/nullability/index program
remains rollbackable, but no S2 `StorageId`, rewrite intent, tag 35, stage,
target, or committed S1 change batch is produced. After explicit rollback and
S1 stream disable, the unchanged Round 50 finalizer proceeds; its complete S2
baseline represents the intentionally disabled migration window.

## Compatibility and exclusions

Canonical schema, NBSC/NBSM, NBSJ v1 tags 1--35, CORD v2, Heap/Page, WAL,
transaction status, IndexCatalog v9, BTree v3, NBTR, PartitionCatalog, LSM,
Columnar manifests/segments, Protocol v1, PostgreSQL framing v3, Manifest v4,
SDK Schema Spec v1, generated SDKs, and inspection formats are unchanged.
There is no parser syntax or executor public-API expansion.

LSM, partitioned, imported/bootstrap, multi-table, defaults, generated values,
type conversion/USING, UNIQUE or multicolumn indexes, savepoints,
online/resumable migration, participant detach, automatic GC, and general
Alembic migration remain outside this slice.
