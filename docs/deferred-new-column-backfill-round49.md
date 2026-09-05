# Deferred new-column backfill architecture audit (Round 49)

Round 49 is the historical architecture audit whose selected Candidate A was
productionized by [Round 50](deferred-new-column-backfill-round50.md). At this
milestone it did not add production SQL. The requested historical baseline was
`0b3a71a112b06eaa42a20a8dc3e6111e57e28fb2`; work began on current `main` at
`84253de8af298efc48bbe426e3dee6492186720d`, whose parent is that baseline and
whose additional commit is the already integrated Columnar Phase 1 work.

## Current gap and decision

After the first adopted-source refinement, production continues to reject
SELECT, INSERT, UPDATE, and DELETE. A newly added column exists only in the
logical final `TableDef`; physical S1 retains the base layout and
`RowProjectionEntry::SynthesizedNull` supplies its value. Consequently a
nonempty table cannot set that column NOT NULL and no SQL operation can populate
it before Round 48 builds a final index.

The audit selects **Candidate A: an ordered deterministic deferred backfill
UPDATE program**. The value authority during an undecided migration is:

```text
transaction-visible S1/P1 row truth
    +
ordered, bound DeferredBackfillAction transformation truth
```

Each accepted action contains one target `TableId`, target `ColumnId`s, typed
RHS expressions, an optional typed predicate, captured scalar parameter values,
the Execute observation, and a canonical semantic digest. Names are diagnostic
compiler data and do not confer identity. Ordered actions preserve statement
order and later matching statements overwrite earlier values.

The alternative of composing one final expression per new column was rejected.
It turns sequential UPDATE behavior, predicates, simultaneous assignments, and
later-wins behavior into expression rewriting. An ordered action list directly
represents the SQL execution order and gives each Execute an independent row
count and result digest.

## Expression and statement boundary

The proposed first production slice accepts only same-table UPDATE when every
assignment target:

- exists in the final overlay;
- does not exist in the adopted base `TableDef`;
- has a durable tag-16 `ColumnId` reservation; and
- is not a base column or primary key.

RHS and WHERE reads are restricted to surviving base `ColumnId`s. Literals,
NULL, bound parameters, casts supported by the current typed language,
comparisons, boolean operators, and `IS [NOT] NULL` are deterministic and
eligible. There are no functions, clocks, randomness, external state,
subqueries, joins, aggregates, windows, or arithmetic in the current UPDATE
expression surface. Reads of any late-added column are rejected, including a
later action reading a value written by an earlier action. One statement may
assign several new columns; all RHS values are evaluated against the same base
row before targets are changed.

Bound parameters are selected for Round 50. Core binds the prepared statement
at Execute, type-checks each supplied `ScalarValue`, and the compiler replaces
parameter nodes with owned literal values. No parameter slot, portal, frontend
binding, or session reference enters the deferred program. The test prototype
executes TEXT, BIGINT, and BOOL bound parameters and proves that distinct bound
values produce distinct program digests.

The prototype limits one transaction to 32 deferred actions, 32 assignments per
action, 256 expression nodes per action, and expression depth 32. Round 50
should retain explicit limits and also count each deferred action in the existing
128 schema/index action ceiling.

## Shared evaluator and Execute semantics

The executor now exposes its existing side-effect-free typed row evaluator as
`evaluate_typed_row_expression` and `typed_row_predicate_matches`. Normal
execution and the prototype therefore share comparisons, NULL propagation,
boolean logic, casts, and TRUE-only predicate semantics. FALSE and UNKNOWN do
not match.

Execute performs a transaction-visible S1 scan before accepting an action. It
evaluates the predicate and every RHS for every matching row, checks runtime
value compatibility, counts the exact matches, and computes a result digest.
Only after the full scan succeeds does it append the action. The normal command
result is `AffectedRows(n)`, including zero. Parse, Prepare, Bind metadata, and
Describe remain pure: they do not touch the journal, allocate S2, acquire a new
writer, or change phase.

The result observation contains the exact affected count and a canonical digest
of ordered source values, target `ColumnId`s, and computed values. During the one
final copy pass, the same evaluator recomputes each action and compares every
observation after the scan. A mismatch is a hard `Corrupt` invariant error.
This check makes a violation of the frozen-S1 premise visible rather than
silently publishing different results.

The executable fixture starts with rows 1/2/3, then performs an ordinary update
of row 1, insertion of row 4, and deletion of row 2 before adoption. Deferred
evaluation sees exactly rows 1/3/4 and their transaction-visible values. It
returns exact counts for TRUE, FALSE, and UNKNOWN predicates.

## Phase model

Round 50 should add one explicit Core-private state:

```text
AdoptedSourceRefining
    -> first eligible deferred UPDATE
AdoptedSourceBackfilling
    -> first accepted final CREATE/DROP INDEX
AdoptedSourceIndexFinalizing
```

`AdoptedSourceBackfilling` freezes structural schema. ADD COLUMN, DROP COLUMN,
RENAME TABLE, and RENAME COLUMN close after the first deferred UPDATE. This
deliberately leaves ADD -> UPDATE -> DROP unsupported in the first slice and
avoids program rewriting or invalidation. More eligible new-column UPDATEs may
repair or overwrite values. Projected new-column SET NOT NULL and compatible
surviving-column nullability work may follow. The first final-index statement
remains terminal and closes deferred UPDATE.

General relational access remains closed. INSERT, DELETE, SELECT, base-column
UPDATE, primary-key UPDATE, cross-table access, LSM, partitioned, imported, and
bootstrap storage do not become legal. The supported authority remains one
managed Single Heap.

## Projected NOT NULL and final indexes

The selected layout is `RowProjection + DeferredRowTransform`:

```text
S1 row
  -> source-column mapping plus synthesized NULL slots
  -> ordered deferred actions
  -> final target constraint validation
  -> S2 insert and final-index maintenance
```

This order generalizes Round 41 Policy B. With no program, a new column is NULL,
so SET NOT NULL succeeds only for an empty table. With a program, SET NOT NULL
scans the frozen transaction-visible S1, applies the same transform, and checks
the final new-column value. A partial fill fails without changing metadata; a
later eligible action can fill the remaining rows and a retry succeeds. An empty
table succeeds vacuously. At the PostgreSQL boundary a failed explicit
transaction still follows the existing error and subsequent `25P02` behavior;
repair is a native Core semantic for a caller that retains an active transaction
under the future API contract.

The full prototype populates TEXT, BIGINT, and BOOL new columns, validates them
NOT NULL, declares a Round 48 final index, and materializes exactly one S2 in one
three-row projection pass. Direct physical BTree point lookups return computed
new-column values, including after three close/reopen cycles. S1 row width,
schema, physical indexes, storage identity, and captured source index digest do
not change at deferred UPDATE Execute.

## Canonical program and durable binding

The proposed canonical action digest begins with a domain/version string and
encodes fixed-width little-endian values in this order:

```text
target TableId
assignment count
for each assignment in SQL order:
    target ColumnId
    typed RHS tree
predicate presence and typed predicate tree
```

Expression nodes use explicit tags for column, literal, cast, binary operator,
unary NOT, and IS NULL. Column references encode `TableId` and `ColumnId`, never
names, ordinals, pointers, Debug output, Rust enum discriminants, or hash-map
iteration. Types encode physical type, nullability, and optional semantic name.
Scalar values use explicit tags and lengths. Execute-bound parameters are
literal nodes. Action ordering is the vector order.

The prototype appends each semantic digest to existing
`SchemaTransactionPlan::action_evidence`. Therefore the existing tag-25
`action_digest` changes with transformation semantics, and the existing tag-35
`SourceBackfillIntent.clone_plan_digest` binds:

```text
transaction + S1 + S2 + action_digest + final snapshot digest
```

The executable test recomputes and matches this chain. The existing records
remain fixed `[u8; 32]` digests; no decoder, tag, or binary layout changes. This
is a compatible extension of digest input semantics, not a new persistent
format. Round 50 should document the new domain string as part of the durable
semantic contract before production use.

## Crash and retry theorem

The program is needed only while the owning process executes an undecided
migration. It does not need a persistent AST or replay encoding:

- Before CORD, recovery loses the entire adopted migration, rolls back P1,
  removes S2/stages, restores base schema and rows, and retains existing durable
  ID burns.
- After CORD, S2 is already fully projected and prepared. Recovery completes
  the existing source/target decision and never evaluates the program.

Subprocess tests terminate after action acceptance, tag 25, tag 35, stage intent,
mid-copy, complete S2/index construction, first prepare, all prepares, durable
decision, first commit in both participant orders, and all commits. Every
pre-decision state recovers the base S1 loser. Every post-decision state recovers
the same computed S2 values and direct physical BTree across three reopens.

A retryable pre-decision finalization error must retain the in-memory program.
An error that enters the existing rollback-required state may discard it only
with the whole transaction. Explicit rollback drops ordinary S1 DML, schema,
and the program; existing tag-16 burns remain. Deferred Execute allocates no
`StorageId` and mutates no physical index.

## Alternatives

**B, persistent scalar DEFAULT**, is a useful independent feature but a larger
and different contract. Honest DEFAULT semantics require adding a default to
canonical `ColumnDef`, changing canonical bytes and fingerprints, selecting a
catalog compatibility/version strategy, extending parser/HIR/compiler and
schema-spec/generated SDKs, and making every omitted-column INSERT path consult
the default. Literal-only NULL/BOOL/BIGINT/TEXT defaults still have durable
future-DML meaning. PostgreSQL-style missing-value metadata additionally changes
Heap reads, updates, index construction, vacuum, and reopen semantics. The
test-only digest probe demonstrates that two defaults for the same current
column require different canonical bytes. Round 49 does not introduce v2.

**C, RowId sidecar**, evaluates once and naturally supports staged reads of new
columns, but stores O(updated rows) values and becomes a second pre-final value
authority. It must define RowId/generation behavior across own updates,
relocation, deletes, multiple statements, rollback, and large migrations. A
probe over the exact post-DML source view records three entries for ids 1/3/4,
confirms their opaque handles and values are stable across repeated frozen-view
scans, and extrapolates a 10,000-row lower bound as
`10,000 * (size_of(StorageRowHandle) + size_of(ScalarValue))`. Real Text
allocations and map overhead are additional. It remains non-durable before CORD
but complicates the authority theorem without helping recovery.

**D, early S2**, makes post-refinement DML conventional only by allocating and
copying before the final schema is known. It loses clean ADD -> DROP and
rename-back no-ops, extends participant lifetime, replaces S1/P1 as sole
pre-decision truth, and may require S1 -> S2 -> S3 for later layout changes.
The Candidate A fixture proves no S2 allocation at Execute and one allocation at
effective finalization; D necessarily gives up that observation.

**E, new-column SET NOT NULL alone**, retains synthesized NULL. It fails for
every nonempty table and succeeds only vacuously for an empty table, so it does
not enable the target migration.

| Criterion | A Deferred program | B Persistent DEFAULT | C Sidecar | D Early S2 | E Cnew NOT NULL only |
| --- | --- | --- | --- | --- | --- |
| standard SQL surface | existing UPDATE | ADD DEFAULT | existing UPDATE | existing UPDATE | ALTER only |
| new parser syntax | no | yes | no | no | no |
| new canonical schema format | no | yes | no | no | no |
| S1 sole physical data truth | yes | yes | sidecar adds value truth | no | yes |
| one final S2 | yes | yes | yes | uncertain for later ALTER | yes |
| clean no-op preservation | yes before backfill; structure then frozen | depends on DEFAULT semantics | structure needs invalidation | poor | yes |
| memory proportional to rows | no | no | yes | physical copy | no |
| expression power | current deterministic typed subset | scalar constants initially | broad but costly | ordinary executor | none |
| exact UPDATE rowcount | yes, validation scan | not an UPDATE | yes | yes | not an UPDATE |
| prepared parameter complexity | bind to owned scalar | persistent DEFAULT binding | bind plus sidecar | ordinary binding | none |
| crash/recovery changes | digest input only | schema/catalog compatibility | authority proof | participant lifecycle | none |
| new durable record | no | likely schema version | no before CORD | may require lifecycle evidence | no |
| Cnew SET NOT NULL enablement | yes | only if default non-NULL | yes | yes | nonempty fails |
| Round 48 final index reuse | direct | direct after semantics work | direct | lifecycle changes | NULL-only |
| Alembic migration value | matches bounded add/update/alter/index shape | useful but different shape | matches with cost | matches with cost | negligible |
| implementation risk | bounded | high and cross-layer | medium/high | highest | low but low value |

## Exact Round 50 production scope

Round 50 should productionize Candidate A only for an explicit transaction with
one managed Single Heap that has performed ordinary one-table DML and then
entered adopted-source refinement. It should accept same-table UPDATEs that
write only durably reserved late-added columns and read only surviving base
columns plus deterministic literals or Execute-bound scalar parameters. It
should support multiple assignments and ordered repair actions, projected Cnew
SET NOT NULL, then the existing terminal single-column non-unique CREATE/DROP
INDEX phase. The finalizer streams transaction-visible S1 through one
`RowProjection + DeferredRowTransform` into one S2.

Round 50 should keep structural ALTER closed after the first deferred UPDATE,
keep late-column reads and all other relational statements closed, and keep the
same-table/managed-Heap boundary. It should introduce no S3, sidecar Heap,
default/generated syntax, arithmetic/functions, new journal tag, persistent
expression encoding, protocol special case, or PostgreSQL adapter path.

## Persistent-format and surface audit

| Surface | Candidate A Round 50 impact |
| --- | --- |
| Canonical Schema / NBSC / NBSM | none |
| NBSJ v1 tags 1-35 | no layout/tag change; action digest gains a documented domain |
| CORD v2 | none |
| Heap/Page, WAL, transaction status | none |
| IndexCatalog v9 / BTree v3 | reuse final materialization |
| PartitionCatalog / LSM | unsupported by eligibility; no format change |
| Protocol v1 / PostgreSQL framing v3 | normal UPDATE command tag and existing errors |
| Manifest v4 | none |
| SDK Schema Spec v1 / inspection | none |

Round 50 supersedes this audit's production closure with the exact bounded
late-column UPDATE and projected Cnew SET NOT NULL design selected above.
Other post-refinement relational access remains closed, with PostgreSQL
`25P02` after a failed explicit transaction. Round 48 final-index behavior stays
positive. The known older `test-sql-alter-table.py` indexed SET NOT NULL blocker
and `test-postgresql-orm.py` imported/bootstrap DROP blocker are unrelated and
are not broadened by this audit.
