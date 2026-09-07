# Production VirtualRow late-column reads (Round 54)

> Round 55 subsequently audited a test-only frozen-E/final-F terminal
> structural architecture. Production DROP/RENAME after deferred backfill
> remains closed; see
> [`deferred-terminal-structural-round55.md`](deferred-terminal-structural-round55.md).

Round 54 productionizes the Candidate A semantics selected by the
[Round 53 audit](deferred-virtual-row-round53.md). During the bounded adopted-
source backfill phase, a deferred same-table `UPDATE` may now read both
surviving base columns and visible, durably reserved late-added columns.

This remains a narrow migration operation. It is not general post-refinement
DML or a transactionally materialized virtual table.

## One production read authority

The earlier test-only `ReadAuthority` split and audit Execute entrance are
removed. Production `Database::execute_in` and `execute_prepared_in` use one
builder and one row transform:

```text
transaction-visible authoritative S1/P1 row
        -> RowProjection by ColumnId
        -> NULL for visible reserved late columns
        -> accepted deferred actions in SQL order
        -> immutable pre-statement VirtualRow
```

The readable late set is the intersection of three facts: the `ColumnId` has a
durable tag-16 reservation, exists in the current target `TableDef`, and is
absent from the captured base `TableDef`. The table identity must be the same
adopted `TableId`. Thus an ADD-then-DROP identity remains burned allocator
history but is not readable. Names and old ordinals are never authority;
renames and earlier-column drops continue to work through stable IDs.

`EvaluationLayout` stores typed `OutputField::Source(ColumnRef)` entries and
their current target positions. A surviving base value at a target position is
exactly the value copied there by `RowProjection`. Late positions initially
contain database `NULL`.

## Statement and program semantics

For action N, Execute projects each S1 row and replays actions `[0..N)` through
`DeferredBackfillProgram::apply_row`. The candidate predicate and every RHS
then evaluate against the same immutable pre-action values. Only after all RHS
values are evaluated and validated are assignments applied, so simultaneous
assignment and swap semantics match SQL. Later statements observe earlier
accepted results and a later matching write wins.

This supports repair and late-to-late copy patterns such as:

```sql
UPDATE users
SET marker = legacy
WHERE marker IS NULL AND legacy IS NOT NULL;

UPDATE users SET marker = 'missing' WHERE marker IS NULL;

UPDATE users
SET normalized = marker
WHERE marker IS NOT NULL;
```

Every candidate performs one frozen transaction-visible S1 scan. A predicate
counts a row only when it is TRUE; FALSE and UNKNOWN do not match. A zero-row
first action is accepted, returns `AffectedRows(0)`, enters Backfilling, and
freezes structural ALTER. The action is appended only after the complete scan,
prefix-observation verification, expression evaluation, type checking, and
current target constraint validation succeed.

Assignments remain restricted to visible reserved late columns. Base, primary-
key, mixed base/late, unknown, foreign, dropped, and unreserved targets are not
eligible. General SELECT, INSERT, DELETE, cross-table access, and relational
shapes outside the existing single Scan/Filter UPDATE stay closed with the
existing migration guard.

## NULL, prepared statements, and failure atomicity

Late columns are synthesized as NULL until a producer writes them. Current
target nullability is captured by each action. Writing literal or bound NULL
to a late NOT NULL target fails during the candidate scan with
`NotNullViolation`; it does not append semantic evidence or an action.
Projected SET NOT NULL remains repairable: a failed validation preserves the
nullable overlay and program, a repair action may follow, and a retry can
succeed. Later actions may read that column using its new non-null metadata.

Execute binds parameters before the deferred route, so retained expressions
contain owned `ScalarValue` literals and no `ParameterId`, portal, frontend, or
session reference. A consumer prepared before its producer remains valid when
the existing table identity/version/fingerprint dependencies are unchanged and
reads the then-current prefix at Execute. Program growth is not a prepared
dependency. A subsequent schema fingerprint/version change still produces
`StalePreparedStatement`.

Failed stale/bind/identity/authority/expression/type/nullability/scan/prefix
checks leave the program, action evidence, phase, S1, source indexes and
storage allocator unchanged. The existing limits remain 32 actions, 32
assignments per action, 256 expression nodes, depth 32, and 128 plan actions.

## Finalization, evidence, and recovery

Execute and finalization share `DeferredBackfillProgram::apply_row`.
Finalization streams S1 once, projects and applies the ordered program,
validates final constraints, inserts each row into exactly one S2, and builds
the complete final index inventory. The accumulated Execute observations must
match finalization exactly; mismatch is hard `Corrupt` before winner
publication.

The action domain remains:

```text
NetbaDB deferred backfill action v1\0
```

The Round 50 base-only golden remains
`1e1bc04b766e0d63bbda95f5d975439f35442d33afc73143e5d68232458216a0`.
The production late-read action `normalized = marker WHERE marker IS NOT NULL`
is pinned as
`ba8201334bcc39194e0ca6e2136626f1e0f16059fc00421c297f07af1d65fdef`.
Tests cover RHS and target ColumnIds, NULL predicate polarity, producer
literals, bound values, and action order. A consumer's own digest can remain
equal while different producers change the ordered program digest, tag-25
action digest and tag-35 clone-plan digest.

The transaction-local program and runtime `ActionObservation` are not durable.
Pre-CORD crashes discard them and recover S1. Post-CORD recovery selects the
already materialized S2, exact late values and final BTree. Recovery never
parses SQL, decodes expressions, binds parameters, or replays VirtualRows.
The full publication/recovery matrix converges over three consecutive opens.

## Change Stream and Columnar boundaries

An Enabled or Unavailable S1 Change Stream does not prevent logical deferred
UPDATE acceptance, because those actions do not mutate S1 and emit no NBCL
event. Round 52 still blocks final `RewriteHeap` with `0A000` before S2
allocation. Explicit rollback leaves the stream frontier and committed batch
count unchanged. Disabled and never-enabled sources may replace; callers may
then explicitly enable S2 and take a new committed read anchor. No automatic
enablement, cursor translation, or rebaseline is introduced.

VirtualRow always begins with authoritative S1/P1. NBCS/NBCD and snapshot or
incremental Columnar projections are never migration read authority. Old
projections remain tied to S1 after replacement and existing Phase 2B/2C/2D
fallback, identity, compaction, retention, lazy index and CRC behavior is
unchanged.

## PostgreSQL behavior

The adapter has no migration-specific evaluator. Simple Query reports exact
`UPDATE 2`, `UPDATE 1`, `UPDATE 3`, ordinary ALTER/CREATE/COMMIT tags, and the
final values survive three opens. Extended Parse may precede a producer; Bind
and Execute later reconstruct the current prefix. Bound scalars remain owned.

True unsupported relational or base/mixed-write attempts remain `25000`, and
the next command in a failed explicit transaction remains `25P02`. Projected
NOT NULL failure remains `23502`. With an Enabled stream the UPDATE succeeds,
COMMIT fails `0A000`, the next query returns `25000`, and ROLLBACK remains
available. Authorization is still ordinary UPDATE plus existing schema-admin
authority for ALTER and index DDL.

## Compatibility and non-goals

Round 54 changes no Canonical Schema, NBSC/NBSM, NBSJ tag 1--35, CORD,
Heap/Page/WAL/status, NBCL, NBCM/NBCS/NBCD/NBPC, IndexCatalog/BTree,
Partition/LSM, Protocol v1, PostgreSQL framing, deployment Manifest, SDK Schema
Spec, inspection or generated-code format. There is no tag 36, persistent
program/AST/VirtualRow, sidecar, row cache, early S2, S3, or `RowEntityId`.

Still unsupported are SELECT/INSERT/DELETE after refinement, base-column
UPDATE, structural ALTER after the first deferred action, DEFAULT/generated
columns, arithmetic/functions beyond the existing expression language,
CASE/COALESCE, subqueries, joins, cross-table migration, UNIQUE or multicolumn
indexes, LSM/partitioned/imported migration, and online/resumable migration.
