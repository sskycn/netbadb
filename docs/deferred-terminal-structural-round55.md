# Post-backfill terminal structural refinement audit (Round 55)

> Round 56 subsequently productionized the selected frozen-E/final-F design.
> See [production terminal structural refinement](deferred-terminal-structural-round56.md).

Round 55 is an architecture audit with executable, test-only prototypes. It
does **not** enable terminal structural ALTER in production. The production
gate still returns `SchemaMutationAfterMaterialization`; PostgreSQL reports
SQLSTATE `25000` and then `25P02` for subsequent commands in the aborted
transaction.

The audit started at `c0880993989fc95fe92702a6a998a0f21eec64be`. The Round 54
production commit remains `40475bb37288ca7c13cba0f6c96dec268f86d07b`.

## Decision

Candidate A is selected for a bounded Round 56 implementation:

```text
transaction-visible S1/P1 row
  -> RowProjection(base S1 -> frozen EvaluationSchema E)
  -> ordered DeferredBackfillProgram over E
  -> FinalOutputProjection(E -> final TableDef F) by ColumnId
  -> validate only F constraints
  -> exactly one S2 insert
  -> final BTree inventory
```

The first successfully accepted deferred UPDATE, including an UPDATE that
affects zero rows, freezes E. E contains the current target columns in their
current order: surviving base columns plus visible, durably reserved late
columns. It retains `TableDef` metadata as the smallest reuse of an existing
validated schema primitive, but compatibility and mapping authority are only
TableId, ordered ColumnIds, and semantic types. Names are diagnostic;
nullability in E is not final constraint authority.

Each existing `EvaluationLayout.target_positions` and
`DeferredAssignment.target_position` therefore remains relative to E. A later
rename does not rewrite it. A nullability refinement may change current schema
metadata without moving a slot. The final F alone supplies publication
nullability constraints.

`FinalOutputProjection` validates that every F ColumnId exists in E with the
same semantic type, derives a checked E ordinal from that exact identity, and
emits values in F column order. Rename changes only F's name. DROP means F no
longer requests the ColumnId. There is no synthesized fallback for a column
added after E freezes.

For the current Round 54 production case E and F have the same identities and
order, so the projection recognizes the identity case and returns the existing
row allocation. The E-to-F copy is used only by the test-only diverged-F
prototype.

## Executable shadow-swap evidence

The prototype uses the real SQL parser/compiler ALTER identities, the real
transaction-visible S1/P1 reader, the production VirtualRow deferred program,
the production schema composer, and the real S2/index materializer. The only
test-only capability is an explicit typed terminal carrier that bypasses the
production gate for DROP/RENAME.

The mandatory fixture proves:

- E is `[C1 id, C2 legacy, C3 flag, C4 shadow]`;
- ordinary own UPDATE/INSERT/DELETE produce rows 1, 3, and 4;
- `C4 = C2` affects two rows and the repair affects one row;
- C4 becomes NOT NULL before the terminal phase;
- terminal DROP removes C2 from F while it remains readable in E;
- terminal RENAME makes logical `legacy` identify C4, never C2;
- final F is `[C1 id, C3 flag, C4 legacy NOT NULL]`;
- one source pass copies three rows into one S2 and allocates no S3;
- the staged and reopened physical row width is exactly three;
- the final BTree definition binds C4 and direct point lookups return rows 1,
  3, and 4;
- three close/reopen cycles converge on the same F/S2 result;
- an existing Columnar projection is no longer Fresh after replacement.

A second executable case freezes
`[C1, C2 legacy, C3, C4 marker, C5 normalized]`, evaluates
`C4 = C2` followed by `C5 = C4`, drops both C2 and C4, and renames C5 to
`legacy`. C2 and the late intermediate C4 remain evaluation-only dependencies;
neither is stored in final S2. Round 56 may therefore support dropping a late
intermediate when it has no primary-key or logical-index dependency.

Terminal RENAME TABLE also composes safely: the prototype renames `users` to
`people`, performs the same ColumnId-based column swap, builds the final C4
index, and commits the expected rows.

## Program and durable binding

The action domain remains exactly
`NetbaDB deferred backfill action v1\0`. Terminal ALTER does not mutate action
expressions, positions, observations, semantic digests, or bytes. The paired
experiment uses the same two-action program for a keep-both case and a
DROP/RENAME case and proves:

- per-action semantic digests are equal;
- whole `action_digest` differs because existing terminal ALTER evidence is
  appended;
- final snapshot digests differ because F differs;
- tag-35 `clone_plan_digest`, which binds action and snapshot digests, differs.

Existing tag-25 schema/index intent plus tag-35 source-backfill intent therefore
bind both how values were produced and which identities/names survive.

E is transaction-local metadata only. It is not a Heap, sidecar, participant,
catalog schema, Columnar projection, MVCC relation, or persistent AST. Before
CORD, a crash loses program/E/F and S1 wins. After CORD, recovery uses the
already-materialized final S2 and never reconstructs E or replays expressions.
No NBSJ tag 36 or new codec is necessary.

## Crash and rollback evidence

Subprocess tests cover terminal DROP, terminal RENAME, before tag 25, durable
tag 25, durable tag 35, stage intent, first stage file, mid-copy, final index,
before/after prepares, durable CORD decision, partial commits in both
participant orders, and all participants committed. Every case is reopened
three times.

All pre-CORD cases recover the original S1, original C2 schema, and committed
base rows. Durable-decision and later cases recover exactly F/S2/C4. Stage files
are reclaimed. Recovery never needs E or the deferred program.

Explicit rollback after terminal DROP/RENAME restores the original C2 schema,
discards C4/E/F, retains S1, writes no S2, and follows existing identity-burn
rules.

## Change Stream, Columnar, and maintenance

The Round 52 guard remains authoritative. Enabled and Unavailable source
streams allow the logical test prototype to reach terminal F, but final
RewriteHeap is rejected before replacement StorageId allocation. Rollback
leaves the old stream/storage authority intact. An explicitly Disabled stream
completes the positive full fixture.

Deferred evaluation reads authoritative S1/P1, never NBCS/NBCD/lazy Columnar.
Replacement makes the old projection stale under existing rules. No NBCM,
NBCS, or NBCD behavior changes.

An active terminal migration makes Phase 2E candidates Busy. An explicit
`maintenance_step` returns `NoWork` and leaves the projection manifest byte-for-
byte unchanged. DML, DDL, finalization, and commit do not invoke maintenance;
maintenance is not the finalizer.

## Indexed-source boundary

Round 56 should support **unindexed old source columns only**. An executable
fixture with an index on C2 rejects terminal DROP with `IndexedColumn(C2)` and
leaves the index bound to C2.

Candidate I2 would require DROP INDEX to remain logical without sealing while
CREATE INDEX still seals, which is a separate state-machine change. Candidate
I3 imposes a surprising order dependency before the first deferred UPDATE.
Round 55 therefore chooses I1/I4: do not silently drop an index, do not retarget
its IndexId to C4, and defer the full indexed-old swap.

## Candidate comparison

| Criterion | A Frozen E + FinalProjection | B Dependency row | C Rebind actions | D Early S2 | E Two txns |
| --- | --- | --- | --- | --- | --- |
| DROP program source dependency | yes | yes | no without hidden special case | yes | yes |
| RENAME shadow to old name | yes, by ColumnId | yes | fragile | yes | yes |
| Stable action positions | yes | requires growing slot plan | no | yes | yes |
| Exactly one S2 | yes | yes | yes if repaired | no; risks S3 | one per transaction |
| S1 pre-final authority | yes | yes | yes | no | yes per transaction |
| Final schema atomic | yes | yes | yes | yes | no |
| Memory O(rows) | no | no | no | potentially | no |
| New persistent format | no | no | no | no, but new lifecycle | no |
| Recovery changes | no | no | no | substantial | no |
| Digest compatibility | unchanged | new layout bookkeeping | rewrite risk | lifecycle changes | separate decisions |
| Implementation risk | low | medium | high | high | low |
| Indexed old column | deferred | deferred | unresolved | unresolved | possible but non-atomic |

Candidate A is the unique selection. B works in principle but adds dependency
set growth, slot construction, typing, and diagnostic bookkeeping without a
demonstrated memory benefit. C fails the dropped-source case unless it
recreates A/B hidden state. D breaks the one-final-S2 and no-early-target
theorems. E remains the operational fallback but exposes an intermediate schema
and is not atomic. Candidate F (persistent hidden columns/program state) is
unnecessary and would add formats and recovery work.

## Exact Round 56 boundary

The proposed state model is:

```text
AdoptedSourceRefining
  -> first accepted deferred UPDATE: AdoptedSourceBackfilling + freeze E
  -> first terminal DROP/RENAME: AdoptedSourceFinalRefining + seal program
  -> first final CREATE INDEX: AdoptedSourceIndexFinalizing
```

Backfilling continues to allow the existing compatible late-column
SET/DROP NOT NULL validation before E and F diverge. FinalRefining should allow
only:

- DROP COLUMN on the same table when the column is non-PK and has no current
  logical index;
- RENAME COLUMN;
- RENAME TABLE;
- further terminal DROP/RENAME operations with ordinary prepared T/V/F
  dependency checks.

FinalRefining closes deferred UPDATE, ADD COLUMN, further nullability changes,
SELECT, INSERT, DELETE, base-column writes, cross-table access, type conversion,
and implicit index removal/retargeting. A prepared UPDATE from before terminal
DROP becomes `StalePreparedStatement`; a freshly prepared UPDATE is rejected as
`MigrationDataAccessAfterRefinement`. A prepared pre-DROP terminal ALTER fails
its exact dependency as `StaleSchemaDependency`.

## Cost observation

The test probe performs 10,000 non-identity E-to-F projections in an unoptimized
test build. It reports one output allocation per row and exactly
`rows * final_width` copied values:

| E width | Dropped | F width | Total time | Allocations | Values copied |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 4 | 1 | 3 | 10.696 ms | 10,000 | 30,000 |
| 16 | 4 | 12 | 16.330 ms | 10,000 | 120,000 |
| 64 | 16 | 48 | 59.446 ms | 10,000 | 480,000 |
| 128 | 32 | 96 | 95.938 ms | 10,000 | 960,000 |

There is no timing assertion. Finalization remains
`O(rows * actions) + O(rows * final column count)` with O(row width + program
metadata) memory, not O(rows). Production Round 54 E=F uses the identity path.

## Compatibility and non-goals

The following remain byte-for-byte or semantically unchanged: Canonical Schema;
NBSC/NBSM; NBSJ tags 1--35; CORD; Heap/Page/WAL/status; NBCL; NBCM v1/v2/v3;
NBCS v1/v2/v3; NBCD v1/v2; NBPC; IndexCatalog/BTree; Partition/LSM;
Protocol/PG; manifests; SDK and inspection contracts.

Round 55 adds no tag, magic, version, decoder, manifest field, protocol field,
SDK surface, recovery record, persistent EvaluationSchema, hidden physical
column, sidecar Heap, early S2, S3, SELECT expansion, type conversion, general
DML expansion, or production terminal ALTER behavior.
