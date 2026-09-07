# Production terminal structural refinement (Round 56)

Round 56 productionizes the frozen-evaluation-schema design selected by the
[Round 55 audit](deferred-terminal-structural-round55.md). A bounded adopted
managed Single-Heap migration may finish its deferred value program and then
compose terminal same-table `DROP COLUMN`, `RENAME COLUMN`, and `RENAME TABLE`
operations before its one final Heap replacement.

This round enabled an atomic unindexed shadow-column replacement. The later
[Round 58](deferred-index-evacuation-round58.md) extends the same terminal phase
with bounded logical evacuation for an indexed source; neither round implements
general online ALTER or type conversion.

## Production state and routing

The production lifecycle is:

```text
AdoptedSourceRefining
  -> first accepted deferred UPDATE
AdoptedSourceBackfilling
  -> first successful terminal DROP/RENAME
AdoptedSourceFinalRefining
  -> first final CREATE/DROP INDEX
AdoptedSourceIndexFinalizing
  -> commit/finalization
```

`AdoptedSourceFinalRefining` is an explicit state, not a seal flag. Entry is
possible only from `AdoptedSourceBackfilling` with a nonempty deferred program,
a frozen evaluation schema, the same adopted TableId, and valid S1/P1 source
authority. The existing logical ALTER composer performs dependency validation,
schema validation, action evidence, and overlay mutation. The state changes
only after that work succeeds, so an indexed-column, primary-key, stale-target,
or duplicate-name failure leaves Backfilling open for repair.

FinalRefining permits further same-table DROP/RENAME operations. It rejects ADD,
SET/DROP NOT NULL, deferred UPDATE, SELECT/INSERT/DELETE, base writes, and
cross-table data access. IndexFinalizing remains terminal for structural ALTER.
An old prepared UPDATE or ALTER validates its original dependency first and
therefore becomes stale after a terminal schema change; a fresh relational
statement receives `MigrationDataAccessAfterRefinement` (`25000`).

## E, F, and physical authority

The first accepted deferred UPDATE, including one affecting zero rows, freezes
EvaluationSchema E. Every cached evaluation and assignment position remains
relative to E. Terminal ALTER never rebinds or rewrites the program.

E compatibility is defined only by TableId, ordered ColumnIds, and semantic
types. Names do not confer identity, and E nullability is not final constraint
authority. The current private overlay is final schema F and supplies the final
constraints.

Finalization remains:

```text
transaction-visible authoritative S1/P1
  -> RowProjection into frozen E
  -> ordered deferred program over E
  -> checked ColumnId-only FinalOutputProjection(E -> F)
  -> F constraint validation
  -> exactly one S2
  -> final index inventory
```

A source or late intermediate dropped from F remains available only as an E
evaluation dependency. It is not exposed by schema inspection, written to S2,
or indexed. Renaming shadow C4 to the old logical name changes only the name:
C4 remains the final identity and dropped C2 is not resurrected. Rename-only
E-to-F mappings retain the identity projection fast path; subset projection
allocates one output row and copies exactly the final width.

The original S1 TableDef, row width, and physical indexes remain unchanged
until commit. Effective finalization performs one source pass and allocates one
S2; there is no S3, sidecar, hidden physical column, or early replacement.

## Index, evidence, and recovery boundaries

Terminal DROP accepts only a present, non-primary-key column with no current
logical index. The existing `PrimaryKeyColumn`, `IndexedColumn`, and
`ColumnNotFound` errors remain authoritative. Round 56 neither drops nor
retargets an index implicitly. A final index resolves the current overlay, so
an index created on the renamed logical `legacy` binds the shadow ColumnId.

Deferred action expressions, positions, observations, and the action domain
`NetbaDB deferred backfill action v1\0` are unchanged. The Round 50 and Round
54 action goldens remain unchanged. Existing ALTER evidence, final snapshot
evidence, tag 25 action/snapshot binding, and tag 35 clone-plan binding
distinguish keep-both and swap outcomes.

E and the deferred program remain transaction-local. Before the durable CORD
winner decision, recovery selects S1 and the base schema. After the decision,
recovery selects the already materialized F/S2 result. Recovery never decodes
E, parses SQL, binds parameters, or replays expressions.

## Phase 3A, Change Stream, and Columnar

In global-visibility mode every private ADD, deferred UPDATE, nullability,
DROP/RENAME, and index step leaves the published `DatabaseSnapshot` unchanged.
A successful commit publishes exactly one next gap-free `DatabaseCommitSeq`
whose visibility vector contains S2 and excludes retired S1. Rollback and
replacement admission failure publish no G. Existing CORD v3 sequenced
decision/Complete recovery supplies the post-decision result; no historical
schema snapshot is introduced.

Round 52 remains authoritative: an Enabled or Unavailable S1 Change Stream
blocks replacement before target allocation and publication. Disabled sources
may replace, after which enabling and anchoring S2 remains explicit. Columnar
and Phase 2E maintenance remain derived and cannot supply or finalize migration
rows.

## PostgreSQL and validation coverage

The PostgreSQL adapter contains no special evaluator. Production Simple Query
coverage verifies UPDATE/INSERT/DELETE, ADD, two deferred UPDATE counts,
pre-terminal SET NOT NULL, terminal DROP/RENAME, final CREATE INDEX, COMMIT,
final C4 identity and values across three opens. Extended coverage verifies
stale prepared UPDATE and DDL ordering. Fresh post-terminal data access remains
`25000`; indexed DROP keeps the existing dependent-object `2BP01` mapping.
The real PostgreSQL 17.11 acceptance is
`python3 scripts/test-deferred-terminal-structural-sql.py`; it covers the full
Simple Query swap, table rename, rollback, indexed-source rejection, and three
catalog-only reopens per fixture using `/opt/local/lib/pgsql` and the required
ICU libraries.

Core coverage additionally proves a dropped late intermediate, E layout and
action-position stability, one source pass/one S2, direct physical C4 BTree
lookups, no-index commit, rollback, Change Stream blocking, maintenance
isolation, deterministic E-to-F cost, global single-publication behavior, and
the local crash matrix.

## Persistent compatibility and non-goals

Round 56 changes no Canonical Schema, NBSC/NBSM, NBSJ v1 tags 1--35, NBCO v1,
CORD v1/v2 legacy or CORD v3 global-visibility record, Heap/Page/WAL/status,
NBCL, NBCM v1/v2/v3, NBCS v1/v2/v3, NBCD v1/v2, NBPC, IndexCatalog/BTree,
Partition/LSM, Protocol v1, PostgreSQL framing, deployment Manifest, SDK Schema
Spec, generated code, or inspection format.

There is no persistent E, tag 36, recovery branch for E, hidden relation,
sidecar, early S2, S3, ADD after E freeze, post-terminal nullability, ALTER TYPE
or USING, implicit index mutation, historical schema snapshot, general
post-refinement DML, cross-table migration, imported/partitioned/LSM migration,
or online/resumable migration.
