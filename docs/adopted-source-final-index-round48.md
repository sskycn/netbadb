# Terminal adopted-source final indexes (Round 48)

Starting SHA: `1e760f70914a70c70003bd130cceab6188ceb8a6`.
Round 48 productionizes the selected Round 47 Candidate A. One managed Single
Heap transaction must first perform ordinary DML and enter AdoptedSourceRefining.
It may then declare single-column non-unique final CREATE/DROP INDEX. The first
accepted index statement freezes the final TableDef and enters the explicit
Core-private AdoptedSourceIndexFinalizing state. Further same-table index DDL
composes before one physical finalization. This does not change non-adopted
DML→index transactional fallback or permit pending index DDL→later adoption.

```sql
BEGIN;
UPDATE users SET legacy = 'updated1' WHERE id = 1;
ALTER TABLE users ADD COLUMN marker TEXT;
CREATE INDEX users_marker_idx ON users(marker);
COMMIT;
```

Nullable Cnew is a logical index target before S2 exists. The final projection
synthesizes NULL; physical construction happens only during finalization.

## Production implementation and authority

Production changes are confined to Core
[`schema_composition.rs`](../crates/netbadb-core/src/schema_composition.rs) and
[`lib.rs`](../crates/netbadb-core/src/lib.rs). The new state owns the same
AdoptedSourceTransaction, retaining T/V/F/S1/P1, locator, catalog identity and
captured source index-definition digest. Private adopted_source helpers cover
both phases; plan returns adopted.logical, is_started is true, and is_sealed is
true for the terminal phase. All later ALTER variants return the existing
SchemaMutationAfterMaterialization. Table relational execution returns
MigrationDataAccessAfterRefinement; PostgreSQL maps both to 25000, followed by
25P02 for subsequent commands in the failed explicit transaction.

CREATE ordering:

1. Validate transaction ownership/state and prove the exact single adopted T.
2. Validate the **original** prepared current-overlay T/V/F and ColumnId.
   Stale IF NOT EXISTS fails before duplicate/no-op handling.
3. Check name and column uniqueness, action/reservation limits and IndexId
   exhaustion through shared logical CREATE machinery.
4. Construct the reservation's canonical final version. The fingerprint is
   already the exact final TableDef fingerprint checked in step 2.
5. Durably reserve tag24, then mutate only logical inventory and enter the
   terminal phase. No allocation occurs during prepare.

The prepared object is never changed or rebound by name. A private
IndexCompositionContext separates Ordinary and AdoptedFinal authority while
sharing duplicate, reservation and logical inventory implementation. Canonical
TableDef equality uses Vbase/Fbase; effective TableDef uses Vbase+1/final F.
Only table version normalizes at acceptance: next_column_id and earlier ADD→DROP
burns remain intact. IndexId burns are independently durable in tag24.

A successful IF NOT EXISTS returning Unchanged also closes the schema phase,
without reserving an ID. A pre-reservation error preserves the provisional
lineage, writer, journal and refining state, allowing native ALTER retry.
RecoveryRequired persistence errors invoke the existing rollback-required
transition; no provisional-state restoration hides a durable failure.

DROP validates the same adopted T and exact logical I, then shares the
plan-level apply_index_drop_to_plan helper with ordinary composition. It does
not swap the transaction through Composing and reserves no ID. The first
successful DROP canonicalizes lineage and closes ALTER. Logical bindings and
logical_index immediately expose newly created indexes, enabling subsequent
DROP, duplicates and IF NOT EXISTS without a planner access path.

## Frozen digest and hybrid finalization

Final CREATE/DROP Execute never changes S1 physical inventory. The captured
source digest stays equal before CREATE, after logical CREATE and after logical
DROP. The extended production finalize_adopted_source is the shared hybrid
finalizer for both adopted states; no independent test finalizer remains.

Before materialization, revalidate_adopted_source_authority checks ownership,
catalog incarnation/committed snapshot, generation/epoch, singleton participant
and write-participant sets, exact P1, Single placement, managed Heap descriptor,
T/base V/F/locator, registry identity and physical source index digest.
Perturbing captured authority fails before tag25 or physical changes.

| Final truth | tag25 | S2 | ΔV | ΔG | Δepoch | Δruntime |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| TableDef effective | RewriteHeap | 1 | 1 | 1 | 1 | 1 |
| TableDef base, active indexes changed | InPlaceIndexDelta | 0 | 0 | 0 | 0 | 1 |
| TableDef and active indexes base | absent | 0 | 0 | 0 | 0 | 0 |

**Table effective:** existing AdoptedSourceBackfill creates real tag25,
mandatory tag35, StageResourceIntent, exactly one S2, one transaction-visible
RowProjection pass, complete final indexes, tag34, prepared NBSC and S1/S2 CORD.
No S3 or repeated per-index materialization occurs. Surviving ColumnIds and
undropped IndexIds remain stable; a renamed column keeps its original ID.

**Index-only:** after consuming the source authority proof, Ordinary creates one
honest InPlaceIndexDelta. Existing transaction.with_write_storage(S1, …) reuses
P1 for exact drops and transaction-visible reserved-ID builds. Tests assert
identical physical transaction ID before/after and one participant/write
participant. No target snapshot, schema reference, S2, tag35, stage, tag34 or
prepared NBSC exists. There are zero projection passes/rows; creating an index
still scans S1's transaction-visible rows. The authorized physical delta changes
the source digest, and the consumed adopted state is never revalidated afterward.
The existing one-participant commit_schema_decision reference=None path commits
DML and index delta atomically and publishes runtime catalog/index truth once.

**Global no-op:** SealedNoEffectiveChange commits DML on S1 with no tag25,
physical index publication or runtime revision. CREATE→DROP burns accepted Inew
but returns active inventory to base. DROP→CREATE same name instead replaces
Iold with fresh Inew and is effective even on the same column/name. A prepared
DROP for Iold returns UndefinedIndex (42704) and cannot remove Inew. Multiple
CREATE operations reserve consecutive IDs and finalize the complete inventory
once. One active index per ColumnId remains required.

## Executable evidence

The retained
[`adopted_source_index_finalization_audit_tests.rs`](../crates/netbadb-core/src/adopted_source_index_finalization_audit_tests.rs)
now has only a fixture wrapper around production APIs. It retains the original
Round 47 architecture, corruption and cost evidence; no FinalIndexPhase or
audit_apply_adopted_source_create_index correctness implementation survives.
Round 44/46 tests were updated from adopted index rejection to terminal-phase
acceptance while preserving their original relational and Cnew nullability gates.

The base fixture contains rows 1/2/3. Ordinary DML updates 1, inserts 4 and
deletes 2 before adoption. Direct physical BTree lookup verifies updated1,
old3 and inserted4, with no old2 entry. The Cnew NULL tree returns exactly
1/3/4, also after each reopen; ANALYZE null_count is 3. IndexCatalog/IndexSpec
validation occurs through physical construction and catalog-only reopen.

The publication matrix covers ADD+Cnew CREATE, renamed surviving CREATE,
rename-back, SET→DROP and ADD→DROP + CREATE. Rollback covers schema dirty,
index-only CREATE/DROP, CREATE→DROP, same-name replacement and multiple CREATE,
both before and after materialization. It checks base rows/schema/index identity,
writer release, durable ID floors and absence of surviving stages. Physical
observations record pre-finalization and pre-commit footprint: table-effective
copies once/three rows to one S2; index-only copies zero rows and retains S1.
Byte counts are observations, never correctness equality assertions.

Additional first-statement tests pin stale/stale IF NOT EXISTS, duplicates,
wrong/already-indexed ColumnId, cross-table, action/reservation/IndexId limits,
and undefined DROP:
journal, lineage and writer remain unchanged and native ALTER can continue.
An injected uncertain tag24 sync transitions to RollbackRequiredLogical,
rejects ALTER/commit and recovers base rows with the burn across three opens.
Accepted unchanged IF NOT EXISTS freezes every ALTER variant; SELECT, INSERT,
UPDATE and DELETE remain closed. Source-authority perturbation tests cover
storage, P1, V, F, locator, generation, epoch and digest before mutation.

## Crash and recovery evidence

| Boundary | Result across three reopens |
| --- | --- |
| adopted-final-index-reservation-durable | P1 loser, base rows/indexes/schema, Inew burned, no S2 |
| composition-intent-durable | real index-only intent loser, DML/index rollback, burn retained |
| adopted-index-delta-first-tree-built | first of multiple trees built; whole P1 loses |
| composition-after-index-delta-1 | complete physical delta loses before decision |
| after-prepare-1 / after-all-prepares | one-P1 index-only loser |
| after-durable-decision / after-commit-1 / after-all-commits, index-only | same-S1 winner; V/G/epoch unchanged; index truth reconstructed |
| tag35 / stage intent / mid-copy, rewrite | existing pre-decision rewrite loser |
| after-durable-decision / after-commit-1 normal and reversed / after-all-commits, rewrite | both-prepared, source-first, target-first, both-committed winner on one S2 |

The first-tree hook remains test-build-only in Core, with no public/storage
runtime feature. Index-only recovery has one participant and no invented target
ordering. Recovery uses existing durable tag24/tag25, tag35 only for rewrites,
WAL/status, CORD and catalog/index evidence. It does not parse or replay SQL,
DDL, adoption, IndexId allocation, lineage construction or RowProjection.
Tag24 strict ordering/successor/T/V/F/reservation checks and truncated/corrupt
journal rejection remain unchanged; identity/fake tag25 stays rejected.

## PostgreSQL evidence and retained fixtures

[`test-adopted-source-final-index-sql.py`](../scripts/test-adopted-source-final-index-sql.py)
uses `/opt/local/lib/pgsql/bin/psql` 17.11 and `/opt/local/lib/icu/lib`.
Simple Query covers Cnew, renamed survivor, no-op CREATE/DROP, CREATE→DROP,
same-name replacement, multiple DDL, own UPDATE/INSERT/DELETE, rollback and
terminal ALTER/relational errors. Every fixture verifies schema/index identities,
allocator placement and rows through three reopens, with manifest bytes unchanged.

psql's unnamed extended query runs through Parse/Bind/Describe/Execute. Named
prepared stale CREATE and old exact DROP are additionally tested using libpq
from the same installation, since psql 17 lacks a named Parse command. Server
unit tests snapshot files before/after Parse/Bind/Describe, verify allocation
only on Execute, exact command tags, stale/no-burn and 42704/25000/25P02.
Production PostgreSQL authorization and adapter code are untouched. Prior DML
permission does not grant schema/index permission.

The Round 47 script delegates to these production positives. Its old CREATE/DROP
negative assertions now target terminal ALTER/relational boundaries. Retained
Round 39/42 DROP-first, Round 44 adoption, Round 46 surviving nullability,
ordinary composition and generic transactional index fallback remain tested.
Cnew SET retains 25000, Cnew already-nullable DROP 58000, followed by 25P02.

## Compatibility and deferred work

No parser/HIR/compiler flags, public API, dependency, decoder or format changes:
Canonical Schema v1; NBSC/NBSM; NBSJ v1 tags 1–35; CORD v2; Heap/Page; NBMV;
WAL/status; IndexCatalog v9; BTree v3; PartitionCatalog/LSM; Protocol v1;
PG framing v3; Manifest v4; SDK Schema Spec v1 and inspection formats remain intact.
No tag33, fake tag25 or new persistent tag is introduced.

Round 49 deferred all later ALTER/relational DML. Round 50 now permits its
bounded late-column-only deferred UPDATE and projected SET NOT NULL before this
final-index phase; once the first final index action is accepted, UPDATE remains
closed. DROP INDEX→DROP COLUMN in this adopted phase, generic index-first adoption, UNIQUE/multicolumn,
constraints/CASCADE, defaults/generated/type conversion/USING, cross-table,
LSM/partitioned/imported/bootstrap, savepoints, online/resumable migration,
participant detach, automatic GC and new formats. Round 42 SourceIndexFinalizing
remains a distinct DROP-first lifecycle.

## Pre-merge design answers

- First stale CREATE can burn I or failed first CREATE can terminalize: **no**.
- Successful unchanged IF NOT EXISTS can terminalize: **yes**, without a burn.
- Index Execute can mutate S1: **no**. Cnew is logical until finalization.
- Captured digest may drift before finalization: **no**; authorized physical
  delta changes it only after full revalidation and consumes its old authority.
- Dirty-table finalization can allocate S3 or index-only can allocate S2: **no**.
- CREATE→DROP can publish tag25: **no**; it can burn I: **yes**.
- DROP→CREATE same name can reuse Iold: **no**.
- Later ALTER or repeated per-CREATE materialization: **no**.
- Index-only can bump V/G/epoch: **no**; runtime advances exactly once: **yes**.
- Recovery can replay SQL, use tag33, or require a new NBSJ tag: **no**.

## Validation

Commands and actual final outcomes are recorded in the delivery report. Required
commands are fmt check, workspace all-target/all-feature check and Clippy with
`-D warnings`, offline all-feature workspace tests, Go tests, generated SDK check,
all retained PostgreSQL fixtures plus Round 48, thirteen copied-corpus fuzz
smokes with `-runs=1000 -seed=48`, and git diff --check.

Rust 1.85.0 check was attempted without feature gates. It retains the known
unrelated planner E0658 let-chain blocker at lines 892 and 904; this task does
not change that planner or claim MSRV success.

Additional retained-client baseline blockers were reproduced on the exact
starting SHA in an isolated detached checkout:

- `test-sql-alter-table.py`: indexed-column SET NOT NULL during its older
  backfill sequence returns `0A000` (column 2 is indexed); fixture teardown then
  reports the client-created table absent because the script stopped early.
- `test-postgresql-orm.py --dsn …`: DROP TABLE teams targets an imported/bootstrap
  Heap and returns `0A000` (runtime table mutation supports only a single Heap).

Neither unrelated production behavior nor retained assertion was changed.
All specifically required Round 39/42/44/46 and Round 48 fixtures pass. The
older CREATE TABLE, staged nullability, clone/backfill audit, psql describe and
standalone Alembic index fixtures also pass. Python client dependencies were
installed only into a temporary virtual environment from the pinned repository
requirements; no dependency manifest changed.

Final validation results:

| Command / suite | Result |
| --- | --- |
| `cargo fmt --all -- --check` | passed |
| `cargo check --workspace --all-targets --all-features` | passed |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | passed |
| `cargo test --workspace --all-features --offline` | passed: 1,148 tests, zero failures, two existing ignored tests |
| Follow-up first-failure, uncertain-sync and physical-P1 assertions | passed in targeted Core runs |
| `cargo +1.85.0 check --workspace --all-targets --all-features --offline` | known planner E0658 blocker at 892/904 |
| `go test ./...` from sdk/go | passed |
| `./scripts/check-generated-sdk.sh` | passed |
| Required Round 39/42/44/46/48 real-client fixtures | passed, including three reopens |
| Older staged-nullability, clone, psql, CREATE TABLE, controlled-backfill, standalone Alembic fixtures | passed |
| Older general ALTER and general ORM fixtures | the two starting-SHA failures detailed above |
| All thirteen fuzz targets, 1,000 runs each, seed 48 | passed; copied corpora/artifacts removed |
| `git diff --check`, changed Markdown paths, changed Python syntax | passed |
