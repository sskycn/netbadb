# Production terminal index evacuation (Round 58)

Round 58 productionizes the terminal logical index evacuation selected and
proved by the [Round 57 audit](deferred-index-evacuation-round57.md). A bounded
managed Single-Heap migration may now replace an indexed column atomically once
its deferred value program has frozen EvaluationSchema E.

## Production routing

The production state machine is:

```text
AdoptedSourceRefining
  -> first deferred UPDATE: AdoptedSourceBackfilling
AdoptedSourceBackfilling
  -> first effective DROP INDEX: AdoptedSourceFinalRefining
AdoptedSourceFinalRefining
  -> further effective DROP INDEX: AdoptedSourceFinalRefining
  -> terminal DROP/RENAME: AdoptedSourceFinalRefining
  -> first CREATE INDEX: AdoptedSourceIndexFinalizing
```

`compose_drop_index_in` selects the explicit `TerminalEvacuation` context only
for Backfilling and FinalRefining. Refining and IndexFinalizing keep the Round
48 `Finalizing` behavior. The former test-only execution entry is removed;
native SQL, prepared DDL and PostgreSQL all use the ordinary production path.

Before the first evacuation, Core requires a nonempty deferred program, frozen
E, the same adopted TableId and valid S1/P1 authority. It revalidates the
captured source proof before calling the shared `apply_index_drop_to_plan`.
Only a `Dropped` result changes phase. `DROP INDEX IF EXISTS` with no target and
all validation failures leave the phase, program and action evidence unchanged.

## Three inventories and identity

An accepted logical evacuation intentionally creates three views:

```text
committed public:       I1 -> C2
transaction final:      I1 absent
physical S1:            I1 -> C2
```

The S1 BTree, index floor and captured `source_index_digest` remain unchanged.
The drop writes no index page or index WAL and consumes no IndexId. Transaction
name binding follows the private logical inventory, so the old name may be
reused. A later CREATE reserves fresh I2 and binds it to shadow C4; I1 is never
retargeted. Rollback therefore needs no physical restoration, while an accepted
I2 reservation remains burned under the existing allocator theorem.

## Finalization and controls

For an effective C2-to-C4 replacement the existing finalizer performs:

```text
authoritative S1/P1
  -> frozen E
  -> ordered deferred program
  -> final schema F
  -> exactly one S2 and one source pass
  -> only the final index inventory
```

The final S2 contains C4 named `legacy` and fresh I2/C4; C2 and I1 are absent.
An unrelated index keeps its original IndexId and is rebuilt on S2. There is no
S3. If the final TableDef equals the base but index identity differs, the
existing `InPlaceIndexDelta(S1)` path applies the change with no target, stage,
copy pass or StorageId allocation.

The same-S1 index-only path remains legal with an Enabled Change Stream and
does not change its generation, frontier or data version after zero-row DML. A
real replacement remains blocked by the Round 52
`ActiveChangeStreamBlocksReplacement` guard before target allocation, staging,
tag 35, a sequenced decision or publication. Logical private composition may
precede that commit-time rejection; rollback exposes the untouched S1/I1.

An active indexed terminal migration also keeps incremental Columnar
maintenance `Busy`: inspection cannot select an advance and `maintenance_step`
cannot complete or modify the private migration. Finalization continues to read
only authoritative S1/P1, never a projection.

## Recovery, global visibility and sync

The existing action evidence order binds deferred actions, I1 drop, C2 drop, C4
rename and I2 create into the tag 25 action/snapshot digests and tag 35 clone
plan. No new digest domain or persistent state is needed.

The production crash matrix covers logical evacuation, terminal structure,
index reservation, tag 25/tag 35, staging, copy, final index build, prepare,
durable Decision, normal/reverse participant commit and Complete/publication.
Every case converges across three opens. Before durable CORD Decision, S1/C2/I1
wins. At and after Decision, recovery finishes S2/C4/I2 without replaying SQL or
DROP INDEX.

With global visibility, all intermediate work remains private and success
publishes exactly one next gap-free G whose vector contains S2 and excludes S1.
This remains a structural transaction, so Phase 3B does not defer its Complete:

```text
Decision sync -> physical/schema completion -> Complete sync -> publication
```

Success leaves `pending_complete_count == 0`. If an earlier pure-data G1 has a
pending Complete, the G2 Decision sync first checkpoints G1, then the structural
G2 follows the conservative foreground path. Recovery preserves strict G1/G2
ordering around the G2 Decision.

## PostgreSQL and compatibility

PostgreSQL contains no evacuation-specific evaluator. Simple Query uses the
Core production state machine and returns the ordinary `DROP INDEX`, `ALTER
TABLE`, `CREATE INDEX` and `COMMIT` tags. Tests cover same-name recreation,
three catalog-only reopens, `IF EXISTS` no-op, post-evacuation `25000` guards,
rollback, and the Enabled-stream commit `0A000`/failed-transaction behavior.
Real psql acceptance is
`python3 scripts/test-deferred-index-evacuation-round58-sql.py` using the local
PostgreSQL 17.11 and ICU installation.

Round 58 changes no Canonical Schema, NBSJ tags 1--35, NBSC/NBSM, NBCO/CORD
v1--v3, Heap/Page/WAL/status, IndexCatalog/BTree, NBCL,
NBCM/NBCS/NBCD/NBPC, Partition/LSM, Protocol/PostgreSQL framing, deployment
manifest, SDK or inspection format. It persists no evacuation context, reason
or index list.

The bounded scope does not include DROP before frozen E, terminal evacuation in
Refining, reopening IndexFinalizing, physical online index evacuation, IndexId
retarget/reuse, unique or multicolumn expansion, ALTER TYPE/USING,
post-terminal DML/nullability, imported/partitioned/LSM migration, historical
schema snapshots, or resumable migration.

Round 59 audits the previously deferred physical-conversion case and selects a
typed Cast into a fresh shadow ColumnId followed by this unchanged evacuation
and recreation sequence. Production cross-physical SQL remains closed in that
round; see
[`deferred-type-conversion-round59.md`](deferred-type-conversion-round59.md).
