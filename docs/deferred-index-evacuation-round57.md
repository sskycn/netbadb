# Indexed shadow-column swap architecture audit (Round 57)

Round 57 starts from Phase 3B commit
`b0e255d5fb3b31525c21496107e5618981da4b99`; its Round 56 implementation and
proof ancestors are `5ac08a8` and `e460a17`. This round is an architecture
audit with a test-only executable carrier. It does **not** enable indexed
shadow-column swaps in production.

## Current blocker and selected design

Production currently routes every adopted-source `DROP INDEX` through
`apply_adopted_source_final_drop_index`. A successful logical drop calls the
shared `apply_index_drop_to_plan` and then unconditionally seals the state as
`AdoptedSourceIndexFinalizing`. The following `DROP COLUMN` consequently fails
with `SchemaMutationAfterMaterialization` (`25000` over PostgreSQL), and the
next command in the explicit failed transaction receives `25P02`.

Executable evidence selects **Candidate A: terminal logical evacuation**:

```text
AdoptedSourceBackfilling
  -- effective DROP INDEX --> AdoptedSourceFinalRefining

AdoptedSourceFinalRefining
  -- effective DROP INDEX --> AdoptedSourceFinalRefining
  -- DROP/RENAME -----------> AdoptedSourceFinalRefining
  -- first CREATE INDEX ----> AdoptedSourceIndexFinalizing
```

Only an effective same-table drop with a nonempty deferred program and frozen
EvaluationSchema E qualifies. Any index on that table qualifies; eligibility
does not predict whether its column will later be dropped. `DROP INDEX IF
EXISTS` returning `Unchanged` does not seal the program. `AdoptedSourceRefining`
is deliberately excluded, and existing production behavior outside the audit
carrier is unchanged.

The test carrier introduces an explicit `AdoptedIndexDropContext` rather than a
boolean. Production selects `Finalizing`. A `#[cfg(test)]` execution entry uses
the real parser/compiler target, exact prepared identity validation, shared
logical drop composer, adopted authority revalidation and production finalizer,
but selects `TerminalEvacuation`. There is no parallel logical index mutation
implementation and no transaction state swap merely to call the composer.

## Why logical evacuation is sound

Immediately after the audit drop there are three intentionally distinct
inventories:

```text
committed public inventory:     I1 -> C2
transaction final inventory:    I1 absent
physical S1 inventory:          I1 -> C2
```

The physical S1 BTree, its `IndexId`, column binding and point lookups remain
unchanged. The captured `source_index_digest` remains byte-identical, so
`revalidate_adopted_source_authority` succeeds without rewriting or updating
the captured proof. No S1 index page, index WAL, physical index floor or
participant delta is produced by the logical drop. DROP increments only the
existing logical index-action/evidence sequence and reserves no identity.

The transaction-visible name binding is derived from its logical inventory,
so removing I1 frees `users_legacy_idx` even though the committed physical tree
still has that name. A later CREATE through the existing allocator reserves a
fresh I2 and binds it to C4. It does not retarget I1. `CREATE INDEX IF NOT
EXISTS` is therefore `Created`, not `Unchanged`. Rollback restores no index:
S1/I1 was never removed. Accepted I2 reservations retain the existing burn
rule, while a drop without CREATE consumes no IndexId.

The full tested final truth is:

```text
S1: C1,C2,C3 + I1/C2              (unchanged until winner)
F:  C1,C3,C4 + I2/C4              (private transaction truth)
S2: C1,C3,C4 + I2/C4              (one final materialization)
```

Finalization performs one S1-to-S2 source pass, builds only the final index
inventory, and never copies or retargets I1. `updated1`, `missing`, and
`inserted4` point lookups resolve through the new physical C4 BTree. Exactly
one S2 is allocated and no S3 exists. A second unrelated I2/C3 is retained and
rebuilt while only I1/C2 is evacuated; the replacement then receives I3/C4.

Whole-migration evidence remains ordered through the existing `drop-index`
action bytes, action/snapshot digests, tag 25 intent and tag 35 clone-plan
digest. Same-name I1/C2 and I2/C4 are distinct because their stable IDs and
ColumnIds differ. No new digest domain or record is necessary.

## Table-noop and Change Stream controls

The executable table-noop control performs ADD shadow, a zero-row deferred
action, DROP shadow and logical DROP I1. The final TableDef equals the base but
the active index inventory differs. The existing finalizer naturally selects:

```text
InPlaceIndexDelta(S1)
source_copy_passes = 0
target = None
staged = empty
```

It does not force a replacement. This same-S1 index delta succeeds with an
Enabled Change Stream; generation, origin frontier and current data version are
unchanged when the preceding ordinary DML affects zero rows. DROP I1 followed
by an otherwise identical CREATE is still effective because the final identity
is I2, so it also produces an in-place delta rather than a global no-op.

For a real C2-to-C4 replacement, Enabled Change Stream still triggers the Round
52 guard before target allocation. S1, I1, the public snapshot and the global G
remain unchanged. Disabled or never-enabled sources complete the one-S2 path;
there is no automatic rebaseline.

## Global visibility and Phase 3B

Every private ADD, deferred UPDATE, nullability change, index evacuation,
DROP/RENAME and final CREATE leaves the published snapshot unchanged. A
successful indexed swap publishes exactly one next `DatabaseCommitSeq`.

The transaction remains structural and therefore retains the conservative
Phase 3A boundary inside Phase 3B:

```text
Decision sync
participant/schema completion
Complete sync
publication
```

After success, `pending_complete_count == 0`. A preceding pure-data G1 with a
deferred Complete is repaired/checkpointed by the structural G2 Decision sync;
the structural Complete then receives its own foreground sync. Tests pin one
additional Decision sync, one combined prior checkpoint, one additional
checkpoint sync, and a single G2 publication.

With a crash before G2's durable Decision, G1/S1/I1 wins. At and after G2's
durable Decision, recovery finishes S2/I2 and publishes G2. Three-reopen tests
confirm strict ordering and no pending Complete after recovery.

## Recovery and rollback evidence

The indexed prototype covers abrupt exit after logical index evacuation,
terminal column DROP, terminal rename, new IndexId reservation, tag 25/tag 35
intent construction, stage creation, mid-copy, final S2 index build, prepare,
Decision, normal/reversed participant commits and all commits. Every point is
opened three times.

Pre-CORD cases retain S1, C2 and physical I1/C2; I2 is absent from published
truth, although an accepted reservation remains burned. Post-CORD cases expose
S2, C4 named `legacy`, and new I2/C4. Recovery never replays DROP INDEX or SQL:
the loser needs no repair because I1 is still present, and the winner consumes
the already materialized S2 inventory.

## Alternatives

| Criterion | A logical evacuation + FinalRefining | B remain Backfilling | C physical S1 drop | D reopen IndexFinalizing | E early drop | F two transactions |
| --- | --- | --- | --- | --- | --- | --- |
| Natural SQL order | yes | yes | yes | yes | no | partial |
| One S2 | yes | yes | uncertain | possible | possible | not one atomic unit |
| S1 index unchanged pre-final | yes | yes | no | depends on implementation | possible | no across txn boundary |
| Source digest stable | yes | yes | no | not guaranteed | possible | separate authority |
| Program sealed | yes | no | possible | already over-sealed | too early | n/a |
| Rollback simplicity | best | good | poor | complex | order-dependent | physical restore unnecessary but non-atomic |
| Same-name reuse | yes | yes | requires overlay split | possible | awkward | yes, non-atomic |
| No new format | yes | yes | recovery evidence likely needed | likely | yes | yes |
| Global atomic publication | yes | yes | risky | risky | possible | no |
| Implementation risk | low, bounded | medium interleaving | high | high invariant expansion | medium usability | low code, wrong semantics |

Candidate B retains physical safety but unnecessarily allows deferred UPDATE
and nullability work to interleave after final index inventory changes.
Candidate C invalidates source authority and complicates rollback/crash/stream
semantics. Candidate D contradicts the existing meaning that
IndexFinalizing freezes TableDef. Candidate E introduces a surprising order
dependency and would expand pre-backfill adopted behavior. Candidate F is an
operational fallback only and loses atomic publication. Candidate A is the
only selected design.

## Exact Round 58 production scope

Round 58 should make only these routing changes:

1. Effective DROP INDEX in `AdoptedSourceBackfilling` revalidates S1 authority,
   seals the deferred program, mutates only the logical final inventory and
   enters `AdoptedSourceFinalRefining`.
2. Effective DROP INDEX in `AdoptedSourceFinalRefining` mutates only the logical
   inventory and remains in FinalRefining.
3. Unchanged or failed DROP does not change phase.
4. First CREATE INDEX retains the existing transition to
   `AdoptedSourceIndexFinalizing`.
5. The natural backfill/repair/NOT NULL, DROP old index, DROP old column, rename
   shadow, CREATE new index sequence finalizes in one S2 and one source pass.

Round 58 still excludes DROP INDEX before a frozen deferred program, reopening
IndexFinalizing, physical S1 evacuation, implicit index retargeting,
unique/multicolumn expansion, ALTER TYPE, post-terminal DML/nullability,
general SELECT, persistent or online migration, imported/partitioned/LSM
migration and any new recovery replay.

## Compatibility matrix

Round 57 changes no Canonical Schema, NBSJ tags 1--35, NBSC/NBSM, NBCO/CORD
v1--v3, Heap/Page/WAL/status, NBCL, NBCM/NBCS/NBCD/NBPC, IndexCatalog/BTree,
Partition/LSM, Protocol/PostgreSQL framing, deployment manifest, SDK Schema
Spec, generated code or public inspection format. It adds no persistent
evacuation phase, journal tag, early S2, S3, async work or new decoder target.

The real PostgreSQL negative acceptance remains deliberate: DROP INDEX
succeeds, DROP COLUMN returns `25000`, the next command returns `25P02`, and
ROLLBACK restores I1/C2. Production enablement is reserved for Round 58.
