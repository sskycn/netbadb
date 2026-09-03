# Indexed nullability rebuild / preparatory index drop audit — Round 34

Round 34 is an architecture and evidence round. It does not expose indexed
nullability migration, change a persistent decoder, or weaken
`backfill_indexed_columns`. The selected Round 35 slice is deliberately
limited to a Heap that already has a private staged replacement.

## 1. Audit basis

The audit was made from `main` commit
`62e4757f781768a9d12e5a8fd88a54d1b06bd3d0` after fetching `origin` and
confirming that local and remote `main` were identical. Production code is
unchanged; only deterministic audit tests and documentation are added.

## 2. Round 32/33 external-client status

No `psql` binary, importable `psycopg`, SQLAlchemy, or Alembic installation,
repository-local virtual environment, or usable cached wheel was available.
No package was installed and no network dependency was introduced.

## 3. Current indexed-nullability guard

`MaterializedSchemaTransaction::backfill_indexed_columns` is a monotonic
historical set. At backfill materialization it receives every column referenced
by the base active index inventory; an index created during the backfill also
adds its column. Dropping an index never removes the column. SET/DROP NOT NULL
is rejected when either the current logical inventory or this history contains
the column. This prevents a later logical DROP from laundering an incompatible
BTree that still exists physically in staged S2.

## 4. Current index-only DDL to DML behavior

`DROP INDEX` first enters logical `Composing`, immediately owns the schema
writer, has no data participant, creates no staged Heap, and emits no
`StageResourceIntent`. The first DML cannot qualify for backfill because the
transaction already has an index action. It materializes an
`InPlaceIndexDelta` on committed S1, enters `MaterializedIndex`, and enlists
one S1 write participant. The transaction-local planner and DML maintenance
exclude the logically dropped index. Rollback restores both the row update and
the index.

The concrete preferred sequence
`DROP INDEX; UPDATE; ALTER ... SET NOT NULL` therefore fails at ALTER with
`SchemaMutationAfterMaterialization` (transaction-state error; PostgreSQL
SQLSTATE `25000`), not with the historical indexed-column reason.

## 5. Current BackfillOpen eligibility

`is_backfill_candidate` requires exactly one touched runtime Single Heap,
zero table actions, zero index actions, no transaction-created table, and a
schema rewrite target. A preparatory `DROP INDEX` makes the transaction
ineligible. If another ALTER creates a rewrite target after the DROP, the
replacement can omit the old index, but the result is a sealed,
non-backfill `MaterializedIndex`, not `BackfillOpen`.

## 6. BTree IndexSpec binding

An active BTree stores an exact `IndexSpec { data_type: SemanticType,
nullable: bool }`. `SemanticType` includes the physical type and optional
nominal semantic name. The expected spec on Heap open is reconstructed from
the indexed final `ColumnDef`; a mismatch is
`IndexError::CatalogSpecMismatch { column_id }`. `IndexDefinition` binds
`IndexId`, optional name, `ColumnId`, and a generation-bearing page handle.
Table/Storage ownership comes from the containing Heap/catalog, and BTree kind
comes from the physical index implementation rather than from `IndexSpec`.

## 7. Retarget-before-drop result

Retargeting a private Heap from nullable to non-nullable while the old nullable
BTree remains active changes only Heap metadata. Existing index and data bytes
beyond page 0 remain unchanged, but reopening under the final `TableDef`
fails exactly with `CatalogSpecMismatch(ColumnId(3))`. The metadata operation
can therefore create an invalid intermediate state and is not an admissible
ordering.

## 8. Drop-before-retarget result

Exact physical retirement of the incompatible staged BTree before retarget,
followed by metadata/owner retarget and final-spec index creation, closes and
reopens successfully. The final BTree has
`IndexSpec { data_type: TEXT, nullable: false }` and resolves the original
`RowId` values.

## 9. Retired IndexCatalog behavior

Immediately after DROP, the in-memory retired inventory retains the complete old
`IndexDefinition`. Heap open skips retired catalog entries before column/spec
validation, so their old nullable spec does not invalidate the final table.
Reclaim may normalize a fully reclaimed retired definition into a minimal
pending record containing its `IndexId` and no metadata page. No allocation
remains owned by the old index. Retired truth is reclamation evidence, not an
active access path and not a schema-validation input.

## 10. StageIndexInventory

For the selected path, `StageIndexInventory` is the exact physical active
inventory of staged S2 immediately before refinement. It must initially agree
with the provisional table and may include Iold. Evacuation computes exact
drops by stable `IndexId`, physically retires every active index whose stored
`IndexSpec` is incompatible with the proposed final `TableDef`, and then
checks that every remaining active entry is compatible. Logical name absence
alone is insufficient proof.

## 11. FinalIndexInventory

`FinalIndexInventory` is the transaction's exact logical final inventory,
including monotonically reserved `IndexId` high-water. After retarget, final
indexes are created from that inventory under the final schema, and the opened
Heap inventory must equal it exactly. The existing NBSJ tag 34 already binds
this exact inventory, final table version/fingerprint, final NBSC digest,
staged/final locators, and evidence digest.

## 12. IndexId semantics

Iold and Inew are distinct identities even when table, column, and SQL name are
the same. The deterministic experiment produces Iold `IndexId(1)` and Inew
`IndexId(2)`; the allocator high-water is monotonic. Index IDs are never
reused to imply an in-place spec mutation.

## 13. Prepared DROP safety

A DROP prepared against Iold is an exact identity operation. After Iold is
dropped and a same-name Inew is created, executing the old prepared
`DROP ... IF EXISTS` returns `Unchanged` and leaves Inew active. Name reuse
does not retarget a prepared operation.

## 14. Candidate A — already-staged evacuation

Use the existing private S2 created by an eligible schema rewrite. DML backfills
and updates S2 while `BackfillOpen`; an exact logical DROP then closes DML and
enters index evacuation. Physically retire incompatible staged indexes, refine
the schema, retarget S2 once, and build final indexes once. This uses existing
staged authority, one final StorageId, one row copy, and the existing CORD
schema-decision model. It does not make DROP-first migrations eligible.

## 15. Candidate B — MigrationCloneHeap

Clone committed S1 solely because an index-only prelude needs later schema
refinement. This would make the preferred DROP-first syntax possible, but the
current intent grammar requires a real schema rewrite: replacement table
version/fingerprint, StorageId, generation, and epoch changes are coupled.
`StageResourceIntent` is resource evidence but is emitted only by current
schema/create materializers. A same-V/F physical replacement needs a new typed
intent and retirement cause; invalidating every ordinary DROP+UPDATE commit to
guess future refinement is unacceptable. The recovery proof is not present.

## 16. Candidate C — late clone from S1 participant

Run DROP and DML transactionally on S1, then copy the transaction-visible S1
view to S2 when ALTER arrives. Current `DatabaseTransaction` has no operation
that copies a writer's view and then detaches or rolls back only that S1
participant while letting S2 win. Predecision rollback resolves all write
participants; cleanup-only removal is not winner selection. This candidate
requires a new scratch/loser participant protocol and is rejected.

## 17. Candidate D — restricted support

Expose indexed nullability only when the transaction is already in
`BackfillOpen` on private S2. This is the honest product boundary around
Candidate A: UPDATE must occur before the index-closing DROP, and a separate
schema rewrite must have staged the table. Direct existing-column
`DROP INDEX; UPDATE; ALTER; CREATE INDEX` remains unsupported.

## 18. Comparison matrix

| Property | A: staged evacuation | B: MigrationCloneHeap | C: late S1 clone | D: restricted support |
| --- | --- | --- | --- | --- |
| Direct existing-column preferred syntax | No | Potentially | Potentially | No |
| One final storage | Yes | Yes, unproved publication | Yes, unproved handoff | Yes |
| One row copy | Yes | Yes | Yes | Yes |
| Avoid committed-S1 physical mutation | Yes | Yes | No before handoff | Yes |
| New retirement cause needed | No | Yes | Yes | No |
| Participant detach needed | No | No | Yes | No |
| Existing CORD decision compatible | Yes | Not yet | No | Yes |
| Recovery proof | Existing staged model plus new ordering crashes | Missing | Missing | Same as A |
| Physical cost | Clone already required + retire/rebuild | New clone + retire/rebuild | S1 WAL + clone + retire/rebuild | Same as A |
| Round 35 implementable slice | Yes | No | No | Yes, as A's scope |

## 19. Chosen architecture

Choose **A: staged incompatible-index evacuation**, with D as its explicit
support restriction. A is the mechanism; D is not a second architecture.

## 20. Chosen phase state machine

`BackfillOpen` (DML allowed) -> `IndexEvacuating` (exact DROP accepted,
DML closed) -> `RefiningAfterEvacuation` (SET/DROP NOT NULL and rename only)
-> `IndexFinalizing` (logical CREATE only) -> `Finalized`.
Failures enter rollback-required handling; no phase reopens DML.

## 21. DML timing

All user DML and NULL repair occurs on already-staged S2 before evacuation.
After the first accepted index DROP, reads and writes are rejected with the
existing post-refinement data-access rule. Final validation scans the
transaction-visible S2 rows.

## 22. Index-drop timing

The logical DROP is accepted only from `BackfillOpen`, by exact IndexId, and
is the irreversible phase boundary inside the transaction state machine. The
physical DROP happens before Heap/owner retarget. Dropping after SET NOT NULL
cannot retroactively make an earlier rejected refinement valid.

## 23. Schema-refinement timing

SET/DROP NOT NULL is accepted only after every incompatible active staged index
has been physically retired and compatibility of the remaining inventory has
been checked. SET NOT NULL then scans the staged transaction-visible rows.
Rename remains allowed only where it preserves physical index specs.

## 24. Replacement-index timing

CREATE INDEX is a final logical action after refinement. Physical construction
happens only after retarget, so its persisted `IndexSpec` is derived from the
final `ColumnDef`. Inew uses its reserved new IndexId and never mutates Iold.

## 25. Physical ordering theorem

The required order is:

`drop every incompatible active staged index -> assert remaining compatibility
-> retarget Heap and owner -> create exact final indexes -> validate exact
catalog -> persist tag 34/final NBSC evidence -> prepare -> one CORD decision`.

Retarget-before-drop is invalid by experiment. Create-before-retarget would
bind the provisional spec and is invalid by construction.

## 26. TableSchemaVersion

Candidate A is attached to a genuine logical schema rewrite, so the final table
uses the already-reserved `TableSchemaVersion = base + 1`. No physical-only
same-version replacement is introduced.

## 27. SchemaGeneration

The database publishes exactly the composed target `SchemaGeneration =
base + 1` once. Index evacuation adds no extra generation.

## 28. NBSC epoch

The prepared NBSC publishes exactly `epoch = base + 1`. Evacuation does not
create a second snapshot or epoch. The epoch remains physical publication
ordering, distinct from logical schema generation even though this path
advances both once.

## 29. Runtime revision

Commit advances the runtime catalog revision once when the new placement and
final index inventory become visible. There is no intermediate public revision.

## 30. StorageId strategy

S1 keeps the committed base StorageId until the decision. The existing staged
S2 keeps its already-reserved replacement StorageId through backfill,
evacuation, retarget, final index build, prepare, publication, and recovery.
No S3 is created.

## 31. Physical-only replacement semantics

They are not part of the chosen slice. A same TableSchemaVersion/fingerprint
S1-to-S2 replacement would need an explicit `PhysicalReplacement` intent,
retirement cause, epoch/runtime rules, and prepared-dependency refresh rules.
It must not masquerade as `RewriteHeap` or fabricate a schema change.

## 32. Retirement semantics

Iold retirement is an exact transactional staged-index retirement. S1 Heap
retirement remains the existing schema-replacement retirement decided by CORD.
No new Heap retirement cause is needed for A. Retired Iold metadata is never
treated as active final-schema truth.

## 33. GC implications

Index page reclaim remains generation-safe and WAL-governed inside staged S2.
The experiment permits numeric PageId reuse but observes a different
`PageGeneration`, so stale handles do not alias Inew. After publication,
existing staged cleanup, retired-index reclaim, and retired-S1 GC authority
remain unchanged.

## 34. Participant model

Only S2 is a winning write participant for the selected backfill path. S1 is a
read source during initial copy and remains untouched by user migration DML.
There is no hidden scratch participant and no selective participant detach.
The database still publishes one CORD schema decision.

## 35. Prepared dependency semantics

Logical prepared dependencies continue to bind stable table/column identity,
table version, and fingerprint. A changes those exactly as an ordinary schema
rewrite. Prepared DROP binds IndexId, so an old operation cannot delete a
same-name replacement. No same-V/F runtime-binding refresh contract is claimed.

## 36. Row/RowId stability

Evacuation, retarget, and index rebuild do not rewrite tuples. In the 128-row
experiment, every scanned row and RowId is identical before and after the
sequence, and all row-page images are byte-for-byte unchanged. Inew lookups
return the pre-existing RowIds.

## 37. BTree/page lifecycle

Iold becomes retired before retarget; its owned pages are reclaimed under the
existing index lifecycle. Inew is built from final rows and final spec with a
new IndexId and a different full `PageRef`. A numeric page ID may be reused
only with a new generation. Active catalog open validates Inew; retired Iold
does not participate in that check.

## 38. Physical byte cost

For the deterministic 128-row TEXT fixture, file lengths were
`with_old=77824`, `after_drop=77824`, `after_retarget=77824`,
`after_rebuild=77824`, and `built_without_old=77824` bytes. Equal final
length reflects safe page reuse, not zero work: building then retiring Iold
still incurs BTree writes, WAL, and reclaim traffic. Round 35 must not promise a
fixed byte saving.

## 39. Crash design

Round 35 must add deterministic predecision crashes around: before/after exact
index retirement, remaining-spec validation, Heap retarget, owner retarget,
final index build, tag 34 durability, NBSC prepare, and participant prepare.
Every predecision restart must retain S1 and clean or resume only authorized
staged resources. Postdecision crashes must publish/finish S2 from durable
NBSJ + NBSC + CORD evidence and validate the exact final inventory.

## 40. Predecision authority

`StageResourceIntent`, composition intent, reserved IndexIds, and participant
WAL authorize only private staged work before CORD. They never authorize
publication. Rollback must restore the committed S1 view and reclaim S2/Iold/
Inew work without depending on SQL names.

## 41. Postdecision authority

One durable CORD v2 schema-commit record is the winner authority. It references
the prepared NBSC epoch/digest; NBSJ tag 34 proves the staged Heap's exact final
index inventory. Recovery may finish rename/publication and participant commit,
but must not invent or reinterpret missing intent.

## 42. psql probe

Unavailable in the audit environment. The internal PostgreSQL-wire test pins
the current `25000`, subsequent `25P02`, and whole-transaction rollback,
but it is not reported as a real `psql` probe.

## 43. psycopg probe

Unavailable: no importable `psycopg` package or repository-local environment.

## 44. SQLAlchemy probe

Unavailable: no importable SQLAlchemy package or repository-local environment.

## 45. Alembic probe

Unavailable: no importable Alembic package or repository-local environment.
The preferred migration sequence remains unsupported and is not advertised.

## 46. Persistent formats

Round 34 changes none. Current versions are: canonical TableSchema v1; schema
envelope v1 for NBSC, NBSM, NBSJ, and NBSA (NBSJ record tags through 34);
partition catalog NBPC v1; coordinator log NBCO v1 with CORD record v1 for
decision/complete and v2 for schema-commit; Heap metadata v5; page/NBPG v5;
MVCC tuple NBMV v1; IndexCatalog v9; BTree v3 (v1/v2 readable); retired BTree
NBTR v1; Heap WAL container v4 with current record v5 (v3/v4 readable);
transaction-status NBTS/TXST v1; LSM manifest v2, WAL v1, SSTable v2, and Bloom
algorithm v1; native protocol v1; PostgreSQL protocol v3 framing; SDK Schema
Spec v1; deployment manifest v4.

## 47. Tests

New core tests pin index-only DROP-to-DML materialization, the exact preferred
sequence failure, monotonic historical guard behavior, rewrite-inventory
omission without BackfillOpen, and prepared old-DROP safety. A Heap experiment
pins invalid retarget-before-drop via the existing mismatch test and successful
drop-retarget-rebuild, retired ownership, IndexId/PageRef separation, exact
rows/RowIds, row-page bytes, lookup behavior, and byte costs. A server test pins
the internal PostgreSQL-wire transaction state and rollback.

## 48. Fuzz

All 13 existing fuzz targets are required for this audit with 1,000 runs and
seed 34, using untracked temporary corpora. Round 34 adds no target and must not
commit corpus or crash artifacts.

## 49. Compatibility

The intended result is no behavior or byte-contract change across Rounds 18–33.
Existing indexed-nullability rejection remains active. Round 32/33 recovery,
one-CORD publication, reservation, DDL composition, SQL, native protocol, and
PostgreSQL-wire suites remain the regression authority.

## 50. Unsupported/deferred

Deferred: direct DROP-first existing-column migration; physical-only same-V/F
clone; late clone from an S1 writer; selective participant detach; indexed
physical-type or nominal-type conversion; ADD/DROP column after DML; multiple
tables/placements; LSM/partition/imported tables; online/resumable migration;
savepoints; external-client enablement; and production SQL exposure.

## 51. NBSJ/CORD compaction debt

NBSJ remains append/rewrite history with tags through 34 and CORD remains an
unbounded decision log. Round 35 must use their current authority without
silently expanding compaction semantics. Checkpointing, history pruning,
decision retention horizons, and bounded growth remain explicit debt.

## 52. Completion boundary

Round 34 is complete only when audit tests and documentation pass the repository
validation matrix, Rust 1.85 is attempted, all fuzz targets run, the task commit
is integrated into and pushed on `main`, remote containment is verified, and
the clean temporary branch/worktree are removed.

## 53. Round 35 recommendation

Implement exactly one phase: **Staged Index Evacuation Lifecycle** for an
already-`BackfillOpen` managed Single Heap. Add the typed phases and crash
proof for physical drop-incompatible -> retarget -> create-final ordering.
Keep direct DROP-first migration and production indexed-nullability SQL
unsupported.

Round 35 implements that private Core foundation in
[staged-index-evacuation-round35.md](staged-index-evacuation-round35.md). Public
SQL remains blocked, the monotonic guard remains authoritative outside the new
typed states, and no persistent format changes were required.
