# Late-clone layout projection architecture audit (Round 40)

Round 40 is an architecture audit and test-only experiment. Production SQL
admission is unchanged: after the DROP-first source prelude and DML, `ADD
COLUMN` and `DROP COLUMN` still fail with SQLSTATE `25000`. No row or persistent
format changes in this round.

## 1. Commits / integration

The audit started from `1d75db6fd5ea3a5a69d83afc95eea5507ef66687` on
`codex/late-clone-layout-audit-round40`. The final commit/integration identifiers
are recorded in the delivery report because they do not exist while this document
is authored.

## 2. Round39 regression baseline

The real psql 17.11 fixture still passes replacement, no-replacement, no-ALTER S1
fast path, net-no-op, failed validation, post-refinement DML rejection, rollback,
and three catalog-only reopens. In particular:

```text
DROP INDEX -> UPDATE -> SET NOT NULL -> CREATE INDEX -> COMMIT  PASS
DROP INDEX -> UPDATE -> COMMIT                                 PASS on S1
```

## 3. Current ADD/DROP post-DML behavior

The permanent psql fixture now pins both current boundaries:

```text
DROP INDEX -> UPDATE -> ADD COLUMN   25000
DROP INDEX -> UPDATE -> DROP COLUMN  25000
next statement in each failed PG transaction                25P02
```

This is a negative compatibility assertion, not a feature test.

## 4. Existing ordinary-rewrite row transform

Both the Round 24 ordinary rewrite and Round 38/39 late clone reach
`materialize_schema_index_rewrite`. It derives a target entry by finding the
source position whose `ColumnId` equals the target `ColumnId`; a missing target
ID becomes `ScalarValue::Null`. The older legacy rewrite contains the same rule.
The test-only prototype makes this implicit theorem explicit.

## 5. Proposed projection abstraction

The prototype is storage- and SQL-neutral:

```text
LateCloneProjection {
    source_table: (TableId, TableSchemaVersion, fingerprint),
    target_table: (TableId, TableSchemaVersion, fingerprint),
    target_entries: Vec<ProjectionEntry>,
}

ProjectionEntry =
    Source { column_id, checked_source_position }
  | SynthesizedNull { column_id }
```

Build is O(columns). Applying it owns one source row and one target row.
The prototype rejects an application whose source or target T/V/F differs from
the exact identity used to build the plan.

## 6. ColumnId identity theorem

Identity is proved by `ColumnId` equality before a source ordinal is cached.
Names and raw ordinals never establish identity. Source and target IDs must be
unique, the TableIds must match, and every target-only ID must be an accepted,
durably reserved new column.

## 7. Target-order semantics

Projection entries are emitted by iterating `target.columns`; output order is
therefore exactly final `TableDef` order. A hash map may assist lookup but may not
choose output order. The test checks `[C1, C3, C4]` maps to source positions
`[0, 2, NULL]`.

## 8. Surviving-column compatibility

The first layout-changing source refinement must require the same semantic type
and the same physical type for every surviving `ColumnId`. Name and nullability
may change; presence may change. The prototype rejects both physical and nominal
changes, including `UserId(INT64) -> TeamId(INT64)`.

## 9. ADD nullable mapping

An accepted nullable Cnew has no source lookup and projects database `NULL` for
every source row. There is no default, expression, backfill, or S1 schema change.

## 10. DROP column mapping

A source-only ID has no target entry and disappears. S1 continues to contain its
physical field until S1 retirement; S1 is never retargeted.

## 11. Same-name DROP+ADD

The hard fixture drops `C2 legacy` and adds `C4 legacy`. Both the pure prototype
and the actual ordinary rewrite produce `C4 = NULL`, never the old C2 value,
even though name and physical type match.

## 12. Rename composition

Renaming surviving C3 from `email` to `canonical_email` preserves C3's value.
Likewise, renaming another survivor into a dropped column's name follows the
survivor ID, not the vacated name.

## 13. Multiple ADD

The prototype projects both a new TEXT and a new BIGINT column as explicit NULL.
The actual rewrite experiment adds two different nullable types without storage
validation failure.

## 14. ColumnId reservation timing

Future ADD must reserve at ALTER acceptance, before overlay mutation. Parse,
Bind, Describe, prepare, final S2 allocation, and COMMIT are too early or too
late to own a new ID.

## 15. Durable reservation reuse

The existing tag 16 `CompositionColumnReservation` is the right durable shape:
it is transaction- and table-scoped, contains Cnew and the next floor, and is
already included by `effective_column`. Today
`prepare_composition_reservation` rejects appending it after a tag 25
`index_intent` with `duplicate or out-of-order composition reservation`; the
new test pins this exact implementation gate after S1 DML and
`SourceBackfillOpen`.

This is not a format blocker. NBSJ rewrites a canonical complete history and
encodes tag 16 reservations before tag 25 regardless of call time; the existing
decoder already understands the combined record shape. Round 41 should narrowly
extend the state-machine invariant and decoder/cross-validation tests while
reusing tag 16. No new tag or old-byte reinterpretation is justified.

## 16. ColumnId high-water

`effective_column` scans legacy rewrite and composition reservations, including
losers. ADD advances monotonically; DROP never lowers the floor. Existing Round
24/28 tests verify three reopens and gaps.

## 17. ADD rollback/crash burn

Existing rewrite and composition crash/rollback tests prove a durable tag 16
reservation burns Cnew even when no schema wins. Round 41 must add the same cases
after source intent materialization once the ordering gate is relaxed.

## 18. ADD→DROP net-no-op

Round 28 already proves final TableDef equality yields no S2, version,
generation, epoch, or runtime-revision change while the ColumnId stays burned.
The source-backfill finalizer already uses final TableDef equality for its S1
fast path, so the same classification applies.

## 19. DROP→ADD effective identity change

Replacing C2 by C4 is not canonical equality, regardless of name or physical
declaration. It requires one final rewrite and projects C4 as NULL.

## 20. TableVersion

Any effective final table change advances V once. Multiple DROP/ADD/rename/
nullability actions still produce V+1. A final net-no-op restores base V.

## 21. SchemaGeneration

An effective target publishes G+1 once. A net-no-op leaves G unchanged.

## 22. NBSC epoch

An effective target publishes E+1 once through the one prepared NBSC. A net-no-op
has no NBSC publication and leaves E unchanged.

## 23. runtime revision

Publication changes the in-memory catalog revision once, not once per column.
The actual combined ordinary-rewrite experiment asserts `+1`.

## 24. StorageId timing

ADD/DROP statement execution must allocate no storage. Effective finalization
allocates exactly one S2 after final truth is frozen. Net-no-op allocates none;
there is no S3.

## 25. Transaction-visible source

Late clone calls `transaction.begin_read_view([S1])`; ordinary rewrite alone
uses the committed view. Existing Round 38 tests prove S1 UPDATE and INSERT are
visible and DELETE is absent. Layout projection changes only the per-row value
mapping after that view is selected.

## 26. Streaming copy

The shared visitor performs one pass. Projection build is O(columns); each row
costs O(target columns) and owns at most one decoded source row and one target
row. No all-row Vec is introduced.

## 27. UPDATE/INSERT/DELETE result

The combined theorem is the product of two independently executable facts:
Round 38 proves the transaction-visible S1 row set, while the Round 40 projection
tests prove `[C1,C2,C3] -> [C1,C3,C4]`. The result preserves updates and inserts,
omits deletes and C2, and gives every visible row C4 NULL. The current reservation
ordering gate prevents honestly running the full ADD lifecycle in one source
transaction in Round 40.

## 28. RowId behavior

Target inserts allocate S2-local RowIds. Old RowId identity is neither copied nor
promised. Final indexes are built against the new S2 RowIds.

## 29. SET NOT NULL surviving column

The existing S1 transaction-view validation remains correct and occurs before
CORD. Projection copies the surviving ID unchanged; no new validator is needed.

## 30. SET NOT NULL new-column policy

Choose **Policy B** for the future Core foundation. A new column projects NULL;
a nonempty source therefore returns `NotNullViolation`, while an empty source
succeeds vacuously. The one-pass projector can enforce this before inserting the
first invalid target row, so no second execution engine or scan is needed. ADD
NOT NULL syntax, implicit fill, and ADD-then-DML remain unsupported.

## 31. Primary-key DROP

Core's composed ALTER guard rejects a target column whose exact ColumnId has
`primary_key` with `PrimaryKeyColumn(ColumnId)`, before logical mutation. The
current SQL CREATE subset cannot create a managed runtime PK fixture, so Round 40
audits this code path rather than claiming a new executable SQL probe.

## 32. Indexed-column DROP

The executable test receives exact `IndexedColumn(C2)` while the final logical
inventory still contains an index on C2. After dropping that exact index first,
ordinary DROP is eligible.

## 33. Final index dependency

Every final index definition must resolve its `column_id` in final TableDef.
Creating or retaining an index on a dropped ID fails before target publication or
index build. There is no cascade.

## 34. Final index on added column

Technically feasible: S2 inserts explicit NULL and the existing nullable-index
path indexes final S2 values/RowIds. Recommendation: defer public support in
Round 41; first land layout lifecycle and reservation authority, then add a
separate exact test before enabling this combination.

## 35. Prepared statement semantics

Prepared ALTER contains the old exact `(TableId,V,F)` and a resolved ColumnId.
Data and index-only revisions do not stale it, but an accepted schema refinement
does. ADD reserves no ColumnId at prepare time.

## 36. Same-name prepared safety

An old prepared statement for C2 cannot bind C4 merely because both are named
`legacy`: provisional/final fingerprint validation rejects it first, and the
typed operation still contains C2.

## 37. Phase-state implications

No `SourceProjecting` state is needed. ADD/DROP remain logical operations inside
`SourceRefining`; physical behavior still begins only in existing
`LateCloneMaterializing`. The only new state-machine work is permitting and
validating late tag 16 reservation before final intent replacement.

## 38. DML closure

The first successful layout refinement must enter `SourceRefining` and preserve
the current `MigrationDataAccessAfterRefinement` boundary. SELECT/INSERT/UPDATE/
DELETE remain closed, so post-DML ADD cannot become an in-transaction backfill
target.

## 39. Source activation scope

Projection feasibility is not generic late schema-writer admission. Natural
`UPDATE -> ADD` or `UPDATE -> DROP unindexed` lacks the current DROP-index prelude
and source-backfill authority. That separate admission problem remains deferred.

## 40. Candidate A — ColumnId projection

Build one checked target-ordered plan by ColumnId and stream the transaction-
visible S1 view directly into final S2. New IDs synthesize NULL, dropped IDs are
omitted, and survivors copy their exact value. This satisfies the theorem.

## 41. Candidate B — positional

Reject positional identity. A cached source position is safe only after deriving
it from a verified ColumnId match; ordinal-to-ordinal meaning fails DROP/ADD and
reorder cases.

## 42. Candidate C — two-stage rewrite

Reject S1 -> S2(source layout) -> S3(final layout). It adds a second copy,
StorageId, participant/resource graph, peak disk cost, and recovery surface for
no semantic gain.

## 43. Candidate D — versioned rows

Reject schema-versioned row decoding/conversion. It changes the row format and
pushes evolution policy into storage for operations already expressible as a
pure projection.

## 44. Comparison matrix

| Criterion | A ColumnId projection | B positional identity | C two-stage rewrite | D versioned rows |
| --- | --- | --- | --- | --- |
| one clone | yes | yes | no | not a bounded clone |
| stable ColumnId semantics | yes | no | possible | possible, much broader |
| ADD NULL | direct | unsafe after layout change | second stage | new decoder policy |
| DROP column | omit entry | unsafe | second stage | tombstone/version policy |
| same-name drop/add safety | yes | no | only with ID-aware stage | possible |
| new persistent format | no | no | more durable resources | yes |
| recovery changes | no | no, but incorrect | larger graph | extensive |
| bounded streaming | yes | yes | two bounded passes | more complex |
| complexity | low | deceptively low/incorrect | high | very high |

## 45. Chosen architecture

Choose exactly **Candidate A — ColumnId projection**.

## 46. Shared-helper recommendation

Round 24 and future late clone should share one Core-private pure projection
builder rather than duplicate loops. Keep it in Core initially because it also
checks migration policy and accepted reservations; do not make storage depend on
schema-mutation policy. If later callers need only canonical ID/type mapping, a
smaller mapping primitive can move to `netbadb-schema` without ScalarValue or
storage dependencies.

## 47. SourceBackfillIntent implications

Tag 35 already binds exact source and target T/V/F/S, action digest, final snapshot
digest, locators, physical transaction, generation/epoch steps, final indexes,
and clone-plan digest. Recovery never reruns projection, so no projection bytes or
digest are required.

## 48. StageResourceIntent

Reuse unchanged. It authorizes the exact provisional S2 resource and locators,
not how predecision rows were computed.

## 49. tag34

Reuse unchanged. Finalization evidence states that S2 already matches final
schema/index truth; it does not need a replayable row mapping.

## 50. Prepared NBSC

One final snapshot remains sufficient. It contains final TableDef order, IDs,
types, version, placement, high-waters, and fingerprint.

## 51. CORD

CORD v2 still decides S1 and S2 plus one schema reference. Projection is a
predecision implementation detail and adds no participant.

## 52. Recovery

Unchanged. Before CORD, roll back S1 and delete partial S2. After CORD, never
project again: finish winner participants, validate/promote S2, retire S1, and
publish prepared NBSC. SQL, DML, mapping, and index build are not replayed.

## 53. Retirement/GC

S1 remains `RetiredBySchemaRewrite` because T survives while V/F/S changes. The
existing Round 25 horizon and GC path apply; no new cause is needed.

## 54. Persistent formats

All remain unchanged: Canonical TableSchema v1, NBSC/NBSM/NBSJ v1 with tags
1--35, CORD v2, Heap metadata/Page v5, NBMV v1, IndexCatalog v9, BTree v3,
Heap WAL v4/record v5, transaction status, Protocol v1, PG framing v3, Manifest
v4, partition and LSM formats, and SDK Schema Spec v1.

## 55. Physical cost

Projection adds an O(columns) plan and O(target columns) work per row, but no
extra row pass, storage, or intermediate family. The existing deterministic S1
late-clone fixture remains representative: S1 112,057 -> 145,057 bytes during
DROP+DML, staged S2 103,152 bytes (103,321 final), peak 264,801 bytes, one pass,
three rows, one target ID. The new two-row ordinary-rewrite probes measured the
same fixed-size small-Heap family shape for each layout: ADD 37,593 -> 37,369
bytes, DROP 37,593 -> 37,369 bytes, and combined ADD+DROP 37,593 -> 37,369
bytes; every case allocated exactly one target StorageId. Exact bytes are
build/fixture observations, not a claim that different layouts always have equal
size.

## 56. Crash-design future matrix

| Crash point | Authority/result |
| --- | --- |
| after Cnew reservation | Cnew burned; base schema wins |
| during projection | no CORD; roll back S1, delete partial S2 |
| after projected S2, before CORD | base wins; delete S2 |
| after CORD | finish S1+S2 winners; never project again |

Round 41 must add the first row to source-backfill crash tests after relaxing the
reservation ordering gate. The remaining rows are already covered by Round 38.

## 57. psql negative probes

Real `/opt/local/lib/pgsql/bin/psql` 17.11 returns `25000` for both post-DML ADD
and DROP and `25P02` for the following command. The fixture uses the required
`/opt/local/lib/icu/lib`.

## 58. psql Round39 regression

The complete Round 39 script passes unchanged positive behavior and now includes
the two negative pins.

## 59. psycopg

Unavailable (`ModuleNotFoundError`); not installed and unverified.

## 60. SQLAlchemy

Unavailable (`ModuleNotFoundError`); not installed and unverified.

## 61. Alembic

Unavailable (`ModuleNotFoundError`); not installed and unverified.

## 62. Tests

`late_clone_layout_audit_tests` covers target-ordered ColumnId mapping, ADD NULL,
DROP omission, same-name replacement, rename, multiple additions, physical and
nominal compatibility rejection, Policy B, actual one-S2 ordinary rewrite,
indexed DROP ordering, and the current post-intent reservation blocker. Existing
Round 24/28/38/39 suites remain the authority for high-water, no-op, prepared,
transaction-view, indexes, CORD, recovery, and GC.

## 63. Fuzz

All thirteen existing targets run with `-runs=1000 -seed=40`. No decoder or
corpus format is added; results are recorded in the delivery report.

## 64. Compatibility

Production accepted SQL, SQLSTATE mapping, APIs, row encoding, wire protocols,
manifest, SDKs, and recovery formats are unchanged. Older binaries see no new
tag. README intentionally gains no feature claim.

## 65. Unsupported/deferred

Public post-DML ADD/DROP, generic late source activation, ADD then DML, ADD NOT
NULL/backfill, DEFAULT/generated values, physical/nominal conversion, USING,
constraints/FK, reorder syntax, multi-table migration, LSM/partition/imported
storage, savepoints, online/resumable migration, automatic GC, and NBSJ/CORD
compaction remain deferred.

## 66. NBSJ/CORD growth

Candidate A adds no durable row-mapping record. Future ADD reuses an existing tag
16 reservation and the one final tag 25/tag 35/tag 34/CORD path. Growth remains
per accepted identity/action/transaction; compaction is separate.

## 67. Completion workflow

The task follows audit/tests, full validation, diff review, commit, fetch, merge
to main, push, remote verification, and clean worktree/branch removal. Any blocked
step must be reported without discarding the worktree.

## 68. Round41 recommendation

Select exactly one implementation phase:

**Round 41 — Late-Clone Row Projection Foundation / post-DML ADD nullable and
DROP column Core lifecycle.**

It should remain Core-foundation-only: one already-valid SourceBackfill TableId,
reuse tag 16 after narrowly extending its ordering invariant, build the shared
ColumnId projection, accept internal ADD nullable/DROP non-PK and unindexed,
create one final S2 at finalization, rebuild final indexes, and reuse tag 35,
stage intent, tag 34, NBSC, CORD v2, retirement, and recovery. Do not expose SQL
until a later round.

The required transformation is therefore:

```text
S1 T/V1/F1: C1 id, C2 obsolete, C3 email
visible rows: (1,"old","filled"), (3,"x","three")

DROP C2; ADD C4 marker NULL; RENAME C3 -> canonical_email

projection: C1 <- source C1; C3 <- source C3; C4 <- NULL

S2 T/V2/F2: (1,"filled",NULL), (3,"three",NULL)
```

C2 is gone; C4 never inherits C2; C1/C3 survive by ID; target order is final
TableDef order; source is scanned once; S3 does not exist.
