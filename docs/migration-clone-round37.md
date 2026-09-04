# DROP-first migration architecture audit (Round 37)

## Decision

**Chosen Round 38 architecture = B: late final clone from the transaction-visible
source, with S1 and S2 committed by one CORD decision.**

Round 37 does not expose that behavior. The public transaction remains rejected
with SQLSTATE `25000` at the post-DML `ALTER`, and no persistent format changed.
Round 38 is a Core foundation round: it must first make the two-participant
winner recoverable at every crash boundary, without public SQL exposure.

Candidate B is selected because the required streaming source view, same-table
S1/S2 write participation, live commit, predecessor retirement, and Round 25 GC
theorem all work with current primitives. The current partial-participant recovery
path fails closed before client admission, rather than converging to S2. That is a
specific foundation gap, not a reason to adopt Candidate A's permanent same-V/F
replacement semantics.

## Scope and evidence

The audit used only test code, a PostgreSQL fixture probe, and documentation.
There is no `MigrationCloneHeap` API, no new SQL, no production state transition,
no new retirement cause, and no new journal record.

The fact-pinning Core module is
`crates/netbadb-core/src/migration_clone_audit_tests.rs`. Its probes establish:

- `DROP INDEX` followed by DML and COMMIT keeps S1, V, F, G, and E; advances the
  in-process runtime revision once; commits one S1 CORD participant; publishes
  the final index inventory; and survives reopen.
- an active S1 transaction view contains its own UPDATE and INSERT and omits its
  own DELETE. A partial backfill still exposes NULL; the complete backfill does
  not.
- `visit_rows_with_view_control` streams that view directly into a standalone S2
  built under final `NOT NULL` metadata. It does not collect all rows and creates
  only the final replacement index with a fresh IndexId.
- S1 and S2 for the same TableId can coexist as write participants, commit in one
  CORD decision, publish S2, retire a mutated S1 as `SchemaRewrite`, and delete
  S1 under the existing coordinator horizon.
- a crash after the first of those participants commits leaves a durable CORD
  winner, but current startup returns `MissingCommitParticipant` for S2. Startup
  does not admit a client or serve the now-committed S1.
- raw NBSC validation accepts a placement-only S1-to-S2 snapshot with the same
  G/V/F and E+1. An old prepared SELECT revalidates logical dependencies and
  binds S2 on execution; an exact SDK expectation also remains valid.
- `StageResourceIntent` and tag 34 fields can describe the same V/F, but NBSJ
  persist rejects them with `backfill stage target is absent from composition`:
  the current `InPlaceIndexDelta` still authorizes S1, not S2. The records are
  evidence subordinate to a typed composition plan, not standalone replacement
  authority.
- a rename away and back before materialization is a final schema no-op. Index
  and DML changes commit through S1; no S2 is created.

`scripts/test-migration-clone-audit.py` runs the actual PostgreSQL 17.11 client.
It proves the existing ordinary commit succeeds and the natural DROP-first
migration still fails at `ALTER ... SET NOT NULL` with `25000`; both outcomes
survive three catalog-only reopens. `psycopg`, SQLAlchemy, and Alembic were absent,
so no Python client was installed or claimed.

## Current state, exactly

### After `DROP INDEX Iold`

| Fact | Current value |
| --- | --- |
| composition state | `Composing` |
| schema writer | owned by the database transaction until commit/rollback |
| logical identity | the same T/V1/F1 |
| base index inventory | contains exact Iold |
| logical final inventory | omits exact Iold; allocator floor is retained |
| physical participants | none |
| write participants | none |
| staged bindings | none |
| StorageId allocation | unchanged; no S2 reservation |
| NBSJ | logical index action/reservation history only; no stage intent |

The operation has not guessed that an ALTER will appear later. It is ordinary
index composition.

### After the first `UPDATE`

| Fact | Current value |
| --- | --- |
| composition state | `MaterializedIndex(InPlaceIndexDelta)` |
| physical participant | S1's active `StorageTransaction` |
| participant mode | write |
| staged binding | none |
| DML destination | S1 |
| Iold in S1 transaction | its IndexCatalog drop is transactional and DML no longer maintains it |
| global/committed S1 inventory | still exposes Iold until the decision commits |
| row visibility | the same transaction reads its own changes |
| planner inventory | transaction composition uses the logical final inventory without Iold |
| schema writer | still held |
| another database transaction | rejected with `SchemaBusy` |

The committed registry continuing to show Iold is intentional isolation, not a
claim that the transactional drop was skipped. The Heap transaction owns WAL and
IndexCatalog mutations; commit publishes them and rollback restores Iold and the
base rows.

### Existing compatibility anchor

For `DROP INDEX; UPDATE; COMMIT`:

| Identity/evidence | Before | After |
| --- | --- | --- |
| StorageId | S1 | S1 |
| TableSchemaVersion | V1 | V1 |
| fingerprint | F1 | F1 |
| SchemaGeneration | G | G |
| NBSC epoch | E | E |
| runtime revision | R | R+1 |
| Iold | active | absent |
| CORD participants | none | one write participant, S1 |
| CORD schema reference | none | none |
| NBSJ resolution | composing | winner, `InPlaceIndexDelta` |

This path is a hard backward-compatibility requirement for the chosen design.

## Copy feasibility and exact blocker

`DatabaseTransaction::begin_read_view` can acquire a view for S1's active
physical transaction. `TableStorage::visit_rows_with_view_control` consumes that
view with a borrowed row visitor, so peak copy memory is bounded by one decoded
row plus the target's ordinary page/index buffers. The API exposes UPDATE,
INSERT, and DELETE effects without committing S1.

The Round 24 rewrite loop is not inherently committed-only. Its current caller
explicitly obtains `registry.get(old_storage).read_view()`, which is a committed
view. Replacing that acquisition with the database transaction's S1 view and
passing it to the same streaming visitor is the exact integration change. No new
storage scanner or `Vec<all rows>` is required.

Stable ColumnIds make the first version's layout-compatible rename and
nullability refinements directly mappable. S2 is constructed under F2, so S1 is
never retargeted. New RowIds are acceptable: no public, protocol, prepared, SDK,
or schema identity treats a rewrite RowId as stable. S2 installs only the final
logical index inventory. Iold is absent and same-name Inew has a fresh IndexId
and an F2-compatible spec.

## Candidate comparison

| Criterion | A: eager MigrationClone | B: late clone + commit source | C: late clone + detach | D: explicit migration mode |
| --- | --- | --- | --- | --- |
| supports natural DROP-first | yes | yes | yes | only after a non-SQL signal exists |
| preserves DROP+UPDATE+COMMIT behavior | only via A1, with a new physical replacement | yes, unchanged S1 path | yes, if detach is correct | not for unannotated ordinary SQL |
| new same-V/F replacement semantics | required for A1 | no | no | required if it chooses eager clone |
| new retirement cause | required | no; `SchemaRewrite` | no; `SchemaRewrite` | required if it chooses eager clone |
| transaction-view copy required | no | yes | yes | depends on selected mode |
| participant detach required | no | no | yes | no if it selects A or B |
| source S1 also commits | no | yes | no | depends on selected mode |
| current CORD compatible | one participant is compatible; intent is not | live commit is compatible; recovery needs extension | no participant-removal protocol | coordinator-compatible only after mode semantics exist |
| one final row copy | yes, before DML | yes, after DML | yes, after DML | yes for either eager or late realization |
| crash complexity | high: same-V/F publication and retirement | high but localized: dual-participant completion | very high: detach/rollback plus set mutation | high: durable mode plus chosen physical model |
| public SQL natural | yes | yes | yes | no; common migration SQL has no signal |
| likely Round 38 scope | physical-placement semantics and new retirement cause | source-participant recovery and late-clone foundation | selective detach protocol | new intent surface before physical work |

Candidate E, “always clone DDL-to-DML transactions,” is Candidate A applied more
broadly. It changes cost and physical identity for an existing valid transaction
without solving the semantic obligations, so it is rejected.

## Why Candidate A is not selected

The raw catalog can represent this physical-only result:

| Identity | Candidate A physical-only commit | Candidate B final commit |
| --- | --- | --- |
| TableId | T unchanged | T unchanged |
| TableVersion before refinement | V1 | V1 |
| TableVersion final | V1 if no ALTER; V2 after real ALTER | V2 after real ALTER |
| Fingerprint before refinement | F1 | F1 |
| Fingerprint final | F1 if no ALTER; F2 after real ALTER | F2 |
| StorageId | S1 to S2 even without ALTER | S1 to S2 only for effective ALTER |
| SchemaGeneration | G for physical-only; G+1 for ALTER | G+1 |
| NBSC epoch | E+1 for physical-only publication | E+1 |
| runtime revision | R+1 | R+1 |
| old IndexId | Iold inactive | Iold inactive |
| new IndexId | fresh if recreated | fresh Inew |

NBSC validation deliberately does not require an epoch increment to imply a
generation increment. `TableLineage` also accepts the same T/V/F with a new
placement. Those are representation facts, not sufficient transaction semantics.

Current table-object and schema-index composition validators require a real
V+1/F2 rewrite plan and exact target participants. Current retirement evidence
has only table-drop and schema-rewrite meanings. Calling same-V/F movement a
rewrite would be false. Candidate A1 therefore needs a new physical-replacement
intent, a new retirement cause, publication/recovery rules, and a permanent
behavioral cost for ordinary DROP+DML. A2—rejecting COMMIT when ALTER never
arrives—would regress valid behavior. Neither is justified when B is feasible.

For a true future physical-only move, logical prepared dependencies and SDK
expectations remain valid because they bind T/V/F, not StorageId. A prepared
SELECT replans and resolves S2. The PostgreSQL synthetic table OID, derived from
TableId and fingerprint, stays stable. The runtime revision must still advance
once so cached access paths and reflected physical inventory are rebuilt. Grants
remain keyed to TableId and are unchanged. These semantics are coherent, but
they are unnecessary scope for DROP-first migration.

Round 25's generic deletion horizon would be sufficient for a future explicit
physical-replacement cause: it is the maximum of the retirement transaction and
all CORD decisions naming the storage. The blocker is authoritative retirement
meaning and validation, not deletion mechanics.

## Chosen phase machine

Round 38 should add typed states rather than booleans on `MaterializedIndex`:

```text
ComposingIndexPrelude
  -- first same-table DML --> SourceBackfillOpen
  -- successful compatible ALTER --> SourceRefining (DML closed)
  -- final logical index DDL --> SourceIndexFinalizing
  -- COMMIT/effective-change --> LateCloneMaterializing
  -- S2 rows and indexes complete --> Prepared
  -- CORD durable --> Decided
```

Rules for the first foundation:

- exactly one managed Single Heap and one TableId;
- preparatory exact DROP INDEX operations may precede or occur during
  `SourceBackfillOpen`; multiple drops are representable;
- SELECT/INSERT/UPDATE/DELETE are allowed on that same target while open;
- CREATE INDEX before refinement, cross-table DML/index DDL, ADD/DROP columns,
  physical conversion, LSM, partitions, imported storage, savepoints, and online
  operation are rejected;
- the first successful layout-compatible ALTER validates the current S1
  transaction view and closes all DML;
- later final index DDL changes only the logical final inventory; no more S1
  physical index mutations are needed after closure;
- the finalizer first compares the final `TableDef` with the base. If equal, it
  takes the existing S1 index+DML commit path and never allocates S2;
- if different, allocate exactly one S2, stream S1's frozen transaction view
  once into an F2 Heap, build only final indexes, and validate the target again;
- all S2 work completes before either physical participant prepares or CORD is
  written. There is no clone, SQL replay, or user-code replay after the decision.

The historical `backfill_indexed_columns` guard remains for retargeting an
already-private Heap. Candidate B needs a separate typed predicate because S1 is
never retargeted. Its minimum physical proof is: exact Iold logically dropped,
the same Iold transactionally inactive in the same database transaction's S1,
no incompatible final active index, one managed Heap source, and DML frozen
before copy.

## Persistent authority for Candidate B

Round 37 changes none of these formats. The minimal future design is:

| Artifact | Needed? | Existing reusable? | New future tag/version? |
| --- | ---: | ---: | ---: |
| logical index reservation | yes | yes, NBSJ reservation records | no |
| source-backfill intent | yes | no typed source-participant phase exists | one future NBSJ record tag; no envelope-version redesign |
| StageResourceIntent | yes, before S2 files | yes, because final F2 is known | no |
| finalization intent/tag34 | yes | yes for exact F2 and final index truth, after validator generalization | no |
| prepared NBSC | yes | yes | no |
| CORD | yes, S1+S2 and one schema reference | yes, CORD v2 participant list | no |
| retirement evidence | yes | yes, real V1/F1 to V2/F2 `SchemaRewrite` | no |

The new source-backfill record is expected to bind T/V1/F1/S1, the owning
database transaction, the accepted phase, and the allowed index-prelude proof.
It must not encode SQL. The existing composition plan already binds predecessor
S1 and target S2 after materialization; CORD supplies both exact physical TxnIds.
Recovery must validate the participant set against that plan, so roles are typed
by intent rather than inferred from StorageId ordering. No CORD format change is
needed.

Current persistent versions remain: Canonical Schema v1, NBSC v1, NBSJ v1,
NBCO/CORD envelopes v1 with CORD schema-reference records (called CORD v2 by the
transaction protocol), Heap v5, MVCC tuple v1, transaction status v1, Page v5,
WAL v4 with record v3 plus later reservation/transition records, BTree v1/v2/v3,
and IndexCatalog v9 with backward decode v2 through v8.

## Recovery authority matrix

| Phase | Crash winner | Required cleanup/completion |
| --- | --- | --- |
| after DROP before DML | base | discard logical composition; no physical participant or S2 exists |
| during S1 DML | base | S1 WAL recovery aborts/undoes rows and IndexCatalog drop; Iold/base rows win |
| after DML before ALTER | base | same S1 rollback; no StorageId was allocated |
| after ALTER/refinement | base | discard provisional V2/F2 and roll S1 back; DML was already closed |
| during S2 clone | base | roll S1 back; delete exact staged S2 bundle from StageResourceIntent |
| S2 ready before prepare | base | roll S1 back; clean complete but private S2 |
| prepared before CORD | base | abort both prepared participants; clean S2; retain burned identities |
| CORD durable partial participants | final | finish both exact physical TxnIds, retire S1, publish prepared S2 NBSC, mark complete |
| retirement before NBSC | final | validate retirement evidence, publish the already-prepared NBSC, complete CORD |

Startup already runs schema-mutation/coordinator recovery before catalog loading
and server client admission. Round 38 must extend that recovery snapshot to open
and resolve both source and target participants named by the winner. It may never
fall back to serving stale NBSC/S1 after CORD is durable. Participant completion
order is sorted today but correctness must be independent of it:

- S1 committed, S2 prepared: finish S2, then retire S1 and publish S2.
- S2 committed, S1 prepared: finish S1 because CORD made it a winner, then retire
  it and publish S2.

The current subprocess probe covers the first ordering and demonstrates the
foundation gap as a fail-closed `MissingCommitParticipant`. Reverse-order fault
injection and successful convergence are Round 38 acceptance tests.

The predecision theorem is “the original committed S1 remains authoritative.”
The postdecision theorem is “S2 final is the only active placement.” Since no
listener is admitted until recovery returns, and recovery must return only after
both participants, retirement, and NBSC publication are complete, no client can
observe committed intermediate S1.

## Retirement and GC

S1 still has V1/F1 after its winner transaction commits. Its rows and index
inventory may differ from the original base, but current replacement validation
checks physical identity, table identity, fingerprint, locator, and recovery
state—not row equality or equality of the old index catalog. The live experiment
commits a new row and drops Iold in S1, then successfully records ordinary
`SchemaRewrite` retirement for the genuine V1/F1 to V2/F2 transition.

Because S1 is itself a CORD participant, its final decision is naturally included
in the Round 25 horizon. Once all transaction handles close and that decision is
complete, existing exact-component GC deletes the mutated retired S1 without
affecting S2. No new retirement cause is needed for Candidate B.

## Identity and prepared dependencies

For an effective refinement, T stays stable; V1/F1 remain in force through the
index prelude and S1 DML; acceptance establishes provisional V2/F2 once; final
commit advances G, E, and runtime revision exactly once. S2 is allocated only at
late materialization. Iold stays inactive and a same-name Inew is a fresh ID.

| Prepared operation | Before refinement | After accepted V2/F2 refinement | Final behavior |
| --- | --- | --- | --- |
| SELECT prepared before DROP | valid against V1/F1 and sees transaction rows | stale | must be reprepared; no stale S1 binding is retained |
| SELECT prepared after DML | valid against V1/F1 while backfill is open | stale | must be reprepared |
| SET NOT NULL prepared before DML | exact V1/F1/C target may execute once if still current | its execution establishes V2/F2 | later reuse is stale |
| CREATE INDEX prepared before final ALTER | remains an old V1/F1 dependency | stale after refinement | reject; prepare/execute in final logical phase |
| DROP exact Iold | valid only while exact Iold is active in the overlay | stale/undefined after its drop | never rebind to another ID |
| same-name replacement | old prepared identity is not reusable | reserve fresh Inew | later same name still identifies Inew, not Iold |

Prepared physical StorageId is not a dependency. Per-statement authorization is
unchanged: schema_admin remains required for DDL and exact table read/write/
transaction capabilities remain required for S1 DML and validation. Core owns
the state machine; frontends neither predict later ALTER nor buffer, split, or
replay the transaction.

## Exact primary-transaction answers

For the future chosen architecture:

- **Where does UPDATE run?** In S1's active write participant.
- **When is a new StorageId allocated?** At COMMIT late-clone materialization,
  after final effective schema/index truth is known.
- **Does S2 exist during UPDATE?** No.
- **How are uncommitted S1 rows copied?** Once, synchronously and streaming, from
  S1's transaction-owned `StorageReadView` directly into final-schema S2.
- **When is DML closed?** By the first successful compatible schema refinement.
- **When is V1 to V2 established?** Provisionally at that refinement; durably in
  the one final NBSC.
- **When is F1 to F2 established?** With the provisional final `TableDef` at the
  same refinement; S2 is created directly under F2.
- **Does S1 remain a CORD participant?** Yes.
- **Does S1 physically commit after CORD?** Yes, even though its data is redundant.
- **When does S1 become retired?** After required participant completion and
  before final NBSC publication, with recovery able to finish the same sequence.
- **What retirement cause is used?** Existing `SchemaRewrite` for genuine F1 to F2.
- **Does SchemaGeneration advance?** Yes, G to G+1 exactly once.
- **Does NBSC epoch advance?** Yes, E to E+1 exactly once.
- **What if no ALTER occurs?** No S2; existing S1 index+DML commit behavior wins.
- **What if the final schema is a net no-op?** Detect it before allocation; no S2;
  commit final index/DML state on S1 and keep consumed IDs burned.
- **Which prepared statements remain valid?** V1/F1 logical statements remain
  valid until effective refinement; afterward normal V/F dependency checks make
  them stale. Index statements retain exact IndexId semantics.
- **How does a pre-CORD crash restore Iold/base rows?** S1's existing WAL/status
  loser recovery rolls back the single physical transaction; staged S2 is exact
  cleanup evidence only.
- **How does a post-CORD crash converge to S2?** Winner recovery opens and commits
  both named physical TxnIds, records/validates S1 retirement, publishes the
  prepared S2 NBSC, and completes CORD without replaying SQL or cloning rows.
- **Why can no client observe committed intermediate S1?** Recovery is synchronous
  and precedes active-catalog opening and listener admission; a durable winner may
  return only after S2 is the sole active placement.

The final primary transaction therefore runs DROP and UPDATE transactionally on
S1, validates `SET NOT NULL` against that exact view, reserves fresh Inew in the
final overlay, clones once into F2/S2 at finalization, prepares S1 and S2, writes
one CORD decision with one schema reference, finishes both, retires S1, and
publishes one G+1/E+1 NBSC.

## Round 38 boundary

Round 38 is **Source-Participant Backfill + Late Final Clone Foundation**:

- one managed Single Heap and one table;
- preparatory DROP INDEX plus ordinary S1 DML;
- layout-compatible rename and SET/DROP NOT NULL close DML;
- logical final CREATE/DROP INDEX inventory;
- one bounded transaction-visible S1-to-final-S2 copy;
- S1 and S2 prepared under one decision;
- recovery snapshots and typed intent validation for both participant roles;
- both partial-completion orders, predecision rollback, exact S2 cleanup,
  predecessor retirement, NBSC publication, reopen, and GC tests;
- no public SQL exposure until the complete crash matrix passes.

Required fault points include index-prelude durability, first/mid/final S1 DML,
constraint validation, logical refinement, S2 reservation and stage intent,
creation/mid-copy/index build/final validation, each prepare, prepared NBSC, CORD
decision, each participant completion, S1 retirement, NBSC publication, CORD
Complete, and winner resolution.

Round 38 must not add ADD/DROP/type conversion, cross-table work, LSM/partitioned
placements, selective detach, same-V/F physical replacement, online serving,
frontend look-ahead, transaction replay, or public migration claims.
