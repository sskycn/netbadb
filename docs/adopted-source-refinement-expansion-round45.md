# Adopted-source refinement expansion architecture audit (Round 45)

Round 45 is the historical architecture audit with test-only executable probes
that selected Candidate A. Round 46 subsequently productionized only its
surviving-base-column SET/DROP NOT NULL result. Final index DDL and every
relational statement after the first refinement remain production-negative;
see [Round 46](post-dml-source-nullability-round46.md).

The decision is to implement **Candidate A, surviving-base-column SET/DROP NOT
NULL, in Round 46**. Candidate A alone preserves S1/P1 as the only pre-final
data truth, the canonical no-op rule, one effective S2, and existing CORD
recovery without allocating a new durable identity.

## Candidate A: surviving-column nullability

The bounded theorem is:

```text
ordinary one-table DML on exact managed Heap S1/P1
  -> SET/DROP NOT NULL for a surviving base ColumnId
  -> final overlay only
  -> no-op commits DML on S1, or one transaction-visible S1 projection to S2
```

The probe uses the real `AdoptedSourceTransaction`,
`AdoptedSourceRefining`, composition journal, final materializer, and recovery
paths. A test-only Core hook admits only SET/DROP NOT NULL and deliberately
does not extend the production dispatcher. It also rejects a late-added
ColumnId: synthesized Cnew nullability remains the separate Round 41 Policy B
problem.

### Transaction-visible validation

SET NOT NULL cannot inspect the committed table view. Round 45 extracted the
existing DROP-first source validator into one Core-private helper used by both
the production DROP-first route and the test-only adopted-source probe. It
opens `transaction.begin_read_view(&[S1], ...)`, scans the exact ColumnId, and
therefore observes own INSERT/UPDATE while excluding own DELETE.

Executable results are:

| Visible state | Result |
| --- | --- |
| committed NULL repaired by own UPDATE | accepts |
| committed clean plus own INSERT NULL | `NotNullViolation(C3)` |
| committed NULL row removed by own DELETE | accepts |
| zero-row UPDATE leaving committed NULL | `NotNullViolation(C3)` |
| all visible rows non-NULL | accepts |
| empty visible table | accepts |

Validation is one semantically necessary column scan. An effective
finalization still performs exactly one separate full-row source projection;
the ADD+SET probe records one source-copy pass and three copied rows.

### First and subsequent operations

For first-operation SET NOT NULL, the safe ordering is complete adoption
preflight and exact S1/P1 capture, transaction-visible validation, then writer
acquisition and adopted-state installation. A failing probe leaves the native
transaction `Active`, the schema writer absent, the schema-composition state
absent, and the encoded journal byte-for-byte unchanged. Native DML can repair
the NULL and retry. PostgreSQL still reports the established first error and
then `25P02`; repairability is a Core transaction property, not a PostgreSQL
failed-transaction exception.

After ADD/DROP/rename has installed `AdoptedSourceRefining`, SET NOT NULL reads
the same frozen transaction-visible S1. Failure returns `NotNullViolation` and
does not reopen relational DML. The adopted transaction remains rollbackable,
and another compatible ALTER can still be meaningful; the probe successfully
renames a different surviving column after the validation error. A successful
surviving-column SET composes into the existing final overlay. DROP NOT NULL
needs no row scan and only changes that overlay.

Prepared ALTER preserves its compiled exact T2/V/F/C3 identity when prepared
before DML; DML does not cause name re-resolution. Preparing inside the
transaction follows the same overlay dependency rules. Parse/prepare,
Bind, and Describe allocate no identity and acquire no writer; Execute remains
the first possible validation/adoption point.

### Indexed survivors and no-op

Nullability is part of the persistent BTree `IndexSpec` and controls whether a
NULL key is accepted, although the scalar key encoding and ordering do not
otherwise change. It would therefore be wrong to relabel and reuse an S1 tree
as the final tree. The effective adopted path does not do that: S1 and its
source-index digest remain frozen, while final indexes are rebuilt on S2 from
the validated final schema. The test proves indexed SET and DROP, reopen, and
final-index reconstruction while retaining the same IndexId, ColumnId, and
index name. An unindexed survivor passes the same path. There is consequently
no physical reason to restrict Round 46 to unindexed columns.

SET then DROP on a nullable base and DROP then SET on a non-nullable base both
return the final TableDef to the base. Each seals `NoEffectiveChange`, commits
the DML on S1, and produces no S2, rewrite intent, tag 35, stage intent, tag
34, prepared NBSC, or V/G/E/runtime-revision advance. No ColumnId or IndexId is
allocated or burned.

### Crash and persistence result

Candidate A introduces no new crash state. Failed validation precedes writer
ownership. Once accepted, the existing Round 44 sequence applies unchanged:
refinement, durable aggregate intent, tag 35, stage, projection and indexes,
tag 34/prepared NBSC, then S1+S2 CORD. Crashes at composition-intent durable,
source-backfill-intent durable, and mid-copy recover as losers. Crashes with
both participants prepared, either participant committed first, or both
committed recover as existing winners. Each tested outcome survives three
catalog-only reopens, without SQL, DML, ALTER, validation, or projection
replay.

## Candidate B: adopted-source final index DDL

The ordinary `CompositionIndexReservation` can be recorded before an aggregate
index intent. Its existing encoder/decoder and `effective_index()` validation
survive reopen; rollback burns the reserved IndexId as intended. Thus Candidate
B does not need a tag 36 or a new reservation record merely to allocate an ID.
Prepare/Bind/Describe can remain pure and Execute can reserve the IndexId.

ADD Cnew followed by CREATE INDEX is also logically and physically feasible
when a table rewrite is already effective. The executable probe reserves I1
for Cnew, projects three rows once into S2, builds three NULL entries under the
final nullable spec, persists tag 34 final index truth, and reopens with I1.
S1 is not mutated.

The discriminator is the index-effective/table-no-op case. Rename and
rename-back make the TableDef equal to the base, while CREATE INDEX still
changes final truth. The current test-only route reaches the real reservation
logic but finalization rejects it with:

```text
SchemaMutation(Corrupt("IndexId reservation table lineage mismatch"))
```

The existing adopted finalizer requires a `RewriteHeap`; an index reservation
captured against overlay lineage cannot be cross-validated as an in-place S1
index delta after the table plan becomes a no-op. Supporting this honestly
requires choosing and proving one of three broader semantics: mutate/build the
index on transactional S1, force S2 for an otherwise canonical table no-op, or
add a distinct index-only adopted finalizer. DROP INDEX has the same issue:
the source digest must stay fixed, S1 should not be mutated merely to express
final truth, yet an index-only final inventory is effective.

Existing record encodings appear sufficient for the reservation and for the
ADD-Cnew effective-rewrite case, but index-only finalization would add intent
ordering, lineage cross-validation, rollback, and crash/recovery states even
if it used existing tags. That architectural cost makes Candidate B a later
round, not a routing-only Round 46 change.

## Candidate C: more DML after refinement

Candidate C is three different features:

- C1, rename-only, is representation-compatible: the overlay and S1 have the
  same width, order, ColumnIds, and physical types. Planning resolves overlay
  names to stable IDs. It may be technically possible, but it creates a
  rename-only phase exception to the clear rule that the first refinement
  closes relational execution. Prepared statements would also need explicit
  before/after-overlay dependency rules.
- C2, nullability-only, is not representation-correct by simply removing the
  guard. After SET, the final overlay rejects NULL while physical S1 remains
  nullable; after DROP, the overlay permits NULL while physical S1 may still
  reject it. Mutation needs an overlay-aware constraint adapter or a new
  physical target.
- C3, ADD/DROP, is physically incompatible. The executable probe measures
  overlay/S1 widths of 4/3 after ADD and 2/3 after DROP. An INSERT compiled for
  either overlay has the wrong normal S1 row shape.

Early S2 materialization would destroy ADD->DROP and rename-back no-op
optimization, lengthen S2 lifetime, move ordinary DML to a different
participant, and enlarge every pre-CORD failure window. Further refinements
could require S1->S2->S3 or a virtual-layout/sidecar/versioned-row subsystem.
Most importantly, it removes the Round 44 recovery theorem that S1/P1 is the
only authoritative data before finalization. Candidate C therefore requires a
new data-authority and recovery design, not a guard deletion.

## Comparison

| Criterion | A Nullability | B Index DDL | C More DML |
| --- | --- | --- | --- |
| reuses S1 as sole pre-final data truth | yes | yes for logical final indexes | no for general C2/C3 |
| one final S2 | yes when effective | yes for table-effective case; unresolved for index-only | threatened by early materialization/refinement |
| clean canonical no-op | yes | no: table-no-op/index-effective | no under early S2 |
| new durable identity reservation | no | yes, existing IndexId reservation | no identity necessarily, but new data authority |
| new persistent format | no | not for reservation; possible new finalization semantics | likely for adapter/sidecar/versioned rows |
| recovery changes | no | index-only intent/recovery proof required | major authority/participant redesign |
| parser/HIR changes | no | no | no syntax change, but replanning/dependency policy |
| PostgreSQL adapter changes | no | no migration-specific branch expected | likely execution-phase policy work |
| transaction-visible validation needed | SET only, existing theorem | index build consumes final projected rows | all mutations need overlay-aware enforcement |
| prepared-statement complexity | low: exact T/V/F/C | medium: Execute-time IndexId and overlay lineage | high: statements across overlay versions |
| existing proven machinery reusable | high | partial | low outside rename-only |
| product/migration value | high: repair-then-constrain | high | broad but not bounded |
| implementation risk | low | medium/high | high/very high |

**Winner: Candidate A -- surviving-base-column SET/DROP NOT NULL.** It is the
only candidate for which the executable evidence preserves every Round 44
late-clone, no-op, one-S2, and recovery invariant without a new identity or
persistent state machine.

Round 46 exposes only same-table SET/DROP NOT NULL for a surviving base
ColumnId after eligible one-managed-Single-Heap DML. SET validates the exact
transaction-visible frozen S1 before first writer acquisition or against the
same frozen source after adoption. Indexed survivors are included because S2
rebuild and stable logical identity were proven. First successful refinement
still closes DML. New-column NOT NULL, CREATE/DROP INDEX, post-refinement
SELECT/INSERT/UPDATE/DELETE, cross-table work, and every existing Round 44
eligibility exclusion remain unsupported.

## Physical observation

One fixed three-row fixture produced the following filesystem observations.
`source` is the complete fixture footprint immediately after DML and before
the tested refinement; `peak` is after S2 materialization but before commit;
`final` is after commit/close. These are observations, not general performance
claims.

| Probe | source bytes | peak bytes | final bytes | target | source-copy passes | rows copied |
| --- | ---: | ---: | ---: | --- | ---: | ---: |
| Round 44 nullable ADD baseline | 68,024 | 115,822 | 116,347 | S3 | 1 | 3 |
| Candidate A SET NOT NULL | 68,024 | 115,741 | 116,248 | S3 | 1 | 3 |
| Candidate B ADD Cnew + index | 68,024 | 173,693 | 174,218 | S3 | 1 | 3 |

Candidate A additionally performs one transaction-visible NOT NULL validation
scan; DROP NOT NULL performs none. The single source-copy pass is the distinct
final row projection. Each effective probe allocates one target StorageId.

## Persistent-format audit

Round 45 implements no format change. Candidate A needs none in Round 46.
Candidate B can reuse the current reservation encoding for an effective table
rewrite but needs new index-only state-machine semantics; Candidate C may need
new row/data-authority representation for C2/C3. No such B/C design is added.

| Format/boundary | Candidate A | Candidate B | Candidate C |
| --- | --- | --- | --- |
| Canonical Schema v1 | unchanged | unchanged | syntax schema unchanged; overlay execution policy grows |
| NBSC v1 | unchanged | existing final index inventory works for table-effective case | authority/publication rules would change |
| NBSM v1 | unchanged | no encoding change proven necessary | likely migration-state expansion |
| NBSJ v1 tags 1--35 | unchanged | existing reservation encodes; index-only ordering unresolved | likely new durable state for non-S1 truth |
| CORD v2 | unchanged | table-effective case reuses it; index-only proof needed | participant/authority theorem changes |
| Heap metadata/Page v5 | unchanged | unchanged | C2/C3 may require adapted/versioned rows |
| Heap WAL v4 | unchanged | unchanged | early/new-target DML changes WAL ownership |
| IndexCatalog v9 | unchanged; S2 tree rebuilt | unchanged in effective probe | depends on chosen data target |
| BTree v3 | unchanged; final nullable spec rebuilt | unchanged in effective probe | depends on chosen data target |
| transaction status v1 | unchanged | unchanged in effective probe | new participant timing likely changes use |
| PartitionCatalog | unchanged; out of scope | unchanged; out of scope | unchanged only if kept out of scope |
| LSM formats | unchanged; out of scope | unchanged; out of scope | unchanged only if kept out of scope |
| Protocol v1 | unchanged | unchanged | potentially unchanged wire syntax, new semantics |
| PostgreSQL framing v3 | unchanged | unchanged | unchanged framing, broader phase semantics |
| Manifest v4 | unchanged | unchanged | unchanged unless new physical resources appear |
| SDK Schema Spec v1 | unchanged | unchanged | unchanged |

Round 46 converted the Candidate A negative pins to production-positive tests.
The outer transaction-state boundaries retain `25000` and the following
PostgreSQL command receives `25P02`. Round 44 positives and the richer Round 42
DROP-index -> DML -> nullability -> layout -> final CREATE-index route remain
covered.
