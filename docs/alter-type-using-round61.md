# `ALTER COLUMN TYPE ... USING` synthetic lowering audit (Round 61)

Round 61 selects an architecture for a future high-level type-change statement
without opening the production grammar, HIR, Core API, native protocol, or
PostgreSQL surface. The executable proof is compiled only under `cfg(test)`.
Production continues to reject both `ALTER COLUMN c TYPE T` and
`ALTER COLUMN c TYPE T USING expression`; `CAST(expression AS type)` also
remains absent. The existing `expression::TYPE` syntax is unchanged.

The audit started from fetched `origin/main`
`de79c3ebbef70854f7be1223747c85d365c67529` (Phase 9 safe Change Stream
reclamation). Relevant retained history is Round 58 `2d4dcc8`, production
cross-physical CAST Round 60 `3e682a8`, Phase 3D `67f62c7`, and Phase 9
`de79c3e`.

## Decision

Candidate A, synthetic shadow lowering, is the unique selection.

| Criterion | A synthetic plan | B fake writer | C SQL choreography | D same-ID | E early S2 | F new evaluator | G multi-txn |
| --- | --- | --- | --- | --- | --- | --- | --- |
| pristine transaction | committed S1 | fabricated P1 | still needs authority | possible only with new theorem | yes | orthogonal | yes |
| DML before DDL | adopted S1/P1 | obscures real authority | sequencing-heavy | new theorem | long-lived S2 | orthogonal | not atomic |
| exactly one S2 | yes | yes | possible | yes | yes, too early | possible | no atomic S2 |
| no fake participant | yes | no | no for pristine | yes | yes | yes | yes |
| one CAST semantics | yes | yes | yes | uncertain | yes | no | application-defined |
| preserves ordinal | exact E-to-F projection | possible | manual reorder needed | yes | possible | possible | manual |
| index recreation | final inventory, fresh ID | same | step churn | identity conflict | possible | orthogonal | exposed gap |
| rollback simplicity | logical plan only | WAL/participant noise | many synthetic phases | difficult | staged resource lifetime | new state | partial commit |
| recovery without replay | yes | noisy authority proof | step evidence | new proof | resource cleanup | likely new program | no |
| no new format | yes | technically, but misleading | likely more evidence | uncertain | yes | risks opcode | yes |
| implementation risk | lowest | high | high | highest | medium/high | high | operational fallback |

Candidate B is rejected because a zero-row physical writer changes WAL, Change
Stream, participant, group-accounting, and recovery facts without representing
user DML. Candidate C is rejected because internal public-SQL choreography
needs an internal name and exposes implementation steps as state-machine and
recovery evidence. Candidate D violates the established E-to-F theorem: a
surviving `ColumnId` cannot change semantic or physical type. Candidate E holds
a staged S2 for the rest of an explicit transaction. Candidate F duplicates
the Round 60 evaluator. Candidate G is useful operationally but is not an
atomic statement.

The selected in-memory lowering is:

```text
Cold = bound pre-change ColumnId
Cnew = current next_column_id candidate
E    = pre-change TableDef plus nullable evaluation-only Cnew slot
P    = one deferred assignment Cnew := typed USING expression
F    = pre-change TableDef with Cold replaced by Cnew at the same ordinal
```

`Cold != Cnew`. The one deferred action is evaluated by
`evaluate_typed_row_expression`, including the production Round 60 CAST kernel.
The ordinary `DeferredBackfillProgram`, `RowProjection`,
`FinalOutputProjection`, `SchemaIndexTablePlan`, Heap rewrite materializer, and
recovery records remain the only implementation.

## Two source authorities, one row semantics

The prototype uses a deliberately narrow `DeferredSourceAuthority`:

```text
CommittedStorageView(S1)
TransactionStorageView(S1/P1)
```

The pristine route has no physical write participant after statement
acceptance. Its validation scan and final materialization read committed S1.
The post-DML route requires the existing exact one-table, one-storage,
one-write-participant adoption theorem and reads the transaction-visible S1/P1
view. A real zero-row same-table UPDATE still establishes that authority. Prior
read-only access, another-table access, or multiple participants remains
ineligible; no schema-writer exclusivity rule is relaxed.

Both routes call the same action builder, evaluator, observation accumulator,
E-to-F projection, index policy, and materializer. Only the source read view
and its durable authority evidence differ. The DML-before-DDL fixture changes
committed `bad` to transaction-visible `0`; validation and S2 contain `0`,
proving the committed row was not read by mistake.

## USING typing and future frontend boundary

USING binds against the complete pre-change table schema. A future typed HIR
operation should be frontend-neutral and retain exact identity:

```text
TypedAlterTableOperation::AlterColumnTypeUsing {
    column_id: ColumnId,
    target_type: SemanticType,
    using: TypedExpr,
}
```

The future Core request should contain `SchemaDependency`, `ColumnId`,
`SemanticType`, and typed relational `Expr`; it must never contain SQL text or
parser AST. Preparation binds the exact `TableId`, old `ColumnId`, all USING
`ColumnRef`s, target type, table version, and fingerprint. Execute never
re-resolves the old name. Existing stale-dependency rules therefore invalidate
both an old query and a prepared type change after winner publication.

USING is any current same-table scalar expression. It may read another column
or be a literal. Its result semantic type must equal the selected target type;
ALTER TYPE adds no implicit coercion. Consequently Text-to-Int64 requires
`legacy::BIGINT`, while `USING legacy` is a datatype mismatch. Casts inside the
expression independently follow the Round 60 matrix, including chained casts.
The direct `Cold -> target` pair is not an admission condition: an old Bool
column may be replaced by Int64 when `USING 0` already produces Int64 even
though `true::BIGINT` remains `CannotCoerce`.

All 15 current physical types are valid target representations when USING
already has the exact target semantic type. The first production scope should
still exclude same-physical rewrites: `TYPE BIGINT USING other_bigint` can
rewrite values and is not a no-op, so it needs a later product decision rather
than accidental admission. USING remains parameter-free with the current DDL
parameter model. Cross-table references, parameters, subqueries, aggregates,
joins, and expression forms outside the existing scalar universe are rejected
before scanning or reservation.

The Round 62 parser should add exactly one action with spans for `ALTER`, column
name, `TYPE`, target type, `USING`, and expression. It should require USING,
resolve target aliases through the existing Physical Types v2 rules, bind the
expression in the old table scope, and report target-type and expression errors
at their own spans. `SET DATA TYPE`, implicit conversion, and `CAST(... AS ...)`
remain separate language work.

## E, F, identity, constraints, and indexes

E appends Cnew after all old columns so old IDs and cached source positions are
stable. Because canonical `TableDef` currently requires unique names, the
prototype selects a deterministic collision-free name based on Cnew, adding a
numeric suffix on collision. The name is evaluation-only: it is never placed in
F, overlay inspection, a durable record, a catalog, or recovery. Two different
hidden spellings produce the same deferred-v1 semantic digest because the
digest binds table/column identities, typed expressions, and values—not names.
A slot-only `FrozenEvaluationSchema` refactor was rejected for this round as a
larger change to proven Round 53–60 code with no correctness gain.

E marks the synthetic slot nullable so its initial projected NULL is not
prematurely checked. The scan evaluates USING, assigns Cnew, then projects to F
and validates F. F gives Cnew Cold's public name, declaration ordinal, and
nullable contract while using the requested target type. Thus
`[C1, C2, C3] -> E [C1, C2, C3, C4] -> F [C1, C4, C3]`. A NOT NULL result that
is NULL fails during statement validation; nullable NULL remains NULL.

Primary-key conversion is rejected before scanning or allocation. For the
current supported single-column non-unique named BTree, the final private
inventory removes Iold and creates a fresh Inew on Cnew with the same public
name. It never retargets Iold. Unrelated index identities remain exact. Physical
S1 and its Iold/Cold tree are untouched until a durable winner; S2 builds only
the final inventory and accepts target-typed point lookups.

## Reservation and failure theorem

Reservation ordering R1 is selected:

```text
read current Cnew/Inew candidates under exclusive schema-writer authority
build and validate E, F, action, and final index inventory in memory
perform one full authoritative source scan
reserve Cnew durably
reserve Inew durably when required
install the accepted logical plan
```

Invalid source text, range errors, type mismatch, NULL constraint failure,
unsupported PK/index shape, active stream, group membership, and stale
dependencies therefore burn neither identity. R2 (reserve before scan) burns
on bad rows, R3 scans twice, and R4 invents a fake identity namespace.

Column and index reservations are separate existing journal records. If index
reservation fails after the column reservation became durable, Cnew is burned
and the transaction becomes rollback-required under the existing composition
loser theorem. No attempt is made to undo monotonic allocation history. A
successful statement followed by ROLLBACK similarly burns accepted Cnew/Inew
but leaves public Cold/Iold/S1 unchanged.

## Sealed statement state and scan counts

Successful acceptance enters test-only `TypeConversionReady`. It freezes F,
the final index inventory, E, and the deferred action but allocates no S2. The
state is explicit because ordinary `Composing` would let a planner bind to F
while the physical source still has S1/Cold layout. Only COMMIT and ROLLBACK are
allowed; SELECT, INSERT, UPDATE, DELETE, any further DDL, another type change,
and index refinement fail. Autocommit would make the state unobservable.

Acceptance performs exactly one full validation/observation scan. It retains
only O(schema width + expression/program) metadata and no converted-row cache.
Commit performs one source-copy/materialization scan and allocates one S2, no
S3. At the deferred-program layer each scan allocates an E row vector and the
nonidentity F projection vector per source row; those values are transient.

The ignored `alter_type_using_cost_probe` removes Heap I/O and fixture loading
from the clock, then runs the exact synthetic action, E-to-F projection, action
observation, and finalization verification over generated rows. One local
debug-profile observation (not a performance guarantee) was:

| Rows | validation | finalization | known transient row vectors | resident program estimate |
| ---: | ---: | ---: | ---: | ---: |
| 10,000 | 30,920 us | 31,115 us | 40,000 | 1,880 bytes |
| 100,000 | 330,304 us | 324,672 us | 400,000 | 1,880 bytes |

The resident estimate is constant with row count and the known transient vector
count is linear, supporting the no-row-cache claim. These timings deliberately
do not estimate Heap scan/rewrite or BTree construction cost.

## Persistence, recovery, and global commit

Pristine materialization writes the existing tag-25
`SchemaIndexChangeSetIntent`; tag 35 is absent because committed S1 is the
source authority. Adopted materialization writes tag 25 plus the existing tag-35
`SourceBackfillIntent`/clone-plan proof for transaction-visible S1/P1. This
asymmetry is intentional. Both use deferred action domain
`NetbaDB deferred backfill action v1\0`; no synthetic ADD/DROP/RENAME evidence,
new tag, persistent expression, hidden column record, or migration program is
needed. Round 50, Round 54, and Round 60 goldens remain unchanged.

The crash tests cover both authority modes across candidate reservation,
logical seal, tag 25, tag 35, staging/copy, prepare, Decision, Complete, and
three reopens. Before Decision, S1/Cold/Iold wins. At and after Decision,
S2/Cnew/Inew wins. Recovery uses the materialized snapshot and index plan; it
never reconstructs E, derives a hidden name, parses USING, or invokes CAST.

ALTER TYPE remains one structural transaction. The entry rejects Phase 3C/3D
group members before scan, writer acquisition, or reservation. It emits no
CORD v5 GroupDecision and calls no per-storage prepared batch barrier. A normal
success obtains one ordinary gap-free G and retains the conservative Phase 3B
Decision sync, participant/schema completion, Complete sync, publication
sequence. Existing coordinator code drains/checkpoints prior pending Completes,
so a previous group range G1..G2 is recoverable before the structural G3.
CORD v4 checkpoint tails, CORD v5 decoding, and the recovered sequenced
`complete_decision` fix are unchanged.

## Change Stream, Columnar, and maintenance isolation

The read-only Round 52 replacement preflight runs before the validation scan
and is repeated by the final materializer. `Enabled` and `Unavailable` remain
blocking states. The executable Phase 9 fixture enables a stream, creates and
advances a Columnar consumer, safely reclaims old NBCL batches, then proves the
replacement is still rejected with no scan, reservation, S2, or stream-state
mutation. Reclamation authority is not replacement authority. A runtime
retention pin is neither advanced nor released by this path. Disabled remains
the only positive state and does not automatically rebaseline S2.

Neither validation nor finalization calls reclamation, compaction, calibration,
or adaptive/automatic maintenance. Authoritative data comes only from S1 or
S1/P1, never Columnar. Old projections remain tied to S1/Cold after a winner;
they are not retargeted. Existing migration-busy admission prevents an
automatic/adaptive step from mutating S1, NBCL, Columnar manifests, or planner
state while the sealed plan owns structural authority.

## Access and authorization recommendation

Future `PreparedDdlStatement::access()` should report the target in
`schema_tables`, `read_tables`, and `write_tables`: schema authority is needed
to replace the object, read authority is needed to evaluate USING over all
source rows, and write authority is needed for the new physical table/index
inventory. An adapter must not infer that schema privilege grants expression
read privilege. The contract belongs at the transport-neutral prepared
statement boundary, not as PostgreSQL-only policy.

## Compatibility and future scope

Round 61 changes no Canonical Schema, NBSC/NBSM, NBSJ tags 1–35, NBCO v1,
CORD v1–v5, Heap/Page/WAL/status, IndexCatalog/BTree, Change Stream/NBCL v2,
Columnar NBCM/NBCS/NBCD/NBPC, Partition/LSM, Protocol v2/v1 compatibility,
Schema Spec v2, Inspection JSON v7, PostgreSQL framing, manifest, or SDK format.

The exact recommended Round 62 production scope is:

```text
ALTER TABLE t ALTER COLUMN c TYPE T USING expr
single runtime-created Single Heap; non-PK source; USING required
same-table current scalar expression; no DDL parameters
fresh ColumnId; public name/ordinal/nullability preserved
zero or one supported old secondary index automatically replaced with fresh IndexId
pristine committed-source and same-table post-DML adopted-source modes
terminal after success; one validation scan; one S2 copy pass; no S3
Round 52 stream guard; one ordinary structural G
```

Explicit exclusions remain implicit conversion, `SET DATA TYPE`, same-physical
USING rewrites, PK/FK/CHECK/UNIQUE/generated/default rewrites, multicolumn or
unique indexes, multiple type changes, post-type-change DML/DDL, cross-table
USING, parameters, unsupported expression forms, same-ID physical rewrite,
online/resumable migration, and imported/partitioned/LSM sources.

When productionized, expected PostgreSQL outcomes are ALTER TABLE success,
`22P02` invalid text, `22003` range, `42846` unsupported explicit cast, `0A000`
active Change Stream, and `25P02` after an error in an explicit transaction.
In Round 61 the exact current rejection remains parser
`UnsupportedFeature("ALTER COLUMN physical type conversion")`, mapped by the
PostgreSQL adapter to `0A000`; the next command in the failed transaction is
`25P02` until ROLLBACK. The real PostgreSQL 17.11 fixture is
`scripts/test-alter-type-using-round61-sql.py`.
