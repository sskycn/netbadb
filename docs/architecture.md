# NetbaDB architecture

## Boundaries

NetbaDB keeps application language concerns at the frontend boundary. A Go,
Rust, or future schema frontend should produce the same Canonical Schema IR:
tables, columns, physical types, semantic types, nullability, keys, and
relationships. The core consumes that representation and does not inspect Go
types or Rust application structs.

In this graph, `A -> B` means A depends on B. The current crate graph is:

```text
netbadb-schema -> netbadb-types
netbadb-schema-spec -> netbadb-schema + netbadb-types + serde + serde_json
netbadb-codegen -> netbadb-schema-spec + netbadb-schema + netbadb-types
netbadb-inspect -> netbadb-schema + netbadb-types
netbadb-hir -> netbadb-parser + netbadb-schema + netbadb-types
netbadb-rel -> netbadb-types
netbadb-compiler -> netbadb-hir + netbadb-parser + netbadb-rel
                    + netbadb-schema + netbadb-types
netbadb-tooling -> netbadb-compiler + netbadb-hir + netbadb-parser
                    + netbadb-schema
netbadb-index -> netbadb-types
netbadb-planner -> netbadb-index + netbadb-rel + netbadb-types
netbadb-storage -> netbadb-index + netbadb-schema + netbadb-types
netbadb-executor -> netbadb-planner + netbadb-rel + netbadb-storage
                    + netbadb-types
netbadb-core -> compiler + inspect + planner + rel + executor + storage
                 + schema + types
netbadb-protocol -> netbadb-types
netbadb-pgwire -> netbadb-types
netbadb-client -> netbadb-protocol + netbadb-schema + netbadb-types
netbadb-server -> netbadb-core + netbadb-protocol + netbadb-pgwire
                    + netbadb-schema
                    + netbadb-types
netbadbd -> netbadb-server
netbadb CLI -> netbadb-sdk embedded + netbadb-server + serde + serde_json
netbadb-lsp -> netbadb-tooling + netbadb-schema-spec + lsp-server + lsp-types
netbadb-sdk embedded -> netbadb-core + netbadb-inspect + netbadb-schema
                        + netbadb-types
netbadb-sdk remote -> netbadb-client + netbadb-schema + netbadb-types
```

No lower layer depends on a higher layer. In particular, storage does not
depend on planner or executor, executor does not depend on an SDK, and compiler
layers do not depend on tooling or LSP protocol types.

## Schema-driven tooling boundary

Editor diagnostics and runtime inspection are deliberately separate paths:

```text
Schema-only editing                         Runtime inspection

SDK Schema Spec v1/v2                      Database runtime state
        ↓                                           ↓
netbadb-schema-spec                         real planner + storage metadata
        ↓                                           ↓
Canonical Schema + source SQL               netbadb-inspect DTOs
        ↓                                           ↓
netbadb-compiler                             embedded SDK / offline CLI
        ↓                                           ↓
ToolingDiagnostic                           text / Inspection JSON v7
        ↓
netbadb-lsp UTF-16 adapter
```

`netbadb-tooling` converts the compiler's first `ParseError` or `HirError` into
a stable code, human message, and half-open UTF-8 byte span. It contains no LSP
URI, range, document version, or severity types. `netbadb-lsp` is the adapter
that tracks full editor buffers and converts those byte spans into zero-based
UTF-16 line/character ranges. It loads a validated SDK Schema Spec once at
startup and never opens a database, starts recovery, invokes physical planning,
connects to `netbadbd`, spawns the inspection CLI, or parses Inspection JSON.

The compiler remains fail-fast and each document represents one SQL statement,
so the current LSP publishes at most one compiler diagnostic per document.
Completion, hover, definitions, semantic tokens, and schema hot reload require
separate parser/source-index designs and are not advertised.

## Stable inspection boundary

Catalog metadata and the physical plan selected by the real planner cross a
one-way conversion boundary in `netbadb-core`:

```text
compiler / planner / storage internal state
                    ↓
              netbadb-core
          exhaustive conversion
                    ↓
             netbadb-inspect
                    ↓
             embedded SDK
                    ↓
       offline CLI text / explicit JSON v7
```

`netbadb-inspect` depends only on canonical schema and type domains. Its DTOs
retain stable table, relation-binding, column, semantic-type, expression, and
chosen-operator meaning, but never contain BTree handles, page/row identities,
WAL state, or planner IR. `Database::inspect_catalog` reads schema, persistent
index registration order, and cached `ANALYZE` snapshots without scanning or
refreshing them. `Database::inspect_statement` compiles once, invokes the same
planning path as execution, and converts the chosen plan without executing it,
opening a transaction, acquiring the writer, or appending WAL.

Inspection DTOs are observation results and never feed back into planning or
execution. The explicit text renderer is deterministic human-readable output,
not SQL, Rust `Debug`, a wire contract, or a versioned JSON API. Statistics are
last-`ANALYZE` snapshots and may be stale.

Columnar Phase 1 adds a second one-way observation and execution boundary.
Core owns a derived `ProjectionRegistry` beside the authoritative physical
bindings. Only exact fresh projection metadata crosses into the planner as a
separate snapshot; it never becomes an access path. Chosen plans expose a
stable `ColumnarScan` inspection node. The executor receives immutable
projection handles separately from mutable `TableStorage` and its read views.
See [Columnar Phase 1](columnar-phase1.md) for token, format, publication,
fallback, and vector-execution invariants.

[Adaptive Operations Phase 1](adaptive-operations-phase1.md)
adds the explicit synchronous Observe→Decide→Revalidate→Change→Measure→Outcome
composition layer for existing incremental projections. It consumes the
planner's immutable cost evaluator without letting planning mutate storage,
binds every proposal to global/source/projection/stream evidence, and uses a
first-class maintenance budget. Revert is runtime-only planner suppression of
one derived generation; it never changes authoritative data or the logical
projection definition.

[Adaptive Operations Phase 2](adaptive-operations-phase2.md) adds opt-in
execution feedback without changing that maintenance action surface. Planner
estimates are frozen from the snapshots that selected the physical plan,
Executor records real access-path counters, Core correlates them by typed
identity and deterministic runtime plan-node ordinal, and the planner's pure
evaluator converts raw evidence to canonical work units. A separate Phase 2
outcome can validate or runtime-suppress only the exact Columnar generation;
feedback never publishes user data, mutates cost coefficients, or changes
Inspection JSON v7.

[Adaptive Operations Phase 3](adaptive-operations-phase3.md) derives
collision-safe logical Query Shapes and physical Plan Variants from typed IR,
then aggregates explicit execution reports in a bounded caller-owned window.
DatabaseCommitSeq orders workload evidence but ordinary G advancement does not
expire it; schema or exact physical-target changes do. Deterministic integer
hysteresis can validate, hold, or runtime-suppress only the existing Columnar
generation and never persists history or calibrates planner coefficients.

[Adaptive Operations Phase 4](adaptive-operations-phase4.md) adds an explicit
runtime-only global access-class calibration overlay. Phase 3 aggregates are
kept separate by calibration epoch and QueryShape; a bounded integer-ratio
advisor must pass diversity, direction, deadband, clamp, and QueryShape-level
shadow-error checks before explicit apply. Base estimates and actual execution
evidence remain unchanged, every apply/revert advances the runtime epoch, and
neither G, durable formats, Columnar suppression, nor Inspection JSON v7 is
modified.

Columnar Phase 2D keeps the same one-way boundary but changes physical
ownership. NBCM v3 selects an indexed NBCS v3 Base and optional NBCD v2 Delta
chain. Open validates and retains only checksummed directories, zone maps,
mutation descriptors, exact suppression keys, and live `DeltaRowRef`s. The
executor's required-column set drives independent payload reads and checksum
verification. Generation leases keep old file handles valid across publish and
defer retirement until the last reader is gone. See [Columnar Phase
2D](columnar-phase2d-lazy-io.md).

The `netbadb` CLI is an offline adapter, not a new compiler or planner layer:

```text
deployment manifest v4
          ↓
netbadb-server ServerConfig bootstrap
          ↓
netbadb-sdk embedded Database
          ↓
netbadb-inspect DTOs
       ↙       ↘
human text   Inspection JSON v7
```

The CLI uses `ServerConfig` only to validate deployment configuration and
obtain required table expectation paths and canonical definitions. It never starts a TCP
server, creates a session, or applies network-principal authorization to local
filesystem access. JSON v7 is used whenever Physical Types v2 values or
metadata appear. With legacy types only, ColumnarScan remains v6,
IndexNestedLoopJoin uses v5, ordinary plans remain v3, and partition plans
remain v4. V1-v6 remain historical contracts and the DTOs themselves remain
serde-free.
Future runtime-inspection tooling, including MCP, consumes those DTOs directly
rather than spawning the CLI. The diagnostics-only LSP does not use this path.

Local inspection requires exclusive process ownership because persistent files
have no cross-process lock. `Database::open_tables` performs normal recovery,
so opening after a crash may redo or undo WAL state before inspection. Output
is fully rendered and the database successfully closed before stdout is
written; inspected SQL, including DML, is compiled and planned but never
executed.

## Canonical Schema IR

`netbadb-schema` stores database meaning in explicit Rust structs that are
independent of any application language. A column has:

- stable `ColumnId`;
- a name;
- a `TypeSpec` containing physical type and optional semantic name;
- nullability;
- primary-key metadata.

`Schema::new` is the fallible construction path and delegates to
`Schema::validate`; unchecked public construction is not available. Validation
rejects duplicate table IDs/names, duplicate column IDs/names within a table,
empty table/column names, and empty semantic-type names. Canonical names are
frontend-independent UTF-8 identities, and equality remains exact and
case-sensitive. The current SQL frontend still supports only its existing
unquoted ASCII identifier syntax and has no quoted identifiers. Names outside
that textual subset can be persisted and identified but cannot yet be referenced
through SQL text.
Zero-column tables remain valid and receive an identity with column count zero.
Primary-key metadata is preserved in identity, but this phase does not add key
enforcement; nullability remains the independently enforced write constraint.

Each validated `TableDef` has canonical encoding version 1. It starts with
`NBTS`, an explicit little-endian version and reserved field, then encodes the
table ID/name and declared column count. Every column follows in declaration
order with its ID/name, an explicit physical-type tag, optional semantic-type
name, nullability, and primary-key booleans. Strings are UTF-8 with little-endian
`u32` byte lengths. SHA-256 over these bytes is the 32-byte
`SchemaFingerprint`; no Rust enum discriminant, layout, `Debug` output, or map
iteration order participates.

## Persistent runtime schema authority (Round 17)

The [Round 16 decision](table-schema-lifecycle-round16.md) is implemented for
initial schema installation and reopen by the
[Round 17 catalog foundation](runtime-schema-catalog-round17.md). Core owns one
immutable `CommittedCatalogState`: a Schema decoded from the persistent snapshot,
SchemaGeneration, per-table TableSchemaVersion and durable identity high-waters.
Compiler/execution/inspection consume it alongside validated physical bindings and
storage handles. Index metadata remains in the existing per-engine catalogs.

Explicit `create_catalog`/`create_catalog_with_placements` bootstrap a full snapshot
at a caller-selected database root. `open_catalog` reconstructs every table and
placement without external schema. `open_catalog_with_expectation` accepts an
optional exact required subset; extra committed tables stay open and visible to
Core. Compatibility open signatures now validate expectations only. Their locator
sidecars are discovery evidence, never logical authority.

[SchemaCatalog v1](schema-catalog-v1.md) defines bounded binary fields, versions,
CRC32C, semantic types, declaration order, placement descriptors and checked
next-ID/exhausted states. A separate durable pending/initialized marker distinguishes
legacy/bootstrap-required state from a damaged initialized catalog. Shadow write,
file sync, atomic rename and directory sync publish the snapshot before the
initialized marker. Missing/corrupt initialized catalogs cannot fall back to a
manifest. Catalog loading precedes physical identity validation and WAL recovery.

Legacy adoption is explicit and requires a separately attested complete physical
inventory; old arbitrary filenames cannot independently establish completeness.
Physical metadata validates TableId, StorageId, fingerprint, engine and partition
identity. [Round 18](core-create-table-round18.md) adds Core transactional Heap
creation: exclusive schema-writer admission, non-rollback reservations, a private
materialized transaction SchemaView and Heap binding, typed same-transaction DML,
prepared NBSC snapshots, CORD v2 schema references and winner-driven promotion.
The committed bundle publishes only after physical/catalog durability. Statement
dependencies preserve unrelated prepared queries; private statements carry exact
transaction scope. Startup resolves journal/coordinator obligations before validating
the active NBSC/state pair. The immutable partition catalog remains baseline evidence;
NBSC includes newly created single Heaps without rewriting that evidence.
SchemaGeneration advances once per creation commit; table versions stay 1 and the
runtime revision increments with checked arithmetic. [Round 19](sql-create-table-round19.md)
adds generic SQL CREATE TABLE through parser declarations, typed HIR,
CompiledDdlStatement and PreparedDdlStatement. Core alone maps it to CreateTableSpec
and invokes the existing transactional lifecycle. Constraint enforcement and
non-Heap runtime creation remain future work.

[Round 26](sql-alter-table-round26.md) adds generic SQL ALTER without changing this
ownership boundary. HIR binds TableId/version/fingerprint and stable ColumnId where
applicable; Core alone maps the typed operation to AlterTableSpec and owns every
StorageId/ColumnId reservation, row rewrite, index rebuild, durable decision,
retirement, recovery, and GC relationship.

[Round 27](schema-transaction-composition-round27.md) audits the next composition
boundary without implementing it. The chosen future aggregate keeps one ordered
transaction-local logical overlay at provisional `V+1`, delays one final StorageId
and base-to-final Heap rewrite per dirty table until global seal/materialization,
and commits all changed tables through one `G+1/E+1` NBSC and one canonical CORD
participant set. First post-schema user physical execution or COMMIT seals the
whole schema transaction; later schema/index mutations are rejected. ADD ColumnIds
remain execution-time durable logical reservations, while physical StorageIds are
materialization-time reservations. Net-no-op composition leaves generation,
versions, epoch, and runtime revision unchanged but never reuses reserved IDs.

[Round 28](core-multi-alter-round28.md) implements that aggregate for the six
existing ALTER actions on runtime-created Single Heaps. Sequential canonical
overlays can touch one or many tables; global materialization performs one
base-to-final rewrite per dirty table, prepares one NBSC, and records one CORD v2
schema decision. NBSJ v1 tags 16–23 retain logical reservations, the typed final
plan, predecessor retirement, resolution, and replacement GC progress. CREATE/
DROP table mixing remains outside this boundary.

[Round 29](schema-index-composition-round29.md) adds CREATE/DROP INDEX to the same
ordered aggregate. Accepted CREATE reserves its logical IndexId durably before the
overlay changes, while physical work waits for global materialization. A schema-
dirty table builds one replacement Heap directly from the final index inventory;
an index-only table keeps its StorageId and contributes one Heap physical
participant. IndexCatalog v9 remains committed allocator authority, with retained
NBSJ reservations supplying the post-catalog effective floor. One CORD v2 decision
coordinates both strategies, and index-only transactions carry no fake NBSC.

[Round 30](table-ddl-composition-round30.md) brings managed runtime Single-Heap
CREATE/DROP TABLE into that aggregate. CREATE acceptance durably reserves only a
TableId and maintains a private V1 table/index namespace; physical StorageId and
Heap allocation wait until materialization. DROP records exact logical absence.
Final TableId-ordered classification emits CreateHeap, DropHeap, RewriteHeap, or
InPlaceIndexDelta plans. CREATE-to-DROP burns only the TableId, ALTER-to-DROP
elides replacement work, and DropHeap contributes no fake physical participant.
One prepared NBSC and one CORD decision publish every surviving object. NBSJ v1
tags 26--30 record TableId reservation, the typed table-object aggregate,
predecessor retirement, and exact-resource GC without changing older tag bytes.

Round 52 adds final-physical-truth admission to every managed-Heap replacement
producer. A surviving touched table whose final `TableDef` differs from its
base would emit `RewriteHeap`; before any replacement `StorageId`, intent,
stage, target, source-backfill target, or CORD participant exists, Core inspects
the exact source Change Stream. `Enabled` and `Unavailable` reject with a typed
feature-not-supported error; `Disabled` proceeds. CreateHeap, DropHeap,
canonical no-op, and InPlaceIndexDelta bypass the guard. See
[change-stream-schema-replacement-round52](change-stream-schema-replacement-round52.md).

[Round 31](migration-backfill-round31.md) audits the still-sealed migration-
backfill boundary without enabling it. Direct transaction-view tests prove that
ordinary DML reads and writes the private materialized Heap, and a test-only scan
validates `SET NOT NULL` against those writes. Heap v5 experiments prove that an
unindexed, layout-compatible provisional Heap can be retargeted to its final
fingerprint without rewriting rows, while indexed nullability also binds BTree
`IndexSpec` and is not Heap-header-only. The selected Round 32 architecture is a
controlled BackfillOpen phase followed by a DML-closing final-refinement phase,
one private Heap/owner metadata retarget, an immutable resource-only stage intent,
then an immutable finalization intent, one prepared NBSC, and one CORD v2 decision.
No intermediate nullable schema is published and recovery never replays SQL.

[Round 33](controlled-backfill-round33.md) extends this lifecycle with
`IndexFinalizing`: after DML and compatible refinement, logical CREATE/DROP
INDEX operations update an exact transaction-local inventory and reserve new
IndexIds durably. Finalization retargets the staged Heap first, applies only
the staged-to-final index delta from transaction-visible rows, and records a
new index-aware NBSJ finalization tag. The same StorageId and one CORD decision
are retained; the existing Round 32 tag 32 remains schema-only.

[Round 35](staged-index-evacuation-round35.md) adds a private typed branch for an
already-open managed Single-Heap backfill. Exact incompatible indexes are retired
inside S2 before refinement; every surviving persisted BTree spec is checked
against the candidate final schema. DML stays closed from the first evacuation,
finalization diffs the current physical S2 inventory, retargets before building
fresh-ID final indexes, validates the exact catalog, and then reuses tag 34, one
prepared NBSC, and one CORD decision. Public DROP-first indexed-nullability SQL
remains unsupported and no persistent format changes.

[Round 36](staged-indexed-nullability-sql-round36.md) adds an Execute-time SQL
dispatcher for that exact branch. It requires `BackfillOpen`/`IndexEvacuating`,
the one private managed Single-Heap target, and the exact IndexId in actual S2.
Ordinary DROP behavior is unchanged elsewhere. SQL evacuation closes DML,
public ALTER reuses the physical S2 compatibility proof, and replacement CREATE
reuses Round 33 finalization. DROP-first-before-S2 remains unsupported; no
persistent or protocol format changes.

[Round 37](migration-clone-round37.md) audits DROP-first-before-S2 without
enabling it. The selected Round 38 foundation keeps preparatory index DDL and DML
on transaction-owned S1, closes DML at the first compatible refinement, and then
streams the frozen S1 transaction view once into final-schema S2. S1 and S2 are
winner write participants in one CORD decision; only S2 becomes active, while S1
is immediately predecessor-retired under the existing real F1-to-F2 rewrite and
Round 25 horizon. Current live commit, copy, and GC primitives are sufficient,
but partial-participant schema recovery currently fails closed before client
admission and must be generalized before any SQL exposure. Same-V/F eager
physical replacement, participant detach, and new retirement causes are rejected
for this migration path. Round 37 changes no persistent format or public behavior.

[Round 38](source-backfill-late-clone-round38.md) implements that crate-private
foundation. Same-table DML remains in a transaction-private S1 view until an
accepted layout-compatible refinement closes relation execution. Commit creates
one final S2 from that view, prepares S1+S2 and one NBSC, and writes one CORD
decision. Exact tag-35/stage/tag-34/NBSC evidence lets startup finish either
partial participant order before retiring S1 and publishing S2.

[Round 39](drop-first-migration-sql-round39.md) adds the Core Execute-time route
from a one-table, drop-only `MaterializedIndex` source participant to that
unchanged lifecycle. The exact typed ALTER, current S1 participant, schema-
writer owner, managed Single-Heap placement, and absence of S2 are checked in
Core; PostgreSQL remains a thin authorization/SQLSTATE adapter. Failed native
SET NOT NULL stays open for repair, successful refinement closes relation
execution, and final index DDL is logical until the one late clone at COMMIT.
No SQL look-ahead or persistent-format change is introduced.

[Round 40](late-clone-layout-projection-round40.md) audits layout-changing
projection without exposing it. The selected design builds a target-ordered
mapping by `ColumnId`, streams the existing transaction-visible S1 view once,
omits dropped IDs, and synthesizes database NULL for durably reserved new IDs.
The existing materializer already has these physical mechanics; Round 41 must
make the projection invariant explicit and safely permit tag-16 ColumnId
reservation after the source index intent. Tag 35, stage/finalization evidence,
prepared NBSC, CORD, recovery, retirement, row format, and public SQL remain
unchanged in Round 40.

[Round 41](late-clone-row-projection-round41.md) implements that Core-only
foundation. One shared checked `RowProjection` now drives ordinary rewrite and
late clone by exact ColumnId, target order, survivor semantic/physical type,
and durable-new-column authority. The NBSJ state machine narrowly permits the
existing tag-16 reservation after the exact one-table drop-only tag-25 source
intent; rollback and predecision crashes retain its high-water burn. Nullable
ADD synthesizes NULL, DROP omits the exact ID, same-name replacement cannot
alias old data, and Policy B rejects a non-empty newly-NOT-NULL column during
projection. Effective layouts retain the existing one-S2/one-CORD lifecycle;
canonical ADD-then-DROP allocates no S2. Native and PostgreSQL SQL admission,
persistent formats, storage policy, and wire capabilities remain unchanged.

[Round 42](drop-first-layout-migration-sql-round42.md) changes only Execute-time
admission: nullable ADD and exact-ID DROP may enter the same source lifecycle
after the exact Round 39 DROP-only authority. ADD reserves a durable `ColumnId`
at Execute, target-ordered projection gives new IDs NULL and omits dropped IDs,
and prepared DROP cannot rebind to a same-name replacement. Multiple effective
changes retain one S2 and one source stream; ADD-then-DROP burns its ID with no
S2. PostgreSQL remains unaware of S1/S2 and recovery remains byte-driven.

[Round 44](post-dml-source-adoption-round44.md) productionizes the Round 43
Candidate B decision. An exact ordinary one-table INSERT/UPDATE/DELETE
transaction on a managed Single Heap may late-adopt S1/P1 for nullable ADD,
unindexed non-PK DROP, and table/column rename. A private
`AdoptedSourceTransaction` binds T/V/F/S1/P1, locator, generation/epoch, and
index digest before writer acquisition; the first accepted refinement closes
relational execution. Canonical no-op commits DML on S1 with no physical
intent. Effective finalization creates the first real rewrite intent and reuses
tag35, one S2, `RowProjection`, tag34, prepared NBSC, and CORD. DROP-first
Round 42 remains a separate broader refinement route. No persistent or wire
meaning changes.

[Round 46](post-dml-source-nullability-round46.md) adds only surviving-base-
column SET/DROP NOT NULL to that adopted source. A dedicated production-private
helper performs the complete Candidate-B preflight, proves base `ColumnId`
identity, and validates SET against transaction-visible S1 before acquiring the
schema writer. DROP does not scan. Indexed survivors keep logical identity;
effective changes rebuild their final `IndexSpec` on the one S2, while SET/DROP
round trips keep the original S1 and physical index. At that milestone,
new-column nullability and post-refinement relational execution remain closed. Round 48 adds the
bounded terminal final-index phase described below. The
existing tag35/stage/tag34/NBSC/CORD lifecycle and every persistent/wire format
are unchanged.

[Round 48](adopted-source-final-index-round48.md) adds the private
`AdoptedSourceIndexFinalizing` state, owning the unchanged S1/P1 adoption proof.
CREATE checks original prepared overlay T/V/F/ColumnId before durable tag24
reservation against canonical final V/F. DROP addresses exact logical T/I.
Neither statement changes physical S1 indexes. The existing adopted finalizer
revalidates source authority once before choosing the existing rewrite path
for a dirty TableDef, Ordinary InPlaceIndexDelta on the same P1 for an index-only
change, or SealedNoEffectiveChange for global no-op. Only the rewrite branch
allocates S2 and writes tag35/stage/tag34/NBSC. Index-only publication changes
runtime revision once and leaves V/G/epoch unchanged; the old digest proof is
consumed before the authorized physical delta. No persistent decoder changes.

[Round 50](deferred-new-column-backfill-round50.md) inserts one private logical
phase before Round 48 final indexes. `AdoptedSourceBackfilling` owns an ordered
typed program whose targets are durably reserved late `ColumnId`s and whose
reads are surviving base `ColumnId`s or bound literals. Execute streams the
exact transaction-visible S1, records affected rows plus a result digest, and
only then appends canonical semantic evidence. Projected late-column SET NOT
NULL validates `RowProjection + program`; finalization applies that same
ordered transform before constraints and the single S2 insert. Observation
verification detects source/program drift. The program is never durable;
existing tag25/tag35 evidence and CORD recovery remain authoritative after the
commit decision. Columnar stays derived and outside transaction participants.

[Round 54](deferred-virtual-row-round54.md) makes the same projected row the
sole production deferred-read model. Readable identities are surviving base
columns plus current-target, durably reserved late columns. Execute projects
the transaction-visible S1 row, synthesizes late NULLs, applies the accepted
prefix through the same `DeferredBackfillProgram::apply_row` used by
finalization, and evaluates WHERE plus every RHS against one immutable
pre-statement row. Assignments are then applied together. Program growth does
not stale prepared statements, while normal T/V/F changes do. S1 remains the
only pre-final physical authority; one final S2, tag25/tag35 evidence, Round 52
replacement admission and CORD recovery are unchanged.

[Round 56](deferred-terminal-structural-round56.md) adds the production-private
`AdoptedSourceFinalRefining` phase. The first successful terminal same-table
DROP/RENAME after a nonempty deferred program seals further deferred DML but
keeps additional DROP/RENAME and the first final index operation open. Frozen E
continues to own program slots while the private overlay owns final schema F;
dropped evaluation dependencies remain readable only during replay and are not
written to S2. The existing ColumnId-only E-to-F projection, one source pass,
one S2, tag25/tag35 evidence, CORD recovery, Round 52 replacement guard, and
Phase 3A single-snapshot publication remain authoritative.

[Round 58](deferred-index-evacuation-round58.md) makes terminal index
evacuation a production context in that same state machine. Only an effective
DROP INDEX in Backfilling or FinalRefining removes the exact I1 binding from the
private logical inventory and enters or remains in FinalRefining. Physical and
committed S1 retain I1/C2, and the captured index digest is revalidated without
being rewritten. A same-name CREATE reserves fresh I2/C4 and seals
IndexFinalizing. The existing finalizer chooses one S1-to-S2 rewrite or an
honest same-S1 index delta; Round 52 admission, CORD recovery, Phase 3A
publication and Phase 3B structural Complete synchronization are unchanged.

[Phase 3B](phase3b-global-commit-pipeline.md) preserves that one-G publication
model while moving only a pure data transaction's Complete sync out of its
foreground path. Decision sync remains the irreversible commit point;
participants commit durably before publication. The next Decision sync drains
prior Complete checkpoints, and flush/checkpoint/close drain the final one.
Structural transactions keep immediate Complete synchronization. No persistent
format, async runtime, background worker, group commit, or multi-writer model is
introduced.

[Round 20](core-drop-table-round20.md) adds Core-only transactional DROP for one
exact active Single Heap. `DropTableTarget` binds TableId/version/fingerprint;
the transaction materializes NBSC G+1 with that identity removed, invalidating old
prepared dependencies before storage access. Coordinator remains the winner
authority. NBSJ retains exact terminal physical-retirement evidence while the Heap,
WAL/status and contained BTree pages remain on disk. Commit removes schema,
placement and registry entry at one synchronous publication boundary; rollback
leaves the exact old identity/data/indexes active and does not advance generation or
allocator floors. Reopen resolves DROP before opening only active NBSC storages.
Physical unlink/GC, LSM and partition DROP remain future work.

[Round 21](sql-drop-table-round21.md) adds a thin generic SQL adapter. The parser
keeps only a logical name/span; HIR resolves the transaction SchemaView to the shared
exact TableId/version/fingerprint target; compiled/prepared DDL retains it until
Execute calls Round 20 Core. Statement access records schema-write authority and a
separate exact schema target, so `schema_admin` does not imply or require row DML
grants. Native and PostgreSQL Simple/Extended paths add no identity or persistent
format. Same-name replacement fails stale/undefined rather than rebinding.

[Round 22](retired-heap-gc-round22.md) adds explicit physical retirement for one
exact runtime-created Single Heap. Append-only completed Coordinator decisions
form a durable per-StorageId recovery horizon; Core resolves and closes local Heap
recovery before persisting a retry-only NBSJ GC intent. Storage authors the exact
Heap/WAL/status bundle, Core adds owner/link metadata, and deletion synchronizes the
parent directory before durable Complete. Startup resumes only existing intents,
never selects candidates. Deleted history remains authoritative for non-reuse and
completed coordinator replay. LSM, partitions, imported locators, background GC and
metadata compaction remain deferred.

[Round 23](schema-evolution-round23.md) audits schema evolution without exposing an
ALTER production path. Heap and LSM row payloads are positional tagged scalars with
no arity, ColumnId or schema version; Heap metadata separately requires the complete
canonical fingerprint. The selected first-version architecture is therefore an
offline staged copy-on-write replacement for every supported ALTER on one
runtime-created Single Heap: preserve TableId and surviving ColumnIds, advance the
table version and fingerprint, allocate a new StorageId, stream current logical
rows through a typed ColumnId transform, rebuild every active index, and retain the
old Heap under its old schema until a generalized Round 22 recovery horizon permits
GC. LSM, partitioned/imported storage, online mixed schemas, SQL syntax and physical
type conversion remain deferred. Round 24 is the Core Heap rewrite foundation, not
a frontend ALTER slice.

`compile_sql_statement` parses once and selects relational or DDL lowering from the
AST. Core's `prepare_sql_statement_in` shares the transaction SchemaView and
identity/version/fingerprint dependency rules of `prepare_statement_in`; sessions
use this combined entry point for both statement families. Prepared statements that
reference staged tables retain exact transaction scope; plans over committed tables
remain reusable. CREATE declarations contain no IDs, and schema-writer admission/
reservations occur only during Execute. DROP preparation similarly performs no
writer admission or journal/catalog/storage mutation.
`StatementAccess::schema_write` describes schema mutation without a sentinel ID.
Server authorization checks the optional default-false manifest principal
`schema_admin` for all network DDL (index DDL also retains table-write checks).
An active creating transaction may read/write only its own staged TableId; this
exception is never written into grants, manifests or catalogs. Committed tables
remain default-denied unless externally granted. Autocommit sessions retain their
transaction handle through fallible commit/rollback and allow pending commit retry.

## Compiler and plans

The first query subset follows:

```text
source → AST → resolved/type-checked HIR → logical plan → physical plan
```

HIR owns source-level resolution and semantic type checking. Relational IR
owns relational meaning and column provenance. Core snapshots each table
storage's advertised access paths; the planner selects exact point
IndexScan or SeqScan access and the correctness-first nested-loop
implementation for logical INNER JOIN. The executor evaluates typed
expressions against rows returned by storage.

The implementation uses IDs and owned values between layers. It does not keep
long-lived references to pages, frames, or tuples, leaving room for future
buffer management and concurrent execution without spreading lifetimes across
the whole system.

## Relation bindings and INNER JOIN

`TableId` identifies a catalog table; `RelationBindingId` identifies one
query-local occurrence in a FROM/JOIN tree and is never persisted. Bindings are
allocated deterministically in source order. This distinction makes a self
join such as `employees e JOIN employees m` two independent relation instances
even though both scans target the same `TableId` and use the same `ColumnId`
values.

Each binding records its catalog table and exposed name. With an alias, only
the alias is exposed; otherwise the table name is exposed. Duplicate exposed
names are rejected. Qualified lookup first resolves that name and then the
column. Unqualified lookup searches all visible bindings and succeeds only for
exactly one candidate; zero candidates are unknown and multiple candidates are
ambiguous. JOIN scopes grow from left to right: an `ON` expression sees the
left subtree plus its current right binding, never a future relation.

Resolved HIR and relational `ColumnRef` values carry binding, table, and column
IDs. Alias strings remain diagnostic metadata and are not used by execution.
Logical scans carry their binding identity, and chained joins form a
left-associated tree:

```text
LogicalPlan::Join(left plan, right plan, Inner, typed predicate)
    ↓ planner (no join reordering)
PhysicalPlan::NestedLoopJoin, eligible PhysicalPlan::HashJoin,
or eligible PhysicalPlan::IndexNestedLoopJoin
```

Every physical node has binding-aware output columns. Expression and projection
lookup uses `RelationBindingId + ColumnId`, which remains unambiguous for self
joins. A scan row retains one hidden, storage-owned `StorageRowHandle` for DML;
the current Heap implementation privately maps it to a generation-safe `RowId`.
A joined row combines scalar values and intentionally drops mutation identity
because multi-table UPDATE/DELETE are not supported.

The planner keeps NestedLoopJoin as the general implementation. At the current
join node it considers HashJoin only for an INNER JOIN over two direct logical
scans, with existing table statistics on both sides and a necessary cross-side
typed column equality found by deterministic left-to-right traversal through
AND nodes:

```text
Join
 |
 +-- no statistics, non-equi, or unsupported child/predicate
 |      -> NestedLoopJoin
 |
 +-- analyzed direct Scan × Scan with cross-side equality
        +-- costed ordered point index on logical right beats right SeqScan
        |      managed-page work and NestedLoop work -> IndexNestedLoopJoin
        +-- left_rows + right_rows < left_rows * right_rows -> HashJoin
        +-- otherwise -> NestedLoopJoin
```

Both costs use checked `u128` work units. Missing or stale statistics can affect
only the algorithm choice; the complete predicate remains the semantic source
of truth. There is no join reorder, selectivity estimate, or global optimizer
cost model.

The cost-unit audit keeps row CPU work and storage access work explicit rather
than adding them into one undocumented scalar. NestedLoop/Hash eligibility uses
row work. IndexJoin then compares its additional right-side point work with the
right engine's sequential work because both alternatives already consume the
same logical left. `managed_page_count` means every managed page the engine's
sequential path must visit; for the current colocated Heap file this includes
pages that SeqScan validates and skips for B+Tree/catalog structures. Point
base, expected point I/O, range startup, and returned-candidate weights are
dimensionless neutral integer work—not nanoseconds. Storage owns optional
hints, planner compares them, and executor only executes the selected node.

IndexNestedLoopJoin is deliberately narrower than general join enumeration. It
requires distinct, non-partitioned direct logical scans, an INNER join, the
same deterministic compatible equality used by HashJoin, both table row-count
snapshots, and statistics for an ordered point-capable access path on the
logical right equality column. Since both candidates read logical left, its
checked additional inner work is one existing point-lookup cost per estimated
left row. The HashJoin comparison uses the same `managed_page_count` SeqScan
cost already used by Filter access-path selection, rather than mixing point
work with right-table row count. It wins only when strictly cheaper than that
full scan and NestedLoop work; registration order breaks equal index candidate
costs, while a tie with either join alternative preserves the older choice.
Stale statistics may change only performance.

Phase 73 retained Heap `cost_hints = None` and the generic
`1 + tree_height + estimated_matches` fallback. LSM continues to publish its
dynamic level/Bloom-derived hints. A tested Heap-data-pages-only scan-cost
pilot was rejected because it did not describe the current physical SeqScan;
no engine-kind branch or outer-row threshold remains in the planner.

The physical node keeps logical left as its sole child and explicitly records
the right binding/table, required right columns, equality key, and opaque
access-path ID. The executor validates all of that setup even for empty input,
then visits left in batches of at most 256 rows. Each non-NULL outer key issues
one `point_lookup_columns_with_view`; one lookup result is consumed and dropped
before the next probe. NULL issues no lookup. Candidate rows retain storage
index order, the complete eager predicate is checked before projection, and
the result is therefore exact logical-left-major/right-minor order without
materializing the full right relation. Heap and LSM use the unchanged shared
point-lookup API; self joins and partitioned inputs remain on existing plans.

NestedLoopJoin materializes both child results, iterates left rows outside and
right rows inside, and evaluates the typed `ON` predicate through a non-owning
joined view:

```text
materialized left child + materialized right child
    -> joined value view for predicate evaluation
    -> TRUE only: materialize left values + right values with row_id None
```

FALSE and UNKNOWN pairs allocate no combined value vector and copy no full row;
ordinary equality therefore still never joins NULL to NULL. TRUE pairs are
materialized in left-then-right order. Join predicate positions are execution
layout properties and are bound only after both child outputs are known:

```text
typed Join predicate Expr with ColumnRef identity
        ↓ executor bind once through RelationBindingId + ColumnId
private position-bound expression borrowing the original Expr
        ↓
candidate-pair evaluation through checked positions
```

NestedLoopJoin binds its complete `ON` expression once before the candidate
loop. HashJoin retains its separately prebound hash-key positions and binds the
complete residual predicate once before probing buckets. The private bound
tree is not Rel IR or planner IR, exposes no public API, and does not replace
logical column identity with `usize` positions. It borrows literal values and
diagnostic column names from the original expression; a missing input field or
short runtime row remains a typed `MissingColumn` error. Filter predicates and
UPDATE assignments intentionally retain dynamic position resolution.

NestedLoopJoin also uses the bound tree for exact runtime rejection before its
inner loop:

```text
bound Join predicate
    -> first necessary direct cross-side inequality under AND
    -> normalize operands to left <op> right child positions
    -> borrow exact non-NULL min/max from materialized right rows
    -> can this left row match any right row?
         no  -> skip the complete right loop
         yes -> existing right-order loop + complete bound predicate
```

`>` and `>=` use the minimum right value; `<` and `<=` use the maximum.
Right NULLs are ignored because their comparison is UNKNOWN, an all-NULL right
key makes the join empty, and a NULL left key skips that left row. Extraction
descends only through AND because each conjunct is necessary for a TRUE result;
it never descends through OR or NOT. Reversed operands are normalized, multiple
eligible conjuncts use the first left-to-right match, and `compare_values`
remains the ordering authority. The extreme is borrowed from the current right
rows and is neither a planner estimate nor persistent statistics.

In Phase 7H alone, a left row that can match scans every right row in its
original order and evaluates the full predicate. That zero-candidate step does
no child sorting or candidate-range narrowing and changes no physical plan or
inspection contract.

Phase 7I retains that zero-candidate fast path, then adaptively narrows possible
probes when the first necessary inequality has a useful exact range:

```text
bound necessary inequality
    -> Phase 7H exact extreme rejection
    -> potential left row indices in original order
    -> borrowed-key sorted left/right index auxiliaries
    -> exact candidate count with a two-pointer sweep
    -> checked integer runtime work comparison
         nested work <= sweep work -> Phase 7H NestedLoop fallback
         sweep work < nested work  -> ordered candidate sweep
    -> complete bound predicate for every selected pair
    -> per-left output buckets
    -> flatten in original left order
```

The runtime choice is execution-local and uses no ANALYZE statistics or magic
selectivity ratio. Nested work is potential-left count times total right rows.
Sweep work adds the exact candidate pairs, checked `n * ceil_log2(n)` index-sort
proxies, and a checked ordered-set proxy; overflow and ties conservatively use
the nested loop. The 100%-reject case returns before sorting, and an all-pairs
boundary proof also avoids building auxiliaries for fully dense candidates.

Sorted auxiliaries contain only original row indices and borrow scalar keys.
Right NULL keys are excluded, left NULL keys are not potential probes, and all
ordering uses the same fallible `compare_values` implementation. For `>`/`>=`,
an original-right-index `BTreeSet` grows with keys `<`/`<=` each ascending left
key; for `<`/`<=`, it starts with every non-NULL right and removes keys `<=`/`<`
the left key. Each right index changes set membership at most once. Iterating
the set preserves original right order, while per-left buckets restore original
left order after key-ordered processing.

The auxiliary memory bound is O(left rows + right rows + output rows): candidate
pairs are never materialized. Only a complete predicate result of TRUE creates
an owned output row. This remains an internal NestedLoopJoin strategy, not a new
physical operator, planner cost, persistent statistic, or inspection contract.

Phase 7J applies required-column propagation only after the complete physical
query tree has been selected. Logical scans retain the full resolved relation,
and UPDATE/DELETE keep full base rows. The public query planner performs one
top-down pass over the raw physical tree:

```text
typed physical query
    -> required source columns (RelationBindingId + ColumnId)
    -> preserve Filter predicate and Sort key columns
    -> preserve group keys and aggregate column inputs
    -> preserve Join predicate columns and defensive HashJoin keys
    -> split Join requirements by binding-aware child output identity
    -> prune SeqScan / IndexScan / RangeIndexScan columns in source order
    -> executor passes ordered ColumnId projections to Heap
    -> Heap validates every encoded scalar
    -> own only requested ScalarValues
```

Membership is deduplicated, but operators never reorder source columns by
discovery order. A repeated result projection remains repeated while its base
scan reads the source once. `COUNT(*)` can drive a zero-column scan. Phase 7R's
executor specialization consumes that exact shape through the Heap presence
summary, so direct all-star aggregates no longer construct one empty
storage-row-handle-bearing execution row per live tuple.

Join children may retain columns needed only by their own predicates. The join
executor therefore binds the current predicate against the concrete
left-then-right child layout, then projects matching rows to the current join's
declared columns. This lets an inner join consume its private predicate columns
without leaking them into an outer join, while outer ON columns still propagate
through a chained left subtree. Self joins remain distinct because TableId or
ColumnId alone never determines membership.

The Heap projected-read APIs resolve typed ColumnIds to schema positions once
per read or scan. Their decoder borrows Text as `&str` while parsing, validates
tag, length, bounds, UTF-8, physical type, nullability, truncation, and trailing
values for every schema column, then allocates owned Text only for selected
values. Request order and duplicates are preserved. Full reads use the same
borrowed decoder core, page validation remains once per immutable Heap page per
scan, and point/range reads retain page bounds, tombstone, and RowId generation
checks. Row encoding and every persistent format are unchanged.

Phase 7K removes the next temporary owner at the executor Project boundary.
Project consumes its `ExecutionRows`, so one operator-level `ProjectionPlan`
resolves binding-aware positions and their last uses before processing rows:

```text
owned input ExecutionRows
    -> identity positions: move rows and values Vec directly
    -> unique subset/reorder: move selected ScalarValues from owned slots
    -> duplicate source used N times: clone N - 1, move at last use
    -> fully owned QueryResult
```

The identity path neither rebuilds each row's values Vec nor clones a
ScalarValue. Generic projection preserves checked indexing, output order,
duplicates, nullable/type metadata, and the opaque `StorageRowHandle`. A
repeated Text output still
requires independent String owners, so the last-use rule only removes clones
that are not semantically necessary. Join candidate materialization is
unchanged because child rows may produce several matches; only a temporary
joined row that is already exclusively owned can use the same projection
helper.

This is not a zero-copy result path. Selective Heap decode still creates one
owned String for each selected Text value because QueryResult owns its rows;
Phase 7K moves that owner through Project instead of allocating a second,
short-lived String. PhysicalPlan, planner decisions, storage APIs, Inspection
JSON v3, and persistent formats are unchanged.

Join-bound evaluation represents an intermediate scalar as either a borrowed
row/literal value or an owned computed value:

```text
bound Column/Literal
    -> borrow row/literal ScalarValue
    -> evaluate binary semantics through ScalarValue references
    -> own only Bool/NULL results produced by Binary, Unary, or IS NULL
```

The borrowed lifetime is limited to one candidate evaluation and cannot escape
the materialized child rows or typed predicate. Binary comparison and
three-valued truth semantics have one reference-based core; the existing owned
evaluator is a thin caller of that core, so Filter and UPDATE remain owned and
dynamically resolved. AND/OR still evaluate both sides without short-circuiting.
The existing expression checker requires BOOL while allowing nullable BOOL,
and nominal compatibility prevents JOIN from comparing distinct semantic types
with the same physical encoding. Text still becomes owned once per decoded
storage row; repeated candidate-level cloning and the separate HashJoin bucket
key allocation are removed.
For the planner-produced direct SeqScan × SeqScan INNER shape, PhysicalPlan
continues to preserve logical left and right. The executor privately reads each
table's last ANALYZE `row_count` and chooses the build side without rewriting
the plan:

```text
HashJoin
   -> read left/right ANALYZE row_count
   -> left < right?
      yes                              no / tie / missing statistics
       |                                |
       v                                v
   build left                       build right
   stream right                     stream left
       |                                |
       v                                v
   outputs_by_left                  direct outputs
       |                                |
       +----------> left-major/right-minor result <----------+
```

The choice follows the last persisted ANALYZE snapshot; it is not an exact
runtime cardinality measurement. A stale estimate may select the actually
larger side but cannot change results. Strict comparison deliberately makes
ties build right, and either missing statistic also preserves the historical
right-build path. No ratio threshold, planner field, join reorder, or
inspection contract participates in the choice.

Build and streamed NULL keys are skipped because SQL `NULL = NULL` is UNKNOWN.
The bucket map borrows each logical key directly from the immutable, fully
materialized selected build rows and stores ordered indices rather than cloned
rows or owned keys. Equal values from different Text allocations therefore
share one logical bucket through `ScalarValue` value Hash/Eq, not pointer
identity; the first inserted build value remains the borrowed map key. Hash
iteration order never determines results. Bool, Int64, UInt64, and Text use
exact ScalarValue equality, while semantic compatibility protects nominal
types and self joins use binding plus column IDs to identify sides.

BuildRight retains the existing direct output path: left rows stream in input
order and ordered right indices produce right-minor order. BuildLeft streams
right rows in at-most-256-row batches and appends each owned match to a private
bucket for its materialized logical-left row. Moving those buckets out in left
row order restores the same exact left-major/right-minor output, including
duplicates, reordered or repeated projections, and zero-width output. This is
executor behavior, not an unordered SQL result-order guarantee.

The selected build rows and borrowed map are separate local variables rather
than a self-referential owner. Build rows remain immobile and immutable from
bucket construction through the complete stream and output projection.
QueryResult values remain fully owned; projecting a Text value still creates
the required output owner independently of the borrowed lookup key.

HashJoin setup resolves both key positions, joined predicate positions, output
positions, storage schema identity, and available row-count snapshots before
executing either selected side. Only the current direct-scan INNER shape enters
this path. Unsupported or malformed setup retains the authoritative
left-then-right, fixed-right materialized implementation; once build or stream
execution starts, runtime and storage errors propagate without replay. Both
sides use the statement's existing read views, including self joins.

The input-side intermediate boundary is
`O(selected build + hash metadata + 256-row streamed batch + output)`, or
approximately `O(min(left, right) + batch + output)` when ANALYZE correctly
ranks the sides. BuildLeft additionally owns one outer output bucket per
materialized left row; that `O(left build rows)` metadata is within the selected
build scale, while the fully owned final output remains unbounded. Phase 68's
borrowed keys, standard-library RandomState HashMap, complete eager residual,
and expected linear build/stream work are unchanged.

Query operators are arranged as
`Scan/Join -> Filter -> Sort -> Project -> Limit`, allowing sorting by source
columns that projection omits. `SELECT *` follows left-to-right relation and
schema order.

Core multi-table catalogs compose one existing heap file per `TableId`. JOIN
therefore introduces no transaction-layer changes. The current storage format
matrix is documented in [Round 10](page-generation-round10.md); JOIN does not
change recovery, checkpoints, or single-writer rules.

## Typed ORDER BY

`ORDER BY` accepts one or more source-column keys, qualified or unqualified,
and resolves them through the same complete `FROM`/`JOIN` `RelationScope` used
by other expressions. HIR makes every option explicit: omitted direction is
`ASC`; omitted NULL placement is `NULLS LAST` for ascending keys and
`NULLS FIRST` for descending keys. Alias names, ordinals, and arbitrary sort
expressions are outside this slice.

Logical `Sort` and physical `Sort` preserve the input's binding-aware output
shape. The executor resolves all key positions once, validates that each
non-NULL runtime value has the key's declared physical type, and then performs
a stable in-memory lexicographic sort. NULL placement is applied independently
of direction; direction reverses only ordinary non-NULL comparison. Stability
preserves input order among equal keys for the current plan, but does not
promise a permanent tie order if future access paths change. A caller that
requires a total order must provide enough keys.

The executor privately specializes the existing
`Limit -> Project -> Sort -> batch-capable child` tree as bounded Top-N. It
resolves sort and projection positions before traversal, then consumes every
produced row without sending Limit cancellation upstream. Each row receives
the same runtime sort-value validation as full Sort. A fallible maximum heap
keeps the worst retained candidate at its root, so intermediate ownership is
bounded by one 256-row input batch plus at most K complete rows. Key equality
is broken by a monotonic input ordinal; final key-plus-ordinal sorting therefore
matches stable Sort before Limit. Hidden sort columns remain in candidates
until the existing move-aware ProjectionPlan produces the final owned rows.
K=0 still consumes and validates the complete logical input while retaining no
candidate. Setup failures, unsupported producers, full Sort without Limit, and
other plan shapes use the authoritative legacy executor. There is no new
PhysicalPlan variant, cost model, spill path, or storage-facing ordering API.

## Full Sort ownership and spill boundary

Full Sort without LIMIT remains the authoritative materialized path:

```text
complete child
    ↓
ExecutionRows { rows: Vec<ExecutionRow> }  // N owned rows
    ↓
resolve key positions and validate all N rows
    ↓
stable in-memory Vec::sort_by
    ↓
the same N owned rows
    ↓
Project -> QueryResult { rows: Vec<Vec<ScalarValue>> }
```

The Sort input is `O(N input rows)`. The public result is also fully owned, so
a query returning N rows has an Ω(N final output) memory lower bound. External
sorting could reduce additional sorting/intermediate peak memory, but it could
not change total query memory to `O(run_size)` while this result contract is in
place. Retained-wide shapes such as `SELECT id, payload ORDER BY payload` keep
the Text in the final result. Hidden-wide shapes such as
`SELECT id ORDER BY payload` temporarily require `id + payload` in Sort but
retain only `id` after projection; only the latter exposes a width difference
that spill or an indirect key/index representation could remove.

Phase 71 adds no release instrumentation or production Sort change. Its
test-only `FullSortStats` is populated by the same private sorting function used
by production. At 513 rows, a narrow one-column Sort reports 513 rows and 513
scalar slots before and after sorting. A hidden 128-byte Text-key Sort reports
513 rows, 1,026 input scalar slots, and 65,664 logical owned Text payload bytes;
the final projection has 513 scalar slots and no Text. These are logical owned
payload counts, not allocator or RSS measurements.

Any future external merge must preserve current stable ties by carrying a
global input ordinal or an equivalent stability mechanism across runs. It must
also preserve error timing: full Sort validates every runtime key before
sorting, so a run-based implementation must not emit early rows or otherwise
skip malformed keys in later input. A future spill phase would additionally
need an explicit memory budget, ephemeral row/ordinal codec, run lifecycle,
k-way merge, and cleanup on every error. Phase 71 deliberately introduces none
of those boundaries and defers spill because its measurements do not justify
that infrastructure under the fully owned result contract.

## Typed global and grouped aggregates

Aggregate function names are contextual only in SELECT projection. A plain
identifier such as `count` remains a source column, while `COUNT(*)` or
`COUNT(column)` is an aggregate. Aggregate arguments are limited to `*` for
COUNT and qualified or unqualified source columns for all four functions. HIR
resolves column inputs and GROUP BY keys through the complete relation scope.
For grouping queries, every projected source column must be one of the
binding-aware group keys. A key need not be projected, GROUP BY may contain
multiple source columns, and GROUP BY without aggregates forms distinct groups.
Wildcard projection is rejected with GROUP BY. Grouped queries also reject
`ORDER BY` in this slice.

Normal and aggregate plans remain distinct:

```text
Scan/Join -> Filter -> Sort -> Project -> Limit
Scan/Join -> Filter -> Aggregate -> Limit
```

An `OutputField` separates source identity from result metadata. Source fields
retain their `RelationBindingId + TableId + ColumnId`; a `DerivedField` carries
only its deterministic name, semantic type, and nullability. Consequently,
`COUNT(*)` never receives fabricated catalog or query-source IDs. Logical and
physical Aggregate operators keep `group_keys` (group identity) separate from
ordered `AggregateOutput` items (result shape). A group-key output remains a
Source field and an aggregate remains Derived, so SELECT order is preserved
without disguising aggregation as projection or inventing identifiers.

The aggregate executor updates every aggregate state in one pass. Eligible
batch children stream bounded owned rows; the authoritative legacy fallback
materializes its input. Group lookup uses randomized hash-to-index metadata and
exact comparison against keys owned only by a `Vec<GroupState>`, whose insertion
order defines deterministic first-seen output; randomized hash iteration never
shapes results. Grouping is currently fully in memory. With no group keys, zero
input rows still form one implicit group:
COUNT returns zero, while SUM/MIN/MAX return NULL. With one or more keys, groups
are created only when rows arrive, so empty input produces zero rows. LIMIT is
above Aggregate and therefore limits complete result groups, never input rows.
Runtime key and aggregate values are checked against typed physical inputs and
SUM uses checked signed or unsigned addition.

Two executor-private global COUNT specializations avoid that generic row
materialization without changing physical plans. A direct `Aggregate →
SeqScan` whose nonempty outputs are all COUNT can request an exact Heap presence
summary when every scan column is consumed by a COUNT(column). This includes a
zero-column scan with one or more COUNT(*) outputs:

```text
direct Aggregate COUNT outputs
              ↓
       direct SeqScan[] ?
          /          \
        no            yes
        |              |
     generic   scan_presence_counts([])
                       ↓
              full row validation
                       ↓
              exact live_rows u128
                       ↓
         checked ordered COUNT outputs
```

Phase 7R adds no storage counter: it reuses the Phase 7M summary and performs
one checked SQL `u64` conversion against each output's exact `AggregateExpr`.
The summary is a current Heap traversal, not statistics, a row-count cache, an
index-only count, or an O(1) slot-header shortcut. Every persisted scalar is
still decoded and validated; tombstones, slot reuse, relocation, index and
ANALYZE pages, stale statistics, and reopen therefore retain exact current-row
semantics. An all-star aggregate over a nonempty SeqScan is rejected by the
specialization rather than guessed about.

Phase 7N additionally recognizes only `Aggregate → Filter → SeqScan`, with all
outputs COUNT and at least one COUNT(column). It splits the scan's source-order
columns into values needed by the predicate and NULL-presence bits needed by
COUNT, then consumes each completely validated live tuple synchronously:

```text
Aggregate COUNT outputs
        ↓
direct Filter → SeqScan eligible?
       / \
     no   yes
     |     |
 generic  predicate values + COUNT presence
                 ↓
        one validated Heap visitor scan
                 ↓
       dynamic borrowed leaf evaluation
                 ↓
        TRUE updates checked counts
                 ↓
          one materialized result row
```

The Heap visitor knows only ColumnIds, runtime scalar-view requests, and
presence requests; storage never receives relational `Expr` or SQL truth
semantics. It invokes the callback only after full row-codec validation.
Executor still applies three-valued logic, so only TRUE qualifies and FALSE or
UNKNOWN is discarded. Count-only Text never becomes an owned String.

Phase 7O changed only the private evaluator called by that filtered-count
consumer. Phase 7P adds one language-independent `ScalarRef<'a>` runtime view
in `netbadb-types` and makes the Heap decoder return it directly. This view is
not a persistent, wire, schema, or SQL IR type. Its Text variant can borrow the
validated record payload only during a higher-ranked synchronous callback.
Phase 7Q then reuses the existing Join `BoundExpr` to resolve predicate source
positions once before the Heap traversal:

```text
FilteredCountPlan
        ↓
source-order predicate fields
        ↓
bind_expression once
        ↓
BoundExpr checked positions
        ↓
persisted row payload
        ↓
decode + complete validation
        ↓
ScalarRef<'row>
  Bool/Int64/UInt64 by value
  Text(&str)
  Null
        ↓
HRTB synchronous visitor callback
        ↓
position-indexed bound evaluation
        ↓
borrowed Column/Literal scalar views
        ↓
ScalarRef binary/truth semantics
        ↓
owned computed Bool/NULL
        ↓
filtered COUNT summary
```

The page guard, validated page, and record payload remain alive for the whole
callback. The HRTB prevents safe code from storing the row-borrowed Text after
the callback returns. Scratch vectors are allocated once per validated Heap
page and reused across its live slots; there is no per-live-row vector
allocation and no unsafe lifetime manipulation. Callback invocation still
waits until every persisted scalar in the row has passed validation, including
completely unrequested trailing values.

The original owned visitor remains and delegates this traversal, converting
only requested views to `ScalarValue`. `EvaluatedScalar::Borrowed` stores a
copied `ScalarRef`; Binary, Unary, and IsNull results remain owned. Existing
ScalarValue binary, comparison, and truth helpers delegate one ScalarRef
semantic core. Phase 7Q generalizes the existing bound evaluator around a
checked ScalarRef getter: the Join wrapper adapts owned rows, while the
filtered-count wrapper adapts the callback slice with `get(position).copied()`.
The hot bound evaluator receives no fields and cannot call
`find_source_position`. Binding remains identity-aware by
`RelationBindingId + ColumnId`, missing fields and short rows remain typed
errors. Phase 7Q originally evaluated both AND/OR operands; the Phase 69
Filter-only boundary below now adds conservative short-circuiting without
changing the general bound evaluator.

Phase 7T makes the row-aware borrowed storage visitor authoritative. The older
borrowed visitor is a thin wrapper that ignores mutation identity, and the
owned visitor delegates through the borrowed traversal. Exact direct
`Filter → SeqScan` may
consume the row-aware boundary before child row ownership; every other Filter
continues to receive fully owned child `ExecutionRows`:

```text
validated live Heap row + opaque StorageRowHandle
        ↓
exact direct sequential Filter
        ↓
dynamic find_source_position per Column leaf
        ↓
borrowed persisted ScalarRef or Expr literal
        ↓
borrowed Column/Literal leaves
        ↓
ScalarRef binary/truth semantics
        ↓
owned computed Bool/NULL
       / \
 TRUE     FALSE/UNKNOWN
  ↓             ↓
own every       own nothing
SeqScan value
  ↓
ExecutionRow + opaque StorageRowHandle
```

Eligibility requires exact SeqScan input, unique scan source identities, every
scan identity to match its binding and table, and every predicate identity to
be present in the scan fields. Literal predicates are eligible. IndexScan,
RangeIndexScan, Join, Sort, nested Filter, and malformed shapes retain generic
execution, including the empty-input behavior that does not evaluate a missing
predicate field.

The dynamic evaluator deliberately does not prebind positions and AND/OR still
evaluate both sides. The callback saves the first predicate error and stops
later predicate evaluation, but returns success so storage still validates all
later rows. A later storage error therefore has the same priority as completing
the old owned child scan; after a successful traversal the saved predicate
error is returned. QueryResult remains fully owned, and no page pin or borrowed
persisted row escapes into executor state.

Phase 7U adds one executor-local consumer for exact
`Project → Filter → SeqScan`. It reuses the Phase 7T visitor, dynamic predicate
lookup, three-valued semantics, and deferred predicate-error handling, but
precomputes the Project's source positions once before traversal:

```text
validated complete borrowed SeqScan row
        ↓
dynamic Filter predicate
       / \
 TRUE     FALSE/UNKNOWN
  ↓             ↓
own only        own nothing
Project values
  ↓
ExecutionRow + opaque StorageRowHandle
```

The specialization requires at least one predicate-used scan column that is
not retained by Project. Every scan column must be used either by the
predicate or by Project, and every Project source must resolve by
`RelationBindingId + ColumnId`. Duplicate and reordered Project sources retain
their exact output semantics, including independent owned Text duplicates;
zero-width output retains the qualifying row handle with no scalar ownership.
Unused scan columns, missing sources, duplicate/mismatched scan identities,
nested Filter, Sort, Join, index scans, and future shapes fall back to generic
execution.

The complete persisted row is still decoded and validated before predicate
evaluation. A retained Text value is necessarily owned at the fully owned
QueryResult boundary; when all predicate columns are also retained, the
specialization deliberately does not apply. UPDATE and DELETE continue to use
the Phase 7T direct Filter path and mutate only after `execute_rows` succeeds.
Assignment evaluation, index maintenance, transactions, INSERT, Join
algorithms, Phase 7N filtered-count precedence, planner, compiler, Rel IR,
PhysicalPlan, protocol, and inspection behavior are unchanged. Phase 7U itself
added no generic Filter prebinding, predicate pushdown, expression
bytecode/compiler, planner rewrite, dependency, or unsafe code.

Phase 63 completes generic Filter position prebinding without adding another
expression representation. Both borrowed streaming shapes build their complete
source-order fields before entering the storage visitor, then reuse the
executor-private expression binder:

```text
Expr + source-order OutputFields
        ↓
bind_expression once per Filter execution
        ↓
BoundExpr with checked source positions
        ↓
validated borrowed ScalarRef row
        ↓
evaluate_bound_scalar_ref_truth by direct position
        ↓
TruthValue
       / \
 TRUE     FALSE/UNKNOWN
  ↓             ↓
own retained    own nothing
output values
```

The valid per-row callback receives only `BoundExpr`, the borrowed scalar
slice, output positions, and result/error state; it has no `Expr` or
`OutputField` slice and therefore cannot repeat identity lookup. Qualified Text
still becomes owned only at the fully owned result boundary. Rejected Text
remains a storage-backed `ScalarRef` for the callback lifetime.

The authoritative materialized fallback uses the same binding boundary after
its child has executed:

```text
materialized child ExecutionRows
        ↓
bind_expression once against child fields
        ↓
evaluate_bound_truth for each owned row
        ↓
move TRUE rows; drop FALSE/UNKNOWN rows
```

Binding is an optimization, not an eager validation contract. If a hand-built
malformed predicate cannot bind, both streaming setup and legacy Filter retain
the dynamic evaluator's row-dependent behavior: an empty child remains empty,
while a nonempty child reports the same predicate error. Dynamic evaluators
also remain for UPDATE/assignment and other unbound expression boundaries.
All existing NULL and three-valued semantics are shared by the bound evaluator.
Batch Filter, filtered COUNT, and Join were already bound and were not reordered
or rewritten by Phase 63.

Phase 69 adds one executor-private qualification between successful Filter
binding and row evaluation:

```text
Filter Expr + source-order OutputFields
        ↓
bind positions once
        ↓
conservatively validate expression metadata
and source columns against attached storage schemas
        ↓
BoundFilterPredicate
       safe?
      /     \
    yes      no
     ↓        ↓
recursive   eager bound
left-to-    evaluation
right 3VL
```

The validator checks resolved source identity, column and output-field type and
nullability agreement, literal type/nullability, Bool operands and results for
AND/OR and NOT, non-null Bool results for IS NULL, and compatible comparison
operands with Bool result metadata. Before evaluation, every referenced source
column must also match the attached `TableStorage` schema by table and column
identity, semantic type, and nullability. This separate proof prevents a
hand-built plan and predicate from consistently misdescribing the same runtime
value. Any uncertainty makes the predicate ineligible; it does not make binding
fail. Consequently the three existing states remain distinct: binding failure
uses the dynamic row-dependent fallback, successful but unproven binding uses
the existing eager bound evaluator, and only fully qualified binding uses the
short-circuit evaluator.

The short-circuit evaluator is recursive through nested logical expressions,
NOT, and IS NULL. It skips only `FALSE AND right` and `TRUE OR right`.
`TRUE AND`, `FALSE OR`, `UNKNOWN AND`, and `UNKNOWN OR` evaluate the right
branch and combine it through the existing `TruthValue::and` or
`TruthValue::or` semantics. SQL three-valued results therefore do not change.

`BatchOperator::Filter`, direct and projected borrowed streaming Filter,
materialized legacy Filter, and filtered COUNT all reuse the same
`BoundFilterPredicate`. FALSE and UNKNOWN streaming rows still own nothing;
only TRUE outputs cross the existing owned result boundary. The general bound
scalar evaluator, dynamic evaluator, DML evaluation, NestedLoopJoin, and
HashJoin residual evaluation remain eager. Phase 69 adds no public API,
PhysicalPlan variant, planner/compiler rewrite, dependency, unsafe code,
protocol change, or persistent-format change.

Grouped, mixed-function, all-star-only, nested, join, sort, and index-backed
shapes retain the generic aggregate path.

`ScalarValue` equality and hashing are used only for current-process group
lookup. NULL equals NULL for grouping, so all NULLs at the same key position
share a group; this is deliberately different from SQL expression equality,
where `NULL = NULL` remains UNKNOWN. Scalar hashing is not a persistent format,
schema fingerprint, WAL, page, or compatibility contract. Although first-seen
order is deterministic for the current executor, SQL queries without ORDER BY
do not guarantee row order.

Aggregate type and NULL rules are:

- `COUNT(*)` counts every row; `COUNT(column)` ignores NULL. Both return a
  non-null physical `UInt64`.
- `SUM` accepts only `Int64` and `UInt64`, ignores NULL, and is nullable because
  empty/all-NULL input returns NULL. Its result is an unnamed physical numeric
  type even when the input is nominal, because a sum is not one input identity.
- `MIN` and `MAX` accept Bool, Int64, UInt64, and Text using the existing value
  comparison, ignore NULL, and are nullable for empty/all-NULL input. They
  preserve the input `SemanticType` because the result is an input value.

There is no HAVING, DISTINCT aggregate, alias, GROUP BY expression, nested
aggregate, aggregate-aware ordering, GROUPING SETS, ROLLUP, or CUBE.

## Typed expressions and NULL

Database NULL is an explicit `ScalarValue::Null`; it is not represented by
Rust `Option<ScalarValue>`. `Option` continues to mean that syntax or metadata,
such as a `WHERE` clause, is absent. Parser NULL literals begin untyped. HIR
assigns them a semantic type from the surrounding boolean or comparison
context without granting NULL an arbitrary nominal identity.

HIR and relational expressions carry an expression type consisting of:

```text
SemanticType + nullable
```

Column nullability originates in Canonical Schema IR. Literal values other than
NULL are non-nullable. A comparison is nullable when either operand is
nullable, boolean `AND`/`OR`/`NOT` preserve possible UNKNOWN results, and
`IS NULL`/`IS NOT NULL` always produce a non-null BOOL. This expression
nullability is distinct from schema nullability: a nullable column may contain
NULL, while an expression over that column may or may not return NULL.

`IS NULL` and `IS NOT NULL` remain explicit AST, HIR, and relational nodes;
they are not lowered to equality with NULL. Ordinary `=`, `!=`, `<`, `<=`,
`>`, and `>=` comparisons return UNKNOWN if either operand is NULL, including
`NULL = NULL`. Nominal compatibility checks still apply to non-NULL operands,
so contextual NULL typing cannot make `UserId = TeamId` legal.

The executor centralizes boolean conversion as three truth values:

```text
Bool(true)  → TRUE
Bool(false) → FALSE
NULL        → UNKNOWN
```

`AND`, `OR`, and `NOT` use the SQL three-valued truth tables. A filter keeps a
row only when its predicate is TRUE; FALSE and UNKNOWN both reject the row.
Storage's existing scalar tag for NULL round-trips through heap pages, buffers,
the database file, and reopen. `HeapStorage` validates every embedded write and
returns `StorageError::NullNotAllowed` when NULL targets a non-nullable column,
independently of query compilation.

## Typed DML

The parser's top level is a typed `Statement` enum with distinct Select,
Insert, Update, and Delete variants. The HIR resolves every table and column to
stable IDs, assigns expression types, rejects duplicate targets and invalid
NULL/nominal assignments, and fills omitted nullable INSERT columns with NULL.
Logical and physical statement enums preserve that distinction. UPDATE and
DELETE select targets through the existing sequential Scan + optional Filter
tree rather than embedding a second predicate implementation.

Execution scan tuples carry a hidden, opaque `StorageRowHandle` alongside
values. Projection can discard SQL-visible columns without manufacturing a
`_rowid` feature, and executor cannot inspect Heap PageId/SlotId details. DML
collects all selected targets before mutation, avoiding scan interference when
a page is compacted. UPDATE evaluates every assignment against the original
row and constructs one complete replacement, so `SET a = b, b = a` swaps the
values. The shared three-valued evaluator modifies only TRUE rows; FALSE and
UNKNOWN are skipped.

`ExecutionResult` distinguishes query rows from `AffectedRows(u64)`. INSERT
returns one; UPDATE counts selected rows, including same-value assignments;
DELETE counts versions logically expired. `Database::execute` wraps one DML
statement in an implicit transaction. `Database::execute_in` uses an explicit
transaction and permits reads of the transaction's currently buffered writes.
Because savepoints do not exist, an execution-time mutating-statement failure
rolls back the whole explicit transaction.

Heap mutation remains below SQL semantics. `insert_in`, `update_in`, and
`delete_in` validate the transaction and full row, build a candidate page,
append the existing full-page before/after-image `PageUpdate`, assign pageLSN,
and only then install the dirty page. Runtime rollback and startup recovery
therefore need no DML-specific undo or WAL record type. A mid-statement error
causes the owning transaction to restore all preceding page images.

## Physical storage identity and database transactions

Logical partition and physical identity are separate:

```text
Schema TableId (logical SQL relation)
    ↓ TablePlacement
Single ───────────────────────────────→ StorageId
RangePartitioned
    ↓
PartitionId (stable logical partition) → StorageId
    ↓ deterministic StorageRegistry
TableStorage
```

`TableId` remains SQL/catalog identity and authorization continues to use it.
`PartitionId` identifies a durable logical physical partition and is not a
range-vector index or path. `StorageId` identifies one physical storage instance. Heap metadata v5 stores
it as a nonzero little-endian value, so close/reopen, process restart, pathname
changes, and catalog reorder preserve identity. It is never a Vec index,
pointer, file descriptor, or pathname. `Single` maps one table to one storage;
`RangePartitioned` resolves one logical table through ordered PartitionIds to
several StorageIds without redefining SQL identity.

PartitionCatalog v1 is an immutable, explicit database-level file with its own
path, magic/version, little-endian fields, bounded counts, schema fingerprints,
and CRC32C. It stores the partition key and ordered `[lower, upper)` Int64 or
UInt64 bounds. Bounds may be unbounded and gaps are legal; overlap, empty
ranges, nullable keys, wrong types, duplicate identities, missing storages,
and schema mismatch are hard typed errors. Paths are open-time materialization
inputs only: reopen matches heap-persisted StorageIds, so reorder or move does
not change partition identity.

Planning consumes a pure range snapshot and never reads the catalog file.
Nested AND comparisons (`=`, `<`, `<=`, `>`, `>=`, including reversed
operands) produce an exact integer interval. OR/NOT conservatively select every
partition. A contradiction produces an empty `PartitionedScan` with the normal
output schema. Each selected partition then chooses its own local SeqScan,
IndexScan, or RangeIndexScan; the complete residual Filter remains above the
scan. The authoritative materialized executor concatenates partitions in
canonical range order and retains required-column propagation. When every
selected access is a SeqScan, the executor may instead visit those same
storages in that same order through the bounded batch source described below.
Global indexes do not exist.

INSERT evaluates and validates its typed row before routing. UPDATE and DELETE
materialize every original target first; UPDATE also evaluates every
replacement and resolves every destination before the first mutation. A
cross-partition UPDATE is one source delete plus one destination insert in the
same `DatabaseTransaction`, consuming the old physical handle and creating a
new one. Existing Prepare/CommitDecision recovery therefore makes
multi-partition INSERT, DELETE, and row movement all-or-nothing across crashes.
`StorageRowHandle` validates its opaque owning StorageId so partitions of the
same TableId cannot exchange physical locators.

`DatabaseTransaction` owns database-level identity, isolation intent,
lifecycle, and a deterministic participant set. Its `DatabaseTxnId` is
distinct from each Heap WAL `TxnId`; retained coordinator decisions, the Phase
3B.5 checkpoint high-water, and prepared WAL records provide the restart floor
so a historical identity is never reused.
Participants are registered on first access and contain:

```text
StorageId + Read/Write mode + StorageTransaction
```

Read participants do not acquire the Heap writer lease. A participant may
upgrade Read → Write. Legacy create/open APIs permit multiple readers and one
writer. Explicit coordinator-enabled APIs accept a separate coordinator-log
path and permit several local write participants. One writer retains the direct
physical commit fast path; two or more use:

```text
Prepare every participant (durable)
    ↓
CommitDecision(DatabaseTxnId, sorted StorageId + physical TxnId set) + sync
    ↓                         GLOBAL COMMIT POINT
Commit every prepared participant (durable)
    ↓
Complete(DatabaseTxnId) + sync
```

Before the global point, rollback durably aborts and undoes every participant.
After it—or after a decision sync with an uncertain result—rollback is a typed
error and only commit retry is legal. Append and sync retries reuse the same
DatabaseTxnId and canonical participant set. Read-only and single-writer
transactions never write the coordinator log.

Every explicit query builds one `DatabaseReadView` owned by the database
transaction and containing the `StorageReadView` adapters for all StorageIds
used by that statement. Executor receives explicit Single TableId→StorageId
bindings plus StorageId-explicit partition scan scopes,
StorageId-tagged storages, and StorageId-tagged views; it never correlates
parallel vectors by position. Current Heap status stores have independent
CommitSeq domains, so `DatabaseReadView` owns logical transaction/isolation
context without inventing a false database-global timestamp.

Coordinator-enabled startup scans and validates the independent log before
opening any participant for recovery. Exact prepared mappings with a
CommitDecision commit; prepared mappings without one abort under presumed
abort. Missing decision participants, extra/mismatched prepared participants,
and corrupt coordinator bytes fail the entire database open. A standalone Heap
open never guesses an in-doubt outcome and returns a typed resolution-required
error. Complete is appended only after every participant commit is durable.
Checkpoint and close retain their quiescent rule, so prepared/in-doubt state is
rejected rather than recycling required WAL.

Phase 3B.5 adds explicit, quiescent checkpoint compaction for completed
data-only global coordinator history. Normal commit/open/close remain
append-only and never compact automatically; retained structural decisions are
still an explicit compaction blocker.
HASH/LIST/DEFAULT partitioning, partition DDL/split/merge, global indexes,
remote placement, Columnar, Raft, replication, and distributed transactions
are not implemented.

## Storage boundary

The synchronous storage path is now:

```text
Executor
    ↓
DatabaseTransaction / DatabaseReadView
    ↓
PhysicalBindings: TableId → TablePlacement
                         ├─ Single → StorageId
                         └─ Range → PartitionId → StorageId
    ↓
StorageRegistry
    ↓
TableStorage capability API
    ├─ TableStorage::Heap(HeapStorage)
    │      ├─ registered BTree access methods
    │      └─ TransactionManager + Heap WAL
    │             ↓
    │         BufferPool + PageGuards → PageManager → database file
    └─ TableStorage::Lsm(LsmStorage)
           ├─ ordered MemTable + transaction-local overlay
           ├─ LSM WAL v1
           └─ Manifest v2 selects Bloom-bearing immutable SSTables
                    ├─ L0 overlapping
                    ├─ L1 non-overlapping
                    ├─ L2 non-overlapping
                    └─ L3 non-overlapping
```

`netbadb-storage` keeps the boundaries concrete and small:

```text
                 DatabaseTransaction
                        │
                 CoordinatorLog
                  /           \
             Heap Txn        LSM Txn
                │              │
            Heap WAL        LSM WAL
```

- `TableStorage` is the database composition boundary and has two real
  variants, `Heap` and `Lsm`. It uses static enum dispatch; there is no empty
  Columnar placeholder and no giant `dyn StorageEngine` interface. B+Tree is
  an access method owned by Heap, while the LSM clustering order is its native
  point/range access path and is not represented by a fake BTree handle.
- `StorageRowHandle` is an opaque, storage-scoped executor mutation identity.
  Heap carries its generation-safe RowId. LSM carries a stable `LsmRowId`, the
  observed visible version, and current clustering key for stale-handle and
  key-moving UPDATE validation. Executor, planner, Rel IR, and SQL inspect
  neither representation. `StorageReadView` and `StorageTransaction` likewise
  keep each engine's MVCC and durability state below the boundary.
- `Database::storage_kind` and `Database::inspect_lsm_storage` expose embedded,
  read-only physical inspection including clustering identity, MemTable/SSTable
  entries, per-level counts/bytes, Bloom bytes, amplification counters, and
  last-`ANALYZE` row/min/max statistics. Deployment
  manifest v4 still bootstraps Heap only, so the offline CLI and Inspection JSON
  v4 remain unchanged; LSM CLI/server bootstrap is deliberately deferred.

LSM maintenance is synchronous and quiescent. `compact()` deterministically
drives L0-count and L1/L2-size triggers, selects complete source/target overlap
closure, preserves every MVCC version and tombstone, and atomically publishes
all split outputs. `compact_full()` covers every level and is the only path
that removes superseded history or tombstones. Per-SSTable Bloom filters index
all represented clustering keys, including tombstones; point reads use range
routing then Bloom, while range reads use range/block metadata only. Both feed
a bounded block-at-a-time k-way merge.
- The capability API covers projected scans, point/range access, borrowed row
  visitors, an owned row consumer with typed `ControlFlow` cancellation,
  presence summaries, and mutation. Heap dispatch delegates to its
  validated-once row traversal. LSM merges MemTable, SSTable, and bounded
  transaction-overlay state in physical-key order and decodes one visible row
  at a time. Neither engine constructs executor batches or knows Filter,
  Project, Limit, SQL expressions, or PhysicalPlan.

Above that storage boundary, one executor-private producer recognizes physical
trees made from either a single SeqScan or an all-SeqScan PartitionedScan plus
Filter, Project, and Limit:

```text
BatchSource
├── SeqScan ───────────────────────────────┐
└── PartitionedSeqScan                     │
      P0 visitor ─┐                        │
      P1 visitor ─┼─ planner order ────────┤
      PN visitor ─┘                        │
                                           ↓
                         one shared ExecutionBatch (<= 256 rows)
                                           ↓
                              Filter / Project / Limit
                                           ↓
                 collector / Aggregate / Top-N / HashJoin probe (SeqScan only)
```

Partition visitors reuse the statement's existing StorageReadView for each
StorageId, validate every storage's logical TableId, and retain a partial batch
across partition boundaries. They do not create a batch per partition. A full
batch can therefore contain one partition's tail and rows from later
partitions. Downstream `Break` stops both the current storage visitor and the
outer partition loop, so Limit skips every unvisited partition. Aggregate and
Top-N return `Continue` and necessarily visit them all.

The producer groups owned rows into a private 256-row `ExecutionBatch`,
evaluates a position-bound `BoundExpr`, applies the existing move-aware
`ProjectionPlan`, and tracks Limit state across batches. A callback consumes
each bounded batch: the normal query
path appends it to the fully owned result, while Aggregate updates incremental
state and releases it. Bounded Top-N is a third consumer for the existing
`Limit -> Project -> Sort` shape: it drains complete owned rows into a
worst-first heap of at most K candidates and never cancels its child, because
later rows may rank earlier. Eligible direct-scan HashJoin is a fourth consumer:
it materializes and hashes the strictly smaller ANALYZE-estimated side, or the
right side on ties or missing statistics, then borrows the other side from each
batch for key lookup and complete residual evaluation. BuildRight emits owned
output directly; BuildLeft buffers owned matches by logical left row and
move-flattens them after the producer clears the last batch. Ordinary batch Limit
cancellation stops the
storage consumer after the current bounded batch; later physical rows are
deliberately not requested.
This is an execution behavior and not a whole-file integrity check: every row
actually requested still receives the engine's complete MVCC, page/SSTable,
codec, type, NULL, and UTF-8 validation.

Partitioned batch eligibility is deliberately all-or-nothing. If any selected
partition uses IndexScan or RangeIndexScan, the complete PartitionedScan stays
on the materialized path; there is no mixture of streamed and owned partition
sources. `point_lookup_columns_with_view` and
`range_lookup_columns_with_view` still return owned vectors, so slicing those
vectors in the executor would not remove their `O(N)` storage materialization
and is not presented as streaming.

The public `QueryResult` remains fully owned and may contain the complete final
result. Intermediate SeqScan, Filter, and Project results no longer require a
full base-scan vector. Aggregate over an eligible SeqScan/Filter/Project child
binds group-key and aggregate-input positions once, then updates COUNT, checked
SUM, MIN, and MAX state across batches. Global aggregation retains only one
state set plus the current batch; grouped aggregation additionally retains one
key and state set per distinct group in first-seen order. Aggregate is still a
blocking boundary: it consumes its complete child before emitting rows, so a
Limit above it truncates finalized groups and never stops aggregate input.

For batches containing MIN or MAX, Aggregate drains owned rows while retaining
the batch allocation. Each candidate is first borrowed for the existing typed
comparison. Only states that actually replace on that row request ownership;
one replacement receives the original `ScalarValue`, while additional
replacements receive the necessary clones. COUNT and SUM continue to inspect
borrowed values and use no ScalarValue ownership. Finalization combines owned
group keys and finalized states, then applies the same last-use projection used
by row Project: unique outputs move, while duplicate outputs clone before their
last use and move the final owner. The group lookup remains
`HashMap<Vec<ScalarValue>, usize>` and its per-row key ownership is explicitly
outside Phase 57.

Phase 67 evaluated, then removed, a narrower typed-column sidecar for eligible
global primitive aggregates. The experiment transposed unique Bool, Int64, and
UInt64 source positions from each row batch into typed vectors plus explicit
validity bits and ran direct COUNT/SUM/MIN/MAX loops. Its mixed benchmark
evidence did not justify retaining a second transient representation. The
production invariant therefore remains one row-owned `ExecutionBatch` feeding
the existing typed `AggregateAccumulator`; there is no primitive column batch,
columnar storage contract, or generalized column execution API.

Phase 58 replaces that owned-key lookup with an executor-private `GroupLookup`:

```text
borrowed row group-key values
            ↓
RandomState hash in source order, including key width
            ↓
HashMap<u64, bucket head> → collision index chain
            ↓
exact comparison against GroupState.key_values
      ┌─────┴─────┐
     hit         miss
      ↓            ↓
no key owner   materialize one durable Vec<ScalarValue>
                   ↓
               append GroupState
```

The hash is only candidate metadata: a hash collision never implies SQL group
equality. Exact `ScalarValue` equality covers Bool, Int64, UInt64, Text, NULL,
and ordered multi-column keys, so NULL keys group together without adopting
predicate `UNKNOWN` semantics. `GroupState` is the sole durable key owner;
lookup metadata stores only hashes and group indices. The `Vec<GroupState>`
continues to define first-seen result order, and HashMap iteration never shapes
query output. Existing-group hits allocate no temporary key and clone no key
values. A miss conservatively clones each source key value once into its single
durable group key; move-on-miss remains separate from MIN/MAX ownership.

Phase 59 gives every grouped batch to Aggregate as owned rows while preserving
the Phase 58 borrowed probe:

```text
owned row → borrowed GroupLookup probe
                  ├─ hit → existing GroupState; no key transfer
                  └─ miss
                       ↓
              validate key and update borrowed COUNT/SUM
                       ↓
              decide actual MIN/MAX replacements
                       ↓
              durable owners by source position
              (group-key slots + replacements)
                       ↓
                 clone N - 1 + move once
                       ↓
              append GroupState and register probe hash
```

The per-position plan supports repeated group-key slots and overlap such as
`GROUP BY payload` with one or more `MAX(payload)` outputs. COUNT/SUM consume
borrowed values and MIN/MAX decide replacement before any source value moves.
For a unique key with no other durable owner, the original `ScalarValue` and
its String allocation move directly into `GroupState`; additional real owners
alone cause clones. The materialized legacy Aggregate retains its borrowed-row
fallback and clones a miss key because it does not own the input row. Hashing,
collision chains, exact equality, NULL grouping, first-seen order, and the
precomputed Phase 58 probe hash are unchanged.

Phase 60 removes the redundant second randomized hash of that precomputed
group hash without changing the group-key or ownership paths:

```text
SQL group-key values
        ↓
RandomState keyed hash (key width + ordered ScalarValues)
        ↓
opaque u64 prehash
        ↓
pass-through executor-private bucket map
        ↓
group-index collision chain
        ↓
exact GroupState.key_values equality
```

The pass-through bucket hasher is not the user-data hash. User-controlled group
values are still hashed by `RandomState` first; the internal map receives only
the resulting keyed, randomized `u64`. A private key wrapper explicitly invokes
`Hasher::write_u64`, and its private hasher returns that value unchanged for
bucket indexing. Unsupported generic byte hashing is an internal programming
error rather than a second generic hash implementation. Equal prehashes remain
candidate metadata only: the collision chain and authoritative exact
`ScalarValue` comparison still distinguish SQL groups. `Vec<GroupState>` still
defines first-seen order, including NULL and multi-key groups, and the Phase 59
probe-to-register path reuses its already computed prehash on a miss.

Phase 61 keeps `ExecutionBatch` as the owner of grouped rows and consumes each
row through a mutable borrow in place:

```text
ExecutionBatch owns rows
        ↓
grouped consumer borrows &mut ExecutionRow
        ↓
borrowed GroupLookup probe
        ↓
borrowed COUNT/SUM transitions and MIN/MAX comparison
        ↓
actual durable owner required?
      ┌────────────┴────────────┐
     no                        yes
      ↓                         ↓
leave row intact       move selected ScalarValue slots
      └────────────┬────────────┘
                   ↓
          clear complete batch
                   ↓
             retain Vec capacity
```

Whole `ExecutionRow` ownership is no longer required for ordinary grouped hits.
A hit without extrema never enters replacement-owner planning, and an extrema
hit enters scalar transfer only when the borrowed comparison selects an actual
replacement. A miss still combines Phase 59 group-key targets with extrema
targets and performs clone `N - 1` plus one move per source slot. Errors stop
processing but the outer grouped consumer clears every remaining row before
returning while preserving the batch allocation. Global COUNT/SUM remains on
its borrowed path, and global MIN/MAX still drains owned rows through the Phase
57 path. Phase 60 randomized prehashing, exact collision chains, NULL grouping,
and first-seen `Vec<GroupState>` order are unchanged.

Phase 62 binds each MIN/MAX state to its resolved physical type once when the
Aggregate accumulator is created:

```text
typed Aggregate metadata
        ↓
ExtremeState::{Bool, Int64, UInt64, Text}(None)
        ↓
borrow candidate of the same physical type
        ↓
direct bool/integer comparison or borrowed str::cmp
        ↓
actual replacement?
        ↓
existing Phase 57/59 selected-value ownership transfer
```

The `Option<T>` inside an extrema state means that no non-NULL candidate has
been observed; SQL NULL remains the explicit `ScalarValue::Null` result at
finalization. Runtime values that disagree with the bound physical type return
`ExecutionError::TypeMismatch`. Text comparison borrows both the candidate and
current `String` as `&str`, then uses the same standard-library lexical ordering
as the generic scalar comparator. No locale collation, cached sort key, custom
string algorithm, or new allocation is introduced. Filter, Join, Sort, and
ordinary expression comparison continue through the authoritative generic
`compare_values` / `compare_scalar_refs` path. Phase 61 grouped lookup and
borrowed-first ownership decisions are unchanged.

Exact standalone Filter and predicate-only Project/Filter shapes retain the
measured borrowed Phase 7 streaming specializations for every scalar type,
avoiding owned values for rejected rows; Filter pipelines with Limit use the
bounded batch runtime so they can stop upstream. Direct and filtered COUNT
specializations remain ahead of generic streaming Aggregate dispatch. Sort,
joins, index/range scans, partition scans, DML, and ineligible Aggregate
children deterministically use the authoritative legacy executor for the
complete tree. PhysicalPlan and Inspection JSON are unchanged, and executor
dispatch contains no Heap/LSM branch.

- `PageManager` owns fixed-size file I/O, page allocation, checked page-offset
  arithmetic, and file sync. It does not interpret heap or index semantics.
- `BufferPool` owns a bounded set of raw page frames. It uses a simple
  round-robin eviction boundary, pins pages while guards are alive, refuses to
  evict pinned pages, and exposes explicit `flush_page`/`flush_all`
  operations. Before writing a dirty data page it makes the WAL durable
  through that page's pageLSN. The data-page write is not attempted if the WAL
  flush fails.
- `Page` validates a versioned page header, PageId-bound checksum, and explicit
  page type before exposing slotted-page operations. Heap pages use a slot
  directory at the front, free space in the middle, and tuple bytes packed
  from the end of the page backward.
- `TransactionManager` allocates strong `TxnId` values, appends `Begin`, and
  owns the per-open-database writer/health state and durable status store. A transaction tracks
  `Active`, `RollbackRequired`, `CommitPending`, `RollbackPending`, `Committed`,
  or `RolledBack` and owns its last LSN. Writer ownership is acquired lazily
  before the first heap mutation; read-only transactions do not reserve it.
  Read Committed statements capture fresh ReadViews, while Repeatable Read pins
  its first ReadView until transaction completion.
- `HeapStorage` validates and encodes rows, constructs a candidate after-image,
  appends its `PageUpdate`, and only then publishes the page to the buffer
  frame. It no longer flushes the entire buffer after each insert. Page guards
  do not escape these operations, so executor and query APIs carry no page
  lifetimes.
- `netbadb-index` is the storage-independent B+Tree domain layer. It owns
  `IndexSpec`, explicit key/RowId ordering, nodes, versioned codecs, and
  byte-balanced split calculation; it has no dependency on storage, WAL, SQL,
  planner, or executor. `netbadb-storage::BTree` owns page traversal,
  allocation, transaction/WAL ordering, publication, and recovery integration.

Heap sequential scan uses one immutable validation proof per buffered page:

```text
Disk / Buffer Page
        ↓
one authoritative Page::header full validation
        ↓
ValidatedPage<'_> borrowing the immutable Page
        ↓
checked slot lookup and borrowed live-record slices
        ↓
unchanged row codec and owned ScalarValue row
```

`ValidatedPage` is crate-private and can only be created through the existing
full `Page::header` validation. Its lifetime prevents mutable access to the
borrowed `Page` while the stored `PageHeader` and structural proof are reused.
It is not a persistent trust bit, a validation cache in `Page`, or an on-disk
marker. Live-record slicing still uses checked ranges; invalid slots return a
typed error, and checksum, generation, record-bound, free-space, and overlap
corruption fails before traversal begins. Public Page operations retain their
existing validation semantics. Non-Heap pages encountered in a table file
still pass their existing single-payload validation before Heap scan skips
them.

The experimental container retains the legacy `NBPG` file-root marker. Heap
metadata has its own `NBD1` marker and version 4 little-endian layout inside
the header page:

```text
16..20  NBD1 heap metadata magic
20..22  u16 heap metadata version (4)
22..24  reserved bytes (zero)
24..32  u64 table ID
32..34  u16 declared column count
34..66  SHA-256 canonical table-schema fingerprint
66..74  u64 IndexCatalog root PageId
74..80  reserved bytes (zero)
```

Create validates the complete table before creating the WAL or heap file. Open
validates metadata and schema identity before recovery can mutate storage, then
checks it again after recovery. A table-ID mismatch and a schema-fingerprint
mismatch are distinct typed storage errors. Heap metadata versions 1 through 3 are
rejected without migration; the file format remains experimental and may
change again between versions. New files reserve page 1 for the empty catalog
root and page 2 for the initial Heap page.

Page 0 is legacy container/heap metadata and is not interpreted as a Page v5
data page. Data pages use the following version 5 little-endian layout:

```text
0..4    NBP1 page magic
4..6    u16 page format version (5)
6       u8 page type (2 heap, 3 BTreeMeta, 4 BTreeInternal, 5 BTreeLeaf,
        6 IndexCatalog; tag 1 remains reserved)
7       reserved byte (zero)
8..10   u16 slot count
10..12  u16 free-space lower bound
12..14  u16 free-space upper bound
14..16  reserved bytes (zero)
16..24  u64 pageLSN (zero means no WAL record)
24..28  u32 CRC32C (little-endian)
28..    8-byte slot entries: u16 offset + u16 length + u32 generation
...     free space
...     tuple bytes, allocated from PAGE_SIZE backward
```

The Page v5 checksum is `CRC32C(page_id_le_u64 || complete_page_image)`, with
bytes 24..28 of the page image treated as zero. It covers all 4096 bytes,
including the header, pageLSN, slot directory, free/unused bytes, tombstones,
and tuple payload. Binding the expected logical PageId also detects a valid
page block read from the wrong physical page position. Magic and version are
checked before CRC32C so an old page reports its explicit unsupported version;
checksum verification then precedes all remaining semantic validation. The
all-zero new-page before-image remains a WAL sentinel, not a valid persisted
data page.

Every allocated slot has a nonzero little-endian `u32` generation. A live slot
stores its checked offset and length; zero-length records remain legal and use
their real offset. The reserved pair `(offset = 0, length = 65535)` means
Deleted and retains the generation. Either reserved component without the
complete pair, or generation zero, is typed corruption. Normal DELETE and
UPDATE do not create Page tombstones: they update MVCC tuple headers and UPDATE
inserts a new physical version. Manual vacuum rebuilds affected pages and marks
only horizon-dead versions Deleted. INSERT deterministically reuses the lowest deleted SlotId whose generation
is below `u32::MAX`, increments it with checked arithmetic, and otherwise
appends a generation-1 slot. A generation-maximum tombstone is permanently
ineligible, so generation can never wrap.

Heap insertion performs a deterministic linear first-fit search from the
lowest data PageId. Each candidate is cloned and passed to
`Page::insert_record`; only `PageFull` advances the search, while corruption or
other errors fail immediately. If no existing page accepts the tuple, the
existing WAL-before-file-extension protocol allocates a new page. This is
intentionally O(number of heap pages); there is no persistent free-space map.

UPDATE inserts its replacement through normal deterministic first-fit, then
expires the predecessor with `xmax/cmax` and a next-version RowId. Both page
images share the caller transaction and full-page WAL chain. The replacement
always has a distinct RowId, even if both versions occupy one page.

`RowId` is the versioned physical locator `PageId + SlotId + generation`, not a
business key, primary key, or globally monotonic identifier. Before vacuum, an
old-version locator still names a checked physical tuple whose visibility is
decided by its ReadView. Vacuum turns dead versions into tombstones; after slot
reuse, the old generation reports `StaleRowId` before live/deleted state is
considered. Scans return the persisted generation, so executor UPDATE/DELETE
retain the complete candidate locator.

Version 3 intentionally changed the meaning of a formerly invalid slot pair;
version 4 added data-page integrity without moving existing header fields;
version 5 adds explicit slot generation. It replaces the pre-Foundation
sequential `HEAP` layout and page versions 1 through 4. These experimental
formats have no migration path and are rejected rather than reinterpreted.

Every live Heap slot payload starts with this fixed 48-byte little-endian MVCC
header before the existing typed row encoding:

```text
0..4    NBMV tuple magic
4..6    u16 tuple format version (1)
6..8    u16 presence flags for xmax, cmax, and next version
8..16   u64 xmin TxnId (non-zero)
16..24  u64 xmax TxnId (zero only when absent)
24..28  u32 cmin CommandId (non-zero)
28..32  u32 cmax CommandId (zero only when absent)
32..40  u64 next-version PageId
40..44  u32 next-version SlotId (checked to u16)
44..48  u32 next-version generation
48..    typed row payload
```

Presence bits and zero fields must agree; `xmax` and `cmax` are either both
present or both absent, and an optional version pointer has no zero component.
Malformed magic, flags, widths, reserved absence encodings, transaction IDs,
command IDs, or pointers are typed storage errors. Heap metadata v4 is the
compatibility boundary requiring this tuple format; legacy unversioned row
payloads are not guessed or migrated.

## Persistent B+Tree boundary

One table database file may interleave Heap and B+Tree pages. Heap scans and
first-fit allocation validate every page, process only Heap pages, and skip
valid index pages. A Heap scan performs that complete page-wide validation once
and reuses it only for the lifetime of the page's immutable borrow. RowId
read/update/delete requires a Heap page, so a locator
for an index page is rejected. B+Tree allocation uses the same PageManager,
buffer pool, transaction manager, WAL generation, recovery, and checkpoint as
heap mutation; there is no second file or durability domain.

Every index page is a normal checksummed Page v5 with exactly one live slot 0,
generation 1. Payload v1 remains supported for raw/legacy trees. Newly
registered trees use v3; the node bodies retain the same typed semantics:

- `NBTM` metadata: stable `BTreeHandle` PageRef, current root PageRef, height,
  physical plus optional nominal semantic type, and nullability;
- `NBTL` leaf: sorted full `(ScalarValue, RowId)` entries and optional next-leaf
  PageRef;
- `NBTI` internal: first child plus sorted persistent lower-bound fence keys
  and right children. A fence's RowId is only an ordering token: it need not
  identify a currently live heap row or leaf entry. Deleting the first live
  entry in a right subtree therefore does not rewrite or enlarge its fence.

The common v1 header is magic[4], version u16=1, reserved u16=0. V2 uses
magic[4], version u16=2, reserved u16=0, nonzero owner IndexId u64 at bytes
8..16, followed by the unchanged v1 node body. Owner IDs are unique among
committed registered trees in one physical Heap file. Meta, internal and leaf
reads require exactly the handle's owner; None accepts only v1. Split allocation
passes owner explicitly and reserves the exact identity/pointer widths before byte-size calculations.
V3 adds nonzero PageGeneration u64 at bytes 16..24 and expands every structural
pointer to PageId u64 + PageGeneration u64. Optional leaf-next encodes absence
as two zero fields; any present pointer with zero generation is corruption.
Every meta/internal/leaf page self-identifies its allocation. Buffer lookup
validates the exact expected generation and traversal validates owner; neither
is a wildcard. Split reserves/syncs generations before emitting references.
Merge/root collapse retains full references and orphans retain owner/generation.
Page v5 and its CRC are unchanged. Raw tree APIs allocate v1 and cannot choose
a registered owner. Existing v1/v2 trees remain read/write without silent upgrade.
The v3 maximum Text key is 3981 bytes, including room for a future separator;
maximum-sized keys are tested through multi-level splits and reopen.

All integers are fixed-width little-endian. Decoders reject wrong magic or
version, nonzero reserved fields, invalid UTF-8/type/value tags, zero child
pages, invalid RowIds, impossible counts, truncation, trailing bytes, and
non-increasing entries. Traversal is bounded by persisted height and validates
each PageId against current page count and each expected node kind. The page
CRC catches raw corruption first; independently tested node decoders catch
semantic corruption after a valid CRC is recomputed.

Key order is NULL first, then native Bool/Int64/UInt64/UTF-8 Text value order.
The tie-break is explicitly PageId, SlotId, generation; `RowId` itself does not
gain a persistent `Ord` contract. Duplicate values are legal and point lookup
returns all matching RowIds in tie-break order. An exact `(key, RowId)` repeat
is `DuplicateEntry`. `IndexSpec` validates runtime physical type, nullability,
and persists optional nominal identity, although nominal identity does not
change physical comparison.

Insertion descends without retaining guards, allowing a buffer pool capacity
of one. Overflow splits deterministically by encoded byte size, updates leaf
links, propagates complete separator entries, splits internal nodes, and uses a
new root plus metadata update when height grows. One compound mutation logs
full-page images in deterministic new-right/existing-left/ancestor/root/meta
order. If it creates pages, all corresponding WAL records become durable before
the first file extension. Any failure after the first PageUpdate makes the
transaction `RollbackRequired`; runtime rollback and startup loser undo restore
existing pages and remove trailing new pages in reverse order.

Exact `(key, RowId)` deletion uses a soft half-capacity encoded-byte threshold
to attempt deterministic right-first merges, falling back to the left sibling
for the last child. Leaf entries are not redistributed. A merge occurs only when
the actual encoded leaf or internal payload fits; otherwise a sparse node
remains valid. Parent separator removal recurses upward, and a zero-separator
root collapses. The surviving physical page is always the left page, repairing
the forward leaf chain without a predecessor lookup. All final page images are
preflighted before WAL publication and logged bottom-up with metadata last.
Delete never allocates or shrinks the file. Removed right pages and old roots
remain valid but unreachable orphan index pages; reclamation is deferred.

Round 9 handles a unary internal path before leaf deletion: it merges that
internal node with its sibling if the combined payload fits, otherwise rotates
one child through the parent fence. This prevents an empty leaf under a parent
with no sibling separator. Normalization is bounded by file page count, uses
the same owner, and logs full-page changes in the caller's transaction; errors
after changes require rollback. Reopen/crash tests cover undo and committed
collapse. Active-tree orphan reclamation remains deferred; after retirement,
Round 11 may reclaim the complete tree when all its pages occupy the tail.

## Persistent index registry

Heap metadata points to a fixed `IndexCatalog` root. Catalog pages are
checksummed Page v5 single-payload pages containing version-9 `NBIC` payloads.
Versions 2 (unnamed), 3 (optional name) and 4 (explicit logical identity and
retirement) and 5 (durable high-water) and 6 (owned pending roots) and 7 (generation-bearing handles) and 8 (tail intent) remain readable; v1 is rejected. Registration order is preserved.
The authoritative allocator high-water lives only in the root, never in Heap
metadata or a cache reconstructed from surviving active entries.

```text
v9 header (48 bytes, little endian)
0..4    NBIC magic
4..6    u16 version (9; decoder also accepts 2, 3, 4, 5, 6, 7, 8)
6       u8 table-statistics presence (0 or 1)
7       u8 root state (0 continuation, 1 root without intent, 2 root with intent)
8..16   u64 next catalog PageId (0 means none)
16..20  u32 entry count
20..24  u32 pending ownership count (zero in v2-v5)
24..32  u64 row_count (zero when absent)
32..40  u64 managed_page_count (zero when absent)
40..48  u64 next_index_id (nonzero on root, zero on continuations)

v7/v8/v9 entry prefix (56 bytes, little endian; v4/v5/v6 use 48 bytes)
0..4    u32 ColumnId
4       u8 index-statistics presence (0 or 1)
5       u8 logical-name presence (0 or 1)
6..8    u16 logical-name byte length
8..16   u64 BTree metadata PageId
16..24  u64 distinct_non_null_keys (zero when absent)
24..32  u64 null_count (zero when absent)
32..36  u32 tree_height (zero when absent)
36      u8 lifecycle state (0 active, 1 retired)
37      u8 tree format (0 v1, 1 legacy owned v2, 2 generation-aware v3)
38..40  reserved zero
40..48  u64 nonzero IndexId
48..56  u64 generation (nonzero for tag 2, zero for legacy)
56..    logical-name UTF-8 bytes when present (maximum 255)

Following all entries: pending ownership count * 32 bytes
0..8    u64 nonzero IndexId
8..16   u64 BTree meta PageId (nonzero for tags 0/1, zero for tag 2)
16..24  u64 generation (nonzero for tag 1, zero for tags 0/2)
24      u8 reference tag (0 legacy, 1 generated root, 2 owner-only v3)
25..32  reserved zero
Presence means Pending; tag 2 is v9-only. Legacy v2 roots remain unreclaimable.
V6 pending records are 16 bytes and decode to explicit legacy references.

When root state = 2, following pending records: reclaim intent
0..8    u64 old_page_count N
8..16   u64 truncate_from M (3 <= M < N)
16..24  u64 checkpoint logical WAL base LSN
24..28  u32 covered identity count (nonzero, bounded by payload)
28..32  reserved zero
32..    count * (u64 IndexId, u64 meta PageId, u64 PageGeneration)
Covered IDs are strictly increasing. V9 owner-only records use (IndexId,0,0);
root-dependent records require nonzero generation below checkpoint base and a
meta in [M,N). V8 rejects zero identities. Storage additionally checks every
remaining covered allocation precedes the checkpoint base, and matches identities
against the entire retired catalog (full entries or minimal pending records).
Unknown, active, legacy, duplicate or mismatching identities are rejected.
V2-v7 reject state 2. V8/v9 have no extension when state is 0 or 1.
```

V2/v3/v4 headers require byte 7 and bytes 40..48 to be zero. Versions 2/3 have a
40-byte entry prefix with bytes 36..40 zero; v2 also requires bytes 5..8 zero.
Their IDs remain exactly their unique metadata PageIds. V4 IDs are decoded
explicitly. Only a v2/v3/v4 root may infer `max(all active AND retired IDs) + 1`,
because those formats never compacted. V5/v6/v7/v8/v9 roots must supply a boundary greater
than every active, retired or pending ID in the entire chain. Presence/zero mismatch, an absent v5/v6/v7/v8/v9 root
boundary, or a boundary on a continuation is corrupt. `u64::MAX` is a valid
exhausted next-ID boundary; CREATE returns typed IndexIdExhausted without
allocating a tree, never wraps or issues that last value. Legacy MAX IDs also
fail checked boundary derivation. A committed ID is never reused; rollback may
reuse an unpublished ID.

Only the root contains table statistics. Active index statistics require root
statistics, valid population counts and nonzero height. Retired entries cannot
contain statistics. Rebuild rejects duplicate IDs/handles across the entire
chain, duplicate active names/columns, bad bounds/kinds/links, cycles, malformed
names, counts and state tags. ANALYZE preserves the high-water and ignores retired
entries. CREATE reserves the next ID in the root in the same transaction as tree
allocation, backfill and final registration. Legacy root writes initialize the
boundary from the complete un-compacted chain, not just that page.

An ordinary legacy update can spill a suffix into a new continuation when the
explicit-ID representation no longer fits. New-page logging precedes link
logging and WAL sync precedes allocation. Reverse undo restores links first.
Explicit `compact_index_catalog` packs active entries/current snapshots in
creation order, followed by minimal pending records in IndexId order, reusing
the existing chain prefix. After a complete ownership proof, v3 full retirements
and generated-root pending records become owner-only pending records. Legacy
owned retirements retain roots; names, columns and statistics are discarded.
Compaction removes owner-only records only when a complete scan finds zero owned
pages. Other pending records are retained; repeated compaction is byte-idempotent. Dense legacy
upgrades may need additional trailing catalog pages. All replacement images are
preflighted and logged in one transaction, new allocations first and root last.
Full-page recovery undo or redo resolves an interrupted rewrite; it does not rely
on an atomic multi-page overwrite. Compaction leaves Heap metadata, root PageId,
Page/WAL formats, existing BTree payload versions and catalog_generation unchanged.

Only removed **legacy** registrations and unused continuation pages follow
permanent physical abandonment. New v3 retired trees remain in durable pending
inventory, including middle holes and merge orphans. Catalog compaction itself
never truncates. `inspect_index_reclaim` uses checkpoint admission plus no pinned pages,
fully decodes every managed Page and BTree payload, traverses authoritative
active/root-dependent-retired/raw roots, and rejects duplicate/overlapping
ownership and cross-owner live links. Owner-only retirements require full v3
payload validation without root reachability. Historical outgoing PageRefs may
be stale, but intrinsic structure, duplicate children and current-generation
owner conflicts still fail. Live incoming aliases from active/raw trees fail;
dormant outgoing links do not claim current Heap/catalog allocations. Their reachability is explicitly
unknown, reported as `None` and `owner_only_pages`, not counted as proven orphans.
Root-dependent v2/v3 orphans retain validated owners; v1 orphans remain unowned. Raw v1 trees cannot be
distinguished from historically abandoned root-reachable v1 trees, so the admin
report labels them unregistered legacy and never authorizes reclaim.

Round 10 implements Route B for registered BTree v3 pages. PageGeneration is
reserved from a synced WAL record's logical LSN; rollback never undoes the
reservation and checkpoint retains the logical end in the next WAL header.
The physical slot cache validates allocation identity. Runtime rollback checks
pins, discards removed frames without stale dirty writeback, truncates/syncs,
and retains the writer until durable RollbackComplete. Recovery skips completed
rollbacks and compares allocation identity before pageLSN; mismatches fail closed.
A generation reservation is record v4 tag 7 with no payload, in the existing
prevLSN chain; ordinary records retain v3 bytes and both versions are decoded.
The WAL v4 header and PageUpdate full-image layout do not change. Images contain
BTree v3 identities. No separate generation allocator or high-water exists.

General reuse remains forbidden: Heap RowId has slot generation, not page
allocation generation; catalog root/continuation links are still physical PageIds.
A dedicated Round 11 operation permits only whole retired v3 tails. The shared
Core maintenance admission rejects every retained database transaction handle;
Heap reuses checkpoint admission and pin checks. Two complete inventory scans
surround an internal checkpoint. No candidate skips all persistent mutation.
The fixed catalog root holds a bounded intent; capacity failure occurs before
checkpoint and never appends a catalog continuation into the suffix. Only
existing affected catalog pages are used for finalization, preflighted before
persisting intent; unrelated legacy continuation bytes are preserved.

Intent is an ordinary catalog PageUpdate transaction, synced through Commit and
then the data file before truncation. The selected checkpoint WAL generation has
no older tree updates: only catalog pages below M are touched until completion.
Clean, unpinned suffix frames are removed without writeback; PageManager checks
actual file length against the expected count, protects pages 0..3, calls set_len,
updates its count, and sync_all. A sync failure cannot restore the allocation.

Finalization logs removal of only covered full-retired/pending records and root
intent clear in one transaction, with root publication last. Commit is the
completion decision and data pages are synced before return. An error after
logging begins poisons the existing transaction runtime as RecoveryRequired;
no ordinary mutation, checkpoint or catalog compaction can continue. Reopen is
the retry entry; no second transaction state machine is introduced.

Open recovers ordinary WAL first (only retained catalog pages can be affected),
then decodes root intent before pending bounds/dereference. Exact covered refs
may be outside current length only while a validated intent is present. N means
revalidate the complete inventory and finish truncate; M means sync and finalize
without reading removed roots. Any other length or WAL base mismatch is hard
corruption. Finalization undo restores the intent after a loser/STEAL crash;
winner redo completes record removal. Normal active registry load happens last.
This is an abrupt-process-crash contract, not a power-loss claim.

The whole-owner restriction includes every remaining allocation, including
orphans after a meta/root has disappeared; an owner with any page below the
suffix cannot be finalized. Middle-hole owners remain pending.
Active trees, Heap, raw/v1/v2, unknown/corrupt, and catalog pages cannot be crossed.
Round 12 adds `inspect_reusable_pages`: a disposable, ascending-PageId inventory
with old PageRef, retired IndexId and `GenerationSafeBTreeV3` capability. Round 13
shares its candidate builder with the production registered-BTree allocator.
A lazy ordered cache retains empty and blocked states; claims locally revalidate
retirement, CRC, owner/generation and clean/unpinned frame eligibility. Root-dependent
retirement converts to owner-only pending in the same transaction before reuse.
Fresh synced reservations precede WAL record v5/tag 8 allocation transitions;
normal PageUpdate stays within one incarnation. Recovery validates full image
lineage before excluding superseded old-generation prefixes, then executes
ordered redo/reverse undo with generation-first LSN checks and exact old-image
restoration. Reopen, DROP, rollback, compaction and tail maintenance invalidate
the disposable cache. No durable general free catalog or cross-kind reuse exists.
Round 14 adds an independent NBTR v1 payload for explicitly retired individual
nodes. The enclosing Page v5 retains its former leaf/internal kind; the payload
contains only owner, generation and PageId, with no active-node semantics.
Deletion/normalization logs and publishes all unlink updates before retirement
PageUpdates. Reverse undo restores original nodes before incoming pointers.
Commit invalidates the cache; the single writer's transaction-local retirement
set excludes uncommitted markers from both rebuild and claim, including stolen
pages. Active traversals reject marker payloads through strict node decoders.
The ordered cache distinguishes whole-owner v3 and individual-marker authority;
same-owner transitions are allowed only from the latter with a fresh generation.
Markers are independent of pending owner records and excluded from whole-owner
zero-page accounting and tail intents. Historical unmarked active orphans remain
non-reusable until explicit Round 15 maintenance. Reachability subtraction alone
never authorizes retirement.

`Database::adopt_historical_btree_orphans(TableId)` requires the existing Core
no-retained-handle gate and Heap checkpoint admission, rejects pins and durable
tail intents, and supports only a single Heap placement. Preflight validates the
whole file and all active/retained/raw root traversals. No eligible ordinary v3
orphan means no WAL write or checkpoint. Otherwise an internal checkpoint flushes
the committed recovery baseline and rotates WAL; a fresh identical global proof
then selects active-owned ordinary v3 pages minus validated reachable pages.
Meta and current root pages are additionally protected. The proof follows full
PageRefs for meta, root and every child; DFS leaf order must exactly match all
leaf-next PageRefs.
PageId membership sets detect physical aliases only after generation validation.

The plan keeps ascending PageId order and exact before images (owner, PageRef,
kind, pageLSN, payload and CRC). Every candidate must be clean, unpinned and match
disk before BEGIN and again before any marker log; each image is revalidated
immediately before its own update too. The exclusive synchronous owner and
dedicated writer prevent an intervening DML/DDL/vacuum operation. One ordinary
PageUpdate transaction converts candidates to existing NBTR v1, without new
generation reservations, a new catalog, or an allocation transition. Partial
runtime failure rolls back; an unresolved commit/rollback or checkpoint failure
requires reopen. Commit invalidates the existing cache, then committed markers
are flushed before success so a following allocator can use them immediately.
No schema/inspection/planner generation changes, automatic open-time adoption,
or NBTR tail eligibility changes occur. See the checkpoint section below and
[Round 15's detailed proof](historical-orphan-round15.md).
See also [Round 14](btree-orphan-round14.md), [Round 13](page-transition-round13.md), [Round 12](page-reuse-round12.md),
[Round 11](index-tail-reclaim-round11.md) and [Round 10](page-generation-round10.md).

A registered table index is distinct from a raw tree created through
`HeapStorage::btree().create`: raw trees never enter the active index registry from page scans. Only admin
ownership inventory validates and classifies their full payloads. Open follows only the metadata root, separates retired ownership from active definitions, rejects cycles, duplicates,
out-of-range links, and wrong page kinds, then verifies every column against
the canonical `TableDef` and every BTree metadata `IndexSpec` against that
column's nominal type and nullability. Validated definitions and optional
optimizer snapshots are cached in persistent creation order. BTree roots
remain uncached; analyzed height is a snapshot, not authoritative metadata.

`HeapStorage::create_index` owns one transaction and single-writer lease. It
creates the tree, walks the stable read view page by page, buffers at most one
Heap page of typed `(value, RowId)` entries, and writes the catalog registration
as the final logical mutation. Only a successful durable commit updates the in-memory
registry, so crashes or errors before commit leave no visible partial index.
Generic named CREATE INDEX uses the same build. Inside an explicit database
transaction, Core publishes planner and inspection caches only after durable
commit; rollback leaves no visible registration.
Round 7 DROP mirrors that publication contract: it logs a retired state and
clears index statistics, then removes the active definition, DML plan, and
statistics cache only after durable commit. Core increments catalog_generation
only on publication. PG sessions refresh on the next metadata query. Uncommitted
DROP leaves the published active path usable and DML-maintained; rollback
restores its catalog bytes. Existing prepared queries replan at execution.
Retired definitions retain tree ownership in storage-only inspection but never
enter ordinary CatalogInspection, access paths, ANALYZE, or vacuum. Explicit
Round 9 compaction replaces owned retired definitions with minimal pending
ownership after validating the full file. Legacy and historical pre-owner pages
remain unreclaimable. No free-list exists; repeated CREATE/DROP still grows the
file in the original Round 9 baseline; current Round 13 reuse bounds that growth.
See [the ownership audit](index-reclaim-round9.md).
For every physical Heap version that has not been vacuumed and every registered
index, one candidate entry `(version[column], version RowId)` exists. Raw
B+Trees are outside this invariant. INSERT and UPDATE publish a new Heap version
before inserting its candidates in persistent creation order. UPDATE and DELETE
leave predecessor candidates intact because an older snapshot may still need
them. Manual vacuum removes each dead version's exact candidates before
physically tombstoning its Heap record. All operations share the caller's
transaction, buffer pool, WAL, and prevLSN chain. Candidate key-size and exact
predecessor-entry checks run while holding the single-writer lease before the
first physical mutation; any later failure marks the transaction
`RollbackRequired`.

## ANALYZE snapshots and costed index access

Logical plans remain storage-independent: query meaning is still represented
as `Filter(Scan)`, never as a logical index operator. Core walks table storage
order and each storage's advertised access paths to materialize plain planning
context. Entries contain table/column identity, opaque table-scoped
`AccessPathId`, point/range capabilities, and optional
`TableStatistics`/`IndexStatistics` domain values. The planner depends on the
pure `netbadb-index` domain crate for typed ranges and statistics, not on
`netbadb-storage`, and receives no BTreeHandle, PageId, WAL, buffer, or catalog
representation.

An access path may additionally provide storage-neutral integer cost hints:
point-probe base work, expected point I/O, range startup work, and sequential
work per estimated match. Heap keeps the historical height-based default. LSM
derives hints from overlapping L0 files, nonempty L1+ levels, and a fixed
conservative Bloom false-positive expectation. The planner does not match
`StorageKind`, read Bloom bits, or perform storage I/O.

For a Filter directly above a Scan, point access recognizes
`indexed_column = non-NULL literal`, its commuted form, and nullable-column
`IS NULL`. Bounded range access recognizes and tightens `>`, `>=`, `<`, and
`<=` literal comparisons through nested AND, including reversed operands, for
Int64/UInt64 indexes only. It never derives access through OR or NOT. Equality
with NULL, IS NOT NULL, one-sided, Text/Bool, and column-to-column ranges,
joins, and index intersection/union are not range access paths.

`HeapStorage::analyze` owns one transaction and writer lease, scans the Heap
once for all registered indexes, reads each BTree's actual height, rewrites
every catalog page at most once in root-to-tail WAL order, commits, and only
then replaces the cache. `Database::analyze(table_id)` exposes that operation.
Table statistics contain live row count and all managed pages (`page_count -
1`); index statistics contain distinct non-NULL keys, NULL count, and analyzed
tree height. DML neither maintains nor invalidates these values. They are
optimizer snapshots and may become stale.

Without table statistics, or when all eligible indexes lack statistics, the
first eligible point path wins exactly as in Phase 4E; ranges require costable
statistics. With a table snapshot, analyzed eligible point and range candidates
are compared with SeqScan using integer `u128` page-visit estimates:

```text
SeqScan             = managed_page_count
Point IndexScan     = 1 + tree_height + estimated_matches
equality matches    = ceil((row_count - null_count) / distinct_non_null_keys)
IS NULL matches     = null_count
RangeIndexScan      = 1 + tree_height + estimated_range_matches
possible keys       = exact discrete count from the two integer bounds
range matches       = min(non_null_rows, possible_keys * average_duplicates)
```

IndexScan must be strictly cheaper; a tie selects SeqScan. Equal index costs
preserve registration order. If the only eligible index is unknown it retains
the Phase 4E fallback. These choices apply identically to SELECT, UPDATE, and
DELETE.

The physical shape always retains the complete predicate:

```text
PhysicalPlan::Filter(original predicate)
    -> costed access selection
         -> PhysicalPlan::SeqScan
         -> PhysicalPlan::IndexScan(opaque AccessPathId, exact key)
         -> PhysicalPlan::RangeIndexScan(opaque AccessPathId, typed bounds)
```

Executor passes the opaque access identity and typed key/range to TableStorage.
The current Heap variant resolves it only against its registered B+Trees; point
lookup performs `BTree::lookup`, while range lookup locates the lower leaf and
traverses ordered `next_leaf` links. Both treat returned RowIds as candidates,
apply the statement's same ReadView, and return opaque `StorageRowHandle`
values. Invisible versions are skipped; an unknown access path, stale/missing
locator, wrong page, or corruption remains an error and execution never hides
it by falling back to SeqScan. UPDATE and DELETE therefore finish access
traversal and
target materialization before maintaining the same index, avoiding iterator
invalidation or revisiting newly inserted keys. Because the complete Filter
remains, stale statistics can affect performance and plan shape but not query
semantics. The range estimate needs no min/max, histogram, or MCV and changes
neither BTree payload v1 nor IndexCatalog v2. Index-only scans, one-sided and
Text/Bool range costing, histograms/MCVs, index intersection/union, index
nested-loop joins, join ordering, and sort elimination remain future work.

## Transaction and WAL boundary

The WAL uses two alternating files, `<database>-wal` and
`<database>-wal.next`. Appending writes a complete record to the selected
generation but does not imply durability. `WalManager` separately tracks the
highest written and durable logical LSN, and `flush_through` advances durability
with `sync_data`. LSN zero is reserved for “no LSN”. Physical WAL offsets and
logical LSNs are deliberately different:

```text
logical LSN = generation base_lsn + (physical record offset - 48)
```

The first generation starts at logical LSN 1. A new generation's base is the
old generation's logical end, which is strictly greater than every record LSN
that existed there. Physical offsets can therefore restart at byte 48 without
making historical pageLSNs incomparable or reusable.

MVCC completion state is stored separately in `<database>-txn-status`. The
append-only file has a checksummed 16-byte header followed by checksummed
32-byte fixed records:

```text
header
0..4    NBTS magic
4..6    u16 status format version (1)
6..8    u16 header size (16)
8..12   reserved zero
12..16  u32 CRC32C

record
0..4    TXST magic
4..6    u16 record version (1)
6       u8 status (Committed=1, Aborted=2)
7       reserved zero
8..16   u64 TxnId (non-zero)
16..24  u64 CommitSeq (non-zero only for Committed)
24..28  u32 CRC32C
28..32  reserved zero
```

Active state is runtime-only. A tuple that references neither a durable status
nor a transaction active in this process is corruption, not implicitly aborted.
The status file is synced for each terminal record and rejects truncation,
unknown tags, conflicting duplicate decisions, nonzero reserved fields, and
checksum failures.

`Snapshot` consists of `visible_csn`, optional own `TxnId`, and a nonzero
statement `CommandId`. The visible CSN is the greatest durably published commit
sequence at capture. Read Committed captures it per statement; Repeatable Read
pins the first capture. Insertion is visible when its owner committed no later
than the snapshot, or it is the reader's own transaction with `cmin` no later
than the statement. Deletion/expiration hides a version only when `xmax`
committed no later than the snapshot, or it is the reader's own transaction
with `cmax` no later than the statement. Active and aborted inserters are
invisible; active and aborted expiring transactions leave the predecessor
visible. Sequential scans, selective/borrowed fast paths, direct counts,
point/range index candidates, and RowId fetches all call this rule.

The WAL file header is:

```text
0..4    NBWL magic
4..6    u16 WAL format version (4)
6..8    u16 header size (48)
8..16   u64 generation ID (starts at 1)
16..24  u64 base logical LSN (non-zero)
24..32  u64 checkpoint LSN (zero means no prior checkpoint)
32..40  u64 next transaction ID high-water mark (non-zero)
40..44  u32 CRC32C (little-endian)
44..48  reserved bytes (zero)
```

The header checksum covers all 48 bytes with bytes 40..44 treated as zero.
Magic, version, header size, and reserved bytes are checked before CRC32C; only
after checksum verification are generation, base LSN, checkpoint LSN, and the
transaction high-water mark trusted. WAL versions 1 through 3 are rejected
explicitly; this experimental format has no migration framework. PageUpdate
remains a pair of complete page images. Page v5 and WAL container v4 are
unchanged in Round 10. Ordinary records retain version 3; the new generation
reservation uses record version 4. Round 13 transitions use record version 5;
record versions 3, 4 and 5 are decoded.

Every record has a 40-byte fixed header followed by a bounded payload:

```text
0..4    WREC magic
4..6    u16 record format version (3 ordinary, 4 reservation, 5 transition)
6       u8 record type (Begin=1, PageUpdate=2, Commit=3, Abort=4,
                        RollbackComplete=5, Prepare=6, PageGenerationReservation=7,
                        PageAllocationTransition=8)
7       reserved byte (zero)
8..12   u32 total record length
12..16  u32 CRC32C (little-endian)
16..24  u64 logical LSN
24..32  u64 transaction ID
32..40  u64 prevLSN (zero only for Begin)
40..    payload
```

`Begin`, `Commit`, `Abort`, `RollbackComplete`, and `PageGenerationReservation`
have no payload. Tag 7 is rejected in record version 3, including partial
headers; tag 8 requires record version 5. `Prepare` contains one nonzero u64 DatabaseTxnId. A reservation is synced
before its logical LSN may become a PageGeneration and is never physically undone.
`PageUpdate` stores an explicit u64 page ID, one 4 KiB before-image, and one 4
KiB after-image. `PageAllocationTransition` uses the same bounded image layout
with strictly different registered-v3 generations and owners; its after pageLSN
is the transition LSN. Consequently, the maximum accepted record is 8,240 bytes. The
record type determines the only valid total length; there is no stored payload
length. The CRC32C covers the complete header and payload with bytes 12..16
treated as zero. The scanner first validates framing and bounded type-derived
lengths without allocation, confirms the complete record physically exists,
then verifies CRC32C before decoding LSN, transaction state, prevLSN, or page
images. Record format version 1 is explicitly unsupported.

A final record whose physical bytes end before its validated total length may
be truncated during recovery after its available prefix passes structural
validation. A complete record with a checksum mismatch is corruption, even at
EOF, and is never converted into a crash-tail truncation.

The write ordering invariant is:

```text
construct after-image with pageLSN
    → append PageUpdate
    → publish dirty buffer frame
    → flush WAL through pageLSN
    → write data page
```

Commit uses `append Commit → flush_through(commitLSN) → append and sync
Committed(TxnId, CommitSeq(commitLSN)) → Committed`. Publishing status cannot
overtake the WAL decision. If a crash occurs after WAL sync but before status
sync, startup scans the durable WAL and idempotently reconciles the missing
status before admitting reads. If a flush or status write fails, the handle
remains `CommitPending`; retrying commit reuses the same record and decision.
A new-page update is also flushed
before extending the database file, because writing the allocator's zero page
is itself a data-file write that must not overtake its WAL record.

Runtime rollback uses:

```text
Active
    → append + flush Abort
    → RollbackPending
    → follow this transaction's prevLSN chain backward
    → validate and install each full before-image
      (zero before-image removes the exact trailing page)
    → sync each affected rollback page or truncation
    → append + flush RollbackComplete
    → RolledBack
```

`RollbackRequired` is distinct from `RollbackPending`. The former means a
compound logical operation has appended only part of its physical WAL history;
no Abort exists yet, and only `rollback()` is permitted. The latter means Abort
has been appended and physical undo is running or retryable. A two-page
versioned UPDATE prepares both after-images before WAL publication, then
deterministically logs the new-version PageUpdate followed by the predecessor
expiration PageUpdate. Any later logging,
flush, allocation, buffer acquisition, or publication failure marks the
transaction `RollbackRequired`, so a half update can never commit.

Before-images restore their historical pageLSN; rollback does not generate
ordinary PageUpdate records. A rollback error leaves `RollbackPending` and
retains writer ownership, so calling `rollback` again safely repeats the
idempotent physical undo. Only the affected rollback page is flushed per undo
step. Commit remains NO-FORCE; rollback synchronizes its physical changes before
reporting success.

All disk widths are explicit; no Rust struct layout, host-endian values,
pointers, or `usize` are persisted. Malformed headers, slot directories,
record ranges, row lengths, tags, and UTF-8 values return typed errors.

## Startup recovery

Recovery is synchronous storage-layer work and completes before a `BufferPool`,
`HeapStorage`, or `Database` is exposed:

```text
Database::open
    │
    ▼
Open Data File + WAL
    │
    ▼
Analysis
    │
    ├── Winners (Commit exists)
    ├── Completed rollback (RollbackComplete exists)
    └── Losers (incomplete or Abort-only)
    │
    ▼
Validate transition lineages; redo retained page operations in ascending LSN
    │
    ▼
Undo losers in descending global LSN
    │
    ▼
Sync undo + durably finalize recovered losers
```

Analysis builds transaction lastLSNs and an LSN lookup. For transitioned slots it
also certifies continuous full-image lineage and the redo start justified by
the current incarnation; see [Round 13](page-transition-round13.md). Redo repeats history,
including loser updates, but skips transactions with durable
RollbackComplete: an existing page first requires the same allocation
generation, then is skipped only when its pageLSN is at least the update LSN, and that pageLSN is trusted only after full page validation;
otherwise the validated after-image is installed. A new page must be exactly
the next trailing page, so WAL cannot create page-ID gaps. Undo follows each
incomplete or Abort-only loser's prevLSN chain through a max-heap and installs
PageUpdate before-images in global descending LSN order. A zero before-image
means the loser allocated that page, which can only remove the exact trailing
page. The page file is synchronized, then recovery appends Abort for any loser
that was still Active, appends RollbackComplete, and flushes those terminal
records before returning. This prevents a recovered loser from conflicting
with or overwriting a later winner on another restart.

After physical recovery, open rescans the selected durable WAL generation and
reconciles the status sidecar: every Commit becomes
`Committed(TxnId, CommitSeq(commit_lsn))`, and every RollbackComplete becomes
`Aborted`. Records already present are idempotent; conflicting decisions are
corruption. This closes both crash windows around terminal status publication
before any ReadView can be created.

A checksum-invalid current page is a hard recovery error before its pageLSN is
read or compared. Recovery does not blindly repair it from retained WAL because
a checkpoint may already have recycled the page's complete history.

Because full-page after-images can include another transaction's uncommitted
contents, the runtime permits one writer and acquires that ownership before any
heap page mutation or allocation. RollbackRequired, CommitPending, and
RollbackPending retain it.
Commit durability or completed physical rollback releases it. Dropping an
unfinished dirty writer marks the open storage recovery-required; later writes
and close fail, while read-only handles may still be created. Analysis also
rejects historical retained WAL where a committed page update follows an
unresolved loser update to the same page before recovery writes any page. This
is the write-side safety invariant; read isolation comes from MVCC status and
ReadViews rather than from the writer lease. It is not cross-process locking.

The algorithm intentionally has no compensation log records. During runtime
rollback, Abort is durable before physical undo and RollbackComplete becomes
durable only after all rollback page changes are synchronized. A crash before
completion therefore leaves an Abort-only loser: startup repeats its history
and deterministically undoes the whole prevLSN chain. Startup recovery uses the
same ordering when it finalizes a loser after undo. A durable completion record
means the already-synchronized transaction images must be skipped, so a later
committed winner is never overwritten by reapplying the old rollback. The
retained valid WAL is synchronized before any recovery page write.

At startup only an incomplete final record whose available header is
structurally valid may be truncated at EOF. Corrupt middle records, invalid
magic/version/type/length fields, invalid transaction state, broken transaction
chains, malformed data pages, and malformed before/after page images fail open
with typed errors.

The current model is single-writer, STEAL, NO-FORCE, WAL-protected, and supports
synchronous physical runtime rollback plus startup crash recovery. `abort` is
an alias for that rollback operation. MVCC provides Read Committed and
Repeatable Read snapshot visibility over active-writer pages. There is no
Serializable isolation, fuzzy checkpoint, concurrent writer queue, or
cross-process writer lock.

## Manual vacuum

`HeapStorage::vacuum` and `Database::vacuum(TableId)` are explicit synchronous
maintenance operations. A ReadView pins its `visible_csn` in the in-memory
status store until drop. Vacuum chooses the oldest pinned CSN, or the current
maximum committed CSN when none is pinned, and reclaims only tuples whose
inserter aborted or whose committed `xmax` is no later than that horizon. Active
or aborted expirers are never dead. The operation first validates and collects
dead versions, then owns one normal write transaction, removes every matching
exact registered-index candidate, physically tombstones the Heap slot, and
commits through the ordinary WAL/status path. Errors roll back the whole vacuum.
Slot reuse still increments generation, so a locator retained past vacuum cannot
name a later occupant. There is no background worker, automatic scheduling,
status-log compaction, or file shrinking in this phase.

## Checkpoint and WAL lifecycle

The first checkpoint model is intentionally quiescent. `TransactionManager`
registers every successful `Begin`, including read-only handles, and unregisters
exactly once after durable commit, completed rollback, or clean active drop.
Dropping a dirty/pending writer unregisters the vanished handle but changes
runtime health to `RecoveryRequired`. Checkpoint admission requires:

```text
writer = Idle
runtime health = Healthy
outstanding transaction handles = 0
```

It never waits or queues. Active, RollbackRequired, CommitPending,
RollbackPending, read-only active, and RecoveryRequired states return typed
errors. Clean close enforces
the same no-outstanding-handle safety property so no live transaction can retain
a prevLSN into recycled history.

The checkpoint order is:

```text
verify quiescence
    → flush WAL through the highest written LSN
    → flush every dirty buffer frame (each preserves WAL-before-page)
    → synchronize the database file
    → capture old logical end + next TxnId
    → remove only the inactive older WAL slot
    → create the inactive slot with generation + 1 and the captured metadata
    → synchronize the new WAL file and its parent directory
    → switch the shared WalManager to that generation
    → delete the superseded slot and synchronize its directory entry
```

The synchronized data file is the checkpoint success foundation: every effect
represented by retired records is already durable before creation of the next
generation begins. `BufferPool`, `TransactionManager`, and `HeapStorage` retain
the same shared `WalManager`, so switching cannot split WAL ownership.

The two slots form a small crash-safe selection mechanism rather than a general
segment manager. Rotation never removes the currently selected valid slot.
Before the new header is complete, open ignores a truncated inactive header and
uses the old generation. Once a complete new header is durable, both files may
remain and open selects the greater generation after checking consecutive IDs,
base/checkpoint continuity, and the TxnId high-water mark. A malformed complete
newer candidate is a hard error. Open deletes a validated superseded generation
before exposing the selected manager. A successful checkpoint therefore keeps
one WAL file; a crash or cleanup failure may temporarily leave two, and the next
open or checkpoint deterministically removes the older one.

Recovery runs the existing analysis/redo/undo algorithm only over records in
the selected latest safe generation. Old pageLSNs are not reset. Post-checkpoint
updates receive logical LSNs above the generation base, so redo can compare them
directly against pages written before checkpoint. The header's `next_txn_id`
preserves transaction identity monotonicity after old records are recycled.

There is no clean-shutdown marker. Scanning one bounded active generation is
simple and deterministic; a marker would require a separate durable
clean-to-dirty invalidation state machine before the next mutation and does not
currently remove enough work to justify that risk.

Deterministic subprocess tests exercise STEAL loser undo, NO-FORCE winner redo,
commit and rollback durability boundaries, recovery interruption, and both WAL
generation rotation windows. Each child terminates without running Rust object
destructors, then the parent opens the database twice to verify convergence and
idempotence. This models abrupt database-process loss only; it does not simulate
kernel, machine, controller, or storage-device power loss.

## Embedded and server modes

`netbadb-core` remains synchronous and embedded. `netbadb-protocol` defines a
transport-independent binary v1 contract and depends only on shared types.
`netbadb-server::SessionState` is also synchronous: it owns handshake and one
optional table-scoped transaction while borrowing the database owner for each
request.

The experimental PostgreSQL foundation adds a parallel frontend boundary:

```text
PostgreSQL bytes
    -> netbadb-pgwire bounded typed messages
    -> PostgreSQL session / prepared statement / portal state
    -> ordinary SQL: compiler-owned typed statement and parameter metadata
    -> metadata SQL: bounded compatibility operation classifier
    -> ephemeral metadata projection <- Database::inspect_catalog()
                                      <- Canonical Schema + index registry
    -> compiler-resolved StatementAccess + authorization
    -> shared netbadb-server DatabaseSession transaction/execution lifecycle
    -> netbadb-core
```

`netbadb-pgwire` depends only on `netbadb-types`; it contains framing, message
domains, PostgreSQL OIDs, format codes, and scalar text/binary adaptation, but no
socket listener or database calls. PostgreSQL OIDs and transaction-aborted
behavior stop at the server adapter. The compiler exposes frontend-neutral
`ParameterId` expressions, contextual parameter inference, typed logical
binding, compile error categories, and statement output metadata. Parse
compiles once; each Bind decodes values once and substitutes typed literals
without formatting or reparsing SQL, so planning sees concrete predicates.
PostgreSQL error and RowDescription mapping do not inspect AST/HIR Rust layouts,
execute a query for metadata, or leak OIDs into HIR, relational IR, planner,
executor, or storage.

Rounds 3 through 5 keep ORM and psql introspection in a separate, typed
server-side adapter because
SQLAlchemy's PostgreSQL catalog SQL uses schema-qualified system relations,
arrays, `ANY`, `regclass`, and catalog-only functions that would otherwise
force a premature full PostgreSQL parser into the native compiler. The adapter
classifies queries from structural relation/function/predicate markers into a
closed `CompatibilityStatement` domain; it does not compare whole SQL strings
or execute user-table SQL. Each session derives a bounded read-only snapshot
from `Database::inspect_catalog()`. The Core DTO combines Canonical Schema
table/column identity with registered index definitions while excluding B+Tree
handles, PageIds, optimizer policy, and storage variants. A registered index
has durable lifecycle identity `(TableId, IndexId)`. The unchanged inspection
DTO exposes only active `(TableId, ColumnId)` membership,
`BTree` kind, and `unique=false`; LSM clustering access is not a secondary
index. Partition-local physical indexes are not projected as logical indexes
because the current registry cannot prove they form one logical definition.
Every emitted row is generated from that snapshot and filtered through the
principal's existing `TableId` visibility.
There is no PostgreSQL catalog heap, WAL record, or second source of schema
truth.

Round 5 adds a second entry into that same evaluator for psql Simple Query.
A bounded tokenizer recognizes semantic catalog relations, columns, functions,
operators, and literal predicates without implementing general PostgreSQL
SELECT. Catalog name patterns compile into a small anchored automaton subset
(`literal`, `.`, and `.*`) with fixed byte/atom/token/nesting limits and a
non-backtracking dynamic-programming matcher. The adapter therefore supports
psql 17.11 `\d`, `\dt`, and `\di` without adding regex or PostgreSQL catalog
syntax to parser, HIR, relational IR, planner, executor, or storage.

Compatibility object OIDs are server-only deterministic identifiers. Separate
SHA-256 domains cover table and index objects; index identity includes the
canonical fingerprint, column, kind, and uniqueness. All candidates share one
high synthetic range and collision set, and deterministic linear probing
resolves collisions. Index names combine a sanitized readable table/column
prefix with a stable digest suffix, are bounded to 63 ASCII bytes, and
collision-check the complete catalog. Built-in PostgreSQL type OIDs stay centralized in
`netbadb-pgwire`, while object OIDs do not enter types, HIR, relational IR,
planning, execution, storage, or persistent formats. Stability is guaranteed
for the same Canonical Schema and index registry across connections and
restarts; names and OIDs are not persistent contracts across schema/index
changes.

The generic compiler did gain two ordinary SQL expression capabilities exposed
by the real client: postfix casts for the lossless BOOL/INT64/TEXT family and
qualified projection aliases. Cast validation occurs in typed HIR and retains
contextual nominal types, then binding removes the proven no-op cast so the
planner still sees concrete values. PostgreSQL-only `regclass`, `regtype`, OID,
array, and catalog function semantics remain confined to the adapter.

Native and PostgreSQL sessions both use the private synchronous
`DatabaseSession` for the optional database transaction, execute/commit/
rollback, disconnect rollback, and row-limit policy. PostgreSQL additionally
owns named/unnamed prepared statements, portals, extended-protocol recovery to
Sync, and the `I`/`T`/`E` compatibility state. These are frontend semantics and
do not alter the core transaction state machine.

FROM-less scalar queries use the ordinary typed pipeline. Parser/HIR represent
an optional source, relational and physical IR use a single empty `OneRow`, and
`ScalarProject` evaluates literal or bound-parameter expressions over it. This
keeps `SELECT 1`, `SELECT true`, `SELECT 'x'`, and `SELECT NULL` out of the
PostgreSQL compatibility-query string matcher.

The first executable boundary is deliberately an exclusive listener mode:
`netbadbd --postgres` uses manifest v4's existing loopback listen address for
PostgreSQL instead of Protocol v2. It has the same dedicated synchronous
Database owner/worker model, compiler-resolved table authorization, connection
cap, and socket timeouts. A future versioned deployment manifest may configure
simultaneous native and PostgreSQL listeners backed by one worker; manifest v4
was not silently reinterpreted to add a second address.

The blocking TCP runtime uses one OS thread per accepted connection and one
dedicated database worker thread. Connection threads own the socket, optional
rustls connection state, authenticated transport identity, SessionId, decoded
client frames, and response batches. Typed channels carry only Send-safe values
to the worker. The worker constructs and exclusively owns the Database, every
SessionState, every Transaction, and the association between a SessionId and
its ClientIdentity. Storage's Rc/RefCell transaction internals never cross a
thread boundary and are not replaced by networking-driven Arc/Mutex state.

Network connections may progress concurrently while reading or writing, but
the worker consumes one FIFO command queue and serializes all database request
execution. Each connection sends one request, waits for its complete response,
writes and flushes that batch, and only then reads the next request. A long
query therefore delays other sessions; Phase 5C1 does not claim concurrent
database execution.

Protocol requests execute one at a time. Query results become `QueryStart`,
zero or more `QueryRow` messages, and `QueryEnd`; they are not encoded as one
unbounded result frame. Hello returns each table's stable `TableId` and
canonical 32-byte schema fingerprint in schema declaration order. Stable wire
error codes and protocol transaction-state tags are mapped explicitly rather
than exposing Rust enum layout or debug output.

The listener is nonblocking only so a shutdown command can stop acceptance;
accepted streams are explicitly restored to blocking mode. Clean EOF, protocol
violations, and response I/O failures all close the corresponding SessionState
through the worker. A close rollback failure is fatal to that worker rather
than dropping a retryable transaction and continuing service. Graceful server
shutdown closes connection sockets, joins their threads, closes remaining
sessions, explicitly closes the Database, and joins the worker.

`netbadbd` reads deployment manifest v4 before startup. Relative heap and TLS
paths are resolved against the manifest directory. Certificate, private-key,
and client-CA material is parsed into a mandatory-client-auth rustls config
before the database worker starts; the worker then calls
`Database::open_tables_with_expectation` before the listener is bound. Manifest
TableDefs are required exact subset expectations; persisted SchemaCatalog alone
reconstructs the complete logical schema and physical inventory. Missing catalogs
never trigger startup creation or legacy migration. Plaintext listeners are
restricted to loopback, while non-loopback listeners require mutual TLS.

Authentication, authorization, compilation, and execution remain separate.
The TLS connection thread authenticates a transport peer and derives the
verified leaf-certificate fingerprint. During `OpenSession`, the database
worker resolves that identity to an immutable principal policy; a trusted but
unlisted certificate is closed before SessionState and Hello. The worker keeps
the resolved grants beside the transport-neutral SessionState. For Execute it
uses `DatabaseSession::prepare` to compile against the current transaction view
and obtain canonical read/write TableIds and schema-write access from the prepared
object. The worker checks those capabilities before passing that same object to
SessionState's execution path. Core describes access but knows nothing
about clients or policy; TLS, authorization, and server dependencies never flow
into compiler, planner, executor, storage, or persistent formats.

Handshake and session sequencing precede authorization, while successful SQL
compilation precedes SQL permission checks. Begin and Analyze validate table
existence before their independent grants. Commit, Rollback, disconnect close,
and shutdown bypass grants so an owned transaction can always be resolved.
HelloAck visibility is filtered by the worker after SessionState builds the
canonical schema-ordered identities; protocol capabilities remain unfiltered.

Connection admission and socket read/write timeouts happen before TLS. A
connection thread explicitly completes certificate verification, derives
SHA-256 over the verified client leaf DER, and only then asks the worker to
create a `WorkerSession`. Failed TLS peers therefore consume a bounded socket
and thread while handshaking but never own SessionState or Transaction. The raw
control-socket clone remains available so shutdown interrupts both TLS and
NDBP reads. ClientIdentity is runtime metadata only; SessionState and all core,
protocol, and persistent layers remain TLS-unaware.

Operational limits remain at server boundaries. The accept loop reaps finished
threads and uses its connection-vector length to reject excess sockets before
creating a SessionState or connection thread. Every admitted blocking stream
has a read timeout for idle and partial-frame clients and a write timeout for
blocked response delivery. Timeout cleanup uses the same fallible
`SessionState::close` path as every other disconnect.

The database worker remains synchronous and cannot be safely preempted. Socket
read inactivity is therefore not a statement execution timeout. Likewise,
`max_result_rows` is checked by SessionState only after core execution has
fully materialized QueryResult. It prevents expansion into an excessive number
of protocol messages but is not a complete executor-memory limit.

Runtime metrics use only standard-library atomics and expose read-only
snapshots. They count admitted, rejected, active, and closed connections;
successful and failed TLS handshakes; authenticated connections; worker
requests; protocol failures; idle timeouts; write failures; and
response-row-limit and authorization-denial errors. Metrics never control admission or database
correctness and contain no SQL, values, table names, certificate contents, or
client-address labels.

Networking remains synchronous and must not leak async into parser, compiler,
planner, executor, page, storage, WAL, or recovery. Protocol v2 is a network
contract, not a database-file format. The current independent persistent
contracts are Canonical Schema v1, Heap metadata v5, MVCC tuple v1,
transaction-status v1, Page v5, WAL v4/record v3 plus v4 reservations and v5 transitions, BTree
v1/v2/v3, and IndexCatalog v9 (backward decode v2 through v8). Deployment manifest v4 is configuration, not a database format or canonical
schema identity.

Rust applications choose either the default embedded SDK or the optional
synchronous remote surface:

```text
Embedded Rust application
    -> netbadb-sdk (default `embedded` feature)
    -> netbadb-core

Remote Rust application
    -> netbadb-sdk::remote (`remote` feature)
    -> netbadb-client
    -> netbadb-protocol
    -> netbadbd
```

`netbadb-client` owns blocking TCP/rustls transport and the Protocol v2 client
state machine. It has no production dependency on core, server, executor,
planner, or storage. It reuses the authoritative Rust protocol frame and value
codec, while the Go client intentionally remains an independent implementation
that proves the wire contract is language-neutral.

Remote Rust `Rows` and `Transaction` values exclusively borrow their Client,
preventing multiplexed requests at the type boundary. Explicit `Rows::close`
drains to QueryEnd; dropping unfinished rows closes the connection. Explicit
commit and rollback wait for a server response, while dropping an active
transaction only closes the transport and relies on disconnect rollback. A
lost response can therefore leave DML, commit, or rollback outcome ambiguous;
the client never reconnects, retries, or replays a request.

Go applications can use generated typed bindings above the independent
Protocol v2 client under `sdk/go`:

```text
Language-neutral SDK Schema Spec v1/v2
    -> Rust validation and canonical TableDef fingerprints
    -> deterministic generated Go bindings
    -> Go Protocol v2 client
    -> Protocol v2
    -> netbadbd
```

The Go transport, codec, values, result streaming, transaction lifecycle, and
schema-fingerprint gate use only the Go standard library. They share no Rust
memory or layout, use no cgo or FFI, and do not replace typed wire messages with
JSON execution IR. Canonical schema fingerprint generation remains
Rust-authoritative; generated Go code embeds those bytes and compares them with
HelloAck identities. Schema Spec JSON ordering or serialization never defines
identity. Generated table wrappers accept only the complete canonical table row
shape in canonical column order and explicitly decode nominal and nullable
values without reflection.

Schema Spec v1/v2 is code-generation input only. It is not Canonical Schema v1's
binary identity encoding, cannot configure listeners, TLS, authorization, or
heap paths, and is not the server's deployment source of truth. Server startup
still gates manifest `TableDef` against heap metadata, while generated `Dial`
gates embedded fingerprints against authorized HelloAck tables. These two hard
failures detect drift while the inputs remain separate.

## Performance baseline boundary

The Phase 7A benchmark is an optimized custom Cargo target attached to
`netbadb-core`. It consumes only public database and inspection APIs and adds no
runtime dependency or alternate execution path:

```text
deterministic temporary databases
              ↓
       public Database API
          ↙          ↘
real execution      StatementInspection
          ↓                 ↓
correctness checksum   chosen operator gate
          ↘                 ↙
         warm-cache timing samples
```

Fixture construction, index backfill, `ANALYZE`, plan inspection, correctness
verification, reporting, close, and cleanup remain outside timed query loops.
The benchmark records current behavior; it does not feed measurements back
into planning, expose a new planner API, or change any persistent representation.
