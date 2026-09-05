# Adopted-source final index architecture audit (Round 47)

Historical audit: [Round 48](adopted-source-final-index-round48.md) now
productionizes Candidate A. The test-only correctness implementation described
below has been removed; its fixture delegates acceptance, state gates and
finalization to production Core. The Round 47 real-client script now runs the
Round 48 positives and terminal negatives. Historical alternatives and cost
observations below explain the architecture decision, not current exclusions.

Round 47 starts at `fbcd9fc5e4490873d71469390679ec5b5812bddf` and is an
architecture audit with executable, test-only prototypes. Production adopted
CREATE/DROP INDEX remains rejected. No README production claim is expanded.

**Choose Candidate A: terminal adopted final-index phase, canonical final
lineage recorded in existing tag24, and hybrid finalization.** Do **not** choose
tag33 for this route. The preferred tag33 hypothesis fails the current decoder's
aggregate-intent prerequisite; relaxing that prerequisite is unnecessary because
tag24 already expresses the complete reservation/loser/no-op/winner history.

The test module is
[`adopted_source_index_finalization_audit_tests.rs`](../crates/netbadb-core/src/adopted_source_index_finalization_audit_tests.rs),
nested under `schema_composition` so the prototype can use private machinery.
Its `FinalIndexPhase` owns a real Transaction and retains the real
`AdoptedSourceTransaction` inside it. Production only extracts the unchanged
source-authority checks into `revalidate_adopted_source_authority`; the new
mid-index-inventory crash hook and entire carrier compile only under `cfg(test)`.
There is no production phase variant or new production routing.

## Exact existing blocker

`materialize_schema_index_composition` already compares TableDef and active
index inventories independently. A dirty table yields RewriteHeap; a clean
table with a genuinely changed inventory yields InPlaceIndexDelta. Index
allocator floors are deliberately excluded from the active-inventory equality
comparison: burning an ID does not make CREATE→DROP an effective index change.

Two independent obstructions explain the Round 45 rename-back result:

1. Round 45 reserved tag24 against provisional overlay Vbase+1. The eventual
   InPlaceIndexDelta names base V/F, and decoder cross-validation rejects
   `Corrupt("IndexId reservation table lineage mismatch")`.
2. Even with a correct reservation, AdoptedSourceBackfill has `source().is_some()`.
   That path requires a target snapshot and exactly one RewriteHeap before it
   can write tag35 and stage S2. An honest index-only intent has neither.

Neither validator is bypassed here. The prototype chooses Ordinary only after
complete adopted authority revalidation and only for a canonical table no-op.
An identity/fake tag25 remains invalid, as pinned by the retained Round 43 test.

## Reservation semantics: tag24 versus tag33

Both encodings carry transaction, TableId, table version, fingerprint, IndexId
and next IndexId. Their legal history and validation are different.

| Property | Composition reservation, tag24 | Migration reservation, tag33 |
| --- | --- | --- |
| allocator authority | `(TableId, IndexId)` contributes to `effective_index` | same |
| may be durable before an aggregate intent | yes | no: decode requires a schema/index/table aggregate |
| may end with reservation-only rollback/no-op | yes | no under that same prerequisite |
| current accepting path | ordinary logical composition, pre-materialization | final migration indexes after an earlier materialization/intent |
| index-only lineage | exact reservation V/F equals InPlaceIndexDelta V/F | not the tag24 cross-check; tag33 is not an alternative strict standalone reservation |
| rewrite lineage authority | exact base/target fragments in RewriteHeap | same fragments, plus staged finalization evidence |
| tag34 linkage | no reservation-specific tag34 rule | when tag34 exists, reservation T/V/high-water must match finalization |
| identity checks | positive T/V/I, exact successor, monotonic per-table I, strict reservation order, intent membership/high-water | established migration path has a different, narrower decoder theorem |

Specifically, `reserve_migration_index` first needs an existing composition
record; `encode` then decodes its result, and the final cross-validator rejects
any record containing migration reservations but no aggregate. Having an ADD
ColumnId reservation does not satisfy the aggregate prerequisite. Rename-only
adoption may not even have a durable composition record yet. A test converts a
valid reservation-only history from tag24 to tag33 and asserts the precise
`migration IndexId reservation without composition` rejection.

This is not a reason to invent an early fake index intent. Even an honest
index intent persisted at the first CREATE cannot later disappear when that
index is dropped: NoEffectiveChange rejects an aggregate intent. CREATE→DROP
must finish without tag25, and DROP-first Round 42 already has a real preceding
index delta whose different `SourceIndexFinalizing` theorem stays intact.

`CompositionIndexReservation.table_version/fingerprint` are **not** a stored
prepared statement and are **not** allocator keys. For tag24 in-place plans
they are durable table-lineage cross-validation evidence; the allocator key is
T/I. In ordinary rewrites they may describe an intermediate overlay, so the
existing decoder does not require each reservation V/F to equal the final
rewrite V/F. Final rewritten schema authority instead resides in the validated
base/target fragments, tag35 and tag34. Similarly, tag33's tag34 check compares
T/V/high-water, not every reservation fingerprint. Do not overstate these
existing checks as a universal reservation-fingerprint theorem.

The new acceptance rule records final V/F even on the rewrite branch. It is
stronger than required by ordinary rewrite reservations, without changing the
meaning or binary interpretation of existing records. Wrong in-place V/F,
wrong T, duplicate I, invalid successor, missing creation reservation,
noncanonical *wire* order and truncated journals are explicitly rejected in
new tests. The encoder sorts reservations, so the ordering test swaps framed
records and recomputes valid outer framing; it tests decoder ordering rather
than merely CRC failure. No decoder is changed or weakened.

## Execute ordering and terminal phase

The Round 48 state should be private `AdoptedSourceIndexFinalizing`, owning the
same adopted carrier and logical final indexes. The test-only equivalent owns
the Transaction and a terminal flag; its schema execution method rejects every
later ALTER and relational statement, while CREATE/DROP continue to operate.
It is not reachable through production SQL.

First accepted index Execute follows this order:

1. Validate transaction and exact **current overlay** prepared T/V/F and
   ColumnId (DROP validates its exact T/I). A stale CREATE fails before any
   reservation, state closure or allocator change, including IF NOT EXISTS.
2. Check normal duplicate name/column, dependency and resource limits.
3. At the final-index boundary, fix canonical final lineage: if final TableDef
   equals base, use Vbase; otherwise Vbase+1. F is the exact final TableDef
   fingerprint. Preserve accepted ColumnId high-water burns.
4. CREATE durably records tag24 with that canonical lineage, then changes only
   logical indexes. DROP changes only the exact logical T/I inventory. Close
   schema refinement permanently after success.
5. Further index DDL uses the now-fixed schema. New names/IDs are visible to
   `index_name_bindings(Some(transaction))` and `logical_index` immediately,
   for duplicate detection, IF NOT EXISTS and subsequent DROP.

The prototype first validates the original prepared object. It then passes an
internal reservation-only copy with canonical V to the existing acceptance
helper; T/F/C are unchanged, and no name is resolved again. Failed acceptance
restores the old provisional version. A successful DROP also canonicalizes the
no-op lineage at phase entry. This is **not** a stale prepared bypass: a CREATE
prepared before an intervening ALTER fails even if the final name exists.
Later statements prepared before the boundary's V normalization can be stale;
there is no promise to revive them.

Parse/prepare, Bind and Describe stay pure. New tests snapshot journal bytes
around prepared CREATE and prepared DROP; retained server/pgwire tests exercise
the existing Parse/Bind/Describe boundary. No new adapter, allocator at prepare
time, or PostgreSQL-specific phase behavior is introduced.

CREATE→DROP leaves the accepted fresh ID burned but removes the logical index.
DROP→CREATE with the same name and ColumnId gets a fresh ID: a DROP prepared for
Iold cannot target Inew. Multiple CREATE accepts I2/I3 with next I4 in the
fixture (I1 already exists) and performs one final physical finalization.

The prototype pins later ADD, DROP COLUMN, rename, SET NOT NULL, SELECT and DML
rejection. DROP NOT NULL shares the same blanket terminal ALTER gate. Logical
indexes cannot be planner access paths while relational execution is closed;
after successful publication a test inspects the real planner's IndexScan.

## Hybrid finalization and frozen source authority

Before *any* physical final index mutation, reuse the complete existing checks:
transaction ownership/state; unchanged catalog incarnation/committed snapshot,
base generation and epoch; exact singleton participant and write-participant
sets `{S1}`; identical P1; exact Single placement; managed Heap kind, TableId,
base V/F, locator and registry schema; and actual source index-definition digest
matching the captured digest. Tests perturb eight independent captured fields
and prove failure before journal or physical index mutation.

| Final truth | Materialization | Placement | ΔV | ΔG | ΔE | Δruntime revision |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| TableDef effective, indexes arbitrary | AdoptedSourceBackfill / RewriteHeap | one S2 | 1 | 1 | 1 | 1 |
| TableDef base, active indexes effective | Ordinary / honest InPlaceIndexDelta | same S1 | 0 | 0 | 0 | 1 |
| TableDef base, active indexes base | SealedNoEffectiveChange | same S1 | 0 | 0 | 0 | 0 |

### Effective table

Use the existing tag25 RewriteHeap, tag35 SourceBackfillIntent,
StageResourceIntent, one transaction-visible RowProjection and complete final
index inventory on S2. Tag34 binds final staged indexes to the final snapshot;
prepared NBSC plus the S1/S2 CORD decision publish once. S1 indexes are not
mutated by final index DDL; its ordinary pre-adoption DML is already inside P1.

Cnew has a fresh ColumnId and CREATE a fresh IndexId. The nullable marker
projection synthesizes exactly three visible NULL values. Direct physical
BTree point lookup of NULL returns IDs 1/3/4 before and after each reopen;
ANALYZE additionally observes
`null_count=3` in the final physical index, queries return IDs 1/3/4, and reopen
validates the index spec. A surviving renamed email retains C3: DROP old I1
then CREATE on `contact` yields fresh I2 on C3 and one S2. Current one-index-per-
column rules still apply; this does not permit two active indexes on C3.

DROP omits the exact index from the S2 final inventory. Multiple final CREATE
still allocates only one target and performs one copy pass. Tag35 and tag34
remain mandatory through the unchanged source/staged materializer.

### Canonical table no-op, effective indexes

After full source revalidation, Ordinary installs one real tag25
InPlaceIndexDelta and uses `transaction.with_write_storage(S1, ...)`. That method
reuses **the existing P1**; it does not open an independent index transaction.
It computes exact drops/creates, advances the physical allocator floor, and
builds new trees through `create_named_index_with_reserved_id_in` against P1's
transaction-visible view. The index therefore sees updated row 1, surviving
row 3 and inserted row 4, excludes deleted row 2 and indexes changed values.
Direct physical BTree point lookups check every expected key and the deleted
key, independently of SQL planner choice.

There is no S2, RowProjection, tag35, StageResourceIntent, tag34, target snapshot,
schema participant reference, or prepared NBSC. The existing materialized index
publication invalidates/publishes runtime index/catalog state once and retains
V/G/E. Ordinary's materialized logical plan owns the same schema writer;
rollback and commit release it using existing cleanup/publication paths.

The source digest is valid **until** the authorized physical delta. Immediately
after physical mutation it is intentionally different. The finalizer consumes
the adopted authority and transitions into materialized index state; it must
never rerun the captured old-digest check after that boundary. A dedicated test
observes this change and successfully commits with the writer released.

### Global canonical no-op

Active inventories are compared without their allocator floors. CREATE→DROP
therefore seals SealedNoEffectiveChange, commits ordinary DML on S1 and resolves
only accepted allocator history. There is no tag25/35/stage/tag34/NBSC or runtime
publication. The tag24 burn remains through three reopens. ADD→DROP column burns
remain similarly independent of schema publication under the existing rules.

## Rollback and durable decision model

The ordinary index-only route uses a **single physical participant S1/P1** and
the existing CORD `commit_schema_decision(..., reference=None)` model. It is not
a two-participant migration and does not need a new adopted-index intent. The
real tag25 plus physical WAL/status and coordinator decision already express
DML and index delta atomically. Before decision, P1 is a loser; after decision,
P1 is committed or completed by recovery. Runtime index truth reconstructs from
the winning Heap/IndexCatalog when reopening; no NBSC version publication is
invented for an index-only winner.

For table rewrites, the existing decision has S1 and S2 plus the schema
reference. Both-prepared, source-first, target-first and both-committed outcomes
retain the existing Round 44 theorem.

| Tested boundary | Participants / evidence | Outcome over three reopens |
| --- | --- | --- |
| adopted-final-index-reservation-durable | P1, tag24 only | base rows/schema/indexes; fresh ID burned; no StorageId burn |
| composition-intent-durable | real tag25, no decision | loser, reservation burns retained |
| adopted-index-delta-first-tree-built | first S1 tree built; second final tree not built | DML and partial index inventory roll back |
| composition-after-index-delta-1 | entire S1 delta applied | loser |
| after-prepare-1 / after-all-prepares | P1 prepared, no decision | loser |
| tag35 / stage-intent / mid-copy | rewrite branch, no decision | loser, allocated stage removed |
| after-durable-decision, index-only | one prepared P1, no schema ref | winner |
| after-commit-1 / after-all-commits, index-only | one committed P1 | winner |
| after-durable-decision, rewrite | prepared P1/P2 | winner on S2 |
| after-commit-1, normal/reversed order | source/target committed first | winner on S2 |
| after-all-commits, rewrite | committed P1/P2 | winner on S2 |

The new core build crash is **between trees in a multiple-index build**, not
inside a BTree row insertion. Existing storage process-crash tests separately
exercise partial-tree/catalog WAL losers and winners, and run in the full
workspace suite. No storage test hook is exposed across production crates.

The rollback matrix runs both before and after materialization for effective
schema+CREATE, no-op schema+CREATE, no-op+DROP, CREATE→DROP, same-name replacement,
and multiple CREATE. It proves base rows/schema/index identity, writer release,
fresh ID burns, unchanged StorageId high-water before S2 allocation, and no
surviving stage. A rollback *after* a real S2 reservation may burn that StorageId;
that is the existing allocator rule, not a surviving target.

Every recovery case uses only durable files. SQL, parsing, prepared objects,
DDL replay, source adoption, IndexId allocation and projection are not replayed.

## Rejected alternatives and physical observations

### B: force S2 for index-only truth

A physical comparison creates a separate same-TableDef Heap using the next
StorageId, scans the exact P1 view once, copies three rows and builds the two
final trees. It is deliberately **not** published or registered as a managed
replacement. A separate valid RewriteHeap history is changed to a same-schema
final fragment with valid framing and fails the unchanged decoder's
`composition table exact identity mismatch` checks: RewriteHeap requires changed F
and Vbase+1. Simply allocating S2 therefore does not provide a legal current
publication path.

Keeping V/G/E unchanged would require a new same-fingerprint physical placement
publication theorem. Advancing V/G/E would abandon canonical no-op semantics,
needlessly invalidate schema dependencies and still need to change the current
same-F rejection. Either costs a full row copy and longer target lifetime.
Candidate A is already executable with unchanged decoding, so reject B.

Fixed fixture observations (bytes include heap/WAL/catalog families; not
performance promises and not strict size assertions):

| Probe | source | target | before finalization | observed pre-commit footprint | source copy passes | rows copied |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| A, ADD Cnew + CREATE | S2 | S3 | 167,651 | 330,978 | 1 | 3 |
| A, renamed column + surviving CREATE | S2 | S3 | 167,606 | 330,913 | 1 | 3 |
| A, rename-back + CREATE | S2 | same S2 | 167,606 | 233,838 | 0 | 0 |
| A, SET→DROP + CREATE | S2 | same S2 | 167,606 | 233,838 | 0 | 0 |
| A, ADD→DROP + CREATE | S2 | same S2 | 167,651 | 233,883 | 0 | 0 |
| B, raw same-schema clone comparison | S2 | unregistered S3 | 167,606 | 337,142 | 1 | 3 |

Here literal StorageIds are 2 and 3 because the fixture has bootstrap seed S1;
conceptual adopted S1 and target S2 refer to these S2/S3 identities. No third
migration generation is allocated. Zero copy passes does not mean zero scans:
the in-place CREATE must scan visible S1 to build each new index. The B footprint
uses standalone physical creation, so it is a cost demonstration rather than a
byte-identical simulated publication protocol. Its V/G/E stay unchanged because
publication was rejected, not because same-F replacement was implemented.

### C: mutate S1 immediately at index Execute

The Cnew ColumnId is absent from S1's physical schema, and actual attempted
physical creation fails. Creating an index on an existing S1 column succeeds
physically but immediately causes `adopted source authority drift`; the captured
index digest is no longer valid. It splits execution into incompatible old/new
layout behaviors, enlarges pre-finalization recovery states and loses the frozen
source theorem. Reject C; the experiment rolls its physical mutation back.

### D: a new adopted-index intent

No additional durable record is needed. If the existing A theorem had failed,
a minimally useful new record would have needed transaction/incarnation,
T/base V/F, S1/P1/locator, base G/E and source index digest, canonical final V/F,
base/final index inventories and their allocator reservation linkage. It would
need explicit loser/winner ordering with the coordinator and idempotent index
recovery. This would add at least one NBSJ tag and a new bounded codec/validator
branch, new malformed/order/cross-record fuzz seeds in `schema_mutation_decode`,
and new recovery decision cases. Old readers reject an unknown tag even without
an envelope-version bump. Changing CORD is not intrinsically necessary, but
cross-validation with it still must be designed. These costs buy nothing over
the proven tag24/tag25/P1 path; reject D without adding a record.

## Exact Round 48 scope

Expose only existing eligible same-table managed Single-Heap
`AdoptedSourceRefining → CREATE/DROP INDEX → AdoptedSourceIndexFinalizing`.
Use tag24 at accepted Execute with the canonical final lineage rule, no new
durable tag. A finalizer must keep ownership/retry/rollback states intact while
consuming the authority proof and selecting the three branches above. Direct
reuse of Ordinary after revalidation is demonstrated; a new persistent mode is
not needed. An optional private helper may name that transition without adding
a format or changing recovery.

Include single-column, non-unique final CREATE on a surviving final ColumnId or
nullable Cnew, exact DROP, multiple final index statements, CREATE→DROP no-op,
and DROP→CREATE fresh identity. Keep normal duplicate/one-index-per-column
rules. Exclude all ALTER after the first accepted index DDL, all relational
execution after refinement, generic index-first→adoption, implicit CASCADE,
constraints, uniqueness, multicolumn indexes, cross-table work and non-Heap.

In particular DROP INDEX→DROP COLUMN within this phase remains disallowed.
Dropping an indexed column may require the established Round 42 DROP-first
route. Do not merge or refactor away its separate SourceIndexFinalizing theorem.

## Compatibility, regression and validation

No NBSJ tag/version, CORD version, Canonical Schema, NBSC/NBSM, Heap/Page/WAL,
BTree, IndexCatalog, protocol, PostgreSQL framing, manifest, SDK or inspection
format changed. No decoder, recovery dispatcher, parser or server production
code changed. Production Native SQL rejects both adopted CREATE and DROP.
The real PostgreSQL probe checks initial `25000`, subsequent `25P02`, rollback
of own DML and the base index/schema inventory over three catalog-only opens.
The retained Round 39/42/44/46 suites and Round 46 Cnew-nullability negatives
remain authoritative and unchanged.

Commands executed on the pinned Rust 1.97.1 toolchain unless stated otherwise:

| Validation | Result |
| --- | --- |
| `cargo fmt --all -- --check` | passed |
| `cargo check --workspace --all-targets --all-features` | passed |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | passed |
| `cargo test -p netbadb-core adopted_source_index_finalization_audit_tests --offline -- --nocapture` | 14 prototype tests passed (including child harness and parameterized matrices) |
| `cargo test --workspace --all-features --offline` | passed; includes retained Round 39/42/44/46 and storage partial-tree crash regressions |
| `cargo +1.85.0 check --workspace --all-targets --all-features --offline` | blocked by existing E0658 let-chains in planner `lib.rs:892` and `:904`; planner untouched |
| `cd sdk/go && go test ./...` | passed |
| `./scripts/check-generated-sdk.sh` | passed |
| `test-drop-first-migration-sql.py` | real PostgreSQL 17.11 Round 39/42 passed |
| `test-post-dml-source-adoption-sql.py` | real PostgreSQL 17.11 Round 44 and retained Round 45 negatives passed |
| `test-post-dml-source-nullability-sql.py` | real PostgreSQL 17.11 Round 46, including Cnew negatives and extended SET, passed |
| `test-adopted-source-index-finalization-audit.py` | real PostgreSQL 17.11 Round 47 CREATE/DROP negatives passed |
| all 13 existing fuzz targets, `cargo +nightly fuzz run TARGET TEMP_CORPUS -- -runs=1000 -seed=47 -artifact_prefix=TEMP_ARTIFACTS/` | all passed, copied corpora and artifacts removed |
| `git diff --check` and relative documentation links | passed |

The psql scripts used `CARGO_TARGET_DIR` pointing at this checkout's `target`,
`/opt/local/lib/pgsql/bin/psql`, and `DYLD_LIBRARY_PATH=/opt/local/lib/icu/lib`.
The initial sandboxed Round 47 fixture could not bind `127.0.0.1:0` (`EPERM`);
the permitted local-listener rerun passed. The failed fixture was removed.
No diagnostic feature-gate override was used for the MSRV check.

Fuzz targets were wal_recovery, page_decode, btree_decode,
index_catalog_decode, protocol_decode, pgwire_decode, coordinator_log_decode,
partition_catalog_decode, lsm_manifest_decode, lsm_wal_decode,
lsm_sstable_decode, schema_catalog_decode and schema_mutation_decode. Tracked
corpora were never mutated. No new target was added because no decoder changed.
