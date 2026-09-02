# Schema transaction composition architecture audit — Round 27

Round 27 is the historical audit, experiment, and design record. It did **not**
implement a production multi-mutation schema transaction API. Round 28 now
implements its ALTER-only Core choice; see
[Core multi-ALTER composition](core-multi-alter-round28.md). Statements below
about the then-current one-mutation behavior are retained as audit evidence.

The chosen future model is one aggregate schema transaction:

```text
committed schema G
  + ordered transaction-local logical actions
  = one final target schema G+1
```

Intermediate overlays are neither committed generations nor recoverable catalog
states. Each changed existing table has one base-to-final physical transition, the
database has one prepared NBSC, and one Coordinator decision commits the complete
participant set.

## 1. Scope and evidence

The audit traced `DatabaseTransaction`, schema-writer admission, `SchemaMutation`,
the transaction SchemaView, staged bindings, prepared dependencies, CREATE/DROP/
rewrite/index state, NBSJ replay, prepared NBSC publication, CORD v2, commit,
rollback, and open recovery. Tests added in this round are test-only observations;
the real-client script treats current multi-DDL failure as the expected result.

Round 28 should implement only the Core Multi-ALTER foundation described in
section 36. CREATE/DROP TABLE and index DDL composition remain later phases.

The final regression evidence passed workspace format, all-target/all-feature
check, Clippy with `-D warnings`, the complete workspace test suite, generated-SDK
`--check`, Go unit and live Protocol v1 integration, and the psql/psycopg/
SQLAlchemy/Alembic probe. All 13 existing decoder/recovery fuzz targets completed
1,000 runs with seed 27. The Rust 1.85 workspace check remains blocked only by the
pre-existing planner let-chain expressions at `netbadb-planner/src/lib.rs:892`
and `:904` (`E0658`); Round 27 does not own or alter them.

## 2. Current transaction model

`DatabaseTransaction` currently contains:

```text
participants: BTreeMap<StorageId, StorageParticipant>
write_participants: BTreeSet<StorageId>
schema_mutation: Option<SchemaMutation>
pending_indexes: Vec<(StorageId, IndexDefinition)>
pending_index_drops: Vec<(StorageId, IndexId)>
preparation_scope: Rc<()>
```

The current schema state is therefore exactly:

```text
None
  -> one CREATE mutation
  or one DROP mutation
  or one Heap rewrite mutation
```

`SchemaMutation` itself owns one reservation, one target snapshot, one schema
reference, at most one staged Heap, and mutually exclusive optional DROP/rewrite
intent. It cannot describe several rewritten tables.

`rewrite_heap_table_schema_in` requires `is_pristine_for_schema_rewrite()`:
the transaction must be Active, have no participants or writers, and have no schema
or index mutation. A direct second ALTER consequently returns
`TransactionNotPristine`. The combined SQL preparation/execution path rejects a
second non-CREATE DDL even earlier as `UnsupportedDdlCombination`. This is a Core
composition boundary, not a frontend autocommit policy.

The current transaction-local SchemaView is already useful but singular. It routes
the one altered TableId to the one staged StorageId and uses the target NBSC schema.
It cannot chain a second target or represent different tables.

## 3. Current physical cost before commit

The first current ALTER already does all transaction-final physical work before it
returns success:

1. validates the committed `(TableId, TableSchemaVersion, fingerprint)`;
2. reserves and synchronizes one fresh StorageId and, for ADD, one ColumnId;
3. writes a one-operation NBSJ rewrite intent;
4. creates the staged Heap and owner evidence;
5. recreates every active logical index;
6. scans the complete committed source and inserts transformed rows;
7. exposes the staged Heap through the private overlay.

The Round 24 fixture measured one illustrative two-row indexed rewrite as:

```text
old retained bundle: 120,448 bytes
new target bundle:   103,184 bytes
both retained:       223,632 bytes
```

Those numbers are a fixture observation, not a benchmark. Five naively separate
same-table ALTER commits conceptually allocate five StorageIds, perform five full
rewrites, create five Coordinator decisions, and retire five predecessors. At the
observed target size that is roughly `5 * 103,184 = 515,920` target bytes before
WAL/history overhead, versus one 103,184-byte final target under composition.
A 100-statement same-table migration similarly means 100 copies/decisions under
per-statement commits versus one copy/decision under composition: a conceptual 99%
reduction in those counts, not a throughput or exact disk claim.

## 4. Alembic and SQL client evidence

The real fixture uses unmodified psql 17.11, psycopg 3.2.13, SQLAlchemy 2.0.52,
and Alembic 1.16.5.

psql, psycopg, and SQLAlchemy each execute this shape in one explicit transaction:

```sql
ALTER TABLE projects ADD COLUMN blocked BOOLEAN;
ALTER TABLE projects RENAME COLUMN name TO blocked_name;
```

The first statement succeeds privately. The second fails with SQLSTATE `0A000`.
The explicit transaction is failed and rollback restores the base schema. A later
query resolves `name`, proving the first ALTER was not committed.

Alembic's normal Operations API, inside one `engine.begin()`, generated:

```sql
ALTER TABLE projects ADD COLUMN blocked BOOLEAN;
ALTER TABLE projects RENAME name TO display_name;
ALTER TABLE projects ALTER COLUMN display_name SET NOT NULL;
CREATE INDEX projects_blocked_idx ON projects (blocked);
```

SQLAlchemy event evidence is one `BEGIN`, then the first two statements, then one
`ROLLBACK`; the second statement is the `0A000` blocker. No DML or connection commit
appears between operations. The existing six individually committed Alembic ALTER
operations still pass, but they are not an atomic migration. The proposed Core
composition makes the first three ALTERs work without a frontend change; CREATE
INDEX remains deferred to the index-composition phase.

## 5. Schema writer and source stability

The current stability guarantee is concrete:

- every transaction handle owns one strong reference to `transaction_owner`;
- the first ALTER admits only when its count is exactly two: Database plus the
  requesting transaction, so no retained transaction handle exists;
- admission stores the requesting DatabaseTxnId in `schema_writer`;
- every new transaction and every autocommit read/write calls
  `ensure_schema_available(None)` and receives `SchemaBusy` while that writer exists;
- only the owning transaction passes `ensure_schema_available(Some(id))`;
- commit, rollback, or recovery releases the writer after terminal cleanup/
  publication.

The server routes all sessions through the same mutable Core Database and cannot
bypass those entry points. Thus the committed source rows cannot change during the
whole admitted schema transaction. The guarantee is process-local; NetbaDB does not
support another process concurrently opening and mutating the same files. Round 28
must retain the writer for the entire composition, materialization, and completion
lifecycle. This is an offline serial migration, not online schema evolution.

## 6. Chosen composition state machine

The production target is:

```text
NoSchemaMutation
    | first admitted schema statement; acquire writer
    v
Composing
    | more validated logical ALTER actions
    | (remains Composing)
    |
    | first executed user physical statement OR COMMIT
    v
SealingAndMaterializing
    | freeze final overlay; reserve final StorageIds; write aggregate intent
    | create/copy/prepare every final target Heap; prepare one NBSC
    v
Prepared
    | synchronize one CORD schema COMMIT decision
    v
CommitDecided
    | finish every participant, promote targets, explain every predecessor,
    | publish NBSC, complete CORD, resolve NBSJ, publish memory bundle
    v
Committed
```

Before the Coordinator decision, rollback discards the entire overlay and all exact
staging artifacts. A statement error in PostgreSQL puts the transaction in failed
state and eventual ROLLBACK discards earlier successful actions. Native Core APIs
may leave the transaction Active after a clean validation error, so each action is
applied atomically: construct and validate a candidate overlay first, synchronize
any required identity reservation, then replace the in-memory overlay in one
infallible move. An uncertain durable reservation or materialization error marks
the transaction RollbackRequired.

After any materialization begins, the whole schema transaction is sealed. No table
or index mutation is accepted anywhere in that transaction. Per-table sealing is
rejected for the first version because it creates order-dependent participant and
validation rules.

## 7. SchemaGeneration semantics

One effective committed database transaction is one externally observable schema
transition:

```text
SchemaGeneration G -> G+1 exactly once
```

It does not matter whether the transaction contains one or 128 logical actions or
touches one or many tables. Intermediate overlays have no SchemaGeneration. The
target generation is assigned only to the frozen final snapshot. A net-no-op
schema transaction does not advance SchemaGeneration.

## 8. TableSchemaVersion semantics

For every existing TableId whose final canonical `TableDef` differs from its base:

```text
TableSchemaVersion V -> V+1 exactly once per committed transaction
```

Two ALTERs of `users` and one ALTER of `teams` produce `users V+1`, `teams W+1`,
and schema G+1. There are no durable `V+1a`, `V+2`, or `V+3` states. A table whose
final definition equals its base does not advance version.

## 9. Provisional version and fingerprint

The first logical change to an existing table gives its transaction overlay the
provisional version `committed V+1`. Every later action on that table keeps `V+1`
but recomputes the canonical fingerprint from the new provisional TableDef.

The test-only experiment proves:

```text
base:                    V1 / F1
rename name -> title:    V2 / Fa
prepare SELECT title:    dependency V2 / Fa
rename title -> name:    V2 / F1
```

The statement prepared at `V2/Fa` is stale at `V2/F1` even though the version did
not change again. A global statement prepared before the transaction at `V1/F1` is
also stale at `V2/F1`; the rename cycle cannot resurrect it. Relational transaction
preparation already reads target lineage. Future composed DDL preparation must be
changed to do the same; the current DDL identity-binding helper consults committed
lineage because a second DDL is rejected today.

Preparation/description alone does not materialize or seal. Execution revalidates
the exact provisional dependency.

## 10. Net-no-op policy

Choose ordinary no-op commit (option C). If the final canonical logical schema
equals the base, materialization is skipped, no NBSC or Coordinator decision is
created, and schema generation, table versions, NBSC epoch, and runtime revision do
not change. The SQL transaction may still commit successfully.

Already synchronized identity reservations remain consumed. For example, ADD C7
then DROP C7 leaves the base TableDef active but `next_column_id > C7`, represented
by retained reservation evidence until a later NBSC checkpoints the floor. NBSJ
needs a distinct terminal `NoEffectiveChange` outcome so this is neither a winner
publication nor a loser that implies IDs may be reused.

## 11. Identity allocation rules

| Identity | Multiple mutations in one transaction |
| --- | --- |
| TableId | Existing ALTER keeps it. Future CREATE reserves once at statement execution; never reused, even CREATE then DROP. |
| ColumnId | ADD reserves and synchronizes at statement execution because later statements bind it. ADD a=C7, ADD b=C8, DROP a leaves b=C8 and floor C9. Never compact. |
| StorageId | Reserve only at seal/materialization, exactly one for each final dirty existing table. Five ALTERs of one table consume one replacement ID. Net-no-op and final DROP consume none for rewrite. |
| IndexId | Future CREATE INDEX reserves at statement execution and never reuses it, even CREATE then DROP. Round 28 does not compose index DDL. |
| TableSchemaVersion | Provisional and committed `V+1` at most once for each effectively changed TableId. |
| SchemaGeneration | Effective transaction `G+1` once; net-no-op unchanged. |
| Runtime catalog revision | Increment once after the complete in-memory publication; net-no-op unchanged. |
| NBSC epoch | One final snapshot `E+1`; net-no-op unchanged. |

ColumnId/IndexId are logical object identities needed by subsequent statement
binding. StorageId identifies a physical incarnation that does not exist until the
final physical plan is frozen. The asymmetry is intentional.

## 12. Logical overlay composition

The future `SchemaTransactionPlan` is an aggregate owned by
`DatabaseTransaction`, conceptually containing:

```text
base_schema_generation and base_epoch
base committed Schema
current canonical logical overlay
name -> TableId and TableId -> provisional TableDef lookup
per touched table:
    base TableDef + base lineage/placement/index snapshot
    current provisional TableDef at V+1
    logical reservations and allocator floors
    physical_dirty
    optional final StorageId/staged participant after seal
bounded ordered diagnostic metadata/action digest
phase: Composing | Sealed/Materializing | Prepared | Decided
```

Each statement resolves against the immediately preceding overlay, constructs a
candidate complete schema, applies all incremental checks, then runs canonical
`Schema::new` validation before publication to the transaction view.

Consequences are sequential and identity-based:

- `ADD c; RENAME c TO d; SET d NOT NULL; DROP d` binds one new ColumnId through
  every step and ends without that column, while the ID remains consumed;
- `RENAME TABLE projects TO work; ALTER TABLE work ...` finds the same TableId by
  its new overlay name;
- `RENAME COLUMN name TO title; ALTER COLUMN title ...` retains one ColumnId;
- DROP then reference returns table/column not found from the overlay;
- rename to another active table name fails immediately;
- a failed action never partially changes either name map.

The final frozen schema is canonically revalidated even though each action was
incrementally checked.

## 13. Materialization and sealing

The first implementation materializes **all** dirty target tables, in stable
TableId order, when either:

1. the first user query or DML statement is executed after schema mutations; or
2. the transaction is committed directly after DDL.

Preparing a query, preparing another ALTER, inspecting metadata required for
binding, or performing an internal schema-validation scan does not trigger it.
Materializing all tables rather than only the accessed table gives one immutable
final schema, participant set, digest, and failure boundary. StorageIds are then
reserved in the same stable order; CORD independently canonicalizes participants
by `(StorageId, physical TxnId)`.

Once the trigger occurs, the transaction is globally sealed. Any later schema or
index mutation returns `SchemaMutationAfterMaterialization` (future typed error).

## 14. User read and DML interaction

The Round 28 policy is:

| Sequence | Result |
| --- | --- |
| user read/DML before first ALTER | ALTER rejected by the retained pristine rule |
| ALTER; ALTER; ... | allowed while Composing |
| ALTER; prepare SELECT/INSERT; ALTER | allowed; preparation is logical only and the prepared dependency may stale |
| ALTER; execute SELECT/DML | materialize all targets, seal, then execute against private targets |
| ALTER; execute SELECT/DML; ALTER | last ALTER rejected because the transaction is sealed |
| ALTER...; final DML; COMMIT | supported; target DML joins the same physical participants |

This preserves Round 24's post-ALTER DML while keeping migration composition
DDL-only until all schema changes are declared. There is no implicit frontend
commit and no DML-between-DDL support in the first version.

## 15. Statement-time validation

Deferring physical creation must not defer normal statement errors to COMMIT.
Every action performs at Execute:

- overlay table/column lookup and duplicate-name checks;
- exact provisional `(TableId, version, fingerprint)` revalidation;
- primary-key and active-index dependency checks;
- type/placement support checks;
- checked identity allocation and capacity checks;
- full candidate canonical schema validation.

`SET NOT NULL` is data-dependent. The schema writer proves a stable source, so the
statement performs an internal validation scan without creating a target Heap or
marking user data observed. For a surviving base ColumnId it maps visible base rows
through the composed transform and rejects the first resulting NULL. A column
added earlier as nullable maps every existing row to NULL, so SET NOT NULL fails
immediately if the relation has any visible row and succeeds for an empty relation.
Repeated validations may rescan in the first version; batching is an optimization.

Before materialization, Core repeats final canonical, placement, index dependency,
and previously established data-validation checks. The repeated check is defense in
depth; the retained writer means external data/schema/index state cannot change.

## 16. Same-table final physical transform

For each final dirty existing Heap, derive one typed transform from the committed
base TableDef and final provisional TableDef:

```text
source S1 rows under base TableDef
  -> map final columns by stable ColumnId
     surviving ID: source value
     newly added ID: explicit database NULL
     dropped ID: absent
  -> target S2 rows under final TableDef
```

Intermediate names, orderings, and nullability states do not produce row copies.
The target rebuilds the final active index inventory once and resets physical
statistics once. The source stays read-only. A five-ALTER table has exactly
`S1 -> S2`, not `S1 -> S2 -> S3 -> S4 -> S5 -> S6`.

If an intermediate staged Heap is ever introduced by a later implementation, it is
a transaction-private loser artifact. It is never a committed incarnation and
must not appear in replacement-retirement lineage.

## 17. Cross-table physical plan

For every existing runtime-created Single Heap whose final TableDef differs:

1. reserve one StorageId;
2. create one target Heap under the final schema;
3. stream the stable old Heap directly into that target;
4. build the final index inventory;
5. enlist and prepare the target as one physical participant.

Tables are copied sequentially in stable TableId order to bound memory; atomicity
does not require atomic pre-decision copying. Failure while copying table B after A
was staged makes every target a loser. Round 28 supports same-table and different-
table ALTER composition for runtime-created Single Heaps only.

## 18. One final prepared NBSC

After all actions are frozen, Core applies the aggregate overlay to the one base
NBSC and generates exactly one complete target snapshot at `G+1/E+1`. It contains
all table definitions, versions, allocator floors, placements, and final StorageId
bindings. Its bytes are synchronized once and its SHA-256 is referenced by both the
aggregate NBSJ intent and CORD v2.

NBSC remains the only committed logical authority. NBSJ actions or digests are
recovery evidence and never rebuild the committed schema by replaying SQL.

## 19. One Coordinator decision

CORD v2 already supports 0..1024 schema-decision participants and canonicalizes
them independent of caller order. `DatabaseTransaction` already prepares and
commits a `BTreeSet<StorageId>` participant set. No CORD format change is needed.

The composed decision contains all final target Heaps (and post-seal DML
participants, which are normally those same targets) plus one schema reference.
All targets and the prepared NBSC are durable before the one decision. The decision
commits the complete transaction; no per-table or per-statement decision exists.

## 20. Retirement lineage and GC

After the durable decision, each replaced **committed** predecessor receives exact
replacement-retired evidence:

```text
(TableId, V, F, old StorageId, locator)
  -> (same TableId, V+1, final F, new StorageId)
```

All predecessor retirements must be durable before final NBSC publication. The
schema transaction cannot reach Complete while any old committed storage is
unexplained. A future composed DROP uses distinct drop-retired evidence. Only a
committed physical incarnation may enter either lineage.

Existing explicit GC remains per retired resource and uses the completed
Coordinator horizon. Composition does not add automatic/background GC and does not
compact NBSJ/CORD history.

## 21. Rollback and loser cleanup

Before the decision, rollback enumerates only the aggregate durable plan and:

- aborts every enlisted/prepared target physical transaction;
- closes target handles;
- removes every exact owner/Heap/WAL/status/index staging component;
- removes the one prepared NBSC if present;
- records one terminal loser (or no-effective-change) for the schema transaction;
- discards the logical overlay and releases the writer.

It never scans the filesystem for candidates. ColumnId/IndexId reservations and
StorageIds actually reserved at materialization remain burned. If rollback occurs
while still purely Composing and no logical identity was reserved, no physical or
journal artifact exists.

## 22. Winner recovery

Winner recovery never reparses SQL and never reruns logical actions or row copies.
It uses only:

```text
aggregate durable final physical plan
prepared physical participant state
prepared NBSC and digest
one Coordinator decision
retirement progress
```

For three targets where two promotions completed before a crash, reopen validates
the decision and prepared NBSC, finishes the third participant/promotion, validates
all final Heaps, records every missing predecessor retirement, publishes the one
NBSC, completes CORD, resolves the aggregate winner, removes prepared artifacts,
and constructs one in-memory publication bundle. No intermediate schema can reopen.

## 23. Atomic in-memory publication

After durable publication, one synchronous ownership move replaces:

```text
Schema
all affected PhysicalBindings
all affected StorageRegistry entries
runtime catalog revision
```

No callback or session observes a subset. Other sessions see the complete base
schema before publication and the complete final schema afterward. Global prepared
statements for any changed table become stale together; unrelated dependencies
remain valid.

## 24. Prepared statement semantics

- preparation after an overlay mutation binds the provisional V+1/fingerprint;
- a later mutation of that table stales it through fingerprint even though V+1 is
  unchanged;
- preparation alone does not materialize;
- execution after the final mutation validates against the final overlay, then
  triggers global materialization/sealing if physical access is needed;
- global pre-transaction statements remain usable by other sessions against the
  committed base until publication, then stale for every changed target table;
- unrelated-table prepared statements remain valid;
- prepared ALTER must bind the exact current overlay identity and never re-resolve
  a name at Execute;
- transaction-scoped prepared objects remain invalid after commit/rollback.

Savepoint rollback of prepared dependencies is deferred with schema savepoints.

## 25. Index DDL interaction

Current IndexCatalog v9 is physical Heap authority. It stores active/retired
definitions, stable IndexId/name/ColumnId, BTree handles, and `next_index_id`.
Current transactional CREATE/DROP INDEX stages physical catalog mutations and
publishes active runtime metadata at commit; create/drop mixing is explicitly
blocked and there is no logical index overlay. A current ALTER snapshots indexes
from the source Heap and rejects indexed-column DROP.

Round 28 therefore excludes index DDL. Existing active indexes are final truth,
remain bound by ColumnId across renames, and are rebuilt once. DROP of an indexed
column remains rejected.

Round 29 should add a transaction-local logical index overlay and durable IndexId
reservations. Then `DROP INDEX idx_x; DROP COLUMN x` succeeds sequentially because
the dependency is absent, while the reverse order fails at the first statement.
`ADD x; CREATE INDEX idx_x ON x` creates the index directly in the final target.
CREATE then DROP consumes IndexId but produces no final index.

## 26. CREATE/DROP TABLE interaction

Round 28 excludes CREATE/DROP mixing because their identity and absence semantics
are distinct:

- CREATE then ALTER should eventually mutate the uncommitted TableDef and create
  one final Heap without a rewrite;
- ALTER then DROP should skip replacement creation and directly drop-retire the
  committed source;
- DROP then ALTER is table-not-found in the overlay;
- CREATE then DROP is a net no-op with consumed TableId and any assigned ColumnIds/
  StorageId; if no physical access occurred, full Heap creation can be avoided;
- DROP then CREATE with the same name is valid future sequential semantics, but the
  final table has a new TableId and is not a net no-op.

These belong after the ALTER and index foundations, not in the first composition
implementation.

## 27. Operation composition matrix

| Sequence | Semantically valid? | First implementation? | Materialization strategy |
| --- | --- | --- | --- |
| ALTER T + ALTER T | Yes | Yes | One base-to-final rewrite for T |
| ALTER A + ALTER B | Yes | Yes | One rewrite per final dirty table |
| ALTER + SELECT + ALTER | Valid in a fuller model | No; final ALTER rejected | SELECT materializes all and seals |
| ALTER + DML + ALTER | Valid in a fuller model | No; final ALTER rejected | DML materializes all and seals |
| ALTER + post-final DML | Yes | Yes | Materialize all, seal, then DML on targets |
| ALTER + CREATE INDEX | Yes | No, Round 29 | Future final target directly includes index |
| DROP INDEX + DROP COLUMN | Yes in this order | No, Round 29 | Future logical index removal then one rewrite |
| CREATE TABLE + ALTER new table | Yes | No, later table-composition round | Build one final new Heap, no rewrite |
| ALTER + DROP TABLE | Yes | No, later table-composition round | Elide rewrite; drop-retire committed source |
| CREATE + DROP same transaction | Yes, net no-op | No, later table-composition round | Avoid physical creation; IDs remain consumed |

## 28. Durable artifact matrix

| Artifact | Per statement? | Per table? | Per transaction? |
| --- | --- | --- | --- |
| ColumnId reservation | One for each accepted ADD | Table-scoped floor | Owned by one schema transaction |
| StorageId reservation | No | One per final materialized dirty table | Batched in aggregate intent |
| Mutation intent | No SQL/action intent per statement | Contains sorted table plans | Exactly one aggregate change-set intent |
| Staged Heap | No | One per final dirty/created physical table | Enumerated by aggregate plan |
| Prepared NBSC | No | No | Exactly one |
| Coordinator decision | No | No | Exactly one with all participants |
| Replacement retirement | No | One per replaced committed predecessor | All required before schema publication |
| Winner/loser/no-change terminal | No | No | Exactly one aggregate terminal outcome |

## 29. Durable intent design

Current NBSJ tags 1-15 are exact: CREATE reservation/intent/loser/winner are
1/2/3/4; DROP intent/retained/loser/winner are 5/6/7/8; retirement GC intent/
complete are 9/10; and rewrite reservation/intent/replacement-retained/loser/
winner are 11/12/13/14/15. Replay tracks one `current` transaction, rejects
overlapping unresolved schema transactions, maps only one rewrite reservation/
intent per DatabaseTxnId, and expects one retirement/terminal sequence. Writing
several current rewrite intents with the same transaction ID would be corrupt.

The future journal should retain its role as recovery evidence and use this bounded
aggregate model:

```text
SchemaLogicalIdentityReservation { txn, base G/E, table, ColumnId or IndexId }
    // synchronized only when an action allocates a logical identity

SchemaChangeSetIntent {                         // exactly one at seal
    schema_txn_id
    base_generation, target_generation
    base_epoch, target_epoch
    prepared_nbsc_digest
    ordered_action_digest                       // diagnostics/corruption binding
    table_plans[] sorted by TableId {
        TableId
        base version/fingerprint/StorageId/locator
        target version/fingerprint/StorageId/locator
        ColumnId-based target-source-or-NULL transform
        final active index identity digest
    }
}

SchemaResourceRetired { txn, old StorageId, retirement_kind } // one per source
SchemaTxnResolved { txn, NoEffectiveChange | Loser | Winner }  // exactly one
```

This chooses option C: persist an action digest plus the complete typed final
physical plan, not ordered SQL actions. The prepared NBSC already contains the full
final schema, so NBSJ must not duplicate the complete database snapshot. Loser
cleanup needs exact resource identities; winner recovery gets target TableDefs from
the digest-verified prepared NBSC. The transform is persisted for plan auditing and
corruption checks but is never executed by winner recovery.

Only logical identity reservations need durability during Composing. An early crash
with no reservation and no physical materialization is an ordinary transaction
loser with nothing to reconstruct. At seal, StorageId reservations and the complete
change-set intent are synchronized atomically before the first staging file.

## 30. Bounds

Existing hard limits are NBSC 16 MiB, 4,096 tables, 65,536 total columns and
65,536 storages; NBSJ 16 MiB/65,536 records; CORD 1,024 participants. The first
composition version should impose stricter per-transaction limits:

```text
schema actions                 <= 128
distinct touched tables        <= 64
new ColumnId reservations      <= 128
new IndexId reservations       = 0 in Round 28; <= 128 in Round 29
new StorageId reservations     <= 64
physical participants          <= 64
encoded aggregate intent       <= 4 MiB
final NBSC                     <= existing 16 MiB
```

Sixty-four participants leave a 16x margin under CORD's 1,024 hard bound; 4 MiB
leaves capacity for terminal records under the 16 MiB NBSJ bound; 128 actions cap
validation and diagnostics independently of final NBSC size. Admission checks the
remaining journal budget before accepting each action and reserves room for the
eventual intent, all retirements, and one terminal record. Exhaustion is a typed
statement error, never a partially durable unfinishable winner.

## 31. Crash design for Round 28

Tests must cover loser/winner recovery at least after:

```text
logical ColumnId reservation 1
logical ColumnId reservation N
aggregate change-set intent
first staged target creation
mid-copy table A
table A prepared
mid-copy table B
all targets prepared
prepared NBSC sync
Coordinator decision sync
participant A completion/promotion
participant B completion/promotion
retirement A
retirement B
NBSC publication
Coordinator Complete
aggregate winner resolution
in-memory publication
```

Every pre-decision window reopens the exact base schema and removes all targets.
Every post-decision window converges to the complete final schema. Tests must reopen
at least three times, vary participant ordering, and prove no partial table version,
binding, retirement, or metadata publication.

## 32. Current NBSJ and CORD capability audit

NBSJ's current tags are CREATE reserve/intent/loser/winner, DROP intent/retained/
loser/winner, retirement GC intent/complete, and Heap rewrite reserve/intent/
replacement-retained/loser/winner. They assume one mutually exclusive mutation
kind and, for rewrite, one table under one DatabaseTxnId. A future aggregate model
is required; repeated current intents are not safe.

CORD v2 is already composition-ready. Its decision binds one DatabaseTxnId, up to
1,024 unique physical participants sorted by stable physical identity, and one
prepared NBSC `(incarnation, epoch, digest)` reference. Existing multi-storage
transaction tests prove partial participant completion recovery. Round 28 must
extend schema recovery/retirement over that capability, not change CORD.

`SchemaTxnId` should be exactly the existing DatabaseTxnId. One transaction ID owns
many logical reservations/actions and many physical participants. A second schema
transaction cannot overlap because the schema writer remains exclusive.

## 33. Compatibility and authorization

NBSC, Heap/row/index formats, Protocol v1, generated Rust/Go SDK identities, grants,
and PG framing need no semantic change for composition. After commit, required
fingerprints for every changed table mismatch exactly as they do for one ALTER.
TableId-based grants survive ALTER; create/drop grant behavior stays unchanged.

Every schema statement is authorized independently. A successful first
`schema_admin` check does not authorize later ALTER/index/table operations. Other
sessions see the base schema until the atomic publication. One composed commit
causes one runtime revision refresh event. No hidden autocommit is permitted.

## 34. Persistent formats in Round 27

All remain unchanged: NBSC v1, NBSM v1, NBSJ v1 tags 1-15, Heap metadata v5,
NBMV v1, Page v5, IndexCatalog v9, BTree v1/v2/v3, WAL/status, CORD v2,
PartitionCatalog v1, LSM formats, Protocol v1, and PostgreSQL framing. Round 27
adds no decoder tag, corpus, compatibility layer, or production schema state.

The future aggregate intent is expected to be the only persistent-format addition
needed for Round 28. Its exact byte layout and downgrade behavior require the normal
persistent-format review; the conceptual record above is not a Round 27 byte claim.

## 35. Deferred work and debt

Explicitly deferred:

- savepoints and ROLLBACK TO SAVEPOINT for schema overlays;
- online/concurrent composition or cross-process writer exclusion;
- user read/DML between schema mutations;
- physical type conversion, defaults, backfill expressions, ADD NOT NULL syntax,
  generated/constraint semantics, and column reorder;
- CREATE/DROP TABLE mixing;
- CREATE/DROP INDEX mixing and its logical overlay;
- LSM, range partition, imported/bootstrap storage, and multi-placement rewrite;
- background/automatic retirement GC;
- NBSJ and CORD checkpoint/compaction.

Composition reduces decisions and physical transitions but does not solve append-
only metadata growth. A migration adds bounded logical-reservation/change-set bytes
to NBSJ and one CORD decision rather than N per-statement decisions. Compaction is a
separate correctness project and must not be bundled into Round 28.

## 36. Exact Round 28 recommendation

Proceed with **Round 28 — Core Multi-ALTER Schema Transaction Composition
Foundation**:

```text
runtime-created Single Heap only
multiple ALTER operations on the same and different tables
DDL-only composition until the global seal trigger
no CREATE/DROP TABLE mixing
no index DDL mixing
no user read/DML between ALTER statements
post-final ALTER read/DML allowed after materialization and seal
one base-to-final rewrite per effectively changed table
one prepared NBSC
one Coordinator decision
one SchemaGeneration/NBSC epoch/runtime-revision increment
one TableSchemaVersion increment per effectively changed TableId
```

The motivating transaction therefore commits as:

```text
SchemaGeneration G -> G+1

projects: same TableId P, V -> V+1, P1 -> exactly one P2
teams:    same TableId T, W -> W+1, T1 -> exactly one T2

one final prepared NBSC
one CORD COMMIT decision for P2 and T2
P1 and T1 both replacement-retired before NBSC publication
no durable intermediate table version, fingerprint, or Heap
```

Round 29 should then implement Schema + Index DDL Composition. CREATE/DROP TABLE
composition follows separately. The source-stability audit does not require a
writer/admission-hardening detour before Round 28.
