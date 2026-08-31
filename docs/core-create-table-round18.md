# Core transactional Heap creation — Round 18

## Baseline architecture audit (64d06a3)

This audit follows the current implementation, not assumptions from Round 16.

1. `DatabaseTransaction::ensure_participant` lazily enlists a physical
   `StorageTransaction` from `StorageRegistry`. The BTreeMap is keyed by persistent
   StorageId; Read can upgrade to Write. A private storage needs an explicit
   enlistment path because the baseline only looks in the committed registry.
2. Coordinator decisions identify `(DatabaseTxnId, StorageId, physical TxnId)`.
   DatabaseTxnId and storage-local TxnId are separate identities. Multiple writers
   prepare with the same database identity; these are genuine 2PC participants.
3. `write_participants` is dynamic. Existing Heap/LSM/partition participants may
   precede a newly staged Heap without changing the participant protocol.
4. Baseline recovery inspects every configured physical path, validates persisted
   StorageIds and prepared WAL identities against coordinator decisions, then
   opens with explicit commit/abort resolutions. Missing winner participants fail.
   Baseline inventory comes from NBSC, so it cannot locate an unpublished Heap.
5. Prepared participants without a decision are presumed aborted. Unprepared Heap
   WAL losers are undone by normal recovery. There is no staged-resource cleanup
   inventory in the baseline; a durable creation intent must supply it.
6. Decision append/sync errors retain DecisionPending; rollback is forbidden even
   if sync success is unknown. Commit retries synchronize the identical decision.
   ApplyingCommit and FinalizePending are likewise retry-only. Dropped dirty or
   pending physical handles force recovery; dropping a handle is not rollback.
7. NBCO file v1 / CORD record v1 describes storage participants only. Complete is
   written after their commits; it cannot represent catalog preparation or delay
   completion until physical promotion and schema publication. A versioned schema
   decision reference and a delayed completion boundary are required.
8. Heap creation initializes the heap file (metadata, index root, first data
   page), WAL, and transaction-status sidecar. WAL has an optional alternate
   generation. Heap metadata binds TableId, StorageId and canonical fingerprint;
   it has no database incarnation or schema-transaction field.
9. Heap initialization flushes metadata/pages. `TableStorage::flush` observes
   WAL-before-data ordering. Prepare appends/syncs the physical WAL and binds the
   database transaction. Parent-directory sync is a separate Core obligation.
10. Physical locators in NBSC are relative UTF-8 paths. They are not a promotion
    API. Heap/WAL objects retain paths, so promotion must close staged handles,
    move known components idempotently, then reopen at the final locator.

`Database` currently holds committed schema, bindings and registry separately,
owned by one synchronous worker. No callbacks run while updating them; publication
can therefore be one private method after every fallible preparation succeeds.
`transaction_owner` Rc counts even untouched retained handles and can enforce
exclusive schema-writer admission. A separate writer lease must survive a dropped
schema transaction so ordinary operations cannot bypass pending recovery.

`schema_catalog_file` installs only initial snapshots. NBSM v1 binds incarnation,
epoch and snapshot CRC. Atomic rename of NBSC and its marker is not jointly atomic;
a runtime winner needs the retained prepared snapshot plus coordinator decision to
finish either half before ordinary `load` validates the pair. `open_authority`
currently loads/validates NBSC before any physical recovery and must be reordered.
The immutable PartitionCatalog is legacy placement evidence, not runtime authority;
a new single Heap must not require rewriting that evidence or dropping its checks.

## Implementation contract

One frontend-neutral Heap creation per transaction; no SQL table DDL, indexes on
staged tables, or mixed index/table DDL. Existing-table DML remains supported.
Canonical schema validation precedes reservation. NBSC allocator floors plus
ordered durable reservation history form one allocator; rollback never rewinds it.
Initial columns use 1..N and next_column_id N+1. PartitionId is not allocated.

A transaction owns its materialized schema view, staged storage and bindings.
Prepared statements carry dependency identities and optional transaction scope;
committed preparation never receives the overlay. A schema decision always takes
2PC, even for an empty new Heap. All physical participants prepare before the
prepared NBSC is synchronized and the coordinator decision is synchronized.

Physical promotion and validation precede NBSC/state publication. Durable winners
are retry-only; presumed losers clean only resources named by their intents.
Recovery uses no directory scan and never recreates missing winner data. Detailed
format and validation results are recorded alongside implementation below.

## Delivered API and visibility

`Database::create_heap_table_in(&mut Transaction, CreateTableSpec)` is the only
creation API added. `CreateColumnSpec` accepts logical name, generic SemanticType
and nullability. No caller IDs, placement, PK, UNIQUE, defaults or expressions are
accepted. Compile-fail examples prove unsupported request fields cannot be supplied.
Canonical schema permits zero columns and keyword names; this API does not invent
SQL-only name restrictions. Invalid/duplicate names fail before reservation.

`prepare_statement_in` compiles normal parameterized DML against the materialized
private schema. HIR resolves real TableId/ColumnIds. A Weak transaction-scope token
and TableId/version/fingerprint dependencies prevent execution outside that exact
active transaction. Committed statements have no private token; unchanged table
dependencies allow old prepared queries to survive creation of an unrelated table.
The embedded SDK reexports the generic requests; generated and remote APIs do not
change. One create per transaction and no index/table DDL mixing are deliberate
limits. Ordinary execution is conservatively serialized while the schema writer
holds its lease; schema preparation/inspection still see only committed metadata.

The admission gate counts every retained database transaction handle, including an
untouched BEGIN. Engine readiness checks also reject a dropped dirty participant or
maintenance recovery requirement. Failed staging marks the transaction
RollbackRequired. A dropped schema handle leaves a recovery-required lease rather
than silently admitting another writer.

## Commit and rollback order

1. Validate canonical schema, duplicate names, capacity and checked generation,
   epoch and runtime revision successors; acquire exclusive schema admission.
2. Activate/reuse the journal/coordinator and durably reserve TableId/StorageId.
3. Persist the create intent, including exact initial columns, relative resource
   identities, target epoch/generation and SHA-256 of the prepared full snapshot.
4. Initialize/sync owner evidence, Heap, WAL and transaction-status files privately.
   Enlist the Heap in the same DatabaseTransaction; allow normal typed DML.
5. Prepare all physical writers (including old Heap/LSM/partition participants),
   write/sync the separate prepared NBSC, then synchronize CORD v2 CommitDecision.
6. Commit physical participants, close staged path-bearing handles, promote exact
   components idempotently with directory sync, validate/reopen the final Heap.
7. Publish NBSC then NBSM, validate the committed pair, sync coordinator Complete,
   resolve the journal, and remove prepared artifacts.
8. Publish decoded committed schema, new registry/binding and checked runtime
   revision in one synchronous worker operation, release the lease, return success.

Rollback before a decision synchronizes participant undo, discards the private
Heap/view, removes only known private components/prepared files, resolves the intent
as loser, then releases admission. A filesystem cleanup error retains
RollbackPending for retry. An uncertain journal sync requires reopen. Reservation
floors never roll back. After an attempted coordinator decision, rollback is
forbidden: DecisionPending, ApplyingCommit and FinalizePending are retry-only.

## Concrete identity and DML evidence

The main fixture begins with users `(TableId=1, StorageId=1)` and teams `(2,2)`.
A staged projects table receives `(3,3)`, ColumnIds `1..5`, next_column_id `6`, and
table version `1`. It contains nominal INT64 `ProjectId`, nominal TEXT `ProjectName`,
BOOL NOT NULL, nullable INT64, and nullable TEXT. Parameterized INSERT/SELECT in the
same transaction returns exactly `(10, 'demo', true, NULL, NULL)` while committed
schema/registry/inspection exclude projects. Both old tables are updated in this
same transaction. Commit changes persistent generation `1 -> 2` and runtime
revision `0 -> 1`; old table versions remain 1. Three catalog-only reopens preserve
all identities/types/nullability and exact old/new rows.

The rollback fixture consumes `(3,3)` and keeps generation 1/revision 0. After close
and reopen, reusing the name projects gets `(4,4)`, never `(3,3)`. The empty new table
commits and queries normally. Additional tests cover empty catalogs/zero columns,
multiple sequential commits, duplicate validation without ID consumption, NOT NULL,
retained handles, dependency scope, dropped writers, failed physical initialization,
checked exhaustion, interrupted journal activation, capacity admission with reserved
resolution space, existing LSM/range participants, stale exact subset expectations,
and uncertain decision/Complete/rollback-journal sync results.

## Subprocess crash results

Every row below is an actual child-process exit followed by **three** catalog-only
reopens. Winners assert table and storage identities, ColumnIds, version, generation,
registry/bindings and exact old/new rows. Losers assert absence, old generation and
rows, and reservation high-waters; a subsequent create proves `(4,4)` allocation.

| Crash window | Observed result |
| --- | --- |
| Reservation durable | Loser; IDs consumed |
| Intent durable | Loser |
| First staged owner file | Loser |
| Complete staged Heap synced | Loser |
| All physical participants prepared | Loser |
| Prepared NBSC shadow written, before sync | Loser |
| Prepared NBSC durable | Loser |
| Before coordinator decision | Loser |
| During coordinator decision append | Loser; valid incomplete tail truncated |
| Complete decision bytes appended, before sync | Winner in this process-exit test |
| Coordinator decision durable | Winner |
| Staged Heap participant committed | Winner |
| Partial component promotion | Winner |
| Complete physical promotion | Winner |
| Before NBSC publication | Winner |
| NBSC published, NBSM still old | Winner |
| NBSC/NBSM durable | Winner |
| Before in-memory publication | Winner |
| After in-memory publication | Winner |
| Before API return | Winner |
| Explicit rollback participant undo durable | Loser |
| Explicit rollback cleanup | Loser |

Additional subprocess tests cover a second winner after an earlier completed schema
history, and both outcomes with an automatically installed coordinator and a write
to an existing Heap. These are abrupt-process tests, not simulated power failures:
complete unsynced decision bytes survive process exit here; recovery always uses the
actual valid log, never a presumed sync outcome. Missing/corrupt winner Heap, owner,
prepared snapshot/digest, journal, and final collisions fail three consecutive opens.

## Persistent compatibility and limits

NBSC, NBSM, NBSL, Heap pages/metadata, WAL, transaction status, index/partition/LSM
formats and Protocol v1 retain their byte versions. New surfaces are NBSJ/NBSR v1,
NBSA v1 activation, NBST v1 owner evidence and CORD record v2 (NBCO file header v1).
[The journal format](schema-mutation-journal-v1.md) specifies framing, bounds,
activation and replay; [CoordinatorLog](coordinator-log-v1.md) specifies backward
reading. Downgrade after writing schema v2 decisions is unsupported.

An existing PG worker session refreshes its catalog through the unchanged runtime
revision mechanism. Its authorization still hides the new table by default; a
separate explicitly granted test principal sees the projection without any PG CREATE
support. No server schema-admin command or wire extension is introduced.

Deferred: SQL CREATE TABLE, DROP/ALTER, enforced PK/UNIQUE/FK/CHECK/default/generated
semantics, runtime LSM/partition creation, multiple creates per transaction, staged
indexes, journal compaction, general aborted-resource GC, and legacy physical-create
cleanup. The journal is deliberately bounded and rewrites retained history; no
performance claim is made. Existing anonymous-index notification gaps and saturating
legacy index revisions are unchanged. Database ownership remains single-process;
filesystem sync/rename guarantees and whole-directory relocation assumptions remain
those of Round 17. Round 19 may add generic typed SQL CREATE only through this Core
path, preserving all unsupported-constraint checks.


## Verification commands and results (2026-08-31)

Primary toolchain: repository-pinned Rust 1.97.1. Commands run in the isolated
Round 18 worktree, with build output outside Git:

```bash
cargo fmt --all -- --check
CARGO_TARGET_DIR=/private/tmp/netbadb-round18-target cargo check --workspace --all-targets --all-features --offline
CARGO_TARGET_DIR=/private/tmp/netbadb-round18-target cargo clippy --workspace --all-targets --all-features --offline -- -D warnings
CARGO_TARGET_DIR=/private/tmp/netbadb-round18-target cargo test --workspace --all-features --offline
```

Formatting, check and Clippy pass. The focused command
`cargo test -p netbadb-core --all-features --offline schema_mutation_tests` passes
20 tests; the one ignored test is an explicitly invoked corpus exporter, not a
skipped correctness test. The 22-window process-crash matrix is included in these
passing tests. The final whole-workspace command also passes, including Core
79 passed / 0 failed / 1 ignored, all 376 storage tests, the existing client/SDK
integration suites, and all three unsupported-request compile-fail doctests.
The command exited successfully after the final storage 100-cycle regression
and all workspace doctests completed.

Rust 1.85 targeted checks were run with:

```bash
CARGO_TARGET_DIR=/private/tmp/netbadb-round18-msrv cargo +1.85.0 check -p PACKAGE --offline
```

| PACKAGE | Result |
| --- | --- |
| netbadb-types | Pass |
| netbadb-schema | Pass |
| netbadb-schema-spec | Pass |
| netbadb-storage | Pass |
| netbadb-core | Blocked by existing planner E0658 let-chains |
| netbadb-server | Blocked by existing planner E0658 let-chains |

The exact blocker is `crates/netbadb-planner/src/lib.rs:892` and `:904` under
Rust 1.85. Those pre-existing expressions are unchanged, as requested; this is not
a claim that Core/server pass MSRV validation.

Seven fuzz targets pass **1,000 runs each**: `schema_catalog_decode`, `btree_decode`,
`index_catalog_decode`, `wal_recovery`, `pgwire_decode`, `schema_mutation_decode`,
and `coordinator_log_decode`. Each was invoked with:

```bash
CARGO_TARGET_DIR=/private/tmp/netbadb-round18-fuzz-target CARGO_NET_OFFLINE=true \
cargo +nightly fuzz run TARGET /private/tmp/netbadb-round18-fuzz-corpus/TARGET -- \
  -runs=1000 -artifact_prefix=/private/tmp/netbadb-round18-fuzz-artifacts/
```

There are no failure artifacts. The first pgwire attempt found a missing temporary
corpus directory; after provisioning its seed input it passed, including the final
repeat. Both deterministic corpus export commands in `fuzz/README.md` ran in two
separate temporary directories; recursive comparison and comparison with all seven
tracked seed files passed.

Final-source real-client checks pass:

- `cargo build --offline -p netbadb-server --example postgres_driver_fixture --example go_sdk_fixture`
  with the same external CARGO_TARGET_DIR.
- `NETBADB_PSQL_TARGET_DIR=/private/tmp/netbadb-round18-target python3 scripts/test-postgresql-psql.py`:
  psql **17.11**, pass.
- `/private/tmp/netbadb-round13-pg-venv/bin/python scripts/test-postgresql-orm.py --dsn DSN`:
  psycopg **3.2.13** and SQLAlchemy **2.0.52**, pass.
- `/private/tmp/netbadb-round13-pg-venv/bin/python scripts/test-postgresql-alembic.py --dsn DSN`:
  Alembic **1.16.5**, pass; baseline/final schema differences both zero. Each Python
  driver script received a fresh `postgres_driver_fixture` endpoint, with DSN
  `postgresql+psycopg://netbadb@127.0.0.1:PORT/netbadb`.
- From `sdk/go`, `GOCACHE=/private/tmp/netbadb-round18-go-cache NETBADB_GO_FIXTURE_BIN=/private/tmp/netbadb-round18-target/debug/examples/go_sdk_fixture go test -count=1 -tags=integration ./...`:
  both Go packages and their real Protocol v1 integration pass.
- `CARGO_TARGET_DIR=/private/tmp/netbadb-round18-target scripts/check-generated-sdk.sh`:
  pass; generated SDK code was checked, not regenerated.

Rust Protocol v1/client/embedded SDK and the existing Phase 73/BTree, ownership,
retirement, adoption, WAL, recovery, LSM and partition suites remain in the full
workspace command. No SQL CREATE TABLE frontend or wire extension was used.
Documentation local links and `git diff --check` pass. Logs and temporary databases,
build outputs, fuzz mutations and artifacts remain outside the commit.
