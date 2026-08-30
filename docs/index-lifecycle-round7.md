# Round 7 index lifecycle audit and contract

Base: main `99300eb658c48fbfe74e5c12ca636791d71e1548`, including Round 6 and
Phase 73 planner/IndexNestedLoopJoin work. The implementation uses retained
registrations with WAL-backed in-place retirement, rather than an event log.

## Audit of the existing implementation

| Question | Implementation evidence and decision |
| --- | --- |
| What was append-only? | `HeapStorage::append_index_definition` appended registrations to the tail; it never removed a registry entry. Statistics were already mutable. |
| Can an entry be WAL-updated? | `rewrite_catalog_statistics` already prepared full-page before/after images and logged them through the existing transaction. Retirement uses that same primitive. |
| How are continuations linked? | `IndexCatalogNode::next_catalog` is a checked PageId chain rooted by Heap metadata. New-page logging precedes its incoming link; WAL is durable before allocation. |
| How are statistics stored? | Only the root stores TableStatistics; each registration stores an optional IndexStatistics snapshot, rewritten by ANALYZE. No statistics events exist. |
| How is the registry rebuilt? | Open follows only the rooted chain, validates IDs/handles/active uniqueness, separates retired ownership, and opens/spec-validates active BTree handles. It never discovers indexes by scanning page kinds. |
| How are BTree pages allocated? | BTree mutations request trailing pages from BufferPool/PageManager. Metadata PageId remains the stable raw tree handle even when roots split/collapse. |
| Arbitrary free/reuse? | None. `PageManager::allocate_page` extends EOF; no free-list or per-tree allocation map exists. |
| What truncation exists? | `remove_trailing_page` is reverse-WAL undo of new allocations, including partial EOF writes. It rejects interior pages and checks buffer pins. It is not a committed-tree reclamation API. |
| Can retired pages remain unreachable? | Yes. BTree merges already leave unreachable right/root pages. Retirement retains the definition/handle but removes every active consumer. Ordinary Heap scans still validate page framing/checksums when passing non-Heap pages. |
| Can checkpoint/vacuum reclaim trees? | Checkpoint requires quiescence and rotates WAL after flush, but does not remap/free pages. Vacuum removes dead tuple candidates using active index plans. Neither is a ready index-page reclaim primitive. |
| Is `(TableId, ColumnId)` sufficient? | It remains sufficient for the current active single-column view, but cannot distinguish successive registrations on that column. DROP targets a durable registry-scoped IndexId instead. |
| Was IndexId already used? | Round 6 defined the u64 newtype in types but did not put it in IndexDefinition/catalog. Round 7 makes it persistent. |

Sources: `crates/netbadb-index/src/lib.rs`, storage `heap.rs`, `table.rs`,
`btree.rs`, `buffer.rs`, `page.rs`, `transaction.rs`, Core `lib.rs`,
`transaction.rs`, `inspection.rs`, planner/executor `lib.rs`, and server
`postgres.rs`. No lower layer depends on PostgreSQL or server policy.

## Identity and persistent representation

IndexCatalog v4 uses the layout in [architecture.md](architecture.md#persistent-index-registry).
Each retained registration has IndexId, optional IndexName, ColumnId, BTreeHandle,
active/retired state, and optional statistics. Retired registrations retain their
name/column/handle for ownership accounting but do not reserve an active name or
column. IndexId and BTreeHandle are never shared by two retained registrations.

IndexId is nonzero and scoped to one physical registry. Supported logical-table
DDL targets `(TableId, IndexId)`. New IDs are checked `max(all IDs) + 1`, including
retired registrations. Committed IDs are not reused. A rollback may reuse an ID
that was never published. Legacy v2/v3 entries deterministically acquire their
previous unique metadata PageId as an initial logical ID; subsequent allocation
is independent of physical PageIds. Lazy v4 rewrite persists that ID explicitly.
A full old page is split safely when its larger v4 prefix no longer fits.

The codec accepts v4, v3 optional names, and v2 unnamed entries. It rejects v1,
unknown state tags, zero IDs, duplicate IDs/handles, malformed lengths/names,
extra bytes, invalid statistics, and duplicate active names/columns. Chain loading
also checks cycles, page bounds/kinds and continuation statistics. Retirement
clears the index snapshot; retired entries with statistics are corrupt. ANALYZE
updates only active entries and preserves retired metadata.

There are no lifecycle events to fold or references to unknown DropIndex events.
The API checks unknown IDs and double retirement before a write. In-place state
is restored by WAL undo or redone by recovery. This avoids introducing an event
ordering language or a second statistics history. No other persistent format or
Inspection JSON version changes.

## Transaction and publication contract

Implicit DROP owns a storage transaction. Explicit DROP stages an identity in the
existing Core coordinator after logging the retired catalog image. Commit first
finishes the existing durable transaction protocol, then removes the active
index definition, registered DML plan, and statistics cache. Core publishes one
catalog-generation change per committing DDL transaction. Rollback publishes
nothing. Raw physical inspection is explicitly separate from active APIs.

Before commit, the published index remains usable by queries and maintained by
DML; the ordinary reflection view remains unchanged. This is the same bounded
publication model used by CREATE, not PostgreSQL catalog MVCC. A transaction
mixing CREATE and DROP is explicitly unsupported. Prepared DROP never retargets
a new registration after name reuse. IF EXISTS missing is a no-op; a missing
required target is an undefined-object error. Non-active transactions cannot
execute a no-op DDL to evade retry-only state.

The audit found that Round 6's Database commit wrapper reused an Active-only
owner check, preventing retry of pending commits through that wrapper. The
wrapper now checks owner identity, then delegates allowed pending states to the
existing coordinator state machine. Tests inject both coordinator decision sync
and finalization sync failures: active caches/generation stay unchanged until
retry succeeds. Storage tests inject WAL commit flush and rollback interruption
failures and verify writer retention and later successful retry. No new commit
policy or log format is introduced.

## Crash and recovery evidence

Actual subprocess exits skip Rust destructors. These model process loss, not
hardware power loss. The retirement crash test reopens each outcome three times.

| Crash point | Recovered outcome |
| --- | --- |
| Before catalog log | Active original index |
| After catalog log/publish, before explicit WAL flush | Active original index (loser undo) |
| After WAL/page flush, before transaction decision | Active original index (loser undo) |
| After commit WAL sync, before status finalization/publication | Retired index (winner redo) |
| After commit returns, before in-memory removal | Retired index (winner redo) |

Tests also cover catalog-log failure, rollback, close/reopen, checkpoint/reopen,
new ID/handle on same-name recreation, and stale prepared DROP not targeting the
replacement. A malformed BTree payload retained behind a retired definition is
not opened or validated as active; normal insert/ANALYZE/vacuum/recreate still work.
Generic page framing/checksum validation is not disabled.

## Planner, DML, and frontend evidence

A two-table Core test with current statistics selects point, range, and costed
IndexNestedLoopJoin access before DROP. After commit each disappears without
clearing unrelated statistics. The same prepared parameterized SELECT and
prepared join execute correctly, using the current planning snapshot. Storage
checks the retired tree's physical page bytes before and after INSERT, UPDATE,
DELETE, ANALYZE and vacuum: they are unchanged. Recreated indexes backfill rows
inserted after retirement and have no inherited index statistics.

PG Simple/Extended DROP use generic Core DDL; Extended covers Parse, Bind,
Describe, Execute and Sync. Explicit name resolution yields table write access.
Legacy aliases are matched only in the adapter's authorized compatibility
catalog, then translated to the current generic ID. Hidden-table alias tests
return absence without exposing the table. No synthetic name is persisted.
Errors preserve I/T/E state: missing `42704`, denied `42501`, unsupported `0A000`,
and subsequent statements in a failed transaction `25P02`.

The psql script verifies CREATE/DROP, rollback/commit, IF EXISTS, `\di` and `\d`.
SQLAlchemy tests use real Index APIs and an already-open observer connection,
plus one persistent psql observer to check both directions of committed removal.
Alembic produces and invokes actual CreateIndexOp/DropIndexOp operations after
validating the entire proposal; named and legacy removals leave `compare_metadata`
empty. No custom dialect, monkey patch, disabled preparation, or SQL replacement
for Index.drop is used.

## Physical lifetime and Round 8

DROP is logically and durably complete. Reclamation is deferred: retired tree
pages remain in the file, are not reused, and are not accessed by active planning,
DML, ANALYZE, vacuum, or registry rebuild. `HeapStorage::retired_indexes()` is an
unstable storage-only ownership view of retained definitions/handles. It is not
ordinary catalog reflection, a free-page map, or an exact accounting of historical
BTree merge orphan pages. Deliberate raw BTree inspection remains possible.

Repeated CREATE/DROP grows both catalog history and physical file size. The next
storage lifecycle audit should cover all retained/merge-orphan ownership, a
quiescent checkpoint boundary, WAL-safe free/reuse or file compaction, buffer pin
and generation safety, and catalog compaction retaining the logical ID high-water
mark. No immediate truncation or fake reclamation is claimed. Table-schema
lifecycle requires a separate authority/fingerprint/manifest/recovery/SDK audit;
CREATE TABLE coding is not part of this phase.

## Additional MSRV observation

The required development toolchain is 1.97.1. An additional
`cargo +1.85.0 check --workspace --all-targets --all-features --offline --locked`
probe fails with E0658 at unchanged `netbadb-planner/src/lib.rs:892,904`: Phase 73
uses let-chain syntax unavailable on the declared workspace MSRV. The planner
file is byte-identical to base main for this change. This pre-existing MSRV
regression is reported separately; Round 7 does not change the toolchain or
rewrite the protected mainline planner work.


## Final verification (2026-08-30)

All primary Rust commands ran on 1.97.1 with
`CARGO_TARGET_DIR=/private/tmp/netbadb-round7-target` (formatting needs no target).
The final run was serialized to avoid another build replacing dependency
artifacts during doctests. It reported 692 passed, zero failed or ignored.

| Command / probe | Result |
| --- | --- |
| `cargo fmt --all -- --check` | Passed |
| `cargo check --workspace --all-targets --all-features --offline` | Passed |
| `cargo clippy --workspace --all-targets --all-features --offline -- -D warnings` | Passed |
| `cargo test --workspace --all-features --offline` | Passed, including doctests, native Protocol v1, CREATE and Phase 73 regressions |
| `python3 scripts/test-postgresql-psql.py` | Passed, real psql 17.11; target directory selected with NETBADB_PSQL_TARGET_DIR |
| `/tmp/netbadb-pg-venv/bin/python scripts/test-postgresql-orm.py --dsn <fixture-dsn>` | Passed, psycopg 3.2.13 / SQLAlchemy 2.0.52, including persistent psql observer |
| `/tmp/netbadb-pg-venv/bin/python scripts/test-postgresql-alembic.py --dsn <fixture-dsn>` | Passed, Alembic 1.16.5; named and legacy removal apply, final comparison empty |
| `cargo +nightly check --manifest-path fuzz/Cargo.toml --all-targets --offline` | Passed |
| `cargo clippy --manifest-path fuzz/Cargo.toml --all-targets --offline -- -D warnings` | Passed |
| `cargo +nightly fuzz run pgwire_decode <corpus> -- -runs=1000` | Passed |
| `cargo +nightly fuzz run index_catalog_decode <corpus> -- -runs=1000` | Passed, regenerated v2/v3/v4 named/unnamed/retired corpus |
| `cargo +nightly fuzz run wal_recovery <corpus> -- -runs=1000` | Passed |
| `cargo +1.85.0 check --workspace --all-targets --all-features --offline --locked` | Blocked by unchanged Phase 73 planner let chains, as described above |
| `cargo +1.85.0 check -p netbadb-index -p netbadb-storage -p netbadb-parser -p netbadb-hir -p netbadb-compiler --all-targets --offline --locked` | Passed |
| `git diff --check`, changed Markdown relative links, Python syntax | Passed |

Fuzz and MSRV builds used separate temporary target directories. Corpus generation
ran through `generate_wal_corpus`; only the reviewed IndexCatalog seeds were
updated in the repository. Each final real-client script used a fresh two-table
fixture; all used the default PostgreSQL dialect and preparation behavior.
