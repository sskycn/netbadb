# Round 8: quiescent IndexCatalog compaction

Base: main `9f5ade6b190163f0fcfea65cbc042c48e183743f`.
Branch: `codex/index-lifecycle-round8`; isolated worktree: `../netbadb-index-round8`.

**Catalog compaction: complete. Physical reclaim: deferred.** This phase bounds
reachable registration metadata, not database-file growth. It deliberately
chooses policy C from the Round 8 request: once compaction removes an ownership
registration, its physical pages are permanently abandoned. They are not a
promised deferred-reclamation inventory. No tail truncate or arbitrary reuse is
implemented. This tradeoff preserves the current raw-handle contract and avoids
silently adding a new allocator/recovery protocol.

## Allocation and ownership audit

Evidence is in storage `page.rs`, `buffer.rs`, `btree.rs`, `heap.rs`,
`transaction.rs`, `recovery.rs`, `wal.rs`; Core `lib.rs`, `transaction.rs`; server
`postgres.rs` and the synchronous database worker. The audit preceded changes.

| Question | Existing code and Round 8 consequence |
| --- | --- |
| Shared manager? | Heap, BTree and IndexCatalog in a physical Heap file share its PageManager, BufferPool, transaction manager and WAL. A multi-table Database may have several independent physical files. |
| PageId scope? | PageId is file-global, not database-wide across physical storages. Metadata is page 0, initial catalog root page 1, initial Heap page 2. |
| Allocation? | PageManager::allocate_page appends zero bytes at EOF and advances page_count. Typed callers then initialize a complete page image. |
| Free reuse? | No free-list, generation per PageId, durable per-tree allocation map or arbitrary deallocation exists. |
| Rollback truncation? | remove_trailing_page removes only a new trailing allocation, including partial EOF extension. Reverse WAL undo restores incoming links first; BufferPool rejects pinned pages and removes cached frames. |
| Interior reclaim? | Unsupported. Removing an interior page would shift file-global IDs or require a new durable allocator. |
| Buffer retirement? | Buffer frames track PageId, pins, writer, dirty state and pageLSN, not index retirement. DROP only changes registry consumers. |
| Pinned dropped tree? | Guards are local to synchronous storage operations; no guard escapes through normal query execution. Internal pins can exist independently of transactions, so maintenance explicitly checks every frame is unpinned before logging. |
| Historical WAL? | Checkpoint flushes WAL, dirty frames and data-file sync before publishing a higher WAL generation and removing the old one. Recovery only uses the selected generation. Current PageUpdate redo assumes existing/appended pages, not committed deallocation. |
| PageId kind reuse? | No committed-page reuse contract. A truncated suffix followed by append WOULD reuse IDs and could name a different page kind. |
| CRC identity? | Normal Page v5 CRC32C binds PageId and bytes, including pageLSN. Page 0 uses separate legacy Heap metadata validation. CRC detects relocation, not a stale handle to correctly reinitialized bytes at the same ID. |
| Complete traversal? | Meta gives authoritative current root and height. DFS follows every internal child to all leaves; leaf next links are checked against DFS order and sorted boundary keys. |
| Shared trees? | Intended tree ownership is exclusive. The new global visited set checks all registered and raw tree roots for cross-tree aliases, duplicate children and cycles. |
| Catalog ownership? | Only the fixed rooted chain is authoritative. Every catalog page has its own file-global ID. All catalog/Heap pages are reserved against tree ownership, including obsolete catalog pages. |
| Quiescence? | Existing checkpoint gate requires healthy runtime, idle writer and zero outstanding physical transaction handles, including read-only/pending/prepared states. Close uses the same transaction-lifetime safety principle. |
| Executing queries? | Core execution is synchronous with owned results. The server's dedicated worker exclusively owns Database and sessions. Core maintenance needs mutable Database access and refuses retained DatabaseTransaction handles, including lazy participants. |

BTree merges leave detached right pages and obsolete roots. They cannot be
recovered as authoritative ownership by walking the current root. A page-kind
scan is not permission to reclaim them. The inventory reserves Heap/catalog
pages, walks active/retired registered trees, then raw BTreeMeta roots; unknown
internal/leaf orphans remain unknown. Invalid CRC, height, child bounds/kind,
duplicate edges, cross-tree alias, leaf linkage or entry order is a hard error
before any maintenance transaction. This may reject a corrupt retired/raw tree
that ordinary active-only open/DML can still ignore; it does not weaken normal
open behavior or silently discard corrupt ownership.

The traversal returns metadata + currently reachable root/internal/leaf pages,
not every page ever allocated by a tree. Its worklist marks children at enqueue,
so malformed fanout cannot grow it beyond the file's page count. It is iterative,
uses short-lived guards and works with buffer capacity one. The API is storage
internal; no new BTree handle crosses into planner or server.

## Why physical reclaim is deferred

A caller can retain a Copy BTreeHandle outside a transaction, then invoke the
raw BTree API later. Neither that handle nor AccessPathId carries a generation.
Normal Core plans re-resolve active paths, but the public raw API does not
invalidate a dropped physical handle. Quiescence alone cannot prove no future
use of such a retained handle. The regression intentionally verifies that the
raw handle still reads the old bytes after catalog compaction.

Additionally, current WAL has no committed truncate/free record. Checkpoint
removes stale page history but does not atomically publish a truncate intent or
make set_len power-loss atomic. Truncating first while tombstones remain could
leave handles beyond EOF; deleting them first loses crash-retry ownership.
A correct physical phase needs a durable protocol and a handle-lifetime or
PageId-generation decision, not just a call to rollback's remove_trailing_page.

No data bytes bypass BufferPool/WAL in production. No pages become free; none
are truncated, punched out or reused. Dropped tree pages and obsolete catalog
continuations stay valid physical pages. Once their registrations/links are
removed, ownership is intentionally abandoned. Prior abandoned trees may be
encountered as raw roots on a later inventory, but that does not reconstruct
retirement provenance or classify them as available for reclamation. Historical
merge orphans remain a separate pre-existing permanent space leak.

## IndexCatalog v5 and high-water proof

The exact 48-byte header and entry layout is documented in
[architecture.md](architecture.md#persistent-index-registry). V5 assigns header
byte 7 as next_index_id presence and bytes 40..48 as its u64 little-endian value.
Only the root has it. Continuations require absence and zero. No Heap metadata,
Page, BTree, WAL, coordinator, protocol or Inspection JSON format changed.

1. A new empty root has next_index_id = 1.
2. V2/v3 keep the Round 7 deterministic metadata-PageId-derived IDs. V4 retains
   explicit IDs. A legacy root derives its initial boundary from ALL active and
   retired entries; no legacy format supported compaction.
3. CREATE obtains the root boundary and checked successor, reserves the successor
   in the same storage transaction as tree allocation/backfill/registration,
   and publishes the index only after durable commit. Rollback may reuse an
   unpublished ID. A committed ID is never reused.
4. V5 open validates the boundary against all entries, across every continuation.
   Zero, missing root boundary, misplaced continuation boundary, duplicate IDs,
   or any entry >= boundary is corrupt. It never falls back to max(active).
5. DROP/ANALYZE preserve the boundary. Compaction copies it unchanged while
   deleting retired registrations and preserving active order/statistics.
6. A committed compaction therefore retains a boundary above every committed
   historical ID. Both WAL loser undo and winner redo keep this invariant.
7. next_index_id = u64::MAX is an exhausted but readable boundary. CREATE checks
   the successor before allocating a tree and returns IndexIdExhausted; no wrap,
   zero or last-ID issuance occurs. Legacy ID MAX cannot derive a valid boundary
   and is rejected with the same typed error.

Mixed legacy chains remain readable while individual ordinary updates lazily
upgrade pages. Rewriting a legacy root derives the boundary from the full chain.
Explicit compaction canonicalizes every surviving page to v5. Names, legacy
aliases, IndexIds, BTree handles and optimizer snapshots are preserved.

## Compaction algorithm and API

`Database::compact_index_catalog(TableId)` supports single-storage Heap tables;
LSM and partitioned tables return typed unsupported errors. It delegates through
TableStorage to HeapStorage::compact_index_catalog. This is an explicit admin
result, not SQL QueryResult. No SQL or PG maintenance command was added.

Heap admission reuses ensure_checkpoint_safe and adds unpinned-frame preflight.
The Core wrapper also rejects all retained database transaction handles,
including completed handles not yet dropped: this is intentionally conservative
because participant registration is lazy. The physical gate applies to the
selected storage. Callers must release such handles before maintenance. There
is no background worker, wait, writer queue, DML-triggered rewrite or DROP scan.

After admission, load and validate the catalog and complete page inventory.
Keep active entries in existing creation order, their current index statistics,
and the root's table statistics. Pack them greedily into the smallest chain
consistent with that order. Reuse the old chain's prefix; keep its fixed root.
Dense v2/v3 pages may require new trailing continuations because persisted IDs
increase entry width. Former continuation pages not used by the replacement
are permanently abandoned after commit.

Preflight all full-page images. Log new pages first, then existing continuation
updates, then root. Flush WAL before any new allocation; publish those images in
the same order. The logical publication point is the existing durable Commit,
not a raw root overwrite. All changed existing pages have before-images;
reverse undo restores links before removing newly allocated trailing pages.
After successful commit clear only the retired-definition cache. Do not rebuild
or reorder active definitions/plans/statistics or increment catalog_generation.
Repeated canonical maintenance logs nothing and allocates no pages.

This deliberately uses existing recovery-supported in-place transactions
instead of a new shadow-root publication format. It is not an unprotected
in-place compaction. A failed partial log or page publication requests rollback;
an uncertain commit follows existing recovery-required behavior on dropped
pending transaction handles and requires reopen, just like implicit DDL.

## Crash reasoning and subprocess matrix

Subprocess exit skips Rust destructors. These tests model abrupt **process**
termination, not power failure or atomic filesystem truncation. No new filesystem
atomicity assumption is introduced. Existing WAL checksums/tail handling,
WAL-before-page, winner redo and loser undo remain authoritative.

| Exit boundary | Required recovered state |
| --- | --- |
| All compaction PageUpdates logged, before explicit WAL flush | Old catalog via loser undo; unwritten complete records need no data undo |
| Legacy new continuation allocated, before payload publication | Old catalog; zero/partial new suffix removed by existing reverse allocation undo |
| A replacement page published, root not yet published | Old catalog, including original links and IDs |
| Replacement continuations durable, before root publication | Old catalog; all overwritten continuation before-images restored |
| Every replacement page durable, before Commit | Old catalog; no partial chain or lost active entry |
| Commit WAL durable, before transaction status/cache finalization | New compact catalog via winner redo; dropped indexes stay absent |
| Commit returned, before retired-cache clearing | New compact catalog on reopen |

Both the shrinking historical-chain matrix and the growing legacy-chain matrix
reopen each result three times. The former checks preserved active names/stats,
old-or-new retirement count and next CREATE identity. The latter checks exact
logical IDs, high-water, chain version and removal/retention of the new page.
Partial WAL append rollback and pinned-page rejection have deterministic unit
regressions. There is no physical reclaim crash matrix because no reclaim or
truncate operation is implemented; it would be misleading to claim otherwise.

## Growth and maintenance report

IndexMaintenanceReport returns before/after catalog and file pages, active
index count, retired registrations removed, next_index_id, reachable retired
tree pages, obsolete catalog pages, reclaimed pages (always zero), permanently
abandoned pages and geometric retired suffix. Counts of abandoned pages describe
this invocation, not an all-time durable leak counter. The suffix is diagnostic
only: it explicitly is not a reclaimable_now promise. Merge-orphan counts are
not included in reachable retired-tree counts.

The deterministic stress uses one active index plus 100 create/drop cycles on
one reusable name/column, with buffer capacity one. It checks every new ID,
compacts, repeats maintenance without new WAL records, checkpoints/reopens,
recreates the same name and exercises INSERT/UPDATE/DELETE/vacuum/ANALYZE.
Observed counts (no timing benchmark):

| Metric | Before maintenance | After maintenance |
| --- | ---: | ---: |
| Reachable catalog pages | 2 | 1 |
| Database-file pages | 206 | 206 |
| Active registrations | 1 | 1 |
| Retired registrations | 100 | 0 |
| Durable next_index_id | 102 | 102 |
| Physical pages reclaimed | — | 0 |
| Reachable retired BTree pages abandoned | — | 200 |
| Old catalog continuation pages abandoned | — | 1 |
| Total newly permanently abandoned pages | — | 201 |

The geometric suffix is 201 pages but is not truncated. Repeated maintenance
reports zero additional abandonment and writes no WAL. Reopen/recreate issues
ID 102, not an ID from the removed 2..101 history. The same-name replacement
gets an independent new physical tree and has no inherited index statistics.

## Compatibility, scope and next work

Core tests preserve complete inspection results, catalog_generation, point,
range and costed IndexNestedLoopJoin plans, and prepared query results across
maintenance. The full workspace protects HashJoin selection, exact vacuum
candidate cleanup, DML, ANALYZE and native Protocol v1. PG catalog tests compare
synthetic names, OIDs and column mappings before/after compaction and reopen;
real psql/psycopg/SQLAlchemy/Alembic scripts exercise the unchanged index SQL
surface. Alembic's CreateIndexOp/DropIndexOp-only guard remains intact.

No table DDL, REINDEX, unique/composite/partial index, LSM/partition index,
protocol change, CLI maintenance command or background GC was added. The
transaction-status sidecar remains append-only; it is not bounded by catalog
maintenance. No performance claim or timing gate is made.

Round 9 should continue the storage allocator/ownership audit: generation-safe
raw handles, authoritative merge-orphan ownership, durable free/truncate intent,
WAL/LSN ordering and buffer invalidation, or an explicit whole-file rewrite.
Full physical compaction and arbitrary page reuse remain deferred. Table Schema
Lifecycle Architecture Audit should follow those decisions; CREATE TABLE is
not the next coding step.

## Verification (2026-08-30)

Primary Rust validation uses the pinned 1.97.1 toolchain and
`CARGO_TARGET_DIR=/private/tmp/netbadb-round8-target`. Commands are serialized
within that target directory so concurrent feature builds cannot replace
dependency artifacts during doctests. The workspace run reports **702 passed,
zero failed or ignored**, including native Protocol v1, planner and executor
regressions. The final NO-FORCE maintenance subset was also rebuilt independently
and passed (11 selected tests); its hooks force page sync only at the explicitly
selected durable-page crash point, preserving genuine unflushed winner-redo
coverage at the Commit boundaries.

| Command | Result |
| --- | --- |
| `cargo fmt --all -- --check` | Passed |
| `rustfmt --edition 2024 --check crates/netbadb-storage/src/index_maintenance_tests.rs` | Passed; explicitly covers the included test source |
| `cargo check --workspace --all-targets --all-features --offline` | Passed |
| `cargo clippy --workspace --all-targets --all-features --offline -- -D warnings` | Passed, no lint suppressions added |
| `cargo test --workspace --all-features --offline` | Passed, 702 tests including doctests |
| `cargo test -p netbadb-storage maintenance --offline -- --nocapture` | Passed, includes both process crash matrices and ownership/high-water negatives |
| `cargo test -p netbadb-storage compaction_bounds_catalog_growth_and_never_reuses_ids --offline -- --nocapture` | Passed; exact report reproduced above |
| `cargo +nightly check --manifest-path fuzz/Cargo.toml --all-targets --offline` | Passed |
| `cargo clippy --manifest-path fuzz/Cargo.toml --all-targets --offline -- -D warnings` | Passed |
| `cargo run --manifest-path fuzz/Cargo.toml --bin generate_wal_corpus --offline -- /private/tmp/netbadb-round8-corpus/wal_recovery` | Passed; only reviewed catalog seeds copied into the repository |
| `cargo +nightly fuzz run pgwire_decode --target-dir /private/tmp/netbadb-round8-fuzz-run /private/tmp/netbadb-round8-corpus/pgwire_decode -- -runs=1000 -seed=8 -artifact_prefix=/private/tmp/netbadb-round8-fuzz-artifacts/` | Passed, 1000 runs |
| `cargo +nightly fuzz run index_catalog_decode --target-dir /private/tmp/netbadb-round8-fuzz-run /private/tmp/netbadb-round8-corpus/index_catalog_decode -- -runs=1000 -seed=8 -artifact_prefix=/private/tmp/netbadb-round8-fuzz-artifacts/` | Passed, 1000 runs; successful decodes canonicalize and round-trip |
| `cargo +nightly fuzz run wal_recovery --target-dir /private/tmp/netbadb-round8-fuzz-run /private/tmp/netbadb-round8-corpus/wal_recovery -- -runs=1000 -seed=8 -artifact_prefix=/private/tmp/netbadb-round8-fuzz-artifacts/` | Passed, 1000 runs |
| `NETBADB_PSQL_TARGET_DIR=/private/tmp/netbadb-round8-pg-target python3 scripts/test-postgresql-psql.py` | Passed, psql 17.11 |
| `/tmp/netbadb-pg-venv/bin/python scripts/test-postgresql-orm.py --dsn <fresh-fixture-dsn>` | Passed, psycopg 3.2.13 / SQLAlchemy 2.0.52 |
| `/tmp/netbadb-pg-venv/bin/python scripts/test-postgresql-alembic.py --dsn <fresh-fixture-dsn>` | Passed, Alembic 1.16.5; final comparison empty |
| `CARGO_TARGET_DIR=/private/tmp/netbadb-round8-pg-target cargo build --offline -p netbadb-server --example go_sdk_fixture` | Passed |
| `GOCACHE=/private/tmp/netbadb-round8-go-cache NETBADB_GO_FIXTURE_BIN=/private/tmp/netbadb-round8-pg-target/debug/examples/go_sdk_fixture go test -count=1 -tags=integration ./...` (from `sdk/go`) | Passed, client and generated test-schema packages |
| `cargo +1.85.0 check -p netbadb-index -p netbadb-storage --all-targets --offline --locked` | Passed |
| `cargo +1.85.0 check --workspace --all-targets --all-features --offline --locked` | Blocked by pre-existing E0658 let chains in planner at lines 892 and 904 |
| `git diff --check`, local Markdown target checks and catalog seed version/length inspection | Passed |

Fuzz ran with CARGO_NET_OFFLINE=true, default AddressSanitizer, and temporary
corpus/artifact directories. Builds for fuzz checks, fuzz Clippy, standalone
maintenance and MSRV used independent temporary target directories. No random
fuzzer discoveries or local databases are committed. The real-client example
creates/drops one extra index, compacts through Core, verifies unchanged active
inspection, checkpoints and reopens before starting its listener. Each client
script receives a fresh fixture. Alembic's entire proposed operation list remains
guarded to CreateIndexOp/DropIndexOp; no table migrations run.

The MSRV failure is unchanged from Round 7. Planner, page allocator, WAL and
recovery production files remain byte-identical to the base; no unrelated MSRV
repair or persistent-format bump was included. Original main remains at the base
commit with a clean working tree. All edits and the requested commit belong only
to the isolated Round 8 worktree.

## Round 35 relationship

Round 35 uses ordinary transactional retirement inside a disposable staged Heap
and adds no compaction or reclamation semantics. Evacuated ownership remains
subject to the same catalog/high-water and generation-safe page rules; whole-S2
predecision cleanup is composition recovery, not index-page reclamation.
