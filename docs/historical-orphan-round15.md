# Round 15: explicit quiescent historical BTree orphan adoption

Base: `4502b841c32501834ec757faba0a4a5d03e31eb0`.
Branch: `codex/historical-orphan-round15`.
Worktree: `../netbadb-historical-orphan-round15`.

## Historical audit and scope

The Round 9/14 fixtures, pre-Round14 implementation and Round 14 mutation diff
were re-audited. `delete_balanced_in` leaf/internal merges formerly removed a
right sibling from its parent (and bypassed the right leaf in the leaf chain)
without rewriting that sibling. Root collapse updated the stable meta without
rewriting the old root. `normalize_delete_path` similarly abandoned a unary
root, an internal merge's right sibling, and sometimes its now-unary parent.
Redistribution, splitting and root growth do not abandon old allocations.
Heap DELETE only expires versions; vacuum invokes those physical BTree deletion
paths. There is no separate vacuum node-removal implementation.

Consequently a pre-Round14 ordinary NBTL/NBTI v3 image can retain its nonzero
IndexId and PageGeneration, original pageLSN, CRC and dormant structural payload
without belonging to today's tree. The 90-row, 1000-byte-key fixture has 83
allocations, height 5 before shrinking, then two reachable allocations and 81
ordinary historical orphans. Tests reconstruct these precise active before
images from Round 14 retirement WAL **only in fixture setup**, then checkpoint.
Production never reconstructs them or automatically adopts them on open.

The allocator previously excluded them because an active owner plus absence of
a current incoming edge is not durable retirement authority. Round 14 future
orphans instead already carry NBTR, and must not be adopted again. Whole-index
DROP remains a separate committed catalog-retirement authority.

## Explicit API and admission

`Database::adopt_historical_btree_orphans(TableId)` delegates through TableStorage
to one HeapStorage. All eligible active registered indexes on that table are
handled together. LSM and range-partitioned placements return typed unsupported
errors. No SQL command, client capability or ordinary CatalogInspection field
is added. Registry identities, statistics and Core catalog_generation do not
change.

Core reuses `ensure_index_maintenance_quiescent`: all DatabaseTransaction handles,
including lazy and resolved retained handles, must be dropped. Heap reuses
`ensure_checkpoint_safe`: no outstanding active transaction, active writer,
CommitPending, RollbackPending, dirty dropped writer or recovery-required state.
Buffer admission rejects all pins, stricter than merely checking candidates.
A durable tail-reclaim intent is independently rejected. This is the existing
synchronous exclusive database-owner contract, not a new concurrency framework;
multiple independent writers opening the same file remain unsupported.

Workflow:

```text
existing quiescent gate + pins + durable intent check
  → complete preflight tree/file validation
  → zero eligible candidates: return 0, no checkpoint/WAL write
  → internal checkpoint
  → fresh complete authoritative tree/file validation
  → ascending-PageId plan + exact clean/unpinned image proof for every candidate
  → BEGIN + dedicated writer + all-candidate revalidation
  → per-page revalidation → ordinary PageUpdate to NBTR
  → COMMIT durable → cache invalidation → committed-marker flush → return
```

Preflight and authoritative validation use the same scanner. Preflight authorizes
only a no-op or checkpoint; its candidate set is never executed. Checkpoint is
inside the exclusive method, with no caller-visible TOCTOU interval. The private
post-checkpoint plan also requires candidate generations and pageLSNs to precede
the WAL generation base. Preflight rejects a generation beyond WAL high-water.

## Checkpoint/WAL horizon proof

This proof depends on actual `HeapStorage::checkpoint`, `WalManager::rotate`,
`open_selected`, `validate_generation_candidates` and recovery behavior, not on
an assumption that flushing a few pages makes old references harmless.

1. Quiescent admission means no transaction can later undo a structural update
   or commit an older view. Resolved rollback is physical undo plus synchronized
   pages and durable RollbackComplete; unresolved/dropped writers fail admission.
   Startup resolves ordinary winners/losers before exposing storage; prepared
   participants retain admission-blocking transaction state.
2. Checkpoint flushes WAL through the highest written LSN, then all dirty frames.
   Each frame preserves WAL-before-data ordering and the data file is synced.
   Thus the complete currently committed tree, including its stable meta/root,
   is the durable baseline before the next generation is created.
3. Rotation records `generation + 1`, `base_lsn = old.next_lsn`, the old checkpoint
   boundary and next-TxnId high-water. It syncs the new header/file and parent
   directory before switching the shared manager. It removes the superseded
   generation and syncs the directory before returning success. Old pageLSNs
   and allocation generations are not reset.
4. An incomplete new header retains the old generation, but adoption has not
   begun. Once the new generation is durable, open chooses only the greater
   consistent generation. If two slots remain, consecutive generations, exact
   logical-end/base continuity, checkpoint LSN and transaction high-water must
   agree. A malformed complete newer generation is an error, not fallback.
5. `open_selected` returns records from **only the selected generation** to
   recovery. Validated older slots are deleted; they are not merged into redo
   or undo. Adoption records therefore have LSNs above every pre-checkpoint
   structural record and a before image from the authoritative baseline.
6. A loser adoption restores its ordinary orphan bytes without touching current
   tree references. A winner installs markers without touching those references.
   A later allocation uses a fresh reserved generation through the existing
   transition protocol. Neither old structural WAL nor an old PageRef can
   become authoritative again under this recovery contract.

The directed horizon regression keeps real former-root bytes in a valid old
structural WAL chain whose final image restores today's root. After production
adoption, it deliberately reinstalls that entire superseded WAL file before
**each of three opens**. Every open selects the new generation, exposes only
post-checkpoint records, removes the old slot, and validates the unchanged tree
plus 81 markers. The fuzz horizon envelope additionally supplies both generations
to the real startup selector. Process-loss tests are not torn-write repair or
machine/controller power-loss simulation; CRC failures remain hard errors.

## Reachability, ownership and candidate proof

The existing `index_page_inventory` is reused, with its typed observations now
available to the maintenance planner. It validates all managed Page v5 CRCs,
layouts and payloads, reserves Heap and catalog pages, discovers metadata roots,
and validates active, root-dependent-retired and raw/legacy trees in one global
physical-overlap set. Owner-only retired pages retain their existing dormant
reference policy. Unknown owners, malformed nodes, conflicting identities and
unexplained metadata roots fail closed.

`collect_owned_pages` starts at the registered meta **PageRef**, decodes the root
PageRef and height, and traverses all internal children. Every read validates
kind, owner, generation and CRC. A PageId membership set catches shared physical
slots/cycles at enqueue time; it does not replace full PageRef validation.
Leaves in child-traversal order must have strictly ordered entries and exactly
matching `next_leaf` PageRefs, with a terminal None. Missing/extra leaves, stale
generations, duplicate children, cross-owner references, and root/child/next links
to markers all fail before mutation. The tests exercise these negative paths.

For each active registered generation-safe owner X:

```text
eligible(X) = fully decoded ordinary v3 allocations owned by X
              − fully validated current structural reachability(X)
```

Stable meta and current root are additionally protected independently of the
inventory membership set, and only leaf/internal envelopes are permitted. NBTR,
dropped/retired owners, owner-only pending records, v1/v2, raw and unknown pages
are never historical candidates. Global ownership validation prevents any
candidate from belonging to two trees; PageId-sorted output is deterministic.

Each plan entry keeps IndexId, PageRef, expected kind and the **entire original
page** as its exact fingerprint. `maintenance_page_snapshot` performs no frame
installation, eviction, flush or invalidation: pinned/writer frames return
PagePinned, dirty frames return PageDirty, and a clean frame must match disk.
Every candidate passes before BEGIN and again before the first marker log, then
each passes immediately before its own write. No candidate is skipped to make
partial progress. The post-checkpoint proof rejects any dirty managed frame
before traversal can hide that condition through eviction/writeback. Dormant
outgoing links do not establish active reachability.

## Representation, transaction size, rollback and visibility

Adoption is `P/G/X ordinary v3 → P/G/X NBTR v1`, using Round 14's existing
`retire_btree_page_in`. It changes neither allocation nor owner, so ordinary
full before/after PageUpdate is sufficient. There is no reservation or
PageAllocationTransition during adoption. Marker bytes use the original
leaf/internal Page v5 envelope; meta cannot become a marker.

WAL has an **8,240-byte per-record** maximum, not a transaction-wide record or
byte cap. Begin/Commit are 40 bytes each and the container header is 48 bytes:
one successful adoption generation occupies `48 + 40 + N*8240 + 40` bytes.
The planner checks u64 arithmetic conservatively including rollback termination
records before BEGIN. There is no hidden batching, durable maintenance intent,
second free catalog or arbitrary new maximum. One API invocation is the atomicity
boundary. Plan memory is O(N*4096); existing WAL scan/recovery memory is O(WAL
bytes). This is not an unlimited-memory claim: very large files retain the
existing full-scan resource model. Measured 81 and 1,186 candidates fit naturally.

A partial runtime publication failure invokes ordinary rollback and restores
every original byte, including owner, generation, pageLSN, payload and CRC. An
unresolved commit/rollback or failed checkpoint requires reopen. In particular,
a commit-sync error may leave a complete readable winner record; the API does
not falsely promise that an error means no commit. Retrying after reopen is safe.

Round 14's transaction-local retired PageRef set excludes new markers from cache
construction and claims even after all dirty pages are stolen and cache is
rebuilt. Other writers cannot acquire the lease. Durable commit invalidates the
cache before releasing the writer. Rollback invalidates before undo. Open derives
it afresh. After commit, adoption flushes committed markers before successful
return because the unchanged allocator skips dirty candidates. It never reuses
an uncommitted marker. A subsequent allocation uses the existing sorted hole
allocator; same-owner reuse requires NBTR, and a newer generation makes G1 stale.

## Directed crash and failure results

Each process-crash row below ran through a child process exiting without Rust
destructors and **three validated reopens**. All cases start with 81 historical
ordinary orphans and two unchanged reachable pages.

| Crash point | Marker pages already flushed | Outcome |
| --- | ---: | --- |
| Before checkpoint | 0 | 81 ordinary, zero markers |
| After checkpoint / before BEGIN | 0 | 81 ordinary, zero markers |
| After first marker log | 0 | Exact loser restore |
| After first marker publish | 0 | Exact loser restore |
| After 40 marker logs/publications, large buffer | 0 | Exact loser restore |
| After 40 publications, capacity 8 | 32 | Exact loser restore |
| Before COMMIT after all 81 publications | 0 | Exact loser restore |
| COMMIT WAL synced, before cache refresh | 0 | All 81 markers recovered |
| Same COMMIT hook, capacity 8 | 73 | All 81 markers recovered |
| After commit/cache invalidation, before return flush | 0 | All 81 markers recovered |
| Adoption commit then same-owner reuse durable, reuse data unflushed | 0 new allocations | All 81 reused; grown tree valid |
| Adoption commit then different-owner CREATE durable, data unflushed | 0 new allocations | 2 reused, 79 markers remain |

The tests independently read disk after child termination to assert these actual
flush counts. Losers compare all original tree allocation bytes. Winners verify
complete ownership/reachability, no active marker refs, same adoption identity,
correct queries and stale old refs after reuse. Runtime all-candidate rejection
uses the **last** candidate dirty, proving no earlier candidate was logged.
API failure after 40 publications rolls back exactly. Failed COMMIT sync blocks
all further maintenance/BEGIN until recovery.

## Growth and consumption

| Workload | Result |
| --- | --- |
| Historical fixture, capacities 1/8 | Reachable 2→2; historical 81→0; NBTR 0→81; file 115→115 |
| Immediate same-owner regrowth | All 81 consumed; 81 transitions, zero BTree appends; file 115→115 |
| Mixed historical + future markers | Historical 81 adopted; 3 existing markers byte-identical; deduplicated total 84 |
| Different-owner full consumption in mixed fixture | All 81 historical markers consumed; file 120→120; pending owner cleaned normally |
| Large real fixture | 1,200 rows; 1,186 candidates adopted in one transaction; 9,772,768 WAL bytes; 1,590 file pages unchanged |
| Round 14 grow/shrink, 100 cycles | Initial/peak/final 115 pages; 8,100 retirements and 8,100 transitions; zero appends, zero new historical orphans |
| Round 13 interleaved reuse, 500 cycles | Initial/after100/after500 404 pages; 1,000 transitions, zero BTree appends; 200 candidates and 100 pending owners retained |

Adoption itself does not shrink a file. Adopted tail markers remain hole
allocator candidates; Round 11 NBTR tail truncation remains deferred. Existing
Round 13/14 stress workloads pass with the complete workspace validation.

## Persistent format matrix

| Contract | Round 15 |
| --- | --- |
| Page | v5 unchanged |
| Ordinary BTree | v3 unchanged; v1/v2 legacy behavior unchanged |
| Retired BTree | NBTR v1 unchanged |
| IndexCatalog | v9 unchanged |
| WAL container | v4 unchanged |
| WAL records | Existing v3 PageUpdate, v4 reservation, v5 transition layouts unchanged |
| Heap metadata | v5 unchanged |
| Native protocol | v1 unchanged |

## Validation and compatibility

Primary validation uses the unchanged pinned Rust 1.97.1 toolchain and offline
dependencies. Build output, test logs, mutated fuzz corpora and client fixtures
are kept under `/private/tmp`; only reviewed deterministic seeds are tracked.

```sh
cargo fmt --all -- --check
CARGO_TARGET_DIR=/private/tmp/netbadb-round15-target cargo check --workspace --all-targets --all-features --offline
CARGO_TARGET_DIR=/private/tmp/netbadb-round15-target cargo clippy --workspace --all-targets --all-features --offline -- -D warnings
CARGO_TARGET_DIR=/private/tmp/netbadb-round15-target cargo test --workspace --all-features --offline -- --show-output
CARGO_TARGET_DIR=/private/tmp/netbadb-round15-msrv cargo +1.85.0 test -p netbadb-types -p netbadb-index -p netbadb-storage --offline
```

The focused `cargo test -p netbadb-storage adoption_ --offline -- --nocapture`
suite passed 14 tests. Workspace validation passed 801 tests, with zero failures
or ignored tests. MSRV validation passed 402 tests (22 index, 376 storage, four
types), with zero failures or ignored tests. Formatting, all-target/all-feature
check and warning-denying Clippy passed. No unrelated planner MSRV let-chain
changes are made. Phase 73 planner/executor correctness is covered by the
workspace suite (including 23 planner and 91 executor tests); no performance
benchmark or wall-clock threshold is claimed.

Real clients passed: psql 17.11; psycopg 3.2.13 and SQLAlchemy 2.0.52 reflection;
Alembic 1.16.5 autogenerate/upgrade (zero baseline/final diff, two removed indexes,
one added index, one named removal, two legacy removals); Rust Protocol v1;
and Go Protocol v1 unit plus live-fixture integration tests. Client scripts use
the existing `/private/tmp/netbadb-round13-pg-venv/bin/python`, but servers are
built from this Round 15 worktree. Each Python driver probe gets its own fresh
`postgres_driver_fixture`; its published address supplies the `--dsn` argument.

```sh
CARGO_NET_OFFLINE=true NETBADB_PSQL_TARGET_DIR=/private/tmp/netbadb-round15-target /private/tmp/netbadb-round13-pg-venv/bin/python scripts/test-postgresql-psql.py
# Against separate live Round 15 postgres_driver_fixture instances:
/private/tmp/netbadb-round13-pg-venv/bin/python scripts/test-postgresql-orm.py --dsn postgresql+psycopg://netbadb@127.0.0.1:53953/test
/private/tmp/netbadb-round13-pg-venv/bin/python scripts/test-postgresql-alembic.py --dsn postgresql+psycopg://netbadb@127.0.0.1:53970/test
# From sdk/go:
GOCACHE=/private/tmp/netbadb-round15-go-cache go test ./...
GOCACHE=/private/tmp/netbadb-round15-go-cache NETBADB_GO_FIXTURE_BIN=/private/tmp/netbadb-round15-target/debug/examples/go_sdk_fixture go test -count=1 -tags=integration ./...
```

Fuzz formatting, all-target Clippy on the pinned toolchain and all-target nightly
check passed. All four targets (`btree_decode`, `index_catalog_decode`,
`wal_recovery`, `pgwire_decode`) completed 1,000 runs each without a finding.
Existing NBTR/corruption corpus files remain unchanged. The five new reviewed
WAL snapshots cover adoption winner/loser, 81-page partial flush, adoption/reuse,
and the old structural WAL horizon. Generation into three independent temporary
directories produced byte-identical complete corpora; each snapshot is checked
by its generator across three reopens. The first pgwire launch had no temporary
corpus directory; creating it and rerunning succeeded (no decoder finding).

```sh
cargo fmt --manifest-path fuzz/Cargo.toml -- --check
CARGO_TARGET_DIR=/private/tmp/netbadb-round15-fuzz-check cargo clippy --manifest-path fuzz/Cargo.toml --all-targets --offline -- -D warnings
CARGO_TARGET_DIR=/private/tmp/netbadb-round15-fuzz-nightly cargo +nightly check --manifest-path fuzz/Cargo.toml --all-targets --offline
CARGO_TARGET_DIR=/private/tmp/netbadb-round15-fuzz-check cargo run --manifest-path fuzz/Cargo.toml --bin generate_wal_corpus --offline -- /private/tmp/netbadb-round15-seeds-final/wal_recovery
# Run separately for each of the four targets above:
CARGO_NET_OFFLINE=true cargo +nightly fuzz run --target-dir /private/tmp/netbadb-round15-fuzz-run TARGET /private/tmp/netbadb-round15-fuzz-corpus/TARGET -- -runs=1000 -artifact_prefix=/private/tmp/netbadb-round15-fuzz-artifacts/
```

## Unsupported and next audit

Not implemented: raw/v1/v2 adoption, unknown/pre-owner abandoned pages, Catalog
orphan reclaim, Heap PageGeneration/RowId migration or reuse, NBTR tail
truncation, global free-list, table CREATE/DROP/ALTER or REINDEX. No automatic
startup scan rewrites ordinary orphans. Those exclusions remain intentional.

The BTree lifecycle now has future-node transactional NBTR retirement,
historical-v3 quiescent adoption, whole-index retired-owner authority, whole-owner
tail truncation and middle-hole generation transitions. Recommend **Table Schema
Lifecycle Architecture Audit** next, not direct CREATE TABLE work. It must settle
canonical schema source of truth versus manifest, mutable schema catalog,
durable TableId high-water and ColumnId lifecycle, fingerprint/version/generation
evolution, prepared/planner/cache invalidation, physical Heap/LSM/partition table
lifecycle, transactional DDL and WAL/recovery, migration/reopen, authorization,
Protocol v1/SDK schema impact and PostgreSQL catalog reflection.

Work remains isolated in the requested worktree. No reset, clean, stash, restore,
merge, cherry-pick, push or edits to other worktrees are part of this round.
