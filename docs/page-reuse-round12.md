# Round 12: reusable page capabilities and retired-owner inventory

Base: `0be293a2775a065a532e341f6c7b3b4ca312591d`, branch
`codex/page-reuse-round12`, worktree `../netbadb-page-reuse-round12`.

## Allocation audit and safety gate (before implementation)

This round implements P0. Production middle-hole allocation remains disabled.
This is a concrete existing protocol gap, not a missing free-list: `wal.rs`
`validate_page_images` rejects nonzero before/after images with different
allocation generations. `recovery.rs` redo requires the current allocation to
match the after-image before consulting pageLSN. `buffer.rs` undo also assumes
that identity even when a logged update has not yet been published. Removing
these guards would weaken Round 10; a transition-aware recovery proof and crash
matrix must precede enabling overwrite. Checkpoint removes old history but does
not fix the first old-generation disk image versus new-generation redo mismatch.

| Component | Audited current behavior / conclusion |
| --- | --- |
| PageManager | `page.rs::allocate_page` appends a zero sentinel at cached EOF; `validated_page_count` stats exact whole-page length. No reuse inventory. |
| Rollback tail | Buffer removes a last frame, truncates, syncs, then records RollbackComplete. Incomplete planned appends have no image to undo. |
| Round 11 tail | Checkpoint + durable catalog intent + clean unpinned suffix invalidation + truncate/sync + catalog finalization. Recovery finishes intent before exposing writes. |
| Generation | `transaction.rs::reserve_page_generation` syncs a WAL reservation and uses its monotonic logical LSN; reservation has no physical undo. IndexId rollback is independent; committed retired IDs never repeat. |
| Buffer | Frames found by PageId; BTree access checks PageGeneration. Dirty writeback is physical, so rejecting a stale read alone cannot make overwrite safe. No single-hole claim/invalidation API yet. |
| BTree allocation | `btree.rs::take_page_ref` plans consecutive EOF slots; `prepare_new_page` uses zero before-images; `apply_changes` allocates before publishing. A hole needs a real old image and a distinct allocation plan. |
| WAL / pageLSN | Full-page before/after images and generation checks exist; nonzero identity transitions are currently forbidden, including during decode. Generation precedes pageLSN. |
| Checkpoint / recovery | Quiescent flush/sync and monotonic WAL rotation remove superseded history. Recovery redoes history, undoes losers, syncs undo, then records rollback completion. Interrupted cross-generation undo needs its own convergence proof. |
| Heap / RowId / MVCC | RowId contains PageId, slot and slot generation; next_version and BTree row locators carry that RowId. Slot generation is not PageGeneration. |
| Catalog | Common Page CRC but no self-generation; metadata root and next_catalog are PageId-only. Abandoned continuations cannot be reused. |

## Page capability matrix

| Page kind | Self generation | Incoming identity | Middle reuse |
| --- | --- | --- | --- |
| Registered BTree v3 | Nonzero payload generation under Page v5 CRC | Handle, root, children and leaf-next use PageRef plus owner | Retired-owner candidate only; production gate closed |
| Registered BTree v2 | Owner only, no generation | Legacy PageId refs | No |
| BTree v1 / raw | Neither owner nor generation | PageId refs | No |
| Heap | Slot generations only | RowId / MVCC lack page generation | No |
| IndexCatalog | None | Root / continuation PageId refs | No |
| Metadata page 0 | Separate fixed v5 metadata layout | Fixed identity | No |

A reusable class is necessary: proof about old BTree incoming references does
not prove safety of future Heap or Catalog references. The first concrete
`PageReuseClass` is `GenerationSafeBTreeV3`. No general `Vec<PageId>` free-list
and no hypothetical future enum variants are introduced.

## Architecture decision

Choose retired-owner-derived inventory, not a second durable free catalog.
Option A would add a second allocation truth requiring atomic reconciliation
with catalog retirement, overwrite, rollback and tail intent. Option B uses
retained committed IndexIds and full validated self-identifying pages; successful
future overwrite changes the scanned owner, undo restores it, and crash loses
only a rebuildable cache. This is sufficient inventory authority independently
of the presently missing overwrite protocol.

```text
PageManager / BufferPool (one synchronous storage owner)
    +-- append allocation                         [current, all page kinds]
    +-- reusable capability allocation            [future production gate]
           +-- retired registered BTree v3         [Round 12 inspection]
           +-- Heap                               [future identity migration]
           +-- Catalog                            [future identity migration]
```

Allocated -> RetiredButOwned -> validated class candidate -> Reusable only after
all buffer/WAL gates -> Reallocated with fresh generation. Retired legacy pages
never become reusable. Candidate bytes remain intact; reusable does not mean
zeroed. Hole reuse bounds future growth, it does not shrink the file.

## Pending ownership and persistent design

IndexCatalog v9 adds owner-only pending records. Existing v2-v8 decode preserves
legacy/root-dependent identities. Compaction first fully validates the old tree,
then converts generated retirements to owner-only records in its existing WAL
transaction. Only committed owner retirement can authorize these records.
`None` is explicitly an owner-only BTree-v3 retirement, not a missing database
value or unknown page identity. Legacy/root-dependent pending retains `Some`.

Full-file scan must decode every CRC/kind/owner/generation/payload. Owner-only
pages require v3, and do not require meta, root, or current graph connectivity.
Dormant outgoing references are not ownership claims: after partial consumption
they may be stale. Active/raw/legacy traversals still validate live graph links
and physical overlap. Owner-only pages have unknown historical reachability;
they must not be mislabeled as newly discovered merge orphans.

Compaction keeps nonempty owner-only records and may remove only those for which
a successful complete scan found zero pages. Tail maintenance uses the same
remaining owner inventory, preserving Round 11 intents and recovery. A remaining
suffix can consist entirely of old orphan pages without a live metadata root.

## Active and catalog orphans

A committed active-owner merge orphan is root-unreachable, but its owner remains
live and old WAL can still mention it. This round excludes all such pages.
Future work needs BTree-specific orphan retirement, a checkpoint/history horizon,
and evidence that no live incoming structural reference remains. A generic
owner retirement proof cannot be applied to a subset of a live tree.

Catalog continuations self-identify only their kind, not a stable owner or page
generation, and are linked by PageId. Abandoned catalog pages remain excluded;
no catalog physical reclamation or cross-kind reuse is implemented.

## Future overwrite protocol to prove

Use a storage allocation boundary with AppendOnly versus class-qualified claims.
Discover candidates once per quiescent maintenance/lazy invalidation epoch, in
lowest PageId order. Do not scan on SELECT or every split. Rebuild/invalidate on
open, DROP, compaction, rollback and tail reclaim. Cache owner + old PageRef,
remove a claim immediately, and revalidate physical bounds, owner, generation,
CRC, kind, payload and retirement before use. Skip pinned/dirty candidates or
append; never discard a dirty frame. Remove the old clean frame before publish.

Start checkpoint-gated to remove old pre-retirement history. Retain the full
old image, reserve ordinary fresh generation, initialize a complete Page v5 /
BTree v3 image, log before/after, and publish through the buffer. WAL validation
and recovery must explicitly accept only authenticated allocation transitions,
redo from old or new identity, and converge after interrupted undo. Multiple
writes to a reused page and repeated reopen must be tested. Never simply ignore
a generation mismatch or use pageLSN as authority. Rollback restores old bytes
and owner; committed reuse leaves only unconsumed owner pages in inventory.
Pending deletion waits for a zero-owner scan. Tail intent / maintenance failure
blocks every claim and normal write until reopen completes recovery.

Production tests still required: claim removal, reservation, pre-log, post-log,
post-publish, pre-commit, durable winner, undo interruption, post-undo/pre-cache;
capacities 1/8, meta-first, orphan-only, split/backfill, pin/dirty skip, fallback,
no cross-kind allocation and bounded churn. No such behavior is claimed by P0.


## IndexCatalog v9 byte contract

The 48-byte header, 56-byte entry prefix and 32-byte pending width are unchanged.
Header version is 9. Pending fields are little-endian u64 IndexId, u64 PageId,
u64 generation, u8 tag and seven zero bytes. Tag 0 retains a legacy root (nonzero
PageId, zero generation); tag 1 retains a generated root (both nonzero); tag 2
is v9 owner-only (PageId and generation both zero). Every IndexId is nonzero,
unique across the complete chain and below root next_index_id. Unknown tags,
nonzero owner-only identity/reserved fields and truncation are hard errors.

Tail intent remains 32 header bytes plus 24 bytes per covered owner. V9 permits
(IndexId,0,0) for owner-only cover, matched exactly against pending. Generated
root covers retain their original geometry/generation checks. V8 decode never
accepts zero covers; its incomplete intents still recover. At the old length,
recovery re-proves the complete remaining suffix and generation horizon. At the
new length it checks no retained page has a covered owner, then finalizes.

No Page, BTree, WAL, Heap metadata, Protocol v1, PG SQL/OID or planner format
changes. Unknown reachability is `Option<bool>::None` in unstable maintenance
allocation observations, separate from proven orphan counts. This is not a
database NULL or ordinary catalog field.

## Validation observations

The evidence below distinguishes executable P0 behavior from deferred P1 work.

### Safety gate outcome

| Gate | Evidence / status |
| --- | --- |
| Crash-safe candidate authority | Owner-only catalog transactions and validated self-identifying pages; partial and zero-owner cleanup crash tests pass |
| Partial ownership without meta/root | 83-page retired fixture, capacities 1/8; consume meta then root, enumerate all 81 remaining former orphans, then remove pending only at zero |
| Fresh generation / stale reference | Offline committed-state fixtures use durable reservations; old PageRefs reject and new PageRefs read. This is identity testing, not production reuse |
| Commit overwrite / rollback restore | **Not enabled**: unchanged WAL rejects nonzero generation transitions before publication, with and without checkpoint |
| Single-hole buffer claim | **Not implemented**: current append and tail invalidation remain unchanged; no dirty/pinned overwrite path exists |
| Old WAL versus new allocation | **Not proven for middle overwrite**: requires transition-aware validation/redo/undo; existing tail/rollback reappend proofs remain intact |

This P0 delivers an executable design, not a placeholder allocator. The inspection
DTO and owner-only maintenance are production code; no production API returns a
hole allocation. Full-file scans happen on explicit maintenance, not SELECT or
every split. There is currently no long-lived candidate cache to invalidate.

### Three measured growth groups

| Workload | File pages before / after | Retired candidates | Pending owners | Physical effect |
| --- | --- | --- | --- | --- |
| A: 100 tail-friendly cycles | 3 / 3, peak 5 | 200 discovered, 0 remain | 0 | 200 tail pages reclaimed; 99 repeated meta slots |
| B: 100 interleaved raw-tree cycles | 3 / 404 | 200 middle candidates | 100 | 0 tail pages reclaimed |
| C: another 100 registered CREATE/DROP cycles after B | 404 / 606 | 200 / 400 | 100 / 200 | 0 reused, 200 BTree pages appended, 2 additional catalog pages |

C deliberately verifies the closed production gate. The P1 bounded-growth target
is **not achieved**. Hole inspection is not file compaction. Offline partial-state
fixtures keep their 117-page file constant while simulating consumption of 83
old allocations; those 83 are never reported as real allocator reuse.

One debug-build observation on this host (not a timing assertion or benchmark):

| File pages | Candidates | Open microseconds | Explicit full-scan microseconds |
| --- | --- | --- | --- |
| 5 | 2 | 3,670 | 514 |
| 404 | 200 | 4,150 | 23,356 |
| 2,005 | 2 | 7,071 | 151,267 |

Open does not eagerly rebuild the inventory. These single samples ran with other
validation workloads; they establish actual scan cost, not latency guarantees.
They do not justify a second durable allocation catalog.

### Crash coverage

Real subprocess termination (without destructors), followed by three opens:

| Operation / actual hook | Expected state, verified |
| --- | --- |
| Owner cleanup `IndexCompactAfterLogs` | Loser preserves pending; partial owner still has one page, empty owner may linger |
| Owner cleanup `IndexCompactAfterPagesDurable` | STEAL loser restores pending after physical catalog publication |
| Owner cleanup `CommitAfterWalSync` | Winner keeps partial owner and removes only empty owner |
| Owner cleanup `IndexCompactAfterCommit` | Completed maintenance persists the same result |

Each hook runs with both partial and zero-owner fixtures: eight crashes and 24
reopens. Existing catalog conversion tests also cover before-root publication,
after allocation, partial publication, WAL durability and checkpoint rotation.
Round 11's intent/checkpoint/invalidation/set_len/sync/finalization hooks and
reappend winner/loser matrix remain in the regression suite. Owner-only remaining
suffixes can be truncated even after their old meta was consumed in a fixture.
These are process-loss tests, not power-loss guarantees. No P1 hole-overwrite
crash matrix is claimed, because ordinary WAL forbids the operation.

### Unsupported and next step

Deferred: production hole allocation and its claim/cache protocol, split/backfill
hole consumption, overwrite winner/loser recovery, bounded churn, active-orphan
reclamation, Heap PageGeneration/RowId migration, Heap/Catalog/raw/legacy reuse,
global free-list and all table DDL. No client configuration or protocol changes.

Round 13 should prove generation-transition WAL validation/recovery and implement
the BTree-v3 allocator using this existing owner inventory. Inventory does **not**
require a durable general allocator catalog. Only after P1 passes should work
move to active BTree orphan retirement or Heap/RowId architecture.


The candidate middle/suffix classification uses only class-qualified v3 pages.
The older `retired_suffix_pages` metric intentionally includes v2 retirement
geometry; it is not reused as allocation capability. A regression places two
v3 middle pages below a two-page retired v2 tail: both v3 pages remain middle
candidates, the v2 owner is blocked, and tail reclaim removes nothing.


A consumed page can later belong to a different retired owner whose complete
suffix is truncated. Ordinary EOF append may then place Heap/Catalog at that
numeric position under the existing Round 11 contract. An old dormant PageRef
cannot claim that current allocation. The scanner therefore validates intrinsic
reference shape and matching-current-generation conflicts, not current existence
of every historical target. Live active/raw incoming aliases remain errors.
A regression simulates one BTree consumption, actually reclaims the next owner's
three-page suffix, actually appends Heap data there, and confirms the first
owner's remaining meta and leaf pages remain candidates across reopen.
This does not implement direct BTree-hole-to-Heap reuse.

## Final validation record

Final source: development toolchain **1.97.1**, MSRV **1.85.0**, unchanged.
The final fixed-toolchain workspace run passes **759 tests**, zero failures and
zero ignored, including doctests. MSRV passes **360 tests**: types 4, index 21,
storage 335. Commands actually run successfully:

```sh
cargo fmt --all -- --check
rustfmt --check --edition 2024 crates/netbadb-storage/src/page_reuse_tests.rs crates/netbadb-storage/src/index_reclaim_tests.rs crates/netbadb-storage/src/index_tail_reclaim_tests.rs crates/netbadb-storage/src/page_generation_tests.rs
CARGO_TARGET_DIR=/private/tmp/netbadb-round12-target cargo check --workspace --all-targets --all-features --offline
CARGO_TARGET_DIR=/private/tmp/netbadb-round12-target cargo clippy --workspace --all-targets --all-features --offline -- -D warnings
CARGO_TARGET_DIR=/private/tmp/netbadb-round12-target cargo test --workspace --all-features --offline
CARGO_TARGET_DIR=/private/tmp/netbadb-round12-msrv cargo +1.85.0 test -p netbadb-types -p netbadb-index -p netbadb-storage --offline
cargo fmt --manifest-path fuzz/Cargo.toml -- --check
CARGO_TARGET_DIR=/private/tmp/netbadb-round12-fuzz-gen cargo check --manifest-path fuzz/Cargo.toml --all-targets --offline
CARGO_TARGET_DIR=/private/tmp/netbadb-round12-fuzz-gen cargo clippy --manifest-path fuzz/Cargo.toml --all-targets --offline -- -D warnings
git diff --check
```

The explicitly attempted Core MSRV command still fails at the two unchanged
planner let-chains (`crates/netbadb-planner/src/lib.rs:892` and `:904`, E0658):

```sh
CARGO_TARGET_DIR=/private/tmp/netbadb-round12-msrv-core cargo +1.85.0 check -p netbadb-core --offline
```

Those unrelated planner lines were not modified. An initial sandboxed workspace
run could not bind local client-test listeners (`PermissionDenied`); rerunning
with local network permission passed. The final source was then revalidated
with the complete matrix above. No test is removed, ignored or weakened. The
old orphan-count assertion is retained before owner-only conversion, followed
by explicit unknown-reachability accounting afterward. Duplicate-child rejection
was preserved, and the mixed v2-tail and dormant-ref lifetime cases gained tests.

All four final fuzz targets pass **1000 runs each**, without findings:

```sh
CARGO_NET_OFFLINE=true cargo +nightly fuzz run --target-dir /private/tmp/netbadb-round12-fuzz-run TARGET /private/tmp/netbadb-round12-fuzz-corpus/TARGET -- -runs=1000 -artifact_prefix=/private/tmp/netbadb-round12-fuzz-artifacts/
```

`TARGET` is `btree_decode`, `index_catalog_decode`, `wal_recovery`, or
`pgwire_decode`. Temporary corpora/artifacts stay outside the worktree. Nine
reviewed deterministic Round 12 seeds were generated twice and matched
byte-for-byte; the fourteen Round 11 seeds also matched unchanged:

```sh
CARGO_TARGET_DIR=/private/tmp/netbadb-round12-fuzz-gen cargo run --manifest-path fuzz/Cargo.toml --bin generate_wal_corpus --offline -- /private/tmp/netbadb-round12-generated/wal_recovery
CARGO_TARGET_DIR=/private/tmp/netbadb-round12-fuzz-gen cargo run --manifest-path fuzz/Cargo.toml --bin generate_wal_corpus --offline -- /private/tmp/netbadb-round12-regenerated/wal_recovery
```

The real-client matrix was repeated with fixture binaries rebuilt from final
source. Each ORM/Alembic invocation uses a fresh fixture process/database, its
printed ephemeral localhost DSN, and orderly shutdown after the script:

```sh
CARGO_NET_OFFLINE=true NETBADB_PSQL_TARGET_DIR=/private/tmp/netbadb-round12-clients /private/tmp/netbadb-pg-venv/bin/python scripts/test-postgresql-psql.py
/private/tmp/netbadb-pg-venv/bin/python scripts/test-postgresql-orm.py --dsn "$DSN"
/private/tmp/netbadb-pg-venv/bin/python scripts/test-postgresql-alembic.py --dsn "$DSN"
CARGO_TARGET_DIR=/private/tmp/netbadb-round12-clients cargo build --offline -p netbadb-server --example go_sdk_fixture
(cd sdk/go && GOCACHE=/private/tmp/netbadb-round12-go-cache go test ./...)
(cd sdk/go && GOCACHE=/private/tmp/netbadb-round12-go-cache NETBADB_GO_FIXTURE_BIN=/private/tmp/netbadb-round12-clients/debug/examples/go_sdk_fixture go test -count=1 -tags=integration ./...)
```

psql **17.11**, psycopg **3.2.13**, SQLAlchemy **2.0.52**, and Alembic **1.16.5**
pass. Alembic baseline/final differences are both zero; add/drop named and legacy
index paths pass. Both Go packages pass ordinary tests and real Protocol v1
plaintext/mutual-TLS integration. Rust Protocol v1, PostgreSQL, ANALYZE/vacuum,
planner/executor/Phase 73 and Core result/plan-preservation regressions are in the
full workspace suite. No wire or SQL changes are introduced.

The original main checkout remains clean at the base above. Only the requested
isolated worktree is changed. Build outputs, transient databases, randomized
corpora and logs are outside the committed tree. This round commits its branch
only: no merge, push, cherry-pick, destructive Git command or worktree removal.
