# Round 14: transactional BTree orphan retirement

Base: `3ea844c58ebbccf0146a3aa973d0bfdd1755aedd`, branch
`codex/btree-orphan-round14`, worktree `../netbadb-btree-orphan-round14`.

## Mutation audit (before implementation)

The production implementation was inspected, including every mutation of
`root_page`, `first_child`, `right_child`, `next_leaf`, and separators.
The following are the complete node-removal paths in `storage/src/btree.rs`:

| Path | Allocation losing reachability | Necessary preceding updates |
| --- | --- | --- |
| `delete_balanced_in` / `preflight_merge`, leaf case | Right leaf (whether the deleted entry was in the left or right sibling) | Left leaf absorbs entries and takes right.next_leaf; parent removes the separator/right child; propagated ancestor changes complete |
| Same bottom-up loop, internal case | Right internal sibling | Left absorbs separator/children; parent removes separator/right child; propagated ancestor changes complete |
| Bottom-up root collapse | Old internal root, in addition to right sibling | Stable meta points to retained left node with decreased height |
| `normalize_delete_path`, unary root (`position == 0`) | Old unary internal root | Stable meta points to its first child with decreased height |
| Unary non-root normalization with fitting internal merge | Right internal sibling | Left absorbs children/fence, parent removes separator/right child |
| Normalization merge immediately below now-unary root | Old parent/root, additionally to right sibling | Stable meta points to merged left internal node with decreased height |

Normalization redistribution rotates a child through a parent fence; both
siblings remain reachable and no allocation retires. A nonmerging deletion
updates a leaf only. Deleting the last entry in a height-one tree retains its
empty root leaf and stable meta. There is no separate leaf removal, separator
removal, rebalance, or internal-normalization implementation outside these
paths. Splits and root growth allocate nodes and retain all old nodes.

`HeapStorage::delete_in` expires MVCC versions; it does not physically delete
BTree entries. `HeapStorage::vacuum` removes exact dead-version index entries
via `BTree::delete_in`, then deletes their heap slots in the same physical
transaction. This is the production consumer of both deletion algorithms.
Registered UPDATE/INSERT retain index candidates until vacuum; neither unlinks
BTree nodes. DROP retires a whole owner via catalog authority, not this protocol.
Catalog compaction/tail reclamation are separate existing maintenance paths.

Retirement must be appended after the complete unlink batch, never inferred by
subtracting reachable pages from owned pages. Root/meta handles are not retired.
Raw/legacy removals and historical unmarked active orphans remain excluded.

## Retirement representation and compatibility

The choice is an independent **NBTR v1** payload, not a reinterpretation of v3
reserved bits. It uses the original leaf/internal PageType envelope; PageType
alone never means an active node. Old binaries reject its unknown magic. New
active-node decoders also reject it. A marker may never occupy a meta envelope.

| Offset | Width | Meaning |
| --- | --- | --- |
| 0 | 4 | `NBTR`: explicit terminal retired state |
| 4 | 2 | version 1, little endian |
| 6 | 2 | reserved zero |
| 8 | 8 | nonzero IndexId owner |
| 16 | 8 | nonzero PageGeneration |
| 24 | 8 | nonzero PageId, must match enclosing physical page |

Exact length is 32 bytes. Unknown magic/version, reserved bits, zero identities,
truncation and trailing bytes fail. Page v5 validates the full CRC32C, which
also binds physical PageId. No child, separator, next-leaf, meta or root fields
survive in the logical payload. PageId/generation/owner remain unchanged.

| Persistent layer | Round 14 |
| --- | --- |
| Common Page | v5 unchanged; allocation checks recognize NBTR and enforce enclosing identity |
| Active BTree | v3 unchanged; v1/v2 legacy behavior unchanged |
| Retired BTree | new independent NBTR v1 payload |
| IndexCatalog | v9 unchanged |
| WAL container | v4 unchanged |
| WAL records | ordinary v3 PageUpdate, v4/tag7 reservation, v5/tag8 transition layouts unchanged |
| Heap metadata | v5 unchanged |
| Protocol | v1 unchanged |

## WAL ordering and transaction visibility

`apply_unlinks_and_retire` first runs the existing preflighted structural batch.
All its PageUpdates are logged and published before any retirement PageUpdate.
Each explicit removed PageRef is then validated against its owner and rewritten
through `retire_btree_page_in`. Already-retired pages return typed
`IndexError::AlreadyRetired`; meta pages reject. No file scan selects retirements.

The order is **leaf/sibling unlink → parent/meta unlink → marker → commit**.
Undo restores markers' original active images before undoing incoming references.
A root-collapse victim is the old internal root, never the stable meta handle.
Any failure after unlink makes the transaction rollback-required. Normalization
iterations retain the existing caller-level rollback-required rule.

Ordinary PageUpdate still rejects generation changes. Its validator additionally
enforces unchanged kind and BTree owner and rejects updates out of a marker;
only an allocation transition can reactivate it. A marker's before image is a
fully validated active v3 allocation. No WAL tag or image layout was added.

The existing synchronous single-writer lease establishes the visibility boundary.
The writer records every new retirement in `Transaction::retired_btree_pages`
before logging it. Cache construction and claim exclude those exact identities,
even if a dirty frame was stolen to disk, or the cache was invalidated/exhausted.
Other writers cannot claim during that lease. Durable commit publishes status
and invalidates the disposable cache before releasing the writer. Rollback
invalidates it before physical undo; reopen starts with no cache. CommitPending
and RollbackPending retain their original retry-only admission semantics.

## Inventory, DROP and reuse

Full-file inventory classifies active ordinary nodes, ordinary retired-owner
nodes, explicit markers, reachable raw/legacy nodes and unowned legacy nodes.
Every active root is fully traversed; a pointer to a marker fails the active
decoder instead of becoming an empty leaf. Markers need no retained owner root
or pending record; their nonzero owner must remain below catalog IndexId high-water.
They do not inflate height, live key/NULL statistics or active-orphan counts.

Candidates retain old PageRef, owner and typed `PageReuseClass`: existing
`GenerationSafeBTreeV3` whole-owner authority or new `RetiredBTreeMarker` authority.
One physical observation per slot and the PageId-keyed BTreeMap deduplicate both
sources. Selection remains lowest PageId first. Claim rereads the physical image,
checks CRC, complete payload, PageRef, owner and source, and skips pinned/dirty
frames. Exhaustion retains the existing append fallback. No second allocator or
durable free catalog exists.

Whole-owner candidates still require committed catalog retirement and a distinct
new owner. Marker candidates allow either the same or a different owner; the
before image must be explicitly retired and the new active generation must be
strictly greater and reserved durably by the same allocation transaction.
Active X/G1 → active X/G2 is still rejected. Same-owner reuse cannot preserve an
old PageRef: generation-first buffer/recovery checks reject X/G1 after reuse.
Transition rollback restores exact P/G1/X **marker** bytes, not the pre-retirement
node. Subsequent attempts burn fresh generations even after rollback.

DROP leaves committed markers as independent candidates and retires only the
remaining active-form pages through owner authority. Pending records can disappear
once ordinary owner pages reach zero, while markers remain reusable. Marker
claims do not detach a pending root or require catalog cleanup. Tail planning
explicitly excludes markers; Round 11's whole-owner suffix protocol is unchanged.

## Historical policy and tests

Historical v3 active-owner unreachable pages remain ordinary pages, never
candidates. Existing Round 9/12/13 fixtures explicitly reconstruct pre-Round14
unmarked images from retirement before-images **in test setup only**, then
checkpoint them. Their historical exclusion, corruption and orphan-only pending
assertions are retained. Production has no analogous adoption or rewrite API.

Runtime retirement tests exercise a height-5 tree at buffer capacities 1/8,
restore all 83 exact images on rollback, then use actual Heap DELETE + vacuum to
produce 81 markers and two reachable pages. DELETE alone produces zero markers.
Repeated vacuum is stable. A deliberately injected active pointer to a marker
fails. A transaction-local cache rebuild after flush cannot claim its 81 markers.

Same-owner DML tests consume markers in stages, exercise leaf and internal splits,
roll back twice to exact marker images and an empty logical tree, then commit
with strictly newer generations and correct point/range/ordered results.
Different-owner tests cover DROP rollback, source dedup, pending cleanup while
81 independent markers remain, and full later consumption. Buffer tests cover
pinned/dirty skips, all-pinned append fallback and real CREATE backfill.

## Process-crash matrix

Every hook below terminates a real subprocess without destructors; every result
is checked across three reopens. These model process loss, not torn-write repair
or machine power loss. The final full suite reruns both matrices.

| Retirement hook (actual vacuum) | Result |
| --- | --- |
| After leaf unlink / after parent unlink | Loser: original tree and exact active images |
| Before marker log / after marker log / after marker publication | Loser: original tree, zero markers |
| After unary normalization | Loser: all normalization iterations undone |
| Before COMMIT | Loser: all 81 retirement images undone |
| COMMIT appended | Complete readable record is a winner in this process-loss model; no power-loss durability claim |
| COMMIT WAL synced, before cache invalidation | Winner: smaller tree and 81 discoverable markers |

| Same-owner reuse hook | Result |
| --- | --- |
| Transition log / transition publication | Loser: exact original marker images |
| Sibling / parent / internal split publication | Loser: markers and complete original tree restored |
| Before COMMIT | Loser: all reused allocations undone |
| COMMIT WAL synced | Winner: complete grown tree and no old markers |
| Page undo / transition undo | Interrupted rollback converges to exact markers |

For both durable winners the test independently reads the file after child exit
and asserts all original BTree bytes remain unchanged: recovery must rebuild the
committed result from WAL. Reuse winner checks every recovered row/key, not just
counts. Marker transition identity tests also reject third generations, active
same-owner transitions, marker destinations, and ordinary owner/generation changes.

## Growth measurements

| Workload | Measured result |
| --- | --- |
| Round 11 tail baseline, 100 cycles | initial 3, peak 5, final 3; 200 reclaimed pages, 99 repeated meta slots |
| Round 13 interleaved baseline, 500 cycles | initial 404, after 100 and 500 cycles 404; 1,000 transitions, 0 BTree appends, 200 candidates and 100 remaining owners |
| New active orphan creation | height 5 → 1; 83 allocations → 2 reachable + 81 explicit markers; 0 new unmarked orphans; 81 candidates |
| Same-index grow/shrink, 100 cycles | initial 115, peak 115, final 115; 8,100 retirements, 8,100 transitions, 0 appends, 0 historical unmarked pages in this new fixture |

The churn fixture flushes committed dirty markers before regrowth and checkpoints
between cycles to bound test WAL size; no-checkpoint safety is tested separately
by the rollback/crash and corpus histories. These numbers measure bounded future
growth, not file truncation, compaction or wall-clock performance.

At both capacities 1/8, 30, 70 and 90 inserted rows consume respectively 25, 63
and all 81 markers, leaving 56, 18 and zero candidates. Two rollback attempts
restore the full 81-marker inventory; the winning attempt uses newer generations.
The retirement crash winners are additionally reopened and immediately regrown
through a real committed INSERT transaction, with unchanged page count.

## Deferred and next direction

Not implemented: historical/quiescent orphan adoption, Catalog orphan reclaim,
Heap/RowId reuse, raw/legacy reuse, global free-list, cross-kind allocation, marker
tail truncation, or table DDL (CREATE/DROP/ALTER TABLE). Recommend a separate
historical adoption audit next: quiescence + checkpoint + complete root proof +
durable adoption. The new grow/shrink source is bounded, so Table Schema Lifecycle
Architecture Audit is the alternative once historical backlog is measured.
Do not start Heap reuse without evidence of a larger Heap/RowId blocker.

## Validation commands and results

The pinned development toolchain is **1.97.1**. These commands were run from the
Round 14 worktree (test runs also used `-- --show-output` to retain measurements):

```bash
cargo fmt --all -- --check
CARGO_TARGET_DIR=/private/tmp/netbadb-round14-target cargo check --workspace --all-targets --all-features --offline
CARGO_TARGET_DIR=/private/tmp/netbadb-round14-target cargo clippy --workspace --all-targets --all-features --offline -- -D warnings
CARGO_TARGET_DIR=/private/tmp/netbadb-round14-target cargo test --workspace --all-features --offline
CARGO_TARGET_DIR=/private/tmp/netbadb-round14-msrv cargo +1.85.0 test -p netbadb-types -p netbadb-index -p netbadb-storage --offline
```

Formatting, workspace check and warning-denying Clippy passed. The full workspace
passed **787 tests, zero failures/ignored**, including **362 storage tests**.
The directed MSRV suite reports **388 passed** (types 4, index 22, storage 362).
The final retirement-crash → reopen → immediate-regrowth assertion additionally
passed on both toolchains:

```bash
CARGO_TARGET_DIR=/private/tmp/netbadb-round14-target cargo test -p netbadb-storage retirement_p0_real_vacuum_crash_matrix --offline -- --nocapture
CARGO_TARGET_DIR=/private/tmp/netbadb-round14-msrv cargo +1.85.0 test -p netbadb-storage retirement_p0_real_vacuum_crash_matrix --offline -- --nocapture
```

The initial sandboxed workspace run could not bind temporary client-test TCP
listeners. It was rerun with localhost access and passed. The first storage
iteration found historical-fixture/error-order failures; the final full suites
retain the original assertions and pass after explicit historical setup and
preserved generation-error ordering. No lint suppressions or assertions were
removed. The unrelated planner MSRV let-chains were not changed.

Real clients passed: psql **17.11**, psycopg **3.2.13**, SQLAlchemy **2.0.52**,
Alembic **1.16.5**, Rust Protocol v1 and Go Protocol v1. Python dependencies were
verified in the existing `/private/tmp/netbadb-round13-pg-venv`; every server
fixture was built from this Round 14 worktree, not reused from Round 13.

```bash
CARGO_NET_OFFLINE=true NETBADB_PSQL_TARGET_DIR=/private/tmp/netbadb-round14-target /private/tmp/netbadb-round13-pg-venv/bin/python scripts/test-postgresql-psql.py
# Each Python script used its own fresh postgres_driver_fixture and actual DSN:
/private/tmp/netbadb-round13-pg-venv/bin/python scripts/test-postgresql-orm.py --dsn postgresql+psycopg://netbadb@HOST:PORT/netbadb
/private/tmp/netbadb-round13-pg-venv/bin/python scripts/test-postgresql-alembic.py --dsn postgresql+psycopg://netbadb@HOST:PORT/netbadb
# From sdk/go, using the newly built fixture:
GOCACHE=/private/tmp/netbadb-round14-go-cache go test ./...
GOCACHE=/private/tmp/netbadb-round14-go-cache NETBADB_GO_FIXTURE_BIN=/private/tmp/netbadb-round14-target/debug/examples/go_sdk_fixture go test -count=1 -tags=integration ./...
```

Alembic reports zero baseline/final differences, one named index CREATE, one
named DROP, and two legacy-name removals. Workspace tests cover Phase 73
IndexNestedLoopJoin, IndexScan/RangeIndexScan, HashJoin fallback and DROP/replan.

The standalone fuzz workspace check and warning-denying Clippy also passed:

```bash
CARGO_TARGET_DIR=/private/tmp/netbadb-round14-fuzz-target cargo check --manifest-path fuzz/Cargo.toml --all-targets --offline
CARGO_TARGET_DIR=/private/tmp/netbadb-round14-fuzz-target cargo clippy --manifest-path fuzz/Cargo.toml --all-targets --offline -- -D warnings
```

Each target below completed **1,000 runs**, exit 0, no finding, using a copied
temporary corpus and this command with the corresponding TARGET:

```bash
CARGO_NET_OFFLINE=true cargo +nightly fuzz run --target-dir /private/tmp/netbadb-round14-fuzz-run TARGET /private/tmp/netbadb-round14-fuzz-corpus/TARGET -- -runs=1000 -artifact_prefix=/private/tmp/netbadb-round14-fuzz-artifacts/
```

| Target | Runs / result |
| --- | --- |
| btree_decode | 1,000 / pass |
| index_catalog_decode | 1,000 / pass |
| wal_recovery | 1,000 / pass |
| pgwire_decode | 1,000 / pass |

The generator ran twice into separate temporary directories. All **17 new seeds**
(9 marker payloads, 8 WAL snapshots) are byte-identical. All **8 Round 11** and
**12 Round 13** committed snapshots remain identical. Only these reviewed new
deterministic seeds are added; random mutations, databases, logs and artifacts
remain outside Git. Cargo-fuzz's empty artifact directories were removed.

Final review confirms clean dependency direction, synchronous core, explicit
identities/NULL behavior, unchanged persistent layouts except independent NBTR,
and no historical adoption or new durable catalog. The original main checkout
remains clean at the base. This task is committed on its isolated branch only;
no merge, cherry-pick, push, branch deletion or worktree removal is performed.
