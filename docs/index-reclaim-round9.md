# Round 9: durable BTree ownership and reclaim inventory

Base: `28fd187a0957117c7edaf624456060781c6486b5`.
Branch: `codex/index-reclaim-round9`. This audit was performed against the
implementation before modifying it, not inferred from Round 8's report.

## Audit and safety decision

**Route B: PageGeneration remains required before arbitrary reuse. Physical
tail reclaim is deferred in this round.** Owner tags establish object ownership;
they are not a generation-safe physical reference protocol.

1. `netbadb-index/src/lib.rs` encodes BTree v1 as an eight-byte magic/version/
   reserved prefix: `NBTM` has root u64, height u32 and typed key specification;
   `NBTI` has separator count u32, first child u64 and full-key/right-child
   separators; `NBTL` has count u32, next leaf u64 and full-key/RowId entries.
   All fields are explicit little-endian. Storage embeds exactly one payload
   in the corresponding Page v5 kind. Page CRC32C is bound to PageId.
2. No v1 page persists a tree owner, including metadata. Neither node kind nor
   a root traversal can identify an unreachable page's original tree.
3. `BTree::insert_in` allocates leaf splits, internal splits and a new root at
   EOF; it logs all before/after images before allocating. All those encoding
   paths must inherit the expected owner, with space reserved before splitting.
4. `delete_in` / `preflight_merge` retain the left page and remove the parent's
   separator. The right page is untouched. Root collapse rewrites metadata to
   the retained child and leaves the old internal root untouched. These are
   real persistent unreachable pages, not zeroed/free pages.
5. `HeapStorage::build_index_in` reserves IndexId and registers/backfills a tree
   in one transaction. Raw `BTree::create[_in]` has no IndexId or registry.
6. Raw handles are public, Copy values, can outlive calls and be retained across
   reopen. There is no lifetime registration or revocation mechanism.
7. At the base, `BTreeHandle` contains only metadata PageId. Page magic cannot
   distinguish an old handle from a later object at that address.
8. BufferPool has no general invalidate API. Its rollback-only
   `undo_page_update(NewPage)` removes a trailing frame, adjusts the victim
   cursor, synchronizes the file and refuses pinned frames. That contract is
   not authorization to discard arbitrary dirty frames.
9. Maintenance uses `ensure_checkpoint_safe` plus `ensure_unpinned`: healthy
   runtime, idle writer, zero outstanding transaction handles and no page
   guards. Core additionally checks lazy DatabaseTransaction handles. Pending
   commit/rollback and recovery-required states fail with typed errors.
10. `PageManager::remove_trailing_page` accepts only the last non-header page,
    or the current EOF to trim a partially failed allocation. It checks offsets,
    calls set_len, then updates page_count. It does not synchronize itself,
    validate owners, invalidate buffers or implement a reclaim transaction.
11. Runtime rollback undoes WAL records in reverse order; zero before-images
    mean new allocations and remove trailing pages. Recovery validates topology
    and undoes losers, with durable rollback completion suppressing old redo.
12. Allocation assigns `PageId(page_count)` and writes zero bytes at EOF.
    Truncate followed by append therefore reuses the exact numeric PageId.
13. New pages begin without pageLSN; WAL assigns the new after-image's LSN.
    Existing rollback images restore historical pageLSNs. No physical generation
    accompanies pageLSN. Current pages must validate checksum before LSN reads.
14. Checkpoint flushes WAL, all dirty frames and the data file, then rotates the
    two WAL slots. New generation base is the previous next_lsn, preserving
    logical LSN monotonicity. A durable higher generation supersedes all old
    records; incomplete new headers retain the old generation. Successful
    rotation removes the superseded slot and synchronizes the directory.
15. Recovery selects the latest consistent durable generation and only scans
    that generation. Thus a *completed* checkpoint removes old records, but it
    does not add generations to subsequent WAL PageUpdate references. Recovery
    still uses numeric PageId, pageLSN and zero before-image topology.
16. Reclaim inventory must retain at least owner identity; retaining meta PageId
    also provides the authoritative specification and reachable-tree validation.
    Names, columns and statistics are not needed after retirement compaction.
17. One Heap owns one PageManager file, one TableDef and one IndexCatalog root.
    Its high-water allocator spans every registered index in that file. IndexId
    alone suffices for owner tags within this physical storage domain; public
    logical DROP still needs (TableId, IndexId). Other Heap files can have the
    same IndexId. Raw trees occupy the same PageId domain without an IndexId.
    Corrupt pointers can alias raw/active/retired trees; global traversal must
    reject overlaps, including meta pages and catalog/heap authority.
18. Page v5 validation checks CRC, type, slot/bounds and single-payload layout;
    node codecs reject versions, reserved bits, malformed lengths, key types,
    zero child IDs, order and trailing bytes. Storage validates child bounds,
    heights, leaf ordering/cycles and duplicate/shared traversal pages. Full
    ownership scanning must also decode orphan payloads, never just magic.

### Why owner is necessary but insufficient

Committed IndexIds never repeat, but reservation is transactional:
`build_index_in` restores high-water on rollback. A returned provisional handle
can outlive rollback; a later create can reuse both IndexId and PageId. Likewise
same-owner internal references have no allocation generation. Raw v1 handles
have neither owner nor generation, and BufferPool/WAL accept PageId alone.
Owner mismatch rejects cross-owner stale references; it does not prove those
other boundaries safe. This round adds no free-list, truncation, arbitrary reuse,
PageId migration or claims about shrinking files.

A future generation design must cover Page/header CRC, WAL before/after images
and recovery topology, buffer frame keys/invalidation, BTree handles and all
child/leaf links, catalog roots, Heap/RowId references, partition/storage
identity, rollback allocation identity, checkpoint and crash migration. It must
specify how generations remain monotonic even when allocation rolls back.

## Implemented persistent contracts

BTree v1 stays readable/writable for legacy and raw trees, non-reclaimable.
New registered trees use v2 with nonzero IndexId on meta/internal/leaf pages.
Handles carry an expected optional owner; None means *strictly v1*, not wildcard.
Split/rebalance pages inherit the owner. Removed pages keep their owner.

IndexCatalog v6 preserves v5 high-water and backward decode v2/v3/v4/v5 (v1
rejected). Active and not-yet-compacted retired definitions retain their format
identity; compaction replaces owned retirements with minimal Pending records
(IndexId, meta PageId). Pending is durable and cannot be erased by repeated
compaction. Legacy retirement retains the Round 8 permanent-abandon policy.
Historical pre-owner orphans remain permanent. Pre-Round9 permanently abandoned
pages remain outside reclaim inventory; magic is never used to invent owners.

### BTree v2 bytes and validation

| Offset | Encoding |
| --- | --- |
| 0..4 | NBTM / NBTI / NBTL magic |
| 4..6 | u16 version = 2 |
| 6..8 | u16 reserved = 0 |
| 8..16 | u64 little-endian nonzero IndexId owner |
| 16.. | unchanged v1 node body |

V1 retains its eight-byte prefix and is never silently upgraded. Metadata
contains the owner in MetaNode; leaf/internal codec APIs require an exact
expected optional owner. New-page preparation and rewrites validate that owner
as well as all traversal reads. The storage layer reserves the eight bytes
before split, merge and key preflight. Page v5 CRC continues to cover the entire
page and its expected PageId. Zero owners, wrong format/owner, truncated fields,
invalid type/tags, trailing bytes and mixed-owner traversals fail closed.
The maximum Text key that fits a future internal separator changes from 4013
(v1) to 4005 bytes (v2). Tests cover the exact boundary and rollback of a build
whose existing heap row is larger, without changing heap row limits.

### IndexCatalog v6 bytes

The header stays 48 bytes: version becomes 6; bytes 20..24 are the u32 pending
record count (must remain zero in v2-v5). The v5 root-only high-water at 40..48
is preserved exactly. Each full registration still uses a 48-byte prefix plus
optional name. Byte 37 is 0 for a legacy tree or 1 for an owned v2 tree; 38..40
remain reserved zero. The expected handle owner is derived from that record's
IndexId, avoiding a second independently mutable identity.

After all full entries, each minimal Pending record is exactly 16 bytes:
IndexId u64, meta PageId u64, both nonzero. Presence is the only reclaim state;
there is no Reclaiming state without a physical protocol. Codec and whole-chain
validation reject duplicate IDs/meta addresses across full and pending records,
invalid counts, missing/root/continuation high-water, and any ID at or above the
allocator boundary. Pending continuation IDs are checked against the root.
Compaction retains active creation order then sorts pending records by IndexId,
so a repeated call is byte-idempotent and writes no additional WAL.

## Ownership inventory and maintenance

`HeapStorage::inspect_index_reclaim`, `TableStorage::inspect_index_reclaim` and
`Database::inspect_index_reclaim(TableId)` expose an unstable admin count report.
They reject non-quiescence and pinned pages using existing typed errors; Core
also checks lazy transaction handles. LSM and range-partitioned tables return
unsupported errors. `compact_index_catalog` remains the explicit mutating
maintenance operation, using the existing full-page WAL transaction protocol.
No PostgreSQL maintenance SQL or ordinary Inspection JSON field was added.

The storage-internal scanner performs complete Page v5 checksum/layout checks
on every managed page. It decodes catalog and BTree metadata, establishes
retained registered and unregistered-v1 roots, and walks their authoritative
root/child/leaf chains with one global set of reserved and reachable pages.
It then fully decodes every node, including unreachable nodes, checking outgoing
links against the target page kind and owner. The report distinguishes active,
retired/pending, unregistered legacy and unowned legacy observations. Heap and
all catalog pages are excluded from tree ownership; unknown kinds and malformed
pages are hard errors. No physical effect is authorized by the geometric suffix.
Buffer reads may perform normal eviction/writeback; inspection does not change
logical state, discard frames, rotate WAL or truncate storage.

A root-reachable v1 tree outside the registry might be raw or a historical
abandoned registered tree: those have identical bytes. The scanner honestly
reports unregistered legacy, never guesses raw lifetime or reconstructs a lost
retirement. Unreachable v1 nodes are structurally decoded with a homogeneous
physical key type but assigned no owner. Historical pre-owner orphans remain
permanent. The old Round 8 count of 201 abandoned pages is not recovered or
silently folded into new pending ownership.

## Merge/orphan proof and deletion boundary

The deterministic 90-row, 1000-byte-key test grows a v2 tree to height 5 with
83 tree pages. After DELETE plus vacuum, height is 1 and two pages remain
reachable; all **81 unreachable pages** retain the same IndexId, including
internal and leaf pages. After DROP, catalog compaction, checkpoint and reopen,
the scan still reports all 83 retired-owned pages and 81 orphans. V1 grow/delete
also remains supported, with its orphans classified unowned rather than free.

This workload exposed a pre-existing merge-only deletion limitation: a unary
internal parent has no local sibling separator for its child. Round 9 normalizes
unary internal paths before removing an entry, merging internal siblings when
they fit or rotating one child through the parent fence. It allocates no pages,
keeps owner identity, bounds the loop by file page count, and uses the same
transactional full-page WAL. Any failure after normalization requires rollback.
A subprocess crash immediately after normalization proves undo restores the
original height/pages; a durable vacuum commit proves redo leaves the collapsed
tree and owned orphans. This is included because correct merge behavior is part
of the ownership acceptance boundary, not a speculative allocator optimization.

## Physical reclaim and PageId reuse

**Physical reclaim: deferred.** No tail truncate, free-list, middle-hole reuse,
new buffer invalidation, pageLSN reset, or PageGeneration change is implemented.
The tail-friendly workload therefore still grows linearly; it demonstrates
retained ownership, not completed space reclamation.

The `provisional_owner_is_not_a_generation_after_rollback` test constructs an
actual rolled-back CREATE handle and shows that the next successful CREATE can
have the same IndexId AND metadata PageId. That is a counterexample to treating
owner identity as a general allocation generation. Cross-owner stale-address
and strict legacy-vs-owned rejection tests pass, but no physical tail reuse
safety is claimed. Existing rollback trailing-page removal remains unchanged.

## Subprocess crash matrix

All rows below use actual child-process termination without destructors,
followed by repeated reopen; this is process-loss testing, not power-loss
simulation.

| Operation / actual hook | Required outcome |
| --- | --- |
| CREATE during backfill, before catalog log, after catalog log/publish | Loser owner/tree/catalog changes undone |
| CREATE committed without data flush | Owned tree and active registration redone |
| DROP before catalog log, after catalog log, after WAL/page flush | Index remains active after loser undo |
| DROP after commit WAL sync / after commit | Index remains retired |
| Compaction after logs, before root publication, after page publication/durable pages | Full retired definitions restored; owner inventory intact |
| Compaction after commit sync / commit return | Minimal pending records preserved (90 owned retirements) |
| Legacy compaction after new allocation and each publication boundary | New continuation undo/redo matches commit outcome |
| Pending lifecycle checkpoint after new generation durable / old generation removal | Dropped index stays dropped; pending ownership survives |
| Checkpoint then read-only ownership maintenance then abrupt exit | No data loss or lost pending ownership |
| Owned vacuum after unary normalization | Entire loser merge/normalization undone |
| Owned vacuum after durable commit | Collapsed root and owned orphans recovered |
| Tail truncate / reused committed PageId | Not implemented; no such crash-safety claim |

## Storage growth (page counts, not a timing benchmark)

Fresh fixtures contain 3 pages (metadata, catalog, initial heap). Each workload
performs 100 CREATE/DROP cycles, checkpoint and compaction/inspection per cycle.
The interleaved workload additionally creates a raw v1 tree after each DROP and
finally adds one active registered tree to demonstrate active allocation exclusion.

| Metric | Tail-friendly | Interleaved holes |
| --- | ---: | ---: |
| Cycles | 100 | 100 |
| Pages before | 3 | 3 |
| Pages after final cycle DROP/raw allocation | 203 | 403 |
| Pages after cycle maintenance | 203 | 403 |
| Final pages (after extra active tree in interleaved case) | 203 | 405 |
| Catalog pages | 1 | 1 |
| Active registrations | 0 | 1 |
| Pending reclaim records | 100 | 100 |
| next_index_id | 101 | 102 |
| Retired-owned pages discovered | 200 | 200 |
| Geometric retired suffix | 200 | 0 |
| Retained middle holes | 0 | 200 |
| Tail-reclaimed pages | 0 | 0 |
| Raw/unregistered legacy pages | 0 | 200 |
| Historical permanent orphans | 0 (fresh fixture) | 0 (fresh fixture) |

A separate 100-cycle batch-compaction regression preserves one active index:
catalog pages 2 → 1; file pages 206 → 206; 100 full retirements become 100 pending
records; 200 tree pages remain tracked. Only one obsolete catalog page is
abandoned, compared with Round 8's 201 abandoned pages. These are new fixtures,
not a recovery claim about historical Round 8 databases.

## Compatibility and deliberately deferred work

Page v5, Heap metadata v5, WAL v4/record v3, native Protocol v1, canonical schema,
PG inspection JSON and planner semantics remain unchanged. BTree v1 remains
read/write compatible; dropping it is logical only. Recreating its column
allocates a new monotonic IndexId and owned v2 tree. The new format byte costs
8 bytes per node and reduces the maximum safe Text key, as specified above.

Arbitrary hole reuse, tail reclamation, free-list, allocation generations,
physical owner revocation, background GC, and all table DDL remain deferred.
Next round: **Generation-Safe Page Reference Architecture**. It must resolve
rollback identity, same-owner references, buffers, pageLSN and retained WAL,
then define migration and crash-safe allocation before implementing reuse.

## Final validation

The exact development toolchain remains Rust 1.97.1; workspace manifests,
lockfiles, edition and MSRV are unchanged. All final commands below exited 0.
Build output, mutated fuzz corpora, client environments and logs stayed under
`/private/tmp`, outside tracked source.

```sh
cargo fmt --all -- --check
CARGO_TARGET_DIR=/private/tmp/netbadb-round9-target cargo check --workspace --all-targets --all-features --offline
CARGO_TARGET_DIR=/private/tmp/netbadb-round9-target cargo clippy --workspace --all-targets --all-features --offline -- -D warnings
CARGO_TARGET_DIR=/private/tmp/netbadb-round9-target cargo test --workspace --all-features --offline
CARGO_TARGET_DIR=/private/tmp/netbadb-round9-target cargo test -p netbadb-storage round9_tail_and_interleaved_stress --offline -- --nocapture
CARGO_TARGET_DIR=/private/tmp/netbadb-round9-msrv cargo +1.85.0 test -p netbadb-index -p netbadb-storage --offline
```

The full workspace run reports 715 passing tests and no failed/ignored tests;
storage has 297 tests and index codecs have 15. Targeted Rust 1.85.0 validation
also passes all 312 storage/index tests. A full MSRV workspace rerun is not
claimed: the previously identified unrelated planner let-chain issue is outside
this round and remains untouched. Primary workspace validation was sequential
to avoid competing Cargo builds sharing one target directory. Earlier sandbox
localhost restrictions were resolved by approved local execution; there are no
remaining primary validation blockers.

Coverage includes strict owner mismatch and legacy handles, every owned node
kind, malformed/truncated formats, full/pending owner and address collisions,
unknown orphan owners, cross-owner links, raw aliases, continuation high-water,
exact key capacity, CREATE rollback, DML, planner selection, compaction
idempotence, close/reopen, process crashes, merge/root collapse, and two 100-cycle
storage-growth workloads. The provisional rollback-identity test deliberately
records the limit of owner identity; it does not assert that such handles are
safe after rollback.

```sh
CARGO_TARGET_DIR=/private/tmp/netbadb-round9-fuzz-check cargo +nightly check --manifest-path fuzz/Cargo.toml --all-targets --offline
CARGO_TARGET_DIR=/private/tmp/netbadb-round9-fuzz-clippy cargo clippy --manifest-path fuzz/Cargo.toml --all-targets --offline -- -D warnings
```

Each of `btree_decode`, `index_catalog_decode`, `wal_recovery` and
`pgwire_decode` passed 1000 runs using the following command with TARGET replaced
by its name:

```sh
CARGO_NET_OFFLINE=true cargo +nightly fuzz run --target-dir /private/tmp/netbadb-round9-fuzz-run TARGET /private/tmp/netbadb-round9-final-corpus/TARGET -- -runs=1000 -artifact_prefix=/private/tmp/netbadb-round9-fuzz-artifacts/
```

There were no findings. Sixteen reviewed deterministic seeds cover owned v2
nodes, malformed owners, v6 pending catalogs and actual recovery images; no
mutation-generated corpus was added. The WAL harness now removes its transaction
status sidecar between iterations and fails loudly if fixture creation fails,
preventing silent no-op coverage. Its owned-tree and pending-catalog seeds are
33088 and 49728 bytes, respectively, both below its 64 KiB input bound. Successful
recovery is followed by the complete ownership inventory.

Real local clients passed against `postgres_driver_fixture` on ephemeral
loopback ports. The Python environment and compiled fixture remain temporary:

```sh
CARGO_TARGET_DIR=/private/tmp/netbadb-round9-pg-target cargo build --offline -p netbadb-server --example postgres_driver_fixture
NETBADB_PSQL_TARGET_DIR=/private/tmp/netbadb-round9-pg-target /private/tmp/netbadb-pg-venv/bin/python scripts/test-postgresql-psql.py
/private/tmp/netbadb-pg-venv/bin/python scripts/test-postgresql-orm.py --dsn postgresql+psycopg://netbadb@HOST:PORT/netbadb
/private/tmp/netbadb-pg-venv/bin/python scripts/test-postgresql-alembic.py --dsn postgresql+psycopg://netbadb@HOST:PORT/netbadb
```

HOST:PORT above represents the actual fresh fixture address supplied to each
script. Versions: psql 17.11, psycopg 3.2.13, SQLAlchemy 2.0.52 and Alembic 1.16.5.
Alembic baseline/final metadata diffs are both zero; add/remove and legacy-index
migration checks pass. Native Protocol v1 and Phase 73 planner regressions are
covered by the successful full workspace run. No PostgreSQL maintenance feature
or protocol format was introduced.

## Worktree and commit safety

Implementation is isolated in
`/Users/sam/Dev/work/sskycn/netbadb-index-round9`, branch
`codex/index-reclaim-round9`, based exactly on
`28fd187a0957117c7edaf624456060781c6486b5`. The original checkout remains clean
at that commit. The requested Round 8 sibling worktree is not present in the
local worktree inventory and was not created or modified. No reset, clean,
stash, restore, merge, cherry-pick or push was performed. Only the new worktree's
reviewed source, documentation and deterministic corpus seeds are committed;
normal Git worktree/branch bookkeeping is shared in the common repository.
