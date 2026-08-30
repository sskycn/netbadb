# Round 11: checkpoint-gated retired BTree tail reclamation

Base: `027e79a8f2c2e2a97917599b3c3480fa3d831bfa`, isolated branch
`codex/index-tail-reclaim-round11` in `../netbadb-index-round11`.

## Pre-implementation audit

1. PageManager initializes its cached count from `file.metadata().len()/4096`.
   BufferPool owns the manager at runtime. Reclaim must additionally stat the
   file and reject disagreement, partial pages, and unexpected lengths.
2. `page.rs::PageManager::remove_trailing_page` serves rollback only. It removes
   one last page, or repairs a partially extended allocation. It is not suitable
   as the maintenance API.
3. That primitive calls `set_len` but not sync; both buffer runtime undo and
   startup recovery synchronize afterwards. Reclaim needs a strict count-based
   primitive with sync inside its contract.
4. BufferState locates frames by PageId. Rollback removes one frame and repairs
   the clock victim cursor. Factor frame removal for reuse by suffix invalidation.
5. Each frame has `pin_count`, `writer`, and `dirty`; maintenance already uses
   `ensure_unpinned`. Dirty suffix frames must never be discarded.
6. Successful checkpoint flushes every dirty frame and syncs the database file;
   clean retired frames may still be cached. Scanning can evict/load clean frames.
7. Checkpoint syncs the WAL/data, rotates to a new monotonic logical-LSN generation,
   syncs its header/directory, then removes the superseded slot. Only the selected
   new generation is recovered. No pre-checkpoint BTree update remains eligible
   for redo/undo after checkpoint returns successfully.
8. Open first recovers the selected WAL, reconciles transaction statuses, creates
   the buffer/transaction manager, then loads the catalog and active trees.
9. `read_index_catalog` checks every active/retired/pending meta PageId against
   file length before returning; it does not dereference pending roots on open.
   The ownership scanner later dereferences them and validates owner/generation.
10. After truncation the old bounds checks in `read_index_catalog` would report
    `InvalidChild` before intent recovery. Decode the root intent first and allow
    only exact covered identities through those bounds checks, finish reclaim,
    then reload the catalog normally before exposing the database.

## Chosen protocol

IndexCatalog v8 keeps the v7 layout without intent, and extends the fixed root
payload with a bounded, checksummed intent. No catalog page is allocated to store
intent. If the existing root cannot hold it, return a typed capacity error before
logging or truncating; catalog packing/general allocation is outside this slice.

Reuse the complete ownership scanner, select the longest suffix consisting of
whole retired v3 trees (including owned orphans), checkpoint internally, and scan
again. Persist intent through an ordinary catalog PageUpdate transaction. Its
commit WAL sync precedes invalidation and truncation. Finalization removes only
covered retired records and clears the root intent in one WAL transaction, with
root publication last. Any operational failure once persistence starts requires
reopen; normal mutation and other maintenance cannot continue on that instance.

Recovery accepts only the old or new length, and runs after ordinary catalog WAL
recovery but before loading active/pending tree references. Old length is scanned
again before truncation; new length needs no removed root dereference. All other
lengths are corruption. Guarantees cover abrupt process loss, not power failure.

Implementation and validation evidence follows below.

## Persistent intent and capacity

Current catalog payload version is 8; v2-v7 decode and v1 rejects. BTree remains
v3 (legacy v1/v2 preserved), Page remains v5, Heap metadata remains v5, WAL remains
container v4 / ordinary records v3 / reservation record v4. No new WAL record or
SQL command is introduced.

The 48-byte v7 header and all entry/pending widths remain. Header byte 7 is 0 on
continuations, 1 on a root without intent, or 2 on a v8 root with intent. After
its normal entries and pending records, an intent root contains:

| Offset | Encoding | Meaning |
| --- | --- | --- |
| 0 | u64 LE | old page count N |
| 8 | u64 LE | truncate from M, `3 <= M < N` |
| 16 | u64 LE | checkpoint logical WAL base LSN |
| 24 | u32 LE | nonzero covered count K |
| 28 | 4 zero bytes | reserved |
| 32 | K * 24 bytes | IndexId, meta PageId, PageGeneration, all u64 LE |

IDs must be strictly increasing and unique; refs are nonzero/generated, meta
pages lie within [M,N), generations precede the checkpoint base. The local codec
checks intrinsic shape and locally present owners; storage matches every covered
identity against the complete chain, including definitions on continuation
pages. Active, unknown, duplicate, legacy, wrong-generation and wrong-root
identities cannot authorize truncation. The Page v5 CRC32C binds the entire
payload and its physical root PageId. Truncation/extra bytes and invalid tags
are rejected. Intent is part of ordinary full-page WAL undo/redo.

The root's existing 4060-byte payload capacity is the hard limit. Required space
is v8-encoded root payload plus `32 + 24*K`. Failure returns ResourceLimit before
checkpoint, logging or file extension. No fallback partially reclaims a tree or
shrinks the chosen maximal plan. The operation preflights every affected final
catalog image, keeps the chain fixed, and does not rewrite unrelated legacy
continuations. Catalog repacking to accommodate an oversized intent is deferred.

## Eligibility and ordering

The scanner validates all Page CRCs/kinds, versioned BTree structural payloads,
owner/generation references, root reachability, orphan ownership and cross-tree
alias/overlap. Only registered retired v3 allocations enter the candidate map.
Walking backwards from EOF finds the geometric eligible suffix. If any owner
also has a page below that boundary, advance past that owner's highest suffix
page; repeat until every included owner is wholly contained. This returns the
maximal suffix under the whole-tree restriction. A raw/Heap/catalog/active/legacy
page stops the geometric scan. Corrupt or unknown ownership fails the complete
operation; it is never silently skipped.

For example, A at {3,4,7} with raw pages {5,6} remains wholly retained. If retired
B occupies {8,9}, only B can be removed. Root-unreachable v3 merge pages are
included in their owner's inventory, so a complete retired tree with orphans can
be reclaimed. Active-owner orphans remain excluded.

One synchronous `&mut Database` maintenance call performs:

1. Reuse the Core retained-handle gate and Heap checkpoint admission, then verify
   no pinned frames and authoritative file length.
2. Full ownership preflight. Empty plan returns 0, with no new WAL records,
   catalog mutation, generation reservation or checkpoint. Ordinary scanner reads
   may flush previously dirty frames through existing eviction/WAL ordering.
3. Check fixed root capacity and all finalization images.
4. Perform checkpoint internally: sync old WAL, flush/sync data, rotate/sync the
   selected WAL header and directory, remove the superseded slot.
5. Re-stat, rescan and re-plan; check clean/unpinned candidate frames again.
6. Log and commit root intent, then sync its data page. File length/pending records
   are unchanged if intent sync fails before truncation.
7. Defensively revalidate ownership and frames, invalidate every cached PageId
   in [M,N), preserving prefix frames and repairing the victim cursor.
8. `PageManager::truncate_to_page_count(N,M)` re-stats exact length, enforces
   protected lower bound, sets length and synchronizes with sync_all.
9. In one catalog transaction remove only covered retired/pending records and
   clear root intent. Publish root last, sync Commit, then sync data before return.

Only catalog pages below M appear in the WAL between checkpoint and completion;
old allocation updates are absent from the selected generation. Later allocations
reserve strictly newer logical LSN generations even if their PageIds repeat.

An error after persistence begins sets the existing writer recovery state and a
maintenance-specific BEGIN gate. Ordinary transaction failure retains its prior
read-only behavior; reclaim failure admits no new BEGIN, CREATE/DROP, DML,
checkpoint or compaction. This flag is a runtime health gate, never evidence of
checkpoint or intent durability. Reopen is the retry mechanism; no intent rollback
or in-place retry is offered on the failed instance. Pins/capacity/admission
failures before persistence remain normally retryable.

## Open and recovery states

Ordinary WAL recovery runs first. Recovered intent and finalization transactions
can affect only retained catalog pages, so their redo/undo does not require a
truncated BTree root. Then decode the catalog root/intent and complete maintenance
before ordinary registry loading. Bounds exceptions apply only to exact covered
retired refs and only with a validated intent.

| Durable state | Open behavior |
| --- | --- |
| No intent | Existing bounds/active-root validation; no reclaim guessed |
| Intent + N pages | Check checkpoint base, catalog identities and full inventory; invalidate, truncate M, sync, finalize |
| Intent + M pages | Sync length and finalize; never dereference removed refs; reject any retained page claiming a covered owner |
| Intent + any other count | Hard error, no length repair by guessing |
| Finalization loser | WAL undo restores the intent and all pending ownership; then finish it |
| Finalization winner | WAL redo retains covered-record removal and cleared intent |

No new PageId allocation is admitted while an intent is incomplete. Repeated
open/checkpoint cannot replay an obsolete generation over reused pages.

## Reuse and process-crash evidence

Actual one-row fixture, buffer capacities 1 and 8:

```text
old meta = PageRef(PageId(3), PageGeneration(16601))
new meta = PageRef(PageId(3), PageGeneration(82921))
old pageLSN = 16681
new pageLSN = 83001
old handle = GenerationMismatch
new key lookup = one correct row
```

Before append, every stale removed reference fails the file boundary instead of
hitting an old cached frame. After append, exact generation checking rejects the
old reference. New IndexId is also monotonic; no generation derives from old
pageLSN. Heap may later append into an old BTree slot: a generation-bearing BTree
ref cannot accept that different kind/allocation. This grants no Heap reclamation
permission and changes no RowId/slot-generation/MVCC representation.

All crash hooks terminate a real subprocess without destructors; each scenario
opens three times. Checkpoint between later opens exercises WAL rotation again.
These tests model process loss only, not machine/storage power loss.

| Actual crash point | Recovered pages / pending | Meaning |
| --- | --- | --- |
| TailAfterCheckpoint | 5 / 1 | No decision; original ownership retained |
| TailIntentAfterLogs | 5 / 1 | Uncommitted intent undone |
| TailIntentDurable | 3 / 0 | Open completes truncate |
| TailAfterInvalidation | 3 / 0 | Open completes truncate |
| TailAfterSetLen | 3 / 0 | New length recognized and synced |
| TailAfterFileSync | 3 / 0 | Intent drives finalization |
| TailFinalizeAfterLogs | 3 / 0 | Undo restores intent; finalization retried |
| TailFinalizeDurable | 3 / 0 | Winner redo finishes metadata |
| TailAfterCompletion | 3 / 0 | Complete |
| TailFinalizeAfterPagePublish, two catalog pages/two trees | 4 / 0 | STEAL writes continuation removal before root clear; recovery converges |

Reappend crash cases: reservation durable, first tree update logged, first page
published, before Commit, and flushed loser all recover zero new indexes. Commit
WAL sync and committed-without-data-flush recover one new index. For both winners
the parent verifies the new meta page is still an all-zero disk allocation before
open, proving recovery actually redoes the new generation. Old pending remains
removed; one baseline row survives all cases.

Failure injection separately checks intent WAL sync, truncate file sync and
finalization append failures, refusal of further mutations/maintenance, then
three successful recovery opens. Corruption tests cover partial/too-long/too-short
lengths, CRC corruption, wrong refs, active/legacy/unknown/duplicate owners,
truncated/extra intent bytes and invalid presence/geometry. Independent buffer
tests cover uncached, cached, multi-frame, pinned and dirty suffixes while
preserving the prefix. PageManager tests cover expected-count rejection,
sync-error retry, reopen and exact numeric append reuse.

## Quantified storage growth

| 100-cycle workload | Initial | Peak | Final | Retired pages discovered | Reclaimed | Pending | Repeated meta PageIds | Retained middle pages |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Tail-friendly | 3 | 5 | 3 | 200 | 200 | 0 | 99 | 0 |
| Interleaved raw tree each cycle | 3 | 404 | 404 | 200 | 0 | 100 | 0 | 200 |

Round 9's no-physical-reclaim tail-friendly test ends at 203 pages with 200
retired tree pages retained. Round 11 repeatedly releases those 200 pages.
Interleaving creates 200 raw pages plus 200 retired pages and one legitimate
extra catalog page; none are misrepresented as generally compacted. A separate
mixed test reclaims a later complete tree while retaining all pages and the
pending record of a tree split across a middle blocker.

Ordinary inspection, active names/IDs/statistics and catalog_generation stay
unchanged. Core tests exercise preserved active plans and result rows after
reclaim; existing ANALYZE/vacuum/index DML and Protocol/PG tests remain unchanged.
No maintenance SQL or PG-specific state/error is added.

## Remaining scope and Round 12

Unsupported: middle-hole reuse, arbitrary free-list, general allocator, active
orphan reclamation, catalog-page reclamation, legacy/raw reclamation, Heap page
reclamation, RowId migration, CREATE/DROP/ALTER TABLE and REINDEX. Oversized
fixed-root intents are rejected without mutation; no general catalog reshuffling
is hidden in reclaim.

Recommend Round 12 **General Free-Page Allocator Architecture audit** because
interleaved growth remains 404 pages in the measured workload. Audit durable
remaining-page inventories and generation-safe allocation for retired middle
holes, active merge orphans and catalog orphans. Do not implement table DDL yet.
If deployment evidence instead shows tail reclamation solves its main growth,
consider a Table Schema Lifecycle Architecture Audit only: canonical schema
source, TableId high-water, schema fingerprints, manifest ownership, table storage
create/drop, transaction/recovery and SDK/schema-spec compatibility.

## Final validation record

Development toolchain remains 1.97.1, MSRV remains 1.85.0. Final fixed-toolchain
workspace validation passes **747 tests, zero failures, zero ignored**, including
all doctests. Index/storage MSRV tests pass **344 tests** (index 19, storage 325).

```sh
cargo fmt --all -- --check
rustfmt --check --edition 2024 crates/netbadb-storage/src/index_tail_reclaim_tests.rs
CARGO_TARGET_DIR=/private/tmp/netbadb-round11-target cargo check --workspace --all-targets --all-features --offline
CARGO_TARGET_DIR=/private/tmp/netbadb-round11-target cargo clippy --workspace --all-targets --all-features --offline -- -D warnings
CARGO_TARGET_DIR=/private/tmp/netbadb-round11-target cargo test --workspace --all-features --offline
CARGO_TARGET_DIR=/private/tmp/netbadb-round11-msrv cargo +1.85.0 test -p netbadb-index -p netbadb-storage --offline
CARGO_TARGET_DIR=/private/tmp/netbadb-round11-fuzz-gen cargo clippy --manifest-path fuzz/Cargo.toml --all-targets --offline -- -D warnings
git diff --check
```

One explicitly attempted targeted command remains blocked by baseline code:

```sh
CARGO_TARGET_DIR=/private/tmp/netbadb-round11-msrv-core cargo +1.85.0 check -p netbadb-core --offline
```

It fails with E0658 at `crates/netbadb-planner/src/lib.rs:892` and `:904` for
pre-existing let-chains. Those planner lines and the toolchain/MSRV are untouched,
as requested. This does not affect fixed-toolchain Core/workspace validation.

An intermediate full run passed all 747 tests but its first doctest could not
find a server rlib after overlapping rebuilds in the same target directory.
The final complete run was isolated from such rebuilds and exited successfully,
including doctests. An initially overbroad BEGIN gate was narrowed to maintenance
failure after three existing read-only-after-failure tests caught the regression;
all original assertions are preserved and pass. No tests or lint checks were
removed or weakened.

All four final fuzz targets completed 1000 runs without findings:

```sh
CARGO_NET_OFFLINE=true cargo +nightly fuzz run --target-dir /private/tmp/netbadb-round11-fuzz-run TARGET /private/tmp/netbadb-round11-fuzz-corpus/TARGET -- -runs=1000 -artifact_prefix=/private/tmp/netbadb-round11-fuzz-artifacts/
```

`TARGET` is each of `btree_decode`, `index_catalog_decode`, `wal_recovery`, and
`pgwire_decode`. A missing temporary pgwire corpus directory on the first pass
was created before rerunning all four successfully. No fuzz finding occurred.
Six new catalog seeds and eight recovery snapshots were generated and verified
through production open; a second generation into a fresh directory matched all
14 committed seeds byte-for-byte:

```sh
CARGO_TARGET_DIR=/private/tmp/netbadb-round11-fuzz-gen cargo run --manifest-path fuzz/Cargo.toml --bin generate_wal_corpus --offline -- /private/tmp/netbadb-round11-generated/wal_recovery
CARGO_TARGET_DIR=/private/tmp/netbadb-round11-fuzz-gen cargo run --manifest-path fuzz/Cargo.toml --bin generate_wal_corpus --offline -- /private/tmp/netbadb-round11-regenerated/wal_recovery
```

Real-client commands pass against the final fixture build. DSN denotes each
fresh fixture's printed ephemeral localhost address; ORM and Alembic each use a
separate process/database, closed after the script:

```sh
NETBADB_PSQL_TARGET_DIR=/private/tmp/netbadb-round11-target /private/tmp/netbadb-pg-venv/bin/python scripts/test-postgresql-psql.py
/private/tmp/netbadb-pg-venv/bin/python scripts/test-postgresql-orm.py --dsn "$DSN"
/private/tmp/netbadb-pg-venv/bin/python scripts/test-postgresql-alembic.py --dsn "$DSN"
(cd sdk/go && go test ./...)
```

psql 17.11, psycopg 3.2.13, SQLAlchemy 2.0.52 and Alembic 1.16.5 pass. Alembic
baseline/final differences are both zero, with named/legacy index add/remove
covered. Rust Protocol v1 and existing planner/executor/Phase 73 regressions are
part of the full workspace suite; both Go module packages pass separately.
No PG feature, SQLSTATE or maintenance SQL is added.

Original main remains clean at the audited base. Only the isolated Round 11
worktree is changed. Build outputs, databases, generated random corpus and logs
remain outside the committed tree. This round commits its task branch only:
no merge, cherry-pick, push, or worktree removal.
