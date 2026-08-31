# Round 13: explicit allocation transitions and BTree middle-hole reuse

Base: `0bbd9fa0fefda651f3aab28d5d4289b164cf09e5` (current main), branch
`codex/page-transition-round13`, worktree `../netbadb-page-transition-round13`.

## WAL blocker and design

Round 12 correctly refused to reinterpret PageUpdate as allocation. Its image
validator, redo and buffer undo require the same allocation generation. A
checkpoint alone cannot make the first old-generation disk page match a new
PageUpdate. Those guards remain unchanged. Round 13 introduces a distinct
`WalRecordKind::PageAllocationTransition`; no normal update can perform it.

`allocation_transition.rs` is the common intrinsic validation and two-state
boundary. It fully validates Page v5 CRC/layout and BTree v3 Meta/Leaf/Internal
payloads. Both images have the same physical PageId, distinct nonzero generations
(strictly increasing), nonzero distinct IndexIds, and valid complete payloads.
Leaf/internal physical keys are self-describing and decoded against supported
physical families, just as owner-only inventory does; nominal language types do
not become allocation authority. Dormant outgoing links are not followed.

Heap, IndexCatalog, raw v1, legacy v2, and zero images are rejected on either
side. Storage's registered allocator separately proves committed retirement of
X and active/in-progress registered ownership of Y. A public raw BTree handle
cannot enroll itself in reuse by supplying an owner. New index build authority
is scoped to the existing physical transaction after reserving its catalog ID.

## Persistent record

WAL container **v4 remains unchanged**. Ordinary records remain v3; generation
reservations remain v4/tag 7. Only transitions use **record v5/tag 8**:

| Offset | Width | Meaning |
| --- | --- | --- |
| 0 | 40 | Existing `WREC` header, version 5, tag 8, length, CRC32C, LSN, TxnId, prevLSN |
| 40 | 8 | Physical PageId, little endian u64 |
| 48 | 4096 | Exact before Page v5 image, including old generation/owner/pageLSN |
| 4144 | 4096 | Complete after Page v5 image, including new generation/owner/pageLSN |

Total 8240 bytes, identical bounds to a full-page PageUpdate. CRC covers header
and both images. Structural framing checks length and tag before allocation;
partial records, unknown tags, nested bad CRCs/payloads and illegal versions are
rejected. A structurally valid incomplete final record retains the existing
recovery-tail truncation rule. V3/v4 readers never silently accept tag 8.

The new generation must name an earlier reservation in **the same transaction**;
append additionally requires that reservation already synced. Reservation LSNs
remain the sole source. The before LSN must precede the transition; after LSN
must equal the transition record LSN. Rollback does not undo reservations and
checkpoint preserves the logical high-water in the existing WAL header.

## Recovery ordering and redo

Analysis retains winner/loser and durable RollbackComplete filtering. For every
physical slot with retained non-rolled-back transitions, it validates the
complete consecutive full-image lineage before writing any page. Consecutive
before/after images must agree exactly. If the disk is an incarnation installed
by transition T, its decoded identity and LSN certify that slot's prefix before
T as superseded. Only that certified prefix is excluded from redo. There is no
`generation mismatch => skip` in PageUpdate or transition execution.

This matters without a checkpoint: old committed X PageUpdates still exist when
the disk already contains Y. They cannot touch Y. A third disk generation or a
broken lineage is an error, not a reason to overwrite or skip. Multiple retained
transitions X→Y→Z are covered, including a last loser returning to Y.

| Disk state at transition redo | Action |
| --- | --- |
| Old PageRef and owner | Require exact before bytes, then install after; old LSN cannot skip the allocation boundary |
| New PageRef and owner | Validate LSN ≥ transition LSN; at equal LSN require exact after bytes; already redone |
| Neither identity, invalid CRC/payload, or impossible new LSN | Hard error |

Redo remains ascending LSN, transition before subsequent Y updates. Ordinary
updates still validate current generation **before** their pageLSN comparison.
Full WAL sync precedes recovery page writes. Metadata exclusion, append topology,
prepared decisions and winner-depends-on-loser rejection include transitions.

## Undo and retry

Losers traverse prevLSN in global descending LSN. Parent/sibling and all Y
updates are undone before Y→X. A transition encountering Y restores the complete
before image, including the original pageLSN; exact X is an idempotent no-op;
any other identity is an error. No synthetic retired pageLSN is generated.

Runtime Abort is synced before undo; all restored pages sync before
RollbackComplete. A retry can occur after the transition was already undone.
Transaction undo therefore first certifies exact restored old images and excludes
only that transaction's later updates to those explicitly certified new
allocations. Normal update undo still rejects generation mismatch. A regression
pins an earlier page, forces failure after transition undo, then retries safely.
CommitPending and RollbackPending retain their original retry-only lifecycle.

## Allocator and cache

`IndexPageInventory::reusable_btree_pages` is shared by inspection and the internal
allocator; the hot path does not call the admin inspection API. The cache contains
old PageRef + retired IndexId in a BTreeMap ordered by lowest PageId, plus retired
owner authority and root-dependency state. It is disposable, not a free catalog.
An empty cache is retained, avoiding repeated empty full scans.

The first claim after invalidation derives inventory from catalog plus validated
on-disk ownership. Every claim checks the current file slot, exact old identity,
owner, CRC and complete v3 payload. Owner membership is checked against the
catalog-derived authority, invalidated at each ownership-changing boundary.
Revalidation reads one page; no file scan per backfill/split allocation occurs.
Selection/removal is ordered; temporarily blocked candidates remain available.

A retirement may still be a full entry or a pre-v9 root-dependent pending record.
After selecting a usable page of that owner, the same allocation transaction
first converts its retirement to v9 owner-only pending. This happens once per
owner, before any transition can remove its meta/root, and rolls back with the
allocation. A scan with only pinned/dirty candidates does not rewrite catalog.
No new Catalog version or durable allocation metadata is needed.

Cache invalidation occurs on committed DROP, compaction, rollback and tail
maintenance; reopen starts empty and builds lazily. Successful reuse removes
only the claimed candidate. Uncommitted DROP is excluded using the still-active
committed registry. Tail intent/recovery-required state prevents normal writes.

## Buffer and allocation publication

1. Select and locally revalidate an old retired candidate; skip pinned or dirty
   frames without discarding them.
2. Reserve and sync a fresh generation, then construct a complete after image.
3. Log the explicit transition in the existing transaction chain. BTree compound
   publication syncs its logs before publishing any allocations.
4. Recheck old disk bytes and frame eligibility, remove a clean unpinned old
   frame, and install a new dirty frame with the new allocation identity.
5. Only subsequent mutations of that new identity use PageUpdate.

No usable candidate means the original zero-before-image EOF append path. Raw
and legacy trees always use that append path; Heap and Catalog never enter this
allocator. CREATE meta/root, leaf/internal splits, backfill and later registered
DML all use the same allocation function. Lowest PageId applies to both middle
and tail candidates; explicit tail maintenance retains priority when called
first, while ordinary allocations may consume eligible tail pages before it.

## Partial ownership and exact outcomes

At capacities 1/8, real CREATE rollback twice and then commit reuses the same
physical meta slot with three distinct fresh generations. Old bytes/owner/LSN
are exactly restored on each rollback; committed old references reject and new
references succeed. Example P=3, X=1/G=8281, Y=3/G=157481 (deterministic fixture).

A 90-row text backfill consumes **83 holes**, reaches height ≥3, and leaves the
**117-page** file unchanged at both capacities. Rollback restores all 83 exact
images. Only the following allocation after exhaustion appends. Real Heap DML
also consumes holes on index split; rollback restores both links and old pages.

A second fixture collapses an active tree, yielding **81 active-owner orphans**.
They are excluded while active. After DROP, production reuse consumes meta/root
first, leaving 81 owner-only candidates without live historical roots; later
DML consumes them all. Pending ownership remains until a complete scan proves
zero remaining pages; compaction then removes it. Reuse does not invent an
active-orphan retirement policy.

## Process-crash matrix

P0 has six real subprocess exits with three reopens each; production backfill has
nine exits with three reopens each. The production matrix uses 83 reusable pages:

| Actual hook | Outcome |
| --- | --- |
| TransitionAfterLog | Loser restores exact old allocation |
| TransitionAfterPublish | Loser restores exact old allocation |
| BTreeAfterSiblingUpdate | Split loser restores links and old allocation |
| BTreeAfterParentUpdate | Split loser restores parent and old allocation |
| BTreeAfterInternalSplit | Internal-split loser restores complete tree and holes |
| GenerationReuseBeforeCommit | Backfill loser restores all 83 exact images |
| CommitAfterWalSync | Winner restores complete new index, all generations fresh |
| RollbackAfterPageUndo | Interrupted rollback converges to old images |
| TransitionAfterUndo | Repeated recovery remains at old images |

For the strongest winner, all 83 physical disk images are asserted byte-identical
to X immediately after child exit: Y's commit WAL is durable, Y data is unflushed.
Recovery installs Y's complete height≥3 index; actual index lookups verify all
90 recovered key/RowId pairs on every reopen. Losers
with many Y PageUpdates converge to byte-exact X. Additional deterministic tests
interrupt redo/undo after 1/3/5 physical operations across old, new and updated
disk states, and repeat recovery three times. These are process-loss tests, not
claims about torn-write repair or machine power loss.

## Growth measurements

| Workload | File pages | Evidence |
| --- | --- | --- |
| 100 tail-friendly cycles | 3 → peak 5 → 3 | 200 pages truncated, 99 repeated meta slots |
| 100 ordinary interleaved raw-tree cycles | 3 → 207 | 200 permanent raw pages, 4 retained middle BTree pages; 97 consecutive meta-slot reuses |
| Historical Round 12 hole setup | 404 | 200 middle candidates, 100 owners; real pins hold holes during fixture construction |
| Same post-hole CREATE/DROP + compaction workload, 100 cycles | 404 → 404 | Round 12's corresponding result was 404 → 606 |
| Extended post-hole churn, 500 cycles | 404 → 404 | 1000 transition records, zero BTree appends; 200 candidates, 100 owners with pages, zero empty pending owners |

Pins are released before the last two workloads, which use the ordinary
capacity-8 allocator. No allocation bypass is used. Some older codec/maintenance
fixtures also use real pins while constructing historical append layouts, keeping
their existing bounds, crash and ownership assertions intact. This bounds future
growth; it does not claim to shrink existing middle holes.

## Compatibility and remaining boundaries

Page **v5**, BTree **v3**, IndexCatalog **v9**, Heap metadata **v5**, WAL container
**v4**, Protocol v1 and PG/SQL/planner surfaces are unchanged. Old WAL corpus,
reservations, tail-intent and rollback-completion paths remain covered. Only the
transition record extends WAL with v5/tag 8.

Deferred: Heap hole reuse and PageGeneration/RowId migration, Catalog reuse,
active orphan retirement, raw/legacy reuse, general free-list, cross-kind reuse,
SQL/table DDL and CREATE TABLE. Next: **Active BTree Orphan Retirement**, establishing
a durable merge/root-collapse retirement boundary before admitting active-owner
unreachable pages to this same BTree-v3 capability inventory.

## Final validation

The authoritative toolchain is **1.97.1**. The following commands passed from
this task worktree (the test invocation also used `-- --show-output` to retain
successful crash/stress measurements):

```bash
cargo fmt --all -- --check
CARGO_TARGET_DIR=/private/tmp/netbadb-round13-target cargo check --workspace --all-targets --all-features --offline
CARGO_TARGET_DIR=/private/tmp/netbadb-round13-target cargo clippy --workspace --all-targets --all-features --offline -- -D warnings
CARGO_TARGET_DIR=/private/tmp/netbadb-round13-target cargo test --workspace --all-features --offline
CARGO_TARGET_DIR=/private/tmp/netbadb-round13-msrv cargo +1.85.0 test -p netbadb-types -p netbadb-index -p netbadb-storage --offline
```

The complete workspace has **775 passed, 0 failed, 0 ignored**, including **351
storage tests**. The 1.85 directed run has **376 passed**: types 4, index 21,
storage 351. Final `cargo test -p netbadb-storage transition_ --offline` reruns
on both toolchains pass all 17 selected tests, including the final row-by-row
index assertions. The new codec assertions also explicitly reject valid raw-v1 and
owned-v2 images on either side, unsynced reservation authority, tag 8 under old
record versions, and bad nested images with a recomputed outer record CRC.

The separate command below remains blocked by the existing planner let-chains
at `crates/netbadb-planner/src/lib.rs:892` and `:904`, both E0658 on 1.85.
Planner and the toolchain/MSRV declarations were not changed:

```bash
CARGO_TARGET_DIR=/private/tmp/netbadb-round13-msrv cargo +1.85.0 test -p netbadb-core --offline
```

Real client regressions passed with psql **17.11**, psycopg **3.2.13**,
SQLAlchemy **2.0.52** and Alembic **1.16.5**. The exact pinned requirements were
installed into `/private/tmp/netbadb-round13-pg-venv`. The psql command was:

```bash
CARGO_NET_OFFLINE=true NETBADB_PSQL_TARGET_DIR=/private/tmp/netbadb-round13-clients /private/tmp/netbadb-round13-pg-venv/bin/python scripts/test-postgresql-psql.py
```

`test-postgresql-orm.py` and `test-postgresql-alembic.py` each ran with `--dsn`
against a fresh local `postgres_driver_fixture` process built from this worktree.
The ORM smoke includes prepared/binary queries and cross-client index visibility.
Alembic reported zero baseline/final differences, one named CREATE, one named
DROP, and two legacy-name removals. Rust Protocol v1 and Phase 73 index-join
planning/execution and DROP/replan regressions passed in the workspace suite.
From `sdk/go`, both commands passed against the real worktree fixture:

```bash
GOCACHE=/private/tmp/netbadb-round13-go-cache go test ./...
GOCACHE=/private/tmp/netbadb-round13-go-cache NETBADB_GO_FIXTURE_BIN=/private/tmp/netbadb-round13-clients/debug/examples/go_sdk_fixture go test -count=1 -tags=integration ./...
```

The standalone fuzz workspace passed `cargo check --all-targets --offline` and
`cargo clippy --all-targets --offline -- -D warnings`. Each target below completed
**1000 runs**, exit 0, without a finding:

| Target | Runs |
| --- | --- |
| `btree_decode` | 1000 |
| `index_catalog_decode` | 1000 |
| `wal_recovery` | 1000, rerun with all twelve final transition seeds |
| `pgwire_decode` | 1000 |

Each used the following command with its target name and a copied temporary
corpus (PG startup/query/sync seeds were initialized in its temporary directory):

```bash
CARGO_NET_OFFLINE=true cargo +nightly fuzz run --target-dir /private/tmp/netbadb-round13-fuzz-run TARGET /private/tmp/netbadb-round13-final-corpus/TARGET -- -runs=1000 -artifact_prefix=/private/tmp/netbadb-round13-fuzz-artifacts/
```

The generator ran twice into temporary directories. All **12 Round 13 snapshots
are byte-identical** between runs; all **8 Round 11 snapshots match the committed
originals**. Original legacy/Round 12 corpus files remain unchanged and were
included in regression inputs. Only the twelve reviewed Round 13 seeds are added;
random mutations, artifacts, databases, logs and build outputs are outside Git.
The fuzz envelope is now 256 KiB to retain full no-checkpoint heap+WAL snapshots;
the persistent WAL record maximum remains 8240 bytes.

The implementation was gated in order: transition codec/redo/undo, no-checkpoint
P0 crash proofs and a 1000-run WAL fuzz pass preceded production allocator wiring.
Final diff review includes unchanged ordinary-update generation guards and the
old append path, source/corpus bounds, ownership authorization, and generated
fixture changes required to preserve historical layouts under the new allocator.
The original checkout remains clean at the base commit. Per this task's explicit
instruction, completion commits only this branch; no merge, cherry-pick, push or
worktree removal is performed.
