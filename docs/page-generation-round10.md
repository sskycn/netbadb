# Round 10: generation-safe page references

## Audit before implementation

Baseline: `187942cddbc1c436bd1dfce73262450ec5280266`. Workspace-wide search
covered PageId, BTreeHandle, root_page, meta_page, first_child, right_child,
next_leaf, next_catalog, RowId, pageLSN, PageUpdate, allocate_page, read_page,
write_page, buffer, frame, truncate and set_len: 2211 matches in 70 files
(including documentation). Implementation decisions below follow that audit. The implementation and
validation record follow the reference inventory.

A = transient address; B = persistent structural reference; C = WAL reference;
D = buffer identity; E = logical row locator; F = tests/debug/inspection;
G = recovery-local address. An address is scoped to one physical storage file.

| Reference / implementation | Class | Treatment / restriction |
| --- | --- | --- |
| types::PageId, Page.id; page.rs PageManager offsets/count | A | Physical file slot only; append-only except existing rollback removal |
| index::BTreeHandle.meta_page | B/A | Explicit legacy address or generation-bearing PageRef; owner independent |
| index::MetaNode.root_page | B | v3 full PageRef; v1/v2 explicitly legacy |
| InternalNode.first_child, InternalSeparator.right_child | B | v3 full PageRef |
| LeafNode.next_leaf | B | v3 optional full PageRef |
| BTree PreparedPage / PathEntry / traversal stack / leaf cycle sets | A | Keep refs through dereference; physical IDs only for geometry/alias detection |
| IndexDefinition.handle, retired full definition | B | v7 serializes complete legacy or v3 handle |
| RetiredIndexOwnership.meta_page | B | v7 preserves full ref; old entries remain legacy and unreclaimable |
| heap.rs index_catalog_root / metadata bytes 66..74 | B | Stable PageId; root is never physically removed/reused |
| IndexCatalogNode.next_catalog, CatalogSnapshot.pages | B/A | PageId; no catalog reclaim; rollback publication restored before suffix removal |
| IndexPageInventory / index_ownership observations/roots/links | F/B | Preserve generation in maintenance report and validate outgoing refs; slot sets detect overlap |
| wal.rs PageUpdate.page_id | C | Physical address plus full before/after page images; v3 payload carries identity |
| Page.page_lsn / WAL chain Lsn | C | Operation order, never allocation identity by itself; validate identity before LSN comparison |
| transaction.rs runtime undo chain | C/A | Reservation has no undo; after-image identifies allocation for undo |
| recovery.rs redo/undo/topology/loser maps | G/C | Skip completed rollbacks; reject cross-generation application; preserve physical topology checks |
| buffer.rs frame, guards, find_frame / flush / rollback | D | Physical slot lookup plus exact expected generation validation; no pinned replacement; invalidate before truncate |
| RowId.page + slot + u32 generation | E | Slot generation only; unchanged. Heap allocation reuse outside rollback remains forbidden |
| mvcc.rs TupleHeader.next_version; leaf entry/fence RowId | E/B | Same RowId limit, including stale tuple/version references |
| heap.rs PreparedInsert, scans, vacuum, relocation | A/E | Physical allocation remains append-only; no general cross-kind reuse |
| table.rs opaque Heap row handle / executor DML locator | E | No allocation management crosses storage capability boundary |
| core index maintenance / benches / fixtures | F/E | Maintenance may expose refs; normal inspection/planner/wire unchanged |
| LSM row IDs, SSTables, LSM WAL | E | Separate identity domain; no PageId-based data allocation |
| PartitionCatalog, coordinator, server metadata | B | StorageId/TableId/PartitionId and paths, not PageId; unaffected |
| fuzz page/btree/catalog/WAL and unit fixtures | F/G | Add latest and malformed refs; preserve legacy fixtures |

## Authority and chosen boundary

**Source of truth: the existing durable logical WAL position domain.**
`Transaction::reserve_page_generation` acquires the single writer, appends a
`PageGenerationReservation` in its prevLSN chain, updates lastLSN, syncs through
that record, and only then returns `PageGeneration(record.lsn.0)`. A reservation
has no payload and no physical undo. No Heap, sidecar, or second WAL high-water
is introduced. PageGeneration remains distinct from Lsn in BTree APIs.

- **Rollback:** complete WAL records are never truncated by transaction undo.
  Reservation gaps remain consumed even when all corresponding pages disappear.
  IndexId can independently roll back; it is not the allocation authority.
- **Crash:** a returned generation belongs to a complete synced record. Recovery
  may trim an incomplete unpublished final record, but cannot trim a published
  reservation. A failed sync returns no generation; a retained record consumes
  its position, and a later successful reservation is strictly greater.
- **Checkpoint:** logical LSN = base_lsn + physical_offset - 48. Rotation writes
  the old logical end as the new base and syncs the new generation before removing
  the old WAL. Offsets restart; logical positions never restart. Existing rotation
  crash tests plus allocation/reopen tests exercise both retained-WAL and rotated
  histories. Catalog compaction has no control over this authority.
- **Exhaustion:** WalManager checks addition before writing; LsnOverflow maps to
  typed IndexError::PageGenerationExhausted. It never wraps to zero or one.
  Injection at u64::MAX and failed-sync tests return no PageRef.

One sync per reservation is a deliberate correctness-first cost. No benchmark
optimization or batching protocol is claimed.

A BTree-payload boundary is sufficient for registered indexes: every new
meta/internal/leaf carries owner and generation, every structural link carries
PageRef, and the common Page v5 CRC covers all these bytes and binds them to the
physical PageId. A common Page bump would require Heap/RowId and catalog-reference
migration without improving this bounded slice. Page::allocation_generation lets
buffer/recovery validate the same payload identity; it is not a second identity.
General cross-kind or free-list reuse remains forbidden.

## Types and persistent formats

`PageId(u64)` names a slot in one physical file. `PageGeneration(u64)` names one
allocation incarnation; zero is reserved and rejected by codecs. `PageRef` is a
named struct holding both, not high-bit packing. `BTreePageRef` distinguishes
`Legacy(PageId)` from `Allocated(PageRef)`. BTreeHandle adds an independent
optional IndexId owner: None accepts only raw/legacy v1. IDs are file-scoped,
not interchangeable references across databases.

| Format | Current behavior | Compatibility / reuse claim |
| --- | --- | --- |
| Common Page | v5, unchanged CRC/layout | No common allocation-generation field |
| Heap metadata page 0 | v5, unchanged | Catalog root remains stable PageId |
| BTree v1 | No owner/generation, narrow pointers | Existing registered and raw trees read/write; raw CREATE still v1; no reuse claim |
| BTree v2 | Owner, no generation, narrow pointers | Existing trees read/write; split/merge keep v2; retired roots explicitly legacy/unreclaimable |
| BTree v3 | Owner + self-generation + full PageRef links | All new registered CREATE; generation-safe architecture, no physical reclaim implemented |
| IndexCatalog v2-v6 | Decode to explicit legacy handles | Query/DML/planner continue; catalog rewrites use v7 without upgrading tree bytes |
| IndexCatalog v7 | Full active/retired/pending refs | Root next_index_id independent, compaction preserves generations |
| IndexCatalog v1 | Rejected | No fabricated migration |
| WAL container | v4, same 48-byte header | Prior container v1-v3 remain rejected as before |
| WAL records | Ordinary records v3 unchanged; reservation tag 7 uses v4 | Decode v3/v4; reject tag 7 in v3; PageUpdate image layout unchanged |

All integer fields are explicit little endian. BTree v3 identity prefix:

```text
0..4   NBTM / NBTI / NBTL
4..6   u16 version 3
6..8   reserved zero
8..16  nonzero u64 IndexId owner
16..24 nonzero u64 PageGeneration
24..   versioned node body
```

Meta root, internal first child/every right child, and leaf next all use
`u64 PageId + u64 PageGeneration`. Only optional leaf-next allows `(0,0)` for
None; half-zero, required zero, mixed-format and truncated references fail.
The existing typed key/RowId semantics are unchanged. Node sizes account for
self-identity and every wide pointer, including an absent next link. The largest
v3 Text key is 3981 bytes with room for internal fences at arbitrary height.

Catalog v7 keeps the 48-byte header. Entry prefix grows 48 to 56 bytes, with
format tag at byte 37 (0=v1, 1=v2, 2=v3), IndexId at 40..48, generation at 48..56,
then the bounded optional name. Pending records grow 16 to 32 bytes: IndexId,
PageId, generation (each u64), tag byte (0 legacy, 1 generated), seven zero bytes.
Legacy generation fields must be zero; generated fields must be nonzero. Alias
checks compare physical slots even when expected generations differ. See the
[architecture layout tables](architecture.md#persistent-index-registry).

## BTree propagation and ownership inspection

All handle/root/child/leaf dereferences require exact expected generation through
the buffer and exact owner through the codec. Split reserves and syncs each new
PageRef before any parent, fence or leaf link can publish it. Prepared updates
keep references through final writes; no PageId-only traversal fallback exists.
Merge and root collapse retain full surviving refs. Orphans retain their own
original generation and owner; their generations need not equal the meta page's.

The maintenance report exposes `allocations` (owner, PageRef, reachable) and
full `pending` roots. The scanner validates CRC, identity and complete payloads,
all outgoing refs, and physical overlap. Even root-unreachable owned pages must
have their owning tree's format: a v2 orphan cannot masquerade as part of a v3
tree. Legacy physical counts describe geometry only, never reclaim permission.
Catalog compaction remains byte-idempotent and never drops pending generations.
Normal CatalogInspection JSON, SDK wire data, PG OIDs, SQL, and planner metadata
have no new physical identity fields.

## Buffer identity and rollback ordering

A frame is indexed physically, but its Page carries the authoritative generation.
Every generation-aware lookup validates that identity on cache hits and misses.
A mismatching pinned frame returns PagePinned; an unpinned mismatch returns
GenerationMismatch. Rejecting a stale request preserves a possibly newer dirty
frame instead of flushing or evicting it. There is no separate cached generation
that can disagree with the Page bytes.

The pool is the sole runtime PageManager owner. Its coordinated rollback path
is the only permitted removal/reappend path. Recovery operates before pool
creation. Out-of-band file replacement under a live pool is unsupported.

Rollback gets expected identity from the PageUpdate after-image, checks current
identity and pins, removes the frame **without dirty writeback**, then truncates
and syncs the file. The writer remains held until every undo is synced and
RollbackComplete is durable. A pin blocks removal; dropping it allows retry.
After rollback completes, a new allocation cannot inherit an old dirty frame.
No generic public eviction/reclaim API is added.

## WAL, recovery and pageLSN proof

PageUpdate still contains physical PageId plus full before/after images. BTree
v3 images carry owner/generation. Validation rejects zero/malformed identity,
self-generation at or beyond the PageUpdate LSN, and nonzero before/after images
that change allocation. A zero before-image denotes an allocation, not a valid
persisted Page or inherited pageLSN.

Consider A allocated at P, then rolled back, and B later allocated at P:

1. The single writer prevents B allocation while A is an unresolved loser.
   Runtime rollback syncs undo/truncation before syncing RollbackComplete and
   releasing writer ownership. Recovered losers use the same terminal ordering.
2. Analysis filters every PageUpdate belonging to completed rollback A before
   redo or undo. Retained old A WAL therefore cannot redo over B or restore A's
   before-image over B. An interrupted A rollback is recovered before any B can
   exist. Partial compound page logs still obey existing topology validation.
3. For records actually applied, a nonzero current page must have exactly the
   after-image generation **before** comparing pageLSN. Same-generation newer LSN
   can skip redo. Different-generation pages are a hard error, not evidence that
   redo should skip and not permission to overwrite. A valid CRC with u64::MAX
   pageLSN still cannot bypass this check; the page stays byte-for-byte unchanged.
4. Undo validates current allocation before restoring a before-image or removing
   a zero-before-image trailing slot. Direct tests feed retained old A undo to B
   and verify both restore/removal variants fail with GenerationMismatch.
5. A new zero slot is initialized from its own validated after-image, including
   its own PageUpdate LSN. Generation never derives from the reused slot's LSN.

This is a proof for the actual rollback/reappend protocol, not an assumption
about a future checkpoint rule. The record extension is required now to reserve
a non-rollback identity before publishing a page. A future reclaim-only
checkpoint rule would not protect today's runtime rollback reuse.

## Deterministic acceptance evidence

The same-owner test prints:

```text
owner: IndexId(1)
old: PageId(3), PageGeneration(8281)
new: PageId(3), PageGeneration(41441)
stale handle: IndexError::GenerationMismatch
new handle: succeeds
```

A separate 90-row, long-key multi-level case captures every old self-reference,
root/internal child and leaf-next ref, rolls back, recreates the same owner and
slots, rejects all old refs, and reopens/checkpoints with all 90 rows intact.
The merge test grows to height 5, collapses to height 1, and retains 83 owned
allocations including 81 orphans. DROP/compaction/checkpoint/reopen retains all
83 identities, all pending ownership, and reclaims zero pages. A downgraded
legacy orphan is rejected. A 3981-byte-key test exercises multi-level split and
reopen. Legacy v2 query/DML/split/delete/vacuum/drop/compaction/reopen retains
legacy semantics and reports one legacy-unreclaimable retired index.

The process-crash matrix passes all ten boundaries. Each case reopens three
times, validates rows and ownership/generation, reserves above every retained
pre-crash reservation, and checkpoints between the second and third reopen.
One committed baseline row survives every case.

| Actual crash boundary | Recovered registered indexes | Result |
| --- | --- | --- |
| Reservation synced, no page publish | 0 | Pass, generation consumed |
| A rollback after Abort sync | 0 | Pass, loser recovered |
| A rollback after page undo | 0 | Pass, loser recovered |
| A rollback after trailing removal | 0 | Pass, loser recovered |
| B after first PageUpdate log | 0 | Pass, partial compound loser |
| B after first page publish | 0 | Pass, loser |
| B before Commit | 0 | Pass, loser |
| B after dirty-page flush, before Commit | 0 | Pass, loser |
| B Commit after WAL sync | 1 | Pass, winner retained |
| B committed without data flush | 1 | Pass, winner redone |

Additional deterministic tests cover corrupted handle/root/child/leaf/pending
references, pinned replacement, stale dirty-frame rollback, CRC-valid generation
mismatch with ordinary/newer pageLSN, failed reservation sync, exhaustion,
v3/v7 positive round-trips, every truncated prefix, zero owners/generations,
legacy decode, alias detection, and compaction idempotence.

## Validation commands and results

The fixed development toolchain remains 1.97.1; MSRV remains 1.85.0. No manifests,
new dependencies, compiler/planner production changes, or protocol changes are
part of this work. Final results: fmt, workspace all-targets/all-features check
and Clippy passed; full workspace tests passed **726 tests, zero failures and
zero ignored**. MSRV targeted tests passed **327 tests** (types 4, index 17,
storage 306). Fuzz Clippy passed; all four fuzz targets completed 1000 runs
without findings. Real clients and Go passed again against the final build.

One sandboxed workspace rerun failed at localhost listener creation with
`PermissionDenied / Operation not permitted` in client tests. The identical
command was rerun with local-network permission and passed completely; no test
was removed or weakened, and no environmental validation blocker remains.

```sh
cargo fmt --all -- --check
CARGO_TARGET_DIR=/private/tmp/netbadb-round10-target cargo check --workspace --all-targets --all-features --offline
CARGO_TARGET_DIR=/private/tmp/netbadb-round10-target cargo clippy --workspace --all-targets --all-features --offline -- -D warnings
CARGO_TARGET_DIR=/private/tmp/netbadb-round10-target cargo test --workspace --all-features --offline
CARGO_TARGET_DIR=/private/tmp/netbadb-round10-msrv cargo +1.85.0 test -p netbadb-types -p netbadb-index -p netbadb-storage --offline
CARGO_TARGET_DIR=/private/tmp/netbadb-round10-fuzz-clippy cargo clippy --manifest-path fuzz/Cargo.toml --all-targets --offline -- -D warnings
```

The additional index/storage checks and targeted reuse/crash tests ran during
iteration. Included test source files were explicitly formatted with rustfmt.
Four nightly libFuzzer targets each run against a temporary corpus:

```sh
CARGO_NET_OFFLINE=true cargo +nightly fuzz run --target-dir /private/tmp/netbadb-round10-fuzz-run TARGET /private/tmp/netbadb-round10-final-corpus/TARGET -- -runs=1000 -artifact_prefix=/private/tmp/netbadb-round10-fuzz-artifacts/
```

`TARGET` is each of btree_decode, index_catalog_decode, wal_recovery and
pgwire_decode. Twenty-four new deterministic corpus files cover v3/v7 identities,
malformed/truncated refs, reservation records and real rollback/reappend WAL.
Legacy seeds remain present. The WAL harness bound is explicitly 128 KiB because
the 66,328-byte reuse seed otherwise exceeds 64 KiB; successful recovery executes
full generation/ownership inspection and does not silently bypass that seed.

The generator was rerun into a fresh temporary output directory; all 24 added
files matched the committed candidates byte-for-byte:

```sh
CARGO_TARGET_DIR=/private/tmp/netbadb-round10-fuzz-gen cargo run --manifest-path fuzz/Cargo.toml --bin generate_wal_corpus --offline -- /private/tmp/netbadb-round10-generated-final/wal_recovery
```

Real clients use the repository fixture, with a fresh fixture process for ORM
and Alembic. DSN below denotes its emitted ephemeral localhost address:

```sh
NETBADB_PSQL_TARGET_DIR=/private/tmp/netbadb-round10-pg-target /private/tmp/netbadb-pg-venv/bin/python scripts/test-postgresql-psql.py
/private/tmp/netbadb-pg-venv/bin/python scripts/test-postgresql-orm.py --dsn "$DSN"
/private/tmp/netbadb-pg-venv/bin/python scripts/test-postgresql-alembic.py --dsn "$DSN"
(cd sdk/go && go test ./...)
git diff --check
```

psql 17.11, psycopg 3.2.13, SQLAlchemy 2.0.52 and Alembic 1.16.5 passed. Alembic
reports baseline and final differences zero, with named/legacy index-only add
and remove operations exercised. Rust Protocol v1 and Phase 73 planner/executor
regressions run in the full workspace suite; Go Protocol v1 tests pass separately.
Round 10 adds no PG features and does not repair unrelated MSRV planner syntax.

## Reclaim readiness and remaining boundaries

**ready for checkpoint-gated tail reclaim**, as an architectural foundation for
**owned, retired BTree v3 suffixes only**. No physical reclaim is implemented or
claimed. The tested rollback/reappend path now has allocation identity, buffer,
WAL and pageLSN protection. Round 11 must still implement and prove checkpoint
admission, suffix selection, pending-root publication/removal, crash-safe
truncate ordering, and buffer invalidation as one coherent lifecycle.

Catalog root/continuation pages and Heap data pages have no PageGeneration.
RowId's u32 slot generation is not a page allocation generation. Raw/v1/v2 trees
remain legacy. These are explicit blockers to arbitrary, cross-kind or free-list
reuse, and must exclude candidates rather than be silently upgraded. They do
not grant permission to reclaim a mixed suffix. There is no current truncate
API for dropped indexes.

Deferred: retired physical reclaim, tail reclamation, free-list, middle-hole
reuse, Heap reuse, RowId migration, CREATE/DROP/ALTER TABLE and REINDEX. Round 11
should be **Checkpoint-Gated Retired Tail Reclamation**, restricted to validated
v3 owned retired suffixes, checkpoint, buffer invalidation, truncation and
reappend with fresh generation. No general allocator or table DDL.
