# Storage decoding fuzzing

Round 14 adds nine strict NBTR v1 payload seeds and eight WAL snapshots covering
real retirement plus same-owner/different-owner marker splits, winner/loser
recovery, a third generation and a corrupted nested marker before image. The
generator validates snapshot recovery and active-tree ownership; all seventeen
seeds reproduce byte-for-byte. Existing Round 11/13 snapshots remain unchanged.
`btree_decode` round-trips markers and asserts that active decoders reject them.
Markers retain the existing Page v5 CRC and WAL binary framing.

Round 15 adds five deterministic adoption snapshots: winner, loser, 81-page
partial-flush winner, adoption followed by a real same-owner split/reuse winner,
and a retained old structural WAL horizon. The generator uses synthetic valid
historical allocations, invokes the production adoption API, and verifies every
snapshot across three reopens. Real pre-Round14 merge images are exercised in
the storage process-crash tests. `NBRH` is a fuzz-only envelope containing three
little-endian u32 lengths followed by heap, selected WAL and optional older WAL
bytes; it does not change any database format or the existing `NBRF` envelope.
The 81-page image needs a larger bounded input (2 MiB); WAL record limits remain
unchanged. Successful ownership inspection additionally checks marker/historical
counts against reuse inventory, active-marker exclusion and pending identities.

`wal_recovery` accepts at most 2 MiB, replaces the WAL belonging to a fresh
minimal heap, and calls `HeapStorage::open`. That public path invokes the
crate-private recovery decoder without adding a fuzz-only production API.
Root, alternate WAL, and heap files are isolated by process ID and removed
before and after every iteration, including the transaction-status sidecar.
Fixture creation must succeed; it cannot silently turn later iterations into
no-ops. Successful recovery also invokes the full ownership scanner.

`page_decode` accepts at most one 4096-byte page, zero-pads shorter inputs,
and exercises the public Page v5 header, generation-aware slot-state, and
record decoders. Its Heap seed uses `PageId(7)` because the CRC32C binds the
expected logical page ID; a second seed is a valid page-1 IndexCatalog root.

`btree_decode` accepts at most one 4060-byte index payload plus a one-byte node
selector and directly exercises the public v1/v2/v3 Meta, Leaf, and Internal
decoders with a nullable UInt64 `IndexSpec`. Arbitrary bytes must return a node
or typed `IndexError` without panicking, unbounded allocation, or traversal.

`index_catalog_decode` accepts at most one 4060-byte `NBIC` payload and
exercises the version-9 registry decoder with backward version-2 through version-8 input.
Seeds cover active/retired registrations, invalid state and zero IDs, duplicate
IDs, truncation, root high-water, compacted catalogs, invalid high-water, and
legacy payloads. Successful decodes also canonicalize and round-trip through v9. Retirement is an in-place state, not a
DropIndex event: unknown/double-drop requests are covered by storage tests. Arbitrary counts and
bytes must remain bounded and return either a catalog node or typed error.

`coordinator_log_decode` accepts at most 64 KiB and opens it through the
version-1 coordinator scanner. Participant counts and record sizes are bounded;
malformed records must return a typed error without panicking.

`partition_catalog_decode` accepts at most 64 KiB and exercises the immutable
PartitionCatalog v1 decoder directly. Header, checksum, bounded table and
partition counts, typed integer bounds, identities, ordering, and overlap
validation must reject corrupt input without panic or unbounded allocation.

`lsm_manifest_decode`, `lsm_wal_decode`, and `lsm_sstable_decode` exercise the
three independent LSM v1 codecs. Inputs and persistent counts/lengths are
bounded before allocation; malformed checksums, keys, versions, tombstones,
and block boundaries return typed errors.

`pgwire_decode` uses one selector byte to exercise either bounded PostgreSQL v3
startup decoding or tagged frontend-message decoding. Arbitrary lengths,
counts, C strings, UTF-8, format codes, parameters, and message tags must return
a typed error without panic or unbounded allocation. The tagged path covers
Parse, Bind (including text/binary payloads and malformed parameter lengths),
Describe, Execute, Close, and every parameter/result format-code cardinality;
the pgwire unit suite supplies valid base-type and exact-width assertions.

Generate the small deterministic seed corpus and run a smoke fuzz with:

```bash
cargo run --manifest-path fuzz/Cargo.toml --bin generate_wal_corpus
cargo +nightly fuzz run wal_recovery -- -runs=1000
cargo +nightly fuzz run page_decode -- -runs=1000
cargo +nightly fuzz run btree_decode -- -runs=1000
cargo +nightly fuzz run index_catalog_decode -- -runs=1000
cargo +nightly fuzz run pgwire_decode -- -runs=1000
cargo +nightly fuzz run coordinator_log_decode -- -runs=1000
cargo +nightly fuzz run partition_catalog_decode -- -runs=1000
cargo +nightly fuzz run lsm_manifest_decode -- -runs=1000
cargo +nightly fuzz run lsm_wal_decode -- -runs=1000
cargo +nightly fuzz run lsm_sstable_decode -- -runs=1000
```

The generated WAL corpus contains empty input, a valid v4 header, Begin,
Begin+Commit, Begin+Prepare, a PageUpdate carrying Page v5 images, and a
structurally valid truncated final record. A separate legal Page v5 seed is generated for
`page_decode`. The B+Tree corpus adds empty input, valid metadata,
empty/one-entry leaves, one internal separator, and a truncated leaf. The
index-catalog corpus covers an empty registry, one entry, a next-page link,
truncation, and an impossible entry count. Do not commit generated findings or
large corpora.

Round 9 adds v2 meta/internal/leaf owners, zero/truncated owners, strict owner
mismatch checks and round trips, v6 owned/pending records with malformed counts
and ownership flags, and WAL seeds containing owned trees and pending catalog
updates. Cross-node mixed-owner graphs are exercised by deterministic storage
corruption tests; the payload fuzzer cannot infer a whole tree from one page.
Run fuzzing with a copied temporary corpus/artifact directory to avoid committing
mutation-generated findings. Only small reviewed deterministic seeds belong here.

Round 10 adds BTree v3 self-generation/root/child/leaf-next seeds, zero owner and
zero generation, truncated PageRefs, v7 active/pending references, and WAL seeds
for durable record-v4 reservations and actual rollback/reappend of the same
PageId. The generator uses public Core transactional DDL for that lifecycle.
The BTree harness retains the decoded allocation generation when re-encoding;
legacy payloads keep their exact version. Successful WAL recovery still runs
full ownership/generation inspection. Use temporary output/corpus directories;
only reviewed deterministic seeds are committed.

Round 10 raised the WAL bound to 128 KiB because its real
CREATE/rollback/re-CREATE seed contains eight full-page updates and exceeds
64 KiB. Round 13 raises it to 256 KiB for retained transition snapshots; the
generator asserts the current bound so seeds cannot be silently skipped.

Round 11 adds six v8 intent payload seeds and eight open/recovery snapshot seeds.
The generator verifies old/new-length valid snapshots converge to three pages,
and invalid partial length, unknown/active/legacy owner, wrong reference and
truncated intent snapshots fail. `wal_recovery` retains raw WAL inputs and also
accepts a bounded fixture envelope: `NBRF`, little-endian u32 heap length, u32 WAL
length, exact heap bytes, selected WAL bytes. This is only a fuzz input container,
not a persistent NetbaDB format. Fixtures contain no rows; the fresh status sidecar
is sufficient. Synthetic post-checkpoint root images use the existing Page v5
CRC32C library and the exact selected WAL base. Each successful decode/open still
runs the production scanner. Random mutations and artifacts stay in temporary
corpus directories; only deterministic `round11-*` seeds are committed.


Round 12 adds seven v9 owner-only pending payload seeds (including an explicit
v8 rejection) and two recovery snapshots with whole and partial owner inventory.
The partial fixture replaces a former meta with a new active-owner orphan using
an otherwise unused durable WAL reservation. It models a post-consumption state,
not production hole allocation. Successful recovery plus full ownership scan
must agree with candidate owner/generation identities, sorted order and pending
high-water. A lazy scan may still report typed dormant-page corruption after
open; that is not counted as successful inventory validation. Round 11 seeds
remain explicitly v8 with root-dependent records.

Round 13 adds twelve reviewed snapshots: transition winner/loser, subsequent
updates, interrupted undo, durable rollback completion, third generation,
corrupted nested before/after images,
and real production split/parent update winner plus unflushed/published losers.
The original PageUpdate/reservation/tail corpora remain inputs. Complete retained
no-checkpoint transition history requires a 256 KiB envelope bound (record bounds
are unchanged). Generation, ownership and pending scans still follow successful
open. The historical owner-only fixture now creates its active tree before
compaction while old dirty pages are ineligible, so production reuse does not
consume the pages the fixture is intended to preserve. Generate twice in temporary
directories, compare deterministic snapshots, and copy only reviewed seeds; keep
random mutations and findings outside Git.

Round 17 adds `schema_catalog_decode`, which calls the bounded production snapshot
decoder and verifies byte-for-byte deterministic re-encoding on success. Reviewed
seeds cover mixed Heap/LSM/range metadata, sparse/nominal schema, truncation, bad
CRC and an invalid TableId high-water. Run with temporary corpus/artifact paths:

```bash
cargo +nightly fuzz run schema_catalog_decode /private/tmp/netbadb-schema-corpus -- -runs=1000
```

Copy `fuzz/corpus/schema_catalog_decode` into that temporary corpus first. The
Core test `schema_catalog_tests::codec_reviewed_seed_export` deterministically
exports these seeds only when `NETBADB_SCHEMA_SEED_DIR` is explicitly set.
`btree_decode`, `index_catalog_decode`, `wal_recovery` and `pgwire_decode` remain
required companion regression targets because catalog loading precedes recovery.

## Round 18 mutation journal and coordinator

`schema_mutation_decode` validates the bounded NBSJ/NBSR v1 codec and replay order,
then requires byte-identical canonical re-encoding. Four deterministic seeds cover
reservation, intent, loser resolution and winner resolution. The coordinator corpus
adds CORD v2 schema decision, Complete and structurally incomplete-tail seeds.

Generate reviewed seeds into a temporary directory (never run mutations in the
tracked corpus):

```bash
NETBADB_ROUND18_CORPUS=/private/tmp/netbadb-round18-reviewed-corpus \
CARGO_TARGET_DIR=/private/tmp/netbadb-round18-target \
cargo test -p netbadb-core write_schema_mutation_fuzz_corpus --offline -- --ignored
cargo +nightly fuzz run schema_mutation_decode /private/tmp/netbadb-round18-fuzz-corpus/schema_mutation_decode -- -runs=1000
```

Copy only the reviewed deterministic output into the repository; use temporary
corpus and artifact directories for all seven Round 18 smoke fuzz targets.

## Round 19 SQL parser bounds

There is no existing generic SQL parser fuzz target. The parser's deterministic
`create_table_tests::parser_bounds_and_deterministic_mutations` runs 2,000 seeded
mutations plus byte/token/identifier/column, deep-parenthesis/NOT and long-cast
bounds. No new fuzz infrastructure or persistent format is introduced. Continue
all seven Round 18 decoder targets at 1,000 runs each using temporary corpus and
artifact directories; tracked storage seeds remain unchanged.

## Round 20 DROP retirement records

`schema_mutation_decode` retains the Round 18 seeds and adds deterministic valid
DROP intent, loser, retained-terminal and winner histories plus a truncated DROP
record. Replay validates exact retired TableId/version/fingerprint/StorageId/Heap
placement, checked generation/epoch, ordering, duplicate retirement and winner/
loser state constraints. `coordinator_log_decode` adds a CORD v2 schema DROP
decision with zero physical participants. The same 1,000-run command applies; no
new standalone persistent format or fuzz target is introduced.

## Round 22 retired Heap GC records

`schema_mutation_decode` also covers valid retry-only GC intent and terminal
complete histories. Replay rejects GC before retained DROP winner, complete
without intent, duplicates, truncated horizon/digest, a horizon older than the
DROP transaction and unknown tags. The existing bounded target and 1,000-run
command remain authoritative; no new fuzz-only production decoder is added.

## Round 24 Heap schema rewrite records

`schema_mutation_decode` retains the NBSJ/NBSR v1 envelope and adds reviewed
rewrite reservation, intent, loser, winner and truncated histories. The intent
contains a bounded typed operation plus exact base/target NBSC v1 fragments.
Replay verifies same TableId, distinct StorageIds, optional ADD ColumnId,
checked version/generation/epoch successors, canonical fingerprints, Single Heap
placement, allocator floors and terminal ordering. The existing 16 MiB harness
bound and 1,000-run command remain authoritative; no new persistent format or
fuzz-only decoder is introduced. Run all thirteen registered targets because
Heap rewrite also relies on the existing catalog, coordinator, page, BTree,
IndexCatalog and WAL decoders.
