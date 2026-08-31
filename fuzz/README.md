# Storage decoding fuzzing

`wal_recovery` accepts at most 256 KiB, replaces the WAL belonging to a fresh
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
