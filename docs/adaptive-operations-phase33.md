# Adaptive Operations Phase 33 — Current-state mutation work inspection

Phase 33 adds synchronous, read-only Core/storage inspection of the current
physical source for an explicitly chosen Index or Columnar candidate. It adds
no admission policy, budgets, automatic apply, Server bridge, CLI command,
manifest field, receipt field, serialization, or persistent inspection state.

A recommendation says a design may help; operator approval says to create it.
Neither describes how much current work creation requires. The storage engine
owns that evidence. `TableStatistics` and `analyzed_live_row_count` describe the
last explicit ANALYZE, and must never become current cardinality or safety
resource authority. Inspection neither reads nor refreshes those statistics.
Performing another full source scan to count work would defeat this preflight:
no inspection path scans/materializes rows, samples values, or decodes blocks.

## Storage model and complexity

`TableStorage::inspect_physical_design_source(&self)` returns
`StoragePhysicalDesignSourceInspection::{Heap, Lsm}` with typed `StorageError`
failures. It checks recovery availability and uses only current engine metadata.

Heap uses `BufferPool::validated_page_count`, which delegates file geometry to
its owning `PageManager`. This is one metadata lookup, O(1), without page reads,
cache changes, WAL flush, checkpoint, vacuum or ANALYZE. File length must be page
aligned and agree with the PageManager's current allocation boundary; the Heap
must retain its mandatory initial pages. Misalignment, truncation or out-of-band
length changes fail with the existing typed format error, never rounded geometry.

For P current pages, S bytes per page and A = 2 + 2*EMPTY_TREE_PAGE_COUNT
(currently six possible pre-backfill pages), the Heap report contains:

- storage identity and current committed `StorageVisibilityBoundary`;
- `managed_page_upper_bound = P - FIRST_MANAGED_PAGE`;
- `main_file_bytes_upper_bound = P * S`;
- `index_backfill_page_upper_bound = P + A - FIRST_MANAGED_PAGE`;
- `index_backfill_bytes_upper_bound = (P + A) * S`.

The Columnar source scan visits `FIRST_MANAGED_PAGE..P` once. Each address is
inside `[0, P*S)`. Heap pages, colocated BTree/catalog pages, and retired/free
pages all count: the production scan validates non-Heap page payloads and skips
them. This is an extent/page-unit theorem, not visible rows or device reads.
Cached pages can reduce physical I/O. No current row or slot count is claimed.

Index requires a separate narrow theorem. Production creates the empty BTree
before capturing its backfill page limit. Its metadata and initial root can add
at most two pages; reusable pages can add fewer. The pre-scan path can also
perform two high-water writes and one retired-owner detach per empty-tree page.
Catalog-wide owner uniqueness limits each detach to one changed catalog node;
`write_catalog_node_in` can append at most one continuation per call. Charging
four additional catalog pages covers even dense legacy catalog re-encoding.
The production creation array is tied to `EMPTY_TREE_PAGE_COUNT`, and backfill
fixes its page limit before inserting entries, so later split growth cannot
enlarge this pass. These bounds cover that one backfill source pass. They
exclude allocator/catalog traversals,
BTree insertion work and output writes: they are **not total mutation bounds**.
The existing construction order and durable behavior are unchanged.

LSM reports the existing `LsmMaintenanceAnchor`, committed visibility boundary,
current MemTable physical entry count and resident byte accounting, SSTable
count, total physical/version entries, and total SSTable file bytes. Entries
include old versions and tombstones; they are never labeled logical live rows.
Persistent SSTable extents and resident MemTable accounting remain separate.
MemTable bytes retain the existing `40 + encoded value length` accounting per
version, not an allocator/RSS bound and not read I/O.

The new inspection sums the current in-memory SSTable references with checked
arithmetic, O(number of current SSTables). It does not use the broad historical
`LsmInspection` L0-overlap calculation or plan compaction. The existing
post-commit/recovery MemTable byte-accounting pass now also maintains exact
physical entry and distinct clustering-key counts. Ordinary, prepared, and
group commits rebuild the same metadata; successful/ambiguous installed flush
clears it with the MemTable. Reopen rebuilds it from recovered WAL state. This
is engine runtime structure, not an inspection cache or persistent format.

A successful full LSM scan constructs cursors only over current SSTables and
resident memory. Each cursor visits its validated nonoverlapping blocks at most
once, monotonically advancing `next_block`. Those blocks lie within their
SSTable file extents. Thus current total SSTable bytes bound persistent source
bytes for an unchanged source. WAL, manifest, stream files and resident bytes
are not read-source SSTable bytes. No LSM-to-Heap work-unit conversion is claimed.

## Bounds and prerequisites

`PhysicalDesignMutationConservativeBound` is exactly `Bounded(u64)` or
`NotProven`, with no `Default`. `Bounded(N)` means successful execution of the
stated component cannot exceed N under its documented accounting definition for
the inspected state. Arithmetic overflow is a typed storage error, preserved
through `PhysicalDesignMutationWorkInspectionError::Database` and
`Error::source()`; it is never saturation or a conversion to `NotProven`.

**`NotProven` is a correctness result, not missing implementation polish.** It
means no trustworthy metadata-only upper bound is established. It never means
zero, acceptable, or permission. All output-write bounds remain `NotProven`:
Phase 33 does not prove BTree split/page-growth or Columnar encoded-output bounds,
compression, memory usage, CPU budget, elapsed time, or predicted savings.

| Production component | Source work units | Source persistent bytes | Prerequisite |
| --- | --- | --- | --- |
| Index, single Heap | Bounded Index backfill pages | Bounded Index backfill extent | None |
| Snapshot Columnar, Heap | Bounded current managed pages | Bounded current main-file extent | None |
| Incremental Columnar, Heap | Bounded current managed pages | Bounded current main-file extent | Healthy existing stream |
| Incremental Columnar, LSM | NotProven | Bounded current SSTable bytes | Healthy existing stream; no flush |
| Snapshot Columnar, LSM, empty MemTable | NotProven | Bounded current SSTable bytes | No extra flush work |
| Snapshot Columnar, LSM, nonempty MemTable | NotProven | NotProven after flush | Separate conservative LSM flush bound |

Snapshot's existing `capture_columnar_source` calls `storage.flush()` on LSM
before scanning. Inspection does not execute it. Instead,
`PhysicalColumnarMutationPrerequisiteInspection::LsmFlush` carries the exact
current maintenance anchor and the existing `LsmMaintenanceBoundInspection`.
The same storage `flush_conservative_bound` function serves maintenance and the
new source inspection. Its format formula is unchanged; it now uses exact
MemTable metadata and shared nonallocating Bloom sizing instead of walking
values, collecting keys and allocating a Bloom bitmap during inspection.

`flush_conservative_bound: None` in the storage report means exactly an empty
MemTable, where production flush produces no SSTable. It is not an ambiguous
unknown resource dimension. The Core prerequisite is then `None`. Incremental
never inherits Snapshot's flush. For nonempty Snapshot LSM, current source
extents and prospective flush output remain separate: Phase 33 deliberately
does not combine them into a claim about total post-flush scan/build cost.

## Core contract

The public methods are:

```rust,ignore
Database::inspect_physical_index_design_mutation_work(candidate)
Database::inspect_physical_columnar_design_mutation_work(&candidate, mode)
```

Both take `&self` and return the candidate, schema generation, table schema
version/fingerprint, storage identity, storage-authored source inspection and
resource bounds. Columnar additionally returns explicit mode and prerequisite.
There is no name, IndexId, ProjectionId, placement directory, evidence epoch,
advisor policy, ranking, proposal identity, or apply token.

Global visibility and a managed durable schema catalog are required. Index
remains single-storage, single-column Heap BTree only; LSM and partitioned Index
targets are rejected. Columnar supports one Heap or LSM; it also requires an
available managed Projection Catalog. Missing tables/columns, partitioned
layouts, empty/duplicate Columnar columns and unavailable catalog/source state
produce typed errors. These are structural capability checks, not admission.

Incremental requires an already enabled, available stream with matching
storage/table/fingerprint and nonzero generation. One shared Core validator is
used by Phase 27 proposal/apply and Phase 33. The storage
`inspect_change_stream_source` method reads just identity/status/generation in
O(1), without the full historical inspection's batch-counting traversal. No
stream is enabled, rebaselined, pinned, advanced, disabled or reclaimed.

Coverage and recommendation are deliberately independent. An already covering
index/projection still permits hypothetical work inspection, and no evidence
window is required. Future admission belongs **after** apply's idempotency and
coverage decisions, immediately before its existing mutation authority.

## Freshness and purity

Reports are runtime observations, never cached, persisted, serialized or used
to authorize apply. The visibility boundary is a committed horizon, not a
physical-layout token. Heap index/reclaim changes can alter geometry; LSM
compaction can alter manifest generation/count/bytes while preserving the
committed horizon and logical rows. DML or maintenance can invalidate a report
immediately. After reopen request a new report; retaining an old one has no
cross-reopen authority.

Repeated inspection of unchanged state is semantically equal. Test-only
thread-local production hooks prove zero calls to `scan_columns_with_view`,
`scan_versioned_columns_with_view`, ANALYZE, flush, buffer page reads and broad
Change Stream history inspection. Byte-for-byte fixture snapshots include
NBPC, index/schema high-waters, Heap/LSM files, WAL and streams. Visibility,
schema, catalog/projection inventory, stream state, evidence, calibration,
Adaptive evidence and scheduler state stay unchanged. LSM amplification
counters are unchanged. Inspection creates no BTree, artifact, pending intent,
receipt, scheduler action or mutation owner.

Storage proofs exercise current/stale ANALYZE independence, MemTable-only and
multiple overlapping L0 sources, versions/tombstones, multiple levels, exact
file/block extents, production flush accounting, compaction with unchanged
horizon, recovery of runtime metadata, Heap index growth, legacy catalog
expansion, tail reclaim/vacuum, malformed geometry and overflow. Core proves
every engine/mode combination,
unsupported targets, stream/catalog rejection, repeated purity, and ordinary
builds with/without prior inspection plus reopen. Full Physical Design and
workspace regressions retain Phase 20–32 behavior.

## Compatibility and Phase 34

Manifest remains v10, NBOP v5, NBMR current write v3, Native Protocol v2 and
Inspection JSON v7. PostgreSQL wire, SDK output, NBMR inode-lock handoff and
post-Begin/Outcome/whole-response uncertainty semantics are unchanged. Canonical
Schema, Schema Catalog/Mutation Journal, Coordinator, Heap, BTree/Index Catalog,
LSM manifest/SSTable/WAL, NBPC/NBPM v2, NBCM/NBCS/NBCD, Change Stream and Database
WAL formats are unchanged. Receipt archive/retention and filesystem free-space
inspection are separate, deferred work.

Phase 34 must recompute current inspection within the same execution-owner
command immediately before mutation. A hard budget may reject using a proven
upper bound. It must not approve a mutation based on an unproven required
dimension, or mistake a per-component source bound for a whole-mutation bound.
Phase 33 introduces no mutation admission policy.

Phase 36 later strengthens this current evidence without rewriting the Phase 33
contract: initial Columnar output writes become proven, and nonempty Snapshot
LSM source bytes use the existing flush-output theorem to bound the prospective
post-flush SSTable extent. Index output and LSM source work remain unproven.

Phase 37 later proves the Heap Index participant-output component from the same
metadata-only row upper bound. This historical Phase 33 result remains the
source-inspection baseline; the later proof adds no row/key/BTree scan.

## Validation and audit notes

The final frozen implementation passed all commands below. The full workspace
run passed 1,904 tests with no failures and three existing explicit fuzz-corpus
generators ignored. This includes 484 storage tests, 671 Core tests (including
31 Physical Design/stream-capability tests), 267 Server unit tests, 12
PostgreSQL integration tests, 27 Native TCP integration tests, and the daemon
SIGINT/SIGTERM regressions. Real psql compatibility passed with PostgreSQL
17.11; MSRV checks passed on Rust 1.85.0.

The implementation started at actual fetched `origin/main`
`0657666e393d1977303607326620b9c57b829624`, preserving the intervening crash
consistency audit and NBMR locking/uncertainty fixes. Validation covers the
repository matrix, including unchanged external boundaries:

```sh
cargo fmt --all -- --check
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo +1.85.0 check --workspace --all-targets
cargo +1.85.0 check -p netbadb-sdk --no-default-features --features remote
cargo check -p netbadb-sdk --no-default-features
cargo check -p netbadb-sdk --no-default-features --features remote
cargo check -p netbadb-sdk --all-features
cargo test -p netbadb-sdk --all-features
./scripts/check-generated-sdk.sh
./scripts/test-go-sdk.sh
PSQL=/opt/local/lib/pgsql/bin/psql DYLD_LIBRARY_PATH=/opt/local/lib/icu/lib \
    python3 scripts/test-postgresql-psql.py
```

From `sdk/go`, formatting is checked with `test -z "$(gofmt -l .)"`, followed
by `go test ./...` and `go vet ./...`. Focused development runs use
`cargo test -p netbadb-storage physical_design_source` and
`cargo test -p netbadb-core mutation_work`; the full workspace run includes all
storage, Physical Design, receipt migration/locking/reconciliation/uncertainty,
Native TCP, PostgreSQL Extended Query, daemon signal, operator lifecycle and
Index/Columnar/receipt exposure regressions.

Local validation uses `CARGO_INCREMENTAL=0`, `CARGO_PROFILE_DEV_DEBUG=0`,
`CARGO_PROFILE_TEST_DEBUG=0` and bounded test parallelism
(`RUST_TEST_THREADS=2` or `4`) to fit available disk space and avoid heavy
parallel-build contention. One earlier full run hit the
existing CLI operator's one-second timeout during concurrent builds; its exact
isolated retry passed. A later linker hit disk exhaustion. Only this task's
generated target directory was cleaned before rerunning with the compact
profile; no test, timeout, assertion or production behavior was weakened.

The audit required three narrow corrections beyond adding report types:
account for Index allocation/catalog growth before source backfill; maintain
MemTable counts alongside existing byte accounting and reuse nonallocating
Bloom sizing; and avoid full retained Change Stream history inspection when
only current capability is needed. Phase 26's description of source capture
was corrected to acknowledge Snapshot LSM's preexisting flush.
