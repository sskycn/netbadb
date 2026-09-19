# Adaptive Operations Phase 37 — Heap Index participant output bounds

Phase 37 proves the application-level output component of one successful
Global Physical Design Heap Index build. It strengthens the existing
`output_write_bytes` evidence; it adds no dimension, policy field, permit,
cache, wire field, persistent record, format version, or mutation authority.

## Component boundary

The bound includes exactly:

- every logical full-page image published for the new BTree and Index Catalog;
- every byte appended to that Heap participant's WAL by the successful build;
- one committed transaction-status record.

It excludes the database Coordinator log, NBMR receipts, schema mutation
journal, filesystem metadata and `set_len`, fsync/device amplification, cache
reads, source reads, CPU, memory, elapsed time, free-space availability, and
all failed-build rollback/recovery/retry writes. Consequently Index
`output_write_bytes` is not the whole Database transaction's writes and is not
a whole-mutation cost.

The proof starts from Phase 36's existing
`HeapPhysicalDesignSourceInspection::row_upper_bound`, called `R`. That value is
derived from validated current Heap geometry and maximum representable slots.
Sizing is O(1) after source inspection: it scans no row, decodes or samples no
key, and traverses neither the BTree nor Index Catalog. Empty-tree and bounded
pre-backfill catalog growth add pages but no Heap rows, so the same `R` remains
conservative.

## BTree structure

Let `E = EMPTY_TREE_PAGE_COUNT`, taken from the production BTree constant. The
empty owned tree publishes and allocates exactly its metadata and root pages.
Before insertion `i` (one based), a nonempty path cannot have more levels than
the insertion ordinal, so its height is at most `i` for `i >= 1`.

An insertion without a split rewrites one leaf. A split publishes the old and
new leaf; each split internal level publishes its old and new node; root growth
publishes a new root and the updated metadata page. At height `h`:

```text
page images per insertion <= 2h + 2
allocations per insertion <= h + 1
```

Summing with `h <= i` for the one-based insertions `i = 1..R` gives:

```text
BTree page images
    <= E + R(R + 3)

BTree allocations
    <= E + R(R + 3)/2
```

Checked `u128` arithmetic divides an even factor before the allocation
multiplication and checked-converts every public result to `u64`. Any
unrepresentable result is `StorageError::ResourceBoundOverflow`; it is never
saturated or changed to `NotProven`.

Every owned BTree allocation calls the production generation reservation path
exactly once, including reuse. `take_page_ref` chooses at most one reusable
page. Reassigning a generation-safe page whose retired owner still has a root
may detach that owner before the page transition. Validated catalog ownership
allows one retained entry/pending identity per owner, so one allocation can
cause at most one changed catalog node. Reuse therefore changes PageUpdate to
PageAllocationTransition where applicable, but does not increase either the
allocation or page-image theorem.

## Index Catalog

One successful `write_catalog_node_in` always rewrites its selected page and
may append at most one continuation. If the overflow payload itself does not
fit, the call fails instead of producing a second continuation. Thus each
successful catalog mutation publishes at most two full-page images.

The successful Global creation has three fixed non-reuse mutation-call
opportunities: the database-wide IndexId floor advance, the build-local next-ID
advance, and final Index registration. A floor advance may be a no-op, but the
bound charges it. Each possible BTree allocation may additionally detach one
retired owner. Therefore:

```text
catalog calls       <= BTree allocations + 3
catalog page images <= 2 * catalog calls
total page images   <= BTree page images + catalog page images
```

With today's `E = 2`, the last expression simplifies to
`2R(R + 3) + 12`; production uses `EMPTY_TREE_PAGE_COUNT` and never hardcodes
that simplification.

Logical page-image output bytes are `total page images * PAGE_SIZE`, using the
storage page-size constant. This is publication responsibility, not final file
growth or device I/O. Buffer eviction can coalesce or repeat lower-level writes
without changing the theorem. Final Heap file length is forbidden as a proxy:
existing pages are repeatedly rewritten and reusable pages need not grow the
file at all.

## WAL and transaction status

The production WAL encoder gives PageUpdate and PageAllocationTransition the
same full-image record length. Storage-owned sizing helpers are tied by tests
to actual encoded lengths for Begin, PageGenerationReservation, PageUpdate,
PageAllocationTransition, Prepare, and Commit.

For a successful Global Physical Design build, the Heap participant envelope
is exactly Begin, Prepare carrying the database transaction identity, and
Commit after the durable Coordinator decision. It is not the local
Begin/Commit shortcut. No Abort or RollbackComplete belongs to the successful
component. Physical Index changes stage zero Change Stream row records even
when the stream is enabled.

```text
Heap WAL bytes
    <= total page images * WAL page-image record bytes
     + BTree allocations * generation-reservation record bytes
     + Begin bytes + Prepare bytes + Commit bytes
```

Successful participant publication appends exactly one committed TxnStatus
record. Its byte count comes from the production TxnStatus encoder. The final
component is:

```text
Index output_write_bytes
    = logical BTree/Index Catalog page-image bytes
    + Heap participant WAL bytes
    + one committed TxnStatus record
```

`HeapIndexBuildWriteBoundInspection` exposes the structural decomposition and
has no `Default`. Core asks the current Heap source inspection for this report
inside `Database::inspect_physical_index_design_mutation_work` and maps only
`total_write_bytes_upper_bound` to the existing `Bounded` dimension. Core does
not duplicate page, WAL, catalog, or status layout knowledge.

## Admission and compatibility

Phase 34/35 admission consumes the stronger fresh evidence unchanged.
`AtMost(N-1)` rejects with the existing `LimitExceeded` diagnostic and
`AtMost(N)` passes. `AtMost(u64::MAX)` no longer fails as
`RequiredBoundNotProven` for an ordinary representable build. Exact
`AlreadyApplied` and `AlreadyCovered`, recovery-required mapping, stale
runtime/proposal/evidence/advisor ordering, and rejection mutation purity are
unchanged.

An output maximum is conditional evidence, not an unconditional disable
switch. Deployments disable Index apply with
`allow_physical_index_apply: false`. Manifest stays v11, NBOP v6, NBMR v3,
Native Protocol v2, PostgreSQL wire, Inspection JSON, SDK schemas, and every
database persistent format stay unchanged.

Tests tie sizing helpers to encoders, cover zero/small/first-overflow formulas,
long-key leaf/internal/root splits, catalog continuation spill, visible rows
against the structural row bound, small buffers, reusable transition pages and
retired-owner cleanup. The real proposal/admitted-apply Global path measures
logical page images, Heap WAL bytes, generation reservations, TxnStatus bytes,
the exact Begin/Prepare/Commit envelope, and zero Change Stream row records;
actual component output must not exceed the fresh pre-build bound.

After Phase 37, Heap Index source work, source bytes, zero prerequisites, and
participant output are conservatively bounded. Index memory, Columnar peak
memory, Coordinator-inclusive output, whole-mutation cost, CPU/time,
filesystem capacity, cumulative quotas, and automatic Physical Design remain
deferred.
