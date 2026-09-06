# Columnar Phase 2E: bounded maintenance controller

Phase 2E adds a caller-driven, synchronous controller over existing storage
maintenance primitives. It makes one deterministic decision, executes at most
one action, and reports the structural estimate and observed work.

```text
Columnar + NBCL + LSM structural state
                  |
          immutable MaintenanceState
                  |
          read-only policy planner
                  |
       decision + typed reason + estimate
                  |
              budget gate
                  |
        one existing maintenance action
                  |
          MaintenanceStepReport
```

`Database::inspect_maintenance` performs the first four stages without
mutation. `Database::maintenance_step` performs the same plan and, when one
candidate is eligible, executes exactly one action. `NoWork` is a normal
outcome. The caller decides when and how often either API is invoked.

## Primitive audit and supported actions

| Subsystem | Existing primitive | Current shape | Quiescence / sync / I/O | Phase 2E |
| --- | --- | --- | --- | --- |
| Columnar | `advance_columnar_projection` | bounded batches and encoded NBCL bytes | publishes and syncs at most one NBCD | supported |
| Columnar | `compact_columnar_projection` | atomic generation rewrite | can read Base+Delta and sync a new Base; generation leases protect readers | estimate-gated |
| NBCL | `gc_change_stream` | atomic retained-history rewrite | requires safe managed acknowledgements and no unresolved writer; syncs file, guard, and directory | estimate-gated |
| LSM | MemTable `flush` | atomic whole MemTable | requires no active transaction/read view; rotates WAL and syncs SST/manifest | estimate-gated |
| LSM | structural compaction | one atomic selected input set | requires quiescence; reads inputs and syncs output/manifest | one-plan estimate-gated action |
| Heap | `vacuum` | whole-Heap scan | quiescence/cost is not exposed as a cheap stable snapshot | explicit API only |
| Heap | `checkpoint` and index/page reclaim | database-wide or ownership-validation operations | checkpoint, WAL, page-tree and durable-intent gates; potentially large | explicit APIs only |

Heap maintenance is deliberately not represented by a decorative action
variant. Its current primitives do not expose a sufficiently cheap and reliable
cost estimate for automatic admission. Phase 2E does not rewrite those
subsystems.

LSM exposes a small storage-owned maintenance inspection: resident MemTable
entries/bytes, its configured flush ceiling, and the next existing compaction
plan's input entries/bytes. The controller executes at most one compaction plan;
the older explicit `compact` API may still drain every eligible plan.

NBCL keeps payload-free per-batch byte metadata in memory. Open derives it from
validated record boundaries, commit publication updates it, and GC drains it in
lockstep with committed batches. Planning can therefore bound a replay prefix
and estimate retained rewrite bytes without cloning rows, re-encoding records,
or reading the log again. This is runtime metadata, not a new persistent index
or format.

## Budget model

`MaintenanceBudget` has four integer limits shared by the whole database step:

- `max_work_units`: batches for Columnar advance, entries/rows for atomic
  rewrites;
- `max_read_bytes`: encoded change input or structural input bytes;
- `max_write_bytes`: structural output admission estimate;
- `max_actions`: admission for an action. A Phase 2E step still executes at
  most one action when this is greater than one.

There are no milliseconds, CPU percentages, device bandwidth guesses, timers,
or wall-clock deadlines in policy. Elapsed time appears only in benchmarks.

The report preserves both estimated and observed meanings. LSM reports use its
existing amplification counters. Columnar compaction and NBCL GC report their
actual old/new bytes. Columnar advance reports actual NBCD bytes written and
the planner's exact encoded NBCL input bytes; no allocator RSS or physical OS
page-cache I/O is fabricated.

Two admission contracts are explicit:

- `HardBoundedInput`: advance is hard bounded by selected batch count and exact
  encoded NBCL input bytes. Its existing atomic NBCD publisher means output
  bytes remain an admission estimate and actual bytes are reported afterward.
- `EstimateGatedAtomic`: Columnar compaction, NBCL rewrite, LSM flush and one
  LSM compaction plan do not start unless their complete structural estimate
  fits every budget dimension. They are not interrupted halfway and actual I/O
  can differ from the admission estimate.

Consequently, a small budget cannot accidentally start a very large Base
rewrite or LSM/NBCL rewrite. A single NBCL batch larger than the available
advance budget also does not start.

## Deterministic policy and dependencies

Eligible candidates use dependency-aware lexicographic order:

```text
1. LSM MemTable flush
2. incremental projection advance
3. safe NBCL retention GC
4. one structural LSM compaction
5. fresh Columnar Delta compaction
```

This is structural policy, not a weighted score. A resident LSM MemTable is a
finite candidate: once flushed it cannot starve the next class. A Lagging
projection advances before it can compact because compaction does not move its
applied frontier. RebuildRequired and Unavailable projections are reported but
are never rebuilt or repaired. Snapshot projections are not automatically
refreshed.

Managed incremental projections on the active stream generation determine the
safe GC frontier. A lagging projection therefore pins NBCL history and its
advance can unblock a later, separate GC step. Old-incarnation
RebuildRequired projections do not pin the current stream, and ordinary
`read_changes` cursors are still not retention leases. Missing managed metadata
or a matching unmanaged incremental projection blocks GC closed.

Within one priority/action class, stable `StorageId` or
`ColumnarProjectionId` order breaks ties. After a successful action, a
non-persistent runtime cursor rotates the next equal-class choice past that
identity and wraps deterministically. The cursor is part of current runtime
planner state, does not mutate during inspection, and is reconstructed empty
after reopen. Crash correctness depends only on the real durable subsystem
state.

## Busy and failure behavior

An outstanding database transaction marks automatic candidates Busy. Planning
may select another eligible candidate; when none exists, the step returns
`NoWork` with blockers and `more_work_remaining = true`. It never waits, polls,
or sleeps.

Underlying action errors propagate as `DatabaseError`; the controller never
turns a failed action into success. Crash recovery remains the responsibility
of the already-tested Columnar publication, generation retirement, NBCL rewrite,
and LSM manifest protocols. There is no half-persisted scheduler state.
Columnar failure cannot replace authoritative Heap/LSM results, and retention
failure leaves history rather than weakening SQL correctness.

## Foreground path

No commit, DML, query, server event loop, or transaction API calls maintenance.
Committed DML still changes only authoritative storage plus an explicitly
enabled NBCL. It leaves NBCD/NBCS untouched until the caller invokes
`maintenance_step`. Phase 2E adds no foreground sync and does not change the
Phase 2A two-sync stream contract.

## Benchmark

`maintenance_phase2e` records DML-only file behavior, bounded catch-up steps,
frontier movement, selected action, estimated/observed bytes, elapsed time,
Advance-to-GC dependency, too-small compaction admission, and final Delta
compaction. Timings are observations, never assertions.

```bash
NETBADB_MAINTENANCE_ROWS=1000000 \
NETBADB_MAINTENANCE_MUTATIONS=100000 \
NETBADB_MAINTENANCE_BATCHES_PER_STEP=1000 \
cargo bench -p netbadb-core --bench maintenance_phase2e
```

The small default shape is intended for routine validation; the environment
variables select the scale run.

The 2026-09-07 default validation run used 1,000 Base rows, 20 single-batch
updates, and a five-batch step budget. Four advance steps each applied exactly
five batches and 825 encoded change bytes. Observed NBCD output was 987 bytes
per step. The next independent step rewrote NBCL from 4,044 to 104 bytes. A
one-byte read/write budget rejected the 44,642-byte compaction estimate; the
admitted compaction then consumed 1,020 work units, read 44,642 bytes, wrote
40,694 bytes, and left zero Delta segments. The DML-only observation grew NBCL
from 104 to 4,044 bytes while the projection manifest remained byte-identical.
Elapsed observations ranged around 15–16 ms per advance/GC step and 36 ms for
compaction on that run; no timing assertion is encoded.

## Explicit non-goals and semantic boundaries

- `MaintenanceController` is not a background worker, timer, daemon, thread,
  thread pool, async runtime, or server scheduler.
- `MaintenanceBudget` is not a wall-clock deadline or a promise about allocator
  RSS.
- `MaintenancePlanner` does not mutate storage.
- `maintenance_step` does not enter transaction commit.
- `DatabaseTxnId` is not a scheduling order.
- `StorageDataVersion` is not a global database version.
- Maintenance scheduling does not establish a global snapshot.
- Phase 2E adds no decoded block cache, global Columnar buffer pool, async
  prefetch, parallel scan, aggregate spill, compression, network CDC, global
  CSN, `RowEntityId`, authoritative Columnar placement, or automatic storage
  migration.
