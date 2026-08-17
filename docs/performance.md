# NetbaDB performance baseline

Phase 7A established a transparent, dependency-free benchmark target for
database and planner behavior. Phase 7B kept that target and used its measured
plan gap to optimize bounded integer ranges. Phase 7C removed rejected-pair
materialization from NestedLoopJoin, and Phase 7D added a narrowly costed
simple equi HashJoin after measurements isolated the remaining quadratic
candidate work. Post-7D source inspection then found that Heap sequential scan
repeated the complete `Page::header` validation in its per-slot paths. Phase 7E
retains the same benchmark target and validates each immutable Heap page once
per scan. The target uses `std::time::Instant` and `std::hint::black_box`, and
Cargo builds it with the optimized bench profile.

Run the default quick profile for development confirmation:

```sh
cargo bench -p netbadb-core --bench phase7_baseline
```

Run the full profile for a manually recorded baseline:

```sh
NETBADB_BENCH_PROFILE=full \
  cargo bench -p netbadb-core --bench phase7_baseline
```

`NETBADB_BENCH_PROFILE` accepts only `quick` or `full` and defaults to
`quick`. Quick uses 250 small rows, 1,000 medium rows, and 100×100 plus 300×300
join inputs. Full uses 1,000 small rows, 10,000 medium rows, and 500×500 plus
1,000×1,000 joins. Profiles change only scale, warmup, and iteration counts;
schemas, distributions, SQL shapes, indexes, plan expectations, and result
semantics remain the same.

## Measurement semantics

This is a warm-process, current-buffer-pool baseline. Each read scenario first
executes unmeasured warmup iterations. It does not claim cold-disk behavior and
does not manipulate operating-system caches, call privileged APIs, or use
platform-specific cache controls.

Every scenario owns distinct temporary database files under
`std::env::temp_dir()`. Query setup creates the schema, deterministically loads
rows, registers indexes, and runs `ANALYZE` where required before starting an
`Instant`. Plan inspection, result verification, output, explicit database
close, and database/WAL cleanup are also outside the timed query loop. INSERT
is intentionally different: each sample starts with a newly created empty
database, and the timed operation includes the transaction begin, deterministic
row inserts, registered-index maintenance, and commit. Index creation itself
is setup and is not timed.

The two direct Heap scan scenarios likewise create and load their fixtures and
commit the load transaction outside the timed loop. They time
`HeapStorage::scan` directly, without SQL compilation, planning, or executor
dispatch, and validate the complete returned row width, row count, and a
deterministic first-column checksum.

Each measurement retains a `Vec<Duration>`. Durations are sorted to report:

- minimum;
- median, using the middle value for an odd count and the integer mean of the
  two middle values for an even count;
- p95 using the nearest-rank rule, `ceil(0.95 × sample_count)`.

INSERT totals are divided by inserted rows and all output is shown as integer
nanoseconds per operation. No elapsed-time or throughput value is a test
assertion. Machine-specific numbers are deliberately not committed as expected
results, and the normal test suite does not execute the workload.

For a formal comparison, record at least the benchmark output, host CPU and
memory, operating system, storage environment, `rustc --version`, Git revision,
and profile. Compare runs on the same controlled machine. Phase 7 optimization
work must name the affected benchmark scenarios and retain their plan and
correctness checks; intuition alone is not a baseline.

## Deterministic data

Rows use their zero-based integer position as `id` and selective `bucket_id`.
`team_id` is a deterministic modulo distribution, `active` is true for every
third row, nullable scenarios use fixed modulo NULL rates, and payloads use a
fixed-width numeric suffix. There is no random generator, timestamp, hostname,
or repository-relative fixture path.

Every timed result is consumed through `black_box` and checked using an
expected row count plus deterministic numeric checksum. The benchmark also
calls `Database::inspect_statement` before timing and rejects a scenario whose
real chosen plan lacks its required operator or contains a forbidden access
path. It never infers an IndexScan merely because an index exists.

## Scenarios

The target currently covers:

- direct Heap scan of a two-Int64-column join-shaped table, using 1,000 rows in
  the full profile and reporting `DirectHeapScan`;
- direct Heap scan of the existing six-column item shape, including its Text
  payload, using 10,000 rows in the full profile and reporting
  `DirectHeapScan`;
- point equality with no index: `Filter → SeqScan`;
- the same point equality with an analyzed registered ID index:
  `Filter → IndexScan`;
- duplicate-heavy indexed equality where the costed planner chooses SeqScan;
- selective equality on a non-primary `bucket_id` index;
- low- and high-rate `IS NULL` distributions over a nullable index;
- a one-percent bounded indexed-ID range selecting `RangeIndexScan`;
- a fifty-percent bounded indexed-ID range retaining `SeqScan` after cost
  comparison;
- a one-sided indexed-ID range retaining `SeqScan` because it is not costed;
- `ORDER BY team_id LIMIT 20`, retaining explicit in-memory Sort and Limit;
- low- and higher-cardinality `GROUP BY team_id` through in-memory Aggregate;
- unique-like, duplicate-key, and fully disjoint analyzed equality joins at two
  input scales, selecting HashJoin after Phase 7D;
- a fully disjoint analyzed non-equality join at both scales, retaining
  NestedLoopJoin; disjoint left keys are `[0, N)` and right keys are `[N, 2N)`
  and both no-match shapes must produce zero rows with checksum zero;
- direct INSERT into tables with zero, one, and two registered indexes;
- SQL UPDATE of an indexed key while locating distinct rows through an index;
- `Database::inspect_statement` compile plus real physical-planning overhead.

The Phase 7B estimator deliberately uses only existing table/index statistics
and the exact discrete key count implied by two Int64/UInt64 literal bounds.
Phase 7A showed that a narrow range paid full SeqScan cost even though point
lookup demonstrated an effective B+Tree path, while a wide range was already
close to the full scan. This is why selection is costed rather than automatic.
No min/max, histogram, MCV, floating selectivity guess, or persistent statistics
change is involved. One-sided and Text/Bool ranges remain SeqScan; index
union/intersection, join alternatives, and sort avoidance remain deferred.

Phase 7C evaluates each join predicate through a non-owning view over the
already materialized left and right child rows. A rejected pair allocates no
combined value vector and copies no row values; a matching pair is materialized
normally in left-then-right order. Controlled full-profile runs at 500×500 and
1,000×1,000 showed that the fully disjoint million-pair case still spent
material time in candidate enumeration and typed predicate evaluation even
though it produced no rows. That qualitative result isolated the remaining
quadratic work and selected Phase 7D's algorithm change.

Phase 7D considers HashJoin only for an INNER JOIN whose current logical
children are direct scans, whose predicate contains a necessary cross-side
column equality in an AND tree, and whose two tables both have existing ANALYZE
row counts. It compares transparent integer work units: `left_rows * right_rows`
for NestedLoopJoin and `left_rows + right_rows` for HashJoin, using checked
`u128` arithmetic and selecting hash only when it is strictly cheaper. Missing
statistics, ties, non-equality predicates, unsupported boolean shapes, and
non-scan children retain NestedLoopJoin.

HashJoin materializes both children, builds the right child into buckets of
right-row indices, probes in left input order, and evaluates the complete typed
predicate for every bucket candidate before materializing TRUE rows. NULL keys
are excluded from both build and probe. Right indices retain input order, so
the current deterministic left-major/right-minor behavior is preserved without
turning unordered SQL output into a language guarantee. There is no join
reordering, dynamic build-side choice, composite key, index coupling, spilling,
or global cost model.

The post-7D full-profile comparison shows approximately input-linear scaling
for unique, duplicate, and no-match HashJoin scenarios, with those three shapes
remaining close despite different bucket-candidate counts. The retained
non-equi no-match NestedLoopJoin remains slower and scales worse. This indicates
that the eligible equality path no longer pays dominant quadratic candidate
work and that shared child scanning, row decoding, and materialization are now
the leading measured follow-up.

Phase 7E source inspection found the concrete storage cause. A Heap scan first
called `Page::header`, then `slot_state` called it again for each slot, and a
live slot's `read_record` path called it yet again. For a page with `N` live
slots, one scan could therefore perform `1 + 2N` complete page validations.
`Page::header` is not a field getter: it verifies magic and version, the
PageId-bound CRC32C, reserved bytes, page type, free-space bounds, every slot
generation and record range, and pairwise record non-overlap, allocating a
record-range vector along the way.

Controlled pre-feature and post-feature full-profile runs were recorded three
times each, strictly serially, after adding the two direct attribution
scenarios. The median of the three per-run medians showed an order-of-magnitude
improvement for both direct shapes and for every scan-dominated query, while
plan and result gates remained unchanged. The two-column direct scan improved
more than the wider Text-bearing shape, which now exposes row width, decoding,
and ownership as a smaller follow-up cost. HashJoin scenarios also improved
substantially because both children are scans. The non-equi million-candidate
NestedLoopJoin improved from cheaper children but remains the largest measured
read-path case because candidate predicate evaluation now dominates.

That post-7E evidence selected predicate column-position prebinding for Phase
7F. The expression evaluator resolved each column by scanning output fields for
every candidate evaluation. A 1,000×1,000 non-equi NestedLoopJoin therefore
performed at least two million repeated identity lookups despite producing no
rows.

Phase 7F adds narrow and wide no-match non-equi attribution pairs at both join
scales. The narrow schema retains two Int64 columns per side. The wide schema
uses eight Int64 columns per side and places `join_key` last, at combined
positions 7 and 15. Both use disjoint key ranges and compare left `join_key`
greater than right `join_key`, so every candidate is FALSE and output
materialization remains absent. The wide shape deliberately contains no Text
column.

The executor now binds the complete Join `ON` expression once after child
output fields are known. NestedLoopJoin candidate pairs and HashJoin residual
bucket candidates evaluate the private position-bound tree without an output
field slice or repeated identity lookup. Binding still uses the authoritative
`RelationBindingId + ColumnId` rule, and evaluation retains checked access,
owned ScalarValue results, the existing binary/NULL semantics, and AND/OR
without short-circuiting. Filter and UPDATE expression evaluation remain
dynamic.

Controlled full-profile pre/post runs were recorded three times each,
strictly serially. Before binding, the representative wide million-pair case
was about 1.70 times the narrow case. Afterwards that ratio was about 1.03,
with the wide case improving materially and the narrow case improving by a
smaller constant factor. HashJoin controls remained in the same sub-millisecond
range; direct scans and grouped queries also stayed at the post-7E scale.
Point/range controls retained their exact plans and results, although individual
timings continued to show machine/code-layout variance. There is no timing
gate.

Phase 7G adds deterministic two-column Text no-match scenarios at 500x500 and
1,000x1,000. Keys are fixed-width `L-{id:020}` and `R-{id:020}` values, so
`l.join_key > r.join_key` is always FALSE, the plan is NestedLoopJoin, and no
output row is materialized. Before the executor change, each candidate cloned
both String operands. Three strictly serial full pre runs gave representative
median-of-three million-pair medians of 9.426 ms narrow Int64, 9.686 ms wide
Int64, and 49.603 ms Text: Text was 5.26 times narrow Int64.

The Join-bound evaluator now borrows Column and Literal ScalarValues and owns
only computed results. Binary and truth semantics operate through references;
the normal Filter/UPDATE evaluator remains owned and AND/OR still evaluates
both sides. Three serial full post runs gave representative medians of 10.194
ms narrow Int64, 10.398 ms wide Int64, and 12.365 ms Text. Text improved about
4.01 times and its ratio to narrow Int64 contracted to 1.21, directly
attributing the removed candidate-level String clones. The Int64 cases did not
improve in these runs (about 8.1% and 7.3% slower respectively), so no broader
constant-factor optimization is claimed or added.

HashJoin million-pair-scale controls remained sub-millisecond at representative
medians of 0.236 ms unique, 0.677 ms duplicate, and 0.180 ms no-match. Point
SeqScan, low/high-cardinality grouping, and 50% range SeqScan controls remained
about 1.14, 1.59, 1.61, and 1.88 ms; direct 1,000-row join-shape and 10,000-row
item-shape Heap scans were about 0.046 and 0.792 ms. Individual runs retain
machine and code-layout variance, and there is no timing gate.

The narrow million-candidate NestedLoopJoin remains the largest measured read
case even though its two scans total only about 0.10 ms. Since leaf lookup and
ownership signals are now isolated, Phase 7H is selected to investigate
non-equi join algorithm alternatives that can reduce candidate work. AND/OR
short-circuit, Filter prebinding, projection/row-codec work, buffer snapshots,
covering reads, and broader HashJoin eligibility remain later measured
candidates.

## CI and compatibility

`cargo check --workspace --all-targets` compiles the benchmark, including on
the Rust 1.85 MSRV. The expensive workload is not a normal CI performance gate
and has no pass/fail timing threshold.

Phase 7B changed the Inspection JSON contract from v1 to v2 for
RangeIndexScan. Phase 7D changes the current contract from v2 to v3 solely to
represent HashJoin. Phases 7E through 7G introduce no plan or inspection
change, so v3 remains current. They change no NetbaDB Protocol v1 message, SDK
Schema Spec v1 field, deployment manifest v4 field, or database persistent
format.
