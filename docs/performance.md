# NetbaDB performance baseline

Phase 7A established a transparent, dependency-free benchmark target for
database and planner behavior. Phase 7B kept that target and used its measured
plan gap to optimize bounded integer ranges. After Phase 7B, join execution was
the highest read-path candidate: NestedLoopJoin materialized and copied a
combined row for every candidate pair before testing its predicate. Phase 7C
removed that rejected-pair work, but measured evidence still showed that a
fully disjoint 1,000×1,000 join spent meaningful time enumerating and testing
one million candidate pairs. Phase 7D therefore adds a narrowly costed simple
equi HashJoin while retaining the same benchmark target. The target uses
`std::time::Instant` and `std::hint::black_box`, and Cargo builds it with the
optimized bench profile.

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
the leading measured follow-up. Phase 7E therefore targets that common scan
throughput boundary; it does not extend HashJoin eligibility or start a
multi-way join optimizer.

## CI and compatibility

`cargo check --workspace --all-targets` compiles the benchmark, including on
the Rust 1.85 MSRV. The expensive workload is not a normal CI performance gate
and has no pass/fail timing threshold.

Phase 7B changed the Inspection JSON contract from v1 to v2 for
RangeIndexScan. Phase 7D changes the current contract from v2 to v3 solely to
represent HashJoin. It changes no NetbaDB Protocol v1 message, SDK Schema Spec
v1 field, deployment manifest v4 field, or database persistent format.
