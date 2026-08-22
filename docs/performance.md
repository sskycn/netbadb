# NetbaDB performance baseline

Phase 7A established a transparent, dependency-free benchmark target for
database and planner behavior. Phase 7B kept that target and used its measured
plan gap to optimize bounded integer ranges. Phase 7C removed rejected-pair
materialization from NestedLoopJoin, and Phase 7D added a narrowly costed
simple equi HashJoin after measurements isolated the remaining quadratic
candidate work. Post-7D source inspection then found that Heap sequential scan
repeated the complete `Page::header` validation in its per-slot paths. Phase 7E
retains the same benchmark target and validates each immutable Heap page once
per scan. Phase 7J adds required-column attribution and makes base-row ownership
match the physical query's actual needs without weakening persisted-row
validation. Phase 7K attributes and removes redundant Project clones after
storage has created the selected owned values. Phase 7L removes scalar and row
materialization from one measured direct global COUNT(column) shape while
retaining complete current-Heap validation. The target uses
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
- ID-only and ID+Text projection over the same six-column/10,000-row fixture,
  with exact base-scan ColumnId gates and Text-shape validation;
- Text-only, Text+ID reordered, and duplicate Text projections, plus a direct
  projected Heap Text scan control; every observer validates exact Text values;
- COUNT(*), COUNT(Int64), COUNT(nullable Int64), and COUNT(Text) over that
  fixture, including exact base-scan column gates and nullable semantics;
- multi-COUNT and filtered COUNT(Text) fallback controls, plus an ID projection
  with a hidden Text predicate;
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

Phase 7H isolates exact inequality existence rejection with three NestedLoopJoin
shapes. The existing disjoint no-match scenarios reject 100% of left probes. A
partial scenario uses left keys `[0, N)`, right keys `[N/2, N/2 + N)`, and the
necessary conjunct `l.join_key > r.join_key`, so about half of left probes can
be rejected. A no-prune control reverses the disjoint ranges so every left has
at least one possible right. Both new scenarios add `l.id < 0` to keep output
at zero and remove materialization from the measurement.

Before implementation, three serial full runs gave representative
median-of-three million-pair medians of 9.532 ms narrow 100%-reject, 10.289 ms
wide 100%-reject, 11.774 ms Text 100%-reject, 24.348 ms partial, and 24.473 ms
no-prune. All three shapes still enumerated the complete right side.

NestedLoopJoin now extracts the first necessary direct cross-side inequality
under AND from its bound predicate, normalizes reversed operands, and borrows
the exact non-NULL right min or max from the materialized rows. Each left row
is rejected only when that necessary condition cannot be TRUE for any right.
This is exact current-data reasoning, not selectivity estimation, statistics,
or a sort-merge/range join. A possible left probe still runs the unchanged full
right loop and complete predicate.

Three serial full post runs gave representative medians of 0.125 ms narrow,
0.482 ms wide, and 0.303 ms Text for 100% rejection: improvements of about
76.4, 21.4, and 38.8 times. Partial rejection improved from 24.348 to 12.118 ms
(2.01 times), while the no-prune control stayed effectively unchanged at
24.291 ms. HashJoin unique/duplicate/no-match controls remained about
0.249/0.663/0.190 ms. Point SeqScan, low/high-cardinality grouping, 50% range
SeqScan, and the direct join/item Heap scans were about 1.09/1.63/1.67/1.82 ms
and 0.051/0.817 ms. One run had broad system variance; the median-of-three and
the 100%/partial/0% gradient remain consistent, with no timing gate.

Partial and no-prune inequality joins remain the largest measured read cases.
Phase 7I is therefore selected to investigate a true inequality candidate-range
algorithm with explicit costing and output-order boundaries. No sorting,
sweep/range execution, or new operator is implemented in Phase 7H. AND/OR
short-circuit, Filter prebinding, projection/row-codec work, buffer snapshots,
covering reads, and broader HashJoin eligibility remain separate candidates.

Phase 7I adds a near-full dense control with left keys `[3N/4, 7N/4)` and right
keys `[0, N)`. About 96.9% of its million pairs satisfy `l.join_key >
r.join_key`; like the partial and no-prune controls it adds the always-false
`l.id < 0` residual, so result materialization remains zero. Before
implementation, three serial full runs gave median-of-three large medians of
0.125 ms for 100% rejection, 11.697 ms partial, 23.593 ms dense, and 23.573 ms
no-prune.

After the retained Phase 7H extreme check, Phase 7I sorts only borrowed key row
indices, counts exact inequality candidates with a two-pointer pass, and uses a
checked integer work model. Sweep is selected only when exact candidates plus
left/right sort and ordered-set work are strictly cheaper than the current
potential-left-by-total-right loop. Ties and arithmetic overflow fall back;
there is no selectivity percentage, planner statistic, or timing threshold.
The selected sweep keeps candidate right indices in original-index order and
buckets output per original left index, preserving the existing deterministic
left-major/right-minor result order without materializing candidate pairs.

Three serial full post runs gave median-of-three large medians of 0.123 ms for
100% rejection, 3.266 ms partial, 24.669 ms dense, and 24.335 ms no-prune. The
partial case improved 3.58 times while evaluating only its 124,750 exact
inequality candidates instead of about 499,000 Phase 7H pairs. The zero-candidate
fast path remained effectively unchanged; dense and no-prune stayed in the same
approximately 24 ms fallback regime rather than paying candidate evaluation
through the sweep. The narrow/wide/Text 100%-reject medians were
0.123/0.474/0.297 ms.

HashJoin unique/duplicate/no-match controls remained about
0.235/0.607/0.182 ms. Point SeqScan, 50% range SeqScan, low/high-cardinality
grouping, and direct narrow/wide Heap scans were about
1.046/2.077/1.472/1.507 ms and 0.045/0.737 ms. The persistent wide-vs-narrow
scan and 100%-reject gaps provided the clearest next measured target, so Phase
7I selected required-column propagation and projection pruning for Phase 7J.
That implementation was deliberately absent from the Phase 7I result;
cross-layer identity, DML, inspection, and storage-read boundaries were its
entry criteria.

Phase 7J first added five same-schema attribution scenarios without changing
the read path, then recorded three strictly serial full pre runs. The
median-of-three per-run medians were 1.139 ms for ID-only, 1.250 ms for
ID+payload, 1.164 ms for COUNT(*), and 1.224 ms for COUNT(payload). ORDER BY was
1.461 ms, low-cardinality GROUP BY was 1.622 ms, and the 1,000x1,000
100%-reject non-equi join medians were 0.135 ms narrow versus 0.467 ms wide, a
3.47x width ratio despite zero predicate candidates. Every pre attribution
plan still exposed all six item columns.

The planner now selects the complete physical tree first and runs one
query-only top-down requirement pass. Source membership is
`RelationBindingId + ColumnId`; operators retain projection inputs, hidden
Filter/Sort columns, group keys, aggregate column inputs, complete join
predicates, and HashJoin keys. Join requirements split by child provenance,
and base operator order remains source order. UPDATE and DELETE bypass pruning
and keep complete rows.

The executor passes ordered ColumnIds to Heap projected scan and point-read
APIs. Their shared decoder parses and validates every encoded value—including
unselected Bool encodings, Text length/bounds/UTF-8, physical types, NULL
constraints, truncation, and trailing values—but turns only selected values
into owned ScalarValues. Text is a borrowed `&str` inside the decoder and stays
owned at the selected query-result boundary. An empty projection still emits
one RowId-bearing empty execution row per live tuple, preserving COUNT(*) row
semantics. Phase 7E once-per-page validation and generation-safe indexed fetch
remain unchanged.

Post-7J quick ran every scenario with plan, row, shape, and checksum gates. The
full post comparison below was recorded three times strictly serially; timing
remains observational rather than a CI threshold.

The post median-of-three medians were 0.863 ms for ID-only, 1.225 ms for
ID+payload, 0.694 ms for COUNT(*), 1.141 ms for COUNT(payload), 1.121 ms for
ORDER BY, and 1.277 ms for low-cardinality GROUP BY. Relative to pre, those
changed by approximately -24.3%, -2.0%, -40.3%, -6.8%, -23.3%, and -21.3%.
The hidden payload Filter improved from 1.555 to 1.493 ms, about 4.0%.

The representative 1,000x1,000 100%-reject non-equi join changed from 0.467
to 0.408 ms wide and from 0.135 to 0.127 ms narrow. The wide/narrow ratio
contracted from about 3.47 to 3.22; complete validation of all eight encoded
Int64 values intentionally remains, so column pruning removes output ownership
but not width-dependent parse and type checks. Direct full Heap controls did
not improve: the two-column shape changed from about 0.044 to 0.048 ms and the
six-column Text shape from about 0.778 to 0.791 ms. Phase 7J therefore claims
selective-query ownership benefits, not a faster full-row decoder.

The sensitivity ratios make the remaining ownership boundary explicit.
ID+payload divided by ID-only grew from about 1.10 to 1.42, and COUNT(payload)
divided by COUNT(*) grew from about 1.05 to 1.64. Once unused ownership is
removed, selecting Text or feeding it to an aggregate becomes a much clearer
incremental cost. This selects remaining row-codec/scalar-consumer ownership as
the first Phase 7K investigation target. Any aggregate-aware or borrowed
consumption design must receive its own benchmark and preserve complete
persisted-row validation. No Phase 7K implementation was included in the Phase
7J result.

Phase 7K first added Text-only, Text+ID reordered, duplicate Text, and direct
projected Heap attribution without changing executor production code. Three
strictly serial full pre runs produced median-of-three medians of 1.306 ms for
Text-only, 1.353 ms for ID+Text, 1.320 ms for reordered Text+ID, and 1.439 ms
for duplicate Text. The direct projected Heap Text control was 0.803 ms;
COUNT(*) and COUNT(payload) controls were 0.753 and 1.179 ms. Plans, exact base
ColumnIds, rows, output shapes, Text values, and checksums were hard gates.

Project now builds one private projection plan per operator. Identity
projections move input rows directly, unique subset/reorder projections move
values out of checked owned slots, and a source used N times is cloned exactly
N - 1 times before its original value moves at the precomputed last use.
Project output fields and RowIds remain unchanged. HashJoin and nested-loop
candidate inputs remain borrowed/reusable and retain their existing clone
boundary.

Post quick passed every correctness gate. Three strictly serial full post runs
gave median-of-three medians of 0.859 ms for Text-only, 0.866 ms for ID+Text,
1.101 ms for reordered Text+ID, and 1.208 ms for duplicate Text: changes of
about -34.2%, -36.0%, -16.6%, and -16.0%. ID-only changed from 0.946 to 0.694
ms, about -26.6%. The direct projected Heap control was effectively unchanged
at 0.807 ms (+0.5%).

Text-only/direct projected Heap contracted from about 1.63x to 1.07x, directly
isolating the removed Project owner. Duplicate Text/Text-only grew from about
1.10x to 1.41x because duplicate output still requires a second independent
String. This is the intended minimum-clone control, not a regression in output
ownership.

Aggregate controls do not traverse Project. COUNT(payload) stayed effectively
flat at 1.173 ms (-0.5%), while COUNT(*) shifted from 0.753 to 0.698 ms (-7.3%),
which is treated as observational code-layout/machine variation rather than a
Phase 7K claim. Their ratio grew from about 1.57x to 1.68x. With Project's
temporary Text owner removed, aggregate consumer-aware scalar ownership is the
first Phase 7L investigation target; Phase 7L is not implemented here.

Phase 7L first added COUNT(id), COUNT(nullable_key), a two-output COUNT control,
and a filtered COUNT(payload) control without changing production execution.
All queries use the existing item fixture and hard-gate the exact Aggregate,
Filter where applicable, direct SeqScan, required base ColumnIds, output shape,
and exact count. Three strictly serial full pre runs gave median-of-three
medians of 0.712 ms for COUNT(*), 0.927 ms for COUNT(id), 0.925 ms for
COUNT(nullable_key), and 1.157 ms for COUNT(payload). The multi-COUNT and
filtered controls were 1.188 and 1.241 ms; direct projected Heap payload and
SQL payload projection were 0.805 and 0.833 ms.

The executor now recognizes only this shape before executing the Aggregate
child:

```text
Aggregate COUNT(column)
        ↓
single output + no group + direct SeqScan of the same source column?
       / \
     no   yes
     |     |
existing   exact Heap presence scan
executor       ↓
          validate every persisted value
               ↓
          count target non-NULL as u128
               ↓
          checked UInt64 aggregate result
```

COUNT(*), grouped or multiple aggregates, Filter, Join, Sort, IndexScan,
RangeIndexScan, mismatched scan columns, and every non-COUNT function retain the
generic Aggregate executor. The physical plan and Inspection JSON remain
unchanged; this is an executor-private runtime specialization, not a new
operator or general aggregate pushdown.

`HeapStorage::scan_column_presence_count` is an exact read of current live Heap
tuples. It resolves the requested ColumnId once, validates each immutable page
once, validates non-Heap single payloads, skips tombstones, and decodes every
column of every live row. Bool encodings, Text lengths/bounds/UTF-8, physical
types, NULL constraints, truncation, and trailing values all remain checked.
The target value stays a borrowed `DecodedScalar` long enough to record only
NULL presence; it never becomes an owned `ScalarValue`, and scanned tuples
never become `ExecutionRow`s. The scan neither reads cached ANALYZE statistics
nor persists a count, writes WAL, or acquires a transaction writer. It returns
a checked exact `u128`; the executor's checked conversion retains the existing
typed COUNT overflow error at the SQL `u64` boundary.

Post quick passed every benchmark correctness and plan gate. Three strictly
serial full post runs gave median-of-three medians of 0.674 ms for COUNT(*),
0.517 ms for COUNT(id), 0.514 ms for COUNT(nullable_key), and 0.518 ms for
COUNT(payload). The eligible column counts improved by approximately 44.3%,
44.5%, and 55.2%, respectively; COUNT(payload) improved 2.23 times. Its ratio
to COUNT(*) contracted from 1.626x to 0.769x, and its ratio to COUNT(id)
contracted from 1.248x to 1.003x. This removes the Text ownership distinction
without reducing row validation.

The non-eligible controls stayed on the generic path: multi-COUNT measured
1.112 ms and filtered COUNT(payload) measured 1.128 ms. Direct projected Heap
payload and SQL payload projection measured 0.752 and 0.800 ms. COUNT(*) and
these controls moved by approximately -5.2%, -6.3%, -9.0%, -6.6%, and -3.9%
relative to pre; those changes are observational machine/code-layout variance,
not Phase 7L claims. Point/range, Phase 7I 100%/partial/dense/no-prune,
HashJoin, grouping, projection, and DML plan/result gates all remained intact.

The closest remaining measured aggregate gaps are the 1.112 ms direct
multi-COUNT control and the 1.128 ms filtered COUNT control. The multi-COUNT
shape has the narrower next design boundary because it can retain one exact
current-Heap validation pass without introducing Filter evaluation semantics.
It is therefore the first Phase 7M investigation target; no Phase 7M
implementation is included here. Filter predicate prebinding/borrowed Text,
MIN/MAX and group-key ownership, buffer snapshots, covering reads, broader
HashJoin eligibility, multi-inequality intersection, AND/OR short-circuiting,
and sequential PageManager traversal remain separate candidates.

Phase 7M retained the existing pair scenario and added duplicate-column,
mixed-nullable, mixed star/column, aggregate-output-order, all-star, and
filtered-pair attribution. Every scenario hard-gates its exact result,
Aggregate/Filter/SeqScan shape, and Phase 7J source-order base ColumnIds. Three
strictly serial full pre runs gave median-of-three medians of 1.170 ms for
COUNT(id)+COUNT(payload), 1.137 ms for duplicate COUNT(payload), 1.182 ms for
three mixed-nullability column counts, 1.143 ms for COUNT(*)+COUNT(payload),
and 1.179 ms for the five-output order/reuse case.

The executor-private direct-count specialization now recognizes a global
Aggregate whose nonempty outputs are all COUNT, whose child is a direct
SeqScan, and which contains at least one COUNT(column):

```text
Global Aggregate
      ↓
all outputs COUNT + direct SeqScan + at least one COUNT(column)?
     / \
   no   yes
   |     |
generic  map aggregate outputs to live rows or source-order scan columns
         ↓
   one exact Heap presence summary
         ↓
   live_rows + ordered per-column non-NULL counts
         ↓
   reconstruct aggregate output order with checked SQL u64 conversion
```

`HeapStorage::scan_presence_counts` returns a typed `PresenceCountSummary`
containing checked `u128` live-row and ordered non-NULL counts. It accepts zero
or duplicate column requests and preserves request order. The Phase 7L
single-column API delegates to this one authoritative traversal. Presence
scratch is allocated once per scan and reset per row; no live row allocates a
presence vector, owns a `ScalarValue`, or becomes an `ExecutionRow`.

The summary is an exact current-Heap read, not statistics or generic aggregate
pushdown. Every managed page is fully validated once, non-Heap single payloads
remain validated, and every live tuple fully decodes all selected and
unselected scalars. Bool encodings, Text bounds and UTF-8, physical types,
NULL constraints, truncation, and trailing values remain checked without
calling `DecodedScalar::into_owned`. Tombstones are excluded and slot reuse,
relocation, index/ANALYZE mixed pages, and reopen count only current live
tuples.

Duplicate COUNT(column) outputs reuse one source-order summary slot. Mixed
COUNT(*) reads the same summary's live-row count, while each final output uses
its own `AggregateExpr` for typed overflow attribution. A single COUNT(*) and
all-star multi-output aggregates deliberately remain generic. Grouping,
Filter, Join, Sort, IndexScan, RangeIndexScan, unused/mismatched scan columns,
and mixed COUNT with SUM/MIN/MAX also retain the complete generic executor.
The planner, PhysicalPlan, and Inspection JSON are unchanged.

Post quick passed every plan/result/column gate. Three strictly serial full
post runs reduced the median-of-three medians to 0.657 ms for the pair, 0.646
ms for duplicate payload, 0.685 ms for mixed nullable, 0.622 ms for
star+payload, and 0.686 ms for output order: improvements of approximately
43.9%, 43.2%, 42.1%, 45.6%, and 41.8%, respectively. They now share one exact
validation scan rather than materializing 10,000 owned rows and values.

Phase 7L single-column controls changed from 0.530/0.543/0.531 ms for
COUNT(id)/COUNT(nullable_key)/COUNT(payload) to 0.630/0.617/0.625 ms
(approximately +18.8%/+13.8%/+17.8%). COUNT(*) changed from 0.678 to 0.778 ms,
the all-star pair from 0.712 to 0.784 ms, filtered single from 1.163 to 1.299
ms, and filtered pair from 1.197 to 1.279 ms. These non-target shifts are
observational code-layout/machine variance; they have unchanged plan/result
gates and no timing threshold.

The remaining measured aggregate gap is now filtered COUNT at approximately
1.28–1.30 ms versus 0.62–0.69 ms for direct presence summaries. A filtered
COUNT consumer path is therefore the selected Phase 7N investigation. Direct
COUNT(*) live-row specialization, MIN/MAX ownership, group-key ownership,
Filter predicate prebinding and borrowed Text evaluation, AND/OR
short-circuiting, BufferPool page snapshot cloning, covering/index-only reads,
broader HashJoin eligibility, multi-inequality intersection, and sequential
PageManager traversal remain separate candidates; Phase 7N is not implemented
here.

## CI and compatibility

`cargo check --workspace --all-targets` compiles the benchmark, including on
the Rust 1.85 MSRV. The expensive workload is not a normal CI performance gate
and has no pass/fail timing threshold.

Phase 7B changed the Inspection JSON contract from v1 to v2 for
RangeIndexScan. Phase 7D changes the current contract from v2 to v3 solely to
represent HashJoin. Phases 7E through 7M introduce no plan or inspection
change, so v3 remains current. They change no NetbaDB Protocol v1 message, SDK
Schema Spec v1 field, deployment manifest v4 field, or database persistent
format.
