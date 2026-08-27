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
retaining complete current-Heap validation. Phase 7M shares one presence scan
across direct multi-COUNT outputs. Phase 7N streams Filter-qualified COUNT
consumption without owning count-only scalars or materializing intermediate
rows. Phase 7O then removes dynamic evaluator leaf clones only inside that
filtered-count consumer. Phase 7P keeps predicate Text borrowed from the
validated Heap payload through that synchronous callback, while deliberately
retaining dynamic column lookup and fully owned generic Filter/QueryResult
boundaries. Phase 7S rolls the borrowed dynamic evaluator into generic Filter,
and Phase 7T streams exact direct sequential Filters over borrowed validated
rows so rejected rows are never owned. The target uses
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
- direct multi-COUNT controls; filtered COUNT(Int64), COUNT(nullable Int64),
  COUNT(Text), multi/mixed-star/output-order cases, an all-star fallback, and a
  Text-predicate overlap control; plus an ID projection with a hidden Text
  predicate;
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
The target value stays a borrowed `ScalarRef` long enough to record only
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

The executor-private direct-count specialization recognizes a global Aggregate
whose nonempty outputs are all COUNT, whose child is a direct SeqScan, and
whose scan columns are exactly the columns consumed by COUNT(column). Phase 7R
extends this to pure COUNT(*) outputs when that exact scan layout is empty:

```text
Global Aggregate
      ↓
all outputs COUNT + direct SeqScan + no unused scan columns?
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
calling `ScalarRef::to_owned`. Tombstones are excluded and slot reuse,
relocation, index/ANALYZE mixed pages, and reopen count only current live
tuples.

Duplicate COUNT(column) outputs reuse one source-order summary slot. Mixed and
pure COUNT(*) outputs read the same summary's live-row count, while each final
output uses its own `AggregateExpr` for typed overflow attribution. Pure
COUNT(*) is eligible only for `SeqScan[]`; a nonempty all-star scan falls back
defensively. Grouping, Filter, Join, Sort, IndexScan, RangeIndexScan,
unused/mismatched scan columns, and mixed COUNT with SUM/MIN/MAX retain the
complete generic executor. The planner, PhysicalPlan, and Inspection JSON are
unchanged.

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

The remaining measured aggregate gap was filtered COUNT at approximately
1.28–1.30 ms versus 0.62–0.69 ms for direct presence summaries. That evidence
selected Phase 7N.

Phase 7N added filtered ID, nullable, star+payload, output-order/reuse,
all-star-fallback, and Text-predicate attribution cases before changing
production code. The executor-private specialization recognizes only this
physical shape:

```text
Global Aggregate COUNT outputs
             ↓
        direct Filter
             ↓
        direct SeqScan
             ↓
split source-order requirements
      /                    \
predicate ScalarValues   COUNT presence bits
      \                    /
 one completely validated Heap visitor traversal
             ↓
 existing dynamic Filter evaluator
     TRUE / FALSE / UNKNOWN
             ↓
 checked count summary + one result row
```

Every output must be COUNT, grouping must be empty, and at least one output
must be COUNT(column). COUNT(*), duplicate columns, multiple columns, nullable
columns, and SQL output reordering are supported inside that boundary. A
filtered all-star aggregate, grouped/mixed-function aggregate, nested Filter,
Sort, Join, IndexScan, RangeIndexScan, mismatched identity, missing requirement,
or unused SeqScan column retains the complete generic executor. The physical
plan remains `Aggregate → Filter → SeqScan`; this is not a planner rewrite or a
new operator.

`HeapStorage::visit_columns_with_presence` is a synchronous low-level read
primitive. It resolves separate value and presence projections once, preserves
request order and duplicates, supports overlap, and allocates value-slot,
visitor-value, and presence scratch once before traversal. A selected scalar
is decoded once: presence is recorded before an owned `ScalarValue` is created,
and ownership occurs only if the value projection requests it. Thus a Text
column used only by COUNT never creates a String, while a Text column genuinely
used by the predicate still does in this phase.

Storage does not receive or evaluate SQL `Expr`. It validates every managed
page once, validates non-Heap single payloads, and fully decodes every scalar
of every live tuple before invoking the callback. Tag, Bool, integer width,
Text length/bounds/UTF-8, physical type, NULL constraint, truncation, and
trailing-value checks are unchanged. Tombstones are skipped; slot reuse,
relocation, mixed index/ANALYZE pages, and reopen expose each current live tuple
once. The first callback error stops traversal and is returned unchanged; the
read writes no WAL, performs no persistent mutation, and acquires no writer.

The executor continues to call the existing dynamic `evaluate_truth` with
source-order predicate fields. TRUE updates checked `u128` qualified-row and
per-source non-NULL counts; FALSE and UNKNOWN update nothing. Each unique
presence source attributes intermediate overflow to its first `AggregateExpr`,
while every final SQL output independently performs the existing checked
`u64` conversion with its exact metadata. No scanned or filtered
`ExecutionRow` collection is constructed. This phase does not prebind Filter
positions, add a borrowed Filter evaluator, or claim that Filter is zero-copy.

Three strictly serial full pre/post runs used distinct
`/private/tmp/netbadb-phase7n-pre-target` and
`/private/tmp/netbadb-phase7n-post-target` build directories. Median-of-three
medians in milliseconds were:

| scenario | pre | post | change |
| --- | ---: | ---: | ---: |
| filtered COUNT(id) | 0.969 | 0.725 | -25.3% |
| filtered COUNT(nullable_key) | 0.979 | 0.715 | -26.9% |
| filtered COUNT(payload) | 1.253 | 0.719 | -42.6% |
| filtered COUNT(id), COUNT(payload) | 1.267 | 0.738 | -41.8% |
| filtered COUNT(*), COUNT(payload) | 1.263 | 0.735 | -41.8% |
| filtered output order/reuse | 1.305 | 0.775 | -40.6% |
| filtered Text-predicate COUNT(payload) | 1.536 | 1.344 | -12.5% |
| filtered COUNT(*), COUNT(*) fallback | 0.976 | 0.889 | observational |

Direct Phase 7M controls changed from 0.622/0.600/0.612/0.628 ms for
COUNT(id), COUNT(nullable_key), COUNT(payload), and the ID+payload pair to
0.551/0.544/0.545/0.583 ms. They retain their dedicated presence-summary path;
these non-target changes are observational machine/code-layout variance.

Filtered payload/direct payload contracted from 2.046x to 1.321x, and filtered
payload/filtered ID contracted from 1.292x to 0.992x. Count-only Text ownership
is therefore no longer visible as the filtered payload penalty. The post Text
predicate/Bool predicate ratio remains 1.868x because Text needed by the
predicate still becomes an owned String. That isolated gap selects borrowed
Text Filter evaluation as the first Phase 7O investigation; Filter position
prebinding remains a separate measured candidate. Direct COUNT(*) live-row
specialization, AND/OR short-circuiting, MIN/MAX and group-key ownership,
BufferPool page snapshot cloning, covering/index-only reads, broader HashJoin
eligibility, multi-inequality intersection, and sequential PageManager
traversal remain separate candidates. There is no timing threshold.

Phase 7O first added two attribution cases without changing production: an
Int64 equality with the same one-column/one-literal/one-match shape as the Text
equality, and a repeated Text range that evaluates the same Column and literal
shape twice through AND. The existing Bool equality and generic
`hidden_filter_payload` controls remain unchanged.

The new executor-private dynamic borrowed evaluator reuses Phase 7G's
`EvaluatedScalar`; it does not introduce another scalar-reference domain.
Column leaves still call `find_source_position` on every evaluation but borrow
the selected row value. Literal leaves borrow the value stored in `Expr`.
Binary nodes evaluate both children and call `evaluate_binary_refs`, while
Unary NOT, IsNull, and binary results own only their computed Bool or NULL.
AND/OR deliberately retain full two-sided evaluation. The original
`evaluate_values`, `evaluate_truth_values`, `evaluate`, and `evaluate_truth`
remain authoritative for generic Filter, UPDATE, and other dynamic callers;
the prebound Join evaluator is unchanged.

Only the Phase 7N visitor callback calls this evaluator. The Heap visitor still
decodes predicate Text into one owned `ScalarValue::Text(String)` before the
callback, so Phase 7O is not borrowed persisted Text, storage-to-executor
zero-copy, Filter prebinding, or a generic Filter rollout. It removes the
subsequent dynamic Column clone, Literal clone, and repeated leaf clones.

Three strictly serial full pre/post runs used separate
`/private/tmp/netbadb-phase7o-pre-target` and
`/private/tmp/netbadb-phase7o-post-target` build directories. Median-of-three
medians in milliseconds were:

| scenario | pre | post | change |
| --- | ---: | ---: | ---: |
| filtered Bool equality COUNT(payload) | 0.774 | 0.795 | observational |
| filtered Int64 equality COUNT(payload) | 0.767 | 0.766 | -0.1% |
| filtered Text equality COUNT(payload) | 1.462 | 1.037 | -29.1% |
| filtered repeated Text COUNT(payload) | 2.013 | 1.163 | -42.2% |
| generic hidden Text Filter | 1.610 | 1.650 | observational |
| direct COUNT(payload) | 0.602 | 0.589 | observational |

The Text/Int ratio contracted from 1.906x to 1.353x, repeated/single Text from
1.377x to 1.122x, and Text/Bool from 1.889x to 1.305x. The generic Filter
control changed by +2.5%, while filtered ID and the direct ID/nullable/payload/
pair controls remained observational at 0.770 and 0.589/0.585/0.589/0.616 ms.
The Phase 7N Bool-filtered ID/payload/pair controls changed from
0.773/0.774/0.767 to 0.770/0.795/0.805 ms, also observational.
The third post run showed broad machine-wide slowdowns, so the documented
median-of-three remains the comparison basis; there is no timing threshold.

Removing leaf clones therefore explains a substantial portion of the Text
predicate cost and repeated-leaf scaling, without changing generic Filter.
Text equality remains 1.353x the equivalent Int64 case, while repeated leaves
add only 1.122x including the extra comparison and AND. This evidence selected
storage-to-executor borrowed predicate values as Phase 7P.

Phase 7P first added Text and Int64 `IS NOT NULL` attribution cases without
changing production. The Text case isolates storage ownership because it needs
the Text Column value but performs no Text comparison and has no Text literal;
the Int64 case is its ownership control.

`netbadb-types` now provides one shared `ScalarRef<'a>` runtime view for Bool,
Int64, UInt64, borrowed Text, and NULL. It is not a persistent representation,
wire type, schema type, or SQL IR node. Heap decoding returns `ScalarRef`
directly, validates the complete persisted row, and exposes requested values
through an HRTB synchronous visitor. Its Text reference can exist only while
the validated page, record payload, and current callback remain alive. Scratch
vectors are reused per validated Heap page, and no unsafe code or page-backed
reference escapes the callback.

The old owned visitor remains additive-compatible and delegates the same
authoritative traversal, converting only requested values with
`ScalarRef::to_owned`. Only the Phase 7N filtered-count callback uses the new
borrowed visitor. `EvaluatedScalar::Borrowed` now contains a `ScalarRef`, while
computed Binary, Unary, and IsNull values remain owned. Dynamic binding-aware
`find_source_position` lookup remains unchanged, and existing ScalarValue
binary/comparison/truth helpers are thin wrappers over one ScalarRef semantic
core. Generic PhysicalPlan Filter still consumes fully owned SeqScan rows.

Three strictly serial full pre/post runs used separate
`/private/tmp/netbadb-phase7p-pre-target` and
`/private/tmp/netbadb-phase7p-post-target` build directories. Median-of-three
medians in milliseconds were:

| scenario | pre | post | change |
| --- | ---: | ---: | ---: |
| filtered Text `IS NOT NULL` COUNT(payload) | 0.969 | 0.646 | -33.3% |
| filtered Int64 `IS NOT NULL` COUNT(payload) | 0.710 | 0.624 | -12.1% |
| filtered Text equality COUNT(payload) | 1.028 | 0.702 | -31.7% |
| filtered Int64 equality COUNT(payload) | 0.775 | 0.672 | -13.3% |
| filtered repeated Text COUNT(payload) | 1.211 | 0.879 | -27.5% |
| generic hidden Text Filter | 1.641 | 1.510 | observational |
| filtered Bool equality COUNT(payload) | 0.797 | 0.711 | observational |

Text/Int equality contracted from 1.327x to 1.045x, while Text/Int `IS NOT
NULL` contracted from 1.365x to 1.035x. This is direct evidence that the
storage-created predicate Text owner is no longer visible. Repeated/single Text
changed from 1.178x to 1.251x: both improved in absolute time, but repeated
dynamic lookup and expression work are now a larger fraction after ownership
was removed. The generic hidden Filter remains outside the specialization.

Direct ID/nullable/payload/pair COUNT controls changed from
0.599/0.590/0.592/0.612 ms to 0.611/0.544/0.550/0.576 ms. Phase 7N filtered
ID/payload/pair controls changed from 0.780/0.797/0.799 ms to
0.739/0.711/0.719 ms. These broader changes are observational machine and code
layout variance; there is no timing threshold.

Post-7P data therefore selected filtered-count predicate position prebinding as
Phase 7Q. Repeated/single alone was not sufficient attribution because the
repeated predicate also adds a comparison, literal, and AND node. Phase 7Q
first added a repeated Int64 range with the same expression shape and a wide
primitive predicate whose five source fields make repeated linear lookup more
visible, without changing production.

The implementation reuses the existing executor-private `BoundExpr`; no bound
IR or PhysicalPlan variant was added. `try_execute_filtered_counts` builds its
source-order predicate fields and calls `bind_expression` once before entering
the Heap visitor. The callback evaluates the resulting positions through a
checked ScalarRef getter and receives no fields, so no Column leaf can call
`find_source_position` in the row hot path. The existing Join owned-row wrapper
and the new filtered-count ScalarRef wrapper share one recursive bound semantic
core. Literal borrowing, three-valued logic, full two-sided AND/OR evaluation,
and owned computed values remain unchanged. Generic PhysicalPlan Filter still
uses `evaluate_truth` over owned SeqScan rows.

Three strictly serial full pre/post runs used separate
`/private/tmp/netbadb-phase7q-pre-target` and
`/private/tmp/netbadb-phase7q-post-target` build directories. Median-of-three
medians in milliseconds were:

| scenario | pre | post | change |
| --- | ---: | ---: | ---: |
| filtered Int64 equality COUNT(payload) | 0.737 | 0.665 | -9.7% |
| filtered repeated Int64 COUNT(payload) | 0.916 | 0.822 | -10.3% |
| filtered wide primitive COUNT(payload) | 1.524 | 1.242 | -18.5% |
| filtered Text equality COUNT(payload) | 0.774 | 0.696 | -10.0% |
| filtered repeated Text COUNT(payload) | 0.948 | 0.839 | -11.6% |
| filtered Bool equality COUNT(payload) | 0.807 | 0.759 | observational |
| filtered Text `IS NOT NULL` COUNT(payload) | 0.716 | 0.617 | observational |
| filtered Int64 `IS NOT NULL` COUNT(payload) | 0.706 | 0.627 | observational |
| generic hidden Text Filter | 1.641 | 1.494 | observational |

Repeated/single Int64 changed only from 1.243x to 1.235x and Text from
1.225x to 1.204x, confirming that the additional expression work dominates
those ratios. The wide/single-Int64 ratio contracted from 2.068x to 1.866x,
while Text/Int64 equality stayed near parity at 1.050x/1.046x and Text/Int64
`IS NOT NULL` at 1.014x/0.983x. The wide case therefore gives the clearest
positive attribution, but the generic Filter and controls also moved broadly;
there is no timing threshold and no claim that all absolute movement is caused
by prebinding.

Direct ID/nullable/payload/pair COUNT controls changed from
0.622/0.625/0.634/0.647 ms to 0.564/0.547/0.542/0.580 ms. Phase 7N filtered
ID/payload/pair controls changed from 0.780/0.807/0.805 ms to
0.713/0.759/0.753 ms. The bounded improvement and broad machine/code-layout
movement mean further Phase 7N micro-tuning is not selected.

The post-7Q full baseline selected direct COUNT(*) live-row specialization for
Phase 7R. Before changing production, Phase 7R added a triple-star scenario to
the existing single and pair cases. The implementation removes only the
artificial requirement that a direct-count plan contain COUNT(column). Existing
unused-scan-column validation therefore admits pure star outputs only for the
planner's `SeqScan[]` shape and continues to reject a future or malformed
all-star `SeqScan[column]`.

The resulting path calls the existing `scan_presence_counts([])` exactly once,
materializes no scanned `ExecutionRow` or per-row `ScalarValue`, and reuses the
exact checked `live_rows: u128` for every star output. Final SQL `u64`
conversion remains per output, so overflow is attributed through that output's
exact `AggregateExpr`. No storage API, statistic, cache, slot-only shortcut,
index-only path, dependency, or unsafe code was added. The zero-column summary
still decodes and validates every persisted scalar, including unrequested
Text, before counting the current live tuple.

Per the requested run limit, one serial full pre run and one serial full post
run used separate `/private/tmp/netbadb-phase7r-pre-target` and
`/private/tmp/netbadb-phase7r-post-target` directories. Their medians in
milliseconds were:

| scenario | pre | post | change |
| --- | ---: | ---: | ---: |
| direct COUNT(*) | 0.757 | 0.558 | -26.3% |
| direct COUNT(*), COUNT(*) | 0.731 | 0.550 | -24.6% |
| direct triple COUNT(*) | 0.739 | 0.545 | -26.3% |
| direct COUNT(id) | 0.584 | 0.650 | observational |
| direct COUNT(payload) | 0.581 | 0.585 | observational |
| filtered COUNT(*), COUNT(*) | 0.948 | 0.968 | observational |

COUNT(*)/COUNT(id) changed from 1.298x to 0.859x and
COUNT(*)/COUNT(payload) from 1.303x to 0.954x. Pair/single stayed
0.964x/0.986x and triple/single stayed 0.976x/0.976x, consistent with one Heap
traversal regardless of star-output multiplicity. Direct COUNT(id) moved
11.3% while the direct pair, filtered all-star, generic hidden Filter, and
other controls moved much less; these one-run wall-clock values have no timing
threshold and do not attribute every absolute change to Phase 7R.

The post-7R full baseline left generic hidden Text Filter at 1.619 ms. Phase 7S
added matching Int64, IS NULL, repeated-leaf, and wide dynamic-lookup cases,
then reused the existing dynamic borrowed evaluator in generic
`PhysicalPlan::Filter`. The Filter still receives owned child `ExecutionRows`.
Each Column leaf still performs binding-aware `find_source_position`, but the
selected owned row value is viewed as `ScalarRef` rather than cloned; Literal
leaves borrow from `Expr`. Binary, Unary, and IsNull results remain owned, and
AND/OR still evaluate both sides. TRUE moves the original owned row unchanged;
FALSE and UNKNOWN drop it. QueryResult remains fully owned.

Per the requested run limit, one serial full pre run and one serial full post
run used separate `/private/tmp/netbadb-phase7s-pre-target` and
`/private/tmp/netbadb-phase7s-post-full-target` directories. A post-change
quick run used a third independent target and passed every exact plan, result,
and base-column gate. Full-run medians in milliseconds were:

| scenario | pre | post | change |
| --- | ---: | ---: | ---: |
| hidden Text equality | 1.619354 | 1.191853 | -26.4% |
| hidden Int64 equality | 0.904979 | 0.912250 | observational |
| hidden Text IS NULL | 1.358145 | 1.109500 | -18.3% |
| hidden Int64 IS NULL | 0.866979 | 0.834645 | observational |
| hidden repeated Text | 2.202375 | 1.335562 | -39.4% |
| hidden repeated Int64 | 1.103104 | 1.059334 | observational |
| hidden wide dynamic lookup | 1.698895 | 1.673500 | observational |
| specialized filtered-count Text equality | 0.744437 | 0.726146 | observational |

Text/Int equality contracted from 1.789x to 1.306x, Text/Int IS NULL from
1.567x to 1.329x, and repeated/single Text from 1.360x to 1.121x. The matching
repeated/single Int64 ratio changed only from 1.219x to 1.161x. The wide/single
Int64 ratio remained 1.877x/1.834x. Phase 7N's specialized repeated-Text
control changed from 0.929541 to 0.899354 ms. Direct COUNT(*)/COUNT(id)/
COUNT(payload) controls changed from 0.575042/0.615562/0.612854 ms to
0.549937/0.586416/0.585979 ms. These one-run wall-clock controls have no timing
threshold, and their small absolute movement is observational.

The target-specific improvements and stable controls strongly attribute the
removed cost to dynamic leaf cloning. The remaining Text/Int IS NULL gap more
directly exposes owned predicate-only Text created by SeqScan, so Phase 7T
selects generic Filter-to-SeqScan ownership/streaming attribution first.
Generic Filter position prebinding remains second because the wide lookup case
is still 1.834x the single Int64 case. Sequential PageManager traversal,
BufferPool page snapshot cloning, AND/OR short-circuiting, MIN/MAX ownership,
group-key ownership, covering/index-only reads, broader HashJoin eligibility,
and multi-inequality intersection remain separate candidates. Phase 7S adds no
streaming, prebinding, storage visitor, predicate pushdown, dependency, unsafe
code, or compatibility change.

Phase 7T adds one authoritative row-aware sibling to the borrowed Heap visitor.
It yields the live slot's exact `RowId` plus ordered borrowed `ScalarRef` values
after complete persisted-row validation. Scratch storage remains per validated
page, the HRTB callback prevents a row borrow from escaping, the old borrowed
visitor ignores RowId through a thin wrapper, and the owned visitor continues
to delegate to borrowed traversal.

The executor consumes this boundary only for exact `Filter → SeqScan` with
unique, binding/table-consistent scan identities and every predicate identity
present in the scan output. It retains dynamic `find_source_position` lookup
on every Column leaf. FALSE and UNKNOWN own nothing; TRUE materializes every
SeqScan column and the exact RowId before returning to the existing parent.
This is intentionally not Filter/Project fusion: a predicate-only Text column
is still owned for a qualifying row even when Project immediately drops it.
All other physical shapes and malformed identities fall back to the prior
generic executor. Storage validation also retains priority over an earlier
predicate error by completing the scan before returning the saved first error.

Per the requested run limit, one serial full pre run and one serial full post
run used separate `/private/tmp/netbadb-phase7t-pre-target` and
`/private/tmp/netbadb-phase7t-post-target` directories. A post-change quick run
used the post target and passed every exact plan, result, base-column, and
checksum gate. Full-run medians in milliseconds were:

| scenario | pre | post | post/pre | change |
| --- | ---: | ---: | ---: | ---: |
| hidden Text equality | 1.172708 | 0.771625 | 0.658x | -34.2% |
| hidden Int64 equality | 0.891999 | 0.700458 | 0.785x | -21.5% |
| hidden Text IS NULL | 1.089500 | 0.695416 | 0.638x | -36.2% |
| hidden Int64 IS NULL | 0.838041 | 0.650563 | 0.776x | -22.4% |
| hidden repeated Text | 1.351271 | 0.962791 | 0.713x | -28.7% |
| hidden repeated Int64 | 1.062854 | 0.881395 | 0.829x | -17.1% |
| hidden wide dynamic lookup | 1.652646 | 1.441604 | 0.872x | -12.8% |
| all-TRUE hidden Text | 1.205125 | 1.296416 | 1.076x | +7.6% |
| all-TRUE hidden Int64 | 0.740396 | 0.827771 | 1.118x | +11.8% |
| selective owned Text output | 1.155479 | 0.737333 | 0.638x | -36.2% |
| specialized filtered-count Text equality | 0.744812 | 0.712458 | 0.957x | observational |
| direct COUNT(*) | 0.554833 | 0.546521 | 0.985x | observational |
| direct COUNT(id) | 0.604021 | 0.597542 | 0.989x | observational |
| direct COUNT(payload) | 0.592792 | 0.564624 | 0.952x | observational |

Text/Int equality contracted from 1.315x to 1.102x and Text/Int IS NULL from
1.300x to 1.069x, strongly attributing the selective improvements to avoiding
ownership for rejected rows. The all-TRUE Text/Int ratio remains 1.566x and
all-TRUE/selective Text expanded from 1.028x to 1.680x because every qualified
row must still own predicate-only Text. Wide/single Int64 also remains 2.058x.
These one-run wall-clock values have no timing threshold.

The post baseline therefore selects retained-column-aware Filter/Project
materialization for Phase 7U investigation; it targets qualified predicate-only
columns without changing this phase's Filter schema contract. Generic Filter
position prebinding remains the next strong candidate. Sequential PageManager
traversal, BufferPool page snapshot cloning, AND/OR short-circuiting, MIN/MAX
ownership, group-key ownership, covering/index-only reads, broader HashJoin
eligibility, and multi-inequality intersection remain separate candidates.
Phase 7T implements none of those follow-ups.

Phase 7U specializes exact `Project → Filter → SeqScan` inside the executor.
The complete borrowed SeqScan row still drives dynamic predicate evaluation
and full persisted validation. On TRUE, however, only the Project's precomputed
source positions become owned; FALSE and UNKNOWN still own nothing. This skips
the complete owned Filter `ExecutionRows` that Project would immediately
discard, without changing PhysicalPlan, Inspection JSON, planner pruning, the
Filter schema contract, or the fully owned QueryResult boundary. Shapes with
no predicate-only scan column, including retained Text, intentionally use the
Phase 7T/generic path.

Per the requested run limit, one serial full pre run and one serial full post
run used `/private/tmp/netbadb-phase7u-pre-target` and
`/private/tmp/netbadb-phase7u-post-target`. The retained-all-TRUE Text scenario
was present before the pre run. A post-change quick run used the post target;
every exact plan, base-column, row-count, checksum, and fixed-width Text gate
passed. No extra full rerun was performed. Full-run medians in milliseconds
were:

| scenario | pre | post | post/pre | change |
| --- | ---: | ---: | ---: | ---: |
| all-TRUE predicate-only Text | 1.292270 | 0.924291 | 0.715x | -28.5% |
| all-TRUE Int64 | 0.827728 | 0.896187 | 1.083x | +8.3% |
| all-TRUE retained Text | 0.952895 | 1.003125 | 1.053x | +5.3% |
| selective hidden Text equality | 0.772958 | 0.776166 | 1.004x | +0.4% |
| selective hidden Int64 equality | 0.758854 | 0.706167 | 0.931x | -6.9% |
| selective owned Text output | 0.746749 | 0.760812 | 1.019x | +1.9% |
| hidden wide dynamic lookup | 1.463499 | 1.582333 | 1.081x | +8.1% |
| specialized filtered-count Text equality | 0.702583 | 0.731021 | 1.040x | observational |
| direct COUNT(*) | 0.554562 | 0.543896 | 0.981x | observational |
| direct COUNT(id) | 0.580104 | 0.581833 | 1.003x | observational |
| direct COUNT(payload) | 0.605062 | 0.572146 | 0.946x | observational |

The predicate-only all-TRUE Text/Int64 ratio contracted from 1.561x to
1.031x, and predicate-only all-TRUE/selective Text contracted from 1.672x to
1.191x. In contrast, retained Text/Int64 remained 1.151x/1.119x, and the
retained Text control did not share the target's improvement. This directly
attributes the target reduction to avoiding ownership of qualified
predicate-only Text, not to borrowing output Text. The wide/single Int64 ratio
remained high and expanded from 1.929x to 2.241x. These one-run wall-clock
values have no timing threshold.

The post baseline therefore stops the Filter ownership line and selects
generic Filter position prebinding for Phase 7V; it is not implemented here.
Sequential PageManager traversal, BufferPool page snapshot cloning, AND/OR
short-circuiting, MIN/MAX ownership, group-key ownership, covering/index-only
reads, broader HashJoin eligibility, and multi-inequality intersection remain
later measured candidates. Phase 7U adds no dependency or unsafe code.

## Phase 55 batch execution attribution

Phase 55 adds six deterministic scenarios before changing production
execution: a full `id,payload` projection, Bool and Text Filter+Project, an
early `LIMIT 20`, Filter+`LIMIT 20`, and the same Filter+Project+Limit shape on
LSM. Each uses the existing exact row-count, checksum/value, PhysicalPlan,
base-column, `black_box`, warm-process, and no-timing-assertion gates.

One serial quick pre run was saved outside the repository at
`/tmp/netbadb-phase55-pre.txt`. After implementation and the final dispatch
review, the same target produced `/tmp/netbadb-phase55-post-review.txt`.
Medians are machine-local nanoseconds per query:

| scenario | pre median | post median | change |
| --- | ---: | ---: | ---: |
| Heap full projected scan | 358,250 | 416,334 | +16.2% |
| Heap Bool Filter+Project | 409,917 | 338,000 | -17.5% |
| Heap Text Filter+Project control | 393,292 | 326,708 | -16.9% |
| Heap early Limit | 353,084 | 15,125 | -95.7% |
| Heap Filter+Limit | 345,000 | 128,125 | -62.9% |
| LSM Filter+Project+Limit | 119,667 | 77,292 | -35.4% |

Early Limit supplied the clearest target reduction. Restoring the existing
borrowed streaming Filter dispatch also moved the Bool and Text Filter+Project
controls below their pre medians. The remaining full projected-scan increase
was smaller than contemporaneous unrelated-control movement: direct Heap item
scan changed from 354,917 to 431,375 ns and direct COUNT(*) from 239,625 to
303,625 ns. These quick runs are not a controlled throughput claim. The final
code is retained for bounded memory, decisive early-stop behavior, and reuse of
the measured streaming specialization where batch ownership is unnecessary.

An intermediate post run also exposed the architectural reason an owned batch
can regress selective predicates when it acquires output and predicate values
for every row before filtering. The final dispatch therefore retains the
existing borrowed Phase 7 streaming specialization for exact standalone Filter
and predicate-only Project/Filter shapes across scalar types. Filter+Limit
continues through the position-bound batch runtime to gain bounded early stop.
These are attribution results, not a latency or speedup contract, and no
wall-clock threshold was added.

## Phase 56 streaming Aggregate attribution

Phase 56 added its benchmark cases before changing production execution. The
targets cover global Int64 SUM, multiple mixed aggregates, Text MIN/MAX,
low- and higher-cardinality grouping, filtered grouped aggregation, and LSM
global SUM. Existing direct/filtered COUNT, full projected scan, early Limit,
and LSM batch-limit cases remain observational regression controls. Every case
keeps exact row/value/checksum, PhysicalPlan, base-column, `black_box`,
warm-process, and no-timing-assertion gates.

One serial quick pre run was saved outside the repository at
`/tmp/netbadb-phase56-pre.txt`; the same command after implementation produced
`/tmp/netbadb-phase56-post.txt`. Medians are machine-local nanoseconds per
query:

| scenario | pre median | post median | change |
| --- | ---: | ---: | ---: |
| Heap global SUM | 381,500 | 158,667 | -58.4% |
| Heap global mixed aggregates | 461,625 | 322,458 | -30.1% |
| Heap Text MIN/MAX | 564,125 | 481,000 | -14.7% |
| Heap grouped mixed aggregates | 556,042 | 221,375 | -60.2% |
| Heap filtered grouped aggregate | 477,000 | 260,708 | -45.3% |
| Heap low-cardinality GROUP BY target | 413,917 | 192,792 | -53.4% |
| Heap higher-cardinality GROUP BY target | 547,709 | 419,250 | -23.5% |
| LSM global SUM | 108,917 | 36,875 | -66.1% |
| direct COUNT(*) control | 310,125 | 168,750 | -45.6% |
| direct COUNT(id) control | 240,375 | 171,291 | -28.7% |
| filtered COUNT(payload) control | 317,250 | 137,125 | -56.8% |
| early Limit control | 19,042 | 17,167 | -9.8% |
| full projected scan control | 414,666 | 319,958 | -22.8% |
| LSM Filter+Project+Limit control | 99,583 | 40,125 | -59.7% |

The target reductions are consistent with removing the full materialized child
`ExecutionRows` before Aggregate, while the broad contemporaneous control
movement shows that these quick wall-clock deltas are attribution evidence,
not a stable throughput claim. The retained design decision is therefore the
bounded producer/consumer composition and its explicit memory bound, not a
latency guarantee. Direct and filtered COUNT controls still take their prior
specialized paths; no timing threshold was added.

Global Aggregate now retains at most one 256-row input batch plus its state.
Grouped Aggregate additionally retains one key and state set per distinct
group and preserves first-seen order. Both Heap and LSM use the same executor
callback, and no storage interface or persistent representation changed.

## Phase 57 move-aware Aggregate ownership attribution

Phase 57 added separate ascending-fixture Text MIN, Text MAX, duplicate Text
MAX, and Int64 MIN/MAX cases before production changes. MIN normally replaces
once, MAX replaces on every row, and duplicate MAX requires two final owners.
The existing Text MIN+MAX, SUM, grouped Aggregate, COUNT, Limit, projected scan,
and LSM cases remain target or observational controls. Every scenario retains
exact plan, result/value, base-column, `black_box`, warm-process, and
no-timing-threshold gates.

The serial quick runs are stored outside the repository at
`/tmp/netbadb-phase57-pre.txt` and `/tmp/netbadb-phase57-post.txt`. Medians are
machine-local nanoseconds per query:

| scenario | pre median | post median | change |
| --- | ---: | ---: | ---: |
| Text MIN, one replacement | 426,375 | 593,417 | +39.2% |
| Text MAX, frequent replacement | 541,834 | 550,542 | +1.6% |
| Text MIN+MAX | 594,541 | 787,583 | +32.5% |
| duplicate Text MAX | 747,625 | 626,208 | -16.2% |
| Int64 MIN control | 367,042 | 467,833 | +27.5% |
| Int64 MAX control | 424,458 | 481,083 | +13.3% |
| Int64 MIN+MAX control | 382,458 | 457,583 | +19.6% |
| global SUM control | 471,833 | 476,500 | +1.0% |
| grouped mixed aggregate | 455,959 | 556,417 | +22.0% |
| filtered grouped aggregate | 488,208 | 533,667 | +9.3% |
| low-cardinality GROUP BY | 418,750 | 643,125 | +53.6% |
| higher-cardinality GROUP BY | 490,334 | 637,042 | +29.9% |
| direct COUNT(*) control | 260,417 | 254,792 | -2.2% |
| direct COUNT(id) control | 380,333 | 312,917 | -17.7% |
| filtered COUNT(payload) control | 327,125 | 366,791 | +12.1% |
| early Limit control | 22,959 | 15,125 | -34.1% |
| full projected scan control | 443,583 | 382,083 | -13.9% |
| LSM SUM control | 72,708 | 98,458 | +35.4% |
| LSM Filter+Project+Limit control | 76,292 | 98,584 | +29.2% |

Absolute results are visibly noisy: unrelated controls range from -34.1% to
+35.4%, so the positive MIN and grouping deltas cannot be assigned solely to
the implementation. Within each run, the ownership-sensitive ratios are more
diagnostic. Text MAX/MIN contracted from 1.271x to 0.928x, duplicate MAX/MAX
from 1.380x to 1.137x, and MIN+MAX/MIN from 1.394x to 1.327x. This matches the
replacement hypothesis: high- and multi-owner replacement work improved
relative to the low-replacement baseline. Structural pointer-identity tests
independently prove that one actual owner receives the original String
allocation and only additional owners clone it.

The implementation is retained for that structural ownership guarantee and
relative attribution, not as an absolute latency claim. Pure COUNT/SUM bypass
replacement bookkeeping, direct/filtered COUNT dispatch remains unchanged,
and no timing assertion or CI performance threshold was added. Group-key
ownership/hash attribution is the leading next measurement; Text comparison
and generic Filter prebinding follow.

## CI and compatibility

`cargo check --workspace --all-targets` compiles the benchmark, including on
the Rust 1.85 MSRV. The expensive workload is not a normal CI performance gate
and has no pass/fail timing threshold.

Phase 7B changed the Inspection JSON contract from v1 to v2 for
RangeIndexScan. Phase 7D changes the current contract from v2 to v3 solely to
represent HashJoin. Phases 7E through 7O introduce no plan or inspection
change, so v3 remains current. They change no NetbaDB Protocol v1 message, SDK
Schema Spec v1 field, deployment manifest v4 field, or database persistent
format. At completion of Phases 7N and 7O, Canonical Schema v1, Heap metadata v3,
Page v5, WAL v3, WAL record v2, BTree payload v1, IndexCatalog v2, and row
encoding unchanged. Phases 7P through 7T retain those contracts as well as
Protocol v1, SDK Schema Spec v1, manifest v4, Inspection JSON v3, and fully owned
QueryResult rows. Phase 7U retains those contracts as well. The later MVCC
phase deliberately advances Heap metadata to v4 and adds tuple/status formats;
it does not invalidate these performance-path results. These phases add no
dependency and no unsafe code.
