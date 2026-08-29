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

Phase 7D's initial HashJoin materialized both children, built the right child
into buckets of right-row indices, probed in left input order, and evaluated the
complete typed predicate for every bucket candidate before materializing TRUE
rows. NULL keys were excluded from both build and probe. Right indices retained
input order, preserving deterministic left-major/right-minor behavior without
turning unordered SQL output into a language guarantee. At that phase there
was no join reordering, dynamic build-side choice, composite key, index
coupling, spilling, or global cost model; Phase 70 later adds only the private
build-side choice.

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

## Phase 58 borrowed group-key lookup attribution

Phase 58 added deterministic one-key cardinality 1, 4, 1%, 50%, and 100%
sweeps, plus `(Int64, Bool)` and unique Text group keys before changing
production lookup. Every query retains exact physical-plan, base-column,
first-seen result/value, `black_box`, warm-process, and no-timing-threshold
gates. The quick raw runs are stored outside the repository at
`/tmp/netbadb-phase58-pre.txt` and `/tmp/netbadb-phase58-post.txt`.

Medians are machine-local nanoseconds per query:

| scenario | cardinality | key shape | pre median | post median | change |
| --- | ---: | --- | ---: | ---: | ---: |
| one group | 1/1,000 | Int64 | 469,167 | 390,958 | -16.7% |
| four groups | 4/1,000 | Int64 | 541,791 | 604,500 | +11.6% |
| one-percent groups | 10/1,000 | Int64 | 572,208 | 580,167 | +1.4% |
| half unique | 500/1,000 | Int64 | 772,792 | 502,166 | -35.0% |
| all unique | 1,000/1,000 | Int64 | 550,459 | 885,416 | +60.9% |
| two primitive keys | 8/1,000 | Int64 + Bool | 730,125 | 520,750 | -28.7% |
| all-unique Text | 1,000/1,000 | Text | 1,183,208 | 880,708 | -25.6% |

Raw timing is noisy and not monotonic by cardinality. The hit/miss-shaped
within-run ratios are more useful: one-group/unique changed from 0.852x to
0.442x, four-group/unique from 0.984x to 0.683x, and two-key/four-group from
1.348x to 0.861x. This supports removal of the previous per-hit owned key,
while unique Int64 grouping identifies the remaining miss/hash path as the
leading residual. Test-only counters provide the stronger structural result:
513 rows over four groups perform 509 allocation-free, clone-free hits and four
durable key materializations; 513 equal Text keys materialize one durable
String-bearing key.

Controls confirm that the machine moved broadly between serial runs:

| control | pre median | post median | change |
| --- | ---: | ---: | ---: |
| Phase 57 Text MIN | 409,209 | 487,083 | +19.0% |
| Phase 57 Text MAX | 455,792 | 386,792 | -15.1% |
| Phase 57 Text MIN+MAX | 521,667 | 626,167 | +20.0% |
| Phase 57 duplicate Text MAX | 521,250 | 652,083 | +25.1% |
| global SUM | 377,000 | 604,708 | +60.4% |
| grouped mixed aggregate | 478,125 | 625,125 | +30.7% |
| filtered grouped aggregate | 425,375 | 404,084 | -5.0% |
| prior low-cardinality GROUP BY | 644,875 | 584,875 | -9.3% |
| prior higher-cardinality GROUP BY | 442,542 | 590,709 | +33.5% |
| direct COUNT(*) | 453,208 | 613,708 | +35.4% |
| filtered COUNT(payload) | 238,959 | 447,542 | +87.3% |
| early Limit | 8,541 | 39,875 | +366.9% |
| full projected scan | 361,791 | 508,791 | +40.6% |
| LSM SUM | 44,541 | 74,333 | +66.9% |

The implementation is retained for the exact structural ownership reduction,
not as a claim of stable throughput improvement. Global aggregation bypasses
group lookup, direct/filtered COUNT priority is unchanged, and no timing
assertion or CI threshold was introduced.

## Phase 59 move-on-miss group-key ownership attribution

Phase 59 added all-unique Text key-only, Text key plus one/two MAX owners, and
all-unique `(Int64, Text)` cases before changing production ownership. The
existing unique Text+COUNT, primitive cardinality sweep, plan/base-column
checks, first-seen value validation, warm-process execution, `black_box`, and
no-timing-threshold policy remain unchanged. Raw quick runs are stored outside
the repository at `/tmp/netbadb-phase59-pre.txt` and
`/tmp/netbadb-phase59-post.txt`.

Medians are machine-local nanoseconds per query. “Durable owners” counts the
values that must survive one miss; COUNT is borrowed and adds no scalar owner.

| scenario | cardinality | key shape | durable owners | pre median | post median | change |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| Text key only | 1,000/1,000 | Text | 1 | 827,250 | 743,542 | -10.1% |
| Text key + COUNT | 1,000/1,000 | Text | 1 | 933,792 | 829,292 | -11.2% |
| Text key + MAX | 1,000/1,000 | Text | 2 | 798,542 | 812,750 | +1.8% |
| Text key + duplicate MAX | 1,000/1,000 | Text | 3 | 988,583 | 830,584 | -16.0% |
| primitive key + COUNT | 1,000/1,000 | Int64 | 1 | 718,709 | 777,458 | +8.2% |
| wide key + COUNT | 1,000/1,000 | Int64 + Text | 2 | 937,875 | 770,416 | -17.9% |

The key-only versus key+MAX pair provides the clearest ownership-shaped signal:
the no-clone target improved 10.1%, while the mandatory-clone control moved
1.8% slower. Unique Text+COUNT and the wide unique key also improved, whereas
unique Int64 regressed despite eliminating its cheap clone. Duplicate MAX's
large improvement conflicts with a simple owner-count curve and is treated as
noise, not as evidence that mandatory clones disappeared.

Structural tests are the primary result. Test-only accumulator counters report:

| workload | key moves | key clones |
| --- | ---: | ---: |
| 513 rows / 4 Int64 groups | 4 | 0 |
| 513 unique Int64 groups | 513 | 0 |
| 513 unique Text groups | 513 | 0 |
| Text key + MAX(Text) | 1 | 1 |
| Text key + MAX(Text) + MAX(Text) | 1 | 2 |

A pointer-identity test additionally proves that the original unique Text
allocation moves into `GroupState`. Repeated group-key slots and `(Int64,
Bool)`, `(Int64, nullable Int64)`, and `(Int64, Text)` keys preserve source
order, NULL grouping, legacy-result equality, and one move per uniquely owned
source value.

Controls again show substantial machine movement:

| control | pre median | post median | change |
| --- | ---: | ---: | ---: |
| Phase 57 Text MIN | 460,250 | 469,125 | +1.9% |
| Phase 57 Text MAX | 467,000 | 458,042 | -1.9% |
| Phase 57 Text MIN+MAX | 398,875 | 492,541 | +23.5% |
| Phase 57 duplicate Text MAX | 567,125 | 447,625 | -21.1% |
| global SUM | 395,375 | 425,000 | +7.5% |
| Phase 58 one group | 394,125 | 403,250 | +2.3% |
| Phase 58 four groups | 453,583 | 409,250 | -9.8% |
| Phase 58 half unique | 685,792 | 558,583 | -18.5% |
| direct COUNT(*) | 241,417 | 252,708 | +4.7% |
| filtered COUNT(payload) | 372,375 | 286,167 | -23.2% |
| early Limit | 20,583 | 15,000 | -27.1% |
| full projected scan | 364,458 | 351,042 | -3.7% |
| LSM SUM | 63,500 | 64,750 | +2.0% |

The implementation is retained for exact move-on-miss ownership, not as a
stable throughput claim. The primitive unique regression and broad control
spread make group hashing plus owned-row bookkeeping the leading Phase 60
measurement, followed by Aggregate Text comparison and generic Filter
prebinding. Column-oriented batches remain unselected.

## Phase 60 prehashed group bucket lookup attribution

Phase 60 reused the existing one-group, four-group, unique Int64, two primitive
key, unique Text, and wide unique scenarios before changing production code.
The same quick command ran serially before and after the change, with raw output
stored outside the repository at `/tmp/netbadb-phase60-pre.txt` and
`/tmp/netbadb-phase60-post.txt`. Plans, base-column gates, exact first-seen
results, warm-process execution, `black_box`, and the no-timing-threshold policy
remain unchanged.

Medians are machine-local nanoseconds per query:

| scenario | cardinality | key shape | pre median | post median | change |
| --- | ---: | --- | ---: | ---: | ---: |
| one group | 1/1,000 | Int64 | 588,667 | 460,875 | -21.7% |
| four groups | 4/1,000 | Int64 | 523,708 | 370,125 | -29.3% |
| all unique | 1,000/1,000 | Int64 | 900,333 | 632,750 | -29.7% |
| two primitive keys | 8/1,000 | Int64 + Bool | 631,875 | 387,333 | -38.7% |
| all-unique Text | 1,000/1,000 | Text | 959,500 | 919,166 | -4.2% |
| wide unique | 1,000/1,000 | Int64 + Text | 980,083 | 720,917 | -26.4% |

The cheap primitive cases improved more than the expensive unique-Text control,
which is the expected shape when the removed second `u64` hash is fixed work
and first-level Text hashing remains. Unique Int64 and wide unique also improved
materially, so the result is not a clean width curve and must be read alongside
the noisy controls. Structurally, the executor still hashes key width and
ordered group values through a keyed `RandomState`; only the resulting opaque
`u64` enters a private pass-through bucket map. Exact `ScalarValue` collision
comparison, NULL grouping, and first-seen `Vec<GroupState>` order are unchanged.

Controls moved broadly in both directions:

| control | pre median | post median | change |
| --- | ---: | ---: | ---: |
| global SUM | 380,833 | 542,833 | +42.5% |
| Phase 57 Text MIN | 599,666 | 401,333 | -33.1% |
| Phase 57 Text MAX | 401,125 | 402,250 | +0.3% |
| Phase 57 Text MIN+MAX | 600,791 | 535,084 | -10.9% |
| Phase 57 duplicate Text MAX | 778,209 | 717,667 | -7.8% |
| Phase 59 Text key-only | 800,166 | 539,958 | -32.5% |
| Phase 59 Text + MAX | 950,167 | 906,959 | -4.5% |
| Phase 59 duplicate MAX | 1,023,959 | 756,500 | -26.1% |
| direct COUNT(*) | 352,667 | 469,209 | +33.0% |
| filtered COUNT(payload) | 435,958 | 324,291 | -25.6% |
| early Limit | 15,750 | 15,208 | -3.4% |
| full projected scan | 363,375 | 540,000 | +48.6% |
| LSM SUM | 65,208 | 163,709 | +151.1% |

The control spread demonstrates significant machine/code-layout variance, so
the single quick comparison is directional rather than a stable throughput
claim. The implementation is retained because structural tests prove that
bucket hashing returns the exact prehash for `0`, `1`, `u64::MAX`, and
representative mixed-bit values, distinct prehashes retrieve distinct heads,
generic byte hashing is rejected, and a forced same-prehash chain still uses
exact typed key equality. Ownership counters and legacy-equivalence tests remain
unchanged. The remaining hit-heavy grouped cost selects selective owned-row
bookkeeping as Phase 61; Aggregate Text comparison, generic Filter prebinding,
and broader column-oriented execution remain separate candidates.

## Phase 61 borrowed-first grouped batch attribution

Phase 61 added group-only cardinality 1/4, one-group SUM, and one-group ascending
Text MIN/MAX scenarios before changing production consumption. Existing Phase
60 cardinality, primitive-width, unique Text, and wide unique cases remain exact
controls. The same quick command ran serially before and after, with raw output
stored outside the repository at `/tmp/netbadb-phase61-pre.txt` and
`/tmp/netbadb-phase61-post.txt`.

Medians are machine-local nanoseconds per query:

| scenario | groups | transfer shape | pre median | post median | change |
| --- | ---: | --- | ---: | ---: | ---: |
| group-only | 1 | one miss transfer | 550,000 | 360,625 | -34.4% |
| group-only | 4 | four miss transfers | 463,875 | 541,625 | +16.8% |
| COUNT(*) | 1 | one miss transfer | 500,791 | 369,875 | -26.1% |
| SUM(id) | 1 | one miss transfer | 464,750 | 431,667 | -7.1% |
| Text MIN | 1 | one miss/replacement transfer | 578,125 | 287,667 | -50.2% |
| Text MAX | 1 | every row transfers | 511,583 | 589,750 | +15.3% |

The MIN/MAX pair is the strongest attribution signal: both hash and exact-probe
the same one-group Text shape, but ascending MIN has 999 borrow-only hits while
ascending MAX transfers its candidate on every row. MIN improved materially and
MAX did not. One-group group-only and COUNT also improved, while SUM improved
more modestly. Four-group group-only regressed and four-group COUNT was flat, so
the quick run does not support a uniform absolute latency claim.

Phase 60 grouped controls were:

| scenario | groups | key shape | pre median | post median | change |
| --- | ---: | --- | ---: | ---: | ---: |
| one-group Int64 COUNT | 1 | Int64 | 500,791 | 369,875 | -26.1% |
| four-group Int64 COUNT | 4 | Int64 | 422,958 | 424,875 | +0.5% |
| unique Int64 COUNT | 1,000 | Int64 | 722,042 | 659,916 | -8.6% |
| two primitive keys | 8 | Int64 + Bool | 389,166 | 386,875 | -0.6% |
| unique Text COUNT | 1,000 | Text | 871,417 | 799,666 | -8.2% |
| wide unique COUNT | 1,000 | Int64 + Text | 913,625 | 887,792 | -2.8% |

Non-target controls again moved in both directions:

| control | pre median | post median | change |
| --- | ---: | ---: | ---: |
| global SUM | 325,167 | 481,708 | +48.1% |
| global Text MIN | 494,625 | 392,458 | -20.7% |
| global Text MAX | 475,708 | 397,708 | -16.4% |
| direct COUNT(*) | 389,250 | 244,208 | -37.3% |
| filtered COUNT(payload) | 439,125 | 442,083 | +0.7% |
| early Limit | 21,333 | 16,792 | -21.3% |
| full projected scan | 352,208 | 355,208 | +0.9% |
| LSM SUM | 95,542 | 82,292 | -13.9% |

Structural counters are the authoritative Phase 61 result. Across 513 rows,
one/four/unique COUNT report 512/509/0 borrow-only hits and 1/4/513 transfer
rows. One-group ascending Text MIN reports 512 borrow-only hits and one transfer
row; MAX reports zero borrow-only hits and 513 transfer rows. Duplicate MAX
pointer identity still proves clone `N - 1` plus one move, and a forced
same-prehash hit performs exact collision comparisons without moving its row.
A mid-batch grouped SUM overflow returns the same domain error, clears every row,
and retains batch capacity. Batch/legacy and Heap/LSM equivalence remain the
semantic gates.

The ownership/bookkeeping line ends here: the implementation structurally
removes whole-row drain work from ordinary grouped hits, but control variance
prevents a stronger throughput claim. Phase 62 should investigate Aggregate
Text comparison; generic Filter position prebinding remains next, while typed
column batches are still premature.

## Phase 62 typed MIN/MAX comparison attribution

Phase 62 first added three deterministic global `MIN(payload)` fixtures with
1,000 rows and exactly 64 bytes per Text value. Each shape replaces only the
initial state: early-difference candidates diverge at byte 0, long-common-prefix
candidates diverge at byte 63, and all-equal candidates compare all 64 bytes.
The existing ascending Text MIN/MAX, one-group Text MIN/MAX, Int64 extrema, and
non-Aggregate scenarios remain controls. Raw serial quick runs are stored
outside the repository at `/tmp/netbadb-phase62-pre.txt` and
`/tmp/netbadb-phase62-post.txt`.

Medians are machine-local nanoseconds per query:

| scenario | Text comparison shape | replacement shape | pre median | post median | change |
| --- | --- | --- | ---: | ---: | ---: |
| early-difference MIN | differs at byte 0 | first row only | 506,208 | 521,666 | +3.1% |
| long-common-prefix MIN | differs at byte 63 | first row only | 425,500 | 229,958 | -46.0% |
| all-equal MIN | equal across 64 bytes | first row only | 422,333 | 521,791 | +23.5% |

The requested within-run comparison ratios were:

| ratio | pre | post |
| --- | ---: | ---: |
| all-equal / early-difference | 0.834x | 1.000x |
| common-prefix / early-difference | 0.841x | 0.441x |

These measurements do not form a credible lexical-comparison curve: the
common-prefix case was unexpectedly faster than early-difference in both runs,
and its relative movement conflicts with the all-equal case. The result cannot
separate `str::cmp` cost from machine and code-layout variance. Phase 62
therefore makes no stable latency claim and does not add a timing threshold or
custom Text comparator.

Extrema controls also moved in conflicting directions:

| control | pre median | post median | change |
| --- | ---: | ---: | ---: |
| global Int64 MIN | 335,375 | 425,208 | +26.8% |
| global Int64 MAX | 420,667 | 439,625 | +4.5% |
| global Int64 MIN+MAX | 450,083 | 447,000 | -0.7% |
| global ascending Text MIN | 396,292 | 493,333 | +24.5% |
| global ascending Text MAX | 400,541 | 504,292 | +25.9% |
| global Text MIN+MAX | 502,459 | 1,006,584 | +100.3% |
| one-group Text MIN | 525,625 | 555,041 | +5.6% |
| one-group Text MAX | 473,750 | 675,083 | +42.5% |
| grouped primitive mixed Aggregate | 553,459 | 535,625 | -3.2% |

Non-target controls confirm broad run variance:

| control | pre median | post median | change |
| --- | ---: | ---: | ---: |
| global SUM | 353,250 | 412,875 | +16.9% |
| Phase 61 one-group group-only | 370,791 | 461,875 | +24.6% |
| Phase 61 one-group COUNT | 445,708 | 537,917 | +20.7% |
| direct COUNT(*) | 303,333 | 431,625 | +42.3% |
| filtered COUNT(payload) | 291,750 | 321,167 | +10.1% |
| early Limit | 18,542 | 15,500 | -16.4% |
| full projected scan | 420,458 | 498,625 | +18.6% |
| LSM SUM | 64,917 | 96,959 | +49.4% |

The authoritative result is structural. Aggregate construction now selects a
private Bool, Int64, UInt64, or Text extrema state from typed metadata. Candidate
comparison directly matches that state; Text borrows both `String` values and
calls `str::cmp`, without `ScalarValue` to `ScalarRef` pair dispatch or a new
allocation. Deterministic pair tests prove the replacement decision matches the
generic comparator for primitive values, empty/ASCII/Unicode Text, equal Text,
and long common prefixes. Across 513 equal Text candidates, MIN and MAX each
replace once, reject 512 equal candidates, and retain the first String allocation.
Existing pointer tests still prove one move for one owner and clone `N - 1`
plus one move for duplicate/global/group-overlap owners. Empty, all-NULL,
nullable mixed, wrong-type, invalid-input, batch/legacy, and Heap/LSM tests
remain semantic gates.

Aggregate ownership and comparison micro-tuning ends with Phase 62. Phase 63
should implement Generic Filter position prebinding, where prior wide-versus-
single-predicate attribution showed a clearer residual. Typed column batches,
HashJoin batch integration, Sort/Top-N, and index/range/partition batch sources
remain later candidates.

## Phase 63 generic Filter position prebinding

Phase 63 adds a deterministic pair in which both queries return
`id, team_id, bucket_id, active`, scan those same four primitive columns, and
produce one middle row through exact `Project>Filter>SeqScan`. Both predicates
contain five comparisons and four AND nodes. The narrow control resolves every
identity leaf at the first `id` position; the wide target uses id, team, bucket,
and active positions before the same final selective id comparison. Plan shape,
base scan columns, row values, and result cardinality are hard benchmark gates.
Raw serial quick runs are stored outside the repository at
`/tmp/netbadb-phase63-pre.txt` and `/tmp/netbadb-phase63-post.txt`.

Machine-local medians are nanoseconds per query:

| scenario | lookup shape | pre median | post median | change |
| --- | --- | ---: | ---: | ---: |
| narrow-position repeated primitive | first position | 626,958 | 651,292 | +3.9% |
| wide-position primitive | first/middle/last | 556,417 | 552,791 | -0.7% |
| hidden Int64 equality | one primitive leaf | 330,459 | 366,917 | +11.0% |
| hidden Text equality | one Text leaf | 368,875 | 347,625 | -5.8% |
| hidden repeated Int64 | repeated primitive leaf | 456,333 | 540,541 | +18.5% |
| hidden repeated Text | repeated Text leaf | 341,625 | 452,750 | +32.5% |
| point IndexScan Filter | legacy materialized child | 29,500 | 22,834 | -22.6% |

The required wide/narrow ratio changed from 0.887x pre to 0.849x post. The
ratio contracted slightly but remained below 1.0, and narrow/wide timing moved
in opposite directions. This single quick run therefore does not demonstrate a
stable position-search cost curve and supports no throughput claim or timing
threshold. The structural result is authoritative: valid borrowed streaming
callbacks receive a prebound expression and scalar slice with no fields, while
valid legacy Filter binds once after materializing its child.

Existing generic Filter controls also moved inconsistently:

| control | pre median | post median | change |
| --- | ---: | ---: | ---: |
| hidden payload IS NULL | 362,125 | 284,958 | -21.3% |
| hidden payload IS NOT NULL | 358,542 | 445,250 | +24.2% |
| hidden owned Text output | 367,042 | 446,042 | +21.5% |
| hidden wide lookup control | 555,458 | 565,375 | +1.8% |

Non-target controls show still broader machine/code-layout variance:

| control | pre median | post median | change |
| --- | ---: | ---: | ---: |
| global SUM | 350,542 | 317,125 | -9.5% |
| global Text MIN | 568,583 | 382,667 | -32.7% |
| global Text MAX | 392,292 | 278,875 | -28.9% |
| one-group GROUP BY | 399,583 | 435,917 | +9.1% |
| direct COUNT(*) | 237,959 | 237,292 | -0.3% |
| prebound filtered COUNT(payload) | 366,750 | 431,833 | +17.7% |
| early Limit | 19,041 | 19,583 | +2.8% |
| full projected scan | 454,792 | 449,250 | -1.2% |
| LSM SUM | 97,208 | 28,000 | -71.2% |

The executor reuses its existing `BoundExpr`: streaming setup binds against the
source-order predicate fields before storage traversal and evaluates borrowed
`ScalarRef` rows by position; legacy Filter binds once against materialized
child fields. Binding failure deliberately retains dynamic row-dependent
evaluation for malformed plans, so an empty malformed child remains empty and
a nonempty child reports the prior error. Bool, Int64, UInt64, Text, NULL,
nullable, comparison, AND/OR/NOT, and IS NULL semantics remain equivalent;
AND/OR still evaluate both operands. Batch Filter, filtered COUNT, and Join
prebinding are unchanged.

Leaf-lookup micro-optimization ends here. Phase 64 should remeasure typed
column-oriented batches, HashJoin batch integration, Sort/Top-N, and
index/range/partition batch sources before selection. AND/OR short-circuiting
remains separate work; Phase 62's Text comparator line is not reopened.

## Phase 64 bounded Top-N over the batch producer

Phase 64 adds an order-sensitive K sweep over one shared 1,000-row fixture.
Every observer checks the complete ordered ID sequence, exact
`Limit>Project>Sort>...` tree, and exact base-scan ColumnIds. Coverage includes
the target `ORDER BY team_id, id LIMIT 1`, duplicate-heavy K values 1, 20, 256,
257, N/2, and N, unique descending and multi-key ordering, Filter, Text, all
four explicit nullable direction/placement combinations, a full Sort without
Limit, and an LSM multi-key Top-N. The existing `order_by_limit` case remains
unchanged. Raw serial quick runs are stored outside the repository at
`/tmp/netbadb-phase64-pre.txt` and `/tmp/netbadb-phase64-post.txt`.

Machine-local medians are nanoseconds per query:

| scenario | N | K | key shape | pre | post | change |
| --- | ---: | ---: | --- | ---: | ---: | ---: |
| target A | 1,000 | 1 | duplicate team + unique ID | 683,916 | 360,291 | -47.3% |
| duplicate sweep | 1,000 | 1 | four team values | 546,833 | 394,000 | -27.9% |
| duplicate sweep | 1,000 | 20 | four team values | 503,750 | 367,958 | -27.0% |
| duplicate sweep | 1,000 | 256 | four team values | 558,500 | 459,709 | -17.7% |
| duplicate sweep | 1,000 | 257 | four team values | 461,791 | 411,875 | -10.8% |
| duplicate sweep | 1,000 | 500 | four team values | 404,375 | 398,709 | -1.4% |
| duplicate sweep | 1,000 | 1,000 | four team values | 391,167 | 432,291 | +10.5% |
| unique descending | 1,000 | 20 | unique ID | 270,625 | 304,583 | +12.5% |
| multi-key | 1,000 | 20 | team ASC, ID DESC | 475,667 | 284,166 | -40.3% |
| filtered | 1,000 | 20 | four team values | 310,625 | 267,250 | -14.0% |
| Text descending | 1,000 | 20 | unique fixed-width payload | 318,500 | 344,708 | +8.2% |
| nullable ASC FIRST | 1,000 | 20 | 1% NULL Int64 | 303,000 | 221,833 | -26.8% |
| nullable ASC LAST | 1,000 | 20 | 1% NULL Int64 | 308,875 | 222,166 | -28.1% |
| nullable DESC FIRST | 1,000 | 20 | 1% NULL Int64 | 293,083 | 280,875 | -4.2% |
| nullable DESC LAST | 1,000 | 20 | 1% NULL Int64 | 283,667 | 261,542 | -7.8% |
| full Sort control | 1,000 | none | four team values | 281,833 | 263,042 | -6.7% |
| existing `order_by_limit` | 1,000 | 20 | four team values | 255,958 | 342,833 | +33.9% |
| LSM multi-key | 250 | 20 | team ASC, ID DESC | 185,834 | 100,084 | -46.1% |

The requested duplicate-key ratios changed as follows: K=20/full Sort moved
from 1.787x to 1.399x; K=1/K=N moved from 1.398x to 0.911x; and K=20/K=N moved
from 1.288x to 0.851x. Target A improved from 683,916 ns to 360,291 ns, while
the LSM multi-key case improved from 185,834 ns to 100,084 ns.

This quick run supports no general throughput claim or timing gate. Some small
K shapes improved, some regressed, K near N pays expected heap maintenance, and
non-target controls again moved widely. The authoritative Phase 64 result is
structural: the eligible executor retains at most `min(K, rows_seen)` candidate
rows plus one 256-row batch instead of materializing and sorting the complete
input. Test-only counters prove all 513 rows are consumed even for K=0/1, the
maximum retained count never exceeds K, and equal keys preserve input order.
Full Sort, malformed and unsupported shapes, physical plans, public APIs,
storage, and persistent formats remain unchanged. A costed K/N crossover,
spilling, full batch Sort, and upstream cancellation remain future measured
work.

## Phase 65 streaming HashJoin probe over the batch producer

Phase 65 adds five plan-gated asymmetric HashJoin scenarios. Every target
requires `HashJoin`, forbids `NestedLoopJoin` and index access, checks the exact
ordered projected IDs, and retains the historical unique, duplicate, and
no-match controls at both symmetric scales. Quick pre/post output is stored
outside the repository at `/tmp/netbadb-phase65-pre.txt` and
`/tmp/netbadb-phase65-post.txt`.

Machine-local medians are nanoseconds per query:

| scenario | probe rows | build rows | result rows | pre | post | change |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| large probe / small build, no match | 4,096 | 64 | 0 | 807,375 | 743,000 | -8.0% |
| small probe / large build, no match | 64 | 4,096 | 0 | 1,039,583 | 910,666 | -12.4% |
| large probe / small build, unique subset | 4,096 | 64 | 64 | 794,000 | 838,292 | +5.6% |
| large probe / small build, ordered duplicate bucket | 4,096 | 64 | 64 | 805,208 | 734,875 | -8.7% |
| large probe / small build, residual predicate | 4,096 | 64 | 64 | 1,026,834 | 1,026,500 | -0.0% |
| existing symmetric unique small | 100 | 100 | 100 | 130,250 | 109,709 | -15.8% |
| existing symmetric unique large | 300 | 300 | 300 | 138,416 | 191,375 | +38.3% |
| existing symmetric duplicate large | 300 | 300 | 3,000 | 307,125 | 304,250 | -0.9% |
| existing symmetric no-match large | 300 | 300 | 0 | 103,958 | 95,792 | -7.9% |

The requested large-probe/small-build divided by
small-probe/large-build no-match ratio moved from 0.777x to 0.816x. Both
absolute cases improved in this serial pair, but the reverse control improved
more, the unique-subset target regressed, and the residual target was flat.
This run therefore supports no general latency or throughput claim. The authoritative result is
structural: the eligible left input no longer has a full `ExecutionRows`
intermediate. Test-only statistics for 513 probe rows report 513 rows seen,
three batches, and a maximum batch of 256; a separate build control reports all
17 right rows materialized. Input-side intermediate memory changes from
`O(left input + right input + hash metadata)` to
`O(right build input + hash metadata + probe batch)`, plus the unchanged fully
owned `O(result rows)` output.

The right child remains the fixed build side. Its owned rows, cloned hash keys,
`HashMap<ScalarValue, Vec<usize>>`, and right insertion order are unchanged.
The probe callback borrows left rows, reuses `hash_join_key`, checks the full
once-bound residual predicate for every candidate, and reuses
`project_join_values` only for TRUE output. NULL keys still do not match;
left-major/right-minor duplicate order remains exact across batch boundaries.
Only direct SeqScan × SeqScan INNER HashJoin enters the specialization.
Metadata setup failures and unsupported children use the retained
materialized implementation, while runtime build/probe errors propagate
without replay. Heap, LSM, self-join read views, zero-width and repeated
projection, and legacy result/error equivalence are covered by executor and
core tests.

Non-target medians again show machine/code-layout variance:

| control | pre | post | change |
| --- | ---: | ---: | ---: |
| Phase 64 Top-N target K=1 | 514,083 | 875,500 | +70.3% |
| Phase 64 full Sort | 280,209 | 242,500 | -13.5% |
| full projected scan | 437,291 | 865,292 | +97.9% |
| generic Bool Filter | 441,417 | 477,375 | +8.1% |
| global SUM | 418,250 | 480,042 | +14.8% |
| low-cardinality GROUP BY | 973,917 | 553,083 | -43.2% |
| direct COUNT(*) | 365,542 | 418,334 | +14.4% |
| filtered COUNT(payload) | 427,125 | 740,875 | +73.5% |
| early LIMIT | 22,666 | 38,542 | +70.0% |
| LSM SUM | 95,959 | 82,541 | -14.0% |

Phase 65 completes row-batch consumption for the current direct-scan HashJoin
probe without changing planner policy. The next attribution work should first
measure the concrete IndexScan, RangeIndexScan, and PartitionedScan producer
coverage gap; typed column-oriented batches become the broader alternative if
primitive-heavy SeqScan, Filter, Aggregate, Top-N, and HashJoin probe workloads
show a shared row-layout or `ScalarValue` dispatch residual. Build-side HashJoin
ownership/hash work, full batch Sort, spilling, and AND/OR short-circuiting
remain separate candidates.

## Phase 66 partitioned SeqScan batch source

Phase 66 first added attribution without changing production. The benchmark
creates real range-partitioned tables through the public Database API, loads a
fixed 1,024-row quick fixture, and requires StatementInspection to report a
PartitionedScan whose every selected partition access is SeqScan. Full
projection covers 1, 2, 4, and 8 partitions. The four-partition fixture also
gates `LIMIT 20`, SUM, grouped COUNT, bounded Top-N, and Filter+Limit physical
trees and exact ordered results. The pre gate passed, so no hand-built plan was
used to justify implementation. Raw serial quick output is stored outside the
repository at `/tmp/netbadb-phase66-pre.txt` and
`/tmp/netbadb-phase66-post.txt`.

Machine-local medians are nanoseconds per query:

| scenario | partitions | source rows | access mix | pre | post | change |
| --- | ---: | ---: | --- | ---: | ---: | ---: |
| full projection | 1 | 1,024 | all SeqScan | 298,333 | 109,375 | -63.3% |
| full projection | 2 | 1,024 | all SeqScan | 389,833 | 155,417 | -60.1% |
| full projection | 4 | 1,024 | all SeqScan | 140,167 | 103,208 | -26.4% |
| full projection | 8 | 1,024 | all SeqScan | 284,042 | 134,042 | -52.8% |
| LIMIT 20 | 4 | 1,024 | all SeqScan | 151,625 | 6,583 | -95.7% |
| SUM | 4 | 1,024 | all SeqScan | 157,917 | 116,084 | -26.5% |
| grouped COUNT | 4 | 1,024 | all SeqScan | 168,625 | 136,333 | -19.2% |
| Top-N K=20 | 4 | 1,024 | all SeqScan | 272,333 | 134,167 | -50.7% |
| Filter+Limit 20 | 4 | 1,024 | all SeqScan | 172,834 | 37,875 | -78.1% |

This single quick pair supports no general throughput threshold. Full
projection still owns all final QueryResult rows and is primarily an order and
producer-overhead control. The authoritative result is structural: selected
partition storages are visited in planner order and feed one shared batch.
Test-only statistics for sizes 100/100/100/213 report 513 rows seen, three
deliveries, and a maximum batch of 256. The first full batch crosses partition
boundaries. A four-by-100-row `LIMIT 20` visits one partition, requests 20
rows, delivers one 20-row batch, and skips the other three partitions. SUM and
Top-N over 0/1/255/257 partitions both visit all four partitions, see 513 rows,
deliver three batches, and report a 256-row maximum. Empty total input,
individual empty partitions, 1/2/4/8 partition counts, 255/256/257/512/513
boundaries, Heap/LSM combinations, and exact materialized-legacy order are
covered.

The source intermediate for eligible LIMIT and collectors is bounded by 256
rows; Aggregate adds its group/state ownership and Top-N adds at most K
candidates. A query returning all N rows still owns `O(N)` final output, so
Phase 66 does not claim total `O(256)` memory for full projection.

Point/range attribution deliberately remains outside the new source. A unique
point IndexScan returned one row. The duplicate secondary-key control selected
SeqScan at 250 of 1,000 rows rather than materializing a large point result.
The exact bounded range sweep selected RangeIndexScan for 1 and 20 rows, then
selected SeqScan for 255, 256, and 257 rows; the 257-row SUM likewise used
SeqScan. No timed Phase 66 partition fixture produced mixed access, while the
correctness suite retains partition-local index planning and executor tests
prove SeqScan+IndexScan and SeqScan+RangeIndexScan plans reject batch
eligibility and execute through the materialized fallback. These measurements
do not show planned IndexScan/RangeIndexScan cardinalities commonly exceeding
256, so executor-side Vec chunking is not justified.

Phase 67 therefore evaluated typed column-oriented aggregate attribution before
adding storage-level point/range visitors. If a future workload shows frequent
RangeIndexScan results above 256
or large duplicate point candidates, the follow-up should add a real
storage-level range or point visitor first and only then expand mixed
PartitionedScan. Phase 66 changes no TableStorage API, storage engine,
PhysicalPlan, planner cost, public contract, dependency, unsafe code, or
persistent format. The Phase 65 HashJoin probe control changed from 735,291 to
721,458 ns (-1.9%); its direct SeqScan source remains unchanged.

## Phase 67 typed primitive aggregate column-batch attribution

Phase 67 added attribution before changing production. Every target uses a real
planned query, verifies its physical shape and aggregate source columns, and
checks the exact result or a deterministic checksum. The pilot was restricted
to global aggregates over Bool, Int64, and UInt64. It deduplicated referenced
source positions, transposed each existing row batch into typed vectors with
explicit validity bits, and used direct typed COUNT/SUM/MIN/MAX loops. Text,
grouped aggregation, non-primitive types, and ineligible trees remained on the
row accumulator. Raw serial quick output is stored outside the repository at
`/tmp/netbadb-phase67-pre.txt` and `/tmp/netbadb-phase67-post.txt`.

Machine-local medians are nanoseconds per query:

| scenario | unique columns | consumers | pre | post | change |
| --- | ---: | ---: | ---: | ---: | ---: |
| global SUM(Int64) | 1 | 1 | 333,833 | 432,292 | +29.5% |
| same-column SUM/MIN/MAX | 1 | 3 | 581,542 | 509,208 | -12.4% |
| duplicate SUM x3 | 1 | 3 | 339,166 | 927,542 | +173.5% |
| three distinct primitive columns | 3 | 3 | 565,167 | 463,333 | -18.0% |
| Filter plus same-column SUM/MIN/MAX | 1 | 3 | 377,042 | 459,417 | +21.9% |
| nullable primitive aggregates | 1 | 3 | 357,417 | 503,125 | +40.8% |
| partitioned same-column aggregates | 1 | 3 | 155,791 | 128,708 | -17.4% |
| LSM same-column aggregates | 1 | 3 | 82,042 | 85,667 | +4.4% |
| Text MIN/MAX control | N/A | 2 | 450,625 | 637,291 | +41.4% |
| grouped mixed-aggregate control | N/A | 4 | 532,750 | 546,042 | +2.5% |

The intended same-column reuse ratio improved from 1.742x the single-SUM time
to 1.178x. However, the distinct-three-column ratio moved from 0.972x the
same-column case to 0.910x rather than producing a coherent reuse curve, while
duplicate SUM moved from 1.016x single SUM to 2.146x. These contradictory
relationships are not credible evidence for keeping the representation.
Non-target movement was also broad: for example, the ordinary batch full scan
moved from 352,542 to 520,083 ns (+47.5%). This is a noisy quick attribution
pair, not a throughput threshold or statistical performance claim.

The pilot's structural tests were positive: a 513-row input delivered 256,
256, and 1 rows; SUM/MIN/MAX over one position allocated one unique typed
column, while three source positions allocated three; validity bits preserved
NULL exclusion; checked SUM and final SQL conversion preserved existing
overflow errors; and row/column results agreed for empty, nullable, boundary,
Heap, LSM, filtered, and partitioned cases. Memory during the experiment was
the existing at-most-256-row owned batch plus at most one transient typed vector
per unique primitive source and aggregate state. It was not columnar storage
and did not remove the row batch or final QueryResult ownership.

The decision is **reject**. The production sidecar and its test-only statistics
were removed; only the benchmark attribution and this decision record remain.
`ExecutionBatch` and `AggregateAccumulator` are unchanged. Phase 68 therefore
selected the narrower HashJoin build-key ownership residual; AND/OR
short-circuiting, full batch Sort, and spilling remained separate candidates.
Any future typed-column trial must isolate row-to-column transposition cost and
show consistent gains across Heap, LSM, Filter, nullable, and partitioned
workloads before generalization.

## Phase 68 borrowed HashJoin build keys

Phase 68 first added five Text equality-join scenarios without changing
production. Each fixture uses analyzed benchmark-only `id Int64, join_key Text`
tables and requires a real planner-produced direct SeqScan × SeqScan HashJoin
whose left key belongs to the probe table and right key belongs to the fixed
build table. Scan-column provenance and exact ordered results are gated. The
64-row probe/4,096-row build no-match targets minimize output ownership; the
matching target emits only 64 rows, and a 4-by-64 duplicate case checks exact
right-minor order. Raw quick output is stored outside the repository at
`/tmp/netbadb-phase68-pre.txt` and `/tmp/netbadb-phase68-post.txt`.

Machine-local medians are nanoseconds per query:

| scenario | build rows | distinct | key shape | pre | post | change |
| --- | ---: | ---: | --- | ---: | ---: | ---: |
| Text unique, no match | 4,096 | 4,096 | 8-byte Text | 1,100,792 | 926,375 | -15.8% |
| Text unique, no match | 4,096 | 4,096 | 128-byte Text | 1,826,708 | 1,785,125 | -2.3% |
| Text duplicate, no match | 4,096 | 64 | 128-byte Text | 1,437,333 | 1,520,417 | +5.8% |
| Text unique, 64 matches | 4,096 | 4,096 | 128-byte Text | 2,073,417 | 1,312,375 | -36.7% |
| Text duplicate ordered matches | 64 | 4 | 8-byte Text | 60,333 | 25,708 | -57.4% |
| Int64 unique control, no match | 4,096 | 4,096 | Int64 | 712,500 | 700,041 | -1.7% |

The long/short unique Text ratio moved from 1.659x to 1.927x, so this pair does
not show the expected width-ratio contraction. Short Text/Int64 moved from
1.545x to 1.323x, which is consistent with removing Text-only ownership.
Duplicate-long/unique-long moved from 0.787x to 0.852x. These mixed ratios and
only three timed samples per join scenario prohibit a general throughput claim.

The authoritative result is structural. Both production HashJoin paths now use
one `HashMap<&ScalarValue, Vec<usize>>` whose keys borrow the immutable right
rows. A 513-row unique Text test reports 513 materialized build rows, 513
non-NULL keys, 513 distinct keys, 513 bucket indices, and zero owned key clones.
A four-key duplicate fixture reports the same row/non-NULL/index counts, four
distinct keys, and zero clones. Distinct String allocations with equal contents
share a bucket by ScalarValue value Hash/Eq, and the stored key pointer equals
the first authoritative right-row String. NULL remains absent and indices stay
in right input order.

Before Phase 68, build memory contained fully owned right rows, map-owned cloned
ScalarValue keys, and ordered index vectors. It now contains the same right rows
and vectors plus borrowed key references; unique Text removes one additional
String allocation per logical map insertion, while the old `entry(key.clone())`
also no longer constructs and drops candidate clones for duplicate rows. The
right build remains fully materialized `O(right)`, the left probe remains
at-most-256-row batched, final QueryResult rows remain owned, and expected build
and probe complexity remains linear. Hashing, RandomState, residual evaluation,
build-side choice, and output projection are unchanged.

Non-target movement was broad: the Phase 65 large-probe/small-build no-match
control changed -19.7%, the small-probe/large-build Int64 control -1.7%, global
SUM -32.0%, generic Filter -61.0%, direct COUNT(*) -64.6%, partition LIMIT
-21.3%, and partition SUM -21.1%, while Top-N K=1 and full Sort were nearly
flat. The duplicate Text regression is therefore recorded rather than treated
as a stable common-path regression.

The decision is **KEEP**: zero build-key ownership and Text pointer identity are
exact, the shared implementation is small, correctness covers Bool/Int64/
UInt64/Text, NULL, duplicates, ordering, Heap/LSM, self joins, errors, and both
probe paths, and the Text/primitive relative signal does not contradict the
ownership hypothesis. Phase 69 should separately audit AND/OR short-circuit
error timing before benchmarking it, or measure full Sort/spill and dynamic
HashJoin build-side materialization/choice. This result does not justify String
interning, dictionary encoding, custom hashing, or reopening column batches.

## Phase 69 safe Filter AND/OR short-circuit

Phase 69 added attribution before production changes. Each order pair is
logically equivalent, scans the same source-order columns, checks the exact
result, and requires either `Aggregate>Filter>SeqScan` or
`Project>Filter>SeqScan` without index access. Text pairs use two repeated
comparisons that are true for the AND fixture or false for the OR fixture; a
selective `id` comparison moves from first to last. Primitive, borrowed
streaming, and filtered COUNT pairs provide separate controls. Raw serial quick
output is stored outside the repository at `/tmp/netbadb-phase69-pre.txt` and
`/tmp/netbadb-phase69-post.txt`.

Machine-local medians are nanoseconds per query:

| scenario | pre | post | cheap/expensive pre | cheap/expensive post |
| --- | ---: | ---: | ---: | ---: |
| Aggregate AND Text, cheap first | 485,458 | 474,041 | 1.715x | 1.159x |
| Aggregate AND Text, expensive first | 283,083 | 409,083 | — | — |
| Aggregate OR Text, cheap first | 447,959 | 209,209 | 1.713x | 0.397x |
| Aggregate OR Text, expensive first | 261,542 | 526,334 | — | — |
| Projected streaming AND Text, cheap first | 390,333 | 459,458 | 0.805x | 1.017x |
| Projected streaming AND Text, expensive first | 484,625 | 451,916 | — | — |
| Filtered COUNT AND Text, cheap first | 180,333 | 283,417 | 0.362x | 0.723x |
| Filtered COUNT AND Text, expensive first | 498,500 | 392,250 | — | — |
| Aggregate AND primitive, cheap first | 259,792 | 226,000 | 0.492x | 0.408x |
| Aggregate AND primitive, expensive first | 528,500 | 553,750 | — | — |

The Text-cheap/primitive-cheap ratio moved from 1.869x to 2.098x. That control
does not isolate a generic evaluator win, and the Aggregate Text AND pair
remained inverse despite moving materially toward parity. The projected
streaming pair was neutral post. In contrast, OR, filtered COUNT, and primitive
pairs had the expected cheap-first direction. Broad non-target movement in the
same runs was substantial, so these quick measurements are attribution signals,
not stable throughput estimates or performance thresholds.

The authoritative evidence is structural and semantic. A metadata-validated
predicate reports zero right-column accesses for FALSE AND and TRUE OR, and one
for TRUE AND, FALSE OR, UNKNOWN AND, and UNKNOWN OR. Nested decisive AND and OR
skip both later branches even under NOT or IS NULL. All 18 SQL three-valued
AND/OR combinations equal the existing eager evaluator. UNKNOWN is never a
decisive left value.

Eligibility is conservative. `bind_expression` still runs first. Complete
column identity/type/nullability, literal metadata, Bool logical/NOT/IS NULL
metadata, and comparison compatibility are validated privately by the
executor. Every predicate source column is then checked against the attached
storage schema before evaluation. Binding failures keep the dynamic
row-dependent fallback, including empty malformed scans. Binding success with
unsafe or schema-unproven metadata keeps eager bound evaluation; explicit
FALSE-AND and TRUE-OR malformed-right tests, including a plan and predicate that
consistently mislabel an `Int64` storage column as Bool, still return the old
`ExpectedBoolean` error. Batch, borrowed and projected streaming, legacy
materialized Filter, and filtered COUNT share the new predicate wrapper.
Dynamic evaluation, DML, NestedLoopJoin, HashJoin residual predicates, and the
general bound scalar evaluator remain eager.

The decision is **KEEP**. The skipped-work invariant is exact, the expression
and storage-schema gates preserve malformed/error behavior, all executor tests
and benchmark gates pass, and the order pairs do not systematically contradict
the hypothesis even though the noisy Aggregate Text AND result prevents a
general speedup claim.
Phase 70 should prioritize measured dynamic HashJoin build-side selection, a
full Sort/spill boundary, or a storage-level range visitor rather than reopening
the rejected column-batch pilot.

## Phase 70 statistics-guided smaller-side HashJoin build

Phase 70 added attribution before production changes. The existing asymmetric
Int64 and Text fixtures now require exact last-ANALYZE row counts as well as a
planner-produced direct SeqScan × SeqScan INNER HashJoin, exact logical key and
scan-column provenance, no index access, and exact ordered results. Separate
64×512 and 512×64 Int64 no-match controls make the side asymmetry observable at
a smaller scale. Raw quick output is stored outside the repository at
`/tmp/netbadb-phase70-pre.txt` and `/tmp/netbadb-phase70-post.txt`.

Machine-local medians are nanoseconds per query:

| scenario | logical rows | selected build post | pre | post | change |
| --- | ---: | --- | ---: | ---: | ---: |
| Int64 no match, small left | 64×4,096 | left/64 | 815,917 | 646,375 | -20.8% |
| Int64 no match, small right | 4,096×64 | right/64 | 639,458 | 759,875 | +18.8% |
| Int64 no match, small left | 64×512 | left/64 | 314,042 | 222,000 | -29.3% |
| Int64 no match, small right | 512×64 | right/64 | 212,209 | 194,417 | -8.4% |
| Text unique short, no match | 64×4,096 | left/64 | 1,065,833 | 657,292 | -38.3% |
| Text unique long, no match | 64×4,096 | left/64 | 2,059,917 | 1,205,208 | -41.5% |
| Text duplicate long, no match | 64×4,096 | left/64 | 1,354,917 | 1,446,167 | +6.7% |
| Text unique long, matching | 64×4,096 | left/64 | 2,081,208 | 1,193,791 | -42.6% |
| Text duplicate ordered matches | 4×64 | left/4 | 68,334 | 40,041 | -41.4% |

The 64×4,096/4,096×64 Int64 ratio moved from 1.276x to 0.851x; the
64×512/512×64 ratio moved from 1.480x to 1.142x. The unchanged large-left,
small-right production branch moved in opposite directions at the two scales,
and duplicate Text no-match regressed while the other targeted Text cases
improved materially. With only three samples per Join target, these quick
results are evidence rather than a stable throughput guarantee or a reason to
add a ratio threshold.

The authoritative evidence is structural. Eligible execution reads both
sides' last ANALYZE `TableStatistics::row_count` and builds left only for a
strictly smaller left estimate. Tests report BuildLeft with estimates
64/4,096, 64 materialized build rows, 4,096 streamed rows in 16 batches of at
most 256, and zero owned build-key clones. The inverse 4,096/64 case reports
BuildRight with 64 build rows and 4,096 streamed rows; 513/513 ties and either
missing statistic preserve BuildRight.

BuildLeft uses the same `HashMap<&ScalarValue, Vec<usize>>`, ScalarValue value
Hash/Eq, and standard-library RandomState as BuildRight. It appends complete
owned matches to one vector per logical left row while streaming right, then
move-flattens those vectors in left order. Exact legacy left-major/right-minor
results hold across batch boundaries, Bool/Int64/UInt64/Text keys, NULL,
duplicates, eager residuals, reordered/repeated/zero-width projections, self
joins, Heap/LSM, and stale-statistics fixtures. No output is replayed after a
runtime error, and the fixed-right materialized fallback is unchanged.

Before Phase 70, eligible streaming HashJoin input memory was
`O(right build + left batch + output)`. It is now
`O(selected build + streamed batch + output)`, approximately
`O(min(left, right) + batch + output)` when ANALYZE correctly ranks the sides.
BuildLeft adds `O(left build rows)` outer output-bucket metadata, which stays
within the selected build scale; final owned QueryResult memory remains
unbounded. The choice follows the last ANALYZE snapshot rather than an exact
runtime count, so stale statistics can choose the actually larger side but
cannot affect correctness.

The decision is **KEEP**. The implementation remains executor-private, reduces
the targeted build from 4,096 owned rows to 64, preserves exact semantics, and
does not change PhysicalPlan, planning, inspection, hashing, storage, public
APIs, dependencies, or persistent contracts. Phase 71 should move to full
Sort/spill attribution, a storage-level range visitor only with new workload
evidence, or larger algorithmic work; HashJoin micro-tuning closes here.

## Phase 71 full Sort memory and spill boundary attribution

Phase 71 changes no production Sort algorithm. It adds real SQL through the
real compiler/planner and requires exact no-LIMIT plans, exact base scan
ColumnIds, and complete ordered results. The matrix covers unique Int64,
duplicate primitive keys with stable ties, unique multi-key order, filtered
input, all four explicit NULL direction/placement combinations, fixed 8-byte
Text, fixed 128-byte retained/hidden Text, a four-way all-SeqScan
PartitionedScan, Heap, and LSM. Each important ordered query has a no-ORDER-BY
control with the same final row count and width. Phase 64's LIMIT 20 path is a
separate Top-N control, not a Full Sort implementation comparison.

Three serial quick runs are stored at
`/tmp/netbadb-phase71-run1.txt`, `/tmp/netbadb-phase71-run2.txt`, and
`/tmp/netbadb-phase71-run3.txt`. The first two used the documented `cargo bench`
command. Before the third, unrelated concurrent workspace edits introduced a
new uncached dependency while the configured local proxy was unavailable, so
Cargo resolution failed before execution; the valid third sample directly ran
the same release benchmark binary produced by the first two runs. Values below
are the median of the three per-run medians in nanoseconds per query.

| scenario | N | Sort input columns | output columns | median | ordered / no-Sort |
| --- | ---: | ---: | ---: | ---: | ---: |
| Int64 unique DESC | 4,096 | 1 | 1 | 754,167 | 0.985x |
| primitive duplicate team | 4,096 | 2 | 1 | 973,542 | 1.271x |
| primitive team ASC, ID DESC | 4,096 | 2 | 1 | 1,300,500 | 1.698x |
| short Text retained | 4,096 | 2 | 2 | 637,333 | 0.985x |
| long Text retained | 4,096 | 2 | 2 | 964,125 | 0.924x |
| long Text hidden | 4,096 | 2 | 1 | 985,416 | 1.290x |
| filtered primitive multi-key | 4,096 source | 2 | 1 | 798,083 | 1.245x |
| partitioned primitive multi-key | 1,024 / 4 partitions | 2 | 1 | 282,959 | 2.273x |
| LSM primitive multi-key | 250 | 2 | 1 | 137,916 | 2.517x |

Ratios below 1 do not mean sorting has negative cost. The three runs moved
widely, especially at small N, and result materialization, scan width, cache
state, and machine noise dominate some pairs. They are evidence against making
a throughput claim or selecting spill from latency alone. The hidden long-Text
ratio was the most consistent width-sensitive target: 1.261x at 512, 1.214x at
2,048, and 1.290x at 4,096. At 4,096, hidden long Text was 1.307x narrow unique
Int64 and only 1.022x retained long Text.

Primitive growth medians are:

| scenario | T(512) | T(1,024) | T(4,096) | T(4,096) / T(1,024) |
| --- | ---: | ---: | ---: | ---: |
| Int64 unique DESC | 176,042 | 406,666 | 754,167 | 1.854x |
| primitive duplicate | 231,625 | 445,292 | 973,542 | 2.187x |
| primitive multi-key | 305,209 | 645,583 | 1,300,500 | 2.014x |
| filtered multi-key | 188,958 | 364,792 | 798,083 | 2.188x |

This is a growth-shape observation, not a complexity proof. Increasing N from
1,024 to 4,096 did not produce a disproportionate jump in these fixtures. Text
uses its requested 512/2,048/4,096 sweep:

| Text shape | T(512) | T(2,048) | T(4,096) | T(4,096) / T(2,048) |
| --- | ---: | ---: | ---: | ---: |
| 8-byte retained | 198,375 | 326,625 | 637,333 | 1.952x |
| 128-byte retained | 289,708 | 777,000 | 964,125 | 1.241x |
| 128-byte hidden | 297,625 | 720,166 | 985,416 | 1.368x |

The structural result is more reliable than the timings. Test-only
`FullSortStats` shares the private function used by production Sort and reports:

| 513-row shape | rows before | rows after | Sort keys | input scalar slots | logical owned Text payload bytes | final scalar slots |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| narrow Int64 | 513 | 513 | 1 | 513 | 0 | 513 |
| hidden 128-byte Text | 513 | 513 | 1 | 1,026 | 65,664 | 513 |

The hidden projection retains no Text. “Logical owned Text payload bytes” is
the sum of String lengths, not exact heap allocation or RSS. The 513-row result
also proves Full Sort crosses both the 256-row and 512-row batch boundaries
without bounded retention. Existing exact-order coverage protects empty input,
255/256/257 and 512/513 boundaries, Text, NULL placement, multi-key order,
stable duplicate ties, Heap, LSM, and PartitionedScan behavior without adding
release counters.

Partitioned source batching is already bounded by Phase 66, yet Full Sort still
materializes all 1,024 rows; its 282,959 ns median versus the 124,500 ns
one-column no-Sort control confirms Sort is a remaining blocking operator. The
small LSM pair similarly measures 137,916 versus 54,792 ns, but both controls
also differ in scanned key width and neither establishes a memory failure.
Phase 64 remains effective for small K: the existing 1,000-row K=1 median was
168,000 ns versus 224,958 ns for the duplicate Full Sort control, while Top-N
retains only K candidates plus one batch.

Current Full Sort owns `O(N input rows)`, then its sorted rows flow into the
fully owned `QueryResult`. Returning N rows therefore has an Ω(N final-output)
memory lower bound. External runs could reduce additional sorting scratch,
bound pre-output work, or eliminate some wide hidden-key residency, but cannot
change total query memory to `O(run_size)` under this contract. A future merge
would also need a global input ordinal or equivalent stable-tie mechanism and
must validate all runtime keys before exposing output, matching current error
timing.

The decision is **DEFER spill**. The hidden-wide shape shows a real 2-to-1
Sort-input/final-output slot difference and about 29% latency overhead, but not
enough evidence to justify a memory budget, temporary run format, codec,
writer/reader, k-way merge, cleanup policy, and failure lifecycle. Phase 72
should select another independently measured large feature. If a future real
workload isolates hidden wide keys, compare a compact indirect key/ordinal
representation before assuming disk spill is the right boundary.

## Phase 72 costed index nested-loop join

Phase 72 first added real analyzed Heap SQL attribution while production still
selected direct SeqScan × SeqScan HashJoin. The 8×4,096 unique match case had a
604,542 ns quick median; the disjoint 64×4,096 case had a 1,018,209 ns median.
Both scanned the complete right table, exposed exact logical key provenance,
and returned exact ordered results. The saved run is
`/tmp/netbadb-phase72-pre.txt`.

The production planner now considers one deliberately narrow third candidate:
INNER direct Scan × Scan, distinct non-partitioned tables, the first compatible
necessary equality under AND, both table statistics, and an analyzed ordered
point access path on logical right. It reuses the Filter planner's non-NULL
average-match estimate and exact point startup/match cost. Since HashJoin and
IndexNestedLoopJoin both read logical left, checked `u128` compares
`left_rows × point_cost` with the right table's `managed_page_count`, the same
SeqScan cost used by Filter access-path planning; the index candidate must also
be strictly cheaper than NestedLoopJoin. Ties retain the old choice. Missing or
stale statistics, no right index, only a left index, high estimated duplicates,
self joins, and partitioned inputs never trigger executor fallback.

Execution validates the complete physical setup even for empty outer input,
then reuses the direct SeqScan batch source with at most 256 owned outer rows.
One non-NULL outer row produces one unchanged storage-neutral point lookup;
that result is consumed and dropped before the next. The full eager predicate
and final owned projection remain authoritative. Test-only structural evidence
is:

| shape | outer rows | batches | probes | point rows | max point rows | candidate pairs | output |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| unique + residual 64×4,096 | 64 | 1 | 64 | 64 | 1 | 64 | 58 |
| disjoint 64×4,096 | 64 | 1 | 64 | 0 | 0 | 0 | 0 |
| duplicate 2×3 | 2 | 1 | 2 | 6 | 3 | 6 | 6 |
| 64 outer with 8 NULL keys | 64 | 1 | 56 | 0 | 0 | 0 | 0 |

The boundary matrix covers 0/1/255/256/257/512/513 outer rows and never exceeds
256 rows per batch. The duplicate result is value-for-value and order-for-order
equal to the existing HashJoin reference (`L0-R0,R1,R2` before
`L1-R0,R1,R2`). Heap and LSM both use
`point_lookup_columns_with_view`; LSM requires no executor branch. Malformed
table, access path, key, type, or predicate metadata returns a typed error and
never replays as HashJoin.

The benchmark additionally gates unique outer sizes 1/8/64/256/512/1,024,
disjoint, high-duplicate, residual, fixed-width Text, no-index, and
left-index-only controls. The initial post run is saved at
`/tmp/netbadb-phase72-post.txt`; the cost-review correction is saved at
`/tmp/netbadb-phase72-cost-fix.txt`. Inspection text exposes the real left child
and explicit right point access rather than a fake right SeqScan. Statement
JSON advances conditionally to v5; ordinary v3 and partition v4 documents
remain unchanged.

The initial serial quick post run exposed a bad crossover:

| scenario | outer × inner | average matches estimate | pre/reference plan | post plan | pre/reference median | post median | change |
| --- | ---: | ---: | --- | --- | ---: | ---: | ---: |
| unique match | 8×4,096 | 1 | HashJoin | IndexNestedLoopJoin | 604,542 | 167,875 | -72.2% |
| disjoint | 64×4,096 | 1 | HashJoin | IndexNestedLoopJoin | 1,018,209 | 384,667 | -62.2% |
| unique match | 64×4,096 | 1 | same-run no-index HashJoin control | IndexNestedLoopJoin | 534,834 | 553,375 | +3.5% |
| duplicate disjoint | 64×4,096 | 64 | HashJoin | HashJoin | — | 641,583 | retained |
| residual no-output | 8×4,096 | 1 | HashJoin before operator | IndexNestedLoopJoin | — | 77,291 | selected |
| fixed Text/8 match | 64×4,096 | 1 | HashJoin before operator | IndexNestedLoopJoin | — | 1,246,041 | selected |
| large outer unique | 1,024×4,096 | 1 | HashJoin | HashJoin | — | 901,875 | retained |
| no right index | 64×4,096 | 1 | HashJoin | HashJoin | — | 534,834 | retained |
| left index only | 64×4,096 | 1 | HashJoin | HashJoin | — | 522,958 | retained |

Only the first two rows are actual before/after samples from the same indexed
fixtures; the 64-row no-index entry is labeled as a control rather than
misrepresented as a pre sample. Unique indexed medians for outer sizes
1/8/64/256/512 were 36,292/167,875/553,375/2,175,375/5,988,458 ns. At 1,024,
the row-count comparison crossed to HashJoin and fell to 901,875 ns. Selecting
the 256/512 point plans despite that evidence was not retained as a deferred
calibration issue.

The review correction compares point work with the already established
managed-page SeqScan cost instead of right row count. Its serial quick run was:

| scenario | outer × inner | corrected plan | corrected median (ns) |
| --- | ---: | --- | ---: |
| unique match | 1×4,096 | IndexNestedLoopJoin | 25,792 |
| unique match | 8×4,096 | IndexNestedLoopJoin | 66,709 |
| unique match | 64×4,096 | HashJoin | 1,129,458 |
| unique match | 256×4,096 | HashJoin | 1,403,625 |
| unique match | 512×4,096 | HashJoin | 1,110,500 |
| unique match | 1,024×4,096 | HashJoin | 1,440,750 |
| disjoint | 64×4,096 | HashJoin | 1,104,375 |
| duplicate disjoint | 64×4,096 | HashJoin | 1,698,916 |
| residual no-output | 8×4,096 | IndexNestedLoopJoin | 144,375 |
| fixed Text/8 match | 64×4,096 | HashJoin | 1,203,416 |
| no right index | 64×4,096 | HashJoin | 651,709 |
| left index only | 64×4,096 | HashJoin | 573,583 |

Machine-local runs remain noisy, so cross-run values are not treated as a
general throughput claim. The directly reported regression case nevertheless
moves from the initial 5,988,458 ns 512-row IndexJoin to a 1,110,500 ns
HashJoin, while 1/8-row and residual/8 targets retain the structurally bounded
point plan. No outer-row threshold was added: actual table layout feeds the
existing managed-page statistic, and duplicate estimates still raise point
cost.

The decision remains **KEEP** for the narrow algorithm after correcting the
cost units. Exact memory/order/error invariants hold, the smallest outer cases
remove the full right input, and 64/256/512, high-duplicate, Text/64,
no-right-index, and left-index-only controls now conservatively retain
HashJoin. Future repeated crossover work may refine engine-provided cost hints;
it is no longer required to tolerate the measured 512-row regression.

## IndexJoin cost-unit audit and Heap calibration (Phase 73)

Phase 73 audited the units before changing any access-method constant. The
current direct-join decision has two separate comparisons:

- NestedLoop and Hash CPU work remain row units: `left_rows × right_rows` and
  `left_rows + right_rows`, respectively;
- IndexJoin inner access work is `left_rows × point_cost`, where the shared
  Filter/Join point cost is either
  `base + expected_io + estimated_matches × sequential_unit` or the retained
  fallback `1 + tree_height + estimated_matches`;
- IndexJoin access work is compared with the right engine's
  `managed_page_count`, the same neutral sequential-work unit used by Filter
  IndexScan versus SeqScan. Heap SeqScan currently visits every managed page
  in its colocated file and validates/skips B+Tree and catalog pages, so those
  pages correctly remain part of Heap sequential work.

The benchmark now records direct Heap point hit/miss probes at 1/8/32/64/256,
a same-fixture projected scan, paired unique and no-match outer sweeps from 1
through 1,024, duplicate estimates 1/4/16/64, inner sizes
512/1,024/4,096/16,384 at outer 8/32/64, fixed-width Text, Filter/range controls,
and an LSM 8×512 IndexJoin control. Each paired SQL fixture loads data once,
measures and warms the no-join-key-index state, creates/analyzes the right
join-key index, then independently warms and measures the indexed state.

The earlier `/tmp/netbadb-phase73-pre-run1.txt`, `run2.txt`, and `run3.txt`
files exercised a deliberately tested pilot in which Heap sequential cost
counted only Heap data pages. The pilot was **rejected and restored**: actual
Heap SeqScan still traverses colocated access pages, and indexed HashJoin was
consistently slower than no-index HashJoin. Those pilot timings are not used in
the results below.

Three serial full quick runs of the final restored Phase 73 implementation are
preserved at
`/tmp/netbadb-phase73-final-run1.txt`, `final-run2.txt`, and `final-run3.txt`.
All three commands completed successfully and each report emitted all 93
Phase 73 scenarios. The following values are therefore medians of restored
Phase 73 per-run medians rather than measurements from the reverted pilot.

The median of the three per-run medians for the core points was:

| workload | outer | indexed median (ns) | no-index median (ns) | indexed plan | evidence |
| --- | ---: | ---: | ---: | --- | --- |
| unique | 1 | 29,791 | 628,000 | IndexJoin | reference is NestedLoop, not Hash |
| unique | 8 | 140,417 | 578,292 | IndexJoin | Index faster in all 3 runs |
| unique | 16 | 265,791 | 570,750 | IndexJoin | Index faster in all 3 runs |
| unique | 32 | 967,792 | 597,792 | HashJoin | both states use Hash; no Index reference |
| unique | 64 | 1,130,458 | 627,542 | HashJoin | both states use Hash; no Index reference |
| unique | 128 | 1,016,708 | 626,958 | HashJoin | both states use Hash; no Index reference |
| unique | 256 | 1,057,250 | 630,708 | HashJoin | both states use Hash; no Index reference |
| unique | 512 | 1,336,959 | 815,584 | HashJoin | both states use Hash; no Index reference |
| unique | 1,024 | 1,500,250 | 1,084,292 | HashJoin | both states use Hash; no Index reference |
| no match | 8 | 100,541 | 586,959 | IndexJoin | Index faster in all 3 runs |
| no match | 16 | 189,542 | 580,042 | IndexJoin | Index faster in all 3 runs |
| no match | 32 | 578,208 | 510,833 | HashJoin | both states use Hash; no Index reference |
| no match | 64 | 1,076,792 | 607,250 | HashJoin | both states use Hash; no Index reference |

The largest measured SQL outer with a valid, consistently faster Index
reference is 16. There is no measured smallest Hash winner: from outer 32 the
planner selects Hash in both fixture states, and Phase 73 intentionally adds no
FORCE JOIN mechanism. Direct Heap attribution provides only a proxy: median
per-probe hit cost was 8,328 ns at 32 probes and 8,409 ns at 64; miss cost was
6,378/6,380 ns, versus a 487,834 ns projected full scan. That places the direct
hit crossover between 32 and 64 probes and the miss crossover between 64 and
256 probes, but excludes SQL executor work and is not promoted to an optimizer
threshold.

Duplicate estimates naturally raise the shared point cost: at outer 8,
matches 1 and 4 select IndexJoin (three-run medians 141,083 and 194,208 ns),
while matches 16 and 64 select HashJoin. The restored final model also varies
with inner layout: all three final runs select Hash/Index/Index/Index for outer
8 at inner 512/1,024/4,096/16,384, and Hash/Hash/Hash/Index for outer 32. Text/8
keeps IndexJoin at outer 8 and HashJoin at outer 64 in every final run. LSM keeps
its dynamic hints and selects IndexJoin for the 8×512 control in every final
run.

The static Heap-hint calibration decision is **REJECT**. Heap hints were
`None` before and remain `None`; the generic
`1 + tree_height + estimated_matches` fallback remains. LSM hints were dynamic
before and remain unchanged. A larger Heap point base would move the planner
earlier even though direct evidence suggests the current 16→32 switch is
already conservative; a smaller base cannot honestly represent the measured
fixed probe work. Hit/miss also cannot be separated from join statistics, and
the paired no-index Hash latency is not a pure indexed-layout Hash reference.
No magic outer threshold, executor change, runtime replanning, or
nanosecond-derived planner constant was added.

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
it does not invalidate these performance-path results. Phase 63 changes only
executor-private predicate setup/evaluation and benchmark coverage. Phase 64
adds an executor-private Top-N consumer of the existing physical tree. Phase 65
adds an executor-private HashJoin probe consumer and retains the materialized
fallback. Phase 66 adds the second executor-private BatchSource variant for the
existing all-SeqScan PartitionedScan and retains mixed-access materialization.
Phase 67 retains only benchmark attribution after rejecting its private pilot.
Phase 68 changes only executor-private HashJoin key ownership and benchmark
coverage. Phase 69 adds only executor-private Filter predicate validation and
evaluation plus benchmark coverage. Phase 70 adds only executor-private
HashJoin build-side selection and benchmark coverage. Phase 71 adds benchmark
coverage and test-only Full Sort statistics while leaving release execution
unchanged. Phase 72 adds the explicit physical/inspection operator and advances
only affected statement Inspection JSON to v5. It retains Protocol v1, SDK
Schema Spec v1, deployment manifest v4, Heap metadata v4, Page v5, WAL and row
formats, the storage API, dependencies, and safe Rust boundaries.
