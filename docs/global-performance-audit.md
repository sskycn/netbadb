# Global performance attribution and optimization

Baseline: `2c1a8bc2e8a2c7a6aa99f6397f6fbd99b75a85de`; investigation 2026-09-18–19.
This engineering investigation follows the [error/concurrency](error-concurrency-audit.md),
[crash-consistency](crash-consistency-audit.md), and
[resource-boundedness](resource-boundedness-audit.md) audits. Their invariants are
regression contracts. No performance result permits weaker validation, durability,
mutation ownership, result ordering, or resource admission.

## Executive summary

The investigation measured 12 suites and 1,142 items per revision, with three
independent original and final runs. Five hotspots were shortlisted; three were
investigated through four production variants. **Two storage optimizations are
KEEP, both transport variants are REJECT, and two broader hotspots are DEFER.**
Only Page reconstruction and insertion preflight change production behavior.

The combined final 10K-row Heap loads measured 16.8–20.4% lower latency across 8/128/1,024-byte
Text, with disjoint original/final three-run ranges. These are end-to-end fixture
loads including their commits, not an executor-wide or pure-commit speedup.
The original and final matrices satisfy the same result and reported plan/shape
checks.
The original timing blocks and all follow-up control measurements are reported
separately; no slow case was removed to improve an aggregate score. Short
post-load read controls showed regressions that did not persist after longer
query warmup; one unchanged LSM control remains a small borderline timing
observation. These limits are included in the KEEP decision below.

## Measurement protocol

Hardware: Apple M4, 10 logical CPUs, 16 GiB RAM; macOS 26.6.2 (25G83), aarch64.
The pinned Rust 1.97.1 toolchain uses LLVM 22.1.6. Benchmarks use Cargo's optimized
bench profile with `CARGO_PROFILE_BENCH_DEBUG=1` for symbolized separate profiling.
Desktop applications remained running; this is a development-machine experiment,
not an isolated production-capacity claim. No unrelated application was stopped.

The clean baseline passed formatting, workspace checking, Clippy with warnings
denied, and the entire workspace test suite: 1,887 passed, zero failed, three
ignored. Production source remained unchanged during baseline matrix collection.
Each important matrix comparison uses three independent, serial process runs.
Builds, tests, and profiling do not run concurrently with measurement. Preserved
executables have SHA-256 identities, allowing comparisons without restoring old
source over current work.

New read scenarios use three warmups and eleven timed samples. Exact rows and
physical operators are checked outside query timers; feedback counters are also
collected outside timers. Compiler and boundary microbenchmarks use 100 samples.
Historical benchmarks retain their documented sample counts. Tables report the
median of the three run statistics, their minimum/maximum, and spread. Historical
commit/group/Columnar targets report means within a run; these are explicitly
distinguished from within-run medians. In particular, the historical commit
target's `mean_commit_ns` includes insertion and transaction processing, not only
the final commit call. The new write matrix times execution and commit separately.
The boundary target's P50 selects the upper middle sorted observation (100 samples
per client, 400 pooled observations for four clients); P95 uses nearest rank.
It does not report a P99 from this small sample. Different within-run statistics
are compared only against the same target and definition.

Reads are warm-process/warm-OS-cache measurements, not cold-disk measurements.
The default Heap buffer pool has eight frames: warm OS cache does not imply that
every database page remains resident in that pool. Reopen timings use new handles
over warm OS cache, with history and checkpointed states reported separately.
Setup, fixture loading, ANALYZE, and plan inspection are outside read timers;
fixture loading has its own measurement. Write batches grow a fresh database
through five successive batches, rather than resetting it between samples.

Native and PostgreSQL end-to-end results use real loopback TCP and one/four
clients. Connection startup and warmup are outside timers. The small benchmark
clients verify returned values within the end-to-end timer. They are neither a
Rust SDK versus pgx comparison nor a server-exclusive CPU measurement. Native
validation/encode/decode and PostgreSQL conversion/frame encoding have separate
microbenchmarks; subtracting them from end-to-end latency would not establish
exclusive layer percentages. PostgreSQL wire compatibility uses the repository's
specified `/opt/local/lib/pgsql` installation and ICU path during validation.

Raw logs, environment metadata, temporary databases, preserved binaries, and
profiler output belong under `/private/tmp/netbadb-global-performance-audit`, not
in Git. The committed runner records exit status, elapsed time, executable hash,
and requested row scale; the summarizer rejects missing/duplicate scenarios or
changed plans/work counters across runs.

## Workload coverage and boundaries

The matrix extends `phase7_baseline` and reuses the existing global commit,
group commit, and Columnar targets. It covers Heap/LSM access paths; filter,
projection, aggregate, Sort/Top-N and joins; partition counts 1/2/4/8; 0/1/4/8
indexes; one-row and 1,000-row write batches; single/multiple-storage and grouped
transactions; compiler/plan/open; embedded, Native and PostgreSQL boundaries.
Text widths are 8/128/1,024 bytes, with 1K/10K row sweeps and a 100K narrow LSM
scale case. A supplementary values suite covers 0/10/50/100% NULL over 1K
rows, Bytes widths 8/128/1,024, Float64 sorting, 255/256/257-row boundaries,
and fresh/analyzed/stale/refreshed statistics. A separate server sweep has 50%
NULL Text, using the same widths and one/four clients. These additions receive
their own three-run original-production baseline before optimization.
The 100K Heap setup is deliberately omitted: its first-fit insertion
traversal already grows strongly at smaller sizes. This is an explicit coverage
limit, not a claim of a measured 100K Heap result. The 100K wide-result and larger
long-running database cases remain outside this local matrix.

QueryResult remains fully owned. Output scalar slots and Text payload bytes are
reported separately from intermediate structures and process peak memory. RSS
and `time -l` peak footprint are whole-process observations, not allocation counts
or per-query ownership. Query feedback records examined/output rows, index probes
and filter work; test-only Page counters separately attribute validation and
payload copying. LSM inspection records files, levels, Bloom checks, data-block
reads and maintenance bytes; these are not all operating-system I/O calls.

| Suite | Measurement items per run | Main boundaries |
| --- | ---: | --- |
| reads | 540 | Heap/LSM, 1K/10K and 100K narrow LSM; three widths, projection/filter/aggregate/sort/access paths, DML, compiler, reopen |
| values | 272 | NULL rates, Bytes/Float64, 255/256/257 rows, fresh/analyzed/stale/refreshed statistics |
| pages | 16 | 1/16/128/256 slots, reject/replace/delete/reuse; 100 samples after three warmups, clone excluded |
| joins | 22 | Existing NestedLoop/Hash/IndexJoin cases and prior decision controls |
| writes | 40 | Heap 0/1/4/8 indexes, batch 1/1K, execution/commit and update/delete |
| partitions | 14 | 1/2/4/8 partitions, pruning, join and cross-storage movement |
| lifecycle | 4 | Explicit open/recovery lifecycle controls |
| boundary | 90 | Embedded, codec, Native/PG loopback; one/four clients, widths 8/128/1,024 |
| boundary_null | 90 | Same boundaries with 50% NULL Text and explicit result order |
| commit | 13 | Existing single/multiple-storage commit pipeline |
| group | 36 | Existing group-size, conflict and durability cases |
| columnar | 5 | Existing scan, point, projection and stale-authoritative-fallback cases |
| **Total** | **1,142** | Three independent runs, without wall-clock pass/fail thresholds |

The Page component uses compact opaque record payloads to exercise slot counts;
256 slots is not a claim that 256 SQL/MVCC row envelopes fit in one 4 KiB page.

Fixture loads have one observation per process (three independent process
observations in total). New write batches have five successive samples, while
read query medians have eleven. Summaries must not interpret these as identical
sample populations. Final exact-result, plan and structural comparisons precede
any timing conclusion.

## Execution and ownership map

| Stage | Owner and main work | Attribution boundary |
| --- | --- | --- |
| Parse, resolve, type check | Compiler creates typed HIR and relational IR | `prepare_statement`; no execution or storage mutation |
| Optimize and plan | Typed access paths, statistics, order and join choice | Plan inspection includes compilation; not obtained by subtracting noisy timers |
| Scan and decode | Storage validates authoritative pages/rows; executor consumes selected owned values or borrowed predicates | Physical plan, row/probe feedback, Page test counters and LSM block counters |
| Filter and projection | Prebound/borrowed paths avoid rejected-row ownership; unique projected values move | Exact NULL/error semantics and duplicate output ownership remain |
| Aggregate and joins | Group state or hash build, bounded probe batches, exact result order | Input cardinality, groups, candidate/match counts, final output |
| Sort and Top-N | Full Sort retains input; Top-N retains at most min(N,K) but validates all input | Hidden keys, ties and NULL order tested independently |
| QueryResult | Core/executor returns all owned output rows | Output payload lower bound is distinct from redundant copies |
| DML and index maintenance | One mutation owner per StorageId; Heap page reconstruction or LSM pending state; index updates and WAL | Execution timing, index count, WAL bytes and structural work |
| Commit and recovery | Durable decisions, ordered visibility, participant barriers and replay | Sync counters, transaction/group size, close/reopen history |
| Native response | Worker creates typed batch; connection encodes and writes frames | Validation, encode/decode, real TCP latency and throughput |
| PostgreSQL response | Worker converts typed result to wire cells; connection writes protocol messages | Conversion, framing, real TCP, one/four-client tail latency |
| Client boundary | Frame reads/decodes, output collection and verification | Included in end-to-end benchmark, not assigned to server CPU |

## Results and decisions

The original complete global pass contained **764 measurement items**. The
candidate list was selected from that pass before behavioral production changes.
The NULL/Bytes/Float64/statistics supplement adds 272, the nullable boundary adds
90, and the Page component adds 16: **1,142 measurement items**, each with three
independent original-production runs. The baseline values below are medians of
the three run statistics; they motivate experiments, not before/after claims.

| Rank | Candidate | Observed cost and source evidence | Scope / risk | Target and controls |
| --- | --- | --- | --- | --- |
| 1 | Repeated full-page validation during Heap reconstruction | 10K loads: 65.31 s at 8-byte Text, 62.18 s at 128 bytes; a full 256-slot page rejection takes 3.022 ms. `rebuild_slots` validates once, then `read_record` validates again for every live record; each header validates all slot ranges. | Reuse the existing immutable validated view; retain every authoritative check. Small storage-private change. | Heap load and DML; wide rows/tombstones; unchanged LSM reads and indexed lookups. |
| 2 | Owned reconstruction before rejecting a full page | First-fit insertion revisits earlier managed pages. The original rejected-page path allocates/copies every live payload before comparing required/available space. | Fully validate before a metadata-only fit decision; preserve exact error priority, generation rules, and bytes on failure. | Growing Heap batches and multiple widths; successful reuse/updates; LSM and read controls. |
| 3 | Small transport writes with TCP_NODELAY | 1K narrow result: embedded 0.158 ms, Native 2.217 ms, PG 4.492 ms; four PG clients 23.254 ms median. Native issues one write per frame, PG three per message, followed by a response flush. | A fixed-capacity response buffer can reduce socket writes; explicit flush and no retry from Drop are correctness requirements. | Native/PG result sizes and 1/4 clients; scalar/point latency, codec microbenchmarks and embedded controls. |
| 4 | Heap first-fit traversal and small buffer-pool pressure | Increasing rows/indexes expands page visits; 1K-row batches with eight indexes cost 18.00 s. This includes necessary WAL/dirty-page I/O. | A free-space directory or buffer-policy change needs explicit invalidation/corruption authority and broader I/O evidence. No arbitrary cache-size tuning. | Growth/index-count sweep provides evidence; separate architecture follow-up. |
| 5 | Owned range/QueryResult and recovery history | 100K narrow LSM identity output owns 600K scalar slots plus 0.8 MB Text; duplicated Text owns 1.6 MB. Heap 10K wide reopen was 250.47 ms before checkpoint versus 0.228 ms after. | Public streaming results and recovery changes cross ownership/durability boundaries. Existing output/history costs are not automatically defects. | Output accounting, range/scan plans, warm-OS-cache history/checkpoint controls. |

## Global attribution table

Costs below are absolute baseline observations; overlapping end-to-end timers
are not added together or converted into exclusive CPU percentages.

| Area | Workload / baseline cost | Attributed work and decision |
| --- | --- | --- |
| Compiler / planner | 100K LSM fixture: prepare 2.979 µs; compile plus plan inspection 3.958 µs | Typed stages are already separate. No parser/planner changes or magic thresholds. |
| Scan / final ownership | 100K narrow LSM identity 28.489 ms; 600K output scalars, 0.8 MB Text | Storage decode plus owned result remains; streaming ownership is **DEFER**. |
| Aggregate / Sort / Top-N | Same 100K fixture: unique groups 40.191 ms, multi-key Sort 55.729 ms, Top-N 28.913 ms | Group/input/retained-output costs measured; no new reason to reopen closed executor designs. |
| Join | 8×1,024 matching Heap control: no-index Hash reference 347.375 µs, indexed choice 250.666 µs | Exact plan/match/order gates; layouts differ, so this is not a pure indexed-layout Hash comparison. Prior calibration remains closed. |
| Partition | Four-partition full join 16.511 ms; cross-StorageId movement 43.579 ms | Existing execution and atomic movement retained; no concurrency change. |
| Heap page work | 256-slot replacement 3.043 ms, rejected insert 3.022 ms | Repeated full validation and rejected owned reconstruction: two **KEEP** changes. |
| Heap growth / I/O | 10K narrow load 65.308 s; 1K-row eight-index execution 17.999 s | First-fit traversal, index maintenance and dirty-frame WAL barriers remain; free-space/buffer policy **DEFER**. |
| Durability / grouping | 1K one-row Heap transactions: group 1 averages 19.173 ms/tx, group 32 averages 10.659 ms/tx | Historical whole-transaction means; barriers and ordering preserved. No sync reduction attributed to this patch. |
| Recovery | Heap 10K wide history reopen 250.470 ms; checkpointed 0.228 ms | Retained history dominates this warm-cache case; recovery redesign **DEFER**. |
| Native boundary | 1K narrow rows: embedded 0.158 ms, Native one client 2.217 ms | Separate validation/codec timings and socket stacks implicate transport work; both buffering pilots withdrawn. |
| PostgreSQL boundary | Same result: one client 4.492 ms, four clients 23.254 ms P50 | Conversion/framing/TCP/client work included. Large-result gains fail small-request controls: **REJECT**. |
| Columnar | Scan 0.311 ms versus same target's Heap scan 0.567 ms | Existing projection and stale authoritative fallback remain controls; no Columnar change. |

## Final combined results

All 36 final matrix processes exited successfully. The 1,142 case identities
and reported workload/result/plan shapes match all three original runs. New SQL
scenarios check exact result rows on every timed iteration; reused targets retain
their existing result, plan or storage-statistics gates.
All 436 read feedback records, 268 values feedback records, 260 explicit Bytes
ownership records and eight write WAL/durability records match exactly across
all six original/final logs.
Timing ranges below are the minimum and maximum of three process observations;
spread is `(max − min) / median`, not a confidence interval.

| Workload | Before median [min, max] | After median [min, max] | Change | Spread before / after |
| --- | ---: | ---: | ---: | ---: |
| Heap load, 10K ×8 B | 65.308 [64.722, 77.273] s | 53.033 [38.542, 55.077] s | -18.80% | 19.22% / 31.18% |
| Heap load, 10K ×128 B | 62.176 [60.922, 113.489] s | 51.748 [39.878, 54.590] s | -16.77% | 84.55% / 28.43% |
| Heap load, 10K ×1,024 B | 79.272 [78.662, 117.061] s | 63.122 [59.681, 69.641] s | -20.37% | 48.44% / 15.78% |
| 1K-row execution, no index | 5.800 [5.523, 5.916] s | 4.667 [4.324, 4.814] s | -19.53% | 6.77% / 10.49% |
| 1K-row execution, eight indexes | 17.999 [17.781, 18.009] s | 16.518 [15.800, 17.140] s | -8.23% | 1.27% / 8.11% |

These five target ranges are disjoint. The smaller eight-index improvement is
consistent with remaining index/WAL work. No percentage improvement is assigned
to an individual storage patch from these combined end-to-end observations.
The unchanged LSM transaction controls also moved, so these are observed
end-to-end differences, not an exclusive CPU or I/O attribution of every saved
second to this patch. Structural and separate Page evidence establish the
removed work independently.

### Original-block non-target controls

| Workload | Before median [min, max] | After median [min, max] | Change | Spread before / after |
| --- | ---: | ---: | ---: | ---: |
| LSM 100K narrow identity | 28.489 [27.572, 29.370] ms | 27.838 [26.748, 28.593] ms | -2.28% | 6.31% / 6.63% |
| LSM 100K unique groups | 40.191 [39.563, 40.523] ms | 37.776 [37.131, 40.486] ms | -6.01% | 2.39% / 8.88% |
| LSM 100K multi-key Sort | 55.729 [54.727, 56.844] ms | 51.857 [51.144, 55.609] ms | -6.95% | 3.80% / 8.61% |
| LSM 100K Top-N | 28.913 [27.790, 29.175] ms | 26.335 [25.981, 28.563] ms | -8.92% | 4.79% / 9.80% |
| Heap 10K wide history reopen | 250.470 [239.471, 284.497] ms | 249.165 [226.527, 249.548] ms | -0.52% | 17.98% / 9.24% |
| Heap 10K wide checkpointed reopen | 228.167 [221.708, 247.333] µs | 228.167 [223.041, 247.708] µs | +0.00% | 11.23% / 10.81% |
| Single-row commit, no index | 7.715 [7.075, 7.872] ms | 7.020 [6.930, 7.941] ms | -9.01% | 10.33% / 14.41% |
| 1K-row commit, eight indexes | 7.931 [7.842, 8.895] ms | 7.735 [7.573, 7.871] ms | -2.48% | 13.27% / 3.85% |
| LSM IndexJoin 8×512 | 51.583 [40.000, 55.583] µs | 54.000 [21.333, 56.750] µs | +4.69% | 30.21% / 65.59% |
| PG scalar, 128 B, one client | 36.500 [35.166, 37.208] µs | 35.250 [32.708, 47.750] µs | -3.42% | 5.59% / 42.67% |
| PG 1K narrow result, four clients | 23.254 [23.183, 23.886] ms | 23.743 [23.700, 25.644] ms | +2.10% | 3.03% / 8.19% |
| LSM 1K-row transaction (mean) | 12.173 [12.170, 20.867] ms | 13.732 [12.719, 13.825] ms | +12.80% | 71.44% / 8.05% |
| Heap group 32, 1K tx (mean/tx) | 10.659 [9.120, 13.440] ms | 8.083 [8.047, 8.100] ms | -24.17% | 40.53% / 0.65% |
| LSM group 32, 1K tx (mean/tx) | 5.847 [4.850, 6.167] ms | 4.106 [4.098, 4.128] ms | -29.77% | 22.51% / 0.75% |

Most read/recovery/commit controls above have overlapping ranges. The unchanged
LSM group-commit path also improved substantially, demonstrating that storage
sync/environment variation affects whole-transaction measurements; those changes
are not claimed as a new batching or durability optimization. The LSM 1K-row
transaction is included despite its slower median.

### Follow-up controls and timing sensitivity

The second global pass was not declared regression-free from unchanged source.
Screening all 1,142 items found 35 cases with a baseline of at least 1 µs, a
median increase above 10%, and disjoint three-run ranges. This is an investigation
trigger, not a statistical significance test or CI threshold. All original
observations remain in the evidence.

Three alternating original/final pairs of the complete 244-item 1K read stratum
changed the flagged narrow Heap Text filter from the original-block
167.334 [151.417, 176.083] → 185.125 [184.000, 195.666] µs (+10.63%) to
197.250 [196.417, 202.750] → 199.084 [194.667, 200.125] µs (+0.93%). The suspected
regression did not reproduce in that paired test.

A further three alternating pairs retained every case in Page, values,
partition, boundary, nullable boundary, Columnar, Join and lifecycle suites.
The Page and both boundary suites had no >10% separated slower case above 1 µs.
For example, one-slot reuse was 1.292 [1.208, 2.250] →
1.208 [1.208, 2.250] µs. The 256-slot replace remained
2.771 [2.699, 2.800] ms → 0.047 [0.047, 0.061] ms; full rejection
2.746 [2.666, 2.758] ms → 0.011 [0.010, 0.014] ms.

The zero-NULL Heap filter's original +81.16% warning became
424.125 [421.958, 426.958] → 426.417 [423.583, 432.875] µs (+0.54%). Other small
Heap/partition cases remained slower and therefore received an additional
predeclared warmup-sensitivity diagnostic, reported below. They are not erased
by the controls that improved.

The diagnostic used isolated copies of the original commit and final production
Page, with identical benchmark-only edits on both: 1,000 actual query warmups,
then the unchanged timed sample count. Partition and Join use
`ProfileSettings.query_warmup = 1_000`; the new query helper uses 1,000 warmups
plus eleven timed queries; Columnar uses 1,000 warmups plus its existing five
mean samples. There is no spin loop, artificial delay, CPU-affinity change,
new production cache, or altered SQL/plan to force a result.

It covers all 14 partition, 22 Join and five Columnar cases, plus a declared
152-item values stratum: both engines, every 1K/128-byte NULL density,
255/256/257 rows at width 8, and fresh/analyzed/stale/refreshed statistics.
The medium/wide batch-boundary cases remain in the complete 272-item original
and paired values suites; they are not claimed as part of this sensitivity run.
Both diagnostic builds finished before any of its three alternating pairs.
The ordinary 1,142-item second pass retains its original warmup protocol.

All 24 diagnostic processes passed, with matching reported gates for all
193 items. The following table retains both measurement protocols. Entries are
three-run median [min, max] in µs; Columnar rows retain within-run means.

| Control | Ordinary paired warmup: before → after | 1,000-query warmup: before → after | Warm diagnostic change |
| --- | --- | --- | ---: |
| Four-partition scan | 118.291 [117.729, 125.937] → 303.666 [275.333, 324.896] | 90.958 [88.624, 92.208] → 90.479 [89.208, 94.562] | -0.53% |
| Partition LIMIT 20 | 7.833 [7.729, 8.437] → 16.708 [16.667, 19.125] | 5.750 [5.750, 6.250] → 6.292 [5.709, 6.375] | +9.43% |
| Partition SUM | 133.416 [132.312, 135.646] → 287.250 [272.521, 348.416] | 98.479 [98.083, 108.729] → 101.146 [98.542, 110.229] | +2.71% |
| Unique-key HashJoin | 137.917 [133.125, 138.333] → 297.041 [281.666, 456.167] | 106.083 [101.375, 112.750] → 107.833 [102.750, 114.708] | +1.65% |
| No-match HashJoin | 99.666 [99.583, 100.542] → 307.334 [305.792, 380.292] | 78.209 [77.041, 83.459] → 84.416 [77.500, 84.750] | +7.94% |
| Heap scan (Columnar target mean) | 836.000 [461.000, 879.000] → 1198.000 [1133.000, 1230.000] | 346.000 [345.000, 359.000] → 339.000 [338.000, 360.000] | -2.02% |
| Columnar scan (mean) | 394.000 [391.000, 407.000] → 426.000 [407.000, 434.000] | 164.000 [160.000, 166.000] → 168.000 [160.000, 170.000] | +2.44% |
| Heap stale-statistics range | 666.250 [664.417, 687.083] → 967.834 [922.042, 976.709] | 315.084 [313.125, 317.541] → 327.667 [326.166, 334.125] | +3.99% |
| Heap 255-row narrow Top-N | 53.416 [53.250, 60.750] → 81.583 [81.541, 92.541] | 39.791 [39.666, 40.667] → 38.667 [36.042, 41.458] | -2.82% |
| Heap all-NULL filter | 233.458 [231.709, 240.125] → 260.709 [260.708, 261.625] | 146.167 [142.667, 149.959] → 141.625 [137.917, 147.375] | -3.11% |
| LSM all-NULL Bytes filter | 239.916 [222.500, 244.167] → 237.250 [222.750, 240.709] | 140.875 [140.750, 155.041] → 155.292 [155.291, 159.458] | +10.23% |

The large post-load gaps did not persist under the longer read-only warmup.
This establishes sensitivity to the measurement protocol/run state, not an
exclusive attribution to CPU placement, frequency, cache or allocator state.
Those remain hypotheses. The short-warmup regressions are real observations
under that protocol and remain a limit for claims about immediate post-load
latency; the diagnostic does not retroactively replace them.

One of the 193 warm controls crossed the investigation trigger: unchanged LSM
all-NULL Bytes filtering was +10.23%, an absolute 14.417 µs. Its ordinary paired
comparison was −1.11% with overlapping ranges; the diagnostic ranges miss by
0.250 µs. This isolated borderline result is retained as measurement uncertainty,
without an LSM speedup claim or a new algorithmic attribution. No additional
reruns were selected to force every observation below a percentage cutoff.

**Final decision: KEEP both Page changes.** Deterministic removed work, repeated
Page timing gains and the ordinary end-to-end target measurements support the
small storage-private changes. The large sustained-read regressions did not
reproduce in the declared diagnostic; the original post-load timing sensitivity
and isolated LSM control remain explicit limitations. The decision is not a
claim that every query or startup latency improves, nor an SLO certification.

### Restored transport: latency, tails and throughput

The final boundary executable is SHA-256 identical to the pre-transport-pilot
step-2 executable (`15bff884687002822604bae980465324f9be5d1f9b5a55f58ab1ce24eb352932`).
The original and final matrix blocks nevertheless show substantial tail drift.
For the narrow 1K-row result with four PG clients, original-block P95 was
82.430 [25.162, 83.225] ms and final-block P95 107.742 [106.628, 130.175] ms;
throughput was 148.478 [143.628, 156.937] → 91.685 [70.372, 94.886] requests/s.
These unfavorable observations prompted the alternating whole-boundary check;
they are not hidden behind the similar P50.

The following are median [min, max] of the three alternating process runs.
End-to-end latency includes decoding and exact client result verification.

| Paired control | P50 before → after, ms | P95 before → after, ms | Requests/s before → after |
| --- | --- | --- | --- |
| Native narrow 1K, one client | 2.513 [2.485, 2.536] → 2.439 [2.331, 2.538] | 2.875 [2.874, 2.882] → 2.888 [2.659, 2.955] | 392.754 [392.440, 404.632] → 406.351 [384.673, 426.350] |
| Native narrow 1K, four clients | 6.275 [6.266, 6.298] → 6.261 [6.249, 6.480] | 8.075 [7.981, 8.127] → 8.050 [8.015, 8.087] | 621.689 [617.699, 621.872] → 620.913 [605.109, 625.436] |
| PG narrow 1K, one client | 4.452 [4.442, 4.466] → 4.455 [4.398, 4.459] | 4.596 [4.590, 4.611] → 4.594 [4.542, 4.639] | 224.827 [224.551, 225.052] → 224.742 [224.451, 227.093] |
| PG narrow 1K, four clients | 23.654 [23.537, 23.977] → 23.849 [23.686, 23.865] | 105.806 [100.090, 107.188] → 109.052 [101.342, 114.097] | 95.601 [85.977, 102.194] → 88.965 [87.218, 89.376] |

Four-client PG throughput remains poor and variable; the paired median is 6.94%
lower with overlapping ranges, and both revisions now exhibit approximately
100 ms tails. This supports a time-dependent environment effect, without
identifying an exclusive scheduling cause or claiming production capacity.
The paired width-128 single-client PG scalar P95 was
43.167 [41.791, 45.667] → 44.167 [43.291, 44.375] µs;
no buffered small-response latency penalty ships in the final code.


## Isolated experiment 1: reuse the validated reconstruction view

**KEEP.** The existing `ValidatedPage` borrow proves that the checksum, complete
slot directory and every record range were validated before any payload copy.
The change reuses that view instead of re-entering `read_record` for every live
slot. Test-only counters show exactly one full validator invocation per rebuild,
unchanged copied payload bytes, and zero copies on malformed-page rejection.
All 24 Page tests and 243 Heap tests passed after this isolated change.
These validation counts describe `rebuild_slots`, not a whole successful
mutation: insertion/deletion/replacement still perform their entry check and
the existing validation in `rebuild_records` on the success path.

Three serial focused runs covered 16 page operations, every one of the 244 1K-row
read-suite items, and all 272 value/statistics items. Page reconstruction at 256
slots fell from 3.043 ms to 55.291 µs (replace); full rejection fell from 3.022 ms
to 33.000 µs. This first comparison also showed a small-input warning: one-slot
replace was 1.458 → 1.958 µs. The unmodified LSM values controls moved broadly
(the median per-case change was +27.09%, versus +24.83% for Heap), so small
end-to-end differences were not assigned to the patch.

An additional three-pair alternating old/new **whole page suite** resolved the
small-input concern without changing fixtures or dropping any case. One-slot
replace was 1.584 [1.584, 1.625] → 1.416 [1.416, 1.417] µs; one-slot delete
1.583 [1.583, 1.584] → 1.375 [1.375, 1.395] µs. The 256-slot replace remained
3.042 [3.039, 3.044] ms → 53.292 [52.583, 53.459] µs. The initial noisy runs
remain in the evidence; the paired check establishes that the suspected
one-slot regression was not reproduced. These are local page CPU results, not
claims of a 98% database or commit speedup.

| KEEP optimization | Paired workload | Original median [min, max], µs | Step 1 median [min, max], µs | Change | Structural evidence |
| --- | --- | ---: | ---: | ---: | --- |
| Reuse validated reconstruction | Replace, 256 slots | 3,041.667 [3,039.229, 3,044.063] | 53.292 [52.583, 53.459] | −98.25% | Owned-slot collection validates 1 + L →1 times; the same L payloads are copied. |
| Same isolated change | Full rejection, 256 slots | 3,018.208 [3,017.271, 3,019.520] | 28.979 [28.958, 29.000] | −99.04% | Rejected insertion validates L + 2 →2 times; payload copies are still present at this step. |


## Isolated experiment 2: reject full pages before payload reconstruction

**KEEP for measured local CPU/copy reduction; no isolated end-to-end load win
claimed.** After the same complete authoritative validation and RecordTooLarge
check, insertion searches slot metadata for the lowest reusable generation and
compares the same required/available byte counts. Only successful-fit attempts
construct owned payloads. A full-page rejection now performs one validator call
and zero payload copies, instead of two validations and `L` copies after step 1
(or `L + 2` validations and `L` copies originally). The validator's bounded
range-vector allocation remains: this is not an allocation-free claim.

All 25 Page tests passed, including checksum/late-slot corruption before space
or record-size errors, exact PageFull fields, unchanged bytes on failure,
zero-length records, lowest-tombstone reuse and exhausted generations. Three
independent runs again covered 16 page, 244 read and 272 values items.

| Operation | Step 1 median [min, max], µs | Step 2 median [min, max], µs | Change |
| --- | --- | --- | --- |
| Full reject, 1 slot | 0.708 [0.625, 1.208] | 0.291 [0.208, 0.333] | −58.90% |
| Full reject, 16 slots | 1.417 [1.333, 2.917] | 0.375 [0.292, 0.417] | −73.54% |
| Full reject, 128 slots | 13.125 [12.125, 27.500] | 4.334 [3.333, 4.667] | −66.98% |
| Full reject, 256 slots | 33.000 [29.042, 43.625] | 12.875 [11.833, 13.334] | −60.98% |
| Replace, 256 slots (adjacent) | 55.291 [51.520, 67.084] | 56.479 [53.083, 57.917] | +2.15%; overlapping |
| Successful reuse, 256 slots | 51.625 [51.583, 57.416] | 53.041 [52.792, 53.125] | +2.74%; overlapping |

The full 1K read matrix's median per-case change was +3.06% for Heap and +2.66%
for the unchanged LSM control. Values controls moved in the other direction
(Heap −1.00%, LSM −19.15%). These summaries are noise diagnostics, not a combined
workload speedup. Heap 1K loads at widths 8/128/1,024 changed +7.97%/+0.44%/+13.05%
with overlapping three-run ranges. Required full-sync I/O, first-fit traversal,
and record ownership outside rejected reconstruction remain. The decision rests
on the independently timed page operation and exact removed copies, not on
attributing those noisy database latencies to the patch.

## Rejected Experiments

**REJECT: unconditional response buffering (step 3).** The pilot applied an
8 KiB `BufWriter` to every Native/PG response, retained explicit response-end
flushes and discarded pending bytes on ordinary errors with `into_parts`.
Correctness tests passed and long responses improved substantially, but this
was insufficient: single-client PostgreSQL scalar and point controls regressed.
Three initial independent normal/NULL boundary runs were followed by three
alternating old/new whole-boundary pairs, retaining all 90 cases per revision.
The paired results were:

| Case | Direct median [min, max], µs | Unconditional buffer median [min, max], µs | Change |
| --- | --- | --- | --- |
| PG scalar, width 128, one client | 36.292 [35.000, 37.667] | 66.250 [63.583, 66.542] | +82.55% |
| PG point, width 128, one client | 57.666 [56.375, 57.959] | 97.208 [80.750, 98.750] | +68.57% |
| Native scalar, width 128, one client | 59.625 [55.750, 61.208] | 59.667 [47.041, 63.667] | +0.07% |
| Embedded 1K result, width 128 | 259.833 [259.042, 264.459] | 264.375 [262.125, 271.166] | +1.75% |
| PG 1K result, width 8, four clients | 24,088.000 [24,018.291, 24,354.417] | 2,069.750 [2,035.500, 2,122.209] | −91.41% |

The small-response regression reproduced even while nearby controls remained
close. Its exclusive CPU/packet-scheduling cause was not established; no claim
that buffering itself consumes 30 µs of CPU is made.

**REJECT: preserving direct writes only for small batches (step 4).** The second
pilot retained original writes for at most four messages (normal single-row
Native replies use three, PG four) and buffered longer batches. Two new tests
called the actual transport wrappers and confirmed original write counts through
four messages and coalescing from five. All 351 boundary tests and server Clippy
passed. Nevertheless, three normal and three NULL network runs still failed the
latency objective: normal PG width-128 scalar was 36.417 [35.250, 45.083] →
88.792 [87.250, 90.500] µs (+143.82%), and point was 58.500 [58.000, 60.541] →
92.417 [72.208, 97.667] µs (+57.98%). The NULL scalar/point controls also regressed.
The normal four-client 1K narrow PG result still improved 18.121 → 2.232 ms, but
this did not justify the small-response regression.

Both transport variants were removed, including their helper, cutoff, modified
callers and pilot-specific tests. Final Native/PG production files are byte-for-
byte the original baseline; the boundary benchmark is retained to expose the
tradeoff in future work. The final matrix measures the restored production
transport, so pilot throughput gains are **not** final product claims. No
packet scheduling, CPU affinity, busy-wait, extra worker or client-contract
change was introduced to make the benchmark win.

The withdrawn helper's deterministic narrow-row test showed 1,000 Native row
frames: 1,000 → 7 writer calls with the same 50,000 bytes; PG: 3,000 → 4 calls
with the same 25,890 bytes. These counts are calls to the test writer, not kernel
syscall counts. The buffer was capped at 8 KiB per active response; encoder,
partial-write and explicit-flush errors propagated without a Drop retry on
ordinary Result paths. This remains useful attribution for a future measured
transport design, but no such buffer or additional memory cost ships here.

Invalid measurement attempts also remain documented, but are not counted as
rejected production optimizations:

- An early read smoke assumed a wide LSM SeqScan; the actual valid plan was a
  RangeIndexScan. The plan gate was corrected before baseline collection.
- A boundary smoke used request ID zero for Hello; it was corrected to one.
- The first values supplement lacked ANALYZE and selected NestedLoopJoin, so
  its HashJoin attribution was rejected; explicit ANALYZE now precedes timing.
- The next values run assumed insertion order for nullable Heap records.
  Small NULL rows can fit earlier pages. The independent expected-order oracle
  now uses insertion RowIds sorted by page/slot; the NULL network result asks
  for explicit `ORDER BY id`. Production ordering was not changed.
- The first Page timing process completed its workload but `/usr/bin/time -l`
  failed its sandboxed `kern.clockrate` read. That exit-1 run was excluded and
  replaced by three successful runs.
- Instruments Allocations could not attach (details below); its invalid trace
  is not allocation evidence. The noisy one-slot step-1 warning was resolved
  by alternating complete-suite measurements, and both sets were retained.

The historical rejected typed aggregate sidecar and IndexJoin calibration were
not reimplemented or counted as new experiments in this audit.

## Separate profiling evidence

Separate profiles used the preserved original binaries and disposable fixtures;
their runs were intentionally stopped and are excluded from latency tables.
`sample` succeeded for Heap insertion and the real loopback boundary. In the
Heap main thread's 4,052 snapshots, 2,960 were inside the dirty-frame WAL
`sync_all` path, and a distinct 774-snapshot reconstruction branch passed through
`slot_state` to full page validation. These are sampled stacks, including blocked
time, not exclusive CPU percentages. The network sample included both Native and
PG writer paths ending in `__sendto`; its collapsed top-of-stack counts included
6,575 send and 8,400 receive snapshots across threads.

Instruments File Activity also succeeded. Its approximately 5.45-second steady
Heap insertion window recorded 41,476 reads (169,885,696 logical bytes), 2,026
writes (12,438,352 logical bytes), 43,502 seeks, and 999 `fcntl` operations with
command 51 (`F_FULLFSYNC`, verified against the local SDK header). Their recorded
fcntl duration totaled 3.658 seconds. There were no new opens in this attached
window; startup was outside it. Logical bytes are not physical SSD traffic.
This supports the separate first-fit/buffer-eviction opportunity without
authorizing deletion of a single required WAL barrier.

Instruments Allocations failed to attach: macOS returned authorization status
`-60006` and xctrace exited 2. Its trace is not valid allocation evidence. Exact
test-only copied-record/byte counts, logical output ownership and process peak
memory are reported instead; no total allocator-call count is invented.

A separate final Heap sample contained 3,485 main-thread snapshots, including
3,092 in the dirty-frame WAL `sync_all` path. The insertion/reconstruction branch
contained two snapshots in `rebuild_slots` in that window. This is
consistent with the deterministic validation/copy counters; it is not an
apples-to-apples exclusive CPU percentage or a promise that WAL waits shrank.
The remaining required I/O dominates this sampled phase.

The previously rejected typed aggregate sidecar, closed HashJoin tuning, and
IndexJoin calibration remain closed. The current matrix includes them as
controls; it does not supply a new reason to reopen their designs. Full Sort
spill remains the separate Phase 71 decision, including its exact hidden-key
ownership analysis and output lower bound.

## Memory and structural work

| Boundary | Original ownership/work | Retained ownership/work |
| --- | --- | --- |
| Full 128-slot Page rejection | 130 full validations; 128 owned payload copies totaling 3,044 bytes, plus reconstruction metadata | One full validation; zero copied records/bytes and no owned reconstruction collection |
| Owned-slot collection (`rebuild_slots`) with L live records | 1 + L full validations; L owned payloads | One full validation; the same L owned payloads and exact generations/bytes |
| 100K narrow LSM identity result | 100K rows, 600K scalar slots, 800,000 Text bytes | Same fully owned output |
| 100K duplicate Text projection | 100K rows, 200K scalar slots, 1,600,000 Text bytes | Same duplicate output ownership |
| 10K ×1,024-byte duplicate Text | 10K rows, 20K scalar slots, 20,480,000 Text bytes | Same output lower bound |
| Full Sort / unique groups / duplicate join output | Existing input-, group- or output-sized ownership | No retained executor representation or memory-policy change |
| Native / PostgreSQL response | Existing owned batch and per-frame codec storage | Original production transport restored; no additional response buffer |

Page counters are test-only and thread-local. Their copied bytes count payload
copies, not allocator capacity, metadata size or total allocator calls. Full
validation still constructs its bounded range vector. Process RSS/peak footprint
also includes fixture construction, storage caches and retained history; it is
not a query-allocation census. Across the three complete 100K-inclusive read processes:

| OS observation, bytes | Before median [min, max] | After median [min, max] |
| --- | ---: | ---: |
| Maximum RSS | 879,968,256 [744,882,176, 1,127,956,480] | 976,011,264 [753,057,792, 1,056,899,072] |
| Peak memory footprint | 660,833,672 [654,853,464, 680,641,904] | 574,276,832 [553,534,688, 615,056,728] |

RSS has a higher median with overlapping ranges, while footprint is lower.
These are different OS observations over fixture construction and the entire
matrix, not interchangeable totals, a per-query peak, an allocator census or
proof of a leak. The deterministic claim is the eliminated reconstruction
collection/copies on PageFull; no whole-process memory percentage is attributed
to it. The failed Allocations attach remains an explicit evidence limit.

For all-live nonempty records, the repeated pairwise overlap checks in one
rebuild fall from a local O(S³) term to O(S²). Full-page rejection removes owned
payload reconstruction and one additional full validation after step 1, while
retaining authoritative validation and its O(S²) worst case. Page size remains
4 KiB and slot count is bounded by that layout. Across a growing Heap, first-fit
page traversal still has quadratic growth; neither change turns table insertion
into O(N). No result rows, required wire bytes, WAL records, syncs or durable
state transitions are removed.

## Deferred opportunities and remaining performance risks

| Classification | Evidence / boundary | Decision |
| --- | --- | --- |
| Measured performance opportunity; candidate 4 | First-fit Heap insertion, eight-frame buffer pool, sampled WAL full-sync and large logical read volume; 0/1/4/8-index growth changes real write cost | **DEFER** free-space inventory/buffer-policy work until invalidation, authoritative corruption handling and measured I/O policy are designed together. No cache-size or sync-count shortcut. |
| Architecture project; candidate 5 | Fully owned QueryResult and range output, retained history and large difference between history/checkpointed reopen | **DEFER** public streaming/range ownership or recovery-history redesign. A borrowed storage visitor is not automatically a public streaming contract. |
| Measured transport tradeoff | Writer calls and long-result latency fell in both pilots, but single-client PG latency failed independent controls | Both pilots **REJECTED**; retain measurements for a future design with latency/throughput controls. Packet scheduling and allocation as exclusive causes remain unmeasured hypotheses. |
| Known execution architecture | One synchronous database owner/worker serializes requests; at the time of this audit the complete QueryResult was materialized before transport row admission | This historical finding is superseded only for output-row enforcement by the later [server execution/resource audit](server-execution-resource-audit.md); no new worker or same-StorageId concurrency was added. |
| Resource-control projects | Full Sort, high-cardinality groups, duplicate join output, retained MVCC/history and large final results scale with input/output | Query memory budgets, cancellation and temporary-disk quotas remain separate work. Existing admission and decode limits are unchanged. |
| Closed prior decisions / roadmap | Phase 67 sidecar rejection, Phase 70 HashJoin closure, Phase 71 Sort ownership/spill study, Phase 73 IndexJoin calibration rejection | Keep as measured controls. This audit supplies no new justification to reopen their cost models or add magic planner thresholds. |

The matrix is representative, not exhaustive: no physical cold-disk test, WAN
capacity claim, 100K Heap load, 100K wide result, allocator-wide trace, arbitrary
power-loss certification or all-size workload optimum is claimed.

## Correctness and compatibility review

The retained production diff is confined to `netbadb-storage/src/page.rs`.
`ValidatedPage` is borrowed from an immutably held Page; no trust proof survives a
mutation, leaves storage, or bypasses authoritative decoding. Insertion checks
the full page and its type before record size and fit, and selects exactly the
same lowest non-exhausted tombstone. Checked generation advancement, zero-length
records, deleted-slot distinctions and unchanged failure bytes remain covered.

| Contract | Review and regression boundary |
| --- | --- |
| SQL results, NULL and exact order | Compiler/planner/executor code unchanged; exact fixture results cover primitive/Text/Bytes/Float64, 0–100% NULL, duplicate projections, group order, joins, ties and NULL sorting. |
| Overflow and errors | No arithmetic check removed. Existing corruption, truncation, slot generation and overflow tests remain; four new Page tests cover work counts, later-slot corruption before copies, PageFull bytes/fields and corruption-before-size error priority. |
| Writer authority and concurrency | StorageId ownership, transaction handles, deterministic multi-domain ordering and the synchronous worker topology are unchanged. Test counters are thread-local and absent from release builds. |
| Recovery and durability | Page v5 bytes, checksums, slot layout, page LSNs, WAL records/barriers, publication order and retry decisions are unchanged. Original/final write WAL lengths and durability observations match; close/reopen and checkpoint controls remain. |
| Resource caps and cleanup | No admission limit, queue, decoder bound, session/portal lifetime or LSM pending-state accounting change. The rejected transport helper and both caller changes were removed. |
| Public compatibility | No dependency, unsafe block, public API, persistent-format, protocol, schema, manifest or inspection-version change. QueryResult remained owned; row policies were post-execution transport limits at this audit baseline. The later server execution/resource audit moves the row-count refusal into Core without changing persistent or wire formats. |

The final production diff was rereviewed against all nine error/concurrency
findings, all six crash-consistency findings and all eleven resource findings.
Coordinator append/cleanup poisoning, fatal failed rollback, listener joins,
redacted errors, locked journal identity, complete-record count admission,
WAL/status tail cleanup, terminal Abort recognition, whole-set recovery
validation, file/directory publication barriers, permit-bound controls,
SAVEPOINT/portal ownership, LSM replacement accounting/row representability,
and stable Change Stream coalescing retain their established authority.
The Page optimization neither removes these paths nor catches their errors.

## Validation and executed commands

The clean baseline ran these commands serially: `cargo fmt --all -- --check`
(2.90 s), `cargo check --workspace --all-targets` (15.46 s),
`cargo clippy --workspace --all-targets -- -D warnings` (12.25 s), and
`cargo test --workspace` (1,375.46 s; 1,887 passed, zero failed, three ignored).
The final run is listed separately below.

| Executed experiment command / family | Result |
| --- | --- |
| `cargo test -p netbadb-storage page::tests -- --nocapture` | Original-work counter baseline 24 passed; step 1 24 passed; step 2 25 passed. |
| `cargo test -p netbadb-storage heap::tests` | 243 passed after step 1, 556.11 s. |
| `cargo clippy -p netbadb-core --bench phase7_baseline -p netbadb-server --bench global_boundary -p netbadb-storage --lib --tests -- -D warnings` | Passed during storage/benchmark iteration. |
| `cargo test -p netbadb-server -p netbadb-client -p netbadb-protocol -p netbadb-pgwire` | Pilot 1: 349 passed; refined pilot: 351 passed. Withdrawn pilot-only tests are not included in the final test-count increase. |
| `cargo clippy -p netbadb-server --all-targets -- -D warnings` | Both transport pilots passed; performance controls still rejected them. |
| `cargo bench -p netbadb-core --bench columnar_phase1 --bench global_commit_pipeline_phase3b --bench global_group_commit_phase3c --bench phase7_baseline --no-run --message-format=json` | Final optimized executables built with `CARGO_PROFILE_BENCH_DEBUG=1`; all builds finished before timing. |
| `cargo bench -p netbadb-server --bench global_boundary --no-run --message-format=json` | Original, pilot and restored final executables built and preserved separately. |
| `python3 scripts/run-global-performance.py …/binaries/after …/after --build` | All 12 suites ×3 processes passed; 1,142 items/run. Full invocation/reproduction is in [performance.md](performance.md#global-performance-attribution-after-the-stability-audits). |
| Runner with `--suites pages,reads,values --rows 1000 --runs 3` | Both isolated storage steps passed all 532 items/run. Original page/values supplements had their own three-run baselines. |
| Runner with `--suites boundary,boundary_null --runs 3` | Three normal/NULL runs at step 2 and both pilots; result/plan gates passed, performance decisions above. |
| `/usr/bin/time -l <preserved executable>` with suite/row environment | Used by all serial/alternating matrix drivers; logs record run status, binary hash and complete case population. |
| `python3 scripts/summarize-global-performance.py <three suite logs>` and separate cross-revision comparator | All 12 final suite populations and shape gates passed; no missing case silently dropped. |
| `/usr/bin/sample <owned benchmark PID> 5 1 -file <sample.txt>` | Original Heap/boundary and final Heap separate profiles succeeded; intentionally stopped profile workloads excluded from timings. |
| `xcrun xctrace record --template 'File Activity' --attach <owned PID> --time-limit 5s --no-prompt --output <trace>` | Passed; exported syscall evidence reported above. |
| Same xctrace invocation with `--template Allocations` | Failed to attach, exit 2, macOS authorization −60006; no allocation result claimed. |
| `git diff --check`; Python AST parse of both added scripts | Passed during final review; no Python cache or raw profile/fixture artifact staged. |

| Command | Final result | Duration |
| --- | --- | ---: |
| `cargo fmt --all -- --check` | Passed | 1.56 s |
| `cargo check --workspace --all-targets` | Passed | 11.61 s |
| `cargo clippy --workspace --all-targets -- -D warnings` | Passed | 11.43 s |
| `cargo test --workspace` | Passed: 1,891 tests including doctests, 0 failed, 3 pre-existing ignored | 1156.72 s |
| `go test -race ./...` (from `sdk/go`) | Passed; Go reused valid test-cache entries | 0.46 s |
| `sh scripts/test-go-sdk.sh` | Passed | 6.47 s |
| `python3 scripts/test-postgresql-psql.py` | Passed | 2.60 s |
| `python3 scripts/test-resource-lifecycle.py` | Passed | 7.46 s |

The final Rust total is 1,888 unit/integration tests plus three executed
doctests. The increase from the clean baseline is exactly the four Page tests.
Existing manual/fuzz-corpus ignores remain unchanged; no test was weakened or
removed. The complete suite includes round-trip, malformed/truncated input,
close/reopen, crash/redo/undo, durability-failure and Native/PG/TLS boundaries.

PostgreSQL validation used `PSQL=/opt/local/lib/pgsql/bin/psql`,
`DYLD_LIBRARY_PATH=/opt/local/lib/icu/lib`, and
`NETBADB_PSQL_TARGET_DIR=<checkout>/target`. All commands ran serially after
performance collection, with diagnostic builds isolated in their own target
directory. Final source/manifests/script hashes match the frozen inputs.

The final lifecycle probe observed seven numeric file descriptors and three
server threads at ready, after 100 malformed connections and after 1,000;
shutdown exited 0. This is the measured fixture lifecycle, not a global
connection-independent memory or descriptor bound. Final whitespace, added
Markdown paths and source/manifest diff review passed. No raw benchmark output,
profile, temporary database or diagnostic build is part of the commit.
