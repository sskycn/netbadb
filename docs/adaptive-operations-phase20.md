# Adaptive Operations Phase 20

Phase 20 adds an observe-only Physical Design Advisor to `netbadb-core`. Real
workload feedback may reveal that a missing physical design is worth operator
consideration, but observation carries no authority to create, drop, alter,
build, apply, name, or reserve that design.

## Mission and ownership

The complete path is explicit and synchronous:

```text
ExecutionFeedbackReport
        -> caller records it
PhysicalDesignEvidenceWindow
        -> Database::advise_physical_design(&self, window, policy)
PhysicalDesignAdvisorReport
```

`PhysicalDesignEvidenceWindow` is caller-owned, runtime-only, bounded, and
non-persistent. It is not a `Database` field and is not part of
`AdaptiveEvidencePool`. In particular, physical-design history does not reuse
`calibration_query_shapes`: calibration retention belongs to a
`PlannerCalibrationEpoch`, while a design measurement cohort belongs to its
independent `PhysicalDesignEvidenceEpoch`. Ordinary `query` and
`execute_prepared` calls therefore retain zero Phase 20 telemetry overhead.
Only an explicit call to `record_execution_feedback` ingests an existing
formal report. The advisor never reparses SQL, executes a query, scans query
logs, synthesizes `EXPLAIN`, or invokes maintenance or scheduling.

The default `PhysicalDesignEvidenceLimits` bounds index candidates, Columnar
candidates, structural query shapes per candidate, and columns per Columnar
candidate. All public limits are `u64`; they are memory/cardinality limits,
not hidden physical-design policy. Recommendation thresholds are always an
explicit `PhysicalDesignAdvisorPolicy` supplied by the caller.

## Cohort chronology

Every admitted report must carry `ExecutionFeedbackAnchor.global_commit_seq =
Some(G)`. G is the deterministic caller ordering and cohort chronology, not a
wall clock or an expiration token. Equal or increasing values are accepted;
decreasing values return `OutOfOrderVisibility`. Explicit window rotation
clears aggregation while retaining this ordering high-water, so an older
report cannot re-enter merely because a cohort was rotated.

The first report anchors one schema generation. Same-generation reports
aggregate. A newer generation advances `PhysicalDesignEvidenceEpoch`, clears
the old design cohort, and records the newer report as the first sample with a
typed `SchemaRotated` outcome. Older schema evidence is rejected. Rotation
touches only the caller's design window; it does not rotate
`AdaptiveEvidencePool`, publish G, or modify the database.

Reports or access samples marked incomplete/overflowed do not contribute
positive evidence. Report-level failures are counted as discarded diagnostics.
Every report, work, row, and diversity counter uses checked arithmetic and
records overflow honestly. Supporting shapes use exact
`LogicalQueryShape` structural equality; hashes are not identity authority.
If candidate or shape capacity rejects evidence, the window becomes truncated
and advice returns `InconclusiveCapacity` with no positive recommendations.
Retained entries are the first entries that fit, not a defensible top-N.

## Privacy

The window retains stable `TableId`/`ColumnId` identities, exact typed
`LogicalQueryShape`, counters, and candidate column sets. The existing query
shape removes SQL text, aliases/display names, and ordinary literal payloads;
it retains only structural type/NULL/parameter meaning. Phase 20 stores no SQL
string, relation or column display name, `ScalarValue`, result row, session,
principal, network address, or request identifier.

## Index advisor v1

An index candidate is exactly `(TableId, ColumnId)`. It contains no `IndexId`,
name, access-path ID, tree root, file, page, or storage placement. V1 recognizes
only a direct `Filter` over a physical `SeqScan` and AND-composed direct
comparisons:

```text
column = literal/parameter                 point
column <|<=|>|>= literal/parameter         range
literal/parameter =|<|<=|>|>= column       corresponding point/range
```

The supported subset matches the production planner's direct comparison
forms. It excludes `NotEq`, OR, NOT, `IS NULL`, casts, column-to-column and
computed expressions, predicates above joins/aggregates/arbitrary projection
chains, join-derived opportunities, and sort-only opportunities. Reversed
range operands follow the production planner's reversal semantics. AND may
support several independent single-column candidates; it never invents a
composite, covering, included-column, partial, expression, hash, or join index.

Positive support additionally requires a real complete
`ExecutionAccessKind::SeqScan` in the same report. Existing IndexPoint/Range
work produces no missing-index evidence. A table appearing under multiple
bindings is skipped to avoid self-join attribution. Partitioned access is
skipped because one logical candidate cannot describe multiple partition-local
physical trees.

Advice rechecks the current physical inventory. Any current active access path
on the column with the required point/range capability suppresses stale
historical absence, including an LSM clustering path. Retired trees are absent
from this inventory. If no path covers the candidate, V1 recommends only the
layout supported by the production create-index surface: a single-storage Heap
table and an existing current column. LSM and range-partitioned layouts are
`UnsupportedCurrentLayout`. The capability check is read-only and reserves no
identity or schema writer.

## Columnar advisor v1

A Columnar candidate is `(TableId, exact column set)`. Its columns come from
the chosen physical `PlanVariant::SeqScan.columns`, not SQL projection text.
Empty sets are absent. At advice time the vector is canonicalized into the
current schema declaration order. Different exact sets remain different
candidates; Phase 20 performs no union, set cover, superset optimization,
clustering, or cross-candidate merging.

Only real complete sequential scans contribute. Multiple bindings of one
table are skipped. A candidate must be a strict subset of the current table,
because without a hypothetical Columnar cost model a full-row copy has only a
possible vectorization argument, not sufficient missing-design evidence. The
production single-storage build restriction is retained.

A current registered projection whose columns are a superset suppresses the
candidate, regardless of whether that projection is Fresh, Lagging, Stale, or
Suppressed: such states are maintenance/eligibility issues, not missing
designs. If a managed projection or its catalog is unavailable and coverage
cannot be proved, advice conservatively returns
`CurrentProjectionUnavailable` for that table instead of suggesting a
duplicate. A recommendation contains no `ColumnarProjectionId`, directory,
filename, row-group size, snapshot/incremental mode, or change-stream policy.

## Evidence and decisions

Each candidate summary contains report count, exact structural shape
diversity, total actual scan work, rows examined, and overflow/incomplete/
truncated flags. Actual work comes only from
`AccessExecutionFeedback.calibration.actual_work_units`, which is calculated
by the canonical planner evaluator. Estimated work is never substituted.

`total_actual_scan_work_units` means work observed in executions structurally
relevant to the candidate. It is not work predicted to be saved, a speedup,
or ROI. One scan may support index A, index B, and Columnar C, so candidate
work numbers are not additive. Phase 20 creates no fake statistics, B+Tree
height, Columnar row groups, compression ratio, or hypothetical plan.

Index and Columnar are separate recommendation domains with separate explicit
minimum report, shape-diversity, actual-work, and maximum-recommendation
policies. There is no universal score. Eligible index candidates rank by
actual work, reports, shape diversity, TableId, then ColumnId. Eligible
Columnar candidates use actual work, reports, shape diversity, TableId, then
the canonical column vector. Ranking orders observed support only; it does not
predict ROI. `max_recommendations = 0` retains inspections and emits no winners.
Every candidate remains in the report with a typed `Recommend` or `NoAction`
reason.

## No-mutation theorem and exclusions

`Database::advise_physical_design` takes `&self` and the evidence window by
shared reference. It only reads current schema, placement, active access paths,
and registered projection metadata. Repeated calls with the same database,
window, and policy are exactly equal. It cannot reserve IndexId or
ColumnarProjectionId, generate names/paths, acquire a schema writer, create a
file, modify a catalog, publish G, advance schema generation, change planner
calibration, change automatic safe-mode/trial/scheduler state, execute DDL, or
mutate evidence.

Phase 20 adds no automatic lane. The four lanes remain Columnar maintenance,
Change Stream reclamation, authoritative maintenance, and planner calibration.
It does not change Server Phase 15 capture, `netbadb-server`, `netbadbd`, CLI,
SQL, Manifest v6, NBOP v1, Native Protocol v2, PostgreSQL wire, Inspection JSON
v7, Canonical Schema, SDK schema, or any Heap/BTree/LSM/Columnar/Change Stream/
Coordinator persistent format.

Future Server/operator presentation and any proposal/apply authority require a
separate phase. A later phase must preserve the distinction proved here:
workload evidence may identify what is worth considering, but observation
alone cannot build it.
