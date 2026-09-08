# Adaptive Operations — Phase 2

## Mission and scope

Phase 2 adds one explicit execution-feedback vertical slice:

```text
planning-time snapshots -> physical plan + estimates
                       -> executor raw counters
                       -> Core correlation
                       -> estimate/actual calibration
                       -> explicit Columnar feedback evaluation
```

The adaptive target remains an already-existing Columnar projection
generation. Phase 2 does not create physical structures, tune planner weights,
collect background workload history, persist telemetry, fingerprint queries,
or introduce a scheduler. Every operation is synchronous, deterministic, and
caller initiated.

## Responsibilities and instrumentation

The planner owns storage-neutral work formulas. `PlannerAccessEstimate`
captures the selected access kind, deterministic `PlanNodeOrdinal`, typed
table/binding/physical identities, estimated work, and the work model required
to evaluate later raw evidence. The estimate is calculated from the exact
immutable snapshots used to build that physical plan and is retained before
execution. It is never reconstructed from newer `ANALYZE` or projection state.

The executor owns measurement, not policy. `ExecutionStatistics` is opt-in;
normal `execute` and `Database::query` callers neither allocate its sample
vectors nor handle a new result type. A report supports multiple access nodes
and partition subscans. It records only counters available from real execution:

- SeqScan rows examined and emitted;
- point/range probe count, candidate rows examined, emitted rows, and the
  selected `AccessPathId`;
- Filter rows evaluated, passed, and rejected; and
- the complete production `ColumnarScanStatistics`, including row-group,
  row, byte/block, decode, pruning, delta, suppression, and merge counters.

Columnar scanner counters are embedded rather than re-counted. Join, sort, and
aggregate work is not assigned invented planner units in Phase 2. An
IndexNestedLoopJoin does expose its actual outer SeqScan and inner point-probe
access work, while general join calibration remains out of scope.

`ExecutionWork` is raw evidence, not planner work. Core converts the executor's
storage-bearing sample into planner-neutral evidence, then calls the planner's
pure `evaluate_actual_access_work`. SeqScan, point/range index access, and
Columnar access therefore receive comparable integer work units from one
canonical evaluator. `PlannerCalibrationSample` keeps estimated and actual
values, an exact direction, and an overflow-safe absolute error; no floating
point ratio is a correctness input.

All counters saturate. Overflow marks both the affected sample and report as
`overflowed` and `incomplete`; it cannot create an `ExecutionError` or change
rows, ordering, transaction semantics, or ordinary query errors. Incomplete
evidence is never allowed to trigger adaptive revert.

## Correlation and identity

`Database::query_with_feedback` returns the ordinary `QueryResult` plus an
`ExecutionFeedbackReport`. Core correlates planning and execution by physical
preorder `PlanNodeOrdinal`, access kind, table, storage, and access-path or
projection identity. Ordinals start at zero and are meaningful only for that
exact in-memory physical plan; they are never serialized and use neither
addresses nor random identity.

The report anchor contains the statement's optional published
`DatabaseCommitSeq` and exact `SchemaGeneration`. Each access also carries
`RelationBindingId`, `TableId`, and `StorageId`. Columnar evidence additionally
binds `ColumnarProjectionId` and `ColumnarGeneration`. Collecting feedback is
observability: it publishes no global visibility boundary and increments no
database commit sequence.

## Columnar feedback lifecycle

Phase 1 `AdaptiveMaintenanceOutcome::Kept` means a physical catch-up completed
and the planner-based post-change threshold accepted it. It does not claim
that real executions validated the choice.

Phase 2 separately evaluates a caller-supplied slice of reports through
`Database::evaluate_adaptive_execution_feedback` and
`ExecutionFeedbackPolicy`:

- `Inconclusive` means the sample count or accumulated actual work is below
  policy, or any relevant evidence is incomplete/overflowed;
- `ValidatedKeep` means sufficient complete samples do not exceed the retained
  planning-time authoritative-source alternative plus the allowed regression;
- `RevertedMeasuredRegression` means sufficient complete actual work exceeds
  that deterministic bound; and
- `StaleFeedback` means the current projection generation or report's global
  and schema anchor no longer matches.

The policy has only `minimum_actual_samples`, `minimum_actual_work_units`, and
`maximum_allowed_regression_work_units`. It contains no clock, window, moving
average, percentile, or automatic calibration.

A measured regression suppresses only the exact runtime pair
`(ColumnarProjectionId, ColumnarGeneration)`. It does not roll back user data,
change Heap or LSM state, drop the projection definition, delete projection
files, or decrement/publish `DatabaseCommitSeq`. A newer generation cannot be
suppressed by an older sample. Suppression and all feedback/report history are
runtime-only and disappear on reopen under the Phase 1 durability rule.

## Compatibility and exclusions

Phase 2 changes no Canonical Schema, Heap, LSM, WAL, BTree, Columnar, Change
Stream, protocol, Schema Spec, or catalog format. It does not modify Inspection
JSON v7. Feedback is a typed embedded runtime API.

Automatic coefficient learning, persistent workload windows, normalized query
fingerprints, latency-based correctness, background collection, automatic
index/projection creation, placement switching, repartitioning, and new
adaptive targets remain explicit future work.
