# Adaptive Operations — Phase 3

## Mission and boundaries

Phase 3 groups explicit Phase 2 execution reports into a bounded,
caller-owned workload window and applies deterministic hysteresis to one exact
existing Columnar projection generation:

```text
typed logical plan -> LogicalQueryShape
physical plan      -> PlanVariant
execution @ G      -> ExecutionFeedbackReport
                  -> AdaptiveWorkloadWindow
                  -> Keep / Hold / Revert / Inconclusive
```

There is no background collector, clock-based window, persistent workload
history, query-learning service, automatic cost calibration, scheduler, or new
adaptive target. Ordinary `Database::query` neither derives shapes nor records
history. `query_with_feedback`, window recording, and evaluation remain
explicit synchronous operations.

## Logical query shape

`netbadb-rel::LogicalQueryShape` is a collision-safe structural key derived
directly from the resolved and type-checked `LogicalPlan`. No SQL text or hash
digest is semantic authority. The type implements structural equality and
`Hash`; a caller may use the hash for bucket selection, but equality still
compares the complete typed representation.

The shape covers OneRow, Scan, Join and JoinKind, Filter, Sort direction and
NULL order, Project, ScalarProject, Aggregate function/input/grouping, and
Limit. Expressions cover resolved columns, normalized literals, parameters,
casts, binary/unary operations, and IS NULL/IS NOT NULL. Derived display names,
table names, relation names, column display names, aliases, whitespace, and
literal textual spelling are absent.

Resolved columns retain TableId, ColumnId, semantic type, nullability, and a
`CanonicalBindingOrdinal`. Query-local `RelationBindingId` values are remapped
by logical scan first occurrence to ordinal 0, 1, and so on. Self-join
occurrences therefore remain distinct while equivalent recompilations and
alias changes remain equal.

Ordinary literal payloads are erased. Their resolved `ExprType` and NULL versus
non-NULL state remain, so `id = 10` and `id = 11` share a shape, while a NULL
literal remains different. Parameters retain `ParameterId` and resolved type
and are not equated with literals. LIMIT values remain structural because they
change the execution contract.

## Physical plan variant

`netbadb-planner::PlanVariant` answers how one query shape was executed. It is
another typed structural key, covering sequential, point-index, range-index,
partitioned, and Columnar access; nested-loop, index nested-loop, and hash
joins; Filter placement; Sort; Aggregate; Project; and Limit.

The key retains stable physical identities where available: TableId,
StorageId, PartitionId, AccessPathId, and ColumnarProjectionId. Point/range
literal payloads are normalized while bound form and inclusivity remain.
`PlanNodeOrdinal` is not part of the key; it remains only the within-one-plan
correlation identity between planner estimates and executor samples.

ColumnarGeneration is deliberately absent from PlanVariant. P1/C8 and P1/C9
represent the same P1 physical strategy, but every execution access and
adaptive target continues to carry the exact generation. Strategy history can
therefore group across generations without allowing C8 evidence to suppress
C9.

`ExecutionFeedbackReport` receives its QueryShape and PlanVariant while the
typed logical and physical plans still exist, before execution. It never
reparses SQL after execution.

## Target window and G timeline

`AdaptiveWorkloadWindow` binds exactly one:

```text
TableId + StorageId + ColumnarProjectionId + ColumnarGeneration
        + SchemaGeneration
```

It stores aggregation evidence only: no result rows, SQL, parameters, literal
payloads, source data versions, or change-stream frontier. Only report access
nodes that actually used that exact P/C/table/storage contribute to target
sample and decision totals. Multiple matching access nodes in one report are
checked-summed into one query sample; access count remains separately visible.
Non-target reports may contribute comparable calibration diagnostics but do
not satisfy target evidence thresholds.

Reports must have global visibility, the window SchemaGeneration, and
nondecreasing DatabaseCommitSeq order. Out-of-order input is rejected before
mutation. The window records first G, last G, and distinct relevant visibility
points.

G is a workload timeline, not an expiration token. Reports from G100, G101,
and G103 may coexist when schema and exact target identity remain unchanged.
Ordinary user-data publication and StorageDataVersion advancement do not by
themselves invalidate evidence. Source version and Change Stream frontier are
maintenance-domain facts, not workload expiration tokens.

This intentionally differs from Phase 2:

- Phase 2 evaluation retains exact `report.anchor == current_anchor` freshness;
- Phase 3 permits historical G values and revalidates current schema and exact
  physical target identity at window evaluation.

A schema change, missing or changed table/storage/projection identity, or
projection generation change returns a typed stale-window outcome. An old C8
window can never suppress C9.

## Bounded aggregation and calibration

The caller supplies fixed maximum query-shape and per-shape plan-variant
counts. A new group beyond either limit is ignored and marks the window
`truncated + incomplete`; such a window cannot revert. Structural keys are
kept directly in bounded vectors, so no digest collision can merge groups.

All sample, access, work, visibility, direction, and absolute-error totals use
checked integer arithmetic. Overflow leaves the previous total intact and
marks the relevant aggregate or target window `overflowed + incomplete`.
Incomplete target evidence, overflow, or truncation cannot trigger Revert.

Each QueryShape/PlanVariant group exposes comparable planner calibration by
access kind: sample count, estimated and actual work, absolute error, and
exact/underestimated/overestimated counts. SeqScan, IndexPoint, IndexRange, and
Columnar evidence use Phase 2's planner-owned evaluator. Filter and operators
without canonical work formulas receive no invented calibration. Observed
alternate-plan actuals are diagnostics only; Phase 3 never rewrites
AccessCostHints, statistics, planner weights, or coefficients.

## Deterministic hysteresis

`AdaptiveWorkloadPolicy` contains only integer thresholds:

- minimum query samples;
- minimum accumulated actual work;
- minimum distinct visibility points;
- minimum keep improvement work; and
- maximum tolerated regression work.

Evidence thresholds run before hysteresis. With target actual work A, retained
planning-time source-alternative work S, keep threshold K, and regression
tolerance R:

```text
S >= A and S - A >= K  -> ValidatedKeep
A > S and A - S > R    -> RevertedMeasuredRegression
otherwise              -> HeldWithinHysteresisBand
```

The comparisons use ordered checked integer differences, never float ratios.
Totals weight expensive queries naturally rather than giving each shape one
vote. Low-frequency shapes do not independently block a complete aggregate.

Hold changes no eligibility. Revert only runtime-suppresses the exact P/C
generation. It does not roll back user DML, decrement or publish G, change Heap
or LSM truth, drop a projection, or delete files. A previously suppressed exact
generation is never unsuppressed by historical Phase 3 evidence: a would-be
Keep or Hold returns `HeldSuppressed`. A new generation has a different
suppression key.

## Durability and compatibility

Shapes, variants, windows, hysteresis outcomes, and calibration aggregates are
runtime values and are never written to WAL, catalogs, manifests, or workload
tables. Close/open retains only the existing authoritative source, schema, G,
and Columnar formats; no database-owned workload history is reconstructed.

Phase 3 changes no Canonical Schema, Heap, LSM, WAL, BTree, Columnar, Change
Stream, native protocol v2, Schema Spec v2, or Inspection JSON v7 contract.
