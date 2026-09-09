# Adaptive Operations — Phase 4

## Mission and boundary

Phase 4 adds an explicit bounded planner-calibration loop above Phase 3:

```text
Phase 3 class/epoch aggregates -> advisor -> proposal -> shadow replay
                              -> explicit apply -> epoch N+1
                              -> future planning
```

Calibration is a runtime overlay on future estimates. It never rewrites
`TableStatistics`, `IndexStatistics`, `AccessCostHints`, Columnar planning
snapshots, executor counters, or recorded actual work. There is no background
collector or automatic apply.

## Ratio, classes, profile, and epoch

`CalibrationRatio` is a normalized positive `u64/u64` rational. Zero
numerators and denominators are rejected. Equality and ordering compare exact
integer fractions; no float participates in planning or policy. Applying a
ratio uses checked `u128` multiplication and ceiling division. Zero remains
zero. If an effective result cannot fit, diagnostic effective work is
unavailable and physical planning falls back to the valid base cost rather
than failing the query.

The only global calibration classes are:

```text
SeqScan          <- SeqScan, PartitionedSeqScan
IndexPoint       <- IndexPoint, PartitionedIndexPoint, index nested-loop point work
IndexRange       <- IndexRange, PartitionedIndexRange
Columnar         <- ColumnarScan
```

There are no per-query, table, storage, index, access-path, projection, join,
sort, aggregate, or filter ratios. `PlannerCalibrationProfile` is a fixed
strongly typed value containing these four ratios and one
`PlannerCalibrationEpoch`. Epoch 0 is the all-1/1 identity profile. Core owns
the current profile and passes one immutable snapshot into each planning call;
the planner reads no mutable global state.

Existing planner APIs use the identity profile. Calibration-aware variants
are explicit. With identity calibration, plan choice, cost ties, base and
effective estimates remain the pre-Phase-4 behavior.

## Base, effective, and actual evidence

Every selected `PlannerAccessEstimate` retains the uncalibrated
`estimated_work_units` base value and separately reports
`effective_work_units` plus its epoch. A selected Columnar scan likewise keeps
the base `source_alternative_work_units` and a separate calibrated effective
source alternative. Phase 2 and Phase 3 physical-regression decisions continue
to use the base source alternative.

`PlannerCalibrationSample` contains base and effective estimates, base and
effective direction/error, actual work, and epoch. Actual work continues to be
derived only from executor raw counters by the planner-owned canonical actual
evaluator. It is never multiplied by a calibration ratio.

The effective value participates in real Seq/index/Columnar, partition-local,
and index nested-loop access selection at the canonical access-cost comparison.
The overlay is applied once to each base candidate; it is not embedded into or
duplicated across the base formulas.

## Phase 3 aggregate input

Phase 4 does not rescan raw execution reports. `AdaptiveWorkloadWindow` groups
calibration evidence by `PlannerCalibrationClass + PlannerCalibrationEpoch`
inside its bounded QueryShape/PlanVariant structure. Each group retains sample
count, base/effective/actual totals, both error totals, and both direction
counts. A bounded class/epoch visibility tracker retains exact distinct G
counts without storing an unbounded report history.

The advisor selects only evidence for the current epoch and builds one bounded
aggregate per distinct logical QueryShape. Epoch 0 and epoch 1 samples may
coexist for diagnostics but cannot be mixed into one proposal. Window,
calibration-group, or arithmetic incompleteness and truncation yield typed
`NoAction(IncompleteEvidence)`.

## Advisor and deterministic policy

`PlannerCalibrationPolicy` requires minimum samples, actual work, distinct
visibility points, and distinct QueryShapes before considering a proposal.
Direction is evaluated per QueryShape using that shape's aggregate effective
estimate versus actual work. A systematic increase requires:

```text
underestimated_shapes - overestimated_shapes >= directional_margin
```

The decrease rule is symmetric. Small aggregate error within the integer work
deadband produces `WithinDeadband`; conflicting shape directions produce
`InconsistentEvidence`.

The numerical target is the aggregate `actual/base` ratio. Bounds are applied
in this order:

```text
raw actual/base -> global hard clamp -> per-epoch up/down step clamp
```

The current ratio must itself be inside the policy's global bounds. Every
comparison uses normalized integer ratios and checked arithmetic.

A proposal retains the base epoch, schema generation, class, current/raw/global
and proposed ratios, complete QueryShape aggregate evidence, and policy. It is
first-class explanatory intent, not mutation authority.

## Shadow, apply, and revert

Shadow evaluation replays model error over each retained QueryShape aggregate;
it does not claim to re-plan historical statements:

```text
old = apply(shape base total, current ratio)
new = apply(shape base total, proposed ratio)
old error = |old - actual|
new error = |new - actual|
```

Checked totals must prove that old error minus new error meets the policy's
minimum shadow improvement. Overflow, unavailable ratio application, or
incomplete evidence rejects shadow. Apply requires the exact accepted shadow
and recomputes it before mutation.

Apply then revalidates current schema generation, calibration epoch, and class
ratio. It does not rebase stale intent. Ordinary `DatabaseCommitSeq` advance is
not a staleness condition because G is the workload timeline; schema or epoch
advance is. Successful apply changes one class ratio and checked-increments the
global epoch. It does not publish G, change schema, mutate a storage data
version, or alter Columnar suppression.

The runtime receipt records class, previous/applied epoch, and previous/applied
ratio. Revert is accepted only while that exact applied epoch and class ratio
remain current. It restores the previous ratio at a newly incremented epoch:

```text
E4 1/1 -> apply -> E5 9/8 -> revert -> E6 1/1
```

An old receipt cannot overwrite later calibration. Calibration revert neither
suppresses nor unsuppresses a Columnar generation, and Phase 3 regression never
reverts a calibration profile.

## Durability and exclusions

Profiles, epochs, aggregate calibration evidence, proposals, shadows, and
receipts are runtime-only. Close/open reconstructs epoch 0 identity while user
data, schema, global G, Heap, LSM, Change Stream, and Columnar state follow
their existing durable contracts.

Phase 4 adds no calibration WAL, catalog, manifest, protocol message, Schema
Spec field, or inspection field. Canonical Schema, Heap, LSM, WAL, BTree,
Columnar, Change Stream, Coordinator, native Protocol v2, Schema Spec v2, and
Inspection JSON v7 remain unchanged. Automatic calibration, persistent
learning, wall-clock windows, moving averages, ML, physical design, new
adaptive targets, and join/sort/aggregate calibration remain out of scope.
