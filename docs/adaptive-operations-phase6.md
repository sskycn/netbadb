# Adaptive Operations — Phase 6

## Mission and boundary

Phase 6 adds explicit multi-target evidence admission above the Phase 5 safe
experiment coordinator:

```text
query_with_feedback -> explicit record -> bounded evidence pool
                                      -> candidate discovery
                                      -> deterministic admission
                                      -> one Phase 5 experiment
```

It changes selection, not scheduling or the safe action surface. There is no
background worker, timer, implicit query capture, generic maintenance lane, or
automatic physical design. The only mutations remain existing incremental
Columnar catch-up, exact-generation suppression through Phase 3, and bounded
Phase 4 calibration apply or receipt-backed revert.

## Explicit caller-owned evidence

`AdaptiveEvidencePool` is a caller-owned, runtime-only value. Ordinary
`Database::query` and `query_with_feedback` never record into it. The caller
must pass each `ExecutionFeedbackReport` to `record_execution_feedback`.
The pool retains typed QueryShape, PlanVariant, exact physical identities,
calibration class/epoch, G coordinates, and integer aggregates. It never
retains SQL, results, literal or parameter values, storage references, page
guards, or transaction handles.

One report may contain several exact Columnar targets. It is recorded once in
each relevant target window, while the global calibration accumulator receives
the report exactly once. Calibration report sample count is one per report and
class even when that class has several access nodes; work totals still include
all comparable access evidence. Phase 3 and Phase 6 share the same Q/V,
visibility, checked-access aggregation, and Phase 4 aggregate/advisor code.

## Rotation, ordering, and bounds

The pool uses database-global `SchemaGeneration`. A greater schema generation
clears all schema-sensitive target and calibration evidence and begins a new
pool epoch. An older schema report is rejected and can never rotate backward.
Within one schema, reports must arrive in nondecreasing `DatabaseCommitSeq`
order. G100, G101, and G105 belong to one workload timeline; G advancing does
not expire evidence.

A Columnar lineage is `TableId + ColumnarProjectionId`; its current authority
also binds `StorageId`, `ColumnarGeneration`, and `SchemaGeneration`. A higher
generation or changed storage identity rotates the lineage to a fresh exact
window. Retired exact identities and lower generations cannot rotate it back.
Only one current full window is retained per lineage.

`AdaptiveEvidencePoolLimits` bounds target windows, Q/V cardinality, retained
calibration epochs, and global calibration Q/V cardinality. New unrelated
targets beyond capacity are rejected without eviction or panic. Calibration
epochs evict the numerically oldest epoch deterministically. Target-window
truncation, pool capacity rejection, report incompleteness, and checked
arithmetic overflow remain typed incomplete evidence and cannot authorize a
workload revert or calibration change. `clear` and `remove_target` affect only
runtime evidence.

## Operator scope and candidate discovery

`AutomaticAdmissionScope` explicitly lists allowed tables and calibration
classes. Inputs and the complete bounded candidate/fairness set are limited by
`AutomaticMultiSafeModePolicy`; duplicate scope entries are stably deduplicated.
Changing scope is a hard boundary: evidence outside the current scope cannot be
selected.

`inspect_automatic_candidates` is read-only. It does not mutate the pool,
fairness age, active trial, projection, calibration profile, G, or schema. Each
candidate has a typed key, lane, readiness/blocker, and integer rank evidence.
Columnar discovery uses one Phase 1 decision for every existing projection,
not the legacy first-proposal shortcut. Calibration discovery uses current
class/epoch evidence and the unchanged Phase 4 advisor and shadow replay.

Candidate keys identify and rank intent; they are never mutation authority.
The selected Columnar candidate still carries an `AdaptiveMaintenanceProposal`
into `execute_adaptive_columnar`, and calibration still requires an accepted
`PlannerCalibrationProposal` shadow at `apply_planner_calibration`. Both APIs
perform their existing current-state revalidation.

## Admission, ranking, and fairness

Admission tiers are fixed:

```text
active trial -> ready Columnar -> ready planner calibration -> NoAction
```

No floating-point or cross-lane utility score exists. Physical state is
stabilized before the cost model. Within a lane, `ready_age` is compared first.
Columnar then compares expected benefit descending, maintenance work/read/write
ascending, and stable table/projection identity ascending. Calibration then
compares accepted shadow improvement, distinct QueryShapes, and report samples
descending, followed by stable class order.

Only a real admission round changes age. The selected candidate resets to zero;
other ready candidates increment with saturating arithmetic. Blocked or missing
candidates are removed during reconciliation, and the state is bounded by the
same admission limit. Saturation is safe because age is only a fairness key,
never evidence or eligibility. Readiness is evaluated before age, so fairness
can change which safe candidate wins but can never make a blocked candidate
safe. Fairness is within a lane only; calibration can remain starved while a
Columnar candidate is continuously ready.

## Attribution firewall and one mutation

An active Phase 5 trial has absolute priority. Multi-step performs no proactive
discovery or fairness update while it exists. A Columnar trial looks up only its
exact target window; absence waits, while current physical/schema mismatch
stales the trial. A calibration trial looks up only its exact applied class and
epoch and uses Phase 5's same-epoch counterfactual ratio replay. Older epochs
cannot substitute.

With no trial, one selected candidate reaches one existing mutation API and the
step returns. Revalidation failure, stale proposal, abort, or inconclusive
execution never falls through to another candidate in the same call. Thus one
global trial and at most one state-changing control action remain invariant.

## Runtime and compatibility

The pool is caller-owned; Database owns only the runtime trial and bounded
fairness state. Reopen resets Database-owned trial, fairness, and calibration
profile. A caller that retains an old pool still cannot use it as authority:
Phase 1, Phase 3, and Phase 4 revalidate current schema, physical identity,
generation, epoch, and ratio before mutation.

Phase 5's narrow `automatic_safe_step` API is unchanged. Phase 6 changes no
Canonical Schema, Heap, LSM, WAL, BTree, Columnar, Change Stream, coordinator,
native protocol v2, Schema Spec v2, or Inspection JSON v7 contract. Evidence,
fairness, trials, and calibration remain non-durable.

## Deliberate limitations

Phase 6 has no wall-clock rotation, persistent history, background collection,
cross-lane quota, automatic LSM/Columnar compaction, Change Stream GC, index or
projection creation, storage placement, or schema design. A later phase may
define explicit cross-lane service policy, but only if it preserves existing
proposal authority and the one-trial attribution firewall.
