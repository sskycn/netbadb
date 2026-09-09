# Adaptive Operations — Phase 5

## Mission and boundary

Phase 5 adds an explicit synchronous automatic safe-mode coordinator above the
existing adaptive APIs:

```text
operator input + fixed policies + caller-owned workload window
                            |
                            v
                 Database::automatic_safe_step
                    /                   \
       Phase 1 Columnar lane       Phase 4 calibration lane
                    \                   /
                     one active trial
                            |
                            v
                  Keep / Hold / Revert / Stale
```

“Automatic” means that one API call chooses one safe next action inside the
operator's typed boundaries. It does not mean a background scheduler. There is
no timer, thread, async task, sleep, workload collector, automatic table or
calibration-class discovery, or persistent policy.

The only automatic mutations are an existing incremental Columnar catch-up, a
Phase 4 planner-calibration apply, a receipt-authorized revert of that automatic
calibration, and Phase 3 suppression of the exact Columnar projection
generation under trial. Safe mode never creates or drops an index or
projection, changes Heap/LSM placement, mutates schema, compacts LSM or
Columnar data, or runs Change Stream GC.

## Typed API and state

`AutomaticSafeModePolicy` contains explicit enable flags plus the existing
Phase 1, Phase 3, and Phase 4 policies and the calibration-trial hysteresis
policy. Both automatic lanes are disabled by default.

`AutomaticSafeModeInput` supplies an optional Columnar table, optional borrowed
`AdaptiveWorkloadWindow`, optional operator-selected calibration class, and a
`MaintenanceBudget`. The window remains caller-owned. Ordinary `query` and
`query_with_feedback` do not record it or invoke safe mode.

`AutomaticSafeModeReport` is the causal trace for one call: detached trial
summaries before and after, selected lane, at most one mutation, existing
Columnar/workload/calibration subsystem reports, calibration-trial replay
totals, and a typed outcome or no-action reason.
`automatic_safe_mode_state()` returns only a copied immutable summary.
`abandon_automatic_safe_trial()` returns the same kind of summary and clears
probation without changing a projection, suppression key, calibration ratio,
epoch, G, schema, or user data.

Internally, `Database` owns one optional enum value:

```text
None
Columnar { exact AdaptiveWorkloadTarget }
PlannerCalibration { private receipt, schema generation }
```

The single enum slot makes overlapping automatic experiments
unrepresentable. Public summaries deliberately omit the private
`PlannerCalibrationReceipt`; inspection is not revert authority.

## Attribution firewall and lane order

An active trial owns the whole step. While it is active, safe mode evaluates
only that trial and cannot start Columnar maintenance or another calibration.
This attribution firewall prevents a second automatic control change from
confounding the evidence for the first.

With no active trial, lane order is deterministic:

```text
existing Columnar catch-up -> planner calibration -> typed NoAction
```

The Columnar lane calls `adaptive_columnar_step`, never generic
`maintenance_step`. A real Phase 1 proposal attempt ends the safe step for any
execution outcome. A Phase 1 `NoAction` permits the calibration lane. The
calibration lane performs advisor, shadow, and apply in order and ends after
either typed rejection/no-action or one apply. Consequently no call can apply
two control mutations.

The maintenance budget is passed unchanged to Phase 1 and controls physical
Columnar work. Calibration is bounded computation over an already bounded
window; it does not invent fake read/write-byte costs to fit an I/O budget.

## Columnar trial

A Phase 1 `Kept` outcome starts a trial bound to the actual post-change
`TableId`, `StorageId`, `ColumnarProjectionId`, `ColumnarGeneration`, and
`SchemaGeneration`. It does not use the pre-change generation.

Trial evaluation reuses `evaluate_adaptive_workload`. Matching complete
evidence therefore retains Phase 3's thresholds, checked aggregate arithmetic,
integer hysteresis, and exact-generation suppression behavior:

- `ValidatedKeep` clears probation and does not mutate eligibility.
- `HeldWithinHysteresisBand` and `Inconclusive` retain probation.
- `RevertedMeasuredRegression` suppresses only the exact P/C and clears it.
- `HeldSuppressed` clears a trial whose exact generation is already suppressed;
  historical evidence never unsuppresses it.
- `StaleWindow` clears probation without mutating the newer target.

No window yields typed awaiting evidence. A window for another target yields a
typed target mismatch and cannot affect either target. Even in those cases an
empty target-bound Phase 3 window is used only to revalidate current physical
and schema identity, avoiding a second staleness formula.

## Calibration trial

Automatic calibration reuses Phase 4
`advise_planner_calibration -> shadow_planner_calibration ->
apply_planner_calibration`. Only an accepted shadow can be applied. A successful
apply starts a trial that privately holds the actual receipt and the schema
generation at apply time.

Post-apply validation selects only bounded evidence for the receipt's class and
applied epoch. Old-epoch raw totals are never compared with new-epoch totals.
For every QueryShape observed under the applied epoch, the same pure ratio
replay used by Phase 4 shadowing evaluates both counterfactuals:

```text
old estimate = apply(shape base total, receipt previous ratio)
new estimate = apply(shape base total, receipt applied ratio)
old error    = |old estimate - shape actual total|
new error    = |new estimate - shape actual total|
```

Checked error totals are then compared only after minimum samples, actual work,
distinct visibility points, and distinct QueryShapes are satisfied:

```text
old_error - new_error >= minimum keep improvement  -> Keep and clear
new_error - old_error > maximum tolerated regression -> receipt revert and clear
otherwise                                           -> Hold and retain
```

Incomplete, overflowed, truncated, arithmetically unavailable, or insufficient
evidence is typed awaiting evidence and cannot revert. Revert invokes Phase
4's existing receipt API, restores the previous ratio, and publishes a new
runtime epoch. Safe mode never reconstructs or forges a receipt.

Before evaluating evidence, the trial checks current schema, global epoch, and
the class's applied ratio. A manual apply/revert or schema change makes the
trial stale; safe mode clears it and never overwrites newer operator state.
Evidence with an unrelated schema is an input mismatch, not mutation
authority.

## G, durability, and unchanged contracts

`DatabaseCommitSeq` remains a logical workload timeline. Ordinary DML and G
advance neither expire a Columnar trial nor a calibration trial. Columnar
physical identity and schema changes stale a Columnar trial; schema, epoch, or
applied-ratio changes stale a calibration trial. Recording, evaluating,
holding, keeping, abandoning, applying calibration, reverting calibration, and
exact-generation suppression do not publish user data or roll it back.

Safe-mode state and both trial kinds are runtime-only. Close/open clears the
trial; the Phase 4 profile independently reopens as epoch-zero identity.
Existing durable Heap, LSM, WAL, BTree, Columnar, Change Stream, coordinator,
catalog, and schema contracts are unchanged. Protocol v2, Schema Spec v2, and
Inspection JSON v7 are unchanged. No safe-mode WAL, catalog record, manifest
field, protocol message, or inspection field is added.

## Deliberate limitations

Phase 5 accepts one explicitly named table and one explicitly named calibration
class. It has no persistent workload history, rolling or wall-clock window,
automatic window rotation, ranking, cross-target policy database, concurrent
experiment scheduler, automatic physical design, or automatic general
maintenance. A future phase may add an operator-owned orchestration layer only
after preserving the one-trial attribution and mutation bounds defined here.
