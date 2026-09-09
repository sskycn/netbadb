# Adaptive Operations — Phase 7

## Mission and boundary

Phase 7 addresses two explicit long-running concerns above Phase 6:

```text
bounded evidence -> typed candidate readiness -> cross-lane service
caller renewal  -> fresh aggregation with preserved safety high-water
```

It adds neither a scheduler nor an automatic action. Automatic Safe Mode still
only advances an existing incremental Columnar projection, evaluates exact
generation suppression, applies bounded planner calibration, or performs its
receipt-backed revert. Compaction, flush, Change Stream GC, physical design,
placement, repartitioning, and schema mutation remain operator-driven.

## Cross-lane service

`AutomaticCrossLaneServicePolicy` is concrete and has two modes:

- `StrictPhysicalPriority` preserves the Phase 6 default: any ready Columnar
  candidate precedes planner calibration.
- `BoundedColumnarBurst { max_consecutive_columnar_admissions }` is opt-in and
  grants calibration one opportunity once the configured positive Columnar
  burst has been consumed, but only when both lanes contain a ready candidate.

A zero burst is a typed invalid policy. Both bounded candidate sets are
discovered before lane selection. If calibration is disabled, out of scope,
missing evidence, blocked by its advisor, or rejected by shadow replay, it
cannot delay a ready Columnar candidate even when the burst counter exceeds
the limit. If only calibration is ready, it is selected immediately.

There is no cross-lane utility score or ratio. Service selects a ready lane;
the unchanged Phase 6 rank selects a candidate within that lane. Columnar
ranking remains ready age, expected benefit, maintenance work/read/write, and
stable physical identity. Calibration ranking remains ready age, accepted
shadow improvement, QueryShape diversity, report samples, and stable class.

`consecutive_columnar_admissions` is runtime service state. Handing a selected
ready Columnar proposal to Phase 1 increments it with saturating arithmetic,
even if subsequent revalidation aborts. Handing a calibration proposal to
Phase 4 resets it. NoAction, inspection, evidence rotation, and every active
trial Keep/Hold/Await/Revert/Stale resolution leave it unchanged. Thus a stale
Columnar proposal cannot evade the service bound.

The active Phase 5 trial remains an absolute attribution firewall: no
proactive discovery, lane service, or ready-age reconciliation occurs while it
exists. At most one selected proposal reaches one existing mutation authority,
and any abort or error returns without a fallback candidate.

`AutomaticLaneSelectionReason` and detached service-state values explain
strict priority, available burst, due calibration service, a single ready
lane, no ready candidate, or active-trial ownership. Candidate inspection is
read-only and never advances either ready age or the service counter.

## Controlled evidence renewal

`AdaptiveEvidenceWindowEpoch` is a caller-owned runtime aggregation lifetime,
starting at W0. It is not `SchemaGeneration`, `DatabaseCommitSeq`,
`ColumnarGeneration`, or `PlannerCalibrationEpoch`. Only an explicit
`AdaptiveEvidencePool::rotate_window` call advances it; clocks, timers, query
count triggers, and automatic rotation do not exist.

Rotation uses checked epoch increment and returns a typed
`AdaptiveEvidenceRotationReport`. Exhaustion leaves the pool unchanged. A
successful rotation discards current target Q/V aggregates, global calibration
aggregates and visibility counts, retained aggregate epochs, per-window G
diagnostics, sample counts, and incomplete/truncated capacity state. The same
exact target and current calibration epoch may then accumulate fresh evidence.

The discarded aggregation is deliberately separate from retained safety
state. Explicit rotation preserves:

- current `SchemaGeneration`;
- the last accepted G ordering high-water;
- each bounded TableId/ProjectionId lineage's current StorageId and
  ColumnarGeneration;
- retired exact-target rollback guards; and
- the renewed calibration-epoch floor.

Consequently, if W0 ended at G100, W1 still rejects G99 and accepts G100 or a
later G. If the current lineage reached S6/P1/C9, rotation cannot make an old
S5 or C8 report admissible. Known lineage guards share the target-window bound;
renewal can rebuild those current windows but cannot grow an unbounded identity
registry.

`clear` retains its Phase 6 meaning of a complete runtime reset. It may forget
all high-water state. `rotate_window` is the safe long-running renewal API;
`remove_target` removes aggregation only and does not erase lineage safety.

## Schema rotation and health

Schema advance and explicit renewal are distinct. A greater schema generation
retains Phase 6's schema reset semantics, starts a new physical identity
namespace, and also advances the evidence-window epoch for diagnostics. An old
schema report remains rejected. Explicit same-schema renewal preserves the
same-schema G and physical guards.

`AdaptiveEvidencePoolHealth::RotationRecommended` is a read-only signal when
target Q/V truncation, global calibration truncation, or target-capacity
rejection made the current aggregation incomplete. It never rotates the pool.
Overflow and incomplete evidence continue to be barred from Revert or Apply by
the existing Phase 3/4 policies.

An active Columnar trial survives renewal: its exact window is temporarily
missing and the safe step waits for new evidence. A calibration receipt behaves
the same way for its exact applied class and epoch. Rotation itself has no
Database reference and cannot change G, schema, projection eligibility,
suppression, calibration ratios, epochs, fairness, or trial state.

## Runtime and compatibility

Service state belongs to Database runtime; evidence epoch, aggregates, and
identity guards belong to the caller-owned pool. Reopen resets Database trial,
ready age, service counter, and calibration profile. A separately retained pool
still cannot authorize a mutation without current Phase 1/3/4 revalidation.

Phase 7 changes no Canonical Schema, Heap, LSM, WAL, BTree, Columnar, Change
Stream, coordinator, native protocol v2, Schema Spec v2, or Inspection JSON v7
contract. G remains a workload timeline, not a clock or evidence-expiration
token.

## Deliberate limitations

The bounded burst is not weighted scheduling, a universal work score, a
cross-lane quota history, or a guarantee that calibration will remain ready
through mutation revalidation. Evidence renewal retains no historical aggregate
ring. Future work may add an explicit operator policy for renewal cadence or
historical summaries, provided those remain diagnostics and never weaken the
retained safety high-water or single-trial firewall.
