# Adaptive Operations — Phase 8

## Mission and boundary

Phase 8 adds one opt-in action to multi-target Automatic Safe Mode:

```text
existing Columnar Catch-up
existing Columnar Compaction
existing Planner Calibration
```

Columnar compaction is the first expanded action because it rewrites only
derived, reconstructable representation. Heap or LSM rows remain authoritative.
LSM flush/compaction, Change Stream GC, physical design, schema mutation, and a
background scheduler remain outside Automatic Safe Mode.

The Phase 5 narrow `automatic_safe_step` API is unchanged. Multi-safe policy
adds `allow_columnar_compaction`, which defaults to false, so Phase 6/7 default
behavior and strict cross-lane priority remain unchanged.

## Production eligibility plus automatic pressure

`AdaptiveColumnarCompactionPolicy` has two integer thresholds:

```text
segment trigger = minimum_delta_segments != 0
                  && actual segments >= threshold
byte trigger    = minimum_delta_bytes != 0
                  && actual bytes >= threshold
eligible pressure = segment trigger || byte trigger
```

At least one threshold must be non-zero. This pressure policy only decides
whether an otherwise valid production candidate is worth automatic
consideration. `MaintenanceBudget` independently admits work, read bytes,
write bytes, and one action.

Core's production `MaintenanceCandidate::CompactColumnar` remains the single
source of structural eligibility and estimate semantics. It requires a fresh
incremental projection with a Delta, no conflicting activity, and a fitting
`EstimateGatedAtomic` budget. Snapshot, lagging, rebuild-required, unavailable,
no-Delta, busy, and budget states retain their existing typed
`MaintenanceBlocker`. The automatic threshold does not modify manual
`inspect_maintenance` or `maintenance_step` behavior.

## Observation, proposal, and revalidation

`observe_adaptive_columnar_compactions` composes the existing Phase 1
source/projection observation with exact production compaction candidates. Its
detached values contain no storage handles. The pure advisor returns either a
typed blocker or `AdaptiveColumnarCompactionProposal`.

The proposal binds TableId, StorageId and kind, SchemaGeneration, source
snapshot and data version, exact projection ID/generation/frontier, source
frontier, Delta segment/byte/mutation/suppressed-version counts, production
bound and estimate, planner evidence, and both policies. It is evidence-bound
intent, not mutation authority.

Execution first admits the supplied budget and then rebuilds the exact current
production candidate and adaptive proposal. Schema, source identity/snapshot/
data version, projection generation/frontier, Delta evidence, eligibility,
estimate, planner evidence, or policy drift aborts before mutation. An
unrelated table may advance `DatabaseCommitSeq`; G is retained for causal
diagnostics but is not the sole stale token. A change to the proposal's source
changes its snapshot/data version and makes the projection lagging, so Catch-up
owns the next operation.

## Production writer and measurement

The only writer remains `Database::compact_columnar_projection`. A small
crate-private maintenance helper lets manual and automatic paths share that
writer and the production consumption mapping without changing the manual
maintenance cursor. No adaptive compaction file writer, WAL, or recovery log
exists.

The adaptive execution report retains the original proposal, budget before and
remaining, actual `MaintenanceConsumption`, the complete production
`ColumnarCompactionReport`, and before/after physical context. Production
measurement provides old/new generations, compacted frontier, Base row counts,
consumed Delta segments and mutations, removed suppressed versions, folded live
rows, bytes before/after/reclaimed, and whether work occurred.

Actual read bytes are production `bytes_before`, actual write bytes are
`bytes_after`, work is the admitted production estimate, and actions is one.
If actual consumption exceeds the granted envelope, compaction is not physically
rewound: the exact resulting generation is suppressed, no trial starts, and the
outcome is `InconclusiveCostBoundExceeded`.

Successful work must advance generation, retain the applied frontier, leave the
source snapshot/data version unchanged, and leave G and SchemaGeneration
unchanged across execution. A failed postcondition suppresses the exact result
and is inconclusive. Query results and authoritative source rows remain
unchanged because compaction changes representation only.

## Physical gate and workload probation

After physical publication, Core reuses Phase 1 planner evidence and
`AdaptivePolicy::minimum_keep_benefit_work_units`. Insufficient measured benefit
suppresses the exact new generation and does not start probation. Passing the
physical gate proves representation and accounting properties; it does not
prove workload value.

A successful automatic compaction starts the existing
`AutomaticColumnarTrial` for the exact new TableId/StorageId/ProjectionId/
ColumnarGeneration/SchemaGeneration. No compaction-specific third trial exists.
The global attribution firewall then blocks catch-up, another compaction, and
calibration until Phase 3 evidence resolves or the operator abandons the trial.

Phase 3 Keep clears probation, Hold/Inconclusive retains it, and measured
regression suppresses only the compacted generation. Revert means planner
fallback to authoritative source, not physical un-compaction. The old
generation is not restored, and its history never moves backward. A later
generation does not inherit an earlier exact-generation suppression.

The caller-owned evidence pool is not mutated by compaction. It initially lacks
the new target and the trial waits. Real query feedback for the new generation
naturally advances the Phase 6/7 lineage window; retired-generation evidence
cannot re-enter.

Manual `compact_columnar_projection` and `maintenance_step` remain available
and do not consult the automatic pressure policy, ready ages, or service
counter. If an operator manually compacts a generation that currently owns an
automatic trial, the next safe-mode step observes the exact target mismatch,
marks that old trial stale, and never suppresses the manually published new
generation. Manual compaction likewise has no hidden hook into the caller-owned
evidence pool; later real feedback performs the ordinary lineage rotation.

## Admission and service

Catch-up and compaction have distinct `AutomaticCandidateKey` variants but
share `ColumnarMaintenance`. Candidate inspection exposes Delta pressure plus
production work/read/write estimates. It remains read-only.

Within the Columnar lane, ready age is compared first. At equal age, Catch-up
precedes compaction so freshness remains primary. Catch-up retains its Phase 6
rank. Compaction then compares Delta bytes, segment count, suppressed versions,
mutation count, lower maintenance cost, and stable identity using integers.
Thus an older compaction candidate can still beat newly ready catch-up work.

A selected compaction consumes one Phase 7 Columnar service opportunity before
revalidation, just like catch-up. Active-trial resolution and inspection do not
change the burst counter. Cross-lane selection still chooses only a ready lane;
it never converts a blocker into authority. One safe step reaches at most one
proposal and never falls through after success, stale revalidation, abort, or
inconclusive completion.

## Crash, reopen, and compatibility

Automatic compaction inherits the existing temporary-file, synchronization,
manifest publication, immutable generation swap, checksum, retirement, crash,
and reopen contracts. The new physical generation is durable. Trial, ready age,
cross-lane service state, suppression, evidence, and calibration remain
runtime-only and reset according to their existing reopen rules; probation is
not restored.

Phase 8 changes no Canonical Schema, Heap, LSM, WAL, BTree, Columnar format,
Change Stream, coordinator, protocol v2, Schema Spec v2, or Inspection JSON v7.
There is no automatic LSM maintenance, Change Stream GC, physical design,
implicit evidence collection, timer, or background scheduler.
