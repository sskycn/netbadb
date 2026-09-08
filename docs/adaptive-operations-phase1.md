# Adaptive Operations — Phase 1

## Mission and scope

Adaptive Operations connects existing observation, planning, storage, change
stream, and maintenance primitives into one explicit synchronous control loop:

```text
Observe -> Decide -> Revalidate -> Change -> Measure -> Keep / Revert
   ^                                                        |
   +--------------------------------------------------------+
```

Phase 1 has exactly one adaptive target: advancing an already-existing
incremental Columnar projection. It does not create or drop projections or
indexes, change Heap/LSM placement, repartition data, perform schema migration,
or run maintenance from `SELECT`. The API has no scheduler, timer, background
thread, or async runtime. The caller invokes each step explicitly.

## Observation and consistency

`Database::observe_adaptive_columnar` returns a detached immutable
`AdaptiveObservation`. It contains values and stable identities, never storage
references, page guards, transaction handles, or mutable subsystem state.

The observation binds its evidence to:

- the published `DatabaseCommitSeq` and current `SchemaGeneration`;
- `TableId`, `StorageId`, `StorageKind`, the equality-only
  `StorageSnapshotToken`, and `StorageDataVersion`;
- current `TableStatistics` when available and existing typed access paths,
  including their statistics and cost hints;
- `ColumnarProjectionId`, `ColumnarGeneration`, applied frontier, health, and
  immutable planner-cost evidence;
- `ChangeStreamGeneration`, origin/current/earliest frontiers, status, and
  payload-free batch cost summaries; and
- current transaction, structural-writer, and group-commit busy state.

Database-global visibility is required. This makes the logical evidence anchor
unambiguous. A derived Columnar advance does not publish user data and therefore
does not increment `DatabaseCommitSeq`. The stream/source frontier and
Columnar generation/frontier independently identify physical derived state.

Heap and LSM enter the same observation surface through their existing
`TableStorage` statistics, access-path snapshots, source version, and change
stream inspection. They remain authoritative source engines; the adaptive
layer does not modify their user rows.

## Advisor and planner boundary

`AdaptiveObservation::decide` is a pure function of an immutable observation,
an `AdaptivePolicy`, and a `MaintenanceBudget`. `AdaptiveDecision::NoAction`
is first-class and carries a typed reason. A proposal can only describe
`CatchUpExistingColumnar`; it is evidence-bound intent, not execution
authority.

The planner remains read-only and decides how to execute each query. Its
storage-neutral Columnar work formula remains the single source of truth in
`netbadb-planner`. The adaptive advisor calls the exported pure
`evaluate_columnar_projection_cost` primitive for the existing projection's
complete declared column set. There is no workload history or duplicate cost
formula in Core. The advisor decides whether maintaining derived state is
worthwhile; it never selects a query plan.

The Phase 1 policy has only two deterministic hysteresis thresholds:
`minimum_expected_benefit_work_units` gates a proposal and
`minimum_keep_benefit_work_units` gates activation after measurement.

## Change Stream and proposal preconditions

The Change Stream is evidence and an incremental feed, not a controller. It
never invokes Columnar maintenance. A catch-up proposal binds the exact source
identity and snapshot, source version, projection ID/generation/frontier, and
stream generation/current/earliest frontiers that justified it. The proposal
also contains its exact bounded batch count, change bytes, cost evidence, and
policy.

`Database::execute_adaptive_columnar` always observes again before mutation.
It rejects a changed global or schema anchor as `StaleObservation`; any other
proposal mismatch—including source, projection, stream, busy state, policy
evidence, or exact bounded action—becomes `PreconditionsChanged`. It never
updates and executes an old proposal silently. Rejection consumes no
maintenance budget and publishes no partial projection state.

## Budget and change

Phase 1 reuses `MaintenanceBudget` and its storage-neutral integer units:

- work units are admitted change batches;
- read bytes are encoded NBCL input bytes;
- write bytes are a checked conservative admission bound for the existing
  atomic NBCD publication; and
- actions bound the number of physical operations (exactly one in a proposal).

All proposal arithmetic is checked. The complete contiguous change range must
fit before mutation starts, and execution checks the supplied budget again
before revalidation. The actual `MaintenanceConsumption` records batches,
input bytes, produced delta bytes, and one action.

Change itself is not reimplemented: execution calls the production
`Database::advance_columnar_projection` path. Its existing synced temporary
files, immutable NBCD publication, manifest activation, checksums, and reopen
rules remain authoritative.

## Measurement and outcome

After a successful physical advance, Core observes again and returns a typed
`AdaptiveColumnarMeasurement` containing:

- projection generation, source version, lag, and planner evidence before the
  change, after the change, and after the outcome;
- logical commit and schema generations before and after;
- whether the source snapshot remained unchanged; and
- estimated and actually consumed work/read/write/action counters.

Execution success and optimization outcome are deliberately different.
`Kept` means the projection caught up and satisfies the measured planner
benefit threshold. `RevertedInsufficientMeasuredBenefit` means the advance is
valid but the generation is excluded from planner candidates. Abort and
inconclusive outcomes remain separately typed.

Phase 1 Revert is a bounded Core runtime suppression keyed by
`(ColumnarProjectionId, ColumnarGeneration)`. It never rolls back a database
commit, mutates Heap/LSM truth, deletes the logical projection definition, or
removes its durable files. The planner safely falls back to the authoritative
source. Suppression and the observation/proposal/report history are
intentionally non-durable in Phase 1; reopening revalidates the durable
projection normally and clears the runtime suppression.

## Recovery and compatibility

Adaptive Operations introduces no persistent format, recovery log, wire
message, Schema Spec, or Inspection JSON shape. The typed API and returned
cycle report are an opt-in runtime inspection surface, so Inspection JSON v7
and all older goldens stay byte-for-byte unchanged.

A kept advance relies wholly on the existing Columnar recovery theorem. After
close/open, the source, projection generation and frontier, checksums, and
planner eligibility are reconstructed from the existing catalogs and
Columnar manifest. No adaptive history is required for correctness.

## Explicit exclusions

Phase 1 does not implement workload learning, query fingerprints, latency
feedback, automatic physical design, generic adaptive-target traits, a rule
DSL, a policy database, or a general workflow engine. Additional adaptive
targets require a later concrete design after a second real use case exists.
