# Adaptive Operations Phase 11: bounded four-lane service

Phase 11 adds one explicit opt-in service policy for the four existing
proactive Automatic Safe Mode lanes and a typed recommendation to renew
caller-owned evidence after a real layout-sensitive mutation. It adds no new
automatic action or mutation authority.

```text
safety/readiness
    -> cross-lane service
    -> lane-local ready age
    -> lane-local merit
    -> stable identity
    -> one existing mutation authority
```

The API remains synchronous and caller-driven. There is no thread, task,
timer, clock, background collector, or generic scheduler.

## Four proactive lanes

`AutomaticProactiveLane` deliberately excludes `None` and active-trial lanes.
Its fixed cycle is:

```text
ColumnarMaintenance
    -> ChangeStreamReclamation
    -> AuthoritativeMaintenance
    -> PlannerCalibration
    -> ColumnarMaintenance
```

The existing `StrictPhysicalPriority` default is unchanged. Its proactive
order remains Columnar, reclamation, authoritative LSM maintenance, then
calibration. `BoundedColumnarBurst` also retains its Phase 7-10 meaning: it
only mediates continuously ready Columnar and calibration work; reclamation
and authoritative maintenance do not increment or reset its counter.

`BoundedFourLaneCycle` is a separate opt-in policy. Its O(1) runtime state is
one `next_lane` enum, initially `ColumnarMaintenance`. Starting at that cursor,
selection scans at most four lanes in cycle order and chooses the first lane
that has a Ready candidate. Blocked and absent lanes are skipped and do not
consume an opportunity. If only one lane is Ready, it is selected on every
proactive admission rather than forcing idle rounds.

Safety is evaluated before service. The cursor cannot turn a retention,
quiescence, recovery, budget, evidence, or production-eligibility blocker into
Ready work. Candidate discovery order and vector insertion order do not confer
authority. After a lane is selected, its unchanged lane-local comparator
chooses one candidate by ready age, local merit, and stable identity. No float,
universal utility score, weighted queue, token bucket, or generic scheduling
trait is introduced.

## Bounded-service theorem

If a proactive lane stays enabled and Ready, `BoundedFourLaneCycle` offers it
service within at most four successful proactive admissions. An opportunity
means that the selected candidate is handed to its existing formal mutation
authority; it does not promise that revalidation or mutation succeeds.

The proof is finite: the cursor has four positions, scans the complete cycle,
and moves to the position immediately after the selected Ready lane. Every
admission therefore passes at most three other continuously Ready lanes before
reaching the subject lane. There is no integer age or wall clock in this
theorem.

The service opportunity is committed after one Ready candidate is selected
and before its formal authority revalidates current state. A stale, aborted,
inconclusive, or error result therefore consumes that opportunity and advances
the cursor. It cannot monopolize the cursor, and the same safe step never
falls through to a second lane. A completed reclamation or LSM action likewise
ends the step and creates no trial.

The following do not advance proactive service state:

- an active Columnar or calibration trial resolution, including keep, hold,
  await, revert, and stale;
- NoAction when no lane has Ready work;
- candidate inspection or any state query; and
- evidence recording or explicit evidence rotation.

An active trial remains the absolute attribution firewall: no proactive
discovery, ready-age reconciliation, service selection, cursor update, or new
mutation occurs until the trial resolves or is explicitly abandoned. A
Columnar or calibration admission advances the cursor before installing its
trial, then later trial-only steps leave that saved cursor unchanged.

## Policy-specific runtime state

The detached service report identifies the active policy kind, retains the
Phase 7 `consecutive_columnar_admissions` counter, and exposes the four-lane
`next_lane`. The counter and cursor are independent policy state; neither is
reinterpreted as the other.

Switching policy variants has deterministic semantics. Inspection evaluates a
different requested policy against its canonical virtual state without
mutating the Database. NoAction also leaves the actual state unchanged. The
next real proactive admission commits the new policy's canonical state and
then records that opportunity. Switching to four-lane therefore starts at
Columnar; switching to bounded burst starts with a zero counter. Reopen resets
all service, ready-age, and trial state to the normal runtime defaults.

## Layout-sensitive evidence renewal

Phase 3/4 evidence can remain safe but become less representative after the
physical or planning environment changes. Phase 11 reports this distinction as
an optional `AutomaticEvidenceRenewalRecommendation` with one typed reason:

- `ColumnarPhysicalStateChanged` after a real incremental catch-up or a real
  compaction publication, including a compaction whose post-publication gate
  suppresses the new generation;
- `ColumnarEligibilityChanged` after an exact-generation workload regression
  actually changes planner eligibility; or
- `AuthoritativeLsmLayoutChanged` after a completed flush or `compact_one`.

The recommendation is derived from the reported real mutation, not candidate
intent. A selected proposal that becomes stale or aborts before mutation has
no layout-change recommendation. Change Stream GC only reclaims history and
does not request layout renewal. Planner calibration apply/revert already
creates a `PlannerCalibrationEpoch` cohort boundary and also does not request
full-window renewal. Trial keep, hold, await, and stale outcomes do not change
layout or eligibility and produce no recommendation.

A recommendation is evidence-quality information, not a safety blocker,
mutation capability, or command to discard evidence. `automatic_safe_step_multi`
continues to take `&AdaptiveEvidencePool`; Database controls mutation
attribution state while the caller controls evidence lifetime. Safe Mode never
calls `rotate_window`.

The recommended operator workflow after a reported shape change is to call the
existing explicit `AdaptiveEvidencePool::rotate_window`, then collect fresh
post-change feedback. Rotation clears current aggregation while preserving the
same-schema G high-water, current storage/generation identities, retired-target
guards, and calibration floor. An active Columnar trial then waits for fresh
evidence. Ignoring the recommendation may reduce representativeness but grants
no additional authority: proposal revalidation, schema/epoch/generation
identity, retention, quiescence, and storage invariants remain intact.

Automatic reports cannot observe a later manual flush, `compact_one`, or
Columnar compaction. Operators that want only post-rewrite evidence should
explicitly rotate after those manual shape changes as well. There is no hidden
hook in manual maintenance and no historical evidence ring.

## Phase 3E and compatibility

Phase 3E staged participant Prepare remains visible to the existing subsystem
theorems. Unresolved prepared Change Stream work blocks GC, while an active
group barrier and staged storage work keep LSM and Columnar structural
maintenance non-quiescent. Four-lane service only sees the resulting Ready or
blocked candidate; it does not swallow recovery/corruption errors or bypass a
production guard.

The one-mutation theorem is unchanged: one selected candidate reaches at most
one existing Columnar, Change Stream, LSM, or calibration authority and the
safe step returns. LSM and GC still have no trial. Columnar and calibration
still share the one global trial slot.

All Phase 11 service and recommendation values are runtime-only embedded API
state. Canonical Schema, Heap, BTree, Columnar formats, Change Stream v2,
coordinator, LSM Manifest v2, LSM WAL v1, LSM SSTable v2, protocol v2, Schema
Spec v2, and Inspection JSON v7 are unchanged. Automatic physical design,
schema mutation, repartitioning, `compact_full`, heap maintenance, persistent
layout epochs, automatic evidence deletion, and background scheduling remain
out of scope.
