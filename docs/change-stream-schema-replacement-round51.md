# Active change streams and schema replacement (Round 51)

Round 51 is an architecture audit with test-only executable experiments. It
does not change production behavior, a persistent format, a public API, or a
wire contract. The selected Round 52 target is **Candidate A2: require an
explicit disable before a final physical replacement, followed by an explicit
S2 enable and committed-read anchor**.

Round 52 has now productionized that selection. The historical observations
below remain the decision evidence; current behavior is documented in
[change-stream-schema-replacement-round52](change-stream-schema-replacement-round52.md).

## Baselines and scope

The requested historical baselines are Round 50 `7fb849b8e79c357a032d2883a3de6b97bc88d891`
and its Phase 2A Change Stream child
`d7b19e8d9087fe86937b498847dd63a8b07a5916`. At the start of this audit,
however, the actual local and remote `main` were already
`e72e351da01e2c41dbd6a8caaf67ff3b36e8faa2`, the Phase 2B incremental
Columnar base/delta merge committed by the immediately preceding task and a
direct child of `d7b19e8`. The audit therefore treats `d7b19e8` as the Change
Stream/schema-replacement evidence baseline while integrating onto `e72e351`;
it does not discard the newer committed work.

The invariant under audit is:

```text
stream identity = (StorageId, ChangeStreamGeneration)
cursor position = (stream identity, storage-local StorageDataVersion)
```

`StorageVersionKey`, `SchemaFingerprint`, and every NBCL batch remain bound to
that exact storage. `DatabaseTxnId` correlates CORD participants but is not a
global commit sequence.

## Executable observation of current production behavior

The Round 51 Core tests enable an S1 stream and execute the complete Round 50
shape in one transaction: ordinary UPDATE/INSERT/DELETE, ADD Cnew, deferred
UPDATE of Cnew, Cnew SET NOT NULL, final index creation, and one S1-to-S2
rewrite.

Current behavior is:

- the final S1 participant prepares and, after the CORD winner decision,
  exposes exactly one committed NBCL batch;
- the batch carries the migration `DatabaseTxnId`, advances the S1 frontier
  exactly once, contains the three ordinary S1 row mutations, and retains the
  base S1 schema fingerprint;
- its INSERT/UPDATE after-images have the three base-S1 columns. Deferred Cnew
  values do not and cannot appear because Cnew is absent from the S1 schema;
- S2 becomes the TableId's current placement and starts with
  `ChangeStreamStatus::Disabled`; it is not automatically enabled;
- `read_changes(TableId, old_s1_cursor, ...)` resolves the TableId to S2 and
  returns `ChangeStreamError::Disabled`. This happens before cursor context is
  examined;
- the retired S1 `.change` and `.change.active` files remain present and are
  listed as `ChangeLog` and `ChangeStreamGuard` components in the replacement
  GC inventory;
- opening the retired S1 physical Heap directly can still read its final batch;
  ordinary table-level consumers have no API that reaches it;
- eligible replacement GC deletes both files. History is then physically
  unavailable as well as unreachable through the table API.

This is classified as **unsafe silent abandonment**, not an intended contract.
The old history is valid and committed, but the successful schema operation
neither requires explicit abandonment nor supplies a transition, discovery, or
retention contract.

## Crash and recovery evidence

With an enabled S1 stream and the same Round 50 transaction:

- a crash after all participant prepares but before durable CORD decision is a
  loser. Reopen keeps S1 authoritative, removes S2, exposes no prepared batch,
  and leaves the S1 frontier unchanged;
- a crash immediately after durable CORD decision is a winner. Reopen repairs
  participant completion and the S1 NBCL finalize marker, publishes S2, and
  retains the readable committed three-mutation batch on retired S1;
- repeated recovery does not synthesize stream events or replay the deferred
  transform.

The change log stays subordinate to its storage participant. It must not become
a new coordinator participant.

## Same-S1 controls

The audit separates schema activity from final physical truth.

| Final path | Current result with enabled S1 stream | Frontier result |
| --- | --- | --- |
| rename then rename-back | S1 retained, generation retained | ordinary DML only |
| ADD then DROP and SET then DROP NOT NULL | S1 retained, generation retained | ordinary DML only |
| CREATE then DROP index global no-op | S1 retained | ordinary DML only |
| Round 48 `InPlaceIndexDelta` | S1 retained, index published | ordinary DML only |
| ordinary CREATE/DROP INDEX, ANALYZE, VACUUM | S1 retained | no row change, no advance |
| surviving ADD/rename/nullability/drop | S2 replaces S1 | S2 Disabled; S1 abandoned |

This proves that a future guard must test the final `RewriteHeap` result, not
whether any ALTER or schema composition occurred. A1 (reject at first
refinement) would incorrectly reject honest no-op and same-S1 index-only work.

## Replacement path inventory

Executable tests cover these production families:

- pristine ordinary schema-first ADD, rename, effective SET NOT NULL, and DROP
  COLUMN;
- the Round 42 DROP-index-first `SourceBackfill` route;
- Round 46 effective nullability replacement and SET-then-DROP no-op;
- Round 48 table-dirty `RewriteHeap`, same-S1 `InPlaceIndexDelta`, and global
  no-op;
- Round 50 post-DML adoption, deferred backfill, and terminal index phase;
- the typed Core `rewrite_heap_table_schema_legacy_in` entry point.

All converge on the same durable `SchemaIndexTablePlan::RewriteHeap` truth.
Round 52 must centralize admission before the first rewrite reservation, target
StorageId allocation, tag-25 intent, stage file, or other physical mutation so
no SQL or direct-Core path can bypass it. `InPlaceIndexDelta` and
`SealedNoEffectiveChange` remain allowed.

Table DROP is intentionally separate. `DROP TABLE` explicitly destroys the
logical table and therefore explicitly abandons all subordinate resources,
including an active stream; it need not require a preceding stream-disable
operation. Existing retired-table GC inventory and tests prove that its NBCL
and guard are retained until eligible GC. Replacement is different because the
same logical TableId survives and silently appears to offer continuing use.

## Explicit rebaseline prototype

The bounded, honest workflow is:

```text
consume S1 as far as the administrator requires
disable_change_stream(T)          -- explicit abandonment of G1
perform and commit S1 -> S2 migration
enable_change_stream(T)           -- create an S2-local incarnation
anchor = committed_read_anchor(T) -- complete S2 snapshot + matching F0
read_changes(T, anchor.cursor, ...) for later S2 DML
```

The executable prototype proves that disable removes the active guard but
retains the inactive S1 change log in the retired manifest until GC. S2 remains
Disabled after migration. Explicit enable returns an S2 cursor whose baseline
and current frontier are equal; the tuple `(S2, G2, F0)` is a distinct context
from `(S1, G1, Fold)` even if the storage-local numeric generation or frontier
happens to match. The committed-read anchor returns that exact cursor, and the
next S2 UPDATE appears exactly once from `F0`, with only S2 version keys.

Old cursor behavior is status-sensitive because storage status is checked
before cursor identity:

| Cursor | Resolved target and status | Current result |
| --- | --- | --- |
| S1/G1/Fx | S1 Enabled before replacement | success |
| S1/G1/Fx | S2 Disabled after replacement | `Disabled` |
| S1/G1/Fx | S2 Enabled after explicit rebaseline | `ContextMismatch` |
| old generation | same storage, re-enabled | `StreamIdentityMismatch` |
| wrong table/storage | enabled different storage | `ContextMismatch` |
| S2/G2/F0 | S2 Enabled | success |

Frontier numbers are never ordered or compared across S1 and S2.

An active stream whose log is missing reopens as `Unavailable`. A schema-only
rewrite currently still succeeds and abandons it into a Disabled S2. Round 52
must treat both `Enabled` and `Unavailable` as blockers. Only `Disabled` is an
admissible replacement state; never-enabled storage is already Disabled and
must incur no new NBCL file or migration overhead.

## Candidate comparison

| Criterion | A2 explicit disable/rebaseline | B automatic S2 F0 | C durable transition | D cross-layout RowEntityId | E read retired stream |
| --- | --- | --- | --- | --- | --- |
| prevents silent abandonment | yes | only with discovery not present | yes | yes | only until GC |
| new persistent format | no | likely | yes | yes | likely metadata/pins |
| cross-storage row identity | no | no for baseline | no for snapshot+delta | required | no |
| old final S1 batch consumable | before explicit disable | no table-level path | yes with retention | yes | temporarily |
| new baseline discoverable | explicit enable + anchor | no current API | transition API | continuity API | unrelated |
| GC changes | no | possible | acknowledgement blocker | extensive | retention pin/ack |
| recovery changes | no | cross-resource ordering | transition repair | extensive | retired reopen |
| API changes | no | required for discovery | required | extensive | required |
| coordinator changes | no | tempting but forbidden | likely metadata coupling | likely | no direct change |
| preserves Round 50 one-S2 theorem | yes | maybe | maybe | uncertain | yes |
| implementation risk | low | medium/high | high | very high | high |
| ergonomics | explicit maintenance window | deceptively automatic | strongest rebaseline UX | strongest but unbounded | fragile/retention-bound |

Candidate B cannot prove how a consumer learns the new cursor or orders the old
final batch before the new snapshot. Enabling S2 before clone would also leak
internal copy inserts as user events; enabling after publish is merely an
undiscoverable rebaseline and adds crash ordering.

Candidate C is semantically possible without RowEntityId: consume S1 through a
declared final frontier, take a complete S2/F0 snapshot, then consume S2 deltas.
It nevertheless needs durable transition metadata, API changes, old-stream
retention, and consumer acknowledgement. Phase 2A has none of those.

Candidate D is true row-level continuity but requires a stable logical row
identity independent of Heap/LSM keys and StorageId. It directly exceeds the
Phase 2A boundary and is rejected for Round 52.

Candidate E requires retired-storage lookup, retained old schemas, a safe
read-only NBCL opener, acknowledgement or pins, and defined post-GC behavior.
Without consumer acknowledgement, GC is either unsafe or unbounded.

## Selected Round 52 target

Select **A2**, exactly:

1. After composition has determined a final `RewriteHeap`, but before tag 25,
   replacement StorageId allocation, stage creation, or physical mutation,
   inspect the source storage's `ChangeStreamStatus` through the storage API.
2. Proceed unchanged for `Disabled`.
3. Reject `Enabled` and `Unavailable` with a new small typed Core/schema error,
   recommended as `SchemaMutationError::ActiveChangeStreamBlocksReplacement`
   carrying the relevant table/storage/status context.
4. Classify it as `DatabaseErrorKind::FeatureNotSupported`; PostgreSQL will
   therefore map it through the existing transport-neutral path to SQLSTATE
   `0A000`. Do not add a PostgreSQL state machine or special branch.
5. The failed migration remains rollbackable under the existing transaction
   contract. Because disable requires a quiescent boundary, the documented
   recovery is `ROLLBACK`, disable S1, retry migration, enable S2, take anchor.

The eligibility probe must be pure: it cannot advance a frontier, write NBCL,
disable a stream, allocate S2, or journal an intent. Round 52 introduces no
automatic disable/enable, no in-transaction disable, and no false continuity.

## Format, recovery, GC, and Columnar impact

Round 51 changes none of NBCL v1, NBSJ v1 tags 1--35, NBSC/NBSM, CORD v2,
Heap/Page/WAL/transaction status, schema catalog, Protocol, SDK, inspection
JSON, or Columnar formats. No `RowEntityId`, transition/reset record, global
CSN, cursor field, or coordinator participant is added.

Candidate A2 likewise requires no recovery or GC format change. Existing
pre-CORD loser and post-CORD winner theorems remain intact. Existing retired
resource handling remains necessary for Disabled streams and older artifacts.

Columnar is a separate derived lifecycle. A stale S1 projection is not repaired
by a stream transition. A future incremental projection may use a complete
S2/F0 baseline plus later S2 deltas after explicit rebaseline; that motivation
does not require or authorize cross-storage row identity.

Known unrelated Round 50 boundaries remain unchanged: the old indexed SET NOT
NULL `0A000` fixture, imported/bootstrap DROP `0A000` fixture, and any current
Rust 1.85 planner let-chain blocker are not part of this audit.
