# Post-DML source adoption architecture audit (Round 43)

Round 43 is an architecture audit and a `#[cfg(test)]` executable experiment.
It does not expose generic ALTER-after-DML. Production native SQL and
PostgreSQL continue to reject `UPDATE` followed by ADD, DROP, RENAME, or
SET/DROP NOT NULL unless the exact Round 39/42 DROP-first authority already
exists.

## Current blocker

Ordinary row DML lazily registers the current physical storage as a write
participant. The SQL execution path registers it even when an UPDATE or DELETE
matches zero rows. It does not create `schema_mutation` or
`schema_composition` state.

On the following ALTER, `try_apply_source_backfill_refinement` accepts only an
existing `MaterializedIndex` created by the DROP-index-first path. With no such
state it returns false. `ensure_composition_started` then sees a non-pristine
transaction because the S1 participant already exists and returns
`TransactionNotPristine` (`25000` over PostgreSQL).

Round 42 works because pristine DROP INDEX first acquires the schema writer,
captures exact table/index/placement truth, persists a strict drop-only
`InPlaceIndexDelta`, and leaves `MaterializedIndex`. Later DML reuses the same
S1 participant, so the next bounded ALTER can enter `SourceBackfillOpen`.

These are different classes of condition:

| Class | Required facts |
| --- | --- |
| Correctness | active owner, exact T/V/F and S1 placement, one admitted table, exact physical transaction, unambiguous row-source provenance, stable schema |
| Durable recovery | tag16 allocator history before overlay mutation; real rewrite intent, tag35, stage intent, tag34 and prepared NBSC before CORD when the final layout is effective |
| Existing scaffolding | DROP-only tag25, `MaterializedIndex`, and `MaterializedSchemaIndexTransaction` before the first refinement |

The first two classes are required. The third is historical coupling, not
source authority in its own right.

## Schema stability and writer exclusivity

Every production schema publication reaches the same writer gate in
`ensure_composition_started`. It covers composed CREATE/DROP TABLE, ALTER TABLE,
and composed CREATE/DROP INDEX. The three retained legacy test-only
create/drop/rewrite entry points repeat the same condition. No other production
path sets `schema_writer`.

Writer acquisition requires both:

```text
schema_writer == None
Rc::strong_count(transaction_owner) == 2
```

The two owners are `Database` and the transaction requesting publication. A
second active transaction makes the count three. Autocommit schema operations
also create a transaction handle, so they cannot bypass the count. All other
transactions are rejected by `ensure_schema_available` while a writer is held.

Therefore:

> While an ordinary DML transaction handle remains active, another transaction
> cannot commit a schema publication. Schema writer admission requires exclusive
> transaction ownership. The schema seen by the DML transaction is stable.

The DML transaction itself may acquire the writer late when it is the only
handle. The Round 44 admission order must be:

1. validate active state, database ownership, operation class, empty schema and
   pending-index state, and the strict participant predicate;
2. validate exact committed T/V/F, placement, managed locator, registry table,
   storage kind, index digest, S1 and physical TxnId;
3. verify the writer is free and transaction-owner count is exactly two;
4. build/capture the private source state;
5. only then set `schema_writer` and accept the refinement.

Contention and ordinary eligibility failures must occur before writer or NBSJ
mutation. Failure after acquisition must transition to rollback-required or
fully release the writer and leave the original data transaction usable. The
executable contention probe verifies no writer leak, no journal byte change,
and an active original transaction after rejection.

## Write-participant provenance inventory

All Core call sites that can populate `write_participants` while both schema
states are empty are:

| Path | `write_context` | Pending index state | Row mutation | Future source? |
| --- | --- | --- | --- | --- |
| SQL INSERT | yes, before/apply mutation | none | yes | yes |
| SQL UPDATE | yes before execution, including zero rows | none | maybe zero or more | yes |
| SQL DELETE | yes before execution, including zero rows | none | maybe zero or more | yes |
| typed `insert_in` / `insert_into_in` | yes | none | yes | yes |
| transactional CREATE INDEX fallback | yes | `pending_indexes` | index pages/catalog | no |
| transactional DROP INDEX fallback | yes | `pending_index_drops` | index catalog | no |
| composed/legacy schema operations | yes when materialized | schema state present | staged/index participant | no |

ANALYZE, VACUUM, checkpoint, index compaction, orphan adoption and reclaim have
no transaction-scoped variant. They either use implicit storage work or require
all transaction handles to be absent. There is no other direct embedded
transactional row-mutation entry point.

Consequently Option P1 is sufficient today:

```text
one participant S1
+ one write participant S1
+ no pending index create/drop
+ no schema state
+ exact managed Single Heap placement
= row-data source provenance
```

Option P2 (`row_write_tables` or a durable adoption-evidence field) adds no
current safety information. Round 44 should not add it. Any future
transaction-scoped maintenance or non-row writer must either gain a distinct
state marker or force this conclusion to be revisited.

Zero affected rows still qualifies. The physical write participant and exact
identity are authority; affected-row count is not. INSERT-, UPDATE-, DELETE-
and mixed-DML-first flows are equivalent when they remain on the same S1.

## Source identity and participant policy

The proposed source theorem is:

```text
Active DatabaseTxnId D
exact committed TableId T / TableVersion V / fingerprint F
current placement Single(T, S1)
managed canonical locator for S1
registry S1 is Heap and owns exact table T/F
transaction participants == {S1}
transaction write participants == {S1}
physical TxnId P1 exists for S1
current index inventory and digest are captured
schema writer is free and D is the exclusive transaction handle
```

Round 44 should use the strict policy: no cross-table read or write participant.
This keeps recovery, isolation, prepared dependencies and commit ordering under
the existing one-table theorem. A read of the same S1 before UPDATE is fine.
Although a relaxed policy could permit read-only B plus written A, it provides
little first-version value and enlarges the proof surface. `UPDATE A; UPDATE B;
ALTER A`, `SELECT B; UPDATE A; ALTER A`, and `UPDATE A; ALTER B` are all rejected
by the executable predicate.

## Existing indexes

Ordinary DML already maintains every S1 index transactionally. Adoption does
not need to drop or mutate them. Capture the active inventory at adoption and
build the final inventory once on S2. S1 remains transaction-correct until the
CORD winner retires it.

An index follows a renamed column by stable ColumnId. DROP of an indexed column
still fails until its exact index has been explicitly dropped. Indexed
nullability rules and the prohibition on indexing a new late column remain
unchanged.

## Candidate comparison

| Candidate | Experiment/result | Format impact | Decision |
| --- | --- | --- | --- |
| A: identity tag25 | Existing encoder/validator rejects `base_indexes == final_indexes` as an invalid in-place delta before journal mutation. `NoEffectiveChange` also rejects any physical intent. Making it work would weaken two honest invariants or invent fake publication handling. | nominally none, but requires semantic weakening | reject |
| B: adopt S1, no pre-intent | Captures exact DML S1/P1, reserves tag16 before intent, handles rollback/no-op, creates a real rewrite intent only for an effective final layout, scans the transaction-visible source once, and reuses current CORD/recovery. | none | choose |
| C: SourceAdoptionIntent | Explicit but duplicates identities already available in the active transaction and later tag35. Adds a new NBSJ tag, decoder/cross-validation/fuzz surface and old-reader incompatibility without closing a demonstrated recovery gap. | NBSJ change | reject |

Candidate D allocates S2 too early, lengthens its lifetime and destroys the
clean no-op. Candidate E performs a hidden user-unrequested index mutation.
Candidate F introduces S1→S2→S3 amplification. All are rejected.

The pre-refinement `MaterializedSchemaIndexTransaction` carrier is accidental
coupling to Candidate A. Round 44 should add a Core-private
`SourceBackfillTransaction` (or equivalently precise source-open state) holding
the logical plan and exact S1/P1/index evidence. Once an effective final truth
is frozen, the existing materialized carrier can be used unchanged. No public
API is needed.

## Chosen Round 44 activation predicate

At the first eligible ALTER Execute, require every item below before acquiring
the writer:

```text
transaction.state == Active
schema_mutation == None
schema_composition == None
pending CREATE INDEX == empty
pending DROP INDEX == empty
participants == {S1}
write_participants == {S1}
row provenance follows the audited P1 call-site theorem
prepared ALTER dependency == current committed T/V/F
placement(T) == Single(S1)
descriptor kind == Heap
descriptor locator == canonical managed locator
registry S1 table == exact T/F
physical TxnId P1 exists
schema_writer == None
transaction owner strong count == 2
operation is in the bounded Round 44 set
```

Acquire and hold the writer at first ALTER Execute, never at DML, Parse, Bind or
Describe. Hold it through commit, rollback or recovery-required terminal
handling. Names are never identity authority.

The first Round 44 SQL surface should be only:

```text
ADD nullable column
DROP unindexed non-PK column
RENAME TABLE
RENAME COLUMN
```

SET/DROP NOT NULL remains deferred for this new activation path even when an
unindexed survivor could theoretically satisfy existing checks. Also deferred:
public new-column NOT NULL, indexes on new columns, defaults, conversions,
constraints, multi-table/partitioned/LSM/imported sources and further DML after
the first refinement.

Prepared ALTER retains its exact compile-time T/V/F and exact DROP ColumnId.
Preparation before or inside the transaction remains pure. Because DML does not
change canonical schema, Execute can use the dependency unchanged; no name
re-resolution or same-name rebinding is allowed.

## Allocator, no-op and effective finalization

Candidate B uses ordinary composition tag16 before changing the overlay. tag16
already supports reservation without an aggregate intent and is decoded on
reopen to preserve the ColumnId high-water. Rollback and pre-CORD crash burn
the accepted ID. Multiple ADDs create ordered tag16 records. No tag25 and no new
tag are needed before finalization.

For ADD→DROP canonical no-op after DML:

```text
DML commits on S1
ColumnId remains burned
no tag25, tag35, stage intent, tag34 or prepared NBSC
no S2
T/V/G/NBSC epoch/runtime revision unchanged
placement remains S1
```

For an effective final layout, finalization creates (rather than replaces) the
existing schema/index rewrite intent, followed by the existing tag35
`SourceBackfillIntent`, stage intent, S2, tag34 finalization evidence, prepared
NBSC and CORD. The experiment preserves surviving base indexes, observes own
UPDATE/INSERT and excludes DELETE, copies three rows in one source pass, and
allocates exactly S2 (`StorageId(3)`) with no S3.

The SourceBackfillIntent meaning is unchanged. It remains the durable bridge
between the real final RewriteHeap plan and exact S1/P1→S2 projection. It does
not become an adoption record and is absent for no-op.

## Crash and recovery theorem

| Crash point | Candidate B result |
| --- | --- |
| after DML, before adoption | ordinary physical transaction loser |
| after eligibility/capture, before writer evidence | no schema publication; rollback/release |
| after tag16 | DML loser; ColumnId burned |
| after logical refinement, before final intent/S2 | base schema wins; burn retained |
| after first real rewrite intent, before tag35 | base wins; composition loser cleanup |
| after tag35 or stage intent | base wins; staged resource cleanup |
| during projected S2 / S2 complete before CORD | base wins; S2 removed; projection is not replayed |
| after CORD, either participant order | existing winner completion publishes S2 and retires S1 |

Executable child-process tests cover the first real rewrite intent, tag35,
stage intent and mid-copy pre-CORD points, plus both-prepared, source-first,
target-first and both-committed post-CORD states. Each result survives three
reopens. Recovery consumes only durable physical/schema evidence. It never
needs SQL, parser, HIR, prepared statements, DML/ALTER replay, or a second
RowProjection pass.

## Fixed-fixture physical observation

One local three-row Heap fixture was measured. These are observations, not
universal performance promises:

| Flow | bytes after S1 DML | observed pre-commit peak | bytes after commit | S2 | passes/rows |
| --- | ---: | ---: | ---: | --- | --- |
| Round 42 DROP/recreate-index prelude | 143,008 | 248,492 | 249,017 | 3 | 1 / 3 |
| Candidate B adoption | 142,793 | 248,399 | 248,924 | 3 | 1 / 3 |

Both flows retain one S1→S2 stream and no S3. The small byte difference is
fixture-specific index/WAL overhead; it is not a general speed claim.

## Persistent and protocol impact

Candidate B changes none of Canonical Schema, NBSC, NBSM, NBSJ tags, CORD,
Heap/Page, WAL, IndexCatalog, BTree, transaction status, Protocol, PostgreSQL
framing, Manifest or SDK Schema Spec. Candidate A would misuse existing tag25;
Candidate C would require a new NBSJ tag and is therefore rejected.

The Round 43 prototype is compiled only for Core tests. Its small shared
materialization dispatch refactor is behavior-neutral and introduces no public
API, SQLSTATE, persistent byte or wire change.

## Exact Round 44 implementation recommendation

Implement Candidate B in `crates/netbadb-core/src/schema_composition.rs`:

- introduce the private source carrier and strict adoption predicate;
- acquire the writer only after complete preflight;
- route the four bounded ALTER operations at Execute;
- use ordinary tag16 reservations;
- create the real rewrite intent only at effective finalization;
- retain current source projection, tag35/tag34, CORD and recovery paths;
- close relational execution after the first accepted refinement.

Add narrow participant-query helpers in `transaction.rs` only if needed to make
the strict `{S1}` predicate explicit. Do not add row provenance state. Extend
Core and PostgreSQL tests, but no server production change is expected.

Round 44 must still reject cross-table activity, non-Single/Heap placement,
indexed or PK DROP, new-column indexes/NOT NULL, defaults/conversions/
constraints, further DML, and every operation outside the bounded set.
