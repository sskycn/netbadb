# Late-clone row projection foundation — Round 41

Round 41 implements the Core-only layout-changing half of the existing
source-participant backfill lifecycle. It does not admit post-DML `ADD COLUMN`
or `DROP COLUMN` through native or PostgreSQL SQL.

## Source authority boundary

The new typed entry point can open only the same exact source authority used by
Round 39: one schema-writer transaction, one touched table, one managed Single
Heap, the current transaction-visible S1 as the sole write participant, no
target or staged resource, and one already-durable tag-25
`InPlaceIndexDelta`. Its final indexes must be a strict subset of its base
indexes. Cross-table, CREATE/DROP TABLE, LSM, range/partitioned, imported,
already-staged, or non-drop-only intent remains rejected.

The public SQL dispatcher still routes only rename and SET/DROP NOT NULL into
this state. Layout changes require `Database::apply_source_backfill_layout_refinement`
with an exact `AlterTableSpec` resolved against the current transaction overlay.

## Late tag-16 reservation theorem

NBSJ keeps the existing tag 16 and encoding order. A late
`CompositionColumnReservation` is accepted after tag 25 only when all of the
following are true:

- the transaction already owns exactly the drop-only source authority above;
- the table and transaction match that authority;
- the requested ColumnId equals the journal-derived authoritative high-water;
- `next_column_id` is exactly the checked successor;
- no legacy rewrite, table mutation, staging, finalization, resolution, source
  backfill evidence, table reservation, or index reservation conflicts;
- the `(TableId, ColumnId)` is globally unique.

The reservation is persisted before the logical ADD enters the overlay.
Rollback and every pre-decision crash therefore discard the column but retain
its allocator burn. Multiple ADDs repeat the same theorem independently. The
decoder cross-validates tag 16 against the table plan and target lineage; this
is stricter validation of existing bytes, not a new record format.

## ColumnId projection theorem

`RowProjection` is one Core-private checked plan shared by ordinary schema
rewrite and source-backfill late clone. Construction is O(source columns +
target columns): it hashes source positions by ColumnId and emits entries in
target schema order. Each projected row is O(target columns).

- a surviving ColumnId copies from its checked source ordinal;
- a dropped ColumnId has no target entry;
- a target-only ColumnId must have a durable tag-16 reservation and produces
  database `NULL`;
- names never confer identity, so `DROP C2 legacy; ADD C4 legacy` cannot copy
  C2 data into C4;
- surviving semantic and physical types must match exactly;
- source width and cached ordinal access are checked;
- target nullability is enforced while projecting.

The materializer constructs this projection once and consumes one frozen S1
transaction view. It does not scan once per ALTER and does not place schema
mutation policy in storage.

## ADD, DROP, rename, and nullability

The Core layout entry point accepts only nullable ADD and exact-ColumnId DROP.
ADD allocates no StorageId. DROP rejects a primary-key column or a column still
used by final active index truth. Existing public rename and SET/DROP NOT NULL
can then refine the same overlay. A final index may be created only on a column
that survived from the base table; an index on a newly added column remains
unsupported.

Policy B applies when a newly added nullable column is subsequently changed to
NOT NULL. Empty S1 succeeds. Non-empty S1 reaches the one projection and fails
with `NotNullViolation(Cnew)` before CORD, because every synthesized value is
NULL. Existing-column SET NOT NULL retains its earlier transaction-visible S1
validation.

## Identity, publication, and no-op

Any effective final layout preserves T and advances V once, regardless of the
number of refinements. The one prepared NBSC advances schema generation and
epoch once, and memory publication advances the runtime catalog revision once.
Exactly one final S2 StorageId is reserved and one source row stream populates
it. The three-row UPDATE/INSERT/DELETE fixture records one copy pass, three
copied rows, and one target StorageId.

For `ADD Cnew; DROP Cnew`, final `TableDef == base TableDef`. Finalization
returns to the already-materialized index transaction: no S2, tag 34, tag 35,
prepared schema catalog, or layout publication is created. Cnew remains burned.
The required drop-only index prelude can still publish its own index-only
change; tests distinguish that existing revision from any additional layout
revision.

## CORD and recovery

An effective layout reuses the Round 38 lifecycle unchanged: reserve and stage
one S2, project the frozen transaction-visible S1 once, build only supported
final indexes, prepare S1 and S2, and decide both participants with one CORD
record. The winner publishes S2 and immediately replacement-retires S1.

Round 41 tests crash after durable late reservation, target reservation,
mid-copy, and final-index construction. All are pre-CORD losers: three repeated
reopens restore the base table and rows while preserving the ColumnId burn.
The both-prepared, source-first, target-first, and both-committed CORD matrix
converges on the same S2 winner across three repeated reopens. Recovery never
reruns row projection after a durable CORD decision.

## Compatibility and deferred work

Canonical Schema, NBSJ v1 tags and ordering, NBSC, CORD, Heap row/page/WAL,
protocol, inspection, and SDK wire meanings are unchanged. Round 41 adds no
dependency, unsafe code, async core path, new tag, or new persistent field.

Deferred work includes public SQL admission, ADD followed by DML, ADD NOT NULL
syntax, defaults, generated columns, expressions, conversions/USING,
constraints, reorder, CASCADE, indexes on new columns, multi-table and non-Heap
sources, savepoints, online/resumable migration, participant detach, automatic
retired-Heap GC, and NBSJ/CORD compaction.
