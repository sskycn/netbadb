# Core multi-ALTER schema transaction composition — Round 28

Round 28 implements atomic ALTER-only composition for runtime-created, single
Heap tables. A transaction may apply several of the six existing ALTER actions
to one or many tables. The parser and typed compiler still compile one statement
at a time; Core composes their canonical results.

## Transaction model

The production state is a typed `SchemaCompositionState`:

```text
None -> Composing -> SealingAndMaterializing -> Materialized
                    \-> SealedNoEffectiveChange
       \-> RollbackRequiredLogical / RollbackRequiredMaterialized
```

`SchemaTransactionPlan` owns the admitted writer, exact base NBSC, current
canonical overlay, ordered touched-table state, action evidence, durable ColumnId
reservation count, journal, and catalog path. The old singular `SchemaMutation`
continues to decode and finish CREATE, DROP, and pre-Round-28 rewrite histories;
new SQL ALTER execution uses the composition state.

The writer is acquired before the first action and held through commit, rollback,
or recovery cleanup. Existing process-local admission rules exclude other retained
transaction handles and ordinary database access, so every rewrite reads a stable
committed source. Cross-process concurrent opening remains unsupported.

## Logical composition and identities

Actions resolve sequentially against the current overlay. Table and column renames
therefore affect later name lookup, and a dropped column cannot be referenced by a
later action. Each touched table uses provisional version `base + 1`; every action
recomputes its canonical fingerprint without incrementing the provisional version
again. Prepared dependencies compare both fields, so neither a later action nor a
fingerprint cycle resurrects an older prepared statement.

ADD reserves and synchronizes a table-scoped ColumnId when that statement executes.
The reservation is permanent even if a later action drops the column or the
transaction rolls back. StorageIds are not reserved by logical ALTER execution.
They are assigned in ascending TableId order only when an effective final plan is
materialized, one per dirty table.

The initial bounds are 128 actions, 64 touched tables, 128 ColumnId reservations,
64 schema-created StorageIds, a 4 MiB aggregate intent record, the existing 16 MiB
NBSC bound, and the existing CORD participant bound. Candidate actions are fully
constructed and validated before replacing the overlay.

## Materialization and seal

Preparing or describing SQL is pure. The first executed user relational statement,
or COMMIT for pure DDL, seals the whole schema transaction and materializes every
effectively changed table. An internal SET NOT NULL validation scan is not a user
execution and does not seal. Later schema or index DDL returns the typed
`SchemaMutationAfterMaterialization` error (`TransactionState`, PostgreSQL
SQLSTATE `25000`).

Materialization freezes one final NBSC, allocates final StorageIds, synchronizes one
aggregate NBSJ intent before the first staged file, then creates one staged Heap per
dirty table. Each source is scanned once. Values are mapped by stable ColumnId;
new columns receive NULL, dropped columns are omitted, and unchanged physical types
are copied. The complete active index inventory is reinstalled with its logical
identity and allocator evidence before rows are inserted. No intermediate overlay
or Heap is a rewrite source.

If the final canonical schema equals the base, materialization is skipped. Schema
generation, NBSC epoch, table versions, runtime revision, StorageId floor, NBSC,
and CORD remain unchanged. Any synchronized ColumnId reservations still advance the
effective allocator floor through journal history.

## Durable commit and recovery

An effective composition advances SchemaGeneration and NBSC epoch once. Every dirty
TableId advances TableSchemaVersion once; touched tables whose final definition is
unchanged do not. Core prepares every staged Heap and one NBSC, and writes one CORD
v2 schema decision whose physical participants are canonically ordered by StorageId.

After that decision, completion is retry-only: finish every participant, promote
every exact target, open/recover all winners, durably retire every predecessor,
publish the one NBSC, append CORD Complete, append the composition winner, clean
prepared artifacts, then publish the in-memory registry/schema bundle. Winner
recovery uses only the typed aggregate plan, prepared participants, NBSC digest,
and CORD decision; it never reparses SQL or recopies source rows. Without a decision,
recovery removes only exact intent-derived staging paths and records a loser.

Composition predecessor records participate in the existing explicit replacement
Heap GC proof. Per-table GC intent/complete records preserve retry-only deletion,
coordinator horizon checks, owner/identity validation, and chained replacement or
later DROP lineage. No automatic GC was added.

## Persistent compatibility

NBSC remains v1, CORD remains v2, Heap/page/WAL/index formats are unchanged, and
NBSJ keeps its v1 envelope. NBSJ adds tags 16–23 for composition ColumnId
reservation, aggregate intent, per-table retirement, loser, no-effective-change,
winner, and per-table GC intent/complete. Tags 1–15 remain readable and their bytes
and meanings are unchanged. Downgrading a database after writing new tags is not
supported.

## Supported and deferred

The supported scope is ALTER-only composition over runtime-created Single Heaps,
including same-table and cross-table actions and post-final-ALTER DML. CREATE/DROP
TABLE mixing, CREATE/DROP INDEX mixing, multiple index DDL, savepoints, defaults or
backfill, physical conversion, constraints, LSM/range/imported storage, online
schema evolution, cross-process writers, automatic GC, and NBSJ/CORD compaction are
deferred. Alembic pure multi-ALTER transactions fit this boundary; an ALTER plus
index migration still fails and rolls back atomically.
