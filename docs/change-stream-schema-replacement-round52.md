# Explicit Change Stream rebaseline guard for Heap replacement (Round 52)

Round 52 productionizes Candidate A2 from the Round 51 audit. NetbaDB Change
Streams are histories of one exact physical storage incarnation:

The historical requested Round 51 baseline was `8c42ff06`, with Round 50
`7fb849b8`, Phase 2A `d7b19e8d`, and Phase 2B `e72e351d` retained. Actual Round
52 work started from `837801e1`, the newer `origin/main` that also contains
Columnar Phase 2C; no earlier history was reset or rebased away.

```text
stream identity = (StorageId, ChangeStreamGeneration)
cursor position = (stream identity, storage-local StorageDataVersion)
```

Replacing authoritative Heap S1 with S2 preserves the logical `TableId`, but
does not preserve that physical identity. A final `RewriteHeap` is therefore
refused while S1's stream is `Enabled` or `Unavailable`. Only `Disabled` is
admissible; a never-enabled stream already has that status.

## Admission rule and timing

Composition first computes the final canonical result. A private pure helper
selects only touched tables for which a final `TableDef` still exists and is
different from the base. Creates, table drops, and canonical no-ops are absent.
The centralized guard then calls `inspect_change_stream()` on each exact old
`StorageId`:

| Final physical truth | Source status | Result |
| --- | --- | --- |
| no replacement | any | existing same-S1 behavior |
| `RewriteHeap` | `Disabled` | proceed |
| `RewriteHeap` | `Enabled` | reject |
| `RewriteHeap` | `Unavailable` | reject |

Rejection is
`SchemaMutationError::ActiveChangeStreamBlocksReplacement { table_id,
storage_id, status }`. It is `DatabaseErrorKind::FeatureNotSupported`, so
PostgreSQL uses the existing mapping to SQLSTATE `0A000`; the adapter has no
migration-specific branch.

The check runs after final no-op/index classification and composition limits,
but before any replacement `StorageId`, plan, or durable state. A blocked
replacement writes no aggregate rewrite intent (NBSJ tag 25), source-backfill
intent (tag 35), stage intent, prepared NBSC, target owner, Heap, WAL,
transaction-status file, change log, or CORD target participant. The
`StorageId` high-water is unchanged.

Earlier accepted logical `ColumnId` or `IndexId` reservations remain durable
allocator history. In the Round 42 DROP-first lifecycle, an already valid
same-S1 index prelude/tag-25 intent may also exist; the guard does not erase or
replace it. It prevents the later rewrite aggregate and target. Explicit
rollback resolves that prelude through its normal loser path.

Round 54 late-column reads do not weaken this admission point. Deferred
VirtualRow UPDATEs may be accepted while S1 is Enabled or Unavailable because
they do not mutate S1 or append NBCL; final RewriteHeap still fails before S2
allocation, and rollback leaves the stream frontier unchanged.

The guard covers all current managed-Heap replacement producers:

- ordinary schema composition (`CompositionTablePlan`);
- table-object composition (`CreateHeap`, `DropHeap`, `RewriteHeap`, and
  `InPlaceIndexDelta` classification);
- schema/index composition, including adopted-source and deferred-backfill
  finalization;
- DROP-first `SourceBackfill` finalization, before its state is removed from
  the transaction;
- the typed Core rewrite API, which flows through those production paths;
- the test-only legacy direct Heap rewrite path.

The early DROP-first check preserves the complete source-backfill state on
error. Other composition failures retain their logical program in the existing
rollback-required state. No guard failure creates a recovery-only state;
explicit rollback restores S1, its base schema, and its physical indexes.

## Same-storage and table lifecycle operations

The guard is based on final replacement truth, not the occurrence of ALTER.
An enabled stream continues to allow `SealedNoEffectiveChange` (including
rename-back, ADD-then-DROP, and nullability round trips), Round 48
`InPlaceIndexDelta`, CREATE-then-DROP index global no-op, ordinary CREATE/DROP
INDEX, ANALYZE, VACUUM, and ordinary DML combined with those same-S1 outcomes.

Schema and index metadata do not create row batches. A transaction with a
nonempty net row change advances the S1 frontier exactly once and retains the
same generation. Pure index/catalog maintenance leaves it unchanged.

`CREATE TABLE` has no source to replace and is unaffected. `DROP TABLE` is
explicit destruction of the logical `TableId`, not replacement. It may retire
an active stream; `.change` and `.change.active` remain in the retired
inventory until normal eligible GC deletes them.

## Explicit administrator rebaseline

The supported workflow is:

```text
consume S1 through the administrator's chosen boundary
disable_change_stream(T)
perform and commit the S1 -> S2 migration
enable_change_stream(T)
anchor = committed_read_anchor(T)
consume later S2 changes from anchor.cursor
```

Disable requires the existing quiescent storage boundary. It is not available
inside the active migration transaction. After a blocked finalization, the
caller rolls back, disables S1, and starts a new migration transaction.

Ordinary DML during the disabled migration window intentionally produces no S1
NBCL batch. The complete S2 baseline represents that window. Migration does
not auto-enable S2. Explicit enable creates an S2-local generation and F0; S1
and S2 frontiers are never compared. The committed-read anchor pins one
complete S2 view and returns its cursor. A later S2 update yields one F0-to-F1
batch whose version keys all name S2.

Before S2 enablement, an old S1 cursor retains the status-first `Disabled`
result. After S2 enablement it returns `ContextMismatch`. There is no cursor
translation. An `Unavailable` S1 is also blocked; the existing explicit disable
operation may abandon it before a new migration attempt.

## Incremental Columnar integration

An incremental projection remains bound to its source `StorageId`, stream
generation, schema fingerprint, Base frontier, and applied frontier. A blocked
rewrite leaves S1 and that projection unchanged; it is never retargeted to S2.

Explicit S1 disable makes the old projection `RebuildRequired`. A later S2
winner does not make it Fresh or advance it from old S1 NBCL. After enabling
S2, callers explicitly build a new projection from an S2 committed-read
anchor. Later S2 DML makes it `Lagging`; bounded advance consumes S2 NBCL and
returns it to `Fresh`. Snapshot projections retain their existing
stale/unavailable authoritative fallback.

## Compatibility boundary

Round 52 adds admission logic only. It changes none of Canonical Schema,
NBSC/NBSM, NBSJ v1 tags 1--35, CORD v2, Heap/Page, Heap WAL, transaction status,
NBCL or cursor layout, NBCS/NBCM/NBCD/NBPC, IndexCatalog v9, BTree v3,
PartitionCatalog, LSM formats, Protocol v1, PostgreSQL framing v3, Manifest v4,
SDK Schema Spec, generated SDKs, or inspection JSON.

There is no new recovery branch, transition/reset record, consumer
acknowledgement, retention pin, `RowEntityId`, global frontier, cross-storage
`DatabaseTxnId` ordering, CORD participant, PostgreSQL stream-management SQL,
automatic S2 enablement, or automatic Columnar rebuild.
