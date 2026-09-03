# Controlled backfill phase — Round 32

Round 32 implements the first controlled DDL–DML–DDL migration slice for one
managed runtime Single Heap. A transaction may compose a layout change such as
`ADD COLUMN`, execute ordinary reads and DML against the private staged Heap,
then accept only table/column renames and `SET/DROP NOT NULL` refinements.

The state machine is explicit: `Composing → BackfillOpen → Refining →
Finalized`. Data access after `Refining` returns
`MigrationDataAccessAfterRefinement`; layout-changing DDL, index/table-object
mutations, cross-table access, and indexed-column nullability return typed
`UnsupportedBackfillRefinement` errors. Failed `SET NOT NULL` validation scans
the transaction-visible staged rows and leaves the open phase unchanged.

Physical publication remains one base-to-stage copy. The staged Heap keeps its
StorageId, rows, RowIds, and B-tree pages. At finalization, the explicit
`retarget_private_schema` storage primitive updates only Heap v5 metadata and
flushes it before the owner envelope is atomically retargeted. The existing
coordinator path then prepares one final NBSC and publishes one winner; no S3
resource or SQL replay is introduced.

The mutation journal adds tags 31 and 32 for `StageResourceIntent` and
`FinalizationIntent`. The former is durable before staged resource creation and
contains only the provisional one-table identity and exact locators. The latter
is written only after Heap metadata and owner proof succeed. Recovery continues
to use exact staging/final paths: before a CORD decision the base remains the
winner and staged resources are discarded; after a durable CORD decision the
existing composition winner path publishes the final target.

Supported targets are runtime-created managed Single Heaps and one effective
physical target per transaction. LSM, partitioned, imported/bootstrap, indexed
nullability changes, post-DML layout changes, savepoints, defaults, type
conversion, resumable/online migration, and multi-target backfill remain out of
scope.

Native regression coverage includes staged read-your-writes, created-table
backfill, compatible refinement, rejected layout changes, post-refinement DML,
predecision crash cleanup, journal round-tripping, and metadata retarget
validation. PostgreSQL client matrices and fuzz expansion remain follow-up
work until exercised against the final server build.
