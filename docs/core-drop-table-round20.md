# Core transactional Heap DROP — Round 20

Round 20 adds frontend-neutral logical retirement for one exact active,
non-partitioned Heap table. It does not add SQL `DROP TABLE`, unlink a table
resource, or garbage-collect retired storage. NBSC remains the only active logical
schema authority; retained NBSJ history explains why an unreachable Heap still
exists.

## Baseline lifecycle audit (62a8727)

The implementation was audited before adding the DROP API.

1. `CommittedCatalogState` is owned by one synchronous `Database` and contains
   Canonical Schema, generation, table lineage and allocator high-waters. Runtime
   schema, `PhysicalBindings`, and `StorageRegistry` are private fields. Publication
   invokes no callbacks, so one method can replace all three without an observable
   mixed interval.
2. Round 18's transaction view was a materialized target NBSC used only while its
   staged Heap existed. The same target snapshot can represent removal; lookup is
   now `CommittedSchema + added - dropped` without a global pre-commit mutation.
3. `PreparedStatement` contains compiled logical IR, exact
   TableId/TableSchemaVersion/SchemaFingerprint dependencies, and optionally a weak
   transaction-scope token. It contains no `TableStorage`, page, row, binding, or
   filesystem handle. Dependency validation happens before planning/storage lookup.
4. `StorageRegistry` and `PhysicalBindings` are Core-private vectors. No API returns
   a `TableStorage` reference. `DatabaseReadView` contains engine read snapshots but
   can only be created internally; retained database transactions are counted by
   the schema-writer admission token. DROP is therefore rejected while any other
   transaction handle could retain a storage transaction/read view.
5. A `TableStorage` value can independently exist when an application constructs
   storage directly, but a handle cannot escape *from* a `Database`. Core DROP owns
   and closes its removed registry value only after durable publication. It never
   invalidates unrelated independently opened storage objects.
6. Physical participants are enlisted lazily by persistent StorageId. DROP itself
   does not write the target Heap and creates no empty physical transaction. A
   schema-only DROP uses a CORD v2 schema decision with zero physical participants.
   DML performed before DROP keeps its ordinary participant and finishes 2PC before
   logical retirement is published. DML after DROP cannot compile against the view.
7. CoordinatorLog is append-only and retains historical decisions. `Complete`
   follows participant completion and schema publication. Completed historical
   participants need not remain in the *active* registry; unresolved decisions still
   require every exact participant. Retired-resource validation separately requires
   the retained Heap/WAL/status resource.
8. SchemaMutationJournal history is bounded, retained, never compacted and already
   required to explain identity reservations and recovery obligations. Resolved
   records are not discarded. It can therefore safely be the P0 physical-retirement
   inventory; a second ledger would duplicate durability/order without adding a
   retention horizon.
9. Final runtime-created locators are derived from incarnation and StorageId:
   `<catalog>.resources-<incarnation>/storage/<StorageId>.heap`. Imported/bootstrap
   locators are preserved from NBSC. A name never participates in either locator.
   CREATE high-waters ensure same-name recreation receives new TableId/StorageId and
   a different generated locator.
10. Recovery previously resolved CREATE obligations before strict NBSC/state load,
    promoted exact staged components, recovered all participants, and published the
    winner. DROP extends this layer; ordinary catalog open subsequently materializes
    only storages named by the final active NBSC.
11. Manifest schemas are expectations and grants are external TableId-based policy.
    DROP rewrites neither. A stale required-table expectation fails after retirement,
    and a recreated name has a new TableId that cannot inherit the old grant.

## Exact target and private visibility

`Database::resolve_drop_table(name)` is a side-effect-free convenience that returns
`DropTableTarget { table_id, table_version, fingerprint }`.
`Database::drop_table_in(transaction, target)` validates those fields against both
the in-memory committed bundle and current NBSC, then binds the exact Single/Heap
placement, StorageId, engine, relative locator, and physical identity. Missing IDs
return `TableNotFound`; a surviving ID with changed version/fingerprint returns
`StaleSchemaDependency`. No name is retained or re-resolved.

The transaction target snapshot is the complete committed snapshot with exactly
that TableId, lineage, placement and storage descriptor removed. Generation and
epoch use checked `+1`; all allocator high-waters are copied byte-for-byte. The
global schema/inspection remains old until commit. Preparation in the dropping
transaction sees the target snapshot, so a new statement naming the table fails.
An older prepared statement's TableId lookup also fails dependency validation before
storage access. Statements depending only on unchanged tables still compare equal.

One table schema mutation per transaction remains the boundary. CREATE+DROP,
DROP+CREATE, a second DROP, and table/index DDL mixing are rejected. Single LSM and
range-partitioned logical tables return `UnsupportedPlacement` before journal,
generation, high-water, writer, or snapshot mutation.

## Durable DROP records and retirement authority

NBSJ v1 retains its envelope/version and backward-decodes all Round 18 records. New
record tags encode:

- `DropTableIntent`: transaction, target generation/epoch, prepared NBSC SHA-256,
  and a one-table NBSC v1 fragment containing final TableDef, lineage/version,
  fingerprint, Single/Heap placement, StorageId, locator, base generation/epoch,
  high-waters, incarnation and coordinator locator;
- `Retained`: terminal physical-lifecycle evidence written only after the durable
  coordinator COMMIT decision and exact retained-resource validation;
- DROP loser/winner resolution. A winner is illegal before `Retained`; a loser is
  illegal after it.

The one-table fragment is recovery/retention evidence, not active schema. It is
never consulted by compiler lookup, `schema()`, `inspect_catalog`, PG reflection or
SDK metadata. `inspect_retired_table_resources()` exposes a separate read-only DTO
with table/version/fingerprint, StorageId, Heap engine, relative locator and retired
SchemaGeneration.

Replay rejects overlap, unknown/duplicate/out-of-order retirement, winner without
retirement, retired loser, duplicate StorageId retirement, mismatched table/storage/
placement/fingerprint, a later CREATE reservation below the preserved allocator
floors, invalid generation/epoch, unsupported engine, truncation and CRC errors.
After recovery, active NBSC StorageIds must be disjoint from committed retired
StorageIds. Every retained Heap's main file, WAL/status recovery metadata, TableId,
StorageId and fingerprint are validated on each open. Absence is a hard storage/
recovery error; no empty replacement is created.

NBSJ is intentionally still append/history-retaining. Physical deletion and a safe
coordinator/handle retention horizon remain future work.

## Commit, rollback and recovery

Live commit order is:

1. validate exact logical/physical target and exclusive schema admission;
2. preflight complete journal capacity and synchronize `DropTableIntent`;
3. expose the private removal overlay;
4. prepare existing physical writers, if any;
5. write and synchronize prepared NBSC G+1;
6. synchronize the CORD v2 schema COMMIT decision (zero physical tuples is valid);
7. commit existing physical participants;
8. validate the retained Heap and synchronize NBSJ `Retained`;
9. publish NBSC then NBSM excluding the table;
10. synchronize Coordinator `Complete`, resolve the DROP winner, and remove the
    prepared snapshot;
11. remove registry storage and binding, replace committed state, and checked-increment
    runtime revision in one synchronous in-memory publication; release the writer.

After the durable decision, every error remains `FinalizePending`/retry-only. No path
restores the table as a final outcome. Before a decision, rollback first rolls back
ordinary participants, removes only the prepared snapshot, records the DROP loser,
discards the overlay and releases admission. It does not touch the active Heap,
indexes, data, table version, fingerprint, StorageId, schema generation, runtime
revision or allocator high-waters.

Startup opens NBSM, NBSJ and Coordinator before strict active catalog publication.
A DROP loser preserves the old NBSC/resource and removes prepared metadata. A winner
validates the digest and exact retained resource, recovers any DML participants using
a temporary recovery-only snapshot that includes the retiring Heap, writes missing
`Retained`, publishes the prepared target, completes Coordinator, resolves the
journal and only then opens the active NBSC storages. The recovery-only Heap never
enters active inspection. Repeated recovery is generation/ID/record idempotent.

## Concrete regression evidence

The primary fixture starts at generation 1 with users `(TableId=1, StorageId=1)`
and teams `(2,2)`. Committed DROP users produces generation 2/runtime revision +1,
keeps both next-ID floors at 3, retains the exact old Heap and hides its index. A
new users receives `(3,3)`, version 1, an empty row set and no indexes; the old Heap
still contains its old row and BTree. Dropping highest identity `(2,2)` followed by
CREATE also allocates `(3,3)`, proving floors are not recalculated from active IDs.

Tests cover rollback equality, exact stale/missing targets, LSM/range rejection,
same-transaction and global prepared invalidation, unrelated prepared survival,
DML-before-DROP, schema-only zero participants, runtime-created Heap retirement,
secondary indexes, retained old data, same-name recreation, three catalog-only
reopens, read-only journal stability, missing retained resource, and a 17-window
real subprocess crash matrix with three reopens per window.

## Persistent compatibility and deferred work

NBSC v1, NBSM v1, Heap, WAL, transaction status, BTree/IndexCatalog, partition,
LSM, Protocol v1 and PG wire formats do not change. NBSJ v1 gains record tags and
CORD v2 permits zero tuples only for schema decisions. Current readers decode old
Round 18 histories; downgrading after writing DROP records is unsupported.

Deferred: SQL/PG `DROP TABLE`, `IF EXISTS`, CASCADE/RESTRICT, multi-table DROP,
physical Heap unlink, retired-resource GC, LSM DROP, partition DROP, CREATE+DROP or
multiple table DDL in one transaction, ALTER TABLE, security-catalog/grant cleanup,
and manifest rewriting. Round 21 may add a thin exact-identity SQL frontend only
after this Core lifecycle remains stable.
