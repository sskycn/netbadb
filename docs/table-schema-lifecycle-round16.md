# Table schema lifecycle — Round 16 architecture audit

Status: chosen architecture; **not implemented**. Audited base:
`c93caf714f25e6c80fa63c693ec58f061a5ef19f` (Round 15).
This round changes documentation and tests only. It adds no table DDL, schema
mutation API, persistent format, protocol tag, or SDK regeneration workflow.

## 1. Decision and evidence boundary

Choose **bootstrap + persistent authority**, with one database-owned
`SchemaCatalog`. On a fresh database, validated Canonical Schema is bootstrap
input. After bootstrap, a recovered persistent catalog supplies the complete
committed logical schema. Application/manifest/SDK definitions become explicit
expectations or migration inputs; they never replace that catalog on reopen.

Today the answer is **C**: caller-supplied `Schema` is both the live logical
schema used for compilation/inspection and the external expectation checked
against storage. It is not a recoverable, database-wide persistent authority.
Splitting these responsibilities is a prerequisite for table DDL.

The most important corrections to plausible assumptions are:

- `SchemaFingerprint` hashes **one table**, not a database.
- `ColumnId` is explicitly supplied and supports sparse values; it is not a
  vector index. Row layout and column presentation nevertheless use vector order.
- Manifest v4 opens existing **Heap files only**. It has no storage-engine,
  partition, coordinator, or external schema-artifact selector.
- Protocol v1 already sends per-table fingerprints. Both remote clients already
  support exact expectations on a subset of visible tables.
- `primary_key` is descriptive metadata, not an enforced uniqueness constraint.
- Index high-water reservations currently roll back with their transaction.
  The proposed non-rollback table reservation deliberately has stronger semantics.
- Single worker ownership serializes commands, **not entire session transactions**.
- Physical creation error cleanup is not crash-atomic database creation.

### Source map

Paths below are relative to this document and name the audited symbols, so they
remain useful after line numbers change. Tests supplement, rather than replace,
inspection of production code.

| Area | Source and relevant symbols |
| --- | --- |
| Canonical schema, fingerprint | [schema/lib.rs](../crates/netbadb-schema/src/lib.rs): `ColumnDef`, `TableDef::{validate,canonical_bytes,fingerprint,column_by_id}`, `Schema::{new,validate,add_table}` |
| IDs and semantics | [types/lib.rs](../crates/netbadb-types/src/lib.rs): `TableId`, `ColumnId`, `StorageId`, `PartitionId`, `SemanticType` |
| Startup and publication | [core/lib.rs](../crates/netbadb-core/src/lib.rs): `Database::{create,open,create_tables,open_tables,create_storages,open_storages,create_with_placements,open_with_placements}`, coordinator variants, `compose*`, `commit_transaction` |
| Physical bindings | [core/registry.rs](../crates/netbadb-core/src/registry.rs): `StorageRegistry`, `PhysicalBindings`, `TablePlacement` |
| Database transaction | [core/transaction.rs](../crates/netbadb-core/src/transaction.rs): `DatabaseTransaction`, `ensure_participant`, `commit_multi_write`, `rollback_before_decision` |
| Durable decision | [core/coordinator_log.rs](../crates/netbadb-core/src/coordinator_log.rs): `CoordinatorLog`, `CoordinatorParticipant`, `CommitDecision`, `Complete` |
| Placement persistence | [core/partition_catalog.rs](../crates/netbadb-core/src/partition_catalog.rs): `PartitionCatalog`, `CatalogTable`, `RangePartitionSpec` |
| Heap lifecycle | [storage/heap.rs](../crates/netbadb-storage/src/heap.rs): `create_with_storage_id_and_buffer_pool_size`, `open_internal`, `validate_heap_metadata`, `inspect_identity`, `resolve_column_position`, `build_registered_index_in` |
| Engine dispatch | [storage/table.rs](../crates/netbadb-storage/src/table.rs): `TableStorage`, `StorageTransaction`, `StorageReadView` |
| LSM lifecycle | [storage/lsm.rs](../crates/netbadb-storage/src/lsm.rs): `LsmStorage::{create_with_storage_id,open_with_prepared_resolutions}`, `Manifest`, `validate_manifest_schema` |
| HIR/compiler | [hir/lib.rs](../crates/netbadb-hir/src/lib.rs), [compiler/lib.rs](../crates/netbadb-compiler/src/lib.rs): schema lookup, `CompiledStatement`, `CompiledDdlStatement`, `bind_statement` |
| Planner/execution | [planner/lib.rs](../crates/netbadb-planner/src/lib.rs), [executor/lib.rs](../crates/netbadb-executor/src/lib.rs): typed logical identities, access snapshots, physical storage scopes |
| Inspection | [core/inspection.rs](../crates/netbadb-core/src/inspection.rs): `catalog`; [inspect/lib.rs](../crates/netbadb-inspect/src/lib.rs): owned DTOs |
| Manifest/security | [server/manifest.rs](../crates/netbadb-server/src/manifest.rs), [server/authorization.rs](../crates/netbadb-server/src/authorization.rs): `ServerConfig`, `TableBootstrap`, `PrincipalAuthorization` |
| Sessions and worker | [server/lib.rs](../crates/netbadb-server/src/lib.rs): `SessionState`, `build_hello_ack`; [server/runtime.rs](../crates/netbadb-server/src/runtime.rs): `DatabaseWorker`, `WorkerSession` |
| PostgreSQL | [server/postgres.rs](../crates/netbadb-server/src/postgres.rs): `PgCompatibilityCatalog::derive`, `refresh_catalog`, `PreparedStatement`, `Portal`, `assign_synthetic_oids` |
| Native protocol/clients | [protocol/lib.rs](../crates/netbadb-protocol/src/lib.rs), [client/lib.rs](../crates/netbadb-client/src/lib.rs), [Go client](../sdk/go/client.go): HelloAck and handshake |
| SDK/code generation | [Rust facade](../sdk/rust/src/lib.rs), [schema-spec/lib.rs](../crates/netbadb-schema-spec/src/lib.rs), [codegen/lib.rs](../crates/netbadb-codegen/src/lib.rs), [generated Go example](../sdk/go/testschema/netbadb_generated.go) |
| CLI/tooling | [netbadbd](../cmd/netbadbd/src/main.rs), [inspection CLI](../cmd/netbadb/src/lib.rs), [tooling](../crates/netbadb-tooling/src/lib.rs), [LSP](../cmd/netbadb-lsp/src/lib.rs) |

Read the current [architecture](architecture.md), [roadmap](roadmap.md),
[manifest contract](server-manifest-v4.md), [coordinator contract](coordinator-log-v1.md),
[partition contract](partition-catalog-v1.md), [protocol](protocol-v1.md),
[SDK spec](sdk-schema-v1.md), and [PG compatibility](postgresql-compatibility.md)
alongside the code. Historical format inventories in older documents are not
proof of current versions; this audit follows decoder/constructor behavior.

## 2. Current authority, manifest, and startup

`Database` owns `schema: Schema`, `bindings: PhysicalBindings`, and
`registry: StorageRegistry` separately. `schema()` returns `&Schema`; there is
no live mutable schema accessor or database schema replacement API. Each
storage also retains its supplied `TableDef` for row interpretation. These
copies agree because constructors validate inputs; there is no mutation
publication contract for replacing them together.

```text
Embedded TableDef / list of TableDefs      manifest v4 JSON
              |                                |
              |                    validate config + paths + grants
              |                          TableBootstrap[]
              +-------------------------------+
                              |
                  Schema::new(caller definitions)
                              |
              Database create/open family (see below)
                              |
       Heap / LSM identity + fingerprint checks and recovery
                              |
            StorageRegistry from persisted StorageIds
                              |
         PhysicalBindings (single or durable RANGE placement)
                              |
              Database.schema = supplied Schema
                    /                  \
             HIR/compiler          inspect_catalog
                    |                  |
            planner/executor        SDK / PG projection
```

**Fresh Heap create**: `create` builds one schema and calls `create_heap`
(default `StorageId(1)`). `create_tables` validates paths/schema, creates
storages in declaration order with IDs 1..N, then composes the registry and
single bindings. The plural API is a construction operation, not a transactional
DDL operation on an existing `Database`.

**Ordinary reopen**: `open_tables` rebuilds `Schema` from the supplied list,
opens each supplied path with its `TableDef`, and builds single bindings from
persisted storage identities. Input order changes presentation order but does
not reassign StorageIds. An omitted table is simply absent from this opened
composition; ordinary reopen has no durable complete-table-list comparison.
This is why external schema is still needed even when every individual file
rejects a wrong fingerprint.

**Coordinator reopen**: inspect supplied storages and decisions first; validate
exact retained decision participants, resolve prepared transactions, append
missing Complete records, then compose. A subset that omits a decision
participant fails. This is recovery validation, not general schema discovery.

**Partition reopen**: read immutable PartitionCatalog; validate exact table
IDs/fingerprints; inspect unordered Heap paths by persisted StorageId; validate
the exact placement storage set; resolve coordinator recovery; compose.
The catalog knows placement and table fingerprints, not full column definitions.

**Server**: manifest v4 embeds table/column IDs, names, physical/semantic types,
nullability, PK flags, and explicit paths. Config loading validates canonical
schema and existing regular files, but does not compare file fingerprints.
The database worker later calls `Database::open_tables` to do that comparison
and recovery. Both native and PG listeners use this Heap-only composition;
they do not enable the optional durable coordinator. Manifest fields also
configure listen address, TLS, limits, and TableId grants. Paths are supplied,
not generated from table names/IDs. There is no engine or partition selection,
and no reference to a separate Schema Spec artifact.

The offline inspection CLI uses the same config/bootstrap and normal recovery;
it is not a no-write forensic reader. The LSP instead loads SDK Schema Spec v1
once and diagnoses against a fixed canonical schema without opening storage.
JSON remains boundary input/output, not execution IR or persistent catalog.

## 3. Identity and persistent-reference audit

| Identity | Allocation now | Persistence and scope | Mutable-schema consequence |
| --- | --- | --- | --- |
| `TableId(u64)` | Caller, manifest, or Schema Spec explicitly supplies it; codegen preserves it. No authoritative allocator. | Unique within `Schema`; Heap page 0 and LSM MANIFEST store it; partition entries store it. Independent compositions can reuse it. | Need one database-wide durable allocator and a database incarnation boundary. |
| `ColumnId(u32)` | Explicit caller/manifest/spec value, not declaration position. | Table-local uniqueness; full set is committed to the fingerprint, not recoverably stored in Heap metadata. Index definitions and LSM clustering/partition-key metadata store referenced IDs. | Preserve IDs, add per-table high-water, separate ID lookup from row-layout ordinals. |
| `StorageId(u64)` | Single constructors default to 1; plural create assigns checked position+1. | Nonzero, persisted in Heap/LSM metadata and transaction identities; reused from metadata at reopen, independent of path/order. Registry validates uniqueness. | Current creation ordinal is not an allocator for runtime additions. Need durable independent high-water. |
| `PartitionId(u64)` | Explicit `RangePartitionSpec` value. | Nonzero, globally unique within PartitionCatalog, persisted with TableId/range/StorageId; not a range-vector index. | Preserve legacy IDs; later partition mutation needs its own durable reservation domain. |

`Schema::validate` rejects duplicate active table names/IDs and duplicate
column names/IDs, but does not reject numeric zero, require a column, or enforce
PK shape. Some physical identity/partition inspectors require nonzero TableId.
That validation asymmetry must be handled explicitly by migration; never silently
renumber an existing table to make it pass.

Table lookup names are exact strings and are not durable identities. The core
schema has one flat table-name scope, no namespace object. Parser keywords are
case-insensitive but identifiers retain spelling and HIR lookup is exact.
Quoted identifiers are unsupported. Existing index DDL admits the literal
`public` qualifier in HIR; it does not create a schema namespace. Future first
DDL keeps that limited behavior without importing PG case folding into Core.

Persistent and long-lived references that make reuse dangerous:

| Reference | Current evidence / persistence |
| --- | --- |
| Heap, LSM, physical row handles | Stored TableId/StorageId; `StorageRowHandle` is tagged with StorageId. |
| Indexes | Per-storage index records reference ColumnId; ownership and access paths belong to that table/storage. Table identity is also available through physical metadata. |
| Partition catalog and coordinator | TableId/ColumnId/PartitionId/StorageId in placement; decisions reference StorageId + physical TxnId, not names. |
| Grants | External manifest persists TableId grants; admitted sessions clone them. No durable in-database grants. |
| Prepared statements | Typed HIR/logical relations, read/write table sets, column provenance, parameter/result types retained in memory. |
| Inspection/PG/SDK | Owned inspection snapshots, synthetic PG identities, HelloAck client caches, generated constants. |
| Events/audit | No durable schema-event or table-audit catalog found. Runtime counters do not supply identity history; future events must use IDs. |

DROP/recreate must mean **same lookup name, new TableId**. Even after old files
are gone, prepared metadata, grants, backup/log references, or generated client
constants can outlive them. Column DROP/recreate has the same rule within a
surviving TableId. Physical relocation/rename must not change logical identity.

Column order is still a real ALTER blocker: tuples encode values in schema
vector order, and HIR INSERT/Heap projection resolve IDs to positions. Stable
sparse IDs already work, but dropping a middle column needs an explicit row
layout/version strategy; merely removing the vector entry would misdecode old
rows. ALTER and row-layout evolution remain deferred.

## 4. Fingerprints, generations, and prepared caches now

`TableDef::canonical_bytes` emits `NBTS`, canonical version 1, reserved bytes,
little-endian TableId, length-prefixed UTF-8 table name, column count, then each
column **in vector order**: ColumnId, name, physical tag, optional semantic name,
nullable byte, and PK byte. `fingerprint()` is SHA-256 of these validated bytes.
It covers semantic nominal types as well as physical representations. It excludes
indexes, statistics, paths, StorageIds, partitions, grants, high-water, and
mutation order. There is no whole-database canonical encoding/fingerprint API.

Actual uses:

- Heap writes fingerprint and column count to page 0 and compares supplied
  TableId first, then fingerprint and column count before recovery.
- LSM stores it in MANIFEST and validates it with TableId before WAL/SST recovery.
- PartitionCatalog stores per-table fingerprints and compares exact supplied
  schemas and inspected storage identities.
- Core inspection computes fingerprints from its live supplied schema.
- Server config computes fingerprints to validate definitions; HelloAck exports
  them. Rust and Go client handshakes compare exact per-required-table values.
- Go codegen calls the Rust canonical implementation to emit constants; Go does
  not reproduce the algorithm. PG table/index OID digests include the fingerprint.
- Schema, storage, manifest, inspection, protocol/client, and codegen tests cover
  these boundaries. Fingerprint has no content decoder and cannot restore schema.

`catalog_generation: u64` is initialized to **0 in every compose path**. Direct
**named** index CREATE, DROP and committed staged index changes increment it with
`saturating_add(1)`. Rollback and physical maintenance do not publish logical
index changes. It is neither persisted nor a schema version, and saturation
would eventually stop refresh. PG sessions compare it in `refresh_catalog`.
Legacy anonymous `Database::create_index` and partition-index creation do not
increment it: this is an existing refresh coverage gap, pinned by the audit
experiment and deferred from this no-runtime-change round.
The planner instead receives current access paths on every execution; the Core
prepared object does not cache a physical plan or a generation.

| Prepared content | Core | PG adapter |
| --- | --- | --- |
| Source SQL | No | Yes, statement and portal |
| Parser AST | No retained raw AST | No standalone AST cache |
| Typed HIR / logical statement | Both in `CompiledStatement` | Core prepared object |
| Parameter metadata | `PreparedParameter` with semantic type | PG parameter OIDs plus Core metadata |
| Result shape | Derivable from typed output fields; `description()` | Cached `FieldDescription` vector |
| Dependencies | IDs embedded in HIR/logical plan; read/write TableId extraction | No independent schema dependency version |
| Physical plan | Rebuilt after each bind | Core executes/replans |
| Bound/result state | Ephemeral bound logical statement | Portal values, execution, fields, optional materialized result |

Current index-only replan is safe with an unchanged logical schema. Table DROP,
column removal/rename/type/nullability change, SELECT-star shape change, stale
DML layouts, foreign-database prepared reuse, authorization snapshots, PG portal
field/OID caches, and SDK connection identities need stronger checks before any
schema mutation is enabled. Names alone cannot repair these dependencies.

## 5. Current transaction and physical lifecycle

The worker constructs and exclusively owns Database and SessionStates; socket
threads send typed commands. Sessions interleave commands while holding separate
transaction handles. Each engine owns its writer lease and MVCC views. The
legacy composition permits at most one write StorageId **per transaction**, not
one database-global writer across all sessions. Coordinator-enabled embedded
compositions allow multiple write participants. Read participants register
lazily; Repeatable Read is storage-local at participation, not a global timestamp
captured at SQL BEGIN. There is no schema snapshot: all compile calls use the
same immutable `Database.schema`.

Current index DDL stages definitions/retirements in `DatabaseTransaction`, uses
existing storage WAL, and publishes active index state only after
`Database::commit_transaction`. Direct transaction commit refuses pending index
mutations. Mixed pending index create/drop is restricted, and DML after pending
index creation is rejected; this is not a ready-made table-schema overlay.

```text
Current multi-storage DML (optional coordinator composition)
  lazy StorageId participant registration
  -> prepare all write participants durably, sorted by StorageId
  -> sync CoordinatorLog CommitDecision = GLOBAL COMMIT POINT
  -> commit each prepared storage
  -> sync Complete
  -> return success
```

Read-only and one-writer transactions bypass CoordinatorLog. Before decision,
rollback undoes participants. Decision sync uncertainty enters DecisionPending:
rollback is forbidden; retry the same decision. After decision, only forward
completion is legal. Startup uses exact participant identity and presumed abort:
prepared + no decision aborts; prepared + decision commits. Standalone in-doubt
opens fail rather than guess. CoordinatorLog v1 records decisions/completion,
not generic metadata, schemas, file-creation intentions, or participant locators.
Its existing participant tuple can identify only a physical StorageId/TxnId.

### Current Heap create/open

```text
validate TableDef + fingerprint + capacity + StorageId
 -> create-new WAL sidecar
 -> create page container
 -> create transaction-status sidecar
 -> write page 0: TableId, column count, fingerprint, index root, StorageId
 -> page 1: empty IndexCatalog; page 2: Heap
 -> flush buffer
 -> build transaction manager / TableStorage
 -> register storage and physical binding in Database
```

Heap open validates metadata against external TableDef, recovers WAL, reconciles
transaction status, restores indexes/statistics and engine state, then becomes
queryable. Heap does not require a primary key; duplicates are legal. PK flags
are not checked as uniqueness constraints in writes, and nullable PK metadata
is accepted by canonical validation. Existing inspection reflects the flags;
it must not be advertised as proof of constraint enforcement.

### Current LSM create/open

```text
validate table + nonzero StorageId
 -> validate NOT NULL Int64/UInt64 clustering ColumnId
 -> create-new root directory and sst subdirectory
 -> initialize MANIFEST (TableId, fingerprint, clustering identity,
    storage/WAL generations, allocator reservations, empty SST references)
 -> create/sync LSM WAL; sync root directory
 -> construct runtime + memtable
 -> compose Registry/Bindings
```

MANIFEST is **LSM physical manifest**, unrelated to server deployment manifest.
It is physical authority for SST references/allocators, not a schema catalog.
Open requires full external TableDef, validates fingerprint, cleans known
engine-local orphans, opens referenced SSTs, resolves WAL and prepared work,
then exposes storage. Clustering order is duplicate-preserving; not a PK.
Manifest physical generation is not database schema generation.

### Current partition create/open and cleanup limits

Placement creation validates the full range plan, creates Heap resources with
new ordinal StorageIds, writes/syncs immutable PartitionCatalog, creates the
coordinator, then composes. Keys are NOT NULL Int64/UInt64, ranges half-open,
gaps legal, overlap invalid. All partitions are Heap; no partition DDL exists.
The catalog stores logical placement IDs/bounds but no paths or full TableDefs.

On handled failure, `create_tables` releases completed handles and removes only
known new heap/WAL/alternate-WAL/status paths. A cleanup failure can return
`CreateTablesRollback { creation, cleanup_path, cleanup }`. Other branches
(coordinator creation failure, mixed-storage cleanup, partition cleanup, several
engine-local failures) discard some cleanup errors. Heap failures after initial
sidecar setup are not uniformly guarded by a rollback object. LSM recursively
cleans only its newly created root on a handled construction error. None of these
paths writes a database-level durable creation intent. Parent-directory durability
is not uniformly established for the whole newly created set (PartitionCatalog
create, for example, syncs its file only). Process death bypasses error cleanup.

No transactional table DROP/file-retirement API exists. Closing storage flushes
and releases it; test cleanup helpers are not production DROP semantics. Current
arbitrary supplied paths can contain table names, and no persistent directory
allocator prevents a later same-name creation colliding with old files.

## 6. Authorization, clients, and PostgreSQL

Authorization is server deployment policy. Manifest grants key by TableId,
unknown IDs are startup errors, and missing operations deny access. There are
no owners, default grants, wildcards, role catalog, SQL GRANT, or CREATE TABLE
privilege. Principal admission clones grants into worker sessions; no policy
hot reload exists. Authorization follows successful compilation and precedes
planning/execution/writer acquisition. Inspection in Core is unfiltered; server
HelloAck and PG metadata filter for admitted principal visibility. Core,
SessionState, and storage remain principal-unaware.

Protocol v1 has HelloAck table ID + 32-byte fingerprint, capabilities and limits;
QueryStart carries result names, semantic/physical types and nullability. It has
no full schema enumeration, schema generation, refresh notification, required
schema payload from client to server, or prepared handle protocol. Begin is
anchored to a TableId. Both client handshakes locally check required identities;
server does not learn those requirements. `ServerInfo` is a connection-start
snapshot, not live schema metadata. Native sessions compile SQL on every Execute.

Rust SDK is a feature-gated facade over Core or synchronous client, with canonical
schema constructors; it has no independent Rust derive-based persistent catalog.
Go's generated package emits nominal types, column constants, row decoders, SQL
builders and Rust-calculated fingerprints from explicit Schema Spec IDs. It does
not own database schema. LSP/Schema Spec knowledge is likewise an expectation,
not live introspection. Additional dynamic tables need not appear in older SDKs.

PG catalog derives from `Database::inspect_catalog`, including active schema,
indexes and placement visibility. `public`, type OIDs, regclass-like lookup,
psql catalogs, SQLAlchemy Inspector and Alembic index reflection remain adapter
projections. No PostgreSQL catalog is persisted.

The current synthetic table OID hashes domain + TableId + table fingerprint;
index OIDs hash table identity/fingerprint + ColumnId + index kind/uniqueness.
Sorted digests with collision probing yield deterministic IDs for a given whole
catalog, independent of input table order. New runtime tables naturally enter
this derivation. **Do not promise OID stability across catalog changes**: table
shape changes alter hashes, and a new colliding digest can shift another object's
probe. Old numeric OID parameters must not silently resolve to another object.
Metadata query/portal invalidation on catalog revision is required. The target
adapter retains a session-local issued-OID-to-canonical-identity map and retired
OID tombstones: after refresh, an old numeric lookup may resolve only to its
original still-active identity. If re-derivation would reassign an issued OID,
require reconnect before further OID-based metadata, rather than alias it. This
also covers newly parsed numeric OID queries, not just old portals. Across
connections/restarts clients must rediscover synthetic OIDs; persistent OID
references are unsupported. These maps are projection caches only; no stored
OID, PG namespace, or PG type ID belongs in SchemaCatalog.

## 7. Chosen target architecture and ownership

```text
Fresh canonical bootstrap / explicit legacy migration
                         |
                         v
             persistent per-database SchemaCatalog
              (sole logical schema authority)
                         |
                 recovery + validation
                         |
               CommittedCatalogState
         schema + placements + generation + identity
             /               |                \
     compiler/HIR       physical bindings       inspection
          |                  |                    |
   typed logical IR     StorageRegistry       PG / SDK metadata
          |                  |
       planner --------> executor

Committed Schema + TransactionSchemaDelta
                         |
          transaction-visible materialized Schema
          + private bindings/staged storage resources
                         |
                  compile / authorize / execute
```

Canonical definitions remain in `netbadb-schema`, primitive newtypes in
`netbadb-types`. A catalog persistence/recovery module belongs with the Core
DB-level coordinator composition, following the existing partition/coordinator
modules; low-level codecs may use storage primitives without a dependency back
from storage to Core. Do not create a speculative crate. Compiler/HIR continue
receiving `&Schema`: materialize a validated overlay as a Schema rather than
introduce a persistence-aware resolver trait now. Planner consumes typed IDs and
physical capability snapshots; it never loads catalog files.

`CommittedCatalogState` is an immutable logical publication bundle: Schema,
placement descriptors, per-table versions/fingerprints, schema generation, and
a matching binding map to registry resources. The registry owns mutable engine
handles; it is not cloned into every immutable schema snapshot. Core prepares
all replacements before one worker-owned publication point. There is no reason
to force `Arc<Mutex<Schema>>`, async, or a global mutable catalog through layers.
An owned snapshot suffices initially; `Rc` sharing is optional when a concrete
lifetime requires it. No observer may see new Schema with old bindings.

Reject an enduring StaticSchemaDatabase/MutableSchemaDatabase split: existing
SDK subset gates can express expectations, so two authority modes add complexity
without resolving a real contract conflict. A temporary explicit **legacy
migration API** is not a second product truth. Existing low-level Heap/LSM APIs
remain useful storage primitives but must not bypass a managed database owner.

### Model selection: committed snapshots plus transactional envelopes

Choose **complete schema snapshots per logical schema commit**, with a bounded
catalog prepare/commit journal and a separate durable allocator-reservation
stream. This is a snapshot model with recovery envelopes, not an event-sourced
logical schema folded from every historical CREATE/DROP. Reopen loads a complete
validated snapshot; recovery selects committed candidates using decisions.

| Alternative | Decision |
| --- | --- |
| External immutable truth forever | Cannot recover runtime-created tables without changing the external artifact; reject as target. |
| Table file page 0 as database catalog | Cannot own absent/retired tables or coordinate several engines; reject. |
| CoordinatorLog as metadata store | Decision-only responsibility and tuple format do not encode schema; reject. |
| Event-only schema log | Requires replay/compaction and historical mutation validation for small infrequently changed schemas; reject initially. |
| Full snapshots | Simple canonical recovery and consistency validation; accept rewrite cost before measured need for deltas. |
| Snapshot + logical delta optimization | Possible later, but no logical event compaction framework now. |

Use an explicit database catalog root/location, not the first table path or its
inferred common parent. Conceptual durable contents are:

- Database incarnation identity, explicit format versions and root/install state;
- active tables: stable TableId, name, ordered ColumnDefs, table schema version,
  canonical table fingerprint, per-table column allocation state;
- database schema generation and optional whole-schema content fingerprint;
- physical resource descriptors: StorageId, TableId, engine kind, owned locator,
  table fingerprint, layout metadata (clustering/range mapping where applicable);
- retired resource descriptors sufficient for recovery and later safe reclamation;
- allocator high-waters/reservations, including aborted allocations;
- prepared candidate snapshot identity/digest, transaction identity, participant
  set and staged-resource inventory; committed root reference and recovery journal.

The file layout is deliberately **not specified or implemented this round**.
A later format must have explicit magic/version, widths/endian, bounded lengths,
checksums, complete/torn-record rules and a crash-tested atomic install contract.
A checksum/digest validates content; it does not decide transaction commitment.
Root publication must reference the intended generation and digest, never select
"the highest generation file" merely because it exists.

The existing PartitionCatalog is imported into the new catalog's placement
section during migration. After adoption its file is legacy evidence, not a
second writable placement truth. Existing per-engine IndexCatalog and LSM
MANIFEST continue to own physical indexes/SSTs, not logical table/column schema.
Do not duplicate independently mutable index definitions in two authorities.
For index reflection cache changes retain a separate runtime revision signal.
No PG fields or security principal catalog are added to schema persistence.

## 8. Durable identity, generation, and content policies

**Table IDs:** reserve from database-wide `next_table_id` before exposing the ID
to a transaction overlay, file name, or log. Sync a non-rollback reservation;
only then use it. Rollback, DROP, restart, and compaction never lower the bound.
Do not reconstruct it from active tables. Repeated creation with the same name
gets a new ID. Even a failed/aborted creation consumes its reserved ID.

**Column IDs:** use per-table `next_column_id`. Initial CREATE allocates a stable
set once and persists the ordered definitions. Imported sparse IDs remain
unchanged. Future ADD reserves durably and DROP does not lower the bound; ADD
rollback also does not reuse an exposed ID within that TableId. A rolled-back
new table needs no globally unique column allocator because its TableId can
never recur. Column position is independent metadata, not the allocator.

**Storage and partition IDs:** separate database-wide non-rollback high-waters;
do not cast TableId to StorageId. Every created physical resource receives its
own StorageId; partitioned tables may own several. Partition mutation is deferred,
but imported partition IDs/high-water must be preserved in the foundation.

Allocation state may be expressed as `Next(value)` or `Exhausted`, avoiding a
wrapped max+1 sentinel. Existing/imported maximum integer IDs can remain valid
but exhaust that domain. Every increment and reservation range uses checked
arithmetic; exhausting IDs produces a typed error before files/logical mutation.
The reservation store is independent of transaction rollback and schema content
snapshot replacement: committing an older candidate must not overwrite a newer
allocator high-water. Recovery takes only authenticated durable reservation
state, never guesses from directory names. Gaps are allowed and expected.

`DatabaseId` currently exists as an unused primitive, not a durable globally
unique owner. Choose a distinct **database incarnation identity**: a nonzero
128-bit OS-random value generated once and durably installed in the catalog root;
do not silently reinterpret the existing u64 newtype. OS randomness failure is
a typed bootstrap error. Reject conflicting roots/resources within a managed
inventory. In-process prepared handles additionally carry an unforgeable
owning-database token, like current transaction ownership. Recovery and an offline
restore of the same database preserve incarnation; a separately writable clone
requires a new incarnation and explicit ownership rebinding. Cloning is unsupported
until that operation exists. Concurrent owners of a copied database remain
unsupported. These are target contracts, not changes to existing formats.

| Concept | Chosen meaning |
| --- | --- |
| TableId / ColumnId | Durable object identity, independent of name/order/version. |
| `SchemaGeneration` | Durable database-wide order of committed table/column schema changes; bootstrap starts at 1; one increment per changed committed transaction. |
| Per-table schema version | Durable checked increment for that table's logical shape/lookup contract; absent table fails dependency validation. |
| `SchemaFingerprint` | Existing per-table canonical content digest, unchanged algorithm. |
| Whole-schema fingerprint | Deferred from the foundation; per-table fingerprints are sufficient. If later exposed, use a distinct domain/version and active TableDefs sorted by TableId, preserving each table's column order. Never use table insertion order. |
| Runtime catalog revision | Checked process-local invalidation epoch for any inspection-visible logical catalog change, including index CREATE/DROP; may reset on reopen. |

No-op DDL, rollback and ID reservation alone do not increment SchemaGeneration.
A transaction's several schema changes increment it once; create-then-drop with
no surviving logical change consumes IDs but need not change logical generation.
Index-only DDL does not stale typed table dependencies and keeps physical
replanning. Snapshot framing/checksums protect allocation history, placement and
retirement records separately from logical content fingerprints. Active-only
content hashes exclude historical allocator state, generation, grants and paths.
Two equal content hashes do not prove the same commit or database incarnation.

Decoder and open invariants must reject duplicate active TableIds/names, duplicate
ColumnIds/names, empty names, unknown type tags, invalid semantic names, invalid
nullable/PK flag encodings, unsupported constraint definitions, invalid ranges,
zero/duplicate physical IDs, ownership/locator conflicts, bad fingerprints,
and inconsistent versions/generations. Next-ID states must exceed every relevant
active/retired/prepared/reserved ID; a truncated reservation history or missing
mandatory root is an error, not permission to derive max(active)+1. Validation
must be bounded before allocation. Legacy descriptive PK flags must be preserved
and identified as unenforced, not reinterpreted as a newly enforced constraint.

## 9. Bootstrap, reopen, manifest migration, and downgrade boundary

Fresh managed databases may have an empty Schema, unlike manifest v4's required
nonempty table list. Bootstrap validates the full input and placement policy,
creates/stages resources with final identities, synchronizes files/directories,
and atomically installs the first catalog root. No sessions start before complete
installation. Failed bootstrap leaves either an uninstalled recoverable staging
inventory or a complete catalog; absence alone must never trigger overwriting
arbitrary existing files. Empty managed databases need no transaction anchor table.

```text
Legacy input: explicit complete TableDefs + storage inventory
              + optional existing placement/coordinator files
 -> exclusive offline migration, inventory/identity validation
 -> resolve existing transactions with existing recovery rules
 -> validate all physical resources against full supplied schema
 -> retain IDs, fingerprints, index definitions and placement
 -> compute initial allocator bounds including retained references
 -> stage and sync complete generation-1 catalog + installation record
    (attach validated existing coordinator or create/sync an empty one)
 -> atomic install + parent directory sync
 -> committed catalog is authority
 -> reopen by catalog location; external schema only verifies
```

Legacy ordinary open cannot prove its supplied list was ever the complete
historical database. Migration therefore requires an explicit declared database
inventory/boundary; do not silently import whatever files a glob finds or claim
that a successful subset open proves completeness. Validate retained coordinator
participants and partition catalog exactly. If old files/SDK/grants from another
composition reuse IDs, require explicit operator resolution, not renumbering.
No historical runtime table allocator existed to recover. The import establishes
the managed identity domain using all declared resources and retained references;
unknown/out-of-inventory files are quarantined/unclaimed, never automatically
attached. Choose fail-before-install for zero TableId/StorageId/PartitionId legacy
resources: return an unsupported-legacy-identity diagnostic and leave legacy data
untouched for a separately designed explicit migration. Never silently renumber.
Zero ColumnId is legal table-local legacy identity and is preserved; new allocation
starts at 1 or above the imported maximum. This exceptional rejection does not
justify rejecting all normal legacy databases.

An aborted installation keeps legacy authority and can retry against the same
validated inventory. A committed installation keeps catalog authority even if
cleanup or a client acknowledgment is lost. Installation marker/root publication
must be crash-safe and idempotent; keep old source files for recovery until the
new root is durably complete. Migration itself does not make legacy error cleanup
transactional. Once a managed root exists, malformed/missing referenced snapshots
are hard errors, not a fallback to manifest bootstrap.

```text
Managed reopen (before any session admission)
 -> open explicit catalog root + decision/reservation/prepare envelopes
 -> validate candidate schemas and resource inventories without publication
 -> resolve committed winner / undecided loser catalog candidates
 -> recover every required physical participant from matching decisions
 -> finish durable committed-root/resource installation
 -> validate active schema ↔ TableId/fingerprint ↔ StorageId/kind/layout
 -> rebuild registry + bindings from catalog resource descriptors
 -> publish one CommittedCatalogState
 -> validate external expectations and grant policy; serve sessions
```

Recovery needs candidate schema/resource descriptors **before** physical open,
since physical open needs TableDef. Reading an uncommitted candidate to recover
its resources is not publishing that schema. Missing committed physical resources
are corruption/unavailable-database errors; never create an empty replacement.
Unknown files classify as known staged loser, known retired resource, or unknown
orphan requiring investigation. Names and filesystem existence are not authority.

The next manifest revision separates deployment (network/TLS/limits/policy),
explicit catalog locator, fresh bootstrap input, and optional expected schemas.
At the Core API boundary, use three explicit operations: fresh managed create
with a catalog location and bootstrap Schema; managed open with a catalog location
and optional expectations; offline legacy import with a declared resource
inventory. Their names/signatures are deferred to implementation, not their
authority semantics. Existing Database create/open entry points must be routed
through this boundary or confined to explicit legacy import; they must not remain
a way to recompose managed files into an arbitrary subset. New managed ownership
records must be checked by every new Database entry point before opening resources.
Direct low-level Heap/LSM access to a managed resource is unsupported while owned
by that database, like today's unsupported multi-process access.

After migration an old application schema is checked as an **exact per-table
subset expectation**, with extra active tables allowed. A missing or changed
expected table fails verification; it is neither ignored nor recreated. An
explicit strict whole-schema verification option may be useful for deployment,
but never mutates the database. Existing manifest v4 is a legacy import input,
not a hidden overlay appended on every restart.

Managed locator ownership replaces arbitrary name-based filenames for new
resources: a database-owned directory keyed by durable StorageId contains Heap
and all sidecars (or an LSM directory). Table rename does not move it. Legacy
resources may retain recorded validated locations; remapping is explicit by
StorageId and preserves identity. Avoid duplicate/symlink-alias ownership and
path traversal. Cross-filesystem relocation is separate maintenance, not an
implicit commit rename. No path literal is part of canonical logical schema.

A marker understood only by new code cannot stop an old binary from opening
legacy files. Until a versioned physical-format guard or enforced database lock
exists, migration requires an explicit offline/downgrade prohibition; do not
claim old binaries fail closed automatically. New managed open APIs must never
detect a missing catalog and silently recreate it. A database ownership
lock/admission mechanism is a prerequisite to concurrent-process safety; current
Core supports a single owning process only. This remains a deployment restriction
until implemented and tested.

## 10. First schema transaction semantics and overlay

Choose a **quiescent schema-writer lease**, not a multi-version schema catalog.
Every database transaction captures the committed schema generation at BEGIN and
registers a database-level lifecycle lease, even before first storage access.
That registration is new work; current lazy engine participants are insufficient.

A transaction may enter schema-write mode only when no other database transaction
is active. Admission failure is a typed busy/conflict error before reservations
or physical changes, with no implicit commit. While its schema-writer lease is
held, other transaction/ordinary statement admissions fail promptly or retry at
the caller; do not block a worker waiting for another session's COMMIT. Cleanup
commands remain available. Committed read-only metadata snapshots can still be
served without exposing the overlay. This deliberately conservative first
release trades concurrency for one schema version and deterministic cleanup.

Thus a long transaction never sees another session's table-schema commit midway
through it: its existence prevents admission of that DDL. Read Committed and
Repeatable Read retain their current data semantics; this does not invent global
data MVCC. Index-only planning changes may retain current semantics because
they do not replace table/column schema. Later concurrent schema snapshots need
explicit retention/GC and dependency locks, not an unannounced relaxation.

**Support DDL → DML in the same explicit transaction from the first Core table
mutation release.** Materialize `committed Schema + TransactionSchemaDelta` into
an immutable transaction-visible Schema; maintain matching private bindings and
staged storage handles. Own SQL resolution, authorization access extraction,
query execution, and transaction-scoped inspection use this same view. Other
sessions see only committed state (or the explicit busy result). Do not mutate
global `Database.schema` early.

```text
BEGIN G -> acquire schema writer lease -> reserve IDs -> stage CREATE x
 -> rebuild private Schema/bindings
 -> INSERT/SELECT x resolves and executes against private storage
 -> prepare private schema and physical writes
 -> durable decision -> publish committed bundle G+1 -> release lease
ROLLBACK -> undo writes -> discard private view -> record loser cleanup
         -> retain allocation high-waters -> release lease
```

A staged DROP removes the old TableId from private lookup immediately, but keeps
its handles/descriptors for rollback/recovery. DROP then CREATE same name in one
transaction allocates a new TableId/storage. CREATE then DROP of a private new
table can eliminate the logical delta, not reverse reserved IDs. Existing table
writes and new-table writes must commit/abort together. Until generalized index
DDL overlay semantics are implemented, unsupported combinations must reject
before changes; do not silently commit them. No savepoint promise is added.

Generic future IR: AST -> HIR validated column/type/name intent ->
`CompiledDdlStatement::{CreateTable,DropTable}` -> Core authorization/admission
boundary -> reservation and transaction delta -> storage operations. Parsing
and compilation do not allocate durable IDs or create files. Revalidate names,
expected generation and referenced identities at execution under the lease.
No PostgreSQL type/OID or physical path is part of compiler IR.

## 11. Future physical CREATE, commit, and recovery contract

Choose **durable creation intent + staged, identity-named resource directory**.
Create resources at their final StorageId-based location after logging ownership;
"staged" means not reachable from committed bindings, not that the pathname must
change. This avoids renaming a multi-file Heap set after decision, and avoids
live handles caching obsolete paths. Never expose it by enumerating directories.
Initialize/sync every required Heap/WAL/status file and sync containing directories
before the prepare vote. Directory sync includes newly created parent entries.
No cross-filesystem atomic rename assumption is needed for table resources.

SchemaCatalog becomes a **distinct database transaction participant**, not a fake
Heap table or reserved StorageId. Extend the coordinator with an explicitly
versioned typed participant contract: physical participants retain StorageId +
physical TxnId; schema participant identifies the database catalog, transaction,
base/target generation and prepared snapshot digest. A prepare envelope contains
bounded resource descriptors for discovery/recovery, including newly created
storages. This requires coordinator format/API work; v1 cannot encode it as-is.
Its old decoder must not reinterpret these records. Decide and test version
migration before writes are enabled.

Any transaction with schema mutation uses the durable coordinator decision,
even if it creates only one empty Heap or drops the last table. Do not apply the
current "one physical writer bypasses coordinator" optimization to schema work.
Pure existing DML can retain its existing fast path. Fresh installation of an
empty catalog uses its dedicated atomic installation contract, not a fictional
physical participant.

### Future CREATE sequence

1. Compile generic intent against the current/private Schema. Validate names,
   supported types/nullability, limits and requested placement; authorize in the
   adapter, acquire schema writer lease, and revalidate before mutation.
2. Reserve TableId and independent StorageId durably. Allocate initial ColumnIds
   once. Use physical-only `SemanticType` for first SQL-created BOOL, INT64 and
   TEXT columns; never invent nominal names in the PG adapter. Application/API
   bootstrap can continue expressing nominal types.
3. Persist a creation intent containing transaction/identity, exact owned resource
   locator, engine kind and candidate TableDef/fingerprint. Sync before creating
   the directory. Pre-existing/conflicting paths fail; never truncate/adopt them.
4. Create and initialize the staged Heap resource set with final identities;
   flush/sync required files and directories. Add it only to private registry and
   bindings. Any own DML uses normal physical transactions on this storage.
5. Build/validate the complete candidate catalog snapshot and publication bundle.
   Include all affected resources and retired descriptors; check generation
   overflow and authorize derived visibility. Prepare/sync every physical writer
   and the SchemaCatalog candidate. Newly created empty storage still needs a
   durable resource-ready proof even when there is no data write transaction.
6. Sync typed CoordinatorLog CommitDecision for the exact participant set.
   This is the sole schema transaction commit point. All physical bytes needed
   for winning recovery already exist durably.
7. Finish physical participant commits and durably install the decided schema
   root/generation. Do not publish intermediate changes. Sync completion evidence
   only after all required durable effects are complete.
8. Replace the committed Schema/bindings/registry visibility bundle, advance
   runtime revision and invalidate dependent caches, then acknowledge success.
   Release lease only after forward completion or successful loser cleanup.

Default storage policy lives in Core database configuration/catalog settings:
**Heap, non-partitioned**. The first SQL surface has no engine clause. LSM and
range bootstrap/import remain supported independently; runtime LSM CREATE and
partition CREATE wait for engine-specific staged-resource crash tests. A SQL
column's nullability defaults per the eventual generic SQL contract (nullable
unless NOT NULL); do not accidentally inherit the manual constructor's false
default. BOOL/INT64/TEXT is the PG-representable first subset; canonical UInt64
and nominal types remain valid in imported/API schemas.

No primary key is required by existing Heap/executor. First SQL CREATE rejects
PRIMARY KEY/UNIQUE (single or composite), foreign keys, defaults, identity,
generated columns, CHECK and ALTER until enforcement exists. Return generic
unsupported (PG maps to 0A000); do not emit misleading PK flags. Future SET NOT
NULL requires validating existing rows, and nominal type changes require typed
compatibility checks plus the same transaction/recovery boundary.

```text
prepare schema snapshot + durable resource intents
              |
stage/init/sync resources + prepare existing/new physical writes
              |
      sync coordinator decision  <--- COMMIT POINT
              |
finish physical commit + install durable catalog root
              |
       durable completion evidence
              |
publish schema + bindings + registry-visible state + generation
              |
    cache/authorization visibility invalidation -> reply
```

All allocations/validation that can reasonably fail must precede decision.
Post-decision I/O/publication failure fences the database from new work and
retains a retry/recovery obligation; it is not reported as a completed rollback.
The single worker simplifies in-memory publication but supplies no durability.

### Failed CREATE / rollback

Before a decision: mark transaction rollback-required on an execution failure,
undo physical DML/prepares, discard overlay, and durably classify owned creation
intents as losers. Close their handles, then clean known files idempotently;
cleanup failure retains the intent for reopen/maintenance and reports the exact
resource. It must not expose the table or delete unknown pre-existing files.
Consumed IDs remain consumed. Disconnect must resolve/report this lifecycle just
as existing session transaction cleanup does.

After a decision or ambiguous decision sync: do not unlink staged resources or
roll back. Retry the same transaction/participant decision or reopen. A lost reply
does not distinguish success from failure; clients must not replay CREATE blindly.

### CREATE crash matrix

| Crash/failure boundary | Required recovery outcome |
| --- | --- |
| Before durable reservation | No logical table, no resource may have used the unreserved identity. |
| After reservation, before intent | Table absent; ID gap is retained. |
| During/after intent, partial file initialization | No decision: table absent; exact known resource is a recoverable loser. |
| Files synced, before schema/physical prepare completes | Table absent; undo existing DML and clean loser staging. |
| All prepares durable, no decision | Presumed abort for catalog and physical participants. |
| Decision append/sync returns uncertain error | Runtime fences rollback/publication; valid durable decision on recovery wins, absence/torn valid tail loses. Complete corrupt record is a hard error. |
| Decision durable, no participant commits yet | Winner: finish every physical commit and install catalog; table fully present. |
| Some physical commits or root installed, before Complete | Same winner; idempotently finish exact remaining participants. |
| Durable completion, before publication/reply | Winner; reopen rebuilds the committed bundle; client outcome may be ambiguous. |
| Committed descriptor points to missing/mismatched storage | Fail database open; never substitute a new empty file. |

Do not require an orphan-free filesystem at every crash boundary. The invariant
is that no **published/committed table** lacks durable recoverable storage, and
all private/retired resources have explicit recoverable ownership. This is
stronger and achievable compared with trying to make several filesystem calls
atomic by error cleanup alone.

## 12. Future DROP and physical retirement

1. Resolve exact active TableId, authorize schema management and validate the
   expected version. Acquire the same schema writer lease; reject unresolved
   dependencies/unsupported cascade before changing anything.
2. Remove the table only from the transaction Schema view. Stage all physical
   StorageIds (Heap/LSM/all range partitions) and local indexes as retired; retain
   full schema and resource descriptors for rollback/old participant recovery.
3. Prepare the complete catalog snapshot. Include any existing transaction writes
   as participants; dropping a written table cannot bypass their decision.
4. Sync the same global decision, finish participants/install root, publish one
   bundle without the TableId, and invalidate prepared/metadata/authorization
   visibility. Do not unlink files before decision.
5. Later maintenance closes/drains handles and removes resources only after
   recovery and retention gates prove no log/transaction/catalog candidate still
   needs them. Delete complete Heap sidecar sets or owned LSM directories; sync
   directory removal and durably record cleanup progress. Retain allocator bounds.

A retired table is unresolvable to new DML/plans/DDL and absent from ordinary
inspection, even while files remain. DROP/recreate allocates new table/storage
IDs and different physical paths. No grant or prepared dependency migrates by
name. Rename later changes lookup metadata, not physical path or TableId.

**Coordinator history is an explicit reclamation gate.** Current coordinator
open validates retained decisions against supplied storage identities, including
completed decisions; v1 has no GC. An early DROP implementation can retire
logically and retain files indefinitely. Before physical unlink, either preserve
sufficient retired participants for current recovery or implement versioned
coordinator checkpoint/retirement evidence proving old decisions no longer need
the files. Do not delete files and then weaken missing-participant validation.
Old snapshots/backups and schema prepare journals impose analogous retention
requirements. Compaction is separate maintenance, not a prerequisite to logical
DROP and not part of Round 16/17.

| DROP crash boundary | Recovery outcome |
| --- | --- |
| Private removal/prepare, no decision | Old table remains active; restore/retain all storage and original grants by identity. |
| Decision uncertain | Fence and resolve by durable decision; never guess from missing/present files. |
| Decision durable, publication incomplete | Finish retirement; table absent from active schema, resource retained. |
| Retirement committed, cleanup not started | Table absent; files are known retired resources. |
| Interrupted deferred unlink | Table absent; resume exact identity-based cleanup only after retention gates. |
| Old file appears after name reuse | Remains old retired identity; never bind it to the newly created table. |

## 13. Prepared invalidation and SDK/session expectations

Choose **dependency-based table validation in the first mutation release**, with
a global schema-generation fast path. Core already exposes logical TableIds and
per-table fingerprints, so a small collected dependency set is justified; no
column-granularity dependency framework is needed initially. Each prepared
statement/DDL stores its owning database token/incarnation, compile generation,
and referenced TableId + table version/fingerprint. Referenced lookup names and
namespace assumptions are part of resolution dependencies. DML assignments,
parameter semantic types, wildcard outputs and result shape all depend on the
table version, even when only one column appears in a query.

```text
prepare using transaction-visible Schema
 -> capture owner + TableId dependencies + logical schema generation
execute / describe / bind portal
 -> validate owner and current transaction Schema view
 -> unchanged generation: fast path (also check overlay revision)
 -> changed: compare every dependency by ID + version/fingerprint
 -> missing/changed dependency: typed stale-prepared error
 -> all dependencies unchanged: bind -> replan using current access paths
```

Choose **explicit stale error, not automatic recompile/rebinding**, because Core
currently does not retain SQL and silent re-resolution of `users` could bind a
new TableId. The caller can explicitly prepare anew. Unrelated new tables do not
invalidate ordinary prepared dependencies. Index-only changes replan as today.
Global invalidate-all would be a valid conservative safety fallback, but is not
the chosen end behavior for ordinary queries. Catalog-reflection statements
naturally depend on the whole catalog revision, including collision-sensitive
OID mappings, so their cache scope is intentionally broader.

Prepared against an overlay also carries the transaction identity and overlay
revision. It may execute only in that transaction while the captured dependencies
remain valid. On commit/rollback those transaction-local prepared handles become
stale; explicitly reprepare against committed state. This avoids reusing a handle
whose table never committed. A portal validates before executing, including if
bound before schema commit; no old field metadata may accompany a different
result shape. First release invalidates suspended/materialized portals on
schema change instead of attempting cross-generation continuation. All row
results are owned today, so this is a compatibility decision, not page pinning.

Required future tests: prepare users, DROP users, CREATE same name, execute old
handle -> stale; same test across column DROP/recreate/rename/type/nullability
changes; wrong database owner -> error; unrelated CREATE -> usable; index CREATE
-> replan; stale PG Describe/Bind/Execute/portal/OID literal -> error, no alias;
overlay commit/abort -> local handle stale; all failures precede writer acquisition.

**SDK expectation contract:** exact required TableId/fingerprint subset remains
the default; additional tables are legal. Generated SDKs remain static views of
known tables, never authors of a second schema catalog. Physical-only SQL columns
do not satisfy an expected nominal type merely because their byte representation
matches. Regeneration is an explicit developer action, not schema recovery.

Handshake checks alone are insufficient after DROP/recreate, particularly for
DML sent as SQL names. Protocol v1 does not send required identities to the
server. Preserve wire v1 and use a conservative future server-side session fence:
retain its advertised table identities; when any advertised table is removed or
changed, finish the committing response/cleanup and invalidate the affected
native session before another ordinary request. Require reconnect/handshake;
do not authorize same-name replacement implicitly. Under the chosen schema-writer
lease there can be no other active user transaction to abandon. A committing
session invalidates after safely completing its transaction. Additive tables can
leave sessions alive, though `ServerInfo` remains a historical snapshot and new
metadata discovery requires reconnect. A future negotiated protocol revision may
carry required dependency sets and schema refresh information for less disruptive
behavior; neither is implemented now. PG prepared/metadata invalidation remains
an adapter to Core dependencies, not a new schema store.

## 14. Authorization and reflection publication policy

Keep logical schema and deployment security responsibility separate. For the
first network table-DDL release introduce an explicit deployment **schema-admin**
capability (default deny), defined to authorize database schema management and
access to all active tables. It is a deliberate broad administrative privilege,
not automatically implied by today's per-table `write`. Trusted embedded Core
calls remain principal-free. Regular principals keep explicit TableId grants;
new tables have **no automatic per-table grants or persisted owner**. The admin
can CREATE then DML in its own overlay under its explicit policy.

DROP is admin-only initially; a future TableId-based drop privilege can be added
without PG roles. Existing grants for a retired TableId become inert and never
apply to a replacement. New manifest policy validation checks IDs against the
recovered active catalog or known retired identities (retired grants may be
reported as inert); unknown IDs remain errors. Current startup's `known_tables`
from manifest cannot remain the validation source after migration.

The atomic publication bundle includes schema-derived authorization visibility,
not an invented durable ACL update: fixed admin policy projects over the new
active Schema, and explicit grants intersect active IDs. No security metadata is
written as a side effect of CREATE/DROP in this first contract. If owner/default
ACL mutation is added later, it needs a separate security catalog participant in
the same decision; do not hide it in a physical Heap header or schema JSON.

`Database::inspect_catalog()` remains active logical schema plus supported index/
placement inspection. Internal high-water, loser/retired files and historical
versions belong only to explicit unstable maintenance inspection. A successful
schema commit updates ordinary inspection immediately and increments the runtime
revision used by existing PG `refresh_catalog`. Authorization filtering runs
against that same view. psql/SQLAlchemy observe updated metadata on the next query
subject to their own client caches (`Inspector.clear_cache()`/new Inspector may
be necessary); NetbaDB cannot invalidate arbitrary external ORM objects.

No persistent OID, `pg_namespace`, regclass, role catalog, PG DDL storage, or
SQLAlchemy migration store is introduced. Runtime-created tables use the existing
projection algorithm plus the session identity/tombstone checks described above. Alembic table migrations
and `create_all()` wait for actual Core DDL/constraints, not catalog fakery.

## 15. Implementation phases and acceptance gates

| Phase | Scope and required proof | Explicitly excluded |
| --- | --- | --- |
| A / recommended Round 17 | **Runtime Schema Catalog Foundation**: versioned bounded snapshot codec; durable database identity and catalog owner/location; bootstrap and explicit legacy migration; preserve TableId/ColumnId/StorageId/PartitionId; allocation high-water and exhaustion states; durable generation/per-table fingerprints; reopen without external authority; inspection from persisted Schema; server/CLI bootstrap-vs-expectation boundary. Import current Heap/LSM/partition compositions, or explicitly fail unsupported imports before mutation. | SQL/core table mutations, file DROP, ALTER, SDK regeneration changes. |
| B / recommended Round 18 | Core transactional nonpartitioned Heap CREATE with private Schema/bindings/resource overlay; quiescent schema writer admission; non-rollback reservations; versioned typed coordinator/schema participant; exact creation intents and resource durability; DDL→DML commit/rollback; prepared owner/dependency validation and native session fencing foundations. Crash-test each decision window, all affected existing DML, uncertain sync, process restart and missing resources. | SQL grammar, LSM/partition runtime CREATE, constraint promises, eager unlink. |
| C / recommended Round 19, only after B gates | Generic SQL CREATE TABLE lowering and explicit admin authorization; BOOL/INT64/TEXT, NULL/NOT NULL, default Heap; native/PG mapping and reflection regression. Core stale checks must protect describe/bind/execute before advertising frontend support. | PRIMARY KEY/UNIQUE until enforced, ALTER, foreign keys, defaults/identity, PG schema namespaces, `create_all()`. |
| D / separate scoped follow-up | Core logical DROP and transaction overlay semantics, retired participant recovery inventory, frontend DROP after crash proof; physical deletion only after coordinator/catalog retention solution. | Unproven log GC or recursive filename-based cleanup. |
| E / evidence-driven | Constraint enforcement, LSM/partition mutation, selected SQLAlchemy/Alembic table workflows after generic behavior exists. | Claiming broad PostgreSQL/Alembic compatibility from a successful demo. |

Phase A must resolve catalog installation/versioning, managed resource ownership,
legacy inventory limits, empty databases, maximum/zero IDs, and forward/downgrade
behavior before format code is accepted. Use snapshot codec round-trip and strict
malformed/truncated/corrupt tests, create-close-reopen, repeated migration and
crashes at every installation boundary. Expectations must never truncate the live
schema; extra persisted tables survive old SDK/bootstrap artifacts. Phase B cannot
start until Phase A can reopen the same logical/physical database independently.

Future tests also need dropped-high-ID restart, rolled-back reservations,
allocator exhaustion, duplicate name/ID rejection, unknown physical type, invalid
PK/NULL metadata, wrong storage owner, missing committed resource, orphan
classification, all-storage rollback, same-name new identity, and deterministic
whole-schema ordering. No decoder or persistent-format surface changes this round,
so no new fuzz seed is required.

## 16. Audit experiments and validation

The initial diagnostic run exposed two incorrect test assumptions: comparing a
nominal RecordId with an unnamed integer literal is rejected by current typing,
and anonymous `create_index` does not increment generation. The final tests use
a valid text predicate and explicitly distinguish anonymous creation from SQL
named-index publication. Production behavior was not changed to fit the audit.

The added [Core lifecycle tests](../crates/netbadb-core/tests/schema_lifecycle.rs)
pin current construction/open behavior, not a pretend runtime catalog: sparse
column identities with real DML/index/reopen; full schema mismatch rejection;
reordered and omitted table composition; persisted Heap/LSM identities; generation
reset with durable index state (and the legacy anonymous-create notification gap); and no-PK Heap behavior. The added manifest test
separates parsing a structurally valid declaration from the later physical-schema
compatibility check. These baselines must be deliberately updated when managed
catalog open replaces external-schema composition; do not preserve legacy
subset-open behavior as the new authority contract.

Executed validation commands/results are recorded in section 18. Logs, corpora and
build outputs live under `/private/tmp`; test databases use the system temporary
directory. None belongs in Git.

## 17. Concrete answer for a future committed `projects` table

For `CREATE TABLE projects (id BIGINT NOT NULL, name TEXT NOT NULL)`, the target
catalog stores the allocated TableId T, ordered ColumnIds C1/C2, physical-only
Int64/Text semantic types, both nullable=false, per-table version/fingerprint,
Heap placement with independently allocated StorageId S, owned resource locator,
and committed SchemaGeneration G+1. IDs are illustrative symbols, not fixed
numbers derived from names or declaration position on reopen.

After complete process exit, the explicit database catalog root and its matching
durable coordinator decision identify the authoritative committed snapshot (or
the prepared winner that recovery must finish). That snapshot supplies the full
TableDef needed to reopen the initialized Heap resource S and validate its
persisted T/fingerprint/physical identity. Recovery finishes winning physical
transactions before publishing schema and bindings together. Without a decision,
the private table is absent and its known files are losers; with a decision,
missing resources are a hard recovery error. IDs never alias an earlier dropped
or aborted table.

The manifest locates/configures the database and verifies expectations; the SDK
verifies its required subset; PostgreSQL derives its catalogs; storage files
validate and implement physical resources. None may independently redefine the
existence, column types, names, nullability or logical identity of `projects`.
This answer depends on implementing the foundation and transaction phases above;
Round 16 itself still rejects table DDL.

## 18. Executed validation and worktree safety

Validation on 2026-08-31 used development Rust 1.97.1. These commands all passed:

```sh
cargo fmt --all -- --check
CARGO_TARGET_DIR=/private/tmp/netbadb-round16-target cargo check --workspace --all-targets --all-features --offline
CARGO_TARGET_DIR=/private/tmp/netbadb-round16-target cargo clippy --workspace --all-targets --all-features --offline -- -D warnings
CARGO_TARGET_DIR=/private/tmp/netbadb-round16-target cargo test --workspace --all-features --offline
```

Workspace testing finished successfully, including all 376 storage tests and
process-crash matrices. The targeted commands also passed independently:

```sh
CARGO_TARGET_DIR=/private/tmp/netbadb-round16-target cargo test -p netbadb-core --test schema_lifecycle --offline
CARGO_TARGET_DIR=/private/tmp/netbadb-round16-target cargo test -p netbadb-server manifest_schema_is_an_expectation_checked_when_storage_opens --offline
```

Seven new Core tests plus one new manifest test passed. Existing partition,
Heap/LSM coordinator, prepared replan, inspection, native protocol and PG suites
ran as part of workspace testing. Full logs are
`/private/tmp/netbadb-round16-{fmt,check,clippy,test}.log`; targeted logs use
`netbadb-round16-targeted-{core,manifest}.log` in the same directory.

**MSRV limitation (pre-existing, not fixed):**

```sh
CARGO_TARGET_DIR=/private/tmp/netbadb-round16-msrv cargo +1.85.0 check -p netbadb-core -p netbadb-server --all-targets --offline
```

This command exits 101 with E0658 at `crates/netbadb-planner/src/lib.rs:892` and
`:904`: let expressions in chained conditions are unstable on 1.85.0. The
unchanged planner blocks compiling the affected Core/server closure; this is not
reported as a passing MSRV check. As requested, no unrelated let-chain fix was
made. The independently buildable schema baseline passes on MSRV:

```sh
CARGO_TARGET_DIR=/private/tmp/netbadb-round16-msrv cargo +1.85.0 test -p netbadb-schema -p netbadb-schema-spec --offline
```

Eight schema tests and two Schema Spec tests passed. Logs:
`/private/tmp/netbadb-round16-msrv.log` and
`/private/tmp/netbadb-round16-msrv-schema.log`.

Real external-client regressions passed against isolated temporary fixture servers:

```sh
NETBADB_PSQL_TARGET_DIR=/private/tmp/netbadb-round16-pg-target python3 scripts/test-postgresql-psql.py
```

The fixture binary was
`/private/tmp/netbadb-round16-pg-target/debug/examples/postgres_driver_fixture`.
Each Python driver script below ran against a fresh fixture's emitted loopback
address, with the fixture shut down via stdin EOF and its successful exit checked:

```sh
/private/tmp/netbadb-round13-pg-venv/bin/python scripts/test-postgresql-orm.py --dsn <fixture-postgresql+psycopg-dsn>
/private/tmp/netbadb-round13-pg-venv/bin/python scripts/test-postgresql-alembic.py --dsn <fixture-postgresql+psycopg-dsn>
```

Versions: psql 17.11, psycopg 3.2.13, SQLAlchemy 2.0.52, Alembic 1.16.5.
The Alembic script remained strictly index-only and ended with zero metadata
differences. No table migration or `create_all()` was run. Logs:
`/private/tmp/netbadb-round16-psql.log` and
`/private/tmp/netbadb-round16-orm-alembic.log`.

Additional unchanged SDK checks passed:

```sh
# From sdk/go:
GOCACHE=/private/tmp/netbadb-round16-go-cache GOPROXY=off go test ./...
# From the repository root:
CARGO_TARGET_DIR=/private/tmp/netbadb-round16-pg-target ./scripts/check-generated-sdk.sh
```

Fuzz smoke: all four targets passed **1,000 runs each**, seed 16, default address
sanitizer, nightly toolchain, offline dependencies. The initial direct invocation
built successfully but cargo-fuzz could not create its default artifact directory
inside the restricted worktree. It was rerun using a copied temporary fuzz project,
with only dependency paths redirected to this worktree. No repository fuzz source
or seed changed. `btree_decode`, `index_catalog_decode`, and `wal_recovery` used
copies of the checked-in corpora. There is no checked-in `pgwire_decode` corpus;
its temporary corpus started empty. Successful invocation pattern:

```sh
CARGO_NET_OFFLINE=true cargo +nightly fuzz run \
  --fuzz-dir /private/tmp/netbadb-round16-fuzz/project \
  --target-dir /private/tmp/netbadb-round16-fuzz/target \
  <target> /private/tmp/netbadb-round16-fuzz/corpus/<target> -- \
  -runs=1000 -seed=16 -max_len=<bound> \
  -artifact_prefix=/private/tmp/netbadb-round16-fuzz/artifacts/<target>/
```

Bounds respectively: 4061, 4060, 2097152, 65536 bytes. Per-target successful logs
are `/private/tmp/netbadb-round16-fuzz/<target>-run.log`. No new decoder/format
surface exists and no new fuzz seeds were added.

Final review checked formatting, `git diff --check`, all 41 local links in this
audit, balanced flow-diagram fences, test-only placement of the server addition,
and absence of production/parser/format/SDK-generated changes.

The task branch is `codex/schema-lifecycle-round16`; worktree is
`/Users/sam/Dev/work/sskycn/netbadb-schema-round16`; base is exactly
`c93caf714f25e6c80fa63c693ec58f061a5ef19f`. The original worktree was observed clean
with local main already at that same SHA, unlike the older branch description
in the request. No remote state was assumed or contacted. No merge, cherry-pick,
push, reset, clean, stash, or restoration of user changes was performed. Original
worktree contents/HEAD remain unchanged; retain this task's branch and worktree
for review. The final response records the resulting local commits.
