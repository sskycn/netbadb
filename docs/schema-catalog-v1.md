# SchemaCatalog v1

Round 17 introduces an experimental database-level full committed snapshot.
This is a typed binary contract, not serde, SQL text, an event log, or a
PostgreSQL catalog. The synchronous implementation is in Core alongside the
existing database coordinator and PartitionCatalog. It depends only on lower
schema/type/storage layers.

## Files and authority

Callers of `Database::create_catalog` or `create_catalog_with_placements` choose
an explicit catalog path. That path names the committed snapshot. Its sibling
`<catalog>.state` is the independent durable installation discriminator.
`<catalog>.next` and `<catalog>.state.next` are shadow write locations. They are
never selected by mtime or trusted by normal open. Initial installation is the
only writer API, private to Core; arbitrary replacement is not exposed.

Each physical storage can have a `<storage>.schema-link` compatibility locator
(with its own `.next` shadow). A link contains only the database incarnation and
a relative path to the catalog. It cannot supply schema, allocation history,
placement, or an initialized decision. Explicit-root reopen does not require
links to be present; any present link must agree with the owning root. Ordinary
legacy-signature wrappers need at least one valid locator, then open the complete
catalog, never just the supplied tables.

The catalog path, marker, shadows, locators, physical storage sidecars and
optional coordinator/partition evidence must have distinct normalized paths.
A catalog or another resource cannot be nested inside an owned LSM directory.
No directory scan discovers or deletes tables. Unknown sibling files are
unclaimed; this catalog is not a filesystem cleanup authority.

## Common envelope

All integers are explicitly little-endian. Every file begins with:

| Offset | Width | Meaning |
| --- | --- | --- |
| 0 | 4 | Magic: `NBSC` snapshot, `NBSM` installation marker, `NBSL` locator |
| 4 | 2 | Format version, exactly 1 |
| 6 | 2 | Reserved, exactly zero |
| 8 | 4 | Payload byte length, exactly file length minus 16 |
| 12 | 4 | CRC32C of bytes 0..12 followed by bytes 16..end |
| 16 | variable | Payload |

The checksum field is excluded from its own CRC. Headers, including version,
length and reserved fields, are covered. Unknown versions fail before payload
interpretation; a fingerprint is never a format version. No trailing bytes,
invalid UTF-8, unknown tags, invalid flags, duplicate identities or inconsistent
cross references are accepted.

The hard file bound is 16 MiB including the envelope. Reads use a bounded stream,
not an unbounded `read_to_end` on a trusted stat result. Count/length arithmetic
is checked before slicing or allocation. Limits are 4,096 tables, 4,096 columns
per table, 65,536 total columns and 65,536 physical storages. Strings are nonempty
UTF-8 with a u32 byte length and at most 4,096 bytes, including semantic names
and locators. These are catalog capacity limits on top of canonical validation;
no new SQL identifier grammar is introduced. Physical lookup validation uses
ordered maps rather than quadratic scans over the maximum storage inventory.

## Snapshot payload (`NBSC`)

Fields appear in this exact order:

| Field | Encoding |
| --- | --- |
| Database incarnation | 16 nonzero-as-a-whole bytes |
| Snapshot epoch | u64, nonzero, initially 1 |
| SchemaGeneration | u64, nonzero, initially 1 |
| next_table_id | u64, zero means exhausted |
| next_storage_id | u64, zero means exhausted |
| next_partition_id | u64, zero means exhausted |
| Table count | u32 |
| Ordered logical tables | Table records below |
| Placement payload byte length | u32 |
| Placement payload | Complete embedded PartitionCatalog v1 bytes |
| Storage descriptor count | u32 |
| Ordered storage descriptors | Records below |
| Optional coordinator locator | Presence byte 0/1; UTF-8 string if present |
| Optional legacy partition evidence locator | Same optional string encoding |

An empty database has zero tables/storages and a zero-length placement payload.
A nonempty database embeds the existing independently versioned `NBPC` format
specified in [PartitionCatalog v1](partition-catalog-v1.md). Its own CRC remains
present, and the outer snapshot CRC covers it too. Placement table entries follow
logical declaration order. The embedded copy is authoritative; a retained
external PartitionCatalog is immutable physical consistency evidence and must
match the embedded entries, independent of evidence-file table order.

Table records:

| Field | Encoding |
| --- | --- |
| TableId | u64, nonzero |
| TableSchemaVersion | u64, nonzero, initially 1 |
| next_column_id | u32, zero means exhausted |
| Table name | UTF-8 string |
| Canonical SchemaFingerprint | 32 bytes |
| Column count | u32 |
| Columns | In logical declaration order |

Column records contain u32 ColumnId, UTF-8 name, one physical tag, optional
semantic name (0/1 byte then string), nullable byte (0/1), primary-key byte (0/1).
Physical tags are 1 Bool, 2 Int64, 3 UInt64, 4 Text. ColumnId zero is legal legacy
identity. Sparse IDs are preserved and never used as vector positions. PK flags
retain their existing descriptive, unenforced meaning.

Decode constructs `ColumnDef`, `TableDef` and an actual validated `Schema`,
recomputes each existing canonical SHA-256 fingerprint and compares all 32 bytes.
Both table and column declaration order are preserved. Table order is observable
through inspection/handshakes; columns additionally determine row layout and the
table fingerprint. No sorting by logical ID occurs during reopen.

Storage descriptors contain u64 StorageId, u64 TableId, relative UTF-8 locator,
then kind byte: 0 Heap; 1 LSM followed by u32 clustering ColumnId. The table's
fingerprint binds every descriptor through TableId. Every storage appears exactly
once in placement, every placement references one existing matching table, and
physical kind/clustering information is checked against persisted physical
identity before WAL recovery. Range placements retain partition key, physical
Int64/UInt64 kind, PartitionIds, StorageIds and typed half-open bounds through
NBPC. Noncanonical order, overlaps, duplicate IDs, nullable/wrong-type keys and
non-Heap range participants are rejected. Range tables require a coordinator.

Locators are relative to the catalog's parent. Absolute paths and NUL bytes are
rejected; `..` is allowed because existing APIs permit explicitly chosen files
in different directories. Moving the complete directory layout preserves reopen.
Moving individual resources requires identity-validated path overrides through
the transition adapters; it does not rewrite the persisted locator. Concurrent
path changes and writable clones are unsupported.

## Installation marker (`NBSM`)

The payload is exactly 32 bytes (48 including envelope):

| Payload offset | Width | Meaning |
| --- | --- | --- |
| 0 | 1 | State: 0 pending explicit bootstrap; 1 catalog initialized |
| 1 | 3 | Reserved zero |
| 4 | 16 | Database incarnation |
| 20 | 8 | Intended/committed snapshot epoch |
| 28 | 4 | CRC32C of the entire encoded snapshot, including its envelope |

A missing marker identifies uninitialized legacy/import-required state, not
permission to trust or reconstruct a catalog. A valid pending marker additionally
pins the exact attempted inventory/snapshot; an explicit retry must match it.
A managed locator whose marker vanished is inconsistent: it cannot authorize
legacy import or mint a new incarnation. A malformed marker is a hard error.
Initialized marker plus missing snapshot is `SchemaCatalogMissing`;
corrupt/unsupported/mismatched snapshots fail without
fallback, even with the correct external expectation.

Incarnation is a nonzero 128-bit OS-random value generated by `getrandom`, reused
on retry and persisted in the marker, snapshot and discovery links. The existing
`DatabaseId(u64)` remains unused; it was not a durable database owner. Current
Heap/LSM formats have no incarnation field, so their cross checks remain TableId,
StorageId, fingerprint and engine metadata. A copied snapshot fails against a
different marker; copying an entire root and matching physical files is an
unsupported writable-clone operation, not an authenticated ownership boundary.
Checksums detect corruption, not malicious file rewriting.

## Discovery locator (`NBSL`)

Payload: 16-byte incarnation, then a u32-length UTF-8 relative catalog path.
No trailing bytes are allowed. Existing links are never silently redirected.
They are discovery/consistency evidence only; the independent marker decides
whether any referenced snapshot may be opened.

## Crash protocol and durability contract

Fresh create prevalidates logical schema, placement, paths and bounded encoding,
then publishes/syncs a **pending** marker before physical creation. Legacy import
first checks the separate complete physical inventory and logical fingerprints;
its pending marker is written before recovery/publication. A pending marker does
not assert that any catalog exists or that partial physical creation completed.

After existing physical create/recovery succeeds:

1. Flush physical participants; sync their parent directories and independent
   coordinator/partition evidence directory entries.
2. Publish compatibility locators by shadow write, file sync, atomic rename,
   parent directory sync.
3. Write the full snapshot to `<catalog>.next`, sync it, rename to the catalog,
   and sync the parent directory.
4. Write an initialized marker to `<catalog>.state.next`, sync it, rename over
   the pending marker, and sync the parent directory.
5. Load and decode that committed snapshot; publish its owned committed Schema
   with the validated registry/bindings. Only then return Database.

There is no initialized-before-catalog window. Before initialized publication,
normal open returns `LegacyCatalogRequired`, even when a complete orphan snapshot
exists. Explicit retry validates the full inventory and original intent before
replacing that orphan. After initialized publication, no bootstrap repair is
allowed. Reopen performs no schema/marker rewrite and changes no persistent
schema generation or table version.

This relies on exclusive process ownership, atomic same-directory rename,
working file fsync and directory fsync on the local filesystem. There is no new
cross-process locking or power-failure simulator. Abrupt child-process tests
exercise partial writes, synced shadows, renamed snapshots, partial/synced
marker shadows, renamed markers, and durable publication. Process exit after
marker rename observes a winner; a power failure before directory sync may
retain either old or new complete marker, each with safe loser/winner semantics.

Physical creation is still the existing static constructor, not transactional
staged table creation. If failure/crash leaves an incomplete physical inventory,
normal open fails and complete-inventory import cannot invent missing files.
Explicit recovery of incomplete physical creation remains operator work; only
catalog installation after complete physical creation has a retry guarantee.

## Identity and compatibility

Bootstrap computes checked successors of complete imported TableId, per-table
ColumnId, StorageId and PartitionId maxima, starting unused domains at 1. Maximal
valid IDs produce explicit exhausted states. Persisted next IDs must exceed every
active ID in their domain; exhausted states can retain past exhaustion after
future removal. Reopen reads the stored states verbatim and never rederives them.
No allocator/reservation or arbitrary schema mutation API is exposed this round.
SchemaCatalog is the only current durable storage/partition high-water source;
registry/physical metadata do not have a second allocator.

SchemaGeneration orders committed logical table/column changes. TableSchemaVersion
orders changes to one table. Snapshot epoch is physical publication identity.
All start at 1 for bootstrap/import and remain unchanged by index operations.
Existing process-local `catalog_generation` starts at 0 on composition and retains
its index-only behavior. Its saturation and anonymous-index notification gap are
documented follow-up work, not silently represented as schema versioning.

Old Heap/LSM/index/partition/coordinator and Protocol v1 bytes are unchanged.
Old uninitialized databases require explicit import, never missing-file fallback.
Old binaries do not enforce the new database marker and are unsupported after
adoption. No CREATE/DROP/ALTER TABLE, schema transaction participant, transaction
schema overlay or physical table cleanup is implemented.

## Runtime publication (Round 18)

NBSC, NBSM and NBSL keep their v1 byte layouts. Runtime creation writes a separate
prepared NBSC and uses the [mutation journal](schema-mutation-journal-v1.md) and
CORD v2 decision to complete physical promotion before replacing active NBSC/state.
An initialized pair mismatch during that winner window is resolved from the exact
prepared digest, never by choosing an old schema or adopting an arbitrary shadow.
Ordinary successful reopen does not rewrite catalog/state or increment generation.
