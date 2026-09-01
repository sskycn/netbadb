# Replacement-retired Heap GC — Round 25

Round 25 closes the physical lifecycle of the old Heap produced by a committed
Round 24 schema rewrite. It reuses the Round 22 exact deletion engine and adds no
SQL, automatic candidate selection, batch GC, or persistent format.

## Round 22 abstraction audit

The physical proof was already cause-independent:

- exact StorageId, generated database-relative locator, Heap identity and owner;
- exact owner, Heap, WAL, transaction-status, optional alternate WAL, catalog
  link and optional link-shadow manifest;
- regular-file and no-symlink validation for every ancestor and leaf;
- database-handle and schema-writer quiescence;
- recovery-only Heap open and terminal prepared-transaction resolution;
- durable tag 9 before unlink, retry-only deletion, parent-directory sync, and
  durable tag 10 last;
- Retained/Deleting/Deleted startup interpretation.

The DROP-specific policy was confined to the in-memory token and journal
attachment: GC looked only in `DropIntent`, required DROP winner evidence, found
the original create only in create reservations, and open always required every
replacement source to remain present. Those are not physical safety invariants.
The correct cause-specific logical rules are:

```text
TableDrop:     TableId is absent and StorageId is absent everywhere active
SchemaRewrite: old StorageId is absent everywhere active; TableId may be active
               on a durable successor StorageId or may later be DROP-retired
```

## Unified identity and cause

`RetiredHeapGcTarget` is a typed cause enum:

```text
TableDrop(RetiredTableResource)
SchemaRewrite(ReplacementRetiredHeap)
```

Both variants resolve through durable NBSJ inventory to one internal retired
Heap identity. Cause never selects paths. Physical deletion always targets the
exact database incarnation, TableId, TableSchemaVersion, fingerprint, StorageId,
generated locator and old `TableDef`. A modified token is rejected.

The replacement variant additionally carries the immediate successor version,
fingerprint and StorageId plus the rewrite DatabaseTxnId. It is never converted
to a DROP and GC never changes grants, schema, indexes, statistics, runtime
revision, SchemaGeneration, TableSchemaVersion, or allocator floors.

## Replacement lineage and active exclusion

For a rewrite:

```text
T/V1/F1/S1 -> T/V2/F2/S2
```

eligibility validates the exact base and target fragments and then follows
durable later rewrite records by exact successor identity. Therefore:

```text
S1 -> S2 -> S3 -> S4 active
```

allows S1, S2 and S3 to be collected independently in any order. The terminal
successor must be either the exact current active `(T,V,F,S)` or the exact source
of a later committed DROP. The immediate successor file need not exist. NBSJ
history, not StorageId ordering, timestamps, names, or file presence, proves the
chain. A same-name table with a different TableId is unrelated.

Active exclusion compares StorageId in all three active views: NBSC inventory,
StorageRegistry and physical bindings. Reappearance of an old StorageId is hard
corruption. The same TableId on a different StorageId is the expected rewrite
state. Replay also rejects duplicate and cross-cause retirement claims for one
StorageId.

## Coordinator horizon and rewrite terminal proof

For any retired Heap `R`:

```text
H(R) = max(
    R.retirement_transaction,
    every CORD decision whose participant references R.storage_id
)
```

Every included decision must have durable `Complete`. For replacement retirement,
the rewrite transaction is included even though S1 was a read-only source and the
CORD participant tuple normally names only S2. The rewrite decision must contain
the exact schema reference and be Complete; NBSJ must independently contain the
rewrite intent, replacement-retirement tag 13 and winner tag 15. Missing or
unresolved evidence is rejected during replay/open before a candidate can be
inspected.

Later rewrite recovery references only its immediate source. Deleted ancestors
are interpreted through terminal NBSJ GC history and are never reopened or
recopied. This is covered by ALTER -> GC -> ALTER and chained-rewrite tests.

## Eligibility and state machine

The implemented eligibility theorem is:

```text
durable cause-specific retirement terminal
AND exact active StorageId exclusion
AND valid DROP or transitive replacement lineage
AND complete retirement decision and participant horizon
AND no unresolved schema writer or database transaction handle
AND generated runtime-Heap locator and exact manifest
AND matching owner, TableId, StorageId and fingerprint
AND terminal Heap WAL/transaction-status recovery
```

The state machine remains:

```text
Retained -> Deleting -> Deleted
```

Inspection is read-only. Retained missing required files without intent is hard
corruption. Once tag 9 is durable, startup resumes only that exact deletion and
partial absence is authorized. Before deleting any still-present Heap after an
interruption, recovery revalidates owner and Heap identity, so a different valid
Heap placed at the old path is refused. Deleted plus any reappeared component is
hard corruption. Repeated GC returns Deleted without appending records.

## NBSJ and recovery compatibility

NBSJ v1 tags 9 and 10 are unchanged byte-for-byte:

```text
9  retirement transaction, coordinator horizon, exact manifest SHA-256
10 retirement transaction
```

Replay now attaches them to the terminal DROP or rewrite retirement with that
transaction. Existing DROP record order and bytes are unchanged. Rewrite GC emits
tags 9/10 after rewrite winner tag 15. Startup resumes Deleting records for both
causes before validating historical resources, but never selects a Retained
candidate.

Completed rewrite sources remain in NBSJ as `SchemaRewrite + Deleted`; logical
catalog inspection continues to expose active schema only. NBSJ and CORD
compaction remain deferred.

## Exact bundle and path safety

The shared manifest contains, in order, the Core owner file, storage-authoritative
Heap main/WAL/transaction-status/optional alternate WAL files, Core catalog link,
and optional atomic link shadow. The Heap main file includes its historical
IndexCatalog and BTree pages. Deletion uses no glob, recursive scan, table name or
replacement-specific filesystem function.

Only the runtime locator
`<catalog>.resources-<incarnation>/storage/<StorageId>.heap` is eligible. Absolute
paths, `..`, non-UTF-8/escaping manifest paths, symlink ancestors/leaves,
directories, imported/bootstrap locations and unsupported engines fail closed.

## Acceptance measurements

The deterministic index-heavy fixture retained 128 wide rows and two BTree
indexes. GC removed 5 present files and 4,106,161 bytes; active Heap bytes,
IndexIds, point/range/join plans and results were unchanged.

The deterministic 100 rewrite + immediate GC fixture reported:

```text
TableId                         3 (constant)
TableSchemaVersion             101
initial / active StorageId     3 / 103
first retired bundle           21,001 bytes
all retired bytes deleted      2,904,772 bytes
retained replacement bytes     0
active Heap bundle             28,992 bytes
NBSJ                           545 -> 99,939 bytes
CORD                           152 -> 13,752 bytes
```

The byte counts describe this fixture, not a fixed format size. Physical retired
Heap growth is closed; append-only NBSJ/CORD metadata growth is not.

## Persistent-format matrix

Round 25 changes no persistent or wire format:

| Component | Version/status |
| --- | --- |
| NBSC | v1 unchanged |
| NBSM | v1 unchanged |
| NBSJ | v1, tags 1–15 unchanged |
| Heap metadata | v5 unchanged |
| Page | v5 unchanged |
| IndexCatalog | v9 unchanged |
| BTree | v1/v2/v3 unchanged |
| WAL | container v4; record v3/v4/v5 unchanged |
| transaction status | v1 unchanged |
| CORD | v2 unchanged |
| native Protocol | v1 unchanged |
| PostgreSQL wire | unchanged |

## Deferred

Automatic/background and batch GC, LSM/partition/imported/bootstrap GC, unknown
orphan GC, StorageId or locator reuse, NBSJ/CORD compaction, cross-process writers,
SQL ALTER TABLE, physical type conversion and online ALTER remain unsupported.
