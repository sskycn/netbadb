# Projection Catalog NBPC v2

NBPC v2 is the durable inventory, identity allocator, and one-build intent
authority for managed Columnar projections. It remains independent from the
authoritative Schema Catalog and from NBCM/NBCS/NBCD. The catalog file is
`<schema-catalog>.projections`; its publication marker is
`<schema-catalog>.projections.state`.

All integers are little-endian. Rust layout and enum discriminants are never
persisted. The catalog bytes are:

| Offset | Width | Field |
| ---: | ---: | --- |
| 0 | 4 | magic `NBPC` |
| 4 | 2 | version `2` |
| 6 | 2 | zero reserved |
| 8 | 16 | Schema Catalog incarnation |
| 24 | 8 | next `ColumnarProjectionId`; zero means exhausted |
| 32 | 4 | active-entry count |
| 36 | 1 | pending-present flag, exactly `0` or `1` |
| 37 | 3 | zero reserved |
| 40 | variable | active entries in ascending projection-ID order |
| after entries | variable | optional pending build intent |
| final 4 | 4 | CRC32C of every preceding catalog byte |

Each active entry encodes `ProjectionId:u64`, `TableId:u64`, `StorageId:u64`,
`ColumnarGeneration:u64`, the 32-byte `SchemaFingerprint`, then a UTF-8 relative
locator as `length:u32 + bytes`.

The optional pending intent encodes, in order:

```text
ProjectionId:u64
TableId:u64
StorageId:u64
ColumnarGeneration:u64
SchemaFingerprint:[u8;32]
mode:u8                    # 1 = snapshot, 2 = incremental
reserved:[u8;3]            # zero
column_count:u32
ordered ColumnId:u32[column_count]
relative locator           # length:u32 + UTF-8 bytes
```

The exact caller-requested column order is identity. It is neither sorted nor
set-normalized. The intent contains no rows, source snapshot payload, SQL,
advisor evidence, candidate ranking, or separate request ID.

The decoder reads at most 64 MiB, at most 1,048,576 active entries, at most
1,048,576 pending columns, and at most 4,096 locator bytes. It rejects unknown
mode tags before constructing an intent, oversized counts before allocation,
truncation, bad UTF-8, trailing bytes, nonzero reserved bytes, checksum errors,
zero required identities/generation, empty or absolute/NUL-containing locators,
empty pending columns, duplicate active identities/locators, and active/pending
identity or locator collisions. `next_id` must be exhausted or strictly greater
than every active and pending ID.

The NBPM v2 marker encodes `NBPM`, version/reserved, the same incarnation,
`next_id`, the CRC32C of the complete catalog file, and its own trailing CRC32C.
Catalog and marker publication reuse the same-directory shadow, complete write,
file sync, atomic rename, and parent-directory sync machinery.

## State transitions

`begin_build` requires no pending intent. It selects the exact current next ID,
advances the high-water, installs the complete intent, and publishes that one
catalog snapshot. A successfully durable intent permanently burns its ID even
if the build is later abandoned.

NBC preparation and final artifact publication do not make the projection
active. `commit_build` compares the artifact's ID, table, source storage,
generation, fingerprint, relative locator, mode, and ordered columns with the
pending intent. One catalog publication removes the pending intent and appends
the exact active entry. The in-memory registry is updated only after that
publication succeeds. `abort_build` removes only the exact pending ID and never
rewinds the allocator or removes an active entry.

## Open recovery

Open resolves v2 pending state before constructing the planner-visible registry:

- no final manifest: storage removes only exact ID+generation temporary files
  and the exact final base segment, retains the caller directory and unknown
  files, then Core durably aborts the intent without reusing the ID;
- a complete final manifest: storage opens and validates every referenced file,
  then Core performs the exact-match pending-to-active transition using the same
  ID;
- a present but corrupt, incomplete, or mismatched artifact: open fails closed
  without deleting, adopting, overwriting, or rebuilding anything.

Cleanup is idempotent, nonrecursive, never removes the placement directory, and
is never invoked while a final manifest exists. Recovery reads only the exact
durable locator; there is no directory walk or orphan adoption.

An error returned by artifact publication or pending-to-active catalog
publication is ambiguous about crash-surviving filesystem state. The current
process retains durable pending authority, reports `RecoveryRequired`, and
blocks build, attach, refresh, advance, compact, drop, and Change Stream GC until
reopen resolves the state. Existing already-active projections may remain
readable, but the ambiguous build is never inserted into the registry.

## V1 migration

Current code accepts NBPC/NBPM v1 only as migration input. It decodes the v1
incarnation, exact next-ID high-water, and active entries; assigns
`pending_build = None`; and atomically rewrites v2 without touching Schema
Generation, DatabaseCommitSeq, NBCM/NBCS/NBCD, projection generations, or
locators. A v2 catalog with a surviving v1 marker is accepted only after the
existing incarnation/high-water checks, then the marker is repaired to v2. A
marker ahead of the catalog is still rejected.

Because v1 stored no build intent, a valid unregistered artifact below its
high-water is not evidence of ownership. Migration never scans for or adopts
historical orphans. Old binaries are not required to read v2.
