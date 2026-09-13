# Physical Design Mutation Receipts — NBMR v1

> Historical format. Current binaries migrate valid v1 journals atomically to
> [NBMR v2](physical-design-mutation-receipts-v2.md) before reconciliation and
> write only v2.

NBMR v1 is an optional, bounded, Server-owned receipt journal for explicit
physical-design mutation controls. It observes the existing programmatic and
local-operator Index and Columnar apply paths; it is not a database transaction
participant, WAL, catalog, protocol, or source of mutation authority.

## Configuration and ownership

`ServerPhysicalDesignMutationReceiptConfig::new(path, max_file_bytes)` is the
only configuration entry point. It has no default and is available through the
Native and PostgreSQL server builders. Construction canonicalizes the existing
parent, freezes the absolute final path, rejects an existing non-regular final
object (including a symlink), validates capacity, and creates no file.

The final server configuration must also enable the Physical Design runtime.
At worker startup, after Database and managed-Columnar recovery, the sole
Database worker creates or opens the journal, repairs an incomplete final
record, reconciles one pending receipt, and durably appends its recovered
outcome before readiness. There is no audit thread, mutex, asynchronous buffer,
or shutdown flush. A new Unix journal is created with mode `0600` where the
standard library supports it.

## File header

All integers are little-endian. The 28-byte header is:

| Offset | Bytes | Meaning |
| ---: | ---: | --- |
| 0 | 4 | ASCII `NBMR` |
| 4 | 2 | version `1` |
| 6 | 2 | zero reserved bytes |
| 8 | 16 | durable Schema Catalog incarnation |
| 24 | 4 | CRC32C of bytes 0 through 23 |

The identity is read through Core's pure
`Database::physical_design_database_identity` API. A journal for another
database installation fails startup without changing either database or file.

## Records

Each record is capped at 64 KiB and framed as:

```text
u32 payload_bytes
u8  record_tag       // 1 Begin, 2 Outcome
u8  reserved[3]      // zero
u64 receipt_id       // nonzero, monotonic Begin IDs
u8  body[payload_bytes - 12]
u32 crc32c           // length prefix through final body byte
```

A `Begin` stores a stable source tag, expected evidence epoch, and one bounded
logical target. Index targets contain table ID, column ID, and index name.
Columnar targets contain table ID, ordered column IDs, mode, logical placement
key, and a journal-private UTF-8 absolute recovery path of at most 4096 bytes.
The decoder bounds the column vector before allocation.

An `Outcome` stores one stable coarse result tag: created Index/Columnar,
already-applied Index/Columnar, already covered, rejected, failed, recovered
applied Index/Columnar, recovered not applied, or recovered conflict. Error
strings, Rust discriminants, SQL, principals, sessions, network addresses,
runtime tokens, proposals, and timestamps are never persisted.

## Durability and recovery

Before an apply path performs provenance, policy, current-state, or Core
mutation checks, it appends and syncs `Begin`. Capacity admission reserves room
for both the exact Begin and the largest Outcome. A Begin failure prevents the
mutation and does not burn an in-memory ID.

After the existing apply path returns a definitive result, Server appends and
syncs `Outcome`. If an outcome cannot be made durable after a mutation may have
occurred, the caller receives a typed receipt failure and the journal gates all
later receipt-controlled applies. Server never rolls back physical design to
compensate. Ordinary SQL and sessions remain available.

On restart, a single unresolved Index target is classified with the exact
named-index inspection API. A Columnar target is classified with its original
absolute path, ordered columns, and mode after NBPC recovery. Reconciliation is
read-only and appends one of the recovered outcome tags. An incomplete final
record is truncated to the last complete checksum-valid boundary and synced;
a complete checksum-invalid or structurally invalid record fails closed and is
not truncated.

## Inspection and compatibility

`ServerPhysicalDesignControlHandle::mutation_receipts(after, limit)` returns
receipts in ascending ID order, with a limit from 1 through 128 and a stable
`next_after` cursor. Public Columnar targets expose only the logical placement
key, never the private recovery path or database identity. Reads change neither
journal bytes nor Database state.

NBMR is programmatic-only in this phase. Deployment Manifest v9, NBOP v4,
Native Protocol v2, PostgreSQL wire behavior, Inspection JSON v7, SDK Schema
Spec, `netbadbd`, and the `netbadb operator` command surface are unchanged.
