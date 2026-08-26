# PartitionCatalog v1

PartitionCatalog v1 is the immutable database-level identity and RANGE layout
format used by the embedded/core API. Its path is explicit and is never derived
from a heap path, common parent, current directory, or lexical path order.

## Semantics

- one partition key column per logical table;
- physical type `Int64` or `UInt64` only;
- key is `NOT NULL`;
- every range is `[lower, upper)`;
- lower and upper may be unbounded;
- gaps are legal and route INSERT/UPDATE to typed `NoPartitionForValue` errors;
- empty or overlapping ranges are invalid;
- all physical partitions currently use Heap and own only local indexes.

Identities have distinct meanings:

```text
TableId       logical SQL relation and authorization entity
PartitionId   stable logical physical partition
StorageId     recoverable physical TableStorage instance
```

Paths are not persisted. Open receives schemas and an unordered set of local
paths, reads each heap's persisted table fingerprint and StorageId, validates
the exact catalog set, and only then resolves coordinator recovery.

## Byte layout

All integers are little-endian. The complete file is bounded to 16 MiB.

```text
Header (16 bytes)
  0..4    magic = "NBPC"
  4..6    version = 1 (u16)
  6..8    reserved = 0
  8..12   payload length (u32)
  12..16  CRC32C(payload) (u32)

Payload
  table count (u32, maximum 4096)
  repeated table entries:
    TableId (u64, nonzero)
    canonical schema fingerprint (32 bytes)
    placement tag (u8)

    Single tag 0:
      StorageId (u64, nonzero)

    RangePartitioned tag 1:
      partition key ColumnId (u32)
      key physical type (u8: 1 Int64, 2 UInt64)
      partition count (u32, maximum 1,000,000)
      repeated canonical-range-order entries:
        PartitionId (u64, nonzero)
        StorageId (u64, nonzero)
        lower bound
        upper bound

Bound:
  tag 0: unbounded
  tag 1: exactly 8 bytes in the declared key physical type
```

The decoder requires exact file/payload length, rejects trailing bytes,
unknown tags/types/versions, nonzero reserved bytes, checksum mismatch,
truncation, excessive counts, zero/duplicate identities, wrong bound types,
`lower >= upper`, overlap, and non-canonical ordering. It performs checked
offset arithmetic and allocates only after bounded counts are validated.

## Creation and recovery order

Creation validates the full logical placement before creating physical files,
creates heaps with their final StorageIds, publishes and syncs the immutable
catalog, then creates the independent coordinator log. Failure removes only
files confirmed new in that invocation; pre-existing paths are never removed.

Open follows:

```text
read and validate PartitionCatalog
  → validate logical schemas/fingerprints
  → inspect and resolve every heap by persisted StorageId
  → validate exact physical storage set
  → validate CoordinatorLog decisions/participants
  → resolve prepared WAL transactions
  → expose Database
```

PartitionCatalog does not contain transaction decisions. CoordinatorLog v1,
Heap metadata v5, WAL v4/record v3, Page v5, Protocol v1, Schema Spec v1, and
server manifest v4 are unchanged by this format.

## Deferred

Catalog mutation, partition DDL, attach/detach, split/merge, HASH/LIST/DEFAULT,
multi-column keys, global indexes, heterogeneous storage kinds, placement,
replication, and distributed transactions require later versioned designs.
