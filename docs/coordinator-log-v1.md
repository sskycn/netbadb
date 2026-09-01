# CoordinatorLog format v1

CoordinatorLog is the independent database-level durability object used only
by explicitly coordinator-enabled local databases. It is not stored in a Heap
participant WAL. All integers are unsigned little-endian. The log is
append-only in v1 and has no GC/checkpoint.

## File header

The file begins with exactly 16 bytes:

| Offset | Size | Meaning |
| ---: | ---: | --- |
| 0 | 4 | magic `NBCO` |
| 4 | 2 | format version `1` |
| 6 | 2 | header size `16` |
| 8 | 4 | zero reserved bytes |
| 12 | 4 | CRC32C of all 16 bytes with this field zeroed |

Creation uses create-new semantics, synchronizes the header and file, and
synchronizes the parent directory.

## Record framing

Every record has a 32-byte header followed by its bounded payload:

| Offset | Size | Meaning |
| ---: | ---: | --- |
| 0 | 4 | magic `CORD` |
| 4 | 2 | record version `1` |
| 6 | 1 | tag: `1` CommitDecision, `2` Complete |
| 7 | 1 | zero reserved byte |
| 8 | 4 | total record length |
| 12 | 4 | CRC32C of the complete record with this field zeroed |
| 16 | 8 | nonzero `DatabaseTxnId` |
| 24 | 4 | participant count |
| 28 | 4 | zero reserved bytes |

CommitDecision has between 1 and 1024 participant tuples. Each tuple is 16
bytes: an 8-byte nonzero persistent `StorageId`, then an 8-byte nonzero
storage-local physical `TxnId`. Tuples are encoded in ascending `StorageId`
order and duplicate StorageIds are invalid. Its total record size is
`32 + count × 16`, bounded to 16,416 bytes.

Complete has count zero, no payload, and total length 32. Complete without an
earlier matching CommitDecision is invalid. Repeating the same decision or
Complete is idempotent; the same DatabaseTxnId with a different participant set
is a hard conflict.

## Durability and recovery

The global commit point is successful synchronization of CommitDecision after
every participant Prepare is durable. From that point rollback is forbidden.
Participant Commit records are then synchronized, followed by a synchronized
Complete record. DatabaseTxnIds are allocated above every retained coordinator
decision and every discovered prepared transaction.

Startup scans the complete coordinator log before participant recovery. Bad
magic, unsupported versions, invalid tags or sizes, arithmetic overflow,
invalid identities, duplicate participants, invalid record order, and checksum
failure are hard errors. Only a structurally valid, physically incomplete final
record may be truncated as a crash tail; corruption in a complete or middle
record is never repaired or ignored.

Participant recovery uses presumed abort:

```text
Prepare + no CommitDecision  -> abort and physical undo
Prepare + CommitDecision     -> commit
CommitDecision + partial Commit records -> finish every participant commit
```

The decision tuple must match the Heap metadata StorageId and the prepared WAL
DatabaseTxnId/physical TxnId exactly. Missing or extra participants fail the
whole database open.

## Round 18 schema participant records (CORD v2)

The NBCO file header stays v1. Existing CORD v1 records are still read and written
for storage-only decisions and Complete. CORD **record version 2, tag 3** adds a
schema CommitDecision. The same 32-byte header and 0..1024 sorted physical
participant tuples are followed by exactly 56 bytes. Zero tuples are valid only
for a schema decision, allowing logical DROP with no physical writes; storage-only
CommitDecision still requires at least one participant.

| Width | Meaning |
| --- | --- |
| 16 | Nonzero database incarnation |
| 8 | Nonzero prepared NBSC target epoch |
| 32 | SHA-256 of the complete prepared NBSC bytes |

Total size is `32 + count * 16 + 56` (maximum 16,472). CRC32C covers the whole
record with the checksum field zeroed, as for v1. Version/tag combinations other
than `(1,1)`, `(1,2)`, `(2,3)` are rejected. Repeated decisions must match both the
physical tuple set and the complete schema reference. No full schema is copied
into CoordinatorLog. DatabaseTxnId locates the corresponding reservation/intent
and prepared relative locator in [SchemaMutationJournal v1](schema-mutation-journal-v1.md).

A schema transaction takes this path even when the new Heap is its only physical
writer or has no rows. Core DROP creates no empty physical transaction: a DROP with
no preceding DML has zero tuples, while existing Heap/LSM/partition writes
participate normally.
Storage prepare and prepared NBSC sync precede the sole commit decision. Physical
commits, staged promotion, and NBSC/state publication must all finish before the
Complete record is synchronized. DecisionPending/FinalizePending retain retry-only
semantics; the live committed schema remains old until complete publication.

Old readers correctly reject CORD v2; do not downgrade a database that has used
runtime schema creation. Existing v1-only databases remain readable by Round 18.
