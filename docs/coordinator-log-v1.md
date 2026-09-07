# CoordinatorLog format v1

CoordinatorLog is the independent database-level durability object used only
by explicitly coordinator-enabled local databases. It is not stored in a Heap
participant WAL. All integers are unsigned little-endian. Normal operation is
append-only. Phase 3B.5 adds an explicit rewrite into a bounded checkpoint
representation without changing the NBCO v1 file header.

Round 38 startup may resolve a schema winner absent from the old active NBSC
only through exact typed NBSJ stage/final authority and an exact CORD StorageId
plus physical TxnId match. Both source and target are winners and must finish
before final schema publication. Missing unexplained participants, rolled-back
participants, and identity mismatches remain hard errors, and client admission
waits for the whole decision to converge.

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

## Phase 3A global visibility records (CORD v3)

The NBCO file header remains v1. Phase 3A adds these record-version-3 tags:

| Tag | Meaning | Payload before participants |
| ---: | --- | --- |
| 4 | `GlobalEnable` | none; header transaction ID and count are zero |
| 5 | sequenced data CommitDecision | nonzero 8-byte `DatabaseCommitSeq` |
| 6 | sequenced schema CommitDecision | nonzero 8-byte `DatabaseCommitSeq` |
| 7 | sequenced Complete | nonzero 8-byte `DatabaseCommitSeq` |

`GlobalEnable` is synchronized as an append-only one-way mode transition. A
sequenced record before it is invalid. Tags 5 and 6 retain the canonical
participant tuples after the sequence; tag 6 then retains the same 56-byte
schema reference as CORD v2. Tag 7 has no participants and must match both the
transaction and sequence of an earlier decision.

Sequenced decisions are consecutive beginning at G1. Complete records are
gap-free: G(n+1) cannot Complete while an earlier G remains incomplete.
Phase 3A used successful Complete synchronization as the durable
database-snapshot publication point. Startup finishes incomplete decided
transactions in G order and reconstructs the current storage visibility vector
before admitting reads. See
[Phase 3A global snapshots](phase3a-global-snapshot.md).

## Phase 3B synchronization semantics (same CORD v3 bytes)

Phase 3B changes no record layout or decoder rule. For pure authoritative data
transactions, the synced sequenced Decision remains the irreversible commit
point and participant commits remain durable before publication. The sequenced
Complete is appended without an immediate sync and serves as a recovery
checkpoint. The next Decision sync also makes earlier appended Completes
durable; flush, checkpoint, and clean close synchronize the final pending
Complete. Missing final Completes and valid partial final records are repaired
during recovery. Fully present checksum-invalid records remain hard errors.

Schema, composition, backfill, replacement, index, and catalog transactions
retain immediate Complete synchronization before structural publication. See
[Phase 3B global commit sync pipeline](phase3b-global-commit-pipeline.md).

## Phase 3B.5 coordinator checkpoint (CORD v4)

CORD **record version 4, tag 8** is `CoordinatorCheckpoint`. Its record header
has transaction ID zero, participant count zero, and the existing reserved and
CRC32C rules. Its fixed 24-byte payload contains three little-endian u64 values:

| Width | Meaning |
| ---: | --- |
| 8 | nonzero published `DatabaseCommitSeq` |
| 8 | nonzero last sequenced decision; equal to published G in this version |
| 8 | nonzero `DatabaseTxnId` high-water |

A checkpoint is legal only after one explicit CORD v3 `GlobalEnable`, before
all tail decisions, and at most once per file. A following sequenced decision
must be exactly checkpoint G + 1 and must use a `DatabaseTxnId` above the
checkpoint high-water. Unsequenced records, duplicate checkpoints, malformed
high-waters, gaps, checksum failures, and truncated checkpoint records are hard
errors. A checkpoint is not treated as a truncatable append tail.

The compacted file is exactly `NBCO header + GlobalEnable + checkpoint`.
Opening old files does not rewrite them. Opening a checkpointed file rebuilds
published G and next G from the checkpoint, keeps only post-checkpoint
decisions in the runtime map, and rebuilds the current visibility vector from
authoritative Heap/LSM boundaries. Terminal physical participants carry their
own durable outcome; only an actually Prepared participant requires an exact
tail decision. A Prepared participant at or below the compacted transaction
high-water is a fail-closed inconsistency.

Explicit compaction writes and synchronizes `<coordinator>.next`, releases the
old long-lived file handle, replaces the primary path, synchronizes the parent
directory, reopens and validates the new primary, then replaces in-memory
state. The primary path is always the sole recovery authority. An orphan
`.next` is ignored, including when the primary is corrupt. Failures before
replacement leave the old authority usable; uncertainty after replacement
leaves the live coordinator handle unavailable so global writes fail closed
until an explicit reopen.

The first version compacts only a fully completed data-only prefix through the
current published frontier. LegacyLocal mode, outstanding transaction handles,
unresolved storage recovery, and any retained schema/structural decision reject
the operation. This conservative structural rule preserves NBSJ/NBSC recovery
and retired-resource evidence without pretending the checkpoint contains it.
