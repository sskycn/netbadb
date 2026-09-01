# SchemaMutationJournal v1

This database-level Core journal records post-snapshot identity reservations and
recovery obligations. It is **not a second committed schema catalog**. The
committed NBSC allocator floors plus its retained reservation history form one
allocator, analogous to a checkpoint plus its subsequent log. Winner/loser outcome
comes only from CoordinatorLog. The formats remain experimental.

## Files, activation and bounds

For catalog `C`, `C.mutations` is the journal; `C.mutations.state` is its activation
witness. Both use same-directory `.next` shadows, file sync, rename and directory
sync. A missing pair is valid for a Round 17 database. Initialization first writes
an empty journal, then a durable activation witness, **before any reservation**.
An empty journal left before activation is retryable. A witness without a journal,
a nonempty journal without a witness, or an incarnation mismatch is a hard error.
An incomplete/corrupt active file is never treated as a torn append or ignored.

This first implementation republishes the complete ordered record history
atomically for each change. It does not append partially framed records to the
active file. This intentionally trades O(history) write work for a simple durable
reservation boundary. History is retained, never rolled back or compacted.
The complete journal is bounded to 16 MiB and 65,536 records; strings are at most
4,096 bytes. Exhausting capacity requires future checkpoint/compaction work; it
does not permit discarding reservations. Unknown sync/rename outcome poisons
in-process journal writes until reopen chooses the complete durable image.

All integers are fixed-width unsigned little-endian. Every envelope is:

| Offset | Width | Meaning |
| --- | --- | --- |
| 0 | 4 | Magic |
| 4 | 2 | Version `1` |
| 6 | 2 | Reserved zero |
| 8 | 4 | Payload byte length |
| 12 | 4 | CRC32C over bytes 0..12 followed by payload (checksum field excluded) |
| 16 | length | Payload, no trailing bytes |

`NBSJ` journal payload: incarnation `[u8;16]` (not all zero), coordinator locator
(u32 byte length + UTF-8), record count u32, then records. Each record is a u32
length followed by a complete `NBSR` envelope. Inner and outer checksums both apply.
`NBSA` activation payload is exactly the same 16-byte incarnation.

## Records

Every NBSR payload starts with tag u8 and nonzero DatabaseTxnId u64.

| Tag | Meaning | Remaining payload |
| --- | --- | --- |
| 1 | Reserve | TableId u64, StorageId u64, base SchemaGeneration u64, base snapshot epoch u64 |
| 2 | CreateTableIntent | prepared NBSC SHA-256 `[u8;32]`, single-new-table NBSC v1 fragment |
| 3 | Resolved loser | None |
| 4 | Resolved winner | None |
| 5 | DropTableIntent | target SchemaGeneration u64, target snapshot epoch u64, prepared NBSC SHA-256 `[u8;32]`, exact retired-table NBSC v1 fragment |
| 6 | Retained physical resource | None |
| 7 | Resolved DROP loser | None |
| 8 | Resolved DROP winner | None |

A reservation consumes both IDs together; it is synchronized before intent or
staged files. ColumnIds are deterministic 1..N; the intent persists these exact
IDs and next_column_id=N+1 (1 for a zero-column canonical table). ColumnIds are
table-scoped. PartitionId is not allocated. The fragment contains only the new
TableDef, version 1, Single/Heap placement, StorageId/final locator, target epoch
and generation, allocator floors and coordinator locator. It never repeats every
old table. The complete prepared schema is a separate NBSC v1 artifact, with its
SHA-256 bound by both intent and the coordinator's CORD v2 schema reference.

Replay rejects duplicate transaction/reservation IDs, decreasing TableId/StorageId,
decreasing base epochs/generations, overlapping unresolved schema transactions,
intent without reservation, duplicate intent, resolution without reservation,
duplicate resolution, commit without intent, and intent after resolution. New
columns must be consecutive/nonzero and have no primary-key metadata. New table
version must be 1. Heap placement and reserved IDs/floors must agree. All count,
length, tag, version, fingerprint and arithmetic checks precede indexing/allocation.
An identical in-process resolution retry does not add another durable record.

DROP consumes no reservation and changes no allocator floor. Its fragment contains
exactly the final TableDef/lineage/version/fingerprint, Single Heap placement,
StorageId/kind/relative locator, database incarnation, base generation/epoch,
unchanged allocator floors and coordinator locator. The separately named target
generation/epoch are each the checked successor of the base. `Retained` is durable
terminal physical-lifecycle evidence, not logical schema: tag 8 is invalid before
tag 6, and tag 7 is invalid after it. Replay also rejects duplicate retirement,
one StorageId retired more than once, identity/placement/fingerprint disagreement,
unsupported engine, overlap with another unresolved schema mutation, a later CREATE
reservation below the retained allocator floors, and invalid ordering. Current
readers accept every Round 18 journal; older readers reject the new tags, so
downgrade after Core DROP is unsupported.

## Locators and ownership

Let `R` be `<catalog-filename>.resources-<32 lowercase incarnation hex digits>`.
The format defines these database-relative locators; no table name participates:

- staged Heap: `R/staging/<DatabaseTxnId>/<StorageId>.heap`;
- prepared catalog: `R/staging/<DatabaseTxnId>/catalog.nbsc`;
- final Heap: `R/storage/<StorageId>.heap`;
- optional automatically installed coordinator: `R/coordinator`.

The final and coordinator locators are encoded; staged/prepared locators are
uniquely derived from the versioned rule and persisted identities. Reopen validates
the encoded final locator against that rule. No directory scan discovers work.
Existing explicit coordinator locators are preserved. A database with no prior
coordinator installs a private one for schema 2PC; a successful schema snapshot
records its locator. On a loser, the journal retains the recovery locator, including
for existing-table prepared transactions. Old direct one-writer transactions remain
unchanged until this path is used.

The first staged file is `<Heap>.owner`, envelope `NBST` v1. Its payload is
incarnation 16 bytes, DatabaseTxnId u64, TableId u64, StorageId u64, canonical
SchemaFingerprint 32 bytes (88 total bytes including envelope). It binds the
metadata fields absent from existing Heap metadata. Winner promotion checks it,
Heap metadata identity/fingerprint and the prepared schema digest. Normal reopen
also validates committed owner evidence. CRC/digests detect corruption; they are
not authentication against a malicious writer with full database-directory access.

Heap, WAL, transaction status, and owner components move separately, with both
source and destination parent directories synchronized after each rename. An
optional alternate WAL generation is included if present. Both source and final
copies of one component are a hard conflict; neither is overwritten. Missing
required winner components fail recovery. Partial movement is idempotently finished
from the exact intent. Catalog publication follows complete physical validation.

## Authority and recovery

Reserve does not consume SchemaGeneration. Effective next ID is the maximum of
NBSC's floor and checked successor of every retained reservation. Exhausted None
is absorbing. A committed snapshot checkpoints those effective floors; retained
history still reconciles by maximum. Rollback and crash losers never rewind IDs.
DatabaseTxnId allocation also advances beyond retained reservations and DROP
intents after reopen.

Recovery loads NBSM incarnation, journal and coordinator before requiring an exact
active NBSC/NBSM pair. An unresolved schema decision must name a known intent and
include its staged StorageId in the physical participant set. Winners verify the
prepared SHA-256, promote, recover **all** physical participants using coordinator
resolutions, publish NBSC/state, synchronize Complete, then resolve the journal and
remove prepared artifacts. An unresolved latest publication is completed before
validating older resolved history. Complete never means just the Heap committed.

Without a decision, only exact private components are cleaned; an incomplete Heap
need not be opened. Old physical participants undergo ordinary presumed-abort
recovery. Runtime rollback first durably undoes enlisted participants, then cleans
private components and resolves the journal. Cleanup failure retains RollbackPending
and the intent for retry/reopen, never publishes a table, and never deletes a winner.
For DROP winners, recovery validates the retained Heap/WAL/status identity, recovers
any ordinary physical participants through a recovery-only snapshot, persists tag 6,
publishes the prepared NBSC excluding the table, completes Coordinator and only then
persists tag 8. DROP losers retain the original active resource and schema. Active
NBSC StorageIds must be disjoint from committed retained StorageIds; missing retained
resources are hard errors. Retired fragments never participate in active lookup or
ordinary inspection.

Dropping an unresolved schema handle requires reopen. Unknown/unlisted files are
not garbage-collected. Journal compaction and general aborted-resource GC are deferred.

Deterministic seeds are produced by the ignored Core test
`write_schema_mutation_fuzz_corpus` with an explicit `NETBADB_ROUND18_CORPUS`
output directory. `schema_mutation_decode` fuzzes nested framing and replay;
`coordinator_log_decode` includes schema decision, zero-participant DROP decision,
Complete and truncated v2 seeds. DROP seeds include intent, loser, retained, winner
and truncated records.

An empty journal without its activation witness is an interrupted initialization,
not reservation history. Read-only reopen preserves it; the next mutation completes
and syncs activation before reserving. Admission validates capacity for the complete
CREATE reserve + intent + resolution or DROP intent + retained + resolution
history, so reaching the byte/record bound cannot strand an already admitted winner
or loser. Both transitions have deterministic
regression tests, including a valid 65,534-record history with insufficient room
for another complete obligation.
