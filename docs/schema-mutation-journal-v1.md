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
| 9 | RetiredHeapGcIntent | coordinator recovery horizon DatabaseTxnId u64, exact bundle SHA-256 `[u8;32]` |
| 10 | RetiredHeapGcComplete | None |
| 11 | HeapRewriteReserve | TableId u64, new StorageId u64, optional ADD ColumnId u32 (`0` means none), base SchemaGeneration u64, base snapshot epoch u64 |
| 12 | HeapRewriteIntent | prepared NBSC SHA-256 `[u8;32]`, bounded typed operation, base-fragment length u32 + NBSC v1 fragment, target-fragment length u32 + NBSC v1 fragment |
| 13 | Replacement Heap retained | None |
| 14 | Resolved rewrite loser | None |
| 15 | Resolved rewrite winner | None |
| 16 | Composition ColumnId reservation | TableId u64, ColumnId u32, optional next ColumnId u32 (`0` means exhausted) |
| 17 | Aggregate schema change-set intent | base/target generation and epoch, action count/digest, prepared NBSC SHA-256, ordered typed per-table base/target fragments |
| 18 | Composition predecessor retained | TableId u64 |
| 19 | Resolved composition loser | None |
| 20 | Resolved composition no-effective-change | None |
| 21 | Resolved composition winner | None |
| 22 | Composition replacement GC intent | TableId u64, coordinator horizon DatabaseTxnId u64, exact bundle SHA-256 |
| 23 | Composition replacement GC complete | TableId u64 |

Tags 9/10 identify the surrounding terminal physical retirement by transaction,
not by cause. They may follow a committed DROP (tags 5/6/8) or a committed Heap
rewrite (tags 11/12/13/15). This is the original tag payload and meaning; Round 25
generalizes replay attachment without changing old DROP bytes or the NBSJ envelope.

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

For a committed retained runtime Heap, tag 9 follows tag 8 and starts a
retry-only physical deletion. The surrounding DROP fragment supplies the exact
identity and generated locator; tag 9 binds the greatest completed coordinator
reference and deterministic component manifest. Tag 10 is invalid without tag
9, duplicate tags are invalid, and tag 9 is invalid before durable retained
winner state. Admission reserves capacity for both records before intent.

A Heap rewrite reservation consumes a fresh StorageId and, only for ADD, one
table-scoped ColumnId. The typed operation encodes only rename table, rename
column, add nullable column, drop column, set/drop NOT NULL, and same-physical
nominal-type change. Its fragments contain the same TableId on distinct old/new
Single Heap StorageIds. Target version, generation and epoch must be checked
successors; target canonical fingerprint must differ; allocator floors and the
optional ADD reservation must agree. Replay reconstructs the target table from the
base plus operation and rejects any mismatch, unsupported physical conversion,
out-of-order retirement/resolution, winner before tag 13, loser after tag 13,
duplicate retirement and truncation. A reservation-only crash may end directly in
tag 14. Current readers accept pre-Round24 histories; older readers reject tags
11–15, so downgrade after Heap rewrite is unsupported.

Composition keeps the same NBSJ v1 envelope. Tag 16 is synchronized at ADD
statement execution and is allocator evidence, not schema authority. Tag 17 is
written at global materialization before any staged file; it contains no SQL and
describes only the final base-to-target recovery plan. Table plans are strictly
ordered by TableId and their new StorageIds are strictly increasing in that same
order. Each plan preserves one TableId, changes to a distinct StorageId, advances
its table version once, and binds canonical base/target fingerprints and Single
Heap fragments. The aggregate advances schema generation and epoch once and is
bounded to 4 MiB inside the existing 16 MiB journal.

Tag 18 is repeated once per table only after its winner target is recovered and
the immutable predecessor is validated. Tag 21 requires every planned predecessor
to be retained. Tags 19 and 20 have no physical winner plan; no-effective-change
may retain earlier tag 16 records but cannot have tag 17. Tags 22/23 attach retry-
only replacement GC to one exact table plan. Replay rejects duplicate transaction,
table, ColumnId, or StorageId ownership, noncanonical order, mismatched fragments,
invalid version/fingerprint transitions, retirement of an unknown plan, winner
without complete retirement, and conflicting terminal outcomes. Tags 1–15 and
their encoding remain unchanged; older readers reject 16–23, so downgrade after
composition is unsupported.

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
NBSC StorageIds must be disjoint from committed retained StorageIds. A missing
Retained resource without tag 9 is a hard error. Tag 9 permits recovery to resume
only its exact component deletion; tag 10 requires every component to remain
absent. A reappeared component is a hard conflict. Retired fragments never
participate in active lookup or ordinary inspection.

For rewrite winners, recovery promotes and resolves the exact staged replacement
participant from the CORD decision, validates the immutable source Heap, persists
tag 13 before NBSC publication, publishes the prepared target and then persists
tag 15. It never recopies rows. Losers remove only exact private replacement
artifacts and preserve the source. The active NBSC may contain the same TableId on
the new StorageId while tag 13 retains the old StorageId; this state is distinct
from DROP retirement and is exposed through a separate inspection API. Physical GC
for replacement retirement is provided by Round 25 tags 9/10.

For composition winners, one CORD v2 schema decision names every final staged
Heap in StorageId order and one prepared NBSC digest. Recovery promotes and
recovers all targets without replaying actions or rescanning sources, retains all
predecessors before publication, publishes the one target NBSC, completes CORD,
and records tag 21. Without a decision it cleans only exact tag-17 staging paths
and records tag 19. Composition predecessors use the same explicit replacement
GC component manifest and coordinator-horizon proof through tags 22/23.

Dropping an unresolved schema handle requires reopen. Unknown/unlisted files are
not garbage-collected. Startup does not choose GC candidates. Journal compaction
and general aborted-resource GC are deferred. See
[Round 22](retired-heap-gc-round22.md) for the retention proof and exact bundle.

Deterministic seeds are produced by the ignored Core test
`write_schema_mutation_fuzz_corpus` with an explicit `NETBADB_ROUND18_CORPUS`
output directory. `schema_mutation_decode` fuzzes nested framing and replay;
`coordinator_log_decode` includes schema decision, zero-participant DROP decision,
Complete and truncated v2 seeds. DROP seeds include intent, loser, retained, winner
and truncated records. Round 24 adds rewrite reservation, intent, loser, winner and
truncated histories to the same bounded target. Round 28 adds composition
reservation, one-table intent, multi-table intent, loser and winner NBSJ seeds plus
a real multi-participant CORD v2 seed.

An empty journal without its activation witness is an interrupted initialization,
not reservation history. Read-only reopen preserves it; the next mutation completes
and syncs activation before reserving. Admission validates capacity for the complete
CREATE reserve + intent + resolution or DROP intent + retained + resolution
history, so reaching the byte/record bound cannot strand an already admitted winner
or loser. Both transitions have deterministic
regression tests, including a valid 65,534-record history with insufficient room
for another complete obligation.
