# Columnar Phase 2A: durable per-storage change stream

Phase 2A adds a synchronous, durable stream of committed logical row changes
owned by each authoritative Heap or LSM `StorageId`. It is infrastructure for a
future Columnar Delta. It is not a network CDC product, a replication protocol,
or a Columnar consumer.

```text
Heap / LSM
    │
transaction net-change accumulator
    │
durable NBCL prepared batch + sync
    │
authoritative commit record + sync
    │
durable NBCL finalize marker + sync
    │
committed per-storage change stream
```

The change log is a subordinate file of its storage participant. It is never a
Core coordinator participant and does not create a third participant beside
Heap/LSM. A coordinated transaction produces one batch in each written
storage. Those batches carry the same `DatabaseTxnId` for correlation.
`DatabaseTxnId` **is not a global commit sequence** and its numeric order does
not define a database snapshot or cross-storage replay order.

## Frontier and stream identity

`StorageDataVersion` is distinct from the Phase 1 `StorageSnapshotToken`.
Snapshot tokens answer projection freshness equality. In particular, the LSM
token contains a WAL generation that can change during flush. A change-stream
frontier instead advances exactly once for each committed transaction with a
nonempty net logical row change. Rollback, read-only commit, checkpoint,
vacuum, ANALYZE, LSM flush, and compaction do not advance it.

Enabling a stream creates a new durable `ChangeStreamGeneration` and defines
the current authoritative state as frontier F0. Batches form an explicit chain:

```text
F0 --batch 1--> F1 --batch 2--> F2
```

The reader verifies every `before`/`after` link. Record offsets and sequence
numbers are diagnostic ordering aids and are not continuity proofs. A cursor
contains `StorageId`, stream generation, and frontier. A cursor from an old
incarnation is rejected after disable/re-enable. A `StorageDataVersion` from
storage A **is not comparable with** one from storage B; the API checks the
storage context before reading.

`TableStorage::committed_read_anchor` returns one pinned engine read view and
the matching stream cursor. The synchronous storage objects and mutable writer
lease prevent a commit from interleaving between those captures. A later
commit is absent from the pinned view and present in `read_changes` after the
anchor cursor.

## Version identity and transaction changes

`StorageVersionKey` identifies one committed physical version in its exact
storage context:

- Heap stores the full generation-safe `RowId` (`PageId`, slot, generation).
- LSM stores `LsmRowId` plus `LsmCommitSeq`.

An LSM clustering-key move remains one logical Update from the old committed
version to the new committed version. Internal Tombstone and Put records do
not escape the storage boundary. Heap relocation similarly records the exact
old and new generation-bearing row locators. Vacuum may reclaim the old tuple;
the change log retains its value identity.

`StorageVersionKey` **is not `RowEntityId`** and does not survive a storage
replacement or layout migration. Phase 2A deliberately does not introduce a
cross-layout logical row identity.

The public mutation model is:

```text
Insert { new_version, full after-image }
Update { old_version, new_version, full after-image }
Delete { old_version }
```

After-images use the existing canonical physical row codec in table column
order. Every batch binds `TableId`, `StorageId`, and `SchemaFingerprint`.
Malformed rows and mismatched identities are domain errors.

Heap accumulates changes alongside its physical transaction. LSM derives the
same logical set from its existing per-row pending map. Both expose only the
net transaction effect:

| statement chain | committed mutation |
| --- | --- |
| Insert → Update | Insert with the final version and row |
| Insert → Delete | none |
| Update → Update | Update from the original to final version |
| Update → Delete | Delete of the original version |

Coalescing never crosses a commit frontier.

## NBCL v1 durability and recovery

Heap stores `<heap>.change`; LSM stores `<lsm-directory>/change.nbcl`. While a
stream is active, a small `<change-log>.active` guard duplicates the checksummed
identity header. It lets startup distinguish a never-enabled stream from a
missing active log; explicit disable removes the guard. NBCL v1 uses explicit
little-endian fields and CRC32C for its fixed header and every record. The
header contains magic `NBCL`, version 1, active flag, engine kind, storage/table
identities, schema fingerprint, stream generation, and baseline.

A prepared-batch record repeats the table/storage/fingerprint context and
contains a physical `TxnId`, optional `DatabaseTxnId`, diagnostic sequence,
before/after frontiers, bounded mutation count, version keys, and bounded
canonical row payloads. A finalize record contains the physical transaction
identity and the LSM commit version when applicable.

The decoder limits a record to 64 MiB, a row to 16 MiB, and a batch to one
million mutations. It uses checked offsets and rejects bad magic/version,
header or record checksums, truncation outside the recoverable final tail,
unknown tags, zero identities, bad generations/versions, excessive lengths,
schema/context mismatch, duplicate transaction identities, trailing bytes, and
broken frontier chains.

Commit ordering is:

1. Produce the final net change set.
2. Append and synchronize the complete prepared NBCL batch.
3. Append and synchronize the authoritative Heap or LSM commit record.
4. Publish the authoritative in-memory state.
5. Append and synchronize the NBCL finalize marker.

The first sync ensures a committed authoritative transition cannot lack
recoverable change payload. If a crash follows the authoritative commit and
precedes a complete finalize marker, startup truncates only the incomplete
final NBCL tail, consults the authoritative transaction outcome, resolves an
LSM pending new-version placeholder from its commit record, and writes the
finalize marker again. A complete checksum-corrupt record is never truncated or
silently repaired. Finalization is keyed by physical `TxnId`; duplicate
publication is rejected.

Heap recovery consults the durable transaction-status sidecar after WAL
reconciliation. LSM recovery resolves prepared coordinator participants and
their commit version before opening NBCL. Durable finalize markers retain the
outcome after LSM flush rotates old WAL generations. Aborted or incomplete
prepared batches are never exposed.

An enabled stream adds one prepared append/sync and one finalize append/sync per
committed changing transaction. It does not add a coordinator sync. A stream
that has never been enabled does not accumulate or encode full row images and
creates no NBCL or guard file. Explicit disable retains only the inactive
identity header.

## Management, reads, and inspection

Core exposes embedded/admin methods for exactly placed tables:

- `enable_change_stream` and `disable_change_stream`;
- `committed_read_anchor`;
- bounded `read_changes(cursor, max_batches, max_bytes)`;
- `inspect_change_stream`.

Enable/disable require a quiescent storage transaction boundary. Disable is an
explicit abandonment of the current history. Re-enable starts a new generation
at F0. There is no automatic disable or repair.

If an active log is missing, corrupt, or unavailable, authoritative storage
still opens and reads remain usable. A changing Heap operation is rejected
before physical mutation; LSM rejects before its authoritative commit. The
administrator may explicitly disable the stream to abandon it and resume
writes. Schema or storage identity mismatch likewise requires rebaseline; an
old stream never attaches to a replacement storage or same-name table.

Reads return committed batches, the current frontier, and `has_more`. They
reject a wrong storage, wrong incarnation, unavailable history, and a broken
frontier chain. History is append-only in Phase 2A. There is no retention GC,
consumer acknowledgement, or overwrite-at-limit policy.

Inspection reports status, identities, fingerprint, baseline/current/earliest
frontier, committed batch and mutation counts, file bytes, unresolved prepared
count, and the last availability error. Batch inspection contains identities,
frontiers, sequence, and mutation count without dumping row payloads by
default.

## Phase 2B boundary

Phase 2A stops at a replayable committed source transition. It does not change
NBCM, NBCS, or NBPC and does not implement Columnar segment version identity,
Columnar Delta, Base+Delta merge, incremental projection maintenance,
retention/acknowledgement GC, a database-global CSN, `RowEntityId`, hybrid
authoritative storage, network CDC, or a background worker.

The `change_stream_phase2a` benchmark compares Heap and LSM with the stream
disabled/enabled for one-, 100-, and 1,000-row insert/update/delete
transactions. It emits elapsed nanoseconds, committed batches/mutations, NBCL
bytes, authoritative WAL bytes, and the two NBCL syncs per enabled changing
transaction. Timings are observations and have no test threshold.
