# Phase 3B: global commit sync pipeline

Phase 3B removes the second foreground coordinator synchronization from pure
authoritative data transactions in global visibility mode. It does not change
the commit point, participant durability, global snapshot ordering, or any
persistent format.

## Commit and publication order

A pure Heap/LSM data transaction follows:

```text
prepare all write participants
    ↓
append Decision(G) + sync                 irreversible commit point
    ↓
durably commit every participant
    ↓
append Complete(G), without sync          recovery checkpoint
    ↓
publish the in-memory boundary vector G
    ↓
return success
```

The successful Decision sync remains the sole database commit point. Every
participant WAL commit is durable before G is published. Readers therefore
observe exactly the same atomic Heap/LSM vector as in Phase 3A; only the
coordinator Complete sync moves off the transaction's foreground path.

Complete means that recovery need not redo participant resolution. It is not
the source of commit authority and no longer gates data publication. Bytes
appended without a sync may already have reached the operating system or
device; inspection reports only what NetbaDB has explicitly synchronized and
does not make claims about incidental physical persistence.

## Sync pipeline and checkpoints

After publishing G, the runtime retains G as a pending Complete checkpoint.
Before it appends the next Decision, it first repairs any missing pending
Complete records. One `sync_data` then makes both the prior Complete records
and the new Decision durable:

```text
Complete(Gn) + Decision(Gn+1) + one sync
```

Thus N sequential data commits perform N foreground coordinator syncs rather
than 2N. The final pending Complete is synchronized by `Database::flush()`,
`Database::checkpoint()`, or clean `Database::close()`. Repeated flushes with
no pending Complete perform no extra coordinator sync.

This is synchronous single-writer pipelining. It is not async commit, group
commit, a background flusher, a timer, or multi-writer coordination. No thread,
Tokio task, or new runtime dependency is involved.

Schema mutation, schema composition, backfill, replacement, index, and catalog
transactions retain the conservative Phase 3A order: Decision sync,
participant/schema finalization, Complete sync, then publication. Their
Complete continues to gate structural publication because those transactions
coordinate more than authoritative row visibility.

LegacyLocal transactions and read-only global transactions are unchanged.

## Failure semantics

- Decision append or sync uncertainty never publishes G. The same transaction
  and sequence must be retried; rollback is forbidden once the decision may be
  durable.
- A participant commit failure after a durable Decision leaves the transaction
  retry-only and unpublished until all participants durably commit.
- A Complete append failure after all participants commit cannot roll back the
  transaction. NetbaDB records the checkpoint error, publishes G, and returns
  success because there is no warning channel. The next write repairs the
  missing Complete before syncing its Decision; if repair fails, that write
  fails closed. Reads remain available.
- An explicit checkpoint or clean close returns an error if it cannot append or
  sync every pending Complete.
- A crash after publication but before Complete sync is recovered from the
  durable Decision and participant WALs. Startup resolves all decisions,
  repairs missing or truncated-tail Complete records in G order, synchronizes
  them, rebuilds the published vector, and only then returns `Database`.
- A checksum-invalid full record remains corruption and is never treated as a
  recoverable unsynchronized tail.

## Inspection and accounting

`Database::inspect_global_visibility()` reports the mode, published and next G,
last sequenced Decision, last appended and explicitly synced Complete, pending
Complete count, last checkpoint error, coordinator bytes, and runtime counts
for Decision syncs, explicit checkpoint syncs, and Decision syncs that also
drained the pipeline.

Counters start at zero when a database process opens. They describe sync calls
made by that live `Database`, not historical records inferred from disk.

## Compatibility and scope

NBCO remains file-header version 1 and uses the existing CORD v3 tags 4–7.
There is no on-disk migration. Protocol v1, query results, SDK schema formats,
Heap/WAL formats, LSM formats, and Columnar authority are unchanged.

The `global_commit_pipeline_phase3b` benchmark records single-transaction Heap
and LSM commits at 1/100/1000 rows, a Heap+LSM transaction, and 10/100/1000
sequential commits for each engine. Output includes transaction count, rows per
transaction, mean and total foreground time, coordinator bytes, sync counters,
published G, and authoritative WAL bytes. Timings are observations, not test
assertions.

One local run on 2026-09-07 produced:

| Scenario | Engine | Transactions | Rows/txn | Mean commit | Decision syncs | Combined syncs | Close checkpoint syncs |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| single | Heap | 1 | 1 | 18.04 ms | 1 | 0 | 1 |
| single | Heap | 1 | 100 | 34.00 ms | 1 | 0 | 1 |
| single | Heap | 1 | 1000 | 3568.15 ms | 1 | 0 | 1 |
| single | LSM | 1 | 1 | 11.83 ms | 1 | 0 | 1 |
| single | LSM | 1 | 100 | 11.00 ms | 1 | 0 | 1 |
| single | LSM | 1 | 1000 | 14.56 ms | 1 | 0 | 1 |
| sequential | Heap | 10 / 100 / 1000 | 1 | 14.11 / 14.54 / 17.31 ms | 10 / 100 / 1000 | 9 / 99 / 999 | 1 |
| sequential | LSM | 10 / 100 / 1000 | 1 | 10.40 / 11.09 / 11.12 ms | 10 / 100 / 1000 | 9 / 99 / 999 | 1 |
| single | Heap+LSM | 1 | 200 | 43.29 ms | 1 | 0 | 1 |

The previously recorded Phase 3A one-row figures were about 17.07 ms for Heap
and 13.96 ms for LSM. They are historical observations, not a controlled paired
run: this Phase 3B run measured Heap at 18.04 ms and LSM at 11.83 ms on the
current environment. The structural result is the sync count—N sequential
commits use N Decision syncs plus one close checkpoint—not a promised latency
ratio.

Phase 3B originally deferred coordinator-log compaction. Phase 3B.5 now adds
explicit checkpoint compaction without changing this pipeline. Persistent
historical G vectors, group commit, async commit, concurrent coordinator
writers, replication, and distributed consensus remain deferred.
