# Disjoint Write Domains and Partitioned Single-Writer Architecture

Status: accepted architecture policy; runtime concurrency implementation is
intentionally deferred.

## Decision

NetbaDB uses a partitioned single-writer architecture.

The database is not conceptually limited to one active writer globally.
Instead, authoritative mutation is divided into independent mutation domains,
whose default physical identity is `StorageId`.

Each `StorageId` has at most one active mutation owner. Multiple transactions
may eventually execute writes concurrently when their mutation-domain sets are
disjoint. Transactions whose domains overlap must serialize mutation of every
overlapping domain.

```text
An authoritative mutation domain has at most one active mutation owner.

Independent mutation domains may execute in parallel when their ownership
sets are disjoint.

Concurrency comes from partitioning authoritative state into independent
domains, not from allowing multiple writers to mutate one domain.
```

The short form is:

> Parallelize independent state; serialize shared state.

The performance form is:

```text
Serialize mutation within a domain.
Parallelize disjoint domains.
Batch durability.
Preserve ordered visibility.
```

This document constrains future implementation. It does not claim that the
current runtime already executes disjoint writers concurrently.

## Motivation and terminology

Calling NetbaDB a "single writer database" is too broad: it suggests that one
writer excludes mutation everywhere. The intended model is **Single Writer per
Mutation Domain**, also called **Partitioned Single-Writer** or **Disjoint Write
Domains**. **Mutation Ownership** is the exclusive authority to change one such
domain.

The term "multi-writer storage" is also misleading and should not describe
this architecture. NetbaDB does not intend to place several active writers
inside one authoritative physical storage merely because other physical
storages may mutate at the same time.

This boundary preserves straightforward ownership, dirty-write exclusion, WAL
ordering, rollback, allocation, B+Tree split, MemTable publication, and crash
recovery rules while allowing scale-out through independent physical state.

## Default mutation domain: `StorageId`

The conceptual definition is:

```text
MutationDomain = StorageId
```

A `StorageId` identifies the currently authoritative coherent physical state,
including, as applicable:

- one Heap and its page/allocation state;
- that Heap's registered B+Tree inventory;
- one LSM instance, its MemTable, WAL, SSTable publication, and maintenance
  state;
- storage-local transaction and MVCC state; and
- storage-local Change Stream state.

The permanent default is therefore:

```text
same StorageId → at most one active mutation owner
```

A future architecture decision may replace or refine this default only after
proving the resulting synchronization and recovery model. A benchmark showing
contention is evidence to investigate, not by itself sufficient authority to
add same-domain writers.

## Physical authority and logical routing

`TableId`, `PartitionId`, and `StorageId` serve different purposes:

```text
logical predicate
    ↓
TableId / PartitionId routing identity
    ↓
current authoritative StorageId
    ↓
mutation-domain ownership
```

`TableId` is the logical relation. `PartitionId` is a stable logical partition
used for planning and routing. Neither is sufficient physical write authority.
Mutation ownership attaches to the currently authoritative `StorageId`.

For example, after replacement:

```text
Partition P1
old S1
    ↓ schema/storage replacement
new S2
```

new mutation authority follows `S2`. Binding ownership only to `P1` would lose
the exact physical identity needed across replacement and recovery.

## Write-set compatibility relation

The architectural transaction model is:

```text
Transaction.write_domains = Set<StorageId>
```

For transactions A and B with write-domain sets `WA` and `WB`:

```text
WA ∩ WB = ∅
    → execution-compatible; future execution may proceed concurrently

WA ∩ WB ≠ ∅
    → mutation of the overlapping domain must serialize
```

This relation concerns active mutation execution. It does not by itself decide
durability grouping, commit-sequence allocation, or snapshot visibility.

## Independent-table and partition concurrency

Different tables may be backed by different authoritative domains:

```text
users     → S1
products  → S2
orders    → S3

W1 → S1
W2 → S2
W3 → S3
```

Future execution may run W1, W2, and W3 concurrently because their ownership
sets are disjoint. This does not violate single-writer ownership.

A hot logical table should scale by partitioning:

```text
orders
├── P1 → S11
├── P2 → S12
├── P3 → S13
└── P4 → S14
```

Transactions routed to `S11` and `S14` may eventually execute concurrently.
Partitioning is the preferred way to obtain more write parallelism for one
logical table:

```text
preferred

S1
 ↓ partition
S11  S12  S13  S14
 W1   W2   W3   W4

not the default

W1 ─┐
W2 ─┼→ S1
W3 ─┤
W4 ─┘
```

When independent physical domains can provide the required throughput,
NetbaDB prefers more domains over more synchronization within one domain.

## Transactions that own multiple domains

A transaction may legitimately own several storages:

```text
Txn A = {S1, S4, S9}
```

The invariant applies independently to each member of the set. For example:

```text
Txn A = {S1, S4}
Txn B = {S7}
→ compatible

Txn A = {S1, S4}
Txn B = {S4, S7}
→ overlap on S4; serialize S4
```

An initial disjoint-domain implementation may intentionally be conservative:

```text
single-StorageId transactions → domain-parallel
multi-StorageId transactions  → conservative or global serialization
```

That is acceptable while the more general ownership protocol is not proven.
Simplicity and recovery clarity take priority over maximizing concurrency.

## Deterministic domain acquisition

If a transaction acquires several mutation domains, acquisition order must be
deterministic. Ascending `StorageId` is the preferred canonical order:

```text
requested {S8, S2, S5}
acquire   S2 → S5 → S8
```

The preferred execution flow is:

```text
plan
→ determine logical partitions and write targets where possible
→ resolve their current authoritative StorageIds
→ acquire mutation ownership in deterministic order
→ execute
```

This is preferable to mutating `S5`, discovering `S2` later, and waiting for it
while retaining `S5`. Future work may predeclare and acquire a complete write
set or use another proven strategy, but it must not casually acquire domains
in arbitrary circular order.

This document does not introduce a lease manager, scheduler, or acquisition
API. `Transaction.write_domains` is a conceptual model, not a new production
type.

## Execution, durability, and visibility are separate

Three different concerns must remain explicit:

```text
1. Execution concurrency
   Disjoint StorageIds may eventually execute concurrently.

2. Durability batching
   Several transactions may share ordered Prepare or Commit barriers.

3. Visibility ordering
   Database commits remain globally ordered.
```

Parallel physical execution does not require parallel or unordered
publication. If A writes `S1` and B writes `S2`, their visibility may still be:

```text
A → DatabaseCommitSeq G101
B → DatabaseCommitSeq G102
```

or an ordered consecutive range committed as one group. `DatabaseCommitSeq`
continues to define one deterministic global commit order.

### Relationship to Phase 3C-3E

Existing durability work is orthogonal to active-writer concurrency:

- Phase 3C uses CORD v5 to record one ordered group decision.
- Phase 3D batches participant Commit barriers per `StorageId`.
- Phase 3E batches participant Prepare barriers per `StorageId`.

Those phases park or freeze members and release the active writer; they do not
permit simultaneous active mutation owners within one storage. A future
disjoint-domain executor may reuse their durability and publication protocols
without weakening the same-domain rule.

## Storage-engine implications

### Heap

A Heap `StorageId` has at most one active mutation owner. Different Heap
storages may eventually mutate concurrently. Page-level multi-writer mutation
is not the throughput path.

### B+Tree

A B+Tree follows the mutation owner of its authoritative Heap `StorageId`:

```text
S1 → BTree I1 → writer W1
S2 → BTree I2 → writer W2
```

W1 and W2 may run concurrently. W1 and W2 simultaneously mutating I1 is
outside the selected model. This keeps split, allocation, page-update, WAL,
rollback, and recovery ordering within one owner.

### LSM

One LSM `StorageId` has one active mutation owner. Different LSM storages may
eventually mutate concurrently and may have independent MemTables, WAL state,
SSTable publication, and maintenance state. Multiple writers do not share one
MemTable or WAL mutation path.

### Partitions

Logical partitions enable routing and scale-out, but `PartitionId` is not the
mutation lease. Each partition must resolve to its current authoritative
`StorageId`; concurrency follows the resulting physical domain set.

## Readers are not mutation owners

Single mutation ownership does not mean one operation per domain. The intended
long-term model may allow:

```text
many snapshot readers
+
one active mutation owner per StorageId
```

subject to MVCC and read-view correctness. Writer serialization does not imply
reader serialization.

## Schema, replacement, and maintenance authority

Disjoint DML ownership does not automatically authorize structural work.
Schema and catalog operations such as `ALTER TABLE`, `ALTER TYPE`, index
creation or removal, table removal, storage replacement, and partition-topology
changes may require authority broader than their currently visible storage
set. The initial policy may remain conservatively exclusive until a separate
architecture proof narrows it safely.

Maintenance likewise retains its formal eligibility and quiescence rules.
Columnar maintenance, Change Stream reclamation, LSM flush/compaction, and
physical replacement are not automatically concurrent merely because they
mention different `StorageId`s. Any narrower authority theorem must be stated
and proven explicitly.

Storage code should nevertheless avoid unnecessary database-global mutable
state. A local storage operation must not assume every other `StorageId` is
idle unless its actual correctness theorem requires global quiescence.

## Rejected default: same-domain multi-writer

NetbaDB intentionally does not begin with `RowId`, `PageId`, B+Tree key range,
or MemTable key range as the mutation domain. Those models require materially
more machinery, potentially including:

- page latches and B+Tree latch coupling;
- concurrent split and allocator protocols;
- row, page, predicate, or key-range lock management;
- lock escalation and deadlock detection;
- concurrent MemTable mutation;
- more complex WAL ordering and rollback dependencies; and
- a much larger global lock hierarchy.

Performance work must not introduce `Arc<Mutex<Heap>>`,
`Arc<Mutex<BTree>>`, multiple active writer handles for one `StorageId`, a row
or page lock manager, a predicate lock manager, a deadlock detector, B+Tree
latch coupling, or a shared concurrent MemTable merely because a benchmark
shows contention. Such a change requires an explicit architecture review that
shows why domain partitioning cannot solve the measured workload acceptably
and proves the new recovery and ordering invariants.

In particular, future work must not casually permit:

```text
two active Heap writers on one StorageId
two active LSM writers on one StorageId
two transactions concurrently mutating one B+Tree
multiple active writers mutating one MemTable
```

This is a correctness and complexity boundary, not an incidental limitation of
the current implementation.

## Performance escalation order

Write-performance work should proceed in this order:

1. Reduce per-operation CPU and allocation cost.
2. Improve prepared and batched execution.
3. Batch WAL and durability work.
4. Improve cache locality and buffer efficiency.
5. Improve B+Tree, LSM, Columnar, and other physical execution.
6. Split hot data into independent `StorageId`s.
7. Allow disjoint `StorageId`s to execute concurrently.
8. Only then reconsider same-domain multi-writer mutation.

Step 8 is intentionally exceptional and requires a new explicit architecture
decision.

## Current implementation boundary

The current implementation already has facts compatible with this decision:

- the synchronous, mutable `Database` path drives participant work serially;
- each Heap or LSM storage runtime lazily admits one active physical writer;
- `StorageRegistry` and partition placement preserve explicit `StorageId`
  identity;
- database transactions can coordinate multiple storage participants;
- participant sets and relevant group durability operations use stable
  `StorageId` ordering;
- snapshot readers are separate from physical writer ownership; and
- schema, group, checkpoint, replacement, and maintenance paths retain broader
  barriers or quiescence gates.

The current implementation does **not** promise that two transactions writing
different `StorageId`s execute concurrently. This policy adds no threads,
Tokio tasks, mutexes, writer lease manager, scheduler, concurrent registry,
planner write-set discovery, or partition-routing change.

It also changes no canonical schema, storage, WAL, coordinator, Change Stream,
Columnar, partition-catalog, protocol, SDK, or inspection format. Runtime
implementation is intentionally deferred and must be justified by measured
workloads while preserving the invariants above.
