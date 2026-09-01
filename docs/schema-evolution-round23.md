# Round 23: ALTER TABLE / schema-evolution architecture audit

This round is an architecture and evidence round. It adds no parser, HIR,
prepared DDL, Core mutation, storage rewrite, catalog-write, Protocol, or
PostgreSQL `ALTER TABLE` production path. The target below is therefore a design
contract for a later implementation, not a supported feature.

Round 24 subsequently implements that typed Core Single-Heap rewrite contract;
see [Core Heap schema rewrite foundation](core-heap-schema-rewrite-round24.md).
This document remains the evidence and alternatives audit, not current support
documentation. SQL and PostgreSQL `ALTER TABLE` remain unsupported.

## 1. Commits / integration

Round 23 is intentionally limited to this audit, current-state regression tests,
and architecture/roadmap documentation. No persistent format version changes and
no public API changes are part of the round. The final commit and integration
identities are reported after the completion workflow rather than predicted here.

## 2. Current row format

Heap and LSM share `row_codec`. A row is only the concatenation of tagged scalar
values in current declaration order:

| Scalar | Exact bytes |
| --- | --- |
| `BOOL` | tag `0`, then byte `0` or `1` |
| `INT64` | tag `1`, then eight little-endian bytes |
| `UINT64` | tag `2`, then eight little-endian bytes |
| `TEXT` | tag `3`, little-endian `u32` UTF-8 length, then bytes |
| `NULL` | tag `4` only |

There is no row envelope, row-format version, field count, arity, null bitmap,
TableId, TableSchemaVersion, SchemaFingerprint, ColumnId, or per-field length
outside the `TEXT` value. The Heap slot length bounds the tuple. A Heap tuple is a
48-byte `NBMV` v1 header followed immediately by these row bytes. Its header stores
MVCC transaction/command identities and an optional next-version RowId, but no
schema identity.

The audit fixture `(a INT64, b TEXT NULL, c BOOL) = (7, 'hi', true)` has the exact
row payload:

```text
01 07 00 00 00 00 00 00 00 03 02 00 00 00 68 69 00 01
```

The new Heap-slot test reads this sequence from disk after the 48-byte header. A
separate assertion pins `NULL` as the single byte `04`.

## 3. Row schema identity

`decode_row` loops over every column of the supplied current `TableDef`, decodes
one tagged value for each position, validates physical type/nullability, and then
requires exact payload exhaustion. It has no bytes from which to discover the
writer's schema. `resolve_columns` maps requested ColumnIds to current vector
positions before decoding; ColumnIds never participate in persisted row decoding.

Consequently, the answer to the audit's decisive example is **nothing**: an old
two-field row contains no bytes saying that a new third nullable field should be
interpreted as implicit `NULL`. NBSC identifies the active schema, but it cannot
make an old positional payload self-describing.

## 4. MVCC lifetime

`NBMV` v1 stores `xmin`, optional `xmax`, `cmin`, optional `cmax`, and optional
`next_version: RowId`. UPDATE writes a replacement tuple and expires the old
version; DELETE expires the current version. Visibility is evaluated against the
transaction-status store and a pinned `CommitSeq` snapshot.

Vacuum computes a horizon from the oldest pinned snapshot, or the maximum commit
sequence when none is pinned, and removes only versions proven dead at or before
that horizon. WAL page before/after images can also retain old tuple bytes until a
checkpoint/recovery boundary. Exclusive schema-writer admission removes concurrent
transaction handles; it does not rewrite dead tuples, erase WAL, or prove that all
physical bytes already use a target schema.

`SET NOT NULL` validation must therefore scan the stable current committed logical
relation, not arbitrary slots and not only the newest page records. With the chosen
replacement strategy, historical versions remain in the old storage and are never
decoded under the target schema.

## 5. Heap physical schema metadata

Heap metadata v5 lives in page 0 and stores:

```text
NBD1, format 5, TableId, u16 column count, 32-byte fingerprint,
IndexCatalog root PageId, StorageId, reserved bytes
```

It does not store column descriptors, ColumnIds, names, types, nullability,
primary-key flags, or TableSchemaVersion. Open receives a `TableDef`, recomputes
its complete canonical fingerprint, and rejects any mismatch before row access.
The production code writes this metadata only while creating a Heap; no WAL-managed
F1-to-F2 header update primitive exists.

Canonical fingerprint bytes include TableId, table name, ordered ColumnIds and
names, physical and optional nominal semantic types, nullability, and primary-key
flags. Table rename, column rename, nullability change, nominal-type change,
physical-type change, ColumnId change, and column reorder all change it. Therefore
even a row-codec-compatible rename cannot reopen an unchanged Heap under its target
schema. Ignoring the mismatch would destroy the physical identity invariant and is
not an acceptable compatibility mechanism.

## 6. LSM / partition findings

LSM uses the same positional row payload. Manifest v2 binds StorageId, TableId,
the exact schema fingerprint, clustering ColumnId/type, allocator high-waters,
statistics, and SSTable inventory. SSTable headers also bind the fingerprint;
put entries add clustering key, LsmRowId and commit sequence metadata around the
same row payload. An LSM ALTER would need manifest/SSTable/WAL/compaction-history
rules beyond a Heap rewrite.

PartitionCatalog v1 is immutable evidence. A logical table entry binds TableId,
fingerprint and placement; range placement additionally binds partition-key
ColumnId/type and every child StorageId. Each child Heap independently requires
the table fingerprint. Altering one partitioned logical table therefore requires
an atomic multi-storage/catalog/evidence protocol. LSM and partitioned ALTER are
outside the first version.

## 7. RowId audit

Heap RowId is a storage-local physical locator `(PageId, u16 slot, u32 generation)`.
LsmRowId is likewise storage-local. Generation prevents a stale slot locator from
aliasing a reused occupant. Heap MVCC next-version pointers and BTree leaf entries
persist Heap RowIds.

Executor mutation rows carry an internal `StorageRowHandle`, but public
`QueryResult` contains only result columns and `Vec<Vec<ScalarValue>>`. Searches of
Protocol, client, server, Rust SDK and Go SDK found no RowId field or SQL exposure.
There are no foreign-key or cross-table persisted RowId references. A copy rewrite
may therefore assign new RowIds provided every target index is rebuilt. Old version
chains remain self-contained in the retired old storage until recovery retention
and GC permit deletion.

## 8. Index dependency audit

IndexCatalog v9 persists each active or retired `IndexDefinition` with stable
IndexId, optional explicit IndexName, ColumnId and BTree handle. The catalog codec
round-trips sparse ColumnIds directly. A BTree stores typed scalar keys plus RowIds;
its `IndexSpec` binds physical/semantic type and nullability. Table statistics store
row/page counts; per-index statistics store distinct non-null keys, null count and
tree height. There is no separate general column-statistics catalog.

A rename preserves surviving ColumnId identity, so its logical index definition
still selects the same column. Explicit index names do not change automatically;
legacy unnamed PostgreSQL aliases are re-derived from table/column names. A rewrite
changes every RowId, so the first version rebuilds every active index even when its
key bytes would otherwise be compatible, and clears table/index ANALYZE snapshots.
Dropping a primary-key, indexed, partition-key or clustering column is rejected
before durable mutation. Physical type change remains unsupported rather than
implicitly converting/rebuilding. Nullability changes also rebuild because it is
part of `IndexSpec`.

## 9. Identity lifecycle

Successful ALTER preserves TableId. Every surviving column preserves ColumnId,
including across rename, reorder decisions and physical replacement. DROP retires
its ColumnId forever; a later ADD consumes `next_column_id` and never fills a gap.
Reservation is durable and non-rollback, so rollback/crash may burn a ColumnId.
Exhaustion is a typed failure.

The chosen first version replaces physical storage for every supported ALTER, so
it reserves a new non-rollback StorageId. The old StorageId is never reused. It
does not reserve a new TableId. A later in-place metadata path, if ever justified,
would keep StorageId, but that is not the first-version architecture.

## 10. Version/generation

One successful ALTER increments exactly the target table's TableSchemaVersion
from V to V+1 and increments database SchemaGeneration once. Both increments are
checked before reservation/mutation. Other table versions remain unchanged.
`next_column_id` advances only for ADD and is preserved for other operations.

NBSC `epoch` remains the physical snapshot publication sequence. The in-memory
`catalog_generation` increments after winner publication so PostgreSQL reflection
and other runtime caches refresh. Fingerprint changes for every supported ALTER,
including rename and same-physical nominal-type change.

## 11. Prepared invalidation

Relational preparation records an exact dependency triple for every referenced
table: `(TableId, TableSchemaVersion, SchemaFingerprint)`. Execution recomputes the
triples before planning or storage access. Tests now independently pin version and
fingerprint mismatch and show that an unrelated table's prepared statement remains
valid.

After ALTER, all old prepared statements that reference the table are stale,
including rename-only statements; there is no name rebinding or special exemption.
Statements depending only on unrelated tables survive. Newly prepared statements
bind `(same TableId, V+1, target fingerprint)` and replan using rebuilt current
indexes. Protocol/PostgreSQL sessions surface the existing stale-statement error
path rather than silently reinterpreting a cached description.

## 12. SDK / Protocol implications

Protocol v1 Hello exposes ordered `(TableId, 32-byte fingerprint)` identities.
Rust and Go remote clients compare their required fingerprints exactly. Generated
SDK identities also embed the fingerprint. ALTER therefore intentionally makes an
old generated client fail its schema gate on its next connection; weakening exact
matching would hide a real shape/type change.

No wire-format change is required for the first implementation. An already
connected session must refresh its server-side catalog after runtime generation
changes and stale any prepared object that references the altered table. Dynamic
clients can reflect/reprepare; generated clients require regeneration when their
expected schema changes.

## 13. PG reflection implications

The PostgreSQL compatibility catalog is derived from current Core inspection and
refreshed when `catalog_generation` changes. Column order is current declaration
order and therefore defines reflected `attnum`; stable ColumnId is not exposed as
`attnum`.

Synthetic table OIDs hash TableId plus fingerprint, and index OIDs hash TableId,
fingerprint, ColumnId, kind and uniqueness. Any ALTER changes fingerprint, so OIDs
change even though TableId remains stable. This is acceptable for synthetic,
session-refreshed compatibility identities but must invalidate cached reflection.
Explicit index names remain fixed. Unnamed legacy aliases and synthesized primary
key names can change after table/column rename. The first implementation does not
claim PostgreSQL `ALTER TABLE` syntax or Alembic migration support.

## 14. Transaction/concurrency model

First-version ALTER is offline and synchronous. Admission requires the durable
runtime catalog, one active transaction handle (the caller), no existing schema
writer, no prepared/in-doubt recovery obligation, a runtime-created Single Heap,
and a transaction with no registered read/write participant or prior schema/index
mutation. This is stronger than merely serializing schema writers and gives the
rewrite one stable committed source view.

The transaction materializes a private target SchemaView with the same TableId,
target columns/version/fingerprint, and a staged binding to the new StorageId.
Globally visible schema and binding remain the old pair until the coordinator
winner is published. One schema version is globally visible at a time. One ALTER
per transaction is allowed; CREATE, DROP, index DDL and a second ALTER cannot mix.
No online ALTER or simultaneous old/new schema decoding is introduced.

Authorization remains identity-based: schema-admin authority is needed to start
ALTER, and same-TableId table grants remain attached without rewrite. Dropped-column
grants do not exist in the current authorization model; a future column-grant model
must key them by ColumnId and retire them explicitly.

## 15. DML-before/DML-after ALTER policy

DML or a read participant before ALTER is rejected. This deliberate first-version
restriction avoids defining whether uncommitted old-schema writes belong in the
source scan and prevents self-visible row versions from crossing the rewrite.
Empty-table ADD NOT NULL without a default is also rejected; the small convenience
does not justify a separate semantic branch.

After staging completes, typed DML against the target table is allowed through the
transaction-local target SchemaView and new binding. ADD nullable sees explicit
`NULL` in every copied row; DROP omits the removed field; renamed identifiers resolve
only under the target names. Target-table SELECT and DML use only the staged Heap.
Existing-table DML unrelated to the ALTER may be allowed only within the existing
coordinator participant rules, but no additional schema mutation may be staged.
Rollback discards all target DML with the staged Heap; commit publishes it together.

## 16. Operation matrix

“First version” below means the intended Core Heap operation after the rewrite
foundation exists, not functionality implemented by Round 23.

| Operation | Logical metadata | Row rewrite | Validation | Index impact | StorageId | First-version support |
| --- | --- | --- | --- | --- | --- | --- |
| Rename table | name | yes, uniform publication | identity/name conflict checks | rebuild all; explicit names fixed | new | yes |
| Rename column | name, same ColumnId | yes | duplicate/dependency checks | rebuild all; same ColumnId | new | yes |
| Add nullable, no default | new ColumnId, nullable | yes; append explicit NULL | declaration/capacity checks | rebuild all | new | yes |
| Add NOT NULL, no default | new ColumnId | impossible without value | n/a | n/a | n/a | no, including empty table |
| Drop column | remove active ColumnId | yes; omit value | dependency checks | rebuild all; indexed/PK rejected | new | yes only unindexed/non-PK |
| Set NOT NULL | nullability | yes, uniform | current-visible non-NULL scan | rebuild all | new | yes |
| Drop NOT NULL | nullability | yes, uniform | none beyond schema validity | rebuild all | new | yes |
| Nominal type change | semantic name, same physical | yes, uniform | canonical/type checks | rebuild all | new | yes |
| Physical type change | physical type | conversion required | conversion/error policy absent | rebuild required | new | no |
| Column reorder | declaration order/attnum | yes | dependency/PG policy absent | rebuild all | new | no |

## 17. Row compatibility matrix

This matrix describes direct use of current V1 row bytes with a V2 `TableDef`,
before the Heap fingerprint gate (which independently rejects every listed schema
change).

| Target change | Current decoder result | Required policy |
| --- | --- | --- |
| Rename table | values decode unchanged | rewrite in chosen v1 strategy |
| Rename column | values decode unchanged | rewrite in chosen v1 strategy |
| Append nullable | `MissingScalarTag`; no implicit NULL | rewrite |
| Append NOT NULL | `MissingScalarTag`; no value exists | unsupported without backfill/default |
| Drop trailing | `ExtraValues` | rewrite |
| Drop middle | later value is interpreted at wrong position, normally type mismatch, and/or extra value | rewrite |
| Reorder | positional reinterpretation/type mismatch | unsupported first version |
| Same-physical nominal rename | values decode unchanged | rewrite in chosen v1 strategy |
| Physical type change | physical tag mismatch | unsupported conversion/rewrite policy |

There is no safe decoder compatibility rule to enable in this round. Adding one
would be a persistent row-format semantic change that must cover update, vacuum,
WAL recovery, LSM and mixed-version rows.

## 18. Architecture alternatives

Four alternatives were evaluated:

1. **In-place metadata update.** It avoids copying rename-only tables, but requires
   a new WAL-managed Heap-header mutation, expected-F1/F2 compare-and-swap,
   coordinator coupling, recovery of header/NBSC split states, and separate rewrite
   machinery for layout changes.
2. **Copy-on-write replacement.** It reuses staged Heap creation and coordinator
   winner publication, isolates old bytes, gives all supported operations one crash
   boundary, and avoids interpreting historical versions under a new schema. Its
   costs are O(table + indexes) I/O, temporary double space and new RowIds.
3. **Versioned/self-describing rows.** It can support online mixed schemas but needs
   a new row format, schema history, migration codecs and cross-version index/MVCC
   rules. This is substantially larger than the current offline goal.
4. **Trailing-null decoder compatibility.** It is narrowly attractive for ADD
   nullable, but current rows lack arity, so truncation cannot be distinguished from
   the old layout. It also leaves the Heap fingerprint publication problem and does
   nothing for other operations.

## 19. Chosen architecture

The first-version target is **all supported ALTER operations use a staged
copy-on-write replacement of one runtime-created Single Heap**. The logical table
keeps TableId; surviving columns keep ColumnId; the table advances to V+1 and a new
fingerprint; physical storage gets a new non-reused StorageId. No supported ALTER
updates the active Heap in place.

This is deliberately conservative. The current positional row bytes and exact Heap
fingerprint contract make a hybrid design two independent transactional mechanisms,
whereas replacement keeps old tuples/WAL recoverable under their old schema and
uses one old-or-new publication rule. The first delivery sequence starts with a
Core/storage-neutral rewrite foundation, not SQL syntax and not a one-off rename.

## 20. Metadata-only mutation protocol

There is no same-StorageId metadata-only protocol in the chosen first version.
Rename, DROP NOT NULL and nominal-only changes are logical metadata-only candidates,
but their fingerprint changes; the replacement Heap is created directly with F2,
while the old Heap remains F1. Thus no durable object ever needs an in-place F1-to-F2
transition.

If measurement later justifies a hybrid path, it must introduce one typed
Heap-schema-metadata update participant—not an unchecked header write—with expected
TableId/StorageId/F1/count and target F2/count, WAL full-page before/after images,
prepare/sync, and a coordinator decision bound to the prepared NBSC digest. Recovery
must expose only old-Heap+old-NBSC or new-header+new-NBSC. That future optimization
is not Round 24 and must not weaken open validation.

## 21. Rewrite mutation protocol

The target protocol is:

1. Resolve an exact target `(TableId, V, F1)` and validate placement, dependencies,
   names, operation support and checked V+1/generation/epoch/revision.
2. Durably reserve any new ColumnId and one new StorageId. Burn reservations on
   rollback/crash; TableId is unchanged.
3. Append one generic typed `AlterHeapRewrite` intent containing base and target
   schemas, a ColumnId-based transform, old/new descriptors, reservation floors and
   target NBSC digest. Do not add one journal tag per SQL subtype.
4. Create a private Heap `(same TableId, new StorageId, F2)` and materialize the
   transaction target SchemaView/binding.
5. Stream current committed source rows in bounded batches. Map source values by
   ColumnId, synthesize explicit NULL for ADD nullable, omit dropped fields, and
   validate target values. Do not collect the table in memory.
6. Recreate every active index definition against surviving ColumnIds and backfill
   with new RowIds. Reject removed dependencies. Clear stale ANALYZE statistics.
7. Apply permitted transaction-local target DML, flush the staged Heap and prepare
   its physical transaction.
8. Write and synchronize the complete prepared NBSC for V+1/new StorageId, then
   synchronize one coordinator CommitDecision bound to its digest and participant.
9. On the winner path, promote/open the staged Heap, persist exact old-resource
   replacement-retirement evidence, publish NBSC/state, then atomically replace the
   in-memory schema/binding/registry view and increment runtime revision.
10. On the loser path, roll back/remove only the known staged resource; old Heap and
    old NBSC remain authoritative.

## 22. Old StorageId retirement/GC

An ALTER replacement is not a DROP: TableId remains active while its old StorageId
retires. Round 22's physical bundle validation, no-symlink deletion, retry-only GC
intent, directory sync and completed-history horizon are reusable, but its
`RetiredTableResource` semantics are not directly reusable because they describe a
retired logical table.

The future journal needs generic replacement retirement evidence containing the
old complete table fragment `(TableId, V, F1)`, exact old Heap descriptor/locator,
new `(V+1, F2, StorageId)`, and deciding database transaction. Retaining the old
fragment is essential: active NBSC need not keep dropped columns or schema history,
yet recovery must still open/resolve the old Heap using F1 before GC. The coordinator
decision or its exact schema reference must make retirement of the old StorageId an
explicit durable fact. Only after the same Round 22 recovery horizon proves no
future replay can need it may explicit GC delete the old Heap/WAL/status/index/link
bundle. Startup resumes an existing GC intent but never chooses a candidate.

## 23. Crash/recovery design

| Last durable point | Recovery result |
| --- | --- |
| Before reservation | old schema/storage; no identity consumed |
| Reservation only | old winner; reserved IDs remain burned |
| Rewrite intent / staged creation / mid-copy | old winner; remove exact staged resource |
| Index rebuild or new-storage sync before prepare | old winner; roll back staged participant |
| Prepared participant or prepared NBSC without decision | presumed abort; old winner |
| Durable CommitDecision | new winner regardless of later crash |
| Decision before staged promotion | recovery promotes/opens exact new Heap |
| Decision before replacement-retirement record | recover it from the durable rewrite intent/decision |
| Decision before NBSC/state publication | publish exact prepared NBSC |
| NBSC before in-memory publication/API return | reopen sees new winner; current process completes idempotently |

Recovery validates intent, base/target snapshots, digest, participant identity,
TableId equality, V-to-V+1, F1/F2, old/new StorageIds and reservation floors before
mutating anything. Missing, extra or mismatched evidence is corruption, never a
reason to choose by mtime or adopt an arbitrary staged file. Repeated recovery is
idempotent. Old and new schema/binding pairs are the only observable states.

## 24. Persistent-format implications

Round 23 changes no persistent format. Its tests consume existing Page v5, Heap
metadata v5, `NBMV` v1, row scalar encoding, IndexCatalog v9, NBSC v1, NBSJ v1,
CORD v2, PartitionCatalog v1 and LSM formats unchanged.

The future rewrite can keep row/page/Heap/IndexCatalog/NBSC byte versions: the new
Heap contains only target-format rows, and NBSC v1 already stores active columns,
TableSchemaVersion, `next_column_id`, fingerprint, placement and descriptors. It
does not need active-catalog tombstoned columns or general schema history. The
mutation journal will need one versioned generic ALTER-rewrite intent/retirement
record (and possibly a coordinator schema-reference extension) so old schema and
resource evidence survive until GC. Any such format change requires strict bounded
decode, corruption/truncation tests, fuzz seeds and documented migration policy.

## 25. Tests / experiments

Round 23 adds three focused tests:

- exact engine-neutral row bytes, single-byte NULL, rename compatibility, sparse
  non-positional ColumnIds, projection-by-ID, and exact failures for append nullable,
  drop middle/trailing and physical type change;
- an actual Heap insert followed by raw slot inspection proving `NBMV` v1 plus the
  exact 18-byte row payload and absence of an extra row envelope;
- prepared dependency checks proving table version and fingerprint independently
  stale the target while an unrelated dependency remains valid.

Existing tests already pin canonical fingerprint changes for every identity field,
unchanged-Heap reopen rejection for table/column rename, nullability, nominal and
physical type, ColumnId/order/PK, IndexCatalog ColumnId bytes, RowId generation and
reopen, MVCC version pointers, index RowId maintenance, and SQL result externality.
No streaming benchmark was added: the round forbids production rewrite machinery,
and a disconnected prototype would not validate the future coordinator protocol.

## 26. Fuzz

No decoder or persistent format changed. The existing bounded fuzz targets for
schema catalog, schema mutation journal, coordinator log, page, BTree, index
catalog, WAL recovery, Protocol, pgwire, PartitionCatalog, LSM manifest, LSM WAL
and LSM SSTable each passed 1,000 runs with seed 23 and isolated temporary corpora
and artifact directories. The future ALTER intent decoder must receive its own
malformed/truncated/cross-field fuzz coverage before implementation is merge-ready.

## 27. Compatibility regressions

Round 23 preserved the current psql 17.11, psycopg 3.2.13, SQLAlchemy 2.0.52,
Alembic 1.16.5, Rust Protocol v1, Go Protocol v1 and generated-SDK `--check`
baselines. Passing them means existing CREATE/DROP/DML/reflection behavior did not
regress; it does not mean ALTER syntax or migrations are supported. The full Rust
workspace also passed formatting, all-target/all-feature check and Clippy with
warnings denied, and all tests. Rust 1.85 passed the types/schema/index/storage
subset (412 tests); the broader Core check remains blocked by the pre-existing
planner `let`-chain at `crates/netbadb-planner/src/lib.rs:892` and `:904`.

## 28. Unsupported/deferred

Round 23 and the first Core rewrite version exclude SQL/HIR/frontend ALTER,
physical type conversion, defaults/backfill expressions, ADD NOT NULL without a
value (including empty-table special casing), column reorder, dropping indexed/PK/
partition/clustering columns, online ALTER, mixed row schemas, LSM ALTER,
partitioned ALTER, imported/bootstrap Heap ALTER, foreign keys, column grants,
automatic/background GC, and PostgreSQL/Alembic ALTER execution.

Long-term candidates include a measured in-place metadata participant, versioned
rows/schema history for online evolution, safe typed conversions, defaults, richer
dependency graphs, multi-storage rewrite, old-resource automatic GC, and rewrite
progress/space observability. None is implied by the chosen first version.

## 29. NBSJ/CORD compaction debt

NBSJ reservation, mutation, retirement and GC history and CORD decisions are
append-only evidence used for non-reuse and recovery. ALTER rewrites would add two
physical identities and retained old-schema fragments, increasing that growth.
Compaction is a separate future maintenance phase: it needs an atomic checkpoint of
allocator floors, unresolved obligations, completed recovery horizons and retained
resources, plus crash-safe replacement and fuzzing. Round 23 neither changes nor
compacts either log.

## 30. Completion workflow

Formatting, all-target/all-feature check and Clippy with warnings denied, full
workspace tests, the applicable Rust 1.85 subset, all 13 fuzz targets, real client
regressions, Go tests and generated-SDK checking passed. The only unavailable
validation is the broader Rust 1.85 Core check described in section 27. The final
completion record supplies the commit, fetch, merge, push, remote-containment and
clean worktree/branch-removal evidence after those necessarily post-document
steps complete.

## 31. Round24 recommendation

Implement exactly one phase next: **Core Heap Schema Rewrite Foundation**. It should
provide the typed ColumnId transform, non-rollback StorageId/ColumnId reservation,
bounded current-row copy, full active-index rebuild, target SchemaView/binding,
generic replacement-retirement evidence, coordinator winner/loser recovery and
crash matrix for one runtime-created Single Heap. It must expose no SQL syntax and
should prove a test-only rename/add-nullable/drop-unindexed transform end to end
before any individual ALTER command becomes public.
