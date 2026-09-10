# Adaptive Operations Phase 10: safe authoritative LSM maintenance

Phase 10 adds explicit, synchronous automatic admission for two existing LSM
maintenance primitives: MemTable `flush` and one `compact_one`. This is a new
safety domain. Columnar maintenance creates a derived generation that can be
suppressed, and Change Stream reclamation destroys history only after proving
that no authority needs it. LSM maintenance rewrites the only authoritative
physical representation and has no fallback copy or adaptive rollback target.

```text
Observe exact authoritative state
        -> production structural eligibility
        -> automatic pressure and hard-bound admission
        -> exact revalidation
        -> one production flush or compact_one
        -> physical and logical postcondition verification
```

Automatic LSM maintenance is disabled by default. It has no background thread,
timer, or implicit workload collection, and one explicit safe step can execute
at most one primitive.

## Production theorems and quiescence

Flush moves committed MemTable versions to an immutable SSTable, creates and
publishes a new WAL generation through the existing manifest protocol, clears
the MemTable, retires the old WAL, and performs the existing orphan cleanup.
Automatic execution calls the same production `flush` entry point; it does not
reuse the writer path's opportunistic threshold flush. The latter retains its
existing commit-path safety contract, while formal maintenance remains subject
to the stronger maintenance quiescence theorem.

`compact_one` selects the existing deterministic production plan, rewrites its
SSTable inputs, publishes the next manifest, removes obsolete inputs, syncs the
SST directory, and cleans orphans. Its production execution uses
`garbage_collect = false`, so every version required by historical LSM
visibility is retained. Automatic execution calls exactly this production
entry point.

`compact`, which may combine a flush with repeated compactions, and
`compact_full`, which merges all immutable levels and discards superseded MVCC
history, are deliberately excluded. Full compaction belongs to a separate
history-reclamation theorem.

Both manual and automatic candidate inspection share the same production
eligibility helper. The storage-owned safety inspection is also the source for
the final writer guard and covers recovery-required state, an active writer,
outstanding transactions, and outstanding read views. Core additionally blocks
while group commit or a schema writer is active. A non-empty MemTable is an
explicit `compact_one` blocker, fixing the former manual-inspection gap where a
compaction estimate could be advertised although the primitive would return
no work.

## Authoritative observation, proposals, and revalidation

`LsmMaintenanceAnchor` binds storage identity, manifest generation, WAL
generation, and visible commit sequence. Every flush and compaction layout
publication advances the manifest generation. A compaction proposal also binds
the exact ordered input SSTable IDs and output level, together with input entry
and byte counts. It does not use a lossy plan hash.

Detached observations contain SchemaGeneration, TableId, StorageId,
StorageKind, the layout anchor, physical snapshot, logical storage data
version, MemTable state, production thresholds and costs, Change Stream state,
and storage-owned bounds. Flush and compaction use distinct proposal structs.
A proposal is intent, never a mutable storage handle or mutation authority.

Immediately before mutation, Core observes again and requires the same schema,
table/storage identity, logical data version, physical snapshot, complete
layout anchor, MemTable state, policy, estimate, bound, and—where applicable—
the exact compaction plan. Production quiescence and the caller's current
budget must still admit the action. A target-table commit, schema change, or
manual flush/compaction therefore makes an old proposal stale. An unrelated
table advancing DatabaseCommitSeq does not by itself invalidate a proposal;
global G is retained for causal measurement but is not the target layout
authority.

## Pressure policy and resource bounds

Automatic flush requires:

```text
memtable_bytes >= max(production_flush_threshold,
                      policy.minimum_memtable_bytes)
```

The operator can make the automatic threshold stricter but cannot lower the
production threshold. Manual flush remains eligible for any non-empty
MemTable. Automatic compaction requires an empty MemTable, the exact plan from
production `pick_compaction`, and optionally
`plan.input_bytes >= policy.minimum_input_bytes`; zero adds no gate beyond the
production structural trigger.

Production admission estimates remain estimates. Automatic authoritative
admission separately uses checked conservative hard bounds computed by storage,
where the SSTable format is owned. Flush scans the exact MemTable. Compaction
performs a mutation-free first pass over the exact selected inputs using the
same `garbage_collect = false` output partitioning that the writer later uses.
For each planned output the bound includes the SST header and footer, exact
encoded entry bytes, Bloom bytes for the exact distinct-key count, and a block
header, checksum, and index record per entry. Charging one block of overhead
per entry is conservative for every real production chunking decision. All
counts and additions are checked; if a strong bound cannot be produced, the
automatic candidate is typed `AutomaticBoundUnavailable` rather than falling
back to the manual estimate.

Execution measures actual structural input/output through deltas of the
production `LsmWriteAmplification` counters. These quantities are structural
bytes and work units, not device writes, cache misses, or CPU time. Counter
saturation now has an explicit overflow signal; saturated or decreasing
counters cannot be reported as trustworthy consumption. Actual consumption
must fit both the admitted budget and the proven conservative bound. Exceeding
the latter is a correctness error, not an adaptive rollback outcome.

## Logical and historical invariants

A completed action records before/after LSM and maintenance inspection,
physical snapshots, logical data versions, Change Stream state,
DatabaseCommitSeq, SchemaGeneration, production estimates and bounds, actual
consumption, and obsolete-byte evidence. Runtime postconditions require:

- identical current logical versioned rows and values;
- unchanged logical StorageDataVersion and visible commit horizon;
- unchanged DatabaseCommitSeq and SchemaGeneration;
- unchanged Change Stream generation, origin, earliest/current frontiers,
  unresolved state, batch count, and maintenance status;
- an advanced manifest generation;
- for flush, an empty MemTable and advanced WAL generation;
- for compaction, an empty MemTable and unchanged WAL generation.

Storage integration tests additionally construct insert, update, and delete
commits, record results at every historical horizon, release all views to obey
quiescence, flush and compact once, then recreate the historical views and
compare them before and after reopen. This proves that ordinary compaction did
not take the `compact_full` garbage-collecting path. Existing failpoint and
reopen coverage remains authoritative for manifest publication, WAL rotation,
output creation, obsolete-file deletion, and orphan cleanup; Phase 10 adds no
automatic-specific journal or persistent format.

## Automatic lane and compatibility

Multi-target Safe Mode adds the distinct `AuthoritativeMaintenance` lane and
the candidate identities `LsmFlush { table_id, storage_id }` and
`LsmCompaction { table_id, storage_id }`. Discovery is limited to the caller's
bounded table scope, ignores Heap placements, and treats each LSM StorageId in
a placement as an independent candidate.

Within the lane, ready age precedes local merit. At equal age flush precedes
compaction; flush then ranks higher MemTable bytes and entries before lower
bound cost, while compaction ranks higher exact input bytes and input entries
before lower bounded output. Stable table/storage identity breaks remaining
ties. A blocked candidate never becomes ready through age.

The Phase 9 proactive order is preserved and the new lane is inserted between
reclamation and calibration: existing Columnar selection, Change Stream
reclamation, authoritative LSM maintenance, then planner calibration. The
existing `BoundedColumnarBurst` still describes only the established Columnar
versus calibration relationship; it is not silently generalized into a
four-lane scheduler. Reclamation or authoritative pressure can therefore
starve calibration in this version. A future phase may define an explicit
multi-maintenance service policy.

An active Columnar or planner-calibration trial still prevents all discovery,
ready-age updates, and LSM mutation. A selected LSM outcome—completed, aborted,
stale, no work, or error—ends the safe step and cannot fall through to another
LSM, Columnar, GC, or calibration action. LSM admission neither increments nor
resets the consecutive-Columnar counter and never reads or advances the manual
`MaintenanceCursor`.

Authoritative maintenance creates no probation trial. Query performance after
a rewrite may inform later planner or compaction policy, and the report can be
used by an operator to renew caller-owned evidence, but old SSTables are not an
adaptive rollback target and Safe Mode never rotates the evidence pool. Phase
3/4 calibration samples do not carry an LSM layout generation, so samples from
before and after a rewrite may be less representative when combined. This does
not confer storage authority or bypass the existing diversity, shadow-error,
epoch, and trial checks; it is an evidence-quality concern. Operators should
renew the caller-owned window after a materially shape-changing compaction.
Manual `flush`, `compact_one`, `maintenance_step`, and maintenance inspection
remain independently caller-controlled.

Phase 10 changes no Canonical Schema, Heap, BTree, Columnar, Change Stream v2,
coordinator, protocol v2, Schema Spec v2, Inspection JSON v7, LSM Manifest v2,
LSM WAL v1, or LSM SSTable v2 format. Proposals, policies, reports, fairness,
and service state are runtime-only. Reopen retains the production LSM layout
while automatic trials and admission age reset as before.
