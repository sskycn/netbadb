# Adaptive Operations Phase 27

Phase 27 adds the first explicit Core bridge from one current
`PhysicalColumnarCandidate` recommendation into the existing managed Columnar
build lifecycle:

```text
advisor recommendation
    -> caller-selected Snapshot or Incremental mode
    -> caller-approved placement directory
    -> runtime-only proposal
    -> current-state revalidation
    -> existing Database Columnar build API
    -> NBPC v2 publication and recovery
```

Physical Design remains evidence and policy. It does not choose maintenance
semantics, generate a directory, enable a Change Stream, tune row groups, or
allocate a `ColumnarProjectionId`.

## Explicit mode and placement

`PhysicalColumnarDesignMode` has exactly `Snapshot` and `Incremental` variants
and deliberately has no `Default`. Snapshot delegates to
`Database::build_columnar_projection`; Incremental delegates to
`Database::build_incremental_columnar_projection`.

The caller supplies the directory during proposal creation. Core resolves it
with the same stable absolute-path authority used by the managed build APIs,
including an existing-parent canonicalization when possible, without requiring
the final directory to exist. The normalized path, selected mode, exact
advisor-ordered columns, evidence/policy anchors, table version and
fingerprint, source `StorageId`, durable database incarnation, and visibility
range are frozen in `PhysicalColumnarDesignProposal`. Apply accepts no
replacement physical parameters.

Incremental proposal creation additionally requires the exact current source
storage to have an already enabled, healthy Change Stream with a nonzero
generation. The proposal captures that generation. Apply requires the same
stream incarnation; disabled, unavailable, and replaced generations are
distinct stale conditions. Snapshot proposals carry no stream generation, and
later stream changes do not alter Snapshot semantics. Neither proposal nor
apply calls `enable_change_stream` or `disable_change_stream`.

## Purity and preflight ordering

Proposal creation takes `&self`, reuses the existing advisor, and requires the
exact candidate to be `Recommend`. It also requires Global visibility, a
durable schema catalog, and an available managed Projection Catalog. It creates
no directory, writes no pending intent or artifact, reserves no identity,
changes no visibility or schema generation, and mutates no evidence or
scheduler state.

Apply first reloads the durable database incarnation and current Global
visibility. It then classifies the proposal's exact normalized directory using
registered projection inventory only:

- an available location continues through validation;
- an available exact artifact with the same table, ordered columns, and mode
  returns `AlreadyApplied` with its existing identity;
- every other registered occupant is a typed conflict.

Exact-location `AlreadyApplied` intentionally precedes evidence-epoch and
recommendation checks. A lost-response retry therefore remains recognizable
after evidence rotation, later DML, or projection refresh. Freshness is not
part of durable action identity. Unregistered manifests are never scanned,
adopted, removed, or claimed as active.

For an available location, apply revalidates schema generation, table version
and fingerprint, exact single-storage placement, every column, Heap/LSM source
support, and Incremental stream lineage. Ordinary forward Global commit
sequence movement is allowed. A different current projection whose columns
cover the candidate returns `AlreadyCovered` before evidence freshness and
consumes no identity. Apply then checks the evidence epoch and reruns the
existing advisor with the frozen policy. `ExistingDesignCovers` remains a
coverage no-op; every other `NoAction` rejects creation.

## Mutation and recovery authority

Only after all rejection and no-op paths finish does apply construct
`ColumnarProjectionSpec::new` with the exact frozen table, path, and ordered
columns. It does not set `row_group_rows`. The selected existing `Database`
build API is the sole mutation call.

From that point, Phase 26 owns ID allocation and burn, durable NBPC v2 pending
intent, NBC artifact preparation/publication, pending-to-active transition,
registry publication, ambiguous failure, and reopen recovery. Phase 27 does
not call Projection Catalog or storage publication internals. A typed
`RecoveryRequired` error propagates through `DatabaseError`; the running
database does not inspect files or guess success. After reopen, the exact
location classifier can recognize a recovered active projection.

`Created`, `AlreadyApplied`, and `AlreadyCovered` are runtime operation
results, not persistent audit or rollback records. Columnar publication is
derived state: it advances neither `DatabaseCommitSeq` nor canonical
`SchemaGeneration`. Snapshot projections retain ordinary stale-after-DML
behavior. Incremental projections retain ordinary lag and explicit
`advance_columnar_projection` behavior; apply performs no automatic refresh,
advance, compaction, scheduler tick, drop, move, conversion, or replacement.

## Compatibility and deferred authority

Phase 27 is Core-only. Deployment Manifest v8, NBOP v3, Native Protocol v2,
PostgreSQL wire, Inspection JSON v7, SDK Schema Spec, NBPC/NBPM v2,
NBCM/NBCS/NBCD, schema/coordinator/storage/WAL formats, and Server manifests are
unchanged. Server, operator, daemon, CLI, protocol, SQL, authorization,
placement-policy, automatic-apply, trial/revert, and background-builder
surfaces remain deferred.
