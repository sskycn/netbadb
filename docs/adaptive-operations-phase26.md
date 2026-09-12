# Adaptive Operations Phase 26

Phase 26 closes the durable publication gap in the two existing managed
Columnar build APIs:

```text
Database::build_columnar_projection
Database::build_incremental_columnar_projection
```

Before this phase, a crash could leave a durable final NBCM/NBCS artifact after
the ProjectionId high-water had advanced but before NBPC registered the active
projection. Filesystem presence alone could not prove whether the artifact
belonged to the interrupted request, so neither safe adoption nor safe deletion
was possible.

The new state machine is:

```text
No build
  -> durable NBPC pending intent + allocated ID
  -> prepared files
  -> durable final NBC artifact
  -> durable active NBPC entry
  -> in-memory ProjectionRegistry entry
```

An ID is not a projection. Prepared files and a final manifest are not managed
planner authority. A pending intent owns one exact identity and locator but is
never planner-visible. Only an active NBPC entry can be loaded into the registry
and offered as a Columnar access path.

## Snapshot and incremental pipelines

Both builds canonicalize and preflight the caller location, reject an existing
final manifest, and capture the authoritative source before consuming an ID.
Capture remains read-only, so unsupported schema, scan, or Change Stream errors
do not burn allocator state. Core then durably begins an NBPC v2 intent with ID,
table, source storage, generation, fingerprint, relative locator, explicit
snapshot/incremental mode, and exact ordered columns.

The existing storage `prepare`/`prepare_incremental` and `publish` authorities
remain unchanged. Snapshot keeps its post-prepare source-token equality check;
incremental keeps its existing stream-anchored cursor semantics and does not
gain that snapshot-only rule. After artifact publication, one exact catalog
transition replaces pending with active. Registry publication is last.

Ordinary prepublication failures drop prepared handles, invoke storage-owned
exact unpublished cleanup, and durably abort the pending intent. The original
operation error is returned only if cleanup and abort both succeed. The ID stays
burned and the Database remains usable. A cleanup or abort failure instead
requires reopen.

## Crash recovery

Managed Database open first reads or migrates NBPC, then resolves a v2 pending
intent before exposing registry entries:

- if the exact locator has no final manifest, storage deletes only exact
  ID+generation temporary artifacts and the possible final base segment, then
  Core clears pending while preserving the allocator high-water;
- if the final manifest and all referenced segments are valid, Core matches ID,
  table, storage, generation, fingerprint, locator, mode, and ordered columns,
  then promotes the same ID to active without rebuilding;
- if the manifest is corrupt, references a missing/corrupt segment, or differs
  from the intent, open fails closed and preserves the artifact and intent for
  operator diagnosis.

The catalog shadow protocol guarantees that a crash during pending-to-active
publication reopens as either the old pending state, which is promoted again,
or the new active state. A crash after active catalog durability but before
registry publication loads exactly one active projection. Recovery is
idempotent across repeated reopen.

## Ambiguous errors and retention safety

`PreparedColumnarProjection::publish` may fail after a rename or sync whose
crash-survival result cannot be inferred by the running process. NBPC commit has
the same ambiguity. Managed builds therefore do not inspect paths and guess;
they return typed `ProjectionCatalogError::RecoveryRequired`, retain the pending
authority, and mark the current projection inventory unavailable for mutation.

Until reopen, build, attach, refresh, advance, compact, and drop are rejected.
Change Stream reclamation also blocks because a pending incremental artifact may
become an active retention consumer. The Physical Design advisor observes the
same unavailable catalog state and cannot recommend a duplicate projection.
The ambiguous build itself is never added to planner snapshots; already-active
registry entries may continue serving read-only queries.

## Compatibility and scope

[NBPC v2](projection-catalog-v2.md) and NBPM v2 are the only persistent format
changes. V1 active inventory and exact allocator gaps migrate in place through
the existing atomic publication machinery. V1 had no intent, so historical
unregistered artifacts remain unregistered and are never scanned or adopted.

NBCM/NBCS/NBCD, Canonical Schema, Schema Catalog, Schema Mutation Journal,
Coordinator, Heap, BTree, Index Catalog, LSM, Change Stream, and WAL formats are
unchanged. SchemaGeneration and DatabaseCommitSeq do not advance. Deployment
Manifest v8, NBOP v3, Native Protocol v2, PostgreSQL wire, Inspection JSON v7,
and SDK Schema Spec are unchanged; there is no Server, daemon, operator, or CLI
change.

Phase 26 does not apply `PhysicalColumnarCandidate`, NBOP advice, or any
recommendation. A future Phase 27 may add explicit Columnar apply only after it
can reuse this `Created`/recovered/failed lifecycle without filesystem guessing.
