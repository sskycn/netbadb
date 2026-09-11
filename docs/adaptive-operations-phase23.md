# Adaptive Operations Phase 23

Phase 23 adds the first explicit physical-design mutation authority. It is a
narrow synchronous Core bridge:

```text
observed recommendation
        -> typed request snapshot
        -> explicit IndexName approval
        -> current-state revalidation
        -> existing durable CREATE INDEX
```

Only a single-column, named, non-unique B+Tree on the exact single-storage
Heap layout already supported by `CREATE INDEX` is in scope. Columnar
projection apply, composite/join/partition indexes, LSM recommendations, SQL
apply syntax, automatic design, scheduler work, trials, and automatic drop or
revert remain out of scope.

## Proposal and apply

`Database::propose_physical_index_design` takes `&self`, a caller-owned
`PhysicalDesignEvidenceWindow`, a copied `PhysicalDesignAdvisorPolicy`, and an
exact `PhysicalIndexCandidate`. It first reuses the existing advisor. A
proposal exists only when that exact candidate is currently `Recommend`;
missing candidates and every typed `NoAction` decision are returned as typed
errors. The proposal stores the candidate's point/range counts and evidence
summary, the policy snapshot, the evidence epoch and G range, the schema
generation, and the table version/fingerprint/storage anchors.

Proposals require global visibility and a managed durable schema catalog. The
proposal binds to the catalog's persistent 16-byte incarnation, which answers
“which database?” but never reserves an `IndexId`. Proposal creation performs
no allocation, naming, catalog publication, schema/G change, filesystem write,
projection change, scheduler tick, or evidence rotation. Proposals are
runtime-only and may be cloned or retained for retry; they are not capability
tokens or persisted audit records.

`Database::apply_physical_index_design` requires the current evidence window
and an explicit typed `IndexName`. Before any index identity reservation it:

1. reloads and checks the durable catalog incarnation;
2. rejects backward global visibility;
3. recognizes an active exact `(name, TableId, ColumnId)` as
   `AlreadyApplied`, or a same-name different target as `IndexNameConflict`;
4. revalidates schema generation, table version/fingerprint, storage identity,
   column existence, and the single-storage Heap layout;
5. returns `AlreadyCovered` when the current active access path already covers
   the observed point/range capability;
6. checks the evidence epoch;
7. reruns the advisor with the proposal's policy and requires the exact
   candidate to remain `Recommend`.

New healthy evidence in the same epoch is allowed; exact report counts and the
last G need not remain unchanged. Evidence rotation, capacity truncation, or a
new non-recommendation blocks creation. Ordinary DML may advance G without
making a proposal stale. A current exact named index remains an idempotent
`AlreadyApplied` result even when the caller later retries with a rotated
window. A different current covering index is `AlreadyCovered`, not a duplicate
build.

## Mutation and durability authority

After preflight, the only mutation call is:

```rust
self.create_named_index(index_name, candidate.table_id, candidate.column_id)
```

Phase 23 does not call storage B+Tree builders, allocate pages, write an index
catalog, format SQL, or copy the global transaction path. Consequently the
ordinary named CREATE INDEX path remains responsible for writer exclusion,
`IndexId` floor advancement, backfill, transaction staging, WAL, coordinator
decision, global publication, catalog/index publication, rollback, and crash
recovery. Preflight failures and no-ops occur before that path, so they burn no
new `IndexId` and advance no G. Once the path starts, its existing allocator
and failure semantics apply unchanged.

Successful creation publishes one ordinary global index transaction. G
advances according to existing CREATE INDEX behavior; canonical
`SchemaGeneration`, table schema version, and table fingerprint do not change.
The apply report is an operation result, not a durable audit log or rollback
receipt, and exposes only stable candidate/name/commit-sequence/index identity.

## Isolation and deferred work

Proposal and evidence are caller-owned runtime control data. Apply does not
rotate the design window or `AdaptiveEvidencePool`, tick a scheduler, alter
safe mode or planner calibration, create a trial, or automatically revert/drop
the index. Reopen recovery reconstructs authoritative index inventory through
the existing catalog/WAL/coordinator machinery; a retained proposal can then
recognize an already-created exact named index.

The managed database incarnation rejects a proposal applied to another
database, even if the databases have identical TableId, ColumnId, and schema.
Manifest v7, NBOP v2, Native Protocol v2, PostgreSQL wire behavior, Inspection
JSON v7, and all persistent formats remain unchanged. Operator/server exposure
and authorization are deferred to a later phase. Columnar apply remains a
separate mutation theorem because it needs projection identity, placement,
source revalidation, and Change Stream lifecycle authority.
