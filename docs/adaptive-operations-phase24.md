# Adaptive Operations Phase 24

Phase 24 exposes the Phase 23 typed single-column Heap index proposal/apply
authority to embedded Server hosts. Native and PostgreSQL use the same
programmatic control handle and the same sole Database worker path:

```text
embedded host caller
        -> ServerPhysicalDesignControlHandle
        -> host forwarding loop
        -> sole Database worker
        -> ServerPhysicalDesignRuntime
        -> Core propose/apply
        -> existing named CREATE INDEX transaction
```

There is no Native, PostgreSQL, SQL, manifest, daemon, CLI, or NBOP apply
operation. The operator plane remains observation-only.

## Programmatic proposal and apply

`ServerPhysicalDesignControlHandle::propose_index` accepts one exact
`PhysicalIndexCandidate`. In the Database worker it calls
`Database::propose_physical_index_design` with the worker-owned evidence window
and fixed policy. It does not mutate evidence, diagnostics, Adaptive state,
catalog inventory, identifiers, files, schema generation, or global commit
sequence.

The returned `ServerPhysicalIndexDesignProposal` is cloneable but has private
fields and no public constructor or Core extraction API. Its getters expose the
candidate, evidence epoch and schema generation, first/last/proposal global
commit sequence, table schema version and fingerprint, storage identity,
point/range report counts, and bounded evidence summary. Durable database
incarnation and Server runtime identity remain hidden. Its manual `Debug`
implementation prints only those semantic fields.

`ServerPhysicalDesignControlHandle::apply_index` clones the proposal into one
typed worker command while the caller retains its value. The runtime first
validates Server provenance, then calls exactly:

```rust
database.apply_physical_index_design(
    &runtime.evidence,
    &proposal.proposal,
    index_name,
)
```

Core therefore remains the only authority for database incarnation, schema,
table, storage, evidence, current coverage, name conflicts, identity
allocation, global publication, WAL, rollback, and recovery. Successful
creation enters Core's existing global autocommit named-index transaction; it
is unrelated to any client session transaction.

## Runtime provenance

A Physical Design evidence epoch is local to one worker runtime. A restart
creates a new empty runtime whose first epoch is again `D0`; equal numeric
epochs across runtimes do not identify the same evidence cohort.

Each enabled `ServerPhysicalDesignRuntime` therefore owns a private
`Arc<ServerPhysicalDesignRuntimeIdentity>`. Every Server proposal stores only a
`Weak` reference to that identity. Apply requires the weak reference to upgrade
and `Arc::ptr_eq` to the current runtime identity before reading or changing
current evidence and before calling Core. A dead origin or another live runtime
returns `ProposalRuntimeChanged`. This check prevents an old `D0` proposal from
being accepted by a restarted `D0` runtime and prevents a proposal from one
live server from reaching another. Because proposals hold only `Weak`, retaining
one cannot keep the worker or server alive.

The provenance check is intentionally distinct from Core's durable catalog
incarnation check. Runtime provenance answers “which evidence window?”; Core's
incarnation answers “which durable database?” Both are required, in that order.
A mismatch reserves no `IndexId`, advances no global commit sequence, and
neither reads nor mutates the destination evidence window.

## Ordering, retries, and errors

Native and PostgreSQL forwarding use their existing host control receiver and
sole Database worker command channel. FIFO worker execution serializes apply
with client statements and other control operations; no mutex, async task,
second database owner, apply scheduler, budget, cache, or diagnostic counter is
introduced.

After provenance succeeds, retry and stale-state behavior is exactly Core
Phase 23 behavior. The same proposal/name returns `Created` once and then
`AlreadyApplied`; a different name after another index covers the capability
returns `AlreadyCovered`. Exact-name idempotency retains Core's precedence over
evidence rotation. Same-epoch new evidence is allowed. Rotation, schema drift,
capacity truncation, writer exclusion, name conflicts, current coverage, and
database identity are not reimplemented in Server.

Control failures add three typed cases:

- `Proposal(PhysicalIndexDesignProposalError)`;
- `Apply(PhysicalIndexDesignApplyError)`;
- `ProposalRuntimeChanged`.

The wrapped proposal/apply errors remain error sources. Runtime mismatch has no
source. When Physical Design is disabled, both operations return
`PhysicalDesignNotEnabled`; requesting them does not create a runtime.

## Frozen contracts and exclusions

Manifest v7, NBOP v2, Native Protocol v2, PostgreSQL wire behavior, Inspection
JSON v7, canonical schema, SDK schema, and every persistent format remain
unchanged. `netbadbd`, `netbadb operator`, and Server metrics gain no apply
surface. Columnar apply, composite/join/partition/LSM indexes, generated names,
automatic apply/drop/revert, trials, background advice, scheduling, apply
budgets, and cross-runtime retry journals remain deferred.

