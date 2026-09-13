# Adaptive Operations Phase 28

Phase 28 carries the Phase 27 explicit Columnar proposal/apply authority through
the embedded Native and PostgreSQL Server hosts while removing arbitrary
filesystem authority from the control caller:

```text
worker-owned Physical Design evidence
    -> explicit Snapshot or Incremental approval
    -> validated logical placement key
    -> Server-approved canonical root/key
    -> same-runtime Server proposal
    -> Core Phase 27 revalidation and apply
    -> Phase 26 managed publication
```

This is a programmatic host API introduced before the local operator bridge.
Manifest v8 and NBOP v3 are historical; the current operator bridge is defined
by Manifest v9 and NBOP v4. `netbadbd`,
`netbadb operator`, Native Protocol v2, PostgreSQL wire, Inspection JSON v7,
and every persistent format remain unchanged by Phase 28.

## Host placement and mode policy

`ServerPhysicalColumnarApplyConfig::new` requires a pre-existing directory and
at least one explicitly allowed mode. It canonicalizes and stores the absolute
root but creates no root, marker, lock, placement, Database, or projection.
There is deliberately no `Default`, and neither Snapshot nor Incremental is
silently selected. The root is re-canonicalized at Server startup and before
every proposal and apply; disappearance, non-directory replacement, or a
different canonical result fails closed without recreating or switching roots.

The caller supplies `ServerPhysicalColumnarPlacementKey`, not a path. Its frozen
grammar is 1–128 UTF-8 bytes, although every accepted byte is ASCII: the first
byte is alphanumeric and later bytes are alphanumeric, `-`, `_`, or `.`. This
rejects empty, `.`, `..`, hidden-dot, absolute, nested, backslash-separated,
space-containing, and non-ASCII forms. A valid key therefore resolves to
exactly one direct child:

```text
canonical configured root / validated placement key
```

An otherwise unregistered target must be absent. Server uses
`symlink_metadata`, so a regular file, directory, symlink, dangling symlink, or
other filesystem object is occupied. It never adopts, deletes, refreshes, or
overwrites such an object. Proposal reserves no path or projection identity.

## Configuration composition

`TcpServer::with_physical_columnar_apply` and
`PostgresTcpServer::with_physical_columnar_apply` add an independent
programmatic configuration dimension beside Adaptive mode and the Physical
Design advisor. Builder order does not change their meanings. The advisor may
come from Manifest v8 or `with_physical_design_advisor`, but Columnar apply
without a final advisor is a typed startup error. Apply configuration is not a
manifest field and does not create a default evidence window or advisor.

The sole Database worker still owns one `ServerPhysicalDesignRuntime`, one
evidence window, its fixed policy, and the optional immutable Columnar placement
policy. There is no second worker, filesystem worker, async task, path
reservation registry, proposal cache, or mutation owner.

## Proposal and runtime provenance

`ServerPhysicalDesignControlHandle::propose_columnar` accepts an exact current
`PhysicalColumnarCandidate`, explicit `PhysicalColumnarDesignMode`, and logical
placement key. In the sole worker it checks that apply is enabled, the mode is
allowed, and the root is still exact; resolves `root/key`; calls Core
`propose_physical_columnar_design` with current worker evidence and policy;
verifies Core retained the exact resolved directory; and finally rejects an
unregistered occupied target. No proposal step creates a directory, consumes a
`ProjectionId`, writes NBPC or NBC files, enables a Change Stream, rotates
evidence, changes G/schema, or invokes scheduling.

The returned `ServerPhysicalColumnarDesignProposal` has private fields and no
public constructor or Core extraction API. It exposes only semantic getters:
candidate, mode, logical placement, evidence/schema/G anchors, table
version/fingerprint, storage identity, optional Change Stream generation, and
the bounded evidence summary. Its manual `Debug` omits the durable database
incarnation, runtime pointer/provenance, configured root, and absolute path.

The proposal holds a `Weak` reference to the same private runtime identity
introduced in Phase 24. Apply upgrades it and requires `Arc::ptr_eq` before
reading destination evidence or invoking Core. Cloned handles in one runtime
therefore interoperate, while another live runtime and an old-D0/new-D0 restart
collision return `ProposalRuntimeChanged`. Retaining a proposal cannot keep the
Server or worker alive. Programmatic Server proposals are same-runtime mutation
approvals; cross-restart operator recovery remains future work.

## Apply, occupancy, and Core authority

`apply_columnar` sends one proposal in one FIFO worker command. After provenance
it rechecks apply enablement, mode permission, root identity, direct-child
resolution, and exact equality with the frozen Core directory. It then calls
Core `inspect_physical_columnar_design_location`:

- `Available` requires the filesystem target still to be absent;
- `AlreadyApplied` is allowed to reach Core despite the now-existing directory;
- `Conflict` is also delegated so Core returns its typed registered-location
  conflict.

This ordering rejects a target that appears after proposal without consuming an
ID, while an exact successful retry returns the same `AlreadyApplied` identity.
A different current covering projection returns `AlreadyCovered`; placement is
not a reservation. After these Server-only checks, the only mutation call is:

```rust
database.apply_physical_columnar_design(
    &runtime.evidence,
    &proposal.proposal,
)
```

Core Phase 27 remains authority for database incarnation, global visibility,
schema/table/storage/column anchors, Change Stream lineage, current coverage,
evidence epoch, advisor revalidation, and choice of the established Snapshot or
Incremental build API. Phase 26 remains authority for ProjectionId allocation
and burn, NBPC v2 pending intent, NBC publication, active registration, and
`RecoveryRequired`. Server neither inspects NBPC nor guesses after an ambiguous
failure, reopens the Database, retries, or rotates its runtime.

Snapshot requires only host mode permission and retains ordinary stale-after-DML
behavior. Incremental additionally relies on Core's already-enabled healthy
Change Stream requirement, captures its exact generation, and becomes Lagging
after later DML until existing explicit/Adaptive maintenance advances it.
Server never enables, disables, rebaselines, advances, refreshes, compacts, or
schedules a projection automatically.

## Isolation and known limitations

Proposal and apply do not change Physical Design diagnostics, evidence epochs,
Adaptive evidence, scheduler state, Server metrics, or client session state.
They borrow no Native or PostgreSQL session, transaction, principal, grant, or
wire response. Failures therefore cannot poison PostgreSQL `ReadyForQuery`,
Native framing, or an unrelated client transaction.

All control and foreground commands remain serialized by the current Database
worker. A synchronous Columnar scan/build may delay later client requests. That
latency is explicit in Phase 28 and does not justify a new thread or mutation
owner. A future operator phase must define versioned logical approval and
cross-restart idempotency without serializing either Core or Server proposal
objects and without accepting caller filesystem paths.
