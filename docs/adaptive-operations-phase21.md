# Adaptive Operations Phase 21

Phase 21 connects real Native and PostgreSQL Server workload to the observe-only
Physical Design Advisor introduced in Phase 20. The integration is deliberately
programmatic-only: it proves worker ownership, capture, control lifetime, and
current-inventory evaluation before any deployment or operator contract is
versioned.

## Runtime and configuration

`TcpServer` and `PostgresTcpServer` expose the independent builder:

```rust
.with_physical_design_advisor(
    ServerPhysicalDesignAdvisorConfig::new(evidence_limits, advisor_policy)
)
```

The configuration has no `Default`. `PhysicalDesignEvidenceLimits` bounds the
in-memory cohort, while `PhysicalDesignAdvisorPolicy` fixes how that Server
runtime interprets evidence. Zero capacities and zero recommendation thresholds
retain their Core meanings. The builder does not alter the manifest-derived or
programmatically overridden Adaptive mode, so Disabled + Design, FeedbackOnly +
Design, and Driven + Design are all valid in either builder order.

Each current Database worker owns the following independent state:

```text
Database worker
    |-- Database + sessions
    |-- optional ServerAdaptiveWorkerRuntime
    |     `-- AdaptiveEvidencePool
    `-- optional ServerPhysicalDesignRuntime
          |-- PhysicalDesignEvidenceWindow
          |-- fixed PhysicalDesignAdvisorPolicy
          `-- O(1) diagnostics
```

The design window is not stored in `Database`, `DatabaseSession`, a connection
thread, or the Adaptive runtime. It is created with the worker, discarded on
shutdown, and starts empty at epoch zero after restart. It is not persisted.

## One execution, independent telemetry sinks

After compilation and authorization, the shared Server observation bridge uses
the existing capture predicate:

```text
successful authorized Core relational query
AND autocommit
AND PreparedStatement::description().is_query
AND (Adaptive enabled OR Physical Design enabled)
```

When neither sink is enabled, Server calls ordinary `execute_prepared` and does
not allocate feedback statistics. When either sink is enabled, Server binds the
existing typed prepared statement once, plans once, and calls
`execute_prepared_with_feedback` once. The single resulting
`ExecutionFeedbackReport` is then offered once to each enabled concrete sink.
There is no SQL reparse, observer trait, duplicate plan, or duplicate execution.

Explicit-transaction queries, DML, DDL, and PostgreSQL compatibility paths are
not captured. Authorization still completes before execution, so a denied query
reaches neither sink. Core execution errors produce no report. PostgreSQL portal
resume returns the already-materialized portal result and does not record again.

Each telemetry admission is independent. An Adaptive failure cannot skip the
design admission, and a design failure cannot skip the Adaptive admission.
Neither failure changes the successful client result, transaction state, or
worker lifetime; neither is retried. In LegacyLocal mode the design sink records
`GlobalVisibilityRequired`, while the query remains successful. Server never
enables global visibility automatically.

Schema-generation changes also remain independent. A newer report applies Core's
design-window schema rotation and begins a new design cohort; the Adaptive pool
applies its own rules. Rotating either domain does not rotate the other.

## Programmatic control

`ServerHandle::physical_design_control()` and
`PostgresServerHandle::physical_design_control()` return the same
`ServerPhysicalDesignControlHandle`. The public handle owns only a typed host
request sender. Native and PostgreSQL accept loops forward requests into their
existing sole Database worker; public handle clones therefore do not own the
worker command channel or extend the worker lifetime.

`status()` returns bounded O(1) diagnostics plus
`PhysicalDesignEvidenceWindowInspection`. It returns no query shapes or
candidate history. Repeated status reads are pure.

`recommendations()` executes this exact operation in the Database worker:

```rust
database.advise_physical_design(&runtime.evidence, runtime.policy)
```

It recomputes on every request and stores no report history or cache. Current
indexes and Columnar projections are therefore revalidated immediately. Errors
such as `NoEvidence`, `StaleSchema`, and `InconclusiveCapacity` are typed control
results and do not make the worker fatal. Repeated calls with unchanged state
return equal reports.

`rotate_evidence_if_epoch(expected)` compares the current design epoch and
rotates in one worker command. A successful response reports the previous and
new epoch. Repeating a request with the old expected epoch returns
`EvidenceEpochChanged` and cannot rotate twice after a lost response. Rotation
clears only the design cohort; it does not run the advisor, mutate `Database`,
move Adaptive evidence, or wake the scheduler.

## Frozen contracts and exclusions

Phase 21 does not create or drop an index or Columnar projection, reserve an ID,
generate a name or path, mutate a schema or catalog, run background advice, add
an automatic lane, or cache/persist recommendations. It changes no SQL, Native
Protocol v2, PostgreSQL wire, Server metrics, Inspection JSON v7, canonical
schema, SDK schema, or database persistent format.

Deployment Manifest v6, NBOP v1, `netbadbd`, and `netbadb operator` remain
unchanged. A future phase may version deployment/operator presentation only
after this programmatic runtime boundary is stable; proposal and apply authority
remain separate future work.
