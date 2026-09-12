# Adaptive Operations Phase 25

Phase 25 exposes one explicit local-daemon approval path for a current
single-column Heap B+Tree recommendation:

```text
OS-authenticated local operator
  -> Manifest v8 durable-mutation permission
  -> NBOP v3 runtime token + evidence epoch + logical candidate + IndexName
  -> one typed Database-worker command
  -> fresh Core proposal and immediate Phase 23 apply
  -> existing durable named CREATE INDEX transaction
```

Operator advice, Physical Design enablement, and operator-plane existence are
not mutation authority. Apply requires both filesystem access and the explicit
Manifest permission. The opaque 128-bit OS-random token identifies one joint
operator/worker lifetime, while the evidence epoch identifies one measurement
cohort inside that lifetime. Neither is a capability or credential.

The listener never owns Database and never decides recommendation, coverage,
schema/storage applicability, name conflict, IndexId, or creation. It forwards
one request as one worker command. The worker first uses Core's shared active
name classifier, then validates runtime and epoch, calls
`propose_physical_index_design` with current evidence/fixed policy, and directly
calls `apply_physical_index_design` with that stack-local proposal. Phase 24's
embedded-host proposal/apply API remains source-compatible and is independent
of the operator-only manifest permission.

Exact-name classification precedes stale guards. Thus a lost successful response
can be retried as `AlreadyApplied` in the same daemon or after restart. An old
request without that exact durable result can never authorize a new mutation in
the new runtime, including an old-D0/new-D0 numeric epoch collision. A current
different covering index yields `AlreadyCovered`; a same name on another target
is a conflict.

Apply is FIFO with foreground Native/PostgreSQL work and synchronous backfill
may delay later work. It borrows no client principal, SessionState, transaction,
or protocol response path. It does not rotate either evidence domain, tick or
reset the scheduler, retry, generate SQL, persist tokens/proposals/requests,
create an audit log, or build Columnar projections. Crash/recovery reuse the
ordinary named-index transaction and existing WAL/coordinator path.

Known limitations are deliberate: authentication is Unix filesystem access
only; there is no peer UID audit, persistent operator identity, durable approval
log, dedupe database, automatic design, Columnar apply, background index builder,
or independent operator-plane restart lifecycle.
