# Adaptive Operations Phase 29 — Manifest v9 and NBOP v4

Phase 29 carries the Phase 28 programmatic managed-Columnar placement authority
to an explicitly operator-approved local control path.

- Manifest v9 is the only current deployment contract. It adds strict optional
  `physical_design.columnar_apply` policy and a required operator
  `allow_physical_columnar_apply` gate.
- The policy resolves relative roots from the manifest directory, requires a
  pre-existing directory, and permits an explicit Snapshot/Incremental
  allowlist. Builder overrides must match the manifest policy when the operator
  gate grants Columnar apply.
- NBOP v4 retains the v3 frame and size limits, rejects v3, and exposes
  independent Index and Columnar apply status while sharing one ephemeral
  runtime token when either domain is authorized.
- The frozen recommendations result remains the Phase 30 `runtime_token` plus
  `report` shape. Capability objects belong to status and the v4 error set does
  not gain an outcome-uncertain variant; mutating clients classify ambiguity
  locally.
- `apply_physical_columnar` accepts only explicit typed approval inputs: the
  current runtime token, evidence epoch, table, ordered columns, mode, and
  logical placement key. Paths and proposal objects never cross the wire.
- The local listener performs permission admission; the sole Database worker
  performs ordered revalidation and one immediate Core proposal/apply command.
  Exact retries, registered conflicts, stale runtime/evidence, occupancy, and
  recovery-required publication retain typed outcomes and stable error codes.
- No automatic stream enablement, refresh, maintenance, scheduling, retry,
  evidence rotation, audit persistence, or new persistent format is introduced.

The operator CLI is documented in [server-operator-protocol-v4.md](server-operator-protocol-v4.md).
Manifest details are in [server-manifest-v9.md](server-manifest-v9.md).
