# Adaptive Operations Phase 30 — Durable mutation receipts

Phase 30 adds optional crash-reconciled receipts around the four explicit
Server Physical Design apply paths.

- Native and PostgreSQL builders accept the same no-default, programmatic-only
  receipt configuration. A final receipt configuration without a final
  Physical Design runtime is a typed startup error.
- Core exposes only the existing durable Schema Catalog incarnation through a
  read-only database-identity API. NBMR remains wholly owned by
  `netbadb-server`.
- The sole Database worker owns the journal and opens it only after Database
  and NBPC recovery. NBMR v1 is historical; current binaries migrate complete
  valid v1/v2 histories and atomically publish new NBMR v3 files before
  readiness.
- Every programmatic or local-operator Index/Columnar apply receives a durable
  nonzero monotonic Begin before the existing worker flow and a durable coarse
  Outcome afterward. Receipt failure never causes compensating physical-design
  rollback.
- One unresolved Begin is reconciled read-only against exact current physical
  state on restart. Protected v3 torn tails are repaired. Ambiguous legacy
  tails, complete corruption, wrong database identity, and unsupported versions
  fail closed.
- Programmatic inspection provides bounded ascending pagination. Public
  receipts contain typed logical targets and source (`Programmatic` or
  `LocalOperator`) but no paths, SQL, identity, token, principal, session,
  address, or timestamp.
- Outcome-write ambiguity gates later receipt-controlled applies until restart
  reconciliation while ordinary SQL, protocol state, and worker lifetime stay
  independent.
- Manifest v9, NBOP v4, Native Protocol v2, PostgreSQL wire behavior,
  Inspection JSON v7, SDK Schema Spec, and every database persistent format
  remain frozen. There is still no automatic physical design.

The exact binary and recovery contract is documented in
[physical-design-mutation-receipts-v1.md](physical-design-mutation-receipts-v1.md)
and the current [NBMR v3](physical-design-mutation-receipts-v3.md).
