# Adaptive Operations Phase 32 — durable receipt exposure

Phase 32 exposes already-proven NBMR operational state without moving mutation
authority. The authority split is deliberate:

```text
Database physical state       says what happened
NBMR v3 operational memory    records explicit Server requests and resolution
Manifest v10                  chooses one bounded daemon-owned journal and read policy
NBOP v5                       presents current-journal logical receipts read-only
```

Manifest v10 adds an optional strict `physical_design.mutation_receipts`
object and an independent required operator read permission. Omission preserves
v9 apply behavior. Read authorization pins the external scope to the exact
manifest-derived canonical path and capacity, while a non-reading embedded host
may still replace the manifest default programmatically. Inspection validates
configuration without touching NBMR.

NBOP v5 carries only the durable journal incarnation plus nonzero receipt ID.
It adds receipt status and bounded scoped pagination, correlates Index and
Columnar apply success or post-Begin semantic failure to that reference, and
freezes explicit Outcome-durability uncertainty. Listener rejection and Begin
failure carry no fabricated reference. Response loss also remains distinct
because no reference was observed.

Receipt reads use the sole Database worker's existing typed read commands and
remain available during recovery gating. They append nothing, mutate nothing,
do not rotate or record evidence, do not tick scheduling or maintenance, and
do not affect Native sessions, PostgreSQL transaction state, or client
transactions. Replacing the configured journal changes the namespace;
reopening or migrating the same journal preserves it. Only the current journal
is read—there is no archive scan, reset, rotation, deletion, replay, or receipt
apply operation.

NBMR stays at v3, including v1/v2 migration readers. Database formats, Native
Protocol v2, PostgreSQL wire, Inspection JSON v7, SQL, SDK Schema Spec, and Core
mutation APIs are unchanged. NBMR is crash-recoverable and checksummed, not a
signed, administrator-tamper-evident, or anti-rollback audit log.

Final hardening gives every active Unix journal inode one cooperative owner via
a nonblocking exclusive advisory lock. Existing v3 is locked before decode;
fresh publication exposes an already locked inode; legacy migration holds the
old and new inode locks across publication and rejects final-path inode
replacement. Post-Begin ambiguous Core errors and Outcome durability failures
are v5 uncertainty with a receipt, while worker/response loss is uncertainty
without a receipt. Begin failure and an existing recovery gate remain
definite-not-started receipt-unavailable failures. Frozen V3 and V4 Rust DTOs
remain exported while only NBOP v5 is accepted on the wire.

See [Manifest v10](server-manifest-v10.md),
[NBOP v5](server-operator-protocol-v5.md), and
[NBMR v3](physical-design-mutation-receipts-v3.md).
