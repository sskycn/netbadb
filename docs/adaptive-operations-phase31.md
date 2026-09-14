# Adaptive Operations Phase 31 — Stable receipt namespaces

Phase 31 upgrades Server Physical Design mutation receipts from NBMR v1 to v2
before any receipt identity is exposed over a wire protocol.

NBMR v2 remains the historical namespace format. The final Phase 29–31 closeout
keeps its incarnation semantics while making NBMR v3 the only current write
format.

A bare numeric receipt ID is ambiguous after offline journal replacement: the
old and new journal may both contain receipt 7. NBMR v2 therefore separates the
durable database incarnation from a new OS-random, nonzero journal incarnation.
The stable receipt identity is the pair of journal incarnation and receipt ID.

- Fresh journals atomically publish a complete synced 44-byte v3 header before
  the final path becomes visible.
- Existing v2 journals retain their incarnation across evidence rotation and
  Server/Physical Design runtime restart.
- Complete valid v1/v2 histories migrate without changing receipt semantics or
  IDs; v2 migration preserves its journal incarnation. Records are re-encoded
  with the protected v3 fixed header. Publication uses a random same-directory
  `create_new` temporary, sync, atomic publication, and parent sync. Historical
  `.next` objects are never touched.
- A legacy tail shorter than its claimed length is ambiguous and fails closed;
  it is not silently omitted. Unresolved legacy Begins reserve recovered
  Outcome capacity before publication and reconcile under the v3 namespace.
- Scoped pagination binds continuation cursors to the current incarnation and
  returns `JournalChanged` for a cursor from a replacement journal. The legacy
  unscoped API remains a current-journal-local convenience.
- Read-only status exposes the incarnation, recovery gate, latest receipt ID,
  and fixed page limit. Status and receipt reads remain available during a
  recovery-required gate and expose no private path or database/runtime state.
- Existing Begin/Outcome ordering, Core mutation authority, reconciliation,
  capacity behavior, and the absence of automatic Physical Design remain
  unchanged.

NBMR v3 is crash-recoverable and namespace-stable. V1/v2 remain readable only
when fully valid; their unprotected length prefix cannot justify general tail
repair. None of these formats is tamper-evident or anti-rollback against a
privileged filesystem administrator. Receipt externalization remains deferred.

See [physical-design-mutation-receipts-v3.md](physical-design-mutation-receipts-v3.md)
for the current binary, migration, recovery, cursor, and limitation contract.
