# Adaptive Operations Phase 31 — Stable receipt namespaces

Phase 31 upgrades Server Physical Design mutation receipts from NBMR v1 to v2
before any receipt identity is exposed over a wire protocol.

A bare numeric receipt ID is ambiguous after offline journal replacement: the
old and new journal may both contain receipt 7. NBMR v2 therefore separates the
durable database incarnation from a new OS-random, nonzero journal incarnation.
The stable receipt identity is the pair of journal incarnation and receipt ID.

- Fresh journals write and sync the exact 44-byte v2 header before use.
- Existing v2 journals retain their incarnation across evidence rotation and
  Server/Physical Design runtime restart.
- Valid v1 journals migrate without renumbering or re-encoding their complete
  records. Publication uses the reserved `<journal>.next` shadow, shadow sync,
  atomic rename, and parent sync.
- Partial final v1 records are omitted from the migrated image; complete
  corruption fails closed. Unresolved v1 Begins are migrated first and then
  reconciled under the new v2 namespace.
- Scoped pagination binds continuation cursors to the current incarnation and
  returns `JournalChanged` for a cursor from a replacement journal. The legacy
  unscoped API remains a current-journal-local convenience.
- Read-only status exposes the incarnation, recovery gate, latest receipt ID,
  and fixed page limit. Status and receipt reads remain available during a
  recovery-required gate and expose no private path or database/runtime state.
- Existing Begin/Outcome ordering, Core mutation authority, reconciliation,
  capacity behavior, and the absence of automatic Physical Design remain
  unchanged.

NBMR v2 is crash-safe and namespace-stable, not tamper-evident or anti-rollback
against a privileged filesystem administrator. Receipt externalization and its
exact encoding remain deferred to a later phase.

See [physical-design-mutation-receipts-v2.md](physical-design-mutation-receipts-v2.md)
for the binary, migration, recovery, cursor, and limitation contract.
