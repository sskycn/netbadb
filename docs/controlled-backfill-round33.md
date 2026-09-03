# Controlled backfill phase — Round 33

Round 33 extends the Round 32 managed Single Heap backfill lifecycle with a
post-backfill index phase. The supported sequence is:

```text
Composing → BackfillOpen → Refining → IndexFinalizing → Finalized
```

`CREATE INDEX` and `DROP INDEX` are accepted logically after backfill DML and
compatible schema refinement. The first successful index operation enters
`IndexFinalizing`; after that point data access, table DDL, and further schema
refinement are rejected, while additional index operations may update the
transaction-local final inventory.

Index creation reserves its exact durable `IndexId` before changing the
overlay. No B-tree is allocated during statement execution. Finalization first
retargets the private Heap from the provisional schema to the final schema,
then computes the delta from the staged index inventory to the final inventory.
Drops run in ascending ID order and creates run in ascending reserved-ID
order. New B-trees are built by the existing storage primitive from the
current transaction-visible staged rows, so INSERT, UPDATE, and DELETE effects
are included without a second Heap copy or SQL replay.

The staged resource keeps the same `StorageId`, row identities, and physical
bundle. Existing Round 32 schema-only finalization continues to use NBSJ tag
32. Round 33 appends a migration-specific index reservation record and an
immutable index-aware finalization record containing exact final index
identity/high-water evidence; it does not reinterpret older journal bytes or
duplicate the prepared NBSC.

Before the single CORD decision, the committed base remains authoritative and
the staged resource is discarded on rollback or crash. After CORD, recovery
promotes the already synchronized staged resource and does not rebuild indexes.
Transient create/drop operations burn their reserved IDs but create no
physical B-tree; same-name recreation receives a new ID.

The native Round 33 regression covers the primary
`ADD → UPDATE → SET NOT NULL → CREATE INDEX` migration, same-staged-storage
publication, transaction-visible backfill values, post-backfill
drop/recreate, and ID non-reuse. External PostgreSQL client validation remains
unverified when the required clients are unavailable.

Round 35 preserves public Round 33 CREATE/DROP behavior and IndexId reservation.
Its private branch feeds `RefiningAfterEvacuation` into `IndexFinalizing`, while
finalization now diffs the current physical S2 inventory rather than the base
inventory, preventing a second retirement of an evacuated index.
