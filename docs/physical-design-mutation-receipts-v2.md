# Physical Design Mutation Receipts — NBMR v2

NBMR v2 is the current, bounded Server-owned journal for explicit Physical
Design mutation receipts. It adds a stable journal namespace while retaining
the v1 Begin/Outcome record contract exactly. NBMR remains operational evidence
about Database truth, not a transaction participant or mutation authority.

## Identity and header

A receipt has three distinct identity levels:

```text
database incarnation  identifies one durable database installation
journal incarnation   identifies one durable receipt history
receipt ID             orders one request inside that history
```

The 16-byte journal incarnation is generated once from OS randomness, must be
nonzero, and is persisted before a new journal becomes usable. It is not
derived from the path, time, process, database identity, runtime token, or
receipt ID. Reopening or byte-copying the journal preserves its incarnation;
creating a replacement journal creates a new namespace in which numeric IDs may
start at one again.

All integers are little-endian. The exact 44-byte v2 header is:

| Offset | Bytes | Meaning |
| ---: | ---: | --- |
| 0 | 4 | ASCII `NBMR` |
| 4 | 2 | version `2` |
| 6 | 2 | zero reserved bytes |
| 8 | 16 | durable Schema Catalog/database incarnation |
| 24 | 16 | durable NBMR journal incarnation |
| 40 | 4 | CRC32C over bytes 0 through 39 |

The database incarnation prevents attaching a journal to another database. The
journal incarnation prevents a persisted receipt cursor from being silently
reinterpreted after journal replacement. Neither identity is authentication.

## Records and capacity

The v1 record framing and every body/tag remain unchanged:

```text
u32 payload_bytes
u8  record_tag       // 1 Begin, 2 Outcome
u8  reserved[3]      // zero
u64 receipt_id       // nonzero, strictly increasing Begin IDs
u8  body[payload_bytes - 12]
u32 crc32c           // length prefix through final body byte
```

Records remain capped at 64 KiB. The configured minimum now accounts for the
44-byte v2 header plus one maximum Begin and one maximum Outcome. Migration
also requires the complete valid v1 history plus the 16-byte header expansion
to fit `max_file_bytes`; otherwise startup returns a typed migration-capacity
error without changing the v1 file. Valid history is never compacted, rotated,
or discarded to satisfy capacity.

## Atomic v1 migration

Current binaries read v1 and v2 but write only v2. A valid v1 journal is fully
decoded before publication. The migration preserves every complete record and
receipt ID byte-for-byte, including one unresolved Begin, generates one
candidate journal incarnation, and builds a complete v2 image at the reserved
same-directory sidecar:

```text
<journal-path>.next
```

Only that exact sidecar is owned by NBMR migration. A stale regular sidecar may
be truncated and rebuilt; a symlink, directory, or other non-regular object
fails closed. Publication writes and syncs the shadow, atomically renames it
over the v1 primary, then syncs the parent directory. The v1 primary is never
rewritten or pre-truncated.

A crash before rename leaves v1 authoritative and a later startup rebuilds the
reserved shadow. A crash after rename but before parent sync may recover either
the old valid v1 or new valid v2 filesystem state; both reopen safely. A
candidate incarnation from an unpublished shadow may be replaced on retry, but
the incarnation in a published v2 file never changes.

An incomplete final v1 record is omitted from the v2 image under the existing
valid-prefix theorem. A complete checksum-invalid or structurally invalid
record fails closed and leaves the v1 primary unchanged. Migration happens
after Database/NBPC recovery and before unresolved-receipt reconciliation, so a
recovered Outcome is appended under the newly published v2 namespace.

## Scoped inspection

The stable durable receipt identity is:

```text
(journal incarnation, receipt ID)
```

`mutation_receipts_scoped(after, limit)` returns the current incarnation and a
bounded ascending page. Its continuation cursor binds that incarnation to a
nonzero receipt ID. A cursor from another journal returns `JournalChanged` and
is never interpreted against the current numeric sequence. Future persisted or
external receipt references must use this pair, never a bare receipt ID.

The Phase 30 `mutation_receipts` API remains as a current-journal-local
convenience for source compatibility. It must not be used as a persisted cursor.
`mutation_receipt_status` reports only the journal incarnation,
recovery-required flag, latest receipt ID, and the read limit. Status and both
read APIs remain pure and available while mutation controls are recovery-gated.
They expose no journal path, private Columnar recovery path, database
incarnation, runtime token, SQL, principal, session, address, or timestamp.

## Durability and limitations

Phase 30 Begin-before-apply and Outcome-after-result ordering is unchanged, as
are Index and Columnar restart reconciliation. Outcome ambiguity still gates
later receipt-controlled mutation without rolling back Database truth.

NBMR v2 is crash-recoverable, checksummed, and namespace-stable. It is not
tamper-proof, rollback-proof, hash-chained, signed, or cryptographically
authenticated against an administrator who can replace files. A byte-for-byte
copy intentionally retains the same receipt-history identity.

Manifest v9, NBOP v4, Native Protocol v2, PostgreSQL wire behavior, Inspection
JSON v7, SDK Schema Spec, `netbadbd`, all CLI surfaces, and all Database
persistent formats remain unchanged. Receipt wire encoding is deferred.
