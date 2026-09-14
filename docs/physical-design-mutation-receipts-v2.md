# Physical Design Mutation Receipts — NBMR v2

NBMR v2 is the historical namespace format. It added the stable journal
incarnation while retaining v1 record framing. Current binaries read complete,
valid v2 histories, preserve their incarnation while migrating to
[NBMR v3](physical-design-mutation-receipts-v3.md), and write only v3.

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

Records remain capped at 64 KiB. The historical configured minimum accounted
for the 44-byte v2 header plus one maximum Begin and one maximum Outcome.
Current v3 migration re-encodes the complete valid history and also reserves a
maximum recovered Outcome when the final Begin is unresolved. Valid history is
never compacted, rotated, or discarded to satisfy capacity.

Outcome tag 7 (`Failed`) remains reserved/historically decodable. The current
runtime does not emit it and does not map arbitrary `DatabaseError` values to a
definitive failure.

## Historical migration behavior and current handling

Phase 31 originally migrated v1 to v2 through the fixed sidecar:

```text
<journal-path>.next
```

Current code no longer treats that name as owned: pre-existing regular files,
other NBMR files, hard links, symlinks, dangling symlinks, and directories at
`.next` are ignored and never truncated, deleted, chmodded, or overwritten.
Current v1/v2 migration uses an unpredictable same-directory `create_new`
temporary and publishes a complete v3 image atomically.

A failure before publication leaves the legacy primary authoritative and
byte-for-byte unchanged. After atomic publication the complete v3 image is the
only final-path history; parent-directory sync completes its durable name
transition. A v1 candidate incarnation from an unpublished temporary has no
public effect, while v2 migration always carries the published v2 incarnation
forward.

The old framing cannot validate its length before using that length to locate
the checksum. Consequently a tail shorter than its claimed length is
ambiguous, not a proven torn record. Current migration fails closed and leaves
the source byte-for-byte unchanged rather than omitting it. A complete invalid
record also fails closed. Admission reserves enough v3 capacity for a recovered
Outcome before publication when the legacy history ends in an unresolved
Begin.

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

NBMR v2 is checksummed and namespace-stable, but its unprotected length prefix
does not permit unambiguous repair of every damaged tail. It is not
tamper-proof, rollback-proof, hash-chained, signed, or cryptographically
authenticated against an administrator who can replace files. A byte-for-byte
copy intentionally retains the same receipt-history identity.

Manifest v9, NBOP v4, Native Protocol v2, PostgreSQL wire behavior, Inspection
JSON v7, SDK Schema Spec, `netbadbd`, all CLI surfaces, and all Database
persistent formats remain unchanged. Receipt wire encoding is deferred.
