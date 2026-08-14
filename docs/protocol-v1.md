# NetbaDB binary protocol v1

This document is the language-neutral byte contract for experimental NetbaDB
protocol version 1. It is versioned, but the project has not declared a
permanent compatibility policy. Multi-byte integers are little-endian. Strings
are strict UTF-8 prefixed by a little-endian `u32` byte length.

Protocol v1 processes one complete request before the next request. There is no
pipelining or multiplexing. A client request ID must be nonzero, and every
response frame for that request echoes it.

## Frame

Every client and server message uses this 24-byte header:

| Offset | Width | Field |
| --- | ---: | --- |
| 0 | 4 | magic `NDBP` |
| 4 | 2 | protocol version `1` |
| 6 | 2 | message kind |
| 8 | 2 | flags, must be zero |
| 10 | 2 | reserved, must be zero |
| 12 | 4 | payload byte length |
| 16 | 8 | nonzero request ID |

Payloads are limited to 16 MiB (`16,777,216` bytes). A decoder validates this
limit before allocation. Clean EOF before any header byte is distinct from a
partial header or payload, which is a truncated frame. A payload decoder must
consume exactly the declared bytes.

Every repeated collection (`table identities`, `result columns`, and `row
values`) is additionally limited to 65,536 items. Encoders and decoders enforce
this limit before reserving collection storage, preventing a compact sequence
of one-byte values from expanding into an unbounded in-memory vector.

## Message kinds

Client kinds:

| Tag | Message |
| ---: | --- |
| `0x0001` | Hello |
| `0x0002` | Execute |
| `0x0003` | Begin |
| `0x0004` | Commit |
| `0x0005` | Rollback |
| `0x0006` | Analyze |
| `0x0007` | Ping |

Server kinds:

| Tag | Message |
| ---: | --- |
| `0x8001` | HelloAck |
| `0x8002` | QueryStart |
| `0x8003` | QueryRow |
| `0x8004` | QueryEnd |
| `0x8005` | AffectedRows |
| `0x8006` | TransactionStarted |
| `0x8007` | TransactionCommitted |
| `0x8008` | TransactionRolledBack |
| `0x8009` | AnalyzeAck |
| `0x800A` | Pong |
| `0x8FFF` | Error |

A client decoder rejects server-only kinds, and a server decoder rejects
client-only kinds.

## Client payloads

- `Hello`, `Commit`, `Rollback`, and `Ping` have empty payloads.
- `Execute` is `u32 sql_byte_length` followed by strict UTF-8 SQL bytes.
- `Begin` is one `u64 TableId`. Transactions belong to this one table heap and
  do not imply cross-table atomicity.
- `Analyze` is one `u64 TableId`.

Hello must be the first successful request. A second Hello is an error.

## Handshake

`HelloAck` contains:

| Field | Encoding |
| --- | --- |
| protocol version | `u16`, value 1 |
| reserved | `u16`, zero |
| maximum frame payload | `u32` |
| capabilities | `u64` bitset |
| table count | `u32` |
| table identities | repeated `u64 TableId` + 32 fingerprint bytes |

Tables remain in canonical schema declaration order. Identity is the pair of
`TableId` and fingerprint, not its position. Each fingerprint is the existing
SHA-256 `SchemaFingerprint` of the canonical table schema encoding.

Capability bits are:

| Bit | Value | Meaning |
| ---: | ---: | --- |
| 0 | `0x1` | explicit table-scoped transactions |
| 1 | `0x2` | explicit ANALYZE API |
| 2 | `0x4` | streamed query-result messages |

TLS and authentication are not advertised because protocol v1 transport does
not implement them.

## Query results

A query response is always:

```text
QueryStart
QueryRow × row_count
QueryEnd
```

An empty result still sends `QueryStart` followed by `QueryEnd(0)`. All frames
echo the request ID.

`QueryStart` contains `u32 column_count`, followed by each result column:

1. column name string;
2. SemanticType encoding;
3. `u8 nullable`, exactly 0 or 1.

Result metadata does not send catalog IDs, relation bindings, row locators, or
storage representation.

`QueryRow` contains `u32 value_count` followed by scalar encodings. The count
must match QueryStart's column count. `QueryEnd` contains one `u64 row_count`,
equal to the number of emitted QueryRow messages.

`AffectedRows` contains one `u64 count`.

## SemanticType

SemanticType is:

| Field | Encoding |
| --- | --- |
| physical type | `u8` tag |
| semantic-name presence | `u8`, 0 or 1 |
| reserved | `u16`, zero |
| optional semantic name | string when presence is 1 |

Physical type tags are:

| Tag | Type |
| ---: | --- |
| 1 | Bool |
| 2 | Int64 |
| 3 | UInt64 |
| 4 | Text |

## ScalarValue

| Tag | Value payload |
| ---: | --- |
| 0 | NULL, no payload |
| 1 | Bool: one byte, exactly 0 or 1 |
| 2 | Int64: little-endian `i64` |
| 3 | UInt64: little-endian `u64` |
| 4 | Text: strict UTF-8 string |

NULL is an explicit database value. Before sending a core query result, the
session validates value count, runtime physical type, and column nullability.

## Transactions and ANALYZE

Successful Begin, Commit, and Rollback return `TransactionStarted`,
`TransactionCommitted`, and `TransactionRolledBack`, each with an empty
payload. Commit and rollback failures retain the transaction handle for retry;
only success clears it. A terminal transaction rolled back by failed DML is
removed from the session. A compile error leaves an Active transaction usable.

Analyze returns empty `AnalyzeAck`. It is rejected while the session owns a
transaction. Ping remains legal and returns empty `Pong` in any post-handshake
state.

Session disconnect is explicit and fallible: the transport must call session
close, which rolls back an owned transaction or returns the rollback failure
without silently discarding its state.

## Errors

Error payloads are:

| Field | Encoding |
| --- | --- |
| error code | `u16` |
| transaction state | `u8` |
| reserved | `u8`, zero |
| message | strict UTF-8 string |

Messages use Rust `Display` text for diagnostics, not `Debug` output or Rust
layout. Error diagnostic text is limited to 16,777,208 bytes so that the fixed
eight-byte Error prefix plus the string remains encodable in one frame. Servers
truncate oversized diagnostics at a UTF-8 boundary. Stable error codes are:

| Code | Meaning |
| ---: | --- |
| 1 | Protocol |
| 2 | HandshakeRequired |
| 3 | AlreadyHandshaken |
| 4 | TransactionAlreadyActive |
| 5 | NoActiveTransaction |
| 6 | OperationNotAllowedInTransaction |
| 7 | Compile |
| 8 | Schema |
| 9 | Storage |
| 10 | Execution |
| 11 | Database |
| 12 | ResponseTooLarge |
| 13 | InternalResultMismatch |

Wire transaction-state tags are independent of Rust enum discriminants:

| Tag | State |
| ---: | --- |
| 0 | None |
| 1 | Active |
| 2 | RollbackRequired |
| 3 | CommitPending |
| 4 | RollbackPending |

Committed and RolledBack are terminal and therefore represented as None after
the session clears the handle.

## Compatibility gates

Golden-byte tests cover the header, both direction tag spaces, Hello/HelloAck,
Begin, Execute, result metadata and values, affected rows, and errors. A change
to layout, byte order, tags, or scalar/type encoding requires an explicit
protocol compatibility decision rather than silently updating those bytes.

Protocol v1 does not define TCP lifecycle, TLS, authentication, prepared
statements, parameter binding, index/checkpoint administration, or a remote
SDK. Those are later phases and do not change database file formats.
