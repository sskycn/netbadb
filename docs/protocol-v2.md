# NetbaDB binary protocol v2

Protocol v2 is the current native network contract. It preserves the v1 frame,
message, error, transaction-state, and legacy scalar bytes, changes the frame
and `HelloAck` version fields to `2`, and extends the physical/scalar registry
for Physical Types v2. Multi-byte values remain little-endian.

The current server and Rust and Go remote clients support v2 only. A v1 frame
is rejected from its header as an unsupported version, before Hello can
succeed; an inner `HelloAck` version mismatch is also fatal. This prevents an
old client from completing a handshake and discovering an unknown result tag
later. `docs/protocol-v1.md` and frozen v1 golden vectors remain the historical
v1 byte contract.

## Append-only type registries

Physical type tags are:

| Tag | Type |
| ---: | --- |
| 1 | Bool |
| 2 | Int64 |
| 3 | UInt64 |
| 4 | Text |
| 5 | Int8 |
| 6 | Int16 |
| 7 | Int32 |
| 8 | Int128 |
| 9 | UInt8 |
| 10 | UInt16 |
| 11 | UInt32 |
| 12 | UInt128 |
| 13 | Float32 |
| 14 | Float64 |
| 15 | Bytes |

Scalar tags keep `0` as Null and use the same `1`-`15` registry. Future
additions must append tags; existing meanings and payloads are immutable.

Integers use fixed-width little-endian two's-complement or unsigned payloads.
Int128 and UInt128 therefore carry exactly 16 bytes. Float32 and Float64 carry
the database's canonical IEEE bits: negative zero is positive zero, every NaN
uses one quiet-NaN bit pattern, and subnormal bits are preserved. Bytes is a
checked little-endian `u32` length followed by arbitrary octets.

The Go v2 client cannot represent Int128 or UInt128. It fails deterministically
when `QueryStart` declares either result type, including for a zero-row result.
This client capability restriction does not change the v2 wire registry.

Protocol v2 does not change Heap, LSM, BTree, Canonical Schema, SchemaCatalog,
or Columnar persistent format versions.
