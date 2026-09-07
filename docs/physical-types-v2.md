# Physical Types v2

Physical Types v2 extends NetbaDB's language-independent scalar foundation to
the fixed-width Rust scalar families plus UTF-8 text and opaque bytes. A
physical type describes durable representation and runtime operations. A
semantic type may add a nominal name, so `UserId(UInt64)` and `TeamId(UInt64)`
remain incompatible even though their physical representation is identical.

## Type set

| Family | Physical types | Width |
| --- | --- | --- |
| Boolean | `Bool` | 1 byte |
| Signed integer | `Int8`, `Int16`, `Int32`, `Int64`, `Int128` | 1, 2, 4, 8, 16 bytes |
| Unsigned integer | `UInt8`, `UInt16`, `UInt32`, `UInt64`, `UInt128` | 1, 2, 4, 8, 16 bytes |
| Floating point | `Float32`, `Float64` | 4, 8 bytes |
| UTF-8 string | `Text` | variable |
| Opaque octets | `Bytes` | variable |

`usize` and `isize` are deliberately absent: durable database meaning cannot
depend on the pointer width of the process that wrote a file. Date, UUID and
Decimal are future semantic/domain types with contracts beyond an underlying
integer or byte sequence; representation alone does not supply those semantics.

There is no implicit numeric widening. Every runtime scalar retains its exact
physical identity, and expressions compare or aggregate values only when the
typed plan says their types are compatible. Integer `SUM` uses checked
arithmetic of the same width. Floating-point `SUM` uses IEEE arithmetic and
canonicalizes its result.

## Stable float and comparison semantics

`Float32Value` and `Float64Value` canonicalize both signed zeros to positive
zero and every NaN sign/payload to one quiet NaN bit pattern. The database
order is total and shared by expressions, sorting, grouping, indexes,
aggregates and columnar statistics:

```text
-Infinity < finite values < +Infinity < NaN
```

This makes equality, hashing and ordering agree and keeps encoded values stable
across equivalent IEEE inputs. Subnormal values remain distinct and are not
rounded or flushed to zero. `Bytes` is ordered lexicographically by unsigned
octets and is never decoded as UTF-8. `NULL` remains a separate database value;
where an ordering is required it sorts before every non-null scalar.

## Durable encoding and compatibility

Fixed-width payloads use little-endian two's-complement integers or canonical
IEEE bits. `Text` and `Bytes` use a checked `u32` byte length followed by their
payload. Each durable subsystem owns its tag namespace. Existing Bool, Int64,
UInt64, Text and Null tags and payload bytes are immutable. New tags are
append-only in this order:

```text
Int8, Int16, Int32, Int128,
UInt8, UInt16, UInt32, UInt128,
Float32, Float64, Bytes
```

Canonical schema input, row storage, B+Tree metadata/keys, schema catalogs,
columnar files and the native protocol follow that rule independently. Unknown
tags, malformed lengths and truncated values are typed decoding errors; no
decoder falls back to another type.

Heap and LSM rows, WAL/recovery, change streams and reopen paths carry the full
type set. The existing LSM physical layout still requires its separate,
non-null clustering column to be Int64 or UInt64; that key-format capability is
independent of which physical types may appear in the row payload.

## SQL names and literals

NetbaDB accepts explicit fixed-width names including `TINYINT`, `INT16`,
`INT32`, `INT64`, `INT128`, `UINT8` through `UINT128`, `FLOAT32`, `FLOAT64`,
`BYTES`, and their documented PostgreSQL-style aliases. The established SQL
alias `INT8` continues to mean 64-bit signed `BIGINT`; native 8-bit signed
storage is spelled `TINYINT`. `REAL`/`FLOAT4` mean Float32 and
`DOUBLE`/`FLOAT8` mean Float64. `BYTEA` aliases Bytes.

Decimal/exponent literals default to Float64 and integer literals are retained
as exact source text until contextual range checking. Uncontextualized integers
choose Int64, then Int128, then UInt128. `X'00ff'` is the native lossless Bytes
literal; it requires an even number of hexadecimal digits.

## PostgreSQL and Go boundaries

The pgwire adapter publishes only lossless mappings. Signed Int8/Int16 use
`INT2`, Int32 uses `INT4`, and Int64 uses `INT8`. UInt8, UInt16 and UInt32 use
the next wider signed PostgreSQL integer. Float32/Float64 use `FLOAT4`/`FLOAT8`
and Bytes uses `BYTEA`. UInt64, Int128 and UInt128 result columns return an
explicit unsupported-type error because PostgreSQL has no lossless built-in
scalar mapping for them. Parameter narrowing is explicit and range checked.

Generated Go models map the supported fixed widths to `int8` through `int64`,
`uint8` through `uint64`, `float32`, `float64`, `string`, and `[]byte`. Go
generation explicitly rejects Int128 and UInt128 rather than silently changing
their meaning. A future SDK may add a deliberate 128-bit representation.

Future domain types should layer language-independent semantic contracts over
these physical values, preserving the separation between canonical schema,
typed IR and storage representation.
