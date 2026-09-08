# NetbaDB Inspection JSON v7

Inspection JSON v7 adds the Physical Types v2 machine contract. Version
selection describes every feature actually present in an output: the CLI takes
the maximum of the structural plan version and the physical-type feature
version. Catalogs and statements that use only `bool`, `int64`, `uint64`,
`text`, and `null` retain their historical v3-v6 envelopes byte for byte.

Any catalog column, result field, plan column, expression, literal, index
bound, or partition bound using another physical type raises the envelope to
version 7. This includes physical types nested below partition, index join, or
columnar operators.

## Physical type spellings

V7 retains the legacy spellings and adds:

```text
int8 int16 int32 int128
uint8 uint16 uint32 uint128
float32 float64 bytes
```

## Scalar shapes

Signed and unsigned values through 64 bits use JSON numbers. The 128-bit
values use decimal strings so their full domains do not depend on a consumer's
JSON number precision:

```json
{"kind":"int128","value":"-170141183460469231731687303715884105728"}
{"kind":"uint128","value":"340282366920938463463374607431768211455"}
```

Floats use lowercase, zero-padded canonical IEEE bit strings, not JSON numbers:

```json
{"kind":"float32","bits":"7fc00000"}
{"kind":"float64","bits":"7ff8000000000000"}
```

Signed zero is canonical positive zero, all NaNs use the database's one quiet
NaN representation, and subnormal bits are preserved. Opaque bytes use an
even-length lowercase hexadecimal string and are never interpreted as UTF-8:

```json
{"kind":"bytes","hex":"00ff80"}
```

The v1-v6 documents remain historical contracts and do not gain these values
retroactively.
