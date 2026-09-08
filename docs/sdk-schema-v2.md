# SDK Schema Spec v2

SDK Schema Spec v2 is the current language-neutral input for code generation
and schema-driven tooling. It retains the strict v1 object shapes, stable IDs,
declaration ordering, canonical-schema validation, and unknown-field rejection,
and extends `physical_type` to the complete Physical Types v2 set:

```text
bool
int8 int16 int32 int64 int128
uint8 uint16 uint32 uint64 uint128
float32 float64
text bytes
```

The parser continues to accept Schema Spec v1 with exactly its historical
`bool`, `int64`, `uint64`, and `text` grammar. A v1 document using a new type is
rejected; it is not silently interpreted as v2. Version 2 accepts the complete
set, and unknown versions are rejected as `UnsupportedVersion`.

```json
{
  "version": 2,
  "tables": [{
    "id": 1,
    "name": "events",
    "columns": [{
      "id": 1,
      "name": "payload",
      "physical_type": "bytes",
      "semantic_type": null,
      "nullable": false,
      "primary_key": false
    }]
  }]
}
```

The Go generator maps the supported widths to `int8` through `int64`, `uint8`
through `uint64`, `float32`, `float64`, `string`, and `[]byte`. It explicitly
rejects Int128 and UInt128 because Go has no built-in lossless 128-bit integer
representation. Parsing a v2 schema and supporting it in a particular target
are separate capability checks.

Schema Spec JSON remains distinct from Canonical Schema encoding and deployment
manifests. Rust remains the sole authority for canonical fingerprints.
