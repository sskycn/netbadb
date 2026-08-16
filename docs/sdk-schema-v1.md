# SDK Schema Spec v1

SDK Schema Spec v1 is the language-neutral schema input shared by code
generation and editor tooling. The `netbadb-schema-spec` crate is its single
strict parser and converts the JSON through canonical schema validation before
returning a `Schema`. Current consumers are `netbadb-codegen` and
`netbadb-lsp`; the format itself remains version 1.

The spec describes stable table and column IDs, names, physical and optional
semantic types, nullability, primary-key metadata, and declaration order.

```json
{
  "version": 1,
  "tables": [{
    "id": 1,
    "name": "users",
    "columns": [{
      "id": 1,
      "name": "id",
      "physical_type": "int64",
      "semantic_type": "UserId",
      "nullable": false,
      "primary_key": true
    }]
  }]
}
```

All objects reject unknown fields. Every shown identity field is required;
`nullable` and `primary_key` have no defaults. `semantic_type` is either a
string or JSON `null`. Physical type spelling is exactly `bool`, `int64`,
`uint64`, or `text`; aliases are rejected. Table and column order is preserved.
Version values other than 1 are rejected as unsupported rather than guessed.

## Identity and boundaries

Schema Spec JSON is not Canonical Schema encoding. JSON whitespace, object key
order, and serializer choices never participate in identity. The shared parser
explicitly converts every table to `netbadb_schema::TableDef` and constructs a
validated `Schema`; the generator then calls `TableDef::fingerprint()`.
Canonical Schema v1's
versioned binary bytes and SHA-256 implementation remain the sole identity
authority; generated Go code only embeds and compares the resulting 32 bytes.

Schema Spec is also not deployment manifest v4. It cannot contain heap paths,
listen addresses, limits, TLS keys, authentication, or authorization policy,
and it does not change server bootstrap. Today the two inputs remain separate:

```text
manifest TableDef -> heap fingerprint gate at server startup
SDK Schema Spec -> generated fingerprint gate during client Dial
```

Either mismatch is a hard failure, which detects drift without treating the
code-generation input as runtime configuration.

The diagnostics-only LSP loads this spec once at startup and uses the returned
schema for parser/HIR diagnostics. It does not treat the spec as a deployment
manifest, open database files, or derive physical planner state from it.

## Go target rules

The first generator target accepts ASCII letters, digits, and underscores in
schema names, with a leading letter and nonempty underscore-separated words.
It maps `user_id` to `UserId` and `id` to `Id` without acronym special cases.
Unsupported names, Go package keywords, mapped-name collisions, and one
semantic name used with different physical types are typed generation errors;
the generator never appends numeric suffixes. These restrictions are Go target
concerns and do not narrow Canonical Schema's UTF-8 name model.

Use repeated numeric `--table-id` arguments to generate an authorization-safe
subset. Selection preserves schema declaration order and affects generated
types, rows, and `RequiredSchemas`. Without selection, every table is emitted.

```bash
netbadb-codegen go --schema schema.json --package appdb \
  --output appdb/netbadb_generated.go --table-id 1
```

The output has no timestamps, hostnames, temporary paths, or Git revisions and
records the exact logical schema path and regeneration command. `--check`
generates in memory and performs a byte comparison without writing; stale
output exits unsuccessfully. Generation completes all parsing, canonical
validation, selection, naming, collision, and rendering checks before opening
the output for writing.

Generated table query wrappers accept only the full canonical row shape in
canonical column order. They do not reorder projections or generate SQL.
Protocol v1 has no typed parameters, and primary-key metadata is not uniqueness
enforcement, so Schema Spec v1 does not produce CRUD, query-builder, or
`GetByPrimaryKey` APIs.
