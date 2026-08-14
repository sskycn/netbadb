# NetbaDB deployment manifest v1

> Historical experimental manifest format. Current `netbadbd` requires
> [deployment manifest v4](server-manifest-v4.md); v1 through v3 are rejected
> explicitly.

The deployment manifest is human-readable startup configuration for
`netbadbd`. It is not a database file format, protocol payload, or canonical
schema encoding. JSON whitespace, field order, and Serde behavior never
participate in `SchemaFingerprint`; decoded values construct ordinary
`TableDef` values whose existing canonical encoding remains authoritative.

Start the server with:

```text
netbadbd --manifest path/to/netbadb-server.json
```

Phase 5B opens existing heap files only. It does not create databases, infer a
schema from heap metadata, or accept a schema from a client.

## Document layout

```json
{
  "version": 1,
  "listen": "127.0.0.1:7878",
  "tables": [
    {
      "path": "./data/users.ndb",
      "id": 1,
      "name": "users",
      "columns": [
        {
          "id": 1,
          "name": "id",
          "physical_type": "uint64",
          "semantic_type": "UserId",
          "nullable": false,
          "primary_key": true
        },
        {
          "id": 2,
          "name": "name",
          "physical_type": "text",
          "semantic_type": null,
          "nullable": false,
          "primary_key": false
        }
      ]
    }
  ]
}
```

Unknown fields and manifest versions are rejected. `tables` must be nonempty.
Canonical schema validation rejects duplicate table or column identities,
duplicate names, empty names, and invalid semantic types. Canonicalized table
paths must exist, refer to files, and be unique.

## Fields

- `version` is required and must be the integer `1`.
- `listen` is optional and defaults to `127.0.0.1:7878`. It must parse as a
  numeric `SocketAddr` whose IP is IPv4 or IPv6 loopback. Phase 5B deliberately
  rejects non-loopback listeners because authentication and TLS do not exist.
- `tables` preserves canonical schema declaration order.
- Each table supplies its heap `path`, stable `u64` `id`, canonical name, and
  complete ordered column list.
- Each column supplies its stable `u32` `id`, name, physical and optional
  semantic type, nullability, and primary-key flag.

The only accepted physical type strings are `bool`, `int64`, `uint64`, and
`text`. `semantic_type` is either a UTF-8 nominal type name or JSON `null` for a
plain physical type.

## Paths and schema safety

Relative heap paths resolve against the directory containing the manifest, not
the process working directory. The directory and each heap path are
canonicalized before duplicate-path checks.

The dedicated database worker reconstructs each `TableDef` and calls
`Database::open_tables` inside the worker thread. Storage then compares the
canonical table fingerprint against the fingerprint persisted in the heap. A
change to semantic type, nullability, column identity, order, or any other
canonical field therefore fails startup before the TCP listener is bound.

Deployment manifest v1 changed configuration only and is now obsolete.
Canonical Schema v1, Heap metadata v3, Page v5, WAL v3/record v2, BTree payload
v1, IndexCatalog v2, and wire Protocol v1 remain unchanged.
