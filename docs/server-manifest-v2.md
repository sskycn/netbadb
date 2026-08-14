# NetbaDB deployment manifest v2

Deployment manifest v2 is the current human-readable startup configuration for
`netbadbd`. It is not a database file format, protocol payload, or canonical
schema encoding. JSON field order and whitespace never participate in
`SchemaFingerprint`; decoded values construct ordinary validated `TableDef`
values, and the heap's canonical fingerprint remains authoritative.

```json
{
  "version": 2,
  "listen": "127.0.0.1:7878",
  "limits": {
    "max_connections": 128,
    "idle_timeout_ms": 300000,
    "write_timeout_ms": 30000,
    "max_result_rows": 100000
  },
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
        }
      ]
    }
  ]
}
```

Start it with `netbadbd --manifest path/to/netbadb-server.json`. Phase 5C1 only
opens existing heap files; it does not create databases, infer schemas, or
accept schemas from clients.

## Fields and strictness

- `version` is required and must be `2`. Historical v1 is rejected rather than
  silently assigned new defaults.
- `listen` is optional and defaults to `127.0.0.1:7878`. Only numeric IPv4 or
  IPv6 loopback `SocketAddr` values are accepted because TLS and authentication
  do not exist.
- `limits` may be omitted or `null`. Every field inside it is optional and uses
  the defaults below. Unknown top-level, limits, table, and column fields are
  rejected.
- `tables` must be nonempty and preserves canonical declaration order. Each
  entry supplies an existing heap path and a complete `TableDef`.
- Physical types accept exactly `bool`, `int64`, `uint64`, and `text`.
  `semantic_type` is a UTF-8 nominal type name or JSON `null`.

Relative heap paths resolve against the canonicalized manifest directory.
Paths must exist, be files, and be unique. Existing schema validation rejects
duplicate identities and invalid definitions. The database worker calls
`Database::open_tables` before listener bind, so any persisted fingerprint
mismatch remains a startup error.

## Operational limits

| Field | Default | Accepted range | Meaning |
| --- | ---: | ---: | --- |
| `max_connections` | 128 | 1–65,536 | Maximum admitted sockets and connection threads, including clients that have not sent Hello. |
| `idle_timeout_ms` | 300,000 | 1–86,400,000 | Socket read timeout while waiting for a first, next, or partial frame. |
| `write_timeout_ms` | 30,000 | 1–86,400,000 | Socket timeout for blocked response delivery. |
| `max_result_rows` | 100,000 | 1–10,000,000 | Maximum rows expanded into protocol response messages. |

An excess connection is closed before session creation and receives no
protocol Error. Read timeout is connection-level inactivity: timeout closes the
session and rolls back an active transaction. It is not a database statement
execution timeout. Write timeout preserves the existing ambiguous-outcome rule
when execution committed before response delivery failed.

`max_result_rows` is checked after synchronous core execution has fully
materialized `QueryResult`, but before any `QueryStart` or `QueryRow` is added to
the response. It bounds protocol response expansion, not executor memory. An
oversized result receives one `ResponseTooLarge` Error and is never truncated.

Deployment manifest v2 changes deployment configuration only. Protocol v1,
Canonical Schema v1, Heap metadata v3, Page v5, WAL v3/record v2, BTree payload
v1, and IndexCatalog v2 remain unchanged.
