# NetbaDB deployment manifest v3

> Historical format. Current `netbadbd` requires
> [deployment manifest v4](server-manifest-v4.md); v3 is rejected explicitly.

Deployment manifest v3 added one secure transport mode without changing
Protocol v1 or any persistent database format.

```json
{
  "version": 3,
  "listen": "0.0.0.0:7878",
  "limits": {
    "max_connections": 128,
    "idle_timeout_ms": 300000,
    "write_timeout_ms": 30000,
    "max_result_rows": 100000
  },
  "tls": {
    "server_certificate": "certs/server.pem",
    "server_private_key": "certs/server-key.pem",
    "client_ca": "certs/client-ca.pem"
  },
  "tables": [
    {
      "path": "data/users.ndb",
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

Start it with `netbadbd --manifest path/to/netbadb-server.json`. The server
opens existing heaps only; it does not create databases or infer schemas.

## Transport security

There are exactly two supported modes:

- A loopback listener with no `tls` object uses plaintext local-development
  transport.
- Any listener with a `tls` object uses mutual TLS. A verified client
  certificate is mandatory. Loopback mTLS is valid, and every non-loopback
  listener requires mTLS.

There is no anonymous TLS, optional client authentication, or insecure
verification switch. In v3, every certificate that chained to `client_ca`
received the same database capabilities. Per-client, table, and operation
authorization arrived with v4; CA membership alone is authentication, not
fine-grained authorization.

The server certificate file may contain a normal chain and must contain at
least one certificate. The private-key file must contain exactly one supported
key, and that key must match the server leaf certificate. `client_ca` must
contain at least one valid trust root. All three paths, as well as table paths,
resolve relative to the manifest directory. TLS material is read and validated
before the database worker starts and before the listener binds.

Certificates are loaded once at startup. Updating them requires restarting
`netbadbd`. Normal TLS certificate chain and validity checks apply; NetbaDB
does not fetch OCSP responses or watch CRLs in this phase.

After a successful handshake, the server derives a runtime-only client identity
as SHA-256 over the verified leaf certificate DER. It is associated with the
worker session until disconnect, is not returned in Protocol v1, and is never
written to a heap, page, WAL, index catalog, or query result.

## Fields, paths, and strictness

- `version` is required and must be `3`; v1 and v2 are rejected.
- `listen` is optional and defaults to `127.0.0.1:7878`.
- `tls` is optional only for loopback listeners. When present, all of
  `server_certificate`, `server_private_key`, and `client_ca` are required.
- `limits` may be omitted or `null`; its individual fields are optional.
- `tables` must be nonempty and preserves canonical declaration order. Each
  entry supplies an existing heap path and a complete validated `TableDef`.
- Unknown top-level, TLS, limits, table, and column fields are rejected.

Relative paths are canonicalized from the directory containing the manifest,
not the process working directory. Paths must exist and identify files. Table
paths must also be unique.

## Operational limits and lifecycle

| Field | Default | Accepted range | Meaning |
| --- | ---: | ---: | --- |
| `max_connections` | 128 | 1–65,536 | Maximum admitted sockets and threads, including pending TLS handshakes. |
| `idle_timeout_ms` | 300,000 | 1–86,400,000 | Read timeout for TLS handshake and subsequent idle or partial NDBP frames. |
| `write_timeout_ms` | 30,000 | 1–86,400,000 | Write timeout for TLS negotiation and protocol responses. |
| `max_result_rows` | 100,000 | 1–10,000,000 | Maximum rows expanded into one protocol response batch. |

Connection admission and socket timeouts happen before the TLS handshake. A
failed, untrusted, or stalled TLS peer never creates a `SessionState` and
receives no NDBP Error. The failure closes only that connection. Once TLS has
authenticated the peer, the ordinary Protocol v1 Hello starts and all existing
session, disconnect rollback, response-limit, and ambiguous committed-outcome
rules apply unchanged.

Deployment manifest v3 changes deployment configuration only. Protocol v1,
Canonical Schema v1, Heap metadata v3, Page v5, WAL v3/record v2, BTree payload
v1, and IndexCatalog v2 remain unchanged.
