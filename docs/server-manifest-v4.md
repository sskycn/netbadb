# NetbaDB deployment manifest v4

> Historical format. Current `netbadbd` requires
> [deployment manifest v5](server-manifest-v5.md); v4 is rejected explicitly.

Deployment manifest v4 added required deployment authorization without
changing Protocol v1 or any persistent database format.

```json
{
  "version": 4,
  "listen": "0.0.0.0:7878",
  "tls": {
    "server_certificate": "certs/server.pem",
    "server_private_key": "certs/server-key.pem",
    "client_ca": "certs/client-ca.pem"
  },
  "authorization": {
    "clients": [
      {
        "certificate_sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        "tables": [
          {
            "table_id": 1,
            "read": true,
            "write": false,
            "transaction": true,
            "analyze": false
          }
        ]
      }
    ]
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
uses the listed existing Heap paths to locate an already initialized runtime
schema catalog. Relative Heap and TLS paths resolve from the manifest directory.
Table definitions are required exact subset expectations (TableId and canonical
fingerprint), not the live logical schema. All committed catalog resources open
before authorization filters session visibility, including extra persisted tables
not listed here. Provisioning must use a database create API or explicit complete
legacy import once; ordinary startup never creates a database or repairs a missing
catalog. See [Round 17](runtime-schema-catalog-round17.md).

## Authentication and principal admission

Transport authentication and authorization are separate. Mutual TLS proves
that the peer certificate chains to `client_ca`. The database worker then
matches SHA-256 of the verified leaf-certificate DER against
`authorization.clients`. A CA-trusted but unlisted certificate is disconnected
before Protocol v1 Hello and receives no schema metadata or protocol Error.

`certificate_sha256` is exactly 64 hexadecimal characters with no separators
or whitespace; upper- and lowercase are accepted and decoded to 32 bytes.
Fingerprints are public identities, not secrets. Reissuing or rotating a leaf
certificate changes its fingerprint, so the manifest must be updated and the
server restarted. Policies and TLS material are loaded once; there is no hot
reload.

Loopback plaintext has no per-process identity. A plaintext configuration must
omit TLS, provide exactly one `authorization.local_plaintext` principal, and
leave `clients` empty:

```json
{
  "authorization": {
    "local_plaintext": {
      "tables": [
        { "table_id": 1, "read": true }
      ]
    },
    "clients": []
  }
}
```

Mutual TLS must omit `local_plaintext` and configure at least one client.
Duplicate fingerprints, duplicate table grants, principals with neither table grants
nor schema_admin, grants with
no enabled operation, and unknown TableIds are startup errors. Unknown JSON
fields are rejected. `authorization` itself is required; omission never means
full access.

## Table operations

Every principal explicitly lists canonical `TableId` grants. There are no
wildcards, defaults, roles, groups, inheritance, SQL GRANT/REVOKE, or row- or
column-level policies. Missing permission booleans default to `false`:

- `read` permits SELECT access to that table. Every table in a typed JOIN must
  be readable.
- `write` permits INSERT, UPDATE, and DELETE, including target-row evaluation.
  It does not imply `read`.
- `transaction` permits `Begin(TableId)` only. Implicit single-statement SQL
  remains controlled by `read` or `write`; Commit and Rollback remain available
  to safely finish an owned transaction.
- `analyze` independently permits `Analyze(TableId)`.

After principal admission, HelloAck lists only tables for which that principal
has at least one operation. The remaining identities preserve canonical schema
declaration order. Capability bits continue to describe server features, not
the current principal's grants.

SQL authorization uses compiler-resolved canonical TableIds, never SQL string
matching, aliases, table names, paths, or manifest positions. Invalid SQL keeps
the existing Compile error. A valid but forbidden request is rejected before
planning or execution, does not acquire a writer, and does not alter an active
transaction.

Protocol v1 has no dedicated authorization error tag. Operation denials use
the existing generic Database code with an `authorization denied ...`
diagnostic. Admission denial for an unlisted mTLS identity happens before Hello
and sends no NDBP response.

## Schema mutation authorization

Round 19 adds an optional principal boolean `schema_admin`, default false, to
`authorization.local_plaintext` and each certificate principal. It is an additive
v4 configuration field; existing v4 manifests still read with schema DDL denied.
No database or Protocol v1 version changes. A schema-only principal may explicitly
set `"schema_admin": true, "tables": []`. Unknown fields still fail validation.

`schema_admin` authorizes generic schema-write access for CREATE TABLE and index
DDL. CREATE/DROP INDEX additionally requires its existing target table write grant.
It does not imply ordinary table read/write/analyze permission or PostgreSQL roles,
ownership, SQL GRANT or a security catalog. Embedded filesystem owners remain
outside network policy. Native Begin(TableId) still requires its transaction grant;
PostgreSQL schema-only admins may begin a table-independent transaction.

During the creating transaction only, an administrator may read/write that exact
staged TableId. Commit/rollback ends this exception. No grant or manifest is mutated,
no automatic durable access is added, and creator SELECT after commit is denied
without external TableId grants. Catalog visibility remains filtered independently.
Authorization precedes writer admission, identity reservation and resource creation.
PG explicit transaction failures retain the frontend's E/25P02 behavior.

## Operational notes

The local `netbadb inspect` CLI reuses this manifest's complete validation and
table bootstrap, but it is not a Protocol session. Server authorization protects
Protocol sessions; it does not restrict a local process that already has
filesystem access to the manifest and database files. Local catalog inspection
therefore shows every configured table and never emits authorization grants,
certificate fingerprints, TLS paths, listen addresses, or storage paths.

Stop `netbadbd` and every embedded process using the configured database files
before local inspection. NetbaDB currently has no cross-process database-file
lock, and concurrent multi-process access is unsupported. The CLI does not use
ports or process IDs to guess ownership.

Offline opening uses normal `Database::open_tables` startup recovery. If the
previous owner crashed, opening may redo or undo WAL state before inspection is
returned. The CLI is not a forensic no-write reader and has no recovery-bypass
option. The SQL supplied to `inspect statement`, including INSERT, UPDATE, or
DELETE, is never executed.

The v3 limits and transport rules remain: plaintext listeners are loopback
only, non-loopback listeners require mTLS, limits are bounded, and disconnect
cleanup rolls back active transactions. Metrics add
`authorization_denials_total` without identity, TableId, or SQL labels.
`tls_handshakes_total` counts successful certificate-authenticated handshakes;
`authenticated_connections_total` counts authenticated worker sessions that
were also admitted. Thus an unlisted trusted certificate increments the TLS
handshake and authorization-denial counters, but not authenticated connections.

Manifest v4 changes deployment configuration only. The current independent
persistent contracts are Protocol v1, Canonical Schema v1, Heap metadata v4,
MVCC tuple v1, transaction-status v1, Page v5, WAL v3/record v2, BTree payload
v1, and IndexCatalog v3 (with legacy v2 decode).
