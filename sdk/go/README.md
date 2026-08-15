# NetbaDB Go client

This module is the independent, standard-library Protocol v1 client for
`netbadbd`:

```text
Go application -> github.com/sskycn/netbadb/sdk/go -> Protocol v1 -> netbadbd
```

It uses no cgo, Rust FFI, shared Rust memory, JSON execution IR, or third-party
runtime dependency. A `Client` owns one protocol connection and session. It is
not safe for concurrent use, and an unfinished `Rows` blocks the next request.

## Dial and schema gate

Plaintext is accepted only when the connected TCP peer is a loopback IP:

```go
client, err := netbadb.Dial(ctx, netbadb.Config{
    Address: "localhost:7878",
    RequiredSchemas: []netbadb.TableIdentity{{
        TableID:     1,
        Fingerprint: usersFingerprint,
    }},
    RequiredCapabilities: netbadb.CapabilityStreamedQueryResults,
})
```

`Dial` performs Hello automatically. Every required table must be visible and
have the exact expected fingerprint; extra authorized tables are allowed. The
client compares `[32]byte` constants only. Canonical schema encoding and
fingerprint generation remain Rust-authoritative.

For mutual TLS, use the normal Go TLS types. The config is cloned, hostname/IP
verification remains enabled, and `InsecureSkipVerify` is rejected:

```go
client, err := netbadb.Dial(ctx, netbadb.Config{
    Address: "db.example.com:7878",
    TLS: &tls.Config{
        RootCAs:      roots,
        Certificates: []tls.Certificate{clientCertificate},
    },
    RequiredSchemas: requiredSchemas,
})
```

`client.ServerInfo()` returns the negotiated version, payload bound,
capabilities, and a copy of visible table identities.

## Query, Exec, and ANALYZE

```go
rows, err := client.Query(ctx, "SELECT id, name FROM users ORDER BY id")
if err != nil { /* handle */ }
defer rows.Close()

for rows.Next() {
    values := rows.Values()
    id, ok := values[0].Int64()
    name, nameOK := values[1].Text()
    _, _, _, _ = id, ok, name, nameOK
}
if err := rows.Err(); err != nil { /* handle */ }

affected, err := client.Exec(ctx,
    "UPDATE users SET name = 'new' WHERE id = 1")
err = client.Analyze(ctx, netbadb.TableID(1))
```

Values are explicitly tagged; `NULL` is its own `ValueKind`. Result metadata
keeps semantic type, physical type, and nullability without exposing storage or
planner IDs. Rows stream one QueryRow frame at a time and validate shape, type,
nullability, and the final row count. `Rows.Close` drains an unfinished result
so the connection can be reused.

## Transactions and errors

```go
tx, err := client.Begin(ctx, netbadb.TableID(1))
_, err = tx.Exec(ctx, "INSERT INTO users (id, name) VALUES (2, 'temporary')")
rows, err := tx.Query(ctx, "SELECT id, name FROM users")
err = rows.Close()
err = tx.Rollback(ctx) // use Commit for a durable commit
```

While a transaction is active, query and DML operations must use `Tx`; Ping is
still allowed. Wire transaction state controls whether an error leaves the
transaction retryable or terminal. A `RemoteError` exposes a stable `ErrorCode`
and `TransactionState`; do not classify errors by message text. In Protocol v1,
authorization denial still uses `ErrorCodeDatabase`.

Protocol violations and network failures close the connection. The client does
not reconnect, retry, or replay SQL because an implicit DML or Commit may have
already become durable. Context cancellation also closes the connection; it
does not guarantee server-side statement cancellation. Call `Tx.Rollback` when
confirmed rollback matters, because `Client.Close` only disconnects and relies
on server cleanup.

There is no generated typed schema/query layer yet, `database/sql` driver,
connection pool, ORM/query builder, prepared statement, or parameter binding.

Run unit conformance tests with `go test ./...`. From the repository root,
`scripts/test-go-sdk.sh` additionally builds a temporary Rust fixture and runs
plaintext plus mutual-TLS cross-language integration tests.
