# NetbaDB Rust SDK

`netbadb-sdk` is the application-facing façade for both embedded and remote
Rust applications. It always exposes canonical schema and value types.

## Embedded (default)

The default `embedded` feature preserves the existing API without requiring an
explicit feature selection:

```toml
[dependencies]
netbadb-sdk = { path = "path/to/netbadb/sdk/rust" }
```

```rust
use netbadb_sdk::{Database, ScalarValue, TableDef};
```

`embedded` enables `netbadb-core`. Existing `Database`, `Transaction`,
`ExecutionResult`, and schema/type imports remain at the crate root.

## Synchronous remote client

A remote-only application can avoid compiling the embedded core and storage
stack:

```toml
[dependencies]
netbadb-sdk = {
    path = "path/to/netbadb/sdk/rust",
    default-features = false,
    features = ["remote"]
}
```

```rust,no_run
use netbadb_sdk::{TableDef, remote};

# fn connect(users_table: &TableDef) -> Result<(), Box<dyn std::error::Error>> {
let expected = remote::TableIdentity::from_table(users_table)?;
let mut client = remote::Client::connect(
    remote::Config::new("127.0.0.1:7878")
        .require_schema(expected),
)?;

client.ping()?;
let mut rows = client.query("SELECT id, name FROM users ORDER BY id")?;
while let Some(values) = rows.next_row()? {
    println!("{values:?}");
}
# Ok(())
# }
```

Plaintext is accepted only when the resolved TCP peer is loopback. Remote
deployments use mandatory verified mutual TLS with a trust root, client
certificate chain, exactly one private key, and a validated server name:

```rust,no_run
use netbadb_sdk::remote;

# fn connect() -> Result<(), Box<dyn std::error::Error>> {
let tls = remote::TlsConfig::from_pem_files(
    "localhost",
    "ca.pem",
    "client.pem",
    "client-key.pem",
)?;
let mut client = remote::Client::connect(
    remote::Config::new("localhost:7878").tls(tls),
)?;
client.ping()?;
# Ok(())
# }
```

There is no anonymous TLS, certificate-verification bypass, automatic retry,
reconnect, pooling, pipelining, or multiplexing.

## Rows and transactions

`Rows<'_>` exclusively borrows its client and validates every row against
QueryStart metadata. Reading through QueryEnd releases the client naturally.
When stopping early, call `rows.close()` to drain the remaining response and
preserve connection reuse. Dropping unfinished Rows closes the connection;
fallible network draining is never hidden in Drop.

`Transaction<'_>` likewise exclusively borrows its client. Use explicit
`commit()` or `rollback()` when confirmation matters. Dropping an active
transaction closes the connection and relies on the server's disconnect
rollback lifecycle; Drop does not send a hidden rollback request. `Client`
close or Drop is therefore not proof that rollback completed.

Transport failure after Execute, Commit, or Rollback may occur after the server
has durably acted but before the response arrives. The outcome is ambiguous and
the client deliberately does not retry or replay the request. Protocol v1 also
reports authorization denial with the generic `ProtocolErrorCode::Database`;
clients must not reclassify it by matching message text.

The Rust remote client reuses the authoritative `netbadb-protocol` crate. The
Go SDK intentionally uses an independent codec to validate the language-neutral
wire contract.
