# Experimental PostgreSQL wire compatibility

Round 2 adds typed prepared parameters, selected binary formats, and generic
FROM-less scalar queries. The feature remains experimental: it is a real path
through NetbaDB's compiler and storage engine, not a claim of complete
PostgreSQL dialect or catalog compatibility.

NetbaDB has an experimental PostgreSQL v3 frontend foundation. It is intended
to let PostgreSQL clients enter the existing typed compiler and database core;
it is not a second SQL engine, catalog, transaction manager, or storage format.

## Architecture and audit result

The native parser AST and typed HIR are distinct stages, but the current HIR
lowering API directly consumes native parser AST types. That coupling is
localized at the parser-to-HIR boundary rather than spread into relational IR,
planning, or execution, but it is not an entry point for a PostgreSQL AST.
Phase 1 therefore does not introduce a speculative second parser or make HIR
accept PostgreSQL nodes. Existing NetbaDB SQL reaches the compiler unchanged,
while PostgreSQL protocol/session semantics stay above a dialect-neutral
compiler boundary. When real PostgreSQL dialect lowering is added, its AST
must enter through a generic semantic lowering input or HIR builder at this
same boundary; PostgreSQL nodes must not pass through it.

```text
PostgreSQL client
    -> netbadb-pgwire codec
    -> PostgreSQL session adapter
    -> Parse once: NetbaDB parser -> typed HIR parameter inference
    -> frontend-neutral prepared logical statement + parameter metadata
    -> Bind once: typed ScalarValue parameters (never SQL text)
    -> planner -> executor -> transaction/storage
```

`netbadb-pgwire` owns only untrusted-byte decoding, typed frontend/backend
messages, OIDs, format codes, and scalar text/binary encoding. It depends on shared
types and cannot call the database. `netbadb-server` owns sockets, startup
policy, prepared statements, portals, compatibility queries, SQLSTATE mapping,
authorization, and PostgreSQL transaction-aborted behavior.

Native Protocol v1 and PostgreSQL share the server's private synchronous
`DatabaseSession`: optional transaction ownership, execute/commit/rollback,
disconnect rollback, and result-row policy. They also share the dedicated
worker ownership rule: the worker constructs and exclusively owns `Database`,
all sessions, and all transaction handles. PostgreSQL OIDs, format codes,
prepared statement names, portal names, and `I`/`T`/`E` state never enter the
compiler, planner, executor, storage, WAL, or recovery layers.

The compiler exposes stable frontend-neutral compile categories, typed
`ParameterId` expressions, parameter metadata, typed logical binding, and a
no-execution statement-description API. That is a general NetbaDB capability:
frontends can map syntax, undefined-name, ambiguity, datatype, NOT NULL, and
unsupported-feature diagnostics and describe typed output without parsing a
Rust debug string or reading rows.

## Running

Use an existing manifest v4 configured for a loopback plaintext address:

```bash
cargo run -p netbadbd -- \
  --manifest /absolute/path/server.json \
  --postgres
```

The `listen` address becomes the PostgreSQL endpoint. For example, when it is
`127.0.0.1:5432`:

```bash
psql -h 127.0.0.1 -p 5432 -U netbadb -d test
```

Phase 1 uses an exclusive listener mode: omit `--postgres` for native Protocol
v1. It does not silently change manifest v4 or open a second port. Only
loopback plaintext manifests are accepted. PostgreSQL `SSLRequest` receives
the protocol-defined `N`; NetbaDB's existing native mTLS configuration is not
misrepresented as PostgreSQL TLS.

## Startup and authentication

The listener supports bounded decoding for:

- protocol v3 `StartupMessage`;
- `SSLRequest` with a cleartext `N` response;
- `CancelRequest` framing. Phase 1 closes the cancel connection but does not
  cancel an executing worker request;
- common and unknown startup key/value parameters. `user` and `database` feed
  compatibility functions; unknown parameters are retained by the typed
  decoder and otherwise ignored safely.

Successful loopback admission maps to manifest v4's `local_plaintext`
principal and returns `AuthenticationOk`. This is a local development
authentication mode, not PostgreSQL password authentication. Authorization is
still checked after compiler name/type resolution and before execution using
the same per-TableId read/write/transaction grants as Protocol v1.

## Protocol support

| Message or behavior | Phase 1 status | Notes |
| --- | --- | --- |
| StartupMessage | complete | bounded UTF-8 parameter map |
| SSLRequest | complete | returns `N`; TLS unsupported |
| CancelRequest | partial | parsed and closed; no execution cancellation |
| AuthenticationOk / ParameterStatus / BackendKeyData | complete | local manifest principal |
| Query | complete | current NetbaDB SQL subset; quoted-semicolon-aware batches |
| RowDescription / DataRow / CommandComplete | complete | typed text output |
| EmptyQueryResponse / ErrorResponse / ReadyForQuery | complete | stable SQLSTATE and `I`/`T`/`E` |
| Parse | complete for the NetbaDB SQL subset | `$n`, supplied/zero/omitted OIDs, contextual inference, Parse-time preparation |
| Bind | complete for supported scalar types | exact parameter count; text/binary decoding; `0`/`1`/`N` format cardinality |
| ParameterDescription | complete | reports inferred or declared PostgreSQL OIDs |
| Describe / Execute / Sync | complete for supported statements | named/unnamed lifecycle, repeated binding, portal suspension |
| Close / Flush / Terminate | complete | statement and portal cleanup |
| Binary format | selected types | bool, int2, int4, int8, text, varchar input; bool, int8, text output |
| PasswordMessage | unsupported | unexpected message is a protocol error |

Extended-protocol errors suppress subsequent frontend messages until `Sync`,
then return `ReadyForQuery`. Named duplicate statements and portals, missing
objects, parameter/format count mismatches, and per-session object limits are
deterministic errors. Unnamed Parse and Bind replace the preceding unnamed
object. Closing a statement also closes its portals; closing an unknown object
still returns `CloseComplete`, as PostgreSQL specifies. Parameter values are
decoded once into `ScalarValue` and bound into cloned typed logical expressions;
they are never interpolated into or reparsed as SQL text.

## SQL and transactions

All ordinary statements use the real NetbaDB parser, typed HIR, planner,
executor, authorization, and transaction/storage path. The supported SQL is
therefore exactly the implemented NetbaDB subset documented in the README:
SELECT (including scalar `SELECT 1`, `SELECT true`, `SELECT 'x'`, and
`SELECT NULL` without `FROM`), INSERT, UPDATE, DELETE, filters, joins, ordering,
aggregation, grouping, and limits subject to their existing constraints.

The PostgreSQL adapter recognizes `BEGIN`, `COMMIT`, and `ROLLBACK`, including
multi-statement Simple Query batches. `ReadyForQuery` reports:

- `I` when idle;
- `T` in an explicit transaction;
- `E` after an error in an explicit transaction.

In state `E`, commands return SQLSTATE `25P02` until `ROLLBACK`. This is a
PostgreSQL session rule layered over the existing NetbaDB transaction cleanup;
the core MVCC model is unchanged.

The adapter additionally owns only these explicit compatibility queries:

- `SHOW client_encoding`, `SHOW DateStyle`, `SHOW TimeZone`;
- `SELECT version()`;
- `SELECT current_database()`;
- `SELECT current_schema()`;
- `SELECT current_user` and `SELECT current_user()`.

Other unknown SQL is compiled normally and returns a mapped error. There is no
application-name branching and no fixed success response for arbitrary system
queries. FROM-less scalar SELECT is implemented generically in parser, HIR,
relational IR, planner, and executor through `OneRow` plus `ScalarProject`;
`SELECT 1` is not a PostgreSQL-session string special case.

## Types and formats

| NetbaDB physical type | PostgreSQL type | OID | Text | Binary |
| --- | --- | ---: | --- | --- |
| BOOL | bool | 16 | yes (`t` / `f`) | yes |
| INT64 | int8 | 20 | yes | yes, big-endian 8 bytes |
| TEXT | text | 25 | yes, UTF-8 | yes, UTF-8 bytes |
| UINT64 | none | — | rejected | no |
| NULL value | field's declared type | field OID | length `-1` | length `-1` |

PostgreSQL input type definitions also recognize int2 (21), int4 (23), int8
(20), text (25), varchar (1043), bool (16), and unknown (705), with checked
text and binary decoders. int2/int4 inputs widen losslessly to NetbaDB INT64;
their binary widths and all integer ranges are checked exactly.

Parse OID `0`, an omitted trailing declaration, or an empty declaration array
means unspecified. Context such as `id = $1` or an INSERT target column infers
the frontend-neutral semantic type, including nominal identity. Repeated uses
must agree. A parameter with no declaration or usable context returns `42P18`.

UINT64 is deliberately unsupported because PostgreSQL has no lossless unsigned
64-bit integer type. It is not silently narrowed to int8 or mislabeled as text.
Nominal semantic types remain attached to NetbaDB HIR and are never weakened;
the PostgreSQL client sees only their supported physical representation.

RowDescription uses the real output name and type. Source table OID and column
attribute number are `0`, and type modifier is `-1`, which are the allowed
unknown/default values; the adapter does not fabricate PostgreSQL catalog IDs.

## Errors and limits

The adapter maps stable core diagnostic categories to SQLSTATE, including:

- `42601` syntax error;
- `42P01` undefined table;
- `42703` undefined column;
- `42702` ambiguous column;
- `42804` datatype mismatch;
- `42P18` indeterminate parameter datatype;
- `08P01` malformed protocol counts, framing, and format cardinality;
- `22P02` invalid text representation;
- `22P03` invalid binary representation or exact-width violation;
- `22003` numeric value out of range;
- `23502` NOT NULL violation;
- `25000` / `25P02` transaction-state errors;
- `0A000` unsupported features;
- `42501` authorization denial.

Compiler messages and one-based source positions are safe to expose. Internal
and operational failures use bounded generic messages rather than Rust debug
output, filesystem paths, enum dumps, or stack traces.

Startup packets, messages, strings, fields, parameters, prepared statements,
portals, row results, and encoded output messages all have explicit limits.
Negative, truncated, oversized, invalid UTF-8, invalid count, and invalid
format inputs return typed errors without panic or unbounded allocation.

## Catalog scope and client compatibility

`pg_catalog` and `information_schema` virtual relations are not implemented in
phase 1. No second schema catalog is maintained. Future catalog adapters must
derive rows from Canonical Schema IR and stable catalog inspection APIs.

Compatibility results actually exercised in this environment:

| Client | Connect | Simple query | Extended query | Transaction |
| --- | --- | --- | --- | --- |
| Raw PostgreSQL v3 TCP integration client | yes | yes | yes, typed text/binary parameters and results | yes, including failed state |
| psql 17.11 | yes | yes, including FROM-less SELECT | through libpq query path | yes |
| Rust `postgres` / `tokio-postgres` | not tested (dependency unavailable offline) | — | — | — |
| pgx v5.7.6 | yes | yes | yes, prepared CRUD and repeated binds | yes |
| JDBC | not tested | — | — | — |

The TCP integration suite performs SSL refusal, startup with known and unknown
parameters, CRUD, NULL/bool/int/text rows, multi-statement transactions,
failed-transaction recovery, named prepared statement and portal lifecycle,
Describe, Execute, Sync, Close, and CancelRequest framing against a real
listener and database worker.

The installed `/opt/local/lib/pgsql/bin/psql` and pgx v5.7.6 were run against a
real listener. psql completed FROM-less scalar queries and a transaction. pgx
used its normal Extended Query path for parameterized INSERT/SELECT/UPDATE/
DELETE, explicit preparation, repeated binds with different values, NULL, and
transactional execution.

## Remaining work

- P0: merge simultaneous native/PostgreSQL listeners behind one worker through
  a versioned manifest, and add PostgreSQL TLS/authentication suitable for
  non-loopback deployment.
- P1: implement actual cancellation and broaden PostgreSQL dialect lowering
  only where real clients demonstrate a need.
- P2: derive minimal `pg_namespace`, `pg_class`, `pg_attribute`, `pg_type`, and
  information-schema views from Canonical Schema and validate real psql/driver
  introspection.
- P3: add only justified PostgreSQL dialect lowering such as projection
  expressions, casts, RETURNING, UPSERT, CTEs, and broader joins without
  leaking dialect policy into core layers.
