# Experimental PostgreSQL wire compatibility

Round 6 adds the first mutable DDL slice: transactional, durable `CREATE INDEX`
for one non-unique Heap BTree column. It is exercised by psql, SQLAlchemy
`Index.create()`, and a guarded Alembic add-index operation.
The feature remains experimental: it is a real path through NetbaDB's compiler, Canonical
Schema, index registry, and storage engine, not a claim of complete PostgreSQL
dialect, catalog, or migration compatibility.

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

## ORM and psql metadata architecture

Real SQLAlchemy PostgreSQL introspection contains catalog-only syntax such as
schema-qualified relations, arrays, `ANY`, `regclass`, and PostgreSQL catalog
functions. Expanding the native SQL grammar into a partial PostgreSQL system
query engine would couple unrelated layers and still be brittle. Rounds 3 through 5 use
a closed, typed compatibility operation boundary:

```text
SQLAlchemy / psycopg Extended Query     psql Simple Query
                 \                       /
    -> structural catalog-query classification
    -> CompatibilityStatement + typed Bind values
    -> per-session read-only compatibility snapshot
    <- Database::inspect_catalog()
    <- Canonical Schema + persistent Heap index registry
    -> authorized virtual rows
    -> ordinary pgwire RowDescription / DataRow encoding
```

Classification uses relation, selected capability, function, and predicate
markers after whitespace/case normalization. It does not compare a complete SQL
string, depend on whitespace, or return hard-coded table rows. Ordinary user
SQL never enters this evaluator. Before metadata evaluation, the snapshot is
refreshed when Core's catalog generation changes, so committed DDL is visible
to existing connections. It is derived from the Core inspection DTO and contains stable table/column identity, physical
types, nullability, primary keys, and real registered index definitions. The
index DTO exposes `(TableId, ColumnId)` logical identity, single-column order,
`BTree` kind, and `unique=false`; it excludes BTreeHandle, PageId, storage kind,
statistics policy, and mutable state. Catalog execution accepts no mutable
database or storage handle, so reflection cannot write a heap, WAL, index
registry, statistics, or checkpoint.

The current virtual surface is exactly the subset observed from SQLAlchemy
2.0.52: namespaces; user-table names and visibility; column definitions;
domain/enum probes (empty because NetbaDB exposes neither); table OID lookup;
primary keys; real non-primary Heap B+Tree indexes; and conservative empty
foreign-key, table-comment, and check-constraint results. LSM clustering is not
reported as a secondary index. Partition-local indexes are rejected for
logical reflection because the current registry cannot prove that every
partition implements one logical index.

The index operation implements only the captured SQLAlchemy result shape over
`pg_index`, index/table `pg_class` objects, table `pg_attribute`, `pg_am`, and
default operator-class flags. Current indexes are ordinary column B+Trees, so
the result needs neither expressions, predicates, INCLUDE columns, reloptions,
nor `pg_get_indexdef` evaluation. Catalog text/bool arrays are output-only
pgwire types and have no NetbaDB physical-type mapping.

Captured SQLAlchemy reflection uses typed Extended Query parameters. Captured
psql reflection uses a bounded tokenizer to lower literal Simple Query catalog
predicates into the same `CompatibilityStatement` evaluator; it does not
compare complete SQL strings or run the hidden queries through the native SQL
engine. Both paths use the same snapshot, result limit, authorization filter,
RowDescription/DataRow encoder, and read-only boundary. INSERT/UPDATE/DELETE
targeting `pg_catalog` returns `0A000`.

## psql describe profile

The supported psql 17.11 surface is deliberately narrow:

| Command | Status | Projection |
| --- | --- | --- |
| `\d users`, `\d public.users`, `\d user*` | supported | columns, PostgreSQL type names, nullability, Canonical primary key, real secondary indexes |
| `\d missing` | supported | normal no-relation result |
| `\dt`, `\dt user*`, `\dt public.*` | supported | authorized `public` tables |
| `\di`, `\di *users*` | supported | compatibility primary-key indexes and real registered secondary indexes |
| `\d+`, `\dt+`, `\di+`, other slash commands | unsupported | outside the Round 5 profile |

psql converts its glob-like input to anchored PostgreSQL regular expressions.
The server implements only the observed catalog subset: `^(...)$`, escaped
literals, `.` and `.*`. Compilation is limited to 1,024 input bytes and 1,024
atoms, matching uses dynamic programming without backtracking, and unsupported
regex constructs return `0A000`. Query tokens and nesting are independently
bounded. This is not a general PostgreSQL regex operator and no regex concept
enters HIR, relational IR, planning, execution, or storage.

NetbaDB has no persistent PostgreSQL role catalog. The `Owner` column in psql
relation lists is an explicit compatibility projection of the authenticated
session username; it does not assert persistent PostgreSQL role ownership.
Policies, publications, extended statistics, and inheritance/partition
relations are typed empty results because those features are absent. Table
access method is NULL rather than falsely reporting PostgreSQL `heap`.

User tables map to `public`; `current_schema()`, `SHOW search_path`, qualified
lookups, and the namespace projection all agree. Metadata visibility reuses the
existing principal's table grants. `UINT64` column reflection fails with
SQLSTATE `0A000` because no lossless PostgreSQL base integer mapping exists.

Synthetic table and index OIDs use the high range beginning at `0x80000000`,
separate from centralized PostgreSQL built-in type OIDs. Domain-separated
SHA-256 digests cover canonical table identity and logical index identity;
candidates share one collision set and use deterministic linear probing.
Compatibility index names use a sanitized readable table/column prefix plus a
12-hex digest suffix, remain at most 63 ASCII bytes, and are collision checked.
Explicit CREATE INDEX names are durable generic metadata and are reflected
verbatim; synthetic names remain only for legacy unnamed registry entries.

## Mutable index DDL profile

Supported syntax is `CREATE INDEX [IF NOT EXISTS] name ON [public.]table
(column)`. It creates one non-unique BTree on an existing non-partitioned Heap
table. Resolution produces frontend-neutral typed DDL with `IndexName`,
`TableId`, and `ColumnId`; the PostgreSQL adapter never manipulates pages,
BTree handles, or catalog bytes. Tree creation, existing-row backfill, named
IndexCatalog registration, and commit share one existing WAL transaction. The
backfill validates all persisted row fields while retaining keys from at most
one Heap page before inserting them into the tree.

Explicit `BEGIN`/`COMMIT` and `ROLLBACK` are supported. Planner and inspection
state is published only after durable commit. Later INSERT/UPDATE/DELETE uses
the ordinary registered-index maintenance path, and prepared queries plan
against the current access-path snapshot at execution.

Unique, composite, partial, expression, INCLUDE, non-BTree, and concurrent
forms return `0A000`. Quoted identifiers are not part of the current generic
SQL identifier grammar and fail closed rather than being partially parsed;
missing tables and columns retain `42P01` and `42703`.
LSM and partitioned-table secondary-index DDL is unsupported. `DROP INDEX`
remains `0A000`: the registry is append-only and BTree page reclamation has no
safe removal lifecycle, so hiding reflection would be a false drop.
The same Canonical Schema/index registry therefore produces the same names and
OIDs across queries, connections, and restarts. They are not persisted or
stable across schema/index changes and remain private to the adapter.

## Compatibility tracing

Set `NETBADB_POSTGRES_TRACE=1` when starting the listener to log startup
parameter names, Parse SQL and declared OIDs, Bind counts and formats,
Describe/Execute/Close lifecycle, compatibility classification, and failure
SQLSTATE. It is disabled by default. Startup values, password payloads, Bind
payloads, TLS secrets, and credentials are never logged; PasswordMessage is
shown only as `<redacted>`. Parse SQL can itself contain application literals,
so enable this diagnostic trace only in a controlled test environment.

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

| Message or behavior | Status | Notes |
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
| DEALLOCATE / DEALLOCATE ALL | complete | SQL-form psycopg prepared-cache cleanup |
| Binary format | selected types | bool, int2, int4, int8, text, varchar input; bool, int8, text output |
| ORM reflection | existing schemas | SQLAlchemy table/column/PK/index autoload through derived metadata |
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
multi-statement Simple Query batches. It also supports the read-only savepoint
subset used by psycopg capability probes: rollback-to is accepted only when no
mutation occurred since the savepoint; otherwise it returns `0A000` rather
than pretending to undo writes. `ReadyForQuery` reports:

- `I` when idle;
- `T` in an explicit transaction;
- `E` after an error in an explicit transaction.

In state `E`, commands return SQLSTATE `25P02` until `ROLLBACK`. This is a
PostgreSQL session rule layered over the existing NetbaDB transaction cleanup;
the core MVCC model is unchanged.

The adapter additionally owns only these explicit compatibility queries:

- `SHOW client_encoding`, `SHOW DateStyle`, `SHOW TimeZone`, transaction
  isolation, `standard_conforming_strings`, and `search_path`;
- `SELECT version()` and `SELECT pg_catalog.version()`; the parseable
  `PostgreSQL 16.0` prefix selects a dialect compatibility profile and does not
  claim that NetbaDB implements PostgreSQL 16;
- `SELECT current_database()`;
- `SELECT current_schema()`;
- `SELECT current_user` and `SELECT current_user()`.

Other unknown SQL is compiled normally and returns a mapped error. There is no
application-name branching and no fixed success response for arbitrary system
queries. FROM-less scalar SELECT is implemented generically in parser, HIR,
relational IR, planner, and executor through `OneRow` plus `ScalarProject`;
`SELECT 1` is not a PostgreSQL-session string special case.

Normal SQLAlchemy Core/ORM statements also use generic postfix casts
(`::BOOL`, `::BIGINT`/`::INT8`, and `::TEXT`/`::VARCHAR`) and qualified column
projection aliases. Casts are validated in typed HIR, preserve an expected
nominal semantic type such as `UserId`, and are erased only after successful
binding as lossless no-ops. PostgreSQL-only `regclass` and `regtype` casts are
not added to the user type system; they remain catalog-adapter details.

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

Rounds 3 and 4 implement only the read-only `pg_catalog` projection required by the
captured SQLAlchemy operations. It does not expose catalog relations as general
user-queryable tables and does not implement `information_schema`. No second
schema catalog is maintained.

Compatibility results actually exercised in this environment:

| Client | Connect | Prepared/binary | Transactions | Reflection/ORM |
| --- | --- | --- | --- | --- |
| Raw PostgreSQL v3 TCP integration client | yes | typed text/binary parameters and results | failed-state recovery | protocol lifecycle regression |
| psql 17.11 | yes | libpq query path | yes | scalar profile and ordinary table SELECT |
| psycopg 3.2.13 | yes | explicit `prepare=True`, repeated reuse, binary result cursor | commit, rollback, failed transaction recovery | SQLAlchemy transport |
| SQLAlchemy 2.0.52 + psycopg | yes | generated typed parameters | Core rollback and ORM transactions | Inspector, autoload, reflected SELECT, Core and ORM CRUD |
| Alembic 1.16.5 | yes | SQLAlchemy transport | read-only rollback | inspect and `compare_metadata`; no migration execution |
| pgx v5.7.6 | yes (Round 2) | prepared CRUD and repeated binds | yes | not tested |

The TCP integration suite performs SSL refusal, startup with known and unknown
parameters, CRUD, NULL/bool/int/text rows, multi-statement transactions,
failed-transaction recovery, named prepared statement and portal lifecycle,
Describe, Execute, Sync, Close, and CancelRequest framing against a real
listener and database worker.

The Python tests are `scripts/test-postgresql-orm.py` and
`scripts/test-postgresql-alembic.py`; their isolated dependencies are pinned in
`scripts/requirements-postgresql-orm.txt`. They test two existing tables, two
real secondary indexes, a zero-index table, missing-table lookup, repeated and
qualified reflection, reopen-stable names/OIDs, autoloaded Index objects,
explicit prepared reuse, selected binary formats, CRUD, transaction/error
rollback, NULL, Core-generated SQL, reflected SELECT, mapped ORM SELECT/CRUD,
and Alembic read-only comparison. Matching metadata has no diff; omitting the
indexes proposes two `remove_index` operations without executing them. See
`postgresql-client-matrix.md` for captured blockers and exact reproduction.

## Remaining work

- P0: merge simultaneous native/PostgreSQL listeners behind one worker through
  a versioned manifest, and add PostgreSQL TLS/authentication suitable for
  non-loopback deployment.
- P1: implement actual cancellation and broaden PostgreSQL dialect lowering
  only where real clients demonstrate a need.
- P2: add `information_schema` or broader catalog query lowering only after a
  captured client needs it. The optional `psql \\d users` probe currently
  reaches an unsupported `OPERATOR(pg_catalog.~)` regex predicate.
- P3: add only justified generic SQL such as RETURNING, UPSERT, CTEs, and
  broader joins without
  leaking dialect policy into core layers.
