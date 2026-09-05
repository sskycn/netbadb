# Experimental PostgreSQL wire compatibility

Round 7 supports transactional, durable `CREATE INDEX` and `DROP INDEX` for
single-column non-unique Heap BTree indexes. It is exercised by psql, SQLAlchemy
`Index.create()` / `Index.drop()`, and guarded Alembic CreateIndexOp/DropIndexOp apply.
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
LSM and partitioned-table secondary-index DDL remains unsupported.
`DROP INDEX [IF EXISTS] [public.]name` resolves a durable named index or, in
this adapter only, an authorized legacy synthetic alias to `(TableId, IndexId)`.
It writes a v4 retired catalog state in the existing transaction and publishes
registry removal after commit. Missing indexes return `42704`; IF EXISTS is a
successful no-op. Explicit `schema_admin` and table write permission are required
(`42501`); existing principals default-deny schema mutations. CONCURRENTLY,
multiple targets, CASCADE, RESTRICT, parameters as identifiers, and quoted
identifiers fail closed (`0A000`). Basic table creation is described below.

Uncommitted DROP keeps the published planner/reflection view active, like
uncommitted CREATE keeps its registration unpublished. This is an experimental
compatibility boundary, not PostgreSQL catalog MVCC. CREATE and DROP cannot be
combined within one explicit transaction (`0A000`); no transaction-local index
registry overlay is implemented. DML after staged DROP still maintains the old tree until
commit. Any DDL error fails the transaction (`E` / `25P02`) until rollback.
Prepared DROP is bound to the resolved ID and never drops a replacement index
that reused the same name. Prepared queries replan using current active paths.

Round 39 adds one bounded DDL-after-DML exception for a managed Single Heap:
exact DROP-only index preparation, same-table transaction-visible DML, a
layout-compatible SET/DROP NOT NULL or rename ALTER, and final logical index
changes. The route is selected by Core only when the ALTER executes; no SQL
sequence is buffered or recognized by this adapter. The first successful ALTER
closes all relation execution (`25000`). SET NOT NULL scans current S1 changes
and returns `23502` for a remaining NULL. COMMIT constructs one final S2, while
rollback restores S1. See
[Round 39](drop-first-migration-sql-round39.md) for exact limits.

Round 42 extends only that same exact Core-selected route to nullable ADD COLUMN
and exact-ID DROP COLUMN. Parse/Bind/Describe stay allocation-free; ADD reserves
its new ID only at Execute. Same-name DROP+ADD produces a new NULL-valued column,
and the first successful refinement still closes DML. New-column indexes and
public new-column NOT NULL remain `0A000`; no-authority post-DML ADD/DROP and
post-refinement DML are `25000`, followed by `25P02` in a failed transaction.
The adapter still has no migration-state logic. See
[Round 42](drop-first-layout-migration-sql-round42.md).

Round 44 adds a separate Core-selected path for ordinary one-S1 DML followed by
nullable ADD COLUMN, unindexed non-PK DROP COLUMN, RENAME TABLE, or RENAME
COLUMN. INSERT/UPDATE/DELETE, mixed DML, zero-row writes, and a prior read of
the same S1 qualify. Read-only, cross-table, pending-index, and index DDL do not.
Parse/Bind/Describe remain pure; Execute performs the
late adoption and any tag-16 reservation. The first accepted refinement closes
relational execution with `25000`, and the next command in the failed explicit
transaction receives `25P02`. Indexed DROP remains `2BP01`. The adapter has no
Round 44 branch; see [Round 44](post-dml-source-adoption-round44.md).

Round 46 extends only this Core route with SET/DROP NOT NULL on a surviving base
ColumnId, including an indexed survivor. SET scans transaction-visible S1 before
writer acquisition; a remaining NULL is `23502`, followed by `25P02` under the
normal PostgreSQL failed-transaction rule. DROP performs no scan. Successful
refinement still closes later relational execution with `25000`. A late-added
nullable column retains the established boundaries: SET is `25000`, while an
already-nullable DROP is rejected as an operational `58000`; neither enters the
adopted nullability path. Simple and Extended Query require no adapter-specific
state. See [Round 46](post-dml-source-nullability-round46.md).

[Round 48](adopted-source-final-index-round48.md) accepts final single-column
non-unique CREATE/DROP INDEX only after this adopted-source refinement has
already begun. The first accepted statement, even unchanged IF NOT EXISTS,
closes all later ALTER with `25000`. Relational access also remains `25000`,
followed by `25P02` in a failed explicit transaction. Further same-table index
DDL can compose. Prepared CREATE validates its original overlay dependency
before reservation; prepared DROP retains exact T/I and cannot delete a new
same-name index (`42704` for the absent old identity). Parse/Bind/Describe remain
pure. Simple and Extended Query retain ordinary authorization and command tags;
no PostgreSQL production adapter changes or new wire state are involved.

[Round 50](deferred-new-column-backfill-round50.md) adds one Core-selected
exception before that terminal phase. UPDATE may target only durably reserved
late columns of the same adopted table and may read only surviving base columns
or Execute-bound deterministic scalars. Simple Query reports exact `UPDATE n`.
Extended Parse/Bind/Describe remain pure and Execute owns bound values before
recording the action. A projected late-column SET NOT NULL succeeds only after
deferred backfill covers every row; a remaining NULL is `23502`, followed by
`25P02` in an explicit failed transaction. Base/mixed/late-reading UPDATE and
SELECT/INSERT/DELETE remain `25000`. The adapter still has no migration-state
branch.


Retirement clears only that index's statistics. ANALYZE and vacuum ignore retired
definitions. Inspection JSON is unchanged and active-only. Existing connections
refresh through committed catalog_generation; neither psql nor SQLAlchemy needs
to reconnect (a cached SQLAlchemy Inspector must be refreshed through its normal
API, or recreated over the same connection). Retired tree definitions are retained
for storage ownership inspection. Physical pages are not reclaimed or reused;
repeated create/drop grows the file. Reclamation/compaction is a separate storage
phase; SQL DROP does not promise file shrinkage.
The same Canonical Schema/index registry therefore produces the same names and
OIDs across queries, connections, and restarts. They are not persisted or
stable across schema/index changes and remain private to the adapter.

## Basic transactional CREATE TABLE (Round 19)

Partial support: basic Heap columns with BOOLEAN/BOOL, BIGINT/INT64/INT8, and
TEXT/unbounded VARCHAR; NULL is the default, explicit NULL and NOT NULL work.
Unquoted, unqualified table/column names follow the generic identifier policy.
1..4096 columns retain declaration order and unnamed physical SemanticTypes.
Native SQL additionally accepts UINT64; this frontend rejects it before writer
admission, ID reservation, journal writes or physical creation.

Simple Query emits exactly `CREATE TABLE`. Extended Parse/Bind/Describe/Execute/
Sync uses the same generic AST/HIR/compiled DDL: zero parameters and NoData;
Parse/Bind have no schema effects; each newly bound execution checks duplicates
against current Core authority. `BEGIN; CREATE; INSERT; SELECT; ROLLBACK/COMMIT`
is transactional. Statements in the creating session compile against the private
SchemaOverlay, while other sessions cannot resolve the table until commit.
Commit/rollback clears portals; transaction-scoped prepared DML cannot escape.

All network DDL requires explicit manifest `schema_admin` (default false).
A creating transaction alone gets temporary staged-table read/write access.
Commit never adds durable table grants, even for its creator; catalog rows still
use the ordinary TableId visibility filter. Metadata refresh after commit does
not imply permission. Schema-only admins may BEGIN without an existing table grant.

Duplicate tables map to 42P07, duplicate columns to 42701, unknown type names to
42704, permission denial to 42501, known unsupported types/features to 0A000,
and schema-writer contention to 55P03. Syntax errors map to 42601. Parser/HIR
errors retain source positions. An explicit transaction's CREATE failure yields
E and subsequent statements return 25P02 until rollback.

Unsupported: PRIMARY KEY (inline/table-level), UNIQUE, DEFAULT, CHECK, REFERENCES,
FOREIGN KEY, GENERATED, IDENTITY, COLLATE, CONSTRAINT, EXCLUDE; VARCHAR(n)/CHAR(n);
SMALLINT/INTEGER/NUMERIC/DECIMAL/FLOAT/DOUBLE/DATE/TIME/TIMESTAMP/INTERVAL/JSON/UUID/
BYTEA/ARRAY/SERIAL/BIGSERIAL and other non-native types; TEMP/TEMPORARY/UNLOGGED,
IF NOT EXISTS, CTAS, LIKE, INHERITS, PARTITION BY, USING, WITH, TABLESPACE, ON COMMIT;
qualified/quoted CREATE names; extended ALTER TABLE grammar; runtime LSM/range placement;
multiple CREATE TABLEs per transaction and table/index DDL mixing. Unsupported
clauses are rejected, never silently discarded or stored as unenforced metadata.

See [Round 19](sql-create-table-round19.md) for native/PG, real-client, recovery and
no-side-effect evidence. This is not full PostgreSQL DDL or ORM migrations.

## Basic transactional DROP TABLE (Round 21)

The supported form is one unquoted, unqualified `DROP TABLE name` with an optional
semicolon. Simple Query emits exactly `DROP TABLE`. Extended Parse/Bind/Describe/
Execute/Sync uses zero parameters and `NoData`; only Execute mutates state. HIR binds
the exact TableId, schema version, and fingerprint from the transaction view, so a
prepared statement never resolves a same-name replacement at Execute.

Autocommit and explicit transaction commit use the Round 20 logical-retirement
lifecycle. Rollback preserves the active table, rows, indexes, generations, and
allocator floors. Physical resources remain retained. A missing or stale target is
`42P01`; permission denial is `42501`; an explicit transaction error leads to
`25P02` until rollback. `schema_admin` is required, but a table DML grant is not.

Unsupported with `0A000` or the generic identifier syntax policy: IF EXISTS,
CASCADE/RESTRICT, multiple names, qualified/quoted names, ONLY, other DROP object
types, LSM/range tables, multiple schema mutations, and table/index DDL mixing.
SQLAlchemy `Table.drop(checkfirst=False)` is covered; checkfirst/drop_all and Alembic
table migration remain outside acceptance. See [Round 21](sql-drop-table-round21.md).

## Basic transactional ALTER TABLE (Round 26)

Six generic forms are supported for runtime-created Single Heaps: rename table,
rename column (`COLUMN` optional), add one nullable basic-type column, restricted
drop column, and set/drop NOT NULL. HIR binds exact TableId/schema version/
fingerprint and existing-column actions bind ColumnId. Parse/Bind/Describe are
side-effect-free; Execute calls the existing Round 24 rewrite and returns
`ALTER TABLE`.

Every committed operation keeps TableId and surviving ColumnIds/IndexIds, advances
the table version and SchemaGeneration once, and installs a new StorageId. Current
indexes are validated and rebuilt at Execute. Prepared ALTER fails after a table
rewrite or DROP+same-name CREATE but survives DML/index-only revisions. ALTER must
precede every executed user data access in its transaction.

Real psql 17.11, psycopg 3.2.13 `prepare=True`, SQLAlchemy 2.0.52 transport/
reflection and native `Index.create/drop`, and Alembic 1.16.5 Operations pass.
ALTER plus CREATE/DROP INDEX composes within one transaction for runtime-created
Single Heaps. Round 30 also composes managed runtime CREATE/DROP TABLE with ALTER
and index DDL, including CREATE/DROP physical elision, ALTER/DROP rewrite elision,
and same-name recreation as a new TableId. DML materializes and seals the
transaction, so later DDL remains unsupported. Defaults, constraints, ADD NOT NULL, physical type
conversion, dependent-column DROP, qualified/quoted/IF EXISTS/CASCADE forms,
imported/bootstrap, LSM or partitioned composition, online ALTER, and automatic GC are
unsupported. See [Round 30](table-ddl-composition-round30.md),
[Round 29](schema-index-composition-round29.md), and
[Round 26](sql-alter-table-round26.md).

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
| Alembic 1.16.5 | yes | SQLAlchemy transport | mixed schema/index commit/rollback | add-column/create-index and drop-index/drop-column Operations, plus guarded index-only regression |
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
Alembic guarded index-only apply, and Round 29 mixed schema/index Operations.
Matching metadata has no diff; named and
legacy synthetic index removal executes DropIndexOp and leaves an empty comparison. See
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
