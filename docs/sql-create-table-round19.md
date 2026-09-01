# Round 19: generic SQL CREATE TABLE

Base: `f658de49248311a7c550cb37a74630c393b7d712`.
Branch: `codex/sql-create-table-round19`. All edits are isolated in the requested
sibling worktree. No merge, push, cherry-pick or worktree deletion is authorized.

## Existing DDL pipeline audit (before implementation)

- Parser: `Statement::CreateIndex(CreateIndexStatement)` and
  `Statement::DropIndex(DropIndexStatement)` in `netbadb-parser/src/lib.rs`.
  Names carry byte `Span`s; CREATE INDEX accepts one column and IF NOT EXISTS;
  DROP accepts IF EXISTS. Qualified names are represented by `ColumnName`.
- HIR: `lower_create_index` resolves TableId/ColumnId to `TypedCreateIndex`;
  `lower_drop_index` uses `IndexNameBinding` to produce `TypedDropIndex` with an
  optional exact `DropIndexTarget`. Existing index HIR contains a `public`
  compatibility check; this precedent will not be extended to table HIR.
- Compiler: `compile_ddl_statement` parses and lowers to
  `CompiledDdlStatement::{CreateIndex,DropIndex}` separately from relational DML.
- Core: `prepare_ddl_statement` supplies active index-name bindings and owns a
  frontend-neutral `PreparedDdlStatement`. Its access describes write TableIds;
  there is no schema capability. Preparation performs no mutation.
  `execute_ddl`/`execute_ddl_in` use existing index lifecycle methods.
- Shared session: `DatabaseSession` owns the optional transaction and routes DDL
  to those Core methods. DML still compiles against the global schema in both
  native authorization and PG Parse/Simple Query; Round 18's
  `prepare_statement_in` exists but is not connected to sessions.
- PG Simple Query: normalized SQL currently recognizes index DDL, prepares,
  authorizes write TableIds and executes through `DatabaseSession`, emitting
  CREATE INDEX/DROP INDEX. Other table DDL is rejected before generic parsing.
- PG Extended Query: Parse stores `PreparedExecution::Ddl`; Bind copies typed
  execution with zero parameters; Describe emits zero ParameterDescription and
  NoData; Execute authorizes then calls shared session DDL; Sync restores protocol
  synchronization. This is the reusable execution boundary.
- Native Protocol v1: `WorkerSession` authorizes `statement_access` before calling
  `SessionState`; both currently only compile relational DML. No native DDL branch
  exists. The new path must preserve Protocol v1 and use AffectedRows(0).
- Errors: ParseError/HirError retain UTF-8 byte spans; CompileError::kind/span and
  DatabaseError::kind/source_position expose generic diagnostics. PG maps them
  centrally, but currently maps index syntax to 0A000 and all SchemaError to an
  operational error. Duplicate-column and unknown-type categories need precision.
- Authorization: strict manifest v4 has only per-TableId read/write/transaction/
  analyze grants. Old principals default-deny missing table permissions. No global
  admin exists. Add an optional, default-false principal `schema_admin`; old v4
  configurations remain readable. It authorizes schema writes, never durable DML.
- Bounds: generic tokenizer currently has no byte/token/identifier/recursion limits
  (PG codecs/metadata classifier have independent bounds). Add explicit generic
  limits and deterministic adversarial tests. Schema/Core permits zero columns;
  SQL P0 deliberately requires 1..4096 columns.

## Implementation contract

Generic SQL is parsed once, lowered through typed HIR and compiled DDL, and only
Core converts the logical declaration to Round 18 `CreateTableSpec`. Names,
semantic types, nullability and source spans contain no physical identities.
Persistent formats and Protocol v1 stay unchanged. A schema-write bit describes
new-table access without a sentinel TableId. Network authorization runs before
execution, and the creating transaction alone can read/write its staged identity.
Preparation sees transaction overlay; ordinary committed preparation cannot.
No permissions are persisted, published or inherited after commit/rollback.

P0 accepts unqualified, unquoted names and basic Heap columns. Qualified CREATE
TABLE is deferred; it must not add PostgreSQL namespaces to generic HIR/Core.

## Implemented SQL grammar and bounds

```sql
CREATE TABLE projects (
    id BIGINT NOT NULL,
    name TEXT,
    active BOOLEAN NOT NULL
);
```

Keywords/types are case-insensitive; identifiers retain the existing exact,
unquoted ASCII policy. Column order is declaration order. Default nullability is
true; NULL means true and NOT NULL means false. Repeated/conflicting nullability,
empty declarations, trailing commas/tokens, missing delimiters, empty/quoted names
and parameterized names/types fail closed. Core still permits zero-column direct
creation; SQL deliberately requires 1..4096 columns.

| Declaration aliases | Generic semantic type | PostgreSQL |
| --- | --- | --- |
| BOOLEAN / BOOL | unnamed physical Bool | supported |
| BIGINT / INT64 / INT8 | unnamed physical Int64 | supported |
| TEXT / VARCHAR | unnamed unbounded physical Text | supported |
| UINT64 | unnamed physical UInt64 | rejected before execution |

The previous generic cast grammar already supported BOOL, BIGINT/INT8 and
TEXT/unbounded VARCHAR. Declaration resolution additionally exposes INT64 and
Core's existing UInt64; no storage capability or nominal type is invented.
PostgreSQL type OIDs remain solely in the adapter. Cast grammar is unchanged.

Generic bounds are 1 MiB SQL, 32,768 tokens, 1,024-byte identifiers, 4,096 CREATE
columns, 64 recursive expression levels and a 256 expression-construction budget.
The latter also bounds left-nested cast/predicate chains. No new globally reserved
keywords were added. Source spans identify names, type names, nullability and
whole declarations. The deterministic parser test exercises 2,000 mutations and
adversarial bound inputs; no existing sql_parse fuzz target was present.

Unsupported constraints: inline/table-level PRIMARY KEY and UNIQUE, DEFAULT,
CHECK, REFERENCES, FOREIGN KEY, GENERATED, IDENTITY, COLLATE, CONSTRAINT, EXCLUDE.
Unsupported forms: TEMP/TEMPORARY/UNLOGGED, IF NOT EXISTS, CTAS, LIKE, INHERITS,
PARTITION BY, USING, WITH, TABLESPACE, ON COMMIT, qualified/quoted CREATE names,
VARCHAR(n)/CHAR(n), multiple CREATE TABLEs per transaction and table/index DDL
mixing (including CREATE INDEX on staged tables).
Known unsupported declarations include SMALLINT/INT2, INTEGER/INT/INT4,
NUMERIC/DECIMAL, REAL/FLOAT/DOUBLE, DATE/TIME/TIMESTAMP/TIMESTAMPTZ/INTERVAL,
JSON/JSONB, UUID, BYTEA, ARRAY, SERIAL/BIGSERIAL/SMALLSERIAL, CHAR/CHARACTER.
No clause is ignored or saved as unenforced constraint metadata.

## Typed compiler, Core and session integration

`CreateTableStatement`/`CreateColumn` contain only syntax and spans.
`TypedCreateTable`/`TypedCreateColumn` contain names, SemanticType, nullable and
source spans, never TableId/ColumnId/StorageId. `CompiledDdlStatement::CreateTable`
carries that logical declaration. `compile_sql_statement` chooses relational or
DDL lowering from one parsed AST; the enum keeps both IR families separate.
`PreparedDdlStatement` exposes schema-write access, completion kind and declaration
types for preflight. `CreateTableSpec::from(&TypedCreateTable)` is a pure conversion.
Only `create_heap_table_in` performs validation/reservation/staging/persistence.
No parser, compiler or server writes NBSC, journal records, Heap or WAL bytes.

`DatabaseSession` prepares SQL using `prepare_sql_statement_in` when active. This
combined entry point applies the same transaction SchemaView and table dependency
validation as Round 18 `prepare_statement_in`, without reparsing to decide DDL.
Only dependencies on staged tables receive transaction scope; committed-table
statements prepared within a transaction remain reusable after it ends. The
explicitly scoped direct `prepare_statement_in` API keeps its existing contract.
This distinction is exercised by the existing psycopg prepared-cache regression
and a new PG prepared-users/CREATE/COMMIT/rebind test. Global preparation cannot
see staged tables; per-table version/fingerprint validation does not invalidate
unrelated statements merely because SchemaGeneration advanced.

Both native and PG execution authorize the actual prepared object before Core
execution. PG compatibility preflights declaration types before authorization or
creation; UINT64 returns 0A000. Native SQL accepts UINT64. Native Protocol v1
returns its existing AffectedRows(0), with unchanged bytes and transaction controls.
PG Simple Query emits CREATE TABLE. Extended Parse and Bind do not change schema,
IDs, files or generations; Describe emits zero ParameterDescription and NoData.
Execute creates, Sync restores protocol framing; rebinding/executing the same
prepared declaration then returns duplicate table. Existing portal result caching
is unchanged; commit/rollback clears portals and scoped staged DML cannot escape.

## Authorization and error behavior

Manifest v4 adds optional `schema_admin` on each principal, default false. Existing
v4 manifests remain readable and deny DDL. This capability is required for all
network DDL; index DDL additionally retains its target-table write permission.
There is no global DML admin/owner role. A schema-only principal may explicitly
configure `tables: []`; PG BEGIN needs no fake table anchor. Native Begin(TableId)
retains its existing transaction grant requirement and Protocol v1 shape.

The creating transaction's exact staged TableId is temporarily readable/writable
by its schema-admin principal. This checks Core lifecycle ownership and never
alters the principal or a persistent grant. Commit/rollback ends access; a creator's
subsequent SELECT is denied without external grants. Observer metadata refresh is
still filtered, so hidden tables are not revealed by catalog queries.

| Failure | SQLSTATE | Before ID reservation? |
| --- | --- | --- |
| Duplicate committed table | 42P07 | yes, Core revalidation |
| Duplicate column | 42701 | yes, HIR |
| Unknown type | 42704 | yes, HIR; position points to type |
| Known unsupported feature/type | 0A000 | yes for declaration preflight |
| PG UINT64 | 0A000 | yes, adapter |
| Missing schema_admin | 42501 | yes, worker |
| Malformed syntax | 42601 | yes, parser |
| Schema writer busy | 55P03 | yes, Core admission |
| Statement after explicit transaction failure | 25P02 | no execution |

Failures in explicit transactions yield E until ROLLBACK, including unsupported
constraints. Session-owned implicit CREATE uses begin/create/commit and retains
handles until fallible commit or rollback succeeds. A pending durable commit is
retryable with COMMIT rather than converted into an abort-only state. Direct Core
implicit SQL cannot return a transaction handle; callers needing explicit retry
use execute_ddl_in/commit_transaction as before.

## Identity, transactions and recovery evidence

The Core SQL fixture starts with users=(1,1), teams=(2,2) and generation 1. A
CREATE/parameterized INSERT/SELECT/ROLLBACK consumes TableId=3 and StorageId=3,
but publishes nothing, leaves generation 1/runtime revision 0, and invalidates the
private prepared INSERT. Recreating and committing gets (4,4), columns (1,2,3),
TableSchemaVersion=1, generation 2/runtime revision 1, and `(10,'demo',true)`.
An UPDATE of users in that same transaction commits and its old prepared SELECT
still executes. Three catalog-only reopens preserve identities, schema, physical
binding, generation, version and rows.

A separate test creates the same declaration through SQL and direct Core APIs in
fresh equal-identity catalogs; full Schema, fingerprint and inspection match.
A staged owner-file collision injected after SQL preparation fails execution,
requires rollback and consumes IDs; preflight errors leave recursive file bytes,
next IDs and generation unchanged. This deliberately distinguishes pre-execution
rejection from a failed staged mutation after durable reservation.

Three SQL-driven child-process exits cover before coordinator decision (loser),
coordinator durable (winner), and partial NBSC publication (winner). Each is checked
through three catalog-only reopens. Losers retain consumed IDs; winners retain
rows. The unchanged Round 18 22-window crash matrix remains part of full regression.
These are process-crash tests, not machine/controller power-loss simulations.

No persistent or wire format changes: NBSC/NBSM/NBSL, NBSJ/NBSR/NBSA/NBST,
coordinator records, Heap, BTree/index/partition metadata, LSM, WAL and Protocol v1
remain unchanged. No lower lifecycle layer learns the requesting frontend.

## Real client evidence

The fresh SQL fixture has one baseline users=(1,1), with schema_admin and **no table
grants**. psql 17.11, psycopg 3.2.13 and SQLAlchemy 2.0.52 each roll back a table
using (2,2), then commit a new table at (3,3). Generation advances 1 -> 2 and table
version is 1. Each reads `(10,'demo',true)` inside its transaction. Post-commit
SELECT is denied; the original manifest remains byte-identical and does not name
the dynamic table. Normal server restart with that subset and catalog-only reopen
both succeed and the committed row persists.

psycopg's real default for zero-parameter CREATE is Simple Query. Parameterized DML
uses Extended Query. A separate normal `prepare=True` CREATE in the same fixture
provides trace-verified Parse/Bind/Execute coverage without changing
prepare_threshold or forcing Simple Query. SQLAlchemy Table.create(connection,
checkfirst=False) uses the ordinary PostgreSQL dialect, BigInteger/Text/Boolean,
no primary_key, defaults, sequences, or VARCHAR length. Its INSERT is parameterized
Extended Query. No MetaData.create_all or Alembic CreateTableOp is executed.

## Deferred and next round

DROP TABLE, ALTER TABLE, enforced constraints/PK/unique, runtime LSM/range placement,
multiple table creates per transaction, index-on-staged-table, general table ORM
migrations and qualified CREATE names remain deferred. No LSP/index-inspection DDL
framework expansion or security catalog is introduced. Next priority is Core
transactional DROP TABLE: exact TableId retirement, overlay removal and rollback,
prepared dependency invalidation, coordinator-backed logical publication, deferred
physical deletion/old handles, reopen, and same-name recreate using a new identity.


## Validation record

Validation used the pinned Rust 1.97.1 toolchain and isolated build/cache/corpus
paths under `/private/tmp`. The worktree contains no generated build output,
fixture databases, driver traces, fuzz artifacts or credentials.

```sh
cargo fmt --all -- --check
CARGO_TARGET_DIR=/private/tmp/netbadb-round19-target cargo check --workspace --all-targets --all-features --offline
CARGO_TARGET_DIR=/private/tmp/netbadb-round19-target cargo clippy --workspace --all-targets --all-features --offline -- -D warnings
CARGO_TARGET_DIR=/private/tmp/netbadb-round19-target cargo test --workspace --all-features --offline
```

All four commands passed. The final full-workspace run completed with zero
failures, including 86 Core unit tests, 376 storage unit tests, 45 server unit tests,
6 PG integration tests and 17 native TCP integration tests. The existing explicit
fuzz-corpus exporter remains ignored; no regression tests were disabled. Focused
parser/HIR/compiler tests passed, as did the seven Core SQL tests and six shared-
session/native/PG CREATE tests. The full suite includes the real TCP Protocol v1
SQL CREATE test, manifest default-deny checks for plaintext and certificate
principals, and the unchanged Round 18 crash matrix.
An initial restricted-sandbox socket test failed with `Operation not permitted`;
the approved local-socket rerun exercises those tests without bypassing auth.

Real client and compatibility commands, all passing:

```sh
CARGO_TARGET_DIR=/private/tmp/netbadb-round19-target /private/tmp/netbadb-round13-pg-venv/bin/python scripts/test-sql-create-table.py
NETBADB_PSQL_TARGET_DIR=/private/tmp/netbadb-round19-target python3 scripts/test-postgresql-psql.py
/private/tmp/netbadb-round13-pg-venv/bin/python scripts/test-postgresql-orm.py --dsn postgresql+psycopg://netbadb@127.0.0.1:PORT/netbadb
/private/tmp/netbadb-round13-pg-venv/bin/python scripts/test-postgresql-alembic.py --dsn postgresql+psycopg://netbadb@127.0.0.1:PORT/netbadb
CARGO_TARGET_DIR=/private/tmp/netbadb-round19-target cargo build --offline -p netbadb-server --example go_sdk_fixture
GOCACHE=/private/tmp/netbadb-round19-go-cache NETBADB_GO_FIXTURE_BIN=/private/tmp/netbadb-round19-target/debug/examples/go_sdk_fixture go -C sdk/go test -count=1 -tags=integration ./...
CARGO_TARGET_DIR=/private/tmp/netbadb-round19-target scripts/check-generated-sdk.sh
```

`PORT` above is each fresh `postgres_driver_fixture` process's printed port;
ORM and Alembic each used an independent database. Client versions were psql 17.11,
psycopg 3.2.13, SQLAlchemy 2.0.52 and Alembic 1.16.5. Alembic exercised only the
existing guarded index mutations, with zero differences before and after; no table
migration was attempted. Go passed both module packages with integration enabled;
SDK code generation used `--check`, with no generated file changes.

The real ORM regression initially found that indiscriminately scoping all
transaction-prepared statements invalidated psycopg's committed-table cache after
commit. The combined preparation API now scopes only private-table dependencies,
as described above; both the real regression and a deterministic PG test pass.
No client protocol or prepare-threshold workaround was introduced.

Rust 1.85 targeted checks used this command for each package:

```sh
CARGO_TARGET_DIR=/private/tmp/netbadb-round19-msrv cargo +1.85.0 check -p netbadb-PACKAGE --offline
```

`types`, `parser`, `hir`, `rel`, `compiler`, `schema` and `storage` passed. `core`
and `server` remain blocked by the pre-existing `netbadb-planner/src/lib.rs:892`
and `:904` let-chain E0658 errors. Those lines, MSRV and toolchain declarations
are unchanged, as requested. This is not a claim of full MSRV compatibility.

Every existing requested fuzz target completed 1,000 runs on the final source:

- `schema_catalog_decode`
- `schema_mutation_decode`
- `coordinator_log_decode`
- `btree_decode`
- `index_catalog_decode`
- `wal_recovery`
- `pgwire_decode`

```sh
CARGO_TARGET_DIR=/private/tmp/netbadb-round19-fuzz-target CARGO_NET_OFFLINE=true cargo +nightly fuzz run TARGET /private/tmp/netbadb-round19-fuzz/TARGET -- -runs=1000 -artifact_prefix=/private/tmp/netbadb-round19-fuzz/artifacts/
```

Corpora were copied to the temporary directory, with a valid CREATE TABLE Query
seed for pgwire. No crash artifacts were produced. Parser robustness additionally
uses the deterministic 2,000-mutation test and explicit byte/token/identifier/
column/recursion bounds, rather than inventing another fuzz infrastructure.

`git diff --check`, Python syntax compilation and local links in changed Markdown
were checked. Completion is commit-only on this branch: no merge, push,
cherry-pick, other-worktree edit or worktree removal.
