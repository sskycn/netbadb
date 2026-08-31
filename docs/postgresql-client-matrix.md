# PostgreSQL client compatibility matrix

This matrix records Round 7 observations against a real loopback TCP listener.
It is evidence for the experimental existing-schema profile, not a claim of
drop-in PostgreSQL compatibility.

## Verified environment

| Component | Version |
| --- | --- |
| Python | 3.9.6 |
| psycopg | 3.2.13 |
| psycopg-binary | 3.2.13 |
| SQLAlchemy | 2.0.52 |
| Alembic | 1.16.5 |
| PostgreSQL dialect | `postgresql+psycopg` |
| psql | 17.11 from `/opt/local/lib/pgsql/bin/psql` |

Python dependencies are compatibility-test tooling only and are not NetbaDB
production or build dependencies.

## Result matrix

| Client operation | Result | Notes |
| --- | --- | --- |
| psycopg connect / `SELECT 1` | yes | normal client defaults |
| psycopg parameterized CRUD | yes | INSERT, SELECT, UPDATE, DELETE, NULL |
| psycopg explicit preparation | yes | `prepare=True`, repeated execution |
| psycopg binary cursor | yes | selected supported scalar result formats |
| psycopg transactions | yes | commit, rollback, error then rollback |
| SQLAlchemy engine / `select(1)` | yes | unmodified PostgreSQL dialect |
| SQLAlchemy Core existing table | yes | normal `select/insert/update/delete` APIs |
| Inspector schemas / tables | yes | `public`; deterministic authorized user tables |
| Inspector `has_table` | yes | existing/missing and `schema="public"` |
| Inspector columns | yes | order, BOOL/BIGINT/TEXT, nullability |
| Inspector primary key | yes | real Canonical Schema primary key |
| Inspector non-primary indexes | yes, real | two non-unique single-column Heap B+Trees; zero-index table remains empty |
| `Table(..., autoload_with=engine)` | yes | repeated indexes stable across `users` / `teams` and connections |
| Reflected parameterized SELECT | yes | returns real stored rows |
| SQLAlchemy ORM mapped SELECT | yes | reflected imperative mapping |
| SQLAlchemy ORM CRUD | yes | explicit primary keys; no schema creation |
| SQLAlchemy `Index.create()` / `Index.drop()` | yes | real named create/drop/recreate; existing observer connection refresh |
| Alembic add_index / remove_index apply | yes | guarded CreateIndexOp/DropIndexOp, named plus two legacy synthetic removals; final compare_metadata is empty |
| psql `\d` | yes | columns, types, nullability, PK and real secondary indexes; qualified/missing/wildcard variants |
| psql `\dt` | yes | authorized `public` tables and name/schema patterns |
| psql `\di` | yes | compatibility PK and real secondary indexes, plus name patterns |
| psql `CREATE INDEX` | yes | transactional commit/rollback; `\d` and `\di` refresh immediately |
| psql `DROP INDEX` / `IF EXISTS` | yes | implicit and explicit commit/rollback; `\di` / `\d` removal |
| SQLAlchemy ↔ existing psql connection | yes | both directions see committed removal without reconnect |
| DROP legacy synthetic name | yes | adapter resolution to generic identity; durable retirement |
| Basic Heap CREATE TABLE | partial | BIGINT/TEXT/BOOLEAN, NULL/NOT NULL; transactional native + PG |
| SQLAlchemy `Table.create(checkfirst=False)` | yes, bounded fixture | no PK, defaults, sequences or VARCHAR length; same-transaction insert/select |
| DROP/ALTER TABLE, table migrations | unsupported | explicit `0A000`; no Alembic CreateTableOp |

No run used `prepare_threshold=None`, a simple-protocol override, a custom
dialect, `implicit_returning=False`, `use_insertmanyvalues=False`, `create_all`,
or another compatibility-disable flag.

## Observed blockers and implemented capabilities

| Observed client action / SQL class | Initial failure | Implemented capability |
| --- | --- | --- |
| `select pg_catalog.version()` | native parser rejected qualified function | explicit scalar compatibility command with a stable parseable profile string |
| `select current_schema()` and `show standard_conforming_strings` | missing startup commands | consistent `public` namespace and truthful setting |
| psycopg hstore probe: savepoint plus `pg_type` / `to_regtype($1)` | savepoints and catalog query unsupported | read-only savepoint recovery and typed empty type lookup |
| generated `$1::BIGINT`, `$2::VARCHAR`, `$3::BOOL` | native parser rejected `::` | generic typed postfix casts across parser, HIR, relational IR, compiler, planner, and executor |
| namespace/table/visibility Inspector queries | `pg_namespace` / `pg_class` syntax unsupported | structured operations derived from authorized Canonical Schema tables |
| column query joining `pg_class`, `pg_attribute`, and type functions | catalog SQL grammar unsupported | ordered derived column rows with centralized physical type mapping |
| domain and enum probes | catalog SQL grammar unsupported | typed empty results matching absent NetbaDB semantics |
| table OID and primary-key queries | no PG object identity or array result type | deterministic synthetic table OIDs, derived PK rows, server-only text-array output |
| foreign-key probe | catalog SQL grammar unsupported | conservative empty result for existing visible tables |
| non-primary-index query using arrays, `ANY`, and index functions | stable Core metadata boundary lacked index definitions | Core `CatalogInspection` index DTO plus bounded real index operation and catalog-array outputs |
| table-comment and check-constraint probes | misclassified as generic `pg_class` lookup | distinct structured operations with absent metadata represented as NULL/empty |
| psycopg prepared-cache maintenance `DEALLOCATE ALL` | native parser syntax error | session-scoped SQL-form prepared/portal cleanup |
| ORM identity load with `users.id AS users_id` | native parser rejected column alias | generic qualified projection aliases |

The classifier keys on catalog relations and semantic functions/predicates,
not the complete SQL text, whitespace, or a fixed result table. Bind values are
validated against an operation-specific type signature and drive table/schema
selection.

The real registry supports non-unique single-column Heap B+Trees. The
compatibility layer does not report the primary key as an extra secondary
index, does not report LSM clustering as an index, and rejects partition-local
physical indexes when asked for logical-table reflection. Legacy unnamed
entries receive bounded deterministic names. Explicit CREATE INDEX names are
persisted in IndexCatalog v4 (v3/v2 readable) and remain stable across reopen and connections;
domain-separated synthetic object OIDs remain server-only and deterministic.

psql Simple Query catalog SQL is recognized structurally and lowered to the
same read-only metadata evaluator as Extended Query reflection. The catalog-only
pattern subset is bounded and non-backtracking; it is not exposed as a general
SQL regex operator.

## Reproduction

Create an isolated environment once:

```bash
python3 -m venv /tmp/netbadb-pg-venv
/tmp/netbadb-pg-venv/bin/pip install \
  -r scripts/requirements-postgresql-orm.txt
```

Start the two-table fixture and copy its printed address. Use a fresh fixture for
each script run; Alembic intentionally drops the baseline legacy indexes. The ORM
script also needs psql for bidirectional checks on existing connections:

```bash
cargo run -p netbadb-server --example postgres_driver_fixture --offline
```

In another terminal, substitute the printed port:

```bash
/tmp/netbadb-pg-venv/bin/python scripts/test-postgresql-orm.py \
  --dsn postgresql+psycopg://netbadb@127.0.0.1:PORT/test

/tmp/netbadb-pg-venv/bin/python scripts/test-postgresql-alembic.py \
  --dsn postgresql+psycopg://netbadb@127.0.0.1:PORT/test

python3 scripts/test-postgresql-psql.py
```

Set `NETBADB_POSTGRES_TRACE=1` only when protocol diagnostics are needed. The
trace records SQL and lifecycle metadata but redacts password and Bind payloads.

## Round 19 real CREATE TABLE fixture

`scripts/test-sql-create-table.py` runs independent fresh databases for psql 17.11,
psycopg 3.2.13 and SQLAlchemy 2.0.52. Each rolls back one CREATE with rows, then
commits one CREATE with `(10, 'demo', true)`. The fixture shuts down, starts the
normal server against the original single-table manifest expectation, shuts down
again, and verifies catalog-only reopen, IDs, unnamed types, nullability, generation
and table version. The manifest contains no dynamic-table grants and never changes.
Post-commit creator SELECT is denied and psql metadata hides the new table.

Actual psycopg 3.2.13 behavior: no-parameter default CREATE uses Simple Query;
parameterized DML uses Extended Query. The fixture additionally uses the public
`prepare=True` API for CREATE and checks Parse/Bind/Execute trace evidence. No
prepare_threshold change, simple-protocol override or custom dialect is used.
SQLAlchemy's unmodified PostgreSQL dialect sends Table.create as Simple Query and
parameterized INSERT as Extended Query; both execute within the same transaction.
Use BigInteger/Text/Boolean, nullable flags and no primary_key/default/length options.
MetaData.create_all and table-migration apply remain outside acceptance.

```bash
CARGO_TARGET_DIR=/private/tmp/netbadb-round19-target \
  /path/to/orm-venv/bin/python scripts/test-sql-create-table.py
```

Install the exact existing `scripts/requirements-postgresql-orm.txt` dependencies.
The prior psql/ORM/Alembic index-only regression scripts continue on separate fresh
`postgres_driver_fixture` databases with explicit schema_admin and table grants.
