# PostgreSQL client compatibility matrix

This matrix records Round 5 observations against a real loopback TCP listener.
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
| Alembic inspect / autogenerate | yes, read-only | matching metadata has no diff; absent indexes propose two `remove_index` operations |
| psql `\d` | yes | columns, types, nullability, PK and real secondary indexes; qualified/missing/wildcard variants |
| psql `\dt` | yes | authorized `public` tables and name/schema patterns |
| psql `\di` | yes | compatibility PK and real secondary indexes, plus name patterns |
| DDL / migration apply | unsupported | not invoked; existing-schema profile only |

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
physical indexes when asked for logical-table reflection. It synthesizes
bounded deterministic names because the native registry has no user-defined
index name. The names and domain-separated synthetic object OIDs are stable for
the same Canonical Schema/index registry across reopen and connections.

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

Start the two-table fixture and copy its printed address:

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
