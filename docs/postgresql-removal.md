# PostgreSQL Compatibility Removal

## 1. Baseline commit

The removal was audited and implemented from commit
`e85087e5436110b9ea0d4c0821797537295218da` on the local
`codex/server-execution-resource-boundary` branch. The implementation work was
performed on `codex/remove-postgresql-compatibility`.

The initial inventory classified the repository as follows:

| Classification | Surface |
| --- | --- |
| DELETE-PG | `netbadb-pgwire`; the server PostgreSQL frontend; daemon protocol-mode dispatch; PostgreSQL session, catalog, OID, SQLSTATE, TLS-request, and cancellation behavior; PostgreSQL-only tests, examples, scripts, benchmarks, fuzzing, dependencies, and current compatibility documentation |
| KEEP-GENERIC | Typed parameters and binds; parameter inference; statement descriptions; stable diagnostics; Native SQL DDL; Core catalog and plan inspection; authorization; Native TLS; transaction ownership and disconnect rollback; storage, recovery, and persistent catalogs |
| REWRITE | Daemon lifecycle and readiness; manifest and operator tests that duplicated Native/PostgreSQL cases; the global-boundary benchmark; README, architecture, roadmap, lifecycle, and physical-type documentation |
| INVESTIGATE | Manifest-version impact, potential persistent PostgreSQL state, OID/SQLSTATE use outside the frontend, and PostgreSQL tests that might uniquely protect Core invariants |

Investigation found no PostgreSQL fields in manifest version 11, no PostgreSQL
identity in persistent state, and no OID or SQLSTATE dependency in the Native
Protocol or Core. No manifest or persistent-format version bump was required.

## 2. Why PostgreSQL compatibility was removed

NetbaDB no longer treats PostgreSQL wire, dialect, or client compatibility as a
product objective. Maintaining it added a second frontend, session model,
catalog projection, type-adaptation layer, and client test matrix that was not
required for the strongly typed Native Protocol direction.

This was a physical removal, not a feature toggle or deprecated compatibility
stub. Git history retains the former implementation.

## 3. Removed architecture

The following path no longer exists:

```text
PostgreSQL client
  -> PostgreSQL v3 wire codec
  -> PostgreSQL session and portal state
  -> PostgreSQL catalog/OID/SQLSTATE adaptation
  -> NetbaDB Core
```

The server now has one database network frontend and one concrete server
handle. The one-variant `RunningServer` dispatch and its forwarding error type
were removed.

## 4. Deleted crates/modules

- Deleted the entire `netbadb-pgwire` workspace crate, including its protocol
  constants, messages, codecs, PostgreSQL type/OID model, and inline tests.
- Deleted `netbadb-server/src/postgres.rs`, including the listener, connection
  handler, frontend/session state, compatibility evaluator, virtual catalog
  rows, and inline tests.
- Deleted the five PostgreSQL server regression modules and the standalone
  PostgreSQL TCP integration test.
- Removed the workspace, server, fuzz-workspace, and lockfile dependency edges
  to `netbadb-pgwire`.

## 5. Deleted runtime surfaces

Removed runtime behavior includes PostgreSQL startup and SSL requests,
CancelRequest, simple and extended query messages, named prepared statements,
portals, PostgreSQL text/binary parameter formats, ReadyForQuery transaction
projection, failed-transaction compatibility, row/result encoding, SQLSTATE
mapping, OID/type adaptation, `pg_catalog` and `information_schema` query
classification, and psql/ORM reflection projections.

NetbaDB typed errors, transaction correctness states, Native cancellation
semantics where independently defined, and Native scalar codecs remain
separate from those removed adapters.

## 6. Deleted config/CLI surfaces

`netbadbd --postgres` and Native/PostgreSQL startup selection were removed.
The daemon now accepts only `--manifest <path>` (plus `--help` and `--version`)
and always starts the Native Protocol server. Readiness and error handling now
use `ServerHandle` and `TcpServerError` directly.

Manifest v11 had no PostgreSQL-specific field. It remains version 11; unknown
removed or invented fields continue to fail under strict manifest decoding.

## 7. Deleted tests/benchmarks/scripts

- Deleted six dedicated PostgreSQL test files and two colocated PostgreSQL test
  suites. Native/Core coverage remains for typed parameters, DDL, schema
  invalidation, authorization, result bounds, transaction rollback,
  disconnect rollback, and recovery.
- Deleted three PostgreSQL SQL/client fixture examples.
- Deleted 21 Python/requirements files for psql, psycopg, PostgreSQL-dialect
  SQLAlchemy, Alembic, and SQL acceptance paths that depended on libpq.
- Removed the PostgreSQL half of the global-boundary benchmark: 24 default
  network matrix entries, four head-of-line scenarios, and 18 codec entries.
  Embedded and Native cases remain.
- Deleted the `pgwire_decode` fuzz target and its manifest, lockfile, and README
  references. The remaining fuzz targets are Native/Core targets.
- Deleted the active PostgreSQL compatibility guide and client matrix.

## 8. Generic capabilities intentionally retained

The cleanup retained capabilities that have current non-PostgreSQL consumers:

- typed `ParameterId`, parameter metadata and inference, prepared logical
  statements, typed scalar binding, and statement description;
- stable Native diagnostics and domain error types;
- Native SQL `CREATE TABLE`, `DROP TABLE`, `ALTER TABLE`, `CREATE INDEX`, and
  `DROP INDEX` paths through parser, HIR, compiler, and Core;
- `Database::inspect_catalog`, plan inspection, stable inspection DTOs, and the
  operator/inspection plane;
- `PhysicalType`, `SemanticType`, `ScalarValue`, stable NetbaDB IDs, SQL type
  aliases documented by Native SQL, and explicit NULL behavior;
- authorization, schema administration, Native mTLS identity, transaction
  ownership, commit/rollback, and disconnect rollback;
- SchemaCatalog, IndexCatalog, ProjectionCatalog, change-stream, heap, LSM,
  columnar, WAL, and recovery behavior.

## 9. Native Protocol verification

Native Protocol v2 is the sole network database protocol. The protocol's
legacy and v2 golden-frame tests passed, including exact client/server frame
bytes and stable message tags. Rust Native client/server integration passed for
handshake, typed queries and parameters, transactions, DDL, large/result-limit
handling, disconnect rollback, plaintext, and mutual TLS. The Go SDK race test
and the real Rust-server Go integration suite (plaintext, mutual TLS, and
generated bindings) also passed.

No file under `crates/netbadb-protocol`, `crates/netbadb-client`, or `sdk` was
changed by the removal.

## 10. Core correctness verification

The full workspace test suite passed with all features. This includes embedded
Core, compiler/HIR/planner/executor, schema DDL, heap and index, LSM, columnar,
change-stream, crash/reopen/recovery, server resource lifecycle, operator, and
daemon lifecycle tests. The storage crate alone completed 496 unit tests plus
its change-stream integration suite.

Required commands:

```text
cargo fmt --all -- --check
cargo check --workspace --all-targets --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
(cd sdk/go && go test -race -count=1 ./...)
./scripts/test-go-sdk.sh
python3 scripts/test-resource-lifecycle.py
cargo check --manifest-path fuzz/Cargo.toml --all-targets
```

The OS-resource probe returned to its baseline of seven descriptors and three
threads after both 100 and 1,000 malformed Native connections, then shut down
cleanly.

## 11. Persistent-format impact

There is no persistent-format change. Heap/page, WAL, SchemaCatalog,
IndexCatalog, LSM, ProjectionCatalog, change-stream, and coordinator encodings
were not edited. No PostgreSQL OID, role, session, or catalog-projection state
was found in those formats. Existing close/reopen, malformed/truncated input,
recovery, and catalog tests passed under the full workspace suite.

## 12. Dependency/LOC reduction

Counts are against the baseline named in section 1:

| Measure | Reduction |
| --- | ---: |
| Workspace crates | 1 |
| PG-only production Rust files | 2 |
| Dedicated PostgreSQL implementation LOC | 6,723 |
| Obsolete production dispatch/API LOC in direct callers | 108 |
| PostgreSQL test LOC | 8,914 |
| PostgreSQL fixture-example files / LOC | 3 / 1,560 |
| PostgreSQL scripts/requirements files / LOC | 21 / 3,818 |
| Active PostgreSQL documentation files / LOC | 2 / 914 |
| Fuzz targets | 1 |
| Public PG item/method declarations | 58 |
| Default benchmark scenario entries | 46 |
| Internal dependency edges | 2 (`server`, fuzz workspace) plus the workspace member |
| Third-party dependency packages | 0; the PG crate used existing workspace dependencies |

The 6,723 dedicated implementation lines exclude the two deleted modules'
inline test suites. The additional 108 lines are the daemon's dual-server
dispatch plus server authorization/export code made obsolete by removal.
Documentation rewrites and stale-comment edits are not included in production
LOC.

## 13. Remaining historical references

PostgreSQL-related wording may remain only where it records past work or states
that compatibility was removed. The final search finds 97 documentation files:
this report, removal notes in `architecture.md` and `roadmap.md`, and historical
records. The historical set consists of `adaptive-operations-phase*.md`, dated
`*-round*.md` records, `columnar-phase1.md`, superseded
`server-manifest-v4.md` through `server-manifest-v10.md`, superseded
`server-operator-protocol-v1.md` through `server-operator-protocol-v5.md`,
`physical-design-mutation-receipts-v1.md`, and the crash, error/concurrency,
global-performance, resource-boundedness, and server-execution-resource audits.
The roadmap labels all older entries as historical and removed from the active
direction. Immutable audit reports retain accurate historical results; the
most likely to be mistaken for current guidance now carry a removal notice.

Current README, architecture, manifest, daemon lifecycle, physical-type,
protocol, benchmark, build, test, script, and SDK surfaces contain no
PostgreSQL product claim or executable compatibility path. This removal report
is intentionally the only current document named for PostgreSQL.

## 14. Final repository boundaries

The supported boundary is now:

```text
Rust Embedded API ------------------+
Rust SDK -> Native Protocol v2 -----+-> NetbaDB Core
Go SDK   -> Native Protocol v2 -----+
                                      -> typed compiler and relational IR
                                      -> planner and executor
                                      -> transactions
                                      -> heap / LSM / derived columnar state
```

There is no replacement compatibility wire protocol. PostgreSQL clients,
pgwire, catalog emulation, and PostgreSQL dialect/client contracts are outside
the product boundary.
