# Runtime Schema Catalog Foundation — Round 17

Implemented on `246e353b45dee9845dd653e6d4804b4617beda6c`, retaining the
[Round 16 bootstrap + persistent authority decision](table-schema-lifecycle-round16.md).
This round adds no table DDL and no schema mutation/replace API.

## Authority and public boundary

`Database::create_catalog(catalog_path, specs, optional_coordinator)` and
`create_catalog_with_placements(catalog_path, specs, partition_config)` accept
initial bootstrap schema. The explicit root supports an empty schema as well as
multiple Heap tables, mixed Heap/LSM and existing range placements. Installation
returns a Database whose `CommittedCatalogState.schema` was decoded from disk.
The registry and physical bindings validate against this schema before publication.

`Database::open_catalog(catalog_path)` needs no external TableDef/Schema. It reads
the independent initialized discriminator, validates the snapshot, reconstructs
all logical metadata, checks all physical identities, recovers every participant,
then publishes the immutable committed state. Compiler, execution, inspection,
Protocol v1 metadata and the PG projection use that state.

`open_catalog_with_expectation(catalog_path, Option<&Schema>)` checks an exact
required subset: same TableId and existing canonical fingerprint. Additional
committed tables remain open and visible before authorization. Missing expected
tables, a same-name/different-ID table, changed shape, column order, PK flags or
nominal type fail. External definitions cannot rewrite schema or repair a missing
initialized snapshot. Read-only opens leave catalog and marker bytes unchanged.

Compatibility constructors/open methods remain to avoid breaking all SDK,
example and benchmark callers. This is a deliberately narrow transition exception
to Round 16's explicit-root recommendation: old signatures have no root argument.
Their create adapters select `<single/first-storage>.schema`,
`<coordinator>.schema`, or `<partition-catalog>.schema` **once**, then invoke the
explicit-root constructor. Database-level snapshots/markers are independent
files; no user Heap page becomes the database anchor. Reopen follows durable
locator sidecars and marker validation, never the first table as schema truth.
New applications should use explicit roots. `open_tables_with_expectation` makes
the server/manifest transition explicit. Public open never invokes migration.

## Installation, persistence and identity

[SchemaCatalog v1](schema-catalog-v1.md) is the complete byte and crash contract.
Magic/version/lengths/endian/reserved bytes/CRC32C are explicit. The full snapshot
preserves table/column declaration order, sparse IDs, physical and nominal types,
NULL and descriptive PK metadata, canonical fingerprints, placements, relative
locators, LSM clustering metadata and coordinator/evidence locations. Ordered
encoding is deterministic for the same snapshot, including its incarnation.

An independent `<catalog>.state` marker distinguishes pending/uninitialized from
initialized. Pending intent is durable before fresh physical creation. Only after
snapshot fsync, rename and directory fsync may an initialized marker be published.
Initialized plus missing/corrupt/wrong-version catalog hard-fails. Orphan snapshots
are never trusted by ordinary open. A surviving managed locator with a missing
marker cannot authorize re-import or a replacement incarnation. Shadow files are
never startup authority.
A 128-bit OS-random incarnation binds snapshot, marker and locator; existing
physical formats lack database incarnation, so writable cloning remains unsupported.

SchemaGeneration and TableSchemaVersion are distinct strong types, initially 1.
They persist across reopen and index CREATE/DROP does not change either. Fingerprint
remains the existing per-table content digest. Runtime `catalog_generation` stays
process-local, initially 0; no index refactor was bundled here.

TableId, ColumnId, StorageId and PartitionId high-waters are stored as authoritative
next-ID or exhausted states. Bootstrap alone computes checked successors. There
was no independent durable StorageId/PartitionId allocator to preserve: creation
ordinals and supplied partitions were the previous sources. The new catalog owns
these high-waters once, while physical files/registry retain only identities.
Maximal IDs exhaust their domains; zero ColumnId remains legal, but zero TableId,
StorageId and PartitionId are rejected rather than renumbered.

## Explicit legacy import

`Database::open_legacy_and_install_catalog(root, complete_schema,
CompleteLegacyInventory::attest_complete(...))` requires a separate physical
inventory containing Heap/LSM locations and optional coordinator/PartitionCatalog
paths. Table definitions cannot double as an implicit completeness declaration.
A schema subset of the declared physical inventory fails before catalog/marker
installation. Identity, fingerprint, engine and clustering metadata are validated;
range placement and exact retained coordinator participants are checked too.
Existing index recovery remains in the physical engines.

Old arbitrary file compositions have no independently enumerable database-owned
namespace. Completeness is an explicit operator attestation; Core proves equality
between that physical inventory and supplied schema, not that the operator listed
every unrelated file on disk. There is no filename guessing, directory scan,
silent adoption, automatic renumbering or cleanup of unknown files. If the operator
omits both a schema table and its physical file, no old database root exists to
prove that omission; this limitation is explicit in the API contract.

Heap, LSM and range legacy imports are tested. Successful import is one-time;
repeated import fails even if its catalog was subsequently deleted. Interrupted
installation can retry only with the same durable intent and complete physical
resources. Incomplete static physical creation requires explicit operator repair;
Round 17 never creates an empty replacement for a missing active storage.

## Server, SDK, inspection and PostgreSQL

Manifest v4 still configures listeners, security and required Heap table
expectations/locators. It does not create a database or automatically migrate old
files. Existing provisioning fixtures install catalog once; the worker calls
`open_tables_with_expectation` and opens all committed resources, including tables
not listed by the manifest. Missing/wrong expected IDs or fingerprints fail before
sessions are admitted. Grant configuration remains external and its expected IDs
are validated against persisted tables before use; authorization still filters
session visibility after Core owns the complete catalog.

Embedded Rust reexports the explicit catalog/import types. Protocol v1 framing,
capabilities and per-table fingerprints are unchanged; the new internal failure
maps to the existing Database error code. Remote Rust/Go still require exact table
fingerprints and allow extra visible tables. Generated code remains unchanged.
The real Go/native and PostgreSQL fixtures now explicitly reopen by catalog alone.
PG catalogs remain pure projections of Core inspection; OID regression tests
compare the same TableIds/fingerprints across catalog-only reopen. No PG schema
persistence, compatibility SQL extension, table DDL or Alembic table apply exists.

## Crash matrix

Both fresh and genuine legacy Heap fixtures run abrupt subprocess exits at each
point below. Exit code 89 proves the intended hook was reached. Losers assert the
specific `LegacyCatalogRequired` error, explicitly import/retry the same inventory,
then reopen successfully. Winners assert ordinary catalog-only reopen succeeds.

| Hook | Observed process-exit result |
| --- | --- |
| before-catalog-write | Loser |
| mid-snapshot-write | Loser |
| after-shadow-sync | Loser |
| after-snapshot-rename | Loser |
| after-snapshot-durable | Loser |
| before-initialized-marker | Loser |
| mid-initialized-marker-write | Loser |
| after-initialized-marker-shadow-sync | Loser |
| after-initialized-marker-rename | Winner |
| after-initialized-marker-durable | Winner |
| before-return | Winner |

These are 22 process-crash cases, not a claim of hardware power-loss simulation.
The publication filesystem assumptions and incomplete physical-creation boundary
are detailed in the format contract.

## Deferred work and Round 18 gate

No runtime CREATE/DROP/ALTER TABLE, parser support, transaction-local schema
overlay, coordinator schema participant, private staged Heap creation, physical
table cleanup or public schema replacement was added. PK flags are still metadata,
not enforced uniqueness. Existing runtime revision saturation and anonymous-index
notification omissions remain follow-up blockers for unified invalidation.
No arbitrary concurrent process ownership, moved-individual-file persistence,
downgrade, or writable-clone protocol is claimed.

Next is **Core Transactional Heap CREATE TABLE Foundation**, only after review of
this catalog boundary: durable non-rollback TableId/ColumnId/StorageId reservation,
a schema overlay and private physical creation, a prepared full snapshot bound to
the database coordinator decision, publication/rollback cleanup, crash winner/loser
proofs, and prepared/schema invalidation primitives. Reservation history must not
be overwritten by an older schema candidate. SQL CREATE TABLE belongs in a later
round, after those Core semantics. If catalog recovery issues remain, resolve them
before opening that scope.

## Validation record

The exact primary toolchain remains **1.97.1**. Commands ran from this isolated
worktree, with build outputs, client fixtures, fuzz mutations and logs under
`/private/tmp`; none are repository artifacts. The following primary checks passed:

```bash
cargo fmt --all -- --check
rustfmt --edition 2024 --check fuzz/fuzz_targets/schema_catalog_decode.rs
CARGO_TARGET_DIR=/private/tmp/netbadb-round17-target cargo check --workspace --all-targets --all-features --offline
CARGO_TARGET_DIR=/private/tmp/netbadb-round17-target cargo clippy --workspace --all-targets --all-features --offline -- -D warnings
```

The full workspace command is:

```bash
CARGO_TARGET_DIR=/private/tmp/netbadb-round17-target cargo test --workspace --all-features --offline
```

The final run passed **827 tests, 0 failed, 0 ignored**, across 58 completed
result groups. Core had 59 library tests, including 17 catalog-specific tests; the
storage library passed all 376 tests. The final run includes the managed-marker
loss regression and all review fixes.
Catalog-specific coverage includes codec bounds/corruption, exact bytes and
fingerprints, exhausted/historical high-waters, catalog-only Heap/LSM/range reopen,
expectation subsets, complete-inventory rejection, missing/swapped physical files,
empty catalogs with retained coordinator participants, whole-directory relocation,
symlink parent semantics, missing managed markers, and the 22 crash cases above.
Existing BTree/index maintenance/WAL/coordinator recovery and Rust Protocol v1
regressions remain part of the workspace run; no assertions were removed to bypass
those invariants.

MSRV validation:

```bash
CARGO_TARGET_DIR=/private/tmp/netbadb-round17-msrv-target cargo +1.85.0 check -p netbadb-types -p netbadb-schema -p netbadb-schema-spec -p netbadb-storage --all-targets --offline
CARGO_TARGET_DIR=/private/tmp/netbadb-round17-msrv-target cargo +1.85.0 check -p netbadb-core -p netbadb-server --all-targets --offline
```

The first command passed. The second is blocked by existing `E0658` let-chain
errors at `crates/netbadb-planner/src/lib.rs:892` and `:904`. That planner code,
the development toolchain and workspace MSRV were deliberately left unchanged.
Core/server MSRV success is therefore **not** claimed.

Real-client and generated-code checks passed against the final implementation:

```bash
CARGO_TARGET_DIR=/private/tmp/netbadb-round17-target cargo build -p netbadb-server --examples --offline
NETBADB_PSQL_TARGET_DIR=/private/tmp/netbadb-round17-target python3 scripts/test-postgresql-psql.py
/private/tmp/netbadb-round13-pg-venv/bin/python scripts/test-postgresql-orm.py --dsn postgresql+psycopg://netbadb@HOST:PORT/netbadb
/private/tmp/netbadb-round13-pg-venv/bin/python scripts/test-postgresql-alembic.py --dsn postgresql+psycopg://netbadb@HOST:PORT/netbadb
(cd sdk/go && NETBADB_GO_FIXTURE_BIN=/private/tmp/netbadb-round17-target/debug/examples/go_sdk_fixture go test -count=1 -tags=integration ./...)
CARGO_TARGET_DIR=/private/tmp/netbadb-round17-target CARGO_NET_OFFLINE=true sh scripts/check-generated-sdk.sh
```

`HOST:PORT` was the ephemeral address printed by a fresh
`postgres_driver_fixture` process for each Python probe; each process shut down
cleanly after closing its stdin. Versions were psql **17.11**, psycopg **3.2.13**,
SQLAlchemy **2.0.52** and Alembic **1.16.5**. Alembic's existing guarded index-only
probe reported baseline/final differences zero and exercised named/legacy index
apply; no table migration apply or `create_all()` was added. Both Go packages
passed. Generated Rust/Go SDK output remained byte-identical.

Fuzz smoke command, executed separately for each target with offline dependencies
and `CARGO_TARGET_DIR=/private/tmp/netbadb-round17-fuzz-target`:

```bash
CARGO_NET_OFFLINE=true CARGO_TARGET_DIR=/private/tmp/netbadb-round17-fuzz-target cargo +nightly fuzz run TARGET /private/tmp/netbadb-round17-fuzz/TARGET -- -runs=1000 -artifact_prefix=/private/tmp/netbadb-round17-fuzz/TARGET-artifacts/
```

The targets are `schema_catalog_decode`, `btree_decode`, `index_catalog_decode`,
`wal_recovery` and `pgwire_decode`. Each passed 1,000 runs without a crash or panic;
this is a smoke test, not exhaustive fuzzing. The four checked-in catalog seeds
matched two independent exports from
`schema_catalog_tests::codec_reviewed_seed_export` byte for byte. Random mutations
and artifacts stayed outside Git.

`git diff --check` and local documentation-link validation also passed. The task
branch/worktree are retained; no merge, cherry-pick, push, reset, restore, stash or
clean was performed. The original worktree remained clean at the requested base.
