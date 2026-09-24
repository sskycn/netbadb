# Capability extraction

This document tracks the staged extraction against the actual workspace. It is
not a format upgrade or a change to database transaction ownership.

## Baseline

- Starting HEAD: `b36f784584132ce21fe24ed840da681613487881` on a clean
  `main` tracking `origin/main` (24 September 2026).
- Task branch: `codex/capability-extraction`; no pre-existing user changes.
- The workspace initially had 22 members. `netbadb-storage` depended on
  `netbadb-index`, `netbadb-schema`, `netbadb-types`, and `crc32c`; it contained
  Heap, LSM, Columnar, Change Stream, row codec, and table dispatch in one crate.
- Baseline `cargo fmt --all -- --check`, `cargo check --workspace --all-targets`,
  and `cargo clippy --workspace --all-targets -- -D warnings` passed after
  fetching the locked dependencies. Baseline `cargo test --workspace` reached
  `netbadb-cli --test cli` and failed two tests because this sandbox denied
  binding `127.0.0.1:0` (`Operation not permitted`). These were baseline
  environment failures, before source edits. Direct dependency fetch succeeded
  after removing a disconnected local proxy from the command environment.
- The default storage feature set is empty. `netbadb-core/test-hooks` enables
  `netbadb-storage/test-hooks`; server test builds also enable Core test hooks.
  Moving instrumentation must preserve one observation stream across crates.

## Dependencies

`A -> B` means A depends on B. Before extraction:

```text
executor -> storage -> index + schema + types
core -> executor + storage + planner + compiler + schema + types + others
planner -> index + rel + types
```

Actual production direction for the extracted capabilities (external libraries omitted):

```text
storage-api -> index + types
row-codec -> schema + types
change-stream -> storage-api + row-codec + schema + types
lsm -> change-stream + storage-api + row-codec + index + schema + types
columnar -> change-stream + storage-api + index + schema + types
heap -> change-stream + storage-api + row-codec + index + schema + types
storage facade -> heap + lsm + columnar + change-stream + storage-api
query-feedback -> planner + rel + storage-api + types
advisor -> lsm + columnar + change-stream + storage-api + types
executor -> query-feedback + planner + rel + storage facade + types
core -> advisor + query-feedback + executor + storage facade + existing compiler/catalog layers
```

`TableStorage`, `StorageTransaction`, and `StorageReadView` stay in the
facade because they contain concrete engine state.

## Ownership and stages

| Stage | Ownership | Status |
| --- | --- | --- |
| 1 | `storage-api`: engine-free access and transaction descriptions; `row-codec`: the one positional row encoder/decoder | Complete; direct, old-path, and isolated-consumer tests passed |
| 2 | `change-stream`: NBCL manager, cursors, replay, pins, GC | Complete; direct, facade integration, and baseline compatibility tests passed |
| 3 | `lsm`: WAL, MemTable, SSTables, transactions and recovery | Complete; 54 direct tests, independent consumer, fuzz build and facade integration passed |
| 4 | `columnar`: derived NBC artifacts and scans | Complete; 31 direct artifact, lazy I/O and crash tests plus isolated consumer passed |
| 5 | `heap`: complete page, WAL, index mutation and transaction boundary | Complete; 380 direct tests, isolated consumer, old-file compatibility, MSRV, SDK and fuzz checks passed |
| 6 | `query-feedback`: telemetry DTOs, typed estimate/actual correlation, caller-owned windows, pure workload assessment | Complete; direct tests and workspace check passed |
| 7 | `advisor`: maintenance admission/ranking, LSM proposal and exact revalidation, logical-tick gate | Complete; direct tests and production Core regression tests passed |

The first stage moves `AccessPathCapabilities`, `StorageAccessCostHints`,
`StorageAccessPath`, `StorageKind`, `StorageVisibilityBoundary`,
`IsolationLevel`, `Snapshot`, and prepared transaction descriptions to
`storage-api`. `ReadView` stays with the engine because its `Drop` unpins live
transaction-status state. Boundary construction now returns the typed
`VisibilityBoundaryError` at the direct API; storage operations map it to the
historical `StorageError` variants. This changes the error type of direct
`StorageVisibilityBoundary::new` calls, while preserving successful values and
validation behavior.
The row codec implementation and `CodecError` move to `row-codec`; storage keeps
only an error adapter so its historical `StorageError` variants remain observable.
The `ChangeStreamManager`, NBCL codec, replay, pin and GC logic, inspection DTOs,
and unit tests move to `change-stream`. Heap and LSM still own their prepare and
commit integration. Its recovery open requires an authoritative outcome callback;
it cannot promote prepared records by itself. Storage re-exports the former
public symbols and maps `ChangeStreamError::Schema` back to the former
`StorageError::Schema`. The shared source-inspection test counter moved to
`storage-api` with explicit test-hooks propagation so engine and stream calls
continue to update one thread-local observation.

The full LSM implementation, fault-injection tests, manifest/WAL/SSTable
decoders, and fuzz entry points now live in `netbadb-lsm`. The facade re-exports
the original `LsmStorage` type identity and maps `LsmStorageError` into its
historical structured `StorageError` variants. An LSM handle exposes only a
checked committed-version identity; its observed state and clustering key stay
private. Direct LSM users can create, commit, read, close, and reopen without
the facade. The three LSM fuzz targets compile against the new crate directly.

The NBCM/NBCS/NBCD implementation and its tests now live in
`netbadb-columnar`. The facade keeps the prior sizing API: it converts a fresh
Heap or LSM source inspection into a mode-specific `ColumnarSourceFootprint`
and maps the new crate's typed overflow error to the historical
`StorageError::ResourceBoundOverflow`. The footprint is an observation for one
admission check, not a durable permit. The projection catalog and pending
build intent remain in Core; a standalone Columnar artifact is derived data.

The complete Heap page, buffer, WAL, MVCC, transaction, recovery, physical
B+Tree and reclaim implementations now live in `netbadb-heap`, with their
local fault and persistence tests. `HeapStorage`, `Page`, `Transaction`,
`RecoveryError` and related types retain their facade import paths through
re-exports. `HeapStorageError` belongs to the engine and the facade translates
it to the prior structured `StorageError` variants. The storage facade now
contains concrete Heap/LSM dispatch, shared compatibility exports and error
adaptation, with no second Heap implementation. A direct consumer tests index
creation, commit, rollback, scan and reopen without Core or the facade.

The compatibility script also creates a baseline LSM table with the original
public dispatcher, compares its deterministic manifest bytes with the current
writer, then opens the baseline table in the extracted engine, appends and
reopens it. Its Heap/NBCL baseline read and append checks still pass.

`netbadb-query-feedback` now owns the executor telemetry DTOs, feedback reports, typed estimate/actual correlation, bounded query-shape windows, calibration aggregation, and pure Columnar/workload outcome assessment. The executor and Core retain the previous public import paths through re-exports. `ColumnarScanStatistics` is a primitive counter DTO in `storage-api`, re-exported from Columnar and storage. Core still obtains the current schema/visibility/target state, checks runtime suppression and applies the existing keep/revert transitions; it never persists a feedback report.

`netbadb-advisor` owns immutable maintenance candidates, budget and consumption arithmetic, structural blocker preservation, deterministic priority/round-robin ranking, LSM flush/compaction observations and proposals, exact proposal revalidation, and the caller-supplied logical-tick gate. Core collects current storage-authored observations, revalidates the operation through its existing execution owner, runs the mutation, and maps the pure scheduler result to the historical API. The evidence progress token and renewal reason are now shared advisor DTOs. No timer, thread, storage handle, or `Database` dependency was added. The advisor's decisions remain proposals, not durable permits. Other existing adaptive and physical-design decisions remain in Core; this extraction keeps their existing ownership rather than copying an entire file.

Cross-crate direct calls required exposing the LSM methods formerly scoped to the monolithic storage crate. An audit found every such method is used by `TableStorage` dispatch. The LSM row handle state remains private and is exposed only through a checked committed-version accessor. Direct Heap APIs now return `HeapStorageError` and direct LSM APIs return `LsmStorageError`; facade operations map them to the historical structured `StorageError`. `RecoveryError::Storage` in the direct Heap path therefore carries `HeapStorageError`. No persistent encoding change is associated with these source-level API changes.

The facade retains the old public paths through `pub use`. The new crate paths
are directly usable. Experimental APIs are not declared stable by this step.

## Compatibility and validation

No magic, version, tag, integer width, byte order, checksum, schema canonical
bytes or fingerprint, WAL record, NBCL record, resource name, protocol message,
or transaction publication order is intentionally changed. Golden row bytes in
the existing row codec tests remain the baseline compatibility fixture. Final
validation must include old-data reopening, direct consumers, dependency
checks, full workspace commands, CI MSRV and feature combinations, and affected
fault injection and fuzz entry builds. Record actual outcomes here as phases
complete; a new encoder round trip alone is not evidence of compatibility.

Final validation on the completed source tree:

- `cargo fmt --all -- --check`, `cargo check --workspace --all-targets --offline`, `cargo clippy --workspace --all-targets --offline -- -D warnings`, and `git diff --cached --check` passed.
- `cargo test --workspace --offline` passed with 1,863 tests across 74 unit, integration, and doc-test suites; 0 failed and 3 existing manual tests ignored. The CLI test suite ran outside the filesystem sandbox to permit loopback port binding.
- `cargo +1.85.0 check --workspace --all-targets --offline` passed. The three CI SDK combinations (`--no-default-features`, `--no-default-features --features remote`, `--all-features`) and `netbadb-core --features test-hooks` passed.
- `cargo check --manifest-path fuzz/Cargo.toml --bins --offline` and `go test ./...` in `sdk/go` passed.
- `scripts/check-independent-consumers.sh` passed for direct Heap, LSM, Columnar, Change Stream, row codec, feedback, and advisor APIs without the storage facade, executor, Core, or server.
- `scripts/check-capability-deps.py` passed for normal, optional, build, and direct development edges reported by Cargo metadata.
- `scripts/check-baseline-change-compatibility.sh` passed: baseline Heap and NBCL writes were read and extended by the new engine; deterministic NBCL bytes matched; a baseline LSM table was opened and extended by the new engine and its deterministic manifest bytes matched.

No persistent magic, version, schema fingerprint, protocol message, or WAL/NBCL publication ordering changed. The baseline CLI loopback failures inside the sandbox were environmental; the final out-of-sandbox workspace suite passed.

## Deferred boundaries

Database transaction coordination, full catalog lifecycles, DDL rewrite,
schema mutation journal, generic RPC and code generation, remote CDC,
replication, asynchronous core execution, and multiple active writers per
`StorageId` are outside this extraction. They remain in their current owners.

## Acceptance hardening (24 September 2026)

This pass validates the extraction at `7b30a6df103a574aa3aa44aab0826e8aee96cd65`
against baseline `b36f784584132ce21fe24ed840da681613487881`. It adds
acceptance scripts and CI wiring, without changing production storage,
protocol, SQL, transaction, or catalog code. The extracted implementation
scope remains the seven capabilities above. Advisor owns maintenance ranking,
budget decisions, LSM proposal/revalidation, and logical-tick gating. Other
adaptive and Physical Design logic remains in Core.

### What the original scripts covered

Before hardening, `check-independent-consumers.sh` ran one external Cargo
project whose manifest declared every extracted engine and decision crate.
`check-capability-deps.py` traversed workspace declarations from
`cargo metadata --no-deps`, including optional path edges, but did not
traverse the resolved graph or fail when a required capability package was
missing. `check-baseline-change-compatibility.sh` tested baseline Heap/NBCL
write to current read/append/reopen, a baseline/current NBCL byte comparison,
baseline LSM write to current read/append/reopen, and a baseline/current LSM
manifest comparison. It did not establish reverse readability or Columnar
baseline artifact compatibility. None of the three scripts ran in CI.

### Dependency and consumer evidence

`capability-acceptance` runs on push and pull request. It checks out complete
history, records the current/baseline commits and Rust/Cargo versions, fetches
locked current and baseline dependencies, then runs the boundary checker,
checker fixtures, combined consumer, seven separate minimal consumers, and
compatibility matrix. A nonzero check exit fails the job. The external
projects still resolve dependencies offline, so the job does not depend on
cache contents inherited from a developer computer.

The boundary checker tests both declared optional path edges and the full
resolved package graph. It compares package identities (source and version),
and follows normal and build edges transitively, including workspace-external
adapters. Dev edges are checked at the capability root only; dependency tests
are not built by a consumer. For an inactive optional external registry
dependency, Cargo has no resolved transitive graph; its declaration remains
visible for inspection, while indirect edges can only be checked when
resolved. Failure messages include each edge, kind, alias, package identity,
and full route.
Isolated checker fixtures verify direct, renamed, transitive external,
inactive optional, build, missing-package, legal, and same-name-different-
identity cases. They do not edit production manifests.

Each of the seven new consumers has its own workspace-external manifest and
declares only the target capability and crates actually used by its case. They
exercise row values/NULL/type and malformed input; Change Stream prepare,
publication after a test-owned durable decision ledger, authoritative-outcome
callback on reopen and bounded replay; LSM and Heap commit/rollback/reopen;
Columnar publish/reopen/scan; nonempty feedback
correlation and rejected identity mismatch; and Advisor ranking, budget
rejection, stale proposal revalidation and tick gating. Advisor's synthetic
eligible observation tests its pure proposal comparison; it does not modify an
engine or grant mutation authority. Each manifest's resolved normal/build
graph is checked for Core, Server, Executor, the storage facade, and
`test-hooks`. The original combined consumer remains in place.

### Persistent and API compatibility evidence

The compatibility matrix compiles independent external baseline and current
Cargo programs. These are normal close/reopen tests, **not** crash-recovery
tests. The existing Heap, LSM, Change Stream and Columnar process-crash/fault
injection tests remain the recovery evidence and are run separately.

| Format | Baseline write → current read | Current write → baseline read | Current append to baseline data → baseline read | Current clean reopen | Deterministic bytes |
| --- | --- | --- | --- | --- | --- |
| Heap with NBCL | yes, including Change Stream replay | yes | yes, two replayed batches | yes | NBCL log bytes |
| LSM | yes | yes | yes, two rows | yes | initial manifest bytes |
| Columnar base artifact | yes | yes | not claimed: managed generation/advance requires Core catalog authority; this matrix opens immutable published base artifacts | yes | manifest and base segment bytes |

These comparisons cover the named artifacts and scenarios, not every page,
WAL, SSTable, delta, or catalog format. A current encoder/decoder round trip
alone does not establish baseline compatibility. No forward-writing guarantee
is claimed beyond the explicit Heap/NBCL and LSM append cases.

External compile cases cover old-path `HeapStorage` and `LsmStorage`
create/open-style calls, `TableStorage` Heap/LSM operations, error adapters,
visibility-boundary construction, and type identity of old reexports:

| API | Source compatibility |
| --- | --- |
| `netbadb_storage::TableStorage` and its `StorageError` results | Retained; baseline and current external programs compile and run. |
| Old `netbadb_storage::{HeapStorage,LsmStorage}` paths | Retained as the same concrete types reexported from the new engine crates. Common externally callable operations compile against both versions. |
| Direct Heap/LSM operation errors | Deliberately changed from facade `StorageError` to `HeapStorageError` / `LsmStorageError`; facade `From` adapters retain structured errors. Callers with explicit direct-operation result types must update them. |
| `StorageVisibilityBoundary::new` errors | Deliberately changed from `StorageError` to `VisibilityBoundaryError`; callers with explicit error types or `?` conversion must adapt. |
| `RecoveryError::Storage` nested error | Deliberately carries `Box<HeapStorageError>` in the direct Heap path, rather than `Box<StorageError>`; pattern matching callers must adapt. |
| Direct LSM mutation methods | Newly public to support facade delegation across crates. The baseline's `LsmStorage::insert` was crate-private, so it is not a preexisting stable external call. |

The current-only external compilation checks typed result/error identities, not
just the presence of `pub use`. New lower-level methods remain implementation
entry points: a public Rust signature does not make transaction ownership,
generation, or recovery bypass a supported external contract. Prepared Change
and Columnar publication tokens retain private fields; recovery and generation
checks remain in their owning engines. LSM row-handle fields are private, and
mutation methods validate handles against the owning engine. Its
`committed_version_key(storage_id)` helper takes a caller-supplied storage ID
and checks committed versus pending version state; that DTO helper is not a
storage-identity authority. This pass found no source-and-test-proven runtime
corruption defect to repair.

### Protocol and verification status

No protocol source or wire format changed in this pass. Protocol compatibility
is therefore bounded to the existing workspace protocol tests; there is no
new cross-version wire fixture in this acceptance matrix.

Local validation on this acceptance tree passed:

- `cargo fmt --all -- --check`, `cargo check --workspace --all-targets`,
  `cargo clippy --workspace --all-targets -- -D warnings`, and
  `cargo test --workspace`. The first sandboxed full test run hit the
  previously documented loopback bind restriction in two CLI tests; the
  complete rerun with local loopback permission passed.
- `cargo +1.85.0 check --workspace --all-targets`, all three CI SDK feature
  combinations, and MSRV remote SDK check.
- Current and baseline `cargo fetch --locked`; checker and nine isolated
  fixture tests; combined and seven minimal external consumers; baseline
  compatibility matrix; script syntax checks; and `git diff --check`.
- Separate process-crash tests for Change Stream grouped finalize, LSM durable
  commit, Columnar delta publication, and Heap no-force winner redo.

Remote CI is a separate result. The new job had not been triggered when this
record was written; inspect the pushed commit's Actions run for its actual
status. Local success is not a remote CI result.
