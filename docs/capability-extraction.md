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
