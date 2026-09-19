# Adaptive Operations Phase 34 — Component-scoped mutation admission

Phase 34 adds explicit, synchronous admission to Core and the programmatic
Native/PostgreSQL Server controls. A caller supplies one immutable policy with
one existing proposal. Only a still-needed mutation gets a fresh Phase 33 work
inspection, a pure component comparison, and immediate entry into the existing
mutation authority. There is no automatic Physical Design or scheduling.

## Policy and component accounting

The public Core types are:

- `PhysicalDesignMutationAdmissionConstraint::{Unconstrained, AtMost(u64)}`;
- `PhysicalDesignMutationAdmissionDimension`;
- `PhysicalDesignMutationAdmissionLimits`, with all six constraints explicit;
- `PhysicalDesignMutationAdmissionPolicy`, with private limits, a `limits()`
  getter, and a validated `new(limits)` constructor.

None of the constraint, limits, or policy types implements `Default`.
`new` rejects an all-Unconstrained policy with
`PhysicalDesignMutationAdmissionPolicyError::NoConstrainedDimension`. Callers
wanting the old behavior use the existing unadmitted APIs. Zero and `u64::MAX`
are legal maxima.

For each explicitly constrained component, `Bounded(N)` passes `AtMost(M)`
exactly when `N <= M`. A constrained `NotProven` returns
`RequiredBoundNotProven`, even at `u64::MAX`. `Unconstrained` means outside this
policy; it does not prove safety, zero work, or a whole-mutation limit.

The first failing dimension is returned in this fixed order:

| Order | Dimension | Phase 33 source |
| ---: | --- | --- |
| 1 | `SourceWorkUnits` | `bounds.source_work_units` |
| 2 | `SourceReadBytes` | `bounds.source_read_bytes` |
| 3 | `PrerequisiteWorkUnits` | prerequisite `work_units` |
| 4 | `PrerequisiteReadBytes` | prerequisite `read_bytes` |
| 5 | `PrerequisiteWriteBytes` | prerequisite `write_bytes` |
| 6 | `OutputWriteBytes` | `bounds.output_write_bytes` |

A formally absent prerequisite (`None`, including Index) is normalized to three
`Bounded(0)` components only during comparison. A Snapshot LSM `LsmFlush`
prerequisite maps the existing `LsmMaintenanceBoundInspection` fields directly;
Phase 34 neither recomputes its formula nor executes the flush during admission.
For a nonempty MemTable, the existing flush read component uses its resident
byte accounting, independently of source persistent SSTable bytes. The current
positive flush work/read/write bounds are each tested one below and at equality.

Components are never summed. For example, separate source-read and prerequisite-
read bounds of N both fit their own maximum N, even when their sum exceeds N.
There is no total work/byte budget, cross-engine work conversion, elapsed-time
prediction, output-size inference, free-space check, CPU or memory limit.

| Mutation | Source work | Source persistent bytes | Prerequisite |
| --- | --- | --- | --- |
| Heap Index | Proven Index backfill pages | Proven Index backfill extent | Zero |
| Heap Snapshot Columnar | Proven managed pages | Proven current main-file extent | Zero |
| Heap Incremental Columnar | Proven managed pages | Proven current main-file extent | Zero; existing healthy stream required |
| LSM Incremental Columnar | NotProven | Proven current SSTable bytes | Zero; existing healthy stream required |
| LSM Snapshot, empty MemTable | NotProven | Proven current SSTable bytes | Zero |
| LSM Snapshot, nonempty MemTable | NotProven | NotProven after flush | Proven existing flush components |

Output-write bytes remain NotProven for every mutation. Any constrained output
therefore rejects a needed build. Index uses the separate Phase 33 backfill
bounds, including possible empty-tree/catalog growth, never ordinary Heap
`P - 1` scan bounds. For nonempty Snapshot LSM, a policy constraining only the
flush's write component may admit the build. This is **partial component
admission**, not a claim that the source traversal or whole build is bounded.

## Core ordering and authority

The additive APIs are:

```rust,ignore
Database::apply_physical_index_design_with_admission(
    &mut self, evidence, proposal, index_name, admission,
)
Database::apply_physical_columnar_design_with_admission(
    &mut self, evidence, proposal, admission,
)
```

Both old and new APIs share private preflight and mutation helpers. Preflight
preserves the existing order: durable database identity, backward visibility,
exact name/location retry or conflict, structural proposal revalidation,
current coverage, evidence epoch, and current advisor recommendation. Exact
`AlreadyApplied` and different-design `AlreadyCovered` complete before any
admission inspection. They remain no-ops even with impossible limits, a rotated
evidence window, or physical growth since proposal. Existing stale, conflict,
lineage, evidence and recommendation errors retain their precedence.

Only `Ready` reaches the fresh corresponding
`inspect_physical_*_design_mutation_work` method. Its report is compared
privately, then the shared mutation helper immediately calls `create_named_index`
or the selected existing Snapshot/Incremental Columnar build API. Only local
value construction intervenes. No query, DML, maintenance, callback, evidence
rotation, worker command or scheduler operation occurs between comparison and
mutation. The synchronous `&mut Database` call retains one execution owner.

The caller cannot supply a report, expected bound, anchor, or cached permit.
DML and maintenance may invalidate a previously observed public Phase 33
report; admitted apply always recomputes. ANALYZE statistics are not consulted
as resource authority. Inspection scans no source rows, so the successful path
performs only the existing backfill/source traversal. A rejected Snapshot LSM
never flushes. An admitted Snapshot follows the ordinary build, including its
one existing flush call; an empty MemTable produces no extra SSTable.

No admission rejection allocates an IndexId/ProjectionId, writes NBPC or an artifact,
changes G/schema, storage files, stream, evidence, calibration or scheduler
state. Once mutation begins, the existing allocator, writer, WAL, coordinator,
publication and recovery contracts apply unchanged. Passing admission does not
guarantee successful mutation.

## Typed errors

`PhysicalDesignMutationAdmissionError` distinguishes:

- `RequiredBoundNotProven { dimension }`;
- `LimitExceeded { dimension, conservative_bound, maximum }`;
- `Inspection(Box<PhysicalDesignMutationWorkInspectionError>)`.

The additive `PhysicalIndexDesignAdmissionApplyError` and
`PhysicalColumnarDesignAdmissionApplyError` each contain typed boxed `Apply`
and `Admission` variants. Existing Core apply enums are unchanged. `Display`
and `Error::source()` retain domain errors down through inspection, Database
and storage errors; no string parsing decides correctness.

Server uses `Admission(Box<PhysicalDesignMutationAdmissionError>)` directly in
its existing public control error enums. At that boundary the additive Core
wrapper is unpacked: its original `Apply` error remains the existing Server
`Apply` variant. This deliberately reuses the established uncertainty classifier
and preserves the unjournaled explicit Projection Catalog `RecoveryRequired`
priority without duplicating it. Adding a Server enum variant requires downstream
exhaustive match updates, but old Core error enums and apply signatures remain.

## One worker command and NBMR

`ServerPhysicalDesignControlHandle::apply_index_with_admission` and
`apply_columnar_with_admission` carry the immutable policy in one typed
programmatic apply command. Existing `apply_index` and `apply_columnar` explicitly
carry no admission policy. There is no startup admission builder/configuration,
shared budget manager, quota, token, proposal extension, or second Database owner.

With receipts configured the flow remains:

```text
durable NBMR Begin
  -> exact Server runtime provenance
  -> Columnar placement/mode/occupancy checks when applicable
  -> shared Core preflight
  -> fresh inspection and component admission if mutation is needed
  -> existing mutation authority if admitted
  -> durable coarse Outcome
```

Begin durability/capacity failure enters neither admission nor Core mutation.
An admission rejection, including an inspection I/O/format error, is definitely
pre-mutation and writes `Rejected`. It does not set the journal recovery gate.
Created and no-op outcomes retain the existing receipt tags. NBMR stores no
policy, bound, dimension or budget. The caller diagnoses admission through the
typed programmatic error, while the journal remains coarse operational memory.

Core mutation ambiguity and Outcome durability failure retain their existing
receipt-aware uncertainty/recovery behavior. Without a journal, generic ambiguous
apply failures remain typed unjournaled uncertainty; explicit Columnar catalog
recovery remains a typed reopen requirement. There is no compensating rollback,
automatic retry, constraint relaxation, maintenance, stream enablement, or
evidence refresh. Journal locking, active inode ownership, migration and
reconciliation are unchanged.

## Frozen contracts and next phase

Admission is programmatic-only. NBOP operator apply continues its existing
unadmitted approved flow. The only operator production adjustment maps the new,
programmatic-only error variants to existing `Internal` behavior where exhaustive
matching requires it; no new error code or operation exists.

Manifest v10 rejects hypothetical admission fields. NBOP stays v5, NBMR current
write v3, Native Protocol v2 and Inspection JSON v7. PostgreSQL wire, CLI and
daemon options, SDK Schema Spec and generated outputs are unchanged. Canonical
Schema, Schema Catalog/Mutation Journal, Coordinator, Heap, BTree/Index Catalog,
LSM manifest/SSTable/WAL, NBPC/NBPM, NBCM/NBCS/NBCD, Change Stream and Database WAL
formats are unchanged. No storage production change or dependency was needed.

Phase 35 should define separately versioned deployment/operator externalization
of these explicit component policies: ownership, allowed constraints, caller
approval and typed diagnostics. It should preserve same-command freshness,
no-op precedence and Begin ordering, and must not reinterpret the currently
unproven source/output dimensions as whole-mutation bounds.

Phase 36 later supplies stronger evidence to these unchanged six dimensions:
initial Columnar `OutputWriteBytes` and nonempty Snapshot-LSM
`SourceReadBytes` become bounded. Comparison order, no-op precedence,
same-command freshness and independent-component semantics do not change.

Phase 37 later supplies bounded Heap Index participant `OutputWriteBytes`
through the same dimension and ordering. It does not widen policy into a total
or Coordinator-inclusive budget.

## Regression coverage and validation

Policy tests cover constructor boundaries, deterministic first failure, zero
prerequisites and independent comparison without hidden summation. Core tests
cover Heap Index and both Columnar modes, all LSM mode/MemTable combinations,
strict/equal component limits, unproven dimensions, source growth after ANALYZE,
retained-report invalidation, rejection purity, no-op inspection bypass, error
precedence and typed source chains. Existing storage production-path counters
verify no preflight scan, one ordinary successful traversal and ordinary flush
counts. No test-only storage extension was required.

Server tests exercise the public channel and production forwarding function,
assert exactly one worker command, persist and reopen rejected/created/no-op
receipts, classify real malformed-geometry inspection errors, and inject Begin,
Outcome and post-mutation failures. Native and PostgreSQL TCP tests exercise
both admitted mutations and subsequent normal client queries. Frozen-contract
regressions reject new Manifest fields and NBOP policy/operation/error schemas.

The required validation matrix includes:

```sh
cargo fmt --all -- --check
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo +1.85.0 check --workspace --all-targets
cargo +1.85.0 check -p netbadb-sdk --no-default-features --features remote
cargo check -p netbadb-sdk --no-default-features
cargo check -p netbadb-sdk --no-default-features --features remote
cargo check -p netbadb-sdk --all-features
cargo test -p netbadb-sdk --all-features
./scripts/check-generated-sdk.sh
./scripts/test-go-sdk.sh
PSQL=/opt/local/lib/pgsql/bin/psql DYLD_LIBRARY_PATH=/opt/local/lib/icu/lib \
    python3 scripts/test-postgresql-psql.py
cargo test -p netbadb-core physical_design
cargo test -p netbadb-server admission
git diff --check
```

From `sdk/go`: `test -z "$(gofmt -l .)"`, `go test ./...`, and `go vet ./...`.
Full workspace validation includes all storage proofs, NBMR lock/handoff and
uncertainty, Native TCP, PostgreSQL Extended Query, daemon readiness/signals,
operator lifecycle and Phase 25/29/32 operator apply/receipt regressions.

The final matrix passed on Rust 1.97.1 and MSRV 1.85.0. The full workspace run
passed **1,928 tests, zero failures**, with three existing explicitly manual
tests ignored (one cost probe and two fuzz-corpus generators). This includes
679 Core, 101 executor, 281 Server, 484 storage unit tests, 13 PostgreSQL TCP,
28 Native TCP and six daemon deployment tests. All fifteen new Phase 34 tests
passed; looped cases cover the engine/mode/dimension and journal matrices.
Rust SDK feature checks/tests, generated SDK checks, Go formatting/test/vet and
Go–Rust interoperability, and real psql 17.11 also passed. The final diff and
local documentation links were checked.

Work started from local `main` at
`bff3f90b2dba40c287583d9f5bc2a6e345aa24b2`; freshly fetched `origin/main` was
`b944cfe8c582e59bb0db85ffb317be768f7af8aa`. The local Columnar recovery fix was
retained. The integration fetch confirmed no intervening remote advance.
No additional production correctness fix, persistent change, storage hook,
dependency, or deferred implementation stub was introduced by Phase 34.

Validation used compact build profiles (`CARGO_INCREMENTAL=0`,
`CARGO_PROFILE_DEV_DEBUG=0`, `CARGO_PROFILE_TEST_DEBUG=0`) and
`RUST_TEST_THREADS=4`. PostgreSQL used `/opt/local/lib/pgsql` with
`DYLD_LIBRARY_PATH=/opt/local/lib/icu/lib`. These affect local validation
resources, not database behavior or the committed toolchain.
