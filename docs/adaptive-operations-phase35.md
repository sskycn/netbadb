# Adaptive Operations Phase 35 — Operator component mutation admission

Phase 35 exposes Phase 34 admission to the local operator while keeping budget
authority with the deployment. Manifest v11 and NBOP v6 are the sole current
contracts; v10/v5 are historical and rejected. V5 public Rust DTOs are frozen
unchanged in `operator_v5.rs`.
The exact external schemas and migration instructions are in
[Manifest v11](server-manifest-v11.md) and [NBOP v6](server-operator-protocol-v6.md).

```text
deployment config -> worker-owned immutable policy
  -> explicit operator approval -> existing Core preflight
  -> fresh Phase33 component inspection -> Phase34 admission
  -> existing mutation authority -> coarse NBMR Outcome
```

A present `operator` must explicitly configure Index, Snapshot Columnar and
Incremental Columnar admission. Each mode is either `unadmitted`, selecting the
old apply API, or `component_limits`, selecting the admitted API with exactly the
validated deployment policy. The latter requires six tagged constraints and at
least one constrained component. Manifest parsing delegates policy validity to
Core and retains its typed error source. Disabled permissions and disallowed
Columnar modes require `unadmitted`. There is no default, migration shortcut,
generic Physical Design admission config, or operator admission builder.

Native and PostgreSQL startup copy the same resolved immutable modes to their
sole Database worker and the operator status projection. The listener never
forwards a policy. Existing approved commands retain only logical target,
runtime match and evidence epoch data. One NBOP apply remains one worker command.
Embedded Core and programmatic Server Phase34 callers retain independent per-call
policy authority; Manifest limits do not constrain those APIs.

The worker preserves exact durable retry before runtime provenance, then existing
evidence, placement/mode, current coverage and proposal semantics. Only after a
fresh proposal does it select the exact Index/Snapshot/Incremental policy slot.
Core retains inspection, comparison, deterministic dimension ordering and mutation
authority. AlreadyApplied and AlreadyCovered bypass admission. Restarting with
stricter limits preserves durable exact retry but cannot revive a stale approval
for a missing target. Runtime tokens remain random per daemon lifetime.

Constrained NotProven rejects even at u64::MAX; equality passes. Each component
is independent. Unconstrained means outside the policy, never zero or proven
safe. In particular, Snapshot LSM can admit only its prerequisite-write component
while source work/read and output writes remain unproven: **partial component
admission**, not whole-mutation bounding. No total, CPU, memory, filesystem
free-space, cumulative quota or automatic design behavior is introduced.

NBOP v6 retains the 12-byte header and 65,536-byte payload cap. Apply JSON and CLI
syntax accept no budget, policy, inspection, expected bound or cached permit.
Strict decoding rejects attempts to add them. Status presents each actual mode
and all six configured constraints without inspecting current storage. The CLI
prints component policies and typed rejection details, optionally correlates a
returned receipt, and never relaxes policy, retries, refreshes approval or looks
up a receipt automatically. `netbadb inspect` validates configuration but does
not evaluate mutation-work bounds.

Admission errors use one stable code,
`physical_design_mutation_admission_rejected`, and one typed nullable diagnostic:
RequiredBoundNotProven, LimitExceeded (dimension/bound/maximum), or InspectionFailed.
Inspection errors never serialize a private Database/Storage display or path.
Non-admission errors retain `admission: null`.

With NBMR, durable Begin remains before semantic processing and admission.
Admission/inspection rejection writes terminal `Rejected`, returns the same
scoped reference, and does not gate recovery. Without NBMR, receipt is null.
NBMR stores no policy, dimension, bound or inspection. Outcome durability failure,
post-mutation uncertainty, unjournaled ambiguity, whole-response loss, explicit
Columnar recovery priority and active journal inode ownership remain unchanged.

## Compatibility and verification

Only Manifest (11) and NBOP (6) versions change. NBMR v3, Native Protocol v2,
PostgreSQL wire, Inspection JSON v7, SDK Schema Spec and all database persistent
formats remain unchanged. Core and Storage production code is unchanged.

Tests cover strict/no-default manifest modes and every component, exact document
example parsing, disabled-mode consistency, frozen v5 shapes, budget injection,
v6 status/error round trips, one-command NBOP apply, Heap and LSM mode/component
boundaries, no-op precedence, rejection purity, inspection privacy, coarse receipt
correlation, programmatic authority and mutation uncertainty. Real Native/PG
daemons exercise rejection, query reuse, admitted Index/Snapshot/Incremental,
healthy pre-enabled stream, stale restart approval, stricter-policy exact retry,
receipt lookup and SIGTERM cleanup. Existing tests retain Extended Query,
SIGINT, receipt migration/locking/reconciliation and response-loss coverage.

The PG daemon test uses the normal wire fixture by default. Set
`NETBADB_TEST_PSQL=/opt/local/lib/pgsql/bin/psql` to run its workload through real
psql, with the required ICU library path. No external service is needed.

```sh
cargo fmt --all -- --check
cargo +1.85.0 check --workspace --all-targets
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo check -p netbadb-sdk --no-default-features
cargo check -p netbadb-sdk --no-default-features --features remote
cargo check -p netbadb-sdk --all-features
cargo +1.85.0 check -p netbadb-sdk --no-default-features --features remote
cargo test -p netbadb-sdk --all-features
./scripts/check-generated-sdk.sh
./scripts/test-go-sdk.sh
PSQL=/opt/local/lib/pgsql/bin/psql DYLD_LIBRARY_PATH=/opt/local/lib/icu/lib \
    python3 scripts/test-postgresql-psql.py
NETBADB_TEST_PSQL=/opt/local/lib/pgsql/bin/psql DYLD_LIBRARY_PATH=/opt/local/lib/icu/lib \
    cargo test -p netbadbd phase35_postgres_daemon_admission_and_restart
# From sdk/go:
test -z "$(gofmt -l .)"
go test ./...
go vet ./...
# From repository root:
git diff --check
```

All commands above passed on 2026-09-19 with the pinned Rust 1.97.1 toolchain,
Rust 1.85.0 for MSRV checks, Go 1.26.4, and `/opt/local/lib/pgsql/bin/psql` 17.11
using `/opt/local/lib/icu/lib`. The validation environment disabled incremental
compilation and dev/test debug information and used four Rust test threads;
these were process environment settings, not repository configuration changes.

| Validation | Recorded result |
| --- | --- |
| Workspace fmt, check, Clippy with `-D warnings`, MSRV all-target check | Passed |
| `cargo test --workspace` | 1,942 passed, zero failed, three existing ignored tests |
| Final complete Server rerun after strengthening the growth/retry assertion | 291 unit, 13 PostgreSQL integration, 28 Native TCP integration tests passed |
| Rust SDK no-default, remote, all-features, MSRV remote, all-features tests | Passed |
| Generated SDK check; Go formatting, tests, vet and Go↔Rust interoperability | Passed |
| Real psql compatibility and Phase35 PG daemon admission/restart workload | Passed with psql 17.11 |
| Included Rust test files' explicit rustfmt check; changed Markdown links; diff whitespace | Passed |

The three existing ignored cases are one manual ALTER TYPE cost probe and two
explicit fuzz corpus generators. No tests were newly ignored or weakened.
The workspace run includes the real Native/PG daemon matrix, SIGINT/SIGTERM,
readiness/socket lifecycle, receipt migration/locking/reconciliation/uncertainty,
and all Core/Storage recovery regressions. There are no outstanding validation
or environment blockers.

Phase36 should begin with a separately justified storage proof for a currently
NotProven component (especially output writes or whole-mutation work), before
extending policy. Memory/CPU/free-space limits, cumulative quotas and automatic
design remain deferred. No blanket safety claim follows from partial admission.

Phase 36 later proves initial Columnar `output_write_bytes` and prospective
Snapshot-LSM `source_read_bytes` under the same Manifest v11/NBOP v6 contract.
An existing `AtMost` policy may therefore admit a build that this historical
phase reported as `RequiredBoundNotProven`; configured maxima are not widened.
Index output remains unproven.

## Implementation audit

The actual starting HEAD and fetched origin/main were both
`e524f71df03a22e9aaa1bee87e2c7a07f1692a18`. Work uses branch
`codex/adaptive-operations-phase35` and the requested implementation title
`feat: enforce operator physical design admission`. No temporary worktree was
needed because the original checkout was clean. Final commit and remote equality
are reported after integration, avoiding a self-referential commit hash here.
The pre-integration fetch confirmed that origin/main had not advanced from that
starting commit; no concurrent changes required reconciliation.

The public runtime mode is
`ServerOperatorPhysicalDesignMutationAdmission::{Unadmitted, ComponentLimits}`.
It has no Default. `ServerOperatorConfig` exposes separate read-only mode getters;
only the strict manifest constructor creates production operator configuration.
`ServerPhysicalDesignStartupConfig` groups the final advisor, placement, receipts
and immutable operator modes for the Native/PG workers. The internal all-unadmitted
constant serves absent-operator runtimes and explicit test fixtures only; a present
manifest operator never falls back to it. No policy builder can override Manifest.

| Worker request | Unadmitted mode | ComponentLimits mode |
| --- | --- | --- |
| Index | existing `apply_physical_index_design` | `apply_physical_index_design_with_admission` with Index policy |
| Snapshot | existing `apply_physical_columnar_design` | `apply_physical_columnar_design_with_admission` with Snapshot policy |
| Incremental | existing `apply_physical_columnar_design` | `apply_physical_columnar_design_with_admission` with Incremental policy |

These choices occur after existing proposal logic. Core production code and
Storage production code were not changed. In particular, Server introduces no
second NotProven decision, bound computation, dimension ordering, sum, allocation
or recovery authority. Programmatic with-admission APIs still carry their own
per-call policy in their existing command; unadmitted programmatic apply remains
independent of the operator modes.

V6 defines separate Constraint, Policy, Mode, Dimension and Rejection DTOs.
Core types are never serialized directly. The manifest's component fields are
flat under `mode: component_limits`; status deliberately uses the explicit wire
`policy` object. Index status has `admission`; Columnar status has independent
`snapshot_admission` and `incremental_admission`. Both preserve their established
enabled/mode/runtime-token fields. Status and recommendations do not acquire
inspection authority or issue preflight work.

The new NBOP tests traverse the real codec, listener, control channel and production
forwarder, asserting exactly one worker command and no second queued command.
Rejection tests cover both receipt configurations, equal and one-below Heap work
and read bounds, unproven output, LSM Incremental source-work rejection and SSTable
bytes, empty Snapshot zero prerequisites, and nonempty Snapshot source-read
NotProven plus each flush prerequisite boundary. Partial prerequisite-write
admission succeeds. Separate source-read and zero prerequisite-read constraints
also verify that source bytes are never charged against the prerequisite limit.
Core's existing independent-components/no-hidden-sum proof remains in the full
workspace matrix.

InspectionFailed fixtures corrupt current Heap geometry deterministically after
recording evidence, then restore it for cleanup. Their rejection has no path,
private error display, physical mutation, ID burn, evidence change or recovery
gate. Created IDs remain the first IDs after prior rejections. NBMR references
match exact journal incarnation and receipt ID; outcomes remain Rejected.
Successful applies and no-ops retain the same receipt protocol. Post-admission
mutation and Outcome durability failures retain uncertainty with null admission.
Existing whole-response loss and explicit unjournaled Columnar recovery tests
remain active under v6.

Same-runtime retries also follow a committed 512-row insertion that increases
both Heap source-work and source-read bounds beyond the original, unchanged
admission limits. Index, Snapshot and Incremental exact retries still return
AlreadyApplied, with and without NBMR.

Real daemon tests cover both transports, independent status policies, rejection
receipt lookup, ordinary queries afterward, all three admitted mutation domains,
already-enabled healthy Change Stream, a changed daemon token, stricter-policy
exact retry, and SIGTERM socket cleanup. The existing full suite also covers
Native TCP, PostgreSQL Extended Query transaction/ReadyForQuery/Sync handling,
SIGINT/readiness, NBMR migration/reconciliation/lock handoff and uncertain outcomes.
No new production evidence, scheduler, session or transaction-state path exists.

The only added dependency is the existing workspace Native client as a daemon
**dev-dependency**, used to exercise the actual Native daemon. An additional test
harness correction joins/stops its owned child daemon on a failing assertion,
preventing leaked test processes and locks. No extra production correctness fix,
new persistent format, or unfinished implementation stub was introduced.

The frozen format set includes Canonical Schema, Schema Catalog and Mutation
Journal, Coordinator, Heap, BTree/Index Catalog, LSM Manifest/SSTable/WAL,
NBPC/NBPM, NBCM/NBCS/NBCD, Change Stream and Database WAL. Inspection JSON stays
v7; SDK Schema Spec and generated SDK output are unchanged.

## Changed files

- [Cargo.lock](../Cargo.lock)
- [README.md](../README.md)
- [cmd/netbadb/README.md](../cmd/netbadb/README.md)
- [cmd/netbadb/src/lib.rs](../cmd/netbadb/src/lib.rs)
- [cmd/netbadb/src/operator_admission_tests.rs](../cmd/netbadb/src/operator_admission_tests.rs)
- [cmd/netbadb/tests/cli.rs](../cmd/netbadb/tests/cli.rs)
- [cmd/netbadbd/Cargo.toml](../cmd/netbadbd/Cargo.toml)
- [cmd/netbadbd/tests/deployment.rs](../cmd/netbadbd/tests/deployment.rs)
- [cmd/netbadbd/tests/support/operator_admission.rs](../cmd/netbadbd/tests/support/operator_admission.rs)
- [crates/netbadb-client/tests/integration.rs](../crates/netbadb-client/tests/integration.rs)
- [crates/netbadb-server/AGENTS.md](../crates/netbadb-server/AGENTS.md)
- [crates/netbadb-server/benches/global_boundary.rs](../crates/netbadb-server/benches/global_boundary.rs)
- [crates/netbadb-server/examples/go_sdk_fixture.rs](../crates/netbadb-server/examples/go_sdk_fixture.rs)
- [crates/netbadb-server/examples/postgres_driver_fixture.rs](../crates/netbadb-server/examples/postgres_driver_fixture.rs)
- [crates/netbadb-server/examples/sql_alter_table_fixture.rs](../crates/netbadb-server/examples/sql_alter_table_fixture.rs)
- [crates/netbadb-server/examples/sql_create_table_fixture.rs](../crates/netbadb-server/examples/sql_create_table_fixture.rs)
- [crates/netbadb-server/src/lib.rs](../crates/netbadb-server/src/lib.rs)
- [crates/netbadb-server/src/manifest.rs](../crates/netbadb-server/src/manifest.rs)
- [crates/netbadb-server/src/manifest_admission_tests.rs](../crates/netbadb-server/src/manifest_admission_tests.rs)
- [crates/netbadb-server/src/native_adaptive_feedback_tests.rs](../crates/netbadb-server/src/native_adaptive_feedback_tests.rs)
- [crates/netbadb-server/src/operator.rs](../crates/netbadb-server/src/operator.rs)
- [crates/netbadb-server/src/operator_admission_tests.rs](../crates/netbadb-server/src/operator_admission_tests.rs)
- [crates/netbadb-server/src/operator_v5.rs](../crates/netbadb-server/src/operator_v5.rs)
- [crates/netbadb-server/src/physical_design.rs](../crates/netbadb-server/src/physical_design.rs)
- [crates/netbadb-server/src/physical_design_operator_admission_tests.rs](../crates/netbadb-server/src/physical_design_operator_admission_tests.rs)
- [crates/netbadb-server/src/postgres.rs](../crates/netbadb-server/src/postgres.rs)
- [crates/netbadb-server/src/postgres_adaptive_feedback_tests.rs](../crates/netbadb-server/src/postgres_adaptive_feedback_tests.rs)
- [crates/netbadb-server/src/runtime.rs](../crates/netbadb-server/src/runtime.rs)
- [crates/netbadb-server/tests/postgres.rs](../crates/netbadb-server/tests/postgres.rs)
- [crates/netbadb-server/tests/tcp.rs](../crates/netbadb-server/tests/tcp.rs)
- [docs/adaptive-operations-phase35.md](adaptive-operations-phase35.md)
- [docs/architecture.md](architecture.md)
- [docs/roadmap.md](roadmap.md)
- [docs/server-manifest-v10.md](server-manifest-v10.md)
- [docs/server-manifest-v11.md](server-manifest-v11.md)
- [docs/server-operator-protocol-v5.md](server-operator-protocol-v5.md)
- [docs/server-operator-protocol-v6.md](server-operator-protocol-v6.md)
