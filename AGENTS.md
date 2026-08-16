# NetbaDB engineering rules

This file contains repository-wide rules for AI coding agents and contributors.
More-specific `AGENTS.md` files add rules for their directory subtree.

Scoped rules currently live in:

- `crates/AGENTS.md` for Rust libraries and compiler/execution layers;
- `crates/netbadb-index/AGENTS.md` for typed index ordering and node codecs;
- `crates/netbadb-storage/AGENTS.md` for persistent formats, pages, heap, and index storage;
- `crates/netbadb-protocol/AGENTS.md` for the versioned wire contract;
- `crates/netbadb-client/AGENTS.md` for synchronous remote-client lifecycle;
- `crates/netbadb-server/AGENTS.md` for synchronous session lifecycle;
- `sdk/AGENTS.md` for Rust, Go, and future language SDKs.

Do not create a directory or crate only to hold an instruction file. Add server,
protocol, transaction, WAL, recovery, or index-specific rules when those
subsystems actually exist.

## 1. Requirement language

The following words have deliberate meanings:

- **MUST / MUST NOT** — required for correctness, architecture, or merge
  readiness. A change that violates one is incomplete.
- **SHOULD / SHOULD NOT** — the default. A justified exception is acceptable
  when the tradeoff is documented in code, an ADR, or the final report.
- **MAY** — optional guidance.

Unqualified imperative sentences such as “Keep the core synchronous” are MUST
rules. Examples explain intent; they do not require speculative implementation.

When rules conflict, follow the most specific applicable instruction file.
User requirements define task scope but do not silently weaken correctness,
data-safety, or compatibility requirements.

## 2. Mission and priorities

NetbaDB is a strongly typed relational database implemented primarily in Rust.
Application languages are frontends and clients; they are not part of the
database's persistent meaning.

The architecture is:

```text
Application Language
        ↓
Language Frontend / SDK
        ↓
Canonical Schema IR
        ↓
Parser → Resolver → Type Checker → Typed HIR
        ↓
Typed Relational IR
        ↓
Optimizer / Planner → Physical Plan
        ↓
Executor
        ↓
Transaction Layer
        ↓
Storage Engine
```

Use this decision order:

```text
correctness
  > explicit invariants
  > type safety
  > architecture clarity
  > testability
  > observability
  > measured performance
  > convenience
```

Prefer the smallest coherent vertical slice that works end to end. Do not
create empty crates, speculative trait hierarchies, placeholder APIs, or broad
frameworks to make an architecture diagram appear complete.

## 3. Dependency direction

In every dependency diagram in this repository:

```text
A -> B means crate or layer A depends on B.
```

The intended dependency direction is:

```text
schema -> types
codegen -> schema + types
inspect -> schema + types
hir -> parser + schema + types
rel -> types
compiler -> hir + parser + rel + schema + types
planner -> index + rel + types
index -> types
storage -> index + schema + types
executor -> planner + rel + storage + types
core -> compiler + inspect + planner + rel + executor + storage + schema + types
protocol -> types
client -> protocol + schema + types
server -> core + protocol + schema + types
netbadbd -> server
Rust SDK embedded -> core + inspect + schema + types
Rust SDK remote -> client + schema + types
```

Lower-level crates MUST NOT depend on higher-level policy. In particular:

```text
storage -X-> planner / executor / SDK
executor -X-> compiler / SDK
compiler -X-> executor / server
types / schema / rel -X-> core / server / SDK
```

`netbadb-core` MAY compose lower-level crates. `netbadb-server` MAY depend on
core and protocol; neither may depend back on server. If two layers need a primitive, move it to the lowest appropriate common
crate instead of creating a cycle, global state, or incidental dynamic
dispatch.

## 4. Cross-language and type invariants

Canonical Schema IR MUST remain language-independent. Never make these database
semantics:

- Rust `TypeId`, crate paths, enum discriminants, pointers, `usize`, or struct
  layout;
- Go reflection identities or Go-only tags;
- JSON objects used as the primary schema, HIR, relational, physical, or
  execution representation.

Application frontends convert their types into explicit canonical concepts such
as stable IDs, physical types, semantic types, nullability, constraints, and
relationships.

Semantically different identifiers MUST use distinct newtypes. Physical type
and semantic type MUST remain distinct. `UserId` and `TeamId` may both use
`u64`, but they are not interchangeable.

Database `NULL` is a database value, not generic “missing metadata.” Its meaning
MUST remain explicit through schema, typing, expression evaluation, filtering,
encoding, and protocol boundaries.

AST, typed HIR, logical relational IR, and physical plans MUST remain distinct
typed stages. Later stages should encode stronger invariants than earlier ones.
Do not execute raw syntax nodes or replace core IR with stringly typed maps.

## 5. Rust baseline

- Development and primary CI use the exact toolchain in
  `rust-toolchain.toml`.
- The workspace MSRV is the exact `workspace.package.rust-version` in
  `Cargo.toml` and is checked by a separate CI job.
- New crates MUST inherit workspace edition, Rust version, license, and lints.
- Nightly features MUST NOT be introduced without an explicit architectural
  requirement and documentation.
- Toolchain, edition, and MSRV changes MUST be deliberate, documented changes;
  do not bundle them into unrelated work.

Pinning the development toolchain prevents a new Clippy release from breaking
unchanged code under `-D warnings`. Passing primary CI does not permit use of
language or library features newer than the MSRV.

## 6. Core implementation rules

### Ownership and concurrency

- Prefer owned stable IDs across subsystem boundaries and short-lived borrows
  within a subsystem.
- Long-lived page-, frame-, or tuple-backed references SHOULD NOT spread into
  planner, executor, transaction, or catalog code.
- Do not use `.clone()`, `'static`, `Box::leak`, `Arc<Mutex<_>>`, `RefCell`, or
  dynamic dispatch merely to silence ownership errors. They MAY be used when
  the ownership or polymorphism requirement is real and understandable.
- Avoid global mutable state. Lock ownership and ordering MUST be explicit.
- Never hold a synchronous lock across `.await`.

The database core is synchronous by default. Parser, resolver, type checker,
optimizer, planner, executor core, page, buffer, storage, index, transaction,
WAL, and recovery SHOULD remain synchronous. Async belongs at external network
and remote-client boundaries and MUST NOT force Tokio types through the core.

### Errors and unsafe code

Library APIs MUST expose domain-specific errors rather than public
`anyhow::Error`. Malformed queries, files, protocol messages, configuration, and
normal operational failures MUST return errors rather than panic.

Production core code SHOULD NOT routinely use `unwrap`, `expect`, `panic`,
`unreachable`, `todo`, or `unimplemented`. Tests MAY use `unwrap` and `expect`
for readability. A panic is acceptable only for a genuine internal programming
invariant, never for untrusted input.

Safe Rust is the default. `unsafe` MAY be used for a concrete systems need such
as mmap, FFI, SIMD, alignment, or audited zero-copy code. It MUST be localized
behind a safe API. Every unsafe block MUST have a nearby `// SAFETY:` comment
that states the invariant making it sound. Higher-level compiler, planner,
executor, and SDK crates SHOULD contain no unsafe code.

### Dependencies and abstractions

- Prefer the standard library, then mature focused crates.
- Add a dependency only when its capability, maintenance cost, features, and
  architectural layer are justified.
- Shared dependency versions SHOULD be declared at workspace level.
- Do not add a trait, generic parameter, macro, or `Box<dyn Trait>` for a
  hypothetical future implementation. Add abstractions when concrete behavior
  demonstrates the need.
- Establish a correct manual schema API before adding derive macros.
- Do not introduce compatibility layers for APIs or formats that have not been
  declared stable.

### Public APIs, naming, and documentation

- Keep public APIs smaller than internal APIs. Use `pub(crate)` or private
  visibility when external callers do not need a symbol.
- Do not expose storage representation through high-level APIs. Explicit
  unstable inspection/debug APIs are acceptable when clearly marked.
- Prefer domain names such as `LogicalPlan`, `TableId`, and `BufferPool` over
  vague `Helper`, `Util`, `Data`, or `Processor` names. `PageManager` and
  `BufferManager` are acceptable because they name precise domain roles.
- Comments SHOULD explain invariants, tradeoffs, safety, and non-obvious
  behavior rather than restating code.
- Public APIs with non-obvious contracts and all persistent layouts MUST be
  documented.

## 7. Development workflow

Before changing code:

1. Read this file and every applicable scoped `AGENTS.md`.
2. Read the relevant README and architecture documentation.
3. Inspect the implementation, nearby tests, manifests, and current Git diff.
4. Determine the intended behavior when documentation and code disagree.
5. Make the smallest coherent change that fully handles the request.

During implementation:

- Preserve unrelated user changes and generated files.
- Do not perform unrelated refactors, mass renames, or formatting changes.
- Do not stop at a plan when implementation was requested.
- TODO comments are acceptable only when current behavior is correct, omitted
  work is outside scope, and the missing requirement is explained. Reachable
  `todo!()` and `unimplemented!()` are not acceptable production behavior.
- Tests MUST be deterministic and SHOULD live close to the invariant they
  protect. Core tests MUST NOT depend on external services or sleep-based
  timing.
- Optimize only after measuring a meaningful database operation. Do not trade
  invariants for an unmeasured optimization.

## 8. Validation matrix

Validation is proportional to the files and behavior changed. “Relevant” below
means the command exercises every affected crate and public boundary.

| Change class | Required validation |
| --- | --- |
| Rust source or manifest | `cargo fmt --all -- --check`; `cargo check --workspace --all-targets`; `cargo clippy --workspace --all-targets -- -D warnings`; `cargo test --workspace` |
| Persistent format, page, heap, WAL, or recovery | All Rust validation plus round-trip, close/reopen, malformed/truncated input, and affected boundary tests |
| Toolchain, workspace, Makefile, or Rust CI | Run the commands whose definitions changed; verify both primary toolchain and MSRV jobs when applicable |
| Executable Go SDK code | `gofmt` on changed Go files and `go test ./...` from the relevant module |
| Protocol or generated code | Language tests plus protocol compatibility or generation checks defined by that subsystem |
| Documentation/instructions only | Inspect `git diff --check`; verify changed links, paths, and commands. Full compiler/test validation is not required unless executable examples or build instructions changed |

Narrow tests MAY be used while iterating, but final validation MUST follow this
matrix whenever the environment permits. If an environment prevents a required
command, report the exact command, failure, and validation that did run.

Never claim a command passed unless it actually ran successfully. Do not remove
tests, weaken assertions, or add blanket lint suppressions to make CI green.

## 9. Repository and Git safety

MUST NOT be committed unless explicitly required:

```text
target/
temporary databases
benchmark output
debug dumps
editor metadata
credentials or secrets
unreviewed generated local files
```

Before and after substantial work, inspect `git status` and `git diff`. Do not
discard changes you did not create. Do not use destructive Git commands or
create commits unless the user explicitly requests them.

Potentially destructive operations MUST resolve exact targets first and SHOULD
prefer recoverable actions. Planning, inspection, and explanation MUST remain
distinct from mutation.

## 10. Definition of done

A change is complete when:

- requested behavior is implemented without reachable placeholders;
- dependency direction and language-independent boundaries remain intact;
- new invariants are explicit and relevant positive/negative tests exist;
- behavior, architecture, and roadmap documentation agree with code;
- validation required by the matrix passes or exact environmental blockers are
  reported;
- no unrelated or temporary changes remain;
- the final diff has been reviewed.

For a non-trivial change, report what changed, why, affected modules, tests,
commands actually run, and intentionally deferred work. Do not describe a stub
or roadmap item as implemented.

## 11. Final design check

Before accepting an architectural change, ask:

```text
Does it preserve strong typing and explicit NULL semantics?
Does it keep Canonical Schema IR language-independent?
Does it make invalid states harder to represent?
Does it keep A -> B dependency direction clean?
Does it preserve synchronous embedded use?
Does it avoid coupling core to Tokio, Go, SDK, or network concerns?
Is persistent representation explicit and safely decoded?
Can behavior be tested deterministically?
Is this simpler than the justified alternatives?
```

If several answers are no, reconsider the design before adding complexity.
