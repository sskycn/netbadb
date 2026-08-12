# AGENTS.md

This file defines the repository-wide engineering rules for AI coding agents working on NetbaDB.

These rules apply to the entire repository unless a more specific `AGENTS.md` exists in a subdirectory.

The goal is not merely to make code compile. Changes must preserve NetbaDB's architectural direction, type safety, storage correctness, and long-term maintainability.

---

# 1. Project Mission

NetbaDB is a strongly typed relational database implemented primarily in Rust.

The intended architecture is:

```text
Application Language
        │
        ▼
Language Frontend / SDK
        │
        ▼
Canonical Schema IR
        │
        ▼
Compiler
        │
        ├── Parser
        ├── Resolver
        ├── Type Checker
        └── Typed HIR
        │
        ▼
Typed Relational IR
        │
        ▼
Optimizer / Planner
        │
        ▼
Physical Plan
        │
        ▼
Executor
        │
        ▼
Transaction Layer
        │
        ▼
Storage Engine
        │
        ├── Page Manager
        ├── Buffer Manager
        ├── Heap
        ├── Index
        ├── WAL
        └── Recovery
```

Rust is the native database implementation language.

Rust must NOT become part of the persistent database model itself.

Canonical schemas, semantic types, query semantics, storage formats, and protocol formats must remain language-independent.

Future clients may include:

* Rust embedded API
* Rust client
* Go client
* generated SDKs
* protocol clients
* other languages

Do not introduce architecture that prevents this.

---

# 2. Core Engineering Priorities

When making design decisions, use this priority order:

```text
Correctness
    >
Explicit invariants
    >
Type safety
    >
Architecture clarity
    >
Testability
    >
Observability
    >
Performance
    >
Convenience
```

Performance matters because NetbaDB is a database, but correctness comes first.

Do not trade correctness for speculative performance.

Do not trade understandable code for clever Rust.

Prefer:

```text
simple
explicit
typed
small
testable
measurable
```

Avoid:

```text
clever
implicit
stringly typed
over-generic
macro-heavy
framework-driven
prematurely optimized
```

---

# 3. Agent Working Rules

Before modifying code:

1. Read this file.
2. Read the relevant README and architecture documentation.
3. Inspect the existing implementation.
4. Inspect nearby tests.
5. Inspect workspace configuration and crate dependencies.
6. Understand existing conventions before introducing new ones.

Do not infer architecture only from filenames.

Do not assume documentation is more accurate than code.

When documentation and implementation disagree, determine the intended design before changing either.

Prefer the smallest coherent change that completely solves the task.

Do not perform unrelated refactors.

Do not rewrite working modules merely because another style is preferred.

Do not leave the repository in an intermediate migration state unless explicitly requested.

Do not stop after writing a plan when the task requests implementation.

---

# 4. Vertical Development

NetbaDB prefers vertical slices over large collections of disconnected abstractions.

Prefer:

```text
Syntax
  ↓
Parsing
  ↓
Resolution
  ↓
Typing
  ↓
Logical Plan
  ↓
Physical Plan
  ↓
Execution
  ↓
Storage
  ↓
Tests
```

for one small feature.

Avoid implementing ten incomplete subsystems simultaneously.

A small feature that works end-to-end is more valuable than a large collection of placeholders.

Do not create crates solely because they appear in an architecture diagram.

Create a crate when there is a real architectural boundary.

---

# 5. Rust Toolchain

Use stable Rust unless the repository explicitly requires otherwise.

For new crates, use Rust 2024 edition unless the workspace specifies another edition.

Respect:

```text
rust-toolchain.toml
Cargo.toml rust-version
workspace.package
workspace.dependencies
workspace.lints
```

Do not change the MSRV, Rust edition, or pinned toolchain as part of an unrelated task.

Do not introduce nightly-only features without explicit justification.

New workspace crates should inherit common package configuration where practical.

Prefer centralized workspace dependencies rather than independently versioning the same dependency in many crates.

---

# 6. Workspace Architecture

Keep crate dependencies directional.

Conceptually, dependencies should flow from higher-level systems toward lower-level primitives.

Typical boundaries are:

```text
netbadb-types
        ↑
netbadb-schema
        ↑
netbadb-catalog
        ↑
compiler / hir / rel
        ↑
optimizer / planner
        ↑
executor
        ↑
netbadb-core
```

Storage has its own lower-level hierarchy:

```text
types
  ↑
page
  ↑
buffer
  ↑
storage
  ↑
index / txn / wal / recovery
```

Composition layers may depend on several lower layers.

Lower-level crates must not depend on higher-level policy.

Examples of forbidden dependency directions:

```text
page      -> executor
storage   -> planner
buffer    -> compiler
txn       -> server
executor  -> SDK
compiler  -> server
```

`netbadb-core` may act as a composition/facade layer.

`netbadb-server` may depend on the core.

SDK crates must not become dependencies of the database core.

If two layers need a shared primitive, move the primitive to the lowest appropriate common crate rather than creating a circular dependency.

Never solve dependency cycles with global state or dynamic dispatch unless the architecture genuinely requires it.

---

# 7. Strong Types Are a Core Feature

Do not represent semantically different identifiers using interchangeable primitive types.

Prefer:

```rust
pub struct DatabaseId(u64);
pub struct TableId(u64);
pub struct ColumnId(u32);
pub struct IndexId(u64);
pub struct PageId(u64);
pub struct FrameId(u32);
pub struct TxnId(u64);
```

over:

```rust
type TableId = u64;
type PageId = u64;
```

when the values represent distinct semantic identities.

The database must distinguish:

```text
Physical Type
```

from:

```text
Semantic Type
```

For example:

```text
UserId
TeamId
```

may both physically use `u64`, while remaining semantically incompatible.

Do not erase semantic types prematurely.

Do not encode important states as strings when an enum or newtype can represent them.

Prefer:

```rust
enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}
```

over:

```rust
String
```

containing `"inner"`.

Make invalid states difficult to represent.

---

# 8. Database NULL Is Explicit

Do not confuse:

* missing metadata
* missing configuration
* absent Rust value
* SQL/database `NULL`

These are different concepts.

Represent database nullability explicitly in schema and execution semantics.

Do not casually use `Option<T>` throughout internal APIs without determining what `None` means.

NULL semantics must remain consistent through:

```text
Schema
Type Checker
Expression Evaluation
Comparison
Filtering
Aggregation
Encoding
Protocol
```

---

# 9. AST, HIR, and Relational IR

Compiler structures must use strongly typed Rust data structures.

Prefer enums and structs.

Example:

```rust
pub enum LogicalPlan {
    Scan(Scan),
    Filter(Filter),
    Project(Project),
    Join(Join),
    Aggregate(Aggregate),
    Sort(Sort),
    Limit(Limit),
}
```

Do not use generic structures such as:

```rust
HashMap<String, serde_json::Value>
```

as the primary representation of:

* AST
* HIR
* logical plans
* physical plans
* expressions
* schema objects
* optimizer nodes

Do not execute directly from raw syntax nodes.

Keep compiler stages conceptually distinct:

```text
Source
  ↓
AST
  ↓
Name Resolution
  ↓
Typed HIR
  ↓
Logical Relational IR
  ↓
Optimized Logical Plan
  ↓
Physical Plan
```

Later stages should have stronger invariants than earlier stages.

Do not repeatedly revalidate invariants that a previous typed phase should guarantee unless crossing a trust boundary.

---

# 10. Canonical Schema IR

Canonical Schema IR must remain language-independent.

Never make Rust-specific concepts part of the canonical schema simply because the native implementation uses Rust.

Do not persist things such as:

```text
Rust TypeId
Rust enum discriminants
pointer addresses
usize
Rust struct layout
crate paths used as identity
compiler implementation details
```

as database semantics.

Schema identity should use explicit stable identifiers and explicit encoding.

Application language frontends convert their language representation into Canonical Schema IR.

The database core consumes Canonical Schema IR.

This boundary must remain clean.

---

# 11. Persistent Formats

Never treat Rust's in-memory representation as a stable database format.

Do NOT persist structs by:

* `transmute`
* raw pointer casting
* dumping arbitrary struct bytes
* assuming `repr(Rust)` layout
* assuming enum discriminant layout
* assuming host endianness
* assuming `usize` width

Persistent structures must define explicit encoding.

Specify intentionally:

* byte order
* integer width
* length encoding
* page boundaries
* versioning
* checksums where appropriate
* optional fields
* corruption handling

All decode paths must validate bounds before accessing data.

Malformed database files must return controlled errors rather than trigger undefined behavior or arbitrary panics.

Checked arithmetic should be used when corrupted or external data can influence offsets, lengths, or sizes.

---

# 12. Page and Storage Code

Storage code requires stronger correctness discipline than ordinary application code.

Page-level invariants must be explicit.

Examples include:

```text
page size
header size
slot count
free-space boundaries
offset bounds
record boundaries
page type
page identifier
checksum/version when present
```

Do not scatter page-layout magic numbers throughout the code.

Centralize layout constants and encoding rules.

Encoding and decoding should be symmetric and testable.

Every persistent structure should have round-trip tests where practical:

```text
value
  ↓
encode
  ↓
bytes
  ↓
decode
  ↓
equivalent value
```

Also test malformed and truncated data.

---

# 13. Buffer Manager Design

Do not allow references to pinned pages to escape arbitrarily through the system.

Prefer explicit handles, IDs, and short-lived guards.

Good architectural concepts include:

```text
PageId
FrameId
PageGuard
ReadPageGuard
WritePageGuard
```

when appropriate.

A guard may use RAII to manage pin/unpin or latch ownership, but its lifetime should remain local.

Avoid spreading complex lifetimes across:

```text
executor
planner
transaction
catalog
```

just because a storage page is borrowed.

Do not solve borrow-checker problems by blindly adding:

```rust
.clone()
```

or:

```rust
'static
```

or:

```rust
Arc<Mutex<_>>
```

Understand the ownership model first.

---

# 14. Ownership and Borrowing

Prefer short, obvious borrowing relationships.

Use owned stable identifiers across subsystem boundaries.

Long-lived references should be rare and intentional.

Avoid self-referential structures.

Avoid storing references into containers whose storage may move.

Do not clone merely to silence borrow-checker errors.

A clone should represent an intentional ownership decision.

Large data clones in execution or storage paths require particular scrutiny.

Use `Arc<T>` only when shared ownership is actually required.

Use `Mutex` / `RwLock` only when shared mutation or synchronization is actually required.

Do not make everything `Arc<Mutex<T>>`.

Lock ownership and lock ordering must remain understandable.

Never hold a synchronous lock across `.await`.

---

# 15. Unsafe Rust

Safe Rust is the default.

`unsafe` is allowed only when there is a concrete systems-level reason such as:

* low-level binary layout
* mmap
* FFI
* SIMD
* specialized memory handling
* carefully justified zero-copy implementation

Unsafe code must be localized behind safe abstractions.

Higher-level crates such as:

```text
compiler
planner
optimizer
executor
SDK
```

should normally contain no unsafe code.

Every `unsafe` block must have a nearby explanation beginning with:

```rust
// SAFETY:
```

The comment must explain the invariant that makes the operation valid.

Do not write meaningless comments such as:

```rust
// SAFETY: this is safe
```

Prefer denying `unsafe_op_in_unsafe_fn` at workspace level.

An `unsafe fn` does not make arbitrary unsafe operations acceptable.

Never introduce unsafe merely to improve a benchmark without first demonstrating that safe code is insufficient.

---

# 16. Error Handling

Library APIs should expose meaningful typed errors.

Prefer domain-specific error enums.

Examples:

```text
ParseError
TypeError
CatalogError
StorageError
PageError
TransactionError
ExecutionError
RecoveryError
```

Using `thiserror` is acceptable when already present or justified.

Do not expose `anyhow::Error` as a core library API.

`anyhow` may be appropriate at application boundaries such as:

```text
CLI
server startup
developer tools
```

where contextual aggregation is useful.

Production core code should not routinely use:

```rust
unwrap()
expect()
panic!()
unreachable!()
todo!()
unimplemented!()
```

Tests may use `unwrap()` or `expect()` when it improves readability.

A panic is acceptable for a truly internal invariant violation when continuing would indicate a programming bug, but malformed user input, malformed queries, corrupt files, network input, and normal operational failures must return errors.

Do not silently discard errors.

---

# 17. Result Context

Preserve enough context to diagnose failures.

A storage error should ideally identify relevant concepts such as:

```text
database
file
page
operation
offset
```

without leaking sensitive application data unnecessarily.

Compiler errors should preserve useful source locations.

Do not convert detailed errors into generic strings too early.

Structured errors should remain structured through internal layers.

---

# 18. Async Policy

The database core is synchronous by default.

Do not introduce async merely because Rust supports it.

These layers should normally remain synchronous:

```text
parser
resolver
type checker
optimizer
planner
executor core
page manager
buffer manager
storage engine
index
transaction core
WAL core
recovery
```

Async is appropriate at external concurrency boundaries such as:

```text
netbadb-server
network protocol
remote client
async application integration
```

A Tokio-based server must not force Tokio types through the storage engine.

Keep boundaries conceptually similar to:

```text
async network
      │
      ▼
request/session boundary
      │
      ▼
synchronous database core
```

Do not add async traits throughout the core without a demonstrated requirement.

---

# 19. Concurrency

Do not add concurrency before correctness.

Shared mutable state should have clear ownership.

Document important lock ordering rules.

Avoid global mutable state.

Avoid broad coarse locks unless deliberately chosen as an initial implementation.

If a simple coarse lock is sufficient for an early correct implementation, prefer it over an incorrect sophisticated design.

Optimization can follow measurement.

Do not mix transaction semantics with incidental Rust synchronization semantics.

A `Mutex` is not a transaction manager.

An `RwLock` is not MVCC.

---

# 20. Transaction Semantics

Transaction state must be explicit.

Avoid boolean combinations such as:

```text
active = false
committed = false
aborted = true
```

when an enum can represent the state correctly.

Prefer concepts similar to:

```rust
enum TransactionState {
    Active,
    Committed,
    Aborted,
}
```

Do not silently change isolation semantics.

Do not introduce a transaction optimization that changes externally observable behavior without tests and documentation.

Transaction IDs and timestamps should use dedicated types.

---

# 21. WAL and Recovery

Durability code must be designed around explicit invariants.

When WAL is implemented or modified, reason about:

```text
log ordering
pageLSN / equivalent metadata
flush ordering
commit durability
checkpoint behavior
redo
undo
idempotency
partial writes
crash boundaries
```

Never assume that a successful in-memory mutation means durable persistence.

Recovery operations should be safe to repeat where the recovery algorithm requires idempotency.

Crash consistency requires tests, not intuition.

---

# 22. Query Optimizer

Optimization rules must preserve semantics.

A rewrite is invalid if it changes observable query behavior even if it appears faster.

Take particular care with:

```text
NULL
three-valued logic
outer joins
aggregation
duplicates
ordering
LIMIT
volatile functions
type conversions
```

Keep logical optimization separate from physical implementation choice where practical.

Do not introduce a cost model until useful statistics exist.

Rule-based optimization is acceptable for early versions.

---

# 23. Executor

Keep logical and physical plans separate.

The executor should consume physical execution decisions rather than rediscover planner policy.

Avoid embedding storage implementation details into logical relational nodes.

Avoid unnecessary per-row heap allocation.

However, do not implement complicated zero-copy or vectorized execution prematurely.

Structure APIs so future batch/vectorized execution remains possible.

Correct row-at-a-time execution is acceptable as the initial implementation.

---

# 24. Internal IDs

Avoid repeatedly using strings to identify internal database objects.

Prefer stable IDs for resolved entities:

```text
DatabaseId
SchemaId
TableId
ColumnId
IndexId
FunctionId
PageId
TxnId
```

Names belong primarily at:

```text
parsing
catalog lookup
diagnostics
serialization/protocol boundaries
```

Resolved plans should prefer IDs where appropriate.

This reduces lookup cost and prevents ambiguous string identity.

---

# 25. Collections

Choose collections according to semantics.

Do not default to `HashMap` for every problem.

Consider:

```text
Vec
HashMap
BTreeMap
HashSet
BTreeSet
VecDeque
```

based on required:

* ordering
* lookup
* memory footprint
* determinism
* iteration behavior

Do not rely on unspecified hash iteration order for persistent data, plans, diagnostics, snapshots, or deterministic tests.

---

# 26. Dependencies

Keep the dependency graph small.

Before adding a dependency:

1. Check whether the workspace already provides the capability.
2. Check whether the standard library is sufficient.
3. Determine whether the dependency belongs in core code.
4. Consider compile-time and maintenance cost.
5. Enable only required features where practical.

Prefer mature, focused crates.

Do not add a framework for a small utility.

Do not introduce multiple crates solving the same problem without strong justification.

Use workspace dependency declarations for dependencies shared by several crates.

Do not upgrade unrelated dependencies as part of a feature change.

Do not regenerate `Cargo.lock` unnecessarily.

---

# 27. Serialization

`serde` is useful at boundaries but should not become NetbaDB's internal architecture.

Appropriate uses may include:

```text
configuration
debug tooling
protocol representations
Canonical Schema interchange
test fixtures
```

Do not use `serde_json::Value` as the primary database type system or execution representation.

Do not assume a generic serialization format is automatically suitable as a long-term on-disk database format.

Persistent formats require deliberate compatibility design.

---

# 28. Macros

Prefer normal Rust first.

Declarative macros are acceptable when they clearly reduce repetitive mechanical code.

Procedural macros require stronger justification.

Do not use macros to hide control flow, storage operations, locking, transaction behavior, or complex invariants.

Do not build derive macros before the underlying manual API is stable.

For example, establish a correct schema API before building:

```rust
#[derive(NetbaTable)]
```

around it.

---

# 29. Traits and Generics

Use traits to represent genuine behavioral abstraction.

Do not create a trait merely because a struct might theoretically have another implementation someday.

Avoid deep trait hierarchies.

Avoid unnecessary associated types and generic parameters.

Prefer concrete types inside implementation code when no abstraction boundary exists.

Use dynamic dispatch only where runtime polymorphism is actually needed.

Do not default to:

```rust
Box<dyn Trait>
```

for every subsystem.

Do not create generic abstractions before at least one concrete implementation demonstrates the need.

---

# 30. Public API Design

Keep the public API smaller than the internal API.

Do not expose implementation details unnecessarily.

Prefer:

```text
pub(crate)
```

when an item only needs workspace-local or crate-local visibility.

Do not make fields public merely to simplify tests.

Public types should have clear semantics and stable invariants.

Breaking public API changes should be intentional.

Do not accidentally expose storage representation through high-level APIs.

---

# 31. Naming

Prefer domain terminology.

Good:

```text
LogicalPlan
PhysicalPlan
TableId
PageId
TupleSlot
Transaction
Catalog
Predicate
Projection
BufferPool
```

Avoid vague names:

```text
Manager
Helper
Util
Data
Info
Thing
Processor
Context
```

unless the term has a precise architectural meaning.

Boolean names should describe their truth condition.

Functions should describe actions.

Types should describe domain concepts.

---

# 32. Modules and Files

Keep modules cohesive.

Split files when there is a real conceptual boundary, not merely because a file reaches an arbitrary line count.

Avoid giant `mod.rs` files containing unrelated systems.

Avoid dozens of tiny one-function modules.

Prefer discoverability over clever module layouts.

Do not build a `utils` dumping ground.

If functionality has a domain, place it with that domain.

---

# 33. Comments and Documentation

Comments should explain:

```text
why
invariant
tradeoff
safety
non-obvious behavior
```

rather than restating obvious code.

Bad:

```rust
// Increment i.
i += 1;
```

Useful:

```rust
// Slot 0 is reserved for the page metadata entry, so user tuples start at 1.
```

Public APIs with non-obvious contracts should use Rust documentation comments.

Storage formats and unsafe invariants require especially strong documentation.

Architecturally significant decisions should be reflected in repository documentation.

---

# 34. Formatting

Use `rustfmt`.

Do not manually fight rustfmt formatting.

Do not create formatting-only diffs in unrelated files.

Before completing Rust changes, run:

```bash
cargo fmt --all -- --check
```

If formatting fails because of your changes:

```bash
cargo fmt --all
```

and inspect the resulting diff.

---

# 35. Clippy

New code should be Clippy-clean.

Before completion, run:

```bash
cargo clippy --workspace --all-targets -- -D warnings
```

Do not silence Clippy warnings globally merely to make CI pass.

Avoid:

```rust
#![allow(...)]
```

for broad lint categories.

If a lint must be suppressed:

* scope the suppression narrowly
* explain why when the reason is not obvious

Do not globally enable all `clippy::pedantic` or `clippy::nursery` lints without evaluating their impact.

Use targeted lints that reinforce actual project invariants.

---

# 36. Suggested Workspace Lints

When configuring workspace lints, prefer a focused policy.

A reasonable baseline may include:

```toml
[workspace.lints.rust]
unsafe_op_in_unsafe_fn = "deny"
unused_must_use = "deny"
```

Do not add large lint lists just to appear strict.

Lint configuration should prevent real classes of mistakes without producing meaningless noise.

All workspace crates should inherit workspace lints where practical.

---

# 37. Tests Are Part of the Feature

A feature is not complete merely because it compiles.

Changes to observable behavior require tests.

Prefer tests close to the invariant being protected.

Use unit tests for local behavior.

Use integration tests for subsystem boundaries.

Use end-to-end tests for vertical database behavior.

---

# 38. Compiler Tests

Parser tests should cover:

```text
valid syntax
invalid syntax
precedence
associativity
source spans
edge cases
```

Resolver/type-checker tests should cover both successful and rejected programs.

Do not test only the happy path.

For typing changes, explicitly test that invalid combinations fail.

NetbaDB's type safety must be demonstrated through negative tests as well as positive tests.

---

# 39. Storage Tests

Storage tests should include:

```text
encode/decode round trips
minimum-size values
maximum-size values
boundary offsets
empty pages
full pages
insert/read cycles
truncated input
invalid offsets
corrupted metadata
reopen persistence
```

When persistence behavior changes, test close/reopen behavior.

Do not rely exclusively on in-memory state when testing persistent semantics.

Use temporary directories/files and ensure cleanup.

---

# 40. Deterministic Tests

Tests should be deterministic.

Avoid:

```text
sleep-based synchronization
wall-clock assumptions
random seeds that cannot be reproduced
network dependencies for core tests
global mutable test state
```

If randomized testing is used, failing cases must be reproducible.

Do not make unit tests depend on external services.

---

# 41. Property Testing and Fuzzing

Property tests and fuzzing are especially valuable for:

```text
parser
binary codecs
page decoding
expression evaluation
optimizer rewrites
storage corruption handling
```

Do not introduce these tools indiscriminately.

Use them where invariants justify the additional dependency and maintenance cost.

A fuzz-discovered bug must receive a deterministic regression test.

---

# 42. Benchmarks

Do not optimize because code "looks slow."

Measure first.

Benchmarks should target meaningful database operations rather than artificial micro-operations whenever possible.

Examples:

```text
page encode/decode
tuple insert
sequential scan
expression evaluation
index lookup
planner throughput
query execution
```

Performance changes should report what was measured.

Do not sacrifice invariants for an unmeasured optimization.

---

# 43. Logging and Tracing

Library code should not use `println!` for operational logging.

Use the project's tracing/logging abstraction where appropriate.

Logging must not change correctness.

Avoid logging complete row contents, credentials, secrets, or arbitrary query parameters by default.

Storage and recovery logs should identify operations using stable metadata such as IDs rather than dumping user data unnecessarily.

---

# 44. CLI and Server Boundaries

CLI and server layers may prioritize ergonomics differently from the database core.

They may use:

```text
anyhow
clap
tokio
tracing
```

when appropriate.

Do not propagate these dependencies into lower-level crates without a real requirement.

The server translates external asynchronous/network concerns into database operations.

The database core must remain independently usable in embedded mode.

---

# 45. Go and Other SDKs

Go is a client language, not the implementation language of the NetbaDB core.

Do not introduce Go-specific semantics into:

```text
Canonical Schema IR
logical plans
physical plans
storage formats
database type system
```

Rust applications may use a native embedded API.

Go and future languages should preferably interact through:

* generated clients
* stable protocol
* carefully designed FFI only when justified

Do not couple the storage engine to SDK code generation.

---

# 46. FFI

FFI is not the default cross-language integration mechanism.

Prefer a stable protocol for independent processes.

If FFI is required:

* keep the exported ABI small
* use C-compatible representations explicitly
* never expose Rust references directly
* never unwind Rust panics across FFI
* define ownership rules explicitly
* define allocation/freeing responsibilities
* validate all foreign input
* isolate unsafe code

Do not expose internal Rust structs as an ABI.

---

# 47. Security and Corrupt Input

Treat the following as untrusted input:

```text
queries
protocol messages
database files
WAL files
schema interchange
client parameters
configuration originating outside the process
```

Never assume decoded lengths and offsets are valid.

Validate before allocation when attacker-controlled sizes are involved.

Avoid integer overflow in size and offset calculations.

Corruption should produce bounded, diagnosable errors.

Malformed input must not cause memory unsafety.

---

# 48. No Placeholder Architecture

Do not create empty crates containing only:

```rust
// TODO
```

Do not add trait hierarchies with no implementation.

Do not introduce fake APIs merely to make the architecture diagram appear complete.

If a future subsystem is not implemented, document it in the roadmap instead.

Real code beats speculative scaffolding.

---

# 49. TODO Policy

`TODO` is acceptable only when:

* the current behavior is still correct
* the missing work is genuinely outside the current task
* the comment explains the missing requirement

Bad:

```rust
// TODO fix this
```

Better:

```rust
// TODO(storage): reclaim deleted tuple space when page compaction is implemented.
```

Do not use `todo!()` or `unimplemented!()` on normal reachable production paths unless the API is explicitly experimental and the behavior is clearly documented.

---

# 50. No Borrow-Checker Workarounds Without Design

These are warning signs:

```rust
.clone()
Arc::clone(...)
Box::leak(...)
'static
unsafe
RefCell
Mutex
Box<dyn Trait>
```

They are not forbidden.

But when introduced primarily to satisfy ownership errors, first reconsider the data model.

Prefer changing ownership boundaries over hiding them.

For database subsystem boundaries, stable IDs and short-lived guards are often better than long-lived object references.

---

# 51. No Premature Zero-Copy

Zero-copy is not automatically faster or simpler.

Do not spread borrowed page-backed values through the query engine purely to avoid allocations.

First establish a correct ownership model.

Then benchmark.

If zero-copy is justified, isolate lifetime-sensitive representations behind narrow interfaces.

---

# 52. Compatibility

Persistent storage and protocols have stronger compatibility requirements than internal Rust APIs.

Once a format is declared stable:

* do not silently change field encoding
* do not reuse numeric tags with new meanings
* do not reinterpret old pages
* do not reorder ABI/protocol fields casually
* provide explicit version handling

Before stability is declared, prefer changing a bad design rather than preserving it forever.

Clearly distinguish experimental formats from stable formats.

---

# 53. Repository Hygiene

Do not commit:

```text
target/
temporary databases
benchmark output
editor metadata
debug dumps
generated local files
secrets
credentials
```

unless explicitly required by the repository.

Do not modify unrelated generated files.

Do not perform mass renames without necessity.

Keep diffs focused.

Preserve existing copyright/license headers.

---

# 54. Git Discipline for Agents

Before major changes, inspect:

```bash
git status
git diff
```

Do not overwrite unrelated user changes.

Do not discard modifications that you did not create.

Do not use destructive Git commands unless explicitly requested.

Avoid:

```bash
git reset --hard
git clean -fd
git checkout -- .
```

Do not create commits unless the task asks for commits.

At the end, inspect the diff again.

---

# 55. Validation Before Completion

For normal Rust changes, run at minimum:

```bash
cargo fmt --all -- --check
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Run narrower tests during development when useful, but run the relevant workspace validation before finishing whenever practical.

If Go SDK code is modified, also run the relevant Go tests, typically:

```bash
go test ./...
```

If protocol, generated code, examples, or documentation have repository-specific validation commands, run those as well.

Do not claim tests passed unless they actually ran successfully.

---

# 56. Handling Validation Failures

If validation fails because of your change:

fix it.

If validation fails because of a clearly pre-existing issue:

1. verify that it is pre-existing where practical
2. do not hide it
3. report the exact failing command
4. explain the relevant failure
5. still validate the portions affected by your work

Do not remove tests to make the suite pass.

Do not weaken assertions merely to satisfy CI.

Do not add blanket lint suppressions to hide errors.

---

# 57. Definition of Done

A change is complete when:

* the requested behavior is implemented
* architecture boundaries remain valid
* new invariants are represented explicitly
* relevant tests exist
* documentation is updated when behavior or architecture changed
* formatting passes
* compilation/checking passes
* Clippy passes for affected workspace targets
* tests pass
* no unrelated changes were introduced
* no temporary debugging code remains
* no reachable placeholder implementation was added
* the final diff has been reviewed

---

# 58. Final Agent Report

When completing a non-trivial task, summarize:

1. What changed.
2. Why the design was chosen.
3. Which crates/modules were affected.
4. What tests were added or changed.
5. Which validation commands were executed.
6. Any known limitations or intentionally deferred work.

Do not describe a stub as an implemented feature.

Do not report commands as successful unless they were actually executed successfully.

---

# 59. Preferred Decision Heuristics

When multiple implementations are valid, prefer the one that has:

```text
fewer concepts
fewer dependencies
fewer allocations
clearer ownership
clearer invariants
smaller public API
smaller unsafe surface
more deterministic behavior
better tests
```

Do not interpret this as an instruction to prematurely micro-optimize.

Architectural simplicity is more important than shaving a small number of instructions.

---

# 60. NetbaDB Design Test

Before accepting an architectural change, ask:

```text
Does this preserve strong typing?

Does this keep Canonical Schema IR language-independent?

Does this make invalid states harder to represent?

Does this keep dependency direction clean?

Does this preserve embedded usage?

Does this avoid coupling the core to Tokio, Go, SDKs, or network concerns?

Does this preserve future storage and transaction evolution?

Can the behavior be tested deterministically?

Is the persistent representation explicit?

Is this simpler than the alternatives?
```

If several answers are "no", reconsider the design.

---

# 61. Things Agents Must Not Do

Unless explicitly required by the task, do not:

* rewrite the project around a framework
* replace strong domain types with primitives
* replace typed IR with JSON
* couple the core to Go
* couple the core to Tokio
* make the entire database async
* expose storage internals through public APIs
* persist Rust memory layouts
* use `usize` as a portable persistent integer type
* spread long lifetimes through the architecture
* solve ownership issues with excessive cloning
* make everything `Arc<Mutex<_>>`
* add unnecessary dynamic dispatch
* introduce broad unsafe code
* introduce nightly Rust
* globally silence Clippy
* use panics for user-controlled failures
* introduce empty architecture crates
* build speculative extension systems
* create compatibility layers for APIs that are not yet stable
* optimize without measurement
* change unrelated dependencies
* make unrelated formatting changes
* delete failing tests
* hide corrupted input errors
* silently change persistent formats
* silently change query semantics

---

# 62. Final Principle

NetbaDB is systems software.

Treat compiler semantics, storage formats, transactions, page layouts, recovery behavior, and type invariants as correctness-critical code.

The preferred implementation is not the most sophisticated implementation.

It is the smallest implementation that is:

```text
correct
explicit
typed
testable
maintainable
and structurally ready for the next layer
```

Build NetbaDB vertically.

Keep the core small.

Keep the boundaries strong.

Make illegal states difficult to represent.

Measure before optimizing.

Do not hide complexity behind Rust cleverness.
