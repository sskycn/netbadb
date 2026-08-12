# XDB

**XDB is a typed relational database built for application development.**

XDB combines a relational database engine, a typed query compiler, application-native schema semantics, developer tooling, and AI-friendly interfaces into one coherent system.

Its goal is not merely to execute SQL.

Its goal is to make the entire path from:

```text
Application Type
        ↓
Database Schema
        ↓
Typed Query
        ↓
Logical Plan
        ↓
Physical Plan
        ↓
Execution
        ↓
Typed Result
```

understandable, verifiable, inspectable, and safe.

> XDB is currently under active design and development.
>
> Features described as roadmap items are architectural goals and should not be considered implemented until explicitly marked otherwise.

---

## Why XDB?

Traditional application database stacks usually contain several independently evolving layers:

```text
Application
    ↓
ORM / Query Builder
    ↓
SQL
    ↓
Database Driver
    ↓
Database
```

Each boundary introduces another place where information can be lost.

Typical problems include:

* application types that do not match database types;
* runtime row scanning failures;
* accidental comparisons between semantically different IDs;
* ORM metadata drifting from the real schema;
* query errors discovered only at runtime;
* generated models becoming stale;
* migrations and application code evolving independently;
* query planners that application tooling cannot understand;
* AI tools operating on SQL strings without enough semantic context.

XDB is designed around a different model:

```text
Application
    ↓
Typed Database API
    ↓
Typed Query Compiler
    ↓
Relational IR
    ↓
XDB Planner
    ↓
XDB Execution Engine
```

Types, relations, parameters, result shapes, source locations, and query plans remain explicit throughout the pipeline.

---

# Design Principles

## 1. Types are semantic

XDB distinguishes between types that have the same physical representation but different meanings.

For example:

```go
type UserID uint64
type TeamID uint64
```

These types may both be represented internally using an unsigned integer, but they are not necessarily interchangeable.

Conceptually:

```text
UserID != TeamID
```

This makes it possible to detect mistakes such as comparing unrelated identifiers before query execution.

XDB therefore distinguishes between:

```text
Physical Type
Semantic Type
Nullability
Domain
Relationship
```

rather than reducing everything to a primitive SQL type as early as possible.

---

## 2. Queries are compiled

XDB treats queries as programs.

The intended compiler pipeline is:

```text
Source
  ↓
Lexer / Parser
  ↓
AST
  ↓
Name Resolution
  ↓
Type Checking
  ↓
Parameter Binding
  ↓
Typed HIR
  ↓
Logical Relational Plan
  ↓
Logical Optimization
  ↓
Property Inference
  ↓
Physical Planning
  ↓
Cost Selection
  ↓
Executable Plan
```

Query execution should never depend on a template engine reverse-engineering the meaning of a generated SQL string.

---

## 3. Relational algebra is the core

XDB uses a typed relational intermediate representation as the semantic boundary between query languages and the execution engine.

Conceptually:

```text
Scalar<T>

Row<{
    ...
}>

Relation<Row>

Command<Returning Relation?>
```

Relational nodes carry properties such as:

```text
column type
nullability
column provenance
candidate keys
cardinality
ordering
multiplicity
parameter requirements
```

The relational model is not merely an optimizer representation.

It is the common language between:

```text
Compiler
Optimizer
Planner
Executor
EXPLAIN
IDE
Debugger
MCP
AI tooling
```

---

## 4. Correctness before cleverness

XDB should prefer a simple, obviously correct plan over a sophisticated optimization whose semantic safety is unclear.

Logical optimizations may include:

```text
constant folding
boolean simplification
predicate normalization
redundant projection elimination
duplicate group/order elimination
safe filter pushdown
```

More advanced optimization should only be introduced when its correctness properties are well defined and thoroughly tested.

---

## 5. The database should be inspectable

Database internals should not be a black box.

Developers should be able to inspect:

```text
parsed query
resolved query
parameter types
result types
logical plan
optimized logical plan
physical plan
selected indexes
estimated rows
actual rows
execution timing
storage statistics
```

A query should be explainable from source code all the way to storage access.

---

## 6. Tooling and database semantics share one model

XDB is designed so that the following components do not independently reimplement database semantics:

```text
XDB Server
XDB CLI
Language Server
IDE
SDK
MCP Server
AI tools
Schema tools
Query debugger
```

They should operate through shared compiler, schema, catalog, diagnostic, and inspection APIs.

---

# Architecture

The long-term architecture is:

```text
                        ┌─────────────────┐
                        │     XDB IDE     │
                        └────────┬────────┘
                                 │
                     ┌───────────┴───────────┐
                     │      Tooling API      │
                     │   LSP / MCP / CLI     │
                     └───────────┬───────────┘
                                 │
                    ┌────────────▼────────────┐
                    │      XDB Compiler       │
                    ├─────────────────────────┤
                    │ Parser                  │
                    │ Resolver                │
                    │ Type Checker            │
                    │ Parameter Binder        │
                    │ Relational Builder      │
                    │ Logical Optimizer       │
                    │ Property Inference      │
                    └────────────┬────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │      XDB Planner        │
                    ├─────────────────────────┤
                    │ Statistics              │
                    │ Access Path Selection   │
                    │ Join Planning           │
                    │ Cost Model              │
                    │ Physical Planning       │
                    └────────────┬────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │      XDB Executor       │
                    ├─────────────────────────┤
                    │ Scan                    │
                    │ Filter                  │
                    │ Project                 │
                    │ Join                    │
                    │ Aggregate               │
                    │ Sort                    │
                    │ Set Operations          │
                    │ Write Operations        │
                    └────────────┬────────────┘
                                 │
              ┌──────────────────▼──────────────────┐
              │          Transaction Layer          │
              │     MVCC / Locks / Snapshots        │
              └──────────────────┬──────────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │     Storage Engine      │
                    ├─────────────────────────┤
                    │ Buffer Manager          │
                    │ Page Manager            │
                    │ Heap Storage            │
                    │ Indexes                 │
                    │ Catalog                 │
                    │ WAL                     │
                    │ Recovery                │
                    └─────────────────────────┘
```

---

# Project Layers

A possible repository structure is:

```text
xdb/
├── cmd/
│   ├── xdb/
│   └── xdbd/
│
├── internal/
│   ├── catalog/
│   ├── compiler/
│   │   ├── lexer/
│   │   ├── parser/
│   │   ├── ast/
│   │   ├── resolve/
│   │   ├── types/
│   │   ├── hir/
│   │   └── diagnostic/
│   │
│   ├── rel/
│   ├── optimizer/
│   ├── planner/
│   ├── physical/
│   ├── executor/
│   ├── txn/
│   ├── storage/
│   ├── buffer/
│   ├── page/
│   ├── index/
│   ├── wal/
│   ├── recovery/
│   ├── server/
│   └── protocol/
│
├── pkg/
│   └── xdb/
│
├── sdk/
│   └── go/
│
├── tools/
│   ├── lsp/
│   └── mcp/
│
├── tests/
│
└── docs/
```

The exact repository layout may evolve.

The architectural boundaries should not.

---

# Storage Engine

The first XDB storage engine is intended to prioritize:

```text
single node
local persistence
predictable behavior
strong crash safety
simple operational model
```

The initial storage hierarchy is expected to resemble:

```text
Database
   ↓
Database File(s)
   ↓
Pages
   ↓
Records / Index Nodes
```

Core storage components include:

### Page Manager

Responsible for:

```text
page allocation
page identifiers
free page tracking
page encoding
page validation
```

### Buffer Manager

Responsible for:

```text
cached pages
pin/unpin
dirty tracking
eviction
flush scheduling
```

### Heap Storage

Stores table tuples independently of index layout.

### Indexes

The initial general-purpose ordered index is expected to be B+Tree based.

Future index families may include:

```text
Hash
Bitmap
Full-text
Vector
Specialized application indexes
```

These are roadmap items rather than initial requirements.

---

# Transactions

Database correctness requires transactions to be a first-class part of the engine.

The intended transaction interface includes:

```text
BEGIN
COMMIT
ROLLBACK
```

The transaction system is expected to eventually provide:

```text
snapshot management
concurrent readers
concurrent writers
isolation levels
conflict detection
deadlock handling where applicable
```

The exact concurrency-control design will be selected based on implementation simplicity, correctness, and benchmark results.

Possible approaches include:

```text
MVCC
locking
hybrid MVCC + locking
```

The implementation should not commit to unnecessary complexity before workload requirements justify it.

---

# WAL and Recovery

A database must remain correct when the process or machine fails at arbitrary points.

XDB therefore treats crash recovery as a core storage requirement rather than a later reliability feature.

The long-term recovery architecture includes:

```text
Write-Ahead Log
Commit Records
Checkpoints
Redo
Recovery Analysis
Page Validation
```

A committed transaction must not depend on all modified data pages already having reached permanent storage.

---

# Catalog

XDB maintains a system catalog describing database objects.

Conceptually:

```text
Catalog
├── Schemas
├── Tables
├── Columns
├── Types
├── Domains
├── Constraints
├── Relationships
├── Indexes
├── Statistics
└── Functions
```

Unlike a conventional relational catalog, XDB may preserve application-level semantic type information.

For example:

```text
users.id

physical type:
    UINT64

semantic type:
    UserID

nullable:
    false

primary key:
    true
```

This information can then be used consistently by:

```text
query compiler
SDK generator
IDE
migration system
AI tooling
```

---

# Relationships

Relationships are explicit schema concepts.

For example:

```text
User.TeamID
     │
     └──────────► Team.ID
```

A relationship may carry enough information for the compiler to understand valid navigation paths.

Conceptually:

```text
User
 └── Team
      └── Organization
```

This allows higher-level queries to describe relationships without allowing arbitrary undeclared implicit joins.

---

# Query Language

XDB is intended to support relational queries through a typed query language.

SQL compatibility may also be provided where useful.

The native compiler model supports concepts such as:

```text
SELECT
INSERT
UPDATE
DELETE

WHERE
HAVING

JOIN
relationship navigation

GROUP BY
ORDER BY

LIMIT
OFFSET

DISTINCT

CTE

UNION
INTERSECT
EXCEPT

EXISTS
IN
BETWEEN

aggregates

RETURNING

UPSERT
```

Not every planned language feature is necessarily available in early versions.

---

# Example

Given:

```go
type UserID uint64
type TeamID uint64

type User struct {
    ID     UserID
    TeamID TeamID
    Name   string
}

type Team struct {
    ID   TeamID
    Name string
}
```

with the relationship:

```text
User.TeamID → Team.ID
```

a typed query may conceptually look like:

```text
select User {
    ID,
    Name,
    Team.Name
}
where User.ID = :id
```

The compiler can derive:

```text
Parameters

id: UserID
```

and a result shape:

```text
{
    ID: UserID,
    Name: string,
    Team.Name: string
}
```

before execution begins.

---

# Nominal Types

Nominal typing is an important XDB design goal.

Consider:

```go
type UserID uint64
type TeamID uint64
```

Although both may use the same physical representation:

```text
UserID != TeamID
```

Therefore a predicate such as:

```text
User.ID = :teamID
```

can be rejected when:

```text
:teamID : TeamID
```

unless an explicit conversion is legal.

The database should help preserve application invariants instead of erasing them.

---

# Logical Plans

Queries are translated into typed logical relational plans.

Example:

```text
Project
├── User.ID
├── User.Name
└── Team.Name
     │
   Filter
     │
 User.ID = :id
     │
    Join
   /    \
 User   Team
```

Logical plans describe meaning.

They should not encode unnecessary storage decisions.

---

# Physical Plans

The physical planner decides how the logical operation will execute.

For example:

```text
Limit
  │
Index Scan users_pkey
```

or:

```text
Hash Join
├── Seq Scan users
└── Hash
    └── Seq Scan teams
```

Physical choices may depend on:

```text
indexes
row count
cardinality estimates
column statistics
ordering
memory limits
cost estimates
```

---

# Query Optimization

XDB separates optimization into two categories.

## Logical optimization

Examples:

```text
constant folding
predicate simplification
projection reduction
safe predicate pushdown
redundant operation elimination
```

## Physical optimization

Examples:

```text
sequential scan vs index scan
join algorithm selection
join order
sort strategy
aggregation strategy
```

The optimizer should remain deterministic and inspectable wherever practical.

---

# EXPLAIN

XDB aims to make query plans easy to inspect.

Conceptually:

```text
xdb> explain
     select User.Name
     where User.TeamID = :team
     order by User.Name
     limit 10
```

could produce:

```text
Limit
  rows: 10
  cost: 4.21

└─ Index Scan users_team_name_idx
   relation: User
   constraint:
       TeamID = :team

   estimated rows: 38
```

More detailed modes may expose:

```text
logical plan
physical plan
estimated cost
actual timing
actual rows
buffer activity
I/O
memory
```

---

# Prepared Plans

Because XDB controls both the compiler and database execution model, application queries may eventually be represented as typed prepared plans rather than raw SQL text.

Conceptually:

```text
Application
    ↓
Query ID
+ Typed Parameters
    ↓
Prepared XDB Plan
    ↓
Execution
```

Example:

```text
Plan:
    UserRepository.GetUser

Parameters:
    id = UserID(42)
```

This can eliminate repeated parsing and reduce runtime query/schema mismatch.

---

# Go Integration

Go is the initial first-class application language for XDB.

A future Go API may look conceptually like:

```go
db, err := xdb.Open("app.xdb")
if err != nil {
    return err
}

user, err := users.Get(ctx, UserID(42))
```

Generated or compiled APIs should preserve:

```text
parameter types
result types
nullability
cardinality
semantic IDs
```

Application code should not need to manually reconstruct this information from SQL strings.

---

# XDB and Repository Generation

XDB can work with repository-oriented application architectures.

For example:

```go
type UserRepository interface {
    GetUser(
        ctx context.Context,
        id UserID,
    ) (User, error)
}
```

The query compiler may verify at build time that:

```text
parameter list matches
parameter types match
result cardinality matches
result shape matches
result Go type matches
```

The generated implementation then becomes a thin adapter over a compiled XDB operation.

---

# Schema Frontends

XDB should not permanently couple its internal schema model to one programming language.

The long-term architecture is:

```text
Go Schema ───────┐
                 │
XDB Schema ──────┼──► Canonical Schema IR
                 │
Future Frontend ─┘
```

Go may remain the primary frontend while the database core operates on a language-independent canonical schema representation.

---

# Migrations

Schema migration is a planned subsystem.

Its role is broader than generating arbitrary DDL.

The migration planner should compare:

```text
Current Catalog
       ↓
Desired Schema
       ↓
Migration Plan
```

and classify operations according to safety.

Conceptually:

```text
create
alter
rename
backfill
rebuild
validate
drop
conflict
```

Potentially destructive operations should always be explicit.

Future migration tooling may support:

```text
dry-run
migration diff
dependency analysis
data validation
rollback strategy
online migration planning
```

---

# XDB CLI

The command-line interface is expected to become the primary low-level administrative and development tool.

Possible command families include:

```text
xdb init

xdb start
xdb stop

xdb shell

xdb check
xdb fmt

xdb schema
xdb migrate

xdb query
xdb explain
xdb profile

xdb inspect

xdb verify

xdb backup
xdb restore

xdb doctor
```

Exact commands remain subject to implementation.

---

# XDB Server

XDB may operate in multiple modes.

## Embedded

```text
Application
    │
    └── XDB Engine
          │
        app.xdb
```

This mode targets:

```text
desktop applications
local-first applications
development tools
edge systems
single-node services
testing
```

## Client / Server

Roadmap:

```text
Application
    │
 XDB Protocol
    │
   xdbd
    │
 Storage
```

This mode may later provide:

```text
authentication
TLS
connection management
remote access
observability
administration
```

---

# XDB IDE

XDB is intended to have a database-aware development environment.

The IDE should not merely wrap a text editor.

Its purpose is to visualize and expose XDB semantics.

Conceptually:

```text
┌─────────────────────────────────────────────────────┐
│                     XDB IDE                         │
├───────────────┬──────────────────────┬──────────────┤
│ Project       │ Editor               │ Inspector    │
│               │                      │              │
│ Tables        │ Go / XDB Query       │ Types        │
│ Relations     │                      │ Parameters   │
│ Repositories  │                      │ Result       │
│ Queries       │                      │ Plan         │
│ Indexes       │                      │ Statistics   │
├───────────────┴──────────────────────┴──────────────┤
│ Diagnostics / Terminal / Query Results / Profile    │
└─────────────────────────────────────────────────────┘
```

---

# Schema Explorer

The IDE may represent schemas directly:

```text
User
├── ID
│   ├── UserID
│   └── PRIMARY KEY
│
├── TeamID
│   ├── TeamID
│   └── REF → Team.ID
│
└── Name
    └── string
```

Relationships can then be inspected graphically:

```text
Organization
      ▲
      │
     Team
      ▲
      │
     User
```

---

# Query Inspector

While editing a query, the IDE should be able to display:

```text
Query Type

Relation<{
    ID: UserID,
    Name: string
}>
```

together with:

```text
Parameters
Result Shape
Cardinality
Relations
Indexes
Logical Plan
Physical Plan
Diagnostics
```

without requiring the developer to manually execute the query first.

---

# Plan Visualizer

A graphical plan may look like:

```text
             Limit
               │
              Sort
               │
           Hash Join
          /         \
   Index Scan     Seq Scan
      User          Team
```

Nodes should be inspectable for:

```text
estimated rows
actual rows
cost
time
memory
I/O
predicate
index
output columns
```

---

# Diagnostics

Diagnostics are a first-class XDB API.

Every compiler or database development error should ideally contain:

```text
stable error code
severity
message
source span
related source spans
structured metadata
optional help
```

Example:

```text
error[XDB3204]:

cannot compare UserID with TeamID

  User.ID = :teamID
  ^^^^^^^   ^^^^^^^

User.ID:
    UserID

:teamID:
    TeamID

help:
    use a UserID parameter or perform an explicit valid conversion
```

Diagnostics should be reusable by:

```text
CLI
LSP
IDE
MCP
CI
AI
```

---

# Language Server

XDB language tooling should provide:

```text
diagnostics
completion
hover
go to definition
references
document symbols
formatting
rename
semantic tokens
code actions
```

The language server should consume the same compiler APIs used by the database itself.

It should never maintain a parallel semantic implementation.

---

# MCP

XDB is intended to expose structured development and database capabilities to AI systems through MCP.

Possible read-only operations include:

```text
inspect_schema
inspect_query
inspect_types
inspect_relations
explain_query
inspect_indexes
inspect_statistics
inspect_migration
check
```

Explicitly mutating operations may include:

```text
apply_migration
create_index
run_write
update_schema
```

Mutation must remain distinguishable from inspection and planning.

AI access must respect workspace, database, transaction, and permission boundaries.

---

# AI-Native Development

Because XDB exposes structured semantics rather than only source text, AI tools can reason about:

```text
schema
types
relationships
queries
plans
indexes
statistics
migrations
diagnostics
```

For example:

```text
Developer:

Why is UserRepository.ListUsers slow?
```

An AI system could inspect:

```text
query
logical plan
physical plan
estimated rows
actual rows
indexes
statistics
```

and answer based on database state rather than guessing from SQL text alone.

---

# Safety

XDB should follow an explicit safety model.

Potentially destructive actions must be distinguishable from planning.

For example:

```text
plan migration
```

must not implicitly become:

```text
apply migration
```

Likewise:

```text
explain query
inspect schema
inspect catalog
```

should be read-only operations.

Administrative tools should avoid ambiguous implicit mutation.

---

# Compatibility Backends

XDB does not need to require every application to immediately use the native XDB storage engine.

The broader compiler and tooling architecture may support:

```text
               XDB Compiler
                     │
          ┌──────────┼───────────┐
          │          │           │
          ▼          ▼           ▼
        XDB       PostgreSQL    MySQL
      Native
```

Additional backends may include SQLite.

This allows applications to use XDB's:

```text
schema model
typed compiler
diagnostics
repository generation
IDE
MCP
```

even when production data remains in an existing database.

The native XDB backend can then provide the deepest integration.

---

# Non-Goals

XDB should not initially attempt to reproduce every feature accumulated by mature database systems.

Initial non-goals include:

```text
distributed consensus
automatic sharding
multi-region transactions
PostgreSQL extension compatibility
complete SQL compatibility
hundreds of index types
arbitrary stored procedure languages
distributed query execution
massive OLAP workloads
```

Those features should only be introduced if real workloads justify them.

---

# Development Strategy

XDB should grow vertically rather than horizontally.

A feature is most valuable when it works through the complete stack:

```text
Syntax
 ↓
Parser
 ↓
Types
 ↓
Logical Plan
 ↓
Physical Plan
 ↓
Executor
 ↓
Storage
 ↓
Diagnostics
 ↓
EXPLAIN
 ↓
Tests
 ↓
IDE
```

It is preferable to have a smaller feature set with complete vertical integration than a broad SQL surface with incomplete semantics.

---

# Roadmap

## Phase 0 — Compiler Foundation

Focus:

```text
canonical schema
type system
AST / HIR
typed relational IR
diagnostics
query compiler
logical properties
```

Goals:

* stable internal compiler boundaries;
* source spans preserved through compilation;
* deterministic diagnostics;
* typed query results;
* nominal scalar types;
* relationship-aware resolution.

---

## Phase 1 — Storage Prototype

Focus:

```text
database file
page manager
buffer manager
heap storage
catalog
basic B+Tree
```

Goals:

* create database;
* create table;
* persist rows;
* scan rows;
* basic primary-key lookup;
* reopen database after process restart.

Correctness is more important than performance.

---

## Phase 2 — Transactions and Recovery

Focus:

```text
transactions
WAL
commit protocol
rollback
checkpoint
crash recovery
```

Goals:

* atomic writes;
* durable commits;
* reliable recovery after process termination;
* transaction test harness.

---

## Phase 3 — Query Execution

Focus:

```text
scan
filter
projection
insert
update
delete
join
aggregate
sort
limit
```

Goals:

* execute typed relational plans directly;
* no SQL-string round trip for native execution;
* deterministic execution semantics.

---

## Phase 4 — Planner

Focus:

```text
statistics
index selection
physical operators
cost estimates
join strategy
```

Goals:

* logical / physical separation;
* EXPLAIN;
* simple cost model;
* index-aware planning.

---

## Phase 5 — Go SDK

Focus:

```text
typed connection API
transactions
prepared plans
repository generation
result decoding
```

Goals:

```text
Go Type
   ↓
XDB Compiler
   ↓
XDB Database
   ↓
Go Type
```

with no untyped mapping boundary.

---

## Phase 6 — Developer Tooling

Focus:

```text
xdb CLI
LSP
MCP
schema explorer
query inspector
plan visualizer
```

---

## Phase 7 — XDB IDE

Focus:

```text
code editor
database explorer
schema graph
query console
diagnostics
plan visualization
profiling
Git integration
AI integration
```

---

## Phase 8 — Server Mode

Focus:

```text
xdbd
wire protocol
authentication
TLS
sessions
remote transactions
connection pooling
```

---

## Future

Only when required by real-world usage:

```text
replication
CDC
read replicas
high availability
distributed storage
distributed execution
cluster management
```

---

# Testing Philosophy

Database correctness must be demonstrated aggressively.

XDB should use multiple complementary testing strategies.

## Unit Tests

Each subsystem should contain adjacent tests for:

```text
parser
types
relational properties
pages
indexes
transactions
WAL
planner
executor
```

## Golden Tests

Useful for:

```text
AST
HIR
logical plans
physical plans
diagnostics
EXPLAIN
generated Go
```

## Property Tests

Useful for:

```text
B+Tree invariants
encoding / decoding
transaction visibility
optimizer equivalence
```

## Fuzzing

Priority fuzz targets include:

```text
lexer
parser
binary decoding
page decoding
WAL recovery
catalog decoding
query normalization
```

## Crash Tests

The process should be terminated at controlled crash points:

```text
before WAL flush
after WAL flush
during page flush
before commit record
after commit record
during checkpoint
```

The reopened database must satisfy transactional invariants.

## Differential Tests

Where XDB supports semantics comparable to another mature relational database, test suites may compare results against a reference database.

---

# Performance Philosophy

Performance work should be evidence-driven.

Measure first:

```text
parse time
compile time
planning time
execution time
buffer hit rate
page reads
page writes
WAL bytes
transaction latency
index lookup latency
```

Then optimize the dominant cost.

XDB should avoid architecture complexity introduced solely for hypothetical performance.

---

# Observability

Future XDB observability may expose:

```text
active connections
active transactions
query latency
slow queries
buffer usage
cache hit rate
WAL size
checkpoint status
database size
table size
index size
row estimates
lock waits
```

These metrics should also be consumable programmatically by developer and AI tooling.

---

# What Makes XDB Different?

XDB is not defined by a new SQL syntax.

Its core differentiator is the integration of:

```text
Database
+
Typed Compiler
+
Application Schema
+
Relational IR
+
Developer Tools
+
IDE
+
AI Interface
```

around one semantic model.

The intended result is:

```text
one schema
one type system
one query model
one diagnostic model
one relational model
```

used consistently across the entire development stack.

---

# Vision

The long-term XDB development experience should look like this:

```text
Define application types
          ↓
Schema becomes visible
          ↓
Relationships become visible
          ↓
Write typed query
          ↓
Compiler validates it
          ↓
IDE shows its result type
          ↓
IDE shows its logical plan
          ↓
Planner selects physical plan
          ↓
XDB executes it
          ↓
Profiler explains what happened
          ↓
Application receives typed result
```

And when something goes wrong:

```text
Source
  ↓
Diagnostic
  ↓
Schema
  ↓
Plan
  ↓
Storage
```

should remain traceable.

XDB should make the database part of the application's type system and development environment rather than an opaque service hidden behind SQL strings.

---

# Status

XDB is an experimental project.

The immediate priorities are:

```text
1. stable schema model
2. stable type system
3. typed relational IR
4. minimal persistent storage
5. transaction correctness
6. WAL and recovery
7. basic execution engine
8. query inspection
```

Distributed database features are intentionally not an early priority.

---

# License

TBD.
