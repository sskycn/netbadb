# NetbaDB architecture

## Boundaries

NetbaDB keeps application language concerns at the frontend boundary. A Go,
Rust, or future schema frontend should produce the same Canonical Schema IR:
tables, columns, physical types, semantic types, nullability, keys, and
relationships. The core consumes that representation and does not inspect Go
types or Rust application structs.

In this graph, `A -> B` means A depends on B. The current crate graph is:

```text
netbadb-schema -> netbadb-types
netbadb-schema-spec -> netbadb-schema + netbadb-types + serde + serde_json
netbadb-codegen -> netbadb-schema-spec + netbadb-schema + netbadb-types
netbadb-inspect -> netbadb-schema + netbadb-types
netbadb-hir -> netbadb-parser + netbadb-schema + netbadb-types
netbadb-rel -> netbadb-types
netbadb-compiler -> netbadb-hir + netbadb-parser + netbadb-rel
                    + netbadb-schema + netbadb-types
netbadb-tooling -> netbadb-compiler + netbadb-hir + netbadb-parser
                    + netbadb-schema
netbadb-index -> netbadb-types
netbadb-planner -> netbadb-index + netbadb-rel + netbadb-types
netbadb-storage -> netbadb-index + netbadb-schema + netbadb-types
netbadb-executor -> netbadb-planner + netbadb-rel + netbadb-storage
                    + netbadb-types
netbadb-core -> compiler + inspect + planner + rel + executor + storage
                 + schema + types
netbadb-protocol -> netbadb-types
netbadb-client -> netbadb-protocol + netbadb-schema + netbadb-types
netbadb-server -> netbadb-core + netbadb-protocol + netbadb-schema
                    + netbadb-types
netbadbd -> netbadb-server
netbadb CLI -> netbadb-sdk embedded + netbadb-server + serde + serde_json
netbadb-lsp -> netbadb-tooling + netbadb-schema-spec + lsp-server + lsp-types
netbadb-sdk embedded -> netbadb-core + netbadb-inspect + netbadb-schema
                        + netbadb-types
netbadb-sdk remote -> netbadb-client + netbadb-schema + netbadb-types
```

No lower layer depends on a higher layer. In particular, storage does not
depend on planner or executor, executor does not depend on an SDK, and compiler
layers do not depend on tooling or LSP protocol types.

## Schema-driven tooling boundary

Editor diagnostics and runtime inspection are deliberately separate paths:

```text
Schema-only editing                         Runtime inspection

SDK Schema Spec v1                         Database runtime state
        ↓                                           ↓
netbadb-schema-spec                         real planner + storage metadata
        ↓                                           ↓
Canonical Schema + source SQL               netbadb-inspect DTOs
        ↓                                           ↓
netbadb-compiler                             embedded SDK / offline CLI
        ↓                                           ↓
ToolingDiagnostic                           text / Inspection JSON v4
        ↓
netbadb-lsp UTF-16 adapter
```

`netbadb-tooling` converts the compiler's first `ParseError` or `HirError` into
a stable code, human message, and half-open UTF-8 byte span. It contains no LSP
URI, range, document version, or severity types. `netbadb-lsp` is the adapter
that tracks full editor buffers and converts those byte spans into zero-based
UTF-16 line/character ranges. It loads a validated SDK Schema Spec once at
startup and never opens a database, starts recovery, invokes physical planning,
connects to `netbadbd`, spawns the inspection CLI, or parses Inspection JSON.

The compiler remains fail-fast and each document represents one SQL statement,
so the current LSP publishes at most one compiler diagnostic per document.
Completion, hover, definitions, semantic tokens, and schema hot reload require
separate parser/source-index designs and are not advertised.

## Stable inspection boundary

Catalog metadata and the physical plan selected by the real planner cross a
one-way conversion boundary in `netbadb-core`:

```text
compiler / planner / storage internal state
                    ↓
              netbadb-core
          exhaustive conversion
                    ↓
             netbadb-inspect
                    ↓
             embedded SDK
                    ↓
       offline CLI text / explicit JSON v4
```

`netbadb-inspect` depends only on canonical schema and type domains. Its DTOs
retain stable table, relation-binding, column, semantic-type, expression, and
chosen-operator meaning, but never contain BTree handles, page/row identities,
WAL state, or planner IR. `Database::inspect_catalog` reads schema, persistent
index registration order, and cached `ANALYZE` snapshots without scanning or
refreshing them. `Database::inspect_statement` compiles once, invokes the same
planning path as execution, and converts the chosen plan without executing it,
opening a transaction, acquiring the writer, or appending WAL.

Inspection DTOs are observation results and never feed back into planning or
execution. The explicit text renderer is deterministic human-readable output,
not SQL, Rust `Debug`, a wire contract, or a versioned JSON API. Statistics are
last-`ANALYZE` snapshots and may be stale.

The `netbadb` CLI is an offline adapter, not a new compiler or planner layer:

```text
deployment manifest v4
          ↓
netbadb-server ServerConfig bootstrap
          ↓
netbadb-sdk embedded Database
          ↓
netbadb-inspect DTOs
       ↙       ↘
human text   Inspection JSON v4
```

The CLI uses `ServerConfig` only to validate deployment configuration and
obtain table bootstrap paths and canonical definitions. It never starts a TCP
server, creates a session, or applies network-principal authorization to local
filesystem access. JSON v4 is the current explicit external CLI contract,
converted exhaustively from inspection DTOs; v1, v2, and v3 remain historical
contracts and the DTOs themselves remain serde-free.
Future runtime-inspection tooling, including MCP, consumes those DTOs directly
rather than spawning the CLI. The diagnostics-only LSP does not use this path.

Local inspection requires exclusive process ownership because persistent files
have no cross-process lock. `Database::open_tables` performs normal recovery,
so opening after a crash may redo or undo WAL state before inspection. Output
is fully rendered and the database successfully closed before stdout is
written; inspected SQL, including DML, is compiled and planned but never
executed.

## Canonical Schema IR

`netbadb-schema` stores database meaning in explicit Rust structs that are
independent of any application language. A column has:

- stable `ColumnId`;
- a name;
- a `TypeSpec` containing physical type and optional semantic name;
- nullability;
- primary-key metadata.

`Schema::new` is the fallible construction path and delegates to
`Schema::validate`; unchecked public construction is not available. Validation
rejects duplicate table IDs/names, duplicate column IDs/names within a table,
empty table/column names, and empty semantic-type names. Canonical names are
frontend-independent UTF-8 identities, and equality remains exact and
case-sensitive. The current SQL frontend still supports only its existing
unquoted ASCII identifier syntax and has no quoted identifiers. Names outside
that textual subset can be persisted and identified but cannot yet be referenced
through SQL text.
Zero-column tables remain valid and receive an identity with column count zero.
Primary-key metadata is preserved in identity, but this phase does not add key
enforcement; nullability remains the independently enforced write constraint.

Each validated `TableDef` has canonical encoding version 1. It starts with
`NBTS`, an explicit little-endian version and reserved field, then encodes the
table ID/name and declared column count. Every column follows in declaration
order with its ID/name, an explicit physical-type tag, optional semantic-type
name, nullability, and primary-key booleans. Strings are UTF-8 with little-endian
`u32` byte lengths. SHA-256 over these bytes is the 32-byte
`SchemaFingerprint`; no Rust enum discriminant, layout, `Debug` output, or map
iteration order participates.

## Compiler and plans

The first query subset follows:

```text
source → AST → resolved/type-checked HIR → logical plan → physical plan
```

HIR owns source-level resolution and semantic type checking. Relational IR
owns relational meaning and column provenance. Core snapshots each table
storage's advertised access paths; the planner selects exact point
IndexScan or SeqScan access and the correctness-first nested-loop
implementation for logical INNER JOIN. The executor evaluates typed
expressions against rows returned by storage.

The implementation uses IDs and owned values between layers. It does not keep
long-lived references to pages, frames, or tuples, leaving room for future
buffer management and concurrent execution without spreading lifetimes across
the whole system.

## Relation bindings and INNER JOIN

`TableId` identifies a catalog table; `RelationBindingId` identifies one
query-local occurrence in a FROM/JOIN tree and is never persisted. Bindings are
allocated deterministically in source order. This distinction makes a self
join such as `employees e JOIN employees m` two independent relation instances
even though both scans target the same `TableId` and use the same `ColumnId`
values.

Each binding records its catalog table and exposed name. With an alias, only
the alias is exposed; otherwise the table name is exposed. Duplicate exposed
names are rejected. Qualified lookup first resolves that name and then the
column. Unqualified lookup searches all visible bindings and succeeds only for
exactly one candidate; zero candidates are unknown and multiple candidates are
ambiguous. JOIN scopes grow from left to right: an `ON` expression sees the
left subtree plus its current right binding, never a future relation.

Resolved HIR and relational `ColumnRef` values carry binding, table, and column
IDs. Alias strings remain diagnostic metadata and are not used by execution.
Logical scans carry their binding identity, and chained joins form a
left-associated tree:

```text
LogicalPlan::Join(left plan, right plan, Inner, typed predicate)
    ↓ planner (no join reordering)
PhysicalPlan::NestedLoopJoin or eligible PhysicalPlan::HashJoin
```

Every physical node has binding-aware output columns. Expression and projection
lookup uses `RelationBindingId + ColumnId`, which remains unambiguous for self
joins. A scan row retains one hidden, storage-owned `StorageRowHandle` for DML;
the current Heap implementation privately maps it to a generation-safe `RowId`.
A joined row combines scalar values and intentionally drops mutation identity
because multi-table UPDATE/DELETE are not supported.

The planner keeps NestedLoopJoin as the general implementation. At the current
join node it considers HashJoin only for an INNER JOIN over two direct logical
scans, with existing table statistics on both sides and a necessary cross-side
typed column equality found by deterministic left-to-right traversal through
AND nodes:

```text
Join
 |
 +-- no statistics, non-equi, or unsupported child/predicate
 |      -> NestedLoopJoin
 |
 +-- analyzed direct Scan × Scan with cross-side equality
        +-- left_rows + right_rows < left_rows * right_rows
               -> HashJoin
        +-- otherwise
               -> NestedLoopJoin
```

Both costs use checked `u128` work units. Missing or stale statistics can affect
only the algorithm choice; the complete predicate remains the semantic source
of truth. There is no join reorder, selectivity estimate, or global optimizer
cost model.

NestedLoopJoin materializes both child results, iterates left rows outside and
right rows inside, and evaluates the typed `ON` predicate through a non-owning
joined view:

```text
materialized left child + materialized right child
    -> joined value view for predicate evaluation
    -> TRUE only: materialize left values + right values with row_id None
```

FALSE and UNKNOWN pairs allocate no combined value vector and copy no full row;
ordinary equality therefore still never joins NULL to NULL. TRUE pairs are
materialized in left-then-right order. Join predicate positions are execution
layout properties and are bound only after both child outputs are known:

```text
typed Join predicate Expr with ColumnRef identity
        ↓ executor bind once through RelationBindingId + ColumnId
private position-bound expression borrowing the original Expr
        ↓
candidate-pair evaluation through checked positions
```

NestedLoopJoin binds its complete `ON` expression once before the candidate
loop. HashJoin retains its separately prebound hash-key positions and binds the
complete residual predicate once before probing buckets. The private bound
tree is not Rel IR or planner IR, exposes no public API, and does not replace
logical column identity with `usize` positions. It borrows literal values and
diagnostic column names from the original expression; a missing input field or
short runtime row remains a typed `MissingColumn` error. Filter predicates and
UPDATE assignments intentionally retain dynamic position resolution.

NestedLoopJoin also uses the bound tree for exact runtime rejection before its
inner loop:

```text
bound Join predicate
    -> first necessary direct cross-side inequality under AND
    -> normalize operands to left <op> right child positions
    -> borrow exact non-NULL min/max from materialized right rows
    -> can this left row match any right row?
         no  -> skip the complete right loop
         yes -> existing right-order loop + complete bound predicate
```

`>` and `>=` use the minimum right value; `<` and `<=` use the maximum.
Right NULLs are ignored because their comparison is UNKNOWN, an all-NULL right
key makes the join empty, and a NULL left key skips that left row. Extraction
descends only through AND because each conjunct is necessary for a TRUE result;
it never descends through OR or NOT. Reversed operands are normalized, multiple
eligible conjuncts use the first left-to-right match, and `compare_values`
remains the ordering authority. The extreme is borrowed from the current right
rows and is neither a planner estimate nor persistent statistics.

In Phase 7H alone, a left row that can match scans every right row in its
original order and evaluates the full predicate. That zero-candidate step does
no child sorting or candidate-range narrowing and changes no physical plan or
inspection contract.

Phase 7I retains that zero-candidate fast path, then adaptively narrows possible
probes when the first necessary inequality has a useful exact range:

```text
bound necessary inequality
    -> Phase 7H exact extreme rejection
    -> potential left row indices in original order
    -> borrowed-key sorted left/right index auxiliaries
    -> exact candidate count with a two-pointer sweep
    -> checked integer runtime work comparison
         nested work <= sweep work -> Phase 7H NestedLoop fallback
         sweep work < nested work  -> ordered candidate sweep
    -> complete bound predicate for every selected pair
    -> per-left output buckets
    -> flatten in original left order
```

The runtime choice is execution-local and uses no ANALYZE statistics or magic
selectivity ratio. Nested work is potential-left count times total right rows.
Sweep work adds the exact candidate pairs, checked `n * ceil_log2(n)` index-sort
proxies, and a checked ordered-set proxy; overflow and ties conservatively use
the nested loop. The 100%-reject case returns before sorting, and an all-pairs
boundary proof also avoids building auxiliaries for fully dense candidates.

Sorted auxiliaries contain only original row indices and borrow scalar keys.
Right NULL keys are excluded, left NULL keys are not potential probes, and all
ordering uses the same fallible `compare_values` implementation. For `>`/`>=`,
an original-right-index `BTreeSet` grows with keys `<`/`<=` each ascending left
key; for `<`/`<=`, it starts with every non-NULL right and removes keys `<=`/`<`
the left key. Each right index changes set membership at most once. Iterating
the set preserves original right order, while per-left buckets restore original
left order after key-ordered processing.

The auxiliary memory bound is O(left rows + right rows + output rows): candidate
pairs are never materialized. Only a complete predicate result of TRUE creates
an owned output row. This remains an internal NestedLoopJoin strategy, not a new
physical operator, planner cost, persistent statistic, or inspection contract.

Phase 7J applies required-column propagation only after the complete physical
query tree has been selected. Logical scans retain the full resolved relation,
and UPDATE/DELETE keep full base rows. The public query planner performs one
top-down pass over the raw physical tree:

```text
typed physical query
    -> required source columns (RelationBindingId + ColumnId)
    -> preserve Filter predicate and Sort key columns
    -> preserve group keys and aggregate column inputs
    -> preserve Join predicate columns and defensive HashJoin keys
    -> split Join requirements by binding-aware child output identity
    -> prune SeqScan / IndexScan / RangeIndexScan columns in source order
    -> executor passes ordered ColumnId projections to Heap
    -> Heap validates every encoded scalar
    -> own only requested ScalarValues
```

Membership is deduplicated, but operators never reorder source columns by
discovery order. A repeated result projection remains repeated while its base
scan reads the source once. `COUNT(*)` can drive a zero-column scan. Phase 7R's
executor specialization consumes that exact shape through the Heap presence
summary, so direct all-star aggregates no longer construct one empty
storage-row-handle-bearing execution row per live tuple.

Join children may retain columns needed only by their own predicates. The join
executor therefore binds the current predicate against the concrete
left-then-right child layout, then projects matching rows to the current join's
declared columns. This lets an inner join consume its private predicate columns
without leaking them into an outer join, while outer ON columns still propagate
through a chained left subtree. Self joins remain distinct because TableId or
ColumnId alone never determines membership.

The Heap projected-read APIs resolve typed ColumnIds to schema positions once
per read or scan. Their decoder borrows Text as `&str` while parsing, validates
tag, length, bounds, UTF-8, physical type, nullability, truncation, and trailing
values for every schema column, then allocates owned Text only for selected
values. Request order and duplicates are preserved. Full reads use the same
borrowed decoder core, page validation remains once per immutable Heap page per
scan, and point/range reads retain page bounds, tombstone, and RowId generation
checks. Row encoding and every persistent format are unchanged.

Phase 7K removes the next temporary owner at the executor Project boundary.
Project consumes its `ExecutionRows`, so one operator-level `ProjectionPlan`
resolves binding-aware positions and their last uses before processing rows:

```text
owned input ExecutionRows
    -> identity positions: move rows and values Vec directly
    -> unique subset/reorder: move selected ScalarValues from owned slots
    -> duplicate source used N times: clone N - 1, move at last use
    -> fully owned QueryResult
```

The identity path neither rebuilds each row's values Vec nor clones a
ScalarValue. Generic projection preserves checked indexing, output order,
duplicates, nullable/type metadata, and the opaque `StorageRowHandle`. A
repeated Text output still
requires independent String owners, so the last-use rule only removes clones
that are not semantically necessary. Join candidate materialization is
unchanged because child rows may produce several matches; only a temporary
joined row that is already exclusively owned can use the same projection
helper.

This is not a zero-copy result path. Selective Heap decode still creates one
owned String for each selected Text value because QueryResult owns its rows;
Phase 7K moves that owner through Project instead of allocating a second,
short-lived String. PhysicalPlan, planner decisions, storage APIs, Inspection
JSON v3, and persistent formats are unchanged.

Join-bound evaluation represents an intermediate scalar as either a borrowed
row/literal value or an owned computed value:

```text
bound Column/Literal
    -> borrow row/literal ScalarValue
    -> evaluate binary semantics through ScalarValue references
    -> own only Bool/NULL results produced by Binary, Unary, or IS NULL
```

The borrowed lifetime is limited to one candidate evaluation and cannot escape
the materialized child rows or typed predicate. Binary comparison and
three-valued truth semantics have one reference-based core; the existing owned
evaluator is a thin caller of that core, so Filter and UPDATE remain owned and
dynamically resolved. AND/OR still evaluate both sides without short-circuiting.
The existing expression checker requires BOOL while allowing nullable BOOL,
and nominal compatibility prevents JOIN from comparing distinct semantic types
with the same physical encoding. Text still becomes owned once per decoded
storage row; only repeated candidate-level Text cloning is removed.
HashJoin also materializes both children but fixes the right child as the build
side:

```text
right rows
   -> HashMap<ScalarValue, Vec<right-row index>>
                 ^
left key probes -+
   -> complete typed residual predicate
   -> TRUE only: materialize left + right with row_id None
```

Build and probe NULL keys are skipped because SQL `NULL = NULL` is UNKNOWN.
Buckets store indices rather than cloned rows, and indices are appended in
right input order. Probing in left input order therefore preserves duplicates
and the current deterministic left-major/right-minor behavior. Hash iteration
order never determines results. This is executor behavior, not an unordered SQL
result-order guarantee. Bool, Int64, UInt64, and Text use exact ScalarValue
identity, while semantic compatibility protects nominal types and self joins
use binding plus column IDs to identify sides.

Query operators are arranged as
`Scan/Join -> Filter -> Sort -> Project -> Limit`, allowing sorting by source
columns that projection omits. `SELECT *` follows left-to-right relation and
schema order.

Core multi-table catalogs compose one existing heap file per `TableId`. JOIN
therefore introduces no transaction-layer changes: the current page format is
version 5, heap metadata is version 3, WAL is version 3 with record version 2,
recovery, checkpoints, and single-writer rules are unchanged.

## Typed ORDER BY

`ORDER BY` accepts one or more source-column keys, qualified or unqualified,
and resolves them through the same complete `FROM`/`JOIN` `RelationScope` used
by other expressions. HIR makes every option explicit: omitted direction is
`ASC`; omitted NULL placement is `NULLS LAST` for ascending keys and
`NULLS FIRST` for descending keys. Alias names, ordinals, and arbitrary sort
expressions are outside this slice.

Logical `Sort` and physical `Sort` preserve the input's binding-aware output
shape. The executor resolves all key positions once, validates that each
non-NULL runtime value has the key's declared physical type, and then performs
a stable in-memory lexicographic sort. NULL placement is applied independently
of direction; direction reverses only ordinary non-NULL comparison. Stability
preserves input order among equal keys for the current plan, but does not
promise a permanent tie order if future access paths change. A caller that
requires a total order must provide enough keys.

## Typed global and grouped aggregates

Aggregate function names are contextual only in SELECT projection. A plain
identifier such as `count` remains a source column, while `COUNT(*)` or
`COUNT(column)` is an aggregate. Aggregate arguments are limited to `*` for
COUNT and qualified or unqualified source columns for all four functions. HIR
resolves column inputs and GROUP BY keys through the complete relation scope.
For grouping queries, every projected source column must be one of the
binding-aware group keys. A key need not be projected, GROUP BY may contain
multiple source columns, and GROUP BY without aggregates forms distinct groups.
Wildcard projection is rejected with GROUP BY. Grouped queries also reject
`ORDER BY` in this slice.

Normal and aggregate plans remain distinct:

```text
Scan/Join -> Filter -> Sort -> Project -> Limit
Scan/Join -> Filter -> Aggregate -> Limit
```

An `OutputField` separates source identity from result metadata. Source fields
retain their `RelationBindingId + TableId + ColumnId`; a `DerivedField` carries
only its deterministic name, semantic type, and nullability. Consequently,
`COUNT(*)` never receives fabricated catalog or query-source IDs. Logical and
physical Aggregate operators keep `group_keys` (group identity) separate from
ordered `AggregateOutput` items (result shape). A group-key output remains a
Source field and an aggregate remains Derived, so SELECT order is preserved
without disguising aggregation as projection or inventing identifiers.

The aggregate executor materializes its input once and updates every aggregate
state in one pass. It uses `HashMap<Vec<ScalarValue>, usize>` only for group
lookup and a `Vec<GroupState>` for deterministic first-seen output order;
randomized hash iteration never shapes results. Grouping is currently fully in
memory. With no group keys, zero input rows still form one implicit group:
COUNT returns zero, while SUM/MIN/MAX return NULL. With one or more keys, groups
are created only when rows arrive, so empty input produces zero rows. LIMIT is
above Aggregate and therefore limits complete result groups, never input rows.
Runtime key and aggregate values are checked against typed physical inputs and
SUM uses checked signed or unsigned addition.

Two executor-private global COUNT specializations avoid that generic row
materialization without changing physical plans. A direct `Aggregate →
SeqScan` whose nonempty outputs are all COUNT can request an exact Heap presence
summary when every scan column is consumed by a COUNT(column). This includes a
zero-column scan with one or more COUNT(*) outputs:

```text
direct Aggregate COUNT outputs
              ↓
       direct SeqScan[] ?
          /          \
        no            yes
        |              |
     generic   scan_presence_counts([])
                       ↓
              full row validation
                       ↓
              exact live_rows u128
                       ↓
         checked ordered COUNT outputs
```

Phase 7R adds no storage counter: it reuses the Phase 7M summary and performs
one checked SQL `u64` conversion against each output's exact `AggregateExpr`.
The summary is a current Heap traversal, not statistics, a row-count cache, an
index-only count, or an O(1) slot-header shortcut. Every persisted scalar is
still decoded and validated; tombstones, slot reuse, relocation, index and
ANALYZE pages, stale statistics, and reopen therefore retain exact current-row
semantics. An all-star aggregate over a nonempty SeqScan is rejected by the
specialization rather than guessed about.

Phase 7N additionally recognizes only `Aggregate → Filter → SeqScan`, with all
outputs COUNT and at least one COUNT(column). It splits the scan's source-order
columns into values needed by the predicate and NULL-presence bits needed by
COUNT, then consumes each completely validated live tuple synchronously:

```text
Aggregate COUNT outputs
        ↓
direct Filter → SeqScan eligible?
       / \
     no   yes
     |     |
 generic  predicate values + COUNT presence
                 ↓
        one validated Heap visitor scan
                 ↓
       dynamic borrowed leaf evaluation
                 ↓
        TRUE updates checked counts
                 ↓
          one materialized result row
```

The Heap visitor knows only ColumnIds, runtime scalar-view requests, and
presence requests; storage never receives relational `Expr` or SQL truth
semantics. It invokes the callback only after full row-codec validation.
Executor still applies three-valued logic, so only TRUE qualifies and FALSE or
UNKNOWN is discarded. Count-only Text never becomes an owned String.

Phase 7O changed only the private evaluator called by that filtered-count
consumer. Phase 7P adds one language-independent `ScalarRef<'a>` runtime view
in `netbadb-types` and makes the Heap decoder return it directly. This view is
not a persistent, wire, schema, or SQL IR type. Its Text variant can borrow the
validated record payload only during a higher-ranked synchronous callback.
Phase 7Q then reuses the existing Join `BoundExpr` to resolve predicate source
positions once before the Heap traversal:

```text
FilteredCountPlan
        ↓
source-order predicate fields
        ↓
bind_expression once
        ↓
BoundExpr checked positions
        ↓
persisted row payload
        ↓
decode + complete validation
        ↓
ScalarRef<'row>
  Bool/Int64/UInt64 by value
  Text(&str)
  Null
        ↓
HRTB synchronous visitor callback
        ↓
position-indexed bound evaluation
        ↓
borrowed Column/Literal scalar views
        ↓
ScalarRef binary/truth semantics
        ↓
owned computed Bool/NULL
        ↓
filtered COUNT summary
```

The page guard, validated page, and record payload remain alive for the whole
callback. The HRTB prevents safe code from storing the row-borrowed Text after
the callback returns. Scratch vectors are allocated once per validated Heap
page and reused across its live slots; there is no per-live-row vector
allocation and no unsafe lifetime manipulation. Callback invocation still
waits until every persisted scalar in the row has passed validation, including
completely unrequested trailing values.

The original owned visitor remains and delegates this traversal, converting
only requested views to `ScalarValue`. `EvaluatedScalar::Borrowed` stores a
copied `ScalarRef`; Binary, Unary, and IsNull results remain owned. Existing
ScalarValue binary, comparison, and truth helpers delegate one ScalarRef
semantic core. Phase 7Q generalizes the existing bound evaluator around a
checked ScalarRef getter: the Join wrapper adapts owned rows, while the
filtered-count wrapper adapts the callback slice with `get(position).copied()`.
The hot bound evaluator receives no fields and cannot call
`find_source_position`. Binding remains identity-aware by
`RelationBindingId + ColumnId`, missing fields and short rows remain typed
errors, and AND/OR still evaluate both sides.

Phase 7T makes the row-aware borrowed storage visitor authoritative. The older
borrowed visitor is a thin wrapper that ignores mutation identity, and the
owned visitor delegates through the borrowed traversal. Exact direct
`Filter → SeqScan` may
consume the row-aware boundary before child row ownership; every other Filter
continues to receive fully owned child `ExecutionRows`:

```text
validated live Heap row + opaque StorageRowHandle
        ↓
exact direct sequential Filter
        ↓
dynamic find_source_position per Column leaf
        ↓
borrowed persisted ScalarRef or Expr literal
        ↓
borrowed Column/Literal leaves
        ↓
ScalarRef binary/truth semantics
        ↓
owned computed Bool/NULL
       / \
 TRUE     FALSE/UNKNOWN
  ↓             ↓
own every       own nothing
SeqScan value
  ↓
ExecutionRow + opaque StorageRowHandle
```

Eligibility requires exact SeqScan input, unique scan source identities, every
scan identity to match its binding and table, and every predicate identity to
be present in the scan fields. Literal predicates are eligible. IndexScan,
RangeIndexScan, Join, Sort, nested Filter, and malformed shapes retain generic
execution, including the empty-input behavior that does not evaluate a missing
predicate field.

The dynamic evaluator deliberately does not prebind positions and AND/OR still
evaluate both sides. The callback saves the first predicate error and stops
later predicate evaluation, but returns success so storage still validates all
later rows. A later storage error therefore has the same priority as completing
the old owned child scan; after a successful traversal the saved predicate
error is returned. QueryResult remains fully owned, and no page pin or borrowed
persisted row escapes into executor state.

Phase 7U adds one executor-local consumer for exact
`Project → Filter → SeqScan`. It reuses the Phase 7T visitor, dynamic predicate
lookup, three-valued semantics, and deferred predicate-error handling, but
precomputes the Project's source positions once before traversal:

```text
validated complete borrowed SeqScan row
        ↓
dynamic Filter predicate
       / \
 TRUE     FALSE/UNKNOWN
  ↓             ↓
own only        own nothing
Project values
  ↓
ExecutionRow + opaque StorageRowHandle
```

The specialization requires at least one predicate-used scan column that is
not retained by Project. Every scan column must be used either by the
predicate or by Project, and every Project source must resolve by
`RelationBindingId + ColumnId`. Duplicate and reordered Project sources retain
their exact output semantics, including independent owned Text duplicates;
zero-width output retains the qualifying row handle with no scalar ownership.
Unused scan columns, missing sources, duplicate/mismatched scan identities,
nested Filter, Sort, Join, index scans, and future shapes fall back to generic
execution.

The complete persisted row is still decoded and validated before predicate
evaluation. A retained Text value is necessarily owned at the fully owned
QueryResult boundary; when all predicate columns are also retained, the
specialization deliberately does not apply. UPDATE and DELETE continue to use
the Phase 7T direct Filter path and mutate only after `execute_rows` succeeds.
Assignment evaluation, index maintenance, transactions, INSERT, Join
algorithms, Phase 7N filtered-count precedence, planner, compiler, Rel IR,
PhysicalPlan, protocol, and inspection behavior are unchanged. There is no
generic Filter prebinding, predicate pushdown, expression bytecode/compiler,
planner rewrite, dependency, or unsafe code.

Grouped, mixed-function, all-star-only, nested, join, sort, and index-backed
shapes retain the generic aggregate path.

`ScalarValue` equality and hashing are used only for current-process group
lookup. NULL equals NULL for grouping, so all NULLs at the same key position
share a group; this is deliberately different from SQL expression equality,
where `NULL = NULL` remains UNKNOWN. Scalar hashing is not a persistent format,
schema fingerprint, WAL, page, or compatibility contract. Although first-seen
order is deterministic for the current executor, SQL queries without ORDER BY
do not guarantee row order.

Aggregate type and NULL rules are:

- `COUNT(*)` counts every row; `COUNT(column)` ignores NULL. Both return a
  non-null physical `UInt64`.
- `SUM` accepts only `Int64` and `UInt64`, ignores NULL, and is nullable because
  empty/all-NULL input returns NULL. Its result is an unnamed physical numeric
  type even when the input is nominal, because a sum is not one input identity.
- `MIN` and `MAX` accept Bool, Int64, UInt64, and Text using the existing value
  comparison, ignore NULL, and are nullable for empty/all-NULL input. They
  preserve the input `SemanticType` because the result is an input value.

There is no HAVING, DISTINCT aggregate, alias, GROUP BY expression, nested
aggregate, aggregate-aware ordering, GROUPING SETS, ROLLUP, or CUBE.

## Typed expressions and NULL

Database NULL is an explicit `ScalarValue::Null`; it is not represented by
Rust `Option<ScalarValue>`. `Option` continues to mean that syntax or metadata,
such as a `WHERE` clause, is absent. Parser NULL literals begin untyped. HIR
assigns them a semantic type from the surrounding boolean or comparison
context without granting NULL an arbitrary nominal identity.

HIR and relational expressions carry an expression type consisting of:

```text
SemanticType + nullable
```

Column nullability originates in Canonical Schema IR. Literal values other than
NULL are non-nullable. A comparison is nullable when either operand is
nullable, boolean `AND`/`OR`/`NOT` preserve possible UNKNOWN results, and
`IS NULL`/`IS NOT NULL` always produce a non-null BOOL. This expression
nullability is distinct from schema nullability: a nullable column may contain
NULL, while an expression over that column may or may not return NULL.

`IS NULL` and `IS NOT NULL` remain explicit AST, HIR, and relational nodes;
they are not lowered to equality with NULL. Ordinary `=`, `!=`, `<`, `<=`,
`>`, and `>=` comparisons return UNKNOWN if either operand is NULL, including
`NULL = NULL`. Nominal compatibility checks still apply to non-NULL operands,
so contextual NULL typing cannot make `UserId = TeamId` legal.

The executor centralizes boolean conversion as three truth values:

```text
Bool(true)  → TRUE
Bool(false) → FALSE
NULL        → UNKNOWN
```

`AND`, `OR`, and `NOT` use the SQL three-valued truth tables. A filter keeps a
row only when its predicate is TRUE; FALSE and UNKNOWN both reject the row.
Storage's existing scalar tag for NULL round-trips through heap pages, buffers,
the database file, and reopen. `HeapStorage` validates every embedded write and
returns `StorageError::NullNotAllowed` when NULL targets a non-nullable column,
independently of query compilation.

## Typed DML

The parser's top level is a typed `Statement` enum with distinct Select,
Insert, Update, and Delete variants. The HIR resolves every table and column to
stable IDs, assigns expression types, rejects duplicate targets and invalid
NULL/nominal assignments, and fills omitted nullable INSERT columns with NULL.
Logical and physical statement enums preserve that distinction. UPDATE and
DELETE select targets through the existing sequential Scan + optional Filter
tree rather than embedding a second predicate implementation.

Execution scan tuples carry a hidden, opaque `StorageRowHandle` alongside
values. Projection can discard SQL-visible columns without manufacturing a
`_rowid` feature, and executor cannot inspect Heap PageId/SlotId details. DML
collects all selected targets before mutation, avoiding scan interference when
a page is compacted. UPDATE evaluates every assignment against the original
row and constructs one complete replacement, so `SET a = b, b = a` swaps the
values. The shared three-valued evaluator modifies only TRUE rows; FALSE and
UNKNOWN are skipped.

`ExecutionResult` distinguishes query rows from `AffectedRows(u64)`. INSERT
returns one; UPDATE counts selected rows, including same-value assignments;
DELETE counts versions logically expired. `Database::execute` wraps one DML
statement in an implicit transaction. `Database::execute_in` uses an explicit
transaction and permits reads of the transaction's currently buffered writes.
Because savepoints do not exist, an execution-time mutating-statement failure
rolls back the whole explicit transaction.

Heap mutation remains below SQL semantics. `insert_in`, `update_in`, and
`delete_in` validate the transaction and full row, build a candidate page,
append the existing full-page before/after-image `PageUpdate`, assign pageLSN,
and only then install the dirty page. Runtime rollback and startup recovery
therefore need no DML-specific undo or WAL record type. A mid-statement error
causes the owning transaction to restore all preceding page images.

## Physical storage identity and database transactions

Logical partition and physical identity are separate:

```text
Schema TableId (logical SQL relation)
    ↓ TablePlacement
Single ───────────────────────────────→ StorageId
RangePartitioned
    ↓
PartitionId (stable logical partition) → StorageId
    ↓ deterministic StorageRegistry
TableStorage
```

`TableId` remains SQL/catalog identity and authorization continues to use it.
`PartitionId` identifies a durable logical physical partition and is not a
range-vector index or path. `StorageId` identifies one physical storage instance. Heap metadata v5 stores
it as a nonzero little-endian value, so close/reopen, process restart, pathname
changes, and catalog reorder preserve identity. It is never a Vec index,
pointer, file descriptor, or pathname. `Single` maps one table to one storage;
`RangePartitioned` resolves one logical table through ordered PartitionIds to
several StorageIds without redefining SQL identity.

PartitionCatalog v1 is an immutable, explicit database-level file with its own
path, magic/version, little-endian fields, bounded counts, schema fingerprints,
and CRC32C. It stores the partition key and ordered `[lower, upper)` Int64 or
UInt64 bounds. Bounds may be unbounded and gaps are legal; overlap, empty
ranges, nullable keys, wrong types, duplicate identities, missing storages,
and schema mismatch are hard typed errors. Paths are open-time materialization
inputs only: reopen matches heap-persisted StorageIds, so reorder or move does
not change partition identity.

Planning consumes a pure range snapshot and never reads the catalog file.
Nested AND comparisons (`=`, `<`, `<=`, `>`, `>=`, including reversed
operands) produce an exact integer interval. OR/NOT conservatively select every
partition. A contradiction produces an empty `PartitionedScan` with the normal
output schema. Each selected partition then chooses its own local SeqScan,
IndexScan, or RangeIndexScan; the complete residual Filter remains above the
scan. Execution concatenates partitions in canonical range order and retains
required-column propagation. Global indexes do not exist.

INSERT evaluates and validates its typed row before routing. UPDATE and DELETE
materialize every original target first; UPDATE also evaluates every
replacement and resolves every destination before the first mutation. A
cross-partition UPDATE is one source delete plus one destination insert in the
same `DatabaseTransaction`, consuming the old physical handle and creating a
new one. Existing Prepare/CommitDecision recovery therefore makes
multi-partition INSERT, DELETE, and row movement all-or-nothing across crashes.
`StorageRowHandle` validates its opaque owning StorageId so partitions of the
same TableId cannot exchange physical locators.

`DatabaseTransaction` owns database-level identity, isolation intent,
lifecycle, and a deterministic participant set. Its `DatabaseTxnId` is
distinct from each Heap WAL `TxnId`; coordinator history plus prepared WAL
records provide the restart high-water mark so a still-relevant identity is
never reused.
Participants are registered on first access and contain:

```text
StorageId + Read/Write mode + StorageTransaction
```

Read participants do not acquire the Heap writer lease. A participant may
upgrade Read → Write. Legacy create/open APIs permit multiple readers and one
writer. Explicit coordinator-enabled APIs accept a separate coordinator-log
path and permit several local write participants. One writer retains the direct
physical commit fast path; two or more use:

```text
Prepare every participant (durable)
    ↓
CommitDecision(DatabaseTxnId, sorted StorageId + physical TxnId set) + sync
    ↓                         GLOBAL COMMIT POINT
Commit every prepared participant (durable)
    ↓
Complete(DatabaseTxnId) + sync
```

Before the global point, rollback durably aborts and undoes every participant.
After it—or after a decision sync with an uncertain result—rollback is a typed
error and only commit retry is legal. Append and sync retries reuse the same
DatabaseTxnId and canonical participant set. Read-only and single-writer
transactions never write the coordinator log.

Every explicit query builds one `DatabaseReadView` owned by the database
transaction and containing the `StorageReadView` adapters for all StorageIds
used by that statement. Executor receives explicit Single TableId→StorageId
bindings plus StorageId-explicit partition scan scopes,
StorageId-tagged storages, and StorageId-tagged views; it never correlates
parallel vectors by position. Current Heap status stores have independent
CommitSeq domains, so `DatabaseReadView` owns logical transaction/isolation
context without inventing a false database-global timestamp.

Coordinator-enabled startup scans and validates the independent log before
opening any participant for recovery. Exact prepared mappings with a
CommitDecision commit; prepared mappings without one abort under presumed
abort. Missing decision participants, extra/mismatched prepared participants,
and corrupt coordinator bytes fail the entire database open. A standalone Heap
open never guesses an in-doubt outcome and returns a typed resolution-required
error. Complete is appended only after every participant commit is durable.
Checkpoint and close retain their quiescent rule, so prepared/in-doubt state is
rejected rather than recycling required WAL.

The coordinator log is append-only in this phase; GC/checkpoint is deferred.
HASH/LIST/DEFAULT partitioning, partition DDL/split/merge, global indexes,
remote placement, Columnar, Raft, replication, and distributed transactions
are not implemented.

## Storage boundary

The synchronous storage path is now:

```text
Executor
    ↓
DatabaseTransaction / DatabaseReadView
    ↓
PhysicalBindings: TableId → TablePlacement
                         ├─ Single → StorageId
                         └─ Range → PartitionId → StorageId
    ↓
StorageRegistry
    ↓
TableStorage capability API
    ├─ TableStorage::Heap(HeapStorage)
    │      ├─ registered BTree access methods
    │      └─ TransactionManager + Heap WAL
    │             ↓
    │         BufferPool + PageGuards → PageManager → database file
    └─ TableStorage::Lsm(LsmStorage)
           ├─ ordered MemTable + transaction-local overlay
           ├─ LSM WAL v1
           └─ Manifest v2 selects Bloom-bearing immutable SSTables
                    ├─ L0 overlapping
                    ├─ L1 non-overlapping
                    ├─ L2 non-overlapping
                    └─ L3 non-overlapping
```

`netbadb-storage` keeps the boundaries concrete and small:

```text
                 DatabaseTransaction
                        │
                 CoordinatorLog
                  /           \
             Heap Txn        LSM Txn
                │              │
            Heap WAL        LSM WAL
```

- `TableStorage` is the database composition boundary and has two real
  variants, `Heap` and `Lsm`. It uses static enum dispatch; there is no empty
  Columnar placeholder and no giant `dyn StorageEngine` interface. B+Tree is
  an access method owned by Heap, while the LSM clustering order is its native
  point/range access path and is not represented by a fake BTree handle.
- `StorageRowHandle` is an opaque, storage-scoped executor mutation identity.
  Heap carries its generation-safe RowId. LSM carries a stable `LsmRowId`, the
  observed visible version, and current clustering key for stale-handle and
  key-moving UPDATE validation. Executor, planner, Rel IR, and SQL inspect
  neither representation. `StorageReadView` and `StorageTransaction` likewise
  keep each engine's MVCC and durability state below the boundary.
- `Database::storage_kind` and `Database::inspect_lsm_storage` expose embedded,
  read-only physical inspection including clustering identity, MemTable/SSTable
  entries, per-level counts/bytes, Bloom bytes, amplification counters, and
  last-`ANALYZE` row/min/max statistics. Deployment
  manifest v4 still bootstraps Heap only, so the offline CLI and Inspection JSON
  v4 remain unchanged; LSM CLI/server bootstrap is deliberately deferred.

LSM maintenance is synchronous and quiescent. `compact()` deterministically
drives L0-count and L1/L2-size triggers, selects complete source/target overlap
closure, preserves every MVCC version and tombstone, and atomically publishes
all split outputs. `compact_full()` covers every level and is the only path
that removes superseded history or tombstones. Per-SSTable Bloom filters index
all represented clustering keys, including tombstones; point reads use range
routing then Bloom, while range reads use range/block metadata only. Both feed
a bounded block-at-a-time k-way merge.
- The capability API covers projected scans, point/range access, borrowed row
  visitors, an owned row consumer with typed `ControlFlow` cancellation,
  presence summaries, and mutation. Heap dispatch delegates to its
  validated-once row traversal. LSM merges MemTable, SSTable, and bounded
  transaction-overlay state in physical-key order and decodes one visible row
  at a time. Neither engine constructs executor batches or knows Filter,
  Project, Limit, SQL expressions, or PhysicalPlan.

Above that storage boundary, the executor recognizes only physical trees made
from SeqScan plus Filter, Project, and Limit. It groups owned rows into a
private 256-row `ExecutionBatch`, evaluates a position-bound `BoundExpr`,
applies the existing move-aware `ProjectionPlan`, and tracks Limit state across
batches. Limit cancellation stops the storage consumer after the current
bounded batch; later physical rows are deliberately not requested. This is an
execution behavior and not a whole-file integrity check: every row actually
requested still receives the engine's complete MVCC, page/SSTable, codec,
type, NULL, and UTF-8 validation.

The public `QueryResult` remains fully owned and may contain the complete final
result. Intermediate SeqScan, Filter, and Project results no longer require a
full base-scan vector. Exact predicate-only Text Project/Filter retains the
measured borrowed Phase 7U specialization so rejected strings are not owned.
Direct COUNT specializations also remain. Sort, Aggregate, joins, index/range
scans, partition scans, and DML deterministically use the authoritative legacy
executor for the complete tree. PhysicalPlan and Inspection JSON are unchanged,
and executor dispatch contains no Heap/LSM branch.

- `PageManager` owns fixed-size file I/O, page allocation, checked page-offset
  arithmetic, and file sync. It does not interpret heap or index semantics.
- `BufferPool` owns a bounded set of raw page frames. It uses a simple
  round-robin eviction boundary, pins pages while guards are alive, refuses to
  evict pinned pages, and exposes explicit `flush_page`/`flush_all`
  operations. Before writing a dirty data page it makes the WAL durable
  through that page's pageLSN. The data-page write is not attempted if the WAL
  flush fails.
- `Page` validates a versioned page header, PageId-bound checksum, and explicit
  page type before exposing slotted-page operations. Heap pages use a slot
  directory at the front, free space in the middle, and tuple bytes packed
  from the end of the page backward.
- `TransactionManager` allocates strong `TxnId` values, appends `Begin`, and
  owns the per-open-database writer/health state and durable status store. A transaction tracks
  `Active`, `RollbackRequired`, `CommitPending`, `RollbackPending`, `Committed`,
  or `RolledBack` and owns its last LSN. Writer ownership is acquired lazily
  before the first heap mutation; read-only transactions do not reserve it.
  Read Committed statements capture fresh ReadViews, while Repeatable Read pins
  its first ReadView until transaction completion.
- `HeapStorage` validates and encodes rows, constructs a candidate after-image,
  appends its `PageUpdate`, and only then publishes the page to the buffer
  frame. It no longer flushes the entire buffer after each insert. Page guards
  do not escape these operations, so executor and query APIs carry no page
  lifetimes.
- `netbadb-index` is the storage-independent B+Tree domain layer. It owns
  `IndexSpec`, explicit key/RowId ordering, nodes, versioned codecs, and
  byte-balanced split calculation; it has no dependency on storage, WAL, SQL,
  planner, or executor. `netbadb-storage::BTree` owns page traversal,
  allocation, transaction/WAL ordering, publication, and recovery integration.

Heap sequential scan uses one immutable validation proof per buffered page:

```text
Disk / Buffer Page
        ↓
one authoritative Page::header full validation
        ↓
ValidatedPage<'_> borrowing the immutable Page
        ↓
checked slot lookup and borrowed live-record slices
        ↓
unchanged row codec and owned ScalarValue row
```

`ValidatedPage` is crate-private and can only be created through the existing
full `Page::header` validation. Its lifetime prevents mutable access to the
borrowed `Page` while the stored `PageHeader` and structural proof are reused.
It is not a persistent trust bit, a validation cache in `Page`, or an on-disk
marker. Live-record slicing still uses checked ranges; invalid slots return a
typed error, and checksum, generation, record-bound, free-space, and overlap
corruption fails before traversal begins. Public Page operations retain their
existing validation semantics. Non-Heap pages encountered in a table file
still pass their existing single-payload validation before Heap scan skips
them.

The experimental container retains the legacy `NBPG` file-root marker. Heap
metadata has its own `NBD1` marker and version 4 little-endian layout inside
the header page:

```text
16..20  NBD1 heap metadata magic
20..22  u16 heap metadata version (4)
22..24  reserved bytes (zero)
24..32  u64 table ID
32..34  u16 declared column count
34..66  SHA-256 canonical table-schema fingerprint
66..74  u64 IndexCatalog root PageId
74..80  reserved bytes (zero)
```

Create validates the complete table before creating the WAL or heap file. Open
validates metadata and schema identity before recovery can mutate storage, then
checks it again after recovery. A table-ID mismatch and a schema-fingerprint
mismatch are distinct typed storage errors. Heap metadata versions 1 through 3 are
rejected without migration; the file format remains experimental and may
change again between versions. New files reserve page 1 for the empty catalog
root and page 2 for the initial Heap page.

Page 0 is legacy container/heap metadata and is not interpreted as a Page v5
data page. Data pages use the following version 5 little-endian layout:

```text
0..4    NBP1 page magic
4..6    u16 page format version (5)
6       u8 page type (2 heap, 3 BTreeMeta, 4 BTreeInternal, 5 BTreeLeaf,
        6 IndexCatalog; tag 1 remains reserved)
7       reserved byte (zero)
8..10   u16 slot count
10..12  u16 free-space lower bound
12..14  u16 free-space upper bound
14..16  reserved bytes (zero)
16..24  u64 pageLSN (zero means no WAL record)
24..28  u32 CRC32C (little-endian)
28..    8-byte slot entries: u16 offset + u16 length + u32 generation
...     free space
...     tuple bytes, allocated from PAGE_SIZE backward
```

The Page v5 checksum is `CRC32C(page_id_le_u64 || complete_page_image)`, with
bytes 24..28 of the page image treated as zero. It covers all 4096 bytes,
including the header, pageLSN, slot directory, free/unused bytes, tombstones,
and tuple payload. Binding the expected logical PageId also detects a valid
page block read from the wrong physical page position. Magic and version are
checked before CRC32C so an old page reports its explicit unsupported version;
checksum verification then precedes all remaining semantic validation. The
all-zero new-page before-image remains a WAL sentinel, not a valid persisted
data page.

Every allocated slot has a nonzero little-endian `u32` generation. A live slot
stores its checked offset and length; zero-length records remain legal and use
their real offset. The reserved pair `(offset = 0, length = 65535)` means
Deleted and retains the generation. Either reserved component without the
complete pair, or generation zero, is typed corruption. Normal DELETE and
UPDATE do not create Page tombstones: they update MVCC tuple headers and UPDATE
inserts a new physical version. Manual vacuum rebuilds affected pages and marks
only horizon-dead versions Deleted. INSERT deterministically reuses the lowest deleted SlotId whose generation
is below `u32::MAX`, increments it with checked arithmetic, and otherwise
appends a generation-1 slot. A generation-maximum tombstone is permanently
ineligible, so generation can never wrap.

Heap insertion performs a deterministic linear first-fit search from the
lowest data PageId. Each candidate is cloned and passed to
`Page::insert_record`; only `PageFull` advances the search, while corruption or
other errors fail immediately. If no existing page accepts the tuple, the
existing WAL-before-file-extension protocol allocates a new page. This is
intentionally O(number of heap pages); there is no persistent free-space map.

UPDATE inserts its replacement through normal deterministic first-fit, then
expires the predecessor with `xmax/cmax` and a next-version RowId. Both page
images share the caller transaction and full-page WAL chain. The replacement
always has a distinct RowId, even if both versions occupy one page.

`RowId` is the versioned physical locator `PageId + SlotId + generation`, not a
business key, primary key, or globally monotonic identifier. Before vacuum, an
old-version locator still names a checked physical tuple whose visibility is
decided by its ReadView. Vacuum turns dead versions into tombstones; after slot
reuse, the old generation reports `StaleRowId` before live/deleted state is
considered. Scans return the persisted generation, so executor UPDATE/DELETE
retain the complete candidate locator.

Version 3 intentionally changed the meaning of a formerly invalid slot pair;
version 4 added data-page integrity without moving existing header fields;
version 5 adds explicit slot generation. It replaces the pre-Foundation
sequential `HEAP` layout and page versions 1 through 4. These experimental
formats have no migration path and are rejected rather than reinterpreted.

Every live Heap slot payload starts with this fixed 48-byte little-endian MVCC
header before the existing typed row encoding:

```text
0..4    NBMV tuple magic
4..6    u16 tuple format version (1)
6..8    u16 presence flags for xmax, cmax, and next version
8..16   u64 xmin TxnId (non-zero)
16..24  u64 xmax TxnId (zero only when absent)
24..28  u32 cmin CommandId (non-zero)
28..32  u32 cmax CommandId (zero only when absent)
32..40  u64 next-version PageId
40..44  u32 next-version SlotId (checked to u16)
44..48  u32 next-version generation
48..    typed row payload
```

Presence bits and zero fields must agree; `xmax` and `cmax` are either both
present or both absent, and an optional version pointer has no zero component.
Malformed magic, flags, widths, reserved absence encodings, transaction IDs,
command IDs, or pointers are typed storage errors. Heap metadata v4 is the
compatibility boundary requiring this tuple format; legacy unversioned row
payloads are not guessed or migrated.

## Persistent B+Tree boundary

One table database file may interleave Heap and B+Tree pages. Heap scans and
first-fit allocation validate every page, process only Heap pages, and skip
valid index pages. A Heap scan performs that complete page-wide validation once
and reuses it only for the lifetime of the page's immutable borrow. RowId
read/update/delete requires a Heap page, so a locator
for an index page is rejected. B+Tree allocation uses the same PageManager,
buffer pool, transaction manager, WAL generation, recovery, and checkpoint as
heap mutation; there is no second file or durability domain.

Every index page is a normal checksummed Page v5 with exactly one live slot 0,
generation 1. The payload has its own version-1 semantic codec:

- `NBTM` metadata: stable `BTreeHandle` page, current root PageId, height,
  physical plus optional nominal semantic type, and nullability;
- `NBTL` leaf: sorted full `(ScalarValue, RowId)` entries and optional next-leaf
  PageId;
- `NBTI` internal: first child plus sorted persistent lower-bound fence keys
  and right children. A fence's RowId is only an ordering token: it need not
  identify a currently live heap row or leaf entry. Deleting the first live
  entry in a right subtree therefore does not rewrite or enlarge its fence.

All integers are fixed-width little-endian. Decoders reject wrong magic or
version, nonzero reserved fields, invalid UTF-8/type/value tags, zero child
pages, invalid RowIds, impossible counts, truncation, trailing bytes, and
non-increasing entries. Traversal is bounded by persisted height and validates
each PageId against current page count and each expected node kind. The page
CRC catches raw corruption first; independently tested node decoders catch
semantic corruption after a valid CRC is recomputed.

Key order is NULL first, then native Bool/Int64/UInt64/UTF-8 Text value order.
The tie-break is explicitly PageId, SlotId, generation; `RowId` itself does not
gain a persistent `Ord` contract. Duplicate values are legal and point lookup
returns all matching RowIds in tie-break order. An exact `(key, RowId)` repeat
is `DuplicateEntry`. `IndexSpec` validates runtime physical type, nullability,
and persists optional nominal identity, although nominal identity does not
change physical comparison.

Insertion descends without retaining guards, allowing a buffer pool capacity
of one. Overflow splits deterministically by encoded byte size, updates leaf
links, propagates complete separator entries, splits internal nodes, and uses a
new root plus metadata update when height grows. One compound mutation logs
full-page images in deterministic new-right/existing-left/ancestor/root/meta
order. If it creates pages, all corresponding WAL records become durable before
the first file extension. Any failure after the first PageUpdate makes the
transaction `RollbackRequired`; runtime rollback and startup loser undo restore
existing pages and remove trailing new pages in reverse order.

Exact `(key, RowId)` deletion uses a soft half-capacity encoded-byte threshold
to attempt deterministic right-first merges, falling back to the left sibling
for the last child. It never redistributes entries. A merge occurs only when
the actual encoded leaf or internal payload fits; otherwise a sparse node
remains valid. Parent separator removal recurses upward, and a zero-separator
root collapses. The surviving physical page is always the left page, repairing
the forward leaf chain without a predecessor lookup. All final page images are
preflighted before WAL publication and logged bottom-up with metadata last.
Delete never allocates or shrinks the file. Removed right pages and old roots
remain valid but unreachable orphan index pages; reclamation is deferred.

Phase 4C2 deliberately has no sibling redistribution or orphan-page
reclamation. Uniqueness, SQL index DDL, and range lookup remain deferred.

## Persistent index registry

Heap metadata v4 points to a fixed `IndexCatalog` root. Catalog pages are
ordinary checksummed Page v5 single-payload pages containing version-2 `NBIC`
payloads; v1 is rejected without migration. They form an append-only,
cycle-checked linked chain in creation order. Overflow logs the new catalog
page before the old tail link, flushes through both records before extending
the file, and therefore follows the existing reverse-order rollback contract.

The v2 payload is fixed-width and little-endian:

```text
header (48 bytes)
0..4    NBIC magic
4..6    u16 version (2)
6       u8 table-statistics presence (0 or 1)
7       reserved zero
8..16   u64 next catalog PageId (0 means none)
16..20  u32 entry count
20..24  reserved zero
24..32  u64 row_count (zero when absent)
32..40  u64 managed_page_count (zero when absent)
40..48  reserved zero

entry (40 bytes)
0..4    u32 ColumnId
4       u8 index-statistics presence (0 or 1)
5..8    reserved zero
8..16   u64 BTree metadata PageId
16..24  u64 distinct_non_null_keys (zero when absent)
24..32  u64 null_count (zero when absent)
32..36  u32 tree_height (zero when absent)
36..40  reserved zero
```

Only the root may contain table statistics. Index statistics on any page
require root table statistics. Loading checks nonzero managed-page count, NULL
count at most row count, a valid distinct count for the non-NULL population,
and tree height at least one. It deliberately does not compare a persisted
snapshot with current rows or current tree height.

Phase 4F changed only IndexCatalog v1 to v2. The later MVCC phase changes Heap
metadata to v4 and tuple payloads to `NBMV` v1; Canonical Schema v1, Page v5,
BTree payload v1, WAL v3, and WAL record v2 remain unchanged.

A registered table index is distinct from a raw tree created through
`HeapStorage::btree().create`: raw trees are never discovered by scanning page
types. Open follows only the metadata root, rejects cycles, duplicates,
out-of-range links, and wrong page kinds, then verifies every column against
the canonical `TableDef` and every BTree metadata `IndexSpec` against that
column's nominal type and nullability. Validated definitions and optional
optimizer snapshots are cached in persistent creation order. BTree roots
remain uncached; analyzed height is a snapshot, not authoritative metadata.

`HeapStorage::create_index` owns one transaction and single-writer lease. It
creates the tree, materializes the current live heap rows, backfills every
typed `(value, RowId)` entry, and writes the catalog registration as the final
logical mutation. Only a successful durable commit updates the in-memory
registry, so crashes or errors before commit leave no visible partial index.
For every physical Heap version that has not been vacuumed and every registered
index, one candidate entry `(version[column], version RowId)` exists. Raw
B+Trees are outside this invariant. INSERT and UPDATE publish a new Heap version
before inserting its candidates in persistent creation order. UPDATE and DELETE
leave predecessor candidates intact because an older snapshot may still need
them. Manual vacuum removes each dead version's exact candidates before
physically tombstoning its Heap record. All operations share the caller's
transaction, buffer pool, WAL, and prevLSN chain. Candidate key-size and exact
predecessor-entry checks run while holding the single-writer lease before the
first physical mutation; any later failure marks the transaction
`RollbackRequired`.

## ANALYZE snapshots and costed index access

Logical plans remain storage-independent: query meaning is still represented
as `Filter(Scan)`, never as a logical index operator. Core walks table storage
order and each storage's advertised access paths to materialize plain planning
context. Entries contain table/column identity, opaque table-scoped
`AccessPathId`, point/range capabilities, and optional
`TableStatistics`/`IndexStatistics` domain values. The planner depends on the
pure `netbadb-index` domain crate for typed ranges and statistics, not on
`netbadb-storage`, and receives no BTreeHandle, PageId, WAL, buffer, or catalog
representation.

An access path may additionally provide storage-neutral integer cost hints:
point-probe base work, expected point I/O, range startup work, and sequential
work per estimated match. Heap keeps the historical height-based default. LSM
derives hints from overlapping L0 files, nonempty L1+ levels, and a fixed
conservative Bloom false-positive expectation. The planner does not match
`StorageKind`, read Bloom bits, or perform storage I/O.

For a Filter directly above a Scan, point access recognizes
`indexed_column = non-NULL literal`, its commuted form, and nullable-column
`IS NULL`. Bounded range access recognizes and tightens `>`, `>=`, `<`, and
`<=` literal comparisons through nested AND, including reversed operands, for
Int64/UInt64 indexes only. It never derives access through OR or NOT. Equality
with NULL, IS NOT NULL, one-sided, Text/Bool, and column-to-column ranges,
joins, and index intersection/union are not range access paths.

`HeapStorage::analyze` owns one transaction and writer lease, scans the Heap
once for all registered indexes, reads each BTree's actual height, rewrites
every catalog page at most once in root-to-tail WAL order, commits, and only
then replaces the cache. `Database::analyze(table_id)` exposes that operation.
Table statistics contain live row count and all managed pages (`page_count -
1`); index statistics contain distinct non-NULL keys, NULL count, and analyzed
tree height. DML neither maintains nor invalidates these values. They are
optimizer snapshots and may become stale.

Without table statistics, or when all eligible indexes lack statistics, the
first eligible point path wins exactly as in Phase 4E; ranges require costable
statistics. With a table snapshot, analyzed eligible point and range candidates
are compared with SeqScan using integer `u128` page-visit estimates:

```text
SeqScan             = managed_page_count
Point IndexScan     = 1 + tree_height + estimated_matches
equality matches    = ceil((row_count - null_count) / distinct_non_null_keys)
IS NULL matches     = null_count
RangeIndexScan      = 1 + tree_height + estimated_range_matches
possible keys       = exact discrete count from the two integer bounds
range matches       = min(non_null_rows, possible_keys * average_duplicates)
```

IndexScan must be strictly cheaper; a tie selects SeqScan. Equal index costs
preserve registration order. If the only eligible index is unknown it retains
the Phase 4E fallback. These choices apply identically to SELECT, UPDATE, and
DELETE.

The physical shape always retains the complete predicate:

```text
PhysicalPlan::Filter(original predicate)
    -> costed access selection
         -> PhysicalPlan::SeqScan
         -> PhysicalPlan::IndexScan(opaque AccessPathId, exact key)
         -> PhysicalPlan::RangeIndexScan(opaque AccessPathId, typed bounds)
```

Executor passes the opaque access identity and typed key/range to TableStorage.
The current Heap variant resolves it only against its registered B+Trees; point
lookup performs `BTree::lookup`, while range lookup locates the lower leaf and
traverses ordered `next_leaf` links. Both treat returned RowIds as candidates,
apply the statement's same ReadView, and return opaque `StorageRowHandle`
values. Invisible versions are skipped; an unknown access path, stale/missing
locator, wrong page, or corruption remains an error and execution never hides
it by falling back to SeqScan. UPDATE and DELETE therefore finish access
traversal and
target materialization before maintaining the same index, avoiding iterator
invalidation or revisiting newly inserted keys. Because the complete Filter
remains, stale statistics can affect performance and plan shape but not query
semantics. The range estimate needs no min/max, histogram, or MCV and changes
neither BTree payload v1 nor IndexCatalog v2. Index-only scans, one-sided and
Text/Bool range costing, histograms/MCVs, index intersection/union, index
nested-loop joins, join ordering, and sort elimination remain future work.

## Transaction and WAL boundary

The WAL uses two alternating files, `<database>-wal` and
`<database>-wal.next`. Appending writes a complete record to the selected
generation but does not imply durability. `WalManager` separately tracks the
highest written and durable logical LSN, and `flush_through` advances durability
with `sync_data`. LSN zero is reserved for “no LSN”. Physical WAL offsets and
logical LSNs are deliberately different:

```text
logical LSN = generation base_lsn + (physical record offset - 48)
```

The first generation starts at logical LSN 1. A new generation's base is the
old generation's logical end, which is strictly greater than every record LSN
that existed there. Physical offsets can therefore restart at byte 48 without
making historical pageLSNs incomparable or reusable.

MVCC completion state is stored separately in `<database>-txn-status`. The
append-only file has a checksummed 16-byte header followed by checksummed
32-byte fixed records:

```text
header
0..4    NBTS magic
4..6    u16 status format version (1)
6..8    u16 header size (16)
8..12   reserved zero
12..16  u32 CRC32C

record
0..4    TXST magic
4..6    u16 record version (1)
6       u8 status (Committed=1, Aborted=2)
7       reserved zero
8..16   u64 TxnId (non-zero)
16..24  u64 CommitSeq (non-zero only for Committed)
24..28  u32 CRC32C
28..32  reserved zero
```

Active state is runtime-only. A tuple that references neither a durable status
nor a transaction active in this process is corruption, not implicitly aborted.
The status file is synced for each terminal record and rejects truncation,
unknown tags, conflicting duplicate decisions, nonzero reserved fields, and
checksum failures.

`Snapshot` consists of `visible_csn`, optional own `TxnId`, and a nonzero
statement `CommandId`. The visible CSN is the greatest durably published commit
sequence at capture. Read Committed captures it per statement; Repeatable Read
pins the first capture. Insertion is visible when its owner committed no later
than the snapshot, or it is the reader's own transaction with `cmin` no later
than the statement. Deletion/expiration hides a version only when `xmax`
committed no later than the snapshot, or it is the reader's own transaction
with `cmax` no later than the statement. Active and aborted inserters are
invisible; active and aborted expiring transactions leave the predecessor
visible. Sequential scans, selective/borrowed fast paths, direct counts,
point/range index candidates, and RowId fetches all call this rule.

The WAL file header is:

```text
0..4    NBWL magic
4..6    u16 WAL format version (3)
6..8    u16 header size (48)
8..16   u64 generation ID (starts at 1)
16..24  u64 base logical LSN (non-zero)
24..32  u64 checkpoint LSN (zero means no prior checkpoint)
32..40  u64 next transaction ID high-water mark (non-zero)
40..44  u32 CRC32C (little-endian)
44..48  reserved bytes (zero)
```

The header checksum covers all 48 bytes with bytes 40..44 treated as zero.
Magic, version, header size, and reserved bytes are checked before CRC32C; only
after checksum verification are generation, base LSN, checkpoint LSN, and the
transaction high-water mark trusted. WAL versions 1 and 2 are rejected
explicitly; this experimental format has no migration framework. PageUpdate
remains a pair of complete page images. The separately versioned data-page
format is version 5; the WAL and record formats remain versions 3 and 2.

Every record has a 40-byte fixed header followed by a bounded payload:

```text
0..4    WREC magic
4..6    u16 record format version (2)
6       u8 record type (Begin=1, PageUpdate=2, Commit=3, Abort=4,
                        RollbackComplete=5)
7       reserved byte (zero)
8..12   u32 total record length
12..16  u32 CRC32C (little-endian)
16..24  u64 logical LSN
24..32  u64 transaction ID
32..40  u64 prevLSN (zero only for Begin)
40..    payload
```

`Begin`, `Commit`, `Abort`, and `RollbackComplete` have no payload.
`PageUpdate` stores an explicit u64 page ID, one 4 KiB before-image, and one 4
KiB after-image. Consequently, the maximum accepted record is 8,240 bytes. The
record type determines the only valid total length; there is no stored payload
length. The CRC32C covers the complete header and payload with bytes 12..16
treated as zero. The scanner first validates framing and bounded type-derived
lengths without allocation, confirms the complete record physically exists,
then verifies CRC32C before decoding LSN, transaction state, prevLSN, or page
images. Record format version 1 is explicitly unsupported.

A final record whose physical bytes end before its validated total length may
be truncated during recovery after its available prefix passes structural
validation. A complete record with a checksum mismatch is corruption, even at
EOF, and is never converted into a crash-tail truncation.

The write ordering invariant is:

```text
construct after-image with pageLSN
    → append PageUpdate
    → publish dirty buffer frame
    → flush WAL through pageLSN
    → write data page
```

Commit uses `append Commit → flush_through(commitLSN) → append and sync
Committed(TxnId, CommitSeq(commitLSN)) → Committed`. Publishing status cannot
overtake the WAL decision. If a crash occurs after WAL sync but before status
sync, startup scans the durable WAL and idempotently reconciles the missing
status before admitting reads. If a flush or status write fails, the handle
remains `CommitPending`; retrying commit reuses the same record and decision.
A new-page update is also flushed
before extending the database file, because writing the allocator's zero page
is itself a data-file write that must not overtake its WAL record.

Runtime rollback uses:

```text
Active
    → append + flush Abort
    → RollbackPending
    → follow this transaction's prevLSN chain backward
    → validate and install each full before-image
      (zero before-image removes the exact trailing page)
    → sync each affected rollback page or truncation
    → append + flush RollbackComplete
    → RolledBack
```

`RollbackRequired` is distinct from `RollbackPending`. The former means a
compound logical operation has appended only part of its physical WAL history;
no Abort exists yet, and only `rollback()` is permitted. The latter means Abort
has been appended and physical undo is running or retryable. A two-page
versioned UPDATE prepares both after-images before WAL publication, then
deterministically logs the new-version PageUpdate followed by the predecessor
expiration PageUpdate. Any later logging,
flush, allocation, buffer acquisition, or publication failure marks the
transaction `RollbackRequired`, so a half update can never commit.

Before-images restore their historical pageLSN; rollback does not generate
ordinary PageUpdate records. A rollback error leaves `RollbackPending` and
retains writer ownership, so calling `rollback` again safely repeats the
idempotent physical undo. Only the affected rollback page is flushed per undo
step. Commit remains NO-FORCE; rollback synchronizes its physical changes before
reporting success.

All disk widths are explicit; no Rust struct layout, host-endian values,
pointers, or `usize` are persisted. Malformed headers, slot directories,
record ranges, row lengths, tags, and UTF-8 values return typed errors.

## Startup recovery

Recovery is synchronous storage-layer work and completes before a `BufferPool`,
`HeapStorage`, or `Database` is exposed:

```text
Database::open
    │
    ▼
Open Data File + WAL
    │
    ▼
Analysis
    │
    ├── Winners (Commit exists)
    ├── Completed rollback (RollbackComplete exists)
    └── Losers (incomplete or Abort-only)
    │
    ▼
Redo non-rolled-back PageUpdates in ascending LSN
    │
    ▼
Undo losers in descending global LSN
    │
    ▼
Sync undo + durably finalize recovered losers
```

Analysis builds transaction lastLSNs and an LSN lookup. Redo repeats history,
including loser updates, but skips transactions with durable
RollbackComplete: an existing page is skipped only when its pageLSN is at least
the update LSN, and that pageLSN is trusted only after full page validation;
otherwise the validated after-image is installed. A new page must be exactly
the next trailing page, so WAL cannot create page-ID gaps. Undo follows each
incomplete or Abort-only loser's prevLSN chain through a max-heap and installs
PageUpdate before-images in global descending LSN order. A zero before-image
means the loser allocated that page, which can only remove the exact trailing
page. The page file is synchronized, then recovery appends Abort for any loser
that was still Active, appends RollbackComplete, and flushes those terminal
records before returning. This prevents a recovered loser from conflicting
with or overwriting a later winner on another restart.

After physical recovery, open rescans the selected durable WAL generation and
reconciles the status sidecar: every Commit becomes
`Committed(TxnId, CommitSeq(commit_lsn))`, and every RollbackComplete becomes
`Aborted`. Records already present are idempotent; conflicting decisions are
corruption. This closes both crash windows around terminal status publication
before any ReadView can be created.

A checksum-invalid current page is a hard recovery error before its pageLSN is
read or compared. Recovery does not blindly repair it from retained WAL because
a checkpoint may already have recycled the page's complete history.

Because full-page after-images can include another transaction's uncommitted
contents, the runtime permits one writer and acquires that ownership before any
heap page mutation or allocation. RollbackRequired, CommitPending, and
RollbackPending retain it.
Commit durability or completed physical rollback releases it. Dropping an
unfinished dirty writer marks the open storage recovery-required; later writes
and close fail, while read-only handles may still be created. Analysis also
rejects historical retained WAL where a committed page update follows an
unresolved loser update to the same page before recovery writes any page. This
is the write-side safety invariant; read isolation comes from MVCC status and
ReadViews rather than from the writer lease. It is not cross-process locking.

The algorithm intentionally has no compensation log records. During runtime
rollback, Abort is durable before physical undo and RollbackComplete becomes
durable only after all rollback page changes are synchronized. A crash before
completion therefore leaves an Abort-only loser: startup repeats its history
and deterministically undoes the whole prevLSN chain. Startup recovery uses the
same ordering when it finalizes a loser after undo. A durable completion record
means the already-synchronized transaction images must be skipped, so a later
committed winner is never overwritten by reapplying the old rollback. The
retained valid WAL is synchronized before any recovery page write.

At startup only an incomplete final record whose available header is
structurally valid may be truncated at EOF. Corrupt middle records, invalid
magic/version/type/length fields, invalid transaction state, broken transaction
chains, malformed data pages, and malformed before/after page images fail open
with typed errors.

The current model is single-writer, STEAL, NO-FORCE, WAL-protected, and supports
synchronous physical runtime rollback plus startup crash recovery. `abort` is
an alias for that rollback operation. MVCC provides Read Committed and
Repeatable Read snapshot visibility over active-writer pages. There is no
Serializable isolation, fuzzy checkpoint, concurrent writer queue, or
cross-process writer lock.

## Manual vacuum

`HeapStorage::vacuum` and `Database::vacuum(TableId)` are explicit synchronous
maintenance operations. A ReadView pins its `visible_csn` in the in-memory
status store until drop. Vacuum chooses the oldest pinned CSN, or the current
maximum committed CSN when none is pinned, and reclaims only tuples whose
inserter aborted or whose committed `xmax` is no later than that horizon. Active
or aborted expirers are never dead. The operation first validates and collects
dead versions, then owns one normal write transaction, removes every matching
exact registered-index candidate, physically tombstones the Heap slot, and
commits through the ordinary WAL/status path. Errors roll back the whole vacuum.
Slot reuse still increments generation, so a locator retained past vacuum cannot
name a later occupant. There is no background worker, automatic scheduling,
status-log compaction, or file shrinking in this phase.

## Checkpoint and WAL lifecycle

The first checkpoint model is intentionally quiescent. `TransactionManager`
registers every successful `Begin`, including read-only handles, and unregisters
exactly once after durable commit, completed rollback, or clean active drop.
Dropping a dirty/pending writer unregisters the vanished handle but changes
runtime health to `RecoveryRequired`. Checkpoint admission requires:

```text
writer = Idle
runtime health = Healthy
outstanding transaction handles = 0
```

It never waits or queues. Active, RollbackRequired, CommitPending,
RollbackPending, read-only active, and RecoveryRequired states return typed
errors. Clean close enforces
the same no-outstanding-handle safety property so no live transaction can retain
a prevLSN into recycled history.

The checkpoint order is:

```text
verify quiescence
    → flush WAL through the highest written LSN
    → flush every dirty buffer frame (each preserves WAL-before-page)
    → synchronize the database file
    → capture old logical end + next TxnId
    → remove only the inactive older WAL slot
    → create the inactive slot with generation + 1 and the captured metadata
    → synchronize the new WAL file and its parent directory
    → switch the shared WalManager to that generation
    → delete the superseded slot and synchronize its directory entry
```

The synchronized data file is the checkpoint success foundation: every effect
represented by retired records is already durable before creation of the next
generation begins. `BufferPool`, `TransactionManager`, and `HeapStorage` retain
the same shared `WalManager`, so switching cannot split WAL ownership.

The two slots form a small crash-safe selection mechanism rather than a general
segment manager. Rotation never removes the currently selected valid slot.
Before the new header is complete, open ignores a truncated inactive header and
uses the old generation. Once a complete new header is durable, both files may
remain and open selects the greater generation after checking consecutive IDs,
base/checkpoint continuity, and the TxnId high-water mark. A malformed complete
newer candidate is a hard error. Open deletes a validated superseded generation
before exposing the selected manager. A successful checkpoint therefore keeps
one WAL file; a crash or cleanup failure may temporarily leave two, and the next
open or checkpoint deterministically removes the older one.

Recovery runs the existing analysis/redo/undo algorithm only over records in
the selected latest safe generation. Old pageLSNs are not reset. Post-checkpoint
updates receive logical LSNs above the generation base, so redo can compare them
directly against pages written before checkpoint. The header's `next_txn_id`
preserves transaction identity monotonicity after old records are recycled.

There is no clean-shutdown marker. Scanning one bounded active generation is
simple and deterministic; a marker would require a separate durable
clean-to-dirty invalidation state machine before the next mutation and does not
currently remove enough work to justify that risk.

Deterministic subprocess tests exercise STEAL loser undo, NO-FORCE winner redo,
commit and rollback durability boundaries, recovery interruption, and both WAL
generation rotation windows. Each child terminates without running Rust object
destructors, then the parent opens the database twice to verify convergence and
idempotence. This models abrupt database-process loss only; it does not simulate
kernel, machine, controller, or storage-device power loss.

## Embedded and server modes

`netbadb-core` remains synchronous and embedded. `netbadb-protocol` defines a
transport-independent binary v1 contract and depends only on shared types.
`netbadb-server::SessionState` is also synchronous: it owns handshake and one
optional table-scoped transaction while borrowing the database owner for each
request.

The blocking TCP runtime uses one OS thread per accepted connection and one
dedicated database worker thread. Connection threads own the socket, optional
rustls connection state, authenticated transport identity, SessionId, decoded
client frames, and response batches. Typed channels carry only Send-safe values
to the worker. The worker constructs and exclusively owns the Database, every
SessionState, every Transaction, and the association between a SessionId and
its ClientIdentity. Storage's Rc/RefCell transaction internals never cross a
thread boundary and are not replaced by networking-driven Arc/Mutex state.

Network connections may progress concurrently while reading or writing, but
the worker consumes one FIFO command queue and serializes all database request
execution. Each connection sends one request, waits for its complete response,
writes and flushes that batch, and only then reads the next request. A long
query therefore delays other sessions; Phase 5C1 does not claim concurrent
database execution.

Protocol requests execute one at a time. Query results become `QueryStart`,
zero or more `QueryRow` messages, and `QueryEnd`; they are not encoded as one
unbounded result frame. Hello returns each table's stable `TableId` and
canonical 32-byte schema fingerprint in schema declaration order. Stable wire
error codes and protocol transaction-state tags are mapped explicitly rather
than exposing Rust enum layout or debug output.

The listener is nonblocking only so a shutdown command can stop acceptance;
accepted streams are explicitly restored to blocking mode. Clean EOF, protocol
violations, and response I/O failures all close the corresponding SessionState
through the worker. A close rollback failure is fatal to that worker rather
than dropping a retryable transaction and continuing service. Graceful server
shutdown closes connection sockets, joins their threads, closes remaining
sessions, explicitly closes the Database, and joins the worker.

`netbadbd` reads deployment manifest v4 before startup. Relative heap and TLS
paths are resolved against the manifest directory. Certificate, private-key,
and client-CA material is parsed into a mandatory-client-auth rustls config
before the database worker starts; the worker then calls `Database::open_tables`
before the listener is bound. The manifest supplies complete TableDefs because
a heap fingerprint cannot reconstruct schema. Plaintext listeners are
restricted to loopback, while non-loopback listeners require mutual TLS.

Authentication, authorization, compilation, and execution remain separate.
The TLS connection thread authenticates a transport peer and derives the
verified leaf-certificate fingerprint. During `OpenSession`, the database
worker resolves that identity to an immutable principal policy; a trusted but
unlisted certificate is closed before SessionState and Hello. The worker keeps
the resolved grants beside the transport-neutral SessionState. For Execute it
asks `netbadb-core::Database::statement_access` to compile SQL and expose only
ordered canonical read/write TableIds. The worker checks those IDs before
calling SessionState's execution path. Core describes access but knows nothing
about clients or policy; TLS, authorization, and server dependencies never flow
into compiler, planner, executor, storage, or persistent formats.

Handshake and session sequencing precede authorization, while successful SQL
compilation precedes SQL permission checks. Begin and Analyze validate table
existence before their independent grants. Commit, Rollback, disconnect close,
and shutdown bypass grants so an owned transaction can always be resolved.
HelloAck visibility is filtered by the worker after SessionState builds the
canonical schema-ordered identities; protocol capabilities remain unfiltered.

Connection admission and socket read/write timeouts happen before TLS. A
connection thread explicitly completes certificate verification, derives
SHA-256 over the verified client leaf DER, and only then asks the worker to
create a `WorkerSession`. Failed TLS peers therefore consume a bounded socket
and thread while handshaking but never own SessionState or Transaction. The raw
control-socket clone remains available so shutdown interrupts both TLS and
NDBP reads. ClientIdentity is runtime metadata only; SessionState and all core,
protocol, and persistent layers remain TLS-unaware.

Operational limits remain at server boundaries. The accept loop reaps finished
threads and uses its connection-vector length to reject excess sockets before
creating a SessionState or connection thread. Every admitted blocking stream
has a read timeout for idle and partial-frame clients and a write timeout for
blocked response delivery. Timeout cleanup uses the same fallible
`SessionState::close` path as every other disconnect.

The database worker remains synchronous and cannot be safely preempted. Socket
read inactivity is therefore not a statement execution timeout. Likewise,
`max_result_rows` is checked by SessionState only after core execution has
fully materialized QueryResult. It prevents expansion into an excessive number
of protocol messages but is not a complete executor-memory limit.

Runtime metrics use only standard-library atomics and expose read-only
snapshots. They count admitted, rejected, active, and closed connections;
successful and failed TLS handshakes; authenticated connections; worker
requests; protocol failures; idle timeouts; write failures; and
response-row-limit and authorization-denial errors. Metrics never control admission or database
correctness and contain no SQL, values, table names, certificate contents, or
client-address labels.

Networking remains synchronous and must not leak async into parser, compiler,
planner, executor, page, storage, WAL, or recovery. Protocol v1 is a network
contract, not a database-file format. The current independent persistent
contracts are Canonical Schema v1, Heap metadata v4, MVCC tuple v1,
transaction-status v1, Page v5, WAL v3/record v2, BTree v1, and IndexCatalog
v2. Deployment manifest v4 is configuration, not a database format or canonical
schema identity.

Rust applications choose either the default embedded SDK or the optional
synchronous remote surface:

```text
Embedded Rust application
    -> netbadb-sdk (default `embedded` feature)
    -> netbadb-core

Remote Rust application
    -> netbadb-sdk::remote (`remote` feature)
    -> netbadb-client
    -> netbadb-protocol
    -> netbadbd
```

`netbadb-client` owns blocking TCP/rustls transport and the Protocol v1 client
state machine. It has no production dependency on core, server, executor,
planner, or storage. It reuses the authoritative Rust protocol frame and value
codec, while the Go client intentionally remains an independent implementation
that proves the wire contract is language-neutral.

Remote Rust `Rows` and `Transaction` values exclusively borrow their Client,
preventing multiplexed requests at the type boundary. Explicit `Rows::close`
drains to QueryEnd; dropping unfinished rows closes the connection. Explicit
commit and rollback wait for a server response, while dropping an active
transaction only closes the transport and relies on disconnect rollback. A
lost response can therefore leave DML, commit, or rollback outcome ambiguous;
the client never reconnects, retries, or replays a request.

Go applications can use generated typed bindings above the independent
Protocol v1 client under `sdk/go`:

```text
Language-neutral SDK Schema Spec v1
    -> Rust validation and canonical TableDef fingerprints
    -> deterministic generated Go bindings
    -> Go Protocol v1 client
    -> Protocol v1
    -> netbadbd
```

The Go transport, codec, values, result streaming, transaction lifecycle, and
schema-fingerprint gate use only the Go standard library. They share no Rust
memory or layout, use no cgo or FFI, and do not replace typed wire messages with
JSON execution IR. Canonical schema fingerprint generation remains
Rust-authoritative; generated Go code embeds those bytes and compares them with
HelloAck identities. Schema Spec JSON ordering or serialization never defines
identity. Generated table wrappers accept only the complete canonical table row
shape in canonical column order and explicitly decode nominal and nullable
values without reflection.

Schema Spec v1 is code-generation input only. It is not Canonical Schema v1's
binary identity encoding, cannot configure listeners, TLS, authorization, or
heap paths, and is not the server's deployment source of truth. Server startup
still gates manifest `TableDef` against heap metadata, while generated `Dial`
gates embedded fingerprints against authorized HelloAck tables. These two hard
failures detect drift while the inputs remain separate.

## Performance baseline boundary

The Phase 7A benchmark is an optimized custom Cargo target attached to
`netbadb-core`. It consumes only public database and inspection APIs and adds no
runtime dependency or alternate execution path:

```text
deterministic temporary databases
              ↓
       public Database API
          ↙          ↘
real execution      StatementInspection
          ↓                 ↓
correctness checksum   chosen operator gate
          ↘                 ↙
         warm-cache timing samples
```

Fixture construction, index backfill, `ANALYZE`, plan inspection, correctness
verification, reporting, close, and cleanup remain outside timed query loops.
The benchmark records current behavior; it does not feed measurements back
into planning, expose a new planner API, or change any persistent representation.
