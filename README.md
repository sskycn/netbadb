# NetbaDB

NetbaDB is a strongly typed relational database core written in Rust.

The project separates application-language schemas from the database engine
through a language-independent Canonical Schema IR. Rust provides native
embedded and synchronous remote APIs. Go and future languages use generated
SDKs or a versioned NetbaDB protocol client rather than coupling the database
core to an application runtime.

> NetbaDB is experimental. The implemented subset is intentionally small, but
> it is a real Rust workspace with a parser-to-storage vertical slice.

## Architecture

The durable architectural boundary is:

```text
Application language schema
        ↓
Language frontend / SDK
        ↓
Canonical Schema IR
        ↓
Parser → HIR + type checking
        ↓
Typed Relational IR
        ↓
Optimizer / Planner
        ↓
Executor
        ↓
Transaction boundary
        ↓
Storage
```

The current embedded path is synchronous:

```text
Rust Schema API
    ↓
SELECT / JOIN / ORDER BY / GROUP BY + typed DML parser
    ↓
Typed HIR
    ↓
Logical query / DML statement plan
    ↓
Scan + nested-loop join + sort + grouped aggregate physical plan
    ↓
Join / filter / sort / aggregate / projection / limit + mutation executor
    ↓
Heap
    ↓
Transaction lifecycle + versioned WAL
    ↓
Buffer pool (guards, pinning, dirty writeback)
    ↓
Slotted pages
    ↓
Page manager / database file
```

The core does not depend on Go, a network runtime, JSON execution IR, or
application-specific Rust structs.

## Strong types

Internal identifiers are newtypes such as `TableId`, `RelationBindingId`,
`ColumnId`, `PageId`, and `RowId`. A relation binding identifies one
query-local table occurrence, so two aliases of the same `TableId` remain
distinct in a self join. Schema columns preserve both a physical representation and an
optional nominal semantic type:

```text
physical: UINT64
semantic: UserId
```

`UserId` and `TeamId` therefore remain distinct even when their physical
representation is the same. The storage format encodes physical values; the
Canonical Schema remains the source of semantic meaning. Each validated table
also has a versioned canonical byte encoding and SHA-256 schema fingerprint.
Heap metadata persists that fingerprint, and reopen requires the caller's full
table identity—including semantic types and column order—to match.

## Repository layout

```text
netbadb/
├── Cargo.toml
├── rust-toolchain.toml
├── crates/
│   ├── netbadb-types/       shared IDs, physical and semantic types
│   ├── netbadb-schema/      language-independent Canonical Schema IR
│   ├── netbadb-schema-spec/ strict SDK Schema Spec v1 parsing
│   ├── netbadb-parser/      small typed-query AST and parser
│   ├── netbadb-hir/         name resolution and type checking
│   ├── netbadb-rel/         typed logical relational IR
│   ├── netbadb-compiler/    AST → HIR → logical plan
│   ├── netbadb-tooling/     stable schema-driven SQL diagnostics
│   ├── netbadb-planner/     logical plan → physical plan
│   ├── netbadb-index/       typed B+Tree ordering, nodes, codecs, and splits
│   ├── netbadb-storage/     transactions, WAL, pages, buffer pool, heap, B+Tree
│   ├── netbadb-executor/    synchronous physical-plan execution
│   ├── netbadb-core/        native embedded database API
│   ├── netbadb-protocol/    versioned language-neutral binary wire contract
│   ├── netbadb-client/      synchronous Protocol v1 remote client
│   ├── netbadb-inspect/     stable inspection DTOs and deterministic text
│   ├── netbadb-server/      sessions, worker ownership, and blocking TCP runtime
│   └── netbadb-codegen/     strict Schema Spec v1 and typed Go generation
├── cmd/
│   ├── netbadb/             offline local inspection CLI
│   ├── netbadb-lsp/         diagnostics-only stdio language server
│   └── netbadbd/            standalone manifest-driven server executable
├── sdk/
│   ├── rust/                Rust application-facing re-export surface
│   └── go/                  Go SDK / protocol direction and contract notes
├── docs/
├── examples/
└── tests/
```

In the dependency graph below, `A -> B` means that crate A depends on crate B.
The direction is acyclic:

```text
schema -> types
schema-spec -> schema + types + serde + serde_json
codegen -> schema-spec + schema + types
inspect -> schema + types
hir -> parser + schema + types
rel -> types
compiler -> hir + parser + rel + schema + types
tooling -> compiler + hir + parser + schema
planner -> index + rel + types
index -> types
storage -> index + schema + types
executor -> planner + rel + storage + types
core -> compiler + inspect + planner + rel + executor + storage + schema + types
protocol -> types
client -> protocol + schema + types
server -> core + protocol + schema + types
netbadbd -> server
netbadb CLI -> Rust SDK embedded + server + serde + serde_json
netbadb-lsp -> tooling + schema-spec + lsp-server + lsp-types
Rust SDK embedded -> core + inspect + schema + types
Rust SDK remote -> client + schema + types
```

Storage has no dependency on the planner or executor. The executor consumes a
physical plan and a safe storage API. The page layer currently uses no
`unsafe`; future binary-layout or mmap work must remain localized and audited.

## Implemented now

The current code genuinely supports:

- Cargo workspace compilation and unit/integration tests;
- Canonical schema definitions with nullable, primary-key, physical, and
  semantic type metadata, unified validation, and stable schema fingerprints;
- parser support for `SELECT`, qualified columns, `AS` and shorthand table
  aliases, chained `JOIN`/`INNER JOIN ... ON`, explicit-column single-row `INSERT`, `UPDATE`,
  `DELETE`, optional DML `WHERE`, source-column `GROUP BY`, multi-key
  source-column `ORDER BY`, contextual `COUNT`/`SUM`/`MIN`/`MAX`, `LIMIT`, wildcard projection,
  `AND`/`OR`/`NOT`, comparisons, `IS NULL`/`IS NOT NULL`, integer/string/
  boolean/NULL literals, and parentheses;
- name resolution and expression type checking with nominal semantic types and
  explicit nullability;
- typed query/DML HIR and logical relational IR;
- deterministic registered-index point scans, sequential scans, and
  left-major/right-minor nested-loop join physical planning;
- synchronous heap storage with fixed 4 KiB pages;
- version 5 slotted heap pages with persistent pageLSNs, PageId-bound full-page
  CRC32C, generation-bearing reusable tombstones, and checked bounds;
- synchronous buffer-pool guards with pinning, dirty tracking, flush, and
  bounded eviction;
- versioned little-endian WAL records for begin, full-page update, commit,
  abort, and rollback completion, with strong LSNs and per-transaction prevLSN
  chains;
- explicit transaction handles plus implicit statement transactions;
- commit durability through WAL sync and WAL-before-data-page writeback;
- lazy single-writer admission and synchronous physical runtime rollback;
- synchronous startup recovery with analysis, repeat-history redo, and
  reverse-LSN undo of incomplete or aborted transactions;
- explicit quiescent checkpoints with bounded two-generation WAL retention,
  monotonic logical LSNs, and persistent transaction-ID high-water marks;
- generation-safe RowId insert, update, delete, scan, stale-locator detection,
  file reopen, row encoding, and row decoding;
- persistent transactional B+Tree create, insert, exact delete, merge-only
  rebalance/root collapse, and duplicate-preserving point lookup with typed
  keys, arbitrary height, and buffer capacity one;
- a persistent append-only index registry plus atomic existing-row backfill,
  reopen discovery, automatic Heap/registered-index DML maintenance, and
  `TableId`/`ColumnId` embedded APIs;
- explicit `Database::analyze` optimizer snapshots with persisted table/index
  statistics and deterministic point-access cost selection;
- executor support for INNER JOIN, filter, stable in-memory sort, one-pass
  global/grouped aggregates, projection, limit, typed DML, affected-row results, SQL
  three-valued boolean logic, and NULL comparisons;
- versioned protocol v1 framing, schema-fingerprint handshake, streamed query
  response messages, bounded synchronous codecs, and stable wire errors;
- a synchronous transport-neutral `SessionState` for handshake, query/DML,
  explicit table-owned transactions, `ANALYZE`, ping, and disconnect rollback;
- a blocking TCP runtime with loopback plaintext or mandatory mutual TLS whose
  dedicated synchronous worker owns the Database and every SessionState, plus
  strict deployment manifest v4 bootstrap, authenticated certificate identity,
  per-certificate and local-plaintext table/operation authorization, secure
  remote listen, bounded connections/socket inactivity, response-row policy,
  in-process metrics, and the standalone `netbadbd` executable;
- a native embedded `netbadb-core::Database` API;
- stable, read-only embedded catalog and chosen-plan inspection DTOs with an
  explicit deterministic text renderer that exposes no planner or storage
  handles and never drives execution;
- an offline `netbadb inspect` CLI that reuses deployment manifest v4 and the
  embedded inspection API, with deterministic human text and explicit
  current versioned Inspection JSON v3 output (with v1/v2 retained historically);
- a diagnostics-only synchronous `netbadb-lsp` server that loads SDK Schema
  Spec v1 once, compiles full editor buffers without database access, and maps
  stable UTF-8 byte diagnostics to UTF-16 LSP ranges;
- a blocking Rust Protocol v1 client with loopback plaintext, verified mTLS,
  schema/capability gates, streamed rows, and explicit transaction lifecycle,
  exposed under the optional `netbadb-sdk::remote` feature;
- an independent standard-library Go Protocol v1 client plus deterministic
  Rust-generated semantic types, canonical IDs/fingerprints, nullable full-row
  decoders, typed row streams, and automatic schema gates.

Protocol v1 is specified byte-for-byte in
[`docs/protocol-v1.md`](docs/protocol-v1.md), and current standalone
configuration is documented in
[`docs/server-manifest-v4.md`](docs/server-manifest-v4.md). The generated SDK
input contract is documented in
[`docs/sdk-schema-v1.md`](docs/sdk-schema-v1.md). Manifests v1 through v3 are
retained as historical documentation and rejected by current
`netbadbd`. Phase 5 is complete: mTLS authenticates transport peers, while the
database worker authorizes compiler-resolved TableIds before execution.

Offline catalog and statement inspection is documented in
[`cmd/netbadb/README.md`](cmd/netbadb/README.md), and its machine-readable
contract is [`docs/inspection-json-v1.md`](docs/inspection-json-v1.md). Stop
`netbadbd` and every embedded process using the same files before running the
local CLI: NetbaDB does not yet provide cross-process file locking. Opening an
offline database performs normal startup recovery and can redo or undo WAL
state; this is not a forensic no-write reader. The inspected SQL itself is
never executed.

Schema-driven editor diagnostics are documented in [`docs/lsp.md`](docs/lsp.md).
Run `netbadb-lsp --schema schema.json` as an LSP stdio process. It neither opens
database files nor reports physical plans; it validates one SQL statement per
document against the canonical schema decoded from SDK Schema Spec v1.

The reproducible warm-cache performance baseline is documented in
[`docs/performance.md`](docs/performance.md). It measures current public
database behavior and real chosen plans without adding a CI wall-clock gate.
Phase 7E adds direct storage-scan attribution and validates each immutable Heap
page once per sequential scan, while retaining the complete checksum and
structural corruption boundary. Phase 7J propagates binding-aware required
columns through physical queries and uses selective Heap reads that validate
every persisted scalar while owning only requested values.

The experimental storage format uses versioned heap metadata and slotted pages.
Heap metadata version 3 retains the canonical table-schema fingerprint and adds
the stable IndexCatalog root PageId; versions 1 and 2 are rejected rather than
guessed or migrated. Phase 2A bumped
data pages from version 1 to version 2 to add pageLSN. Phase 3B bumps them to
version 3 because a formerly invalid slot encoding now means Deleted. Page v4
added a 28-byte header and CRC32C integrity; Page v5 expands each slot with a
generation used for safe tombstone reuse. Versions 1 through 4 are rejected
rather than guessed or migrated. Files created by
the pre-Foundation sequential `HEAP` page prototype are likewise not migrated.
The legacy metadata page 0 retains its separate version-3 layout and is not a
checksummed Page v5 data page.

IndexCatalog payload version 2 stores optional table and per-index optimizer
statistics in explicit fixed-width little-endian fields. Version 1 is rejected
without migration. These values are snapshots created only by explicit
`ANALYZE`; ordinary DML deliberately does not update or invalidate them.

Each database uses two alternating WAL slots named `<database>-wal` and
`<database>-wal.next`. Creation uses create-new semantics and refuses to
overwrite an existing database or WAL slot. A successful checkpoint retains
only the current generation; at most one superseded slot can remain after an
interrupted rotation and is cleaned on open or the next checkpoint.
`Database::insert` runs as an implicit transaction. Call
`begin_transaction`, `insert_in`, and `Transaction::commit` when several
inserts must share one WAL chain, or call `Transaction::rollback` (equivalently
`abort`) to remove their physical effects. A successful commit means its
commit record has reached durable storage; heap pages may remain buffered until
eviction, `flush`, or `close`.

The current full-page-image model permits one writer per open database object.
Writer ownership is acquired lazily by the first write, so read-only
transactions do not reserve it. Commit releases ownership only after the
Commit record is durable. Rollback first makes Abort durable, follows the
transaction's prevLSN chain backward, installs and synchronizes each validated
before-image (or removes newly allocated trailing pages), then durably records
RollbackComplete and releases ownership. A failed commit or rollback remains
pending and retains the writer for retry.

Dropping an unfinished dirty writer does not silently release it: the open
storage becomes recovery-required for subsequent writes, and `close` reports
an error. `flush` remains legal during an active transaction because the engine
uses STEAL and WAL-orders each page write; flush success does not mean commit.
Readers are not isolated and may observe an active writer's buffered changes.

`Database::checkpoint` and `HeapStorage::checkpoint` are explicit synchronous
quiescent checkpoints. They return a typed error instead of waiting whenever a
transaction handle remains outstanding, a writer is active/pending, or runtime
health requires startup recovery. A successful checkpoint first flushes the
current WAL, WAL-orders and synchronizes every dirty page, then creates and
synchronizes the next WAL generation. Commit, rollback, and clean read-only
drop unregister their transaction handle; `close` also rejects any still-live
read-only transaction so it cannot invalidate that handle's prevLSN chain.

`Database::open` and `HeapStorage::open` synchronously recover before exposing
the buffer pool. Recovery classifies transactions with a Commit record as
winners, RollbackComplete transactions as already physically undone, and
incomplete or Abort-only transactions as losers. It redoes non-rolled-back page
updates in ascending LSN order while using pageLSN to skip installed images,
then undoes losers in global descending LSN order from full before-images.
After synchronizing physical undo, startup appends and flushes Abort when
needed plus RollbackComplete for each recovered loser. A crash before that
completion is durable safely repeats history and deterministic undo; later
opens skip transactions whose completion is durable.

An incomplete final WAL record caused by EOF is discarded at the recovery
boundary only when its available header bytes are structurally valid. Invalid
magic, versions, tags, lengths, checksums, transaction chains, middle records,
and page images remain hard errors. Existing data pages are fully validated
before their pageLSN can suppress redo. WAL format v3 protects its 48-byte
header and every record with CRC32C. Record format v2 reuses bytes 12..16 for
the checksum, so the fixed record header remains 40 bytes and record sizes and
logical LSN spacing do not grow. Both checksums cover the complete header or
record with the checksum field treated as zero. A physically complete record
whose checksum fails is corruption and is never truncated as a crash tail.

Each Page v5 data page stores a little-endian CRC32C in bytes 24..28 of its
28-byte header. The checksum covers the expected PageId (as a little-endian
u64) followed by all 4096 page bytes, treating the checksum field as zero. It
therefore detects persisted header, slot-directory, free-space, and payload
corruption—including after checkpoint recycling removes old WAL history—and
must validate before recovery trusts pageLSN. A mismatch is a typed hard error;
CRC32C neither repairs corruption nor provides cryptographic authentication.
WAL checksums independently protect retained log bytes.

The WAL header separates physical file offsets from logical LSNs: for a record
at physical offset `P`,
`LSN = base_lsn + (P - 48)`. A checkpoint chooses the old logical end as the
new base, so LSNs never move backward even though physical WAL bytes are
recycled and historical pageLSNs remain unchanged.

Startup validates both WAL slots and deterministically chooses the valid slot
with the greatest consistent generation. A truncated newly-created header is
an interrupted rotation and falls back to the last valid slot; a corrupt newer
complete generation is a hard error. Recovery scans only the selected
post-checkpoint generation. Clean shutdown markers are intentionally omitted:
the bounded current generation is scanned on open, avoiding a second persistent
state machine whose marker would need invalidation before writes.

Phase 2C still does not provide MVCC, reader isolation, fuzzy or background
checkpoints, concurrent writers, or cross-process writer coordination. A
successful explicit `close` rejects every outstanding transaction and then
WAL-orders and flushes dirty pages; WAL recycling remains an explicit
checkpoint operation.

The query language is a deliberately small native subset, not a claim of full
SQL compatibility. Database NULL is represented explicitly as
`ScalarValue::Null`, while Rust `Option` remains reserved for absent clauses or
metadata. Untyped NULL literals receive a semantic type from their expression
context. Comparisons with NULL evaluate to UNKNOWN; `IS NULL` and `IS NOT NULL`
are the explicit tests. `AND`, `OR`, and `NOT` use SQL three-valued logic, and a
`WHERE` filter retains only TRUE, rejecting both FALSE and UNKNOWN. Heap writes
independently enforce schema nullability, including through the embedded insert
API.

Typed DML uses the same compiler, transaction, full-page WAL, rollback, and
recovery path as heap writes. `Database::execute` returns either query rows or
an explicit `AffectedRows(u64)` result; `query` rejects mutating statements.
Single-row INSERT requires an explicit column list. Omitted nullable columns
become NULL, while omitted non-nullable columns are rejected. UPDATE evaluates
all right-hand sides against the original row, and UPDATE/DELETE reuse the
SELECT predicate evaluator, so FALSE and UNKNOWN do not mutate a row.

Mutation is located by an internal versioned physical `RowId` (`PageId +
SlotId + u32 generation`) that is never exposed as a SQL column or treated as a
business key. Generation zero is never issued. DELETE compacts tuple bytes
without renumbering slots and retains the current generation in an explicit
tombstone. A later insertion may reuse the lowest eligible tombstone after a
checked generation increment. Before reuse, the old locator reports
`RowDeleted`; afterward it reports `StaleRowId` and cannot access the new
occupant. UPDATE rebuilds the current 4 KiB page while preserving the slot and
generation when it fits. Otherwise storage relocates the replacement to the
lowest existing PageId accepted by normal Page v5 insertion, or allocates a new
page, and returns the current `RowId`. The source becomes a same-generation
tombstone (`RowDeleted`) until later reuse makes the old locator stale. Heap
insertion uses the same deterministic linear first-fit search across all data
pages. There is no persistent free-space map or forwarding pointer.
Implicit DML owns one transaction. `execute_in` supports multiple statements
in an explicit transaction; until savepoints exist, an execution-time DML
failure rolls back that whole transaction.

Heap and B+Tree pages share one database file, buffer pool, transaction chain,
WAL, recovery pass, and checkpoint. Page v5 assigns distinct page-type tags to
Heap, BTreeMeta, BTreeInternal, BTreeLeaf, and IndexCatalog pages. Non-heap
pages contain exactly one generation-1 payload slot; heap scans and first-fit
allocation validate and skip them, while RowId access rejects them as non-heap.

`BTreeHandle` is a stable metadata-page identity. Its `NBTM` version-1 payload
stores the current root, height, and `IndexSpec`; root splits can therefore
replace the root without changing the handle. Leaf (`NBTL`) and internal
(`NBTI`) version-1 payloads encode fixed-width little-endian fields and typed
keys. Supported keys are Bool, Int64, UInt64, Text, and nullable NULL. Ordering
is typed value order with NULL first, followed by explicit
`(PageId, SlotId, generation)` RowId order. Equal keys are supported; only an
exact `(key, RowId)` duplicate is rejected. Insert splits by encoded byte size,
supports arbitrary height, and logs deterministic full-page changes in the
order new right page, existing left page, ancestors, then new root/meta when
needed. All new-page WAL is durable before file extension.

Internal separators are persistent lower-bound fence keys. Their RowId is an
ordering token and need not remain a live leaf entry or heap locator; delete
therefore does not rewrite a fence merely because a right-subtree minimum
changed. Exact delete uses encoded-byte soft underflow, deterministic
merge-only rebalance, recursive parent compaction, and root collapse. Removed
right pages and old roots remain valid, unreachable index pages and are not
reclaimed or reused yet.

The registry persists `ColumnId -> BTreeHandle` separately from raw B+Trees.
`create_index` atomically backfills current rows and registers only after the
full build; reopen discovers and validates the mapping. Later Heap and SQL DML
maintains all registered indexes in the same transaction and propagates every
RowId relocation. Raw B+Trees remain independent. Raw BTree lookup alone does
not validate referenced Heap rows or enforce uniqueness, and SQL index DDL
remains deferred. Core maps registered definitions and their cached optimizer
snapshots into an ordered, read-only planner context. Eligible
`column = non-NULL literal`, commuted equality, and nullable `column IS NULL`
predicates use exact point `IndexScan`; analyzed two-sided Int64/UInt64 bounds
can use a costed `RangeIndexScan`;
the executor then fetches complete Heap rows by generation-safe RowId. The
original SQL Filter remains above every IndexScan. Without statistics, the
first eligible registered index still wins. With statistics, SeqScan costs
`managed_page_count`, point IndexScan costs
`1 + tree_height + estimated_matches`, equality uses average non-NULL
frequency, `IS NULL` uses `null_count`, and bounded integer range estimates use
the exact discrete bound count times average duplicates. SeqScan wins an equal
cost; equal index costs preserve registration order. Stale snapshots can
change only plan choice and performance, never query semantics. One-sided and
Text/Bool range costing, index joins, and index-only scans remain deferred.

Typed INNER JOIN resolution assigns deterministic `RelationBindingId` values
in source order. An alias hides the underlying table name. Qualified columns
resolve through the exposed relation name; unqualified columns are accepted
only when exactly one visible relation provides the name. Each `ON` expression
can see the complete left subtree and its current right relation, but not later
joins. HIR preserves nominal types and requires BOOL (nullable BOOL is valid).
At execution, only TRUE matches; FALSE and UNKNOWN, including `NULL = NULL`,
do not. `SELECT *` emits columns in left-to-right relation/schema order, and
the nested-loop operator preserves duplicates in deterministic left-major,
right-minor order.

The core composes multiple unchanged one-table heap files with
`Database::create_tables`/`open_tables`; `insert_into` targets a table for
embedded data loading. No page, heap, WAL, recovery, checkpoint, or transaction
format changed for JOIN. Multi-table write transactions remain unsupported.

`ORDER BY` accepts one or more qualified or unqualified source-column keys.
Each key may specify `ASC` or `DESC` and `NULLS FIRST` or `NULLS LAST`; omitted
options become `ASC NULLS LAST` or `DESC NULLS FIRST`. Keys are resolved against
the complete `FROM`/`JOIN` scope before projection, so a query may sort by a
column it does not return. Alias names, ordinals, and arbitrary sort
expressions are not supported. Planning preserves the order
`Scan/Join -> Filter -> Sort -> Project -> Limit`. The executor resolves key
positions once, validates runtime physical types, and performs a stable
in-memory lexicographic sort. Stability makes ties repeatable for the current
input order, but it is not a permanent ordering guarantee across future plan
changes; callers that need a total order must include sufficient keys.

Aggregates accept `COUNT(*)` or source-column arguments to `COUNT`,
`SUM`, `MIN`, and `MAX`. Aggregate names are contextual in projection, so a
plain column named `count`, `sum`, `min`, or `max` remains selectable. The
aggregate plan is `Scan/Join -> Filter -> Aggregate -> Limit`, and one input
pass updates every aggregate state. `COUNT(*)` counts rows, while
`COUNT(column)` ignores NULL; both return non-null `UInt64`. Numeric `SUM`
ignores NULL, returns NULL for empty/all-NULL input, uses checked arithmetic,
and strips nominal meaning from its result. `MIN`/`MAX` support all current
ordered physical types, ignore NULL, return NULL for empty/all-NULL input, and
preserve the input `SemanticType`.

`GROUP BY` accepts one or more qualified or unqualified source columns. Every
projected source column must be a group key, while keys may remain hidden from
the result; GROUP BY without aggregates produces one row per distinct key.
Logical and physical Aggregate operators keep `group_keys` separate from their
ordered outputs, so interleaved source and derived fields preserve SELECT
order without synthetic IDs. Grouping is in memory: a hash map finds group
indexes and an insertion-ordered vector makes the current implementation
deterministic. NULL keys share one group, unlike expression `NULL = NULL`,
which remains UNKNOWN. Empty global input has one implicit group, while empty
input with explicit keys has no groups. LIMIT applies after grouping.

Grouped queries currently reject `ORDER BY`; SQL without ORDER BY makes no row
order guarantee despite the executor's first-seen implementation order.
HAVING, aliases, `DISTINCT`, GROUP BY expressions, aggregate expressions,
nested aggregates, grouping sets, rollup, and cube remain unsupported.

## SDK and protocol strategy

Go is no longer treated as the database implementation language. The intended
support boundary is:

```text
Rust embedded: netbadb-sdk -> netbadb-core
Rust remote:   netbadb-sdk::remote -> netbadb-client -> Protocol v1
Go remote:     generated typed bindings over the independent Go Protocol v1 client
```

The language-neutral SDK Schema Spec v1 is validated and fingerprinted by the
Rust `netbadb-codegen` crate, which emits deterministic Go source above the
independent Protocol v1 client. The JSON spec is neither Canonical Schema
encoding nor deployment configuration. Generated full-row wrappers validate
exact result order, names, physical and semantic types, and nullability before
decoding; they do not generate SQL, CRUD, or query-builder APIs.

The blocking Rust remote client deliberately reuses the authoritative
`netbadb-protocol` codec. It supports loopback plaintext or verified mutual
TLS, automatic capability/schema gates, streamed rows, and explicit
transactions. The SDK default remains the embedded API; remote-only users can
disable default features and enable `remote`. See
[`sdk/rust/README.md`](sdk/rust/README.md).

## Development

The repository pins Rust 1.97.1 with `rustfmt` and `clippy` in
`rust-toolchain.toml`; rustup installs it automatically when needed. The
workspace MSRV is Rust 1.85.0. Run:

```bash
cargo fmt --all -- --check
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo +1.85.0 check --workspace --all-targets
```

Convenience targets are available through `make`:

```bash
make fmt-check
make check
make clippy
make test
make msrv-check
```

The Go SDK notes are under [`sdk/go`](sdk/go/README.md). Test it from that
module with `go test ./...`; use `scripts/check-generated-sdk.sh` to verify
committed generated source.

## Roadmap

The implementation sequence is intentionally vertical:

1. Rust foundation — stable types, schema, parser, HIR, and relational IR.
2. Storage Foundation — versioned slotted pages, checked page decoding,
   bounded buffer pool, guards, dirty writeback, heap insert/scan, and reopen.
3. Transaction + WAL Core (Phase 2A) — transaction lifecycle, versioned WAL,
   LSN/pageLSN, durable commit, and WAL-ordered page writeback.
4. Recovery (Phase 2B) — startup analysis, repeat-history redo, reverse-LSN
   undo, crash-tail handling, and crash-reopen guarantees.
5. Single Writer + Runtime Rollback (Phase 2B.1) — lazy writer ownership,
   retryable commit/rollback states, physical before-image undo, and
   crash-during-rollback safety.
6. Checkpoint + WAL Lifecycle (Phase 2C) — quiescent recovery boundaries,
   monotonic logical LSNs, and crash-safe bounded WAL generation recycling.
7. Typed expressions + NULL semantics (Phase 3A) — contextual NULL typing,
   expression nullability, three-valued logic, and explicit NULL predicates.
8. Typed DML (Phase 3B) — typed insert/update/delete plans, stable-RowId page
   mutation, affected-row results, and atomic WAL-backed execution. Complete.
9. Join execution (Phase 3C) — qualified columns, aliases, typed INNER JOIN,
   self joins, nested-loop execution, and NULL-aware join predicates. Complete.
10. Data-page integrity — Page v4 CRC32C bound to PageId, recovery-safe pageLSN
    validation, checkpoint-baseline corruption detection, and page fuzzing.
11. Aggregate + Sort (Phase 3D) — typed source-column `ORDER BY`, global
    aggregates, and in-memory `GROUP BY`/grouped aggregates are complete.
12. Versioned RowId + slot reuse (Phase 4A) — Page v5 slot generations,
    generation-safe tombstone reuse, and stale-locator detection. Complete.
13. Heap-wide reuse + safe relocation (Phase 4B) — deterministic first-fit,
    RowId-returning UPDATE relocation, and rollback-required multi-page
    mutation safety. Complete.
14. Persistent B+Tree (Phase 4C1) — transactional create/insert/point lookup,
    mixed pages, split recovery, and explicit typed codecs. Complete.
15. B+Tree exact delete/merge/root-collapse (Phase 4C2) — complete.
16. Persistent IndexCatalog discovery and transactional existing-row backfill
    (Phase 4D1) — complete.
17. Atomic heap/index DML maintenance (Phase 4D2) — complete.
18. Deterministic registered-index point IndexScan (Phase 4E) — complete.
19. Explicit ANALYZE statistics and deterministic cost-based point access-path
    selection (Phase 4F) — complete.
20. Protocol v1 + synchronous sessions (Phase 5A) — binary framing, schema
    handshake, streamed results, stable errors, and transaction lifecycle.
    Complete.
21. Network server (Phase 5B) — manifest bootstrap, loopback `netbadbd`, TCP
    connection lifecycle, a dedicated synchronous database worker, disconnect
    rollback, graceful shutdown, and multiple sessions. Complete.
22. Operational resource hardening (Phase 5C1) — connection/thread caps,
    socket timeouts, response-row policy, and in-process metrics. Complete.
23. Secure remote transport (Phase 5C2a) — mutual TLS, authenticated
    certificate identity, and secure non-loopback listening. Complete.
24. Per-client authorization (Phase 5C2b) — certificate/local principals,
    table and operation scopes, typed SQL preflight, and filtered Hello schema
    visibility. Complete.
25. Go Protocol v1 client and generated typed bindings (Phases 6A and 6B) —
    complete.
26. Synchronous Rust remote client and SDK feature stabilization (Phase 6C) —
    complete.
27. Structured inspection and offline local CLI (Phases 6D1 and 6D2) — stable
    DTOs, deterministic text, manifest-v4 bootstrap, and historical Inspection
    JSON v1. Complete; the current CLI contract is v3 after Phase 7D.
    Complete.
28. Shared SQL diagnostics and diagnostics-only LSP (Phase 6E1) — complete.
29. MCP and additional tooling adapters (Phase 6E2) — future work.
30. Costed bounded Int64/UInt64 RangeIndexScan (Phase 7B) — complete.
31. Predicate-first NestedLoopJoin rejected-pair materialization avoidance
    (Phase 7C) — complete.
32. Costed simple equi HashJoin for analyzed direct Scan × Scan INNER JOINs
    (Phase 7D) — complete; Inspection JSON v3 is current.
33. Validate-once Heap sequential scan (Phase 7E) — complete; one authoritative
    full validation is reused through a crate-private immutable page borrow,
    with checked record access and unchanged persistent formats.
34. Join predicate column-position prebinding (Phase 7F) — complete; NLJ and
    HashJoin residual predicates bind logical column identities to checked
    executor-layout positions once, with Inspection JSON v3 unchanged.
35. Borrowed Join predicate scalar evaluation (Phase 7G) — complete; Join-bound
    Column/Literal leaves borrow ScalarValues while computed results remain
    owned, with shared reference-based binary and truth semantics.
36. Exact inequality bound rejection (Phase 7H) — complete; NestedLoopJoin uses
    a necessary bound conjunct and borrowed real-data right min/max to skip
    left probes that cannot match any right row, without changing its plan.
37. Adaptive exact inequality candidate sweep (Phase 7I) — complete; execution
    sorts borrowed row-index auxiliaries, counts exact candidates, and uses a
    checked integer work choice before an original-order-preserving sweep.
38. Required-column propagation and selective base-row decode (Phase 7J) —
    complete; query-only physical pruning preserves hidden semantic columns,
    Heap validates every encoded value, and only requested values become owned.
39. Remaining row-codec/scalar-consumer ownership (Phase 7K) — selected from
    post-7J Text projection and COUNT(Text) sensitivity, not started.

Isolation/MVCC, one-sided/Text range costing, and index-join planning remain
roadmap items.
See [`docs/architecture.md`](docs/architecture.md) and
[`docs/roadmap.md`](docs/roadmap.md) for the maintained design notes.

## License

NetbaDB is licensed under the [GNU Affero General Public License v3.0 or later](LICENSE).

This project is identified as `AGPL-3.0-or-later` in its Cargo package metadata.
