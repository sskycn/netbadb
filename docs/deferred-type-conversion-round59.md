# Cross-physical CAST and atomic shadow conversion audit (Round 59)

Round 59 selects one architecture for physical type migration but deliberately
does not open the production SQL/HIR gate. The winning primitive is the existing
typed `ExprKind::Cast`, evaluated as one deterministic scalar conversion and
consumed by the existing deferred shadow-column program.

Round 60 has now productionized this selected design. This document remains
the historical audit record; current semantics and verification are in
[`deferred-type-conversion-round60.md`](deferred-type-conversion-round60.md).

The audit started from `origin/main` `9e8c65dd0264d0286aac5de28b29a4ae45515a11`.
That is later than the requested planning baseline: Round 58 is
`2d4dcc8a53c46716b04126b0e561c5c120267433`, Phase 3B.5 is
`ba039a61f9a304da4fbdbe6efebd83b7397164b1`, and Physical Types v2 plus its
review/MSRV repairs had already landed through `9e8c65d`. This audit therefore
uses the real 15-type inventory rather than the obsolete four-type premise.

## Current boundary and retained production behavior

`PhysicalType` contains:

```text
Bool
Int8 Int16 Int32 Int64 Int128
UInt8 UInt16 UInt32 UInt64 UInt128
Float32 Float64
Text Bytes
```

The parser accepts PostgreSQL-style `expression::TYPE` for all 15 types and
their documented aliases. It does not accept `CAST(expression AS TYPE)`.
Unlike the older premise, `expr::UINT64` is already a parser target.

HIR still lowers a cast child using the target as its expected type and calls
`require_type(child, target)`. Consequently the only production casts are
same-physical wrappers. The exact native negative for
`SELECT '42'::BIGINT` and for `shadow = legacy::BIGINT` is
`CompileErrorKind::DatatypeMismatch`, rendered as
`expected INT64, found TEXT`. PostgreSQL maps both to `42804`; a failure inside
an explicit transaction makes later commands return `25P02` until `ROLLBACK`.
`ALTER TABLE ... ALTER COLUMN ... TYPE ...` remains unsupported and maps to
`0A000`, with the same failed-transaction behavior. Same-physical
`42::BIGINT`, `'42'::TEXT`, and `true::BOOL` are unchanged.

`AlterTableOperation::ChangeNominalType` remains a separate Core-only feature.
It requires equal physical types and changes semantic/nominal metadata only.
It is not a value conversion and must not be reinterpreted as one.

The executable carrier is deliberately non-default. Core tests compile a
normal typed UPDATE, replace only its RHS with a manually constructed typed
relational Cast, and opt into executor feature `round59-audit`. Default SQL,
HIR, planning, execution and the public `evaluate_typed_row_expression` entry
retain their previous identity-cast behavior. The feature exposes one
explicitly named low-level audit entry point, so the all-features Rust surface
is larger; this is an experimental test carrier, not a stable SQL or SDK
contract.

## Architecture decision

| Criterion | A typed Cast + shadow | B ALTER TYPE sugar | C same-ID rewrite | D migration opcode | E rewrite S1 | F two transactions |
| --- | --- | --- | --- | --- | --- | --- |
| one expression semantics | yes | delegates to A | no | no | no | application-defined |
| one S2 | yes | yes after lowering | possible, complex | yes | no S2 | not atomic |
| S1 frozen | yes | yes | yes | yes | no | yes per transaction |
| rollback | existing | existing after lowering | new proof | existing | difficult | partial state visible |
| bad-row repair | yes | depends on exposed workflow | awkward | duplicate model | no safe prefix | yes, non-atomic |
| index swap reuse | exact Round 58 path | must synthesize it | identity conflict | exact path | invalidates source | manual |
| parser impact | Cast gate only | large ALTER/USING surface | none | new migration surface | none | none |
| persistent/recovery change | none | none after A | likely | risks opcode persistence | major | none |
| general SQL value | yes | syntax only | no | no | no | no |
| implementation risk | lowest | medium after A | high | medium/high | unacceptable | operational baseline |

Candidate A is the unique winner. Candidate B should be later syntax sugar
that creates a hidden shadow identity and lowers onto A. Candidate C violates
the frozen-E/final-F theorem that every surviving ColumnId keeps compatible
semantic and physical type; C2 and C4 must be distinct in the first design.
Candidate D creates a second evaluator and error model. Candidate E destroys
S1's frozen-source and rollback authority. Candidate F remains a supported
operational fallback but is not an atomic migration.

The selected theorem is:

```text
S1 / C2 Text authority
  -> one typed scalar Cast
  -> VirtualRow C4 Int64
  -> repair and NOT NULL
  -> logical I1/C2 evacuation
  -> DROP C2, RENAME C4, CREATE I2/C4
  -> one source pass into one S2
  -> one structural publication
```

No migration evaluator, conversion opcode, per-row sidecar, early S2, S3, or
recovery conversion pass is needed.

## Complete 15 by 15 classification

The following family rules exhaust all 225 ordered pairs. “Supported” here
means selected and exercised by the audit kernel for Round 60; production SQL
still rejects every cross-physical pair in Round 59.

| Source family -> target family | Bool | signed integer | unsigned integer | Float32/64 | Text | Bytes |
| --- | --- | --- | --- | --- | --- | --- |
| Bool | identity supported | rejected | rejected | rejected | supported | rejected |
| signed integer | rejected | checked supported | checked supported | potential future | supported | rejected |
| unsigned integer | rejected | checked supported | checked supported | potential future | supported | rejected |
| Float32/64 | rejected | potential future | potential future | identity supported; cross-width future | potential future | rejected |
| Text | supported | supported | supported | potential future | identity supported | rejected |
| Bytes | rejected | rejected | rejected | rejected | rejected | identity supported |

Expansion is exact:

- all 15 identity pairs are value-equivalent no-ops;
- every ordered pair among the ten integer types is supported with checked
  range conversion, including signed/unsigned boundaries;
- `Text` converts to and from each of the ten integer types;
- `Bool <-> Text` is supported;
- all pairs involving a float and a different float/integer/Text physical type
  are potential future work because rounding, special values, underflow and
  canonical text require a separate contract;
- Bool/numeric and every nonidentity Bytes pair are rejected, not guessed.

The exact recommended Round 60 matrix is therefore identities, all checked
integer-to-integer pairs, Text/integer pairs, and Bool/Text. Nothing else.

## Deterministic scalar contract

NULL is handled before parsing or range checking and always produces target-
typed NULL. A non-NULL scalar must match the child's declared physical type;
the evaluator never guesses a source type from a convenient variant.

Text-to-integer grammar is a deliberate NetbaDB subset:

- decimal ASCII digits only;
- at least one digit;
- leading zeroes are accepted;
- signed targets accept one leading `-`, including `-0`;
- unsigned targets accept no sign;
- whitespace, leading `+`, underscores, grouping, decimal points, exponents,
  Unicode digits and empty/sign-only strings are rejected.

The grammar is checked independently before conversion. Parsing into i128 or
u128 is only the checked implementation after that SQL contract is established.
`i64::MIN`, `i64::MAX` and `u64::MAX` are pinned. Negative signed-to-unsigned,
unsigned values above a signed maximum, and narrower conversions fail; values
never wrap, truncate or reinterpret bits.

Integer-to-Text output is base-10 ASCII with no locale, grouping or leading
plus. Negative signed values retain `-`. Bool-to-Text emits exactly `true` or
`false`; Text-to-Bool accepts exactly those two lowercase strings. PostgreSQL
17 deliberately accepts a broader case-insensitive, whitespace-tolerant Bool
set with synonyms and prefixes, so NetbaDB does not claim full compatibility.
PostgreSQL also has only signed 16/32/64-bit integer SQL types. The NetbaDB
contract is locale-, timezone- and platform-independent across its wider
physical set. See the PostgreSQL 17 [Boolean type](https://www.postgresql.org/docs/17/datatype-boolean.html),
[numeric types](https://www.postgresql.org/docs/17/datatype-numeric.html), and
[error codes](https://www.postgresql.org/docs/17/errcodes-appendix.html).

The audit error categories are separate:

| Executor category | Meaning | recommended future SQLSTATE |
| --- | --- | --- |
| `InvalidCastText` | grammar/representation is invalid | `22P02` |
| `CastOutOfRange` | syntactically valid value does not fit | `22003` |
| `UnsupportedCast` | typed pair is outside the matrix | `42846` |
| `InvalidCastInput` | typed IR child and runtime variant disagree | `XX000` |

Transport mapping remains future work; no adapter-specific rule was added to
the executor in Round 59.

## Executor and optimizer audit

Production currently elides Cast in three correctness-sensitive places:
`evaluate_values`, `evaluate_dynamic_with`, and `bind_expression`. The audit
mode parameterizes the shared recursion rather than creating a second SQL
interpreter. Contiguous evaluation converts an owned result, dynamic borrowed
evaluation converts `ScalarRef` into `EvaluatedScalar::Owned`, and bound
evaluation retains `BoundExprKind::Cast { source, target, expression }`.
Unit tests run the same eight minimum pairs through all three paths.

Round 60 should retain same-physical bound eligibility and use the tested bound
Cast node for general expressions. Specialized fast paths must decline a
cross-physical expression until they prove equivalent behavior. WHERE and join
predicates can then use the generic bound/dynamic evaluator correctly; they
must not be partially supported or silently erased.

Current constraint extraction is already conservative: zone maps, partition
constraints and B-tree point/range discovery recognize direct column/literal
shapes and do not look through Cast. `text_col::BIGINT = 42` therefore must not
derive a Text zone or Text B-tree key. Columnar may supply the raw source vector
but must use the same scalar conversion. Inspection currently strips Cast and
must retain it when the HIR gate opens. Result columns already derive their
type from the outer expression metadata, so future `SELECT legacy::BIGINT`
reports BIGINT.

An untyped `$1::BIGINT` is constrained directly as BIGINT today. A parameter
declared Text and then cast to BIGINT remains a cross-physical conversion and
must follow the same kernel. Deferred actions retain only bound owned
`ScalarValue`s, never portal/session references. All RHS expressions still read
one pre-statement VirtualRow; later actions read earlier converted values.

## Executable migration and evidence

The principal fixture uses C1 id BIGINT, C2 legacy TEXT NOT NULL, C3 flag BOOL,
I1/C2, and rows `42`, `-7`, `bad`. Transaction-local DML changes row 1 to `43`,
inserts `99`, deletes `-7`, then adds C4 BIGINT.

The first typed conversion encounters `bad` and returns `InvalidCastText`.
No action, action evidence, observation or frozen E is retained, the phase
stays Refining, S1/I1 and its digest remain unchanged, and no StorageId is
allocated. A normal deferred repair writes C4=0 for `bad` and freezes E. Retry
converts only `43` and `99`: the C4-not-NULL predicate prevents evaluation of
the invalid repaired row, and the exact affected count is two.

After C4 NOT NULL, the unchanged Round 58 sequence drops I1, drops C2, renames
C4 to `legacy`, and creates I2/C4. Finalization observes exactly the same
converted results as Execute, copies three rows in one source pass, allocates
one S2 and no S3, and publishes `[43, 0, 99]` as physical Int64. Direct Int64
B-tree lookups find all three rows. Prepared dependencies on C2 are stale;
C2 is gone and the product contract does not claim ColumnId preservation.

Additional executable controls prove:

- a range failure after an accepted prefix leaves action/evidence counts and
  Backfilling state unchanged;
- result-observation corruption returns `Corrupt` before S2 publication;
- a later `Int64 -> Text` action reads the preceding converted C4 value;
- target, source ColumnId, predicate literal, and action order alter semantic
  and whole-program digests;
- existing Round 50 and Round 54 golden digests remain unchanged in the full
  workspace tests;
- Enabled Change Stream blocks the replacement before S2, while disabled
  fixtures succeed; rebaseline remains explicit on S2;
- pre-Decision crashes restore S1/C2/I1 and post-Decision crashes finish
  S2/C4/I2 across three reopens without parsing source Text again.

The crash matrix covers repair acceptance, conversion acceptance, terminal
index evacuation, DROP, RENAME, new index reservation, tag-25 intent, tag-35
intent, mid-copy, prepare, Decision and Complete/publication.

The historical PostgreSQL 17 audit fixture was superseded by
`python3 scripts/test-type-conversion-round60-sql.py`, which exercises the
production cross-physical CAST path and the same atomic shadow-migration and
three-reopen boundary.

## Global recovery and CORD v4 finding

Success is a structural transaction: private ADD/repair/conversion/swap steps
publish no intermediate G, final success advances exactly one G, and Phase 3B
keeps Decision sync, physical/schema completion, Complete sync, publication.
It does not enter the pure-data deferred-Complete pipeline.

The audit first compacted ten pure-data decisions into the existing NBCO v1 +
CORD v3 GlobalEnable + CORD v4 checkpoint, reopened, and then ran the conversion.
The structural tail remains ineligible for another compaction, as designed.

This control exposed one pre-existing recovery defect: schema-mutation recovery
always appended legacy unsequenced Complete even when the recovered decision
had a post-checkpoint G. The resulting log rejected its own tail as
`UnsequencedRecordAfterCheckpoint`. Recovery now dispatches completion from the
decoded decision: a decision with `commit_seq` appends sequenced Complete; an
old local decision appends legacy Complete. No record encoding or compaction
policy changed. A Decision crash after a CORD v4 checkpoint now preserves the
checkpoint frontier, finishes exactly one new G, and converges over three
reopens.

## Formats, performance and next rounds

Round 59 adds no persistent conversion opcode. Existing deferred v1 remains
correct because `put_expr` hashes the outer target ExprType before the Cast tag
and recursively hashes the typed source child and ColumnId. The nonpersistent
program, result observation, action digest, snapshot digest and clone-plan
digest already bind the conversion.

No encoding changes are made to Canonical Schema, NBSC/NBSM, NBSJ tags 1--35,
NBCO/CORD v1--v4, Heap/Page/WAL/status, IndexCatalog/BTree, NBCL,
NBCM/NBCS/NBCD/NBPC, Partition/LSM, Protocol/PostgreSQL framing, deployment
manifest, SDK schema or inspection JSON. Columnar remains derived from its
exact source storage, and maintenance never performs conversion/finalization.

Debug-profile micro-observations, with no threshold, were:

| pair | rows | rows/s | temporary Text allocations | output Text bytes |
| --- | ---: | ---: | ---: | ---: |
| Text -> Int64 | 10,000 | 2,111,839 | 0 | 0 |
| Int64 -> Text | 10,000 | 2,985,668 | 10,000 | 100,000 |
| Text -> Int64 | 100,000 | 3,415,505 | 0 | 0 |
| Int64 -> Text | 100,000 | 7,496,275 | 100,000 | 1,000,000 |

The finalizer remains one S1 scan with `O(rows * actions)` work and no retained
per-row conversion cache. Int64-to-Text necessarily allocates one output String.

Final validation used the exact repository matrix: formatting, all-target and
all-feature check/Clippy, the complete offline workspace suite, the Rust 1.85.0
MSRV check, Go tests, generated-SDK verification, and `git diff --check` all
passed. The real PostgreSQL 17.11 fixture passed. Dynamic fuzz inventory found
13 existing targets (`btree_decode`, `coordinator_log_decode`,
`index_catalog_decode`, `lsm_manifest_decode`, `lsm_sstable_decode`,
`lsm_wal_decode`, `page_decode`, `partition_catalog_decode`, `pgwire_decode`,
`protocol_decode`, `schema_catalog_decode`, `schema_mutation_decode`, and
`wal_recovery`); each passed 1,000 runs with seed 59. No parser/decoder changed,
so no new fuzz target was added.

Round 60 productionized the exact selected matrix through normal HIR, routed
every evaluator path through the one kernel, retained conservative optimizer
fallback, added transport-neutral conversion error kinds and PostgreSQL
mapping, and exposed manual atomic shadow migration. It keeps
`ALTER COLUMN TYPE ... USING` absent.

Round 61 or later may add that syntax as ergonomic lowering. It must explicitly
synthesize a new ColumnId, bind USING against the old schema, own index
evacuation/recreation policy, and document that physical conversion does not
preserve the old ColumnId. Foreign keys, CHECK/UNIQUE/generated/default
constraints, unique or multicolumn indexes, imported/partitioned/LSM migration,
and float/Bytes conversion remain out of scope.
