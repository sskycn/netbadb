# Production cross-physical CAST and atomic shadow type migration (Round 60)

Round 60 promotes the typed CAST architecture selected in Round 59 to the one
production expression semantics used by native SQL, PostgreSQL, ordinary query
execution, prepared execution, Columnar input and deferred migration replay.
`CAST(expr AS TYPE)` and `ALTER COLUMN TYPE ... USING` remain absent; physical
replacement is an explicit shadow-column workflow.

## Typed cast contract

`PhysicalType::supports_explicit_cast_to` is the sole pair authority. HIR asks
it whether a resolved source and target pair is admissible; the executor asks
it again as a malformed-IR defense. Scalar conversion itself remains in the
executor. The 225 ordered pairs reduce to these rules:

| Source | Supported targets |
| --- | --- |
| every type | its exact identity |
| any signed/unsigned integer | every signed/unsigned integer, checked |
| `Text` | every integer and `Bool` |
| every integer | `Text` |
| `Bool` | `Text` |

Bool/numeric, every cross-physical Float pair (including Float32/Float64),
Float/Text and every nonidentity Bytes pair are statically rejected. There is
no implicit numeric widening and explicit-cast support does not loosen ordinary
assignment, comparison or parameter compatibility.

HIR resolves the child before validating the pair. A string literal in
`'42'::BIGINT` remains Text; an uncontextualized integer in `256::UINT8`
remains Int64 and fails only during checked execution. `NULL::BIGINT` uses the
target solely as its NULL carrier and never parses. An untyped `$1::BIGINT`
uses BIGINT as parameter context, while a frontend-declared Text `$1` remains
Text and undergoes Text-to-Int64 conversion. Existing nominal same-physical
compatibility remains exact.

## Deterministic conversion and errors

Text-to-integer accepts ASCII decimal digits with at least one digit and
leading zeroes. Signed targets additionally accept one leading `-`, including
`-0`; unsigned targets accept no sign. Whitespace, `+`, separators, decimal
points, exponents and Unicode digits are invalid. Integer-to-Text emits
canonical base-10 ASCII. Bool-to-Text emits `true` or `false`; Text-to-Bool
accepts only those exact lowercase tokens. Integer conversion is checked and
never wraps, truncates or reinterprets bits.

| Executor error | Database category | PostgreSQL SQLSTATE |
| --- | --- | --- |
| `InvalidCastText` | `InvalidTextRepresentation` | `22P02` |
| `CastOutOfRange` | `NumericValueOutOfRange` | `22003` |
| `UnsupportedCast` | `CannotCoerce` | `42846` |
| `InvalidCastInput` | `Internal` | `XX000` |

Unsupported pairs normally stop in HIR as `UnsupportedCast` /
`CompileErrorKind::CannotCoerce`; the executor variant protects manually
constructed or corrupt typed IR. PostgreSQL transaction failure and subsequent
`25P02` behavior are unchanged. UInt64/Int128/UInt128 can execute in embedded
and native Protocol v2 paths, but PostgreSQL still rejects result reflection
when it has no lossless carrier.

## Execution and planning

`ExprKind::Cast` survives logical binding. Bound, borrowed/dynamic,
contiguous, filter short-circuit, join and deferred-row evaluators all call the
same kernel. Identity casts validate the runtime variant while retaining a
borrowed fast path where possible. A skipped `AND`/`OR` branch does not perform
conversion, and deferred actions evaluate their predicate before assignment
RHS expressions.

The planner deliberately does not look through a cross-physical cast for
B+Tree point/range access, Columnar zone-map constraints, partition pruning or
hash/index join key extraction. Columnar may supply raw values, after which the
ordinary scalar evaluator converts them. Statement inspection retains a typed
Cast node with source, target and child rather than describing the child alone.

## Atomic shadow migration

The production workflow is:

```text
S1 / old C2 Text / old I1
  -> ordinary repair DML
  -> ADD late shadow C4 Int64
  -> UPDATE shadow = old::BIGINT
  -> SET shadow NOT NULL
  -> DROP I1 and DROP C2
  -> RENAME C4 to the public name
  -> CREATE fresh I2 on C4
  -> one S2 and one atomic structural publication
```

The conversion UPDATE is an ordinary compiled SQL statement accepted by the
existing bounded `DeferredBackfillProgram`. Execute and finalization replay use
the same kernel and the existing action-v1 digest, which already binds the
outer type and recursive typed child. A failed conversion appends no action or
partial row evidence. Repair can exclude a bad row before RHS conversion.
The source Heap and I1 remain unchanged until the winner; physical replacement
uses a new ColumnId and replacement indexes use fresh IndexIds.

The production Text-to-Int64 action golden is
`545aaff94f660c7507e33079031e321ddf2f54afc08661bda029d336155edbd6`.
Target type, source ColumnId, predicate literal and action ordering have
separate sensitivity checks; the Round 50 and Round 54 v1 goldens remain exact.

Recovery never converts values. A pre-Decision crash restores S1; a
post-Decision crash completes the already materialized S2. Existing Round 52
Change Stream replacement admission, CORD v5 group/checkpoint recovery and the
single-G structural theorem remain authoritative. Work is one source scan at
finalization with bounded row-width/program metadata and no per-row cache.

## Compatibility and scope

No workload, conversion program or outcome is persisted. Canonical Schema,
NBSC/NBSM, NBSJ tags 1-35, NBCO v1, CORD v1-v5, Heap/Page/WAL/status,
IndexCatalog/BTree, NBCL, NBCM/NBCS/NBCD/NBPC, Partition/LSM, native Protocol
v2, PostgreSQL framing, manifests, SDK schema and Inspection JSON version are
unchanged. Inspection content is more truthful for Cast but retains JSON v7.

Round 61 has now selected and executable-proved a fresh-ColumnId synthetic
lowering for `ALTER COLUMN TYPE ... USING` without opening production syntax;
see [`alter-type-using-round61.md`](alter-type-using-round61.md). Float
conversion, Bytes conversion, imported/LSM/partitioned migration, general
constraint rewriting and automatic type migration remain outside Round 60.
