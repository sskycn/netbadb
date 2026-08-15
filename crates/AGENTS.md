# Rust crate rules

These rules apply to every crate under `crates/` in addition to the repository
root rules. `netbadb-storage` has further persistent-format rules in its own
instruction file.

## Crate boundaries

- Each crate MUST represent a real architectural boundary with concrete
  behavior. Do not create empty future crates.
- Dependencies MUST point toward lower-level primitives as defined in the root
  dependency graph.
- `netbadb-core` is the composition facade. Lower-level crates MUST NOT depend
  on it.
- Shared IDs and scalar primitives belong in `netbadb-types`; canonical schema
  concepts belong in `netbadb-schema`.
- Code generators MUST convert their input through `netbadb-schema` and use
  its canonical fingerprint implementation rather than reproducing schema
  validation or identity algorithms in a target language.
- An executor MUST consume physical decisions. It MUST NOT rediscover planner
  policy or compile raw syntax.

## Typed compiler and query layers

When editing parser, HIR, relational IR, compiler, planner, or executor code:

- AST represents syntax and source spans.
- HIR represents resolved names and checked types.
- Logical plans represent relational meaning without storage choices.
- Physical plans represent selected execution operators.
- Executor code evaluates physical plans against safe storage APIs.

Do not merge these stages for convenience. Resolved entities SHOULD use stable
IDs; names remain useful at parsing, lookup, diagnostics, and external
boundaries.

Expressions and plans MUST use explicit enums/structs, not
`HashMap<String, Value>`, JSON, or arbitrary strings. Type-checking changes MUST
have successful and rejected-program tests. Parser changes SHOULD test valid and
invalid syntax, precedence, associativity, spans, and relevant edge cases.

Database NULL semantics MUST be deliberate. Rewrites and execution changes must
consider three-valued logic, duplicates, ordering, aggregation, outer joins,
and `LIMIT` whenever those concepts are involved.

## Executor and planner discipline

- Keep logical rewrites separate from physical implementation selection.
- Optimization rules MUST preserve observable semantics.
- Do not add a cost model until useful statistics exist.
- Correct row-at-a-time execution is acceptable. Keep APIs open to future batch
  execution without implementing speculative vectorization or zero-copy paths.
- Avoid unnecessary per-row allocation and cloning, but optimize only with
  evidence.

## Library quality

- Public types MUST have stable, documented invariants and domain-specific
  errors.
- Crate-local implementation details SHOULD remain private.
- Production paths MUST return structured errors for user-controlled input.
- Unit tests protect local invariants; integration tests protect crate
  boundaries; end-to-end tests protect vertical behavior.
