# Virtual projected rows for deferred backfill (Round 53 audit)

Round 53 is an architecture audit with an executable, test-only prototype. It
does **not** enable late-column reads in production. Production still accepts
only deferred UPDATEs whose right-hand sides and predicates read surviving base
columns; a late-column RHS or WHERE expression falls through to
`MigrationDataAccessAfterRefinement` (`25000` over PostgreSQL, then `25P02` in
an explicit failed transaction).

The audit started from `5adfa29c866ac32909834dbb78745a417d48b03e`, preserving
the Round 52 guard at `358b06d89c72cb35c299300546f33a54ac0a00fc` and
Columnar Phase 2D at `5adfa29c866ac32909834dbb78745a417d48b03e`.

## Decision

Candidate A wins: a deferred migration's logical row is the current projected
target row reconstructed from frozen S1 plus the accepted action prefix.

```text
transaction-visible S1/P1 row
    -> RowProjection without target constraints
       (surviving ColumnIds copied, reserved late ColumnIds synthesized NULL)
    -> action 0
    -> action 1
    -> ...
    -> VirtualRow N
```

For action N, its WHERE and every RHS read one immutable snapshot of
`VirtualRow N`. Assignment results are computed first and applied together, so
same-statement assignment is simultaneous. The resulting `VirtualRow N+1` is
visible to later statements. This is an in-memory deterministic transform, not
a Heap, MVCC participant, sidecar, Columnar source, or durable authority.

No new phase is needed. The first accepted action, including one that reads a
new column's synthesized NULL, enters `AdoptedSourceBackfilling`; the existing
`AdoptedSourceRefining -> AdoptedSourceBackfilling ->
AdoptedSourceIndexFinalizing` lifecycle remains sufficient.

## Executable model

The behavior-neutral core refactor changes `EvaluationLayout` from S1
ordinals to checked target-row positions. Both base and late positions are
resolved by exact `(TableId, ColumnId)` identity against the current target
definition; names never establish identity. A surviving base value in the
target position is exactly the value copied there by `RowProjection`, including
when a dropped column changed its ordinal or a rename retained its ColumnId.

The production builder still selects `SurvivingBase` read authority. Only a
`#[cfg(test)]` audit entrance selects `VirtualProjected`, where the readable
set is:

```text
same TableId
AND (surviving base ColumnId OR durably reserved visible late ColumnId)
```

Foreign, dropped, unknown, and merely current-but-unreserved columns remain
ineligible. Assignment targets remain reserved late columns only. The test-only
entrance is not called by `execute_prepared_in`, any server path, SDK, or public
API.

The shared action primitive now evaluates against the pre-action projected row
and applies all results afterward. Execute reconstructs each row from S1 plus
the existing prefix, evaluates the candidate, and appends only after the full
scan and prefix-observation verification succeed. Finalization uses the same
primitive while streaming S1 once into the one final S2.

The full fixture proves:

- the first `WHERE marker IS NULL` sees synthesized NULL and fills the two rows
  whose surviving `legacy` value is non-NULL;
- failed projected `SET marker NOT NULL` leaves metadata repairable;
- `WHERE marker IS NULL` then repairs exactly one row;
- after `marker` becomes NOT NULL, `normalized = marker` reads all three prior
  values;
- a NULL Execute-bound scalar assigned to NOT NULL `marker` fails during
  action Execute and is not appended;
- a zero-match late predicate is an ordered accepted action with
  `AffectedRows(0)`;
- final NOT NULL constraints, one physical BTree on `normalized`, one S2, one
  source copy pass, three copied rows, and three reopens preserve exact values;
- the S1 index digest and physical source schema remain unchanged before
  finalization.

Separate fixtures prove that `SET marker='new-marker', normalized=marker`
stores the old marker in `normalized`, and `SET marker=normalized,
normalized=marker` swaps the two pre-action values.

## Prepared statements and parameters

A statement prepared before its producer is reusable after the producer when
the table identity, version, and fingerprint are unchanged. Execute binds the
prepared statement, reconstructs the then-current VirtualRow from S1 and the
current prefix, and sees the producer's values. Program growth is not a schema
dependency and therefore does not make the statement stale.

A successful nullability or other schema change still changes the existing
dependency evidence. An older statement then fails with
`StalePreparedStatement`; there is no program-version bypass. Parameters are
bound before the audit route and retained only as owned `ScalarValue` literals.
Two executions with different bound values produce different action digests.

## Affected rows and observation evidence

Every candidate Execute performs one frozen S1 scan. For every S1 row it
replays the ordered prefix, evaluates the predicate once against the pre-action
row, and evaluates all RHS expressions against that same row. The exact match
count is returned. There is no estimate and zero matches remain valid.

The existing `ActionObservation` remains sufficient and is not versioned again.
It binds the original source row and the ordered assignment ColumnIds/results.
Earlier observations bind every producer result; ordered v1 semantic digests
bind each expression and the whole prefix. Finalization recomputes those same
observations. Injecting a mismatch returns hard `Corrupt` before publication.
A separate pre-action input hash would duplicate evidence without protecting a
new authority.

The semantic domain remains exactly:

```text
NetbaDB deferred backfill action v1\0
```

Existing base-only action meaning is unchanged. Late ColumnIds extend a
previously rejected input domain rather than reinterpret old bytes. The Round
50 golden digest remains:

```text
1e1bc04b766e0d63bbda95f5d975439f35442d33afc73143e5d68232458216a0
```

Tests distinguish a marker RHS from another late RHS, `IS NULL` from `IS NOT
NULL`, forward from reverse action order, and producer literal changes. A
consumer action's own digest remains equal when only its producer changes,
while the ordered whole-program/action digest changes, proving prefix binding.

## Cost observation and action limit

Candidate action N costs one S1 scan plus N prior action evaluations per row.
Accepting a whole N-action program therefore has cumulative `O(rows * N^2)`
CPU, while resident state remains `O(actions + expression bytes)` and not
`O(rows)`. Finalization remains `O(rows * N)`, one S1 pass, and one S2 copy.

The deterministic debug-build probe below ran on the Round 53 development host.
Elapsed time is observational, not an assertion; evaluation counts and bounded
metadata estimates are asserted. The estimate covers owned action/layout/
assignment vector storage and intentionally does not claim allocator-exact
resident size.

| Rows | Actions | Evaluations | Elapsed (µs) | Metadata estimate (bytes) |
| ---: | ---: | ---: | ---: | ---: |
| 1,000 | 1 | 1,000 | 2,327 | 1,064 |
| 1,000 | 4 | 4,000 | 6,395 | 4,184 |
| 1,000 | 8 | 8,000 | 10,120 | 8,344 |
| 1,000 | 16 | 16,000 | 15,651 | 16,664 |
| 1,000 | 32 | 32,000 | 24,909 | 33,304 |
| 10,000 | 1 | 10,000 | 9,152 | 1,064 |
| 10,000 | 4 | 40,000 | 27,755 | 4,184 |
| 10,000 | 8 | 80,000 | 51,296 | 8,344 |
| 10,000 | 16 | 160,000 | 100,451 | 16,664 |
| 10,000 | 32 | 320,000 | 201,946 | 33,304 |
| 100,000 | 1 | 100,000 | 81,800 | 1,064 |
| 100,000 | 4 | 400,000 | 270,913 | 4,184 |
| 100,000 | 8 | 800,000 | 517,761 | 8,344 |
| 100,000 | 16 | 1,600,000 | 1,017,045 | 16,664 |
| 100,000 | 32 | 3,200,000 | 2,005,391 | 33,304 |

Recommendation for Round 54: retain the existing hard limit of 32. The curve
is linear for a fixed prefix and bounded at a deliberately small N; there is no
evidence yet that a lower limit is needed. Keep the limit observable and revisit
it only with workload evidence. This does not justify unbounded programs.

## Candidate comparison

| Criterion | A: projected prefix | B: RowHandle sidecar | C: symbolic composition | D: early S2 DML | E: repair special case |
| --- | --- | --- | --- | --- | --- |
| natural SQL | yes | yes | superficially | yes | no |
| late WHERE / RHS | both | both | difficult predicates | both | WHERE repair only |
| first synthesized NULL | direct | must initialize map | symbolic special case | requires early copy | narrow special case |
| simultaneous assignment | shared evaluator | extra snapshot discipline | difficult substitution | executor-dependent | incomplete |
| ordered later-wins | direct prefix | mutable sidecar | expression explosion | physical writes | only chosen cases |
| exact affected rows | full scan | full scan/map | difficult with predicates | physical DML | narrow only |
| resident memory | not `O(rows)` | `O(updated rows)` | grows with composition | storage-backed | small |
| Execute CPU | prefix replay | lower after map build | potentially high/complex | ordinary DML | low but incomplete |
| one final S2 | yes | yes | yes if solved | no/risks S3 | yes |
| S1 sole pre-final truth | yes | no, second value authority | ambiguous | no | yes |
| rollback complexity | existing | sidecar lifecycle | new symbolic semantics | target transaction lifecycle | special path |
| source digest frozen | yes | yes physically | yes physically | no | yes |
| new persistent format | no | pressure to spill/persist | maybe program encoding | existing Heap but early authority | no |
| recovery changes | no | likely | likely if durable | yes | special-case risk |
| digest compatibility | v1 retained | new sidecar evidence | new canonical form | physical history changes | special evidence |
| implementation risk | lowest | high | highest semantic risk | violates architecture | accumulates debt |

Candidate B is rejected because RowHandle-keyed values become a second
transaction authority, consume row-proportional memory, couple to physical row
identity, and add rollback/spill pressure. Candidate C is rejected because
predicates, partial fills, later-wins, NULL logic, and simultaneous assignment
make symbolic substitution a second SQL evaluator. Candidate D is rejected
because early S2 breaks S1/P1 authority, the one-final-S2 theorem and Round 52
guard timing, and can require S3. Candidate E is rejected because it cannot
naturally express late-to-late copies or general late predicates and would fork
evaluation semantics.

## Change Stream, Columnar, and recovery

The successful audit fixture uses the default Disabled/never-enabled stream.
An otherwise complete virtual program with an Enabled S1 stream still fails
`ActiveChangeStreamBlocksReplacement` before S2 allocation. The retained Round
52 unavailable-stream fixture proves the same `Unavailable` result. There is no
late-read exception and the administrator must still explicitly rebaseline.

VirtualRow always starts from the transaction read view of authoritative S1/P1.
It never reads NBCS/NBCD, even if a projection is fresh. Phase 2B incremental,
Phase 2C compaction/retention, and Phase 2D lazy indexed I/O remain derived
acceleration and retain their authoritative fallback behavior.

The full test-only program runs through the existing tag-25, tag-35, stage,
mid-copy, prepared, CORD, partial-commit, reverse-participant, and all-commit
crash matrix. Pre-CORD opens choose S1 and lose the in-memory program. Post-CORD
opens choose the already materialized S2 with exact late values and index.
Three repeated opens converge. Recovery never decodes or evaluates an action.

## Persistent and external compatibility

| Surface | Round 53 impact |
| --- | --- |
| Canonical Schema | none |
| NBSJ tags 1--35 | none; no action/AST record |
| NBSC / NBSM | none |
| CORD | none |
| Heap / Page / WAL / transaction status | none |
| NBCL / Change Stream cursor | none |
| NBCM v1/v2/v3 | none |
| NBCS v1/v2/v3 | none |
| NBCD v1/v2 | none |
| NBPC | none |
| IndexCatalog / BTree | none |
| Protocol v1 / PostgreSQL framing and SQLSTATE | none |
| deployment Manifest | none |
| SDK Schema Spec / generated SDKs | none |
| inspection DTO/JSON | none |

The program remains transaction-local memory. No NBSJ program bytes, virtual
row records, sidecar files, StorageId, S3, replay codec, protocol field, or
inspection surface is introduced.

## Exact Round 54 production scope

Round 54 should productionize Candidate A only for same-table deferred UPDATE:

- writes: durably reserved late-added columns only;
- reads: surviving base columns, reserved visible late-added columns, literals,
  and Execute-bound scalars;
- each statement reads its pre-statement VirtualRow;
- initial late columns are synthesized NULL;
- same-statement assignments are simultaneous;
- multiple statements are ordered and later writes win;
- structural ALTER freezes after the first action;
- the Round 48 final-index phase remains terminal;
- the Round 52 Enabled/Unavailable replacement guard remains mandatory.

It must still exclude general post-refinement SELECT, INSERT, DELETE,
base-column UPDATE, structural ALTER after backfill, DEFAULT, generated columns,
new function/arithmetic syntax, subqueries, joins, cross-table reads,
UNIQUE/multicolumn indexes, LSM/partitioned/imported tables, and online or
resumable migration.

## Resulting theorem

A deferred migration already defines a logical target row before S2 exists:
transaction-visible S1, exact-ID projection with synthesized NULL late columns,
and ordered accepted actions. That row is sufficient as the pre-statement read
view for later deferred UPDATEs. Execute reconstructs it with one S1 scan;
finalization reconstructs it with the same primitive during one S1-to-S2 pass.
No second physical or durable value authority is necessary. Existing base-only
v1 meaning, Round 52 replacement safety, Columnar derivation, and every binary
and wire format remain intact.
