# NetbaDB local operator protocol v7

NBOP v7 is the current local Unix-domain operator contract. NBOP v6 and v5 are
historical DTO contracts and are explicitly rejected on the wire. Native
Protocol v2, PostgreSQL wire, Inspection JSON v7, and all database persistent
formats are unchanged.

| Contract | Current version |
| --- | ---: |
| Deployment Manifest | 11 |
| NBOP | 7 |
| NBMR | 3 |
| Native Protocol | 2 |
| Inspection JSON | 7 |

## Framing and errors

The frame remains exactly 12 bytes: `NBOP`, big-endian `u16` version 7, zero
reserved `u16`, and a big-endian `u32` JSON payload length. The payload cap
remains 65,536 bytes. Receipt pages are never silently truncated: an encoded
page above the cap receives the existing bounded `response_too_large` fallback
and the caller may retry with a smaller limit. One maximum public receipt fits,
so `limit = 1` always makes progress without truncating Column IDs, an index
name, or a placement key.

Every v7 remote error has `code`, bounded `message`, nullable `receipt`, and
nullable `admission`. All non-admission errors carry `admission: null`.
Preflight/listener failures normally carry `receipt: null`. A worker failure
after durable Begin may carry the new request's scoped reference. Journal Begin
capacity rejection maps to
`physical_design_mutation_receipt_capacity_exceeded`; other Begin/journal
failure maps to `physical_design_mutation_receipt_unavailable`. Neither case
executes the mutation and neither fabricates a receipt.
An existing receipt recovery gate uses the same unavailable code, states that
restart/reopen is required, and still means the new request did not enter Core.

## Scoped identity

A wire receipt identity is exactly:

```json
{
  "journal_incarnation": "00112233445566778899aabbccddeeff",
  "receipt_id": 41
}
```

The incarnation is exactly 16 bytes encoded as 32 lowercase hexadecimal
characters. Uppercase, prefixes, UUID dashes, base64, short forms, whitespace,
the all-zero incarnation, and zero receipt IDs are invalid. This type is
distinct from the same-sized ephemeral runtime token. A receipt reference
identifies one record; a cursor means continue strictly after it. Neither is a
mutation, retry, replay, or authorization capability.

## Status and scoped pagination

For receipt access, general Physical Design status exposes only the operator
capability projection:

```json
"physical_design_mutation_receipts": {"read_enabled": true}
```

It does not disclose hidden programmatic configuration. The dedicated status
operation is:

```json
{"request_id": 1, "operation": {"type": "physical_design_mutation_receipt_status"}}
```

Its result contains `journal_incarnation`, `recovery_required`, nullable
`latest_receipt_id`, and `max_receipts_per_read` (128). It contains no path,
capacity, database incarnation, or runtime token.

The list operation is:

```json
{
  "request_id": 2,
  "operation": {
    "type": "physical_design_mutation_receipts",
    "after": {
      "journal_incarnation": "00112233445566778899aabbccddeeff",
      "receipt_id": 41
    },
    "limit": 32
  }
}
```

Use `after: null` for the first page. A page contains its journal incarnation,
ascending complete receipts, and nullable `next_after`. A bare numeric cursor
is never accepted. Limits outside 1–128 map to
`invalid_physical_design_mutation_receipt_limit`; malformed scoped cursors map
to `invalid_physical_design_mutation_receipt_cursor`. A cursor from another
journal maps to `physical_design_mutation_receipt_journal_changed`; the server
never restarts at receipt 1 or searches old journals. Other read failures map
to path-private `physical_design_mutation_receipt_read_failed`.

Receipt reads require the Manifest v11 read permission. Disabled permission
maps to `physical_design_mutation_receipt_read_not_allowed` before forwarding;
a permitted but journal-less defensive state maps to
`physical_design_mutation_receipts_not_enabled`. Status and list remain pure
and available while `recovery_required` gates new receipt-controlled mutations.

## Receipt representation

`source` is `programmatic` or `local_operator`. Targets are tagged `index`
(`table_id`, `column_id`, `index_name`) or `columnar` (`table_id`, ordered
`columns`, explicit `mode`, logical `placement_key`). Outcomes cover `pending`,
`created_index`, `created_columnar`, `already_applied_index`,
`already_applied_columnar`, `already_covered`, `rejected`, historical `failed`,
`recovered_applied_index`, `recovered_applied_columnar`,
`recovered_not_applied`, and `recovered_conflict`. Owning outcomes alone carry
`index_id` or `projection_id`. `evidence_epoch` is historical context, never
current approval authority.

No receipt surface exposes the NBMR path, private Columnar recovery path,
database incarnation, SQL, principal, session, network address, or a receipt
timestamp. The runtime token remains only in its existing approval fields.

## Apply correlation and uncertainty

Index and Columnar apply results add nullable `receipt`. With journaling
disabled it is null. Once durable Begin succeeds, the same single worker
command carries the resulting scoped reference through success or semantic
error; there is no follow-up receipt lookup. Success is returned only after a
durable Outcome.

If Database mutation may have occurred but Outcome cannot become durable, the
server returns `physical_design_mutation_outcome_uncertain` with the durable
receipt reference and guidance to restart/reopen, allow startup reconciliation,
then inspect that receipt. Immediate retry is not advised while the recovery
gate is active. Whole-response loss is distinct: the client has no observed
reference, reports local uncertainty with `recovery_required = false`, and may
retain exact-approval retry guidance. No receipt lookup heuristic or automatic
retry is performed.

The same uncertainty code with `receipt: null` covers an accepted worker
command whose reply was lost, or an ambiguous Core Index/Columnar apply error
when the optional journal is disabled. Core `Database` and `Advisor(Database)`
apply errors cannot prove that mutation did not occur, including failures after
index/projection creation. The programmatic API returns typed unjournaled
mutation uncertainty and retains the original error as its source. Without a
receipt, the client reports `recovery_required = false`; the exact same approval
may be used for idempotent discovery. Definite pre-mutation rejections retain
their existing typed errors, and no automatic retry or evidence refresh occurs.
An explicit Columnar `Database(ProjectionCatalog::RecoveryRequired)` takes
precedence over generic unjournaled uncertainty. Without a journal, the Server
preserves the original typed apply error and returns the existing
`physical_columnar_recovery_required` code with `receipt: null` and mandatory
restart/reopen-before-retry guidance. The client preserves this as `Remote`,
not `MutationOutcomeUncertain { recovery_required: false, .. }`.
With `receipt` present, this Core recovery error still uses receipt-aware
mutation uncertainty: the Server knows a durable Begin exists, gates the
journal, and requires startup reconciliation followed by receipt inspection.
Client classification uses the code and nullable receipt, never message text.
Connect/configuration and request encoding failures before dispatch remain
ordinary definite local failures; protocol, request-ID, response-shape, or
response-loss failures after dispatch are local mutation uncertainty.

The remaining v7 operations retain v6 semantics: `status`, `rotate_evidence`,
`reset_faulted_scheduler`, `physical_design_recommendations`,
`rotate_physical_design_evidence`, `apply_physical_index`, and
`apply_physical_columnar`.

## Deployment-owned component admission

The operator approves only a logical design. Existing apply request fields are
unchanged; neither Index nor Columnar accepts `budget`, `limits`, `admission`,
`max_*`, inspection, expected bound, or cached report fields. Unknown fields fail
strict decoding. No admission operation, SQL syntax or CLI budget flag exists.
One approved request remains one worker command. The listener forwards logical
approval only, while the worker selects its immutable Manifest v11 Index,
Snapshot, or Incremental mode and calls the corresponding Core apply API.

Index status adds `admission`; Columnar status adds `snapshot_admission` and
`incremental_admission`. Each is either `{"mode":"unadmitted"}` or
`{"mode":"component_limits","policy":{...}}`. Policy has exactly the six
explicit constraint objects named in [Manifest v11](server-manifest-v11.md).
Status presents the same resolved deployment configuration that the worker uses;
it does not inspect current work, scan sources, ANALYZE, flush, or report current
bytes/pages/bounds. Recommendations remain workload recommendations only.

Admission failures share code `physical_design_mutation_admission_rejected`.
The nullable `admission` field carries exactly one typed shape:

```json
{"kind":"required_bound_not_proven","dimension":"output_write_bytes"}
```

```json
{"kind":"limit_exceeded","dimension":"source_read_bytes","conservative_bound":12345,"maximum":10000}
```

```json
{"kind":"inspection_failed"}
```

```json
{"kind":"recovery_required"}
```

`recovery_required` is the only public DTO expansion from v6 to v7. It means the
typed storage inspection encountered an already recovery-gated runtime before
mutation authority. The error code remains
`physical_design_mutation_admission_rejected`, and the bounded message is exactly
`current mutation-work inspection requires restart/reopen before retry`. The
receipt is null when NBMR is disabled and is the durable Begin reference when
NBMR is enabled. No internal error text or filesystem path is exposed. Frozen
v6 decoders reject this shape.

Dimension tags are `source_work_units`, `source_read_bytes`,
`prerequisite_work_units`, `prerequisite_read_bytes`,
`prerequisite_write_bytes`, and `output_write_bytes`. InspectionFailed exposes
no underlying Database/Storage display, path, or recovery detail. Listener
rejections have null admission and receipt fields.

Durable Begin remains before semantic processing/admission. Rejection writes a
coarse NBMR v3 `Rejected` and the remote error carries that exact receipt reference.
Without NBMR, receipt is null. No policy, dimension, bound, maximum or inspection
is persisted in receipts. Mutation/Outcome uncertainty and whole-response loss
retain their independent classifications and never fabricate an admission reason.

Exact AlreadyApplied precedes stale runtime and admission, including after restart
under stricter limits. AlreadyCovered precedes admission. Runtime/evidence/current
recommendation errors retain their existing order before admission. Policies do
not determine token bytes; every daemon lifetime has a new random token.
Core recomputes current work immediately before a still-needed mutation.

Constrained NotProven fails closed, equality passes, and unconstrained components
remain outside the policy. This is partial component admission, not a total cost
model. Current initial Columnar builds provide a storage-authored
`output_write_bytes` bound; Snapshot LSM source read is also bounded after its
prospective flush output is included. Index output remains NotProven. These are
stronger values for the existing v6/v7 dimensions, not new fields or policy
widening.
The CLI renders exact components and structured failures, correlates a
returned receipt, and never retries, relaxes a limit, switches mode, or queries
receipts automatically. No automatic design, evidence rotation, stream enablement,
scheduler action, or client transaction-state change is introduced.

V5 and V6 public Rust DTOs are frozen in `operator_v5.rs` and `operator_v6.rs`;
only V7 is accepted on the wire. The listener and client reject v6, v5, and all
other version numbers with `unsupported_protocol_version`. Header size and
payload cap, NBMR v3, Native Protocol v2, PostgreSQL wire, Inspection JSON v7
and all database persistent formats are unchanged.
