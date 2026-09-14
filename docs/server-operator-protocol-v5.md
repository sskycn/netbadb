# NetbaDB local operator protocol v5

NBOP v5 is the current local Unix-domain operator contract. NBOP v4 is
historical and explicitly rejected. Native Protocol v2, PostgreSQL wire,
Inspection JSON v7, and all database persistent formats are unchanged.

## Framing and errors

The frame remains exactly 12 bytes: `NBOP`, big-endian `u16` version 5, zero
reserved `u16`, and a big-endian `u32` JSON payload length. The payload cap
remains 65,536 bytes. Receipt pages are never silently truncated: an encoded
page above the cap receives the existing bounded `response_too_large` fallback
and the caller may retry with a smaller limit. One maximum public receipt fits,
so `limit = 1` always makes progress without truncating Column IDs, an index
name, or a placement key.

Every v5 remote error has `code`, bounded `message`, and nullable `receipt`.
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

General Physical Design status adds only the operator capability projection:

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

Receipt reads require the Manifest v10 read permission. Disabled permission
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
command whose reply was lost: no durable reference was observed and
`recovery_required` is not asserted by the client. With `receipt` present, the
Server knows a durable Begin exists and startup reconciliation is required.
Client classification uses the code and nullable receipt, never message text.
Connect/configuration and request encoding failures before dispatch remain
ordinary definite local failures; protocol, request-ID, response-shape, or
response-loss failures after dispatch are local mutation uncertainty.

The remaining v5 operations retain v4 semantics: `status`, `rotate_evidence`,
`reset_faulted_scheduler`, `physical_design_recommendations`,
`rotate_physical_design_evidence`, `apply_physical_index`, and
`apply_physical_columnar`.
