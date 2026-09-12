# NetBaDB Operator Protocol v3

NBOP v3 is the sole current local operator contract. NBOP v2 is historical and
is explicitly rejected with `unsupported_protocol_version`. It is independent
of Native Protocol v2, PostgreSQL wire, Inspection JSON v7, and database
persistent formats.

## Trust, framing, and lifecycle

The operator remains one serial AF_UNIX listener on a mode-`0600` socket.
Filesystem access authenticates the local operator. Manifest v8 additionally
requires the explicit `allow_physical_index_apply` permission for durable index
apply. There is no username/password, JWT, TLS, peer-UID audit, persistent
operator identity, or durable approval log.

The fixed header is unchanged:

| Offset | Size | Meaning |
| ---: | ---: | --- |
| 0 | 4 | ASCII `NBOP` |
| 4 | 2 | big-endian unsigned version, exactly `3` |
| 6 | 2 | reserved, exactly zero |
| 8 | 4 | big-endian JSON payload length |

The cap remains 65,536 bytes and is checked before allocation. Every connection
has exactly one request and one response. All JSON objects reject unknown
fields. Wrong magic, nonzero reserved bytes, truncated frames, invalid JSON,
unknown operations, and oversized payloads fail closed. `request_id` is echoed
but never stored for deduplication.

Read/write timeouts cover request framing and response writing. Once a complete
apply request is forwarded, disconnect or timeout does not cancel the worker
operation. The client must not retry automatically.

## Existing operations

The v3 forms of `status`, `rotate_evidence`, `reset_faulted_scheduler`,
`physical_design_recommendations`, and `rotate_physical_design_evidence` retain
their v2 meaning. Adaptive and Physical Design snapshots remain individually
worker-serialized but are not a cross-domain transactional snapshot.

Physical Design status adds:

```json
{"physical_index_apply":{"enabled":true,"runtime_token":"00112233445566778899aabbccddeeff"}}
```

When disabled, `runtime_token` is `null`. Recommendations also return
`runtime_token` beside the report, so the token and report come from one
operator-plane lifetime. The token is exactly 16 random bytes encoded as 32
lowercase hexadecimal characters. Uppercase, `0x`, UUID dashes, short forms,
and base64 are invalid. It is ephemeral, never persisted or printed in daemon
readiness, changes on operator/server restart, and is unchanged by explicit or
schema-driven evidence rotation. It is a stale cross-restart approval guard,
not an authentication secret; filesystem permissions remain authentication.

## Explicit physical-index approval

An apply request names only the current logical candidate and explicit typed
preconditions:

```json
{"request_id":6,"operation":{"type":"apply_physical_index","expected_runtime_token":"00112233445566778899aabbccddeeff","expected_evidence_epoch":7,"table_id":1,"column_id":3,"index_name":"idx_users_email"}}
```

`table_id` and `column_id` are canonical logical IDs. Names, SQL fragments, and
`CREATE INDEX` text are not accepted as candidate identity. `index_name` is
constructed only with `IndexName::new`; invalid names return
`invalid_index_name`.

The listener checks Manifest permission, canonical token syntax, token equality,
and the typed index name, then forwards exactly one command to the sole Database
worker. The worker performs, without an interleavable second command:

1. current Core exact-name classification;
2. `AlreadyApplied` for the same name/TableId/ColumnId, or
   `physical_index_name_conflict` for another target;
3. runtime-token match;
4. exact current evidence epoch;
5. a fresh Core proposal from current worker evidence and fixed policy;
6. current recommendation revalidation;
7. immediate Core Phase 23 apply through the existing named `CREATE INDEX`.

The Core proposal is a worker-local temporary value. No Core or Server proposal,
database incarnation, schema/storage anchor, evidence summary, runtime identity,
or policy snapshot crosses the wire or is cached.

Success is one small result with the requested IDs/name and one stable outcome:
`created {index_id}`, `already_applied {index_id}`, or `already_covered`.
`already_covered` performs no mutation because current physical state already
covers the candidate.

## Retry theorem and errors

An exact same-runtime retry after a lost response returns `already_applied`.
After restart, an old approval may still confirm an exact durable named index as
`already_applied`; exact-name truth deliberately precedes token and epoch checks.
If that name was not committed, the old token returns
`physical_design_runtime_changed`, even when both runtimes happen to call their
first cohort D0. A matching token with a rotated cohort returns
`physical_design_evidence_epoch_changed`, unless exact-name idempotency already
applies. Neither stale condition reserves an IndexId or changes G.

Stable apply-related errors are `physical_index_apply_not_enabled`,
`physical_design_runtime_changed`, `invalid_index_name`,
`physical_index_candidate_not_observed`, `physical_index_not_recommended`,
`physical_index_name_conflict`, and `physical_index_apply_failed`. Existing
Physical Design and protocol errors retain v2 meanings, including
`physical_design_not_enabled`, `physical_design_evidence_epoch_changed`,
`physical_design_no_evidence`, `physical_design_stale_schema`,
`physical_design_inconclusive_capacity`, `server_stopped`, `internal`, and
`response_too_large`. Rust Debug/internal Database errors never cross the wire.

There is no automatic retry, token/epoch refresh, candidate selection, apply
cache/history, request JSON log, scheduler-triggered/query-triggered/timer apply,
background builder, trial/revert, or Columnar projection apply. Ordinary
single-column named non-unique single-storage Heap B+Tree `CREATE INDEX` remains
the only mutation, durability, rollback, WAL, and recovery authority.
