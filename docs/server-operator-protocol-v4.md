# NetbaDB local operator protocol v4

NBOP v4 is the current local Unix-domain operator protocol. It is independent
of Native Protocol v2, PostgreSQL wire, Inspection JSON, and all persistent
formats. The frame remains exactly 12 bytes: `NBOP`, big-endian protocol
version, zero reserved field, and a big-endian payload length. Payloads remain
bounded to 65536 bytes. NBOP v3 is rejected explicitly.

## Runtime status

When Physical Design is present, status exposes independent `physical_index_apply`
and `physical_columnar_apply` objects. The Columnar object reports `enabled`,
`allow_snapshot`, `allow_incremental`, and an optional ephemeral
`runtime_token`. The token is shared with index approval when both permissions
are enabled; it is absent when neither permission is enabled. The root path is
never returned. Recommendations expose the same shared token, independent
`physical_index_apply.enabled` and `physical_columnar_apply` capability objects,
and the exact evidence epoch needed for a subsequent approval. The Columnar
capability includes `enabled`, `allow_snapshot`, and `allow_incremental`;
clients must not infer either permission from token presence.

## Explicit Columnar approval

The operation is `apply_physical_columnar`:

```json
{
  "request_id": 42,
  "operation": {
    "type": "apply_physical_columnar",
    "expected_runtime_token": "00112233445566778899aabbccddeeff",
    "expected_evidence_epoch": 7,
    "table_id": 1,
    "columns": [2, 3, 4],
    "mode": "snapshot",
    "placement_key": "users-v1"
  }
}
```

`columns` is an ordered, nonempty list; an empty list is rejected as
`malformed_request` before any worker command. `mode` is explicit (`snapshot` or
`incremental`), and `placement_key` is a validated logical key, not a path.
Unknown fields are rejected. The listener checks the manifest permission before
forwarding. A syntactically valid but stale token is forwarded so the worker
can report the typed stale-runtime result.

The sole Database worker handles one request as one typed
`ApplyApprovedColumnar` command. It revalidates the configured root and the
registered location, recognizes an exact already-applied location before token
and evidence checks, validates mode, runtime token, evidence epoch, and
unregistered occupancy, then derives a fresh Core proposal and immediately
delegates the apply. There is no second mutation authority.

Successful results contain only the table ID, ordered columns, explicit mode,
logical placement key, and one of `created`, `already_applied`, or
`already_covered` outcomes. Stable error codes cover disabled permission,
invalid keys, disallowed modes, unavailable or occupied placements, registered
conflicts, stale evidence/runtime, missing recommendations, Change Stream
state, recovery-required publication, and bounded apply failure. Error text
does not expose filesystem paths. `server_stopped` means the mutation command
was not accepted by the worker path. If the worker accepted the command but its
reply was lost, the response uses `mutation_outcome_uncertain`; the operator
must retry the same exact approval to obtain the idempotent durable outcome.

## CLI

The explicit command is:

```text
netbadb operator physical-design apply-columnar \
  --manifest server.json \
  --expected-runtime-token 00112233445566778899aabbccddeeff \
  --expected-evidence-epoch 7 \
  --table-id 1 \
  --column-id 2 --column-id 3 --column-id 4 \
  --mode snapshot \
  --placement-key users-v1
```

The CLI requires every approval input, preserves repeated column order, and
does not infer a token or epoch, accept a path, run recommendations, or retry.
After an uncertain transport/control outcome it instructs the operator to
manually retry the same exact approval without refreshing the token or epoch.

NBOP v3 is documented at [server-operator-protocol-v3.md](server-operator-protocol-v3.md)
for historical reference only.
