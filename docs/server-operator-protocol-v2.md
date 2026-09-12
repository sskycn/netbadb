# NetBaDB Operator Protocol v2

> Historical protocol contract. The current operator plane requires NBOP v3;
> NBOP v2 is explicitly rejected.

NBOP v2 is the current local operator contract. NBOP v1 is historical and is
explicitly rejected. This version is independent of Deployment Manifest v7,
Native Protocol v2, PostgreSQL wire, Inspection JSON v7, and persistent formats.

## Trust, lifecycle, and frame

The trust boundary is unchanged: one AF_UNIX stream socket, filesystem access,
mode `0600`, no peer identity capture, and no network exposure. The listener
owns only typed Adaptive and Physical Design control handles. The sole Database
worker owns the Database and both runtimes.

Every connection performs one request, one response, then close. One dedicated
listener thread handles one active request; other connections remain in the OS
backlog. There is no per-request thread, async task, worker pool, multiplexing,
or subscription.

The fixed 12-byte big-endian header is unchanged except for the version:

| Offset | Size | Meaning |
| ---: | ---: | --- |
| 0 | 4 | ASCII `NBOP` |
| 4 | 2 | unsigned version, exactly `2` |
| 6 | 2 | reserved, exactly zero |
| 8 | 4 | JSON payload length |

The payload cap remains 65,536 bytes. Length is checked before allocation.
Wrong magic, nonzero reserved bytes, truncation, invalid UTF-8/JSON, unknown
operations or fields, and oversized requests fail closed. A version-1 request
receives `unsupported_protocol_version`; compatibility is never guessed.

All JSON objects are strict. `request_id` is echoed for correlation, not stored
for deduplication.

## Operations

Existing Adaptive operations retain their v1 meaning:

```json
{"request_id":1,"operation":{"type":"status"}}
{"request_id":2,"operation":{"type":"rotate_evidence","expected_window_epoch":7}}
{"request_id":3,"operation":{"type":"reset_faulted_scheduler"}}
```

`rotate_evidence` means only `AdaptiveEvidencePool` rotation. It returns the
same Adaptive rotation report and does not move Physical Design epoch D.

Physical Design adds two operations:

```json
{"request_id":4,"operation":{"type":"physical_design_recommendations"}}
{"request_id":5,"operation":{"type":"rotate_physical_design_evidence","expected_evidence_epoch":7}}
```

Recommendations synchronously evaluate the current worker-owned evidence
against current Database access paths, placement, projection inventory, and
schema. They execute no query, ANALYZE, maintenance, scheduler, or mutation and
do not change diagnostics. There is no recommendation cache.

Physical Design rotation compares and rotates in one Database-worker command.
The result contains `previous_epoch` and `new_epoch`. A successful D7-to-D8
whose response is lost cannot rotate again: retrying expected D7 returns
`physical_design_evidence_epoch_changed`, and D8 remains D8. It does not move
Adaptive window W.

There is no policy mutation, enable/disable, apply, create/drop index,
projection build, automatic design, background advisor, recommendation ID,
identity reservation, path generation, or scheduling operation.

## Multi-domain status

Status returns two optional domains:

```json
{"adaptive":null,"physical_design":{"diagnostics":{},"evidence":{}}}
```

Adaptive-only, Physical-Design-only, and both are valid. A properly validated
Manifest v7 cannot start an operator with neither. The adapter nevertheless
fails safely with `internal` if both controls report disabled.

`adaptive`, when present, contains the v1 `mode`, `feedback`, and optional
`driver` model nested as `OperatorAdaptiveStatusV2`. Physical Design contains
explicit wire DTOs for diagnostics and evidence inspection. Diagnostics expose
eligible queries, record successes/errors, schema rotations, capacity
rejections, incomplete reports, counter overflow, last record outcome, and last
record error. Stable outcome tags are `recorded`, `schema_rotated`,
`recorded_with_capacity_rejection`, and
`schema_rotated_with_capacity_rejection`. Stable design record errors are
`global_visibility_required`, `stale_schema_evidence`,
`out_of_order_visibility`, and `evidence_window_epoch_exhausted`.

Evidence status exposes all configured evidence limits, epoch, optional schema
generation, optional first/last G, ordering high-water, recorded report and
candidate counts, capacity rejections, discarded incomplete reports, and
overflowed/incomplete/truncated flags.

Each domain snapshot is individually worker-serialized, but the v2 status
response does not promise cross-domain transactional atomicity. Status does not
run the advisor, rotate evidence, tick a scheduler, or mutate the Database.

Status and recommendations never expose SQL, literals, parameter values,
`QueryShape`, candidate history, principals, SessionId, IP, filesystem paths,
rows, pages, or Rust debug strings.

## Recommendation DTOs

`OperatorPhysicalDesignAdvisorReportV2` contains evidence epoch, schema
generation, first/last G, recorded reports, discarded incomplete reports, and
overflowed/incomplete flags. It preserves every index and columnar candidate,
including NoAction candidates.

Index candidates carry `table_id`, `column_id`, point/range report counts,
evidence, and decision. Columnar candidates carry `table_id`, canonical
`columns`, evidence, and decision. Evidence contains `report_count`,
`distinct_query_shapes`, `total_actual_scan_work_units`,
`total_rows_examined`, and overflowed/incomplete/truncated flags. Observed work
is not predicted savings.

Decision JSON is either:

```json
{"kind":"recommend"}
{"kind":"no_action","reason":"existing_design_covers"}
```

Stable NoAction reasons are:

- `below_minimum_reports`
- `below_minimum_shape_diversity`
- `below_minimum_actual_work`
- `existing_design_covers`
- `unsupported_current_layout`
- `incomplete_evidence`
- `current_projection_unavailable`
- `recommendation_limit_reached`

The wire never includes predicted savings, speedup, ROI, SQL, an index name or
ID, a projection ID, or a projection path.

## Errors and bounded responses

Success uses `outcome: "ok"`; failure uses `outcome: "error"` with a stable
code and bounded human message. Existing codes retain their meaning:
`adaptive_not_enabled`, `driver_not_enabled`, `scheduler_not_faulted`,
`evidence_window_changed`, `evidence_window_epoch_exhausted`, `server_stopped`,
`malformed_request`, `unsupported_protocol_version`, `request_too_large`, and
`internal`.

New stable codes are:

- `physical_design_not_enabled`
- `physical_design_evidence_epoch_changed`
- `physical_design_evidence_epoch_exhausted`
- `physical_design_no_evidence`
- `physical_design_stale_schema`
- `physical_design_inconclusive_capacity`
- `response_too_large`

Underlying advisor Database errors map to `internal`; internal details do not
cross the wire.

A complete success response that cannot fit 65,536 bytes is never truncated.
The server sends a small `response_too_large` response preserving `request_id`.
It does not drop candidate tails or columns, rotate evidence, change policy, or
mutate Core evidence to make the response fit. An operator may reduce manifest
evidence/recommendation cardinality, restart, and retry.

Read/write timeouts cover framing only, not cancellation of a decoded worker
operation. The listener accept loop remains independent from Native/PostgreSQL
traffic; an advisor request may wait for the Database worker without blocking
normal network acceptance.
