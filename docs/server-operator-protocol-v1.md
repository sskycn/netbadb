# NetbaDB Operator Protocol v1

NBOP v1 is the local live-control contract for one manifest-configured
Adaptive runtime. It is available only through an AF_UNIX stream socket. It is
not Native Protocol traffic, PostgreSQL wire traffic, SQL, HTTP, WebSocket, or
gRPC, and it must never be exposed on a network listener.

## Trust and lifecycle boundary

The daemon creates the configured socket with mode `0600` before accepting.
The effective daemon user, root, and equivalent operating-system privilege are
the v1 operator trust boundary. Database principals, table grants, client
certificates, usernames, bearer tokens, and peer UID/PID records do not
participate. The plane neither records peer identity nor places it in status or
logs.

An existing socket path, regular file, symlink, or directory makes startup
fail. Nothing is automatically unlinked because a path alone cannot prove that
an older daemon is dead. After a crash, verify that the old daemon stopped,
remove its stale socket explicitly, and restart. Normal shutdown uses `lstat`
and removes only a socket whose device and inode equal those captured after
bind. A replacement path is preserved and reported.

## Frame

Every request and response is one binary frame. Integers are big-endian.

| Offset | Size | Meaning |
| ---: | ---: | --- |
| 0 | 4 | ASCII magic `NBOP` |
| 4 | 2 | protocol version, unsigned; exactly `1` |
| 6 | 2 | reserved; exactly zero |
| 8 | 4 | JSON payload length, unsigned |

The fixed payload limit is 65,536 bytes. Length is checked before allocation.
Wrong magic, a nonzero reserved field, truncation, invalid UTF-8/JSON, and an
oversized payload are connection-fatal. A version other than one is reported
as `unsupported_protocol_version`; compatibility is never guessed.

Each connection performs exactly:

```text
connect -> one request -> one response -> close
```

There are no sessions, pipelines, multiplexing, subscriptions, streams, or
request-history/deduplication stores. `request_id` is echoed for correlation
only.

## Strict JSON requests

All objects and variants reject unknown fields.

```json
{"request_id":42,"operation":{"type":"status"}}
```

```json
{
  "request_id": 42,
  "operation": {
    "type": "rotate_evidence",
    "expected_window_epoch": 7
  }
}
```

```json
{"request_id":42,"operation":{"type":"reset_faulted_scheduler"}}
```

Status is read-only. Rotation compares the expected epoch and rotates in one
Database-worker command. A mismatch returns `evidence_window_changed` without
mutation. Reset succeeds only for the Phase 13 `faulted` gate; feedback-only
returns `driver_not_enabled`, while open, backoff, trial, and evidence-renewal
gates return `scheduler_not_faulted`.

## Responses and stable values

Success uses an `ok` envelope and a typed result:

```json
{
  "request_id": 42,
  "outcome": "ok",
  "result": {"type":"scheduler_reset"}
}
```

Error responses use stable snake-case codes:

```json
{
  "request_id": 42,
  "outcome": "error",
  "error": {
    "code": "scheduler_not_faulted",
    "message": "adaptive scheduler is not faulted"
  }
}
```

The v1 code set includes `adaptive_not_enabled`, `driver_not_enabled`,
`scheduler_not_faulted`, `evidence_window_changed`,
`evidence_window_epoch_exhausted`, `server_stopped`, `malformed_request`,
`unsupported_protocol_version`, `request_too_large`, and `internal`. Client
logic must use `code`, never the bounded human message.

Status projects runtime state into independent protocol DTOs. It includes mode,
bounded feedback counters and evidence progress, pool health, stable last-record
codes, and—in driven mode—scheduler ticks, gate, delay/renewal/fault code,
pending/exhaustion flags, bounded counters, and a stable last-stop tag. It never
contains SQL, values, rows, candidates, query shapes, reports, history,
principals, session IDs, peer data, filesystem internals, or Rust debug output.
Rotation returns previous/new epoch, schema and ordering high-waters, discarded
bounded counts, and incomplete/truncated flags. Reset returns only
`scheduler_reset`.

## Timeout, retry, and backpressure

The manifest I/O timeout covers reading the request frame and writing the
response frame. It never times out or cancels a control operation after the
complete request has been decoded and forwarded. If a client disconnects then,
the worker still produces a definitive result and only the response write may
fail.

The client does not automatically retry a mutation after a lost response. A
successful W7-to-W8 rotation followed by retry of the same expected-W7 request
returns `evidence_window_changed`, leaving W8 unchanged. An operator may query
status before deciding what to do next.

One dedicated listener thread serially handles one active operator connection.
Later connections remain in the bounded OS listen backlog; there is no thread
per client, async task per request, or unbounded application queue. The normal
Native/PostgreSQL accept loop remains independent. The listener owns only its
socket and a `ServerAdaptiveControlHandle`; the existing Database worker owns
the Database, evidence pool, and scheduler.
