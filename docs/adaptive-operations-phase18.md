# Adaptive Operations Phase 18: local operator plane v1

## Mission and ownership

Phase 17 made the existing Adaptive runtime deployable. Phase 18 makes that
running runtime locally observable and recoverable without creating a second
Database owner:

```text
local operator process
    -> manifest-resolved Unix socket
    -> dedicated serial NBOP listener
    -> ServerAdaptiveControlHandle
    -> existing Native/PostgreSQL accept-loop control path
    -> existing sole Database worker
```

The listener owns only local IPC. It has no `Database`, evidence pool,
scheduler, worker sender, session, or transaction. The worker still owns all
Adaptive state and runs the established typed commands in FIFO order. The
plane does not run a scheduler tick, orchestration cycle, candidate selection,
Columnar maintenance, Change Stream GC, LSM maintenance, or calibration
directly.

## Manifest v6 and trust boundary

V6 is the sole accepted deployment manifest. V1-v5 and future versions are
rejected. Migrating v5 without an operator is mechanical: change version 5 to
6 and leave all other fields unchanged. Runtime behavior is identical and no
socket is created.

The optional strict top-level `operator` object contains only `unix_socket` and
required nonzero `io_timeout_ms`. It is legal with feedback-only or driven
Adaptive and illegal when Adaptive is absent. A relative socket resolves from
the manifest directory by canonicalizing its existing parent and retaining the
final filename. Parsing and offline inspection validate this configuration but
never bind, connect, start Adaptive, or require the daemon to be online.

The first plane is Unix-domain only. Mode `0600` and local filesystem access
are its authentication boundary. It has no token, TLS identity, database
principal, grants, peer UID/PID record, TCP port, HTTP endpoint, Native frame,
PostgreSQL extension, or SQL command. Non-Unix builds still parse v6 and return
a typed unsupported-platform startup error when the plane is configured.

## Operations and transport outcomes

NBOP v1 independently versions its fixed 12-byte `NBOP`/version/reserved/length
header, 64 KiB payload cap, and strict JSON envelopes. One connection carries
one request and one response. Status is a bounded, privacy-preserving snapshot;
feedback-only exposes feedback and no driver, while driven mode includes the
stable scheduler gate, delay/renewal/fault reason, ticks, counters, pending
state, exhaustion, and last-stop tag.

Evidence rotation is the only non-idempotent v1 operation, so the wire requires
`expected_window_epoch`. Compare and rotation occur inside one worker command.
If W7 already became W8, replaying expected W7 returns
`evidence_window_changed` and cannot rotate again. A disconnect after dispatch
does not cancel the command; a lost response creates human uncertainty but not
an unsafe automatic retry.

Fault reset reuses the Phase 16 authority. It reconstructs only a genuinely
faulted Phase 13 scheduler with the same policy and waits for a future host
opportunity. It does not rotate evidence, run maintenance, clear a renewal or
trial gate, or mutate Database state. Feedback-only returns
`driver_not_enabled`; every non-faulted driven gate returns
`scheduler_not_faulted`.

## Backpressure and lifecycle

One small listener thread serially handles operator connections. Socket reads
and writes use the manifest timeout, but waiting for a forwarded worker result
has no destructive timeout. Later connections stay in the OS backlog, so no
per-client threads, async tasks, sessions, client map, or unbounded control
queue are created. Normal database admission continues on its independent
accept loop.

Startup rejects every existing filesystem object at the socket path and never
auto-removes a presumed stale socket. After bind, the server records device and
inode and applies `0600` before accept. Shutdown first stops new operator
acceptance and joins an in-flight request while the Database worker remains
available, then stops normal service and the worker. Cleanup removes only the
original socket identity. A missing path needs no action; a replacement is
preserved and reported. An abrupt crash may leave a stale socket that an
operator removes only after verifying the old daemon stopped.

`netbadb operator status`, `rotate-evidence --expected-window-epoch N`, and
`reset-faulted-scheduler` locate the socket only through Manifest v6 and speak
NBOP v1. The CLI never opens the Database or falls back to database protocols.
Its default human text is not a second stable machine contract.

## Explicit exclusions

There is no manifest reload, policy mutation, run-now action, automatic rotate
or reset, server shutdown action, SQL execution, arbitrary internal inspection,
peer logging, request history, metrics endpoint, global-visibility enablement,
or operator persistence. Native Protocol remains v2, PostgreSQL wire and
transaction state remain unchanged, Inspection JSON remains v7, Server metrics
remain unchanged, and no database persistent format changes.
