# Adaptive Operations Phase 19

Phase 19 adds a truthful Unix daemon lifecycle boundary around the Phase 18
local operator plane. It does not add Adaptive maintenance authority.

## Implemented slice

- `netbadbd` registers `SIGINT` and `SIGTERM` after CLI parsing and before
  manifest parsing or resource acquisition. `signal-hook` records only atomic
  shutdown intent in signal context.
- One small daemon-private `RunningServer` enum normalizes Native and
  PostgreSQL `is_finished`, readiness, `shutdown`, and `wait` behavior without
  a trait framework.
- Public Native and PostgreSQL handle observations include both the main server
  thread and configured operator listener. They are lifecycle observations,
  not health APIs.
- The daemon publishes one explicitly flushed `netbadbd ready:` stderr line
  only after full startup and only while the server is still running and no
  signal has been observed.
- A fixed bounded supervisor loop reacts to signal intent or natural server
  completion. Signals use the existing operator-first handle shutdown; natural
  completion uses `wait()` and preserves its error.
- Real subprocess coverage waits on the readiness line, sends Native SIGTERM,
  Native SIGINT, and PostgreSQL SIGTERM with Python's standard-library
  `os.kill`, and enforces an external test deadline. Coverage includes Driven
  mode with NBOP, Disabled mode without NBOP, exact socket removal, database
  reopen, and startup failure without false readiness.

The existing Native server test
`graceful_shutdown_closes_idle_sessions_rolls_back_and_closes_database` remains
the transaction-resolution proof used by this daemon path: `netbadbd` invokes
that same `ServerHandle::shutdown()` authority. The Phase 18 replacement-path
regression also remains unchanged and continues to prove that shutdown cannot
unlink a replacement file.

## Safety and compatibility

The server library does not install signal handlers and its `run()` methods do
not change semantics. The daemon has no second-signal force exit, internal
timeout, SIGHUP reload, PID/ready file, systemd notification, daemonizing fork,
HTTP health endpoint, or NBOP shutdown command.

Manifest v6 is still the strict current deployment contract. NBOP v1, Native
Protocol v2, PostgreSQL wire behavior, Inspection JSON v7, Server metrics, SDK
schema, and persistent formats are byte-for-byte and semantically unchanged.
The complete normative process contract is
[Daemon lifecycle v1](daemon-lifecycle-v1.md).

## Deferred boundary

Phase 20 may build higher-level supervisor integration only from new explicit
requirements. It must not reinterpret the readiness line as continuous health,
move signal ownership into the library, add an implicit shutdown deadline, or
make operator status a remote protocol surface.
