# NetbaDB daemon lifecycle v1

This contract defines Unix process lifecycle and readiness for `netbadbd`. It
does not change the synchronous `netbadb-server` library lifecycle.

## Signal boundary

After command-line parsing selects a daemon run, but before manifest parsing or
creation of database, TCP, TLS, or operator resources, `netbadbd` registers
`SIGINT` and `SIGTERM`. Registration failure is a startup failure. The installed
handlers use `signal-hook` only to store `true` in a shared `AtomicBool`; they do
not allocate, print, lock, access database state, or perform socket cleanup.

Both signals mean the same graceful-shutdown request. Repeated signals
coalesce, do not introduce a force-exit path, and do not replace an earlier
cleanup or server error. `SIGKILL` is not catchable. `SIGHUP` has no NetbaDB
meaning, and there is no hot reload or signal-configurable timeout.

Signal ownership remains in the executable:

```text
Unix SIGINT / SIGTERM
        ↓ atomic intent only
netbadbd lifecycle supervisor
        ↓ ServerHandle::shutdown()
netbadb-server's existing operator-first shutdown authority
```

`TcpServer::run`, `PostgresTcpServer::run`, and embedded callers remain
signal-unaware.

## Readiness boundary

The stable readiness marker is one flushed stderr line beginning exactly with:

```text
netbadbd ready:
```

It is written only after the selected Native or PostgreSQL `start()` has
returned a handle, the handle is not already finished, and no shutdown signal
has been observed. Successful `start()` includes database-worker startup, TCP
bind and listener-thread startup, plus operator bind, `0600` mode and identity
capture when configured. The bounded line identifies the wire transport and
Adaptive mode, but never prints the operator path or authorization identities.

Failure to write or flush the readiness line is a daemon failure. The daemon
still invokes the official handle shutdown and reports both publication and
cleanup errors when both occur. A startup failure or a signal observed before
the boundary produces no readiness marker.

Readiness means only that the owned startup sequence completed at that
boundary. It is not a query health check, maintenance-progress signal, liveness
lease, PID file, ready file, systemd notification, HTTP endpoint, or protocol
message.

## Supervision and exit

`ServerHandle::is_finished()` and `PostgresServerHandle::is_finished()` are
pure lifecycle observations. They return true when the main server thread or a
configured operator listener has terminated; they do not report database
health. The daemon polls this state and the atomic signal intent at one fixed,
bounded 10 ms interval that is independent of Manifest v6 and Adaptive ticks.

Natural termination is completed through `wait()` so its real error is
preserved. A signal is completed through `shutdown()`. That existing path
stops and joins the operator listener first, then stops the main accept loop,
closes and joins connections, resolves session transactions, closes the sole
database worker, and joins all owned threads. Operator socket cleanup continues
to remove only the captured device/inode and never unlinks a pre-existing or
replacement path.

A signal-driven shutdown exits successfully only if the complete shutdown path
succeeds. Cleanup, join, database-close, operator, or server errors produce a
failure exit. There is deliberately no internal forced-shutdown deadline;
external supervisors retain escalation authority.

After a successful graceful exit, the database is closed and the daemon's
captured operator socket has been removed, so an external supervisor may start
the same manifest again. An abrupt process termination can leave that socket
behind. The next startup deliberately rejects the existing path instead of
guessing ownership or auto-unlinking it; an operator must resolve the stale
path using deployment-specific evidence.

## Compatibility

This lifecycle adds no manifest field. Deployment Manifest v6, NBOP v1, Native
Protocol v2, PostgreSQL wire behavior, Inspection JSON v7, metrics, SDK schema,
and every persistent format remain unchanged.
