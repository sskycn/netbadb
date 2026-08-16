# `netbadb` offline inspection CLI

`netbadb` opens the existing tables declared by deployment manifest v4 and
reports catalog metadata or the physical plan chosen for one SQL statement.
It does not create databases, execute queries or DML, start a server, connect
remotely, refresh `ANALYZE`, create indexes, or checkpoint.

```sh
netbadb inspect catalog \
  --manifest server.json

netbadb inspect statement \
  --manifest server.json \
  --sql "SELECT id FROM users WHERE id = 42"

netbadb inspect statement \
  --manifest server.json \
  --sql-file query.sql \
  --format json
```

`--format` defaults to `text`; `json` emits
[Inspection JSON v1](../../docs/inspection-json-v1.md). A statement requires
exactly one of `--sql` and `--sql-file`. SQL files must be UTF-8 and are read
before the manifest or database is opened. Success writes only the completed
inspection to stdout. Usage failures exit 2, operational failures exit 1, and
all failures write diagnostics only to stderr.

## Ownership, recovery, and authorization

This is an offline local tool. Stop `netbadbd` and any embedded process using
the same files first. NetbaDB has no cross-process database-file lock, so
concurrent multi-process access is unsupported and the CLI does not attempt to
infer ownership from a listening port.

The CLI uses normal `Database::open_tables` startup recovery. Opening after a
crash may redo or undo WAL state before inspection; this is not a forensic
no-write reader and there is no `--no-recovery` mode. The inspected SQL itself
is compiled and planned but never executed, including INSERT, UPDATE, and
DELETE.

Deployment authorization protects network Protocol sessions. A local process
with filesystem access is outside that boundary, so catalog inspection shows
the complete manifest catalog even when a configured principal has narrower
grants. Inspection output contains no manifest paths, listener, TLS material,
or authorization identities.
