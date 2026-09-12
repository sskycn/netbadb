# `netbadb` inspection and local operator CLI

`netbadb inspect` opens the existing tables declared by deployment manifest v8
and reports catalog metadata or the physical plan chosen for one SQL
statement. It does not create databases, execute queries or DML, start a
server, connect remotely, refresh `ANALYZE`, create indexes, or checkpoint.

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
[Inspection JSON v7](../../docs/inspection-json-v7.md). A statement requires
exactly one of `--sql` and `--sql-file`. SQL files must be UTF-8 and are read
before the manifest or database is opened. Success writes only the completed
inspection to stdout. Usage failures exit 2, operational failures exit 1, and
all failures write diagnostics only to stderr.

`netbadb operator` does not open a Database. It parses the same manifest v8,
uses only its configured Unix socket, and exchanges one NBOP v3 request:

```sh
netbadb operator status --manifest server.json
netbadb operator rotate-evidence --manifest server.json \
  --expected-window-epoch 7
netbadb operator reset-faulted-scheduler --manifest server.json
netbadb operator physical-design recommendations --manifest server.json
netbadb operator physical-design rotate-evidence --manifest server.json \
  --expected-evidence-epoch 7
netbadb operator physical-design apply-index --manifest server.json \
  --expected-runtime-token 00112233445566778899aabbccddeeff \
  --expected-evidence-epoch 7 --table-id 1 --column-id 3 \
  --index-name idx_users_email
```

Rotation and mutation preconditions are required and are never inferred by a
hidden status or recommendations request. Apply never selects a candidate,
refreshes a token/epoch, or retries automatically. The human output is not a
stable machine-readable contract; NBOP v3 is the versioned contract.

## Ownership, recovery, and authorization

Inspection is an offline local operation. Before using `netbadb inspect`, stop
`netbadbd` and any embedded process using the same files. NetbaDB has no
cross-process database-file lock, so concurrent multi-process file access is
unsupported and the CLI does not attempt to infer ownership from a listening
port. Operator commands instead require the live daemon and access only the
manifest-configured Unix socket.

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
