# `netbadb` inspection and local operator CLI

`netbadb inspect` opens the existing tables declared by deployment manifest v11
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

`netbadb operator` does not open a Database. It parses the same manifest v11,
uses only its configured Unix socket, and exchanges one NBOP v6 request:

```sh
netbadb operator status --manifest server.json
netbadb operator rotate-evidence --manifest server.json \
  --expected-window-epoch 7
netbadb operator reset-faulted-scheduler --manifest server.json
netbadb operator physical-design recommendations --manifest server.json
netbadb operator physical-design rotate-evidence --manifest server.json \
  --expected-evidence-epoch 7
netbadb operator physical-design receipts status --manifest server.json
netbadb operator physical-design receipts list --manifest server.json --limit 32
netbadb operator physical-design receipts list --manifest server.json --limit 32 \
  --after-journal-incarnation 00112233445566778899aabbccddeeff \
  --after-receipt-id 41
netbadb operator physical-design apply-index --manifest server.json \
  --expected-runtime-token 00112233445566778899aabbccddeeff \
  --expected-evidence-epoch 7 --table-id 1 --column-id 3 \
  --index-name idx_users_email
netbadb operator physical-design apply-columnar --manifest server.json \
  --expected-runtime-token 00112233445566778899aabbccddeeff \
  --expected-evidence-epoch 7 --table-id 1 \
  --column-id 1 --column-id 3 --column-id 5 \
  --mode incremental --placement-key users-analytics-v1
```

Rotation and mutation preconditions are required and are never inferred by a
hidden status or recommendations request. Apply never selects a candidate,
refreshes a token/epoch, or retries automatically. The human output is not a
stable machine-readable contract; NBOP v6 is the versioned contract. Receipt
commands never infer a cursor, automatically restart after journal replacement,
or turn a receipt into replay authority. Apply output prints a receipt only when
the server returned one, and never automatically queries it.

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

Operator status presents independent Index, Columnar Snapshot and Columnar
Incremental admission modes. Component limits show all six configured dimensions;
these are partial component limits, not a total mutation budget. Apply accepts
no budget flags. Structured admission failures name an unproven component or its
exceeded conservative bound/maximum; inspection failure diagnostics remain
private. The CLI never relaxes a policy, retries, or queries a receipt automatically.
