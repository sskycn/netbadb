# Server execution and resource-boundary audit

## 1. Baseline commit

The audit baseline is `5b2ee5c76e2c16debc6ca4816d48c11c2c6e3ae6`
(`feat: bound physical index output writes`). `main` and `origin/main` matched at
the start. Measurements used Rust 1.97.1 on Darwin 25.6.0 arm64. Raw logs and
fixtures are under `/private/tmp/netbadb-server-execution-audit` and are not
repository artifacts.

## 2. Current worker architecture

Native and PostgreSQL listeners each create exactly one dedicated database
worker. The listener owns acceptance and lifecycle; one blocking thread per
connection owns its socket; the worker constructs and exclusively owns
`Database`, every protocol session, and every session transaction.

```text
connection thread
  -> unbounded std::sync::mpsc Sender<command>
  -> database worker: prepare -> plan/execute -> QueryResult -> encode batch
  -> capacity-one per-request reply channel
  -> connection thread: socket write + flush
```

Native and PostgreSQL have separate command enums and worker loops, but the
ownership and one-outstanding-request-per-connection behavior are equivalent.

## 3. Thread ownership map

| Thread | Owns | Does not own |
| --- | --- | --- |
| Listener | listening socket, connection registry, shutdown coordination | Database, sessions, query execution |
| Connection | one socket and one response batch while writing | Database or transaction authority |
| Database worker | Database, all sessions, transaction handles, adaptive and physical-design runtimes | client socket |
| Operator listener, when configured | one operator connection and admitted control request | SQL execution authority |

`Database` contains `Rc`/`RefCell`-based coordination and mutable storage
registries. It is intentionally not made `Send` or `Sync` by this audit.

| Stage | Thread and owner | Blocking / proportional allocation |
| --- | --- | --- |
| TCP accept and admission | listener; owns socket until handoff | blocking accept; connection vector is the admission truth |
| Native/PG read and decode | connection thread; owns socket and bounded input frame | blocking read with timeout; input allocation is protocol bounded |
| command submission / reply wait | connection thread; command holds session ID and request | unbounded-channel send, then blocking capacity-one reply receive |
| prepare and typed dependency validation | sole database worker; borrows Database/session | serialized; may read schema/catalog but does not create final rows |
| planning | sole database worker; borrows current schema/statistics/access paths | serialized; plan-sized allocation |
| storage scan or mutation | sole database worker; transaction handle remains in its session | may block on local I/O; reads use captured views, writes obey StorageId authority |
| executor and QueryResult construction | sole database worker | serialized; operator state and owned output can scale with input/output |
| Native/PG response-object construction | sole database worker | proportional to permitted result; Native builds messages and PG builds DataRows/portal state |
| wire encode, socket write and flush | connection thread after worker reply | may block on a slow client and retain one response; worker is already free |
| disconnect cleanup | connection thread submits close; worker rolls back/removes session | queued/running statement is not cancelled |
| shutdown | listener closes sockets and joins handlers, then worker is joined | waits for non-preemptible active execution before Database close |

## 4. Queue model

Both SQL command channels are syntactically unbounded `mpsc::channel`s. Network
growth is nevertheless logically bounded: each admitted connection sends one
command and blocks on a capacity-one reply before reading its next command.
The maximum queued network requests is therefore proportional to configured
connections, not to bytes a client can pipeline. Control families retain their
existing separate admission permits. Slow readers do not keep adding worker
commands; they retain one already-produced response on their connection thread.

The `execution-audit` Cargo feature adds observation only. It counts submitted,
current/max queue depth, completed, worker active/busy/idle, queue wait, socket
write, and request total. Default builds compile the calls to no-ops and expose
no audit snapshot API.

Instrumentation precision is deliberately stated rather than invented:

| Requested phase | Measurement |
| --- | --- |
| `queue_wait_ns` | exact enqueue-to-worker-start duration |
| `prepare_ns`, `plan_ns`, `execute_ns`, `result_materialize_ns`, `encode_ns` | one shared `worker_busy_ns` timer; current Core/server APIs do not expose safe non-overlapping boundaries for all paths |
| standalone encode | existing exact Native/PG codec loops, outside request timing |
| `socket_write_ns` | exact connection-thread wire encoding plus socket writes and flush; those operations are combined by the existing writer APIs |
| `total_request_ns` | worker submission through completed socket flush |

## 5. Single-writer versus single-worker analysis

The storage rule is one active mutation owner per `StorageId`. That rule does
not logically require unrelated read-only SELECTs to execute serially. The
current serialization follows from the stronger implementation ownership:
one mutable, non-`Send` `Database` owns the storage registry, schema/catalog
publication state, transaction coordination, projection quarantine and GC
lifetimes, and session transactions.

Consequently storage mutation ownership (A), database coordination ownership
(B), and query execution thread ownership (C) are distinct. A constrains
writers. Current B makes C single-threaded, but A alone does not prove C must
remain so.

## 6. QueryResult ownership model

The executor returns `QueryResult { rows: Vec<Vec<ScalarValue>> }`. Text and
Bytes values are owned. Core transfers that result to the worker. Native then
owns a full `Vec<ServerMessage>`; PostgreSQL owns encoded DataRows and a
suspended portal can retain its unsent iterator. The worker releases database
authority after sending the response object to the connection thread, while a
slow socket can keep that object alive until write completion, timeout, error,
or shutdown.

Worst-case size is SQL-output dependent: `O(rows × projected width + owned
Text/Bytes payload)`, up to the configured row count but without a scalar-slot
or byte budget. A 100,000-row duplicate projection in the measured fixture owns
500,000 scalar slots and 51,200,000 Text/Bytes payload bytes before allocator
and vector overhead.

## 7. Response-limit enforcement point

Before this change, `max_result_rows` was checked only by Native/PG encoders
after Core had returned the complete `QueryResult`. It limited protocol-message
expansion but not executor output accumulation.

The retained change introduces `QueryExecutionLimits` and typed
`ExecutionError::OutputRowsExceeded`. `DatabaseSession` passes the same Core
limit for Native and PostgreSQL, for autocommit, explicit transactions, and the
adaptive-feedback path. Common Heap batch pipelines stop before appending the
first over-limit batch; Top-N retains at most `min(K, limit + 1)` candidates.
Other physical paths deterministically reject before returning a successful
`QueryResult`, although an operator may already have materialized internal
state. Checked addition protects the batch accumulator, and no partial rows are
returned as success.

Native maps the typed resource refusal to `ResponseTooLarge`; PostgreSQL maps
it to SQLSTATE `54000`. Existing encoder checks remain defense-in-depth and
continue to cover compatibility-catalog results.

## 8. Cancellation and disconnect semantics

- Native EOF/disconnect does not signal a queued or running worker command.
  The connection thread is blocked waiting for its reply and requests session
  close only after that wait returns.
- PostgreSQL behaves the same. `CancelRequest` is decoded and its connection is
  closed; process/secret IDs are not registered with running work and no
  cancellation flag is checked by executor or storage.
- A failed response send means the worker drops the response and then performs
  session cleanup when the connection thread's close command arrives. It does
  not retroactively cancel execution.
- Shutdown closes sockets and joins connection threads, but the worker shutdown
  command remains ordered behind an already-running non-preemptible statement.
- Slow readers cannot hold the database worker, but they retain response memory
  on their connection thread until write timeout or shutdown.

No fake cancellation was added. Safe cancellation still needs explicit
operator checkpoints, transaction cleanup rules, rollback/error priority, and
shutdown integration.

## 9. Baseline benchmarks

The existing optimized real-TCP benchmark validates exact results and excludes
setup/handshake/warmup. Values below are ranges from three independent process
runs; times are microseconds and throughput is requests/second.

| Protocol/workload | Clients | P50 | P95 | Throughput |
| --- | ---: | ---: | ---: | ---: |
| Native point, baseline | 1 | 88.2–93.3 | 95.1–101.0 | 10.6k–11.4k |
| Native point, baseline | 4 | 116.9–117.9 | 130.5–145.8 | 33.1k–33.5k |
| PG point, baseline | 1 | 60.9–65.5 | 69.9–75.3 | 15.0k–16.1k |
| PG point, baseline | 4 | 124.9–166.8 | 141.3–241.4 | 23.4k–31.5k |
| Native 1K result, baseline | 1 | 2,075–2,232 | 2,488–2,678 | 459–467 |
| PG 1K result, baseline | 1 | 4,372–4,664 | 4,679–4,905 | 214–226 |

The extended audit also covers 16 clients. In instrumented runs, Native point
P50 was 474–477 microseconds and PG point P50 was 474–487 microseconds. Average
queue wait was 407–418 microseconds for Native and 301–353 microseconds for PG;
the worker service average stayed about 28–30 microseconds. Queueing, not point
execution, dominates at that concurrency.

One final same-process run gives the requested timer attribution below. The
worker timer combines prepare, plan, execute, result materialization and server
response construction; the write timer combines wire encoding and socket I/O.
The client P50 additionally includes request write and response read, so it is
not expected to equal the server-side total average.

| Protocol/workload | Rows / scalar slots / owned payload | Client P50 | Avg queue | Avg worker | Avg encode+socket | Avg server total |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Native point, 1 client | 1 / 2 / 8 B | 83.7 µs | 1.9 µs | 33.4 µs | 5.7 µs | 43.5 µs |
| PG point, 1 client | 1 / 2 / 8 B | 65.3 µs | 2.2 µs | 30.0 µs | 20.3 µs | 54.4 µs |
| Native 1K result, 1 client | 1,000 / 2,000 / 8,000 B | 1.845 ms | 2.2 µs | 297.7 µs | 1.542 ms | 1.845 ms |
| PG 1K result, 1 client | 1,000 / 2,000 / 8,000 B | 4.554 ms | 4.0 µs | 214.8 µs | 4.312 ms | 4.533 ms |

The standalone 1K codec controls measured Native encode P50 101.7 µs and PG
conversion/encode P50 44.2/80.8 µs. They isolate CPU codec work but are not
summed into the end-to-end timers.

## 10. Head-of-line blocking experiment

The long query is a 1,000 × 1,000 inequality join followed by SUM, so output is
one scalar and network volume is not the cause. Already-open point-query
clients submit only after the audit hook observes the long query active.

| Protocol | Point clients | Point P50 | Point P95 | Throughput | Max queue wait |
| --- | ---: | ---: | ---: | ---: | ---: |
| Native | 1 | 37.88 ms | 37.88 ms | 26.4/s | 37.77 ms |
| Native | 4 | 35.19 ms | 35.19 ms | 114.0/s | 35.11 ms |
| Native | 8 | 36.22 ms | 36.31 ms | 221.6/s | 36.18 ms |
| PostgreSQL | 1 | 33.44 ms | 33.44 ms | 29.9/s | 33.31 ms |
| PostgreSQL | 4 | 33.06 ms | 33.07 ms | 121.5/s | 32.94 ms |
| PostgreSQL | 8 | 34.82 ms | 34.86 ms | 230.9/s | 34.68 ms |

In the same process the unblocked one-client point P50 was 83.7 µs Native and
65.3 µs PostgreSQL. Socket write averaged only tens of microseconds in the HOL
runs, while the maximum queue wait tracked the point latency. This is a
measured database-worker head-of-line bottleneck. Three earlier independent
eight-client processes reproduced 37–40 ms queue waits.

Repeated long-read plus transactional single-row UPDATE runs measured
32.6–35.3 ms for Native and 32.8–32.9 ms for PostgreSQL, with respective
maximum queue waits of 32.2–35.0 ms and 32.4–32.6 ms. The read owns no mutation
authority; each write waits solely because query execution ownership is serial.
Both transactions were rolled back after measurement so later controls
retained the original fixture.

## 11. Large-result resource experiment

The release executor test incrementally builds one fixture and validates exact
results at 1K, 10K, and 100K rows.

| Rows | Projection | Scalar slots | Owned Text/Bytes payload |
| ---: | --- | ---: | ---: |
| 1,000 | id/text/bytes | 3,000 | 256,000 B |
| 1,000 | duplicate text/bytes | 5,000 | 512,000 B |
| 10,000 | id/text/bytes | 30,000 | 2,560,000 B |
| 10,000 | duplicate text/bytes | 50,000 | 5,120,000 B |
| 100,000 | id/text/bytes | 300,000 | 25,600,000 B |
| 100,000 | duplicate text/bytes | 500,000 | 51,200,000 B |

The incremental six-query experiment completed in 1.66 seconds and its
whole-process peak RSS was 136,462,336 bytes. RSS is not allocator accounting:
the fixture, storage caches, previous iterations and allocator retention are
included. Logical owned payload and process peak are therefore reported
separately.

Operator ownership remains:

- Full Sort owns all input rows and sort metadata; no spill.
- Aggregate owns `O(groups × keys/states)` even if final output is later
  rejected by the row budget.
- HashJoin owns its build side/buckets and can produce duplicate-amplified
  output; NestedLoop has `O(N×M)` work.
- Index/range APIs return owned match vectors; the output-row limit does not
  turn them into cursors.

## 12. Candidate fixes

| Candidate | Evidence | Decision |
| --- | --- | --- |
| Feature-gated queue/request timing | Required to distinguish service from queue delay | KEEP |
| Core execution output-row budget | Existing row cap was post-materialization | KEEP |
| Generic worker pool | HOL exists, but Database/snapshot/schema lifetimes are not detachable | REJECT |
| Unsafe `Send`/`Sync` or global locking | Would hide, not prove, ownership invariants | REJECT |
| Read-priority scheduler | Could starve writes and does not remove non-preemptible work | REJECT |
| Bounded detached read workers | Promising only after snapshot and DDL/GC lifetime proof | DEFER |
| Real cancellation | No safe operator/transaction checkpoint contract yet | DEFER |
| Scalar-slot/byte/operator memory budgets | Valuable, but requires separate exact accounting | DEFER |
| Public streaming result API | Crosses Core/Native/PG ownership and portal semantics | DEFER |

## 13. KEEP changes

1. `QueryExecutionLimits::with_max_output_rows` at executor/Core boundaries.
2. Typed `ResourceLimit` database classification and stable Native/PG mapping.
3. Common server enforcement before successful Core result return, including
   explicit transactions and adaptive observation.
4. `execution-audit` benchmark feature and 1/4/16-client plus HOL workloads.

No protocol version, manifest version, persistent format, dependency, async
runtime, or unsafe block changed.

## 14. REJECT changes

The audit rejects a generic thread pool, cloning/sharing mutable `Database`, an
unsafe `Send`/`Sync` assertion, read-priority scheduling, and moving the
unbounded queue behind another unbounded queue. None establishes storage-view,
schema-generation, transaction, recovery, or shutdown correctness.

## 15. DEFER changes

Concurrent detached reads, true cancellation, response streaming, a byte/scalar
output budget, Sort/Group/Join memory budgets, range cursors, and a hard global
response-memory budget remain deferred. The smallest concurrency blocker is an
owned, thread-safe read execution context that pins the exact schema generation,
storage bindings, snapshot/read views, projection generations, and file/page
lifetimes across DDL replacement, quarantine, GC, disconnect and shutdown.

Repeatable Read transactions remain worker-owned. Read Committed statement
snapshots are created at execution time on the worker; moving their creation or
use off-worker without the context above would change semantics.

## 16. Before/after latency

Directly comparable non-instrumented three-process runs used the legacy 1/4
client matrix. Point-query tails did not show a systematic regression:

| Workload | Baseline P50/P95 | After P50/P95 |
| --- | --- | --- |
| Native point, 1 client | 88–93 / 95–101 µs | 88–95 / 95–106 µs |
| Native point, 4 clients | 117–118 / 131–146 µs | 116–125 / 127–141 µs |
| PG point, 1 client | 61–66 / 70–75 µs | 56–64 / 61–71 µs |
| PG point, 4 clients | 125–167 / 141–241 µs | 116–119 / 127–170 µs |

The 1K result cases were noisier: Native one-client P50 was 2.28–2.45 ms after
versus 2.08–2.23 ms before, while four-client medians overlapped; PG one-client
P50 was 4.74–4.78 ms after versus 4.37–4.66 ms before. These are reported as
measurement variability/cost, not as an optimization claim. The retained
change is a resource-correctness boundary.

## 17. Before/after throughput

Point throughput ranges remained overlapping or improved in most controls:
Native 4-client point was 31.7k–34.0k after versus 33.1k–33.5k before; PG
4-client point was 31.6k–33.5k after versus 23.4k–31.5k before. Native 1K result
4-client throughput was 606–612/s after versus 600–629/s before. PG 1K result
4-client throughput was 88–102/s after versus 117–124/s before, with large
100+ ms tails in both series. Because the PG long-result result is unstable and
the change is not a throughput optimization, no positive performance claim is
made and no transport batching change is retained.

## 18. Resource-bound changes

The configured row cap now bounds the successful final output accumulator on
common batch pipelines and guarantees that no Core query returns more rows
than the limit. It still does not bound row width, scalar slots, Text/Bytes
payload, Sort input, aggregate groups, join build state, range-match vectors,
encoded response bytes, suspended portals, or the sum of response objects held
by slow clients. “Query memory is bounded” would therefore be false.

## 19. Correctness and concurrency verification

Tests cover the Core typed error, exact boundary success, unlimited embedded
behavior, Native transaction survival, Native/PG stable error mapping, and the
rule that a resource-refused query is not recorded as successful adaptive
feedback. Existing real-TCP slow-reader, disconnect rollback, worker failure,
shutdown, Native protocol, PostgreSQL protocol and recovery suites remain the
regression boundary. The benchmark checks every returned row and uses the
worker-active observation rather than timing sleeps to start HOL contenders.

No Rust ThreadSanitizer workflow exists in the repository, so none is claimed.
The Go SDK race command is listed below.

| Validation | Result |
| --- | --- |
| `cargo fmt --all -- --check` | Passed |
| `cargo check --workspace --all-targets --all-features` | Passed |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | Passed; final benchmark-only edit also passed scoped all-target Clippy |
| `cargo test --workspace --all-features` | Passed, including Native/PG real TCP and crash/recovery subprocess matrices |
| `cargo bench -p netbadb-server --bench global_boundary --features execution-audit` | Passed with exact result validation and the 1/4/16 baseline plus 1/4/8 HOL matrix |
| `go test ./...` and `go test -race ./...` | Passed from `sdk/go` |
| Go SDK integration and integration `-race` | Passed against the real Native fixture |
| `scripts/check-generated-sdk.sh` | Passed |
| `python3 scripts/test-resource-lifecycle.py` | Passed: 7 FDs / 3 threads at ready, after 100 and after 1,000 failed connections; clean shutdown |
| `python3 scripts/test-postgresql-psql.py` | Passed with PostgreSQL 17.11 from `/opt/local/lib/pgsql` and `/opt/local/lib/icu/lib` |
| `scripts/test-postgresql-alembic.py` | Passed with psycopg 3.2.13, SQLAlchemy 2.0.52 and Alembic 1.16.5 |
| `scripts/test-postgresql-orm.py` | Reproduced the documented pre-existing bootstrap `teams` DROP limitation (`0A000`, single-Heap runtime mutation); `docs/postgresql-client-matrix.md` records the same baseline failure |

## 20. Remaining risks

1. One long non-preemptible statement blocks all sessions and shutdown progress.
2. Disconnect and PostgreSQL CancelRequest do not stop queued/running work.
3. `mpsc` is syntactically unbounded; its network bound depends on preserving
   one outstanding command per admitted connection.
4. Wide rows and duplicate projection can consume substantial memory inside a
   permitted row count.
5. Sort, aggregate, join, index/range and columnar paths retain their documented
   operator/input ownership before a final row-limit refusal.
6. Slow clients can collectively retain up to one response object per admitted
   connection; PostgreSQL portals can retain unsent encoded rows.

## Issue summary

| Issue | Evidence | Classification | Fix | Result |
| --- | --- | --- | --- | --- |
| All SQL requests use one worker | Both worker loops own one Database and all sessions | Architecture limitation | None | Explicitly documented |
| Long read blocks point reads | 37–40 ms queue wait, tens of µs socket write | Measured bottleneck | Measurement retained; concurrency deferred | Attribution proven |
| Long read blocks a write | Native 32.2–35.0 ms and PG 32.4–32.6 ms max queue wait; read owns no mutation authority | Measured bottleneck | No priority scheduler | Attribution proven |
| Row cap checked after QueryResult | Previous encoder-only checks | Resource-bound defect | Core/executor output-row limit | Fixed for successful output; operator memory excluded |
| QueryResult width/bytes unbounded | 100K duplicate projection owns 51.2 MB payload | Architecture limitation | None this round | Byte/scalar budgets deferred |
| CancelRequest implies cancellation | Decoder closes request without lookup/signal | Not a problem | Documentation clarity | No false claim |
| Disconnect stops work | Worker has no cancel signal | Architecture limitation | None | Cancellation deferred |
| Concurrent reads are safe now | No detached snapshot/schema/GC lifetime owner | Architecture limitation | Reject worker pool | Correctness preserved |
| Exact per-stage timing | APIs combine plan/execute/materialize for several paths | Measurement uncertainty | Shared timer explicitly labeled | No fabricated attribution |

## Direct answers to the eight required questions

1. **Do all queries currently pass through one Database worker?** Yes, per
   running Native or PostgreSQL listener, including all sessions on that
   listener.
2. **Does single-writer per StorageId require all SELECTs to serialize?** No.
   The serialization is caused by current Database/coordination ownership, not
   by the mutation-owner invariant alone.
3. **Does a long SELECT block another session's point query?** Yes. The measured
   additional latency is overwhelmingly worker queue wait.
4. **Do current row/response limits bound executor memory?** The retained row
   limit now bounds successful final output rows and stops common Heap batch
   output early. It does not bound operator working memory or output bytes.
5. **Who owns QueryResult and how large can it be?** Executor/Core creates it,
   then the worker and response/portal own it or its encoded form. It is
   `O(R×W)` with owned variable payload; 100K duplicate rows measured 51.2 MB
   logical payload and 500K scalar slots.
6. **Can execution stop when the client disconnects?** No. It normally runs to
   completion; only response delivery/cleanup observes the disconnect.
7. **Is concurrent read execution currently safe?** No proven implementation
   exists, so this audit does not enable it.
8. **What is the minimum blocker?** A detached, thread-safe execution context
   that owns/pins statement snapshot, schema/storage/projection generations and
   their GC/DDL lifetimes, integrated with transaction, cancellation, failure,
   and shutdown semantics.
