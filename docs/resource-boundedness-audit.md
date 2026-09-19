# Resource exhaustion and boundedness audit

> Historical audit. PostgreSQL compatibility findings below describe the code
> at the audit baseline; that frontend and its tests were subsequently removed.

Baseline: `3ce00a8` (2026-09-18). This is an engineering audit of the owner's
NetbaDB implementation. The [error/concurrency](error-concurrency-audit.md) and
[crash-consistency](crash-consistency-audit.md) audits remain regression contracts.
The Phase 71 [Full Sort analysis](performance.md#phase-71-full-sort-memory-and-spill-boundary-attribution)
was reviewed before classifying materialization as a defect.

## 1. Summary

Eleven findings fixed: **P0: 0; P1: 7; P2: 4**. No confirmed implementation finding
listed below is intentionally left unfixed. Whole-result materialization,
non-preemptible execution, and input-sized maintenance/recovery remain explicit
architecture limits, not claims of globally bounded memory or CPU.

Added **27 test functions**, and strengthened three existing tests: PostgreSQL
abandoned admission now loops 1,000 times, TLS admission fills four slots before
rejecting the fifth, and Top-N includes `usize::MAX`. An additional real-server
1,000-connection probe measures OS descriptors and threads. No wire/persistent
version, mutation owner, isolation, commit ordering, or durability barrier changed.

## 2. Resource budget map

`C`: configured connections; `N`: input rows; `R`: output rows; `W`: owned row
width; `G`: groups; `K`: LIMIT; `P`: partitions; `B`: frame/block byte bound.
A hard count bound is not necessarily a practical deployment memory budget.

| Resource | Owner | Current bound / classification | User-controlled growth? | Release / cleanup |
| --- | --- | --- | --- | --- |
| TCP connections, TLS handshakes | Listener connection vector | Hard `C`, default 128, configuration maximum 65,536; checked before spawning | Yes, up to admission cap | Reject excess immediately; close socket and join/reap handler |
| Runtime threads | Listener, sole Database worker, connection handlers, optional operator | Hard `C + 2`, or `C + 3` with operator | Connections; host-created programmatic caller threads are outside this count | Join on all listener exits, including failure |
| Sessions / transaction handles | Sole Database worker | Lifetime bounded by admitted connections; one execution session per connection | Yes; active SQL transaction has no age deadline | Explicit close/rollback; rollback failure terminates service |
| Worker commands | Connection/control producers and listener | Logically bounded by producer permits: at most one outstanding command per connection, two control families, one scheduler tick, plus shutdown | Network request concurrency bounded by `C`; handle clone count no longer expands queue | Replies release permits; worker/receiver drop wakes callers |
| Programmatic controls | Shared admission per handle family | Hard one queued/forwarded/executing control per family, across clones | Host controls number of blocked callers | Permit spans send and reply; failure/panic releases it |
| Scheduler ticks | Host driver | Hard one pending reply; coalesced missed cadence | Wall-clock opportunities, not SQL-created queue nodes | Completion clears pending state |
| Parked prepared participants | Core group / storage transaction handles | Core explicit group ≤1,024 members and ≤1,024 participants total; direct low-level storage API is caller/lifetime bounded | Embedded host APIs, not a streaming network command producer | Ordered commit/abort removes parked IDs and conflict entries; recovery gate on unresolved ownership |
| QueryResult | Executor/Core, then session/transport | Successful Core output rows bounded by SessionState policy; `O(R × W)` within that count and no byte/scalar-slot budget | SQL cardinality, projection, stored values | Drop after response/rejection; suspended PG portals retain unsent results |
| Full Sort | Executor | Input bounded `O(N × W)` plus sort metadata; no spill | Yes | On result/error/drop |
| Top-N | Executor | Retained candidates `≤ min(N,K)`; initial reserve `min(K,256)` | Yes | On result/error/drop |
| Aggregate | Executor accumulator | `O(G × (keys + aggregate states))` plus one batch | Yes | Finish/error/drop; repeated keys do not create new groups |
| HashJoin | Executor | Build rows/bucket indices `O(build)`; probe batch ≤256; result `O(R × W)`; left-build ordering can retain result buckets | Yes, including duplicate-key output | On result/error/drop |
| NestedLoop / IndexJoin | Executor | NestedLoop `O(N×M)` work; IndexJoin one owned match vector per probe; result output sized | Yes | Per-probe candidates drop; final result drops |
| Partitioned sequential batch | Executor pipeline | Hard **one shared 256-row batch**, independent of `P`; routing/views/metadata `O(P)` | Catalog and query | Batch drained between deliveries; views and metadata drop at end |
| Index / range lookup results | Storage caller | Input bounded owned `Vec` of matches; not a cursor memory limit | Match count and row width | At caller/probe completion |
| Native request/response frame | Protocol/connection | Hard 16 MiB payload; 65,536 collection items; frame/payload copies `O(B)` | Yes within validated lengths | Per request/message; malformed frame closes connection |
| PostgreSQL frame | PG wire / connection | Hard 16 MiB message, 1 MiB string; 1,024 parameters / 4,096 fields; remaining-byte proof before count reserve | Yes | Per message / connection |
| Native response batch | Connection thread | Complete permitted result batch; default 100,000 rows, maximum configurable 10,000,000; encoder rechecks the Core-owned row policy | Yes within configured row count and frame bounds | After writing/error; slow consumer retains batch |
| PostgreSQL prepared statements / portals | Worker session | Each map ≤1,024 objects; count bound, not aggregate byte budget | SQL, parameter values, pending results | Close/replace/session teardown; completed portal drops all row storage |
| Read-only SAVEPOINT metadata | Worker session | Hard 1,024 names, ≤63 bytes/name | SQL transaction | RELEASE/ROLLBACK/COMMIT/session teardown |
| Heap buffer frames | BufferPool | Hard configured frames, default 8 | Host configuration, not client-created frames | Pins via guards; pool closes/flushed through existing contracts |
| Heap transaction / net changes | Transaction and optional Change Stream | Input/lifetime bounded, not covered by the buffer-frame cap; live stream after-images use O(net mutations × row width); vector/index capacity can follow peak mutation count. NBCL's 64 MiB /1,000,000 mutation record limits apply at prepare, not as a transaction RAM budget | SQL writes and transaction lifetime | Insert-delete releases payload immediately; update replaces its old payload; terminal cleanup/drop releases state; rollback/recovery may read retained WAL |
| LSM pending transaction | LsmTransaction | Hard estimated 16 MiB and 65,536 pending rows, including tombstones; checked before mutation | SQL writes | Replacement credits old bytes; insert-delete removes entry; terminal cleanup clears |
| LSM encoded row | Write admission | Hard 65,504 bytes: existing 64 KiB SST payload minus 32-byte entry envelope | SQL row width | Oversized insert/update rejects before staging or WAL publication |
| LSM MemTable | Storage | Soft 4 MiB default flush threshold; retained generations/read views and maintenance affect total | Writes and long snapshots | Existing flush/compaction/snapshot lifecycle |
| SST metadata / Bloom | Storage, pinned read views | Current manifest ≤4,096 SSTs; ≤262,144 index entries/SST, 56 encoded bytes each; Bloom ≤8 MiB/SST; data block ≤64 KiB | Stored database size, snapshots | Obsolete files after publication and view release; reopen orphan cleanup |
| Partition / projection catalogs | Core | 16 MiB / 64 MiB file bounds; count reserves additionally proven by remaining bytes | DDL / untrusted persistent bytes | Open failure drops partial decode; durable publication unchanged |
| Columnar artifacts | Projection / read view | Lazy payload block ≤64 MiB, index ≤256 MiB; legacy file cap 16 GiB is not a practical RAM budget | Persisted data / projection definition | Lazy block drops; generation files retire after owners release |
| Adaptive evidence | Worker-owned pool | Defaults: 16 target windows, 64 workload shapes ×8 variants, 4 calibration epochs, 64 calibration shapes ×8 variants | Eligible query diversity within configured bounds | Typed capacity rejection/incomplete state; explicit rotation/retention |
| Physical-design evidence | Worker-owned window | Defaults: 64 index +64 Columnar candidates, 32 shapes/candidate, 64 columns/Columnar candidate | Eligible query diversity | Typed truncation/incomplete state; explicit epoch rotation |
| NBOP operator requests | Serial Unix listener | One active connection, 64 KiB payload, configured I/O timeout | Local operator | Per request/connection; worker control admission shared with clones |
| NBMR receipts | Sole worker / locked journal | Configured total file bytes; 64 KiB record; read page ≤128 receipts, columns ≤4,096 | Authorized controls, deployment config | Capacity rejects before Begin/ID; recovery gating and locked inode preserved |
| Hello/schema identities, inspection/CLI JSON | Worker / local tool | Input sized by catalog; Native Hello uses IDs/fingerprints, then existing collection/frame limits | DDL/deployment controls catalog size | Per reply/inspection; local JSON may be fully materialized |
| Local LSP documents / stdio | Editor tool / `lsp-server` 0.7.9 | Document map is input/lifetime bounded; no aggregate byte quota; three library I/O threads with rendezvous channels | Local editor, outside DB listener | didClose removes document; stdio owner joins I/O threads on termination |
| Diagnostics / optional PG trace | Error response / stderr sink | Native error text ≤16 MiB−8 bytes, PG ≤8 KiB; trace input is protocol bounded but total log volume has no DB quota | Queries and malformed connections; trace requires host opt-in | Transient formatting drops; external log retention is deployment-owned |
| Files / descriptors | Storage handles, read views, socket handlers | Lifetime/input bounded; table/SST/view counts matter independently of `C` | DDL, stored graph, snapshots | RAII + explicit close; admission cap does not cap total database descriptors |
| Mutation / advisory / admission locks | Transaction owner, NBMR runtime, admitted control caller | One mutation owner per StorageId; one exclusive lock on the journal inode; one permit per control family | Transaction/control lifetime | Terminal transaction cleanup, file/runtime drop, or reply/error releases owner; failed rollback remains fatal |
| WAL/status/history/recovery vectors | Storage / Core | Input sized `O(retained log/history)`; record bounds do not bound total replay | Writes, checkpoint/GC policy, snapshots | Existing checkpoint/GC, terminal handle release |
| Temporary files / retired generations | Publication operation / snapshot owner | Input/lifetime bounded; **no aggregate temporary-disk quota** | Maintenance/build size; failed unlink and old snapshots retain files | RAII best effort, explicit cleanup errors, validated reopen GC |

The synchronous Rust client's read/write timeout options default to `None`;
that is a caller wait policy, distinct from the server's mandatory socket limits.

## 3. Findings

| ID / severity | Subsystem / resource | Trigger and previous growth/failure | Fix | Deterministic regression |
| --- | --- | --- | --- | --- |
| R1 / P1 | LSM pending state and budget | Insert excluded the 64-byte entry estimate from preflight, then checked after insertion; an error could leave its row pending. First UPDATE/DELETE of committed rows could exceed the mutation count. Equal-size replacement at the byte cap was falsely rejected. | Compute complete replacement bytes/count before mutation, subtract old entry, account tombstone/removal correctly; existing 16 MiB/65,536 limits preserved. | Exact byte boundary, rejected insertion leaves existing state unchanged, equal-size/shrinking update, insert-delete credit, 65,536 entries plus insert/update/delete rejection, rollback, commit/reopen proves only accepted rows persisted. |
| R2 / P1 | Programmatic control queues | Arbitrarily cloned public handles each submitted one request: queue/proposal/reply memory `O(number of callers)`, independently of `C`; drain-until-empty could monopolize listener. | Shared synchronous permit, one outstanding per family; admission before reply allocation/proposal clone; forward one command per listener iteration. | Block reply, inspect occupied shared permit/empty queue, drop reply/receiver and check typed errors; 16 cloned contenders ×100 admissions plus panic recovery. |
| R3 / P1 | PG SAVEPOINT metadata | One transaction could append indefinitely: `O(savepoints × name bytes)`, reverse lookup `O(savepoints)`. | Reuse existing 1,024 named-object budget and 63-byte PG identifier width; SQLSTATE `54000`, no truncation. | Fill limit, reject next/oversized name without growth, release/reuse/rollback. |
| R4 / P1 | PG portal result payload | Each Execute cloned returned rows and retained all already-sent data; completed named portals still held `O(R×W)`. | Move rows from an owning iterator; completion replaces result with its command tag and frees backing storage. | Payload pointer identity, suspended remainder, completion has no Query rows, repeated Execute returns same SELECT tag without replay. |
| R5 / P1 | Persistent decoder count amplification | Checksum-valid tiny payloads could reserve header-declared counts before verifying available records: `O(declared count)` versus tiny input. Columnar row/version/group counts could be especially large. | Format-derived minimum-record checks in partition/projection catalogs, Columnar manifests/base/delta/lazy directories/version blocks, LSM WAL, Change Stream; also PG parameter and NBMR column counts. Delta batches cannot exceed declared mutation total. | Forged valid-limit counts with short/checksummed payloads fail at count validation; existing positive, legacy, corruption and reopen matrices retained. |
| R6 / P1 | Native/PG response encoding | Frame limit was checked after appending all large strings/values; encoder copied `O(unencodable response bytes)` before rejecting. | Checked total growth before copying variable payloads or field envelopes, using existing frame caps. | Near-full output retains identical length/capacity on rejection; oversized row emits zero wire bytes; golden/compatibility suites retained. |
| R7 / P2 | Columnar manifest CPU | Duplicate column/file checks scanned all preceding entries: `O(C² + D²)` on format-valid metadata. | Randomized HashSet membership, expected `O(C+D)` checks and input-sized set memory. | 1k/10k/100k valid columns decode; duplicate last column rejected. |
| R8 / P2 | Whole-file reads | Change Stream guard used unbounded `fs::read` although only an 80/104-byte header is valid. Partition/legacy Columnar reads relied on initial metadata length only if file grew concurrently. | Guard reads at most 105 bytes and rejects extra data; catalog/artifact reads use existing file cap plus one sentinel byte. | Both guard versions accepted, oversized guard rejected; existing malformed/oversized catalog and artifact tests. Concurrent file growth was identified by code inspection, not a race-timing claim. |
| R9 / P2 | LSM accounting CPU | Every pending INSERT/UPDATE/DELETE rescanned every entry: N inserts required `N(N+1)/2` accounting visits, despite bounded pending memory. | R1 replacement accounting inspects only the replaced row; map operations remain `O(log N)`. | Real 65,536-row pending transaction and 100k-row partition fixture; accounting totals cross-checked independently after replacement/deletion. |
| R10 / P1 | LSM flushability / writer availability | WAL admission accepted rows too large for an SST's 64 KiB block. A committed oversized row then made flush fail; once automatic flush was required, later writers could not be admitted. | Check encoded row plus the existing 32-byte SST entry envelope before staging insert/update. This is a format-derived, non-configurable bound; WAL decoding remains compatible. | Exact-fit row commits, flushes and reopens; one extra byte rejects insert/update without pending-state change or invalidating the old handle; subsequent writer succeeds. |
| R11 / P2 | Change Stream net-change coalescing CPU | Heap updates/deletes and LSM logical-change construction searched the accumulated vector on each operation; deleting an inserted row additionally shifted the vector. Work could be O(N²). An isolated original-code counter measured 5,050 /500,500 /50,005,000 lookup visits for 100 /1k /10k inserted-then-updated rows. | Index current physical versions with a randomized HashMap. Deleted inserts become payload-free holes; stable compaction occurs after over half the slots are removed, or before preparing a slice. No change to mutation order, original versions, formats or durable barriers. | 1/1k/10k/100k insert/update/delete sequences preserve exact output order and rebuild indices correctly; vector length (not allocated capacity) is ≤2×retained mutations and total compaction visits ≤2N; mixed Update→Update, Update→Delete, Insert→Delete and clear/reuse retain exact net effects. |

Implementation and regression entry points: [LSM](../crates/netbadb-storage/src/lsm.rs)
(R1/R9/R10), [control admission](../crates/netbadb-server/src/control_admission.rs)
(R2), [PG session and portal](../crates/netbadb-server/src/postgres.rs) (R3/R4),
[Native wire](../crates/netbadb-protocol/src/lib.rs) and
[PG wire](../crates/netbadb-pgwire/src/lib.rs) (R6),
[Columnar](../crates/netbadb-storage/src/columnar.rs) (R5/R7/R8), and
[Change Stream](../crates/netbadb-storage/src/change_stream.rs) (R5/R8/R11).
R5 also covers [partition catalogs](../crates/netbadb-core/src/partition_catalog.rs),
[projection catalogs](../crates/netbadb-core/src/projection_catalog.rs), and
[NBMR receipts](../crates/netbadb-server/src/physical_design_receipts.rs).

Minimum encoded sizes used by R5 are lower bounds, not estimates of Rust layout:
partition table 49 bytes / range binding 18; projection entry 68 / pending ColumnId
4; Columnar manifest column 8 / delta segment reference 48 / version key 24 /
mutation descriptor 25 / legacy row-group prefix 4 / lazy row-group prefix 12 /
legacy column chunk 30 / lazy column directory 46; LSM WAL mutation 28; Change
Stream mutation 16; PG OID or parameter envelope 4 and format code 2; NBMR ColumnId
4. Variable payload checks still run afterward. These checks cannot reject a
complete valid record merely because it is large; absolute existing format caps
continue to apply separately.

The SAVEPOINT limits are new admission policy, not new wire fields. They are not
configurable in this slice. Transactions needing over 1,024 simultaneously live
savepoints or names over 63 UTF-8 bytes now receive a typed error; applications
can release old savepoints and use shorter names. The count deliberately matches
the existing prepared-object budget. The 63-byte bound matches the existing PG
identifier width; no implicit truncation or name aliasing is introduced. Control
capacity one follows the existing synchronous, single-worker ownership model;
extra callers block in their own host threads. All other checks preserve existing
configuration or derive directly from format envelopes.

R10 brings write admission into agreement with the unchanged SST format. The
65,504-byte limit applies to the entire canonical encoded row, not each Text/Bytes
value. Previously accepted larger rows were not flushable. This audit prevents
new oversized rows; it does not silently discard or rewrite historical WAL data.

## 4. Network DoS audit

Native and PG admission check the live connection vector before handler spawn;
TLS occupies an admitted slot before authentication, and authentication completes
before a principal/session is admitted. Failed handshakes, malformed startup,
authorization failure, normal EOF, read/write errors, and handler panic follow
reaper/cleanup paths. The TLS test admits four stalled handshakes, rejects the
fifth, then closes and joins them during shutdown. Existing Native/PG integration
tests cover partial frames, idle clients, disconnect rollback, and replacement
admission; previous fatal rollback/worker-exit contracts remain intact.

A new Native test keeps two clients' sockets open after QueryStart, without
reading either 16 MiB result (128 rows ×128 copies of a 1 KiB Text field). A third
client completes Hello/Ping while those response batches belong to connection
threads. With a 500 ms write timeout, both failed writes are observed and active
connections return to zero; a second run shuts down with the peers still open
and joins every handler. It synchronizes on protocol replies and runtime metrics,
not sleep-based assumptions. This is bounded test input, not an OOM experiment.

A PostgreSQL startup test occupies four slots, confirms each SSL refusal response,
then leaves a partial startup frame. The fifth connection is rejected. Idle
timeout returns all four to EOF and a replacement is admitted; another run shuts
down all four unfinished startups. The current PostgreSQL listener is loopback
plaintext only; mutual TLS coverage belongs to Native.

Defaults are five minutes read inactivity and 30 seconds write inactivity;
configured timeouts are 1 ms through 24 hours. They apply to blocking socket I/O,
including TLS, **not a deadline for an entire frame or SQL statement**. A client
that continually makes small progress can hold its admitted slot. One expensive
query can monopolize the sole Database worker. Shutdown closes sockets to unblock
network waits, but cannot preempt synchronous query/maintenance code already
executing. The serial operator socket likewise has an inactivity timeout, not a
whole-operation deadline.

Response queues do not accumulate multiple outstanding queries per connection.
Nevertheless, C slow readers can retain C complete response batches. PG suspended
portals multiply retained, unsent results by their per-session portal count.
The Core output-row cap and per-frame byte cap do not jointly provide a
practical whole-result/global memory budget: row width, operator state and the
sum of retained responses remain separately unbounded. This audit does not
claim otherwise. See the later
[server execution/resource audit](server-execution-resource-audit.md) for the
enforcement-point correction and measurements.

An idle transaction is closed by socket inactivity timeout or disconnect cleanup,
but a client making periodic progress can keep its transaction and writer owner
alive indefinitely. There is no transaction-age budget. Disconnect does not
cancel a command already queued at the worker: the connection handler is waiting
for that command's reply and observes transport failure afterward. Its admitted
command remains until completion; adding cancellation would require explicit
transaction and mutation-outcome semantics.

## 5. Queue audit

| Channel / producer | Actual admission bound | Full / stopped behavior |
| --- | --- | --- |
| Native `WorkerCommand`, PG `PgWorkerCommand` (`mpsc::channel`) | Private clients used by ≤C handlers, each waiting for its reply before another Open/Request/Close; two permit-bound control families; one scheduler tick; terminal shutdown | Implicit producer backpressure through synchronous response; receiver loss is typed error. Mutation receive failure remains uncertain, not safe-to-retry success. |
| Adaptive control requests (`mpsc::channel`) | Shared permit across every public handle clone; one outstanding through worker reply | Caller blocks before publishing; dropped worker/host wakes admitted caller, which releases permit. |
| Physical-design controls (`mpsc::channel`) | Same, independently one for this family; proposal clone occurs after admission | Same; existing `MutationOutcomeUncertain`/post-Begin behavior preserved. |
| Tick replies (`sync_channel(1)`) | One pending tick and one reply receiver | Missed ticks coalesce; no unbounded catch-up queue. Existing logical-cadence tests retained. |
| Request replies / worker startup (`sync_channel(1)`), worker handoff (`sync_channel(0)`) | One per admitted operation/startup, fixed lifetime | Completion or disconnection; abandoned PG Open unregisters session. |
| Server/operator shutdown, operator-failure, worker-fatal events | Private fixed lifecycle senders, constant number of terminal events | No public producer able to stream events; receivers drop on teardown. |
| Operator connection handling | One serial accepted Unix connection | Socket backpressure and configured I/O timeout; shares control permits. |
| Storage `parked_prepared` VecDeque / conflict maps | Explicit Core group caps both members and participants at 1,024; direct storage API follows caller-owned prepared handles | Stable commit/abort order; terminal resolution removes entries. This is retained transaction state, not an asynchronous work-producer queue. |
| Local LSP library stdio channels | Pinned `lsp-server` 0.7.9 uses `bounded(0)` for reader, writer and message dropper | Synchronous backpressure; local stdio/document input is a separate tool boundary with no new aggregate quota in this audit. |

A syntactically unbounded channel is not automatically an unbounded queue, but
cloneable public producers previously invalidated the worker-queue argument.
The new permit closes that hole without dropping mutations or adding a Busy wire
variant. Worker/listener code never takes the admission mutex, so reply handling
and receiver teardown do not need the permit held by a blocked caller. Mutex
poison recovery is safe because it protects admission only, not mutable data.

Native's listener may additionally submit one synchronous Close after a handler
panic, while an abandoned request from that handler still awaits the worker. This
adds at most one listener-owned cleanup command, not an additional producer loop.

## 6. Query memory audit and measurements

Measurements use real LSM storage and the production executor, plus existing
private operator counters. They count logical ownership, not process RSS.
Text and Bytes are 128 bytes each. Ordinary output is `(id,text,bytes)`; duplicate
projection is `(id,text,bytes,text,bytes)`. LIMIT 1 is also executed at each scale.

| Output rows | Ordinary scalar slots | Ordinary owned payload bytes | Duplicate slots | Duplicate owned payload bytes |
| ---: | ---: | ---: | ---: | ---: |
| 1,000 | 3,000 | 256,000 | 5,000 | 512,000 |
| 10,000 | 30,000 | 2,560,000 | 50,000 | 5,120,000 |
| 100,000 | 300,000 | 25,600,000 | 500,000 | 51,200,000 |

The payload numbers exclude Vec/String headers, row vectors, spare capacity,
storage cache, and encoding scratch; they are not peak allocator/RSS figures.
They demonstrate `Ω(R×W)` ownership before transport policy. Fixing that requires
an executor/result budget or streaming contract that reaches blocking operators;
this audit deliberately does not replace the public result API or add spill.

| Operator | Evidence / classification |
| --- | --- |
| Aggregate | 1k/10k/100k distinct Int64 or 128-byte Text keys (including NULL), twice as many input rows, COUNT plus MIN. Exactly G key materializations/groups. A non-NULL key used by both GROUP BY and MIN needs two owners: G−1 key clones in this test are necessary, not a leak. Existing COUNT-only tests prove moved unique keys / borrow-only hits. |
| Full Sort | 1k/10k/100k rows retain respectively 128,000 / 1,280,000 / 12,800,000 Text bytes and exactly N scalar slots before/after sort. This confirms Phase 71's full-input ownership boundary, not a new streaming claim. |
| Top-N | Existing boundary/equality/NULL/projection tests extended through `usize::MAX`: candidates ≤min(K,N), stable equal-key ordering, no `reserve(K)` for huge LIMIT. SQL's maximum unsigned integer LIMIT is rejected by the current signed literal contract; OFFSET is unsupported and errors. |
| HashJoin | Duplicate-key 10×10, 100×100, 300×300 inputs yield 100 / 10,000 / 90,000 correct rows. Build rows and bucket indices remain 10 / 100 / 300; one distinct bucket; no owned key clone; probe batch ≤256. Quadratic result ownership is required by SQL. Existing build-left/order/NULL/residual tests remain. |
| NestedLoop | General `O(N×M)` predicate work and potentially `O(N×M)` output are legitimate for the chosen plan. No optimizer/algorithm rewrite. Existing differential join tests cover results and errors. |
| Index/range/IndexJoin | Owned Vec match APIs can retain all matches; turning the returned Vec into later chunks would not cap allocation. This remains a storage/API limit. SST iteration itself is block based and metadata bounded. |
| Projection | Existing pointer tests establish moves for unique/reordered values and clones only for duplicate output ownership. The wide-output experiment quantifies the required multiplication. |
| PG value conversion | The worker can temporarily own a policy-permitted QueryResult together with all encoded rows. Text-format bytea uses `2×bytes + 2` hex bytes, which is required representation expansion. R6 bounds subsequent wire-buffer copying; neither it nor the output-row cap turns this conversion into a streaming or byte-budgeted API. |
| Partitioned SeqScan | 1k/10k/100k total rows over six partitions (three empty) always peak at one shared 256-row batch and `ceil(N/256)` deliveries. 64 all-empty partitions emit no batch. Metadata/views remain O(P), not constant total memory. |

The first four-test resource operator run took 105.18 s, exposing LSM's repeated
pending-map accounting during fixture writes. With R9 fixed, the same four tests
took 6.50 s in this environment. These are observations, not timing assertions or
a general database performance claim. The constant-time accounting derivation
and state/boundary assertions are the regression contract.

## 7. Lifetime and leak audit

A separate plaintext Native fixture accepted 1,000 sequential malformed-frame
connections; each client waited for EOF/reset before proceeding. `lsof` counted
numeric descriptors and `ps -M` counted server threads at quiescent boundaries:

| Boundary | Numeric FDs | Threads |
| --- | ---: | ---: |
| Ready | 7 | 3 |
| After 100 disconnects | 7 | 3 |
| After 1,000 disconnects | 7 | 3 |

Fixture shutdown exited 0. This is a real OS observation on macOS, not a portable
RSS/FD upper bound for all deployments. The deterministic PG owner loop separately
abandons 1,000 Open replies, then successfully admits the same SessionId; previous
failed rollback, worker panic, startup failure and handler panic tests are retained.

Connection handles own socket/control descriptors; the reaper removes finished
handles and closes worker sessions. Pending messages/replies drop with their
receiver; completed portal payload ownership now ends promptly. Storage read views
can intentionally pin history and generation files. A client holding a legitimate
long transaction is not diagnosed as a reference leak.

LSM streaming builders and Columnar prepared artifacts have RAII cleanup;
publication, obsolete-file and orphan cleanup errors are observable/recoverable
through existing paths. Reopen only reclaims validated non-authoritative objects.
The prior crash matrix and existing failed-unlink/orphan tests remain the evidence.
Best-effort Drop cannot promise disk reclamation when the filesystem rejects
unlink; no hard aggregate temporary-disk quota or new retry loop was introduced.

## 8. Algorithmic DoS and second review

Parser limits remain 1 MiB SQL, 32,768 tokens, 1,024 identifier bytes, 4,096 CREATE
columns, 256 expression admissions and depth 64. New inputs of nesting/unary depth
100/1,000/10,000, very wide projections, overlong SQL, and huge LIMIT/OFFSET return
errors without attempting stack overflow or OOM. PG compatibility lexer limits
(4,096 tokens, nesting 32, pattern bytes/atoms 1,024) remain independent. No timing
threshold is used to declare parser safety.

The second review searched allocation/reserve/resize, channels/spawn, file reads,
collections, QueryResult conversion, operator state, and temporary-file cleanup.
It classified input-backed allocations separately from fixed, configuration and
lifecycle bounds. That pass found the extra Change Stream guard/read and mutation
count checks, the NBMR count check, and the LSM accounting and SST representability
defects and Change Stream coalescing cost; it also checked
that the platform gate stayed on the original non-Unix receipt test.

Schema catalog/journal and coordinator decoders already prove record envelopes
before reserving; the prior SST entry-count fix remains. Adaptive/physical-design
capacity, shape truncation, counter overflow and rotation tests remain. NBMR keeps
its configured file budget, reserved Outcome space, bounded read pages and
post-Begin uncertainty/recovery gate. No telemetry overflow can grow queues or
silently turn an unsuccessful mutation into success.

HashJoin/group lookup use randomized hashing and exact equality after collisions.
Manifest duplicate validation now uses randomized sets rather than quadratic
prefix scans. Full Sort `O(N log N)`, high-cardinality aggregation, large valid joins,
DDL/backfill and recovery work can still monopolize the synchronous owner. Count
bounds reduce admissible input; they do not supply CPU scheduling or cancellation.

Public error text retains its existing caps: Native 16 MiB−8 bytes and PG 8 KiB;
operational/internal diagnostics remain redacted. Some safe compiler errors first format their
input-bounded diagnostic before truncation; this is not a global allocation
budget. `NETBADB_POSTGRES_TRACE` is off by default; when enabled it writes complete
protocol-bounded SQL to stderr (Bind logs counts/formats, not parameter values).
Repeated trace/error output needs an external bounded log sink/retention policy;
a blocked synchronous stderr sink can also stall its emitting thread. Hello
fingerprints and local inspection JSON remain proportional to catalog size.
Server/SDK request paths do not add an automatic retry loop for resource errors.

## 9. Remaining risks and deliberate deferrals

- **Confirmed implementation defects left unfixed:** none from R1–R11 for newly
  admitted operations. Historical oversized LSM rows accepted by earlier versions
  can remain unflushable; no destructive automatic repair or migration is supplied.
- **Architecture limits:** fully owned, row-count-limited but not byte-limited QueryResult; C retained response batches;
  suspended portals retaining unsent results; full Sort; input-sized grouping,
  joins and index/range Vec APIs; non-preemptible worker execution/shutdown;
  long-lived transactions pinning visibility/history; input-sized Heap transaction
  state and Change Stream after-images (record limits are checked at prepare).
- **Configuration/deployment responsibility:** connection/timeouts/result-row
  limits, Heap frames, adaptive/physical evidence sizes, NBMR total bytes, OS
  FD/memory/disk capacity, TLS exposure, checkpoint/GC cadence. A maximum numeric
  setting is not a recommended practical setting. Legacy Columnar's 16 GiB file
  ceiling and aggregate SST metadata ceilings can exceed available RAM.
- **Legitimate expensive workloads:** high-cardinality groups, duplicate-key join
  outputs, wide duplicate projections, many partitions/tables, large DDL and
  maintenance/recovery histories. No silent truncation or semantic weakening.
- **Future result budgets/streaming:** resource refusal now propagates through
  Core/executor/transaction ownership; define blocking-operator and output-byte
  budgets, cancellation checkpoints, and a separate spill design before
  changing result API contracts. The later correction adds neither another
  worker nor a protocol cancellation surface.
- **Not reproduced / evidence limits:** no unbounded connection-thread/FD leak in
  the 1,000-connection probe; no actual OOM or stack overflow attempted; no physical
  power-loss claim; file-growth races are bounded by reader construction and not
  supported by sleep-based race tests; socket inactivity is not a total deadline.

## 10. Validation

Baseline fmt/check/Clippy passed. Baseline `cargo test --workspace` ran 1,857
passing unit/integration tests (3 ignored), then failed in rustdoc with E0463
because a subsequent incremental build reused its target artifacts. This was a
validation scheduling mistake, not a baseline regression. An isolated `git archive
3ce00a8` source/target rerun of `cargo test --workspace --doc` passed all documentation
checks (3 executed doctests). Final validation uses serial commands on one target.

The final source passed all commands below, run serially on the pinned Rust
1.97.1 toolchain on macOS. The full Rust run took 1,225.37 seconds and completed
**1,884 unit/integration tests plus 3 doctests: 1,887 passed, 0 failed, 3 existing
ignored tests**. The baseline-to-final increase is exactly the 27 added tests.

| Command | Final result |
| --- | --- |
| `cargo fmt --all -- --check` | Passed |
| `cargo check --workspace --all-targets` | Passed |
| `cargo clippy --workspace --all-targets -- -D warnings` | Passed; no lint suppression added |
| `cargo test --workspace resource_ -- --nocapture` | 26 passed; the additional control-admission test runs in the full suite |
| `cargo test --workspace` | 1,887 passed including doctests; 3 existing ignored tests |
| `go test -race ./...` (from `sdk/go`) | Passed; Go reused its valid test cache |
| `sh scripts/test-go-sdk.sh` | Passed; live Rust fixture and Go cross-language tests |
| `python3 scripts/test-postgresql-psql.py` | Passed with `/opt/local/lib/pgsql/bin/psql` 17.11 and `DYLD_LIBRARY_PATH=/opt/local/lib/icu/lib` |
| `python3 scripts/test-resource-lifecycle.py` | Passed; 7 numeric FDs /3 threads at ready, after 100 and after 1,000 failed connections; shutdown exit 0 |
| `git diff --check`, Python AST parse, changed local Markdown paths | Passed |

The final workspace suite includes Native/PG/TLS integration, golden wire and
format checks, malformed/truncated input, close/reopen, panic/failure cleanup,
and the prior error/concurrency and crash-consistency matrices. No test was
removed or weakened. Real sockets and OS inspection were run with the required
local permissions; no validation blocker remains.

Counterexample checks used an isolated baseline source/target so they could not
invalidate the final build artifacts. Applying the new R1 and R10 regression
cases there failed at their intended assertions: the rejected pending insert left
256 entries instead of 255, and the oversized SST row was accepted. Both pass on
the final implementation. A temporary original-code counter for R11 measured
5,050 /500,500 /50,005,000 linear-search visits at 100 /1k /10k rows; no probe
instrumentation remains in production. Final coalescing tests measure 1,986 /
19,983 /199,979 compaction visits at 1k /10k /100k rows, below 2N.

Second-pass allocation/channel/lifetime review and final diff review found no
additional confirmed defect beyond the fixes and residual conditions listed
above. The audit does not infer a global memory, CPU, transaction-age or temporary-
disk budget from passing these tests.
