# Error handling and concurrency audit — 2026-09-18

> Historical audit. PostgreSQL compatibility referenced below was removed after
> this audit; retained findings describe the state and evidence at that time.

Baseline: `2da3bc1`. Scope: error propagation, transaction ownership, worker and
session teardown, retry boundaries, bounded decoding, and filesystem identity.
The Go SDK cancellation/transaction boundary was also inspected against its
explicit non-concurrent Client contract. No wire/persistent version, scheduling
policy, writer concurrency, or planner
cost model changes are included.

## Findings

Nine distinct findings were fixed: one P0 and eight P1. No confirmed finding is
deferred. Error and concurrency classifications below can overlap.

| ID | Severity | Boundary / classification | Reproduction and previous failure | Resolution / regression |
| --- | --- | --- | --- | --- |
| F1 | P0 | Core coordinator; Error / Retry | A partial CORD append and its rollback truncate both fail. The cleanup error was discarded; retry appended after the incomplete record, turning a recoverable tail into middle corruption. | Preserve both I/O errors in `AppendCleanup`; invalidate the existing optional file authority until reopen. A deferred Complete cannot hide this fatal loss of authority. `failed_append_and_truncate_disable_authority_until_reopen` proves no retry writes and two successful reopen boundaries; `failed_deferred_complete_tail_cleanup_is_not_a_benign_checkpoint_error` covers checkpoint handling. |
| F2 | P1 | PostgreSQL worker; Lifecycle / Ownership | Damage an active transaction's WAL undo input, then queue Close and Query. Close returned an error but the worker executed the Query. | Return the contextual session cleanup failure from the worker immediately. `disconnect_rollback_failure_stops_worker_before_the_next_request` also restores the injected bytes, reopens, verifies rollback, and admits a new writer. |
| F3 | P1 | PostgreSQL listener; Lifecycle | Worker exits with an error or panics while no external shutdown is requested; listener remained running. | Poll worker completion and join to recover its actual failure. `worker_failure_terminates_the_accept_loop_without_external_shutdown`. |
| F4 | P1 | PostgreSQL listener; Shutdown | Socket configuration, clone, ID allocation, spawn, or accept error returned before connection/worker cleanup. Reaper and shutdown also discarded connection panics. | Every loop result reaches one cleanup path; close sockets, join every connection, then shut down/join worker. Report connection panics. `accept_failure_closes_connections_joins_worker_and_retains_both_errors` and `connection_panic_is_reported_by_reaper`. |
| F5 | P1 | PostgreSQL admission; Lifecycle | Drop admission reply receiver before Open; session remained registered. Reusing that exact ID demonstrates the leak without timing. | Failed Open/Request delivery closes its session; rollback failure is fatal, not a benign disconnect. `abandoned_admission_does_not_leave_a_registered_session`. |
| F6 | P1 | Native + PostgreSQL runtime; Error | Startup reported its configuration error but ignored the worker's close error/panic. Native runtime cleanup could overwrite the primary listener failure. | Join startup workers and preserve both failures; runtime cleanup preserves primary plus cleanup errors. Both `startup_failure_retains_worker_cleanup_error_and_panic` tests and both accept-failure cleanup tests. |
| F7 | P1 | Native protocol; Error disclosure | Direct and wrapped Database errors serialized a private storage path. | Operational, internal, and transaction-state errors use bounded public messages, retaining the existing code and transaction state. `native_database_errors_do_not_expose_private_paths_or_internal_details`. PostgreSQL already had this category boundary. |
| F8 | P1 | NBMR journal; TOCTOU | Rename the active journal and install a replacement; Begin/Outcome still appended to the locked but orphaned inode. | Verify final component device/inode against the open locked file at startup, before tail repair, and before/after synced append. Failure recovery-gates further mutations. `live_journal_replacement_blocks_begin_and_outcome` verifies both files remain unchanged on pre-append detection. |
| F9 | P1 | LSM SSTable decoder; Error / Allocation | A block's unchecked entry count reached `Vec::with_capacity` before the decoder checked payload size. A forged `u32::MAX` count could request hundreds of GiB. | Bound count by the minimum 32-byte entry envelope before allocation. `sstable_entry_count_is_checked_before_reserving_rows` covers legal tombstones, every short envelope, a valid-checksum forged block, and file reopen. The pre-fix probe uses count 2 to avoid deliberately attempting a huge allocation. |

The old implementations were run against the new F1, F2, F3, F5, F7, F8, and F9
regressions and failed as expected. F4/F6 exercise the cleanup boundaries directly,
including actual socket interruption and thread joins. Tests use channels,
thread completion, and explicit I/O injection; no sleep-based race probability.
A timeout in F3 bounds a failed test, not the schedule that reproduces the bug.

## Ownership model

| Object | Creation / owner / read and mutation authority | Send / Sync boundary | Cleanup and join |
| --- | --- | --- | --- |
| Native / PG listener | `start` binds and configures; accept thread owns listener and connection registry | Listener moves to accept thread; no Database is moved there | Handle signals accept loop and joins it; all loop exits resolve connections then worker |
| Connection / TLS | Accept thread creates handler and control-socket clone; handler alone parses/writes protocol. Native authenticates TLS before worker admission | Socket/client channel handles cross threads; no session/transaction object crosses | Handler sends Close; listener interrupts sockets and joins handlers; TLS admission failure has no database session |
| Database / SessionState / PG execution session | Database is opened inside dedicated worker; worker creates sessions, authorizes and executes commands serially | Database's `Rc`/`RefCell` graph is not Send/Sync; it remains worker-local | Explicit close/rollback, then Database close; fatal cleanup terminates worker and becomes a server error |
| Transaction / Heap / BTree / LSM / partition coordinator | Session or embedded caller owns transaction; storage runtime owns leases/status; BTree follows Heap's authoritative StorageId | Synchronous local ownership, not a shared mutex-protected Database | Failed resolution retains handles/state. Participant sets use ordered storage identities; group handoff keeps prepared ownership explicit |
| Operator listener | Server creates local Unix listener, owns policy/token and control-channel handles; worker owns applied state | Channels carry owned request values, not mutable core references | Operator shutdown joins listener; socket cleanup verifies its original device/inode |
| Adaptive host / scheduler | Host owns clock/cadence and one pending reply; worker owns evidence, scheduler and maintenance execution | Typed commands only; at most one pending tick, missed ticks coalesce | Channel closure clears pending host observation; scheduler faults remain separate from foreground results |
| Physical-design runtime | Worker owns bounded evidence and apply logic; programmatic proposal holds weak lifetime identity | Weak identity/token can cross as approval data; worker rechecks identity and epoch in the same command as apply | Restart invalidates old mutation authority; exact already-created target may be recognized without replay |
| Remote Rust / Go clients | Caller owns one connection/session; transaction and row handles enforce the request sequence. Go cancellation callback only closes the immutable connection field | Rust uses exclusive borrows; Go explicitly does not support concurrent Client use. Transport close may interrupt pending I/O | Protocol/I/O failure closes or poisons the connection; no automatic mutation replay. Explicit rollback confirms rollback, socket close does not |
| NBMR | Worker opens, exclusively locks and owns the file; startup reconciles before readiness | No second journal mutation owner; file identity is distinct from pathname | Drop releases advisory lock. Ambiguous writes gate mutations until reopen/reconciliation; readers do not authorize replay |

## Wait graph and shutdown

```text
connection -> worker reply
operator -> accept-loop forwarding -> worker reply
accept-loop -> completed connection join / shutdown connection joins
accept-loop -> worker shutdown reply -> worker join
server handle -> operator shutdown/join -> accept-loop shutdown/join
worker -> synchronous storage I/O
```

Worker replies use one response per capacity-one channel. The worker does not
wait for a connection to consume its response. Dropping a reply receiver makes
send fail; dropping the command receiver drops queued reply senders and wakes
waiters. During normal server-handle shutdown the accept loop and worker remain
available while the operator finishes. Connection sockets are interrupted before
joining; worker shutdown is requested after those joins. On worker panic/error,
receiver destruction wakes connections and the listener resolves the join cause.
No worker callback waits on the accept/operator/connection thread, and no
synchronous mutex is held across these waits. Metrics atomics are observational.

This establishes no identified channel wait cycle. It does not supply a
statement deadline: a stuck OS I/O call or unbounded execution time can delay
shutdown. That is a current synchronous-execution limitation, not evidence of a
new deadlock or authorization for additional database workers.

## Transaction state and retry

| Boundary | Failure behavior / retry rule |
| --- | --- |
| Explicit BEGIN / execution | Session has one transaction handle; all operations run on the worker. Authorization precedes writer acquisition. Pending/failed states reject incompatible statements. |
| COMMIT before/after durable decision | Session clears its handle only after success. Storage commit/prepare sync retries reuse their record identity; coordinator decision uncertainty keeps the handle and blocks rollback. No failed response proves that a mutation did not commit. |
| ROLLBACK | Session retains the handle if rollback fails. No new writer is allowed to bypass unresolved ownership. |
| Disconnect / shutdown | Attempt close/rollback; failure stops service. The F2 test confirms the PG worker no longer serves a queued request after failed undo. Crash recovery, not a fabricated successful rollback, resolves the durable state. |
| Coordinator append cleanup | Successful tail truncate preserves existing retry behavior. Failed truncate disables file authority; retry returns `AuthorityUnavailable` without appending. Reopen validates/truncates the legal tail before another decision. |
| Autocommit | Callers cannot infer rollback from an I/O error. No server/client automatic SQL replay is introduced. Existing implicit schema-transaction handles remain session-owned on resolution failure. |
| Network reconnect / lost response | Client poisons/closes the affected connection on protocol/I/O failures; reconnect does not replay an operation. Mutation outcome may be unknown. |
| Operator / physical design | Exact-target recognition is idempotency, not general mutation replay. A lost response does not refresh token/epoch or change target. After durable Begin plus uncertain outcome, inspect/reconcile the known receipt; do not repeat mutation automatically. |

Inspection included Heap/LSM transaction state transitions, coordinator ordered
participants/group resolution, retained schema-transaction ownership, WAL append
and sync failure paths, BTree publication under a validated writer lease,
partition routing, and managed projection pending-intent publication. Change
Stream finalization is separate from authoritative commit: a finalization I/O
failure can leave committed data and an unavailable stream requiring reopen.
It must not be interpreted as permission to replay DML.

## Scan and second review

Both passes searched production source candidates for panic/assert/unwrap/expect,
ignored results, fallback conversions, `map_err`, take/replace, shared-state
primitives, channel operations, joins, Drop/close/shutdown, commit/rollback,
StorageId, runtime identity/epoch, and completion flags. Tests, fuzz assertions,
`#[cfg(test)]` hooks and the fallible Columnar decoder method named `expect` were
separated from production panics. This is a code-and-test audit, not a proof of
all thread schedules or a new fuzzing campaign.

The retained production panic sites were traced to private validated invariants:
retired Heap intent variants; committed index publication under its writer lease;
validated prepared batches; Change Stream enum branches under exclusive mutation;
executor's private opaque-prehash hasher; owned temporary-file lifetime; infallible
formatting into String; daemon lifecycle action exhaustiveness. No newly proven
external-input panic was found in those paths.

Ignored-result classifications include disconnected one-shot reply receivers,
best-effort socket shutdown followed by join, cleanup of unpublished temporary
files, and Drop fallback after explicit close APIs. Conversion `.ok()` in optional
optimization/inspection paths is not equivalent to discarding an authoritative
commit. Group prepare cleanup retains an unresolved member even when returning
the original prepare error. F1 was discovered while rechecking the superficially
similar ignored tail truncate: it did permit unsafe later appends and was fixed.

Native frame/count/UTF-8 decoding, PG lengths/tags/counts, page/slot/row decoders,
WAL/CORD record envelopes, index node payloads, SSTable/manifest/catalog bounds
were reviewed along with their existing malformed/truncation tests. Bounded
protocol frames are not an executor result-memory bound. No format or protocol
version changed.

Final review checks the new paths for receiver-drop progress, no new production
panic/lock/async runtime, no removal before successful rollback, no reply replay,
no append after poisoned authority, and preservation of both primary and cleanup
errors.

## Remaining risks

- **Confirmed unfixed defects:** none from this audit.
- **Architecture/deployment limits:** one synchronous execution owner; socket
  timeouts are not statement cancellation; the Core output-row limit does not
  bound operator working memory or output bytes; programmatic control queues do not provide a global
  producer memory budget. Advisory locks and pathname checks assume trusted
  parent directories and cooperative actors. A continuously racing or privileged
  namespace actor remains outside the documented guarantee.
- **Roadmap:** general concurrent writers, same-StorageId multi-writer execution,
  multiple database workers and serializable isolation are not audit defects.
- **Not demonstrated:** hardware power-loss outcomes, arbitrary OS scheduling,
  network filesystem lock semantics, and all possible multi-fault combinations.
  Injected append/sync failures prove the tested state transitions, not a hardware
  durability certification. No live retry was added for unavailable Change Stream
  finalization; authoritative commit and stream recovery remain separate.

## Validation

Commands were run with the pinned Rust toolchain. Local socket tests were rerun
outside the filesystem/network sandbox after its loopback bind returned
`PermissionDenied (Operation not permitted)`; this is not a remaining blocker.

| Command | Result |
| --- | --- |
| `git diff --check` | Passed |
| `cargo fmt --all -- --check` | Passed |
| `cargo check --workspace --all-targets` | Passed |
| `cargo clippy --workspace --all-targets -- -D warnings` | Passed |
| `cargo test --workspace` | Passed on final code, including all 13 added regressions; no failed tests |
| `cargo test -p netbadb-core coordinator_log::tests` | Passed: 18 tests, including append cleanup and retry/reopen coverage |
| `cargo test -p netbadb-storage sstable_entry_count_is_checked_before_reserving_rows -- --nocapture` | Passed: malformed counts, truncated envelopes, valid-checksum forged block and reopen |
| `cargo test -p netbadb-server --lib` | Passed: 260 tests at the first server pass; the later Native cleanup test is included in final boundary/workspace runs |
| `cargo test -p netbadb-server -p netbadb-client -p netbadb-protocol -p netbadb-pgwire` | Passed on final code: Server 261 unit + 11 PostgreSQL integration + 26 TCP integration; Client 14 unit + 3 integration; Protocol 11; pgwire 8 |
| `go test -race ./...` (from `sdk/go`) | Passed for both Go packages; no race reported |
| `GOFLAGS=-race sh scripts/test-go-sdk.sh` | Passed: plaintext and mutual-TLS cross-language integration, race detector enabled |
| `python3 scripts/test-postgresql-psql.py` | Passed with psql 17.11; invoked via `runpy` with explicit environment below |

The psql probe used `PSQL=/opt/local/lib/pgsql/bin/psql`,
`DYLD_LIBRARY_PATH=/opt/local/lib/icu/lib`, and
`NETBADB_PSQL_TARGET_DIR` set to this checkout's ignored `target` directory.
No alternate PostgreSQL installation was used.

Pre-fix failure evidence came from `cargo test -p netbadb-server lifecycle_tests
-- --nocapture`, `cargo test -p netbadb-server native_database_errors_do_not_expose
-- --nocapture`, `cargo test -p netbadb-server live_journal_replacement
-- --nocapture`, `cargo test -p netbadb-core
failed_append_and_truncate_disable_authority_until_reopen -- --nocapture`, and
`cargo test -p netbadb-storage sstable_entry_count_is_checked_before_reserving_rows
-- --nocapture`. These failures are intentional red regressions, not final
validation failures. Two initial synthetic-listener harness runs were stopped
and corrected (unused command receiver and blocking test listener); they are not
counted as repository findings. Standalone storage tests emit existing unused
test-hook warnings; the required workspace Clippy invocation passes without
suppressions.

The final Core unit run passed 660 tests; its three pre-existing ignored tests
are a manual 10K/100K cost probe and two explicit fuzz-corpus generators. They
were not enabled or changed for this audit. The final workspace run completed
successfully, including 450 Storage unit tests, the Core/Server suites, recovery
and protocol integrations, SDK tests, and doctests. Final diff/whitespace and
local documentation links/test references were checked; no unrelated generated
files, database fixtures, or benchmark outputs are included.
