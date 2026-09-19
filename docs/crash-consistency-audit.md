# Crash consistency and recovery audit

> Historical audit. PostgreSQL compatibility test counts below remain factual
> for this audit's baseline; that frontend was removed later.

Audit baseline: `3a9631bba960d4e79aa4cbf92dd708ade3a8fc63` (2026-09-18).
This audit changes recovery and I/O failure handling, without changing persistent
versions, the synchronous core, mutation ownership, or planner policy. The
[error/concurrency audit](error-concurrency-audit.md) remains a regression contract.

## 1. Summary

Six confirmed P1 findings were fixed; no P0 or P2 finding and no confirmed finding
was deferred. Four are demonstrated by erroneous recovery/retry results. Two are
missing filesystem durability barriers, demonstrated by fault-injection tests
that must prevent publication or reclamation when the required sync fails.
Those tests establish syscall ordering and error propagation, not physical
power-loss behavior.

Added 16 test functions: 15 regression tests and one subprocess entry point. Two
new subprocess matrices perform six `process::exit(86)` interruptions, including
interruptions inside recovery. Two existing LSM/mixed-storage matrices now open
three times to detect a terminal record incorrectly appended by the second open.

## 2. Durability invariants and state machines

| Subsystem | Required ordering / authority |
| --- | --- |
| Heap commit | PageUpdate WAL → Commit → WAL sync → transaction-status append/sync → publish committed status/release writer. Dirty pages may remain buffered (NO-FORCE). |
| Heap writeback | WAL through pageLSN durable before writing the corresponding page; validate checksum, identity and generation before trusting pageLSN. |
| Heap rollback | Abort/sync → reverse undo → page/truncation sync → RollbackComplete/sync → Aborted status. A repeated recovery skips completed undo. |
| Status sidecar | Full records are checksummed and conflicting outcomes fail closed. An incomplete last record is removable only when it matches a terminal record retained in validated WAL. Recovery republishes that terminal decision before exposing snapshots. |
| Checkpoint | No outstanding transactions/writer → sync WAL → flush/sync pages → sync new generation header and directory → switch → delete old generation. Reopen synchronizes selected authority before reclaiming the other slot. |
| Coordinator | Durable participant Prepare → canonical CommitDecision/sync (irreversible) → participant Commit/sync → Complete. Missing Complete never revokes a decision. Sequenced data Complete may use the existing deferred checkpoint policy. |
| LSM commit | Canonical MutationBatch → Commit or Prepare → WAL sync → visibility publication. Failed partial append must be truncated before reuse of the append offset. |
| LSM flush/compaction | Sync replacement SSTables/new WAL → sync MANIFEST.next → rename → directory sync → install candidate → remove obsolete inputs. Reopen validates the selected graph and synchronizes its authority before orphan deletion. |
| Partition mutation | Durable immutable routing/StorageId catalog; source removal and destination insertion share one database transaction and exact coordinator participant set. |
| Schema/projection | Pending identity/intent is durable before artifacts become eligible for publication; an artifact alone does not establish catalog authority. |

Successful `write_all` does not establish durability. A readable file after a
process exit does not establish that the prior `sync` or directory publication
completed. Failed sync of a decision remains uncertain: callers must retry or
reopen, never reinterpret it as authorization to roll back a possible winner.

## 3. Findings

| Severity | Subsystem | Crash Window | Wrong Outcome | Root Cause | Fix | Regression |
| --- | --- | --- | --- | --- | --- | --- |
| P1 / CC-01 | Heap transaction status | Partial TXST append after durable Commit/RollbackComplete | Retry could succeed with an invalid log; crash reopen rejected a recoverable tail | Append had no cleanup; open rejected every partial record before consulting WAL | Truncate failed append, poison on cleanup failure; WAL-matched tail recovery; resync complete status prefix | `crash_audit_status_partial_append_retry_reopens`, `crash_audit_partial_status_publication_recovers_wal_decision`, status-tail and double-failure tests |
| P1 / CC-02 | LSM WAL | Partial long append, then rollback and shorter successful transaction | Stale bytes remained after the new record; reopen rejected WAL | Logical append offset stayed unchanged but physical suffix was not removed | Truncate on failure; preserve append and cleanup errors; block writers/maintenance after double failure | `crash_audit_partial_append_can_retry_a_shorter_record`, `crash_audit_wal_double_failure_requires_reopen` |
| P1 / CC-03 | LSM prepared recovery | Prepare followed by durable Abort, then reopen | Standalone open demanded another resolution; coordinated open appended duplicate Abort, making later recovery invalid | Recovery checked Prepare before recognizing terminal Abort | Recognize Abort as terminal; never append another terminal record | `crash_audit_prepared_abort_is_terminal_on_every_reopen`, recovery-exit matrix, third-open mixed/group matrices |
| P1 / CC-04 | Heap prepared recovery | Valid Commit resolution plus an unknown or already-aborting participant | Rejected request nevertheless durably committed the valid participant | Validation and WAL append were interleaved | Validate the whole set before writing; emit accepted Commit records in deterministic physical order | `crash_audit_invalid_resolution_set_does_not_commit_a_valid_prefix` and `crash_audit_abort_started_resolution_cannot_commit_other_participants` check unchanged WAL and successful subsequent abort/reopen |
| P1 / CC-05 | Reopened WAL/Manifest/Coordinator authority | Earlier creation/rename sync did not complete | Required sync fault was skipped: open could return writable authority or delete old inputs | Readable selected file treated as sufficient publication proof | Synchronize selected file and directory before granting authority/reclaiming inputs; resync LSM WAL before using terminal records | WAL-generation, LSM-orphan and coordinator-compaction recovery barrier tests |
| P1 / CC-06 | Initial LSM / Partition Catalog creation | File sync completed but containing directory entry not synced | Creation returned success without the necessary parent-directory barrier | LSM synced its own directory but not its parent; Partition Catalog relied on later independent coordinator creation | Sync each newly published object's own parent; propagate failure | LSM and Partition Catalog creation-parent-sync tests |

The functional counterexamples failed before their respective fixes. Barrier
regressions also failed when only the new barriers were removed, and passed with
those barriers restored. The power-loss consequences of a skipped directory
barrier are an ordering argument; no real power outage was induced.

## 4. Coordinator audit

Reviewed create/open, canonical decision validation, decision append/sync,
Complete and deferred Complete, partial-tail recovery, compaction/reopen, and
Core participant resolution. A fully checksummed but semantically invalid record
is rejected; a structurally valid incomplete final record can be truncated.
Every decision's StorageId/physical TxnId/database identity must match.

Prepare never publishes a global commit. A durable decision commits all exact
participants even when some local commits or Complete are absent. Without a
decision, in-doubt participants abort. Missing participants and contradictory
terminal state remain errors. Pure data deferred Complete does not weaken the
already durable decision.

The earlier append-plus-truncate double-failure contract is preserved: the
writer loses authority, including the deferred Complete path, until reopen.
The added compaction test fails directory sync during compaction, fails it again
during reopen, verifies no writable object is returned, then reopens, appends a
new sequenced commit, and checks two further opens.

## 5. WAL / checkpoint audit

Heap WAL framing validates bounded length, checksum, prevLSN, transaction state,
page images and generation reservations. Only structurally valid incomplete
last records are crash tails. Complete checksum failures remain corruption.
Logical LSNs do not reset when the two physical generation slots rotate.

Checkpoint requires quiescence and persists pages before selecting a fresh WAL.
New generation file and directory barriers precede old-generation deletion.
Startup now repeats those barriers, including for a header-only generation;
file-sync and directory-sync faults both preserve the old slot. Existing
checkpoint/process-crash tests exercise new-header publication and old-file
removal boundaries. Ordinary WAL append already truncated failed writes and
poisoned the handle when cleanup failed; its existing behavior is retained.

Heap recovery validates the entire resolution set before durable Commit append.
An Abort without RollbackComplete is still undergoing physical undo, but already
forbids a Commit decision; it is checked before any other member is committed.
It then redoes history and undoes losers in descending global LSN order, syncing
physical undo before RollbackComplete. The new status-tail subprocess test exits
once during foreground status publication and again during recovery's status
publication, then verifies the exact row state on two normal opens.

## 6. Heap / index audit

Reviewed insert/update/delete, relocated tuples, page allocation/reuse, rollback,
B-tree mutation ordering, catalog publication and recovery. RowId generation
checks prevent a stale slot identity from referring to a reused tuple. Relocation
logs source and destination mutations in the same transaction. Page checksums
and generation identity are validated before pageLSN can suppress redo.

Heap rows, B-tree nodes, root/parent transitions and index catalog changes use the
same physical transaction/WAL. Existing crash tests cover indexed DML, relocation,
split/merge, root/catalog changes, rollback interruption and page retirement.
Uncommitted index state must not become planner-visible. No index format or
ordering change was necessary in this audit.

## 7. LSM audit

Reviewed batch encoding, Prepare/Commit/Abort, reopen classification, flush,
SSTable checksums and Bloom validation, manifest candidate publication,
compaction overlap closure, tombstones and obsolete-file deletion.

Failed appends now restore the physical end before retry. If truncation also
fails, both errors are retained and all new mutation/maintenance attempts are
blocked until reopen. Complete valid records are resynchronized during open.
Prepared Abort is terminal and idempotent. The new recovery subprocess matrix
starts with an SSTable row, prepares its deletion plus a replacement row, exits
after recovery's Commit or Abort sync, and verifies exact rows/tombstones on
three further opens without checkpointing away the evidence.

Manifest selects the only live graph. Open validates referenced SSTables, WAL,
rows and resolution identities before deleting orphans. It synchronizes the
selected manifest and root directory first. Failed barriers preserve old WAL,
MANIFEST.next and unreferenced SSTables. Compaction still retains historical
versions/tombstones under its existing closure/quiescence rules; no GC policy or
performance threshold changed.

## 8. Partition audit

Reviewed immutable catalog validation/creation, typed half-open range routing,
persisted StorageId resolution, key-changing row movement, multi-partition
insert/delete and exact multi-storage recovery. Catalog paths and coordinator
paths may have different parents, so each publication must sync its own parent.
The new catalog regression injects failure at that boundary.

Existing subprocess matrices check insert/delete/key movement before the global
decision, after durable decision, and after a single participant commit. Reopen
reverses input path order and checks exact rows. Mixed Heap/LSM recovery now uses
three opens so a duplicated abort cannot hide behind a successful second open.
No dynamic partition catalog mutation or general multiwriter support was added.

## 9. Filesystem durability audit

Inspected paths include Heap page/WAL/status files; WAL generation recycling;
LSM root/SST directory, WAL, SSTables, MANIFEST.next and MANIFEST; coordinator
append/compaction; Partition Catalog; NBSC snapshot/installation marker; NBSJ
atomic snapshots; Projection Catalog pending/final publication and artifacts.

`write_all` handles ordinary short writes but can still return an error after a
prefix was written. Heap status and LSM WAL now remove that prefix on error.
Successful cleanup permits retry at the same boundary; failed cleanup revokes
writer authority. A crash before the cleanup itself becomes durable is handled
by startup's validated-tail rules. Sync failures never publish an in-memory
committed result merely because complete bytes can be read back.

Rename publication uses a synchronized candidate followed by parent-directory
sync. New LSM roots also sync their parent, independently of their own contents.
Partition catalogs sync their own parent independently of coordinator placement.
Reopened authority repeats missing file/directory barriers before use or
reclamation. Temporary files do not independently confer catalog authority;
malformed referenced artifacts fail closed. Successful garbage deletion is not
required to remove every orphan durably: an orphan reappearing after a crash is
safe because it is never selected without authoritative metadata.

## 10. Crash / failure test matrix

| Operation | Injection point | Expected | Actual |
| --- | --- | --- | --- |
| Heap commit status | 17 bytes of TXST written after durable Commit; exit again during recovery | Commit remains visible; recovery converges | Exact `after` row on two subsequent opens |
| Heap rollback status | 17 bytes after durable RollbackComplete; exit again during recovery | Before-image remains visible | Exact `before` row on two subsequent opens |
| Status append retry | Partial append error | Remove prefix, retry, valid log | Committed status survives two opens |
| Status double failure | Partial write + truncate error | Preserve both errors, no new WAL/status mutations | Existing/new transaction and checkpoint/close gates reject; WAL-based reopen succeeds |
| Status negative inputs | All 31 incomplete lengths; missing WAL, mismatched prefix, corrupt complete record | Repair only matching tail; corruption unchanged | All assertions pass |
| LSM append retry | 2048 bytes of a large batch then error; shorter later transaction | No stale suffix; only successful row | Exact winner row on two opens |
| LSM double failure | Partial batch + truncate failure | No writer/maintenance authority until reopen | Gates reject; only previous durable row survives |
| LSM terminal Abort | Prepared abort followed by alternating ordinary/coordinated open | No new resolution or Abort record | Four opens, WAL byte-for-byte unchanged |
| LSM recovery | Exit immediately after recovery Commit/Abort sync | Terminal decision and tombstone semantics persist | Exact row set on three opens, WAL unchanged |
| Heap resolution validation | Valid Commit plus unknown/already-aborting participant | Reject before any Commit append | WAL unchanged; retry Abort and two opens restore original page |
| WAL recovery authority | Selected generation file sync / parent sync fail | Preserve previous generation | Both slots retained; successful retry and two opens select generation 2 |
| LSM recovery authority | Manifest file sync / root sync fail after uncertain rename | Preserve all old inputs and candidates | Old WAL and orphan files unchanged; retry recovers exact row |
| Coordinator recovery authority | Directory sync fails after uncertain compaction rename | No append authority returned | Rejected open; retry accepts next sequence and survives two opens |
| Initial publication | LSM parent sync / catalog parent sync fail | No successful create acknowledgment | Typed error; clean retry and two opens succeed |
| Existing multi-storage / partition matrices | Prepare, decision, participant commit, Complete boundaries | All-or-nothing exact row state | Included in final workspace validation |
| Existing Heap/index/LSM maintenance matrices | Page/index/redo/undo, SST/manifest/compaction/checkpoint boundaries | Exact winner/loser state; no lost input before publication | Included in final workspace validation |

## 11. Remaining risks and explicit limits

- Confirmed defects: none intentionally deferred from the six findings above.
- Untested platform properties: real power interruption, device/controller cache
  loss, reordered or torn hardware writes, and the behavior of other filesystems.
  Subprocess exit bypasses Rust destructors but leaves the OS and its page cache
  running; it is not a power-loss simulator.
- Filesystem assumptions: successful file/directory sync supplies the durability
  promised by the local OS/filesystem; rename is atomic within the containing
  filesystem. NFS/network filesystems and controllers that misreport flush
  completion were not validated. No new portability claim is made.
- Corruption policy: complete checksum failures and contradictory durable
  identities fail closed. Checksums detect damage; they do not generally repair
  torn data pages. A retained, matching WAL terminal record is required for
  partial transaction-status repair; unexplained archived-history truncation
  remains an error.
- Architecture limits: synchronous, single-process physical mutation ownership;
  no distributed consensus or expanded writer concurrency. Caller acknowledgment
  may be uncertain after a commit sync error and still requires retry/reopen.
- Roadmap: broader filesystem/power-cut testing, hardware fault campaigns and
  repair/backup designs are separate work. No such capability is marked complete.

## 12. Validation

The unmodified baseline passed all four required commands:

```sh
cargo fmt --all -- --check
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Targeted functional and barrier counterexamples were run and failed as expected
without the corresponding fixes. Test-fixture compilation errors and two Clippy
style diagnostics found during iteration were corrected before final validation. An intermediate workspace run
was deliberately interrupted when the second review found the already-aborting
participant case in CC-04; that run is not counted as a full pass. The fixed
`cargo test -p netbadb-storage -p netbadb-core crash_audit_ -- --nocapture` matrix
passed on the final code: Core 2 tests and Storage 14 tests (including the
subprocess entry point). The final workspace results are recorded below.

No protocol/server acknowledgment shape or SDK executable code changed, so Go
SDK tests are outside this change's validation boundary. Workspace Rust tests
include the affected Core/Storage consumers and previous error-audit regressions.

| Command | Baseline | Final |
| --- | --- | --- |
| `cargo fmt --all -- --check` | Passed | Passed |
| `cargo check --workspace --all-targets` | Passed | Passed |
| `cargo clippy --workspace --all-targets -- -D warnings` | Passed | Passed |
| `cargo test --workspace` | Passed | Passed; 0 failures |
| `cargo test -p netbadb-storage -p netbadb-core crash_audit_ -- --nocapture` | New tests | Passed: 16 test functions |
| `git diff --check` | Clean | Passed |
| Added Markdown link/path verification | Not applicable | Passed |

Counterexample commands additionally included
`cargo test -p netbadb-storage crash_audit_ -- --nocapture`,
`cargo test -p netbadb-storage crash_audit_prepared_abort -- --nocapture`, and
`cargo test -p netbadb-storage crash_audit_abort_started_resolution -- --nocapture`.
The same audit selector was run after temporarily removing only the newly added
filesystem barriers: two Core and three Storage barrier regressions failed as
expected; fixed files were restored before the passing final validation.

Final workspace execution completed successfully in 1,314.3 seconds. Selected
suite totals: Core 662 passed / 3 existing ignored, Executor 97 passed, Server
261 unit + 11 PostgreSQL integration + 26 TCP integration passed, and Storage
464 passed. The three existing ignores are the manual ALTER TYPE cost probe
and two explicit fuzz-corpus generators; no ignore or blanket lint suppression
was added. All 16 new test functions also passed in the complete workspace run.
Final source digests were checked unchanged throughout that run, and the final
code/documentation diff was reviewed before integration.
