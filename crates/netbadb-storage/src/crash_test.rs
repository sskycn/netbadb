//! Test-only abrupt-process termination hooks.
//!
//! These hooks are compiled only into the storage unit-test executable. They
//! model loss of the database process without running Rust destructors; they do
//! not model kernel, machine, or storage-device power loss.

use std::ffi::OsStr;

pub(crate) const CHILD_ENV: &str = "NETBADB_TEST_CRASH_CHILD";
pub(crate) const CASE_ENV: &str = "NETBADB_TEST_CRASH_CASE";
pub(crate) const DATABASE_PATH_ENV: &str = "NETBADB_TEST_DB_PATH";
const POINT_ENV: &str = "NETBADB_TEST_CRASH_POINT";
pub(crate) const EXIT_CODE: i32 = 86;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TestCrashPoint {
    ActiveWriterAfterDurablePageFlush,
    CommittedWithoutDataFlush,
    CommitAfterAppend,
    CommitAfterWalSync,
    RollbackAfterAbortAppend,
    RollbackAfterAbortSync,
    RollbackAfterPageUndo,
    RollbackAfterCompleteAppend,
    RollbackAfterCompleteSync,
    RecoveryAfterPageOperation,
    CheckpointAfterNewGenerationDurable,
    CheckpointAfterOldGenerationRemoved,
    RelocationAfterFirstPageUpdateLog,
    RelocationAfterBothPageUpdateLogs,
    RelocationAfterFirstPagePublish,
    BTreeAfterFirstPageUpdateLog,
    BTreeAfterFirstPagePublish,
    IndexBuildBeforeCatalogLog,
    IndexBuildAfterCatalogLog,
    IndexBuildAfterCatalogPublish,
    WalPartialFinalRecord,
}

impl TestCrashPoint {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::ActiveWriterAfterDurablePageFlush => "active-writer-after-durable-page-flush",
            Self::CommittedWithoutDataFlush => "committed-without-data-flush",
            Self::CommitAfterAppend => "commit-after-append",
            Self::CommitAfterWalSync => "commit-after-wal-sync",
            Self::RollbackAfterAbortAppend => "rollback-after-abort-append",
            Self::RollbackAfterAbortSync => "rollback-after-abort-sync",
            Self::RollbackAfterPageUndo => "rollback-after-page-undo",
            Self::RollbackAfterCompleteAppend => "rollback-after-complete-append",
            Self::RollbackAfterCompleteSync => "rollback-after-complete-sync",
            Self::RecoveryAfterPageOperation => "recovery-after-page-operation",
            Self::CheckpointAfterNewGenerationDurable => "checkpoint-after-new-generation-durable",
            Self::CheckpointAfterOldGenerationRemoved => "checkpoint-after-old-generation-removed",
            Self::RelocationAfterFirstPageUpdateLog => "relocation-after-first-page-update-log",
            Self::RelocationAfterBothPageUpdateLogs => "relocation-after-both-page-update-logs",
            Self::RelocationAfterFirstPagePublish => "relocation-after-first-page-publish",
            Self::BTreeAfterFirstPageUpdateLog => "btree-after-first-page-update-log",
            Self::BTreeAfterFirstPagePublish => "btree-after-first-page-publish",
            Self::IndexBuildBeforeCatalogLog => "index-build-before-catalog-log",
            Self::IndexBuildAfterCatalogLog => "index-build-after-catalog-log",
            Self::IndexBuildAfterCatalogPublish => "index-build-after-catalog-publish",
            Self::WalPartialFinalRecord => "wal-partial-final-record",
        }
    }
}

pub(crate) fn is_enabled(point: TestCrashPoint) -> bool {
    std::env::var_os(CHILD_ENV).as_deref() == Some(OsStr::new("1"))
        && std::env::var_os(POINT_ENV).as_deref() == Some(OsStr::new(point.as_str()))
}

pub(crate) fn maybe_crash(point: TestCrashPoint) {
    if is_enabled(point) {
        crash_now();
    }
}

pub(crate) fn crash_now() -> ! {
    // `process::exit` does not unwind or run Rust destructors, so live
    // HeapStorage and Transaction values cannot flush or repair state.
    std::process::exit(EXIT_CODE);
}

pub(crate) fn configure_child(
    command: &mut std::process::Command,
    case: &str,
    path: &std::path::Path,
    point: TestCrashPoint,
) {
    command
        .env(CHILD_ENV, "1")
        .env(CASE_ENV, case)
        .env(DATABASE_PATH_ENV, path)
        .env(POINT_ENV, point.as_str());
}
