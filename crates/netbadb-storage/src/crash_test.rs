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
    RetirementBeforeCommit,
    RetirementBeforeLog,
    RetirementAfterLog,
    RetirementAfterPublish,
    RetirementAfterLeafUnlink,
    RetirementAfterParentUnlink,

    BTreeAfterInternalSplit,
    BTreeAfterSiblingUpdate,
    BTreeAfterParentUpdate,
    TransitionAfterLog,
    TransitionAfterPublish,
    TransitionAfterUndo,
    TailAfterCheckpoint,
    TailIntentAfterLogs,
    TailIntentDurable,
    TailAfterInvalidation,
    TailAfterSetLen,
    TailAfterFileSync,
    TailFinalizeAfterLogs,
    TailFinalizeAfterPagePublish,
    TailFinalizeDurable,
    TailAfterCompletion,

    PageGenerationReserved,
    RollbackAfterTrailingRemoval,
    GenerationReuseBeforeCommit,
    GenerationReuseAfterFlush,
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
    BTreeAfterUnaryNormalization,
    IndexBuildDuringBackfill,
    IndexBuildBeforeCatalogLog,
    IndexBuildAfterCatalogLog,
    IndexBuildAfterCatalogPublish,
    IndexDropBeforeCatalogLog,
    IndexDropAfterCatalogLog,
    IndexDropAfterWalDurable,
    IndexDropAfterCommit,
    AnalyzeAfterCatalogPublish,
    AnalyzeAfterCommit,
    RegisteredInsertAfterHeapPublish,
    RegisteredUpdateAfterHeapPublish,
    RegisteredDeleteAfterFirstIndexPublish,
    IndexCompactAfterLogs,
    IndexCompactAfterAllocation,
    IndexCompactBeforeRootPublish,
    IndexCompactAfterPagePublish,
    IndexCompactAfterPagesDurable,
    IndexCompactAfterCommit,
    WalPartialFinalRecord,
}

impl TestCrashPoint {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::RetirementBeforeCommit => "RetirementBeforeCommit",
            Self::RetirementBeforeLog => "RetirementBeforeLog",
            Self::RetirementAfterLog => "RetirementAfterLog",
            Self::RetirementAfterPublish => "RetirementAfterPublish",
            Self::RetirementAfterLeafUnlink => "RetirementAfterLeafUnlink",
            Self::RetirementAfterParentUnlink => "RetirementAfterParentUnlink",

            Self::BTreeAfterInternalSplit => "btree-after-internal-split",
            Self::BTreeAfterSiblingUpdate => "btree-after-sibling-update",
            Self::BTreeAfterParentUpdate => "btree-after-parent-update",
            Self::TransitionAfterLog => "transition-after-log",
            Self::TransitionAfterPublish => "transition-after-publish",
            Self::TransitionAfterUndo => "transition-after-undo",
            Self::TailAfterCheckpoint => "TailAfterCheckpoint",
            Self::TailIntentAfterLogs => "TailIntentAfterLogs",
            Self::TailIntentDurable => "TailIntentDurable",
            Self::TailAfterInvalidation => "TailAfterInvalidation",
            Self::TailAfterSetLen => "TailAfterSetLen",
            Self::TailAfterFileSync => "TailAfterFileSync",
            Self::TailFinalizeAfterPagePublish => "TailFinalizeAfterPagePublish",
            Self::TailFinalizeAfterLogs => "TailFinalizeAfterLogs",
            Self::TailFinalizeDurable => "TailFinalizeDurable",
            Self::TailAfterCompletion => "TailAfterCompletion",

            Self::RollbackAfterTrailingRemoval => "rollback-after-trailing-removal",
            Self::GenerationReuseBeforeCommit => "generation-reuse-before-commit",
            Self::GenerationReuseAfterFlush => "generation-reuse-after-flush",
            Self::PageGenerationReserved => "page-generation-reserved",
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
            Self::BTreeAfterUnaryNormalization => "btree-after-unary-normalization",
            Self::BTreeAfterFirstPagePublish => "btree-after-first-page-publish",
            Self::IndexBuildDuringBackfill => "index-build-during-backfill",
            Self::IndexBuildBeforeCatalogLog => "index-build-before-catalog-log",
            Self::IndexBuildAfterCatalogLog => "index-build-after-catalog-log",
            Self::IndexBuildAfterCatalogPublish => "index-build-after-catalog-publish",
            Self::IndexDropBeforeCatalogLog => "index-drop-before-catalog-log",
            Self::IndexDropAfterCatalogLog => "index-drop-after-catalog-log",
            Self::IndexDropAfterWalDurable => "index-drop-after-wal-durable",
            Self::IndexDropAfterCommit => "index-drop-after-commit",
            Self::AnalyzeAfterCatalogPublish => "analyze-after-catalog-publish",
            Self::AnalyzeAfterCommit => "analyze-after-commit",
            Self::RegisteredInsertAfterHeapPublish => "registered-insert-after-heap-publish",
            Self::RegisteredUpdateAfterHeapPublish => "registered-update-after-heap-publish",
            Self::RegisteredDeleteAfterFirstIndexPublish => {
                "registered-delete-after-first-index-publish"
            }
            Self::IndexCompactAfterLogs => "IndexCompactAfterLogs",
            Self::IndexCompactAfterAllocation => "IndexCompactAfterAllocation",
            Self::IndexCompactBeforeRootPublish => "IndexCompactBeforeRootPublish",
            Self::IndexCompactAfterPagePublish => "IndexCompactAfterPagePublish",
            Self::IndexCompactAfterPagesDurable => "IndexCompactAfterPagesDurable",
            Self::IndexCompactAfterCommit => "IndexCompactAfterCommit",
            Self::WalPartialFinalRecord => "wal-partial-final-record",
        }
    }
}

thread_local! { static SUPPRESSED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }

pub(crate) fn without_crash<T>(operation: impl FnOnce() -> T) -> T {
    struct Reset(bool);
    impl Drop for Reset {
        fn drop(&mut self) {
            SUPPRESSED.set(self.0);
        }
    }
    let _reset = Reset(SUPPRESSED.replace(true));
    operation()
}

pub(crate) fn is_enabled(point: TestCrashPoint) -> bool {
    !SUPPRESSED.get()
        && std::env::var_os(CHILD_ENV).as_deref() == Some(OsStr::new("1"))
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
