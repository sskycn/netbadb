//! Synchronous Heap transaction storage, page formats, WAL, recovery, and indexes.
mod allocation_transition;
mod btree;
mod buffer;
#[cfg(test)]
mod crash_test;
mod error;
mod heap;
mod inspection;
mod mvcc;
mod page;
mod recovery;
mod row_codec;
mod transaction;
mod txn_status;
mod wal;

pub(crate) use HeapStorageError as StorageError;
pub use btree::BTree;
pub use buffer::{BufferPool, DEFAULT_BUFFER_POOL_SIZE, ReadPageGuard};
pub(crate) use error::invalid_format;
pub use error::{BufferError, HeapStorageError, MetadataError, PageError};
pub use heap::{
    HeapIdentityInspection, HeapIndexBuildWriteBoundInspection, HeapRecoveryInspection,
    HeapStorage, HistoricalOrphanAdoptionReport, IndexMaintenanceReport, IndexPageAllocation,
    IndexReclaimReport, IndexTailReclaimReport, PageReuseClass, PageReuseInspection,
    ReusablePageInspection,
};
pub use inspection::{
    HeapPhysicalDesignSourceInspection, HeapResourceComponent, HeapResourceComponentKind,
    HeapRewriteIndex, HeapRewriteIndexes, heap_resource_components,
};
pub use mvcc::ReadView;
pub(crate) use netbadb_change_stream as change_stream;
pub use netbadb_change_stream::{
    ChangeBatch, ChangeReadResult, ChangeStorageKind, ChangeStreamCursor, ChangeStreamError,
    ChangeStreamGcStorageReport, ChangeStreamInspection, ChangeStreamMaintenanceInspection,
    ChangeStreamRetentionPin, ChangeStreamSourceInspection, StorageChange, StorageVersionKey,
    change_stream_guard_path, heap_change_log_path,
};
pub use netbadb_index::{IndexDefinition, IndexStatistics, TableStatistics};
pub use netbadb_row_codec::CodecError;
#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub use netbadb_storage_api::source_inspection_test_activity;
pub use netbadb_storage_api::{
    CheckpointError, IsolationLevel, PreparedDecision, PreparedRuntimeInspection,
    PreparedTransaction, PreparedTransactionState, PreparedTxnResolution, PresenceCountSummary,
    Snapshot, StorageAccessCostHints, StorageKind, StorageVisibilityBoundary, TransactionError,
    TransactionState,
};
#[cfg(test)]
pub(crate) use netbadb_types::SlotId;
pub use page::{
    PAGE_FORMAT_VERSION, PAGE_HEADER_SIZE, PAGE_MAGIC, PAGE_SIZE, Page, PageHeader, PageManager,
    PageType, SLOT_SIZE, Slot, SlotRef, SlotState,
};
pub use recovery::RecoveryError;
pub use transaction::Transaction;
pub use txn_status::{TxnStatus, TxnStatusError, txn_status_path};
pub use wal::{
    WAL_FORMAT_VERSION, WAL_HEADER_SIZE, WAL_MAX_RECORD_SIZE, WalError, WalManager, WalRecord,
    WalRecordKind, wal_alternate_path, wal_path,
};
/// Thread-local production-path instrumentation for deterministic Index writer
/// bound tests. It is absent unless tests or the explicit test-hooks feature
/// are enabled and never participates in sizing or mutation decisions.
#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub mod index_write_bound_test_activity {
    use std::cell::Cell;

    use crate::{TxnStatus, WalRecordKind};

    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
    pub struct Activity {
        pub published_page_images: u64,
        pub wal_page_image_records: u64,
        pub wal_page_update_records: u64,
        pub wal_page_transition_records: u64,
        pub generation_reservation_records: u64,
        pub begin_records: u64,
        pub prepare_records: u64,
        pub commit_records: u64,
        pub abort_records: u64,
        pub rollback_complete_records: u64,
        pub wal_appended_bytes: u64,
        pub committed_txn_status_records: u64,
        pub txn_status_appended_bytes: u64,
        pub staged_change_stream_rows: u64,
    }

    thread_local! { static ACTIVITY: Cell<Activity> = Cell::new(Activity::default()); }

    pub fn take() -> Activity {
        ACTIVITY.with(|value| value.replace(Activity::default()))
    }

    fn record(update: impl FnOnce(&mut Activity)) {
        ACTIVITY.with(|value| {
            let mut current = value.get();
            update(&mut current);
            value.set(current);
        });
    }

    pub(crate) fn record_wal_append(kind: &WalRecordKind, bytes: usize) {
        let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
        record(|activity| {
            activity.wal_appended_bytes = activity.wal_appended_bytes.saturating_add(bytes);
            match kind {
                WalRecordKind::Begin => activity.begin_records += 1,
                WalRecordKind::PageGenerationReservation => {
                    activity.generation_reservation_records += 1;
                }
                WalRecordKind::PageUpdate { .. } => {
                    activity.wal_page_image_records += 1;
                    activity.wal_page_update_records += 1;
                }
                WalRecordKind::PageAllocationTransition { .. } => {
                    activity.wal_page_image_records += 1;
                    activity.wal_page_transition_records += 1;
                }
                WalRecordKind::Commit => activity.commit_records += 1,
                WalRecordKind::Abort => activity.abort_records += 1,
                WalRecordKind::RollbackComplete => activity.rollback_complete_records += 1,
                WalRecordKind::Prepare { .. } => activity.prepare_records += 1,
            }
        });
    }

    pub(crate) fn record_txn_status_append(status: TxnStatus, bytes: usize) {
        let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
        record(|activity| {
            activity.txn_status_appended_bytes =
                activity.txn_status_appended_bytes.saturating_add(bytes);
            if matches!(status, TxnStatus::Committed(_)) {
                activity.committed_txn_status_records += 1;
            }
        });
    }

    pub(crate) fn record_published_page_images(count: usize) {
        let count = u64::try_from(count).unwrap_or(u64::MAX);
        record(|activity| {
            activity.published_page_images = activity.published_page_images.saturating_add(count);
        });
    }

    pub(crate) fn record_staged_change_stream_rows(count: usize) {
        let count = u64::try_from(count).unwrap_or(u64::MAX);
        record(|activity| {
            activity.staged_change_stream_rows =
                activity.staged_change_stream_rows.saturating_add(count);
        });
    }
}
