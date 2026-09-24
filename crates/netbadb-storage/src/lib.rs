//! Synchronous page, buffer, and heap storage for the embedded vertical slice.

mod table;

pub use netbadb_columnar::{
    ColumnarBaseArtifactMode, ColumnarBaseArtifactWriteBoundInspection, ColumnarBatch,
    ColumnarBatchColumn, ColumnarColumnSpec, ColumnarColumnStatistics, ColumnarConstraint,
    ColumnarDeltaSegmentMetadata, ColumnarError, ColumnarIncrementalMetadata, ColumnarProjection,
    ColumnarProjectionMetadata, ColumnarRepresentationStatistics, ColumnarRowGroupStatistics,
    ColumnarScanStatistics, ColumnarVector, PreparedColumnarAdvance, PreparedColumnarProjection,
    StorageSnapshotToken, cleanup_unpublished_projection_build,
    columnar_projection_manifest_exists,
};
pub use netbadb_heap::{
    BTree, BufferPool, DEFAULT_BUFFER_POOL_SIZE, HeapIdentityInspection,
    HeapIndexBuildWriteBoundInspection, HeapPhysicalDesignSourceInspection, HeapRecoveryInspection,
    HeapResourceComponent, HeapResourceComponentKind, HeapRewriteIndex, HeapRewriteIndexes,
    HeapStorage, HistoricalOrphanAdoptionReport, IndexMaintenanceReport, IndexPageAllocation,
    IndexReclaimReport, IndexTailReclaimReport, PAGE_FORMAT_VERSION, PAGE_HEADER_SIZE, PAGE_MAGIC,
    PAGE_SIZE, Page, PageHeader, PageManager, PageReuseClass, PageReuseInspection, PageType,
    PresenceCountSummary, ReadPageGuard, ReadView, RecoveryError, ReusablePageInspection,
    SLOT_SIZE, Slot, SlotRef, SlotState, Transaction, TxnStatus, TxnStatusError,
    WAL_FORMAT_VERSION, WAL_HEADER_SIZE, WAL_MAX_RECORD_SIZE, WalError, WalManager, WalRecord,
    WalRecordKind, heap_resource_components, txn_status_path, wal_alternate_path, wal_path,
};

/// Computes the initial Columnar artifact bound from a fresh authoritative
/// source inspection, preserving the historical facade API.
pub fn inspect_columnar_base_artifact_write_bound(
    table: &netbadb_schema::TableDef,
    source: StoragePhysicalDesignSourceInspection,
    columns: &[netbadb_types::ColumnId],
    mode: ColumnarBaseArtifactMode,
) -> Result<ColumnarBaseArtifactWriteBoundInspection, StorageError> {
    let footprint = match source {
        StoragePhysicalDesignSourceInspection::Heap(heap) => {
            netbadb_columnar::ColumnarSourceFootprint {
                row_upper_bound: heap.row_upper_bound,
                scalar_payload_bytes_upper_bound: heap.main_file_bytes_upper_bound,
            }
        }
        StoragePhysicalDesignSourceInspection::Lsm(lsm) => {
            let scalar_payload_bytes_upper_bound = match mode {
                ColumnarBaseArtifactMode::Snapshot => lsm
                    .prospective_snapshot_sstable_bytes_upper_bound()
                    .map_err(StorageError::from)?,
                ColumnarBaseArtifactMode::Incremental => lsm
                    .total_sstable_bytes
                    .checked_add(lsm.memtable_bytes)
                    .ok_or(StorageError::ResourceBoundOverflow {
                        resource: "Incremental LSM scalar payload bytes",
                    })?,
            };
            netbadb_columnar::ColumnarSourceFootprint {
                row_upper_bound: lsm.row_upper_bound().map_err(StorageError::from)?,
                scalar_payload_bytes_upper_bound,
            }
        }
    };
    netbadb_columnar::inspect_columnar_base_artifact_write_bound(table, footprint, columns, mode)
        .map_err(Into::into)
}
pub use netbadb_change_stream::{
    CHANGE_LOG_FORMAT_VERSION, CHANGE_LOG_MAGIC, CHANGE_LOG_MAX_MUTATIONS,
    CHANGE_LOG_MAX_RECORD_BYTES, CHANGE_LOG_MAX_ROW_BYTES, ChangeBatch, ChangeBatchInspection,
    ChangeBatchMaintenanceInspection, ChangeReadResult, ChangeStorageKind, ChangeStreamCursor,
    ChangeStreamError, ChangeStreamGcStorageReport, ChangeStreamInspection,
    ChangeStreamMaintenanceInspection, ChangeStreamRetentionPin,
    ChangeStreamRetentionPinInspection, ChangeStreamSourceInspection, ChangeStreamStatus,
    StorageChange, StorageVersionKey, change_stream_guard_path, heap_change_log_path,
    lsm_change_log_path, validate_change_log_file,
};
pub use netbadb_index::{IndexDefinition, IndexStatistics, TableStatistics};
pub(crate) use netbadb_lsm::LsmRowHandle;
pub use netbadb_lsm::{
    DEFAULT_LSM_MEMTABLE_FLUSH_BYTES, LSM_MANIFEST_FORMAT_VERSION, LSM_MAX_LEVELS,
    LSM_MAX_PENDING_MUTATIONS, LSM_MAX_PENDING_TRANSACTION_BYTES, LSM_SSTABLE_FORMAT_VERSION,
    LSM_WAL_FORMAT_VERSION, LsmCompactionPlanInspection, LsmError, LsmIdentityInspection,
    LsmInspection, LsmLevelInspection, LsmMaintenanceAnchor, LsmMaintenanceBoundInspection,
    LsmMaintenanceCostInspection, LsmMaintenanceInspection, LsmMaintenanceSafetyBlocker,
    LsmPhysicalDesignSourceInspection, LsmReadAmplification, LsmReadView, LsmRecoveryInspection,
    LsmStorage, LsmTransaction, LsmWriteAmplification, fuzz_lsm_manifest_bytes,
    fuzz_lsm_sstable_block_bytes, fuzz_lsm_wal_bytes,
};
pub use netbadb_row_codec::CodecError;
#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub use netbadb_storage_api::source_inspection_test_activity;
pub use netbadb_storage_api::{
    AccessPathCapabilities, CheckpointError, IsolationLevel, PreparedDecision,
    PreparedRuntimeInspection, PreparedTransaction, PreparedTransactionState,
    PreparedTxnResolution, Snapshot, StorageAccessCostHints, StorageAccessPath, StorageKind,
    StorageVisibilityBoundary, TransactionError, TransactionState,
};
pub use table::{
    CommittedReadAnchor, StorageChangeFinalizeBatchReport, StorageChangePrepareBatchReport,
    StorageCommitBatchReport, StoragePhysicalDesignSourceInspection, StoragePrepareBatchReport,
    StorageReadView, StorageRowHandle, StorageTransaction, StorageVisibilityPin, TableStorage,
};

#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub use netbadb_heap::index_write_bound_test_activity;

use std::error::Error;
use std::fmt;

use netbadb_index::IndexError;
use netbadb_schema::{SchemaError, SchemaFingerprint};
use netbadb_types::{AccessPathId, PageId, PhysicalType, TableId};

pub use netbadb_heap::{BufferError, MetadataError, PageError};
#[derive(Debug)]
pub enum StorageError {
    Io(std::io::Error),
    Schema(SchemaError),
    InvalidFormat(String),
    Page(PageError),
    Buffer(BufferError),
    Codec(CodecError),
    Metadata(MetadataError),
    Index(IndexError),
    Recovery(RecoveryError),
    Wal(WalError),
    Transaction(TransactionError),
    TxnStatus(TxnStatusError),
    Checkpoint(CheckpointError),
    Lsm(LsmError),
    Columnar(ColumnarError),
    ChangeStream(ChangeStreamError),
    TableIdMismatch {
        expected: TableId,
        actual: TableId,
    },
    SchemaMismatch {
        expected: SchemaFingerprint,
        actual: SchemaFingerprint,
    },
    InvalidRowLength {
        expected: usize,
        actual: usize,
    },
    TypeMismatch {
        column: String,
        expected: PhysicalType,
        actual: Option<PhysicalType>,
    },
    NullNotAllowed {
        column: String,
    },
    UnknownColumn {
        column_id: netbadb_types::ColumnId,
    },
    CountOverflow,
    ResourceBoundOverflow {
        resource: &'static str,
    },
    RowNotFound {
        row_id: netbadb_types::RowId,
    },
    RowDeleted {
        row_id: netbadb_types::RowId,
    },
    StaleRowId {
        row_id: netbadb_types::RowId,
        actual_generation: u32,
    },
    RowTooLarge {
        size: usize,
        capacity: usize,
    },
    PageOffsetOverflow {
        page_id: PageId,
    },
    InvalidMvccHeader(&'static str),
    UnsupportedTupleVersion(u16),
    StorageContextMismatch {
        expected: TableId,
        actual: TableId,
    },
    InvalidVisibilityBoundary {
        storage_id: netbadb_types::StorageId,
        value: u64,
    },
    VisibilityBoundaryExhausted {
        storage_id: netbadb_types::StorageId,
    },
    VisibilityBoundaryContextMismatch {
        expected_storage_id: netbadb_types::StorageId,
        actual_storage_id: netbadb_types::StorageId,
        expected_kind: crate::StorageKind,
        actual_kind: crate::StorageKind,
    },
    FutureVisibilityBoundary {
        storage_id: netbadb_types::StorageId,
        requested: u64,
        current: u64,
    },
    UnknownAccessPath {
        table_id: TableId,
        access_path: AccessPathId,
    },
    ResourceLimit {
        resource: &'static str,
        limit: u64,
    },
    UnsupportedOperation {
        operation: &'static str,
        storage_kind: &'static str,
    },
}

impl fmt::Display for StorageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "storage I/O error: {error}"),
            Self::Schema(error) => write!(formatter, "schema error: {error}"),
            Self::InvalidFormat(message) => write!(formatter, "invalid database format: {message}"),
            Self::Page(error) => write!(formatter, "page error: {error}"),
            Self::Buffer(error) => write!(formatter, "buffer pool error: {error}"),
            Self::Codec(error) => write!(formatter, "row codec error: {error}"),
            Self::Metadata(error) => write!(formatter, "heap metadata error: {error}"),
            Self::Index(error) => write!(formatter, "index error: {error}"),
            Self::Recovery(error) => write!(formatter, "recovery error: {error}"),
            Self::Wal(error) => write!(formatter, "write-ahead log error: {error}"),
            Self::Transaction(error) => write!(formatter, "transaction error: {error}"),
            Self::TxnStatus(error) => write!(formatter, "transaction-status error: {error}"),
            Self::Checkpoint(error) => write!(formatter, "checkpoint error: {error}"),
            Self::Lsm(error) => write!(formatter, "LSM error: {error}"),
            Self::Columnar(error) => error.fmt(formatter),
            Self::ChangeStream(error) => error.fmt(formatter),
            Self::TableIdMismatch { expected, actual } => write!(
                formatter,
                "table ID mismatch: expected {}, found {}",
                expected.0, actual.0
            ),
            Self::SchemaMismatch { expected, actual } => write!(
                formatter,
                "schema fingerprint mismatch: expected {expected}, found {actual}"
            ),
            Self::InvalidRowLength { expected, actual } => {
                write!(formatter, "expected {expected} row values, found {actual}")
            }
            Self::TypeMismatch {
                column,
                expected,
                actual,
            } => write!(
                formatter,
                "column `{column}` expects {expected}, found {actual:?}"
            ),
            Self::NullNotAllowed { column } => {
                write!(formatter, "column `{column}` is not nullable")
            }
            Self::UnknownColumn { column_id } => {
                write!(formatter, "table has no column with ID {}", column_id.0)
            }
            Self::CountOverflow => formatter.write_str("exact presence count overflowed u128"),
            Self::ResourceBoundOverflow { resource } => {
                write!(formatter, "{resource} conservative bound overflowed u64")
            }
            Self::RowNotFound { row_id } => write!(
                formatter,
                "row at page {}, slot {}, generation {} does not exist",
                row_id.page.0, row_id.slot, row_id.generation
            ),
            Self::RowDeleted { row_id } => write!(
                formatter,
                "row at page {}, slot {}, generation {} has been deleted",
                row_id.page.0, row_id.slot, row_id.generation
            ),
            Self::StaleRowId {
                row_id,
                actual_generation,
            } => write!(
                formatter,
                "row locator at page {}, slot {} has stale generation {}; current generation is {actual_generation}",
                row_id.page.0, row_id.slot, row_id.generation
            ),
            Self::RowTooLarge { size, capacity } => write!(
                formatter,
                "row payload of {size} bytes exceeds page capacity {capacity}"
            ),
            Self::PageOffsetOverflow { page_id } => {
                write!(
                    formatter,
                    "page {} offset overflows the disk address",
                    page_id.0
                )
            }
            Self::InvalidMvccHeader(message) => {
                write!(formatter, "invalid MVCC tuple header: {message}")
            }
            Self::UnsupportedTupleVersion(version) => {
                write!(formatter, "unsupported MVCC tuple version {version}")
            }
            Self::StorageContextMismatch { expected, actual } => write!(
                formatter,
                "storage context belongs to table {}, expected table {}",
                actual.0, expected.0
            ),
            Self::InvalidVisibilityBoundary { storage_id, value } => write!(
                formatter,
                "storage {} has invalid visibility boundary value {value}",
                storage_id.0
            ),
            Self::VisibilityBoundaryExhausted { storage_id } => write!(
                formatter,
                "storage {} visibility boundary space is exhausted",
                storage_id.0
            ),
            Self::VisibilityBoundaryContextMismatch {
                expected_storage_id,
                actual_storage_id,
                expected_kind,
                actual_kind,
            } => write!(
                formatter,
                "visibility boundary belongs to storage {} ({actual_kind:?}), expected storage {} ({expected_kind:?})",
                actual_storage_id.0, expected_storage_id.0
            ),
            Self::FutureVisibilityBoundary {
                storage_id,
                requested,
                current,
            } => write!(
                formatter,
                "storage {} visibility boundary {requested} is newer than current boundary {current}",
                storage_id.0
            ),
            Self::UnknownAccessPath {
                table_id,
                access_path,
            } => write!(
                formatter,
                "table {} has no registered access path {}",
                table_id.0, access_path.0
            ),
            Self::ResourceLimit { resource, limit } => {
                write!(formatter, "{resource} exceeds configured limit {limit}")
            }
            Self::UnsupportedOperation {
                operation,
                storage_kind,
            } => {
                write!(
                    formatter,
                    "{operation} is unsupported for {storage_kind} storage"
                )
            }
        }
    }
}

impl Error for StorageError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Schema(error) => Some(error),
            Self::Page(error) => Some(error),
            Self::Buffer(error) => Some(error),
            Self::Codec(error) => Some(error),
            Self::Metadata(error) => Some(error),
            Self::Index(error) => Some(error),
            Self::Recovery(error) => Some(error),
            Self::Wal(error) => Some(error),
            Self::Transaction(error) => Some(error),
            Self::TxnStatus(error) => Some(error),
            Self::Checkpoint(error) => Some(error),
            Self::Lsm(error) => Some(error),
            Self::Columnar(error) => Some(error),
            Self::ChangeStream(error) => Some(error),
            _ => None,
        }
    }
}

impl From<std::io::Error> for StorageError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<ColumnarError> for StorageError {
    fn from(error: ColumnarError) -> Self {
        match error {
            ColumnarError::ResourceBoundOverflow { resource } => {
                Self::ResourceBoundOverflow { resource }
            }
            other => Self::Columnar(other),
        }
    }
}

impl From<ChangeStreamError> for StorageError {
    fn from(error: ChangeStreamError) -> Self {
        match error {
            ChangeStreamError::Schema(error) => Self::Schema(error),
            other => Self::ChangeStream(other),
        }
    }
}

impl From<SchemaError> for StorageError {
    fn from(error: SchemaError) -> Self {
        Self::Schema(error)
    }
}

impl From<PageError> for StorageError {
    fn from(error: PageError) -> Self {
        Self::Page(error)
    }
}

impl From<BufferError> for StorageError {
    fn from(error: BufferError) -> Self {
        Self::Buffer(error)
    }
}

impl From<CodecError> for StorageError {
    fn from(error: CodecError) -> Self {
        Self::Codec(error)
    }
}

impl From<MetadataError> for StorageError {
    fn from(error: MetadataError) -> Self {
        Self::Metadata(error)
    }
}

impl From<IndexError> for StorageError {
    fn from(error: IndexError) -> Self {
        Self::Index(error)
    }
}

impl From<RecoveryError> for StorageError {
    fn from(error: RecoveryError) -> Self {
        Self::Recovery(error)
    }
}

impl From<WalError> for StorageError {
    fn from(error: WalError) -> Self {
        Self::Wal(error)
    }
}

impl From<LsmError> for StorageError {
    fn from(error: LsmError) -> Self {
        Self::Lsm(error)
    }
}

impl From<netbadb_lsm::LsmStorageError> for StorageError {
    fn from(error: netbadb_lsm::LsmStorageError) -> Self {
        use netbadb_lsm::LsmStorageError as E;
        match error {
            E::Io(error) => Self::Io(error),
            E::Schema(error) => Self::Schema(error),
            E::InvalidFormat(message) => Self::InvalidFormat(message),
            E::Codec(error) => Self::Codec(error),
            E::Lsm(error) => Self::Lsm(error),
            E::ChangeStream(error) => error.into(),
            E::Transaction(error) => Self::Transaction(error),
            E::Checkpoint(error) => Self::Checkpoint(error),
            E::Recovery(error) => Self::Recovery(error.into()),
            E::TableIdMismatch { expected, actual } => Self::TableIdMismatch { expected, actual },
            E::SchemaMismatch { expected, actual } => Self::SchemaMismatch { expected, actual },
            E::InvalidRowLength { expected, actual } => Self::InvalidRowLength { expected, actual },
            E::TypeMismatch {
                column,
                expected,
                actual,
            } => Self::TypeMismatch {
                column,
                expected,
                actual,
            },
            E::NullNotAllowed { column } => Self::NullNotAllowed { column },
            E::UnknownColumn { column_id } => Self::UnknownColumn { column_id },
            E::CountOverflow => Self::CountOverflow,
            E::ResourceBoundOverflow { resource } => Self::ResourceBoundOverflow { resource },
            E::ResourceLimit { resource, limit } => Self::ResourceLimit { resource, limit },
            E::FutureVisibilityBoundary {
                storage_id,
                requested,
                current,
            } => Self::FutureVisibilityBoundary {
                storage_id,
                requested,
                current,
            },
            E::VisibilityBoundaryExhausted { storage_id } => {
                Self::VisibilityBoundaryExhausted { storage_id }
            }
            E::InvalidVisibilityBoundary { storage_id, value } => {
                Self::InvalidVisibilityBoundary { storage_id, value }
            }
            E::VisibilityBoundaryContextMismatch {
                expected_storage_id,
                actual_storage_id,
                expected_kind,
                actual_kind,
            } => Self::VisibilityBoundaryContextMismatch {
                expected_storage_id,
                actual_storage_id,
                expected_kind,
                actual_kind,
            },
        }
    }
}

impl From<netbadb_heap::HeapStorageError> for StorageError {
    fn from(error: netbadb_heap::HeapStorageError) -> Self {
        use netbadb_heap::HeapStorageError as E;
        match error {
            E::Io(error) => Self::Io(error),
            E::Schema(error) => Self::Schema(error),
            E::InvalidFormat(message) => Self::InvalidFormat(message),
            E::Page(error) => Self::Page(error),
            E::Buffer(error) => Self::Buffer(error),
            E::Codec(error) => Self::Codec(error),
            E::Metadata(error) => Self::Metadata(error),
            E::Index(error) => Self::Index(error),
            E::Recovery(error) => Self::Recovery(error),
            E::Wal(error) => Self::Wal(error),
            E::Transaction(error) => Self::Transaction(error),
            E::TxnStatus(error) => Self::TxnStatus(error),
            E::Checkpoint(error) => Self::Checkpoint(error),
            E::ChangeStream(error) => error.into(),
            E::TableIdMismatch { expected, actual } => Self::TableIdMismatch { expected, actual },
            E::SchemaMismatch { expected, actual } => Self::SchemaMismatch { expected, actual },
            E::InvalidRowLength { expected, actual } => Self::InvalidRowLength { expected, actual },
            E::TypeMismatch {
                column,
                expected,
                actual,
            } => Self::TypeMismatch {
                column,
                expected,
                actual,
            },
            E::NullNotAllowed { column } => Self::NullNotAllowed { column },
            E::UnknownColumn { column_id } => Self::UnknownColumn { column_id },
            E::CountOverflow => Self::CountOverflow,
            E::ResourceBoundOverflow { resource } => Self::ResourceBoundOverflow { resource },
            E::RowNotFound { row_id } => Self::RowNotFound { row_id },
            E::RowDeleted { row_id } => Self::RowDeleted { row_id },
            E::StaleRowId {
                row_id,
                actual_generation,
            } => Self::StaleRowId {
                row_id,
                actual_generation,
            },
            E::RowTooLarge { size, capacity } => Self::RowTooLarge { size, capacity },
            E::PageOffsetOverflow { page_id } => Self::PageOffsetOverflow { page_id },
            E::InvalidMvccHeader(message) => Self::InvalidMvccHeader(message),
            E::UnsupportedTupleVersion(version) => Self::UnsupportedTupleVersion(version),
            E::StorageContextMismatch { expected, actual } => {
                Self::StorageContextMismatch { expected, actual }
            }
            E::InvalidVisibilityBoundary { storage_id, value } => {
                Self::InvalidVisibilityBoundary { storage_id, value }
            }
            E::VisibilityBoundaryExhausted { storage_id } => {
                Self::VisibilityBoundaryExhausted { storage_id }
            }
            E::VisibilityBoundaryContextMismatch {
                expected_storage_id,
                actual_storage_id,
                expected_kind,
                actual_kind,
            } => Self::VisibilityBoundaryContextMismatch {
                expected_storage_id,
                actual_storage_id,
                expected_kind,
                actual_kind,
            },
            E::FutureVisibilityBoundary {
                storage_id,
                requested,
                current,
            } => Self::FutureVisibilityBoundary {
                storage_id,
                requested,
                current,
            },
            E::UnknownAccessPath {
                table_id,
                access_path,
            } => Self::UnknownAccessPath {
                table_id,
                access_path,
            },
            E::ResourceLimit { resource, limit } => Self::ResourceLimit { resource, limit },
            E::UnsupportedOperation {
                operation,
                storage_kind,
            } => Self::UnsupportedOperation {
                operation,
                storage_kind,
            },
        }
    }
}

impl From<TransactionError> for StorageError {
    fn from(error: TransactionError) -> Self {
        Self::Transaction(error)
    }
}

impl From<TxnStatusError> for StorageError {
    fn from(error: TxnStatusError) -> Self {
        Self::TxnStatus(error)
    }
}

impl From<CheckpointError> for StorageError {
    fn from(error: CheckpointError) -> Self {
        Self::Checkpoint(error)
    }
}

impl From<netbadb_storage_api::VisibilityBoundaryError> for StorageError {
    fn from(error: netbadb_storage_api::VisibilityBoundaryError) -> Self {
        use netbadb_storage_api::VisibilityBoundaryError as E;
        match error {
            E::InvalidVisibilityBoundary { storage_id, value } => {
                Self::InvalidVisibilityBoundary { storage_id, value }
            }
            E::VisibilityBoundaryExhausted { storage_id } => {
                Self::VisibilityBoundaryExhausted { storage_id }
            }
            E::VisibilityBoundaryContextMismatch {
                expected_storage_id,
                actual_storage_id,
                expected_kind,
                actual_kind,
            } => Self::VisibilityBoundaryContextMismatch {
                expected_storage_id,
                actual_storage_id,
                expected_kind,
                actual_kind,
            },
            E::FutureVisibilityBoundary {
                storage_id,
                requested,
                current,
            } => Self::FutureVisibilityBoundary {
                storage_id,
                requested,
                current,
            },
        }
    }
}
