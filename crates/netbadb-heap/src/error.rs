use std::error::Error;
use std::fmt;

use crate::{
    CheckpointError, CodecError, PageType, RecoveryError, TransactionError, TxnStatusError,
    WalError,
};
use netbadb_change_stream::ChangeStreamError;
use netbadb_index::IndexError;
use netbadb_schema::{SchemaError, SchemaFingerprint};
use netbadb_types::{AccessPathId, PageId, PhysicalType, SlotId, TableId};

/// Errors raised while validating or mutating a raw database page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PageError {
    InvalidMagic,
    UnsupportedVersion(u16),
    ChecksumMismatch {
        stored: u32,
        computed: u32,
    },
    UnknownPageType(u8),
    InvalidReservedByte(u8),
    InvalidSlotCount(u16),
    InvalidFreeSpace {
        free_start: u16,
        free_end: u16,
    },
    SlotDirectoryOutOfBounds {
        slot_count: u16,
        free_start: u16,
    },
    InvalidSlot {
        slot: SlotId,
    },
    InvalidSlotGeneration {
        slot: SlotId,
        generation: u32,
    },
    SlotDeleted {
        slot: SlotId,
    },
    InvalidDeletedSlotEncoding {
        slot: SlotId,
        offset: u16,
        length: u16,
    },
    RecordOutOfBounds {
        slot: SlotId,
        offset: u16,
        length: u16,
    },
    RecordOverlapsFreeSpace {
        slot: SlotId,
        offset: u16,
        free_end: u16,
    },
    OverlappingRecords {
        first: SlotId,
        second: SlotId,
    },
    WrongPageType {
        expected: PageType,
        actual: PageType,
    },
    InvalidSinglePayload {
        page_type: PageType,
        slot_count: u16,
    },
    InvalidSinglePayloadGeneration {
        page_type: PageType,
        generation: u32,
    },
    PageFull {
        required: usize,
        available: usize,
    },
    RecordTooLarge {
        size: usize,
        capacity: usize,
    },
    UpdateWouldOverflowPage {
        slot: SlotId,
        size: usize,
        capacity: usize,
    },
}

impl fmt::Display for PageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidMagic => formatter.write_str("page magic does not match"),
            Self::UnsupportedVersion(version) => {
                write!(formatter, "unsupported page format version {version}")
            }
            Self::ChecksumMismatch { stored, computed } => write!(
                formatter,
                "page checksum mismatch: stored {stored:#010x}, computed {computed:#010x}"
            ),
            Self::UnknownPageType(tag) => write!(formatter, "unknown page type tag {tag}"),
            Self::InvalidReservedByte(value) => {
                write!(formatter, "page reserved byte must be zero, found {value}")
            }
            Self::InvalidSlotCount(count) => write!(formatter, "invalid page slot count {count}"),
            Self::InvalidFreeSpace {
                free_start,
                free_end,
            } => write!(
                formatter,
                "invalid page free-space bounds {free_start}..{free_end}"
            ),
            Self::SlotDirectoryOutOfBounds {
                slot_count,
                free_start,
            } => write!(
                formatter,
                "slot directory with {slot_count} slots ends at {free_start}"
            ),
            Self::InvalidSlot { slot } => write!(formatter, "invalid page slot {}", slot.0),
            Self::InvalidSlotGeneration { slot, generation } => write!(
                formatter,
                "page slot {} has invalid generation {generation}",
                slot.0
            ),
            Self::SlotDeleted { slot } => write!(formatter, "page slot {} is deleted", slot.0),
            Self::InvalidDeletedSlotEncoding {
                slot,
                offset,
                length,
            } => write!(
                formatter,
                "slot {} has invalid deleted encoding ({offset}, {length})",
                slot.0
            ),
            Self::RecordOutOfBounds {
                slot,
                offset,
                length,
            } => write!(
                formatter,
                "record in slot {} is out of bounds at {offset} with length {length}",
                slot.0
            ),
            Self::RecordOverlapsFreeSpace {
                slot,
                offset,
                free_end,
            } => write!(
                formatter,
                "record in slot {} at {offset} overlaps free space beginning at {free_end}",
                slot.0
            ),
            Self::OverlappingRecords { first, second } => write!(
                formatter,
                "records in slots {} and {} overlap",
                first.0, second.0
            ),
            Self::WrongPageType { expected, actual } => {
                write!(formatter, "expected {expected:?} page, found {actual:?}")
            }
            Self::InvalidSinglePayload {
                page_type,
                slot_count,
            } => write!(
                formatter,
                "{page_type:?} page must contain exactly one live payload slot, found {slot_count}"
            ),
            Self::InvalidSinglePayloadGeneration {
                page_type,
                generation,
            } => write!(
                formatter,
                "{page_type:?} single payload must have generation 1, found {generation}"
            ),
            Self::PageFull {
                required,
                available,
            } => write!(
                formatter,
                "page needs {required} bytes but only {available} are free"
            ),
            Self::RecordTooLarge { size, capacity } => write!(
                formatter,
                "record of {size} bytes exceeds page record capacity {capacity}"
            ),
            Self::UpdateWouldOverflowPage {
                slot,
                size,
                capacity,
            } => write!(
                formatter,
                "replacement record of {size} bytes for slot {} exceeds its page capacity {capacity}",
                slot.0
            ),
        }
    }
}

impl Error for PageError {}

/// Errors raised by the in-memory buffer pool.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BufferError {
    PageDirty {
        page_id: PageId,
    },
    InvalidCapacity,
    Exhausted {
        capacity: usize,
    },
    PagePinned {
        page_id: PageId,
    },
    PageNotCached {
        page_id: PageId,
    },
    PinCountOverflow {
        page_id: PageId,
    },
    WalUnavailable {
        page_id: PageId,
        page_lsn: netbadb_types::Lsn,
    },
}

impl fmt::Display for BufferError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PageDirty { page_id } => write!(
                formatter,
                "page {} is dirty during suffix invalidation",
                page_id.0
            ),
            Self::InvalidCapacity => formatter.write_str("buffer pool capacity must be non-zero"),
            Self::Exhausted { capacity } => write!(
                formatter,
                "buffer pool with capacity {capacity} has no evictable frame"
            ),
            Self::PagePinned { page_id } => {
                write!(formatter, "page {} is pinned by an active guard", page_id.0)
            }
            Self::PageNotCached { page_id } => {
                write!(formatter, "page {} is not cached", page_id.0)
            }
            Self::PinCountOverflow { page_id } => {
                write!(formatter, "pin count for page {} overflows", page_id.0)
            }
            Self::WalUnavailable { page_id, page_lsn } => write!(
                formatter,
                "page {} at LSN {} cannot be flushed without its WAL",
                page_id.0, page_lsn.0
            ),
        }
    }
}

impl Error for BufferError {}

/// Errors raised while decoding the heap file root metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataError {
    InvalidMagic,
    UnsupportedVersion(u16),
    InvalidReservedBytes,
    InvalidStorageId(netbadb_types::StorageId),
    InvalidColumnCount { stored: u16, expected: usize },
}

impl fmt::Display for MetadataError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidMagic => formatter.write_str("heap metadata magic does not match"),
            Self::UnsupportedVersion(version) => {
                write!(formatter, "unsupported heap metadata version {version}")
            }
            Self::InvalidReservedBytes => {
                formatter.write_str("heap metadata reserved bytes are non-zero")
            }
            Self::InvalidStorageId(storage_id) => write!(
                formatter,
                "heap metadata stores invalid physical storage ID {}",
                storage_id.0
            ),
            Self::InvalidColumnCount { stored, expected } => write!(
                formatter,
                "heap metadata stores {stored} columns but its schema fingerprint identifies {expected}"
            ),
        }
    }
}

impl Error for MetadataError {}

#[derive(Debug)]
pub enum HeapStorageError {
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

impl fmt::Display for HeapStorageError {
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

impl Error for HeapStorageError {
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
            Self::ChangeStream(error) => Some(error),
            _ => None,
        }
    }
}

impl From<std::io::Error> for HeapStorageError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<ChangeStreamError> for HeapStorageError {
    fn from(error: ChangeStreamError) -> Self {
        match error {
            ChangeStreamError::Schema(error) => Self::Schema(error),
            other => Self::ChangeStream(other),
        }
    }
}

impl From<SchemaError> for HeapStorageError {
    fn from(error: SchemaError) -> Self {
        Self::Schema(error)
    }
}

impl From<PageError> for HeapStorageError {
    fn from(error: PageError) -> Self {
        Self::Page(error)
    }
}

impl From<BufferError> for HeapStorageError {
    fn from(error: BufferError) -> Self {
        Self::Buffer(error)
    }
}

impl From<CodecError> for HeapStorageError {
    fn from(error: CodecError) -> Self {
        Self::Codec(error)
    }
}

impl From<MetadataError> for HeapStorageError {
    fn from(error: MetadataError) -> Self {
        Self::Metadata(error)
    }
}

impl From<IndexError> for HeapStorageError {
    fn from(error: IndexError) -> Self {
        Self::Index(error)
    }
}

impl From<RecoveryError> for HeapStorageError {
    fn from(error: RecoveryError) -> Self {
        Self::Recovery(error)
    }
}

impl From<WalError> for HeapStorageError {
    fn from(error: WalError) -> Self {
        Self::Wal(error)
    }
}

impl From<TransactionError> for HeapStorageError {
    fn from(error: TransactionError) -> Self {
        Self::Transaction(error)
    }
}

impl From<TxnStatusError> for HeapStorageError {
    fn from(error: TxnStatusError) -> Self {
        Self::TxnStatus(error)
    }
}

impl From<CheckpointError> for HeapStorageError {
    fn from(error: CheckpointError) -> Self {
        Self::Checkpoint(error)
    }
}

pub(crate) fn invalid_format(message: impl Into<String>) -> HeapStorageError {
    HeapStorageError::InvalidFormat(message.into())
}

impl From<netbadb_storage_api::VisibilityBoundaryError> for HeapStorageError {
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
