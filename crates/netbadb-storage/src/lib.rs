//! Synchronous page, buffer, and heap storage for the embedded vertical slice.

mod buffer;
mod heap;
mod page;
mod recovery;
mod transaction;
mod wal;

pub use buffer::{BufferPool, DEFAULT_BUFFER_POOL_SIZE, ReadPageGuard};
pub use heap::HeapStorage;
pub use page::{
    PAGE_FORMAT_VERSION, PAGE_HEADER_SIZE, PAGE_MAGIC, PAGE_SIZE, Page, PageHeader, PageManager,
    PageType, SLOT_SIZE, Slot,
};
pub use recovery::RecoveryError;
pub use transaction::{Transaction, TransactionState};
pub use wal::{
    WAL_FORMAT_VERSION, WAL_HEADER_SIZE, WAL_MAX_RECORD_SIZE, WalError, WalManager, WalRecord,
    WalRecordKind, wal_alternate_path, wal_path,
};

use std::error::Error;
use std::fmt;

use netbadb_types::{PageId, PhysicalType, SlotId};

/// Errors raised while validating or mutating a raw database page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PageError {
    InvalidMagic,
    UnsupportedVersion(u16),
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
    PageFull {
        required: usize,
        available: usize,
    },
    RecordTooLarge {
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
        }
    }
}

impl Error for PageError {}

/// Errors raised by the in-memory buffer pool.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BufferError {
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

/// Errors raised while decoding the explicit row scalar format.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodecError {
    MissingScalarTag,
    UnknownScalarTag(u8),
    InvalidBoolean(u8),
    ScalarTruncated,
    LengthOverflow,
    TextNotUtf8,
    ExtraValues,
}

impl fmt::Display for CodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingScalarTag => formatter.write_str("row is missing a scalar tag"),
            Self::UnknownScalarTag(tag) => write!(formatter, "unknown scalar tag {tag}"),
            Self::InvalidBoolean(value) => write!(formatter, "invalid boolean value {value}"),
            Self::ScalarTruncated => formatter.write_str("scalar value is truncated"),
            Self::LengthOverflow => formatter.write_str("scalar length overflows the row"),
            Self::TextNotUtf8 => formatter.write_str("text value is not valid UTF-8"),
            Self::ExtraValues => formatter.write_str("row contains extra values"),
        }
    }
}

impl Error for CodecError {}

/// Errors raised while decoding the heap file root metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataError {
    InvalidMagic,
    UnsupportedVersion(u16),
    InvalidReservedBytes,
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
        }
    }
}

impl Error for MetadataError {}

/// Errors raised by the transaction state machine and single-writer guard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransactionError {
    NotActive {
        txn_id: netbadb_types::TxnId,
        state: TransactionState,
    },
    IdExhausted,
    OutstandingTransactionCountOverflow,
    WalBusy,
    WriterBusy {
        txn_id: netbadb_types::TxnId,
    },
    InvalidRollbackChain {
        txn_id: netbadb_types::TxnId,
        lsn: netbadb_types::Lsn,
    },
    UnfinishedWriter {
        txn_id: netbadb_types::TxnId,
    },
    OutstandingTransactions {
        count: u64,
    },
    RecoveryRequired,
    ForeignTransaction {
        txn_id: netbadb_types::TxnId,
    },
    #[cfg(test)]
    RollbackInterrupted,
}

impl fmt::Display for TransactionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotActive { txn_id, state } => write!(
                formatter,
                "transaction {} is {state:?}, not active",
                txn_id.0
            ),
            Self::IdExhausted => formatter.write_str("transaction ID space is exhausted"),
            Self::OutstandingTransactionCountOverflow => {
                formatter.write_str("outstanding transaction count overflowed")
            }
            Self::WalBusy => formatter.write_str("transaction WAL is already borrowed"),
            Self::WriterBusy { txn_id } => {
                write!(formatter, "transaction {} is the active writer", txn_id.0)
            }
            Self::InvalidRollbackChain { txn_id, lsn } => write!(
                formatter,
                "transaction {} has an invalid rollback chain at WAL record {}",
                txn_id.0, lsn.0
            ),
            Self::UnfinishedWriter { txn_id } => write!(
                formatter,
                "transaction {} still owns the database writer",
                txn_id.0
            ),
            Self::OutstandingTransactions { count } => write!(
                formatter,
                "{count} transaction handle(s) are still outstanding"
            ),
            Self::RecoveryRequired => formatter
                .write_str("an unfinished writer requires database recovery before writing again"),
            Self::ForeignTransaction { txn_id } => write!(
                formatter,
                "transaction {} belongs to a different database",
                txn_id.0
            ),
            #[cfg(test)]
            Self::RollbackInterrupted => {
                formatter.write_str("rollback interrupted by a test failure injection")
            }
        }
    }
}

impl Error for TransactionError {}

/// Errors raised when a quiescent checkpoint cannot be admitted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckpointError {
    OutstandingTransactions { count: u64 },
    WriterActive { txn_id: netbadb_types::TxnId },
    RecoveryRequired,
}

impl fmt::Display for CheckpointError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OutstandingTransactions { count } => write!(
                formatter,
                "checkpoint requires quiescence but {count} transaction handle(s) are outstanding"
            ),
            Self::WriterActive { txn_id } => write!(
                formatter,
                "checkpoint cannot run while transaction {} owns the writer",
                txn_id.0
            ),
            Self::RecoveryRequired => formatter
                .write_str("checkpoint cannot clear a database that requires startup recovery"),
        }
    }
}

impl Error for CheckpointError {}

#[derive(Debug)]
pub enum StorageError {
    Io(std::io::Error),
    InvalidFormat(String),
    Page(PageError),
    Buffer(BufferError),
    Codec(CodecError),
    Metadata(MetadataError),
    Recovery(RecoveryError),
    Wal(WalError),
    Transaction(TransactionError),
    Checkpoint(CheckpointError),
    SchemaMismatch {
        expected: String,
        actual: String,
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
    RowTooLarge {
        size: usize,
        capacity: usize,
    },
    PageOffsetOverflow {
        page_id: PageId,
    },
}

impl fmt::Display for StorageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "storage I/O error: {error}"),
            Self::InvalidFormat(message) => write!(formatter, "invalid database format: {message}"),
            Self::Page(error) => write!(formatter, "page error: {error}"),
            Self::Buffer(error) => write!(formatter, "buffer pool error: {error}"),
            Self::Codec(error) => write!(formatter, "row codec error: {error}"),
            Self::Metadata(error) => write!(formatter, "heap metadata error: {error}"),
            Self::Recovery(error) => write!(formatter, "recovery error: {error}"),
            Self::Wal(error) => write!(formatter, "write-ahead log error: {error}"),
            Self::Transaction(error) => write!(formatter, "transaction error: {error}"),
            Self::Checkpoint(error) => write!(formatter, "checkpoint error: {error}"),
            Self::SchemaMismatch { expected, actual } => write!(
                formatter,
                "schema mismatch: expected {expected}, found {actual}"
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
        }
    }
}

impl Error for StorageError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Page(error) => Some(error),
            Self::Buffer(error) => Some(error),
            Self::Codec(error) => Some(error),
            Self::Metadata(error) => Some(error),
            Self::Recovery(error) => Some(error),
            Self::Wal(error) => Some(error),
            Self::Transaction(error) => Some(error),
            Self::Checkpoint(error) => Some(error),
            _ => None,
        }
    }
}

impl From<std::io::Error> for StorageError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
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

impl From<TransactionError> for StorageError {
    fn from(error: TransactionError) -> Self {
        Self::Transaction(error)
    }
}

impl From<CheckpointError> for StorageError {
    fn from(error: CheckpointError) -> Self {
        Self::Checkpoint(error)
    }
}

fn invalid_format(message: impl Into<String>) -> StorageError {
    StorageError::InvalidFormat(message.into())
}
