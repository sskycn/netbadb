//! Synchronous single-writer LSM table engine and durable formats.

mod engine;
mod lsm_source_inspection;

pub use engine::*;
pub use lsm_source_inspection::LsmPhysicalDesignSourceInspection;

pub use netbadb_change_stream::{
    ChangeReadResult, ChangeStorageKind, ChangeStreamCursor, ChangeStreamGcStorageReport,
    ChangeStreamInspection, ChangeStreamMaintenanceInspection, ChangeStreamRetentionPin,
    ChangeStreamSourceInspection, StorageVersionKey, lsm_change_log_path,
};
pub use netbadb_row_codec::CodecError;
pub use netbadb_storage_api::{
    CheckpointError, IsolationLevel, PreparedDecision, PreparedRecoveryError as RecoveryError,
    PreparedRuntimeInspection, PreparedTransaction, PreparedTransactionState,
    PreparedTxnResolution, PresenceCountSummary, StorageAccessCostHints, StorageKind,
    StorageVisibilityBoundary, TransactionError, TransactionState,
};

use std::error::Error;
use std::fmt;
use std::io;

use netbadb_change_stream::ChangeStreamError;
use netbadb_row_codec::RowError;
use netbadb_schema::{SchemaError, SchemaFingerprint};
use netbadb_types::{ColumnId, PhysicalType, StorageId, TableId};

#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub use netbadb_storage_api::source_inspection_test_activity;

pub(crate) use netbadb_change_stream as change_stream;

/// Errors at the LSM engine boundary. The storage facade maps these to its
/// historical `StorageError` variants without changing on-disk formats.
#[derive(Debug)]
pub enum LsmStorageError {
    Io(io::Error),
    Schema(SchemaError),
    InvalidFormat(String),
    Codec(CodecError),
    Lsm(LsmError),
    ChangeStream(ChangeStreamError),
    Transaction(TransactionError),
    Checkpoint(CheckpointError),
    Recovery(RecoveryError),
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
        column_id: ColumnId,
    },
    CountOverflow,
    ResourceBoundOverflow {
        resource: &'static str,
    },
    ResourceLimit {
        resource: &'static str,
        limit: u64,
    },
    FutureVisibilityBoundary {
        storage_id: StorageId,
        requested: u64,
        current: u64,
    },
    VisibilityBoundaryExhausted {
        storage_id: StorageId,
    },
    InvalidVisibilityBoundary {
        storage_id: StorageId,
        value: u64,
    },
    VisibilityBoundaryContextMismatch {
        expected_storage_id: StorageId,
        actual_storage_id: StorageId,
        expected_kind: StorageKind,
        actual_kind: StorageKind,
    },
}

pub(crate) use LsmStorageError as StorageError;

impl fmt::Display for LsmStorageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl Error for LsmStorageError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Schema(error) => Some(error),
            Self::Codec(error) => Some(error),
            Self::Lsm(error) => Some(error),
            Self::ChangeStream(error) => Some(error),
            Self::Transaction(error) => Some(error),
            Self::Checkpoint(error) => Some(error),
            Self::Recovery(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for LsmStorageError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}
impl From<SchemaError> for LsmStorageError {
    fn from(value: SchemaError) -> Self {
        Self::Schema(value)
    }
}
impl From<CodecError> for LsmStorageError {
    fn from(value: CodecError) -> Self {
        Self::Codec(value)
    }
}
impl From<LsmError> for LsmStorageError {
    fn from(value: LsmError) -> Self {
        Self::Lsm(value)
    }
}
impl From<ChangeStreamError> for LsmStorageError {
    fn from(value: ChangeStreamError) -> Self {
        match value {
            ChangeStreamError::Schema(error) => Self::Schema(error),
            other => Self::ChangeStream(other),
        }
    }
}
impl From<TransactionError> for LsmStorageError {
    fn from(value: TransactionError) -> Self {
        Self::Transaction(value)
    }
}
impl From<CheckpointError> for LsmStorageError {
    fn from(value: CheckpointError) -> Self {
        Self::Checkpoint(value)
    }
}
impl From<RecoveryError> for LsmStorageError {
    fn from(value: RecoveryError) -> Self {
        Self::Recovery(value)
    }
}
impl From<netbadb_storage_api::VisibilityBoundaryError> for LsmStorageError {
    fn from(value: netbadb_storage_api::VisibilityBoundaryError) -> Self {
        use netbadb_storage_api::VisibilityBoundaryError as E;
        match value {
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
impl From<RowError> for LsmStorageError {
    fn from(value: RowError) -> Self {
        match value {
            RowError::Codec(error) => Self::Codec(error),
            RowError::InvalidRowLength { expected, actual } => {
                Self::InvalidRowLength { expected, actual }
            }
            RowError::TypeMismatch {
                column,
                expected,
                actual,
            } => Self::TypeMismatch {
                column,
                expected,
                actual,
            },
            RowError::NullNotAllowed { column } => Self::NullNotAllowed { column },
            RowError::UnknownColumn { column_id } => Self::UnknownColumn { column_id },
            RowError::InvalidFormat(message) => invalid_format(message),
            RowError::ResourceLimit { resource, limit } => Self::ResourceLimit { resource, limit },
        }
    }
}

fn invalid_format(message: impl Into<String>) -> LsmStorageError {
    LsmStorageError::InvalidFormat(message.into())
}

mod row_codec {
    use crate::LsmStorageError;
    use netbadb_schema::TableDef;
    use netbadb_types::{ColumnId, ScalarValue};
    pub fn encode_row(values: &[ScalarValue]) -> Result<Vec<u8>, LsmStorageError> {
        netbadb_row_codec::encode_row(values).map_err(Into::into)
    }
    pub fn validate_row(table: &TableDef, values: &[ScalarValue]) -> Result<(), LsmStorageError> {
        netbadb_row_codec::validate_row(table, values).map_err(Into::into)
    }
    pub fn resolve_columns(
        table: &TableDef,
        columns: &[ColumnId],
    ) -> Result<Vec<usize>, LsmStorageError> {
        netbadb_row_codec::resolve_columns(table, columns).map_err(Into::into)
    }
    pub fn decode_row(
        payload: &[u8],
        table: &TableDef,
    ) -> Result<Vec<ScalarValue>, LsmStorageError> {
        netbadb_row_codec::decode_row(payload, table).map_err(Into::into)
    }
    pub fn decode_row_columns(
        payload: &[u8],
        table: &TableDef,
        columns: &[ColumnId],
    ) -> Result<Vec<ScalarValue>, LsmStorageError> {
        netbadb_row_codec::decode_row_columns(payload, table, columns).map_err(Into::into)
    }
    pub fn decode_row_positions(
        payload: &[u8],
        table: &TableDef,
        positions: &[usize],
    ) -> Result<Vec<ScalarValue>, LsmStorageError> {
        netbadb_row_codec::decode_row_positions(payload, table, positions).map_err(Into::into)
    }
}
