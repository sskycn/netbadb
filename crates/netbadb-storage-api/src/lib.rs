//! Engine-independent storage identities, access descriptions, and transaction observations.
#[cfg(feature = "test-hooks")]
#[doc(hidden)]
pub mod source_inspection_test_activity;

use netbadb_index::IndexStatistics;
use netbadb_types::{
    AccessPathId, ColumnId, CommandId, CommitSeq, DatabaseTxnId, StorageId, TxnId,
};

/// Executable capabilities advertised by one table-scoped access path.
///
/// These properties describe operations available to planning and execution;
/// they do not expose the access method's persistent representation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AccessPathCapabilities {
    pub point_lookup: bool,
    pub range_lookup: bool,
    /// Results are deterministic in access-key then storage row-identity order.
    pub ordered: bool,
}

/// Storage-owned weights in neutral integer planning-work units.
///
/// One unit is conventionally comparable to one managed sequential-page unit
/// from [`TableStatistics::managed_page_count`]; these values are neither
/// elapsed time nor persistent page identities. `point_probe_base_cost` is the
/// fixed CPU/access-method startup for one probe, excluding source reads and
/// returned rows. `expected_point_io` is the engine's expected count of
/// candidate-source reads for one point probe in the same neutral scale.
/// `range_startup_cost` is the fixed access-method work before a range returns
/// candidates. `sequential_unit_cost` converts each returned candidate row to
/// the same scale. Engines must not encode outer-row thresholds or measured
/// nanoseconds in these fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageAccessCostHints {
    pub point_probe_base_cost: u32,
    pub expected_point_io: u32,
    pub range_startup_cost: u32,
    pub sequential_unit_cost: u32,
}

/// Storage-owned optimizer snapshot for one registered access method.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageAccessPath {
    pub id: AccessPathId,
    pub column_id: ColumnId,
    pub capabilities: AccessPathCapabilities,
    pub statistics: Option<IndexStatistics>,
    pub cost_hints: Option<StorageAccessCostHints>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum StorageKind {
    Heap,
    Lsm,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    ReadCommitted,
    RepeatableRead,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Snapshot {
    pub visible_csn: CommitSeq,
    pub own_txn: Option<TxnId>,
    pub command_id: CommandId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreparedTransactionState {
    Prepared,
    Committed,
    RolledBack,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PreparedTransaction {
    pub database_txn_id: DatabaseTxnId,
    pub physical_txn_id: TxnId,
    /// Monotonic WAL position used to reconstruct prepare/rollback stack order.
    pub prepare_order: u64,
    pub state: PreparedTransactionState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreparedDecision {
    Commit,
    Abort,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PreparedTxnResolution {
    pub database_txn_id: DatabaseTxnId,
    pub physical_txn_id: TxnId,
    pub decision: PreparedDecision,
}

/// Invalid identity, horizon, or context for a committed storage boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VisibilityBoundaryError {
    InvalidVisibilityBoundary {
        storage_id: StorageId,
        value: u64,
    },
    VisibilityBoundaryExhausted {
        storage_id: StorageId,
    },
    VisibilityBoundaryContextMismatch {
        expected_storage_id: StorageId,
        actual_storage_id: StorageId,
        expected_kind: StorageKind,
        actual_kind: StorageKind,
    },
    FutureVisibilityBoundary {
        storage_id: StorageId,
        requested: u64,
        current: u64,
    },
}

impl std::fmt::Display for VisibilityBoundaryError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidVisibilityBoundary { storage_id, value } => write!(
                formatter,
                "invalid visibility boundary {value} for storage {}",
                storage_id.0
            ),
            Self::VisibilityBoundaryExhausted { storage_id } => {
                write!(
                    formatter,
                    "visibility boundary exhausted for storage {}",
                    storage_id.0
                )
            }
            Self::VisibilityBoundaryContextMismatch {
                expected_storage_id,
                actual_storage_id,
                expected_kind,
                actual_kind,
            } => write!(
                formatter,
                "visibility boundary context mismatch: expected {expected_kind:?} storage {}, found {actual_kind:?} storage {}",
                expected_storage_id.0, actual_storage_id.0
            ),
            Self::FutureVisibilityBoundary {
                storage_id,
                requested,
                current,
            } => write!(
                formatter,
                "visibility boundary {requested} is beyond current boundary {current} for storage {}",
                storage_id.0
            ),
        }
    }
}
impl std::error::Error for VisibilityBoundaryError {}

/// Opaque committed visibility boundary for one physical storage.
///
/// The numeric value is meaningful only with both the storage identity and
/// engine kind. Value zero is reserved; an empty engine's local horizon zero
/// is encoded as boundary value one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StorageVisibilityBoundary {
    storage_id: StorageId,
    storage_kind: StorageKind,
    value: u64,
}

impl StorageVisibilityBoundary {
    pub fn new(
        storage_id: StorageId,
        storage_kind: StorageKind,
        value: u64,
    ) -> Result<Self, VisibilityBoundaryError> {
        if storage_id.0 == 0 || value == 0 {
            return Err(VisibilityBoundaryError::InvalidVisibilityBoundary { storage_id, value });
        }
        Ok(Self {
            storage_id,
            storage_kind,
            value,
        })
    }

    #[must_use]
    pub const fn storage_id(self) -> StorageId {
        self.storage_id
    }

    #[must_use]
    pub const fn storage_kind(self) -> StorageKind {
        self.storage_kind
    }

    #[must_use]
    pub const fn value(self) -> u64 {
        self.value
    }

    pub fn from_local_horizon(
        storage_id: StorageId,
        storage_kind: StorageKind,
        horizon: u64,
    ) -> Result<Self, VisibilityBoundaryError> {
        let value = horizon
            .checked_add(1)
            .ok_or(VisibilityBoundaryError::VisibilityBoundaryExhausted { storage_id })?;
        Self::new(storage_id, storage_kind, value)
    }

    pub const fn local_horizon(self) -> u64 {
        self.value - 1
    }
}

pub fn validate_visibility_boundary(
    expected_storage_id: StorageId,
    expected_kind: StorageKind,
    current_horizon: u64,
    boundary: StorageVisibilityBoundary,
) -> Result<(), VisibilityBoundaryError> {
    if boundary.storage_id != expected_storage_id || boundary.storage_kind != expected_kind {
        return Err(VisibilityBoundaryError::VisibilityBoundaryContextMismatch {
            expected_storage_id,
            actual_storage_id: boundary.storage_id,
            expected_kind,
            actual_kind: boundary.storage_kind,
        });
    }
    let current_value = current_horizon.checked_add(1).ok_or(
        VisibilityBoundaryError::VisibilityBoundaryExhausted {
            storage_id: expected_storage_id,
        },
    )?;
    let requested_horizon = boundary.local_horizon();
    if requested_horizon > current_horizon {
        return Err(VisibilityBoundaryError::FutureVisibilityBoundary {
            storage_id: expected_storage_id,
            requested: boundary.value,
            current: current_value,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use netbadb_types::StorageId;

    use super::{
        StorageKind, StorageVisibilityBoundary, VisibilityBoundaryError,
        validate_visibility_boundary,
    };

    #[test]
    fn boundary_requires_exact_storage_kind_and_current_horizon() {
        let heap =
            StorageVisibilityBoundary::from_local_horizon(StorageId(7), StorageKind::Heap, 11)
                .unwrap();
        assert_eq!(heap.value(), 12);
        assert_eq!(heap.local_horizon(), 11);
        assert!(matches!(
            validate_visibility_boundary(StorageId(8), StorageKind::Heap, 11, heap),
            Err(VisibilityBoundaryError::VisibilityBoundaryContextMismatch { .. })
        ));
        assert!(matches!(
            validate_visibility_boundary(StorageId(7), StorageKind::Lsm, 11, heap),
            Err(VisibilityBoundaryError::VisibilityBoundaryContextMismatch { .. })
        ));
        assert!(matches!(
            validate_visibility_boundary(StorageId(7), StorageKind::Heap, 10, heap),
            Err(VisibilityBoundaryError::FutureVisibilityBoundary { .. })
        ));
    }
}

use std::error::Error;
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    Active,
    /// A compound logical operation logged only part of its physical work.
    /// The transaction may be rolled back but cannot continue or commit.
    RollbackRequired,
    PreparePending,
    Prepared,
    /// A group Prepare is staged and the writer is released, but its storage
    /// durability barrier has not yet succeeded.
    ParkedPreparePending,
    ParkedPrepared,
    CommitPending,
    /// Authoritative commit is durable, while a grouped Change Stream
    /// Finalize barrier has not yet promoted the prepared batch.
    ChangeFinalizePending,
    RollbackPending,
    Committed,
    RolledBack,
}

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
    StatusBusy,
    CommandIdExhausted,
    InvalidDatabaseTxnId,
    DatabaseTxnMismatch {
        txn_id: netbadb_types::TxnId,
        expected: Option<netbadb_types::DatabaseTxnId>,
        actual: netbadb_types::DatabaseTxnId,
    },
    NotPrepared {
        txn_id: netbadb_types::TxnId,
        state: TransactionState,
    },
    PreparedWriteConflict {
        txn_id: netbadb_types::TxnId,
        conflicting_txn_id: netbadb_types::TxnId,
    },
    PreparedResolutionOrder {
        txn_id: netbadb_types::TxnId,
        expected: netbadb_types::TxnId,
    },
    EmptyPreparedCommitBatch,
    PreparedCommitBatchStorageMismatch,
    EmptyPreparedPrepareBatch,
    PreparedPrepareBatchStorageMismatch,
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
    #[cfg(any(test, feature = "test-hooks"))]
    RollbackInterrupted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedRuntimeInspection {
    pub parked_prepared_count: usize,
    pub parked_prepare_pending_count: usize,
    pub active_group_chain: Vec<netbadb_types::TxnId>,
    pub prepared_write_conflict_count: u64,
    /// Durable prepare barriers issued by this live storage runtime.
    pub prepare_sync_count: u64,
    /// Prepare barriers shared by explicitly staged group members.
    pub group_prepare_barrier_sync_count: u64,
    /// Commit barriers issued by ordinary or individually resolved commits.
    pub single_commit_sync_count: u64,
    /// Post-decision barriers shared by explicit group members.
    pub group_commit_barrier_sync_count: u64,
    /// All NBCL prepare and finalize sync calls successfully completed by this
    /// live runtime. Failed or uncertain attempts are not counted.
    pub change_stream_sync_count: u64,
    /// Successful per-member NBCL Prepare syncs in this live runtime.
    pub change_stream_member_prepare_sync_count: u64,
    /// Successful grouped NBCL Prepare barriers in this live runtime.
    pub change_stream_group_prepare_barrier_sync_count: u64,
    /// Successful per-member NBCL Finalize syncs in this live runtime.
    pub change_stream_member_finalize_sync_count: u64,
    /// Successful grouped NBCL Finalize barriers in this live runtime.
    pub change_stream_group_finalize_barrier_sync_count: u64,
    /// Successful syncs that checkpointed previously promoted pipelined
    /// Finalizes. This overlaps the reason-specific counters below.
    pub change_stream_pipelined_finalize_checkpoint_sync_count: u64,
    pub change_stream_combined_finalize_prepare_sync_count: u64,
    pub change_stream_explicit_finalize_checkpoint_sync_count: u64,
    pub change_stream_recovery_finalize_sync_count: u64,
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
            Self::StatusBusy => formatter.write_str("transaction-status store is already borrowed"),
            Self::CommandIdExhausted => formatter.write_str("transaction command ID exhausted"),
            Self::InvalidDatabaseTxnId => {
                formatter.write_str("database transaction ID zero is invalid")
            }
            Self::DatabaseTxnMismatch {
                txn_id,
                expected,
                actual,
            } => write!(
                formatter,
                "physical transaction {} is prepared for database transaction {expected:?}, not {}",
                txn_id.0, actual.0
            ),
            Self::NotPrepared { txn_id, state } => write!(
                formatter,
                "physical transaction {} is {state:?}, not prepared",
                txn_id.0
            ),
            Self::PreparedWriteConflict {
                txn_id,
                conflicting_txn_id,
            } => write!(
                formatter,
                "transaction {} conflicts with parked prepared transaction {}",
                txn_id.0, conflicting_txn_id.0
            ),
            Self::PreparedResolutionOrder { txn_id, expected } => write!(
                formatter,
                "parked prepared transaction {} cannot resolve before transaction {}",
                txn_id.0, expected.0
            ),
            Self::EmptyPreparedCommitBatch => formatter.write_str("prepared commit batch is empty"),
            Self::PreparedCommitBatchStorageMismatch => formatter
                .write_str("prepared commit batch mixes physical storage identities or kinds"),
            Self::EmptyPreparedPrepareBatch => formatter.write_str("staged prepare batch is empty"),
            Self::PreparedPrepareBatchStorageMismatch => formatter
                .write_str("staged prepare batch mixes physical storage identities or kinds"),
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
            #[cfg(any(test, feature = "test-hooks"))]
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

/// Exact counts collected by one current live Heap scan.
///
/// `non_null_counts` follows the caller-provided column request order and
/// retains duplicate requests. Both the live-row count and column counts use
/// checked `u128` arithmetic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PresenceCountSummary {
    pub live_rows: u128,
    pub non_null_counts: Vec<u128>,
}

/// Prepared transaction resolution failures shared by authoritative engines.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreparedRecoveryError {
    PreparedTransactionRequiresResolution {
        database_txn_id: DatabaseTxnId,
        physical_txn_id: TxnId,
    },
    DuplicatePreparedResolution {
        physical_txn_id: TxnId,
    },
    PreparedResolutionMismatch {
        physical_txn_id: TxnId,
        expected: DatabaseTxnId,
        actual: DatabaseTxnId,
    },
    UnknownPreparedResolution {
        database_txn_id: DatabaseTxnId,
        physical_txn_id: TxnId,
    },
    PreparedResolutionConflictsWithTerminalState {
        database_txn_id: DatabaseTxnId,
        physical_txn_id: TxnId,
        state: PreparedTransactionState,
        decision: PreparedDecision,
    },
}

impl fmt::Display for PreparedRecoveryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl Error for PreparedRecoveryError {}

/// Primitive scan counters shared by projection readers and query feedback.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ColumnarScanStatistics {
    pub row_groups_total: u64,
    pub row_groups_read: u64,
    pub row_groups_pruned: u64,
    pub rows_read: u64,
    pub column_chunks_read: u64,
    pub bytes_read: u64,
    pub base_rows_suppressed: u64,
    pub delta_segments: u64,
    pub delta_mutations: u64,
    pub delta_live_rows: u64,
    pub delta_rows_emitted: u64,
    pub delta_bytes_read: u64,
    pub merged_rows: u64,
    /// Physical payload blocks fetched for this scan. Header/footer reads made
    /// while opening the immutable projection are intentionally excluded.
    pub physical_block_reads: u64,
    /// Physical payload bytes fetched for this scan.
    pub physical_bytes_read: u64,
    /// Column chunks decoded for this scan.
    pub decoded_column_chunks: u64,
    /// Hidden source-version blocks decoded for suppression.
    pub decoded_version_blocks: u64,
    pub base_data_bytes_read: u64,
    pub base_version_key_bytes_read: u64,
    pub delta_data_bytes_read: u64,
    pub version_key_chunks_read: u64,
    pub blocks_verified: u64,
    pub row_groups_pruned_before_data_read: u64,
}
