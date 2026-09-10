use std::ops::ControlFlow;
use std::path::{Path, PathBuf};

use netbadb_index::{BTreeHandle, IndexDefinition, IndexRange, IndexStatistics, TableStatistics};
use netbadb_schema::TableDef;
use netbadb_types::{
    AccessPathId, ColumnId, DatabaseTxnId, IndexId, IndexName, Lsn, RowId, ScalarRef, ScalarValue,
    StorageDataVersion, StorageId, TableId, TxnId,
};

use crate::{
    HeapIdentityInspection, HeapRecoveryInspection, HeapStorage, IsolationLevel,
    LsmIdentityInspection, LsmInspection, LsmReadView, LsmRecoveryInspection, LsmRowHandle,
    LsmStorage, LsmTransaction, PreparedTxnResolution, PresenceCountSummary, ReadView,
    StorageError, StorageSnapshotToken, Transaction, TransactionError, TransactionState,
};

/// Logical index identity copied into a private replacement Heap.
/// Physical BTree handles are intentionally replaced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeapRewriteIndex {
    pub id: IndexId,
    pub name: Option<IndexName>,
    pub column_id: ColumnId,
}

/// Active index inventory plus its durable allocation boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeapRewriteIndexes {
    pub active: Vec<HeapRewriteIndex>,
    pub next_index_id: IndexId,
}

/// One exact file owned by a single Heap storage resource.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeapResourceComponentKind {
    Main,
    Wal,
    TransactionStatus,
    AlternateWal,
    ChangeLog,
    ChangeStreamGuard,
}

/// Storage-authored physical bundle member. Callers may add their own
/// higher-layer metadata, but must not infer Heap suffixes independently.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeapResourceComponent {
    pub kind: HeapResourceComponentKind,
    pub path: PathBuf,
    pub required: bool,
}

/// Returns the complete, exact set of files owned by one Heap implementation.
/// Index-catalog and BTree pages are contained in `Main`.
#[must_use]
pub fn heap_resource_components(path: impl AsRef<Path>) -> Vec<HeapResourceComponent> {
    let main = path.as_ref();
    let wal = crate::wal_path(main);
    vec![
        HeapResourceComponent {
            kind: HeapResourceComponentKind::Main,
            path: main.to_owned(),
            required: true,
        },
        HeapResourceComponent {
            kind: HeapResourceComponentKind::Wal,
            path: wal.clone(),
            required: true,
        },
        HeapResourceComponent {
            kind: HeapResourceComponentKind::TransactionStatus,
            path: crate::txn_status_path(main),
            required: true,
        },
        HeapResourceComponent {
            kind: HeapResourceComponentKind::AlternateWal,
            path: crate::wal_alternate_path(wal),
            required: false,
        },
        HeapResourceComponent {
            kind: HeapResourceComponentKind::ChangeLog,
            path: crate::heap_change_log_path(main),
            required: false,
        },
        HeapResourceComponent {
            kind: HeapResourceComponentKind::ChangeStreamGuard,
            path: crate::change_stream_guard_path(crate::heap_change_log_path(main)),
            required: false,
        },
    ]
}

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
    ) -> Result<Self, StorageError> {
        if storage_id.0 == 0 || value == 0 {
            return Err(StorageError::InvalidVisibilityBoundary { storage_id, value });
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

    fn from_local_horizon(
        storage_id: StorageId,
        storage_kind: StorageKind,
        horizon: u64,
    ) -> Result<Self, StorageError> {
        let value = horizon
            .checked_add(1)
            .ok_or(StorageError::VisibilityBoundaryExhausted { storage_id })?;
        Self::new(storage_id, storage_kind, value)
    }

    fn local_horizon(self) -> u64 {
        self.value - 1
    }
}

fn validate_visibility_boundary(
    expected_storage_id: StorageId,
    expected_kind: StorageKind,
    current_horizon: u64,
    boundary: StorageVisibilityBoundary,
) -> Result<(), StorageError> {
    if boundary.storage_id != expected_storage_id || boundary.storage_kind != expected_kind {
        return Err(StorageError::VisibilityBoundaryContextMismatch {
            expected_storage_id,
            actual_storage_id: boundary.storage_id,
            expected_kind,
            actual_kind: boundary.storage_kind,
        });
    }
    let current_value =
        current_horizon
            .checked_add(1)
            .ok_or(StorageError::VisibilityBoundaryExhausted {
                storage_id: expected_storage_id,
            })?;
    let requested_horizon = boundary.local_horizon();
    if requested_horizon > current_horizon {
        return Err(StorageError::FutureVisibilityBoundary {
            storage_id: expected_storage_id,
            requested: boundary.value,
            current: current_value,
        });
    }
    Ok(())
}

/// Opaque executor identity for a physical row version.
///
/// Heap keeps using its generation-safe [`RowId`] internally. Executor code
/// may retain and return this handle to the owning [`TableStorage`], but cannot
/// inspect PageId or SlotId or assume another storage layout shares them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StorageRowHandle {
    table_id: TableId,
    storage_id: StorageId,
    inner: StorageRowHandleKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum StorageRowHandleKind {
    Heap(RowId),
    Lsm(LsmRowHandle),
}

impl StorageRowHandle {
    fn heap(table_id: TableId, storage_id: StorageId, row_id: RowId) -> Self {
        Self {
            table_id,
            storage_id,
            inner: StorageRowHandleKind::Heap(row_id),
        }
    }

    fn heap_row_id(self, table_id: TableId, storage_id: StorageId) -> Result<RowId, StorageError> {
        if self.table_id != table_id || self.storage_id != storage_id {
            return Err(StorageError::StorageContextMismatch {
                expected: table_id,
                actual: self.table_id,
            });
        }
        match self.inner {
            StorageRowHandleKind::Heap(row_id) => Ok(row_id),
            StorageRowHandleKind::Lsm(_) => Err(StorageError::StorageContextMismatch {
                expected: table_id,
                actual: self.table_id,
            }),
        }
    }

    fn lsm(table_id: TableId, storage_id: StorageId, row: LsmRowHandle) -> Self {
        Self {
            table_id,
            storage_id,
            inner: StorageRowHandleKind::Lsm(row),
        }
    }

    fn lsm_handle(
        self,
        table_id: TableId,
        storage_id: StorageId,
    ) -> Result<LsmRowHandle, StorageError> {
        if self.table_id != table_id || self.storage_id != storage_id {
            return Err(StorageError::StorageContextMismatch {
                expected: table_id,
                actual: self.table_id,
            });
        }
        match self.inner {
            StorageRowHandleKind::Lsm(row) => Ok(row),
            StorageRowHandleKind::Heap(_) => Err(StorageError::StorageContextMismatch {
                expected: table_id,
                actual: self.table_id,
            }),
        }
    }

    /// Returns the owning physical storage without exposing its row locator.
    #[must_use]
    pub const fn storage_id(self) -> StorageId {
        self.storage_id
    }

    /// Converts an opaque handle observed through a committed read view into
    /// the exact storage-local version identity used by change streams.
    pub fn committed_version_key(self) -> Result<crate::StorageVersionKey, StorageError> {
        match self.inner {
            StorageRowHandleKind::Heap(row_id) => Ok(crate::StorageVersionKey::Heap {
                storage_id: self.storage_id,
                row_id,
            }),
            StorageRowHandleKind::Lsm(row) => match row.observed {
                crate::lsm::LsmObservedVersion::Committed(version) if version.0 != 0 => {
                    Ok(crate::StorageVersionKey::Lsm {
                        storage_id: self.storage_id,
                        row_id: row.row_id,
                        version,
                    })
                }
                crate::lsm::LsmObservedVersion::Committed(_) => Err(StorageError::InvalidFormat(
                    "committed LSM row has zero commit sequence".into(),
                )),
                crate::lsm::LsmObservedVersion::Pending(_) => {
                    Err(StorageError::UnsupportedOperation {
                        operation: "version identity for an uncommitted LSM row",
                        storage_kind: "LSM",
                    })
                }
            },
        }
    }
}

/// Table-scoped read context passed through executor storage capabilities.
#[derive(Debug)]
pub struct StorageReadView {
    table_id: TableId,
    inner: StorageReadViewKind,
}

/// An ownership-only pin that keeps the storage history needed by a database
/// snapshot alive without materializing row data.
#[derive(Debug)]
pub struct StorageVisibilityPin {
    boundary: StorageVisibilityBoundary,
    _view: StorageReadView,
}

impl StorageVisibilityPin {
    #[must_use]
    pub const fn boundary(&self) -> StorageVisibilityBoundary {
        self.boundary
    }
}

/// An atomic committed read view and matching storage-local change frontier.
#[derive(Debug)]
pub struct CommittedReadAnchor {
    pub read_view: StorageReadView,
    pub cursor: crate::ChangeStreamCursor,
}

#[derive(Debug)]
enum StorageReadViewKind {
    Heap(ReadView),
    Lsm(LsmReadView),
}

impl StorageReadView {
    fn heap(table_id: TableId, view: ReadView) -> Self {
        Self {
            table_id,
            inner: StorageReadViewKind::Heap(view),
        }
    }

    fn heap_view(&self, table_id: TableId) -> Result<&ReadView, StorageError> {
        if self.table_id != table_id {
            return Err(StorageError::StorageContextMismatch {
                expected: table_id,
                actual: self.table_id,
            });
        }
        match &self.inner {
            StorageReadViewKind::Heap(view) => Ok(view),
            StorageReadViewKind::Lsm(_) => Err(StorageError::StorageContextMismatch {
                expected: table_id,
                actual: self.table_id,
            }),
        }
    }

    fn lsm(table_id: TableId, view: LsmReadView) -> Self {
        Self {
            table_id,
            inner: StorageReadViewKind::Lsm(view),
        }
    }

    fn lsm_view(&self, table_id: TableId) -> Result<&LsmReadView, StorageError> {
        if self.table_id != table_id {
            return Err(StorageError::StorageContextMismatch {
                expected: table_id,
                actual: self.table_id,
            });
        }
        match &self.inner {
            StorageReadViewKind::Lsm(view) => Ok(view),
            StorageReadViewKind::Heap(_) => Err(StorageError::StorageContextMismatch {
                expected: table_id,
                actual: self.table_id,
            }),
        }
    }
}

/// Database-facing transaction wrapper with engine-specific state kept below
/// the table-storage boundary.
#[derive(Debug)]
pub struct StorageTransaction {
    table_id: TableId,
    storage_id: StorageId,
    inner: StorageTransactionKind,
}

/// Result of durably resolving one ordered prepared prefix for one storage.
///
/// Every member retains its own transaction and local commit identity. The
/// report describes the one shared post-decision WAL barrier only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageCommitBatchReport {
    pub storage_id: StorageId,
    pub member_count: usize,
    pub commit_records_staged: usize,
    pub wal_syncs: u64,
    pub first_local_boundary: u64,
    pub last_local_boundary: u64,
}

/// Result of durabilizing one exact staged-Prepare prefix for one storage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoragePrepareBatchReport {
    pub storage_id: StorageId,
    pub member_count: usize,
    pub prepare_records_staged: usize,
    pub wal_syncs: u64,
    /// Heap reports Prepare LSNs and LSM reports WAL record boundaries.
    pub first_local_boundary: u64,
    pub last_local_boundary: u64,
}

/// Result of one storage-local NBCL PreparedChange durability barrier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageChangePrepareBatchReport {
    pub storage_id: StorageId,
    pub changing_member_count: usize,
    pub records_staged: usize,
    pub record_bytes: u64,
    pub syncs: u64,
    pub first_sequence: u64,
    pub last_sequence: u64,
    pub before_frontier: StorageDataVersion,
    /// Reserved by staged records; this is not the committed stream frontier.
    pub after_reserved_frontier: StorageDataVersion,
}

/// Result of one storage-local NBCL Finalize durability barrier and promotion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageChangeFinalizeBatchReport {
    pub storage_id: StorageId,
    pub finalized_member_count: usize,
    pub markers_staged: usize,
    pub marker_bytes: u64,
    pub syncs: u64,
    pub before_committed_frontier: StorageDataVersion,
    pub after_committed_frontier: StorageDataVersion,
}

#[derive(Debug)]
enum StorageTransactionKind {
    Heap(Transaction),
    Lsm(LsmTransaction),
}

impl StorageTransaction {
    fn heap(table_id: TableId, storage_id: StorageId, transaction: Transaction) -> Self {
        Self {
            table_id,
            storage_id,
            inner: StorageTransactionKind::Heap(transaction),
        }
    }

    fn heap_transaction(&self, table_id: TableId) -> Result<&Transaction, StorageError> {
        if self.table_id != table_id {
            return Err(StorageError::Transaction(
                TransactionError::ForeignTransaction { txn_id: self.id() },
            ));
        }
        match &self.inner {
            StorageTransactionKind::Heap(transaction) => Ok(transaction),
            StorageTransactionKind::Lsm(_) => Err(StorageError::Transaction(
                TransactionError::ForeignTransaction { txn_id: self.id() },
            )),
        }
    }

    fn heap_transaction_mut(
        &mut self,
        table_id: TableId,
    ) -> Result<&mut Transaction, StorageError> {
        if self.table_id != table_id {
            return Err(StorageError::Transaction(
                TransactionError::ForeignTransaction { txn_id: self.id() },
            ));
        }
        match &mut self.inner {
            StorageTransactionKind::Heap(transaction) => Ok(transaction),
            StorageTransactionKind::Lsm(transaction) => Err(StorageError::Transaction(
                TransactionError::ForeignTransaction {
                    txn_id: transaction.id(),
                },
            )),
        }
    }

    fn lsm(table_id: TableId, storage_id: StorageId, transaction: LsmTransaction) -> Self {
        Self {
            table_id,
            storage_id,
            inner: StorageTransactionKind::Lsm(transaction),
        }
    }

    fn lsm_transaction(&self, table_id: TableId) -> Result<&LsmTransaction, StorageError> {
        if self.table_id != table_id {
            return Err(StorageError::Transaction(
                TransactionError::ForeignTransaction { txn_id: self.id() },
            ));
        }
        match &self.inner {
            StorageTransactionKind::Lsm(transaction) => Ok(transaction),
            StorageTransactionKind::Heap(transaction) => Err(StorageError::Transaction(
                TransactionError::ForeignTransaction {
                    txn_id: transaction.id(),
                },
            )),
        }
    }

    fn lsm_transaction_mut(
        &mut self,
        table_id: TableId,
    ) -> Result<&mut LsmTransaction, StorageError> {
        if self.table_id != table_id {
            return Err(StorageError::Transaction(
                TransactionError::ForeignTransaction { txn_id: self.id() },
            ));
        }
        match &mut self.inner {
            StorageTransactionKind::Lsm(transaction) => Ok(transaction),
            StorageTransactionKind::Heap(transaction) => Err(StorageError::Transaction(
                TransactionError::ForeignTransaction {
                    txn_id: transaction.id(),
                },
            )),
        }
    }

    #[must_use]
    pub fn id(&self) -> TxnId {
        match &self.inner {
            StorageTransactionKind::Heap(transaction) => transaction.id(),
            StorageTransactionKind::Lsm(transaction) => transaction.id(),
        }
    }

    #[must_use]
    pub fn state(&self) -> TransactionState {
        match &self.inner {
            StorageTransactionKind::Heap(transaction) => transaction.state(),
            StorageTransactionKind::Lsm(transaction) => transaction.state(),
        }
    }

    #[must_use]
    pub fn last_lsn(&self) -> Lsn {
        match &self.inner {
            StorageTransactionKind::Heap(transaction) => transaction.last_lsn(),
            StorageTransactionKind::Lsm(transaction) => transaction.last_lsn(),
        }
    }

    #[must_use]
    pub fn isolation_level(&self) -> IsolationLevel {
        match &self.inner {
            StorageTransactionKind::Heap(transaction) => transaction.isolation_level(),
            StorageTransactionKind::Lsm(transaction) => transaction.isolation_level(),
        }
    }

    pub fn begin_statement(&mut self) -> Result<StorageReadView, StorageError> {
        let table_id = self.table_id;
        match &mut self.inner {
            StorageTransactionKind::Heap(transaction) => Ok(StorageReadView::heap(
                table_id,
                transaction.begin_statement()?,
            )),
            StorageTransactionKind::Lsm(transaction) => Ok(StorageReadView::lsm(
                table_id,
                transaction.begin_statement()?,
            )),
        }
    }

    pub fn begin_statement_at(
        &mut self,
        boundary: StorageVisibilityBoundary,
    ) -> Result<StorageReadView, StorageError> {
        self.validate_visibility_boundary(boundary)?;
        let table_id = self.table_id;
        let horizon = boundary.local_horizon();
        match &mut self.inner {
            StorageTransactionKind::Heap(transaction) => Ok(StorageReadView::heap(
                table_id,
                transaction.begin_statement_at(netbadb_types::CommitSeq(horizon))?,
            )),
            StorageTransactionKind::Lsm(transaction) => Ok(StorageReadView::lsm(
                table_id,
                transaction.begin_statement_at(netbadb_types::LsmCommitSeq(horizon))?,
            )),
        }
    }

    fn validate_visibility_boundary(
        &self,
        boundary: StorageVisibilityBoundary,
    ) -> Result<(), StorageError> {
        let (kind, current) = match &self.inner {
            StorageTransactionKind::Heap(transaction) => {
                (StorageKind::Heap, transaction.current_commit_seq().0)
            }
            StorageTransactionKind::Lsm(transaction) => {
                (StorageKind::Lsm, transaction.current_commit_seq().0)
            }
        };
        validate_visibility_boundary(self.storage_id, kind, current, boundary)
    }

    pub fn current_visibility_boundary(&self) -> Result<StorageVisibilityBoundary, StorageError> {
        match &self.inner {
            StorageTransactionKind::Heap(transaction) => {
                StorageVisibilityBoundary::from_local_horizon(
                    self.storage_id,
                    StorageKind::Heap,
                    transaction.current_commit_seq().0,
                )
            }
            StorageTransactionKind::Lsm(transaction) => {
                StorageVisibilityBoundary::from_local_horizon(
                    self.storage_id,
                    StorageKind::Lsm,
                    transaction.current_commit_seq().0,
                )
            }
        }
    }

    pub fn commit(&mut self) -> Result<(), StorageError> {
        match &mut self.inner {
            StorageTransactionKind::Heap(txn) => txn.commit(),
            StorageTransactionKind::Lsm(txn) => txn.commit(),
        }
    }

    pub fn prepare(&mut self, database_txn_id: DatabaseTxnId) -> Result<(), StorageError> {
        match &mut self.inner {
            StorageTransactionKind::Heap(txn) => txn.prepare(database_txn_id),
            StorageTransactionKind::Lsm(txn) => txn.prepare(database_txn_id),
        }
    }

    /// Group-only staged Prepare. Success freezes the transaction and releases
    /// its writer without claiming authoritative WAL durability.
    #[doc(hidden)]
    pub fn stage_group_prepare(
        &mut self,
        database_txn_id: DatabaseTxnId,
    ) -> Result<(), StorageError> {
        match &mut self.inner {
            StorageTransactionKind::Heap(txn) => txn.stage_group_prepare(database_txn_id),
            StorageTransactionKind::Lsm(txn) => txn.stage_group_prepare(database_txn_id),
        }
    }

    /// Phase 3F group-only staging additionally leaves NBCL PreparedChange
    /// records unsynchronized for the explicit storage-local barrier.
    #[doc(hidden)]
    pub fn stage_group_prepare_with_batched_change_stream(
        &mut self,
        database_txn_id: DatabaseTxnId,
    ) -> Result<(), StorageError> {
        match &mut self.inner {
            StorageTransactionKind::Heap(txn) => {
                txn.stage_group_prepare_with_batched_change_stream(database_txn_id)
            }
            StorageTransactionKind::Lsm(txn) => {
                txn.stage_group_prepare_with_batched_change_stream(database_txn_id)
            }
        }
    }

    pub fn park_prepared(&mut self, database_txn_id: DatabaseTxnId) -> Result<(), StorageError> {
        match &mut self.inner {
            StorageTransactionKind::Heap(txn) => txn.park_prepared(database_txn_id),
            StorageTransactionKind::Lsm(txn) => txn.park_prepared(database_txn_id),
        }
    }

    pub fn commit_prepared(&mut self, database_txn_id: DatabaseTxnId) -> Result<(), StorageError> {
        match &mut self.inner {
            StorageTransactionKind::Heap(txn) => txn.commit_prepared(database_txn_id),
            StorageTransactionKind::Lsm(txn) => txn.commit_prepared(database_txn_id),
        }
    }

    /// Commits an exact parked prefix with one post-decision WAL barrier.
    ///
    /// This deliberately exposes a complete stage/sync/finalize operation,
    /// never a generally callable "commit without sync" primitive.
    pub fn commit_prepared_batch(
        participants: &mut [(&mut Self, DatabaseTxnId)],
    ) -> Result<StorageCommitBatchReport, StorageError> {
        let Some((first, _)) = participants.first() else {
            return Err(TransactionError::EmptyPreparedCommitBatch.into());
        };
        let storage_id = first.storage_id;
        let table_id = first.table_id;
        let heap = matches!(first.inner, StorageTransactionKind::Heap(_));
        if heap {
            let mut batch = Vec::with_capacity(participants.len());
            for (participant, database_txn_id) in participants {
                if participant.storage_id != storage_id || participant.table_id != table_id {
                    return Err(TransactionError::PreparedCommitBatchStorageMismatch.into());
                }
                match &mut participant.inner {
                    StorageTransactionKind::Heap(transaction) => {
                        batch.push((transaction, *database_txn_id));
                    }
                    StorageTransactionKind::Lsm(_) => {
                        return Err(TransactionError::PreparedCommitBatchStorageMismatch.into());
                    }
                }
            }
            let report = Transaction::commit_prepared_batch(&mut batch)?;
            Ok(StorageCommitBatchReport {
                storage_id,
                member_count: report.member_count,
                commit_records_staged: report.commit_records_staged,
                wal_syncs: report.wal_syncs,
                first_local_boundary: report.first_local_boundary,
                last_local_boundary: report.last_local_boundary,
            })
        } else {
            let mut batch = Vec::with_capacity(participants.len());
            for (participant, database_txn_id) in participants {
                if participant.storage_id != storage_id || participant.table_id != table_id {
                    return Err(TransactionError::PreparedCommitBatchStorageMismatch.into());
                }
                match &mut participant.inner {
                    StorageTransactionKind::Lsm(transaction) => {
                        batch.push((transaction, *database_txn_id));
                    }
                    StorageTransactionKind::Heap(_) => {
                        return Err(TransactionError::PreparedCommitBatchStorageMismatch.into());
                    }
                }
            }
            let report = LsmTransaction::commit_prepared_batch(&mut batch)?;
            Ok(StorageCommitBatchReport {
                storage_id,
                member_count: report.member_count,
                commit_records_staged: report.commit_records_staged,
                wal_syncs: report.wal_syncs,
                first_local_boundary: report.first_local_boundary,
                last_local_boundary: report.last_local_boundary,
            })
        }
    }

    /// Makes authoritative commit records durable and applies their local
    /// winner state without publishing grouped Change Stream batches.
    #[doc(hidden)]
    pub fn commit_prepared_batch_authoritative(
        participants: &mut [(&mut Self, DatabaseTxnId)],
    ) -> Result<StorageCommitBatchReport, StorageError> {
        let Some((first, _)) = participants.first() else {
            return Err(TransactionError::EmptyPreparedCommitBatch.into());
        };
        let storage_id = first.storage_id;
        let table_id = first.table_id;
        let heap = matches!(first.inner, StorageTransactionKind::Heap(_));
        if heap {
            let mut batch = Vec::with_capacity(participants.len());
            for (participant, database_txn_id) in participants {
                if participant.storage_id != storage_id || participant.table_id != table_id {
                    return Err(TransactionError::PreparedCommitBatchStorageMismatch.into());
                }
                match &mut participant.inner {
                    StorageTransactionKind::Heap(transaction) => {
                        batch.push((transaction, *database_txn_id));
                    }
                    StorageTransactionKind::Lsm(_) => {
                        return Err(TransactionError::PreparedCommitBatchStorageMismatch.into());
                    }
                }
            }
            let report = Transaction::commit_prepared_batch_authoritative(&mut batch)?;
            Ok(StorageCommitBatchReport {
                storage_id,
                member_count: report.member_count,
                commit_records_staged: report.commit_records_staged,
                wal_syncs: report.wal_syncs,
                first_local_boundary: report.first_local_boundary,
                last_local_boundary: report.last_local_boundary,
            })
        } else {
            let mut batch = Vec::with_capacity(participants.len());
            for (participant, database_txn_id) in participants {
                if participant.storage_id != storage_id || participant.table_id != table_id {
                    return Err(TransactionError::PreparedCommitBatchStorageMismatch.into());
                }
                match &mut participant.inner {
                    StorageTransactionKind::Lsm(transaction) => {
                        batch.push((transaction, *database_txn_id));
                    }
                    StorageTransactionKind::Heap(_) => {
                        return Err(TransactionError::PreparedCommitBatchStorageMismatch.into());
                    }
                }
            }
            let report = LsmTransaction::commit_prepared_batch_authoritative(&mut batch)?;
            Ok(StorageCommitBatchReport {
                storage_id,
                member_count: report.member_count,
                commit_records_staged: report.commit_records_staged,
                wal_syncs: report.wal_syncs,
                first_local_boundary: report.first_local_boundary,
                last_local_boundary: report.last_local_boundary,
            })
        }
    }

    /// Durabilizes and promotes the exact Change Stream finalize subsequence.
    #[doc(hidden)]
    pub fn finalize_group_changes_batch(
        participants: &mut [(&mut Self, DatabaseTxnId)],
    ) -> Result<Option<StorageChangeFinalizeBatchReport>, StorageError> {
        let Some((first, _)) = participants.first() else {
            return Err(TransactionError::EmptyPreparedCommitBatch.into());
        };
        let storage_id = first.storage_id;
        let table_id = first.table_id;
        let heap = matches!(first.inner, StorageTransactionKind::Heap(_));
        let report = if heap {
            let mut batch = Vec::with_capacity(participants.len());
            for (participant, database_txn_id) in participants {
                if participant.storage_id != storage_id || participant.table_id != table_id {
                    return Err(TransactionError::PreparedCommitBatchStorageMismatch.into());
                }
                match &mut participant.inner {
                    StorageTransactionKind::Heap(transaction) => {
                        batch.push((transaction, *database_txn_id));
                    }
                    StorageTransactionKind::Lsm(_) => {
                        return Err(TransactionError::PreparedCommitBatchStorageMismatch.into());
                    }
                }
            }
            Transaction::finalize_group_changes_batch(&mut batch)?
        } else {
            let mut batch = Vec::with_capacity(participants.len());
            for (participant, database_txn_id) in participants {
                if participant.storage_id != storage_id || participant.table_id != table_id {
                    return Err(TransactionError::PreparedCommitBatchStorageMismatch.into());
                }
                match &mut participant.inner {
                    StorageTransactionKind::Lsm(transaction) => {
                        batch.push((transaction, *database_txn_id));
                    }
                    StorageTransactionKind::Heap(_) => {
                        return Err(TransactionError::PreparedCommitBatchStorageMismatch.into());
                    }
                }
            }
            LsmTransaction::finalize_group_changes_batch(&mut batch)?
        };
        Ok(report.map(|report| StorageChangeFinalizeBatchReport {
            storage_id,
            finalized_member_count: report.finalized_member_count,
            markers_staged: report.markers_staged,
            marker_bytes: report.marker_bytes,
            syncs: report.syncs,
            before_committed_frontier: report.before_committed_frontier,
            after_committed_frontier: report.after_committed_frontier,
        }))
    }

    /// Durabilizes the exact parked staged-Prepare prefix with one WAL barrier.
    #[doc(hidden)]
    pub fn durabilize_group_prepare_batch(
        participants: &mut [(&mut Self, DatabaseTxnId)],
    ) -> Result<StoragePrepareBatchReport, StorageError> {
        let Some((first, _)) = participants.first() else {
            return Err(TransactionError::EmptyPreparedPrepareBatch.into());
        };
        let storage_id = first.storage_id;
        let table_id = first.table_id;
        let heap = matches!(first.inner, StorageTransactionKind::Heap(_));
        if heap {
            let mut batch = Vec::with_capacity(participants.len());
            for (participant, database_txn_id) in participants {
                if participant.storage_id != storage_id || participant.table_id != table_id {
                    return Err(TransactionError::PreparedPrepareBatchStorageMismatch.into());
                }
                match &mut participant.inner {
                    StorageTransactionKind::Heap(transaction) => {
                        batch.push((transaction, *database_txn_id));
                    }
                    StorageTransactionKind::Lsm(_) => {
                        return Err(TransactionError::PreparedPrepareBatchStorageMismatch.into());
                    }
                }
            }
            let report = Transaction::durabilize_group_prepare_batch(&mut batch)?;
            Ok(StoragePrepareBatchReport {
                storage_id,
                member_count: report.member_count,
                prepare_records_staged: report.prepare_records_staged,
                wal_syncs: report.wal_syncs,
                first_local_boundary: report.first_local_boundary,
                last_local_boundary: report.last_local_boundary,
            })
        } else {
            let mut batch = Vec::with_capacity(participants.len());
            for (participant, database_txn_id) in participants {
                if participant.storage_id != storage_id || participant.table_id != table_id {
                    return Err(TransactionError::PreparedPrepareBatchStorageMismatch.into());
                }
                match &mut participant.inner {
                    StorageTransactionKind::Lsm(transaction) => {
                        batch.push((transaction, *database_txn_id));
                    }
                    StorageTransactionKind::Heap(_) => {
                        return Err(TransactionError::PreparedPrepareBatchStorageMismatch.into());
                    }
                }
            }
            let report = LsmTransaction::durabilize_group_prepare_batch(&mut batch)?;
            Ok(StoragePrepareBatchReport {
                storage_id,
                member_count: report.member_count,
                prepare_records_staged: report.prepare_records_staged,
                wal_syncs: report.wal_syncs,
                first_local_boundary: report.first_local_boundary,
                last_local_boundary: report.last_local_boundary,
            })
        }
    }

    /// Durabilizes the exact staged NBCL PreparedChange subsequence.
    #[doc(hidden)]
    pub fn durabilize_group_change_prepare_batch(
        participants: &mut [(&mut Self, DatabaseTxnId)],
    ) -> Result<Option<StorageChangePrepareBatchReport>, StorageError> {
        let Some((first, _)) = participants.first() else {
            return Err(TransactionError::EmptyPreparedPrepareBatch.into());
        };
        let storage_id = first.storage_id;
        let table_id = first.table_id;
        let heap = matches!(first.inner, StorageTransactionKind::Heap(_));
        let report = if heap {
            let mut batch = Vec::with_capacity(participants.len());
            for (participant, database_txn_id) in participants {
                if participant.storage_id != storage_id || participant.table_id != table_id {
                    return Err(TransactionError::PreparedPrepareBatchStorageMismatch.into());
                }
                match &mut participant.inner {
                    StorageTransactionKind::Heap(transaction) => {
                        batch.push((transaction, *database_txn_id));
                    }
                    StorageTransactionKind::Lsm(_) => {
                        return Err(TransactionError::PreparedPrepareBatchStorageMismatch.into());
                    }
                }
            }
            Transaction::durabilize_group_change_prepare_batch(&mut batch)?
        } else {
            let mut batch = Vec::with_capacity(participants.len());
            for (participant, database_txn_id) in participants {
                if participant.storage_id != storage_id || participant.table_id != table_id {
                    return Err(TransactionError::PreparedPrepareBatchStorageMismatch.into());
                }
                match &mut participant.inner {
                    StorageTransactionKind::Lsm(transaction) => {
                        batch.push((transaction, *database_txn_id));
                    }
                    StorageTransactionKind::Heap(_) => {
                        return Err(TransactionError::PreparedPrepareBatchStorageMismatch.into());
                    }
                }
            }
            LsmTransaction::durabilize_group_change_prepare_batch(&mut batch)?
        };
        Ok(report.map(|report| StorageChangePrepareBatchReport {
            storage_id,
            changing_member_count: report.changing_member_count,
            records_staged: report.records_staged,
            record_bytes: report.record_bytes,
            syncs: report.syncs,
            first_sequence: report.first_sequence,
            last_sequence: report.last_sequence,
            before_frontier: report.before_frontier,
            after_reserved_frontier: report.after_reserved_frontier,
        }))
    }

    pub fn rollback_prepared(
        &mut self,
        database_txn_id: DatabaseTxnId,
    ) -> Result<(), StorageError> {
        match &mut self.inner {
            StorageTransactionKind::Heap(txn) => txn.rollback_prepared(database_txn_id),
            StorageTransactionKind::Lsm(txn) => txn.rollback_prepared(database_txn_id),
        }
    }

    pub fn rollback(&mut self) -> Result<(), StorageError> {
        match &mut self.inner {
            StorageTransactionKind::Heap(txn) => txn.rollback(),
            StorageTransactionKind::Lsm(txn) => txn.rollback(),
        }
    }

    pub fn abort(&mut self) -> Result<(), StorageError> {
        self.rollback()
    }
}

/// One table's physical storage/layout implementation.
///
/// B+Trees are registered access methods owned by the Heap variant; they are
/// deliberately not variants of this enum. New table layouts can extend this
/// enum without changing planner or executor storage interfaces.
#[derive(Debug)]
pub enum TableStorage {
    Heap(Box<HeapStorage>),
    Lsm(LsmStorage),
}

impl From<HeapStorage> for TableStorage {
    fn from(storage: HeapStorage) -> Self {
        Self::Heap(Box::new(storage))
    }
}

impl From<LsmStorage> for TableStorage {
    fn from(storage: LsmStorage) -> Self {
        Self::Lsm(storage)
    }
}

impl TableStorage {
    #[must_use]
    pub fn prepared_runtime_inspection(&self) -> crate::PreparedRuntimeInspection {
        match self {
            Self::Heap(storage) => storage.prepared_runtime_inspection(),
            Self::Lsm(storage) => storage.prepared_runtime_inspection(),
        }
    }

    #[must_use]
    pub const fn kind(&self) -> StorageKind {
        match self {
            Self::Heap(_) => StorageKind::Heap,
            Self::Lsm(_) => StorageKind::Lsm,
        }
    }

    #[must_use]
    pub fn lsm_inspection(&self) -> Option<LsmInspection> {
        match self {
            Self::Heap(_) => None,
            Self::Lsm(storage) => Some(storage.inspection()),
        }
    }

    pub fn lsm_maintenance_inspection(
        &self,
    ) -> Result<Option<crate::LsmMaintenanceInspection>, StorageError> {
        match self {
            Self::Heap(_) => Ok(None),
            Self::Lsm(storage) => storage.maintenance_inspection().map(Some),
        }
    }
    pub fn create_heap(path: impl AsRef<Path>, table: TableDef) -> Result<Self, StorageError> {
        HeapStorage::create(path, table)
            .map(Box::new)
            .map(Self::Heap)
    }

    pub fn create_heap_with_storage_id(
        path: impl AsRef<Path>,
        table: TableDef,
        storage_id: StorageId,
    ) -> Result<Self, StorageError> {
        HeapStorage::create_with_storage_id(path, table, storage_id)
            .map(Box::new)
            .map(Self::Heap)
    }

    pub fn open_heap(path: impl AsRef<Path>, table: TableDef) -> Result<Self, StorageError> {
        HeapStorage::open(path, table).map(Box::new).map(Self::Heap)
    }

    pub fn open_heap_with_prepared_resolutions(
        path: impl AsRef<Path>,
        table: TableDef,
        resolutions: &[PreparedTxnResolution],
    ) -> Result<Self, StorageError> {
        HeapStorage::open_with_prepared_resolutions(path, table, resolutions)
            .map(Box::new)
            .map(Self::Heap)
    }

    pub fn inspect_heap_recovery(
        path: impl AsRef<Path>,
        table: &TableDef,
    ) -> Result<HeapRecoveryInspection, StorageError> {
        HeapStorage::inspect_recovery(path, table)
    }

    pub fn inspect_heap_identity(
        path: impl AsRef<Path>,
    ) -> Result<HeapIdentityInspection, StorageError> {
        HeapStorage::inspect_identity(path)
    }

    pub fn create_lsm(
        root: impl AsRef<Path>,
        table: TableDef,
        clustering_column: ColumnId,
    ) -> Result<Self, StorageError> {
        LsmStorage::create(root, table, clustering_column).map(Self::Lsm)
    }

    pub fn create_lsm_with_storage_id(
        root: impl AsRef<Path>,
        table: TableDef,
        clustering_column: ColumnId,
        storage_id: StorageId,
    ) -> Result<Self, StorageError> {
        LsmStorage::create_with_storage_id(root, table, clustering_column, storage_id)
            .map(Self::Lsm)
    }

    pub fn open_lsm(root: impl AsRef<Path>, table: TableDef) -> Result<Self, StorageError> {
        LsmStorage::open(root, table).map(Self::Lsm)
    }

    pub fn open_lsm_with_prepared_resolutions(
        root: impl AsRef<Path>,
        table: TableDef,
        resolutions: &[PreparedTxnResolution],
    ) -> Result<Self, StorageError> {
        LsmStorage::open_with_prepared_resolutions(root, table, resolutions).map(Self::Lsm)
    }

    pub fn inspect_lsm_recovery(
        root: impl AsRef<Path>,
        table: &TableDef,
    ) -> Result<LsmRecoveryInspection, StorageError> {
        LsmStorage::inspect_recovery(root, table)
    }

    pub fn inspect_lsm_identity(
        root: impl AsRef<Path>,
    ) -> Result<LsmIdentityInspection, StorageError> {
        LsmStorage::inspect_identity(root)
    }

    #[must_use]
    pub fn storage_id(&self) -> StorageId {
        match self {
            Self::Heap(storage) => storage.storage_id(),
            Self::Lsm(storage) => storage.storage_id(),
        }
    }

    #[must_use]
    pub fn table(&self) -> &TableDef {
        match self {
            Self::Heap(storage) => storage.table(),
            Self::Lsm(storage) => storage.table(),
        }
    }

    /// Retargets metadata on a private staged Heap without rewriting rows or
    /// indexes.  Non-Heap storage is intentionally not eligible.
    pub fn retarget_private_schema(
        &mut self,
        expected: &TableDef,
        target: TableDef,
    ) -> Result<(), StorageError> {
        match self {
            Self::Heap(storage) => storage.retarget_private_schema(expected, target),
            Self::Lsm(_) => Err(StorageError::UnsupportedOperation {
                operation: "private Heap schema retarget",
                storage_kind: "LSM",
            }),
        }
    }

    pub fn read_view(&self) -> Result<StorageReadView, StorageError> {
        match self {
            Self::Heap(storage) => Ok(StorageReadView::heap(
                storage.table().id,
                storage.read_view()?,
            )),
            Self::Lsm(storage) => Ok(StorageReadView::lsm(
                storage.table().id,
                storage.read_view()?,
            )),
        }
    }

    /// Captures the latest committed boundary for this exact storage.
    pub fn current_visibility_boundary(&self) -> Result<StorageVisibilityBoundary, StorageError> {
        match self {
            Self::Heap(storage) => StorageVisibilityBoundary::from_local_horizon(
                storage.storage_id(),
                StorageKind::Heap,
                storage.current_commit_seq().0,
            ),
            Self::Lsm(storage) => StorageVisibilityBoundary::from_local_horizon(
                storage.storage_id(),
                StorageKind::Lsm,
                storage.current_commit_seq().0,
            ),
        }
    }

    /// Opens a committed read view at a previously captured local boundary.
    pub fn read_view_at(
        &self,
        boundary: StorageVisibilityBoundary,
    ) -> Result<StorageReadView, StorageError> {
        let current = self.current_visibility_boundary()?;
        validate_visibility_boundary(
            current.storage_id,
            current.storage_kind,
            current.local_horizon(),
            boundary,
        )?;
        match self {
            Self::Heap(storage) => Ok(StorageReadView::heap(
                storage.table().id,
                storage.read_view_at(netbadb_types::CommitSeq(boundary.local_horizon()))?,
            )),
            Self::Lsm(storage) => Ok(StorageReadView::lsm(
                storage.table().id,
                storage.read_view_at(netbadb_types::LsmCommitSeq(boundary.local_horizon()))?,
            )),
        }
    }

    /// Pins history at `boundary` without caching or decoding row payloads.
    pub fn pin_visibility_boundary(
        &self,
        boundary: StorageVisibilityBoundary,
    ) -> Result<StorageVisibilityPin, StorageError> {
        Ok(StorageVisibilityPin {
            boundary,
            _view: self.read_view_at(boundary)?,
        })
    }

    pub fn enable_change_stream(&mut self) -> Result<crate::ChangeStreamCursor, StorageError> {
        match self {
            Self::Heap(storage) => storage.enable_change_stream(),
            Self::Lsm(storage) => storage.enable_change_stream(),
        }
    }

    pub fn disable_change_stream(&mut self) -> Result<(), StorageError> {
        match self {
            Self::Heap(storage) => storage.disable_change_stream(),
            Self::Lsm(storage) => storage.disable_change_stream(),
        }
    }

    pub fn change_stream_cursor(&self) -> Result<crate::ChangeStreamCursor, StorageError> {
        match self {
            Self::Heap(storage) => storage.change_stream_cursor(),
            Self::Lsm(storage) => storage.change_stream_cursor(),
        }
    }

    pub fn committed_read_anchor(&self) -> Result<CommittedReadAnchor, StorageError> {
        let read_view = self.read_view()?;
        let cursor = self.change_stream_cursor()?;
        Ok(CommittedReadAnchor { read_view, cursor })
    }

    pub fn read_changes(
        &self,
        cursor: crate::ChangeStreamCursor,
        max_batches: usize,
        max_bytes: u64,
    ) -> Result<crate::ChangeReadResult, StorageError> {
        match self {
            Self::Heap(storage) => storage.read_changes(cursor, max_batches, max_bytes),
            Self::Lsm(storage) => storage.read_changes(cursor, max_batches, max_bytes),
        }
    }

    pub fn acquire_change_stream_retention_pin(
        &self,
        cursor: crate::ChangeStreamCursor,
    ) -> Result<crate::ChangeStreamRetentionPin, StorageError> {
        match self {
            Self::Heap(storage) => storage.acquire_change_stream_retention_pin(cursor),
            Self::Lsm(storage) => storage.acquire_change_stream_retention_pin(cursor),
        }
    }

    pub fn advance_change_stream_retention_pin(
        &self,
        pin: &mut crate::ChangeStreamRetentionPin,
        frontier: netbadb_types::StorageDataVersion,
    ) -> Result<(), StorageError> {
        match self {
            Self::Heap(storage) => storage.advance_change_stream_retention_pin(pin, frontier),
            Self::Lsm(storage) => storage.advance_change_stream_retention_pin(pin, frontier),
        }
    }

    pub fn gc_change_stream(
        &mut self,
        frontier: netbadb_types::StorageDataVersion,
    ) -> Result<crate::ChangeStreamGcStorageReport, StorageError> {
        match self {
            Self::Heap(storage) => storage.gc_change_stream(frontier),
            Self::Lsm(storage) => storage.gc_change_stream(frontier),
        }
    }

    #[must_use]
    pub fn inspect_change_stream(&self) -> crate::ChangeStreamInspection {
        match self {
            Self::Heap(storage) => storage.change_stream_inspection(),
            Self::Lsm(storage) => storage.change_stream_inspection(),
        }
    }

    /// Returns small record metadata for maintenance policy without cloning or
    /// decoding committed row payloads.
    #[must_use]
    pub fn inspect_change_stream_maintenance(&self) -> crate::ChangeStreamMaintenanceInspection {
        match self {
            Self::Heap(storage) => storage.change_stream_maintenance_inspection(),
            Self::Lsm(storage) => storage.change_stream_maintenance_inspection(),
        }
    }

    /// Captures an equality-only committed horizon for this exact storage.
    pub fn snapshot_token(
        &self,
        view: &StorageReadView,
    ) -> Result<StorageSnapshotToken, StorageError> {
        let table_id = self.table().id;
        match self {
            Self::Heap(storage) => Ok(StorageSnapshotToken::heap(
                storage.storage_id(),
                view.heap_view(table_id)?.snapshot().visible_csn.0,
            )),
            Self::Lsm(storage) => {
                let _ = view.lsm_view(table_id)?;
                let (epoch, sequence) = storage.projection_snapshot_parts()?;
                Ok(StorageSnapshotToken::lsm(
                    storage.storage_id(),
                    epoch,
                    sequence,
                ))
            }
        }
    }

    /// Returns the latest committed horizon without creating a cross-storage order.
    pub fn current_snapshot_token(&self) -> Result<StorageSnapshotToken, StorageError> {
        let view = self.read_view()?;
        self.snapshot_token(&view)
    }

    pub fn begin_transaction(&mut self) -> Result<StorageTransaction, StorageError> {
        self.begin_transaction_with_isolation(IsolationLevel::ReadCommitted)
    }

    pub fn begin_transaction_with_isolation(
        &mut self,
        isolation_level: IsolationLevel,
    ) -> Result<StorageTransaction, StorageError> {
        match self {
            Self::Heap(storage) => {
                let table_id = storage.table().id;
                let transaction = storage.begin_transaction_with_isolation(isolation_level)?;
                Ok(StorageTransaction::heap(
                    table_id,
                    storage.storage_id(),
                    transaction,
                ))
            }
            Self::Lsm(storage) => {
                let table_id = storage.table().id;
                Ok(StorageTransaction::lsm(
                    table_id,
                    storage.storage_id(),
                    storage.begin_transaction_with_isolation(isolation_level)?,
                ))
            }
        }
    }

    pub fn validate_transaction(
        &self,
        transaction: &StorageTransaction,
    ) -> Result<(), StorageError> {
        match self {
            Self::Heap(storage) => {
                storage.validate_transaction(transaction.heap_transaction(storage.table().id)?)
            }
            Self::Lsm(storage) => {
                storage.validate_transaction(transaction.lsm_transaction(storage.table().id)?)
            }
        }
    }

    pub fn insert(&mut self, values: &[ScalarValue]) -> Result<StorageRowHandle, StorageError> {
        match self {
            Self::Heap(storage) => {
                let table_id = storage.table().id;
                storage
                    .insert(values)
                    .map(|row_id| StorageRowHandle::heap(table_id, storage.storage_id(), row_id))
            }
            Self::Lsm(storage) => {
                let table_id = storage.table().id;
                storage
                    .insert(values)
                    .map(|row| StorageRowHandle::lsm(table_id, storage.storage_id(), row))
            }
        }
    }

    pub fn update(
        &mut self,
        row: StorageRowHandle,
        values: &[ScalarValue],
    ) -> Result<StorageRowHandle, StorageError> {
        match self {
            Self::Heap(storage) => {
                let table_id = storage.table().id;
                let current =
                    storage.update(row.heap_row_id(table_id, storage.storage_id())?, values)?;
                Ok(StorageRowHandle::heap(
                    table_id,
                    storage.storage_id(),
                    current,
                ))
            }
            Self::Lsm(storage) => {
                let table_id = storage.table().id;
                let updated =
                    storage.update(row.lsm_handle(table_id, storage.storage_id())?, values)?;
                Ok(StorageRowHandle::lsm(
                    table_id,
                    storage.storage_id(),
                    updated,
                ))
            }
        }
    }

    pub fn delete(&mut self, row: StorageRowHandle) -> Result<(), StorageError> {
        match self {
            Self::Heap(storage) => {
                let table_id = storage.table().id;
                storage.delete(row.heap_row_id(table_id, storage.storage_id())?)
            }
            Self::Lsm(storage) => {
                let table_id = storage.table().id;
                storage.delete(row.lsm_handle(table_id, storage.storage_id())?)
            }
        }
    }

    pub fn insert_in(
        &mut self,
        transaction: &mut StorageTransaction,
        values: &[ScalarValue],
    ) -> Result<StorageRowHandle, StorageError> {
        match self {
            Self::Heap(storage) => {
                let table_id = storage.table().id;
                let row_id =
                    storage.insert_in(transaction.heap_transaction_mut(table_id)?, values)?;
                Ok(StorageRowHandle::heap(
                    table_id,
                    storage.storage_id(),
                    row_id,
                ))
            }
            Self::Lsm(storage) => {
                let table_id = storage.table().id;
                let row = storage.insert_in(transaction.lsm_transaction_mut(table_id)?, values)?;
                Ok(StorageRowHandle::lsm(table_id, storage.storage_id(), row))
            }
        }
    }

    pub fn update_in(
        &mut self,
        transaction: &mut StorageTransaction,
        row: StorageRowHandle,
        values: &[ScalarValue],
    ) -> Result<StorageRowHandle, StorageError> {
        match self {
            Self::Heap(storage) => {
                let table_id = storage.table().id;
                let row_id = row.heap_row_id(table_id, storage.storage_id())?;
                let current = storage.update_in(
                    transaction.heap_transaction_mut(table_id)?,
                    row_id,
                    values,
                )?;
                Ok(StorageRowHandle::heap(
                    table_id,
                    storage.storage_id(),
                    current,
                ))
            }
            Self::Lsm(storage) => {
                let table_id = storage.table().id;
                let row = row.lsm_handle(table_id, storage.storage_id())?;
                let updated =
                    storage.update_in(transaction.lsm_transaction_mut(table_id)?, row, values)?;
                Ok(StorageRowHandle::lsm(
                    table_id,
                    storage.storage_id(),
                    updated,
                ))
            }
        }
    }

    pub fn delete_in(
        &mut self,
        transaction: &mut StorageTransaction,
        row: StorageRowHandle,
    ) -> Result<(), StorageError> {
        match self {
            Self::Heap(storage) => {
                let table_id = storage.table().id;
                storage.delete_in(
                    transaction.heap_transaction_mut(table_id)?,
                    row.heap_row_id(table_id, storage.storage_id())?,
                )
            }
            Self::Lsm(storage) => {
                let table_id = storage.table().id;
                let row = row.lsm_handle(table_id, storage.storage_id())?;
                storage.delete_in(transaction.lsm_transaction_mut(table_id)?, row)
            }
        }
    }

    pub fn scan_columns_with_view(
        &mut self,
        columns: &[ColumnId],
        view: &StorageReadView,
    ) -> Result<Vec<(StorageRowHandle, Vec<ScalarValue>)>, StorageError> {
        match self {
            Self::Heap(storage) => {
                let table_id = storage.table().id;
                let view = view.heap_view(table_id)?;
                Ok(storage
                    .scan_columns_with_view(columns, view)?
                    .into_iter()
                    .map(|(row_id, values)| {
                        (
                            StorageRowHandle::heap(table_id, storage.storage_id(), row_id),
                            values,
                        )
                    })
                    .collect())
            }
            Self::Lsm(storage) => {
                let table_id = storage.table().id;
                Ok(storage
                    .scan_columns_with_view(columns, view.lsm_view(table_id)?)?
                    .into_iter()
                    .map(|(row, values)| {
                        (
                            StorageRowHandle::lsm(table_id, storage.storage_id(), row),
                            values,
                        )
                    })
                    .collect())
            }
        }
    }

    /// Scans projected values together with their exact committed physical
    /// version identity. The identity is storage metadata, not a SQL column.
    pub fn scan_versioned_columns_with_view(
        &mut self,
        columns: &[ColumnId],
        view: &StorageReadView,
    ) -> Result<Vec<(crate::StorageVersionKey, Vec<ScalarValue>)>, StorageError> {
        self.scan_columns_with_view(columns, view)?
            .into_iter()
            .map(|(handle, values)| Ok((handle.committed_version_key()?, values)))
            .collect()
    }

    /// Produces validated visible rows synchronously until the consumer breaks.
    ///
    /// Requested columns retain order and duplicates, including an empty
    /// projection. Values and the opaque row handle are owned so callers may
    /// retain a bounded group after the callback returns. `Break` is successful
    /// cancellation and prevents storage from requesting later rows.
    pub fn visit_rows_with_view_control<E, F>(
        &mut self,
        columns: &[ColumnId],
        view: &StorageReadView,
        mut visitor: F,
    ) -> Result<ControlFlow<()>, E>
    where
        E: From<StorageError>,
        F: FnMut(StorageRowHandle, Vec<ScalarValue>) -> Result<ControlFlow<()>, E>,
    {
        match self {
            Self::Heap(storage) => {
                let table_id = storage.table().id;
                let storage_id = storage.storage_id();
                let view = view.heap_view(table_id).map_err(E::from)?;
                storage.visit_row_scalar_refs_with_presence_view_control(
                    columns,
                    &[],
                    view,
                    |row_id, values, _presence| {
                        visitor(
                            StorageRowHandle::heap(table_id, storage_id, row_id),
                            values.iter().copied().map(ScalarRef::to_owned).collect(),
                        )
                    },
                )
            }
            Self::Lsm(storage) => {
                let table_id = storage.table().id;
                let storage_id = storage.storage_id();
                let view = view.lsm_view(table_id).map_err(E::from)?;
                storage.visit_columns_with_view_control(columns, view, |row, values| {
                    visitor(StorageRowHandle::lsm(table_id, storage_id, row), values)
                })
            }
        }
    }

    pub fn point_lookup_columns_with_view(
        &mut self,
        access_path: AccessPathId,
        key: &ScalarValue,
        columns: &[ColumnId],
        view: &StorageReadView,
    ) -> Result<Vec<(StorageRowHandle, Vec<ScalarValue>)>, StorageError> {
        match self {
            Self::Heap(storage) => {
                let table_id = storage.table().id;
                let view = view.heap_view(table_id)?;
                let handle = registered_handle(storage, access_path)?;
                let candidates = storage.btree().lookup(handle, key)?;
                let mut rows = Vec::new();
                for row_id in candidates {
                    if let Some(values) =
                        storage.read_row_columns_with_view(row_id, columns, view)?
                    {
                        rows.push((
                            StorageRowHandle::heap(table_id, storage.storage_id(), row_id),
                            values,
                        ));
                    }
                }
                Ok(rows)
            }
            Self::Lsm(storage) => {
                let table_id = storage.table().id;
                if access_path != storage.access_path_id() {
                    return Err(StorageError::UnknownAccessPath {
                        table_id,
                        access_path,
                    });
                }
                Ok(storage
                    .point_lookup_columns_with_view(key, columns, view.lsm_view(table_id)?)?
                    .into_iter()
                    .map(|(row, values)| {
                        (
                            StorageRowHandle::lsm(table_id, storage.storage_id(), row),
                            values,
                        )
                    })
                    .collect())
            }
        }
    }

    pub fn range_lookup_columns_with_view(
        &mut self,
        access_path: AccessPathId,
        range: &IndexRange,
        columns: &[ColumnId],
        view: &StorageReadView,
    ) -> Result<Vec<(StorageRowHandle, Vec<ScalarValue>)>, StorageError> {
        match self {
            Self::Heap(storage) => {
                let table_id = storage.table().id;
                let view = view.heap_view(table_id)?;
                let handle = registered_handle(storage, access_path)?;
                let candidates = storage.btree().lookup_range(handle, range)?;
                let mut rows = Vec::new();
                for row_id in candidates {
                    if let Some(values) =
                        storage.read_row_columns_with_view(row_id, columns, view)?
                    {
                        rows.push((
                            StorageRowHandle::heap(table_id, storage.storage_id(), row_id),
                            values,
                        ));
                    }
                }
                Ok(rows)
            }
            Self::Lsm(storage) => {
                let table_id = storage.table().id;
                if access_path != storage.access_path_id() {
                    return Err(StorageError::UnknownAccessPath {
                        table_id,
                        access_path,
                    });
                }
                Ok(storage
                    .range_lookup_columns_with_view(range, columns, view.lsm_view(table_id)?)?
                    .into_iter()
                    .map(|(row, values)| {
                        (
                            StorageRowHandle::lsm(table_id, storage.storage_id(), row),
                            values,
                        )
                    })
                    .collect())
            }
        }
    }

    pub fn scan_presence_counts_with_view(
        &mut self,
        columns: &[ColumnId],
        view: &StorageReadView,
    ) -> Result<PresenceCountSummary, StorageError> {
        match self {
            Self::Heap(storage) => {
                storage.scan_presence_counts_with_view(columns, view.heap_view(storage.table().id)?)
            }
            Self::Lsm(storage) => {
                storage.scan_presence_counts_with_view(columns, view.lsm_view(storage.table().id)?)
            }
        }
    }

    pub fn visit_scalar_refs_with_presence_view<E, F>(
        &mut self,
        value_columns: &[ColumnId],
        presence_columns: &[ColumnId],
        view: &StorageReadView,
        mut visitor: F,
    ) -> Result<(), E>
    where
        E: From<StorageError>,
        F: for<'row> FnMut(&[ScalarRef<'row>], &[bool]) -> Result<(), E>,
    {
        match self {
            Self::Heap(storage) => {
                let view = view.heap_view(storage.table().id).map_err(E::from)?;
                storage.visit_scalar_refs_with_presence_view(
                    value_columns,
                    presence_columns,
                    view,
                    visitor,
                )
            }
            Self::Lsm(storage) => {
                let table_id = storage.table().id;
                let view = view.lsm_view(table_id).map_err(E::from)?;
                let requested = value_columns
                    .iter()
                    .chain(presence_columns)
                    .copied()
                    .collect::<Vec<_>>();
                let rows = storage
                    .scan_columns_with_view(&requested, view)
                    .map_err(E::from)?;
                for (_, values) in rows {
                    let split = value_columns.len();
                    let scalar_refs = values[..split]
                        .iter()
                        .map(ScalarRef::from)
                        .collect::<Vec<_>>();
                    let presence = values[split..]
                        .iter()
                        .map(|value| !matches!(value, ScalarValue::Null))
                        .collect::<Vec<_>>();
                    visitor(&scalar_refs, &presence)?;
                }
                Ok(())
            }
        }
    }

    pub fn visit_row_scalar_refs_with_presence_view<E, F>(
        &mut self,
        value_columns: &[ColumnId],
        presence_columns: &[ColumnId],
        view: &StorageReadView,
        mut visitor: F,
    ) -> Result<(), E>
    where
        E: From<StorageError>,
        F: for<'row> FnMut(StorageRowHandle, &[ScalarRef<'row>], &[bool]) -> Result<(), E>,
    {
        match self {
            Self::Heap(storage) => {
                let table_id = storage.table().id;
                let storage_id = storage.storage_id();
                let view = view.heap_view(table_id).map_err(E::from)?;
                storage.visit_row_scalar_refs_with_presence_view(
                    value_columns,
                    presence_columns,
                    view,
                    |row_id, values, presence| {
                        visitor(
                            StorageRowHandle::heap(table_id, storage_id, row_id),
                            values,
                            presence,
                        )
                    },
                )
            }
            Self::Lsm(storage) => {
                let table_id = storage.table().id;
                let storage_id = storage.storage_id();
                let view = view.lsm_view(table_id).map_err(E::from)?;
                let requested = value_columns
                    .iter()
                    .chain(presence_columns)
                    .copied()
                    .collect::<Vec<_>>();
                let rows = storage
                    .scan_columns_with_view(&requested, view)
                    .map_err(E::from)?;
                for (row, values) in rows {
                    let split = value_columns.len();
                    let scalar_refs = values[..split]
                        .iter()
                        .map(ScalarRef::from)
                        .collect::<Vec<_>>();
                    let presence = values[split..]
                        .iter()
                        .map(|value| !matches!(value, ScalarValue::Null))
                        .collect::<Vec<_>>();
                    visitor(
                        StorageRowHandle::lsm(table_id, storage_id, row),
                        &scalar_refs,
                        &presence,
                    )?;
                }
                Ok(())
            }
        }
    }

    #[must_use]
    pub fn indexes(&self) -> &[IndexDefinition] {
        match self {
            Self::Heap(storage) => storage.indexes(),
            Self::Lsm(_) => &[],
        }
    }

    #[must_use]
    pub fn access_paths(&self) -> Vec<StorageAccessPath> {
        match self {
            Self::Heap(storage) => storage
                .indexes()
                .iter()
                .map(|definition| StorageAccessPath {
                    id: access_path_id(definition.handle),
                    column_id: definition.column_id,
                    capabilities: AccessPathCapabilities {
                        point_lookup: true,
                        range_lookup: true,
                        ordered: true,
                    },
                    statistics: storage.index_statistics(definition.column_id),
                    cost_hints: None,
                })
                .collect(),
            Self::Lsm(storage) => vec![StorageAccessPath {
                id: storage.access_path_id(),
                column_id: storage.clustering_column(),
                capabilities: AccessPathCapabilities {
                    point_lookup: true,
                    range_lookup: true,
                    ordered: true,
                },
                statistics: storage.access_statistics(),
                cost_hints: Some(storage.access_cost_hints()),
            }],
        }
    }

    #[must_use]
    pub fn table_statistics(&self) -> Option<TableStatistics> {
        match self {
            Self::Heap(storage) => storage.table_statistics(),
            Self::Lsm(storage) => storage.table_statistics(),
        }
    }

    #[must_use]
    pub fn index_statistics(&self, column_id: ColumnId) -> Option<IndexStatistics> {
        match self {
            Self::Heap(storage) => storage.index_statistics(column_id),
            Self::Lsm(storage) if storage.clustering_column() == column_id => {
                storage.access_statistics()
            }
            Self::Lsm(_) => None,
        }
    }

    pub fn create_index(&mut self, column_id: ColumnId) -> Result<IndexDefinition, StorageError> {
        match self {
            Self::Heap(storage) => storage.create_index(column_id),
            Self::Lsm(_) => Err(StorageError::UnsupportedOperation {
                operation: "create B+Tree access method",
                storage_kind: "LSM",
            }),
        }
    }

    pub fn create_named_index(
        &mut self,
        name: IndexName,
        column_id: ColumnId,
    ) -> Result<IndexDefinition, StorageError> {
        match self {
            Self::Heap(storage) => storage.create_named_index(name, column_id),
            Self::Lsm(_) => Err(StorageError::UnsupportedOperation {
                operation: "create B+Tree access method",
                storage_kind: "LSM",
            }),
        }
    }

    pub fn create_index_from_floor(
        &mut self,
        name: Option<IndexName>,
        column_id: ColumnId,
        floor: IndexId,
    ) -> Result<IndexDefinition, StorageError> {
        match self {
            Self::Heap(storage) => storage.create_index_from_floor(name, column_id, floor),
            Self::Lsm(_) => Err(StorageError::UnsupportedOperation {
                operation: "create B+Tree access method from durable floor",
                storage_kind: "LSM",
            }),
        }
    }

    pub fn create_named_index_in(
        &mut self,
        transaction: &mut StorageTransaction,
        name: IndexName,
        column_id: ColumnId,
    ) -> Result<IndexDefinition, StorageError> {
        match self {
            Self::Heap(storage) => {
                let table_id = storage.table().id;
                storage.create_named_index_in(
                    transaction.heap_transaction_mut(table_id)?,
                    name,
                    column_id,
                )
            }
            Self::Lsm(_) => Err(StorageError::UnsupportedOperation {
                operation: "create B+Tree access method",
                storage_kind: "LSM",
            }),
        }
    }

    pub fn create_index_in(
        &mut self,
        transaction: &mut StorageTransaction,
        column_id: ColumnId,
    ) -> Result<IndexDefinition, StorageError> {
        match self {
            Self::Heap(storage) => {
                let table_id = storage.table().id;
                storage.create_index_in(transaction.heap_transaction_mut(table_id)?, column_id)
            }
            Self::Lsm(_) => Err(StorageError::UnsupportedOperation {
                operation: "create B+Tree access method",
                storage_kind: "LSM",
            }),
        }
    }

    pub fn create_named_index_with_reserved_id_in(
        &mut self,
        transaction: &mut StorageTransaction,
        name: IndexName,
        column_id: ColumnId,
        id: IndexId,
        next_index_id: IndexId,
    ) -> Result<IndexDefinition, StorageError> {
        match self {
            Self::Heap(storage) => {
                let table_id = storage.table().id;
                storage.create_named_index_with_reserved_id_in(
                    transaction.heap_transaction_mut(table_id)?,
                    name,
                    column_id,
                    id,
                    next_index_id,
                )
            }
            Self::Lsm(_) => Err(StorageError::UnsupportedOperation {
                operation: "create reserved B+Tree access method",
                storage_kind: "LSM",
            }),
        }
    }

    pub fn advance_index_id_floor_in(
        &mut self,
        transaction: &mut StorageTransaction,
        target: IndexId,
    ) -> Result<(), StorageError> {
        match self {
            Self::Heap(storage) => {
                let table_id = storage.table().id;
                storage
                    .advance_index_id_floor_in(transaction.heap_transaction_mut(table_id)?, target)
            }
            Self::Lsm(_) => Err(StorageError::UnsupportedOperation {
                operation: "advance B+Tree IndexId floor",
                storage_kind: "LSM",
            }),
        }
    }

    /// Captures only logical active indexes and the authoritative high-water.
    pub fn heap_rewrite_indexes(&mut self) -> Result<HeapRewriteIndexes, StorageError> {
        match self {
            Self::Heap(storage) => storage.rewrite_indexes(),
            Self::Lsm(_) => Err(StorageError::UnsupportedOperation {
                operation: "snapshot Heap indexes for schema rewrite",
                storage_kind: "LSM",
            }),
        }
    }

    /// Validates an exact private-Heap index inventory against a prospective
    /// schema without changing the Heap fingerprint.
    #[doc(hidden)]
    pub fn validate_heap_rewrite_index_inventory(
        &mut self,
        target: &TableDef,
        expected: &HeapRewriteIndexes,
    ) -> Result<(), StorageError> {
        match self {
            Self::Heap(storage) => storage.validate_rewrite_index_inventory(target, expected),
            Self::Lsm(_) => Err(StorageError::UnsupportedOperation {
                operation: "validate private Heap index inventory",
                storage_kind: "LSM",
            }),
        }
    }

    /// Installs empty replacement trees before streaming row copy.
    pub fn install_heap_rewrite_indexes_in(
        &mut self,
        transaction: &mut StorageTransaction,
        snapshot: &HeapRewriteIndexes,
    ) -> Result<(), StorageError> {
        match self {
            Self::Heap(storage) => {
                let table_id = storage.table().id;
                storage.install_rewrite_indexes_in(
                    transaction.heap_transaction_mut(table_id)?,
                    snapshot,
                )
            }
            Self::Lsm(_) => Err(StorageError::UnsupportedOperation {
                operation: "install Heap indexes for schema rewrite",
                storage_kind: "LSM",
            }),
        }
    }

    #[doc(hidden)]
    pub fn publish_committed_index(&mut self, definition: IndexDefinition) {
        if let Self::Heap(storage) = self {
            storage.publish_committed_index(definition);
        }
    }

    pub fn drop_index(&mut self, id: IndexId) -> Result<(), StorageError> {
        match self {
            Self::Heap(storage) => storage.drop_index(id),
            Self::Lsm(_) => Err(StorageError::UnsupportedOperation {
                operation: "drop B+Tree access method",
                storage_kind: "LSM",
            }),
        }
    }

    pub fn drop_index_in(
        &mut self,
        transaction: &mut StorageTransaction,
        id: IndexId,
    ) -> Result<(), StorageError> {
        match self {
            Self::Heap(storage) => {
                let table_id = storage.table().id;
                storage.drop_index_in(transaction.heap_transaction_mut(table_id)?, id)
            }
            Self::Lsm(_) => Err(StorageError::UnsupportedOperation {
                operation: "drop B+Tree access method",
                storage_kind: "LSM",
            }),
        }
    }

    #[doc(hidden)]
    pub fn publish_committed_index_drop(&mut self, id: IndexId) {
        if let Self::Heap(storage) = self {
            storage.publish_committed_index_drop(id);
        }
    }

    pub fn analyze(&mut self) -> Result<(), StorageError> {
        match self {
            Self::Heap(storage) => storage.analyze(),
            Self::Lsm(storage) => storage.analyze(),
        }
    }

    pub fn vacuum(&mut self) -> Result<u64, StorageError> {
        match self {
            Self::Heap(storage) => storage.vacuum(),
            Self::Lsm(_) => Err(StorageError::UnsupportedOperation {
                operation: "Heap vacuum",
                storage_kind: "LSM",
            }),
        }
    }

    /// Reject admission when a dropped writer or failed maintenance requires
    /// startup recovery, while permitting the caller's active transaction.
    pub fn ensure_recovery_ready(&self) -> Result<(), StorageError> {
        match self {
            Self::Heap(storage) => storage.ensure_recovery_ready(),
            Self::Lsm(storage) => storage.ensure_recovery_ready(),
        }
    }

    pub fn flush(&self) -> Result<(), StorageError> {
        match self {
            Self::Heap(storage) => storage.flush(),
            Self::Lsm(storage) => storage.flush(),
        }
    }

    pub fn checkpoint(&mut self) -> Result<(), StorageError> {
        match self {
            Self::Heap(storage) => storage.checkpoint(),
            Self::Lsm(storage) => storage.checkpoint(),
        }
    }

    pub fn close(self) -> Result<(), StorageError> {
        match self {
            Self::Heap(storage) => storage.close(),
            Self::Lsm(storage) => storage.close(),
        }
    }

    /// Reclaims a whole retired-v3 Heap suffix using checkpoint and durable intent.
    /// Persistence failures require reopen before further mutations.
    pub fn reclaim_retired_index_tail(
        &mut self,
    ) -> Result<crate::IndexTailReclaimReport, StorageError> {
        match self {
            Self::Heap(storage) => storage.reclaim_retired_index_tail(),
            Self::Lsm(_) => Err(StorageError::UnsupportedOperation {
                operation: "retired index tail reclamation",
                storage_kind: "LSM",
            }),
        }
    }

    /// Quiescent candidate inspection, not allocation permission.
    pub fn inspect_reusable_pages(&mut self) -> Result<crate::PageReuseInspection, StorageError> {
        match self {
            Self::Heap(storage) => storage.inspect_reusable_pages(),
            Self::Lsm(_) => Err(StorageError::UnsupportedOperation {
                operation: "reusable page inspection",
                storage_kind: "LSM",
            }),
        }
    }

    /// Explicit checkpoint-gated historical v3 adoption for a physical Heap.
    pub fn adopt_historical_btree_orphans(
        &mut self,
    ) -> Result<crate::HistoricalOrphanAdoptionReport, StorageError> {
        match self {
            Self::Heap(storage) => storage.adopt_historical_btree_orphans(),
            Self::Lsm(_) => Err(StorageError::UnsupportedOperation {
                operation: "historical BTree orphan adoption",
                storage_kind: "LSM",
            }),
        }
    }

    /// Validates the full Heap file and reports pending retirement ownership.
    pub fn inspect_index_reclaim(&mut self) -> Result<crate::IndexReclaimReport, StorageError> {
        match self {
            Self::Heap(storage) => storage.inspect_index_reclaim(),
            Self::Lsm(_) => Err(StorageError::UnsupportedOperation {
                operation: "index reclaim inspection",
                storage_kind: "LSM",
            }),
        }
    }

    /// Explicit Heap index metadata maintenance; LSM indexes are unsupported.
    pub fn compact_index_catalog(&mut self) -> Result<crate::IndexMaintenanceReport, StorageError> {
        match self {
            Self::Heap(storage) => storage.compact_index_catalog(),
            Self::Lsm(_) => Err(StorageError::UnsupportedOperation {
                operation: "index catalog compaction",
                storage_kind: "LSM",
            }),
        }
    }

    pub fn compact(&mut self) -> Result<(), StorageError> {
        match self {
            Self::Heap(_) => Err(StorageError::UnsupportedOperation {
                operation: "LSM compaction",
                storage_kind: "Heap",
            }),
            Self::Lsm(storage) => storage.compact(),
        }
    }

    pub fn compact_lsm_one(&mut self) -> Result<bool, StorageError> {
        match self {
            Self::Heap(_) => Err(StorageError::UnsupportedOperation {
                operation: "LSM bounded compaction",
                storage_kind: "Heap",
            }),
            Self::Lsm(storage) => storage.compact_one(),
        }
    }

    pub fn compact_full(&mut self) -> Result<(), StorageError> {
        match self {
            Self::Heap(_) => Err(StorageError::UnsupportedOperation {
                operation: "LSM full compaction",
                storage_kind: "Heap",
            }),
            Self::Lsm(storage) => storage.compact_full(),
        }
    }
}

fn access_path_id(handle: BTreeHandle) -> AccessPathId {
    AccessPathId(handle.meta_page.page_id().0)
}

fn registered_handle(
    storage: &mut HeapStorage,
    access_path: AccessPathId,
) -> Result<BTreeHandle, StorageError> {
    if let Some(handle) = storage
        .indexes()
        .iter()
        .find(|definition| access_path_id(definition.handle) == access_path)
        .map(|definition| definition.handle)
    {
        return Ok(handle);
    }
    storage
        .rewrite_indexes_with_definitions()?
        .into_iter()
        .find(|definition| access_path_id(definition.handle) == access_path)
        .map(|definition| definition.handle)
        .ok_or(StorageError::UnknownAccessPath {
            table_id: storage.table().id,
            access_path,
        })
}

#[cfg(test)]
mod tests {
    use std::ops::ControlFlow;

    use netbadb_index::{IndexBound, IndexRange};
    use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
    use netbadb_types::{ColumnId, PhysicalType, ScalarValue, StorageId, TableId};

    use super::{StorageKind, StorageVisibilityBoundary, TableStorage};
    use crate::{
        StorageError, TransactionError, TransactionState, txn_status_path, wal_alternate_path,
        wal_path,
    };

    fn table(table_id: u64) -> TableDef {
        TableDef::new(
            TableId(table_id),
            format!("items_{table_id}"),
            vec![
                ColumnDef::new(ColumnId(1), "id", TypeSpec::Physical(PhysicalType::Int64)),
                ColumnDef::new(
                    ColumnId(2),
                    "payload",
                    TypeSpec::Physical(PhysicalType::Text),
                ),
            ],
        )
    }

    fn path(case: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "netbadb-table-storage-{case}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ))
    }

    fn cleanup(path: &std::path::Path) {
        let wal = wal_path(path);
        let _ = std::fs::remove_file(wal_alternate_path(&wal));
        let _ = std::fs::remove_file(wal);
        let _ = std::fs::remove_file(txn_status_path(path));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn bounded_row_consumer_stops_identically_for_heap_and_lsm() {
        let heap_path = path("bounded-consumer-heap");
        let lsm_path = path("bounded-consumer-lsm");
        cleanup(&heap_path);
        let _ = std::fs::remove_dir_all(&lsm_path);
        let heap = TableStorage::create_heap(&heap_path, table(80)).expect("create Heap");
        let lsm = TableStorage::create_lsm(&lsm_path, table(80), ColumnId(1)).expect("create LSM");

        for (kind, mut storage) in [("Heap", heap), ("LSM", lsm)] {
            let mut transaction = storage.begin_transaction().expect("begin load");
            for id in 0..300 {
                storage
                    .insert_in(
                        &mut transaction,
                        &[
                            ScalarValue::Int64(id),
                            ScalarValue::Text(format!("value-{id}")),
                        ],
                    )
                    .expect("insert row");
            }
            transaction.commit().expect("commit load");
            let view = storage.read_view().expect("create read view");
            let mut visited = Vec::new();
            let flow = storage
                .visit_rows_with_view_control::<StorageError, _>(
                    &[ColumnId(2), ColumnId(1), ColumnId(2)],
                    &view,
                    |_row, values| {
                        visited.push(values);
                        if visited.len() == 7 {
                            Ok(ControlFlow::Break(()))
                        } else {
                            Ok(ControlFlow::Continue(()))
                        }
                    },
                )
                .expect("visit bounded rows");
            assert!(flow.is_break(), "{kind} must propagate typed cancellation");
            assert_eq!(visited.len(), 7, "{kind} must not visit later rows");
            for (id, values) in visited.into_iter().enumerate() {
                assert_eq!(
                    values,
                    vec![
                        ScalarValue::Text(format!("value-{id}")),
                        ScalarValue::Int64(id as i64),
                        ScalarValue::Text(format!("value-{id}")),
                    ]
                );
            }
            drop(view);

            let view = storage.read_view().expect("create zero-width view");
            let mut zero_width = 0;
            let flow = storage
                .visit_rows_with_view_control::<StorageError, _>(&[], &view, |_row, values| {
                    assert!(values.is_empty());
                    zero_width += 1;
                    if zero_width == 1 {
                        Ok(ControlFlow::Break(()))
                    } else {
                        Ok(ControlFlow::Continue(()))
                    }
                })
                .expect("visit zero-width row");
            assert!(flow.is_break());
            assert_eq!(zero_width, 1);
            drop(view);
            storage.close().expect("close bounded storage");
        }
        cleanup(&heap_path);
        let _ = std::fs::remove_dir_all(lsm_path);
    }

    #[test]
    fn heap_dispatch_preserves_fast_paths_access_methods_and_generation_safety() {
        let path = path("heap-capabilities");
        cleanup(&path);
        let mut storage = TableStorage::create_heap(&path, table(1)).expect("create table storage");
        let first = storage
            .insert(&[ScalarValue::Int64(1), ScalarValue::Text("A".into())])
            .expect("insert first");
        storage
            .insert(&[ScalarValue::Int64(2), ScalarValue::Text("B".into())])
            .expect("insert second");
        storage
            .create_index(ColumnId(1))
            .expect("create access method");

        let paths = storage.access_paths();
        assert_eq!(paths.len(), 1);
        assert!(paths[0].capabilities.point_lookup);
        assert!(paths[0].capabilities.range_lookup);
        let access_path = paths[0].id;
        let view = storage.read_view().expect("read view");
        assert_eq!(
            storage
                .point_lookup_columns_with_view(
                    access_path,
                    &ScalarValue::Int64(1),
                    &[ColumnId(2)],
                    &view,
                )
                .expect("point lookup"),
            vec![(first, vec![ScalarValue::Text("A".into())])]
        );
        assert_eq!(
            storage
                .range_lookup_columns_with_view(
                    access_path,
                    &IndexRange {
                        lower: IndexBound::Included(ScalarValue::Int64(1)),
                        upper: IndexBound::Included(ScalarValue::Int64(2)),
                    },
                    &[ColumnId(1)],
                    &view,
                )
                .expect("range lookup")
                .len(),
            2
        );
        let summary = storage
            .scan_presence_counts_with_view(&[ColumnId(2)], &view)
            .expect("presence fast path");
        assert_eq!(summary.live_rows, 2);
        assert_eq!(summary.non_null_counts, vec![2]);
        let mut borrowed = Vec::new();
        storage
            .visit_scalar_refs_with_presence_view::<StorageError, _>(
                &[ColumnId(2)],
                &[],
                &view,
                |values, _| {
                    borrowed.push(values[0].to_owned());
                    Ok(())
                },
            )
            .expect("borrowed visitor");
        assert_eq!(
            borrowed,
            vec![ScalarValue::Text("A".into()), ScalarValue::Text("B".into())]
        );
        drop(view);

        let current = storage
            .update(
                first,
                &[ScalarValue::Int64(3), ScalarValue::Text("C".into())],
            )
            .expect("append updated version");
        storage
            .delete(current)
            .expect("logically delete current version");
        assert!(storage.vacuum().expect("vacuum versions") >= 2);
        storage
            .insert(&[ScalarValue::Int64(4), ScalarValue::Text("D".into())])
            .expect("reuse vacuumed slot");
        assert!(matches!(
            storage.update(
                first,
                &[ScalarValue::Int64(5), ScalarValue::Text("stale".into())]
            ),
            Err(StorageError::StaleRowId { .. })
        ));

        storage.close().expect("close table storage");
        cleanup(&path);
    }

    #[test]
    fn table_scoped_rows_views_and_transactions_reject_cross_storage_use() {
        let first_path = path("context-first");
        let second_path = path("context-second");
        cleanup(&first_path);
        cleanup(&second_path);
        let mut first =
            TableStorage::create_heap(&first_path, table(1)).expect("create first storage");
        let mut second =
            TableStorage::create_heap(&second_path, table(2)).expect("create second storage");
        let row = first
            .insert(&[ScalarValue::Int64(1), ScalarValue::Text("A".into())])
            .expect("insert first row");
        let view = first.read_view().expect("first view");
        assert!(matches!(
            second.scan_columns_with_view(&[ColumnId(1)], &view),
            Err(StorageError::StorageContextMismatch {
                expected: TableId(2),
                actual: TableId(1)
            })
        ));
        assert!(matches!(
            second.delete(row),
            Err(StorageError::StorageContextMismatch {
                expected: TableId(2),
                actual: TableId(1)
            })
        ));
        drop(view);

        let mut transaction = first.begin_transaction().expect("begin first transaction");
        assert!(matches!(
            second.validate_transaction(&transaction),
            Err(StorageError::Transaction(
                TransactionError::ForeignTransaction { .. }
            ))
        ));
        transaction.rollback().expect("roll back first transaction");
        assert_eq!(transaction.state(), TransactionState::RolledBack);

        first.close().expect("close first storage");
        second.close().expect("close second storage");
        cleanup(&first_path);
        cleanup(&second_path);
    }

    #[test]
    fn heap_and_lsm_boundaries_reopen_exact_history_and_reject_wrong_context() {
        let heap_path = path("visibility-boundary-heap");
        let lsm_path = path("visibility-boundary-lsm");
        cleanup(&heap_path);
        let _ = std::fs::remove_dir_all(&lsm_path);
        let heap = TableStorage::create_heap_with_storage_id(&heap_path, table(91), StorageId(91))
            .expect("create Heap");
        let lsm = TableStorage::create_lsm_with_storage_id(
            &lsm_path,
            table(92),
            ColumnId(1),
            StorageId(92),
        )
        .expect("create LSM");

        for mut storage in [heap, lsm] {
            let kind = storage.kind();
            let storage_id = storage.storage_id();
            let baseline = storage
                .current_visibility_boundary()
                .expect("capture baseline");
            assert_eq!(baseline.value(), 1);
            assert!(matches!(
                StorageVisibilityBoundary::new(storage_id, kind, 0),
                Err(StorageError::InvalidVisibilityBoundary { .. })
            ));
            let row = storage
                .insert(&[ScalarValue::Int64(1), ScalarValue::Text("new".into())])
                .expect("commit row");
            let inserted = storage
                .current_visibility_boundary()
                .expect("capture inserted boundary");
            assert!(inserted.value() > baseline.value());
            let old = storage.read_view_at(baseline).expect("read old boundary");
            assert!(
                storage
                    .scan_columns_with_view(&[ColumnId(1)], &old)
                    .expect("scan old")
                    .is_empty()
            );
            drop(old);
            if storage.kind() == StorageKind::Lsm {
                let before_flush = storage
                    .current_visibility_boundary()
                    .expect("boundary before flush");
                storage.flush().expect("flush before pinning history");
                assert_eq!(
                    storage
                        .current_visibility_boundary()
                        .expect("boundary after flush"),
                    before_flush
                );
            }
            let pinned = storage
                .read_view_at(inserted)
                .expect("pin inserted boundary");
            storage
                .update(
                    row,
                    &[ScalarValue::Int64(1), ScalarValue::Text("updated".into())],
                )
                .expect("commit update");
            match storage.kind() {
                StorageKind::Heap => {
                    let _ = storage.vacuum().expect("vacuum with old pin");
                }
                StorageKind::Lsm => {
                    assert!(
                        storage.compact().is_err(),
                        "LSM must not reclaim pinned history"
                    );
                }
            }
            assert_eq!(
                storage
                    .scan_columns_with_view(&[ColumnId(2)], &pinned)
                    .expect("read pinned history"),
                vec![(row, vec![ScalarValue::Text("new".into())])]
            );
            drop(pinned);
            let current = storage
                .current_visibility_boundary()
                .expect("capture current boundary");
            let now = storage
                .read_view_at(current)
                .expect("read current boundary");
            let current_rows = storage
                .scan_columns_with_view(&[ColumnId(2)], &now)
                .expect("scan current");
            assert_eq!(current_rows.len(), 1);
            assert_eq!(current_rows[0].1, vec![ScalarValue::Text("updated".into())]);
            drop(now);

            let wrong_kind = match storage.kind() {
                StorageKind::Heap => StorageKind::Lsm,
                StorageKind::Lsm => StorageKind::Heap,
            };
            let wrong =
                StorageVisibilityBoundary::new(storage.storage_id(), wrong_kind, current.value())
                    .expect("construct wrong-kind boundary");
            assert!(matches!(
                storage.read_view_at(wrong),
                Err(StorageError::VisibilityBoundaryContextMismatch { .. })
            ));
            let wrong_storage = StorageVisibilityBoundary::new(
                StorageId(storage.storage_id().0 + 1),
                storage.kind(),
                current.value(),
            )
            .expect("construct wrong-storage boundary");
            assert!(matches!(
                storage.read_view_at(wrong_storage),
                Err(StorageError::VisibilityBoundaryContextMismatch { .. })
            ));
            let future = StorageVisibilityBoundary::new(
                storage.storage_id(),
                storage.kind(),
                current.value() + 1,
            )
            .expect("construct future boundary");
            assert!(matches!(
                storage.read_view_at(future),
                Err(StorageError::FutureVisibilityBoundary { .. })
            ));
            let before_checkpoint = storage
                .current_visibility_boundary()
                .expect("boundary before checkpoint");
            storage.checkpoint().expect("checkpoint storage");
            assert_eq!(
                storage
                    .current_visibility_boundary()
                    .expect("boundary after checkpoint"),
                before_checkpoint
            );
            storage.close().expect("close storage");

            let mut reopened = match kind {
                StorageKind::Heap => {
                    TableStorage::open_heap(&heap_path, table(storage_id.0)).expect("reopen Heap")
                }
                StorageKind::Lsm => {
                    TableStorage::open_lsm(&lsm_path, table(storage_id.0)).expect("reopen LSM")
                }
            };
            assert_eq!(
                reopened
                    .current_visibility_boundary()
                    .expect("reopened current boundary"),
                current
            );
            let reopened_old = reopened
                .read_view_at(inserted)
                .expect("reopen old boundary");
            assert_eq!(
                reopened
                    .scan_columns_with_view(&[ColumnId(2)], &reopened_old)
                    .expect("scan reopened old boundary"),
                vec![(row, vec![ScalarValue::Text("new".into())])]
            );
            drop(reopened_old);
            reopened.close().expect("close reopened storage");
        }
        cleanup(&heap_path);
        let _ = std::fs::remove_dir_all(lsm_path);
    }
}
