use std::ops::ControlFlow;
use std::path::Path;

use netbadb_index::{BTreeHandle, IndexDefinition, IndexRange, IndexStatistics, TableStatistics};
use netbadb_schema::TableDef;
use netbadb_types::{
    AccessPathId, ColumnId, DatabaseTxnId, IndexName, Lsn, RowId, ScalarRef, ScalarValue,
    StorageId, TableId, TxnId,
};

use crate::{
    HeapIdentityInspection, HeapRecoveryInspection, HeapStorage, IsolationLevel,
    LsmIdentityInspection, LsmInspection, LsmReadView, LsmRecoveryInspection, LsmRowHandle,
    LsmStorage, LsmTransaction, PreparedTxnResolution, PresenceCountSummary, ReadView,
    StorageError, Transaction, TransactionError, TransactionState,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageKind {
    Heap,
    Lsm,
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
}

/// Table-scoped read context passed through executor storage capabilities.
#[derive(Debug)]
pub struct StorageReadView {
    table_id: TableId,
    inner: StorageReadViewKind,
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
    inner: StorageTransactionKind,
}

#[derive(Debug)]
enum StorageTransactionKind {
    Heap(Transaction),
    Lsm(LsmTransaction),
}

impl StorageTransaction {
    fn heap(table_id: TableId, transaction: Transaction) -> Self {
        Self {
            table_id,
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

    fn lsm(table_id: TableId, transaction: LsmTransaction) -> Self {
        Self {
            table_id,
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

    pub fn commit_prepared(&mut self, database_txn_id: DatabaseTxnId) -> Result<(), StorageError> {
        match &mut self.inner {
            StorageTransactionKind::Heap(txn) => txn.commit_prepared(database_txn_id),
            StorageTransactionKind::Lsm(txn) => txn.commit_prepared(database_txn_id),
        }
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
                Ok(StorageTransaction::heap(table_id, transaction))
            }
            Self::Lsm(storage) => {
                let table_id = storage.table().id;
                Ok(StorageTransaction::lsm(
                    table_id,
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

    #[doc(hidden)]
    pub fn publish_committed_index(&mut self, definition: IndexDefinition) {
        if let Self::Heap(storage) = self {
            storage.publish_committed_index(definition);
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

    pub fn compact(&mut self) -> Result<(), StorageError> {
        match self {
            Self::Heap(_) => Err(StorageError::UnsupportedOperation {
                operation: "LSM compaction",
                storage_kind: "Heap",
            }),
            Self::Lsm(storage) => storage.compact(),
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
    AccessPathId(handle.meta_page.0)
}

fn registered_handle(
    storage: &HeapStorage,
    access_path: AccessPathId,
) -> Result<BTreeHandle, StorageError> {
    storage
        .indexes()
        .iter()
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
    use netbadb_types::{ColumnId, PhysicalType, ScalarValue, TableId};

    use super::TableStorage;
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
}
