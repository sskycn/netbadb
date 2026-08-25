use std::path::Path;

use netbadb_index::{BTreeHandle, IndexDefinition, IndexRange, IndexStatistics, TableStatistics};
use netbadb_schema::TableDef;
use netbadb_types::{AccessPathId, ColumnId, Lsn, RowId, ScalarRef, ScalarValue, TableId, TxnId};

use crate::{
    HeapStorage, IsolationLevel, PresenceCountSummary, ReadView, StorageError, Transaction,
    TransactionError, TransactionState,
};

/// Executable capabilities advertised by one table-scoped access path.
///
/// These properties describe operations available to planning and execution;
/// they do not expose the access method's persistent representation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AccessPathCapabilities {
    pub point_lookup: bool,
    pub range_lookup: bool,
}

/// Storage-owned optimizer snapshot for one registered access method.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageAccessPath {
    pub id: AccessPathId,
    pub column_id: ColumnId,
    pub capabilities: AccessPathCapabilities,
    pub statistics: Option<IndexStatistics>,
}

/// Opaque executor identity for a physical row version.
///
/// Heap keeps using its generation-safe [`RowId`] internally. Executor code
/// may retain and return this handle to the owning [`TableStorage`], but cannot
/// inspect PageId or SlotId or assume another storage layout shares them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StorageRowHandle {
    table_id: TableId,
    inner: StorageRowHandleKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum StorageRowHandleKind {
    Heap(RowId),
}

impl StorageRowHandle {
    fn heap(table_id: TableId, row_id: RowId) -> Self {
        Self {
            table_id,
            inner: StorageRowHandleKind::Heap(row_id),
        }
    }

    fn heap_row_id(self, table_id: TableId) -> Result<RowId, StorageError> {
        if self.table_id != table_id {
            return Err(StorageError::StorageContextMismatch {
                expected: table_id,
                actual: self.table_id,
            });
        }
        match self.inner {
            StorageRowHandleKind::Heap(row_id) => Ok(row_id),
        }
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
        }
    }

    #[must_use]
    pub fn id(&self) -> TxnId {
        match &self.inner {
            StorageTransactionKind::Heap(transaction) => transaction.id(),
        }
    }

    #[must_use]
    pub fn state(&self) -> TransactionState {
        match &self.inner {
            StorageTransactionKind::Heap(transaction) => transaction.state(),
        }
    }

    #[must_use]
    pub fn last_lsn(&self) -> Lsn {
        match &self.inner {
            StorageTransactionKind::Heap(transaction) => transaction.last_lsn(),
        }
    }

    #[must_use]
    pub fn isolation_level(&self) -> IsolationLevel {
        match &self.inner {
            StorageTransactionKind::Heap(transaction) => transaction.isolation_level(),
        }
    }

    pub fn begin_statement(&mut self) -> Result<StorageReadView, StorageError> {
        let table_id = self.table_id;
        let view = self.heap_transaction_mut(table_id)?.begin_statement()?;
        Ok(StorageReadView::heap(table_id, view))
    }

    pub fn commit(&mut self) -> Result<(), StorageError> {
        let table_id = self.table_id;
        self.heap_transaction_mut(table_id)?.commit()
    }

    pub fn rollback(&mut self) -> Result<(), StorageError> {
        let table_id = self.table_id;
        self.heap_transaction_mut(table_id)?.rollback()
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
    Heap(HeapStorage),
}

impl From<HeapStorage> for TableStorage {
    fn from(storage: HeapStorage) -> Self {
        Self::Heap(storage)
    }
}

impl TableStorage {
    pub fn create_heap(path: impl AsRef<Path>, table: TableDef) -> Result<Self, StorageError> {
        HeapStorage::create(path, table).map(Self::Heap)
    }

    pub fn open_heap(path: impl AsRef<Path>, table: TableDef) -> Result<Self, StorageError> {
        HeapStorage::open(path, table).map(Self::Heap)
    }

    #[must_use]
    pub fn table(&self) -> &TableDef {
        match self {
            Self::Heap(storage) => storage.table(),
        }
    }

    pub fn read_view(&self) -> Result<StorageReadView, StorageError> {
        match self {
            Self::Heap(storage) => Ok(StorageReadView::heap(
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
        }
    }

    pub fn insert(&mut self, values: &[ScalarValue]) -> Result<StorageRowHandle, StorageError> {
        match self {
            Self::Heap(storage) => {
                let table_id = storage.table().id;
                storage
                    .insert(values)
                    .map(|row_id| StorageRowHandle::heap(table_id, row_id))
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
                let current = storage.update(row.heap_row_id(table_id)?, values)?;
                Ok(StorageRowHandle::heap(table_id, current))
            }
        }
    }

    pub fn delete(&mut self, row: StorageRowHandle) -> Result<(), StorageError> {
        match self {
            Self::Heap(storage) => {
                let table_id = storage.table().id;
                storage.delete(row.heap_row_id(table_id)?)
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
                Ok(StorageRowHandle::heap(table_id, row_id))
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
                let row_id = row.heap_row_id(table_id)?;
                let current = storage.update_in(
                    transaction.heap_transaction_mut(table_id)?,
                    row_id,
                    values,
                )?;
                Ok(StorageRowHandle::heap(table_id, current))
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
                    row.heap_row_id(table_id)?,
                )
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
                    .map(|(row_id, values)| (StorageRowHandle::heap(table_id, row_id), values))
                    .collect())
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
                        rows.push((StorageRowHandle::heap(table_id, row_id), values));
                    }
                }
                Ok(rows)
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
                        rows.push((StorageRowHandle::heap(table_id, row_id), values));
                    }
                }
                Ok(rows)
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
        }
    }

    pub fn visit_scalar_refs_with_presence_view<E, F>(
        &mut self,
        value_columns: &[ColumnId],
        presence_columns: &[ColumnId],
        view: &StorageReadView,
        visitor: F,
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
                let view = view.heap_view(table_id).map_err(E::from)?;
                storage.visit_row_scalar_refs_with_presence_view(
                    value_columns,
                    presence_columns,
                    view,
                    |row_id, values, presence| {
                        visitor(StorageRowHandle::heap(table_id, row_id), values, presence)
                    },
                )
            }
        }
    }

    #[must_use]
    pub fn indexes(&self) -> &[IndexDefinition] {
        match self {
            Self::Heap(storage) => storage.indexes(),
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
                    },
                    statistics: storage.index_statistics(definition.column_id),
                })
                .collect(),
        }
    }

    #[must_use]
    pub fn table_statistics(&self) -> Option<TableStatistics> {
        match self {
            Self::Heap(storage) => storage.table_statistics(),
        }
    }

    #[must_use]
    pub fn index_statistics(&self, column_id: ColumnId) -> Option<IndexStatistics> {
        match self {
            Self::Heap(storage) => storage.index_statistics(column_id),
        }
    }

    pub fn create_index(&mut self, column_id: ColumnId) -> Result<IndexDefinition, StorageError> {
        match self {
            Self::Heap(storage) => storage.create_index(column_id),
        }
    }

    pub fn analyze(&mut self) -> Result<(), StorageError> {
        match self {
            Self::Heap(storage) => storage.analyze(),
        }
    }

    pub fn vacuum(&mut self) -> Result<u64, StorageError> {
        match self {
            Self::Heap(storage) => storage.vacuum(),
        }
    }

    pub fn flush(&self) -> Result<(), StorageError> {
        match self {
            Self::Heap(storage) => storage.flush(),
        }
    }

    pub fn checkpoint(&mut self) -> Result<(), StorageError> {
        match self {
            Self::Heap(storage) => storage.checkpoint(),
        }
    }

    pub fn close(self) -> Result<(), StorageError> {
        match self {
            Self::Heap(storage) => storage.close(),
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
