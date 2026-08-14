use std::cell::RefCell;
use std::collections::HashSet;
use std::path::Path;
use std::rc::Rc;

use netbadb_index::{
    IndexCatalogNode, IndexDefinition, IndexError, IndexSpec, decode_index_catalog,
    encode_index_catalog, ensure_key_fits,
};
use netbadb_schema::{SchemaFingerprint, TableDef};
use netbadb_types::{ColumnId, PageId, RowId, ScalarValue, SlotId};

use crate::recovery::RecoveryManager;
use crate::transaction::TransactionManager;
use crate::{
    BufferPool, CodecError, DEFAULT_BUFFER_POOL_SIZE, MetadataError, PAGE_HEADER_SIZE, PAGE_SIZE,
    Page, PageError, PageManager, PageType, SLOT_SIZE, SlotRef, SlotState, StorageError,
    Transaction, TransactionError, WalManager, wal_path,
};

const HEADER_PAGE: PageId = PageId(0);
const FIRST_MANAGED_PAGE: PageId = PageId(1);
const HEADER_MAGIC: &[u8; 4] = b"NBD1";
const HEAP_FORMAT_VERSION: u16 = 3;
const HEAP_METADATA_OFFSET: usize = 16;
const HEAP_VERSION_OFFSET: usize = HEAP_METADATA_OFFSET + 4;
const HEAP_RESERVED_OFFSET: usize = HEAP_VERSION_OFFSET + 2;
const HEAP_TABLE_ID_OFFSET: usize = HEAP_RESERVED_OFFSET + 2;
const HEAP_COLUMN_COUNT_OFFSET: usize = HEAP_TABLE_ID_OFFSET + 8;
const HEAP_SCHEMA_FINGERPRINT_OFFSET: usize = HEAP_COLUMN_COUNT_OFFSET + 2;
const HEAP_INDEX_CATALOG_ROOT_OFFSET: usize =
    HEAP_SCHEMA_FINGERPRINT_OFFSET + SchemaFingerprint::LENGTH;
const HEAP_TRAILING_RESERVED_OFFSET: usize = HEAP_INDEX_CATALOG_ROOT_OFFSET + 8;
const HEAP_TRAILING_RESERVED_END: usize = HEAP_TRAILING_RESERVED_OFFSET + 6;

/// Heap storage over the buffer pool. Heap code interprets pages as heap pages;
/// the buffer pool and page manager remain generic over raw database pages.
#[derive(Debug)]
pub struct HeapStorage {
    buffer: BufferPool,
    table: TableDef,
    transactions: TransactionManager,
    indexes: Vec<IndexDefinition>,
    index_plans: Vec<RegisteredIndexPlan>,
    index_catalog_root: PageId,
    #[cfg(test)]
    skip_drop_flush: bool,
    #[cfg(test)]
    fail_relocation_second_log: bool,
    #[cfg(test)]
    fail_relocation_source_publish: bool,
    #[cfg(test)]
    fail_index_catalog_log: bool,
    #[cfg(test)]
    index_catalog_payload_capacity: Option<usize>,
    #[cfg(test)]
    fail_registered_mutation_after: Option<usize>,
}

#[derive(Debug, Clone)]
struct RegisteredIndexPlan {
    definition: IndexDefinition,
    column_position: usize,
    spec: IndexSpec,
}

#[derive(Debug)]
struct PreparedInsert {
    page_id: PageId,
    before: Page,
    after: Page,
    slot_ref: SlotRef,
    new_page: bool,
}

impl PreparedInsert {
    fn row_id(&self) -> RowId {
        RowId {
            page: self.page_id,
            slot: self.slot_ref.slot.0,
            generation: self.slot_ref.generation,
        }
    }
}

impl HeapStorage {
    /// Borrows this heap's shared file, buffer, transaction, and WAL domain as
    /// a persistent B+Tree API.
    pub fn btree(&mut self) -> crate::BTree<'_> {
        crate::BTree::new(self)
    }

    pub fn create(path: impl AsRef<Path>, table: TableDef) -> Result<Self, StorageError> {
        Self::create_with_buffer_pool_size(path, table, DEFAULT_BUFFER_POOL_SIZE)
    }

    pub fn create_with_buffer_pool_size(
        path: impl AsRef<Path>,
        table: TableDef,
        buffer_pool_size: usize,
    ) -> Result<Self, StorageError> {
        let fingerprint = validate_table(&table)?;
        BufferPool::validate_capacity(buffer_pool_size)?;
        let path = path.as_ref();
        let wal_path = wal_path(path);
        let wal_manager = WalManager::create(&wal_path)?;
        let pages = match PageManager::create(path) {
            Ok(pages) => pages,
            Err(error) => {
                drop(wal_manager);
                let _ = std::fs::remove_file(wal_path);
                return Err(error);
            }
        };
        let wal = Rc::new(RefCell::new(wal_manager));
        let buffer = BufferPool::with_wal(pages, buffer_pool_size, Rc::clone(&wal))?;
        {
            let mut header = buffer.write_page(HEADER_PAGE)?;
            write_heap_metadata(
                header.page_mut().bytes_mut(),
                &table,
                fingerprint,
                FIRST_MANAGED_PAGE,
            );
        }
        {
            let mut catalog_page = buffer.new_page()?;
            let page_id = catalog_page.page_id();
            if page_id != FIRST_MANAGED_PAGE {
                return Err(crate::invalid_format(
                    "index catalog root page is not page 1",
                ));
            }
            let payload = encode_index_catalog(&IndexCatalogNode::empty())?;
            let page = catalog_page.page_mut();
            *page = Page::new(page_id, PageType::IndexCatalog);
            page.initialize_single_payload(PageType::IndexCatalog, &payload)?;
        }
        {
            let mut data_page = buffer.new_page()?;
            let page_id = data_page.page_id();
            let page = data_page.page_mut();
            *page = Page::new(page_id, PageType::Heap);
        }
        buffer.flush_all()?;
        let next_txn_id = wal.borrow().next_txn_id();
        let transactions = TransactionManager::new(wal, buffer.clone(), next_txn_id)?;
        Ok(Self {
            buffer,
            table,
            transactions,
            indexes: Vec::new(),
            index_plans: Vec::new(),
            index_catalog_root: FIRST_MANAGED_PAGE,
            #[cfg(test)]
            skip_drop_flush: false,
            #[cfg(test)]
            fail_relocation_second_log: false,
            #[cfg(test)]
            fail_relocation_source_publish: false,
            #[cfg(test)]
            fail_index_catalog_log: false,
            #[cfg(test)]
            index_catalog_payload_capacity: None,
            #[cfg(test)]
            fail_registered_mutation_after: None,
        })
    }

    pub fn open(path: impl AsRef<Path>, table: TableDef) -> Result<Self, StorageError> {
        Self::open_with_buffer_pool_size(path, table, DEFAULT_BUFFER_POOL_SIZE)
    }

    pub fn open_with_buffer_pool_size(
        path: impl AsRef<Path>,
        table: TableDef,
        buffer_pool_size: usize,
    ) -> Result<Self, StorageError> {
        let fingerprint = validate_table(&table)?;
        BufferPool::validate_capacity(buffer_pool_size)?;
        let path = path.as_ref();
        let mut pages = PageManager::open(path)?;
        if pages.page_count() < 3 {
            return Err(crate::invalid_format("heap file has no data page"));
        }
        let catalog_root =
            validate_heap_metadata(pages.read_page(HEADER_PAGE)?.bytes(), &table, fingerprint)?;
        validate_catalog_root_bounds(catalog_root, pages.page_count())?;
        let (mut wal_manager, records, truncated_wal_tail) =
            WalManager::open_for_recovery(wal_path(path))?;
        if let Err(error) =
            RecoveryManager::recover(&mut pages, &mut wal_manager, &records, truncated_wal_tail)
        {
            return Err(match error {
                crate::RecoveryError::Storage(storage) => *storage,
                recovery => recovery.into(),
            });
        }
        let recovered_catalog_root =
            validate_heap_metadata(pages.read_page(HEADER_PAGE)?.bytes(), &table, fingerprint)?;
        validate_catalog_root_bounds(recovered_catalog_root, pages.page_count())?;
        if recovered_catalog_root != catalog_root {
            return Err(crate::invalid_format(
                "index catalog root changed during recovery",
            ));
        }
        let wal = Rc::new(RefCell::new(wal_manager));
        let buffer = BufferPool::with_wal(pages, buffer_pool_size, Rc::clone(&wal))?;
        {
            let header = buffer.read_page(HEADER_PAGE)?;
            validate_heap_metadata(header.page().bytes(), &table, fingerprint)?;
        }
        let next_txn_id = wal.borrow().next_txn_id();
        let transactions = TransactionManager::new(wal, buffer.clone(), next_txn_id)?;
        let mut storage = Self {
            buffer,
            table,
            transactions,
            indexes: Vec::new(),
            index_plans: Vec::new(),
            index_catalog_root: catalog_root,
            #[cfg(test)]
            skip_drop_flush: false,
            #[cfg(test)]
            fail_relocation_second_log: false,
            #[cfg(test)]
            fail_relocation_source_publish: false,
            #[cfg(test)]
            fail_index_catalog_log: false,
            #[cfg(test)]
            index_catalog_payload_capacity: None,
            #[cfg(test)]
            fail_registered_mutation_after: None,
        };
        storage.indexes = storage.load_index_registry(catalog_root)?;
        storage.index_plans = storage.build_registered_index_plans()?;
        Ok(storage)
    }

    /// Returns registered table indexes in persistent creation order. Raw
    /// trees created through [`Self::btree`] are intentionally absent.
    #[must_use]
    pub fn indexes(&self) -> &[IndexDefinition] {
        &self.indexes
    }

    #[must_use]
    pub fn index_for_column(&self, column_id: ColumnId) -> Option<&IndexDefinition> {
        self.indexes
            .iter()
            .find(|definition| definition.column_id == column_id)
    }

    /// Atomically builds a non-unique single-column index over all currently
    /// live rows, then registers it as the transaction's final logical step.
    /// Subsequent heap DML maintains every registered index automatically.
    pub fn create_index(&mut self, column_id: ColumnId) -> Result<IndexDefinition, StorageError> {
        let (column_position, spec) = self
            .table
            .columns
            .iter()
            .enumerate()
            .find(|(_, column)| column.id == column_id)
            .map(|(position, column)| {
                (
                    position,
                    IndexSpec {
                        data_type: column.semantic_type(),
                        nullable: column.nullable,
                    },
                )
            })
            .ok_or(IndexError::UnknownIndexColumn { column_id })?;
        if self.index_for_column(column_id).is_some() {
            return Err(IndexError::IndexAlreadyExists { column_id }.into());
        }

        let mut transaction = self.begin_transaction()?;
        transaction.acquire_writer()?;
        let result =
            (|| {
                let handle = self.btree().create_in(&mut transaction, spec.clone())?;
                // Writer ownership is already held. Materialize the stable heap
                // snapshot before further BTree growth extends shared PageIds.
                let rows = self.scan()?;
                for (row_id, values) in rows {
                    let key = values.get(column_position).cloned().ok_or(
                        StorageError::InvalidRowLength {
                            expected: self.table.columns.len(),
                            actual: values.len(),
                        },
                    )?;
                    self.btree()
                        .insert_in(&mut transaction, handle, key, row_id)?;
                }
                let definition = IndexDefinition { column_id, handle };
                // Registration is deliberately last: no committed catalog entry
                // can ever describe a partially backfilled tree.
                #[cfg(test)]
                crate::crash_test::maybe_crash(
                    crate::crash_test::TestCrashPoint::IndexBuildBeforeCatalogLog,
                );
                self.append_index_definition(&mut transaction, &definition)?;
                Ok(RegisteredIndexPlan {
                    definition,
                    column_position,
                    spec: spec.clone(),
                })
            })();

        match result {
            Ok(plan) => {
                transaction.commit()?;
                let definition = plan.definition.clone();
                self.indexes.push(definition.clone());
                self.index_plans.push(plan);
                Ok(definition)
            }
            Err(error) => match transaction.rollback() {
                Ok(()) => Err(error),
                Err(rollback) => Err(rollback),
            },
        }
    }

    fn load_index_registry(
        &mut self,
        root_page: PageId,
    ) -> Result<Vec<IndexDefinition>, StorageError> {
        let page_count = self.buffer.page_count();
        if root_page.0 == 0 || root_page.0 >= page_count {
            return Err(IndexError::InvalidChild(root_page).into());
        }
        let mut page_id = root_page;
        let mut visited = HashSet::new();
        let mut definitions: Vec<IndexDefinition> = Vec::new();
        loop {
            if !visited.insert(page_id) || visited.len() as u64 > page_count {
                return Err(IndexError::CatalogCycle { page_id }.into());
            }
            let page = self.buffer.read_page(page_id)?;
            if page.page().header()?.page_type != PageType::IndexCatalog {
                return Err(IndexError::InvalidNodeType.into());
            }
            let node = decode_index_catalog(page.page().single_payload(PageType::IndexCatalog)?)?;
            drop(page);
            for definition in node.definitions {
                if definitions
                    .iter()
                    .any(|existing| existing.column_id == definition.column_id)
                {
                    return Err(IndexError::DuplicateRegisteredColumn {
                        column_id: definition.column_id,
                    }
                    .into());
                }
                let column = self.table.column_by_id(definition.column_id).ok_or(
                    IndexError::UnknownIndexColumn {
                        column_id: definition.column_id,
                    },
                )?;
                let expected = IndexSpec {
                    data_type: column.semantic_type(),
                    nullable: column.nullable,
                };
                let actual = self.btree().spec(definition.handle)?;
                if actual != expected {
                    return Err(IndexError::CatalogSpecMismatch {
                        column_id: definition.column_id,
                    }
                    .into());
                }
                definitions.push(definition);
            }
            match node.next_catalog {
                Some(next) => {
                    if next.0 == 0 || next.0 >= page_count {
                        return Err(IndexError::InvalidChild(next).into());
                    }
                    page_id = next;
                }
                None => break,
            }
        }
        Ok(definitions)
    }

    fn append_index_definition(
        &mut self,
        transaction: &mut Transaction,
        definition: &IndexDefinition,
    ) -> Result<(), StorageError> {
        let mut page_id = self.index_catalog_root;
        let page_count = self.buffer.page_count();
        let mut visited = HashSet::new();
        let (tail_page, mut tail_node) = loop {
            if !visited.insert(page_id) || visited.len() as u64 > page_count {
                return Err(IndexError::CatalogCycle { page_id }.into());
            }
            let page = self.buffer.read_page(page_id)?;
            if page.page().header()?.page_type != PageType::IndexCatalog {
                return Err(IndexError::InvalidNodeType.into());
            }
            let node = decode_index_catalog(page.page().single_payload(PageType::IndexCatalog)?)?;
            let before = page.page().clone();
            drop(page);
            match node.next_catalog {
                Some(next) => {
                    if next.0 == 0 || next.0 >= page_count {
                        return Err(IndexError::InvalidChild(next).into());
                    }
                    page_id = next;
                }
                None => break (before, node),
            }
        };

        tail_node.definitions.push(definition.clone());
        let payload = encode_index_catalog(&tail_node)?;
        if payload.len() <= self.index_catalog_payload_capacity() {
            let mut after = tail_page.clone();
            after.replace_single_payload(PageType::IndexCatalog, &payload)?;
            #[cfg(test)]
            if std::mem::take(&mut self.fail_index_catalog_log) {
                transaction.inject_partial_append_failure(0);
            }
            if let Err(error) = transaction.log_page_update(&tail_page, &mut after) {
                transaction.require_rollback();
                return Err(error);
            }
            #[cfg(test)]
            crate::crash_test::maybe_crash(
                crate::crash_test::TestCrashPoint::IndexBuildAfterCatalogLog,
            );
            if let Err(error) = self.publish_page_image(page_id, after) {
                transaction.require_rollback();
                return Err(error);
            }
            #[cfg(test)]
            self.crash_after_catalog_publish()?;
            return Ok(());
        }

        tail_node.definitions.pop();
        let new_page_id = PageId(page_count);
        let new_node = IndexCatalogNode {
            next_catalog: None,
            definitions: vec![definition.clone()],
        };
        let mut new_after = Page::new(new_page_id, PageType::IndexCatalog);
        new_after
            .initialize_single_payload(PageType::IndexCatalog, &encode_index_catalog(&new_node)?)?;
        if let Err(error) = transaction.log_page_update(&Page::zero(new_page_id), &mut new_after) {
            transaction.require_rollback();
            return Err(error);
        }

        tail_node.next_catalog = Some(new_page_id);
        let mut tail_after = tail_page.clone();
        tail_after
            .replace_single_payload(PageType::IndexCatalog, &encode_index_catalog(&tail_node)?)?;
        let tail_lsn = match transaction.log_page_update(&tail_page, &mut tail_after) {
            Ok(lsn) => lsn,
            Err(error) => {
                transaction.require_rollback();
                return Err(error);
            }
        };
        if let Err(error) = transaction.flush_through(tail_lsn) {
            transaction.require_rollback();
            return Err(error);
        }
        let mut new_page = match self.buffer.new_page() {
            Ok(page) => page,
            Err(error) => {
                transaction.require_rollback();
                return Err(error);
            }
        };
        if new_page.page_id() != new_page_id {
            transaction.require_rollback();
            return Err(IndexError::InvalidChild(new_page.page_id()).into());
        }
        *new_page.page_mut() = new_after;
        drop(new_page);
        if let Err(error) = self.publish_page_image(page_id, tail_after) {
            transaction.require_rollback();
            return Err(error);
        }
        #[cfg(test)]
        self.crash_after_catalog_publish()?;
        Ok(())
    }

    fn index_catalog_payload_capacity(&self) -> usize {
        #[cfg(test)]
        if let Some(capacity) = self.index_catalog_payload_capacity {
            return capacity;
        }
        Page::single_payload_capacity()
    }

    #[cfg(test)]
    fn crash_after_catalog_publish(&self) -> Result<(), StorageError> {
        if crate::crash_test::is_enabled(
            crate::crash_test::TestCrashPoint::IndexBuildAfterCatalogPublish,
        ) {
            self.buffer.flush_all()?;
            crate::crash_test::maybe_crash(
                crate::crash_test::TestCrashPoint::IndexBuildAfterCatalogPublish,
            );
        }
        Ok(())
    }

    #[cfg(test)]
    fn inject_index_catalog_log_failure(&mut self) {
        self.fail_index_catalog_log = true;
    }

    pub fn insert(&mut self, values: &[ScalarValue]) -> Result<RowId, StorageError> {
        let mut transaction = self.begin_transaction()?;
        match self.insert_in(&mut transaction, values) {
            Ok(row_id) => {
                transaction.commit()?;
                Ok(row_id)
            }
            Err(error) => match transaction.rollback() {
                Ok(()) => Err(error),
                Err(rollback_error) => Err(rollback_error),
            },
        }
    }

    /// Replaces a row and returns its current physical locator. The locator is
    /// unchanged when the replacement fits its source page; otherwise the row
    /// is relocated and the old locator becomes a tombstone.
    pub fn update(&mut self, row_id: RowId, values: &[ScalarValue]) -> Result<RowId, StorageError> {
        let mut transaction = self.begin_transaction()?;
        match self.update_in(&mut transaction, row_id, values) {
            Ok(current_row_id) => {
                transaction.commit()?;
                Ok(current_row_id)
            }
            Err(error) => match transaction.rollback() {
                Ok(()) => Err(error),
                Err(rollback_error) => Err(rollback_error),
            },
        }
    }

    pub fn delete(&mut self, row_id: RowId) -> Result<(), StorageError> {
        let mut transaction = self.begin_transaction()?;
        match self.delete_in(&mut transaction, row_id) {
            Ok(()) => {
                transaction.commit()?;
                Ok(())
            }
            Err(error) => match transaction.rollback() {
                Ok(()) => Err(error),
                Err(rollback_error) => Err(rollback_error),
            },
        }
    }

    pub fn begin_transaction(&mut self) -> Result<Transaction, StorageError> {
        self.transactions.begin()
    }

    pub(crate) fn buffer(&self) -> &BufferPool {
        &self.buffer
    }

    /// Verifies that a transaction is active and belongs to this heap. DML
    /// executors call this even when a predicate selects no rows.
    pub fn validate_transaction(&self, transaction: &Transaction) -> Result<(), StorageError> {
        if !transaction.belongs_to(self.transactions.wal()) {
            return Err(TransactionError::ForeignTransaction {
                txn_id: transaction.id(),
            }
            .into());
        }
        transaction.ensure_active()
    }

    pub fn insert_in(
        &mut self,
        transaction: &mut Transaction,
        values: &[ScalarValue],
    ) -> Result<RowId, StorageError> {
        self.validate_transaction(transaction)?;
        self.validate_row(values)?;
        let payload = encode_row(values)?;
        let max_record_size = PAGE_SIZE - PAGE_HEADER_SIZE - SLOT_SIZE;
        if payload.len() > max_record_size {
            return Err(PageError::RecordTooLarge {
                size: payload.len(),
                capacity: max_record_size,
            }
            .into());
        }
        transaction.acquire_writer()?;
        let plans = self.index_plans.clone();
        for plan in &plans {
            ensure_key_fits(
                &plan.spec,
                &values[plan.column_position],
                Page::single_payload_capacity(),
            )?;
        }

        let row_id = self.insert_heap_in(transaction, &payload)?;
        #[cfg(test)]
        self.crash_after_registered_publish(
            crate::crash_test::TestCrashPoint::RegisteredInsertAfterHeapPublish,
        )?;
        for (completed_index_mutations, plan) in plans.into_iter().enumerate() {
            self.maybe_fail_registered_mutation(transaction, completed_index_mutations);
            let result = self.btree().insert_in(
                transaction,
                plan.definition.handle,
                values[plan.column_position].clone(),
                row_id,
            );
            if let Err(error) = result {
                transaction.require_rollback();
                return Err(error);
            }
        }
        Ok(row_id)
    }

    pub fn read_row(&self, row_id: RowId) -> Result<Vec<ScalarValue>, StorageError> {
        self.ensure_row_page(row_id)?;
        let page = self.buffer.read_page(row_id.page)?;
        let slot = validate_row_slot(page.page(), row_id)?;
        decode_row(
            page.page()
                .read_record(slot)
                .map_err(|error| map_row_error(error, row_id))?,
            &self.table,
        )
    }

    /// Transactional form of [`Self::update`]. A failure after relocation has
    /// appended partial physical history leaves the transaction requiring
    /// rollback, so it cannot commit or perform another write.
    pub fn update_in(
        &mut self,
        transaction: &mut Transaction,
        row_id: RowId,
        values: &[ScalarValue],
    ) -> Result<RowId, StorageError> {
        self.validate_transaction(transaction)?;
        let old_values = self.read_row(row_id)?;
        self.validate_row(values)?;
        let payload = encode_row(values)?;
        let max_record_size = PAGE_SIZE - PAGE_HEADER_SIZE - SLOT_SIZE;
        if payload.len() > max_record_size {
            return Err(PageError::RecordTooLarge {
                size: payload.len(),
                capacity: max_record_size,
            }
            .into());
        }
        transaction.acquire_writer()?;
        let plans = self.index_plans.clone();
        for plan in &plans {
            let old_key = &old_values[plan.column_position];
            let new_key = &values[plan.column_position];
            ensure_key_fits(&plan.spec, new_key, Page::single_payload_capacity())?;
            if !self
                .btree()
                .contains_exact(plan.definition.handle, old_key, row_id)?
            {
                return Err(IndexError::EntryNotFound.into());
            }
        }

        let current_row_id = self.update_heap_in(transaction, row_id, &payload)?;
        #[cfg(test)]
        self.crash_after_registered_publish(
            crate::crash_test::TestCrashPoint::RegisteredUpdateAfterHeapPublish,
        )?;
        let mut completed_index_mutations = 0;
        for plan in plans {
            let old_key = &old_values[plan.column_position];
            let new_key = &values[plan.column_position];
            if old_key == new_key && row_id == current_row_id {
                continue;
            }

            self.maybe_fail_registered_mutation(transaction, completed_index_mutations);
            if let Err(error) =
                self.btree()
                    .delete_in(transaction, plan.definition.handle, old_key.clone(), row_id)
            {
                transaction.require_rollback();
                return Err(error);
            }
            completed_index_mutations += 1;

            self.maybe_fail_registered_mutation(transaction, completed_index_mutations);
            if let Err(error) = self.btree().insert_in(
                transaction,
                plan.definition.handle,
                new_key.clone(),
                current_row_id,
            ) {
                transaction.require_rollback();
                return Err(error);
            }
            completed_index_mutations += 1;
        }
        Ok(current_row_id)
    }

    fn update_heap_in(
        &mut self,
        transaction: &mut Transaction,
        row_id: RowId,
        payload: &[u8],
    ) -> Result<RowId, StorageError> {
        self.ensure_row_page(row_id)?;
        let source_before = {
            let page = self.buffer.read_page(row_id.page)?;
            validate_row_slot(page.page(), row_id)?;
            page.page().clone()
        };
        let slot = validate_row_slot(&source_before, row_id)?;
        let mut source_replacement = source_before.clone();
        match source_replacement.replace_record(slot, payload) {
            Ok(()) => {
                let mut page = self.buffer.write_page(row_id.page)?;
                let before = page.page().clone();
                let mut after = before.clone();
                let slot = validate_row_slot(&after, row_id)?;
                after
                    .replace_record(slot, payload)
                    .map_err(|error| map_row_error(error, row_id))?;
                transaction.log_page_update(&before, &mut after)?;
                *page.page_mut() = after;
                Ok(row_id)
            }
            Err(StorageError::Page(PageError::UpdateWouldOverflowPage { .. })) => {
                self.relocate_update(transaction, row_id, slot, source_before, payload)
            }
            Err(error) => Err(map_row_error(error, row_id)),
        }
    }

    pub fn delete_in(
        &mut self,
        transaction: &mut Transaction,
        row_id: RowId,
    ) -> Result<(), StorageError> {
        self.validate_transaction(transaction)?;
        let old_values = self.read_row(row_id)?;
        transaction.acquire_writer()?;
        let plans = self.index_plans.clone();
        for plan in &plans {
            if !self.btree().contains_exact(
                plan.definition.handle,
                &old_values[plan.column_position],
                row_id,
            )? {
                return Err(IndexError::EntryNotFound.into());
            }
        }

        let mut completed_index_mutations = 0;
        for plan in plans {
            self.maybe_fail_registered_mutation(transaction, completed_index_mutations);
            if let Err(error) = self.btree().delete_in(
                transaction,
                plan.definition.handle,
                old_values[plan.column_position].clone(),
                row_id,
            ) {
                if completed_index_mutations != 0 {
                    transaction.require_rollback();
                }
                return Err(error);
            }
            completed_index_mutations += 1;
            #[cfg(test)]
            if completed_index_mutations == 1 {
                self.crash_after_registered_publish(
                    crate::crash_test::TestCrashPoint::RegisteredDeleteAfterFirstIndexPublish,
                )?;
            }
        }

        if let Err(error) = self.delete_heap_in(transaction, row_id) {
            if completed_index_mutations != 0 {
                transaction.require_rollback();
            }
            return Err(error);
        }
        Ok(())
    }

    fn delete_heap_in(
        &mut self,
        transaction: &mut Transaction,
        row_id: RowId,
    ) -> Result<(), StorageError> {
        let mut page = self.buffer.write_page(row_id.page)?;
        let before = page.page().clone();
        let mut after = before.clone();
        let slot = validate_row_slot(&after, row_id)?;
        after
            .delete_record(slot)
            .map_err(|error| map_row_error(error, row_id))?;
        transaction.log_page_update(&before, &mut after)?;
        *page.page_mut() = after;
        Ok(())
    }

    fn insert_heap_in(
        &mut self,
        transaction: &mut Transaction,
        payload: &[u8],
    ) -> Result<RowId, StorageError> {
        let prepared = self.prepare_insert(payload, None)?;
        self.apply_single_page_insert(transaction, prepared)
    }

    fn build_registered_index_plans(&mut self) -> Result<Vec<RegisteredIndexPlan>, StorageError> {
        let definitions = self.indexes.clone();
        let mut plans = Vec::with_capacity(definitions.len());
        for definition in definitions {
            let (column_position, column) = self
                .table
                .columns
                .iter()
                .enumerate()
                .find(|(_, column)| column.id == definition.column_id)
                .ok_or(IndexError::UnknownIndexColumn {
                    column_id: definition.column_id,
                })?;
            let spec = IndexSpec {
                data_type: column.semantic_type(),
                nullable: column.nullable,
            };
            if self.btree().spec(definition.handle)? != spec {
                return Err(IndexError::CatalogSpecMismatch {
                    column_id: definition.column_id,
                }
                .into());
            }
            plans.push(RegisteredIndexPlan {
                definition,
                column_position,
                spec,
            });
        }
        Ok(plans)
    }

    #[cfg(test)]
    fn maybe_fail_registered_mutation(&mut self, transaction: &Transaction, completed: usize) {
        if self.fail_registered_mutation_after == Some(completed) {
            self.fail_registered_mutation_after = None;
            transaction.inject_partial_append_failure(0);
        }
    }

    #[cfg(not(test))]
    fn maybe_fail_registered_mutation(&mut self, _transaction: &Transaction, _completed: usize) {}

    #[cfg(test)]
    fn inject_registered_mutation_failure_after(&mut self, completed: usize) {
        self.fail_registered_mutation_after = Some(completed);
    }

    #[cfg(test)]
    fn crash_after_registered_publish(
        &self,
        point: crate::crash_test::TestCrashPoint,
    ) -> Result<(), StorageError> {
        if crate::crash_test::is_enabled(point) {
            // Force the deliberately mixed uncommitted Heap/index state to
            // disk so startup recovery must undo the whole WAL chain.
            self.buffer.flush_all()?;
            crate::crash_test::crash_now();
        }
        Ok(())
    }

    fn prepare_insert(
        &self,
        payload: &[u8],
        excluded_page: Option<PageId>,
    ) -> Result<PreparedInsert, StorageError> {
        let page_count = self.buffer.page_count();
        for page_number in FIRST_MANAGED_PAGE.0..page_count {
            let page_id = PageId(page_number);
            if excluded_page == Some(page_id) {
                continue;
            }
            let page = self.buffer.read_page(page_id)?;
            let before = page.page().clone();
            drop(page);
            let page_type = before.header()?.page_type;
            if page_type != PageType::Heap {
                before.single_payload(page_type)?;
                continue;
            }
            let mut after = before.clone();
            match after.insert_record(payload) {
                Ok(slot_ref) => {
                    return Ok(PreparedInsert {
                        page_id,
                        before,
                        after,
                        slot_ref,
                        new_page: false,
                    });
                }
                Err(StorageError::Page(PageError::PageFull { .. })) => {}
                Err(error) => return Err(error),
            }
        }

        let page_id = PageId(page_count);
        let before = Page::zero(page_id);
        let mut after = Page::new(page_id, PageType::Heap);
        let slot_ref = after.insert_record(payload)?;
        Ok(PreparedInsert {
            page_id,
            before,
            after,
            slot_ref,
            new_page: true,
        })
    }

    fn apply_single_page_insert(
        &mut self,
        transaction: &mut Transaction,
        mut prepared: PreparedInsert,
    ) -> Result<RowId, StorageError> {
        let update_lsn = transaction.log_page_update(&prepared.before, &mut prepared.after)?;
        let publication = if prepared.new_page {
            transaction
                .flush_through(update_lsn)
                .and_then(|()| self.publish_new_page(&prepared))
        } else {
            self.publish_existing_page(&prepared)
        };
        if let Err(error) = publication {
            transaction.require_rollback();
            return Err(error);
        }
        Ok(prepared.row_id())
    }

    fn relocate_update(
        &mut self,
        transaction: &mut Transaction,
        row_id: RowId,
        source_slot: SlotId,
        source_before: Page,
        payload: &[u8],
    ) -> Result<RowId, StorageError> {
        let mut source_after = source_before.clone();
        source_after.delete_record(source_slot)?;
        let mut destination = self.prepare_insert(payload, Some(row_id.page))?;
        transaction.acquire_writer()?;

        transaction.log_page_update(&destination.before, &mut destination.after)?;
        #[cfg(test)]
        crate::crash_test::maybe_crash(
            crate::crash_test::TestCrashPoint::RelocationAfterFirstPageUpdateLog,
        );
        #[cfg(test)]
        if std::mem::take(&mut self.fail_relocation_second_log) {
            self.transactions
                .wal()
                .try_borrow_mut()
                .map_err(|_| TransactionError::WalBusy)?
                .inject_partial_append_failure(0);
        }
        let source_lsn = match transaction.log_page_update(&source_before, &mut source_after) {
            Ok(lsn) => lsn,
            Err(error) => {
                transaction.require_rollback();
                return Err(error);
            }
        };
        #[cfg(test)]
        crate::crash_test::maybe_crash(
            crate::crash_test::TestCrashPoint::RelocationAfterBothPageUpdateLogs,
        );

        let publish_destination = if destination.new_page {
            transaction
                .flush_through(source_lsn)
                .and_then(|()| self.publish_new_page(&destination))
        } else {
            self.publish_existing_page(&destination)
        };
        if let Err(error) = publish_destination {
            transaction.require_rollback();
            return Err(error);
        }
        #[cfg(test)]
        if std::mem::take(&mut self.fail_relocation_source_publish) {
            self.buffer.inject_page_write_failure();
        }
        #[cfg(test)]
        if crate::crash_test::is_enabled(
            crate::crash_test::TestCrashPoint::RelocationAfterFirstPagePublish,
        ) {
            // Exercise the mixed STEAL state: destination is durable while
            // source still contains its before-image and no Commit exists.
            if let Err(error) = self.buffer.flush_page(destination.page_id) {
                transaction.require_rollback();
                return Err(error);
            }
            crate::crash_test::crash_now();
        }
        if let Err(error) = self.publish_page_image(row_id.page, source_after) {
            transaction.require_rollback();
            return Err(error);
        }
        Ok(destination.row_id())
    }

    fn publish_existing_page(&self, prepared: &PreparedInsert) -> Result<(), StorageError> {
        self.publish_page_image(prepared.page_id, prepared.after.clone())
    }

    fn publish_page_image(&self, page_id: PageId, image: Page) -> Result<(), StorageError> {
        let mut page = self.buffer.write_page(page_id)?;
        *page.page_mut() = image;
        Ok(())
    }

    fn publish_new_page(&self, prepared: &PreparedInsert) -> Result<(), StorageError> {
        let mut page = self.buffer.new_page()?;
        let actual_page_id = page.page_id();
        if actual_page_id != prepared.page_id {
            return Err(crate::invalid_format(format!(
                "allocated page {}, expected {}",
                actual_page_id.0, prepared.page_id.0
            )));
        }
        *page.page_mut() = prepared.after.clone();
        Ok(())
    }

    pub fn scan(&mut self) -> Result<Vec<(RowId, Vec<ScalarValue>)>, StorageError> {
        let mut rows = Vec::new();
        for page_number in FIRST_MANAGED_PAGE.0..self.buffer.page_count() {
            let page_id = PageId(page_number);
            let page = self.buffer.read_page(page_id)?;
            let header = page.page().header()?;
            if header.page_type != PageType::Heap {
                page.page().single_payload(header.page_type)?;
                continue;
            }
            for slot_number in 0..header.slot_count {
                let slot = SlotId(slot_number);
                if let SlotState::Live(slot_entry) = page.page().slot_state(slot)? {
                    let values = decode_row(page.page().read_record(slot)?, &self.table)?;
                    rows.push((
                        RowId {
                            page: page_id,
                            slot: slot.0,
                            generation: slot_entry.generation,
                        },
                        values,
                    ));
                }
            }
        }
        Ok(rows)
    }

    pub fn flush(&self) -> Result<(), StorageError> {
        let written = self
            .transactions
            .wal()
            .try_borrow()
            .map_err(|_| TransactionError::WalBusy)?
            .written_lsn();
        if let Some(lsn) = written {
            self.transactions
                .wal()
                .try_borrow_mut()
                .map_err(|_| TransactionError::WalBusy)?
                .flush_through(lsn)?;
        }
        self.buffer.flush_all()
    }

    /// Establishes a quiescent recovery boundary and starts a new bounded WAL
    /// generation. The method never waits for transaction handles to finish.
    pub fn checkpoint(&mut self) -> Result<(), StorageError> {
        self.transactions.ensure_checkpoint_safe()?;
        let written = self
            .transactions
            .wal()
            .try_borrow()
            .map_err(|_| TransactionError::WalBusy)?
            .written_lsn();
        if let Some(lsn) = written {
            self.transactions
                .wal()
                .try_borrow_mut()
                .map_err(|_| TransactionError::WalBusy)?
                .flush_through(lsn)?;
        }
        // flush_all preserves WAL-before-page for each frame and syncs the
        // database file before the old generation becomes recyclable.
        self.buffer.flush_all()?;
        let next_txn_id = self.transactions.next_txn_id();
        self.transactions
            .wal()
            .try_borrow_mut()
            .map_err(|_| TransactionError::WalBusy)?
            .rotate(next_txn_id)?;
        Ok(())
    }

    pub fn close(self) -> Result<(), StorageError> {
        self.transactions.ensure_clean_close()?;
        self.flush()
    }

    #[must_use]
    pub fn table(&self) -> &TableDef {
        &self.table
    }

    #[cfg(test)]
    fn wal_records(&self) -> Result<Vec<crate::WalRecord>, StorageError> {
        Ok(self
            .transactions
            .wal()
            .try_borrow_mut()
            .map_err(|_| TransactionError::WalBusy)?
            .scan()?)
    }

    #[cfg(test)]
    fn durable_lsn(&self) -> Result<Option<netbadb_types::Lsn>, StorageError> {
        Ok(self
            .transactions
            .wal()
            .try_borrow()
            .map_err(|_| TransactionError::WalBusy)?
            .durable_lsn())
    }

    #[cfg(test)]
    fn wal_generation(&self) -> Result<u64, StorageError> {
        Ok(self
            .transactions
            .wal()
            .try_borrow()
            .map_err(|_| TransactionError::WalBusy)?
            .generation())
    }

    #[cfg(test)]
    fn current_wal_path(&self) -> Result<std::path::PathBuf, StorageError> {
        Ok(self
            .transactions
            .wal()
            .try_borrow()
            .map_err(|_| TransactionError::WalBusy)?
            .path()
            .to_owned())
    }

    #[cfg(test)]
    fn inject_partial_checkpoint_rotation(&self, after_bytes: usize) -> Result<(), StorageError> {
        self.transactions
            .wal()
            .try_borrow_mut()
            .map_err(|_| TransactionError::WalBusy)?
            .inject_partial_rotation_failure(after_bytes);
        Ok(())
    }

    #[cfg(test)]
    fn inject_relocation_second_log_failure(&mut self) {
        self.fail_relocation_second_log = true;
    }

    #[cfg(test)]
    fn inject_relocation_source_publish_failure(&mut self) {
        self.fail_relocation_source_publish = true;
    }

    #[cfg(test)]
    pub(crate) fn simulate_crash(mut self) {
        self.skip_drop_flush = true;
    }

    fn validate_row(&self, values: &[ScalarValue]) -> Result<(), StorageError> {
        if values.len() != self.table.columns.len() {
            return Err(StorageError::InvalidRowLength {
                expected: self.table.columns.len(),
                actual: values.len(),
            });
        }
        for (value, column) in values.iter().zip(&self.table.columns) {
            if matches!(value, ScalarValue::Null) {
                if !column.nullable {
                    return Err(StorageError::NullNotAllowed {
                        column: column.name.clone(),
                    });
                }
                continue;
            }
            if !value.matches_type(&column.semantic_type()) {
                return Err(StorageError::TypeMismatch {
                    column: column.name.clone(),
                    expected: column.semantic_type().physical,
                    actual: value.physical_type(),
                });
            }
        }
        Ok(())
    }

    fn ensure_row_page(&self, row_id: RowId) -> Result<(), StorageError> {
        if row_id.page < FIRST_MANAGED_PAGE || row_id.page.0 >= self.buffer.page_count() {
            return Err(StorageError::RowNotFound { row_id });
        }
        Ok(())
    }
}

fn map_row_error(error: StorageError, row_id: RowId) -> StorageError {
    match error {
        StorageError::Page(PageError::InvalidSlot { .. }) => StorageError::RowNotFound { row_id },
        StorageError::Page(PageError::SlotDeleted { .. }) => StorageError::RowDeleted { row_id },
        other => other,
    }
}

fn validate_row_slot(page: &Page, row_id: RowId) -> Result<SlotId, StorageError> {
    if page.header()?.page_type != PageType::Heap {
        return Err(StorageError::RowNotFound { row_id });
    }
    let slot = SlotId(row_id.slot);
    let state = page
        .slot_state(slot)
        .map_err(|error| map_row_error(error, row_id))?;
    let actual_generation = match state {
        SlotState::Live(slot) => slot.generation,
        SlotState::Deleted { generation } => generation,
    };
    if row_id.generation != actual_generation {
        return Err(StorageError::StaleRowId {
            row_id,
            actual_generation,
        });
    }
    match state {
        SlotState::Live(_) => Ok(slot),
        SlotState::Deleted { .. } => Err(StorageError::RowDeleted { row_id }),
    }
}

impl Drop for HeapStorage {
    fn drop(&mut self) {
        // Explicit `flush`/`close` report errors. Drop only preserves the old
        // embedded behavior with best-effort cleanup and is not durability.
        #[cfg(test)]
        if self.skip_drop_flush {
            return;
        }
        let _ = self.buffer.flush_all();
    }
}

fn validate_table(table: &TableDef) -> Result<SchemaFingerprint, StorageError> {
    table.validate()?;
    if table.columns.len() > u16::MAX as usize {
        return Err(crate::invalid_format("table has more than 65535 columns"));
    }
    Ok(table.fingerprint()?)
}

fn write_heap_metadata(
    bytes: &mut [u8; PAGE_SIZE],
    table: &TableDef,
    fingerprint: SchemaFingerprint,
    index_catalog_root: PageId,
) {
    bytes[HEAP_METADATA_OFFSET..HEAP_METADATA_OFFSET + HEADER_MAGIC.len()]
        .copy_from_slice(HEADER_MAGIC);
    bytes[HEAP_VERSION_OFFSET..HEAP_VERSION_OFFSET + 2]
        .copy_from_slice(&HEAP_FORMAT_VERSION.to_le_bytes());
    bytes[HEAP_RESERVED_OFFSET..HEAP_RESERVED_OFFSET + 2].fill(0);
    bytes[HEAP_TABLE_ID_OFFSET..HEAP_TABLE_ID_OFFSET + 8]
        .copy_from_slice(&table.id.0.to_le_bytes());
    bytes[HEAP_COLUMN_COUNT_OFFSET..HEAP_COLUMN_COUNT_OFFSET + 2]
        .copy_from_slice(&(table.columns.len() as u16).to_le_bytes());
    bytes[HEAP_SCHEMA_FINGERPRINT_OFFSET
        ..HEAP_SCHEMA_FINGERPRINT_OFFSET + SchemaFingerprint::LENGTH]
        .copy_from_slice(fingerprint.as_bytes());
    bytes[HEAP_INDEX_CATALOG_ROOT_OFFSET..HEAP_INDEX_CATALOG_ROOT_OFFSET + 8]
        .copy_from_slice(&index_catalog_root.0.to_le_bytes());
    bytes[HEAP_TRAILING_RESERVED_OFFSET..HEAP_TRAILING_RESERVED_END].fill(0);
}

fn validate_heap_metadata(
    bytes: &[u8; PAGE_SIZE],
    table: &TableDef,
    expected_fingerprint: SchemaFingerprint,
) -> Result<PageId, StorageError> {
    if &bytes[HEAP_METADATA_OFFSET..HEAP_METADATA_OFFSET + HEADER_MAGIC.len()] != HEADER_MAGIC {
        return Err(MetadataError::InvalidMagic.into());
    }
    let version = read_u16(bytes, HEAP_VERSION_OFFSET)?;
    if version != HEAP_FORMAT_VERSION {
        return Err(MetadataError::UnsupportedVersion(version).into());
    }
    if bytes[HEAP_RESERVED_OFFSET..HEAP_RESERVED_OFFSET + 2]
        .iter()
        .any(|byte| *byte != 0)
    {
        return Err(MetadataError::InvalidReservedBytes.into());
    }
    let table_id = read_u64(bytes, HEAP_TABLE_ID_OFFSET)?;
    let column_count = usize::from(read_u16(bytes, HEAP_COLUMN_COUNT_OFFSET)?);
    if table_id != table.id.0 {
        return Err(StorageError::TableIdMismatch {
            expected: table.id,
            actual: netbadb_types::TableId(table_id),
        });
    }
    let actual_fingerprint =
        SchemaFingerprint::from_bytes(read_array_at(bytes, HEAP_SCHEMA_FINGERPRINT_OFFSET)?);
    if actual_fingerprint != expected_fingerprint {
        return Err(StorageError::SchemaMismatch {
            expected: expected_fingerprint,
            actual: actual_fingerprint,
        });
    }
    if column_count != table.columns.len() {
        return Err(MetadataError::InvalidColumnCount {
            stored: column_count as u16,
            expected: table.columns.len(),
        }
        .into());
    }
    if bytes[HEAP_TRAILING_RESERVED_OFFSET..HEAP_TRAILING_RESERVED_END]
        .iter()
        .any(|byte| *byte != 0)
    {
        return Err(MetadataError::InvalidReservedBytes.into());
    }
    let catalog_root = PageId(read_u64(bytes, HEAP_INDEX_CATALOG_ROOT_OFFSET)?);
    if catalog_root.0 == 0 {
        return Err(IndexError::InvalidChild(catalog_root).into());
    }
    Ok(catalog_root)
}

fn validate_catalog_root_bounds(catalog_root: PageId, page_count: u64) -> Result<(), StorageError> {
    if catalog_root.0 >= page_count {
        return Err(IndexError::InvalidChild(catalog_root).into());
    }
    Ok(())
}

fn encode_row(values: &[ScalarValue]) -> Result<Vec<u8>, StorageError> {
    let mut encoded = Vec::new();
    for value in values {
        match value {
            ScalarValue::Bool(value) => {
                encoded.push(0);
                encoded.push(u8::from(*value));
            }
            ScalarValue::Int64(value) => {
                encoded.push(1);
                encoded.extend_from_slice(&value.to_le_bytes());
            }
            ScalarValue::UInt64(value) => {
                encoded.push(2);
                encoded.extend_from_slice(&value.to_le_bytes());
            }
            ScalarValue::Text(value) => {
                encoded.push(3);
                let length = u32::try_from(value.len()).map_err(|_| StorageError::RowTooLarge {
                    size: value.len(),
                    capacity: PAGE_SIZE - PAGE_HEADER_SIZE - SLOT_SIZE,
                })?;
                encoded.extend_from_slice(&length.to_le_bytes());
                encoded.extend_from_slice(value.as_bytes());
            }
            ScalarValue::Null => encoded.push(4),
        }
    }
    Ok(encoded)
}

fn decode_row(payload: &[u8], table: &TableDef) -> Result<Vec<ScalarValue>, StorageError> {
    let mut offset = 0;
    let mut values = Vec::with_capacity(table.columns.len());
    for column in &table.columns {
        let value = decode_value(payload, &mut offset)?;
        if matches!(value, ScalarValue::Null) {
            if !column.nullable {
                return Err(StorageError::NullNotAllowed {
                    column: column.name.clone(),
                });
            }
        } else if !value.matches_type(&column.semantic_type()) {
            return Err(StorageError::TypeMismatch {
                column: column.name.clone(),
                expected: column.semantic_type().physical,
                actual: value.physical_type(),
            });
        }
        values.push(value);
    }
    if offset != payload.len() {
        return Err(CodecError::ExtraValues.into());
    }
    Ok(values)
}

fn decode_value(payload: &[u8], offset: &mut usize) -> Result<ScalarValue, StorageError> {
    let tag = *payload.get(*offset).ok_or(CodecError::MissingScalarTag)?;
    *offset += 1;
    match tag {
        0 => {
            let value = read_byte(payload, offset)?;
            match value {
                0 => Ok(ScalarValue::Bool(false)),
                1 => Ok(ScalarValue::Bool(true)),
                other => Err(CodecError::InvalidBoolean(other).into()),
            }
        }
        1 => Ok(ScalarValue::Int64(i64::from_le_bytes(read_array(
            payload, offset,
        )?))),
        2 => Ok(ScalarValue::UInt64(u64::from_le_bytes(read_array(
            payload, offset,
        )?))),
        3 => {
            let length = u32::from_le_bytes(read_array(payload, offset)?) as usize;
            let end = (*offset)
                .checked_add(length)
                .ok_or(CodecError::LengthOverflow)?;
            let text_bytes = payload
                .get(*offset..end)
                .ok_or(CodecError::ScalarTruncated)?;
            let text = std::str::from_utf8(text_bytes)
                .map_err(|_| CodecError::TextNotUtf8)?
                .to_owned();
            *offset = end;
            Ok(ScalarValue::Text(text))
        }
        4 => Ok(ScalarValue::Null),
        other => Err(CodecError::UnknownScalarTag(other).into()),
    }
}

fn read_byte(bytes: &[u8], offset: &mut usize) -> Result<u8, StorageError> {
    let byte = *bytes.get(*offset).ok_or(CodecError::ScalarTruncated)?;
    *offset += 1;
    Ok(byte)
}

fn read_array<const N: usize>(bytes: &[u8], offset: &mut usize) -> Result<[u8; N], StorageError> {
    let end = (*offset).checked_add(N).ok_or(CodecError::LengthOverflow)?;
    let source = bytes.get(*offset..end).ok_or(CodecError::ScalarTruncated)?;
    let mut output = [0; N];
    output.copy_from_slice(source);
    *offset = end;
    Ok(output)
}

fn read_u16(bytes: &[u8], offset: usize) -> Result<u16, StorageError> {
    Ok(u16::from_le_bytes(read_array_at(bytes, offset)?))
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64, StorageError> {
    Ok(u64::from_le_bytes(read_array_at(bytes, offset)?))
}

fn read_array_at<const N: usize>(bytes: &[u8], offset: usize) -> Result<[u8; N], StorageError> {
    let end = offset
        .checked_add(N)
        .ok_or_else(|| crate::invalid_format("metadata offset overflows"))?;
    let source = bytes
        .get(offset..end)
        .ok_or_else(|| crate::invalid_format("metadata is truncated"))?;
    let mut output = [0; N];
    output.copy_from_slice(source);
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::{HeapStorage, decode_row, encode_row};
    use crate::crash_test::{self, TestCrashPoint};
    use crate::{
        BufferError, CheckpointError, PageError, PageManager, PageType, SlotId, StorageError,
        TransactionError, TransactionState, WAL_HEADER_SIZE, WAL_MAX_RECORD_SIZE, WalError,
        WalManager, WalRecordKind, wal_alternate_path, wal_path,
    };
    use netbadb_index::{
        BTreeHandle, IndexCatalogNode, IndexError, IndexSpec, decode_index_catalog,
        encode_index_catalog,
    };
    use netbadb_schema::{ColumnDef, SchemaError, TableDef, TypeSpec};
    use netbadb_types::{ColumnId, Lsn, PageId, PhysicalType, ScalarValue, SemanticType, TableId};

    const FIRST_HEAP_PAGE: PageId = PageId(2);

    fn table() -> TableDef {
        TableDef::new(
            TableId(1),
            "users",
            vec![
                ColumnDef::new(ColumnId(1), "id", TypeSpec::Physical(PhysicalType::Int64))
                    .primary_key(true),
                ColumnDef::new(ColumnId(2), "name", TypeSpec::Physical(PhysicalType::Text)),
            ],
        )
    }

    fn test_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("netbadb-{name}-{}", std::process::id()))
    }

    fn cleanup(path: &std::path::Path) {
        let _ = std::fs::remove_file(path);
        let wal = wal_path(path);
        let _ = std::fs::remove_file(wal_alternate_path(&wal));
        let _ = std::fs::remove_file(wal);
    }

    fn identity_table() -> TableDef {
        TableDef::new(
            TableId(17),
            "users",
            vec![
                ColumnDef::new(
                    ColumnId(1),
                    "id",
                    TypeSpec::Semantic {
                        name: "UserId".into(),
                        physical: PhysicalType::UInt64,
                    },
                )
                .primary_key(true),
                ColumnDef::new(
                    ColumnId(2),
                    "team_id",
                    TypeSpec::Semantic {
                        name: "TeamId".into(),
                        physical: PhysicalType::UInt64,
                    },
                ),
            ],
        )
    }

    fn indexed_table() -> TableDef {
        TableDef::new(
            TableId(23),
            "members",
            vec![
                ColumnDef::new(
                    ColumnId(1),
                    "id",
                    TypeSpec::Semantic {
                        name: "UserId".into(),
                        physical: PhysicalType::UInt64,
                    },
                ),
                ColumnDef::new(
                    ColumnId(2),
                    "team_id",
                    TypeSpec::Semantic {
                        name: "TeamId".into(),
                        physical: PhysicalType::UInt64,
                    },
                )
                .nullable(true),
                ColumnDef::new(ColumnId(3), "name", TypeSpec::Physical(PhysicalType::Text)),
            ],
        )
    }

    fn indexed_rows() -> Vec<Vec<ScalarValue>> {
        vec![
            vec![
                ScalarValue::UInt64(1),
                ScalarValue::UInt64(10),
                ScalarValue::Text("A".into()),
            ],
            vec![
                ScalarValue::UInt64(2),
                ScalarValue::UInt64(10),
                ScalarValue::Text("B".into()),
            ],
            vec![
                ScalarValue::UInt64(3),
                ScalarValue::UInt64(20),
                ScalarValue::Text("C".into()),
            ],
            vec![
                ScalarValue::UInt64(4),
                ScalarValue::Null,
                ScalarValue::Text("D".into()),
            ],
        ]
    }

    fn rewrite_catalog(path: &std::path::Path, mutate: impl FnOnce(&mut IndexCatalogNode)) {
        let mut pages = PageManager::open(path).expect("open catalog pages");
        let mut page = pages.read_page(PageId(1)).expect("read catalog root");
        let mut node = decode_index_catalog(
            page.single_payload(PageType::IndexCatalog)
                .expect("catalog payload"),
        )
        .expect("decode catalog");
        mutate(&mut node);
        let payload = encode_index_catalog(&node).expect("encode changed catalog");
        page.replace_single_payload(PageType::IndexCatalog, &payload)
            .expect("replace catalog payload");
        page.refresh_checksum();
        pages.write_page(&page).expect("write catalog root");
        pages.sync().expect("sync catalog root");
    }

    fn duplicate_first_catalog_entry(path: &std::path::Path) {
        let mut pages = PageManager::open(path).expect("open catalog pages");
        let mut page = pages.read_page(PageId(1)).expect("read catalog root");
        let mut payload = page
            .single_payload(PageType::IndexCatalog)
            .expect("catalog payload")
            .to_vec();
        assert_eq!(u32::from_le_bytes(payload[16..20].try_into().unwrap()), 1);
        let entry = payload[24..36].to_vec();
        payload[16..20].copy_from_slice(&2_u32.to_le_bytes());
        payload.extend_from_slice(&entry);
        page.replace_single_payload(PageType::IndexCatalog, &payload)
            .expect("replace duplicate catalog payload");
        page.refresh_checksum();
        pages.write_page(&page).expect("write catalog root");
        pages.sync().expect("sync catalog root");
    }

    #[test]
    fn registered_index_backfills_typed_null_and_duplicate_keys_and_reopens() {
        let path = test_path("registered-index-backfill");
        cleanup(&path);
        let schema = indexed_table();
        let mut storage = HeapStorage::create_with_buffer_pool_size(&path, schema.clone(), 1)
            .expect("create indexed heap");
        let mut row_ids = Vec::new();
        for row in indexed_rows() {
            row_ids.push(storage.insert(&row).expect("insert backfill row"));
        }

        let definition = storage
            .create_index(ColumnId(2))
            .expect("build registered index");
        assert_eq!(storage.indexes(), std::slice::from_ref(&definition));
        assert_eq!(storage.index_for_column(ColumnId(2)), Some(&definition));
        assert_eq!(
            storage
                .btree()
                .spec(definition.handle)
                .expect("registered spec"),
            IndexSpec {
                data_type: SemanticType::named("TeamId", PhysicalType::UInt64),
                nullable: true,
            }
        );
        assert_eq!(
            storage
                .btree()
                .lookup(definition.handle, &ScalarValue::UInt64(10))
                .expect("lookup duplicate key"),
            row_ids[..2]
        );
        assert_eq!(
            storage
                .btree()
                .lookup(definition.handle, &ScalarValue::UInt64(20))
                .expect("lookup key"),
            vec![row_ids[2]]
        );
        assert_eq!(
            storage
                .btree()
                .lookup(definition.handle, &ScalarValue::Null)
                .expect("lookup NULL"),
            vec![row_ids[3]]
        );

        let later = storage
            .insert(&[
                ScalarValue::UInt64(5),
                ScalarValue::UInt64(10),
                ScalarValue::Text("E".into()),
            ])
            .expect("insert after build");
        assert!(
            storage
                .btree()
                .lookup(definition.handle, &ScalarValue::UInt64(10))
                .expect("lookup maintained index")
                .contains(&later)
        );
        storage.close().expect("close indexed heap");

        let mut reopened = HeapStorage::open_with_buffer_pool_size(&path, schema, 1)
            .expect("discover registered index");
        assert_eq!(reopened.index_for_column(ColumnId(2)), Some(&definition));
        assert_eq!(
            reopened
                .btree()
                .lookup(definition.handle, &ScalarValue::UInt64(10))
                .expect("lookup reopened index"),
            vec![row_ids[0], row_ids[1], later]
        );
        reopened.close().expect("close reopened heap");
        cleanup(&path);
    }

    #[test]
    fn registered_index_on_empty_heap_is_persistent_and_empty() {
        let path = test_path("registered-index-empty");
        cleanup(&path);
        let mut storage = HeapStorage::create(&path, indexed_table()).expect("create empty heap");
        let definition = storage
            .create_index(ColumnId(2))
            .expect("create empty index");
        assert!(
            storage
                .btree()
                .lookup(definition.handle, &ScalarValue::UInt64(10))
                .expect("lookup empty index")
                .is_empty()
        );
        storage.close().expect("close empty index");
        let reopened = HeapStorage::open(&path, indexed_table()).expect("reopen empty index");
        assert_eq!(reopened.indexes(), std::slice::from_ref(&definition));
        cleanup(&path);
    }

    #[test]
    fn registered_indexes_track_insert_update_relocation_delete_null_and_duplicates() {
        let path = test_path("registered-index-dml");
        cleanup(&path);
        let schema = indexed_table();
        let mut storage = HeapStorage::create_with_buffer_pool_size(&path, schema.clone(), 1)
            .expect("create indexed heap");
        let team = storage.create_index(ColumnId(2)).expect("team index");
        let name = storage.create_index(ColumnId(3)).expect("name index");

        let first = storage
            .insert(&[
                ScalarValue::UInt64(1),
                ScalarValue::UInt64(10),
                ScalarValue::Text("A".into()),
            ])
            .expect("insert first");
        let duplicate = storage
            .insert(&[
                ScalarValue::UInt64(2),
                ScalarValue::UInt64(10),
                ScalarValue::Text("B".into()),
            ])
            .expect("insert duplicate key");
        storage
            .insert(&[
                ScalarValue::UInt64(3),
                ScalarValue::UInt64(30),
                ScalarValue::Text("f".repeat(1500)),
            ])
            .expect("fill source page for relocation");
        assert_eq!(
            storage
                .btree()
                .lookup(team.handle, &ScalarValue::UInt64(10))
                .expect("lookup duplicates"),
            vec![first, duplicate]
        );

        let page_updates_before = storage
            .wal_records()
            .unwrap()
            .iter()
            .filter(|record| matches!(record.kind, WalRecordKind::PageUpdate { .. }))
            .count();
        let same = storage
            .update(
                first,
                &[
                    ScalarValue::UInt64(1),
                    ScalarValue::UInt64(10),
                    ScalarValue::Text("A".into()),
                ],
            )
            .expect("unchanged update");
        assert_eq!(same, first);
        let page_updates_after = storage
            .wal_records()
            .unwrap()
            .iter()
            .filter(|record| matches!(record.kind, WalRecordKind::PageUpdate { .. }))
            .count();
        assert_eq!(page_updates_after - page_updates_before, 1);

        let key_changed = storage
            .update(
                first,
                &[
                    ScalarValue::UInt64(1),
                    ScalarValue::Null,
                    ScalarValue::Text("A".into()),
                ],
            )
            .expect("key change");
        assert_eq!(key_changed, first);
        assert_eq!(
            storage
                .btree()
                .lookup(team.handle, &ScalarValue::UInt64(10))
                .expect("old duplicate key"),
            vec![duplicate]
        );
        assert_eq!(
            storage
                .btree()
                .lookup(team.handle, &ScalarValue::Null)
                .expect("new NULL key"),
            vec![first]
        );

        let relocated = storage
            .update(
                first,
                &[
                    ScalarValue::UInt64(1),
                    ScalarValue::UInt64(20),
                    ScalarValue::Text("Z".repeat(3000)),
                ],
            )
            .expect("relocating key update");
        assert_ne!(relocated, first);
        for (handle, old_key, new_key) in [
            (team.handle, ScalarValue::Null, ScalarValue::UInt64(20)),
            (
                name.handle,
                ScalarValue::Text("A".into()),
                ScalarValue::Text("Z".repeat(3000)),
            ),
        ] {
            assert!(
                !storage
                    .btree()
                    .contains_exact(handle, &old_key, first)
                    .unwrap()
            );
            assert!(
                storage
                    .btree()
                    .contains_exact(handle, &new_key, relocated)
                    .unwrap()
            );
        }

        let same_key_old = duplicate;
        let same_key_relocated = storage
            .update(
                same_key_old,
                &[
                    ScalarValue::UInt64(2),
                    ScalarValue::UInt64(10),
                    ScalarValue::Text("Q".repeat(3000)),
                ],
            )
            .expect("relocate without changing team key");
        assert_ne!(same_key_relocated, same_key_old);
        assert!(
            !storage
                .btree()
                .contains_exact(team.handle, &ScalarValue::UInt64(10), same_key_old)
                .unwrap()
        );
        assert!(
            storage
                .btree()
                .contains_exact(team.handle, &ScalarValue::UInt64(10), same_key_relocated,)
                .unwrap()
        );

        storage.delete(relocated).expect("delete relocated row");
        assert!(
            storage
                .btree()
                .lookup(team.handle, &ScalarValue::UInt64(20))
                .unwrap()
                .is_empty()
        );
        storage.checkpoint().expect("checkpoint maintained indexes");
        storage.close().expect("close indexed heap");

        let mut reopened = HeapStorage::open_with_buffer_pool_size(&path, schema, 1)
            .expect("reopen maintained indexes");
        assert_eq!(
            reopened
                .btree()
                .lookup(team.handle, &ScalarValue::UInt64(10))
                .unwrap(),
            vec![same_key_relocated]
        );
        reopened.close().expect("close reopened heap");
        cleanup(&path);
    }

    #[test]
    fn registered_index_key_preflight_and_missing_entry_leave_transaction_active() {
        let path = test_path("registered-index-preflight");
        cleanup(&path);
        let mut storage = HeapStorage::create(&path, indexed_table()).expect("create heap");
        let definition = storage.create_index(ColumnId(3)).expect("name index");
        let old = storage
            .insert(&[
                ScalarValue::UInt64(1),
                ScalarValue::UInt64(10),
                ScalarValue::Text("small".into()),
            ])
            .expect("insert baseline");

        let too_large = "x".repeat(4020);
        let mut transaction = storage.begin_transaction().expect("begin preflight");
        assert!(matches!(
            storage.insert_in(
                &mut transaction,
                &[
                    ScalarValue::UInt64(2),
                    ScalarValue::UInt64(20),
                    ScalarValue::Text(too_large.clone()),
                ],
            ),
            Err(StorageError::Index(IndexError::KeyTooLarge { .. }))
        ));
        assert_eq!(transaction.state(), TransactionState::Active);
        assert!(matches!(
            storage.update_in(
                &mut transaction,
                old,
                &[
                    ScalarValue::UInt64(1),
                    ScalarValue::UInt64(10),
                    ScalarValue::Text(too_large),
                ],
            ),
            Err(StorageError::Index(IndexError::KeyTooLarge { .. }))
        ));
        assert_eq!(transaction.state(), TransactionState::Active);
        transaction
            .rollback()
            .expect("finish preflight transaction");
        assert_eq!(
            storage.read_row(old).unwrap()[2],
            ScalarValue::Text("small".into())
        );

        // Deliberately create a registry/heap inconsistency through the raw
        // tree API, then verify DML refuses to enlarge it before touching Heap.
        storage
            .btree()
            .delete(definition.handle, ScalarValue::Text("small".into()), old)
            .expect("remove registered entry through raw API");
        let mut transaction = storage.begin_transaction().expect("begin missing entry");
        assert!(matches!(
            storage.update_in(
                &mut transaction,
                old,
                &[
                    ScalarValue::UInt64(1),
                    ScalarValue::UInt64(10),
                    ScalarValue::Text("changed".into()),
                ],
            ),
            Err(StorageError::Index(IndexError::EntryNotFound))
        ));
        assert_eq!(transaction.state(), TransactionState::Active);
        assert_eq!(
            storage.read_row(old).unwrap()[2],
            ScalarValue::Text("small".into())
        );
        assert!(matches!(
            storage.delete_in(&mut transaction, old),
            Err(StorageError::Index(IndexError::EntryNotFound))
        ));
        assert_eq!(transaction.state(), TransactionState::Active);
        assert_eq!(
            storage.read_row(old).unwrap()[2],
            ScalarValue::Text("small".into())
        );
        transaction
            .rollback()
            .expect("finish missing-entry transaction");
        cleanup(&path);
    }

    #[test]
    fn registered_index_partial_failure_requires_and_supports_full_rollback() {
        let path = test_path("registered-index-partial-failure");
        cleanup(&path);
        let mut storage = HeapStorage::create(&path, indexed_table()).expect("create heap");
        let team = storage.create_index(ColumnId(2)).expect("team index");
        let name = storage.create_index(ColumnId(3)).expect("name index");
        let baseline = storage.scan().expect("empty baseline");

        for completed in [0, 1] {
            let mut transaction = storage.begin_transaction().expect("begin failed insert");
            storage.inject_registered_mutation_failure_after(completed);
            assert!(
                storage
                    .insert_in(
                        &mut transaction,
                        &[
                            ScalarValue::UInt64(1),
                            ScalarValue::UInt64(10),
                            ScalarValue::Text("A".into()),
                        ],
                    )
                    .is_err()
            );
            assert_eq!(transaction.state(), TransactionState::RollbackRequired);
            assert!(transaction.commit().is_err());
            transaction.rollback().expect("rollback partial insert");
            assert_eq!(storage.scan().unwrap(), baseline);
            assert!(
                storage
                    .btree()
                    .lookup(team.handle, &ScalarValue::UInt64(10))
                    .unwrap()
                    .is_empty()
            );
            assert!(
                storage
                    .btree()
                    .lookup(name.handle, &ScalarValue::Text("A".into()))
                    .unwrap()
                    .is_empty()
            );
        }

        let row = storage
            .insert(&[
                ScalarValue::UInt64(2),
                ScalarValue::UInt64(10),
                ScalarValue::Text("B".into()),
            ])
            .expect("insert update baseline");
        let mut transaction = storage.begin_transaction().expect("begin failed update");
        storage.inject_registered_mutation_failure_after(1);
        assert!(
            storage
                .update_in(
                    &mut transaction,
                    row,
                    &[
                        ScalarValue::UInt64(2),
                        ScalarValue::UInt64(20),
                        ScalarValue::Text("B".into()),
                    ],
                )
                .is_err()
        );
        assert_eq!(transaction.state(), TransactionState::RollbackRequired);
        transaction
            .rollback()
            .expect("rollback delete-insert window");
        assert_eq!(storage.read_row(row).unwrap()[1], ScalarValue::UInt64(10));
        assert!(
            storage
                .btree()
                .contains_exact(team.handle, &ScalarValue::UInt64(10), row)
                .unwrap()
        );
        assert!(
            !storage
                .btree()
                .contains_exact(team.handle, &ScalarValue::UInt64(20), row)
                .unwrap()
        );

        let before_delete = storage.read_row(row).expect("read delete baseline");
        let mut transaction = storage.begin_transaction().expect("begin failed delete");
        storage.inject_registered_mutation_failure_after(1);
        assert!(storage.delete_in(&mut transaction, row).is_err());
        assert_eq!(transaction.state(), TransactionState::RollbackRequired);
        transaction.rollback().expect("rollback partial delete");
        assert_eq!(storage.read_row(row).unwrap(), before_delete);
        assert!(
            storage
                .btree()
                .contains_exact(team.handle, &ScalarValue::UInt64(10), row)
                .unwrap()
        );
        assert!(
            storage
                .btree()
                .contains_exact(name.handle, &ScalarValue::Text("B".into()), row)
                .unwrap()
        );
        cleanup(&path);
    }

    #[test]
    fn index_catalog_root_metadata_is_bounded_and_must_name_a_catalog_page() {
        for case in ["zero", "out-of-range", "wrong-type", "reserved"] {
            let path = test_path(&format!("index-catalog-root-{case}"));
            cleanup(&path);
            HeapStorage::create(&path, indexed_table())
                .expect("create heap")
                .close()
                .expect("close heap");
            let mut pages = PageManager::open(&path).expect("open page manager");
            let mut header = pages.read_page(PageId(0)).expect("read metadata page");
            match case {
                "zero" => header.bytes_mut()[66..74].fill(0),
                "out-of-range" => {
                    header.bytes_mut()[66..74].copy_from_slice(&u64::MAX.to_le_bytes());
                }
                "wrong-type" => {
                    header.bytes_mut()[66..74].copy_from_slice(&FIRST_HEAP_PAGE.0.to_le_bytes());
                }
                "reserved" => header.bytes_mut()[74] = 1,
                _ => unreachable!(),
            }
            pages.write_page(&header).expect("write metadata mutation");
            pages.sync().expect("sync metadata mutation");
            drop(pages);
            let error = HeapStorage::open(&path, indexed_table()).expect_err("reject root");
            match case {
                "zero" => assert!(matches!(
                    error,
                    StorageError::Index(IndexError::InvalidChild(PageId(0)))
                )),
                "out-of-range" => assert!(matches!(
                    error,
                    StorageError::Index(IndexError::InvalidChild(PageId(u64::MAX)))
                )),
                "wrong-type" => assert!(matches!(
                    error,
                    StorageError::Index(IndexError::InvalidNodeType)
                )),
                "reserved" => assert!(matches!(
                    error,
                    StorageError::Metadata(crate::MetadataError::InvalidReservedBytes)
                )),
                _ => unreachable!(),
            }
            cleanup(&path);
        }
    }

    #[test]
    fn raw_btree_is_not_registered_and_duplicate_create_is_preflighted() {
        let path = test_path("raw-and-duplicate-index");
        cleanup(&path);
        let mut storage = HeapStorage::create(&path, indexed_table()).expect("create heap");
        let raw = storage
            .btree()
            .create(IndexSpec {
                data_type: SemanticType::physical(PhysicalType::UInt64),
                nullable: false,
            })
            .expect("create raw tree");
        assert!(storage.indexes().is_empty());
        let registered = storage.create_index(ColumnId(2)).expect("register index");
        let page_count = storage.buffer.page_count();
        let wal_count = storage.wal_records().expect("scan WAL").len();
        assert!(matches!(
            storage.create_index(ColumnId(2)),
            Err(StorageError::Index(IndexError::IndexAlreadyExists {
                column_id: ColumnId(2)
            }))
        ));
        assert_eq!(storage.buffer.page_count(), page_count);
        assert_eq!(
            storage.wal_records().expect("scan unchanged WAL").len(),
            wal_count
        );
        let row = storage
            .insert(&[
                ScalarValue::UInt64(1),
                ScalarValue::UInt64(10),
                ScalarValue::Text("raw-independent".into()),
            ])
            .expect("insert with registered index");
        assert!(
            storage
                .btree()
                .lookup(raw, &ScalarValue::UInt64(10))
                .expect("lookup untouched raw tree")
                .is_empty()
        );
        assert!(
            storage
                .btree()
                .contains_exact(registered.handle, &ScalarValue::UInt64(10), row)
                .expect("lookup maintained registered tree")
        );
        storage.close().expect("close heap");

        let mut reopened = HeapStorage::open(&path, indexed_table()).expect("reopen heap");
        assert_eq!(reopened.indexes(), std::slice::from_ref(&registered));
        assert!(
            reopened
                .btree()
                .lookup(raw, &ScalarValue::UInt64(1))
                .is_ok()
        );
        reopened.close().expect("close reopened heap");
        cleanup(&path);
    }

    #[test]
    fn unknown_column_and_backfill_failures_leave_no_registry_or_pages() {
        let path = test_path("index-build-rollback");
        cleanup(&path);
        let text_table = TableDef::new(
            TableId(24),
            "documents",
            vec![ColumnDef::new(
                ColumnId(1),
                "body",
                TypeSpec::Physical(PhysicalType::Text),
            )],
        );
        let mut storage = HeapStorage::create(&path, text_table.clone()).expect("create heap");
        storage
            .insert(&[ScalarValue::Text("x".repeat(4_030))])
            .expect("insert valid heap row");
        let baseline_pages = storage.buffer.page_count();
        let baseline_wal = storage.wal_records().expect("baseline WAL").len();
        assert!(matches!(
            storage.create_index(ColumnId(99)),
            Err(StorageError::Index(IndexError::UnknownIndexColumn {
                column_id: ColumnId(99)
            }))
        ));
        assert_eq!(storage.buffer.page_count(), baseline_pages);
        assert_eq!(
            storage.wal_records().expect("unchanged WAL").len(),
            baseline_wal
        );

        assert!(matches!(
            storage.create_index(ColumnId(1)),
            Err(StorageError::Index(IndexError::KeyTooLarge { .. }))
        ));
        assert!(storage.indexes().is_empty());
        assert_eq!(storage.buffer.page_count(), baseline_pages);
        assert_eq!(
            storage.scan().expect("heap survives build failure").len(),
            1
        );
        storage
            .insert(&[ScalarValue::Text("runtime remains healthy".into())])
            .expect("write after rollback");
        storage.close().expect("close heap");
        let reopened = HeapStorage::open(&path, text_table).expect("reopen heap");
        assert!(reopened.indexes().is_empty());
        cleanup(&path);
    }

    #[test]
    fn catalog_log_failure_rolls_back_complete_backfill() {
        let path = test_path("index-catalog-log-failure");
        cleanup(&path);
        let mut storage = HeapStorage::create(&path, indexed_table()).expect("create heap");
        for row in indexed_rows() {
            storage.insert(&row).expect("insert row");
        }
        let baseline_pages = storage.buffer.page_count();
        storage.inject_index_catalog_log_failure();
        assert!(matches!(
            storage.create_index(ColumnId(2)),
            Err(StorageError::Wal(_))
        ));
        assert!(storage.indexes().is_empty());
        assert_eq!(storage.buffer.page_count(), baseline_pages);
        assert_eq!(
            storage.scan().expect("rows survive catalog failure").len(),
            4
        );
        storage.close().expect("close heap");
        let reopened = HeapStorage::open(&path, indexed_table()).expect("reopen heap");
        assert!(reopened.indexes().is_empty());
        cleanup(&path);
    }

    #[test]
    fn index_catalog_overflow_chain_reopens_in_creation_order() {
        let path = test_path("index-catalog-overflow");
        cleanup(&path);
        let mut storage = HeapStorage::create(&path, indexed_table()).expect("create heap");
        storage.index_catalog_payload_capacity = Some(36);
        let first = storage.create_index(ColumnId(1)).expect("first index");
        let second = storage.create_index(ColumnId(2)).expect("overflow index");
        assert_eq!(storage.indexes(), &[first.clone(), second.clone()]);
        storage.close().expect("close overflow catalog");

        let reopened = HeapStorage::open(&path, indexed_table()).expect("reopen catalog chain");
        assert_eq!(reopened.indexes(), &[first, second]);
        cleanup(&path);
    }

    #[test]
    fn registered_backfill_splits_to_multiple_tree_levels_with_capacity_one() {
        let path = test_path("index-backfill-multilevel");
        cleanup(&path);
        let schema = TableDef::new(
            TableId(25),
            "wide_keys",
            vec![ColumnDef::new(
                ColumnId(1),
                "key",
                TypeSpec::Physical(PhysicalType::Text),
            )],
        );
        let mut storage = HeapStorage::create_with_buffer_pool_size(&path, schema.clone(), 1)
            .expect("create heap");
        let mut expected = Vec::new();
        for ordinal in 0..40_u64 {
            let key = format!("{ordinal:04}-{}", "x".repeat(895));
            expected.push((
                key.clone(),
                storage
                    .insert(&[ScalarValue::Text(key)])
                    .expect("insert wide key"),
            ));
        }
        let definition = storage.create_index(ColumnId(1)).expect("build deep index");
        assert!(
            storage
                .btree()
                .read_meta(definition.handle)
                .expect("read index metadata")
                .height
                >= 3
        );
        for (key, row_id) in expected.iter().step_by(7) {
            assert_eq!(
                storage
                    .btree()
                    .lookup(definition.handle, &ScalarValue::Text(key.clone()))
                    .expect("lookup deep key"),
                vec![*row_id]
            );
        }
        storage.checkpoint().expect("checkpoint registered index");
        storage.close().expect("close deep index");
        let reopened =
            HeapStorage::open_with_buffer_pool_size(&path, schema, 1).expect("reopen deep index");
        assert_eq!(reopened.indexes(), std::slice::from_ref(&definition));
        reopened.close().expect("close reopened deep index");
        cleanup(&path);
    }

    #[test]
    fn registry_semantic_corruption_is_rejected_on_open() {
        let cases = [
            "unknown-column",
            "duplicate-column",
            "wrong-handle",
            "cycle",
            "out-of-range-next",
            "wrong-next-type",
        ];
        for case in cases {
            let path = test_path(&format!("index-catalog-corrupt-{case}"));
            cleanup(&path);
            let mut storage = HeapStorage::create(&path, indexed_table()).expect("create heap");
            storage
                .create_index(ColumnId(2))
                .expect("create registered index");
            storage.close().expect("close heap");
            if case == "duplicate-column" {
                duplicate_first_catalog_entry(&path);
            } else {
                rewrite_catalog(&path, |node| match case {
                    "unknown-column" => node.definitions[0].column_id = ColumnId(99),
                    "wrong-handle" => {
                        node.definitions[0].handle = BTreeHandle {
                            meta_page: FIRST_HEAP_PAGE,
                        };
                    }
                    "cycle" => node.next_catalog = Some(PageId(1)),
                    "out-of-range-next" => node.next_catalog = Some(PageId(u64::MAX)),
                    "wrong-next-type" => node.next_catalog = Some(FIRST_HEAP_PAGE),
                    _ => unreachable!(),
                });
            }
            let error = HeapStorage::open(&path, indexed_table()).expect_err("reject catalog");
            match case {
                "unknown-column" => assert!(matches!(
                    error,
                    StorageError::Index(IndexError::UnknownIndexColumn { .. })
                )),
                "duplicate-column" => assert!(matches!(
                    error,
                    StorageError::Index(IndexError::DuplicateRegisteredColumn { .. })
                )),
                "wrong-handle" => assert!(matches!(
                    error,
                    StorageError::Index(IndexError::InvalidNodeType)
                )),
                "cycle" => assert!(matches!(
                    error,
                    StorageError::Index(IndexError::CatalogCycle { .. })
                )),
                "out-of-range-next" => assert!(matches!(
                    error,
                    StorageError::Index(IndexError::InvalidChild(PageId(u64::MAX)))
                )),
                "wrong-next-type" => assert!(matches!(
                    error,
                    StorageError::Index(IndexError::InvalidNodeType)
                )),
                _ => unreachable!(),
            }
            cleanup(&path);
        }
    }

    #[test]
    fn registry_rejects_nominal_meta_spec_mismatch() {
        let path = test_path("index-catalog-spec-mismatch");
        cleanup(&path);
        let mut storage = HeapStorage::create(&path, indexed_table()).expect("create heap");
        storage
            .create_index(ColumnId(2))
            .expect("create registered index");
        let raw = storage
            .btree()
            .create(IndexSpec {
                data_type: SemanticType::named("UserId", PhysicalType::UInt64),
                nullable: true,
            })
            .expect("create mismatched raw tree");
        storage.close().expect("close heap");
        rewrite_catalog(&path, |node| node.definitions[0].handle = raw);
        assert!(matches!(
            HeapStorage::open(&path, indexed_table()),
            Err(StorageError::Index(IndexError::CatalogSpecMismatch {
                column_id: ColumnId(2)
            }))
        ));
        cleanup(&path);
    }

    fn text_row(id: i64, length: usize, byte: u8) -> Vec<ScalarValue> {
        vec![
            ScalarValue::Int64(id),
            ScalarValue::Text(String::from_utf8(vec![byte; length]).expect("ASCII test row")),
        ]
    }

    #[test]
    fn insert_write_read_decode_round_trip() {
        let path = test_path("heap-round-trip");
        let mut storage = HeapStorage::create(&path, table()).expect("create heap");
        let row_id = storage
            .insert(&[ScalarValue::Int64(7), ScalarValue::Text("Ada".into())])
            .expect("insert");
        assert_eq!(row_id.slot, 0);
        storage.close().expect("close heap");

        let mut reopened = HeapStorage::open(&path, table()).expect("reopen heap");
        let rows = reopened.scan().expect("scan");
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].1,
            vec![ScalarValue::Int64(7), ScalarValue::Text("Ada".into())]
        );
        cleanup(&path);
    }

    #[test]
    fn frontend_independent_schema_names_survive_reopen() {
        let path = test_path("heap-frontend-independent-names");
        cleanup(&path);
        let table = TableDef::new(
            TableId(19),
            "用户",
            vec![ColumnDef::new(
                ColumnId(1),
                "用户-id",
                TypeSpec::Physical(PhysicalType::UInt64),
            )],
        );
        let mut storage = HeapStorage::create(&path, table.clone()).expect("create heap");
        storage
            .insert(&[ScalarValue::UInt64(7)])
            .expect("insert row");
        storage.close().expect("close heap");

        let mut reopened = HeapStorage::open(&path, table).expect("reopen heap");
        assert_eq!(
            reopened.scan().expect("scan reopened heap")[0].1,
            vec![ScalarValue::UInt64(7)]
        );
        reopened.close().expect("close reopened heap");
        cleanup(&path);
    }

    #[test]
    fn reopen_requires_the_complete_canonical_schema_identity() {
        let path = test_path("heap-schema-identity");
        cleanup(&path);
        let baseline = identity_table();
        let mut storage = HeapStorage::create(&path, baseline.clone()).expect("create heap");
        storage
            .insert(&[ScalarValue::UInt64(1), ScalarValue::UInt64(9)])
            .expect("insert row");
        storage.close().expect("close heap");

        let mut identical = HeapStorage::open(&path, baseline.clone()).expect("identical schema");
        assert_eq!(
            identical.scan().expect("scan matching heap")[0].1,
            vec![ScalarValue::UInt64(1), ScalarValue::UInt64(9)]
        );
        identical.close().expect("close matching heap");

        let mut variants = Vec::new();
        let mut column_order = baseline.clone();
        column_order.columns.swap(0, 1);
        variants.push(column_order);
        let mut column_id = baseline.clone();
        column_id.columns[0].id = ColumnId(3);
        variants.push(column_id);
        let mut column_name = baseline.clone();
        column_name.columns[0].name = "user_id".into();
        variants.push(column_name);
        let mut physical_type = baseline.clone();
        physical_type.columns[0].type_spec = TypeSpec::Semantic {
            name: "UserId".into(),
            physical: PhysicalType::Int64,
        };
        variants.push(physical_type);
        let mut semantic_type = baseline.clone();
        semantic_type.columns[0].type_spec = TypeSpec::Semantic {
            name: "TeamId".into(),
            physical: PhysicalType::UInt64,
        };
        semantic_type.columns[1].type_spec = TypeSpec::Semantic {
            name: "UserId".into(),
            physical: PhysicalType::UInt64,
        };
        variants.push(semantic_type);
        let mut nullable = baseline.clone();
        nullable.columns[0].nullable = true;
        variants.push(nullable);
        let mut primary_key = baseline.clone();
        primary_key.columns[0].primary_key = false;
        variants.push(primary_key);
        let mut table_name = baseline.clone();
        table_name.name = "members".into();
        variants.push(table_name);

        for variant in variants {
            assert!(matches!(
                HeapStorage::open(&path, variant),
                Err(StorageError::SchemaMismatch { .. })
            ));
        }
        let mut table_id = baseline;
        table_id.id = TableId(18);
        assert!(matches!(
            HeapStorage::open(&path, table_id),
            Err(StorageError::TableIdMismatch {
                expected: TableId(18),
                actual: TableId(17)
            })
        ));
        cleanup(&path);
    }

    #[test]
    fn invalid_schema_is_rejected_before_heap_or_wal_creation() {
        let path = test_path("heap-invalid-schema");
        cleanup(&path);
        let mut invalid = identity_table();
        invalid.columns[1].id = ColumnId(1);

        assert!(matches!(
            HeapStorage::create(&path, invalid),
            Err(StorageError::Schema(SchemaError::DuplicateColumnId {
                column_id: ColumnId(1),
                ..
            }))
        ));
        assert!(!path.exists());
        assert!(!wal_path(&path).exists());
        assert!(!wal_alternate_path(wal_path(&path)).exists());
    }

    #[test]
    fn heap_metadata_persists_versioned_schema_fingerprint_and_checks_its_count() {
        let path = test_path("heap-schema-metadata");
        cleanup(&path);
        let table = identity_table();
        HeapStorage::create(&path, table.clone())
            .expect("create heap")
            .close()
            .expect("close heap");

        let mut pages = PageManager::open(&path).expect("open page manager");
        let mut header = pages.read_page(PageId(0)).expect("read metadata page");
        let bytes = header.bytes();
        assert_eq!(&bytes[16..20], b"NBD1");
        assert_eq!(&bytes[20..22], &3_u16.to_le_bytes());
        assert_eq!(&bytes[22..24], &[0, 0]);
        assert_eq!(&bytes[24..32], &table.id.0.to_le_bytes());
        assert_eq!(&bytes[32..34], &2_u16.to_le_bytes());
        assert_eq!(
            &bytes[34..66],
            table.fingerprint().expect("table fingerprint").as_bytes()
        );
        assert_eq!(&bytes[66..74], &1_u64.to_le_bytes());
        assert_eq!(&bytes[74..80], &[0; 6]);

        header.bytes_mut()[32..34].copy_from_slice(&1_u16.to_le_bytes());
        pages.write_page(&header).expect("write corrupt count");
        pages.sync().expect("sync corrupt count");
        drop(pages);
        assert!(matches!(
            HeapStorage::open(&path, table),
            Err(StorageError::Metadata(
                crate::MetadataError::InvalidColumnCount {
                    stored: 1,
                    expected: 2
                }
            ))
        ));
        cleanup(&path);
    }

    #[test]
    fn zero_column_row_round_trips() {
        let path = test_path("heap-empty-row");
        let empty_table = TableDef::new(TableId(2), "events", vec![]);
        let mut storage = HeapStorage::create(&path, empty_table.clone()).expect("create heap");
        let row_id = storage.insert(&[]).expect("insert empty row");
        assert_eq!(row_id.slot, 0);
        storage.close().expect("close heap");

        let mut reopened = HeapStorage::open(&path, empty_table).expect("reopen heap");
        let rows = reopened.scan().expect("scan heap");
        assert_eq!(rows.len(), 1);
        assert!(rows[0].1.is_empty());
        cleanup(&path);
    }

    #[test]
    fn nullable_row_round_trips_after_close_and_reopen() {
        let path = test_path("heap-null-round-trip");
        let nullable_table = TableDef::new(
            TableId(3),
            "profiles",
            vec![
                ColumnDef::new(ColumnId(1), "id", TypeSpec::Physical(PhysicalType::Int64)),
                ColumnDef::new(
                    ColumnId(2),
                    "nickname",
                    TypeSpec::Physical(PhysicalType::Text),
                )
                .nullable(true),
            ],
        );
        let mut storage = HeapStorage::create(&path, nullable_table.clone()).expect("create heap");
        storage
            .insert(&[ScalarValue::Int64(1), ScalarValue::Null])
            .expect("insert NULL");
        storage.close().expect("close heap");

        let mut reopened = HeapStorage::open(&path, nullable_table).expect("reopen heap");
        assert_eq!(
            reopened.scan().expect("scan")[0].1,
            vec![ScalarValue::Int64(1), ScalarValue::Null]
        );
        cleanup(&path);
    }

    #[test]
    fn non_nullable_column_rejects_null_at_the_heap_boundary() {
        let path = test_path("heap-null-rejected");
        let mut storage = HeapStorage::create(&path, table()).expect("create heap");
        assert!(matches!(
            storage.insert(&[ScalarValue::Null, ScalarValue::Text("Ada".into())]),
            Err(StorageError::NullNotAllowed { column }) if column == "id"
        ));
        assert!(storage.scan().expect("scan").is_empty());
        storage.close().expect("close heap");
        cleanup(&path);
    }

    #[test]
    fn null_scalar_boundaries_and_truncated_following_values_are_checked() {
        let nullable_table = TableDef::new(
            TableId(4),
            "profiles",
            vec![
                ColumnDef::new(
                    ColumnId(1),
                    "nickname",
                    TypeSpec::Physical(PhysicalType::Text),
                )
                .nullable(true),
                ColumnDef::new(
                    ColumnId(2),
                    "active",
                    TypeSpec::Physical(PhysicalType::Bool),
                ),
            ],
        );
        let encoded = encode_row(&[ScalarValue::Null, ScalarValue::Bool(true)]).expect("encode");
        assert_eq!(
            decode_row(&encoded, &nullable_table).expect("decode"),
            vec![ScalarValue::Null, ScalarValue::Bool(true)]
        );
        assert!(matches!(
            decode_row(&encoded[..1], &nullable_table),
            Err(StorageError::Codec(crate::CodecError::MissingScalarTag))
        ));
    }

    #[test]
    fn row_id_update_delete_and_tombstones_survive_reopen() {
        let path = test_path("heap-row-mutation");
        let mut storage = HeapStorage::create(&path, table()).expect("create heap");
        let first = storage
            .insert(&[
                ScalarValue::Int64(1),
                ScalarValue::Text("a long original value".into()),
            ])
            .expect("insert first");
        let middle = storage
            .insert(&[ScalarValue::Int64(2), ScalarValue::Text("middle".into())])
            .expect("insert middle");
        let third = storage
            .insert(&[ScalarValue::Int64(3), ScalarValue::Text("third".into())])
            .expect("insert third");

        storage
            .update(
                first,
                &[ScalarValue::Int64(1), ScalarValue::Text("x".into())],
            )
            .expect("shrink first");
        storage
            .update(
                first,
                &[
                    ScalarValue::Int64(1),
                    ScalarValue::Text("a replacement that grows again".into()),
                ],
            )
            .expect("grow first");
        storage.delete(middle).expect("delete middle");
        assert!(matches!(
            storage.read_row(middle),
            Err(StorageError::RowDeleted { row_id }) if row_id == middle
        ));
        assert_eq!(
            storage.read_row(third).expect("third remains")[0],
            ScalarValue::Int64(3)
        );
        storage.close().expect("close heap");

        let mut reopened = HeapStorage::open(&path, table()).expect("reopen heap");
        let rows = reopened.scan().expect("scan");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, first);
        assert_eq!(rows[1].0, third);
        assert!(matches!(
            reopened.delete(middle),
            Err(StorageError::RowDeleted { .. })
        ));
        cleanup(&path);
    }

    #[test]
    fn reused_slot_distinguishes_deleted_and_stale_row_ids() {
        let path = test_path("heap-generation-safe-row-id");
        let mut storage = HeapStorage::create(&path, table()).expect("create heap");
        let old = storage
            .insert(&[ScalarValue::Int64(1), ScalarValue::Text("old".into())])
            .expect("insert old occupant");
        assert_eq!(old.generation, 1);
        storage.delete(old).expect("delete old occupant");
        assert!(matches!(
            storage.read_row(old),
            Err(StorageError::RowDeleted { row_id }) if row_id == old
        ));

        let new = storage
            .insert(&[ScalarValue::Int64(2), ScalarValue::Text("new".into())])
            .expect("reuse old slot");
        assert_eq!(new.page, old.page);
        assert_eq!(new.slot, old.slot);
        assert_eq!(new.generation, old.generation + 1);
        assert!(matches!(
            storage.read_row(old),
            Err(StorageError::StaleRowId {
                row_id,
                actual_generation
            }) if row_id == old && actual_generation == new.generation
        ));
        assert!(matches!(
            storage.update(
                old,
                &[ScalarValue::Int64(3), ScalarValue::Text("stale".into())]
            ),
            Err(StorageError::StaleRowId { row_id, .. }) if row_id == old
        ));
        assert!(matches!(
            storage.delete(old),
            Err(StorageError::StaleRowId { row_id, .. }) if row_id == old
        ));
        assert_eq!(
            storage.read_row(new).expect("read new occupant"),
            vec![ScalarValue::Int64(2), ScalarValue::Text("new".into())]
        );

        storage.close().expect("close heap");
        let mut reopened = HeapStorage::open(&path, table()).expect("reopen heap");
        assert!(matches!(
            reopened.read_row(old),
            Err(StorageError::StaleRowId { .. })
        ));
        assert_eq!(reopened.scan().expect("scan reopened")[0].0, new);
        cleanup(&path);
    }

    #[test]
    fn heap_insert_reuses_earlier_page_tombstone_with_capacity_one() {
        let path = test_path("heap-first-fit-tombstone");
        let mut storage =
            HeapStorage::create_with_buffer_pool_size(&path, table(), 1).expect("create heap");
        let earlier = storage
            .insert(&text_row(1, 3_900, b'a'))
            .expect("fill page 1");
        let later = storage
            .insert(&text_row(2, 300, b'b'))
            .expect("create page 2");
        assert_eq!(earlier.page, FIRST_HEAP_PAGE);
        assert_eq!(later.page, PageId(3));
        let page_count = storage.buffer.page_count();

        storage.delete(earlier).expect("delete earlier row");
        let reused = storage
            .insert(&text_row(3, 100, b'c'))
            .expect("reuse page 1");
        assert_eq!(reused.page, FIRST_HEAP_PAGE);
        assert_eq!(reused.slot, earlier.slot);
        assert_eq!(reused.generation, earlier.generation + 1);
        assert_eq!(storage.buffer.page_count(), page_count);
        assert_eq!(
            storage.read_row(later).expect("later row remains"),
            text_row(2, 300, b'b')
        );
        storage.close().expect("close heap");
        cleanup(&path);
    }

    #[test]
    fn heap_insert_uses_lowest_page_free_payload_without_growing_file() {
        let path = test_path("heap-first-fit-free-payload");
        let mut storage = HeapStorage::create(&path, table()).expect("create heap");
        let first = storage
            .insert(&text_row(1, 3_500, b'a'))
            .expect("fill page 1");
        let second = storage
            .insert(&text_row(2, 1_000, b'b'))
            .expect("create page 2");
        let third = storage
            .insert(&text_row(3, 3_000, b'c'))
            .expect("fill page 2");
        assert_eq!(
            (first.page, second.page, third.page),
            (FIRST_HEAP_PAGE, PageId(3), PageId(3))
        );
        storage
            .update(first, &text_row(1, 10, b'd'))
            .expect("shrink page 1");
        storage
            .update(second, &text_row(2, 10, b'e'))
            .expect("shrink page 2");
        let page_count = storage.buffer.page_count();

        for id in 10..15 {
            let inserted = storage
                .insert(&text_row(id, 500, b'f'))
                .expect("reuse free payload");
            assert_eq!(inserted.page, FIRST_HEAP_PAGE);
        }
        assert_eq!(storage.buffer.page_count(), page_count);
        storage.close().expect("close heap");
        cleanup(&path);
    }

    #[test]
    fn first_fit_does_not_skip_a_corrupt_earlier_page() {
        let path = test_path("heap-first-fit-corruption");
        let mut storage = HeapStorage::create(&path, table()).expect("create heap");
        storage
            .insert(&text_row(1, 3_900, b'a'))
            .expect("fill page 1");
        storage
            .insert(&text_row(2, 100, b'b'))
            .expect("create page 2");
        let page_count = storage.buffer.page_count();
        {
            let mut page = storage
                .buffer
                .write_page(FIRST_HEAP_PAGE)
                .expect("write first heap page");
            page.page_mut().bytes_mut()[4..6].copy_from_slice(&99_u16.to_le_bytes());
        }

        assert!(matches!(
            storage.insert(&text_row(3, 10, b'c')),
            Err(StorageError::Page(PageError::UnsupportedVersion(99)))
        ));
        assert_eq!(storage.buffer.page_count(), page_count);
        storage.simulate_crash();
        cleanup(&path);
    }

    #[test]
    fn update_prefers_in_place_then_relocates_and_tracks_old_locator_lifecycle() {
        let path = test_path("heap-update-relocation");
        let mut storage =
            HeapStorage::create_with_buffer_pool_size(&path, table(), 1).expect("create heap");
        let old = storage
            .insert(&text_row(1, 100, b'a'))
            .expect("insert source");
        storage
            .insert(&text_row(2, 3_800, b'b'))
            .expect("fill source page");
        let destination_seed = storage
            .insert(&text_row(3, 300, b'c'))
            .expect("create destination");
        assert_eq!(destination_seed.page, PageId(3));

        let unchanged = storage
            .update(old, &text_row(1, 50, b'd'))
            .expect("in-place update");
        assert_eq!(unchanged, old);
        let relocated = storage
            .update(old, &text_row(1, 1_000, b'e'))
            .expect("relocate update");
        assert_eq!(relocated.page, PageId(3));
        assert_ne!(relocated, old);
        assert_eq!(
            storage.read_row(relocated).expect("read relocation"),
            text_row(1, 1_000, b'e')
        );
        assert!(
            matches!(storage.read_row(old), Err(StorageError::RowDeleted { row_id }) if row_id == old)
        );
        assert!(matches!(
            storage.update(old, &text_row(1, 10, b'x')),
            Err(StorageError::RowDeleted { .. })
        ));
        assert!(matches!(
            storage.delete(old),
            Err(StorageError::RowDeleted { .. })
        ));

        let source_reuse = storage
            .insert(&text_row(4, 20, b'f'))
            .expect("reuse source slot");
        assert_eq!((source_reuse.page, source_reuse.slot), (old.page, old.slot));
        assert_eq!(source_reuse.generation, old.generation + 1);
        assert!(matches!(
            storage.read_row(old),
            Err(StorageError::StaleRowId { .. })
        ));
        storage.close().expect("close heap");
        cleanup(&path);
    }

    #[test]
    fn relocation_reuses_destination_tombstone_generation() {
        let path = test_path("heap-relocation-tombstone");
        let mut storage = HeapStorage::create(&path, table()).expect("create heap");
        let old = storage
            .insert(&text_row(1, 100, b'a'))
            .expect("insert source");
        storage
            .insert(&text_row(2, 3_800, b'b'))
            .expect("fill source page");
        let destination = storage
            .insert(&text_row(3, 300, b'c'))
            .expect("destination row");
        storage
            .delete(destination)
            .expect("create destination tombstone");

        let relocated = storage
            .update(old, &text_row(1, 1_000, b'd'))
            .expect("relocate");
        assert_eq!(
            (relocated.page, relocated.slot),
            (destination.page, destination.slot)
        );
        assert_eq!(relocated.generation, destination.generation + 1);
        storage.close().expect("close heap");
        cleanup(&path);
    }

    #[test]
    fn runtime_rollback_of_existing_page_relocation_restores_both_pages() {
        let path = test_path("heap-relocation-existing-rollback");
        let mut storage =
            HeapStorage::create_with_buffer_pool_size(&path, table(), 1).expect("create heap");
        let original = text_row(1, 100, b'a');
        let old = storage.insert(&original).expect("insert source");
        storage
            .insert(&text_row(2, 3_800, b'b'))
            .expect("fill source page");
        let destination = storage
            .insert(&text_row(3, 300, b'c'))
            .expect("destination row");
        let mut transaction = storage.begin_transaction().expect("begin relocation");

        let relocated = storage
            .update_in(&mut transaction, old, &text_row(1, 1_000, b'd'))
            .expect("relocate to existing page");
        assert_eq!(relocated.page, destination.page);
        let relocation_updates = storage
            .wal_records()
            .expect("scan relocation WAL")
            .into_iter()
            .filter_map(|record| match record.kind {
                WalRecordKind::PageUpdate { page_id, .. } if record.txn_id == transaction.id() => {
                    Some(page_id)
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(relocation_updates, vec![relocated.page, old.page]);
        transaction.rollback().expect("rollback relocation");
        assert_eq!(storage.read_row(old).expect("source restored"), original);
        assert_eq!(
            storage.read_row(destination).expect("destination restored"),
            text_row(3, 300, b'c')
        );
        assert!(matches!(
            storage.read_row(relocated),
            Err(StorageError::RowNotFound { .. })
                | Err(StorageError::RowDeleted { .. })
                | Err(StorageError::StaleRowId { .. })
        ));
        storage.close().expect("close heap");
        cleanup(&path);
    }

    #[test]
    fn committed_new_page_relocation_survives_checkpoint_and_reopen() {
        let path = test_path("heap-relocation-new-page-checkpoint");
        let mut storage = HeapStorage::create(&path, table()).expect("create heap");
        let old = storage
            .insert(&text_row(1, 100, b'a'))
            .expect("insert source");
        storage
            .insert(&text_row(2, 3_800, b'b'))
            .expect("fill source page");
        let relocated = storage
            .update(old, &text_row(1, 1_000, b'c'))
            .expect("relocate");
        assert_eq!(relocated.page, PageId(3));
        storage.checkpoint().expect("checkpoint relocation");
        storage.close().expect("close heap");

        let reopened = HeapStorage::open(&path, table()).expect("reopen heap");
        assert_eq!(
            reopened.read_row(relocated).expect("read relocated row"),
            text_row(1, 1_000, b'c')
        );
        assert!(matches!(
            reopened.read_row(old),
            Err(StorageError::RowDeleted { row_id }) if row_id == old
        ));
        reopened.close().expect("close reopened heap");
        cleanup(&path);
    }

    #[test]
    fn relocation_to_new_page_rolls_back_page_count_with_capacity_one() {
        let path = test_path("heap-relocation-new-page-rollback");
        let mut storage =
            HeapStorage::create_with_buffer_pool_size(&path, table(), 1).expect("create heap");
        let original = text_row(1, 100, b'a');
        let old = storage.insert(&original).expect("insert source");
        storage
            .insert(&text_row(2, 3_800, b'b'))
            .expect("fill source page");
        let page_count = storage.buffer.page_count();
        let mut transaction = storage.begin_transaction().expect("begin relocation");
        let relocated = storage
            .update_in(&mut transaction, old, &text_row(1, 1_000, b'c'))
            .expect("relocate to new page");
        assert_eq!(relocated.page, PageId(page_count));
        transaction.rollback().expect("rollback relocation");
        assert_eq!(storage.buffer.page_count(), page_count);
        assert_eq!(storage.read_row(old).expect("old row restored"), original);
        assert!(matches!(
            storage.read_row(relocated),
            Err(StorageError::RowNotFound { .. })
        ));
        storage.close().expect("close heap");
        cleanup(&path);
    }

    #[test]
    fn startup_undo_of_new_page_relocation_restores_page_count() {
        let path = test_path("heap-relocation-new-page-startup-undo");
        let mut storage = HeapStorage::create(&path, table()).expect("create heap");
        let original = text_row(1, 100, b'a');
        let old = storage.insert(&original).expect("insert source");
        storage
            .insert(&text_row(2, 3_800, b'b'))
            .expect("fill source page");
        let page_count = storage.buffer.page_count();
        let mut transaction = storage.begin_transaction().expect("begin relocation");
        let relocated = storage
            .update_in(&mut transaction, old, &text_row(1, 1_000, b'c'))
            .expect("relocate to new page");
        assert_eq!(relocated.page, PageId(page_count));
        storage.flush().expect("steal flush relocation");
        drop(transaction);
        storage.simulate_crash();

        let reopened = HeapStorage::open(&path, table()).expect("startup undo relocation");
        assert_eq!(reopened.buffer.page_count(), page_count);
        assert_eq!(reopened.read_row(old).expect("source restored"), original);
        reopened.close().expect("close recovered heap");
        cleanup(&path);
    }

    #[test]
    fn startup_undo_of_tombstone_destination_restores_generation() {
        let path = test_path("heap-relocation-tombstone-startup-undo");
        let mut storage = HeapStorage::create(&path, table()).expect("create heap");
        let old = storage
            .insert(&text_row(1, 100, b'a'))
            .expect("insert source");
        storage
            .insert(&text_row(2, 3_800, b'b'))
            .expect("fill source page");
        let tombstone = storage
            .insert(&text_row(3, 300, b'c'))
            .expect("destination row");
        storage
            .delete(tombstone)
            .expect("commit destination tombstone");
        let mut transaction = storage.begin_transaction().expect("begin relocation");
        let relocated = storage
            .update_in(&mut transaction, old, &text_row(1, 1_000, b'd'))
            .expect("reuse destination tombstone");
        assert_eq!(relocated.generation, tombstone.generation + 1);
        storage.flush().expect("steal flush relocation");
        drop(transaction);
        storage.simulate_crash();

        let reopened = HeapStorage::open(&path, table()).expect("startup undo relocation");
        assert!(matches!(
            reopened.read_row(tombstone),
            Err(StorageError::RowDeleted { row_id }) if row_id == tombstone
        ));
        assert!(matches!(
            reopened.read_row(relocated),
            Err(StorageError::StaleRowId {
                actual_generation,
                ..
            }) if actual_generation == tombstone.generation
        ));
        reopened.close().expect("close recovered heap");
        cleanup(&path);
    }

    #[test]
    fn partial_relocation_log_failure_requires_rollback_and_blocks_commit() {
        let path = test_path("heap-relocation-rollback-required");
        let mut storage = HeapStorage::create(&path, table()).expect("create heap");
        let original = text_row(1, 100, b'a');
        let old = storage.insert(&original).expect("insert source");
        storage
            .insert(&text_row(2, 3_800, b'b'))
            .expect("fill source page");
        let destination_seed = storage
            .insert(&text_row(3, 300, b'c'))
            .expect("destination seed");
        let mut transaction = storage.begin_transaction().expect("begin relocation");
        storage.inject_relocation_second_log_failure();

        assert!(matches!(
            storage.update_in(&mut transaction, old, &text_row(1, 1_000, b'd')),
            Err(StorageError::Wal(_))
        ));
        assert_eq!(transaction.state(), TransactionState::RollbackRequired);
        assert!(matches!(
            transaction.commit(),
            Err(StorageError::Transaction(TransactionError::NotActive {
                state: TransactionState::RollbackRequired,
                ..
            }))
        ));
        assert!(matches!(
            storage.delete_in(&mut transaction, destination_seed),
            Err(StorageError::Transaction(TransactionError::NotActive {
                state: TransactionState::RollbackRequired,
                ..
            }))
        ));
        transaction.rollback().expect("rollback partial relocation");
        assert_eq!(storage.read_row(old).expect("source intact"), original);
        assert_eq!(
            storage
                .read_row(destination_seed)
                .expect("destination intact"),
            text_row(3, 300, b'c')
        );
        storage.close().expect("close heap");
        cleanup(&path);
    }

    #[test]
    fn new_page_allocation_failure_requires_rollback_and_restores_partial_extension() {
        let path = test_path("heap-relocation-allocation-failure");
        let mut storage =
            HeapStorage::create_with_buffer_pool_size(&path, table(), 1).expect("create heap");
        let original = text_row(1, 100, b'a');
        let old = storage.insert(&original).expect("insert source");
        storage
            .insert(&text_row(2, 3_800, b'b'))
            .expect("fill source page");
        let page_count = storage.buffer.page_count();
        let mut transaction = storage.begin_transaction().expect("begin relocation");
        storage.buffer.inject_partial_page_allocation_failure(137);

        assert!(matches!(
            storage.update_in(&mut transaction, old, &text_row(1, 1_000, b'c')),
            Err(StorageError::Io(_))
        ));
        assert_eq!(transaction.state(), TransactionState::RollbackRequired);
        assert!(transaction.commit().is_err());
        transaction.rollback().expect("rollback failed allocation");
        assert_eq!(storage.buffer.page_count(), page_count);
        assert_eq!(storage.read_row(old).expect("source intact"), original);
        storage.close().expect("close heap");
        cleanup(&path);
    }

    #[test]
    fn source_publish_failure_requires_rollback_and_restores_both_pages() {
        let path = test_path("heap-relocation-source-publish-failure");
        let mut storage =
            HeapStorage::create_with_buffer_pool_size(&path, table(), 1).expect("create heap");
        let original = text_row(1, 100, b'a');
        let old = storage.insert(&original).expect("insert source");
        storage
            .insert(&text_row(2, 3_800, b'b'))
            .expect("fill source page");
        let destination = storage
            .insert(&text_row(3, 300, b'c'))
            .expect("destination row");
        let mut transaction = storage.begin_transaction().expect("begin relocation");
        storage.inject_relocation_source_publish_failure();

        assert!(matches!(
            storage.update_in(&mut transaction, old, &text_row(1, 1_000, b'd')),
            Err(StorageError::Io(_))
        ));
        assert_eq!(transaction.state(), TransactionState::RollbackRequired);
        assert!(transaction.commit().is_err());
        transaction.rollback().expect("rollback publish failure");
        assert_eq!(storage.read_row(old).expect("source intact"), original);
        assert_eq!(
            storage.read_row(destination).expect("destination intact"),
            text_row(3, 300, b'c')
        );
        storage.close().expect("close heap");
        cleanup(&path);
    }

    #[test]
    fn stale_update_does_not_poison_explicit_transaction() {
        let path = test_path("heap-stale-update-transaction");
        let mut storage = HeapStorage::create(&path, table()).expect("create heap");
        let stale = storage
            .insert(&text_row(1, 20, b'a'))
            .expect("insert stale source");
        let valid = storage
            .insert(&text_row(2, 20, b'b'))
            .expect("insert valid source");
        storage.delete(stale).expect("delete stale source");
        storage
            .insert(&text_row(3, 20, b'c'))
            .expect("reuse stale source");
        let mut transaction = storage.begin_transaction().expect("begin transaction");

        assert!(matches!(
            storage.update_in(&mut transaction, stale, &text_row(1, 20, b'x')),
            Err(StorageError::StaleRowId { .. })
        ));
        assert_eq!(transaction.state(), TransactionState::Active);
        let current = storage
            .update_in(&mut transaction, valid, &text_row(2, 20, b'd'))
            .expect("valid update");
        assert_eq!(current, valid);
        transaction.commit().expect("commit after ordinary error");
        assert_eq!(
            storage.read_row(valid).expect("read valid update"),
            text_row(2, 20, b'd')
        );
        storage.close().expect("close heap");
        cleanup(&path);
    }

    #[test]
    fn repeated_heap_reuse_keeps_one_slot_and_increments_row_id_generation() {
        let path = test_path("heap-repeated-slot-reuse");
        let mut storage = HeapStorage::create(&path, table()).expect("create heap");
        let mut current = storage
            .insert(&[ScalarValue::Int64(0), ScalarValue::Text("row-0".into())])
            .expect("insert initial occupant");
        for generation in 2..=65 {
            storage.delete(current).expect("delete current occupant");
            current = storage
                .insert(&[
                    ScalarValue::Int64(i64::from(generation)),
                    ScalarValue::Text(format!("row-{generation}")),
                ])
                .expect("reuse current slot");
            assert_eq!(current.page, FIRST_HEAP_PAGE);
            assert_eq!(current.slot, 0);
            assert_eq!(current.generation, generation);
        }
        let page = storage
            .buffer
            .read_page(FIRST_HEAP_PAGE)
            .expect("read data page");
        assert_eq!(page.page().header().expect("valid page").slot_count, 1);
        drop(page);
        storage.close().expect("close heap");
        cleanup(&path);
    }

    #[test]
    fn rollback_of_reused_slot_restores_tombstone_generation() {
        let path = test_path("heap-reuse-rollback");
        let mut storage = HeapStorage::create(&path, table()).expect("create heap");
        let old = storage
            .insert(&[ScalarValue::Int64(1), ScalarValue::Text("old".into())])
            .expect("insert old occupant");
        storage.delete(old).expect("commit tombstone");

        let mut transaction = storage.begin_transaction().expect("begin reuse");
        let candidate = storage
            .insert_in(
                &mut transaction,
                &[ScalarValue::Int64(2), ScalarValue::Text("candidate".into())],
            )
            .expect("reuse tombstone");
        assert_eq!(candidate.generation, old.generation + 1);
        transaction.rollback().expect("rollback reuse");

        assert!(matches!(
            storage.read_row(old),
            Err(StorageError::RowDeleted { row_id }) if row_id == old
        ));
        assert!(matches!(
            storage.read_row(candidate),
            Err(StorageError::StaleRowId {
                actual_generation,
                ..
            }) if actual_generation == old.generation
        ));
        storage.close().expect("close heap");
        cleanup(&path);
    }

    #[test]
    fn committed_reuse_redoes_and_uncommitted_flushed_reuse_undoes_generation() {
        let path = test_path("heap-reuse-recovery");
        let mut storage = HeapStorage::create(&path, table()).expect("create heap");
        let old = storage
            .insert(&[ScalarValue::Int64(1), ScalarValue::Text("old".into())])
            .expect("insert old occupant");
        storage.delete(old).expect("commit tombstone");
        storage.close().expect("persist tombstone baseline");

        let mut storage = HeapStorage::open(&path, table()).expect("open tombstone baseline");
        let committed = storage
            .insert(&[ScalarValue::Int64(2), ScalarValue::Text("committed".into())])
            .expect("commit reused occupant");
        assert_eq!(committed.generation, old.generation + 1);
        storage.simulate_crash();

        let mut storage = HeapStorage::open(&path, table()).expect("redo committed reuse");
        assert_eq!(
            storage.scan().expect("scan committed reuse")[0].0,
            committed
        );
        storage.delete(committed).expect("commit second tombstone");
        storage.close().expect("persist second tombstone");

        let mut storage = HeapStorage::open(&path, table()).expect("open second tombstone");
        let mut transaction = storage.begin_transaction().expect("begin loser reuse");
        let loser = storage
            .insert_in(
                &mut transaction,
                &[ScalarValue::Int64(3), ScalarValue::Text("loser".into())],
            )
            .expect("reuse tombstone as loser");
        assert_eq!(loser.generation, committed.generation + 1);
        storage.flush().expect("steal-flush loser reuse");
        drop(transaction);
        storage.simulate_crash();

        let reopened = HeapStorage::open(&path, table()).expect("undo loser reuse");
        assert!(matches!(
            reopened.read_row(committed),
            Err(StorageError::RowDeleted { row_id }) if row_id == committed
        ));
        assert!(matches!(
            reopened.read_row(loser),
            Err(StorageError::StaleRowId {
                actual_generation,
                ..
            }) if actual_generation == committed.generation
        ));
        drop(reopened);
        cleanup(&path);
    }

    #[test]
    fn runtime_rollback_restores_updates_and_deletes() {
        let path = test_path("heap-mutation-rollback");
        let mut storage = HeapStorage::create(&path, table()).expect("create heap");
        let first = storage
            .insert(&[ScalarValue::Int64(1), ScalarValue::Text("first".into())])
            .expect("insert first");
        let second = storage
            .insert(&[ScalarValue::Int64(2), ScalarValue::Text("second".into())])
            .expect("insert second");
        let mut transaction = storage.begin_transaction().expect("begin");
        storage
            .update_in(
                &mut transaction,
                first,
                &[ScalarValue::Int64(1), ScalarValue::Text("updated".into())],
            )
            .expect("update");
        storage.delete_in(&mut transaction, second).expect("delete");
        transaction.rollback().expect("rollback");

        assert_eq!(
            storage.read_row(first).expect("restored update")[1],
            ScalarValue::Text("first".into())
        );
        assert_eq!(
            storage.read_row(second).expect("restored delete")[1],
            ScalarValue::Text("second".into())
        );
        storage.close().expect("close heap");
        cleanup(&path);
    }

    #[test]
    fn committed_and_loser_row_mutations_recover_from_full_page_images() {
        let path = test_path("heap-mutation-recovery");
        let mut storage = HeapStorage::create(&path, table()).expect("create heap");
        let first = storage
            .insert(&[ScalarValue::Int64(1), ScalarValue::Text("first".into())])
            .expect("insert first");
        let second = storage
            .insert(&[ScalarValue::Int64(2), ScalarValue::Text("second".into())])
            .expect("insert second");
        storage.close().expect("persist baseline");

        let mut storage = HeapStorage::open(&path, table()).expect("open baseline");
        let mut winner = storage.begin_transaction().expect("begin winner");
        storage
            .update_in(
                &mut winner,
                first,
                &[ScalarValue::Int64(1), ScalarValue::Text("committed".into())],
            )
            .expect("winner update");
        winner.commit().expect("commit winner");
        storage.simulate_crash();

        let mut storage = HeapStorage::open(&path, table()).expect("redo winner");
        assert_eq!(
            storage.read_row(first).expect("committed row")[1],
            ScalarValue::Text("committed".into())
        );
        let mut loser = storage.begin_transaction().expect("begin loser");
        storage.delete_in(&mut loser, second).expect("loser delete");
        storage.flush().expect("steal loser delete");
        drop(loser);
        storage.simulate_crash();

        let recovered = HeapStorage::open(&path, table()).expect("undo loser");
        assert_eq!(
            recovered.read_row(second).expect("restored row")[1],
            ScalarValue::Text("second".into())
        );
        drop(recovered);

        let mut storage = HeapStorage::open(&path, table()).expect("open for delete winner");
        let mut delete_winner = storage.begin_transaction().expect("begin delete winner");
        storage
            .delete_in(&mut delete_winner, second)
            .expect("winner delete");
        delete_winner.commit().expect("commit delete winner");
        storage.simulate_crash();

        let mut storage = HeapStorage::open(&path, table()).expect("redo delete winner");
        assert!(matches!(
            storage.read_row(second),
            Err(StorageError::RowDeleted { .. })
        ));
        let mut update_loser = storage.begin_transaction().expect("begin update loser");
        storage
            .update_in(
                &mut update_loser,
                first,
                &[ScalarValue::Int64(1), ScalarValue::Text("loser".into())],
            )
            .expect("loser update");
        storage.flush().expect("steal loser update");
        drop(update_loser);
        storage.simulate_crash();

        let recovered = HeapStorage::open(&path, table()).expect("undo update loser");
        assert_eq!(
            recovered.read_row(first).expect("restored winner value")[1],
            ScalarValue::Text("committed".into())
        );
        assert!(matches!(
            recovered.read_row(second),
            Err(StorageError::RowDeleted { .. })
        ));
        drop(recovered);
        cleanup(&path);
    }

    #[test]
    fn mid_statement_delete_wal_failure_rolls_back_prior_deletes() {
        let path = test_path("heap-delete-statement-failure");
        let mut storage = HeapStorage::create(&path, table()).expect("create heap");
        let first = storage
            .insert(&[ScalarValue::Int64(1), ScalarValue::Text("first".into())])
            .expect("insert first");
        let second = storage
            .insert(&[ScalarValue::Int64(2), ScalarValue::Text("second".into())])
            .expect("insert second");
        let mut transaction = storage.begin_transaction().expect("begin statement");

        storage
            .delete_in(&mut transaction, first)
            .expect("delete first target");
        storage
            .transactions
            .wal()
            .borrow_mut()
            .inject_partial_append_failure(100);
        assert!(matches!(
            storage.delete_in(&mut transaction, second),
            Err(StorageError::Wal(_))
        ));
        transaction
            .rollback()
            .expect("rollback whole failed statement");

        assert_eq!(
            storage.read_row(first).expect("first restored")[0],
            ScalarValue::Int64(1)
        );
        assert_eq!(
            storage.read_row(second).expect("second unchanged")[0],
            ScalarValue::Int64(2)
        );
        storage.close().expect("close heap");
        cleanup(&path);
    }

    #[test]
    fn invalid_buffer_capacity_does_not_truncate_an_existing_heap() {
        let path = test_path("heap-invalid-capacity");
        let mut storage = HeapStorage::create(&path, table()).expect("create heap");
        storage
            .insert(&[ScalarValue::Int64(7), ScalarValue::Text("Ada".into())])
            .expect("insert row");
        storage.close().expect("close heap");

        assert!(matches!(
            HeapStorage::create_with_buffer_pool_size(&path, table(), 0),
            Err(StorageError::Buffer(BufferError::InvalidCapacity))
        ));

        let mut reopened = HeapStorage::open(&path, table()).expect("reopen preserved heap");
        assert_eq!(reopened.scan().expect("scan preserved heap").len(), 1);
        cleanup(&path);
    }

    #[test]
    fn wal_create_failure_does_not_truncate_an_existing_database_path() {
        let path = test_path("heap-wal-create-failure");
        let original = b"existing database contents";
        std::fs::write(&path, original).expect("write existing database");
        let wal_path = wal_path(&path);
        std::fs::create_dir(&wal_path).expect("create conflicting WAL directory");

        assert!(matches!(
            HeapStorage::create(&path, table()),
            Err(StorageError::Wal(crate::WalError::Io(_)))
        ));
        assert_eq!(
            std::fs::read(&path).expect("read preserved database"),
            original
        );

        std::fs::remove_dir(wal_path).expect("remove WAL directory");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn inserts_across_pages_and_scans_with_capacity_one() {
        let path = test_path("heap-multi-page");
        let mut storage =
            HeapStorage::create_with_buffer_pool_size(&path, table(), 1).expect("create heap");
        for id in 0..700_i64 {
            storage
                .insert(&[ScalarValue::Int64(id), ScalarValue::Text("row".into())])
                .expect("insert row");
        }
        let rows = storage.scan().expect("scan multi-page heap");
        assert_eq!(rows.len(), 700);
        assert!(rows.iter().any(|(row_id, _)| row_id.page.0 > 1));
        storage.close().expect("close heap");

        let mut reopened =
            HeapStorage::open_with_buffer_pool_size(&path, table(), 1).expect("reopen heap");
        assert_eq!(reopened.scan().expect("reopen scan").len(), 700);
        cleanup(&path);
    }

    #[test]
    fn corrupted_tuple_encoding_is_rejected_during_scan() {
        let path = test_path("heap-corrupt-row");
        let mut storage = HeapStorage::create(&path, table()).expect("create heap");
        storage
            .insert(&[ScalarValue::Int64(1), ScalarValue::Text("Ada".into())])
            .expect("insert row");
        storage.close().expect("close heap");

        let mut pages = PageManager::open(&path).expect("open page manager");
        let mut page = pages.read_page(FIRST_HEAP_PAGE).expect("read data page");
        let slot = page.slot(SlotId(0)).expect("read row slot");
        page.bytes_mut()[usize::from(slot.offset)] = 99;
        page.refresh_checksum();
        pages.write_page(&page).expect("write corrupt row");
        pages.sync().expect("sync corrupt row");
        drop(pages);

        let mut reopened = HeapStorage::open(&path, table()).expect("reopen heap");
        assert!(matches!(
            reopened.scan(),
            Err(StorageError::Codec(crate::CodecError::UnknownScalarTag(99)))
        ));
        cleanup(&path);
    }

    #[test]
    fn corrupted_page_bounds_are_rejected_during_recovery_open() {
        let path = test_path("heap-corrupt-slot");
        let mut storage = HeapStorage::create(&path, table()).expect("create heap");
        storage
            .insert(&[ScalarValue::Int64(1), ScalarValue::Text("Ada".into())])
            .expect("insert row");
        storage.close().expect("close heap");

        let mut pages = PageManager::open(&path).expect("open page manager");
        let mut page = pages.read_page(FIRST_HEAP_PAGE).expect("read data page");
        page.bytes_mut()[crate::PAGE_HEADER_SIZE + 2..crate::PAGE_HEADER_SIZE + 4]
            .copy_from_slice(&(u16::MAX - 1).to_le_bytes());
        page.refresh_checksum();
        pages.write_page(&page).expect("write corrupt slot");
        pages.sync().expect("sync corrupt slot");
        drop(pages);

        assert!(matches!(
            HeapStorage::open(&path, table()),
            Err(StorageError::Page(
                crate::PageError::RecordOutOfBounds { .. }
            ))
        ));
        cleanup(&path);
    }

    #[test]
    fn corruption_after_checkpoint_is_detected_on_later_page_access() {
        let path = test_path("heap-checkpoint-page-corruption");
        let mut storage = HeapStorage::create(&path, table()).expect("create heap");
        storage
            .insert(&[
                ScalarValue::Int64(1),
                ScalarValue::Text("checkpointed".into()),
            ])
            .expect("insert row");
        storage.checkpoint().expect("checkpoint heap");
        assert!(storage.wal_records().expect("scan recycled WAL").is_empty());
        storage.close().expect("close heap");

        let mut pages = PageManager::open(&path).expect("open page manager");
        let mut page = pages.read_page(FIRST_HEAP_PAGE).expect("read data page");
        let slot = page.slot(SlotId(0)).expect("read row slot");
        page.bytes_mut()[usize::from(slot.offset)] ^= 0x40;
        pages.write_page(&page).expect("write corrupt page");
        pages.sync().expect("sync corrupt page");
        drop(pages);

        let mut reopened = HeapStorage::open(&path, table()).expect("open without WAL page read");
        assert!(matches!(
            reopened.scan(),
            Err(StorageError::Page(
                crate::PageError::ChecksumMismatch { .. }
            ))
        ));
        cleanup(&path);
    }

    #[test]
    fn recovery_hard_fails_before_trusting_a_corrupt_pages_lsn() {
        let path = test_path("heap-recovery-page-corruption");
        let mut storage = HeapStorage::create(&path, table()).expect("create heap");
        storage
            .insert(&[
                ScalarValue::Int64(1),
                ScalarValue::Text("durable winner".into()),
            ])
            .expect("insert committed row");
        storage.flush().expect("flush WAL and data page");
        storage.simulate_crash();

        let mut pages = PageManager::open(&path).expect("open page manager");
        let mut page = pages.read_page(FIRST_HEAP_PAGE).expect("read data page");
        assert!(page.page_lsn().expect("valid high pageLSN").is_some());
        let slot = page.slot(SlotId(0)).expect("read row slot");
        page.bytes_mut()[usize::from(slot.offset)] ^= 0x20;
        pages.write_page(&page).expect("write corrupt page");
        pages.sync().expect("sync corrupt page");
        drop(pages);

        assert!(matches!(
            HeapStorage::open(&path, table()),
            Err(StorageError::Page(
                crate::PageError::ChecksumMismatch { .. }
            ))
        ));
        cleanup(&path);
    }

    #[test]
    fn unsupported_heap_metadata_version_is_rejected() {
        let path = test_path("heap-corrupt-metadata");
        let storage = HeapStorage::create(&path, table()).expect("create heap");
        storage.close().expect("close heap");

        let mut pages = PageManager::open(&path).expect("open page manager");
        let mut header = pages.read_page(PageId(0)).expect("read metadata page");
        header.bytes_mut()[20..22].copy_from_slice(&99_u16.to_le_bytes());
        pages.write_page(&header).expect("write corrupt metadata");
        pages.sync().expect("sync corrupt metadata");
        drop(pages);

        assert!(matches!(
            HeapStorage::open(&path, table()),
            Err(StorageError::Metadata(
                crate::MetadataError::UnsupportedVersion(99)
            ))
        ));
        cleanup(&path);
    }

    #[test]
    fn previous_heap_metadata_version_is_not_reinterpreted() {
        let path = test_path("heap-old-metadata-version");
        let storage = HeapStorage::create(&path, table()).expect("create heap");
        storage.close().expect("close heap");

        let mut pages = PageManager::open(&path).expect("open page manager");
        let mut header = pages.read_page(PageId(0)).expect("read metadata page");
        for old_version in [1_u16, 2] {
            header.bytes_mut()[20..22].copy_from_slice(&old_version.to_le_bytes());
            pages
                .write_page(&header)
                .expect("write old metadata version");
            pages.sync().expect("sync old metadata version");
            assert!(matches!(
                HeapStorage::open(&path, table()),
                Err(StorageError::Metadata(
                    crate::MetadataError::UnsupportedVersion(version)
                )) if version == old_version
            ));
        }
        drop(pages);
        cleanup(&path);
    }

    #[test]
    fn implicit_insert_commits_wal_without_flushing_the_heap_page() {
        let path = test_path("heap-no-insert-flush");
        let mut storage = HeapStorage::create(&path, table()).expect("create heap");
        storage
            .insert(&[ScalarValue::Int64(1), ScalarValue::Text("Ada".into())])
            .expect("insert row");
        assert!(storage.durable_lsn().expect("durable LSN").is_some());

        let mut disk = PageManager::open(&path).expect("open page file");
        assert_eq!(
            disk.read_page(FIRST_HEAP_PAGE)
                .expect("read data page")
                .header()
                .expect("valid page")
                .slot_count,
            0
        );
        drop(disk);
        storage.close().expect("close heap");
        let mut reopened = HeapStorage::open(&path, table()).expect("reopen heap");
        assert_eq!(reopened.scan().expect("scan").len(), 1);
        cleanup(&path);
    }

    #[test]
    fn open_automatically_recovers_a_committed_unflushed_insert() {
        let path = test_path("heap-open-recovery");
        let mut storage = HeapStorage::create(&path, table()).expect("create heap");
        storage
            .insert(&[ScalarValue::Int64(1), ScalarValue::Text("recovered".into())])
            .expect("insert committed row");
        storage.simulate_crash();

        let mut reopened = HeapStorage::open(&path, table()).expect("open with recovery");
        let rows = reopened.scan().expect("scan recovered rows");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1[0], ScalarValue::Int64(1));
        cleanup(&path);
    }

    #[test]
    fn open_automatically_undoes_an_active_flushed_insert() {
        let path = test_path("heap-open-undo");
        let mut storage = HeapStorage::create(&path, table()).expect("create heap");
        let mut transaction = storage.begin_transaction().expect("begin transaction");
        storage
            .insert_in(
                &mut transaction,
                &[ScalarValue::Int64(1), ScalarValue::Text("loser".into())],
            )
            .expect("insert active row");
        storage.flush().expect("flush active page and WAL");
        drop(transaction);
        storage.simulate_crash();

        let mut reopened = HeapStorage::open(&path, table()).expect("open with undo");
        assert!(reopened.scan().expect("scan recovered rows").is_empty());
        cleanup(&path);
    }

    #[test]
    fn one_transaction_updates_multiple_pages_with_one_prev_lsn_chain() {
        let path = test_path("heap-multi-page-transaction");
        let mut storage =
            HeapStorage::create_with_buffer_pool_size(&path, table(), 1).expect("create heap");
        let mut transaction = storage.begin_transaction().expect("begin transaction");
        let text = "x".repeat(1_000);
        for id in 0..12_i64 {
            storage
                .insert_in(
                    &mut transaction,
                    &[ScalarValue::Int64(id), ScalarValue::Text(text.clone())],
                )
                .expect("insert in transaction");
        }
        transaction.commit().expect("commit transaction");
        assert_eq!(transaction.state(), TransactionState::Committed);
        let records = storage.wal_records().expect("scan WAL");
        assert_eq!(records.len(), 14);
        for pair in records.windows(2) {
            assert_eq!(pair[1].prev_lsn, Some(pair[0].lsn));
        }
        storage.close().expect("close heap");

        let mut reopened =
            HeapStorage::open_with_buffer_pool_size(&path, table(), 1).expect("reopen heap");
        let rows = reopened.scan().expect("scan rows");
        assert_eq!(rows.len(), 12);
        assert!(rows.iter().any(|(row_id, _)| row_id.page.0 > 1));
        cleanup(&path);
    }

    #[test]
    fn wal_flush_failure_prevents_new_page_allocation() {
        let path = test_path("heap-allocation-wal-failure");
        let mut storage = HeapStorage::create(&path, table()).expect("create heap");
        storage
            .insert(&[ScalarValue::Int64(1), ScalarValue::Text("x".repeat(3_900))])
            .expect("fill first page");
        storage
            .transactions
            .wal()
            .borrow_mut()
            .inject_flush_failure();

        assert!(matches!(
            storage.insert(&[ScalarValue::Int64(2), ScalarValue::Text("y".repeat(1_000)),]),
            Err(StorageError::Wal(_))
        ));
        assert_eq!(storage.buffer.page_count(), 3);
        drop(storage);
        cleanup(&path);
    }

    #[test]
    fn rollback_truncates_a_partial_new_page_allocation() {
        let path = test_path("heap-partial-allocation-rollback");
        let mut storage = HeapStorage::create(&path, table()).expect("create heap");
        storage
            .insert(&[ScalarValue::Int64(1), ScalarValue::Text("x".repeat(3_900))])
            .expect("fill first page");
        storage.buffer.inject_partial_page_allocation_failure(137);

        assert!(matches!(
            storage.insert(&[
                ScalarValue::Int64(2),
                ScalarValue::Text("new page".repeat(200)),
            ]),
            Err(StorageError::Io(_))
        ));
        assert_eq!(storage.buffer.page_count(), 3);
        storage.close().expect("close after rollback");

        let mut reopened = HeapStorage::open(&path, table()).expect("reopen aligned heap");
        let rows = reopened.scan().expect("scan rows");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1[0], ScalarValue::Int64(1));
        cleanup(&path);
    }

    #[test]
    fn startup_undo_is_finalized_before_a_later_winner_commits() {
        let path = test_path("heap-startup-undo-later-winner");
        let mut storage = HeapStorage::create(&path, table()).expect("create heap");
        let mut loser = storage.begin_transaction().expect("begin loser");
        storage
            .insert_in(
                &mut loser,
                &[ScalarValue::Int64(1), ScalarValue::Text("loser".into())],
            )
            .expect("insert loser");
        storage.flush().expect("steal loser page");
        drop(loser);
        storage.simulate_crash();

        let mut recovered = HeapStorage::open(&path, table()).expect("recover loser");
        assert!(recovered.scan().expect("scan recovered heap").is_empty());
        assert!(matches!(
            recovered
                .wal_records()
                .expect("scan finalized WAL")
                .last()
                .map(|record| &record.kind),
            Some(WalRecordKind::RollbackComplete)
        ));
        recovered
            .insert(&[ScalarValue::Int64(2), ScalarValue::Text("winner".into())])
            .expect("insert later winner");
        recovered.close().expect("close recovered heap");

        let mut reopened = HeapStorage::open(&path, table()).expect("reopen after winner");
        let rows = reopened.scan().expect("scan winner");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1[0], ScalarValue::Int64(2));
        cleanup(&path);
    }

    #[test]
    fn active_writer_blocks_another_write_until_runtime_rollback_finishes() {
        let path = test_path("heap-single-writer-rollback");
        let mut storage = HeapStorage::create(&path, table()).expect("create heap");
        let mut loser = storage.begin_transaction().expect("begin loser");
        let mut winner = storage
            .begin_transaction()
            .expect("begin concurrent reader");
        storage
            .insert_in(
                &mut loser,
                &[ScalarValue::Int64(1), ScalarValue::Text("loser".into())],
            )
            .expect("insert loser row");

        assert!(matches!(
            storage.insert_in(
                &mut winner,
                &[ScalarValue::Int64(2), ScalarValue::Text("winner".into())],
            ),
            Err(StorageError::Transaction(TransactionError::WriterBusy {
                txn_id
            })) if txn_id == loser.id()
        ));
        loser.rollback().expect("rollback loser");
        storage
            .insert_in(
                &mut winner,
                &[ScalarValue::Int64(2), ScalarValue::Text("winner".into())],
            )
            .expect("insert winner after rollback");
        winner.commit().expect("commit winner");
        assert_eq!(loser.state(), TransactionState::RolledBack);
        assert_eq!(storage.scan().expect("scan rows").len(), 1);
        storage.close().expect("close heap");

        let mut reopened = HeapStorage::open(&path, table()).expect("reopen heap");
        let rows = reopened.scan().expect("scan reopened rows");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1[0], ScalarValue::Int64(2));
        cleanup(&path);
    }

    #[test]
    fn rollback_reverses_multiple_updates_to_the_same_page() {
        let path = test_path("heap-rollback-same-page");
        let mut storage = HeapStorage::create(&path, table()).expect("create heap");
        storage
            .insert(&[ScalarValue::Int64(0), ScalarValue::Text("base".into())])
            .expect("insert committed base row");
        let mut transaction = storage.begin_transaction().expect("begin transaction");
        for id in 1..=2_i64 {
            storage
                .insert_in(
                    &mut transaction,
                    &[
                        ScalarValue::Int64(id),
                        ScalarValue::Text("temporary".into()),
                    ],
                )
                .expect("insert temporary row");
        }

        transaction.inject_rollback_interruption_after(1);
        assert!(matches!(
            transaction.rollback(),
            Err(StorageError::Transaction(
                TransactionError::RollbackInterrupted
            ))
        ));
        assert_eq!(transaction.state(), TransactionState::RollbackPending);
        assert_eq!(storage.scan().expect("scan partial rollback").len(), 2);
        transaction.rollback().expect("rollback transaction");
        let rows = storage.scan().expect("scan after rollback");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1[0], ScalarValue::Int64(0));
        let records = storage.wal_records().expect("scan WAL");
        assert!(matches!(
            records[records.len() - 2].kind,
            WalRecordKind::Abort
        ));
        assert!(matches!(
            records.last().map(|record| &record.kind),
            Some(WalRecordKind::RollbackComplete)
        ));
        storage.close().expect("close heap");

        let mut reopened = HeapStorage::open(&path, table()).expect("reopen heap");
        assert_eq!(reopened.scan().expect("scan reopened rows").len(), 1);
        cleanup(&path);
    }

    #[test]
    fn rollback_failure_keeps_writer_and_retry_completes_physical_undo() {
        let path = test_path("heap-rollback-retry");
        let mut storage = HeapStorage::create(&path, table()).expect("create heap");
        let mut first = storage
            .begin_transaction()
            .expect("begin first transaction");
        let mut second = storage
            .begin_transaction()
            .expect("begin second transaction");
        storage
            .insert_in(
                &mut first,
                &[ScalarValue::Int64(1), ScalarValue::Text("temporary".into())],
            )
            .expect("insert temporary row");
        storage.buffer.inject_page_write_failure();

        assert!(matches!(first.rollback(), Err(StorageError::Io(_))));
        assert_eq!(first.state(), TransactionState::RollbackPending);
        assert!(matches!(
            storage.insert_in(
                &mut second,
                &[ScalarValue::Int64(2), ScalarValue::Text("blocked".into())],
            ),
            Err(StorageError::Transaction(TransactionError::WriterBusy {
                txn_id
            })) if txn_id == first.id()
        ));

        first.rollback().expect("retry rollback");
        storage
            .insert_in(
                &mut second,
                &[ScalarValue::Int64(2), ScalarValue::Text("winner".into())],
            )
            .expect("write after rollback retry");
        second.commit().expect("commit second transaction");
        let rows = storage.scan().expect("scan rows");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1[0], ScalarValue::Int64(2));
        storage.close().expect("close heap");
        cleanup(&path);
    }

    #[test]
    fn rollback_removes_multiple_new_pages_in_reverse_order() {
        let path = test_path("heap-rollback-new-pages");
        let mut storage = HeapStorage::create(&path, table()).expect("create heap");
        storage
            .insert(&[ScalarValue::Int64(0), ScalarValue::Text("base".repeat(975))])
            .expect("fill original data page");
        let original_page_count = storage.buffer.page_count();
        let mut transaction = storage.begin_transaction().expect("begin transaction");
        for id in 1..=2_i64 {
            storage
                .insert_in(
                    &mut transaction,
                    &[
                        ScalarValue::Int64(id),
                        ScalarValue::Text("temporary".repeat(430)),
                    ],
                )
                .expect("allocate transaction page");
        }
        assert_eq!(storage.buffer.page_count(), original_page_count + 2);

        storage.buffer.inject_page_sync_failure();
        assert!(matches!(transaction.rollback(), Err(StorageError::Io(_))));
        assert_eq!(transaction.state(), TransactionState::RollbackPending);
        assert_eq!(storage.buffer.page_count(), original_page_count + 1);
        transaction.rollback().expect("rollback allocated pages");
        assert_eq!(storage.buffer.page_count(), original_page_count);
        assert_eq!(storage.scan().expect("scan after rollback").len(), 1);
        storage.close().expect("close heap");

        let mut reopened = HeapStorage::open(&path, table()).expect("reopen heap");
        assert_eq!(reopened.buffer.page_count(), original_page_count);
        assert_eq!(reopened.scan().expect("scan reopened rows").len(), 1);
        cleanup(&path);
    }

    #[test]
    fn crash_during_runtime_rollback_is_completed_by_startup_recovery() {
        let path = test_path("heap-crash-during-rollback");
        let mut storage = HeapStorage::create(&path, table()).expect("create heap");
        let mut transaction = storage.begin_transaction().expect("begin transaction");
        storage
            .insert_in(
                &mut transaction,
                &[ScalarValue::Int64(1), ScalarValue::Text("temporary".into())],
            )
            .expect("insert first temporary row");
        storage
            .insert_in(
                &mut transaction,
                &[
                    ScalarValue::Int64(2),
                    ScalarValue::Text("temporary-2".into()),
                ],
            )
            .expect("insert second temporary row");
        transaction.inject_rollback_interruption_after(1);

        assert!(matches!(
            transaction.rollback(),
            Err(StorageError::Transaction(
                TransactionError::RollbackInterrupted
            ))
        ));
        assert_eq!(transaction.state(), TransactionState::RollbackPending);
        drop(transaction);
        storage.simulate_crash();

        let mut reopened = HeapStorage::open(&path, table()).expect("recover interrupted rollback");
        assert!(reopened.scan().expect("scan recovered rows").is_empty());
        cleanup(&path);
    }

    #[test]
    fn completed_rollback_is_not_reapplied_over_a_later_winner() {
        let path = test_path("heap-rollback-later-winner");
        let mut storage = HeapStorage::create(&path, table()).expect("create heap");
        let mut loser = storage.begin_transaction().expect("begin loser");
        storage
            .insert_in(
                &mut loser,
                &[ScalarValue::Int64(1), ScalarValue::Text("loser".into())],
            )
            .expect("insert loser");
        loser.rollback().expect("rollback loser");

        let mut winner = storage.begin_transaction().expect("begin winner");
        storage
            .insert_in(
                &mut winner,
                &[ScalarValue::Int64(2), ScalarValue::Text("winner".into())],
            )
            .expect("insert winner");
        winner.commit().expect("commit winner");
        storage.simulate_crash();

        let mut reopened = HeapStorage::open(&path, table()).expect("recover database");
        let rows = reopened.scan().expect("scan recovered winner");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1[0], ScalarValue::Int64(2));
        cleanup(&path);
    }

    #[test]
    fn read_only_transaction_does_not_reserve_the_writer() {
        let path = test_path("heap-read-only-does-not-block");
        let mut storage = HeapStorage::create(&path, table()).expect("create heap");
        let mut read_only = storage
            .begin_transaction()
            .expect("begin read-only transaction");
        let mut writer = storage
            .begin_transaction()
            .expect("begin writer transaction");

        storage
            .insert_in(
                &mut writer,
                &[ScalarValue::Int64(1), ScalarValue::Text("writer".into())],
            )
            .expect("read-only transaction must not block writer");
        writer.commit().expect("commit writer");
        read_only.commit().expect("commit read-only transaction");
        storage.close().expect("close heap");
        cleanup(&path);
    }

    #[test]
    fn commit_failure_keeps_writer_until_the_same_commit_is_retried() {
        let path = test_path("heap-commit-failure-writer");
        let mut storage = HeapStorage::create(&path, table()).expect("create heap");
        let mut first = storage
            .begin_transaction()
            .expect("begin first transaction");
        let mut second = storage
            .begin_transaction()
            .expect("begin second transaction");
        storage
            .insert_in(
                &mut first,
                &[ScalarValue::Int64(1), ScalarValue::Text("first".into())],
            )
            .expect("insert first row");
        storage
            .transactions
            .wal()
            .borrow_mut()
            .inject_flush_failure();

        assert!(matches!(first.commit(), Err(StorageError::Wal(_))));
        assert_eq!(first.state(), TransactionState::CommitPending);
        assert!(matches!(
            storage.insert_in(
                &mut second,
                &[ScalarValue::Int64(2), ScalarValue::Text("blocked".into())],
            ),
            Err(StorageError::Transaction(TransactionError::WriterBusy {
                txn_id
            })) if txn_id == first.id()
        ));
        first.commit().expect("retry commit");
        storage
            .insert_in(
                &mut second,
                &[ScalarValue::Int64(2), ScalarValue::Text("second".into())],
            )
            .expect("write after durable commit");
        second.commit().expect("commit second transaction");
        storage.close().expect("close heap");
        cleanup(&path);
    }

    #[test]
    fn dropping_a_dirty_writer_poisons_later_writes() {
        let path = test_path("heap-drop-dirty-writer");
        let mut storage = HeapStorage::create(&path, table()).expect("create heap");
        let mut first = storage
            .begin_transaction()
            .expect("begin first transaction");
        storage
            .insert_in(
                &mut first,
                &[
                    ScalarValue::Int64(1),
                    ScalarValue::Text("unfinished".into()),
                ],
            )
            .expect("insert unfinished row");
        drop(first);

        let mut second = storage.begin_transaction().expect("begin read-only handle");
        assert!(matches!(
            storage.insert_in(
                &mut second,
                &[ScalarValue::Int64(2), ScalarValue::Text("must fail".into())],
            ),
            Err(StorageError::Transaction(
                TransactionError::RecoveryRequired
            ))
        ));
        assert!(matches!(
            storage.close(),
            Err(StorageError::Transaction(
                TransactionError::RecoveryRequired
            ))
        ));
        drop(second);
        cleanup(&path);
    }

    #[test]
    fn transaction_from_another_database_is_rejected_before_writer_acquisition() {
        let first_path = test_path("heap-foreign-first");
        let second_path = test_path("heap-foreign-second");
        let mut first = HeapStorage::create(&first_path, table()).expect("create first heap");
        let mut second = HeapStorage::create(&second_path, table()).expect("create second heap");
        let mut transaction = first.begin_transaction().expect("begin first transaction");

        assert!(matches!(
            second.insert_in(
                &mut transaction,
                &[ScalarValue::Int64(1), ScalarValue::Text("foreign".into())],
            ),
            Err(StorageError::Transaction(
                TransactionError::ForeignTransaction { txn_id }
            )) if txn_id == transaction.id()
        ));
        transaction.rollback().expect("finish transaction");
        first.close().expect("close first heap");
        second.close().expect("close second heap");
        cleanup(&first_path);
        cleanup(&second_path);
    }

    #[test]
    fn close_rejects_an_unfinished_writer() {
        let path = test_path("heap-close-active-writer");
        let mut storage = HeapStorage::create(&path, table()).expect("create heap");
        let mut transaction = storage.begin_transaction().expect("begin transaction");
        storage
            .insert_in(
                &mut transaction,
                &[
                    ScalarValue::Int64(1),
                    ScalarValue::Text("unfinished".into()),
                ],
            )
            .expect("insert unfinished row");

        assert!(matches!(
            storage.close(),
            Err(StorageError::Transaction(
                TransactionError::UnfinishedWriter { txn_id }
            )) if txn_id == transaction.id()
        ));
        drop(transaction);
        cleanup(&path);
    }

    #[test]
    fn checkpoint_requires_zero_outstanding_transactions() {
        let path = test_path("checkpoint-outstanding");
        let mut storage = HeapStorage::create(&path, table()).expect("create heap");
        let mut read_only = storage.begin_transaction().expect("begin read-only");

        assert!(matches!(
            storage.checkpoint(),
            Err(StorageError::Checkpoint(
                CheckpointError::OutstandingTransactions { count: 1 }
            ))
        ));
        read_only.commit().expect("commit read-only");
        storage.checkpoint().expect("checkpoint after commit");
        assert_eq!(storage.wal_generation().expect("generation"), 2);
        storage.close().expect("close heap");
        cleanup(&path);
    }

    #[test]
    fn close_rejects_a_live_read_only_transaction() {
        let path = test_path("close-read-only");
        let mut storage = HeapStorage::create(&path, table()).expect("create heap");
        let transaction = storage.begin_transaction().expect("begin read-only");
        assert!(matches!(
            storage.close(),
            Err(StorageError::Transaction(
                TransactionError::OutstandingTransactions { count: 1 }
            ))
        ));
        drop(transaction);
        cleanup(&path);
    }

    #[test]
    fn active_and_pending_writers_block_checkpoint() {
        let path = test_path("checkpoint-writer-states");
        let mut storage = HeapStorage::create(&path, table()).expect("create heap");
        let mut transaction = storage.begin_transaction().expect("begin writer");
        storage
            .insert_in(
                &mut transaction,
                &[ScalarValue::Int64(1), ScalarValue::Text("active".into())],
            )
            .expect("insert row");
        assert!(matches!(
            storage.checkpoint(),
            Err(StorageError::Checkpoint(CheckpointError::WriterActive {
                txn_id
            })) if txn_id == transaction.id()
        ));

        storage
            .transactions
            .wal()
            .borrow_mut()
            .inject_flush_failure();
        assert!(matches!(transaction.commit(), Err(StorageError::Wal(_))));
        assert_eq!(transaction.state(), TransactionState::CommitPending);
        assert!(matches!(
            storage.checkpoint(),
            Err(StorageError::Checkpoint(
                CheckpointError::WriterActive { .. }
            ))
        ));
        transaction.commit().expect("retry commit");

        let mut rollback = storage.begin_transaction().expect("begin rollback");
        storage
            .insert_in(
                &mut rollback,
                &[ScalarValue::Int64(2), ScalarValue::Text("rollback".into())],
            )
            .expect("insert rollback row");
        storage.buffer.inject_page_sync_failure();
        assert!(matches!(rollback.rollback(), Err(StorageError::Io(_))));
        assert_eq!(rollback.state(), TransactionState::RollbackPending);
        assert!(matches!(
            storage.checkpoint(),
            Err(StorageError::Checkpoint(
                CheckpointError::WriterActive { .. }
            ))
        ));
        rollback.rollback().expect("retry rollback");
        storage.checkpoint().expect("checkpoint quiescent storage");
        storage.close().expect("close heap");
        cleanup(&path);
    }

    #[test]
    fn recovery_required_state_blocks_checkpoint() {
        let path = test_path("checkpoint-recovery-required");
        let mut storage = HeapStorage::create(&path, table()).expect("create heap");
        let mut transaction = storage.begin_transaction().expect("begin writer");
        storage
            .insert_in(
                &mut transaction,
                &[ScalarValue::Int64(1), ScalarValue::Text("dirty".into())],
            )
            .expect("insert dirty row");
        drop(transaction);

        assert!(matches!(
            storage.checkpoint(),
            Err(StorageError::Checkpoint(CheckpointError::RecoveryRequired))
        ));
        storage.simulate_crash();
        let reopened = HeapStorage::open(&path, table()).expect("recover database");
        reopened.close().expect("close recovered database");
        cleanup(&path);
    }

    #[test]
    fn logical_lsn_and_page_lsn_remain_comparable_after_checkpoint() {
        let path = test_path("checkpoint-page-lsn");
        let mut storage = HeapStorage::create(&path, table()).expect("create heap");
        storage
            .insert(&[ScalarValue::Int64(1), ScalarValue::Text("before".into())])
            .expect("insert before checkpoint");
        let old_update = storage
            .wal_records()
            .expect("scan old WAL")
            .into_iter()
            .find(|record| matches!(record.kind, WalRecordKind::PageUpdate { .. }))
            .expect("old page update")
            .lsn;
        storage.checkpoint().expect("checkpoint");
        assert!(storage.wal_records().expect("scan new WAL").is_empty());

        storage
            .insert(&[ScalarValue::Int64(2), ScalarValue::Text("after".into())])
            .expect("insert after checkpoint");
        let new_records = storage.wal_records().expect("scan current WAL");
        let new_update = new_records
            .iter()
            .find(|record| matches!(record.kind, WalRecordKind::PageUpdate { .. }))
            .expect("new page update")
            .lsn;
        assert!(new_update > old_update);
        assert_eq!(new_records.len(), 3);
        storage.simulate_crash();

        let mut reopened = HeapStorage::open(&path, table()).expect("recover current generation");
        let rows = reopened.scan().expect("scan recovered rows");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[1].1[0], ScalarValue::Int64(2));
        cleanup(&path);
    }

    #[test]
    fn repeated_checkpoints_bound_wal_size_and_keep_lsn_monotonic() {
        let path = test_path("checkpoint-bounded-growth");
        let mut storage = HeapStorage::create(&path, table()).expect("create heap");
        let mut last_update = None;
        for value in 0..20 {
            storage
                .insert(&[
                    ScalarValue::Int64(value),
                    ScalarValue::Text(format!("row-{value}")),
                ])
                .expect("insert row");
            let update = storage
                .wal_records()
                .expect("scan WAL")
                .into_iter()
                .find(|record| matches!(record.kind, WalRecordKind::PageUpdate { .. }))
                .expect("page update")
                .lsn;
            assert!(last_update.is_none_or(|previous| update > previous));
            last_update = Some(update);
            storage.checkpoint().expect("checkpoint cycle");
        }
        assert_eq!(storage.wal_generation().expect("generation"), 21);
        let root = wal_path(&path);
        let alternate = wal_alternate_path(&root);
        let retained_bytes = [&root, &alternate]
            .into_iter()
            .filter_map(|candidate| std::fs::metadata(candidate).ok())
            .map(|metadata| metadata.len())
            .sum::<u64>();
        let bound = 2 * (WAL_HEADER_SIZE + WAL_MAX_RECORD_SIZE + 80) as u64;
        assert!(retained_bytes <= bound, "retained {retained_bytes} bytes");
        storage.close().expect("close heap");

        let mut reopened = HeapStorage::open(&path, table()).expect("reopen checkpoints");
        assert_eq!(reopened.scan().expect("scan rows").len(), 20);
        cleanup(&path);
    }

    #[test]
    fn transaction_id_high_water_survives_checkpoint_and_reopen() {
        let path = test_path("checkpoint-txn-id");
        let mut storage = HeapStorage::create(&path, table()).expect("create heap");
        let mut first = storage.begin_transaction().expect("begin first");
        let first_id = first.id();
        first.commit().expect("commit first");
        storage.checkpoint().expect("checkpoint");
        storage.close().expect("close heap");

        let mut reopened = HeapStorage::open(&path, table()).expect("reopen heap");
        let mut next = reopened.begin_transaction().expect("begin next");
        assert!(next.id() > first_id);
        next.commit().expect("commit next");
        reopened.close().expect("close reopened heap");
        cleanup(&path);
    }

    #[test]
    fn partial_generation_creation_falls_back_to_the_last_valid_wal() {
        let path = test_path("checkpoint-partial-generation");
        let mut storage = HeapStorage::create(&path, table()).expect("create heap");
        storage
            .insert(&[ScalarValue::Int64(1), ScalarValue::Text("durable".into())])
            .expect("insert row");
        storage
            .inject_partial_checkpoint_rotation(20)
            .expect("inject rotation failure");
        assert!(matches!(
            storage.checkpoint(),
            Err(StorageError::Wal(WalError::Io(_)))
        ));
        storage.simulate_crash();

        let mut reopened = HeapStorage::open(&path, table()).expect("fallback to old generation");
        assert_eq!(reopened.wal_generation().expect("generation"), 1);
        assert_eq!(reopened.scan().expect("scan rows").len(), 1);
        assert!(!wal_alternate_path(wal_path(&path)).exists());
        cleanup(&path);
    }

    #[test]
    fn durable_new_header_is_selected_if_rotation_stops_before_runtime_switch() {
        let path = test_path("checkpoint-durable-new-header");
        let mut storage = HeapStorage::create(&path, table()).expect("create heap");
        storage
            .insert(&[ScalarValue::Int64(1), ScalarValue::Text("durable".into())])
            .expect("insert row");
        storage
            .inject_partial_checkpoint_rotation(WAL_HEADER_SIZE)
            .expect("inject post-header failure");
        assert!(matches!(
            storage.checkpoint(),
            Err(StorageError::Wal(WalError::Io(_)))
        ));
        storage.simulate_crash();

        let root = wal_path(&path);
        let alternate = wal_alternate_path(&root);
        assert!(root.exists() && alternate.exists());
        let mut reopened = HeapStorage::open(&path, table()).expect("select durable generation");
        assert_eq!(reopened.wal_generation().expect("generation"), 2);
        assert_eq!(reopened.scan().expect("scan rows").len(), 1);
        assert!(!root.exists());
        cleanup(&path);
    }

    #[test]
    fn valid_new_generation_wins_and_corrupt_newer_generation_is_rejected() {
        let path = test_path("checkpoint-generation-selection");
        let mut storage = HeapStorage::create(&path, table()).expect("create heap");
        storage
            .insert(&[ScalarValue::Int64(1), ScalarValue::Text("durable".into())])
            .expect("insert row");
        storage.checkpoint().expect("checkpoint");
        let current = storage.current_wal_path().expect("current WAL path");
        assert_ne!(current, wal_path(&path));
        storage.simulate_crash();

        let reopened = HeapStorage::open(&path, table()).expect("select generation 2");
        assert_eq!(reopened.wal_generation().expect("generation"), 2);
        reopened.simulate_crash();
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(&current)
            .expect("open newer WAL");
        use std::io::{Seek, SeekFrom, Write};
        file.seek(SeekFrom::Start(4)).expect("seek version");
        file.write_all(&99_u16.to_le_bytes())
            .expect("corrupt newer version");
        drop(file);
        assert!(matches!(
            HeapStorage::open(&path, table()),
            Err(StorageError::Wal(WalError::UnsupportedVersion(99)))
        ));
        cleanup(&path);
    }

    #[test]
    fn recovery_input_contains_only_post_checkpoint_records() {
        let path = test_path("checkpoint-recovery-range");
        let mut storage = HeapStorage::create(&path, table()).expect("create heap");
        for value in 0..12 {
            storage
                .insert(&[
                    ScalarValue::Int64(value),
                    ScalarValue::Text(format!("old-{value}")),
                ])
                .expect("insert old row");
        }
        storage.checkpoint().expect("checkpoint old history");
        storage
            .insert(&[ScalarValue::Int64(20), ScalarValue::Text("new-a".into())])
            .expect("insert new row");
        storage
            .insert(&[ScalarValue::Int64(21), ScalarValue::Text("new-b".into())])
            .expect("insert new row");
        storage.simulate_crash();

        let (_, records, _) =
            WalManager::open_for_recovery(wal_path(&path)).expect("select recovery generation");
        assert_eq!(records.len(), 6);
        let mut reopened = HeapStorage::open(&path, table()).expect("recover new history");
        assert_eq!(reopened.scan().expect("scan all rows").len(), 14);
        cleanup(&path);
    }

    #[test]
    fn checkpoint_after_runtime_rollback_recycles_the_completed_chain() {
        let path = test_path("checkpoint-rollback");
        let mut storage = HeapStorage::create(&path, table()).expect("create heap");
        storage.checkpoint().expect("initial checkpoint");
        let mut transaction = storage.begin_transaction().expect("begin rollback");
        storage
            .insert_in(
                &mut transaction,
                &[ScalarValue::Int64(1), ScalarValue::Text("temporary".into())],
            )
            .expect("insert temporary row");
        transaction.rollback().expect("rollback transaction");
        storage.checkpoint().expect("checkpoint rollback");
        assert!(storage.wal_records().expect("scan WAL").is_empty());
        storage.close().expect("close heap");

        let mut reopened = HeapStorage::open(&path, table()).expect("reopen heap");
        assert!(reopened.scan().expect("scan heap").is_empty());
        cleanup(&path);
    }

    const PROCESS_CRASH_CHILD_TEST: &str = "heap::tests::process_crash_child_entrypoint";

    fn prepare_process_crash_baseline(case: &str) -> (std::path::PathBuf, Lsn, u64) {
        let path = test_path(case);
        cleanup(&path);
        let mut storage = HeapStorage::create(&path, table()).expect("create crash-test heap");
        storage
            .insert(&[ScalarValue::Int64(1), ScalarValue::Text("before".into())])
            .expect("insert crash-test baseline");
        let last_lsn = storage
            .wal_records()
            .expect("scan baseline WAL")
            .last()
            .expect("baseline WAL record")
            .lsn;
        let generation = storage.wal_generation().expect("baseline WAL generation");
        storage.close().expect("close crash-test baseline");
        (path, last_lsn, generation)
    }

    fn spawn_crash_child(path: &std::path::Path, case: &str, point: TestCrashPoint) {
        let mut command =
            std::process::Command::new(std::env::current_exe().expect("current test executable"));
        command
            .arg("--exact")
            .arg(PROCESS_CRASH_CHILD_TEST)
            .arg("--nocapture");
        crash_test::configure_child(&mut command, case, path, point);
        let status = command.status().expect("start crash-test child");
        assert_eq!(
            status.code(),
            Some(crash_test::EXIT_CODE),
            "child `{case}` did not terminate at crash point {point:?}: {status}"
        );
    }

    fn only_row(storage: &mut HeapStorage) -> (netbadb_types::RowId, String) {
        let rows = storage.scan().expect("scan crash-test heap");
        assert_eq!(rows.len(), 1, "crash recovery changed row cardinality");
        let (row_id, values) = rows.into_iter().next().expect("one crash-test row");
        assert_eq!(values[0], ScalarValue::Int64(1));
        let ScalarValue::Text(value) = &values[1] else {
            panic!("crash-test value is not text");
        };
        (row_id, value.clone())
    }

    fn reopen_value(path: &std::path::Path) -> String {
        let mut storage = HeapStorage::open(path, table()).expect("reopen crash-test heap");
        let (_, value) = only_row(&mut storage);
        storage.close().expect("close recovered crash-test heap");
        value
    }

    fn assert_reopens_twice_with(path: &std::path::Path, expected: &str) {
        assert_eq!(reopen_value(path), expected);
        assert_eq!(reopen_value(path), expected);
    }

    fn update_crash_test_value(
        storage: &mut HeapStorage,
        transaction: &mut crate::Transaction,
        value: &str,
    ) {
        let (row_id, _) = only_row(storage);
        storage
            .update_in(
                transaction,
                row_id,
                &[ScalarValue::Int64(1), ScalarValue::Text(value.into())],
            )
            .expect("update crash-test row");
    }

    fn relocation_source(storage: &mut HeapStorage) -> netbadb_types::RowId {
        storage
            .scan()
            .expect("scan relocation heap")
            .into_iter()
            .find_map(|(row_id, values)| {
                (values.first() == Some(&ScalarValue::Int64(1))).then_some(row_id)
            })
            .expect("relocation source row")
    }

    fn run_process_crash_child(case: &str, path: &std::path::Path) {
        match case {
            "active-writer-after-durable-page-flush" => {
                let mut storage = HeapStorage::open(path, table()).expect("open child heap");
                let mut transaction = storage.begin_transaction().expect("begin active writer");
                update_crash_test_value(&mut storage, &mut transaction, "uncommitted");
                // STEAL: PageUpdate WAL is durable, then the uncommitted data
                // page is written and synchronized, but no Commit exists.
                storage.flush().expect("durably flush uncommitted page");
                crash_test::maybe_crash(TestCrashPoint::ActiveWriterAfterDurablePageFlush);
            }
            "committed-without-data-flush" | "commit-boundary" => {
                let mut storage = HeapStorage::open(path, table()).expect("open child heap");
                let mut transaction = storage.begin_transaction().expect("begin commit writer");
                update_crash_test_value(&mut storage, &mut transaction, "after");
                transaction.commit().expect("commit child transaction");
                // NO-FORCE: commit returned after durable WAL, while the dirty
                // data page has not been explicitly flushed or closed.
                crash_test::maybe_crash(TestCrashPoint::CommittedWithoutDataFlush);
            }
            "rollback-single" => {
                let mut storage = HeapStorage::open(path, table()).expect("open child heap");
                let mut transaction = storage.begin_transaction().expect("begin rollback writer");
                update_crash_test_value(&mut storage, &mut transaction, "after");
                transaction.rollback().expect("rollback child transaction");
            }
            "rollback-multiple" => {
                let mut storage = HeapStorage::open(path, table()).expect("open child heap");
                let mut transaction = storage.begin_transaction().expect("begin rollback writer");
                update_crash_test_value(&mut storage, &mut transaction, "v1");
                update_crash_test_value(&mut storage, &mut transaction, "v2");
                transaction.rollback().expect("rollback child transaction");
            }
            "active-multiple-for-recovery" => {
                let mut storage = HeapStorage::open(path, table()).expect("open child heap");
                let mut transaction = storage.begin_transaction().expect("begin active writer");
                update_crash_test_value(&mut storage, &mut transaction, "v1");
                update_crash_test_value(&mut storage, &mut transaction, "v2");
                storage.flush().expect("durably flush active writer");
                crash_test::maybe_crash(TestCrashPoint::ActiveWriterAfterDurablePageFlush);
            }
            "committed-reuse-without-data-flush" => {
                let mut storage = HeapStorage::open(path, table()).expect("open reuse child heap");
                let reused = storage
                    .insert(&[
                        ScalarValue::Int64(2),
                        ScalarValue::Text("committed reuse".into()),
                    ])
                    .expect("commit reused slot");
                assert_eq!(reused.generation, 2);
                crash_test::maybe_crash(TestCrashPoint::CommittedWithoutDataFlush);
            }
            "active-reuse-after-durable-page-flush" => {
                let mut storage = HeapStorage::open(path, table()).expect("open reuse child heap");
                let mut transaction = storage.begin_transaction().expect("begin reuse loser");
                let reused = storage
                    .insert_in(
                        &mut transaction,
                        &[
                            ScalarValue::Int64(2),
                            ScalarValue::Text("loser reuse".into()),
                        ],
                    )
                    .expect("reuse slot as loser");
                assert_eq!(reused.generation, 2);
                storage.flush().expect("durably flush reused loser page");
                crash_test::maybe_crash(TestCrashPoint::ActiveWriterAfterDurablePageFlush);
            }
            "relocation-boundary" => {
                let mut storage = HeapStorage::open(path, table()).expect("open relocation heap");
                let old = relocation_source(&mut storage);
                let mut transaction = storage.begin_transaction().expect("begin relocation");
                let _ = storage
                    .update_in(&mut transaction, old, &text_row(1, 1_000, b'r'))
                    .expect("relocate row");
                panic!("relocation completed without reaching configured crash point");
            }
            "active-relocation-after-durable-page-flush" => {
                let mut storage = HeapStorage::open(path, table()).expect("open relocation heap");
                let old = relocation_source(&mut storage);
                let mut transaction = storage.begin_transaction().expect("begin relocation");
                storage
                    .update_in(&mut transaction, old, &text_row(1, 1_000, b'u'))
                    .expect("relocate loser");
                storage.flush().expect("flush uncommitted relocation");
                crash_test::maybe_crash(TestCrashPoint::ActiveWriterAfterDurablePageFlush);
            }
            "committed-relocation-without-data-flush" => {
                let mut storage = HeapStorage::open(path, table()).expect("open relocation heap");
                let old = relocation_source(&mut storage);
                let mut transaction = storage.begin_transaction().expect("begin relocation");
                storage
                    .update_in(&mut transaction, old, &text_row(1, 1_000, b'c'))
                    .expect("relocate winner");
                transaction.commit().expect("commit relocation");
                crash_test::maybe_crash(TestCrashPoint::CommittedWithoutDataFlush);
            }
            "index-build-loser" => {
                let mut storage =
                    HeapStorage::open(path, indexed_table()).expect("open index heap");
                storage
                    .create_index(ColumnId(2))
                    .expect("build index until crash point");
            }
            "index-build-winner" => {
                let mut storage =
                    HeapStorage::open(path, indexed_table()).expect("open index heap");
                storage
                    .create_index(ColumnId(2))
                    .expect("commit index build");
                crash_test::maybe_crash(TestCrashPoint::CommittedWithoutDataFlush);
            }
            "registered-insert-loser" => {
                let mut storage =
                    HeapStorage::open(path, indexed_table()).expect("open registered heap");
                let mut transaction = storage.begin_transaction().expect("begin insert loser");
                storage
                    .insert_in(
                        &mut transaction,
                        &[
                            ScalarValue::UInt64(9),
                            ScalarValue::UInt64(90),
                            ScalarValue::Text("loser-insert".into()),
                        ],
                    )
                    .expect("reach registered insert crash point");
            }
            "registered-update-loser" => {
                let mut storage =
                    HeapStorage::open(path, indexed_table()).expect("open registered heap");
                let old = storage
                    .scan()
                    .unwrap()
                    .into_iter()
                    .find(|(_, values)| values[0] == ScalarValue::UInt64(1))
                    .unwrap()
                    .0;
                let mut transaction = storage.begin_transaction().expect("begin update loser");
                storage
                    .update_in(
                        &mut transaction,
                        old,
                        &[
                            ScalarValue::UInt64(1),
                            ScalarValue::UInt64(99),
                            ScalarValue::Text("U".repeat(3000)),
                        ],
                    )
                    .expect("reach registered update crash point");
            }
            "registered-delete-loser" => {
                let mut storage =
                    HeapStorage::open(path, indexed_table()).expect("open registered heap");
                let old = storage
                    .scan()
                    .unwrap()
                    .into_iter()
                    .find(|(_, values)| values[0] == ScalarValue::UInt64(1))
                    .unwrap()
                    .0;
                let mut transaction = storage.begin_transaction().expect("begin delete loser");
                storage
                    .delete_in(&mut transaction, old)
                    .expect("reach registered delete crash point");
            }
            "registered-insert-winner"
            | "registered-update-winner"
            | "registered-delete-winner" => {
                let mut storage =
                    HeapStorage::open(path, indexed_table()).expect("open registered heap");
                match case {
                    "registered-insert-winner" => {
                        storage
                            .insert(&[
                                ScalarValue::UInt64(9),
                                ScalarValue::UInt64(90),
                                ScalarValue::Text("winner-insert".into()),
                            ])
                            .expect("commit registered insert");
                    }
                    "registered-update-winner" => {
                        let old = storage
                            .scan()
                            .unwrap()
                            .into_iter()
                            .find(|(_, values)| values[0] == ScalarValue::UInt64(1))
                            .unwrap()
                            .0;
                        storage
                            .update(
                                old,
                                &[
                                    ScalarValue::UInt64(1),
                                    ScalarValue::UInt64(99),
                                    ScalarValue::Text("W".repeat(3000)),
                                ],
                            )
                            .expect("commit registered update");
                    }
                    "registered-delete-winner" => {
                        let old = storage
                            .scan()
                            .unwrap()
                            .into_iter()
                            .find(|(_, values)| values[0] == ScalarValue::UInt64(1))
                            .unwrap()
                            .0;
                        storage.delete(old).expect("commit registered delete");
                    }
                    _ => unreachable!(),
                }
                crash_test::maybe_crash(TestCrashPoint::CommittedWithoutDataFlush);
            }
            "recovery-open" => {
                let _storage = HeapStorage::open(path, table()).expect("start child recovery");
            }
            "checkpoint" => {
                let mut storage = HeapStorage::open(path, table()).expect("open child heap");
                storage.checkpoint().expect("checkpoint child heap");
            }
            "partial-final-wal-record" => {
                let mut storage = HeapStorage::open(path, table()).expect("open child heap");
                let _transaction = storage.begin_transaction().expect("append partial Begin");
            }
            other => panic!("unknown process crash case `{other}`"),
        }
        panic!("process crash child `{case}` returned without reaching its crash point");
    }

    #[test]
    fn process_crash_child_entrypoint() {
        if std::env::var_os(crash_test::CHILD_ENV).is_none() {
            return;
        }
        let case = std::env::var(crash_test::CASE_ENV).expect("crash child case");
        let path = std::env::var_os(crash_test::DATABASE_PATH_ENV)
            .map(std::path::PathBuf::from)
            .expect("crash child database path");
        run_process_crash_child(&case, &path);
    }

    #[test]
    fn process_crash_steal_loser_is_undone_after_durable_page_flush() {
        let (path, _, _) = prepare_process_crash_baseline("process-crash-steal-durable-page");
        spawn_crash_child(
            &path,
            "active-writer-after-durable-page-flush",
            TestCrashPoint::ActiveWriterAfterDurablePageFlush,
        );
        assert_reopens_twice_with(&path, "before");
        cleanup(&path);
    }

    #[test]
    fn process_crash_no_force_winner_is_redone_after_commit_returns() {
        let (path, _, _) = prepare_process_crash_baseline("process-crash-no-force-commit");
        spawn_crash_child(
            &path,
            "committed-without-data-flush",
            TestCrashPoint::CommittedWithoutDataFlush,
        );
        assert_reopens_twice_with(&path, "after");
        cleanup(&path);
    }

    fn prepare_index_build_crash_baseline(case: &str) -> std::path::PathBuf {
        let path = test_path(case);
        cleanup(&path);
        let mut storage = HeapStorage::create(&path, indexed_table()).expect("create index heap");
        for row in indexed_rows() {
            storage.insert(&row).expect("insert index baseline row");
        }
        storage.close().expect("close index baseline");
        path
    }

    fn assert_index_build_loser_is_absent(path: &std::path::Path) {
        let mut reopened = HeapStorage::open(path, indexed_table()).expect("recover index loser");
        assert!(reopened.indexes().is_empty());
        assert_eq!(reopened.scan().expect("scan index loser heap").len(), 4);
        reopened.close().expect("close recovered index loser");
    }

    #[test]
    fn process_crash_index_build_loser_never_exposes_registration() {
        for (case, point) in [
            ("before-catalog", TestCrashPoint::IndexBuildBeforeCatalogLog),
            (
                "after-catalog-log",
                TestCrashPoint::IndexBuildAfterCatalogLog,
            ),
            (
                "after-catalog-publish",
                TestCrashPoint::IndexBuildAfterCatalogPublish,
            ),
        ] {
            let path = prepare_index_build_crash_baseline(&format!("index-build-{case}"));
            spawn_crash_child(&path, "index-build-loser", point);
            assert_index_build_loser_is_absent(&path);
            cleanup(&path);
        }
    }

    #[test]
    fn process_crash_committed_index_build_redoes_registry_and_tree() {
        let path = prepare_index_build_crash_baseline("index-build-winner");
        spawn_crash_child(
            &path,
            "index-build-winner",
            TestCrashPoint::CommittedWithoutDataFlush,
        );
        let mut reopened = HeapStorage::open(&path, indexed_table()).expect("recover index winner");
        let definition = reopened
            .index_for_column(ColumnId(2))
            .cloned()
            .expect("discover committed index");
        assert_eq!(
            reopened
                .btree()
                .lookup(definition.handle, &ScalarValue::UInt64(10))
                .expect("lookup recovered index")
                .len(),
            2
        );
        reopened.close().expect("close index winner");
        cleanup(&path);
    }

    fn prepare_registered_dml_crash_baseline(case: &str) -> std::path::PathBuf {
        let path = test_path(case);
        cleanup(&path);
        let mut storage = HeapStorage::create_with_buffer_pool_size(&path, indexed_table(), 1)
            .expect("create registered DML baseline");
        storage
            .insert(&[
                ScalarValue::UInt64(1),
                ScalarValue::UInt64(10),
                ScalarValue::Text("before".into()),
            ])
            .expect("insert registered baseline");
        storage
            .insert(&[
                ScalarValue::UInt64(2),
                ScalarValue::UInt64(20),
                ScalarValue::Text("F".repeat(1500)),
            ])
            .expect("insert relocation filler");
        storage.create_index(ColumnId(2)).expect("team index");
        storage.create_index(ColumnId(3)).expect("name index");
        storage.close().expect("close registered baseline");
        path
    }

    fn assert_registered_dml_state(path: &std::path::Path, case: &str, winner: bool) {
        let mut storage = HeapStorage::open_with_buffer_pool_size(path, indexed_table(), 1)
            .expect("recover registered DML");
        let team = storage.index_for_column(ColumnId(2)).unwrap().handle;
        let name = storage.index_for_column(ColumnId(3)).unwrap().handle;
        let rows = storage.scan().expect("scan recovered registered DML");
        let row_one = rows
            .iter()
            .find(|(_, values)| values[0] == ScalarValue::UInt64(1));
        match case {
            "insert" if winner => {
                let (row_id, _) = rows
                    .iter()
                    .find(|(_, values)| values[0] == ScalarValue::UInt64(9))
                    .expect("winner insert row");
                assert!(
                    storage
                        .btree()
                        .contains_exact(team, &ScalarValue::UInt64(90), *row_id)
                        .unwrap()
                );
            }
            "insert" => {
                assert!(
                    rows.iter()
                        .all(|(_, values)| values[0] != ScalarValue::UInt64(9))
                );
                assert!(
                    storage
                        .btree()
                        .lookup(team, &ScalarValue::UInt64(90))
                        .unwrap()
                        .is_empty()
                );
            }
            "update" if winner => {
                let (row_id, values) = row_one.expect("winner updated row");
                assert_eq!(values[1], ScalarValue::UInt64(99));
                assert!(
                    storage
                        .btree()
                        .contains_exact(team, &ScalarValue::UInt64(99), *row_id)
                        .unwrap()
                );
                assert!(
                    storage
                        .btree()
                        .contains_exact(name, &ScalarValue::Text("W".repeat(3000)), *row_id)
                        .unwrap()
                );
            }
            "update" => {
                let (row_id, values) = row_one.expect("restored updated row");
                assert_eq!(values[1], ScalarValue::UInt64(10));
                assert_eq!(values[2], ScalarValue::Text("before".into()));
                assert!(
                    storage
                        .btree()
                        .contains_exact(team, &ScalarValue::UInt64(10), *row_id)
                        .unwrap()
                );
                assert!(
                    storage
                        .btree()
                        .contains_exact(name, &ScalarValue::Text("before".into()), *row_id)
                        .unwrap()
                );
            }
            "delete" if winner => assert!(row_one.is_none()),
            "delete" => {
                let (row_id, _) = row_one.expect("restored deleted row");
                assert!(
                    storage
                        .btree()
                        .contains_exact(team, &ScalarValue::UInt64(10), *row_id)
                        .unwrap()
                );
                assert!(
                    storage
                        .btree()
                        .contains_exact(name, &ScalarValue::Text("before".into()), *row_id)
                        .unwrap()
                );
            }
            _ => unreachable!(),
        }
        storage.close().expect("close recovered registered DML");
    }

    #[test]
    fn process_crash_registered_dml_losers_undo_heap_and_indexes() {
        for (case, point) in [
            ("insert", TestCrashPoint::RegisteredInsertAfterHeapPublish),
            ("update", TestCrashPoint::RegisteredUpdateAfterHeapPublish),
            (
                "delete",
                TestCrashPoint::RegisteredDeleteAfterFirstIndexPublish,
            ),
        ] {
            let path = prepare_registered_dml_crash_baseline(&format!("registered-{case}-loser"));
            spawn_crash_child(&path, &format!("registered-{case}-loser"), point);
            assert_registered_dml_state(&path, case, false);
            assert_registered_dml_state(&path, case, false);
            cleanup(&path);
        }
    }

    #[test]
    fn process_crash_registered_dml_winners_redo_heap_and_indexes() {
        for case in ["insert", "update", "delete"] {
            let path = prepare_registered_dml_crash_baseline(&format!("registered-{case}-winner"));
            spawn_crash_child(
                &path,
                &format!("registered-{case}-winner"),
                TestCrashPoint::CommittedWithoutDataFlush,
            );
            assert_registered_dml_state(&path, case, true);
            assert_registered_dml_state(&path, case, true);
            cleanup(&path);
        }
    }

    fn prepare_reuse_crash_baseline(case: &str) -> (std::path::PathBuf, netbadb_types::RowId) {
        let path = test_path(case);
        cleanup(&path);
        let mut storage = HeapStorage::create(&path, table()).expect("create reuse baseline");
        let old = storage
            .insert(&[
                ScalarValue::Int64(1),
                ScalarValue::Text("old occupant".into()),
            ])
            .expect("insert reuse baseline");
        storage.delete(old).expect("commit reuse tombstone");
        storage.close().expect("close reuse baseline");
        (path, old)
    }

    #[test]
    fn process_crash_no_force_committed_reuse_preserves_new_generation() {
        let (path, old) = prepare_reuse_crash_baseline("process-crash-reuse-winner");
        spawn_crash_child(
            &path,
            "committed-reuse-without-data-flush",
            TestCrashPoint::CommittedWithoutDataFlush,
        );
        let mut reopened = HeapStorage::open(&path, table()).expect("redo committed reuse");
        let rows = reopened.scan().expect("scan committed reuse");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0.generation, old.generation + 1);
        assert!(matches!(
            reopened.read_row(old),
            Err(StorageError::StaleRowId { .. })
        ));
        reopened.close().expect("close committed reuse");
        cleanup(&path);
    }

    #[test]
    fn process_crash_steal_loser_reuse_restores_old_tombstone_generation() {
        let (path, old) = prepare_reuse_crash_baseline("process-crash-reuse-loser");
        spawn_crash_child(
            &path,
            "active-reuse-after-durable-page-flush",
            TestCrashPoint::ActiveWriterAfterDurablePageFlush,
        );
        let mut reopened = HeapStorage::open(&path, table()).expect("undo loser reuse");
        assert!(reopened.scan().expect("scan undone reuse").is_empty());
        assert!(matches!(
            reopened.read_row(old),
            Err(StorageError::RowDeleted { row_id }) if row_id == old
        ));
        reopened.close().expect("close undone reuse");
        cleanup(&path);
    }

    fn prepare_relocation_crash_baseline(case: &str) -> (std::path::PathBuf, netbadb_types::RowId) {
        let path = test_path(case);
        cleanup(&path);
        let mut storage = HeapStorage::create(&path, table()).expect("create relocation baseline");
        let old = storage
            .insert(&text_row(1, 100, b'a'))
            .expect("insert source");
        storage
            .insert(&text_row(2, 3_800, b'b'))
            .expect("fill source page");
        let destination = storage
            .insert(&text_row(3, 300, b'd'))
            .expect("create destination");
        assert_eq!((old.page, destination.page), (FIRST_HEAP_PAGE, PageId(3)));
        storage.close().expect("close relocation baseline");
        (path, old)
    }

    fn assert_relocation_loser_restored(path: &std::path::Path, old: netbadb_types::RowId) {
        for _ in 0..2 {
            let mut reopened = HeapStorage::open(path, table()).expect("recover relocation loser");
            assert_eq!(
                reopened.read_row(old).expect("source restored"),
                text_row(1, 100, b'a')
            );
            let rows = reopened.scan().expect("scan restored heap");
            assert_eq!(rows.len(), 3);
            assert!(
                rows.iter()
                    .any(|(_, values)| *values == text_row(3, 300, b'd'))
            );
            reopened.close().expect("close recovered heap");
        }
    }

    #[test]
    fn process_crash_after_first_relocation_log_restores_both_pages() {
        let (path, old) = prepare_relocation_crash_baseline("relocation-crash-first-log");
        spawn_crash_child(
            &path,
            "relocation-boundary",
            TestCrashPoint::RelocationAfterFirstPageUpdateLog,
        );
        assert_relocation_loser_restored(&path, old);
        cleanup(&path);
    }

    #[test]
    fn process_crash_after_both_relocation_logs_restores_both_pages() {
        let (path, old) = prepare_relocation_crash_baseline("relocation-crash-both-logs");
        spawn_crash_child(
            &path,
            "relocation-boundary",
            TestCrashPoint::RelocationAfterBothPageUpdateLogs,
        );
        assert_relocation_loser_restored(&path, old);
        cleanup(&path);
    }

    #[test]
    fn process_crash_after_first_relocation_publish_restores_mixed_pages() {
        let (path, old) = prepare_relocation_crash_baseline("relocation-crash-first-publish");
        spawn_crash_child(
            &path,
            "relocation-boundary",
            TestCrashPoint::RelocationAfterFirstPagePublish,
        );
        assert_relocation_loser_restored(&path, old);
        cleanup(&path);
    }

    #[test]
    fn process_crash_steal_loser_relocation_restores_source_and_destination() {
        let (path, old) = prepare_relocation_crash_baseline("relocation-crash-steal");
        spawn_crash_child(
            &path,
            "active-relocation-after-durable-page-flush",
            TestCrashPoint::ActiveWriterAfterDurablePageFlush,
        );
        assert_relocation_loser_restored(&path, old);
        cleanup(&path);
    }

    #[test]
    fn process_crash_no_force_committed_relocation_redoes_both_pages() {
        let (path, old) = prepare_relocation_crash_baseline("relocation-crash-winner");
        spawn_crash_child(
            &path,
            "committed-relocation-without-data-flush",
            TestCrashPoint::CommittedWithoutDataFlush,
        );
        let mut reopened = HeapStorage::open(&path, table()).expect("redo relocation winner");
        assert!(matches!(
            reopened.read_row(old),
            Err(StorageError::RowDeleted { .. })
        ));
        let (current, values) = reopened
            .scan()
            .expect("scan relocation winner")
            .into_iter()
            .find(|(_, values)| values.first() == Some(&ScalarValue::Int64(1)))
            .expect("relocated winner");
        assert_ne!(current, old);
        assert_eq!(values, text_row(1, 1_000, b'c'));
        reopened.close().expect("close relocation winner");
        cleanup(&path);
    }

    #[test]
    fn process_crash_commit_after_append_recovers_a_valid_wal_prefix() {
        let (path, _, _) = prepare_process_crash_baseline("process-crash-commit-after-append");
        spawn_crash_child(&path, "commit-boundary", TestCrashPoint::CommitAfterAppend);
        let first = reopen_value(&path);
        assert!(matches!(first.as_str(), "before" | "after"));
        assert_eq!(reopen_value(&path), first);
        cleanup(&path);
    }

    #[test]
    fn process_crash_commit_after_wal_sync_preserves_winner() {
        let (path, _, _) = prepare_process_crash_baseline("process-crash-commit-after-sync");
        spawn_crash_child(&path, "commit-boundary", TestCrashPoint::CommitAfterWalSync);
        assert_reopens_twice_with(&path, "after");
        cleanup(&path);
    }

    #[test]
    fn process_crash_rollback_after_abort_append_restores_before() {
        let (path, _, _) = prepare_process_crash_baseline("process-crash-abort-after-append");
        spawn_crash_child(
            &path,
            "rollback-single",
            TestCrashPoint::RollbackAfterAbortAppend,
        );
        assert_reopens_twice_with(&path, "before");
        cleanup(&path);
    }

    #[test]
    fn process_crash_rollback_after_abort_sync_restores_before() {
        let (path, _, _) = prepare_process_crash_baseline("process-crash-abort-after-sync");
        spawn_crash_child(
            &path,
            "rollback-single",
            TestCrashPoint::RollbackAfterAbortSync,
        );
        assert_reopens_twice_with(&path, "before");
        cleanup(&path);
    }

    #[test]
    fn process_crash_mid_rollback_converges_after_first_durable_undo() {
        let (path, _, _) = prepare_process_crash_baseline("process-crash-mid-rollback");
        spawn_crash_child(
            &path,
            "rollback-multiple",
            TestCrashPoint::RollbackAfterPageUndo,
        );
        assert_reopens_twice_with(&path, "before");
        cleanup(&path);
    }

    #[test]
    fn process_crash_after_rollback_complete_append_restores_before() {
        let (path, _, _) = prepare_process_crash_baseline("process-crash-complete-after-append");
        spawn_crash_child(
            &path,
            "rollback-single",
            TestCrashPoint::RollbackAfterCompleteAppend,
        );
        assert_reopens_twice_with(&path, "before");
        cleanup(&path);
    }

    #[test]
    fn process_crash_after_rollback_complete_sync_restores_before() {
        let (path, _, _) = prepare_process_crash_baseline("process-crash-complete-after-sync");
        spawn_crash_child(
            &path,
            "rollback-single",
            TestCrashPoint::RollbackAfterCompleteSync,
        );
        assert_reopens_twice_with(&path, "before");
        cleanup(&path);
    }

    #[test]
    fn process_crash_recovery_interruption_is_idempotent() {
        let (path, _, _) = prepare_process_crash_baseline("process-crash-recovery-operation");
        spawn_crash_child(
            &path,
            "active-multiple-for-recovery",
            TestCrashPoint::ActiveWriterAfterDurablePageFlush,
        );
        spawn_crash_child(
            &path,
            "recovery-open",
            TestCrashPoint::RecoveryAfterPageOperation,
        );
        assert_reopens_twice_with(&path, "before");
        cleanup(&path);
    }

    fn verify_checkpoint_crash(
        path: &std::path::Path,
        baseline_lsn: Lsn,
        baseline_generation: u64,
    ) {
        let mut storage = HeapStorage::open(path, table()).expect("reopen checkpoint crash");
        assert_eq!(
            storage.wal_generation().expect("selected generation"),
            baseline_generation + 1
        );
        assert_eq!(only_row(&mut storage).1, "before");
        let mut transaction = storage.begin_transaction().expect("begin after checkpoint");
        assert!(transaction.id().0 > 1);
        assert!(transaction.last_lsn() > baseline_lsn);
        update_crash_test_value(&mut storage, &mut transaction, "after-checkpoint");
        transaction.commit().expect("commit after checkpoint crash");
        storage.close().expect("close checkpoint crash heap");
        assert_reopens_twice_with(path, "after-checkpoint");
    }

    #[test]
    fn process_crash_checkpoint_selects_durable_higher_generation() {
        let (path, baseline_lsn, baseline_generation) =
            prepare_process_crash_baseline("process-crash-checkpoint-new-generation");
        spawn_crash_child(
            &path,
            "checkpoint",
            TestCrashPoint::CheckpointAfterNewGenerationDurable,
        );
        let root = wal_path(&path);
        let alternate = wal_alternate_path(&root);
        assert!(root.exists() && alternate.exists());
        verify_checkpoint_crash(&path, baseline_lsn, baseline_generation);
        assert!(!root.exists() && alternate.exists());
        cleanup(&path);
    }

    #[test]
    fn process_crash_checkpoint_reopens_after_old_generation_removal() {
        let (path, baseline_lsn, baseline_generation) =
            prepare_process_crash_baseline("process-crash-checkpoint-old-removed");
        spawn_crash_child(
            &path,
            "checkpoint",
            TestCrashPoint::CheckpointAfterOldGenerationRemoved,
        );
        let root = wal_path(&path);
        let alternate = wal_alternate_path(&root);
        assert!(!root.exists() && alternate.exists());
        verify_checkpoint_crash(&path, baseline_lsn, baseline_generation);
        cleanup(&path);
    }

    #[test]
    fn process_crash_partial_final_wal_record_is_truncated_once() {
        let (path, _, _) = prepare_process_crash_baseline("process-crash-partial-wal-tail");
        let wal = wal_path(&path);
        let valid_length = std::fs::metadata(&wal)
            .expect("read valid WAL metadata")
            .len();
        spawn_crash_child(
            &path,
            "partial-final-wal-record",
            TestCrashPoint::WalPartialFinalRecord,
        );
        let partial_length = std::fs::metadata(&wal)
            .expect("read partial WAL metadata")
            .len();
        assert!(partial_length > valid_length);

        assert_eq!(reopen_value(&path), "before");
        assert_eq!(
            std::fs::metadata(&wal)
                .expect("read truncated WAL metadata")
                .len(),
            valid_length
        );
        assert_eq!(reopen_value(&path), "before");
        assert_eq!(
            std::fs::metadata(&wal)
                .expect("read stable WAL metadata")
                .len(),
            valid_length
        );
        cleanup(&path);
    }
}
