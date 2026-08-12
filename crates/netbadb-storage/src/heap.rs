use std::cell::RefCell;
use std::path::Path;
use std::rc::Rc;

use netbadb_schema::TableDef;
use netbadb_types::{PageId, RowId, ScalarValue, SlotId};

use crate::recovery::RecoveryManager;
use crate::transaction::TransactionManager;
use crate::{
    BufferPool, CodecError, DEFAULT_BUFFER_POOL_SIZE, MetadataError, PAGE_HEADER_SIZE, PAGE_SIZE,
    Page, PageError, PageManager, PageType, SLOT_SIZE, StorageError, Transaction, TransactionError,
    WalManager, wal_path,
};

const HEADER_PAGE: PageId = PageId(0);
const FIRST_DATA_PAGE: PageId = PageId(1);
const HEADER_MAGIC: &[u8; 4] = b"NBD1";
const HEAP_FORMAT_VERSION: u16 = 1;
const HEAP_METADATA_OFFSET: usize = 16;
const HEAP_VERSION_OFFSET: usize = HEAP_METADATA_OFFSET + 4;
const HEAP_RESERVED_OFFSET: usize = HEAP_VERSION_OFFSET + 2;
const HEAP_TABLE_ID_OFFSET: usize = HEAP_RESERVED_OFFSET + 2;
const HEAP_COLUMN_COUNT_OFFSET: usize = HEAP_TABLE_ID_OFFSET + 8;

/// Heap storage over the buffer pool. Heap code interprets pages as heap pages;
/// the buffer pool and page manager remain generic over raw database pages.
#[derive(Debug)]
pub struct HeapStorage {
    buffer: BufferPool,
    table: TableDef,
    transactions: TransactionManager,
    #[cfg(test)]
    skip_drop_flush: bool,
}

impl HeapStorage {
    pub fn create(path: impl AsRef<Path>, table: TableDef) -> Result<Self, StorageError> {
        Self::create_with_buffer_pool_size(path, table, DEFAULT_BUFFER_POOL_SIZE)
    }

    pub fn create_with_buffer_pool_size(
        path: impl AsRef<Path>,
        table: TableDef,
        buffer_pool_size: usize,
    ) -> Result<Self, StorageError> {
        validate_table(&table)?;
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
            write_heap_metadata(header.page_mut().bytes_mut(), &table);
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
            #[cfg(test)]
            skip_drop_flush: false,
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
        validate_table(&table)?;
        BufferPool::validate_capacity(buffer_pool_size)?;
        let path = path.as_ref();
        let mut pages = PageManager::open(path)?;
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
        if pages.page_count() < 2 {
            return Err(crate::invalid_format("heap file has no data page"));
        }
        let wal = Rc::new(RefCell::new(wal_manager));
        let buffer = BufferPool::with_wal(pages, buffer_pool_size, Rc::clone(&wal))?;
        {
            let header = buffer.read_page(HEADER_PAGE)?;
            validate_heap_metadata(header.page().bytes(), &table)?;
        }
        let next_txn_id = wal.borrow().next_txn_id();
        let transactions = TransactionManager::new(wal, buffer.clone(), next_txn_id)?;
        Ok(Self {
            buffer,
            table,
            transactions,
            #[cfg(test)]
            skip_drop_flush: false,
        })
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

    pub fn begin_transaction(&mut self) -> Result<Transaction, StorageError> {
        self.transactions.begin()
    }

    pub fn insert_in(
        &mut self,
        transaction: &mut Transaction,
        values: &[ScalarValue],
    ) -> Result<RowId, StorageError> {
        if !transaction.belongs_to(self.transactions.wal()) {
            return Err(TransactionError::ForeignTransaction {
                txn_id: transaction.id(),
            }
            .into());
        }
        transaction.ensure_active()?;
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

        let last_page = self
            .buffer
            .page_count()
            .checked_sub(1)
            .ok_or_else(|| crate::invalid_format("heap file has no pages"))?;
        let existing = {
            let mut page = self.buffer.write_page(PageId(last_page))?;
            let before = page.page().clone();
            let mut after = before.clone();
            match after.insert_record(&payload) {
                Ok(slot) => {
                    transaction.log_page_update(&before, &mut after)?;
                    *page.page_mut() = after;
                    Ok(Some(slot))
                }
                Err(StorageError::Page(PageError::PageFull { .. })) => Ok(None),
                Err(error) => Err(error),
            }
        };
        let (page_id, slot) = match existing {
            Ok(Some(slot)) => (PageId(last_page), slot),
            Ok(None) => {
                let expected_page_id = PageId(self.buffer.page_count());
                let before = Page::zero(expected_page_id);
                let mut after = Page::new(expected_page_id, PageType::Heap);
                let slot = after.insert_record(&payload)?;
                let update_lsn = transaction.log_page_update(&before, &mut after)?;
                // Extending the database file writes a zero-filled page before
                // the buffer can install the logged after-image. Make the WAL
                // durable first so even allocation obeys write-ahead ordering.
                transaction.flush_through(update_lsn)?;
                let mut page = self.buffer.new_page()?;
                let page_id = page.page_id();
                if page_id != expected_page_id {
                    return Err(crate::invalid_format(format!(
                        "allocated page {}, expected {}",
                        page_id.0, expected_page_id.0
                    )));
                }
                *page.page_mut() = after;
                (page_id, slot)
            }
            Err(error) => return Err(error),
        };
        Ok(RowId {
            page: page_id,
            slot: slot.0,
        })
    }

    pub fn scan(&mut self) -> Result<Vec<(RowId, Vec<ScalarValue>)>, StorageError> {
        let mut rows = Vec::new();
        for page_number in FIRST_DATA_PAGE.0..self.buffer.page_count() {
            let page_id = PageId(page_number);
            let page = self.buffer.read_page(page_id)?;
            let header = page.page().header()?;
            if header.page_type != PageType::Heap {
                return Err(PageError::WrongPageType {
                    expected: PageType::Heap,
                    actual: header.page_type,
                }
                .into());
            }
            for slot_number in 0..header.slot_count {
                let slot = SlotId(slot_number);
                let values = decode_row(page.page().read_record(slot)?, &self.table)?;
                rows.push((
                    RowId {
                        page: page_id,
                        slot: slot.0,
                    },
                    values,
                ));
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
    fn simulate_crash(mut self) {
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

fn validate_table(table: &TableDef) -> Result<(), StorageError> {
    if table.columns.len() > u16::MAX as usize {
        return Err(crate::invalid_format("table has more than 65535 columns"));
    }
    Ok(())
}

fn write_heap_metadata(bytes: &mut [u8; PAGE_SIZE], table: &TableDef) {
    bytes[HEAP_METADATA_OFFSET..HEAP_METADATA_OFFSET + HEADER_MAGIC.len()]
        .copy_from_slice(HEADER_MAGIC);
    bytes[HEAP_VERSION_OFFSET..HEAP_VERSION_OFFSET + 2]
        .copy_from_slice(&HEAP_FORMAT_VERSION.to_le_bytes());
    bytes[HEAP_RESERVED_OFFSET..HEAP_RESERVED_OFFSET + 2].fill(0);
    bytes[HEAP_TABLE_ID_OFFSET..HEAP_TABLE_ID_OFFSET + 8]
        .copy_from_slice(&table.id.0.to_le_bytes());
    bytes[HEAP_COLUMN_COUNT_OFFSET..HEAP_COLUMN_COUNT_OFFSET + 2]
        .copy_from_slice(&(table.columns.len() as u16).to_le_bytes());
}

fn validate_heap_metadata(bytes: &[u8; PAGE_SIZE], table: &TableDef) -> Result<(), StorageError> {
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
        return Err(StorageError::SchemaMismatch {
            expected: format!("table id {}", table.id.0),
            actual: format!("table id {table_id}"),
        });
    }
    if column_count != table.columns.len() {
        return Err(StorageError::SchemaMismatch {
            expected: format!("{} columns", table.columns.len()),
            actual: format!("{column_count} columns"),
        });
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
    use super::HeapStorage;
    use crate::{
        BufferError, CheckpointError, PageManager, SlotId, StorageError, TransactionError,
        TransactionState, WAL_HEADER_SIZE, WAL_MAX_RECORD_SIZE, WalError, WalManager,
        WalRecordKind, wal_alternate_path, wal_path,
    };
    use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
    use netbadb_types::{ColumnId, PageId, PhysicalType, ScalarValue, TableId};

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
        let mut page = pages.read_page(PageId(1)).expect("read data page");
        let slot = page.slot(SlotId(0)).expect("read row slot");
        page.bytes_mut()[usize::from(slot.offset)] = 99;
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
        let mut page = pages.read_page(PageId(1)).expect("read data page");
        page.bytes_mut()[crate::PAGE_HEADER_SIZE + 2..crate::PAGE_HEADER_SIZE + 4]
            .copy_from_slice(&u16::MAX.to_le_bytes());
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
    fn implicit_insert_commits_wal_without_flushing_the_heap_page() {
        let path = test_path("heap-no-insert-flush");
        let mut storage = HeapStorage::create(&path, table()).expect("create heap");
        storage
            .insert(&[ScalarValue::Int64(1), ScalarValue::Text("Ada".into())])
            .expect("insert row");
        assert!(storage.durable_lsn().expect("durable LSN").is_some());

        let mut disk = PageManager::open(&path).expect("open page file");
        assert_eq!(
            disk.read_page(PageId(1))
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
        assert_eq!(storage.buffer.page_count(), 2);
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
        assert_eq!(storage.buffer.page_count(), 2);
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
}
