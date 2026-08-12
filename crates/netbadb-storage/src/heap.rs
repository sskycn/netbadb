use std::path::Path;

use netbadb_schema::TableDef;
use netbadb_types::{PageId, RowId, ScalarValue};

use crate::{PAGE_SIZE, Page, PageManager, StorageError, invalid_format};

const HEADER_PAGE: PageId = PageId(0);
const FIRST_DATA_PAGE: PageId = PageId(1);
const HEADER_MAGIC: &[u8; 4] = b"NBD1";
const DATA_MAGIC: &[u8; 4] = b"HEAP";
const HEAP_HEADER_OFFSET: usize = 16;
const DATA_HEADER_SIZE: usize = 12;

#[derive(Debug)]
pub struct HeapStorage {
    pages: PageManager,
    table: TableDef,
}

impl HeapStorage {
    pub fn create(path: impl AsRef<Path>, table: TableDef) -> Result<Self, StorageError> {
        if table.columns.len() > u16::MAX as usize {
            return Err(invalid_format("table has more than 65535 columns"));
        }
        let mut pages = PageManager::create(path)?;
        let mut header = pages.read_page(HEADER_PAGE)?;
        let bytes = header.bytes_mut();
        bytes[HEAP_HEADER_OFFSET..HEAP_HEADER_OFFSET + HEADER_MAGIC.len()]
            .copy_from_slice(HEADER_MAGIC);
        bytes[HEAP_HEADER_OFFSET + 4..HEAP_HEADER_OFFSET + 12]
            .copy_from_slice(&table.id.0.to_le_bytes());
        bytes[HEAP_HEADER_OFFSET + 12..HEAP_HEADER_OFFSET + 14]
            .copy_from_slice(&(table.columns.len() as u16).to_le_bytes());
        pages.write_page(&header)?;
        let data_page = pages.allocate_page()?;
        let data_page = initialize_data_page(data_page);
        pages.write_page(&data_page)?;
        pages.sync()?;
        Ok(Self { pages, table })
    }

    pub fn open(path: impl AsRef<Path>, table: TableDef) -> Result<Self, StorageError> {
        let mut pages = PageManager::open(path)?;
        if pages.page_count() < 2 {
            return Err(invalid_format("heap file has no data page"));
        }
        let header = pages.read_page(HEADER_PAGE)?;
        let bytes = header.bytes();
        if &bytes[HEAP_HEADER_OFFSET..HEAP_HEADER_OFFSET + HEADER_MAGIC.len()] != HEADER_MAGIC {
            return Err(invalid_format("heap header magic does not match"));
        }
        let table_id = read_u64(bytes, HEAP_HEADER_OFFSET + 4)?;
        let column_count = read_u16(bytes, HEAP_HEADER_OFFSET + 12)? as usize;
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
        Ok(Self { pages, table })
    }

    pub fn insert(&mut self, values: &[ScalarValue]) -> Result<RowId, StorageError> {
        self.validate_row(values)?;
        let payload = encode_row(values)?;
        let needed = 4 + payload.len();
        let capacity = PAGE_SIZE - DATA_HEADER_SIZE;
        if needed > capacity {
            return Err(StorageError::RowTooLarge {
                size: needed,
                capacity,
            });
        }

        let mut page_id = PageId(self.pages.page_count() - 1);
        let mut page = self.pages.read_page(page_id)?;
        let mut used = read_u32(page.bytes(), 8)? as usize;
        let mut row_count = read_u16(page.bytes(), 4)?;
        if &page.bytes()[0..4] != DATA_MAGIC || !(DATA_HEADER_SIZE..=PAGE_SIZE).contains(&used) {
            return Err(invalid_format(format!(
                "data page {} is corrupt",
                page_id.0
            )));
        }
        let fits_on_page = used
            .checked_add(needed)
            .map(|end| end <= PAGE_SIZE)
            .unwrap_or(false);
        if row_count == u16::MAX || !fits_on_page {
            page = initialize_data_page(self.pages.allocate_page()?);
            page_id = page.id;
            used = DATA_HEADER_SIZE;
            row_count = 0;
        }

        let slot = row_count;
        let bytes = page.bytes_mut();
        bytes[used..used + 4].copy_from_slice(&(payload.len() as u32).to_le_bytes());
        bytes[used + 4..used + needed].copy_from_slice(&payload);
        bytes[8..12].copy_from_slice(&((used + needed) as u32).to_le_bytes());
        bytes[4..6].copy_from_slice(&(row_count + 1).to_le_bytes());
        self.pages.write_page(&page)?;
        self.pages.sync()?;
        Ok(RowId {
            page: page_id,
            slot,
        })
    }

    pub fn scan(&mut self) -> Result<Vec<(RowId, Vec<ScalarValue>)>, StorageError> {
        let mut rows = Vec::new();
        for page_number in FIRST_DATA_PAGE.0..self.pages.page_count() {
            let page_id = PageId(page_number);
            let page = self.pages.read_page(page_id)?;
            let bytes = page.bytes();
            if &bytes[0..4] != DATA_MAGIC {
                return Err(invalid_format(format!(
                    "data page {} has invalid magic",
                    page_id.0
                )));
            }
            let row_count = read_u16(bytes, 4)?;
            let used = read_u32(bytes, 8)? as usize;
            if !(DATA_HEADER_SIZE..=PAGE_SIZE).contains(&used) {
                return Err(invalid_format(format!(
                    "data page {} has invalid used length",
                    page_id.0
                )));
            }
            let mut offset = DATA_HEADER_SIZE;
            for slot in 0..row_count {
                let length_end = offset
                    .checked_add(4)
                    .ok_or_else(|| invalid_format("row length offset overflows"))?;
                if length_end > used {
                    return Err(invalid_format("row length exceeds page used length"));
                }
                let row_length = read_u32(bytes, offset)? as usize;
                offset = length_end;
                let row_end = offset
                    .checked_add(row_length)
                    .ok_or_else(|| invalid_format("row payload offset overflows"))?;
                if row_end > used {
                    return Err(invalid_format("row payload exceeds page used length"));
                }
                let values = decode_row(&bytes[offset..row_end], &self.table)?;
                rows.push((
                    RowId {
                        page: page_id,
                        slot,
                    },
                    values,
                ));
                offset = row_end;
            }
            if offset != used {
                return Err(invalid_format("data page contains trailing bytes"));
            }
        }
        Ok(rows)
    }

    #[must_use]
    pub fn table(&self) -> &TableDef {
        &self.table
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

fn initialize_data_page(mut page: Page) -> Page {
    let bytes = page.bytes_mut();
    bytes[0..4].copy_from_slice(DATA_MAGIC);
    bytes[8..12].copy_from_slice(&(DATA_HEADER_SIZE as u32).to_le_bytes());
    page
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
                    capacity: PAGE_SIZE - DATA_HEADER_SIZE,
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
        return Err(invalid_format("row contains extra values"));
    }
    Ok(values)
}

fn decode_value(payload: &[u8], offset: &mut usize) -> Result<ScalarValue, StorageError> {
    let tag = *payload
        .get(*offset)
        .ok_or_else(|| invalid_format("missing scalar tag"))?;
    *offset += 1;
    match tag {
        0 => Ok(ScalarValue::Bool(read_byte(payload, offset)? != 0)),
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
                .ok_or_else(|| invalid_format("text length overflows"))?;
            let text = std::str::from_utf8(
                payload
                    .get(*offset..end)
                    .ok_or_else(|| invalid_format("text exceeds row"))?,
            )
            .map_err(|_| invalid_format("text is not valid UTF-8"))?
            .to_owned();
            *offset = end;
            Ok(ScalarValue::Text(text))
        }
        4 => Ok(ScalarValue::Null),
        _ => Err(invalid_format("unknown scalar tag")),
    }
}

fn read_byte(bytes: &[u8], offset: &mut usize) -> Result<u8, StorageError> {
    let byte = *bytes
        .get(*offset)
        .ok_or_else(|| invalid_format("scalar exceeds row"))?;
    *offset += 1;
    Ok(byte)
}

fn read_array<const N: usize>(bytes: &[u8], offset: &mut usize) -> Result<[u8; N], StorageError> {
    let end = (*offset)
        .checked_add(N)
        .ok_or_else(|| invalid_format("scalar length overflows"))?;
    let source = bytes
        .get(*offset..end)
        .ok_or_else(|| invalid_format("scalar exceeds row"))?;
    let mut output = [0; N];
    output.copy_from_slice(source);
    *offset = end;
    Ok(output)
}

fn read_u16(bytes: &[u8], offset: usize) -> Result<u16, StorageError> {
    Ok(u16::from_le_bytes(read_array_at(bytes, offset)?))
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32, StorageError> {
    Ok(u32::from_le_bytes(read_array_at(bytes, offset)?))
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64, StorageError> {
    Ok(u64::from_le_bytes(read_array_at(bytes, offset)?))
}

fn read_array_at<const N: usize>(bytes: &[u8], offset: usize) -> Result<[u8; N], StorageError> {
    let end = offset
        .checked_add(N)
        .ok_or_else(|| invalid_format("header length overflows"))?;
    let source = bytes
        .get(offset..end)
        .ok_or_else(|| invalid_format("header is truncated"))?;
    let mut output = [0; N];
    output.copy_from_slice(source);
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::HeapStorage;
    use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
    use netbadb_types::{ColumnId, PhysicalType, ScalarValue, TableId};

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

    #[test]
    fn insert_write_read_decode_round_trip() {
        let path = std::env::temp_dir().join(format!("netbadb-heap-{}", std::process::id()));
        let mut storage = HeapStorage::create(&path, table()).expect("create heap");
        let row_id = storage
            .insert(&[ScalarValue::Int64(7), ScalarValue::Text("Ada".into())])
            .expect("insert");
        assert_eq!(row_id.slot, 0);
        drop(storage);

        let mut reopened = HeapStorage::open(&path, table()).expect("reopen heap");
        let rows = reopened.scan().expect("scan");
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].1,
            vec![ScalarValue::Int64(7), ScalarValue::Text("Ada".into())]
        );
        let _ = std::fs::remove_file(path);
    }
}
