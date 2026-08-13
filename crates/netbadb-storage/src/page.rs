use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use netbadb_types::{Lsn, PageId, SlotId};

use crate::{PageError, StorageError, invalid_format};

/// The single storage page size used by the database file.
pub const PAGE_SIZE: usize = 4 * 1024;
/// Magic for versioned database pages. The explicit version field remains
/// separate so a decoder never has to infer layout from a magic string.
pub const PAGE_MAGIC: &[u8; 4] = b"NBP1";
pub const PAGE_FORMAT_VERSION: u16 = 4;
pub const PAGE_HEADER_SIZE: usize = 28;
pub const SLOT_SIZE: usize = 4;

const FILE_MAGIC: &[u8; 4] = b"NBPG";
const PAGE_TYPE_OFFSET: usize = 6;
const RESERVED_OFFSET: usize = 7;
const SLOT_COUNT_OFFSET: usize = 8;
const FREE_START_OFFSET: usize = 10;
const FREE_END_OFFSET: usize = 12;
const PAGE_LSN_OFFSET: usize = 16;
const CHECKSUM_OFFSET: usize = 24;
const CHECKSUM_END: usize = CHECKSUM_OFFSET + 4;
const DELETED_SLOT_OFFSET: u16 = 0;
const DELETED_SLOT_LENGTH: u16 = u16::MAX;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageType {
    Heap,
}

impl PageType {
    const fn tag(self) -> u8 {
        match self {
            Self::Heap => 2,
        }
    }

    fn from_tag(tag: u8) -> Result<Self, PageError> {
        match tag {
            2 => Ok(Self::Heap),
            other => Err(PageError::UnknownPageType(other)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageHeader {
    pub page_type: PageType,
    pub slot_count: u16,
    pub free_start: u16,
    pub free_end: u16,
    /// The WAL record that produced the current persistent page image.
    pub page_lsn: Option<Lsn>,
}

impl PageHeader {
    #[must_use]
    pub fn free_space(self) -> usize {
        usize::from(self.free_end.saturating_sub(self.free_start))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Slot {
    pub offset: u16,
    pub length: u16,
}

/// A fixed-size raw page. `from_bytes` deliberately does not validate the
/// bytes; callers must use `header`, `read_record`, or `insert_record` before
/// interpreting a page as a particular page type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Page {
    pub id: PageId,
    bytes: [u8; PAGE_SIZE],
}

#[derive(Debug)]
pub(crate) enum ValidatedBeforeImage {
    Existing(Box<Page>),
    NewPage,
}

pub(crate) fn validate_before_image(
    page_id: PageId,
    bytes: &[u8; PAGE_SIZE],
) -> Result<ValidatedBeforeImage, StorageError> {
    if bytes.iter().all(|byte| *byte == 0) {
        return Ok(ValidatedBeforeImage::NewPage);
    }
    let page = Page::from_bytes(page_id, *bytes);
    page.header()?;
    Ok(ValidatedBeforeImage::Existing(Box::new(page)))
}

impl Page {
    #[must_use]
    pub fn zero(id: PageId) -> Self {
        Self {
            id,
            bytes: [0; PAGE_SIZE],
        }
    }

    #[must_use]
    pub fn new(id: PageId, page_type: PageType) -> Self {
        let mut page = Self::zero(id);
        page.bytes[0..4].copy_from_slice(PAGE_MAGIC);
        page.bytes[4..6].copy_from_slice(&PAGE_FORMAT_VERSION.to_le_bytes());
        page.bytes[PAGE_TYPE_OFFSET] = page_type.tag();
        page.write_u16(SLOT_COUNT_OFFSET, 0);
        page.write_u16(FREE_START_OFFSET, PAGE_HEADER_SIZE as u16);
        page.write_u16(FREE_END_OFFSET, PAGE_SIZE as u16);
        page.refresh_checksum();
        page
    }

    #[must_use]
    pub fn from_bytes(id: PageId, bytes: [u8; PAGE_SIZE]) -> Self {
        Self { id, bytes }
    }

    #[must_use]
    pub fn bytes(&self) -> &[u8; PAGE_SIZE] {
        &self.bytes
    }

    #[must_use]
    pub(crate) fn bytes_mut(&mut self) -> &mut [u8; PAGE_SIZE] {
        &mut self.bytes
    }

    pub fn page_lsn(&self) -> Result<Option<Lsn>, StorageError> {
        Ok(self.header()?.page_lsn)
    }

    pub(crate) fn set_page_lsn(&mut self, lsn: Lsn) {
        self.bytes[PAGE_LSN_OFFSET..PAGE_LSN_OFFSET + 8].copy_from_slice(&lsn.0.to_le_bytes());
        self.refresh_checksum();
    }

    pub fn header(&self) -> Result<PageHeader, StorageError> {
        if &self.bytes[0..4] != PAGE_MAGIC {
            return Err(PageError::InvalidMagic.into());
        }
        let version = self.read_u16(4);
        if version != PAGE_FORMAT_VERSION {
            return Err(PageError::UnsupportedVersion(version).into());
        }
        self.verify_checksum()?;
        if self.bytes[RESERVED_OFFSET] != 0 {
            return Err(PageError::InvalidReservedByte(self.bytes[RESERVED_OFFSET]).into());
        }
        if let Some(&value) = self.bytes[14..PAGE_LSN_OFFSET]
            .iter()
            .find(|byte| **byte != 0)
        {
            return Err(PageError::InvalidReservedByte(value).into());
        }

        let page_type = PageType::from_tag(self.bytes[PAGE_TYPE_OFFSET])?;
        let slot_count = self.read_u16(SLOT_COUNT_OFFSET);
        let free_start = self.read_u16(FREE_START_OFFSET);
        let free_end = self.read_u16(FREE_END_OFFSET);
        let raw_page_lsn = self.read_u64(PAGE_LSN_OFFSET);
        let page_lsn = (raw_page_lsn != 0).then_some(Lsn(raw_page_lsn));
        let max_slots = (PAGE_SIZE - PAGE_HEADER_SIZE) / SLOT_SIZE;
        if usize::from(slot_count) > max_slots {
            return Err(PageError::InvalidSlotCount(slot_count).into());
        }

        let directory_end = PAGE_HEADER_SIZE
            .checked_add(usize::from(slot_count).checked_mul(SLOT_SIZE).ok_or(
                PageError::SlotDirectoryOutOfBounds {
                    slot_count,
                    free_start,
                },
            )?)
            .ok_or(PageError::SlotDirectoryOutOfBounds {
                slot_count,
                free_start,
            })?;
        if directory_end != usize::from(free_start) {
            return Err(PageError::SlotDirectoryOutOfBounds {
                slot_count,
                free_start,
            }
            .into());
        }
        if usize::from(free_start) > usize::from(free_end) || usize::from(free_end) > PAGE_SIZE {
            return Err(PageError::InvalidFreeSpace {
                free_start,
                free_end,
            }
            .into());
        }

        let mut ranges = Vec::with_capacity(usize::from(slot_count));
        for index in 0..slot_count {
            let slot_id = SlotId(index);
            let slot = self.slot_at(slot_id);
            if is_deleted_slot(slot) {
                continue;
            }
            if slot.offset == DELETED_SLOT_OFFSET || slot.length == DELETED_SLOT_LENGTH {
                return Err(PageError::InvalidDeletedSlotEncoding {
                    slot: slot_id,
                    offset: slot.offset,
                    length: slot.length,
                }
                .into());
            }
            let offset = usize::from(slot.offset);
            let end = offset.checked_add(usize::from(slot.length)).ok_or(
                PageError::RecordOutOfBounds {
                    slot: slot_id,
                    offset: slot.offset,
                    length: slot.length,
                },
            )?;
            if offset > PAGE_SIZE || end > PAGE_SIZE {
                return Err(PageError::RecordOutOfBounds {
                    slot: slot_id,
                    offset: slot.offset,
                    length: slot.length,
                }
                .into());
            }
            if offset < usize::from(free_end) {
                return Err(PageError::RecordOverlapsFreeSpace {
                    slot: slot_id,
                    offset: slot.offset,
                    free_end,
                }
                .into());
            }
            for &(other_slot, other_start, other_end) in &ranges {
                if offset < other_end && other_start < end {
                    return Err(PageError::OverlappingRecords {
                        first: other_slot,
                        second: slot_id,
                    }
                    .into());
                }
            }
            ranges.push((slot_id, offset, end));
        }

        Ok(PageHeader {
            page_type,
            slot_count,
            free_start,
            free_end,
            page_lsn,
        })
    }

    pub fn slot(&self, slot: SlotId) -> Result<Slot, StorageError> {
        let header = self.header()?;
        if slot.0 >= header.slot_count {
            return Err(PageError::InvalidSlot { slot }.into());
        }
        let entry = self.slot_at(slot);
        if is_deleted_slot(entry) {
            return Err(PageError::SlotDeleted { slot }.into());
        }
        Ok(entry)
    }

    pub fn is_slot_deleted(&self, slot: SlotId) -> Result<bool, StorageError> {
        let header = self.header()?;
        if slot.0 >= header.slot_count {
            return Err(PageError::InvalidSlot { slot }.into());
        }
        Ok(is_deleted_slot(self.slot_at(slot)))
    }

    pub fn read_record(&self, slot: SlotId) -> Result<&[u8], StorageError> {
        let slot_entry = self.slot(slot)?;
        let start = usize::from(slot_entry.offset);
        let end = start.checked_add(usize::from(slot_entry.length)).ok_or(
            PageError::RecordOutOfBounds {
                slot,
                offset: slot_entry.offset,
                length: slot_entry.length,
            },
        )?;
        self.bytes
            .get(start..end)
            .ok_or(PageError::RecordOutOfBounds {
                slot,
                offset: slot_entry.offset,
                length: slot_entry.length,
            })
            .map_err(Into::into)
    }

    pub fn insert_record(&mut self, record: &[u8]) -> Result<SlotId, StorageError> {
        let header = self.header()?;
        if header.page_type != PageType::Heap {
            return Err(PageError::WrongPageType {
                expected: PageType::Heap,
                actual: header.page_type,
            }
            .into());
        }

        let max_record_size = PAGE_SIZE - PAGE_HEADER_SIZE - SLOT_SIZE;
        if record.len() > max_record_size || record.len() > u16::MAX as usize {
            return Err(PageError::RecordTooLarge {
                size: record.len(),
                capacity: max_record_size,
            }
            .into());
        }
        let required = SLOT_SIZE
            .checked_add(record.len())
            .ok_or(PageError::RecordTooLarge {
                size: record.len(),
                capacity: max_record_size,
            })?;
        let available = header.free_space();
        if required > available {
            return Err(PageError::PageFull {
                required,
                available,
            }
            .into());
        }
        let slot = SlotId(header.slot_count);
        let new_free_start = usize::from(header.free_start)
            .checked_add(SLOT_SIZE)
            .ok_or(PageError::PageFull {
                required,
                available,
            })?;
        let new_free_end = usize::from(header.free_end)
            .checked_sub(record.len())
            .ok_or(PageError::PageFull {
                required,
                available,
            })?;
        let record_start = new_free_end;
        let record_end =
            record_start
                .checked_add(record.len())
                .ok_or(PageError::RecordTooLarge {
                    size: record.len(),
                    capacity: max_record_size,
                })?;

        self.bytes[record_start..record_end].copy_from_slice(record);
        self.write_u16(usize::from(header.free_start), record_start as u16);
        self.write_u16(usize::from(header.free_start) + 2, record.len() as u16);
        self.write_u16(SLOT_COUNT_OFFSET, header.slot_count + 1);
        self.write_u16(FREE_START_OFFSET, new_free_start as u16);
        self.write_u16(FREE_END_OFFSET, new_free_end as u16);
        self.refresh_checksum();
        Ok(slot)
    }

    /// Marks a live slot deleted and compacts the remaining payloads without
    /// renumbering or reusing any slot.
    pub fn delete_record(&mut self, slot: SlotId) -> Result<(), StorageError> {
        let mut records = self.live_records()?;
        let target = records
            .get_mut(usize::from(slot.0))
            .ok_or(PageError::InvalidSlot { slot })?;
        if target.is_none() {
            return Err(PageError::SlotDeleted { slot }.into());
        }
        *target = None;
        self.rebuild_records(&records, None)
    }

    /// Replaces one live slot while preserving its SlotId. The page remains
    /// byte-for-byte unchanged when the replacement cannot fit.
    pub fn replace_record(&mut self, slot: SlotId, record: &[u8]) -> Result<(), StorageError> {
        let max_record_size = PAGE_SIZE - PAGE_HEADER_SIZE - SLOT_SIZE;
        if record.len() > max_record_size || record.len() > u16::MAX as usize {
            return Err(PageError::RecordTooLarge {
                size: record.len(),
                capacity: max_record_size,
            }
            .into());
        }
        let mut records = self.live_records()?;
        let target = records
            .get_mut(usize::from(slot.0))
            .ok_or(PageError::InvalidSlot { slot })?;
        if target.is_none() {
            return Err(PageError::SlotDeleted { slot }.into());
        }
        *target = Some(record.to_vec());
        self.rebuild_records(&records, Some((slot, record.len())))
    }

    fn live_records(&self) -> Result<Vec<Option<Vec<u8>>>, StorageError> {
        let header = self.header()?;
        (0..header.slot_count)
            .map(|index| {
                let slot = SlotId(index);
                if is_deleted_slot(self.slot_at(slot)) {
                    Ok(None)
                } else {
                    Ok(Some(self.read_record(slot)?.to_vec()))
                }
            })
            .collect()
    }

    fn rebuild_records(
        &mut self,
        records: &[Option<Vec<u8>>],
        replacement: Option<(SlotId, usize)>,
    ) -> Result<(), StorageError> {
        let header = self.header()?;
        let directory_end = PAGE_HEADER_SIZE
            .checked_add(
                records
                    .len()
                    .checked_mul(SLOT_SIZE)
                    .ok_or(PageError::InvalidSlotCount(header.slot_count))?,
            )
            .ok_or(PageError::InvalidSlotCount(header.slot_count))?;
        let payload_size = records.iter().try_fold(0_usize, |total, record| {
            total
                .checked_add(record.as_ref().map_or(0, Vec::len))
                .ok_or(PageError::RecordTooLarge {
                    size: usize::MAX,
                    capacity: PAGE_SIZE.saturating_sub(directory_end),
                })
        })?;
        let capacity = PAGE_SIZE.saturating_sub(directory_end);
        if payload_size > capacity {
            if let Some((slot, size)) = replacement {
                return Err(PageError::UpdateWouldOverflowPage {
                    slot,
                    size,
                    capacity: capacity.saturating_sub(payload_size.saturating_sub(size)),
                }
                .into());
            }
            return Err(PageError::PageFull {
                required: payload_size,
                available: capacity,
            }
            .into());
        }

        let mut rebuilt = Page::new(self.id, header.page_type);
        if let Some(page_lsn) = header.page_lsn {
            rebuilt.set_page_lsn(page_lsn);
        }
        rebuilt.write_u16(SLOT_COUNT_OFFSET, header.slot_count);
        rebuilt.write_u16(FREE_START_OFFSET, directory_end as u16);
        let mut free_end = PAGE_SIZE;
        for (index, record) in records.iter().enumerate() {
            let entry_offset = PAGE_HEADER_SIZE + index * SLOT_SIZE;
            match record {
                Some(record) => {
                    free_end = free_end
                        .checked_sub(record.len())
                        .ok_or(PageError::PageFull {
                            required: payload_size,
                            available: capacity,
                        })?;
                    let end = free_end + record.len();
                    rebuilt.bytes[free_end..end].copy_from_slice(record);
                    rebuilt.write_u16(entry_offset, free_end as u16);
                    rebuilt.write_u16(entry_offset + 2, record.len() as u16);
                }
                None => {
                    rebuilt.write_u16(entry_offset, DELETED_SLOT_OFFSET);
                    rebuilt.write_u16(entry_offset + 2, DELETED_SLOT_LENGTH);
                }
            }
        }
        rebuilt.write_u16(FREE_END_OFFSET, free_end as u16);
        rebuilt.refresh_checksum();
        rebuilt.header()?;
        *self = rebuilt;
        Ok(())
    }

    fn slot_at(&self, slot: SlotId) -> Slot {
        let offset = PAGE_HEADER_SIZE + usize::from(slot.0) * SLOT_SIZE;
        Slot {
            offset: self.read_u16(offset),
            length: self.read_u16(offset + 2),
        }
    }

    fn read_u16(&self, offset: usize) -> u16 {
        u16::from_le_bytes([self.bytes[offset], self.bytes[offset + 1]])
    }

    fn read_u64(&self, offset: usize) -> u64 {
        u64::from_le_bytes([
            self.bytes[offset],
            self.bytes[offset + 1],
            self.bytes[offset + 2],
            self.bytes[offset + 3],
            self.bytes[offset + 4],
            self.bytes[offset + 5],
            self.bytes[offset + 6],
            self.bytes[offset + 7],
        ])
    }

    fn write_u16(&mut self, offset: usize, value: u16) {
        self.bytes[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
    }

    fn read_u32(&self, offset: usize) -> u32 {
        u32::from_le_bytes([
            self.bytes[offset],
            self.bytes[offset + 1],
            self.bytes[offset + 2],
            self.bytes[offset + 3],
        ])
    }

    fn computed_checksum(&self) -> u32 {
        let mut checksum = crc32c::crc32c(&self.id.0.to_le_bytes());
        checksum = crc32c::crc32c_append(checksum, &self.bytes[..CHECKSUM_OFFSET]);
        checksum = crc32c::crc32c_append(checksum, &[0; CHECKSUM_END - CHECKSUM_OFFSET]);
        crc32c::crc32c_append(checksum, &self.bytes[CHECKSUM_END..])
    }

    fn verify_checksum(&self) -> Result<(), PageError> {
        let stored = self.read_u32(CHECKSUM_OFFSET);
        let computed = self.computed_checksum();
        if stored != computed {
            return Err(PageError::ChecksumMismatch { stored, computed });
        }
        Ok(())
    }

    pub(crate) fn refresh_checksum(&mut self) {
        self.bytes[CHECKSUM_OFFSET..CHECKSUM_END].fill(0);
        let checksum = self.computed_checksum();
        self.bytes[CHECKSUM_OFFSET..CHECKSUM_END].copy_from_slice(&checksum.to_le_bytes());
    }
}

const fn is_deleted_slot(slot: Slot) -> bool {
    slot.offset == DELETED_SLOT_OFFSET && slot.length == DELETED_SLOT_LENGTH
}

/// Synchronous fixed-page I/O. This type owns the file but does not interpret
/// data pages; page type and slotted-page validation belong to `Page`.
#[derive(Debug)]
pub struct PageManager {
    file: File,
    page_count: u64,
    #[cfg(test)]
    fail_next_write: bool,
    #[cfg(test)]
    fail_next_sync: bool,
    #[cfg(test)]
    fail_next_allocation_after: Option<usize>,
}

impl PageManager {
    pub fn create(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let path = path.as_ref().to_owned();
        let mut file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)?;
        // NBPG is the legacy experimental container header. It is intentionally
        // kept unchanged; versioned page headers live in data pages.
        let initialization = (|| -> std::io::Result<()> {
            file.write_all(FILE_MAGIC)?;
            file.write_all(&[0; PAGE_SIZE - FILE_MAGIC.len()])?;
            file.sync_all()
        })();
        if let Err(error) = initialization {
            drop(file);
            let _ = std::fs::remove_file(path);
            return Err(error.into());
        }
        Ok(Self {
            file,
            page_count: 1,
            #[cfg(test)]
            fail_next_write: false,
            #[cfg(test)]
            fail_next_sync: false,
            #[cfg(test)]
            fail_next_allocation_after: None,
        })
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;
        let length = file.metadata()?.len();
        let page_size = PAGE_SIZE as u64;
        if length < page_size || length % page_size != 0 {
            return Err(invalid_format("file size is not a whole number of pages"));
        }
        let mut magic = [0; FILE_MAGIC.len()];
        file.read_exact(&mut magic)?;
        if &magic != FILE_MAGIC {
            return Err(invalid_format("page file magic does not match"));
        }
        Ok(Self {
            file,
            page_count: length / page_size,
            #[cfg(test)]
            fail_next_write: false,
            #[cfg(test)]
            fail_next_sync: false,
            #[cfg(test)]
            fail_next_allocation_after: None,
        })
    }

    #[must_use]
    pub fn page_count(&self) -> u64 {
        self.page_count
    }

    pub(crate) fn allocate_page(&mut self) -> Result<Page, StorageError> {
        let id = PageId(self.page_count);
        let next_count = self
            .page_count
            .checked_add(1)
            .ok_or(StorageError::PageOffsetOverflow { page_id: id })?;
        let page = Page::zero(id);
        self.file.seek(SeekFrom::End(0))?;
        #[cfg(test)]
        if let Some(prefix_len) = self.fail_next_allocation_after.take() {
            let prefix_len = prefix_len.min(PAGE_SIZE);
            self.file.write_all(&page.bytes()[..prefix_len])?;
            return Err(std::io::Error::other("injected partial page allocation failure").into());
        }
        self.file.write_all(page.bytes())?;
        self.page_count = next_count;
        Ok(page)
    }

    pub(crate) fn remove_trailing_page(&mut self, id: PageId) -> Result<bool, StorageError> {
        if id.0 == self.page_count {
            // `allocate_page` may have extended the file partially before an
            // I/O error without advancing the logical page count. Restore the
            // exact logical boundary even when the page is already absent.
            self.file.set_len(self.page_offset(id)?)?;
            return Ok(false);
        }
        let expected_count =
            id.0.checked_add(1)
                .ok_or(StorageError::PageOffsetOverflow { page_id: id })?;
        if expected_count != self.page_count || id.0 == 0 {
            return Err(invalid_format(format!(
                "page {} cannot be removed from a {}-page file",
                id.0, self.page_count
            )));
        }
        self.file.set_len(self.page_offset(id)?)?;
        self.page_count = id.0;
        Ok(true)
    }

    pub fn read_page(&mut self, id: PageId) -> Result<Page, StorageError> {
        self.ensure_page_exists(id)?;
        let mut bytes = [0; PAGE_SIZE];
        self.file.seek(SeekFrom::Start(self.page_offset(id)?))?;
        self.file.read_exact(&mut bytes)?;
        Ok(Page::from_bytes(id, bytes))
    }

    pub(crate) fn write_page(&mut self, page: &Page) -> Result<(), StorageError> {
        self.ensure_page_exists(page.id)?;
        #[cfg(test)]
        if std::mem::take(&mut self.fail_next_write) {
            return Err(std::io::Error::other("injected page write failure").into());
        }
        self.file
            .seek(SeekFrom::Start(self.page_offset(page.id)?))?;
        self.file.write_all(page.bytes())?;
        Ok(())
    }

    pub fn sync(&mut self) -> Result<(), StorageError> {
        #[cfg(test)]
        if std::mem::take(&mut self.fail_next_sync) {
            return Err(std::io::Error::other("injected page sync failure").into());
        }
        self.file.sync_all()?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn inject_write_failure(&mut self) {
        self.fail_next_write = true;
    }

    #[cfg(test)]
    pub(crate) fn inject_sync_failure(&mut self) {
        self.fail_next_sync = true;
    }

    #[cfg(test)]
    pub(crate) fn inject_partial_allocation_failure(&mut self, after_bytes: usize) {
        self.fail_next_allocation_after = Some(after_bytes);
    }

    fn page_offset(&self, id: PageId) -> Result<u64, StorageError> {
        id.0.checked_mul(PAGE_SIZE as u64)
            .ok_or(StorageError::PageOffsetOverflow { page_id: id })
    }

    fn ensure_page_exists(&self, id: PageId) -> Result<(), StorageError> {
        if id.0 >= self.page_count {
            return Err(invalid_format(format!("page {} is outside file", id.0)));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Write;

    use super::{PAGE_SIZE, Page, PageError, PageManager, PageType, SLOT_SIZE};
    use crate::StorageError;
    use netbadb_types::{Lsn, PageId, SlotId};

    fn test_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("netbadb-{name}-{}", std::process::id()))
    }

    #[test]
    fn slotted_page_round_trip_uses_explicit_header_and_slots() {
        let mut page = Page::new(PageId(1), PageType::Heap);
        let first = page.insert_record(b"first").expect("insert first");
        let second = page.insert_record(b"second").expect("insert second");
        let header = page.header().expect("valid page header");

        assert_eq!(header.slot_count, 2);
        assert_eq!(header.page_lsn, None);
        assert_eq!(
            header.free_start,
            (super::PAGE_HEADER_SIZE + 2 * SLOT_SIZE) as u16
        );
        assert_eq!(page.read_record(first).expect("read first"), b"first");
        assert_eq!(page.read_record(second).expect("read second"), b"second");
        assert!(matches!(
            page.slot(SlotId(2)),
            Err(StorageError::Page(PageError::InvalidSlot {
                slot: SlotId(2)
            }))
        ));
    }

    #[test]
    fn page_lsn_round_trips_and_older_versions_are_rejected() {
        let mut page = Page::new(PageId(1), PageType::Heap);
        page.set_page_lsn(Lsn(1234));
        let decoded = Page::from_bytes(PageId(1), *page.bytes());
        assert_eq!(decoded.page_lsn().expect("decode pageLSN"), Some(Lsn(1234)));

        let mut version_one = Page::new(PageId(2), PageType::Heap);
        version_one.bytes_mut()[4..6].copy_from_slice(&1_u16.to_le_bytes());
        assert!(matches!(
            version_one.header(),
            Err(StorageError::Page(PageError::UnsupportedVersion(1)))
        ));
        let mut version_two = Page::new(PageId(3), PageType::Heap);
        version_two.bytes_mut()[4..6].copy_from_slice(&2_u16.to_le_bytes());
        assert!(matches!(
            version_two.header(),
            Err(StorageError::Page(PageError::UnsupportedVersion(2)))
        ));
        let mut version_three = Page::new(PageId(4), PageType::Heap);
        version_three.bytes_mut()[4..6].copy_from_slice(&3_u16.to_le_bytes());
        assert!(matches!(
            version_three.header(),
            Err(StorageError::Page(PageError::UnsupportedVersion(3)))
        ));
    }

    #[test]
    fn page_v4_checksum_has_a_stable_golden_value_and_binds_page_id() {
        let mut page = Page::new(PageId(7), PageType::Heap);
        page.insert_record(b"checksum-golden")
            .expect("insert golden payload");
        page.set_page_lsn(Lsn(123));
        let checksum = u32::from_le_bytes(
            page.bytes()[super::CHECKSUM_OFFSET..super::CHECKSUM_END]
                .try_into()
                .expect("checksum field"),
        );
        assert_eq!(checksum, 0x1cb3_695a);
        page.header().expect("new page checksum is valid");

        let wrong_id = Page::from_bytes(PageId(8), *page.bytes());
        assert!(matches!(
            wrong_id.header(),
            Err(StorageError::Page(PageError::ChecksumMismatch { .. }))
        ));
    }

    #[test]
    fn checksum_detects_payload_header_and_checksum_corruption() {
        let mut page = Page::new(PageId(1), PageType::Heap);
        let slot = page.insert_record(b"checksum payload").expect("insert");
        let payload_offset = usize::from(page.slot(slot).expect("slot").offset);

        for offset in [
            payload_offset,
            100,
            super::SLOT_COUNT_OFFSET,
            super::FREE_START_OFFSET,
            super::FREE_END_OFFSET,
            super::PAGE_LSN_OFFSET,
            super::CHECKSUM_OFFSET,
        ] {
            let mut corrupted = page.clone();
            corrupted.bytes_mut()[offset] ^= 0x80;
            assert!(matches!(
                corrupted.header(),
                Err(StorageError::Page(PageError::ChecksumMismatch { .. }))
            ));
        }
    }

    #[test]
    fn zero_page_is_only_an_allocation_sentinel() {
        let zero = Page::zero(PageId(9));
        assert!(zero.bytes().iter().all(|byte| *byte == 0));
        assert!(matches!(
            zero.header(),
            Err(StorageError::Page(PageError::InvalidMagic))
        ));
        assert!(matches!(
            super::validate_before_image(PageId(9), zero.bytes()),
            Ok(super::ValidatedBeforeImage::NewPage)
        ));
    }

    #[test]
    fn page_lsn_persists_through_page_manager_io() {
        let path = test_path("page-lsn");
        let mut manager = PageManager::create(&path).expect("create page file");
        let allocated = manager.allocate_page().expect("allocate page");
        let mut page = Page::new(allocated.id, PageType::Heap);
        page.set_page_lsn(Lsn(4321));
        manager.write_page(&page).expect("write page");
        manager.sync().expect("sync page");
        drop(manager);

        let mut reopened = PageManager::open(&path).expect("reopen page file");
        assert_eq!(
            reopened
                .read_page(PageId(1))
                .expect("read page")
                .page_lsn()
                .expect("decode pageLSN"),
            Some(Lsn(4321))
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn zero_length_record_has_a_real_slot() {
        let mut page = Page::new(PageId(1), PageType::Heap);
        let empty = page.insert_record(&[]).expect("insert empty record");
        let non_empty = page.insert_record(b"value").expect("insert value");
        let header = page.header().expect("valid page header");

        assert_eq!(header.slot_count, 2);
        assert_eq!(page.read_record(empty).expect("read empty record"), b"");
        assert_eq!(
            page.read_record(non_empty).expect("read non-empty record"),
            b"value"
        );
        page.delete_record(empty).expect("delete empty record");
        assert!(matches!(
            page.read_record(empty),
            Err(StorageError::Page(PageError::SlotDeleted { slot })) if slot == empty
        ));
        assert_eq!(
            page.read_record(non_empty).expect("read stable record"),
            b"value"
        );
    }

    #[test]
    fn delete_compacts_payloads_without_renumbering_slots() {
        let mut page = Page::new(PageId(1), PageType::Heap);
        let first = page.insert_record(b"first").expect("insert first");
        let middle = page.insert_record(b"middle").expect("insert middle");
        let third = page.insert_record(b"third").expect("insert third");

        page.delete_record(middle).expect("delete middle");
        assert_eq!(page.read_record(first).expect("first remains"), b"first");
        assert_eq!(page.read_record(third).expect("third remains"), b"third");
        assert!(page.is_slot_deleted(middle).expect("deleted state"));
        assert_eq!(page.header().expect("valid compacted page").slot_count, 3);
        assert!(matches!(
            page.delete_record(middle),
            Err(StorageError::Page(PageError::SlotDeleted { .. }))
        ));
    }

    #[test]
    fn replace_record_grows_and_shrinks_without_changing_slot() {
        let mut page = Page::new(PageId(1), PageType::Heap);
        let first = page
            .insert_record(b"a long original")
            .expect("insert first");
        let second = page.insert_record(b"second").expect("insert second");
        page.replace_record(first, b"x").expect("shrink");
        assert_eq!(page.read_record(first).expect("read shrink"), b"x");
        page.replace_record(first, b"a substantially longer replacement")
            .expect("grow");
        assert_eq!(
            page.read_record(first).expect("read grow"),
            b"a substantially longer replacement"
        );
        assert_eq!(page.read_record(second).expect("stable second"), b"second");
    }

    #[test]
    fn overflowing_replace_leaves_page_unchanged() {
        let mut page = Page::new(PageId(1), PageType::Heap);
        let first = page.insert_record(b"first").expect("insert first");
        page.insert_record(&[7; 3_900]).expect("fill page");
        let before = page.clone();
        assert!(matches!(
            page.replace_record(first, &[8; 200]),
            Err(StorageError::Page(
                PageError::UpdateWouldOverflowPage { .. }
            ))
        ));
        assert_eq!(page, before);
    }

    #[test]
    fn page_full_and_record_too_large_are_explicit_errors() {
        let mut page = Page::new(PageId(1), PageType::Heap);
        let capacity = PAGE_SIZE - super::PAGE_HEADER_SIZE - SLOT_SIZE;
        page.insert_record(&vec![7; capacity]).expect("record fits");
        let full = page.clone();
        assert!(matches!(
            page.insert_record(b"next"),
            Err(StorageError::Page(PageError::PageFull { .. }))
        ));
        assert_eq!(page, full);

        let mut empty = Page::new(PageId(2), PageType::Heap);
        let before = empty.clone();
        assert!(matches!(
            empty.insert_record(&vec![0; capacity + 1]),
            Err(StorageError::Page(PageError::RecordTooLarge { .. }))
        ));
        assert_eq!(empty, before);
    }

    #[test]
    fn corrupt_page_headers_and_slots_return_errors() {
        let mut unsupported_version = Page::new(PageId(1), PageType::Heap);
        unsupported_version.bytes_mut()[4..6].copy_from_slice(&99_u16.to_le_bytes());
        assert!(matches!(
            unsupported_version.header(),
            Err(StorageError::Page(PageError::UnsupportedVersion(99)))
        ));

        let mut unknown_type = Page::new(PageId(2), PageType::Heap);
        unknown_type.bytes_mut()[6] = 99;
        unknown_type.refresh_checksum();
        assert!(matches!(
            unknown_type.header(),
            Err(StorageError::Page(PageError::UnknownPageType(99)))
        ));

        let mut invalid_reserved = Page::new(PageId(20), PageType::Heap);
        invalid_reserved.bytes_mut()[7] = 1;
        invalid_reserved.refresh_checksum();
        assert!(matches!(
            invalid_reserved.header(),
            Err(StorageError::Page(PageError::InvalidReservedByte(1)))
        ));

        let mut invalid_slot_count = Page::new(PageId(21), PageType::Heap);
        invalid_slot_count.bytes_mut()[8..10].copy_from_slice(&u16::MAX.to_le_bytes());
        invalid_slot_count.refresh_checksum();
        assert!(matches!(
            invalid_slot_count.header(),
            Err(StorageError::Page(PageError::InvalidSlotCount(u16::MAX)))
        ));

        let mut invalid_directory = Page::new(PageId(3), PageType::Heap);
        invalid_directory.bytes_mut()[10..12].copy_from_slice(&17_u16.to_le_bytes());
        invalid_directory.refresh_checksum();
        assert!(matches!(
            invalid_directory.header(),
            Err(StorageError::Page(
                PageError::SlotDirectoryOutOfBounds { .. }
            ))
        ));

        let mut invalid_free_space = Page::new(PageId(4), PageType::Heap);
        invalid_free_space.bytes_mut()[10..12]
            .copy_from_slice(&(super::PAGE_HEADER_SIZE as u16).to_le_bytes());
        invalid_free_space.bytes_mut()[12..14]
            .copy_from_slice(&((super::PAGE_HEADER_SIZE - 1) as u16).to_le_bytes());
        invalid_free_space.refresh_checksum();
        assert!(matches!(
            invalid_free_space.header(),
            Err(StorageError::Page(PageError::InvalidFreeSpace { .. }))
        ));

        let mut invalid_slot = Page::new(PageId(5), PageType::Heap);
        invalid_slot.insert_record(b"safe").expect("insert record");
        invalid_slot.bytes_mut()[super::PAGE_HEADER_SIZE..super::PAGE_HEADER_SIZE + 2]
            .copy_from_slice(&1_u16.to_le_bytes());
        invalid_slot.bytes_mut()[super::PAGE_HEADER_SIZE + 2..super::PAGE_HEADER_SIZE + 4]
            .copy_from_slice(&(u16::MAX - 1).to_le_bytes());
        invalid_slot.refresh_checksum();
        assert!(matches!(
            invalid_slot.header(),
            Err(StorageError::Page(PageError::RecordOutOfBounds { .. }))
        ));

        let mut invalid_deleted = Page::new(PageId(6), PageType::Heap);
        invalid_deleted
            .insert_record(b"safe")
            .expect("insert record");
        invalid_deleted.bytes_mut()[super::PAGE_HEADER_SIZE..super::PAGE_HEADER_SIZE + 2]
            .copy_from_slice(&0_u16.to_le_bytes());
        invalid_deleted.refresh_checksum();
        assert!(matches!(
            invalid_deleted.header(),
            Err(StorageError::Page(
                PageError::InvalidDeletedSlotEncoding { .. }
            ))
        ));

        let mut overlaps_free = Page::new(PageId(22), PageType::Heap);
        let slot = overlaps_free
            .insert_record(b"record")
            .expect("insert record");
        let record_offset = overlaps_free.slot(slot).expect("slot").offset;
        overlaps_free.bytes_mut()[12..14].copy_from_slice(&(record_offset + 1).to_le_bytes());
        overlaps_free.refresh_checksum();
        assert!(matches!(
            overlaps_free.header(),
            Err(StorageError::Page(
                PageError::RecordOverlapsFreeSpace { .. }
            ))
        ));

        let mut overlapping_records = Page::new(PageId(23), PageType::Heap);
        let first = overlapping_records
            .insert_record(b"first")
            .expect("insert first");
        let second = overlapping_records
            .insert_record(b"other")
            .expect("insert second");
        let first_offset = overlapping_records.slot(first).expect("first slot").offset;
        let second_entry = super::PAGE_HEADER_SIZE + usize::from(second.0) * super::SLOT_SIZE;
        overlapping_records.bytes_mut()[second_entry..second_entry + 2]
            .copy_from_slice(&first_offset.to_le_bytes());
        overlapping_records.refresh_checksum();
        assert!(matches!(
            overlapping_records.header(),
            Err(StorageError::Page(PageError::OverlappingRecords {
                first: SlotId(0),
                second: SlotId(1),
            }))
        ));
    }

    #[test]
    fn page_bytes_round_trip_through_file() {
        let path = test_path("page");
        let mut manager = PageManager::create(&path).expect("create page file");
        let mut page = manager.allocate_page().expect("allocate page");
        page.bytes_mut()[12] = 42;
        manager.write_page(&page).expect("write page");
        manager.sync().expect("sync page");
        drop(manager);

        let mut reopened = PageManager::open(&path).expect("reopen page file");
        let loaded = reopened.read_page(PageId(1)).expect("read page");
        assert_eq!(loaded.bytes()[12], 42);
        assert_eq!(loaded.bytes().len(), PAGE_SIZE);
        assert_eq!(Page::zero(PageId(9)).bytes().iter().sum::<u8>(), 0);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn rejects_a_truncated_page_file() {
        let path = test_path("truncated-page");
        let mut file = File::create(&path).expect("create truncated file");
        file.write_all(b"NBPG").expect("write truncated header");
        drop(file);

        assert!(PageManager::open(&path).is_err());
        let _ = std::fs::remove_file(path);
    }
}
