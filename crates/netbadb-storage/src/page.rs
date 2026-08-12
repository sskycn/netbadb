use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use netbadb_types::{PageId, SlotId};

use crate::{PageError, StorageError, invalid_format};

/// The single storage page size used by the database file.
pub const PAGE_SIZE: usize = 4 * 1024;
/// Magic for versioned database pages. The explicit version field remains
/// separate so a decoder never has to infer layout from a magic string.
pub const PAGE_MAGIC: &[u8; 4] = b"NBP1";
pub const PAGE_FORMAT_VERSION: u16 = 1;
pub const PAGE_HEADER_SIZE: usize = 16;
pub const SLOT_SIZE: usize = 4;

const FILE_MAGIC: &[u8; 4] = b"NBPG";
const PAGE_TYPE_OFFSET: usize = 6;
const RESERVED_OFFSET: usize = 7;
const SLOT_COUNT_OFFSET: usize = 8;
const FREE_START_OFFSET: usize = 10;
const FREE_END_OFFSET: usize = 12;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageType {
    Metadata,
    Heap,
}

impl PageType {
    const fn tag(self) -> u8 {
        match self {
            Self::Metadata => 1,
            Self::Heap => 2,
        }
    }

    fn from_tag(tag: u8) -> Result<Self, PageError> {
        match tag {
            1 => Ok(Self::Metadata),
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
    pub fn bytes_mut(&mut self) -> &mut [u8; PAGE_SIZE] {
        &mut self.bytes
    }

    pub fn header(&self) -> Result<PageHeader, StorageError> {
        if &self.bytes[0..4] != PAGE_MAGIC {
            return Err(PageError::InvalidMagic.into());
        }
        let version = self.read_u16(4);
        if version != PAGE_FORMAT_VERSION {
            return Err(PageError::UnsupportedVersion(version).into());
        }
        if self.bytes[RESERVED_OFFSET] != 0 {
            return Err(PageError::InvalidReservedByte(self.bytes[RESERVED_OFFSET]).into());
        }
        if let Some(&value) = self.bytes[14..PAGE_HEADER_SIZE]
            .iter()
            .find(|byte| **byte != 0)
        {
            return Err(PageError::InvalidReservedByte(value).into());
        }

        let page_type = PageType::from_tag(self.bytes[PAGE_TYPE_OFFSET])?;
        let slot_count = self.read_u16(SLOT_COUNT_OFFSET);
        let free_start = self.read_u16(FREE_START_OFFSET);
        let free_end = self.read_u16(FREE_END_OFFSET);
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
            let offset = usize::from(slot.offset);
            let end = offset.checked_add(usize::from(slot.length)).ok_or(
                PageError::RecordOutOfBounds {
                    slot: slot_id,
                    offset: slot.offset,
                    length: slot.length,
                },
            )?;
            if slot.length == 0 || offset >= PAGE_SIZE || end > PAGE_SIZE {
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
            for (other_index, &(other_start, other_end)) in ranges.iter().enumerate() {
                if offset < other_end && other_start < end {
                    return Err(PageError::OverlappingRecords {
                        first: SlotId(other_index as u16),
                        second: slot_id,
                    }
                    .into());
                }
            }
            ranges.push((offset, end));
        }

        Ok(PageHeader {
            page_type,
            slot_count,
            free_start,
            free_end,
        })
    }

    pub fn slot(&self, slot: SlotId) -> Result<Slot, StorageError> {
        let header = self.header()?;
        if slot.0 >= header.slot_count {
            return Err(PageError::InvalidSlot { slot }.into());
        }
        Ok(self.slot_at(slot))
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
        if record.is_empty() || record.len() > max_record_size || record.len() > u16::MAX as usize {
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
        Ok(slot)
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

    fn write_u16(&mut self, offset: usize, value: u16) {
        self.bytes[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
    }
}

/// Synchronous fixed-page I/O. This type owns the file but does not interpret
/// data pages; page type and slotted-page validation belong to `Page`.
#[derive(Debug)]
pub struct PageManager {
    file: File,
    page_count: u64,
}

impl PageManager {
    pub fn create(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(path)?;
        // NBPG is the legacy experimental container header. It is intentionally
        // kept unchanged; versioned page headers live in data pages.
        file.write_all(FILE_MAGIC)?;
        file.write_all(&[0; PAGE_SIZE - FILE_MAGIC.len()])?;
        file.sync_all()?;
        Ok(Self {
            file,
            page_count: 1,
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
        })
    }

    #[must_use]
    pub fn page_count(&self) -> u64 {
        self.page_count
    }

    pub fn allocate_page(&mut self) -> Result<Page, StorageError> {
        let id = PageId(self.page_count);
        let next_count = self
            .page_count
            .checked_add(1)
            .ok_or(StorageError::PageOffsetOverflow { page_id: id })?;
        let page = Page::zero(id);
        self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(page.bytes())?;
        self.page_count = next_count;
        Ok(page)
    }

    pub fn read_page(&mut self, id: PageId) -> Result<Page, StorageError> {
        self.ensure_page_exists(id)?;
        let mut bytes = [0; PAGE_SIZE];
        self.file.seek(SeekFrom::Start(self.page_offset(id)?))?;
        self.file.read_exact(&mut bytes)?;
        Ok(Page::from_bytes(id, bytes))
    }

    pub fn write_page(&mut self, page: &Page) -> Result<(), StorageError> {
        self.ensure_page_exists(page.id)?;
        self.file
            .seek(SeekFrom::Start(self.page_offset(page.id)?))?;
        self.file.write_all(page.bytes())?;
        Ok(())
    }

    pub fn sync(&mut self) -> Result<(), StorageError> {
        self.file.sync_all()?;
        Ok(())
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
    use netbadb_types::{PageId, SlotId};

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
    fn page_full_and_record_too_large_are_explicit_errors() {
        let mut page = Page::new(PageId(1), PageType::Heap);
        let capacity = PAGE_SIZE - super::PAGE_HEADER_SIZE - SLOT_SIZE;
        page.insert_record(&vec![7; capacity]).expect("record fits");
        assert!(matches!(
            page.insert_record(b"next"),
            Err(StorageError::Page(PageError::PageFull { .. }))
        ));

        let mut empty = Page::new(PageId(2), PageType::Heap);
        assert!(matches!(
            empty.insert_record(&vec![0; capacity + 1]),
            Err(StorageError::Page(PageError::RecordTooLarge { .. }))
        ));
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
        assert!(matches!(
            unknown_type.header(),
            Err(StorageError::Page(PageError::UnknownPageType(99)))
        ));

        let mut invalid_directory = Page::new(PageId(3), PageType::Heap);
        invalid_directory.bytes_mut()[10..12].copy_from_slice(&17_u16.to_le_bytes());
        assert!(matches!(
            invalid_directory.header(),
            Err(StorageError::Page(
                PageError::SlotDirectoryOutOfBounds { .. }
            ))
        ));

        let mut invalid_free_space = Page::new(PageId(4), PageType::Heap);
        invalid_free_space.bytes_mut()[10..12].copy_from_slice(&16_u16.to_le_bytes());
        invalid_free_space.bytes_mut()[12..14].copy_from_slice(&15_u16.to_le_bytes());
        assert!(matches!(
            invalid_free_space.header(),
            Err(StorageError::Page(PageError::InvalidFreeSpace { .. }))
        ));

        let mut invalid_slot = Page::new(PageId(5), PageType::Heap);
        invalid_slot.insert_record(b"safe").expect("insert record");
        invalid_slot.bytes_mut()[16..18].copy_from_slice(&1_u16.to_le_bytes());
        invalid_slot.bytes_mut()[18..20].copy_from_slice(&u16::MAX.to_le_bytes());
        assert!(matches!(
            invalid_slot.header(),
            Err(StorageError::Page(PageError::RecordOutOfBounds { .. }))
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
