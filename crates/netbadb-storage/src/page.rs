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
pub const PAGE_FORMAT_VERSION: u16 = 5;
pub const PAGE_HEADER_SIZE: usize = 28;
/// Persistent slot layout: offset (u16 LE), length (u16 LE), generation (u32 LE).
pub const SLOT_SIZE: usize = 8;

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
    BTreeMeta,
    BTreeInternal,
    BTreeLeaf,
    IndexCatalog,
}

impl PageType {
    const fn tag(self) -> u8 {
        match self {
            Self::Heap => 2,
            Self::BTreeMeta => 3,
            Self::BTreeInternal => 4,
            Self::BTreeLeaf => 5,
            Self::IndexCatalog => 6,
        }
    }

    fn from_tag(tag: u8) -> Result<Self, PageError> {
        match tag {
            2 => Ok(Self::Heap),
            3 => Ok(Self::BTreeMeta),
            4 => Ok(Self::BTreeInternal),
            5 => Ok(Self::BTreeLeaf),
            6 => Ok(Self::IndexCatalog),
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
    pub generation: u32,
}

/// The page-local part of a versioned row locator returned by insertion.
///
/// Higher layers must retain both fields. Page mutation and inspection APIs
/// accept only an explicit [`SlotId`], so discarding the generation cannot
/// happen through an implicit conversion; generation validation belongs at
/// the heap [`netbadb_types::RowId`] boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlotRef {
    pub slot: SlotId,
    pub generation: u32,
}

/// Persisted state of an allocated slot. Deleted slots retain their generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotState {
    Live(Slot),
    Deleted { generation: u32 },
}

#[derive(Debug)]
struct RebuildSlot {
    generation: u32,
    payload: Option<Vec<u8>>,
}

/// A fixed-size raw page. `from_bytes` deliberately does not validate the
/// bytes; callers must use `header`, `read_record`, or `insert_record` before
/// interpreting a page as a particular page type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Page {
    pub id: PageId,
    bytes: [u8; PAGE_SIZE],
}

/// A short-lived immutable view whose page-wide structure and checksum were
/// validated by [`Page::header`]. The borrow prevents mutation from invalidating
/// the cached header while the view is live; this is not a persistent trust bit,
/// an on-disk marker, or validation cached inside [`Page`].
#[derive(Debug)]
pub(crate) struct ValidatedPage<'a> {
    page: &'a Page,
    header: PageHeader,
}

impl ValidatedPage<'_> {
    #[must_use]
    pub(crate) const fn header(&self) -> PageHeader {
        self.header
    }

    /// Returns one live slot and its payload after the page-wide validation
    /// performed when this immutable view was created. Deleted slots are
    /// represented as `None`; the payload remains borrowed from the page.
    pub(crate) fn live_record(&self, slot: SlotId) -> Result<Option<(Slot, &[u8])>, StorageError> {
        if slot.0 >= self.header.slot_count {
            return Err(PageError::InvalidSlot { slot }.into());
        }
        let entry = self.page.slot_at(slot);
        if is_deleted_slot(entry) {
            return Ok(None);
        }
        let start = usize::from(entry.offset);
        let end =
            start
                .checked_add(usize::from(entry.length))
                .ok_or(PageError::RecordOutOfBounds {
                    slot,
                    offset: entry.offset,
                    length: entry.length,
                })?;
        let payload = self
            .page
            .bytes
            .get(start..end)
            .ok_or(PageError::RecordOutOfBounds {
                slot,
                offset: entry.offset,
                length: entry.length,
            })?;
        Ok(Some((entry, payload)))
    }
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
            if slot.generation == 0 {
                return Err(PageError::InvalidSlotGeneration {
                    slot: slot_id,
                    generation: slot.generation,
                }
                .into());
            }
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

    /// Performs the authoritative full page validation once and returns an
    /// immutable view that can reuse the resulting structural proof for the
    /// lifetime of its borrow.
    pub(crate) fn validated(&self) -> Result<ValidatedPage<'_>, StorageError> {
        Ok(ValidatedPage {
            page: self,
            header: self.header()?,
        })
    }

    pub fn slot(&self, slot: SlotId) -> Result<Slot, StorageError> {
        match self.slot_state(slot)? {
            SlotState::Live(entry) => Ok(entry),
            SlotState::Deleted { .. } => Err(PageError::SlotDeleted { slot }.into()),
        }
    }

    pub fn slot_state(&self, slot: SlotId) -> Result<SlotState, StorageError> {
        let header = self.header()?;
        if slot.0 >= header.slot_count {
            return Err(PageError::InvalidSlot { slot }.into());
        }
        let entry = self.slot_at(slot);
        if is_deleted_slot(entry) {
            Ok(SlotState::Deleted {
                generation: entry.generation,
            })
        } else {
            Ok(SlotState::Live(entry))
        }
    }

    pub fn is_slot_deleted(&self, slot: SlotId) -> Result<bool, StorageError> {
        Ok(matches!(self.slot_state(slot)?, SlotState::Deleted { .. }))
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

    pub fn insert_record(&mut self, record: &[u8]) -> Result<SlotRef, StorageError> {
        let header = self.expect_page_type(PageType::Heap)?;

        let max_record_size = PAGE_SIZE - PAGE_HEADER_SIZE - SLOT_SIZE;
        if record.len() > max_record_size || record.len() > u16::MAX as usize {
            return Err(PageError::RecordTooLarge {
                size: record.len(),
                capacity: max_record_size,
            }
            .into());
        }
        let mut records = self.rebuild_slots()?;
        let reusable = records
            .iter()
            .position(|slot| slot.payload.is_none() && slot.generation < u32::MAX);
        let required = if reusable.is_some() {
            record.len()
        } else {
            SLOT_SIZE
                .checked_add(record.len())
                .ok_or(PageError::RecordTooLarge {
                    size: record.len(),
                    capacity: max_record_size,
                })?
        };
        let available = header.free_space();
        if required > available {
            return Err(PageError::PageFull {
                required,
                available,
            }
            .into());
        }
        let slot_ref = if let Some(index) = reusable {
            let slot = &mut records[index];
            slot.generation = slot.generation.checked_add(1).ok_or(PageError::PageFull {
                required,
                available,
            })?;
            slot.payload = Some(record.to_vec());
            SlotRef {
                slot: SlotId(index as u16),
                generation: slot.generation,
            }
        } else {
            let slot = SlotId(header.slot_count);
            records.push(RebuildSlot {
                generation: 1,
                payload: Some(record.to_vec()),
            });
            SlotRef {
                slot,
                generation: 1,
            }
        };
        self.rebuild_records(&records, None)?;
        Ok(slot_ref)
    }

    /// Marks a live slot deleted and compacts remaining payloads. Generation is
    /// retained until a later insertion reuses this slot.
    pub fn delete_record(&mut self, slot: SlotId) -> Result<(), StorageError> {
        self.expect_page_type(PageType::Heap)?;
        let mut records = self.rebuild_slots()?;
        let target = records
            .get_mut(usize::from(slot.0))
            .ok_or(PageError::InvalidSlot { slot })?;
        if target.payload.is_none() {
            return Err(PageError::SlotDeleted { slot }.into());
        }
        target.payload = None;
        self.rebuild_records(&records, None)
    }

    /// Replaces one live slot while preserving its SlotId. The page remains
    /// byte-for-byte unchanged when the replacement cannot fit.
    pub fn replace_record(&mut self, slot: SlotId, record: &[u8]) -> Result<(), StorageError> {
        self.expect_page_type(PageType::Heap)?;
        let max_record_size = PAGE_SIZE - PAGE_HEADER_SIZE - SLOT_SIZE;
        if record.len() > max_record_size || record.len() > u16::MAX as usize {
            return Err(PageError::RecordTooLarge {
                size: record.len(),
                capacity: max_record_size,
            }
            .into());
        }
        let mut records = self.rebuild_slots()?;
        let target = records
            .get_mut(usize::from(slot.0))
            .ok_or(PageError::InvalidSlot { slot })?;
        if target.payload.is_none() {
            return Err(PageError::SlotDeleted { slot }.into());
        }
        target.payload = Some(record.to_vec());
        self.rebuild_records(&records, Some((slot, record.len())))
    }

    /// Initializes a non-heap page with its single generation-1 payload.
    pub(crate) fn initialize_single_payload(
        &mut self,
        expected: PageType,
        payload: &[u8],
    ) -> Result<(), StorageError> {
        let header = self.expect_page_type(expected)?;
        if expected == PageType::Heap || header.slot_count != 0 {
            return Err(PageError::InvalidSinglePayload {
                page_type: expected,
                slot_count: header.slot_count,
            }
            .into());
        }
        let capacity = Self::single_payload_capacity();
        if payload.len() > capacity || payload.len() > u16::MAX as usize {
            return Err(PageError::RecordTooLarge {
                size: payload.len(),
                capacity,
            }
            .into());
        }
        self.rebuild_records(
            &[RebuildSlot {
                generation: 1,
                payload: Some(payload.to_vec()),
            }],
            None,
        )
    }

    /// Replaces the only payload of a non-heap page without changing its slot.
    pub(crate) fn replace_single_payload(
        &mut self,
        expected: PageType,
        payload: &[u8],
    ) -> Result<(), StorageError> {
        self.validate_single_payload(expected)?;
        let capacity = Self::single_payload_capacity();
        if payload.len() > capacity || payload.len() > u16::MAX as usize {
            return Err(PageError::RecordTooLarge {
                size: payload.len(),
                capacity,
            }
            .into());
        }
        self.rebuild_records(
            &[RebuildSlot {
                generation: 1,
                payload: Some(payload.to_vec()),
            }],
            Some((SlotId(0), payload.len())),
        )
    }

    pub(crate) fn single_payload(&self, expected: PageType) -> Result<&[u8], StorageError> {
        self.validate_single_payload(expected)?;
        self.read_record(SlotId(0))
    }

    #[must_use]
    pub(crate) const fn single_payload_capacity() -> usize {
        PAGE_SIZE - PAGE_HEADER_SIZE - SLOT_SIZE
    }

    fn validate_single_payload(&self, expected: PageType) -> Result<(), StorageError> {
        let header = self.expect_page_type(expected)?;
        if expected == PageType::Heap || header.slot_count != 1 {
            return Err(PageError::InvalidSinglePayload {
                page_type: expected,
                slot_count: header.slot_count,
            }
            .into());
        }
        match self.slot_state(SlotId(0))? {
            SlotState::Live(slot) if slot.generation == 1 => Ok(()),
            SlotState::Live(slot) => Err(PageError::InvalidSinglePayloadGeneration {
                page_type: expected,
                generation: slot.generation,
            }
            .into()),
            SlotState::Deleted { generation } => Err(PageError::InvalidSinglePayloadGeneration {
                page_type: expected,
                generation,
            }
            .into()),
        }
    }

    fn expect_page_type(&self, expected: PageType) -> Result<PageHeader, StorageError> {
        let header = self.header()?;
        if header.page_type != expected {
            return Err(PageError::WrongPageType {
                expected,
                actual: header.page_type,
            }
            .into());
        }
        Ok(header)
    }

    fn rebuild_slots(&self) -> Result<Vec<RebuildSlot>, StorageError> {
        let header = self.header()?;
        (0..header.slot_count)
            .map(|index| {
                let slot = SlotId(index);
                let entry = self.slot_at(slot);
                Ok(RebuildSlot {
                    generation: entry.generation,
                    payload: if is_deleted_slot(entry) {
                        None
                    } else {
                        Some(self.read_record(slot)?.to_vec())
                    },
                })
            })
            .collect()
    }

    fn rebuild_records(
        &mut self,
        records: &[RebuildSlot],
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
        let payload_size = records.iter().try_fold(0_usize, |total, slot| {
            total
                .checked_add(slot.payload.as_ref().map_or(0, Vec::len))
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
        let slot_count =
            u16::try_from(records.len()).map_err(|_| PageError::InvalidSlotCount(u16::MAX))?;
        rebuilt.write_u16(SLOT_COUNT_OFFSET, slot_count);
        rebuilt.write_u16(FREE_START_OFFSET, directory_end as u16);
        let mut free_end = PAGE_SIZE;
        for (index, slot) in records.iter().enumerate() {
            let entry_offset = PAGE_HEADER_SIZE + index * SLOT_SIZE;
            match &slot.payload {
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
            rebuilt.write_u32(entry_offset + 4, slot.generation);
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
            generation: self.read_u32(offset + 4),
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

    fn write_u32(&mut self, offset: usize, value: u32) {
        self.bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
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
        assert_eq!(page.read_record(first.slot).expect("read first"), b"first");
        assert_eq!(
            page.read_record(second.slot).expect("read second"),
            b"second"
        );
        assert!(matches!(
            page.slot(SlotId(2)),
            Err(StorageError::Page(PageError::InvalidSlot {
                slot: SlotId(2)
            }))
        ));
    }

    #[test]
    fn validated_page_view_reads_live_deleted_and_invalid_slots() {
        let mut page = Page::new(PageId(1), PageType::Heap);
        let live = page.insert_record(b"live").expect("insert live slot");
        let deleted = page.insert_record(b"deleted").expect("insert deleted slot");
        page.delete_record(deleted.slot).expect("delete slot");
        let expected_header = page.header().expect("validate page normally");

        let validated = page.validated().expect("create validated view");
        assert_eq!(validated.header(), expected_header);
        assert_eq!(
            validated.live_record(live.slot).expect("read live slot"),
            Some((
                page.slot(live.slot).expect("read expected slot"),
                b"live".as_slice()
            ))
        );
        assert_eq!(
            validated
                .live_record(deleted.slot)
                .expect("read deleted slot"),
            None
        );
        assert!(matches!(
            validated.live_record(SlotId(expected_header.slot_count)),
            Err(StorageError::Page(PageError::InvalidSlot { slot }))
                if slot == SlotId(expected_header.slot_count)
        ));
    }

    #[test]
    fn validated_page_creation_rejects_checksum_and_structural_corruption() {
        let mut valid = Page::new(PageId(30), PageType::Heap);
        valid.insert_record(b"first").expect("insert first");
        valid.insert_record(b"other").expect("insert second");

        let mut bad_checksum = valid.clone();
        bad_checksum.bytes_mut()[PAGE_SIZE - 1] ^= 0x80;
        assert!(matches!(
            bad_checksum.validated(),
            Err(StorageError::Page(PageError::ChecksumMismatch { .. }))
        ));

        let mut zero_generation = valid.clone();
        zero_generation.bytes_mut()[super::PAGE_HEADER_SIZE + 4..super::PAGE_HEADER_SIZE + 8]
            .copy_from_slice(&0_u32.to_le_bytes());
        zero_generation.refresh_checksum();
        assert!(matches!(
            zero_generation.validated(),
            Err(StorageError::Page(PageError::InvalidSlotGeneration {
                slot: SlotId(0),
                generation: 0,
            }))
        ));

        let mut bad_bounds = valid.clone();
        bad_bounds.bytes_mut()[super::PAGE_HEADER_SIZE + 2..super::PAGE_HEADER_SIZE + 4]
            .copy_from_slice(&(u16::MAX - 1).to_le_bytes());
        bad_bounds.refresh_checksum();
        assert!(matches!(
            bad_bounds.validated(),
            Err(StorageError::Page(PageError::RecordOutOfBounds { .. }))
        ));

        let mut bad_free_space = valid.clone();
        bad_free_space.bytes_mut()[super::FREE_END_OFFSET..super::FREE_END_OFFSET + 2]
            .copy_from_slice(&((super::PAGE_HEADER_SIZE - 1) as u16).to_le_bytes());
        bad_free_space.refresh_checksum();
        assert!(matches!(
            bad_free_space.validated(),
            Err(StorageError::Page(PageError::InvalidFreeSpace { .. }))
        ));

        let mut overlap = valid;
        let first_offset = overlap.slot(SlotId(0)).expect("first slot").offset;
        let second_entry = super::PAGE_HEADER_SIZE + super::SLOT_SIZE;
        overlap.bytes_mut()[second_entry..second_entry + 2]
            .copy_from_slice(&first_offset.to_le_bytes());
        overlap.refresh_checksum();
        assert!(matches!(
            overlap.validated(),
            Err(StorageError::Page(PageError::OverlappingRecords {
                first: SlotId(0),
                second: SlotId(1),
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
        let mut version_four = Page::new(PageId(5), PageType::Heap);
        version_four.bytes_mut()[4..6].copy_from_slice(&4_u16.to_le_bytes());
        assert!(matches!(
            version_four.header(),
            Err(StorageError::Page(PageError::UnsupportedVersion(4)))
        ));
    }

    #[test]
    fn page_v5_checksum_has_a_stable_golden_value_and_binds_page_id() {
        let mut page = Page::new(PageId(7), PageType::Heap);
        page.insert_record(b"checksum-golden")
            .expect("insert golden payload");
        page.set_page_lsn(Lsn(123));
        let checksum = u32::from_le_bytes(
            page.bytes()[super::CHECKSUM_OFFSET..super::CHECKSUM_END]
                .try_into()
                .expect("checksum field"),
        );
        assert_eq!(checksum, 0x4ec3_88c1);
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
        let payload_offset = usize::from(page.slot(slot.slot).expect("slot").offset);

        for offset in [
            payload_offset,
            100,
            super::PAGE_HEADER_SIZE + 4,
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
        assert_eq!(
            page.read_record(empty.slot).expect("read empty record"),
            b""
        );
        assert_eq!(
            page.read_record(non_empty.slot)
                .expect("read non-empty record"),
            b"value"
        );
        page.delete_record(empty.slot).expect("delete empty record");
        assert!(matches!(
            page.read_record(empty.slot),
            Err(StorageError::Page(PageError::SlotDeleted { slot })) if slot == empty.slot
        ));
        assert_eq!(
            page.read_record(non_empty.slot)
                .expect("read stable record"),
            b"value"
        );
        let reused = page.insert_record(&[]).expect("reuse for empty record");
        assert_eq!(reused.slot, empty.slot);
        assert_eq!(reused.generation, empty.generation + 1);
        assert_eq!(
            page.read_record(reused.slot).expect("read reused empty"),
            b""
        );
    }

    #[test]
    fn insert_reuses_lowest_deleted_slot_and_increments_generation() {
        let mut page = Page::new(PageId(1), PageType::Heap);
        let first = page.insert_record(b"first").expect("insert first");
        let second = page.insert_record(b"second").expect("insert second");
        let third = page.insert_record(b"third").expect("insert third");
        let fourth = page.insert_record(b"fourth").expect("insert fourth");
        page.delete_record(second.slot).expect("delete second");
        page.delete_record(third.slot).expect("delete third");

        let reused_second = page.insert_record(b"new second").expect("reuse second");
        let reused_third = page.insert_record(b"new third").expect("reuse third");
        assert_eq!(reused_second.slot, second.slot);
        assert_eq!(reused_second.generation, second.generation + 1);
        assert_eq!(reused_third.slot, third.slot);
        assert_eq!(reused_third.generation, third.generation + 1);
        assert_eq!(page.header().expect("valid reused page").slot_count, 4);
        assert_eq!(
            page.read_record(first.slot).expect("first remains"),
            b"first"
        );
        assert_eq!(
            page.read_record(fourth.slot).expect("fourth remains"),
            b"fourth"
        );
        assert_eq!(
            page.read_record(reused_second.slot)
                .expect("read second reuse"),
            b"new second"
        );
    }

    #[test]
    fn repeated_reuse_keeps_slot_count_stable_and_generation_monotonic() {
        let mut page = Page::new(PageId(1), PageType::Heap);
        let mut current = page.insert_record(b"value").expect("insert initial");
        for expected_generation in 2..=65 {
            page.delete_record(current.slot).expect("delete occupant");
            current = page.insert_record(b"next").expect("reuse occupant");
            assert_eq!(current.slot, SlotId(0));
            assert_eq!(current.generation, expected_generation);
            assert_eq!(page.header().expect("valid page").slot_count, 1);
        }
    }

    #[test]
    fn exhausted_tombstone_generation_is_never_reused_or_wrapped() {
        let mut page = Page::new(PageId(1), PageType::Heap);
        let first = page.insert_record(b"first").expect("insert first");
        page.delete_record(first.slot).expect("delete first");
        page.write_u32(super::PAGE_HEADER_SIZE + 4, u32::MAX);
        page.refresh_checksum();

        let appended = page.insert_record(b"appended").expect("append new slot");
        assert_eq!(appended.slot, SlotId(1));
        assert_eq!(appended.generation, 1);
        assert_eq!(
            page.slot_state(first.slot).expect("read max tombstone"),
            super::SlotState::Deleted {
                generation: u32::MAX
            }
        );
    }

    #[test]
    fn exhausted_tombstone_returns_page_full_atomically_when_append_cannot_fit() {
        let mut page = Page::new(PageId(1), PageType::Heap);
        let first = page.insert_record(b"x").expect("insert first");
        page.insert_record(&vec![
            7;
            PAGE_SIZE - super::PAGE_HEADER_SIZE - 2 * SLOT_SIZE - 1
        ])
        .expect("fill remaining page");
        page.delete_record(first.slot).expect("delete first");
        page.write_u32(super::PAGE_HEADER_SIZE + 4, u32::MAX);
        page.refresh_checksum();
        let before = page.clone();

        assert!(matches!(
            page.insert_record(&[]),
            Err(StorageError::Page(PageError::PageFull { .. }))
        ));
        assert_eq!(page, before);
    }

    #[test]
    fn delete_compacts_payloads_without_renumbering_slots() {
        let mut page = Page::new(PageId(1), PageType::Heap);
        let first = page.insert_record(b"first").expect("insert first");
        let middle = page.insert_record(b"middle").expect("insert middle");
        let third = page.insert_record(b"third").expect("insert third");

        page.delete_record(middle.slot).expect("delete middle");
        assert_eq!(
            page.read_record(first.slot).expect("first remains"),
            b"first"
        );
        assert_eq!(
            page.read_record(third.slot).expect("third remains"),
            b"third"
        );
        assert!(page.is_slot_deleted(middle.slot).expect("deleted state"));
        assert_eq!(page.header().expect("valid compacted page").slot_count, 3);
        assert!(matches!(
            page.delete_record(middle.slot),
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
        page.replace_record(first.slot, b"x").expect("shrink");
        assert_eq!(page.read_record(first.slot).expect("read shrink"), b"x");
        page.replace_record(first.slot, b"a substantially longer replacement")
            .expect("grow");
        assert_eq!(
            page.read_record(first.slot).expect("read grow"),
            b"a substantially longer replacement"
        );
        assert_eq!(
            page.read_record(second.slot).expect("stable second"),
            b"second"
        );
    }

    #[test]
    fn overflowing_replace_leaves_page_unchanged() {
        let mut page = Page::new(PageId(1), PageType::Heap);
        let first = page.insert_record(b"first").expect("insert first");
        page.insert_record(&[7; 3_900]).expect("fill page");
        let before = page.clone();
        assert!(matches!(
            page.replace_record(first.slot, &[8; 200]),
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
    fn btree_pages_enforce_one_live_generation_one_payload() {
        for page_type in [
            PageType::BTreeMeta,
            PageType::BTreeInternal,
            PageType::BTreeLeaf,
            PageType::IndexCatalog,
        ] {
            let mut page = Page::new(PageId(30), page_type);
            page.initialize_single_payload(page_type, b"node")
                .expect("initialize payload");
            assert_eq!(
                page.single_payload(page_type).expect("single payload"),
                b"node"
            );
            page.replace_single_payload(page_type, b"replacement")
                .expect("replace payload");
            assert_eq!(
                page.single_payload(page_type).expect("replacement"),
                b"replacement"
            );
            assert!(matches!(
                page.insert_record(b"heap"),
                Err(StorageError::Page(PageError::WrongPageType { .. }))
            ));
        }

        let empty = Page::new(PageId(31), PageType::BTreeLeaf);
        assert!(matches!(
            empty.single_payload(PageType::BTreeLeaf),
            Err(StorageError::Page(PageError::InvalidSinglePayload {
                slot_count: 0,
                ..
            }))
        ));
        let mut heap = Page::new(PageId(32), PageType::Heap);
        heap.insert_record(b"row").expect("heap row");
        assert!(matches!(
            heap.single_payload(PageType::BTreeLeaf),
            Err(StorageError::Page(PageError::WrongPageType { .. }))
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

        let mut invalid_generation = Page::new(PageId(24), PageType::Heap);
        invalid_generation
            .insert_record(b"safe")
            .expect("insert record");
        invalid_generation.bytes_mut()[super::PAGE_HEADER_SIZE + 4..super::PAGE_HEADER_SIZE + 8]
            .copy_from_slice(&0_u32.to_le_bytes());
        invalid_generation.refresh_checksum();
        assert!(matches!(
            invalid_generation.header(),
            Err(StorageError::Page(PageError::InvalidSlotGeneration {
                slot: SlotId(0),
                generation: 0
            }))
        ));

        let mut overlaps_free = Page::new(PageId(22), PageType::Heap);
        let slot = overlaps_free
            .insert_record(b"record")
            .expect("insert record");
        let record_offset = overlaps_free.slot(slot.slot).expect("slot").offset;
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
        let first_offset = overlapping_records
            .slot(first.slot)
            .expect("first slot")
            .offset;
        let second_entry = super::PAGE_HEADER_SIZE + usize::from(second.slot.0) * super::SLOT_SIZE;
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
