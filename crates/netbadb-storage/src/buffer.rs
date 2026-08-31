use std::cell::RefCell;
use std::rc::Rc;

use netbadb_types::PageId;

use crate::page::{ValidatedBeforeImage, validate_before_image};
use crate::transaction::SharedWal;
use crate::{BufferError, Page, PageManager, StorageError, TransactionError};

#[derive(Debug)]
struct BufferFrame {
    page_id: PageId,
    page: Page,
    pin_count: u32,
    writer: bool,
    dirty: bool,
}

#[derive(Debug)]
struct BufferState {
    disk: PageManager,
    frames: Vec<BufferFrame>,
    capacity: usize,
    next_victim: usize,
    reuse_inventory_invalid: bool,
    wal: Option<SharedWal>,
}

impl BufferState {
    /// Shared removal mechanics. Rollback may discard its own dirty allocation;
    /// maintenance must preflight clean, unpinned frames before calling this.
    fn remove_frame(&mut self, index: usize) {
        self.frames.remove(index);
        self.next_victim = self.next_victim.min(self.frames.len().saturating_sub(1));
    }

    fn ensure_clean_suffix(&self, start: u64) -> Result<(), StorageError> {
        for frame in self.frames.iter().filter(|frame| frame.page_id.0 >= start) {
            if frame.pin_count != 0 || frame.writer {
                return Err(BufferError::PagePinned {
                    page_id: frame.page_id,
                }
                .into());
            }
            if frame.dirty {
                return Err(BufferError::PageDirty {
                    page_id: frame.page_id,
                }
                .into());
            }
        }
        Ok(())
    }

    fn find_frame(&self, page_id: PageId) -> Option<usize> {
        self.frames
            .iter()
            .position(|frame| frame.page_id == page_id)
    }

    fn prepare_frame(&mut self) -> Result<usize, StorageError> {
        if self.frames.len() < self.capacity {
            return Ok(self.frames.len());
        }

        for offset in 0..self.frames.len() {
            let index = (self.next_victim + offset) % self.frames.len();
            if self.frames[index].pin_count == 0 {
                self.flush_frame(index)?;
                self.next_victim = (index + 1) % self.frames.len();
                return Ok(index);
            }
        }
        Err(BufferError::Exhausted {
            capacity: self.capacity,
        }
        .into())
    }

    fn install_frame(&mut self, index: usize, page: Page, writer: bool) {
        let frame = BufferFrame {
            page_id: page.id,
            page,
            pin_count: 1,
            writer,
            dirty: false,
        };
        if index == self.frames.len() {
            self.frames.push(frame);
        } else {
            debug_assert!(index < self.frames.len());
            self.frames[index] = frame;
        }
    }

    fn flush_frame(&mut self, index: usize) -> Result<(), StorageError> {
        let frame = self.frames.get_mut(index).ok_or(BufferError::Exhausted {
            capacity: self.capacity,
        })?;
        if frame.writer {
            return Err(BufferError::PagePinned {
                page_id: frame.page_id,
            }
            .into());
        }
        if frame.dirty {
            if frame.page_id.0 != 0 {
                if let Some(page_lsn) = frame.page.page_lsn()? {
                    let wal = self.wal.as_ref().ok_or(BufferError::WalUnavailable {
                        page_id: frame.page_id,
                        page_lsn,
                    })?;
                    wal.try_borrow_mut()
                        .map_err(|_| TransactionError::WalBusy)?
                        .flush_through(page_lsn)?;
                }
            }
            self.disk.write_page(&frame.page)?;
            frame.dirty = false;
        }
        Ok(())
    }

    fn pin_read(&mut self, page_id: PageId) -> Result<Page, StorageError> {
        if let Some(index) = self.find_frame(page_id) {
            let frame = &mut self.frames[index];
            if frame.writer {
                return Err(BufferError::PagePinned { page_id }.into());
            }
            frame.pin_count = frame
                .pin_count
                .checked_add(1)
                .ok_or(BufferError::PinCountOverflow { page_id })?;
            return Ok(frame.page.clone());
        }

        let page = self.disk.read_page(page_id)?;
        let index = self.prepare_frame()?;
        self.install_frame(index, page.clone(), false);
        Ok(page)
    }

    fn validate_btree_frame(
        &self,
        reference: netbadb_index::BTreePageRef,
    ) -> Result<(), StorageError> {
        if let Some(index) = self.find_frame(reference.page_id()) {
            let frame = &self.frames[index];
            if frame.page.allocation_generation()? != reference.generation() && frame.pin_count != 0
            {
                return Err(BufferError::PagePinned {
                    page_id: frame.page_id,
                }
                .into());
            }
            frame.page.validate_allocation(reference.generation())?;
        }
        Ok(())
    }

    fn pin_write(&mut self, page_id: PageId) -> Result<Page, StorageError> {
        if let Some(index) = self.find_frame(page_id) {
            let frame = &self.frames[index];
            if frame.pin_count != 0 {
                return Err(BufferError::PagePinned { page_id }.into());
            }
            let frame = &mut self.frames[index];
            frame.pin_count = 1;
            frame.writer = true;
            return Ok(frame.page.clone());
        }

        let page = self.disk.read_page(page_id)?;
        let index = self.prepare_frame()?;
        self.install_frame(index, page.clone(), true);
        Ok(page)
    }

    fn allocate_page(&mut self) -> Result<Page, StorageError> {
        // Reserve an evictable frame before growing the file, so a pinned pool
        // does not allocate an unreachable page and then report exhaustion.
        let index = self.prepare_frame()?;
        let page = self.disk.allocate_page()?;
        self.install_frame(index, page.clone(), true);
        Ok(page)
    }

    fn release_read(&mut self, page_id: PageId) {
        if let Some(index) = self.find_frame(page_id) {
            let frame = &mut self.frames[index];
            debug_assert!(!frame.writer);
            debug_assert!(frame.pin_count > 0);
            if frame.pin_count > 0 {
                frame.pin_count -= 1;
            }
        }
    }

    fn release_write(&mut self, page_id: PageId, mut page: Page, dirty: bool) {
        if let Some(index) = self.find_frame(page_id) {
            let frame = &mut self.frames[index];
            debug_assert!(frame.writer);
            debug_assert_eq!(frame.pin_count, 1);
            page.id = page_id;
            frame.page = page;
            frame.dirty |= dirty;
            frame.writer = false;
            if frame.pin_count > 0 {
                frame.pin_count -= 1;
            }
        }
    }

    fn flush_page(&mut self, page_id: PageId) -> Result<(), StorageError> {
        let index = self
            .find_frame(page_id)
            .ok_or(BufferError::PageNotCached { page_id })?;
        self.flush_frame(index)?;
        self.disk.sync()
    }

    fn flush_all(&mut self) -> Result<(), StorageError> {
        if let Some(frame) = self.frames.iter().find(|frame| frame.writer) {
            return Err(BufferError::PagePinned {
                page_id: frame.page_id,
            }
            .into());
        }
        for index in 0..self.frames.len() {
            self.flush_frame(index)?;
        }
        self.disk.sync()
    }

    fn undo_page_update(
        &mut self,
        page_id: PageId,
        before: &[u8; crate::PAGE_SIZE],
        expected: Option<netbadb_types::PageGeneration>,
    ) -> Result<(), StorageError> {
        if page_id.0 < self.disk.page_count() {
            let current = if let Some(index) = self.find_frame(page_id) {
                if self.frames[index].pin_count != 0 {
                    return Err(BufferError::PagePinned { page_id }.into());
                }
                self.frames[index].page.clone()
            } else {
                self.disk.read_page(page_id)?
            };
            if current.bytes().iter().any(|b| *b != 0) {
                current.validate_allocation(expected)?;
            }
        }
        match validate_before_image(page_id, before)? {
            ValidatedBeforeImage::Existing(page) => {
                let page = *page;
                if page_id.0 >= self.disk.page_count() {
                    return Err(crate::invalid_format(format!(
                        "rollback page {} is outside the data file",
                        page_id.0
                    )));
                }
                let index = if let Some(index) = self.find_frame(page_id) {
                    let frame = &mut self.frames[index];
                    if frame.pin_count != 0 {
                        return Err(BufferError::PagePinned { page_id }.into());
                    }
                    frame.page = page;
                    frame.dirty = true;
                    index
                } else {
                    let index = self.prepare_frame()?;
                    self.install_frame(index, page, false);
                    let frame = &mut self.frames[index];
                    frame.pin_count = 0;
                    frame.dirty = true;
                    index
                };
                self.flush_frame(index)?;
                self.disk.sync()?;
            }
            ValidatedBeforeImage::NewPage => {
                let page_count = self.disk.page_count();
                if page_id.0 > page_count {
                    // A compound operation can log several future pages before
                    // allocating any of them. Reverse undo reaches the highest
                    // not-yet-allocated page first; there is no physical state
                    // to restore for that record.
                    return Ok(());
                }
                if page_id.0 == page_count {
                    // Also truncates a partial allocation that extended the
                    // file without advancing the logical page count.
                    self.disk.remove_trailing_page(page_id)?;
                    self.disk.sync()?;
                    return Ok(());
                }
                if let Some(index) = self.find_frame(page_id) {
                    if self.frames[index].pin_count != 0 {
                        return Err(BufferError::PagePinned { page_id }.into());
                    }
                }
                if let Some(index) = self.find_frame(page_id) {
                    self.remove_frame(index);
                }
                self.disk.remove_trailing_page(page_id)?;
                self.disk.sync()?;
                #[cfg(test)]
                crate::crash_test::maybe_crash(
                    crate::crash_test::TestCrashPoint::RollbackAfterTrailingRemoval,
                );
            }
        }
        Ok(())
    }
}

/// A synchronous, single-threaded buffer pool. `Rc<RefCell<_>>` is confined to
/// this module so guards can release pins without spreading page lifetimes or
/// synchronization primitives through Heap, Executor, or Database.
#[derive(Debug, Clone)]
pub struct BufferPool {
    state: Rc<RefCell<BufferState>>,
}

pub const DEFAULT_BUFFER_POOL_SIZE: usize = 8;

impl BufferPool {
    pub(crate) fn validate_capacity(capacity: usize) -> Result<(), StorageError> {
        if capacity == 0 {
            return Err(BufferError::InvalidCapacity.into());
        }
        Ok(())
    }

    pub fn new(page_manager: PageManager, capacity: usize) -> Result<Self, StorageError> {
        Self::new_inner(page_manager, capacity, None)
    }

    pub(crate) fn with_wal(
        page_manager: PageManager,
        capacity: usize,
        wal: SharedWal,
    ) -> Result<Self, StorageError> {
        Self::new_inner(page_manager, capacity, Some(wal))
    }

    fn new_inner(
        page_manager: PageManager,
        capacity: usize,
        wal: Option<SharedWal>,
    ) -> Result<Self, StorageError> {
        Self::validate_capacity(capacity)?;
        Ok(Self {
            state: Rc::new(RefCell::new(BufferState {
                disk: page_manager,
                frames: Vec::with_capacity(capacity),
                capacity,
                next_victim: 0,
                reuse_inventory_invalid: true,
                wal,
            })),
        })
    }

    pub fn with_default_capacity(page_manager: PageManager) -> Result<Self, StorageError> {
        Self::new(page_manager, DEFAULT_BUFFER_POOL_SIZE)
    }

    #[must_use]
    pub fn capacity(&self) -> usize {
        self.state.borrow().capacity
    }

    #[must_use]
    pub fn page_count(&self) -> u64 {
        self.state.borrow().disk.page_count()
    }

    pub(crate) fn validated_page_count(&self) -> Result<u64, StorageError> {
        self.state.borrow().disk.validated_page_count()
    }

    pub(crate) fn ensure_clean_suffix(&self, start: u64) -> Result<(), StorageError> {
        self.state.borrow().ensure_clean_suffix(start)
    }

    pub(crate) fn invalidate_suffix(&self, start: u64) -> Result<(), StorageError> {
        let mut state = self.state.borrow_mut();
        state.ensure_clean_suffix(start)?;
        for index in (0..state.frames.len()).rev() {
            if state.frames[index].page_id.0 >= start {
                state.remove_frame(index);
            }
        }
        Ok(())
    }

    pub(crate) fn truncate_to_page_count(&self, old: u64, new: u64) -> Result<(), StorageError> {
        let mut state = self.state.borrow_mut();
        if state.frames.iter().any(|frame| frame.page_id.0 >= new) {
            return Err(crate::invalid_format("tail frames remain before truncate"));
        }
        state.disk.truncate_to_page_count(old, new)
    }

    pub fn fetch_page(&self, page_id: PageId) -> Result<ReadPageGuard, StorageError> {
        self.read_page(page_id)
    }

    pub fn read_page(&self, page_id: PageId) -> Result<ReadPageGuard, StorageError> {
        let page = self.state.borrow_mut().pin_read(page_id)?;
        Ok(ReadPageGuard {
            state: Rc::clone(&self.state),
            page_id,
            page,
        })
    }

    /// Exact allocation lookup. A slot hit is not an identity hit. The pool is
    /// the sole runtime owner of PageManager, and only coordinated rollback or tail maintenance can
    /// remove/reappend slots; stale requests never evict a newer dirty frame.
    pub(crate) fn read_btree_page(
        &self,
        reference: netbadb_index::BTreePageRef,
    ) -> Result<ReadPageGuard, StorageError> {
        self.state.borrow().validate_btree_frame(reference)?;
        let guard = self.read_page(reference.page_id())?;
        guard.page().validate_allocation(reference.generation())?;
        Ok(guard)
    }

    pub(crate) fn write_btree_page(
        &self,
        reference: netbadb_index::BTreePageRef,
    ) -> Result<WritePageGuard, StorageError> {
        self.state.borrow().validate_btree_frame(reference)?;
        let guard = self.write_page(reference.page_id())?;
        guard.page().validate_allocation(reference.generation())?;
        Ok(guard)
    }

    pub(crate) fn write_page(&self, page_id: PageId) -> Result<WritePageGuard, StorageError> {
        let page = self.state.borrow_mut().pin_write(page_id)?;
        Ok(WritePageGuard {
            state: Rc::clone(&self.state),
            page_id,
            page,
            dirty: false,
        })
    }

    pub(crate) fn new_page(&self) -> Result<WritePageGuard, StorageError> {
        let page = self.state.borrow_mut().allocate_page()?;
        Ok(WritePageGuard {
            state: Rc::clone(&self.state),
            page_id: page.id,
            page,
            dirty: false,
        })
    }

    pub fn flush_page(&self, page_id: PageId) -> Result<(), StorageError> {
        self.state.borrow_mut().flush_page(page_id)
    }

    pub fn flush_all(&self) -> Result<(), StorageError> {
        self.state.borrow_mut().flush_all()
    }

    /// Maintenance admission preflight; no live guard may span a catalog rewrite.
    pub(crate) fn ensure_unpinned(&self) -> Result<(), StorageError> {
        for frame in &self.state.borrow().frames {
            if frame.pin_count != 0 {
                return Err(BufferError::PagePinned {
                    page_id: frame.page_id,
                }
                .into());
            }
        }
        Ok(())
    }

    /// Snapshot without installing/evicting a frame. Used for allocation
    /// preflight and retryable physical undo under the single writer lease.
    pub(crate) fn allocation_snapshot(&self, page_id: PageId) -> Result<Page, StorageError> {
        let mut state = self.state.borrow_mut();
        if let Some(index) = state.find_frame(page_id) {
            if state.frames[index].pin_count != 0 {
                return Err(BufferError::PagePinned { page_id }.into());
            }
            return Ok(state.frames[index].page.clone());
        }
        state.disk.read_page(page_id)
    }

    pub(crate) fn invalidate_reuse_inventory(&self) {
        self.state.borrow_mut().reuse_inventory_invalid = true;
    }

    pub(crate) fn take_reuse_invalidation(&self) -> bool {
        std::mem::take(&mut self.state.borrow_mut().reuse_inventory_invalid)
    }

    pub(crate) fn reusable_page_snapshot(
        &self,
        page_id: PageId,
    ) -> Result<Option<Page>, StorageError> {
        let mut state = self.state.borrow_mut();
        if let Some(index) = state.find_frame(page_id) {
            let frame = &state.frames[index];
            if frame.pin_count != 0 || frame.writer || frame.dirty {
                return Ok(None);
            }
        }
        Ok(Some(state.disk.read_page(page_id)?))
    }

    /// Strict maintenance precondition, unlike allocation's skip-and-append
    /// policy. No eviction, writeback, invalidation or frame installation.
    pub(crate) fn maintenance_page_snapshot(&self, page_id: PageId) -> Result<Page, StorageError> {
        let mut state = self.state.borrow_mut();
        if let Some(index) = state.find_frame(page_id) {
            let frame = &state.frames[index];
            if frame.pin_count != 0 || frame.writer {
                return Err(BufferError::PagePinned { page_id }.into());
            }
            if frame.dirty {
                return Err(BufferError::PageDirty { page_id }.into());
            }
        }
        let disk = state.disk.read_page(page_id)?;
        if let Some(index) = state.find_frame(page_id) {
            if state.frames[index].page.bytes() != disk.bytes() {
                return Err(crate::invalid_format("maintenance frame differs from disk"));
            }
        }
        Ok(disk)
    }

    /// Only after the explicit transition was logged. Never retain an old
    /// allocation frame: remove it, then install the new identity as dirty.
    pub(crate) fn publish_page_transition(
        &self,
        before: &Page,
        after: &Page,
    ) -> Result<(), StorageError> {
        crate::allocation_transition::validate(before, after)?;
        let current = self
            .reusable_page_snapshot(before.id)?
            .ok_or(BufferError::PageDirty { page_id: before.id })?;
        if current.bytes() != before.bytes() {
            return Err(crate::invalid_format(
                "transition candidate changed before publication",
            ));
        }
        let mut state = self.state.borrow_mut();
        if let Some(index) = state.find_frame(before.id) {
            state.remove_frame(index);
        }
        let index = state.prepare_frame()?;
        state.install_frame(index, after.clone(), false);
        state.frames[index].pin_count = 0;
        state.frames[index].dirty = true;
        #[cfg(test)]
        crate::crash_test::maybe_crash(crate::crash_test::TestCrashPoint::TransitionAfterPublish);
        Ok(())
    }

    pub(crate) fn undo_page_transition(
        &self,
        before: &Page,
        after: &Page,
    ) -> Result<(), StorageError> {
        let current = self.allocation_snapshot(before.id)?;
        if !crate::allocation_transition::undo(&current, before, after)? {
            return Ok(());
        }
        let mut state = self.state.borrow_mut();
        // This dirty frame belongs to the transaction being physically undone.
        if let Some(index) = state.find_frame(before.id) {
            state.remove_frame(index);
        }
        state.disk.write_page(before)?;
        state.disk.sync()?;
        #[cfg(test)]
        crate::crash_test::maybe_crash(crate::crash_test::TestCrashPoint::TransitionAfterUndo);
        Ok(())
    }

    pub(crate) fn undo_page_update(
        &self,
        page_id: PageId,
        before: &[u8; crate::PAGE_SIZE],
        expected: Option<netbadb_types::PageGeneration>,
    ) -> Result<(), StorageError> {
        self.state
            .borrow_mut()
            .undo_page_update(page_id, before, expected)
    }

    #[cfg(test)]
    pub(crate) fn inject_page_write_failure(&self) {
        self.state.borrow_mut().disk.inject_write_failure();
    }

    #[cfg(test)]
    pub(crate) fn inject_page_sync_failure(&self) {
        self.state.borrow_mut().disk.inject_sync_failure();
    }

    #[cfg(test)]
    pub(crate) fn inject_partial_page_allocation_failure(&self, after_bytes: usize) {
        self.state
            .borrow_mut()
            .disk
            .inject_partial_allocation_failure(after_bytes);
    }
}

/// A short-lived owned snapshot of a pinned page. The guard does not expose a
/// reference into a frame, so its lifetime cannot escape into higher layers.
#[derive(Debug)]
pub struct ReadPageGuard {
    state: Rc<RefCell<BufferState>>,
    page_id: PageId,
    page: Page,
}

impl ReadPageGuard {
    #[must_use]
    pub fn page_id(&self) -> PageId {
        self.page_id
    }

    #[must_use]
    pub fn page(&self) -> &Page {
        &self.page
    }
}

impl Drop for ReadPageGuard {
    fn drop(&mut self) {
        if let Ok(mut state) = self.state.try_borrow_mut() {
            state.release_read(self.page_id);
        }
    }
}

/// A short-lived owned mutable page snapshot. Calling `page_mut` causes the
/// page to be written back to its frame on drop.
#[derive(Debug)]
pub(crate) struct WritePageGuard {
    state: Rc<RefCell<BufferState>>,
    page_id: PageId,
    page: Page,
    dirty: bool,
}

impl WritePageGuard {
    #[must_use]
    pub fn page_id(&self) -> PageId {
        self.page_id
    }

    #[must_use]
    pub fn page(&self) -> &Page {
        &self.page
    }

    pub(crate) fn page_mut(&mut self) -> &mut Page {
        self.dirty = true;
        &mut self.page
    }
}

impl Drop for WritePageGuard {
    fn drop(&mut self) {
        if let Ok(mut state) = self.state.try_borrow_mut() {
            state.release_write(self.page_id, self.page.clone(), self.dirty);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use super::BufferPool;
    use crate::{Page, PageManager, PageType, StorageError, WalManager, WalRecordKind};
    use netbadb_types::{PageId, SlotId, TxnId};

    fn test_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("netbadb-{name}-{}", std::process::id()))
    }

    fn prepared_manager(path: &std::path::Path) -> PageManager {
        let mut manager = PageManager::create(path).expect("create page file");
        for id in [PageId(1), PageId(2)] {
            let page = Page::new(id, PageType::Heap);
            let allocated = manager.allocate_page().expect("allocate page");
            assert_eq!(allocated.id, id);
            manager.write_page(&page).expect("write page");
        }
        manager.sync().expect("sync pages");
        manager
    }

    fn prepare_logged_update(
        pool: &BufferPool,
        wal: &Rc<RefCell<WalManager>>,
    ) -> netbadb_types::Lsn {
        let begin = wal
            .borrow_mut()
            .append(TxnId(1), None, WalRecordKind::Begin)
            .expect("append begin");
        let mut guard = pool.write_page(PageId(1)).expect("write page");
        let before = guard.page().clone();
        let mut after = before.clone();
        after.insert_record(b"logged").expect("insert record");
        let update_lsn = wal.borrow().next_lsn();
        after.set_page_lsn(update_lsn);
        let appended = wal
            .borrow_mut()
            .append(
                TxnId(1),
                Some(begin),
                crate::wal::page_update_kind(&before, &after),
            )
            .expect("append update");
        *guard.page_mut() = after;
        appended
    }

    #[test]
    fn capacity_one_evicts_clean_page_and_reloads_from_disk() {
        let path = test_path("buffer-clean-eviction");
        let manager = prepared_manager(&path);
        let pool = BufferPool::new(manager, 1).expect("create buffer pool");

        let page_a = pool.read_page(PageId(1)).expect("read page A");
        assert_eq!(page_a.page_id(), PageId(1));
        drop(page_a);
        let page_b = pool.read_page(PageId(2)).expect("read page B");
        drop(page_b);
        let page_a_again = pool.read_page(PageId(1)).expect("reload page A");
        assert_eq!(
            page_a_again.page().header().expect("valid page"),
            Page::new(PageId(1), PageType::Heap)
                .header()
                .expect("valid page")
        );
        drop(page_a_again);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn dirty_page_is_written_before_eviction() {
        let path = test_path("buffer-dirty-eviction");
        let manager = prepared_manager(&path);
        let pool = BufferPool::new(manager, 1).expect("create buffer pool");

        {
            let mut page_a = pool.write_page(PageId(1)).expect("write page A");
            page_a
                .page_mut()
                .insert_record(b"dirty")
                .expect("mutate page A");
        }
        let page_b = pool.read_page(PageId(2)).expect("evict page A for B");
        drop(page_b);
        let page_a_again = pool.read_page(PageId(1)).expect("reload dirty page A");
        assert_eq!(
            page_a_again
                .page()
                .read_record(SlotId(0))
                .expect("read dirty record"),
            b"dirty"
        );
        drop(page_a_again);
        pool.flush_all().expect("flush pool");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn pinned_page_is_not_evicted() {
        let path = test_path("buffer-pinned");
        let manager = prepared_manager(&path);
        let pool = BufferPool::new(manager, 1).expect("create buffer pool");
        let page_a = pool.read_page(PageId(1)).expect("pin page A");
        assert!(matches!(
            pool.read_page(PageId(2)),
            Err(StorageError::Buffer(crate::BufferError::Exhausted {
                capacity: 1
            }))
        ));
        drop(page_a);
        let page_b = pool.read_page(PageId(2)).expect("load page B");
        drop(page_b);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn flush_rejects_an_active_write_guard_and_succeeds_after_release() {
        let path = test_path("buffer-flush-writer");
        let manager = prepared_manager(&path);
        let pool = BufferPool::new(manager, 1).expect("create buffer pool");
        let mut page = pool.write_page(PageId(1)).expect("write page");
        page.page_mut()
            .insert_record(b"flushed")
            .expect("mutate page");

        assert!(matches!(
            pool.flush_all(),
            Err(StorageError::Buffer(crate::BufferError::PagePinned {
                page_id: PageId(1)
            }))
        ));

        drop(page);
        pool.flush_all().expect("flush released page");
        drop(pool);

        let mut reopened = PageManager::open(&path).expect("reopen page file");
        assert_eq!(
            reopened
                .read_page(PageId(1))
                .expect("read page")
                .read_record(SlotId(0))
                .expect("read flushed record"),
            b"flushed"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn capacity_one_eviction_flushes_wal_before_the_dirty_page() {
        let path = test_path("buffer-wal-eviction");
        let wal_path = crate::wal_path(&path);
        let manager = prepared_manager(&path);
        let wal = Rc::new(RefCell::new(
            WalManager::create(&wal_path).expect("create WAL"),
        ));
        let pool = BufferPool::with_wal(manager, 1, Rc::clone(&wal)).expect("create pool");
        let update_lsn = prepare_logged_update(&pool, &wal);

        let other = pool.read_page(PageId(2)).expect("evict dirty page");
        drop(other);
        assert!(
            wal.borrow()
                .durable_lsn()
                .is_some_and(|lsn| lsn >= update_lsn)
        );
        let reloaded = pool.read_page(PageId(1)).expect("reload page");
        assert_eq!(
            reloaded.page().page_lsn().expect("pageLSN"),
            Some(update_lsn)
        );
        drop(reloaded);
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(wal_path);
    }

    #[test]
    fn wal_flush_failure_prevents_the_data_page_write() {
        let path = test_path("buffer-wal-failure");
        let wal_path = crate::wal_path(&path);
        let manager = prepared_manager(&path);
        let wal = Rc::new(RefCell::new(
            WalManager::create(&wal_path).expect("create WAL"),
        ));
        let pool = BufferPool::with_wal(manager, 1, Rc::clone(&wal)).expect("create pool");
        prepare_logged_update(&pool, &wal);
        wal.borrow_mut().inject_flush_failure();

        assert!(matches!(pool.flush_all(), Err(StorageError::Wal(_))));
        drop(pool);
        let mut disk = PageManager::open(&path).expect("open data file");
        assert_eq!(
            disk.read_page(PageId(1))
                .expect("read unchanged page")
                .page_lsn()
                .expect("pageLSN"),
            None
        );
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(wal_path);
    }

    #[test]
    fn page_write_failure_happens_only_after_wal_is_durable() {
        let path = test_path("buffer-page-write-failure");
        let wal_path = crate::wal_path(&path);
        let manager = prepared_manager(&path);
        let wal = Rc::new(RefCell::new(
            WalManager::create(&wal_path).expect("create WAL"),
        ));
        let pool = BufferPool::with_wal(manager, 1, Rc::clone(&wal)).expect("create pool");
        let update_lsn = prepare_logged_update(&pool, &wal);
        pool.inject_page_write_failure();

        assert!(matches!(pool.flush_all(), Err(StorageError::Io(_))));
        assert!(
            wal.borrow()
                .durable_lsn()
                .is_some_and(|lsn| lsn >= update_lsn)
        );
        pool.flush_all().expect("retry page flush");
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(wal_path);
    }
}

#[cfg(test)]
mod tail_tests {
    use super::*;

    #[test]
    fn tail_invalidation_preflights_every_frame_and_preserves_prefix() {
        for cached in [false, true] {
            let path = std::env::temp_dir().join(format!(
                "netbadb-round11-buffer-{cached}-{}",
                std::process::id()
            ));
            let _ = std::fs::remove_file(&path);
            let mut disk = PageManager::create(&path).unwrap();
            for number in 1..6 {
                let page = disk.allocate_page().unwrap();
                disk.write_page(&Page::new(page.id, crate::PageType::Heap))
                    .unwrap();
                assert_eq!(page.id, PageId(number));
            }
            disk.sync().unwrap();
            let pool = BufferPool::new(disk, 6).unwrap();
            drop(pool.read_page(PageId(2)).unwrap());
            if cached {
                drop(pool.read_page(PageId(3)).unwrap());
                let pin = pool.read_page(PageId(5)).unwrap();
                assert!(matches!(
                    pool.invalidate_suffix(3),
                    Err(StorageError::Buffer(BufferError::PagePinned { .. }))
                ));
                assert!(pool.state.borrow().find_frame(PageId(3)).is_some());
                drop(pin);
                pool.write_page(PageId(4))
                    .unwrap()
                    .page_mut()
                    .insert_record(b"dirty")
                    .unwrap();
                assert!(matches!(
                    pool.invalidate_suffix(3),
                    Err(StorageError::Buffer(BufferError::PageDirty { .. }))
                ));
                assert_eq!(pool.validated_page_count().unwrap(), 6);
                assert!(pool.state.borrow().find_frame(PageId(3)).is_some());
                pool.flush_all().unwrap();
            }
            pool.invalidate_suffix(3).unwrap();
            assert!(pool.state.borrow().frames.iter().all(|f| f.page_id.0 < 3));
            assert!(pool.state.borrow().find_frame(PageId(2)).is_some());
            pool.truncate_to_page_count(6, 3).unwrap();
            assert!(pool.read_page(PageId(3)).is_err());
            assert!(pool.read_page(PageId(2)).is_ok());
            drop(pool);
            assert_eq!(PageManager::open(&path).unwrap().page_count(), 3);
            std::fs::remove_file(path).unwrap();
        }
    }
}
