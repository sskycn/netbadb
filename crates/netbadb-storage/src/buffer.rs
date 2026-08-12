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
    wal: Option<SharedWal>,
}

impl BufferState {
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
    ) -> Result<(), StorageError> {
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
                if page_id.0 == self.disk.page_count() {
                    // A prior retry may have completed `set_len` but failed
                    // its sync. Synchronize even when the page is already
                    // absent before allowing RollbackComplete to become
                    // durable.
                    self.disk.sync()?;
                    return Ok(());
                }
                if let Some(index) = self.find_frame(page_id) {
                    if self.frames[index].pin_count != 0 {
                        return Err(BufferError::PagePinned { page_id }.into());
                    }
                }
                self.disk.remove_trailing_page(page_id)?;
                if let Some(index) = self.find_frame(page_id) {
                    self.frames.remove(index);
                    self.next_victim = if self.frames.is_empty() {
                        0
                    } else {
                        self.next_victim.min(self.frames.len() - 1)
                    };
                }
                self.disk.sync()?;
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

    pub(crate) fn undo_page_update(
        &self,
        page_id: PageId,
        before: &[u8; crate::PAGE_SIZE],
    ) -> Result<(), StorageError> {
        self.state.borrow_mut().undo_page_update(page_id, before)
    }

    #[cfg(test)]
    pub(crate) fn inject_page_write_failure(&self) {
        self.state.borrow_mut().disk.inject_write_failure();
    }

    #[cfg(test)]
    pub(crate) fn inject_page_sync_failure(&self) {
        self.state.borrow_mut().disk.inject_sync_failure();
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
    use netbadb_types::{PageId, TxnId};

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
            page_a.page_mut().bytes_mut()[100] = 77;
        }
        let page_b = pool.read_page(PageId(2)).expect("evict page A for B");
        drop(page_b);
        let page_a_again = pool.read_page(PageId(1)).expect("reload dirty page A");
        assert_eq!(page_a_again.page().bytes()[100], 77);
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
        page.page_mut().bytes_mut()[100] = 91;

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
            reopened.read_page(PageId(1)).expect("read page").bytes()[100],
            91
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
