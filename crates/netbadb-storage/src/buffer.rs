use std::cell::RefCell;
use std::rc::Rc;

use netbadb_types::PageId;

use crate::{BufferError, Page, PageManager, StorageError};

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
        if frame.dirty {
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
        for index in 0..self.frames.len() {
            self.flush_frame(index)?;
        }
        self.disk.sync()
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
    pub fn new(page_manager: PageManager, capacity: usize) -> Result<Self, StorageError> {
        if capacity == 0 {
            return Err(BufferError::InvalidCapacity.into());
        }
        Ok(Self {
            state: Rc::new(RefCell::new(BufferState {
                disk: page_manager,
                frames: Vec::with_capacity(capacity),
                capacity,
                next_victim: 0,
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

    pub fn write_page(&self, page_id: PageId) -> Result<WritePageGuard, StorageError> {
        let page = self.state.borrow_mut().pin_write(page_id)?;
        Ok(WritePageGuard {
            state: Rc::clone(&self.state),
            page_id,
            page,
            dirty: false,
        })
    }

    pub fn new_page(&self) -> Result<WritePageGuard, StorageError> {
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

/// A short-lived owned mutable page snapshot. Calling `page_mut` or
/// `mark_dirty` causes the page to be written back to its frame on drop.
#[derive(Debug)]
pub struct WritePageGuard {
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

    pub fn page_mut(&mut self) -> &mut Page {
        self.dirty = true;
        &mut self.page
    }

    pub fn mark_dirty(&mut self) {
        self.dirty = true;
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
    use super::BufferPool;
    use crate::{Page, PageManager, PageType, StorageError};
    use netbadb_types::PageId;

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
}
