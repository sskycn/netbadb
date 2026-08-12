use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use netbadb_types::PageId;

use crate::{StorageError, invalid_format};

pub const PAGE_SIZE: usize = 4096;
const FILE_MAGIC: &[u8; 4] = b"NBPG";

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
}

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
        if length < PAGE_SIZE as u64 || length % PAGE_SIZE as u64 != 0 {
            return Err(invalid_format("file size is not a whole number of pages"));
        }
        let mut magic = [0; FILE_MAGIC.len()];
        file.read_exact(&mut magic)?;
        if &magic != FILE_MAGIC {
            return Err(invalid_format("page file magic does not match"));
        }
        Ok(Self {
            file,
            page_count: length / PAGE_SIZE as u64,
        })
    }

    #[must_use]
    pub fn page_count(&self) -> u64 {
        self.page_count
    }

    pub fn allocate_page(&mut self) -> Result<Page, StorageError> {
        let id = PageId(self.page_count);
        let page = Page::zero(id);
        self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(page.bytes())?;
        self.page_count += 1;
        Ok(page)
    }

    pub fn read_page(&mut self, id: PageId) -> Result<Page, StorageError> {
        self.ensure_page_exists(id)?;
        let mut bytes = [0; PAGE_SIZE];
        self.file.seek(SeekFrom::Start(id.0 * PAGE_SIZE as u64))?;
        self.file.read_exact(&mut bytes)?;
        Ok(Page::from_bytes(id, bytes))
    }

    pub fn write_page(&mut self, page: &Page) -> Result<(), StorageError> {
        self.ensure_page_exists(page.id)?;
        self.file
            .seek(SeekFrom::Start(page.id.0 * PAGE_SIZE as u64))?;
        self.file.write_all(page.bytes())?;
        Ok(())
    }

    pub fn sync(&mut self) -> Result<(), StorageError> {
        self.file.sync_all()?;
        Ok(())
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

    use super::{PAGE_SIZE, Page, PageManager};
    use netbadb_types::PageId;

    #[test]
    fn page_bytes_round_trip_through_file() {
        let path = std::env::temp_dir().join(format!("netbadb-page-{}", std::process::id()));
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
        let path =
            std::env::temp_dir().join(format!("netbadb-truncated-page-{}", std::process::id()));
        let mut file = File::create(&path).expect("create truncated file");
        file.write_all(b"NBPG").expect("write truncated header");
        drop(file);

        assert!(PageManager::open(&path).is_err());
        let _ = std::fs::remove_file(path);
    }
}
