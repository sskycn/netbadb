use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};

/// The LSM root directory inode is stable across Manifest and WAL rotation.
/// Holding this descriptor in shared transaction state keeps ownership alive
/// until every storage user has finished.
#[derive(Debug)]
pub(crate) struct DirectoryOwner {
    file: File,
}

impl DirectoryOwner {
    #[cfg(unix)]
    #[allow(unsafe_code)]
    pub(crate) fn acquire(root: &Path) -> io::Result<(PathBuf, Self)> {
        use std::os::fd::AsRawFd;
        use std::os::unix::fs::MetadataExt;

        let root = root.canonicalize()?;
        let file = File::open(&root)?;
        let metadata = file.metadata()?;
        if !metadata.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "LSM root must be a directory",
            ));
        }
        loop {
            // SAFETY: File owns the live directory descriptor. flock retains
            // no Rust pointer and the descriptor outlives the lock operation.
            if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
                break;
            }
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            if error.kind() == io::ErrorKind::WouldBlock {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    format!("LSM storage {} is already in use", root.display()),
                ));
            }
            return Err(error);
        }
        let named = root.symlink_metadata()?;
        if !named.is_dir() || named.dev() != metadata.dev() || named.ino() != metadata.ino() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "LSM root changed while acquiring ownership",
            ));
        }
        Ok((root, Self { file }))
    }

    #[cfg(not(unix))]
    pub(crate) fn acquire(_root: &Path) -> io::Result<(PathBuf, Self)> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "LSM ownership requires Unix flock",
        ))
    }

    #[cfg(unix)]
    #[allow(unsafe_code)]
    pub(crate) fn release_after_clean_close(&self) -> io::Result<()> {
        use std::os::fd::AsRawFd;

        // SAFETY: File owns the live descriptor for the duration of this call.
        if unsafe { libc::flock(self.file.as_raw_fd(), libc::LOCK_UN) } == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    #[cfg(not(unix))]
    pub(crate) fn release_after_clean_close(&self) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "LSM ownership requires Unix flock",
        ))
    }
}
