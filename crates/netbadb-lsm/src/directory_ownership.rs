use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};

#[cfg(unix)]
#[derive(Debug)]
struct LsmLockContention {
    root: PathBuf,
    pid: u32,
    device: u64,
    inode: u64,
    source: io::Error,
}

#[cfg(unix)]
impl std::fmt::Display for LsmLockContention {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "flock LSM root {} (pid {}, device {}, inode {}) failed: {} (raw_os_error={:?})",
            self.root.display(),
            self.pid,
            self.device,
            self.inode,
            self.source,
            self.source.raw_os_error()
        )
    }
}

#[cfg(unix)]
impl std::error::Error for LsmLockContention {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

/// The LSM root directory inode is stable across Manifest and WAL rotation.
/// Holding this descriptor in shared transaction state keeps ownership alive
/// until every storage user has finished.
#[derive(Debug)]
pub(crate) struct DirectoryOwner {
    file: File,
}

/// Exclusive ownership of one existing LSM root, transferable to an LSM writer.
/// The token is not cloneable and is consumed by `LsmStorage::open_with_ownership`.
#[derive(Debug)]
pub struct LsmOwnership {
    root: PathBuf,
    owner: DirectoryOwner,
}

impl LsmOwnership {
    pub(crate) fn from_parts(root: PathBuf, owner: DirectoryOwner) -> Self {
        Self { root, owner }
    }

    /// Claims the existing root directory inode without waiting. No recovery
    /// files are changed by this call.
    pub fn acquire(root: impl AsRef<Path>) -> io::Result<Self> {
        let (root, owner) = DirectoryOwner::acquire(root.as_ref())?;
        Ok(Self { root, owner })
    }

    /// Reads Manifest identity while the directory owner remains held.
    pub fn inspect_identity(&self) -> Result<crate::LsmIdentityInspection, crate::LsmStorageError> {
        self.verify_path()?;
        let result = crate::LsmStorage::inspect_identity(&self.root)?;
        self.verify_path()?;
        Ok(result)
    }

    /// Reads prepared recovery state without applying it.
    pub fn inspect_recovery(
        &self,
        table: &netbadb_schema::TableDef,
    ) -> Result<crate::LsmRecoveryInspection, crate::LsmStorageError> {
        self.verify_path()?;
        let result = crate::LsmStorage::inspect_recovery(&self.root, table)?;
        self.verify_path()?;
        Ok(result)
    }

    #[cfg(unix)]
    /// Rejects a root pathname that no longer names the locked inode.
    pub fn verify_path(&self) -> io::Result<()> {
        use std::os::unix::fs::MetadataExt;
        let named = self.root.symlink_metadata()?;
        let held = self.owner.file.metadata()?;
        if !named.is_dir() || named.dev() != held.dev() || named.ino() != held.ino() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "LSM root changed after ownership acquisition",
            ));
        }
        Ok(())
    }

    #[cfg(not(unix))]
    /// Non-Unix platforms cannot verify this ownership capability.
    pub fn verify_path(&self) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "LSM ownership requires Unix flock",
        ))
    }

    pub(crate) fn into_parts(self) -> io::Result<(PathBuf, DirectoryOwner)> {
        self.verify_path()?;
        Ok((self.root, self.owner))
    }
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
                    LsmLockContention {
                        root,
                        pid: std::process::id(),
                        device: metadata.dev(),
                        inode: metadata.ino(),
                        source: error,
                    },
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
