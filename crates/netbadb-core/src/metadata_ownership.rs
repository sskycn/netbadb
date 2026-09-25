use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};

/// Stable lock carrier for a metadata path whose data inode can be replaced.
/// It is intentionally separate from recovery files and never performs recovery.
#[derive(Debug)]
pub(crate) struct MetadataOwner {
    _file: File,
    _path: PathBuf,
}

impl MetadataOwner {
    #[cfg(unix)]
    #[allow(unsafe_code)]
    pub(crate) fn release_after_clean_close(&self) -> io::Result<()> {
        use std::os::fd::AsRawFd;
        // SAFETY: The owned File keeps the descriptor valid for this call.
        if unsafe { libc::flock(self._file.as_raw_fd(), libc::LOCK_UN) } == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    #[cfg(not(unix))]
    pub(crate) fn release_after_clean_close(&self) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "metadata ownership requires Unix flock",
        ))
    }

    #[cfg(unix)]
    #[allow(unsafe_code)]
    pub(crate) fn acquire(path: &Path) -> io::Result<Self> {
        use std::os::fd::AsRawFd;
        use std::os::unix::fs::MetadataExt;

        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        let parent = parent.canonicalize()?;
        let name = path.file_name().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "metadata path has no filename")
        })?;
        let mut lock_name = name.to_os_string();
        lock_name.push(".core-owner");
        let lock_path = parent.join(lock_name);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)?;
        let held = file.metadata()?;
        let named = lock_path.symlink_metadata()?;
        if !held.is_file()
            || !named.is_file()
            || held.nlink() != 1
            || held.dev() != named.dev()
            || held.ino() != named.ino()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid metadata lock carrier",
            ));
        }
        loop {
            // SAFETY: File owns the live descriptor; flock retains no Rust pointer.
            if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
                break;
            }
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(error);
        }
        let named = lock_path.symlink_metadata()?;
        if !named.is_file() || held.dev() != named.dev() || held.ino() != named.ino() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "metadata lock carrier changed",
            ));
        }
        match path.symlink_metadata() {
            Ok(metadata) if !metadata.is_file() || metadata.nlink() != 1 => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "metadata file alias or non-regular path",
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        Ok(Self {
            _file: file,
            _path: lock_path,
        })
    }

    #[cfg(not(unix))]
    pub(crate) fn acquire(_path: &Path) -> io::Result<Self> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "metadata ownership requires Unix flock",
        ))
    }
}
