use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};

/// Resolve directory aliases once for the Heap, WAL, and status paths.
pub(crate) fn authority_path(path: &Path) -> io::Result<PathBuf> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
        .canonicalize()?;
    let name = path
        .file_name()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "Heap path has no filename"))?;
    let path = parent.join(name);
    match path.symlink_metadata() {
        Ok(metadata) if !metadata.file_type().is_file() => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Heap path must be a regular file, not a file symlink",
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(path),
        Err(error) => Err(error),
        _ => Ok(path),
    }
}

/// Lock the durable Heap data inode. It follows a staged-file rename, unlike
/// WAL generations or Core's structured owner metadata.
#[cfg(unix)]
#[allow(unsafe_code)]
pub(crate) fn lock_file(file: &File, path: &Path) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::MetadataExt;

    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.nlink() != 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Heap data file must be regular and have no hard links",
        ));
    }
    loop {
        // SAFETY: File owns the live descriptor, and flock does not retain a
        // pointer into Rust memory. Its lock remains held by this open file.
        let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if result == 0 {
            break;
        }
        let error = io::Error::last_os_error();
        if error.kind() == io::ErrorKind::Interrupted {
            continue;
        }
        if error.kind() == io::ErrorKind::WouldBlock {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                format!("Heap storage {} is already in use", path.display()),
            ));
        }
        return Err(error);
    }
    let named = path.symlink_metadata()?;
    if named.dev() != metadata.dev() || named.ino() != metadata.ino() || named.nlink() != 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Heap data path changed while acquiring ownership",
        ));
    }
    Ok(())
}

#[cfg(not(unix))]
pub(crate) fn lock_file(_file: &File, _path: &Path) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "Heap ownership requires Unix flock",
    ))
}

#[cfg(unix)]
#[allow(unsafe_code)]
pub(crate) fn unlock_file(file: &File) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    // SAFETY: File owns the live descriptor for the duration of this call.
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_UN) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(not(unix))]
pub(crate) fn unlock_file(_file: &File) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "Heap ownership requires Unix flock",
    ))
}
