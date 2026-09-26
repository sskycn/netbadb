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

impl Drop for MetadataOwner {
    fn drop(&mut self) {
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            // SAFETY: This is the final Rust owner of this open file description.
            // A forked child may temporarily retain a duplicate until exec, so
            // closing only this descriptor would leave that child's flock held.
            #[allow(unsafe_code)]
            unsafe {
                libc::flock(self._file.as_raw_fd(), libc::LOCK_UN);
            }
        }
    }
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

#[cfg(all(test, unix))]
mod tests {
    use super::MetadataOwner;
    use std::io::{Read, Write};
    use std::os::fd::AsRawFd;
    use std::os::unix::net::UnixStream;
    use std::os::unix::process::CommandExt;
    use std::process::Command;
    use std::rc::Rc;
    use std::time::Duration;

    #[test]
    #[allow(unsafe_code)]
    fn final_owner_drop_releases_a_fork_inherited_lock() {
        let root = std::env::temp_dir().join(format!(
            "netbadb-metadata-fork-owner-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("catalog");
        let owner = Rc::new(MetadataOwner::acquire(&path).unwrap());
        let retained = Rc::clone(&owner);
        let owner_fd = owner._file.as_raw_fd();
        let owner_metadata = owner._file.metadata().unwrap();
        use std::os::unix::fs::MetadataExt;

        let (mut ready_parent, ready_child) = UnixStream::pair().unwrap();
        let (mut release_parent, release_child) = UnixStream::pair().unwrap();
        ready_parent
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        release_parent
            .set_write_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let ready_fd = ready_child.as_raw_fd();
        let release_fd = release_child.as_raw_fd();
        let spawn = std::thread::spawn(move || {
            let mut command = Command::new("/usr/bin/true");
            // SAFETY: The child uses only async-signal-safe syscalls between
            // fork and exec; every descriptor is live in the parent at spawn.
            unsafe {
                command.pre_exec(move || {
                    let mut stat: libc::stat = std::mem::zeroed();
                    if libc::fstat(owner_fd, &mut stat) != 0 {
                        libc::_exit(101);
                    }
                    let event = [
                        libc::getpid() as u64,
                        owner_fd as u64,
                        stat.st_dev as u64,
                        stat.st_ino as u64,
                    ];
                    if libc::write(
                        ready_fd,
                        event.as_ptr().cast(),
                        std::mem::size_of_val(&event),
                    ) != std::mem::size_of_val(&event) as isize
                    {
                        libc::_exit(102);
                    }
                    let mut release = 0_u8;
                    if libc::read(release_fd, (&mut release as *mut u8).cast(), 1) != 1 {
                        libc::_exit(103);
                    }
                    Ok(())
                });
            }
            command.spawn().and_then(|mut child| child.wait())
        });
        let mut event_bytes = [0_u8; 32];
        let ready_result = ready_parent.read_exact(&mut event_bytes);
        if let Err(error) = ready_result {
            let _ = release_parent.write_all(&[1]);
            let child = spawn.join();
            panic!("fork child did not report inherited owner: {error}; child={child:?}");
        }
        let event = std::array::from_fn::<_, 4, _>(|index| {
            u64::from_ne_bytes(event_bytes[index * 8..(index + 1) * 8].try_into().unwrap())
        });
        eprintln!(
            "metadata fork owner: parent_pid={} child_pid={} fd={} dev={} ino={}",
            std::process::id(),
            event[0],
            event[1],
            event[2],
            event[3]
        );
        drop(owner);
        let blocked = MetadataOwner::acquire(&path);
        drop(retained);
        let reopened = MetadataOwner::acquire(&path);
        release_parent.write_all(&[1]).unwrap();
        assert!(spawn.join().unwrap().unwrap().success());
        assert_eq!(event[2], owner_metadata.dev());
        assert_eq!(event[3], owner_metadata.ino());
        assert!(
            matches!(blocked, Err(ref error) if error.kind() == std::io::ErrorKind::WouldBlock),
            "live owner was not protected: {blocked:?}"
        );
        assert!(
            reopened.is_ok(),
            "owner remained locked by fork child: {reopened:?}"
        );
        drop(reopened);
        std::fs::remove_dir_all(root).unwrap();
    }
}
