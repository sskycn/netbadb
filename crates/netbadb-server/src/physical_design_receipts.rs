//! Server-owned durable receipts for explicit Physical Design mutations.
//!
//! NBMR records observation around existing Core mutation authority. It is not
//! a database transaction participant and never creates physical state.

use std::error::Error;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

#[cfg(test)]
use std::cell::{Cell, RefCell};
#[cfg(test)]
use std::collections::VecDeque;

use netbadb_core::{
    Database, DatabaseError, PhysicalColumnarCandidate, PhysicalColumnarDesignLocationState,
    PhysicalColumnarDesignMode, PhysicalDesignDatabaseIdentity, PhysicalDesignEvidenceEpoch,
    PhysicalIndexCandidate, PhysicalIndexDesignNameState,
};
use netbadb_types::{ColumnId, ColumnarProjectionId, IndexId, IndexName, TableId};

use crate::{ServerPhysicalColumnarPlacementKey, ServerPhysicalColumnarPlacementKeyError};

pub const MAX_MUTATION_RECEIPT_RECORD_BYTES: usize = 64 * 1024;
pub const MAX_MUTATION_RECEIPTS_PER_READ: u32 = 128;
const MAX_COLUMN_IDS: usize = 4_096;
const MAX_PATH_BYTES: usize = 4_096;
const MAX_PLACEMENT_BYTES: usize = 128;
const V1_HEADER_BYTES: usize = 28;
const V2_HEADER_BYTES: usize = 44;
const V3_HEADER_BYTES: usize = 44;
const LEGACY_RECORD_FIXED_BYTES: usize = 4 + 1 + 3 + 8 + 4;
const V3_RECORD_HEADER_BYTES: usize = 24;
const V3_RECORD_TRAILER_BYTES: usize = 4;
const BEGIN_COMMON_BYTES: usize = 1 + 1 + 2 + 8;
const MAX_COLUMNAR_BEGIN_BODY_BYTES: usize = BEGIN_COMMON_BYTES
    + 8
    + 1
    + 3
    + 4
    + MAX_COLUMN_IDS * 4
    + 2
    + MAX_PLACEMENT_BYTES
    + 2
    + MAX_PATH_BYTES;
const MAX_COLUMNAR_BEGIN_BYTES: usize =
    V3_RECORD_HEADER_BYTES + MAX_COLUMNAR_BEGIN_BODY_BYTES + V3_RECORD_TRAILER_BYTES;
const MAX_OUTCOME_BODY_BYTES: usize = 1 + 3 + 8;
const MAX_RECOVERED_OUTCOME_BYTES: usize =
    V3_RECORD_HEADER_BYTES + MAX_OUTCOME_BODY_BYTES + V3_RECORD_TRAILER_BYTES;
const MIN_FILE_BYTES: u64 =
    (V3_HEADER_BYTES + MAX_COLUMNAR_BEGIN_BYTES + MAX_RECOVERED_OUTCOME_BYTES) as u64;
const V1_VERSION: u16 = 1;
const V2_VERSION: u16 = 2;
const CURRENT_VERSION: u16 = 3;
const BEGIN_TAG: u8 = 1;
const OUTCOME_TAG: u8 = 2;

/// Runtime-ready configuration for one bounded NBMR v3 journal.
///
/// Construction freezes an absolute path through its canonical existing
/// parent, but deliberately does not create or open the final file.
#[derive(Clone, PartialEq, Eq)]
pub struct ServerPhysicalDesignMutationReceiptConfig {
    path: PathBuf,
    max_file_bytes: u64,
}

impl fmt::Debug for ServerPhysicalDesignMutationReceiptConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ServerPhysicalDesignMutationReceiptConfig")
            .field("path", &self.path)
            .field("max_file_bytes", &self.max_file_bytes)
            .finish()
    }
}

impl ServerPhysicalDesignMutationReceiptConfig {
    pub fn new(
        path: impl AsRef<Path>,
        max_file_bytes: u64,
    ) -> Result<Self, ServerPhysicalDesignMutationReceiptConfigError> {
        if max_file_bytes < MIN_FILE_BYTES {
            return Err(
                ServerPhysicalDesignMutationReceiptConfigError::MaxFileBytesTooSmall {
                    supplied: max_file_bytes,
                    minimum: MIN_FILE_BYTES,
                },
            );
        }
        let supplied = path.as_ref();
        let absolute = if supplied.is_absolute() {
            supplied.to_path_buf()
        } else {
            std::env::current_dir()
                .map_err(ServerPhysicalDesignMutationReceiptConfigError::CurrentDirectory)?
                .join(supplied)
        };
        let file_name = absolute.file_name().ok_or_else(|| {
            ServerPhysicalDesignMutationReceiptConfigError::MissingFileName(absolute.clone())
        })?;
        let parent = absolute.parent().ok_or_else(|| {
            ServerPhysicalDesignMutationReceiptConfigError::MissingFileName(absolute.clone())
        })?;
        let canonical_parent = fs::canonicalize(parent).map_err(|source| {
            ServerPhysicalDesignMutationReceiptConfigError::ParentResolution {
                path: parent.to_path_buf(),
                source,
            }
        })?;
        if !canonical_parent.is_dir() {
            return Err(
                ServerPhysicalDesignMutationReceiptConfigError::ParentNotDirectory(
                    canonical_parent,
                ),
            );
        }
        let path = canonical_parent.join(file_name);
        validate_final_path(&path).map_err(ServerPhysicalDesignMutationReceiptConfigError::Path)?;
        Ok(Self {
            path,
            max_file_bytes,
        })
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    #[must_use]
    pub const fn max_file_bytes(&self) -> u64 {
        self.max_file_bytes
    }
}

#[derive(Debug)]
pub enum ServerPhysicalDesignMutationReceiptConfigError {
    CurrentDirectory(io::Error),
    MissingFileName(PathBuf),
    ParentResolution { path: PathBuf, source: io::Error },
    ParentNotDirectory(PathBuf),
    Path(ServerPhysicalDesignMutationReceiptPathError),
    MaxFileBytesTooSmall { supplied: u64, minimum: u64 },
}

impl fmt::Display for ServerPhysicalDesignMutationReceiptConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CurrentDirectory(error) => {
                write!(formatter, "failed to resolve current directory: {error}")
            }
            Self::MissingFileName(path) => write!(
                formatter,
                "receipt journal path {} has no file name",
                path.display()
            ),
            Self::ParentResolution { path, source } => write!(
                formatter,
                "failed to resolve receipt journal parent {}: {source}",
                path.display()
            ),
            Self::ParentNotDirectory(path) => write!(
                formatter,
                "receipt journal parent {} is not a directory",
                path.display()
            ),
            Self::Path(error) => error.fmt(formatter),
            Self::MaxFileBytesTooSmall { supplied, minimum } => write!(
                formatter,
                "receipt journal max_file_bytes {supplied} is below required minimum {minimum}"
            ),
        }
    }
}

impl Error for ServerPhysicalDesignMutationReceiptConfigError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::CurrentDirectory(error) => Some(error),
            Self::ParentResolution { source, .. } => Some(source),
            Self::Path(error) => Some(error),
            Self::MissingFileName(_)
            | Self::ParentNotDirectory(_)
            | Self::MaxFileBytesTooSmall { .. } => None,
        }
    }
}

#[derive(Debug)]
pub enum ServerPhysicalDesignMutationReceiptPathError {
    Metadata { path: PathBuf, source: io::Error },
    ExistingObjectNotRegular(PathBuf),
}

impl fmt::Display for ServerPhysicalDesignMutationReceiptPathError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Metadata { path, source } => write!(
                formatter,
                "failed to inspect receipt journal path {}: {source}",
                path.display()
            ),
            Self::ExistingObjectNotRegular(path) => write!(
                formatter,
                "receipt journal path {} is not a regular file",
                path.display()
            ),
        }
    }
}

impl Error for ServerPhysicalDesignMutationReceiptPathError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Metadata { source, .. } => Some(source),
            Self::ExistingObjectNotRegular(_) => None,
        }
    }
}

fn validate_final_path(path: &Path) -> Result<(), ServerPhysicalDesignMutationReceiptPathError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() => Ok(()),
        Ok(_) => Err(
            ServerPhysicalDesignMutationReceiptPathError::ExistingObjectNotRegular(
                path.to_path_buf(),
            ),
        ),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(ServerPhysicalDesignMutationReceiptPathError::Metadata {
            path: path.to_path_buf(),
            source,
        }),
    }
}

fn generate_journal_incarnation() -> Result<
    ServerPhysicalDesignMutationReceiptJournalIncarnation,
    ServerPhysicalDesignMutationReceiptJournalError,
> {
    #[cfg(test)]
    if TEST_RANDOMNESS_FAILURE.with(|failure| failure.replace(false)) {
        return Err(ServerPhysicalDesignMutationReceiptJournalError::Randomness(
            getrandom::Error::UNSUPPORTED,
        ));
    }
    #[cfg(test)]
    if let Some(bytes) = TEST_JOURNAL_INCARNATIONS.with(|values| values.borrow_mut().pop_front()) {
        return nonzero_journal_incarnation(bytes);
    }

    let mut bytes = [0_u8; 16];
    getrandom::getrandom(&mut bytes)
        .map_err(ServerPhysicalDesignMutationReceiptJournalError::Randomness)?;
    nonzero_journal_incarnation(bytes)
}

fn nonzero_journal_incarnation(
    bytes: [u8; 16],
) -> Result<
    ServerPhysicalDesignMutationReceiptJournalIncarnation,
    ServerPhysicalDesignMutationReceiptJournalError,
> {
    if bytes == [0; 16] {
        return Err(
            ServerPhysicalDesignMutationReceiptJournalError::GeneratedZeroJournalIncarnation,
        );
    }
    Ok(ServerPhysicalDesignMutationReceiptJournalIncarnation(bytes))
}

fn create_v3_journal(
    config: &ServerPhysicalDesignMutationReceiptConfig,
    database_incarnation: [u8; 16],
) -> Result<File, ServerPhysicalDesignMutationReceiptJournalError> {
    let journal_incarnation = generate_journal_incarnation()?;
    let mut temp = create_owned_temp(config.path())?;
    let header = encode_v3_header(database_incarnation, journal_incarnation);
    #[cfg(test)]
    inject_publication_failure(TestPublicationFailure::TemporaryCreated, &temp.path)?;
    #[cfg(test)]
    if take_publication_failure(TestPublicationFailure::PartialHeaderWritten) {
        temp.file_mut()
            .write_all(&header[..header.len() / 2])
            .map_err(|source| io_error("partial fresh header write", &temp.path, source))?;
        return Err(io_error(
            "injected fresh publication failure",
            &temp.path,
            io::Error::other("injected fresh publication failure"),
        ));
    }
    temp.file_mut()
        .write_all(&header)
        .map_err(|source| io_error("fresh header write", &temp.path, source))?;
    #[cfg(test)]
    inject_publication_failure(TestPublicationFailure::FullHeaderWritten, &temp.path)?;
    temp.file_mut()
        .sync_all()
        .map_err(|source| io_error("fresh header sync", &temp.path, source))?;
    #[cfg(test)]
    inject_publication_failure(TestPublicationFailure::TemporarySynced, &temp.path)?;
    publish_new(&mut temp, config.path())?;
    temp.into_published_file()
}

#[cfg(unix)]
#[allow(unsafe_code)]
fn lock_journal_exclusive(
    file: &File,
) -> Result<(), ServerPhysicalDesignMutationReceiptJournalError> {
    use std::os::fd::AsRawFd;

    loop {
        // SAFETY: `file` owns a live descriptor for the journal inode for the
        // duration of this call. `flock` neither retains the pointer nor
        // accesses Rust memory.
        let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if result == 0 {
            return Ok(());
        }
        let source = io::Error::last_os_error();
        if source.kind() == io::ErrorKind::Interrupted {
            continue;
        }
        let raw = source.raw_os_error();
        if source.kind() == io::ErrorKind::WouldBlock
            || raw == Some(libc::EAGAIN)
            || raw == Some(libc::EWOULDBLOCK)
        {
            return Err(ServerPhysicalDesignMutationReceiptJournalError::AlreadyInUse);
        }
        return Err(ServerPhysicalDesignMutationReceiptJournalError::Lock(
            source,
        ));
    }
}

fn secure_open_existing(
    config: &ServerPhysicalDesignMutationReceiptConfig,
    write: bool,
) -> Result<File, ServerPhysicalDesignMutationReceiptJournalError> {
    validate_final_path(config.path())
        .map_err(ServerPhysicalDesignMutationReceiptJournalError::Path)?;
    #[cfg(test)]
    inject_existing_open_replacement(config.path())?;
    #[cfg(not(unix))]
    {
        let _ = write;
        return Err(ServerPhysicalDesignMutationReceiptJournalError::FilesystemSafetyUnavailable);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        let mut options = OpenOptions::new();
        options
            .read(true)
            .write(write)
            .custom_flags(libc::O_NOFOLLOW);
        options
            .open(config.path())
            .map_err(|source| io_error("secure open", config.path(), source))
    }
}

fn read_open_file(
    file: &mut File,
    config: &ServerPhysicalDesignMutationReceiptConfig,
) -> Result<Vec<u8>, ServerPhysicalDesignMutationReceiptJournalError> {
    let metadata = file
        .metadata()
        .map_err(|source| io_error("metadata", config.path(), source))?;
    if !metadata.file_type().is_file() {
        return Err(ServerPhysicalDesignMutationReceiptJournalError::Path(
            ServerPhysicalDesignMutationReceiptPathError::ExistingObjectNotRegular(
                config.path().to_path_buf(),
            ),
        ));
    }
    if metadata.len() > config.max_file_bytes() {
        return Err(
            ServerPhysicalDesignMutationReceiptJournalError::FileTooLarge {
                bytes: metadata.len(),
                maximum: config.max_file_bytes(),
            },
        );
    }
    let byte_len = usize::try_from(metadata.len()).map_err(|_| {
        ServerPhysicalDesignMutationReceiptJournalError::FileTooLarge {
            bytes: metadata.len(),
            maximum: config.max_file_bytes(),
        }
    })?;
    let mut bytes = Vec::with_capacity(byte_len);
    file.seek(SeekFrom::Start(0))
        .map_err(|source| io_error("seek", config.path(), source))?;
    file.take(config.max_file_bytes().saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|source| io_error("read", config.path(), source))?;
    if bytes.len() as u64 > config.max_file_bytes() {
        return Err(
            ServerPhysicalDesignMutationReceiptJournalError::FileTooLarge {
                bytes: bytes.len() as u64,
                maximum: config.max_file_bytes(),
            },
        );
    }
    Ok(bytes)
}

fn verify_opened_regular(
    file: &File,
    config: &ServerPhysicalDesignMutationReceiptConfig,
) -> Result<(), ServerPhysicalDesignMutationReceiptJournalError> {
    let metadata = file
        .metadata()
        .map_err(|source| io_error("metadata", config.path(), source))?;
    if metadata.file_type().is_file() {
        Ok(())
    } else {
        Err(ServerPhysicalDesignMutationReceiptJournalError::Path(
            ServerPhysicalDesignMutationReceiptPathError::ExistingObjectNotRegular(
                config.path().to_path_buf(),
            ),
        ))
    }
}

fn journal_version(bytes: &[u8]) -> Result<u16, ServerPhysicalDesignMutationReceiptJournalError> {
    if bytes.len() < 8 {
        return Err(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
            "truncated header",
        ));
    }
    if &bytes[..4] != b"NBMR" {
        return Err(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
            "bad header magic",
        ));
    }
    Ok(u16::from_le_bytes([bytes[4], bytes[5]]))
}

struct OwnedTemp {
    file: Option<File>,
    path: PathBuf,
    published: bool,
}

impl OwnedTemp {
    fn file_mut(&mut self) -> &mut File {
        self.file
            .as_mut()
            .expect("owned temporary retains its file")
    }

    fn into_published_file(
        mut self,
    ) -> Result<File, ServerPhysicalDesignMutationReceiptJournalError> {
        if !self.published {
            return Err(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
                "temporary journal was not published",
            ));
        }
        self.file
            .take()
            .ok_or(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
                "published temporary journal lost its file",
            ))
    }
}

impl Drop for OwnedTemp {
    fn drop(&mut self) {
        if !self.published {
            let _ = fs::remove_file(&self.path);
        }
    }
}

fn create_owned_temp(
    final_path: &Path,
) -> Result<OwnedTemp, ServerPhysicalDesignMutationReceiptJournalError> {
    let parent =
        final_path
            .parent()
            .ok_or(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
                "journal path has no parent",
            ))?;
    let name =
        final_path
            .file_name()
            .ok_or(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
                "journal path has no file name",
            ))?;
    for _ in 0..16 {
        let mut random = [0_u8; 16];
        getrandom::getrandom(&mut random)
            .map_err(ServerPhysicalDesignMutationReceiptJournalError::Randomness)?;
        let mut suffix = String::with_capacity(32);
        for byte in random {
            use fmt::Write as _;
            write!(&mut suffix, "{byte:02x}").map_err(|_| {
                ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
                    "temporary name formatting failed",
                )
            })?;
        }
        let mut temp_name = name.to_os_string();
        temp_name.push(format!(".tmp-{suffix}"));
        let path = parent.join(temp_name);
        let mut options = OpenOptions::new();
        options.read(true).write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        match options.open(&path) {
            Ok(file) => {
                #[cfg(unix)]
                lock_journal_exclusive(&file)?;
                return Ok(OwnedTemp {
                    file: Some(file),
                    path,
                    published: false,
                });
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(source) => return Err(io_error("create owned temporary journal", &path, source)),
        }
    }
    Err(ServerPhysicalDesignMutationReceiptJournalError::TemporaryNameExhausted)
}

fn publish_new(
    temp: &mut OwnedTemp,
    final_path: &Path,
) -> Result<(), ServerPhysicalDesignMutationReceiptJournalError> {
    fs::hard_link(&temp.path, final_path)
        .map_err(|source| io_error("publish fresh journal", final_path, source))?;
    #[cfg(test)]
    inject_publication_failure(
        TestPublicationFailure::PublishedBeforeParentSync,
        final_path,
    )?;
    sync_parent(final_path)?;
    fs::remove_file(&temp.path)
        .map_err(|source| io_error("remove published journal temporary", &temp.path, source))?;
    temp.published = true;
    sync_parent(final_path)
}

fn migrate_legacy(
    config: &ServerPhysicalDesignMutationReceiptConfig,
    database_incarnation: [u8; 16],
    version: u16,
    legacy_bytes: &[u8],
    source: &File,
) -> Result<File, ServerPhysicalDesignMutationReceiptJournalError> {
    let (header_bytes, journal_incarnation) = match version {
        V1_VERSION => {
            validate_v1_header(legacy_bytes, database_incarnation)?;
            (V1_HEADER_BYTES, generate_journal_incarnation()?)
        }
        V2_VERSION => (
            V2_HEADER_BYTES,
            validate_v2_header(legacy_bytes, database_incarnation)?,
        ),
        _ => {
            return Err(
                ServerPhysicalDesignMutationReceiptJournalError::UnsupportedVersion(version),
            );
        }
    };
    let decoded = decode_legacy_records(legacy_bytes, header_bytes)?;
    if decoded.valid_bytes != legacy_bytes.len() {
        return Err(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
            "ambiguous legacy partial tail",
        ));
    }
    let mut bytes = encode_v3_header(database_incarnation, journal_incarnation).to_vec();
    for record in &decoded.records {
        bytes.extend_from_slice(&encode_parsed_v3(record)?);
    }
    let required = bytes
        .len()
        .checked_add(
            decoded
                .unresolved
                .as_ref()
                .map_or(0, |_| MAX_RECOVERED_OUTCOME_BYTES),
        )
        .ok_or(ServerPhysicalDesignMutationReceiptJournalError::CapacityExceeded)?;
    let required = u64::try_from(required)
        .map_err(|_| ServerPhysicalDesignMutationReceiptJournalError::CapacityExceeded)?;
    if required > config.max_file_bytes() {
        return Err(
            ServerPhysicalDesignMutationReceiptJournalError::MigrationCapacityExceeded {
                migrated_bytes: required,
                maximum: config.max_file_bytes(),
            },
        );
    }
    let mut temp = create_owned_temp(config.path())?;
    temp.file_mut()
        .write_all(&bytes)
        .map_err(|source| io_error("write migration temporary", &temp.path, source))?;
    temp.file_mut()
        .sync_all()
        .map_err(|source| io_error("sync migration temporary", &temp.path, source))?;
    #[cfg(test)]
    inject_migration_failure(TestMigrationFailure::AfterTemporarySync, &temp.path)?;
    #[cfg(test)]
    migration_checkpoint();
    #[cfg(test)]
    inject_migration_final_replacement(config.path())?;
    verify_final_path_is_opened_inode(source, config.path())?;
    fs::rename(&temp.path, config.path())
        .map_err(|source| io_error("publish v3 migration", config.path(), source))?;
    temp.published = true;
    #[cfg(test)]
    inject_migration_failure(TestMigrationFailure::AfterRename, config.path())?;
    sync_parent(config.path())?;
    temp.into_published_file()
}

#[cfg(unix)]
fn verify_final_path_is_opened_inode(
    source: &File,
    final_path: &Path,
) -> Result<(), ServerPhysicalDesignMutationReceiptJournalError> {
    use std::os::unix::fs::MetadataExt;

    let opened = source
        .metadata()
        .map_err(|error| io_error("journal handle metadata", final_path, error))?;
    let current = fs::symlink_metadata(final_path)
        .map_err(|error| io_error("journal final path metadata", final_path, error))?;
    if !current.file_type().is_file()
        || opened.dev() != current.dev()
        || opened.ino() != current.ino()
    {
        return Err(ServerPhysicalDesignMutationReceiptJournalError::FinalPathChanged);
    }
    Ok(())
}

#[cfg(not(unix))]
fn verify_final_path_is_opened_inode(
    _source: &File,
    _final_path: &Path,
) -> Result<(), ServerPhysicalDesignMutationReceiptJournalError> {
    Err(ServerPhysicalDesignMutationReceiptJournalError::FilesystemSafetyUnavailable)
}

#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum TestMigrationFailure {
    AfterTemporarySync,
    AfterRename,
}

#[cfg(test)]
struct TestMigrationCheckpoint {
    reached: std::sync::mpsc::SyncSender<()>,
    resume: std::sync::mpsc::Receiver<()>,
}

#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum TestPublicationFailure {
    TemporaryCreated,
    PartialHeaderWritten,
    FullHeaderWritten,
    TemporarySynced,
    PublishedBeforeParentSync,
}

#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum TestJournalIoFailure {
    BeginAfterFullWriteBeforeSync,
    OutcomeAfterPartialWrite,
    OutcomeAfterFullWriteBeforeSync,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum JournalRecordKind {
    Begin,
    Outcome,
}

#[cfg(all(test, unix))]
#[derive(Clone)]
struct TestExistingOpenReplacement {
    target: PathBuf,
}

#[cfg(test)]
thread_local! {
    static TEST_JOURNAL_INCARNATIONS: RefCell<VecDeque<[u8; 16]>> = const { RefCell::new(VecDeque::new()) };
    static TEST_MIGRATION_FAILURE: RefCell<Option<TestMigrationFailure>> = const { RefCell::new(None) };
    static TEST_RANDOMNESS_FAILURE: Cell<bool> = const { Cell::new(false) };
    static TEST_PUBLICATION_FAILURE: RefCell<Option<TestPublicationFailure>> = const { RefCell::new(None) };
    static TEST_MIGRATION_FINAL_REPLACEMENT: RefCell<Option<Vec<u8>>> = const { RefCell::new(None) };
    static TEST_MIGRATION_CHECKPOINT: RefCell<Option<TestMigrationCheckpoint>> = const { RefCell::new(None) };
    #[cfg(unix)]
    static TEST_EXISTING_OPEN_REPLACEMENT: RefCell<Option<TestExistingOpenReplacement>> = const { RefCell::new(None) };
}

#[cfg(test)]
fn migration_checkpoint() {
    TEST_MIGRATION_CHECKPOINT.with(|value| {
        if let Some(checkpoint) = value.borrow_mut().take() {
            checkpoint.reached.send(()).unwrap();
            checkpoint.resume.recv().unwrap();
        }
    });
}

#[cfg(test)]
fn inject_migration_final_replacement(
    final_path: &Path,
) -> Result<(), ServerPhysicalDesignMutationReceiptJournalError> {
    let replacement = TEST_MIGRATION_FINAL_REPLACEMENT.with(|value| value.borrow_mut().take());
    if let Some(replacement) = replacement {
        fs::remove_file(final_path)
            .map_err(|source| io_error("injected migration path removal", final_path, source))?;
        fs::write(final_path, replacement).map_err(|source| {
            io_error("injected migration path replacement", final_path, source)
        })?;
    }
    Ok(())
}

#[cfg(test)]
fn take_publication_failure(point: TestPublicationFailure) -> bool {
    TEST_PUBLICATION_FAILURE.with(|value| {
        let mut value = value.borrow_mut();
        if *value == Some(point) {
            value.take();
            true
        } else {
            false
        }
    })
}

#[cfg(test)]
fn inject_publication_failure(
    point: TestPublicationFailure,
    path: &Path,
) -> Result<(), ServerPhysicalDesignMutationReceiptJournalError> {
    if take_publication_failure(point) {
        Err(io_error(
            "injected fresh publication failure",
            path,
            io::Error::other("injected fresh publication failure"),
        ))
    } else {
        Ok(())
    }
}

#[cfg(test)]
fn inject_existing_open_replacement(
    path: &Path,
) -> Result<(), ServerPhysicalDesignMutationReceiptJournalError> {
    #[cfg(unix)]
    if let Some(replacement) =
        TEST_EXISTING_OPEN_REPLACEMENT.with(|value| value.borrow_mut().take())
    {
        use std::os::unix::fs::symlink;

        fs::remove_file(path)
            .map_err(|source| io_error("injected path replacement", path, source))?;
        symlink(&replacement.target, path)
            .map_err(|source| io_error("injected symlink replacement", path, source))?;
    }
    Ok(())
}

#[cfg(test)]
fn inject_migration_failure(
    point: TestMigrationFailure,
    path: &Path,
) -> Result<(), ServerPhysicalDesignMutationReceiptJournalError> {
    let fail = TEST_MIGRATION_FAILURE.with(|value| {
        let mut value = value.borrow_mut();
        if *value == Some(point) {
            value.take();
            true
        } else {
            false
        }
    });
    if fail {
        Err(io_error(
            "injected migration failure",
            path,
            io::Error::other("injected migration failure"),
        ))
    } else {
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ServerPhysicalDesignMutationReceiptId(pub u64);

/// Stable namespace of one durable NBMR receipt history.
///
/// The value is generated from OS randomness when a current journal is first
/// created or a v1 journal is migrated. It is neither authentication nor a
/// database or runtime identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ServerPhysicalDesignMutationReceiptJournalIncarnation([u8; 16]);

impl ServerPhysicalDesignMutationReceiptJournalIncarnation {
    pub fn new(
        bytes: [u8; 16],
    ) -> Result<Self, ServerPhysicalDesignMutationReceiptJournalIncarnationError> {
        if bytes == [0; 16] {
            return Err(ServerPhysicalDesignMutationReceiptJournalIncarnationError::Zero);
        }
        Ok(Self(bytes))
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

/// Stable identity of one receipt in one durable NBMR journal namespace.
///
/// A reference identifies one receipt. Unlike a cursor, it does not imply
/// continuation semantics and never authorizes replay or mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ServerPhysicalDesignMutationReceiptReference {
    journal_incarnation: ServerPhysicalDesignMutationReceiptJournalIncarnation,
    receipt_id: ServerPhysicalDesignMutationReceiptId,
}

impl ServerPhysicalDesignMutationReceiptReference {
    pub fn new(
        journal_incarnation: ServerPhysicalDesignMutationReceiptJournalIncarnation,
        receipt_id: ServerPhysicalDesignMutationReceiptId,
    ) -> Result<Self, ServerPhysicalDesignMutationReceiptCursorError> {
        if receipt_id.0 == 0 {
            return Err(ServerPhysicalDesignMutationReceiptCursorError::ZeroReceiptId);
        }
        Ok(Self {
            journal_incarnation,
            receipt_id,
        })
    }

    #[must_use]
    pub const fn journal_incarnation(
        &self,
    ) -> ServerPhysicalDesignMutationReceiptJournalIncarnation {
        self.journal_incarnation
    }

    #[must_use]
    pub const fn receipt_id(&self) -> ServerPhysicalDesignMutationReceiptId {
        self.receipt_id
    }

    #[must_use]
    pub const fn to_cursor(self) -> ServerPhysicalDesignMutationReceiptCursor {
        ServerPhysicalDesignMutationReceiptCursor {
            journal_incarnation: self.journal_incarnation,
            receipt_id: self.receipt_id,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerPhysicalDesignMutationReceiptJournalIncarnationError {
    Zero,
}

impl fmt::Display for ServerPhysicalDesignMutationReceiptJournalIncarnationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Zero => formatter.write_str("receipt journal incarnation must be nonzero"),
        }
    }
}

impl Error for ServerPhysicalDesignMutationReceiptJournalIncarnationError {}

/// Persistable position inside exactly one durable NBMR journal namespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ServerPhysicalDesignMutationReceiptCursor {
    journal_incarnation: ServerPhysicalDesignMutationReceiptJournalIncarnation,
    receipt_id: ServerPhysicalDesignMutationReceiptId,
}

impl ServerPhysicalDesignMutationReceiptCursor {
    pub fn new(
        journal_incarnation: ServerPhysicalDesignMutationReceiptJournalIncarnation,
        receipt_id: ServerPhysicalDesignMutationReceiptId,
    ) -> Result<Self, ServerPhysicalDesignMutationReceiptCursorError> {
        if receipt_id.0 == 0 {
            return Err(ServerPhysicalDesignMutationReceiptCursorError::ZeroReceiptId);
        }
        Ok(Self {
            journal_incarnation,
            receipt_id,
        })
    }

    #[must_use]
    pub const fn journal_incarnation(
        &self,
    ) -> ServerPhysicalDesignMutationReceiptJournalIncarnation {
        self.journal_incarnation
    }

    #[must_use]
    pub const fn receipt_id(&self) -> ServerPhysicalDesignMutationReceiptId {
        self.receipt_id
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerPhysicalDesignMutationReceiptCursorError {
    ZeroReceiptId,
}

impl fmt::Display for ServerPhysicalDesignMutationReceiptCursorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroReceiptId => formatter.write_str("receipt cursor ID must be nonzero"),
        }
    }
}

impl Error for ServerPhysicalDesignMutationReceiptCursorError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerPhysicalDesignMutationSource {
    Programmatic,
    LocalOperator,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerPhysicalDesignMutationTarget {
    Index {
        table_id: TableId,
        column_id: ColumnId,
        index_name: IndexName,
    },
    Columnar {
        table_id: TableId,
        columns: Vec<ColumnId>,
        mode: PhysicalColumnarDesignMode,
        placement: ServerPhysicalColumnarPlacementKey,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerPhysicalDesignMutationReceiptOutcome {
    Pending,
    CreatedIndex { index_id: IndexId },
    CreatedColumnar { projection_id: ColumnarProjectionId },
    AlreadyAppliedIndex { index_id: IndexId },
    AlreadyAppliedColumnar { projection_id: ColumnarProjectionId },
    AlreadyCovered,
    Rejected,
    Failed,
    RecoveredAppliedIndex { index_id: IndexId },
    RecoveredAppliedColumnar { projection_id: ColumnarProjectionId },
    RecoveredNotApplied,
    RecoveredConflict,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerPhysicalDesignMutationReceipt {
    pub id: ServerPhysicalDesignMutationReceiptId,
    pub source: ServerPhysicalDesignMutationSource,
    pub evidence_epoch: PhysicalDesignEvidenceEpoch,
    pub target: ServerPhysicalDesignMutationTarget,
    pub outcome: ServerPhysicalDesignMutationReceiptOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerPhysicalDesignMutationReceiptPage {
    pub receipts: Vec<ServerPhysicalDesignMutationReceipt>,
    pub next_after: Option<ServerPhysicalDesignMutationReceiptId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerPhysicalDesignMutationReceiptScopedPage {
    pub journal_incarnation: ServerPhysicalDesignMutationReceiptJournalIncarnation,
    pub receipts: Vec<ServerPhysicalDesignMutationReceipt>,
    pub next_after: Option<ServerPhysicalDesignMutationReceiptCursor>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServerPhysicalDesignMutationReceiptStatus {
    pub journal_incarnation: ServerPhysicalDesignMutationReceiptJournalIncarnation,
    pub recovery_required: bool,
    pub latest_receipt_id: Option<ServerPhysicalDesignMutationReceiptId>,
    pub max_receipts_per_read: u32,
}

#[derive(Debug)]
pub enum ServerPhysicalDesignMutationReceiptControlError {
    NotEnabled,
    InvalidLimit {
        supplied: u32,
        maximum: u32,
    },
    JournalChanged {
        expected: ServerPhysicalDesignMutationReceiptJournalIncarnation,
        actual: ServerPhysicalDesignMutationReceiptJournalIncarnation,
    },
    RecoveryRequired,
    Journal(ServerPhysicalDesignMutationReceiptJournalError),
    ServerStopped,
}

impl fmt::Display for ServerPhysicalDesignMutationReceiptControlError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotEnabled => {
                formatter.write_str("physical-design mutation receipts are not enabled")
            }
            Self::InvalidLimit { supplied, maximum } => write!(
                formatter,
                "receipt read limit {supplied} must be between 1 and {maximum}"
            ),
            Self::JournalChanged { expected, actual } => write!(
                formatter,
                "receipt cursor journal changed from {expected:?} to {actual:?}"
            ),
            Self::RecoveryRequired => formatter.write_str(
                "physical-design mutation receipt journal requires restart reconciliation",
            ),
            Self::Journal(error) => error.fmt(formatter),
            Self::ServerStopped => formatter.write_str("server physical-design control is stopped"),
        }
    }
}

impl Error for ServerPhysicalDesignMutationReceiptControlError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Journal(error) => Some(error),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub enum ServerPhysicalDesignMutationReceiptJournalError {
    Path(ServerPhysicalDesignMutationReceiptPathError),
    Io {
        operation: &'static str,
        path: PathBuf,
        source: io::Error,
    },
    UnsupportedVersion(u16),
    Corrupt(&'static str),
    DatabaseIdentityMismatch,
    Randomness(getrandom::Error),
    GeneratedZeroJournalIncarnation,
    TemporaryNameExhausted,
    FilesystemSafetyUnavailable,
    AlreadyInUse,
    Lock(io::Error),
    FinalPathChanged,
    FileTooLarge {
        bytes: u64,
        maximum: u64,
    },
    CapacityExceeded,
    MigrationCapacityExceeded {
        migrated_bytes: u64,
        maximum: u64,
    },
    ReceiptIdExhausted,
    ColumnCountExceeded {
        count: usize,
        maximum: usize,
    },
    PathNotUtf8,
    PathTooLong {
        bytes: usize,
        maximum: usize,
    },
    InvalidIndexName,
    InvalidPlacement(ServerPhysicalColumnarPlacementKeyError),
    Reconciliation(DatabaseError),
}

#[derive(Debug)]
pub enum ServerPhysicalDesignMutationReceiptStartupError {
    PhysicalDesignRequired,
    OperatorPolicyMismatch,
    DatabaseIdentity(DatabaseError),
    Journal(ServerPhysicalDesignMutationReceiptJournalError),
}

impl fmt::Display for ServerPhysicalDesignMutationReceiptStartupError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PhysicalDesignRequired => formatter.write_str(
                "physical-design mutation receipts require a configured physical-design advisor",
            ),
            Self::OperatorPolicyMismatch => formatter.write_str(
                "operator receipt-read permission pins the manifest mutation_receipts journal",
            ),
            Self::DatabaseIdentity(error) => {
                write!(
                    formatter,
                    "failed to read database identity for NBMR: {error}"
                )
            }
            Self::Journal(error) => error.fmt(formatter),
        }
    }
}

impl Error for ServerPhysicalDesignMutationReceiptStartupError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::PhysicalDesignRequired | Self::OperatorPolicyMismatch => None,
            Self::DatabaseIdentity(error) => Some(error),
            Self::Journal(error) => Some(error),
        }
    }
}

impl fmt::Display for ServerPhysicalDesignMutationReceiptJournalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Path(error) => error.fmt(formatter),
            Self::Io {
                operation,
                path,
                source,
            } => write!(
                formatter,
                "receipt journal {operation} failed for {}: {source}",
                path.display()
            ),
            Self::UnsupportedVersion(version) => {
                write!(formatter, "unsupported NBMR version {version}")
            }
            Self::Corrupt(reason) => write!(formatter, "NBMR journal corrupt: {reason}"),
            Self::DatabaseIdentityMismatch => {
                formatter.write_str("NBMR journal belongs to another database installation")
            }
            Self::Randomness(error) => {
                write!(
                    formatter,
                    "failed to generate NBMR journal incarnation: {error}"
                )
            }
            Self::GeneratedZeroJournalIncarnation => {
                formatter.write_str("generated NBMR journal incarnation was zero")
            }
            Self::TemporaryNameExhausted => {
                formatter.write_str("failed to allocate a unique NBMR temporary name")
            }
            Self::FilesystemSafetyUnavailable => formatter
                .write_str("secure no-follow receipt journal open is unavailable on this platform"),
            Self::AlreadyInUse => formatter.write_str(
                "physical-design mutation receipt journal is already owned by another active runtime",
            ),
            Self::Lock(source) => {
                write!(formatter, "failed to acquire exclusive NBMR journal ownership: {source}")
            }
            Self::FinalPathChanged => formatter.write_str(
                "NBMR journal final path no longer identifies the locked journal",
            ),
            Self::FileTooLarge { bytes, maximum } => write!(
                formatter,
                "NBMR journal is {bytes} bytes, exceeding configured maximum {maximum}"
            ),
            Self::CapacityExceeded => formatter.write_str("NBMR journal capacity is exhausted"),
            Self::MigrationCapacityExceeded {
                migrated_bytes,
                maximum,
            } => write!(
                formatter,
                "migrated NBMR v3 journal plus required recovery reserve would be {migrated_bytes} bytes, exceeding configured maximum {maximum}"
            ),
            Self::ReceiptIdExhausted => formatter.write_str("NBMR receipt IDs are exhausted"),
            Self::ColumnCountExceeded { count, maximum } => write!(
                formatter,
                "receipt Columnar target has {count} columns; maximum is {maximum}"
            ),
            Self::PathNotUtf8 => {
                formatter.write_str("receipt Columnar target path is not valid UTF-8")
            }
            Self::PathTooLong { bytes, maximum } => write!(
                formatter,
                "receipt Columnar target path is {bytes} bytes; maximum is {maximum}"
            ),
            Self::InvalidIndexName => {
                formatter.write_str("NBMR journal contains an invalid index name")
            }
            Self::InvalidPlacement(error) => write!(
                formatter,
                "NBMR journal contains an invalid placement key: {error}"
            ),
            Self::Reconciliation(error) => write!(formatter, "NBMR reconciliation failed: {error}"),
        }
    }
}

impl Error for ServerPhysicalDesignMutationReceiptJournalError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Path(error) => Some(error),
            Self::Io { source, .. } => Some(source),
            Self::Lock(source) => Some(source),
            Self::InvalidPlacement(error) => Some(error),
            Self::Reconciliation(error) => Some(error),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MutationReceiptTarget {
    Index {
        candidate: PhysicalIndexCandidate,
        index_name: IndexName,
    },
    Columnar {
        candidate: PhysicalColumnarCandidate,
        mode: PhysicalColumnarDesignMode,
        placement: ServerPhysicalColumnarPlacementKey,
        directory: PathBuf,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MutationReceiptBegin {
    id: ServerPhysicalDesignMutationReceiptId,
    source: ServerPhysicalDesignMutationSource,
    evidence_epoch: PhysicalDesignEvidenceEpoch,
    target: MutationReceiptTarget,
}

pub(crate) struct ServerPhysicalDesignMutationReceiptJournal {
    config: ServerPhysicalDesignMutationReceiptConfig,
    journal_incarnation: ServerPhysicalDesignMutationReceiptJournalIncarnation,
    file: File,
    file_len: u64,
    next_id: u64,
    receipts: Vec<ServerPhysicalDesignMutationReceipt>,
    unresolved: Option<MutationReceiptBegin>,
    recovery_required: bool,
    #[cfg(test)]
    fail_next_begin_before_write: bool,
    #[cfg(test)]
    fail_next_outcome_before_write: bool,
    #[cfg(test)]
    test_io_failure: Option<TestJournalIoFailure>,
}

impl ServerPhysicalDesignMutationReceiptJournal {
    pub(crate) const fn reference(
        &self,
        receipt_id: ServerPhysicalDesignMutationReceiptId,
    ) -> ServerPhysicalDesignMutationReceiptReference {
        ServerPhysicalDesignMutationReceiptReference {
            journal_incarnation: self.journal_incarnation,
            receipt_id,
        }
    }

    pub(crate) fn open(
        config: ServerPhysicalDesignMutationReceiptConfig,
        identity: PhysicalDesignDatabaseIdentity,
        database: &Database,
    ) -> Result<Self, ServerPhysicalDesignMutationReceiptJournalError> {
        #[cfg(not(unix))]
        return Err(ServerPhysicalDesignMutationReceiptJournalError::FilesystemSafetyUnavailable);

        #[cfg(unix)]
        {
            let exists = match fs::symlink_metadata(config.path()) {
                Ok(metadata) if metadata.file_type().is_file() => true,
                Ok(_) => {
                    return Err(ServerPhysicalDesignMutationReceiptJournalError::Path(
                        ServerPhysicalDesignMutationReceiptPathError::ExistingObjectNotRegular(
                            config.path().to_path_buf(),
                        ),
                    ));
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => false,
                Err(source) => {
                    return Err(ServerPhysicalDesignMutationReceiptJournalError::Path(
                        ServerPhysicalDesignMutationReceiptPathError::Metadata {
                            path: config.path().to_path_buf(),
                            source,
                        },
                    ));
                }
            };
            let mut file = if !exists {
                create_v3_journal(&config, *identity.as_bytes())?
            } else {
                let mut source = secure_open_existing(&config, true)?;
                verify_opened_regular(&source, &config)?;
                lock_journal_exclusive(&source)?;
                verify_final_path_is_opened_inode(&source, config.path())?;
                let bytes = read_open_file(&mut source, &config)?;
                let version = journal_version(&bytes)?;
                match version {
                    V1_VERSION | V2_VERSION => {
                        migrate_legacy(&config, *identity.as_bytes(), version, &bytes, &source)?
                    }
                    CURRENT_VERSION => {
                        validate_v3_header(&bytes, *identity.as_bytes())?;
                        source
                    }
                    version => {
                        return Err(
                            ServerPhysicalDesignMutationReceiptJournalError::UnsupportedVersion(
                                version,
                            ),
                        );
                    }
                }
            };

            verify_final_path_is_opened_inode(&file, config.path())?;
            let bytes = read_open_file(&mut file, &config)?;
            let journal_incarnation = validate_v3_header(&bytes, *identity.as_bytes())?;
            let decoded = decode_v3_records(&bytes, V3_HEADER_BYTES)?;
            if decoded.valid_bytes < bytes.len() {
                verify_final_path_is_opened_inode(&file, config.path())?;
                file.set_len(decoded.valid_bytes as u64)
                    .map_err(|source| io_error("tail truncate", config.path(), source))?;
                file.sync_all()
                    .map_err(|source| io_error("tail truncate sync", config.path(), source))?;
            }
            file.seek(SeekFrom::End(0))
                .map_err(|source| io_error("seek", config.path(), source))?;
            let next_id = decoded
                .highest_begin
                .checked_add(1)
                .ok_or(ServerPhysicalDesignMutationReceiptJournalError::ReceiptIdExhausted)?;
            let mut journal = Self {
                config,
                journal_incarnation,
                file,
                file_len: decoded.valid_bytes as u64,
                next_id,
                receipts: decoded.receipts,
                unresolved: decoded.unresolved,
                recovery_required: false,
                #[cfg(test)]
                fail_next_begin_before_write: false,
                #[cfg(test)]
                fail_next_outcome_before_write: false,
                #[cfg(test)]
                test_io_failure: None,
            };
            journal.reconcile(database)?;
            verify_final_path_is_opened_inode(&journal.file, journal.config.path())?;
            Ok(journal)
        }
    }

    pub(crate) fn begin(
        &mut self,
        source: ServerPhysicalDesignMutationSource,
        evidence_epoch: PhysicalDesignEvidenceEpoch,
        target: MutationReceiptTarget,
    ) -> Result<
        ServerPhysicalDesignMutationReceiptId,
        ServerPhysicalDesignMutationReceiptControlError,
    > {
        if self.recovery_required || self.unresolved.is_some() {
            return Err(ServerPhysicalDesignMutationReceiptControlError::RecoveryRequired);
        }
        if self.next_id == 0 {
            return Err(ServerPhysicalDesignMutationReceiptControlError::Journal(
                ServerPhysicalDesignMutationReceiptJournalError::ReceiptIdExhausted,
            ));
        }
        let begin = MutationReceiptBegin {
            id: ServerPhysicalDesignMutationReceiptId(self.next_id),
            source,
            evidence_epoch,
            target,
        };
        let bytes = encode_begin(&begin)
            .map_err(ServerPhysicalDesignMutationReceiptControlError::Journal)?;
        let required = self
            .file_len
            .checked_add(bytes.len() as u64)
            .and_then(|length| length.checked_add(MAX_RECOVERED_OUTCOME_BYTES as u64));
        if required.is_none_or(|length| length > self.config.max_file_bytes()) {
            return Err(ServerPhysicalDesignMutationReceiptControlError::Journal(
                ServerPhysicalDesignMutationReceiptJournalError::CapacityExceeded,
            ));
        }
        #[cfg(test)]
        if std::mem::take(&mut self.fail_next_begin_before_write) {
            self.recovery_required = true;
            return Err(ServerPhysicalDesignMutationReceiptControlError::Journal(
                io_error(
                    "Begin append",
                    self.config.path(),
                    io::Error::other("injected Begin append failure"),
                ),
            ));
        }
        if let Err(error) = self.append_synced(&bytes, "Begin append", JournalRecordKind::Begin) {
            self.recovery_required = true;
            return Err(ServerPhysicalDesignMutationReceiptControlError::Journal(
                error,
            ));
        }
        self.next_id = self.next_id.checked_add(1).unwrap_or(0);
        self.receipts.push(public_receipt(&begin));
        self.unresolved = Some(begin);
        Ok(ServerPhysicalDesignMutationReceiptId(
            self.next_id.wrapping_sub(1),
        ))
    }

    pub(crate) fn finish(
        &mut self,
        id: ServerPhysicalDesignMutationReceiptId,
        outcome: ServerPhysicalDesignMutationReceiptOutcome,
    ) -> Result<(), ServerPhysicalDesignMutationReceiptControlError> {
        if self.recovery_required {
            return Err(ServerPhysicalDesignMutationReceiptControlError::RecoveryRequired);
        }
        let unresolved = self.unresolved.as_ref().ok_or({
            ServerPhysicalDesignMutationReceiptControlError::Journal(
                ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
                    "Outcome has no unresolved Begin",
                ),
            )
        })?;
        if unresolved.id != id
            || matches!(outcome, ServerPhysicalDesignMutationReceiptOutcome::Pending)
        {
            return Err(ServerPhysicalDesignMutationReceiptControlError::Journal(
                ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
                    "Outcome does not match current unresolved Begin",
                ),
            ));
        }
        validate_outcome_for_target(&unresolved.target, outcome)
            .map_err(ServerPhysicalDesignMutationReceiptControlError::Journal)?;
        let bytes = encode_outcome(id, outcome)
            .map_err(ServerPhysicalDesignMutationReceiptControlError::Journal)?;
        if self
            .file_len
            .checked_add(bytes.len() as u64)
            .is_none_or(|length| length > self.config.max_file_bytes())
        {
            self.recovery_required = true;
            return Err(ServerPhysicalDesignMutationReceiptControlError::Journal(
                ServerPhysicalDesignMutationReceiptJournalError::CapacityExceeded,
            ));
        }
        #[cfg(test)]
        if std::mem::take(&mut self.fail_next_outcome_before_write) {
            self.recovery_required = true;
            return Err(ServerPhysicalDesignMutationReceiptControlError::Journal(
                io_error(
                    "Outcome append",
                    self.config.path(),
                    io::Error::other("injected Outcome append failure"),
                ),
            ));
        }
        if let Err(error) = self.append_synced(&bytes, "Outcome append", JournalRecordKind::Outcome)
        {
            self.recovery_required = true;
            return Err(ServerPhysicalDesignMutationReceiptControlError::Journal(
                error,
            ));
        }
        if let Some(receipt) = self.receipts.last_mut() {
            receipt.outcome = outcome;
        }
        self.unresolved = None;
        Ok(())
    }

    pub(crate) fn mark_recovery_required(&mut self) {
        self.recovery_required = true;
    }

    #[cfg(test)]
    pub(crate) fn fail_next_begin_before_write(&mut self) {
        self.fail_next_begin_before_write = true;
    }

    #[cfg(test)]
    pub(crate) fn fail_next_outcome_before_write(&mut self) {
        self.fail_next_outcome_before_write = true;
    }

    #[cfg(test)]
    pub(crate) fn fail_io_at(&mut self, failure: TestJournalIoFailure) {
        self.test_io_failure = Some(failure);
    }

    pub(crate) fn page(
        &self,
        after: Option<ServerPhysicalDesignMutationReceiptId>,
        limit: u32,
    ) -> Result<
        ServerPhysicalDesignMutationReceiptPage,
        ServerPhysicalDesignMutationReceiptControlError,
    > {
        if limit == 0 || limit > MAX_MUTATION_RECEIPTS_PER_READ {
            return Err(
                ServerPhysicalDesignMutationReceiptControlError::InvalidLimit {
                    supplied: limit,
                    maximum: MAX_MUTATION_RECEIPTS_PER_READ,
                },
            );
        }
        let start = self
            .receipts
            .partition_point(|receipt| after.is_some_and(|after| receipt.id <= after));
        let end = start
            .saturating_add(limit as usize)
            .min(self.receipts.len());
        let receipts = self.receipts[start..end].to_vec();
        let next_after = (end < self.receipts.len())
            .then(|| receipts.last().map(|receipt| receipt.id))
            .flatten();
        Ok(ServerPhysicalDesignMutationReceiptPage {
            receipts,
            next_after,
        })
    }

    pub(crate) fn scoped_page(
        &self,
        after: Option<ServerPhysicalDesignMutationReceiptCursor>,
        limit: u32,
    ) -> Result<
        ServerPhysicalDesignMutationReceiptScopedPage,
        ServerPhysicalDesignMutationReceiptControlError,
    > {
        let after_id = if let Some(cursor) = after {
            if cursor.journal_incarnation != self.journal_incarnation {
                return Err(
                    ServerPhysicalDesignMutationReceiptControlError::JournalChanged {
                        expected: cursor.journal_incarnation,
                        actual: self.journal_incarnation,
                    },
                );
            }
            Some(cursor.receipt_id)
        } else {
            None
        };
        let page = self.page(after_id, limit)?;
        let next_after =
            page.next_after
                .map(|receipt_id| ServerPhysicalDesignMutationReceiptCursor {
                    journal_incarnation: self.journal_incarnation,
                    receipt_id,
                });
        Ok(ServerPhysicalDesignMutationReceiptScopedPage {
            journal_incarnation: self.journal_incarnation,
            receipts: page.receipts,
            next_after,
        })
    }

    pub(crate) fn status(&self) -> ServerPhysicalDesignMutationReceiptStatus {
        ServerPhysicalDesignMutationReceiptStatus {
            journal_incarnation: self.journal_incarnation,
            recovery_required: self.recovery_required,
            latest_receipt_id: self.receipts.last().map(|receipt| receipt.id),
            max_receipts_per_read: MAX_MUTATION_RECEIPTS_PER_READ,
        }
    }

    fn reconcile(
        &mut self,
        database: &Database,
    ) -> Result<(), ServerPhysicalDesignMutationReceiptJournalError> {
        let Some(begin) = self.unresolved.clone() else {
            return Ok(());
        };
        let outcome = match &begin.target {
            MutationReceiptTarget::Index {
                candidate,
                index_name,
            } => match database.inspect_physical_index_design_name(*candidate, index_name) {
                PhysicalIndexDesignNameState::AlreadyApplied { index_id } => {
                    ServerPhysicalDesignMutationReceiptOutcome::RecoveredAppliedIndex { index_id }
                }
                PhysicalIndexDesignNameState::Conflict => {
                    ServerPhysicalDesignMutationReceiptOutcome::RecoveredConflict
                }
                PhysicalIndexDesignNameState::Available => {
                    ServerPhysicalDesignMutationReceiptOutcome::RecoveredNotApplied
                }
            },
            MutationReceiptTarget::Columnar {
                candidate,
                mode,
                directory,
                ..
            } => match database
                .inspect_physical_columnar_design_location(candidate, *mode, directory)
                .map_err(ServerPhysicalDesignMutationReceiptJournalError::Reconciliation)?
            {
                PhysicalColumnarDesignLocationState::AlreadyApplied { projection_id } => {
                    ServerPhysicalDesignMutationReceiptOutcome::RecoveredAppliedColumnar {
                        projection_id,
                    }
                }
                PhysicalColumnarDesignLocationState::Conflict { .. } => {
                    ServerPhysicalDesignMutationReceiptOutcome::RecoveredConflict
                }
                PhysicalColumnarDesignLocationState::Available => {
                    ServerPhysicalDesignMutationReceiptOutcome::RecoveredNotApplied
                }
            },
        };
        self.finish(begin.id, outcome).map_err(|error| match error {
            ServerPhysicalDesignMutationReceiptControlError::Journal(error) => error,
            ServerPhysicalDesignMutationReceiptControlError::RecoveryRequired => {
                ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
                    "recovered Outcome could not become durable",
                )
            }
            _ => ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
                "invalid reconciliation state",
            ),
        })
    }

    fn append_synced(
        &mut self,
        bytes: &[u8],
        operation: &'static str,
        record_kind: JournalRecordKind,
    ) -> Result<(), ServerPhysicalDesignMutationReceiptJournalError> {
        // flock owns the open inode, not its directory entry. Detect replacement
        // at append boundaries so detected stale handles cannot authorize mutation.
        verify_final_path_is_opened_inode(&self.file, self.config.path())?;
        #[cfg(not(test))]
        let _ = record_kind;
        #[cfg(test)]
        if record_kind == JournalRecordKind::Outcome
            && self.test_io_failure == Some(TestJournalIoFailure::OutcomeAfterPartialWrite)
        {
            self.test_io_failure = None;
            let partial = bytes.len().max(2) / 2;
            self.file
                .write_all(&bytes[..partial])
                .map_err(|source| io_error(operation, self.config.path(), source))?;
            return Err(io_error(
                operation,
                self.config.path(),
                io::Error::other("injected Outcome failure after partial write"),
            ));
        }
        self.file
            .write_all(bytes)
            .map_err(|source| io_error(operation, self.config.path(), source))?;
        #[cfg(test)]
        {
            let injected = matches!(
                (record_kind, self.test_io_failure),
                (
                    JournalRecordKind::Begin,
                    Some(TestJournalIoFailure::BeginAfterFullWriteBeforeSync)
                ) | (
                    JournalRecordKind::Outcome,
                    Some(TestJournalIoFailure::OutcomeAfterFullWriteBeforeSync)
                )
            );
            if injected {
                self.test_io_failure = None;
                return Err(io_error(
                    "record sync",
                    self.config.path(),
                    io::Error::other("injected failure after full write before sync"),
                ));
            }
        }
        self.file
            .sync_all()
            .map_err(|source| io_error("record sync", self.config.path(), source))?;
        verify_final_path_is_opened_inode(&self.file, self.config.path())?;
        self.file_len = self
            .file_len
            .checked_add(bytes.len() as u64)
            .ok_or(ServerPhysicalDesignMutationReceiptJournalError::CapacityExceeded)?;
        Ok(())
    }
}

fn public_receipt(begin: &MutationReceiptBegin) -> ServerPhysicalDesignMutationReceipt {
    let target = match &begin.target {
        MutationReceiptTarget::Index {
            candidate,
            index_name,
        } => ServerPhysicalDesignMutationTarget::Index {
            table_id: candidate.table_id,
            column_id: candidate.column_id,
            index_name: index_name.clone(),
        },
        MutationReceiptTarget::Columnar {
            candidate,
            mode,
            placement,
            ..
        } => ServerPhysicalDesignMutationTarget::Columnar {
            table_id: candidate.table_id,
            columns: candidate.columns.clone(),
            mode: *mode,
            placement: placement.clone(),
        },
    };
    ServerPhysicalDesignMutationReceipt {
        id: begin.id,
        source: begin.source,
        evidence_epoch: begin.evidence_epoch,
        target,
        outcome: ServerPhysicalDesignMutationReceiptOutcome::Pending,
    }
}

#[cfg(test)]
fn encode_v1_header(identity: [u8; 16]) -> [u8; V1_HEADER_BYTES] {
    let mut bytes = [0_u8; V1_HEADER_BYTES];
    bytes[..4].copy_from_slice(b"NBMR");
    bytes[4..6].copy_from_slice(&V1_VERSION.to_le_bytes());
    bytes[8..24].copy_from_slice(&identity);
    let checksum = crc32c::crc32c(&bytes[..24]).to_le_bytes();
    bytes[24..28].copy_from_slice(&checksum);
    bytes
}

fn encode_v2_header(
    database_incarnation: [u8; 16],
    journal_incarnation: ServerPhysicalDesignMutationReceiptJournalIncarnation,
) -> [u8; V2_HEADER_BYTES] {
    let mut bytes = [0_u8; V2_HEADER_BYTES];
    bytes[..4].copy_from_slice(b"NBMR");
    bytes[4..6].copy_from_slice(&V2_VERSION.to_le_bytes());
    bytes[8..24].copy_from_slice(&database_incarnation);
    bytes[24..40].copy_from_slice(journal_incarnation.as_bytes());
    let checksum = crc32c::crc32c(&bytes[..40]).to_le_bytes();
    bytes[40..44].copy_from_slice(&checksum);
    bytes
}

fn validate_v1_header(
    bytes: &[u8],
    identity: [u8; 16],
) -> Result<(), ServerPhysicalDesignMutationReceiptJournalError> {
    if bytes.len() < V1_HEADER_BYTES {
        return Err(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
            "truncated header",
        ));
    }
    let version = journal_version(bytes)?;
    if version != V1_VERSION {
        return Err(ServerPhysicalDesignMutationReceiptJournalError::UnsupportedVersion(version));
    }
    if bytes[6..8] != [0, 0] {
        return Err(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
            "nonzero header reserved bytes",
        ));
    }
    let mut checksum_bytes = [0_u8; 4];
    checksum_bytes.copy_from_slice(&bytes[24..28]);
    if crc32c::crc32c(&bytes[..24]) != u32::from_le_bytes(checksum_bytes) {
        return Err(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
            "header checksum mismatch",
        ));
    }
    if bytes[8..24] != identity {
        return Err(ServerPhysicalDesignMutationReceiptJournalError::DatabaseIdentityMismatch);
    }
    Ok(())
}

fn validate_v2_header(
    bytes: &[u8],
    identity: [u8; 16],
) -> Result<
    ServerPhysicalDesignMutationReceiptJournalIncarnation,
    ServerPhysicalDesignMutationReceiptJournalError,
> {
    if bytes.len() < V2_HEADER_BYTES {
        return Err(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
            "truncated header",
        ));
    }
    let version = journal_version(bytes)?;
    if version != V2_VERSION {
        return Err(ServerPhysicalDesignMutationReceiptJournalError::UnsupportedVersion(version));
    }
    if bytes[6..8] != [0, 0] {
        return Err(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
            "nonzero header reserved bytes",
        ));
    }
    let mut checksum_bytes = [0_u8; 4];
    checksum_bytes.copy_from_slice(&bytes[40..44]);
    if crc32c::crc32c(&bytes[..40]) != u32::from_le_bytes(checksum_bytes) {
        return Err(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
            "header checksum mismatch",
        ));
    }
    if bytes[8..24] != identity {
        return Err(ServerPhysicalDesignMutationReceiptJournalError::DatabaseIdentityMismatch);
    }
    let mut journal_incarnation = [0_u8; 16];
    journal_incarnation.copy_from_slice(&bytes[24..40]);
    if journal_incarnation == [0; 16] {
        return Err(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
            "zero journal incarnation",
        ));
    }
    Ok(ServerPhysicalDesignMutationReceiptJournalIncarnation(
        journal_incarnation,
    ))
}

fn encode_v3_header(
    database_incarnation: [u8; 16],
    journal_incarnation: ServerPhysicalDesignMutationReceiptJournalIncarnation,
) -> [u8; V3_HEADER_BYTES] {
    let mut bytes = encode_v2_header(database_incarnation, journal_incarnation);
    bytes[4..6].copy_from_slice(&CURRENT_VERSION.to_le_bytes());
    let checksum = crc32c::crc32c(&bytes[..40]).to_le_bytes();
    bytes[40..44].copy_from_slice(&checksum);
    bytes
}

fn validate_v3_header(
    bytes: &[u8],
    identity: [u8; 16],
) -> Result<
    ServerPhysicalDesignMutationReceiptJournalIncarnation,
    ServerPhysicalDesignMutationReceiptJournalError,
> {
    if bytes.len() < V3_HEADER_BYTES {
        return Err(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
            "truncated header",
        ));
    }
    let version = journal_version(bytes)?;
    if version != CURRENT_VERSION {
        return Err(ServerPhysicalDesignMutationReceiptJournalError::UnsupportedVersion(version));
    }
    if bytes[6..8] != [0, 0] {
        return Err(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
            "nonzero header reserved bytes",
        ));
    }
    let expected = read_u32(&bytes[40..44]);
    if crc32c::crc32c(&bytes[..40]) != expected {
        return Err(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
            "header checksum mismatch",
        ));
    }
    if bytes[8..24] != identity {
        return Err(ServerPhysicalDesignMutationReceiptJournalError::DatabaseIdentityMismatch);
    }
    let mut journal_incarnation = [0_u8; 16];
    journal_incarnation.copy_from_slice(&bytes[24..40]);
    if journal_incarnation == [0; 16] {
        return Err(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
            "zero journal incarnation",
        ));
    }
    Ok(ServerPhysicalDesignMutationReceiptJournalIncarnation(
        journal_incarnation,
    ))
}

fn encode_begin(
    begin: &MutationReceiptBegin,
) -> Result<Vec<u8>, ServerPhysicalDesignMutationReceiptJournalError> {
    let mut body = Vec::new();
    body.push(match begin.source {
        ServerPhysicalDesignMutationSource::Programmatic => 1,
        ServerPhysicalDesignMutationSource::LocalOperator => 2,
    });
    body.push(match begin.target {
        MutationReceiptTarget::Index { .. } => 1,
        MutationReceiptTarget::Columnar { .. } => 2,
    });
    body.extend_from_slice(&[0, 0]);
    body.extend_from_slice(&begin.evidence_epoch.0.to_le_bytes());
    match &begin.target {
        MutationReceiptTarget::Index {
            candidate,
            index_name,
        } => {
            body.extend_from_slice(&candidate.table_id.0.to_le_bytes());
            body.extend_from_slice(&candidate.column_id.0.to_le_bytes());
            put_string(&mut body, index_name.as_str(), 255)?;
        }
        MutationReceiptTarget::Columnar {
            candidate,
            mode,
            placement,
            directory,
        } => {
            if candidate.columns.len() > MAX_COLUMN_IDS {
                return Err(
                    ServerPhysicalDesignMutationReceiptJournalError::ColumnCountExceeded {
                        count: candidate.columns.len(),
                        maximum: MAX_COLUMN_IDS,
                    },
                );
            }
            body.extend_from_slice(&candidate.table_id.0.to_le_bytes());
            body.push(match mode {
                PhysicalColumnarDesignMode::Snapshot => 1,
                PhysicalColumnarDesignMode::Incremental => 2,
            });
            body.extend_from_slice(&[0, 0, 0]);
            body.extend_from_slice(&(candidate.columns.len() as u32).to_le_bytes());
            for column in &candidate.columns {
                body.extend_from_slice(&column.0.to_le_bytes());
            }
            put_string(&mut body, placement.as_str(), MAX_PLACEMENT_BYTES)?;
            let path = directory
                .to_str()
                .ok_or(ServerPhysicalDesignMutationReceiptJournalError::PathNotUtf8)?;
            put_string(&mut body, path, MAX_PATH_BYTES)?;
        }
    }
    encode_record(BEGIN_TAG, begin.id, &body)
}

fn encode_outcome(
    id: ServerPhysicalDesignMutationReceiptId,
    outcome: ServerPhysicalDesignMutationReceiptOutcome,
) -> Result<Vec<u8>, ServerPhysicalDesignMutationReceiptJournalError> {
    let (tag, physical_id) = match outcome {
        ServerPhysicalDesignMutationReceiptOutcome::CreatedIndex { index_id } => {
            (1, Some(index_id.0))
        }
        ServerPhysicalDesignMutationReceiptOutcome::CreatedColumnar { projection_id } => {
            (2, Some(projection_id.0))
        }
        ServerPhysicalDesignMutationReceiptOutcome::AlreadyAppliedIndex { index_id } => {
            (3, Some(index_id.0))
        }
        ServerPhysicalDesignMutationReceiptOutcome::AlreadyAppliedColumnar { projection_id } => {
            (4, Some(projection_id.0))
        }
        ServerPhysicalDesignMutationReceiptOutcome::AlreadyCovered => (5, None),
        ServerPhysicalDesignMutationReceiptOutcome::Rejected => (6, None),
        ServerPhysicalDesignMutationReceiptOutcome::Failed => (7, None),
        ServerPhysicalDesignMutationReceiptOutcome::RecoveredAppliedIndex { index_id } => {
            (8, Some(index_id.0))
        }
        ServerPhysicalDesignMutationReceiptOutcome::RecoveredAppliedColumnar { projection_id } => {
            (9, Some(projection_id.0))
        }
        ServerPhysicalDesignMutationReceiptOutcome::RecoveredNotApplied => (10, None),
        ServerPhysicalDesignMutationReceiptOutcome::RecoveredConflict => (11, None),
        ServerPhysicalDesignMutationReceiptOutcome::Pending => {
            return Err(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
                "Pending cannot be encoded as an Outcome",
            ));
        }
    };
    let mut body = vec![tag, 0, 0, 0];
    if let Some(physical_id) = physical_id {
        body.extend_from_slice(&physical_id.to_le_bytes());
    }
    encode_record(OUTCOME_TAG, id, &body)
}

fn encode_record(
    tag: u8,
    id: ServerPhysicalDesignMutationReceiptId,
    body: &[u8],
) -> Result<Vec<u8>, ServerPhysicalDesignMutationReceiptJournalError> {
    if id.0 == 0 {
        return Err(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
            "zero receipt ID",
        ));
    }
    let total = V3_RECORD_HEADER_BYTES
        .checked_add(body.len())
        .and_then(|length| length.checked_add(V3_RECORD_TRAILER_BYTES))
        .ok_or(ServerPhysicalDesignMutationReceiptJournalError::CapacityExceeded)?;
    if total > MAX_MUTATION_RECEIPT_RECORD_BYTES {
        return Err(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
            "record exceeds NBMR cap",
        ));
    }
    let mut bytes = Vec::with_capacity(total);
    let body_len = u32::try_from(body.len())
        .map_err(|_| ServerPhysicalDesignMutationReceiptJournalError::CapacityExceeded)?;
    bytes.extend_from_slice(&body_len.to_le_bytes());
    bytes.extend_from_slice(&(!body_len).to_le_bytes());
    bytes.push(tag);
    bytes.extend_from_slice(&[0, 0, 0]);
    bytes.extend_from_slice(&id.0.to_le_bytes());
    bytes.extend_from_slice(&crc32c::crc32c(&bytes).to_le_bytes());
    bytes.extend_from_slice(body);
    bytes.extend_from_slice(&crc32c::crc32c(&bytes).to_le_bytes());
    Ok(bytes)
}

#[cfg(test)]
fn encode_legacy_record(
    tag: u8,
    id: ServerPhysicalDesignMutationReceiptId,
    body: &[u8],
) -> Result<Vec<u8>, ServerPhysicalDesignMutationReceiptJournalError> {
    if id.0 == 0 {
        return Err(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
            "zero receipt ID",
        ));
    }
    let payload_len = 12_usize
        .checked_add(body.len())
        .ok_or(ServerPhysicalDesignMutationReceiptJournalError::CapacityExceeded)?;
    let total = 4_usize
        .checked_add(payload_len)
        .and_then(|length| length.checked_add(4))
        .ok_or(ServerPhysicalDesignMutationReceiptJournalError::CapacityExceeded)?;
    if total > MAX_MUTATION_RECEIPT_RECORD_BYTES {
        return Err(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
            "record exceeds NBMR cap",
        ));
    }
    let mut bytes = Vec::with_capacity(total);
    bytes.extend_from_slice(&(payload_len as u32).to_le_bytes());
    bytes.push(tag);
    bytes.extend_from_slice(&[0, 0, 0]);
    bytes.extend_from_slice(&id.0.to_le_bytes());
    bytes.extend_from_slice(body);
    bytes.extend_from_slice(&crc32c::crc32c(&bytes).to_le_bytes());
    Ok(bytes)
}

struct DecodedRecords {
    valid_bytes: usize,
    highest_begin: u64,
    receipts: Vec<ServerPhysicalDesignMutationReceipt>,
    unresolved: Option<MutationReceiptBegin>,
    records: Vec<ParsedRecord>,
}

#[derive(Clone)]
enum ParsedRecord {
    Begin(MutationReceiptBegin),
    Outcome(
        ServerPhysicalDesignMutationReceiptId,
        ServerPhysicalDesignMutationReceiptOutcome,
    ),
}

fn decode_legacy_records(
    bytes: &[u8],
    header_bytes: usize,
) -> Result<DecodedRecords, ServerPhysicalDesignMutationReceiptJournalError> {
    let mut offset = header_bytes;
    let mut highest = 0_u64;
    let mut receipts = Vec::new();
    let mut unresolved: Option<MutationReceiptBegin> = None;
    let mut records = Vec::new();
    while offset < bytes.len() {
        if bytes.len() - offset < 4 {
            break;
        }
        let payload_len = read_u32(&bytes[offset..offset + 4]) as usize;
        let total = 4_usize
            .checked_add(payload_len)
            .and_then(|length| length.checked_add(4))
            .ok_or(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
                "record length overflow",
            ))?;
        if !(LEGACY_RECORD_FIXED_BYTES..=MAX_MUTATION_RECEIPT_RECORD_BYTES).contains(&total)
            || payload_len < 12
        {
            return Err(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
                "invalid record length",
            ));
        }
        if bytes.len() - offset < total {
            break;
        }
        let record = &bytes[offset..offset + total];
        let expected = read_u32(&record[total - 4..]);
        if crc32c::crc32c(&record[..total - 4]) != expected {
            return Err(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
                "record checksum mismatch",
            ));
        }
        let parsed = decode_legacy_record(record)?;
        accept_record(&parsed, &mut highest, &mut receipts, &mut unresolved)?;
        records.push(parsed);
        offset += total;
    }
    Ok(DecodedRecords {
        valid_bytes: offset,
        highest_begin: highest,
        receipts,
        unresolved,
        records,
    })
}

fn decode_legacy_record(
    record: &[u8],
) -> Result<ParsedRecord, ServerPhysicalDesignMutationReceiptJournalError> {
    let payload_len = read_u32(&record[..4]) as usize;
    let payload = &record[4..4 + payload_len];
    let tag = payload[0];
    if payload[1..4] != [0, 0, 0] {
        return Err(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
            "nonzero record reserved bytes",
        ));
    }
    let id = ServerPhysicalDesignMutationReceiptId(read_u64(&payload[4..12]));
    if id.0 == 0 {
        return Err(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
            "zero receipt ID",
        ));
    }
    decode_record_body(tag, id, &payload[12..])
}

fn decode_record_body(
    tag: u8,
    id: ServerPhysicalDesignMutationReceiptId,
    body: &[u8],
) -> Result<ParsedRecord, ServerPhysicalDesignMutationReceiptJournalError> {
    let mut reader = Reader(body);
    match tag {
        BEGIN_TAG => {
            let source = match reader.u8()? {
                1 => ServerPhysicalDesignMutationSource::Programmatic,
                2 => ServerPhysicalDesignMutationSource::LocalOperator,
                _ => {
                    return Err(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
                        "unknown receipt source",
                    ));
                }
            };
            let target_tag = reader.u8()?;
            reader.zeros(2)?;
            let evidence_epoch = PhysicalDesignEvidenceEpoch(reader.u64()?);
            let target = match target_tag {
                1 => {
                    let candidate = PhysicalIndexCandidate {
                        table_id: TableId(reader.u64()?),
                        column_id: ColumnId(reader.u32()?),
                    };
                    let index_name = IndexName::new(reader.string(255)?).map_err(|_| {
                        ServerPhysicalDesignMutationReceiptJournalError::InvalidIndexName
                    })?;
                    MutationReceiptTarget::Index {
                        candidate,
                        index_name,
                    }
                }
                2 => {
                    let table_id = TableId(reader.u64()?);
                    let mode = match reader.u8()? {
                        1 => PhysicalColumnarDesignMode::Snapshot,
                        2 => PhysicalColumnarDesignMode::Incremental,
                        _ => {
                            return Err(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
                                "unknown Columnar mode",
                            ));
                        }
                    };
                    reader.zeros(3)?;
                    let count = reader.u32()? as usize;
                    if count > MAX_COLUMN_IDS {
                        return Err(
                            ServerPhysicalDesignMutationReceiptJournalError::ColumnCountExceeded {
                                count,
                                maximum: MAX_COLUMN_IDS,
                            },
                        );
                    }
                    if count > reader.0.len() / 4 {
                        return Err(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
                            "column count exceeds remaining bytes",
                        ));
                    }
                    let mut columns = Vec::with_capacity(count);
                    for _ in 0..count {
                        columns.push(ColumnId(reader.u32()?));
                    }
                    let placement = ServerPhysicalColumnarPlacementKey::new(
                        reader.string(MAX_PLACEMENT_BYTES)?,
                    )
                    .map_err(ServerPhysicalDesignMutationReceiptJournalError::InvalidPlacement)?;
                    let directory = PathBuf::from(reader.string(MAX_PATH_BYTES)?);
                    if !directory.is_absolute() {
                        return Err(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
                            "Columnar recovery path is not absolute",
                        ));
                    }
                    MutationReceiptTarget::Columnar {
                        candidate: PhysicalColumnarCandidate { table_id, columns },
                        mode,
                        placement,
                        directory,
                    }
                }
                _ => {
                    return Err(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
                        "unknown Begin target tag",
                    ));
                }
            };
            reader.finish()?;
            Ok(ParsedRecord::Begin(MutationReceiptBegin {
                id,
                source,
                evidence_epoch,
                target,
            }))
        }
        OUTCOME_TAG => {
            let outcome_tag = reader.u8()?;
            reader.zeros(3)?;
            let outcome = match outcome_tag {
                1 => ServerPhysicalDesignMutationReceiptOutcome::CreatedIndex {
                    index_id: IndexId(reader.u64()?),
                },
                2 => ServerPhysicalDesignMutationReceiptOutcome::CreatedColumnar {
                    projection_id: ColumnarProjectionId(reader.u64()?),
                },
                3 => ServerPhysicalDesignMutationReceiptOutcome::AlreadyAppliedIndex {
                    index_id: IndexId(reader.u64()?),
                },
                4 => ServerPhysicalDesignMutationReceiptOutcome::AlreadyAppliedColumnar {
                    projection_id: ColumnarProjectionId(reader.u64()?),
                },
                5 => ServerPhysicalDesignMutationReceiptOutcome::AlreadyCovered,
                6 => ServerPhysicalDesignMutationReceiptOutcome::Rejected,
                7 => ServerPhysicalDesignMutationReceiptOutcome::Failed,
                8 => ServerPhysicalDesignMutationReceiptOutcome::RecoveredAppliedIndex {
                    index_id: IndexId(reader.u64()?),
                },
                9 => ServerPhysicalDesignMutationReceiptOutcome::RecoveredAppliedColumnar {
                    projection_id: ColumnarProjectionId(reader.u64()?),
                },
                10 => ServerPhysicalDesignMutationReceiptOutcome::RecoveredNotApplied,
                11 => ServerPhysicalDesignMutationReceiptOutcome::RecoveredConflict,
                _ => {
                    return Err(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
                        "unknown Outcome tag",
                    ));
                }
            };
            reader.finish()?;
            Ok(ParsedRecord::Outcome(id, outcome))
        }
        _ => Err(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
            "unknown record tag",
        )),
    }
}

fn decode_record(
    record: &[u8],
) -> Result<ParsedRecord, ServerPhysicalDesignMutationReceiptJournalError> {
    if record.len() < V3_RECORD_HEADER_BYTES + V3_RECORD_TRAILER_BYTES {
        return Err(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
            "truncated v3 record",
        ));
    }
    let body_len = read_u32(&record[..4]);
    if read_u32(&record[4..8]) != !body_len {
        return Err(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
            "record length complement mismatch",
        ));
    }
    if record[9..12] != [0, 0, 0] {
        return Err(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
            "nonzero record reserved bytes",
        ));
    }
    let expected_header_crc = read_u32(&record[20..24]);
    if crc32c::crc32c(&record[..20]) != expected_header_crc {
        return Err(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
            "record header checksum mismatch",
        ));
    }
    let body_len = body_len as usize;
    let total = V3_RECORD_HEADER_BYTES
        .checked_add(body_len)
        .and_then(|length| length.checked_add(V3_RECORD_TRAILER_BYTES))
        .ok_or(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
            "record length overflow",
        ))?;
    if total != record.len() || total > MAX_MUTATION_RECEIPT_RECORD_BYTES {
        return Err(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
            "invalid record length",
        ));
    }
    let expected_body_crc = read_u32(&record[total - V3_RECORD_TRAILER_BYTES..]);
    if crc32c::crc32c(&record[..total - V3_RECORD_TRAILER_BYTES]) != expected_body_crc {
        return Err(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
            "record body checksum mismatch",
        ));
    }
    let id = ServerPhysicalDesignMutationReceiptId(read_u64(&record[12..20]));
    if id.0 == 0 {
        return Err(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
            "zero receipt ID",
        ));
    }
    decode_record_body(
        record[8],
        id,
        &record[V3_RECORD_HEADER_BYTES..total - V3_RECORD_TRAILER_BYTES],
    )
}

fn decode_v3_records(
    bytes: &[u8],
    header_bytes: usize,
) -> Result<DecodedRecords, ServerPhysicalDesignMutationReceiptJournalError> {
    let mut offset = header_bytes;
    let mut highest = 0_u64;
    let mut receipts = Vec::new();
    let mut unresolved = None;
    let mut records = Vec::new();
    while offset < bytes.len() {
        let remaining = bytes.len() - offset;
        if remaining < V3_RECORD_HEADER_BYTES {
            break;
        }
        let header = &bytes[offset..offset + V3_RECORD_HEADER_BYTES];
        let body_len = read_u32(&header[..4]);
        if read_u32(&header[4..8]) != !body_len {
            return Err(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
                "record length complement mismatch",
            ));
        }
        if header[9..12] != [0, 0, 0] {
            return Err(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
                "nonzero record reserved bytes",
            ));
        }
        if crc32c::crc32c(&header[..20]) != read_u32(&header[20..24]) {
            return Err(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
                "record header checksum mismatch",
            ));
        }
        if header[8] != BEGIN_TAG && header[8] != OUTCOME_TAG {
            return Err(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
                "unknown record tag",
            ));
        }
        if read_u64(&header[12..20]) == 0 {
            return Err(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
                "zero receipt ID",
            ));
        }
        let total = V3_RECORD_HEADER_BYTES
            .checked_add(body_len as usize)
            .and_then(|length| length.checked_add(V3_RECORD_TRAILER_BYTES))
            .ok_or(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
                "record length overflow",
            ))?;
        if total > MAX_MUTATION_RECEIPT_RECORD_BYTES {
            return Err(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
                "invalid record length",
            ));
        }
        if remaining < total {
            break;
        }
        let parsed = decode_record(&bytes[offset..offset + total])?;
        accept_record(&parsed, &mut highest, &mut receipts, &mut unresolved)?;
        records.push(parsed);
        offset += total;
    }
    Ok(DecodedRecords {
        valid_bytes: offset,
        highest_begin: highest,
        receipts,
        unresolved,
        records,
    })
}

fn accept_record(
    parsed: &ParsedRecord,
    highest: &mut u64,
    receipts: &mut Vec<ServerPhysicalDesignMutationReceipt>,
    unresolved: &mut Option<MutationReceiptBegin>,
) -> Result<(), ServerPhysicalDesignMutationReceiptJournalError> {
    match parsed {
        ParsedRecord::Begin(begin) => {
            if unresolved.is_some() {
                return Err(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
                    "multiple unresolved Begin records",
                ));
            }
            if begin.id.0 <= *highest {
                return Err(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
                    "non-monotonic Begin receipt ID",
                ));
            }
            *highest = begin.id.0;
            receipts.push(public_receipt(begin));
            *unresolved = Some(begin.clone());
        }
        ParsedRecord::Outcome(id, outcome) => {
            let begin = unresolved.as_ref().ok_or(
                ServerPhysicalDesignMutationReceiptJournalError::Corrupt("Outcome without Begin"),
            )?;
            if begin.id != *id {
                return Err(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
                    "Outcome does not reference current Begin",
                ));
            }
            validate_outcome_for_target(&begin.target, *outcome)?;
            receipts
                .last_mut()
                .ok_or(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
                    "Outcome receipt missing",
                ))?
                .outcome = *outcome;
            *unresolved = None;
        }
    }
    Ok(())
}

fn validate_outcome_for_target(
    target: &MutationReceiptTarget,
    outcome: ServerPhysicalDesignMutationReceiptOutcome,
) -> Result<(), ServerPhysicalDesignMutationReceiptJournalError> {
    let incompatible = matches!(
        (target, outcome),
        (
            MutationReceiptTarget::Index { .. },
            ServerPhysicalDesignMutationReceiptOutcome::CreatedColumnar { .. }
                | ServerPhysicalDesignMutationReceiptOutcome::AlreadyAppliedColumnar { .. }
                | ServerPhysicalDesignMutationReceiptOutcome::RecoveredAppliedColumnar { .. }
        ) | (
            MutationReceiptTarget::Columnar { .. },
            ServerPhysicalDesignMutationReceiptOutcome::CreatedIndex { .. }
                | ServerPhysicalDesignMutationReceiptOutcome::AlreadyAppliedIndex { .. }
                | ServerPhysicalDesignMutationReceiptOutcome::RecoveredAppliedIndex { .. }
        )
    );
    if incompatible || matches!(outcome, ServerPhysicalDesignMutationReceiptOutcome::Pending) {
        Err(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
            "Outcome is incompatible with Begin target",
        ))
    } else {
        Ok(())
    }
}

fn encode_parsed_v3(
    record: &ParsedRecord,
) -> Result<Vec<u8>, ServerPhysicalDesignMutationReceiptJournalError> {
    match record {
        ParsedRecord::Begin(begin) => encode_begin(begin),
        ParsedRecord::Outcome(id, outcome) => encode_outcome(*id, *outcome),
    }
}

#[cfg(test)]
fn encode_legacy_begin(
    begin: &MutationReceiptBegin,
) -> Result<Vec<u8>, ServerPhysicalDesignMutationReceiptJournalError> {
    let current = encode_begin(begin)?;
    encode_legacy_record(
        BEGIN_TAG,
        begin.id,
        &current[V3_RECORD_HEADER_BYTES..current.len() - V3_RECORD_TRAILER_BYTES],
    )
}

#[cfg(test)]
fn encode_legacy_outcome(
    id: ServerPhysicalDesignMutationReceiptId,
    outcome: ServerPhysicalDesignMutationReceiptOutcome,
) -> Result<Vec<u8>, ServerPhysicalDesignMutationReceiptJournalError> {
    let current = encode_outcome(id, outcome)?;
    encode_legacy_record(
        OUTCOME_TAG,
        id,
        &current[V3_RECORD_HEADER_BYTES..current.len() - V3_RECORD_TRAILER_BYTES],
    )
}

struct Reader<'a>(&'a [u8]);
impl<'a> Reader<'a> {
    fn take(
        &mut self,
        count: usize,
    ) -> Result<&'a [u8], ServerPhysicalDesignMutationReceiptJournalError> {
        if self.0.len() < count {
            return Err(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
                "truncated record payload",
            ));
        }
        let (value, rest) = self.0.split_at(count);
        self.0 = rest;
        Ok(value)
    }
    fn u8(&mut self) -> Result<u8, ServerPhysicalDesignMutationReceiptJournalError> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> Result<u32, ServerPhysicalDesignMutationReceiptJournalError> {
        Ok(read_u32(self.take(4)?))
    }
    fn u64(&mut self) -> Result<u64, ServerPhysicalDesignMutationReceiptJournalError> {
        Ok(read_u64(self.take(8)?))
    }
    fn zeros(
        &mut self,
        count: usize,
    ) -> Result<(), ServerPhysicalDesignMutationReceiptJournalError> {
        if self.take(count)?.iter().any(|byte| *byte != 0) {
            Err(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
                "nonzero payload reserved bytes",
            ))
        } else {
            Ok(())
        }
    }
    fn string(
        &mut self,
        maximum: usize,
    ) -> Result<String, ServerPhysicalDesignMutationReceiptJournalError> {
        let length_bytes = self.take(2)?;
        let length = u16::from_le_bytes([length_bytes[0], length_bytes[1]]) as usize;
        if length > maximum {
            return Err(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
                "string exceeds field bound",
            ));
        }
        std::str::from_utf8(self.take(length)?)
            .map(str::to_owned)
            .map_err(|_| {
                ServerPhysicalDesignMutationReceiptJournalError::Corrupt("string is not UTF-8")
            })
    }
    fn finish(self) -> Result<(), ServerPhysicalDesignMutationReceiptJournalError> {
        if self.0.is_empty() {
            Ok(())
        } else {
            Err(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
                "trailing record payload",
            ))
        }
    }
}

fn read_u32(bytes: &[u8]) -> u32 {
    u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
}

fn read_u64(bytes: &[u8]) -> u64 {
    u64::from_le_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ])
}

fn put_string(
    bytes: &mut Vec<u8>,
    value: &str,
    maximum: usize,
) -> Result<(), ServerPhysicalDesignMutationReceiptJournalError> {
    if value.len() > maximum {
        return Err(
            ServerPhysicalDesignMutationReceiptJournalError::PathTooLong {
                bytes: value.len(),
                maximum,
            },
        );
    }
    bytes.extend_from_slice(&(value.len() as u16).to_le_bytes());
    bytes.extend_from_slice(value.as_bytes());
    Ok(())
}

fn io_error(
    operation: &'static str,
    path: &Path,
    source: io::Error,
) -> ServerPhysicalDesignMutationReceiptJournalError {
    ServerPhysicalDesignMutationReceiptJournalError::Io {
        operation,
        path: path.to_path_buf(),
        source,
    }
}

fn sync_parent(path: &Path) -> Result<(), ServerPhysicalDesignMutationReceiptJournalError> {
    let parent = path
        .parent()
        .ok_or(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
            "journal path has no parent",
        ))?;
    File::open(parent)
        .and_then(|file| file.sync_all())
        .map_err(|source| io_error("parent sync", parent, source))
}

#[cfg(test)]
mod tests {
    use std::io::{BufRead, BufReader};
    use std::process::{Command, Stdio};
    use std::sync::atomic::{AtomicU64, Ordering};

    use netbadb_core::{DatabaseCoordinatorConfig, TableStorageCreateSpec};
    use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
    use netbadb_types::PhysicalType;

    use super::*;

    static NEXT_PATH: AtomicU64 = AtomicU64::new(1);

    fn index_begin(id: u64) -> MutationReceiptBegin {
        MutationReceiptBegin {
            id: ServerPhysicalDesignMutationReceiptId(id),
            source: ServerPhysicalDesignMutationSource::Programmatic,
            evidence_epoch: PhysicalDesignEvidenceEpoch(4),
            target: MutationReceiptTarget::Index {
                candidate: PhysicalIndexCandidate {
                    table_id: TableId(2),
                    column_id: ColumnId(3),
                },
                index_name: IndexName::new("items_value_idx").unwrap(),
            },
        }
    }

    fn bytes_with(records: &[Vec<u8>]) -> Vec<u8> {
        let mut bytes = encode_v3_header(
            [7; 16],
            ServerPhysicalDesignMutationReceiptJournalIncarnation([9; 16]),
        )
        .to_vec();
        for record in records {
            bytes.extend_from_slice(record);
        }
        bytes
    }

    fn v1_bytes_with(identity: [u8; 16], records: &[Vec<u8>]) -> Vec<u8> {
        let mut bytes = encode_v1_header(identity).to_vec();
        for record in records {
            let legacy = match decode_record(record).unwrap() {
                ParsedRecord::Begin(begin) => encode_legacy_begin(&begin).unwrap(),
                ParsedRecord::Outcome(id, outcome) => encode_legacy_outcome(id, outcome).unwrap(),
            };
            bytes.extend_from_slice(&legacy);
        }
        bytes
    }

    fn decode_records(
        bytes: &[u8],
        header_bytes: usize,
    ) -> Result<DecodedRecords, ServerPhysicalDesignMutationReceiptJournalError> {
        decode_v3_records(bytes, header_bytes)
    }

    fn queue_journal_incarnations(values: impl IntoIterator<Item = [u8; 16]>) {
        TEST_JOURNAL_INCARNATIONS.with(|queued| queued.borrow_mut().extend(values));
    }

    fn fail_migration_at(point: TestMigrationFailure) {
        TEST_MIGRATION_FAILURE.with(|failure| *failure.borrow_mut() = Some(point));
    }

    fn fail_publication_at(point: TestPublicationFailure) {
        TEST_PUBLICATION_FAILURE.with(|failure| *failure.borrow_mut() = Some(point));
    }

    fn replace_record_crc(record: &mut [u8]) {
        let crc_offset = record.len() - 4;
        let checksum = crc32c::crc32c(&record[..crc_offset]).to_le_bytes();
        record[crc_offset..].copy_from_slice(&checksum);
    }

    fn replace_record_header_crc(record: &mut [u8]) {
        let checksum = crc32c::crc32c(&record[..20]).to_le_bytes();
        record[20..24].copy_from_slice(&checksum);
        replace_record_crc(record);
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    fn fixture_root(name: &str) -> PathBuf {
        let suffix = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "netbadb-nbmr-{name}-{}-{suffix}",
            std::process::id()
        ));
        fs::create_dir_all(&root).unwrap();
        root
    }

    fn create_database(root: &Path) -> Database {
        let table = TableDef::new(
            TableId(2),
            "items",
            vec![
                ColumnDef::new(ColumnId(1), "id", TypeSpec::Physical(PhysicalType::Int64)),
                ColumnDef::new(
                    ColumnId(3),
                    "value",
                    TypeSpec::Physical(PhysicalType::Int64),
                ),
            ],
        );
        Database::create_catalog(
            root.join("catalog"),
            vec![TableStorageCreateSpec::heap(root.join("items"), table)],
            Some(DatabaseCoordinatorConfig::new(root.join("coordinator")).with_global_visibility()),
        )
        .unwrap()
    }

    #[test]
    fn resource_receipt_column_count_cannot_amplify_short_record() {
        let mut body = vec![1, 2, 0, 0];
        body.extend_from_slice(&1_u64.to_le_bytes());
        body.extend_from_slice(&1_u64.to_le_bytes());
        body.extend_from_slice(&[1, 0, 0, 0]);
        body.extend_from_slice(&(MAX_COLUMN_IDS as u32).to_le_bytes());
        let record =
            encode_record(BEGIN_TAG, ServerPhysicalDesignMutationReceiptId(1), &body).unwrap();
        assert!(matches!(
            decode_record(&record),
            Err(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
                "column count exceeds remaining bytes"
            ))
        ));
    }

    #[cfg(not(unix))]
    #[test]
    fn unsupported_platform_rejects_before_any_journal_side_effect() {
        let root = fixture_root("unsupported-platform");
        let database = create_database(&root);
        let identity = database.physical_design_database_identity().unwrap();
        let config =
            ServerPhysicalDesignMutationReceiptConfig::new(root.join("receipts.nbmr"), 1_000_000)
                .unwrap();
        assert!(matches!(
            ServerPhysicalDesignMutationReceiptJournal::open(config.clone(), identity, &database),
            Err(ServerPhysicalDesignMutationReceiptJournalError::FilesystemSafetyUnavailable)
        ));
        assert!(!config.path().exists());
        assert!(fs::read_dir(&root).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with("receipts.nbmr")
        }));
        database.close().unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn same_process_double_open_is_rejected_and_lock_releases_on_drop() {
        let root = fixture_root("same-process-lock");
        let database = create_database(&root);
        let identity = database.physical_design_database_identity().unwrap();
        let config =
            ServerPhysicalDesignMutationReceiptConfig::new(root.join("receipts.nbmr"), 1_000_000)
                .unwrap();
        let mut owner =
            ServerPhysicalDesignMutationReceiptJournal::open(config.clone(), identity, &database)
                .unwrap();
        assert!(matches!(
            ServerPhysicalDesignMutationReceiptJournal::open(config.clone(), identity, &database),
            Err(ServerPhysicalDesignMutationReceiptJournalError::AlreadyInUse)
        ));
        let id = owner
            .begin(
                ServerPhysicalDesignMutationSource::Programmatic,
                PhysicalDesignEvidenceEpoch(4),
                index_begin(1).target,
            )
            .unwrap();
        owner
            .finish(id, ServerPhysicalDesignMutationReceiptOutcome::Rejected)
            .unwrap();
        drop(owner);

        let reopened =
            ServerPhysicalDesignMutationReceiptJournal::open(config, identity, &database).unwrap();
        let page = reopened.page(None, 2).unwrap();
        assert_eq!(page.receipts.len(), 1);
        assert_eq!(
            page.receipts[0].id,
            ServerPhysicalDesignMutationReceiptId(1)
        );
        drop(reopened);
        database.close().unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn journal_lock_child() {
        let Ok(path) = std::env::var("NETBADB_NBMR_LOCK_CHILD") else {
            return;
        };
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        lock_journal_exclusive(&file).unwrap();
        println!("NBMR_LOCKED");
        let mut byte = [0_u8; 1];
        std::io::stdin().read_exact(&mut byte).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn cross_process_journal_lock_is_exclusive_and_releases_after_owner_exit() {
        let root = fixture_root("cross-process-lock");
        let database = create_database(&root);
        let identity = database.physical_design_database_identity().unwrap();
        let config =
            ServerPhysicalDesignMutationReceiptConfig::new(root.join("receipts.nbmr"), 1_000_000)
                .unwrap();
        drop(
            ServerPhysicalDesignMutationReceiptJournal::open(config.clone(), identity, &database)
                .unwrap(),
        );

        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "physical_design_receipts::tests::journal_lock_child",
                "--nocapture",
            ])
            .env("NETBADB_NBMR_LOCK_CHILD", config.path())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let mut output = BufReader::new(child.stdout.take().unwrap());
        let mut line = String::new();
        loop {
            line.clear();
            assert_ne!(output.read_line(&mut line).unwrap(), 0);
            if line.contains("NBMR_LOCKED") {
                break;
            }
        }
        assert!(matches!(
            ServerPhysicalDesignMutationReceiptJournal::open(config.clone(), identity, &database),
            Err(ServerPhysicalDesignMutationReceiptJournalError::AlreadyInUse)
        ));
        child.stdin.take().unwrap().write_all(b"x").unwrap();
        assert!(child.wait().unwrap().success());
        drop(
            ServerPhysicalDesignMutationReceiptJournal::open(config, identity, &database).unwrap(),
        );
        database.close().unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn fresh_publication_is_locked_before_visibility() {
        let root = fixture_root("fresh-publication-lock");
        let database = create_database(&root);
        let identity = database.physical_design_database_identity().unwrap();
        let config =
            ServerPhysicalDesignMutationReceiptConfig::new(root.join("receipts.nbmr"), 1_000_000)
                .unwrap();
        let mut temp = create_owned_temp(config.path()).unwrap();
        temp.file_mut()
            .write_all(&encode_v3_header(
                *identity.as_bytes(),
                ServerPhysicalDesignMutationReceiptJournalIncarnation([0x31; 16]),
            ))
            .unwrap();
        temp.file_mut().sync_all().unwrap();
        publish_new(&mut temp, config.path()).unwrap();
        assert!(config.path().is_file());
        assert!(matches!(
            ServerPhysicalDesignMutationReceiptJournal::open(config.clone(), identity, &database),
            Err(ServerPhysicalDesignMutationReceiptJournalError::AlreadyInUse)
        ));
        drop(temp.into_published_file().unwrap());
        drop(
            ServerPhysicalDesignMutationReceiptJournal::open(config, identity, &database).unwrap(),
        );
        database.close().unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn migration_rejects_replaced_final_inode() {
        let root = fixture_root("migration-path-replaced");
        let database = create_database(&root);
        let identity = database.physical_design_database_identity().unwrap();
        let path = root.join("receipts.nbmr");
        fs::write(&path, encode_v1_header(*identity.as_bytes())).unwrap();
        let replacement = b"replacement must survive".to_vec();
        TEST_MIGRATION_FINAL_REPLACEMENT
            .with(|value| *value.borrow_mut() = Some(replacement.clone()));
        let config = ServerPhysicalDesignMutationReceiptConfig::new(&path, 1_000_000).unwrap();
        assert!(matches!(
            ServerPhysicalDesignMutationReceiptJournal::open(config, identity, &database),
            Err(ServerPhysicalDesignMutationReceiptJournalError::FinalPathChanged)
        ));
        assert_eq!(fs::read(&path).unwrap(), replacement);
        database.close().unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn legacy_migration_holds_old_and_new_ownership_without_gap() {
        let root = fixture_root("migration-lock-handoff");
        let database = create_database(&root);
        let identity = database.physical_design_database_identity().unwrap();
        let incarnation = ServerPhysicalDesignMutationReceiptJournalIncarnation([0x41; 16]);
        let begin = index_begin(1);
        let mut legacy = encode_v2_header(*identity.as_bytes(), incarnation).to_vec();
        legacy.extend_from_slice(&encode_legacy_begin(&begin).unwrap());
        legacy.extend_from_slice(
            &encode_legacy_outcome(
                begin.id,
                ServerPhysicalDesignMutationReceiptOutcome::Rejected,
            )
            .unwrap(),
        );
        let path = root.join("receipts.nbmr");
        fs::write(&path, legacy).unwrap();
        let config = ServerPhysicalDesignMutationReceiptConfig::new(&path, 1_000_000).unwrap();
        database.close().unwrap();
        let (reached_tx, reached_rx) = std::sync::mpsc::sync_channel(1);
        let (resume_tx, resume_rx) = std::sync::mpsc::sync_channel(1);
        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
        let (drop_tx, drop_rx) = std::sync::mpsc::sync_channel(1);
        let owner_config = config.clone();
        let owner_root = root.clone();
        let owner = std::thread::spawn(move || {
            let database = Database::open_catalog(owner_root.join("catalog")).unwrap();
            TEST_MIGRATION_CHECKPOINT.with(|value| {
                *value.borrow_mut() = Some(TestMigrationCheckpoint {
                    reached: reached_tx,
                    resume: resume_rx,
                });
            });
            let journal =
                ServerPhysicalDesignMutationReceiptJournal::open(owner_config, identity, &database)
                    .unwrap();
            ready_tx.send(()).unwrap();
            drop_rx.recv().unwrap();
            drop(journal);
            database.close().unwrap();
        });

        reached_rx.recv().unwrap();
        let legacy_contender = secure_open_existing(&config, true).unwrap();
        assert!(matches!(
            lock_journal_exclusive(&legacy_contender),
            Err(ServerPhysicalDesignMutationReceiptJournalError::AlreadyInUse)
        ));
        drop(legacy_contender);
        resume_tx.send(()).unwrap();
        ready_rx.recv().unwrap();
        assert_eq!(
            journal_version(&fs::read(&path).unwrap()).unwrap(),
            CURRENT_VERSION
        );
        let v3_contender = secure_open_existing(&config, true).unwrap();
        assert!(matches!(
            lock_journal_exclusive(&v3_contender),
            Err(ServerPhysicalDesignMutationReceiptJournalError::AlreadyInUse)
        ));
        drop(v3_contender);
        drop_tx.send(()).unwrap();
        owner.join().unwrap();

        let reopened_database = Database::open_catalog(root.join("catalog")).unwrap();
        let reopened =
            ServerPhysicalDesignMutationReceiptJournal::open(config, identity, &reopened_database)
                .unwrap();
        assert_eq!(reopened.status().journal_incarnation, incarnation);
        assert_eq!(reopened.page(None, 2).unwrap().receipts.len(), 1);
        drop(reopened);
        reopened_database.close().unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn live_journal_replacement_blocks_begin_and_outcome() {
        for after_begin in [false, true] {
            let root = fixture_root("live-inode-replaced");
            let database = create_database(&root);
            let identity = database.physical_design_database_identity().unwrap();
            let path = root.join("receipts.nbmr");
            let config = ServerPhysicalDesignMutationReceiptConfig::new(&path, 1_000_000).unwrap();
            let mut journal =
                ServerPhysicalDesignMutationReceiptJournal::open(config, identity, &database)
                    .unwrap();
            let begin = index_begin(1);
            if after_begin {
                journal
                    .begin(begin.source, begin.evidence_epoch, begin.target.clone())
                    .unwrap();
            }
            let old_path = root.join("old.nbmr");
            fs::rename(&path, &old_path).unwrap();
            let old_bytes = fs::read(&old_path).unwrap();
            let replacement = b"replacement must survive";
            fs::write(&path, replacement).unwrap();
            let result = if after_begin {
                journal.finish(
                    begin.id,
                    ServerPhysicalDesignMutationReceiptOutcome::Rejected,
                )
            } else {
                journal
                    .begin(begin.source, begin.evidence_epoch, begin.target.clone())
                    .map(|_| ())
            };
            let blocked = journal.begin(begin.source, begin.evidence_epoch, begin.target);
            let current_bytes = fs::read(&path).unwrap();
            let orphan_bytes = fs::read(&old_path).unwrap();
            drop(journal);
            database.close().unwrap();
            fs::remove_dir_all(root).unwrap();
            assert!(matches!(
                result,
                Err(ServerPhysicalDesignMutationReceiptControlError::Journal(
                    ServerPhysicalDesignMutationReceiptJournalError::FinalPathChanged
                ))
            ));
            assert!(matches!(
                blocked,
                Err(ServerPhysicalDesignMutationReceiptControlError::RecoveryRequired)
            ));
            assert_eq!(current_bytes, replacement);
            assert_eq!(
                orphan_bytes, old_bytes,
                "appended to an orphaned journal inode"
            );
        }
    }

    #[test]
    fn header_is_exact_and_record_round_trips() {
        let identity = [7; 16];
        let journal_incarnation = ServerPhysicalDesignMutationReceiptJournalIncarnation([9; 16]);
        let header = encode_v2_header(identity, journal_incarnation);
        assert_eq!(
            header,
            [
                78, 66, 77, 82, 2, 0, 0, 0, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 9, 9,
                9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 189, 1, 108, 118,
            ]
        );
        assert_eq!(
            validate_v2_header(&header, identity).unwrap(),
            journal_incarnation
        );

        let begin = index_begin(1);
        let record = encode_begin(&begin).unwrap();
        let legacy_begin = encode_legacy_begin(&begin).unwrap();
        let legacy_outcome = encode_legacy_outcome(
            begin.id,
            ServerPhysicalDesignMutationReceiptOutcome::Rejected,
        )
        .unwrap();
        let v3_outcome = encode_outcome(
            begin.id,
            ServerPhysicalDesignMutationReceiptOutcome::Rejected,
        )
        .unwrap();
        assert_eq!(
            hex(&encode_v1_header(identity)),
            "4e424d5201000000070707070707070707070707070707076317a410"
        );
        assert_eq!(
            hex(&legacy_begin),
            "350000000100000001000000000000000101000004000000000000000200000000000000030000000f006974656d735f76616c75655f6964786ede42bc"
        );
        assert_eq!(
            hex(&legacy_outcome),
            "1000000002000000010000000000000006000000b21d0ed2"
        );
        assert_eq!(
            hex(&encode_v3_header(identity, journal_incarnation)),
            "4e424d5203000000070707070707070707070707070707070909090909090909090909090909090901baa045"
        );
        assert_eq!(
            hex(&record),
            "29000000d6ffffff010000000100000000000000b91405520101000004000000000000000200000000000000030000000f006974656d735f76616c75655f696478d09ab990"
        );
        assert_eq!(
            hex(&v3_outcome),
            "04000000fbffffff02000000010000000000000073949e3106000000f8a06d48"
        );
        let v1 = v1_bytes_with(identity, &[record.clone(), v3_outcome.clone()]);
        assert_eq!(
            decode_legacy_records(&v1, V1_HEADER_BYTES)
                .unwrap()
                .receipts
                .len(),
            1
        );
        let mut v2 = header.to_vec();
        v2.extend_from_slice(&legacy_begin);
        v2.extend_from_slice(&legacy_outcome);
        assert_eq!(
            decode_legacy_records(&v2, V2_HEADER_BYTES)
                .unwrap()
                .receipts
                .len(),
            1
        );
        assert_eq!(decode_record(&record).unwrap().unwrap_begin(), begin);
        for outcome in [
            ServerPhysicalDesignMutationReceiptOutcome::CreatedIndex {
                index_id: IndexId(5),
            },
            ServerPhysicalDesignMutationReceiptOutcome::CreatedColumnar {
                projection_id: ColumnarProjectionId(6),
            },
            ServerPhysicalDesignMutationReceiptOutcome::AlreadyAppliedIndex {
                index_id: IndexId(5),
            },
            ServerPhysicalDesignMutationReceiptOutcome::AlreadyAppliedColumnar {
                projection_id: ColumnarProjectionId(6),
            },
            ServerPhysicalDesignMutationReceiptOutcome::AlreadyCovered,
            ServerPhysicalDesignMutationReceiptOutcome::Rejected,
            ServerPhysicalDesignMutationReceiptOutcome::Failed,
            ServerPhysicalDesignMutationReceiptOutcome::RecoveredAppliedIndex {
                index_id: IndexId(5),
            },
            ServerPhysicalDesignMutationReceiptOutcome::RecoveredAppliedColumnar {
                projection_id: ColumnarProjectionId(6),
            },
            ServerPhysicalDesignMutationReceiptOutcome::RecoveredNotApplied,
            ServerPhysicalDesignMutationReceiptOutcome::RecoveredConflict,
        ] {
            match decode_record(&encode_outcome(begin.id, outcome).unwrap()) {
                Ok(ParsedRecord::Outcome(id, decoded)) => {
                    assert_eq!(id, begin.id);
                    assert_eq!(decoded, outcome);
                }
                _ => panic!("Outcome did not round trip"),
            }
        }
    }

    #[test]
    fn columnar_begin_round_trips_with_private_absolute_path() {
        let begin = MutationReceiptBegin {
            id: ServerPhysicalDesignMutationReceiptId(9),
            source: ServerPhysicalDesignMutationSource::LocalOperator,
            evidence_epoch: PhysicalDesignEvidenceEpoch(12),
            target: MutationReceiptTarget::Columnar {
                candidate: PhysicalColumnarCandidate {
                    table_id: TableId(8),
                    columns: vec![ColumnId(3), ColumnId(5)],
                },
                mode: PhysicalColumnarDesignMode::Incremental,
                placement: ServerPhysicalColumnarPlacementKey::new("hot-events").unwrap(),
                directory: PathBuf::from("/var/lib/netbadb/columnar/hot-events"),
            },
        };
        let decoded = decode_record(&encode_begin(&begin).unwrap())
            .unwrap()
            .unwrap_begin();
        assert_eq!(decoded, begin);
        let public = public_receipt(&decoded);
        assert!(matches!(
            public.target,
            ServerPhysicalDesignMutationTarget::Columnar { .. }
        ));
        assert!(!format!("{public:?}").contains("/var/lib/netbadb"));
    }

    #[test]
    fn columnar_begin_bounds_are_checked_before_encoding() {
        let base = MutationReceiptBegin {
            id: ServerPhysicalDesignMutationReceiptId(1),
            source: ServerPhysicalDesignMutationSource::Programmatic,
            evidence_epoch: PhysicalDesignEvidenceEpoch(1),
            target: MutationReceiptTarget::Columnar {
                candidate: PhysicalColumnarCandidate {
                    table_id: TableId(2),
                    columns: vec![ColumnId(1); MAX_COLUMN_IDS + 1],
                },
                mode: PhysicalColumnarDesignMode::Snapshot,
                placement: ServerPhysicalColumnarPlacementKey::new("reporting").unwrap(),
                directory: PathBuf::from("/reporting"),
            },
        };
        assert!(matches!(
            encode_begin(&base),
            Err(ServerPhysicalDesignMutationReceiptJournalError::ColumnCountExceeded { .. })
        ));

        let mut long_path = base;
        let MutationReceiptTarget::Columnar {
            candidate,
            directory,
            ..
        } = &mut long_path.target
        else {
            panic!("expected Columnar target");
        };
        candidate.columns.truncate(1);
        *directory = PathBuf::from(format!("/{}", "x".repeat(MAX_PATH_BYTES)));
        assert!(matches!(
            encode_begin(&long_path),
            Err(ServerPhysicalDesignMutationReceiptJournalError::PathTooLong { .. })
        ));

        #[cfg(unix)]
        {
            use std::ffi::OsString;
            use std::os::unix::ffi::OsStringExt;

            let MutationReceiptTarget::Columnar { directory, .. } = &mut long_path.target else {
                panic!("expected Columnar target");
            };
            *directory = PathBuf::from(OsString::from_vec(vec![b'/', 0xff]));
            assert!(matches!(
                encode_begin(&long_path),
                Err(ServerPhysicalDesignMutationReceiptJournalError::PathNotUtf8)
            ));
        }
    }

    #[test]
    fn every_record_truncation_is_an_incomplete_tail() {
        let begin = encode_begin(&index_begin(1)).unwrap();
        let outcome = encode_outcome(
            ServerPhysicalDesignMutationReceiptId(1),
            ServerPhysicalDesignMutationReceiptOutcome::Rejected,
        )
        .unwrap();
        let complete = bytes_with(&[begin.clone(), outcome.clone()]);
        for length in 0..V3_HEADER_BYTES {
            assert!(validate_v3_header(&complete[..length], [7; 16]).is_err());
        }
        for length in V3_HEADER_BYTES..complete.len() {
            let decoded = decode_records(&complete[..length], V3_HEADER_BYTES).unwrap();
            let expected = if length < V3_HEADER_BYTES + begin.len() {
                V3_HEADER_BYTES
            } else {
                V3_HEADER_BYTES + begin.len()
            };
            assert_eq!(decoded.valid_bytes, expected);
        }
        assert_eq!(
            decode_records(&complete, V3_HEADER_BYTES)
                .unwrap()
                .valid_bytes,
            complete.len()
        );
    }

    #[test]
    fn malformed_headers_fail_closed() {
        let incarnation = ServerPhysicalDesignMutationReceiptJournalIncarnation([9; 16]);
        let mut bad_magic = encode_v2_header([7; 16], incarnation);
        bad_magic[0] = b'X';
        assert!(matches!(
            validate_v2_header(&bad_magic, [7; 16]),
            Err(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
                "bad header magic"
            ))
        ));

        let mut zero_version = encode_v2_header([7; 16], incarnation);
        zero_version[4..6].copy_from_slice(&0_u16.to_le_bytes());
        let checksum = crc32c::crc32c(&zero_version[..40]).to_le_bytes();
        zero_version[40..].copy_from_slice(&checksum);
        assert!(matches!(
            validate_v2_header(&zero_version, [7; 16]),
            Err(ServerPhysicalDesignMutationReceiptJournalError::UnsupportedVersion(0))
        ));

        let mut future = encode_v2_header([7; 16], incarnation);
        future[4..6].copy_from_slice(&3_u16.to_le_bytes());
        let checksum = crc32c::crc32c(&future[..40]).to_le_bytes();
        future[40..].copy_from_slice(&checksum);
        assert!(matches!(
            validate_v2_header(&future, [7; 16]),
            Err(ServerPhysicalDesignMutationReceiptJournalError::UnsupportedVersion(3))
        ));

        let mut reserved = encode_v2_header([7; 16], incarnation);
        reserved[6] = 1;
        let checksum = crc32c::crc32c(&reserved[..40]).to_le_bytes();
        reserved[40..].copy_from_slice(&checksum);
        assert!(validate_v2_header(&reserved, [7; 16]).is_err());

        let mut zero_incarnation = encode_v2_header([7; 16], incarnation);
        zero_incarnation[24..40].fill(0);
        let checksum = crc32c::crc32c(&zero_incarnation[..40]).to_le_bytes();
        zero_incarnation[40..].copy_from_slice(&checksum);
        assert!(validate_v2_header(&zero_incarnation, [7; 16]).is_err());

        let mut bad_crc = encode_v2_header([7; 16], incarnation);
        bad_crc[40] ^= 1;
        assert!(validate_v2_header(&bad_crc, [7; 16]).is_err());
        assert!(matches!(
            validate_v2_header(&encode_v2_header([8; 16], incarnation), [7; 16]),
            Err(ServerPhysicalDesignMutationReceiptJournalError::DatabaseIdentityMismatch)
        ));

        let v1 = encode_v1_header([7; 16]);
        assert_eq!(journal_version(&v1).unwrap(), V1_VERSION);
        validate_v1_header(&v1, [7; 16]).unwrap();
    }

    #[test]
    fn malformed_complete_records_fail_closed() {
        let begin = encode_begin(&index_begin(1)).unwrap();

        let mut bad_crc = begin.clone();
        let last = bad_crc.len() - 1;
        bad_crc[last] ^= 1;
        assert!(decode_records(&bytes_with(&[bad_crc]), V2_HEADER_BYTES).is_err());

        let unknown = encode_record(99, ServerPhysicalDesignMutationReceiptId(1), &[]).unwrap();
        assert!(decode_records(&bytes_with(&[unknown]), V2_HEADER_BYTES).is_err());
        let unknown_outcome = encode_record(
            OUTCOME_TAG,
            ServerPhysicalDesignMutationReceiptId(1),
            &[99, 0, 0, 0],
        )
        .unwrap();
        assert!(
            decode_records(
                &bytes_with(&[begin.clone(), unknown_outcome]),
                V2_HEADER_BYTES
            )
            .is_err()
        );

        let mut record_reserved = begin.clone();
        record_reserved[9] = 1;
        replace_record_header_crc(&mut record_reserved);
        assert!(decode_records(&bytes_with(&[record_reserved]), V2_HEADER_BYTES).is_err());

        let mut body_reserved = begin.clone();
        body_reserved[V3_RECORD_HEADER_BYTES + 2] = 1;
        replace_record_crc(&mut body_reserved);
        assert!(decode_records(&bytes_with(&[body_reserved]), V2_HEADER_BYTES).is_err());

        let mut zero_id = begin.clone();
        zero_id[12..20].fill(0);
        replace_record_header_crc(&mut zero_id);
        assert!(decode_records(&bytes_with(&[zero_id]), V2_HEADER_BYTES).is_err());

        let mut oversized = encode_v3_header(
            [7; 16],
            ServerPhysicalDesignMutationReceiptJournalIncarnation([9; 16]),
        )
        .to_vec();
        let body_len = MAX_MUTATION_RECEIPT_RECORD_BYTES as u32;
        let mut header = [0_u8; V3_RECORD_HEADER_BYTES];
        header[..4].copy_from_slice(&body_len.to_le_bytes());
        header[4..8].copy_from_slice(&(!body_len).to_le_bytes());
        header[8] = BEGIN_TAG;
        header[12..20].copy_from_slice(&1_u64.to_le_bytes());
        let checksum = crc32c::crc32c(&header[..20]).to_le_bytes();
        header[20..24].copy_from_slice(&checksum);
        oversized.extend_from_slice(&header);
        assert!(decode_records(&oversized, V2_HEADER_BYTES).is_err());
    }

    #[test]
    fn invalid_record_ordering_fails_closed() {
        let first = encode_begin(&index_begin(1)).unwrap();
        let duplicate = encode_begin(&index_begin(1)).unwrap();
        assert!(decode_records(&bytes_with(&[first.clone(), duplicate]), V2_HEADER_BYTES).is_err());

        let outcome = encode_outcome(
            ServerPhysicalDesignMutationReceiptId(1),
            ServerPhysicalDesignMutationReceiptOutcome::Rejected,
        )
        .unwrap();
        assert!(
            decode_records(&bytes_with(std::slice::from_ref(&outcome)), V2_HEADER_BYTES).is_err()
        );
        assert!(
            decode_records(
                &bytes_with(&[first.clone(), outcome.clone(), outcome]),
                V2_HEADER_BYTES
            )
            .is_err()
        );

        let second = encode_begin(&index_begin(2)).unwrap();
        let second_outcome = encode_outcome(
            ServerPhysicalDesignMutationReceiptId(2),
            ServerPhysicalDesignMutationReceiptOutcome::Rejected,
        )
        .unwrap();
        let lower = encode_begin(&index_begin(1)).unwrap();
        assert!(
            decode_records(
                &bytes_with(&[second, second_outcome, lower]),
                V2_HEADER_BYTES
            )
            .is_err()
        );
    }

    #[test]
    fn v3_protected_lengths_headers_bodies_and_middle_records_fail_closed() {
        let root = fixture_root("v3-corruption");
        let database = create_database(&root);
        let identity = database.physical_design_database_identity().unwrap();
        let path = root.join("receipts.nbmr");
        let config = ServerPhysicalDesignMutationReceiptConfig::new(&path, 1_000_000).unwrap();
        let begin = encode_begin(&index_begin(1)).unwrap();
        let outcome = encode_outcome(
            ServerPhysicalDesignMutationReceiptId(1),
            ServerPhysicalDesignMutationReceiptOutcome::Rejected,
        )
        .unwrap();
        let second_begin = encode_begin(&index_begin(2)).unwrap();
        let second_outcome = encode_outcome(
            ServerPhysicalDesignMutationReceiptId(2),
            ServerPhysicalDesignMutationReceiptOutcome::Rejected,
        )
        .unwrap();
        let mut cases = Vec::new();
        for delta in [1_u32, 8] {
            let mut record = begin.clone();
            let length = read_u32(&record[..4]).saturating_add(delta);
            record[..4].copy_from_slice(&length.to_le_bytes());
            cases.push(bytes_with(&[record]));
        }
        let mut one_bit_length = begin.clone();
        one_bit_length[0] ^= 1;
        cases.push(bytes_with(&[one_bit_length]));
        let mut outcome_length = outcome.clone();
        outcome_length[0] ^= 8;
        cases.push(bytes_with(&[begin.clone(), outcome_length]));
        let mut header_crc = begin.clone();
        header_crc[20] ^= 1;
        cases.push(bytes_with(&[header_crc]));
        let mut body_crc = begin.clone();
        let body_crc_offset = body_crc.len() - 1;
        body_crc[body_crc_offset] ^= 1;
        cases.push(bytes_with(&[body_crc]));
        let mut tag = begin.clone();
        tag[8] = 99;
        replace_record_header_crc(&mut tag);
        cases.push(bytes_with(&[tag]));
        let mut reserved = begin.clone();
        reserved[9] = 1;
        replace_record_header_crc(&mut reserved);
        cases.push(bytes_with(&[reserved]));
        let mut corrupt_middle = outcome.clone();
        let last = corrupt_middle.len() - 1;
        corrupt_middle[last] ^= 1;
        cases.push(bytes_with(&[
            begin.clone(),
            corrupt_middle,
            second_begin,
            second_outcome,
        ]));

        for bytes in cases {
            fs::write(&path, &bytes).unwrap();
            assert!(
                ServerPhysicalDesignMutationReceiptJournal::open(
                    config.clone(),
                    identity,
                    &database
                )
                .is_err()
            );
            assert_eq!(fs::read(&path).unwrap(), bytes);
        }
        database.close().unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn outcome_target_domain_is_shared_by_decode_migration_and_finish() {
        let root = fixture_root("domain-mismatch");
        let database = create_database(&root);
        let identity = database.physical_design_database_identity().unwrap();
        let path = root.join("receipts.nbmr");
        let config = ServerPhysicalDesignMutationReceiptConfig::new(&path, 1_000_000).unwrap();
        let begin = index_begin(1);
        let incompatible = [
            ServerPhysicalDesignMutationReceiptOutcome::CreatedColumnar {
                projection_id: ColumnarProjectionId(7),
            },
            ServerPhysicalDesignMutationReceiptOutcome::AlreadyAppliedColumnar {
                projection_id: ColumnarProjectionId(7),
            },
            ServerPhysicalDesignMutationReceiptOutcome::RecoveredAppliedColumnar {
                projection_id: ColumnarProjectionId(7),
            },
        ];
        let columnar_target = MutationReceiptTarget::Columnar {
            candidate: PhysicalColumnarCandidate {
                table_id: TableId(2),
                columns: vec![ColumnId(3)],
            },
            mode: PhysicalColumnarDesignMode::Snapshot,
            placement: ServerPhysicalColumnarPlacementKey::new("domain").unwrap(),
            directory: root.join("domain"),
        };
        for outcome in [
            ServerPhysicalDesignMutationReceiptOutcome::CreatedIndex {
                index_id: IndexId(7),
            },
            ServerPhysicalDesignMutationReceiptOutcome::AlreadyAppliedIndex {
                index_id: IndexId(7),
            },
            ServerPhysicalDesignMutationReceiptOutcome::RecoveredAppliedIndex {
                index_id: IndexId(7),
            },
        ] {
            assert!(validate_outcome_for_target(&columnar_target, outcome).is_err());
        }
        for outcome in incompatible {
            assert!(validate_outcome_for_target(&begin.target, outcome).is_err());
            let current = bytes_with(&[
                encode_begin(&begin).unwrap(),
                encode_outcome(begin.id, outcome).unwrap(),
            ]);
            assert!(decode_v3_records(&current, V3_HEADER_BYTES).is_err());
            let legacy = v1_bytes_with(
                *identity.as_bytes(),
                &[
                    encode_begin(&begin).unwrap(),
                    encode_outcome(begin.id, outcome).unwrap(),
                ],
            );
            fs::write(&path, &legacy).unwrap();
            assert!(
                ServerPhysicalDesignMutationReceiptJournal::open(
                    config.clone(),
                    identity,
                    &database
                )
                .is_err()
            );
            assert_eq!(fs::read(&path).unwrap(), legacy);
        }

        fs::remove_file(&path).unwrap();
        let mut journal =
            ServerPhysicalDesignMutationReceiptJournal::open(config.clone(), identity, &database)
                .unwrap();
        let id = journal
            .begin(
                ServerPhysicalDesignMutationSource::Programmatic,
                PhysicalDesignEvidenceEpoch(1),
                begin.target,
            )
            .unwrap();
        let before = fs::read(&path).unwrap();
        assert!(
            journal
                .finish(
                    id,
                    ServerPhysicalDesignMutationReceiptOutcome::CreatedColumnar {
                        projection_id: ColumnarProjectionId(8),
                    },
                )
                .is_err()
        );
        assert_eq!(fs::read(&path).unwrap(), before);
        drop(journal);
        database.close().unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn config_freezes_parent_without_creating_file_and_rejects_bad_targets() {
        let suffix = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "netbadb-nbmr-config-{}-{suffix}",
            std::process::id()
        ));
        fs::create_dir_all(&root).unwrap();
        let path = root.join("receipts.nbmr");
        let config = ServerPhysicalDesignMutationReceiptConfig::new(&path, MIN_FILE_BYTES).unwrap();
        assert_eq!(
            config.path(),
            fs::canonicalize(&root).unwrap().join("receipts.nbmr")
        );
        assert!(!path.exists());
        assert!(matches!(
            ServerPhysicalDesignMutationReceiptConfig::new(&path, MIN_FILE_BYTES - 1),
            Err(ServerPhysicalDesignMutationReceiptConfigError::MaxFileBytesTooSmall { .. })
        ));

        fs::create_dir(&path).unwrap();
        assert!(matches!(
            ServerPhysicalDesignMutationReceiptConfig::new(&path, MIN_FILE_BYTES),
            Err(ServerPhysicalDesignMutationReceiptConfigError::Path(
                ServerPhysicalDesignMutationReceiptPathError::ExistingObjectNotRegular(_)
            ))
        ));
        fs::remove_dir(&path).unwrap();

        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;
            let target = root.join("target");
            File::create(&target).unwrap();
            symlink(&target, &path).unwrap();
            assert!(ServerPhysicalDesignMutationReceiptConfig::new(&path, MIN_FILE_BYTES).is_err());
            fs::remove_file(&path).unwrap();
            fs::remove_file(target).unwrap();
        }
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn unresolved_begin_is_reconciled_and_ids_continue() {
        let root = fixture_root("reconcile");
        let database = create_database(&root);
        let identity = database.physical_design_database_identity().unwrap();
        let config =
            ServerPhysicalDesignMutationReceiptConfig::new(root.join("receipts.nbmr"), 1_000_000)
                .unwrap();
        let mut journal =
            ServerPhysicalDesignMutationReceiptJournal::open(config.clone(), identity, &database)
                .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(config.path()).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        assert_eq!(
            journal
                .begin(
                    ServerPhysicalDesignMutationSource::Programmatic,
                    PhysicalDesignEvidenceEpoch(3),
                    index_begin(1).target,
                )
                .unwrap(),
            ServerPhysicalDesignMutationReceiptId(1)
        );
        drop(journal);

        let mut reopened =
            ServerPhysicalDesignMutationReceiptJournal::open(config, identity, &database).unwrap();
        let page = reopened.page(None, 128).unwrap();
        assert_eq!(page.receipts.len(), 1);
        assert_eq!(
            page.receipts[0].outcome,
            ServerPhysicalDesignMutationReceiptOutcome::RecoveredNotApplied
        );
        assert_eq!(
            reopened
                .begin(
                    ServerPhysicalDesignMutationSource::Programmatic,
                    PhysicalDesignEvidenceEpoch(4),
                    index_begin(2).target,
                )
                .unwrap(),
            ServerPhysicalDesignMutationReceiptId(2)
        );
        reopened
            .finish(
                ServerPhysicalDesignMutationReceiptId(2),
                ServerPhysicalDesignMutationReceiptOutcome::Rejected,
            )
            .unwrap();
        drop(reopened);
        database.close().unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn unresolved_columnar_begin_uses_original_absolute_path() {
        let root = fixture_root("columnar-reconcile");
        let database = create_database(&root);
        let identity = database.physical_design_database_identity().unwrap();
        let config =
            ServerPhysicalDesignMutationReceiptConfig::new(root.join("receipts.nbmr"), 1_000_000)
                .unwrap();
        let original_directory = fs::canonicalize(&root).unwrap().join("old-root/reporting");
        let mut journal =
            ServerPhysicalDesignMutationReceiptJournal::open(config.clone(), identity, &database)
                .unwrap();
        journal
            .begin(
                ServerPhysicalDesignMutationSource::Programmatic,
                PhysicalDesignEvidenceEpoch(1),
                MutationReceiptTarget::Columnar {
                    candidate: PhysicalColumnarCandidate {
                        table_id: TableId(2),
                        columns: vec![ColumnId(1)],
                    },
                    mode: PhysicalColumnarDesignMode::Snapshot,
                    placement: ServerPhysicalColumnarPlacementKey::new("reporting").unwrap(),
                    directory: original_directory,
                },
            )
            .unwrap();
        drop(journal);

        let reopened =
            ServerPhysicalDesignMutationReceiptJournal::open(config, identity, &database).unwrap();
        let page = reopened.page(None, 1).unwrap();
        assert_eq!(
            page.receipts[0].outcome,
            ServerPhysicalDesignMutationReceiptOutcome::RecoveredNotApplied
        );
        assert!(!format!("{page:?}").contains("old-root"));
        drop(reopened);
        database.close().unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn partial_tail_is_truncated_but_complete_bad_crc_is_preserved() {
        let root = fixture_root("tail");
        let database = create_database(&root);
        let identity = database.physical_design_database_identity().unwrap();
        let config =
            ServerPhysicalDesignMutationReceiptConfig::new(root.join("receipts.nbmr"), 1_000_000)
                .unwrap();
        let journal =
            ServerPhysicalDesignMutationReceiptJournal::open(config.clone(), identity, &database)
                .unwrap();
        drop(journal);
        let stable_len = fs::metadata(config.path()).unwrap().len();
        let partial = encode_begin(&index_begin(1)).unwrap();
        OpenOptions::new()
            .append(true)
            .open(config.path())
            .unwrap()
            .write_all(&partial[..partial.len() - 1])
            .unwrap();
        drop(
            ServerPhysicalDesignMutationReceiptJournal::open(config.clone(), identity, &database)
                .unwrap(),
        );
        assert_eq!(fs::metadata(config.path()).unwrap().len(), stable_len);

        let mut pending =
            ServerPhysicalDesignMutationReceiptJournal::open(config.clone(), identity, &database)
                .unwrap();
        let receipt_id = pending
            .begin(
                ServerPhysicalDesignMutationSource::Programmatic,
                PhysicalDesignEvidenceEpoch(1),
                index_begin(1).target,
            )
            .unwrap();
        drop(pending);
        let partial_outcome = encode_outcome(
            receipt_id,
            ServerPhysicalDesignMutationReceiptOutcome::Rejected,
        )
        .unwrap();
        OpenOptions::new()
            .append(true)
            .open(config.path())
            .unwrap()
            .write_all(&partial_outcome[..partial_outcome.len() - 1])
            .unwrap();
        let reconciled =
            ServerPhysicalDesignMutationReceiptJournal::open(config.clone(), identity, &database)
                .unwrap();
        assert_eq!(
            reconciled.page(None, 1).unwrap().receipts[0].outcome,
            ServerPhysicalDesignMutationReceiptOutcome::RecoveredNotApplied
        );
        drop(reconciled);

        let mut bad = encode_begin(&index_begin(2)).unwrap();
        let last = bad.len() - 1;
        bad[last] ^= 1;
        OpenOptions::new()
            .append(true)
            .open(config.path())
            .unwrap()
            .write_all(&bad)
            .unwrap();
        let corrupt_len = fs::metadata(config.path()).unwrap().len();
        assert!(
            ServerPhysicalDesignMutationReceiptJournal::open(config.clone(), identity, &database)
                .is_err()
        );
        assert_eq!(fs::metadata(config.path()).unwrap().len(), corrupt_len);
        database.close().unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn database_identity_mismatch_does_not_change_journal() {
        let first_root = fixture_root("identity-a");
        let second_root = fixture_root("identity-b");
        let first = create_database(&first_root);
        let second = create_database(&second_root);
        let config = ServerPhysicalDesignMutationReceiptConfig::new(
            first_root.join("receipts.nbmr"),
            1_000_000,
        )
        .unwrap();
        drop(
            ServerPhysicalDesignMutationReceiptJournal::open(
                config.clone(),
                first.physical_design_database_identity().unwrap(),
                &first,
            )
            .unwrap(),
        );
        let before = fs::read(config.path()).unwrap();
        assert!(matches!(
            ServerPhysicalDesignMutationReceiptJournal::open(
                config.clone(),
                second.physical_design_database_identity().unwrap(),
                &second,
            ),
            Err(ServerPhysicalDesignMutationReceiptJournalError::DatabaseIdentityMismatch)
        ));
        assert_eq!(fs::read(config.path()).unwrap(), before);
        first.close().unwrap();
        second.close().unwrap();
        fs::remove_dir_all(first_root).unwrap();
        fs::remove_dir_all(second_root).unwrap();
    }

    #[test]
    fn capacity_rejection_happens_before_begin_and_id_allocation() {
        let root = fixture_root("capacity");
        let database = create_database(&root);
        let identity = database.physical_design_database_identity().unwrap();
        let config = ServerPhysicalDesignMutationReceiptConfig::new(
            root.join("receipts.nbmr"),
            MIN_FILE_BYTES,
        )
        .unwrap();
        let mut journal =
            ServerPhysicalDesignMutationReceiptJournal::open(config, identity, &database).unwrap();
        let rejected_id = loop {
            let id = journal.next_id;
            let target = MutationReceiptTarget::Index {
                candidate: PhysicalIndexCandidate {
                    table_id: TableId(2),
                    column_id: ColumnId(3),
                },
                index_name: IndexName::new(format!("items_value_idx_{id}")).unwrap(),
            };
            match journal.begin(
                ServerPhysicalDesignMutationSource::Programmatic,
                PhysicalDesignEvidenceEpoch(1),
                target,
            ) {
                Ok(receipt_id) => journal
                    .finish(
                        receipt_id,
                        ServerPhysicalDesignMutationReceiptOutcome::Rejected,
                    )
                    .unwrap(),
                Err(ServerPhysicalDesignMutationReceiptControlError::Journal(
                    ServerPhysicalDesignMutationReceiptJournalError::CapacityExceeded,
                )) => break id,
                Err(error) => panic!("unexpected capacity test error: {error}"),
            }
        };
        let file_len = journal.file_len;
        assert_eq!(journal.next_id, rejected_id);
        assert!(matches!(
            journal.begin(
                ServerPhysicalDesignMutationSource::Programmatic,
                PhysicalDesignEvidenceEpoch(1),
                MutationReceiptTarget::Index {
                    candidate: PhysicalIndexCandidate {
                        table_id: TableId(2),
                        column_id: ColumnId(3),
                    },
                    index_name: IndexName::new("capacity_retry").unwrap(),
                },
            ),
            Err(ServerPhysicalDesignMutationReceiptControlError::Journal(
                ServerPhysicalDesignMutationReceiptJournalError::CapacityExceeded
            ))
        ));
        assert_eq!(journal.next_id, rejected_id);
        assert_eq!(journal.file_len, file_len);
        drop(journal);
        database.close().unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn fresh_v3_journals_have_stable_distinct_nonzero_namespaces() {
        let root = fixture_root("v3-identities");
        let database = create_database(&root);
        let identity = database.physical_design_database_identity().unwrap();
        let first_config =
            ServerPhysicalDesignMutationReceiptConfig::new(root.join("first.nbmr"), 1_000_000)
                .unwrap();
        let second_config =
            ServerPhysicalDesignMutationReceiptConfig::new(root.join("second.nbmr"), 1_000_000)
                .unwrap();
        let zero_config =
            ServerPhysicalDesignMutationReceiptConfig::new(root.join("zero.nbmr"), 1_000_000)
                .unwrap();
        let random_failure_config = ServerPhysicalDesignMutationReceiptConfig::new(
            root.join("random-failure.nbmr"),
            1_000_000,
        )
        .unwrap();
        queue_journal_incarnations([[1; 16], [2; 16], [0; 16]]);
        TEST_RANDOMNESS_FAILURE.with(|failure| failure.set(true));
        assert!(matches!(
            ServerPhysicalDesignMutationReceiptJournal::open(
                random_failure_config.clone(),
                identity,
                &database
            ),
            Err(ServerPhysicalDesignMutationReceiptJournalError::Randomness(
                _
            ))
        ));
        assert!(!random_failure_config.path().exists());

        let mut first = ServerPhysicalDesignMutationReceiptJournal::open(
            first_config.clone(),
            identity,
            &database,
        )
        .unwrap();
        let first_incarnation = first.status().journal_incarnation;
        assert_eq!(first_incarnation.as_bytes(), &[1; 16]);
        let first_id = first
            .begin(
                ServerPhysicalDesignMutationSource::Programmatic,
                PhysicalDesignEvidenceEpoch(1),
                index_begin(1).target,
            )
            .unwrap();
        assert_eq!(first_id, ServerPhysicalDesignMutationReceiptId(1));
        first
            .finish(
                first_id,
                ServerPhysicalDesignMutationReceiptOutcome::Rejected,
            )
            .unwrap();
        let old_cursor =
            ServerPhysicalDesignMutationReceiptCursor::new(first_incarnation, first_id).unwrap();
        assert_eq!(
            journal_version(&fs::read(first_config.path()).unwrap()).unwrap(),
            3
        );
        drop(first);
        let reopened = ServerPhysicalDesignMutationReceiptJournal::open(
            first_config.clone(),
            identity,
            &database,
        )
        .unwrap();
        assert_eq!(reopened.status().journal_incarnation, first_incarnation);
        drop(reopened);
        let copy_path = root.join("copied.nbmr");
        fs::copy(first_config.path(), &copy_path).unwrap();
        let copy_config =
            ServerPhysicalDesignMutationReceiptConfig::new(&copy_path, 1_000_000).unwrap();
        let copied =
            ServerPhysicalDesignMutationReceiptJournal::open(copy_config, identity, &database)
                .unwrap();
        assert_eq!(copied.status().journal_incarnation, first_incarnation);
        drop(copied);

        let mut second =
            ServerPhysicalDesignMutationReceiptJournal::open(second_config, identity, &database)
                .unwrap();
        assert_eq!(second.status().journal_incarnation.as_bytes(), &[2; 16]);
        assert_ne!(second.status().journal_incarnation, first_incarnation);
        let second_id = second
            .begin(
                ServerPhysicalDesignMutationSource::Programmatic,
                PhysicalDesignEvidenceEpoch(1),
                index_begin(1).target,
            )
            .unwrap();
        assert_eq!(second_id, ServerPhysicalDesignMutationReceiptId(1));
        second
            .finish(
                second_id,
                ServerPhysicalDesignMutationReceiptOutcome::Rejected,
            )
            .unwrap();
        assert!(matches!(
            second.scoped_page(Some(old_cursor), 1),
            Err(ServerPhysicalDesignMutationReceiptControlError::JournalChanged { .. })
        ));
        drop(second);

        assert!(matches!(
            ServerPhysicalDesignMutationReceiptJournal::open(
                zero_config.clone(),
                identity,
                &database
            ),
            Err(ServerPhysicalDesignMutationReceiptJournalError::GeneratedZeroJournalIncarnation)
        ));
        assert!(!zero_config.path().exists());
        database.close().unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn scoped_pages_reject_another_namespace_and_status_reads_stay_pure() {
        let root = fixture_root("scoped-pages");
        let database = create_database(&root);
        let identity = database.physical_design_database_identity().unwrap();
        let config =
            ServerPhysicalDesignMutationReceiptConfig::new(root.join("receipts.nbmr"), 1_000_000)
                .unwrap();
        queue_journal_incarnations([[3; 16]]);
        let mut journal =
            ServerPhysicalDesignMutationReceiptJournal::open(config.clone(), identity, &database)
                .unwrap();
        for id in 1..=3 {
            let receipt_id = journal
                .begin(
                    ServerPhysicalDesignMutationSource::Programmatic,
                    PhysicalDesignEvidenceEpoch(id),
                    index_begin(id).target,
                )
                .unwrap();
            journal
                .finish(
                    receipt_id,
                    ServerPhysicalDesignMutationReceiptOutcome::Rejected,
                )
                .unwrap();
        }
        let bytes_before = fs::read(config.path()).unwrap();
        let status = journal.status();
        assert_eq!(status.journal_incarnation.as_bytes(), &[3; 16]);
        assert!(!status.recovery_required);
        assert_eq!(
            status.latest_receipt_id,
            Some(ServerPhysicalDesignMutationReceiptId(3))
        );
        assert_eq!(status.max_receipts_per_read, 128);

        let first = journal.scoped_page(None, 2).unwrap();
        assert_eq!(first.journal_incarnation, status.journal_incarnation);
        assert_eq!(
            first
                .receipts
                .iter()
                .map(|receipt| receipt.id.0)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        let cursor = first.next_after.unwrap();
        assert_eq!(cursor.journal_incarnation(), status.journal_incarnation);
        assert_eq!(
            cursor.receipt_id(),
            ServerPhysicalDesignMutationReceiptId(2)
        );
        let second = journal.scoped_page(Some(cursor), 2).unwrap();
        assert_eq!(
            second
                .receipts
                .iter()
                .map(|receipt| receipt.id.0)
                .collect::<Vec<_>>(),
            vec![3]
        );
        assert_eq!(second.next_after, None);

        let wrong = ServerPhysicalDesignMutationReceiptCursor::new(
            ServerPhysicalDesignMutationReceiptJournalIncarnation([4; 16]),
            ServerPhysicalDesignMutationReceiptId(2),
        )
        .unwrap();
        assert!(matches!(
            journal.scoped_page(Some(wrong), 1),
            Err(ServerPhysicalDesignMutationReceiptControlError::JournalChanged {
                expected,
                actual,
            }) if expected.as_bytes() == &[4; 16] && actual == status.journal_incarnation
        ));
        assert!(matches!(
            ServerPhysicalDesignMutationReceiptCursor::new(
                status.journal_incarnation,
                ServerPhysicalDesignMutationReceiptId(0),
            ),
            Err(ServerPhysicalDesignMutationReceiptCursorError::ZeroReceiptId)
        ));
        assert!(matches!(
            ServerPhysicalDesignMutationReceiptJournalIncarnation::new([0; 16]),
            Err(ServerPhysicalDesignMutationReceiptJournalIncarnationError::Zero)
        ));

        journal
            .begin(
                ServerPhysicalDesignMutationSource::Programmatic,
                PhysicalDesignEvidenceEpoch(4),
                index_begin(4).target,
            )
            .unwrap();
        journal.mark_recovery_required();
        assert!(journal.status().recovery_required);
        assert_eq!(
            journal.scoped_page(None, 4).unwrap().receipts[3].outcome,
            ServerPhysicalDesignMutationReceiptOutcome::Pending
        );
        let bytes_after_pending = fs::read(config.path()).unwrap();
        let _ = journal.status();
        let _ = journal.scoped_page(None, 4).unwrap();
        assert_eq!(fs::read(config.path()).unwrap(), bytes_after_pending);
        assert_ne!(bytes_before, bytes_after_pending);
        drop(journal);
        database.close().unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn v1_migration_preserves_history_and_receipt_id_continuity() {
        let root = fixture_root("v1-history");
        let database = create_database(&root);
        let identity = database.physical_design_database_identity().unwrap();
        let database_before = database.current_database_snapshot().unwrap();
        let schema_before = database.schema_generation();
        let mut begin = index_begin(42);
        begin.source = ServerPhysicalDesignMutationSource::LocalOperator;
        begin.evidence_epoch = PhysicalDesignEvidenceEpoch(17);
        let outcome = ServerPhysicalDesignMutationReceiptOutcome::AlreadyAppliedIndex {
            index_id: IndexId(9),
        };
        let records = vec![
            encode_begin(&begin).unwrap(),
            encode_outcome(begin.id, outcome).unwrap(),
        ];
        let config =
            ServerPhysicalDesignMutationReceiptConfig::new(root.join("receipts.nbmr"), 1_000_000)
                .unwrap();
        fs::write(config.path(), v1_bytes_with(*identity.as_bytes(), &records)).unwrap();
        queue_journal_incarnations([[5; 16]]);

        let mut journal =
            ServerPhysicalDesignMutationReceiptJournal::open(config.clone(), identity, &database)
                .unwrap();
        assert_eq!(journal.status().journal_incarnation.as_bytes(), &[5; 16]);
        assert_eq!(
            journal_version(&fs::read(config.path()).unwrap()).unwrap(),
            3
        );
        let page = journal.page(None, 1).unwrap();
        let mut expected = public_receipt(&begin);
        expected.outcome = outcome;
        assert_eq!(page.receipts, vec![expected]);
        let next = journal
            .begin(
                ServerPhysicalDesignMutationSource::Programmatic,
                PhysicalDesignEvidenceEpoch(18),
                index_begin(43).target,
            )
            .unwrap();
        assert_eq!(next, ServerPhysicalDesignMutationReceiptId(43));
        journal
            .finish(next, ServerPhysicalDesignMutationReceiptOutcome::Rejected)
            .unwrap();
        assert_eq!(
            database.current_database_snapshot().unwrap(),
            database_before
        );
        assert_eq!(database.schema_generation(), schema_before);
        assert!(database.indexes(TableId(2)).unwrap().is_empty());
        assert!(database.inspect_columnar_projections().is_empty());
        drop(journal);
        database.close().unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn v2_migration_preserves_incarnation_cursor_and_reconciles_pending_begin() {
        let root = fixture_root("v2-migration");
        let database = create_database(&root);
        let identity = database.physical_design_database_identity().unwrap();
        let incarnation = ServerPhysicalDesignMutationReceiptJournalIncarnation([12; 16]);
        let first = index_begin(7);
        let pending = index_begin(8);
        let mut legacy = encode_v2_header(*identity.as_bytes(), incarnation).to_vec();
        legacy.extend_from_slice(&encode_legacy_begin(&first).unwrap());
        legacy.extend_from_slice(
            &encode_legacy_outcome(
                first.id,
                ServerPhysicalDesignMutationReceiptOutcome::Rejected,
            )
            .unwrap(),
        );
        legacy.extend_from_slice(&encode_legacy_begin(&pending).unwrap());
        let path = root.join("receipts.nbmr");
        fs::write(&path, legacy).unwrap();
        let config = ServerPhysicalDesignMutationReceiptConfig::new(&path, 1_000_000).unwrap();
        let old_cursor =
            ServerPhysicalDesignMutationReceiptCursor::new(incarnation, first.id).unwrap();

        let journal =
            ServerPhysicalDesignMutationReceiptJournal::open(config, identity, &database).unwrap();
        assert_eq!(journal.status().journal_incarnation, incarnation);
        assert_eq!(
            journal_version(&fs::read(&path).unwrap()).unwrap(),
            CURRENT_VERSION
        );
        let page = journal.scoped_page(Some(old_cursor), 2).unwrap();
        assert_eq!(page.receipts.len(), 1);
        assert_eq!(page.receipts[0].id, pending.id);
        assert_eq!(
            page.receipts[0].outcome,
            ServerPhysicalDesignMutationReceiptOutcome::RecoveredNotApplied
        );
        drop(journal);
        database.close().unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn legacy_migration_reserves_recovered_outcome_before_publication() {
        let root = fixture_root("migration-recovery-capacity");
        let database = create_database(&root);
        let identity = database.physical_design_database_identity().unwrap();
        let incarnation = ServerPhysicalDesignMutationReceiptJournalIncarnation([13; 16]);
        let mut parsed = Vec::new();
        let mut id = 1_u64;
        let image_len = loop {
            let begin = index_begin(id);
            parsed.push(ParsedRecord::Begin(begin.clone()));
            if id > 1 {
                parsed.push(ParsedRecord::Outcome(
                    begin.id,
                    ServerPhysicalDesignMutationReceiptOutcome::Rejected,
                ));
            }
            let mut length = V3_HEADER_BYTES;
            for record in &parsed {
                length += encode_parsed_v3(record).unwrap().len();
            }
            if length >= MIN_FILE_BYTES as usize {
                break length;
            }
            if matches!(parsed.last(), Some(ParsedRecord::Outcome(_, _))) {
                id += 1;
            } else {
                parsed.push(ParsedRecord::Outcome(
                    begin.id,
                    ServerPhysicalDesignMutationReceiptOutcome::Rejected,
                ));
                id += 1;
            }
        };
        if matches!(parsed.last(), Some(ParsedRecord::Outcome(_, _))) {
            id += 1;
            parsed.push(ParsedRecord::Begin(index_begin(id)));
        }
        let mut legacy = encode_v2_header(*identity.as_bytes(), incarnation).to_vec();
        for record in &parsed {
            let bytes = match record {
                ParsedRecord::Begin(begin) => encode_legacy_begin(begin).unwrap(),
                ParsedRecord::Outcome(id, outcome) => encode_legacy_outcome(*id, *outcome).unwrap(),
            };
            legacy.extend_from_slice(&bytes);
        }
        let path = root.join("receipts.nbmr");
        fs::write(&path, &legacy).unwrap();
        let mut v3_image = encode_v3_header(*identity.as_bytes(), incarnation).to_vec();
        for record in &parsed {
            v3_image.extend_from_slice(&encode_parsed_v3(record).unwrap());
        }
        let one_short = (v3_image.len() + MAX_RECOVERED_OUTCOME_BYTES - 1) as u64;
        assert!(one_short >= MIN_FILE_BYTES);
        let tight = ServerPhysicalDesignMutationReceiptConfig::new(&path, one_short).unwrap();
        assert!(matches!(
            ServerPhysicalDesignMutationReceiptJournal::open(tight, identity, &database),
            Err(ServerPhysicalDesignMutationReceiptJournalError::MigrationCapacityExceeded { .. })
        ));
        assert_eq!(fs::read(&path).unwrap(), legacy);

        let exact = ServerPhysicalDesignMutationReceiptConfig::new(
            &path,
            (v3_image.len() + MAX_RECOVERED_OUTCOME_BYTES) as u64,
        )
        .unwrap();
        let journal =
            ServerPhysicalDesignMutationReceiptJournal::open(exact, identity, &database).unwrap();
        assert_eq!(journal.status().journal_incarnation, incarnation);
        assert!(!journal.status().recovery_required);
        drop(journal);
        database.close().unwrap();
        fs::remove_dir_all(root).unwrap();
        let _ = image_len;
    }

    #[test]
    fn ambiguous_partial_v1_tail_fails_closed_without_losing_pending_begin() {
        let root = fixture_root("v1-unresolved");
        let database = create_database(&root);
        let identity = database.physical_design_database_identity().unwrap();
        let config =
            ServerPhysicalDesignMutationReceiptConfig::new(root.join("receipts.nbmr"), 1_000_000)
                .unwrap();
        let begin = index_begin(1);
        let mut legacy = v1_bytes_with(*identity.as_bytes(), &[encode_begin(&begin).unwrap()]);
        let partial = encode_legacy_outcome(
            begin.id,
            ServerPhysicalDesignMutationReceiptOutcome::Rejected,
        )
        .unwrap();
        legacy.extend_from_slice(&partial[..partial.len() - 1]);
        fs::write(config.path(), &legacy).unwrap();
        queue_journal_incarnations([[6; 16]]);

        assert!(matches!(
            ServerPhysicalDesignMutationReceiptJournal::open(config.clone(), identity, &database),
            Err(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
                "ambiguous legacy partial tail"
            ))
        ));
        assert_eq!(fs::read(config.path()).unwrap(), legacy);
        database.close().unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn legacy_length_corruption_never_silently_omits_begin_or_outcome() {
        let root = fixture_root("legacy-length-corruption");
        let database = create_database(&root);
        let identity = database.physical_design_database_identity().unwrap();
        let path = root.join("receipts.nbmr");
        let config = ServerPhysicalDesignMutationReceiptConfig::new(&path, 1_000_000).unwrap();
        let begin = index_begin(1);
        let legacy_begin = encode_legacy_begin(&begin).unwrap();
        let legacy_outcome = encode_legacy_outcome(
            begin.id,
            ServerPhysicalDesignMutationReceiptOutcome::Rejected,
        )
        .unwrap();
        let begin_offset = V2_HEADER_BYTES;
        let outcome_offset = begin_offset + legacy_begin.len();
        let mut base = encode_v2_header(
            *identity.as_bytes(),
            ServerPhysicalDesignMutationReceiptJournalIncarnation([17; 16]),
        )
        .to_vec();
        base.extend_from_slice(&legacy_begin);
        base.extend_from_slice(&legacy_outcome);
        let mut cases = Vec::new();
        for delta in [1_u32, 8] {
            let mut bytes = base.clone();
            let length = read_u32(&bytes[begin_offset..begin_offset + 4]) + delta;
            bytes[begin_offset..begin_offset + 4].copy_from_slice(&length.to_le_bytes());
            cases.push(bytes);
        }
        let mut one_bit = base.clone();
        one_bit[begin_offset] ^= 1;
        cases.push(one_bit);
        let mut outcome_length = base;
        outcome_length[outcome_offset] ^= 1;
        cases.push(outcome_length);
        for bytes in cases {
            fs::write(&path, &bytes).unwrap();
            assert!(
                ServerPhysicalDesignMutationReceiptJournal::open(
                    config.clone(),
                    identity,
                    &database
                )
                .is_err()
            );
            assert_eq!(fs::read(&path).unwrap(), bytes);
        }
        database.close().unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn v1_complete_corruption_and_migration_capacity_fail_without_replacement() {
        let root = fixture_root("v1-fail-closed");
        let database = create_database(&root);
        let identity = database.physical_design_database_identity().unwrap();
        let path = root.join("receipts.nbmr");
        let mut bad_record = encode_legacy_begin(&index_begin(1)).unwrap();
        let last = bad_record.len() - 1;
        bad_record[last] ^= 1;
        let mut corrupt = encode_v1_header(*identity.as_bytes()).to_vec();
        corrupt.extend_from_slice(&bad_record);
        fs::write(&path, &corrupt).unwrap();
        let config = ServerPhysicalDesignMutationReceiptConfig::new(&path, 1_000_000).unwrap();
        assert!(
            ServerPhysicalDesignMutationReceiptJournal::open(config, identity, &database).is_err()
        );
        assert_eq!(fs::read(&path).unwrap(), corrupt);

        let mut records = Vec::new();
        for id in 1..=300 {
            let begin = index_begin(id);
            records.push(encode_begin(&begin).unwrap());
            records.push(
                encode_outcome(
                    begin.id,
                    ServerPhysicalDesignMutationReceiptOutcome::Rejected,
                )
                .unwrap(),
            );
        }
        let legacy = v1_bytes_with(*identity.as_bytes(), &records);
        assert!(legacy.len() as u64 >= MIN_FILE_BYTES);
        fs::write(&path, &legacy).unwrap();
        let tight =
            ServerPhysicalDesignMutationReceiptConfig::new(&path, legacy.len() as u64).unwrap();
        assert!(matches!(
            ServerPhysicalDesignMutationReceiptJournal::open(tight, identity, &database),
            Err(ServerPhysicalDesignMutationReceiptJournalError::MigrationCapacityExceeded { .. })
        ));
        assert_eq!(fs::read(&path).unwrap(), legacy);
        let decoded = decode_legacy_records(&legacy, V1_HEADER_BYTES).unwrap();
        let mut migrated = encode_v3_header(
            *identity.as_bytes(),
            ServerPhysicalDesignMutationReceiptJournalIncarnation([7; 16]),
        )
        .to_vec();
        for record in &decoded.records {
            migrated.extend_from_slice(&encode_parsed_v3(record).unwrap());
        }
        let sufficient =
            ServerPhysicalDesignMutationReceiptConfig::new(&path, migrated.len() as u64).unwrap();
        queue_journal_incarnations([[7; 16]]);
        drop(
            ServerPhysicalDesignMutationReceiptJournal::open(sufficient, identity, &database)
                .unwrap(),
        );
        assert_eq!(journal_version(&fs::read(&path).unwrap()).unwrap(), 3);
        database.close().unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn migration_shadow_failures_preserve_one_authoritative_history() {
        let root = fixture_root("v1-shadow-failures");
        let database = create_database(&root);
        let identity = database.physical_design_database_identity().unwrap();
        let path = root.join("receipts.nbmr");
        let begin = index_begin(1);
        let legacy = v1_bytes_with(
            *identity.as_bytes(),
            &[
                encode_begin(&begin).unwrap(),
                encode_outcome(
                    begin.id,
                    ServerPhysicalDesignMutationReceiptOutcome::Rejected,
                )
                .unwrap(),
            ],
        );
        fs::write(&path, &legacy).unwrap();
        let config = ServerPhysicalDesignMutationReceiptConfig::new(&path, 1_000_000).unwrap();
        queue_journal_incarnations([[8; 16]]);
        fail_migration_at(TestMigrationFailure::AfterTemporarySync);
        assert!(
            ServerPhysicalDesignMutationReceiptJournal::open(config.clone(), identity, &database)
                .is_err()
        );
        assert_eq!(fs::read(&path).unwrap(), legacy);

        queue_journal_incarnations([[9; 16]]);
        let migrated =
            ServerPhysicalDesignMutationReceiptJournal::open(config.clone(), identity, &database)
                .unwrap();
        assert_eq!(migrated.status().journal_incarnation.as_bytes(), &[9; 16]);
        assert_eq!(migrated.page(None, 2).unwrap().receipts.len(), 1);
        drop(migrated);

        fs::write(&path, &legacy).unwrap();
        queue_journal_incarnations([[10; 16]]);
        fail_migration_at(TestMigrationFailure::AfterRename);
        assert!(
            ServerPhysicalDesignMutationReceiptJournal::open(config.clone(), identity, &database)
                .is_err()
        );
        assert_eq!(journal_version(&fs::read(&path).unwrap()).unwrap(), 3);
        let reopened =
            ServerPhysicalDesignMutationReceiptJournal::open(config, identity, &database).unwrap();
        assert_eq!(reopened.status().journal_incarnation.as_bytes(), &[10; 16]);
        assert_eq!(reopened.page(None, 2).unwrap().receipts.len(), 1);
        drop(reopened);
        database.close().unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn legacy_next_collision_is_ignored_and_unchanged() {
        let root = fixture_root("v1-shadow-conflict");
        let database = create_database(&root);
        let identity = database.physical_design_database_identity().unwrap();
        let path = root.join("receipts.nbmr");
        let legacy = v1_bytes_with(*identity.as_bytes(), &[]);
        fs::write(&path, &legacy).unwrap();
        let config = ServerPhysicalDesignMutationReceiptConfig::new(&path, 1_000_000).unwrap();
        let mut shadow_name = config.path().as_os_str().to_os_string();
        shadow_name.push(".next");
        let shadow = PathBuf::from(shadow_name);
        fs::create_dir(&shadow).unwrap();
        drop(
            ServerPhysicalDesignMutationReceiptJournal::open(config, identity, &database).unwrap(),
        );
        assert!(shadow.is_dir());
        assert_eq!(journal_version(&fs::read(&path).unwrap()).unwrap(), 3);
        fs::remove_dir(&shadow).unwrap();
        database.close().unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn every_legacy_next_collision_kind_is_ignored_without_overwrite() {
        use std::os::unix::fs::symlink;

        for kind in [
            "regular",
            "other-journal",
            "hardlink",
            "symlink",
            "dangling",
            "directory",
        ] {
            let root = fixture_root(kind);
            let database = create_database(&root);
            let identity = database.physical_design_database_identity().unwrap();
            let path = root.join("receipts.nbmr");
            let legacy = v1_bytes_with(*identity.as_bytes(), &[]);
            fs::write(&path, &legacy).unwrap();
            let next = root.join("receipts.nbmr.next");
            let unrelated = root.join("unrelated-target");
            let expected = match kind {
                "regular" => {
                    fs::write(&next, b"ordinary user bytes").unwrap();
                    Some(b"ordinary user bytes".to_vec())
                }
                "other-journal" => {
                    let bytes = encode_v2_header(
                        *identity.as_bytes(),
                        ServerPhysicalDesignMutationReceiptJournalIncarnation([22; 16]),
                    );
                    fs::write(&next, bytes).unwrap();
                    Some(bytes.to_vec())
                }
                "hardlink" => {
                    fs::write(&unrelated, b"hard-linked truth").unwrap();
                    fs::hard_link(&unrelated, &next).unwrap();
                    Some(b"hard-linked truth".to_vec())
                }
                "symlink" => {
                    fs::write(&unrelated, b"symlink truth").unwrap();
                    symlink(&unrelated, &next).unwrap();
                    None
                }
                "dangling" => {
                    symlink(root.join("missing"), &next).unwrap();
                    None
                }
                "directory" => {
                    fs::create_dir(&next).unwrap();
                    None
                }
                _ => unreachable!(),
            };
            let config = ServerPhysicalDesignMutationReceiptConfig::new(&path, 1_000_000).unwrap();
            drop(
                ServerPhysicalDesignMutationReceiptJournal::open(config, identity, &database)
                    .unwrap(),
            );
            assert_eq!(
                journal_version(&fs::read(&path).unwrap()).unwrap(),
                CURRENT_VERSION
            );
            match kind {
                "symlink" => assert_eq!(fs::read(&unrelated).unwrap(), b"symlink truth"),
                "dangling" => assert!(
                    fs::symlink_metadata(&next)
                        .unwrap()
                        .file_type()
                        .is_symlink()
                ),
                "directory" => assert!(next.is_dir()),
                _ => assert_eq!(fs::read(&next).unwrap(), expected.unwrap()),
            }
            if kind == "hardlink" {
                assert_eq!(fs::read(&unrelated).unwrap(), b"hard-linked truth");
            }
            database.close().unwrap();
            fs::remove_dir_all(root).unwrap();
        }
    }

    #[cfg(unix)]
    #[test]
    fn nofollow_open_rejects_deterministic_final_component_symlink_swap() {
        let root = fixture_root("nofollow-swap");
        let database = create_database(&root);
        let identity = database.physical_design_database_identity().unwrap();
        let path = root.join("receipts.nbmr");
        let config = ServerPhysicalDesignMutationReceiptConfig::new(&path, 1_000_000).unwrap();
        drop(
            ServerPhysicalDesignMutationReceiptJournal::open(config.clone(), identity, &database)
                .unwrap(),
        );
        let target = root.join("unrelated-target");
        fs::write(&target, b"must remain untouched").unwrap();
        TEST_EXISTING_OPEN_REPLACEMENT.with(|replacement| {
            *replacement.borrow_mut() = Some(TestExistingOpenReplacement {
                target: target.clone(),
            });
        });
        assert!(
            ServerPhysicalDesignMutationReceiptJournal::open(config, identity, &database).is_err()
        );
        assert_eq!(fs::read(&target).unwrap(), b"must remain untouched");
        assert!(
            fs::symlink_metadata(&path)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        database.close().unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn fresh_v3_publication_crash_points_leave_at_most_one_ready_history() {
        for point in [
            TestPublicationFailure::TemporaryCreated,
            TestPublicationFailure::PartialHeaderWritten,
            TestPublicationFailure::FullHeaderWritten,
            TestPublicationFailure::TemporarySynced,
            TestPublicationFailure::PublishedBeforeParentSync,
        ] {
            let root = fixture_root("fresh-crash");
            let database = create_database(&root);
            let identity = database.physical_design_database_identity().unwrap();
            let path = root.join("receipts.nbmr");
            let config = ServerPhysicalDesignMutationReceiptConfig::new(&path, 1_000_000).unwrap();
            fail_publication_at(point);
            assert!(
                ServerPhysicalDesignMutationReceiptJournal::open(
                    config.clone(),
                    identity,
                    &database
                )
                .is_err()
            );
            let final_was_published = point == TestPublicationFailure::PublishedBeforeParentSync;
            assert_eq!(path.exists(), final_was_published);
            let journal =
                ServerPhysicalDesignMutationReceiptJournal::open(config, identity, &database)
                    .unwrap();
            assert_eq!(
                journal_version(&fs::read(&path).unwrap()).unwrap(),
                CURRENT_VERSION
            );
            assert!(journal.page(None, 1).unwrap().receipts.is_empty());
            drop(journal);
            database.close().unwrap();
            fs::remove_dir_all(root).unwrap();
        }
    }

    impl ParsedRecord {
        fn unwrap_begin(self) -> MutationReceiptBegin {
            match self {
                Self::Begin(begin) => begin,
                Self::Outcome(_, _) => panic!("expected Begin"),
            }
        }
    }
}
