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
const RECORD_FIXED_BYTES: usize = 4 + 1 + 3 + 8 + 4;
const BEGIN_COMMON_BYTES: usize = 1 + 1 + 2 + 8;
const MAX_COLUMNAR_BEGIN_BYTES: usize = RECORD_FIXED_BYTES
    + BEGIN_COMMON_BYTES
    + 8
    + 1
    + 3
    + 4
    + MAX_COLUMN_IDS * 4
    + 2
    + MAX_PLACEMENT_BYTES
    + 2
    + MAX_PATH_BYTES;
const MAX_OUTCOME_BYTES: usize = RECORD_FIXED_BYTES + 1 + 3 + 8;
const MIN_FILE_BYTES: u64 = (V2_HEADER_BYTES + MAX_COLUMNAR_BEGIN_BYTES + MAX_OUTCOME_BYTES) as u64;
const V1_VERSION: u16 = 1;
const CURRENT_VERSION: u16 = 2;
const BEGIN_TAG: u8 = 1;
const OUTCOME_TAG: u8 = 2;

/// Programmatic-only configuration for one bounded NBMR v2 journal.
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

fn journal_shadow_path(path: &Path) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(".next");
    value.into()
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

fn create_v2_journal(
    config: &ServerPhysicalDesignMutationReceiptConfig,
    database_incarnation: [u8; 16],
) -> Result<(), ServerPhysicalDesignMutationReceiptJournalError> {
    let journal_incarnation = generate_journal_incarnation()?;
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(config.path())
        .map_err(|source| io_error("create", config.path(), source))?;
    let header = encode_v2_header(database_incarnation, journal_incarnation);
    file.write_all(&header)
        .map_err(|source| io_error("header write", config.path(), source))?;
    file.sync_all()
        .map_err(|source| io_error("header sync", config.path(), source))?;
    sync_parent(config.path())
}

fn read_journal_bytes(
    config: &ServerPhysicalDesignMutationReceiptConfig,
) -> Result<Vec<u8>, ServerPhysicalDesignMutationReceiptJournalError> {
    let mut file = OpenOptions::new()
        .read(true)
        .open(config.path())
        .map_err(|source| io_error("open", config.path(), source))?;
    read_open_file(&mut file, config)
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

fn migrate_v1(
    config: &ServerPhysicalDesignMutationReceiptConfig,
    database_incarnation: [u8; 16],
    v1_bytes: &[u8],
) -> Result<(), ServerPhysicalDesignMutationReceiptJournalError> {
    validate_v1_header(v1_bytes, database_incarnation)?;
    let decoded = decode_records(v1_bytes, V1_HEADER_BYTES)?;
    let record_bytes = &v1_bytes[V1_HEADER_BYTES..decoded.valid_bytes];
    let migrated_bytes = V2_HEADER_BYTES
        .checked_add(record_bytes.len())
        .ok_or(ServerPhysicalDesignMutationReceiptJournalError::CapacityExceeded)?;
    let migrated_bytes_u64 = migrated_bytes as u64;
    if migrated_bytes_u64 > config.max_file_bytes() {
        return Err(
            ServerPhysicalDesignMutationReceiptJournalError::MigrationCapacityExceeded {
                migrated_bytes: migrated_bytes_u64,
                maximum: config.max_file_bytes(),
            },
        );
    }
    let shadow = journal_shadow_path(config.path());
    validate_final_path(&shadow).map_err(ServerPhysicalDesignMutationReceiptJournalError::Path)?;
    let journal_incarnation = generate_journal_incarnation()?;
    let mut bytes = Vec::with_capacity(migrated_bytes);
    bytes.extend_from_slice(&encode_v2_header(database_incarnation, journal_incarnation));
    bytes.extend_from_slice(record_bytes);

    let mut options = OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(&shadow)
        .map_err(|source| io_error("create migration shadow", &shadow, source))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(fs::Permissions::from_mode(0o600))
            .map_err(|source| io_error("secure migration shadow", &shadow, source))?;
    }
    file.write_all(&bytes)
        .map_err(|source| io_error("write migration shadow", &shadow, source))?;
    file.sync_all()
        .map_err(|source| io_error("sync migration shadow", &shadow, source))?;
    drop(file);
    #[cfg(test)]
    inject_migration_failure(TestMigrationFailure::AfterShadowSync, &shadow)?;
    fs::rename(&shadow, config.path())
        .map_err(|source| io_error("publish v2 migration", config.path(), source))?;
    #[cfg(test)]
    inject_migration_failure(TestMigrationFailure::AfterRename, config.path())?;
    sync_parent(config.path())
}

#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum TestMigrationFailure {
    AfterShadowSync,
    AfterRename,
}

#[cfg(test)]
thread_local! {
    static TEST_JOURNAL_INCARNATIONS: RefCell<VecDeque<[u8; 16]>> = const { RefCell::new(VecDeque::new()) };
    static TEST_MIGRATION_FAILURE: RefCell<Option<TestMigrationFailure>> = const { RefCell::new(None) };
    static TEST_RANDOMNESS_FAILURE: Cell<bool> = const { Cell::new(false) };
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
/// The value is generated from OS randomness when a v2 journal is first
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
    DatabaseIdentity(DatabaseError),
    Journal(ServerPhysicalDesignMutationReceiptJournalError),
}

impl fmt::Display for ServerPhysicalDesignMutationReceiptStartupError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PhysicalDesignRequired => formatter.write_str(
                "physical-design mutation receipts require a configured physical-design advisor",
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
            Self::PhysicalDesignRequired => None,
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
                "migrated NBMR v2 journal would be {migrated_bytes} bytes, exceeding configured maximum {maximum}"
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
    fail_next_begin_append: bool,
    #[cfg(test)]
    fail_next_outcome_append: bool,
}

impl ServerPhysicalDesignMutationReceiptJournal {
    pub(crate) fn open(
        config: ServerPhysicalDesignMutationReceiptConfig,
        identity: PhysicalDesignDatabaseIdentity,
        database: &Database,
    ) -> Result<Self, ServerPhysicalDesignMutationReceiptJournalError> {
        validate_final_path(config.path())
            .map_err(ServerPhysicalDesignMutationReceiptJournalError::Path)?;
        let exists = match fs::symlink_metadata(config.path()) {
            Ok(_) => true,
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
        if !exists {
            create_v2_journal(&config, *identity.as_bytes())?;
        } else {
            let bytes = read_journal_bytes(&config)?;
            match journal_version(&bytes)? {
                V1_VERSION => migrate_v1(&config, *identity.as_bytes(), &bytes)?,
                CURRENT_VERSION => {}
                version => {
                    return Err(
                        ServerPhysicalDesignMutationReceiptJournalError::UnsupportedVersion(
                            version,
                        ),
                    );
                }
            }
        }

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(config.path())
            .map_err(|source| io_error("open", config.path(), source))?;
        let bytes = read_open_file(&mut file, &config)?;
        let journal_incarnation = validate_v2_header(&bytes, *identity.as_bytes())?;
        let decoded = decode_records(&bytes, V2_HEADER_BYTES)?;
        if decoded.valid_bytes < bytes.len() {
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
            fail_next_begin_append: false,
            #[cfg(test)]
            fail_next_outcome_append: false,
        };
        journal.reconcile(database)?;
        Ok(journal)
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
            .and_then(|length| length.checked_add(MAX_OUTCOME_BYTES as u64));
        if required.is_none_or(|length| length > self.config.max_file_bytes()) {
            return Err(ServerPhysicalDesignMutationReceiptControlError::Journal(
                ServerPhysicalDesignMutationReceiptJournalError::CapacityExceeded,
            ));
        }
        #[cfg(test)]
        if std::mem::take(&mut self.fail_next_begin_append) {
            self.recovery_required = true;
            return Err(ServerPhysicalDesignMutationReceiptControlError::Journal(
                io_error(
                    "Begin append",
                    self.config.path(),
                    io::Error::other("injected Begin append failure"),
                ),
            ));
        }
        if let Err(error) = self.append_synced(&bytes, "Begin append") {
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
        if std::mem::take(&mut self.fail_next_outcome_append) {
            self.recovery_required = true;
            return Err(ServerPhysicalDesignMutationReceiptControlError::Journal(
                io_error(
                    "Outcome append",
                    self.config.path(),
                    io::Error::other("injected Outcome append failure"),
                ),
            ));
        }
        if let Err(error) = self.append_synced(&bytes, "Outcome append") {
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
    pub(crate) fn fail_next_begin_append(&mut self) {
        self.fail_next_begin_append = true;
    }

    #[cfg(test)]
    pub(crate) fn fail_next_outcome_append(&mut self) {
        self.fail_next_outcome_append = true;
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
    ) -> Result<(), ServerPhysicalDesignMutationReceiptJournalError> {
        self.file
            .write_all(bytes)
            .map_err(|source| io_error(operation, self.config.path(), source))?;
        self.file
            .sync_all()
            .map_err(|source| io_error("record sync", self.config.path(), source))?;
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
    bytes[4..6].copy_from_slice(&CURRENT_VERSION.to_le_bytes());
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
    if version != CURRENT_VERSION {
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
}

enum ParsedRecord {
    Begin(MutationReceiptBegin),
    Outcome(
        ServerPhysicalDesignMutationReceiptId,
        ServerPhysicalDesignMutationReceiptOutcome,
    ),
}

fn decode_records(
    bytes: &[u8],
    header_bytes: usize,
) -> Result<DecodedRecords, ServerPhysicalDesignMutationReceiptJournalError> {
    let mut offset = header_bytes;
    let mut highest = 0_u64;
    let mut receipts = Vec::new();
    let mut unresolved: Option<MutationReceiptBegin> = None;
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
        if total > MAX_MUTATION_RECEIPT_RECORD_BYTES || payload_len < 12 {
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
        match decode_record(record)? {
            ParsedRecord::Begin(begin) => {
                if unresolved.is_some() {
                    return Err(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
                        "multiple unresolved Begin records",
                    ));
                }
                if begin.id.0 <= highest {
                    return Err(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
                        "non-monotonic Begin receipt ID",
                    ));
                }
                highest = begin.id.0;
                receipts.push(public_receipt(&begin));
                unresolved = Some(begin);
            }
            ParsedRecord::Outcome(id, outcome) => {
                let Some(begin) = unresolved.take() else {
                    return Err(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
                        "Outcome without Begin",
                    ));
                };
                if begin.id != id {
                    return Err(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
                        "Outcome does not reference current Begin",
                    ));
                }
                receipts
                    .last_mut()
                    .ok_or(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
                        "Outcome receipt missing",
                    ))?
                    .outcome = outcome;
            }
        }
        offset += total;
    }
    Ok(DecodedRecords {
        valid_bytes: offset,
        highest_begin: highest,
        receipts,
        unresolved,
    })
}

fn decode_record(
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
    let mut reader = Reader(&payload[12..]);
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
        let mut bytes = encode_v2_header(
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
            bytes.extend_from_slice(record);
        }
        bytes
    }

    fn queue_journal_incarnations(values: impl IntoIterator<Item = [u8; 16]>) {
        TEST_JOURNAL_INCARNATIONS.with(|queued| queued.borrow_mut().extend(values));
    }

    fn fail_migration_at(point: TestMigrationFailure) {
        TEST_MIGRATION_FAILURE.with(|failure| *failure.borrow_mut() = Some(point));
    }

    fn replace_record_crc(record: &mut [u8]) {
        let crc_offset = record.len() - 4;
        let checksum = crc32c::crc32c(&record[..crc_offset]).to_le_bytes();
        record[crc_offset..].copy_from_slice(&checksum);
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
        for length in 0..V2_HEADER_BYTES {
            assert!(validate_v2_header(&complete[..length], [7; 16]).is_err());
        }
        for length in V2_HEADER_BYTES..complete.len() {
            let decoded = decode_records(&complete[..length], V2_HEADER_BYTES).unwrap();
            let expected = if length < V2_HEADER_BYTES + begin.len() {
                V2_HEADER_BYTES
            } else {
                V2_HEADER_BYTES + begin.len()
            };
            assert_eq!(decoded.valid_bytes, expected);
        }
        assert_eq!(
            decode_records(&complete, V2_HEADER_BYTES)
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
        record_reserved[5] = 1;
        replace_record_crc(&mut record_reserved);
        assert!(decode_records(&bytes_with(&[record_reserved]), V2_HEADER_BYTES).is_err());

        let mut body_reserved = begin.clone();
        body_reserved[18] = 1;
        replace_record_crc(&mut body_reserved);
        assert!(decode_records(&bytes_with(&[body_reserved]), V2_HEADER_BYTES).is_err());

        let mut zero_id = begin.clone();
        zero_id[8..16].fill(0);
        replace_record_crc(&mut zero_id);
        assert!(decode_records(&bytes_with(&[zero_id]), V2_HEADER_BYTES).is_err());

        let mut oversized = encode_v2_header(
            [7; 16],
            ServerPhysicalDesignMutationReceiptJournalIncarnation([9; 16]),
        )
        .to_vec();
        oversized
            .extend_from_slice(&((MAX_MUTATION_RECEIPT_RECORD_BYTES as u32) + 1).to_le_bytes());
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
    fn fresh_v2_journals_have_stable_distinct_nonzero_namespaces() {
        let root = fixture_root("v2-identities");
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
            2
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
            2
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
    fn unresolved_and_partial_v1_history_migrates_before_reconciliation() {
        let root = fixture_root("v1-unresolved");
        let database = create_database(&root);
        let identity = database.physical_design_database_identity().unwrap();
        let config =
            ServerPhysicalDesignMutationReceiptConfig::new(root.join("receipts.nbmr"), 1_000_000)
                .unwrap();
        let begin = index_begin(1);
        let mut legacy = v1_bytes_with(*identity.as_bytes(), &[encode_begin(&begin).unwrap()]);
        let partial = encode_outcome(
            begin.id,
            ServerPhysicalDesignMutationReceiptOutcome::Rejected,
        )
        .unwrap();
        legacy.extend_from_slice(&partial[..partial.len() - 1]);
        fs::write(config.path(), legacy).unwrap();
        queue_journal_incarnations([[6; 16]]);

        let journal =
            ServerPhysicalDesignMutationReceiptJournal::open(config.clone(), identity, &database)
                .unwrap();
        let scoped = journal.scoped_page(None, 1).unwrap();
        assert_eq!(scoped.journal_incarnation.as_bytes(), &[6; 16]);
        assert_eq!(
            scoped.receipts[0].outcome,
            ServerPhysicalDesignMutationReceiptOutcome::RecoveredNotApplied
        );
        let bytes = fs::read(config.path()).unwrap();
        assert_eq!(journal_version(&bytes).unwrap(), 2);
        assert_eq!(
            decode_records(&bytes, V2_HEADER_BYTES).unwrap().valid_bytes,
            bytes.len()
        );
        drop(journal);
        database.close().unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn v1_complete_corruption_and_migration_capacity_fail_without_replacement() {
        let root = fixture_root("v1-fail-closed");
        let database = create_database(&root);
        let identity = database.physical_design_database_identity().unwrap();
        let path = root.join("receipts.nbmr");
        let mut bad_record = encode_begin(&index_begin(1)).unwrap();
        let last = bad_record.len() - 1;
        bad_record[last] ^= 1;
        let corrupt = v1_bytes_with(*identity.as_bytes(), &[bad_record]);
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
        let sufficient = ServerPhysicalDesignMutationReceiptConfig::new(
            &path,
            legacy.len() as u64 + (V2_HEADER_BYTES - V1_HEADER_BYTES) as u64,
        )
        .unwrap();
        queue_journal_incarnations([[7; 16]]);
        drop(
            ServerPhysicalDesignMutationReceiptJournal::open(sufficient, identity, &database)
                .unwrap(),
        );
        assert_eq!(journal_version(&fs::read(&path).unwrap()).unwrap(), 2);
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
        fail_migration_at(TestMigrationFailure::AfterShadowSync);
        assert!(
            ServerPhysicalDesignMutationReceiptJournal::open(config.clone(), identity, &database)
                .is_err()
        );
        assert_eq!(fs::read(&path).unwrap(), legacy);
        assert!(journal_shadow_path(&path).is_file());

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
        assert_eq!(journal_version(&fs::read(&path).unwrap()).unwrap(), 2);
        let reopened =
            ServerPhysicalDesignMutationReceiptJournal::open(config, identity, &database).unwrap();
        assert_eq!(reopened.status().journal_incarnation.as_bytes(), &[10; 16]);
        assert_eq!(reopened.page(None, 2).unwrap().receipts.len(), 1);
        drop(reopened);
        database.close().unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn non_regular_reserved_migration_shadow_fails_closed() {
        let root = fixture_root("v1-shadow-conflict");
        let database = create_database(&root);
        let identity = database.physical_design_database_identity().unwrap();
        let path = root.join("receipts.nbmr");
        let legacy = v1_bytes_with(*identity.as_bytes(), &[]);
        fs::write(&path, &legacy).unwrap();
        let config = ServerPhysicalDesignMutationReceiptConfig::new(&path, 1_000_000).unwrap();
        let shadow = journal_shadow_path(config.path());
        fs::create_dir(&shadow).unwrap();
        assert!(matches!(
            ServerPhysicalDesignMutationReceiptJournal::open(config, identity, &database),
            Err(ServerPhysicalDesignMutationReceiptJournalError::Path(
                ServerPhysicalDesignMutationReceiptPathError::ExistingObjectNotRegular(path)
            )) if path == shadow
        ));
        assert_eq!(fs::read(&path).unwrap(), legacy);
        fs::remove_dir(&shadow).unwrap();
        database.close().unwrap();
        fs::remove_dir_all(root).unwrap();
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
