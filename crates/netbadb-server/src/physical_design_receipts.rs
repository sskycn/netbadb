//! Server-owned durable receipts for explicit Physical Design mutations.
//!
//! NBMR records observation around existing Core mutation authority. It is not
//! a database transaction participant and never creates physical state.

use std::error::Error;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

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
const HEADER_BYTES: usize = 28;
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
const MIN_FILE_BYTES: u64 = (HEADER_BYTES + MAX_COLUMNAR_BEGIN_BYTES + MAX_OUTCOME_BYTES) as u64;
const VERSION: u16 = 1;
const BEGIN_TAG: u8 = 1;
const OUTCOME_TAG: u8 = 2;

/// Programmatic-only configuration for one bounded NBMR v1 journal.
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ServerPhysicalDesignMutationReceiptId(pub u64);

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

#[derive(Debug)]
pub enum ServerPhysicalDesignMutationReceiptControlError {
    NotEnabled,
    InvalidLimit { supplied: u32, maximum: u32 },
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
    FileTooLarge {
        bytes: u64,
        maximum: u64,
    },
    CapacityExceeded,
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
            Self::FileTooLarge { bytes, maximum } => write!(
                formatter,
                "NBMR journal is {bytes} bytes, exceeding configured maximum {maximum}"
            ),
            Self::CapacityExceeded => formatter.write_str("NBMR journal capacity is exhausted"),
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
        let created = match fs::symlink_metadata(config.path()) {
            Ok(_) => false,
            Err(error) if error.kind() == io::ErrorKind::NotFound => true,
            Err(source) => {
                return Err(ServerPhysicalDesignMutationReceiptJournalError::Path(
                    ServerPhysicalDesignMutationReceiptPathError::Metadata {
                        path: config.path().to_path_buf(),
                        source,
                    },
                ));
            }
        };
        let mut options = OpenOptions::new();
        options.read(true).write(true);
        if created {
            options.create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
        }
        let mut file = options
            .open(config.path())
            .map_err(|source| io_error("open", config.path(), source))?;
        if created {
            let header = encode_header(*identity.as_bytes());
            file.write_all(&header)
                .map_err(|source| io_error("header write", config.path(), source))?;
            file.sync_all()
                .map_err(|source| io_error("header sync", config.path(), source))?;
            sync_parent(config.path())?;
        }
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
        file.read_to_end(&mut bytes)
            .map_err(|source| io_error("read", config.path(), source))?;
        validate_header(&bytes, *identity.as_bytes())?;
        let decoded = decode_records(&bytes)?;
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

fn encode_header(identity: [u8; 16]) -> [u8; HEADER_BYTES] {
    let mut bytes = [0_u8; HEADER_BYTES];
    bytes[..4].copy_from_slice(b"NBMR");
    bytes[4..6].copy_from_slice(&VERSION.to_le_bytes());
    bytes[8..24].copy_from_slice(&identity);
    let checksum = crc32c::crc32c(&bytes[..24]).to_le_bytes();
    bytes[24..28].copy_from_slice(&checksum);
    bytes
}

fn validate_header(
    bytes: &[u8],
    identity: [u8; 16],
) -> Result<(), ServerPhysicalDesignMutationReceiptJournalError> {
    if bytes.len() < HEADER_BYTES {
        return Err(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
            "truncated header",
        ));
    }
    if &bytes[..4] != b"NBMR" {
        return Err(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
            "bad header magic",
        ));
    }
    let version = u16::from_le_bytes([bytes[4], bytes[5]]);
    if version != VERSION {
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
) -> Result<DecodedRecords, ServerPhysicalDesignMutationReceiptJournalError> {
    let mut offset = HEADER_BYTES;
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
        let mut bytes = encode_header([7; 16]).to_vec();
        for record in records {
            bytes.extend_from_slice(record);
        }
        bytes
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
        let header = encode_header(identity);
        assert_eq!(
            header,
            [
                78, 66, 77, 82, 1, 0, 0, 0, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 99, 23,
                164, 16,
            ]
        );
        validate_header(&header, identity).unwrap();

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
        for length in 0..HEADER_BYTES {
            assert!(validate_header(&complete[..length], [7; 16]).is_err());
        }
        for length in HEADER_BYTES..complete.len() {
            let decoded = decode_records(&complete[..length]).unwrap();
            let expected = if length < HEADER_BYTES + begin.len() {
                HEADER_BYTES
            } else {
                HEADER_BYTES + begin.len()
            };
            assert_eq!(decoded.valid_bytes, expected);
        }
        assert_eq!(
            decode_records(&complete).unwrap().valid_bytes,
            complete.len()
        );
    }

    #[test]
    fn malformed_headers_fail_closed() {
        let mut bad_magic = encode_header([7; 16]);
        bad_magic[0] = b'X';
        assert!(matches!(
            validate_header(&bad_magic, [7; 16]),
            Err(ServerPhysicalDesignMutationReceiptJournalError::Corrupt(
                "bad header magic"
            ))
        ));

        let mut future = encode_header([7; 16]);
        future[4..6].copy_from_slice(&2_u16.to_le_bytes());
        let checksum = crc32c::crc32c(&future[..24]).to_le_bytes();
        future[24..].copy_from_slice(&checksum);
        assert!(matches!(
            validate_header(&future, [7; 16]),
            Err(ServerPhysicalDesignMutationReceiptJournalError::UnsupportedVersion(2))
        ));

        let mut reserved = encode_header([7; 16]);
        reserved[6] = 1;
        let checksum = crc32c::crc32c(&reserved[..24]).to_le_bytes();
        reserved[24..].copy_from_slice(&checksum);
        assert!(validate_header(&reserved, [7; 16]).is_err());

        let mut bad_crc = encode_header([7; 16]);
        bad_crc[24] ^= 1;
        assert!(validate_header(&bad_crc, [7; 16]).is_err());
        assert!(matches!(
            validate_header(&encode_header([8; 16]), [7; 16]),
            Err(ServerPhysicalDesignMutationReceiptJournalError::DatabaseIdentityMismatch)
        ));
    }

    #[test]
    fn malformed_complete_records_fail_closed() {
        let begin = encode_begin(&index_begin(1)).unwrap();

        let mut bad_crc = begin.clone();
        let last = bad_crc.len() - 1;
        bad_crc[last] ^= 1;
        assert!(decode_records(&bytes_with(&[bad_crc])).is_err());

        let unknown = encode_record(99, ServerPhysicalDesignMutationReceiptId(1), &[]).unwrap();
        assert!(decode_records(&bytes_with(&[unknown])).is_err());
        let unknown_outcome = encode_record(
            OUTCOME_TAG,
            ServerPhysicalDesignMutationReceiptId(1),
            &[99, 0, 0, 0],
        )
        .unwrap();
        assert!(decode_records(&bytes_with(&[begin.clone(), unknown_outcome])).is_err());

        let mut record_reserved = begin.clone();
        record_reserved[5] = 1;
        replace_record_crc(&mut record_reserved);
        assert!(decode_records(&bytes_with(&[record_reserved])).is_err());

        let mut body_reserved = begin.clone();
        body_reserved[18] = 1;
        replace_record_crc(&mut body_reserved);
        assert!(decode_records(&bytes_with(&[body_reserved])).is_err());

        let mut zero_id = begin.clone();
        zero_id[8..16].fill(0);
        replace_record_crc(&mut zero_id);
        assert!(decode_records(&bytes_with(&[zero_id])).is_err());

        let mut oversized = encode_header([7; 16]).to_vec();
        oversized
            .extend_from_slice(&((MAX_MUTATION_RECEIPT_RECORD_BYTES as u32) + 1).to_le_bytes());
        assert!(decode_records(&oversized).is_err());
    }

    #[test]
    fn invalid_record_ordering_fails_closed() {
        let first = encode_begin(&index_begin(1)).unwrap();
        let duplicate = encode_begin(&index_begin(1)).unwrap();
        assert!(decode_records(&bytes_with(&[first.clone(), duplicate])).is_err());

        let outcome = encode_outcome(
            ServerPhysicalDesignMutationReceiptId(1),
            ServerPhysicalDesignMutationReceiptOutcome::Rejected,
        )
        .unwrap();
        assert!(decode_records(&bytes_with(std::slice::from_ref(&outcome))).is_err());
        assert!(decode_records(&bytes_with(&[first.clone(), outcome.clone(), outcome])).is_err());

        let second = encode_begin(&index_begin(2)).unwrap();
        let second_outcome = encode_outcome(
            ServerPhysicalDesignMutationReceiptId(2),
            ServerPhysicalDesignMutationReceiptOutcome::Rejected,
        )
        .unwrap();
        let lower = encode_begin(&index_begin(1)).unwrap();
        assert!(decode_records(&bytes_with(&[second, second_outcome, lower])).is_err());
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

    impl ParsedRecord {
        fn unwrap_begin(self) -> MutationReceiptBegin {
            match self {
                Self::Begin(begin) => begin,
                Self::Outcome(_, _) => panic!("expected Begin"),
            }
        }
    }
}
