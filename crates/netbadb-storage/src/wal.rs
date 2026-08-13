use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use netbadb_types::{Lsn, PageId, TxnId};

use crate::{PAGE_SIZE, Page};

const WAL_MAGIC: &[u8; 4] = b"NBWL";
const RECORD_MAGIC: &[u8; 4] = b"WREC";
const RECORD_FORMAT_VERSION: u16 = 1;
const RECORD_HEADER_SIZE: usize = 40;
const PAGE_UPDATE_PAYLOAD_SIZE: usize = 8 + PAGE_SIZE * 2;

pub const WAL_FORMAT_VERSION: u16 = 2;
pub const WAL_HEADER_SIZE: usize = 48;
pub const WAL_MAX_RECORD_SIZE: usize = RECORD_HEADER_SIZE + PAGE_UPDATE_PAYLOAD_SIZE;

const INITIAL_GENERATION: u64 = 1;
const INITIAL_BASE_LSN: Lsn = Lsn(1);
const INITIAL_NEXT_TXN_ID: TxnId = TxnId(1);

#[derive(Debug)]
pub enum WalError {
    Io(std::io::Error),
    InvalidMagic,
    UnsupportedVersion(u16),
    InvalidHeaderSize(u16),
    InvalidReservedBytes,
    InvalidGeneration(u64),
    InvalidBaseLsn {
        base_lsn: Lsn,
        checkpoint_lsn: Option<Lsn>,
    },
    InvalidNextTxnId(u64),
    GenerationConflict,
    TruncatedHeader,
    TruncatedRecord {
        lsn: Lsn,
    },
    InvalidRecordMagic {
        lsn: Lsn,
    },
    UnsupportedRecordVersion {
        lsn: Lsn,
        version: u16,
    },
    UnknownRecordType {
        lsn: Lsn,
        tag: u8,
    },
    InvalidRecordLength {
        lsn: Lsn,
        length: u32,
    },
    RecordTooLarge {
        lsn: Lsn,
        length: u32,
    },
    InvalidPayloadLength {
        lsn: Lsn,
        expected: u32,
        actual: u32,
    },
    InvalidRecordedLsn {
        expected: Lsn,
        actual: Lsn,
    },
    InvalidPrevLsn {
        lsn: Lsn,
        expected: Option<Lsn>,
        actual: Option<Lsn>,
    },
    InvalidPartialPrevLsn {
        lsn: Lsn,
    },
    InvalidTransactionSequence {
        lsn: Lsn,
        txn_id: TxnId,
        record_type: u8,
    },
    InvalidPageImage {
        lsn: Lsn,
        image: &'static str,
    },
    InvalidPageLsn {
        record_lsn: Lsn,
        page_lsn: Option<Lsn>,
    },
    LsnOverflow,
    FlushBeyondWritten {
        requested: Lsn,
        written: Option<Lsn>,
    },
    Poisoned,
}

impl fmt::Display for WalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "WAL I/O error: {error}"),
            Self::InvalidMagic => formatter.write_str("WAL magic does not match"),
            Self::UnsupportedVersion(version) => {
                write!(formatter, "unsupported WAL format version {version}")
            }
            Self::InvalidHeaderSize(size) => write!(formatter, "invalid WAL header size {size}"),
            Self::InvalidReservedBytes => formatter.write_str("WAL reserved bytes are non-zero"),
            Self::InvalidGeneration(generation) => {
                write!(formatter, "invalid WAL generation {generation}")
            }
            Self::InvalidBaseLsn {
                base_lsn,
                checkpoint_lsn,
            } => write!(
                formatter,
                "WAL base LSN {} does not follow checkpoint LSN {checkpoint_lsn:?}",
                base_lsn.0
            ),
            Self::InvalidNextTxnId(txn_id) => {
                write!(
                    formatter,
                    "invalid next transaction ID {txn_id} in WAL header"
                )
            }
            Self::GenerationConflict => {
                formatter.write_str("WAL generation slots have inconsistent generation metadata")
            }
            Self::TruncatedHeader => formatter.write_str("WAL header is truncated"),
            Self::TruncatedRecord { lsn } => {
                write!(formatter, "WAL record at {} is truncated", lsn.0)
            }
            Self::InvalidRecordMagic { lsn } => {
                write!(formatter, "WAL record at {} has invalid magic", lsn.0)
            }
            Self::UnsupportedRecordVersion { lsn, version } => write!(
                formatter,
                "WAL record at {} has unsupported version {version}",
                lsn.0
            ),
            Self::UnknownRecordType { lsn, tag } => {
                write!(formatter, "WAL record at {} has unknown type {tag}", lsn.0)
            }
            Self::InvalidRecordLength { lsn, length } => write!(
                formatter,
                "WAL record at {} has invalid length {length}",
                lsn.0
            ),
            Self::RecordTooLarge { lsn, length } => write!(
                formatter,
                "WAL record at {} exceeds the maximum length: {length}",
                lsn.0
            ),
            Self::InvalidPayloadLength {
                lsn,
                expected,
                actual,
            } => write!(
                formatter,
                "WAL record at {} declares payload length {actual}, expected {expected}",
                lsn.0
            ),
            Self::InvalidRecordedLsn { expected, actual } => write!(
                formatter,
                "WAL record at {} stores mismatched LSN {}",
                expected.0, actual.0
            ),
            Self::InvalidPrevLsn {
                lsn,
                expected,
                actual,
            } => write!(
                formatter,
                "WAL record at {} has prevLSN {actual:?}, expected {expected:?}",
                lsn.0
            ),
            Self::InvalidPartialPrevLsn { lsn } => write!(
                formatter,
                "partial WAL record at {} has a mismatched prevLSN prefix",
                lsn.0
            ),
            Self::InvalidTransactionSequence {
                lsn,
                txn_id,
                record_type,
            } => write!(
                formatter,
                "WAL record type {record_type} at {} is invalid for transaction {}",
                lsn.0, txn_id.0
            ),
            Self::InvalidPageImage { lsn, image } => write!(
                formatter,
                "WAL record at {} has an invalid {image} page image",
                lsn.0
            ),
            Self::InvalidPageLsn {
                record_lsn,
                page_lsn,
            } => write!(
                formatter,
                "WAL record at {} has after-image pageLSN {page_lsn:?}",
                record_lsn.0
            ),
            Self::LsnOverflow => formatter.write_str("WAL LSN space is exhausted"),
            Self::FlushBeyondWritten { requested, written } => write!(
                formatter,
                "cannot flush through LSN {} when last written LSN is {written:?}",
                requested.0
            ),
            Self::Poisoned => formatter.write_str(
                "WAL manager is poisoned after an append failure that could not be rolled back",
            ),
        }
    }
}

impl Error for WalError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<std::io::Error> for WalError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalRecordKind {
    Begin,
    PageUpdate {
        page_id: PageId,
        before: Box<[u8; PAGE_SIZE]>,
        after: Box<[u8; PAGE_SIZE]>,
    },
    Commit,
    Abort,
    RollbackComplete,
}

impl WalRecordKind {
    const fn tag(&self) -> u8 {
        match self {
            Self::Begin => 1,
            Self::PageUpdate { .. } => 2,
            Self::Commit => 3,
            Self::Abort => 4,
            Self::RollbackComplete => 5,
        }
    }

    const fn payload_len(&self) -> usize {
        match self {
            Self::PageUpdate { .. } => PAGE_UPDATE_PAYLOAD_SIZE,
            Self::Begin | Self::Commit | Self::Abort | Self::RollbackComplete => 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalRecord {
    pub lsn: Lsn,
    pub txn_id: TxnId,
    pub prev_lsn: Option<Lsn>,
    pub kind: WalRecordKind,
}

#[derive(Debug)]
pub struct WalManager {
    file: File,
    root_path: PathBuf,
    path: PathBuf,
    generation: u64,
    base_lsn: Lsn,
    checkpoint_lsn: Option<Lsn>,
    next_txn_id: TxnId,
    next_offset: u64,
    next_lsn: Lsn,
    written_lsn: Option<Lsn>,
    durable_lsn: Option<Lsn>,
    last_by_txn: HashMap<TxnId, Lsn>,
    txn_states: HashMap<TxnId, WalTxnState>,
    poisoned: bool,
    #[cfg(test)]
    fail_next_flush: bool,
    #[cfg(test)]
    fail_next_append_after: Option<usize>,
    #[cfg(test)]
    fail_next_rotation_after: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WalHeader {
    generation: u64,
    base_lsn: Lsn,
    checkpoint_lsn: Option<Lsn>,
    next_txn_id: TxnId,
}

impl WalManager {
    pub fn create(path: impl AsRef<Path>) -> Result<Self, WalError> {
        let root_path = path.as_ref().to_owned();
        let alternate_path = wal_alternate_path(&root_path);
        if alternate_path.try_exists()? {
            return Err(WalError::GenerationConflict);
        }
        let header = WalHeader {
            generation: INITIAL_GENERATION,
            base_lsn: INITIAL_BASE_LSN,
            checkpoint_lsn: None,
            next_txn_id: INITIAL_NEXT_TXN_ID,
        };
        let file = match create_generation_file(&root_path, header, None) {
            Ok(file) => file,
            Err(failure) => {
                if failure.file_created {
                    let _ = std::fs::remove_file(&root_path);
                }
                return Err(failure.error);
            }
        };
        if let Err(error) = sync_parent_directory(&root_path) {
            drop(file);
            let _ = std::fs::remove_file(&root_path);
            return Err(error);
        }
        Self::from_scan(
            file,
            root_path.clone(),
            root_path,
            header,
            &[],
            WAL_HEADER_SIZE as u64,
        )
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self, WalError> {
        let (manager, _, _) = Self::open_selected(path.as_ref(), TailPolicy::Reject)?;
        Ok(manager)
    }

    pub(crate) fn open_for_recovery(
        path: impl AsRef<Path>,
    ) -> Result<(Self, Vec<WalRecord>, bool), WalError> {
        Self::open_selected(path.as_ref(), TailPolicy::AllowIncompleteFinalRecord)
    }

    fn open_selected(
        root_path: &Path,
        tail_policy: TailPolicy,
    ) -> Result<(Self, Vec<WalRecord>, bool), WalError> {
        let root_path = root_path.to_owned();
        let alternate_path = wal_alternate_path(&root_path);
        let mut candidates = Vec::new();
        let mut failures = Vec::new();
        for path in [&root_path, &alternate_path] {
            if !path.try_exists()? {
                continue;
            }
            let length = std::fs::metadata(path)?.len();
            if length < WAL_HEADER_SIZE as u64 {
                failures.push((path.to_owned(), None, WalError::TruncatedHeader));
                continue;
            }
            let mut file = OpenOptions::new().read(true).write(true).open(path)?;
            let header = match read_header(&mut file) {
                Ok(header) => header,
                Err(error) => {
                    failures.push((path.to_owned(), None, error));
                    continue;
                }
            };
            match scan_file(&mut file, tail_policy) {
                Ok(scan) => candidates.push((path.to_owned(), file, header, scan)),
                Err(error) => failures.push((path.to_owned(), Some(header.generation), error)),
            }
        }
        if candidates.is_empty() {
            return Err(failures.into_iter().next().map_or_else(
                || WalError::Io(std::io::Error::from(std::io::ErrorKind::NotFound)),
                |(_, _, error)| error,
            ));
        }
        candidates.sort_unstable_by_key(|(_, _, header, _)| header.generation);
        validate_generation_candidates(&candidates)?;
        let selected_generation = candidates
            .last()
            .map(|(_, _, header, _)| header.generation)
            .ok_or(WalError::GenerationConflict)?;
        let mut ignored_failure_paths = Vec::new();
        for (path, generation, error) in failures {
            let blocks_open = match generation {
                Some(generation) => generation >= selected_generation,
                None => !matches!(error, WalError::TruncatedHeader),
            };
            if blocks_open {
                return Err(error);
            }
            ignored_failure_paths.push(path);
        }
        let (path, file, header, scan) = candidates.pop().ok_or(WalError::GenerationConflict)?;
        let mut superseded_paths = candidates
            .iter()
            .map(|(path, _, _, _)| path.clone())
            .collect::<Vec<_>>();
        superseded_paths.extend(ignored_failure_paths);
        drop(candidates);
        if scan.incomplete_tail {
            file.set_len(scan.valid_end)?;
            file.sync_data()?;
        }
        for superseded in superseded_paths {
            std::fs::remove_file(&superseded)?;
            sync_parent_directory(&superseded)?;
        }
        let records = scan.records;
        let manager = Self::from_scan(file, root_path, path, header, &records, scan.valid_end)?;
        Ok((manager, records, scan.incomplete_tail))
    }

    fn from_scan(
        file: File,
        root_path: PathBuf,
        path: PathBuf,
        header: WalHeader,
        records: &[WalRecord],
        length: u64,
    ) -> Result<Self, WalError> {
        let last_lsn = records.last().map(|record| record.lsn);
        let last_by_txn = records
            .iter()
            .map(|record| (record.txn_id, record.lsn))
            .collect();
        let txn_states = records.iter().fold(HashMap::new(), |mut states, record| {
            states.insert(record.txn_id, wal_state_after(&record.kind));
            states
        });
        let next_lsn = logical_lsn(header.base_lsn, length)?;
        let records_next_txn_id = records.iter().map(|record| record.txn_id.0).max().map_or(
            Ok(header.next_txn_id.0),
            |maximum| {
                maximum
                    .checked_add(1)
                    .ok_or(WalError::InvalidNextTxnId(maximum))
            },
        )?;
        let next_txn_id = TxnId(header.next_txn_id.0.max(records_next_txn_id));
        Ok(Self {
            file,
            root_path,
            path,
            generation: header.generation,
            base_lsn: header.base_lsn,
            checkpoint_lsn: header.checkpoint_lsn,
            next_txn_id,
            next_offset: length,
            next_lsn,
            written_lsn: last_lsn,
            // Readability after reopening does not prove that a prior owner
            // called fsync. Conservatively require the next durability request
            // to synchronize the file again.
            durable_lsn: None,
            last_by_txn,
            txn_states,
            poisoned: false,
            #[cfg(test)]
            fail_next_flush: false,
            #[cfg(test)]
            fail_next_append_after: None,
            #[cfg(test)]
            fail_next_rotation_after: None,
        })
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    #[must_use]
    pub fn generation(&self) -> u64 {
        self.generation
    }

    #[must_use]
    pub fn base_lsn(&self) -> Lsn {
        self.base_lsn
    }

    #[must_use]
    pub fn checkpoint_lsn(&self) -> Option<Lsn> {
        self.checkpoint_lsn
    }

    #[must_use]
    pub fn next_txn_id(&self) -> TxnId {
        self.next_txn_id
    }

    #[must_use]
    pub fn next_lsn(&self) -> Lsn {
        self.next_lsn
    }

    #[must_use]
    pub fn written_lsn(&self) -> Option<Lsn> {
        self.written_lsn
    }

    #[must_use]
    pub fn durable_lsn(&self) -> Option<Lsn> {
        self.durable_lsn
    }

    pub fn append(
        &mut self,
        txn_id: TxnId,
        prev_lsn: Option<Lsn>,
        kind: WalRecordKind,
    ) -> Result<Lsn, WalError> {
        self.ensure_healthy()?;
        let lsn = self.next_lsn;
        let expected_prev = self.last_by_txn.get(&txn_id).copied();
        if prev_lsn != expected_prev {
            return Err(WalError::InvalidPrevLsn {
                lsn,
                expected: expected_prev,
                actual: prev_lsn,
            });
        }
        validate_transaction_sequence(lsn, txn_id, self.txn_states.get(&txn_id).copied(), &kind)?;
        validate_page_images(lsn, &kind)?;
        let begin_next_txn_id = if matches!(kind, WalRecordKind::Begin) {
            Some(
                txn_id
                    .0
                    .checked_add(1)
                    .ok_or(WalError::InvalidNextTxnId(txn_id.0))?,
            )
        } else {
            None
        };
        let record = WalRecord {
            lsn,
            txn_id,
            prev_lsn,
            kind,
        };
        let bytes = encode_record(&record)?;
        let next_lsn = Lsn(lsn
            .0
            .checked_add(bytes.len() as u64)
            .ok_or(WalError::LsnOverflow)?);
        let next_offset = self
            .next_offset
            .checked_add(bytes.len() as u64)
            .ok_or(WalError::LsnOverflow)?;
        self.file.seek(SeekFrom::Start(self.next_offset))?;
        #[cfg(test)]
        if let Some(prefix_len) = self.fail_next_append_after.take() {
            let prefix_len = prefix_len.min(bytes.len());
            if let Err(error) = self.file.write_all(&bytes[..prefix_len]) {
                return Err(self.rollback_failed_append(error));
            }
            return Err(self.rollback_failed_append(std::io::Error::other(
                "injected partial WAL append failure",
            )));
        }
        #[cfg(test)]
        if crate::crash_test::is_enabled(crate::crash_test::TestCrashPoint::WalPartialFinalRecord) {
            self.file.write_all(&bytes[..bytes.len() / 2])?;
            crate::crash_test::crash_now();
        }
        if let Err(error) = self.file.write_all(&bytes) {
            return Err(self.rollback_failed_append(error));
        }
        self.next_offset = next_offset;
        self.next_lsn = next_lsn;
        self.written_lsn = Some(lsn);
        self.last_by_txn.insert(txn_id, lsn);
        self.txn_states
            .insert(txn_id, wal_state_after(&record.kind));
        if let Some(next_txn_id) = begin_next_txn_id {
            self.next_txn_id = TxnId(next_txn_id.max(self.next_txn_id.0));
        }
        Ok(lsn)
    }

    pub fn flush_through(&mut self, lsn: Lsn) -> Result<(), WalError> {
        self.ensure_healthy()?;
        if self.durable_lsn.is_some_and(|durable| durable >= lsn) {
            return Ok(());
        }
        if !self.written_lsn.is_some_and(|written| written >= lsn) {
            return Err(WalError::FlushBeyondWritten {
                requested: lsn,
                written: self.written_lsn,
            });
        }
        #[cfg(test)]
        if std::mem::take(&mut self.fail_next_flush) {
            return Err(WalError::Io(std::io::Error::other(
                "injected WAL flush failure",
            )));
        }
        self.file.sync_data()?;
        self.durable_lsn = self.written_lsn;
        Ok(())
    }

    pub fn scan(&mut self) -> Result<Vec<WalRecord>, WalError> {
        self.ensure_healthy()?;
        Ok(scan_file(&mut self.file, TailPolicy::Reject)?.records)
    }

    /// Starts a new durable WAL generation after the caller has synchronized
    /// every database page represented by the current generation.
    pub(crate) fn rotate(&mut self, next_txn_id: TxnId) -> Result<(), WalError> {
        self.ensure_healthy()?;
        if next_txn_id.0 == 0 || next_txn_id.0 < self.next_txn_id.0 {
            return Err(WalError::InvalidNextTxnId(next_txn_id.0));
        }
        if let Some(written) = self.written_lsn {
            self.flush_through(written)?;
        }
        let generation = self
            .generation
            .checked_add(1)
            .ok_or(WalError::InvalidGeneration(self.generation))?;
        let header = WalHeader {
            generation,
            base_lsn: self.next_lsn,
            checkpoint_lsn: self.written_lsn.or(self.checkpoint_lsn),
            next_txn_id,
        };
        validate_header(header)?;

        let alternate = wal_alternate_path(&self.root_path);
        let target = if self.path == self.root_path {
            alternate
        } else {
            self.root_path.clone()
        };
        if target.try_exists()? {
            std::fs::remove_file(&target)?;
            sync_parent_directory(&target)?;
        }
        #[cfg(test)]
        let failure_after = self.fail_next_rotation_after.take();
        #[cfg(not(test))]
        let failure_after = None;
        let file = match create_generation_file(&target, header, failure_after) {
            Ok(file) => file,
            Err(failure) => {
                self.poisoned = true;
                return Err(failure.error);
            }
        };
        if let Err(error) = sync_parent_directory(&target) {
            self.poisoned = true;
            return Err(error);
        }
        #[cfg(test)]
        crate::crash_test::maybe_crash(
            crate::crash_test::TestCrashPoint::CheckpointAfterNewGenerationDurable,
        );

        let old_path = self.path.clone();
        let old_file = std::mem::replace(&mut self.file, file);
        self.path = target;
        self.generation = header.generation;
        self.base_lsn = header.base_lsn;
        self.checkpoint_lsn = header.checkpoint_lsn;
        self.next_txn_id = header.next_txn_id;
        self.next_offset = WAL_HEADER_SIZE as u64;
        self.next_lsn = header.base_lsn;
        self.written_lsn = None;
        self.durable_lsn = None;
        self.last_by_txn.clear();
        self.txn_states.clear();
        drop(old_file);
        if let Err(error) = std::fs::remove_file(&old_path) {
            self.poisoned = true;
            return Err(error.into());
        }
        #[cfg(test)]
        crate::crash_test::maybe_crash(
            crate::crash_test::TestCrashPoint::CheckpointAfterOldGenerationRemoved,
        );
        if let Err(error) = sync_parent_directory(&old_path) {
            self.poisoned = true;
            return Err(error);
        }
        Ok(())
    }

    pub fn close(mut self) -> Result<(), WalError> {
        self.ensure_healthy()?;
        if let Some(written) = self.written_lsn {
            self.flush_through(written)?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn inject_flush_failure(&mut self) {
        self.fail_next_flush = true;
    }

    #[cfg(test)]
    pub(crate) fn inject_partial_append_failure(&mut self, after_bytes: usize) {
        self.fail_next_append_after = Some(after_bytes);
    }

    #[cfg(test)]
    pub(crate) fn inject_partial_rotation_failure(&mut self, after_bytes: usize) {
        self.fail_next_rotation_after = Some(after_bytes);
    }

    fn rollback_failed_append(&mut self, append_error: std::io::Error) -> WalError {
        if self.file.set_len(self.next_offset).is_err()
            || self.file.seek(SeekFrom::Start(self.next_offset)).is_err()
        {
            self.poisoned = true;
        }
        WalError::Io(append_error)
    }

    fn ensure_healthy(&self) -> Result<(), WalError> {
        if self.poisoned {
            return Err(WalError::Poisoned);
        }
        Ok(())
    }
}

#[must_use]
pub fn wal_path(database_path: impl AsRef<Path>) -> PathBuf {
    let mut path = database_path.as_ref().as_os_str().to_os_string();
    path.push("-wal");
    PathBuf::from(path)
}

#[must_use]
pub fn wal_alternate_path(wal_root_path: impl AsRef<Path>) -> PathBuf {
    let mut path = wal_root_path.as_ref().as_os_str().to_os_string();
    path.push(".next");
    PathBuf::from(path)
}

fn encode_header(header: WalHeader) -> Result<[u8; WAL_HEADER_SIZE], WalError> {
    validate_header(header)?;
    let mut bytes = [0_u8; WAL_HEADER_SIZE];
    bytes[0..4].copy_from_slice(WAL_MAGIC);
    bytes[4..6].copy_from_slice(&WAL_FORMAT_VERSION.to_le_bytes());
    bytes[6..8].copy_from_slice(&(WAL_HEADER_SIZE as u16).to_le_bytes());
    bytes[8..16].copy_from_slice(&header.generation.to_le_bytes());
    bytes[16..24].copy_from_slice(&header.base_lsn.0.to_le_bytes());
    bytes[24..32].copy_from_slice(&header.checkpoint_lsn.map_or(0, |lsn| lsn.0).to_le_bytes());
    bytes[32..40].copy_from_slice(&header.next_txn_id.0.to_le_bytes());
    Ok(bytes)
}

fn read_header(file: &mut File) -> Result<WalHeader, WalError> {
    let length = file.metadata()?.len();
    if length < WAL_HEADER_SIZE as u64 {
        return Err(WalError::TruncatedHeader);
    }
    file.seek(SeekFrom::Start(0))?;
    let mut bytes = [0_u8; WAL_HEADER_SIZE];
    file.read_exact(&mut bytes)?;
    if &bytes[0..4] != WAL_MAGIC {
        return Err(WalError::InvalidMagic);
    }
    let version = read_u16(&bytes, 4);
    if version != WAL_FORMAT_VERSION {
        return Err(WalError::UnsupportedVersion(version));
    }
    let header_size = read_u16(&bytes, 6);
    if usize::from(header_size) != WAL_HEADER_SIZE {
        return Err(WalError::InvalidHeaderSize(header_size));
    }
    if bytes[40..48].iter().any(|byte| *byte != 0) {
        return Err(WalError::InvalidReservedBytes);
    }
    let raw_checkpoint = read_u64(&bytes, 24);
    let header = WalHeader {
        generation: read_u64(&bytes, 8),
        base_lsn: Lsn(read_u64(&bytes, 16)),
        checkpoint_lsn: (raw_checkpoint != 0).then_some(Lsn(raw_checkpoint)),
        next_txn_id: TxnId(read_u64(&bytes, 32)),
    };
    validate_header(header)?;
    Ok(header)
}

fn validate_header(header: WalHeader) -> Result<(), WalError> {
    if header.generation == 0 {
        return Err(WalError::InvalidGeneration(header.generation));
    }
    if header.base_lsn.0 == 0
        || header
            .checkpoint_lsn
            .is_some_and(|checkpoint| checkpoint >= header.base_lsn)
    {
        return Err(WalError::InvalidBaseLsn {
            base_lsn: header.base_lsn,
            checkpoint_lsn: header.checkpoint_lsn,
        });
    }
    if header.next_txn_id.0 == 0 {
        return Err(WalError::InvalidNextTxnId(0));
    }
    Ok(())
}

struct GenerationCreateFailure {
    error: WalError,
    file_created: bool,
}

fn create_generation_file(
    path: &Path,
    header: WalHeader,
    fail_after: Option<usize>,
) -> Result<File, GenerationCreateFailure> {
    let bytes = encode_header(header).map_err(|error| GenerationCreateFailure {
        error,
        file_created: false,
    })?;
    let mut file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(path)
        .map_err(|error| GenerationCreateFailure {
            error: error.into(),
            file_created: false,
        })?;
    if let Some(prefix_len) = fail_after {
        file.write_all(&bytes[..prefix_len.min(bytes.len())])
            .map_err(|error| GenerationCreateFailure {
                error: error.into(),
                file_created: true,
            })?;
        file.sync_all().map_err(|error| GenerationCreateFailure {
            error: error.into(),
            file_created: true,
        })?;
        return Err(GenerationCreateFailure {
            error: WalError::Io(std::io::Error::other(
                "injected partial WAL generation creation failure",
            )),
            file_created: true,
        });
    }
    file.write_all(&bytes)
        .map_err(|error| GenerationCreateFailure {
            error: error.into(),
            file_created: true,
        })?;
    file.sync_all().map_err(|error| GenerationCreateFailure {
        error: error.into(),
        file_created: true,
    })?;
    Ok(file)
}

fn sync_parent_directory(path: &Path) -> Result<(), WalError> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    File::open(parent)?.sync_all()?;
    Ok(())
}

fn logical_lsn(base_lsn: Lsn, physical_offset: u64) -> Result<Lsn, WalError> {
    let relative = physical_offset
        .checked_sub(WAL_HEADER_SIZE as u64)
        .ok_or(WalError::TruncatedHeader)?;
    Ok(Lsn(base_lsn
        .0
        .checked_add(relative)
        .ok_or(WalError::LsnOverflow)?))
}

fn encode_record(record: &WalRecord) -> Result<Vec<u8>, WalError> {
    let payload_len = record.kind.payload_len();
    let total_len = RECORD_HEADER_SIZE
        .checked_add(payload_len)
        .ok_or(WalError::LsnOverflow)?;
    let mut bytes = Vec::with_capacity(total_len);
    bytes.extend_from_slice(RECORD_MAGIC);
    bytes.extend_from_slice(&RECORD_FORMAT_VERSION.to_le_bytes());
    bytes.push(record.kind.tag());
    bytes.push(0);
    bytes.extend_from_slice(&(total_len as u32).to_le_bytes());
    bytes.extend_from_slice(&(payload_len as u32).to_le_bytes());
    bytes.extend_from_slice(&record.lsn.0.to_le_bytes());
    bytes.extend_from_slice(&record.txn_id.0.to_le_bytes());
    bytes.extend_from_slice(&record.prev_lsn.map_or(0, |lsn| lsn.0).to_le_bytes());
    if let WalRecordKind::PageUpdate {
        page_id,
        before,
        after,
    } = &record.kind
    {
        bytes.extend_from_slice(&page_id.0.to_le_bytes());
        bytes.extend_from_slice(before.as_ref());
        bytes.extend_from_slice(after.as_ref());
    }
    Ok(bytes)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TailPolicy {
    Reject,
    AllowIncompleteFinalRecord,
}

#[derive(Debug)]
struct ScanResult {
    records: Vec<WalRecord>,
    valid_end: u64,
    incomplete_tail: bool,
}

fn validate_generation_candidates(
    candidates: &[(PathBuf, File, WalHeader, ScanResult)],
) -> Result<(), WalError> {
    if candidates.len() < 2 {
        return Ok(());
    }
    let (_, _, older_header, older_scan) = &candidates[candidates.len() - 2];
    let (_, _, newer_header, _) = &candidates[candidates.len() - 1];
    if newer_header.generation == older_header.generation {
        return Err(WalError::GenerationConflict);
    }
    let expected_generation = older_header
        .generation
        .checked_add(1)
        .ok_or(WalError::InvalidGeneration(older_header.generation))?;
    let older_end = logical_lsn(older_header.base_lsn, older_scan.valid_end)?;
    let older_checkpoint = older_scan
        .records
        .last()
        .map(|record| record.lsn)
        .or(older_header.checkpoint_lsn);
    let older_next_txn_id = older_scan
        .records
        .iter()
        .map(|record| record.txn_id.0)
        .max()
        .map_or(older_header.next_txn_id.0, |maximum| {
            maximum.saturating_add(1).max(older_header.next_txn_id.0)
        });
    if newer_header.generation != expected_generation
        || newer_header.base_lsn != older_end
        || newer_header.checkpoint_lsn != older_checkpoint
        || newer_header.next_txn_id.0 < older_next_txn_id
    {
        return Err(WalError::GenerationConflict);
    }
    Ok(())
}

fn scan_file(file: &mut File, tail_policy: TailPolicy) -> Result<ScanResult, WalError> {
    let length = file.metadata()?.len();
    let header = read_header(file)?;

    let mut records = Vec::new();
    let mut offset = WAL_HEADER_SIZE as u64;
    let mut txn_last_lsn = HashMap::<TxnId, Lsn>::new();
    let mut txn_states = HashMap::<TxnId, WalTxnState>::new();
    while offset < length {
        let lsn = logical_lsn(header.base_lsn, offset)?;
        let remaining = length - offset;
        if remaining < RECORD_HEADER_SIZE as u64 {
            let mut partial = vec![0_u8; remaining as usize];
            file.seek(SeekFrom::Start(offset))?;
            file.read_exact(&mut partial)?;
            validate_partial_record_header(lsn, &partial, &txn_last_lsn, &txn_states)?;
            if tail_policy == TailPolicy::AllowIncompleteFinalRecord {
                return Ok(ScanResult {
                    records,
                    valid_end: offset,
                    incomplete_tail: true,
                });
            }
            return Err(WalError::TruncatedRecord { lsn });
        }
        let mut record_header = [0_u8; RECORD_HEADER_SIZE];
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(&mut record_header)?;
        if &record_header[0..4] != RECORD_MAGIC {
            return Err(WalError::InvalidRecordMagic { lsn });
        }
        let record_version = read_u16(&record_header, 4);
        if record_version != RECORD_FORMAT_VERSION {
            return Err(WalError::UnsupportedRecordVersion {
                lsn,
                version: record_version,
            });
        }
        if record_header[7] != 0 {
            return Err(WalError::InvalidReservedBytes);
        }
        let total_len = read_u32(&record_header, 8);
        if total_len < RECORD_HEADER_SIZE as u32 {
            return Err(WalError::InvalidRecordLength {
                lsn,
                length: total_len,
            });
        }
        if total_len as usize > WAL_MAX_RECORD_SIZE {
            return Err(WalError::RecordTooLarge {
                lsn,
                length: total_len,
            });
        }
        let payload_len = read_u32(&record_header, 12);
        let expected_payload = total_len - RECORD_HEADER_SIZE as u32;
        if payload_len != expected_payload {
            return Err(WalError::InvalidPayloadLength {
                lsn,
                expected: expected_payload,
                actual: payload_len,
            });
        }
        let record_type = record_header[6];
        require_payload_length(
            lsn,
            payload_len,
            expected_payload_for_tag(lsn, record_type)?,
        )?;
        let actual_lsn = Lsn(read_u64(&record_header, 16));
        if actual_lsn != lsn {
            return Err(WalError::InvalidRecordedLsn {
                expected: lsn,
                actual: actual_lsn,
            });
        }
        let txn_id = TxnId(read_u64(&record_header, 24));
        let raw_prev = read_u64(&record_header, 32);
        let prev_lsn = (raw_prev != 0).then_some(Lsn(raw_prev));
        let expected_prev = txn_last_lsn.get(&txn_id).copied();
        if prev_lsn != expected_prev {
            return Err(WalError::InvalidPrevLsn {
                lsn,
                expected: expected_prev,
                actual: prev_lsn,
            });
        }
        validate_transaction_tag_sequence(
            lsn,
            txn_id,
            txn_states.get(&txn_id).copied(),
            record_type,
        )?;

        if u64::from(total_len) > remaining {
            if tail_policy == TailPolicy::AllowIncompleteFinalRecord {
                return Ok(ScanResult {
                    records,
                    valid_end: offset,
                    incomplete_tail: true,
                });
            }
            return Err(WalError::TruncatedRecord { lsn });
        }

        let kind = match record_type {
            1 => {
                require_payload_length(lsn, payload_len, 0)?;
                WalRecordKind::Begin
            }
            2 => {
                require_payload_length(lsn, payload_len, PAGE_UPDATE_PAYLOAD_SIZE as u32)?;
                let mut payload = [0_u8; PAGE_UPDATE_PAYLOAD_SIZE];
                file.read_exact(&mut payload)?;
                let page_id = PageId(read_u64(&payload, 0));
                let mut before = Box::new([0_u8; PAGE_SIZE]);
                before.copy_from_slice(&payload[8..8 + PAGE_SIZE]);
                let mut after = Box::new([0_u8; PAGE_SIZE]);
                after.copy_from_slice(&payload[8 + PAGE_SIZE..]);
                WalRecordKind::PageUpdate {
                    page_id,
                    before,
                    after,
                }
            }
            3 => {
                require_payload_length(lsn, payload_len, 0)?;
                WalRecordKind::Commit
            }
            4 => {
                require_payload_length(lsn, payload_len, 0)?;
                WalRecordKind::Abort
            }
            5 => {
                require_payload_length(lsn, payload_len, 0)?;
                WalRecordKind::RollbackComplete
            }
            tag => return Err(WalError::UnknownRecordType { lsn, tag }),
        };
        validate_page_images(lsn, &kind)?;
        txn_last_lsn.insert(txn_id, lsn);
        txn_states.insert(txn_id, wal_state_after(&kind));
        records.push(WalRecord {
            lsn,
            txn_id,
            prev_lsn,
            kind,
        });
        offset = offset
            .checked_add(u64::from(total_len))
            .ok_or(WalError::LsnOverflow)?;
    }
    Ok(ScanResult {
        records,
        valid_end: offset,
        incomplete_tail: false,
    })
}

fn validate_partial_record_header(
    lsn: Lsn,
    bytes: &[u8],
    txn_last_lsn: &HashMap<TxnId, Lsn>,
    txn_states: &HashMap<TxnId, WalTxnState>,
) -> Result<(), WalError> {
    let magic_len = bytes.len().min(RECORD_MAGIC.len());
    if bytes[..magic_len] != RECORD_MAGIC[..magic_len] {
        return Err(WalError::InvalidRecordMagic { lsn });
    }
    if bytes.len() >= 6 {
        let version = read_u16(bytes, 4);
        if version != RECORD_FORMAT_VERSION {
            return Err(WalError::UnsupportedRecordVersion { lsn, version });
        }
    }
    if bytes.len() >= 7 {
        expected_payload_for_tag(lsn, bytes[6])?;
    }
    if bytes.len() >= 8 && bytes[7] != 0 {
        return Err(WalError::InvalidReservedBytes);
    }
    if bytes.len() >= 12 {
        let total_len = read_u32(bytes, 8);
        if total_len < RECORD_HEADER_SIZE as u32 {
            return Err(WalError::InvalidRecordLength {
                lsn,
                length: total_len,
            });
        }
        if total_len as usize > WAL_MAX_RECORD_SIZE {
            return Err(WalError::RecordTooLarge {
                lsn,
                length: total_len,
            });
        }
    }
    if bytes.len() >= 16 {
        let total_len = read_u32(bytes, 8);
        let payload_len = read_u32(bytes, 12);
        let expected = total_len - RECORD_HEADER_SIZE as u32;
        if payload_len != expected {
            return Err(WalError::InvalidPayloadLength {
                lsn,
                expected,
                actual: payload_len,
            });
        }
        let kind_expected = expected_payload_for_tag(lsn, bytes[6])?;
        require_payload_length(lsn, payload_len, kind_expected)?;
    }
    if bytes.len() >= 24 {
        let actual = Lsn(read_u64(bytes, 16));
        if actual != lsn {
            return Err(WalError::InvalidRecordedLsn {
                expected: lsn,
                actual,
            });
        }
    }
    if bytes.len() >= 32 {
        let txn_id = TxnId(read_u64(bytes, 24));
        let record_type = bytes[6];
        let state = txn_states.get(&txn_id).copied();
        validate_transaction_tag_sequence(lsn, txn_id, state, record_type)?;
        let available_prev = bytes.len().saturating_sub(32).min(8);
        if available_prev > 0 {
            let expected = txn_last_lsn
                .get(&txn_id)
                .copied()
                .map_or(0, |prev_lsn| prev_lsn.0)
                .to_le_bytes();
            if bytes[32..32 + available_prev] != expected[..available_prev] {
                return Err(WalError::InvalidPartialPrevLsn { lsn });
            }
        }
    }
    Ok(())
}

fn expected_payload_for_tag(lsn: Lsn, tag: u8) -> Result<u32, WalError> {
    match tag {
        1 | 3 | 4 | 5 => Ok(0),
        2 => Ok(PAGE_UPDATE_PAYLOAD_SIZE as u32),
        tag => Err(WalError::UnknownRecordType { lsn, tag }),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WalTxnState {
    Active,
    Aborting,
    Complete,
}

fn wal_state_after(kind: &WalRecordKind) -> WalTxnState {
    match kind {
        WalRecordKind::Begin | WalRecordKind::PageUpdate { .. } => WalTxnState::Active,
        WalRecordKind::Abort => WalTxnState::Aborting,
        WalRecordKind::Commit | WalRecordKind::RollbackComplete => WalTxnState::Complete,
    }
}

fn validate_transaction_sequence(
    lsn: Lsn,
    txn_id: TxnId,
    state: Option<WalTxnState>,
    kind: &WalRecordKind,
) -> Result<(), WalError> {
    validate_transaction_tag_sequence(lsn, txn_id, state, kind.tag())
}

fn validate_transaction_tag_sequence(
    lsn: Lsn,
    txn_id: TxnId,
    state: Option<WalTxnState>,
    record_type: u8,
) -> Result<(), WalError> {
    let valid = matches!(
        (state, record_type),
        (None, 1) | (Some(WalTxnState::Active), 2..=4) | (Some(WalTxnState::Aborting), 5)
    );
    if !valid {
        return Err(WalError::InvalidTransactionSequence {
            lsn,
            txn_id,
            record_type,
        });
    }
    Ok(())
}

fn validate_page_images(lsn: Lsn, kind: &WalRecordKind) -> Result<(), WalError> {
    let WalRecordKind::PageUpdate {
        page_id,
        before,
        after,
    } = kind
    else {
        return Ok(());
    };

    if before.iter().any(|byte| *byte != 0) {
        let before_page = Page::from_bytes(*page_id, **before);
        let before_lsn = before_page
            .page_lsn()
            .map_err(|_| WalError::InvalidPageImage {
                lsn,
                image: "before",
            })?;
        if before_lsn.is_some_and(|before_lsn| before_lsn >= lsn) {
            return Err(WalError::InvalidPageImage {
                lsn,
                image: "before",
            });
        }
    }

    let after_page = Page::from_bytes(*page_id, **after);
    let page_lsn = after_page
        .page_lsn()
        .map_err(|_| WalError::InvalidPageImage {
            lsn,
            image: "after",
        })?;
    if page_lsn != Some(lsn) {
        return Err(WalError::InvalidPageLsn {
            record_lsn: lsn,
            page_lsn,
        });
    }
    Ok(())
}

fn require_payload_length(lsn: Lsn, actual: u32, expected: u32) -> Result<(), WalError> {
    if actual != expected {
        return Err(WalError::InvalidPayloadLength {
            lsn,
            expected,
            actual,
        });
    }
    Ok(())
}

fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes([bytes[offset], bytes[offset + 1]])
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
    ])
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
        bytes[offset + 4],
        bytes[offset + 5],
        bytes[offset + 6],
        bytes[offset + 7],
    ])
}

pub(crate) fn page_update_kind(before: &Page, after: &Page) -> WalRecordKind {
    WalRecordKind::PageUpdate {
        page_id: after.id,
        before: Box::new(*before.bytes()),
        after: Box::new(*after.bytes()),
    }
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;
    use std::io::{Seek, SeekFrom, Write};

    use netbadb_types::{PageId, TxnId};

    use super::{WAL_HEADER_SIZE, WalError, WalManager, WalRecordKind};
    use crate::{Page, PageType};

    fn test_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("netbadb-{name}-{}-wal", std::process::id()))
    }

    fn initial_physical_offset(lsn: netbadb_types::Lsn) -> u64 {
        super::WAL_HEADER_SIZE as u64 + lsn.0 - super::INITIAL_BASE_LSN.0
    }

    #[test]
    fn create_does_not_remove_an_existing_wal() {
        let path = test_path("wal-create-existing");
        let _ = std::fs::remove_file(&path);
        let mut wal = WalManager::create(&path).expect("create original WAL");
        let begin = wal
            .append(TxnId(7), None, WalRecordKind::Begin)
            .expect("append original record");
        wal.flush_through(begin).expect("flush original record");
        drop(wal);
        let original = std::fs::read(&path).expect("read original WAL");

        assert!(matches!(
            WalManager::create(&path),
            Err(WalError::Io(error)) if error.kind() == std::io::ErrorKind::AlreadyExists
        ));
        assert_eq!(
            std::fs::read(&path).expect("read WAL after rejected create"),
            original
        );
        let mut reopened = WalManager::open(&path).expect("reopen original WAL");
        assert_eq!(reopened.scan().expect("scan original WAL").len(), 1);
        drop(reopened);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn records_round_trip_after_reopen_with_prev_lsn_chain() {
        let path = test_path("wal-round-trip");
        let mut wal = WalManager::create(&path).expect("create WAL");
        let begin = wal
            .append(TxnId(7), None, WalRecordKind::Begin)
            .expect("append begin");
        let before = Page::new(PageId(1), PageType::Heap);
        let mut after = before.clone();
        after.insert_record(b"row").expect("insert record");
        let update_lsn = wal.next_lsn();
        after.set_page_lsn(update_lsn);
        let update = wal
            .append(
                TxnId(7),
                Some(begin),
                super::page_update_kind(&before, &after),
            )
            .expect("append update");
        let commit = wal
            .append(TxnId(7), Some(update), WalRecordKind::Commit)
            .expect("append commit");
        wal.flush_through(commit).expect("flush commit");
        drop(wal);

        let mut reopened = WalManager::open(&path).expect("open WAL");
        let records = reopened.scan().expect("scan WAL");
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].prev_lsn, None);
        assert_eq!(records[1].prev_lsn, Some(begin));
        assert_eq!(records[2].prev_lsn, Some(update));
        assert_eq!(reopened.durable_lsn(), None);
        reopened.inject_flush_failure();
        assert!(matches!(
            reopened.flush_through(commit),
            Err(WalError::Io(_))
        ));
        assert_eq!(reopened.durable_lsn(), None);
        reopened.flush_through(commit).expect("resync reopened WAL");
        assert_eq!(reopened.durable_lsn(), Some(commit));
        let WalRecordKind::PageUpdate {
            page_id,
            before: decoded_before,
            after: decoded_after,
        } = &records[1].kind
        else {
            panic!("expected page update");
        };
        assert_eq!(*page_id, PageId(1));
        assert_eq!(decoded_before.as_ref(), before.bytes());
        assert_eq!(decoded_after.as_ref(), after.bytes());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn scanner_rejects_truncated_and_oversized_records_before_allocating() {
        let truncated_path = test_path("wal-truncated");
        let wal = WalManager::create(&truncated_path).expect("create WAL");
        drop(wal);
        OpenOptions::new()
            .append(true)
            .open(&truncated_path)
            .expect("open WAL")
            .write_all(b"WREC")
            .expect("write partial record");
        assert!(matches!(
            WalManager::open(&truncated_path),
            Err(WalError::TruncatedRecord { .. })
        ));

        let oversized_path = test_path("wal-oversized");
        let mut wal = WalManager::create(&oversized_path).expect("create WAL");
        wal.append(TxnId(1), None, WalRecordKind::Begin)
            .expect("append begin");
        drop(wal);
        let mut file = OpenOptions::new()
            .write(true)
            .open(&oversized_path)
            .expect("open WAL");
        file.seek(SeekFrom::Start(WAL_HEADER_SIZE as u64 + 8))
            .expect("seek record length");
        file.write_all(&u32::MAX.to_le_bytes())
            .expect("corrupt record length");
        drop(file);
        assert!(matches!(
            WalManager::open(&oversized_path),
            Err(WalError::RecordTooLarge { .. })
        ));
        let _ = std::fs::remove_file(truncated_path);
        let _ = std::fs::remove_file(oversized_path);
    }

    #[test]
    fn recovery_rejects_an_invalid_transaction_sequence_in_a_partial_page_update() {
        let path = test_path("wal-invalid-partial-sequence");
        let wal = WalManager::create(&path).expect("create WAL");
        let lsn = wal.next_lsn();
        drop(wal);

        let mut header = [0_u8; super::RECORD_HEADER_SIZE];
        header[0..4].copy_from_slice(super::RECORD_MAGIC);
        header[4..6].copy_from_slice(&super::RECORD_FORMAT_VERSION.to_le_bytes());
        header[6] = 2;
        header[8..12].copy_from_slice(&(super::WAL_MAX_RECORD_SIZE as u32).to_le_bytes());
        header[12..16].copy_from_slice(&(super::PAGE_UPDATE_PAYLOAD_SIZE as u32).to_le_bytes());
        header[16..24].copy_from_slice(&lsn.0.to_le_bytes());
        header[24..32].copy_from_slice(&999_u64.to_le_bytes());
        let mut file = OpenOptions::new()
            .append(true)
            .open(&path)
            .expect("open WAL tail");
        file.write_all(&header).expect("write record header");
        file.write_all(&[0_u8; 8]).expect("write partial payload");
        drop(file);
        let length_before = std::fs::metadata(&path).expect("WAL metadata").len();

        assert!(matches!(
            WalManager::open_for_recovery(&path),
            Err(WalError::InvalidTransactionSequence {
                txn_id: TxnId(999),
                record_type: 2,
                ..
            })
        ));
        assert_eq!(
            std::fs::metadata(&path).expect("WAL metadata").len(),
            length_before
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn scanner_rejects_bad_magic_version_and_short_declared_length() {
        let magic_path = test_path("wal-bad-magic");
        let wal = WalManager::create(&magic_path).expect("create WAL");
        drop(wal);
        overwrite(&magic_path, 0, b"FAIL");
        assert!(matches!(
            WalManager::open(&magic_path),
            Err(WalError::InvalidMagic)
        ));

        let version_path = test_path("wal-bad-version");
        let wal = WalManager::create(&version_path).expect("create WAL");
        drop(wal);
        overwrite(&version_path, 4, &1_u16.to_le_bytes());
        assert!(matches!(
            WalManager::open(&version_path),
            Err(WalError::UnsupportedVersion(1))
        ));

        let length_path = test_path("wal-short-length");
        let mut wal = WalManager::create(&length_path).expect("create WAL");
        wal.append(TxnId(1), None, WalRecordKind::Begin)
            .expect("append begin");
        drop(wal);
        overwrite(
            &length_path,
            WAL_HEADER_SIZE as u64 + 8,
            &39_u32.to_le_bytes(),
        );
        assert!(matches!(
            WalManager::open(&length_path),
            Err(WalError::InvalidRecordLength { .. })
        ));
        let _ = std::fs::remove_file(magic_path);
        let _ = std::fs::remove_file(version_path);
        let _ = std::fs::remove_file(length_path);
    }

    #[test]
    fn scanner_rejects_invalid_generation_metadata_and_slot_conflicts() {
        let generation_path = test_path("wal-zero-generation");
        drop(WalManager::create(&generation_path).expect("create WAL"));
        overwrite(&generation_path, 8, &0_u64.to_le_bytes());
        assert!(matches!(
            WalManager::open(&generation_path),
            Err(WalError::InvalidGeneration(0))
        ));

        let base_path = test_path("wal-invalid-base");
        drop(WalManager::create(&base_path).expect("create WAL"));
        overwrite(&base_path, 24, &1_u64.to_le_bytes());
        assert!(matches!(
            WalManager::open(&base_path),
            Err(WalError::InvalidBaseLsn { .. })
        ));

        let txn_path = test_path("wal-zero-next-txn");
        drop(WalManager::create(&txn_path).expect("create WAL"));
        overwrite(&txn_path, 32, &0_u64.to_le_bytes());
        assert!(matches!(
            WalManager::open(&txn_path),
            Err(WalError::InvalidNextTxnId(0))
        ));

        let conflict_path = test_path("wal-generation-conflict");
        drop(WalManager::create(&conflict_path).expect("create WAL"));
        let alternate = super::wal_alternate_path(&conflict_path);
        std::fs::copy(&conflict_path, &alternate).expect("copy conflicting generation");
        assert!(matches!(
            WalManager::open(&conflict_path),
            Err(WalError::GenerationConflict)
        ));

        for path in [
            generation_path,
            base_path,
            txn_path,
            conflict_path,
            alternate,
        ] {
            let _ = std::fs::remove_file(path);
        }
    }

    #[test]
    fn scanner_rejects_a_malformed_page_update_image() {
        let path = test_path("wal-bad-page-image");
        let mut wal = WalManager::create(&path).expect("create WAL");
        let begin = wal
            .append(TxnId(1), None, WalRecordKind::Begin)
            .expect("append begin");
        let before = Page::new(PageId(1), PageType::Heap);
        let mut after = before.clone();
        after.insert_record(b"row").expect("insert record");
        let update_lsn = wal.next_lsn();
        after.set_page_lsn(update_lsn);
        wal.append(
            TxnId(1),
            Some(begin),
            super::page_update_kind(&before, &after),
        )
        .expect("append update");
        drop(wal);

        let after_image_offset = initial_physical_offset(update_lsn)
            + super::RECORD_HEADER_SIZE as u64
            + 8
            + crate::PAGE_SIZE as u64;
        overwrite(&path, after_image_offset + 4, &99_u16.to_le_bytes());
        assert!(matches!(
            WalManager::open(&path),
            Err(WalError::InvalidPageImage { image: "after", .. })
        ));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn partial_append_failure_is_truncated_before_the_next_record() {
        let path = test_path("wal-partial-append");
        let mut wal = WalManager::create(&path).expect("create WAL");
        let begin = wal
            .append(TxnId(1), None, WalRecordKind::Begin)
            .expect("append begin");
        let before = Page::new(PageId(1), PageType::Heap);
        let mut after = before.clone();
        after.insert_record(b"row").expect("insert record");
        let update_lsn = wal.next_lsn();
        after.set_page_lsn(update_lsn);
        wal.inject_partial_append_failure(100);

        assert!(matches!(
            wal.append(
                TxnId(1),
                Some(begin),
                super::page_update_kind(&before, &after),
            ),
            Err(WalError::Io(_))
        ));
        assert_eq!(
            std::fs::metadata(&path).expect("read WAL length").len(),
            initial_physical_offset(update_lsn)
        );
        wal.append(TxnId(1), Some(begin), WalRecordKind::Abort)
            .expect("append abort after rollback");
        let records = wal.scan().expect("scan repaired WAL");
        assert_eq!(records.len(), 2);
        assert!(matches!(records[1].kind, WalRecordKind::Abort));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn scanner_rejects_a_broken_prev_lsn_chain() {
        let path = test_path("wal-prev-chain");
        let mut wal = WalManager::create(&path).expect("create WAL");
        let begin = wal
            .append(TxnId(1), None, WalRecordKind::Begin)
            .expect("append begin");
        let commit = wal
            .append(TxnId(1), Some(begin), WalRecordKind::Commit)
            .expect("append commit");
        drop(wal);

        let mut file = OpenOptions::new()
            .write(true)
            .open(&path)
            .expect("open WAL");
        file.seek(SeekFrom::Start(initial_physical_offset(commit) + 32))
            .expect("seek prevLSN");
        file.write_all(&999_u64.to_le_bytes())
            .expect("corrupt prevLSN");
        drop(file);
        assert!(matches!(
            WalManager::open(&path),
            Err(WalError::InvalidPrevLsn { .. })
        ));
        let _ = std::fs::remove_file(path);
    }

    fn overwrite(path: &std::path::Path, offset: u64, bytes: &[u8]) {
        let mut file = OpenOptions::new()
            .write(true)
            .open(path)
            .expect("open WAL for corruption");
        file.seek(SeekFrom::Start(offset)).expect("seek WAL");
        file.write_all(bytes).expect("overwrite WAL bytes");
    }
}
