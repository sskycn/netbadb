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

pub const WAL_FORMAT_VERSION: u16 = 1;
pub const WAL_HEADER_SIZE: usize = 16;
pub const WAL_MAX_RECORD_SIZE: usize = RECORD_HEADER_SIZE + PAGE_UPDATE_PAYLOAD_SIZE;

#[derive(Debug)]
pub enum WalError {
    Io(std::io::Error),
    InvalidMagic,
    UnsupportedVersion(u16),
    InvalidHeaderSize(u16),
    InvalidReservedBytes,
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
}

impl WalRecordKind {
    const fn tag(&self) -> u8 {
        match self {
            Self::Begin => 1,
            Self::PageUpdate { .. } => 2,
            Self::Commit => 3,
            Self::Abort => 4,
        }
    }

    const fn payload_len(&self) -> usize {
        match self {
            Self::PageUpdate { .. } => PAGE_UPDATE_PAYLOAD_SIZE,
            Self::Begin | Self::Commit | Self::Abort => 0,
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
    path: PathBuf,
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
}

impl WalManager {
    pub fn create(path: impl AsRef<Path>) -> Result<Self, WalError> {
        let path = path.as_ref().to_owned();
        let mut file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)?;
        let mut header = [0_u8; WAL_HEADER_SIZE];
        header[0..4].copy_from_slice(WAL_MAGIC);
        header[4..6].copy_from_slice(&WAL_FORMAT_VERSION.to_le_bytes());
        header[6..8].copy_from_slice(&(WAL_HEADER_SIZE as u16).to_le_bytes());
        let initialization = (|| -> std::io::Result<()> {
            file.write_all(&header)?;
            file.sync_all()
        })();
        if let Err(error) = initialization {
            drop(file);
            let _ = std::fs::remove_file(&path);
            return Err(error.into());
        }
        Ok(Self {
            file,
            path,
            next_lsn: Lsn(WAL_HEADER_SIZE as u64),
            written_lsn: None,
            durable_lsn: None,
            last_by_txn: HashMap::new(),
            txn_states: HashMap::new(),
            poisoned: false,
            #[cfg(test)]
            fail_next_flush: false,
            #[cfg(test)]
            fail_next_append_after: None,
        })
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self, WalError> {
        let path = path.as_ref().to_owned();
        let mut file = OpenOptions::new().read(true).write(true).open(&path)?;
        let records = scan_file(&mut file)?;
        let length = file.metadata()?.len();
        let last_lsn = records.last().map(|record| record.lsn);
        let last_by_txn = records
            .iter()
            .map(|record| (record.txn_id, record.lsn))
            .collect();
        let txn_states = records.iter().fold(HashMap::new(), |mut states, record| {
            states.insert(
                record.txn_id,
                if matches!(&record.kind, WalRecordKind::Commit | WalRecordKind::Abort) {
                    WalTxnState::Complete
                } else {
                    WalTxnState::Active
                },
            );
            states
        });
        Ok(Self {
            file,
            path,
            next_lsn: Lsn(length),
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
        })
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
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
        self.file.seek(SeekFrom::Start(lsn.0))?;
        #[cfg(test)]
        if let Some(prefix_len) = self.fail_next_append_after.take() {
            let prefix_len = prefix_len.min(bytes.len());
            if let Err(error) = self.file.write_all(&bytes[..prefix_len]) {
                return Err(self.rollback_failed_append(lsn, error));
            }
            return Err(self.rollback_failed_append(
                lsn,
                std::io::Error::other("injected partial WAL append failure"),
            ));
        }
        if let Err(error) = self.file.write_all(&bytes) {
            return Err(self.rollback_failed_append(lsn, error));
        }
        self.next_lsn = next_lsn;
        self.written_lsn = Some(lsn);
        self.last_by_txn.insert(txn_id, lsn);
        self.txn_states.insert(
            txn_id,
            if matches!(&record.kind, WalRecordKind::Commit | WalRecordKind::Abort) {
                WalTxnState::Complete
            } else {
                WalTxnState::Active
            },
        );
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
        scan_file(&mut self.file)
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

    fn rollback_failed_append(&mut self, lsn: Lsn, append_error: std::io::Error) -> WalError {
        if self.file.set_len(lsn.0).is_err() || self.file.seek(SeekFrom::Start(lsn.0)).is_err() {
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

fn scan_file(file: &mut File) -> Result<Vec<WalRecord>, WalError> {
    let length = file.metadata()?.len();
    if length < WAL_HEADER_SIZE as u64 {
        return Err(WalError::TruncatedHeader);
    }
    file.seek(SeekFrom::Start(0))?;
    let mut header = [0_u8; WAL_HEADER_SIZE];
    file.read_exact(&mut header)?;
    if &header[0..4] != WAL_MAGIC {
        return Err(WalError::InvalidMagic);
    }
    let version = read_u16(&header, 4);
    if version != WAL_FORMAT_VERSION {
        return Err(WalError::UnsupportedVersion(version));
    }
    let header_size = read_u16(&header, 6);
    if usize::from(header_size) != WAL_HEADER_SIZE {
        return Err(WalError::InvalidHeaderSize(header_size));
    }
    if header[8..].iter().any(|byte| *byte != 0) {
        return Err(WalError::InvalidReservedBytes);
    }

    let mut records = Vec::new();
    let mut offset = WAL_HEADER_SIZE as u64;
    let mut txn_last_lsn = HashMap::<TxnId, Lsn>::new();
    let mut txn_states = HashMap::<TxnId, WalTxnState>::new();
    while offset < length {
        let lsn = Lsn(offset);
        let remaining = length - offset;
        if remaining < RECORD_HEADER_SIZE as u64 {
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
        if u64::from(total_len) > remaining {
            return Err(WalError::TruncatedRecord { lsn });
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

        let kind = match record_header[6] {
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
            tag => return Err(WalError::UnknownRecordType { lsn, tag }),
        };
        validate_transaction_sequence(lsn, txn_id, txn_states.get(&txn_id).copied(), &kind)?;
        validate_page_images(lsn, &kind)?;
        txn_last_lsn.insert(txn_id, lsn);
        txn_states.insert(
            txn_id,
            if matches!(&kind, WalRecordKind::Commit | WalRecordKind::Abort) {
                WalTxnState::Complete
            } else {
                WalTxnState::Active
            },
        );
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
    Ok(records)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WalTxnState {
    Active,
    Complete,
}

fn validate_transaction_sequence(
    lsn: Lsn,
    txn_id: TxnId,
    state: Option<WalTxnState>,
    kind: &WalRecordKind,
) -> Result<(), WalError> {
    let valid = matches!(
        (state, kind),
        (None, WalRecordKind::Begin)
            | (Some(WalTxnState::Active), WalRecordKind::PageUpdate { .. })
            | (Some(WalTxnState::Active), WalRecordKind::Commit)
            | (Some(WalTxnState::Active), WalRecordKind::Abort)
    );
    if !valid {
        return Err(WalError::InvalidTransactionSequence {
            lsn,
            txn_id,
            record_type: kind.tag(),
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
        overwrite(&version_path, 4, &99_u16.to_le_bytes());
        assert!(matches!(
            WalManager::open(&version_path),
            Err(WalError::UnsupportedVersion(99))
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

        let after_image_offset =
            update_lsn.0 + super::RECORD_HEADER_SIZE as u64 + 8 + crate::PAGE_SIZE as u64;
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
            update_lsn.0
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
        file.seek(SeekFrom::Start(commit.0 + 32))
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
