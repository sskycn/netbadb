use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::rc::Rc;

use netbadb_types::{CommitSeq, TxnId};

const STATUS_MAGIC: &[u8; 4] = b"NBTS";
const STATUS_VERSION: u16 = 1;
const STATUS_HEADER_SIZE: usize = 16;
const RECORD_MAGIC: &[u8; 4] = b"TXST";
const RECORD_VERSION: u16 = 1;
const RECORD_SIZE: usize = 32;
const COMMITTED_TAG: u8 = 1;
const ABORTED_TAG: u8 = 2;

pub(crate) const fn committed_record_write_bytes() -> usize {
    RECORD_SIZE
}

pub(crate) type SharedTxnStatus = Rc<RefCell<TxnStatusStore>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnStatus {
    Active,
    Committed(CommitSeq),
    Aborted,
}

#[derive(Debug)]
pub enum TxnStatusError {
    Io(std::io::Error),
    AppendCleanup {
        offset: u64,
        append: std::io::Error,
        cleanup: std::io::Error,
    },
    RecoveryRequired,
    InvalidMagic,
    UnsupportedVersion(u16),
    InvalidHeaderSize(u16),
    InvalidReservedBytes,
    HeaderChecksumMismatch {
        stored: u32,
        computed: u32,
    },
    TruncatedRecord,
    InvalidRecordMagic {
        offset: u64,
    },
    UnsupportedRecordVersion {
        offset: u64,
        version: u16,
    },
    InvalidStatusTag {
        offset: u64,
        tag: u8,
    },
    InvalidTxnId {
        offset: u64,
    },
    InvalidCommitSeq {
        offset: u64,
    },
    InvalidRecordReserved {
        offset: u64,
    },
    RecordChecksumMismatch {
        offset: u64,
        stored: u32,
        computed: u32,
    },
    ConflictingStatus {
        txn_id: TxnId,
    },
    UnknownTransaction {
        txn_id: TxnId,
    },
    SnapshotCountOverflow,
}

impl std::fmt::Display for TxnStatusError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "transaction-status I/O error: {error}"),
            Self::AppendCleanup {
                offset,
                append,
                cleanup,
            } => write!(
                formatter,
                "transaction-status append at {offset} failed: {append}; truncation failed: {cleanup}; reopen required"
            ),
            Self::RecoveryRequired => {
                formatter.write_str("transaction-status writer requires reopen/recovery")
            }
            Self::InvalidMagic => formatter.write_str("transaction-status magic does not match"),
            Self::UnsupportedVersion(version) => {
                write!(
                    formatter,
                    "unsupported transaction-status version {version}"
                )
            }
            Self::InvalidHeaderSize(size) => {
                write!(formatter, "invalid transaction-status header size {size}")
            }
            Self::InvalidReservedBytes => {
                formatter.write_str("transaction-status reserved bytes are non-zero")
            }
            Self::HeaderChecksumMismatch { stored, computed } => write!(
                formatter,
                "transaction-status header checksum mismatch: stored {stored:#010x}, computed {computed:#010x}"
            ),
            Self::TruncatedRecord => formatter.write_str("transaction-status record is truncated"),
            Self::InvalidRecordMagic { offset } => {
                write!(
                    formatter,
                    "transaction-status record at {offset} has invalid magic"
                )
            }
            Self::UnsupportedRecordVersion { offset, version } => write!(
                formatter,
                "transaction-status record at {offset} has unsupported version {version}"
            ),
            Self::InvalidStatusTag { offset, tag } => write!(
                formatter,
                "transaction-status record at {offset} has invalid tag {tag}"
            ),
            Self::InvalidTxnId { offset } => {
                write!(
                    formatter,
                    "transaction-status record at {offset} has transaction ID zero"
                )
            }
            Self::InvalidCommitSeq { offset } => write!(
                formatter,
                "transaction-status record at {offset} has an invalid commit sequence"
            ),
            Self::InvalidRecordReserved { offset } => write!(
                formatter,
                "transaction-status record at {offset} has non-zero reserved bytes"
            ),
            Self::RecordChecksumMismatch {
                offset,
                stored,
                computed,
            } => write!(
                formatter,
                "transaction-status record at {offset} has checksum {stored:#010x}, computed {computed:#010x}"
            ),
            Self::ConflictingStatus { txn_id } => write!(
                formatter,
                "transaction {} has conflicting durable statuses",
                txn_id.0
            ),
            Self::UnknownTransaction { txn_id } => write!(
                formatter,
                "tuple references unknown transaction {}",
                txn_id.0
            ),
            Self::SnapshotCountOverflow => {
                formatter.write_str("active snapshot reference count overflowed")
            }
        }
    }
}

impl std::error::Error for TxnStatusError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::AppendCleanup { append, .. } => Some(append),
            _ => None,
        }
    }
}

impl From<std::io::Error> for TxnStatusError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

#[derive(Debug)]
pub(crate) struct TxnStatusStore {
    file: File,
    durable: HashMap<TxnId, TxnStatus>,
    active: HashSet<TxnId>,
    snapshots: BTreeMap<CommitSeq, u64>,
    maximum_commit_seq: CommitSeq,
    poisoned: bool,
    #[cfg(test)]
    pub(crate) fail_next_append_after: Option<usize>,
    #[cfg(test)]
    pub(crate) fail_next_truncate: bool,
}

impl TxnStatusStore {
    pub(crate) fn create(path: impl AsRef<Path>) -> Result<Self, TxnStatusError> {
        let path = path.as_ref().to_owned();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)?;
        let header = encode_header();
        file.write_all(&header)?;
        file.sync_all()?;
        sync_parent_directory(&path)?;
        Ok(Self {
            file,
            durable: HashMap::new(),
            active: HashSet::new(),
            snapshots: BTreeMap::new(),
            maximum_commit_seq: CommitSeq(0),
            poisoned: false,
            #[cfg(test)]
            fail_next_append_after: None,
            #[cfg(test)]
            fail_next_truncate: false,
        })
    }

    #[cfg(test)]
    pub(crate) fn open(path: impl AsRef<Path>) -> Result<Self, TxnStatusError> {
        Self::open_for_recovery(path, &[])
    }

    pub(crate) fn open_for_recovery(
        path: impl AsRef<Path>,
        records: &[crate::WalRecord],
    ) -> Result<Self, TxnStatusError> {
        let path = path.as_ref().to_owned();
        let mut file = OpenOptions::new().read(true).write(true).open(&path)?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        validate_header(&bytes)?;
        let mut durable = HashMap::new();
        let mut maximum_commit_seq = CommitSeq(0);
        for (position, record) in bytes[STATUS_HEADER_SIZE..]
            .chunks_exact(RECORD_SIZE)
            .enumerate()
        {
            let offset = (STATUS_HEADER_SIZE + position * RECORD_SIZE) as u64;
            let (txn_id, status) = decode_record(record, offset)?;
            insert_status(&mut durable, txn_id, status)?;
            if let TxnStatus::Committed(commit_seq) = status {
                maximum_commit_seq = maximum_commit_seq.max(commit_seq);
            }
        }
        let tail_size = (bytes.len() - STATUS_HEADER_SIZE) % RECORD_SIZE;
        if tail_size != 0 {
            let valid_end = bytes.len() - tail_size;
            let tail = &bytes[valid_end..];
            // Never discard archived status history by guessing. Only a prefix
            // of a terminal decision still present in validated WAL is repairable.
            // Recovery replays that decision before snapshots become available.
            let certified = records.iter().any(|record| {
                let status = match record.kind {
                    crate::WalRecordKind::Commit => TxnStatus::Committed(CommitSeq(record.lsn.0)),
                    crate::WalRecordKind::RollbackComplete => TxnStatus::Aborted,
                    _ => return false,
                };
                durable
                    .get(&record.txn_id)
                    .is_none_or(|existing| *existing == status)
                    && encode_record(record.txn_id, status)
                        .is_ok_and(|bytes| bytes.starts_with(tail))
            });
            if !certified {
                return Err(TxnStatusError::TruncatedRecord);
            }
            file.set_len(valid_end as u64)?;
        }
        // Readable bytes from a previous process are not proof of a completed
        // sync. Establish durability before an equal-status retry can return OK.
        file.sync_data()?;
        Ok(Self {
            file,
            durable,
            active: HashSet::new(),
            snapshots: BTreeMap::new(),
            maximum_commit_seq,
            poisoned: false,
            #[cfg(test)]
            fail_next_append_after: None,
            #[cfg(test)]
            fail_next_truncate: false,
        })
    }

    pub(crate) fn mark_active(&mut self, txn_id: TxnId) {
        self.active.insert(txn_id);
    }

    pub(crate) fn clear_active(&mut self, txn_id: TxnId) {
        self.active.remove(&txn_id);
    }

    pub(crate) fn record_committed(
        &mut self,
        txn_id: TxnId,
        commit_seq: CommitSeq,
    ) -> Result<(), TxnStatusError> {
        self.record(txn_id, TxnStatus::Committed(commit_seq))
    }

    pub(crate) fn record_aborted(&mut self, txn_id: TxnId) -> Result<(), TxnStatusError> {
        self.record(txn_id, TxnStatus::Aborted)
    }

    fn record(&mut self, txn_id: TxnId, status: TxnStatus) -> Result<(), TxnStatusError> {
        if self.poisoned {
            return Err(TxnStatusError::RecoveryRequired);
        }
        if let Some(existing) = self.durable.get(&txn_id).copied() {
            if existing == status {
                self.active.remove(&txn_id);
                return Ok(());
            }
            return Err(TxnStatusError::ConflictingStatus { txn_id });
        }
        let bytes = encode_record(txn_id, status)?;
        let offset = self.file.seek(SeekFrom::End(0))?;
        let append = (|| {
            #[cfg(test)]
            if let Some(count) = self.fail_next_append_after.take() {
                self.file.write_all(&bytes[..count.min(bytes.len())])?;
                return Err(std::io::Error::other("injected partial status append"));
            }
            #[cfg(test)]
            {
                self.file.write_all(&bytes[..17])?;
                crate::crash_test::maybe_crash_named("status-after-partial-record");
            }
            #[cfg(test)]
            let bytes = &bytes[17..];
            #[cfg(not(test))]
            let bytes = &bytes[..];
            self.file.write_all(bytes)?;
            self.file.sync_data()
        })();
        if let Err(append) = append {
            let cleanup = self.truncate_failed_append(offset);
            return Err(match cleanup {
                Ok(()) => TxnStatusError::Io(append),
                Err(cleanup) => {
                    self.poisoned = true;
                    TxnStatusError::AppendCleanup {
                        offset,
                        append,
                        cleanup,
                    }
                }
            });
        }
        self.durable.insert(txn_id, status);
        self.active.remove(&txn_id);
        if let TxnStatus::Committed(commit_seq) = status {
            self.maximum_commit_seq = self.maximum_commit_seq.max(commit_seq);
        }
        #[cfg(any(test, feature = "test-hooks"))]
        crate::index_write_bound_test_activity::record_txn_status_append(status, bytes.len());
        Ok(())
    }

    fn truncate_failed_append(&mut self, offset: u64) -> std::io::Result<()> {
        #[cfg(test)]
        if std::mem::take(&mut self.fail_next_truncate) {
            return Err(std::io::Error::other("injected status truncation failure"));
        }
        self.file.set_len(offset)
    }

    pub(crate) fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    pub(crate) fn status(&self, txn_id: TxnId) -> Result<TxnStatus, TxnStatusError> {
        if let Some(status) = self.durable.get(&txn_id).copied() {
            return Ok(status);
        }
        if self.active.contains(&txn_id) {
            return Ok(TxnStatus::Active);
        }
        Err(TxnStatusError::UnknownTransaction { txn_id })
    }

    pub(crate) fn maximum_commit_seq(&self) -> CommitSeq {
        self.maximum_commit_seq
    }

    pub(crate) fn pin_snapshot(&mut self, commit_seq: CommitSeq) -> Result<(), TxnStatusError> {
        let count = self.snapshots.entry(commit_seq).or_insert(0);
        *count = count
            .checked_add(1)
            .ok_or(TxnStatusError::SnapshotCountOverflow)?;
        Ok(())
    }

    pub(crate) fn unpin_snapshot(&mut self, commit_seq: CommitSeq) {
        if let Some(count) = self.snapshots.get_mut(&commit_seq) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                self.snapshots.remove(&commit_seq);
            }
        }
    }

    pub(crate) fn oldest_snapshot(&self) -> Option<CommitSeq> {
        self.snapshots
            .first_key_value()
            .map(|(sequence, _)| *sequence)
    }
}

fn insert_status(
    statuses: &mut HashMap<TxnId, TxnStatus>,
    txn_id: TxnId,
    status: TxnStatus,
) -> Result<(), TxnStatusError> {
    match statuses.insert(txn_id, status) {
        None => Ok(()),
        Some(existing) if existing == status => Ok(()),
        Some(_) => Err(TxnStatusError::ConflictingStatus { txn_id }),
    }
}

fn encode_header() -> [u8; STATUS_HEADER_SIZE] {
    let mut bytes = [0_u8; STATUS_HEADER_SIZE];
    bytes[0..4].copy_from_slice(STATUS_MAGIC);
    bytes[4..6].copy_from_slice(&STATUS_VERSION.to_le_bytes());
    bytes[6..8].copy_from_slice(&(STATUS_HEADER_SIZE as u16).to_le_bytes());
    let checksum = crc32c::crc32c(&bytes);
    bytes[12..16].copy_from_slice(&checksum.to_le_bytes());
    bytes
}

fn validate_header(bytes: &[u8]) -> Result<(), TxnStatusError> {
    if bytes.len() < STATUS_HEADER_SIZE {
        return Err(TxnStatusError::TruncatedRecord);
    }
    if &bytes[0..4] != STATUS_MAGIC {
        return Err(TxnStatusError::InvalidMagic);
    }
    let version = u16::from_le_bytes([bytes[4], bytes[5]]);
    if version != STATUS_VERSION {
        return Err(TxnStatusError::UnsupportedVersion(version));
    }
    let size = u16::from_le_bytes([bytes[6], bytes[7]]);
    if usize::from(size) != STATUS_HEADER_SIZE {
        return Err(TxnStatusError::InvalidHeaderSize(size));
    }
    if bytes[8..12] != [0; 4] {
        return Err(TxnStatusError::InvalidReservedBytes);
    }
    let stored = u32::from_le_bytes(
        bytes[12..16]
            .try_into()
            .map_err(|_| TxnStatusError::TruncatedRecord)?,
    );
    let mut header = bytes[..STATUS_HEADER_SIZE].to_vec();
    header[12..16].fill(0);
    let computed = crc32c::crc32c(&header);
    if stored != computed {
        return Err(TxnStatusError::HeaderChecksumMismatch { stored, computed });
    }
    Ok(())
}

fn encode_record(txn_id: TxnId, status: TxnStatus) -> Result<[u8; RECORD_SIZE], TxnStatusError> {
    if txn_id.0 == 0 {
        return Err(TxnStatusError::InvalidTxnId { offset: 0 });
    }
    let (tag, commit_seq) = match status {
        TxnStatus::Committed(sequence) if sequence.0 != 0 => (COMMITTED_TAG, sequence.0),
        TxnStatus::Committed(_) => return Err(TxnStatusError::InvalidCommitSeq { offset: 0 }),
        TxnStatus::Aborted => (ABORTED_TAG, 0),
        TxnStatus::Active => return Err(TxnStatusError::InvalidStatusTag { offset: 0, tag: 0 }),
    };
    let mut bytes = [0_u8; RECORD_SIZE];
    bytes[0..4].copy_from_slice(RECORD_MAGIC);
    bytes[4..6].copy_from_slice(&RECORD_VERSION.to_le_bytes());
    bytes[6] = tag;
    bytes[8..16].copy_from_slice(&txn_id.0.to_le_bytes());
    bytes[16..24].copy_from_slice(&commit_seq.to_le_bytes());
    let checksum = crc32c::crc32c(&bytes);
    bytes[24..28].copy_from_slice(&checksum.to_le_bytes());
    Ok(bytes)
}

fn decode_record(bytes: &[u8], offset: u64) -> Result<(TxnId, TxnStatus), TxnStatusError> {
    if bytes.len() != RECORD_SIZE {
        return Err(TxnStatusError::TruncatedRecord);
    }
    if &bytes[0..4] != RECORD_MAGIC {
        return Err(TxnStatusError::InvalidRecordMagic { offset });
    }
    let version = u16::from_le_bytes([bytes[4], bytes[5]]);
    if version != RECORD_VERSION {
        return Err(TxnStatusError::UnsupportedRecordVersion { offset, version });
    }
    if bytes[7] != 0 || bytes[28..32] != [0; 4] {
        return Err(TxnStatusError::InvalidRecordReserved { offset });
    }
    let stored = u32::from_le_bytes(
        bytes[24..28]
            .try_into()
            .map_err(|_| TxnStatusError::TruncatedRecord)?,
    );
    let mut checked = bytes.to_vec();
    checked[24..28].fill(0);
    let computed = crc32c::crc32c(&checked);
    if stored != computed {
        return Err(TxnStatusError::RecordChecksumMismatch {
            offset,
            stored,
            computed,
        });
    }
    let txn_id = TxnId(u64::from_le_bytes(
        bytes[8..16]
            .try_into()
            .map_err(|_| TxnStatusError::TruncatedRecord)?,
    ));
    if txn_id.0 == 0 {
        return Err(TxnStatusError::InvalidTxnId { offset });
    }
    let sequence = CommitSeq(u64::from_le_bytes(
        bytes[16..24]
            .try_into()
            .map_err(|_| TxnStatusError::TruncatedRecord)?,
    ));
    let status = match bytes[6] {
        COMMITTED_TAG if sequence.0 != 0 => TxnStatus::Committed(sequence),
        COMMITTED_TAG => return Err(TxnStatusError::InvalidCommitSeq { offset }),
        ABORTED_TAG if sequence.0 == 0 => TxnStatus::Aborted,
        ABORTED_TAG => return Err(TxnStatusError::InvalidCommitSeq { offset }),
        tag => return Err(TxnStatusError::InvalidStatusTag { offset, tag }),
    };
    Ok((txn_id, status))
}

fn sync_parent_directory(path: &Path) -> Result<(), TxnStatusError> {
    File::open(path.parent().unwrap_or_else(|| Path::new(".")))?.sync_all()?;
    Ok(())
}

pub fn txn_status_path(database_path: &Path) -> PathBuf {
    let mut value = database_path.as_os_str().to_owned();
    value.push("-txn-status");
    PathBuf::from(value)
}

#[cfg(test)]
mod tests {
    use super::{TxnStatus, TxnStatusError, TxnStatusStore};
    use netbadb_types::{CommitSeq, TxnId};

    fn path(case: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "netbadb-txn-status-{case}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ))
    }

    #[test]
    fn committed_record_sizing_helper_matches_the_production_encoder() {
        let bytes = super::encode_record(TxnId(1), TxnStatus::Committed(CommitSeq(1))).unwrap();
        assert_eq!(bytes.len(), super::committed_record_write_bytes());
    }

    #[test]
    fn crash_audit_status_partial_append_retry_reopens() {
        let path = path("partial-append-retry");
        let _ = std::fs::remove_file(&path);
        let mut store = TxnStatusStore::create(&path).unwrap();
        store.fail_next_append_after = Some(17);
        assert!(store.record_committed(TxnId(7), CommitSeq(91)).is_err());
        store.record_committed(TxnId(7), CommitSeq(91)).unwrap();
        drop(store);
        for _ in 0..2 {
            let store =
                TxnStatusStore::open(&path).expect("successful retry leaves a valid status log");
            assert_eq!(
                store.status(TxnId(7)).unwrap(),
                TxnStatus::Committed(CommitSeq(91))
            );
        }
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn crash_audit_status_tail_requires_matching_wal_and_valid_complete_prefix() {
        use crate::{WalRecord, WalRecordKind};
        use netbadb_types::Lsn;
        let path = path("certified-tail");
        let _ = std::fs::remove_file(&path);
        let store = TxnStatusStore::create(&path).unwrap();
        drop(store);
        let header = std::fs::read(&path).unwrap();
        let record = super::encode_record(TxnId(7), TxnStatus::Committed(CommitSeq(91))).unwrap();
        let wal = [WalRecord {
            lsn: Lsn(91),
            txn_id: TxnId(7),
            prev_lsn: Some(Lsn(1)),
            kind: WalRecordKind::Commit,
        }];
        for length in 1..super::RECORD_SIZE {
            let bytes = [header.as_slice(), &record[..length]].concat();
            std::fs::write(&path, &bytes).unwrap();
            assert!(matches!(
                TxnStatusStore::open_for_recovery(&path, &[]),
                Err(TxnStatusError::TruncatedRecord)
            ));
            assert_eq!(std::fs::read(&path).unwrap(), bytes);
            let mut store = TxnStatusStore::open_for_recovery(&path, &wal).unwrap();
            store.record_committed(TxnId(7), CommitSeq(91)).unwrap();
            drop(store);
            assert!(TxnStatusStore::open(&path).is_ok());
        }
        let mut bad_tail = [header.as_slice(), &record[..17]].concat();
        bad_tail[super::STATUS_HEADER_SIZE] ^= 1;
        std::fs::write(&path, &bad_tail).unwrap();
        assert!(TxnStatusStore::open_for_recovery(&path, &wal).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), bad_tail);
        let mut corrupt_complete = [header.as_slice(), record.as_slice(), &record[..17]].concat();
        corrupt_complete[super::STATUS_HEADER_SIZE + 24] ^= 1;
        std::fs::write(&path, &corrupt_complete).unwrap();
        assert!(matches!(
            TxnStatusStore::open_for_recovery(&path, &wal),
            Err(TxnStatusError::RecordChecksumMismatch { .. })
        ));
        assert_eq!(std::fs::read(&path).unwrap(), corrupt_complete);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn durable_status_round_trips_committed_and_aborted_records() {
        let path = path("round-trip");
        let _ = std::fs::remove_file(&path);
        let mut store = TxnStatusStore::create(&path).expect("create status store");
        store
            .record_committed(TxnId(7), CommitSeq(91))
            .expect("record commit");
        store.record_aborted(TxnId(8)).expect("record abort");
        drop(store);

        let reopened = TxnStatusStore::open(&path).expect("reopen status store");
        assert_eq!(
            reopened.status(TxnId(7)).unwrap(),
            TxnStatus::Committed(CommitSeq(91))
        );
        assert_eq!(reopened.status(TxnId(8)).unwrap(), TxnStatus::Aborted);
        assert_eq!(reopened.maximum_commit_seq(), CommitSeq(91));
        drop(reopened);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn durable_status_rejects_header_record_and_truncation_corruption() {
        let source = path("corruption-source");
        let _ = std::fs::remove_file(&source);
        let mut store = TxnStatusStore::create(&source).expect("create status store");
        store
            .record_committed(TxnId(1), CommitSeq(40))
            .expect("record commit");
        drop(store);
        let original = std::fs::read(&source).expect("read status bytes");

        type CorruptionCase = (&'static str, Vec<u8>, fn(&TxnStatusError) -> bool);
        let cases: Vec<CorruptionCase> = vec![
            (
                "bad-magic",
                {
                    let mut bytes = original.clone();
                    bytes[0] ^= 0xff;
                    bytes
                },
                |error| matches!(error, TxnStatusError::InvalidMagic),
            ),
            (
                "bad-version",
                {
                    let mut bytes = original.clone();
                    bytes[4..6].copy_from_slice(&2_u16.to_le_bytes());
                    bytes
                },
                |error| matches!(error, TxnStatusError::UnsupportedVersion(2)),
            ),
            (
                "truncated",
                original[..original.len() - 1].to_vec(),
                |error| matches!(error, TxnStatusError::TruncatedRecord),
            ),
            (
                "bad-tag",
                {
                    let mut bytes = original.clone();
                    bytes[22] = 99;
                    bytes[40..44].fill(0);
                    let checksum = crc32c::crc32c(&bytes[16..48]);
                    bytes[40..44].copy_from_slice(&checksum.to_le_bytes());
                    bytes
                },
                |error| matches!(error, TxnStatusError::InvalidStatusTag { tag: 99, .. }),
            ),
            (
                "bad-checksum",
                {
                    let mut bytes = original.clone();
                    bytes[32] ^= 1;
                    bytes
                },
                |error| matches!(error, TxnStatusError::RecordChecksumMismatch { .. }),
            ),
        ];
        for (case, bytes, predicate) in cases {
            let target = path(case);
            let _ = std::fs::remove_file(&target);
            std::fs::write(&target, bytes).expect("write corrupt status");
            let error = TxnStatusStore::open(&target).expect_err("reject corruption");
            assert!(predicate(&error), "unexpected {case} error: {error}");
            let _ = std::fs::remove_file(target);
        }
        let _ = std::fs::remove_file(source);
    }
}
