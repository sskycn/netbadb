use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use netbadb_types::{DatabaseTxnId, StorageId, TxnId};

const LOG_MAGIC: &[u8; 4] = b"NBCO";
const LOG_VERSION: u16 = 1;
const LOG_HEADER_SIZE: usize = 16;
const RECORD_MAGIC: &[u8; 4] = b"CORD";
const RECORD_VERSION: u16 = 1;
const RECORD_HEADER_SIZE: usize = 32;
const RECORD_CHECKSUM_OFFSET: usize = 12;
const COMMIT_DECISION_TAG: u8 = 1;
const COMPLETE_TAG: u8 = 2;
const SCHEMA_COMMIT_TAG: u8 = 3;
const SCHEMA_REFERENCE_SIZE: usize = 56;
pub(crate) const MAX_COORDINATOR_PARTICIPANTS: usize = 1_024;
const PARTICIPANT_SIZE: usize = 16;
const MAX_RECORD_SIZE: usize =
    RECORD_HEADER_SIZE + MAX_COORDINATOR_PARTICIPANTS * PARTICIPANT_SIZE + SCHEMA_REFERENCE_SIZE;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct CoordinatorParticipant {
    pub(crate) storage_id: StorageId,
    pub(crate) physical_txn_id: TxnId,
}

/// CORD v2 reference; the full schema remains in a separately synced NBSC.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SchemaParticipantReference {
    pub(crate) incarnation: [u8; 16],
    pub(crate) target_epoch: u64,
    pub(crate) digest: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CoordinatorDecision {
    pub(crate) database_txn_id: DatabaseTxnId,
    pub(crate) participants: Vec<CoordinatorParticipant>,
    pub(crate) complete: bool,
    pub(crate) schema: Option<SchemaParticipantReference>,
}

#[derive(Debug)]
pub(crate) struct CoordinatorLog {
    file: File,
    decisions: BTreeMap<DatabaseTxnId, CoordinatorDecision>,
    #[cfg(test)]
    fail_next_decision_append: bool,
    #[cfg(test)]
    fail_next_decision_sync: bool,
    #[cfg(test)]
    fail_next_complete_append: bool,
    #[cfg(test)]
    fail_next_complete_sync: bool,
}

impl CoordinatorLog {
    pub(crate) fn create(path: impl AsRef<Path>) -> Result<Self, CoordinatorLogError> {
        let path = path.as_ref().to_owned();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)?;
        file.write_all(&encode_header())?;
        file.sync_all()?;
        sync_parent_directory(&path)?;
        Ok(Self {
            file,
            decisions: BTreeMap::new(),
            #[cfg(test)]
            fail_next_decision_append: false,
            #[cfg(test)]
            fail_next_decision_sync: false,
            #[cfg(test)]
            fail_next_complete_append: false,
            #[cfg(test)]
            fail_next_complete_sync: false,
        })
    }

    pub(crate) fn open(path: impl AsRef<Path>) -> Result<Self, CoordinatorLogError> {
        let path = path.as_ref().to_owned();
        let mut file = OpenOptions::new().read(true).write(true).open(&path)?;
        let scan = scan_file(&mut file, true)?;
        if scan.incomplete_tail {
            file.set_len(scan.valid_end)?;
            file.sync_data()?;
        }
        Ok(Self {
            file,
            decisions: scan.decisions,
            #[cfg(test)]
            fail_next_decision_append: false,
            #[cfg(test)]
            fail_next_decision_sync: false,
            #[cfg(test)]
            fail_next_complete_append: false,
            #[cfg(test)]
            fail_next_complete_sync: false,
        })
    }

    pub(crate) fn decisions(&self) -> impl Iterator<Item = &CoordinatorDecision> {
        self.decisions.values()
    }

    /// Appends and synchronizes the canonical database commit decision.
    /// Repeating an identical decision only retries synchronization.
    pub(crate) fn commit_decision(
        &mut self,
        database_txn_id: DatabaseTxnId,
        participants: &[CoordinatorParticipant],
    ) -> Result<(), CoordinatorLogError> {
        self.commit_schema_decision(database_txn_id, participants, None)
    }

    pub(crate) fn commit_schema_decision(
        &mut self,
        database_txn_id: DatabaseTxnId,
        participants: &[CoordinatorParticipant],
        schema: Option<&SchemaParticipantReference>,
    ) -> Result<(), CoordinatorLogError> {
        let participants = canonical_participants(database_txn_id, participants, schema.is_some())?;
        if let Some(existing) = self.decisions.get(&database_txn_id) {
            if existing.participants != participants || existing.schema.as_ref() != schema {
                return Err(CoordinatorLogError::ConflictingDecision { database_txn_id });
            }
            self.file.sync_data()?;
            return Ok(());
        }
        let bytes = encode_record(
            database_txn_id,
            match schema {
                Some(reference) => CoordinatorRecord::SchemaCommit(&participants, reference),
                None => CoordinatorRecord::CommitDecision(&participants),
            },
        )?;
        #[cfg(test)]
        if std::mem::take(&mut self.fail_next_decision_append) {
            inject_partial_append_failure(&mut self.file, &bytes)?;
        }
        #[cfg(test)]
        if crate::coordinator_crash::enabled("during-decision-append") {
            self.file.seek(SeekFrom::End(0))?;
            self.file.write_all(&bytes[..bytes.len() / 2])?;
            crate::coordinator_crash::maybe_crash("during-decision-append");
        }
        append_record(&mut self.file, &bytes)?;
        #[cfg(test)]
        crate::coordinator_crash::maybe_crash("after-decision-append");
        self.decisions.insert(
            database_txn_id,
            CoordinatorDecision {
                database_txn_id,
                participants,
                complete: false,
                schema: schema.cloned(),
            },
        );
        #[cfg(test)]
        if std::mem::take(&mut self.fail_next_decision_sync) {
            return Err(injected_io_error("decision sync").into());
        }
        self.file.sync_data()?;
        Ok(())
    }

    /// Records that every participant commit is durable. Repeating Complete
    /// is idempotent and only retries synchronization.
    pub(crate) fn complete(
        &mut self,
        database_txn_id: DatabaseTxnId,
    ) -> Result<(), CoordinatorLogError> {
        let decision = self
            .decisions
            .get(&database_txn_id)
            .ok_or(CoordinatorLogError::CompleteWithoutDecision { database_txn_id })?;
        if decision.complete {
            self.file.sync_data()?;
            return Ok(());
        }
        let bytes = encode_record(database_txn_id, CoordinatorRecord::Complete)?;
        #[cfg(test)]
        if std::mem::take(&mut self.fail_next_complete_append) {
            inject_partial_append_failure(&mut self.file, &bytes)?;
        }
        #[cfg(test)]
        if crate::coordinator_crash::enabled("during-complete-append") {
            self.file.seek(SeekFrom::End(0))?;
            self.file.write_all(&bytes[..bytes.len() / 2])?;
            crate::coordinator_crash::maybe_crash("during-complete-append");
        }
        append_record(&mut self.file, &bytes)?;
        #[cfg(test)]
        crate::coordinator_crash::maybe_crash("after-complete-append");
        self.decisions
            .get_mut(&database_txn_id)
            .ok_or(CoordinatorLogError::CompleteWithoutDecision { database_txn_id })?
            .complete = true;
        #[cfg(test)]
        if std::mem::take(&mut self.fail_next_complete_sync) {
            return Err(injected_io_error("Complete sync").into());
        }
        self.file.sync_data()?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn inject_decision_append_failure(&mut self) {
        self.fail_next_decision_append = true;
    }

    #[cfg(test)]
    pub(crate) fn inject_decision_sync_failure(&mut self) {
        self.fail_next_decision_sync = true;
    }

    #[cfg(test)]
    pub(crate) fn inject_complete_append_failure(&mut self) {
        self.fail_next_complete_append = true;
    }

    #[cfg(test)]
    pub(crate) fn inject_complete_sync_failure(&mut self) {
        self.fail_next_complete_sync = true;
    }
}

enum CoordinatorRecord<'a> {
    CommitDecision(&'a [CoordinatorParticipant]),
    SchemaCommit(&'a [CoordinatorParticipant], &'a SchemaParticipantReference),
    Complete,
}

#[derive(Debug)]
struct CoordinatorScan {
    decisions: BTreeMap<DatabaseTxnId, CoordinatorDecision>,
    valid_end: u64,
    incomplete_tail: bool,
}

fn scan_file(
    file: &mut File,
    allow_incomplete_tail: bool,
) -> Result<CoordinatorScan, CoordinatorLogError> {
    file.seek(SeekFrom::Start(0))?;
    let length = file.metadata()?.len();
    if length < LOG_HEADER_SIZE as u64 {
        return Err(CoordinatorLogError::TruncatedHeader);
    }
    let mut header = [0_u8; LOG_HEADER_SIZE];
    file.read_exact(&mut header)?;
    validate_header(&header)?;

    let mut offset = LOG_HEADER_SIZE as u64;
    let mut decisions = BTreeMap::new();
    while offset < length {
        let remaining = length - offset;
        if remaining < RECORD_HEADER_SIZE as u64 {
            let mut partial = vec![
                0_u8;
                usize::try_from(remaining)
                    .map_err(|_| CoordinatorLogError::RecordSizeOverflow)?
            ];
            file.read_exact(&mut partial)?;
            validate_partial_header(&partial, offset)?;
            if allow_incomplete_tail {
                return Ok(CoordinatorScan {
                    decisions,
                    valid_end: offset,
                    incomplete_tail: true,
                });
            }
            return Err(CoordinatorLogError::TruncatedRecord { offset });
        }
        let mut record_header = [0_u8; RECORD_HEADER_SIZE];
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(&mut record_header)?;
        let total_len = validate_record_header(&record_header, offset)?;
        if u64::from(total_len) > remaining {
            if allow_incomplete_tail {
                return Ok(CoordinatorScan {
                    decisions,
                    valid_end: offset,
                    incomplete_tail: true,
                });
            }
            return Err(CoordinatorLogError::TruncatedRecord { offset });
        }
        let mut bytes = vec![
            0_u8;
            usize::try_from(total_len)
                .map_err(|_| CoordinatorLogError::RecordSizeOverflow)?
        ];
        bytes[..RECORD_HEADER_SIZE].copy_from_slice(&record_header);
        file.read_exact(&mut bytes[RECORD_HEADER_SIZE..])?;
        verify_record_checksum(&bytes, offset)?;
        apply_decoded_record(&bytes, offset, &mut decisions)?;
        offset = offset
            .checked_add(u64::from(total_len))
            .ok_or(CoordinatorLogError::RecordSizeOverflow)?;
    }
    Ok(CoordinatorScan {
        decisions,
        valid_end: offset,
        incomplete_tail: false,
    })
}

fn apply_decoded_record(
    bytes: &[u8],
    offset: u64,
    decisions: &mut BTreeMap<DatabaseTxnId, CoordinatorDecision>,
) -> Result<(), CoordinatorLogError> {
    let database_txn_id = DatabaseTxnId(read_u64(bytes, 16));
    if database_txn_id.0 == 0 {
        return Err(CoordinatorLogError::InvalidTransactionId { offset });
    }
    let participant_count = usize::try_from(read_u32(bytes, 24))
        .map_err(|_| CoordinatorLogError::ParticipantCountOverflow { offset })?;
    if bytes[28..32] != [0; 4] {
        return Err(CoordinatorLogError::InvalidReservedBytes { offset });
    }
    match bytes[6] {
        COMMIT_DECISION_TAG | SCHEMA_COMMIT_TAG => {
            if (participant_count == 0 && bytes[6] != SCHEMA_COMMIT_TAG)
                || participant_count > MAX_COORDINATOR_PARTICIPANTS
            {
                return Err(CoordinatorLogError::InvalidParticipantCount {
                    offset,
                    count: participant_count,
                });
            }
            let mut participants = Vec::with_capacity(participant_count);
            for position in 0..participant_count {
                let base = RECORD_HEADER_SIZE
                    .checked_add(
                        position
                            .checked_mul(PARTICIPANT_SIZE)
                            .ok_or(CoordinatorLogError::RecordSizeOverflow)?,
                    )
                    .ok_or(CoordinatorLogError::RecordSizeOverflow)?;
                let storage_id = StorageId(read_u64(bytes, base));
                let physical_txn_id = TxnId(read_u64(bytes, base + 8));
                participants.push(CoordinatorParticipant {
                    storage_id,
                    physical_txn_id,
                });
            }
            let participants = canonical_participants(
                database_txn_id,
                &participants,
                bytes[6] == SCHEMA_COMMIT_TAG,
            )?;
            let schema = if bytes[6] == SCHEMA_COMMIT_TAG {
                let base = RECORD_HEADER_SIZE + participant_count * PARTICIPANT_SIZE;
                let mut incarnation = [0; 16];
                incarnation.copy_from_slice(&bytes[base..base + 16]);
                let target_epoch = read_u64(bytes, base + 16);
                let mut digest = [0; 32];
                digest.copy_from_slice(&bytes[base + 24..base + 56]);
                if incarnation == [0; 16] || target_epoch == 0 {
                    return Err(CoordinatorLogError::InvalidReservedBytes { offset });
                }
                Some(SchemaParticipantReference {
                    incarnation,
                    target_epoch,
                    digest,
                })
            } else {
                None
            };
            if let Some(existing) = decisions.get(&database_txn_id) {
                if existing.participants != participants || existing.schema != schema {
                    return Err(CoordinatorLogError::ConflictingDecision { database_txn_id });
                }
            } else {
                decisions.insert(
                    database_txn_id,
                    CoordinatorDecision {
                        database_txn_id,
                        participants,
                        complete: false,
                        schema,
                    },
                );
            }
        }
        COMPLETE_TAG => {
            if participant_count != 0 || bytes.len() != RECORD_HEADER_SIZE {
                return Err(CoordinatorLogError::InvalidCompleteLength { offset });
            }
            let decision = decisions
                .get_mut(&database_txn_id)
                .ok_or(CoordinatorLogError::CompleteWithoutDecision { database_txn_id })?;
            decision.complete = true;
        }
        tag => return Err(CoordinatorLogError::UnknownRecordTag { offset, tag }),
    }
    Ok(())
}

fn canonical_participants(
    database_txn_id: DatabaseTxnId,
    participants: &[CoordinatorParticipant],
    allow_empty: bool,
) -> Result<Vec<CoordinatorParticipant>, CoordinatorLogError> {
    if database_txn_id.0 == 0 {
        return Err(CoordinatorLogError::InvalidTransactionId { offset: 0 });
    }
    if (participants.is_empty() && !allow_empty)
        || participants.len() > MAX_COORDINATOR_PARTICIPANTS
    {
        return Err(CoordinatorLogError::InvalidParticipantCount {
            offset: 0,
            count: participants.len(),
        });
    }
    let mut canonical = participants.to_vec();
    canonical.sort_unstable();
    let mut storage_ids = BTreeSet::new();
    for participant in &canonical {
        if participant.storage_id.0 == 0 || participant.physical_txn_id.0 == 0 {
            return Err(CoordinatorLogError::InvalidParticipantIdentity {
                database_txn_id,
                storage_id: participant.storage_id,
                physical_txn_id: participant.physical_txn_id,
            });
        }
        if !storage_ids.insert(participant.storage_id) {
            return Err(CoordinatorLogError::DuplicateStorageId {
                database_txn_id,
                storage_id: participant.storage_id,
            });
        }
    }
    Ok(canonical)
}

fn encode_header() -> [u8; LOG_HEADER_SIZE] {
    let mut bytes = [0_u8; LOG_HEADER_SIZE];
    bytes[0..4].copy_from_slice(LOG_MAGIC);
    bytes[4..6].copy_from_slice(&LOG_VERSION.to_le_bytes());
    bytes[6..8].copy_from_slice(&(LOG_HEADER_SIZE as u16).to_le_bytes());
    let checksum = crc32c::crc32c(&bytes);
    bytes[12..16].copy_from_slice(&checksum.to_le_bytes());
    bytes
}

fn validate_header(bytes: &[u8; LOG_HEADER_SIZE]) -> Result<(), CoordinatorLogError> {
    if &bytes[0..4] != LOG_MAGIC {
        return Err(CoordinatorLogError::InvalidMagic);
    }
    let version = read_u16(bytes, 4);
    if version != LOG_VERSION {
        return Err(CoordinatorLogError::UnsupportedVersion(version));
    }
    let size = read_u16(bytes, 6);
    if usize::from(size) != LOG_HEADER_SIZE {
        return Err(CoordinatorLogError::InvalidHeaderSize(size));
    }
    if bytes[8..12] != [0; 4] {
        return Err(CoordinatorLogError::InvalidHeaderReserved);
    }
    let stored = read_u32(bytes, 12);
    let mut checked = *bytes;
    checked[12..16].fill(0);
    let computed = crc32c::crc32c(&checked);
    if stored != computed {
        return Err(CoordinatorLogError::HeaderChecksumMismatch { stored, computed });
    }
    Ok(())
}

fn encode_record(
    database_txn_id: DatabaseTxnId,
    record: CoordinatorRecord<'_>,
) -> Result<Vec<u8>, CoordinatorLogError> {
    let schema = match &record {
        CoordinatorRecord::SchemaCommit(_, reference) => Some(*reference),
        _ => None,
    };
    let (tag, participants) = match record {
        CoordinatorRecord::CommitDecision(participants) => (
            COMMIT_DECISION_TAG,
            canonical_participants(database_txn_id, participants, false)?,
        ),
        CoordinatorRecord::SchemaCommit(participants, reference) => {
            if reference.incarnation == [0; 16] || reference.target_epoch == 0 {
                return Err(CoordinatorLogError::InvalidReservedBytes { offset: 0 });
            }
            (
                SCHEMA_COMMIT_TAG,
                canonical_participants(database_txn_id, participants, true)?,
            )
        }
        CoordinatorRecord::Complete => (COMPLETE_TAG, Vec::new()),
    };
    let total_len = (RECORD_HEADER_SIZE
        + if schema.is_some() {
            SCHEMA_REFERENCE_SIZE
        } else {
            0
        })
    .checked_add(
        participants
            .len()
            .checked_mul(PARTICIPANT_SIZE)
            .ok_or(CoordinatorLogError::RecordSizeOverflow)?,
    )
    .ok_or(CoordinatorLogError::RecordSizeOverflow)?;
    let total_len_u32 =
        u32::try_from(total_len).map_err(|_| CoordinatorLogError::RecordSizeOverflow)?;
    let participant_count = u32::try_from(participants.len())
        .map_err(|_| CoordinatorLogError::ParticipantCountOverflow { offset: 0 })?;
    let mut bytes = Vec::with_capacity(total_len);
    bytes.extend_from_slice(RECORD_MAGIC);
    bytes.extend_from_slice(
        &(if schema.is_some() {
            2_u16
        } else {
            RECORD_VERSION
        })
        .to_le_bytes(),
    );
    bytes.push(tag);
    bytes.push(0);
    bytes.extend_from_slice(&total_len_u32.to_le_bytes());
    bytes.extend_from_slice(&0_u32.to_le_bytes());
    bytes.extend_from_slice(&database_txn_id.0.to_le_bytes());
    bytes.extend_from_slice(&participant_count.to_le_bytes());
    bytes.extend_from_slice(&0_u32.to_le_bytes());
    for participant in participants {
        bytes.extend_from_slice(&participant.storage_id.0.to_le_bytes());
        bytes.extend_from_slice(&participant.physical_txn_id.0.to_le_bytes());
    }
    if let Some(reference) = schema {
        bytes.extend_from_slice(&reference.incarnation);
        bytes.extend_from_slice(&reference.target_epoch.to_le_bytes());
        bytes.extend_from_slice(&reference.digest);
    }
    let checksum = crc32c::crc32c(&bytes);
    bytes[RECORD_CHECKSUM_OFFSET..RECORD_CHECKSUM_OFFSET + 4]
        .copy_from_slice(&checksum.to_le_bytes());
    Ok(bytes)
}

fn validate_partial_header(bytes: &[u8], offset: u64) -> Result<(), CoordinatorLogError> {
    let magic_len = bytes.len().min(RECORD_MAGIC.len());
    if bytes[..magic_len] != RECORD_MAGIC[..magic_len] {
        return Err(CoordinatorLogError::InvalidRecordMagic { offset });
    }
    if bytes.len() >= 6 && !matches!(read_u16(bytes, 4), 1 | 2) {
        return Err(CoordinatorLogError::UnsupportedRecordVersion {
            offset,
            version: read_u16(bytes, 4),
        });
    }
    if bytes.len() >= 7
        && !matches!(
            (read_u16(bytes, 4), bytes[6]),
            (1, COMMIT_DECISION_TAG | COMPLETE_TAG) | (2, SCHEMA_COMMIT_TAG)
        )
    {
        return Err(CoordinatorLogError::UnknownRecordTag {
            offset,
            tag: bytes[6],
        });
    }
    if bytes.len() >= 8 && bytes[7] != 0 {
        return Err(CoordinatorLogError::InvalidReservedBytes { offset });
    }
    if bytes.len() >= 12 {
        let length = usize::try_from(read_u32(bytes, 8))
            .map_err(|_| CoordinatorLogError::RecordSizeOverflow)?;
        if !(RECORD_HEADER_SIZE..=MAX_RECORD_SIZE).contains(&length) {
            return Err(CoordinatorLogError::InvalidRecordLength { offset, length });
        }
    }
    Ok(())
}

fn validate_record_header(
    bytes: &[u8; RECORD_HEADER_SIZE],
    offset: u64,
) -> Result<u32, CoordinatorLogError> {
    validate_partial_header(bytes, offset)?;
    let tag = bytes[6];
    let total_len =
        usize::try_from(read_u32(bytes, 8)).map_err(|_| CoordinatorLogError::RecordSizeOverflow)?;
    let count = usize::try_from(read_u32(bytes, 24))
        .map_err(|_| CoordinatorLogError::ParticipantCountOverflow { offset })?;
    if count > MAX_COORDINATOR_PARTICIPANTS {
        return Err(CoordinatorLogError::InvalidParticipantCount { offset, count });
    }
    let expected = match tag {
        COMMIT_DECISION_TAG | SCHEMA_COMMIT_TAG => (RECORD_HEADER_SIZE
            + if tag == SCHEMA_COMMIT_TAG {
                SCHEMA_REFERENCE_SIZE
            } else {
                0
            })
        .checked_add(
            count
                .checked_mul(PARTICIPANT_SIZE)
                .ok_or(CoordinatorLogError::RecordSizeOverflow)?,
        ),
        COMPLETE_TAG => Some(RECORD_HEADER_SIZE),
        _ => None,
    }
    .ok_or(CoordinatorLogError::RecordSizeOverflow)?;
    if total_len != expected {
        return Err(CoordinatorLogError::InvalidRecordLength {
            offset,
            length: total_len,
        });
    }
    u32::try_from(total_len).map_err(|_| CoordinatorLogError::RecordSizeOverflow)
}

fn verify_record_checksum(bytes: &[u8], offset: u64) -> Result<(), CoordinatorLogError> {
    let stored = read_u32(bytes, RECORD_CHECKSUM_OFFSET);
    let mut checked = bytes.to_vec();
    checked[RECORD_CHECKSUM_OFFSET..RECORD_CHECKSUM_OFFSET + 4].fill(0);
    let computed = crc32c::crc32c(&checked);
    if stored != computed {
        return Err(CoordinatorLogError::RecordChecksumMismatch {
            offset,
            stored,
            computed,
        });
    }
    Ok(())
}

fn append_record(file: &mut File, bytes: &[u8]) -> Result<(), CoordinatorLogError> {
    let offset = file.seek(SeekFrom::End(0))?;
    if let Err(error) = file.write_all(bytes) {
        let _ = file.set_len(offset);
        return Err(error.into());
    }
    Ok(())
}

#[cfg(test)]
fn inject_partial_append_failure(file: &mut File, bytes: &[u8]) -> Result<(), CoordinatorLogError> {
    let offset = file.seek(SeekFrom::End(0))?;
    file.write_all(&bytes[..bytes.len() / 2])?;
    file.set_len(offset)?;
    file.seek(SeekFrom::Start(offset))?;
    Err(injected_io_error("partial append").into())
}

#[cfg(test)]
fn injected_io_error(operation: &str) -> std::io::Error {
    std::io::Error::other(format!("injected coordinator {operation} failure"))
}

fn sync_parent_directory(path: &Path) -> Result<(), CoordinatorLogError> {
    File::open(path.parent().unwrap_or_else(|| Path::new(".")))?.sync_all()?;
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

#[derive(Debug)]
pub enum CoordinatorLogError {
    Io(std::io::Error),
    InvalidMagic,
    UnsupportedVersion(u16),
    InvalidHeaderSize(u16),
    InvalidHeaderReserved,
    HeaderChecksumMismatch {
        stored: u32,
        computed: u32,
    },
    TruncatedHeader,
    TruncatedRecord {
        offset: u64,
    },
    InvalidRecordMagic {
        offset: u64,
    },
    UnsupportedRecordVersion {
        offset: u64,
        version: u16,
    },
    UnknownRecordTag {
        offset: u64,
        tag: u8,
    },
    InvalidRecordLength {
        offset: u64,
        length: usize,
    },
    InvalidReservedBytes {
        offset: u64,
    },
    RecordChecksumMismatch {
        offset: u64,
        stored: u32,
        computed: u32,
    },
    InvalidTransactionId {
        offset: u64,
    },
    TransactionIdExhausted,
    ParticipantCountOverflow {
        offset: u64,
    },
    InvalidParticipantCount {
        offset: u64,
        count: usize,
    },
    InvalidParticipantIdentity {
        database_txn_id: DatabaseTxnId,
        storage_id: StorageId,
        physical_txn_id: TxnId,
    },
    DuplicateStorageId {
        database_txn_id: DatabaseTxnId,
        storage_id: StorageId,
    },
    ConflictingDecision {
        database_txn_id: DatabaseTxnId,
    },
    CompleteWithoutDecision {
        database_txn_id: DatabaseTxnId,
    },
    InvalidCompleteLength {
        offset: u64,
    },
    RecordSizeOverflow,
}

impl fmt::Display for CoordinatorLogError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "coordinator log I/O error: {error}"),
            Self::InvalidMagic => formatter.write_str("coordinator log magic does not match"),
            Self::UnsupportedVersion(version) => {
                write!(formatter, "unsupported coordinator log version {version}")
            }
            Self::InvalidHeaderSize(size) => {
                write!(formatter, "invalid coordinator log header size {size}")
            }
            Self::InvalidHeaderReserved => {
                formatter.write_str("coordinator log header reserved bytes are non-zero")
            }
            Self::HeaderChecksumMismatch { stored, computed } => write!(
                formatter,
                "coordinator log header checksum mismatch: stored {stored:#010x}, computed {computed:#010x}"
            ),
            Self::TruncatedHeader => formatter.write_str("coordinator log header is truncated"),
            Self::TruncatedRecord { offset } => {
                write!(formatter, "coordinator record at {offset} is truncated")
            }
            Self::InvalidRecordMagic { offset } => write!(
                formatter,
                "coordinator record at {offset} has invalid magic"
            ),
            Self::UnsupportedRecordVersion { offset, version } => write!(
                formatter,
                "coordinator record at {offset} has unsupported version {version}"
            ),
            Self::UnknownRecordTag { offset, tag } => write!(
                formatter,
                "coordinator record at {offset} has unknown tag {tag}"
            ),
            Self::InvalidRecordLength { offset, length } => write!(
                formatter,
                "coordinator record at {offset} has invalid length {length}"
            ),
            Self::InvalidReservedBytes { offset } => write!(
                formatter,
                "coordinator record at {offset} has non-zero reserved bytes"
            ),
            Self::RecordChecksumMismatch {
                offset,
                stored,
                computed,
            } => write!(
                formatter,
                "coordinator record at {offset} has checksum {stored:#010x}, computed {computed:#010x}"
            ),
            Self::InvalidTransactionId { offset } => write!(
                formatter,
                "coordinator record at {offset} has transaction ID zero"
            ),
            Self::TransactionIdExhausted => {
                formatter.write_str("database transaction identity space is exhausted")
            }
            Self::ParticipantCountOverflow { offset } => write!(
                formatter,
                "coordinator participant count at {offset} does not fit memory size"
            ),
            Self::InvalidParticipantCount { offset, count } => write!(
                formatter,
                "coordinator record at {offset} has invalid participant count {count}"
            ),
            Self::InvalidParticipantIdentity {
                database_txn_id,
                storage_id,
                physical_txn_id,
            } => write!(
                formatter,
                "database transaction {} has invalid participant storage {} physical transaction {}",
                database_txn_id.0, storage_id.0, physical_txn_id.0
            ),
            Self::DuplicateStorageId {
                database_txn_id,
                storage_id,
            } => write!(
                formatter,
                "database transaction {} repeats storage participant {}",
                database_txn_id.0, storage_id.0
            ),
            Self::ConflictingDecision { database_txn_id } => write!(
                formatter,
                "database transaction {} has conflicting commit decisions",
                database_txn_id.0
            ),
            Self::CompleteWithoutDecision { database_txn_id } => write!(
                formatter,
                "database transaction {} completes without a commit decision",
                database_txn_id.0
            ),
            Self::InvalidCompleteLength { offset } => write!(
                formatter,
                "Complete record at {offset} has an invalid payload"
            ),
            Self::RecordSizeOverflow => formatter.write_str("coordinator record size overflows"),
        }
    }
}

impl Error for CoordinatorLogError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<std::io::Error> for CoordinatorLogError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Seek, SeekFrom, Write};

    use super::{
        CoordinatorLog, CoordinatorLogError, CoordinatorParticipant, CoordinatorRecord,
        LOG_HEADER_SIZE, MAX_COORDINATOR_PARTICIPANTS, RECORD_CHECKSUM_OFFSET, RECORD_HEADER_SIZE,
        encode_record,
    };
    use netbadb_types::{DatabaseTxnId, StorageId, TxnId};

    fn path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "netbadb-coordinator-{name}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ))
    }

    fn participants() -> [CoordinatorParticipant; 2] {
        [
            CoordinatorParticipant {
                storage_id: StorageId(9),
                physical_txn_id: TxnId(90),
            },
            CoordinatorParticipant {
                storage_id: StorageId(3),
                physical_txn_id: TxnId(30),
            },
        ]
    }

    fn rewrite_record_checksum(bytes: &mut [u8], offset: usize, length: usize) {
        let record = &mut bytes[offset..offset + length];
        record[RECORD_CHECKSUM_OFFSET..RECORD_CHECKSUM_OFFSET + 4].fill(0);
        let checksum = crc32c::crc32c(record);
        record[RECORD_CHECKSUM_OFFSET..RECORD_CHECKSUM_OFFSET + 4]
            .copy_from_slice(&checksum.to_le_bytes());
    }

    #[test]
    fn commit_decision_and_complete_round_trip_canonically_and_idempotently() {
        let path = path("round-trip");
        let _ = std::fs::remove_file(&path);
        let mut log = CoordinatorLog::create(&path).expect("create coordinator log");
        log.commit_decision(DatabaseTxnId(7), &participants())
            .expect("write decision");
        log.commit_decision(DatabaseTxnId(7), &participants())
            .expect("retry same decision");
        log.complete(DatabaseTxnId(7)).expect("write complete");
        log.complete(DatabaseTxnId(7)).expect("retry complete");
        assert!(matches!(
            log.commit_decision(
                DatabaseTxnId(7),
                &[CoordinatorParticipant {
                    storage_id: StorageId(3),
                    physical_txn_id: TxnId(31),
                }]
            ),
            Err(CoordinatorLogError::ConflictingDecision { .. })
        ));
        drop(log);

        let reopened = CoordinatorLog::open(&path).expect("reopen coordinator log");
        let decisions = reopened.decisions().collect::<Vec<_>>();
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].database_txn_id, DatabaseTxnId(7));
        assert_eq!(decisions[0].participants[0].storage_id, StorageId(3));
        assert_eq!(decisions[0].participants[1].storage_id, StorageId(9));
        assert!(decisions[0].complete);
        let bytes = std::fs::read(&path).expect("read coordinator bytes");
        assert_eq!(&bytes[0..4], b"NBCO");
        assert_eq!(&bytes[LOG_HEADER_SIZE..LOG_HEADER_SIZE + 4], b"CORD");
        assert_eq!(bytes[LOG_HEADER_SIZE + 6], 1);
        assert_eq!(
            u32::from_le_bytes(
                bytes[LOG_HEADER_SIZE + RECORD_CHECKSUM_OFFSET
                    ..LOG_HEADER_SIZE + RECORD_CHECKSUM_OFFSET + 4]
                    .try_into()
                    .expect("checksum bytes")
            ),
            0x70de_9acb
        );
        drop(reopened);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn corruption_and_invalid_coordinator_sequences_are_hard_errors() {
        for case in [
            "bad-magic",
            "bad-version",
            "bad-checksum",
            "bad-tag",
            "oversized-count",
            "duplicate-storage",
            "truncated-array-middle",
            "middle-corruption",
            "invalid-tail",
        ] {
            let path = path(case);
            let _ = std::fs::remove_file(&path);
            let mut log = CoordinatorLog::create(&path).expect("create baseline log");
            log.commit_decision(DatabaseTxnId(8), &participants())
                .expect("write baseline decision");
            log.complete(DatabaseTxnId(8))
                .expect("write baseline complete");
            drop(log);
            let mut bytes = std::fs::read(&path).expect("read baseline");
            let decision_len = RECORD_HEADER_SIZE + 2 * 16;
            match case {
                "bad-magic" => bytes[0] ^= 1,
                "bad-version" => bytes[4..6].copy_from_slice(&99_u16.to_le_bytes()),
                "bad-checksum" => bytes[12] ^= 1,
                "bad-tag" => {
                    bytes[LOG_HEADER_SIZE + 6] = 99;
                    rewrite_record_checksum(&mut bytes, LOG_HEADER_SIZE, decision_len);
                }
                "oversized-count" => bytes[LOG_HEADER_SIZE + 24..LOG_HEADER_SIZE + 28]
                    .copy_from_slice(
                        &u32::try_from(MAX_COORDINATOR_PARTICIPANTS + 1)
                            .expect("bounded count")
                            .to_le_bytes(),
                    ),
                "duplicate-storage" => {
                    let first_storage = bytes[LOG_HEADER_SIZE + RECORD_HEADER_SIZE
                        ..LOG_HEADER_SIZE + RECORD_HEADER_SIZE + 8]
                        .to_vec();
                    bytes[LOG_HEADER_SIZE + RECORD_HEADER_SIZE + 16
                        ..LOG_HEADER_SIZE + RECORD_HEADER_SIZE + 24]
                        .copy_from_slice(&first_storage);
                    rewrite_record_checksum(&mut bytes, LOG_HEADER_SIZE, decision_len);
                }
                "truncated-array-middle" => {
                    let start = LOG_HEADER_SIZE + decision_len - 8;
                    bytes.drain(start..start + 8);
                }
                "middle-corruption" => bytes[LOG_HEADER_SIZE + RECORD_HEADER_SIZE + 8] ^= 1,
                "invalid-tail" => bytes.extend_from_slice(b"BAD"),
                _ => unreachable!(),
            }
            std::fs::write(&path, bytes).expect("write corruption");
            assert!(CoordinatorLog::open(&path).is_err(), "case {case}");
            let _ = std::fs::remove_file(path);
        }

        let conflict_path = path("conflicting-decision");
        let _ = std::fs::remove_file(&conflict_path);
        let mut log = CoordinatorLog::create(&conflict_path).expect("create conflict log");
        log.commit_decision(DatabaseTxnId(8), &participants())
            .expect("write first decision");
        drop(log);
        let conflict = encode_record(
            DatabaseTxnId(8),
            CoordinatorRecord::CommitDecision(&[CoordinatorParticipant {
                storage_id: StorageId(3),
                physical_txn_id: TxnId(31),
            }]),
        )
        .expect("encode conflicting decision");
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&conflict_path)
            .expect("open conflict log");
        file.write_all(&conflict).expect("append conflict");
        file.sync_data().expect("sync conflict");
        drop(file);
        assert!(matches!(
            CoordinatorLog::open(&conflict_path),
            Err(CoordinatorLogError::ConflictingDecision { .. })
        ));
        let _ = std::fs::remove_file(conflict_path);

        for case in ["complete-without-decision", "mismatched-complete"] {
            let path = path(case);
            let _ = std::fs::remove_file(&path);
            let mut log = CoordinatorLog::create(&path).expect("create sequence log");
            if case == "mismatched-complete" {
                log.commit_decision(DatabaseTxnId(1), &participants())
                    .expect("write different decision");
            }
            drop(log);
            let transaction_id = if case == "complete-without-decision" {
                1
            } else {
                2
            };
            let complete =
                encode_record(DatabaseTxnId(transaction_id), CoordinatorRecord::Complete)
                    .expect("encode complete");
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .open(&path)
                .expect("open sequence log");
            file.seek(SeekFrom::End(0)).expect("seek end");
            file.write_all(&complete).expect("append complete");
            file.sync_data().expect("sync complete");
            drop(file);
            assert!(matches!(
                CoordinatorLog::open(&path),
                Err(CoordinatorLogError::CompleteWithoutDecision { .. })
            ));
            let _ = std::fs::remove_file(path);
        }
    }

    #[test]
    fn structurally_valid_incomplete_final_record_is_truncated_once() {
        let path = path("incomplete-tail");
        let _ = std::fs::remove_file(&path);
        let mut log = CoordinatorLog::create(&path).expect("create tail log");
        log.commit_decision(DatabaseTxnId(11), &participants())
            .expect("write decision");
        drop(log);
        let complete =
            encode_record(DatabaseTxnId(11), CoordinatorRecord::Complete).expect("encode complete");
        let valid_length = std::fs::metadata(&path).expect("metadata").len();
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .expect("open tail");
        file.write_all(&complete[..complete.len() / 2])
            .expect("write partial tail");
        drop(file);
        let reopened = CoordinatorLog::open(&path).expect("truncate crash tail");
        assert!(!reopened.decisions().next().expect("decision").complete);
        drop(reopened);
        assert_eq!(
            std::fs::metadata(&path).expect("truncated metadata").len(),
            valid_length
        );
        CoordinatorLog::open(&path).expect("second open is stable");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn append_and_sync_failures_retry_the_same_decision_and_complete() {
        let path = path("retry-faults");
        let _ = std::fs::remove_file(&path);
        let mut log = CoordinatorLog::create(&path).expect("create retry log");

        log.inject_decision_append_failure();
        assert!(
            log.commit_decision(DatabaseTxnId(17), &participants())
                .is_err()
        );
        assert_eq!(log.decisions().count(), 0);
        log.inject_decision_sync_failure();
        assert!(
            log.commit_decision(DatabaseTxnId(17), &participants())
                .is_err()
        );
        assert_eq!(log.decisions().count(), 1);
        log.commit_decision(DatabaseTxnId(17), &participants())
            .expect("retry decision sync");

        log.inject_complete_append_failure();
        assert!(log.complete(DatabaseTxnId(17)).is_err());
        assert!(!log.decisions().next().expect("decision").complete);
        log.inject_complete_sync_failure();
        assert!(log.complete(DatabaseTxnId(17)).is_err());
        assert!(log.decisions().next().expect("decision").complete);
        log.complete(DatabaseTxnId(17))
            .expect("retry Complete sync");
        drop(log);

        let reopened = CoordinatorLog::open(&path).expect("reopen retry log");
        let decisions = reopened.decisions().collect::<Vec<_>>();
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].database_txn_id, DatabaseTxnId(17));
        assert!(decisions[0].complete);
        drop(reopened);
        let _ = std::fs::remove_file(path);
    }
}
