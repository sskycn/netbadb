use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use netbadb_types::{DatabaseCommitSeq, DatabaseTxnId, StorageId, TxnId};

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
const GLOBAL_ENABLE_TAG: u8 = 4;
const SEQUENCED_COMMIT_TAG: u8 = 5;
const SEQUENCED_SCHEMA_COMMIT_TAG: u8 = 6;
const SEQUENCED_COMPLETE_TAG: u8 = 7;
const SEQUENCED_PREFIX_SIZE: usize = 8;
const SCHEMA_REFERENCE_SIZE: usize = 56;
pub(crate) const MAX_COORDINATOR_PARTICIPANTS: usize = 1_024;
const PARTICIPANT_SIZE: usize = 16;
const MAX_RECORD_SIZE: usize = RECORD_HEADER_SIZE
    + SEQUENCED_PREFIX_SIZE
    + MAX_COORDINATOR_PARTICIPANTS * PARTICIPANT_SIZE
    + SCHEMA_REFERENCE_SIZE;

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
    pub(crate) commit_seq: Option<DatabaseCommitSeq>,
    pub(crate) participants: Vec<CoordinatorParticipant>,
    pub(crate) complete: bool,
    pub(crate) schema: Option<SchemaParticipantReference>,
}

#[derive(Debug)]
pub(crate) struct CoordinatorLog {
    file: File,
    decisions: BTreeMap<DatabaseTxnId, CoordinatorDecision>,
    global_visibility: bool,
    pending_complete_checkpoints: BTreeSet<DatabaseCommitSeq>,
    last_appended_complete: DatabaseCommitSeq,
    last_synced_complete: DatabaseCommitSeq,
    last_checkpoint_error: Option<String>,
    decision_sync_count: u64,
    checkpoint_sync_count: u64,
    combined_pipeline_sync_count: u64,
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
            global_visibility: false,
            pending_complete_checkpoints: BTreeSet::new(),
            last_appended_complete: DatabaseCommitSeq(0),
            last_synced_complete: DatabaseCommitSeq(0),
            last_checkpoint_error: None,
            decision_sync_count: 0,
            checkpoint_sync_count: 0,
            combined_pipeline_sync_count: 0,
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
        }
        // Establish one durable baseline for inspection even when an
        // unsynchronized but complete final checkpoint survived a process
        // crash. Startup synchronization is not a foreground commit counter.
        file.sync_data()?;
        let last_complete = scan
            .decisions
            .values()
            .filter(|decision| decision.complete)
            .filter_map(|decision| decision.commit_seq)
            .max()
            .unwrap_or(DatabaseCommitSeq(0));
        Ok(Self {
            file,
            decisions: scan.decisions,
            global_visibility: scan.global_visibility,
            pending_complete_checkpoints: BTreeSet::new(),
            last_appended_complete: last_complete,
            last_synced_complete: last_complete,
            last_checkpoint_error: None,
            decision_sync_count: 0,
            checkpoint_sync_count: 0,
            combined_pipeline_sync_count: 0,
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

    pub(crate) const fn global_visibility_enabled(&self) -> bool {
        self.global_visibility
    }

    pub(crate) fn published_commit_seq(&self) -> DatabaseCommitSeq {
        self.decisions
            .values()
            .filter(|decision| decision.complete)
            .filter_map(|decision| decision.commit_seq)
            .max()
            .unwrap_or(DatabaseCommitSeq(0))
    }

    pub(crate) fn next_commit_seq(&self) -> Result<DatabaseCommitSeq, CoordinatorLogError> {
        let last = self
            .decisions
            .values()
            .filter_map(|decision| decision.commit_seq)
            .max()
            .unwrap_or(DatabaseCommitSeq(0));
        last.0
            .checked_add(1)
            .map(DatabaseCommitSeq)
            .ok_or(CoordinatorLogError::CommitSequenceExhausted)
    }

    pub(crate) fn last_sequenced_decision(&self) -> DatabaseCommitSeq {
        self.decisions
            .values()
            .filter_map(|decision| decision.commit_seq)
            .max()
            .unwrap_or(DatabaseCommitSeq(0))
    }

    pub(crate) const fn last_appended_complete(&self) -> DatabaseCommitSeq {
        self.last_appended_complete
    }

    pub(crate) const fn last_synced_complete(&self) -> DatabaseCommitSeq {
        self.last_synced_complete
    }

    pub(crate) fn pending_complete_count(&self) -> usize {
        self.pending_complete_checkpoints.len()
    }

    pub(crate) fn last_checkpoint_error(&self) -> Option<&str> {
        self.last_checkpoint_error.as_deref()
    }

    pub(crate) const fn decision_sync_count(&self) -> u64 {
        self.decision_sync_count
    }

    pub(crate) const fn checkpoint_sync_count(&self) -> u64 {
        self.checkpoint_sync_count
    }

    pub(crate) const fn combined_pipeline_sync_count(&self) -> u64 {
        self.combined_pipeline_sync_count
    }

    pub(crate) fn byte_len(&self) -> Result<u64, CoordinatorLogError> {
        Ok(self.file.metadata()?.len())
    }

    /// Durably and irreversibly enables database-global snapshot publication.
    pub(crate) fn enable_global_visibility(&mut self) -> Result<(), CoordinatorLogError> {
        if self.global_visibility {
            self.file.sync_data()?;
            return Ok(());
        }
        if self.decisions.values().any(|decision| !decision.complete) {
            return Err(CoordinatorLogError::GlobalEnableWithIncompleteDecision);
        }
        let bytes = encode_record(DatabaseTxnId(0), CoordinatorRecord::GlobalEnable)?;
        append_record(&mut self.file, &bytes)?;
        self.file.sync_data()?;
        self.global_visibility = true;
        Ok(())
    }

    pub(crate) fn sequenced_commit_decision(
        &mut self,
        database_txn_id: DatabaseTxnId,
        participants: &[CoordinatorParticipant],
        schema: Option<&SchemaParticipantReference>,
    ) -> Result<DatabaseCommitSeq, CoordinatorLogError> {
        if !self.global_visibility {
            return Err(CoordinatorLogError::GlobalVisibilityNotEnabled);
        }
        let participants = canonical_participants(database_txn_id, participants, schema.is_some())?;
        let combined_checkpoint = !self.pending_complete_checkpoints.is_empty();
        self.append_missing_pending_completes()?;
        if let Some(existing) = self.decisions.get(&database_txn_id) {
            if existing.participants != participants || existing.schema.as_ref() != schema {
                return Err(CoordinatorLogError::ConflictingDecision { database_txn_id });
            }
            let sequence = existing
                .commit_seq
                .ok_or(CoordinatorLogError::ConflictingDecision { database_txn_id })?;
            self.sync_decision(combined_checkpoint)?;
            return Ok(sequence);
        }
        let commit_seq = self.next_commit_seq()?;
        let record = match schema {
            Some(reference) => {
                CoordinatorRecord::SequencedSchemaCommit(commit_seq, &participants, reference)
            }
            None => CoordinatorRecord::SequencedCommit(commit_seq, &participants),
        };
        let bytes = encode_record(database_txn_id, record)?;
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
                commit_seq: Some(commit_seq),
                participants,
                complete: false,
                schema: schema.cloned(),
            },
        );
        #[cfg(test)]
        if std::mem::take(&mut self.fail_next_decision_sync) {
            return Err(injected_io_error("sequenced decision sync").into());
        }
        self.sync_decision(combined_checkpoint)?;
        Ok(commit_seq)
    }

    /// Appends a data transaction's recovery checkpoint without synchronizing
    /// it. The already durable decision and participant commits remain the
    /// commit proof; a later Decision sync or explicit checkpoint makes this
    /// Complete durable.
    pub(crate) fn defer_complete_sequenced(
        &mut self,
        database_txn_id: DatabaseTxnId,
        commit_seq: DatabaseCommitSeq,
    ) -> Result<(), CoordinatorLogError> {
        self.validate_sequenced_complete(database_txn_id, commit_seq)?;
        self.pending_complete_checkpoints.insert(commit_seq);
        if self
            .decisions
            .get(&database_txn_id)
            .is_some_and(|decision| decision.complete)
        {
            return Ok(());
        }
        if let Err(error) = self.append_sequenced_complete(database_txn_id, commit_seq) {
            self.last_checkpoint_error = Some(error.to_string());
            return Ok(());
        }
        self.last_checkpoint_error = None;
        Ok(())
    }

    pub(crate) fn complete_sequenced(
        &mut self,
        database_txn_id: DatabaseTxnId,
        commit_seq: DatabaseCommitSeq,
    ) -> Result<(), CoordinatorLogError> {
        self.validate_sequenced_complete(database_txn_id, commit_seq)?;
        if self
            .decisions
            .get(&database_txn_id)
            .is_some_and(|decision| decision.complete)
        {
            if let Err(error) = self.file.sync_data() {
                let error = CoordinatorLogError::from(error);
                self.last_checkpoint_error = Some(error.to_string());
                return Err(error);
            }
            self.checkpoint_sync_count = self.checkpoint_sync_count.saturating_add(1);
            self.mark_pending_completes_synced();
            self.last_checkpoint_error = None;
            return Ok(());
        }
        self.append_sequenced_complete(database_txn_id, commit_seq)?;
        self.pending_complete_checkpoints.insert(commit_seq);
        #[cfg(test)]
        if std::mem::take(&mut self.fail_next_complete_sync) {
            let error: CoordinatorLogError = injected_io_error("sequenced Complete sync").into();
            self.last_checkpoint_error = Some(error.to_string());
            return Err(error);
        }
        if let Err(error) = self.file.sync_data() {
            let error = CoordinatorLogError::from(error);
            self.last_checkpoint_error = Some(error.to_string());
            return Err(error);
        }
        self.checkpoint_sync_count = self.checkpoint_sync_count.saturating_add(1);
        self.mark_pending_completes_synced();
        self.last_checkpoint_error = None;
        Ok(())
    }

    pub(crate) fn flush_complete_checkpoints(&mut self) -> Result<(), CoordinatorLogError> {
        if self.pending_complete_checkpoints.is_empty() {
            return Ok(());
        }
        self.append_missing_pending_completes()?;
        #[cfg(test)]
        if std::mem::take(&mut self.fail_next_complete_sync) {
            let error: CoordinatorLogError = injected_io_error("sequenced Complete sync").into();
            self.last_checkpoint_error = Some(error.to_string());
            return Err(error);
        }
        if let Err(error) = self.file.sync_data() {
            let error = CoordinatorLogError::from(error);
            self.last_checkpoint_error = Some(error.to_string());
            return Err(error);
        }
        self.checkpoint_sync_count = self.checkpoint_sync_count.saturating_add(1);
        self.mark_pending_completes_synced();
        self.last_checkpoint_error = None;
        Ok(())
    }

    fn validate_sequenced_complete(
        &self,
        database_txn_id: DatabaseTxnId,
        commit_seq: DatabaseCommitSeq,
    ) -> Result<(), CoordinatorLogError> {
        let decision = self
            .decisions
            .get(&database_txn_id)
            .ok_or(CoordinatorLogError::CompleteWithoutDecision { database_txn_id })?;
        if decision.commit_seq != Some(commit_seq) {
            return Err(CoordinatorLogError::CompleteSequenceMismatch {
                database_txn_id,
                expected: decision.commit_seq,
                actual: commit_seq,
            });
        }
        if !decision.complete
            && self.decisions.values().any(|other| {
                other
                    .commit_seq
                    .is_some_and(|sequence| sequence < commit_seq)
                    && !other.complete
            })
        {
            return Err(CoordinatorLogError::CommitSequenceGap { commit_seq });
        }
        Ok(())
    }

    fn append_sequenced_complete(
        &mut self,
        database_txn_id: DatabaseTxnId,
        commit_seq: DatabaseCommitSeq,
    ) -> Result<(), CoordinatorLogError> {
        let bytes = encode_record(
            database_txn_id,
            CoordinatorRecord::SequencedComplete(commit_seq),
        )?;
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
        self.last_appended_complete = self.last_appended_complete.max(commit_seq);
        Ok(())
    }

    fn append_missing_pending_completes(&mut self) -> Result<(), CoordinatorLogError> {
        let missing = self
            .pending_complete_checkpoints
            .iter()
            .filter_map(|commit_seq| {
                self.decisions
                    .values()
                    .find(|decision| decision.commit_seq == Some(*commit_seq) && !decision.complete)
                    .map(|decision| (decision.database_txn_id, *commit_seq))
            })
            .collect::<Vec<_>>();
        for (database_txn_id, commit_seq) in missing {
            if let Err(error) = self.append_sequenced_complete(database_txn_id, commit_seq) {
                self.last_checkpoint_error = Some(error.to_string());
                return Err(error);
            }
        }
        Ok(())
    }

    fn sync_decision(&mut self, combined_checkpoint: bool) -> Result<(), CoordinatorLogError> {
        if let Err(error) = self.file.sync_data() {
            let error = CoordinatorLogError::from(error);
            if combined_checkpoint {
                self.last_checkpoint_error = Some(error.to_string());
            }
            return Err(error);
        }
        self.decision_sync_count = self.decision_sync_count.saturating_add(1);
        if combined_checkpoint {
            self.combined_pipeline_sync_count = self.combined_pipeline_sync_count.saturating_add(1);
            self.mark_pending_completes_synced();
        }
        self.last_checkpoint_error = None;
        Ok(())
    }

    fn mark_pending_completes_synced(&mut self) {
        if let Some(last) = self
            .pending_complete_checkpoints
            .iter()
            .next_back()
            .copied()
        {
            self.last_synced_complete = self.last_synced_complete.max(last);
        }
        self.pending_complete_checkpoints.clear();
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
                commit_seq: None,
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
    GlobalEnable,
    SequencedCommit(DatabaseCommitSeq, &'a [CoordinatorParticipant]),
    SequencedSchemaCommit(
        DatabaseCommitSeq,
        &'a [CoordinatorParticipant],
        &'a SchemaParticipantReference,
    ),
    SequencedComplete(DatabaseCommitSeq),
}

#[derive(Debug)]
struct CoordinatorScan {
    decisions: BTreeMap<DatabaseTxnId, CoordinatorDecision>,
    global_visibility: bool,
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
    let mut global_visibility = false;
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
                    global_visibility,
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
                    global_visibility,
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
        apply_decoded_record(&bytes, offset, &mut decisions, &mut global_visibility)?;
        offset = offset
            .checked_add(u64::from(total_len))
            .ok_or(CoordinatorLogError::RecordSizeOverflow)?;
    }
    Ok(CoordinatorScan {
        decisions,
        global_visibility,
        valid_end: offset,
        incomplete_tail: false,
    })
}

fn apply_decoded_record(
    bytes: &[u8],
    offset: u64,
    decisions: &mut BTreeMap<DatabaseTxnId, CoordinatorDecision>,
    global_visibility: &mut bool,
) -> Result<(), CoordinatorLogError> {
    let database_txn_id = DatabaseTxnId(read_u64(bytes, 16));
    let tag = bytes[6];
    if database_txn_id.0 == 0 && tag != GLOBAL_ENABLE_TAG {
        return Err(CoordinatorLogError::InvalidTransactionId { offset });
    }
    let participant_count = usize::try_from(read_u32(bytes, 24))
        .map_err(|_| CoordinatorLogError::ParticipantCountOverflow { offset })?;
    if bytes[28..32] != [0; 4] {
        return Err(CoordinatorLogError::InvalidReservedBytes { offset });
    }
    match tag {
        GLOBAL_ENABLE_TAG => {
            if database_txn_id.0 != 0 || participant_count != 0 || bytes.len() != RECORD_HEADER_SIZE
            {
                return Err(CoordinatorLogError::InvalidGlobalEnable { offset });
            }
            *global_visibility = true;
        }
        COMMIT_DECISION_TAG
        | SCHEMA_COMMIT_TAG
        | SEQUENCED_COMMIT_TAG
        | SEQUENCED_SCHEMA_COMMIT_TAG => {
            let sequenced = matches!(tag, SEQUENCED_COMMIT_TAG | SEQUENCED_SCHEMA_COMMIT_TAG);
            let schema_record = matches!(tag, SCHEMA_COMMIT_TAG | SEQUENCED_SCHEMA_COMMIT_TAG);
            if sequenced && !*global_visibility {
                return Err(CoordinatorLogError::SequencedRecordBeforeGlobalEnable { offset });
            }
            if (participant_count == 0 && !schema_record)
                || participant_count > MAX_COORDINATOR_PARTICIPANTS
            {
                return Err(CoordinatorLogError::InvalidParticipantCount {
                    offset,
                    count: participant_count,
                });
            }
            let mut participants = Vec::with_capacity(participant_count);
            for position in 0..participant_count {
                let base = (RECORD_HEADER_SIZE + if sequenced { SEQUENCED_PREFIX_SIZE } else { 0 })
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
            let participants =
                canonical_participants(database_txn_id, &participants, schema_record)?;
            let commit_seq = if sequenced {
                let sequence = DatabaseCommitSeq(read_u64(bytes, RECORD_HEADER_SIZE));
                if sequence.0 == 0 {
                    return Err(CoordinatorLogError::InvalidCommitSequence { offset });
                }
                let expected = decisions
                    .values()
                    .filter_map(|decision| decision.commit_seq)
                    .max()
                    .unwrap_or(DatabaseCommitSeq(0))
                    .0
                    .checked_add(1)
                    .ok_or(CoordinatorLogError::CommitSequenceExhausted)?;
                if sequence.0 != expected {
                    return Err(CoordinatorLogError::NonConsecutiveCommitSequence {
                        offset,
                        expected: DatabaseCommitSeq(expected),
                        actual: sequence,
                    });
                }
                Some(sequence)
            } else {
                None
            };
            let schema = if schema_record {
                let base = RECORD_HEADER_SIZE
                    + if sequenced { SEQUENCED_PREFIX_SIZE } else { 0 }
                    + participant_count * PARTICIPANT_SIZE;
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
                if existing.participants != participants
                    || existing.schema != schema
                    || existing.commit_seq != commit_seq
                {
                    return Err(CoordinatorLogError::ConflictingDecision { database_txn_id });
                }
            } else {
                decisions.insert(
                    database_txn_id,
                    CoordinatorDecision {
                        database_txn_id,
                        commit_seq,
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
        SEQUENCED_COMPLETE_TAG => {
            if !*global_visibility
                || participant_count != 0
                || bytes.len() != RECORD_HEADER_SIZE + SEQUENCED_PREFIX_SIZE
            {
                return Err(CoordinatorLogError::InvalidCompleteLength { offset });
            }
            let commit_seq = DatabaseCommitSeq(read_u64(bytes, RECORD_HEADER_SIZE));
            let decision = decisions
                .get_mut(&database_txn_id)
                .ok_or(CoordinatorLogError::CompleteWithoutDecision { database_txn_id })?;
            if decision.commit_seq != Some(commit_seq) {
                return Err(CoordinatorLogError::CompleteSequenceMismatch {
                    database_txn_id,
                    expected: decision.commit_seq,
                    actual: commit_seq,
                });
            }
            if decisions.values().any(|other| {
                other
                    .commit_seq
                    .is_some_and(|sequence| sequence < commit_seq)
                    && !other.complete
            }) {
                return Err(CoordinatorLogError::CommitSequenceGap { commit_seq });
            }
            decisions
                .get_mut(&database_txn_id)
                .ok_or(CoordinatorLogError::CompleteWithoutDecision { database_txn_id })?
                .complete = true;
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
        CoordinatorRecord::SchemaCommit(_, reference)
        | CoordinatorRecord::SequencedSchemaCommit(_, _, reference) => Some(*reference),
        _ => None,
    };
    let sequence = match &record {
        CoordinatorRecord::SequencedCommit(sequence, _)
        | CoordinatorRecord::SequencedSchemaCommit(sequence, _, _)
        | CoordinatorRecord::SequencedComplete(sequence) => Some(*sequence),
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
        CoordinatorRecord::GlobalEnable => (GLOBAL_ENABLE_TAG, Vec::new()),
        CoordinatorRecord::SequencedCommit(_, participants) => (
            SEQUENCED_COMMIT_TAG,
            canonical_participants(database_txn_id, participants, false)?,
        ),
        CoordinatorRecord::SequencedSchemaCommit(_, participants, reference) => {
            if reference.incarnation == [0; 16] || reference.target_epoch == 0 {
                return Err(CoordinatorLogError::InvalidReservedBytes { offset: 0 });
            }
            (
                SEQUENCED_SCHEMA_COMMIT_TAG,
                canonical_participants(database_txn_id, participants, true)?,
            )
        }
        CoordinatorRecord::SequencedComplete(_) => (SEQUENCED_COMPLETE_TAG, Vec::new()),
    };
    let total_len = (RECORD_HEADER_SIZE
        + if sequence.is_some() {
            SEQUENCED_PREFIX_SIZE
        } else {
            0
        }
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
        &(if sequence.is_some() || tag == GLOBAL_ENABLE_TAG {
            3_u16
        } else if schema.is_some() {
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
    if let Some(sequence) = sequence {
        if sequence.0 == 0 {
            return Err(CoordinatorLogError::InvalidCommitSequence { offset: 0 });
        }
        bytes.extend_from_slice(&sequence.0.to_le_bytes());
    }
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
    if bytes.len() >= 6 && !matches!(read_u16(bytes, 4), 1..=3) {
        return Err(CoordinatorLogError::UnsupportedRecordVersion {
            offset,
            version: read_u16(bytes, 4),
        });
    }
    if bytes.len() >= 7
        && !matches!(
            (read_u16(bytes, 4), bytes[6]),
            (1, COMMIT_DECISION_TAG | COMPLETE_TAG)
                | (2, SCHEMA_COMMIT_TAG)
                | (
                    3,
                    GLOBAL_ENABLE_TAG
                        | SEQUENCED_COMMIT_TAG
                        | SEQUENCED_SCHEMA_COMMIT_TAG
                        | SEQUENCED_COMPLETE_TAG
                )
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
        COMMIT_DECISION_TAG
        | SCHEMA_COMMIT_TAG
        | SEQUENCED_COMMIT_TAG
        | SEQUENCED_SCHEMA_COMMIT_TAG => (RECORD_HEADER_SIZE
            + if matches!(tag, SEQUENCED_COMMIT_TAG | SEQUENCED_SCHEMA_COMMIT_TAG) {
                SEQUENCED_PREFIX_SIZE
            } else {
                0
            }
            + if matches!(tag, SCHEMA_COMMIT_TAG | SEQUENCED_SCHEMA_COMMIT_TAG) {
                SCHEMA_REFERENCE_SIZE
            } else {
                0
            })
        .checked_add(
            count
                .checked_mul(PARTICIPANT_SIZE)
                .ok_or(CoordinatorLogError::RecordSizeOverflow)?,
        ),
        COMPLETE_TAG | GLOBAL_ENABLE_TAG => Some(RECORD_HEADER_SIZE),
        SEQUENCED_COMPLETE_TAG => Some(RECORD_HEADER_SIZE + SEQUENCED_PREFIX_SIZE),
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
    CommitSequenceExhausted,
    InvalidCommitSequence {
        offset: u64,
    },
    NonConsecutiveCommitSequence {
        offset: u64,
        expected: DatabaseCommitSeq,
        actual: DatabaseCommitSeq,
    },
    CompleteSequenceMismatch {
        database_txn_id: DatabaseTxnId,
        expected: Option<DatabaseCommitSeq>,
        actual: DatabaseCommitSeq,
    },
    CommitSequenceGap {
        commit_seq: DatabaseCommitSeq,
    },
    GlobalVisibilityNotEnabled,
    GlobalEnableWithIncompleteDecision,
    InvalidGlobalEnable {
        offset: u64,
    },
    SequencedRecordBeforeGlobalEnable {
        offset: u64,
    },
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
            Self::CommitSequenceExhausted => {
                formatter.write_str("database commit sequence space is exhausted")
            }
            Self::InvalidCommitSequence { offset } => write!(
                formatter,
                "coordinator record at {offset} has database commit sequence zero"
            ),
            Self::NonConsecutiveCommitSequence {
                offset,
                expected,
                actual,
            } => write!(
                formatter,
                "coordinator record at {offset} has database commit sequence {}, expected {}",
                actual.0, expected.0
            ),
            Self::CompleteSequenceMismatch {
                database_txn_id,
                expected,
                actual,
            } => write!(
                formatter,
                "database transaction {} completes sequence {}, expected {:?}",
                database_txn_id.0,
                actual.0,
                expected.map(|sequence| sequence.0)
            ),
            Self::CommitSequenceGap { commit_seq } => write!(
                formatter,
                "database commit sequence {} cannot publish before an earlier decision",
                commit_seq.0
            ),
            Self::GlobalVisibilityNotEnabled => {
                formatter.write_str("database-global visibility is not enabled")
            }
            Self::GlobalEnableWithIncompleteDecision => formatter.write_str(
                "database-global visibility cannot be enabled with an incomplete decision",
            ),
            Self::InvalidGlobalEnable { offset } => write!(
                formatter,
                "coordinator global-enable record at {offset} has an invalid payload"
            ),
            Self::SequencedRecordBeforeGlobalEnable { offset } => write!(
                formatter,
                "coordinator sequenced record at {offset} precedes global-enable"
            ),
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
    use netbadb_types::{DatabaseCommitSeq, DatabaseTxnId, StorageId, TxnId};

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

    #[test]
    fn global_mode_and_gap_free_sequences_round_trip_without_rewriting_v1() {
        let path = path("global-sequences");
        let _ = std::fs::remove_file(&path);
        let mut log = CoordinatorLog::create(&path).expect("create global log");
        assert!(!log.global_visibility_enabled());
        log.enable_global_visibility().expect("enable global mode");
        let first = log
            .sequenced_commit_decision(DatabaseTxnId(20), &participants(), None)
            .expect("first sequence");
        let second = log
            .sequenced_commit_decision(DatabaseTxnId(19), &participants(), None)
            .expect("second sequence");
        assert_eq!(first, DatabaseCommitSeq(1));
        assert_eq!(second, DatabaseCommitSeq(2));
        assert!(matches!(
            log.complete_sequenced(DatabaseTxnId(19), second),
            Err(CoordinatorLogError::CommitSequenceGap { .. })
        ));
        log.complete_sequenced(DatabaseTxnId(20), first)
            .expect("publish G1");
        log.complete_sequenced(DatabaseTxnId(19), second)
            .expect("publish G2");
        drop(log);

        let reopened = CoordinatorLog::open(&path).expect("reopen global log");
        assert!(reopened.global_visibility_enabled());
        assert_eq!(reopened.published_commit_seq(), DatabaseCommitSeq(2));
        assert_eq!(
            reopened.next_commit_seq().expect("next"),
            DatabaseCommitSeq(3)
        );
        let bytes = std::fs::read(&path).expect("read global log");
        assert_eq!(u16::from_le_bytes([bytes[4], bytes[5]]), 1);
        drop(reopened);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn deferred_complete_is_combined_with_the_next_decision_sync() {
        let path = path("deferred-complete-pipeline");
        let _ = std::fs::remove_file(&path);
        let mut log = CoordinatorLog::create(&path).expect("create pipeline log");
        log.enable_global_visibility().expect("enable global mode");

        let first = log
            .sequenced_commit_decision(DatabaseTxnId(30), &participants(), None)
            .expect("write G1 decision");
        log.defer_complete_sequenced(DatabaseTxnId(30), first)
            .expect("append deferred G1 Complete");
        assert_eq!(log.decision_sync_count(), 1);
        assert_eq!(log.checkpoint_sync_count(), 0);
        assert_eq!(log.combined_pipeline_sync_count(), 0);
        assert_eq!(log.pending_complete_count(), 1);
        assert_eq!(log.last_appended_complete(), DatabaseCommitSeq(1));
        assert_eq!(log.last_synced_complete(), DatabaseCommitSeq(0));

        let second = log
            .sequenced_commit_decision(DatabaseTxnId(31), &participants(), None)
            .expect("combine G1 Complete with G2 decision");
        assert_eq!(second, DatabaseCommitSeq(2));
        assert_eq!(log.decision_sync_count(), 2);
        assert_eq!(log.combined_pipeline_sync_count(), 1);
        assert_eq!(log.pending_complete_count(), 0);
        assert_eq!(log.last_synced_complete(), DatabaseCommitSeq(1));

        log.defer_complete_sequenced(DatabaseTxnId(31), second)
            .expect("append deferred G2 Complete");
        log.flush_complete_checkpoints()
            .expect("explicitly checkpoint G2");
        assert_eq!(log.checkpoint_sync_count(), 1);
        assert_eq!(log.pending_complete_count(), 0);
        assert_eq!(log.last_synced_complete(), DatabaseCommitSeq(2));
        drop(log);

        let reopened = CoordinatorLog::open(&path).expect("reopen checkpointed pipeline");
        assert_eq!(reopened.published_commit_seq(), DatabaseCommitSeq(2));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn failed_deferred_complete_append_is_repaired_before_the_next_decision() {
        let path = path("deferred-complete-repair");
        let _ = std::fs::remove_file(&path);
        let mut log = CoordinatorLog::create(&path).expect("create repair log");
        log.enable_global_visibility().expect("enable global mode");
        let first = log
            .sequenced_commit_decision(DatabaseTxnId(40), &participants(), None)
            .expect("write G1 decision");

        log.inject_complete_append_failure();
        log.defer_complete_sequenced(DatabaseTxnId(40), first)
            .expect("committed data does not fail on checkpoint append");
        assert_eq!(log.pending_complete_count(), 1);
        assert_eq!(log.last_appended_complete(), DatabaseCommitSeq(0));
        assert!(log.last_checkpoint_error().is_some());

        let second = log
            .sequenced_commit_decision(DatabaseTxnId(41), &participants(), None)
            .expect("repair G1 Complete before syncing G2 decision");
        assert_eq!(second, DatabaseCommitSeq(2));
        assert_eq!(log.last_appended_complete(), DatabaseCommitSeq(1));
        assert_eq!(log.last_synced_complete(), DatabaseCommitSeq(1));
        assert_eq!(log.pending_complete_count(), 0);
        assert!(log.last_checkpoint_error().is_none());
        assert_eq!(log.combined_pipeline_sync_count(), 1);
        let _ = std::fs::remove_file(path);
    }
}
