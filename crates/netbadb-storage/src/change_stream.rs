use std::cell::RefCell;
use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::rc::Rc;

use netbadb_schema::{SchemaFingerprint, TableDef};
use netbadb_types::{
    ChangeStreamGeneration, DatabaseTxnId, LsmCommitSeq, LsmRowId, PageId, RowId, ScalarValue,
    StorageDataVersion, StorageId, TableId, TxnId,
};

use crate::StorageError;
use crate::row_codec::{decode_row, encode_row};

pub(crate) type SharedChangeStream = Rc<RefCell<ChangeStreamManager>>;

pub const CHANGE_LOG_MAGIC: &[u8; 4] = b"NBCL";
pub const CHANGE_LOG_FORMAT_VERSION: u16 = 2;
pub const CHANGE_LOG_MAX_RECORD_BYTES: u32 = 64 * 1024 * 1024;
pub const CHANGE_LOG_MAX_MUTATIONS: u32 = 1_000_000;
pub const CHANGE_LOG_MAX_ROW_BYTES: u32 = 16 * 1024 * 1024;

const V1_HEADER_SIZE: usize = 80;
const V2_HEADER_SIZE: usize = 104;
const PREPARED_TAG: u8 = 1;
const FINALIZE_TAG: u8 = 2;
const HEAP_TAG: u8 = 1;
const LSM_TAG: u8 = 2;
const INSERT_TAG: u8 = 1;
const UPDATE_TAG: u8 = 2;
const DELETE_TAG: u8 = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeStorageKind {
    Heap,
    Lsm,
}

impl ChangeStorageKind {
    const fn tag(self) -> u8 {
        match self {
            Self::Heap => HEAP_TAG,
            Self::Lsm => LSM_TAG,
        }
    }

    fn decode(tag: u8) -> Result<Self, ChangeStreamError> {
        match tag {
            HEAP_TAG => Ok(Self::Heap),
            LSM_TAG => Ok(Self::Lsm),
            _ => Err(ChangeStreamError::InvalidHeader("unknown storage kind")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ChangeStreamCursor {
    pub storage_id: StorageId,
    pub generation: ChangeStreamGeneration,
    pub frontier: StorageDataVersion,
}

/// An explicit, runtime-only promise that history at `frontier` remains
/// readable for this exact stream incarnation.
///
/// Unlike [`ChangeStreamCursor`], a pin participates in reclamation safety.
/// Dropping or explicitly releasing it removes that authority. Pins are never
/// persisted and therefore do not survive process restart.
pub struct ChangeStreamRetentionPin {
    storage_id: StorageId,
    generation: ChangeStreamGeneration,
    frontier: StorageDataVersion,
    id: u64,
    registry: Rc<RefCell<RetentionPinRegistry>>,
    active: bool,
}

impl fmt::Debug for ChangeStreamRetentionPin {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ChangeStreamRetentionPin")
            .field("storage_id", &self.storage_id)
            .field("generation", &self.generation)
            .field("frontier", &self.frontier)
            .field("active", &self.active)
            .finish()
    }
}

impl ChangeStreamRetentionPin {
    #[must_use]
    pub const fn cursor(&self) -> ChangeStreamCursor {
        ChangeStreamCursor {
            storage_id: self.storage_id,
            generation: self.generation,
            frontier: self.frontier,
        }
    }

    #[must_use]
    pub const fn frontier(&self) -> StorageDataVersion {
        self.frontier
    }

    /// Releases this runtime retention authority before the handle is dropped.
    pub fn release(mut self) {
        self.unregister();
    }

    fn unregister(&mut self) {
        if self.active {
            self.registry.borrow_mut().pins.remove(&self.id);
            self.active = false;
        }
    }
}

impl Drop for ChangeStreamRetentionPin {
    fn drop(&mut self) {
        self.unregister();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChangeStreamRetentionPinInspection {
    pub storage_id: StorageId,
    pub generation: ChangeStreamGeneration,
    pub frontier: StorageDataVersion,
}

#[derive(Debug, Clone, Copy)]
struct RetentionPinEntry {
    storage_id: StorageId,
    generation: ChangeStreamGeneration,
    frontier: StorageDataVersion,
}

#[derive(Debug, Default)]
struct RetentionPinRegistry {
    next_id: u64,
    pins: BTreeMap<u64, RetentionPinEntry>,
}

/// Identity of one committed physical row version within its owning storage.
/// It is not a logical row identity and does not survive layout migration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StorageVersionKey {
    Heap {
        storage_id: StorageId,
        row_id: RowId,
    },
    Lsm {
        storage_id: StorageId,
        row_id: LsmRowId,
        version: LsmCommitSeq,
    },
}

impl StorageVersionKey {
    #[must_use]
    pub const fn storage_id(self) -> StorageId {
        match self {
            Self::Heap { storage_id, .. } | Self::Lsm { storage_id, .. } => storage_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageChange {
    Insert {
        new_version: StorageVersionKey,
        after: Vec<ScalarValue>,
    },
    Update {
        old_version: StorageVersionKey,
        new_version: StorageVersionKey,
        after: Vec<ScalarValue>,
    },
    Delete {
        old_version: StorageVersionKey,
    },
}

#[derive(Clone, PartialEq, Eq)]
pub struct ChangeBatch {
    pub sequence: u64,
    pub physical_txn_id: TxnId,
    pub database_txn_id: Option<DatabaseTxnId>,
    pub table_id: TableId,
    pub storage_id: StorageId,
    pub schema_fingerprint: SchemaFingerprint,
    pub before: StorageDataVersion,
    pub after: StorageDataVersion,
    pub mutations: Vec<StorageChange>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChangeBatchInspection {
    pub sequence: u64,
    pub physical_txn_id: TxnId,
    pub database_txn_id: Option<DatabaseTxnId>,
    pub before: StorageDataVersion,
    pub after: StorageDataVersion,
    pub mutation_count: u64,
}

impl ChangeBatch {
    #[must_use]
    pub fn inspection(&self) -> ChangeBatchInspection {
        ChangeBatchInspection {
            sequence: self.sequence,
            physical_txn_id: self.physical_txn_id,
            database_txn_id: self.database_txn_id,
            before: self.before,
            after: self.after,
            mutation_count: self.mutations.len() as u64,
        }
    }
}

impl fmt::Debug for ChangeBatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ChangeBatch")
            .field("sequence", &self.sequence)
            .field("physical_txn_id", &self.physical_txn_id)
            .field("database_txn_id", &self.database_txn_id)
            .field("table_id", &self.table_id)
            .field("storage_id", &self.storage_id)
            .field("schema_fingerprint", &self.schema_fingerprint)
            .field("before", &self.before)
            .field("after", &self.after)
            .field("mutation_count", &self.mutations.len())
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeReadResult {
    pub batches: Vec<ChangeBatch>,
    pub current_frontier: StorageDataVersion,
    pub has_more: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeStreamStatus {
    Disabled,
    Enabled,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeStreamInspection {
    pub storage_id: StorageId,
    pub table_id: TableId,
    pub status: ChangeStreamStatus,
    pub generation: Option<ChangeStreamGeneration>,
    pub schema_fingerprint: SchemaFingerprint,
    /// Frontier at which this stream incarnation was enabled.
    pub stream_origin_frontier: Option<StorageDataVersion>,
    /// Compatibility alias for `stream_origin_frontier`.
    pub baseline_data_version: Option<StorageDataVersion>,
    /// Highest recoverably committed frontier visible to runtime readers. In
    /// pipelined mode this may lead `finalize_checkpointed_through`.
    pub current_data_version: StorageDataVersion,
    /// Highest committed frontier whose Finalize marker has crossed an
    /// explicit NBCL sync boundary in this process (or during reopen repair).
    pub finalize_checkpointed_through: Option<StorageDataVersion>,
    /// Committed runtime batches whose Finalize marker is appended but has not
    /// yet crossed an explicit NBCL sync boundary.
    pub pending_finalize_checkpoint_count: u64,
    pub earliest_available_frontier: Option<StorageDataVersion>,
    pub committed_batch_count: u64,
    pub committed_mutation_count: u64,
    pub file_bytes: u64,
    pub prepared_unresolved_count: u64,
    pub last_error: Option<String>,
}

/// Payload-free metadata used to plan bounded change-stream maintenance.
///
/// `change_bytes` is the exact encoded prepared-record length consumed by a
/// bounded reader. `retained_file_bytes` additionally includes the matching
/// finalize record and is therefore the exact contribution retained by an
/// NBCL rewrite.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChangeBatchMaintenanceInspection {
    pub before: StorageDataVersion,
    pub after: StorageDataVersion,
    pub mutation_count: u64,
    pub change_bytes: u64,
    pub retained_file_bytes: u64,
}

/// Immutable, payload-free NBCL state for maintenance planning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeStreamMaintenanceInspection {
    pub stream: ChangeStreamInspection,
    pub batches: Vec<ChangeBatchMaintenanceInspection>,
    pub retention_pins: Vec<ChangeStreamRetentionPinInspection>,
    /// Exact encoded header size produced by the current production rewrite.
    pub rewrite_header_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeStreamGcStorageReport {
    pub storage_id: StorageId,
    pub generation: ChangeStreamGeneration,
    pub previous_earliest_frontier: StorageDataVersion,
    pub new_earliest_frontier: StorageDataVersion,
    pub current_frontier: StorageDataVersion,
    pub batches_removed: u64,
    pub mutations_removed: u64,
    pub bytes_before: u64,
    pub bytes_after: u64,
    pub bytes_reclaimed: u64,
}

#[derive(Debug)]
pub enum ChangeStreamError {
    Io(std::io::Error),
    InvalidMagic,
    UnsupportedVersion(u16),
    ChecksumMismatch {
        offset: u64,
    },
    Truncated {
        offset: u64,
    },
    InvalidHeader(&'static str),
    InvalidRecord(&'static str),
    RecordTooLarge(u64),
    MutationCountTooLarge(u64),
    RowPayloadTooLarge(u64),
    ContextMismatch,
    StreamIdentityMismatch,
    HistoryUnavailable,
    ChangeGap {
        expected: StorageDataVersion,
        actual: StorageDataVersion,
    },
    Disabled,
    Busy,
    FinalizeCheckpointPending,
    RetentionPinned {
        requested: StorageDataVersion,
        pinned: StorageDataVersion,
    },
    RetentionPinFrontierRegression {
        current: StorageDataVersion,
        requested: StorageDataVersion,
    },
    RetentionPinUnavailable,
    Unavailable(String),
    VersionExhausted,
}

impl fmt::Display for ChangeStreamError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "change-log I/O error: {error}"),
            Self::InvalidMagic => f.write_str("change-log magic does not match"),
            Self::UnsupportedVersion(version) => {
                write!(f, "unsupported change-log format version {version}")
            }
            Self::ChecksumMismatch { offset } => {
                write!(f, "change-log checksum mismatch at byte {offset}")
            }
            Self::Truncated { offset } => write!(f, "change-log is truncated at byte {offset}"),
            Self::InvalidHeader(reason) => write!(f, "invalid change-log header: {reason}"),
            Self::InvalidRecord(reason) => write!(f, "invalid change-log record: {reason}"),
            Self::RecordTooLarge(size) => write!(f, "change-log record has {size} bytes"),
            Self::MutationCountTooLarge(count) => {
                write!(f, "change-log record has {count} mutations")
            }
            Self::RowPayloadTooLarge(size) => write!(f, "change-log row has {size} bytes"),
            Self::ContextMismatch => f.write_str("change-stream storage context does not match"),
            Self::StreamIdentityMismatch => f.write_str("change-stream incarnation does not match"),
            Self::HistoryUnavailable => f.write_str("requested change history is unavailable"),
            Self::ChangeGap { expected, actual } => write!(
                f,
                "change stream has a frontier gap: expected {}, found {}",
                expected.0, actual.0
            ),
            Self::Disabled => f.write_str("change stream is disabled"),
            Self::Busy => f.write_str("change stream has unresolved prepared changes"),
            Self::FinalizeCheckpointPending => {
                f.write_str("change stream has committed Finalize checkpoints pending durability")
            }
            Self::RetentionPinned { requested, pinned } => write!(
                f,
                "change-stream reclamation through {} is blocked by a retention pin at {}",
                requested.0, pinned.0
            ),
            Self::RetentionPinFrontierRegression { current, requested } => write!(
                f,
                "retention pin cannot move backward from {} to {}",
                current.0, requested.0
            ),
            Self::RetentionPinUnavailable => {
                f.write_str("change-stream retention pin is no longer registered")
            }
            Self::Unavailable(reason) => write!(f, "change stream is unavailable: {reason}"),
            Self::VersionExhausted => f.write_str("change-stream version space is exhausted"),
        }
    }
}

impl std::error::Error for ChangeStreamError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<std::io::Error> for ChangeStreamError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum AuthoritativeOutcome {
    Committed(Option<LsmCommitSeq>),
    Aborted,
    Unresolved,
}

#[derive(Debug, Clone)]
pub(crate) struct PendingChangeSet {
    mutations: Vec<StorageChange>,
}

impl PendingChangeSet {
    pub(crate) const fn new() -> Self {
        Self {
            mutations: Vec::new(),
        }
    }

    pub(crate) fn record_insert(
        &mut self,
        new_version: StorageVersionKey,
        after: Vec<ScalarValue>,
    ) {
        self.mutations
            .push(StorageChange::Insert { new_version, after });
    }

    pub(crate) fn record_update(
        &mut self,
        old_version: StorageVersionKey,
        new_version: StorageVersionKey,
        after: Vec<ScalarValue>,
    ) {
        if let Some(position) = self.current_position(old_version) {
            match &mut self.mutations[position] {
                StorageChange::Insert {
                    new_version: current,
                    after: row,
                }
                | StorageChange::Update {
                    new_version: current,
                    after: row,
                    ..
                } => {
                    *current = new_version;
                    *row = after;
                }
                StorageChange::Delete { .. } => {}
            }
        } else {
            self.mutations.push(StorageChange::Update {
                old_version,
                new_version,
                after,
            });
        }
    }

    pub(crate) fn record_delete(&mut self, old_version: StorageVersionKey) {
        if let Some(position) = self.current_position(old_version) {
            match self.mutations.remove(position) {
                StorageChange::Insert { .. } => {}
                StorageChange::Update { old_version, .. }
                | StorageChange::Delete { old_version } => {
                    self.mutations
                        .insert(position, StorageChange::Delete { old_version });
                }
            }
        } else {
            self.mutations.push(StorageChange::Delete { old_version });
        }
    }

    fn current_position(&self, key: StorageVersionKey) -> Option<usize> {
        self.mutations.iter().position(|mutation| match mutation {
            StorageChange::Insert { new_version, .. }
            | StorageChange::Update { new_version, .. } => *new_version == key,
            StorageChange::Delete { .. } => false,
        })
    }

    pub(crate) fn as_slice(&self) -> &[StorageChange] {
        &self.mutations
    }

    pub(crate) fn clear(&mut self) {
        self.mutations.clear();
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct PreparedChange {
    sequence: u64,
    before: StorageDataVersion,
    after: StorageDataVersion,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ChangePrepareBatchReport {
    pub(crate) changing_member_count: usize,
    pub(crate) records_staged: usize,
    pub(crate) record_bytes: u64,
    pub(crate) syncs: u64,
    pub(crate) first_sequence: u64,
    pub(crate) last_sequence: u64,
    pub(crate) before_frontier: StorageDataVersion,
    pub(crate) after_reserved_frontier: StorageDataVersion,
    pub(crate) prior_finalize_checkpoints_checkpointed: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ChangeFinalizeBatchReport {
    pub(crate) finalized_member_count: usize,
    pub(crate) markers_staged: usize,
    pub(crate) marker_bytes: u64,
    pub(crate) syncs: u64,
    pub(crate) before_committed_frontier: StorageDataVersion,
    pub(crate) after_committed_frontier: StorageDataVersion,
    pub(crate) pending_finalize_checkpoints_after: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct ChangeStreamSyncCounts {
    pub(crate) total: u64,
    pub(crate) member_prepare: u64,
    pub(crate) group_prepare: u64,
    pub(crate) member_finalize: u64,
    pub(crate) group_finalize: u64,
    pub(crate) pipelined_finalize_checkpoint: u64,
    pub(crate) combined_finalize_prepare: u64,
    pub(crate) explicit_finalize_checkpoint: u64,
    pub(crate) recovery_finalize: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SyncReason {
    MemberPrepare,
    GroupPrepare,
    MemberFinalize,
    GroupFinalize,
    ExplicitFinalizeCheckpoint,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct SyncOutcome {
    prior_finalize_checkpoints: usize,
}

#[derive(Debug, Clone, Copy)]
struct Header {
    version: u16,
    active: bool,
    kind: ChangeStorageKind,
    storage_id: StorageId,
    table_id: TableId,
    fingerprint: SchemaFingerprint,
    generation: ChangeStreamGeneration,
    origin: StorageDataVersion,
    earliest: StorageDataVersion,
    /// Durable high-water at the last rewrite. Appended committed records may
    /// advance beyond this checkpoint without rewriting the header.
    current: StorageDataVersion,
    /// Durable sequence floor at the last rewrite. Appended records may
    /// advance the in-memory value beyond it.
    next_sequence: u64,
}

#[derive(Debug, Clone)]
struct PreparedRecord {
    batch: ChangeBatch,
    outcome: AuthoritativeOutcome,
    finalized: bool,
    prepared_durable: bool,
    staged_finalize: Option<Option<LsmCommitSeq>>,
    prepared_file_bytes: u64,
    retained_file_bytes: u64,
}

#[derive(Debug, Clone, Copy)]
struct BatchFileBytes {
    change_bytes: u64,
    retained_file_bytes: u64,
}

#[derive(Debug)]
enum State {
    Disabled {
        generation: ChangeStreamGeneration,
    },
    Enabled {
        /// Indirection keeps the state enum compact beside the disabled and
        /// unavailable variants; there is exactly one header per manager.
        header: Box<Header>,
        file: File,
        file_bytes: u64,
        batches: Vec<ChangeBatch>,
        batch_file_bytes: Vec<BatchFileBytes>,
        finalize_versions: BTreeMap<TxnId, Option<LsmCommitSeq>>,
        unresolved: BTreeMap<TxnId, PreparedRecord>,
        /// Sequence-ordered committed batches whose Finalize bytes have been
        /// appended but not explicitly synchronized.
        pending_finalize_checkpoints: BTreeMap<u64, TxnId>,
        finalize_checkpointed_through: StorageDataVersion,
        next_sequence: u64,
    },
    Unavailable {
        generation: Option<ChangeStreamGeneration>,
        reason: String,
    },
}

#[derive(Debug)]
pub(crate) struct ChangeStreamManager {
    path: PathBuf,
    kind: ChangeStorageKind,
    storage_id: StorageId,
    table_id: TableId,
    fingerprint: SchemaFingerprint,
    state: State,
    sync_counts: ChangeStreamSyncCounts,
    retention_pins: Rc<RefCell<RetentionPinRegistry>>,
    #[cfg(any(test, feature = "test-hooks"))]
    fail_next_group_prepare_sync: bool,
    #[cfg(any(test, feature = "test-hooks"))]
    fail_next_group_finalize_sync: bool,
    #[cfg(test)]
    fail_next_explicit_checkpoint_sync: bool,
}

impl ChangeStreamManager {
    pub(crate) fn disabled(
        path: PathBuf,
        kind: ChangeStorageKind,
        storage_id: StorageId,
        table: &TableDef,
    ) -> Result<Self, StorageError> {
        Ok(Self {
            path,
            kind,
            storage_id,
            table_id: table.id,
            fingerprint: table.fingerprint()?,
            state: State::Disabled {
                generation: ChangeStreamGeneration(0),
            },
            sync_counts: ChangeStreamSyncCounts::default(),
            retention_pins: Rc::new(RefCell::new(RetentionPinRegistry::default())),
            #[cfg(any(test, feature = "test-hooks"))]
            fail_next_group_prepare_sync: false,
            #[cfg(any(test, feature = "test-hooks"))]
            fail_next_group_finalize_sync: false,
            #[cfg(test)]
            fail_next_explicit_checkpoint_sync: false,
        })
    }

    pub(crate) fn open<F>(
        path: PathBuf,
        kind: ChangeStorageKind,
        storage_id: StorageId,
        table: &TableDef,
        outcome: F,
    ) -> Result<Self, StorageError>
    where
        F: Fn(TxnId) -> AuthoritativeOutcome,
    {
        let fingerprint = table.fingerprint()?;
        let guard_path = change_stream_guard_path(&path);
        if !path.exists() {
            let state = if guard_path.exists() {
                State::Unavailable {
                    generation: read_guard(&guard_path).ok().map(|header| header.generation),
                    reason: "active change log is missing".into(),
                }
            } else {
                State::Disabled {
                    generation: ChangeStreamGeneration(0),
                }
            };
            return Ok(Self {
                path,
                kind,
                storage_id,
                table_id: table.id,
                fingerprint,
                state,
                sync_counts: ChangeStreamSyncCounts::default(),
                retention_pins: Rc::new(RefCell::new(RetentionPinRegistry::default())),
                #[cfg(any(test, feature = "test-hooks"))]
                fail_next_group_prepare_sync: false,
                #[cfg(any(test, feature = "test-hooks"))]
                fail_next_group_finalize_sync: false,
                #[cfg(test)]
                fail_next_explicit_checkpoint_sync: false,
            });
        }
        let state = match load_file(&path, table, &outcome, true) {
            Ok((header, file, file_bytes, records)) => {
                if header.storage_id != storage_id
                    || header.table_id != table.id
                    || header.fingerprint != fingerprint
                    || header.kind != kind
                {
                    State::Unavailable {
                        generation: Some(header.generation),
                        reason: "persisted identity does not match authoritative storage".into(),
                    }
                } else if !header.active {
                    let _ = remove_guard(&guard_path);
                    State::Disabled {
                        generation: header.generation,
                    }
                } else {
                    let generation = header.generation;
                    let guard_valid = if guard_path.exists() {
                        read_guard(&guard_path).and_then(|guard| {
                            if guard_identity_matches(&guard, &header) {
                                Ok(())
                            } else {
                                Err(ChangeStreamError::InvalidHeader(
                                    "active guard identity mismatch",
                                ))
                            }
                        })
                    } else {
                        Ok(())
                    };
                    match guard_valid.and_then(|()| write_guard(&guard_path, &header)) {
                        Ok(()) => match build_enabled_state(header, file, file_bytes, records) {
                            Ok((state, recovery_finalize_syncs)) => {
                                let counts = ChangeStreamSyncCounts {
                                    total: recovery_finalize_syncs,
                                    recovery_finalize: recovery_finalize_syncs,
                                    ..ChangeStreamSyncCounts::default()
                                };
                                return Ok(Self {
                                    path,
                                    kind,
                                    storage_id,
                                    table_id: table.id,
                                    fingerprint,
                                    state,
                                    sync_counts: counts,
                                    retention_pins: Rc::new(RefCell::new(
                                        RetentionPinRegistry::default(),
                                    )),
                                    #[cfg(any(test, feature = "test-hooks"))]
                                    fail_next_group_prepare_sync: false,
                                    #[cfg(any(test, feature = "test-hooks"))]
                                    fail_next_group_finalize_sync: false,
                                    #[cfg(test)]
                                    fail_next_explicit_checkpoint_sync: false,
                                });
                            }
                            Err(error) => State::Unavailable {
                                generation: Some(generation),
                                reason: error.to_string(),
                            },
                        },
                        Err(error) => State::Unavailable {
                            generation: Some(generation),
                            reason: error.to_string(),
                        },
                    }
                }
            }
            Err(error) => State::Unavailable {
                generation: read_guard(&guard_path).ok().map(|header| header.generation),
                reason: error.to_string(),
            },
        };
        Ok(Self {
            path,
            kind,
            storage_id,
            table_id: table.id,
            fingerprint,
            state,
            sync_counts: ChangeStreamSyncCounts::default(),
            retention_pins: Rc::new(RefCell::new(RetentionPinRegistry::default())),
            #[cfg(any(test, feature = "test-hooks"))]
            fail_next_group_prepare_sync: false,
            #[cfg(any(test, feature = "test-hooks"))]
            fail_next_group_finalize_sync: false,
            #[cfg(test)]
            fail_next_explicit_checkpoint_sync: false,
        })
    }

    pub(crate) const fn sync_count(&self) -> u64 {
        self.sync_counts.total
    }

    pub(crate) const fn sync_counts(&self) -> ChangeStreamSyncCounts {
        self.sync_counts
    }

    pub(crate) fn requires_changes(&self) -> bool {
        !matches!(self.state, State::Disabled { .. })
    }

    pub(crate) fn ensure_mutation_available(&self) -> Result<(), StorageError> {
        match &self.state {
            State::Unavailable { reason, .. } => {
                Err(ChangeStreamError::Unavailable(reason.clone()).into())
            }
            State::Disabled { .. } | State::Enabled { .. } => Ok(()),
        }
    }

    pub(crate) fn enable(
        &mut self,
        baseline: StorageDataVersion,
    ) -> Result<ChangeStreamCursor, StorageError> {
        let prior = match &self.state {
            State::Disabled { generation } => generation.0,
            State::Enabled {
                header, batches, ..
            } => {
                return Ok(cursor_for(header, effective_current(header, batches)));
            }
            State::Unavailable { generation, .. } => generation.map_or(0, |value| value.0),
        };
        let generation = ChangeStreamGeneration(
            prior
                .checked_add(1)
                .ok_or(ChangeStreamError::VersionExhausted)?,
        );
        let header = Header {
            version: CHANGE_LOG_FORMAT_VERSION,
            active: true,
            kind: self.kind,
            storage_id: self.storage_id,
            table_id: self.table_id,
            fingerprint: self.fingerprint,
            generation,
            origin: baseline,
            earliest: baseline,
            current: baseline,
            next_sequence: 1,
        };
        let (file, file_bytes) = match rewrite_header(&self.path, &header) {
            Ok(value) => value,
            Err(error) => {
                self.state = State::Unavailable {
                    generation: Some(generation),
                    reason: error.to_string(),
                };
                return Err(error.into());
            }
        };
        if let Err(error) = write_guard(&change_stream_guard_path(&self.path), &header) {
            self.state = State::Unavailable {
                generation: Some(generation),
                reason: error.to_string(),
            };
            return Err(error.into());
        }
        let cursor = cursor_for(&header, baseline);
        self.state = State::Enabled {
            header: Box::new(header),
            file,
            file_bytes,
            batches: Vec::new(),
            batch_file_bytes: Vec::new(),
            finalize_versions: BTreeMap::new(),
            unresolved: BTreeMap::new(),
            pending_finalize_checkpoints: BTreeMap::new(),
            finalize_checkpointed_through: baseline,
            next_sequence: 1,
        };
        Ok(cursor)
    }

    pub(crate) fn disable(&mut self) -> Result<(), StorageError> {
        let generation = match &self.state {
            State::Disabled { .. } => return Ok(()),
            State::Enabled { header, .. } => header.generation,
            State::Unavailable { generation, .. } => {
                generation.unwrap_or(ChangeStreamGeneration(0))
            }
        };
        let header = Header {
            version: CHANGE_LOG_FORMAT_VERSION,
            active: false,
            kind: self.kind,
            storage_id: self.storage_id,
            table_id: self.table_id,
            fingerprint: self.fingerprint,
            generation,
            origin: StorageDataVersion(0),
            earliest: StorageDataVersion(0),
            current: StorageDataVersion(0),
            next_sequence: 1,
        };
        let _ = rewrite_header(&self.path, &header)?;
        self.state = State::Disabled { generation };
        remove_guard(&change_stream_guard_path(&self.path))?;
        Ok(())
    }

    pub(crate) fn prepare(
        &mut self,
        txn_id: TxnId,
        database_txn_id: Option<DatabaseTxnId>,
        changes: &[StorageChange],
    ) -> Result<Option<PreparedChange>, StorageError> {
        let result = (|| {
            let prepared =
                stage_prepare_enabled(&mut self.state, txn_id, database_txn_id, changes)?;
            if let Some(prepared) = prepared {
                let report = durabilize_prepared_batch(
                    &mut self.state,
                    &mut self.sync_counts,
                    &[(txn_id, database_txn_id, prepared)],
                    false,
                    SyncReason::MemberPrepare,
                    #[cfg(any(test, feature = "test-hooks"))]
                    false,
                )?;
                debug_assert!(report.syncs <= 1);
            }
            Ok(prepared)
        })();
        if let Err(error) = &result {
            self.poison_after_io_error(error);
        }
        result.map_err(Into::into)
    }

    /// Appends one group member's existing PreparedChange without claiming
    /// durability. This is intentionally crate-private and group-only.
    pub(crate) fn stage_group_prepare(
        &mut self,
        txn_id: TxnId,
        database_txn_id: DatabaseTxnId,
        changes: &[StorageChange],
    ) -> Result<Option<PreparedChange>, StorageError> {
        let result = stage_prepare_enabled(&mut self.state, txn_id, Some(database_txn_id), changes);
        if let Err(error) = &result {
            self.poison_after_io_error(error);
        }
        result.map_err(Into::into)
    }

    pub(crate) fn durabilize_group_prepared_batch(
        &mut self,
        candidates: &[(TxnId, DatabaseTxnId, PreparedChange)],
    ) -> Result<ChangePrepareBatchReport, StorageError> {
        #[cfg(any(test, feature = "test-hooks"))]
        let fail_sync = std::mem::take(&mut self.fail_next_group_prepare_sync);
        let identities = candidates
            .iter()
            .map(|(txn_id, database_txn_id, prepared)| (*txn_id, Some(*database_txn_id), *prepared))
            .collect::<Vec<_>>();
        let result = durabilize_prepared_batch(
            &mut self.state,
            &mut self.sync_counts,
            &identities,
            true,
            SyncReason::GroupPrepare,
            #[cfg(any(test, feature = "test-hooks"))]
            fail_sync,
        );
        match result {
            Ok(report) => Ok(report),
            Err(error) => {
                self.poison_after_io_error(&error);
                Err(error.into())
            }
        }
    }

    pub(crate) fn publish(
        &mut self,
        txn_id: TxnId,
        prepared: PreparedChange,
        lsm_commit: Option<LsmCommitSeq>,
    ) -> Result<(), StorageError> {
        let result = finalize_batch(
            &mut self.state,
            &mut self.sync_counts,
            &[(txn_id, prepared, lsm_commit)],
            true,
            SyncReason::MemberFinalize,
            #[cfg(any(test, feature = "test-hooks"))]
            false,
        );
        if let Err(error) = &result {
            self.poison_after_io_error(error);
        }
        result.map(|_| ()).map_err(Into::into)
    }

    pub(crate) fn finalize_group_batch(
        &mut self,
        candidates: &[(TxnId, PreparedChange, Option<LsmCommitSeq>)],
    ) -> Result<ChangeFinalizeBatchReport, StorageError> {
        #[cfg(any(test, feature = "test-hooks"))]
        let fail_sync = std::mem::take(&mut self.fail_next_group_finalize_sync);
        let result = finalize_batch(
            &mut self.state,
            &mut self.sync_counts,
            candidates,
            true,
            SyncReason::GroupFinalize,
            #[cfg(any(test, feature = "test-hooks"))]
            fail_sync,
        );
        match result {
            Ok(report) => Ok(report),
            Err(error) => {
                self.poison_after_io_error(&error);
                Err(error.into())
            }
        }
    }

    pub(crate) fn finalize_group_batch_pipelined(
        &mut self,
        candidates: &[(TxnId, PreparedChange, Option<LsmCommitSeq>)],
    ) -> Result<ChangeFinalizeBatchReport, StorageError> {
        let result = finalize_batch(
            &mut self.state,
            &mut self.sync_counts,
            candidates,
            false,
            SyncReason::GroupFinalize,
            #[cfg(any(test, feature = "test-hooks"))]
            false,
        );
        if let Err(error) = &result {
            self.poison_after_io_error(error);
        }
        result.map_err(Into::into)
    }

    /// Explicitly synchronizes committed Finalize checkpoints left by the
    /// pipelined group path. A no-op performs no I/O.
    pub(crate) fn checkpoint_pending_finalizes(&mut self) -> Result<u64, StorageError> {
        let pending = match &self.state {
            State::Enabled {
                pending_finalize_checkpoints,
                ..
            } => pending_finalize_checkpoints.len(),
            State::Disabled { .. } => return Ok(0),
            // An unavailable stream has no live pending-checkpoint inventory.
            // Preserve the existing database flush/close contract: the stream
            // remains visibly unavailable, but it cannot contribute work to
            // this narrow checkpoint operation.
            State::Unavailable { .. } => return Ok(0),
        };
        if pending == 0 {
            return Ok(0);
        }
        #[cfg(test)]
        if std::mem::take(&mut self.fail_next_explicit_checkpoint_sync) {
            let error = ChangeStreamError::Io(std::io::Error::other(
                "injected Finalize checkpoint sync failure",
            ));
            self.poison_after_io_error(&error);
            return Err(error.into());
        }
        #[cfg(test)]
        crate::crash_test::maybe_crash_named("change-before-explicit-finalize-checkpoint-sync");
        let result = sync_log(
            &mut self.state,
            &mut self.sync_counts,
            SyncReason::ExplicitFinalizeCheckpoint,
        );
        #[cfg(test)]
        if result.is_ok() {
            crate::crash_test::maybe_crash_named("change-after-explicit-finalize-checkpoint-sync");
        }
        if let Err(error) = &result {
            self.poison_after_io_error(error);
        }
        result.map(|_| 1).map_err(Into::into)
    }

    #[cfg(test)]
    pub(crate) fn inject_finalize_checkpoint_sync_failure(&mut self) {
        self.fail_next_explicit_checkpoint_sync = true;
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn inject_group_prepare_sync_failure(&mut self) {
        self.fail_next_group_prepare_sync = true;
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn inject_group_finalize_sync_failure(&mut self) {
        self.fail_next_group_finalize_sync = true;
    }

    fn poison_after_io_error(&mut self, error: &ChangeStreamError) {
        if !matches!(error, ChangeStreamError::Io(_)) {
            return;
        }
        let generation = match &self.state {
            State::Disabled { generation } => Some(*generation),
            State::Enabled { header, .. } => Some(header.generation),
            State::Unavailable { generation, .. } => *generation,
        };
        self.state = State::Unavailable {
            generation,
            reason: error.to_string(),
        };
    }

    pub(crate) fn abandon(&mut self, txn_id: TxnId) -> Result<(), StorageError> {
        if let State::Enabled { unresolved, .. } = &mut self.state {
            if let Some(record) = unresolved.get(&txn_id) {
                let tail = unresolved
                    .values()
                    .max_by_key(|candidate| candidate.batch.sequence)
                    .map(|candidate| candidate.batch.physical_txn_id);
                if tail != Some(record.batch.physical_txn_id) {
                    return Err(ChangeStreamError::InvalidRecord(
                        "prepared reservation is not the chain tail",
                    )
                    .into());
                }
            }
            unresolved.remove(&txn_id);
        }
        Ok(())
    }

    pub(crate) fn cursor(&self) -> Result<ChangeStreamCursor, StorageError> {
        match &self.state {
            State::Enabled {
                header, batches, ..
            } => Ok(cursor_for(header, effective_current(header, batches))),
            State::Disabled { .. } => Err(ChangeStreamError::Disabled.into()),
            State::Unavailable { reason, .. } => {
                Err(ChangeStreamError::Unavailable(reason.clone()).into())
            }
        }
    }

    pub(crate) fn read(
        &self,
        cursor: ChangeStreamCursor,
        max_batches: usize,
        max_bytes: u64,
    ) -> Result<ChangeReadResult, StorageError> {
        let State::Enabled {
            header,
            batches,
            batch_file_bytes,
            ..
        } = &self.state
        else {
            return match &self.state {
                State::Disabled { .. } => Err(ChangeStreamError::Disabled.into()),
                State::Unavailable { reason, .. } => {
                    Err(ChangeStreamError::Unavailable(reason.clone()).into())
                }
                State::Enabled { .. } => unreachable!(),
            };
        };
        if cursor.storage_id != header.storage_id {
            return Err(ChangeStreamError::ContextMismatch.into());
        }
        if cursor.generation != header.generation {
            return Err(ChangeStreamError::StreamIdentityMismatch.into());
        }
        let current = effective_current(header, batches);
        if cursor.frontier.0 < header.earliest.0 || cursor.frontier.0 > current.0 {
            return Err(ChangeStreamError::HistoryUnavailable.into());
        }
        if cursor.frontier == current {
            return Ok(ChangeReadResult {
                batches: Vec::new(),
                current_frontier: current,
                has_more: false,
            });
        }
        let start = batches
            .iter()
            .position(|batch| batch.before == cursor.frontier)
            .ok_or(ChangeStreamError::HistoryUnavailable)?;
        let mut expected = cursor.frontier;
        let mut bytes = 0_u64;
        let mut selected = Vec::new();
        for (batch, record_bytes) in batches[start..].iter().zip(&batch_file_bytes[start..]) {
            if batch.before != expected {
                return Err(ChangeStreamError::ChangeGap {
                    expected,
                    actual: batch.before,
                }
                .into());
            }
            let encoded_bytes = record_bytes.change_bytes;
            if selected.len() >= max_batches
                || bytes
                    .checked_add(encoded_bytes)
                    .is_none_or(|total| total > max_bytes)
            {
                break;
            }
            bytes += encoded_bytes;
            expected = batch.after;
            selected.push(batch.clone());
        }
        Ok(ChangeReadResult {
            has_more: expected != current,
            batches: selected,
            current_frontier: current,
        })
    }

    pub(crate) fn acquire_retention_pin(
        &self,
        cursor: ChangeStreamCursor,
    ) -> Result<ChangeStreamRetentionPin, StorageError> {
        let State::Enabled {
            header, batches, ..
        } = &self.state
        else {
            return match &self.state {
                State::Disabled { .. } => Err(ChangeStreamError::Disabled.into()),
                State::Unavailable { reason, .. } => {
                    Err(ChangeStreamError::Unavailable(reason.clone()).into())
                }
                State::Enabled { .. } => unreachable!(),
            };
        };
        validate_cursor_identity_and_frontier(header, batches, cursor)?;
        let mut registry = self.retention_pins.borrow_mut();
        let id = registry.next_id;
        registry.next_id = id
            .checked_add(1)
            .ok_or(ChangeStreamError::VersionExhausted)?;
        registry.pins.insert(
            id,
            RetentionPinEntry {
                storage_id: cursor.storage_id,
                generation: cursor.generation,
                frontier: cursor.frontier,
            },
        );
        drop(registry);
        Ok(ChangeStreamRetentionPin {
            storage_id: cursor.storage_id,
            generation: cursor.generation,
            frontier: cursor.frontier,
            id,
            registry: Rc::clone(&self.retention_pins),
            active: true,
        })
    }

    pub(crate) fn advance_retention_pin(
        &self,
        pin: &mut ChangeStreamRetentionPin,
        frontier: StorageDataVersion,
    ) -> Result<(), StorageError> {
        if frontier.0 < pin.frontier.0 {
            return Err(ChangeStreamError::RetentionPinFrontierRegression {
                current: pin.frontier,
                requested: frontier,
            }
            .into());
        }
        let cursor = ChangeStreamCursor {
            storage_id: pin.storage_id,
            generation: pin.generation,
            frontier,
        };
        let State::Enabled {
            header, batches, ..
        } = &self.state
        else {
            return Err(ChangeStreamError::StreamIdentityMismatch.into());
        };
        validate_cursor_identity_and_frontier(header, batches, cursor)?;
        if !Rc::ptr_eq(&pin.registry, &self.retention_pins) || !pin.active {
            return Err(ChangeStreamError::RetentionPinUnavailable.into());
        }
        let mut registry = self.retention_pins.borrow_mut();
        let entry = registry
            .pins
            .get_mut(&pin.id)
            .ok_or(ChangeStreamError::RetentionPinUnavailable)?;
        if entry.storage_id != pin.storage_id || entry.generation != pin.generation {
            return Err(ChangeStreamError::RetentionPinUnavailable.into());
        }
        entry.frontier = frontier;
        pin.frontier = frontier;
        Ok(())
    }

    pub(crate) fn gc_through(
        &mut self,
        frontier: StorageDataVersion,
    ) -> Result<ChangeStreamGcStorageReport, StorageError> {
        let path = self.path.clone();
        match &self.state {
            State::Disabled { .. } => return Err(ChangeStreamError::Disabled.into()),
            State::Unavailable { reason, .. } => {
                return Err(ChangeStreamError::Unavailable(reason.clone()).into());
            }
            State::Enabled { .. } => {}
        }
        let State::Enabled {
            header,
            file,
            file_bytes,
            batches,
            batch_file_bytes,
            finalize_versions,
            unresolved,
            pending_finalize_checkpoints,
            finalize_checkpointed_through,
            next_sequence,
        } = &mut self.state
        else {
            return Err(ChangeStreamError::Unavailable(
                "change stream state changed during synchronous GC".into(),
            )
            .into());
        };
        if !unresolved.is_empty() {
            return Err(ChangeStreamError::Busy.into());
        }
        if !pending_finalize_checkpoints.is_empty() {
            return Err(ChangeStreamError::FinalizeCheckpointPending.into());
        }
        if let Some(pinned) = self
            .retention_pins
            .borrow()
            .pins
            .values()
            .filter(|pin| {
                pin.storage_id == header.storage_id && pin.generation == header.generation
            })
            .map(|pin| pin.frontier)
            .min_by_key(|value| value.0)
            .filter(|pinned| frontier.0 > pinned.0)
        {
            return Err(ChangeStreamError::RetentionPinned {
                requested: frontier,
                pinned,
            }
            .into());
        }
        let current = effective_current(header, batches);
        if frontier.0 < header.earliest.0 || frontier.0 > current.0 {
            return Err(ChangeStreamError::HistoryUnavailable.into());
        }
        let first_retained = batches
            .iter()
            .position(|batch| batch.after.0 > frontier.0)
            .unwrap_or(batches.len());
        if first_retained < batches.len() && batches[first_retained].before != frontier {
            return Err(ChangeStreamError::HistoryUnavailable.into());
        }
        if first_retained == batches.len() && frontier != current {
            return Err(ChangeStreamError::HistoryUnavailable.into());
        }
        let previous_earliest = header.earliest;
        let bytes_before = *file_bytes;
        if frontier == previous_earliest && header.version == CHANGE_LOG_FORMAT_VERSION {
            return Ok(ChangeStreamGcStorageReport {
                storage_id: header.storage_id,
                generation: header.generation,
                previous_earliest_frontier: previous_earliest,
                new_earliest_frontier: previous_earliest,
                current_frontier: current,
                batches_removed: 0,
                mutations_removed: 0,
                bytes_before,
                bytes_after: bytes_before,
                bytes_reclaimed: 0,
            });
        }
        let batches_removed = first_retained as u64;
        let mutations_removed = batches[..first_retained]
            .iter()
            .map(|batch| batch.mutations.len() as u64)
            .sum();
        let replacement_header = Header {
            version: CHANGE_LOG_FORMAT_VERSION,
            active: true,
            kind: header.kind,
            storage_id: header.storage_id,
            table_id: header.table_id,
            fingerprint: header.fingerprint,
            generation: header.generation,
            origin: header.origin,
            earliest: frontier,
            current,
            next_sequence: *next_sequence,
        };
        let (replacement_file, replacement_bytes) = rewrite_retained(
            &path,
            &replacement_header,
            &batches[first_retained..],
            finalize_versions,
        )?;
        gc_crash("gc-log-published");
        write_guard(&change_stream_guard_path(&path), &replacement_header)?;
        gc_crash("gc-guard-published");
        let removed_ids = batches[..first_retained]
            .iter()
            .map(|batch| batch.physical_txn_id)
            .collect::<Vec<_>>();
        batches.drain(..first_retained);
        batch_file_bytes.drain(..first_retained);
        for txn_id in removed_ids {
            finalize_versions.remove(&txn_id);
        }
        **header = replacement_header;
        *file = replacement_file;
        *file_bytes = replacement_bytes;
        *finalize_checkpointed_through = current;
        Ok(ChangeStreamGcStorageReport {
            storage_id: header.storage_id,
            generation: header.generation,
            previous_earliest_frontier: previous_earliest,
            new_earliest_frontier: frontier,
            current_frontier: current,
            batches_removed,
            mutations_removed,
            bytes_before,
            bytes_after: replacement_bytes,
            bytes_reclaimed: bytes_before.saturating_sub(replacement_bytes),
        })
    }

    pub(crate) fn inspection(&self) -> ChangeStreamInspection {
        let (
            status,
            generation,
            origin,
            earliest,
            current,
            batches,
            file_bytes,
            unresolved,
            checkpointed,
            pending_finalize_checkpoints,
            last_error,
        ) = match &self.state {
            State::Disabled { generation } => (
                ChangeStreamStatus::Disabled,
                Some(*generation),
                None,
                None,
                StorageDataVersion(0),
                &[][..],
                fs::metadata(&self.path).map_or(0, |metadata| metadata.len()),
                0,
                None,
                0,
                None,
            ),
            State::Enabled {
                header,
                batches,
                file_bytes,
                unresolved,
                finalize_checkpointed_through,
                pending_finalize_checkpoints,
                ..
            } => (
                ChangeStreamStatus::Enabled,
                Some(header.generation),
                Some(header.origin),
                Some(header.earliest),
                effective_current(header, batches),
                batches.as_slice(),
                *file_bytes,
                unresolved.len() as u64,
                Some(*finalize_checkpointed_through),
                pending_finalize_checkpoints.len() as u64,
                None,
            ),
            State::Unavailable { generation, reason } => (
                ChangeStreamStatus::Unavailable,
                *generation,
                None,
                None,
                StorageDataVersion(0),
                &[][..],
                fs::metadata(&self.path).map_or(0, |metadata| metadata.len()),
                0,
                None,
                0,
                Some(reason.clone()),
            ),
        };
        ChangeStreamInspection {
            storage_id: self.storage_id,
            table_id: self.table_id,
            status,
            generation,
            schema_fingerprint: self.fingerprint,
            stream_origin_frontier: origin,
            baseline_data_version: origin,
            current_data_version: current,
            finalize_checkpointed_through: checkpointed,
            pending_finalize_checkpoint_count: pending_finalize_checkpoints,
            earliest_available_frontier: earliest,
            committed_batch_count: batches.len() as u64,
            committed_mutation_count: batches
                .iter()
                .map(|batch| batch.mutations.len() as u64)
                .sum(),
            file_bytes,
            prepared_unresolved_count: unresolved,
            last_error,
        }
    }

    pub(crate) fn maintenance_inspection(&self) -> ChangeStreamMaintenanceInspection {
        let stream = self.inspection();
        let batches = match &self.state {
            State::Enabled {
                batches,
                batch_file_bytes,
                ..
            } => batches
                .iter()
                .zip(batch_file_bytes)
                .map(|(batch, bytes)| ChangeBatchMaintenanceInspection {
                    before: batch.before,
                    after: batch.after,
                    mutation_count: batch.mutations.len() as u64,
                    change_bytes: bytes.change_bytes,
                    retained_file_bytes: bytes.retained_file_bytes,
                })
                .collect(),
            State::Disabled { .. } | State::Unavailable { .. } => Vec::new(),
        };
        let retention_pins = self
            .retention_pins
            .borrow()
            .pins
            .values()
            .map(|pin| ChangeStreamRetentionPinInspection {
                storage_id: pin.storage_id,
                generation: pin.generation,
                frontier: pin.frontier,
            })
            .collect();
        ChangeStreamMaintenanceInspection {
            stream,
            batches,
            retention_pins,
            rewrite_header_bytes: V2_HEADER_SIZE as u64,
        }
    }
}

fn validate_cursor_identity_and_frontier(
    header: &Header,
    batches: &[ChangeBatch],
    cursor: ChangeStreamCursor,
) -> Result<(), ChangeStreamError> {
    if cursor.storage_id != header.storage_id {
        return Err(ChangeStreamError::ContextMismatch);
    }
    if cursor.generation != header.generation {
        return Err(ChangeStreamError::StreamIdentityMismatch);
    }
    let current = effective_current(header, batches);
    if cursor.frontier.0 < header.earliest.0 || cursor.frontier.0 > current.0 {
        return Err(ChangeStreamError::HistoryUnavailable);
    }
    Ok(())
}

fn stage_prepare_enabled(
    state: &mut State,
    txn_id: TxnId,
    database_txn_id: Option<DatabaseTxnId>,
    changes: &[StorageChange],
) -> Result<Option<PreparedChange>, ChangeStreamError> {
    if changes.is_empty() {
        return Ok(None);
    }
    let State::Enabled {
        header,
        file,
        file_bytes,
        batches,
        unresolved,
        next_sequence,
        ..
    } = state
    else {
        return match state {
            State::Disabled { .. } => Ok(None),
            State::Unavailable { reason, .. } => {
                Err(ChangeStreamError::Unavailable(reason.clone()))
            }
            State::Enabled { .. } => unreachable!(),
        };
    };
    if let Some(record) = unresolved.get(&txn_id) {
        return Ok(Some(prepared_identity(&record.batch)));
    }
    let before = unresolved
        .values()
        .max_by_key(|record| record.batch.sequence)
        .map_or_else(
            || effective_current(header, batches),
            |record| record.batch.after,
        );
    let after = StorageDataVersion(
        before
            .0
            .checked_add(1)
            .ok_or(ChangeStreamError::VersionExhausted)?,
    );
    let sequence = *next_sequence;
    let following_sequence = next_sequence
        .checked_add(1)
        .ok_or(ChangeStreamError::VersionExhausted)?;
    let batch = ChangeBatch {
        sequence,
        physical_txn_id: txn_id,
        database_txn_id,
        table_id: header.table_id,
        storage_id: header.storage_id,
        schema_fingerprint: header.fingerprint,
        before,
        after,
        mutations: changes.to_vec(),
    };
    let encoded = encode_record(&batch)?;
    let following_file_bytes = file_bytes
        .checked_add(encoded.len() as u64)
        .ok_or(ChangeStreamError::RecordTooLarge(u64::MAX))?;
    file.seek(SeekFrom::End(0))?;
    file.write_all(&encoded)?;
    *file_bytes = following_file_bytes;
    *next_sequence = following_sequence;
    unresolved.insert(
        txn_id,
        PreparedRecord {
            batch,
            outcome: AuthoritativeOutcome::Unresolved,
            finalized: false,
            prepared_durable: false,
            staged_finalize: None,
            prepared_file_bytes: encoded.len() as u64,
            retained_file_bytes: encoded.len() as u64,
        },
    );
    Ok(Some(PreparedChange {
        sequence,
        before,
        after,
    }))
}

fn durabilize_prepared_batch(
    state: &mut State,
    sync_counts: &mut ChangeStreamSyncCounts,
    candidates: &[(TxnId, Option<DatabaseTxnId>, PreparedChange)],
    require_exact_prefix: bool,
    sync_reason: SyncReason,
    #[cfg(any(test, feature = "test-hooks"))] fail_sync: bool,
) -> Result<ChangePrepareBatchReport, ChangeStreamError> {
    let Some((_, _, first_prepared)) = candidates.first().copied() else {
        return Err(ChangeStreamError::InvalidRecord(
            "prepared durability batch is empty",
        ));
    };
    let (record_bytes, needs_sync) = {
        let State::Enabled { unresolved, .. } = state else {
            return Err(ChangeStreamError::Unavailable(
                "enabled stream disappeared before prepared durability".into(),
            ));
        };
        let mut ordered = unresolved
            .values()
            .map(|record| record.batch.physical_txn_id)
            .collect::<Vec<_>>();
        ordered.sort_by_key(|txn_id| {
            unresolved
                .get(txn_id)
                .map_or(u64::MAX, |record| record.batch.sequence)
        });
        if require_exact_prefix && ordered.len() < candidates.len() {
            return Err(ChangeStreamError::InvalidRecord(
                "prepared durability batch is not an unresolved prefix",
            ));
        }
        let mut record_bytes = 0_u64;
        let mut needs_sync = false;
        for (position, (txn_id, database_txn_id, prepared)) in candidates.iter().enumerate() {
            let record_id = if require_exact_prefix {
                ordered[position]
            } else {
                *txn_id
            };
            let record = unresolved
                .get(&record_id)
                .ok_or(ChangeStreamError::InvalidRecord(
                    "prepared transaction is missing",
                ))?;
            if record.batch.physical_txn_id != *txn_id
                || record.batch.database_txn_id != *database_txn_id
                || prepared_identity(&record.batch).sequence != prepared.sequence
                || record.batch.before != prepared.before
                || record.batch.after != prepared.after
            {
                return Err(ChangeStreamError::InvalidRecord(
                    "prepared durability batch is not the exact change-order prefix",
                ));
            }
            if position != 0 && candidates[position - 1].2.after != prepared.before {
                return Err(ChangeStreamError::ChangeGap {
                    expected: candidates[position - 1].2.after,
                    actual: prepared.before,
                });
            }
            record_bytes = record_bytes
                .checked_add(record.prepared_file_bytes)
                .ok_or(ChangeStreamError::RecordTooLarge(u64::MAX))?;
            needs_sync |= !record.prepared_durable;
        }
        (record_bytes, needs_sync)
    };
    if needs_sync {
        #[cfg(any(test, feature = "test-hooks"))]
        if fail_sync {
            return Err(ChangeStreamError::Io(std::io::Error::other(
                "injected group Prepare sync failure",
            )));
        }
        #[cfg(test)]
        crate::crash_test::maybe_crash_named("change-group-before-prepare-sync");
        let checkpoint = sync_log(state, sync_counts, sync_reason)?;
        #[cfg(test)]
        crate::crash_test::maybe_crash_named("change-group-after-prepare-sync");
        let State::Enabled { unresolved, .. } = state else {
            unreachable!("enabled stream was synchronized");
        };
        for (txn_id, _, _) in candidates {
            let record = unresolved
                .get_mut(txn_id)
                .ok_or(ChangeStreamError::InvalidRecord(
                    "prepared transaction disappeared during durability",
                ))?;
            record.prepared_durable = true;
        }
        return Ok(ChangePrepareBatchReport {
            changing_member_count: candidates.len(),
            records_staged: candidates.len(),
            record_bytes,
            syncs: 1,
            first_sequence: first_prepared.sequence,
            last_sequence: candidates
                .last()
                .map_or(first_prepared.sequence, |candidate| candidate.2.sequence),
            before_frontier: first_prepared.before,
            after_reserved_frontier: candidates
                .last()
                .map_or(first_prepared.after, |candidate| candidate.2.after),
            prior_finalize_checkpoints_checkpointed: checkpoint.prior_finalize_checkpoints,
        });
    }
    Ok(ChangePrepareBatchReport {
        changing_member_count: candidates.len(),
        records_staged: candidates.len(),
        record_bytes,
        syncs: u64::from(needs_sync),
        first_sequence: first_prepared.sequence,
        last_sequence: candidates
            .last()
            .map_or(first_prepared.sequence, |candidate| candidate.2.sequence),
        before_frontier: first_prepared.before,
        after_reserved_frontier: candidates
            .last()
            .map_or(first_prepared.after, |candidate| candidate.2.after),
        prior_finalize_checkpoints_checkpointed: 0,
    })
}

fn sync_log(
    state: &mut State,
    sync_counts: &mut ChangeStreamSyncCounts,
    reason: SyncReason,
) -> Result<SyncOutcome, ChangeStreamError> {
    let State::Enabled {
        file,
        header,
        batches,
        pending_finalize_checkpoints,
        finalize_checkpointed_through,
        ..
    } = state
    else {
        return Err(ChangeStreamError::Unavailable(
            "enabled stream disappeared before durability sync".into(),
        ));
    };
    file.sync_data()?;
    #[cfg(test)]
    crate::crash_test::maybe_crash_named("change-after-sync-before-checkpoint-bookkeeping");
    let pending = pending_finalize_checkpoints.len();
    if pending != 0 {
        *finalize_checkpointed_through = effective_current(header, batches);
        pending_finalize_checkpoints.clear();
        sync_counts.pipelined_finalize_checkpoint =
            sync_counts.pipelined_finalize_checkpoint.saturating_add(1);
        if matches!(reason, SyncReason::MemberPrepare | SyncReason::GroupPrepare) {
            sync_counts.combined_finalize_prepare =
                sync_counts.combined_finalize_prepare.saturating_add(1);
        }
    }
    sync_counts.total = sync_counts.total.saturating_add(1);
    match reason {
        SyncReason::MemberPrepare => {
            sync_counts.member_prepare = sync_counts.member_prepare.saturating_add(1);
        }
        SyncReason::GroupPrepare => {
            sync_counts.group_prepare = sync_counts.group_prepare.saturating_add(1);
        }
        SyncReason::MemberFinalize => {
            sync_counts.member_finalize = sync_counts.member_finalize.saturating_add(1);
        }
        SyncReason::GroupFinalize => {
            sync_counts.group_finalize = sync_counts.group_finalize.saturating_add(1);
        }
        SyncReason::ExplicitFinalizeCheckpoint => {
            sync_counts.explicit_finalize_checkpoint =
                sync_counts.explicit_finalize_checkpoint.saturating_add(1);
        }
    }
    Ok(SyncOutcome {
        prior_finalize_checkpoints: pending,
    })
}

#[derive(Debug)]
struct ValidatedFinalize {
    txn_id: TxnId,
    prepared: PreparedChange,
    lsm_commit: Option<LsmCommitSeq>,
    marker: Vec<u8>,
}

fn finalize_batch(
    state: &mut State,
    sync_counts: &mut ChangeStreamSyncCounts,
    candidates: &[(TxnId, PreparedChange, Option<LsmCommitSeq>)],
    sync_immediately: bool,
    sync_reason: SyncReason,
    #[cfg(any(test, feature = "test-hooks"))] fail_sync: bool,
) -> Result<ChangeFinalizeBatchReport, ChangeStreamError> {
    let Some((_, first_prepared, _)) = candidates.first().copied() else {
        return Err(ChangeStreamError::InvalidRecord(
            "finalize durability batch is empty",
        ));
    };
    let (before_committed_frontier, already_published, pending_before) = match state {
        State::Enabled {
            header,
            batches,
            pending_finalize_checkpoints,
            ..
        } => (
            effective_current(header, batches),
            candidates
                .iter()
                .all(|(txn_id, _, _)| batches.iter().any(|batch| batch.physical_txn_id == *txn_id)),
            pending_finalize_checkpoints.len(),
        ),
        _ => {
            return Err(ChangeStreamError::Unavailable(
                "enabled stream disappeared before publication".into(),
            ));
        }
    };
    if already_published {
        return Ok(ChangeFinalizeBatchReport {
            finalized_member_count: candidates.len(),
            markers_staged: 0,
            marker_bytes: 0,
            syncs: 0,
            before_committed_frontier: first_prepared.before,
            after_committed_frontier: candidates
                .last()
                .map_or(first_prepared.after, |candidate| candidate.1.after),
            pending_finalize_checkpoints_after: pending_before,
        });
    }
    if first_prepared.before != before_committed_frontier {
        return Err(ChangeStreamError::ChangeGap {
            expected: before_committed_frontier,
            actual: first_prepared.before,
        });
    }
    let mut validated = Vec::with_capacity(candidates.len());
    let mut marker_bytes = 0_u64;
    for (position, (txn_id, prepared, lsm_commit)) in candidates.iter().enumerate() {
        if position != 0 && candidates[position - 1].1.after != prepared.before {
            return Err(ChangeStreamError::ChangeGap {
                expected: candidates[position - 1].1.after,
                actual: prepared.before,
            });
        }
        let (record, kind, pending_sequence_exists) = match state {
            State::Enabled {
                header,
                unresolved,
                pending_finalize_checkpoints,
                ..
            } => (
                unresolved
                    .get(txn_id)
                    .ok_or(ChangeStreamError::InvalidRecord(
                        "prepared transaction is missing",
                    ))?,
                header.kind,
                pending_finalize_checkpoints.contains_key(&prepared.sequence),
            ),
            _ => unreachable!("validated enabled change stream"),
        };
        if !sync_immediately && pending_sequence_exists {
            return Err(ChangeStreamError::InvalidRecord(
                "duplicate pending finalize checkpoint sequence",
            ));
        }
        if prepared_identity(&record.batch).sequence != prepared.sequence
            || record.batch.before != prepared.before
            || record.batch.after != prepared.after
            || !record.prepared_durable
        {
            return Err(ChangeStreamError::InvalidRecord(
                "prepared identity is changed or not durable",
            ));
        }
        let mut committed = record.batch.clone();
        if let Some(commit) = *lsm_commit {
            resolve_lsm_versions(&mut committed, commit);
        }
        validate_committed_versions(&committed, kind)?;
        let marker = encode_finalize(*txn_id, *lsm_commit)?;
        marker_bytes = marker_bytes
            .checked_add(marker.len() as u64)
            .ok_or(ChangeStreamError::RecordTooLarge(u64::MAX))?;
        match record.staged_finalize {
            Some(existing) if existing == *lsm_commit => {}
            Some(_) => {
                return Err(ChangeStreamError::InvalidRecord(
                    "finalize commit identity changed",
                ));
            }
            None => {}
        }
        validated.push(ValidatedFinalize {
            txn_id: *txn_id,
            prepared: *prepared,
            lsm_commit: *lsm_commit,
            marker,
        });
    }

    let mut markers_staged = 0_usize;
    for (position, candidate) in validated.iter().enumerate() {
        let State::Enabled {
            file,
            file_bytes,
            unresolved,
            ..
        } = state
        else {
            unreachable!("validated enabled change stream");
        };
        let record =
            unresolved
                .get_mut(&candidate.txn_id)
                .ok_or(ChangeStreamError::InvalidRecord(
                    "prepared transaction disappeared before finalize append",
                ))?;
        if record.staged_finalize.is_none() {
            let following_file_bytes = file_bytes
                .checked_add(candidate.marker.len() as u64)
                .ok_or(ChangeStreamError::RecordTooLarge(u64::MAX))?;
            file.seek(SeekFrom::End(0))?;
            file.write_all(&candidate.marker)?;
            *file_bytes = following_file_bytes;
            record.retained_file_bytes = record
                .retained_file_bytes
                .checked_add(candidate.marker.len() as u64)
                .ok_or(ChangeStreamError::RecordTooLarge(u64::MAX))?;
            record.staged_finalize = Some(candidate.lsm_commit);
            markers_staged = markers_staged.saturating_add(1);
            #[cfg(test)]
            crate::crash_test::maybe_crash_indexed(
                "change-group-after-finalize-append",
                position + 1,
            );
            #[cfg(not(test))]
            let _ = position;
        }
    }

    if sync_immediately {
        #[cfg(any(test, feature = "test-hooks"))]
        if fail_sync {
            return Err(ChangeStreamError::Io(std::io::Error::other(
                "injected group Finalize sync failure",
            )));
        }
        #[cfg(test)]
        crate::crash_test::maybe_crash_named("change-group-before-finalize-sync");
        sync_log(state, sync_counts, sync_reason)?;
        #[cfg(test)]
        crate::crash_test::maybe_crash_named("change-group-after-finalize-sync");
    }

    for (position, candidate) in validated.iter().enumerate() {
        let State::Enabled {
            batches,
            batch_file_bytes,
            finalize_versions,
            unresolved,
            pending_finalize_checkpoints,
            finalize_checkpointed_through,
            ..
        } = state
        else {
            unreachable!("validated enabled change stream");
        };
        let mut record =
            unresolved
                .remove(&candidate.txn_id)
                .ok_or(ChangeStreamError::InvalidRecord(
                    "prepared transaction disappeared during publication",
                ))?;
        if let Some(commit) = candidate.lsm_commit {
            resolve_lsm_versions(&mut record.batch, commit);
        }
        finalize_versions.insert(candidate.txn_id, candidate.lsm_commit);
        batch_file_bytes.push(BatchFileBytes {
            change_bytes: record.prepared_file_bytes,
            retained_file_bytes: record.retained_file_bytes,
        });
        let sequence = record.batch.sequence;
        batches.push(record.batch);
        if sync_immediately {
            *finalize_checkpointed_through = candidate.prepared.after;
        } else {
            let previous = pending_finalize_checkpoints.insert(sequence, candidate.txn_id);
            debug_assert!(
                previous.is_none(),
                "pending Finalize sequence was prevalidated"
            );
        }
        #[cfg(test)]
        crate::crash_test::maybe_crash_indexed(
            "change-group-after-finalize-promotion",
            position + 1,
        );
        #[cfg(not(test))]
        let _ = position;
    }
    let pending_after = match state {
        State::Enabled {
            pending_finalize_checkpoints,
            ..
        } => pending_finalize_checkpoints.len(),
        _ => unreachable!("validated enabled change stream"),
    };
    Ok(ChangeFinalizeBatchReport {
        finalized_member_count: candidates.len(),
        markers_staged,
        marker_bytes,
        syncs: u64::from(sync_immediately),
        before_committed_frontier,
        after_committed_frontier: candidates
            .last()
            .map_or(first_prepared.after, |candidate| candidate.1.after),
        pending_finalize_checkpoints_after: pending_after,
    })
}

fn build_enabled_state(
    header: Header,
    mut file: File,
    mut file_bytes: u64,
    records: Vec<PreparedRecord>,
) -> Result<(State, u64), ChangeStreamError> {
    let mut committed = Vec::new();
    let mut finalize_versions = BTreeMap::new();
    let mut unresolved = BTreeMap::new();
    let mut repair_count = 0_usize;
    let mut max_sequence = 0;
    let mut seen = HashSet::new();
    for mut record in records {
        max_sequence = max_sequence.max(record.batch.sequence);
        if !seen.insert(record.batch.physical_txn_id) {
            return Err(ChangeStreamError::InvalidRecord(
                "duplicate physical transaction identity",
            ));
        }
        match record.outcome {
            AuthoritativeOutcome::Committed(commit) => {
                if header.kind == ChangeStorageKind::Lsm && commit.is_none() {
                    return Err(ChangeStreamError::InvalidRecord(
                        "LSM finalize marker has no commit version",
                    ));
                }
                if let Some(commit) = commit {
                    resolve_lsm_versions(&mut record.batch, commit);
                }
                validate_committed_versions(&record.batch, header.kind)?;
                if !record.finalized {
                    let marker = encode_finalize(record.batch.physical_txn_id, commit)?;
                    file.seek(SeekFrom::End(0))?;
                    file.write_all(&marker)?;
                    file_bytes = file_bytes
                        .checked_add(marker.len() as u64)
                        .ok_or(ChangeStreamError::RecordTooLarge(u64::MAX))?;
                    record.retained_file_bytes = record
                        .retained_file_bytes
                        .checked_add(marker.len() as u64)
                        .ok_or(ChangeStreamError::RecordTooLarge(u64::MAX))?;
                    repair_count = repair_count.saturating_add(1);
                }
                finalize_versions.insert(record.batch.physical_txn_id, commit);
                committed.push((
                    record.batch,
                    record.prepared_file_bytes,
                    record.retained_file_bytes,
                ));
            }
            AuthoritativeOutcome::Aborted => {}
            AuthoritativeOutcome::Unresolved => {
                unresolved.insert(record.batch.physical_txn_id, record);
            }
        }
    }
    if repair_count != 0 {
        file.sync_data()?;
    }
    committed.sort_by_key(|(batch, _, _)| batch.sequence);
    let (batches, batch_file_bytes): (Vec<_>, Vec<_>) = committed
        .into_iter()
        .map(|(batch, change_bytes, retained_file_bytes)| {
            (
                batch,
                BatchFileBytes {
                    change_bytes,
                    retained_file_bytes,
                },
            )
        })
        .unzip();
    validate_committed_chain(header.earliest, &batches)?;
    let effective = effective_current(&header, &batches);
    if batches.is_empty() && header.earliest != header.current {
        return Err(ChangeStreamError::InvalidHeader(
            "empty retained log has unequal earliest and current frontiers",
        ));
    }
    if effective.0 < header.current.0 {
        return Err(ChangeStreamError::InvalidHeader(
            "retained history ends before the durable current frontier",
        ));
    }
    let following_max = max_sequence
        .checked_add(1)
        .ok_or(ChangeStreamError::VersionExhausted)?;
    let next_sequence = header.next_sequence.max(following_max);
    let finalize_checkpointed_through = effective;
    Ok((
        State::Enabled {
            header: Box::new(header),
            file,
            file_bytes,
            batches,
            batch_file_bytes,
            finalize_versions,
            unresolved,
            pending_finalize_checkpoints: BTreeMap::new(),
            finalize_checkpointed_through,
            next_sequence,
        },
        u64::from(repair_count != 0),
    ))
}

fn prepared_identity(batch: &ChangeBatch) -> PreparedChange {
    PreparedChange {
        sequence: batch.sequence,
        before: batch.before,
        after: batch.after,
    }
}

fn cursor_for(header: &Header, frontier: StorageDataVersion) -> ChangeStreamCursor {
    ChangeStreamCursor {
        storage_id: header.storage_id,
        generation: header.generation,
        frontier,
    }
}

fn effective_current(header: &Header, batches: &[ChangeBatch]) -> StorageDataVersion {
    batches.last().map_or(header.current, |batch| batch.after)
}

fn resolve_lsm_versions(batch: &mut ChangeBatch, commit: LsmCommitSeq) {
    for mutation in &mut batch.mutations {
        let key = match mutation {
            StorageChange::Insert { new_version, .. }
            | StorageChange::Update { new_version, .. } => Some(new_version),
            StorageChange::Delete { .. } => None,
        };
        if let Some(StorageVersionKey::Lsm { version, .. }) = key {
            if version.0 == 0 {
                *version = commit;
            }
        }
    }
}

fn validate_committed_chain(
    baseline: StorageDataVersion,
    batches: &[ChangeBatch],
) -> Result<(), ChangeStreamError> {
    let mut expected = baseline;
    for batch in batches {
        if batch.before != expected {
            return Err(ChangeStreamError::ChangeGap {
                expected,
                actual: batch.before,
            });
        }
        if batch.after.0
            != batch
                .before
                .0
                .checked_add(1)
                .ok_or(ChangeStreamError::VersionExhausted)?
        {
            return Err(ChangeStreamError::InvalidRecord(
                "data version does not advance by one",
            ));
        }
        expected = batch.after;
    }
    Ok(())
}

fn rewrite_header(path: &Path, header: &Header) -> Result<(File, u64), ChangeStreamError> {
    let temporary = path.with_extension("change.tmp");
    let bytes = encode_header(header);
    {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
    }
    fs::rename(&temporary, path)?;
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    let file = OpenOptions::new().read(true).write(true).open(path)?;
    Ok((file, bytes.len() as u64))
}

fn rewrite_retained(
    path: &Path,
    header: &Header,
    batches: &[ChangeBatch],
    finalize_versions: &BTreeMap<TxnId, Option<LsmCommitSeq>>,
) -> Result<(File, u64), ChangeStreamError> {
    let temporary = path.with_extension(format!(
        "change.gc.{}.{}.tmp",
        std::process::id(),
        header.earliest.0
    ));
    let result = (|| {
        let mut output = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)?;
        let encoded_header = encode_header(header);
        output.write_all(&encoded_header)?;
        gc_crash("gc-header-written");
        let mut bytes = encoded_header.len() as u64;
        for batch in batches {
            let prepared = encode_record(batch)?;
            output.write_all(&prepared)?;
            bytes = bytes
                .checked_add(prepared.len() as u64)
                .ok_or(ChangeStreamError::RecordTooLarge(u64::MAX))?;
            let commit = finalize_versions
                .get(&batch.physical_txn_id)
                .copied()
                .ok_or(ChangeStreamError::InvalidRecord(
                    "committed batch has no finalize identity",
                ))?;
            let finalize = encode_finalize(batch.physical_txn_id, commit)?;
            output.write_all(&finalize)?;
            bytes = bytes
                .checked_add(finalize.len() as u64)
                .ok_or(ChangeStreamError::RecordTooLarge(u64::MAX))?;
        }
        gc_crash("gc-records-written");
        output.sync_all()?;
        gc_crash("gc-file-synced");
        fs::rename(&temporary, path)?;
        gc_crash("gc-file-renamed");
        if let Some(parent) = path.parent() {
            File::open(parent)?.sync_all()?;
        }
        gc_crash("gc-directory-synced");
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        Ok((file, bytes))
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn write_guard(path: &Path, header: &Header) -> Result<(), ChangeStreamError> {
    let temporary = path.with_extension("active.tmp");
    {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary)?;
        file.write_all(&encode_header(header))?;
        file.sync_all()?;
    }
    fs::rename(&temporary, path)?;
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

fn read_guard(path: &Path) -> Result<Header, ChangeStreamError> {
    decode_header(&fs::read(path)?)
}

fn guard_identity_matches(left: &Header, right: &Header) -> bool {
    left.active
        && right.active
        && left.kind == right.kind
        && left.storage_id == right.storage_id
        && left.table_id == right.table_id
        && left.fingerprint == right.fingerprint
        && left.generation == right.generation
}

fn remove_guard(path: &Path) -> Result<(), ChangeStreamError> {
    match fs::remove_file(path) {
        Ok(()) => {
            if let Some(parent) = path.parent() {
                File::open(parent)?.sync_all()?;
            }
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn encode_header(header: &Header) -> Vec<u8> {
    if header.version == 1 {
        return encode_v1_header(header);
    }
    debug_assert_eq!(header.version, CHANGE_LOG_FORMAT_VERSION);
    let mut bytes = vec![0_u8; V2_HEADER_SIZE];
    bytes[0..4].copy_from_slice(CHANGE_LOG_MAGIC);
    bytes[4..6].copy_from_slice(&header.version.to_le_bytes());
    bytes[6] = u8::from(header.active);
    bytes[7] = header.kind.tag();
    bytes[8..16].copy_from_slice(&header.storage_id.0.to_le_bytes());
    bytes[16..24].copy_from_slice(&header.table_id.0.to_le_bytes());
    bytes[24..56].copy_from_slice(header.fingerprint.as_bytes());
    bytes[56..64].copy_from_slice(&header.generation.0.to_le_bytes());
    bytes[64..72].copy_from_slice(&header.origin.0.to_le_bytes());
    bytes[72..80].copy_from_slice(&header.earliest.0.to_le_bytes());
    bytes[80..88].copy_from_slice(&header.current.0.to_le_bytes());
    bytes[88..96].copy_from_slice(&header.next_sequence.to_le_bytes());
    let checksum = crc32c::crc32c(&bytes[..100]);
    bytes[100..104].copy_from_slice(&checksum.to_le_bytes());
    bytes
}

fn encode_v1_header(header: &Header) -> Vec<u8> {
    let mut bytes = vec![0_u8; V1_HEADER_SIZE];
    bytes[0..4].copy_from_slice(CHANGE_LOG_MAGIC);
    bytes[4..6].copy_from_slice(&1_u16.to_le_bytes());
    bytes[6] = u8::from(header.active);
    bytes[7] = header.kind.tag();
    bytes[8..16].copy_from_slice(&header.storage_id.0.to_le_bytes());
    bytes[16..24].copy_from_slice(&header.table_id.0.to_le_bytes());
    bytes[24..56].copy_from_slice(header.fingerprint.as_bytes());
    bytes[56..64].copy_from_slice(&header.generation.0.to_le_bytes());
    bytes[64..72].copy_from_slice(&header.origin.0.to_le_bytes());
    let checksum = crc32c::crc32c(&bytes[..76]);
    bytes[76..80].copy_from_slice(&checksum.to_le_bytes());
    bytes
}

fn decode_header(bytes: &[u8]) -> Result<Header, ChangeStreamError> {
    if bytes.len() < 6 {
        return Err(ChangeStreamError::Truncated {
            offset: bytes.len() as u64,
        });
    }
    if &bytes[0..4] != CHANGE_LOG_MAGIC {
        return Err(ChangeStreamError::InvalidMagic);
    }
    let version = u16::from_le_bytes([bytes[4], bytes[5]]);
    let expected = match version {
        1 => V1_HEADER_SIZE,
        CHANGE_LOG_FORMAT_VERSION => V2_HEADER_SIZE,
        _ => return Err(ChangeStreamError::UnsupportedVersion(version)),
    };
    if bytes.len() != expected {
        return Err(ChangeStreamError::Truncated {
            offset: bytes.len() as u64,
        });
    }
    let (reserved, checksum_offset) = if version == 1 {
        (&bytes[72..76], 76)
    } else {
        (&bytes[96..100], 100)
    };
    if bytes[6] > 1 || reserved != [0; 4] {
        return Err(ChangeStreamError::InvalidHeader(
            "invalid flags or reserved bytes",
        ));
    }
    if crc32c::crc32c(&bytes[..checksum_offset])
        != u32::from_le_bytes(
            bytes[checksum_offset..checksum_offset + 4]
                .try_into()
                .map_err(|_| ChangeStreamError::Truncated {
                    offset: checksum_offset as u64,
                })?,
        )
    {
        return Err(ChangeStreamError::ChecksumMismatch { offset: 0 });
    }
    let storage_id = StorageId(read_u64(bytes, 8)?);
    let table_id = TableId(read_u64(bytes, 16)?);
    let generation = ChangeStreamGeneration(read_u64(bytes, 56)?);
    if storage_id.0 == 0 || table_id.0 == 0 || generation.0 == 0 {
        return Err(ChangeStreamError::InvalidHeader("zero identity"));
    }
    let origin = StorageDataVersion(read_u64(bytes, 64)?);
    let (earliest, current, next_sequence) = if version == 1 {
        (origin, origin, 1)
    } else {
        (
            StorageDataVersion(read_u64(bytes, 72)?),
            StorageDataVersion(read_u64(bytes, 80)?),
            read_u64(bytes, 88)?,
        )
    };
    if origin.0 > earliest.0 || earliest.0 > current.0 {
        return Err(ChangeStreamError::InvalidHeader(
            "origin, earliest, and current frontiers are out of order",
        ));
    }
    if next_sequence == 0 {
        return Err(ChangeStreamError::InvalidHeader("zero next sequence"));
    }
    Ok(Header {
        version,
        active: bytes[6] == 1,
        kind: ChangeStorageKind::decode(bytes[7])?,
        storage_id,
        table_id,
        fingerprint: SchemaFingerprint::from_bytes(
            bytes[24..56]
                .try_into()
                .map_err(|_| ChangeStreamError::Truncated { offset: 24 })?,
        ),
        generation,
        origin,
        earliest,
        current,
        next_sequence,
    })
}

fn encode_record(batch: &ChangeBatch) -> Result<Vec<u8>, ChangeStreamError> {
    if batch.mutations.is_empty() || batch.mutations.len() > CHANGE_LOG_MAX_MUTATIONS as usize {
        return Err(ChangeStreamError::MutationCountTooLarge(
            batch.mutations.len() as u64,
        ));
    }
    let mut payload = Vec::new();
    payload.push(PREPARED_TAG);
    payload.extend_from_slice(&[0; 7]);
    payload.extend_from_slice(&batch.sequence.to_le_bytes());
    payload.extend_from_slice(&batch.physical_txn_id.0.to_le_bytes());
    payload.extend_from_slice(&batch.database_txn_id.map_or(0, |id| id.0).to_le_bytes());
    payload.extend_from_slice(&batch.table_id.0.to_le_bytes());
    payload.extend_from_slice(&batch.storage_id.0.to_le_bytes());
    payload.extend_from_slice(batch.schema_fingerprint.as_bytes());
    payload.extend_from_slice(&batch.before.0.to_le_bytes());
    payload.extend_from_slice(&batch.after.0.to_le_bytes());
    payload.extend_from_slice(&(batch.mutations.len() as u32).to_le_bytes());
    payload.extend_from_slice(&0_u32.to_le_bytes());
    for mutation in &batch.mutations {
        encode_mutation(&mut payload, mutation, batch.storage_id)?;
    }
    if payload.len() > CHANGE_LOG_MAX_RECORD_BYTES as usize {
        return Err(ChangeStreamError::RecordTooLarge(payload.len() as u64));
    }
    let mut bytes = Vec::with_capacity(payload.len() + 8);
    bytes.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&payload);
    bytes.extend_from_slice(&crc32c::crc32c(&payload).to_le_bytes());
    Ok(bytes)
}

fn encode_finalize(
    txn_id: TxnId,
    lsm_commit: Option<LsmCommitSeq>,
) -> Result<Vec<u8>, ChangeStreamError> {
    if txn_id.0 == 0 {
        return Err(ChangeStreamError::InvalidRecord(
            "zero transaction identity",
        ));
    }
    let mut payload = Vec::with_capacity(24);
    payload.push(FINALIZE_TAG);
    payload.extend_from_slice(&[0; 7]);
    payload.extend_from_slice(&txn_id.0.to_le_bytes());
    payload.extend_from_slice(&lsm_commit.map_or(0, |version| version.0).to_le_bytes());
    let mut bytes = Vec::with_capacity(32);
    bytes.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&payload);
    bytes.extend_from_slice(&crc32c::crc32c(&payload).to_le_bytes());
    Ok(bytes)
}

fn encode_mutation(
    output: &mut Vec<u8>,
    mutation: &StorageChange,
    storage_id: StorageId,
) -> Result<(), ChangeStreamError> {
    match mutation {
        StorageChange::Insert { new_version, after } => {
            output.push(INSERT_TAG);
            encode_key(output, *new_version, storage_id)?;
            encode_after(output, after)?;
        }
        StorageChange::Update {
            old_version,
            new_version,
            after,
        } => {
            output.push(UPDATE_TAG);
            encode_key(output, *old_version, storage_id)?;
            encode_key(output, *new_version, storage_id)?;
            encode_after(output, after)?;
        }
        StorageChange::Delete { old_version } => {
            output.push(DELETE_TAG);
            encode_key(output, *old_version, storage_id)?;
        }
    }
    Ok(())
}

fn encode_key(
    output: &mut Vec<u8>,
    key: StorageVersionKey,
    storage_id: StorageId,
) -> Result<(), ChangeStreamError> {
    if key.storage_id() != storage_id {
        return Err(ChangeStreamError::ContextMismatch);
    }
    match key {
        StorageVersionKey::Heap { row_id, .. } => {
            if row_id.generation == 0 {
                return Err(ChangeStreamError::InvalidRecord("zero Heap row generation"));
            }
            output.push(HEAP_TAG);
            output.extend_from_slice(&row_id.page.0.to_le_bytes());
            output.extend_from_slice(&row_id.slot.to_le_bytes());
            output.extend_from_slice(&row_id.generation.to_le_bytes());
        }
        StorageVersionKey::Lsm {
            row_id, version, ..
        } => {
            if row_id.0 == 0 {
                return Err(ChangeStreamError::InvalidRecord("zero LSM row identity"));
            }
            output.push(LSM_TAG);
            output.extend_from_slice(&row_id.0.to_le_bytes());
            output.extend_from_slice(&version.0.to_le_bytes());
        }
    }
    Ok(())
}

fn encode_after(output: &mut Vec<u8>, after: &[ScalarValue]) -> Result<(), ChangeStreamError> {
    let row =
        encode_row(after).map_err(|_| ChangeStreamError::InvalidRecord("row encoding failed"))?;
    if row.len() > CHANGE_LOG_MAX_ROW_BYTES as usize {
        return Err(ChangeStreamError::RowPayloadTooLarge(row.len() as u64));
    }
    output.extend_from_slice(&(row.len() as u32).to_le_bytes());
    output.extend_from_slice(&row);
    Ok(())
}

fn load_file<F>(
    path: &Path,
    table: &TableDef,
    outcome: &F,
    recover_partial_tail: bool,
) -> Result<(Header, File, u64, Vec<PreparedRecord>), ChangeStreamError>
where
    F: Fn(TxnId) -> AuthoritativeOutcome,
{
    let mut file = OpenOptions::new().read(true).write(true).open(path)?;
    let mut file_bytes = file.metadata()?.len();
    let mut prefix = [0_u8; 6];
    file.read_exact(&mut prefix)
        .map_err(|error| map_truncated(error, 0))?;
    if &prefix[..4] != CHANGE_LOG_MAGIC {
        return Err(ChangeStreamError::InvalidMagic);
    }
    let version = u16::from_le_bytes([prefix[4], prefix[5]]);
    let header_size = match version {
        1 => V1_HEADER_SIZE,
        CHANGE_LOG_FORMAT_VERSION => V2_HEADER_SIZE,
        _ => return Err(ChangeStreamError::UnsupportedVersion(version)),
    };
    file.seek(SeekFrom::Start(0))?;
    let mut header_bytes = vec![0_u8; header_size];
    file.read_exact(&mut header_bytes)
        .map_err(|error| map_truncated(error, 0))?;
    let header = decode_header(&header_bytes)?;
    if !header.active {
        return Ok((header, file, file_bytes, Vec::new()));
    }
    let mut records = Vec::<PreparedRecord>::new();
    let mut offset = header_size as u64;
    while offset < file_bytes {
        let mut length_bytes = [0_u8; 4];
        if let Err(error) = file.read_exact(&mut length_bytes) {
            if recover_partial_tail && error.kind() == std::io::ErrorKind::UnexpectedEof {
                file.set_len(offset)?;
                file.sync_all()?;
                file_bytes = offset;
                break;
            }
            return Err(map_truncated(error, offset));
        }
        let length = u32::from_le_bytes(length_bytes);
        if length == 0 || length > CHANGE_LOG_MAX_RECORD_BYTES {
            return Err(ChangeStreamError::RecordTooLarge(u64::from(length)));
        }
        let mut payload = vec![0_u8; length as usize];
        if let Err(error) = file.read_exact(&mut payload) {
            if recover_partial_tail && error.kind() == std::io::ErrorKind::UnexpectedEof {
                file.set_len(offset)?;
                file.sync_all()?;
                file_bytes = offset;
                break;
            }
            return Err(map_truncated(error, offset));
        }
        let mut checksum = [0_u8; 4];
        if let Err(error) = file.read_exact(&mut checksum) {
            if recover_partial_tail && error.kind() == std::io::ErrorKind::UnexpectedEof {
                file.set_len(offset)?;
                file.sync_all()?;
                file_bytes = offset;
                break;
            }
            return Err(map_truncated(error, offset));
        }
        if crc32c::crc32c(&payload) != u32::from_le_bytes(checksum) {
            return Err(ChangeStreamError::ChecksumMismatch { offset });
        }
        match payload.first().copied() {
            Some(PREPARED_TAG) => {
                let batch = decode_record(&payload, &header, table)?;
                let record_file_bytes = 8_u64 + u64::from(length);
                records.push(PreparedRecord {
                    outcome: outcome(batch.physical_txn_id),
                    batch,
                    finalized: false,
                    prepared_durable: true,
                    staged_finalize: None,
                    prepared_file_bytes: record_file_bytes,
                    retained_file_bytes: record_file_bytes,
                });
            }
            Some(FINALIZE_TAG) => {
                let (txn_id, commit) = decode_finalize(&payload)?;
                let record = records
                    .iter_mut()
                    .find(|record| record.batch.physical_txn_id == txn_id)
                    .ok_or(ChangeStreamError::InvalidRecord(
                        "finalize marker has no prepared batch",
                    ))?;
                if record.finalized {
                    return Err(ChangeStreamError::InvalidRecord(
                        "duplicate finalize marker",
                    ));
                }
                record.finalized = true;
                record.outcome = AuthoritativeOutcome::Committed(commit);
                record.retained_file_bytes = record
                    .retained_file_bytes
                    .checked_add(8_u64 + u64::from(length))
                    .ok_or(ChangeStreamError::RecordTooLarge(u64::MAX))?;
            }
            _ => return Err(ChangeStreamError::InvalidRecord("unknown record tag")),
        }
        offset = offset
            .checked_add(8 + u64::from(length))
            .ok_or(ChangeStreamError::RecordTooLarge(u64::MAX))?;
    }
    Ok((header, file, file_bytes, records))
}

fn decode_finalize(payload: &[u8]) -> Result<(TxnId, Option<LsmCommitSeq>), ChangeStreamError> {
    if payload.len() != 24 || payload[0] != FINALIZE_TAG || payload[1..8] != [0; 7] {
        return Err(ChangeStreamError::InvalidRecord("invalid finalize marker"));
    }
    let txn_id = TxnId(read_u64(payload, 8)?);
    if txn_id.0 == 0 {
        return Err(ChangeStreamError::InvalidRecord(
            "zero transaction identity",
        ));
    }
    let commit = read_u64(payload, 16)?;
    Ok((txn_id, (commit != 0).then_some(LsmCommitSeq(commit))))
}

fn map_truncated(error: std::io::Error, offset: u64) -> ChangeStreamError {
    if error.kind() == std::io::ErrorKind::UnexpectedEof {
        ChangeStreamError::Truncated { offset }
    } else {
        ChangeStreamError::Io(error)
    }
}

fn decode_record(
    payload: &[u8],
    header: &Header,
    table: &TableDef,
) -> Result<ChangeBatch, ChangeStreamError> {
    const FIXED: usize = 104;
    if payload.len() < FIXED {
        return Err(ChangeStreamError::Truncated { offset: 0 });
    }
    if payload[0] != PREPARED_TAG {
        return Err(ChangeStreamError::InvalidRecord("unknown record tag"));
    }
    if payload[1..8] != [0; 7] {
        return Err(ChangeStreamError::InvalidRecord(
            "record reserved bytes are non-zero",
        ));
    }
    let sequence = read_u64(payload, 8)?;
    let physical_txn_id = TxnId(read_u64(payload, 16)?);
    let database = read_u64(payload, 24)?;
    let table_id = TableId(read_u64(payload, 32)?);
    let storage_id = StorageId(read_u64(payload, 40)?);
    let fingerprint = SchemaFingerprint::from_bytes(
        payload[48..80]
            .try_into()
            .map_err(|_| ChangeStreamError::Truncated { offset: 48 })?,
    );
    let before = StorageDataVersion(read_u64(payload, 80)?);
    let after = StorageDataVersion(read_u64(payload, 88)?);
    let count = u32::from_le_bytes(
        payload[96..100]
            .try_into()
            .map_err(|_| ChangeStreamError::Truncated { offset: 96 })?,
    );
    if payload[100..104] != [0; 4] {
        return Err(ChangeStreamError::InvalidRecord(
            "record reserved bytes are non-zero",
        ));
    }
    if sequence == 0 || physical_txn_id.0 == 0 {
        return Err(ChangeStreamError::InvalidRecord("zero record identity"));
    }
    if table_id != header.table_id
        || storage_id != header.storage_id
        || fingerprint != header.fingerprint
    {
        return Err(ChangeStreamError::InvalidRecord("record context mismatch"));
    }
    if count == 0 || count > CHANGE_LOG_MAX_MUTATIONS {
        return Err(ChangeStreamError::MutationCountTooLarge(u64::from(count)));
    }
    let mut offset = FIXED;
    let mut mutations = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let mutation = decode_mutation(payload, &mut offset, storage_id, table)?;
        validate_pending_mutation(&mutation, header.kind)?;
        mutations.push(mutation);
    }
    if offset != payload.len() {
        return Err(ChangeStreamError::InvalidRecord("trailing record bytes"));
    }
    Ok(ChangeBatch {
        sequence,
        physical_txn_id,
        database_txn_id: (database != 0).then_some(DatabaseTxnId(database)),
        table_id,
        storage_id,
        schema_fingerprint: fingerprint,
        before,
        after,
        mutations,
    })
}

fn decode_mutation(
    payload: &[u8],
    offset: &mut usize,
    storage_id: StorageId,
    table: &TableDef,
) -> Result<StorageChange, ChangeStreamError> {
    match take(payload, offset, 1)?[0] {
        INSERT_TAG => Ok(StorageChange::Insert {
            new_version: decode_key(payload, offset, storage_id)?,
            after: decode_after(payload, offset, table)?,
        }),
        UPDATE_TAG => Ok(StorageChange::Update {
            old_version: decode_key(payload, offset, storage_id)?,
            new_version: decode_key(payload, offset, storage_id)?,
            after: decode_after(payload, offset, table)?,
        }),
        DELETE_TAG => Ok(StorageChange::Delete {
            old_version: decode_key(payload, offset, storage_id)?,
        }),
        _ => Err(ChangeStreamError::InvalidRecord("unknown mutation tag")),
    }
}

fn decode_key(
    payload: &[u8],
    offset: &mut usize,
    storage_id: StorageId,
) -> Result<StorageVersionKey, ChangeStreamError> {
    match take(payload, offset, 1)?[0] {
        HEAP_TAG => {
            let page = PageId(read_take_u64(payload, offset)?);
            let slot = u16::from_le_bytes(take(payload, offset, 2)?.try_into().map_err(|_| {
                ChangeStreamError::Truncated {
                    offset: *offset as u64,
                }
            })?);
            let generation =
                u32::from_le_bytes(take(payload, offset, 4)?.try_into().map_err(|_| {
                    ChangeStreamError::Truncated {
                        offset: *offset as u64,
                    }
                })?);
            if generation == 0 {
                return Err(ChangeStreamError::InvalidRecord("zero Heap row generation"));
            }
            Ok(StorageVersionKey::Heap {
                storage_id,
                row_id: RowId {
                    page,
                    slot,
                    generation,
                },
            })
        }
        LSM_TAG => {
            let row_id = LsmRowId(read_take_u64(payload, offset)?);
            let version = LsmCommitSeq(read_take_u64(payload, offset)?);
            if row_id.0 == 0 {
                return Err(ChangeStreamError::InvalidRecord("zero LSM row identity"));
            }
            Ok(StorageVersionKey::Lsm {
                storage_id,
                row_id,
                version,
            })
        }
        _ => Err(ChangeStreamError::InvalidRecord("invalid row-version tag")),
    }
}

fn decode_after(
    payload: &[u8],
    offset: &mut usize,
    table: &TableDef,
) -> Result<Vec<ScalarValue>, ChangeStreamError> {
    let length = u32::from_le_bytes(take(payload, offset, 4)?.try_into().map_err(|_| {
        ChangeStreamError::Truncated {
            offset: *offset as u64,
        }
    })?);
    if length > CHANGE_LOG_MAX_ROW_BYTES {
        return Err(ChangeStreamError::RowPayloadTooLarge(u64::from(length)));
    }
    let bytes = take(payload, offset, length as usize)?;
    decode_row(bytes, table)
        .map_err(|_| ChangeStreamError::InvalidRecord("invalid canonical row payload"))
}

fn validate_pending_mutation(
    mutation: &StorageChange,
    kind: ChangeStorageKind,
) -> Result<(), ChangeStreamError> {
    let validate_key = |key: StorageVersionKey, allow_pending: bool| match (kind, key) {
        (ChangeStorageKind::Heap, StorageVersionKey::Heap { .. }) => Ok(()),
        (ChangeStorageKind::Lsm, StorageVersionKey::Lsm { version, .. })
            if allow_pending || version.0 != 0 =>
        {
            Ok(())
        }
        (ChangeStorageKind::Lsm, StorageVersionKey::Lsm { .. }) => Err(
            ChangeStreamError::InvalidRecord("zero committed LSM version identity"),
        ),
        _ => Err(ChangeStreamError::InvalidRecord(
            "row-version kind differs from storage engine",
        )),
    };
    match mutation {
        StorageChange::Insert { new_version, .. } => validate_key(*new_version, true),
        StorageChange::Update {
            old_version,
            new_version,
            ..
        } => {
            validate_key(*old_version, false)?;
            validate_key(*new_version, true)
        }
        StorageChange::Delete { old_version } => validate_key(*old_version, false),
    }
}

fn validate_committed_versions(
    batch: &ChangeBatch,
    kind: ChangeStorageKind,
) -> Result<(), ChangeStreamError> {
    for mutation in &batch.mutations {
        validate_pending_mutation(mutation, kind)?;
        let new = match mutation {
            StorageChange::Insert { new_version, .. }
            | StorageChange::Update { new_version, .. } => Some(*new_version),
            StorageChange::Delete { .. } => None,
        };
        if matches!(new, Some(StorageVersionKey::Lsm { version, .. }) if version.0 == 0) {
            return Err(ChangeStreamError::InvalidRecord(
                "zero committed LSM version identity",
            ));
        }
    }
    Ok(())
}

fn take<'a>(
    bytes: &'a [u8],
    offset: &mut usize,
    count: usize,
) -> Result<&'a [u8], ChangeStreamError> {
    let end = offset
        .checked_add(count)
        .ok_or(ChangeStreamError::RecordTooLarge(u64::MAX))?;
    let value = bytes
        .get(*offset..end)
        .ok_or(ChangeStreamError::Truncated {
            offset: *offset as u64,
        })?;
    *offset = end;
    Ok(value)
}

fn read_take_u64(bytes: &[u8], offset: &mut usize) -> Result<u64, ChangeStreamError> {
    let start = *offset as u64;
    Ok(u64::from_le_bytes(
        take(bytes, offset, 8)?
            .try_into()
            .map_err(|_| ChangeStreamError::Truncated { offset: start })?,
    ))
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64, ChangeStreamError> {
    Ok(u64::from_le_bytes(
        bytes
            .get(offset..offset + 8)
            .ok_or(ChangeStreamError::Truncated {
                offset: offset as u64,
            })?
            .try_into()
            .map_err(|_| ChangeStreamError::Truncated {
                offset: offset as u64,
            })?,
    ))
}

#[cfg(test)]
fn gc_crash(point: &str) {
    if std::env::var("NETBADB_CHANGE_STREAM_GC_CRASH_POINT").as_deref() == Ok(point) {
        std::process::exit(89);
    }
}

#[cfg(not(test))]
fn gc_crash(_: &str) {}

#[must_use]
pub fn heap_change_log_path(path: impl AsRef<Path>) -> PathBuf {
    let mut value = path.as_ref().as_os_str().to_owned();
    value.push(".change");
    PathBuf::from(value)
}

#[must_use]
pub fn lsm_change_log_path(root: impl AsRef<Path>) -> PathBuf {
    root.as_ref().join("change.nbcl")
}

#[must_use]
pub fn change_stream_guard_path(path: impl AsRef<Path>) -> PathBuf {
    let mut value = path.as_ref().as_os_str().to_owned();
    value.push(".active");
    PathBuf::from(value)
}

/// Strictly validates an NBCL file without modifying or recovering it.
pub fn validate_change_log_file(
    path: impl AsRef<Path>,
    table: &TableDef,
) -> Result<(), ChangeStreamError> {
    let (header, file, file_bytes, records) = load_file(
        path.as_ref(),
        table,
        &|_| AuthoritativeOutcome::Unresolved,
        false,
    )?;
    if header.table_id != table.id
        || header.fingerprint
            != table.fingerprint().map_err(|_| {
                ChangeStreamError::InvalidHeader("schema fingerprint cannot be computed")
            })?
    {
        return Err(ChangeStreamError::InvalidHeader("schema identity mismatch"));
    }
    if header.active {
        let _ = build_enabled_state(header, file, file_bytes, records)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use netbadb_schema::{ColumnDef, TypeSpec};
    use netbadb_types::{ColumnId, PhysicalType};

    fn table() -> TableDef {
        TableDef::new(
            TableId(9),
            "changes",
            vec![ColumnDef::new(
                ColumnId(1),
                "value",
                TypeSpec::Physical(PhysicalType::Int64),
            )],
        )
    }

    fn header() -> Header {
        let table = table();
        Header {
            version: CHANGE_LOG_FORMAT_VERSION,
            active: true,
            kind: ChangeStorageKind::Heap,
            storage_id: StorageId(8),
            table_id: table.id,
            fingerprint: table.fingerprint().unwrap(),
            generation: ChangeStreamGeneration(3),
            origin: StorageDataVersion(11),
            earliest: StorageDataVersion(11),
            current: StorageDataVersion(11),
            next_sequence: 1,
        }
    }

    fn batch() -> ChangeBatch {
        let header = header();
        ChangeBatch {
            sequence: 1,
            physical_txn_id: TxnId(4),
            database_txn_id: Some(DatabaseTxnId(5)),
            table_id: header.table_id,
            storage_id: header.storage_id,
            schema_fingerprint: header.fingerprint,
            before: header.current,
            after: StorageDataVersion(12),
            mutations: vec![StorageChange::Insert {
                new_version: StorageVersionKey::Heap {
                    storage_id: header.storage_id,
                    row_id: RowId {
                        page: PageId(2),
                        slot: 1,
                        generation: 7,
                    },
                },
                after: vec![ScalarValue::Int64(42)],
            }],
        }
    }

    #[test]
    fn header_rejects_magic_version_checksum_reserved_and_zero_identity() {
        let valid = encode_header(&header());
        let cases = [
            (0, b'X'),
            (4, 99),
            (96, 1),
            (8, 0),
            (16, 0),
            (24, valid[24] ^ 1),
        ];
        for (offset, value) in cases {
            let mut bytes = valid.clone();
            bytes[offset] = value;
            assert!(decode_header(&bytes).is_err(), "offset {offset}");
        }
        assert!(matches!(
            decode_header(&valid[..20]),
            Err(ChangeStreamError::Truncated { .. })
        ));
    }

    #[test]
    fn v1_header_remains_readable_and_v2_frontiers_are_validated() {
        let expected = header();
        let decoded = decode_header(&encode_v1_header(&expected)).expect("decode NBCL v1");
        assert_eq!(decoded.version, 1);
        assert_eq!(decoded.origin, expected.origin);
        assert_eq!(decoded.earliest, expected.origin);
        assert_eq!(decoded.current, expected.origin);
        assert_eq!(decoded.next_sequence, 1);

        let mut invalid_order = encode_header(&expected);
        invalid_order[72..80].copy_from_slice(&StorageDataVersion(12).0.to_le_bytes());
        invalid_order[80..88].copy_from_slice(&StorageDataVersion(11).0.to_le_bytes());
        let checksum = crc32c::crc32c(&invalid_order[..100]);
        invalid_order[100..104].copy_from_slice(&checksum.to_le_bytes());
        assert!(matches!(
            decode_header(&invalid_order),
            Err(ChangeStreamError::InvalidHeader(
                "origin, earliest, and current frontiers are out of order"
            ))
        ));

        let mut zero_sequence = encode_header(&expected);
        zero_sequence[88..96].fill(0);
        let checksum = crc32c::crc32c(&zero_sequence[..100]);
        zero_sequence[100..104].copy_from_slice(&checksum.to_le_bytes());
        assert!(matches!(
            decode_header(&zero_sequence),
            Err(ChangeStreamError::InvalidHeader("zero next sequence"))
        ));
    }

    #[test]
    fn record_rejects_checksum_unknown_tags_oversized_counts_and_bad_versions() {
        let table = table();
        let header = header();
        let encoded = encode_record(&batch()).unwrap();
        let payload_length = u32::from_le_bytes(encoded[..4].try_into().unwrap()) as usize;
        let payload = &encoded[4..4 + payload_length];
        assert_eq!(decode_record(payload, &header, &table).unwrap(), batch());

        let mut unknown_record = payload.to_vec();
        unknown_record[0] = 99;
        assert!(decode_record(&unknown_record, &header, &table).is_err());
        let mut unknown_mutation = payload.to_vec();
        unknown_mutation[104] = 99;
        assert!(decode_record(&unknown_mutation, &header, &table).is_err());
        let mut oversized = payload.to_vec();
        oversized[96..100].copy_from_slice(&(CHANGE_LOG_MAX_MUTATIONS + 1).to_le_bytes());
        assert!(matches!(
            decode_record(&oversized, &header, &table),
            Err(ChangeStreamError::MutationCountTooLarge(_))
        ));
        let mut zero_generation = payload.to_vec();
        zero_generation[116..120].fill(0);
        assert!(decode_record(&zero_generation, &header, &table).is_err());
        let mut invalid_version_tag = payload.to_vec();
        invalid_version_tag[105] = 99;
        assert!(decode_record(&invalid_version_tag, &header, &table).is_err());
        let mut oversized_row = payload.to_vec();
        oversized_row[120..124].copy_from_slice(&(CHANGE_LOG_MAX_ROW_BYTES + 1).to_le_bytes());
        assert!(matches!(
            decode_record(&oversized_row, &header, &table),
            Err(ChangeStreamError::RowPayloadTooLarge(_))
        ));
        let mut wrong_context = payload.to_vec();
        wrong_context[32..40].fill(0);
        assert!(decode_record(&wrong_context, &header, &table).is_err());
        let mut wrong_schema = payload.to_vec();
        wrong_schema[48] ^= 1;
        assert!(decode_record(&wrong_schema, &header, &table).is_err());
        let mut zero_txn = payload.to_vec();
        zero_txn[16..24].fill(0);
        assert!(decode_record(&zero_txn, &header, &table).is_err());
        let mut trailing = payload.to_vec();
        trailing.push(0);
        assert!(decode_record(&trailing, &header, &table).is_err());

        let mut lsm_header = header;
        lsm_header.kind = ChangeStorageKind::Lsm;
        let mut invalid_lsm = batch();
        invalid_lsm.mutations = vec![StorageChange::Delete {
            old_version: StorageVersionKey::Lsm {
                storage_id: lsm_header.storage_id,
                row_id: LsmRowId(0),
                version: LsmCommitSeq(1),
            },
        }];
        assert!(encode_record(&invalid_lsm).is_err());
        invalid_lsm.mutations = vec![StorageChange::Delete {
            old_version: StorageVersionKey::Lsm {
                storage_id: lsm_header.storage_id,
                row_id: LsmRowId(1),
                version: LsmCommitSeq(0),
            },
        }];
        let invalid_lsm = encode_record(&invalid_lsm).unwrap();
        let length = u32::from_le_bytes(invalid_lsm[..4].try_into().unwrap()) as usize;
        assert!(decode_record(&invalid_lsm[4..4 + length], &lsm_header, &table).is_err());
    }

    #[test]
    fn strict_file_validation_rejects_truncation_checksum_and_trailing_garbage() {
        let path = std::env::temp_dir().join(format!(
            "netbadb-change-codec-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let mut bytes = encode_header(&header()).to_vec();
        bytes.extend_from_slice(&encode_record(&batch()).unwrap());
        bytes.extend_from_slice(&encode_finalize(TxnId(4), None).unwrap());
        fs::write(&path, &bytes).unwrap();
        validate_change_log_file(&path, &table()).unwrap();

        fs::write(&path, &bytes[..bytes.len() - 1]).unwrap();
        assert!(matches!(
            validate_change_log_file(&path, &table()),
            Err(ChangeStreamError::Truncated { .. })
        ));
        let mut corrupt = bytes.clone();
        corrupt[V2_HEADER_SIZE + 10] ^= 1;
        fs::write(&path, corrupt).unwrap();
        assert!(matches!(
            validate_change_log_file(&path, &table()),
            Err(ChangeStreamError::ChecksumMismatch { .. })
        ));
        let mut trailing = bytes;
        trailing.push(1);
        fs::write(&path, trailing).unwrap();
        assert!(matches!(
            validate_change_log_file(&path, &table()),
            Err(ChangeStreamError::Truncated { .. })
        ));

        let mut oversized = encode_header(&header()).to_vec();
        oversized.extend_from_slice(&(CHANGE_LOG_MAX_RECORD_BYTES + 1).to_le_bytes());
        fs::write(&path, oversized).unwrap();
        assert!(matches!(
            validate_change_log_file(&path, &table()),
            Err(ChangeStreamError::RecordTooLarge(_))
        ));

        let mut duplicate = encode_header(&header()).to_vec();
        duplicate.extend_from_slice(&encode_record(&batch()).unwrap());
        duplicate.extend_from_slice(&encode_record(&batch()).unwrap());
        fs::write(&path, duplicate).unwrap();
        assert!(matches!(
            validate_change_log_file(&path, &table()),
            Err(ChangeStreamError::InvalidRecord(
                "duplicate physical transaction identity"
            ))
        ));

        let different_schema = TableDef::new(
            TableId(9),
            "changes",
            vec![ColumnDef::new(
                ColumnId(1),
                "value",
                TypeSpec::Physical(PhysicalType::Text),
            )],
        );
        fs::write(&path, encode_header(&header())).unwrap();
        assert!(matches!(
            validate_change_log_file(&path, &different_schema),
            Err(ChangeStreamError::InvalidHeader("schema identity mismatch"))
        ));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn committed_chain_detects_gaps() {
        let first = batch();
        let mut second = batch();
        second.sequence = 2;
        second.physical_txn_id = TxnId(6);
        second.before = StorageDataVersion(99);
        second.after = StorageDataVersion(100);
        assert!(matches!(
            validate_committed_chain(StorageDataVersion(11), &[first, second]),
            Err(ChangeStreamError::ChangeGap { .. })
        ));
    }

    #[test]
    fn prepared_reservations_chain_and_require_head_publish_tail_abort() {
        let path = std::env::temp_dir().join(format!(
            "netbadb-change-prepared-chain-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = fs::remove_file(&path);
        fs::write(&path, encode_header(&header())).unwrap();
        let table = table();
        let mut manager = ChangeStreamManager::open(
            path.clone(),
            ChangeStorageKind::Heap,
            StorageId(8),
            &table,
            |_| AuthoritativeOutcome::Unresolved,
        )
        .unwrap();
        let first_change = batch().mutations[0].clone();
        let mut second_change = first_change.clone();
        if let StorageChange::Insert { new_version, .. } = &mut second_change {
            *new_version = StorageVersionKey::Heap {
                storage_id: StorageId(8),
                row_id: RowId {
                    page: PageId(3),
                    slot: 1,
                    generation: 1,
                },
            };
        }
        let first = manager
            .prepare(TxnId(5), Some(DatabaseTxnId(50)), &[first_change])
            .unwrap()
            .unwrap();
        let second = manager
            .prepare(TxnId(6), Some(DatabaseTxnId(60)), &[second_change])
            .unwrap()
            .unwrap();
        assert_eq!(second.before, first.after);
        assert!(manager.publish(TxnId(6), second, None).is_err());
        assert!(manager.abandon(TxnId(5)).is_err());
        manager.abandon(TxnId(6)).unwrap();
        manager.publish(TxnId(5), first, None).unwrap();
        assert_eq!(manager.inspection().prepared_unresolved_count, 0);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn grouped_prepare_and_finalize_barriers_preserve_independent_batches() {
        let path = std::env::temp_dir().join(format!(
            "netbadb-change-group-barriers-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = fs::remove_file(&path);
        fs::write(&path, encode_header(&header())).unwrap();
        let table = table();
        let mut manager = ChangeStreamManager::open(
            path.clone(),
            ChangeStorageKind::Heap,
            StorageId(8),
            &table,
            |_| AuthoritativeOutcome::Unresolved,
        )
        .unwrap();
        let first_change = batch().mutations[0].clone();
        let mut second_change = first_change.clone();
        if let StorageChange::Insert { new_version, .. } = &mut second_change {
            *new_version = StorageVersionKey::Heap {
                storage_id: StorageId(8),
                row_id: RowId {
                    page: PageId(3),
                    slot: 1,
                    generation: 1,
                },
            };
        }
        let first = manager
            .stage_group_prepare(TxnId(5), DatabaseTxnId(50), &[first_change])
            .unwrap()
            .unwrap();
        let second = manager
            .stage_group_prepare(TxnId(6), DatabaseTxnId(60), &[second_change])
            .unwrap()
            .unwrap();
        assert_eq!(manager.sync_count(), 0);
        assert_eq!(manager.inspection().prepared_unresolved_count, 2);
        assert_eq!(manager.inspection().current_data_version, first.before);
        assert!(
            manager
                .durabilize_group_prepared_batch(&[
                    (TxnId(6), DatabaseTxnId(60), second),
                    (TxnId(5), DatabaseTxnId(50), first),
                ])
                .is_err()
        );
        let prepare = manager
            .durabilize_group_prepared_batch(&[
                (TxnId(5), DatabaseTxnId(50), first),
                (TxnId(6), DatabaseTxnId(60), second),
            ])
            .unwrap();
        assert_eq!(prepare.syncs, 1);
        assert_eq!(prepare.first_sequence, first.sequence);
        assert_eq!(prepare.last_sequence, second.sequence);
        assert_eq!(prepare.before_frontier, first.before);
        assert_eq!(prepare.after_reserved_frontier, second.after);
        assert_eq!(manager.inspection().current_data_version, first.before);
        assert!(
            manager
                .finalize_group_batch(&[(TxnId(6), second, None), (TxnId(5), first, None),])
                .is_err()
        );
        let finalize = manager
            .finalize_group_batch(&[(TxnId(5), first, None), (TxnId(6), second, None)])
            .unwrap();
        assert_eq!(finalize.syncs, 1);
        assert_eq!(finalize.finalized_member_count, 2);
        assert_eq!(finalize.before_committed_frontier, first.before);
        assert_eq!(finalize.after_committed_frontier, second.after);
        let inspection = manager.inspection();
        assert_eq!(inspection.prepared_unresolved_count, 0);
        assert_eq!(inspection.current_data_version, second.after);
        assert_eq!(inspection.committed_batch_count, 2);
        let counts = manager.sync_counts();
        assert_eq!(counts.total, 2);
        assert_eq!(counts.member_prepare, 0);
        assert_eq!(counts.group_prepare, 1);
        assert_eq!(counts.member_finalize, 0);
        assert_eq!(counts.group_finalize, 1);
        let mut ordinary_change = batch().mutations[0].clone();
        if let StorageChange::Insert { new_version, .. } = &mut ordinary_change {
            *new_version = StorageVersionKey::Heap {
                storage_id: StorageId(8),
                row_id: RowId {
                    page: PageId(3),
                    slot: 2,
                    generation: 1,
                },
            };
        }
        let ordinary = manager
            .prepare(TxnId(7), Some(DatabaseTxnId(70)), &[ordinary_change])
            .unwrap()
            .unwrap();
        manager.publish(TxnId(7), ordinary, None).unwrap();
        let counts = manager.sync_counts();
        assert_eq!(counts.total, 4);
        assert_eq!(counts.member_prepare, 1);
        assert_eq!(counts.group_prepare, 1);
        assert_eq!(counts.member_finalize, 1);
        assert_eq!(counts.group_finalize, 1);
        drop(manager);

        let reopened = ChangeStreamManager::open(
            path.clone(),
            ChangeStorageKind::Heap,
            StorageId(8),
            &table,
            |_| AuthoritativeOutcome::Unresolved,
        )
        .unwrap();
        assert_eq!(reopened.inspection().committed_batch_count, 3);
        let _ = fs::remove_file(change_stream_guard_path(&path));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn pipelined_finalize_promotes_before_sync_and_next_prepare_checkpoints_it() {
        let path = std::env::temp_dir().join(format!(
            "netbadb-change-finalize-pipeline-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = fs::remove_file(&path);
        fs::write(&path, encode_header(&header())).unwrap();
        let table = table();
        let mut manager = ChangeStreamManager::open(
            path.clone(),
            ChangeStorageKind::Heap,
            StorageId(8),
            &table,
            |_| AuthoritativeOutcome::Unresolved,
        )
        .unwrap();
        let changes = (0..3_u64)
            .map(|number| StorageChange::Insert {
                new_version: StorageVersionKey::Heap {
                    storage_id: StorageId(8),
                    row_id: RowId {
                        page: PageId(20 + number),
                        slot: 1,
                        generation: 1,
                    },
                },
                after: vec![ScalarValue::Int64(number as i64)],
            })
            .collect::<Vec<_>>();
        let first = manager
            .stage_group_prepare(TxnId(10), DatabaseTxnId(110), &changes[..1])
            .unwrap()
            .unwrap();
        let second = manager
            .stage_group_prepare(TxnId(11), DatabaseTxnId(111), &changes[1..2])
            .unwrap()
            .unwrap();
        manager
            .durabilize_group_prepared_batch(&[
                (TxnId(10), DatabaseTxnId(110), first),
                (TxnId(11), DatabaseTxnId(111), second),
            ])
            .unwrap();
        let finalize = manager
            .finalize_group_batch_pipelined(&[(TxnId(10), first, None), (TxnId(11), second, None)])
            .unwrap();
        assert_eq!(finalize.syncs, 0);
        assert_eq!(finalize.pending_finalize_checkpoints_after, 2);
        let inspection = manager.inspection();
        assert_eq!(inspection.current_data_version, second.after);
        assert_eq!(inspection.finalize_checkpointed_through, Some(first.before));
        assert_eq!(inspection.pending_finalize_checkpoint_count, 2);
        assert_eq!(inspection.prepared_unresolved_count, 0);
        let file_bytes_before_retry = inspection.file_bytes;
        let retry = manager
            .finalize_group_batch_pipelined(&[(TxnId(10), first, None), (TxnId(11), second, None)])
            .unwrap();
        assert_eq!(retry.markers_staged, 0);
        assert_eq!(retry.syncs, 0);
        assert_eq!(retry.pending_finalize_checkpoints_after, 2);
        assert_eq!(manager.inspection().file_bytes, file_bytes_before_retry);
        assert!(matches!(
            manager.gc_through(first.after),
            Err(StorageError::ChangeStream(
                ChangeStreamError::FinalizeCheckpointPending
            ))
        ));

        let third = manager
            .stage_group_prepare(TxnId(12), DatabaseTxnId(112), &changes[2..])
            .unwrap()
            .unwrap();
        let prepare = manager
            .durabilize_group_prepared_batch(&[(TxnId(12), DatabaseTxnId(112), third)])
            .unwrap();
        assert_eq!(prepare.syncs, 1);
        assert_eq!(prepare.prior_finalize_checkpoints_checkpointed, 2);
        let inspection = manager.inspection();
        assert_eq!(inspection.finalize_checkpointed_through, Some(second.after));
        assert_eq!(inspection.pending_finalize_checkpoint_count, 0);
        let counts = manager.sync_counts();
        assert_eq!(counts.total, 2);
        assert_eq!(counts.group_prepare, 2);
        assert_eq!(counts.group_finalize, 0);
        assert_eq!(counts.pipelined_finalize_checkpoint, 1);
        assert_eq!(counts.combined_finalize_prepare, 1);
        manager.publish(TxnId(12), third, None).unwrap();
        let counts = manager.sync_counts();
        assert_eq!(counts.total, 3);
        assert_eq!(counts.member_finalize, 1);
        assert_eq!(manager.inspection().current_data_version, third.after);
        let _ = fs::remove_file(change_stream_guard_path(&path));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn explicit_checkpoint_flushes_one_pipelined_finalize_batch() {
        let path = std::env::temp_dir().join(format!(
            "netbadb-change-finalize-explicit-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = fs::remove_file(&path);
        fs::write(&path, encode_header(&header())).unwrap();
        let table = table();
        let mut manager = ChangeStreamManager::open(
            path.clone(),
            ChangeStorageKind::Heap,
            StorageId(8),
            &table,
            |_| AuthoritativeOutcome::Unresolved,
        )
        .unwrap();
        let change = batch().mutations;
        let prepared = manager
            .stage_group_prepare(TxnId(20), DatabaseTxnId(120), &change)
            .unwrap()
            .unwrap();
        manager
            .durabilize_group_prepared_batch(&[(TxnId(20), DatabaseTxnId(120), prepared)])
            .unwrap();
        manager
            .finalize_group_batch_pipelined(&[(TxnId(20), prepared, None)])
            .unwrap();
        assert_eq!(manager.checkpoint_pending_finalizes().unwrap(), 1);
        assert_eq!(manager.checkpoint_pending_finalizes().unwrap(), 0);
        let inspection = manager.inspection();
        assert_eq!(
            inspection.finalize_checkpointed_through,
            Some(prepared.after)
        );
        assert_eq!(inspection.pending_finalize_checkpoint_count, 0);
        let counts = manager.sync_counts();
        assert_eq!(counts.total, 2);
        assert_eq!(counts.explicit_finalize_checkpoint, 1);
        assert_eq!(counts.pipelined_finalize_checkpoint, 1);
        let _ = fs::remove_file(change_stream_guard_path(&path));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn explicit_disable_abandons_a_pending_finalize_checkpoint() {
        let path = std::env::temp_dir().join(format!(
            "netbadb-change-finalize-disable-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = fs::remove_file(&path);
        fs::write(&path, encode_header(&header())).unwrap();
        let table = table();
        let mut manager = ChangeStreamManager::open(
            path.clone(),
            ChangeStorageKind::Heap,
            StorageId(8),
            &table,
            |_| AuthoritativeOutcome::Unresolved,
        )
        .unwrap();
        let changes = batch().mutations;
        let prepared = manager
            .stage_group_prepare(TxnId(25), DatabaseTxnId(125), &changes)
            .unwrap()
            .unwrap();
        manager
            .durabilize_group_prepared_batch(&[(TxnId(25), DatabaseTxnId(125), prepared)])
            .unwrap();
        manager
            .finalize_group_batch_pipelined(&[(TxnId(25), prepared, None)])
            .unwrap();
        assert_eq!(manager.inspection().pending_finalize_checkpoint_count, 1);

        manager.disable().unwrap();
        let inspection = manager.inspection();
        assert_eq!(inspection.status, ChangeStreamStatus::Disabled);
        assert_eq!(inspection.pending_finalize_checkpoint_count, 0);
        assert_eq!(inspection.finalize_checkpointed_through, None);
        drop(manager);

        let reopened = ChangeStreamManager::open(
            path.clone(),
            ChangeStorageKind::Heap,
            StorageId(8),
            &table,
            |_| AuthoritativeOutcome::Committed(None),
        )
        .unwrap();
        assert_eq!(reopened.inspection().status, ChangeStreamStatus::Disabled);
        assert_eq!(reopened.inspection().committed_batch_count, 0);
        let _ = fs::remove_file(change_stream_guard_path(&path));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn explicit_checkpoint_failure_never_uncommits_promoted_change() {
        let path = std::env::temp_dir().join(format!(
            "netbadb-change-finalize-checkpoint-failure-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = fs::remove_file(&path);
        fs::write(&path, encode_header(&header())).unwrap();
        let table = table();
        let mut manager = ChangeStreamManager::open(
            path.clone(),
            ChangeStorageKind::Heap,
            StorageId(8),
            &table,
            |_| AuthoritativeOutcome::Unresolved,
        )
        .unwrap();
        let changes = batch().mutations;
        let prepared = manager
            .stage_group_prepare(TxnId(30), DatabaseTxnId(130), &changes)
            .unwrap()
            .unwrap();
        manager
            .durabilize_group_prepared_batch(&[(TxnId(30), DatabaseTxnId(130), prepared)])
            .unwrap();
        manager
            .finalize_group_batch_pipelined(&[(TxnId(30), prepared, None)])
            .unwrap();
        manager.inject_finalize_checkpoint_sync_failure();
        assert!(manager.checkpoint_pending_finalizes().is_err());
        assert_eq!(manager.inspection().status, ChangeStreamStatus::Unavailable);
        drop(manager);

        let reopened = ChangeStreamManager::open(
            path.clone(),
            ChangeStorageKind::Heap,
            StorageId(8),
            &table,
            |txn_id| {
                assert_eq!(txn_id, TxnId(30));
                AuthoritativeOutcome::Committed(None)
            },
        )
        .unwrap();
        assert_eq!(reopened.inspection().current_data_version, prepared.after);
        assert_eq!(reopened.inspection().committed_batch_count, 1);
        assert_eq!(reopened.inspection().pending_finalize_checkpoint_count, 0);
        let _ = fs::remove_file(change_stream_guard_path(&path));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn recovery_repairs_many_missing_finalizes_with_one_sync() {
        let path = std::env::temp_dir().join(format!(
            "netbadb-change-finalize-recovery-batch-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = fs::remove_file(&path);
        let header = header();
        let mut bytes = encode_header(&header).to_vec();
        for index in 0..1024_u64 {
            let batch = ChangeBatch {
                sequence: index + 1,
                physical_txn_id: TxnId(index + 1),
                database_txn_id: Some(DatabaseTxnId(index + 1)),
                table_id: header.table_id,
                storage_id: header.storage_id,
                schema_fingerprint: header.fingerprint,
                before: StorageDataVersion(header.current.0 + index),
                after: StorageDataVersion(header.current.0 + index + 1),
                mutations: vec![StorageChange::Insert {
                    new_version: StorageVersionKey::Heap {
                        storage_id: header.storage_id,
                        row_id: RowId {
                            page: PageId(index + 1),
                            slot: 1,
                            generation: 1,
                        },
                    },
                    after: vec![ScalarValue::Int64(index as i64)],
                }],
            };
            bytes.extend_from_slice(&encode_record(&batch).unwrap());
        }
        fs::write(&path, bytes).unwrap();
        let table = table();
        let manager = ChangeStreamManager::open(
            path.clone(),
            ChangeStorageKind::Heap,
            StorageId(8),
            &table,
            |_| AuthoritativeOutcome::Committed(None),
        )
        .unwrap();
        let inspection = manager.inspection();
        assert_eq!(inspection.committed_batch_count, 1024);
        assert_eq!(
            inspection.current_data_version,
            StorageDataVersion(header.current.0 + 1024)
        );
        assert_eq!(inspection.pending_finalize_checkpoint_count, 0);
        assert_eq!(manager.sync_counts().total, 1);
        assert_eq!(manager.sync_counts().recovery_finalize, 1);
        drop(manager);

        let reopened = ChangeStreamManager::open(
            path.clone(),
            ChangeStorageKind::Heap,
            StorageId(8),
            &table,
            |_| AuthoritativeOutcome::Committed(None),
        )
        .unwrap();
        assert_eq!(reopened.inspection().committed_batch_count, 1024);
        assert_eq!(reopened.sync_counts().recovery_finalize, 0);
        let _ = fs::remove_file(change_stream_guard_path(&path));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn grouped_finalize_crash_child() {
        let case = std::env::var(crate::crash_test::CASE_ENV).unwrap_or_default();
        if std::env::var_os(crate::crash_test::CHILD_ENV).is_none()
            || !matches!(
                case.as_str(),
                "phase3f-change-finalize" | "phase3g-change-finalize" | "phase3g-change-checkpoint"
            )
        {
            return;
        }
        let path = std::env::var_os(crate::crash_test::DATABASE_PATH_ENV)
            .map(std::path::PathBuf::from)
            .expect("Phase 3F change crash path");
        let mut manager = ChangeStreamManager::open(
            path,
            ChangeStorageKind::Heap,
            StorageId(8),
            &table(),
            |_| AuthoritativeOutcome::Unresolved,
        )
        .unwrap();
        let mut prepared = Vec::new();
        for number in 0..3_u32 {
            let txn_id = TxnId(50 + u64::from(number));
            let database_txn_id = DatabaseTxnId(70 + u64::from(number));
            let change = StorageChange::Insert {
                new_version: StorageVersionKey::Heap {
                    storage_id: StorageId(8),
                    row_id: RowId {
                        page: PageId(10 + u64::from(number)),
                        slot: 1,
                        generation: 1,
                    },
                },
                after: vec![ScalarValue::Int64(i64::from(number))],
            };
            let identity = manager
                .stage_group_prepare(txn_id, database_txn_id, &[change])
                .unwrap()
                .unwrap();
            prepared.push((txn_id, database_txn_id, identity));
        }
        manager.durabilize_group_prepared_batch(&prepared).unwrap();
        let finalize = prepared
            .iter()
            .map(|(txn_id, _, identity)| (*txn_id, *identity, None))
            .collect::<Vec<_>>();
        if case == "phase3g-change-finalize" || case == "phase3g-change-checkpoint" {
            manager.finalize_group_batch_pipelined(&finalize).unwrap();
            if case == "phase3g-change-checkpoint" {
                manager.checkpoint_pending_finalizes().unwrap();
            }
        } else {
            manager.finalize_group_batch(&finalize).unwrap();
        }
        panic!("change finalize child did not reach crash point");
    }

    #[test]
    fn grouped_finalize_crash_windows_recover_exact_individual_batches() {
        for point in [
            "change-group-after-finalize-append-1",
            "change-group-after-finalize-append-2",
            "change-group-after-finalize-append-3",
            "change-group-before-finalize-sync",
            "change-group-after-finalize-sync",
            "change-group-after-finalize-promotion-1",
            "change-group-after-finalize-promotion-2",
        ] {
            let path = std::env::temp_dir().join(format!(
                "netbadb-phase3f-change-finalize-{point}-{}",
                std::process::id()
            ));
            let _ = fs::remove_file(&path);
            fs::write(&path, encode_header(&header())).unwrap();
            let mut command = std::process::Command::new(std::env::current_exe().unwrap());
            command
                .arg("--exact")
                .arg("change_stream::tests::grouped_finalize_crash_child")
                .arg("--nocapture");
            crate::crash_test::configure_named_child(
                &mut command,
                "phase3f-change-finalize",
                &path,
                point,
            );
            let status = command.status().expect("run Phase 3F finalize crash child");
            assert_eq!(status.code(), Some(crate::crash_test::EXIT_CODE));

            let manager = ChangeStreamManager::open(
                path.clone(),
                ChangeStorageKind::Heap,
                StorageId(8),
                &table(),
                |_| AuthoritativeOutcome::Committed(None),
            )
            .unwrap_or_else(|error| panic!("recover {point}: {error}"));
            let changes = manager
                .read(cursor_for(&header(), StorageDataVersion(11)), 10, u64::MAX)
                .unwrap();
            assert_eq!(changes.batches.len(), 3, "point {point}");
            assert_eq!(changes.current_frontier, StorageDataVersion(14));
            assert!(
                changes
                    .batches
                    .windows(2)
                    .all(|pair| pair[0].after == pair[1].before)
            );
            drop(manager);
            let _ = fs::remove_file(path);
        }
    }

    #[test]
    fn pipelined_finalize_append_and_promotion_crashes_recover_exact_batches() {
        for point in [
            "change-group-after-finalize-append-1",
            "change-group-after-finalize-append-2",
            "change-group-after-finalize-append-3",
            "change-group-after-finalize-promotion-1",
            "change-group-after-finalize-promotion-2",
            "change-group-after-finalize-promotion-3",
        ] {
            let path = std::env::temp_dir().join(format!(
                "netbadb-phase3g-change-finalize-{point}-{}",
                std::process::id()
            ));
            let _ = fs::remove_file(&path);
            fs::write(&path, encode_header(&header())).unwrap();
            let mut command = std::process::Command::new(std::env::current_exe().unwrap());
            command
                .arg("--exact")
                .arg("change_stream::tests::grouped_finalize_crash_child")
                .arg("--nocapture");
            crate::crash_test::configure_named_child(
                &mut command,
                "phase3g-change-finalize",
                &path,
                point,
            );
            let status = command.status().expect("run Phase 3G finalize crash child");
            assert_eq!(status.code(), Some(crate::crash_test::EXIT_CODE));

            let manager = ChangeStreamManager::open(
                path.clone(),
                ChangeStorageKind::Heap,
                StorageId(8),
                &table(),
                |_| AuthoritativeOutcome::Committed(None),
            )
            .unwrap_or_else(|error| panic!("recover {point}: {error}"));
            let changes = manager
                .read(cursor_for(&header(), StorageDataVersion(11)), 10, u64::MAX)
                .unwrap();
            assert_eq!(changes.batches.len(), 3, "point {point}");
            assert_eq!(changes.current_frontier, StorageDataVersion(14));
            assert!(
                changes
                    .batches
                    .windows(2)
                    .all(|pair| pair[0].after == pair[1].before)
            );
            drop(manager);
            let _ = fs::remove_file(path);
        }
    }

    #[test]
    fn pipelined_finalize_checkpoint_crashes_recover_committed_batches() {
        for point in [
            "change-before-explicit-finalize-checkpoint-sync",
            "change-after-sync-before-checkpoint-bookkeeping",
            "change-after-explicit-finalize-checkpoint-sync",
        ] {
            let path = std::env::temp_dir().join(format!(
                "netbadb-phase3g-change-checkpoint-{point}-{}",
                std::process::id()
            ));
            let _ = fs::remove_file(&path);
            fs::write(&path, encode_header(&header())).unwrap();
            let mut command = std::process::Command::new(std::env::current_exe().unwrap());
            command
                .arg("--exact")
                .arg("change_stream::tests::grouped_finalize_crash_child")
                .arg("--nocapture");
            crate::crash_test::configure_named_child(
                &mut command,
                "phase3g-change-checkpoint",
                &path,
                point,
            );
            let status = command
                .status()
                .expect("run Phase 3G checkpoint crash child");
            assert_eq!(status.code(), Some(crate::crash_test::EXIT_CODE));
            let manager = ChangeStreamManager::open(
                path.clone(),
                ChangeStorageKind::Heap,
                StorageId(8),
                &table(),
                |_| AuthoritativeOutcome::Committed(None),
            )
            .unwrap_or_else(|error| panic!("recover {point}: {error}"));
            assert_eq!(
                manager.inspection().current_data_version,
                StorageDataVersion(14)
            );
            assert_eq!(manager.inspection().pending_finalize_checkpoint_count, 0);
            drop(manager);
            let _ = fs::remove_file(path);
        }
    }

    #[test]
    fn phase3g_next_group_crash_child() {
        if std::env::var_os(crate::crash_test::CHILD_ENV).is_none()
            || std::env::var_os(crate::crash_test::CASE_ENV).as_deref()
                != Some(std::ffi::OsStr::new("phase3g-next-group"))
        {
            return;
        }
        let path = std::env::var_os(crate::crash_test::DATABASE_PATH_ENV)
            .map(std::path::PathBuf::from)
            .expect("Phase 3G next-group crash path");
        let mut manager = ChangeStreamManager::open(
            path,
            ChangeStorageKind::Heap,
            StorageId(8),
            &table(),
            |_| AuthoritativeOutcome::Unresolved,
        )
        .unwrap();
        let make_change = |number: u64| StorageChange::Insert {
            new_version: StorageVersionKey::Heap {
                storage_id: StorageId(8),
                row_id: RowId {
                    page: PageId(40 + number),
                    slot: 1,
                    generation: 1,
                },
            },
            after: vec![ScalarValue::Int64(number as i64)],
        };
        let mut first_group = Vec::new();
        for number in 0..2_u64 {
            let txn_id = TxnId(90 + number);
            let database_txn_id = DatabaseTxnId(90 + number);
            let prepared = manager
                .stage_group_prepare(txn_id, database_txn_id, &[make_change(number)])
                .unwrap()
                .unwrap();
            first_group.push((txn_id, database_txn_id, prepared));
        }
        crate::crash_test::without_crash(|| {
            manager
                .durabilize_group_prepared_batch(&first_group)
                .unwrap();
        });
        let first_finalize = first_group
            .iter()
            .map(|(txn_id, _, prepared)| (*txn_id, *prepared, None))
            .collect::<Vec<_>>();
        manager
            .finalize_group_batch_pipelined(&first_finalize)
            .unwrap();

        let mut second_group = Vec::new();
        for number in 0..2_u64 {
            let txn_id = TxnId(190 + number);
            let database_txn_id = DatabaseTxnId(190 + number);
            let prepared = manager
                .stage_group_prepare(txn_id, database_txn_id, &[make_change(10 + number)])
                .unwrap()
                .unwrap();
            second_group.push((txn_id, database_txn_id, prepared));
            if number == 0 {
                crate::crash_test::maybe_crash_named("change-phase3g-next-after-prepare-append-1");
            }
        }
        manager
            .durabilize_group_prepared_batch(&second_group)
            .unwrap();
        panic!("Phase 3G next-group child did not reach crash point");
    }

    #[test]
    fn phase3g_next_group_crashes_keep_prior_group_and_abort_current_group() {
        for point in [
            "change-phase3g-next-after-prepare-append-1",
            "change-group-before-prepare-sync",
            "change-after-sync-before-checkpoint-bookkeeping",
            "change-group-after-prepare-sync",
        ] {
            let path = std::env::temp_dir().join(format!(
                "netbadb-phase3g-next-group-{point}-{}",
                std::process::id()
            ));
            let _ = fs::remove_file(&path);
            fs::write(&path, encode_header(&header())).unwrap();
            let mut command = std::process::Command::new(std::env::current_exe().unwrap());
            command
                .arg("--exact")
                .arg("change_stream::tests::phase3g_next_group_crash_child")
                .arg("--nocapture");
            crate::crash_test::configure_named_child(
                &mut command,
                "phase3g-next-group",
                &path,
                point,
            );
            let status = command
                .status()
                .expect("run Phase 3G next-group crash child");
            assert_eq!(status.code(), Some(crate::crash_test::EXIT_CODE));
            let manager = ChangeStreamManager::open(
                path.clone(),
                ChangeStorageKind::Heap,
                StorageId(8),
                &table(),
                |txn_id| {
                    if txn_id.0 < 100 {
                        AuthoritativeOutcome::Committed(None)
                    } else {
                        AuthoritativeOutcome::Aborted
                    }
                },
            )
            .unwrap_or_else(|error| panic!("recover {point}: {error}"));
            let changes = manager
                .read(cursor_for(&header(), StorageDataVersion(11)), 10, u64::MAX)
                .unwrap();
            assert_eq!(changes.batches.len(), 2, "point {point}");
            assert_eq!(changes.current_frontier, StorageDataVersion(13));
            assert_eq!(manager.inspection().prepared_unresolved_count, 0);
            drop(manager);
            let _ = fs::remove_file(path);
        }
    }

    #[test]
    fn grouped_torn_finalize_tail_is_repaired_but_full_checksum_corruption_is_rejected() {
        let path = std::env::temp_dir().join(format!(
            "netbadb-phase3f-torn-finalize-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let mut batches = Vec::new();
        for number in 0..3_u64 {
            let mut current = batch();
            current.sequence = number + 1;
            current.physical_txn_id = TxnId(80 + number);
            current.database_txn_id = Some(DatabaseTxnId(90 + number));
            current.before = StorageDataVersion(11 + number);
            current.after = StorageDataVersion(12 + number);
            if let StorageChange::Insert { new_version, after } = &mut current.mutations[0] {
                *new_version = StorageVersionKey::Heap {
                    storage_id: StorageId(8),
                    row_id: RowId {
                        page: PageId(20 + number),
                        slot: 1,
                        generation: 1,
                    },
                };
                *after = vec![ScalarValue::Int64(number as i64)];
            }
            batches.push(current);
        }
        let mut bytes = encode_header(&header());
        for current in &batches {
            bytes.extend_from_slice(&encode_record(current).unwrap());
        }
        bytes.extend_from_slice(&encode_finalize(TxnId(80), None).unwrap());
        let second_finalize = encode_finalize(TxnId(81), None).unwrap();
        bytes.extend_from_slice(&second_finalize[..second_finalize.len() / 2]);
        fs::write(&path, &bytes).unwrap();

        let manager = ChangeStreamManager::open(
            path.clone(),
            ChangeStorageKind::Heap,
            StorageId(8),
            &table(),
            |_| AuthoritativeOutcome::Committed(None),
        )
        .unwrap();
        let recovered = manager
            .read(cursor_for(&header(), StorageDataVersion(11)), 10, u64::MAX)
            .unwrap();
        assert_eq!(recovered.batches.len(), 3);
        assert_eq!(recovered.current_frontier, StorageDataVersion(14));
        drop(manager);

        let mut corrupt = fs::read(&path).unwrap();
        let final_payload_byte = corrupt.len() - 12;
        corrupt[final_payload_byte] ^= 0x5a;
        fs::write(&path, corrupt).unwrap();
        let corrupt_manager = ChangeStreamManager::open(
            path.clone(),
            ChangeStorageKind::Heap,
            StorageId(8),
            &table(),
            |_| AuthoritativeOutcome::Committed(None),
        )
        .unwrap();
        assert_eq!(
            corrupt_manager.inspection().status,
            ChangeStreamStatus::Unavailable
        );
        let _ = fs::remove_file(path);
    }

    #[test]
    fn batch_debug_and_inspection_do_not_dump_row_payloads() {
        let batch = batch();
        let inspection = batch.inspection();
        assert_eq!(inspection.mutation_count, 1);
        let debug = format!("{batch:?}");
        assert!(debug.contains("mutation_count: 1"));
        assert!(!debug.contains("Int64(42)"));
    }

    fn write_committed_fixture(path: &Path, version: u16, batches: &[ChangeBatch]) {
        let mut fixture_header = header();
        fixture_header.version = version;
        let mut bytes = if version == 1 {
            encode_v1_header(&fixture_header)
        } else {
            encode_header(&fixture_header)
        };
        for batch in batches {
            bytes.extend_from_slice(&encode_record(batch).expect("encode fixture batch"));
            bytes.extend_from_slice(
                &encode_finalize(batch.physical_txn_id, None).expect("encode fixture finalize"),
            );
        }
        fs::write(path, bytes).expect("write committed fixture");
    }

    #[test]
    fn v1_gc_migrates_to_v2_and_future_sequence_stays_monotonic() {
        let path = std::env::temp_dir().join(format!(
            "netbadb-change-v1-gc-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let first = batch();
        write_committed_fixture(&path, 1, std::slice::from_ref(&first));
        let table = table();
        let mut manager = ChangeStreamManager::open(
            path.clone(),
            ChangeStorageKind::Heap,
            StorageId(8),
            &table,
            |_| AuthoritativeOutcome::Unresolved,
        )
        .expect("open v1 stream");
        manager
            .gc_through(first.after)
            .expect("migrate v1 through current");
        let bytes = fs::read(&path).expect("read migrated stream");
        assert_eq!(u16::from_le_bytes([bytes[4], bytes[5]]), 2);
        drop(manager);

        let mut reopened = ChangeStreamManager::open(
            path.clone(),
            ChangeStorageKind::Heap,
            StorageId(8),
            &table,
            |_| AuthoritativeOutcome::Unresolved,
        )
        .expect("reopen migrated stream");
        let change = StorageChange::Insert {
            new_version: StorageVersionKey::Heap {
                storage_id: StorageId(8),
                row_id: RowId {
                    page: PageId(3),
                    slot: 0,
                    generation: 1,
                },
            },
            after: vec![ScalarValue::Int64(99)],
        };
        let prepared = reopened
            .prepare(TxnId(5), None, &[change])
            .expect("prepare post-GC")
            .expect("prepared identity");
        reopened
            .publish(TxnId(5), prepared, None)
            .expect("publish post-GC");
        let cursor = ChangeStreamCursor {
            storage_id: StorageId(8),
            generation: ChangeStreamGeneration(3),
            frontier: first.after,
        };
        let result = reopened.read(cursor, 1, 1_000_000).expect("read post-GC");
        assert_eq!(result.batches[0].sequence, 2);
        assert_eq!(result.batches[0].before, first.after);
        let _ = fs::remove_file(change_stream_guard_path(&path));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn gc_publication_crash_child() {
        if std::env::var("NETBADB_CHANGE_STREAM_GC_CRASH_CHILD").as_deref() != Ok("1") {
            return;
        }
        let path = PathBuf::from(
            std::env::var("NETBADB_CHANGE_STREAM_GC_CRASH_PATH").expect("GC crash path"),
        );
        let table = table();
        let mut manager =
            ChangeStreamManager::open(path, ChangeStorageKind::Heap, StorageId(8), &table, |_| {
                AuthoritativeOutcome::Unresolved
            })
            .expect("open GC crash fixture");
        manager
            .gc_through(StorageDataVersion(12))
            .expect("GC must reach crash point");
        panic!("configured GC crash point did not terminate child");
    }

    #[test]
    fn gc_crash_matrix_reopens_complete_old_or_new_history() {
        let mut second = batch();
        second.sequence = 2;
        second.physical_txn_id = TxnId(6);
        second.before = StorageDataVersion(12);
        second.after = StorageDataVersion(13);
        let points = [
            "gc-header-written",
            "gc-records-written",
            "gc-file-synced",
            "gc-file-renamed",
            "gc-directory-synced",
            "gc-log-published",
            "gc-guard-published",
        ];
        for point in points {
            let path = std::env::temp_dir().join(format!(
                "netbadb-change-gc-crash-{point}-{}",
                std::process::id()
            ));
            write_committed_fixture(&path, CHANGE_LOG_FORMAT_VERSION, &[batch(), second.clone()]);
            let status =
                std::process::Command::new(std::env::current_exe().expect("test executable"))
                    .arg("change_stream::tests::gc_publication_crash_child")
                    .arg("--exact")
                    .arg("--nocapture")
                    .env("NETBADB_CHANGE_STREAM_GC_CRASH_CHILD", "1")
                    .env("NETBADB_CHANGE_STREAM_GC_CRASH_PATH", &path)
                    .env("NETBADB_CHANGE_STREAM_GC_CRASH_POINT", point)
                    .status()
                    .expect("run GC crash child");
            assert_eq!(status.code(), Some(89), "crash point {point}");
            let manager = ChangeStreamManager::open(
                path.clone(),
                ChangeStorageKind::Heap,
                StorageId(8),
                &table(),
                |_| AuthoritativeOutcome::Unresolved,
            )
            .unwrap_or_else(|error| panic!("reopen after {point}: {error}"));
            let earliest = manager
                .inspection()
                .earliest_available_frontier
                .expect("earliest frontier");
            let expected = if matches!(
                point,
                "gc-file-renamed"
                    | "gc-directory-synced"
                    | "gc-log-published"
                    | "gc-guard-published"
            ) {
                StorageDataVersion(12)
            } else {
                StorageDataVersion(11)
            };
            assert_eq!(earliest, expected, "crash point {point}");
            assert_eq!(
                manager.inspection().current_data_version,
                StorageDataVersion(13)
            );
            drop(manager);
            let _ = fs::remove_file(change_stream_guard_path(&path));
            let _ = fs::remove_file(path);
        }
    }
}
