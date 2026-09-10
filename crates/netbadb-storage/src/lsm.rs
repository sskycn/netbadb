//! Synchronous, single-writer LSM table storage.
//!
//! The implementation deliberately has no page manager dependency. Commits
//! are durable mutation batches in an LSM-specific WAL, while flush publishes
//! immutable SSTables through a checksummed manifest generation.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::error::Error;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use netbadb_index::{IndexBound, IndexRange, IndexStatistics, TableStatistics};
use netbadb_schema::{SchemaFingerprint, TableDef};
use netbadb_types::{
    ColumnId, DatabaseTxnId, LsmCommitSeq, LsmRowId, Lsn, PhysicalType, ScalarValue, StorageId,
    TableId, TxnId,
};

use crate::StorageVersionKey;
use crate::change_stream::{
    AuthoritativeOutcome, ChangeStreamManager, PendingChangeSet, PreparedChange,
};
use crate::row_codec::{
    decode_row, decode_row_columns, decode_row_positions, encode_row, resolve_columns, validate_row,
};
use crate::{
    CheckpointError, IsolationLevel, PreparedDecision, PreparedTransaction,
    PreparedTransactionState, PreparedTxnResolution, PresenceCountSummary, RecoveryError,
    StorageError, TransactionError, TransactionState,
};

pub const LSM_MANIFEST_FORMAT_VERSION: u16 = 2;
pub const LSM_WAL_FORMAT_VERSION: u16 = 1;
pub const LSM_SSTABLE_FORMAT_VERSION: u16 = 2;
pub const LSM_MAX_LEVELS: u8 = 4;
pub const LSM_MAX_PENDING_TRANSACTION_BYTES: u64 = 16 * 1024 * 1024;
pub const LSM_MAX_PENDING_MUTATIONS: u64 = 65_536;
pub const DEFAULT_LSM_MEMTABLE_FLUSH_BYTES: u64 = 4 * 1024 * 1024;

const MANIFEST_MAGIC: &[u8; 4] = b"NBLM";
const WAL_MAGIC: &[u8; 4] = b"NBLW";
const WAL_RECORD_MAGIC: &[u8; 4] = b"NBLR";
const SST_MAGIC: &[u8; 4] = b"NBLS";
const SST_BLOCK_MAGIC: &[u8; 4] = b"NBLB";
const SST_FOOTER_MAGIC: &[u8; 4] = b"NBLF";
const MANIFEST_NAME: &str = "MANIFEST";
const MANIFEST_NEXT_NAME: &str = "MANIFEST.next";
const SST_DIR_NAME: &str = "sst";
const MANIFEST_FIXED_SIZE: usize = 176;
const MANIFEST_ENTRY_SIZE: usize = 76;
const MAX_SSTABLES: usize = 4_096;
const WAL_HEADER_SIZE: usize = 32;
const WAL_RECORD_HEADER_SIZE: usize = 20;
const WAL_MAX_RECORD_BYTES: usize = 32 * 1024 * 1024;
const SST_HEADER_SIZE: usize = 160;
const SST_BLOCK_HEADER_SIZE: usize = 52;
const SST_INDEX_ENTRY_SIZE: usize = 56;
const SST_FOOTER_SIZE: usize = 32;
const SST_TARGET_BLOCK_BYTES: usize = 32 * 1024;
const SST_MAX_BLOCK_BYTES: usize = 64 * 1024;
const SST_MAX_BLOCKS: u32 = 262_144;
const SST_TARGET_FILE_BYTES: u64 = 256 * 1024;
const L0_COMPACTION_TRIGGER: usize = 2;
const BASE_LEVEL_BYTES: u64 = 256 * 1024;
const LEVEL_SIZE_MULTIPLIER: u64 = 4;
const BLOOM_ALGORITHM_VERSION: u8 = 1;
const BLOOM_BITS_PER_KEY: u64 = 10;
const BLOOM_HASH_COUNT: u8 = 7;
const BLOOM_MIN_BITS: u64 = 64;
const BLOOM_MAX_BITS: u64 = 64 * 1024 * 1024;
const ALLOCATOR_RESERVATION: u64 = 1_024;
const LSM_ACCESS_PATH_PREFIX: u64 = 0x4c53_4d00_0000_0000;

#[derive(Debug)]
pub enum LsmError {
    InvalidManifest(&'static str),
    ManifestChecksum {
        stored: u32,
        computed: u32,
    },
    UnsupportedManifestVersion(u16),
    InvalidWal(&'static str),
    WalChecksum {
        offset: u64,
        stored: u32,
        computed: u32,
    },
    UnsupportedWalVersion(u16),
    InvalidSstable {
        sstable_id: u64,
        reason: &'static str,
    },
    SstableChecksum {
        sstable_id: u64,
        block: u32,
        stored: u32,
        computed: u32,
    },
    UnsupportedSstableVersion(u16),
    MissingSstable(u64),
    InvalidClusteringColumn(ColumnId),
    UnsupportedClusteringType(PhysicalType),
    StorageIdMismatch {
        expected: StorageId,
        actual: StorageId,
    },
    StaleHandle {
        row_id: LsmRowId,
    },
    RowNotFound(LsmRowId),
    Busy(&'static str),
    AllocatorExhausted(&'static str),
}

impl fmt::Display for LsmError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidManifest(reason) => write!(formatter, "invalid LSM manifest: {reason}"),
            Self::ManifestChecksum { stored, computed } => write!(
                formatter,
                "LSM manifest checksum mismatch: stored {stored:#010x}, computed {computed:#010x}"
            ),
            Self::UnsupportedManifestVersion(version) => {
                write!(formatter, "unsupported LSM manifest version {version}")
            }
            Self::InvalidWal(reason) => write!(formatter, "invalid LSM WAL: {reason}"),
            Self::WalChecksum {
                offset,
                stored,
                computed,
            } => write!(
                formatter,
                "LSM WAL record at {offset} checksum mismatch: stored {stored:#010x}, computed {computed:#010x}"
            ),
            Self::UnsupportedWalVersion(version) => {
                write!(formatter, "unsupported LSM WAL version {version}")
            }
            Self::InvalidSstable { sstable_id, reason } => {
                write!(formatter, "invalid LSM SSTable {sstable_id}: {reason}")
            }
            Self::SstableChecksum {
                sstable_id,
                block,
                stored,
                computed,
            } => write!(
                formatter,
                "LSM SSTable {sstable_id} block {block} checksum mismatch: stored {stored:#010x}, computed {computed:#010x}"
            ),
            Self::UnsupportedSstableVersion(version) => {
                write!(formatter, "unsupported LSM SSTable version {version}")
            }
            Self::MissingSstable(id) => {
                write!(formatter, "manifest references missing SSTable {id}")
            }
            Self::InvalidClusteringColumn(column) => write!(
                formatter,
                "column {} is not a valid NOT NULL LSM clustering column",
                column.0
            ),
            Self::UnsupportedClusteringType(kind) => {
                write!(formatter, "LSM clustering type {kind} is unsupported")
            }
            Self::StorageIdMismatch { expected, actual } => write!(
                formatter,
                "LSM storage ID mismatch: expected {}, found {}",
                expected.0, actual.0
            ),
            Self::StaleHandle { row_id } => {
                write!(formatter, "LSM row handle {} is stale", row_id.0)
            }
            Self::RowNotFound(row_id) => write!(formatter, "LSM row {} does not exist", row_id.0),
            Self::Busy(reason) => {
                write!(formatter, "LSM maintenance requires quiescence: {reason}")
            }
            Self::AllocatorExhausted(name) => write!(formatter, "LSM {name} allocator exhausted"),
        }
    }
}

impl Error for LsmError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum ClusteringKey {
    Int64(i64),
    UInt64(u64),
}

impl ClusteringKey {
    fn from_value(value: &ScalarValue, expected: PhysicalType) -> Result<Self, StorageError> {
        match (value, expected) {
            (ScalarValue::Int64(value), PhysicalType::Int64) => Ok(Self::Int64(*value)),
            (ScalarValue::UInt64(value), PhysicalType::UInt64) => Ok(Self::UInt64(*value)),
            (ScalarValue::Null, _) => Err(LsmError::InvalidClusteringColumn(ColumnId(0)).into()),
            (value, expected) => Err(StorageError::TypeMismatch {
                column: "LSM clustering key".into(),
                expected,
                actual: value.physical_type(),
            }),
        }
    }

    const fn kind(self) -> PhysicalType {
        match self {
            Self::Int64(_) => PhysicalType::Int64,
            Self::UInt64(_) => PhysicalType::UInt64,
        }
    }

    const fn bits(self) -> u64 {
        match self {
            Self::Int64(value) => value as u64,
            Self::UInt64(value) => value,
        }
    }

    fn into_value(self) -> ScalarValue {
        match self {
            Self::Int64(value) => ScalarValue::Int64(value),
            Self::UInt64(value) => ScalarValue::UInt64(value),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct PhysicalKey {
    clustering: ClusteringKey,
    row_id: LsmRowId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum EntryValue {
    Put(Vec<u8>),
    Tombstone,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct VersionedEntry {
    key: PhysicalKey,
    version: LsmCommitSeq,
    value: EntryValue,
}

#[derive(Debug, Clone)]
struct PendingRow {
    original_key: Option<ClusteringKey>,
    current_key: ClusteringKey,
    row: Option<Vec<u8>>,
    base_version: Option<LsmCommitSeq>,
    revision: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum LsmObservedVersion {
    Committed(LsmCommitSeq),
    Pending(u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct LsmRowHandle {
    pub(crate) row_id: LsmRowId,
    pub(crate) observed: LsmObservedVersion,
    pub(crate) clustering_key: ScalarKeyHandle,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum ScalarKeyHandle {
    Int64(i64),
    UInt64(u64),
}

impl From<ClusteringKey> for ScalarKeyHandle {
    fn from(value: ClusteringKey) -> Self {
        match value {
            ClusteringKey::Int64(value) => Self::Int64(value),
            ClusteringKey::UInt64(value) => Self::UInt64(value),
        }
    }
}

impl From<ScalarKeyHandle> for ClusteringKey {
    fn from(value: ScalarKeyHandle) -> Self {
        match value {
            ScalarKeyHandle::Int64(value) => Self::Int64(value),
            ScalarKeyHandle::UInt64(value) => Self::UInt64(value),
        }
    }
}

#[derive(Debug, Clone)]
struct SstableRef {
    id: u64,
    level: u8,
    entry_count: u64,
    file_bytes: u64,
    bloom_bytes: u64,
    min: PhysicalKey,
    max: PhysicalKey,
}

#[derive(Debug, Clone)]
struct BlockMeta {
    offset: u64,
    payload_length: u32,
    entry_count: u32,
    first: PhysicalKey,
    last: PhysicalKey,
}

#[derive(Debug, Clone)]
struct Sstable {
    reference: SstableRef,
    path: PathBuf,
    blocks: Vec<BlockMeta>,
    bloom: BloomFilter,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BloomFilter {
    algorithm: u8,
    bit_count: u64,
    hash_count: u8,
    bits: Vec<u8>,
}

impl BloomFilter {
    fn empty() -> Self {
        Self {
            algorithm: BLOOM_ALGORITHM_VERSION,
            bit_count: 0,
            hash_count: 0,
            bits: Vec::new(),
        }
    }

    fn build(entries: &[VersionedEntry]) -> Result<Self, StorageError> {
        let distinct = entries
            .iter()
            .map(|entry| entry.key.clustering)
            .collect::<BTreeSet<_>>();
        if distinct.is_empty() {
            return Ok(Self::empty());
        }
        let mut filter = Self::for_distinct_keys(u64::try_from(distinct.len()).map_err(|_| {
            LsmError::InvalidSstable {
                sstable_id: 0,
                reason: "Bloom key count does not fit u64",
            }
        })?)?;
        for key in distinct {
            filter.insert(key);
        }
        Ok(filter)
    }

    fn for_distinct_keys(distinct: u64) -> Result<Self, StorageError> {
        if distinct == 0 {
            return Ok(Self::empty());
        }
        let requested =
            distinct
                .checked_mul(BLOOM_BITS_PER_KEY)
                .ok_or(LsmError::InvalidSstable {
                    sstable_id: 0,
                    reason: "Bloom bit count overflows",
                })?;
        let bit_count = requested
            .max(BLOOM_MIN_BITS)
            .div_ceil(8)
            .checked_mul(8)
            .ok_or(LsmError::InvalidSstable {
                sstable_id: 0,
                reason: "Bloom byte alignment overflows",
            })?;
        if bit_count > BLOOM_MAX_BITS {
            return Err(StorageError::ResourceLimit {
                resource: "LSM Bloom bits",
                limit: BLOOM_MAX_BITS,
            });
        }
        let byte_count =
            usize::try_from(bit_count / 8).map_err(|_| StorageError::ResourceLimit {
                resource: "LSM Bloom bytes",
                limit: BLOOM_MAX_BITS / 8,
            })?;
        Ok(Self {
            algorithm: BLOOM_ALGORITHM_VERSION,
            bit_count,
            hash_count: BLOOM_HASH_COUNT,
            bits: vec![0; byte_count],
        })
    }

    fn insert(&mut self, key: ClusteringKey) {
        if self.bit_count == 0 {
            return;
        }
        let (first, second) = bloom_hashes(key);
        for index in 0..self.hash_count {
            let bit = first.wrapping_add(u64::from(index).wrapping_mul(second)) % self.bit_count;
            self.bits[(bit / 8) as usize] |= 1 << (bit % 8);
        }
    }

    fn might_contain(&self, key: ClusteringKey) -> bool {
        if self.bit_count == 0 {
            return false;
        }
        let (first, second) = bloom_hashes(key);
        (0..self.hash_count).all(|index| {
            let bit = first.wrapping_add(u64::from(index).wrapping_mul(second)) % self.bit_count;
            self.bits[(bit / 8) as usize] & (1 << (bit % 8)) != 0
        })
    }
}

fn bloom_hashes(key: ClusteringKey) -> (u64, u64) {
    let mut bytes = [0_u8; 9];
    bytes[0] = match key {
        ClusteringKey::Int64(_) => 1,
        ClusteringKey::UInt64(_) => 2,
    };
    bytes[1..].copy_from_slice(&key.bits().to_le_bytes());
    let first = stable_fnv1a64(0xcbf2_9ce4_8422_2325, &bytes);
    let second = stable_fnv1a64(0x8422_2325_cbf2_9ce4, &bytes) | 1;
    (first, second)
}

fn stable_fnv1a64(mut hash: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

#[derive(Debug, Clone)]
struct Manifest {
    storage_id: StorageId,
    table_id: TableId,
    schema_fingerprint: SchemaFingerprint,
    clustering_column: ColumnId,
    key_type: PhysicalType,
    generation: u64,
    wal_generation: u64,
    row_reservation_end: u64,
    txn_reservation_end: u64,
    commit_reservation_end: u64,
    next_sstable_id: u64,
    table_statistics: Option<TableStatistics>,
    access_statistics: Option<IndexStatistics>,
    clustering_statistics: Option<(ClusteringKey, ClusteringKey)>,
    sstables: Vec<SstableRef>,
}

#[derive(Debug)]
enum ManifestPublishError {
    BeforeInstall(StorageError),
    InstalledButUnsynced {
        manifest: Box<Manifest>,
        source: StorageError,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ManifestPublishPoint {
    CandidateWrite,
    CandidateSync,
    BeforeInstall,
    AfterInstall,
}

#[derive(Debug)]
struct Runtime {
    writer: Cell<Option<TxnId>>,
    recovery_required: Cell<bool>,
    outstanding_transactions: Cell<u64>,
    outstanding_read_views: Cell<u64>,
    parked_prepared: RefCell<VecDeque<TxnId>>,
    parked_prepare_pending: RefCell<std::collections::HashSet<TxnId>>,
    parked_writes: RefCell<BTreeMap<LsmRowId, (TxnId, LsmCommitSeq)>>,
    prepared_write_conflict_count: Cell<u64>,
    prepare_sync_count: Cell<u64>,
    group_prepare_barrier_sync_count: Cell<u64>,
    single_commit_sync_count: Cell<u64>,
    group_commit_barrier_sync_count: Cell<u64>,
    amplification: AmplificationCounters,
}

#[derive(Debug, Default)]
struct AmplificationCounters {
    sstables_considered: Cell<u64>,
    sstables_read: Cell<u64>,
    data_blocks_read: Cell<u64>,
    bloom_checks: Cell<u64>,
    bloom_negatives: Cell<u64>,
    bloom_positives: Cell<u64>,
    flush_input_bytes: Cell<u64>,
    flush_output_bytes: Cell<u64>,
    compaction_input_bytes: Cell<u64>,
    compaction_output_bytes: Cell<u64>,
    obsolete_bytes: Cell<u64>,
    write_overflowed: Cell<bool>,
}

impl AmplificationCounters {
    fn read_snapshot(&self) -> LsmReadAmplification {
        LsmReadAmplification {
            sstables_considered: self.sstables_considered.get(),
            sstables_read: self.sstables_read.get(),
            data_blocks_read: self.data_blocks_read.get(),
            bloom_checks: self.bloom_checks.get(),
            bloom_negatives: self.bloom_negatives.get(),
            bloom_positives: self.bloom_positives.get(),
        }
    }

    fn write_snapshot(&self) -> LsmWriteAmplification {
        LsmWriteAmplification {
            flush_input_bytes: self.flush_input_bytes.get(),
            flush_output_bytes: self.flush_output_bytes.get(),
            compaction_input_bytes: self.compaction_input_bytes.get(),
            compaction_output_bytes: self.compaction_output_bytes.get(),
            obsolete_bytes: self.obsolete_bytes.get(),
            overflowed: self.write_overflowed.get(),
        }
    }
}

fn increment(counter: &Cell<u64>, value: u64) {
    counter.set(counter.get().saturating_add(value));
}

fn increment_write_counter(counters: &AmplificationCounters, counter: &Cell<u64>, value: u64) {
    let current = counter.get();
    match current.checked_add(value) {
        Some(next) => counter.set(next),
        None => {
            counter.set(u64::MAX);
            counters.write_overflowed.set(true);
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LsmReadAmplification {
    pub sstables_considered: u64,
    pub sstables_read: u64,
    pub data_blocks_read: u64,
    pub bloom_checks: u64,
    pub bloom_negatives: u64,
    pub bloom_positives: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LsmWriteAmplification {
    pub flush_input_bytes: u64,
    pub flush_output_bytes: u64,
    pub compaction_input_bytes: u64,
    pub compaction_output_bytes: u64,
    pub obsolete_bytes: u64,
    /// At least one write-amplification counter saturated, so deltas are no
    /// longer authoritative execution measurements.
    pub overflowed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LsmLevelInspection {
    pub level: u8,
    pub sstable_count: usize,
    pub bytes: u64,
}

#[derive(Debug)]
struct LsmShared {
    root: PathBuf,
    table: TableDef,
    clustering_position: usize,
    manifest: Manifest,
    wal: LsmWal,
    sstables: Vec<Sstable>,
    memtable: BTreeMap<PhysicalKey, BTreeMap<LsmCommitSeq, EntryValue>>,
    memtable_bytes: u64,
    next_row_id: u64,
    next_txn_id: u64,
    next_commit_seq: u64,
    visible_commit_seq: LsmCommitSeq,
    flush_threshold: u64,
    runtime: Rc<Runtime>,
    table_statistics: Option<TableStatistics>,
    access_statistics: Option<IndexStatistics>,
    change_stream: ChangeStreamManager,
}

#[derive(Debug)]
struct LsmWal {
    file: File,
    path: PathBuf,
    storage_id: StorageId,
    end: u64,
    #[cfg(test)]
    fail_next_sync: bool,
    #[cfg(test)]
    fail_append_after_calls: Option<usize>,
}

#[derive(Debug)]
pub struct LsmStorage {
    table: TableDef,
    shared: Rc<RefCell<LsmShared>>,
}

#[derive(Debug)]
pub struct LsmReadView {
    storage_id: StorageId,
    horizon: LsmCommitSeq,
    own_txn: Option<TxnId>,
    pending: BTreeMap<LsmRowId, PendingRow>,
    runtime: Rc<Runtime>,
}

impl Drop for LsmReadView {
    fn drop(&mut self) {
        self.runtime
            .outstanding_read_views
            .set(self.runtime.outstanding_read_views.get().saturating_sub(1));
    }
}

#[derive(Debug)]
pub struct LsmTransaction {
    id: TxnId,
    state: TransactionState,
    isolation_level: IsolationLevel,
    repeatable_horizon: Option<LsmCommitSeq>,
    pending: BTreeMap<LsmRowId, PendingRow>,
    pending_bytes: u64,
    next_revision: u64,
    prepared_database_txn_id: Option<DatabaseTxnId>,
    durable_batch: Option<Vec<WalMutation>>,
    pending_commit_seq: Option<LsmCommitSeq>,
    last_lsn: Lsn,
    owns_writer: bool,
    registered: bool,
    prepared_change: Option<PreparedChange>,
    shared: Rc<RefCell<LsmShared>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PreparedCommitBatchReport {
    pub(crate) member_count: usize,
    pub(crate) commit_records_staged: usize,
    pub(crate) wal_syncs: u64,
    pub(crate) first_local_boundary: u64,
    pub(crate) last_local_boundary: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PreparedPrepareBatchReport {
    pub(crate) member_count: usize,
    pub(crate) prepare_records_staged: usize,
    pub(crate) wal_syncs: u64,
    pub(crate) first_local_boundary: u64,
    pub(crate) last_local_boundary: u64,
}

#[derive(Debug, Clone)]
enum WalMutation {
    Put { key: PhysicalKey, row: Vec<u8> },
    Tombstone { key: PhysicalKey },
}

#[derive(Debug, Clone)]
enum WalRecord {
    MutationBatch {
        txn_id: TxnId,
        mutations: Vec<WalMutation>,
    },
    Prepare {
        txn_id: TxnId,
        database_txn_id: DatabaseTxnId,
    },
    Commit {
        txn_id: TxnId,
        commit_seq: LsmCommitSeq,
    },
    Abort {
        txn_id: TxnId,
    },
}

#[derive(Debug, Clone, Default)]
struct RecoveredTxn {
    mutations: Option<Vec<WalMutation>>,
    prepared: Option<DatabaseTxnId>,
    prepare_order: Option<u64>,
    commit: Option<LsmCommitSeq>,
    aborted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LsmIdentityInspection {
    pub storage_id: StorageId,
    pub table_id: TableId,
    pub schema_fingerprint: SchemaFingerprint,
    pub clustering_column: ColumnId,
    pub clustering_type: PhysicalType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LsmRecoveryInspection {
    pub storage_id: StorageId,
    pub prepared_transactions: Vec<PreparedTransaction>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LsmInspection {
    pub clustering_column: ColumnId,
    pub clustering_type: PhysicalType,
    pub sstable_count: usize,
    pub l0_count: usize,
    pub l1_count: usize,
    pub level_count: usize,
    pub levels: Vec<LsmLevelInspection>,
    pub l0_overlapping_file_count: usize,
    pub total_sstable_bytes: u64,
    pub bloom_enabled: bool,
    pub bloom_version: u8,
    pub bloom_filter_bytes: u64,
    pub read_amplification: LsmReadAmplification,
    pub write_amplification: LsmWriteAmplification,
    pub memtable_entry_count: u64,
    pub sstable_entry_count: u64,
    pub analyzed_live_row_count: Option<u64>,
    pub analyzed_min_clustering: Option<ScalarValue>,
    pub analyzed_max_clustering: Option<ScalarValue>,
}

/// Admission estimate for one existing atomic LSM maintenance primitive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LsmMaintenanceCostInspection {
    pub work_units: u64,
    pub read_bytes: u64,
    pub write_bytes: u64,
}

/// Conservative structural resource bound for one authoritative LSM rewrite.
///
/// `read_bytes` and `write_bytes` describe the production format and are not
/// claims about device I/O. Successful production accounting is guaranteed to
/// remain at or below these values while amplification counters are complete.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LsmMaintenanceBoundInspection {
    pub work_units: u64,
    pub read_bytes: u64,
    pub write_bytes: u64,
}

/// Equality authority for the exact current authoritative LSM layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LsmMaintenanceAnchor {
    pub storage_id: StorageId,
    pub manifest_generation: u64,
    pub wal_generation: u64,
    pub visible_commit_sequence: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LsmMaintenanceSafetyBlocker {
    RecoveryRequired,
    WriterActive { txn_id: TxnId },
    OutstandingTransactions { count: u64 },
    OutstandingReadViews { count: u64 },
}

/// Exact identity and storage-owned resource evidence for the next production
/// `compact_one` selection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LsmCompactionPlanInspection {
    pub input_sstable_ids: Vec<u64>,
    pub output_level: u8,
    pub input_entries: u64,
    pub input_bytes: u64,
    pub estimated_cost: LsmMaintenanceCostInspection,
    pub conservative_bound: LsmMaintenanceBoundInspection,
}

/// Immutable structural state used by higher-level maintenance policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LsmMaintenanceInspection {
    pub anchor: LsmMaintenanceAnchor,
    pub safety_blocker: Option<LsmMaintenanceSafetyBlocker>,
    pub memtable_entry_count: u64,
    pub memtable_bytes: u64,
    pub memtable_flush_threshold_bytes: u64,
    pub flush_cost: Option<LsmMaintenanceCostInspection>,
    pub flush_conservative_bound: Option<LsmMaintenanceBoundInspection>,
    pub next_compaction_cost: Option<LsmMaintenanceCostInspection>,
    pub next_compaction: Option<LsmCompactionPlanInspection>,
}

#[derive(Debug)]
struct VisibleRow {
    key: PhysicalKey,
    observed: LsmObservedVersion,
    row: Vec<u8>,
}

impl LsmStorage {
    pub fn prepared_runtime_inspection(&self) -> crate::PreparedRuntimeInspection {
        let shared = self.shared.borrow();
        let chain = shared
            .runtime
            .parked_prepared
            .borrow()
            .iter()
            .copied()
            .collect::<Vec<_>>();
        crate::PreparedRuntimeInspection {
            parked_prepared_count: chain.len(),
            parked_prepare_pending_count: shared.runtime.parked_prepare_pending.borrow().len(),
            active_group_chain: chain,
            prepared_write_conflict_count: shared.runtime.prepared_write_conflict_count.get(),
            prepare_sync_count: shared.runtime.prepare_sync_count.get(),
            group_prepare_barrier_sync_count: shared.runtime.group_prepare_barrier_sync_count.get(),
            single_commit_sync_count: shared.runtime.single_commit_sync_count.get(),
            group_commit_barrier_sync_count: shared.runtime.group_commit_barrier_sync_count.get(),
            change_stream_sync_count: shared.change_stream.sync_count(),
        }
    }

    /// Equality-only committed-state token for derived projections.
    ///
    /// Allocator reservations and manifest generations can advance without a
    /// logical data change. Phase 1 therefore combines the current WAL
    /// generation with the exact maximum committed version over the memtable
    /// and immutable runs. A projection build first flushes the memtable, so
    /// its WAL generation stays stable across close/reopen. Any later flush
    /// changes that generation, preventing compaction from making an old token
    /// equal again after it discards tombstones.
    pub(crate) fn projection_snapshot_parts(&self) -> Result<(u64, u64), StorageError> {
        let shared = self.shared.borrow();
        let mut maximum = shared
            .memtable
            .values()
            .flat_map(|versions| versions.keys())
            .map(|sequence| sequence.0)
            .max()
            .unwrap_or(0);
        for sstable in &shared.sstables {
            let mut entries = SstableEntryCursor::new(sstable, &shared.table, None, None)?;
            while let Some(entry) = entries.next()? {
                maximum = maximum.max(entry.version.0);
            }
        }
        Ok((shared.manifest.wal_generation, maximum))
    }

    pub fn create(
        root: impl AsRef<Path>,
        table: TableDef,
        clustering_column: ColumnId,
    ) -> Result<Self, StorageError> {
        Self::create_with_storage_id(root, table, clustering_column, StorageId(1))
    }

    pub fn create_with_storage_id(
        root: impl AsRef<Path>,
        table: TableDef,
        clustering_column: ColumnId,
        storage_id: StorageId,
    ) -> Result<Self, StorageError> {
        if storage_id.0 == 0 {
            return Err(LsmError::StorageIdMismatch {
                expected: StorageId(1),
                actual: storage_id,
            }
            .into());
        }
        table.validate()?;
        let fingerprint = table.fingerprint()?;
        let (clustering_position, key_type) =
            validate_clustering_column(&table, clustering_column)?;
        let root = root.as_ref().to_path_buf();
        fs::create_dir(&root)?;
        let creation = (|| {
            fs::create_dir(root.join(SST_DIR_NAME))?;
            let manifest = Manifest {
                storage_id,
                table_id: table.id,
                schema_fingerprint: fingerprint,
                clustering_column,
                key_type,
                generation: 1,
                wal_generation: 1,
                row_reservation_end: ALLOCATOR_RESERVATION + 1,
                txn_reservation_end: ALLOCATOR_RESERVATION + 1,
                commit_reservation_end: ALLOCATOR_RESERVATION + 1,
                next_sstable_id: 1,
                table_statistics: None,
                access_statistics: None,
                clustering_statistics: None,
                sstables: Vec::new(),
            };
            write_manifest_initial(&root, &manifest)?;
            let wal = LsmWal::create(&root, storage_id, 1)?;
            sync_directory(&root)?;
            let runtime = Rc::new(Runtime {
                writer: Cell::new(None),
                recovery_required: Cell::new(false),
                outstanding_transactions: Cell::new(0),
                outstanding_read_views: Cell::new(0),
                parked_prepared: RefCell::new(VecDeque::new()),
                parked_prepare_pending: RefCell::new(std::collections::HashSet::new()),
                parked_writes: RefCell::new(BTreeMap::new()),
                prepared_write_conflict_count: Cell::new(0),
                prepare_sync_count: Cell::new(0),
                group_prepare_barrier_sync_count: Cell::new(0),
                single_commit_sync_count: Cell::new(0),
                group_commit_barrier_sync_count: Cell::new(0),
                amplification: AmplificationCounters::default(),
            });
            let change_stream = ChangeStreamManager::disabled(
                crate::lsm_change_log_path(&root),
                crate::ChangeStorageKind::Lsm,
                storage_id,
                &table,
            )?;
            Ok(Self {
                table: table.clone(),
                shared: Rc::new(RefCell::new(LsmShared {
                    root: root.clone(),
                    table,
                    clustering_position,
                    manifest,
                    wal,
                    sstables: Vec::new(),
                    memtable: BTreeMap::new(),
                    memtable_bytes: 0,
                    next_row_id: 1,
                    next_txn_id: 1,
                    next_commit_seq: 1,
                    visible_commit_seq: LsmCommitSeq(0),
                    flush_threshold: DEFAULT_LSM_MEMTABLE_FLUSH_BYTES,
                    runtime,
                    table_statistics: None,
                    access_statistics: None,
                    change_stream,
                })),
            })
        })();
        if creation.is_err() {
            let _ = fs::remove_dir_all(&root);
        }
        creation
    }

    pub fn open(root: impl AsRef<Path>, table: TableDef) -> Result<Self, StorageError> {
        Self::open_with_prepared_resolutions(root, table, &[])
    }

    pub fn open_with_prepared_resolutions(
        root: impl AsRef<Path>,
        table: TableDef,
        resolutions: &[PreparedTxnResolution],
    ) -> Result<Self, StorageError> {
        table.validate()?;
        let root = root.as_ref().to_path_buf();
        let mut manifest = read_manifest(&root)?;
        validate_manifest_schema(&manifest, &table)?;
        cleanup_authoritative_orphans(&root, &manifest)?;
        let (clustering_position, key_type) =
            validate_clustering_column(&table, manifest.clustering_column)?;
        if key_type != manifest.key_type {
            return Err(
                LsmError::InvalidManifest("clustering physical type differs from schema").into(),
            );
        }
        let mut sstables = Vec::with_capacity(manifest.sstables.len());
        let mut max_commit = 0_u64;
        for reference in &manifest.sstables {
            let (sstable, sstable_max_commit) = open_sstable(&root, &manifest, reference, &table)?;
            sstables.push(sstable);
            max_commit = max_commit.max(sstable_max_commit);
        }
        let (mut wal, records) = LsmWal::open(&root, manifest.storage_id, manifest.wal_generation)?;
        let recovered = analyze_wal(&records)?;
        validate_recovered_transactions(&recovered, &manifest, &table)?;
        let prepared = classify_prepared(&recovered)?;
        validate_resolutions(&prepared, resolutions)?;
        let mut memtable = BTreeMap::new();
        let mut recovery_commit = allocate_recovery_commit(&manifest, &recovered)?;
        let mut change_outcomes = BTreeMap::new();
        for (txn_id, transaction) in &recovered {
            let decision = if let Some(commit) = transaction.commit {
                Some((PreparedDecision::Commit, commit))
            } else if let Some(database_txn_id) = transaction.prepared {
                let resolution = resolutions
                    .iter()
                    .find(|resolution| resolution.physical_txn_id == *txn_id)
                    .ok_or(RecoveryError::PreparedTransactionRequiresResolution {
                        database_txn_id,
                        physical_txn_id: *txn_id,
                    })?;
                match resolution.decision {
                    PreparedDecision::Commit => {
                        let commit = LsmCommitSeq(recovery_commit);
                        recovery_commit = recovery_commit
                            .checked_add(1)
                            .ok_or(LsmError::AllocatorExhausted("recovery commit sequence"))?;
                        wal.append(&WalRecord::Commit {
                            txn_id: *txn_id,
                            commit_seq: commit,
                        })?;
                        wal.sync()?;
                        Some((PreparedDecision::Commit, commit))
                    }
                    PreparedDecision::Abort => {
                        wal.append(&WalRecord::Abort { txn_id: *txn_id })?;
                        wal.sync()?;
                        None
                    }
                }
            } else {
                None
            };
            if let Some((PreparedDecision::Commit, commit)) = decision {
                let mutations = transaction.mutations.as_ref().ok_or(LsmError::InvalidWal(
                    "committed transaction has no mutation batch",
                ))?;
                apply_mutations(&mut memtable, mutations, commit)?;
                max_commit = max_commit.max(commit.0);
                change_outcomes.insert(*txn_id, AuthoritativeOutcome::Committed(Some(commit)));
            } else if transaction.aborted
                || transaction
                    .prepared
                    .and_then(|database_txn_id| {
                        resolutions.iter().find(|resolution| {
                            resolution.physical_txn_id == *txn_id
                                && resolution.database_txn_id == database_txn_id
                        })
                    })
                    .is_some_and(|resolution| resolution.decision == PreparedDecision::Abort)
            {
                change_outcomes.insert(*txn_id, AuthoritativeOutcome::Aborted);
            }
        }
        manifest.commit_reservation_end = manifest.commit_reservation_end.max(
            max_commit
                .checked_add(1)
                .ok_or(LsmError::AllocatorExhausted("commit sequence"))?,
        );
        let memtable_bytes = estimate_memtable_bytes(&memtable)?;
        let runtime = Rc::new(Runtime {
            writer: Cell::new(None),
            recovery_required: Cell::new(false),
            outstanding_transactions: Cell::new(0),
            outstanding_read_views: Cell::new(0),
            parked_prepared: RefCell::new(VecDeque::new()),
            parked_prepare_pending: RefCell::new(std::collections::HashSet::new()),
            parked_writes: RefCell::new(BTreeMap::new()),
            prepared_write_conflict_count: Cell::new(0),
            prepare_sync_count: Cell::new(0),
            group_prepare_barrier_sync_count: Cell::new(0),
            single_commit_sync_count: Cell::new(0),
            group_commit_barrier_sync_count: Cell::new(0),
            amplification: AmplificationCounters::default(),
        });
        let change_stream = ChangeStreamManager::open(
            crate::lsm_change_log_path(&root),
            crate::ChangeStorageKind::Lsm,
            manifest.storage_id,
            &table,
            |txn_id| {
                change_outcomes
                    .get(&txn_id)
                    .copied()
                    .unwrap_or(AuthoritativeOutcome::Unresolved)
            },
        )?;
        Ok(Self {
            table: table.clone(),
            shared: Rc::new(RefCell::new(LsmShared {
                root,
                table,
                clustering_position,
                manifest: manifest.clone(),
                wal,
                sstables,
                memtable,
                memtable_bytes,
                next_row_id: manifest.row_reservation_end,
                next_txn_id: manifest.txn_reservation_end,
                next_commit_seq: manifest.commit_reservation_end.max(max_commit + 1),
                visible_commit_seq: LsmCommitSeq(max_commit),
                flush_threshold: DEFAULT_LSM_MEMTABLE_FLUSH_BYTES,
                runtime,
                table_statistics: manifest.table_statistics,
                access_statistics: manifest.access_statistics,
                change_stream,
            })),
        })
    }

    pub fn inspect_identity(root: impl AsRef<Path>) -> Result<LsmIdentityInspection, StorageError> {
        let manifest = read_manifest(root.as_ref())?;
        Ok(LsmIdentityInspection {
            storage_id: manifest.storage_id,
            table_id: manifest.table_id,
            schema_fingerprint: manifest.schema_fingerprint,
            clustering_column: manifest.clustering_column,
            clustering_type: manifest.key_type,
        })
    }

    pub fn inspect_recovery(
        root: impl AsRef<Path>,
        table: &TableDef,
    ) -> Result<LsmRecoveryInspection, StorageError> {
        let manifest = read_manifest(root.as_ref())?;
        validate_manifest_schema(&manifest, table)?;
        let records = LsmWal::inspect(root.as_ref(), manifest.storage_id, manifest.wal_generation)?;
        let recovered = analyze_wal(&records)?;
        validate_recovered_transactions(&recovered, &manifest, table)?;
        Ok(LsmRecoveryInspection {
            storage_id: manifest.storage_id,
            prepared_transactions: classify_prepared(&recovered)?,
        })
    }

    #[must_use]
    pub fn storage_id(&self) -> StorageId {
        self.shared.borrow().manifest.storage_id
    }

    #[must_use]
    pub fn table(&self) -> &TableDef {
        &self.table
    }

    #[must_use]
    pub fn clustering_column(&self) -> ColumnId {
        self.shared.borrow().manifest.clustering_column
    }

    #[must_use]
    pub fn inspection(&self) -> LsmInspection {
        let shared = self.shared.borrow();
        let levels = (0..LSM_MAX_LEVELS)
            .filter_map(|level| {
                let files = shared
                    .sstables
                    .iter()
                    .filter(|sstable| sstable.reference.level == level)
                    .collect::<Vec<_>>();
                (!files.is_empty()).then(|| LsmLevelInspection {
                    level,
                    sstable_count: files.len(),
                    bytes: files
                        .iter()
                        .map(|sstable| sstable.reference.file_bytes)
                        .sum(),
                })
            })
            .collect::<Vec<_>>();
        let l0 = shared
            .sstables
            .iter()
            .filter(|sstable| sstable.reference.level == 0)
            .collect::<Vec<_>>();
        let l0_overlapping_file_count = l0
            .iter()
            .filter(|left| {
                l0.iter().any(|right| {
                    left.reference.id != right.reference.id
                        && ranges_overlap(
                            left.reference.min,
                            left.reference.max,
                            right.reference.min,
                            right.reference.max,
                        )
                })
            })
            .count();
        LsmInspection {
            clustering_column: shared.manifest.clustering_column,
            clustering_type: shared.manifest.key_type,
            sstable_count: shared.sstables.len(),
            l0_count: shared
                .sstables
                .iter()
                .filter(|sst| sst.reference.level == 0)
                .count(),
            l1_count: shared
                .sstables
                .iter()
                .filter(|sst| sst.reference.level == 1)
                .count(),
            level_count: levels.len(),
            levels,
            l0_overlapping_file_count,
            total_sstable_bytes: shared
                .sstables
                .iter()
                .map(|sstable| sstable.reference.file_bytes)
                .sum(),
            bloom_enabled: true,
            bloom_version: BLOOM_ALGORITHM_VERSION,
            bloom_filter_bytes: shared
                .sstables
                .iter()
                .map(|sstable| sstable.reference.bloom_bytes)
                .sum(),
            read_amplification: shared.runtime.amplification.read_snapshot(),
            write_amplification: shared.runtime.amplification.write_snapshot(),
            memtable_entry_count: shared
                .memtable
                .values()
                .map(|versions| versions.len() as u64)
                .sum(),
            sstable_entry_count: shared
                .sstables
                .iter()
                .map(|sstable| sstable.reference.entry_count)
                .sum(),
            analyzed_live_row_count: shared.table_statistics.map(|stats| stats.row_count),
            analyzed_min_clustering: shared
                .manifest
                .clustering_statistics
                .map(|(min, _)| min.into_value()),
            analyzed_max_clustering: shared
                .manifest
                .clustering_statistics
                .map(|(_, max)| max.into_value()),
        }
    }

    pub fn maintenance_inspection(&self) -> Result<LsmMaintenanceInspection, StorageError> {
        let shared = self.shared.borrow();
        let memtable_entry_count = shared
            .memtable
            .values()
            .map(|versions| versions.len() as u64)
            .sum::<u64>();
        let flush_cost = (memtable_entry_count != 0).then_some(LsmMaintenanceCostInspection {
            work_units: memtable_entry_count,
            read_bytes: shared.memtable_bytes,
            write_bytes: shared.memtable_bytes,
        });
        let flush_conservative_bound = if memtable_entry_count == 0 {
            None
        } else {
            Some(flush_conservative_bound(&shared)?)
        };
        let next_compaction = pick_compaction(&shared)?
            .map(|plan| compaction_plan_inspection(&shared, &plan))
            .transpose()?;
        let next_compaction_cost = next_compaction.as_ref().map(|plan| plan.estimated_cost);
        Ok(LsmMaintenanceInspection {
            anchor: LsmMaintenanceAnchor {
                storage_id: shared.manifest.storage_id,
                manifest_generation: shared.manifest.generation,
                wal_generation: shared.manifest.wal_generation,
                visible_commit_sequence: shared.visible_commit_seq.0,
            },
            safety_blocker: maintenance_safety_blocker(&shared),
            memtable_entry_count,
            memtable_bytes: shared.memtable_bytes,
            memtable_flush_threshold_bytes: shared.flush_threshold,
            flush_cost,
            flush_conservative_bound,
            next_compaction_cost,
            next_compaction,
        })
    }

    pub(crate) fn ensure_recovery_ready(&self) -> Result<(), StorageError> {
        if self.shared.borrow().runtime.recovery_required.get() {
            Err(TransactionError::RecoveryRequired.into())
        } else {
            Ok(())
        }
    }

    pub fn read_view(&self) -> Result<LsmReadView, StorageError> {
        let shared = self.shared.borrow();
        new_read_view(&shared, None, BTreeMap::new(), shared.maximum_commit_seq())
    }

    #[must_use]
    pub(crate) fn current_commit_seq(&self) -> LsmCommitSeq {
        self.shared.borrow().maximum_commit_seq()
    }

    pub(crate) fn read_view_at(&self, horizon: LsmCommitSeq) -> Result<LsmReadView, StorageError> {
        let shared = self.shared.borrow();
        let current = shared.maximum_commit_seq();
        if horizon > current {
            return Err(StorageError::FutureVisibilityBoundary {
                storage_id: shared.manifest.storage_id,
                requested: horizon.0.saturating_add(1),
                current: current.0.saturating_add(1),
            });
        }
        new_read_view(&shared, None, BTreeMap::new(), horizon)
    }

    pub(crate) fn enable_change_stream(
        &mut self,
    ) -> Result<crate::ChangeStreamCursor, StorageError> {
        let mut shared = self.shared.borrow_mut();
        if shared.runtime.outstanding_transactions.get() != 0
            || shared.runtime.writer.get().is_some()
        {
            return Err(TransactionError::OutstandingTransactions {
                count: shared.runtime.outstanding_transactions.get(),
            }
            .into());
        }
        // The incarnation identity binds this F0 to the current authoritative
        // state; later logical row commits alone advance the frontier.
        let baseline = netbadb_types::StorageDataVersion(0);
        shared.change_stream.enable(baseline)
    }

    pub(crate) fn disable_change_stream(&mut self) -> Result<(), StorageError> {
        let mut shared = self.shared.borrow_mut();
        if shared.runtime.outstanding_transactions.get() != 0
            || shared.runtime.writer.get().is_some()
        {
            return Err(TransactionError::OutstandingTransactions {
                count: shared.runtime.outstanding_transactions.get(),
            }
            .into());
        }
        shared.change_stream.disable()
    }

    pub(crate) fn change_stream_cursor(&self) -> Result<crate::ChangeStreamCursor, StorageError> {
        self.shared.borrow().change_stream.cursor()
    }

    pub(crate) fn read_changes(
        &self,
        cursor: crate::ChangeStreamCursor,
        max_batches: usize,
        max_bytes: u64,
    ) -> Result<crate::ChangeReadResult, StorageError> {
        self.shared
            .borrow()
            .change_stream
            .read(cursor, max_batches, max_bytes)
    }

    pub(crate) fn acquire_change_stream_retention_pin(
        &self,
        cursor: crate::ChangeStreamCursor,
    ) -> Result<crate::ChangeStreamRetentionPin, StorageError> {
        self.shared
            .borrow()
            .change_stream
            .acquire_retention_pin(cursor)
    }

    pub(crate) fn advance_change_stream_retention_pin(
        &self,
        pin: &mut crate::ChangeStreamRetentionPin,
        frontier: netbadb_types::StorageDataVersion,
    ) -> Result<(), StorageError> {
        self.shared
            .borrow()
            .change_stream
            .advance_retention_pin(pin, frontier)
    }

    pub(crate) fn gc_change_stream(
        &mut self,
        frontier: netbadb_types::StorageDataVersion,
    ) -> Result<crate::ChangeStreamGcStorageReport, StorageError> {
        let mut shared = self.shared.borrow_mut();
        if shared.runtime.outstanding_transactions.get() != 0
            || shared.runtime.writer.get().is_some()
        {
            return Err(TransactionError::OutstandingTransactions {
                count: shared.runtime.outstanding_transactions.get(),
            }
            .into());
        }
        shared.change_stream.gc_through(frontier)
    }

    pub(crate) fn change_stream_inspection(&self) -> crate::ChangeStreamInspection {
        self.shared.borrow().change_stream.inspection()
    }

    pub(crate) fn change_stream_maintenance_inspection(
        &self,
    ) -> crate::ChangeStreamMaintenanceInspection {
        self.shared.borrow().change_stream.maintenance_inspection()
    }

    pub fn begin_transaction(&mut self) -> Result<LsmTransaction, StorageError> {
        self.begin_transaction_with_isolation(IsolationLevel::ReadCommitted)
    }

    pub fn begin_transaction_with_isolation(
        &mut self,
        isolation_level: IsolationLevel,
    ) -> Result<LsmTransaction, StorageError> {
        let mut shared = self.shared.borrow_mut();
        let txn_id = TxnId(shared.allocate_txn_id()?);
        let count = shared
            .runtime
            .outstanding_transactions
            .get()
            .checked_add(1)
            .ok_or(TransactionError::OutstandingTransactionCountOverflow)?;
        shared.runtime.outstanding_transactions.set(count);
        Ok(LsmTransaction {
            id: txn_id,
            state: TransactionState::Active,
            isolation_level,
            repeatable_horizon: None,
            pending: BTreeMap::new(),
            pending_bytes: 0,
            next_revision: 1,
            prepared_database_txn_id: None,
            durable_batch: None,
            pending_commit_seq: None,
            last_lsn: Lsn(0),
            owns_writer: false,
            registered: true,
            prepared_change: None,
            shared: Rc::clone(&self.shared),
        })
    }

    pub fn validate_transaction(&self, transaction: &LsmTransaction) -> Result<(), StorageError> {
        if !Rc::ptr_eq(&self.shared, &transaction.shared) {
            return Err(TransactionError::ForeignTransaction {
                txn_id: transaction.id,
            }
            .into());
        }
        Ok(())
    }

    pub(crate) fn insert(&mut self, values: &[ScalarValue]) -> Result<LsmRowHandle, StorageError> {
        let mut transaction = self.begin_transaction()?;
        let row = self.insert_in(&mut transaction, values)?;
        transaction.commit()?;
        let view = self.read_view()?;
        self.refresh_handle(row.row_id, &view)
    }

    pub(crate) fn update(
        &mut self,
        row: LsmRowHandle,
        values: &[ScalarValue],
    ) -> Result<LsmRowHandle, StorageError> {
        let mut transaction = self.begin_transaction()?;
        let updated = self.update_in(&mut transaction, row, values)?;
        transaction.commit()?;
        let view = self.read_view()?;
        self.refresh_handle(updated.row_id, &view)
    }

    pub(crate) fn delete(&mut self, row: LsmRowHandle) -> Result<(), StorageError> {
        let mut transaction = self.begin_transaction()?;
        self.delete_in(&mut transaction, row)?;
        transaction.commit()
    }

    pub(crate) fn insert_in(
        &mut self,
        transaction: &mut LsmTransaction,
        values: &[ScalarValue],
    ) -> Result<LsmRowHandle, StorageError> {
        self.validate_transaction(transaction)?;
        transaction.ensure_active()?;
        validate_row(&self.shared.borrow().table, values)?;
        transaction.acquire_writer()?;
        let encoded = encode_row(values)?;
        transaction.ensure_capacity(encoded.len() as u64, 1)?;
        let mut shared = self.shared.borrow_mut();
        let key = clustering_key_from_values(&shared, values)?;
        let row_id = LsmRowId(shared.allocate_row_id()?);
        drop(shared);
        let revision = transaction.next_revision()?;
        transaction.pending.insert(
            row_id,
            PendingRow {
                original_key: None,
                current_key: key,
                row: Some(encoded),
                base_version: None,
                revision,
            },
        );
        transaction.recalculate_pending_bytes()?;
        Ok(LsmRowHandle {
            row_id,
            observed: LsmObservedVersion::Pending(revision),
            clustering_key: key.into(),
        })
    }

    pub(crate) fn update_in(
        &mut self,
        transaction: &mut LsmTransaction,
        handle: LsmRowHandle,
        values: &[ScalarValue],
    ) -> Result<LsmRowHandle, StorageError> {
        self.validate_transaction(transaction)?;
        transaction.ensure_active()?;
        validate_row(&self.shared.borrow().table, values)?;
        transaction.acquire_writer()?;
        transaction.validate_handle(self, handle)?;
        transaction.ensure_no_prepared_write_conflict(handle.row_id, handle.observed)?;
        let encoded = encode_row(values)?;
        transaction.ensure_capacity(encoded.len() as u64, 0)?;
        let key = clustering_key_from_values(&self.shared.borrow(), values)?;
        let revision = transaction.next_revision()?;
        if let Some(pending) = transaction.pending.get_mut(&handle.row_id) {
            pending.current_key = key;
            pending.row = Some(encoded);
            pending.revision = revision;
        } else {
            let old_key = ClusteringKey::from(handle.clustering_key);
            let base_version = match handle.observed {
                LsmObservedVersion::Committed(version) => Some(version),
                LsmObservedVersion::Pending(_) => {
                    return Err(LsmError::StaleHandle {
                        row_id: handle.row_id,
                    }
                    .into());
                }
            };
            transaction.pending.insert(
                handle.row_id,
                PendingRow {
                    original_key: Some(old_key),
                    current_key: key,
                    row: Some(encoded),
                    base_version,
                    revision,
                },
            );
        }
        transaction.recalculate_pending_bytes()?;
        Ok(LsmRowHandle {
            row_id: handle.row_id,
            observed: LsmObservedVersion::Pending(revision),
            clustering_key: key.into(),
        })
    }

    pub(crate) fn delete_in(
        &mut self,
        transaction: &mut LsmTransaction,
        handle: LsmRowHandle,
    ) -> Result<(), StorageError> {
        self.validate_transaction(transaction)?;
        transaction.ensure_active()?;
        transaction.acquire_writer()?;
        transaction.validate_handle(self, handle)?;
        transaction.ensure_no_prepared_write_conflict(handle.row_id, handle.observed)?;
        let revision = transaction.next_revision()?;
        if let Some(pending) = transaction.pending.get_mut(&handle.row_id) {
            if pending.original_key.is_none() {
                transaction.pending.remove(&handle.row_id);
            } else {
                pending.row = None;
                pending.revision = revision;
            }
        } else {
            let base_version = match handle.observed {
                LsmObservedVersion::Committed(version) => Some(version),
                LsmObservedVersion::Pending(_) => {
                    return Err(LsmError::StaleHandle {
                        row_id: handle.row_id,
                    }
                    .into());
                }
            };
            let key = ClusteringKey::from(handle.clustering_key);
            transaction.pending.insert(
                handle.row_id,
                PendingRow {
                    original_key: Some(key),
                    current_key: key,
                    row: None,
                    base_version,
                    revision,
                },
            );
        }
        transaction.recalculate_pending_bytes()?;
        Ok(())
    }

    pub(crate) fn scan_columns_with_view(
        &mut self,
        columns: &[ColumnId],
        view: &LsmReadView,
    ) -> Result<Vec<(LsmRowHandle, Vec<ScalarValue>)>, StorageError> {
        self.scan_range_columns_with_view(None, columns, view)
    }

    pub(crate) fn visit_columns_with_view_control<E, F>(
        &mut self,
        columns: &[ColumnId],
        view: &LsmReadView,
        mut visitor: F,
    ) -> Result<ControlFlow<()>, E>
    where
        E: From<StorageError>,
        F: FnMut(LsmRowHandle, Vec<ScalarValue>) -> Result<ControlFlow<()>, E>,
    {
        let shared = self.shared.borrow();
        validate_view(&shared, view).map_err(E::from)?;
        let positions = resolve_columns(&shared.table, columns).map_err(E::from)?;
        visit_visible_rows(&shared, view, None, |key, observed, row| {
            let values = decode_row_positions(row, &shared.table, &positions).map_err(E::from)?;
            visitor(
                LsmRowHandle {
                    row_id: key.row_id,
                    observed,
                    clustering_key: key.clustering.into(),
                },
                values,
            )
        })
    }

    pub(crate) fn point_lookup_columns_with_view(
        &mut self,
        key: &ScalarValue,
        columns: &[ColumnId],
        view: &LsmReadView,
    ) -> Result<Vec<(LsmRowHandle, Vec<ScalarValue>)>, StorageError> {
        let key = ClusteringKey::from_value(key, self.shared.borrow().manifest.key_type)?;
        self.scan_range_columns_with_view(Some(KeyRange::Point(key)), columns, view)
    }

    pub(crate) fn range_lookup_columns_with_view(
        &mut self,
        range: &IndexRange,
        columns: &[ColumnId],
        view: &LsmReadView,
    ) -> Result<Vec<(LsmRowHandle, Vec<ScalarValue>)>, StorageError> {
        let key_range = KeyRange::from_index(range, self.shared.borrow().manifest.key_type)?;
        self.scan_range_columns_with_view(Some(key_range), columns, view)
    }

    fn scan_range_columns_with_view(
        &mut self,
        range: Option<KeyRange>,
        columns: &[ColumnId],
        view: &LsmReadView,
    ) -> Result<Vec<(LsmRowHandle, Vec<ScalarValue>)>, StorageError> {
        let shared = self.shared.borrow();
        validate_view(&shared, view)?;
        let visible = collect_visible_rows(&shared, view, range.as_ref())?;
        visible
            .into_iter()
            .map(|row| {
                let values = decode_row_columns(&row.row, &shared.table, columns)?;
                Ok((
                    LsmRowHandle {
                        row_id: row.key.row_id,
                        observed: row.observed,
                        clustering_key: row.key.clustering.into(),
                    },
                    values,
                ))
            })
            .collect()
    }

    fn refresh_handle(
        &self,
        row_id: LsmRowId,
        view: &LsmReadView,
    ) -> Result<LsmRowHandle, StorageError> {
        let shared = self.shared.borrow();
        validate_view(&shared, view)?;
        collect_visible_rows(&shared, view, None)?
            .into_iter()
            .find(|row| row.key.row_id == row_id)
            .map(|row| LsmRowHandle {
                row_id,
                observed: row.observed,
                clustering_key: row.key.clustering.into(),
            })
            .ok_or_else(|| LsmError::RowNotFound(row_id).into())
    }

    pub fn scan_presence_counts_with_view(
        &mut self,
        columns: &[ColumnId],
        view: &LsmReadView,
    ) -> Result<PresenceCountSummary, StorageError> {
        let shared = self.shared.borrow();
        validate_view(&shared, view)?;
        let mut result = PresenceCountSummary {
            live_rows: 0,
            non_null_counts: vec![0; columns.len()],
        };
        for row in collect_visible_rows(&shared, view, None)? {
            let values = decode_row_columns(&row.row, &shared.table, columns)?;
            result.live_rows = result
                .live_rows
                .checked_add(1)
                .ok_or(StorageError::CountOverflow)?;
            for (count, value) in result.non_null_counts.iter_mut().zip(values) {
                if value != ScalarValue::Null {
                    *count = count.checked_add(1).ok_or(StorageError::CountOverflow)?;
                }
            }
        }
        Ok(result)
    }

    #[must_use]
    pub fn access_path_id(&self) -> netbadb_types::AccessPathId {
        netbadb_types::AccessPathId(LSM_ACCESS_PATH_PREFIX | u64::from(self.clustering_column().0))
    }

    #[must_use]
    pub fn table_statistics(&self) -> Option<TableStatistics> {
        self.shared.borrow().table_statistics
    }

    #[must_use]
    pub fn access_statistics(&self) -> Option<IndexStatistics> {
        self.shared.borrow().access_statistics
    }

    #[must_use]
    pub fn access_cost_hints(&self) -> crate::StorageAccessCostHints {
        let shared = self.shared.borrow();
        let l0 = shared
            .sstables
            .iter()
            .filter(|sstable| sstable.reference.level == 0)
            .count() as u64;
        let upper_levels = (1..LSM_MAX_LEVELS)
            .filter(|level| {
                shared
                    .sstables
                    .iter()
                    .any(|sstable| sstable.reference.level == *level)
            })
            .count() as u64;
        let candidates = l0.saturating_add(upper_levels);
        // Bloom is sized at ten bits/key with seven probes. Use a conservative
        // integer one-in-eight expectation rather than a floating estimate.
        let expected_false_positives = candidates.div_ceil(8);
        crate::StorageAccessCostHints {
            point_probe_base_cost: 1,
            expected_point_io: u32::try_from(
                u64::from(candidates != 0).saturating_add(expected_false_positives),
            )
            .unwrap_or(u32::MAX),
            range_startup_cost: u32::try_from(upper_levels.saturating_add(u64::from(l0 != 0)))
                .unwrap_or(u32::MAX),
            sequential_unit_cost: 1,
        }
    }

    pub fn analyze(&mut self) -> Result<(), StorageError> {
        let view = self.read_view()?;
        let shared = self.shared.borrow();
        let rows = collect_visible_rows(&shared, &view, None)?;
        let row_count = u64::try_from(rows.len()).map_err(|_| StorageError::CountOverflow)?;
        let distinct = rows
            .iter()
            .map(|row| row.key.clustering)
            .collect::<BTreeSet<_>>()
            .len();
        let blocks = shared
            .sstables
            .iter()
            .map(|sst| sst.blocks.len() as u64)
            .sum::<u64>()
            + u64::from(!shared.memtable.is_empty());
        drop(shared);
        let mut shared = self.shared.borrow_mut();
        // `managed_page_count` is the storage-neutral sequential work unit in
        // the current planner. MemTable traversal still scales with rows even
        // though it has no disk page, so include row work rather than claiming
        // an unrealistically constant one-block full scan.
        let sequential_work = blocks.max(row_count.saturating_add(1));
        let table_statistics = Some(TableStatistics {
            row_count,
            managed_page_count: sequential_work,
        });
        let access_statistics = Some(IndexStatistics {
            distinct_non_null_keys: u64::try_from(distinct)
                .map_err(|_| StorageError::CountOverflow)?,
            null_count: 0,
            // The existing neutral field is an access traversal work unit.
            // A MemTable-only point lookup has no on-disk level to traverse;
            // each immutable table adds one prunable seek candidate.
            tree_height: u32::try_from(shared.sstables.len()).unwrap_or(u32::MAX),
        });
        let clustering_statistics = match (rows.first(), rows.last()) {
            (Some(first), Some(last)) => Some((first.key.clustering, last.key.clustering)),
            (None, None) => None,
            _ => return Err(LsmError::InvalidManifest("ANALYZE key bounds differ").into()),
        };
        let mut candidate = shared.manifest.clone();
        candidate.clustering_statistics = clustering_statistics;
        candidate.table_statistics = table_statistics;
        candidate.access_statistics = access_statistics;
        let result = publish_manifest(&shared.root, candidate);
        let outcome = finish_manifest_publish(&mut shared, result);
        shared.table_statistics = shared.manifest.table_statistics;
        shared.access_statistics = shared.manifest.access_statistics;
        outcome
    }

    pub fn flush(&self) -> Result<(), StorageError> {
        let mut shared = self.shared.borrow_mut();
        ensure_maintenance_safe(&shared)?;
        flush_memtable(&mut shared)
    }

    pub fn compact(&self) -> Result<(), StorageError> {
        let mut shared = self.shared.borrow_mut();
        ensure_maintenance_safe(&shared)?;
        if !shared.memtable.is_empty() {
            flush_memtable(&mut shared)?;
        }
        compact_sstables(&mut shared)
    }

    /// Executes at most one already-eligible structural compaction. It never
    /// folds a MemTable flush into the same action.
    pub fn compact_one(&self) -> Result<bool, StorageError> {
        let mut shared = self.shared.borrow_mut();
        ensure_maintenance_safe(&shared)?;
        if !shared.memtable.is_empty() {
            return Ok(false);
        }
        let Some(plan) = pick_compaction(&shared)? else {
            return Ok(false);
        };
        execute_compaction(&mut shared, &plan, false)?;
        Ok(true)
    }

    /// Merges every immutable level and discards superseded history. This is
    /// deliberately quiescent so no snapshot can still require an old version.
    pub fn compact_full(&self) -> Result<(), StorageError> {
        let mut shared = self.shared.borrow_mut();
        ensure_maintenance_safe(&shared)?;
        if !shared.memtable.is_empty() {
            flush_memtable(&mut shared)?;
        }
        compact_full_sstables(&mut shared)
    }

    pub fn checkpoint(&self) -> Result<(), StorageError> {
        self.flush()
    }

    pub fn close(self) -> Result<(), StorageError> {
        {
            let mut shared = self.shared.borrow_mut();
            ensure_maintenance_safe(&shared)?;
            if !shared.memtable.is_empty() {
                flush_memtable(&mut shared)?;
            }
            shared.wal.sync()?;
        }
        Ok(())
    }
}

#[derive(Debug)]
enum KeyRange {
    Point(ClusteringKey),
    Bounds {
        lower: Option<(ClusteringKey, bool)>,
        upper: Option<(ClusteringKey, bool)>,
    },
}

impl KeyRange {
    fn from_index(range: &IndexRange, key_type: PhysicalType) -> Result<Self, StorageError> {
        let lower = match &range.lower {
            IndexBound::Unbounded => None,
            IndexBound::Included(value) => {
                Some((ClusteringKey::from_value(value, key_type)?, true))
            }
            IndexBound::Excluded(value) => {
                Some((ClusteringKey::from_value(value, key_type)?, false))
            }
        };
        let upper = match &range.upper {
            IndexBound::Unbounded => None,
            IndexBound::Included(value) => {
                Some((ClusteringKey::from_value(value, key_type)?, true))
            }
            IndexBound::Excluded(value) => {
                Some((ClusteringKey::from_value(value, key_type)?, false))
            }
        };
        Ok(Self::Bounds { lower, upper })
    }

    fn contains(&self, key: ClusteringKey) -> bool {
        match self {
            Self::Point(point) => key == *point,
            Self::Bounds { lower, upper } => {
                lower.is_none_or(|(bound, included)| key > bound || (included && key == bound))
                    && upper
                        .is_none_or(|(bound, included)| key < bound || (included && key == bound))
            }
        }
    }

    fn overlaps(&self, min: ClusteringKey, max: ClusteringKey) -> bool {
        match self {
            Self::Point(point) => min <= *point && *point <= max,
            Self::Bounds { lower, upper } => {
                lower.is_none_or(|(bound, included)| max > bound || (included && max == bound))
                    && upper
                        .is_none_or(|(bound, included)| min < bound || (included && min == bound))
            }
        }
    }

    fn starts_after(&self, max: ClusteringKey) -> bool {
        match self {
            Self::Point(point) => *point > max,
            Self::Bounds { lower, .. } => {
                lower.is_some_and(|(bound, included)| bound > max || (!included && bound == max))
            }
        }
    }

    fn ends_before(&self, min: ClusteringKey) -> bool {
        match self {
            Self::Point(point) => *point < min,
            Self::Bounds { upper, .. } => {
                upper.is_some_and(|(bound, included)| bound < min || (!included && bound == min))
            }
        }
    }
}

impl LsmTransaction {
    #[must_use]
    pub fn id(&self) -> TxnId {
        self.id
    }

    #[must_use]
    pub fn state(&self) -> TransactionState {
        self.state
    }

    #[must_use]
    pub fn last_lsn(&self) -> Lsn {
        self.last_lsn
    }

    #[must_use]
    pub fn isolation_level(&self) -> IsolationLevel {
        self.isolation_level
    }

    pub fn begin_statement(&mut self) -> Result<LsmReadView, StorageError> {
        self.ensure_active()?;
        let shared = self.shared.borrow();
        let current = shared.maximum_commit_seq();
        let horizon = match self.isolation_level {
            IsolationLevel::ReadCommitted => current,
            IsolationLevel::RepeatableRead => *self.repeatable_horizon.get_or_insert(current),
        };
        new_read_view(&shared, Some(self.id), self.pending.clone(), horizon)
    }

    pub(crate) fn begin_statement_at(
        &mut self,
        horizon: LsmCommitSeq,
    ) -> Result<LsmReadView, StorageError> {
        self.ensure_active()?;
        let shared = self.shared.borrow();
        let current = shared.maximum_commit_seq();
        if horizon > current {
            return Err(StorageError::FutureVisibilityBoundary {
                storage_id: shared.manifest.storage_id,
                requested: horizon.0.saturating_add(1),
                current: current.0.saturating_add(1),
            });
        }
        new_read_view(&shared, Some(self.id), self.pending.clone(), horizon)
    }

    #[must_use]
    pub(crate) fn current_commit_seq(&self) -> LsmCommitSeq {
        self.shared.borrow().maximum_commit_seq()
    }

    pub fn commit(&mut self) -> Result<(), StorageError> {
        self.ensure_recovery_not_required()?;
        let commit_seq = match self.state {
            TransactionState::Active => {
                if !self.owns_writer {
                    self.state = TransactionState::Committed;
                    self.unregister();
                    return Ok(());
                }
                let batch = self.canonical_batch()?;
                let mut shared = self.shared.borrow_mut();
                // Reserve before the first complete WAL record is appended. If
                // manifest publication fails, the transaction remains Active
                // without a durable batch that a retry could duplicate.
                let commit_seq = LsmCommitSeq(shared.allocate_commit_seq()?);
                if shared.change_stream.requires_changes() {
                    let changes = self.canonical_changes(
                        shared.manifest.storage_id,
                        &shared.table,
                        commit_seq,
                    )?;
                    self.prepared_change =
                        shared
                            .change_stream
                            .prepare(self.id, None, changes.as_slice())?;
                }
                let batch_lsn = shared.wal.append(&WalRecord::MutationBatch {
                    txn_id: self.id,
                    mutations: batch.clone(),
                })?;
                self.last_lsn = batch_lsn;
                self.durable_batch = Some(batch);
                self.pending_commit_seq = Some(commit_seq);
                self.state = TransactionState::CommitPending;
                let commit_lsn = shared.wal.append(&WalRecord::Commit {
                    txn_id: self.id,
                    commit_seq,
                })?;
                self.last_lsn = commit_lsn;
                commit_seq
            }
            TransactionState::CommitPending => {
                let expected = self
                    .pending_commit_seq
                    .ok_or(LsmError::InvalidWal("pending commit sequence is missing"))?;
                let mut shared = self.shared.borrow_mut();
                self.last_lsn =
                    ensure_commit_record(&mut shared, self.id, expected, self.last_lsn)?.0;
                expected
            }
            state => {
                return Err(TransactionError::NotActive {
                    txn_id: self.id,
                    state,
                }
                .into());
            }
        };
        {
            let mut shared = self.shared.borrow_mut();
            #[cfg(test)]
            maybe_lsm_crash("before-commit-sync");
            shared.wal.sync()?;
            increment(&shared.runtime.single_commit_sync_count, 1);
            #[cfg(test)]
            maybe_lsm_crash("after-commit-sync");
            let batch = self
                .durable_batch
                .as_ref()
                .ok_or(LsmError::InvalidWal("pending commit batch is missing"))?;
            apply_mutations(&mut shared.memtable, batch, commit_seq)?;
            shared.memtable_bytes = estimate_memtable_bytes(&shared.memtable)?;
            shared.visible_commit_seq = shared.visible_commit_seq.max(commit_seq);
            if let Some(prepared) = self.prepared_change {
                shared
                    .change_stream
                    .publish(self.id, prepared, Some(commit_seq))?;
            }
            #[cfg(test)]
            maybe_lsm_crash("after-memtable-apply");
        }
        self.finish_terminal(TransactionState::Committed);
        Ok(())
    }

    pub fn prepare(&mut self, database_txn_id: DatabaseTxnId) -> Result<(), StorageError> {
        self.ensure_recovery_not_required()?;
        if database_txn_id.0 == 0 {
            return Err(TransactionError::InvalidDatabaseTxnId.into());
        }
        match self.state {
            TransactionState::Active => {
                if !self.owns_writer {
                    return Err(TransactionError::NotActive {
                        txn_id: self.id,
                        state: self.state,
                    }
                    .into());
                }
                let batch = self.canonical_batch()?;
                let mut shared = self.shared.borrow_mut();
                if shared.change_stream.requires_changes() {
                    let changes = self.canonical_changes(
                        shared.manifest.storage_id,
                        &shared.table,
                        LsmCommitSeq(0),
                    )?;
                    self.prepared_change = shared.change_stream.prepare(
                        self.id,
                        Some(database_txn_id),
                        changes.as_slice(),
                    )?;
                }
                self.last_lsn = shared.wal.append(&WalRecord::MutationBatch {
                    txn_id: self.id,
                    mutations: batch.clone(),
                })?;
                self.durable_batch = Some(batch);
                self.prepared_database_txn_id = Some(database_txn_id);
                self.state = TransactionState::PreparePending;
                self.last_lsn = shared.wal.append(&WalRecord::Prepare {
                    txn_id: self.id,
                    database_txn_id,
                })?;
            }
            TransactionState::PreparePending
                if self.prepared_database_txn_id == Some(database_txn_id) =>
            {
                let mut shared = self.shared.borrow_mut();
                self.last_lsn =
                    ensure_prepare_record(&mut shared, self.id, database_txn_id, self.last_lsn)?;
            }
            TransactionState::Prepared
                if self.prepared_database_txn_id == Some(database_txn_id) =>
            {
                return Ok(());
            }
            TransactionState::PreparePending | TransactionState::Prepared => {
                return Err(TransactionError::DatabaseTxnMismatch {
                    txn_id: self.id,
                    expected: self.prepared_database_txn_id,
                    actual: database_txn_id,
                }
                .into());
            }
            state => {
                return Err(TransactionError::NotActive {
                    txn_id: self.id,
                    state,
                }
                .into());
            }
        }
        self.shared.borrow_mut().wal.sync()?;
        increment(&self.shared.borrow().runtime.prepare_sync_count, 1);
        self.state = TransactionState::Prepared;
        Ok(())
    }

    pub(crate) fn stage_group_prepare(
        &mut self,
        database_txn_id: DatabaseTxnId,
    ) -> Result<(), StorageError> {
        self.ensure_recovery_not_required()?;
        if database_txn_id.0 == 0 {
            return Err(TransactionError::InvalidDatabaseTxnId.into());
        }
        match self.state {
            TransactionState::Active => {
                if !self.owns_writer {
                    return Err(TransactionError::NotActive {
                        txn_id: self.id,
                        state: self.state,
                    }
                    .into());
                }
                let batch = self.canonical_batch()?;
                let mut shared = self.shared.borrow_mut();
                if shared.change_stream.requires_changes() {
                    let changes = self.canonical_changes(
                        shared.manifest.storage_id,
                        &shared.table,
                        LsmCommitSeq(0),
                    )?;
                    self.prepared_change = shared.change_stream.prepare(
                        self.id,
                        Some(database_txn_id),
                        changes.as_slice(),
                    )?;
                }
                self.last_lsn = shared.wal.append(&WalRecord::MutationBatch {
                    txn_id: self.id,
                    mutations: batch.clone(),
                })?;
                self.durable_batch = Some(batch);
                self.prepared_database_txn_id = Some(database_txn_id);
                self.state = TransactionState::PreparePending;
                self.last_lsn = shared.wal.append(&WalRecord::Prepare {
                    txn_id: self.id,
                    database_txn_id,
                })?;
            }
            TransactionState::PreparePending
                if self.prepared_database_txn_id == Some(database_txn_id) =>
            {
                let mut shared = self.shared.borrow_mut();
                self.last_lsn =
                    ensure_prepare_record(&mut shared, self.id, database_txn_id, self.last_lsn)?;
            }
            TransactionState::ParkedPreparePending
                if self.prepared_database_txn_id == Some(database_txn_id) =>
            {
                return Ok(());
            }
            TransactionState::PreparePending | TransactionState::ParkedPreparePending => {
                return Err(TransactionError::DatabaseTxnMismatch {
                    txn_id: self.id,
                    expected: self.prepared_database_txn_id,
                    actual: database_txn_id,
                }
                .into());
            }
            state => {
                return Err(TransactionError::NotActive {
                    txn_id: self.id,
                    state,
                }
                .into());
            }
        }
        {
            let shared = self.shared.borrow();
            let mut writes = shared.runtime.parked_writes.borrow_mut();
            for (row_id, pending) in &self.pending {
                if pending.base_version.is_some() {
                    if let Some((conflicting_txn_id, _)) = writes.get(row_id) {
                        return Err(TransactionError::PreparedWriteConflict {
                            txn_id: self.id,
                            conflicting_txn_id: *conflicting_txn_id,
                        }
                        .into());
                    }
                }
            }
            for (row_id, pending) in &self.pending {
                if let Some(base_version) = pending.base_version {
                    writes.insert(*row_id, (self.id, base_version));
                }
            }
            shared
                .runtime
                .parked_prepared
                .borrow_mut()
                .push_back(self.id);
            shared
                .runtime
                .parked_prepare_pending
                .borrow_mut()
                .insert(self.id);
            if shared.runtime.writer.get() == Some(self.id) {
                shared.runtime.writer.set(None);
            }
        }
        self.owns_writer = false;
        self.state = TransactionState::ParkedPreparePending;
        Ok(())
    }

    /// Parks a durably prepared participant and releases the LSM writer lease.
    pub fn park_prepared(&mut self, database_txn_id: DatabaseTxnId) -> Result<(), StorageError> {
        self.validate_database_txn(database_txn_id)?;
        if self.state == TransactionState::ParkedPrepared {
            return Ok(());
        }
        if self.state != TransactionState::Prepared {
            return Err(TransactionError::NotPrepared {
                txn_id: self.id,
                state: self.state,
            }
            .into());
        }
        {
            let shared = self.shared.borrow();
            let mut writes = shared.runtime.parked_writes.borrow_mut();
            for (row_id, pending) in &self.pending {
                if pending.base_version.is_some() {
                    if let Some((conflicting_txn_id, _)) = writes.get(row_id) {
                        return Err(TransactionError::PreparedWriteConflict {
                            txn_id: self.id,
                            conflicting_txn_id: *conflicting_txn_id,
                        }
                        .into());
                    }
                }
            }
            for (row_id, pending) in &self.pending {
                if let Some(base_version) = pending.base_version {
                    writes.insert(*row_id, (self.id, base_version));
                }
            }
            shared
                .runtime
                .parked_prepared
                .borrow_mut()
                .push_back(self.id);
            if shared.runtime.writer.get() == Some(self.id) {
                shared.runtime.writer.set(None);
            }
        }
        self.owns_writer = false;
        self.state = TransactionState::ParkedPrepared;
        Ok(())
    }

    pub fn commit_prepared(&mut self, database_txn_id: DatabaseTxnId) -> Result<(), StorageError> {
        self.ensure_recovery_not_required()?;
        self.validate_database_txn(database_txn_id)?;
        let commit_seq = match self.state {
            TransactionState::Prepared | TransactionState::ParkedPrepared => {
                if self.state == TransactionState::ParkedPrepared {
                    self.acquire_parked_resolution(false)?;
                }
                let mut shared = self.shared.borrow_mut();
                let seq = LsmCommitSeq(shared.allocate_commit_seq()?);
                self.pending_commit_seq = Some(seq);
                self.state = TransactionState::CommitPending;
                self.last_lsn = shared.wal.append(&WalRecord::Commit {
                    txn_id: self.id,
                    commit_seq: seq,
                })?;
                seq
            }
            TransactionState::CommitPending => {
                let expected = self.pending_commit_seq.ok_or(LsmError::InvalidWal(
                    "prepared pending commit sequence is missing",
                ))?;
                let mut shared = self.shared.borrow_mut();
                self.last_lsn =
                    ensure_commit_record(&mut shared, self.id, expected, self.last_lsn)?.0;
                expected
            }
            TransactionState::Committed => return Ok(()),
            state => {
                return Err(TransactionError::NotPrepared {
                    txn_id: self.id,
                    state,
                }
                .into());
            }
        };
        {
            let mut shared = self.shared.borrow_mut();
            shared.wal.sync()?;
            increment(&shared.runtime.single_commit_sync_count, 1);
            let batch = self
                .durable_batch
                .as_ref()
                .ok_or(LsmError::InvalidWal("prepared mutation batch is missing"))?;
            apply_mutations(&mut shared.memtable, batch, commit_seq)?;
            shared.memtable_bytes = estimate_memtable_bytes(&shared.memtable)?;
            shared.visible_commit_seq = shared.visible_commit_seq.max(commit_seq);
            if let Some(prepared) = self.prepared_change {
                shared
                    .change_stream
                    .publish(self.id, prepared, Some(commit_seq))?;
            }
        }
        self.finish_terminal(TransactionState::Committed);
        Ok(())
    }

    pub(crate) fn commit_prepared_batch(
        participants: &mut [(&mut Self, DatabaseTxnId)],
    ) -> Result<PreparedCommitBatchReport, StorageError> {
        let Some((first, _)) = participants.first() else {
            return Err(TransactionError::EmptyPreparedCommitBatch.into());
        };
        let shared_handle = first.shared.clone();
        let expected_prefix = shared_handle
            .borrow()
            .runtime
            .parked_prepared
            .borrow()
            .iter()
            .take(participants.len())
            .copied()
            .collect::<Vec<_>>();
        if expected_prefix.len() != participants.len() {
            return Err(TransactionError::PreparedCommitBatchStorageMismatch.into());
        }
        for (position, (participant, database_txn_id)) in participants.iter().enumerate() {
            if !Rc::ptr_eq(&participant.shared, &shared_handle) {
                return Err(TransactionError::PreparedCommitBatchStorageMismatch.into());
            }
            participant.ensure_recovery_not_required()?;
            participant.validate_database_txn(*database_txn_id)?;
            if !matches!(
                participant.state,
                TransactionState::ParkedPrepared | TransactionState::CommitPending
            ) {
                return Err(TransactionError::NotPrepared {
                    txn_id: participant.id,
                    state: participant.state,
                }
                .into());
            }
            if expected_prefix[position] != participant.id {
                return Err(TransactionError::PreparedResolutionOrder {
                    txn_id: participant.id,
                    expected: expected_prefix[position],
                }
                .into());
            }
        }

        let mut staged = 0_usize;
        let mut first_seq = None;
        let mut last_seq = None;
        {
            let mut shared = shared_handle.borrow_mut();
            for (position, (participant, _)) in participants.iter_mut().enumerate() {
                let commit_seq = match participant.state {
                    TransactionState::ParkedPrepared => {
                        let seq = LsmCommitSeq(shared.allocate_commit_seq()?);
                        participant.pending_commit_seq = Some(seq);
                        participant.state = TransactionState::CommitPending;
                        participant.last_lsn = shared.wal.append(&WalRecord::Commit {
                            txn_id: participant.id,
                            commit_seq: seq,
                        })?;
                        staged = staged.saturating_add(1);
                        #[cfg(test)]
                        maybe_lsm_crash(&format!("group-after-commit-append-{}", position + 1));
                        #[cfg(not(test))]
                        let _ = position;
                        seq
                    }
                    TransactionState::CommitPending => {
                        let expected = participant.pending_commit_seq.ok_or(
                            LsmError::InvalidWal("prepared pending commit sequence is missing"),
                        )?;
                        let (lsn, appended) = ensure_commit_record(
                            &mut shared,
                            participant.id,
                            expected,
                            participant.last_lsn,
                        )?;
                        participant.last_lsn = lsn;
                        if appended {
                            staged = staged.saturating_add(1);
                        }
                        expected
                    }
                    _ => unreachable!("validated prepared batch state"),
                };
                first_seq.get_or_insert(commit_seq);
                last_seq = Some(commit_seq);
            }
            #[cfg(test)]
            maybe_lsm_crash("group-before-commit-sync");
            shared.wal.sync()?;
            increment(&shared.runtime.group_commit_barrier_sync_count, 1);
            #[cfg(test)]
            maybe_lsm_crash("group-after-commit-sync");
        }

        for (position, (participant, _)) in participants.iter_mut().enumerate() {
            let commit_seq = participant.pending_commit_seq.ok_or(LsmError::InvalidWal(
                "prepared pending commit sequence is missing",
            ))?;
            {
                let mut shared = shared_handle.borrow_mut();
                let batch = participant
                    .durable_batch
                    .as_ref()
                    .ok_or(LsmError::InvalidWal("prepared mutation batch is missing"))?;
                apply_mutations(&mut shared.memtable, batch, commit_seq)?;
                shared.memtable_bytes = estimate_memtable_bytes(&shared.memtable)?;
                shared.visible_commit_seq = shared.visible_commit_seq.max(commit_seq);
                if let Some(prepared) = participant.prepared_change {
                    shared
                        .change_stream
                        .publish(participant.id, prepared, Some(commit_seq))?;
                }
            }
            participant.finish_terminal(TransactionState::Committed);
            #[cfg(test)]
            maybe_lsm_crash(&format!("group-after-runtime-finalize-{}", position + 1));
            #[cfg(not(test))]
            let _ = position;
        }
        let first_seq = first_seq.ok_or(TransactionError::EmptyPreparedCommitBatch)?;
        let last_seq = last_seq.ok_or(TransactionError::EmptyPreparedCommitBatch)?;
        Ok(PreparedCommitBatchReport {
            member_count: participants.len(),
            commit_records_staged: staged,
            wal_syncs: 1,
            first_local_boundary: first_seq.0,
            last_local_boundary: last_seq.0,
        })
    }

    pub(crate) fn durabilize_group_prepare_batch(
        participants: &mut [(&mut Self, DatabaseTxnId)],
    ) -> Result<PreparedPrepareBatchReport, StorageError> {
        let Some((first, _)) = participants.first() else {
            return Err(TransactionError::EmptyPreparedPrepareBatch.into());
        };
        let shared_handle = first.shared.clone();
        let expected_prefix = shared_handle
            .borrow()
            .runtime
            .parked_prepared
            .borrow()
            .iter()
            .take(participants.len())
            .copied()
            .collect::<Vec<_>>();
        if expected_prefix.len() != participants.len() {
            return Err(TransactionError::PreparedPrepareBatchStorageMismatch.into());
        }
        for (position, (participant, database_txn_id)) in participants.iter().enumerate() {
            if !Rc::ptr_eq(&participant.shared, &shared_handle) {
                return Err(TransactionError::PreparedPrepareBatchStorageMismatch.into());
            }
            participant.ensure_recovery_not_required()?;
            participant.validate_database_txn(*database_txn_id)?;
            if !matches!(
                participant.state,
                TransactionState::ParkedPreparePending | TransactionState::ParkedPrepared
            ) {
                return Err(TransactionError::NotPrepared {
                    txn_id: participant.id,
                    state: participant.state,
                }
                .into());
            }
            if expected_prefix[position] != participant.id {
                return Err(TransactionError::PreparedResolutionOrder {
                    txn_id: participant.id,
                    expected: expected_prefix[position],
                }
                .into());
            }
        }
        let first_lsn = participants
            .first()
            .map(|(participant, _)| participant.last_lsn)
            .ok_or(TransactionError::EmptyPreparedPrepareBatch)?;
        let last_lsn = participants
            .last()
            .map(|(participant, _)| participant.last_lsn)
            .ok_or(TransactionError::EmptyPreparedPrepareBatch)?;
        {
            let mut shared = shared_handle.borrow_mut();
            #[cfg(test)]
            maybe_lsm_crash("group-before-prepare-sync");
            shared.wal.sync()?;
            increment(&shared.runtime.group_prepare_barrier_sync_count, 1);
            #[cfg(test)]
            maybe_lsm_crash("group-after-prepare-sync");
        }
        for (position, (participant, _)) in participants.iter_mut().enumerate() {
            participant.state = TransactionState::ParkedPrepared;
            shared_handle
                .borrow()
                .runtime
                .parked_prepare_pending
                .borrow_mut()
                .remove(&participant.id);
            #[cfg(test)]
            maybe_lsm_crash(&format!("group-after-prepare-state-{}", position + 1));
            #[cfg(not(test))]
            let _ = position;
        }
        Ok(PreparedPrepareBatchReport {
            member_count: participants.len(),
            prepare_records_staged: participants.len(),
            wal_syncs: 1,
            first_local_boundary: first_lsn.0,
            last_local_boundary: last_lsn.0,
        })
    }

    pub fn rollback_prepared(
        &mut self,
        database_txn_id: DatabaseTxnId,
    ) -> Result<(), StorageError> {
        self.ensure_recovery_not_required()?;
        self.validate_database_txn(database_txn_id)?;
        if self.state == TransactionState::RolledBack {
            return Ok(());
        }
        if !matches!(
            self.state,
            TransactionState::Prepared
                | TransactionState::ParkedPrepared
                | TransactionState::ParkedPreparePending
                | TransactionState::PreparePending
                | TransactionState::RollbackPending
        ) {
            return Err(TransactionError::NotPrepared {
                txn_id: self.id,
                state: self.state,
            }
            .into());
        }
        if self.state != TransactionState::RollbackPending {
            if matches!(
                self.state,
                TransactionState::ParkedPreparePending | TransactionState::ParkedPrepared
            ) {
                self.acquire_parked_resolution(true)?;
            }
            let mut shared = self.shared.borrow_mut();
            self.state = TransactionState::RollbackPending;
            self.last_lsn = shared.wal.append(&WalRecord::Abort { txn_id: self.id })?;
        } else {
            let mut shared = self.shared.borrow_mut();
            self.last_lsn = ensure_abort_record(&mut shared, self.id, self.last_lsn)?;
        }
        self.shared.borrow_mut().wal.sync()?;
        self.shared.borrow_mut().change_stream.abandon(self.id)?;
        self.finish_terminal(TransactionState::RolledBack);
        Ok(())
    }

    pub fn rollback(&mut self) -> Result<(), StorageError> {
        match self.state {
            TransactionState::Active => {
                self.pending.clear();
                self.pending_bytes = 0;
                self.shared.borrow_mut().change_stream.abandon(self.id)?;
                self.finish_terminal(TransactionState::RolledBack);
                Ok(())
            }
            TransactionState::RolledBack => Ok(()),
            TransactionState::Prepared | TransactionState::PreparePending => {
                Err(TransactionError::NotActive {
                    txn_id: self.id,
                    state: self.state,
                }
                .into())
            }
            state => Err(TransactionError::NotActive {
                txn_id: self.id,
                state,
            }
            .into()),
        }
    }

    pub fn abort(&mut self) -> Result<(), StorageError> {
        self.rollback()
    }

    fn ensure_active(&self) -> Result<(), StorageError> {
        if self.state != TransactionState::Active {
            return Err(TransactionError::NotActive {
                txn_id: self.id,
                state: self.state,
            }
            .into());
        }
        Ok(())
    }

    fn ensure_recovery_not_required(&self) -> Result<(), StorageError> {
        if self.shared.borrow().runtime.recovery_required.get() {
            return Err(TransactionError::RecoveryRequired.into());
        }
        Ok(())
    }

    fn acquire_writer(&mut self) -> Result<(), StorageError> {
        self.ensure_recovery_not_required()?;
        if self.owns_writer {
            return Ok(());
        }
        {
            let mut shared = self.shared.borrow_mut();
            if shared.memtable_bytes >= shared.flush_threshold
                && shared.runtime.writer.get().is_none()
            {
                // Pending writes live only in this transaction and are not in
                // the committed MemTable. Flushing every committed version is
                // snapshot-safe even while read-only views remain pinned.
                flush_memtable(&mut shared)?;
            }
        }
        let shared = self.shared.borrow();
        match shared.runtime.writer.get() {
            None => {
                shared.runtime.writer.set(Some(self.id));
                self.owns_writer = true;
                Ok(())
            }
            Some(txn_id) if txn_id == self.id => {
                self.owns_writer = true;
                Ok(())
            }
            Some(txn_id) => Err(TransactionError::WriterBusy { txn_id }.into()),
        }
    }

    fn ensure_no_prepared_write_conflict(
        &self,
        row_id: LsmRowId,
        observed: LsmObservedVersion,
    ) -> Result<(), StorageError> {
        let LsmObservedVersion::Committed(base_version) = observed else {
            return Ok(());
        };
        let shared = self.shared.borrow();
        if let Some((conflicting_txn_id, conflicting_version)) =
            shared.runtime.parked_writes.borrow().get(&row_id).copied()
        {
            if conflicting_version == base_version && conflicting_txn_id != self.id {
                shared.runtime.prepared_write_conflict_count.set(
                    shared
                        .runtime
                        .prepared_write_conflict_count
                        .get()
                        .saturating_add(1),
                );
                return Err(TransactionError::PreparedWriteConflict {
                    txn_id: self.id,
                    conflicting_txn_id,
                }
                .into());
            }
        }
        Ok(())
    }

    fn acquire_parked_resolution(&mut self, abort: bool) -> Result<(), StorageError> {
        let shared = self.shared.borrow();
        let parked = shared.runtime.parked_prepared.borrow();
        let expected = if abort { parked.back() } else { parked.front() }
            .copied()
            .ok_or(TransactionError::NotPrepared {
                txn_id: self.id,
                state: self.state,
            })?;
        if expected != self.id {
            return Err(TransactionError::PreparedResolutionOrder {
                txn_id: self.id,
                expected,
            }
            .into());
        }
        drop(parked);
        match shared.runtime.writer.get() {
            None => shared.runtime.writer.set(Some(self.id)),
            Some(txn_id) if txn_id == self.id => {}
            Some(txn_id) => return Err(TransactionError::WriterBusy { txn_id }.into()),
        }
        self.owns_writer = true;
        Ok(())
    }

    fn next_revision(&mut self) -> Result<u64, StorageError> {
        let revision = self.next_revision;
        self.next_revision = revision
            .checked_add(1)
            .ok_or(LsmError::AllocatorExhausted("pending revision"))?;
        Ok(revision)
    }

    fn ensure_capacity(
        &self,
        additional_bytes: u64,
        additional_rows: u64,
    ) -> Result<(), StorageError> {
        if self
            .pending_bytes
            .checked_add(additional_bytes)
            .is_none_or(|total| total > LSM_MAX_PENDING_TRANSACTION_BYTES)
        {
            return Err(StorageError::ResourceLimit {
                resource: "LSM pending transaction bytes",
                limit: LSM_MAX_PENDING_TRANSACTION_BYTES,
            });
        }
        if u64::try_from(self.pending.len())
            .unwrap_or(u64::MAX)
            .checked_add(additional_rows)
            .is_none_or(|total| total > LSM_MAX_PENDING_MUTATIONS)
        {
            return Err(StorageError::ResourceLimit {
                resource: "LSM pending mutations",
                limit: LSM_MAX_PENDING_MUTATIONS,
            });
        }
        Ok(())
    }

    fn recalculate_pending_bytes(&mut self) -> Result<(), StorageError> {
        let mut total = 0_u64;
        for row in self.pending.values() {
            total = total
                .checked_add(64)
                .and_then(|value| {
                    value.checked_add(row.row.as_ref().map_or(0, |bytes| bytes.len() as u64))
                })
                .ok_or(StorageError::ResourceLimit {
                    resource: "LSM pending transaction bytes",
                    limit: LSM_MAX_PENDING_TRANSACTION_BYTES,
                })?;
        }
        if total > LSM_MAX_PENDING_TRANSACTION_BYTES {
            return Err(StorageError::ResourceLimit {
                resource: "LSM pending transaction bytes",
                limit: LSM_MAX_PENDING_TRANSACTION_BYTES,
            });
        }
        self.pending_bytes = total;
        Ok(())
    }

    fn validate_handle(
        &self,
        storage: &LsmStorage,
        handle: LsmRowHandle,
    ) -> Result<(), StorageError> {
        if let Some(pending) = self.pending.get(&handle.row_id) {
            return if handle.observed == LsmObservedVersion::Pending(pending.revision)
                && ClusteringKey::from(handle.clustering_key) == pending.current_key
                && pending.row.is_some()
            {
                Ok(())
            } else {
                Err(LsmError::StaleHandle {
                    row_id: handle.row_id,
                }
                .into())
            };
        }
        let view = {
            let shared = self.shared.borrow();
            new_read_view(
                &shared,
                Some(self.id),
                BTreeMap::new(),
                shared.maximum_commit_seq(),
            )?
        };
        let refreshed = storage.refresh_handle(handle.row_id, &view)?;
        if refreshed == handle {
            Ok(())
        } else {
            Err(LsmError::StaleHandle {
                row_id: handle.row_id,
            }
            .into())
        }
    }

    fn canonical_batch(&self) -> Result<Vec<WalMutation>, StorageError> {
        let mut mutations = Vec::new();
        for (row_id, pending) in &self.pending {
            if pending.original_key.is_some() != pending.base_version.is_some() {
                return Err(LsmError::StaleHandle { row_id: *row_id }.into());
            }
            if let Some(original) = pending.original_key {
                if original != pending.current_key || pending.row.is_none() {
                    mutations.push(WalMutation::Tombstone {
                        key: PhysicalKey {
                            clustering: original,
                            row_id: *row_id,
                        },
                    });
                }
            }
            if let Some(row) = &pending.row {
                mutations.push(WalMutation::Put {
                    key: PhysicalKey {
                        clustering: pending.current_key,
                        row_id: *row_id,
                    },
                    row: row.clone(),
                });
            }
        }
        mutations.sort_by_key(|mutation| match mutation {
            WalMutation::Put { key, .. } | WalMutation::Tombstone { key } => *key,
        });
        Ok(mutations)
    }

    fn canonical_changes(
        &self,
        storage_id: StorageId,
        table: &TableDef,
        new_version: LsmCommitSeq,
    ) -> Result<PendingChangeSet, StorageError> {
        let mut changes = PendingChangeSet::new();
        for (row_id, pending) in &self.pending {
            let new_key = StorageVersionKey::Lsm {
                storage_id,
                row_id: *row_id,
                version: new_version,
            };
            match (pending.base_version, &pending.row) {
                (None, Some(row)) => {
                    changes.record_insert(new_key, decode_row(row, table)?);
                }
                (Some(old_version), Some(row)) => {
                    changes.record_update(
                        StorageVersionKey::Lsm {
                            storage_id,
                            row_id: *row_id,
                            version: old_version,
                        },
                        new_key,
                        decode_row(row, table)?,
                    );
                }
                (Some(old_version), None) => {
                    changes.record_delete(StorageVersionKey::Lsm {
                        storage_id,
                        row_id: *row_id,
                        version: old_version,
                    });
                }
                (None, None) => {}
            }
        }
        Ok(changes)
    }

    fn validate_database_txn(&self, database_txn_id: DatabaseTxnId) -> Result<(), StorageError> {
        if self.prepared_database_txn_id != Some(database_txn_id) {
            return Err(TransactionError::DatabaseTxnMismatch {
                txn_id: self.id,
                expected: self.prepared_database_txn_id,
                actual: database_txn_id,
            }
            .into());
        }
        Ok(())
    }

    fn finish_terminal(&mut self, state: TransactionState) {
        self.state = state;
        self.pending.clear();
        self.pending_bytes = 0;
        if self.owns_writer {
            let shared = self.shared.borrow();
            if shared.runtime.writer.get() == Some(self.id) {
                shared.runtime.writer.set(None);
            }
            self.owns_writer = false;
        }
        {
            let shared = self.shared.borrow();
            let mut parked = shared.runtime.parked_prepared.borrow_mut();
            let removed = match state {
                TransactionState::Committed => parked
                    .front()
                    .copied()
                    .filter(|id| *id == self.id)
                    .map(|_| parked.pop_front()),
                TransactionState::RolledBack => parked
                    .back()
                    .copied()
                    .filter(|id| *id == self.id)
                    .map(|_| parked.pop_back()),
                _ => None,
            };
            if removed.is_some() {
                shared
                    .runtime
                    .parked_prepare_pending
                    .borrow_mut()
                    .remove(&self.id);
                shared
                    .runtime
                    .parked_writes
                    .borrow_mut()
                    .retain(|_, (txn_id, _)| *txn_id != self.id);
            }
        }
        self.unregister();
    }

    fn unregister(&mut self) {
        if self.registered {
            let shared = self.shared.borrow();
            shared.runtime.outstanding_transactions.set(
                shared
                    .runtime
                    .outstanding_transactions
                    .get()
                    .saturating_sub(1),
            );
            self.registered = false;
        }
    }
}

impl Drop for LsmTransaction {
    fn drop(&mut self) {
        if self
            .shared
            .borrow()
            .runtime
            .parked_prepared
            .borrow()
            .contains(&self.id)
        {
            self.shared.borrow().runtime.recovery_required.set(true);
        }
        if self.owns_writer {
            let shared = self.shared.borrow();
            if matches!(
                self.state,
                TransactionState::Prepared
                    | TransactionState::PreparePending
                    | TransactionState::CommitPending
                    | TransactionState::RollbackPending
            ) {
                shared.runtime.recovery_required.set(true);
            } else if shared.runtime.writer.get() == Some(self.id) {
                shared.runtime.writer.set(None);
            }
        }
        self.unregister();
    }
}

impl LsmShared {
    fn maximum_commit_seq(&self) -> LsmCommitSeq {
        self.visible_commit_seq
    }

    fn allocate_row_id(&mut self) -> Result<u64, StorageError> {
        reserve_if_needed(self, AllocatorKind::Row)?;
        let value = self.next_row_id;
        if value == 0 {
            return Err(LsmError::AllocatorExhausted("row ID").into());
        }
        self.next_row_id = value
            .checked_add(1)
            .ok_or(LsmError::AllocatorExhausted("row ID"))?;
        Ok(value)
    }

    fn allocate_txn_id(&mut self) -> Result<u64, StorageError> {
        reserve_if_needed(self, AllocatorKind::Txn)?;
        let value = self.next_txn_id;
        if value == 0 {
            return Err(LsmError::AllocatorExhausted("transaction ID").into());
        }
        self.next_txn_id = value
            .checked_add(1)
            .ok_or(LsmError::AllocatorExhausted("transaction ID"))?;
        Ok(value)
    }

    fn allocate_commit_seq(&mut self) -> Result<u64, StorageError> {
        reserve_if_needed(self, AllocatorKind::Commit)?;
        let value = self.next_commit_seq;
        if value == 0 {
            return Err(LsmError::AllocatorExhausted("commit sequence").into());
        }
        self.next_commit_seq = value
            .checked_add(1)
            .ok_or(LsmError::AllocatorExhausted("commit sequence"))?;
        Ok(value)
    }
}

#[derive(Debug, Clone, Copy)]
enum AllocatorKind {
    Row,
    Txn,
    Commit,
}

fn reserve_if_needed(shared: &mut LsmShared, kind: AllocatorKind) -> Result<(), StorageError> {
    let (next, end) = match kind {
        AllocatorKind::Row => (shared.next_row_id, shared.manifest.row_reservation_end),
        AllocatorKind::Txn => (shared.next_txn_id, shared.manifest.txn_reservation_end),
        AllocatorKind::Commit => (
            shared.next_commit_seq,
            shared.manifest.commit_reservation_end,
        ),
    };
    if next < end {
        return Ok(());
    }
    let new_end = next
        .checked_add(ALLOCATOR_RESERVATION)
        .ok_or(LsmError::AllocatorExhausted(match kind {
            AllocatorKind::Row => "row ID",
            AllocatorKind::Txn => "transaction ID",
            AllocatorKind::Commit => "commit sequence",
        }))?;
    let mut candidate = shared.manifest.clone();
    match kind {
        AllocatorKind::Row => candidate.row_reservation_end = new_end,
        AllocatorKind::Txn => candidate.txn_reservation_end = new_end,
        AllocatorKind::Commit => candidate.commit_reservation_end = new_end,
    }
    let result = publish_manifest(&shared.root, candidate);
    finish_manifest_publish(shared, result)
}

fn new_read_view(
    shared: &LsmShared,
    own_txn: Option<TxnId>,
    pending: BTreeMap<LsmRowId, PendingRow>,
    horizon: LsmCommitSeq,
) -> Result<LsmReadView, StorageError> {
    let count = shared
        .runtime
        .outstanding_read_views
        .get()
        .checked_add(1)
        .ok_or(TransactionError::OutstandingTransactionCountOverflow)?;
    shared.runtime.outstanding_read_views.set(count);
    Ok(LsmReadView {
        storage_id: shared.manifest.storage_id,
        horizon,
        own_txn,
        pending,
        runtime: Rc::clone(&shared.runtime),
    })
}

fn validate_view(shared: &LsmShared, view: &LsmReadView) -> Result<(), StorageError> {
    if view.storage_id != shared.manifest.storage_id {
        return Err(LsmError::StorageIdMismatch {
            expected: shared.manifest.storage_id,
            actual: view.storage_id,
        }
        .into());
    }
    let _ = view.own_txn;
    Ok(())
}

fn clustering_key_from_values(
    shared: &LsmShared,
    values: &[ScalarValue],
) -> Result<ClusteringKey, StorageError> {
    let value = values
        .get(shared.clustering_position)
        .ok_or(StorageError::InvalidRowLength {
            expected: shared.table.columns.len(),
            actual: values.len(),
        })?;
    ClusteringKey::from_value(value, shared.manifest.key_type)
}

fn validate_clustering_column(
    table: &TableDef,
    column_id: ColumnId,
) -> Result<(usize, PhysicalType), StorageError> {
    let position = table
        .columns
        .iter()
        .position(|column| column.id == column_id)
        .ok_or(LsmError::InvalidClusteringColumn(column_id))?;
    let column = &table.columns[position];
    if column.nullable {
        return Err(LsmError::InvalidClusteringColumn(column_id).into());
    }
    let kind = column.semantic_type().physical;
    if !matches!(kind, PhysicalType::Int64 | PhysicalType::UInt64) {
        return Err(LsmError::UnsupportedClusteringType(kind).into());
    }
    Ok((position, kind))
}

fn apply_mutations(
    memtable: &mut BTreeMap<PhysicalKey, BTreeMap<LsmCommitSeq, EntryValue>>,
    mutations: &[WalMutation],
    version: LsmCommitSeq,
) -> Result<(), StorageError> {
    if version.0 == 0 {
        return Err(LsmError::InvalidWal("commit sequence is zero").into());
    }
    for mutation in mutations {
        let (key, value) = match mutation {
            WalMutation::Put { key, row } => (*key, EntryValue::Put(row.clone())),
            WalMutation::Tombstone { key } => (*key, EntryValue::Tombstone),
        };
        let replaced = memtable
            .entry(key)
            .or_default()
            .insert(version, value.clone());
        if replaced.as_ref().is_some_and(|previous| previous != &value) {
            return Err(LsmError::InvalidWal("same key/version has conflicting mutations").into());
        }
    }
    Ok(())
}

fn validate_recovered_transactions(
    recovered: &BTreeMap<TxnId, RecoveredTxn>,
    manifest: &Manifest,
    table: &TableDef,
) -> Result<(), StorageError> {
    for transaction in recovered.values() {
        if let Some(mutations) = &transaction.mutations {
            validate_recovered_mutations(mutations, manifest, table)?;
        }
    }
    Ok(())
}

fn validate_recovered_mutations(
    mutations: &[WalMutation],
    manifest: &Manifest,
    table: &TableDef,
) -> Result<(), StorageError> {
    for mutation in mutations {
        let key = match mutation {
            WalMutation::Put { key, row } => {
                let values = decode_row(row, table)?;
                let stored_key = ClusteringKey::from_value(
                    values
                        .get(
                            table
                                .columns
                                .iter()
                                .position(|column| column.id == manifest.clustering_column)
                                .ok_or(LsmError::InvalidClusteringColumn(
                                    manifest.clustering_column,
                                ))?,
                        )
                        .ok_or(LsmError::InvalidWal("row omits clustering value"))?,
                    manifest.key_type,
                )?;
                if stored_key != key.clustering {
                    return Err(LsmError::InvalidWal("Put key differs from encoded row").into());
                }
                key
            }
            WalMutation::Tombstone { key } => key,
        };
        if key.row_id.0 == 0 || key.clustering.kind() != manifest.key_type {
            return Err(LsmError::InvalidWal("mutation key is invalid").into());
        }
    }
    Ok(())
}

fn collect_visible_rows(
    shared: &LsmShared,
    view: &LsmReadView,
    range: Option<&KeyRange>,
) -> Result<Vec<VisibleRow>, StorageError> {
    let mut visible = Vec::new();
    let _ = visit_visible_rows::<StorageError, _>(shared, view, range, |key, observed, row| {
        visible.push(VisibleRow {
            key,
            observed,
            row: row.to_vec(),
        });
        Ok(ControlFlow::Continue(()))
    })?;
    Ok(visible)
}

fn visit_visible_rows<E, F>(
    shared: &LsmShared,
    view: &LsmReadView,
    range: Option<&KeyRange>,
    mut visitor: F,
) -> Result<ControlFlow<()>, E>
where
    E: From<StorageError>,
    F: FnMut(PhysicalKey, LsmObservedVersion, &[u8]) -> Result<ControlFlow<()>, E>,
{
    let mut committed =
        VisibleCommittedCursor::new(shared, view.horizon, range).map_err(E::from)?;
    let mut pending = view
        .pending
        .iter()
        .filter_map(|(row_id, pending)| {
            let row = pending.row.as_deref()?;
            let key = PhysicalKey {
                clustering: pending.current_key,
                row_id: *row_id,
            };
            range
                .is_none_or(|range| range.contains(key.clustering))
                .then_some((key, pending.revision, row))
        })
        .collect::<Vec<_>>();
    pending.sort_unstable_by_key(|(key, _, _)| *key);

    let mut pending_position = 0;
    let mut committed_row =
        next_unshadowed_committed(&mut committed, &view.pending).map_err(E::from)?;
    loop {
        let pending_row = pending.get(pending_position).copied();
        match (committed_row.as_ref(), pending_row) {
            (None, None) => return Ok(ControlFlow::Continue(())),
            (Some(row), None) => {
                if visitor(row.key, row.observed, &row.row)?.is_break() {
                    return Ok(ControlFlow::Break(()));
                }
                committed_row =
                    next_unshadowed_committed(&mut committed, &view.pending).map_err(E::from)?;
            }
            (None, Some((key, revision, row))) => {
                if visitor(key, LsmObservedVersion::Pending(revision), row)?.is_break() {
                    return Ok(ControlFlow::Break(()));
                }
                pending_position += 1;
            }
            (Some(row), Some((key, revision, pending_values))) => match row.key.cmp(&key) {
                std::cmp::Ordering::Less => {
                    if visitor(row.key, row.observed, &row.row)?.is_break() {
                        return Ok(ControlFlow::Break(()));
                    }
                    committed_row = next_unshadowed_committed(&mut committed, &view.pending)
                        .map_err(E::from)?;
                }
                std::cmp::Ordering::Equal => {
                    if visitor(key, LsmObservedVersion::Pending(revision), pending_values)?
                        .is_break()
                    {
                        return Ok(ControlFlow::Break(()));
                    }
                    pending_position += 1;
                    committed_row = next_unshadowed_committed(&mut committed, &view.pending)
                        .map_err(E::from)?;
                }
                std::cmp::Ordering::Greater => {
                    if visitor(key, LsmObservedVersion::Pending(revision), pending_values)?
                        .is_break()
                    {
                        return Ok(ControlFlow::Break(()));
                    }
                    pending_position += 1;
                }
            },
        }
    }
}

fn next_unshadowed_committed(
    cursor: &mut VisibleCommittedCursor<'_>,
    pending: &BTreeMap<LsmRowId, PendingRow>,
) -> Result<Option<VisibleRow>, StorageError> {
    loop {
        let Some(row) = cursor.next()? else {
            return Ok(None);
        };
        let shadowed = pending
            .get(&row.key.row_id)
            .is_some_and(|pending| pending.original_key == Some(row.key.clustering));
        if !shadowed {
            return Ok(Some(row));
        }
    }
}

struct VisibleCommittedCursor<'a> {
    runs: Vec<MergeRun<'a>>,
    horizon: LsmCommitSeq,
    current_key: Option<PhysicalKey>,
    selected: Option<VersionedEntry>,
    exhausted: bool,
}

impl<'a> VisibleCommittedCursor<'a> {
    fn new(
        shared: &'a LsmShared,
        horizon: LsmCommitSeq,
        range: Option<&'a KeyRange>,
    ) -> Result<Self, StorageError> {
        let mut runs = Vec::new();
        let memory = MemoryEntryCursor::new(&shared.memtable, range);
        if !shared.memtable.is_empty() {
            runs.push(MergeRun::new(EntryCursor::Memory(memory))?);
        }
        for sstable in select_sstables_for_read(shared, range) {
            runs.push(MergeRun::new(EntryCursor::Sstable(
                SstableEntryCursor::new(
                    sstable,
                    &shared.table,
                    range,
                    Some(&shared.runtime.amplification),
                )?,
            ))?);
        }
        Ok(Self {
            runs,
            horizon,
            current_key: None,
            selected: None,
            exhausted: false,
        })
    }

    fn next(&mut self) -> Result<Option<VisibleRow>, StorageError> {
        if self.exhausted {
            return Ok(None);
        }
        loop {
            let Some(entry) = next_merged_entry(&mut self.runs)? else {
                self.exhausted = true;
                return Ok(self.selected.take().and_then(visible_committed_row));
            };
            if self.current_key.is_some_and(|key| key != entry.key) {
                let completed = self.selected.take().and_then(visible_committed_row);
                self.current_key = Some(entry.key);
                if entry.version <= self.horizon {
                    self.selected = Some(entry);
                }
                if completed.is_some() {
                    return Ok(completed);
                }
            } else {
                self.current_key = Some(entry.key);
                if entry.version <= self.horizon {
                    self.selected = Some(entry);
                }
            }
        }
    }
}

fn visible_committed_row(entry: VersionedEntry) -> Option<VisibleRow> {
    match entry {
        VersionedEntry {
            key,
            version,
            value: EntryValue::Put(row),
        } => Some(VisibleRow {
            key,
            observed: LsmObservedVersion::Committed(version),
            row,
        }),
        VersionedEntry {
            value: EntryValue::Tombstone,
            ..
        } => None,
    }
}

struct MemoryEntryCursor<'a> {
    entries: std::collections::btree_map::Iter<'a, PhysicalKey, BTreeMap<LsmCommitSeq, EntryValue>>,
    versions: Option<(
        PhysicalKey,
        std::collections::btree_map::Iter<'a, LsmCommitSeq, EntryValue>,
    )>,
    range: Option<&'a KeyRange>,
}

impl<'a> MemoryEntryCursor<'a> {
    fn new(
        memtable: &'a BTreeMap<PhysicalKey, BTreeMap<LsmCommitSeq, EntryValue>>,
        range: Option<&'a KeyRange>,
    ) -> Self {
        Self {
            entries: memtable.iter(),
            versions: None,
            range,
        }
    }

    fn next(&mut self) -> Option<VersionedEntry> {
        loop {
            if let Some((key, versions)) = &mut self.versions {
                if let Some((version, value)) = versions.next() {
                    return Some(VersionedEntry {
                        key: *key,
                        version: *version,
                        value: value.clone(),
                    });
                }
                self.versions = None;
            }
            let (key, versions) = self.entries.next()?;
            if self
                .range
                .is_some_and(|range| !range.contains(key.clustering))
            {
                continue;
            }
            self.versions = Some((*key, versions.iter()));
        }
    }
}

enum EntryCursor<'a> {
    Memory(MemoryEntryCursor<'a>),
    Sstable(SstableEntryCursor<'a>),
}

impl EntryCursor<'_> {
    fn next(&mut self) -> Result<Option<VersionedEntry>, StorageError> {
        match self {
            Self::Memory(entries) => Ok(entries.next()),
            Self::Sstable(cursor) => cursor.next(),
        }
    }
}

struct MergeRun<'a> {
    cursor: EntryCursor<'a>,
    current: Option<VersionedEntry>,
}

impl<'a> MergeRun<'a> {
    fn new(mut cursor: EntryCursor<'a>) -> Result<Self, StorageError> {
        let current = cursor.next()?;
        Ok(Self { cursor, current })
    }

    fn advance(&mut self) -> Result<(), StorageError> {
        self.current = self.cursor.next()?;
        Ok(())
    }
}

struct SstableEntryCursor<'a> {
    sstable: &'a Sstable,
    table: &'a TableDef,
    range: Option<&'a KeyRange>,
    counters: Option<&'a AmplificationCounters>,
    next_block: usize,
    entries: VecDeque<VersionedEntry>,
}

impl<'a> SstableEntryCursor<'a> {
    fn new(
        sstable: &'a Sstable,
        table: &'a TableDef,
        range: Option<&'a KeyRange>,
        counters: Option<&'a AmplificationCounters>,
    ) -> Result<Self, StorageError> {
        // Validate the path eagerly, but do not retain one descriptor per merge
        // run. A legal manifest may contain far more SSTables than the process
        // descriptor limit; each block read opens the file only for that block.
        drop(File::open(&sstable.path)?);
        if let Some(counters) = counters {
            increment(&counters.sstables_read, 1);
        }
        Ok(Self {
            sstable,
            table,
            range,
            counters,
            next_block: range.map_or(0, |range| {
                sstable
                    .blocks
                    .partition_point(|block| range.starts_after(block.last.clustering))
            }),
            entries: VecDeque::new(),
        })
    }

    fn next(&mut self) -> Result<Option<VersionedEntry>, StorageError> {
        loop {
            if let Some(entry) = self.entries.pop_front() {
                return Ok(Some(entry));
            }
            let Some(meta) = self.sstable.blocks.get(self.next_block) else {
                return Ok(None);
            };
            if self
                .range
                .is_some_and(|range| range.ends_before(meta.first.clustering))
            {
                return Ok(None);
            }
            let block_index = self.next_block;
            self.next_block += 1;
            if self
                .range
                .is_some_and(|range| !range.overlaps(meta.first.clustering, meta.last.clustering))
            {
                continue;
            }
            let mut file = File::open(&self.sstable.path)?;
            let (actual, entries, _) = read_sstable_block(
                &mut file,
                self.sstable.reference.id,
                block_index as u32,
                meta.offset,
                meta.first.clustering.kind(),
                self.table,
            )?;
            if actual.payload_length != meta.payload_length
                || actual.entry_count != meta.entry_count
                || actual.first != meta.first
                || actual.last != meta.last
            {
                return Err(LsmError::InvalidSstable {
                    sstable_id: self.sstable.reference.id,
                    reason: "sparse block metadata changed",
                }
                .into());
            }
            if let Some(counters) = self.counters {
                increment(&counters.data_blocks_read, 1);
            }
            self.entries.extend(entries.into_iter().filter(|entry| {
                self.range
                    .is_none_or(|range| range.contains(entry.key.clustering))
            }));
        }
    }
}

fn next_merged_entry(runs: &mut [MergeRun<'_>]) -> Result<Option<VersionedEntry>, StorageError> {
    let Some(key) = runs
        .iter()
        .filter_map(|run| run.current.as_ref())
        .map(|entry| (entry.key, entry.version))
        .min()
    else {
        return Ok(None);
    };
    let mut selected = None;
    for run in runs {
        if run
            .current
            .as_ref()
            .is_some_and(|entry| (entry.key, entry.version) == key)
        {
            let entry = run.current.take().ok_or(LsmError::InvalidSstable {
                sstable_id: 0,
                reason: "merge cursor lost current entry",
            })?;
            if selected
                .as_ref()
                .is_some_and(|previous: &VersionedEntry| previous.value != entry.value)
            {
                return Err(LsmError::InvalidSstable {
                    sstable_id: 0,
                    reason: "duplicate internal key has conflicting values",
                }
                .into());
            }
            selected = Some(entry);
            run.advance()?;
        }
    }
    Ok(selected)
}

fn select_sstables_for_read<'a>(
    shared: &'a LsmShared,
    range: Option<&KeyRange>,
) -> Vec<&'a Sstable> {
    let mut selected = Vec::new();
    for level in 0..LSM_MAX_LEVELS {
        let start = shared
            .sstables
            .partition_point(|sstable| sstable.reference.level < level);
        let end = shared
            .sstables
            .partition_point(|sstable| sstable.reference.level <= level);
        let files = &shared.sstables[start..end];
        if level == 0 {
            increment(
                &shared.runtime.amplification.sstables_considered,
                files.len() as u64,
            );
            for sstable in files {
                if range.is_none_or(|range| {
                    range.overlaps(
                        sstable.reference.min.clustering,
                        sstable.reference.max.clustering,
                    )
                }) && bloom_allows(shared, sstable, range)
                {
                    selected.push(sstable);
                }
            }
            continue;
        }
        if files.is_empty() {
            continue;
        }
        let first_candidate = range.map_or(0, |range| {
            files.partition_point(|sstable| range.starts_after(sstable.reference.max.clustering))
        });
        for sstable in files.iter().skip(first_candidate) {
            increment(&shared.runtime.amplification.sstables_considered, 1);
            if range.is_some_and(|range| range.ends_before(sstable.reference.min.clustering)) {
                break;
            }
            if range.is_none_or(|range| {
                range.overlaps(
                    sstable.reference.min.clustering,
                    sstable.reference.max.clustering,
                )
            }) && bloom_allows(shared, sstable, range)
            {
                selected.push(sstable);
            }
        }
    }
    selected
}

fn bloom_allows(shared: &LsmShared, sstable: &Sstable, range: Option<&KeyRange>) -> bool {
    let Some(KeyRange::Point(key)) = range else {
        return true;
    };
    increment(&shared.runtime.amplification.bloom_checks, 1);
    if sstable.bloom.might_contain(*key) {
        increment(&shared.runtime.amplification.bloom_positives, 1);
        true
    } else {
        increment(&shared.runtime.amplification.bloom_negatives, 1);
        false
    }
}

fn estimate_memtable_bytes(
    memtable: &BTreeMap<PhysicalKey, BTreeMap<LsmCommitSeq, EntryValue>>,
) -> Result<u64, StorageError> {
    let mut bytes = 0_u64;
    for versions in memtable.values() {
        for value in versions.values() {
            bytes =
                bytes
                    .checked_add(40 + value_size(value))
                    .ok_or(StorageError::ResourceLimit {
                        resource: "LSM MemTable bytes",
                        limit: u64::MAX,
                    })?;
        }
    }
    Ok(bytes)
}

fn value_size(value: &EntryValue) -> u64 {
    match value {
        EntryValue::Put(row) => row.len() as u64,
        EntryValue::Tombstone => 0,
    }
}

fn ensure_maintenance_safe(shared: &LsmShared) -> Result<(), StorageError> {
    match maintenance_safety_blocker(shared) {
        Some(LsmMaintenanceSafetyBlocker::RecoveryRequired) => {
            Err(CheckpointError::RecoveryRequired.into())
        }
        Some(LsmMaintenanceSafetyBlocker::WriterActive { txn_id }) => {
            Err(CheckpointError::WriterActive { txn_id }.into())
        }
        Some(LsmMaintenanceSafetyBlocker::OutstandingTransactions { count }) => {
            Err(CheckpointError::OutstandingTransactions { count }.into())
        }
        Some(LsmMaintenanceSafetyBlocker::OutstandingReadViews { .. }) => {
            Err(LsmError::Busy("outstanding read views").into())
        }
        None => Ok(()),
    }
}

fn maintenance_safety_blocker(shared: &LsmShared) -> Option<LsmMaintenanceSafetyBlocker> {
    if shared.runtime.recovery_required.get() {
        Some(LsmMaintenanceSafetyBlocker::RecoveryRequired)
    } else if let Some(txn_id) = shared.runtime.writer.get() {
        Some(LsmMaintenanceSafetyBlocker::WriterActive { txn_id })
    } else if shared.runtime.outstanding_transactions.get() != 0 {
        Some(LsmMaintenanceSafetyBlocker::OutstandingTransactions {
            count: shared.runtime.outstanding_transactions.get(),
        })
    } else if shared.runtime.outstanding_read_views.get() != 0 {
        Some(LsmMaintenanceSafetyBlocker::OutstandingReadViews {
            count: shared.runtime.outstanding_read_views.get(),
        })
    } else {
        None
    }
}

fn manifest_path(root: &Path) -> PathBuf {
    root.join(MANIFEST_NAME)
}
fn manifest_next_path(root: &Path) -> PathBuf {
    root.join(MANIFEST_NEXT_NAME)
}
fn wal_file_name(generation: u64) -> String {
    format!("wal-{generation:020}.nblw")
}
fn wal_path(root: &Path, generation: u64) -> PathBuf {
    root.join(wal_file_name(generation))
}
fn sstable_file_name(id: u64, level: u8) -> String {
    format!("sst-{id:020}-l{level}.nbls")
}
fn sstable_path(root: &Path, id: u64, level: u8) -> PathBuf {
    root.join(SST_DIR_NAME).join(sstable_file_name(id, level))
}
fn sstable_temp_path(root: &Path, id: u64, level: u8) -> PathBuf {
    root.join(SST_DIR_NAME)
        .join(format!("{}.next", sstable_file_name(id, level)))
}

fn encode_manifest(manifest: &Manifest) -> Result<Vec<u8>, StorageError> {
    if manifest.storage_id.0 == 0
        || manifest.table_id.0 == 0
        || manifest.generation == 0
        || manifest.wal_generation == 0
        || manifest.next_sstable_id == 0
    {
        return Err(LsmError::InvalidManifest("identity or generation is zero").into());
    }
    if manifest.sstables.len() > MAX_SSTABLES {
        return Err(StorageError::ResourceLimit {
            resource: "LSM manifest SSTable count",
            limit: MAX_SSTABLES as u64,
        });
    }
    validate_manifest_sstable_layout(&manifest.sstables, manifest.key_type)?;
    let count = u32::try_from(manifest.sstables.len())
        .map_err(|_| LsmError::InvalidManifest("SSTable count exceeds u32"))?;
    let total = MANIFEST_FIXED_SIZE
        .checked_add(
            manifest
                .sstables
                .len()
                .checked_mul(MANIFEST_ENTRY_SIZE)
                .ok_or(LsmError::InvalidManifest("manifest size overflows"))?,
        )
        .ok_or(LsmError::InvalidManifest("manifest size overflows"))?;
    let mut bytes = vec![0_u8; total];
    bytes[0..4].copy_from_slice(MANIFEST_MAGIC);
    bytes[4..6].copy_from_slice(&LSM_MANIFEST_FORMAT_VERSION.to_le_bytes());
    bytes[8..16].copy_from_slice(&manifest.storage_id.0.to_le_bytes());
    bytes[16..24].copy_from_slice(&manifest.table_id.0.to_le_bytes());
    bytes[24..56].copy_from_slice(manifest.schema_fingerprint.as_bytes());
    bytes[56..60].copy_from_slice(&manifest.clustering_column.0.to_le_bytes());
    bytes[60] = physical_type_tag(manifest.key_type)?;
    bytes[64..72].copy_from_slice(&manifest.generation.to_le_bytes());
    bytes[72..80].copy_from_slice(&manifest.wal_generation.to_le_bytes());
    bytes[80..88].copy_from_slice(&manifest.row_reservation_end.to_le_bytes());
    bytes[88..96].copy_from_slice(&manifest.txn_reservation_end.to_le_bytes());
    bytes[96..104].copy_from_slice(&manifest.commit_reservation_end.to_le_bytes());
    bytes[104..112].copy_from_slice(&manifest.next_sstable_id.to_le_bytes());
    match (
        manifest.table_statistics,
        manifest.access_statistics,
        manifest.clustering_statistics,
    ) {
        (None, None, None) => {}
        (Some(table), Some(access), bounds) => {
            bytes[112] = 1;
            bytes[120..128].copy_from_slice(&table.row_count.to_le_bytes());
            bytes[128..136].copy_from_slice(&table.managed_page_count.to_le_bytes());
            bytes[136..144].copy_from_slice(&access.distinct_non_null_keys.to_le_bytes());
            bytes[144..152].copy_from_slice(&access.null_count.to_le_bytes());
            bytes[152..156].copy_from_slice(&access.tree_height.to_le_bytes());
            match (table.row_count, bounds) {
                (0, None) => {}
                (1.., Some((min, max))) if min.kind() == manifest.key_type && min <= max => {
                    bytes[160..168].copy_from_slice(&min.bits().to_le_bytes());
                    bytes[168..176].copy_from_slice(&max.bits().to_le_bytes());
                }
                _ => {
                    return Err(
                        LsmError::InvalidManifest("statistics key bounds are invalid").into(),
                    );
                }
            }
        }
        _ => return Err(LsmError::InvalidManifest("statistics presence differs").into()),
    }
    bytes[156..160].copy_from_slice(&count.to_le_bytes());
    for (index, sstable) in manifest.sstables.iter().enumerate() {
        let offset = MANIFEST_FIXED_SIZE + index * MANIFEST_ENTRY_SIZE;
        bytes[offset..offset + 8].copy_from_slice(&sstable.id.to_le_bytes());
        bytes[offset + 8] = sstable.level;
        bytes[offset + 12..offset + 20].copy_from_slice(&sstable.entry_count.to_le_bytes());
        bytes[offset + 20..offset + 28].copy_from_slice(&sstable.file_bytes.to_le_bytes());
        bytes[offset + 28..offset + 36].copy_from_slice(&sstable.bloom_bytes.to_le_bytes());
        encode_physical_key(&mut bytes[offset + 36..offset + 56], sstable.min)?;
        encode_physical_key(&mut bytes[offset + 56..offset + 76], sstable.max)?;
    }
    let checksum = crc32c::crc32c(&bytes);
    bytes.extend_from_slice(&checksum.to_le_bytes());
    Ok(bytes)
}

fn decode_manifest(bytes: &[u8]) -> Result<Manifest, StorageError> {
    if bytes.len() < MANIFEST_FIXED_SIZE + 4 {
        return Err(LsmError::InvalidManifest("file is truncated").into());
    }
    if &bytes[0..4] != MANIFEST_MAGIC {
        return Err(LsmError::InvalidManifest("magic does not match").into());
    }
    let version = read_u16(bytes, 4)?;
    if version != LSM_MANIFEST_FORMAT_VERSION {
        return Err(LsmError::UnsupportedManifestVersion(version).into());
    }
    if bytes[6..8].iter().any(|byte| *byte != 0)
        || bytes[61..64].iter().any(|byte| *byte != 0)
        || bytes[113..120].iter().any(|byte| *byte != 0)
    {
        return Err(LsmError::InvalidManifest("reserved bytes are nonzero").into());
    }
    let count = usize::try_from(read_u32(bytes, 156)?)
        .map_err(|_| LsmError::InvalidManifest("SSTable count does not fit usize"))?;
    if count > MAX_SSTABLES {
        return Err(LsmError::InvalidManifest("SSTable count exceeds bound").into());
    }
    let expected = MANIFEST_FIXED_SIZE
        .checked_add(
            count
                .checked_mul(MANIFEST_ENTRY_SIZE)
                .ok_or(LsmError::InvalidManifest("manifest length overflows"))?,
        )
        .and_then(|size| size.checked_add(4))
        .ok_or(LsmError::InvalidManifest("manifest length overflows"))?;
    if bytes.len() != expected {
        return Err(LsmError::InvalidManifest("file length does not match SSTable count").into());
    }
    let stored = read_u32(bytes, expected - 4)?;
    let computed = crc32c::crc32c(&bytes[..expected - 4]);
    if stored != computed {
        return Err(LsmError::ManifestChecksum { stored, computed }.into());
    }
    let storage_id = StorageId(read_u64(bytes, 8)?);
    let table_id = TableId(read_u64(bytes, 16)?);
    let schema_fingerprint = SchemaFingerprint::from_bytes(
        bytes[24..56]
            .try_into()
            .map_err(|_| LsmError::InvalidManifest("schema fingerprint is truncated"))?,
    );
    let clustering_column = ColumnId(read_u32(bytes, 56)?);
    let key_type = decode_physical_type(bytes[60])?;
    let generation = read_u64(bytes, 64)?;
    let wal_generation = read_u64(bytes, 72)?;
    let row_reservation_end = read_u64(bytes, 80)?;
    let txn_reservation_end = read_u64(bytes, 88)?;
    let commit_reservation_end = read_u64(bytes, 96)?;
    let next_sstable_id = read_u64(bytes, 104)?;
    let (table_statistics, access_statistics, clustering_statistics) = match bytes[112] {
        0 => {
            if bytes[120..156]
                .iter()
                .chain(bytes[160..176].iter())
                .any(|byte| *byte != 0)
            {
                return Err(LsmError::InvalidManifest("absent statistics contain data").into());
            }
            (None, None, None)
        }
        1 => {
            let row_count = read_u64(bytes, 120)?;
            let bounds = if row_count == 0 {
                if bytes[160..176].iter().any(|byte| *byte != 0) {
                    return Err(
                        LsmError::InvalidManifest("empty statistics contain key bounds").into(),
                    );
                }
                None
            } else {
                let min = decode_key_bits(read_u64(bytes, 160)?, key_type)?;
                let max = decode_key_bits(read_u64(bytes, 168)?, key_type)?;
                if min > max {
                    return Err(
                        LsmError::InvalidManifest("statistics key bounds are reversed").into(),
                    );
                }
                Some((min, max))
            };
            (
                Some(TableStatistics {
                    row_count,
                    managed_page_count: read_u64(bytes, 128)?,
                }),
                Some(IndexStatistics {
                    distinct_non_null_keys: read_u64(bytes, 136)?,
                    null_count: read_u64(bytes, 144)?,
                    tree_height: read_u32(bytes, 152)?,
                }),
                bounds,
            )
        }
        _ => return Err(LsmError::InvalidManifest("statistics presence tag is invalid").into()),
    };
    if storage_id.0 == 0
        || table_id.0 == 0
        || generation == 0
        || wal_generation == 0
        || row_reservation_end == 0
        || txn_reservation_end == 0
        || commit_reservation_end == 0
        || next_sstable_id == 0
    {
        return Err(LsmError::InvalidManifest("identity or allocator high-water is zero").into());
    }
    let mut sstables = Vec::with_capacity(count);
    let mut ids = BTreeSet::new();
    for index in 0..count {
        let offset = MANIFEST_FIXED_SIZE + index * MANIFEST_ENTRY_SIZE;
        if bytes[offset + 9..offset + 12].iter().any(|byte| *byte != 0) {
            return Err(LsmError::InvalidManifest("SSTable reserved bytes are nonzero").into());
        }
        let id = read_u64(bytes, offset)?;
        let level = bytes[offset + 8];
        let entry_count = read_u64(bytes, offset + 12)?;
        let file_bytes = read_u64(bytes, offset + 20)?;
        let bloom_bytes = read_u64(bytes, offset + 28)?;
        let min = decode_physical_key(&bytes[offset + 36..offset + 56], key_type)?;
        let max = decode_physical_key(&bytes[offset + 56..offset + 76], key_type)?;
        if id == 0
            || !ids.insert(id)
            || level >= LSM_MAX_LEVELS
            || entry_count == 0
            || file_bytes < SST_HEADER_SIZE as u64
            || bloom_bytes == 0
            || bloom_bytes > BLOOM_MAX_BITS / 8
            || min > max
        {
            return Err(LsmError::InvalidManifest("invalid SSTable descriptor").into());
        }
        sstables.push(SstableRef {
            id,
            level,
            entry_count,
            file_bytes,
            bloom_bytes,
            min,
            max,
        });
    }
    validate_manifest_sstable_layout(&sstables, key_type)?;
    if ids
        .iter()
        .next_back()
        .is_some_and(|id| *id >= next_sstable_id)
    {
        return Err(LsmError::InvalidManifest("SSTable allocator high-water is stale").into());
    }
    Ok(Manifest {
        storage_id,
        table_id,
        schema_fingerprint,
        clustering_column,
        key_type,
        generation,
        wal_generation,
        row_reservation_end,
        txn_reservation_end,
        commit_reservation_end,
        next_sstable_id,
        table_statistics,
        access_statistics,
        clustering_statistics,
        sstables,
    })
}

fn validate_manifest_sstable_layout(
    sstables: &[SstableRef],
    key_type: PhysicalType,
) -> Result<(), StorageError> {
    let mut previous: Option<&SstableRef> = None;
    let mut ids = BTreeSet::new();
    for current in sstables {
        if current.id == 0
            || !ids.insert(current.id)
            || current.level >= LSM_MAX_LEVELS
            || current.entry_count == 0
            || current.file_bytes < SST_HEADER_SIZE as u64
            || current.bloom_bytes == 0
            || current.bloom_bytes > BLOOM_MAX_BITS / 8
            || current.min > current.max
            || current.min.clustering.kind() != key_type
            || current.max.clustering.kind() != key_type
        {
            return Err(LsmError::InvalidManifest("invalid SSTable descriptor").into());
        }
        if let Some(previous) = previous {
            if previous.level > current.level {
                return Err(LsmError::InvalidManifest("SSTable levels are not sorted").into());
            }
            if previous.level == current.level {
                if current.level == 0 {
                    if previous.id >= current.id {
                        return Err(LsmError::InvalidManifest(
                            "L0 generation order is not canonical",
                        )
                        .into());
                    }
                } else if previous.min >= current.min || previous.max >= current.min {
                    return Err(LsmError::InvalidManifest(
                        "L1+ SSTables are unsorted or overlapping",
                    )
                    .into());
                }
            }
        }
        previous = Some(current);
    }
    Ok(())
}

fn write_manifest_initial(root: &Path, manifest: &Manifest) -> Result<(), StorageError> {
    let bytes = encode_manifest(manifest)?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(manifest_path(root))?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    Ok(())
}

fn publish_manifest(root: &Path, mut manifest: Manifest) -> Result<Manifest, ManifestPublishError> {
    manifest.generation = manifest
        .generation
        .checked_add(1)
        .ok_or(LsmError::AllocatorExhausted("manifest generation"))
        .map_err(StorageError::from)
        .map_err(ManifestPublishError::BeforeInstall)?;
    let bytes = encode_manifest(&manifest).map_err(ManifestPublishError::BeforeInstall)?;
    let next = manifest_next_path(root);
    match OpenOptions::new().write(true).create_new(true).open(&next) {
        Ok(mut file) => {
            maybe_fail_manifest_publish(ManifestPublishPoint::CandidateWrite)
                .map_err(ManifestPublishError::BeforeInstall)?;
            file.write_all(&bytes)
                .map_err(StorageError::from)
                .map_err(ManifestPublishError::BeforeInstall)?;
            #[cfg(test)]
            maybe_lsm_crash("during-manifest-write");
            maybe_fail_manifest_publish(ManifestPublishPoint::CandidateSync)
                .map_err(ManifestPublishError::BeforeInstall)?;
            file.sync_all()
                .map_err(StorageError::from)
                .map_err(ManifestPublishError::BeforeInstall)?;
            #[cfg(test)]
            maybe_lsm_crash("after-manifest-write");
        }
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            fs::remove_file(&next)
                .map_err(StorageError::from)
                .map_err(ManifestPublishError::BeforeInstall)?;
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&next)
                .map_err(StorageError::from)
                .map_err(ManifestPublishError::BeforeInstall)?;
            maybe_fail_manifest_publish(ManifestPublishPoint::CandidateWrite)
                .map_err(ManifestPublishError::BeforeInstall)?;
            file.write_all(&bytes)
                .map_err(StorageError::from)
                .map_err(ManifestPublishError::BeforeInstall)?;
            #[cfg(test)]
            maybe_lsm_crash("during-manifest-write");
            maybe_fail_manifest_publish(ManifestPublishPoint::CandidateSync)
                .map_err(ManifestPublishError::BeforeInstall)?;
            file.sync_all()
                .map_err(StorageError::from)
                .map_err(ManifestPublishError::BeforeInstall)?;
            #[cfg(test)]
            maybe_lsm_crash("after-manifest-write");
        }
        Err(error) => {
            return Err(ManifestPublishError::BeforeInstall(error.into()));
        }
    }
    maybe_fail_manifest_publish(ManifestPublishPoint::BeforeInstall)
        .map_err(ManifestPublishError::BeforeInstall)?;
    fs::rename(&next, manifest_path(root))
        .map_err(StorageError::from)
        .map_err(ManifestPublishError::BeforeInstall)?;
    if let Err(source) = maybe_fail_manifest_publish(ManifestPublishPoint::AfterInstall)
        .and_then(|()| sync_directory(root))
    {
        return Err(ManifestPublishError::InstalledButUnsynced {
            manifest: Box::new(manifest),
            source,
        });
    }
    #[cfg(test)]
    maybe_lsm_crash("after-manifest-sync");
    Ok(manifest)
}

fn finish_manifest_publish(
    shared: &mut LsmShared,
    result: Result<Manifest, ManifestPublishError>,
) -> Result<(), StorageError> {
    match result {
        Ok(manifest) => {
            shared.manifest = manifest;
            Ok(())
        }
        Err(ManifestPublishError::BeforeInstall(error)) => Err(error),
        Err(ManifestPublishError::InstalledButUnsynced { manifest, source }) => {
            // The rename is visible, but its directory entry may not survive a
            // crash. Keep both generations and require reopen before any more
            // writes can depend on which manifest generation wins.
            shared.manifest = *manifest;
            shared.runtime.recovery_required.set(true);
            Err(source)
        }
    }
}

fn read_manifest(root: &Path) -> Result<Manifest, StorageError> {
    let path = manifest_path(root);
    let metadata = fs::metadata(&path)?;
    let length = usize::try_from(metadata.len())
        .map_err(|_| LsmError::InvalidManifest("file is too large"))?;
    let max = MANIFEST_FIXED_SIZE + MAX_SSTABLES * MANIFEST_ENTRY_SIZE + 4;
    if length > max {
        return Err(LsmError::InvalidManifest("file exceeds bounded maximum").into());
    }
    let mut bytes = vec![0_u8; length];
    File::open(path)?.read_exact(&mut bytes)?;
    decode_manifest(&bytes)
}

fn validate_manifest_schema(manifest: &Manifest, table: &TableDef) -> Result<(), StorageError> {
    if manifest.table_id != table.id {
        return Err(StorageError::TableIdMismatch {
            expected: table.id,
            actual: manifest.table_id,
        });
    }
    let expected = table.fingerprint()?;
    if manifest.schema_fingerprint != expected {
        return Err(StorageError::SchemaMismatch {
            expected,
            actual: manifest.schema_fingerprint,
        });
    }
    Ok(())
}

impl LsmWal {
    fn create(root: &Path, storage_id: StorageId, generation: u64) -> Result<Self, StorageError> {
        let path = wal_path(root, generation);
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)?;
        let header = encode_wal_header(storage_id, generation);
        file.write_all(&header)?;
        #[cfg(test)]
        maybe_lsm_crash("during-new-wal-creation");
        file.sync_all()?;
        #[cfg(test)]
        maybe_lsm_crash("after-new-wal-sync");
        Ok(Self {
            file,
            path,
            storage_id,
            end: WAL_HEADER_SIZE as u64,
            #[cfg(test)]
            fail_next_sync: false,
            #[cfg(test)]
            fail_append_after_calls: None,
        })
    }

    fn open(
        root: &Path,
        storage_id: StorageId,
        generation: u64,
    ) -> Result<(Self, Vec<WalRecord>), StorageError> {
        let path = wal_path(root, generation);
        let mut file = OpenOptions::new().read(true).write(true).open(&path)?;
        let mut header = [0_u8; WAL_HEADER_SIZE];
        file.read_exact(&mut header).map_err(|error| {
            if error.kind() == io::ErrorKind::UnexpectedEof {
                StorageError::from(LsmError::InvalidWal("header is truncated"))
            } else {
                error.into()
            }
        })?;
        decode_wal_header(&header, storage_id, generation)?;
        let (records, valid_end) = decode_wal_records(&mut file, storage_id)?;
        if valid_end < file.metadata()?.len() {
            file.set_len(valid_end)?;
            file.sync_all()?;
        }
        file.seek(SeekFrom::Start(valid_end))?;
        Ok((
            Self {
                file,
                path,
                storage_id,
                end: valid_end,
                #[cfg(test)]
                fail_next_sync: false,
                #[cfg(test)]
                fail_append_after_calls: None,
            },
            records,
        ))
    }

    fn inspect(
        root: &Path,
        storage_id: StorageId,
        generation: u64,
    ) -> Result<Vec<WalRecord>, StorageError> {
        let mut file = File::open(wal_path(root, generation))?;
        let mut header = [0_u8; WAL_HEADER_SIZE];
        file.read_exact(&mut header).map_err(|error| {
            if error.kind() == io::ErrorKind::UnexpectedEof {
                StorageError::from(LsmError::InvalidWal("header is truncated"))
            } else {
                error.into()
            }
        })?;
        decode_wal_header(&header, storage_id, generation)?;
        let (records, _) = decode_wal_records(&mut file, storage_id)?;
        Ok(records)
    }

    fn append(&mut self, record: &WalRecord) -> Result<Lsn, StorageError> {
        #[cfg(test)]
        if let Some(remaining) = self.fail_append_after_calls.as_mut() {
            if *remaining == 0 {
                self.fail_append_after_calls = None;
                return Err(io::Error::other("injected LSM WAL append failure").into());
            }
            *remaining -= 1;
        }
        let bytes = encode_wal_record(record, self.storage_id)?;
        let lsn = Lsn(self.end);
        self.file.seek(SeekFrom::Start(self.end))?;
        self.file.write_all(&bytes)?;
        self.end = self
            .end
            .checked_add(bytes.len() as u64)
            .ok_or(LsmError::InvalidWal("file offset overflows"))?;
        Ok(lsn)
    }

    fn sync(&mut self) -> Result<(), StorageError> {
        #[cfg(test)]
        if std::mem::take(&mut self.fail_next_sync) {
            return Err(io::Error::other("injected LSM WAL sync failure").into());
        }
        self.file.sync_data().map_err(Into::into)
    }
}

fn encode_wal_header(storage_id: StorageId, generation: u64) -> [u8; WAL_HEADER_SIZE] {
    let mut bytes = [0_u8; WAL_HEADER_SIZE];
    bytes[0..4].copy_from_slice(WAL_MAGIC);
    bytes[4..6].copy_from_slice(&LSM_WAL_FORMAT_VERSION.to_le_bytes());
    bytes[8..16].copy_from_slice(&storage_id.0.to_le_bytes());
    bytes[16..24].copy_from_slice(&generation.to_le_bytes());
    let checksum = crc32c::crc32c(&bytes[..28]);
    bytes[28..32].copy_from_slice(&checksum.to_le_bytes());
    bytes
}

fn decode_wal_header(
    bytes: &[u8; WAL_HEADER_SIZE],
    expected_storage: StorageId,
    expected_generation: u64,
) -> Result<(), StorageError> {
    if &bytes[0..4] != WAL_MAGIC {
        return Err(LsmError::InvalidWal("header magic does not match").into());
    }
    let version = read_u16(bytes, 4)?;
    if version != LSM_WAL_FORMAT_VERSION {
        return Err(LsmError::UnsupportedWalVersion(version).into());
    }
    if bytes[6..8].iter().any(|byte| *byte != 0) || bytes[24..28].iter().any(|byte| *byte != 0) {
        return Err(LsmError::InvalidWal("header reserved bytes are nonzero").into());
    }
    let actual_storage = StorageId(read_u64(bytes, 8)?);
    if actual_storage != expected_storage {
        return Err(LsmError::StorageIdMismatch {
            expected: expected_storage,
            actual: actual_storage,
        }
        .into());
    }
    if read_u64(bytes, 16)? != expected_generation {
        return Err(LsmError::InvalidWal("generation differs from manifest").into());
    }
    let stored = read_u32(bytes, 28)?;
    let computed = crc32c::crc32c(&bytes[..28]);
    if stored != computed {
        return Err(LsmError::WalChecksum {
            offset: 0,
            stored,
            computed,
        }
        .into());
    }
    Ok(())
}

fn encode_wal_record(record: &WalRecord, storage_id: StorageId) -> Result<Vec<u8>, StorageError> {
    let (tag, mut payload) = match record {
        WalRecord::MutationBatch { txn_id, mutations } => {
            let mut payload = Vec::new();
            payload.extend_from_slice(&txn_id.0.to_le_bytes());
            payload.extend_from_slice(
                &(u32::try_from(mutations.len()).map_err(|_| StorageError::ResourceLimit {
                    resource: "LSM WAL mutation count",
                    limit: LSM_MAX_PENDING_MUTATIONS,
                })?)
                .to_le_bytes(),
            );
            for mutation in mutations {
                encode_wal_mutation(&mut payload, mutation)?;
            }
            (1_u8, payload)
        }
        WalRecord::Prepare {
            txn_id,
            database_txn_id,
        } => {
            let mut payload = Vec::with_capacity(16);
            payload.extend_from_slice(&txn_id.0.to_le_bytes());
            payload.extend_from_slice(&database_txn_id.0.to_le_bytes());
            (2, payload)
        }
        WalRecord::Commit { txn_id, commit_seq } => {
            let mut payload = Vec::with_capacity(16);
            payload.extend_from_slice(&txn_id.0.to_le_bytes());
            payload.extend_from_slice(&commit_seq.0.to_le_bytes());
            (3, payload)
        }
        WalRecord::Abort { txn_id } => (4, txn_id.0.to_le_bytes().to_vec()),
    };
    if payload.len() > WAL_MAX_RECORD_BYTES {
        return Err(StorageError::ResourceLimit {
            resource: "LSM WAL record bytes",
            limit: WAL_MAX_RECORD_BYTES as u64,
        });
    }
    let mut bytes = Vec::with_capacity(WAL_RECORD_HEADER_SIZE + payload.len());
    bytes.extend_from_slice(WAL_RECORD_MAGIC);
    bytes.extend_from_slice(&LSM_WAL_FORMAT_VERSION.to_le_bytes());
    bytes.push(tag);
    bytes.push(0);
    bytes.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&storage_id.0.to_le_bytes());
    bytes.append(&mut payload);
    let checksum = crc32c::crc32c(&bytes);
    bytes.extend_from_slice(&checksum.to_le_bytes());
    Ok(bytes)
}

fn encode_wal_mutation(bytes: &mut Vec<u8>, mutation: &WalMutation) -> Result<(), StorageError> {
    let (tag, key) = match mutation {
        WalMutation::Put { key, .. } => (1_u8, *key),
        WalMutation::Tombstone { key } => (2_u8, *key),
    };
    bytes.push(tag);
    bytes.extend_from_slice(&[0; 3]);
    encode_physical_key_vec(bytes, key)?;
    match mutation {
        WalMutation::Put { row, .. } => {
            let length = u32::try_from(row.len()).map_err(|_| StorageError::ResourceLimit {
                resource: "LSM WAL row bytes",
                limit: u32::MAX as u64,
            })?;
            bytes.extend_from_slice(&length.to_le_bytes());
            bytes.extend_from_slice(row);
        }
        WalMutation::Tombstone { .. } => bytes.extend_from_slice(&0_u32.to_le_bytes()),
    }
    Ok(())
}

fn decode_wal_records(
    file: &mut File,
    expected_storage: StorageId,
) -> Result<(Vec<WalRecord>, u64), StorageError> {
    let mut records = Vec::new();
    let mut offset = WAL_HEADER_SIZE as u64;
    loop {
        file.seek(SeekFrom::Start(offset))?;
        let mut header = [0_u8; WAL_RECORD_HEADER_SIZE];
        let read = read_partial(file, &mut header)?;
        if read == 0 {
            return Ok((records, offset));
        }
        if read < header.len() {
            return Ok((records, offset));
        }
        if &header[0..4] != WAL_RECORD_MAGIC {
            return Err(LsmError::InvalidWal("record magic does not match").into());
        }
        let version = read_u16(&header, 4)?;
        if version != LSM_WAL_FORMAT_VERSION {
            return Err(LsmError::UnsupportedWalVersion(version).into());
        }
        if header[7] != 0 {
            return Err(LsmError::InvalidWal("record reserved byte is nonzero").into());
        }
        let length = usize::try_from(read_u32(&header, 8)?)
            .map_err(|_| LsmError::InvalidWal("record length does not fit usize"))?;
        if length > WAL_MAX_RECORD_BYTES {
            return Err(LsmError::InvalidWal("record length exceeds bound").into());
        }
        let actual_storage = StorageId(read_u64(&header, 12)?);
        if actual_storage != expected_storage {
            return Err(LsmError::StorageIdMismatch {
                expected: expected_storage,
                actual: actual_storage,
            }
            .into());
        }
        let mut payload_and_checksum = vec![
            0_u8;
            length.checked_add(4).ok_or(LsmError::InvalidWal(
                "record length overflows"
            ))?
        ];
        let read = read_partial(file, &mut payload_and_checksum)?;
        if read < payload_and_checksum.len() {
            return Ok((records, offset));
        }
        let mut checksummed = Vec::with_capacity(header.len() + length);
        checksummed.extend_from_slice(&header);
        checksummed.extend_from_slice(&payload_and_checksum[..length]);
        let stored = read_u32(&payload_and_checksum, length)?;
        let computed = crc32c::crc32c(&checksummed);
        if stored != computed {
            return Err(LsmError::WalChecksum {
                offset,
                stored,
                computed,
            }
            .into());
        }
        records.push(decode_wal_record(
            header[6],
            &payload_and_checksum[..length],
        )?);
        offset = offset
            .checked_add((header.len() + payload_and_checksum.len()) as u64)
            .ok_or(LsmError::InvalidWal("record end offset overflows"))?;
    }
}

fn decode_wal_record(tag: u8, payload: &[u8]) -> Result<WalRecord, StorageError> {
    let expected_length = match tag {
        2 | 3 => Some(16),
        4 => Some(8),
        _ => None,
    };
    if expected_length.is_some_and(|expected| payload.len() != expected) {
        return Err(LsmError::InvalidWal("record payload has invalid length").into());
    }
    match tag {
        1 => {
            if payload.len() < 12 {
                return Err(LsmError::InvalidWal("mutation batch is truncated").into());
            }
            let txn_id = TxnId(read_u64(payload, 0)?);
            let count = usize::try_from(read_u32(payload, 8)?)
                .map_err(|_| LsmError::InvalidWal("mutation count does not fit usize"))?;
            if txn_id.0 == 0 || count > LSM_MAX_PENDING_MUTATIONS as usize {
                return Err(
                    LsmError::InvalidWal("invalid transaction ID or mutation count").into(),
                );
            }
            let mut offset = 12;
            let mut mutations = Vec::with_capacity(count);
            let mut previous = None;
            for _ in 0..count {
                if payload.len().saturating_sub(offset) < 28 {
                    return Err(LsmError::InvalidWal("mutation is truncated").into());
                }
                let mutation_tag = payload[offset];
                if payload[offset + 1..offset + 4]
                    .iter()
                    .any(|byte| *byte != 0)
                {
                    return Err(LsmError::InvalidWal("mutation reserved bytes are nonzero").into());
                }
                let key_type = decode_physical_type(payload[offset + 4])?;
                let key = decode_physical_key(&payload[offset + 4..offset + 24], key_type)?;
                let length = usize::try_from(read_u32(payload, offset + 24)?)
                    .map_err(|_| LsmError::InvalidWal("row length does not fit usize"))?;
                offset = offset
                    .checked_add(28)
                    .ok_or(LsmError::InvalidWal("mutation offset overflows"))?;
                let end = offset
                    .checked_add(length)
                    .ok_or(LsmError::InvalidWal("row length overflows"))?;
                let row = payload
                    .get(offset..end)
                    .ok_or(LsmError::InvalidWal("row payload is truncated"))?;
                offset = end;
                if previous.is_some_and(|previous| previous > key) {
                    return Err(LsmError::InvalidWal("mutation batch is not sorted").into());
                }
                previous = Some(key);
                mutations.push(match mutation_tag {
                    1 => WalMutation::Put {
                        key,
                        row: row.to_vec(),
                    },
                    2 if length == 0 => WalMutation::Tombstone { key },
                    2 => return Err(LsmError::InvalidWal("tombstone has row bytes").into()),
                    _ => return Err(LsmError::InvalidWal("unknown mutation tag").into()),
                });
            }
            if offset != payload.len() {
                return Err(LsmError::InvalidWal("mutation batch has trailing bytes").into());
            }
            Ok(WalRecord::MutationBatch { txn_id, mutations })
        }
        2 => {
            let txn_id = TxnId(read_u64(payload, 0)?);
            let database_txn_id = DatabaseTxnId(read_u64(payload, 8)?);
            if txn_id.0 == 0 || database_txn_id.0 == 0 {
                return Err(LsmError::InvalidWal("prepare identity is zero").into());
            }
            Ok(WalRecord::Prepare {
                txn_id,
                database_txn_id,
            })
        }
        3 => {
            let txn_id = TxnId(read_u64(payload, 0)?);
            let commit_seq = LsmCommitSeq(read_u64(payload, 8)?);
            if txn_id.0 == 0 || commit_seq.0 == 0 {
                return Err(LsmError::InvalidWal("commit identity is zero").into());
            }
            Ok(WalRecord::Commit { txn_id, commit_seq })
        }
        4 => {
            let txn_id = TxnId(read_u64(payload, 0)?);
            if txn_id.0 == 0 {
                return Err(LsmError::InvalidWal("abort transaction ID is zero").into());
            }
            Ok(WalRecord::Abort { txn_id })
        }
        _ => Err(LsmError::InvalidWal("unknown record tag").into()),
    }
}

fn analyze_wal(records: &[WalRecord]) -> Result<BTreeMap<TxnId, RecoveredTxn>, StorageError> {
    let mut transactions = BTreeMap::<TxnId, RecoveredTxn>::new();
    let mut commits = BTreeSet::new();
    for (position, record) in records.iter().enumerate() {
        match record {
            WalRecord::MutationBatch { txn_id, mutations } => {
                let txn = transactions.entry(*txn_id).or_default();
                if txn.mutations.replace(mutations.clone()).is_some()
                    || txn.commit.is_some()
                    || txn.aborted
                {
                    return Err(LsmError::InvalidWal("duplicate or late mutation batch").into());
                }
            }
            WalRecord::Prepare {
                txn_id,
                database_txn_id,
            } => {
                let txn = transactions.entry(*txn_id).or_default();
                if txn.mutations.is_none()
                    || txn.prepared.replace(*database_txn_id).is_some()
                    || txn.prepare_order.replace(position as u64 + 1).is_some()
                    || txn.commit.is_some()
                    || txn.aborted
                {
                    return Err(LsmError::InvalidWal("invalid prepare state transition").into());
                }
            }
            WalRecord::Commit { txn_id, commit_seq } => {
                let txn = transactions.entry(*txn_id).or_default();
                if txn.mutations.is_none()
                    || txn.commit.replace(*commit_seq).is_some()
                    || txn.aborted
                    || !commits.insert(*commit_seq)
                {
                    return Err(LsmError::InvalidWal("invalid or duplicate commit state").into());
                }
            }
            WalRecord::Abort { txn_id } => {
                let txn = transactions.entry(*txn_id).or_default();
                if txn.commit.is_some() || txn.aborted {
                    return Err(LsmError::InvalidWal("invalid abort state").into());
                }
                txn.aborted = true;
            }
        }
    }
    Ok(transactions)
}

fn classify_prepared(
    recovered: &BTreeMap<TxnId, RecoveredTxn>,
) -> Result<Vec<PreparedTransaction>, StorageError> {
    recovered
        .iter()
        .filter_map(|(txn_id, txn)| {
            txn.prepared.map(|database_txn_id| {
                Ok(PreparedTransaction {
                    database_txn_id,
                    physical_txn_id: *txn_id,
                    prepare_order: txn.prepare_order.ok_or(LsmError::InvalidWal(
                        "prepared transaction has no WAL order",
                    ))?,
                    state: if txn.commit.is_some() {
                        PreparedTransactionState::Committed
                    } else if txn.aborted {
                        PreparedTransactionState::RolledBack
                    } else {
                        PreparedTransactionState::Prepared
                    },
                })
            })
        })
        .collect()
}

fn validate_resolutions(
    prepared: &[PreparedTransaction],
    resolutions: &[PreparedTxnResolution],
) -> Result<(), StorageError> {
    let mut seen = BTreeSet::new();
    for resolution in resolutions {
        if !seen.insert(resolution.physical_txn_id) {
            return Err(RecoveryError::DuplicatePreparedResolution {
                physical_txn_id: resolution.physical_txn_id,
            }
            .into());
        }
        let transaction = prepared
            .iter()
            .find(|prepared| prepared.physical_txn_id == resolution.physical_txn_id)
            .ok_or(RecoveryError::UnknownPreparedResolution {
                database_txn_id: resolution.database_txn_id,
                physical_txn_id: resolution.physical_txn_id,
            })?;
        if transaction.database_txn_id != resolution.database_txn_id {
            return Err(RecoveryError::PreparedResolutionMismatch {
                physical_txn_id: resolution.physical_txn_id,
                expected: transaction.database_txn_id,
                actual: resolution.database_txn_id,
            }
            .into());
        }
        let terminal_matches = matches!(
            (transaction.state, resolution.decision),
            (
                PreparedTransactionState::Committed,
                PreparedDecision::Commit
            ) | (
                PreparedTransactionState::RolledBack,
                PreparedDecision::Abort
            )
        );
        if transaction.state != PreparedTransactionState::Prepared && !terminal_matches {
            return Err(
                RecoveryError::PreparedResolutionConflictsWithTerminalState {
                    database_txn_id: resolution.database_txn_id,
                    physical_txn_id: resolution.physical_txn_id,
                    state: transaction.state,
                    decision: resolution.decision,
                }
                .into(),
            );
        }
    }
    for transaction in prepared
        .iter()
        .filter(|txn| txn.state == PreparedTransactionState::Prepared)
    {
        if !resolutions
            .iter()
            .any(|resolution| resolution.physical_txn_id == transaction.physical_txn_id)
        {
            return Err(RecoveryError::PreparedTransactionRequiresResolution {
                database_txn_id: transaction.database_txn_id,
                physical_txn_id: transaction.physical_txn_id,
            }
            .into());
        }
    }
    Ok(())
}

fn allocate_recovery_commit(
    manifest: &Manifest,
    recovered: &BTreeMap<TxnId, RecoveredTxn>,
) -> Result<u64, StorageError> {
    let maximum = recovered
        .values()
        .filter_map(|txn| txn.commit)
        .map(|seq| seq.0)
        .max()
        .unwrap_or(manifest.commit_reservation_end.saturating_sub(1));
    maximum
        .checked_add(1)
        .ok_or_else(|| LsmError::AllocatorExhausted("recovery commit sequence").into())
}

fn find_commit_seq(
    path: &Path,
    storage_id: StorageId,
    generation: u64,
    txn_id: TxnId,
) -> Result<Option<LsmCommitSeq>, StorageError> {
    let root = path
        .parent()
        .ok_or(LsmError::InvalidWal("WAL path has no parent"))?;
    let (_, records) = LsmWal::open(root, storage_id, generation)?;
    Ok(records.into_iter().find_map(|record| match record {
        WalRecord::Commit {
            txn_id: actual,
            commit_seq,
        } if actual == txn_id => Some(commit_seq),
        _ => None,
    }))
}

fn wal_has_prepare(
    path: &Path,
    storage_id: StorageId,
    generation: u64,
    txn_id: TxnId,
    database_txn_id: DatabaseTxnId,
) -> Result<bool, StorageError> {
    let root = path
        .parent()
        .ok_or(LsmError::InvalidWal("WAL path has no parent"))?;
    let (_, records) = LsmWal::open(root, storage_id, generation)?;
    Ok(records.into_iter().any(|record| {
        matches!(record, WalRecord::Prepare { txn_id: actual_txn, database_txn_id: actual_database }
            if actual_txn == txn_id && actual_database == database_txn_id)
    }))
}

fn ensure_commit_record(
    shared: &mut LsmShared,
    txn_id: TxnId,
    expected: LsmCommitSeq,
    current_lsn: Lsn,
) -> Result<(Lsn, bool), StorageError> {
    if let Some(existing) = find_commit_seq(
        &shared.wal.path,
        shared.manifest.storage_id,
        shared.manifest.wal_generation,
        txn_id,
    )? {
        if existing != expected {
            return Err(LsmError::InvalidWal("retry commit sequence changed").into());
        }
        return Ok((current_lsn, false));
    }
    shared
        .wal
        .append(&WalRecord::Commit {
            txn_id,
            commit_seq: expected,
        })
        .map(|lsn| (lsn, true))
}

fn ensure_prepare_record(
    shared: &mut LsmShared,
    txn_id: TxnId,
    database_txn_id: DatabaseTxnId,
    current_lsn: Lsn,
) -> Result<Lsn, StorageError> {
    if wal_has_prepare(
        &shared.wal.path,
        shared.manifest.storage_id,
        shared.manifest.wal_generation,
        txn_id,
        database_txn_id,
    )? {
        return Ok(current_lsn);
    }
    shared.wal.append(&WalRecord::Prepare {
        txn_id,
        database_txn_id,
    })
}

fn ensure_abort_record(
    shared: &mut LsmShared,
    txn_id: TxnId,
    current_lsn: Lsn,
) -> Result<Lsn, StorageError> {
    let root = shared
        .wal
        .path
        .parent()
        .ok_or(LsmError::InvalidWal("WAL path has no parent"))?;
    let (_, records) = LsmWal::open(
        root,
        shared.manifest.storage_id,
        shared.manifest.wal_generation,
    )?;
    if records
        .into_iter()
        .any(|record| matches!(record, WalRecord::Abort { txn_id: actual } if actual == txn_id))
    {
        return Ok(current_lsn);
    }
    shared.wal.append(&WalRecord::Abort { txn_id })
}

fn flush_memtable(shared: &mut LsmShared) -> Result<(), StorageError> {
    if shared.memtable.is_empty() {
        return Ok(());
    }
    let entries = shared
        .memtable
        .iter()
        .flat_map(|(key, versions)| {
            versions.iter().map(move |(version, value)| VersionedEntry {
                key: *key,
                version: *version,
                value: value.clone(),
            })
        })
        .collect::<Vec<_>>();
    let id = shared.manifest.next_sstable_id;
    let mut candidate = shared.manifest.clone();
    candidate.next_sstable_id = id
        .checked_add(1)
        .ok_or(LsmError::AllocatorExhausted("SSTable ID"))?;
    let sstable = write_sstable(
        &shared.root,
        &shared.manifest,
        id,
        0,
        &entries,
        &shared.table,
    )?;

    let old_wal_generation = shared.manifest.wal_generation;
    let new_wal_generation = old_wal_generation
        .checked_add(1)
        .ok_or(LsmError::AllocatorExhausted("WAL generation"))?;
    let new_wal = LsmWal::create(&shared.root, shared.manifest.storage_id, new_wal_generation)?;
    if let Err(error) = sync_directory(&shared.root) {
        drop(new_wal);
        if let Err(cleanup) = remove_unreferenced_file(&sstable.path)
            .and_then(|()| remove_unreferenced_file(&wal_path(&shared.root, new_wal_generation)))
        {
            shared.runtime.recovery_required.set(true);
            return Err(cleanup);
        }
        return Err(error);
    }
    let old_wal = std::mem::replace(&mut shared.wal, new_wal);
    candidate.wal_generation = new_wal_generation;
    candidate.sstables.push(sstable.reference.clone());
    canonicalize_manifest_sstables(&mut candidate.sstables);
    match publish_manifest(&shared.root, candidate) {
        Ok(manifest) => shared.manifest = manifest,
        Err(ManifestPublishError::BeforeInstall(error)) => {
            let new_wal = std::mem::replace(&mut shared.wal, old_wal);
            drop(new_wal);
            if let Err(cleanup) = remove_unreferenced_file(&sstable.path).and_then(|()| {
                remove_unreferenced_file(&wal_path(&shared.root, new_wal_generation))
            }) {
                shared.runtime.recovery_required.set(true);
                return Err(cleanup);
            }
            return Err(error);
        }
        Err(ManifestPublishError::InstalledButUnsynced { manifest, source }) => {
            shared.manifest = *manifest;
            shared.sstables.push(sstable);
            canonicalize_sstables(&mut shared.sstables);
            shared.memtable.clear();
            shared.memtable_bytes = 0;
            shared.runtime.recovery_required.set(true);
            drop(old_wal);
            return Err(source);
        }
    }
    shared.sstables.push(sstable);
    canonicalize_sstables(&mut shared.sstables);
    shared.memtable.clear();
    shared.memtable_bytes = 0;
    increment_write_counter(
        &shared.runtime.amplification,
        &shared.runtime.amplification.flush_input_bytes,
        entries.iter().map(estimated_entry_bytes).sum(),
    );
    increment_write_counter(
        &shared.runtime.amplification,
        &shared.runtime.amplification.flush_output_bytes,
        shared
            .sstables
            .iter()
            .find(|sstable| sstable.reference.id == id)
            .map_or(0, |sstable| sstable.reference.file_bytes),
    );
    drop(old_wal);
    #[cfg(test)]
    maybe_lsm_crash("before-old-wal-removal");
    match fs::remove_file(wal_path(&shared.root, old_wal_generation)) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    #[cfg(test)]
    maybe_lsm_crash("after-old-wal-removal");
    cleanup_orphans(shared)?;
    Ok(())
}

fn compact_sstables(shared: &mut LsmShared) -> Result<(), StorageError> {
    while let Some(plan) = pick_compaction(shared)? {
        execute_compaction(shared, &plan, false)?;
    }
    Ok(())
}

fn compact_full_sstables(shared: &mut LsmShared) -> Result<(), StorageError> {
    if shared.sstables.is_empty() {
        return Ok(());
    }
    let plan = CompactionPlan {
        output_level: LSM_MAX_LEVELS - 1,
        input_ids: shared
            .sstables
            .iter()
            .map(|sstable| sstable.reference.id)
            .collect(),
    };
    execute_compaction(shared, &plan, true)
}

#[derive(Debug)]
struct CompactionPlan {
    output_level: u8,
    input_ids: BTreeSet<u64>,
}

fn conservative_sstable_output_bound(
    entry_count: u64,
    distinct_clustering_keys: u64,
    encoded_entry_bytes: u64,
) -> Result<u64, StorageError> {
    if entry_count == 0 || distinct_clustering_keys == 0 {
        return Err(LsmError::InvalidSstable {
            sstable_id: 0,
            reason: "maintenance output bound has no entries or clustering keys",
        }
        .into());
    }
    let bloom_bytes = u64::try_from(
        BloomFilter::for_distinct_keys(distinct_clustering_keys)?
            .bits
            .len(),
    )
    .map_err(|_| StorageError::CountOverflow)?;
    // Every real block contains at least one entry. Charging one block header,
    // checksum and index record per entry is therefore a conservative bound
    // independent of the production chunking decisions.
    let per_block_overhead = u64::try_from(
        SST_BLOCK_HEADER_SIZE
            .checked_add(4)
            .and_then(|bytes| bytes.checked_add(SST_INDEX_ENTRY_SIZE))
            .ok_or(StorageError::CountOverflow)?,
    )
    .map_err(|_| StorageError::CountOverflow)?;
    (SST_HEADER_SIZE as u64)
        .checked_add(bloom_bytes)
        .and_then(|bytes| bytes.checked_add(encoded_entry_bytes))
        .and_then(|bytes| {
            entry_count
                .checked_mul(per_block_overhead)
                .and_then(|overhead| bytes.checked_add(overhead))
        })
        .and_then(|bytes| bytes.checked_add(SST_FOOTER_SIZE as u64))
        .ok_or(StorageError::CountOverflow)
}

fn flush_conservative_bound(
    shared: &LsmShared,
) -> Result<LsmMaintenanceBoundInspection, StorageError> {
    let mut entry_count = 0_u64;
    let mut encoded_entry_bytes = 0_u64;
    let mut distinct = BTreeSet::new();
    for (key, versions) in &shared.memtable {
        distinct.insert(key.clustering);
        for value in versions.values() {
            entry_count = entry_count
                .checked_add(1)
                .ok_or(StorageError::CountOverflow)?;
            encoded_entry_bytes = encoded_entry_bytes
                .checked_add(32)
                .and_then(|bytes| bytes.checked_add(value_size(value)))
                .ok_or(StorageError::CountOverflow)?;
        }
    }
    let distinct = u64::try_from(distinct.len()).map_err(|_| StorageError::CountOverflow)?;
    Ok(LsmMaintenanceBoundInspection {
        work_units: entry_count,
        read_bytes: shared.memtable_bytes,
        write_bytes: conservative_sstable_output_bound(entry_count, distinct, encoded_entry_bytes)?,
    })
}

fn compaction_plan_inspection(
    shared: &LsmShared,
    plan: &CompactionPlan,
) -> Result<LsmCompactionPlanInspection, StorageError> {
    let estimated_cost = compaction_cost_inspection(shared, plan)?;
    let inputs = shared
        .sstables
        .iter()
        .filter(|sstable| plan.input_ids.contains(&sstable.reference.id))
        .collect::<Vec<_>>();
    let output_plans = plan_compaction_outputs(shared, &inputs, false)?;
    let write_bytes = output_plans.iter().try_fold(0_u64, |total, output| {
        conservative_sstable_output_bound(
            output.entry_count,
            output.distinct_clustering_keys,
            output.approximate_bytes,
        )
        .and_then(|bound| total.checked_add(bound).ok_or(StorageError::CountOverflow))
    })?;
    Ok(LsmCompactionPlanInspection {
        input_sstable_ids: plan.input_ids.iter().copied().collect(),
        output_level: plan.output_level,
        input_entries: estimated_cost.work_units,
        input_bytes: estimated_cost.read_bytes,
        estimated_cost,
        conservative_bound: LsmMaintenanceBoundInspection {
            work_units: estimated_cost.work_units,
            read_bytes: estimated_cost.read_bytes,
            write_bytes,
        },
    })
}

fn compaction_cost_inspection(
    shared: &LsmShared,
    plan: &CompactionPlan,
) -> Result<LsmMaintenanceCostInspection, StorageError> {
    let mut work_units = 0_u64;
    let mut input_bytes = 0_u64;
    for sstable in shared
        .sstables
        .iter()
        .filter(|sstable| plan.input_ids.contains(&sstable.reference.id))
    {
        work_units = work_units
            .checked_add(sstable.reference.entry_count)
            .ok_or(LsmError::InvalidManifest(
                "compaction work estimate overflows",
            ))?;
        input_bytes = input_bytes
            .checked_add(sstable.reference.file_bytes)
            .ok_or(LsmError::InvalidManifest(
                "compaction byte estimate overflows",
            ))?;
    }
    Ok(LsmMaintenanceCostInspection {
        work_units,
        read_bytes: input_bytes,
        // This is an admission estimate, not an I/O counter. Existing input
        // bytes are the closest storage-owned structural estimate available
        // without decoding every SSTable during planning.
        write_bytes: input_bytes,
    })
}

fn pick_compaction(shared: &LsmShared) -> Result<Option<CompactionPlan>, StorageError> {
    let l0 = shared
        .sstables
        .iter()
        .filter(|sstable| sstable.reference.level == 0)
        .collect::<Vec<_>>();
    if l0.len() >= L0_COMPACTION_TRIGGER {
        let mut input_ids = l0
            .iter()
            .map(|sstable| sstable.reference.id)
            .collect::<BTreeSet<_>>();
        let (min, max) = input_range(shared, &input_ids)?;
        for target in shared
            .sstables
            .iter()
            .filter(|sstable| sstable.reference.level == 1)
        {
            if ranges_overlap(min, max, target.reference.min, target.reference.max) {
                input_ids.insert(target.reference.id);
            }
        }
        return Ok(Some(CompactionPlan {
            output_level: 1,
            input_ids,
        }));
    }
    for level in 1..LSM_MAX_LEVELS - 1 {
        let bytes = shared
            .sstables
            .iter()
            .filter(|sstable| sstable.reference.level == level)
            .try_fold(0_u64, |total, sstable| {
                total
                    .checked_add(sstable.reference.file_bytes)
                    .ok_or(LsmError::InvalidManifest("level bytes overflow"))
            })?;
        if bytes <= level_target_bytes(level)? {
            continue;
        }
        let source = shared
            .sstables
            .iter()
            .filter(|sstable| sstable.reference.level == level)
            .min_by_key(|sstable| (sstable.reference.min, sstable.reference.id))
            .ok_or(LsmError::InvalidManifest("overflowing level is empty"))?;
        let mut input_ids = BTreeSet::from([source.reference.id]);
        loop {
            let old_len = input_ids.len();
            let (min, max) = input_range(shared, &input_ids)?;
            for sstable in shared.sstables.iter().filter(|sstable| {
                sstable.reference.level == level || sstable.reference.level == level + 1
            }) {
                if ranges_overlap(min, max, sstable.reference.min, sstable.reference.max) {
                    input_ids.insert(sstable.reference.id);
                }
            }
            if input_ids.len() == old_len {
                break;
            }
        }
        return Ok(Some(CompactionPlan {
            output_level: level + 1,
            input_ids,
        }));
    }
    Ok(None)
}

fn level_target_bytes(level: u8) -> Result<u64, StorageError> {
    if level == 0 || level >= LSM_MAX_LEVELS {
        return Err(LsmError::InvalidManifest("level target requested for invalid level").into());
    }
    let mut target = BASE_LEVEL_BYTES;
    for _ in 1..level {
        target = target
            .checked_mul(LEVEL_SIZE_MULTIPLIER)
            .ok_or(LsmError::InvalidManifest("level target overflows"))?;
    }
    Ok(target)
}

fn input_range(
    shared: &LsmShared,
    input_ids: &BTreeSet<u64>,
) -> Result<(PhysicalKey, PhysicalKey), StorageError> {
    let mut inputs = shared
        .sstables
        .iter()
        .filter(|sstable| input_ids.contains(&sstable.reference.id));
    let first = inputs
        .next()
        .ok_or(LsmError::InvalidManifest("compaction has no inputs"))?;
    let mut min = first.reference.min;
    let mut max = first.reference.max;
    for input in inputs {
        min = min.min(input.reference.min);
        max = max.max(input.reference.max);
    }
    Ok((min, max))
}

fn ranges_overlap(
    left_min: PhysicalKey,
    left_max: PhysicalKey,
    right_min: PhysicalKey,
    right_max: PhysicalKey,
) -> bool {
    left_min <= right_max && right_min <= left_max
}

fn execute_compaction(
    shared: &mut LsmShared,
    plan: &CompactionPlan,
    garbage_collect: bool,
) -> Result<(), StorageError> {
    let inputs = shared
        .sstables
        .iter()
        .filter(|sstable| plan.input_ids.contains(&sstable.reference.id))
        .collect::<Vec<_>>();
    let input_bytes = inputs.iter().try_fold(0_u64, |total, input| {
        total
            .checked_add(input.reference.file_bytes)
            .ok_or(LsmError::InvalidManifest("compaction input bytes overflow"))
    })?;
    let mut next_id = shared.manifest.next_sstable_id;
    let outputs = write_compaction_outputs(
        shared,
        &inputs,
        plan.output_level,
        garbage_collect,
        &mut next_id,
    )?;
    let output_bytes = outputs.iter().try_fold(0_u64, |total, output| {
        total
            .checked_add(output.reference.file_bytes)
            .ok_or(LsmError::InvalidManifest(
                "compaction output bytes overflow",
            ))
    })?;
    let mut candidate = shared.manifest.clone();
    candidate.next_sstable_id = next_id;
    candidate
        .sstables
        .retain(|reference| !plan.input_ids.contains(&reference.id));
    candidate
        .sstables
        .extend(outputs.iter().map(|output| output.reference.clone()));
    canonicalize_manifest_sstables(&mut candidate.sstables);
    let old_sstables = shared.sstables.clone();
    let mut next_sstables = shared
        .sstables
        .iter()
        .filter(|sstable| !plan.input_ids.contains(&sstable.reference.id))
        .cloned()
        .collect::<Vec<_>>();
    next_sstables.extend(outputs.iter().cloned());
    canonicalize_sstables(&mut next_sstables);
    match publish_manifest(&shared.root, candidate) {
        Ok(manifest) => {
            shared.manifest = manifest;
            shared.sstables = next_sstables;
        }
        Err(ManifestPublishError::BeforeInstall(error)) => {
            if let Err(cleanup) = cleanup_compaction_outputs(&outputs) {
                shared.runtime.recovery_required.set(true);
                return Err(cleanup);
            }
            return Err(error);
        }
        Err(ManifestPublishError::InstalledButUnsynced { manifest, source }) => {
            shared.manifest = *manifest;
            shared.sstables = next_sstables;
            shared.runtime.recovery_required.set(true);
            return Err(source);
        }
    }
    increment_write_counter(
        &shared.runtime.amplification,
        &shared.runtime.amplification.compaction_input_bytes,
        input_bytes,
    );
    increment_write_counter(
        &shared.runtime.amplification,
        &shared.runtime.amplification.compaction_output_bytes,
        output_bytes,
    );
    increment_write_counter(
        &shared.runtime.amplification,
        &shared.runtime.amplification.obsolete_bytes,
        input_bytes,
    );
    for old in old_sstables {
        if plan.input_ids.contains(&old.reference.id) {
            #[cfg(test)]
            maybe_lsm_crash("while-deleting-old-sst");
            remove_obsolete_file(&old.path)?;
        }
    }
    sync_directory(&shared.root.join(SST_DIR_NAME))?;
    cleanup_orphans(shared)?;
    Ok(())
}

fn write_compaction_outputs(
    shared: &LsmShared,
    inputs: &[&Sstable],
    output_level: u8,
    garbage_collect: bool,
    next_id: &mut u64,
) -> Result<Vec<Sstable>, StorageError> {
    let plans = plan_compaction_outputs(shared, inputs, garbage_collect)?;

    let mut outputs = Vec::new();
    let mut plan_index = 0_usize;
    let mut writer: Option<StreamingSstableWriter<'_>> = None;
    let second_pass = for_each_compaction_entry(shared, inputs, garbage_collect, |entry| {
        if writer.as_ref().is_some_and(|_| {
            plans
                .get(plan_index)
                .is_some_and(|plan| entry.key.clustering > plan.max_clustering)
        }) {
            let output = writer
                .take()
                .ok_or(LsmError::InvalidSstable {
                    sstable_id: 0,
                    reason: "compaction writer disappeared",
                })?
                .finish()?;
            outputs.push(output);
            plan_index += 1;
            #[cfg(test)]
            maybe_lsm_crash("between-compaction-outputs");
        }
        if writer.is_none() {
            let plan = plans.get(plan_index).ok_or(LsmError::InvalidSstable {
                sstable_id: 0,
                reason: "compaction produced more entries than planned",
            })?;
            let id = *next_id;
            *next_id = next_id
                .checked_add(1)
                .ok_or(LsmError::AllocatorExhausted("SSTable ID"))?;
            writer = Some(StreamingSstableWriter::create(
                shared,
                id,
                output_level,
                plan,
            )?);
        }
        writer
            .as_mut()
            .ok_or(LsmError::InvalidSstable {
                sstable_id: 0,
                reason: "compaction writer is missing",
            })?
            .push(entry)
    });
    if let Err(error) = second_pass {
        drop(writer);
        cleanup_compaction_outputs(&outputs)?;
        return Err(error);
    }
    if let Some(writer) = writer {
        match writer.finish() {
            Ok(output) => {
                outputs.push(output);
                plan_index += 1;
            }
            Err(error) => {
                cleanup_compaction_outputs(&outputs)?;
                return Err(error);
            }
        }
    }
    if plan_index != plans.len() {
        cleanup_compaction_outputs(&outputs)?;
        return Err(LsmError::InvalidSstable {
            sstable_id: 0,
            reason: "compaction output plan count differs from writes",
        }
        .into());
    }
    Ok(outputs)
}

fn plan_compaction_outputs(
    shared: &LsmShared,
    inputs: &[&Sstable],
    garbage_collect: bool,
) -> Result<Vec<CompactionOutputPlan>, StorageError> {
    let mut plans = Vec::<CompactionOutputPlan>::new();
    let mut current: Option<CompactionOutputPlan> = None;
    for_each_compaction_entry(shared, inputs, garbage_collect, |entry| {
        if current.as_ref().is_some_and(|plan| {
            plan.approximate_bytes >= SST_TARGET_FILE_BYTES
                && plan.max_clustering != entry.key.clustering
        }) {
            plans.push(current.take().ok_or(LsmError::InvalidSstable {
                sstable_id: 0,
                reason: "compaction output plan disappeared",
            })?);
        }
        let plan = current.get_or_insert(CompactionOutputPlan {
            max_clustering: entry.key.clustering,
            entry_count: 0,
            distinct_clustering_keys: 0,
            approximate_bytes: 0,
        });
        if plan.entry_count == 0 || plan.max_clustering != entry.key.clustering {
            plan.distinct_clustering_keys =
                plan.distinct_clustering_keys
                    .checked_add(1)
                    .ok_or(LsmError::InvalidSstable {
                        sstable_id: 0,
                        reason: "distinct clustering-key count overflows",
                    })?;
        }
        plan.max_clustering = entry.key.clustering;
        plan.entry_count = plan
            .entry_count
            .checked_add(1)
            .ok_or(LsmError::InvalidSstable {
                sstable_id: 0,
                reason: "compaction output entry count overflows",
            })?;
        plan.approximate_bytes = plan
            .approximate_bytes
            .checked_add(estimated_entry_bytes(entry))
            .ok_or(LsmError::InvalidSstable {
                sstable_id: 0,
                reason: "compaction output byte estimate overflows",
            })?;
        Ok(())
    })?;
    if let Some(plan) = current {
        plans.push(plan);
    }
    Ok(plans)
}

#[derive(Debug)]
struct CompactionOutputPlan {
    max_clustering: ClusteringKey,
    entry_count: u64,
    distinct_clustering_keys: u64,
    approximate_bytes: u64,
}

fn for_each_compaction_entry(
    shared: &LsmShared,
    inputs: &[&Sstable],
    garbage_collect: bool,
    mut visitor: impl FnMut(&VersionedEntry) -> Result<(), StorageError>,
) -> Result<(), StorageError> {
    let mut runs = inputs
        .iter()
        .map(|sstable| {
            SstableEntryCursor::new(sstable, &shared.table, None, None)
                .and_then(|cursor| MergeRun::new(EntryCursor::Sstable(cursor)))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut pending_gc: Option<VersionedEntry> = None;
    while let Some(entry) = next_merged_entry(&mut runs)? {
        if garbage_collect {
            if pending_gc
                .as_ref()
                .is_some_and(|pending| pending.key != entry.key)
            {
                if let Some(completed) = pending_gc
                    .take()
                    .filter(|completed| matches!(completed.value, EntryValue::Put(_)))
                {
                    visitor(&completed)?;
                }
            }
            pending_gc = Some(entry);
        } else {
            visitor(&entry)?;
        }
    }
    if let Some(completed) =
        pending_gc.filter(|completed| matches!(completed.value, EntryValue::Put(_)))
    {
        visitor(&completed)?;
    }
    Ok(())
}

struct StreamingSstableWriter<'a> {
    shared: &'a LsmShared,
    id: u64,
    level: u8,
    expected_entries: u64,
    expected_distinct: u64,
    file: File,
    temp_path: PathBuf,
    final_path: PathBuf,
    offset: u64,
    blocks: Vec<BlockMeta>,
    block_entries: Vec<VersionedEntry>,
    block_bytes: usize,
    bloom: BloomFilter,
    entry_count: u64,
    distinct_count: u64,
    min: Option<PhysicalKey>,
    max: Option<PhysicalKey>,
    previous: Option<(PhysicalKey, LsmCommitSeq)>,
    previous_clustering: Option<ClusteringKey>,
    finished: bool,
}

impl<'a> StreamingSstableWriter<'a> {
    fn create(
        shared: &'a LsmShared,
        id: u64,
        level: u8,
        plan: &CompactionOutputPlan,
    ) -> Result<Self, StorageError> {
        let bloom = BloomFilter::for_distinct_keys(plan.distinct_clustering_keys)?;
        let temp_path = sstable_temp_path(&shared.root, id, level);
        let final_path = sstable_path(&shared.root, id, level);
        let mut file = OpenOptions::new()
            .write(true)
            .read(true)
            .create_new(true)
            .open(&temp_path)?;
        file.write_all(&[0_u8; SST_HEADER_SIZE])?;
        file.write_all(&bloom.bits)?;
        Ok(Self {
            shared,
            id,
            level,
            expected_entries: plan.entry_count,
            expected_distinct: plan.distinct_clustering_keys,
            file,
            temp_path,
            final_path,
            offset: SST_HEADER_SIZE as u64 + bloom.bits.len() as u64,
            blocks: Vec::new(),
            block_entries: Vec::new(),
            block_bytes: 0,
            bloom,
            entry_count: 0,
            distinct_count: 0,
            min: None,
            max: None,
            previous: None,
            previous_clustering: None,
            finished: false,
        })
    }

    fn push(&mut self, entry: &VersionedEntry) -> Result<(), StorageError> {
        let encoded_bytes = usize::try_from(estimated_entry_bytes(entry)).map_err(|_| {
            StorageError::ResourceLimit {
                resource: "LSM SSTable entry bytes",
                limit: SST_MAX_BLOCK_BYTES as u64,
            }
        })?;
        if encoded_bytes > SST_MAX_BLOCK_BYTES {
            return Err(StorageError::ResourceLimit {
                resource: "LSM SSTable entry bytes",
                limit: SST_MAX_BLOCK_BYTES as u64,
            });
        }
        if self
            .previous
            .is_some_and(|previous| previous >= (entry.key, entry.version))
        {
            return Err(LsmError::InvalidSstable {
                sstable_id: self.id,
                reason: "streamed entries are not strictly sorted",
            }
            .into());
        }
        if let EntryValue::Put(row) = &entry.value {
            let _ = decode_row(row, &self.shared.table)?;
        }
        if !self.block_entries.is_empty()
            && self.block_bytes.saturating_add(encoded_bytes) > SST_TARGET_BLOCK_BYTES
        {
            self.flush_block()?;
        }
        if self.previous_clustering != Some(entry.key.clustering) {
            self.bloom.insert(entry.key.clustering);
            self.distinct_count =
                self.distinct_count
                    .checked_add(1)
                    .ok_or(LsmError::InvalidSstable {
                        sstable_id: self.id,
                        reason: "streamed distinct key count overflows",
                    })?;
        }
        self.entry_count = self
            .entry_count
            .checked_add(1)
            .ok_or(LsmError::InvalidSstable {
                sstable_id: self.id,
                reason: "streamed entry count overflows",
            })?;
        self.min.get_or_insert(entry.key);
        self.max = Some(entry.key);
        self.previous = Some((entry.key, entry.version));
        self.previous_clustering = Some(entry.key.clustering);
        self.block_bytes = self.block_bytes.saturating_add(encoded_bytes);
        self.block_entries.push(entry.clone());
        Ok(())
    }

    fn flush_block(&mut self) -> Result<(), StorageError> {
        if self.block_entries.is_empty() {
            return Ok(());
        }
        if self.blocks.len() >= SST_MAX_BLOCKS as usize {
            return Err(StorageError::ResourceLimit {
                resource: "LSM SSTable blocks",
                limit: u64::from(SST_MAX_BLOCKS),
            });
        }
        let (bytes, meta) = encode_sstable_block(
            self.id,
            self.blocks.len() as u32,
            &self.block_entries,
            self.offset,
        )?;
        self.file.write_all(&bytes)?;
        self.offset =
            self.offset
                .checked_add(bytes.len() as u64)
                .ok_or(LsmError::InvalidSstable {
                    sstable_id: self.id,
                    reason: "streamed file offset overflows",
                })?;
        self.blocks.push(meta);
        self.block_entries.clear();
        self.block_bytes = 0;
        Ok(())
    }

    fn finish(mut self) -> Result<Sstable, StorageError> {
        self.flush_block()?;
        if self.entry_count != self.expected_entries
            || self.distinct_count != self.expected_distinct
            || self.blocks.is_empty()
        {
            return Err(LsmError::InvalidSstable {
                sstable_id: self.id,
                reason: "streamed output differs from its first-pass plan",
            }
            .into());
        }
        let index = encode_sstable_index(&self.blocks)?;
        let footer = encode_sstable_footer(&index, self.blocks.len())?;
        self.file.write_all(&index)?;
        self.file.write_all(&footer)?;
        let file_bytes = self
            .offset
            .checked_add(index.len() as u64)
            .and_then(|value| value.checked_add(SST_FOOTER_SIZE as u64))
            .ok_or(LsmError::InvalidSstable {
                sstable_id: self.id,
                reason: "streamed footer offset overflows",
            })?;
        let reference = SstableRef {
            id: self.id,
            level: self.level,
            entry_count: self.entry_count,
            file_bytes,
            bloom_bytes: self.bloom.bits.len() as u64,
            min: self.min.ok_or(LsmError::InvalidSstable {
                sstable_id: self.id,
                reason: "streamed output has no minimum key",
            })?,
            max: self.max.ok_or(LsmError::InvalidSstable {
                sstable_id: self.id,
                reason: "streamed output has no maximum key",
            })?,
        };
        let header = encode_sstable_header(
            &self.shared.manifest,
            &reference,
            self.blocks.len() as u32,
            &self.bloom,
        )?;
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&header)?;
        self.file.write_all(&self.bloom.bits)?;
        #[cfg(test)]
        maybe_lsm_crash("during-sst-write");
        self.file.sync_all()?;
        #[cfg(test)]
        maybe_lsm_crash("after-sst-sync");
        fs::rename(&self.temp_path, &self.final_path)?;
        sync_directory(&self.shared.root.join(SST_DIR_NAME))?;
        self.finished = true;
        Ok(Sstable {
            reference,
            path: self.final_path.clone(),
            blocks: self.blocks.clone(),
            bloom: self.bloom.clone(),
        })
    }
}

impl Drop for StreamingSstableWriter<'_> {
    fn drop(&mut self) {
        if !self.finished {
            let _ = fs::remove_file(&self.temp_path);
            let _ = fs::remove_file(&self.final_path);
        }
    }
}

fn cleanup_compaction_outputs(outputs: &[Sstable]) -> Result<(), StorageError> {
    for output in outputs {
        remove_unreferenced_file(&output.path)?;
    }
    Ok(())
}

fn canonicalize_manifest_sstables(sstables: &mut [SstableRef]) {
    sstables.sort_by_key(|sstable| {
        (
            sstable.level,
            if sstable.level == 0 {
                PhysicalKey {
                    clustering: match sstable.min.clustering {
                        ClusteringKey::Int64(_) => ClusteringKey::Int64(i64::MIN),
                        ClusteringKey::UInt64(_) => ClusteringKey::UInt64(u64::MIN),
                    },
                    row_id: LsmRowId(1),
                }
            } else {
                sstable.min
            },
            sstable.id,
        )
    });
}

fn canonicalize_sstables(sstables: &mut [Sstable]) {
    sstables.sort_by_key(|sstable| {
        (
            sstable.reference.level,
            if sstable.reference.level == 0 {
                PhysicalKey {
                    clustering: match sstable.reference.min.clustering {
                        ClusteringKey::Int64(_) => ClusteringKey::Int64(i64::MIN),
                        ClusteringKey::UInt64(_) => ClusteringKey::UInt64(u64::MIN),
                    },
                    row_id: LsmRowId(1),
                }
            } else {
                sstable.reference.min
            },
            sstable.reference.id,
        )
    });
}

fn estimated_entry_bytes(entry: &VersionedEntry) -> u64 {
    32_u64.saturating_add(value_size(&entry.value))
}

fn remove_unreferenced_file(path: &Path) -> Result<(), StorageError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn remove_obsolete_file(path: &Path) -> Result<(), StorageError> {
    maybe_fail_obsolete_delete()?;
    remove_unreferenced_file(path)
}

fn cleanup_orphans(shared: &LsmShared) -> Result<(), StorageError> {
    let referenced = shared
        .manifest
        .sstables
        .iter()
        .map(|sst| sstable_file_name(sst.id, sst.level))
        .collect::<BTreeSet<_>>();
    for entry in fs::read_dir(shared.root.join(SST_DIR_NAME))? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if (name.ends_with(".next") || name.ends_with(".nbls"))
            && !referenced.contains(name.as_ref())
        {
            fs::remove_file(entry.path())?;
        }
    }
    for entry in fs::read_dir(&shared.root)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with("wal-")
            && name.ends_with(".nblw")
            && name != wal_file_name(shared.manifest.wal_generation)
        {
            fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}

fn cleanup_authoritative_orphans(root: &Path, manifest: &Manifest) -> Result<(), StorageError> {
    match fs::remove_file(manifest_next_path(root)) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let referenced = manifest
        .sstables
        .iter()
        .map(|sst| sstable_file_name(sst.id, sst.level))
        .collect::<BTreeSet<_>>();
    for entry in fs::read_dir(root.join(SST_DIR_NAME))? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if (name.ends_with(".next") || name.ends_with(".nbls"))
            && !referenced.contains(name.as_ref())
        {
            fs::remove_file(entry.path())?;
        }
    }
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with("wal-")
            && name.ends_with(".nblw")
            && name != wal_file_name(manifest.wal_generation)
        {
            fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}

fn write_sstable(
    root: &Path,
    manifest: &Manifest,
    id: u64,
    level: u8,
    entries: &[VersionedEntry],
    table: &TableDef,
) -> Result<Sstable, StorageError> {
    if entries.is_empty() || level >= LSM_MAX_LEVELS {
        return Err(LsmError::InvalidSstable {
            sstable_id: id,
            reason: "cannot write an empty SSTable or invalid level",
        }
        .into());
    }
    let mut previous = None;
    for entry in entries {
        if entry.key.row_id.0 == 0
            || entry.version.0 == 0
            || entry.key.clustering.kind() != manifest.key_type
        {
            return Err(LsmError::InvalidSstable {
                sstable_id: id,
                reason: "entry identity or type is invalid",
            }
            .into());
        }
        if previous.is_some_and(|previous: (PhysicalKey, LsmCommitSeq)| {
            previous >= (entry.key, entry.version)
        }) {
            return Err(LsmError::InvalidSstable {
                sstable_id: id,
                reason: "entries are not strictly sorted",
            }
            .into());
        }
        if let EntryValue::Put(row) = &entry.value {
            let _ = decode_row(row, table)?;
        }
        previous = Some((entry.key, entry.version));
    }
    let blocks = chunk_entries(entries)?;
    if blocks.len() > SST_MAX_BLOCKS as usize {
        return Err(StorageError::ResourceLimit {
            resource: "LSM SSTable blocks",
            limit: u64::from(SST_MAX_BLOCKS),
        });
    }
    let bloom = BloomFilter::build(entries)?;
    let mut offset = (SST_HEADER_SIZE as u64)
        .checked_add(bloom.bits.len() as u64)
        .ok_or(LsmError::InvalidSstable {
            sstable_id: id,
            reason: "Bloom offset overflows",
        })?;
    let mut encoded_blocks = Vec::with_capacity(blocks.len());
    let mut metas = Vec::with_capacity(blocks.len());
    for (block_index, block) in blocks.iter().enumerate() {
        let (block_bytes, meta) = encode_sstable_block(id, block_index as u32, block, offset)?;
        offset = offset
            .checked_add(block_bytes.len() as u64)
            .ok_or(LsmError::InvalidSstable {
                sstable_id: id,
                reason: "file offset overflows",
            })?;
        encoded_blocks.push(block_bytes);
        metas.push(meta);
    }
    let index = encode_sstable_index(&metas)?;
    let footer = encode_sstable_footer(&index, metas.len())?;
    offset = offset
        .checked_add(index.len() as u64)
        .and_then(|value| value.checked_add(SST_FOOTER_SIZE as u64))
        .ok_or(LsmError::InvalidSstable {
            sstable_id: id,
            reason: "footer offset overflows",
        })?;
    let reference = SstableRef {
        id,
        level,
        entry_count: entries.len() as u64,
        file_bytes: offset,
        bloom_bytes: bloom.bits.len() as u64,
        min: entries
            .first()
            .ok_or(LsmError::InvalidSstable {
                sstable_id: id,
                reason: "missing first entry",
            })?
            .key,
        max: entries
            .last()
            .ok_or(LsmError::InvalidSstable {
                sstable_id: id,
                reason: "missing last entry",
            })?
            .key,
    };
    let temp = sstable_temp_path(root, id, level);
    let final_path = sstable_path(root, id, level);
    let header = encode_sstable_header(manifest, &reference, blocks.len() as u32, &bloom)?;
    let write_result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)?;
        file.write_all(&header)?;
        file.write_all(&bloom.bits)?;
        for block_bytes in encoded_blocks {
            file.write_all(&block_bytes)?;
        }
        file.write_all(&index)?;
        file.write_all(&footer)?;
        #[cfg(test)]
        maybe_lsm_crash("during-sst-write");
        file.sync_all()?;
        #[cfg(test)]
        maybe_lsm_crash("after-sst-sync");
        drop(file);
        fs::rename(&temp, &final_path)?;
        sync_directory(&root.join(SST_DIR_NAME))
    })();
    if let Err(error) = write_result {
        remove_unreferenced_file(&temp)?;
        remove_unreferenced_file(&final_path)?;
        return Err(error);
    }
    Ok(Sstable {
        reference,
        path: final_path,
        blocks: metas,
        bloom,
    })
}

fn chunk_entries(entries: &[VersionedEntry]) -> Result<Vec<Vec<VersionedEntry>>, StorageError> {
    let mut blocks = Vec::new();
    let mut current = Vec::new();
    let mut size = 0_usize;
    for entry in entries {
        let entry_size = 8
            + 8
            + 8
            + 1
            + 3
            + 4
            + match &entry.value {
                EntryValue::Put(row) => row.len(),
                EntryValue::Tombstone => 0,
            };
        if entry_size > SST_MAX_BLOCK_BYTES {
            return Err(StorageError::ResourceLimit {
                resource: "LSM SSTable entry bytes",
                limit: SST_MAX_BLOCK_BYTES as u64,
            });
        }
        if !current.is_empty() && size + entry_size > SST_TARGET_BLOCK_BYTES {
            blocks.push(std::mem::take(&mut current));
            size = 0;
        }
        current.push(entry.clone());
        size += entry_size;
    }
    if !current.is_empty() {
        blocks.push(current);
    }
    Ok(blocks)
}

fn encode_sstable_header(
    manifest: &Manifest,
    reference: &SstableRef,
    block_count: u32,
    bloom: &BloomFilter,
) -> Result<[u8; SST_HEADER_SIZE], StorageError> {
    let mut bytes = [0_u8; SST_HEADER_SIZE];
    bytes[0..4].copy_from_slice(SST_MAGIC);
    bytes[4..6].copy_from_slice(&LSM_SSTABLE_FORMAT_VERSION.to_le_bytes());
    bytes[8..16].copy_from_slice(&manifest.storage_id.0.to_le_bytes());
    bytes[16..24].copy_from_slice(&manifest.table_id.0.to_le_bytes());
    bytes[24..56].copy_from_slice(manifest.schema_fingerprint.as_bytes());
    bytes[56..64].copy_from_slice(&reference.id.to_le_bytes());
    bytes[64] = reference.level;
    bytes[65] = physical_type_tag(manifest.key_type)?;
    bytes[68..76].copy_from_slice(&reference.entry_count.to_le_bytes());
    bytes[76..80].copy_from_slice(&block_count.to_le_bytes());
    encode_physical_key(&mut bytes[80..100], reference.min)?;
    encode_physical_key(&mut bytes[100..120], reference.max)?;
    bytes[120] = bloom.algorithm;
    bytes[121] = bloom.hash_count;
    bytes[124..132].copy_from_slice(&bloom.bit_count.to_le_bytes());
    bytes[132..140].copy_from_slice(&(bloom.bits.len() as u64).to_le_bytes());
    bytes[140..144].copy_from_slice(&crc32c::crc32c(&bloom.bits).to_le_bytes());
    bytes[144..152].copy_from_slice(&reference.file_bytes.to_le_bytes());
    let checksum = crc32c::crc32c(&bytes[..156]);
    bytes[156..160].copy_from_slice(&checksum.to_le_bytes());
    Ok(bytes)
}

fn decode_sstable_header(
    bytes: &[u8; SST_HEADER_SIZE],
    manifest: &Manifest,
    reference: &SstableRef,
) -> Result<(u32, BloomFilter), StorageError> {
    let invalid = |reason| LsmError::InvalidSstable {
        sstable_id: reference.id,
        reason,
    };
    if &bytes[0..4] != SST_MAGIC {
        return Err(invalid("header magic does not match").into());
    }
    let version = read_u16(bytes, 4)?;
    if version != LSM_SSTABLE_FORMAT_VERSION {
        return Err(LsmError::UnsupportedSstableVersion(version).into());
    }
    if bytes[6..8]
        .iter()
        .chain(bytes[66..68].iter())
        .chain(bytes[122..124].iter())
        .chain(bytes[152..156].iter())
        .any(|byte| *byte != 0)
    {
        return Err(invalid("header reserved bytes are nonzero").into());
    }
    let storage_id = StorageId(read_u64(bytes, 8)?);
    if storage_id != manifest.storage_id {
        return Err(LsmError::StorageIdMismatch {
            expected: manifest.storage_id,
            actual: storage_id,
        }
        .into());
    }
    if TableId(read_u64(bytes, 16)?) != manifest.table_id
        || bytes[24..56] != *manifest.schema_fingerprint.as_bytes()
        || read_u64(bytes, 56)? != reference.id
        || bytes[64] != reference.level
        || decode_physical_type(bytes[65])? != manifest.key_type
        || read_u64(bytes, 68)? != reference.entry_count
        || decode_physical_key(&bytes[80..100], manifest.key_type)? != reference.min
        || decode_physical_key(&bytes[100..120], manifest.key_type)? != reference.max
        || read_u64(bytes, 144)? != reference.file_bytes
    {
        return Err(invalid("header identity differs from manifest").into());
    }
    let stored = read_u32(bytes, 156)?;
    let computed = crc32c::crc32c(&bytes[..156]);
    if stored != computed {
        return Err(LsmError::SstableChecksum {
            sstable_id: reference.id,
            block: u32::MAX,
            stored,
            computed,
        }
        .into());
    }
    let count = read_u32(bytes, 76)?;
    if count == 0 || count > SST_MAX_BLOCKS || u64::from(count) > reference.entry_count {
        return Err(invalid("invalid block count").into());
    }
    let algorithm = bytes[120];
    let hash_count = bytes[121];
    let bit_count = read_u64(bytes, 124)?;
    let bloom_bytes = read_u64(bytes, 132)?;
    if algorithm != BLOOM_ALGORITHM_VERSION
        || hash_count == 0
        || hash_count > 32
        || bit_count == 0
        || bit_count > BLOOM_MAX_BITS
        || bit_count % 8 != 0
        || bloom_bytes != bit_count / 8
        || bloom_bytes != reference.bloom_bytes
    {
        return Err(invalid("Bloom metadata is invalid").into());
    }
    let byte_count =
        usize::try_from(bloom_bytes).map_err(|_| invalid("Bloom byte count does not fit usize"))?;
    Ok((
        count,
        BloomFilter {
            algorithm,
            bit_count,
            hash_count,
            bits: vec![0; byte_count],
        },
    ))
}

fn encode_sstable_block(
    id: u64,
    _block_index: u32,
    entries: &[VersionedEntry],
    offset: u64,
) -> Result<(Vec<u8>, BlockMeta), StorageError> {
    let mut payload = Vec::new();
    for entry in entries {
        encode_sstable_entry(&mut payload, entry)?;
    }
    if payload.len() > SST_MAX_BLOCK_BYTES {
        return Err(StorageError::ResourceLimit {
            resource: "LSM SSTable block bytes",
            limit: SST_MAX_BLOCK_BYTES as u64,
        });
    }
    let first = entries
        .first()
        .ok_or(LsmError::InvalidSstable {
            sstable_id: id,
            reason: "block is empty",
        })?
        .key;
    let last = entries
        .last()
        .ok_or(LsmError::InvalidSstable {
            sstable_id: id,
            reason: "block is empty",
        })?
        .key;
    let mut bytes = vec![0_u8; SST_BLOCK_HEADER_SIZE];
    bytes[0..4].copy_from_slice(SST_BLOCK_MAGIC);
    bytes[4..8].copy_from_slice(&(payload.len() as u32).to_le_bytes());
    bytes[8..12].copy_from_slice(&(entries.len() as u32).to_le_bytes());
    encode_physical_key(&mut bytes[12..32], first)?;
    encode_physical_key(&mut bytes[32..52], last)?;
    bytes.extend_from_slice(&payload);
    let checksum = crc32c::crc32c(&bytes);
    bytes.extend_from_slice(&checksum.to_le_bytes());
    Ok((
        bytes,
        BlockMeta {
            offset,
            payload_length: payload.len() as u32,
            entry_count: entries.len() as u32,
            first,
            last,
        },
    ))
}

fn encode_sstable_index(blocks: &[BlockMeta]) -> Result<Vec<u8>, StorageError> {
    let length =
        blocks
            .len()
            .checked_mul(SST_INDEX_ENTRY_SIZE)
            .ok_or(LsmError::InvalidSstable {
                sstable_id: 0,
                reason: "block index length overflows",
            })?;
    let mut bytes = vec![0_u8; length];
    for (position, block) in blocks.iter().enumerate() {
        let offset = position * SST_INDEX_ENTRY_SIZE;
        bytes[offset..offset + 8].copy_from_slice(&block.offset.to_le_bytes());
        bytes[offset + 8..offset + 12].copy_from_slice(&block.payload_length.to_le_bytes());
        bytes[offset + 12..offset + 16].copy_from_slice(&block.entry_count.to_le_bytes());
        encode_physical_key(&mut bytes[offset + 16..offset + 36], block.first)?;
        encode_physical_key(&mut bytes[offset + 36..offset + 56], block.last)?;
    }
    Ok(bytes)
}

fn encode_sstable_footer(
    index: &[u8],
    block_count: usize,
) -> Result<[u8; SST_FOOTER_SIZE], StorageError> {
    if block_count > SST_MAX_BLOCKS as usize {
        return Err(StorageError::ResourceLimit {
            resource: "LSM SSTable blocks",
            limit: u64::from(SST_MAX_BLOCKS),
        });
    }
    let mut bytes = [0_u8; SST_FOOTER_SIZE];
    bytes[0..4].copy_from_slice(SST_FOOTER_MAGIC);
    bytes[4..6].copy_from_slice(&LSM_SSTABLE_FORMAT_VERSION.to_le_bytes());
    bytes[8..12].copy_from_slice(
        &u32::try_from(block_count)
            .map_err(|_| LsmError::InvalidSstable {
                sstable_id: 0,
                reason: "block count exceeds u32",
            })?
            .to_le_bytes(),
    );
    bytes[12..16].copy_from_slice(&(SST_INDEX_ENTRY_SIZE as u32).to_le_bytes());
    bytes[16..24].copy_from_slice(&(index.len() as u64).to_le_bytes());
    bytes[24..28].copy_from_slice(&crc32c::crc32c(index).to_le_bytes());
    let checksum = crc32c::crc32c(&bytes[..28]);
    bytes[28..32].copy_from_slice(&checksum.to_le_bytes());
    Ok(bytes)
}

fn read_sstable_footer(
    file: &mut File,
    reference: &SstableRef,
    block_count: u32,
    key_type: PhysicalType,
    first_block_offset: u64,
) -> Result<Vec<BlockMeta>, StorageError> {
    let invalid = |reason| LsmError::InvalidSstable {
        sstable_id: reference.id,
        reason,
    };
    let file_length = file.metadata()?.len();
    if file_length != reference.file_bytes {
        return Err(invalid("file length differs from manifest").into());
    }
    let footer_offset = file_length
        .checked_sub(SST_FOOTER_SIZE as u64)
        .ok_or_else(|| invalid("footer offset underflows"))?;
    if footer_offset < first_block_offset {
        return Err(invalid("footer overlaps header or Bloom").into());
    }
    file.seek(SeekFrom::Start(footer_offset))?;
    let mut footer = [0_u8; SST_FOOTER_SIZE];
    file.read_exact(&mut footer)
        .map_err(|_| invalid("footer is truncated"))?;
    if &footer[0..4] != SST_FOOTER_MAGIC
        || read_u16(&footer, 4)? != LSM_SSTABLE_FORMAT_VERSION
        || footer[6..8].iter().any(|byte| *byte != 0)
    {
        return Err(invalid("footer identity is invalid").into());
    }
    let stored_footer = read_u32(&footer, 28)?;
    let computed_footer = crc32c::crc32c(&footer[..28]);
    if stored_footer != computed_footer {
        return Err(LsmError::SstableChecksum {
            sstable_id: reference.id,
            block: u32::MAX - 2,
            stored: stored_footer,
            computed: computed_footer,
        }
        .into());
    }
    if read_u32(&footer, 8)? != block_count || read_u32(&footer, 12)? != SST_INDEX_ENTRY_SIZE as u32
    {
        return Err(invalid("footer block metadata differs from header").into());
    }
    let expected_index_length = usize::try_from(block_count)
        .ok()
        .and_then(|count| count.checked_mul(SST_INDEX_ENTRY_SIZE))
        .ok_or_else(|| invalid("block index length overflows"))?;
    if read_u64(&footer, 16)? != expected_index_length as u64 {
        return Err(invalid("block index length is invalid").into());
    }
    let index_offset = footer_offset
        .checked_sub(expected_index_length as u64)
        .ok_or_else(|| invalid("block index offset underflows"))?;
    if index_offset < first_block_offset {
        return Err(invalid("block index overlaps header or Bloom").into());
    }
    let mut index = vec![0_u8; expected_index_length];
    file.seek(SeekFrom::Start(index_offset))?;
    file.read_exact(&mut index)
        .map_err(|_| invalid("block index is truncated"))?;
    let stored_index = read_u32(&footer, 24)?;
    let computed_index = crc32c::crc32c(&index);
    if stored_index != computed_index {
        return Err(LsmError::SstableChecksum {
            sstable_id: reference.id,
            block: u32::MAX - 3,
            stored: stored_index,
            computed: computed_index,
        }
        .into());
    }
    let mut blocks = Vec::with_capacity(block_count as usize);
    let mut expected_offset = first_block_offset;
    let mut previous_last = None;
    let mut total_entries = 0_u64;
    for position in 0..block_count as usize {
        let offset = position * SST_INDEX_ENTRY_SIZE;
        let block_offset = read_u64(&index, offset)?;
        let payload_length = read_u32(&index, offset + 8)?;
        let entry_count = read_u32(&index, offset + 12)?;
        let first = decode_physical_key(&index[offset + 16..offset + 36], key_type)?;
        let last = decode_physical_key(&index[offset + 36..offset + 56], key_type)?;
        if block_offset != expected_offset
            || payload_length as usize > SST_MAX_BLOCK_BYTES
            || entry_count == 0
            || first > last
            || previous_last.is_some_and(|previous| previous > first)
        {
            return Err(invalid("block index entry is invalid or unsorted").into());
        }
        expected_offset = expected_offset
            .checked_add(SST_BLOCK_HEADER_SIZE as u64)
            .and_then(|value| value.checked_add(u64::from(payload_length)))
            .and_then(|value| value.checked_add(4))
            .ok_or_else(|| invalid("block byte range overflows"))?;
        if expected_offset > index_offset {
            return Err(invalid("block bytes overlap the index").into());
        }
        total_entries = total_entries
            .checked_add(u64::from(entry_count))
            .ok_or_else(|| invalid("block entry count overflows"))?;
        previous_last = Some(last);
        blocks.push(BlockMeta {
            offset: block_offset,
            payload_length,
            entry_count,
            first,
            last,
        });
    }
    if expected_offset != index_offset || total_entries != reference.entry_count {
        return Err(invalid("block index coverage differs from file").into());
    }
    Ok(blocks)
}

fn encode_sstable_entry(bytes: &mut Vec<u8>, entry: &VersionedEntry) -> Result<(), StorageError> {
    bytes.extend_from_slice(&entry.key.clustering.bits().to_le_bytes());
    bytes.extend_from_slice(&entry.key.row_id.0.to_le_bytes());
    bytes.extend_from_slice(&entry.version.0.to_le_bytes());
    let (tag, row) = match &entry.value {
        EntryValue::Put(row) => (1_u8, row.as_slice()),
        EntryValue::Tombstone => (2_u8, &[][..]),
    };
    bytes.push(tag);
    bytes.extend_from_slice(&[0; 3]);
    bytes.extend_from_slice(
        &(u32::try_from(row.len()).map_err(|_| StorageError::ResourceLimit {
            resource: "LSM row bytes",
            limit: u32::MAX as u64,
        })?)
        .to_le_bytes(),
    );
    bytes.extend_from_slice(row);
    Ok(())
}

fn open_sstable(
    root: &Path,
    manifest: &Manifest,
    reference: &SstableRef,
    table: &TableDef,
) -> Result<(Sstable, u64), StorageError> {
    let path = sstable_path(root, reference.id, reference.level);
    let mut file = File::open(&path).map_err(|error| {
        if error.kind() == io::ErrorKind::NotFound {
            StorageError::from(LsmError::MissingSstable(reference.id))
        } else {
            error.into()
        }
    })?;
    let mut header = [0_u8; SST_HEADER_SIZE];
    file.read_exact(&mut header).map_err(|error| {
        if error.kind() == io::ErrorKind::UnexpectedEof {
            StorageError::from(LsmError::InvalidSstable {
                sstable_id: reference.id,
                reason: "header is truncated",
            })
        } else {
            error.into()
        }
    })?;
    let (block_count, mut bloom) = decode_sstable_header(&header, manifest, reference)?;
    file.read_exact(&mut bloom.bits).map_err(|error| {
        if error.kind() == io::ErrorKind::UnexpectedEof {
            StorageError::from(LsmError::InvalidSstable {
                sstable_id: reference.id,
                reason: "Bloom payload is truncated",
            })
        } else {
            error.into()
        }
    })?;
    bloom = decode_bloom_payload(
        bloom.algorithm,
        bloom.hash_count,
        bloom.bit_count,
        &bloom.bits,
        read_u32(&header, 140)?,
        reference.id,
    )?;
    let first_block_offset = (SST_HEADER_SIZE as u64)
        .checked_add(reference.bloom_bytes)
        .ok_or(LsmError::InvalidSstable {
            sstable_id: reference.id,
            reason: "Bloom end overflows",
        })?;
    let blocks = read_sstable_footer(
        &mut file,
        reference,
        block_count,
        manifest.key_type,
        first_block_offset,
    )?;
    let mut total_entries = 0_u64;
    let mut previous = None;
    let mut actual_min = None;
    let mut max_commit = 0_u64;
    for (block_index, expected) in blocks.iter().enumerate() {
        let (meta, entries, _) = read_sstable_block(
            &mut file,
            reference.id,
            block_index as u32,
            expected.offset,
            manifest.key_type,
            table,
        )?;
        if meta.offset != expected.offset
            || meta.payload_length != expected.payload_length
            || meta.entry_count != expected.entry_count
            || meta.first != expected.first
            || meta.last != expected.last
        {
            return Err(LsmError::InvalidSstable {
                sstable_id: reference.id,
                reason: "block differs from footer index",
            }
            .into());
        }
        for entry in &entries {
            max_commit = max_commit.max(entry.version.0);
            actual_min.get_or_insert(entry.key);
            if previous.is_some_and(|previous: (PhysicalKey, LsmCommitSeq)| {
                previous >= (entry.key, entry.version)
            }) {
                return Err(LsmError::InvalidSstable {
                    sstable_id: reference.id,
                    reason: "entries are not strictly sorted",
                }
                .into());
            }
            if !bloom.might_contain(entry.key.clustering) {
                return Err(LsmError::InvalidSstable {
                    sstable_id: reference.id,
                    reason: "Bloom has a false negative for a persisted entry",
                }
                .into());
            }
            previous = Some((entry.key, entry.version));
        }
        total_entries =
            total_entries
                .checked_add(entries.len() as u64)
                .ok_or(LsmError::InvalidSstable {
                    sstable_id: reference.id,
                    reason: "entry count overflows",
                })?;
    }
    if total_entries != reference.entry_count
        || actual_min != Some(reference.min)
        || previous.map(|(key, _)| key) != Some(reference.max)
    {
        return Err(LsmError::InvalidSstable {
            sstable_id: reference.id,
            reason: "file entry count or key bounds differ from header",
        }
        .into());
    }
    Ok((
        Sstable {
            reference: reference.clone(),
            path,
            blocks,
            bloom,
        },
        max_commit,
    ))
}

fn decode_bloom_payload(
    algorithm: u8,
    hash_count: u8,
    bit_count: u64,
    bits: &[u8],
    stored_checksum: u32,
    sstable_id: u64,
) -> Result<BloomFilter, StorageError> {
    let expected_bytes = bit_count.checked_div(8).ok_or(LsmError::InvalidSstable {
        sstable_id,
        reason: "Bloom bit count is invalid",
    })?;
    if algorithm != BLOOM_ALGORITHM_VERSION
        || hash_count == 0
        || hash_count > 32
        || bit_count == 0
        || bit_count > BLOOM_MAX_BITS
        || bit_count % 8 != 0
        || expected_bytes != bits.len() as u64
    {
        return Err(LsmError::InvalidSstable {
            sstable_id,
            reason: "Bloom payload metadata is invalid",
        }
        .into());
    }
    let computed = crc32c::crc32c(bits);
    if stored_checksum != computed {
        return Err(LsmError::SstableChecksum {
            sstable_id,
            block: u32::MAX - 1,
            stored: stored_checksum,
            computed,
        }
        .into());
    }
    Ok(BloomFilter {
        algorithm,
        bit_count,
        hash_count,
        bits: bits.to_vec(),
    })
}

fn read_sstable_block(
    file: &mut File,
    sstable_id: u64,
    block_index: u32,
    offset: u64,
    key_type: PhysicalType,
    table: &TableDef,
) -> Result<(BlockMeta, Vec<VersionedEntry>, u64), StorageError> {
    file.seek(SeekFrom::Start(offset))?;
    let mut header = [0_u8; SST_BLOCK_HEADER_SIZE];
    file.read_exact(&mut header).map_err(|error| {
        if error.kind() == io::ErrorKind::UnexpectedEof {
            StorageError::from(LsmError::InvalidSstable {
                sstable_id,
                reason: "block header is truncated",
            })
        } else {
            error.into()
        }
    })?;
    if &header[0..4] != SST_BLOCK_MAGIC {
        return Err(LsmError::InvalidSstable {
            sstable_id,
            reason: "block magic does not match",
        }
        .into());
    }
    let payload_length = read_u32(&header, 4)?;
    let entry_count = read_u32(&header, 8)?;
    if payload_length as usize > SST_MAX_BLOCK_BYTES || entry_count == 0 {
        return Err(LsmError::InvalidSstable {
            sstable_id,
            reason: "block length or count is invalid",
        }
        .into());
    }
    let first = decode_physical_key(&header[12..32], key_type)?;
    let last = decode_physical_key(&header[32..52], key_type)?;
    if first > last {
        return Err(LsmError::InvalidSstable {
            sstable_id,
            reason: "block key bounds are reversed",
        }
        .into());
    }
    let mut payload = vec![0_u8; payload_length as usize];
    file.read_exact(&mut payload).map_err(|error| {
        if error.kind() == io::ErrorKind::UnexpectedEof {
            StorageError::from(LsmError::InvalidSstable {
                sstable_id,
                reason: "block payload is truncated",
            })
        } else {
            error.into()
        }
    })?;
    let mut checksum_bytes = [0_u8; 4];
    file.read_exact(&mut checksum_bytes).map_err(|error| {
        if error.kind() == io::ErrorKind::UnexpectedEof {
            StorageError::from(LsmError::InvalidSstable {
                sstable_id,
                reason: "block checksum is truncated",
            })
        } else {
            error.into()
        }
    })?;
    let stored = u32::from_le_bytes(checksum_bytes);
    let mut checksummed = Vec::with_capacity(header.len() + payload.len());
    checksummed.extend_from_slice(&header);
    checksummed.extend_from_slice(&payload);
    let computed = crc32c::crc32c(&checksummed);
    if stored != computed {
        return Err(LsmError::SstableChecksum {
            sstable_id,
            block: block_index,
            stored,
            computed,
        }
        .into());
    }
    let entries = decode_sstable_entries(&payload, entry_count, key_type, sstable_id, table)?;
    if entries.first().map(|entry| entry.key) != Some(first)
        || entries.last().map(|entry| entry.key) != Some(last)
    {
        return Err(LsmError::InvalidSstable {
            sstable_id,
            reason: "block bounds differ from entries",
        }
        .into());
    }
    let next = offset
        .checked_add((SST_BLOCK_HEADER_SIZE + payload.len() + 4) as u64)
        .ok_or(LsmError::InvalidSstable {
            sstable_id,
            reason: "block end overflows",
        })?;
    Ok((
        BlockMeta {
            offset,
            payload_length,
            entry_count,
            first,
            last,
        },
        entries,
        next,
    ))
}

fn decode_sstable_entries(
    payload: &[u8],
    count: u32,
    key_type: PhysicalType,
    sstable_id: u64,
    table: &TableDef,
) -> Result<Vec<VersionedEntry>, StorageError> {
    let mut offset = 0_usize;
    let mut entries = Vec::with_capacity(count as usize);
    for _ in 0..count {
        if payload.len().saturating_sub(offset) < 32 {
            return Err(LsmError::InvalidSstable {
                sstable_id,
                reason: "entry is truncated",
            }
            .into());
        }
        let clustering = decode_key_bits(read_u64(payload, offset)?, key_type)?;
        let row_id = LsmRowId(read_u64(payload, offset + 8)?);
        let version = LsmCommitSeq(read_u64(payload, offset + 16)?);
        let tag = payload[offset + 24];
        if payload[offset + 25..offset + 28]
            .iter()
            .any(|byte| *byte != 0)
        {
            return Err(LsmError::InvalidSstable {
                sstable_id,
                reason: "entry reserved bytes are nonzero",
            }
            .into());
        }
        let length = usize::try_from(read_u32(payload, offset + 28)?).map_err(|_| {
            LsmError::InvalidSstable {
                sstable_id,
                reason: "row length does not fit usize",
            }
        })?;
        offset = offset.checked_add(32).ok_or(LsmError::InvalidSstable {
            sstable_id,
            reason: "entry offset overflows",
        })?;
        let end = offset.checked_add(length).ok_or(LsmError::InvalidSstable {
            sstable_id,
            reason: "row length overflows",
        })?;
        let row = payload.get(offset..end).ok_or(LsmError::InvalidSstable {
            sstable_id,
            reason: "row payload is truncated",
        })?;
        offset = end;
        if row_id.0 == 0 || version.0 == 0 {
            return Err(LsmError::InvalidSstable {
                sstable_id,
                reason: "row ID or version is zero",
            }
            .into());
        }
        let value = match tag {
            1 => {
                let _ = decode_row(row, table)?;
                EntryValue::Put(row.to_vec())
            }
            2 if length == 0 => EntryValue::Tombstone,
            2 => {
                return Err(LsmError::InvalidSstable {
                    sstable_id,
                    reason: "tombstone contains row bytes",
                }
                .into());
            }
            _ => {
                return Err(LsmError::InvalidSstable {
                    sstable_id,
                    reason: "entry tag is invalid",
                }
                .into());
            }
        };
        entries.push(VersionedEntry {
            key: PhysicalKey { clustering, row_id },
            version,
            value,
        });
    }
    if offset != payload.len() {
        return Err(LsmError::InvalidSstable {
            sstable_id,
            reason: "block has trailing bytes",
        }
        .into());
    }
    Ok(entries)
}

fn physical_type_tag(kind: PhysicalType) -> Result<u8, StorageError> {
    match kind {
        PhysicalType::Int64 => Ok(1),
        PhysicalType::UInt64 => Ok(2),
        other => Err(LsmError::UnsupportedClusteringType(other).into()),
    }
}
fn decode_physical_type(tag: u8) -> Result<PhysicalType, StorageError> {
    match tag {
        1 => Ok(PhysicalType::Int64),
        2 => Ok(PhysicalType::UInt64),
        _ => Err(LsmError::InvalidManifest("invalid clustering type tag").into()),
    }
}
fn encode_physical_key(bytes: &mut [u8], key: PhysicalKey) -> Result<(), StorageError> {
    if bytes.len() != 20 || key.row_id.0 == 0 {
        return Err(LsmError::InvalidManifest("physical key encoding is invalid").into());
    }
    bytes[0] = physical_type_tag(key.clustering.kind())?;
    bytes[1..4].fill(0);
    bytes[4..12].copy_from_slice(&key.clustering.bits().to_le_bytes());
    bytes[12..20].copy_from_slice(&key.row_id.0.to_le_bytes());
    Ok(())
}
fn encode_physical_key_vec(bytes: &mut Vec<u8>, key: PhysicalKey) -> Result<(), StorageError> {
    let mut encoded = [0_u8; 20];
    encode_physical_key(&mut encoded, key)?;
    bytes.extend_from_slice(&encoded);
    Ok(())
}
fn decode_physical_key(bytes: &[u8], expected: PhysicalType) -> Result<PhysicalKey, StorageError> {
    if bytes.len() != 20 || bytes[1..4].iter().any(|byte| *byte != 0) {
        return Err(LsmError::InvalidManifest("physical key is malformed").into());
    }
    let kind = decode_physical_type(bytes[0])?;
    if kind != expected {
        return Err(LsmError::InvalidManifest("physical key type differs from manifest").into());
    }
    let clustering = decode_key_bits(read_u64(bytes, 4)?, kind)?;
    let row_id = LsmRowId(read_u64(bytes, 12)?);
    if row_id.0 == 0 {
        return Err(LsmError::InvalidManifest("physical key row ID is zero").into());
    }
    Ok(PhysicalKey { clustering, row_id })
}
fn decode_key_bits(bits: u64, kind: PhysicalType) -> Result<ClusteringKey, StorageError> {
    match kind {
        PhysicalType::Int64 => Ok(ClusteringKey::Int64(bits as i64)),
        PhysicalType::UInt64 => Ok(ClusteringKey::UInt64(bits)),
        other => Err(LsmError::UnsupportedClusteringType(other).into()),
    }
}

fn read_u16(bytes: &[u8], offset: usize) -> Result<u16, StorageError> {
    Ok(u16::from_le_bytes(read_array(bytes, offset)?))
}
fn read_u32(bytes: &[u8], offset: usize) -> Result<u32, StorageError> {
    Ok(u32::from_le_bytes(read_array(bytes, offset)?))
}
fn read_u64(bytes: &[u8], offset: usize) -> Result<u64, StorageError> {
    Ok(u64::from_le_bytes(read_array(bytes, offset)?))
}
fn read_array<const N: usize>(bytes: &[u8], offset: usize) -> Result<[u8; N], StorageError> {
    let end = offset
        .checked_add(N)
        .ok_or_else(|| crate::invalid_format("LSM decode offset overflows"))?;
    bytes
        .get(offset..end)
        .ok_or_else(|| crate::invalid_format("LSM input is truncated"))?
        .try_into()
        .map_err(|_| crate::invalid_format("LSM fixed-width field is truncated"))
}
fn read_partial(reader: &mut impl Read, bytes: &mut [u8]) -> Result<usize, StorageError> {
    let mut read = 0;
    while read < bytes.len() {
        match reader.read(&mut bytes[read..]) {
            Ok(0) => break,
            Ok(count) => read += count,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(read)
}
fn sync_directory(path: &Path) -> Result<(), StorageError> {
    File::open(path)?.sync_all().map_err(Into::into)
}

/// Bounded manifest decoder entry point for the fuzz target.
pub fn fuzz_lsm_manifest_bytes(bytes: &[u8]) {
    let _ = decode_manifest(bytes);
}

/// Bounded WAL decoder entry point for the fuzz target.
pub fn fuzz_lsm_wal_bytes(bytes: &[u8]) {
    if bytes.len() < WAL_HEADER_SIZE {
        return;
    }
    let mut cursor = io::Cursor::new(bytes);
    let mut header = [0_u8; WAL_HEADER_SIZE];
    if cursor.read_exact(&mut header).is_ok() {
        let storage_id = StorageId(u64::from_le_bytes(
            header[8..16].try_into().unwrap_or([0; 8]),
        ));
        let generation = u64::from_le_bytes(header[16..24].try_into().unwrap_or([0; 8]));
        if decode_wal_header(&header, storage_id, generation).is_ok() {
            let _ = decode_wal_records_cursor(&mut cursor, storage_id);
        }
    }
}

fn decode_wal_records_cursor(
    cursor: &mut io::Cursor<&[u8]>,
    storage_id: StorageId,
) -> Result<(), StorageError> {
    let mut offset = WAL_HEADER_SIZE;
    let bytes = cursor.get_ref();
    while offset < bytes.len() {
        if bytes.len() - offset < WAL_RECORD_HEADER_SIZE {
            return Ok(());
        }
        let header = &bytes[offset..offset + WAL_RECORD_HEADER_SIZE];
        let length = read_u32(header, 8)? as usize;
        if length > WAL_MAX_RECORD_BYTES {
            return Err(LsmError::InvalidWal("record length exceeds bound").into());
        }
        let end = offset
            .checked_add(WAL_RECORD_HEADER_SIZE)
            .and_then(|value| value.checked_add(length))
            .and_then(|value| value.checked_add(4))
            .ok_or(LsmError::InvalidWal("record size overflows"))?;
        if end > bytes.len() {
            return Ok(());
        }
        let actual = StorageId(read_u64(header, 12)?);
        if actual != storage_id {
            return Err(LsmError::StorageIdMismatch {
                expected: storage_id,
                actual,
            }
            .into());
        }
        let _ = decode_wal_record(header[6], &bytes[offset + WAL_RECORD_HEADER_SIZE..end - 4]);
        offset = end;
    }
    Ok(())
}

/// Bounded SSTable-block decoder entry point for the fuzz target.
pub fn fuzz_lsm_sstable_block_bytes(bytes: &[u8]) {
    if bytes.len() > 6 {
        let payload = &bytes[6..];
        let bit_count = (payload.len() as u64).saturating_mul(8);
        let stored = u32::from_le_bytes(bytes[0..4].try_into().unwrap_or([0; 4]));
        let _ = decode_bloom_payload(bytes[4], bytes[5], bit_count, payload, stored, 1);
        if let Ok(filter) = decode_bloom_payload(
            BLOOM_ALGORITHM_VERSION,
            BLOOM_HASH_COUNT,
            bit_count,
            payload,
            crc32c::crc32c(payload),
            1,
        ) {
            let _ = filter.might_contain(ClusteringKey::Int64(i64::MIN));
            let _ = filter.might_contain(ClusteringKey::UInt64(u64::MAX));
        }
    }
    if bytes.len() >= SST_HEADER_SIZE && &bytes[0..4] == SST_MAGIC {
        let header: &[u8; SST_HEADER_SIZE] = match bytes[..SST_HEADER_SIZE].try_into() {
            Ok(header) => header,
            Err(_) => return,
        };
        let Ok(key_type) = decode_physical_type(header[65]) else {
            return;
        };
        let Ok(min) = decode_physical_key(&header[80..100], key_type) else {
            return;
        };
        let Ok(max) = decode_physical_key(&header[100..120], key_type) else {
            return;
        };
        let manifest = Manifest {
            storage_id: StorageId(u64::from_le_bytes(
                header[8..16].try_into().unwrap_or([0; 8]),
            )),
            table_id: TableId(u64::from_le_bytes(
                header[16..24].try_into().unwrap_or([0; 8]),
            )),
            schema_fingerprint: SchemaFingerprint::from_bytes(
                header[24..56].try_into().unwrap_or([0; 32]),
            ),
            clustering_column: ColumnId(1),
            key_type,
            generation: 1,
            wal_generation: 1,
            row_reservation_end: 1,
            txn_reservation_end: 1,
            commit_reservation_end: 1,
            next_sstable_id: u64::MAX,
            table_statistics: None,
            access_statistics: None,
            clustering_statistics: None,
            sstables: Vec::new(),
        };
        let reference = SstableRef {
            id: u64::from_le_bytes(header[56..64].try_into().unwrap_or([0; 8])),
            level: header[64],
            entry_count: u64::from_le_bytes(header[68..76].try_into().unwrap_or([0; 8])),
            file_bytes: u64::from_le_bytes(header[144..152].try_into().unwrap_or([0; 8])),
            bloom_bytes: u64::from_le_bytes(header[132..140].try_into().unwrap_or([0; 8])),
            min,
            max,
        };
        if let Ok((_, bloom)) = decode_sstable_header(header, &manifest, &reference) {
            let end = SST_HEADER_SIZE.saturating_add(bloom.bits.len());
            if let Some(payload) = bytes.get(SST_HEADER_SIZE..end) {
                let _ = crc32c::crc32c(payload)
                    == u32::from_le_bytes(header[140..144].try_into().unwrap_or([0; 4]));
            }
        }
    }
    if bytes.len() < SST_BLOCK_HEADER_SIZE || &bytes[0..4] != SST_BLOCK_MAGIC {
        return;
    }
    let length = u32::from_le_bytes(bytes[4..8].try_into().unwrap_or([0; 4])) as usize;
    if length > SST_MAX_BLOCK_BYTES || bytes.len() < SST_BLOCK_HEADER_SIZE + length + 4 {
        return;
    }
    let table = TableDef::new(TableId(1), "fuzz", Vec::new());
    let _ = decode_sstable_entries(
        &bytes[SST_BLOCK_HEADER_SIZE..SST_BLOCK_HEADER_SIZE + length],
        u32::from_le_bytes(bytes[8..12].try_into().unwrap_or([0; 4])),
        PhysicalType::Int64,
        1,
        &table,
    );
}

#[cfg(test)]
thread_local! {
    static MANIFEST_PUBLISH_FAILURE: Cell<Option<ManifestPublishPoint>> = const { Cell::new(None) };
    static OBSOLETE_DELETE_FAILURE: Cell<bool> = const { Cell::new(false) };
}

#[cfg(test)]
fn maybe_fail_manifest_publish(point: ManifestPublishPoint) -> Result<(), StorageError> {
    MANIFEST_PUBLISH_FAILURE.with(|failure| {
        if failure.get() == Some(point) {
            failure.set(None);
            Err(io::Error::other("injected LSM manifest publish failure").into())
        } else {
            Ok(())
        }
    })
}

#[cfg(test)]
fn maybe_fail_obsolete_delete() -> Result<(), StorageError> {
    OBSOLETE_DELETE_FAILURE.with(|failure| {
        if failure.replace(false) {
            Err(io::Error::other("injected obsolete SSTable delete failure").into())
        } else {
            Ok(())
        }
    })
}

#[cfg(not(test))]
fn maybe_fail_manifest_publish(_point: ManifestPublishPoint) -> Result<(), StorageError> {
    Ok(())
}

#[cfg(not(test))]
fn maybe_fail_obsolete_delete() -> Result<(), StorageError> {
    Ok(())
}

#[cfg(test)]
fn maybe_lsm_crash(point: &str) {
    if std::env::var_os("NETBADB_LSM_CRASH_CHILD").as_deref() == Some(std::ffi::OsStr::new("1"))
        && std::env::var_os("NETBADB_LSM_CRASH_POINT").as_deref()
            == Some(std::ffi::OsStr::new(point))
    {
        std::process::exit(86);
    }
}

#[cfg(test)]
mod tests {
    use netbadb_index::{IndexBound, IndexRange};
    use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
    use netbadb_types::{
        ColumnId, DatabaseTxnId, Float32Value, Float64Value, LsmRowId, PhysicalType, ScalarValue,
        StorageId, TableId, TxnId,
    };

    use super::{LsmObservedVersion, LsmStorage, LsmTransaction};
    use crate::{
        PreparedDecision, PreparedTransactionState, PreparedTxnResolution, RecoveryError,
        StorageError, TransactionState,
    };

    fn table() -> TableDef {
        TableDef::new(
            TableId(11),
            "events",
            vec![
                ColumnDef::new(
                    ColumnId(1),
                    "cluster",
                    TypeSpec::Physical(PhysicalType::Int64),
                ),
                ColumnDef::new(
                    ColumnId(2),
                    "payload",
                    TypeSpec::Physical(PhysicalType::Text),
                )
                .nullable(true),
            ],
        )
    }

    fn root(case: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "netbadb-lsm-{case}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ))
    }

    fn cleanup(path: &std::path::Path) {
        let _ = std::fs::remove_dir_all(path);
    }

    fn row(key: i64, payload: &str) -> Vec<ScalarValue> {
        vec![ScalarValue::Int64(key), ScalarValue::Text(payload.into())]
    }

    fn uint_table() -> TableDef {
        TableDef::new(
            TableId(12),
            "uint_events",
            vec![
                ColumnDef::new(
                    ColumnId(1),
                    "cluster",
                    TypeSpec::Physical(PhysicalType::UInt64),
                ),
                ColumnDef::new(
                    ColumnId(2),
                    "payload",
                    TypeSpec::Physical(PhysicalType::Text),
                ),
            ],
        )
    }

    fn all_scalar_table() -> TableDef {
        let mut columns = vec![ColumnDef::new(
            ColumnId(1),
            "cluster",
            TypeSpec::Physical(PhysicalType::Int64),
        )];
        for (position, physical) in [
            PhysicalType::Bool,
            PhysicalType::Int8,
            PhysicalType::Int16,
            PhysicalType::Int32,
            PhysicalType::Int128,
            PhysicalType::UInt8,
            PhysicalType::UInt16,
            PhysicalType::UInt32,
            PhysicalType::UInt64,
            PhysicalType::UInt128,
            PhysicalType::Float32,
            PhysicalType::Float64,
            PhysicalType::Text,
            PhysicalType::Bytes,
        ]
        .into_iter()
        .enumerate()
        {
            columns.push(ColumnDef::new(
                ColumnId(position as u32 + 2),
                format!("v{position}"),
                TypeSpec::Physical(physical),
            ));
        }
        TableDef::new(TableId(13), "all_scalars", columns)
    }

    fn all_scalar_row(key: i64, alternate: bool) -> Vec<ScalarValue> {
        vec![
            ScalarValue::Int64(key),
            ScalarValue::Bool(alternate),
            ScalarValue::Int8(if alternate { i8::MAX } else { i8::MIN }),
            ScalarValue::Int16(if alternate { i16::MAX } else { i16::MIN }),
            ScalarValue::Int32(if alternate { i32::MAX } else { i32::MIN }),
            ScalarValue::Int128(if alternate { i128::MAX } else { i128::MIN }),
            ScalarValue::UInt8(if alternate { u8::MAX } else { 0 }),
            ScalarValue::UInt16(if alternate { u16::MAX } else { 0 }),
            ScalarValue::UInt32(if alternate { u32::MAX } else { 0 }),
            ScalarValue::UInt64(if alternate { u64::MAX } else { 0 }),
            ScalarValue::UInt128(if alternate { u128::MAX } else { 0 }),
            ScalarValue::Float32(Float32Value::new(if alternate {
                f32::INFINITY
            } else {
                f32::NEG_INFINITY
            })),
            ScalarValue::Float64(Float64Value::new(if alternate {
                f64::NAN
            } else {
                f64::from_bits(1)
            })),
            ScalarValue::Text(if alternate { "updated" } else { "initial" }.into()),
            ScalarValue::Bytes(if alternate {
                vec![0, 0xff, 0x80]
            } else {
                vec![]
            }),
        ]
    }

    #[test]
    fn every_scalar_payload_survives_lsm_wal_update_flush_and_reopen() {
        let root = root("all-scalar-payloads");
        cleanup(&root);
        let schema = all_scalar_table();
        let mut storage = LsmStorage::create(&root, schema.clone(), ColumnId(1)).expect("create");
        let handle = storage
            .insert(&all_scalar_row(7, false))
            .expect("insert all scalar payloads");
        storage
            .update(handle, &all_scalar_row(7, true))
            .expect("update all scalar payloads");
        storage.flush().expect("flush all scalar payloads");
        storage.close().expect("close all scalar payloads");

        let mut reopened = LsmStorage::open(&root, schema).expect("reopen all scalar payloads");
        let view = reopened.read_view().expect("read view");
        let columns = (1..=15).map(ColumnId).collect::<Vec<_>>();
        let rows = reopened
            .scan_columns_with_view(&columns, &view)
            .expect("scan reopened all scalar payloads");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1, all_scalar_row(7, true));
        drop(view);
        reopened.close().expect("close reopened storage");
        cleanup(&root);
    }

    #[test]
    fn duplicates_read_your_writes_snapshots_key_moves_and_tombstones() {
        let root = root("mvcc");
        cleanup(&root);
        let mut storage =
            LsmStorage::create_with_storage_id(&root, table(), ColumnId(1), StorageId(9))
                .expect("create");
        let first = storage.insert(&row(10, "a")).expect("first");
        let second = storage.insert(&row(10, "b")).expect("second");
        assert_ne!(first.row_id, second.row_id);
        let old_view = storage.read_view().expect("old view");

        let mut transaction = storage.begin_transaction().expect("transaction");
        let statement = transaction.begin_statement().expect("statement");
        let targets = storage
            .point_lookup_columns_with_view(&ScalarValue::Int64(10), &[ColumnId(2)], &statement)
            .expect("point");
        assert_eq!(targets.len(), 2);
        drop(statement);
        let moved = storage
            .update_in(&mut transaction, targets[0].0, &row(20, "moved"))
            .expect("move");
        let own_view = transaction.begin_statement().expect("own view");
        assert_eq!(
            storage
                .point_lookup_columns_with_view(&ScalarValue::Int64(10), &[ColumnId(2)], &own_view)
                .expect("old key")
                .len(),
            1
        );
        assert_eq!(
            storage
                .point_lookup_columns_with_view(&ScalarValue::Int64(20), &[ColumnId(2)], &own_view)
                .expect("new key")
                .len(),
            1
        );
        drop(own_view);
        transaction.commit().expect("commit");

        assert_eq!(
            storage
                .point_lookup_columns_with_view(&ScalarValue::Int64(10), &[ColumnId(2)], &old_view)
                .expect("old snapshot")
                .len(),
            2
        );
        drop(old_view);
        let new_view = storage.read_view().expect("new view");
        assert_eq!(
            storage
                .point_lookup_columns_with_view(&ScalarValue::Int64(10), &[ColumnId(2)], &new_view)
                .expect("new old key")
                .len(),
            1
        );
        assert_eq!(
            storage
                .point_lookup_columns_with_view(&ScalarValue::Int64(20), &[ColumnId(2)], &new_view)
                .expect("new key")
                .len(),
            1
        );
        drop(new_view);

        assert!(matches!(moved.observed, LsmObservedVersion::Pending(_)));
        let current = storage
            .read_view()
            .and_then(|view| storage.refresh_handle(moved.row_id, &view))
            .expect("current handle");
        let stale = storage.delete(first).expect_err("old handle must be stale");
        assert!(
            matches!(
                stale,
                StorageError::Lsm(super::LsmError::StaleHandle { .. })
            ),
            "unexpected stale-handle error: {stale:?}"
        );
        storage.delete(current).expect("delete");
        let view = storage.read_view().expect("after delete");
        assert!(
            storage
                .point_lookup_columns_with_view(&ScalarValue::Int64(20), &[ColumnId(2)], &view)
                .expect("deleted point")
                .is_empty()
        );
        drop(view);
        cleanup(&root);
    }

    #[test]
    fn uint64_bounds_and_analyze_statistics_survive_reopen() {
        let root = root("uint-bounds-stats");
        cleanup(&root);
        let schema = uint_table();
        let mut storage = LsmStorage::create(&root, schema.clone(), ColumnId(1)).expect("create");
        for (key, payload) in [(u64::MIN, "min"), (u64::MAX, "max"), (u64::MAX, "dup")] {
            storage
                .insert(&[ScalarValue::UInt64(key), ScalarValue::Text(payload.into())])
                .expect("insert boundary");
        }
        let view = storage.read_view().expect("view");
        assert_eq!(
            storage
                .point_lookup_columns_with_view(
                    &ScalarValue::UInt64(u64::MAX),
                    &[ColumnId(2)],
                    &view,
                )
                .expect("max point")
                .len(),
            2
        );
        assert_eq!(
            storage
                .range_lookup_columns_with_view(
                    &IndexRange {
                        lower: IndexBound::Included(ScalarValue::UInt64(u64::MIN)),
                        upper: IndexBound::Included(ScalarValue::UInt64(u64::MAX)),
                    },
                    &[ColumnId(1)],
                    &view,
                )
                .expect("full boundary range")
                .len(),
            3
        );
        drop(view);
        storage.analyze().expect("analyze");
        assert_eq!(
            storage.table_statistics().expect("table stats").row_count,
            3
        );
        assert_eq!(
            storage
                .access_statistics()
                .expect("access stats")
                .distinct_non_null_keys,
            2
        );
        storage.close().expect("close");
        let reopened = LsmStorage::open(&root, schema).expect("reopen");
        assert_eq!(
            reopened
                .table_statistics()
                .expect("persisted stats")
                .row_count,
            3
        );
        assert_eq!(
            reopened
                .access_statistics()
                .expect("persisted access stats")
                .distinct_non_null_keys,
            2
        );
        let inspection = reopened.inspection();
        assert_eq!(inspection.analyzed_live_row_count, Some(3));
        assert_eq!(
            inspection.analyzed_min_clustering,
            Some(ScalarValue::UInt64(u64::MIN))
        );
        assert_eq!(
            inspection.analyzed_max_clustering,
            Some(ScalarValue::UInt64(u64::MAX))
        );
        reopened.close().expect("close reopened");
        cleanup(&root);
    }

    #[test]
    fn wal_sync_failures_retry_same_prepare_and_commit_records() {
        let root = root("wal-retry");
        cleanup(&root);
        let mut storage = LsmStorage::create(&root, table(), ColumnId(1)).expect("create");
        let mut transaction = storage.begin_transaction().expect("transaction");
        storage
            .insert_in(&mut transaction, &row(1, "retry"))
            .expect("insert");
        transaction.shared.borrow_mut().wal.fail_next_sync = true;
        assert!(transaction.prepare(DatabaseTxnId(700)).is_err());
        assert_eq!(transaction.state(), TransactionState::PreparePending);
        transaction
            .prepare(DatabaseTxnId(700))
            .expect("retry same prepare");
        transaction.shared.borrow_mut().wal.fail_next_sync = true;
        assert!(transaction.commit_prepared(DatabaseTxnId(700)).is_err());
        assert_eq!(transaction.state(), TransactionState::CommitPending);
        transaction
            .commit_prepared(DatabaseTxnId(700))
            .expect("retry same commit");
        let manifest = super::read_manifest(&root).expect("manifest");
        let (_, records) =
            super::LsmWal::open(&root, manifest.storage_id, manifest.wal_generation).expect("WAL");
        assert_eq!(
            records
                .iter()
                .filter(|record| matches!(record, super::WalRecord::MutationBatch { .. }))
                .count(),
            1
        );
        assert_eq!(
            records
                .iter()
                .filter(|record| matches!(record, super::WalRecord::Prepare { .. }))
                .count(),
            1
        );
        assert_eq!(
            records
                .iter()
                .filter(|record| matches!(record, super::WalRecord::Commit { .. }))
                .count(),
            1
        );
        drop(transaction);
        storage.close().expect("close");
        let mut reopened = LsmStorage::open(&root, table()).expect("reopen");
        let view = reopened.read_view().expect("view");
        assert_eq!(
            reopened
                .scan_columns_with_view(&[ColumnId(1)], &view)
                .expect("scan")
                .len(),
            1
        );
        drop(view);
        reopened.close().expect("close reopened");
        cleanup(&root);
    }

    #[test]
    fn prepared_batch_preserves_commit_sequences_and_uses_one_wal_sync() {
        let root = root("prepared-batch");
        cleanup(&root);
        let mut storage = LsmStorage::create(&root, table(), ColumnId(1)).unwrap();
        let mut first = storage.begin_transaction().unwrap();
        storage.insert_in(&mut first, &row(1, "one")).unwrap();
        first.prepare(DatabaseTxnId(801)).unwrap();
        first.park_prepared(DatabaseTxnId(801)).unwrap();
        let mut second = storage.begin_transaction().unwrap();
        storage.insert_in(&mut second, &row(2, "two")).unwrap();
        second.prepare(DatabaseTxnId(802)).unwrap();
        second.park_prepared(DatabaseTxnId(802)).unwrap();
        let mut third = storage.begin_transaction().unwrap();
        storage.insert_in(&mut third, &row(3, "three")).unwrap();
        third.prepare(DatabaseTxnId(803)).unwrap();
        third.park_prepared(DatabaseTxnId(803)).unwrap();

        let report = LsmTransaction::commit_prepared_batch(&mut [
            (&mut first, DatabaseTxnId(801)),
            (&mut second, DatabaseTxnId(802)),
            (&mut third, DatabaseTxnId(803)),
        ])
        .unwrap();
        assert_eq!(report.member_count, 3);
        assert_eq!(report.commit_records_staged, 3);
        assert_eq!(report.wal_syncs, 1);
        assert_eq!(report.last_local_boundary - report.first_local_boundary, 2);
        let inspection = storage.prepared_runtime_inspection();
        assert_eq!(inspection.prepare_sync_count, 3);
        assert_eq!(inspection.single_commit_sync_count, 0);
        assert_eq!(inspection.group_commit_barrier_sync_count, 1);
        assert!(inspection.active_group_chain.is_empty());
        let view = storage.read_view().unwrap();
        assert_eq!(
            storage
                .scan_columns_with_view(&[ColumnId(1)], &view)
                .unwrap()
                .len(),
            3
        );
        drop(view);
        drop(first);
        drop(second);
        drop(third);
        storage.close().unwrap();
        cleanup(&root);
    }

    #[test]
    fn staged_prepare_batch_releases_writer_retries_same_lsm_records_and_aborts_tail_first() {
        let root = root("staged-prepare-batch");
        cleanup(&root);
        let mut storage = LsmStorage::create(&root, table(), ColumnId(1)).unwrap();
        let mut first = storage.begin_transaction().unwrap();
        storage.insert_in(&mut first, &row(1, "one")).unwrap();
        first.stage_group_prepare(DatabaseTxnId(841)).unwrap();
        let first_lsn = first.last_lsn();
        let mut second = storage.begin_transaction().unwrap();
        storage
            .insert_in(&mut second, &row(2, "two"))
            .expect("staging releases the LSM writer");
        second.stage_group_prepare(DatabaseTxnId(842)).unwrap();
        let second_lsn = second.last_lsn();
        assert_eq!(first.state(), TransactionState::ParkedPreparePending);
        assert_eq!(second.state(), TransactionState::ParkedPreparePending);
        let pending = storage.prepared_runtime_inspection();
        assert_eq!(pending.parked_prepare_pending_count, 2);
        assert_eq!(pending.prepare_sync_count, 0);

        first.shared.borrow_mut().wal.fail_next_sync = true;
        assert!(
            LsmTransaction::durabilize_group_prepare_batch(&mut [
                (&mut first, DatabaseTxnId(841)),
                (&mut second, DatabaseTxnId(842)),
            ])
            .is_err()
        );
        assert_eq!(first.last_lsn(), first_lsn);
        assert_eq!(second.last_lsn(), second_lsn);
        let report = LsmTransaction::durabilize_group_prepare_batch(&mut [
            (&mut first, DatabaseTxnId(841)),
            (&mut second, DatabaseTxnId(842)),
        ])
        .unwrap();
        assert_eq!(report.prepare_records_staged, 2);
        assert_eq!(report.wal_syncs, 1);
        assert_eq!(first.state(), TransactionState::ParkedPrepared);
        assert_eq!(second.state(), TransactionState::ParkedPrepared);
        let durable = storage.prepared_runtime_inspection();
        assert_eq!(durable.parked_prepare_pending_count, 0);
        assert_eq!(durable.group_prepare_barrier_sync_count, 1);
        let manifest = super::read_manifest(&root).unwrap();
        let (_, records) =
            super::LsmWal::open(&root, manifest.storage_id, manifest.wal_generation).unwrap();
        assert_eq!(
            records
                .iter()
                .filter(|record| matches!(record, super::WalRecord::Prepare { .. }))
                .count(),
            2
        );
        second.rollback_prepared(DatabaseTxnId(842)).unwrap();
        first.rollback_prepared(DatabaseTxnId(841)).unwrap();
        assert!(
            storage
                .prepared_runtime_inspection()
                .active_group_chain
                .is_empty()
        );

        drop(first);
        drop(second);
        storage.close().unwrap();
        cleanup(&root);
    }

    #[test]
    fn prepared_batch_sync_retry_reuses_lsm_commit_sequences() {
        let root = root("prepared-batch-retry");
        cleanup(&root);
        let mut storage = LsmStorage::create(&root, table(), ColumnId(1)).unwrap();
        let mut first = storage.begin_transaction().unwrap();
        storage.insert_in(&mut first, &row(1, "one")).unwrap();
        first.prepare(DatabaseTxnId(811)).unwrap();
        first.park_prepared(DatabaseTxnId(811)).unwrap();
        let mut second = storage.begin_transaction().unwrap();
        storage.insert_in(&mut second, &row(2, "two")).unwrap();
        second.prepare(DatabaseTxnId(812)).unwrap();
        second.park_prepared(DatabaseTxnId(812)).unwrap();

        first.shared.borrow_mut().wal.fail_next_sync = true;
        assert!(
            LsmTransaction::commit_prepared_batch(&mut [
                (&mut first, DatabaseTxnId(811)),
                (&mut second, DatabaseTxnId(812)),
            ])
            .is_err()
        );
        let first_seq = first.pending_commit_seq.unwrap();
        let second_seq = second.pending_commit_seq.unwrap();
        let report = LsmTransaction::commit_prepared_batch(&mut [
            (&mut first, DatabaseTxnId(811)),
            (&mut second, DatabaseTxnId(812)),
        ])
        .unwrap();
        assert_eq!(report.commit_records_staged, 0);
        assert_eq!(first.pending_commit_seq, Some(first_seq));
        assert_eq!(second.pending_commit_seq, Some(second_seq));
        assert_eq!(
            storage
                .prepared_runtime_inspection()
                .group_commit_barrier_sync_count,
            1
        );
        let manifest = super::read_manifest(&root).unwrap();
        let (_, records) =
            super::LsmWal::open(&root, manifest.storage_id, manifest.wal_generation).unwrap();
        assert_eq!(
            records
                .iter()
                .filter(|record| matches!(record, super::WalRecord::Commit { .. }))
                .count(),
            2
        );
        drop(first);
        drop(second);
        storage.close().unwrap();
        cleanup(&root);
    }

    #[test]
    fn prepared_batch_mid_append_retry_preserves_lsm_sequence_identity() {
        let root = root("prepared-batch-append-retry");
        cleanup(&root);
        let mut storage = LsmStorage::create(&root, table(), ColumnId(1)).unwrap();
        let mut first = storage.begin_transaction().unwrap();
        storage.insert_in(&mut first, &row(1, "one")).unwrap();
        first.prepare(DatabaseTxnId(821)).unwrap();
        first.park_prepared(DatabaseTxnId(821)).unwrap();
        let mut second = storage.begin_transaction().unwrap();
        storage.insert_in(&mut second, &row(2, "two")).unwrap();
        second.prepare(DatabaseTxnId(822)).unwrap();
        second.park_prepared(DatabaseTxnId(822)).unwrap();
        let mut third = storage.begin_transaction().unwrap();
        storage.insert_in(&mut third, &row(3, "three")).unwrap();
        third.prepare(DatabaseTxnId(823)).unwrap();
        third.park_prepared(DatabaseTxnId(823)).unwrap();
        first.shared.borrow_mut().wal.fail_append_after_calls = Some(1);
        assert!(
            LsmTransaction::commit_prepared_batch(&mut [
                (&mut first, DatabaseTxnId(821)),
                (&mut second, DatabaseTxnId(822)),
                (&mut third, DatabaseTxnId(823)),
            ])
            .is_err()
        );
        let first_seq = first.pending_commit_seq.unwrap();
        let second_seq = second.pending_commit_seq.unwrap();
        assert_eq!(second.state(), TransactionState::CommitPending);
        assert_eq!(third.state(), TransactionState::ParkedPrepared);

        let report = LsmTransaction::commit_prepared_batch(&mut [
            (&mut first, DatabaseTxnId(821)),
            (&mut second, DatabaseTxnId(822)),
            (&mut third, DatabaseTxnId(823)),
        ])
        .unwrap();
        assert_eq!(report.commit_records_staged, 2);
        assert_eq!(first.pending_commit_seq, Some(first_seq));
        assert_eq!(second.pending_commit_seq, Some(second_seq));
        let manifest = super::read_manifest(&root).unwrap();
        let (_, records) =
            super::LsmWal::open(&root, manifest.storage_id, manifest.wal_generation).unwrap();
        assert_eq!(
            records
                .iter()
                .filter(|record| matches!(record, super::WalRecord::Commit { .. }))
                .count(),
            3
        );
        drop(first);
        drop(second);
        drop(third);
        storage.close().unwrap();
        cleanup(&root);
    }

    #[test]
    fn manifest_reservation_failure_leaves_no_batch_and_commit_retry_is_canonical() {
        let root = root("manifest-reservation-retry");
        cleanup(&root);
        let mut storage = LsmStorage::create(&root, table(), ColumnId(1)).expect("create");
        let mut transaction = storage.begin_transaction().expect("transaction");
        storage
            .insert_in(&mut transaction, &row(1, "retry"))
            .expect("insert");
        let old_reservation = {
            let mut shared = transaction.shared.borrow_mut();
            let end = shared.manifest.commit_reservation_end;
            shared.next_commit_seq = end;
            end
        };
        super::MANIFEST_PUBLISH_FAILURE.with(|failure| {
            failure.set(Some(super::ManifestPublishPoint::BeforeInstall));
        });
        assert!(transaction.commit().is_err());
        assert_eq!(transaction.state(), TransactionState::Active);
        assert_eq!(
            transaction.shared.borrow().manifest.commit_reservation_end,
            old_reservation
        );
        let manifest = super::read_manifest(&root).expect("manifest");
        let records = super::LsmWal::inspect(&root, manifest.storage_id, manifest.wal_generation)
            .expect("inspect WAL");
        assert!(records.is_empty(), "failed reservation wrote a WAL batch");

        transaction.commit().expect("retry commit");
        let manifest = super::read_manifest(&root).expect("published manifest");
        let records = super::LsmWal::inspect(&root, manifest.storage_id, manifest.wal_generation)
            .expect("inspect retried WAL");
        assert_eq!(
            records
                .iter()
                .filter(|record| matches!(record, super::WalRecord::MutationBatch { .. }))
                .count(),
            1
        );
        drop(transaction);
        storage.close().expect("close");
        cleanup(&root);
    }

    #[test]
    fn uncertain_manifest_install_requires_reopen_and_preserves_rows() {
        let root = root("manifest-uncertain");
        cleanup(&root);
        let mut storage = LsmStorage::create(&root, table(), ColumnId(1)).expect("create");
        storage.insert(&row(1, "durable")).expect("insert");
        super::MANIFEST_PUBLISH_FAILURE.with(|failure| {
            failure.set(Some(super::ManifestPublishPoint::AfterInstall));
        });
        assert!(storage.flush().is_err());
        assert!(storage.shared.borrow().runtime.recovery_required.get());
        assert!(matches!(
            storage.insert(&row(2, "blocked")),
            Err(StorageError::Transaction(
                crate::TransactionError::RecoveryRequired
            ))
        ));
        drop(storage);

        let mut reopened = LsmStorage::open(&root, table()).expect("reopen uncertain publish");
        let view = reopened.read_view().expect("view");
        assert_eq!(
            reopened
                .scan_columns_with_view(&[ColumnId(1)], &view)
                .expect("scan")
                .len(),
            1
        );
        drop(view);
        reopened.close().expect("close reopened");
        cleanup(&root);
    }

    #[test]
    fn recovery_inspection_does_not_truncate_a_crash_tail() {
        use std::io::Write as _;

        let root = root("recovery-inspection-read-only");
        cleanup(&root);
        let mut storage = LsmStorage::create(&root, table(), ColumnId(1)).expect("create");
        storage.insert(&row(1, "wal")).expect("insert");
        drop(storage);
        let manifest = super::read_manifest(&root).expect("manifest");
        let wal_path = super::wal_path(&root, manifest.wal_generation);
        let valid_length = std::fs::metadata(&wal_path).expect("WAL metadata").len();
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&wal_path)
            .expect("open WAL tail");
        file.write_all(&[1, 2, 3]).expect("append crash tail");
        file.sync_all().expect("sync crash tail");
        drop(file);
        let tailed_length = valid_length + 3;

        LsmStorage::inspect_recovery(&root, &table()).expect("inspect recovery");
        assert_eq!(
            std::fs::metadata(&wal_path)
                .expect("inspected WAL metadata")
                .len(),
            tailed_length
        );
        let reopened = LsmStorage::open(&root, table()).expect("open truncates tail");
        assert_eq!(
            std::fs::metadata(&wal_path)
                .expect("recovered WAL metadata")
                .len(),
            valid_length
        );
        drop(reopened);
        cleanup(&root);
    }

    #[test]
    fn aborted_wal_batches_still_require_schema_valid_rows() {
        let root = root("aborted-wal-row-validation");
        cleanup(&root);
        let storage = LsmStorage::create(&root, table(), ColumnId(1)).expect("create");
        drop(storage);
        let manifest = super::read_manifest(&root).expect("manifest");
        let (mut wal, records) =
            super::LsmWal::open(&root, manifest.storage_id, manifest.wal_generation)
                .expect("open WAL");
        assert!(records.is_empty());
        let mut invalid_row = super::encode_row(&row(1, "x")).expect("encode row");
        *invalid_row.last_mut().expect("text byte") = 0xff;
        wal.append(&super::WalRecord::MutationBatch {
            txn_id: TxnId(1),
            mutations: vec![super::WalMutation::Put {
                key: super::PhysicalKey {
                    clustering: super::ClusteringKey::Int64(1),
                    row_id: LsmRowId(1),
                },
                row: invalid_row,
            }],
        })
        .expect("append invalid batch");
        wal.append(&super::WalRecord::Abort { txn_id: TxnId(1) })
            .expect("append abort");
        wal.sync().expect("sync invalid batch");
        drop(wal);

        assert!(matches!(
            LsmStorage::open(&root, table()),
            Err(StorageError::Codec(crate::CodecError::TextNotUtf8))
        ));
        cleanup(&root);
    }

    #[test]
    fn manifest_wal_and_sstable_corruption_are_hard_errors_but_wal_tail_is_discarded() {
        fn flip(path: &std::path::Path, offset: u64) {
            use std::io::{Read as _, Seek as _, Write as _};

            let mut file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(path)
                .expect("open corruption target");
            file.seek(std::io::SeekFrom::Start(offset)).expect("seek");
            let mut byte = [0_u8; 1];
            file.read_exact(&mut byte).expect("read byte");
            byte[0] ^= 0x5a;
            file.seek(std::io::SeekFrom::Start(offset)).expect("rewind");
            file.write_all(&byte).expect("write flipped byte");
            file.sync_all().expect("sync corruption");
        }

        let manifest_root = root("manifest-corrupt");
        cleanup(&manifest_root);
        let storage = LsmStorage::create(&manifest_root, table(), ColumnId(1)).expect("create");
        drop(storage);
        let manifest_file = super::manifest_path(&manifest_root);
        let last = std::fs::metadata(&manifest_file).expect("metadata").len() - 1;
        flip(&manifest_file, last);
        assert!(matches!(
            LsmStorage::open(&manifest_root, table()),
            Err(StorageError::Lsm(super::LsmError::ManifestChecksum { .. }))
        ));
        cleanup(&manifest_root);

        let wal_root = root("wal-corrupt");
        cleanup(&wal_root);
        let mut storage = LsmStorage::create(&wal_root, table(), ColumnId(1)).expect("create");
        storage.insert(&row(1, "wal")).expect("insert");
        drop(storage);
        let wal_file = super::wal_path(&wal_root, 1);
        let wal_length = std::fs::metadata(&wal_file).expect("WAL metadata").len();
        std::fs::OpenOptions::new()
            .append(true)
            .open(&wal_file)
            .and_then(|mut file| std::io::Write::write_all(&mut file, &[1, 2, 3]))
            .expect("append crash tail");
        let storage = LsmStorage::open(&wal_root, table()).expect("discard crash tail");
        drop(storage);
        assert_eq!(
            std::fs::metadata(&wal_file)
                .expect("truncated metadata")
                .len(),
            wal_length
        );
        flip(&wal_file, wal_length - 1);
        assert!(matches!(
            LsmStorage::open(&wal_root, table()),
            Err(StorageError::Lsm(super::LsmError::WalChecksum { .. }))
        ));
        cleanup(&wal_root);

        let sst_root = root("sst-corrupt");
        cleanup(&sst_root);
        let mut storage = LsmStorage::create(&sst_root, table(), ColumnId(1)).expect("create");
        storage.insert(&row(1, "sst")).expect("insert");
        storage.flush().expect("flush");
        let inspection = storage.inspection();
        assert_eq!(inspection.sstable_count, 1);
        drop(storage);
        let manifest = super::read_manifest(&sst_root).expect("manifest");
        let reference = manifest.sstables.first().expect("SSTable reference");
        let sstable = super::sstable_path(&sst_root, reference.id, reference.level);
        flip(
            &sstable,
            (super::SST_HEADER_SIZE + super::SST_BLOCK_HEADER_SIZE) as u64,
        );
        assert!(matches!(
            LsmStorage::open(&sst_root, table()),
            Err(StorageError::Lsm(super::LsmError::SstableChecksum { .. }))
        ));
        cleanup(&sst_root);

        for (case, offset_from_end) in [("bloom-corrupt", None), ("footer-corrupt", Some(1))] {
            let root = root(case);
            cleanup(&root);
            let mut storage = LsmStorage::create(&root, table(), ColumnId(1)).expect("create");
            storage.insert(&row(1, "sst")).expect("insert");
            storage.flush().expect("flush");
            drop(storage);
            let manifest = super::read_manifest(&root).expect("manifest");
            let reference = manifest.sstables.first().expect("reference");
            let path = super::sstable_path(&root, reference.id, reference.level);
            let offset = offset_from_end.map_or(super::SST_HEADER_SIZE as u64, |distance| {
                std::fs::metadata(&path).expect("metadata").len() - distance
            });
            flip(&path, offset);
            assert!(LsmStorage::open(&root, table()).is_err(), "case {case}");
            cleanup(&root);
        }
    }

    #[test]
    fn self_consistent_false_negative_bloom_is_rejected() {
        use std::io::{Read as _, Seek as _, Write as _};

        let root = root("semantic-bloom-corrupt");
        cleanup(&root);
        let mut storage = LsmStorage::create(&root, table(), ColumnId(1)).expect("create");
        storage.insert(&row(1, "sst")).expect("insert");
        storage.flush().expect("flush");
        drop(storage);

        let manifest = super::read_manifest(&root).expect("manifest");
        let reference = manifest.sstables.first().expect("reference");
        let path = super::sstable_path(&root, reference.id, reference.level);
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .expect("open SSTable");
        let mut header = [0_u8; super::SST_HEADER_SIZE];
        file.read_exact(&mut header).expect("read header");
        let empty_bits = vec![0_u8; reference.bloom_bytes as usize];
        header[140..144].copy_from_slice(&crc32c::crc32c(&empty_bits).to_le_bytes());
        let header_checksum = crc32c::crc32c(&header[..156]);
        header[156..160].copy_from_slice(&header_checksum.to_le_bytes());
        file.seek(std::io::SeekFrom::Start(0)).expect("seek");
        file.write_all(&header).expect("rewrite header");
        file.write_all(&empty_bits).expect("rewrite Bloom");
        file.sync_all().expect("sync semantic corruption");
        drop(file);

        assert!(matches!(
            LsmStorage::open(&root, table()),
            Err(StorageError::Lsm(super::LsmError::InvalidSstable {
                reason: "Bloom has a false negative for a persisted entry",
                ..
            }))
        ));
        cleanup(&root);
    }

    #[test]
    fn self_consistent_file_bounds_mismatch_is_rejected() {
        use std::io::{Read as _, Seek as _, Write as _};

        let root = root("semantic-bounds-corrupt");
        cleanup(&root);
        let mut storage = LsmStorage::create(&root, table(), ColumnId(1)).expect("create");
        storage.insert(&row(1, "sst")).expect("insert");
        storage.flush().expect("flush");
        drop(storage);

        let mut manifest = super::read_manifest(&root).expect("manifest");
        let reference = manifest.sstables.first_mut().expect("reference");
        let false_bound = super::PhysicalKey {
            clustering: super::ClusteringKey::Int64(2),
            row_id: LsmRowId(1),
        };
        reference.min = false_bound;
        reference.max = false_bound;
        let path = super::sstable_path(&root, reference.id, reference.level);
        let manifest_bytes = super::encode_manifest(&manifest).expect("encode manifest");
        std::fs::write(super::manifest_path(&root), manifest_bytes).expect("rewrite manifest");

        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .expect("open SSTable");
        let mut header = [0_u8; super::SST_HEADER_SIZE];
        file.read_exact(&mut header).expect("read header");
        super::encode_physical_key(&mut header[80..100], false_bound).expect("encode min");
        super::encode_physical_key(&mut header[100..120], false_bound).expect("encode max");
        let checksum = crc32c::crc32c(&header[..156]);
        header[156..160].copy_from_slice(&checksum.to_le_bytes());
        file.seek(std::io::SeekFrom::Start(0)).expect("seek");
        file.write_all(&header).expect("rewrite header");
        file.sync_all().expect("sync semantic corruption");
        drop(file);

        assert!(matches!(
            LsmStorage::open(&root, table()),
            Err(StorageError::Lsm(super::LsmError::InvalidSstable {
                reason: "file entry count or key bounds differ from header",
                ..
            }))
        ));
        cleanup(&root);
    }

    #[test]
    fn sstable_header_rejects_block_count_above_metadata_bound() {
        use std::io::Read as _;

        let root = root("sstable-block-count-bound");
        cleanup(&root);
        let mut storage = LsmStorage::create(&root, table(), ColumnId(1)).expect("create");
        storage.insert(&row(1, "sst")).expect("insert");
        storage.flush().expect("flush");
        drop(storage);

        let manifest = super::read_manifest(&root).expect("manifest");
        let reference = manifest.sstables.first().expect("reference");
        let path = super::sstable_path(&root, reference.id, reference.level);
        let mut header = [0_u8; super::SST_HEADER_SIZE];
        std::fs::File::open(path)
            .expect("open SSTable")
            .read_exact(&mut header)
            .expect("read header");
        header[76..80].copy_from_slice(&(super::SST_MAX_BLOCKS + 1).to_le_bytes());
        let checksum = crc32c::crc32c(&header[..156]);
        header[156..160].copy_from_slice(&checksum.to_le_bytes());

        assert!(matches!(
            super::decode_sstable_header(&header, &manifest, reference),
            Err(StorageError::Lsm(super::LsmError::InvalidSstable {
                reason: "invalid block count",
                ..
            }))
        ));
        cleanup(&root);
    }

    #[test]
    fn flush_reopen_move_directory_range_seek_and_quiescent_compaction() {
        let original = root("flush");
        let moved = root("flush-moved");
        cleanup(&original);
        cleanup(&moved);
        let mut storage =
            LsmStorage::create_with_storage_id(&original, table(), ColumnId(1), StorageId(77))
                .expect("create");
        for key in -20..=20 {
            storage
                .insert(&row(key, &format!("v{key}")))
                .expect("insert");
        }
        storage.flush().expect("flush");
        storage.insert(&row(5, "duplicate")).expect("duplicate");
        storage.flush().expect("second flush");
        storage.compact().expect("compact");
        storage.close().expect("close");
        std::fs::rename(&original, &moved).expect("rename");
        let mut reopened = LsmStorage::open(&moved, table()).expect("reopen");
        assert_eq!(reopened.storage_id(), StorageId(77));
        let view = reopened.read_view().expect("view");
        let rows = reopened
            .range_lookup_columns_with_view(
                &IndexRange {
                    lower: IndexBound::Included(ScalarValue::Int64(4)),
                    upper: IndexBound::Excluded(ScalarValue::Int64(7)),
                },
                &[ColumnId(1), ColumnId(2)],
                &view,
            )
            .expect("range");
        assert_eq!(rows.len(), 4);
        assert_eq!(reopened.inspection().l1_count, 1);
        drop(view);
        reopened.close().expect("second close");
        let reopened = LsmStorage::open(&moved, table()).expect("second reopen");
        reopened.close().expect("final close");
        cleanup(&moved);
    }

    #[test]
    fn prepared_transaction_is_in_doubt_and_resolves_idempotently() {
        let root = root("prepared");
        cleanup(&root);
        let mut storage =
            LsmStorage::create_with_storage_id(&root, table(), ColumnId(1), StorageId(88))
                .expect("create");
        let mut transaction = storage.begin_transaction().expect("transaction");
        storage
            .insert_in(&mut transaction, &row(1, "prepared"))
            .expect("insert");
        let physical = transaction.id();
        transaction.prepare(DatabaseTxnId(901)).expect("prepare");
        drop(transaction);
        drop(storage);
        assert!(matches!(
            LsmStorage::open(&root, table()),
            Err(StorageError::Recovery(
                RecoveryError::PreparedTransactionRequiresResolution { .. }
            ))
        ));
        let resolution = PreparedTxnResolution {
            database_txn_id: DatabaseTxnId(901),
            physical_txn_id: physical,
            decision: PreparedDecision::Commit,
        };
        let mut reopened =
            LsmStorage::open_with_prepared_resolutions(&root, table(), &[resolution])
                .expect("resolve");
        let view = reopened.read_view().expect("view");
        assert_eq!(
            reopened
                .scan_columns_with_view(&[ColumnId(2)], &view)
                .expect("scan")
                .len(),
            1
        );
        drop(view);
        reopened.close().expect("close");
        let mut reopened = LsmStorage::open(&root, table()).expect("idempotent reopen");
        let view = reopened.read_view().expect("view");
        assert_eq!(
            reopened
                .scan_columns_with_view(&[ColumnId(2)], &view)
                .expect("scan")
                .len(),
            1
        );
        drop(view);
        reopened.close().expect("close again");
        cleanup(&root);
    }

    #[test]
    fn lsm_commit_crash_child() {
        if std::env::var_os("NETBADB_LSM_COMMIT_CRASH_CHILD").is_none() {
            return;
        }
        let root = std::env::var_os("NETBADB_LSM_CRASH_ROOT")
            .map(std::path::PathBuf::from)
            .expect("commit crash root");
        let mut storage = LsmStorage::open(&root, table()).expect("open commit fixture");
        let mut transaction = storage.begin_transaction().expect("transaction");
        storage
            .insert_in(&mut transaction, &row(42, "committed"))
            .expect("insert");
        transaction.commit().expect("commit until crash");
        panic!("commit returned without reaching configured crash point");
    }

    #[test]
    fn lsm_group_commit_crash_child() {
        let prepare_batch = std::env::var_os("NETBADB_LSM_GROUP_PREPARE_CRASH_CHILD").is_some();
        if std::env::var_os("NETBADB_LSM_GROUP_CRASH_CHILD").is_none() && !prepare_batch {
            return;
        }
        let root = std::env::var_os("NETBADB_LSM_CRASH_ROOT")
            .map(std::path::PathBuf::from)
            .expect("group crash root");
        let mut storage = LsmStorage::open(&root, table()).expect("open group fixture");
        let mut transactions = Vec::new();
        for id in 1..=3_i64 {
            let mut transaction = storage.begin_transaction().unwrap();
            storage
                .insert_in(&mut transaction, &row(id, &format!("member-{id}")))
                .unwrap();
            let database_txn_id = DatabaseTxnId(950 + id as u64);
            if prepare_batch {
                transaction.stage_group_prepare(database_txn_id).unwrap();
                super::maybe_lsm_crash(&format!("group-after-staged-member-{id}"));
            } else {
                transaction.prepare(database_txn_id).unwrap();
                transaction.park_prepared(database_txn_id).unwrap();
            }
            transactions.push((transaction, database_txn_id));
        }
        let mut batch = transactions
            .iter_mut()
            .map(|(transaction, database_txn_id)| (transaction, *database_txn_id))
            .collect::<Vec<_>>();
        if prepare_batch {
            LsmTransaction::durabilize_group_prepare_batch(&mut batch)
                .expect("durabilize group Prepare until crash");
        } else {
            LsmTransaction::commit_prepared_batch(&mut batch).expect("commit group until crash");
        }
        panic!("group commit returned without reaching configured crash point");
    }

    #[test]
    fn single_lsm_commit_crash_matrix_recovers_durable_decision_idempotently() {
        for point in [
            "before-commit-sync",
            "after-commit-sync",
            "after-memtable-apply",
        ] {
            let root = root(&format!("commit-crash-{point}"));
            cleanup(&root);
            let storage = LsmStorage::create(&root, table(), ColumnId(1)).expect("create");
            drop(storage);
            let status = std::process::Command::new(
                std::env::current_exe().expect("current storage test executable"),
            )
            .arg("--exact")
            .arg("lsm::tests::lsm_commit_crash_child")
            .arg("--nocapture")
            .env("NETBADB_LSM_COMMIT_CRASH_CHILD", "1")
            .env("NETBADB_LSM_CRASH_CHILD", "1")
            .env("NETBADB_LSM_CRASH_ROOT", &root)
            .env("NETBADB_LSM_CRASH_POINT", point)
            .status()
            .expect("run commit crash child");
            assert_eq!(status.code(), Some(86), "point {point}");
            for pass in 0..2 {
                let mut reopened = LsmStorage::open(&root, table()).expect("recover commit");
                let view = reopened.read_view().expect("view");
                let count = reopened
                    .scan_columns_with_view(&[ColumnId(1)], &view)
                    .expect("scan")
                    .len();
                if point == "before-commit-sync" {
                    assert!(count <= 1, "point {point}, pass {pass}");
                } else {
                    assert_eq!(count, 1, "point {point}, pass {pass}");
                }
                drop(view);
                reopened.close().expect("close recovered");
            }
            cleanup(&root);
        }
    }

    #[test]
    fn lsm_prepared_commit_batch_crash_matrix_recovers_every_decided_member() {
        for point in [
            "group-after-commit-append-1",
            "group-after-commit-append-2",
            "group-after-commit-append-3",
            "group-before-commit-sync",
            "group-after-commit-sync",
            "group-after-runtime-finalize-1",
            "group-after-runtime-finalize-2",
        ] {
            let root = root(&format!("group-commit-crash-{point}"));
            cleanup(&root);
            LsmStorage::create(&root, table(), ColumnId(1)).unwrap();
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg("lsm::tests::lsm_group_commit_crash_child")
                .arg("--nocapture")
                .env("NETBADB_LSM_GROUP_CRASH_CHILD", "1")
                .env("NETBADB_LSM_CRASH_CHILD", "1")
                .env("NETBADB_LSM_CRASH_ROOT", &root)
                .env("NETBADB_LSM_CRASH_POINT", point)
                .status()
                .unwrap();
            assert_eq!(status.code(), Some(86), "point {point}");
            let resolutions = LsmStorage::inspect_recovery(&root, &table())
                .unwrap()
                .prepared_transactions
                .into_iter()
                .filter(|transaction| transaction.state == PreparedTransactionState::Prepared)
                .map(|transaction| PreparedTxnResolution {
                    database_txn_id: transaction.database_txn_id,
                    physical_txn_id: transaction.physical_txn_id,
                    decision: PreparedDecision::Commit,
                })
                .collect::<Vec<_>>();
            for pass in 0..2 {
                let mut reopened = if pass == 0 {
                    LsmStorage::open_with_prepared_resolutions(&root, table(), &resolutions)
                        .unwrap()
                } else {
                    LsmStorage::open(&root, table()).unwrap()
                };
                let view = reopened.read_view().unwrap();
                let rows = reopened
                    .scan_columns_with_view(&[ColumnId(1)], &view)
                    .unwrap();
                assert_eq!(rows.len(), 3, "point {point}, pass {pass}");
                drop(view);
                reopened.close().unwrap();
            }
            cleanup(&root);
        }
    }

    #[test]
    fn lsm_staged_prepare_batch_crash_matrix_aborts_without_global_decision() {
        for point in [
            "group-after-staged-member-1",
            "group-after-staged-member-2",
            "group-after-staged-member-3",
            "group-before-prepare-sync",
            "group-after-prepare-sync",
            "group-after-prepare-state-1",
            "group-after-prepare-state-2",
        ] {
            let root = root(&format!("group-prepare-crash-{point}"));
            cleanup(&root);
            LsmStorage::create(&root, table(), ColumnId(1)).unwrap();
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg("lsm::tests::lsm_group_commit_crash_child")
                .arg("--nocapture")
                .env("NETBADB_LSM_GROUP_PREPARE_CRASH_CHILD", "1")
                .env("NETBADB_LSM_CRASH_CHILD", "1")
                .env("NETBADB_LSM_CRASH_ROOT", &root)
                .env("NETBADB_LSM_CRASH_POINT", point)
                .status()
                .unwrap();
            assert_eq!(status.code(), Some(86), "point {point}");
            for pass in 0..2 {
                let resolutions = LsmStorage::inspect_recovery(&root, &table())
                    .unwrap()
                    .prepared_transactions
                    .into_iter()
                    .filter(|transaction| transaction.state != PreparedTransactionState::Committed)
                    .map(|transaction| PreparedTxnResolution {
                        database_txn_id: transaction.database_txn_id,
                        physical_txn_id: transaction.physical_txn_id,
                        decision: PreparedDecision::Abort,
                    })
                    .collect::<Vec<_>>();
                let mut reopened = if resolutions.is_empty() {
                    LsmStorage::open(&root, table()).unwrap()
                } else {
                    LsmStorage::open_with_prepared_resolutions(&root, table(), &resolutions)
                        .unwrap()
                };
                let view = reopened.read_view().unwrap();
                assert!(
                    reopened
                        .scan_columns_with_view(&[ColumnId(1)], &view)
                        .unwrap()
                        .is_empty(),
                    "point {point}, pass {pass}"
                );
                drop(view);
                reopened.close().unwrap();
            }
            cleanup(&root);
        }
    }

    #[test]
    fn lsm_maintenance_crash_child() {
        if std::env::var_os("NETBADB_LSM_CRASH_CHILD").is_none() {
            return;
        }
        let root = std::env::var_os("NETBADB_LSM_CRASH_ROOT")
            .map(std::path::PathBuf::from)
            .expect("crash root");
        let operation = std::env::var("NETBADB_LSM_CRASH_OPERATION").expect("operation");
        let storage = LsmStorage::open(&root, table()).expect("open crash fixture");
        match operation.as_str() {
            "flush" => storage.flush().expect("flush until crash"),
            "compact" => storage.compact().expect("compact until crash"),
            _ => panic!("unknown crash operation"),
        }
        panic!("maintenance returned without reaching configured crash point");
    }

    fn run_maintenance_crash(
        root: &std::path::Path,
        operation: &str,
        point: &str,
        expected_rows: usize,
    ) {
        let mut command = std::process::Command::new(
            std::env::current_exe().expect("current storage test executable"),
        );
        command
            .arg("--exact")
            .arg("lsm::tests::lsm_maintenance_crash_child")
            .arg("--nocapture")
            .env("NETBADB_LSM_CRASH_CHILD", "1")
            .env("NETBADB_LSM_CRASH_ROOT", root)
            .env("NETBADB_LSM_CRASH_OPERATION", operation)
            .env("NETBADB_LSM_CRASH_POINT", point);
        let status = command.status().expect("run crash child");
        assert_eq!(
            status.code(),
            Some(86),
            "operation {operation}, point {point}"
        );
        for pass in 0..2 {
            let mut reopened = LsmStorage::open(root, table()).expect("recover maintenance");
            let view = reopened.read_view().expect("view");
            assert_eq!(
                reopened
                    .scan_columns_with_view(&[ColumnId(1)], &view)
                    .expect("scan")
                    .len(),
                expected_rows,
                "operation {operation}, point {point}, pass {pass}"
            );
            drop(view);
            reopened.close().expect("close recovered");
        }
    }

    #[test]
    fn flush_publish_crash_matrix_preserves_committed_rows() {
        for point in [
            "during-sst-write",
            "after-sst-sync",
            "during-new-wal-creation",
            "after-new-wal-sync",
            "during-manifest-write",
            "after-manifest-write",
            "after-manifest-sync",
            "before-old-wal-removal",
            "after-old-wal-removal",
        ] {
            let root = root(&format!("flush-crash-{point}"));
            cleanup(&root);
            let mut storage = LsmStorage::create(&root, table(), ColumnId(1)).expect("create");
            for key in 0..10 {
                storage.insert(&row(key, "value")).expect("insert");
            }
            drop(storage);
            run_maintenance_crash(&root, "flush", point, 10);
            cleanup(&root);
        }
    }

    #[test]
    fn compaction_publish_crash_matrix_preserves_committed_rows() {
        for point in [
            "during-sst-write",
            "after-sst-sync",
            "during-manifest-write",
            "after-manifest-write",
            "after-manifest-sync",
            "while-deleting-old-sst",
        ] {
            let root = root(&format!("compact-crash-{point}"));
            cleanup(&root);
            let mut storage = LsmStorage::create(&root, table(), ColumnId(1)).expect("create");
            for key in 0..5 {
                storage.insert(&row(key, "a")).expect("insert");
            }
            storage.flush().expect("first flush");
            for key in 5..10 {
                storage.insert(&row(key, "b")).expect("insert");
            }
            storage.flush().expect("second flush");
            drop(storage);
            run_maintenance_crash(&root, "compact", point, 10);
            cleanup(&root);
        }
    }

    #[test]
    fn bloom_filter_is_stable_has_no_false_negatives_and_includes_tombstones() {
        let make = |key, row_id, version, value| super::VersionedEntry {
            key: super::PhysicalKey {
                clustering: super::ClusteringKey::Int64(key),
                row_id: LsmRowId(row_id),
            },
            version: netbadb_types::LsmCommitSeq(version),
            value,
        };
        let entries = vec![
            make(-1, 1, 1, super::EntryValue::Tombstone),
            make(0, 2, 1, super::EntryValue::Put(vec![1])),
            make(42, 3, 1, super::EntryValue::Put(vec![2])),
            make(42, 3, 2, super::EntryValue::Tombstone),
        ];
        let bloom = super::BloomFilter::build(&entries).expect("Bloom");
        assert_eq!(bloom.algorithm, super::BLOOM_ALGORITHM_VERSION);
        assert_eq!(bloom.bit_count, 64);
        assert_eq!(bloom.hash_count, 7);
        assert_eq!(bloom.bits, [0x71, 0x02, 0x04, 0x0c, 0xd0, 0xdf, 0x81, 0x00]);
        for key in [-1, 0, 42] {
            assert!(
                bloom.might_contain(super::ClusteringKey::Int64(key)),
                "inserted key {key} must never be negative"
            );
        }
        assert!(!super::BloomFilter::empty().might_contain(super::ClusteringKey::Int64(0)));

        let uint_entries = [
            super::VersionedEntry {
                key: super::PhysicalKey {
                    clustering: super::ClusteringKey::UInt64(u64::MIN),
                    row_id: LsmRowId(1),
                },
                version: netbadb_types::LsmCommitSeq(1),
                value: super::EntryValue::Tombstone,
            },
            super::VersionedEntry {
                key: super::PhysicalKey {
                    clustering: super::ClusteringKey::UInt64(u64::MAX),
                    row_id: LsmRowId(2),
                },
                version: netbadb_types::LsmCommitSeq(1),
                value: super::EntryValue::Tombstone,
            },
        ];
        let uint_bloom = super::BloomFilter::build(&uint_entries).expect("UInt Bloom");
        assert_eq!(
            uint_bloom.bits,
            [0x10, 0x0a, 0x90, 0xe0, 0x20, 0x01, 0x0a, 0x41]
        );
        assert!(uint_bloom.might_contain(super::ClusteringKey::UInt64(u64::MIN)));
        assert!(uint_bloom.might_contain(super::ClusteringKey::UInt64(u64::MAX)));
    }

    #[test]
    fn point_miss_uses_bloom_without_reading_a_data_block() {
        let root = root("bloom-negative-read");
        cleanup(&root);
        let mut storage = LsmStorage::create(&root, table(), ColumnId(1)).expect("create");
        storage.insert(&row(0, "low")).expect("low");
        storage.insert(&row(100, "high")).expect("high");
        storage.flush().expect("flush");
        let absent = {
            let shared = storage.shared.borrow();
            (1..100)
                .find(|key| {
                    !shared.sstables[0]
                        .bloom
                        .might_contain(super::ClusteringKey::Int64(*key))
                })
                .expect("the small Bloom must have a negative in the file range")
        };
        let before = storage.inspection().read_amplification;
        let view = storage.read_view().expect("view");
        assert!(
            storage
                .point_lookup_columns_with_view(&ScalarValue::Int64(absent), &[ColumnId(1)], &view,)
                .expect("point miss")
                .is_empty()
        );
        drop(view);
        let after = storage.inspection().read_amplification;
        assert_eq!(after.bloom_checks - before.bloom_checks, 1);
        assert_eq!(after.bloom_negatives - before.bloom_negatives, 1);
        assert_eq!(after.data_blocks_read - before.data_blocks_read, 0);
        storage.close().expect("close");
        cleanup(&root);
    }

    #[test]
    fn point_hit_seeks_directly_to_the_sparse_index_block() {
        let root = root("sparse-index-point-hit");
        cleanup(&root);
        let mut storage = LsmStorage::create(&root, table(), ColumnId(1)).expect("create");
        let payload = "x".repeat(20_000);
        for key in 0..8 {
            storage.insert(&row(key, &payload)).expect("insert");
        }
        storage.flush().expect("flush");
        assert!(storage.shared.borrow().sstables[0].blocks.len() > 1);

        let before = storage.inspection().read_amplification;
        let view = storage.read_view().expect("view");
        assert_eq!(
            storage
                .point_lookup_columns_with_view(&ScalarValue::Int64(7), &[ColumnId(1)], &view)
                .expect("point hit")
                .len(),
            1
        );
        drop(view);
        let after = storage.inspection().read_amplification;
        assert_eq!(after.bloom_checks - before.bloom_checks, 1);
        assert_eq!(after.bloom_positives - before.bloom_positives, 1);
        assert_eq!(after.data_blocks_read - before.data_blocks_read, 1);
        storage.close().expect("close");
        cleanup(&root);
    }

    #[test]
    fn leveled_compaction_splits_outputs_and_reaches_deeper_levels() {
        let root = root("multi-level-split");
        cleanup(&root);
        let mut storage = LsmStorage::create(&root, table(), ColumnId(1)).expect("create");
        let payload = "x".repeat(40_000);
        for key in 0..40 {
            storage.insert(&row(key, &payload)).expect("insert");
            if key == 19 || key == 39 {
                storage.flush().expect("flush run");
            }
        }
        storage.compact().expect("drive leveled compaction");
        let inspection = storage.inspection();
        assert!(inspection.level_count >= 2);
        assert!(inspection.levels.iter().any(|level| level.level >= 2));
        assert!(inspection.sstable_count > 1, "outputs must be split");
        let shared = storage.shared.borrow();
        super::validate_manifest_sstable_layout(
            &shared.manifest.sstables,
            shared.manifest.key_type,
        )
        .expect("level invariants");
        assert!(
            shared
                .manifest
                .sstables
                .iter()
                .all(|reference| reference.file_bytes > reference.bloom_bytes)
        );
        drop(shared);
        let view = storage.read_view().expect("view");
        assert_eq!(
            storage
                .scan_columns_with_view(&[ColumnId(1)], &view)
                .expect("scan")
                .len(),
            40
        );
        drop(view);
        let writes = storage.inspection().write_amplification;
        assert!(writes.compaction_input_bytes > 0);
        assert!(writes.compaction_output_bytes > 0);
        storage.close().expect("close");
        let reopened = LsmStorage::open(&root, table()).expect("reopen");
        assert!(
            reopened
                .inspection()
                .levels
                .iter()
                .any(|level| level.level >= 2)
        );
        reopened.close().expect("close reopened");
        cleanup(&root);
    }

    #[test]
    fn regular_compaction_preserves_history_and_full_compaction_gcs_quiescently() {
        let root = root("full-gc");
        cleanup(&root);
        let mut storage = LsmStorage::create(&root, table(), ColumnId(1)).expect("create");
        let first = storage.insert(&row(1, "v1")).expect("v1");
        storage.flush().expect("flush v1");
        let current = storage
            .read_view()
            .and_then(|view| storage.refresh_handle(first.row_id, &view))
            .expect("current");
        let updated = storage.update(current, &row(2, "v2")).expect("move");
        storage.flush().expect("flush v2");
        storage.compact().expect("regular compact");
        let preserved = storage.inspection().sstable_entry_count;
        assert!(
            preserved >= 3,
            "regular compaction must preserve MVCC history"
        );
        let current = storage
            .read_view()
            .and_then(|view| storage.refresh_handle(updated.row_id, &view))
            .expect("updated current");
        storage.delete(current).expect("delete");
        storage.flush().expect("flush delete");

        let old_view = storage.read_view().expect("live view");
        assert!(matches!(
            storage.compact_full(),
            Err(StorageError::Lsm(super::LsmError::Busy(
                "outstanding read views"
            )))
        ));
        drop(old_view);
        storage.compact_full().expect("full GC");
        assert_eq!(storage.inspection().sstable_entry_count, 0);
        let view = storage.read_view().expect("view");
        assert!(
            storage
                .scan_columns_with_view(&[ColumnId(1)], &view)
                .expect("empty scan")
                .is_empty()
        );
        drop(view);
        storage.close().expect("close");
        cleanup(&root);
    }

    #[test]
    fn flush_and_compact_one_bounds_preserve_every_historical_horizon_after_reopen() {
        fn rows_at(
            storage: &mut LsmStorage,
            horizon: netbadb_types::LsmCommitSeq,
        ) -> Vec<Vec<ScalarValue>> {
            let view = storage.read_view_at(horizon).expect("historical view");
            storage
                .scan_columns_with_view(&[ColumnId(1), ColumnId(2)], &view)
                .expect("historical scan")
                .into_iter()
                .map(|(_, values)| values)
                .collect()
        }

        fn flush_with_bound(storage: &LsmStorage) {
            let plan = storage.maintenance_inspection().expect("flush plan");
            let bound = plan.flush_conservative_bound.expect("flush hard bound");
            let before = storage.inspection().write_amplification;
            storage.flush().expect("bounded flush");
            let after = storage.inspection().write_amplification;
            assert!(after.flush_input_bytes - before.flush_input_bytes <= bound.read_bytes);
            assert!(after.flush_output_bytes - before.flush_output_bytes <= bound.write_bytes);
        }

        let root = root("maintenance-history-bound");
        cleanup(&root);
        let mut storage = LsmStorage::create(&root, table(), ColumnId(1)).expect("create");

        let first = storage.insert(&row(1, "v1")).expect("insert v1");
        let c1 = storage.current_commit_seq();
        flush_with_bound(&storage);

        let current = storage
            .read_view()
            .and_then(|view| storage.refresh_handle(first.row_id, &view))
            .expect("current v1");
        let updated = storage.update(current, &row(1, "v2")).expect("update v2");
        let c2 = storage.current_commit_seq();
        flush_with_bound(&storage);
        let current = storage
            .read_view()
            .and_then(|view| storage.refresh_handle(updated.row_id, &view))
            .expect("current v2");
        storage.delete(current).expect("delete v2");
        let c3 = storage.current_commit_seq();
        flush_with_bound(&storage);

        let expected = [
            rows_at(&mut storage, c1),
            rows_at(&mut storage, c2),
            rows_at(&mut storage, c3),
        ];
        assert_eq!(expected[0], vec![row(1, "v1")]);
        assert_eq!(expected[1], vec![row(1, "v2")]);
        assert!(expected[2].is_empty());

        let compaction = storage.maintenance_inspection().expect("compaction plan");
        let plan = compaction.next_compaction.expect("exact compact_one plan");
        assert_eq!(compaction.memtable_entry_count, 0);
        let before_compaction = storage.inspection().write_amplification;
        assert!(storage.compact_one().expect("compact one"));
        let after_compaction = storage.inspection().write_amplification;
        assert!(
            after_compaction.compaction_input_bytes - before_compaction.compaction_input_bytes
                <= plan.conservative_bound.read_bytes
        );
        assert!(
            after_compaction.compaction_output_bytes - before_compaction.compaction_output_bytes
                <= plan.conservative_bound.write_bytes
        );
        assert_eq!(rows_at(&mut storage, c1), expected[0]);
        assert_eq!(rows_at(&mut storage, c2), expected[1]);
        assert_eq!(rows_at(&mut storage, c3), expected[2]);

        storage.close().expect("close");
        let mut reopened = LsmStorage::open(&root, table()).expect("reopen");
        assert_eq!(rows_at(&mut reopened, c1), expected[0]);
        assert_eq!(rows_at(&mut reopened, c2), expected[1]);
        assert_eq!(rows_at(&mut reopened, c3), expected[2]);
        reopened.close().expect("close reopened");
        cleanup(&root);
    }

    #[test]
    fn maintenance_inspection_exposes_the_same_recovery_required_guard_as_the_writer() {
        let root = root("maintenance-recovery-blocker");
        cleanup(&root);
        let storage = LsmStorage::create(&root, table(), ColumnId(1)).expect("create");
        storage.shared.borrow().runtime.recovery_required.set(true);
        assert_eq!(
            storage
                .maintenance_inspection()
                .expect("inspect blocker")
                .safety_blocker,
            Some(super::LsmMaintenanceSafetyBlocker::RecoveryRequired)
        );
        assert!(matches!(
            storage.flush(),
            Err(StorageError::Checkpoint(
                super::CheckpointError::RecoveryRequired
            ))
        ));
        drop(storage);
        cleanup(&root);
    }

    #[test]
    fn manifest_preinstall_faults_keep_old_authority_and_retry() {
        for point in [
            super::ManifestPublishPoint::CandidateWrite,
            super::ManifestPublishPoint::CandidateSync,
            super::ManifestPublishPoint::BeforeInstall,
        ] {
            let root = root(&format!("manifest-fault-{point:?}"));
            cleanup(&root);
            let mut storage = LsmStorage::create(&root, table(), ColumnId(1)).expect("create");
            storage.insert(&row(1, "value")).expect("insert");
            let before = super::read_manifest(&root).expect("old manifest");
            super::MANIFEST_PUBLISH_FAILURE.with(|failure| failure.set(Some(point)));
            assert!(storage.flush().is_err());
            assert_eq!(
                storage.shared.borrow().manifest.generation,
                before.generation
            );
            assert_eq!(
                super::read_manifest(&root)
                    .expect("disk authority")
                    .generation,
                before.generation
            );
            storage.flush().expect("retry");
            assert_eq!(storage.inspection().sstable_count, 1);
            storage.close().expect("close");
            cleanup(&root);
        }
    }

    #[test]
    fn obsolete_delete_failure_keeps_published_manifest_and_reopen_ignores_orphan() {
        let root = root("obsolete-delete-failure");
        cleanup(&root);
        let mut storage = LsmStorage::create(&root, table(), ColumnId(1)).expect("create");
        for key in 0..2 {
            storage.insert(&row(key, "first")).expect("insert");
            storage.flush().expect("flush");
        }
        let old_generation = storage.shared.borrow().manifest.generation;
        super::OBSOLETE_DELETE_FAILURE.with(|failure| failure.set(true));
        assert!(storage.compact().is_err());
        assert!(storage.shared.borrow().manifest.generation > old_generation);
        drop(storage);
        let mut reopened = LsmStorage::open(&root, table()).expect("reopen published manifest");
        let view = reopened.read_view().expect("view");
        assert_eq!(
            reopened
                .scan_columns_with_view(&[ColumnId(1)], &view)
                .expect("scan")
                .len(),
            2
        );
        drop(view);
        reopened.close().expect("close");
        cleanup(&root);
    }

    #[test]
    fn compaction_picker_computes_source_target_overlap_closure() {
        let root = root("overlap-closure");
        cleanup(&root);
        let storage = LsmStorage::create(&root, table(), ColumnId(1)).expect("create");
        let make = |id, level, min, max, bytes| super::Sstable {
            reference: super::SstableRef {
                id,
                level,
                entry_count: 1,
                file_bytes: bytes,
                bloom_bytes: 8,
                min: super::PhysicalKey {
                    clustering: super::ClusteringKey::Int64(min),
                    row_id: LsmRowId(1),
                },
                max: super::PhysicalKey {
                    clustering: super::ClusteringKey::Int64(max),
                    row_id: LsmRowId(1),
                },
            },
            path: root.join(format!("dummy-{id}")),
            blocks: Vec::new(),
            bloom: super::BloomFilter::empty(),
        };
        {
            let mut shared = storage.shared.borrow_mut();
            shared.sstables = vec![
                make(1, 1, 0, 10, super::BASE_LEVEL_BYTES + 1),
                make(2, 1, 20, 30, 1),
                make(3, 2, 5, 25, 1),
            ];
            let plan = super::pick_compaction(&shared)
                .expect("pick")
                .expect("overflow plan");
            assert_eq!(plan.output_level, 2);
            assert_eq!(plan.input_ids, std::collections::BTreeSet::from([1, 2, 3]));
        }
        drop(storage);
        cleanup(&root);
    }

    #[test]
    fn l1_to_l2_compaction_crash_matrix_preserves_rows() {
        for point in [
            "during-sst-write",
            "after-sst-sync",
            "during-manifest-write",
            "after-manifest-write",
            "after-manifest-sync",
            "while-deleting-old-sst",
        ] {
            let root = root(&format!("l1-l2-crash-{point}"));
            cleanup(&root);
            let mut storage = LsmStorage::create(&root, table(), ColumnId(1)).expect("create");
            let payload = "x".repeat(40_000);
            for key in 0..20 {
                storage.insert(&row(key, &payload)).expect("insert");
                if key == 9 || key == 19 {
                    storage.flush().expect("flush");
                }
            }
            {
                let mut shared = storage.shared.borrow_mut();
                let l0_plan = super::pick_compaction(&shared)
                    .expect("pick L0")
                    .expect("L0 plan");
                assert_eq!(l0_plan.output_level, 1);
                super::execute_compaction(&mut shared, &l0_plan, false).expect("build L1");
                assert!(
                    super::pick_compaction(&shared)
                        .expect("pick L1")
                        .is_some_and(|plan| plan.output_level == 2)
                );
            }
            drop(storage);
            run_maintenance_crash(&root, "compact", point, 20);
            cleanup(&root);
        }
    }

    #[test]
    fn tombstone_bloom_prevents_resurrection_from_a_deeper_level() {
        let root = root("tombstone-bloom");
        cleanup(&root);
        let mut storage = LsmStorage::create(&root, table(), ColumnId(1)).expect("create");
        let target = storage.insert(&row(10, "old")).expect("target");
        storage.insert(&row(20, "other")).expect("other");
        storage.flush().expect("first L0");
        storage.insert(&row(30, "trigger")).expect("trigger");
        storage.flush().expect("second L0");
        storage.compact().expect("put into L1");
        let target = storage
            .read_view()
            .and_then(|view| storage.refresh_handle(target.row_id, &view))
            .expect("refresh");
        storage.delete(target).expect("delete");
        storage.flush().expect("tombstone L0");
        let before = storage.inspection().read_amplification;
        let view = storage.read_view().expect("view");
        assert!(
            storage
                .point_lookup_columns_with_view(&ScalarValue::Int64(10), &[ColumnId(1)], &view)
                .expect("point")
                .is_empty()
        );
        drop(view);
        let after = storage.inspection().read_amplification;
        assert!(after.bloom_positives > before.bloom_positives);
        storage.close().expect("close");
        cleanup(&root);
    }

    #[test]
    fn oversized_duplicate_clustering_group_is_streamed_without_splitting() {
        let root = root("oversized-duplicate-group");
        cleanup(&root);
        let mut storage = LsmStorage::create(&root, table(), ColumnId(1)).expect("create");
        let payload = "x".repeat(40_000);
        for index in 0..10 {
            storage.insert(&row(7, &payload)).expect("duplicate");
            if index == 4 || index == 9 {
                storage.flush().expect("flush duplicate run");
            }
        }
        storage.compact().expect("compact duplicate group");
        let inspection = storage.inspection();
        assert_eq!(inspection.sstable_count, 1);
        assert!(inspection.total_sstable_bytes > super::SST_TARGET_FILE_BYTES);
        let view = storage.read_view().expect("view");
        assert_eq!(
            storage
                .point_lookup_columns_with_view(&ScalarValue::Int64(7), &[ColumnId(1)], &view,)
                .expect("point")
                .len(),
            10
        );
        drop(view);
        storage.close().expect("close");
        cleanup(&root);
    }
}
