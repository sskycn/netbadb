//! Synchronous, single-writer LSM table storage.
//!
//! The implementation deliberately has no page manager dependency. Commits
//! are durable mutation batches in an LSM-specific WAL, while flush publishes
//! immutable SSTables through a checksummed manifest generation.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::rc::Rc;

use netbadb_index::{IndexBound, IndexRange, IndexStatistics, TableStatistics};
use netbadb_schema::{SchemaFingerprint, TableDef};
use netbadb_types::{
    ColumnId, DatabaseTxnId, LsmCommitSeq, LsmRowId, Lsn, PhysicalType, ScalarValue, StorageId,
    TableId, TxnId,
};

use crate::row_codec::{decode_row, decode_row_columns, encode_row, validate_row};
use crate::{
    CheckpointError, IsolationLevel, PreparedDecision, PreparedTransaction,
    PreparedTransactionState, PreparedTxnResolution, PresenceCountSummary, RecoveryError,
    StorageError, TransactionError, TransactionState,
};

pub const LSM_MANIFEST_FORMAT_VERSION: u16 = 1;
pub const LSM_WAL_FORMAT_VERSION: u16 = 1;
pub const LSM_SSTABLE_FORMAT_VERSION: u16 = 1;
pub const LSM_MAX_PENDING_TRANSACTION_BYTES: u64 = 16 * 1024 * 1024;
pub const LSM_MAX_PENDING_MUTATIONS: u64 = 65_536;
pub const DEFAULT_LSM_MEMTABLE_FLUSH_BYTES: u64 = 4 * 1024 * 1024;

const MANIFEST_MAGIC: &[u8; 4] = b"NBLM";
const WAL_MAGIC: &[u8; 4] = b"NBLW";
const WAL_RECORD_MAGIC: &[u8; 4] = b"NBLR";
const SST_MAGIC: &[u8; 4] = b"NBLS";
const SST_BLOCK_MAGIC: &[u8; 4] = b"NBLB";
const MANIFEST_NAME: &str = "MANIFEST";
const MANIFEST_NEXT_NAME: &str = "MANIFEST.next";
const SST_DIR_NAME: &str = "sst";
const MANIFEST_FIXED_SIZE: usize = 176;
const MANIFEST_ENTRY_SIZE: usize = 60;
const MAX_SSTABLES: usize = 4_096;
const WAL_HEADER_SIZE: usize = 32;
const WAL_RECORD_HEADER_SIZE: usize = 20;
const WAL_MAX_RECORD_BYTES: usize = 32 * 1024 * 1024;
const SST_HEADER_SIZE: usize = 144;
const SST_BLOCK_HEADER_SIZE: usize = 52;
const SST_TARGET_BLOCK_BYTES: usize = 32 * 1024;
const SST_MAX_BLOCK_BYTES: usize = 64 * 1024;
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
    BeforeInstall,
    AfterInstall,
}

#[derive(Debug)]
struct Runtime {
    writer: Cell<Option<TxnId>>,
    recovery_required: Cell<bool>,
    outstanding_transactions: Cell<u64>,
    outstanding_read_views: Cell<u64>,
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
    flush_threshold: u64,
    runtime: Rc<Runtime>,
    table_statistics: Option<TableStatistics>,
    access_statistics: Option<IndexStatistics>,
}

#[derive(Debug)]
struct LsmWal {
    file: File,
    path: PathBuf,
    storage_id: StorageId,
    end: u64,
    #[cfg(test)]
    fail_next_sync: bool,
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
    shared: Rc<RefCell<LsmShared>>,
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
    pub memtable_entry_count: u64,
    pub sstable_entry_count: u64,
    pub analyzed_live_row_count: Option<u64>,
    pub analyzed_min_clustering: Option<ScalarValue>,
    pub analyzed_max_clustering: Option<ScalarValue>,
}

#[derive(Debug)]
struct VisibleRow {
    key: PhysicalKey,
    observed: LsmObservedVersion,
    row: Vec<u8>,
}

impl LsmStorage {
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
            });
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
                    flush_threshold: DEFAULT_LSM_MEMTABLE_FLUSH_BYTES,
                    runtime,
                    table_statistics: None,
                    access_statistics: None,
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
        for reference in &manifest.sstables {
            sstables.push(open_sstable(&root, &manifest, reference, &table)?);
        }
        let (mut wal, records) = LsmWal::open(&root, manifest.storage_id, manifest.wal_generation)?;
        let recovered = analyze_wal(&records)?;
        validate_recovered_transactions(&recovered, &manifest, &table)?;
        let prepared = classify_prepared(&recovered);
        validate_resolutions(&prepared, resolutions)?;
        let mut memtable = BTreeMap::new();
        let mut max_commit = 0_u64;
        let mut recovery_commit = allocate_recovery_commit(&manifest, &recovered)?;
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
        });
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
                flush_threshold: DEFAULT_LSM_MEMTABLE_FLUSH_BYTES,
                runtime,
                table_statistics: manifest.table_statistics,
                access_statistics: manifest.access_statistics,
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
            prepared_transactions: classify_prepared(&recovered),
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

    pub fn read_view(&self) -> Result<LsmReadView, StorageError> {
        let shared = self.shared.borrow();
        new_read_view(&shared, None, BTreeMap::new(), shared.maximum_commit_seq())
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
                    ensure_commit_record(&mut shared, self.id, expected, self.last_lsn)?;
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
            #[cfg(test)]
            maybe_lsm_crash("after-commit-sync");
            let batch = self
                .durable_batch
                .as_ref()
                .ok_or(LsmError::InvalidWal("pending commit batch is missing"))?;
            apply_mutations(&mut shared.memtable, batch, commit_seq)?;
            shared.memtable_bytes = estimate_memtable_bytes(&shared.memtable)?;
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
        self.state = TransactionState::Prepared;
        Ok(())
    }

    pub fn commit_prepared(&mut self, database_txn_id: DatabaseTxnId) -> Result<(), StorageError> {
        self.ensure_recovery_not_required()?;
        self.validate_database_txn(database_txn_id)?;
        let commit_seq = match self.state {
            TransactionState::Prepared => {
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
                    ensure_commit_record(&mut shared, self.id, expected, self.last_lsn)?;
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
            let batch = self
                .durable_batch
                .as_ref()
                .ok_or(LsmError::InvalidWal("prepared mutation batch is missing"))?;
            apply_mutations(&mut shared.memtable, batch, commit_seq)?;
            shared.memtable_bytes = estimate_memtable_bytes(&shared.memtable)?;
        }
        self.finish_terminal(TransactionState::Committed);
        Ok(())
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
            let mut shared = self.shared.borrow_mut();
            self.state = TransactionState::RollbackPending;
            self.last_lsn = shared.wal.append(&WalRecord::Abort { txn_id: self.id })?;
        } else {
            let mut shared = self.shared.borrow_mut();
            self.last_lsn = ensure_abort_record(&mut shared, self.id, self.last_lsn)?;
        }
        self.shared.borrow_mut().wal.sync()?;
        self.finish_terminal(TransactionState::RolledBack);
        Ok(())
    }

    pub fn rollback(&mut self) -> Result<(), StorageError> {
        match self.state {
            TransactionState::Active => {
                self.pending.clear();
                self.pending_bytes = 0;
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
        let mem_max = self
            .memtable
            .values()
            .flat_map(|versions| versions.keys())
            .map(|seq| seq.0)
            .max()
            .unwrap_or(0);
        let sst_max = self
            .sstables
            .iter()
            .flat_map(|sst| sst.blocks.iter())
            .map(|_| 0_u64)
            .max()
            .unwrap_or(0);
        LsmCommitSeq(
            mem_max
                .max(sst_max)
                .max(self.next_commit_seq.saturating_sub(1)),
        )
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
    let mut versions = BTreeMap::<PhysicalKey, BTreeMap<LsmCommitSeq, EntryValue>>::new();
    for sstable in &shared.sstables {
        if range.is_some_and(|range| {
            !range.overlaps(
                sstable.reference.min.clustering,
                sstable.reference.max.clustering,
            )
        }) {
            continue;
        }
        for entry in read_sstable_entries(sstable, &shared.table, range)? {
            versions
                .entry(entry.key)
                .or_default()
                .insert(entry.version, entry.value);
        }
    }
    for (key, mem_versions) in &shared.memtable {
        if range.is_none_or(|range| range.contains(key.clustering)) {
            versions
                .entry(*key)
                .or_default()
                .extend(mem_versions.clone());
        }
    }
    let mut visible = BTreeMap::<PhysicalKey, VisibleRow>::new();
    for (key, entries) in versions {
        if let Some((version, EntryValue::Put(row))) = entries.range(..=view.horizon).next_back() {
            visible.insert(
                key,
                VisibleRow {
                    key,
                    observed: LsmObservedVersion::Committed(*version),
                    row: row.clone(),
                },
            );
        }
    }
    for (row_id, pending) in &view.pending {
        if let Some(original) = pending.original_key {
            visible.remove(&PhysicalKey {
                clustering: original,
                row_id: *row_id,
            });
        }
        if let Some(row) = &pending.row {
            let key = PhysicalKey {
                clustering: pending.current_key,
                row_id: *row_id,
            };
            if range.is_none_or(|range| range.contains(key.clustering)) {
                visible.insert(
                    key,
                    VisibleRow {
                        key,
                        observed: LsmObservedVersion::Pending(pending.revision),
                        row: row.clone(),
                    },
                );
            }
        }
    }
    Ok(visible.into_values().collect())
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
    if shared.runtime.recovery_required.get() {
        return Err(CheckpointError::RecoveryRequired.into());
    }
    if let Some(txn_id) = shared.runtime.writer.get() {
        return Err(CheckpointError::WriterActive { txn_id }.into());
    }
    if shared.runtime.outstanding_transactions.get() != 0 {
        return Err(CheckpointError::OutstandingTransactions {
            count: shared.runtime.outstanding_transactions.get(),
        }
        .into());
    }
    if shared.runtime.outstanding_read_views.get() != 0 {
        return Err(LsmError::Busy("outstanding read views").into());
    }
    Ok(())
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
        encode_physical_key(&mut bytes[offset + 20..offset + 40], sstable.min)?;
        encode_physical_key(&mut bytes[offset + 40..offset + 60], sstable.max)?;
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
        let min = decode_physical_key(&bytes[offset + 20..offset + 40], key_type)?;
        let max = decode_physical_key(&bytes[offset + 40..offset + 60], key_type)?;
        if id == 0 || !ids.insert(id) || level > 1 || entry_count == 0 || min > max {
            return Err(LsmError::InvalidManifest("invalid SSTable descriptor").into());
        }
        sstables.push(SstableRef {
            id,
            level,
            entry_count,
            min,
            max,
        });
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
            file.write_all(&bytes)
                .map_err(StorageError::from)
                .map_err(ManifestPublishError::BeforeInstall)?;
            #[cfg(test)]
            maybe_lsm_crash("during-manifest-write");
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
            file.write_all(&bytes)
                .map_err(StorageError::from)
                .map_err(ManifestPublishError::BeforeInstall)?;
            #[cfg(test)]
            maybe_lsm_crash("during-manifest-write");
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
    for record in records {
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

fn classify_prepared(recovered: &BTreeMap<TxnId, RecoveredTxn>) -> Vec<PreparedTransaction> {
    recovered
        .iter()
        .filter_map(|(txn_id, txn)| {
            txn.prepared.map(|database_txn_id| PreparedTransaction {
                database_txn_id,
                physical_txn_id: *txn_id,
                state: if txn.commit.is_some() {
                    PreparedTransactionState::Committed
                } else if txn.aborted {
                    PreparedTransactionState::RolledBack
                } else {
                    PreparedTransactionState::Prepared
                },
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
) -> Result<Lsn, StorageError> {
    if let Some(existing) = find_commit_seq(
        &shared.wal.path,
        shared.manifest.storage_id,
        shared.manifest.wal_generation,
        txn_id,
    )? {
        if existing != expected {
            return Err(LsmError::InvalidWal("retry commit sequence changed").into());
        }
        return Ok(current_lsn);
    }
    shared.wal.append(&WalRecord::Commit {
        txn_id,
        commit_seq: expected,
    })
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
            shared.memtable.clear();
            shared.memtable_bytes = 0;
            shared.runtime.recovery_required.set(true);
            drop(old_wal);
            return Err(source);
        }
    }
    shared.sstables.push(sstable);
    shared.memtable.clear();
    shared.memtable_bytes = 0;
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
    if shared.sstables.len() <= 1
        && shared
            .sstables
            .first()
            .is_none_or(|sst| sst.reference.level == 1)
    {
        return Ok(());
    }
    let mut versions = BTreeMap::<PhysicalKey, BTreeMap<LsmCommitSeq, EntryValue>>::new();
    for sstable in &shared.sstables {
        for entry in read_sstable_entries(sstable, &shared.table, None)? {
            versions
                .entry(entry.key)
                .or_default()
                .insert(entry.version, entry.value);
        }
    }
    let entries = versions
        .into_iter()
        .filter_map(|(key, versions)| {
            versions
                .into_iter()
                .next_back()
                .and_then(|(version, value)| match value {
                    EntryValue::Put(row) => Some(VersionedEntry {
                        key,
                        version,
                        value: EntryValue::Put(row),
                    }),
                    EntryValue::Tombstone => None,
                })
        })
        .collect::<Vec<_>>();
    let old_sstables = shared.sstables.clone();
    let mut candidate = shared.manifest.clone();
    let output = if entries.is_empty() {
        candidate.sstables.clear();
        None
    } else {
        let id = shared.manifest.next_sstable_id;
        candidate.next_sstable_id = id
            .checked_add(1)
            .ok_or(LsmError::AllocatorExhausted("SSTable ID"))?;
        let output = write_sstable(
            &shared.root,
            &shared.manifest,
            id,
            1,
            &entries,
            &shared.table,
        )?;
        candidate.sstables = vec![output.reference.clone()];
        Some(output)
    };
    match publish_manifest(&shared.root, candidate) {
        Ok(manifest) => {
            shared.manifest = manifest;
            shared.sstables = output.into_iter().collect();
        }
        Err(ManifestPublishError::BeforeInstall(error)) => {
            if let Some(output) = output {
                if let Err(cleanup) = remove_unreferenced_file(&output.path) {
                    shared.runtime.recovery_required.set(true);
                    return Err(cleanup);
                }
            }
            return Err(error);
        }
        Err(ManifestPublishError::InstalledButUnsynced { manifest, source }) => {
            shared.manifest = *manifest;
            shared.sstables = output.into_iter().collect();
            shared.runtime.recovery_required.set(true);
            return Err(source);
        }
    }
    for old in old_sstables {
        if !shared
            .sstables
            .iter()
            .any(|current| current.reference.id == old.reference.id)
        {
            #[cfg(test)]
            maybe_lsm_crash("while-deleting-old-sst");
            match fs::remove_file(old.path) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
    }
    sync_directory(&shared.root.join(SST_DIR_NAME))?;
    cleanup_orphans(shared)?;
    Ok(())
}

fn remove_unreferenced_file(path: &Path) -> Result<(), StorageError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
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
    if entries.is_empty() {
        return Err(LsmError::InvalidSstable {
            sstable_id: id,
            reason: "cannot write an empty SSTable",
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
    let reference = SstableRef {
        id,
        level,
        entry_count: entries.len() as u64,
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
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp)?;
    let header = encode_sstable_header(manifest, &reference, blocks.len() as u32)?;
    file.write_all(&header)?;
    let mut offset = SST_HEADER_SIZE as u64;
    let mut metas = Vec::with_capacity(blocks.len());
    for (block_index, block) in blocks.iter().enumerate() {
        let (block_bytes, meta) = encode_sstable_block(id, block_index as u32, block, offset)?;
        file.write_all(&block_bytes)?;
        offset = offset
            .checked_add(block_bytes.len() as u64)
            .ok_or(LsmError::InvalidSstable {
                sstable_id: id,
                reason: "file offset overflows",
            })?;
        metas.push(meta);
    }
    #[cfg(test)]
    maybe_lsm_crash("during-sst-write");
    file.sync_all()?;
    #[cfg(test)]
    maybe_lsm_crash("after-sst-sync");
    fs::rename(&temp, &final_path)?;
    sync_directory(&root.join(SST_DIR_NAME))?;
    Ok(Sstable {
        reference,
        path: final_path,
        blocks: metas,
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
    let checksum = crc32c::crc32c(&bytes[..140]);
    bytes[140..144].copy_from_slice(&checksum.to_le_bytes());
    Ok(bytes)
}

fn decode_sstable_header(
    bytes: &[u8; SST_HEADER_SIZE],
    manifest: &Manifest,
    reference: &SstableRef,
) -> Result<u32, StorageError> {
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
        .chain(bytes[120..140].iter())
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
    {
        return Err(invalid("header identity differs from manifest").into());
    }
    let stored = read_u32(bytes, 140)?;
    let computed = crc32c::crc32c(&bytes[..140]);
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
    if count == 0 || u64::from(count) > reference.entry_count {
        return Err(invalid("invalid block count").into());
    }
    Ok(count)
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
) -> Result<Sstable, StorageError> {
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
    let block_count = decode_sstable_header(&header, manifest, reference)?;
    let mut offset = SST_HEADER_SIZE as u64;
    let mut blocks = Vec::with_capacity(block_count as usize);
    let mut total_entries = 0_u64;
    let mut previous = None;
    for block_index in 0..block_count {
        let (meta, entries, next) = read_sstable_block(
            &mut file,
            reference.id,
            block_index,
            offset,
            manifest.key_type,
            table,
        )?;
        for entry in &entries {
            if previous.is_some_and(|previous: (PhysicalKey, LsmCommitSeq)| {
                previous >= (entry.key, entry.version)
            }) {
                return Err(LsmError::InvalidSstable {
                    sstable_id: reference.id,
                    reason: "entries are not strictly sorted",
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
        blocks.push(meta);
        offset = next;
    }
    if total_entries != reference.entry_count || offset != file.metadata()?.len() {
        return Err(LsmError::InvalidSstable {
            sstable_id: reference.id,
            reason: "file length or entry count differs from header",
        }
        .into());
    }
    Ok(Sstable {
        reference: reference.clone(),
        path,
        blocks,
    })
}

fn read_sstable_entries(
    sstable: &Sstable,
    table: &TableDef,
    range: Option<&KeyRange>,
) -> Result<Vec<VersionedEntry>, StorageError> {
    let mut file = File::open(&sstable.path)?;
    let mut entries = Vec::new();
    for (index, meta) in sstable.blocks.iter().enumerate() {
        if range.is_some_and(|range| !range.overlaps(meta.first.clustering, meta.last.clustering)) {
            continue;
        }
        let (_, block_entries, _) = read_sstable_block(
            &mut file,
            sstable.reference.id,
            index as u32,
            meta.offset,
            meta.first.clustering.kind(),
            table,
        )?;
        if block_entries.len() != meta.entry_count as usize {
            return Err(LsmError::InvalidSstable {
                sstable_id: sstable.reference.id,
                reason: "sparse block entry count changed",
            }
            .into());
        }
        let expected_payload_end = meta
            .offset
            .checked_add(SST_BLOCK_HEADER_SIZE as u64)
            .and_then(|value| value.checked_add(u64::from(meta.payload_length)))
            .and_then(|value| value.checked_add(4))
            .ok_or(LsmError::InvalidSstable {
                sstable_id: sstable.reference.id,
                reason: "sparse block offset overflows",
            })?;
        if file.stream_position()? != expected_payload_end {
            return Err(LsmError::InvalidSstable {
                sstable_id: sstable.reference.id,
                reason: "sparse block length changed",
            }
            .into());
        }
        entries.extend(
            block_entries
                .into_iter()
                .filter(|entry| range.is_none_or(|range| range.contains(entry.key.clustering))),
        );
    }
    Ok(entries)
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

#[cfg(not(test))]
fn maybe_fail_manifest_publish(_point: ManifestPublishPoint) -> Result<(), StorageError> {
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
        ColumnId, DatabaseTxnId, LsmRowId, PhysicalType, ScalarValue, StorageId, TableId, TxnId,
    };

    use super::{LsmObservedVersion, LsmStorage};
    use crate::{
        PreparedDecision, PreparedTxnResolution, RecoveryError, StorageError, TransactionState,
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

    fn run_maintenance_crash(root: &std::path::Path, operation: &str, point: &str) {
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
                10,
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
            run_maintenance_crash(&root, "flush", point);
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
            run_maintenance_crash(&root, "compact", point);
            cleanup(&root);
        }
    }
}
