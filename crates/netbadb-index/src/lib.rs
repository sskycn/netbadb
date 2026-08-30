//! Pure typed B+Tree domain objects, ordering, codecs, and split calculations.
//!
//! Physical pages, WAL, transactions, and file allocation intentionally live
//! in `netbadb-storage`.

use std::cmp::Ordering;
use std::error::Error;
use std::fmt;

use netbadb_types::{
    ColumnId, IndexId, IndexName, PageGeneration, PageId, PageRef, PhysicalType, RowId,
    ScalarValue, SemanticType,
};

pub const BTREE_FORMAT_VERSION: u16 = 3;
/// Additional bytes reserved on every owned BTree page.
pub const BTREE_OWNER_SIZE: usize = 8;
pub const INDEX_CATALOG_FORMAT_VERSION: u16 = 9;
const META_MAGIC: &[u8; 4] = b"NBTM";
const LEAF_MAGIC: &[u8; 4] = b"NBTL";
const INTERNAL_MAGIC: &[u8; 4] = b"NBTI";
const COMMON_HEADER_SIZE: usize = 8;
const NODE_HEADER_SIZE: usize = COMMON_HEADER_SIZE + 4 + 8;
const ROW_ID_SIZE: usize = 8 + 2 + 4;
const MIN_ENTRY_SIZE: usize = 1 + ROW_ID_SIZE;
const INDEX_CATALOG_MAGIC: &[u8; 4] = b"NBIC";
const INDEX_CATALOG_HEADER_SIZE: usize = 48;
const INDEX_CATALOG_ENTRY_HEADER_SIZE: usize = 56;
const INDEX_CATALOG_PENDING_SIZE: usize = 32;
const LEGACY_INDEX_CATALOG_ENTRY_HEADER_SIZE: usize = 40;
const LEGACY_INDEX_CATALOG_FORMAT_VERSION: u16 = 2;

/// Persistent physical/nominal key identity and NULL acceptance for one tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexSpec {
    pub data_type: SemanticType,
    pub nullable: bool,
}

/// One explicit endpoint of an ordered index lookup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexBound {
    Unbounded,
    Included(ScalarValue),
    Excluded(ScalarValue),
}

/// Typed lower and upper endpoints for one ordered index lookup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexRange {
    pub lower: IndexBound,
    pub upper: IndexBound,
}

/// Persistent tree reference. Legacy is explicit and never assigned a fabricated
/// generation. A generation-aware pointer cannot be downgraded during traversal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BTreePageRef {
    Legacy(PageId),
    Allocated(PageRef),
}

impl BTreePageRef {
    #[must_use]
    pub const fn page_id(self) -> PageId {
        match self {
            Self::Legacy(id) => id,
            Self::Allocated(reference) => reference.page_id,
        }
    }
    #[must_use]
    pub const fn generation(self) -> Option<PageGeneration> {
        match self {
            Self::Legacy(_) => None,
            Self::Allocated(reference) => Some(reference.generation),
        }
    }
}

/// Owner and exact allocation reference, valid across reopen in one storage.
/// Legacy handles retain their historical non-generation-safe semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BTreeHandle {
    /// None accepts only unowned v1, never an arbitrary owned page.
    pub owner: Option<IndexId>,
    pub meta_page: BTreePageRef,
}

/// Persistent single-column registered-index identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexDefinition {
    /// Stable, nonzero table-scoped logical identity. Never reused after retirement.
    pub id: IndexId,
    /// Durable user-visible logical name. Legacy pre-v3 entries are unnamed.
    pub name: Option<IndexName>,
    pub column_id: ColumnId,
    pub handle: BTreeHandle,
}

/// Optimizer snapshot for one table at the last explicit `ANALYZE`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableStatistics {
    pub row_count: u64,
    /// Storage-neutral work for one sequential table scan. It includes every
    /// managed page the engine's scan path must visit, including colocated
    /// access-method pages when the engine validates and skips them.
    pub managed_page_count: u64,
}

/// Optimizer snapshot for one registered index at the last explicit `ANALYZE`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexStatistics {
    pub distinct_non_null_keys: u64,
    pub null_count: u64,
    pub tree_height: u32,
}

/// One registered-index identity plus its optional optimizer snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexCatalogEntry {
    /// Durable retirement; the definition retains ownership of the old tree.
    pub retired: bool,
    pub definition: IndexDefinition,
    pub statistics: Option<IndexStatistics>,
}

/// One page in the persistent registration chain, with WAL-backed retirement.
///
/// The version-9 `NBIC` payload uses a fixed-width little-endian header and
/// entry prefixes plus bounded optional names. Storage validates chain rules.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexCatalogNode {
    /// Root-only crash recovery intent; never stored on a newly allocated page.
    pub reclaim_intent: Option<TailReclaimIntent>,
    /// Minimal retired ownership, retained until physical reclamation completes.
    pub pending: Vec<RetiredIndexOwnership>,
    /// Root-only durable allocator boundary; None on continuations and legacy decode.
    pub next_index_id: Option<IndexId>,
    pub next_catalog: Option<PageId>,
    pub table_statistics: Option<TableStatistics>,
    pub entries: Vec<IndexCatalogEntry>,
}

impl IndexCatalogNode {
    #[must_use]
    pub fn empty() -> Self {
        Self {
            next_index_id: Some(IndexId(1)),
            next_catalog: None,
            table_statistics: None,
            entries: Vec::new(),
            pending: Vec::new(),
            reclaim_intent: None,
        }
    }
}

/// An owned tree pending physical reclamation. Legacy refs remain unreclaimable.
/// Presence means Pending; no names, columns or statistics survive compaction.
/// IndexId is nonzero and is never reused after committed retirement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetiredIndexOwnership {
    pub index_id: IndexId,
    /// None is an owner-only v3 retirement. Some retains a legacy or pre-v9
    /// root-dependent identity until validated catalog compaction converts it.
    pub meta_page: Option<BTreePageRef>,
}

impl RetiredIndexOwnership {
    /// Only owner-only v3 retirements and explicit generated roots qualify.
    #[must_use]
    pub fn is_generation_safe(self) -> bool {
        self.meta_page
            .is_none_or(|page| page.generation().is_some())
    }
}

/// A validated whole-tree suffix decision, persisted before file truncation.
/// Covered identities duplicate retained catalog ownership until finalization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TailReclaimIntent {
    pub old_page_count: u64,
    pub truncate_from: u64,
    /// Logical WAL generation base established by the immediately preceding checkpoint.
    pub checkpoint_lsn: netbadb_types::Lsn,
    pub covered: Vec<RetiredIndexOwnership>,
}

impl TailReclaimIntent {
    /// Validates intrinsic geometry and exact allocation identities. Storage also
    /// checks every covered identity against the complete retained catalog chain.
    pub fn validate(&self) -> Result<(), IndexError> {
        if self.truncate_from < 3
            || self.truncate_from >= self.old_page_count
            || self.checkpoint_lsn.0 == 0
            || self.covered.is_empty()
        {
            return Err(IndexError::InvalidReclaimIntent);
        }
        validate_pending_ownership(None, &[], &self.covered)?;
        if self
            .covered
            .windows(2)
            .any(|pair| pair[0].index_id >= pair[1].index_id)
        {
            return Err(IndexError::InvalidReclaimIntent);
        }
        for record in &self.covered {
            let Some(reference) = record.meta_page else {
                continue;
            };
            let page = reference.page_id().0;
            let generation = reference
                .generation()
                .ok_or(IndexError::InvalidReclaimIntent)?;
            validate_generation(generation)?;
            if page < self.truncate_from
                || page >= self.old_page_count
                || generation.0 >= self.checkpoint_lsn.0
            {
                return Err(IndexError::InvalidReclaimIntent);
            }
        }
        Ok(())
    }
}

/// Complete ordered identity of one leaf entry or persistent internal fence.
///
/// A separator's `RowId` is an ordering token. It need not continue to name a
/// live heap row or leaf entry after deletion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexEntryKey {
    pub key: ScalarValue,
    pub row_id: RowId,
}

/// Alias emphasizing the leaf-entry role of [`IndexEntryKey`].
pub type IndexEntry = IndexEntryKey;

/// Decoded `NBTM` v1 (raw), v2 (owned legacy), or v3 (generation-aware) metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaNode {
    pub generation: Option<PageGeneration>,
    pub owner: Option<IndexId>,
    pub root_page: BTreePageRef,
    pub height: u32,
    pub spec: IndexSpec,
}

/// Decoded `NBTL` v1/v2/v3 leaf payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeafNode {
    pub entries: Vec<IndexEntry>,
    pub next_leaf: Option<BTreePageRef>,
}

/// One full-key routing boundary and its right child.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InternalSeparator {
    pub key: IndexEntryKey,
    pub right_child: BTreePageRef,
}

/// Decoded `NBTI` v1/v2/v3 internal payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InternalNode {
    pub first_child: BTreePageRef,
    pub separators: Vec<InternalSeparator>,
}

/// Typed failures from index validation, codecs, ordering, and split logic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexError {
    InvalidReclaimIntent,
    InvalidPageGeneration(PageGeneration),
    PageGenerationExhausted,
    GenerationMismatch {
        expected: Option<PageGeneration>,
        actual: Option<PageGeneration>,
    },
    InvalidMagic {
        expected: [u8; 4],
        actual: [u8; 4],
    },
    UnsupportedVersion(u16),
    InvalidReservedBytes,
    InvalidNodeType,
    OwnerMismatch {
        expected: Option<IndexId>,
        actual: Option<IndexId>,
    },
    InvalidHeight(u32),
    InvalidChild(PageId),
    InvalidEntryOrder,
    InvalidPhysicalType(u8),
    InvalidNullable(u8),
    InvalidSemanticNamePresence(u8),
    InvalidStatisticsPresence(u8),
    InvalidIndexNamePresence(u8),
    InvalidIndexName,
    InvalidIndexState(u8),
    InvalidIndexId(IndexId),
    InvalidIndexHighWater(IndexId),
    IndexIdExhausted,
    SharedTreePage(PageId),
    InvalidLeafChain(PageId),
    DuplicateIndexId(IndexId),
    DuplicateTreeHandle(PageId),
    UnknownIndexId(IndexId),
    IndexAlreadyRetired(IndexId),
    RetiredIndexStatistics(IndexId),
    InvalidValueTag(u8),
    InvalidBoolean(u8),
    InvalidUtf8,
    InvalidRowId {
        page: PageId,
        generation: u32,
    },
    Truncated,
    LengthOverflow,
    ExtraBytes,
    TypeMismatch {
        expected: PhysicalType,
        actual: Option<PhysicalType>,
    },
    NullNotAllowed,
    DuplicateEntry,
    EntryNotFound,
    KeyTooLarge {
        size: usize,
        capacity: usize,
    },
    NodeTooLarge {
        size: usize,
        capacity: usize,
    },
    LeafChainCycle {
        page_id: PageId,
    },
    EmptyLeaf {
        page_id: PageId,
    },
    LeafChainOrder {
        left_page: PageId,
        right_page: PageId,
    },
    InvalidLeafLink {
        left_page: PageId,
        expected_right: PageId,
        actual_right: Option<PageId>,
    },
    IndexAlreadyExists {
        column_id: ColumnId,
    },
    IndexNameAlreadyExists {
        name: IndexName,
    },
    UnknownIndexColumn {
        column_id: ColumnId,
    },
    DuplicateRegisteredColumn {
        column_id: ColumnId,
    },
    CatalogCycle {
        page_id: PageId,
    },
    TableStatisticsOnContinuation {
        page_id: PageId,
    },
    CatalogSpecMismatch {
        column_id: ColumnId,
    },
    MissingTableStatistics,
    InvalidManagedPageCount(u64),
    InvalidNullCount {
        null_count: u64,
        row_count: u64,
    },
    InvalidDistinctCount {
        distinct_non_null_keys: u64,
        non_null_rows: u64,
    },
}

impl fmt::Display for IndexError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidReclaimIntent => formatter.write_str("invalid tail reclaim intent"),
            Self::InvalidPageGeneration(generation) => {
                write!(formatter, "invalid page generation {}", generation.0)
            }
            Self::PageGenerationExhausted => formatter.write_str("page generation exhausted"),
            Self::GenerationMismatch { expected, actual } => write!(
                formatter,
                "stale page reference: generation mismatch (expected {expected:?}, actual {actual:?})"
            ),
            Self::InvalidMagic { expected, actual } => write!(
                formatter,
                "B+Tree payload magic {:?} does not match {:?}",
                actual, expected
            ),
            Self::UnsupportedVersion(version) => {
                write!(formatter, "unsupported B+Tree payload version {version}")
            }
            Self::InvalidReservedBytes => {
                formatter.write_str("B+Tree payload reserved bytes are non-zero")
            }
            Self::OwnerMismatch { expected, actual } => write!(
                formatter,
                "BTree owner {actual:?} does not match expected {expected:?}"
            ),
            Self::InvalidNodeType => formatter.write_str("invalid B+Tree node type"),
            Self::InvalidHeight(height) => write!(formatter, "invalid B+Tree height {height}"),
            Self::InvalidChild(page) => write!(formatter, "invalid B+Tree child page {}", page.0),
            Self::InvalidEntryOrder => {
                formatter.write_str("B+Tree entries are not strictly increasing")
            }
            Self::InvalidPhysicalType(tag) => {
                write!(formatter, "invalid B+Tree physical type tag {tag}")
            }
            Self::InvalidNullable(value) => {
                write!(formatter, "invalid B+Tree nullable flag {value}")
            }
            Self::InvalidSemanticNamePresence(value) => {
                write!(formatter, "invalid semantic-name presence flag {value}")
            }
            Self::InvalidStatisticsPresence(value) => {
                write!(formatter, "invalid statistics presence flag {value}")
            }
            Self::InvalidIndexNamePresence(value) => {
                write!(formatter, "invalid index-name presence flag {value}")
            }
            Self::InvalidIndexState(value) => {
                write!(formatter, "invalid index lifecycle state {value}")
            }
            Self::InvalidIndexHighWater(id) => {
                write!(formatter, "invalid index high-water {}", id.0)
            }
            Self::IndexIdExhausted => formatter.write_str("index identity space exhausted"),
            Self::SharedTreePage(page) => {
                write!(formatter, "duplicate or shared tree page {}", page.0)
            }
            Self::InvalidLeafChain(page) => {
                write!(formatter, "invalid leaf chain at page {}", page.0)
            }
            Self::InvalidIndexId(id) => write!(formatter, "invalid index identity {}", id.0),
            Self::DuplicateIndexId(id) => write!(formatter, "duplicate index identity {}", id.0),
            Self::DuplicateTreeHandle(page) => {
                write!(formatter, "duplicate registered tree {}", page.0)
            }
            Self::UnknownIndexId(id) => write!(formatter, "unknown index identity {}", id.0),
            Self::IndexAlreadyRetired(id) => write!(formatter, "index {} is already retired", id.0),
            Self::RetiredIndexStatistics(id) => {
                write!(formatter, "retired index {} has statistics", id.0)
            }
            Self::InvalidIndexName => formatter.write_str("invalid persistent index name"),
            Self::InvalidValueTag(tag) => write!(formatter, "invalid index value tag {tag}"),
            Self::InvalidBoolean(value) => write!(formatter, "invalid index boolean {value}"),
            Self::InvalidUtf8 => formatter.write_str("B+Tree text is not valid UTF-8"),
            Self::InvalidRowId { page, generation } => write!(
                formatter,
                "invalid index RowId at page {} with generation {generation}",
                page.0
            ),
            Self::Truncated => formatter.write_str("B+Tree payload is truncated"),
            Self::LengthOverflow => formatter.write_str("B+Tree payload length overflows"),
            Self::ExtraBytes => formatter.write_str("B+Tree payload contains extra bytes"),
            Self::TypeMismatch { expected, actual } => {
                write!(formatter, "index expects {expected}, found {actual:?}")
            }
            Self::NullNotAllowed => formatter.write_str("index key must not be NULL"),
            Self::DuplicateEntry => formatter.write_str("index entry already exists"),
            Self::EntryNotFound => formatter.write_str("index entry does not exist"),
            Self::KeyTooLarge { size, capacity } => write!(
                formatter,
                "encoded index key entry is {size} bytes; leaf capacity is {capacity}"
            ),
            Self::NodeTooLarge { size, capacity } => write!(
                formatter,
                "encoded B+Tree node is {size} bytes; payload capacity is {capacity}"
            ),
            Self::LeafChainCycle { page_id } => {
                write!(formatter, "B+Tree leaf chain cycles at page {}", page_id.0)
            }
            Self::EmptyLeaf { page_id } => {
                write!(
                    formatter,
                    "non-root B+Tree leaf page {} is empty",
                    page_id.0
                )
            }
            Self::LeafChainOrder {
                left_page,
                right_page,
            } => write!(
                formatter,
                "B+Tree leaf chain is not strictly increasing between pages {} and {}",
                left_page.0, right_page.0
            ),
            Self::InvalidLeafLink {
                left_page,
                expected_right,
                actual_right,
            } => write!(
                formatter,
                "B+Tree leaf page {} links to {:?}, expected page {}",
                left_page.0,
                actual_right.map(|page| page.0),
                expected_right.0
            ),
            Self::IndexAlreadyExists { column_id } => {
                write!(
                    formatter,
                    "column {} already has a registered index",
                    column_id.0
                )
            }
            Self::IndexNameAlreadyExists { name } => {
                write!(formatter, "index name `{name}` already exists")
            }
            Self::UnknownIndexColumn { column_id } => {
                write!(
                    formatter,
                    "registered index column {} does not exist",
                    column_id.0
                )
            }
            Self::DuplicateRegisteredColumn { column_id } => write!(
                formatter,
                "index catalog contains duplicate column {}",
                column_id.0
            ),
            Self::CatalogCycle { page_id } => {
                write!(
                    formatter,
                    "index catalog chain cycles at page {}",
                    page_id.0
                )
            }
            Self::TableStatisticsOnContinuation { page_id } => write!(
                formatter,
                "index catalog continuation page {} contains table statistics",
                page_id.0
            ),
            Self::CatalogSpecMismatch { column_id } => write!(
                formatter,
                "registered index spec does not match column {}",
                column_id.0
            ),
            Self::MissingTableStatistics => {
                formatter.write_str("index statistics require table statistics")
            }
            Self::InvalidManagedPageCount(count) => {
                write!(formatter, "invalid managed page count {count}")
            }
            Self::InvalidNullCount {
                null_count,
                row_count,
            } => write!(
                formatter,
                "index NULL count {null_count} exceeds table row count {row_count}"
            ),
            Self::InvalidDistinctCount {
                distinct_non_null_keys,
                non_null_rows,
            } => write!(
                formatter,
                "index distinct non-NULL count {distinct_non_null_keys} is invalid for {non_null_rows} non-NULL rows"
            ),
        }
    }
}

impl Error for IndexError {}

impl IndexSpec {
    /// Validates a runtime scalar against physical type and NULL policy.
    pub fn validate_key(&self, key: &ScalarValue) -> Result<(), IndexError> {
        if matches!(key, ScalarValue::Null) {
            return if self.nullable {
                Ok(())
            } else {
                Err(IndexError::NullNotAllowed)
            };
        }
        let actual = key.physical_type();
        if actual != Some(self.data_type.physical) {
            return Err(IndexError::TypeMismatch {
                expected: self.data_type.physical,
                actual,
            });
        }
        Ok(())
    }
}

impl IndexRange {
    /// Validates both endpoints against the tree's physical key identity and
    /// NULL policy. Logically empty ranges remain valid.
    pub fn validate(&self, spec: &IndexSpec) -> Result<(), IndexError> {
        for bound in [&self.lower, &self.upper] {
            match bound {
                IndexBound::Unbounded => {}
                IndexBound::Included(value) | IndexBound::Excluded(value) => {
                    spec.validate_key(value)?;
                }
            }
        }
        Ok(())
    }

    /// Reports whether the explicit endpoints contain no ordered key.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        let (lower_value, lower_included) = match &self.lower {
            IndexBound::Unbounded => return false,
            IndexBound::Included(value) => (value, true),
            IndexBound::Excluded(value) => (value, false),
        };
        let (upper_value, upper_included) = match &self.upper {
            IndexBound::Unbounded => return false,
            IndexBound::Included(value) => (value, true),
            IndexBound::Excluded(value) => (value, false),
        };
        match compare_values(lower_value, upper_value) {
            Ordering::Greater => true,
            Ordering::Equal => !(lower_included && upper_included),
            Ordering::Less => false,
        }
    }
}

impl LeafNode {
    /// Constructs a leaf with no entries or next-leaf link.
    pub fn empty() -> Self {
        Self {
            entries: Vec::new(),
            next_leaf: None,
        }
    }

    /// Inserts in full `(key, RowId)` order and rejects an exact duplicate.
    pub fn insert(&mut self, spec: &IndexSpec, entry: IndexEntry) -> Result<usize, IndexError> {
        spec.validate_key(&entry.key)?;
        validate_row_id(entry.row_id)?;
        match self
            .entries
            .binary_search_by(|existing| compare_entry_keys(existing, &entry))
        {
            Ok(_) => Err(IndexError::DuplicateEntry),
            Err(index) => {
                self.entries.insert(index, entry);
                Ok(index)
            }
        }
    }

    /// Removes exactly one `(key, RowId)` entry while preserving full order.
    pub fn remove(&mut self, spec: &IndexSpec, entry: &IndexEntryKey) -> Result<(), IndexError> {
        spec.validate_key(&entry.key)?;
        validate_row_id(entry.row_id)?;
        let position = self
            .entries
            .binary_search_by(|existing| compare_entry_keys(existing, entry))
            .map_err(|_| IndexError::EntryNotFound)?;
        self.entries.remove(position);
        Ok(())
    }

    /// Reports whether the complete `(key, RowId)` identity is present.
    pub fn contains_exact(
        &self,
        spec: &IndexSpec,
        entry: &IndexEntryKey,
    ) -> Result<bool, IndexError> {
        spec.validate_key(&entry.key)?;
        validate_row_id(entry.row_id)?;
        Ok(self
            .entries
            .binary_search_by(|existing| compare_entry_keys(existing, entry))
            .is_ok())
    }
}

impl InternalNode {
    /// Returns the child at a separator boundary position.
    pub fn child(&self, position: usize) -> Result<BTreePageRef, IndexError> {
        let child = if position == 0 {
            self.first_child
        } else {
            self.separators
                .get(position - 1)
                .ok_or(IndexError::InvalidNodeType)?
                .right_child
        };
        validate_child(child)?;
        Ok(child)
    }

    /// Chooses a child using complete `(key, RowId)` separator order.
    pub fn child_position(&self, key: &IndexEntryKey) -> usize {
        self.separators
            .partition_point(|separator| compare_entry_keys(key, &separator.key) != Ordering::Less)
    }

    /// Inserts a separator immediately after the split child position.
    pub fn insert_separator(
        &mut self,
        child_position: usize,
        key: IndexEntryKey,
        right_child: BTreePageRef,
    ) -> Result<(), IndexError> {
        validate_child(right_child)?;
        if child_position > self.separators.len() {
            return Err(IndexError::InvalidNodeType);
        }
        self.separators
            .insert(child_position, InternalSeparator { key, right_child });
        validate_internal_order(self)
    }

    /// Removes the separator that owns the child immediately to its right.
    pub fn remove_separator(&mut self, position: usize) -> Result<InternalSeparator, IndexError> {
        if position >= self.separators.len() {
            return Err(IndexError::InvalidNodeType);
        }
        Ok(self.separators.remove(position))
    }
}

/// Compares RowIds by PageId, SlotId, then generation without adding an `Ord`
/// contract to the shared RowId type.
pub fn compare_row_ids(left: RowId, right: RowId) -> Ordering {
    left.page
        .0
        .cmp(&right.page.0)
        .then_with(|| left.slot.cmp(&right.slot))
        .then_with(|| left.generation.cmp(&right.generation))
}

/// Compares index values with NULL first and native typed value ordering.
pub fn compare_values(left: &ScalarValue, right: &ScalarValue) -> Ordering {
    match (left, right) {
        (ScalarValue::Null, ScalarValue::Null) => Ordering::Equal,
        (ScalarValue::Null, _) => Ordering::Less,
        (_, ScalarValue::Null) => Ordering::Greater,
        (ScalarValue::Bool(left), ScalarValue::Bool(right)) => left.cmp(right),
        (ScalarValue::Int64(left), ScalarValue::Int64(right)) => left.cmp(right),
        (ScalarValue::UInt64(left), ScalarValue::UInt64(right)) => left.cmp(right),
        (ScalarValue::Text(left), ScalarValue::Text(right)) => left.cmp(right),
        (left, right) => value_rank(left).cmp(&value_rank(right)),
    }
}

/// Compares complete entry identities by key and then explicit RowId order.
pub fn compare_entry_keys(left: &IndexEntryKey, right: &IndexEntryKey) -> Ordering {
    compare_values(&left.key, &right.key).then_with(|| compare_row_ids(left.row_id, right.row_id))
}

/// Compares only a user key with an entry's user key.
pub fn compare_key_to_entry(key: &ScalarValue, entry: &IndexEntryKey) -> Ordering {
    compare_values(key, &entry.key)
}

/// Ensures a key can fit both a leaf entry and a future internal separator.
///
/// The fixed-width RowId payload is included without requiring a fabricated
/// physical locator, making this suitable for preflighting heap inserts.
pub fn ensure_key_fits(
    spec: &IndexSpec,
    key: &ScalarValue,
    capacity: usize,
) -> Result<(), IndexError> {
    spec.validate_key(key)?;
    let leaf_size = NODE_HEADER_SIZE
        .checked_add(encoded_key_len(key)?)
        .and_then(|size| size.checked_add(ROW_ID_SIZE))
        .ok_or(IndexError::LengthOverflow)?;
    let internal_size = leaf_size.checked_add(8).ok_or(IndexError::LengthOverflow)?;
    if internal_size > capacity {
        return Err(IndexError::KeyTooLarge {
            size: internal_size,
            capacity,
        });
    }
    Ok(())
}

/// Ensures an entry can fit both a leaf and a future internal separator.
pub fn ensure_entry_fits(
    spec: &IndexSpec,
    entry: &IndexEntry,
    capacity: usize,
) -> Result<(), IndexError> {
    spec.validate_key(&entry.key)?;
    validate_row_id(entry.row_id)?;
    ensure_key_fits(spec, &entry.key, capacity)
}

/// Encodes one validated `NBTM` v1/v2/v3 payload.
pub fn encode_meta(node: &MetaNode) -> Result<Vec<u8>, IndexError> {
    validate_meta(node)?;
    let mut output = common_header_generation(META_MAGIC, node.owner, node.generation)?;
    encode_page_ref(&mut output, Some(node.root_page), node.generation.is_some())?;
    output.extend_from_slice(&node.height.to_le_bytes());
    output.push(physical_type_tag(node.spec.data_type.physical));
    output.push(u8::from(node.spec.nullable));
    let name = node.spec.data_type.name.as_deref();
    output.push(u8::from(name.is_some()));
    output.push(0);
    push_text(&mut output, name.unwrap_or(""))?;
    Ok(output)
}

/// Decodes and fully validates one `NBTM` v1/v2/v3 payload.
pub fn decode_meta(input: &[u8]) -> Result<MetaNode, IndexError> {
    let mut decoder = Decoder::new(input);
    let owner = decoder.common_header(META_MAGIC)?;
    let generation = decoder.generation;
    let root_page = decoder
        .page_ref(generation.is_some())?
        .ok_or(IndexError::InvalidChild(PageId(0)))?;
    let height = decoder.u32()?;
    let physical = physical_type_from_tag(decoder.u8()?)?;
    let nullable = decode_bool_flag(decoder.u8()?).map_err(IndexError::InvalidNullable)?;
    let name_present = match decoder.u8()? {
        0 => false,
        1 => true,
        other => return Err(IndexError::InvalidSemanticNamePresence(other)),
    };
    if decoder.u8()? != 0 {
        return Err(IndexError::InvalidReservedBytes);
    }
    let name = decoder.text()?;
    decoder.finish()?;
    if name_present != !name.is_empty() {
        return Err(IndexError::InvalidSemanticNamePresence(u8::from(
            name_present,
        )));
    }
    let node = MetaNode {
        generation,
        owner,
        root_page,
        height,
        spec: IndexSpec {
            data_type: SemanticType {
                physical,
                name: name_present.then_some(name),
            },
            nullable,
        },
    };
    validate_meta(&node)?;
    Ok(node)
}

/// Encodes one validated `NBTL` version-1 payload.
pub fn encode_leaf(spec: &IndexSpec, node: &LeafNode) -> Result<Vec<u8>, IndexError> {
    encode_leaf_owned(spec, node, None)
}

/// Encodes v1 for None or v2 for a nonzero durable owner.
pub fn encode_leaf_owned(
    spec: &IndexSpec,
    node: &LeafNode,
    owner: Option<IndexId>,
) -> Result<Vec<u8>, IndexError> {
    encode_leaf_generation(spec, node, owner, None)
}

/// Encodes an exact v1/v2/v3 identity; all links must match its format.
pub fn encode_leaf_generation(
    spec: &IndexSpec,
    node: &LeafNode,
    owner: Option<IndexId>,
    generation: Option<PageGeneration>,
) -> Result<Vec<u8>, IndexError> {
    validate_leaf(spec, node)?;
    let count = u32::try_from(node.entries.len()).map_err(|_| IndexError::LengthOverflow)?;
    let mut output = common_header_generation(LEAF_MAGIC, owner, generation)?;
    output.extend_from_slice(&count.to_le_bytes());
    encode_page_ref(&mut output, node.next_leaf, generation.is_some())?;
    for entry in &node.entries {
        encode_entry(&mut output, entry)?;
    }
    Ok(output)
}

/// Decodes and fully validates one `NBTL` version-1 payload.
pub fn decode_leaf(spec: &IndexSpec, input: &[u8]) -> Result<LeafNode, IndexError> {
    decode_leaf_owned(spec, input, None)
}

/// Fully validates the payload and requires the exact expected owner/format.
pub fn decode_leaf_owned(
    spec: &IndexSpec,
    input: &[u8],
    expected: Option<IndexId>,
) -> Result<LeafNode, IndexError> {
    let mut decoder = Decoder::new(input);
    let actual = decoder.common_header(LEAF_MAGIC)?;
    validate_btree_owner(expected, actual)?;
    let count = decoder.count(MIN_ENTRY_SIZE)?;
    let next_leaf = decoder.page_ref(decoder.generation.is_some())?;
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        entries.push(decoder.entry(spec)?);
    }
    decoder.finish()?;
    let node = LeafNode { entries, next_leaf };
    validate_leaf(spec, &node)?;
    Ok(node)
}

/// Encodes one validated `NBTI` version-1 payload.
pub fn encode_internal(spec: &IndexSpec, node: &InternalNode) -> Result<Vec<u8>, IndexError> {
    encode_internal_owned(spec, node, None)
}

/// Encodes v1 for None or v2 for a nonzero durable owner.
pub fn encode_internal_owned(
    spec: &IndexSpec,
    node: &InternalNode,
    owner: Option<IndexId>,
) -> Result<Vec<u8>, IndexError> {
    encode_internal_generation(spec, node, owner, None)
}

/// Encodes an exact v1/v2/v3 identity; all links must match its format.
pub fn encode_internal_generation(
    spec: &IndexSpec,
    node: &InternalNode,
    owner: Option<IndexId>,
    generation: Option<PageGeneration>,
) -> Result<Vec<u8>, IndexError> {
    validate_internal(spec, node)?;
    let count = u32::try_from(node.separators.len()).map_err(|_| IndexError::LengthOverflow)?;
    let mut output = common_header_generation(INTERNAL_MAGIC, owner, generation)?;
    output.extend_from_slice(&count.to_le_bytes());
    encode_page_ref(&mut output, Some(node.first_child), generation.is_some())?;
    for separator in &node.separators {
        encode_entry(&mut output, &separator.key)?;
        encode_page_ref(
            &mut output,
            Some(separator.right_child),
            generation.is_some(),
        )?;
    }
    Ok(output)
}

/// Decodes and fully validates one `NBTI` version-1 payload.
pub fn decode_internal(spec: &IndexSpec, input: &[u8]) -> Result<InternalNode, IndexError> {
    decode_internal_owned(spec, input, None)
}

/// Fully validates the payload and requires the exact expected owner/format.
pub fn decode_internal_owned(
    spec: &IndexSpec,
    input: &[u8],
    expected: Option<IndexId>,
) -> Result<InternalNode, IndexError> {
    let mut decoder = Decoder::new(input);
    let actual = decoder.common_header(INTERNAL_MAGIC)?;
    validate_btree_owner(expected, actual)?;
    let count = decoder.count(MIN_ENTRY_SIZE + 8)?;
    let first_child = decoder
        .page_ref(decoder.generation.is_some())?
        .ok_or(IndexError::InvalidChild(PageId(0)))?;
    let mut separators = Vec::with_capacity(count);
    for _ in 0..count {
        separators.push(InternalSeparator {
            key: decoder.entry(spec)?,
            right_child: decoder
                .page_ref(decoder.generation.is_some())?
                .ok_or(IndexError::InvalidChild(PageId(0)))?,
        });
    }
    decoder.finish()?;
    let node = InternalNode {
        first_child,
        separators,
    };
    validate_internal(spec, &node)?;
    Ok(node)
}

/// Chooses a deterministic encoded-byte boundary and links two fitting leaves.
pub fn split_leaf(
    spec: &IndexSpec,
    entries: Vec<IndexEntry>,
    old_next: Option<BTreePageRef>,
    right_page: BTreePageRef,
    capacity: usize,
) -> Result<(LeafNode, LeafNode, IndexEntryKey), IndexError> {
    validate_child(right_page)?;
    if entries.len() < 2 {
        return Err(IndexError::NodeTooLarge {
            size: encoded_entries_size(&entries)?,
            capacity,
        });
    }
    let mut best = None;
    for split in 1..entries.len() {
        let left = LeafNode {
            entries: entries[..split].to_vec(),
            next_leaf: Some(right_page),
        };
        let right = LeafNode {
            entries: entries[split..].to_vec(),
            next_leaf: old_next,
        };
        let left_size = leaf_encoded_len(spec, &left, right_page.generation().is_some())?;
        let right_size = leaf_encoded_len(spec, &right, right_page.generation().is_some())?;
        if left_size <= capacity && right_size <= capacity {
            let imbalance = left_size.abs_diff(right_size);
            if best.is_none_or(|(best_imbalance, best_split)| {
                (imbalance, split) < (best_imbalance, best_split)
            }) {
                best = Some((imbalance, split));
            }
        }
    }
    let Some((_, split)) = best else {
        return Err(IndexError::NodeTooLarge {
            size: encoded_entries_size(&entries)?,
            capacity,
        });
    };
    let left = LeafNode {
        entries: entries[..split].to_vec(),
        next_leaf: Some(right_page),
    };
    let right = LeafNode {
        entries: entries[split..].to_vec(),
        next_leaf: old_next,
    };
    let promoted = right
        .entries
        .first()
        .cloned()
        .ok_or(IndexError::InvalidEntryOrder)?;
    Ok((left, right, promoted))
}

/// Promotes one full separator at a deterministic encoded-byte boundary.
pub fn split_internal(
    spec: &IndexSpec,
    node: InternalNode,
    capacity: usize,
) -> Result<(InternalNode, IndexEntryKey, InternalNode), IndexError> {
    validate_internal(spec, &node)?;
    if node.separators.is_empty() {
        return Err(IndexError::NodeTooLarge {
            size: internal_encoded_len(spec, &node)?,
            capacity,
        });
    }
    let mut best = None;
    for middle in 0..node.separators.len() {
        let promoted = &node.separators[middle];
        let left = InternalNode {
            first_child: node.first_child,
            separators: node.separators[..middle].to_vec(),
        };
        let right = InternalNode {
            first_child: promoted.right_child,
            separators: node.separators[middle + 1..].to_vec(),
        };
        let left_size = internal_encoded_len(spec, &left)?;
        let right_size = internal_encoded_len(spec, &right)?;
        if left_size <= capacity && right_size <= capacity {
            let imbalance = left_size.abs_diff(right_size);
            if best.is_none_or(|(best_imbalance, best_middle)| {
                (imbalance, middle) < (best_imbalance, best_middle)
            }) {
                best = Some((imbalance, middle));
            }
        }
    }
    let Some((_, middle)) = best else {
        return Err(IndexError::NodeTooLarge {
            size: internal_encoded_len(spec, &node)?,
            capacity,
        });
    };
    let promoted = node.separators[middle].clone();
    let left = InternalNode {
        first_child: node.first_child,
        separators: node.separators[..middle].to_vec(),
    };
    let right = InternalNode {
        first_child: promoted.right_child,
        separators: node.separators[middle + 1..].to_vec(),
    };
    Ok((left, promoted.key, right))
}

/// Merges adjacent leaves when their actual version-1 encoding fits one page.
/// The caller keeps the left physical page and orphans the right one.
pub fn merge_leaves_if_fits(
    spec: &IndexSpec,
    left: &LeafNode,
    right: &LeafNode,
    capacity: usize,
) -> Result<Option<LeafNode>, IndexError> {
    let mut entries = Vec::with_capacity(
        left.entries
            .len()
            .checked_add(right.entries.len())
            .ok_or(IndexError::LengthOverflow)?,
    );
    entries.extend(left.entries.iter().cloned());
    entries.extend(right.entries.iter().cloned());
    let merged = LeafNode {
        entries,
        next_leaf: right.next_leaf,
    };
    Ok((leaf_encoded_len(
        spec,
        &merged,
        left.next_leaf.is_some_and(|p| p.generation().is_some()),
    )? <= capacity)
        .then_some(merged))
}

/// Merges adjacent internal nodes through their parent's persistent fence when
/// the actual version-1 encoding fits one page.
pub fn merge_internals_if_fits(
    spec: &IndexSpec,
    left: &InternalNode,
    parent_fence: &IndexEntryKey,
    right: &InternalNode,
    capacity: usize,
) -> Result<Option<InternalNode>, IndexError> {
    spec.validate_key(&parent_fence.key)?;
    validate_row_id(parent_fence.row_id)?;
    validate_child(right.first_child)?;
    let separator_count = left
        .separators
        .len()
        .checked_add(1)
        .and_then(|count| count.checked_add(right.separators.len()))
        .ok_or(IndexError::LengthOverflow)?;
    let mut separators = Vec::with_capacity(separator_count);
    separators.extend(left.separators.iter().cloned());
    separators.push(InternalSeparator {
        key: parent_fence.clone(),
        right_child: right.first_child,
    });
    separators.extend(right.separators.iter().cloned());
    let merged = InternalNode {
        first_child: left.first_child,
        separators,
    };
    Ok((internal_encoded_len(spec, &merged)? <= capacity).then_some(merged))
}

/// Encodes one explicit version-8 mutable index catalog page payload.
pub fn encode_index_catalog(node: &IndexCatalogNode) -> Result<Vec<u8>, IndexError> {
    validate_local_reclaim_intent(node)?;
    if let Some(next) = node.next_catalog {
        validate_child(next)?;
    }
    if let Some(statistics) = node.table_statistics {
        validate_table_statistics(&statistics)?;
        for entry in &node.entries {
            if let Some(index_statistics) = entry.statistics {
                validate_index_statistics(&statistics, &index_statistics)?;
            }
        }
    } else {
        for entry in &node.entries {
            if let Some(statistics) = entry.statistics {
                validate_index_statistics_intrinsic(&statistics)?;
            }
        }
    }
    if let Some(intent) = &node.reclaim_intent {
        intent.validate()?;
        if node.next_index_id.is_none() {
            return Err(IndexError::InvalidReclaimIntent);
        }
    }
    validate_catalog_entries(&node.entries)?;
    validate_pending_ownership(node.next_index_id, &node.entries, &node.pending)?;
    let count = u32::try_from(node.entries.len()).map_err(|_| IndexError::LengthOverflow)?;
    let capacity = node
        .entries
        .iter()
        .try_fold(INDEX_CATALOG_HEADER_SIZE, |size, entry| {
            size.checked_add(INDEX_CATALOG_ENTRY_HEADER_SIZE)
                .and_then(|size| {
                    size.checked_add(
                        entry
                            .definition
                            .name
                            .as_ref()
                            .map_or(0, |name| name.as_str().len()),
                    )
                })
                .ok_or(IndexError::LengthOverflow)
        })?;
    let pending_size = node
        .pending
        .len()
        .checked_mul(INDEX_CATALOG_PENDING_SIZE)
        .ok_or(IndexError::LengthOverflow)?;
    let capacity = capacity
        .checked_add(pending_size)
        .ok_or(IndexError::LengthOverflow)?;
    let mut output = Vec::with_capacity(capacity);
    output.extend_from_slice(INDEX_CATALOG_MAGIC);
    output.extend_from_slice(&INDEX_CATALOG_FORMAT_VERSION.to_le_bytes());
    output.push(u8::from(node.table_statistics.is_some()));
    output.push(if node.reclaim_intent.is_some() {
        2
    } else {
        u8::from(node.next_index_id.is_some())
    });
    output.extend_from_slice(&node.next_catalog.map_or(0, |page| page.0).to_le_bytes());
    output.extend_from_slice(&count.to_le_bytes());
    output.extend_from_slice(
        &u32::try_from(node.pending.len())
            .map_err(|_| IndexError::LengthOverflow)?
            .to_le_bytes(),
    );
    let table_statistics = node.table_statistics.unwrap_or(TableStatistics {
        row_count: 0,
        managed_page_count: 0,
    });
    output.extend_from_slice(&table_statistics.row_count.to_le_bytes());
    output.extend_from_slice(&table_statistics.managed_page_count.to_le_bytes());
    output.extend_from_slice(&node.next_index_id.map_or(0, |id| id.0).to_le_bytes());
    for entry in &node.entries {
        output.extend_from_slice(&entry.definition.column_id.0.to_le_bytes());
        output.push(u8::from(entry.statistics.is_some()));
        output.push(u8::from(entry.definition.name.is_some()));
        let name_length = u16::try_from(
            entry
                .definition
                .name
                .as_ref()
                .map_or(0, |name| name.as_str().len()),
        )
        .map_err(|_| IndexError::LengthOverflow)?;
        output.extend_from_slice(&name_length.to_le_bytes());
        output.extend_from_slice(&entry.definition.handle.meta_page.page_id().0.to_le_bytes());
        let statistics = entry.statistics.unwrap_or(IndexStatistics {
            distinct_non_null_keys: 0,
            null_count: 0,
            tree_height: 0,
        });
        output.extend_from_slice(&statistics.distinct_non_null_keys.to_le_bytes());
        output.extend_from_slice(&statistics.null_count.to_le_bytes());
        output.extend_from_slice(&statistics.tree_height.to_le_bytes());
        output.push(u8::from(entry.retired));
        output.push(
            if entry.definition.handle.meta_page.generation().is_some() {
                2
            } else {
                u8::from(entry.definition.handle.owner.is_some())
            },
        );
        output.extend_from_slice(&[0; 2]);
        output.extend_from_slice(&entry.definition.id.0.to_le_bytes());
        output.extend_from_slice(
            &entry
                .definition
                .handle
                .meta_page
                .generation()
                .map_or(0, |g| g.0)
                .to_le_bytes(),
        );
        if let Some(name) = &entry.definition.name {
            output.extend_from_slice(name.as_str().as_bytes());
        }
    }
    for pending in &node.pending {
        output.extend_from_slice(&pending.index_id.0.to_le_bytes());
        output.extend_from_slice(
            &pending
                .meta_page
                .map_or(0, |page| page.page_id().0)
                .to_le_bytes(),
        );
        output.extend_from_slice(
            &pending
                .meta_page
                .and_then(BTreePageRef::generation)
                .map_or(0, |g| g.0)
                .to_le_bytes(),
        );
        output.push(match pending.meta_page {
            None => 2,
            Some(page) => u8::from(page.generation().is_some()),
        });
        output.extend_from_slice(&[0; 7]);
    }
    if let Some(intent) = &node.reclaim_intent {
        output.extend_from_slice(&intent.old_page_count.to_le_bytes());
        output.extend_from_slice(&intent.truncate_from.to_le_bytes());
        output.extend_from_slice(&intent.checkpoint_lsn.0.to_le_bytes());
        output.extend_from_slice(
            &u32::try_from(intent.covered.len())
                .map_err(|_| IndexError::LengthOverflow)?
                .to_le_bytes(),
        );
        output.extend_from_slice(&[0; 4]);
        for record in &intent.covered {
            output.extend_from_slice(&record.index_id.0.to_le_bytes());
            output.extend_from_slice(
                &record
                    .meta_page
                    .map_or(0, |page| page.page_id().0)
                    .to_le_bytes(),
            );
            output.extend_from_slice(
                &record
                    .meta_page
                    .and_then(BTreePageRef::generation)
                    .map_or(0, |generation| generation.0)
                    .to_le_bytes(),
            );
        }
    }
    Ok(output)
}

/// Decodes legacy version-2 through version-8 or current version-9 index catalog payloads.
pub fn decode_index_catalog(input: &[u8]) -> Result<IndexCatalogNode, IndexError> {
    if input.len() < INDEX_CATALOG_HEADER_SIZE {
        return Err(IndexError::Truncated);
    }
    if &input[..4] != INDEX_CATALOG_MAGIC {
        return Err(IndexError::InvalidMagic {
            expected: *INDEX_CATALOG_MAGIC,
            actual: input[..4].try_into().map_err(|_| IndexError::Truncated)?,
        });
    }
    let version = u16::from_le_bytes(input[4..6].try_into().map_err(|_| IndexError::Truncated)?);
    if !matches!(
        version,
        2 | 3 | 4 | 5 | 6 | 7 | 8 | INDEX_CATALOG_FORMAT_VERSION
    ) {
        return Err(IndexError::UnsupportedVersion(version));
    }
    let table_statistics_present = decode_statistics_presence(input[6])?;
    let next_index_id = if version >= 5 {
        let value = IndexId(u64::from_le_bytes(
            input[40..48]
                .try_into()
                .map_err(|_| IndexError::Truncated)?,
        ));
        match (input[7], value.0) {
            (0, 0) => None,
            (1, 1..) => Some(value),
            (2, 1..) if version >= 8 => Some(value),
            _ => return Err(IndexError::InvalidIndexHighWater(value)),
        }
    } else {
        if input[7] != 0 || input[40..48].iter().any(|byte| *byte != 0) {
            return Err(IndexError::InvalidReservedBytes);
        }
        None
    };
    if version < 6 && input[20..24].iter().any(|byte| *byte != 0) {
        return Err(IndexError::InvalidReservedBytes);
    }
    let raw_next = u64::from_le_bytes(input[8..16].try_into().map_err(|_| IndexError::Truncated)?);
    let next_catalog = (raw_next != 0).then_some(PageId(raw_next));
    let count = u32::from_le_bytes(
        input[16..20]
            .try_into()
            .map_err(|_| IndexError::Truncated)?,
    ) as usize;
    let entry_header_size = if version >= 7 {
        INDEX_CATALOG_ENTRY_HEADER_SIZE
    } else if version >= 4 {
        48
    } else {
        LEGACY_INDEX_CATALOG_ENTRY_HEADER_SIZE
    };
    let maximum_entries = input.len().saturating_sub(INDEX_CATALOG_HEADER_SIZE) / entry_header_size;
    if count > maximum_entries {
        return Err(IndexError::Truncated);
    }
    let raw_table_statistics = TableStatistics {
        row_count: u64::from_le_bytes(
            input[24..32]
                .try_into()
                .map_err(|_| IndexError::Truncated)?,
        ),
        managed_page_count: u64::from_le_bytes(
            input[32..40]
                .try_into()
                .map_err(|_| IndexError::Truncated)?,
        ),
    };
    let table_statistics = if table_statistics_present {
        validate_table_statistics(&raw_table_statistics)?;
        Some(raw_table_statistics)
    } else {
        if raw_table_statistics.row_count != 0 || raw_table_statistics.managed_page_count != 0 {
            return Err(IndexError::InvalidReservedBytes);
        }
        None
    };
    let mut entries = Vec::with_capacity(count);
    let mut offset = INDEX_CATALOG_HEADER_SIZE;
    for _ in 0..count {
        let end = offset
            .checked_add(entry_header_size)
            .ok_or(IndexError::LengthOverflow)?;
        let chunk = input.get(offset..end).ok_or(IndexError::Truncated)?;
        let column_id = ColumnId(u32::from_le_bytes(
            chunk[..4].try_into().map_err(|_| IndexError::Truncated)?,
        ));
        let statistics_present = decode_statistics_presence(chunk[4])?;
        let (name_present, name_length) = if version == LEGACY_INDEX_CATALOG_FORMAT_VERSION {
            if chunk[5..8].iter().any(|byte| *byte != 0) {
                return Err(IndexError::InvalidReservedBytes);
            }
            (false, 0_usize)
        } else {
            let present = decode_index_name_presence(chunk[5])?;
            let length = usize::from(u16::from_le_bytes(
                chunk[6..8].try_into().map_err(|_| IndexError::Truncated)?,
            ));
            if present != (length != 0) {
                return Err(IndexError::InvalidIndexName);
            }
            (present, length)
        };
        let retired = if version >= 4 {
            match chunk[36] {
                0 => false,
                1 => true,
                value => return Err(IndexError::InvalidIndexState(value)),
            }
        } else {
            if chunk[36] != 0 {
                return Err(IndexError::InvalidReservedBytes);
            }
            false
        };
        if chunk[if version >= 6 { 38 } else { 37 }..40]
            .iter()
            .any(|byte| *byte != 0)
        {
            return Err(IndexError::InvalidReservedBytes);
        }
        let meta_page = PageId(u64::from_le_bytes(
            chunk[8..16].try_into().map_err(|_| IndexError::Truncated)?,
        ));
        validate_child(meta_page)?;
        // Legacy registrations had no logical ID. Metadata pages were unique
        // and never reused; freeze that value as the migration ID, then allocate
        // subsequent IDs independently above all active AND retired identities.
        let id = if version >= 4 {
            IndexId(u64::from_le_bytes(
                chunk[40..48]
                    .try_into()
                    .map_err(|_| IndexError::Truncated)?,
            ))
        } else {
            IndexId(meta_page.0)
        };
        let raw_statistics = IndexStatistics {
            distinct_non_null_keys: u64::from_le_bytes(
                chunk[16..24]
                    .try_into()
                    .map_err(|_| IndexError::Truncated)?,
            ),
            null_count: u64::from_le_bytes(
                chunk[24..32]
                    .try_into()
                    .map_err(|_| IndexError::Truncated)?,
            ),
            tree_height: u32::from_le_bytes(
                chunk[32..36]
                    .try_into()
                    .map_err(|_| IndexError::Truncated)?,
            ),
        };
        let statistics = if statistics_present {
            validate_index_statistics_intrinsic(&raw_statistics)?;
            if let Some(table_statistics) = table_statistics.as_ref() {
                validate_index_statistics(table_statistics, &raw_statistics)?;
            }
            Some(raw_statistics)
        } else {
            if raw_statistics.distinct_non_null_keys != 0
                || raw_statistics.null_count != 0
                || raw_statistics.tree_height != 0
            {
                return Err(IndexError::InvalidReservedBytes);
            }
            None
        };
        offset = end;
        let name = if name_present {
            let name_end = offset
                .checked_add(name_length)
                .ok_or(IndexError::LengthOverflow)?;
            let bytes = input.get(offset..name_end).ok_or(IndexError::Truncated)?;
            let value = std::str::from_utf8(bytes).map_err(|_| IndexError::InvalidUtf8)?;
            offset = name_end;
            Some(IndexName::new(value).map_err(|_| IndexError::InvalidIndexName)?)
        } else {
            None
        };
        entries.push(IndexCatalogEntry {
            retired,
            definition: IndexDefinition {
                id,
                name,
                column_id,
                handle: BTreeHandle {
                    owner: if version >= 6 {
                        match chunk[37] {
                            0 => None,
                            1 => Some(id),
                            2 if version >= 7 => Some(id),
                            _ => return Err(IndexError::InvalidReservedBytes),
                        }
                    } else {
                        None
                    },
                    meta_page: if version >= 7 && chunk[37] == 2 {
                        let generation = PageGeneration(u64::from_le_bytes(
                            chunk[48..56]
                                .try_into()
                                .map_err(|_| IndexError::Truncated)?,
                        ));
                        validate_generation(generation)?;
                        BTreePageRef::Allocated(PageRef {
                            page_id: meta_page,
                            generation,
                        })
                    } else {
                        if version >= 7 && chunk[48..56].iter().any(|b| *b != 0) {
                            return Err(IndexError::InvalidReservedBytes);
                        }
                        BTreePageRef::Legacy(meta_page)
                    },
                },
            },
            statistics,
        });
    }
    let pending_count = if version >= 6 {
        u32::from_le_bytes(
            input[20..24]
                .try_into()
                .map_err(|_| IndexError::Truncated)?,
        ) as usize
    } else {
        0
    };
    let pending_size = if version >= 7 {
        INDEX_CATALOG_PENDING_SIZE
    } else {
        16
    };
    if pending_count > input.len().saturating_sub(offset) / pending_size {
        return Err(IndexError::Truncated);
    }
    let mut pending = Vec::with_capacity(pending_count);
    for _ in 0..pending_count {
        let mut decoder = Decoder::new(&input[offset..offset + pending_size]);
        let index_id = IndexId(decoder.u64()?);
        let page_id = PageId(decoder.u64()?);
        let raw = if version >= 7 { decoder.u64()? } else { 0 };
        let tag = if version >= 7 {
            let tag = decoder.u8()?;
            if decoder.array::<7>()? != [0; 7] {
                return Err(IndexError::InvalidReservedBytes);
            }
            tag
        } else {
            0
        };
        let meta_page = match tag {
            0 if raw == 0 => Some(BTreePageRef::Legacy(page_id)),
            1 => {
                let generation = PageGeneration(raw);
                validate_generation(generation)?;
                Some(BTreePageRef::Allocated(PageRef {
                    page_id,
                    generation,
                }))
            }
            2 if version >= 9 && page_id.0 == 0 && raw == 0 => None,
            _ => return Err(IndexError::InvalidReservedBytes),
        };
        pending.push(RetiredIndexOwnership {
            index_id,
            meta_page,
        });
        offset += pending_size;
    }
    validate_pending_ownership(next_index_id, &entries, &pending)?;
    let reclaim_intent = if version >= 8 && input[7] == 2 {
        let mut decoder = Decoder::new(&input[offset..]);
        let old_page_count = decoder.u64()?;
        let truncate_from = decoder.u64()?;
        let checkpoint_lsn = netbadb_types::Lsn(decoder.u64()?);
        let count = decoder.u32()? as usize;
        if decoder.array::<4>()? != [0; 4] {
            return Err(IndexError::InvalidReservedBytes);
        }
        if count > input.len().saturating_sub(offset + 32) / 24 {
            return Err(IndexError::Truncated);
        }
        let mut covered = Vec::with_capacity(count);
        for _ in 0..count {
            let index_id = IndexId(decoder.u64()?);
            let page_id = PageId(decoder.u64()?);
            let generation = PageGeneration(decoder.u64()?);
            let meta_page = if version >= 9 && page_id.0 == 0 && generation.0 == 0 {
                None
            } else {
                Some(BTreePageRef::Allocated(PageRef {
                    page_id,
                    generation,
                }))
            };
            covered.push(RetiredIndexOwnership {
                index_id,
                meta_page,
            });
        }
        offset += 32 + count * 24;
        let intent = TailReclaimIntent {
            old_page_count,
            truncate_from,
            checkpoint_lsn,
            covered,
        };
        intent.validate()?;
        Some(intent)
    } else {
        None
    };
    if offset != input.len() {
        return Err(IndexError::ExtraBytes);
    }
    validate_catalog_entries(&entries)?;
    validate_index_high_water(next_index_id, &entries)?;
    let node = IndexCatalogNode {
        reclaim_intent,
        pending,
        next_index_id,
        next_catalog,
        table_statistics,
        entries,
    };
    validate_local_reclaim_intent(&node)?;
    Ok(node)
}

fn validate_local_reclaim_intent(node: &IndexCatalogNode) -> Result<(), IndexError> {
    if let Some(intent) = &node.reclaim_intent {
        intent.validate()?;
        let next = node.next_index_id.ok_or(IndexError::InvalidReclaimIntent)?;
        for record in &intent.covered {
            if record.index_id >= next {
                return Err(IndexError::InvalidReclaimIntent);
            }
            if let Some(entry) = node
                .entries
                .iter()
                .find(|entry| entry.definition.id == record.index_id)
            {
                if !entry.retired
                    || entry.definition.handle.owner != Some(record.index_id)
                    || Some(entry.definition.handle.meta_page) != record.meta_page
                {
                    return Err(IndexError::InvalidReclaimIntent);
                }
            }
            if let Some(pending) = node
                .pending
                .iter()
                .find(|pending| pending.index_id == record.index_id)
            {
                if pending != record {
                    return Err(IndexError::InvalidReclaimIntent);
                }
            }
        }
    }
    Ok(())
}

/// Checks a root allocator boundary against all supplied registrations.
/// Storage supplies the complete chain; codecs can validate only one page.
pub fn validate_index_high_water(
    next: Option<IndexId>,
    entries: &[IndexCatalogEntry],
) -> Result<(), IndexError> {
    if let Some(next) = next {
        if next.0 == 0 || entries.iter().any(|entry| entry.definition.id.0 >= next.0) {
            return Err(IndexError::InvalidIndexHighWater(next));
        }
    }
    Ok(())
}

/// Checks minimal pending records against the entire catalog identity domain.
pub fn validate_pending_ownership(
    next: Option<IndexId>,
    entries: &[IndexCatalogEntry],
    pending: &[RetiredIndexOwnership],
) -> Result<(), IndexError> {
    validate_index_high_water(next, entries)?;
    for (position, record) in pending.iter().enumerate() {
        if record.index_id.0 == 0 {
            return Err(IndexError::InvalidIndexId(record.index_id));
        }
        if let Some(page) = record.meta_page {
            validate_child(page)?;
        }
        if let Some(next) = next {
            if record.index_id.0 >= next.0 {
                return Err(IndexError::InvalidIndexHighWater(next));
            }
        }
        if entries
            .iter()
            .any(|entry| entry.definition.id == record.index_id)
            || pending[..position]
                .iter()
                .any(|other| other.index_id == record.index_id)
        {
            return Err(IndexError::DuplicateIndexId(record.index_id));
        }
        if let Some(page) = record.meta_page {
            if entries
                .iter()
                .any(|entry| entry.definition.handle.meta_page.page_id() == page.page_id())
                || pending[..position].iter().any(|other| {
                    other
                        .meta_page
                        .is_some_and(|other| other.page_id() == page.page_id())
                })
            {
                return Err(IndexError::DuplicateTreeHandle(page.page_id()));
            }
        }
    }
    Ok(())
}

fn decode_index_name_presence(value: u8) -> Result<bool, IndexError> {
    match value {
        0 => Ok(false),
        1 => Ok(true),
        other => Err(IndexError::InvalidIndexNamePresence(other)),
    }
}

fn decode_statistics_presence(value: u8) -> Result<bool, IndexError> {
    match value {
        0 => Ok(false),
        1 => Ok(true),
        other => Err(IndexError::InvalidStatisticsPresence(other)),
    }
}

/// Validates the table-level invariants of one persisted optimizer snapshot.
pub fn validate_table_statistics(statistics: &TableStatistics) -> Result<(), IndexError> {
    if statistics.managed_page_count == 0 {
        return Err(IndexError::InvalidManagedPageCount(0));
    }
    Ok(())
}

/// Validates one index snapshot against the table snapshot from the root page.
pub fn validate_index_statistics(
    table: &TableStatistics,
    index: &IndexStatistics,
) -> Result<(), IndexError> {
    validate_table_statistics(table)?;
    validate_index_statistics_intrinsic(index)?;
    if index.null_count > table.row_count {
        return Err(IndexError::InvalidNullCount {
            null_count: index.null_count,
            row_count: table.row_count,
        });
    }
    let non_null_rows = table.row_count - index.null_count;
    if index.distinct_non_null_keys > non_null_rows
        || (non_null_rows == 0 && index.distinct_non_null_keys != 0)
        || (non_null_rows > 0 && index.distinct_non_null_keys == 0)
    {
        return Err(IndexError::InvalidDistinctCount {
            distinct_non_null_keys: index.distinct_non_null_keys,
            non_null_rows,
        });
    }
    Ok(())
}

/// Validates that an index snapshot belongs to a catalog with table statistics.
pub fn validate_catalog_index_statistics(
    table: Option<&TableStatistics>,
    index: &IndexStatistics,
) -> Result<(), IndexError> {
    let table = table.ok_or(IndexError::MissingTableStatistics)?;
    validate_index_statistics(table, index)
}

fn validate_index_statistics_intrinsic(statistics: &IndexStatistics) -> Result<(), IndexError> {
    if statistics.tree_height == 0 {
        return Err(IndexError::InvalidHeight(0));
    }
    Ok(())
}

/// Validates registration identity, active uniqueness, and retired ownership.
/// Storage calls this on the complete chain as well as codec-local pages.
pub fn validate_catalog_entries(entries: &[IndexCatalogEntry]) -> Result<(), IndexError> {
    for (position, entry) in entries.iter().enumerate() {
        let definition = &entry.definition;
        validate_child(definition.handle.meta_page)?;
        if definition.handle.meta_page.generation().is_some() && definition.handle.owner.is_none() {
            return Err(IndexError::InvalidIndexId(definition.id));
        }
        if definition.handle.owner.is_some() {
            validate_btree_owner(Some(definition.id), definition.handle.owner)?;
        }
        if definition.id.0 == 0 {
            return Err(IndexError::InvalidIndexId(definition.id));
        }
        if entry.retired && entry.statistics.is_some() {
            return Err(IndexError::RetiredIndexStatistics(definition.id));
        }
        for existing in &entries[..position] {
            if existing.definition.id == definition.id {
                return Err(IndexError::DuplicateIndexId(definition.id));
            }
            if existing.definition.handle.meta_page.page_id()
                == definition.handle.meta_page.page_id()
            {
                return Err(IndexError::DuplicateTreeHandle(
                    definition.handle.meta_page.page_id(),
                ));
            }
            if !existing.retired && !entry.retired {
                if existing.definition.column_id == definition.column_id {
                    return Err(IndexError::DuplicateRegisteredColumn {
                        column_id: definition.column_id,
                    });
                }
                if let Some(name) = &definition.name {
                    if existing.definition.name.as_ref() == Some(name) {
                        return Err(IndexError::IndexNameAlreadyExists { name: name.clone() });
                    }
                }
            }
        }
    }
    Ok(())
}

/// Validates the versioned header only. Callers MUST fully decode the payload
/// before treating a page as a valid ownership observation.
pub fn btree_page_owner(input: &[u8]) -> Result<Option<IndexId>, IndexError> {
    let magic = input.get(..4).ok_or(IndexError::Truncated)?;
    let expected = match magic {
        b"NBTM" => META_MAGIC,
        b"NBTL" => LEAF_MAGIC,
        b"NBTI" => INTERNAL_MAGIC,
        _ => return Err(IndexError::InvalidNodeType),
    };
    Decoder::new(input).common_header(expected)
}

/// None is a strict legacy expectation, not a wildcard.
pub fn validate_btree_owner(
    expected: Option<IndexId>,
    actual: Option<IndexId>,
) -> Result<(), IndexError> {
    if expected != actual {
        return Err(IndexError::OwnerMismatch { expected, actual });
    }
    Ok(())
}

/// Reads the identity prefix; callers still decode the complete payload.
pub fn btree_page_generation(input: &[u8]) -> Result<Option<PageGeneration>, IndexError> {
    let magic = input.get(..4).ok_or(IndexError::Truncated)?;
    let expected = match magic {
        b"NBTM" => META_MAGIC,
        b"NBTL" => LEAF_MAGIC,
        b"NBTI" => INTERNAL_MAGIC,
        _ => return Err(IndexError::InvalidNodeType),
    };
    let mut decoder = Decoder::new(input);
    decoder.common_header(expected)?;
    Ok(decoder.generation)
}

pub fn validate_btree_generation(
    expected: Option<PageGeneration>,
    actual: Option<PageGeneration>,
) -> Result<(), IndexError> {
    if expected != actual {
        return Err(IndexError::GenerationMismatch { expected, actual });
    }
    Ok(())
}

fn validate_generation(generation: PageGeneration) -> Result<(), IndexError> {
    if generation.0 == 0 {
        return Err(IndexError::InvalidPageGeneration(generation));
    }
    Ok(())
}

fn common_header_generation(
    magic: &[u8; 4],
    owner: Option<IndexId>,
    generation: Option<PageGeneration>,
) -> Result<Vec<u8>, IndexError> {
    let mut output = Vec::with_capacity(24);
    output.extend_from_slice(magic);
    if generation.is_some() && owner.is_none() {
        return Err(IndexError::InvalidIndexId(IndexId(0)));
    }
    output.extend_from_slice(
        &if generation.is_some() {
            3_u16
        } else if owner.is_some() {
            2_u16
        } else {
            1_u16
        }
        .to_le_bytes(),
    );
    output.extend_from_slice(&0_u16.to_le_bytes());
    if let Some(owner) = owner {
        if owner.0 == 0 {
            return Err(IndexError::InvalidIndexId(owner));
        }
        output.extend_from_slice(&owner.0.to_le_bytes());
    }
    if let Some(generation) = generation {
        validate_generation(generation)?;
        output.extend_from_slice(&generation.0.to_le_bytes());
    }
    Ok(output)
}

fn encode_page_ref(
    output: &mut Vec<u8>,
    reference: Option<BTreePageRef>,
    generated: bool,
) -> Result<(), IndexError> {
    if let Some(reference) = reference {
        validate_child(reference)?;
        if reference.generation().is_some() != generated {
            return Err(IndexError::InvalidNodeType);
        }
    }
    output.extend_from_slice(&reference.map_or(0, |r| r.page_id().0).to_le_bytes());
    if generated {
        output.extend_from_slice(
            &reference
                .and_then(BTreePageRef::generation)
                .map_or(0, |g| g.0)
                .to_le_bytes(),
        );
    }
    Ok(())
}

impl From<PageId> for BTreePageRef {
    fn from(page: PageId) -> Self {
        Self::Legacy(page)
    }
}

/// Encoded size excluding owner/self-generation prefix, including pointer width.
pub fn leaf_encoded_len(
    spec: &IndexSpec,
    node: &LeafNode,
    generated: bool,
) -> Result<usize, IndexError> {
    validate_leaf(spec, node)?;
    if node
        .next_leaf
        .is_some_and(|p| p.generation().is_some() != generated)
    {
        return Err(IndexError::InvalidNodeType);
    }
    Ok(if generated { 8 } else { 0 } + encoded_entries_size(&node.entries)?)
}

/// Encoded size excluding owner/self-generation prefix, including pointer widths.
pub fn internal_encoded_len(spec: &IndexSpec, node: &InternalNode) -> Result<usize, IndexError> {
    validate_internal(spec, node)?;
    let width = if node.first_child.generation().is_some() {
        16
    } else {
        8
    };
    node.separators
        .iter()
        .try_fold(COMMON_HEADER_SIZE + 4 + width, |size, separator| {
            size.checked_add(encoded_entry_len(&separator.key)?)
                .and_then(|size| size.checked_add(width))
                .ok_or(IndexError::LengthOverflow)
        })
}

fn validate_meta(node: &MetaNode) -> Result<(), IndexError> {
    validate_child(node.root_page)?;
    if node.height == 0 {
        return Err(IndexError::InvalidHeight(node.height));
    }
    if node
        .spec
        .data_type
        .name
        .as_ref()
        .is_some_and(String::is_empty)
    {
        return Err(IndexError::InvalidSemanticNamePresence(1));
    }
    Ok(())
}

fn validate_leaf(spec: &IndexSpec, node: &LeafNode) -> Result<(), IndexError> {
    if node.next_leaf.is_some_and(|p| p.page_id().0 == 0) {
        return Err(IndexError::InvalidChild(PageId(0)));
    }
    for entry in &node.entries {
        spec.validate_key(&entry.key)?;
        validate_row_id(entry.row_id)?;
    }
    validate_entry_order(node.entries.iter())
}

fn validate_internal(spec: &IndexSpec, node: &InternalNode) -> Result<(), IndexError> {
    validate_child(node.first_child)?;
    for separator in &node.separators {
        spec.validate_key(&separator.key.key)?;
        validate_row_id(separator.key.row_id)?;
        validate_child(separator.right_child)?;
    }
    validate_internal_order(node)
}

fn validate_internal_order(node: &InternalNode) -> Result<(), IndexError> {
    validate_entry_order(node.separators.iter().map(|separator| &separator.key))
}

fn validate_entry_order<'a>(
    entries: impl Iterator<Item = &'a IndexEntryKey>,
) -> Result<(), IndexError> {
    let mut previous: Option<&IndexEntryKey> = None;
    for entry in entries {
        if previous.is_some_and(|left| compare_entry_keys(left, entry) != Ordering::Less) {
            return Err(IndexError::InvalidEntryOrder);
        }
        previous = Some(entry);
    }
    Ok(())
}

fn validate_child(page: impl Into<BTreePageRef>) -> Result<(), IndexError> {
    let reference = page.into();
    if let Some(generation) = reference.generation() {
        validate_generation(generation)?;
    }
    let page = reference.page_id();
    if page.0 == 0 {
        Err(IndexError::InvalidChild(page))
    } else {
        Ok(())
    }
}

fn validate_row_id(row_id: RowId) -> Result<(), IndexError> {
    if row_id.page.0 == 0 || row_id.generation == 0 {
        return Err(IndexError::InvalidRowId {
            page: row_id.page,
            generation: row_id.generation,
        });
    }
    Ok(())
}

fn encode_entry(output: &mut Vec<u8>, entry: &IndexEntry) -> Result<(), IndexError> {
    match &entry.key {
        ScalarValue::Null => output.push(0),
        ScalarValue::Bool(value) => {
            output.push(1);
            output.push(u8::from(*value));
        }
        ScalarValue::Int64(value) => {
            output.push(2);
            output.extend_from_slice(&value.to_le_bytes());
        }
        ScalarValue::UInt64(value) => {
            output.push(3);
            output.extend_from_slice(&value.to_le_bytes());
        }
        ScalarValue::Text(value) => {
            output.push(4);
            push_text(output, value)?;
        }
    }
    output.extend_from_slice(&entry.row_id.page.0.to_le_bytes());
    output.extend_from_slice(&entry.row_id.slot.to_le_bytes());
    output.extend_from_slice(&entry.row_id.generation.to_le_bytes());
    Ok(())
}

fn encoded_key_len(key: &ScalarValue) -> Result<usize, IndexError> {
    Ok(match key {
        ScalarValue::Null => 1,
        ScalarValue::Bool(_) => 2,
        ScalarValue::Int64(_) | ScalarValue::UInt64(_) => 9,
        ScalarValue::Text(value) => 5_usize
            .checked_add(value.len())
            .ok_or(IndexError::LengthOverflow)?,
    })
}

fn encoded_entry_len(entry: &IndexEntry) -> Result<usize, IndexError> {
    encoded_key_len(&entry.key)?
        .checked_add(ROW_ID_SIZE)
        .ok_or(IndexError::LengthOverflow)
}

fn encoded_entries_size(entries: &[IndexEntry]) -> Result<usize, IndexError> {
    entries.iter().try_fold(NODE_HEADER_SIZE, |size, entry| {
        size.checked_add(encoded_entry_len(entry)?)
            .ok_or(IndexError::LengthOverflow)
    })
}

fn push_text(output: &mut Vec<u8>, value: &str) -> Result<(), IndexError> {
    let length = u32::try_from(value.len()).map_err(|_| IndexError::LengthOverflow)?;
    output.extend_from_slice(&length.to_le_bytes());
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

const fn physical_type_tag(value: PhysicalType) -> u8 {
    match value {
        PhysicalType::Bool => 1,
        PhysicalType::Int64 => 2,
        PhysicalType::UInt64 => 3,
        PhysicalType::Text => 4,
    }
}

fn physical_type_from_tag(tag: u8) -> Result<PhysicalType, IndexError> {
    match tag {
        1 => Ok(PhysicalType::Bool),
        2 => Ok(PhysicalType::Int64),
        3 => Ok(PhysicalType::UInt64),
        4 => Ok(PhysicalType::Text),
        other => Err(IndexError::InvalidPhysicalType(other)),
    }
}

const fn value_rank(value: &ScalarValue) -> u8 {
    match value {
        ScalarValue::Null => 0,
        ScalarValue::Bool(_) => 1,
        ScalarValue::Int64(_) => 2,
        ScalarValue::UInt64(_) => 3,
        ScalarValue::Text(_) => 4,
    }
}

fn decode_bool_flag(value: u8) -> Result<bool, u8> {
    match value {
        0 => Ok(false),
        1 => Ok(true),
        other => Err(other),
    }
}

struct Decoder<'a> {
    generation: Option<PageGeneration>,
    input: &'a [u8],
    offset: usize,
}

impl<'a> Decoder<'a> {
    fn new(input: &'a [u8]) -> Self {
        Self {
            input,
            offset: 0,
            generation: None,
        }
    }

    fn common_header(&mut self, expected: &[u8; 4]) -> Result<Option<IndexId>, IndexError> {
        let actual = self.array::<4>()?;
        if &actual != expected {
            return Err(IndexError::InvalidMagic {
                expected: *expected,
                actual,
            });
        }
        let version = self.u16()?;
        if !matches!(version, 1 | 2 | BTREE_FORMAT_VERSION) {
            return Err(IndexError::UnsupportedVersion(version));
        }
        if self.u16()? != 0 {
            return Err(IndexError::InvalidReservedBytes);
        }
        if version == 1 {
            return Ok(None);
        }
        let owner = IndexId(self.u64()?);
        if owner.0 == 0 {
            return Err(IndexError::InvalidIndexId(owner));
        }
        if version >= 3 {
            let generation = PageGeneration(self.u64()?);
            validate_generation(generation)?;
            self.generation = Some(generation);
        }
        Ok(Some(owner))
    }

    fn page_ref(&mut self, generated: bool) -> Result<Option<BTreePageRef>, IndexError> {
        let page_id = PageId(self.u64()?);
        let raw_generation = if generated { self.u64()? } else { 0 };
        if page_id.0 == 0 {
            if raw_generation != 0 {
                return Err(IndexError::InvalidChild(page_id));
            }
            return Ok(None);
        }
        if generated {
            let generation = PageGeneration(raw_generation);
            validate_generation(generation)?;
            Ok(Some(BTreePageRef::Allocated(PageRef {
                page_id,
                generation,
            })))
        } else {
            Ok(Some(BTreePageRef::Legacy(page_id)))
        }
    }

    fn count(&mut self, minimum_size: usize) -> Result<usize, IndexError> {
        let bytes = self
            .input
            .get(self.offset..self.offset + 4)
            .ok_or(IndexError::Truncated)?;
        let raw = u32::from_le_bytes(bytes.try_into().map_err(|_| IndexError::Truncated)?) as usize;
        self.offset += 4;
        let remaining_after_header = self.input.len().saturating_sub(self.offset + 8);
        if raw > remaining_after_header / minimum_size {
            return Err(IndexError::Truncated);
        }
        Ok(raw)
    }

    fn entry(&mut self, spec: &IndexSpec) -> Result<IndexEntry, IndexError> {
        let key = match self.u8()? {
            0 => ScalarValue::Null,
            1 => match self.u8()? {
                0 => ScalarValue::Bool(false),
                1 => ScalarValue::Bool(true),
                other => return Err(IndexError::InvalidBoolean(other)),
            },
            2 => ScalarValue::Int64(i64::from_le_bytes(self.array()?)),
            3 => ScalarValue::UInt64(u64::from_le_bytes(self.array()?)),
            4 => ScalarValue::Text(self.text()?),
            other => return Err(IndexError::InvalidValueTag(other)),
        };
        spec.validate_key(&key)?;
        let row_id = RowId {
            page: PageId(self.u64()?),
            slot: self.u16()?,
            generation: self.u32()?,
        };
        validate_row_id(row_id)?;
        Ok(IndexEntry { key, row_id })
    }

    fn text(&mut self) -> Result<String, IndexError> {
        let length = self.u32()? as usize;
        let end = self
            .offset
            .checked_add(length)
            .ok_or(IndexError::LengthOverflow)?;
        let bytes = self
            .input
            .get(self.offset..end)
            .ok_or(IndexError::Truncated)?;
        self.offset = end;
        std::str::from_utf8(bytes)
            .map(str::to_owned)
            .map_err(|_| IndexError::InvalidUtf8)
    }

    fn u8(&mut self) -> Result<u8, IndexError> {
        Ok(self.array::<1>()?[0])
    }
    fn u16(&mut self) -> Result<u16, IndexError> {
        Ok(u16::from_le_bytes(self.array()?))
    }
    fn u32(&mut self) -> Result<u32, IndexError> {
        Ok(u32::from_le_bytes(self.array()?))
    }
    fn u64(&mut self) -> Result<u64, IndexError> {
        Ok(u64::from_le_bytes(self.array()?))
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], IndexError> {
        let end = self
            .offset
            .checked_add(N)
            .ok_or(IndexError::LengthOverflow)?;
        let bytes = self
            .input
            .get(self.offset..end)
            .ok_or(IndexError::Truncated)?;
        self.offset = end;
        bytes.try_into().map_err(|_| IndexError::Truncated)
    }

    fn finish(self) -> Result<(), IndexError> {
        if self.offset == self.input.len() {
            Ok(())
        } else {
            Err(IndexError::ExtraBytes)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(physical: PhysicalType, nullable: bool) -> IndexSpec {
        IndexSpec {
            data_type: SemanticType::physical(physical),
            nullable,
        }
    }

    #[test]
    fn index_range_validates_typed_bounds_and_accepts_empty_ranges() {
        let nullable = spec(PhysicalType::Int64, true);
        let empty = IndexRange {
            lower: IndexBound::Excluded(ScalarValue::Int64(10)),
            upper: IndexBound::Included(ScalarValue::Int64(10)),
        };
        empty.validate(&nullable).expect("valid typed range");
        assert!(empty.is_empty());
        let singleton = IndexRange {
            lower: IndexBound::Included(ScalarValue::Int64(10)),
            upper: IndexBound::Included(ScalarValue::Int64(10)),
        };
        assert!(!singleton.is_empty());
        assert!(
            IndexRange {
                lower: IndexBound::Included(ScalarValue::UInt64(10)),
                upper: IndexBound::Unbounded,
            }
            .validate(&nullable)
            .is_err()
        );
        IndexRange {
            lower: IndexBound::Included(ScalarValue::Null),
            upper: IndexBound::Excluded(ScalarValue::Int64(10)),
        }
        .validate(&nullable)
        .expect("nullable NULL bound");
        assert!(matches!(
            IndexRange {
                lower: IndexBound::Included(ScalarValue::Null),
                upper: IndexBound::Unbounded,
            }
            .validate(&spec(PhysicalType::Int64, false)),
            Err(IndexError::NullNotAllowed)
        ));
    }

    fn entry(key: ScalarValue, page: u64, slot: u16) -> IndexEntry {
        IndexEntry {
            key,
            row_id: RowId {
                page: PageId(page),
                slot,
                generation: 1,
            },
        }
    }

    #[test]
    fn codecs_round_trip_all_node_kinds_and_semantic_identity() {
        let named = IndexSpec {
            data_type: SemanticType::named("UserId", PhysicalType::UInt64),
            nullable: true,
        };
        let meta = MetaNode {
            generation: None,
            owner: None,
            root_page: crate::BTreePageRef::Legacy(PageId(9)),
            height: 3,
            spec: named.clone(),
        };
        assert_eq!(decode_meta(&encode_meta(&meta).unwrap()).unwrap(), meta);
        let leaf = LeafNode {
            entries: vec![
                entry(ScalarValue::Null, 1, 0),
                entry(ScalarValue::UInt64(7), 2, 0),
            ],
            next_leaf: Some(crate::BTreePageRef::Legacy(PageId(11))),
        };
        assert_eq!(
            decode_leaf(&named, &encode_leaf(&named, &leaf).unwrap()).unwrap(),
            leaf
        );
        let internal = InternalNode {
            first_child: crate::BTreePageRef::Legacy(PageId(5)),
            separators: vec![InternalSeparator {
                key: entry(ScalarValue::UInt64(7), 2, 0),
                right_child: crate::BTreePageRef::Legacy(PageId(6)),
            }],
        };
        assert_eq!(
            decode_internal(&named, &encode_internal(&named, &internal).unwrap()).unwrap(),
            internal
        );
    }

    #[test]
    fn leaf_codec_round_trips_every_supported_scalar_key() {
        for (physical, key) in [
            (PhysicalType::Bool, ScalarValue::Bool(true)),
            (PhysicalType::Int64, ScalarValue::Int64(-42)),
            (PhysicalType::UInt64, ScalarValue::UInt64(42)),
            (PhysicalType::Text, ScalarValue::Text("用户".into())),
        ] {
            let spec = spec(physical, false);
            let leaf = LeafNode {
                entries: vec![entry(key, 1, 0)],
                next_leaf: None,
            };
            assert_eq!(
                decode_leaf(&spec, &encode_leaf(&spec, &leaf).unwrap()).unwrap(),
                leaf
            );
        }
    }

    #[test]
    fn ordering_is_typed_numeric_text_null_then_explicit_row_id() {
        let mut signed = vec![
            ScalarValue::Int64(100),
            ScalarValue::Int64(-1),
            ScalarValue::Int64(0),
            ScalarValue::Int64(-100),
            ScalarValue::Int64(1),
        ];
        signed.sort_by(compare_values);
        assert_eq!(
            signed,
            vec![
                ScalarValue::Int64(-100),
                ScalarValue::Int64(-1),
                ScalarValue::Int64(0),
                ScalarValue::Int64(1),
                ScalarValue::Int64(100)
            ]
        );
        let mut text = ["用户", "b", "aa", "a"];
        text.sort();
        let mut values = text
            .iter()
            .rev()
            .map(|value| ScalarValue::Text((*value).into()))
            .collect::<Vec<_>>();
        values.sort_by(compare_values);
        assert_eq!(
            values,
            vec![
                ScalarValue::Text("a".into()),
                ScalarValue::Text("aa".into()),
                ScalarValue::Text("b".into()),
                ScalarValue::Text("用户".into())
            ]
        );
        assert_eq!(
            compare_values(&ScalarValue::Null, &ScalarValue::Int64(-100)),
            Ordering::Less
        );
        assert_eq!(
            compare_row_ids(
                RowId {
                    page: PageId(1),
                    slot: 9,
                    generation: 1
                },
                RowId {
                    page: PageId(2),
                    slot: 0,
                    generation: 1
                }
            ),
            Ordering::Less
        );
    }

    #[test]
    fn leaf_insert_supports_duplicates_but_rejects_exact_entry() {
        let spec = spec(PhysicalType::UInt64, false);
        let mut leaf = LeafNode::empty();
        let first = entry(ScalarValue::UInt64(42), 2, 0);
        let second = entry(ScalarValue::UInt64(42), 1, 0);
        leaf.insert(&spec, first.clone()).unwrap();
        leaf.insert(&spec, second.clone()).unwrap();
        assert_eq!(leaf.entries, vec![second, first.clone()]);
        assert_eq!(leaf.insert(&spec, first), Err(IndexError::DuplicateEntry));
    }

    #[test]
    fn exact_remove_preserves_duplicate_user_keys_and_reports_missing() {
        let spec = spec(PhysicalType::UInt64, false);
        let mut leaf = LeafNode::empty();
        let first = entry(ScalarValue::UInt64(42), 1, 0);
        let middle = entry(ScalarValue::UInt64(42), 2, 0);
        let last = entry(ScalarValue::UInt64(42), 3, 0);
        for item in [&last, &first, &middle] {
            leaf.insert(&spec, item.clone()).unwrap();
        }
        leaf.remove(&spec, &middle).unwrap();
        assert_eq!(leaf.entries, vec![first, last]);
        assert_eq!(leaf.remove(&spec, &middle), Err(IndexError::EntryNotFound));
    }

    #[test]
    fn byte_aware_merge_helpers_validate_order_and_capacity() {
        let spec = spec(PhysicalType::Text, false);
        let left = LeafNode {
            entries: vec![entry(ScalarValue::Text("a".into()), 1, 0)],
            next_leaf: Some(crate::BTreePageRef::Legacy(PageId(9))),
        };
        let right = LeafNode {
            entries: vec![entry(ScalarValue::Text("b".into()), 2, 0)],
            next_leaf: Some(crate::BTreePageRef::Legacy(PageId(10))),
        };
        let merged = merge_leaves_if_fits(&spec, &left, &right, 128)
            .unwrap()
            .unwrap();
        assert_eq!(
            merged.next_leaf,
            Some(crate::BTreePageRef::Legacy(PageId(10)))
        );
        assert_eq!(merged.entries.len(), 2);
        assert!(
            merge_leaves_if_fits(&spec, &left, &right, 32)
                .unwrap()
                .is_none()
        );

        let left_internal = InternalNode {
            first_child: crate::BTreePageRef::Legacy(PageId(1)),
            separators: vec![],
        };
        let right_internal = InternalNode {
            first_child: crate::BTreePageRef::Legacy(PageId(2)),
            separators: vec![],
        };
        let fence = entry(ScalarValue::Text("b".into()), 2, 0);
        let merged = merge_internals_if_fits(&spec, &left_internal, &fence, &right_internal, 128)
            .unwrap()
            .unwrap();
        assert_eq!(merged.first_child, crate::BTreePageRef::Legacy(PageId(1)));
        assert_eq!(merged.separators[0].key, fence);
        assert_eq!(
            merged.separators[0].right_child,
            crate::BTreePageRef::Legacy(PageId(2))
        );
    }

    #[test]
    fn catalog_retirement_codec_validates_state_identity_and_statistics() {
        let mut node = IndexCatalogNode::empty();
        node.next_index_id = Some(IndexId(100));
        node.entries.push(IndexCatalogEntry {
            retired: true,
            definition: IndexDefinition {
                id: IndexId(17),
                name: Some(IndexName::new("old").unwrap()),
                column_id: ColumnId(1),
                handle: BTreeHandle {
                    owner: None,
                    meta_page: crate::BTreePageRef::Legacy(PageId(9)),
                },
            },
            statistics: None,
        });
        let bytes = encode_index_catalog(&node).unwrap();
        assert_eq!(&bytes[84..96], &[1, 0, 0, 0, 17, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(decode_index_catalog(&bytes).unwrap(), node);
        for end in 0..bytes.len() {
            assert!(decode_index_catalog(&bytes[..end]).is_err());
        }
        let mut invalid = bytes.clone();
        invalid[84] = 2;
        assert_eq!(
            decode_index_catalog(&invalid),
            Err(IndexError::InvalidIndexState(2))
        );
        let mut invalid = bytes.clone();
        invalid[88..96].fill(0);
        assert_eq!(
            decode_index_catalog(&invalid),
            Err(IndexError::InvalidIndexId(IndexId(0)))
        );
        let mut extra = bytes;
        extra.push(0);
        assert_eq!(decode_index_catalog(&extra), Err(IndexError::ExtraBytes));
        node.entries[0].statistics = Some(IndexStatistics {
            distinct_non_null_keys: 0,
            null_count: 0,
            tree_height: 1,
        });
        assert_eq!(
            encode_index_catalog(&node),
            Err(IndexError::RetiredIndexStatistics(IndexId(17)))
        );
        node.entries[0].statistics = None;
        let mut fresh = node.entries[0].clone();
        fresh.retired = false;
        fresh.definition.id = IndexId(18);
        fresh.definition.handle.meta_page = crate::BTreePageRef::Legacy(PageId(19));
        node.entries.push(fresh.clone());
        assert_eq!(
            decode_index_catalog(&encode_index_catalog(&node).unwrap()).unwrap(),
            node
        );
        fresh.definition.id = IndexId(20);
        fresh.definition.handle.meta_page = crate::BTreePageRef::Legacy(PageId(21));
        node.entries.push(fresh);
        assert!(matches!(
            encode_index_catalog(&node),
            Err(IndexError::DuplicateRegisteredColumn { .. })
        ));
        node.entries[2].retired = true;
        node.entries[2].definition.id = IndexId(17);
        assert_eq!(
            encode_index_catalog(&node),
            Err(IndexError::DuplicateIndexId(IndexId(17)))
        );
    }

    #[test]
    fn index_catalog_codec_round_trips_and_rejects_corruption() {
        let node = IndexCatalogNode {
            reclaim_intent: None,
            pending: Vec::new(),
            next_index_id: Some(IndexId(12)),
            next_catalog: Some(PageId(9)),
            table_statistics: Some(TableStatistics {
                row_count: 10,
                managed_page_count: 4,
            }),
            entries: vec![IndexCatalogEntry {
                retired: false,
                definition: IndexDefinition {
                    id: IndexId(11),
                    name: None,
                    column_id: ColumnId(7),
                    handle: BTreeHandle {
                        owner: None,
                        meta_page: crate::BTreePageRef::Legacy(PageId(11)),
                    },
                },
                statistics: Some(IndexStatistics {
                    distinct_non_null_keys: 8,
                    null_count: 2,
                    tree_height: 2,
                }),
            }],
        };
        let bytes = encode_index_catalog(&node).unwrap();
        assert_eq!(decode_index_catalog(&bytes).unwrap(), node);
        let mut legacy = bytes.clone();
        legacy[4..6].copy_from_slice(&LEGACY_INDEX_CATALOG_FORMAT_VERSION.to_le_bytes());
        legacy[7] = 0;
        legacy[40..48].fill(0);
        legacy.drain(88..104);
        let mut legacy_node = node.clone();
        legacy_node.next_index_id = None;
        assert_eq!(decode_index_catalog(&legacy).unwrap(), legacy_node);
        let mut named = node.clone();
        named.entries[0].definition.name = Some(IndexName::new("users_name_idx").unwrap());
        let named_bytes = encode_index_catalog(&named).unwrap();
        let named_golden = [
            b"NBIC".as_slice(),
            &[9, 0, 1, 1],
            &[9, 0, 0, 0, 0, 0, 0, 0],
            &[1, 0, 0, 0],
            &[0, 0, 0, 0],
            &[10, 0, 0, 0, 0, 0, 0, 0],
            &[4, 0, 0, 0, 0, 0, 0, 0],
            &[12, 0, 0, 0, 0, 0, 0, 0],
            &[7, 0, 0, 0],
            &[1, 1, 14, 0],
            &[11, 0, 0, 0, 0, 0, 0, 0],
            &[8, 0, 0, 0, 0, 0, 0, 0],
            &[2, 0, 0, 0, 0, 0, 0, 0],
            &[2, 0, 0, 0],
            &[0, 0, 0, 0],
            &[11, 0, 0, 0, 0, 0, 0, 0],
            &[0; 8],
            b"users_name_idx".as_slice(),
        ]
        .concat();
        let mut v3_named = named_golden.clone();
        v3_named[4..6].copy_from_slice(&3_u16.to_le_bytes());
        v3_named[7] = 0;
        v3_named[40..48].fill(0);
        v3_named.drain(88..104);
        let mut legacy_named = named.clone();
        legacy_named.next_index_id = None;
        assert_eq!(decode_index_catalog(&v3_named).unwrap(), legacy_named);
        assert_eq!(named_bytes, named_golden);
        assert_eq!(decode_index_catalog(&named_bytes).unwrap(), named);
        for end in 0..bytes.len() {
            assert!(decode_index_catalog(&bytes[..end]).is_err());
        }
        let mut reserved = bytes.clone();
        reserved[20] = 1;
        assert_eq!(decode_index_catalog(&reserved), Err(IndexError::Truncated));
        let mut extra = bytes.clone();
        extra.push(0);
        assert_eq!(decode_index_catalog(&extra), Err(IndexError::ExtraBytes));
        let mut bad_magic = bytes.clone();
        bad_magic[0] = b'X';
        assert!(matches!(
            decode_index_catalog(&bad_magic),
            Err(IndexError::InvalidMagic { .. })
        ));
        let mut old_version = bytes.clone();
        old_version[4..6].copy_from_slice(&1_u16.to_le_bytes());
        assert_eq!(
            decode_index_catalog(&old_version),
            Err(IndexError::UnsupportedVersion(1))
        );
        let mut impossible_count = bytes.clone();
        impossible_count[16..20].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(matches!(
            decode_index_catalog(&impossible_count),
            Err(IndexError::Truncated | IndexError::LengthOverflow)
        ));
        let mut zero_handle = bytes.clone();
        zero_handle[56..64].fill(0);
        assert_eq!(
            decode_index_catalog(&zero_handle),
            Err(IndexError::InvalidChild(PageId(0)))
        );
        let duplicate = IndexCatalogNode {
            reclaim_intent: None,
            pending: Vec::new(),
            next_index_id: node.next_index_id,
            next_catalog: None,
            table_statistics: node.table_statistics,
            entries: vec![node.entries[0].clone(), node.entries[0].clone()],
        };
        assert_eq!(
            encode_index_catalog(&duplicate),
            Err(IndexError::DuplicateIndexId(IndexId(11)))
        );

        let mut invalid_table_flag = bytes.clone();
        invalid_table_flag[6] = 2;
        assert_eq!(
            decode_index_catalog(&invalid_table_flag),
            Err(IndexError::InvalidStatisticsPresence(2))
        );
        let mut invalid_index_flag = bytes.clone();
        invalid_index_flag[52] = 3;
        assert_eq!(
            decode_index_catalog(&invalid_index_flag),
            Err(IndexError::InvalidStatisticsPresence(3))
        );
        let mut entry_reserved = bytes.clone();
        entry_reserved[86] = 1;
        assert_eq!(
            decode_index_catalog(&entry_reserved),
            Err(IndexError::InvalidReservedBytes)
        );
        let mut invalid_name_flag = bytes.clone();
        invalid_name_flag[53] = 2;
        assert_eq!(
            decode_index_catalog(&invalid_name_flag),
            Err(IndexError::InvalidIndexNamePresence(2))
        );
        let mut invalid_name_utf8 = named_bytes.clone();
        *invalid_name_utf8.last_mut().unwrap() = 0xff;
        assert_eq!(
            decode_index_catalog(&invalid_name_utf8),
            Err(IndexError::InvalidUtf8)
        );
        let mut oversized_name = named_bytes.clone();
        oversized_name[54..56].copy_from_slice(&256_u16.to_le_bytes());
        oversized_name.resize(
            INDEX_CATALOG_HEADER_SIZE + INDEX_CATALOG_ENTRY_HEADER_SIZE + 256,
            b'x',
        );
        assert_eq!(
            decode_index_catalog(&oversized_name),
            Err(IndexError::InvalidIndexName)
        );
        let mut zero_managed_pages = bytes.clone();
        zero_managed_pages[32..40].fill(0);
        assert_eq!(
            decode_index_catalog(&zero_managed_pages),
            Err(IndexError::InvalidManagedPageCount(0))
        );
        let mut null_overflow = bytes.clone();
        null_overflow[72..80].copy_from_slice(&11_u64.to_le_bytes());
        assert_eq!(
            decode_index_catalog(&null_overflow),
            Err(IndexError::InvalidNullCount {
                null_count: 11,
                row_count: 10
            })
        );
        let mut distinct_overflow = bytes.clone();
        distinct_overflow[64..72].copy_from_slice(&9_u64.to_le_bytes());
        assert_eq!(
            decode_index_catalog(&distinct_overflow),
            Err(IndexError::InvalidDistinctCount {
                distinct_non_null_keys: 9,
                non_null_rows: 8
            })
        );
        let mut zero_distinct = bytes.clone();
        zero_distinct[64..72].fill(0);
        assert_eq!(
            decode_index_catalog(&zero_distinct),
            Err(IndexError::InvalidDistinctCount {
                distinct_non_null_keys: 0,
                non_null_rows: 8
            })
        );
        let mut zero_height = bytes.clone();
        zero_height[80..84].fill(0);
        assert_eq!(
            decode_index_catalog(&zero_height),
            Err(IndexError::InvalidHeight(0))
        );
        assert_eq!(
            validate_catalog_index_statistics(None, &node.entries[0].statistics.unwrap()),
            Err(IndexError::MissingTableStatistics)
        );
    }

    #[test]
    fn variable_width_leaf_split_finds_a_fitting_byte_boundary() {
        let spec = spec(PhysicalType::Text, false);
        let entries = vec![
            entry(ScalarValue::Text("a".repeat(70)), 1, 0),
            entry(ScalarValue::Text("b".repeat(5)), 2, 0),
            entry(ScalarValue::Text("c".repeat(60)), 3, 0),
        ];
        let (left, right, promoted) = split_leaf(
            &spec,
            entries,
            None,
            crate::BTreePageRef::Legacy(PageId(8)),
            125,
        )
        .unwrap();
        assert!(!left.entries.is_empty());
        assert!(!right.entries.is_empty());
        assert!(encode_leaf(&spec, &left).unwrap().len() <= 125);
        assert!(encode_leaf(&spec, &right).unwrap().len() <= 125);
        assert_eq!(promoted, right.entries[0]);
    }

    #[test]
    fn entry_preflight_reserves_space_for_an_internal_child_pointer() {
        let spec = spec(PhysicalType::Text, false);
        let entry = entry(ScalarValue::Text("x".repeat(82)), 1, 0);
        assert_eq!(
            ensure_entry_fits(&spec, &entry, 125),
            Err(IndexError::KeyTooLarge {
                size: 129,
                capacity: 125,
            })
        );
        assert_eq!(
            ensure_key_fits(&spec, &entry.key, 125),
            Err(IndexError::KeyTooLarge {
                size: 129,
                capacity: 125,
            })
        );
        assert_eq!(
            ensure_key_fits(&spec, &ScalarValue::UInt64(1), 125),
            Err(IndexError::TypeMismatch {
                expected: PhysicalType::Text,
                actual: Some(PhysicalType::UInt64),
            })
        );
    }

    #[test]
    fn malformed_payloads_return_typed_errors() {
        let uint_spec = spec(PhysicalType::UInt64, false);
        let valid = encode_leaf(
            &uint_spec,
            &LeafNode {
                entries: vec![entry(ScalarValue::UInt64(1), 1, 0)],
                next_leaf: None,
            },
        )
        .unwrap();
        let valid_meta = encode_meta(&MetaNode {
            generation: None,
            owner: None,
            root_page: crate::BTreePageRef::Legacy(PageId(1)),
            height: 1,
            spec: uint_spec.clone(),
        })
        .unwrap();
        let valid_internal = encode_internal(
            &uint_spec,
            &InternalNode {
                first_child: crate::BTreePageRef::Legacy(PageId(1)),
                separators: vec![InternalSeparator {
                    key: entry(ScalarValue::UInt64(1), 1, 0),
                    right_child: crate::BTreePageRef::Legacy(PageId(2)),
                }],
            },
        )
        .unwrap();
        for end in 0..valid_meta.len() {
            assert!(decode_meta(&valid_meta[..end]).is_err());
        }
        for end in 0..valid.len() {
            assert!(decode_leaf(&uint_spec, &valid[..end]).is_err());
        }
        for end in 0..valid_internal.len() {
            assert!(decode_internal(&uint_spec, &valid_internal[..end]).is_err());
        }
        assert!(matches!(
            decode_meta(&with_byte(&valid_meta, 0, b'X')),
            Err(IndexError::InvalidMagic { .. })
        ));
        assert!(matches!(
            decode_leaf(&uint_spec, &with_byte(&valid, 0, b'X')),
            Err(IndexError::InvalidMagic { .. })
        ));
        assert!(matches!(
            decode_internal(&uint_spec, &with_byte(&valid_internal, 0, b'X')),
            Err(IndexError::InvalidMagic { .. })
        ));
        let mut bad_magic = valid.clone();
        bad_magic[0] = b'X';
        assert!(matches!(
            decode_leaf(&uint_spec, &bad_magic),
            Err(IndexError::InvalidMagic { .. })
        ));
        let mut old = valid.clone();
        old[4..6].copy_from_slice(&9_u16.to_le_bytes());
        assert_eq!(
            decode_leaf(&uint_spec, &old),
            Err(IndexError::UnsupportedVersion(9))
        );
        let mut reserved = valid.clone();
        reserved[6] = 1;
        assert_eq!(
            decode_leaf(&uint_spec, &reserved),
            Err(IndexError::InvalidReservedBytes)
        );
        let mut zero_generation = valid.clone();
        let last = zero_generation.len();
        zero_generation[last - 4..].fill(0);
        assert!(matches!(
            decode_leaf(&uint_spec, &zero_generation),
            Err(IndexError::InvalidRowId { generation: 0, .. })
        ));
        let mut zero_child = encode_internal(
            &uint_spec,
            &InternalNode {
                first_child: crate::BTreePageRef::Legacy(PageId(1)),
                separators: vec![],
            },
        )
        .unwrap();
        zero_child[12..20].fill(0);
        assert_eq!(
            decode_internal(&uint_spec, &zero_child),
            Err(IndexError::InvalidChild(PageId(0)))
        );

        let mut invalid_type = encode_meta(&MetaNode {
            generation: None,
            owner: None,
            root_page: crate::BTreePageRef::Legacy(PageId(1)),
            height: 1,
            spec: uint_spec.clone(),
        })
        .unwrap();
        invalid_type[20] = 99;
        assert_eq!(
            decode_meta(&invalid_type),
            Err(IndexError::InvalidPhysicalType(99))
        );

        let mut invalid_utf8 = encode_leaf(
            &spec(PhysicalType::Text, false),
            &LeafNode {
                entries: vec![entry(ScalarValue::Text("x".into()), 1, 0)],
                next_leaf: None,
            },
        )
        .unwrap();
        invalid_utf8[25] = 0xff;
        assert_eq!(
            decode_leaf(&spec(PhysicalType::Text, false), &invalid_utf8),
            Err(IndexError::InvalidUtf8)
        );

        let mut unsorted = encode_leaf(
            &uint_spec,
            &LeafNode {
                entries: vec![
                    entry(ScalarValue::UInt64(1), 1, 0),
                    entry(ScalarValue::UInt64(2), 2, 0),
                ],
                next_leaf: None,
            },
        )
        .unwrap();
        let first_key = 21;
        let second_key = first_key + 23;
        unsorted[first_key..first_key + 8].copy_from_slice(&2_u64.to_le_bytes());
        unsorted[second_key..second_key + 8].copy_from_slice(&1_u64.to_le_bytes());
        assert_eq!(
            decode_leaf(&uint_spec, &unsorted),
            Err(IndexError::InvalidEntryOrder)
        );

        let mut duplicate_separator = encode_internal(
            &uint_spec,
            &InternalNode {
                first_child: crate::BTreePageRef::Legacy(PageId(1)),
                separators: vec![
                    InternalSeparator {
                        key: entry(ScalarValue::UInt64(1), 1, 0),
                        right_child: crate::BTreePageRef::Legacy(PageId(2)),
                    },
                    InternalSeparator {
                        key: entry(ScalarValue::UInt64(2), 2, 0),
                        right_child: crate::BTreePageRef::Legacy(PageId(3)),
                    },
                ],
            },
        )
        .unwrap();
        let first_separator = 20;
        let second_separator = first_separator + 31;
        let first_entry = duplicate_separator[first_separator..first_separator + 23].to_vec();
        duplicate_separator[second_separator..second_separator + 23].copy_from_slice(&first_entry);
        assert_eq!(
            decode_internal(&uint_spec, &duplicate_separator),
            Err(IndexError::InvalidEntryOrder)
        );
    }

    fn with_byte(input: &[u8], offset: usize, byte: u8) -> Vec<u8> {
        let mut output = input.to_vec();
        output[offset] = byte;
        output
    }

    #[test]
    fn spec_rejects_null_and_physical_mismatch() {
        let nonnull = spec(PhysicalType::UInt64, false);
        assert_eq!(
            nonnull.validate_key(&ScalarValue::Null),
            Err(IndexError::NullNotAllowed)
        );
        assert!(matches!(
            nonnull.validate_key(&ScalarValue::Int64(1)),
            Err(IndexError::TypeMismatch { .. })
        ));
        let nullable = spec(PhysicalType::UInt64, true);
        assert!(nullable.validate_key(&ScalarValue::Null).is_ok());
    }
}
#[cfg(test)]
mod ownership_tests {
    use super::*;

    #[test]
    fn v2_all_kinds_round_trip_and_reject_owner_corruption() {
        let owner = Some(IndexId(91));
        let spec = IndexSpec {
            data_type: SemanticType::physical(PhysicalType::UInt64),
            nullable: true,
        };
        let entry = IndexEntry {
            key: ScalarValue::UInt64(7),
            row_id: RowId {
                page: PageId(8),
                slot: 0,
                generation: 1,
            },
        };
        let meta = MetaNode {
            generation: None,
            owner,
            root_page: crate::BTreePageRef::Legacy(PageId(2)),
            height: 2,
            spec: spec.clone(),
        };
        let leaf = LeafNode {
            entries: vec![entry.clone()],
            next_leaf: None,
        };
        let internal = InternalNode {
            first_child: crate::BTreePageRef::Legacy(PageId(2)),
            separators: vec![InternalSeparator {
                key: entry,
                right_child: crate::BTreePageRef::Legacy(PageId(3)),
            }],
        };
        let payloads = [
            encode_meta(&meta).unwrap(),
            encode_leaf_owned(&spec, &leaf, owner).unwrap(),
            encode_internal_owned(&spec, &internal, owner).unwrap(),
        ];
        let decode = |kind, bytes: &[u8]| -> Result<(), IndexError> {
            match kind {
                0 => {
                    let node = decode_meta(bytes)?;
                    validate_btree_owner(owner, node.owner)?;
                }
                1 => {
                    decode_leaf_owned(&spec, bytes, owner)?;
                }
                _ => {
                    decode_internal_owned(&spec, bytes, owner)?;
                }
            }
            Ok(())
        };
        for (kind, bytes) in payloads.iter().enumerate() {
            assert_eq!(&bytes[4..6], &2_u16.to_le_bytes());
            assert_eq!(&bytes[8..16], &91_u64.to_le_bytes());
            decode(kind, bytes).unwrap();
            for end in 0..bytes.len() {
                assert!(decode(kind, &bytes[..end]).is_err());
            }
            let mut zero = bytes.clone();
            zero[8..16].fill(0);
            assert_eq!(
                decode(kind, &zero),
                Err(IndexError::InvalidIndexId(IndexId(0)))
            );
            let mut wrong = bytes.clone();
            wrong[8..16].copy_from_slice(&92_u64.to_le_bytes());
            assert!(matches!(
                decode(kind, &wrong),
                Err(IndexError::OwnerMismatch { .. })
            ));
            let mut extra = bytes.clone();
            extra.push(0);
            assert_eq!(decode(kind, &extra), Err(IndexError::ExtraBytes));
        }
        assert_eq!(decode_meta(&payloads[0]).unwrap(), meta);
        assert_eq!(decode_leaf_owned(&spec, &payloads[1], owner).unwrap(), leaf);
        assert_eq!(
            decode_internal_owned(&spec, &payloads[2], owner).unwrap(),
            internal
        );
        assert!(matches!(
            decode_leaf(&spec, &payloads[1]),
            Err(IndexError::OwnerMismatch { .. })
        ));
        assert!(matches!(
            decode_internal(&spec, &payloads[2]),
            Err(IndexError::OwnerMismatch { .. })
        ));
        let legacy = encode_leaf(&spec, &leaf).unwrap();
        assert_eq!(decode_leaf(&spec, &legacy).unwrap(), leaf);
        assert!(decode_leaf_owned(&spec, &legacy, owner).is_err());
    }

    #[test]
    fn v6_pending_records_are_minimal_bounded_and_disjoint() {
        let record = RetiredIndexOwnership {
            index_id: IndexId(17),
            meta_page: Some(crate::BTreePageRef::Legacy(PageId(99))),
        };
        let mut node = IndexCatalogNode::empty();
        node.next_index_id = Some(IndexId(18));
        node.pending.push(record);
        let bytes = encode_index_catalog(&node).unwrap();
        assert_eq!(bytes.len(), 80);
        assert_eq!(&bytes[20..24], &1_u32.to_le_bytes());
        assert_eq!(&bytes[48..56], &17_u64.to_le_bytes());
        assert_eq!(&bytes[56..64], &99_u64.to_le_bytes());
        assert_eq!(decode_index_catalog(&bytes).unwrap(), node);
        for end in 0..bytes.len() {
            assert!(decode_index_catalog(&bytes[..end]).is_err());
        }
        for offset in [48, 56] {
            let mut zero = bytes.clone();
            zero[offset..offset + 8].fill(0);
            assert!(decode_index_catalog(&zero).is_err());
        }
        let mut excessive = bytes.clone();
        excessive[20..24].copy_from_slice(&u32::MAX.to_le_bytes());
        assert_eq!(decode_index_catalog(&excessive), Err(IndexError::Truncated));
        let mut extra = bytes.clone();
        extra.push(0);
        assert_eq!(decode_index_catalog(&extra), Err(IndexError::ExtraBytes));
        node.pending.push(record);
        assert_eq!(
            encode_index_catalog(&node),
            Err(IndexError::DuplicateIndexId(record.index_id))
        );
        node.pending[1].index_id = IndexId(16);
        assert_eq!(
            encode_index_catalog(&node),
            Err(IndexError::DuplicateTreeHandle(
                record.meta_page.unwrap().page_id()
            ))
        );
        node.pending.pop();
        node.next_index_id = Some(IndexId(17));
        assert!(matches!(
            encode_index_catalog(&node),
            Err(IndexError::InvalidIndexHighWater(_))
        ));
        // v5's allocator must survive even after all entries have been removed.
        let mut v5 = encode_index_catalog(&IndexCatalogNode {
            reclaim_intent: None,
            next_index_id: Some(IndexId(900)),
            ..IndexCatalogNode::empty()
        })
        .unwrap();
        v5[4..6].copy_from_slice(&5_u16.to_le_bytes());
        assert_eq!(
            decode_index_catalog(&v5).unwrap().next_index_id,
            Some(IndexId(900))
        );
    }
}

#[cfg(test)]
mod generation_tests {
    use super::*;
    fn reference(id: u64, generation: u64) -> BTreePageRef {
        BTreePageRef::Allocated(PageRef {
            page_id: PageId(id),
            generation: PageGeneration(generation),
        })
    }
    #[test]
    fn v3_identity_and_all_link_codecs_reject_zero_and_truncation() {
        let spec = IndexSpec {
            data_type: SemanticType::physical(PhysicalType::UInt64),
            nullable: false,
        };
        let owner = Some(IndexId(7));
        let generation = Some(PageGeneration(41));
        let meta = MetaNode {
            owner,
            generation,
            root_page: reference(2, 42),
            height: 2,
            spec: spec.clone(),
        };
        let leaf = LeafNode {
            entries: vec![],
            next_leaf: Some(reference(3, 43)),
        };
        let internal = InternalNode {
            first_child: reference(2, 42),
            separators: vec![InternalSeparator {
                key: IndexEntryKey {
                    key: ScalarValue::UInt64(5),
                    row_id: RowId {
                        page: PageId(8),
                        slot: 0,
                        generation: 1,
                    },
                },
                right_child: reference(3, 43),
            }],
        };
        let meta_bytes = encode_meta(&meta).unwrap();
        let leaf_bytes = encode_leaf_generation(&spec, &leaf, owner, generation).unwrap();
        let internal_bytes =
            encode_internal_generation(&spec, &internal, owner, generation).unwrap();
        assert_eq!(decode_meta(&meta_bytes).unwrap(), meta);
        assert_eq!(decode_leaf_owned(&spec, &leaf_bytes, owner).unwrap(), leaf);
        assert_eq!(
            decode_internal_owned(&spec, &internal_bytes, owner).unwrap(),
            internal
        );
        for (kind, bytes, offsets) in [
            (0, meta_bytes, vec![16, 32]),
            (1, leaf_bytes, vec![16, 36]),
            (2, internal_bytes, vec![16, 36, 75]),
        ] {
            let decode = |bytes: &[u8]| match kind {
                0 => decode_meta(bytes).map(|_| ()),
                1 => decode_leaf_owned(&spec, bytes, owner).map(|_| ()),
                _ => decode_internal_owned(&spec, bytes, owner).map(|_| ()),
            };
            assert_eq!(&bytes[4..6], &3_u16.to_le_bytes());
            assert_eq!(btree_page_generation(&bytes).unwrap(), generation);
            for end in 0..bytes.len() {
                assert!(decode(&bytes[..end]).is_err(), "kind={kind} end={end}");
            }
            for offset in offsets {
                let mut corrupt = bytes.clone();
                corrupt[offset..offset + 8].fill(0);
                assert!(matches!(
                    decode(&corrupt),
                    Err(IndexError::InvalidPageGeneration(PageGeneration(0)))
                ));
            }
            let mut owner_zero = bytes.clone();
            owner_zero[8..16].fill(0);
            assert!(matches!(
                decode(&owner_zero),
                Err(IndexError::InvalidIndexId(IndexId(0)))
            ));
        }
        assert!(encode_leaf_generation(&spec, &leaf, None, generation).is_err());
        assert!(encode_internal_owned(&spec, &internal, owner).is_err());
        assert!(encode_leaf_generation(&spec, &leaf, owner, Some(PageGeneration(0))).is_err());
    }

    #[test]
    fn v7_active_pending_refs_and_v6_legacy_are_explicit() {
        let mut node = IndexCatalogNode::empty();
        node.next_index_id = Some(IndexId(3));
        node.entries.push(IndexCatalogEntry {
            retired: false,
            statistics: None,
            definition: IndexDefinition {
                id: IndexId(1),
                name: None,
                column_id: ColumnId(1),
                handle: BTreeHandle {
                    owner: Some(IndexId(1)),
                    meta_page: reference(9, 101),
                },
            },
        });
        node.pending.push(RetiredIndexOwnership {
            index_id: IndexId(2),
            meta_page: Some(reference(10, 102)),
        });
        let bytes = encode_index_catalog(&node).unwrap();
        assert_eq!(bytes.len(), 48 + 56 + 32);
        assert_eq!(decode_index_catalog(&bytes).unwrap(), node);
        for end in 0..bytes.len() {
            assert!(decode_index_catalog(&bytes[..end]).is_err());
        }
        for offset in [96, 120] {
            let mut corrupt = bytes.clone();
            corrupt[offset..offset + 8].fill(0);
            assert!(matches!(
                decode_index_catalog(&corrupt),
                Err(IndexError::InvalidPageGeneration(PageGeneration(0)))
            ));
        }
        // v6 has neither active nor pending generation, with no invented value.
        let mut legacy = bytes[..48].to_vec();
        legacy[4..6].copy_from_slice(&6_u16.to_le_bytes());
        legacy.extend_from_slice(&bytes[48..96]);
        legacy[85] = 1;
        legacy.extend_from_slice(&bytes[104..120]);
        let decoded = decode_index_catalog(&legacy).unwrap();
        assert_eq!(
            decoded.entries[0].definition.handle.meta_page,
            BTreePageRef::Legacy(PageId(9))
        );
        assert_eq!(
            decoded.pending[0].meta_page,
            Some(BTreePageRef::Legacy(PageId(10)))
        );
        assert_eq!(
            decode_index_catalog(&encode_index_catalog(&decoded).unwrap()).unwrap(),
            decoded
        );
        // Active-to-active aliases must also compare physical geometry.
        let mut alias = node.entries[0].clone();
        alias.definition.id = IndexId(3);
        alias.definition.column_id = ColumnId(2);
        alias.definition.handle.owner = Some(IndexId(3));
        alias.definition.handle.meta_page = reference(9, 999);
        node.next_index_id = Some(IndexId(4));
        node.entries.push(alias);
        assert!(matches!(
            encode_index_catalog(&node),
            Err(IndexError::DuplicateTreeHandle(PageId(9)))
        ));
        node.entries.pop();
        // Same physical slot is an alias even with a different expected generation.
        node.pending[0].meta_page = Some(reference(9, 999));
        assert!(matches!(
            encode_index_catalog(&node),
            Err(IndexError::DuplicateTreeHandle(PageId(9)))
        ));
    }
}

#[cfg(test)]
mod tail_intent_tests {
    use super::*;

    fn node() -> IndexCatalogNode {
        let record = RetiredIndexOwnership {
            index_id: IndexId(1),
            meta_page: Some(BTreePageRef::Allocated(PageRef {
                page_id: PageId(3),
                generation: PageGeneration(7),
            })),
        };
        IndexCatalogNode {
            next_index_id: Some(IndexId(2)),
            pending: vec![record],
            reclaim_intent: Some(TailReclaimIntent {
                old_page_count: 5,
                truncate_from: 3,
                checkpoint_lsn: netbadb_types::Lsn(100),
                covered: vec![record],
            }),
            ..IndexCatalogNode::empty()
        }
    }

    #[test]
    fn tail_intent_roundtrip_and_corrupt_fields_are_bounded() {
        let node = node();
        let bytes = encode_index_catalog(&node).unwrap();
        assert_eq!(bytes.len(), 48 + 32 + 32 + 24);
        assert_eq!(decode_index_catalog(&bytes).unwrap(), node);
        for length in 0..bytes.len() {
            assert!(decode_index_catalog(&bytes[..length]).is_err());
        }
        for (offset, value) in [
            (80, 0_u64),
            (88, 5),
            (88, 2),
            (96, 0),
            (112, 0),
            (120, 2),
            (128, 0),
            (128, 100),
        ] {
            let mut bad = bytes.clone();
            bad[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
            assert!(
                decode_index_catalog(&bad).is_err(),
                "offset={offset} value={value}"
            );
        }
        for value in [3, 255] {
            let mut bad = bytes.clone();
            bad[7] = value;
            assert!(decode_index_catalog(&bad).is_err());
        }
        let mut extra = bytes.clone();
        extra.push(0);
        assert_eq!(decode_index_catalog(&extra), Err(IndexError::ExtraBytes));
        let mut duplicate = node.clone();
        duplicate
            .reclaim_intent
            .as_mut()
            .unwrap()
            .covered
            .push(node.pending[0]);
        assert!(encode_index_catalog(&duplicate).is_err());
        let mut legacy = node.clone();
        legacy.pending[0].meta_page = Some(BTreePageRef::Legacy(PageId(3)));
        assert!(encode_index_catalog(&legacy).is_err());
        let mut active = node.clone();
        active.pending.clear();
        active.entries.push(IndexCatalogEntry {
            retired: false,
            statistics: None,
            definition: IndexDefinition {
                id: IndexId(1),
                name: None,
                column_id: ColumnId(1),
                handle: BTreeHandle {
                    owner: Some(IndexId(1)),
                    meta_page: node.pending[0].meta_page.unwrap(),
                },
            },
        });
        assert!(encode_index_catalog(&active).is_err());
        active.entries[0].retired = true;
        assert!(encode_index_catalog(&active).is_ok());
        active.reclaim_intent.as_mut().unwrap().covered[0].meta_page =
            Some(BTreePageRef::Allocated(PageRef {
                page_id: PageId(3),
                generation: PageGeneration(8),
            }));
        assert!(encode_index_catalog(&active).is_err());
    }

    #[test]
    fn tail_no_intent_v2_through_v8_decode_and_v7_refs_upgrade_exactly() {
        for version in 2_u16..=8 {
            let mut bytes = encode_index_catalog(&IndexCatalogNode::empty()).unwrap();
            bytes[4..6].copy_from_slice(&version.to_le_bytes());
            if version < 5 {
                bytes[7] = 0;
                bytes[40..48].fill(0);
            }
            let decoded = decode_index_catalog(&bytes).unwrap();
            assert!(decoded.reclaim_intent.is_none());
            assert_eq!(
                decode_index_catalog(&encode_index_catalog(&decoded).unwrap()).unwrap(),
                decoded
            );
        }
        let mut old = node();
        old.reclaim_intent = None;
        let mut bytes = encode_index_catalog(&old).unwrap();
        bytes[4..6].copy_from_slice(&7_u16.to_le_bytes());
        assert_eq!(decode_index_catalog(&bytes).unwrap(), old);
        bytes[4..6].copy_from_slice(&1_u16.to_le_bytes());
        assert_eq!(
            decode_index_catalog(&bytes),
            Err(IndexError::UnsupportedVersion(1))
        );
    }
}

#[cfg(test)]
mod owner_only_tests {
    use super::*;

    #[test]
    fn v9_owner_only_pending_has_one_authority_and_bounded_encoding() {
        let mut node = IndexCatalogNode::empty();
        node.next_index_id = Some(IndexId(3));
        node.pending = vec![
            RetiredIndexOwnership {
                index_id: IndexId(1),
                meta_page: None,
            },
            RetiredIndexOwnership {
                index_id: IndexId(2),
                meta_page: None,
            },
        ];
        let bytes = encode_index_catalog(&node).unwrap();
        assert_eq!(bytes.len(), 48 + 2 * 32);
        assert_eq!(&bytes[4..6], &9_u16.to_le_bytes());
        assert_eq!(bytes[72], 2);
        assert!(bytes[56..72].iter().all(|byte| *byte == 0));
        assert_eq!(decode_index_catalog(&bytes).unwrap(), node);
        for end in 0..bytes.len() {
            assert!(decode_index_catalog(&bytes[..end]).is_err());
        }
        for offset in [56, 64, 73] {
            let mut bad = bytes.clone();
            bad[offset] = 1;
            assert!(decode_index_catalog(&bad).is_err());
        }
        for tag in [0, 1, 3, 255] {
            let mut bad = bytes.clone();
            bad[72] = tag;
            assert!(decode_index_catalog(&bad).is_err());
        }
        for version in [7_u16, 8] {
            let mut bad = bytes.clone();
            bad[4..6].copy_from_slice(&version.to_le_bytes());
            assert!(decode_index_catalog(&bad).is_err());
        }
        node.pending[1].index_id = IndexId(1);
        assert!(matches!(
            encode_index_catalog(&node),
            Err(IndexError::DuplicateIndexId(IndexId(1)))
        ));
        node.pending.pop();
        node.next_index_id = Some(IndexId(1));
        assert!(matches!(
            encode_index_catalog(&node),
            Err(IndexError::InvalidIndexHighWater(IndexId(1)))
        ));
    }

    #[test]
    fn v9_owner_only_tail_intent_and_v8_exact_root_intent_remain_distinct() {
        let record = RetiredIndexOwnership {
            index_id: IndexId(1),
            meta_page: None,
        };
        let mut node = IndexCatalogNode::empty();
        node.next_index_id = Some(IndexId(2));
        node.pending.push(record);
        node.reclaim_intent = Some(TailReclaimIntent {
            old_page_count: 7,
            truncate_from: 6,
            checkpoint_lsn: netbadb_types::Lsn(100),
            covered: vec![record],
        });
        let bytes = encode_index_catalog(&node).unwrap();
        assert_eq!(bytes.len(), 136);
        assert!(bytes[120..136].iter().all(|byte| *byte == 0));
        assert_eq!(decode_index_catalog(&bytes).unwrap(), node);
        for offset in [120, 128] {
            let mut bad = bytes.clone();
            bad[offset] = 1;
            assert!(decode_index_catalog(&bad).is_err());
        }
        let root = BTreePageRef::Allocated(PageRef {
            page_id: PageId(6),
            generation: PageGeneration(50),
        });
        node.pending[0].meta_page = Some(root);
        node.reclaim_intent.as_mut().unwrap().covered[0].meta_page = Some(root);
        let mut old = encode_index_catalog(&node).unwrap();
        old[4..6].copy_from_slice(&8_u16.to_le_bytes());
        assert_eq!(decode_index_catalog(&old).unwrap(), node);
        let mut forbidden = old;
        forbidden[120..136].fill(0);
        assert!(decode_index_catalog(&forbidden).is_err());
    }
}
