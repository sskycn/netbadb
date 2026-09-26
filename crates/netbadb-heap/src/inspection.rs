use crate::StorageVisibilityBoundary;
use netbadb_types::{ColumnId, IndexId, IndexName, StorageId};
use std::path::{Path, PathBuf};

/// Logical index identity copied into a private replacement Heap.
/// Physical BTree handles are intentionally replaced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeapRewriteIndex {
    pub id: IndexId,
    pub name: Option<IndexName>,
    pub column_id: ColumnId,
}

/// Active index inventory plus its durable allocation boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeapRewriteIndexes {
    pub active: Vec<HeapRewriteIndex>,
    pub next_index_id: IndexId,
}

/// One exact file owned by a single Heap storage resource.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeapResourceComponentKind {
    Main,
    Wal,
    WalOwnerLock,
    TransactionStatus,
    AlternateWal,
    ChangeLog,
    ChangeStreamGuard,
}

/// Storage-authored physical bundle member. Callers may add their own
/// higher-layer metadata, but must not infer Heap suffixes independently.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeapResourceComponent {
    pub kind: HeapResourceComponentKind,
    pub path: PathBuf,
    pub required: bool,
}

/// Returns the complete, exact set of files owned by one Heap implementation.
/// Index-catalog and BTree pages are contained in `Main`.
#[must_use]
pub fn heap_resource_components(path: impl AsRef<Path>) -> Vec<HeapResourceComponent> {
    let main = path.as_ref();
    let wal = crate::wal_path(main);
    vec![
        HeapResourceComponent {
            kind: HeapResourceComponentKind::Main,
            path: main.to_owned(),
            required: true,
        },
        HeapResourceComponent {
            kind: HeapResourceComponentKind::Wal,
            path: wal.clone(),
            required: true,
        },
        HeapResourceComponent {
            kind: HeapResourceComponentKind::WalOwnerLock,
            path: crate::wal_owner_path(&wal),
            required: false,
        },
        HeapResourceComponent {
            kind: HeapResourceComponentKind::WalOwnerLock,
            path: crate::wal_owner_path(crate::wal_alternate_path(&wal)),
            required: false,
        },
        HeapResourceComponent {
            kind: HeapResourceComponentKind::TransactionStatus,
            path: crate::txn_status_path(main),
            required: true,
        },
        HeapResourceComponent {
            kind: HeapResourceComponentKind::AlternateWal,
            path: crate::wal_alternate_path(wal),
            required: false,
        },
        HeapResourceComponent {
            kind: HeapResourceComponentKind::ChangeLog,
            path: crate::heap_change_log_path(main),
            required: false,
        },
        HeapResourceComponent {
            kind: HeapResourceComponentKind::ChangeStreamGuard,
            path: crate::change_stream_guard_path(crate::heap_change_log_path(main)),
            required: false,
        },
    ]
}

/// Current main-file geometry, independent of optimizer ANALYZE snapshots.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeapPhysicalDesignSourceInspection {
    pub storage_id: StorageId,
    /// Committed read horizon only; this is not a physical-layout token.
    pub visibility_boundary: StorageVisibilityBoundary,
    /// All managed pages, including access-method/catalog/free pages that the
    /// production sequential scan validates and skips. No row-count claim.
    pub managed_page_upper_bound: u64,
    /// Maximum rows a valid production scan could emit from the current page
    /// geometry. This is deliberately not a current live-row count.
    pub row_upper_bound: u64,
    /// Full aligned main-file extent, including its header. A format-level
    /// source footprint, not device I/O, cache misses, or elapsed time.
    pub main_file_bytes_upper_bound: u64,
    /// One production Index backfill pass starts after empty-tree allocation
    /// and bounded pre-scan catalog growth (including legacy re-encoding).
    /// Excludes allocator/catalog traversal, tree inserts and output writes.
    pub index_backfill_page_upper_bound: u64,
    /// Main-file extent addressable by that backfill pass, including bounded
    /// pre-scan growth. Later tree splits are outside its fixed limit.
    pub index_backfill_bytes_upper_bound: u64,
}
