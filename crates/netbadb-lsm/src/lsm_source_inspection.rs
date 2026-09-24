use crate::{StorageError, StorageVisibilityBoundary};

/// Exact current runtime/manifest structure; versions and tombstones count as
/// physical entries, never as current logical rows. No source rows are read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LsmPhysicalDesignSourceInspection {
    pub anchor: crate::LsmMaintenanceAnchor,
    pub visibility_boundary: StorageVisibilityBoundary,
    pub memtable_entry_count: u64,
    /// Resident encoded-payload accounting (40 bytes per version plus value
    /// length), not allocator/RSS usage and not persistent read I/O.
    pub memtable_bytes: u64,
    pub sstable_count: u64,
    pub sstable_entry_count: u64,
    /// Current persistent SSTable file extents. One full source traversal
    /// reads only blocks inside these files, at most once per block.
    pub total_sstable_bytes: u64,
    /// Existing production flush theorem. None means precisely an empty
    /// MemTable (no flush output), not an unknown or zero resource bound.
    pub flush_conservative_bound: Option<crate::LsmMaintenanceBoundInspection>,
}

impl LsmPhysicalDesignSourceInspection {
    /// Physical versions and tombstones are included, so visible rows can only
    /// be fewer than this current structural bound.
    pub fn row_upper_bound(self) -> Result<u64, StorageError> {
        self.sstable_entry_count
            .checked_add(self.memtable_entry_count)
            .ok_or(StorageError::ResourceBoundOverflow {
                resource: "LSM source row",
            })
    }

    /// Bounds the persistent SSTable extent that Snapshot capture can scan
    /// after its ordinary pre-scan flush. The existing flush theorem describes
    /// the one newly added SSTable; that flush performs no compaction.
    pub fn prospective_snapshot_sstable_bytes_upper_bound(self) -> Result<u64, StorageError> {
        match self.flush_conservative_bound {
            None => Ok(self.total_sstable_bytes),
            Some(flush) => self
                .total_sstable_bytes
                .checked_add(flush.write_bytes)
                .ok_or(StorageError::ResourceBoundOverflow {
                    resource: "prospective Snapshot LSM SSTable bytes",
                }),
        }
    }
}
