//! Immutable derived columnar projections.
//!
//! `NBCM`/`NBCS` version 1 describe snapshot projections. Version 2 adds the
//! hidden source-version identities and incremental metadata needed to merge
//! immutable `NBCD` version 1 delta segments. All integers are little-endian
//! and every file ends in a CRC32C of the preceding bytes. Heap or LSM data
//! remains authoritative.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use netbadb_index::compare_values;
use netbadb_schema::{SchemaFingerprint, TableDef};
use netbadb_types::{
    ChangeStreamGeneration, ColumnId, ColumnarGeneration, ColumnarProjectionId, ColumnarSegmentId,
    LsmCommitSeq, LsmRowId, PageId, PhysicalType, RowId, ScalarValue, StorageDataVersion,
    StorageId, TableId,
};

use crate::{ChangeBatch, StorageChange, StorageVersionKey};

const MANIFEST_MAGIC: &[u8; 4] = b"NBCM";
const SEGMENT_MAGIC: &[u8; 4] = b"NBCS";
const SNAPSHOT_FORMAT_VERSION: u16 = 1;
const INCREMENTAL_FORMAT_VERSION: u16 = 2;
const DELTA_MAGIC: &[u8; 4] = b"NBCD";
const DELTA_FORMAT_VERSION: u16 = 1;
const MANIFEST_FILE: &str = "projection.nbcmanifest";
const MAX_FILE_BYTES: u64 = 1 << 34;
const MAX_COLUMNS: u32 = 1 << 20;
const MAX_ROW_GROUPS: u32 = 1 << 24;
const MAX_TEXT_BYTES: u64 = 1 << 32;
const DEFAULT_ROW_GROUP_ROWS: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SnapshotKind {
    Heap,
    Lsm,
}

/// Equality-only identity of one committed read horizon in one physical storage.
///
/// Its parts have no meaning outside the exact `StorageId` and engine kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageSnapshotToken {
    storage_id: StorageId,
    kind: SnapshotKind,
    epoch: u64,
    sequence: u64,
}

impl StorageSnapshotToken {
    pub(crate) const fn heap(storage_id: StorageId, sequence: u64) -> Self {
        Self {
            storage_id,
            kind: SnapshotKind::Heap,
            epoch: 0,
            sequence,
        }
    }

    pub(crate) const fn lsm(storage_id: StorageId, epoch: u64, sequence: u64) -> Self {
        Self {
            storage_id,
            kind: SnapshotKind::Lsm,
            epoch,
            sequence,
        }
    }

    #[must_use]
    pub const fn storage_id(self) -> StorageId {
        self.storage_id
    }

    /// Stable diagnostic form; callers must treat it as opaque.
    #[must_use]
    pub fn diagnostic(&self) -> String {
        let kind = match self.kind {
            SnapshotKind::Heap => "heap",
            SnapshotKind::Lsm => "lsm",
        };
        format!(
            "{kind}:{}:{}:{}",
            self.storage_id.0, self.epoch, self.sequence
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnarColumnSpec {
    pub column_id: ColumnId,
    pub physical_type: PhysicalType,
    pub nullable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnarColumnStatistics {
    pub null_count: u64,
    pub minimum: Option<ScalarValue>,
    pub maximum: Option<ScalarValue>,
    /// Encoded validity, offset, and value bytes for this row-group chunk.
    /// This is derived from the decoded immutable vector, not persisted twice.
    pub encoded_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnarRowGroupStatistics {
    pub rows: u32,
    pub columns: Vec<(ColumnId, ColumnarColumnStatistics)>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ColumnarScanStatistics {
    pub row_groups_total: u64,
    pub row_groups_read: u64,
    pub row_groups_pruned: u64,
    pub rows_read: u64,
    pub column_chunks_read: u64,
    pub bytes_read: u64,
    pub base_rows_suppressed: u64,
    pub delta_segments: u64,
    pub delta_mutations: u64,
    pub delta_live_rows: u64,
    pub delta_rows_emitted: u64,
    pub delta_bytes_read: u64,
    pub merged_rows: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnarConstraint {
    pub column_id: ColumnId,
    pub lower: Option<(ScalarValue, bool)>,
    pub upper: Option<(ScalarValue, bool)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnarVector {
    Bool {
        values: Vec<bool>,
        validity: Vec<u8>,
    },
    Int64 {
        values: Vec<i64>,
        validity: Vec<u8>,
    },
    UInt64 {
        values: Vec<u64>,
        validity: Vec<u8>,
    },
    Text {
        offsets: Vec<u32>,
        bytes: Vec<u8>,
        validity: Vec<u8>,
    },
}

impl ColumnarVector {
    #[must_use]
    pub fn len(&self) -> usize {
        match self {
            Self::Bool { values, .. } => values.len(),
            Self::Int64 { values, .. } => values.len(),
            Self::UInt64 { values, .. } => values.len(),
            Self::Text { offsets, .. } => offsets.len().saturating_sub(1),
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn value(&self, row: usize) -> Result<ScalarValue, ColumnarError> {
        let validity = match self {
            Self::Bool { validity, .. }
            | Self::Int64 { validity, .. }
            | Self::UInt64 { validity, .. }
            | Self::Text { validity, .. } => validity,
        };
        if row >= self.len() {
            return Err(ColumnarError::Corrupt("column row index is out of bounds"));
        }
        if !valid_at(validity, row) {
            return Ok(ScalarValue::Null);
        }
        match self {
            Self::Bool { values, .. } => Ok(ScalarValue::Bool(values[row])),
            Self::Int64 { values, .. } => Ok(ScalarValue::Int64(values[row])),
            Self::UInt64 { values, .. } => Ok(ScalarValue::UInt64(values[row])),
            Self::Text { offsets, bytes, .. } => {
                let start = usize::try_from(offsets[row])
                    .map_err(|_| ColumnarError::Corrupt("text start offset overflow"))?;
                let end = usize::try_from(offsets[row + 1])
                    .map_err(|_| ColumnarError::Corrupt("text end offset overflow"))?;
                let value = bytes
                    .get(start..end)
                    .ok_or(ColumnarError::Corrupt("text offsets are out of bounds"))?;
                Ok(ScalarValue::Text(
                    std::str::from_utf8(value)
                        .map_err(|_| ColumnarError::Corrupt("text payload is not UTF-8"))?
                        .to_owned(),
                ))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnarBatchColumn {
    pub column_id: ColumnId,
    pub values: ColumnarVector,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnarBatch {
    pub row_count: usize,
    pub columns: Vec<ColumnarBatchColumn>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RowGroup {
    rows: u32,
    source_versions: Option<Vec<StorageVersionKey>>,
    columns: Vec<ColumnarBatchColumn>,
    statistics: Vec<(ColumnId, ColumnarColumnStatistics)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnarProjectionMetadata {
    pub id: ColumnarProjectionId,
    pub generation: ColumnarGeneration,
    pub table_id: TableId,
    pub source_storage_id: StorageId,
    pub source_token: StorageSnapshotToken,
    pub schema_fingerprint: SchemaFingerprint,
    pub columns: Vec<ColumnarColumnSpec>,
    pub row_count: u64,
    pub row_group_count: u64,
    pub segment_count: u64,
    pub segment_bytes: u64,
    pub incremental: Option<ColumnarIncrementalMetadata>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnarIncrementalMetadata {
    pub stream_generation: ChangeStreamGeneration,
    pub base_frontier: StorageDataVersion,
    pub applied_frontier: StorageDataVersion,
    pub delta_segments: Vec<ColumnarDeltaSegmentMetadata>,
    pub delta_mutation_count: u64,
    pub delta_live_row_count: u64,
    pub suppressed_version_count: u64,
    pub delta_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnarDeltaSegmentMetadata {
    pub file: String,
    pub before: StorageDataVersion,
    pub after: StorageDataVersion,
    pub mutation_count: u64,
    pub after_row_count: u64,
    pub bytes: u64,
    pub checksum: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProjectedDeltaMutation {
    old_version: Option<StorageVersionKey>,
    new_version: Option<StorageVersionKey>,
    after: Option<Vec<ScalarValue>>,
}

#[derive(Debug, Clone, Default)]
struct DeltaOverlay {
    suppressed: HashSet<StorageVersionKey>,
    live: HashMap<StorageVersionKey, Vec<ScalarValue>>,
}

#[derive(Debug, Clone)]
pub struct ColumnarProjection {
    root: PathBuf,
    metadata: ColumnarProjectionMetadata,
    segment_id: ColumnarSegmentId,
    segment_file: String,
    row_groups: Vec<RowGroup>,
    overlay: DeltaOverlay,
}

/// Fully written and synced generation that is not yet visible to readers.
#[derive(Debug)]
pub struct PreparedColumnarProjection {
    root: PathBuf,
    metadata: ColumnarProjectionMetadata,
    segment_id: ColumnarSegmentId,
    segment_file: String,
    row_groups: Vec<RowGroup>,
    overlay: DeltaOverlay,
    segment_tmp: PathBuf,
    manifest_tmp: PathBuf,
    published: bool,
}

#[derive(Debug)]
pub struct PreparedColumnarAdvance {
    root: PathBuf,
    projection: ColumnarProjection,
    delta_tmp: PathBuf,
    delta_final: PathBuf,
    manifest_tmp: PathBuf,
    published: bool,
}

impl PreparedColumnarProjection {
    pub fn publish(mut self) -> Result<ColumnarProjection, ColumnarError> {
        let segment_final = self.root.join(&self.segment_file);
        fs::rename(&self.segment_tmp, &segment_final).map_err(ColumnarError::Io)?;
        if let Err(error) = sync_directory(&self.root) {
            let _ = fs::remove_file(&segment_final);
            return Err(error);
        }
        if let Err(error) = fs::rename(&self.manifest_tmp, self.root.join(MANIFEST_FILE)) {
            let _ = fs::remove_file(&segment_final);
            return Err(ColumnarError::Io(error));
        }
        sync_directory(&self.root)?;
        self.published = true;
        Ok(ColumnarProjection {
            root: self.root.clone(),
            metadata: self.metadata.clone(),
            segment_id: self.segment_id,
            segment_file: self.segment_file.clone(),
            row_groups: self.row_groups.clone(),
            overlay: self.overlay.clone(),
        })
    }
}

impl PreparedColumnarAdvance {
    pub fn publish(mut self) -> Result<ColumnarProjection, ColumnarError> {
        fs::rename(&self.delta_tmp, &self.delta_final).map_err(ColumnarError::Io)?;
        crash("delta-renamed");
        sync_directory(&self.root)?;
        crash("delta-directory-synced");
        fs::rename(&self.manifest_tmp, self.root.join(MANIFEST_FILE)).map_err(ColumnarError::Io)?;
        crash("delta-manifest-renamed");
        sync_directory(&self.root)?;
        self.published = true;
        Ok(self.projection.clone())
    }
}

impl Drop for PreparedColumnarAdvance {
    fn drop(&mut self) {
        if !self.published {
            let _ = fs::remove_file(&self.delta_tmp);
            let _ = fs::remove_file(&self.manifest_tmp);
        }
    }
}

impl Drop for PreparedColumnarProjection {
    fn drop(&mut self) {
        if !self.published {
            let _ = fs::remove_file(&self.segment_tmp);
            let _ = fs::remove_file(&self.manifest_tmp);
        }
    }
}

impl ColumnarProjection {
    #[allow(clippy::too_many_arguments)]
    pub fn prepare(
        root: impl AsRef<Path>,
        id: ColumnarProjectionId,
        generation: ColumnarGeneration,
        table: &TableDef,
        source_storage_id: StorageId,
        source_token: StorageSnapshotToken,
        columns: &[ColumnId],
        rows: &[Vec<ScalarValue>],
        row_group_rows: Option<usize>,
    ) -> Result<PreparedColumnarProjection, ColumnarError> {
        if source_token.storage_id() != source_storage_id {
            return Err(ColumnarError::IdentityMismatch("snapshot storage"));
        }
        let column_specs = resolve_columns(table, columns)?;
        for row in rows {
            if row.len() != column_specs.len() {
                return Err(ColumnarError::InvalidInput(
                    "row width differs from projection",
                ));
            }
        }
        let group_rows = row_group_rows.unwrap_or(DEFAULT_ROW_GROUP_ROWS);
        if group_rows == 0 || group_rows > u32::MAX as usize {
            return Err(ColumnarError::InvalidInput("invalid row-group size"));
        }
        let root = root.as_ref().to_owned();
        fs::create_dir_all(&root).map_err(ColumnarError::Io)?;
        let segment_id = ColumnarSegmentId(generation.0);
        let segment_file = format!("projection-{}-g{}.nbcs", id.0, generation.0);
        let row_groups = rows
            .chunks(group_rows)
            .map(|chunk| encode_row_group(&column_specs, chunk))
            .collect::<Result<Vec<_>, _>>()?;
        let fingerprint = table.fingerprint().map_err(ColumnarError::Schema)?;
        let segment = encode_segment(
            SegmentIdentity {
                projection_id: id,
                generation,
                segment_id,
                table_id: table.id,
                storage_id: source_storage_id,
                fingerprint,
            },
            &column_specs,
            &row_groups,
            SNAPSHOT_FORMAT_VERSION,
        )?;
        let segment_checksum = stored_checksum(&segment)?;
        let segment_bytes = u64::try_from(segment.len())
            .map_err(|_| ColumnarError::InvalidInput("segment length overflow"))?;
        let row_count = u64::try_from(rows.len())
            .map_err(|_| ColumnarError::InvalidInput("row count overflow"))?;
        let metadata = ColumnarProjectionMetadata {
            id,
            generation,
            table_id: table.id,
            source_storage_id,
            source_token,
            schema_fingerprint: fingerprint,
            columns: column_specs,
            row_count,
            row_group_count: u64::try_from(row_groups.len())
                .map_err(|_| ColumnarError::InvalidInput("row-group count overflow"))?,
            segment_count: 1,
            segment_bytes,
            incremental: None,
        };
        let manifest = encode_manifest(&metadata, segment_id, &segment_file, segment_checksum)?;
        let suffix = format!("{}.{}.{}", std::process::id(), id.0, generation.0);
        let segment_tmp = root.join(format!(".{segment_file}.tmp.{suffix}"));
        let manifest_tmp = root.join(format!(".{MANIFEST_FILE}.tmp.{suffix}"));
        write_new_synced(&segment_tmp, &segment)?;
        if let Err(error) = write_new_synced(&manifest_tmp, &manifest) {
            let _ = fs::remove_file(&segment_tmp);
            return Err(error);
        }
        Ok(PreparedColumnarProjection {
            root,
            metadata,
            segment_id,
            segment_file,
            row_groups,
            overlay: DeltaOverlay::default(),
            segment_tmp,
            manifest_tmp,
            published: false,
        })
    }

    /// Builds an NBCS/NBCM v2 base bound to one committed change-stream
    /// frontier. Source version keys remain hidden from the SQL schema.
    #[allow(clippy::too_many_arguments)]
    pub fn prepare_incremental(
        root: impl AsRef<Path>,
        id: ColumnarProjectionId,
        generation: ColumnarGeneration,
        table: &TableDef,
        source_storage_id: StorageId,
        source_token: StorageSnapshotToken,
        cursor: crate::ChangeStreamCursor,
        columns: &[ColumnId],
        rows: &[(StorageVersionKey, Vec<ScalarValue>)],
        row_group_rows: Option<usize>,
    ) -> Result<PreparedColumnarProjection, ColumnarError> {
        if source_token.storage_id() != source_storage_id || cursor.storage_id != source_storage_id
        {
            return Err(ColumnarError::IdentityMismatch(
                "incremental source storage",
            ));
        }
        if cursor.generation.0 == 0 {
            return Err(ColumnarError::InvalidInput("zero change-stream generation"));
        }
        let column_specs = resolve_columns(table, columns)?;
        validate_versioned_rows(source_storage_id, source_token.kind, &column_specs, rows)?;
        let group_rows = row_group_rows.unwrap_or(DEFAULT_ROW_GROUP_ROWS);
        if group_rows == 0 || group_rows > u32::MAX as usize {
            return Err(ColumnarError::InvalidInput("invalid row-group size"));
        }
        let root = root.as_ref().to_owned();
        fs::create_dir_all(&root).map_err(ColumnarError::Io)?;
        let segment_id = ColumnarSegmentId(generation.0);
        let segment_file = format!("projection-{}-g{}.nbcs", id.0, generation.0);
        let row_groups = rows
            .chunks(group_rows)
            .map(|chunk| encode_versioned_row_group(&column_specs, chunk))
            .collect::<Result<Vec<_>, _>>()?;
        let fingerprint = table.fingerprint().map_err(ColumnarError::Schema)?;
        let metadata_seed = ColumnarProjectionMetadata {
            id,
            generation,
            table_id: table.id,
            source_storage_id,
            source_token,
            schema_fingerprint: fingerprint,
            columns: column_specs,
            row_count: u64::try_from(rows.len())
                .map_err(|_| ColumnarError::InvalidInput("row count overflow"))?,
            row_group_count: u64::try_from(row_groups.len())
                .map_err(|_| ColumnarError::InvalidInput("row-group count overflow"))?,
            segment_count: 1,
            segment_bytes: 0,
            incremental: Some(ColumnarIncrementalMetadata {
                stream_generation: cursor.generation,
                base_frontier: cursor.frontier,
                applied_frontier: cursor.frontier,
                delta_segments: Vec::new(),
                delta_mutation_count: 0,
                delta_live_row_count: 0,
                suppressed_version_count: 0,
                delta_bytes: 0,
            }),
        };
        let segment = encode_segment(
            SegmentIdentity {
                projection_id: id,
                generation,
                segment_id,
                table_id: table.id,
                storage_id: source_storage_id,
                fingerprint,
            },
            &metadata_seed.columns,
            &row_groups,
            INCREMENTAL_FORMAT_VERSION,
        )?;
        let segment_checksum = stored_checksum(&segment)?;
        let mut metadata = metadata_seed;
        metadata.segment_bytes = u64::try_from(segment.len())
            .map_err(|_| ColumnarError::InvalidInput("segment length overflow"))?;
        let manifest = encode_manifest(&metadata, segment_id, &segment_file, segment_checksum)?;
        let suffix = format!("{}.{}.{}", std::process::id(), id.0, generation.0);
        let segment_tmp = root.join(format!(".{segment_file}.tmp.{suffix}"));
        let manifest_tmp = root.join(format!(".{MANIFEST_FILE}.tmp.{suffix}"));
        write_new_synced(&segment_tmp, &segment)?;
        if let Err(error) = write_new_synced(&manifest_tmp, &manifest) {
            let _ = fs::remove_file(&segment_tmp);
            return Err(error);
        }
        Ok(PreparedColumnarProjection {
            root,
            metadata,
            segment_id,
            segment_file,
            row_groups,
            overlay: DeltaOverlay::default(),
            segment_tmp,
            manifest_tmp,
            published: false,
        })
    }

    pub fn open(root: impl AsRef<Path>, table: &TableDef) -> Result<Self, ColumnarError> {
        let root = root.as_ref().to_owned();
        let manifest_bytes = read_bounded(&root.join(MANIFEST_FILE))?;
        let manifest = decode_manifest(&manifest_bytes)?;
        if manifest.metadata.table_id != table.id {
            return Err(ColumnarError::IdentityMismatch("table"));
        }
        if manifest.metadata.schema_fingerprint
            != table.fingerprint().map_err(ColumnarError::Schema)?
        {
            return Err(ColumnarError::IdentityMismatch("schema fingerprint"));
        }
        let segment_path = root.join(&manifest.segment_file);
        let segment_bytes = read_bounded(&segment_path)?;
        if u64::try_from(segment_bytes.len())
            .map_err(|_| ColumnarError::Corrupt("segment length does not fit u64"))?
            != manifest.metadata.segment_bytes
        {
            return Err(ColumnarError::Corrupt(
                "segment length differs from manifest",
            ));
        }
        if stored_checksum(&segment_bytes)? != manifest.segment_checksum {
            return Err(ColumnarError::ChecksumMismatch { path: segment_path });
        }
        let decoded = decode_segment(&segment_bytes, &manifest.metadata)?;
        if decoded.segment_id != manifest.segment_id {
            return Err(ColumnarError::IdentityMismatch("segment"));
        }
        let mut overlay = DeltaOverlay::default();
        if let Some(incremental) = &manifest.metadata.incremental {
            let mut expected = incremental.base_frontier;
            for delta in &incremental.delta_segments {
                if delta.before != expected || delta.after.0 <= delta.before.0 {
                    return Err(ColumnarError::Corrupt("broken delta frontier chain"));
                }
                let path = root.join(&delta.file);
                let bytes = read_bounded(&path)?;
                if u64::try_from(bytes.len()).ok() != Some(delta.bytes)
                    || stored_checksum(&bytes)? != delta.checksum
                {
                    return Err(ColumnarError::ChecksumMismatch { path });
                }
                let mutations = decode_delta(&bytes, &manifest.metadata, delta)?;
                apply_overlay(&mut overlay, &mutations)?;
                expected = delta.after;
            }
            if expected != incremental.applied_frontier
                || overlay.live.len() as u64 != incremental.delta_live_row_count
                || overlay.suppressed.len() as u64 != incremental.suppressed_version_count
            {
                return Err(ColumnarError::Corrupt("delta manifest statistics mismatch"));
            }
        }
        Ok(Self {
            root,
            metadata: manifest.metadata,
            segment_id: manifest.segment_id,
            segment_file: manifest.segment_file,
            row_groups: decoded.row_groups,
            overlay,
        })
    }

    #[must_use]
    pub fn metadata(&self) -> &ColumnarProjectionMetadata {
        &self.metadata
    }

    #[must_use]
    pub const fn segment_id(&self) -> ColumnarSegmentId {
        self.segment_id
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    #[must_use]
    pub fn row_group_statistics(&self) -> Vec<ColumnarRowGroupStatistics> {
        self.row_groups
            .iter()
            .map(|group| ColumnarRowGroupStatistics {
                rows: group.rows,
                columns: group.statistics.clone(),
            })
            .collect()
    }

    #[must_use]
    pub fn is_incremental(&self) -> bool {
        self.metadata.incremental.is_some()
    }

    /// Converts one bounded contiguous NBCL read into a synced immutable NBCD
    /// segment and a synced replacement NBCM. Publication remains explicit.
    pub fn prepare_advance(
        &self,
        table: &TableDef,
        batches: &[ChangeBatch],
    ) -> Result<PreparedColumnarAdvance, ColumnarError> {
        let incremental = self
            .metadata
            .incremental
            .as_ref()
            .ok_or(ColumnarError::InvalidInput(
                "snapshot projection cannot advance",
            ))?;
        if batches.is_empty() {
            return Err(ColumnarError::InvalidInput("advance contains no batches"));
        }
        if table.id != self.metadata.table_id
            || table.fingerprint().map_err(ColumnarError::Schema)?
                != self.metadata.schema_fingerprint
        {
            return Err(ColumnarError::IdentityMismatch("advance schema"));
        }
        let projected = project_change_batches(table, &self.metadata, batches)?;
        let before = batches[0].before;
        let after = batches
            .last()
            .map(|batch| batch.after)
            .ok_or(ColumnarError::InvalidInput("advance contains no batches"))?;
        if before != incremental.applied_frontier {
            return Err(ColumnarError::IdentityMismatch("advance frontier"));
        }
        let delta_file = format!(
            "projection-{}-g{}-d{}.nbcd",
            self.metadata.id.0, self.metadata.generation.0, after.0
        );
        let delta = encode_delta(&self.metadata, batches, &projected)?;
        let checksum = stored_checksum(&delta)?;
        let bytes = u64::try_from(delta.len())
            .map_err(|_| ColumnarError::InvalidInput("delta length overflow"))?;
        let mutation_count = u64::try_from(projected.len())
            .map_err(|_| ColumnarError::InvalidInput("delta mutation count overflow"))?;
        let after_row_count = projected
            .iter()
            .filter(|change| change.after.is_some())
            .count() as u64;
        let mut replacement = self.clone();
        apply_overlay(&mut replacement.overlay, &projected)?;
        let replacement_incremental = replacement
            .metadata
            .incremental
            .as_mut()
            .ok_or(ColumnarError::Corrupt("incremental metadata disappeared"))?;
        replacement_incremental
            .delta_segments
            .push(ColumnarDeltaSegmentMetadata {
                file: delta_file.clone(),
                before,
                after,
                mutation_count,
                after_row_count,
                bytes,
                checksum,
            });
        replacement_incremental.applied_frontier = after;
        replacement_incremental.delta_mutation_count = replacement_incremental
            .delta_mutation_count
            .checked_add(mutation_count)
            .ok_or(ColumnarError::InvalidInput("delta mutation total overflow"))?;
        replacement_incremental.delta_live_row_count = replacement.overlay.live.len() as u64;
        replacement_incremental.suppressed_version_count =
            replacement.overlay.suppressed.len() as u64;
        replacement_incremental.delta_bytes = replacement_incremental
            .delta_bytes
            .checked_add(bytes)
            .ok_or(ColumnarError::InvalidInput("delta byte total overflow"))?;
        replacement.metadata.segment_count = 1_u64
            .checked_add(replacement_incremental.delta_segments.len() as u64)
            .ok_or(ColumnarError::InvalidInput("segment count overflow"))?;
        let manifest = encode_manifest(
            &replacement.metadata,
            replacement.segment_id,
            &replacement.segment_file,
            stored_checksum(&read_bounded(&self.root.join(&self.segment_file))?)?,
        )?;
        let suffix = format!("{}.{}.{}", std::process::id(), self.metadata.id.0, after.0);
        let delta_tmp = self.root.join(format!(".{delta_file}.tmp.{suffix}"));
        let manifest_tmp = self.root.join(format!(".{MANIFEST_FILE}.tmp.{suffix}"));
        write_advance_file_synced(
            &delta_tmp,
            &delta,
            "delta-temp-created",
            "delta-written",
            "delta-synced",
        )?;
        if let Err(error) = write_advance_file_synced(
            &manifest_tmp,
            &manifest,
            "delta-manifest-temp-created",
            "delta-manifest-written",
            "delta-manifest-synced",
        ) {
            let _ = fs::remove_file(&delta_tmp);
            return Err(error);
        }
        crash("delta-manifest-synced");
        Ok(PreparedColumnarAdvance {
            root: self.root.clone(),
            projection: replacement,
            delta_tmp,
            delta_final: self.root.join(delta_file),
            manifest_tmp,
            published: false,
        })
    }

    pub fn scan(
        &self,
        columns: &[ColumnId],
        constraints: &[ColumnarConstraint],
    ) -> Result<(Vec<ColumnarBatch>, ColumnarScanStatistics), ColumnarError> {
        for column in columns {
            if !self
                .metadata
                .columns
                .iter()
                .any(|candidate| candidate.column_id == *column)
            {
                return Err(ColumnarError::UnknownColumn(*column));
            }
        }
        let mut statistics = ColumnarScanStatistics {
            row_groups_total: u64::try_from(self.row_groups.len()).unwrap_or(u64::MAX),
            delta_segments: self
                .metadata
                .incremental
                .as_ref()
                .map_or(0, |value| value.delta_segments.len() as u64),
            delta_mutations: self
                .metadata
                .incremental
                .as_ref()
                .map_or(0, |value| value.delta_mutation_count),
            delta_live_rows: self.overlay.live.len() as u64,
            delta_bytes_read: self
                .metadata
                .incremental
                .as_ref()
                .map_or(0, |value| value.delta_bytes),
            ..ColumnarScanStatistics::default()
        };
        let mut batches = Vec::new();
        for group in &self.row_groups {
            if constraints
                .iter()
                .any(|constraint| group_cannot_match(group, constraint))
            {
                statistics.row_groups_pruned += 1;
                continue;
            }
            statistics.row_groups_read += 1;
            let retained = match &group.source_versions {
                Some(keys) => keys
                    .iter()
                    .enumerate()
                    .filter_map(|(row, key)| {
                        if self.overlay.suppressed.contains(key) {
                            statistics.base_rows_suppressed =
                                statistics.base_rows_suppressed.saturating_add(1);
                            None
                        } else {
                            Some(row)
                        }
                    })
                    .collect::<Vec<_>>(),
                None => (0..group.rows as usize).collect(),
            };
            statistics.rows_read = statistics.rows_read.saturating_add(retained.len() as u64);
            let selected = columns
                .iter()
                .map(|column| {
                    group
                        .columns
                        .iter()
                        .find(|candidate| candidate.column_id == *column)
                        .map(|candidate| ColumnarBatchColumn {
                            column_id: candidate.column_id,
                            values: select_vector_rows(&candidate.values, &retained),
                        })
                        .ok_or(ColumnarError::Corrupt(
                            "row group is missing a projected column",
                        ))
                })
                .collect::<Result<Vec<_>, _>>()?;
            statistics.column_chunks_read = statistics
                .column_chunks_read
                .saturating_add(u64::try_from(selected.len()).unwrap_or(u64::MAX));
            for column in &selected {
                statistics.bytes_read = statistics
                    .bytes_read
                    .saturating_add(vector_encoded_bytes(&column.values));
            }
            batches.push(ColumnarBatch {
                row_count: retained.len(),
                columns: selected,
            });
        }
        if !self.overlay.live.is_empty() {
            let mut live = self.overlay.live.iter().collect::<Vec<_>>();
            live.sort_by_key(|(key, _)| version_key_sort_key(**key));
            let positions = columns
                .iter()
                .map(|column| {
                    self.metadata
                        .columns
                        .iter()
                        .position(|candidate| candidate.column_id == *column)
                        .ok_or(ColumnarError::UnknownColumn(*column))
                })
                .collect::<Result<Vec<_>, _>>()?;
            let rows = live
                .into_iter()
                .map(|(_, row)| {
                    positions
                        .iter()
                        .map(|position| row[*position].clone())
                        .collect()
                })
                .collect::<Vec<Vec<ScalarValue>>>();
            let specs = columns
                .iter()
                .map(|column| {
                    self.metadata
                        .columns
                        .iter()
                        .find(|candidate| candidate.column_id == *column)
                        .cloned()
                        .ok_or(ColumnarError::UnknownColumn(*column))
                })
                .collect::<Result<Vec<_>, _>>()?;
            let delta_group = encode_row_group(&specs, &rows)?;
            statistics.delta_rows_emitted = rows.len() as u64;
            statistics.rows_read = statistics.rows_read.saturating_add(rows.len() as u64);
            statistics.column_chunks_read = statistics
                .column_chunks_read
                .saturating_add(delta_group.columns.len() as u64);
            statistics.bytes_read = statistics.bytes_read.saturating_add(
                delta_group
                    .columns
                    .iter()
                    .map(|column| vector_encoded_bytes(&column.values))
                    .sum::<u64>(),
            );
            batches.push(ColumnarBatch {
                row_count: rows.len(),
                columns: delta_group.columns,
            });
        }
        statistics.merged_rows = statistics.rows_read;
        Ok((batches, statistics))
    }

    pub fn drop_files(self) -> Result<(), ColumnarError> {
        let manifest = self.root.join(MANIFEST_FILE);
        let segment = self.root.join(&self.segment_file);
        match fs::remove_file(&manifest) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(ColumnarError::Io(error)),
        }
        match fs::remove_file(segment) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(ColumnarError::Io(error)),
        }
        if let Some(incremental) = &self.metadata.incremental {
            for delta in &incremental.delta_segments {
                match fs::remove_file(self.root.join(&delta.file)) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(ColumnarError::Io(error)),
                }
            }
        }
        sync_directory(&self.root)
    }

    /// Removes only this immutable generation after a newer manifest is durable.
    pub fn retire_segment(self) -> Result<(), ColumnarError> {
        match fs::remove_file(self.root.join(&self.segment_file)) {
            Ok(()) => {
                if let Some(incremental) = &self.metadata.incremental {
                    for delta in &incremental.delta_segments {
                        match fs::remove_file(self.root.join(&delta.file)) {
                            Ok(()) => {}
                            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                            Err(error) => return Err(ColumnarError::Io(error)),
                        }
                    }
                }
                sync_directory(&self.root)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(ColumnarError::Io(error)),
        }
    }
}

fn select_vector_rows(vector: &ColumnarVector, rows: &[usize]) -> ColumnarVector {
    let mut validity = vec![0_u8; rows.len().div_ceil(8)];
    for (output, input) in rows.iter().copied().enumerate() {
        let source = match vector {
            ColumnarVector::Bool { validity, .. }
            | ColumnarVector::Int64 { validity, .. }
            | ColumnarVector::UInt64 { validity, .. }
            | ColumnarVector::Text { validity, .. } => validity,
        };
        if valid_at(source, input) {
            set_valid(&mut validity, output);
        }
    }
    match vector {
        ColumnarVector::Bool { values, .. } => ColumnarVector::Bool {
            values: rows.iter().map(|row| values[*row]).collect(),
            validity,
        },
        ColumnarVector::Int64 { values, .. } => ColumnarVector::Int64 {
            values: rows.iter().map(|row| values[*row]).collect(),
            validity,
        },
        ColumnarVector::UInt64 { values, .. } => ColumnarVector::UInt64 {
            values: rows.iter().map(|row| values[*row]).collect(),
            validity,
        },
        ColumnarVector::Text { offsets, bytes, .. } => {
            let mut selected_offsets = Vec::with_capacity(rows.len() + 1);
            let mut selected_bytes = Vec::new();
            selected_offsets.push(0);
            for row in rows {
                let start = offsets[*row] as usize;
                let end = offsets[*row + 1] as usize;
                selected_bytes.extend_from_slice(&bytes[start..end]);
                selected_offsets.push(u32::try_from(selected_bytes.len()).unwrap_or(u32::MAX));
            }
            ColumnarVector::Text {
                offsets: selected_offsets,
                bytes: selected_bytes,
                validity,
            }
        }
    }
}

fn version_key_sort_key(key: StorageVersionKey) -> (u8, u64, u64, u64) {
    match key {
        StorageVersionKey::Heap { row_id, .. } => (
            1,
            row_id.page.0,
            u64::from(row_id.slot),
            u64::from(row_id.generation),
        ),
        StorageVersionKey::Lsm {
            row_id, version, ..
        } => (2, row_id.0, version.0, 0),
    }
}

fn vector_encoded_bytes(vector: &ColumnarVector) -> u64 {
    let (validity, data) = match vector {
        ColumnarVector::Bool { values, validity } => (validity.len(), values.len()),
        ColumnarVector::Int64 { values, validity } => {
            (validity.len(), values.len().saturating_mul(8))
        }
        ColumnarVector::UInt64 { values, validity } => {
            (validity.len(), values.len().saturating_mul(8))
        }
        ColumnarVector::Text {
            offsets,
            bytes,
            validity,
        } => (
            validity.len(),
            offsets.len().saturating_mul(4).saturating_add(bytes.len()),
        ),
    };
    u64::try_from(validity.saturating_add(data)).unwrap_or(u64::MAX)
}

#[derive(Debug)]
pub enum ColumnarError {
    Io(std::io::Error),
    Schema(netbadb_schema::SchemaError),
    InvalidInput(&'static str),
    InvalidFormat(&'static str),
    Corrupt(&'static str),
    UnsupportedVersion(u16),
    ChecksumMismatch { path: PathBuf },
    IdentityMismatch(&'static str),
    UnknownColumn(ColumnId),
    TypeMismatch(ColumnId),
    ResourceLimit { resource: &'static str, value: u64 },
}

impl fmt::Display for ColumnarError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "columnar I/O error: {error}"),
            Self::Schema(error) => write!(formatter, "columnar schema error: {error}"),
            Self::InvalidInput(message) => write!(formatter, "invalid columnar input: {message}"),
            Self::InvalidFormat(message) => write!(formatter, "invalid columnar format: {message}"),
            Self::Corrupt(message) => write!(formatter, "corrupt columnar projection: {message}"),
            Self::UnsupportedVersion(version) => {
                write!(formatter, "unsupported columnar format version {version}")
            }
            Self::ChecksumMismatch { path } => {
                write!(
                    formatter,
                    "columnar checksum mismatch in {}",
                    path.display()
                )
            }
            Self::IdentityMismatch(identity) => {
                write!(formatter, "columnar {identity} identity mismatch")
            }
            Self::UnknownColumn(column) => {
                write!(formatter, "columnar projection has no column {}", column.0)
            }
            Self::TypeMismatch(column) => {
                write!(
                    formatter,
                    "columnar value type mismatch for column {}",
                    column.0
                )
            }
            Self::ResourceLimit { resource, value } => {
                write!(formatter, "columnar {resource} exceeds limit: {value}")
            }
        }
    }
}

impl Error for ColumnarError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Schema(error) => Some(error),
            _ => None,
        }
    }
}

fn resolve_columns(
    table: &TableDef,
    columns: &[ColumnId],
) -> Result<Vec<ColumnarColumnSpec>, ColumnarError> {
    if columns.is_empty() {
        return Err(ColumnarError::InvalidInput("projection has no columns"));
    }
    let mut seen = Vec::new();
    columns
        .iter()
        .map(|column_id| {
            if seen.contains(column_id) {
                return Err(ColumnarError::InvalidInput("duplicate projected column"));
            }
            seen.push(*column_id);
            let column = table
                .column_by_id(*column_id)
                .ok_or(ColumnarError::UnknownColumn(*column_id))?;
            Ok(ColumnarColumnSpec {
                column_id: *column_id,
                physical_type: column.semantic_type().physical,
                nullable: column.nullable,
            })
        })
        .collect()
}

fn encode_row_group(
    columns: &[ColumnarColumnSpec],
    rows: &[Vec<ScalarValue>],
) -> Result<RowGroup, ColumnarError> {
    let mut vectors = Vec::with_capacity(columns.len());
    let mut statistics = Vec::with_capacity(columns.len());
    for (position, column) in columns.iter().enumerate() {
        let values = rows
            .iter()
            .map(|row| {
                row.get(position)
                    .ok_or(ColumnarError::InvalidInput("short row"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let vector = vector_from_values(column, &values)?;
        let mut stats = statistics_for_values(&values)?;
        stats.encoded_bytes = vector_encoded_bytes(&vector);
        vectors.push(ColumnarBatchColumn {
            column_id: column.column_id,
            values: vector,
        });
        statistics.push((column.column_id, stats));
    }
    Ok(RowGroup {
        rows: u32::try_from(rows.len())
            .map_err(|_| ColumnarError::InvalidInput("row-group row count overflow"))?,
        source_versions: None,
        columns: vectors,
        statistics,
    })
}

fn encode_versioned_row_group(
    columns: &[ColumnarColumnSpec],
    rows: &[(StorageVersionKey, Vec<ScalarValue>)],
) -> Result<RowGroup, ColumnarError> {
    let values = rows
        .iter()
        .map(|(_, values)| values.clone())
        .collect::<Vec<_>>();
    let mut group = encode_row_group(columns, &values)?;
    group.source_versions = Some(rows.iter().map(|(key, _)| *key).collect());
    Ok(group)
}

fn vector_from_values(
    spec: &ColumnarColumnSpec,
    values: &[&ScalarValue],
) -> Result<ColumnarVector, ColumnarError> {
    let mut validity = vec![0_u8; values.len().div_ceil(8)];
    match spec.physical_type {
        PhysicalType::Bool => {
            let mut output = Vec::with_capacity(values.len());
            for (row, value) in values.iter().enumerate() {
                match value {
                    ScalarValue::Bool(value) => {
                        set_valid(&mut validity, row);
                        output.push(*value);
                    }
                    ScalarValue::Null if spec.nullable => output.push(false),
                    _ => return Err(ColumnarError::TypeMismatch(spec.column_id)),
                }
            }
            Ok(ColumnarVector::Bool {
                values: output,
                validity,
            })
        }
        PhysicalType::Int64 => {
            let mut output = Vec::with_capacity(values.len());
            for (row, value) in values.iter().enumerate() {
                match value {
                    ScalarValue::Int64(value) => {
                        set_valid(&mut validity, row);
                        output.push(*value);
                    }
                    ScalarValue::Null if spec.nullable => output.push(0),
                    _ => return Err(ColumnarError::TypeMismatch(spec.column_id)),
                }
            }
            Ok(ColumnarVector::Int64 {
                values: output,
                validity,
            })
        }
        PhysicalType::UInt64 => {
            let mut output = Vec::with_capacity(values.len());
            for (row, value) in values.iter().enumerate() {
                match value {
                    ScalarValue::UInt64(value) => {
                        set_valid(&mut validity, row);
                        output.push(*value);
                    }
                    ScalarValue::Null if spec.nullable => output.push(0),
                    _ => return Err(ColumnarError::TypeMismatch(spec.column_id)),
                }
            }
            Ok(ColumnarVector::UInt64 {
                values: output,
                validity,
            })
        }
        PhysicalType::Text => {
            let mut offsets = Vec::with_capacity(values.len() + 1);
            let mut bytes = Vec::new();
            offsets.push(0);
            for (row, value) in values.iter().enumerate() {
                match value {
                    ScalarValue::Text(value) => {
                        set_valid(&mut validity, row);
                        bytes.extend_from_slice(value.as_bytes());
                    }
                    ScalarValue::Null if spec.nullable => {}
                    _ => return Err(ColumnarError::TypeMismatch(spec.column_id)),
                }
                offsets.push(u32::try_from(bytes.len()).map_err(|_| {
                    ColumnarError::ResourceLimit {
                        resource: "text payload bytes",
                        value: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
                    }
                })?);
            }
            Ok(ColumnarVector::Text {
                offsets,
                bytes,
                validity,
            })
        }
    }
}

fn statistics_for_values(
    values: &[&ScalarValue],
) -> Result<ColumnarColumnStatistics, ColumnarError> {
    let mut null_count = 0_u64;
    let mut minimum: Option<ScalarValue> = None;
    let mut maximum: Option<ScalarValue> = None;
    for value in values {
        if matches!(value, ScalarValue::Null) {
            null_count = null_count.saturating_add(1);
            continue;
        }
        if minimum
            .as_ref()
            .is_none_or(|current| compare_values(value, current) == Ordering::Less)
        {
            minimum = Some((*value).clone());
        }
        if maximum
            .as_ref()
            .is_none_or(|current| compare_values(value, current) == Ordering::Greater)
        {
            maximum = Some((*value).clone());
        }
    }
    Ok(ColumnarColumnStatistics {
        null_count,
        minimum,
        maximum,
        encoded_bytes: 0,
    })
}

fn group_cannot_match(group: &RowGroup, constraint: &ColumnarConstraint) -> bool {
    let Some((_, stats)) = group
        .statistics
        .iter()
        .find(|(column, _)| *column == constraint.column_id)
    else {
        return false;
    };
    let Some(minimum) = stats.minimum.as_ref() else {
        return true;
    };
    let Some(maximum) = stats.maximum.as_ref() else {
        return true;
    };
    if let Some((lower, inclusive)) = &constraint.lower {
        let ordering = compare_values(maximum, lower);
        if ordering == Ordering::Less || (!inclusive && ordering == Ordering::Equal) {
            return true;
        }
    }
    if let Some((upper, inclusive)) = &constraint.upper {
        let ordering = compare_values(minimum, upper);
        if ordering == Ordering::Greater || (!inclusive && ordering == Ordering::Equal) {
            return true;
        }
    }
    false
}

fn validate_versioned_rows(
    storage_id: StorageId,
    kind: SnapshotKind,
    columns: &[ColumnarColumnSpec],
    rows: &[(StorageVersionKey, Vec<ScalarValue>)],
) -> Result<(), ColumnarError> {
    let mut seen = HashSet::with_capacity(rows.len());
    for (key, values) in rows {
        validate_version_key(*key, storage_id, kind)?;
        if !seen.insert(*key) {
            return Err(ColumnarError::InvalidInput("duplicate base source version"));
        }
        if values.len() != columns.len() {
            return Err(ColumnarError::InvalidInput(
                "row width differs from projection",
            ));
        }
    }
    Ok(())
}

fn validate_version_key(
    key: StorageVersionKey,
    storage_id: StorageId,
    kind: SnapshotKind,
) -> Result<(), ColumnarError> {
    if storage_id.0 == 0 || key.storage_id() != storage_id {
        return Err(ColumnarError::IdentityMismatch("version storage"));
    }
    match (kind, key) {
        (SnapshotKind::Heap, StorageVersionKey::Heap { row_id, .. })
            if row_id.page.0 != 0 && row_id.generation != 0 =>
        {
            Ok(())
        }
        (
            SnapshotKind::Lsm,
            StorageVersionKey::Lsm {
                row_id, version, ..
            },
        ) if row_id.0 != 0 && version.0 != 0 => Ok(()),
        (SnapshotKind::Heap, StorageVersionKey::Heap { .. }) => {
            Err(ColumnarError::Corrupt("invalid Heap version identity"))
        }
        (SnapshotKind::Lsm, StorageVersionKey::Lsm { .. }) => {
            Err(ColumnarError::Corrupt("invalid LSM version identity"))
        }
        _ => Err(ColumnarError::IdentityMismatch("version engine kind")),
    }
}

fn encode_version_key(
    output: &mut Vec<u8>,
    key: StorageVersionKey,
    storage_id: StorageId,
) -> Result<(), ColumnarError> {
    let kind = match key {
        StorageVersionKey::Heap { .. } => SnapshotKind::Heap,
        StorageVersionKey::Lsm { .. } => SnapshotKind::Lsm,
    };
    validate_version_key(key, storage_id, kind)?;
    match key {
        StorageVersionKey::Heap { row_id, .. } => {
            output.push(1);
            output.extend_from_slice(&[0; 7]);
            push_u64(output, row_id.page.0);
            push_u16(output, row_id.slot);
            push_u32(output, row_id.generation);
            push_u16(output, 0);
        }
        StorageVersionKey::Lsm {
            row_id, version, ..
        } => {
            output.push(2);
            output.extend_from_slice(&[0; 7]);
            push_u64(output, row_id.0);
            push_u64(output, version.0);
        }
    }
    Ok(())
}

fn decode_version_key(
    reader: &mut Reader<'_>,
    storage_id: StorageId,
    expected_kind: SnapshotKind,
) -> Result<StorageVersionKey, ColumnarError> {
    let tag = reader.u8()?;
    if reader.take(7)?.iter().any(|byte| *byte != 0) {
        return Err(ColumnarError::Corrupt(
            "version key reserved bytes are nonzero",
        ));
    }
    let key = match tag {
        1 => {
            let page = PageId(reader.u64()?);
            let slot = reader.u16()?;
            let generation = reader.u32()?;
            if reader.u16()? != 0 {
                return Err(ColumnarError::Corrupt(
                    "Heap key reserved bytes are nonzero",
                ));
            }
            StorageVersionKey::Heap {
                storage_id,
                row_id: RowId {
                    page,
                    slot,
                    generation,
                },
            }
        }
        2 => StorageVersionKey::Lsm {
            storage_id,
            row_id: LsmRowId(reader.u64()?),
            version: LsmCommitSeq(reader.u64()?),
        },
        _ => return Err(ColumnarError::Corrupt("unknown version key tag")),
    };
    validate_version_key(key, storage_id, expected_kind)?;
    Ok(key)
}

fn project_change_batches(
    table: &TableDef,
    metadata: &ColumnarProjectionMetadata,
    batches: &[ChangeBatch],
) -> Result<Vec<ProjectedDeltaMutation>, ColumnarError> {
    let incremental = metadata
        .incremental
        .as_ref()
        .ok_or(ColumnarError::InvalidInput(
            "snapshot projection cannot consume changes",
        ))?;
    let positions = metadata
        .columns
        .iter()
        .map(|column| {
            table
                .columns
                .iter()
                .position(|candidate| candidate.id == column.column_id)
                .ok_or(ColumnarError::UnknownColumn(column.column_id))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut expected = incremental.applied_frontier;
    let mut projected = Vec::new();
    for batch in batches {
        if batch.table_id != metadata.table_id
            || batch.storage_id != metadata.source_storage_id
            || batch.schema_fingerprint != metadata.schema_fingerprint
            || batch.before != expected
            || batch.after.0 <= batch.before.0
        {
            return Err(ColumnarError::IdentityMismatch(
                "change batch context or frontier",
            ));
        }
        expected = batch.after;
        for mutation in &batch.mutations {
            let value = match mutation {
                StorageChange::Insert { new_version, after } => ProjectedDeltaMutation {
                    old_version: None,
                    new_version: Some(*new_version),
                    after: Some(project_after(after, &positions)?),
                },
                StorageChange::Update {
                    old_version,
                    new_version,
                    after,
                } => {
                    if old_version == new_version {
                        return Err(ColumnarError::Corrupt("update preserves version identity"));
                    }
                    ProjectedDeltaMutation {
                        old_version: Some(*old_version),
                        new_version: Some(*new_version),
                        after: Some(project_after(after, &positions)?),
                    }
                }
                StorageChange::Delete { old_version } => ProjectedDeltaMutation {
                    old_version: Some(*old_version),
                    new_version: None,
                    after: None,
                },
            };
            if let Some(key) = value.old_version {
                validate_version_key(key, metadata.source_storage_id, metadata.source_token.kind)?;
            }
            if let Some(key) = value.new_version {
                validate_version_key(key, metadata.source_storage_id, metadata.source_token.kind)?;
            }
            projected.push(value);
        }
    }
    Ok(projected)
}

fn project_after(
    after: &[ScalarValue],
    positions: &[usize],
) -> Result<Vec<ScalarValue>, ColumnarError> {
    positions
        .iter()
        .map(|position| {
            after
                .get(*position)
                .cloned()
                .ok_or(ColumnarError::Corrupt("change after-image is too short"))
        })
        .collect()
}

fn apply_overlay(
    overlay: &mut DeltaOverlay,
    mutations: &[ProjectedDeltaMutation],
) -> Result<(), ColumnarError> {
    for mutation in mutations {
        if let Some(old) = mutation.old_version {
            if !overlay.suppressed.insert(old) {
                return Err(ColumnarError::Corrupt(
                    "one version has multiple durable delta transitions",
                ));
            }
            overlay.live.remove(&old);
        }
        match (mutation.new_version, mutation.after.as_ref()) {
            (Some(new), Some(after)) => {
                if overlay.suppressed.contains(&new)
                    || overlay.live.insert(new, after.clone()).is_some()
                {
                    return Err(ColumnarError::Corrupt(
                        "duplicate or suppressed new delta version",
                    ));
                }
            }
            (None, None) => {}
            _ => return Err(ColumnarError::Corrupt("invalid delta mutation image")),
        }
    }
    Ok(())
}

fn encode_delta(
    metadata: &ColumnarProjectionMetadata,
    batches: &[ChangeBatch],
    mutations: &[ProjectedDeltaMutation],
) -> Result<Vec<u8>, ColumnarError> {
    let incremental = metadata
        .incremental
        .as_ref()
        .ok_or(ColumnarError::InvalidInput(
            "snapshot projection cannot encode delta",
        ))?;
    let before = batches[0].before;
    let after = batches
        .last()
        .ok_or(ColumnarError::InvalidInput("empty delta"))?
        .after;
    let after_rows = mutations
        .iter()
        .filter_map(|mutation| mutation.after.clone())
        .collect::<Vec<_>>();
    let mut output = Vec::new();
    output.extend_from_slice(DELTA_MAGIC);
    push_u16(&mut output, DELTA_FORMAT_VERSION);
    push_u16(&mut output, 0);
    push_u64(&mut output, metadata.id.0);
    push_u64(&mut output, metadata.generation.0);
    push_u64(&mut output, metadata.table_id.0);
    push_u64(&mut output, metadata.source_storage_id.0);
    output.extend_from_slice(metadata.schema_fingerprint.as_bytes());
    output.push(snapshot_tag(metadata.source_token.kind));
    output.extend_from_slice(&[0; 7]);
    push_u64(&mut output, incremental.stream_generation.0);
    push_u64(&mut output, before.0);
    push_u64(&mut output, after.0);
    push_u32(
        &mut output,
        u32::try_from(batches.len())
            .map_err(|_| ColumnarError::InvalidInput("batch count overflow"))?,
    );
    push_u64(&mut output, mutations.len() as u64);
    push_u64(&mut output, after_rows.len() as u64);
    push_u32(&mut output, metadata.columns.len() as u32);
    let mut mutation_position = 0_usize;
    let mut after_index = 0_u64;
    for batch in batches {
        push_u64(&mut output, batch.before.0);
        push_u64(&mut output, batch.after.0);
        push_u32(&mut output, batch.mutations.len() as u32);
        for _ in &batch.mutations {
            let mutation = &mutations[mutation_position];
            mutation_position += 1;
            match (
                mutation.old_version,
                mutation.new_version,
                mutation.after.as_ref(),
            ) {
                (None, Some(new), Some(_)) => {
                    output.push(1);
                    encode_version_key(&mut output, new, metadata.source_storage_id)?;
                    push_u64(&mut output, after_index);
                    after_index += 1;
                }
                (Some(old), Some(new), Some(_)) => {
                    output.push(2);
                    encode_version_key(&mut output, old, metadata.source_storage_id)?;
                    encode_version_key(&mut output, new, metadata.source_storage_id)?;
                    push_u64(&mut output, after_index);
                    after_index += 1;
                }
                (Some(old), None, None) => {
                    output.push(3);
                    encode_version_key(&mut output, old, metadata.source_storage_id)?;
                }
                _ => return Err(ColumnarError::InvalidInput("invalid projected mutation")),
            }
        }
    }
    let group = encode_row_group(&metadata.columns, &after_rows)?;
    for (column, batch) in metadata.columns.iter().zip(&group.columns) {
        let stats = group
            .statistics
            .iter()
            .find(|(id, _)| *id == column.column_id)
            .map(|(_, stats)| stats)
            .ok_or(ColumnarError::InvalidInput(
                "delta column statistics missing",
            ))?;
        encode_column_chunk(&mut output, column, &batch.values, stats)?;
    }
    append_checksum(&mut output);
    Ok(output)
}

fn decode_delta(
    bytes: &[u8],
    metadata: &ColumnarProjectionMetadata,
    expected_segment: &ColumnarDeltaSegmentMetadata,
) -> Result<Vec<ProjectedDeltaMutation>, ColumnarError> {
    validate_checksum(bytes)?;
    let payload = bytes
        .get(..bytes.len().saturating_sub(4))
        .ok_or(ColumnarError::InvalidFormat("delta is truncated"))?;
    let mut reader = Reader::new(payload);
    reader.expect(DELTA_MAGIC)?;
    let version = reader.u16()?;
    if version != DELTA_FORMAT_VERSION {
        return Err(ColumnarError::UnsupportedVersion(version));
    }
    if reader.u16()? != 0 {
        return Err(ColumnarError::Corrupt("delta reserved field is nonzero"));
    }
    if ColumnarProjectionId(reader.u64()?) != metadata.id
        || ColumnarGeneration(reader.u64()?) != metadata.generation
        || TableId(reader.u64()?) != metadata.table_id
        || StorageId(reader.u64()?) != metadata.source_storage_id
        || SchemaFingerprint::from_bytes(reader.array()?) != metadata.schema_fingerprint
    {
        return Err(ColumnarError::IdentityMismatch("delta projection context"));
    }
    let kind = decode_snapshot_tag(reader.u8()?)?;
    if kind != metadata.source_token.kind || reader.take(7)?.iter().any(|byte| *byte != 0) {
        return Err(ColumnarError::IdentityMismatch("delta engine kind"));
    }
    let incremental = metadata
        .incremental
        .as_ref()
        .ok_or(ColumnarError::Corrupt("delta on snapshot projection"))?;
    if ChangeStreamGeneration(reader.u64()?) != incremental.stream_generation {
        return Err(ColumnarError::IdentityMismatch("delta stream generation"));
    }
    let before = StorageDataVersion(reader.u64()?);
    let after = StorageDataVersion(reader.u64()?);
    if before != expected_segment.before || after != expected_segment.after {
        return Err(ColumnarError::IdentityMismatch("delta frontier"));
    }
    let batch_count = reader.u32()?;
    if batch_count == 0 || batch_count > crate::CHANGE_LOG_MAX_MUTATIONS {
        return Err(ColumnarError::ResourceLimit {
            resource: "delta batch count",
            value: u64::from(batch_count),
        });
    }
    let mutation_count = reader.u64()?;
    let after_count = reader.u64()?;
    if mutation_count != expected_segment.mutation_count
        || after_count != expected_segment.after_row_count
        || mutation_count > u64::from(crate::CHANGE_LOG_MAX_MUTATIONS)
    {
        return Err(ColumnarError::Corrupt("delta counts mismatch"));
    }
    if reader.u32()? as usize != metadata.columns.len() {
        return Err(ColumnarError::Corrupt("delta column count mismatch"));
    }
    let mut descriptors = Vec::with_capacity(mutation_count as usize);
    let mut frontier = before;
    for _ in 0..batch_count {
        let batch_before = StorageDataVersion(reader.u64()?);
        let batch_after = StorageDataVersion(reader.u64()?);
        let count = reader.u32()?;
        if batch_before != frontier || batch_after.0 <= batch_before.0 {
            return Err(ColumnarError::Corrupt("broken delta batch frontier"));
        }
        frontier = batch_after;
        for _ in 0..count {
            let descriptor = match reader.u8()? {
                1 => (
                    None,
                    Some(decode_version_key(
                        &mut reader,
                        metadata.source_storage_id,
                        kind,
                    )?),
                    Some(reader.u64()?),
                ),
                2 => {
                    let old = decode_version_key(&mut reader, metadata.source_storage_id, kind)?;
                    let new = decode_version_key(&mut reader, metadata.source_storage_id, kind)?;
                    if old == new {
                        return Err(ColumnarError::Corrupt("update preserves version identity"));
                    }
                    (Some(old), Some(new), Some(reader.u64()?))
                }
                3 => (
                    Some(decode_version_key(
                        &mut reader,
                        metadata.source_storage_id,
                        kind,
                    )?),
                    None,
                    None,
                ),
                _ => return Err(ColumnarError::Corrupt("unknown delta mutation tag")),
            };
            descriptors.push(descriptor);
        }
    }
    if frontier != after || descriptors.len() as u64 != mutation_count {
        return Err(ColumnarError::Corrupt(
            "delta chain or mutation count mismatch",
        ));
    }
    let rows_u32 = u32::try_from(after_count).map_err(|_| ColumnarError::ResourceLimit {
        resource: "delta after rows",
        value: after_count,
    })?;
    let mut vectors = Vec::with_capacity(metadata.columns.len());
    for column in &metadata.columns {
        vectors.push(decode_column_chunk(&mut reader, column, rows_u32)?.0);
    }
    reader.finish()?;
    let mut mutations = Vec::with_capacity(descriptors.len());
    let mut used = HashSet::new();
    for (old_version, new_version, index) in descriptors {
        let after_row = match index {
            Some(index) => {
                if index >= after_count || !used.insert(index) {
                    return Err(ColumnarError::Corrupt("invalid delta after-row reference"));
                }
                let row = usize::try_from(index)
                    .map_err(|_| ColumnarError::Corrupt("after-row index overflow"))?;
                Some(
                    vectors
                        .iter()
                        .map(|vector| vector.value(row))
                        .collect::<Result<Vec<_>, _>>()?,
                )
            }
            None => None,
        };
        mutations.push(ProjectedDeltaMutation {
            old_version,
            new_version,
            after: after_row,
        });
    }
    if used.len() as u64 != after_count {
        return Err(ColumnarError::Corrupt("unreferenced delta after row"));
    }
    Ok(mutations)
}

fn write_new_synced(path: &Path, bytes: &[u8]) -> Result<(), ColumnarError> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(ColumnarError::Io)?;
    file.write_all(bytes).map_err(ColumnarError::Io)?;
    file.sync_data().map_err(ColumnarError::Io)
}

fn write_advance_file_synced(
    path: &Path,
    bytes: &[u8],
    created_point: &str,
    written_point: &str,
    synced_point: &str,
) -> Result<(), ColumnarError> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(ColumnarError::Io)?;
    crash(created_point);
    file.write_all(bytes).map_err(ColumnarError::Io)?;
    crash(written_point);
    file.sync_data().map_err(ColumnarError::Io)?;
    crash(synced_point);
    Ok(())
}

fn sync_directory(path: &Path) -> Result<(), ColumnarError> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(ColumnarError::Io)
}

fn read_bounded(path: &Path) -> Result<Vec<u8>, ColumnarError> {
    let mut file = File::open(path).map_err(ColumnarError::Io)?;
    let length = file.metadata().map_err(ColumnarError::Io)?.len();
    if length > MAX_FILE_BYTES {
        return Err(ColumnarError::ResourceLimit {
            resource: "file bytes",
            value: length,
        });
    }
    let capacity = usize::try_from(length).map_err(|_| ColumnarError::ResourceLimit {
        resource: "file bytes",
        value: length,
    })?;
    let mut bytes = Vec::with_capacity(capacity);
    file.read_to_end(&mut bytes).map_err(ColumnarError::Io)?;
    Ok(bytes)
}

fn encode_manifest(
    metadata: &ColumnarProjectionMetadata,
    segment_id: ColumnarSegmentId,
    segment_file: &str,
    segment_checksum: u32,
) -> Result<Vec<u8>, ColumnarError> {
    let mut output = Vec::new();
    output.extend_from_slice(MANIFEST_MAGIC);
    let version = if metadata.incremental.is_some() {
        INCREMENTAL_FORMAT_VERSION
    } else {
        SNAPSHOT_FORMAT_VERSION
    };
    push_u16(&mut output, version);
    push_u16(&mut output, 0);
    push_u64(&mut output, metadata.id.0);
    push_u64(&mut output, metadata.generation.0);
    push_u64(&mut output, metadata.table_id.0);
    push_u64(&mut output, metadata.source_storage_id.0);
    output.extend_from_slice(metadata.schema_fingerprint.as_bytes());
    output.push(snapshot_tag(metadata.source_token.kind));
    output.extend_from_slice(&[0; 7]);
    push_u64(&mut output, metadata.source_token.epoch);
    push_u64(&mut output, metadata.source_token.sequence);
    push_u64(&mut output, metadata.row_count);
    push_u64(&mut output, metadata.row_group_count);
    push_u64(&mut output, metadata.segment_count);
    push_u64(&mut output, metadata.segment_bytes);
    push_u32(
        &mut output,
        u32::try_from(metadata.columns.len())
            .map_err(|_| ColumnarError::InvalidInput("column count overflow"))?,
    );
    for column in &metadata.columns {
        push_u32(&mut output, column.column_id.0);
        output.push(type_tag(column.physical_type));
        output.push(u8::from(column.nullable));
        push_u16(&mut output, 0);
    }
    push_u64(&mut output, segment_id.0);
    push_string(&mut output, segment_file)?;
    push_u32(&mut output, segment_checksum);
    if let Some(incremental) = &metadata.incremental {
        push_u64(&mut output, incremental.stream_generation.0);
        push_u64(&mut output, incremental.base_frontier.0);
        push_u64(&mut output, incremental.applied_frontier.0);
        push_u64(&mut output, incremental.delta_mutation_count);
        push_u64(&mut output, incremental.delta_live_row_count);
        push_u64(&mut output, incremental.suppressed_version_count);
        push_u64(&mut output, incremental.delta_bytes);
        push_u32(
            &mut output,
            u32::try_from(incremental.delta_segments.len())
                .map_err(|_| ColumnarError::InvalidInput("delta segment count overflow"))?,
        );
        for delta in &incremental.delta_segments {
            push_string(&mut output, &delta.file)?;
            push_u64(&mut output, delta.before.0);
            push_u64(&mut output, delta.after.0);
            push_u64(&mut output, delta.mutation_count);
            push_u64(&mut output, delta.after_row_count);
            push_u64(&mut output, delta.bytes);
            push_u32(&mut output, delta.checksum);
        }
    }
    append_checksum(&mut output);
    Ok(output)
}

struct Manifest {
    metadata: ColumnarProjectionMetadata,
    segment_id: ColumnarSegmentId,
    segment_file: String,
    segment_checksum: u32,
}

fn decode_manifest(bytes: &[u8]) -> Result<Manifest, ColumnarError> {
    validate_checksum(bytes)?;
    let payload = bytes
        .get(..bytes.len().saturating_sub(4))
        .ok_or(ColumnarError::InvalidFormat("manifest is truncated"))?;
    let mut reader = Reader::new(payload);
    reader.expect(MANIFEST_MAGIC)?;
    let version = reader.u16()?;
    if version != SNAPSHOT_FORMAT_VERSION && version != INCREMENTAL_FORMAT_VERSION {
        return Err(ColumnarError::UnsupportedVersion(version));
    }
    if reader.u16()? != 0 {
        return Err(ColumnarError::Corrupt("manifest reserved field is nonzero"));
    }
    let id = ColumnarProjectionId(reader.u64()?);
    let generation = ColumnarGeneration(reader.u64()?);
    let table_id = TableId(reader.u64()?);
    let source_storage_id = StorageId(reader.u64()?);
    let fingerprint = SchemaFingerprint::from_bytes(reader.array()?);
    let kind = decode_snapshot_tag(reader.u8()?)?;
    if reader.take(7)?.iter().any(|byte| *byte != 0) {
        return Err(ColumnarError::Corrupt(
            "manifest snapshot reserved bytes are nonzero",
        ));
    }
    let epoch = reader.u64()?;
    let sequence = reader.u64()?;
    let source_token = StorageSnapshotToken {
        storage_id: source_storage_id,
        kind,
        epoch,
        sequence,
    };
    let row_count = reader.u64()?;
    let row_group_count = reader.u64()?;
    let segment_count = reader.u64()?;
    if version == SNAPSHOT_FORMAT_VERSION && segment_count != 1 {
        return Err(ColumnarError::Corrupt(
            "phase 1 manifest must contain one segment",
        ));
    }
    let segment_bytes = reader.u64()?;
    let column_count = reader.u32()?;
    if column_count == 0 || column_count > MAX_COLUMNS {
        return Err(ColumnarError::ResourceLimit {
            resource: "manifest columns",
            value: u64::from(column_count),
        });
    }
    let mut columns = Vec::with_capacity(column_count as usize);
    for _ in 0..column_count {
        let column_id = ColumnId(reader.u32()?);
        let physical_type = decode_type_tag(reader.u8()?)?;
        let nullable = match reader.u8()? {
            0 => false,
            1 => true,
            _ => return Err(ColumnarError::Corrupt("invalid nullable flag")),
        };
        if reader.u16()? != 0 {
            return Err(ColumnarError::Corrupt(
                "manifest column reserved field is nonzero",
            ));
        }
        if columns
            .iter()
            .any(|column: &ColumnarColumnSpec| column.column_id == column_id)
        {
            return Err(ColumnarError::Corrupt("duplicate manifest column"));
        }
        columns.push(ColumnarColumnSpec {
            column_id,
            physical_type,
            nullable,
        });
    }
    let segment_id = ColumnarSegmentId(reader.u64()?);
    let segment_file = reader.string()?;
    if segment_file.is_empty()
        || Path::new(&segment_file).components().count() != 1
        || segment_file == MANIFEST_FILE
    {
        return Err(ColumnarError::Corrupt("invalid segment file name"));
    }
    let segment_checksum = reader.u32()?;
    let incremental = if version == INCREMENTAL_FORMAT_VERSION {
        let stream_generation = ChangeStreamGeneration(reader.u64()?);
        let base_frontier = StorageDataVersion(reader.u64()?);
        let applied_frontier = StorageDataVersion(reader.u64()?);
        let delta_mutation_count = reader.u64()?;
        let delta_live_row_count = reader.u64()?;
        let suppressed_version_count = reader.u64()?;
        let delta_bytes = reader.u64()?;
        let delta_count = reader.u32()?;
        if delta_count > MAX_ROW_GROUPS {
            return Err(ColumnarError::ResourceLimit {
                resource: "delta segment count",
                value: u64::from(delta_count),
            });
        }
        if segment_count != u64::from(delta_count).saturating_add(1) {
            return Err(ColumnarError::Corrupt("manifest segment count mismatch"));
        }
        if stream_generation.0 == 0 || applied_frontier.0 < base_frontier.0 {
            return Err(ColumnarError::Corrupt("invalid incremental frontier"));
        }
        let mut delta_segments = Vec::with_capacity(delta_count as usize);
        let mut expected = base_frontier;
        let mut accumulated_bytes = 0_u64;
        let mut accumulated_mutations = 0_u64;
        for _ in 0..delta_count {
            let file = reader.string()?;
            if file.is_empty() || Path::new(&file).components().count() != 1 {
                return Err(ColumnarError::Corrupt("invalid delta file name"));
            }
            let before = StorageDataVersion(reader.u64()?);
            let after = StorageDataVersion(reader.u64()?);
            let mutation_count = reader.u64()?;
            let after_row_count = reader.u64()?;
            let bytes = reader.u64()?;
            let checksum = reader.u32()?;
            if before != expected || after.0 <= before.0 || mutation_count == 0 {
                return Err(ColumnarError::Corrupt("broken delta frontier chain"));
            }
            expected = after;
            accumulated_bytes = accumulated_bytes
                .checked_add(bytes)
                .ok_or(ColumnarError::Corrupt("delta byte count overflow"))?;
            accumulated_mutations = accumulated_mutations
                .checked_add(mutation_count)
                .ok_or(ColumnarError::Corrupt("delta mutation count overflow"))?;
            delta_segments.push(ColumnarDeltaSegmentMetadata {
                file,
                before,
                after,
                mutation_count,
                after_row_count,
                bytes,
                checksum,
            });
        }
        if expected != applied_frontier
            || accumulated_bytes != delta_bytes
            || accumulated_mutations != delta_mutation_count
        {
            return Err(ColumnarError::Corrupt("delta inventory totals mismatch"));
        }
        Some(ColumnarIncrementalMetadata {
            stream_generation,
            base_frontier,
            applied_frontier,
            delta_segments,
            delta_mutation_count,
            delta_live_row_count,
            suppressed_version_count,
            delta_bytes,
        })
    } else {
        None
    };
    reader.finish()?;
    Ok(Manifest {
        metadata: ColumnarProjectionMetadata {
            id,
            generation,
            table_id,
            source_storage_id,
            source_token,
            schema_fingerprint: fingerprint,
            columns,
            row_count,
            row_group_count,
            segment_count,
            segment_bytes,
            incremental,
        },
        segment_id,
        segment_file,
        segment_checksum,
    })
}

#[derive(Debug, Clone, Copy)]
struct SegmentIdentity {
    projection_id: ColumnarProjectionId,
    generation: ColumnarGeneration,
    segment_id: ColumnarSegmentId,
    table_id: TableId,
    storage_id: StorageId,
    fingerprint: SchemaFingerprint,
}

fn encode_segment(
    identity: SegmentIdentity,
    columns: &[ColumnarColumnSpec],
    groups: &[RowGroup],
    version: u16,
) -> Result<Vec<u8>, ColumnarError> {
    let mut output = Vec::new();
    output.extend_from_slice(SEGMENT_MAGIC);
    push_u16(&mut output, version);
    push_u16(&mut output, 0);
    push_u64(&mut output, identity.projection_id.0);
    push_u64(&mut output, identity.generation.0);
    push_u64(&mut output, identity.segment_id.0);
    push_u64(&mut output, identity.table_id.0);
    push_u64(&mut output, identity.storage_id.0);
    output.extend_from_slice(identity.fingerprint.as_bytes());
    push_u32(
        &mut output,
        u32::try_from(columns.len())
            .map_err(|_| ColumnarError::InvalidInput("column count overflow"))?,
    );
    push_u32(
        &mut output,
        u32::try_from(groups.len())
            .map_err(|_| ColumnarError::InvalidInput("row-group count overflow"))?,
    );
    for group in groups {
        push_u32(&mut output, group.rows);
        if version == INCREMENTAL_FORMAT_VERSION {
            let keys = group
                .source_versions
                .as_ref()
                .ok_or(ColumnarError::InvalidInput(
                    "incremental row group is missing source identities",
                ))?;
            if keys.len() != group.rows as usize {
                return Err(ColumnarError::InvalidInput(
                    "source identity count differs from row count",
                ));
            }
            for key in keys {
                encode_version_key(&mut output, *key, identity.storage_id)?;
            }
        }
        for (column, batch) in columns.iter().zip(&group.columns) {
            let stats = group
                .statistics
                .iter()
                .find(|(id, _)| *id == column.column_id)
                .map(|(_, stats)| stats)
                .ok_or(ColumnarError::InvalidInput("row-group statistics missing"))?;
            encode_column_chunk(&mut output, column, &batch.values, stats)?;
        }
    }
    append_checksum(&mut output);
    Ok(output)
}

struct DecodedSegment {
    segment_id: ColumnarSegmentId,
    row_groups: Vec<RowGroup>,
}

fn decode_segment(
    bytes: &[u8],
    metadata: &ColumnarProjectionMetadata,
) -> Result<DecodedSegment, ColumnarError> {
    validate_checksum(bytes)?;
    let payload = bytes
        .get(..bytes.len().saturating_sub(4))
        .ok_or(ColumnarError::InvalidFormat("segment is truncated"))?;
    let mut reader = Reader::new(payload);
    reader.expect(SEGMENT_MAGIC)?;
    let version = reader.u16()?;
    if version != SNAPSHOT_FORMAT_VERSION && version != INCREMENTAL_FORMAT_VERSION {
        return Err(ColumnarError::UnsupportedVersion(version));
    }
    if (version == INCREMENTAL_FORMAT_VERSION) != metadata.incremental.is_some() {
        return Err(ColumnarError::IdentityMismatch("segment mode"));
    }
    if reader.u16()? != 0 {
        return Err(ColumnarError::Corrupt("segment reserved field is nonzero"));
    }
    if ColumnarProjectionId(reader.u64()?) != metadata.id
        || ColumnarGeneration(reader.u64()?) != metadata.generation
    {
        return Err(ColumnarError::IdentityMismatch("projection generation"));
    }
    let segment_id = ColumnarSegmentId(reader.u64()?);
    if TableId(reader.u64()?) != metadata.table_id {
        return Err(ColumnarError::IdentityMismatch("segment table"));
    }
    if StorageId(reader.u64()?) != metadata.source_storage_id {
        return Err(ColumnarError::IdentityMismatch("segment source storage"));
    }
    if SchemaFingerprint::from_bytes(reader.array()?) != metadata.schema_fingerprint {
        return Err(ColumnarError::IdentityMismatch("segment schema"));
    }
    let column_count = reader.u32()?;
    if usize::try_from(column_count).ok() != Some(metadata.columns.len()) {
        return Err(ColumnarError::Corrupt("segment column count mismatch"));
    }
    let group_count = reader.u32()?;
    if group_count > MAX_ROW_GROUPS || u64::from(group_count) != metadata.row_group_count {
        return Err(ColumnarError::Corrupt("segment row-group count mismatch"));
    }
    let mut row_groups = Vec::with_capacity(group_count as usize);
    let mut decoded_rows = 0_u64;
    for _ in 0..group_count {
        let rows = reader.u32()?;
        if rows == 0 {
            return Err(ColumnarError::Corrupt("empty row group"));
        }
        decoded_rows = decoded_rows
            .checked_add(u64::from(rows))
            .ok_or(ColumnarError::Corrupt("segment row count overflow"))?;
        let source_versions = if version == INCREMENTAL_FORMAT_VERSION {
            let mut keys = Vec::with_capacity(rows as usize);
            for _ in 0..rows {
                keys.push(decode_version_key(
                    &mut reader,
                    metadata.source_storage_id,
                    metadata.source_token.kind,
                )?);
            }
            Some(keys)
        } else {
            None
        };
        let mut chunks = Vec::with_capacity(metadata.columns.len());
        let mut statistics = Vec::with_capacity(metadata.columns.len());
        for column in &metadata.columns {
            let (values, stats) = decode_column_chunk(&mut reader, column, rows)?;
            chunks.push(ColumnarBatchColumn {
                column_id: column.column_id,
                values,
            });
            statistics.push((column.column_id, stats));
        }
        row_groups.push(RowGroup {
            rows,
            source_versions,
            columns: chunks,
            statistics,
        });
    }
    if decoded_rows != metadata.row_count {
        return Err(ColumnarError::Corrupt("segment row count mismatch"));
    }
    reader.finish()?;
    Ok(DecodedSegment {
        segment_id,
        row_groups,
    })
}

fn encode_column_chunk(
    output: &mut Vec<u8>,
    spec: &ColumnarColumnSpec,
    vector: &ColumnarVector,
    statistics: &ColumnarColumnStatistics,
) -> Result<(), ColumnarError> {
    push_u32(output, spec.column_id.0);
    output.push(type_tag(spec.physical_type));
    output.extend_from_slice(&[0; 3]);
    push_u64(output, statistics.null_count);
    encode_optional_scalar(output, statistics.minimum.as_ref(), spec.physical_type)?;
    encode_optional_scalar(output, statistics.maximum.as_ref(), spec.physical_type)?;
    let (validity, data) = encode_vector_data(vector)?;
    push_u32(
        output,
        u32::try_from(validity.len())
            .map_err(|_| ColumnarError::InvalidInput("validity length overflow"))?,
    );
    push_u64(
        output,
        u64::try_from(data.len())
            .map_err(|_| ColumnarError::InvalidInput("column data length overflow"))?,
    );
    output.extend_from_slice(validity);
    output.extend_from_slice(&data);
    Ok(())
}

fn decode_column_chunk(
    reader: &mut Reader<'_>,
    spec: &ColumnarColumnSpec,
    rows: u32,
) -> Result<(ColumnarVector, ColumnarColumnStatistics), ColumnarError> {
    if ColumnId(reader.u32()?) != spec.column_id {
        return Err(ColumnarError::IdentityMismatch("segment column"));
    }
    if decode_type_tag(reader.u8()?)? != spec.physical_type {
        return Err(ColumnarError::IdentityMismatch("segment column type"));
    }
    if reader.take(3)?.iter().any(|byte| *byte != 0) {
        return Err(ColumnarError::Corrupt(
            "segment column reserved bytes are nonzero",
        ));
    }
    let null_count = reader.u64()?;
    if null_count > u64::from(rows) {
        return Err(ColumnarError::Corrupt(
            "column null count exceeds row count",
        ));
    }
    let minimum = decode_optional_scalar(reader, spec.physical_type)?;
    let maximum = decode_optional_scalar(reader, spec.physical_type)?;
    if minimum.is_some() != maximum.is_some() {
        return Err(ColumnarError::Corrupt("incomplete min/max statistics"));
    }
    let validity_len = reader.u32()?;
    let expected_validity = usize::try_from(rows).unwrap_or(usize::MAX).div_ceil(8);
    if usize::try_from(validity_len).ok() != Some(expected_validity) {
        return Err(ColumnarError::Corrupt("invalid validity bitmap length"));
    }
    let data_len = reader.u64()?;
    if data_len > MAX_TEXT_BYTES {
        return Err(ColumnarError::ResourceLimit {
            resource: "column data bytes",
            value: data_len,
        });
    }
    let validity = reader.take(validity_len as usize)?.to_vec();
    let data_len = usize::try_from(data_len).map_err(|_| ColumnarError::ResourceLimit {
        resource: "column data bytes",
        value: data_len,
    })?;
    let data = reader.take(data_len)?;
    let vector = decode_vector_data(spec.physical_type, rows as usize, validity, data)?;
    let actual_nulls = (0..rows as usize)
        .filter(|row| {
            !valid_at(
                match &vector {
                    ColumnarVector::Bool { validity, .. }
                    | ColumnarVector::Int64 { validity, .. }
                    | ColumnarVector::UInt64 { validity, .. }
                    | ColumnarVector::Text { validity, .. } => validity,
                },
                *row,
            )
        })
        .count() as u64;
    if actual_nulls != null_count {
        return Err(ColumnarError::Corrupt(
            "validity bitmap disagrees with null count",
        ));
    }
    let encoded_bytes = vector_encoded_bytes(&vector);
    Ok((
        vector,
        ColumnarColumnStatistics {
            null_count,
            minimum,
            maximum,
            encoded_bytes,
        },
    ))
}

fn encode_vector_data(vector: &ColumnarVector) -> Result<(&[u8], Vec<u8>), ColumnarError> {
    let mut data = Vec::new();
    let validity = match vector {
        ColumnarVector::Bool { values, validity } => {
            data.extend(values.iter().map(|value| u8::from(*value)));
            validity
        }
        ColumnarVector::Int64 { values, validity } => {
            for value in values {
                data.extend_from_slice(&value.to_le_bytes());
            }
            validity
        }
        ColumnarVector::UInt64 { values, validity } => {
            for value in values {
                data.extend_from_slice(&value.to_le_bytes());
            }
            validity
        }
        ColumnarVector::Text {
            offsets,
            bytes,
            validity,
        } => {
            for offset in offsets {
                data.extend_from_slice(&offset.to_le_bytes());
            }
            data.extend_from_slice(bytes);
            validity
        }
    };
    Ok((validity, data))
}

fn decode_vector_data(
    physical: PhysicalType,
    rows: usize,
    validity: Vec<u8>,
    data: &[u8],
) -> Result<ColumnarVector, ColumnarError> {
    match physical {
        PhysicalType::Bool => {
            if data.len() != rows || data.iter().any(|value| *value > 1) {
                return Err(ColumnarError::Corrupt("invalid bool column data"));
            }
            Ok(ColumnarVector::Bool {
                values: data.iter().map(|value| *value != 0).collect(),
                validity,
            })
        }
        PhysicalType::Int64 => Ok(ColumnarVector::Int64 {
            values: decode_fixed_u64(data, rows)?
                .into_iter()
                .map(|value| i64::from_le_bytes(value.to_le_bytes()))
                .collect(),
            validity,
        }),
        PhysicalType::UInt64 => Ok(ColumnarVector::UInt64 {
            values: decode_fixed_u64(data, rows)?,
            validity,
        }),
        PhysicalType::Text => {
            let offset_count = rows
                .checked_add(1)
                .ok_or(ColumnarError::Corrupt("text offset count overflow"))?;
            let offset_bytes = offset_count
                .checked_mul(4)
                .ok_or(ColumnarError::Corrupt("text offset bytes overflow"))?;
            let (encoded_offsets, bytes) = data
                .split_at_checked(offset_bytes)
                .ok_or(ColumnarError::Corrupt("truncated text offsets"))?;
            let offsets = encoded_offsets
                .chunks_exact(4)
                .map(|chunk| u32::from_le_bytes(chunk.try_into().unwrap_or([0; 4])))
                .collect::<Vec<_>>();
            if offsets.first().copied() != Some(0)
                || offsets.windows(2).any(|pair| pair[0] > pair[1])
                || usize::try_from(offsets.last().copied().unwrap_or(0)).ok() != Some(bytes.len())
            {
                return Err(ColumnarError::Corrupt("invalid text offsets"));
            }
            for row in 0..rows {
                let start = offsets[row] as usize;
                let end = offsets[row + 1] as usize;
                std::str::from_utf8(&bytes[start..end])
                    .map_err(|_| ColumnarError::Corrupt("text payload is not UTF-8"))?;
            }
            Ok(ColumnarVector::Text {
                offsets,
                bytes: bytes.to_vec(),
                validity,
            })
        }
    }
}

fn decode_fixed_u64(data: &[u8], rows: usize) -> Result<Vec<u64>, ColumnarError> {
    if data.len()
        != rows
            .checked_mul(8)
            .ok_or(ColumnarError::Corrupt("fixed data overflow"))?
    {
        return Err(ColumnarError::Corrupt("invalid fixed-width column data"));
    }
    Ok(data
        .chunks_exact(8)
        .map(|chunk| u64::from_le_bytes(chunk.try_into().unwrap_or([0; 8])))
        .collect())
}

fn encode_optional_scalar(
    output: &mut Vec<u8>,
    value: Option<&ScalarValue>,
    physical: PhysicalType,
) -> Result<(), ColumnarError> {
    match value {
        None => output.push(0),
        Some(value) => {
            output.push(1);
            encode_scalar(output, value, physical)?;
        }
    }
    Ok(())
}

fn decode_optional_scalar(
    reader: &mut Reader<'_>,
    physical: PhysicalType,
) -> Result<Option<ScalarValue>, ColumnarError> {
    match reader.u8()? {
        0 => Ok(None),
        1 => decode_scalar(reader, physical).map(Some),
        _ => Err(ColumnarError::Corrupt("invalid optional scalar tag")),
    }
}

fn encode_scalar(
    output: &mut Vec<u8>,
    value: &ScalarValue,
    physical: PhysicalType,
) -> Result<(), ColumnarError> {
    match (physical, value) {
        (PhysicalType::Bool, ScalarValue::Bool(value)) => output.push(u8::from(*value)),
        (PhysicalType::Int64, ScalarValue::Int64(value)) => {
            output.extend_from_slice(&value.to_le_bytes())
        }
        (PhysicalType::UInt64, ScalarValue::UInt64(value)) => {
            output.extend_from_slice(&value.to_le_bytes())
        }
        (PhysicalType::Text, ScalarValue::Text(value)) => {
            push_u32(
                output,
                u32::try_from(value.len())
                    .map_err(|_| ColumnarError::InvalidInput("statistic text too long"))?,
            );
            output.extend_from_slice(value.as_bytes());
        }
        _ => return Err(ColumnarError::InvalidInput("statistic type mismatch")),
    }
    Ok(())
}

fn decode_scalar(
    reader: &mut Reader<'_>,
    physical: PhysicalType,
) -> Result<ScalarValue, ColumnarError> {
    match physical {
        PhysicalType::Bool => match reader.u8()? {
            0 => Ok(ScalarValue::Bool(false)),
            1 => Ok(ScalarValue::Bool(true)),
            _ => Err(ColumnarError::Corrupt("invalid bool statistic")),
        },
        PhysicalType::Int64 => Ok(ScalarValue::Int64(i64::from_le_bytes(reader.array()?))),
        PhysicalType::UInt64 => Ok(ScalarValue::UInt64(reader.u64()?)),
        PhysicalType::Text => Ok(ScalarValue::Text(reader.string()?)),
    }
}

fn push_string(output: &mut Vec<u8>, value: &str) -> Result<(), ColumnarError> {
    push_u32(
        output,
        u32::try_from(value.len()).map_err(|_| ColumnarError::InvalidInput("string too long"))?,
    );
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

fn append_checksum(output: &mut Vec<u8>) {
    let checksum = crc32c::crc32c(output);
    push_u32(output, checksum);
}

fn stored_checksum(bytes: &[u8]) -> Result<u32, ColumnarError> {
    let checksum = bytes
        .get(bytes.len().saturating_sub(4)..)
        .and_then(|value| <[u8; 4]>::try_from(value).ok())
        .map(u32::from_le_bytes)
        .ok_or(ColumnarError::InvalidFormat("file is missing checksum"))?;
    Ok(checksum)
}

fn validate_checksum(bytes: &[u8]) -> Result<(), ColumnarError> {
    let stored = stored_checksum(bytes)?;
    let payload = bytes
        .get(..bytes.len().saturating_sub(4))
        .ok_or(ColumnarError::InvalidFormat("file is truncated"))?;
    if crc32c::crc32c(payload) != stored {
        return Err(ColumnarError::Corrupt("file checksum mismatch"));
    }
    Ok(())
}

const fn snapshot_tag(kind: SnapshotKind) -> u8 {
    match kind {
        SnapshotKind::Heap => 1,
        SnapshotKind::Lsm => 2,
    }
}

fn decode_snapshot_tag(tag: u8) -> Result<SnapshotKind, ColumnarError> {
    match tag {
        1 => Ok(SnapshotKind::Heap),
        2 => Ok(SnapshotKind::Lsm),
        _ => Err(ColumnarError::Corrupt("unknown snapshot token kind")),
    }
}

const fn type_tag(physical: PhysicalType) -> u8 {
    match physical {
        PhysicalType::Bool => 1,
        PhysicalType::Int64 => 2,
        PhysicalType::UInt64 => 3,
        PhysicalType::Text => 4,
    }
}

fn decode_type_tag(tag: u8) -> Result<PhysicalType, ColumnarError> {
    match tag {
        1 => Ok(PhysicalType::Bool),
        2 => Ok(PhysicalType::Int64),
        3 => Ok(PhysicalType::UInt64),
        4 => Ok(PhysicalType::Text),
        _ => Err(ColumnarError::Corrupt("unknown physical type tag")),
    }
}

fn set_valid(validity: &mut [u8], row: usize) {
    validity[row / 8] |= 1 << (row % 8);
}

fn valid_at(validity: &[u8], row: usize) -> bool {
    validity
        .get(row / 8)
        .is_some_and(|byte| byte & (1 << (row % 8)) != 0)
}

fn push_u16(output: &mut Vec<u8>, value: u16) {
    output.extend_from_slice(&value.to_le_bytes());
}
fn push_u32(output: &mut Vec<u8>, value: u32) {
    output.extend_from_slice(&value.to_le_bytes());
}
fn push_u64(output: &mut Vec<u8>, value: u64) {
    output.extend_from_slice(&value.to_le_bytes());
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], ColumnarError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(ColumnarError::Corrupt("decode offset overflow"))?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or(ColumnarError::InvalidFormat("file is truncated"))?;
        self.offset = end;
        Ok(value)
    }

    fn expect(&mut self, expected: &[u8]) -> Result<(), ColumnarError> {
        if self.take(expected.len())? != expected {
            return Err(ColumnarError::InvalidFormat("invalid magic"));
        }
        Ok(())
    }

    fn u8(&mut self) -> Result<u8, ColumnarError> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16, ColumnarError> {
        Ok(u16::from_le_bytes(self.array()?))
    }
    fn u32(&mut self) -> Result<u32, ColumnarError> {
        Ok(u32::from_le_bytes(self.array()?))
    }
    fn u64(&mut self) -> Result<u64, ColumnarError> {
        Ok(u64::from_le_bytes(self.array()?))
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], ColumnarError> {
        self.take(N)?
            .try_into()
            .map_err(|_| ColumnarError::InvalidFormat("file is truncated"))
    }

    fn string(&mut self) -> Result<String, ColumnarError> {
        let length = self.u32()?;
        let value = self.take(length as usize)?;
        Ok(std::str::from_utf8(value)
            .map_err(|_| ColumnarError::Corrupt("string is not UTF-8"))?
            .to_owned())
    }

    fn finish(self) -> Result<(), ColumnarError> {
        if self.offset != self.bytes.len() {
            return Err(ColumnarError::Corrupt("trailing bytes"));
        }
        Ok(())
    }
}

#[cfg(test)]
fn crash(point: &str) {
    if std::env::var("NETBADB_COLUMNAR_DELTA_CRASH_POINT").as_deref() == Ok(point) {
        std::process::exit(89);
    }
}

#[cfg(not(test))]
fn crash(_: &str) {}

#[cfg(test)]
mod tests {
    use super::{ColumnarConstraint, ColumnarError, ColumnarProjection, StorageSnapshotToken};
    use crate::{ChangeBatch, StorageChange, StorageVersionKey};
    use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
    use netbadb_types::{
        ChangeStreamGeneration, ColumnId, ColumnarGeneration, ColumnarProjectionId, LsmCommitSeq,
        LsmRowId, PageId, PhysicalType, RowId, ScalarValue, StorageDataVersion, StorageId, TableId,
        TxnId,
    };
    use std::fs;
    use std::path::PathBuf;
    use std::process::Command;
    use std::time::{SystemTime, UNIX_EPOCH};

    macro_rules! build_projection {
        ($root:expr, $id:expr, $generation:expr, $table:expr, $storage:expr, $token:expr, $columns:expr, $rows:expr, $group_rows:expr $(,)?) => {
            ColumnarProjection::prepare(
                $root,
                $id,
                $generation,
                $table,
                $storage,
                $token,
                $columns,
                $rows,
                $group_rows,
            )
            .and_then(|prepared| prepared.publish())
        };
    }

    fn rewrite_checksum(bytes: &mut [u8]) {
        let payload_length = bytes.len() - 4;
        let checksum = crc32c::crc32c(&bytes[..payload_length]);
        bytes[payload_length..].copy_from_slice(&checksum.to_le_bytes());
    }

    fn replace_u16(bytes: &mut [u8], offset: usize, value: u16) {
        bytes[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
        rewrite_checksum(bytes);
    }

    fn replace_u64(bytes: &mut [u8], offset: usize, value: u64) {
        bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
        rewrite_checksum(bytes);
    }

    fn test_directory(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "netbadb-columnar-{name}-{}-{nonce}",
            std::process::id()
        ))
    }

    fn table() -> TableDef {
        TableDef::new(
            TableId(7),
            "events",
            vec![
                ColumnDef::new(
                    ColumnId(1),
                    "signed",
                    TypeSpec::Physical(PhysicalType::Int64),
                ),
                ColumnDef::new(
                    ColumnId(2),
                    "unsigned",
                    TypeSpec::Physical(PhysicalType::UInt64),
                ),
                ColumnDef::new(ColumnId(3), "flag", TypeSpec::Physical(PhysicalType::Bool))
                    .nullable(true),
                ColumnDef::new(ColumnId(4), "label", TypeSpec::Physical(PhysicalType::Text))
                    .nullable(true),
            ],
        )
    }

    fn rows() -> Vec<Vec<ScalarValue>> {
        vec![
            vec![
                ScalarValue::Int64(-3),
                ScalarValue::UInt64(1),
                ScalarValue::Bool(true),
                ScalarValue::Text("alpha".into()),
            ],
            vec![
                ScalarValue::Int64(4),
                ScalarValue::UInt64(2),
                ScalarValue::Null,
                ScalarValue::Null,
            ],
            vec![
                ScalarValue::Int64(9),
                ScalarValue::UInt64(3),
                ScalarValue::Bool(false),
                ScalarValue::Text("omega".into()),
            ],
        ]
    }

    fn heap_key(storage_id: StorageId, page: u64, slot: u16) -> StorageVersionKey {
        StorageVersionKey::Heap {
            storage_id,
            row_id: RowId {
                page: PageId(page),
                slot,
                generation: 1,
            },
        }
    }

    fn lsm_key(storage_id: StorageId, row: u64, version: u64) -> StorageVersionKey {
        StorageVersionKey::Lsm {
            storage_id,
            row_id: LsmRowId(row),
            version: LsmCommitSeq(version),
        }
    }

    #[test]
    fn incremental_v2_delta_round_trip_suppresses_exact_versions() {
        let directory = test_directory("incremental-round-trip");
        let table = table();
        let storage_id = StorageId(3);
        let base_rows = rows()
            .into_iter()
            .enumerate()
            .map(|(slot, row)| (heap_key(storage_id, 1, slot as u16), row))
            .collect::<Vec<_>>();
        let base = ColumnarProjection::prepare_incremental(
            &directory,
            ColumnarProjectionId(21),
            ColumnarGeneration(1),
            &table,
            storage_id,
            StorageSnapshotToken::heap(storage_id, 19),
            crate::ChangeStreamCursor {
                storage_id,
                generation: ChangeStreamGeneration(2),
                frontier: StorageDataVersion(10),
            },
            &[ColumnId(1), ColumnId(2), ColumnId(3), ColumnId(4)],
            &base_rows,
            Some(2),
        )
        .and_then(|prepared| prepared.publish())
        .expect("publish incremental base");
        assert!(base.is_incremental());
        let new_version = heap_key(storage_id, 2, 0);
        let fingerprint = table.fingerprint().expect("table fingerprint");
        let batch = ChangeBatch {
            sequence: 1,
            physical_txn_id: TxnId(1),
            database_txn_id: None,
            table_id: table.id,
            storage_id,
            schema_fingerprint: fingerprint,
            before: StorageDataVersion(10),
            after: StorageDataVersion(11),
            mutations: vec![
                StorageChange::Update {
                    old_version: base_rows[0].0,
                    new_version,
                    after: vec![
                        ScalarValue::Int64(100),
                        ScalarValue::UInt64(10),
                        ScalarValue::Null,
                        ScalarValue::Text("delta".into()),
                    ],
                },
                StorageChange::Delete {
                    old_version: base_rows[1].0,
                },
            ],
        };
        let advanced = base
            .prepare_advance(&table, &[batch])
            .and_then(|prepared| prepared.publish())
            .expect("publish delta");
        drop(advanced);
        let reopened = ColumnarProjection::open(&directory, &table).expect("reopen v2 chain");
        let incremental = reopened
            .metadata()
            .incremental
            .as_ref()
            .expect("incremental metadata");
        assert_eq!(incremental.base_frontier, StorageDataVersion(10));
        assert_eq!(incremental.applied_frontier, StorageDataVersion(11));
        assert_eq!(incremental.delta_live_row_count, 1);
        assert_eq!(incremental.suppressed_version_count, 2);
        let (batches, statistics) = reopened
            .scan(&[ColumnId(1), ColumnId(4)], &[])
            .expect("scan merged projection");
        let values = batches
            .iter()
            .flat_map(|batch| {
                (0..batch.row_count).map(|row| batch.columns[0].values.value(row).expect("value"))
            })
            .collect::<Vec<_>>();
        assert_eq!(values, vec![ScalarValue::Int64(9), ScalarValue::Int64(100)]);
        assert_eq!(statistics.base_rows_suppressed, 2);
        assert_eq!(statistics.delta_rows_emitted, 1);
        let newest_version = heap_key(storage_id, 3, 0);
        let second = ChangeBatch {
            sequence: 2,
            physical_txn_id: TxnId(2),
            database_txn_id: None,
            table_id: table.id,
            storage_id,
            schema_fingerprint: fingerprint,
            before: StorageDataVersion(11),
            after: StorageDataVersion(12),
            mutations: vec![StorageChange::Update {
                old_version: new_version,
                new_version: newest_version,
                after: vec![
                    ScalarValue::Int64(200),
                    ScalarValue::UInt64(20),
                    ScalarValue::Bool(true),
                    ScalarValue::Null,
                ],
            }],
        };
        let advanced = reopened
            .prepare_advance(&table, &[second])
            .and_then(|prepared| prepared.publish())
            .expect("publish second delta segment");
        drop(advanced);
        let reopened = ColumnarProjection::open(&directory, &table).expect("reopen delta chain");
        let incremental = reopened
            .metadata()
            .incremental
            .as_ref()
            .expect("incremental metadata");
        assert_eq!(incremental.applied_frontier, StorageDataVersion(12));
        assert_eq!(incremental.delta_segments.len(), 2);
        assert_eq!(incremental.suppressed_version_count, 3);
        let values = reopened
            .scan(&[ColumnId(1)], &[])
            .expect("scan second merge")
            .0
            .iter()
            .flat_map(|batch| {
                (0..batch.row_count).map(|row| batch.columns[0].values.value(row).expect("value"))
            })
            .collect::<Vec<_>>();
        assert_eq!(values, vec![ScalarValue::Int64(9), ScalarValue::Int64(200)]);
        let delta_file = incremental.delta_segments[1].file.clone();
        let delta_path = directory.join(delta_file);
        let mut corrupt = fs::read(&delta_path).expect("read delta");
        corrupt[32] ^= 0x20;
        fs::write(&delta_path, corrupt).expect("corrupt delta");
        assert!(matches!(
            ColumnarProjection::open(&directory, &table),
            Err(ColumnarError::ChecksumMismatch { .. }) | Err(ColumnarError::Corrupt(_))
        ));
        fs::remove_dir_all(directory).expect("remove fixture");
    }

    #[test]
    fn nbcs_v2_round_trips_both_key_kinds_and_rejects_identity_corruption() {
        let table = table();
        let heap_directory = test_directory("nbcs-v2-heap-boundaries");
        let heap_storage = StorageId(31);
        let heap_rows = rows()
            .into_iter()
            .enumerate()
            .map(|(slot, values)| (heap_key(heap_storage, 7, slot as u16), values))
            .collect::<Vec<_>>();
        let prepared = ColumnarProjection::prepare_incremental(
            &heap_directory,
            ColumnarProjectionId(31),
            ColumnarGeneration(1),
            &table,
            heap_storage,
            StorageSnapshotToken::heap(heap_storage, 3),
            crate::ChangeStreamCursor {
                storage_id: heap_storage,
                generation: ChangeStreamGeneration(1),
                frontier: StorageDataVersion(3),
            },
            &[ColumnId(1), ColumnId(2), ColumnId(3), ColumnId(4)],
            &heap_rows,
            Some(2),
        )
        .expect("prepare Heap NBCS v2");
        assert_eq!(prepared.row_groups.len(), 2);
        let segment = fs::read(&prepared.segment_tmp).expect("read NBCS v2");
        assert_eq!(
            super::decode_segment(&segment, &prepared.metadata)
                .expect("decode NBCS v2")
                .row_groups,
            prepared.row_groups
        );

        let identity = super::SegmentIdentity {
            projection_id: prepared.metadata.id,
            generation: prepared.metadata.generation,
            segment_id: prepared.segment_id,
            table_id: prepared.metadata.table_id,
            storage_id: prepared.metadata.source_storage_id,
            fingerprint: prepared.metadata.schema_fingerprint,
        };
        let mut missing = prepared.row_groups.clone();
        missing[0].source_versions = None;
        assert!(matches!(
            super::encode_segment(
                identity,
                &prepared.metadata.columns,
                &missing,
                super::INCREMENTAL_FORMAT_VERSION
            ),
            Err(ColumnarError::InvalidInput(
                "incremental row group is missing source identities"
            ))
        ));
        let mut short = prepared.row_groups.clone();
        short[0].source_versions.as_mut().expect("identities").pop();
        assert!(matches!(
            super::encode_segment(
                identity,
                &prepared.metadata.columns,
                &short,
                super::INCREMENTAL_FORMAT_VERSION
            ),
            Err(ColumnarError::InvalidInput(
                "source identity count differs from row count"
            ))
        ));

        let mut wrong_storage = segment.clone();
        replace_u64(&mut wrong_storage, 40, 999);
        assert!(matches!(
            super::decode_segment(&wrong_storage, &prepared.metadata),
            Err(ColumnarError::IdentityMismatch("segment source storage"))
        ));
        let mut wrong_engine = segment.clone();
        wrong_engine[92] = 2;
        rewrite_checksum(&mut wrong_engine);
        assert!(matches!(
            super::decode_segment(&wrong_engine, &prepared.metadata),
            Err(ColumnarError::IdentityMismatch("version engine kind"))
        ));
        let mut zero_generation = segment.clone();
        zero_generation[110..114].copy_from_slice(&0_u32.to_le_bytes());
        rewrite_checksum(&mut zero_generation);
        assert!(matches!(
            super::decode_segment(&zero_generation, &prepared.metadata),
            Err(ColumnarError::Corrupt("invalid Heap version identity"))
        ));
        let mut checksum = segment;
        checksum[32] ^= 1;
        assert!(matches!(
            super::decode_segment(&checksum, &prepared.metadata),
            Err(ColumnarError::Corrupt("file checksum mismatch"))
        ));
        drop(prepared);
        fs::remove_dir_all(&heap_directory).expect("remove Heap NBCS fixture");

        let lsm_directory = test_directory("nbcs-v2-lsm-boundaries");
        let lsm_storage = StorageId(32);
        let lsm_rows = [
            (lsm_key(lsm_storage, 1, 9), rows()[0].clone()),
            (lsm_key(lsm_storage, 2, 10), rows()[1].clone()),
        ];
        let lsm = ColumnarProjection::prepare_incremental(
            &lsm_directory,
            ColumnarProjectionId(32),
            ColumnarGeneration(1),
            &table,
            lsm_storage,
            StorageSnapshotToken::lsm(lsm_storage, 5, 10),
            crate::ChangeStreamCursor {
                storage_id: lsm_storage,
                generation: ChangeStreamGeneration(2),
                frontier: StorageDataVersion(8),
            },
            &[ColumnId(1), ColumnId(4)],
            &lsm_rows
                .iter()
                .map(|(key, values)| (*key, vec![values[0].clone(), values[3].clone()]))
                .collect::<Vec<_>>(),
            Some(1),
        )
        .and_then(|prepared| prepared.publish())
        .expect("publish LSM NBCS v2");
        drop(lsm);
        assert!(
            ColumnarProjection::open(&lsm_directory, &table)
                .expect("open LSM NBCS v2")
                .is_incremental()
        );
        fs::remove_dir_all(&lsm_directory).expect("remove LSM NBCS fixture");

        for (name, key, token) in [
            (
                "wrong-kind",
                lsm_key(heap_storage, 1, 1),
                StorageSnapshotToken::heap(heap_storage, 1),
            ),
            (
                "zero-heap-generation",
                StorageVersionKey::Heap {
                    storage_id: heap_storage,
                    row_id: RowId {
                        page: PageId(1),
                        slot: 0,
                        generation: 0,
                    },
                },
                StorageSnapshotToken::heap(heap_storage, 1),
            ),
            (
                "zero-lsm-sequence",
                lsm_key(heap_storage, 1, 0),
                StorageSnapshotToken::lsm(heap_storage, 1, 1),
            ),
        ] {
            let directory = test_directory(name);
            assert!(
                ColumnarProjection::prepare_incremental(
                    &directory,
                    ColumnarProjectionId(40),
                    ColumnarGeneration(1),
                    &table,
                    heap_storage,
                    token,
                    crate::ChangeStreamCursor {
                        storage_id: heap_storage,
                        generation: ChangeStreamGeneration(1),
                        frontier: StorageDataVersion(1),
                    },
                    &[ColumnId(1)],
                    &[(key, vec![ScalarValue::Int64(1)])],
                    None,
                )
                .is_err()
            );
            let _ = fs::remove_dir_all(directory);
        }

        let empty_directory = test_directory("nbcs-v2-empty");
        let empty = ColumnarProjection::prepare_incremental(
            &empty_directory,
            ColumnarProjectionId(41),
            ColumnarGeneration(1),
            &table,
            heap_storage,
            StorageSnapshotToken::heap(heap_storage, 1),
            crate::ChangeStreamCursor {
                storage_id: heap_storage,
                generation: ChangeStreamGeneration(1),
                frontier: StorageDataVersion(1),
            },
            &[ColumnId(1)],
            &[],
            None,
        )
        .and_then(|prepared| prepared.publish())
        .expect("publish empty NBCS v2");
        assert!(
            empty
                .scan(&[ColumnId(1)], &[])
                .expect("scan empty")
                .0
                .is_empty()
        );
        drop(empty);
        fs::remove_dir_all(empty_directory).expect("remove empty NBCS fixture");
    }

    #[test]
    fn nbcd_v1_decoder_rejects_malformed_identity_counts_frontiers_and_references() {
        let directory = test_directory("nbcd-v1-boundaries");
        let table = table();
        let storage_id = StorageId(51);
        let base_keys = [heap_key(storage_id, 1, 0), heap_key(storage_id, 1, 1)];
        let base_rows = vec![
            (
                base_keys[0],
                vec![ScalarValue::Int64(10), ScalarValue::Null],
            ),
            (
                base_keys[1],
                vec![ScalarValue::Int64(20), ScalarValue::Text("base".into())],
            ),
        ];
        let base = ColumnarProjection::prepare_incremental(
            &directory,
            ColumnarProjectionId(51),
            ColumnarGeneration(2),
            &table,
            storage_id,
            StorageSnapshotToken::heap(storage_id, 20),
            crate::ChangeStreamCursor {
                storage_id,
                generation: ChangeStreamGeneration(7),
                frontier: StorageDataVersion(20),
            },
            &[ColumnId(1), ColumnId(4)],
            &base_rows,
            None,
        )
        .and_then(|prepared| prepared.publish())
        .expect("publish NBCD base");
        let fingerprint = table.fingerprint().expect("fingerprint");
        let updated = heap_key(storage_id, 2, 0);
        let inserted = heap_key(storage_id, 2, 1);
        let final_version = heap_key(storage_id, 3, 0);
        let batches = vec![
            ChangeBatch {
                sequence: 1,
                physical_txn_id: TxnId(1),
                database_txn_id: None,
                table_id: table.id,
                storage_id,
                schema_fingerprint: fingerprint,
                before: StorageDataVersion(20),
                after: StorageDataVersion(21),
                mutations: vec![
                    StorageChange::Update {
                        old_version: base_keys[0],
                        new_version: updated,
                        after: vec![
                            ScalarValue::Int64(11),
                            ScalarValue::UInt64(1),
                            ScalarValue::Null,
                            ScalarValue::Text("delta-text".into()),
                        ],
                    },
                    StorageChange::Insert {
                        new_version: inserted,
                        after: vec![
                            ScalarValue::Int64(30),
                            ScalarValue::UInt64(2),
                            ScalarValue::Bool(true),
                            ScalarValue::Null,
                        ],
                    },
                ],
            },
            ChangeBatch {
                sequence: 2,
                physical_txn_id: TxnId(2),
                database_txn_id: None,
                table_id: table.id,
                storage_id,
                schema_fingerprint: fingerprint,
                before: StorageDataVersion(21),
                after: StorageDataVersion(22),
                mutations: vec![
                    StorageChange::Delete {
                        old_version: updated,
                    },
                    StorageChange::Update {
                        old_version: base_keys[1],
                        new_version: final_version,
                        after: vec![
                            ScalarValue::Int64(21),
                            ScalarValue::UInt64(3),
                            ScalarValue::Bool(false),
                            ScalarValue::Text("final".into()),
                        ],
                    },
                ],
            },
        ];
        let prepared = base
            .prepare_advance(&table, &batches)
            .expect("prepare valid NBCD");
        let bytes = fs::read(&prepared.delta_tmp).expect("read NBCD");
        let metadata = prepared.projection.metadata().clone();
        let expected = metadata
            .incremental
            .as_ref()
            .expect("incremental")
            .delta_segments
            .last()
            .expect("delta metadata")
            .clone();
        assert_eq!(
            super::decode_delta(&bytes, &metadata, &expected)
                .expect("decode multiple batches")
                .len(),
            4
        );

        let assert_rejected = |candidate: &[u8], expected: &super::ColumnarDeltaSegmentMetadata| {
            assert!(
                super::decode_delta(candidate, &metadata, expected).is_err(),
                "malformed NBCD unexpectedly decoded"
            );
        };
        let mut malformed = bytes.clone();
        malformed[0] = b'X';
        rewrite_checksum(&mut malformed);
        assert_rejected(&malformed, &expected);
        let mut malformed = bytes.clone();
        replace_u16(&mut malformed, 4, 99);
        assert_rejected(&malformed, &expected);
        let mut malformed = bytes.clone();
        malformed[12] ^= 1;
        assert_rejected(&malformed, &expected);
        assert_rejected(&bytes[..bytes.len() - 9], &expected);
        let mut malformed = bytes[..bytes.len() - 4].to_vec();
        malformed.push(0xff);
        super::append_checksum(&mut malformed);
        assert_rejected(&malformed, &expected);

        for (offset, value) in [
            (8, 999_u64),
            (16, 999),
            (24, 999),
            (32, 999),
            (40, 999),
            (80, 999),
        ] {
            let mut malformed = bytes.clone();
            replace_u64(&mut malformed, offset, value);
            assert_rejected(&malformed, &expected);
        }
        let mut malformed = bytes.clone();
        replace_u64(&mut malformed, 136, 20);
        assert_rejected(&malformed, &expected);
        let mut malformed = bytes.clone();
        malformed[149] = 2;
        rewrite_checksum(&mut malformed);
        assert_rejected(&malformed, &expected);
        let mut malformed = bytes.clone();
        replace_u64(&mut malformed, 197, 3);
        assert_rejected(&malformed, &expected);

        let mut oversized_mutations = bytes.clone();
        replace_u64(
            &mut oversized_mutations,
            108,
            u64::from(crate::CHANGE_LOG_MAX_MUTATIONS) + 1,
        );
        let mut oversized_expected = expected.clone();
        oversized_expected.mutation_count = u64::from(crate::CHANGE_LOG_MAX_MUTATIONS) + 1;
        assert_rejected(&oversized_mutations, &oversized_expected);
        let mut oversized_rows = bytes.clone();
        replace_u64(&mut oversized_rows, 116, u64::MAX);
        let mut oversized_expected = expected.clone();
        oversized_expected.after_row_count = u64::MAX;
        assert_rejected(&oversized_rows, &oversized_expected);

        drop(prepared);
        let duplicate = ChangeBatch {
            sequence: 3,
            physical_txn_id: TxnId(3),
            database_txn_id: None,
            table_id: table.id,
            storage_id,
            schema_fingerprint: fingerprint,
            before: StorageDataVersion(20),
            after: StorageDataVersion(21),
            mutations: vec![
                StorageChange::Delete {
                    old_version: base_keys[0],
                },
                StorageChange::Delete {
                    old_version: base_keys[0],
                },
            ],
        };
        assert!(matches!(
            base.prepare_advance(&table, &[duplicate]),
            Err(ColumnarError::Corrupt(
                "one version has multiple durable delta transitions"
            ))
        ));
        drop(base);
        fs::remove_dir_all(directory).expect("remove NBCD fixture");

        let lsm_directory = test_directory("nbcd-v1-lsm-projected-only");
        let lsm_storage = StorageId(52);
        let old = lsm_key(lsm_storage, 8, 10);
        let lsm = ColumnarProjection::prepare_incremental(
            &lsm_directory,
            ColumnarProjectionId(52),
            ColumnarGeneration(1),
            &table,
            lsm_storage,
            StorageSnapshotToken::lsm(lsm_storage, 2, 10),
            crate::ChangeStreamCursor {
                storage_id: lsm_storage,
                generation: ChangeStreamGeneration(3),
                frontier: StorageDataVersion(5),
            },
            &[ColumnId(1)],
            &[(old, vec![ScalarValue::Int64(1)])],
            None,
        )
        .and_then(|prepared| prepared.publish())
        .expect("publish LSM base");
        let sentinel = "unprojected-text-must-not-be-persisted";
        let lsm_batch = ChangeBatch {
            sequence: 1,
            physical_txn_id: TxnId(4),
            database_txn_id: None,
            table_id: table.id,
            storage_id: lsm_storage,
            schema_fingerprint: fingerprint,
            before: StorageDataVersion(5),
            after: StorageDataVersion(6),
            mutations: vec![StorageChange::Update {
                old_version: old,
                new_version: lsm_key(lsm_storage, 8, 11),
                after: vec![
                    ScalarValue::Int64(2),
                    ScalarValue::UInt64(9),
                    ScalarValue::Null,
                    ScalarValue::Text(sentinel.into()),
                ],
            }],
        };
        let prepared = lsm
            .prepare_advance(&table, &[lsm_batch])
            .expect("prepare LSM NBCD");
        let bytes = fs::read(&prepared.delta_tmp).expect("read LSM NBCD");
        assert!(
            !bytes
                .windows(sentinel.len())
                .any(|window| window == sentinel.as_bytes()),
            "NBCD must persist only projected after-image columns"
        );
        let advanced = prepared.publish().expect("publish LSM NBCD");
        drop(advanced);
        let values = ColumnarProjection::open(&lsm_directory, &table)
            .expect("open LSM NBCD")
            .scan(&[ColumnId(1)], &[])
            .expect("scan LSM merge")
            .0
            .iter()
            .flat_map(|batch| {
                (0..batch.row_count)
                    .map(|row| batch.columns[0].values.value(row).expect("LSM delta value"))
            })
            .collect::<Vec<_>>();
        assert_eq!(values, vec![ScalarValue::Int64(2)]);
        fs::remove_dir_all(lsm_directory).expect("remove LSM NBCD fixture");
    }

    #[test]
    fn delta_publication_reopens_at_only_old_or_new_frontier() {
        let directory = test_directory("incremental-publication");
        let table = table();
        let storage_id = StorageId(3);
        let base_rows = vec![(heap_key(storage_id, 1, 0), rows()[0].clone())];
        let base = ColumnarProjection::prepare_incremental(
            &directory,
            ColumnarProjectionId(22),
            ColumnarGeneration(1),
            &table,
            storage_id,
            StorageSnapshotToken::heap(storage_id, 1),
            crate::ChangeStreamCursor {
                storage_id,
                generation: ChangeStreamGeneration(1),
                frontier: StorageDataVersion(4),
            },
            &[ColumnId(1), ColumnId(2), ColumnId(3), ColumnId(4)],
            &base_rows,
            None,
        )
        .and_then(|prepared| prepared.publish())
        .expect("publish base");
        let batch = ChangeBatch {
            sequence: 1,
            physical_txn_id: TxnId(1),
            database_txn_id: None,
            table_id: table.id,
            storage_id,
            schema_fingerprint: table.fingerprint().expect("fingerprint"),
            before: StorageDataVersion(4),
            after: StorageDataVersion(5),
            mutations: vec![StorageChange::Delete {
                old_version: base_rows[0].0,
            }],
        };

        let prepared = base
            .prepare_advance(&table, std::slice::from_ref(&batch))
            .expect("prepare delta");
        fs::rename(&prepared.delta_tmp, &prepared.delta_final).expect("install orphan delta");
        super::sync_directory(&directory).expect("sync orphan delta");
        drop(prepared);
        let old = ColumnarProjection::open(&directory, &table).expect("open old authority");
        assert_eq!(
            old.metadata()
                .incremental
                .as_ref()
                .expect("incremental")
                .applied_frontier,
            StorageDataVersion(4)
        );

        let prepared = old
            .prepare_advance(&table, &[batch])
            .expect("prepare retry");
        fs::rename(&prepared.delta_tmp, &prepared.delta_final).expect("install delta");
        super::sync_directory(&directory).expect("sync delta");
        fs::rename(&prepared.manifest_tmp, directory.join(super::MANIFEST_FILE))
            .expect("install manifest");
        super::sync_directory(&directory).expect("sync manifest");
        drop(prepared);
        let new = ColumnarProjection::open(&directory, &table).expect("open new authority");
        assert_eq!(
            new.metadata()
                .incremental
                .as_ref()
                .expect("incremental")
                .applied_frontier,
            StorageDataVersion(5)
        );
        assert_eq!(
            new.scan(&[ColumnId(1)], &[]).expect("scan").0[0].row_count,
            0
        );
        assert_eq!(
            base.scan(&[ColumnId(1)], &[]).expect("old reader scan").0[0].row_count,
            1,
            "an active immutable reader retains its pre-advance overlay"
        );
        fs::remove_dir_all(directory).expect("remove fixture");
    }

    fn crash_delta_batch(table: &TableDef, storage_id: StorageId) -> ChangeBatch {
        ChangeBatch {
            sequence: 1,
            physical_txn_id: TxnId(1),
            database_txn_id: None,
            table_id: table.id,
            storage_id,
            schema_fingerprint: table.fingerprint().expect("fingerprint"),
            before: StorageDataVersion(4),
            after: StorageDataVersion(5),
            mutations: vec![StorageChange::Update {
                old_version: heap_key(storage_id, 1, 0),
                new_version: heap_key(storage_id, 2, 0),
                after: vec![
                    ScalarValue::Int64(2),
                    ScalarValue::UInt64(2),
                    ScalarValue::Null,
                    ScalarValue::Text("new".into()),
                ],
            }],
        }
    }

    fn seed_delta_crash_projection(directory: &PathBuf) {
        let table = table();
        let storage_id = StorageId(61);
        let projection = ColumnarProjection::prepare_incremental(
            directory,
            ColumnarProjectionId(61),
            ColumnarGeneration(1),
            &table,
            storage_id,
            StorageSnapshotToken::heap(storage_id, 4),
            crate::ChangeStreamCursor {
                storage_id,
                generation: ChangeStreamGeneration(1),
                frontier: StorageDataVersion(4),
            },
            &[ColumnId(1), ColumnId(2), ColumnId(3), ColumnId(4)],
            &[(heap_key(storage_id, 1, 0), rows()[0].clone())],
            None,
        )
        .and_then(|prepared| prepared.publish())
        .expect("seed delta crash projection");
        drop(projection);
    }

    #[test]
    fn delta_publication_crash_child() {
        if std::env::var("NETBADB_COLUMNAR_DELTA_CRASH_CHILD").as_deref() != Ok("1") {
            return;
        }
        let directory = PathBuf::from(
            std::env::var("NETBADB_COLUMNAR_DELTA_CRASH_DIRECTORY").expect("crash directory"),
        );
        let table = table();
        let storage_id = StorageId(61);
        let projection = ColumnarProjection::open(&directory, &table).expect("open crash base");
        projection
            .prepare_advance(&table, &[crash_delta_batch(&table, storage_id)])
            .and_then(|prepared| prepared.publish())
            .expect("advance must reach configured crash point");
        panic!("configured delta crash point did not terminate the child");
    }

    #[test]
    fn delta_publication_crash_matrix_reopens_at_old_or_new_authority() {
        let points = [
            "delta-temp-created",
            "delta-written",
            "delta-synced",
            "delta-manifest-temp-created",
            "delta-manifest-written",
            "delta-manifest-synced",
            "delta-renamed",
            "delta-directory-synced",
            "delta-manifest-renamed",
        ];
        for point in points {
            let directory = test_directory(point);
            seed_delta_crash_projection(&directory);
            let status = Command::new(std::env::current_exe().expect("test executable"))
                .arg("columnar::tests::delta_publication_crash_child")
                .arg("--exact")
                .arg("--nocapture")
                .env("NETBADB_COLUMNAR_DELTA_CRASH_CHILD", "1")
                .env("NETBADB_COLUMNAR_DELTA_CRASH_DIRECTORY", &directory)
                .env("NETBADB_COLUMNAR_DELTA_CRASH_POINT", point)
                .status()
                .expect("run delta crash child");
            assert_eq!(status.code(), Some(89), "crash point {point}");
            let reopened = ColumnarProjection::open(&directory, &table())
                .unwrap_or_else(|error| panic!("reopen after {point}: {error}"));
            let frontier = reopened
                .metadata()
                .incremental
                .as_ref()
                .expect("incremental")
                .applied_frontier;
            let expected = if point == "delta-manifest-renamed" {
                StorageDataVersion(5)
            } else {
                StorageDataVersion(4)
            };
            assert_eq!(frontier, expected, "crash point {point}");
            fs::remove_dir_all(directory).expect("remove delta crash fixture");
        }
    }

    #[test]
    fn round_trip_reopen_preserves_typed_vectors_nulls_and_row_groups() {
        let directory = test_directory("round-trip");
        let table = table();
        let projection = build_projection!(
            &directory,
            ColumnarProjectionId(11),
            ColumnarGeneration(1),
            &table,
            StorageId(3),
            StorageSnapshotToken::heap(StorageId(3), 19),
            &[ColumnId(1), ColumnId(2), ColumnId(3), ColumnId(4)],
            &rows(),
            Some(2),
        )
        .expect("build projection");
        assert_eq!(projection.metadata().row_group_count, 2);
        drop(projection);

        let reopened = ColumnarProjection::open(&directory, &table).expect("reopen projection");
        let (batches, stats) = reopened
            .scan(&[ColumnId(1), ColumnId(2), ColumnId(3), ColumnId(4)], &[])
            .expect("scan projection");
        let decoded = batches
            .iter()
            .flat_map(|batch| {
                (0..batch.row_count).map(|row| {
                    batch
                        .columns
                        .iter()
                        .map(|column| column.values.value(row).expect("decode value"))
                        .collect::<Vec<_>>()
                })
            })
            .collect::<Vec<_>>();
        assert_eq!(decoded, rows());
        assert_eq!(stats.row_groups_read, 2);
        assert_eq!(stats.rows_read, 3);
        fs::remove_dir_all(directory).expect("remove test directory");
    }

    #[test]
    fn zone_map_prunes_only_impossible_row_groups() {
        let directory = test_directory("zone-map");
        let table = table();
        let projection = build_projection!(
            &directory,
            ColumnarProjectionId(12),
            ColumnarGeneration(1),
            &table,
            StorageId(4),
            StorageSnapshotToken::lsm(StorageId(4), 1, 8),
            &[ColumnId(1), ColumnId(2), ColumnId(3), ColumnId(4)],
            &rows(),
            Some(1),
        )
        .expect("build projection");
        let (batches, stats) = projection
            .scan(
                &[ColumnId(1)],
                &[ColumnarConstraint {
                    column_id: ColumnId(1),
                    lower: Some((ScalarValue::Int64(4), true)),
                    upper: Some((ScalarValue::Int64(4), true)),
                }],
            )
            .expect("scan projection");
        assert_eq!(stats.row_groups_total, 3);
        assert_eq!(stats.row_groups_pruned, 2);
        assert_eq!(batches.len(), 1);
        assert_eq!(
            batches[0].columns[0].values.value(0).expect("value"),
            ScalarValue::Int64(4)
        );
        drop(projection);
        fs::remove_dir_all(directory).expect("remove test directory");
    }

    #[test]
    fn active_reader_keeps_immutable_generation_after_refresh_retires_its_file() {
        let directory = test_directory("active-reader");
        let table = table();
        let generation_one = build_projection!(
            &directory,
            ColumnarProjectionId(17),
            ColumnarGeneration(1),
            &table,
            StorageId(9),
            StorageSnapshotToken::heap(StorageId(9), 1),
            &[ColumnId(1)],
            &[vec![ScalarValue::Int64(10)]],
            None,
        )
        .expect("build generation one");
        let active_reader = generation_one.clone();
        let generation_two = build_projection!(
            &directory,
            ColumnarProjectionId(17),
            ColumnarGeneration(2),
            &table,
            StorageId(9),
            StorageSnapshotToken::heap(StorageId(9), 2),
            &[ColumnId(1)],
            &[vec![ScalarValue::Int64(20)]],
            None,
        )
        .expect("publish generation two");
        generation_one
            .retire_segment()
            .expect("retire generation one file");

        let (old_batches, _) = active_reader
            .scan(&[ColumnId(1)], &[])
            .expect("active reader scans retained generation");
        assert_eq!(
            old_batches[0].columns[0]
                .values
                .value(0)
                .expect("old value"),
            ScalarValue::Int64(10)
        );
        let (new_batches, _) = generation_two
            .scan(&[ColumnId(1)], &[])
            .expect("new reader scans new generation");
        assert_eq!(
            new_batches[0].columns[0]
                .values
                .value(0)
                .expect("new value"),
            ScalarValue::Int64(20)
        );
        drop(active_reader);
        drop(generation_two);
        fs::remove_dir_all(directory).expect("remove test directory");
    }

    #[test]
    fn reopen_rejects_truncated_manifest_and_corrupt_segment() {
        let table = table();
        let truncated = test_directory("truncated");
        let projection = build_projection!(
            &truncated,
            ColumnarProjectionId(13),
            ColumnarGeneration(1),
            &table,
            StorageId(5),
            StorageSnapshotToken::heap(StorageId(5), 1),
            &[ColumnId(1)],
            &[vec![ScalarValue::Int64(1)]],
            None,
        )
        .expect("build projection");
        drop(projection);
        fs::write(truncated.join("projection.nbcmanifest"), b"NBCM").expect("truncate manifest");
        assert!(matches!(
            ColumnarProjection::open(&truncated, &table),
            Err(ColumnarError::InvalidFormat(_)) | Err(ColumnarError::Corrupt(_))
        ));
        fs::remove_dir_all(&truncated).expect("remove truncated directory");

        let corrupt = test_directory("corrupt");
        let projection = build_projection!(
            &corrupt,
            ColumnarProjectionId(14),
            ColumnarGeneration(1),
            &table,
            StorageId(6),
            StorageSnapshotToken::heap(StorageId(6), 2),
            &[ColumnId(1)],
            &[vec![ScalarValue::Int64(2)]],
            None,
        )
        .expect("build projection");
        let segment = corrupt.join("projection-14-g1.nbcs");
        let mut bytes = fs::read(&segment).expect("read segment");
        bytes[16] ^= 0x40;
        fs::write(&segment, bytes).expect("corrupt segment");
        drop(projection);
        assert!(matches!(
            ColumnarProjection::open(&corrupt, &table),
            Err(ColumnarError::ChecksumMismatch { .. })
                | Err(ColumnarError::Corrupt("file checksum mismatch"))
        ));
        fs::remove_dir_all(corrupt).expect("remove corrupt directory");
    }

    #[test]
    fn reopen_rejects_schema_and_source_identity_mismatch() {
        let directory = test_directory("identity");
        let table = table();
        let projection = build_projection!(
            &directory,
            ColumnarProjectionId(15),
            ColumnarGeneration(1),
            &table,
            StorageId(7),
            StorageSnapshotToken::heap(StorageId(7), 3),
            &[ColumnId(1)],
            &[vec![ScalarValue::Int64(3)]],
            None,
        )
        .expect("build projection");
        drop(projection);
        let mut wrong = table.clone();
        wrong.name = "other".into();
        assert!(matches!(
            ColumnarProjection::open(&directory, &wrong),
            Err(ColumnarError::IdentityMismatch("schema fingerprint"))
        ));
        let mut wrong_table = table.clone();
        wrong_table.id = TableId(99);
        assert!(matches!(
            ColumnarProjection::open(&directory, &wrong_table),
            Err(ColumnarError::IdentityMismatch("table"))
        ));
        fs::remove_dir_all(directory).expect("remove test directory");
    }

    #[test]
    fn empty_projection_and_manifest_decoder_boundaries_are_explicit() {
        let directory = test_directory("decoder-boundaries");
        let table = table();
        let projection = build_projection!(
            &directory,
            ColumnarProjectionId(16),
            ColumnarGeneration(1),
            &table,
            StorageId(8),
            StorageSnapshotToken::heap(StorageId(8), 0),
            &[ColumnId(1), ColumnId(2), ColumnId(3), ColumnId(4)],
            &[],
            None,
        )
        .expect("build empty projection");
        assert!(
            projection
                .scan(&[ColumnId(1)], &[])
                .expect("scan empty projection")
                .0
                .is_empty()
        );
        drop(projection);

        let manifest_path = directory.join("projection.nbcmanifest");
        let original = fs::read(&manifest_path).expect("read manifest");

        let mut bad_magic = original.clone();
        bad_magic[0] = b'X';
        rewrite_checksum(&mut bad_magic);
        fs::write(&manifest_path, bad_magic).expect("write bad magic");
        assert!(matches!(
            ColumnarProjection::open(&directory, &table),
            Err(ColumnarError::InvalidFormat("invalid magic"))
        ));

        let mut unsupported = original.clone();
        unsupported[4..6].copy_from_slice(&99_u16.to_le_bytes());
        rewrite_checksum(&mut unsupported);
        fs::write(&manifest_path, unsupported).expect("write unsupported version");
        assert!(matches!(
            ColumnarProjection::open(&directory, &table),
            Err(ColumnarError::UnsupportedVersion(99))
        ));

        let mut oversized = original.clone();
        oversized[128..132].copy_from_slice(&(super::MAX_COLUMNS + 1).to_le_bytes());
        rewrite_checksum(&mut oversized);
        fs::write(&manifest_path, oversized).expect("write oversized column count");
        assert!(matches!(
            ColumnarProjection::open(&directory, &table),
            Err(ColumnarError::ResourceLimit {
                resource: "manifest columns",
                ..
            })
        ));

        fs::write(&manifest_path, original).expect("restore manifest");
        fs::remove_file(directory.join("projection-16-g1.nbcs")).expect("remove segment");
        assert!(matches!(
            ColumnarProjection::open(&directory, &table),
            Err(ColumnarError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound
        ));

        let invalid_text =
            super::decode_vector_data(PhysicalType::Text, 1, vec![1], &[1, 0, 0, 0, 1, 0, 0, 0]);
        assert!(matches!(
            invalid_text,
            Err(ColumnarError::Corrupt("invalid text offsets"))
        ));
        fs::remove_dir_all(directory).expect("remove test directory");
    }

    #[test]
    fn crash_after_segment_install_keeps_old_manifest_and_cleans_manifest_temp() {
        let directory = test_directory("publish-boundary");
        let table = table();
        let published = build_projection!(
            &directory,
            ColumnarProjectionId(17),
            ColumnarGeneration(1),
            &table,
            StorageId(9),
            StorageSnapshotToken::heap(StorageId(9), 1),
            &[ColumnId(1)],
            &[vec![ScalarValue::Int64(1)]],
            Some(1),
        )
        .expect("publish first generation");
        drop(published);

        let prepared = ColumnarProjection::prepare(
            &directory,
            ColumnarProjectionId(17),
            ColumnarGeneration(2),
            &table,
            StorageId(9),
            StorageSnapshotToken::heap(StorageId(9), 2),
            &[ColumnId(1)],
            &[vec![ScalarValue::Int64(2)]],
            Some(1),
        )
        .expect("prepare second generation");
        let orphan_segment = prepared.root.join(&prepared.segment_file);
        fs::rename(&prepared.segment_tmp, &orphan_segment)
            .expect("simulate crash boundary after segment rename");
        drop(prepared);

        let reopened = ColumnarProjection::open(&directory, &table)
            .expect("old manifest remains publishable after abandoned prepare");
        assert_eq!(reopened.metadata().generation, ColumnarGeneration(1));
        assert_eq!(
            reopened
                .scan(&[ColumnId(1)], &[])
                .expect("scan old generation")
                .0[0]
                .columns[0]
                .values
                .value(0)
                .expect("old value"),
            ScalarValue::Int64(1)
        );
        assert!(
            fs::read_dir(&directory)
                .expect("read projection directory")
                .all(|entry| !entry
                    .expect("directory entry")
                    .file_name()
                    .to_string_lossy()
                    .contains(".tmp."))
        );
        assert!(
            orphan_segment.exists(),
            "unreferenced immutable segment may remain as harmless derived-state debris"
        );
        drop(reopened);
        fs::remove_dir_all(directory).expect("remove test directory");
    }
}
