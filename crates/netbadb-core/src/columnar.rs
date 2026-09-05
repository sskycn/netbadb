use std::path::{Path, PathBuf};

use netbadb_schema::SchemaFingerprint;
use netbadb_storage::{ColumnarProjection, StorageSnapshotToken};
use netbadb_types::{ColumnId, ColumnarGeneration, ColumnarProjectionId, StorageId, TableId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnarProjectionSpec {
    pub table_id: TableId,
    pub directory: PathBuf,
    pub columns: Vec<ColumnId>,
    pub row_group_rows: Option<usize>,
}

impl ColumnarProjectionSpec {
    #[must_use]
    pub fn new(table_id: TableId, directory: impl Into<PathBuf>, columns: Vec<ColumnId>) -> Self {
        Self {
            table_id,
            directory: directory.into(),
            columns,
            row_group_rows: None,
        }
    }

    #[must_use]
    pub const fn with_row_group_rows(mut self, rows: usize) -> Self {
        self.row_group_rows = Some(rows);
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnarProjectionHealth {
    Fresh,
    Stale,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnarProjectionInspection {
    pub projection_id: Option<ColumnarProjectionId>,
    pub generation: Option<ColumnarGeneration>,
    pub table_id: TableId,
    pub source_storage_id: Option<StorageId>,
    pub source_snapshot: Option<String>,
    pub schema_fingerprint: Option<SchemaFingerprint>,
    pub schema_fingerprint_matches: Option<bool>,
    pub columns: Vec<ColumnId>,
    pub row_count: Option<u64>,
    pub row_group_count: Option<u64>,
    pub segment_count: Option<u64>,
    pub segment_bytes: Option<u64>,
    pub health: ColumnarProjectionHealth,
    pub detail: Option<String>,
    pub directory: PathBuf,
}

#[derive(Debug)]
pub(crate) struct ProjectionRegistryEntry {
    pub(crate) projection: ColumnarProjection,
}

#[derive(Debug, Default)]
pub(crate) struct ProjectionRegistry {
    entries: Vec<ProjectionRegistryEntry>,
    next_id: u64,
}

impl ProjectionRegistry {
    pub(crate) fn new() -> Self {
        Self {
            entries: Vec::new(),
            next_id: 1,
        }
    }

    pub(crate) fn allocate_id(&mut self) -> Option<ColumnarProjectionId> {
        let id = ColumnarProjectionId(self.next_id);
        self.next_id = self.next_id.checked_add(1)?;
        Some(id)
    }

    pub(crate) fn publish(&mut self, projection: ColumnarProjection) {
        let id = projection.metadata().id;
        self.next_id = self.next_id.max(id.0.saturating_add(1));
        if let Some(entry) = self
            .entries
            .iter_mut()
            .find(|entry| entry.projection.metadata().id == id)
        {
            entry.projection = projection;
        } else {
            self.entries.push(ProjectionRegistryEntry { projection });
        }
    }

    pub(crate) fn get(&self, id: ColumnarProjectionId) -> Option<&ColumnarProjection> {
        self.entries
            .iter()
            .find(|entry| entry.projection.metadata().id == id)
            .map(|entry| &entry.projection)
    }

    pub(crate) fn remove(&mut self, id: ColumnarProjectionId) -> Option<ColumnarProjection> {
        let position = self
            .entries
            .iter()
            .position(|entry| entry.projection.metadata().id == id)?;
        Some(self.entries.remove(position).projection)
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &ProjectionRegistryEntry> {
        self.entries.iter()
    }
}

pub(crate) fn inspection(
    projection: &ColumnarProjection,
    current_token: Option<StorageSnapshotToken>,
    current_schema_fingerprint: Option<SchemaFingerprint>,
) -> ColumnarProjectionInspection {
    let metadata = projection.metadata();
    let schema_fingerprint_matches =
        current_schema_fingerprint.map(|current| current == metadata.schema_fingerprint);
    let health = if current_token == Some(metadata.source_token)
        && schema_fingerprint_matches == Some(true)
    {
        ColumnarProjectionHealth::Fresh
    } else {
        ColumnarProjectionHealth::Stale
    };
    ColumnarProjectionInspection {
        projection_id: Some(metadata.id),
        generation: Some(metadata.generation),
        table_id: metadata.table_id,
        source_storage_id: Some(metadata.source_storage_id),
        source_snapshot: Some(metadata.source_token.diagnostic()),
        schema_fingerprint: Some(metadata.schema_fingerprint),
        schema_fingerprint_matches,
        columns: metadata
            .columns
            .iter()
            .map(|column| column.column_id)
            .collect(),
        row_count: Some(metadata.row_count),
        row_group_count: Some(metadata.row_group_count),
        segment_count: Some(metadata.segment_count),
        segment_bytes: Some(metadata.segment_bytes),
        health,
        detail: None,
        directory: projection.root().to_owned(),
    }
}

pub(crate) fn unavailable_inspection(
    directory: &Path,
    table_id: TableId,
    detail: String,
) -> ColumnarProjectionInspection {
    ColumnarProjectionInspection {
        projection_id: None,
        generation: None,
        table_id,
        source_storage_id: None,
        source_snapshot: None,
        schema_fingerprint: None,
        schema_fingerprint_matches: None,
        columns: Vec::new(),
        row_count: None,
        row_group_count: None,
        segment_count: None,
        segment_bytes: None,
        health: ColumnarProjectionHealth::Unavailable,
        detail: Some(detail),
        directory: directory.to_owned(),
    }
}
