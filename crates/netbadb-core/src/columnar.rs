use std::path::{Path, PathBuf};

use netbadb_schema::SchemaFingerprint;
use netbadb_storage::{ColumnarProjection, StorageSnapshotToken};
use netbadb_types::{ColumnId, ColumnarGeneration, ColumnarProjectionId, StorageId, TableId};

use crate::projection_catalog::{
    ProjectionCatalog, ProjectionCatalogEntry, ProjectionCatalogError,
};

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
pub struct ColumnarProjectionCatalogInspection {
    pub managed: bool,
    pub available: bool,
    pub path: Option<PathBuf>,
    pub next_projection_id: Option<ColumnarProjectionId>,
    pub detail: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnarProjectionInspection {
    pub managed: bool,
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
    pub(crate) identity: ProjectionCatalogEntry,
    pub(crate) directory: PathBuf,
    pub(crate) projection: Option<ColumnarProjection>,
    pub(crate) detail: Option<String>,
    pub(crate) managed: bool,
}

#[derive(Debug)]
pub(crate) struct ProjectionRegistry {
    entries: Vec<ProjectionRegistryEntry>,
    next_id: u64,
    catalog: Option<ProjectionCatalog>,
    catalog_path: Option<PathBuf>,
    catalog_error: Option<String>,
}

impl ProjectionRegistry {
    pub(crate) fn new() -> Self {
        Self {
            entries: Vec::new(),
            next_id: 1,
            catalog: None,
            catalog_path: None,
            catalog_error: None,
        }
    }

    pub(crate) fn managed(
        catalog: ProjectionCatalog,
        entries: Vec<ProjectionRegistryEntry>,
    ) -> Self {
        Self {
            next_id: catalog.next_id().map_or(0, |id| id.0),
            catalog_path: Some(catalog.path().to_owned()),
            catalog: Some(catalog),
            catalog_error: None,
            entries,
        }
    }

    pub(crate) fn degraded(path: PathBuf, detail: String) -> Self {
        Self {
            entries: Vec::new(),
            next_id: 0,
            catalog: None,
            catalog_path: Some(path),
            catalog_error: Some(detail),
        }
    }

    pub(crate) fn reserve_id(&mut self) -> Result<ColumnarProjectionId, ProjectionCatalogError> {
        self.ensure_catalog_available()?;
        if let Some(catalog) = &mut self.catalog {
            let id = catalog.reserve_id()?;
            self.next_id = catalog.next_id().map_or(0, |next| next.0);
            return Ok(id);
        }
        if self.next_id == 0 {
            return Err(ProjectionCatalogError::CapacityExceeded("identity space"));
        }
        let id = ColumnarProjectionId(self.next_id);
        self.next_id = self
            .next_id
            .checked_add(1)
            .ok_or(ProjectionCatalogError::CapacityExceeded("identity space"))?;
        Ok(id)
    }

    pub(crate) fn preflight_location(
        &self,
        directory: &Path,
    ) -> Result<(), ProjectionCatalogError> {
        self.ensure_catalog_available()?;
        if let Some(catalog) = &self.catalog {
            let locator = catalog.locator(directory)?;
            if catalog.contains_locator(&locator) {
                return Err(ProjectionCatalogError::Corrupt(
                    "projection location is already registered",
                ));
            }
        } else if self.entries.iter().any(|entry| {
            entry
                .projection
                .as_ref()
                .is_some_and(|projection| projection.root() == directory)
        }) {
            return Err(ProjectionCatalogError::Corrupt(
                "projection location is already registered",
            ));
        }
        Ok(())
    }

    pub(crate) fn publish(
        &mut self,
        projection: ColumnarProjection,
    ) -> Result<(), ProjectionCatalogError> {
        self.ensure_catalog_available()?;
        let metadata = projection.metadata();
        let managed = self.catalog.is_some();
        let locator = match &self.catalog {
            Some(catalog) => catalog.locator(projection.root())?,
            None => projection.root().to_string_lossy().into_owned(),
        };
        let identity = ProjectionCatalogEntry {
            id: metadata.id,
            table_id: metadata.table_id,
            source_storage_id: metadata.source_storage_id,
            generation: metadata.generation,
            schema_fingerprint: metadata.schema_fingerprint,
            locator,
        };
        if let Some(catalog) = &mut self.catalog {
            catalog.insert(identity.clone())?;
            self.next_id = catalog.next_id().map_or(0, |next| next.0);
        } else {
            self.next_id = identity.id.0.checked_add(1).unwrap_or(0);
        }
        crash("before-registry-publish");
        self.entries.push(ProjectionRegistryEntry {
            identity,
            directory: projection.root().to_owned(),
            projection: Some(projection),
            detail: None,
            managed,
        });
        self.entries.sort_by_key(|entry| entry.identity.id.0);
        Ok(())
    }

    pub(crate) fn adopt(
        &mut self,
        projection: ColumnarProjection,
    ) -> Result<(), ProjectionCatalogError> {
        self.ensure_catalog_available()?;
        let metadata = projection.metadata().clone();
        let locator = match &self.catalog {
            Some(catalog) => catalog.locator(projection.root())?,
            None => projection.root().to_string_lossy().into_owned(),
        };
        if let Some(existing) = self
            .entries
            .iter_mut()
            .find(|entry| entry.identity.id == metadata.id)
        {
            if existing.identity.locator != locator
                || existing.identity.table_id != metadata.table_id
                || existing.identity.source_storage_id != metadata.source_storage_id
                || existing.identity.schema_fingerprint != metadata.schema_fingerprint
            {
                return Err(ProjectionCatalogError::Corrupt(
                    "projection identity belongs to another location, source, or schema",
                ));
            }
            if metadata.generation.0 < existing.identity.generation.0 {
                return Err(ProjectionCatalogError::Corrupt(
                    "projection manifest generation moved backwards",
                ));
            }
            if let Some(catalog) = &mut self.catalog {
                catalog.update_generation(metadata.id, metadata.generation)?;
            }
            existing.identity.generation = metadata.generation;
            existing.projection = Some(projection);
            existing.detail = None;
            return Ok(());
        }
        if self.next_id == 0 || metadata.id.0 < self.next_id {
            return Err(ProjectionCatalogError::Corrupt(
                "projection identity was previously reserved",
            ));
        }
        let managed = self.catalog.is_some();
        let identity = ProjectionCatalogEntry {
            id: metadata.id,
            table_id: metadata.table_id,
            source_storage_id: metadata.source_storage_id,
            generation: metadata.generation,
            schema_fingerprint: metadata.schema_fingerprint,
            locator,
        };
        if let Some(catalog) = &mut self.catalog {
            catalog.insert(identity.clone())?;
            self.next_id = catalog.next_id().map_or(0, |next| next.0);
        } else {
            self.next_id = metadata.id.0.checked_add(1).unwrap_or(0);
        }
        self.entries.push(ProjectionRegistryEntry {
            identity,
            directory: projection.root().to_owned(),
            projection: Some(projection),
            detail: None,
            managed,
        });
        self.entries.sort_by_key(|entry| entry.identity.id.0);
        Ok(())
    }

    pub(crate) fn replace(
        &mut self,
        id: ColumnarProjectionId,
        replacement: ColumnarProjection,
    ) -> Result<ColumnarProjection, ProjectionCatalogError> {
        self.ensure_catalog_available()?;
        let position = self
            .entries
            .iter()
            .position(|entry| entry.identity.id == id)
            .ok_or(ProjectionCatalogError::Corrupt(
                "projection identity absent",
            ))?;
        let metadata = replacement.metadata().clone();
        if metadata.id != id
            || metadata.table_id != self.entries[position].identity.table_id
            || metadata.source_storage_id != self.entries[position].identity.source_storage_id
            || metadata.schema_fingerprint != self.entries[position].identity.schema_fingerprint
        {
            return Err(ProjectionCatalogError::Corrupt(
                "replacement projection identity changed",
            ));
        }
        if let Some(catalog) = &mut self.catalog {
            catalog.update_generation(id, metadata.generation)?;
        }
        let old = self.entries[position]
            .projection
            .replace(replacement)
            .ok_or(ProjectionCatalogError::Corrupt(
                "unavailable projection cannot refresh",
            ))?;
        self.entries[position].identity.generation = metadata.generation;
        self.entries[position].identity.schema_fingerprint = metadata.schema_fingerprint;
        self.entries[position].detail = None;
        crash("refresh-catalog-updated");
        Ok(old)
    }

    pub(crate) fn get(&self, id: ColumnarProjectionId) -> Option<&ColumnarProjection> {
        self.entries
            .iter()
            .find(|entry| entry.identity.id == id)
            .and_then(|entry| entry.projection.as_ref())
    }

    pub(crate) fn remove(
        &mut self,
        id: ColumnarProjectionId,
    ) -> Result<Option<ColumnarProjection>, ProjectionCatalogError> {
        self.ensure_catalog_available()?;
        let Some(position) = self
            .entries
            .iter()
            .position(|entry| entry.identity.id == id)
        else {
            return Ok(None);
        };
        if let Some(catalog) = &mut self.catalog {
            if !catalog.remove(id)? {
                return Err(ProjectionCatalogError::Corrupt(
                    "managed projection missing from catalog",
                ));
            }
        }
        let projection = self.entries.remove(position).projection;
        crash("drop-registry-removal");
        Ok(projection)
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &ProjectionRegistryEntry> {
        self.entries.iter()
    }

    pub(crate) fn catalog_inspection(&self) -> ColumnarProjectionCatalogInspection {
        ColumnarProjectionCatalogInspection {
            managed: self.catalog_path.is_some(),
            available: self.catalog_error.is_none(),
            path: self.catalog_path.clone(),
            next_projection_id: (self.next_id != 0).then_some(ColumnarProjectionId(self.next_id)),
            detail: self.catalog_error.clone(),
        }
    }

    fn ensure_catalog_available(&self) -> Result<(), ProjectionCatalogError> {
        match &self.catalog_error {
            Some(detail) => Err(ProjectionCatalogError::Unavailable(detail.clone())),
            None => Ok(()),
        }
    }
}

pub(crate) fn inspection(
    entry: &ProjectionRegistryEntry,
    current_token: Option<StorageSnapshotToken>,
    current_schema_fingerprint: Option<SchemaFingerprint>,
) -> ColumnarProjectionInspection {
    let Some(projection) = &entry.projection else {
        return unavailable_inspection(entry);
    };
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
        managed: entry.managed,
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
        detail: entry.detail.clone(),
        directory: projection.root().to_owned(),
    }
}

pub(crate) fn unavailable_inspection(
    entry: &ProjectionRegistryEntry,
) -> ColumnarProjectionInspection {
    ColumnarProjectionInspection {
        managed: entry.managed,
        projection_id: Some(entry.identity.id),
        generation: Some(entry.identity.generation),
        table_id: entry.identity.table_id,
        source_storage_id: Some(entry.identity.source_storage_id),
        source_snapshot: None,
        schema_fingerprint: Some(entry.identity.schema_fingerprint),
        schema_fingerprint_matches: None,
        columns: Vec::new(),
        row_count: None,
        row_group_count: None,
        segment_count: None,
        segment_bytes: None,
        health: ColumnarProjectionHealth::Unavailable,
        detail: entry.detail.clone(),
        directory: entry.directory.clone(),
    }
}

pub(crate) fn unmanaged_path_inspection(
    directory: &Path,
    table_id: TableId,
    detail: String,
) -> ColumnarProjectionInspection {
    ColumnarProjectionInspection {
        managed: false,
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

pub(crate) fn unmanaged_projection_inspection(
    projection: ColumnarProjection,
    current_schema_fingerprint: Option<SchemaFingerprint>,
) -> ColumnarProjectionInspection {
    let metadata = projection.metadata().clone();
    let entry = ProjectionRegistryEntry {
        identity: ProjectionCatalogEntry {
            id: metadata.id,
            table_id: metadata.table_id,
            source_storage_id: metadata.source_storage_id,
            generation: metadata.generation,
            schema_fingerprint: metadata.schema_fingerprint,
            locator: projection.root().to_string_lossy().into_owned(),
        },
        directory: projection.root().to_owned(),
        projection: Some(projection),
        detail: None,
        managed: false,
    };
    inspection(&entry, None, current_schema_fingerprint)
}

#[cfg(test)]
pub(crate) fn crash(point: &str) {
    if std::env::var("NETBADB_PROJECTION_CATALOG_CRASH_POINT").as_deref() == Ok(point) {
        std::process::exit(88);
    }
}

#[cfg(not(test))]
pub(crate) fn crash(_: &str) {}
