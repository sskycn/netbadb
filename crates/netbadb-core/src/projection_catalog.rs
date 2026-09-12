//! Durable inventory and identity allocation for derived columnar projections.
//!
//! `NBPC` is deliberately separate from the authoritative schema catalog and
//! from the `NBCM` generation manifest. Losing or corrupting it disables only
//! managed projections; Heap and LSM recovery remains authoritative.

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use netbadb_schema::SchemaFingerprint;
use netbadb_types::{ColumnId, ColumnarGeneration, ColumnarProjectionId, StorageId, TableId};

use crate::schema_catalog_file as schema_file;

const CATALOG_MAGIC: &[u8; 4] = b"NBPC";
const MARKER_MAGIC: &[u8; 4] = b"NBPM";
const LEGACY_FORMAT_VERSION: u16 = 1;
const FORMAT_VERSION: u16 = 2;
const MAX_FILE_BYTES: u64 = 64 * 1024 * 1024;
const MAX_ENTRIES: u32 = 1 << 20;
const MAX_LOCATOR_BYTES: u32 = 4096;
const MAX_PENDING_COLUMNS: u32 = 1 << 20;

#[derive(Debug)]
pub enum ProjectionCatalogError {
    Io {
        operation: &'static str,
        path: PathBuf,
        source: std::io::Error,
    },
    InvalidFormat(&'static str),
    UnsupportedVersion(u16),
    ChecksumMismatch,
    CapacityExceeded(&'static str),
    Corrupt(&'static str),
    PendingBuildExists(ColumnarProjectionId),
    PendingBuildAbsent,
    PendingBuildMismatch(&'static str),
    PendingBuildCorrupt(String),
    RecoveryRequired {
        projection_id: ColumnarProjectionId,
        operation: &'static str,
        detail: String,
    },
    Unavailable(String),
}

impl fmt::Display for ProjectionCatalogError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io {
                operation,
                path,
                source,
            } => write!(formatter, "cannot {operation} {}: {source}", path.display()),
            Self::InvalidFormat(detail) => {
                write!(formatter, "invalid projection catalog format: {detail}")
            }
            Self::UnsupportedVersion(version) => {
                write!(
                    formatter,
                    "unsupported projection catalog version {version}"
                )
            }
            Self::ChecksumMismatch => formatter.write_str("projection catalog checksum mismatch"),
            Self::CapacityExceeded(resource) => {
                write!(formatter, "projection catalog {resource} exceeds its bound")
            }
            Self::Corrupt(detail) => write!(formatter, "corrupt projection catalog: {detail}"),
            Self::PendingBuildExists(id) => {
                write!(formatter, "projection build {} is already pending", id.0)
            }
            Self::PendingBuildAbsent => formatter.write_str("no projection build is pending"),
            Self::PendingBuildMismatch(detail) => {
                write!(formatter, "pending projection build mismatch: {detail}")
            }
            Self::PendingBuildCorrupt(detail) => {
                write!(
                    formatter,
                    "pending projection build artifact is corrupt: {detail}"
                )
            }
            Self::RecoveryRequired {
                projection_id,
                operation,
                detail,
            } => write!(
                formatter,
                "projection build {} requires reopen recovery after {operation}: {detail}",
                projection_id.0
            ),
            Self::Unavailable(detail) => {
                write!(formatter, "projection catalog is unavailable: {detail}")
            }
        }
    }
}

impl Error for ProjectionCatalogError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProjectionCatalogEntry {
    pub(crate) id: ColumnarProjectionId,
    pub(crate) table_id: TableId,
    pub(crate) source_storage_id: StorageId,
    pub(crate) generation: ColumnarGeneration,
    pub(crate) schema_fingerprint: SchemaFingerprint,
    pub(crate) locator: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProjectionBuildMode {
    Snapshot,
    Incremental,
}

impl ProjectionBuildMode {
    const fn tag(self) -> u8 {
        match self {
            Self::Snapshot => 1,
            Self::Incremental => 2,
        }
    }

    fn from_tag(tag: u8) -> Result<Self, ProjectionCatalogError> {
        match tag {
            1 => Ok(Self::Snapshot),
            2 => Ok(Self::Incremental),
            _ => Err(ProjectionCatalogError::PendingBuildCorrupt(
                "unknown projection build mode".into(),
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProjectionBuildIntent {
    pub(crate) id: ColumnarProjectionId,
    pub(crate) table_id: TableId,
    pub(crate) source_storage_id: StorageId,
    pub(crate) generation: ColumnarGeneration,
    pub(crate) schema_fingerprint: SchemaFingerprint,
    pub(crate) locator: String,
    pub(crate) mode: ProjectionBuildMode,
    pub(crate) columns: Vec<ColumnId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProjectionCatalog {
    path: PathBuf,
    incarnation: [u8; 16],
    next_id: u64,
    entries: Vec<ProjectionCatalogEntry>,
    pending_build: Option<ProjectionBuildIntent>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CatalogMarker {
    version: u16,
    incarnation: [u8; 16],
    next_id: u64,
    catalog_crc: u32,
}

impl ProjectionCatalog {
    pub(crate) fn open_or_initialize(
        schema_catalog: &Path,
        incarnation: [u8; 16],
    ) -> Result<Self, ProjectionCatalogError> {
        let path = catalog_path(schema_catalog);
        let marker_path = marker_path(&path);
        let catalog_bytes = optional_read(&path)?;
        let marker_bytes = optional_read(&marker_path)?;
        match (catalog_bytes, marker_bytes) {
            (None, None) => {
                let catalog = Self {
                    path,
                    incarnation,
                    next_id: 1,
                    entries: Vec::new(),
                    pending_build: None,
                };
                catalog.publish()?;
                Ok(catalog)
            }
            (None, Some(_)) => Err(ProjectionCatalogError::Corrupt(
                "catalog marker exists but catalog is missing",
            )),
            (Some(bytes), marker) => {
                let (mut catalog, catalog_version) = Self::decode_at(&path, &bytes)?;
                if catalog.incarnation != incarnation {
                    return Err(ProjectionCatalogError::Corrupt(
                        "database incarnation mismatch",
                    ));
                }
                let crc = crc32c::crc32c(&bytes);
                match marker {
                    Some(marker) => {
                        let marker = CatalogMarker::decode(&marker)?;
                        let catalog_is_at_least_marker = match (catalog.next_id, marker.next_id) {
                            (0, _) => true,
                            (_, 0) => false,
                            (catalog, marker) => catalog >= marker,
                        };
                        if marker.incarnation != incarnation || !catalog_is_at_least_marker {
                            return Err(ProjectionCatalogError::Corrupt(
                                "catalog marker identity or high-water mismatch",
                            ));
                        }
                        if catalog_version == LEGACY_FORMAT_VERSION {
                            catalog.path = path.clone();
                            catalog.publish()?;
                        } else if marker.version != FORMAT_VERSION
                            || marker.catalog_crc != crc
                            || marker.next_id != catalog.next_id
                        {
                            write_marker(&marker_path, incarnation, catalog.next_id, crc)?;
                        }
                    }
                    None if catalog_version == LEGACY_FORMAT_VERSION => {
                        catalog.path = path.clone();
                        catalog.publish()?;
                    }
                    None => write_marker(&marker_path, incarnation, catalog.next_id, crc)?,
                }
                catalog.path = path;
                remove_shadow(&catalog.path);
                remove_shadow(&marker_path);
                Ok(catalog)
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn decode_for_test(bytes: &[u8]) -> Result<Self, ProjectionCatalogError> {
        Self::decode_at(Path::new("catalog.nbpc"), bytes).map(|(catalog, _)| catalog)
    }

    #[cfg(test)]
    pub(crate) fn encoded_for_test(&self) -> Result<Vec<u8>, ProjectionCatalogError> {
        self.encode()
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn next_id(&self) -> Option<ColumnarProjectionId> {
        (self.next_id != 0).then_some(ColumnarProjectionId(self.next_id))
    }

    pub(crate) fn entries(&self) -> &[ProjectionCatalogEntry] {
        &self.entries
    }

    pub(crate) fn pending_build(&self) -> Option<&ProjectionBuildIntent> {
        self.pending_build.as_ref()
    }

    pub(crate) fn resolve(&self, entry: &ProjectionCatalogEntry) -> PathBuf {
        schema_file::resolve(&self.path, &entry.locator)
    }

    pub(crate) fn resolve_pending(&self, intent: &ProjectionBuildIntent) -> PathBuf {
        schema_file::resolve(&self.path, &intent.locator)
    }

    pub(crate) fn locator(&self, directory: &Path) -> Result<String, ProjectionCatalogError> {
        schema_file::relative(&self.path, directory).map_err(|_| {
            ProjectionCatalogError::InvalidFormat(
                "projection location must be a nonempty UTF-8 relative path",
            )
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn begin_build(
        &mut self,
        table_id: TableId,
        source_storage_id: StorageId,
        generation: ColumnarGeneration,
        schema_fingerprint: SchemaFingerprint,
        locator: String,
        mode: ProjectionBuildMode,
        columns: Vec<ColumnId>,
    ) -> Result<ColumnarProjectionId, ProjectionCatalogError> {
        if let Some(pending) = &self.pending_build {
            return Err(ProjectionCatalogError::PendingBuildExists(pending.id));
        }
        let id = self
            .next_id()
            .ok_or(ProjectionCatalogError::CapacityExceeded("identity space"))?;
        let mut candidate = self.clone();
        candidate.next_id = self.next_id.checked_add(1).unwrap_or(0);
        candidate.pending_build = Some(ProjectionBuildIntent {
            id,
            table_id,
            source_storage_id,
            generation,
            schema_fingerprint,
            locator,
            mode,
            columns,
        });
        candidate.validate()?;
        *self = candidate;
        self.publish()?;
        crash("build-intent-durable");
        Ok(id)
    }

    pub(crate) fn commit_build(
        &mut self,
        entry: ProjectionCatalogEntry,
        mode: ProjectionBuildMode,
        columns: &[ColumnId],
    ) -> Result<(), ProjectionCatalogError> {
        let pending = self
            .pending_build
            .as_ref()
            .ok_or(ProjectionCatalogError::PendingBuildAbsent)?;
        if pending.id != entry.id
            || pending.table_id != entry.table_id
            || pending.source_storage_id != entry.source_storage_id
            || pending.generation != entry.generation
            || pending.schema_fingerprint != entry.schema_fingerprint
            || pending.locator != entry.locator
            || pending.mode != mode
            || pending.columns != columns
        {
            return Err(ProjectionCatalogError::PendingBuildMismatch(
                "published artifact identity differs from durable intent",
            ));
        }
        if self.entries.iter().any(|current| current.id == entry.id) {
            return Err(ProjectionCatalogError::Corrupt(
                "duplicate projection identity",
            ));
        }
        if self.entries.len() >= MAX_ENTRIES as usize {
            return Err(ProjectionCatalogError::CapacityExceeded("entry count"));
        }
        if self
            .entries
            .iter()
            .any(|current| current.locator == entry.locator)
        {
            return Err(ProjectionCatalogError::Corrupt(
                "duplicate projection location",
            ));
        }
        self.pending_build = None;
        self.entries.push(entry);
        self.entries.sort_by_key(|entry| entry.id.0);
        self.publish()
    }

    pub(crate) fn abort_build(
        &mut self,
        id: ColumnarProjectionId,
    ) -> Result<(), ProjectionCatalogError> {
        let pending = self
            .pending_build
            .as_ref()
            .ok_or(ProjectionCatalogError::PendingBuildAbsent)?;
        if pending.id != id {
            return Err(ProjectionCatalogError::PendingBuildMismatch(
                "abort identity differs from durable intent",
            ));
        }
        self.pending_build = None;
        self.publish()
    }

    pub(crate) fn insert(
        &mut self,
        entry: ProjectionCatalogEntry,
    ) -> Result<(), ProjectionCatalogError> {
        if self.entries.iter().any(|current| current.id == entry.id) {
            return Err(ProjectionCatalogError::Corrupt(
                "duplicate projection identity",
            ));
        }
        if self
            .pending_build
            .as_ref()
            .is_some_and(|pending| pending.id == entry.id)
        {
            return Err(ProjectionCatalogError::Corrupt(
                "active and pending projection identities collide",
            ));
        }
        if self
            .entries
            .iter()
            .any(|current| current.locator == entry.locator)
        {
            return Err(ProjectionCatalogError::Corrupt(
                "duplicate projection location",
            ));
        }
        if self
            .pending_build
            .as_ref()
            .is_some_and(|pending| pending.locator == entry.locator)
        {
            return Err(ProjectionCatalogError::Corrupt(
                "active and pending projection locations collide",
            ));
        }
        if self.next_id != 0 && entry.id.0 >= self.next_id {
            self.next_id = entry.id.0.checked_add(1).unwrap_or(0);
        }
        self.entries.push(entry);
        self.entries.sort_by_key(|entry| entry.id.0);
        self.publish()
    }

    pub(crate) fn update_generation(
        &mut self,
        id: ColumnarProjectionId,
        generation: ColumnarGeneration,
    ) -> Result<(), ProjectionCatalogError> {
        let entry = self.entries.iter_mut().find(|entry| entry.id == id).ok_or(
            ProjectionCatalogError::Corrupt("projection identity absent"),
        )?;
        if generation.0 < entry.generation.0 {
            return Err(ProjectionCatalogError::Corrupt(
                "projection generation moved backwards",
            ));
        }
        entry.generation = generation;
        self.publish()
    }

    pub(crate) fn remove(
        &mut self,
        id: ColumnarProjectionId,
    ) -> Result<bool, ProjectionCatalogError> {
        let old_len = self.entries.len();
        self.entries.retain(|entry| entry.id != id);
        if self.entries.len() == old_len {
            return Ok(false);
        }
        self.publish()?;
        crash("drop-catalog-removal");
        Ok(true)
    }

    pub(crate) fn contains_locator(&self, locator: &str) -> bool {
        self.entries.iter().any(|entry| entry.locator == locator)
    }

    fn publish(&self) -> Result<(), ProjectionCatalogError> {
        let bytes = self.encode()?;
        atomic_write(&self.path, &bytes, true)?;
        crash("catalog-renamed");
        write_marker(
            &marker_path(&self.path),
            self.incarnation,
            self.next_id,
            crc32c::crc32c(&bytes),
        )
    }

    fn encode(&self) -> Result<Vec<u8>, ProjectionCatalogError> {
        self.validate()?;
        let mut bytes = Vec::new();
        bytes.extend_from_slice(CATALOG_MAGIC);
        push_u16(&mut bytes, FORMAT_VERSION);
        push_u16(&mut bytes, 0);
        bytes.extend_from_slice(&self.incarnation);
        push_u64(&mut bytes, self.next_id);
        push_u32(
            &mut bytes,
            u32::try_from(self.entries.len())
                .map_err(|_| ProjectionCatalogError::CapacityExceeded("entry count"))?,
        );
        bytes.push(u8::from(self.pending_build.is_some()));
        bytes.extend_from_slice(&[0; 3]);
        for entry in &self.entries {
            push_u64(&mut bytes, entry.id.0);
            push_u64(&mut bytes, entry.table_id.0);
            push_u64(&mut bytes, entry.source_storage_id.0);
            push_u64(&mut bytes, entry.generation.0);
            bytes.extend_from_slice(entry.schema_fingerprint.as_bytes());
            push_string(&mut bytes, &entry.locator)?;
        }
        if let Some(pending) = &self.pending_build {
            push_u64(&mut bytes, pending.id.0);
            push_u64(&mut bytes, pending.table_id.0);
            push_u64(&mut bytes, pending.source_storage_id.0);
            push_u64(&mut bytes, pending.generation.0);
            bytes.extend_from_slice(pending.schema_fingerprint.as_bytes());
            bytes.push(pending.mode.tag());
            bytes.extend_from_slice(&[0; 3]);
            push_u32(
                &mut bytes,
                u32::try_from(pending.columns.len()).map_err(|_| {
                    ProjectionCatalogError::CapacityExceeded("pending column count")
                })?,
            );
            for column_id in &pending.columns {
                push_u32(&mut bytes, column_id.0);
            }
            push_string(&mut bytes, &pending.locator)?;
        }
        let checksum = crc32c::crc32c(&bytes);
        push_u32(&mut bytes, checksum);
        Ok(bytes)
    }

    fn decode_at(path: &Path, bytes: &[u8]) -> Result<(Self, u16), ProjectionCatalogError> {
        validate_checksum(bytes)?;
        let payload = bytes
            .get(..bytes.len().saturating_sub(4))
            .ok_or(ProjectionCatalogError::InvalidFormat("truncated checksum"))?;
        let mut reader = Reader(payload);
        if reader.take(4)? != CATALOG_MAGIC {
            return Err(ProjectionCatalogError::InvalidFormat("bad magic"));
        }
        let version = reader.u16()?;
        if version != LEGACY_FORMAT_VERSION && version != FORMAT_VERSION {
            return Err(ProjectionCatalogError::UnsupportedVersion(version));
        }
        if reader.u16()? != 0 {
            return Err(ProjectionCatalogError::Corrupt("reserved header bytes"));
        }
        let incarnation = reader.array()?;
        let next_id = reader.u64()?;
        let count = reader.u32()?;
        if count > MAX_ENTRIES {
            return Err(ProjectionCatalogError::CapacityExceeded("entry count"));
        }
        let pending_present = if version == FORMAT_VERSION {
            let present = reader.u8()?;
            if present > 1 || reader.take(3)? != [0; 3] {
                return Err(ProjectionCatalogError::PendingBuildCorrupt(
                    "invalid pending-build header".into(),
                ));
            }
            present == 1
        } else {
            false
        };
        let mut entries = Vec::with_capacity(count as usize);
        for _ in 0..count {
            entries.push(ProjectionCatalogEntry {
                id: ColumnarProjectionId(reader.u64()?),
                table_id: TableId(reader.u64()?),
                source_storage_id: StorageId(reader.u64()?),
                generation: ColumnarGeneration(reader.u64()?),
                schema_fingerprint: SchemaFingerprint::from_bytes(reader.array()?),
                locator: reader.string()?,
            });
        }
        let pending_build = if pending_present {
            let id = ColumnarProjectionId(reader.u64()?);
            let table_id = TableId(reader.u64()?);
            let source_storage_id = StorageId(reader.u64()?);
            let generation = ColumnarGeneration(reader.u64()?);
            let schema_fingerprint = SchemaFingerprint::from_bytes(reader.array()?);
            let mode = ProjectionBuildMode::from_tag(reader.u8()?)?;
            if reader.take(3)? != [0; 3] {
                return Err(ProjectionCatalogError::PendingBuildCorrupt(
                    "reserved pending-build bytes".into(),
                ));
            }
            let column_count = reader.u32()?;
            if column_count > MAX_PENDING_COLUMNS {
                return Err(ProjectionCatalogError::CapacityExceeded(
                    "pending column count",
                ));
            }
            let mut columns = Vec::with_capacity(column_count as usize);
            for _ in 0..column_count {
                columns.push(ColumnId(reader.u32()?));
            }
            Some(ProjectionBuildIntent {
                id,
                table_id,
                source_storage_id,
                generation,
                schema_fingerprint,
                locator: reader.string()?,
                mode,
                columns,
            })
        } else {
            None
        };
        if !reader.0.is_empty() {
            return Err(ProjectionCatalogError::Corrupt("trailing bytes"));
        }
        let catalog = Self {
            path: path.to_owned(),
            incarnation,
            next_id,
            entries,
            pending_build,
        };
        catalog.validate()?;
        Ok((catalog, version))
    }

    fn validate(&self) -> Result<(), ProjectionCatalogError> {
        if self.incarnation == [0; 16] {
            return Err(ProjectionCatalogError::Corrupt("zero database incarnation"));
        }
        if self.entries.len() > MAX_ENTRIES as usize {
            return Err(ProjectionCatalogError::CapacityExceeded("entry count"));
        }
        let mut ids = BTreeSet::new();
        let mut locators = BTreeSet::new();
        let mut maximum = 0;
        for entry in &self.entries {
            if entry.id.0 == 0
                || entry.table_id.0 == 0
                || entry.source_storage_id.0 == 0
                || entry.generation.0 == 0
                || entry.locator.is_empty()
                || entry.locator.as_bytes().contains(&0)
                || Path::new(&entry.locator).is_absolute()
                || !ids.insert(entry.id)
                || !locators.insert(entry.locator.as_str())
            {
                return Err(ProjectionCatalogError::Corrupt(
                    "invalid or duplicate projection identity/location",
                ));
            }
            maximum = maximum.max(entry.id.0);
        }
        if let Some(pending) = &self.pending_build {
            if pending.id.0 == 0
                || pending.table_id.0 == 0
                || pending.source_storage_id.0 == 0
                || pending.generation.0 == 0
                || pending.locator.is_empty()
                || pending.locator.as_bytes().contains(&0)
                || Path::new(&pending.locator).is_absolute()
                || pending.columns.is_empty()
                || pending.columns.len() > MAX_PENDING_COLUMNS as usize
                || !ids.insert(pending.id)
                || !locators.insert(pending.locator.as_str())
            {
                return Err(ProjectionCatalogError::PendingBuildMismatch(
                    "invalid or duplicate pending build identity/location",
                ));
            }
            maximum = maximum.max(pending.id.0);
        }
        if self.next_id != 0 && self.next_id <= maximum {
            return Err(ProjectionCatalogError::Corrupt(
                "invalid identity high-water",
            ));
        }
        Ok(())
    }
}

impl CatalogMarker {
    fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MARKER_MAGIC);
        push_u16(&mut bytes, self.version);
        push_u16(&mut bytes, 0);
        bytes.extend_from_slice(&self.incarnation);
        push_u64(&mut bytes, self.next_id);
        push_u32(&mut bytes, self.catalog_crc);
        let checksum = crc32c::crc32c(&bytes);
        push_u32(&mut bytes, checksum);
        bytes
    }

    fn decode(bytes: &[u8]) -> Result<Self, ProjectionCatalogError> {
        validate_checksum(bytes)?;
        let payload = &bytes[..bytes.len() - 4];
        let mut reader = Reader(payload);
        if reader.take(4)? != MARKER_MAGIC {
            return Err(ProjectionCatalogError::InvalidFormat("bad marker magic"));
        }
        let version = reader.u16()?;
        if version != LEGACY_FORMAT_VERSION && version != FORMAT_VERSION {
            return Err(ProjectionCatalogError::UnsupportedVersion(version));
        }
        if reader.u16()? != 0 {
            return Err(ProjectionCatalogError::Corrupt("reserved marker bytes"));
        }
        let marker = Self {
            version,
            incarnation: reader.array()?,
            next_id: reader.u64()?,
            catalog_crc: reader.u32()?,
        };
        if !reader.0.is_empty() || marker.incarnation == [0; 16] {
            return Err(ProjectionCatalogError::Corrupt("invalid marker"));
        }
        Ok(marker)
    }
}

pub(crate) fn catalog_path(schema_catalog: &Path) -> PathBuf {
    schema_file::suffix(schema_catalog, ".projections")
}

fn marker_path(catalog: &Path) -> PathBuf {
    schema_file::suffix(catalog, ".state")
}

fn write_marker(
    path: &Path,
    incarnation: [u8; 16],
    next_id: u64,
    catalog_crc: u32,
) -> Result<(), ProjectionCatalogError> {
    atomic_write(
        path,
        &CatalogMarker {
            version: FORMAT_VERSION,
            incarnation,
            next_id,
            catalog_crc,
        }
        .encode(),
        false,
    )
}

fn read_bounded(path: &Path) -> Result<Vec<u8>, ProjectionCatalogError> {
    let file = File::open(path).map_err(|source| ProjectionCatalogError::Io {
        operation: "open projection catalog metadata",
        path: path.to_owned(),
        source,
    })?;
    let length = file
        .metadata()
        .map_err(|source| ProjectionCatalogError::Io {
            operation: "stat projection catalog metadata",
            path: path.to_owned(),
            source,
        })?
        .len();
    if length > MAX_FILE_BYTES {
        return Err(ProjectionCatalogError::CapacityExceeded("file bytes"));
    }
    let mut bytes = Vec::with_capacity(length as usize);
    file.take(MAX_FILE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|source| ProjectionCatalogError::Io {
            operation: "read projection catalog metadata",
            path: path.to_owned(),
            source,
        })?;
    if bytes.len() as u64 > MAX_FILE_BYTES {
        return Err(ProjectionCatalogError::CapacityExceeded("file bytes"));
    }
    Ok(bytes)
}

fn optional_read(path: &Path) -> Result<Option<Vec<u8>>, ProjectionCatalogError> {
    match read_bounded(path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(ProjectionCatalogError::Io { source, .. })
            if source.kind() == std::io::ErrorKind::NotFound =>
        {
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

fn atomic_write(path: &Path, bytes: &[u8], catalog: bool) -> Result<(), ProjectionCatalogError> {
    let shadow = schema_file::suffix(path, ".next");
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&shadow)
        .map_err(|source| ProjectionCatalogError::Io {
            operation: "create projection catalog shadow",
            path: shadow.clone(),
            source,
        })?;
    let split = bytes.len() / 2;
    file.write_all(&bytes[..split])
        .map_err(|source| ProjectionCatalogError::Io {
            operation: "write projection catalog shadow",
            path: shadow.clone(),
            source,
        })?;
    if catalog {
        crash("catalog-mid-write");
    }
    file.write_all(&bytes[split..])
        .map_err(|source| ProjectionCatalogError::Io {
            operation: "write projection catalog shadow",
            path: shadow.clone(),
            source,
        })?;
    if catalog {
        crash("catalog-temp-written");
    }
    file.sync_all()
        .map_err(|source| ProjectionCatalogError::Io {
            operation: "sync projection catalog shadow",
            path: shadow.clone(),
            source,
        })?;
    if catalog {
        crash("catalog-temp-synced");
        crash("catalog-before-rename");
    }
    std::fs::rename(&shadow, path).map_err(|source| ProjectionCatalogError::Io {
        operation: "publish projection catalog metadata",
        path: path.to_owned(),
        source,
    })?;
    if catalog {
        crash("catalog-after-rename");
    }
    schema_file::sync_parent(path).map_err(|error| ProjectionCatalogError::Io {
        operation: "sync projection catalog directory",
        path: path.parent().unwrap_or(Path::new(".")).to_owned(),
        source: std::io::Error::other(error.to_string()),
    })
}

fn remove_shadow(path: &Path) {
    let _ = std::fs::remove_file(schema_file::suffix(path, ".next"));
}

fn validate_checksum(bytes: &[u8]) -> Result<(), ProjectionCatalogError> {
    let stored = bytes
        .get(bytes.len().saturating_sub(4)..)
        .and_then(|bytes| <[u8; 4]>::try_from(bytes).ok())
        .map(u32::from_le_bytes)
        .ok_or(ProjectionCatalogError::InvalidFormat("missing checksum"))?;
    let payload = bytes
        .get(..bytes.len().saturating_sub(4))
        .ok_or(ProjectionCatalogError::InvalidFormat("truncated checksum"))?;
    if crc32c::crc32c(payload) != stored {
        return Err(ProjectionCatalogError::ChecksumMismatch);
    }
    Ok(())
}

fn push_u16(bytes: &mut Vec<u8>, value: u16) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn push_u32(bytes: &mut Vec<u8>, value: u32) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn push_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn push_string(bytes: &mut Vec<u8>, value: &str) -> Result<(), ProjectionCatalogError> {
    let length = u32::try_from(value.len())
        .map_err(|_| ProjectionCatalogError::CapacityExceeded("locator bytes"))?;
    if length > MAX_LOCATOR_BYTES {
        return Err(ProjectionCatalogError::CapacityExceeded("locator bytes"));
    }
    push_u32(bytes, length);
    bytes.extend_from_slice(value.as_bytes());
    Ok(())
}

#[cfg(test)]
pub(crate) fn write_v1_fixture_for_test(
    schema_catalog: &Path,
) -> Result<(), ProjectionCatalogError> {
    let path = catalog_path(schema_catalog);
    let bytes = read_bounded(&path)?;
    let (catalog, _) = ProjectionCatalog::decode_at(&path, &bytes)?;
    let mut legacy = Vec::new();
    legacy.extend_from_slice(CATALOG_MAGIC);
    push_u16(&mut legacy, LEGACY_FORMAT_VERSION);
    push_u16(&mut legacy, 0);
    legacy.extend_from_slice(&catalog.incarnation);
    push_u64(&mut legacy, catalog.next_id);
    push_u32(
        &mut legacy,
        u32::try_from(catalog.entries.len())
            .map_err(|_| ProjectionCatalogError::CapacityExceeded("entry count"))?,
    );
    for entry in &catalog.entries {
        push_u64(&mut legacy, entry.id.0);
        push_u64(&mut legacy, entry.table_id.0);
        push_u64(&mut legacy, entry.source_storage_id.0);
        push_u64(&mut legacy, entry.generation.0);
        legacy.extend_from_slice(entry.schema_fingerprint.as_bytes());
        push_string(&mut legacy, &entry.locator)?;
    }
    let checksum = crc32c::crc32c(&legacy);
    push_u32(&mut legacy, checksum);
    atomic_write(&path, &legacy, true)?;
    atomic_write(
        &marker_path(&path),
        &CatalogMarker {
            version: LEGACY_FORMAT_VERSION,
            incarnation: catalog.incarnation,
            next_id: catalog.next_id,
            catalog_crc: crc32c::crc32c(&legacy),
        }
        .encode(),
        false,
    )
}

struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn take(&mut self, length: usize) -> Result<&'a [u8], ProjectionCatalogError> {
        let (value, remaining) = self
            .0
            .split_at_checked(length)
            .ok_or(ProjectionCatalogError::InvalidFormat("truncated field"))?;
        self.0 = remaining;
        Ok(value)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], ProjectionCatalogError> {
        self.take(N)?
            .try_into()
            .map_err(|_| ProjectionCatalogError::InvalidFormat("truncated array"))
    }

    fn u16(&mut self) -> Result<u16, ProjectionCatalogError> {
        Ok(u16::from_le_bytes(self.array()?))
    }

    fn u8(&mut self) -> Result<u8, ProjectionCatalogError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, ProjectionCatalogError> {
        Ok(u32::from_le_bytes(self.array()?))
    }

    fn u64(&mut self) -> Result<u64, ProjectionCatalogError> {
        Ok(u64::from_le_bytes(self.array()?))
    }

    fn string(&mut self) -> Result<String, ProjectionCatalogError> {
        let length = self.u32()?;
        if length > MAX_LOCATOR_BYTES {
            return Err(ProjectionCatalogError::CapacityExceeded("locator bytes"));
        }
        String::from_utf8(self.take(length as usize)?.to_vec())
            .map_err(|_| ProjectionCatalogError::Corrupt("locator is not UTF-8"))
    }
}

#[cfg(test)]
fn crash(point: &str) {
    if std::env::var("NETBADB_PROJECTION_CATALOG_CRASH_POINT").as_deref() == Ok(point) {
        std::process::exit(88);
    }
}

#[cfg(not(test))]
fn crash(_: &str) {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_PATH: AtomicU64 = AtomicU64::new(1);

    fn sample() -> ProjectionCatalog {
        ProjectionCatalog {
            path: PathBuf::from("catalog.nbpc"),
            incarnation: [7; 16],
            next_id: 3,
            entries: vec![ProjectionCatalogEntry {
                id: ColumnarProjectionId(2),
                table_id: TableId(4),
                source_storage_id: StorageId(6),
                generation: ColumnarGeneration(8),
                schema_fingerprint: SchemaFingerprint::from_bytes([9; 32]),
                locator: "projection-a".into(),
            }],
            pending_build: None,
        }
    }

    fn pending_sample() -> ProjectionCatalog {
        let mut catalog = sample();
        catalog.next_id = 4;
        catalog.pending_build = Some(ProjectionBuildIntent {
            id: ColumnarProjectionId(3),
            table_id: TableId(4),
            source_storage_id: StorageId(6),
            generation: ColumnarGeneration(1),
            schema_fingerprint: SchemaFingerprint::from_bytes([9; 32]),
            locator: "projection-b".into(),
            mode: ProjectionBuildMode::Incremental,
            columns: vec![ColumnId(8), ColumnId(3)],
        });
        catalog
    }

    fn encode_v1(catalog: &ProjectionCatalog) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(CATALOG_MAGIC);
        push_u16(&mut bytes, LEGACY_FORMAT_VERSION);
        push_u16(&mut bytes, 0);
        bytes.extend_from_slice(&catalog.incarnation);
        push_u64(&mut bytes, catalog.next_id);
        push_u32(&mut bytes, catalog.entries.len() as u32);
        for entry in &catalog.entries {
            push_u64(&mut bytes, entry.id.0);
            push_u64(&mut bytes, entry.table_id.0);
            push_u64(&mut bytes, entry.source_storage_id.0);
            push_u64(&mut bytes, entry.generation.0);
            bytes.extend_from_slice(entry.schema_fingerprint.as_bytes());
            push_string(&mut bytes, &entry.locator).unwrap();
        }
        let checksum = crc32c::crc32c(&bytes);
        push_u32(&mut bytes, checksum);
        bytes
    }

    fn migration_root(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "netbadb-projection-catalog-{name}-{}-{}",
            std::process::id(),
            NEXT_PATH.fetch_add(1, Ordering::Relaxed)
        ))
    }

    #[test]
    fn codec_round_trips_and_rejects_header_checksum_and_truncation() {
        let catalog = sample();
        let bytes = catalog.encoded_for_test().unwrap();
        assert_eq!(ProjectionCatalog::decode_for_test(&bytes).unwrap(), catalog);
        for length in 0..bytes.len() {
            assert!(ProjectionCatalog::decode_for_test(&bytes[..length]).is_err());
        }
        let mut bad_magic = bytes.clone();
        bad_magic[0] ^= 1;
        let checksum = crc32c::crc32c(&bad_magic[..bad_magic.len() - 4]);
        let end = bad_magic.len();
        bad_magic[end - 4..].copy_from_slice(&checksum.to_le_bytes());
        assert!(matches!(
            ProjectionCatalog::decode_for_test(&bad_magic),
            Err(ProjectionCatalogError::InvalidFormat("bad magic"))
        ));
        let mut unsupported = bytes.clone();
        unsupported[4..6].copy_from_slice(&3_u16.to_le_bytes());
        let checksum = crc32c::crc32c(&unsupported[..unsupported.len() - 4]);
        let end = unsupported.len();
        unsupported[end - 4..].copy_from_slice(&checksum.to_le_bytes());
        assert!(matches!(
            ProjectionCatalog::decode_for_test(&unsupported),
            Err(ProjectionCatalogError::UnsupportedVersion(3))
        ));
        let mut checksum = bytes;
        checksum[20] ^= 1;
        assert!(matches!(
            ProjectionCatalog::decode_for_test(&checksum),
            Err(ProjectionCatalogError::ChecksumMismatch)
        ));
    }

    #[test]
    fn codec_rejects_counts_duplicates_paths_and_invalid_high_water() {
        let mut oversized = sample().encoded_for_test().unwrap();
        oversized[32..36].copy_from_slice(&(MAX_ENTRIES + 1).to_le_bytes());
        let checksum = crc32c::crc32c(&oversized[..oversized.len() - 4]);
        let end = oversized.len();
        oversized[end - 4..].copy_from_slice(&checksum.to_le_bytes());
        assert!(matches!(
            ProjectionCatalog::decode_for_test(&oversized),
            Err(ProjectionCatalogError::CapacityExceeded("entry count"))
        ));

        let mut two_entries = sample();
        let mut second = two_entries.entries[0].clone();
        second.id = ColumnarProjectionId(3);
        second.locator = "projection-b".into();
        two_entries.entries.push(second);
        two_entries.next_id = 4;
        let valid = two_entries.encoded_for_test().unwrap();
        let second_offset = 40 + 68 + "projection-a".len();

        let mut duplicate_id = valid.clone();
        duplicate_id[second_offset..second_offset + 8]
            .copy_from_slice(&ColumnarProjectionId(2).0.to_le_bytes());
        let end = duplicate_id.len();
        let checksum = crc32c::crc32c(&duplicate_id[..end - 4]);
        duplicate_id[end - 4..].copy_from_slice(&checksum.to_le_bytes());
        assert!(matches!(
            ProjectionCatalog::decode_for_test(&duplicate_id),
            Err(ProjectionCatalogError::Corrupt(
                "invalid or duplicate projection identity/location"
            ))
        ));

        let mut duplicate_path = valid;
        let second_locator = second_offset + 68;
        duplicate_path[second_locator..second_locator + "projection-a".len()]
            .copy_from_slice(b"projection-a");
        let end = duplicate_path.len();
        let checksum = crc32c::crc32c(&duplicate_path[..end - 4]);
        duplicate_path[end - 4..].copy_from_slice(&checksum.to_le_bytes());
        assert!(matches!(
            ProjectionCatalog::decode_for_test(&duplicate_path),
            Err(ProjectionCatalogError::Corrupt(
                "invalid or duplicate projection identity/location"
            ))
        ));

        let mut high_water = sample();
        high_water.next_id = 2;
        assert!(high_water.encode().is_err());
    }

    #[test]
    fn v2_pending_codec_has_fixed_layout_and_preserves_ordered_columns() {
        let catalog = pending_sample();
        let bytes = catalog.encoded_for_test().unwrap();
        assert_eq!(&bytes[0..4], CATALOG_MAGIC);
        assert_eq!(u16::from_le_bytes(bytes[4..6].try_into().unwrap()), 2);
        assert_eq!(&bytes[6..8], &[0; 2]);
        assert_eq!(&bytes[8..24], &[7; 16]);
        assert_eq!(u64::from_le_bytes(bytes[24..32].try_into().unwrap()), 4);
        assert_eq!(u32::from_le_bytes(bytes[32..36].try_into().unwrap()), 1);
        assert_eq!(&bytes[36..40], &[1, 0, 0, 0]);
        let pending_offset = 40 + 68 + "projection-a".len();
        assert_eq!(
            u64::from_le_bytes(
                bytes[pending_offset..pending_offset + 8]
                    .try_into()
                    .unwrap()
            ),
            3
        );
        assert_eq!(bytes[pending_offset + 64], 2);
        assert_eq!(
            u32::from_le_bytes(
                bytes[pending_offset + 68..pending_offset + 72]
                    .try_into()
                    .unwrap()
            ),
            2
        );
        assert_eq!(
            u32::from_le_bytes(
                bytes[pending_offset + 72..pending_offset + 76]
                    .try_into()
                    .unwrap()
            ),
            8
        );
        assert_eq!(
            u32::from_le_bytes(
                bytes[pending_offset + 76..pending_offset + 80]
                    .try_into()
                    .unwrap()
            ),
            3
        );
        assert_eq!(ProjectionCatalog::decode_for_test(&bytes).unwrap(), catalog);
        for length in 0..bytes.len() {
            assert!(ProjectionCatalog::decode_for_test(&bytes[..length]).is_err());
        }
    }

    #[test]
    fn v2_pending_codec_rejects_unknown_mode_oversized_count_and_active_collisions() {
        let valid = pending_sample().encoded_for_test().unwrap();
        let pending_offset = 40 + 68 + "projection-a".len();

        let mut unknown_mode = valid.clone();
        unknown_mode[pending_offset + 64] = 9;
        let end = unknown_mode.len();
        let checksum = crc32c::crc32c(&unknown_mode[..end - 4]);
        unknown_mode[end - 4..].copy_from_slice(&checksum.to_le_bytes());
        assert!(matches!(
            ProjectionCatalog::decode_for_test(&unknown_mode),
            Err(ProjectionCatalogError::PendingBuildCorrupt(_))
        ));

        let mut oversized = valid.clone();
        oversized[pending_offset + 68..pending_offset + 72]
            .copy_from_slice(&(MAX_PENDING_COLUMNS + 1).to_le_bytes());
        let end = oversized.len();
        let checksum = crc32c::crc32c(&oversized[..end - 4]);
        oversized[end - 4..].copy_from_slice(&checksum.to_le_bytes());
        assert!(matches!(
            ProjectionCatalog::decode_for_test(&oversized),
            Err(ProjectionCatalogError::CapacityExceeded(
                "pending column count"
            ))
        ));

        let mut duplicate_id = valid.clone();
        duplicate_id[pending_offset..pending_offset + 8]
            .copy_from_slice(&ColumnarProjectionId(2).0.to_le_bytes());
        let end = duplicate_id.len();
        let checksum = crc32c::crc32c(&duplicate_id[..end - 4]);
        duplicate_id[end - 4..].copy_from_slice(&checksum.to_le_bytes());
        assert!(matches!(
            ProjectionCatalog::decode_for_test(&duplicate_id),
            Err(ProjectionCatalogError::PendingBuildMismatch(_))
        ));

        let mut duplicate_locator = valid;
        let locator_offset = pending_offset + 84;
        duplicate_locator[locator_offset..locator_offset + "projection-a".len()]
            .copy_from_slice(b"projection-a");
        let end = duplicate_locator.len();
        let checksum = crc32c::crc32c(&duplicate_locator[..end - 4]);
        duplicate_locator[end - 4..].copy_from_slice(&checksum.to_le_bytes());
        assert!(matches!(
            ProjectionCatalog::decode_for_test(&duplicate_locator),
            Err(ProjectionCatalogError::PendingBuildMismatch(_))
        ));
    }

    #[test]
    fn v1_catalog_migrates_to_v2_without_changing_entries_or_high_water() {
        let root = migration_root("v1");
        std::fs::create_dir_all(&root).unwrap();
        let schema_catalog = root.join("catalog");
        let path = catalog_path(&schema_catalog);
        let marker = marker_path(&path);
        let mut legacy = sample();
        legacy.next_id = 10;
        let historical_orphan = root.join("historical-orphan");
        std::fs::create_dir_all(&historical_orphan).unwrap();
        std::fs::write(
            historical_orphan.join("projection.nbcmanifest"),
            b"unregistered",
        )
        .unwrap();
        let v1 = encode_v1(&legacy);
        std::fs::write(&path, &v1).unwrap();
        std::fs::write(
            &marker,
            CatalogMarker {
                version: LEGACY_FORMAT_VERSION,
                incarnation: legacy.incarnation,
                next_id: legacy.next_id,
                catalog_crc: crc32c::crc32c(&v1),
            }
            .encode(),
        )
        .unwrap();

        let migrated = ProjectionCatalog::open_or_initialize(&schema_catalog, [7; 16]).unwrap();
        assert_eq!(migrated.entries(), legacy.entries());
        assert_eq!(migrated.next_id(), Some(ColumnarProjectionId(10)));
        assert!(migrated.pending_build().is_none());
        assert_eq!(
            migrated.entries().len(),
            1,
            "filesystem orphans are not scanned"
        );
        assert!(historical_orphan.join("projection.nbcmanifest").is_file());
        assert_eq!(
            u16::from_le_bytes(std::fs::read(&path).unwrap()[4..6].try_into().unwrap()),
            2
        );
        assert_eq!(
            u16::from_le_bytes(std::fs::read(&marker).unwrap()[4..6].try_into().unwrap()),
            2
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn v2_catalog_with_v1_marker_repairs_the_marker() {
        let root = migration_root("mixed-marker");
        std::fs::create_dir_all(&root).unwrap();
        let schema_catalog = root.join("catalog");
        let path = catalog_path(&schema_catalog);
        let marker = marker_path(&path);
        let catalog = sample();
        let v2 = catalog.encoded_for_test().unwrap();
        let v1 = encode_v1(&catalog);
        std::fs::write(&path, &v2).unwrap();
        std::fs::write(
            &marker,
            CatalogMarker {
                version: LEGACY_FORMAT_VERSION,
                incarnation: catalog.incarnation,
                next_id: catalog.next_id,
                catalog_crc: crc32c::crc32c(&v1),
            }
            .encode(),
        )
        .unwrap();

        let reopened = ProjectionCatalog::open_or_initialize(&schema_catalog, [7; 16]).unwrap();
        assert_eq!(reopened.entries(), catalog.entries());
        assert_eq!(
            u16::from_le_bytes(std::fs::read(&marker).unwrap()[4..6].try_into().unwrap()),
            2
        );
        std::fs::remove_dir_all(root).unwrap();
    }
}
