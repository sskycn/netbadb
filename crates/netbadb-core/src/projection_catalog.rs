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
use netbadb_types::{ColumnarGeneration, ColumnarProjectionId, StorageId, TableId};

use crate::schema_catalog_file as schema_file;

const CATALOG_MAGIC: &[u8; 4] = b"NBPC";
const MARKER_MAGIC: &[u8; 4] = b"NBPM";
const FORMAT_VERSION: u16 = 1;
const MAX_FILE_BYTES: u64 = 64 * 1024 * 1024;
const MAX_ENTRIES: u32 = 1 << 20;
const MAX_LOCATOR_BYTES: u32 = 4096;

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProjectionCatalog {
    path: PathBuf,
    incarnation: [u8; 16],
    next_id: u64,
    entries: Vec<ProjectionCatalogEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CatalogMarker {
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
                };
                catalog.publish()?;
                Ok(catalog)
            }
            (None, Some(_)) => Err(ProjectionCatalogError::Corrupt(
                "catalog marker exists but catalog is missing",
            )),
            (Some(bytes), marker) => {
                let mut catalog = Self::decode_at(&path, &bytes)?;
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
                        if marker.catalog_crc != crc || marker.next_id != catalog.next_id {
                            write_marker(&marker_path, incarnation, catalog.next_id, crc)?;
                        }
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
        Self::decode_at(Path::new("catalog.nbpc"), bytes)
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

    pub(crate) fn resolve(&self, entry: &ProjectionCatalogEntry) -> PathBuf {
        schema_file::resolve(&self.path, &entry.locator)
    }

    pub(crate) fn locator(&self, directory: &Path) -> Result<String, ProjectionCatalogError> {
        schema_file::relative(&self.path, directory).map_err(|_| {
            ProjectionCatalogError::InvalidFormat(
                "projection location must be a nonempty UTF-8 relative path",
            )
        })
    }

    pub(crate) fn reserve_id(&mut self) -> Result<ColumnarProjectionId, ProjectionCatalogError> {
        let id = self
            .next_id()
            .ok_or(ProjectionCatalogError::CapacityExceeded("identity space"))?;
        self.next_id = self.next_id.checked_add(1).unwrap_or(0);
        self.publish()?;
        crash("id-reserved");
        Ok(id)
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
            .entries
            .iter()
            .any(|current| current.locator == entry.locator)
        {
            return Err(ProjectionCatalogError::Corrupt(
                "duplicate projection location",
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
        for entry in &self.entries {
            push_u64(&mut bytes, entry.id.0);
            push_u64(&mut bytes, entry.table_id.0);
            push_u64(&mut bytes, entry.source_storage_id.0);
            push_u64(&mut bytes, entry.generation.0);
            bytes.extend_from_slice(entry.schema_fingerprint.as_bytes());
            push_string(&mut bytes, &entry.locator)?;
        }
        let checksum = crc32c::crc32c(&bytes);
        push_u32(&mut bytes, checksum);
        Ok(bytes)
    }

    fn decode_at(path: &Path, bytes: &[u8]) -> Result<Self, ProjectionCatalogError> {
        validate_checksum(bytes)?;
        let payload = bytes
            .get(..bytes.len().saturating_sub(4))
            .ok_or(ProjectionCatalogError::InvalidFormat("truncated checksum"))?;
        let mut reader = Reader(payload);
        if reader.take(4)? != CATALOG_MAGIC {
            return Err(ProjectionCatalogError::InvalidFormat("bad magic"));
        }
        let version = reader.u16()?;
        if version != FORMAT_VERSION {
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
        if !reader.0.is_empty() {
            return Err(ProjectionCatalogError::Corrupt("trailing bytes"));
        }
        let catalog = Self {
            path: path.to_owned(),
            incarnation,
            next_id,
            entries,
        };
        catalog.validate()?;
        Ok(catalog)
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
        push_u16(&mut bytes, FORMAT_VERSION);
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
        if version != FORMAT_VERSION {
            return Err(ProjectionCatalogError::UnsupportedVersion(version));
        }
        if reader.u16()? != 0 {
            return Err(ProjectionCatalogError::Corrupt("reserved marker bytes"));
        }
        let marker = Self {
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
        }
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
        unsupported[4..6].copy_from_slice(&2_u16.to_le_bytes());
        let checksum = crc32c::crc32c(&unsupported[..unsupported.len() - 4]);
        let end = unsupported.len();
        unsupported[end - 4..].copy_from_slice(&checksum.to_le_bytes());
        assert!(matches!(
            ProjectionCatalog::decode_for_test(&unsupported),
            Err(ProjectionCatalogError::UnsupportedVersion(2))
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
        let second_offset = 36 + 68 + "projection-a".len();

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
}
