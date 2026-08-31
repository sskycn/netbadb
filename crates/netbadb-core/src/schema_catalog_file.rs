//! Atomic initial installation only; no runtime schema replacement API.
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};

use crate::schema_catalog::{
    MAX_BYTES, Reader, SchemaCatalogError, SchemaCatalogSnapshot, Writer, corrupt, envelope,
    open_envelope,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InstallMarker {
    pub(crate) initialized: bool,
    pub(crate) incarnation: [u8; 16],
    pub(crate) snapshot_crc: u32,
    pub(crate) epoch: u64,
}
impl InstallMarker {
    fn for_snapshot(snapshot: &SchemaCatalogSnapshot, bytes: &[u8]) -> Self {
        Self {
            initialized: false,
            incarnation: snapshot.incarnation,
            snapshot_crc: crc32c::crc32c(bytes),
            epoch: snapshot.epoch,
        }
    }
    fn encode(&self) -> Result<Vec<u8>, SchemaCatalogError> {
        let mut w = Writer(vec![u8::from(self.initialized), 0, 0, 0]);
        w.0.extend_from_slice(&self.incarnation);
        w.u64(self.epoch);
        w.u32(self.snapshot_crc);
        envelope(b"NBSM", &w.0)
    }
    fn decode(bytes: &[u8]) -> Result<Self, SchemaCatalogError> {
        let mut r = Reader(open_envelope(bytes, b"NBSM")?);
        let initialized = match r.u8()? {
            0 => false,
            1 => true,
            _ => return Err(corrupt("unknown installation state")),
        };
        if r.take(3)? != [0; 3] {
            return Err(corrupt("reserved marker bytes"));
        }
        let incarnation = r
            .take(16)?
            .try_into()
            .map_err(|_| corrupt("marker incarnation"))?;
        let epoch = r.u64()?;
        let snapshot_crc = r.u32()?;
        if !r.0.is_empty() || incarnation == [0; 16] || epoch == 0 {
            return Err(corrupt("invalid installation marker"));
        }
        Ok(Self {
            initialized,
            incarnation,
            snapshot_crc,
            epoch,
        })
    }
}

pub(crate) fn suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    value.into()
}
pub(crate) fn marker_path(catalog: &Path) -> PathBuf {
    suffix(catalog, ".state")
}
pub(crate) fn link_path(storage: &Path) -> PathBuf {
    suffix(storage, ".schema-link")
}
pub(crate) fn io(
    operation: &'static str,
    path: &Path,
    source: std::io::Error,
) -> SchemaCatalogError {
    SchemaCatalogError::Io {
        operation,
        path: path.to_owned(),
        source,
    }
}
pub(crate) fn read(path: &Path) -> Result<Vec<u8>, SchemaCatalogError> {
    let file = File::open(path).map_err(|e| io("open schema metadata", path, e))?;
    let length = file
        .metadata()
        .map_err(|e| io("stat schema metadata", path, e))?
        .len();
    if length > MAX_BYTES as u64 {
        return Err(SchemaCatalogError::CapacityExceeded("file bytes"));
    }
    let mut bytes = Vec::new();
    file.take(MAX_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|e| io("read schema metadata", path, e))?;
    if bytes.len() > MAX_BYTES {
        return Err(SchemaCatalogError::CapacityExceeded("file bytes"));
    }
    Ok(bytes)
}
fn optional_read(path: &Path) -> Result<Option<Vec<u8>>, SchemaCatalogError> {
    match read(path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(SchemaCatalogError::Io { source, .. })
            if source.kind() == std::io::ErrorKind::NotFound =>
        {
            Ok(None)
        }
        Err(error) => Err(error),
    }
}
pub(crate) fn marker(catalog: &Path) -> Result<Option<InstallMarker>, SchemaCatalogError> {
    optional_read(&marker_path(catalog))?
        .map(|bytes| InstallMarker::decode(&bytes))
        .transpose()
}
pub(crate) fn load(catalog: &Path) -> Result<SchemaCatalogSnapshot, SchemaCatalogError> {
    let marker = marker(catalog)?
        .filter(|m| m.initialized)
        .ok_or(SchemaCatalogError::LegacyCatalogRequired)?;
    let bytes = optional_read(catalog)?.ok_or(SchemaCatalogError::SchemaCatalogMissing)?;
    let snapshot = SchemaCatalogSnapshot::decode(&bytes)?;
    if marker.incarnation != snapshot.incarnation
        || marker.epoch != snapshot.epoch
        || marker.snapshot_crc != crc32c::crc32c(&bytes)
    {
        return Err(corrupt(
            "snapshot does not match committed installation marker",
        ));
    }
    Ok(snapshot)
}

pub(crate) fn incarnation(catalog: &Path) -> Result<[u8; 16], SchemaCatalogError> {
    if let Some(marker) = marker(catalog)? {
        if marker.initialized {
            return Err(SchemaCatalogError::AlreadyInitialized);
        }
        return Ok(marker.incarnation);
    }
    let mut value = [0; 16];
    getrandom::getrandom(&mut value).map_err(SchemaCatalogError::Randomness)?;
    if value == [0; 16] {
        return Err(corrupt("random incarnation was zero"));
    }
    Ok(value)
}

/// A durable pending marker is never an initialized declaration. Retry must
/// match the original intent, including complete inventory and incarnation.
pub(crate) fn begin(
    catalog: &Path,
    snapshot: &SchemaCatalogSnapshot,
    fresh: bool,
) -> Result<(), SchemaCatalogError> {
    let bytes = snapshot.encode()?;
    let intent = InstallMarker::for_snapshot(snapshot, &bytes);
    match marker(catalog)? {
        Some(existing) if existing.initialized => {
            return Err(SchemaCatalogError::AlreadyInitialized);
        }
        Some(_) if fresh => return Err(SchemaCatalogError::LegacyCatalogRequired),
        Some(existing) if existing != intent => {
            return Err(SchemaCatalogError::InventoryMismatch(
                "bootstrap retry differs from durable intent",
            ));
        }
        Some(_) => return Ok(()),
        None => {}
    }
    if fresh
        && catalog
            .try_exists()
            .map_err(|e| io("inspect catalog path", catalog, e))?
    {
        return Err(SchemaCatalogError::PathConflict(catalog.to_owned()));
    }
    atomic_write(&marker_path(catalog), &intent.encode()?, false)?;
    Ok(())
}

pub(crate) fn install_initial(
    catalog: &Path,
    snapshot: &SchemaCatalogSnapshot,
) -> Result<SchemaCatalogSnapshot, SchemaCatalogError> {
    let bytes = snapshot.encode()?;
    let mut intent = InstallMarker::for_snapshot(snapshot, &bytes);
    if marker(catalog)? != Some(intent.clone()) {
        return Err(SchemaCatalogError::InventoryMismatch(
            "installation intent missing or changed",
        ));
    }
    // Discovery links contain no schema or commitment decision. They are only
    // compatibility locators; the database-level marker must still validate.
    for storage in &snapshot.storages {
        write_link(
            &resolve(catalog, &storage.locator),
            catalog,
            snapshot.incarnation,
        )?;
    }
    crash("before-catalog-write");
    atomic_write(catalog, &bytes, true)?;
    crash("after-snapshot-durable");
    crash("before-initialized-marker");
    intent.initialized = true;
    atomic_write(&marker_path(catalog), &intent.encode()?, false)?;
    crash("after-initialized-marker-durable");
    let committed = load(catalog)?;
    crash("before-return");
    Ok(committed)
}

fn atomic_write(path: &Path, bytes: &[u8], snapshot: bool) -> Result<(), SchemaCatalogError> {
    let shadow = suffix(path, ".next");
    if let Ok(metadata) = std::fs::symlink_metadata(&shadow) {
        if !metadata.is_file() {
            return Err(SchemaCatalogError::PathConflict(shadow));
        }
    }
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&shadow)
        .map_err(|e| io("create shadow metadata", &shadow, e))?;
    let initialized_marker = bytes.get(..4) == Some(b"NBSM") && bytes.get(16) == Some(&1);
    let split = bytes.len() / 2;
    file.write_all(&bytes[..split])
        .map_err(|e| io("write shadow metadata", &shadow, e))?;
    if snapshot {
        crash("mid-snapshot-write");
    }
    if initialized_marker {
        crash("mid-initialized-marker-write");
    }
    file.write_all(&bytes[split..])
        .map_err(|e| io("write shadow metadata", &shadow, e))?;
    file.sync_all()
        .map_err(|e| io("sync shadow metadata", &shadow, e))?;
    if snapshot {
        crash("after-shadow-sync");
    }
    if initialized_marker {
        crash("after-initialized-marker-shadow-sync");
    }
    std::fs::rename(&shadow, path).map_err(|e| io("publish schema metadata", path, e))?;
    if snapshot {
        crash("after-snapshot-rename");
    }
    if initialized_marker {
        crash("after-initialized-marker-rename");
    }
    sync_parent(path)
}

pub(crate) fn sync_parent(path: &Path) -> Result<(), SchemaCatalogError> {
    let parent = parent(path);
    File::open(parent)
        .and_then(|file| file.sync_all())
        .map_err(|e| io("sync metadata directory", parent, e))
}
fn parent(path: &Path) -> &Path {
    path.parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."))
}

// Normalize lexical paths without requiring a not-yet-created file to exist.
// Canonicalize the parent when possible so aliases cannot split one authority.
pub(crate) fn absolute(path: &Path) -> Result<PathBuf, SchemaCatalogError> {
    let path = if path.is_absolute() {
        path.to_owned()
    } else {
        std::env::current_dir()
            .map_err(|e| io("read working directory", path, e))?
            .join(path)
    };
    // Resolve an existing parent before lexical `..` reduction: a symlink
    // followed by `..` must retain the filesystem's actual path semantics.
    if let (Ok(base), Some(name)) = (parent(&path).canonicalize(), path.file_name()) {
        return Ok(base.join(name));
    }
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            other => normalized.push(other.as_os_str()),
        }
    }
    if let (Ok(base), Some(name)) = (parent(&normalized).canonicalize(), normalized.file_name()) {
        Ok(base.join(name))
    } else {
        Ok(normalized)
    }
}
pub(crate) fn relative(catalog: &Path, target: &Path) -> Result<String, SchemaCatalogError> {
    let base = absolute(parent(catalog))?;
    let target = absolute(target)?;
    let base = base.components().collect::<Vec<_>>();
    let target = target.components().collect::<Vec<_>>();
    let common = base.iter().zip(&target).take_while(|(a, b)| a == b).count();
    let mut result = PathBuf::new();
    for _ in common..base.len() {
        result.push("..");
    }
    for component in &target[common..] {
        result.push(component.as_os_str());
    }
    result
        .to_str()
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .ok_or(corrupt("catalog locators require nonempty UTF-8 paths"))
}
pub(crate) fn resolve(catalog: &Path, locator: &str) -> PathBuf {
    parent(catalog).join(locator)
}

fn write_link(
    storage: &Path,
    catalog: &Path,
    incarnation: [u8; 16],
) -> Result<(), SchemaCatalogError> {
    let path = link_path(storage);
    let mut w = Writer(incarnation.to_vec());
    w.string(&relative(&path, catalog)?)?;
    let bytes = envelope(b"NBSL", &w.0)?;
    if let Some(existing) = optional_read(&path)? {
        if existing == bytes {
            return Ok(());
        }
        return Err(SchemaCatalogError::PathConflict(path));
    }
    atomic_write(&path, &bytes, false)
}
pub(crate) fn discover(storage: &Path) -> Result<PathBuf, SchemaCatalogError> {
    let path = link_path(storage);
    let bytes = optional_read(&path)?.ok_or(SchemaCatalogError::LegacyCatalogRequired)?;
    let mut r = Reader(open_envelope(&bytes, b"NBSL")?);
    let incarnation: [u8; 16] = r
        .take(16)?
        .try_into()
        .map_err(|_| corrupt("link incarnation"))?;
    let locator = r.string()?;
    if !r.0.is_empty()
        || incarnation == [0; 16]
        || locator.as_bytes().contains(&0)
        || Path::new(&locator).is_absolute()
    {
        return Err(corrupt("invalid catalog link"));
    }
    let catalog = absolute(&resolve(&path, &locator))?;
    let marker = marker(&catalog)?
        .ok_or_else(|| corrupt("catalog discovery link has no installation marker"))?;
    if marker.incarnation != incarnation {
        return Err(corrupt("catalog link incarnation mismatch"));
    }
    Ok(catalog)
}

#[cfg(test)]
fn crash(point: &str) {
    if std::env::var("NETBADB_SCHEMA_CRASH_POINT").as_deref() == Ok(point) {
        std::process::exit(89);
    }
}
#[cfg(not(test))]
fn crash(_: &str) {}
