//! Database-level committed schema snapshots. Byte contract: docs/schema-catalog-v1.md.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;
use std::path::PathBuf;

use netbadb_schema::{ColumnDef, Schema, TableDef, TypeSpec};
use netbadb_types::{
    ColumnId, PartitionId, PhysicalType, SchemaGeneration, StorageId, TableId, TableSchemaVersion,
};

use crate::partition_catalog::PartitionCatalog;
use crate::registry::TablePlacement;

pub(crate) const MAX_BYTES: usize = 16 * 1024 * 1024;
pub(crate) const MAX_STRING: usize = 4096;
const MAX_TABLES: usize = 4096;
const MAX_COLUMNS: usize = 4096;
const MAX_TOTAL_COLUMNS: usize = 65536;
const MAX_STORAGES: usize = 65536;

#[derive(Debug)]
pub enum SchemaCatalogError {
    LegacyCatalogRequired,
    SchemaCatalogMissing,
    AlreadyInitialized,
    SchemaCatalogCorrupt(&'static str),
    UnsupportedVersion(u16),
    CapacityExceeded(&'static str),
    IncompleteLegacyInventory,
    InventoryMismatch(&'static str),
    ExpectationMissingTable(TableId),
    PathConflict(PathBuf),
    Randomness(getrandom::Error),
    Io {
        operation: &'static str,
        path: PathBuf,
        source: std::io::Error,
    },
}

impl fmt::Display for SchemaCatalogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LegacyCatalogRequired => {
                f.write_str("explicit complete legacy catalog bootstrap required")
            }
            Self::SchemaCatalogMissing => {
                f.write_str("initialized database schema catalog is missing")
            }
            Self::AlreadyInitialized => {
                f.write_str("database schema catalog is already initialized")
            }
            Self::SchemaCatalogCorrupt(reason) => write!(f, "schema catalog corrupt: {reason}"),
            Self::UnsupportedVersion(version) => {
                write!(f, "unsupported schema catalog version {version}")
            }
            Self::CapacityExceeded(field) => write!(f, "schema catalog capacity exceeded: {field}"),
            Self::IncompleteLegacyInventory => {
                f.write_str("legacy schema and complete physical inventory differ")
            }
            Self::InventoryMismatch(reason) => {
                write!(f, "schema catalog physical inventory mismatch: {reason}")
            }
            Self::ExpectationMissingTable(id) => {
                write!(f, "required table {} is absent from persisted schema", id.0)
            }
            Self::PathConflict(path) => write!(
                f,
                "schema catalog path conflicts with existing resource: {}",
                path.display()
            ),
            Self::Randomness(error) => write!(f, "cannot generate database incarnation: {error}"),
            Self::Io {
                operation,
                path,
                source,
            } => write!(f, "{operation} {}: {source}", path.display()),
        }
    }
}
impl Error for SchemaCatalogError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}

pub(crate) fn corrupt(reason: &'static str) -> SchemaCatalogError {
    SchemaCatalogError::SchemaCatalogCorrupt(reason)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TableLineage {
    pub(crate) table_id: TableId,
    pub(crate) version: TableSchemaVersion,
    // None is explicitly exhausted, never a wrapped successor.
    pub(crate) next_column_id: Option<ColumnId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommittedCatalogState {
    pub(crate) schema: Schema,
    pub(crate) generation: SchemaGeneration,
    pub(crate) next_table_id: Option<TableId>,
    pub(crate) next_storage_id: Option<StorageId>,
    pub(crate) next_partition_id: Option<PartitionId>,
    pub(crate) tables: Vec<TableLineage>,
}

impl CommittedCatalogState {
    pub(crate) fn initial(
        schema: Schema,
        placements: impl Iterator<Item = TablePlacement>,
    ) -> Self {
        let placements = placements.collect::<Vec<_>>();
        let next_storage_id = placements
            .iter()
            .flat_map(TablePlacement::storage_ids)
            .map(|id| id.0)
            .max()
            .unwrap_or(0)
            .checked_add(1)
            .map(StorageId);
        let next_partition_id = placements
            .iter()
            .filter_map(|p| match p {
                TablePlacement::RangePartitioned { partitions, .. } => Some(partitions),
                _ => None,
            })
            .flatten()
            .map(|p| p.partition_id.0)
            .max()
            .unwrap_or(0)
            .checked_add(1)
            .map(PartitionId);
        let tables = schema
            .tables()
            .iter()
            .map(|table| TableLineage {
                table_id: table.id,
                version: TableSchemaVersion(1),
                next_column_id: table
                    .columns
                    .iter()
                    .map(|c| c.id.0)
                    .max()
                    .unwrap_or(0)
                    .checked_add(1)
                    .map(ColumnId),
            })
            .collect();
        Self {
            next_table_id: schema
                .tables()
                .iter()
                .map(|t| t.id.0)
                .max()
                .unwrap_or(0)
                .checked_add(1)
                .map(TableId),
            schema,
            generation: SchemaGeneration(1),
            next_storage_id,
            next_partition_id,
            tables,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CatalogStorageKind {
    Heap,
    Lsm { clustering_column: ColumnId },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CatalogStorage {
    pub(crate) id: StorageId,
    pub(crate) table_id: TableId,
    pub(crate) locator: String,
    pub(crate) kind: CatalogStorageKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SchemaCatalogSnapshot {
    pub(crate) incarnation: [u8; 16],
    // Physical publication order, distinct from logical SchemaGeneration.
    pub(crate) epoch: u64,
    pub(crate) committed: CommittedCatalogState,
    pub(crate) placements: PartitionCatalog,
    pub(crate) storages: Vec<CatalogStorage>,
    pub(crate) coordinator: Option<String>,
    pub(crate) partition_evidence: Option<String>,
}

impl SchemaCatalogSnapshot {
    pub(crate) fn validate(&self) -> Result<(), SchemaCatalogError> {
        if self.incarnation == [0; 16] || self.epoch == 0 || self.committed.generation.0 == 0 {
            return Err(corrupt("zero incarnation, epoch or schema generation"));
        }
        let tables = self.committed.schema.tables();
        if tables.len() > MAX_TABLES || self.storages.len() > MAX_STORAGES {
            return Err(SchemaCatalogError::CapacityExceeded("inventory"));
        }
        let mut total_columns = 0_usize;
        for table in tables {
            total_columns = total_columns
                .checked_add(table.columns.len())
                .ok_or(corrupt("column count overflow"))?;
            if table.columns.len() > MAX_COLUMNS || total_columns > MAX_TOTAL_COLUMNS {
                return Err(SchemaCatalogError::CapacityExceeded("columns"));
            }
            check_string(&table.name)?;
            for column in &table.columns {
                check_string(&column.name)?;
                if let TypeSpec::Semantic { name, .. } = &column.type_spec {
                    check_string(name)?;
                }
            }
        }
        self.committed
            .schema
            .validate()
            .map_err(|_| corrupt("invalid logical schema"))?;
        if self.committed.tables.len() != tables.len()
            || self.placements.tables.len() != tables.len()
        {
            return Err(corrupt("table metadata count mismatch"));
        }
        // Empty databases have no legacy PartitionCatalog. Nonempty placements reuse its strict codec.
        if !tables.is_empty() {
            self.placements
                .validate()
                .map_err(|_| corrupt("invalid placements"))?;
        }
        let table_by_id = tables.iter().map(|t| (t.id, t)).collect::<BTreeMap<_, _>>();
        let storage_by_id = self
            .storages
            .iter()
            .map(|s| (s.id, s))
            .collect::<BTreeMap<_, _>>();
        let mut storage_ids = BTreeSet::new();
        let mut locators = BTreeSet::new();
        for storage in &self.storages {
            check_locator(&storage.locator)?;
            if storage.id.0 == 0
                || !storage_ids.insert(storage.id)
                || !locators.insert(&storage.locator)
            {
                return Err(corrupt("duplicate storage identity or locator"));
            }
            let table = table_by_id
                .get(&storage.table_id)
                .ok_or(corrupt("storage table absent"))?;
            if let CatalogStorageKind::Lsm { clustering_column } = storage.kind {
                let column = table
                    .column_by_id(clustering_column)
                    .ok_or(corrupt("LSM clustering column absent"))?;
                if column.nullable
                    || !matches!(
                        column.semantic_type().physical,
                        PhysicalType::Int64 | PhysicalType::UInt64
                    )
                {
                    return Err(corrupt("invalid LSM clustering column"));
                }
            }
        }
        for locator in self
            .coordinator
            .iter()
            .chain(self.partition_evidence.iter())
        {
            check_locator(locator)?;
            if !locators.insert(locator) {
                return Err(corrupt("metadata locator conflicts with storage"));
            }
        }
        if self.partition_evidence.is_some() && self.coordinator.is_none() {
            return Err(corrupt("partition evidence requires coordinator"));
        }
        let mut bound = BTreeSet::new();
        let mut max_partition = 0;
        for ((table, lineage), placement) in tables
            .iter()
            .zip(&self.committed.tables)
            .zip(&self.placements.tables)
        {
            if table.id.0 == 0
                || lineage.table_id != table.id
                || lineage.version.0 == 0
                || placement.table_id != table.id
                || placement.schema_fingerprint
                    != table
                        .fingerprint()
                        .map_err(|_| corrupt("invalid fingerprint input"))?
            {
                return Err(corrupt("table identity, version or fingerprint mismatch"));
            }
            validate_high_water(
                lineage.next_column_id.map(|id| u64::from(id.0)),
                table
                    .columns
                    .iter()
                    .map(|c| u64::from(c.id.0))
                    .max()
                    .unwrap_or(0),
            )?;
            for id in placement.placement.storage_ids() {
                if !bound.insert(id) {
                    return Err(corrupt("multiply bound storage"));
                }
                let descriptor = storage_by_id
                    .get(&id)
                    .ok_or(corrupt("placement storage absent"))?;
                if descriptor.table_id != table.id {
                    return Err(corrupt("placement logical identity mismatch"));
                }
            }
            if let TablePlacement::RangePartitioned {
                partition_key,
                key_type,
                partitions,
                ..
            } = &placement.placement
            {
                let column = table
                    .column_by_id(*partition_key)
                    .ok_or(corrupt("partition key absent"))?;
                if column.nullable
                    || column.semantic_type().physical != *key_type
                    || self.coordinator.is_none()
                {
                    return Err(corrupt("invalid partition key or coordinator"));
                }
                if crate::partition_catalog::canonicalize_partitions(*key_type, partitions.clone())
                    .map_err(|_| corrupt("invalid partition order"))?
                    != *partitions
                    || partitions.iter().skip(1).any(|p| p.lower.is_none())
                {
                    return Err(corrupt("noncanonical partition order"));
                }
                for partition in partitions {
                    max_partition = max_partition.max(partition.partition_id.0);
                    if !matches!(
                        storage_by_id.get(&partition.storage_id).map(|s| s.kind),
                        Some(CatalogStorageKind::Heap)
                    ) {
                        return Err(corrupt("partition engine must be Heap"));
                    }
                }
            }
        }
        if bound != storage_ids {
            return Err(corrupt("unbound physical storage"));
        }
        validate_high_water(
            self.committed.next_table_id.map(|id| id.0),
            tables.iter().map(|t| t.id.0).max().unwrap_or(0),
        )?;
        validate_high_water(
            self.committed.next_storage_id.map(|id| id.0),
            storage_ids.iter().map(|id| id.0).max().unwrap_or(0),
        )?;
        validate_high_water(
            self.committed.next_partition_id.map(|id| id.0),
            max_partition,
        )?;
        Ok(())
    }

    pub(crate) fn encode(&self) -> Result<Vec<u8>, SchemaCatalogError> {
        self.validate()?;
        let mut w = Writer(Vec::new());
        w.0.extend_from_slice(&self.incarnation);
        w.u64(self.epoch);
        w.u64(self.committed.generation.0);
        w.u64(self.committed.next_table_id.map_or(0, |id| id.0));
        w.u64(self.committed.next_storage_id.map_or(0, |id| id.0));
        w.u64(self.committed.next_partition_id.map_or(0, |id| id.0));
        w.u32(self.committed.tables.len() as u32);
        for (table, lineage) in self
            .committed
            .schema
            .tables()
            .iter()
            .zip(&self.committed.tables)
        {
            w.u64(table.id.0);
            w.u64(lineage.version.0);
            w.u32(lineage.next_column_id.map_or(0, |id| id.0));
            w.string(&table.name)?;
            w.0.extend_from_slice(
                table
                    .fingerprint()
                    .map_err(|_| corrupt("invalid schema"))?
                    .as_bytes(),
            );
            w.u32(table.columns.len() as u32);
            for column in &table.columns {
                w.u32(column.id.0);
                w.string(&column.name)?;
                let (physical, semantic) = match &column.type_spec {
                    TypeSpec::Physical(p) => (*p, None),
                    TypeSpec::Semantic { physical, name } => (*physical, Some(name.as_str())),
                };
                w.u8(match physical {
                    PhysicalType::Bool => 1,
                    PhysicalType::Int64 => 2,
                    PhysicalType::UInt64 => 3,
                    PhysicalType::Text => 4,
                });
                w.optional_string(semantic)?;
                w.u8(u8::from(column.nullable));
                w.u8(u8::from(column.primary_key));
            }
            if w.0.len() > MAX_BYTES {
                return Err(SchemaCatalogError::CapacityExceeded("snapshot bytes"));
            }
        }
        let placement_bytes = if self.placements.tables.is_empty() {
            Vec::new()
        } else {
            self.placements
                .encode()
                .map_err(|_| corrupt("invalid placements"))?
        };
        if w.0
            .len()
            .checked_add(placement_bytes.len())
            .and_then(|n| n.checked_add(4))
            .is_none_or(|n| n > MAX_BYTES - 16)
        {
            return Err(SchemaCatalogError::CapacityExceeded("placement bytes"));
        }
        w.u32(placement_bytes.len() as u32);
        w.0.extend_from_slice(&placement_bytes);
        w.u32(self.storages.len() as u32);
        for storage in &self.storages {
            w.u64(storage.id.0);
            w.u64(storage.table_id.0);
            w.string(&storage.locator)?;
            match storage.kind {
                CatalogStorageKind::Heap => w.u8(0),
                CatalogStorageKind::Lsm { clustering_column } => {
                    w.u8(1);
                    w.u32(clustering_column.0);
                }
            }
        }
        w.optional_string(self.coordinator.as_deref())?;
        w.optional_string(self.partition_evidence.as_deref())?;
        envelope(b"NBSC", &w.0)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, SchemaCatalogError> {
        let mut r = Reader(open_envelope(bytes, b"NBSC")?);
        let incarnation = r.take(16)?.try_into().map_err(|_| corrupt("incarnation"))?;
        let epoch = r.u64()?;
        let generation = SchemaGeneration(r.u64()?);
        let next_table_id = nonzero(r.u64()?).map(TableId);
        let next_storage_id = nonzero(r.u64()?).map(StorageId);
        let next_partition_id = nonzero(r.u64()?).map(PartitionId);
        let count = r.count(MAX_TABLES, 64)?;
        let mut tables = Vec::with_capacity(count);
        let mut lineages = Vec::with_capacity(count);
        let mut total_columns = 0_usize;
        for _ in 0..count {
            let id = TableId(r.u64()?);
            let version = TableSchemaVersion(r.u64()?);
            let next_column_id = nonzero(u64::from(r.u32()?)).map(|id| ColumnId(id as u32));
            let name = r.string()?;
            let fingerprint: [u8; 32] =
                r.take(32)?.try_into().map_err(|_| corrupt("fingerprint"))?;
            let count = r.count(MAX_COLUMNS, 12)?;
            total_columns = total_columns
                .checked_add(count)
                .ok_or(corrupt("column count overflow"))?;
            if total_columns > MAX_TOTAL_COLUMNS {
                return Err(SchemaCatalogError::CapacityExceeded("total columns"));
            }
            let mut columns = Vec::with_capacity(count);
            for _ in 0..count {
                let id = ColumnId(r.u32()?);
                let name = r.string()?;
                let physical = match r.u8()? {
                    1 => PhysicalType::Bool,
                    2 => PhysicalType::Int64,
                    3 => PhysicalType::UInt64,
                    4 => PhysicalType::Text,
                    _ => return Err(corrupt("unknown physical type")),
                };
                let type_spec = match r.optional_string()? {
                    None => TypeSpec::Physical(physical),
                    Some(name) => TypeSpec::Semantic { name, physical },
                };
                columns.push(ColumnDef {
                    id,
                    name,
                    type_spec,
                    nullable: r.boolean()?,
                    primary_key: r.boolean()?,
                });
            }
            let table = TableDef::new(id, name, columns);
            if table
                .fingerprint()
                .map_err(|_| corrupt("invalid table schema"))?
                .as_bytes()
                != &fingerprint
            {
                return Err(corrupt("logical fingerprint mismatch"));
            }
            tables.push(table);
            lineages.push(TableLineage {
                table_id: id,
                version,
                next_column_id,
            });
        }
        let length = r.u32()? as usize;
        let placement_bytes = r.take(length)?;
        let placements = if placement_bytes.is_empty() {
            PartitionCatalog { tables: Vec::new() }
        } else {
            PartitionCatalog::decode(placement_bytes)
                .map_err(|_| corrupt("invalid placement snapshot"))?
        };
        let count = r.count(MAX_STORAGES, 22)?;
        let mut storages = Vec::with_capacity(count);
        for _ in 0..count {
            let id = StorageId(r.u64()?);
            let table_id = TableId(r.u64()?);
            let locator = r.string()?;
            let kind = match r.u8()? {
                0 => CatalogStorageKind::Heap,
                1 => CatalogStorageKind::Lsm {
                    clustering_column: ColumnId(r.u32()?),
                },
                _ => return Err(corrupt("unknown storage kind")),
            };
            storages.push(CatalogStorage {
                id,
                table_id,
                locator,
                kind,
            });
        }
        let coordinator = r.optional_string()?;
        let partition_evidence = r.optional_string()?;
        if !r.0.is_empty() {
            return Err(corrupt("trailing snapshot bytes"));
        }
        let snapshot = Self {
            incarnation,
            epoch,
            committed: CommittedCatalogState {
                schema: Schema::new(tables).map_err(|_| corrupt("invalid schema"))?,
                generation,
                next_table_id,
                next_storage_id,
                next_partition_id,
                tables: lineages,
            },
            placements,
            storages,
            coordinator,
            partition_evidence,
        };
        snapshot.validate()?;
        Ok(snapshot)
    }
}

fn nonzero(value: u64) -> Option<u64> {
    if value == 0 { None } else { Some(value) }
}
fn validate_high_water(next: Option<u64>, maximum: u64) -> Result<(), SchemaCatalogError> {
    if next.is_some_and(|next| next == 0 || next <= maximum) {
        return Err(corrupt("invalid identity high-water"));
    }
    Ok(())
}
fn check_string(value: &str) -> Result<(), SchemaCatalogError> {
    if value.is_empty() {
        return Err(corrupt("empty string"));
    }
    if value.len() > MAX_STRING {
        return Err(SchemaCatalogError::CapacityExceeded("UTF-8 string"));
    }
    Ok(())
}
fn check_locator(value: &str) -> Result<(), SchemaCatalogError> {
    check_string(value)?;
    if value.contains('\0') || std::path::Path::new(value).is_absolute() {
        return Err(corrupt("locator must be a relative UTF-8 path"));
    }
    Ok(())
}

pub(crate) fn envelope(magic: &[u8; 4], payload: &[u8]) -> Result<Vec<u8>, SchemaCatalogError> {
    let length = payload
        .len()
        .checked_add(16)
        .ok_or(corrupt("file length overflow"))?;
    if length > MAX_BYTES {
        return Err(SchemaCatalogError::CapacityExceeded("file bytes"));
    }
    let mut bytes = Vec::with_capacity(length);
    bytes.extend_from_slice(magic);
    bytes.extend_from_slice(&1_u16.to_le_bytes());
    bytes.extend_from_slice(&0_u16.to_le_bytes());
    bytes.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&[0; 4]);
    bytes.extend_from_slice(payload);
    let crc = crc32c::crc32c_append(crc32c::crc32c(&bytes[..12]), &bytes[16..]);
    bytes[12..16].copy_from_slice(&crc.to_le_bytes());
    Ok(bytes)
}
pub(crate) fn open_envelope<'a>(
    bytes: &'a [u8],
    magic: &[u8; 4],
) -> Result<&'a [u8], SchemaCatalogError> {
    if bytes.len() < 16 || bytes.len() > MAX_BYTES {
        return Err(corrupt("file length out of bounds"));
    }
    if &bytes[..4] != magic {
        return Err(corrupt("invalid magic"));
    }
    let mut r = Reader(&bytes[4..]);
    let version = r.u16()?;
    if version != 1 {
        return Err(SchemaCatalogError::UnsupportedVersion(version));
    }
    if r.u16()? != 0 {
        return Err(corrupt("nonzero reserved header"));
    }
    if r.u32()? as usize != bytes.len() - 16 {
        return Err(corrupt("payload length mismatch"));
    }
    if r.u32()? != crc32c::crc32c_append(crc32c::crc32c(&bytes[..12]), &bytes[16..]) {
        return Err(corrupt("CRC32C mismatch"));
    }
    Ok(&bytes[16..])
}

pub(crate) struct Writer(pub(crate) Vec<u8>);
impl Writer {
    pub(crate) fn u8(&mut self, v: u8) {
        self.0.push(v);
    }
    pub(crate) fn u32(&mut self, v: u32) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    pub(crate) fn u64(&mut self, v: u64) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    pub(crate) fn string(&mut self, v: &str) -> Result<(), SchemaCatalogError> {
        check_string(v)?;
        if self
            .0
            .len()
            .checked_add(4)
            .and_then(|n| n.checked_add(v.len()))
            .is_none_or(|n| n > MAX_BYTES - 16)
        {
            return Err(SchemaCatalogError::CapacityExceeded("payload bytes"));
        }
        self.u32(v.len() as u32);
        self.0.extend_from_slice(v.as_bytes());
        Ok(())
    }
    fn optional_string(&mut self, v: Option<&str>) -> Result<(), SchemaCatalogError> {
        self.u8(u8::from(v.is_some()));
        if let Some(v) = v {
            self.string(v)?;
        }
        Ok(())
    }
}
pub(crate) struct Reader<'a>(pub(crate) &'a [u8]);
impl<'a> Reader<'a> {
    pub(crate) fn take(&mut self, count: usize) -> Result<&'a [u8], SchemaCatalogError> {
        if count > self.0.len() {
            return Err(corrupt("truncated payload"));
        }
        let (value, rest) = self.0.split_at(count);
        self.0 = rest;
        Ok(value)
    }
    pub(crate) fn u8(&mut self) -> Result<u8, SchemaCatalogError> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16, SchemaCatalogError> {
        Ok(u16::from_le_bytes(
            self.take(2)?.try_into().map_err(|_| corrupt("u16"))?,
        ))
    }
    pub(crate) fn u32(&mut self) -> Result<u32, SchemaCatalogError> {
        Ok(u32::from_le_bytes(
            self.take(4)?.try_into().map_err(|_| corrupt("u32"))?,
        ))
    }
    pub(crate) fn u64(&mut self) -> Result<u64, SchemaCatalogError> {
        Ok(u64::from_le_bytes(
            self.take(8)?.try_into().map_err(|_| corrupt("u64"))?,
        ))
    }
    pub(crate) fn string(&mut self) -> Result<String, SchemaCatalogError> {
        let length = self.u32()? as usize;
        if length == 0 || length > MAX_STRING {
            return Err(corrupt("string length out of bounds"));
        }
        Ok(std::str::from_utf8(self.take(length)?)
            .map_err(|_| corrupt("invalid UTF-8"))?
            .to_owned())
    }
    fn boolean(&mut self) -> Result<bool, SchemaCatalogError> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(corrupt("invalid boolean")),
        }
    }
    fn optional_string(&mut self) -> Result<Option<String>, SchemaCatalogError> {
        if self.boolean()? {
            Ok(Some(self.string()?))
        } else {
            Ok(None)
        }
    }
    pub(crate) fn count(
        &mut self,
        maximum: usize,
        min_bytes: usize,
    ) -> Result<usize, SchemaCatalogError> {
        let count = self.u32()? as usize;
        if count > maximum || count > self.0.len() / min_bytes {
            return Err(corrupt("count out of bounds"));
        }
        Ok(count)
    }
}
