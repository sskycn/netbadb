use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use netbadb_schema::{SchemaFingerprint, TableDef};
use netbadb_types::{ColumnId, PartitionId, PhysicalType, ScalarValue, StorageId, TableId};

use crate::registry::{RangePartitionBinding, TablePlacement};

const MAGIC: &[u8; 4] = b"NBPC";
const VERSION: u16 = 1;
const HEADER_LEN: usize = 16;
const MAX_FILE_BYTES: usize = 16 * 1024 * 1024;
const MAX_TABLES: usize = 4096;
const MAX_PARTITIONS: usize = 1_000_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangePartitionSpec {
    pub partition_id: PartitionId,
    pub path: PathBuf,
    pub lower: Option<ScalarValue>,
    pub upper: Option<ScalarValue>,
}

impl RangePartitionSpec {
    #[must_use]
    pub fn new(
        partition_id: PartitionId,
        path: impl Into<PathBuf>,
        lower: Option<ScalarValue>,
        upper: Option<ScalarValue>,
    ) -> Self {
        Self {
            partition_id,
            path: path.into(),
            lower,
            upper,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TablePlacementSpec {
    Single {
        path: PathBuf,
        table: TableDef,
    },
    RangePartitioned {
        table: TableDef,
        partition_key: ColumnId,
        partitions: Vec<RangePartitionSpec>,
    },
}

impl TablePlacementSpec {
    #[must_use]
    pub fn single(path: impl Into<PathBuf>, table: TableDef) -> Self {
        Self::Single {
            path: path.into(),
            table,
        }
    }

    #[must_use]
    pub fn range_partitioned(
        table: TableDef,
        partition_key: ColumnId,
        partitions: Vec<RangePartitionSpec>,
    ) -> Self {
        Self::RangePartitioned {
            table,
            partition_key,
            partitions,
        }
    }

    pub(crate) const fn table(&self) -> &TableDef {
        match self {
            Self::Single { table, .. } | Self::RangePartitioned { table, .. } => table,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionCatalogConfig {
    catalog_path: PathBuf,
    coordinator_log_path: PathBuf,
}

impl PartitionCatalogConfig {
    #[must_use]
    pub fn new(catalog_path: impl Into<PathBuf>, coordinator_log_path: impl Into<PathBuf>) -> Self {
        Self {
            catalog_path: catalog_path.into(),
            coordinator_log_path: coordinator_log_path.into(),
        }
    }

    #[must_use]
    pub fn catalog_path(&self) -> &Path {
        &self.catalog_path
    }

    #[must_use]
    pub fn coordinator_log_path(&self) -> &Path {
        &self.coordinator_log_path
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CatalogTable {
    pub(crate) table_id: TableId,
    pub(crate) schema_fingerprint: SchemaFingerprint,
    pub(crate) placement: TablePlacement,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PartitionCatalog {
    pub(crate) tables: Vec<CatalogTable>,
}

impl PartitionCatalog {
    pub(crate) fn create(path: &Path, tables: Vec<CatalogTable>) -> Result<Self, PartitionError> {
        let catalog = Self { tables };
        catalog.validate()?;
        let bytes = catalog.encode()?;
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|source| PartitionError::Io {
                operation: "create partition catalog",
                path: path.to_owned(),
                source,
            })?;
        file.write_all(&bytes)
            .map_err(|source| PartitionError::Io {
                operation: "write partition catalog",
                path: path.to_owned(),
                source,
            })?;
        file.sync_all().map_err(|source| PartitionError::Io {
            operation: "sync partition catalog",
            path: path.to_owned(),
            source,
        })?;
        Ok(catalog)
    }

    pub(crate) fn open(path: &Path) -> Result<Self, PartitionError> {
        let mut file = File::open(path).map_err(|source| PartitionError::Io {
            operation: "open partition catalog",
            path: path.to_owned(),
            source,
        })?;
        let length = file
            .metadata()
            .map_err(|source| PartitionError::Io {
                operation: "inspect partition catalog",
                path: path.to_owned(),
                source,
            })?
            .len();
        let length = usize::try_from(length).map_err(|_| PartitionError::CatalogTooLarge)?;
        if length > MAX_FILE_BYTES {
            return Err(PartitionError::CatalogTooLarge);
        }
        let mut bytes = Vec::with_capacity(length);
        file.read_to_end(&mut bytes)
            .map_err(|source| PartitionError::Io {
                operation: "read partition catalog",
                path: path.to_owned(),
                source,
            })?;
        Self::decode(&bytes)
    }

    fn encode(&self) -> Result<Vec<u8>, PartitionError> {
        let mut payload = Vec::new();
        push_u32(&mut payload, self.tables.len(), "table count")?;
        for entry in &self.tables {
            payload.extend_from_slice(&entry.table_id.0.to_le_bytes());
            payload.extend_from_slice(entry.schema_fingerprint.as_bytes());
            match &entry.placement {
                TablePlacement::Single { storage_id, .. } => {
                    payload.push(0);
                    payload.extend_from_slice(&storage_id.0.to_le_bytes());
                }
                TablePlacement::RangePartitioned {
                    partition_key,
                    key_type,
                    partitions,
                    ..
                } => {
                    payload.push(1);
                    payload.extend_from_slice(&partition_key.0.to_le_bytes());
                    payload.push(physical_type_tag(*key_type)?);
                    push_u32(&mut payload, partitions.len(), "partition count")?;
                    for partition in partitions {
                        payload.extend_from_slice(&partition.partition_id.0.to_le_bytes());
                        payload.extend_from_slice(&partition.storage_id.0.to_le_bytes());
                        encode_bound(&mut payload, &partition.lower, *key_type)?;
                        encode_bound(&mut payload, &partition.upper, *key_type)?;
                    }
                }
            }
        }
        let payload_len =
            u32::try_from(payload.len()).map_err(|_| PartitionError::CatalogTooLarge)?;
        let mut bytes = Vec::with_capacity(HEADER_LEN + payload.len());
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&VERSION.to_le_bytes());
        bytes.extend_from_slice(&0_u16.to_le_bytes());
        bytes.extend_from_slice(&payload_len.to_le_bytes());
        bytes.extend_from_slice(&crc32c::crc32c(&payload).to_le_bytes());
        bytes.extend_from_slice(&payload);
        if bytes.len() > MAX_FILE_BYTES {
            return Err(PartitionError::CatalogTooLarge);
        }
        Ok(bytes)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, PartitionError> {
        if bytes.len() < HEADER_LEN {
            return Err(PartitionError::CatalogCorrupt("truncated header"));
        }
        if &bytes[..4] != MAGIC {
            return Err(PartitionError::CatalogCorrupt("invalid magic"));
        }
        let version = u16::from_le_bytes([bytes[4], bytes[5]]);
        if version != VERSION {
            return Err(PartitionError::UnsupportedCatalogVersion(version));
        }
        if bytes[6] != 0 || bytes[7] != 0 {
            return Err(PartitionError::CatalogCorrupt("non-zero reserved bytes"));
        }
        let payload_len = u32::from_le_bytes(
            bytes[8..12]
                .try_into()
                .map_err(|_| PartitionError::CatalogCorrupt("truncated payload length"))?,
        ) as usize;
        let expected_len = HEADER_LEN
            .checked_add(payload_len)
            .ok_or(PartitionError::CatalogTooLarge)?;
        if expected_len != bytes.len() || expected_len > MAX_FILE_BYTES {
            return Err(PartitionError::CatalogCorrupt("invalid payload length"));
        }
        let checksum = u32::from_le_bytes(
            bytes[12..16]
                .try_into()
                .map_err(|_| PartitionError::CatalogCorrupt("truncated checksum"))?,
        );
        let payload = &bytes[HEADER_LEN..];
        if crc32c::crc32c(payload) != checksum {
            return Err(PartitionError::CatalogCorrupt("checksum mismatch"));
        }
        let mut decoder = Decoder::new(payload);
        let table_count = decoder.count("table count", MAX_TABLES)?;
        let mut tables = Vec::with_capacity(table_count);
        for _ in 0..table_count {
            let table_id = TableId(decoder.u64("table id")?);
            let fingerprint = SchemaFingerprint::from_bytes(decoder.array("schema fingerprint")?);
            let tag = decoder.u8("placement tag")?;
            let placement = match tag {
                0 => TablePlacement::Single {
                    table_id,
                    storage_id: StorageId(decoder.u64("storage id")?),
                },
                1 => {
                    let partition_key = ColumnId(decoder.u32("partition key")?);
                    let key_type = decode_physical_type(decoder.u8("partition key type")?)?;
                    let partition_count = decoder.count("partition count", MAX_PARTITIONS)?;
                    let mut partitions = Vec::with_capacity(partition_count);
                    for _ in 0..partition_count {
                        partitions.push(RangePartitionBinding {
                            partition_id: PartitionId(decoder.u64("partition id")?),
                            storage_id: StorageId(decoder.u64("partition storage id")?),
                            lower: decode_bound(&mut decoder, key_type)?,
                            upper: decode_bound(&mut decoder, key_type)?,
                        });
                    }
                    TablePlacement::RangePartitioned {
                        table_id,
                        partition_key,
                        key_type,
                        partitions,
                    }
                }
                _ => return Err(PartitionError::CatalogCorrupt("invalid placement tag")),
            };
            tables.push(CatalogTable {
                table_id,
                schema_fingerprint: fingerprint,
                placement,
            });
        }
        if !decoder.finished() {
            return Err(PartitionError::CatalogCorrupt("trailing payload bytes"));
        }
        let catalog = Self { tables };
        catalog.validate()?;
        Ok(catalog)
    }

    pub(crate) fn validate(&self) -> Result<(), PartitionError> {
        if self.tables.is_empty() {
            return Err(PartitionError::CatalogCorrupt("empty catalog"));
        }
        if self.tables.len() > MAX_TABLES {
            return Err(PartitionError::CatalogTooLarge);
        }
        let mut tables = BTreeSet::new();
        let mut storages = BTreeSet::new();
        let mut partitions = BTreeSet::new();
        for entry in &self.tables {
            if entry.table_id.0 == 0 || !tables.insert(entry.table_id) {
                return Err(PartitionError::CatalogCorrupt(
                    "invalid or duplicate table id",
                ));
            }
            if entry.placement.table_id() != entry.table_id {
                return Err(PartitionError::CatalogCorrupt("placement table mismatch"));
            }
            match &entry.placement {
                TablePlacement::Single { storage_id, .. } => {
                    validate_storage_id(*storage_id, &mut storages)?
                }
                TablePlacement::RangePartitioned {
                    partitions: entries,
                    key_type,
                    ..
                } => {
                    if entries.is_empty() || entries.len() > MAX_PARTITIONS {
                        return Err(PartitionError::InvalidPartitionRange);
                    }
                    if !matches!(key_type, PhysicalType::Int64 | PhysicalType::UInt64) {
                        return Err(PartitionError::UnsupportedPartitionKeyType(*key_type));
                    }
                    for (position, partition) in entries.iter().enumerate() {
                        if partition.partition_id.0 == 0
                            || !partitions.insert(partition.partition_id)
                        {
                            return Err(PartitionError::DuplicatePartitionId(
                                partition.partition_id,
                            ));
                        }
                        validate_storage_id(partition.storage_id, &mut storages)?;
                        validate_bound_type(&partition.lower, *key_type)?;
                        validate_bound_type(&partition.upper, *key_type)?;
                        if let (Some(lower), Some(upper)) = (&partition.lower, &partition.upper) {
                            if compare_partition_values(lower, upper)? != Ordering::Less {
                                return Err(PartitionError::InvalidPartitionRange);
                            }
                        }
                        if let Some(previous) =
                            position.checked_sub(1).and_then(|index| entries.get(index))
                        {
                            if let (Some(previous_upper), Some(lower)) =
                                (&previous.upper, &partition.lower)
                            {
                                if compare_partition_values(previous_upper, lower)?
                                    == Ordering::Greater
                                {
                                    return Err(PartitionError::OverlappingPartitions);
                                }
                            } else if previous.upper.is_none() {
                                return Err(PartitionError::OverlappingPartitions);
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

pub(crate) fn canonicalize_partitions(
    key_type: PhysicalType,
    mut partitions: Vec<RangePartitionBinding>,
) -> Result<Vec<RangePartitionBinding>, PartitionError> {
    for partition in &partitions {
        validate_bound_type(&partition.lower, key_type)?;
        validate_bound_type(&partition.upper, key_type)?;
    }
    let mut comparison_error = None;
    partitions.sort_by(|left, right| match (&left.lower, &right.lower) {
        (None, None) => left.partition_id.cmp(&right.partition_id),
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(left_bound), Some(right_bound)) => compare_partition_values(left_bound, right_bound)
            .unwrap_or_else(|error| {
                comparison_error = Some(error);
                Ordering::Equal
            })
            .then_with(|| left.partition_id.cmp(&right.partition_id)),
    });
    if let Some(error) = comparison_error {
        return Err(error);
    }
    Ok(partitions)
}

pub(crate) fn route_partition<'a>(
    partitions: &'a [RangePartitionBinding],
    value: &ScalarValue,
) -> Result<&'a RangePartitionBinding, PartitionError> {
    if matches!(value, ScalarValue::Null) {
        return Err(PartitionError::NoPartitionForValue(value.clone()));
    }
    for partition in partitions {
        let after_lower = match &partition.lower {
            None => true,
            Some(lower) => compare_partition_values(value, lower)? != Ordering::Less,
        };
        let before_upper = match &partition.upper {
            None => true,
            Some(upper) => compare_partition_values(value, upper)? == Ordering::Less,
        };
        if after_lower && before_upper {
            return Ok(partition);
        }
    }
    Err(PartitionError::NoPartitionForValue(value.clone()))
}

fn validate_storage_id(
    storage_id: StorageId,
    storages: &mut BTreeSet<StorageId>,
) -> Result<(), PartitionError> {
    if storage_id.0 == 0 || !storages.insert(storage_id) {
        return Err(PartitionError::DuplicatePartitionStorage(storage_id));
    }
    Ok(())
}

fn validate_bound_type(
    bound: &Option<ScalarValue>,
    key_type: PhysicalType,
) -> Result<(), PartitionError> {
    match (key_type, bound) {
        (_, None)
        | (PhysicalType::Int64, Some(ScalarValue::Int64(_)))
        | (PhysicalType::UInt64, Some(ScalarValue::UInt64(_))) => Ok(()),
        _ => Err(PartitionError::PartitionBoundTypeMismatch),
    }
}

fn compare_partition_values(
    left: &ScalarValue,
    right: &ScalarValue,
) -> Result<Ordering, PartitionError> {
    match (left, right) {
        (ScalarValue::Int64(left), ScalarValue::Int64(right)) => Ok(left.cmp(right)),
        (ScalarValue::UInt64(left), ScalarValue::UInt64(right)) => Ok(left.cmp(right)),
        _ => Err(PartitionError::PartitionBoundTypeMismatch),
    }
}

fn physical_type_tag(value: PhysicalType) -> Result<u8, PartitionError> {
    match value {
        PhysicalType::Int64 => Ok(1),
        PhysicalType::UInt64 => Ok(2),
        other => Err(PartitionError::UnsupportedPartitionKeyType(other)),
    }
}

fn decode_physical_type(tag: u8) -> Result<PhysicalType, PartitionError> {
    match tag {
        1 => Ok(PhysicalType::Int64),
        2 => Ok(PhysicalType::UInt64),
        _ => Err(PartitionError::CatalogCorrupt("invalid physical type")),
    }
}

fn encode_bound(
    bytes: &mut Vec<u8>,
    bound: &Option<ScalarValue>,
    key_type: PhysicalType,
) -> Result<(), PartitionError> {
    validate_bound_type(bound, key_type)?;
    match bound {
        None => bytes.push(0),
        Some(ScalarValue::Int64(value)) => {
            bytes.push(1);
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        Some(ScalarValue::UInt64(value)) => {
            bytes.push(1);
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        _ => return Err(PartitionError::PartitionBoundTypeMismatch),
    }
    Ok(())
}

fn decode_bound(
    decoder: &mut Decoder<'_>,
    key_type: PhysicalType,
) -> Result<Option<ScalarValue>, PartitionError> {
    match decoder.u8("bound tag")? {
        0 => Ok(None),
        1 => match key_type {
            PhysicalType::Int64 => Ok(Some(ScalarValue::Int64(decoder.i64("Int64 bound")?))),
            PhysicalType::UInt64 => Ok(Some(ScalarValue::UInt64(decoder.u64("UInt64 bound")?))),
            _ => Err(PartitionError::CatalogCorrupt("invalid bound type")),
        },
        _ => Err(PartitionError::CatalogCorrupt("invalid bound tag")),
    }
}

fn push_u32(bytes: &mut Vec<u8>, value: usize, field: &'static str) -> Result<(), PartitionError> {
    let value = u32::try_from(value).map_err(|_| PartitionError::CountOverflow(field))?;
    bytes.extend_from_slice(&value.to_le_bytes());
    Ok(())
}

struct Decoder<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Decoder<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }
    fn take(&mut self, length: usize, field: &'static str) -> Result<&'a [u8], PartitionError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(PartitionError::CatalogCorrupt("offset overflow"))?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or(PartitionError::TruncatedField(field))?;
        self.offset = end;
        Ok(value)
    }
    fn u8(&mut self, field: &'static str) -> Result<u8, PartitionError> {
        Ok(self.take(1, field)?[0])
    }
    fn u32(&mut self, field: &'static str) -> Result<u32, PartitionError> {
        Ok(u32::from_le_bytes(
            self.take(4, field)?
                .try_into()
                .map_err(|_| PartitionError::TruncatedField(field))?,
        ))
    }
    fn u64(&mut self, field: &'static str) -> Result<u64, PartitionError> {
        Ok(u64::from_le_bytes(
            self.take(8, field)?
                .try_into()
                .map_err(|_| PartitionError::TruncatedField(field))?,
        ))
    }
    fn i64(&mut self, field: &'static str) -> Result<i64, PartitionError> {
        Ok(i64::from_le_bytes(
            self.take(8, field)?
                .try_into()
                .map_err(|_| PartitionError::TruncatedField(field))?,
        ))
    }
    fn array<const N: usize>(&mut self, field: &'static str) -> Result<[u8; N], PartitionError> {
        self.take(N, field)?
            .try_into()
            .map_err(|_| PartitionError::TruncatedField(field))
    }
    fn count(&mut self, field: &'static str, maximum: usize) -> Result<usize, PartitionError> {
        let value =
            usize::try_from(self.u32(field)?).map_err(|_| PartitionError::CountOverflow(field))?;
        if value > maximum {
            return Err(PartitionError::CountTooLarge(field));
        }
        Ok(value)
    }
    const fn finished(&self) -> bool {
        self.offset == self.bytes.len()
    }
}

#[derive(Debug)]
pub enum PartitionError {
    PartitionKeyMissing {
        table_id: TableId,
        column_id: ColumnId,
    },
    UnsupportedPartitionKeyType(PhysicalType),
    NullablePartitionKey {
        table_id: TableId,
        column_id: ColumnId,
    },
    InvalidPartitionRange,
    OverlappingPartitions,
    DuplicatePartitionId(PartitionId),
    UnknownPartitionId(PartitionId),
    DuplicatePartitionStorage(StorageId),
    PartitionBoundTypeMismatch,
    NoPartitionForValue(ScalarValue),
    PartitionedIndexCreationNotSupported(TableId),
    CatalogCorrupt(&'static str),
    UnsupportedCatalogVersion(u16),
    CatalogTooLarge,
    CountOverflow(&'static str),
    CountTooLarge(&'static str),
    TruncatedField(&'static str),
    SchemaFingerprintMismatch {
        table_id: TableId,
    },
    PartitionStorageMissing(StorageId),
    PartitionStorageMismatch(StorageId),
    Io {
        operation: &'static str,
        path: PathBuf,
        source: std::io::Error,
    },
}

impl fmt::Display for PartitionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PartitionKeyMissing {
                table_id,
                column_id,
            } => write!(
                f,
                "partition key column {} is missing from table {}",
                column_id.0, table_id.0
            ),
            Self::UnsupportedPartitionKeyType(value) => {
                write!(f, "{value} is not supported as a range partition key")
            }
            Self::NullablePartitionKey {
                table_id,
                column_id,
            } => write!(
                f,
                "partition key column {} on table {} must be NOT NULL",
                column_id.0, table_id.0
            ),
            Self::InvalidPartitionRange => {
                f.write_str("range partition must be a non-empty half-open interval")
            }
            Self::OverlappingPartitions => f.write_str("range partitions overlap"),
            Self::DuplicatePartitionId(value) => {
                write!(f, "partition identity {} is duplicated", value.0)
            }
            Self::UnknownPartitionId(value) => write!(
                f,
                "partition identity {} is not present in the table placement",
                value.0
            ),
            Self::DuplicatePartitionStorage(value) => {
                write!(f, "storage identity {} is assigned more than once", value.0)
            }
            Self::PartitionBoundTypeMismatch => {
                f.write_str("partition bound does not match the partition key type")
            }
            Self::NoPartitionForValue(value) => {
                write!(f, "no range partition contains value {value:?}")
            }
            Self::PartitionedIndexCreationNotSupported(table_id) => write!(
                f,
                "logical index creation for partitioned table {} is not supported; create equivalent local indexes before opening",
                table_id.0
            ),
            Self::CatalogCorrupt(reason) => write!(f, "partition catalog is corrupt: {reason}"),
            Self::UnsupportedCatalogVersion(version) => {
                write!(f, "partition catalog version {version} is unsupported")
            }
            Self::CatalogTooLarge => f.write_str("partition catalog exceeds its bounded size"),
            Self::CountOverflow(field) => write!(f, "partition catalog {field} does not fit u32"),
            Self::CountTooLarge(field) => write!(f, "partition catalog {field} exceeds its limit"),
            Self::TruncatedField(field) => {
                write!(f, "partition catalog is truncated while reading {field}")
            }
            Self::SchemaFingerprintMismatch { table_id } => write!(
                f,
                "partition catalog schema fingerprint does not match table {}",
                table_id.0
            ),
            Self::PartitionStorageMissing(storage_id) => {
                write!(f, "partition storage {} is missing", storage_id.0)
            }
            Self::PartitionStorageMismatch(storage_id) => write!(
                f,
                "partition storage {} does not match its durable catalog entry",
                storage_id.0
            ),
            Self::Io {
                operation,
                path,
                source,
            } => write!(f, "failed to {operation} `{}`: {source}", path.display()),
        }
    }
}

impl Error for PartitionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use netbadb_schema::{ColumnDef, TypeSpec};

    fn table() -> TableDef {
        TableDef::new(
            TableId(7),
            "events",
            vec![ColumnDef::new(
                ColumnId(3),
                "key",
                TypeSpec::Physical(PhysicalType::Int64),
            )],
        )
    }

    fn catalog() -> PartitionCatalog {
        let table = table();
        PartitionCatalog {
            tables: vec![CatalogTable {
                table_id: table.id,
                schema_fingerprint: table.fingerprint().expect("fingerprint"),
                placement: TablePlacement::RangePartitioned {
                    table_id: table.id,
                    partition_key: ColumnId(3),
                    key_type: PhysicalType::Int64,
                    partitions: vec![
                        RangePartitionBinding {
                            partition_id: PartitionId(10),
                            storage_id: StorageId(20),
                            lower: None,
                            upper: Some(ScalarValue::Int64(0)),
                        },
                        RangePartitionBinding {
                            partition_id: PartitionId(11),
                            storage_id: StorageId(21),
                            lower: Some(ScalarValue::Int64(0)),
                            upper: None,
                        },
                    ],
                },
            }],
        }
    }

    #[test]
    fn catalog_v1_round_trips_and_routes_half_open_boundaries() {
        let expected = catalog();
        let encoded = expected.encode().expect("encode catalog");
        let decoded = PartitionCatalog::decode(&encoded).expect("decode catalog");
        assert_eq!(decoded, expected);
        let TablePlacement::RangePartitioned { partitions, .. } = &decoded.tables[0].placement
        else {
            panic!("expected range placement");
        };
        assert_eq!(
            route_partition(partitions, &ScalarValue::Int64(-1))
                .expect("negative")
                .partition_id,
            PartitionId(10)
        );
        assert_eq!(
            route_partition(partitions, &ScalarValue::Int64(0))
                .expect("boundary")
                .partition_id,
            PartitionId(11)
        );
    }

    #[test]
    fn catalog_decoder_hard_rejects_header_checksum_truncation_and_counts() {
        let encoded = catalog().encode().expect("encode catalog");
        let mut bad_magic = encoded.clone();
        bad_magic[0] ^= 0xff;
        assert!(matches!(
            PartitionCatalog::decode(&bad_magic),
            Err(PartitionError::CatalogCorrupt("invalid magic"))
        ));
        let mut version = encoded.clone();
        version[4..6].copy_from_slice(&2_u16.to_le_bytes());
        assert!(matches!(
            PartitionCatalog::decode(&version),
            Err(PartitionError::UnsupportedCatalogVersion(2))
        ));
        let mut checksum = encoded.clone();
        let last = checksum.len() - 1;
        checksum[last] ^= 1;
        assert!(matches!(
            PartitionCatalog::decode(&checksum),
            Err(PartitionError::CatalogCorrupt("checksum mismatch"))
        ));
        for length in 0..encoded.len() {
            assert!(PartitionCatalog::decode(&encoded[..length]).is_err());
        }

        let mut payload = Vec::new();
        payload.extend_from_slice(&u32::MAX.to_le_bytes());
        let mut oversized = Vec::new();
        oversized.extend_from_slice(MAGIC);
        oversized.extend_from_slice(&VERSION.to_le_bytes());
        oversized.extend_from_slice(&0_u16.to_le_bytes());
        oversized.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        oversized.extend_from_slice(&crc32c::crc32c(&payload).to_le_bytes());
        oversized.extend_from_slice(&payload);
        assert!(matches!(
            PartitionCatalog::decode(&oversized),
            Err(PartitionError::CountTooLarge("table count"))
        ));
    }

    #[test]
    fn range_validation_rejects_wrong_types_empty_overlap_and_duplicate_identities() {
        let mut invalid = catalog();
        let TablePlacement::RangePartitioned { partitions, .. } = &mut invalid.tables[0].placement
        else {
            panic!("expected range placement");
        };
        partitions[0].lower = Some(ScalarValue::Int64(0));
        assert!(matches!(
            invalid.validate(),
            Err(PartitionError::InvalidPartitionRange)
        ));

        let mut overlap = catalog();
        let TablePlacement::RangePartitioned { partitions, .. } = &mut overlap.tables[0].placement
        else {
            panic!("expected range placement");
        };
        partitions[0].upper = Some(ScalarValue::Int64(10));
        assert!(matches!(
            overlap.validate(),
            Err(PartitionError::OverlappingPartitions)
        ));

        let mut duplicate = catalog();
        let TablePlacement::RangePartitioned { partitions, .. } =
            &mut duplicate.tables[0].placement
        else {
            panic!("expected range placement");
        };
        partitions[1].partition_id = partitions[0].partition_id;
        assert!(matches!(
            duplicate.validate(),
            Err(PartitionError::DuplicatePartitionId(_))
        ));

        let mut wrong_type = catalog();
        let TablePlacement::RangePartitioned { partitions, .. } =
            &mut wrong_type.tables[0].placement
        else {
            panic!("expected range placement");
        };
        partitions[0].upper = Some(ScalarValue::UInt64(0));
        assert!(matches!(
            wrong_type.validate(),
            Err(PartitionError::PartitionBoundTypeMismatch)
        ));
    }
}
