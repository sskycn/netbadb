//! Shared, language-independent identifiers and scalar types.

use std::cmp::Ordering;
use std::fmt;

pub const MAX_INDEX_NAME_BYTES: usize = 255;

macro_rules! id_type {
    ($name:ident, $inner:ty) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(pub $inner);
    };
}

id_type!(DatabaseId, u64);
id_type!(TableId, u64);
/// Durable order of committed logical table/column schema changes. Zero is uninitialized.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SchemaGeneration(pub u64);
/// Durable logical version of one table, independent of its content fingerprint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TableSchemaVersion(pub u64);
/// Stable identity of one logical physical partition.
///
/// A partition belongs to one logical [`TableId`] and resolves to a physical
/// [`StorageId`]. It is neither a vector position nor a storage identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PartitionId(pub u64);
/// Physical storage identity within one opened database composition.
///
/// This is distinct from logical [`TableId`], is deterministically assigned
/// at creation, and is persisted by every physical storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StorageId(pub u64);
/// Stable identity of one derived, read-only analytical projection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ColumnarProjectionId(pub u64);
/// Stable identity of one immutable columnar segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ColumnarSegmentId(pub u64);
/// Monotonic publication generation within one columnar projection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ColumnarGeneration(pub u64);
/// Database-coordinator transaction identity within one opened database.
///
/// Heap WAL transactions retain their independent [`TxnId`] identities.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DatabaseTxnId(pub u64);
/// Durable, database-scoped order in which committed states become visible.
///
/// `G0` is the bootstrap baseline. Every published write uses a non-zero value;
/// this identity is distinct from both [`DatabaseTxnId`] and storage-local
/// commit sequences.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DatabaseCommitSeq(pub u64);
/// Query-local identity for one occurrence of a relation in a FROM tree.
/// Unlike [`TableId`], this identifier is never persisted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RelationBindingId(pub u32);
/// Zero-based identity of one parameter slot in a prepared statement.
///
/// This identity is frontend-neutral and query-local. It is never persisted
/// and carries no wire-protocol type identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ParameterId(pub u32);
id_type!(ColumnId, u32);
id_type!(IndexId, u64);

/// Stable logical name of a registered secondary index.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct IndexName(String);

impl IndexName {
    pub fn new(name: impl Into<String>) -> Result<Self, IndexNameError> {
        let name = name.into();
        if name.is_empty() {
            return Err(IndexNameError::Empty);
        }
        if name.len() > MAX_INDEX_NAME_BYTES {
            return Err(IndexNameError::TooLong {
                length: name.len(),
                maximum: MAX_INDEX_NAME_BYTES,
            });
        }
        Ok(Self(name))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for IndexName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexNameError {
    Empty,
    TooLong { length: usize, maximum: usize },
}

impl fmt::Display for IndexNameError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => formatter.write_str("index name must not be empty"),
            Self::TooLong { length, maximum } => write!(
                formatter,
                "index name is {length} bytes; maximum is {maximum} bytes"
            ),
        }
    }
}

impl std::error::Error for IndexNameError {}
/// Table-scoped opaque identity for one executable physical access path.
///
/// The planner may compare and copy this value but must not infer a B+Tree
/// page, LSM structure, or columnar segment from its numeric representation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct AccessPathId(pub u64);
/// Physical slot number in one file; not an allocation identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PageId(pub u64);
/// Identity of one allocation incarnation. Zero is reserved and rejected by codecs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PageGeneration(pub u64);
/// Generation-safe reference within one storage file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PageRef {
    pub page_id: PageId,
    pub generation: PageGeneration,
}

id_type!(FrameId, u32);
id_type!(TxnId, u64);
id_type!(Lsn, u64);
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
/// Stable logical row identity inside one LSM physical storage.
///
/// Zero is reserved. Allocators are monotonic, durable, and never reuse an
/// identity after deletion or compaction.
pub struct LsmRowId(pub u64);
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
/// Storage-local committed-version order for one LSM physical storage.
///
/// This is deliberately not a database-global timestamp domain.
pub struct LsmCommitSeq(pub u64);
/// Durable logical row-state frontier within one physical storage.
/// Values are meaningful only together with the owning `StorageId`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StorageDataVersion(pub u64);
/// Incarnation of one explicitly enabled per-storage change stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ChangeStreamGeneration(pub u64);
// Monotonic MVCC commit order, currently derived from a durable Commit LSN.
id_type!(CommitSeq, u64);
// Transaction-local statement order used for own-write visibility.
id_type!(CommandId, u32);

/// A stable, explicitly sized slot identifier inside a database page.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SlotId(pub u16);

/// A versioned physical locator for one occupant of one heap slot.
///
/// Generation zero is reserved and is never issued for a live row. Reusing a
/// deleted slot increments its generation so an older locator cannot alias the
/// new occupant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RowId {
    pub page: PageId,
    pub slot: u16,
    pub generation: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PhysicalType {
    Bool,
    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    UInt128,
    Float32,
    Float64,
    Text,
    Bytes,
}

impl PhysicalType {
    #[must_use]
    pub const fn is_integer(self) -> bool {
        self.is_signed_integer() || self.is_unsigned_integer()
    }

    #[must_use]
    pub const fn is_signed_integer(self) -> bool {
        matches!(
            self,
            Self::Int8 | Self::Int16 | Self::Int32 | Self::Int64 | Self::Int128
        )
    }

    #[must_use]
    pub const fn is_unsigned_integer(self) -> bool {
        matches!(
            self,
            Self::UInt8 | Self::UInt16 | Self::UInt32 | Self::UInt64 | Self::UInt128
        )
    }

    #[must_use]
    pub const fn is_float(self) -> bool {
        matches!(self, Self::Float32 | Self::Float64)
    }

    #[must_use]
    pub const fn is_numeric(self) -> bool {
        self.is_integer() || self.is_float()
    }

    /// Every foundational scalar has a deterministic database total order.
    #[must_use]
    pub const fn is_orderable(self) -> bool {
        true
    }

    #[must_use]
    pub const fn fixed_width(self) -> Option<usize> {
        match self {
            Self::Bool | Self::Int8 | Self::UInt8 => Some(1),
            Self::Int16 | Self::UInt16 => Some(2),
            Self::Int32 | Self::UInt32 | Self::Float32 => Some(4),
            Self::Int64 | Self::UInt64 | Self::Float64 => Some(8),
            Self::Int128 | Self::UInt128 => Some(16),
            Self::Text | Self::Bytes => None,
        }
    }
}

impl fmt::Display for PhysicalType {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Bool => "BOOL",
            Self::Int8 => "INT8",
            Self::Int16 => "INT16",
            Self::Int32 => "INT32",
            Self::Int64 => "INT64",
            Self::Int128 => "INT128",
            Self::UInt8 => "UINT8",
            Self::UInt16 => "UINT16",
            Self::UInt32 => "UINT32",
            Self::UInt64 => "UINT64",
            Self::UInt128 => "UINT128",
            Self::Float32 => "FLOAT32",
            Self::Float64 => "FLOAT64",
            Self::Text => "TEXT",
            Self::Bytes => "BYTES",
        };
        formatter.write_str(name)
    }
}

/// A physical representation plus an optional nominal application meaning.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SemanticType {
    pub physical: PhysicalType,
    pub name: Option<String>,
}

impl SemanticType {
    #[must_use]
    pub fn physical(physical: PhysicalType) -> Self {
        Self {
            physical,
            name: None,
        }
    }

    #[must_use]
    pub fn named(name: impl Into<String>, physical: PhysicalType) -> Self {
        Self {
            physical,
            name: Some(name.into()),
        }
    }

    /// Nominal types only compare equal to the same nominal type.
    #[must_use]
    pub fn is_compatible_with(&self, other: &Self) -> bool {
        self.physical == other.physical
            && match (&self.name, &other.name) {
                (Some(left), Some(right)) => left == right,
                (None, None) => true,
                _ => false,
            }
    }
}

impl fmt::Display for SemanticType {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.name {
            Some(name) => formatter.write_str(name),
            None => self.physical.fmt(formatter),
        }
    }
}

/// A resolved expression's semantic type and whether it may evaluate to NULL.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ExprType {
    pub data_type: SemanticType,
    pub nullable: bool,
}

/// Canonical database `f32`: signed zero is positive zero and every NaN uses
/// one quiet-NaN payload. Equality, hashing and ordering therefore agree.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Float32Value(u32);

impl Float32Value {
    pub const CANONICAL_NAN_BITS: u32 = 0x7fc0_0000;

    #[must_use]
    pub fn new(value: f32) -> Self {
        Self::from_bits(value.to_bits())
    }

    #[must_use]
    pub const fn from_bits(bits: u32) -> Self {
        let magnitude = bits & 0x7fff_ffff;
        if magnitude == 0 {
            Self(0)
        } else if magnitude > 0x7f80_0000 {
            Self(Self::CANONICAL_NAN_BITS)
        } else {
            Self(bits)
        }
    }

    #[must_use]
    pub const fn to_bits(self) -> u32 {
        self.0
    }

    #[must_use]
    pub fn get(self) -> f32 {
        f32::from_bits(self.0)
    }
}

impl fmt::Debug for Float32Value {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.get().fmt(formatter)
    }
}

impl fmt::Display for Float32Value {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.get().fmt(formatter)
    }
}

impl PartialOrd for Float32Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Float32Value {
    fn cmp(&self, other: &Self) -> Ordering {
        self.get().total_cmp(&other.get())
    }
}

impl From<f32> for Float32Value {
    fn from(value: f32) -> Self {
        Self::new(value)
    }
}

impl From<Float32Value> for f32 {
    fn from(value: Float32Value) -> Self {
        value.get()
    }
}

/// Canonical database `f64`; see [`Float32Value`] for the invariants.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Float64Value(u64);

impl Float64Value {
    pub const CANONICAL_NAN_BITS: u64 = 0x7ff8_0000_0000_0000;

    #[must_use]
    pub fn new(value: f64) -> Self {
        Self::from_bits(value.to_bits())
    }

    #[must_use]
    pub const fn from_bits(bits: u64) -> Self {
        let magnitude = bits & 0x7fff_ffff_ffff_ffff;
        if magnitude == 0 {
            Self(0)
        } else if magnitude > 0x7ff0_0000_0000_0000 {
            Self(Self::CANONICAL_NAN_BITS)
        } else {
            Self(bits)
        }
    }

    #[must_use]
    pub const fn to_bits(self) -> u64 {
        self.0
    }

    #[must_use]
    pub fn get(self) -> f64 {
        f64::from_bits(self.0)
    }
}

impl fmt::Debug for Float64Value {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.get().fmt(formatter)
    }
}

impl fmt::Display for Float64Value {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.get().fmt(formatter)
    }
}

impl PartialOrd for Float64Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Float64Value {
    fn cmp(&self, other: &Self) -> Ordering {
        self.get().total_cmp(&other.get())
    }
}

impl From<f64> for Float64Value {
    fn from(value: f64) -> Self {
        Self::new(value)
    }
}

impl From<Float64Value> for f64 {
    fn from(value: Float64Value) -> Self {
        value.get()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ScalarValue {
    Bool(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Int128(i128),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    UInt128(u128),
    Float32(Float32Value),
    Float64(Float64Value),
    Text(String),
    Bytes(Vec<u8>),
    Null,
}

/// A borrowed runtime view of a scalar value.
///
/// This type carries no persistence, wire, schema, or SQL-expression
/// semantics. In particular, [`ScalarRef::Text`] may borrow short-lived
/// validated storage bytes and must remain scoped to their owning operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ScalarRef<'a> {
    Bool(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Int128(i128),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    UInt128(u128),
    Float32(Float32Value),
    Float64(Float64Value),
    Text(&'a str),
    Bytes(&'a [u8]),
    Null,
}

impl ScalarRef<'_> {
    #[must_use]
    pub const fn physical_type(self) -> Option<PhysicalType> {
        match self {
            Self::Bool(_) => Some(PhysicalType::Bool),
            Self::Int8(_) => Some(PhysicalType::Int8),
            Self::Int16(_) => Some(PhysicalType::Int16),
            Self::Int32(_) => Some(PhysicalType::Int32),
            Self::Int64(_) => Some(PhysicalType::Int64),
            Self::Int128(_) => Some(PhysicalType::Int128),
            Self::UInt8(_) => Some(PhysicalType::UInt8),
            Self::UInt16(_) => Some(PhysicalType::UInt16),
            Self::UInt32(_) => Some(PhysicalType::UInt32),
            Self::UInt64(_) => Some(PhysicalType::UInt64),
            Self::UInt128(_) => Some(PhysicalType::UInt128),
            Self::Float32(_) => Some(PhysicalType::Float32),
            Self::Float64(_) => Some(PhysicalType::Float64),
            Self::Text(_) => Some(PhysicalType::Text),
            Self::Bytes(_) => Some(PhysicalType::Bytes),
            Self::Null => None,
        }
    }

    #[must_use]
    pub fn matches_type(self, expected: &SemanticType) -> bool {
        match self.physical_type() {
            Some(actual) => actual == expected.physical,
            None => true,
        }
    }

    #[must_use]
    pub const fn is_null(self) -> bool {
        matches!(self, Self::Null)
    }

    /// Compares values using the database's single scalar order. NULL sorts
    /// first for index/statistics purposes; different physical types are not
    /// comparable and return `None`.
    #[must_use]
    pub fn database_cmp(self, other: Self) -> Option<Ordering> {
        match (self, other) {
            (Self::Null, Self::Null) => Some(Ordering::Equal),
            (Self::Null, _) => Some(Ordering::Less),
            (_, Self::Null) => Some(Ordering::Greater),
            (Self::Bool(left), Self::Bool(right)) => Some(left.cmp(&right)),
            (Self::Int8(left), Self::Int8(right)) => Some(left.cmp(&right)),
            (Self::Int16(left), Self::Int16(right)) => Some(left.cmp(&right)),
            (Self::Int32(left), Self::Int32(right)) => Some(left.cmp(&right)),
            (Self::Int64(left), Self::Int64(right)) => Some(left.cmp(&right)),
            (Self::Int128(left), Self::Int128(right)) => Some(left.cmp(&right)),
            (Self::UInt8(left), Self::UInt8(right)) => Some(left.cmp(&right)),
            (Self::UInt16(left), Self::UInt16(right)) => Some(left.cmp(&right)),
            (Self::UInt32(left), Self::UInt32(right)) => Some(left.cmp(&right)),
            (Self::UInt64(left), Self::UInt64(right)) => Some(left.cmp(&right)),
            (Self::UInt128(left), Self::UInt128(right)) => Some(left.cmp(&right)),
            (Self::Float32(left), Self::Float32(right)) => Some(left.cmp(&right)),
            (Self::Float64(left), Self::Float64(right)) => Some(left.cmp(&right)),
            (Self::Text(left), Self::Text(right)) => Some(left.cmp(right)),
            (Self::Bytes(left), Self::Bytes(right)) => Some(left.cmp(right)),
            _ => None,
        }
    }

    #[must_use]
    pub fn to_owned(self) -> ScalarValue {
        match self {
            Self::Bool(value) => ScalarValue::Bool(value),
            Self::Int8(value) => ScalarValue::Int8(value),
            Self::Int16(value) => ScalarValue::Int16(value),
            Self::Int32(value) => ScalarValue::Int32(value),
            Self::Int64(value) => ScalarValue::Int64(value),
            Self::Int128(value) => ScalarValue::Int128(value),
            Self::UInt8(value) => ScalarValue::UInt8(value),
            Self::UInt16(value) => ScalarValue::UInt16(value),
            Self::UInt32(value) => ScalarValue::UInt32(value),
            Self::UInt64(value) => ScalarValue::UInt64(value),
            Self::UInt128(value) => ScalarValue::UInt128(value),
            Self::Float32(value) => ScalarValue::Float32(value),
            Self::Float64(value) => ScalarValue::Float64(value),
            Self::Text(value) => ScalarValue::Text(value.to_owned()),
            Self::Bytes(value) => ScalarValue::Bytes(value.to_owned()),
            Self::Null => ScalarValue::Null,
        }
    }
}

impl<'a> From<&'a ScalarValue> for ScalarRef<'a> {
    fn from(value: &'a ScalarValue) -> Self {
        match value {
            ScalarValue::Bool(value) => Self::Bool(*value),
            ScalarValue::Int8(value) => Self::Int8(*value),
            ScalarValue::Int16(value) => Self::Int16(*value),
            ScalarValue::Int32(value) => Self::Int32(*value),
            ScalarValue::Int64(value) => Self::Int64(*value),
            ScalarValue::Int128(value) => Self::Int128(*value),
            ScalarValue::UInt8(value) => Self::UInt8(*value),
            ScalarValue::UInt16(value) => Self::UInt16(*value),
            ScalarValue::UInt32(value) => Self::UInt32(*value),
            ScalarValue::UInt64(value) => Self::UInt64(*value),
            ScalarValue::UInt128(value) => Self::UInt128(*value),
            ScalarValue::Float32(value) => Self::Float32(*value),
            ScalarValue::Float64(value) => Self::Float64(*value),
            ScalarValue::Text(value) => Self::Text(value.as_str()),
            ScalarValue::Bytes(value) => Self::Bytes(value.as_slice()),
            ScalarValue::Null => Self::Null,
        }
    }
}

impl ScalarValue {
    #[must_use]
    pub fn physical_type(&self) -> Option<PhysicalType> {
        match self {
            Self::Bool(_) => Some(PhysicalType::Bool),
            Self::Int8(_) => Some(PhysicalType::Int8),
            Self::Int16(_) => Some(PhysicalType::Int16),
            Self::Int32(_) => Some(PhysicalType::Int32),
            Self::Int64(_) => Some(PhysicalType::Int64),
            Self::Int128(_) => Some(PhysicalType::Int128),
            Self::UInt8(_) => Some(PhysicalType::UInt8),
            Self::UInt16(_) => Some(PhysicalType::UInt16),
            Self::UInt32(_) => Some(PhysicalType::UInt32),
            Self::UInt64(_) => Some(PhysicalType::UInt64),
            Self::UInt128(_) => Some(PhysicalType::UInt128),
            Self::Float32(_) => Some(PhysicalType::Float32),
            Self::Float64(_) => Some(PhysicalType::Float64),
            Self::Text(_) => Some(PhysicalType::Text),
            Self::Bytes(_) => Some(PhysicalType::Bytes),
            Self::Null => None,
        }
    }

    #[must_use]
    pub fn matches_type(&self, expected: &SemanticType) -> bool {
        match self.physical_type() {
            Some(actual) => actual == expected.physical,
            None => true,
        }
    }

    /// Owned counterpart of [`ScalarRef::database_cmp`].
    #[must_use]
    pub fn database_cmp(&self, other: &Self) -> Option<Ordering> {
        ScalarRef::from(self).database_cmp(ScalarRef::from(other))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    use super::{
        Float32Value, Float64Value, PageId, PhysicalType, RowId, ScalarRef, ScalarValue,
        SemanticType,
    };

    fn scalar_cases() -> Vec<ScalarValue> {
        vec![
            ScalarValue::Bool(true),
            ScalarValue::Int8(i8::MIN),
            ScalarValue::Int16(i16::MIN),
            ScalarValue::Int32(i32::MIN),
            ScalarValue::Int64(i64::MIN),
            ScalarValue::Int128(i128::MIN),
            ScalarValue::UInt8(u8::MAX),
            ScalarValue::UInt16(u16::MAX),
            ScalarValue::UInt32(u32::MAX),
            ScalarValue::UInt64(u64::MAX),
            ScalarValue::UInt128(u128::MAX),
            ScalarValue::Float32(Float32Value::new(1.5)),
            ScalarValue::Float64(Float64Value::new(-2.5)),
            ScalarValue::Text("payload".into()),
            ScalarValue::Bytes(vec![0, 0xff, 1]),
            ScalarValue::Null,
        ]
    }

    #[test]
    fn nominal_types_do_not_collapse_to_their_physical_type() {
        let user_id = SemanticType::named("UserId", PhysicalType::UInt64);
        let team_id = SemanticType::named("TeamId", PhysicalType::UInt64);
        let raw_id = SemanticType::physical(PhysicalType::UInt64);

        assert!(!user_id.is_compatible_with(&team_id));
        assert!(!user_id.is_compatible_with(&raw_id));
        assert!(user_id.is_compatible_with(&user_id));
    }

    #[test]
    fn row_id_generation_is_explicit_identity() {
        let first = RowId {
            page: PageId(5),
            slot: 3,
            generation: 1,
        };
        assert_ne!(
            first,
            RowId {
                generation: 2,
                ..first
            }
        );
    }

    #[test]
    fn scalar_refs_preserve_kind_type_nullability_and_owned_value() {
        let cases = scalar_cases();
        for value in &cases {
            let scalar_ref = ScalarRef::from(value);
            assert_eq!(scalar_ref.physical_type(), value.physical_type());
            assert_eq!(scalar_ref.is_null(), matches!(value, ScalarValue::Null));
            assert_eq!(scalar_ref.to_owned(), *value);
            for physical in [
                PhysicalType::Bool,
                PhysicalType::Int8,
                PhysicalType::Int16,
                PhysicalType::Int32,
                PhysicalType::Int64,
                PhysicalType::Int128,
                PhysicalType::UInt8,
                PhysicalType::UInt16,
                PhysicalType::UInt32,
                PhysicalType::UInt64,
                PhysicalType::UInt128,
                PhysicalType::Float32,
                PhysicalType::Float64,
                PhysicalType::Text,
                PhysicalType::Bytes,
            ] {
                let semantic = SemanticType::physical(physical);
                assert_eq!(
                    scalar_ref.matches_type(&semantic),
                    value.matches_type(&semantic)
                );
            }
        }
    }

    #[test]
    fn text_scalar_ref_borrows_the_original_string_allocation() {
        let value = ScalarValue::Text(String::from("borrowed payload"));
        let ScalarValue::Text(text) = &value else {
            panic!("test value must be Text");
        };
        let ScalarRef::Text(view) = ScalarRef::from(&value) else {
            panic!("Text value must produce a Text view");
        };
        assert_eq!(view, text);
        assert_eq!(view.as_ptr(), text.as_ptr());
    }

    #[test]
    fn every_physical_type_has_stable_capabilities_and_display() {
        let expected = [
            (PhysicalType::Bool, "BOOL", Some(1)),
            (PhysicalType::Int8, "INT8", Some(1)),
            (PhysicalType::Int16, "INT16", Some(2)),
            (PhysicalType::Int32, "INT32", Some(4)),
            (PhysicalType::Int64, "INT64", Some(8)),
            (PhysicalType::Int128, "INT128", Some(16)),
            (PhysicalType::UInt8, "UINT8", Some(1)),
            (PhysicalType::UInt16, "UINT16", Some(2)),
            (PhysicalType::UInt32, "UINT32", Some(4)),
            (PhysicalType::UInt64, "UINT64", Some(8)),
            (PhysicalType::UInt128, "UINT128", Some(16)),
            (PhysicalType::Float32, "FLOAT32", Some(4)),
            (PhysicalType::Float64, "FLOAT64", Some(8)),
            (PhysicalType::Text, "TEXT", None),
            (PhysicalType::Bytes, "BYTES", None),
        ];
        for (physical, display, width) in expected {
            assert_eq!(physical.to_string(), display);
            assert_eq!(physical.fixed_width(), width);
            assert!(physical.is_orderable());
        }
    }

    #[test]
    fn canonical_floats_make_equality_hash_and_total_order_agree() {
        let zero32 = Float32Value::new(0.0);
        assert_eq!(zero32, Float32Value::new(-0.0));
        let nan32 = Float32Value::from_bits(0x7fc0_0001);
        assert_eq!(nan32, Float32Value::from_bits(0xffff_ffff));
        assert_eq!(nan32.to_bits(), Float32Value::CANONICAL_NAN_BITS);
        assert!(Float32Value::new(f32::INFINITY) < nan32);
        assert!(Float32Value::new(f32::NEG_INFINITY) < Float32Value::new(-1.0));
        assert!(Float32Value::from_bits(1) > zero32);

        let zero64 = Float64Value::new(0.0);
        assert_eq!(zero64, Float64Value::new(-0.0));
        let nan64 = Float64Value::from_bits(0x7ff0_0000_0000_0001);
        assert_eq!(nan64, Float64Value::from_bits(u64::MAX));
        assert_eq!(nan64.to_bits(), Float64Value::CANONICAL_NAN_BITS);
        assert!(Float64Value::new(f64::INFINITY) < nan64);

        let mut left = DefaultHasher::new();
        Float64Value::new(-0.0).hash(&mut left);
        let mut right = DefaultHasher::new();
        Float64Value::new(0.0).hash(&mut right);
        assert_eq!(left.finish(), right.finish());
    }

    #[test]
    fn database_comparison_is_exactly_typed_and_bytes_are_lexicographic() {
        assert_eq!(
            ScalarValue::Bytes(vec![0, 0xff]).database_cmp(&ScalarValue::Bytes(vec![1])),
            Some(std::cmp::Ordering::Less)
        );
        assert_eq!(
            ScalarValue::Int32(1).database_cmp(&ScalarValue::Int64(1)),
            None
        );
        assert_eq!(
            ScalarValue::Null.database_cmp(&ScalarValue::Int8(i8::MIN)),
            Some(std::cmp::Ordering::Less)
        );
    }
}
