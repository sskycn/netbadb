//! Shared, language-independent identifiers and scalar types.

use std::fmt;

macro_rules! id_type {
    ($name:ident, $inner:ty) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(pub $inner);
    };
}

id_type!(DatabaseId, u64);
id_type!(TableId, u64);
/// Physical storage identity within one opened database composition.
///
/// This is distinct from logical [`TableId`], is deterministically assigned
/// from validated catalog order, and is not currently persisted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StorageId(pub u64);
/// Database-coordinator transaction identity within one opened database.
///
/// Heap WAL transactions retain their independent [`TxnId`] identities.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DatabaseTxnId(pub u64);
/// Query-local identity for one occurrence of a relation in a FROM tree.
/// Unlike [`TableId`], this identifier is never persisted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RelationBindingId(pub u32);
id_type!(ColumnId, u32);
id_type!(IndexId, u64);
/// Table-scoped opaque identity for one executable physical access path.
///
/// The planner may compare and copy this value but must not infer a B+Tree
/// page, LSM structure, or columnar segment from its numeric representation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct AccessPathId(pub u64);
id_type!(PageId, u64);
id_type!(FrameId, u32);
id_type!(TxnId, u64);
id_type!(Lsn, u64);
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
    Int64,
    UInt64,
    Text,
}

impl fmt::Display for PhysicalType {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Bool => "BOOL",
            Self::Int64 => "INT64",
            Self::UInt64 => "UINT64",
            Self::Text => "TEXT",
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ScalarValue {
    Bool(bool),
    Int64(i64),
    UInt64(u64),
    Text(String),
    Null,
}

/// A borrowed runtime view of a scalar value.
///
/// This type carries no persistence, wire, schema, or SQL-expression
/// semantics. In particular, [`ScalarRef::Text`] may borrow short-lived
/// validated storage bytes and must remain scoped to their owning operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarRef<'a> {
    Bool(bool),
    Int64(i64),
    UInt64(u64),
    Text(&'a str),
    Null,
}

impl ScalarRef<'_> {
    #[must_use]
    pub const fn physical_type(self) -> Option<PhysicalType> {
        match self {
            Self::Bool(_) => Some(PhysicalType::Bool),
            Self::Int64(_) => Some(PhysicalType::Int64),
            Self::UInt64(_) => Some(PhysicalType::UInt64),
            Self::Text(_) => Some(PhysicalType::Text),
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

    #[must_use]
    pub fn to_owned(self) -> ScalarValue {
        match self {
            Self::Bool(value) => ScalarValue::Bool(value),
            Self::Int64(value) => ScalarValue::Int64(value),
            Self::UInt64(value) => ScalarValue::UInt64(value),
            Self::Text(value) => ScalarValue::Text(value.to_owned()),
            Self::Null => ScalarValue::Null,
        }
    }
}

impl<'a> From<&'a ScalarValue> for ScalarRef<'a> {
    fn from(value: &'a ScalarValue) -> Self {
        match value {
            ScalarValue::Bool(value) => Self::Bool(*value),
            ScalarValue::Int64(value) => Self::Int64(*value),
            ScalarValue::UInt64(value) => Self::UInt64(*value),
            ScalarValue::Text(value) => Self::Text(value.as_str()),
            ScalarValue::Null => Self::Null,
        }
    }
}

impl ScalarValue {
    #[must_use]
    pub fn physical_type(&self) -> Option<PhysicalType> {
        match self {
            Self::Bool(_) => Some(PhysicalType::Bool),
            Self::Int64(_) => Some(PhysicalType::Int64),
            Self::UInt64(_) => Some(PhysicalType::UInt64),
            Self::Text(_) => Some(PhysicalType::Text),
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
}

#[cfg(test)]
mod tests {
    use super::{PageId, PhysicalType, RowId, ScalarRef, ScalarValue, SemanticType};

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
        let cases = [
            ScalarValue::Bool(true),
            ScalarValue::Int64(-7),
            ScalarValue::UInt64(11),
            ScalarValue::Text("payload".into()),
            ScalarValue::Null,
        ];
        for value in &cases {
            let scalar_ref = ScalarRef::from(value);
            assert_eq!(scalar_ref.physical_type(), value.physical_type());
            assert_eq!(scalar_ref.is_null(), matches!(value, ScalarValue::Null));
            assert_eq!(scalar_ref.to_owned(), *value);
            for physical in [
                PhysicalType::Bool,
                PhysicalType::Int64,
                PhysicalType::UInt64,
                PhysicalType::Text,
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
}
