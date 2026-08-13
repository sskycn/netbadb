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
/// Query-local identity for one occurrence of a relation in a FROM tree.
/// Unlike [`TableId`], this identifier is never persisted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RelationBindingId(pub u32);
id_type!(ColumnId, u32);
id_type!(IndexId, u64);
id_type!(PageId, u64);
id_type!(FrameId, u32);
id_type!(TxnId, u64);
id_type!(Lsn, u64);

/// A stable, explicitly sized slot identifier inside a database page.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SlotId(pub u16);

/// A stable location of a row inside a heap page.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RowId {
    pub page: PageId,
    pub slot: u16,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScalarValue {
    Bool(bool),
    Int64(i64),
    UInt64(u64),
    Text(String),
    Null,
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
    use super::{PhysicalType, SemanticType};

    #[test]
    fn nominal_types_do_not_collapse_to_their_physical_type() {
        let user_id = SemanticType::named("UserId", PhysicalType::UInt64);
        let team_id = SemanticType::named("TeamId", PhysicalType::UInt64);
        let raw_id = SemanticType::physical(PhysicalType::UInt64);

        assert!(!user_id.is_compatible_with(&team_id));
        assert!(!user_id.is_compatible_with(&raw_id));
        assert!(user_id.is_compatible_with(&user_id));
    }
}
