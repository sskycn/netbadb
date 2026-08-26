use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;

use netbadb_storage::TableStorage;
use netbadb_types::{StorageId, TableId};

/// Current one-to-one logical-to-physical binding for an opened catalog.
///
/// The resolver owns this temporary mapping so callers do not turn it into a
/// permanent `TableId -> StorageId` assumption when partition routing arrives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PhysicalTableBinding {
    pub(crate) table_id: TableId,
    pub(crate) storage_id: StorageId,
}

#[derive(Debug)]
pub(crate) struct PhysicalBindings {
    bindings: Vec<PhysicalTableBinding>,
}

impl PhysicalBindings {
    pub(crate) fn new(
        bindings: Vec<PhysicalTableBinding>,
        registry: &StorageRegistry,
    ) -> Result<Self, StorageRegistryError> {
        let mut tables = BTreeSet::new();
        for binding in &bindings {
            if !tables.insert(binding.table_id) {
                return Err(StorageRegistryError::DuplicateTableBinding {
                    table_id: binding.table_id,
                });
            }
            if registry.get(binding.storage_id).is_none() {
                return Err(StorageRegistryError::UnknownStorageId {
                    storage_id: binding.storage_id,
                });
            }
        }
        Ok(Self { bindings })
    }

    pub(crate) fn resolve_current(
        &self,
        table_id: TableId,
    ) -> Result<StorageId, StorageRegistryError> {
        self.bindings
            .iter()
            .find(|binding| binding.table_id == table_id)
            .map(|binding| binding.storage_id)
            .ok_or(StorageRegistryError::MissingPhysicalBinding { table_id })
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = PhysicalTableBinding> + '_ {
        self.bindings.iter().copied()
    }
}

#[derive(Debug)]
pub(crate) struct StorageRegistryEntry {
    pub(crate) id: StorageId,
    pub(crate) storage: TableStorage,
}

/// Deterministic owner of physical storage instances for one opened database.
#[derive(Debug)]
pub(crate) struct StorageRegistry {
    entries: Vec<StorageRegistryEntry>,
}

impl StorageRegistry {
    pub(crate) fn from_catalog_order(
        storages: Vec<TableStorage>,
    ) -> Result<(Self, PhysicalBindings), StorageRegistryError> {
        let mut entries = Vec::with_capacity(storages.len());
        let mut bindings = Vec::with_capacity(storages.len());
        for (position, storage) in storages.into_iter().enumerate() {
            let ordinal = position
                .checked_add(1)
                .and_then(|value| u64::try_from(value).ok())
                .ok_or(StorageRegistryError::StorageIdExhausted)?;
            let storage_id = StorageId(ordinal);
            bindings.push(PhysicalTableBinding {
                table_id: storage.table().id,
                storage_id,
            });
            entries.push(StorageRegistryEntry {
                id: storage_id,
                storage,
            });
        }
        let registry = Self::new(entries)?;
        let bindings = PhysicalBindings::new(bindings, &registry)?;
        Ok((registry, bindings))
    }

    pub(crate) fn new(entries: Vec<StorageRegistryEntry>) -> Result<Self, StorageRegistryError> {
        let mut ids = BTreeSet::new();
        for entry in &entries {
            if !ids.insert(entry.id) {
                return Err(StorageRegistryError::DuplicateStorageId {
                    storage_id: entry.id,
                });
            }
        }
        Ok(Self { entries })
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn get(&self, storage_id: StorageId) -> Option<&TableStorage> {
        self.entries
            .iter()
            .find(|entry| entry.id == storage_id)
            .map(|entry| &entry.storage)
    }

    pub(crate) fn get_mut(&mut self, storage_id: StorageId) -> Option<&mut TableStorage> {
        self.entries
            .iter_mut()
            .find(|entry| entry.id == storage_id)
            .map(|entry| &mut entry.storage)
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &StorageRegistryEntry> {
        self.entries.iter()
    }

    pub(crate) fn iter_mut(&mut self) -> impl Iterator<Item = &mut StorageRegistryEntry> {
        self.entries.iter_mut()
    }

    pub(crate) fn into_entries(self) -> Vec<StorageRegistryEntry> {
        self.entries
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageRegistryError {
    UnknownStorageId { storage_id: StorageId },
    MissingPhysicalBinding { table_id: TableId },
    DuplicateStorageId { storage_id: StorageId },
    DuplicateTableBinding { table_id: TableId },
    StorageIdExhausted,
}

impl fmt::Display for StorageRegistryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownStorageId { storage_id } => {
                write!(
                    formatter,
                    "physical storage {} is not registered",
                    storage_id.0
                )
            }
            Self::MissingPhysicalBinding { table_id } => write!(
                formatter,
                "logical table {} has no physical storage binding",
                table_id.0
            ),
            Self::DuplicateStorageId { storage_id } => write!(
                formatter,
                "physical storage {} is registered more than once",
                storage_id.0
            ),
            Self::DuplicateTableBinding { table_id } => write!(
                formatter,
                "logical table {} has more than one current physical binding",
                table_id.0
            ),
            Self::StorageIdExhausted => {
                formatter.write_str("physical storage identity space is exhausted")
            }
        }
    }
}

impl Error for StorageRegistryError {}
