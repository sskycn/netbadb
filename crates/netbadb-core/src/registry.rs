use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;

use netbadb_storage::TableStorage;
use netbadb_types::{
    ColumnId, IndexName, PartitionId, PhysicalType, ScalarValue, StorageId, TableId,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RangePartitionBinding {
    pub(crate) partition_id: PartitionId,
    pub(crate) storage_id: StorageId,
    pub(crate) lower: Option<ScalarValue>,
    pub(crate) upper: Option<ScalarValue>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TablePlacement {
    Single {
        table_id: TableId,
        storage_id: StorageId,
    },
    RangePartitioned {
        table_id: TableId,
        partition_key: ColumnId,
        key_type: PhysicalType,
        partitions: Vec<RangePartitionBinding>,
    },
}

impl TablePlacement {
    pub(crate) const fn table_id(&self) -> TableId {
        match self {
            Self::Single { table_id, .. } | Self::RangePartitioned { table_id, .. } => *table_id,
        }
    }

    pub(crate) fn storage_ids(&self) -> impl Iterator<Item = StorageId> + '_ {
        let single = match self {
            Self::Single { storage_id, .. } => Some(*storage_id),
            Self::RangePartitioned { .. } => None,
        };
        let partitions = match self {
            Self::RangePartitioned { partitions, .. } => Some(partitions.as_slice()),
            Self::Single { .. } => None,
        };
        single.into_iter().chain(
            partitions
                .into_iter()
                .flatten()
                .map(|entry| entry.storage_id),
        )
    }
}

#[derive(Debug)]
pub(crate) struct PhysicalBindings {
    placements: Vec<TablePlacement>,
}

impl PhysicalBindings {
    pub(crate) fn publish_created(&mut self, placement: TablePlacement) {
        self.placements.push(placement);
    }
    pub(crate) fn publish_dropped(&mut self, table_id: TableId) {
        self.placements
            .retain(|placement| placement.table_id() != table_id);
    }
    pub(crate) fn new(
        placements: Vec<TablePlacement>,
        registry: &StorageRegistry,
    ) -> Result<Self, StorageRegistryError> {
        let mut tables = BTreeSet::new();
        let mut assigned_storages = BTreeSet::new();
        for placement in &placements {
            let table_id = placement.table_id();
            if !tables.insert(table_id) {
                return Err(StorageRegistryError::DuplicateTableBinding { table_id });
            }
            for storage_id in placement.storage_ids() {
                if !assigned_storages.insert(storage_id) {
                    return Err(StorageRegistryError::DuplicatePlacementStorage { storage_id });
                }
                let storage = registry
                    .get(storage_id)
                    .ok_or(StorageRegistryError::UnknownStorageId { storage_id })?;
                if storage.table().id != table_id {
                    return Err(StorageRegistryError::PlacementTableMismatch {
                        table_id,
                        storage_id,
                        storage_table_id: storage.table().id,
                    });
                }
            }
        }
        if assigned_storages.len() != registry.len() {
            let storage_id = registry
                .iter()
                .find(|entry| !assigned_storages.contains(&entry.id))
                .map(|entry| entry.id)
                .ok_or(StorageRegistryError::StorageCountMismatch)?;
            return Err(StorageRegistryError::UnboundStorage { storage_id });
        }
        Ok(Self { placements })
    }

    pub(crate) fn from_single_storages(
        registry: &StorageRegistry,
    ) -> Result<Self, StorageRegistryError> {
        Self::new(
            registry
                .iter()
                .map(|entry| TablePlacement::Single {
                    table_id: entry.storage.table().id,
                    storage_id: entry.id,
                })
                .collect(),
            registry,
        )
    }

    pub(crate) fn placement(
        &self,
        table_id: TableId,
    ) -> Result<&TablePlacement, StorageRegistryError> {
        self.placements
            .iter()
            .find(|placement| placement.table_id() == table_id)
            .ok_or(StorageRegistryError::MissingPhysicalBinding { table_id })
    }

    #[cfg(test)]
    pub(crate) fn resolve_single(
        &self,
        table_id: TableId,
    ) -> Result<StorageId, StorageRegistryError> {
        match self.placement(table_id)? {
            TablePlacement::Single { storage_id, .. } => Ok(*storage_id),
            TablePlacement::RangePartitioned { .. } => {
                Err(StorageRegistryError::PartitionRoutingRequired { table_id })
            }
        }
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &TablePlacement> {
        self.placements.iter()
    }
}

#[derive(Debug)]
pub(crate) struct StorageRegistryEntry {
    pub(crate) id: StorageId,
    pub(crate) storage: TableStorage,
}

#[derive(Debug)]
pub(crate) struct StorageRegistry {
    entries: Vec<StorageRegistryEntry>,
}

impl StorageRegistry {
    pub(crate) fn publish_created(&mut self, storage: TableStorage) {
        self.entries.push(StorageRegistryEntry {
            id: storage.storage_id(),
            storage,
        });
    }
    pub(crate) fn publish_dropped(&mut self, storage_id: StorageId) -> Option<TableStorage> {
        let position = self
            .entries
            .iter()
            .position(|entry| entry.id == storage_id)?;
        Some(self.entries.remove(position).storage)
    }
    pub(crate) fn from_catalog_order(
        storages: Vec<TableStorage>,
    ) -> Result<(Self, PhysicalBindings), StorageRegistryError> {
        let entries = storages
            .into_iter()
            .map(|storage| StorageRegistryEntry {
                id: storage.storage_id(),
                storage,
            })
            .collect();
        let registry = Self::new(entries)?;
        let bindings = PhysicalBindings::from_single_storages(&registry)?;
        Ok((registry, bindings))
    }

    pub(crate) fn new(entries: Vec<StorageRegistryEntry>) -> Result<Self, StorageRegistryError> {
        let mut ids = BTreeSet::new();
        let mut index_names = BTreeSet::new();
        for entry in &entries {
            if entry.id != entry.storage.storage_id() {
                return Err(StorageRegistryError::RegistryStorageIdentityMismatch {
                    registered: entry.id,
                    persisted: entry.storage.storage_id(),
                });
            }
            if !ids.insert(entry.id) {
                return Err(StorageRegistryError::DuplicateStorageId {
                    storage_id: entry.id,
                });
            }
            for definition in entry.storage.indexes() {
                if let Some(name) = &definition.name {
                    if !index_names.insert(name.clone()) {
                        return Err(StorageRegistryError::DuplicateIndexName {
                            name: name.clone(),
                        });
                    }
                }
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
    DuplicateIndexName {
        name: IndexName,
    },
    UnknownStorageId {
        storage_id: StorageId,
    },
    MissingPhysicalBinding {
        table_id: TableId,
    },
    PartitionRoutingRequired {
        table_id: TableId,
    },
    DuplicateStorageId {
        storage_id: StorageId,
    },
    DuplicatePlacementStorage {
        storage_id: StorageId,
    },
    DuplicateTableBinding {
        table_id: TableId,
    },
    PlacementTableMismatch {
        table_id: TableId,
        storage_id: StorageId,
        storage_table_id: TableId,
    },
    UnboundStorage {
        storage_id: StorageId,
    },
    StorageCountMismatch,
    RegistryStorageIdentityMismatch {
        registered: StorageId,
        persisted: StorageId,
    },
    StorageIdExhausted,
}

impl fmt::Display for StorageRegistryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateIndexName { name } => {
                write!(
                    formatter,
                    "index name `{name}` is registered more than once"
                )
            }
            Self::UnknownStorageId { storage_id } => write!(
                formatter,
                "physical storage {} is not registered",
                storage_id.0
            ),
            Self::MissingPhysicalBinding { table_id } => write!(
                formatter,
                "logical table {} has no physical storage binding",
                table_id.0
            ),
            Self::PartitionRoutingRequired { table_id } => write!(
                formatter,
                "logical table {} requires range-partition routing",
                table_id.0
            ),
            Self::DuplicateStorageId { storage_id } => write!(
                formatter,
                "physical storage {} is registered more than once",
                storage_id.0
            ),
            Self::DuplicatePlacementStorage { storage_id } => write!(
                formatter,
                "physical storage {} belongs to more than one placement",
                storage_id.0
            ),
            Self::DuplicateTableBinding { table_id } => write!(
                formatter,
                "logical table {} has more than one placement",
                table_id.0
            ),
            Self::PlacementTableMismatch {
                table_id,
                storage_id,
                storage_table_id,
            } => write!(
                formatter,
                "storage {} contains table {}, not placement table {}",
                storage_id.0, storage_table_id.0, table_id.0
            ),
            Self::UnboundStorage { storage_id } => write!(
                formatter,
                "physical storage {} is not present in any table placement",
                storage_id.0
            ),
            Self::StorageCountMismatch => {
                formatter.write_str("physical storage placement count is inconsistent")
            }
            Self::RegistryStorageIdentityMismatch {
                registered,
                persisted,
            } => write!(
                formatter,
                "registry storage identity {} does not match persisted identity {}",
                registered.0, persisted.0
            ),
            Self::StorageIdExhausted => {
                formatter.write_str("physical storage identity space is exhausted")
            }
        }
    }
}

impl Error for StorageRegistryError {}
