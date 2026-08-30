//! Quiescent, read-only ownership proof. Page kind never authorizes reclamation.
use std::collections::{HashMap, HashSet};

use netbadb_index::{
    BTreeHandle, IndexError, IndexSpec, MetaNode, btree_page_owner, decode_index_catalog,
    decode_internal_owned, decode_leaf_owned, decode_meta,
};
use netbadb_types::{IndexId, PageId, PhysicalType, SemanticType};

use super::{CatalogSnapshot, HeapStorage, IndexPageInventory};
use crate::{PageType, StorageError};

/// Unstable admin inventory, not ordinary catalog/Inspection JSON. Counts are
/// observations, never permission to truncate. Physical reclamation is deferred.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexReclaimReport {
    pub catalog_pages: u64,
    pub database_pages: u64,
    pub active_indexes: u64,
    /// Includes full owned retirements awaiting catalog compaction.
    pub pending_reclaim_indexes: u64,
    pub legacy_unreclaimable_indexes: u64,
    pub next_index_id: IndexId,
    pub owned_pages: u64,
    pub retired_owned_pages: u64,
    pub retired_orphan_pages: u64,
    pub active_orphan_pages: u64,
    /// Pure geometry; generation/buffer/WAL reuse is NOT authorized.
    pub retired_suffix_pages: u64,
    pub retained_middle_pages: u64,
    pub pages_reclaimed: u64,
    /// v1 root-reachable pages outside current registrations. Raw trees and
    /// historically abandoned legacy trees are indistinguishable on disk.
    pub unregistered_legacy_pages: u64,
    /// v1 nodes without a reachable metadata root; never heuristically owned.
    pub unowned_legacy_pages: u64,
    pub obsolete_catalog_pages: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum OwnershipClass {
    Active,
    Retired,
    UnregisteredLegacy,
    UnownedLegacy,
}

#[derive(Debug)]
pub(super) struct BTreePageOwnership {
    pub page_id: PageId,
    pub owner: Option<IndexId>,
    pub kind: PageType,
    pub class: OwnershipClass,
    pub reachable: bool,
}

impl HeapStorage {
    /// Reads and fully validates the managed file under checkpoint admission.
    /// No checkpoint, catalog rewrite, frame discard or physical reclaim occurs.
    pub fn inspect_index_reclaim(&mut self) -> Result<IndexReclaimReport, StorageError> {
        self.transactions.ensure_checkpoint_safe()?;
        self.buffer.ensure_unpinned()?;
        let catalog = self.read_index_catalog(self.index_catalog_root)?;
        Ok(self.index_page_inventory(&catalog)?.report)
    }

    pub(super) fn index_page_inventory(
        &mut self,
        catalog: &CatalogSnapshot,
    ) -> Result<IndexPageInventory, StorageError> {
        netbadb_index::validate_catalog_entries(&catalog.entries)?;
        netbadb_index::validate_pending_ownership(
            Some(catalog.next_index_id),
            &catalog.entries,
            &catalog.pending,
        )?;
        let count = self.buffer.page_count();
        let mut reserved = HashSet::from([PageId(0)]);
        let mut metadata = HashMap::<PageId, MetaNode>::new();
        let mut page_owners = HashMap::<PageId, (PageType, Option<IndexId>)>::new();
        let mut owner_roots = HashMap::<IndexId, PageId>::new();
        let mut obsolete_catalog_pages = 0;
        // First pass validates every Page CRC/layout and discovers full metadata
        // payloads. A v2 orphan must resolve to an authoritative retained owner.
        for number in 1..count {
            let id = PageId(number);
            let guard = self.buffer.read_page(id)?;
            let page = guard.page();
            let kind = page.validated()?.header().page_type;
            match kind {
                PageType::Heap => {
                    reserved.insert(id);
                }
                PageType::IndexCatalog => {
                    decode_index_catalog(page.single_payload(kind)?)?;
                    reserved.insert(id);
                    if !catalog.pages.contains(&id) {
                        obsolete_catalog_pages += 1;
                    }
                }
                PageType::BTreeMeta => {
                    let meta = decode_meta(page.single_payload(kind)?)?;
                    if let Some(owner) = meta.owner {
                        if owner_roots.insert(owner, id).is_some() {
                            return Err(IndexError::DuplicateIndexId(owner).into());
                        }
                    }
                    page_owners.insert(id, (kind, meta.owner));
                    metadata.insert(id, meta);
                }
                PageType::BTreeLeaf | PageType::BTreeInternal => {
                    let owner = btree_page_owner(page.single_payload(kind)?)?;
                    page_owners.insert(id, (kind, owner));
                }
            }
        }
        let mut roots = HashMap::<PageId, (BTreeHandle, OwnershipClass)>::new();
        for entry in &catalog.entries {
            roots.insert(
                entry.definition.handle.meta_page,
                (
                    entry.definition.handle,
                    if entry.retired {
                        OwnershipClass::Retired
                    } else {
                        OwnershipClass::Active
                    },
                ),
            );
        }
        for record in &catalog.pending {
            roots.insert(
                record.meta_page,
                (
                    BTreeHandle {
                        owner: Some(record.index_id),
                        meta_page: record.meta_page,
                    },
                    OwnershipClass::Retired,
                ),
            );
        }
        for (id, meta) in &metadata {
            if !roots.contains_key(id) {
                if let Some(owner) = meta.owner {
                    return Err(IndexError::UnknownIndexId(owner).into());
                }
                roots.insert(
                    *id,
                    (
                        BTreeHandle {
                            owner: None,
                            meta_page: *id,
                        },
                        OwnershipClass::UnregisteredLegacy,
                    ),
                );
            }
        }
        let mut reachable = HashMap::<PageId, (OwnershipClass, IndexSpec)>::new();
        let mut classes = HashMap::<IndexId, OwnershipClass>::new();
        let mut legacy_retired = HashSet::new();
        // Deterministic order makes malformed overlap diagnostics reproducible.
        let mut ordered_roots: Vec<_> = roots.values().cloned().collect();
        ordered_roots.sort_by_key(|(handle, _)| handle.meta_page.0);
        for (handle, class) in ordered_roots {
            let pages = self.btree().collect_owned_pages(handle, &mut reserved)?;
            let meta = metadata
                .get(&handle.meta_page)
                .ok_or(IndexError::InvalidNodeType)?;
            if let Some(owner) = handle.owner {
                classes.insert(owner, class);
            }
            for id in pages {
                if class == OwnershipClass::Retired && handle.owner.is_none() {
                    legacy_retired.insert(id);
                }
                reachable.insert(id, (class, meta.spec.clone()));
            }
        }
        let mut observations = Vec::new();
        let mut retired = HashSet::new();
        for number in 1..count {
            let id = PageId(number);
            let guard = self.buffer.read_page(id)?;
            let page = guard.page();
            let kind = page.validated()?.header().page_type;
            if !matches!(
                kind,
                PageType::BTreeMeta | PageType::BTreeInternal | PageType::BTreeLeaf
            ) {
                continue;
            }
            let payload = page.single_payload(kind)?;
            let owner = btree_page_owner(payload)?;
            let class = if let Some(owner) = owner {
                *classes
                    .get(&owner)
                    .ok_or(IndexError::UnknownIndexId(owner))?
            } else {
                reachable
                    .get(&id)
                    .map_or(OwnershipClass::UnownedLegacy, |(class, _)| *class)
            };
            if kind == PageType::BTreeMeta {
                decode_meta(payload)?;
            } else if let Some(owner) = owner {
                let root = owner_roots
                    .get(&owner)
                    .ok_or(IndexError::UnknownIndexId(owner))?;
                let meta = metadata.get(root).ok_or(IndexError::InvalidNodeType)?;
                validate_node_payload(kind, payload, &meta.spec, Some(owner), &page_owners)?;
            } else if let Some((_, spec)) = reachable.get(&id) {
                validate_node_payload(kind, payload, spec, None, &page_owners)?;
            } else {
                // Legacy orphan has no canonical nominal spec. Decode all
                // structural fields with a homogeneous physical key type;
                // success proves only valid legacy bytes, NEVER ownership.
                let mut result = Err(IndexError::InvalidNodeType.into());
                for physical in [
                    PhysicalType::Bool,
                    PhysicalType::Int64,
                    PhysicalType::UInt64,
                    PhysicalType::Text,
                ] {
                    let spec = IndexSpec {
                        data_type: SemanticType::physical(physical),
                        nullable: true,
                    };
                    result = validate_node_payload(kind, payload, &spec, None, &page_owners);
                    if result.is_ok() {
                        break;
                    }
                }
                result?;
            }
            if class == OwnershipClass::Retired && owner.is_some() {
                retired.insert(id);
            }
            observations.push(BTreePageOwnership {
                page_id: id,
                owner,
                kind,
                class,
                reachable: reachable.contains_key(&id),
            });
        }
        let mut suffix = count;
        while suffix > 1 && retired.contains(&PageId(suffix - 1)) {
            suffix -= 1;
        }
        let report = IndexReclaimReport {
            catalog_pages: catalog.pages.len() as u64,
            database_pages: count,
            active_indexes: catalog
                .entries
                .iter()
                .filter(|entry| !entry.retired)
                .count() as u64,
            pending_reclaim_indexes: (catalog.pending.len()
                + catalog
                    .entries
                    .iter()
                    .filter(|entry| entry.retired && entry.definition.handle.owner.is_some())
                    .count()) as u64,
            legacy_unreclaimable_indexes: catalog
                .entries
                .iter()
                .filter(|entry| entry.retired && entry.definition.handle.owner.is_none())
                .count() as u64,
            next_index_id: catalog.next_index_id,
            owned_pages: observations
                .iter()
                .filter(|page| page.owner.is_some())
                .count() as u64,
            retired_owned_pages: retired.len() as u64,
            retired_orphan_pages: observations
                .iter()
                .filter(|page| {
                    page.owner.is_some() && page.class == OwnershipClass::Retired && !page.reachable
                })
                .count() as u64,
            active_orphan_pages: observations
                .iter()
                .filter(|page| {
                    page.owner.is_some() && page.class == OwnershipClass::Active && !page.reachable
                })
                .count() as u64,
            retired_suffix_pages: count - suffix,
            retained_middle_pages: retired.len() as u64 - (count - suffix),
            pages_reclaimed: 0,
            unregistered_legacy_pages: observations
                .iter()
                .filter(|page| page.class == OwnershipClass::UnregisteredLegacy)
                .count() as u64,
            unowned_legacy_pages: observations
                .iter()
                .filter(|page| page.class == OwnershipClass::UnownedLegacy)
                .count() as u64,
            obsolete_catalog_pages,
        };
        if observations
            .iter()
            .filter(|page| page.kind == PageType::BTreeMeta)
            .count()
            != roots.len()
        {
            return Err(IndexError::InvalidNodeType.into());
        }
        // Retired suffix membership comes only from fully decoded observations.
        debug_assert!(observations.iter().all(|page| page.page_id.0 < count));
        Ok(IndexPageInventory {
            retired,
            legacy_retired,
            report,
            #[cfg(test)]
            observations,
        })
    }
}

fn validate_node_payload(
    kind: PageType,
    payload: &[u8],
    spec: &IndexSpec,
    owner: Option<IndexId>,
    pages: &HashMap<PageId, (PageType, Option<IndexId>)>,
) -> Result<(), StorageError> {
    let check = |id: PageId, leaf: bool| -> Result<(), StorageError> {
        let (kind, actual) = pages.get(&id).ok_or(IndexError::InvalidChild(id))?;
        if (leaf && *kind != PageType::BTreeLeaf)
            || (!leaf && !matches!(kind, PageType::BTreeLeaf | PageType::BTreeInternal))
        {
            return Err(IndexError::InvalidNodeType.into());
        }
        netbadb_index::validate_btree_owner(owner, *actual)?;
        Ok(())
    };
    match kind {
        PageType::BTreeLeaf => {
            let node = decode_leaf_owned(spec, payload, owner)?;
            if let Some(next) = node.next_leaf {
                check(next, true)?;
            }
        }
        PageType::BTreeInternal => {
            let node = decode_internal_owned(spec, payload, owner)?;
            check(node.first_child, false)?;
            for separator in node.separators {
                check(separator.right_child, false)?;
            }
        }
        _ => return Err(IndexError::InvalidNodeType.into()),
    }
    Ok(())
}
