//! Quiescent, read-only ownership proof. Page kind never authorizes reclamation.
use std::collections::{HashMap, HashSet};

use netbadb_index::{
    BTreeHandle, BTreePageRef, IndexError, IndexSpec, MetaNode, btree_page_owner,
    decode_index_catalog, decode_internal_owned, decode_leaf_owned, decode_meta,
};
use netbadb_types::{IndexId, PageId, PhysicalType, SemanticType};

use super::{CatalogSnapshot, HeapStorage, IndexPageInventory};
use crate::{PageType, StorageError};

/// Unstable admin inventory, not ordinary catalog/Inspection JSON. Counts are
/// observations, never standalone permission to truncate. Tail maintenance also
/// requires whole-tree, checkpoint, buffer and durable-intent validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexReclaimReport {
    /// Exact identities of all generation-aware owned pages, including orphans.
    pub allocations: Vec<IndexPageAllocation>,
    /// Pending records retain either owner-only v3 or explicit root identity.
    pub pending: Vec<netbadb_index::RetiredIndexOwnership>,
    pub catalog_pages: u64,
    pub database_pages: u64,
    pub active_indexes: u64,
    /// Includes full owned retirements awaiting catalog compaction.
    pub pending_reclaim_indexes: u64,
    pub legacy_unreclaimable_indexes: u64,
    pub next_index_id: IndexId,
    pub owned_pages: u64,
    pub retired_owned_pages: u64,
    /// Proven orphans of still root-dependent retirements only.
    pub retired_orphan_pages: u64,
    /// Owner-only pages whose former reachability is no longer authoritative.
    pub owner_only_pages: u64,
    pub active_orphan_pages: u64,
    /// Explicit NBTR v1 states, independent of whole-index pending ownership.
    pub retired_marker_pages: u64,
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

/// One validated allocation in the unstable maintenance inventory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexPageAllocation {
    pub owner: IndexId,
    pub page_ref: netbadb_types::PageRef,
    /// None means owner-only retirement: historical reachability is unknown.
    pub reachable: Option<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum OwnershipClass {
    Active,
    Retired,
    RetiredMarker,
    UnregisteredLegacy,
    UnownedLegacy,
}

#[derive(Debug)]
pub(super) struct BTreePageOwnership {
    pub generation: Option<netbadb_types::PageGeneration>,
    pub page_id: PageId,
    pub owner: Option<IndexId>,
    pub kind: PageType,
    pub class: OwnershipClass,
    pub reachable: bool,
}

/// Storage capability, not a page-kind tag or global free-list membership.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageReuseClass {
    /// Retired registered BTree v3 allocations; never Heap/Catalog/raw pages.
    GenerationSafeBTreeV3,
    /// NBTR v1 independently proves retirement, including for an active owner.
    RetiredBTreeMarker,
}

/// Validated old identity. Inspection is not an allocation or buffer claim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReusablePageInspection {
    pub page_ref: netbadb_types::PageRef,
    pub retired_index_id: IndexId,
    pub class: PageReuseClass,
}

/// Unstable quiescent maintenance DTO. Claims independently revalidate identity,
/// committed retirement and buffer eligibility before logging a transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageReuseInspection {
    /// Lowest physical PageId first, including eligible tail pages.
    pub candidates: Vec<ReusablePageInspection>,
    pub file_pages: u64,
    pub middle_candidates: u64,
    pub pending_owners: u64,
    pub blocked_legacy_indexes: u64,
    pub blocked_active_orphans: u64,
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

    /// Builds a disposable, fully validated candidate inventory without
    /// checkpointing, reserving generations, or modifying persistent metadata.
    /// No caller may treat this result as permission to overwrite a page.
    pub fn inspect_reusable_pages(&mut self) -> Result<PageReuseInspection, StorageError> {
        self.transactions.ensure_checkpoint_safe()?;
        self.buffer.ensure_unpinned()?;
        self.buffer.validated_page_count()?;
        let catalog = self.read_index_catalog(self.index_catalog_root)?;
        if catalog.reclaim_intent.is_some() {
            return Err(IndexError::InvalidReclaimIntent.into());
        }
        let inventory = self.index_page_inventory(&catalog)?;
        let candidates: Vec<_> = inventory
            .reusable_btree_pages()
            .into_iter()
            .map(|page| ReusablePageInspection {
                page_ref: page.old_page_ref,
                retired_index_id: page.retired_index_id,
                class: page.source,
            })
            .collect();
        // The older geometric report includes retired v2 pages. Only this
        // capability's candidates can form a reusable suffix; legacy tails are
        // blockers, so generated pages below them remain middle holes.
        let mut suffix_start = inventory.report.database_pages;
        for candidate in candidates.iter().rev() {
            if candidate.page_ref.page_id.0 != suffix_start - 1 {
                break;
            }
            suffix_start -= 1;
        }
        Ok(PageReuseInspection {
            middle_candidates: candidates
                .iter()
                .filter(|page| page.page_ref.page_id.0 < suffix_start)
                .count() as u64,
            candidates,
            file_pages: inventory.report.database_pages,
            pending_owners: inventory.report.pending_reclaim_indexes,
            blocked_legacy_indexes: inventory.report.legacy_unreclaimable_indexes,
            blocked_active_orphans: inventory.report.active_orphan_pages,
        })
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
        let count = self.buffer.validated_page_count()?;
        let owner_only: HashSet<_> = catalog
            .pending
            .iter()
            .filter(|record| record.meta_page.is_none())
            .map(|record| record.index_id)
            .collect();
        let mut reserved = HashSet::from([PageId(0)]);
        let mut metadata = HashMap::<PageId, MetaNode>::new();
        let mut page_owners = HashMap::<
            PageId,
            (
                PageType,
                Option<IndexId>,
                Option<netbadb_types::PageGeneration>,
            ),
        >::new();
        let mut owner_roots = HashMap::<IndexId, PageId>::new();
        let mut obsolete_catalog_pages = 0;
        // First pass validates every Page CRC/layout and discovers full metadata
        // payloads. An owned orphan must resolve to an authoritative retained owner.
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
                    page_owners.insert(id, (kind, meta.owner, meta.generation));
                    metadata.insert(id, meta);
                }
                PageType::BTreeLeaf | PageType::BTreeInternal => {
                    page.allocation_generation()?;
                    let owner = btree_page_owner(page.single_payload(kind)?)?;
                    page_owners.insert(
                        id,
                        (
                            kind,
                            owner,
                            netbadb_index::btree_page_generation(page.single_payload(kind)?)?,
                        ),
                    );
                }
            }
        }
        let mut roots = HashMap::<PageId, (BTreeHandle, OwnershipClass)>::new();
        for entry in &catalog.entries {
            roots.insert(
                entry.definition.handle.meta_page.page_id(),
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
            let Some(reference) = record.meta_page else {
                continue;
            };
            roots.insert(
                reference.page_id(),
                (
                    BTreeHandle {
                        owner: Some(record.index_id),
                        meta_page: reference,
                    },
                    OwnershipClass::Retired,
                ),
            );
        }
        for (id, meta) in &metadata {
            if !roots.contains_key(id) {
                if let Some(owner) = meta.owner {
                    if owner_only.contains(&owner) {
                        continue;
                    }
                    return Err(IndexError::UnknownIndexId(owner).into());
                }
                roots.insert(
                    *id,
                    (
                        BTreeHandle {
                            owner: None,
                            meta_page: BTreePageRef::Legacy(*id),
                        },
                        OwnershipClass::UnregisteredLegacy,
                    ),
                );
            }
        }
        let mut reachable = HashMap::<PageId, (OwnershipClass, IndexSpec)>::new();
        let mut classes: HashMap<_, _> = owner_only
            .iter()
            .map(|owner| (*owner, OwnershipClass::Retired))
            .collect();
        let mut legacy_retired = HashSet::new();
        // Deterministic order makes malformed overlap diagnostics reproducible.
        let mut ordered_roots: Vec<_> = roots.values().cloned().collect();
        ordered_roots.sort_by_key(|(handle, _)| handle.meta_page.page_id().0);
        for (handle, class) in ordered_roots {
            let pages = self.btree().collect_owned_pages(handle, &mut reserved)?;
            let meta = metadata
                .get(&handle.meta_page.page_id())
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
        let mut markers = HashSet::new();
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
            if let Some(marker) = netbadb_index::retired_btree_page(payload)? {
                page.allocation_generation()?;
                if marker.owner >= catalog.next_index_id || reachable.contains_key(&id) {
                    return Err(IndexError::InvalidNodeType.into());
                }
                markers.insert(id);
                observations.push(BTreePageOwnership {
                    generation: Some(marker.page_ref.generation),
                    page_id: id,
                    owner: Some(marker.owner),
                    kind,
                    class: OwnershipClass::RetiredMarker,
                    reachable: false,
                });
                continue;
            }
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
            if owner.is_some_and(|owner| owner_only.contains(&owner)) {
                if netbadb_index::btree_page_generation(payload)?.is_none() {
                    return Err(IndexError::InvalidNodeType.into());
                }
                validate_owner_only_payload(id, kind, payload, owner, &page_owners)?;
                // Outgoing dormant refs are not allocation ownership. Active
                // and raw traversals above still reject incoming aliases.
            } else if kind == PageType::BTreeMeta {
                decode_meta(payload)?;
            } else if let Some(owner) = owner {
                let root = owner_roots
                    .get(&owner)
                    .ok_or(IndexError::UnknownIndexId(owner))?;
                let meta = metadata.get(root).ok_or(IndexError::InvalidNodeType)?;
                // Orphans have no incoming ref, but cannot change the format
                // of their owning tree. Every v3 orphan must self-identify.
                if netbadb_index::btree_page_generation(payload)?.is_some()
                    != meta.generation.is_some()
                {
                    return Err(IndexError::InvalidNodeType.into());
                }
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
                generation: netbadb_index::btree_page_generation(payload)?,
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
            allocations: observations
                .iter()
                .filter_map(|page| {
                    Some(IndexPageAllocation {
                        owner: page.owner?,
                        page_ref: netbadb_types::PageRef {
                            page_id: page.page_id,
                            generation: page.generation?,
                        },
                        reachable: (!owner_only.contains(&page.owner?)).then_some(page.reachable),
                    })
                })
                .collect(),
            pending: catalog.pending.clone(),
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
                .filter(|entry| {
                    entry.retired && entry.definition.handle.meta_page.generation().is_none()
                })
                .count() as u64
                + catalog
                    .pending
                    .iter()
                    .filter(|p| !p.is_generation_safe())
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
                    page.owner.is_some()
                        && page.class == OwnershipClass::Retired
                        && !page.reachable
                        && !page.owner.is_some_and(|owner| owner_only.contains(&owner))
                })
                .count() as u64,
            owner_only_pages: observations
                .iter()
                .filter(|page| {
                    page.class != OwnershipClass::RetiredMarker
                        && page.owner.is_some_and(|owner| owner_only.contains(&owner))
                })
                .count() as u64,
            active_orphan_pages: observations
                .iter()
                .filter(|page| {
                    page.owner.is_some() && page.class == OwnershipClass::Active && !page.reachable
                })
                .count() as u64,
            retired_marker_pages: markers.len() as u64,
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
            .filter(|page| {
                page.kind == PageType::BTreeMeta
                    && !page.owner.is_some_and(|owner| owner_only.contains(&owner))
            })
            .count()
            != roots.len()
        {
            return Err(IndexError::InvalidNodeType.into());
        }
        // Retired suffix membership comes only from fully decoded observations.
        debug_assert!(observations.iter().all(|page| page.page_id.0 < count));
        Ok(IndexPageInventory {
            retired,
            markers,
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
    pages: &HashMap<
        PageId,
        (
            PageType,
            Option<IndexId>,
            Option<netbadb_types::PageGeneration>,
        ),
    >,
) -> Result<(), StorageError> {
    let check = |reference: BTreePageRef, leaf: bool| -> Result<(), StorageError> {
        let id = reference.page_id();
        let (kind, actual, generation) = pages.get(&id).ok_or(IndexError::InvalidChild(id))?;
        if (leaf && *kind != PageType::BTreeLeaf)
            || (!leaf && !matches!(kind, PageType::BTreeLeaf | PageType::BTreeInternal))
        {
            return Err(IndexError::InvalidNodeType.into());
        }
        netbadb_index::validate_btree_owner(owner, *actual)?;
        netbadb_index::validate_btree_generation(reference.generation(), *generation)?;
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

/// A retired owner no longer has a canonical live tree spec. The homogeneous
/// physical key representation is self-describing; decode the complete payload
/// with each supported physical family. Nominal identity is not a free-page
/// authority. Required and optional PageRefs still undergo intrinsic decoding,
/// but historical outgoing edges may name allocations already consumed.
fn validate_owner_only_payload(
    page_id: PageId,
    kind: PageType,
    payload: &[u8],
    owner: Option<IndexId>,
    pages: &HashMap<
        PageId,
        (
            PageType,
            Option<IndexId>,
            Option<netbadb_types::PageGeneration>,
        ),
    >,
) -> Result<(), StorageError> {
    let check_refs = |references: Vec<BTreePageRef>| -> Result<(), StorageError> {
        let mut seen = HashSet::from([page_id]);
        for reference in references {
            if !seen.insert(reference.page_id()) {
                return Err(IndexError::SharedTreePage(reference.page_id()).into());
            }
            if let Some((target_kind, target_owner, generation)) = pages.get(&reference.page_id()) {
                if *generation == reference.generation() {
                    netbadb_index::validate_btree_owner(owner, *target_owner)?;
                    if *target_kind == PageType::BTreeMeta
                        || (kind == PageType::BTreeLeaf && *target_kind != PageType::BTreeLeaf)
                    {
                        return Err(IndexError::InvalidNodeType.into());
                    }
                }
            }
            // Missing or non-BTree targets can be historical: a consumed page's
            // new owner may later be tail-reclaimed, then EOF appended as Heap
            // or Catalog. Dormant outgoing edges never claim those new pages.
            // Live incoming aliases are rejected by the root traversals.
        }
        Ok(())
    };
    if kind == PageType::BTreeMeta {
        return check_refs(vec![decode_meta(payload)?.root_page]);
    }
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
        result = match kind {
            PageType::BTreeLeaf => match decode_leaf_owned(&spec, payload, owner) {
                Ok(node) => return check_refs(node.next_leaf.into_iter().collect()),
                Err(error) => Err(error.into()),
            },
            PageType::BTreeInternal => match decode_internal_owned(&spec, payload, owner) {
                Ok(node) => {
                    return check_refs(
                        std::iter::once(node.first_child)
                            .chain(
                                node.separators
                                    .into_iter()
                                    .map(|separator| separator.right_child),
                            )
                            .collect(),
                    );
                }
                Err(error) => Err(error.into()),
            },
            _ => Err(IndexError::InvalidNodeType.into()),
        };
        if result.is_ok() {
            break;
        }
    }
    result
}
