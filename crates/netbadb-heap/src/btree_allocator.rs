//! Registered-v3 allocation policy. Persistent truth is whole-owner catalog
//! retirement or individual NBTR pages; this cache, including empty, is disposable.
use super::ownership::PageReuseClass;
use super::{HeapStorage, IndexPageInventory};
use crate::{Page, PageType, StorageError, Transaction};
use netbadb_index::{IndexError, RetiredIndexOwnership, decode_index_catalog};
use netbadb_types::{IndexId, PageId, PageRef};
use std::collections::{BTreeMap, HashSet};

#[derive(Debug)]
pub(super) struct ReusableBTreePage {
    pub old_page_ref: PageRef,
    pub retired_index_id: IndexId,
    pub source: PageReuseClass,
}
#[derive(Debug, Default)]
pub(super) struct ReusableBTreePageCache {
    candidates: BTreeMap<PageId, ReusableBTreePage>,
    retired: HashSet<IndexId>,
    root_dependent: HashSet<IndexId>,
}
impl IndexPageInventory {
    pub(super) fn reusable_btree_pages(&self) -> Vec<ReusableBTreePage> {
        self.report
            .allocations
            .iter()
            .filter(|page| {
                self.retired.contains(&page.page_ref.page_id)
                    || self.markers.contains(&page.page_ref.page_id)
            })
            .map(|page| ReusableBTreePage {
                old_page_ref: page.page_ref,
                retired_index_id: page.owner,
                source: if self.markers.contains(&page.page_ref.page_id) {
                    PageReuseClass::RetiredBTreeMarker
                } else {
                    PageReuseClass::GenerationSafeBTreeV3
                },
            })
            .collect()
    }
}
impl HeapStorage {
    fn ensure_reusable_btree_pages(
        &mut self,
        transaction: &Transaction,
    ) -> Result<(), StorageError> {
        if self.buffer.take_reuse_invalidation() {
            self.reusable_btree_pages = None;
        }
        if self.reusable_btree_pages.is_some() {
            return Ok(());
        }
        let catalog = self.read_index_catalog(self.index_catalog_root)?;
        if catalog.reclaim_intent.is_some() {
            return Err(IndexError::InvalidReclaimIntent.into());
        }
        // In-flight DROP cannot authorize reuse: the committed active registry
        // still contains its owner until the durable decision is published.
        let retired: HashSet<_> = catalog
            .pending
            .iter()
            .filter(|p| p.is_generation_safe())
            .map(|p| p.index_id)
            .chain(
                catalog
                    .entries
                    .iter()
                    .filter(|e| e.retired && e.definition.handle.meta_page.generation().is_some())
                    .map(|e| e.definition.id),
            )
            .filter(|owner| !self.indexes.iter().any(|index| index.id == *owner))
            .collect();
        let inventory = self.index_page_inventory(&catalog)?;
        let candidates = inventory
            .reusable_btree_pages()
            .into_iter()
            .filter(|page| {
                !transaction.retired_btree_pages.contains(&page.old_page_ref)
                    && (page.source == PageReuseClass::RetiredBTreeMarker
                        || retired.contains(&page.retired_index_id))
            })
            .map(|page| (page.old_page_ref.page_id, page))
            .collect();
        let root_dependent = catalog
            .entries
            .iter()
            .filter(|entry| entry.retired && retired.contains(&entry.definition.id))
            .map(|entry| entry.definition.id)
            .chain(
                catalog
                    .pending
                    .iter()
                    .filter(|record| {
                        record.meta_page.is_some() && retired.contains(&record.index_id)
                    })
                    .map(|record| record.index_id),
            )
            .collect();
        self.reusable_btree_pages = Some(ReusableBTreePageCache {
            candidates,
            retired,
            root_dependent,
        });
        Ok(())
    }

    pub(crate) fn claim_reusable_btree_page(
        &mut self,
        transaction: &mut Transaction,
        owner: IndexId,
    ) -> Result<Option<Page>, StorageError> {
        self.validate_transaction(transaction)?;
        transaction.acquire_writer()?;
        if !self.indexes.iter().any(|index| index.id == owner)
            && !transaction.index_state.building.contains(&owner)
        {
            return Err(IndexError::UnknownIndexId(owner).into());
        }
        self.ensure_reusable_btree_pages(transaction)?;
        let cache = self
            .reusable_btree_pages
            .as_mut()
            .ok_or(IndexError::InvalidNodeType)?;
        let mut chosen = None;
        for (id, candidate) in &cache.candidates {
            if transaction
                .retired_btree_pages
                .contains(&candidate.old_page_ref)
            {
                continue;
            }
            if candidate.source == PageReuseClass::GenerationSafeBTreeV3
                && (!cache.retired.contains(&candidate.retired_index_id)
                    || candidate.retired_index_id == owner)
            {
                return Err(IndexError::InvalidNodeType.into());
            }
            let Some(page) = self.buffer.reusable_page_snapshot(*id)? else {
                continue;
            };
            let (actual, actual_owner) = crate::allocation_transition::identity(&page)?;
            if actual != candidate.old_page_ref
                || actual_owner != candidate.retired_index_id
                || crate::allocation_transition::is_retired(&page)?
                    != (candidate.source == PageReuseClass::RetiredBTreeMarker)
            {
                return Err(crate::invalid_format(
                    "reusable BTree candidate identity changed",
                ));
            }
            chosen = Some((page, candidate.retired_index_id, candidate.source));
            break;
        }
        let Some((page, retired_owner, source)) = chosen else {
            return Ok(None);
        };
        let convert = source == PageReuseClass::GenerationSafeBTreeV3
            && cache.root_dependent.contains(&retired_owner);
        if convert {
            self.detach_retired_owner_root(transaction, retired_owner)?;
        }
        let cache = self
            .reusable_btree_pages
            .as_mut()
            .ok_or(IndexError::InvalidNodeType)?;
        if convert {
            cache.root_dependent.remove(&retired_owner);
        }
        cache.candidates.remove(&page.id);
        Ok(Some(page))
    }

    /// Convert only an owner we will actually claim. Pinned/dirty-only scans
    /// never rewrite catalog metadata. The conversion precedes transition WAL
    /// and is restored after the transition if the transaction rolls back.
    fn detach_retired_owner_root(
        &mut self,
        transaction: &mut Transaction,
        owner: IndexId,
    ) -> Result<(), StorageError> {
        let catalog = self.read_index_catalog(self.index_catalog_root)?;
        for page_id in catalog.pages {
            let guard = self.buffer.read_page(page_id)?;
            let before = guard.page().clone();
            let mut node = decode_index_catalog(before.single_payload(PageType::IndexCatalog)?)?;
            drop(guard);
            let mut changed = false;
            node.entries.retain(|entry| {
                if entry.retired && entry.definition.id == owner {
                    node.pending.push(RetiredIndexOwnership {
                        index_id: owner,
                        meta_page: None,
                    });
                    changed = true;
                    false
                } else {
                    true
                }
            });
            for pending in &mut node.pending {
                if pending.index_id == owner && pending.meta_page.is_some() {
                    pending.meta_page = None;
                    changed = true;
                }
            }
            if changed {
                self.write_catalog_node_in(transaction, page_id, before, node)?;
            }
        }
        Ok(())
    }
}
