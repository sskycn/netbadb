//! Checkpoint-gated whole-retired-tree suffix maintenance.
use std::collections::{BTreeMap, HashSet};

use netbadb_index::{IndexCatalogNode, IndexError, RetiredIndexOwnership, TailReclaimIntent};
use netbadb_types::PageId;

use super::{CatalogSnapshot, HeapStorage, IndexPageInventory};
use crate::{Page, PageType, StorageError};

/// Unstable Core maintenance result; never a SQL or wire result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexTailReclaimReport {
    pub page_count_before: u64,
    pub page_count_after: u64,
    pub truncate_from: Option<PageId>,
    /// Geometric retired-v3 suffix before applying the whole-tree restriction.
    pub eligible_pages: u64,
    pub candidate_pages: u64,
    pub reclaimed_pages: u64,
    pub eligible_indexes: u64,
    pub reclaimed_indexes: u64,
    /// Owned retired v3 pages retained outside the whole-tree suffix.
    pub blocked_pages: u64,
    pub pending_indexes_remaining: u64,
    pub blocked_by_page: Option<PageId>,
}

struct TailPlan {
    report: IndexTailReclaimReport,
    covered: Vec<RetiredIndexOwnership>,
}

fn retired_identities(catalog: &CatalogSnapshot) -> Vec<RetiredIndexOwnership> {
    catalog
        .pending
        .iter()
        .copied()
        .chain(
            catalog
                .entries
                .iter()
                .filter(|entry| entry.retired && entry.definition.handle.owner.is_some())
                .map(|entry| RetiredIndexOwnership {
                    index_id: entry.definition.id,
                    meta_page: entry.definition.handle.meta_page,
                }),
        )
        .collect()
}

fn plan_tail(catalog: &CatalogSnapshot, inventory: &IndexPageInventory) -> TailPlan {
    let identities = retired_identities(catalog);
    let owners: HashSet<_> = identities
        .iter()
        .filter(|r| r.meta_page.generation().is_some())
        .map(|r| r.index_id)
        .collect();
    let pages: BTreeMap<_, _> = inventory
        .report
        .allocations
        .iter()
        .filter(|p| owners.contains(&p.owner))
        .map(|p| (p.page_ref.page_id.0, p.owner))
        .collect();
    let count = inventory.report.database_pages;
    let mut start = count;
    while start > 3 && pages.contains_key(&(start - 1)) {
        start -= 1;
    }
    let eligible_pages = count - start;
    // Excluding the highest page of any incomplete owner can expose another
    // incomplete owner. The boundary only advances, so this terminates.
    loop {
        let incomplete: HashSet<_> = pages.range(..start).map(|(_, owner)| *owner).collect();
        let next = pages
            .range(start..)
            .filter(|(_, owner)| incomplete.contains(owner))
            .map(|(id, _)| id + 1)
            .max()
            .unwrap_or(start);
        if next == start {
            break;
        }
        start = next;
    }
    let covered_owners: HashSet<_> = pages.range(start..).map(|(_, owner)| *owner).collect();
    let mut covered: Vec<_> = identities
        .into_iter()
        .filter(|r| covered_owners.contains(&r.index_id))
        .collect();
    covered.sort_by_key(|r| r.index_id);
    TailPlan {
        report: IndexTailReclaimReport {
            page_count_before: count,
            page_count_after: count,
            truncate_from: (start < count).then_some(PageId(start)),
            eligible_pages,
            candidate_pages: count - start,
            reclaimed_pages: 0,
            eligible_indexes: covered.len() as u64,
            reclaimed_indexes: 0,
            blocked_pages: pages.len() as u64 - (count - start),
            pending_indexes_remaining: inventory.report.pending_reclaim_indexes,
            blocked_by_page: start.checked_sub(1).map(PageId),
        },
        covered,
    }
}

impl HeapStorage {
    /// Reclaims only the maximal suffix of complete, fully validated retired v3
    /// trees. No candidate means no checkpoint or WAL/catalog mutation. After a
    /// persistence failure this instance requires reopen; it cannot resume writes.
    pub fn reclaim_retired_index_tail(&mut self) -> Result<IndexTailReclaimReport, StorageError> {
        self.transactions.ensure_checkpoint_safe()?;
        self.buffer.ensure_unpinned()?;
        self.buffer.validated_page_count()?;
        let catalog = self.read_index_catalog(self.index_catalog_root)?;
        let inventory = self.index_page_inventory(&catalog)?;
        let preflight = plan_tail(&catalog, &inventory);
        if preflight.covered.is_empty() {
            return Ok(preflight.report);
        }
        // Capacity rejection is also side-effect-free. A fixed root cannot grow
        // the file into its own candidate suffix to make room for an intent.
        let provisional = TailReclaimIntent {
            old_page_count: preflight.report.page_count_before,
            truncate_from: preflight
                .report
                .truncate_from
                .ok_or(IndexError::InvalidReclaimIntent)?
                .0,
            checkpoint_lsn: self.transactions.wal().borrow().next_lsn(),
            covered: preflight.covered,
        };
        self.intent_root_update(&provisional)?;
        self.tail_finalization_updates(&catalog, &provisional)?;
        self.checkpoint()?;
        #[cfg(test)]
        crate::crash_test::maybe_crash(crate::crash_test::TestCrashPoint::TailAfterCheckpoint);
        // Checkpoint can change WAL/cache state: never execute the preflight plan.
        self.buffer.validated_page_count()?;
        let catalog = self.read_index_catalog(self.index_catalog_root)?;
        let inventory = self.index_page_inventory(&catalog)?;
        let plan = plan_tail(&catalog, &inventory);
        let Some(start) = plan.report.truncate_from else {
            return Ok(plan.report);
        };
        self.buffer.ensure_clean_suffix(start.0)?;
        let intent = TailReclaimIntent {
            old_page_count: plan.report.page_count_before,
            truncate_from: start.0,
            checkpoint_lsn: self.transactions.wal().borrow().base_lsn(),
            covered: plan.covered,
        };
        if inventory.report.allocations.iter().any(|allocation| {
            allocation.page_ref.page_id.0 >= start.0
                && allocation.page_ref.generation.0 >= intent.checkpoint_lsn.0
        }) {
            return Err(IndexError::InvalidReclaimIntent.into());
        }
        let update = self.intent_root_update(&intent)?;
        self.tail_finalization_updates(&catalog, &intent)?;
        let result = (|| {
            self.persist_tail_catalog(vec![update], false)?;
            #[cfg(test)]
            crate::crash_test::maybe_crash(crate::crash_test::TestCrashPoint::TailIntentDurable);
            self.complete_tail_intent(&intent)?;
            let mut report = plan.report;
            report.page_count_after = self.buffer.validated_page_count()?;
            report.reclaimed_pages = report.page_count_before - report.page_count_after;
            report.reclaimed_indexes = intent.covered.len() as u64;
            report.pending_indexes_remaining -= report.reclaimed_indexes;
            self.retired_indexes
                .retain(|definition| !intent.covered.iter().any(|r| r.index_id == definition.id));
            #[cfg(test)]
            crate::crash_test::maybe_crash(crate::crash_test::TestCrashPoint::TailAfterCompletion);
            Ok(report)
        })();
        if result.is_err() {
            self.transactions.require_maintenance_recovery();
        }
        result
    }

    fn intent_root_update(&self, intent: &TailReclaimIntent) -> Result<(Page, Page), StorageError> {
        let before = self
            .buffer
            .read_page(self.index_catalog_root)?
            .page()
            .clone();
        let mut node =
            netbadb_index::decode_index_catalog(before.single_payload(PageType::IndexCatalog)?)?;
        if node.reclaim_intent.is_some() {
            return Err(IndexError::InvalidReclaimIntent.into());
        }
        node.reclaim_intent = Some(intent.clone());
        let payload = netbadb_index::encode_index_catalog(&node)?;
        if payload.len() > self.index_catalog_payload_capacity() {
            return Err(StorageError::ResourceLimit {
                resource: "fixed index catalog root reclaim intent bytes",
                limit: self.index_catalog_payload_capacity() as u64,
            });
        }
        let mut after = Page::new(before.id, PageType::IndexCatalog);
        after.initialize_single_payload(PageType::IndexCatalog, &payload)?;
        Ok((before, after))
    }

    /// Ordinary WAL transactions cover only existing catalog pages below the
    /// suffix. Root clear is published last and becomes durable atomically with
    /// removal of covered records. A loser restores the still-present intent.
    fn persist_tail_catalog(
        &mut self,
        mut updates: Vec<(Page, Page)>,
        finalizing: bool,
    ) -> Result<(), StorageError> {
        updates.sort_by_key(|(before, _)| (before.id == self.index_catalog_root, before.id));
        let mut transaction = self.begin_transaction()?;
        transaction.acquire_writer()?;
        #[cfg(test)]
        if finalizing && self.fail_tail == Some("finalize-log") {
            self.fail_tail = None;
            transaction.inject_partial_append_failure(0);
        }
        for (before, after) in &mut updates {
            transaction.log_page_update(before, after)?;
        }
        #[cfg(test)]
        crate::crash_test::maybe_crash(if finalizing {
            crate::crash_test::TestCrashPoint::TailFinalizeAfterLogs
        } else {
            crate::crash_test::TestCrashPoint::TailIntentAfterLogs
        });
        #[cfg(not(test))]
        let _ = finalizing;
        for (_, after) in updates {
            self.publish_page_image(after.id, after)?;
            #[cfg(test)]
            if finalizing
                && crate::crash_test::is_enabled(
                    crate::crash_test::TestCrashPoint::TailFinalizeAfterPagePublish,
                )
            {
                self.buffer.flush_all()?;
                crate::crash_test::maybe_crash(
                    crate::crash_test::TestCrashPoint::TailFinalizeAfterPagePublish,
                );
            }
        }
        #[cfg(test)]
        if !finalizing && self.fail_tail == Some("intent-sync") {
            self.fail_tail = None;
            self.transactions.wal().borrow_mut().inject_flush_failure();
        }
        transaction.commit()?;
        #[cfg(test)]
        if finalizing {
            crate::crash_test::maybe_crash(crate::crash_test::TestCrashPoint::TailFinalizeDurable);
        }
        self.buffer.flush_all()
    }

    pub(super) fn recover_tail_intent(&mut self) -> Result<(), StorageError> {
        let catalog = self.read_index_catalog(self.index_catalog_root)?;
        if let Some(intent) = catalog.reclaim_intent {
            let result = self.complete_tail_intent(&intent);
            if result.is_err() {
                self.transactions.require_maintenance_recovery();
            }
            result?;
        }
        Ok(())
    }

    fn complete_tail_intent(&mut self, intent: &TailReclaimIntent) -> Result<(), StorageError> {
        let catalog = self.read_index_catalog(self.index_catalog_root)?;
        self.validate_tail_intent_catalog(intent, &catalog)?;
        let count = self.buffer.validated_page_count()?;
        if count == intent.old_page_count {
            let inventory = self.index_page_inventory(&catalog)?;
            let plan = plan_tail(&catalog, &inventory);
            if plan.report.truncate_from != Some(PageId(intent.truncate_from))
                || plan.covered != intent.covered
            {
                return Err(IndexError::InvalidReclaimIntent.into());
            }
        }
        self.buffer.ensure_clean_suffix(intent.truncate_from)?;
        self.buffer.invalidate_suffix(intent.truncate_from)?;
        #[cfg(test)]
        crate::crash_test::maybe_crash(crate::crash_test::TestCrashPoint::TailAfterInvalidation);
        #[cfg(test)]
        if self.fail_tail == Some("truncate-sync") {
            self.fail_tail = None;
            self.buffer.inject_page_sync_failure();
        }
        if count == intent.old_page_count {
            self.buffer
                .truncate_to_page_count(count, intent.truncate_from)?;
        } else {
            // Reopen after set_len but before sync must establish durability too.
            self.buffer.flush_all()?;
        }
        #[cfg(test)]
        crate::crash_test::maybe_crash(crate::crash_test::TestCrashPoint::TailAfterFileSync);
        let covered: HashSet<_> = intent.covered.iter().map(|r| r.index_id).collect();
        // New-length recovery never reads a removed root. It does reject any
        // remaining page claiming a supposedly fully reclaimed owner.
        for number in 1..intent.truncate_from {
            let guard = self.buffer.read_page(PageId(number))?;
            let kind = guard.page().validated()?.header().page_type;
            if matches!(
                kind,
                PageType::BTreeMeta | PageType::BTreeInternal | PageType::BTreeLeaf
            ) && netbadb_index::btree_page_owner(guard.page().single_payload(kind)?)?
                .is_some_and(|owner| covered.contains(&owner))
            {
                return Err(IndexError::InvalidReclaimIntent.into());
            }
        }
        let updates = self.tail_finalization_updates(&catalog, intent)?;
        self.persist_tail_catalog(updates, true)
    }

    fn tail_finalization_updates(
        &self,
        catalog: &CatalogSnapshot,
        intent: &TailReclaimIntent,
    ) -> Result<Vec<(Page, Page)>, StorageError> {
        let covered: HashSet<_> = intent.covered.iter().map(|r| r.index_id).collect();
        let mut updates = Vec::new();
        for id in &catalog.pages {
            let before = self.buffer.read_page(*id)?.page().clone();
            let mut node: IndexCatalogNode = netbadb_index::decode_index_catalog(
                before.single_payload(PageType::IndexCatalog)?,
            )?;
            let changed = node.reclaim_intent.is_some()
                || node
                    .entries
                    .iter()
                    .any(|entry| covered.contains(&entry.definition.id))
                || node
                    .pending
                    .iter()
                    .any(|record| covered.contains(&record.index_id));
            // Do not upgrade unrelated legacy continuations during finalization.
            if !changed {
                continue;
            }
            node.entries
                .retain(|entry| !covered.contains(&entry.definition.id));
            node.pending
                .retain(|record| !covered.contains(&record.index_id));
            node.reclaim_intent = None;
            let payload = netbadb_index::encode_index_catalog(&node)?;
            let mut after = Page::new(*id, PageType::IndexCatalog);
            after.initialize_single_payload(PageType::IndexCatalog, &payload)?;
            updates.push((before, after));
        }
        Ok(updates)
    }

    pub(super) fn validate_tail_intent_catalog(
        &self,
        intent: &TailReclaimIntent,
        catalog: &CatalogSnapshot,
    ) -> Result<(), StorageError> {
        intent.validate()?;
        let count = self.buffer.validated_page_count()?;
        if (count != intent.old_page_count && count != intent.truncate_from)
            || self.transactions.wal().borrow().base_lsn() != intent.checkpoint_lsn
            || catalog.pages.iter().any(|id| id.0 >= intent.truncate_from)
        {
            return Err(IndexError::InvalidReclaimIntent.into());
        }
        let identities = retired_identities(catalog);
        for record in &intent.covered {
            if !identities.contains(record) {
                return Err(IndexError::InvalidReclaimIntent.into());
            }
        }
        for entry in &catalog.entries {
            if entry.definition.handle.meta_page.page_id().0 >= intent.truncate_from
                && (!entry.retired
                    || !intent.covered.contains(&RetiredIndexOwnership {
                        index_id: entry.definition.id,
                        meta_page: entry.definition.handle.meta_page,
                    }))
            {
                return Err(IndexError::InvalidReclaimIntent.into());
            }
        }
        for record in &catalog.pending {
            if record.meta_page.page_id().0 >= intent.truncate_from
                && !intent.covered.contains(record)
            {
                return Err(IndexError::InvalidReclaimIntent.into());
            }
        }
        Ok(())
    }
}
