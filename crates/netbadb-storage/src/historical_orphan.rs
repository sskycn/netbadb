//! Explicit adoption of historical ordinary v3 orphans, never startup repair.
use std::collections::{BTreeMap, HashSet};

use netbadb_index::{BTreeHandle, IndexError};
use netbadb_types::{IndexId, PageRef};

use super::ownership::OwnershipClass;
use super::{HeapStorage, IndexPageInventory};
use crate::{Page, PageType, StorageError, Transaction, TransactionError};

/// Unstable administrative result, not ordinary catalog inspection or SQL.
/// One invocation commits all eligible pages in one physical transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoricalOrphanAdoptionReport {
    /// Active registered generation-safe trees; legacy trees are only validated.
    pub indexes_scanned: u64,
    /// Physical file pages, including the Heap header. A nonempty operation
    /// validates the file twice (preflight and authoritative post-checkpoint).
    pub pages_scanned: u64,
    pub candidates: u64,
    pub adopted: u64,
    pub already_retired_markers: u64,
    /// Remaining eligible ordinary v3 active-owner orphans, not legacy pages.
    pub historical_remaining: u64,
}

struct HistoricalCandidate {
    owner: IndexId,
    reference: PageRef,
    kind: PageType,
    // Exact byte fingerprint includes CRC, pageLSN and dormant node payload.
    // Also bounds plan memory to one page per candidate, not full WAL history.
    before: Page,
}

pub(super) struct HistoricalPlan {
    candidates: Vec<HistoricalCandidate>,
    report: HistoricalOrphanAdoptionReport,
}

impl HeapStorage {
    /// Adopts only fully validated, unreachable ordinary v3 nodes belonging to
    /// currently registered active indexes. No Heap, Catalog, raw/legacy page,
    /// retired owner, meta page or already-retired marker is adopted.
    ///
    /// Requires the existing checkpoint gate and no buffer pins. A full
    /// preflight with zero candidates returns without checkpoint or WAL writes.
    /// Otherwise checkpoint is internal, followed by a fresh full-file/tree
    /// proof and clean/unpinned exact-image validation of EVERY candidate before
    /// BEGIN. The exclusive synchronous borrow and dedicated writer prevent
    /// intervening structural mutations. Adoption preserves PageRef and owner.
    ///
    /// All markers commit together using ordinary PageUpdate undo/redo. Success
    /// flushes committed markers so the next allocator can immediately reuse
    /// them. Persistence failures with an unresolved outcome require reopen;
    /// an error after COMMIT may still have committed all adoptions.
    pub fn adopt_historical_btree_orphans(
        &mut self,
    ) -> Result<HistoricalOrphanAdoptionReport, StorageError> {
        self.transactions.ensure_checkpoint_safe()?;
        self.buffer.ensure_unpinned()?;
        let (active, inventory) = self.historical_inventory()?;
        let preflight = historical_candidates(&active, &inventory)?;
        if preflight.is_empty() {
            return Ok(historical_report(active.len(), &inventory, 0));
        }
        let next_lsn = self
            .transactions
            .wal()
            .try_borrow()
            .map_err(|_| TransactionError::WalBusy)?
            .next_lsn();
        if preflight
            .iter()
            .any(|(_, reference, _)| reference.generation.0 >= next_lsn.0)
        {
            return Err(crate::invalid_format(
                "historical generation exceeds WAL high-water",
            ));
        }
        // Never use preflight candidates as authority after checkpoint.
        #[cfg(test)]
        crate::crash_test::maybe_crash(crate::crash_test::TestCrashPoint::AdoptionBeforeCheckpoint);
        if let Err(error) = self.checkpoint() {
            self.transactions.require_maintenance_recovery();
            return Err(error);
        }
        #[cfg(test)]
        crate::crash_test::maybe_crash(crate::crash_test::TestCrashPoint::AdoptionAfterCheckpoint);
        let plan = self.historical_adoption_plan()?;
        if plan.candidates.is_empty() {
            return Ok(plan.report);
        }
        let mut transaction = self.begin_transaction()?;
        transaction.acquire_writer()?;
        if let Err(error) = self.apply_historical_adoption(&mut transaction, &plan) {
            transaction.require_rollback();
            if let Err(rollback_error) = transaction.rollback() {
                self.transactions.require_maintenance_recovery();
                return Err(rollback_error);
            }
            return Err(error);
        }
        #[cfg(test)]
        if self.fail_adoption == Some("commit-sync") {
            self.fail_adoption = None;
            self.transactions.wal().borrow_mut().inject_flush_failure();
        }
        if let Err(error) = transaction.commit() {
            self.transactions.require_maintenance_recovery();
            return Err(error);
        }
        drop(transaction);
        #[cfg(test)]
        crate::crash_test::maybe_crash(crate::crash_test::TestCrashPoint::AdoptionAfterCommit);
        // Commit invalidates the existing disposable inventory before releasing
        // its writer. Flush only AFTER commit: no uncommitted marker is reusable.
        if let Err(error) = self.flush() {
            self.transactions.require_maintenance_recovery();
            return Err(error);
        }
        Ok(HistoricalOrphanAdoptionReport {
            adopted: plan.report.candidates,
            historical_remaining: 0,
            ..plan.report
        })
    }

    fn historical_inventory(
        &mut self,
    ) -> Result<(BTreeMap<IndexId, BTreeHandle>, IndexPageInventory), StorageError> {
        let catalog = self.read_index_catalog(self.index_catalog_root)?;
        if catalog.reclaim_intent.is_some() {
            return Err(IndexError::InvalidReclaimIntent.into());
        }
        // The durable registry is authoritative, but ordinary execution's
        // published registry must agree before maintenance can establish proof.
        if !catalog
            .entries
            .iter()
            .filter(|entry| !entry.retired)
            .map(|entry| &entry.definition)
            .eq(self.indexes.iter())
        {
            return Err(crate::invalid_format(
                "historical adoption registry mismatch",
            ));
        }
        let active = catalog
            .entries
            .iter()
            .filter(|entry| {
                !entry.retired && entry.definition.handle.meta_page.generation().is_some()
            })
            .map(|entry| (entry.definition.id, entry.definition.handle))
            .collect();
        // Includes all active, retained retired and raw roots in one global
        // overlap set, full child traversal plus exact leaf-chain cross-check,
        // and complete decoding of every ordinary orphan and existing marker.
        let inventory = self.index_page_inventory(&catalog)?;
        Ok((active, inventory))
    }

    pub(super) fn historical_adoption_plan(&mut self) -> Result<HistoricalPlan, StorageError> {
        self.transactions.ensure_checkpoint_safe()?;
        self.buffer.ensure_unpinned()?;
        // A post-checkpoint dirty frame is an invariant failure. Reject before
        // traversal can evict/write it back and conceal that failed precondition.
        self.buffer.ensure_clean_suffix(1)?;
        let (active, inventory) = self.historical_inventory()?;
        let identities = historical_candidates(&active, &inventory)?;
        // Independent protection of stable meta AND current root even if a
        // future inventory refactor accidentally omits either from reachability.
        let mut protected = HashSet::new();
        for handle in active.values() {
            protected.insert(handle.meta_page.page_id());
            protected.insert(self.btree().read_meta(*handle)?.root_page.page_id());
        }
        let horizon = self
            .transactions
            .wal()
            .try_borrow()
            .map_err(|_| TransactionError::WalBusy)?
            .base_lsn();
        let mut candidates = Vec::with_capacity(identities.len());
        for (owner, reference, kind) in identities {
            if protected.contains(&reference.page_id) {
                return Err(IndexError::InvalidChild(reference.page_id).into());
            }
            let before = self.buffer.maintenance_page_snapshot(reference.page_id)?;
            let candidate = HistoricalCandidate {
                owner,
                reference,
                kind,
                before,
            };
            validate_candidate(&candidate, &candidate.before)?;
            if reference.generation.0 >= horizon.0
                || candidate
                    .before
                    .page_lsn()?
                    .is_some_and(|lsn| lsn >= horizon)
            {
                return Err(crate::invalid_format(
                    "historical adoption exceeds checkpoint horizon",
                ));
            }
            candidates.push(candidate);
        }
        // WAL has a per-record bound, not a transaction page/count bound. Each
        // update is exactly the existing maximum full before/after record.
        // Conservatively allow Begin + Commit OR Abort + RollbackComplete too.
        let records = (candidates.len() as u64)
            .checked_add(3)
            .and_then(|n| n.checked_mul(crate::wal::WAL_MAX_RECORD_SIZE as u64))
            .and_then(|bytes| horizon.0.checked_add(bytes));
        if records.is_none() {
            return Err(IndexError::LengthOverflow.into());
        }
        let report = historical_report(active.len(), &inventory, candidates.len());
        Ok(HistoricalPlan { candidates, report })
    }

    pub(super) fn apply_historical_adoption(
        &mut self,
        transaction: &mut Transaction,
        plan: &HistoricalPlan,
    ) -> Result<(), StorageError> {
        // All-candidate preconditions precede the first marker WAL append.
        for candidate in &plan.candidates {
            self.validate_historical_candidate(candidate)?;
        }
        for candidate in &plan.candidates {
            self.validate_historical_candidate(candidate)?;
            self.btree()
                .retire_btree_page_in(transaction, candidate.reference, candidate.owner)?;
            #[cfg(test)]
            if transaction.retired_btree_pages.len() == 40 {
                if self.fail_adoption == Some("after40") {
                    self.fail_adoption = None;
                    return Err(crate::invalid_format(
                        "injected adoption publication failure",
                    ));
                }
                crate::crash_test::maybe_crash(
                    crate::crash_test::TestCrashPoint::AdoptionAfterManyPages,
                );
            }
        }
        Ok(())
    }

    fn validate_historical_candidate(
        &self,
        candidate: &HistoricalCandidate,
    ) -> Result<(), StorageError> {
        if !self.indexes.iter().any(|index| {
            index.id == candidate.owner
                && index.handle.meta_page.page_id() != candidate.reference.page_id
                && index.handle.meta_page.generation().is_some()
        }) {
            return Err(IndexError::UnknownIndexId(candidate.owner).into());
        }
        let current = self
            .buffer
            .maintenance_page_snapshot(candidate.reference.page_id)?;
        validate_candidate(candidate, &current)
    }
}

fn historical_candidates(
    active: &BTreeMap<IndexId, BTreeHandle>,
    inventory: &IndexPageInventory,
) -> Result<Vec<(IndexId, PageRef, PageType)>, StorageError> {
    let mut candidates = Vec::new();
    let mut seen = HashSet::new();
    for page in &inventory.observations {
        if page.class != OwnershipClass::Active || page.reachable || page.generation.is_none() {
            continue;
        }
        let owner = page.owner.ok_or(IndexError::InvalidNodeType)?;
        let handle = active
            .get(&owner)
            .ok_or(IndexError::UnknownIndexId(owner))?;
        if handle.meta_page.page_id() == page.page_id
            || !matches!(page.kind, PageType::BTreeLeaf | PageType::BTreeInternal)
            || inventory.markers.contains(&page.page_id)
            || !seen.insert(page.page_id)
        {
            return Err(IndexError::InvalidNodeType.into());
        }
        candidates.push((
            owner,
            PageRef {
                page_id: page.page_id,
                generation: page.generation.ok_or(IndexError::InvalidNodeType)?,
            },
            page.kind,
        ));
    }
    candidates.sort_by_key(|(_, reference, _)| reference.page_id);
    Ok(candidates)
}

fn validate_candidate(candidate: &HistoricalCandidate, current: &Page) -> Result<(), StorageError> {
    let (reference, owner) = crate::allocation_transition::identity(current)?;
    if reference != candidate.reference
        || owner != candidate.owner
        || current.validated()?.header().page_type != candidate.kind
        || crate::allocation_transition::is_retired(current)?
        || current.bytes() != candidate.before.bytes()
    {
        return Err(crate::invalid_format(
            "historical adoption candidate changed",
        ));
    }
    Ok(())
}

fn historical_report(
    indexes: usize,
    inventory: &IndexPageInventory,
    candidates: usize,
) -> HistoricalOrphanAdoptionReport {
    HistoricalOrphanAdoptionReport {
        indexes_scanned: indexes as u64,
        pages_scanned: inventory.report.database_pages,
        candidates: candidates as u64,
        adopted: 0,
        already_retired_markers: inventory.report.retired_marker_pages,
        historical_remaining: candidates as u64,
    }
}
