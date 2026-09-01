//! Ordered reservation/intention history; NBSC floors + this history are one allocator.
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use netbadb_schema::TypeSpec;
use netbadb_types::{
    ColumnId, DatabaseTxnId, PhysicalType, SchemaGeneration, SemanticType, StorageId, TableId,
};

use crate::schema_catalog::{Reader, SchemaCatalogSnapshot, Writer, envelope, open_envelope};
use crate::schema_catalog_file as file;
use crate::schema_mutation::{AlterTableOperation, SchemaMutationError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Reservation {
    pub(crate) transaction: DatabaseTxnId,
    pub(crate) table: TableId,
    pub(crate) storage: StorageId,
    pub(crate) base_generation: SchemaGeneration,
    pub(crate) base_epoch: u64,
    pub(crate) intent: Option<CreateIntent>,
    pub(crate) resolved: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CreateIntent {
    // Only the new table, never a repeated full database schema.
    pub(crate) fragment: SchemaCatalogSnapshot,
    pub(crate) snapshot_digest: [u8; 32],
}

/// Durable description of one logically dropped Heap. The single-table
/// fragment preserves the exact retired logical/physical identity for recovery;
/// it is never consulted by normal schema lookup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DropIntent {
    pub(crate) transaction: DatabaseTxnId,
    pub(crate) fragment: SchemaCatalogSnapshot,
    pub(crate) target_generation: SchemaGeneration,
    pub(crate) target_epoch: u64,
    pub(crate) snapshot_digest: [u8; 32],
    pub(crate) retired: bool,
    pub(crate) resolved: Option<bool>,
    pub(crate) gc: Option<RetiredHeapGcRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RewriteReservation {
    pub(crate) transaction: DatabaseTxnId,
    pub(crate) table: TableId,
    pub(crate) storage: StorageId,
    pub(crate) column: Option<ColumnId>,
    pub(crate) base_generation: SchemaGeneration,
    pub(crate) base_epoch: u64,
}

/// Durable old/new logical and physical identity for one Heap replacement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RewriteIntent {
    pub(crate) reservation: RewriteReservation,
    pub(crate) operation: AlterTableOperation,
    pub(crate) base: SchemaCatalogSnapshot,
    pub(crate) target: SchemaCatalogSnapshot,
    pub(crate) snapshot_digest: [u8; 32],
    pub(crate) retired: bool,
    pub(crate) resolved: Option<bool>,
}

impl RewriteIntent {
    pub(crate) fn old_storage(&self) -> StorageId {
        self.base.storages[0].id
    }

    pub(crate) fn new_storage(&self) -> StorageId {
        self.reservation.storage
    }

    pub(crate) fn table(&self) -> TableId {
        self.reservation.table
    }
}

/// Durable retry-only physical deletion state. The surrounding DropIntent
/// already carries the exact database incarnation, table/version/fingerprint,
/// StorageId, locator and retirement transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RetiredHeapGcRecord {
    pub(crate) coordinator_horizon: DatabaseTxnId,
    pub(crate) manifest_digest: [u8; 32],
    pub(crate) complete: bool,
}

impl DropIntent {
    pub(crate) fn table(&self) -> TableId {
        self.fragment.committed.schema.tables()[0].id
    }

    pub(crate) fn storage(&self) -> StorageId {
        self.fragment.storages[0].id
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SchemaMutationJournal {
    pub(crate) path: PathBuf,
    pub(crate) incarnation: [u8; 16],
    pub(crate) coordinator: String,
    pub(crate) reservations: BTreeMap<DatabaseTxnId, Reservation>,
    pub(crate) drops: BTreeMap<DatabaseTxnId, DropIntent>,
    pub(crate) rewrite_reservations: BTreeMap<DatabaseTxnId, RewriteReservation>,
    pub(crate) rewrites: BTreeMap<DatabaseTxnId, RewriteIntent>,
    pub(crate) rewrite_losers: BTreeSet<DatabaseTxnId>,
    poisoned: bool,
    #[cfg(test)]
    fail_next_sync: bool,
}

fn corrupt(reason: &'static str) -> SchemaMutationError {
    SchemaMutationError::Corrupt(reason)
}

pub(crate) fn namespace(
    catalog: &Path,
    incarnation: [u8; 16],
) -> Result<String, SchemaMutationError> {
    let name = catalog
        .file_name()
        .and_then(|s| s.to_str())
        .ok_or(corrupt("catalog filename is not UTF-8"))?;
    let identity = incarnation
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    Ok(format!("{name}.resources-{identity}"))
}

pub(crate) fn stage_locator(
    catalog: &Path,
    incarnation: [u8; 16],
    txn: DatabaseTxnId,
    storage: StorageId,
) -> Result<String, SchemaMutationError> {
    Ok(format!(
        "{}/staging/{}/{}.heap",
        namespace(catalog, incarnation)?,
        txn.0,
        storage.0
    ))
}
pub(crate) fn final_locator(
    catalog: &Path,
    incarnation: [u8; 16],
    storage: StorageId,
) -> Result<String, SchemaMutationError> {
    Ok(format!(
        "{}/storage/{}.heap",
        namespace(catalog, incarnation)?,
        storage.0
    ))
}
pub(crate) fn prepared_locator(
    catalog: &Path,
    incarnation: [u8; 16],
    txn: DatabaseTxnId,
) -> Result<String, SchemaMutationError> {
    Ok(format!(
        "{}/staging/{}/catalog.nbsc",
        namespace(catalog, incarnation)?,
        txn.0
    ))
}

impl SchemaMutationJournal {
    pub(crate) fn open(
        catalog: &Path,
        incarnation: [u8; 16],
    ) -> Result<Option<Self>, SchemaMutationError> {
        let path = file::suffix(catalog, ".mutations");
        let witness = file::suffix(&path, ".state");
        let exists = path
            .try_exists()
            .map_err(|e| file::io("inspect mutation journal", &path, e))?;
        let activated = witness
            .try_exists()
            .map_err(|e| file::io("inspect mutation activation", &witness, e))?;
        if !exists && !activated {
            return Ok(None);
        }
        if !exists {
            return Err(corrupt("activated mutation journal is missing"));
        }
        let mut journal = Self::decode(&file::read(&path)?)?;
        if journal.incarnation != incarnation {
            return Err(corrupt("mutation journal incarnation mismatch"));
        }
        journal.path = path;
        for reservation in journal.reservations.values() {
            if let Some(intent) = &reservation.intent {
                if intent.fragment.storages[0].locator
                    != final_locator(catalog, incarnation, reservation.storage)?
                {
                    return Err(corrupt(
                        "intent final locator differs from reserved identity",
                    ));
                }
            }
        }
        for rewrite in journal.rewrites.values() {
            if rewrite.target.storages[0].locator
                != final_locator(catalog, incarnation, rewrite.new_storage())?
                || rewrite.base.storages[0].locator
                    != final_locator(catalog, incarnation, rewrite.old_storage())?
            {
                return Err(corrupt("rewrite locator differs from physical identity"));
            }
        }
        if activated {
            if open_envelope(&file::read(&witness)?, b"NBSA")? != incarnation {
                return Err(corrupt("mutation activation incarnation mismatch"));
            }
        } else if !journal.reservations.is_empty()
            || !journal.drops.is_empty()
            || !journal.rewrite_reservations.is_empty()
        {
            return Err(corrupt("reservation history has no activation witness"));
        }
        Ok(Some(journal))
    }

    pub(crate) fn initialize(
        catalog: &Path,
        incarnation: [u8; 16],
        coordinator: String,
    ) -> Result<Self, SchemaMutationError> {
        let journal = match Self::open(catalog, incarnation)? {
            Some(journal) => {
                if journal.coordinator != coordinator {
                    return Err(corrupt("coordinator locator changed"));
                }
                journal
            }
            None => {
                let journal = Self {
                    path: file::suffix(catalog, ".mutations"),
                    incarnation,
                    coordinator,
                    reservations: BTreeMap::new(),
                    drops: BTreeMap::new(),
                    rewrite_reservations: BTreeMap::new(),
                    rewrites: BTreeMap::new(),
                    rewrite_losers: BTreeSet::new(),
                    poisoned: false,
                    #[cfg(test)]
                    fail_next_sync: false,
                };
                file::atomic_write(&journal.path, &journal.encode()?, false)?;
                journal
            }
        };
        journal.activate()?;
        Ok(journal)
    }

    fn activate(&self) -> Result<(), SchemaMutationError> {
        let witness = file::suffix(&self.path, ".state");
        if !witness
            .try_exists()
            .map_err(|e| file::io("inspect mutation activation", &witness, e))?
        {
            file::atomic_write(&witness, &envelope(b"NBSA", &self.incarnation)?, false)?;
        }
        Ok(())
    }

    pub(crate) fn prepare_reservation(
        &self,
        reservation: &Reservation,
        intent: &CreateIntent,
    ) -> Result<(), SchemaMutationError> {
        self.ensure_ready()?;
        // Reserve room for the entire obligation before consuming an identity:
        // a full journal must never prevent winner/loser resolution.
        let mut complete = self.clone();
        let mut projected = reservation.clone();
        projected.intent = Some(intent.clone());
        projected.resolved = Some(false);
        complete
            .reservations
            .insert(projected.transaction, projected);
        complete.encode()?;
        // An empty journal may survive a crash before its activation witness.
        // Reopened handles must finish activation before their first reservation.
        self.activate()
    }

    pub(crate) fn prepare_drop(&self, intent: &DropIntent) -> Result<(), SchemaMutationError> {
        self.ensure_ready()?;
        if self.reservations.contains_key(&intent.transaction)
            || self.drops.contains_key(&intent.transaction)
        {
            return Err(corrupt("duplicate schema mutation transaction"));
        }
        let mut complete = self.clone();
        let mut projected = intent.clone();
        projected.retired = true;
        projected.resolved = Some(true);
        complete.drops.insert(projected.transaction, projected);
        complete.encode()?;
        self.activate()
    }

    pub(crate) fn prepare_rewrite(
        &self,
        reservation: &RewriteReservation,
        intent: &RewriteIntent,
    ) -> Result<(), SchemaMutationError> {
        self.ensure_ready()?;
        if self.reservations.contains_key(&reservation.transaction)
            || self.drops.contains_key(&reservation.transaction)
            || self
                .rewrite_reservations
                .contains_key(&reservation.transaction)
            || reservation != &intent.reservation
        {
            return Err(corrupt("duplicate or mismatched rewrite transaction"));
        }
        let mut complete = self.clone();
        complete
            .rewrite_reservations
            .insert(reservation.transaction, reservation.clone());
        let mut projected = intent.clone();
        projected.retired = true;
        projected.resolved = Some(true);
        complete.rewrites.insert(reservation.transaction, projected);
        complete.encode()?;
        self.activate()
    }

    pub(crate) fn prepare_gc(
        &self,
        txn: DatabaseTxnId,
        gc: &RetiredHeapGcRecord,
    ) -> Result<(), SchemaMutationError> {
        self.ensure_ready()?;
        let drop = self
            .drops
            .get(&txn)
            .ok_or(corrupt("GC intent without drop history"))?;
        if !drop.retired || drop.resolved != Some(true) || drop.gc.is_some() || gc.complete {
            return Err(corrupt("out-of-order GC intent"));
        }
        // Reserve both terminal records before the first unlink. Capacity can
        // therefore never strand a durable deleting state without Complete.
        let mut projected = self.clone();
        let projected_drop = projected
            .drops
            .get_mut(&txn)
            .ok_or(corrupt("projected GC drop disappeared"))?;
        projected_drop.gc = Some(RetiredHeapGcRecord {
            complete: true,
            ..gc.clone()
        });
        projected.encode()?;
        Ok(())
    }

    pub(crate) fn effective_table(&self, floor: Option<TableId>) -> Option<TableId> {
        let floor = floor?;
        self.reservations
            .values()
            .map(|r| r.table.0)
            .max()
            .map_or(Some(floor), |max| {
                max.checked_add(1).map(|n| TableId(n.max(floor.0)))
            })
    }
    pub(crate) fn effective_storage(&self, floor: Option<StorageId>) -> Option<StorageId> {
        let floor = floor?;
        self.reservations
            .values()
            .map(|r| r.storage.0)
            .chain(
                self.rewrite_reservations
                    .values()
                    .map(|reservation| reservation.storage.0),
            )
            .max()
            .map_or(Some(floor), |max| {
                max.checked_add(1).map(|n| StorageId(n.max(floor.0)))
            })
    }
    pub(crate) fn effective_column(
        &self,
        table: TableId,
        floor: Option<ColumnId>,
    ) -> Option<ColumnId> {
        let floor = floor?;
        self.rewrite_reservations
            .values()
            .filter(|reservation| reservation.table == table)
            .filter_map(|reservation| reservation.column)
            .map(|column| column.0)
            .max()
            .map_or(Some(floor), |maximum| {
                maximum
                    .checked_add(1)
                    .map(|next| ColumnId(next.max(floor.0)))
            })
    }
    pub(crate) fn ensure_ready(&self) -> Result<(), SchemaMutationError> {
        if self.poisoned {
            Err(SchemaMutationError::RecoveryRequired)
        } else {
            Ok(())
        }
    }
    pub(crate) fn reserve(&mut self, reservation: Reservation) -> Result<(), SchemaMutationError> {
        self.ensure_ready()?;
        if self.reservations.contains_key(&reservation.transaction) {
            return Err(corrupt("duplicate reservation transaction"));
        }
        self.reservations
            .insert(reservation.transaction, reservation);
        self.persist()
    }
    pub(crate) fn reserve_rewrite(
        &mut self,
        reservation: RewriteReservation,
    ) -> Result<(), SchemaMutationError> {
        self.ensure_ready()?;
        if self.reservations.contains_key(&reservation.transaction)
            || self.drops.contains_key(&reservation.transaction)
            || self
                .rewrite_reservations
                .contains_key(&reservation.transaction)
        {
            return Err(corrupt("duplicate rewrite reservation"));
        }
        self.rewrite_reservations
            .insert(reservation.transaction, reservation);
        self.persist()
    }
    pub(crate) fn rewrite_intent(
        &mut self,
        intent: RewriteIntent,
    ) -> Result<(), SchemaMutationError> {
        self.ensure_ready()?;
        let reservation = self
            .rewrite_reservations
            .get(&intent.reservation.transaction)
            .ok_or(corrupt("rewrite intent without reservation"))?;
        if reservation != &intent.reservation
            || self.rewrites.contains_key(&intent.reservation.transaction)
            || self
                .rewrite_losers
                .contains(&intent.reservation.transaction)
        {
            return Err(corrupt("duplicate or mismatched rewrite intent"));
        }
        self.rewrites.insert(intent.reservation.transaction, intent);
        self.persist()
    }

    pub(crate) fn retire_rewrite(&mut self, txn: DatabaseTxnId) -> Result<(), SchemaMutationError> {
        self.ensure_ready()?;
        let intent = self
            .rewrites
            .get_mut(&txn)
            .ok_or(corrupt("replacement retirement without rewrite intent"))?;
        if intent.resolved == Some(false) {
            return Err(corrupt("loser rewrite cannot retire source"));
        }
        if intent.retired {
            return Ok(());
        }
        intent.retired = true;
        self.persist()
    }

    pub(crate) fn resolve_rewrite(
        &mut self,
        txn: DatabaseTxnId,
        committed: bool,
    ) -> Result<(), SchemaMutationError> {
        self.ensure_ready()?;
        if !committed && !self.rewrites.contains_key(&txn) {
            if !self.rewrite_reservations.contains_key(&txn) {
                return Err(corrupt("rewrite resolution without reservation"));
            }
            if !self.rewrite_losers.insert(txn) {
                return Ok(());
            }
            return self.persist();
        }
        let intent = self
            .rewrites
            .get_mut(&txn)
            .ok_or(corrupt("rewrite winner without intent"))?;
        if committed && !intent.retired {
            return Err(corrupt("rewrite winner source is not durably retired"));
        }
        if !committed && intent.retired {
            return Err(corrupt("retired rewrite cannot resolve as loser"));
        }
        if let Some(previous) = intent.resolved {
            return if previous == committed {
                Ok(())
            } else {
                Err(corrupt("conflicting rewrite resolution"))
            };
        }
        intent.resolved = Some(committed);
        self.persist()
    }
    pub(crate) fn intent(
        &mut self,
        txn: DatabaseTxnId,
        intent: CreateIntent,
    ) -> Result<(), SchemaMutationError> {
        self.ensure_ready()?;
        let reservation = self
            .reservations
            .get_mut(&txn)
            .ok_or(corrupt("intent without reservation"))?;
        if reservation.intent.is_some() || reservation.resolved.is_some() {
            return Err(corrupt("out-of-order create intent"));
        }
        reservation.intent = Some(intent);
        self.persist()
    }

    pub(crate) fn drop_intent(&mut self, intent: DropIntent) -> Result<(), SchemaMutationError> {
        self.ensure_ready()?;
        if self.reservations.contains_key(&intent.transaction)
            || self.drops.contains_key(&intent.transaction)
        {
            return Err(corrupt("duplicate schema mutation transaction"));
        }
        self.drops.insert(intent.transaction, intent);
        self.persist()
    }

    pub(crate) fn retire_drop(&mut self, txn: DatabaseTxnId) -> Result<(), SchemaMutationError> {
        self.ensure_ready()?;
        let intent = self
            .drops
            .get_mut(&txn)
            .ok_or(corrupt("retirement without drop intent"))?;
        if intent.resolved == Some(false) {
            return Err(corrupt("loser drop cannot retire storage"));
        }
        if intent.retired {
            return Ok(());
        }
        intent.retired = true;
        self.persist()
    }

    pub(crate) fn resolve_drop(
        &mut self,
        txn: DatabaseTxnId,
        committed: bool,
    ) -> Result<(), SchemaMutationError> {
        self.ensure_ready()?;
        let intent = self
            .drops
            .get_mut(&txn)
            .ok_or(corrupt("drop resolution without intent"))?;
        if committed && !intent.retired {
            return Err(corrupt("drop winner is not durably retired"));
        }
        if !committed && intent.retired {
            return Err(corrupt("retired drop cannot resolve as loser"));
        }
        if let Some(previous) = intent.resolved {
            return if previous == committed {
                Ok(())
            } else {
                Err(corrupt("conflicting drop resolution"))
            };
        }
        intent.resolved = Some(committed);
        self.persist()
    }

    pub(crate) fn gc_intent(
        &mut self,
        txn: DatabaseTxnId,
        gc: RetiredHeapGcRecord,
    ) -> Result<(), SchemaMutationError> {
        self.prepare_gc(txn, &gc)?;
        self.drops
            .get_mut(&txn)
            .ok_or(corrupt("GC intent without drop history"))?
            .gc = Some(gc);
        self.persist()
    }

    pub(crate) fn complete_gc(&mut self, txn: DatabaseTxnId) -> Result<(), SchemaMutationError> {
        self.ensure_ready()?;
        let gc = self
            .drops
            .get_mut(&txn)
            .and_then(|drop| drop.gc.as_mut())
            .ok_or(corrupt("GC complete without intent"))?;
        if gc.complete {
            return Ok(());
        }
        gc.complete = true;
        self.persist()
    }
    pub(crate) fn resolve(
        &mut self,
        txn: DatabaseTxnId,
        committed: bool,
    ) -> Result<(), SchemaMutationError> {
        self.ensure_ready()?;
        let reservation = self
            .reservations
            .get_mut(&txn)
            .ok_or(corrupt("resolution without reservation"))?;
        if let Some(previous) = reservation.resolved {
            return if previous == committed {
                Ok(())
            } else {
                Err(corrupt("conflicting mutation resolution"))
            };
        }
        reservation.resolved = Some(committed);
        self.persist()
    }
    #[cfg(test)]
    pub(crate) fn inject_sync_failure(&mut self) {
        self.fail_next_sync = true;
    }

    fn persist(&mut self) -> Result<(), SchemaMutationError> {
        // An uncertain metadata rename/sync consumes the in-process allocation and
        // poisons further writes. Reopen selects the complete durable history.
        let result = self
            .encode()
            .and_then(|bytes| file::atomic_write(&self.path, &bytes, false).map_err(Into::into));
        #[cfg(test)]
        let result = if result.is_ok() && std::mem::take(&mut self.fail_next_sync) {
            Err(file::io(
                "sync mutation journal (injected uncertain result)",
                &self.path,
                std::io::Error::other("injected sync result"),
            )
            .into())
        } else {
            result
        };
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }

    pub(crate) fn encode(&self) -> Result<Vec<u8>, SchemaMutationError> {
        let mut w = Writer(self.incarnation.to_vec());
        w.string(&self.coordinator)?;
        let create_count = self
            .reservations
            .values()
            .map(|r| 1 + usize::from(r.intent.is_some()) + usize::from(r.resolved.is_some()))
            .sum::<usize>();
        let drop_count = self
            .drops
            .values()
            .map(|d| {
                1 + usize::from(d.retired)
                    + usize::from(d.resolved.is_some())
                    + usize::from(d.gc.is_some())
                    + usize::from(d.gc.as_ref().is_some_and(|gc| gc.complete))
            })
            .sum::<usize>();
        let rewrite_count = self
            .rewrite_reservations
            .values()
            .map(|reservation| {
                1 + self
                    .rewrites
                    .get(&reservation.transaction)
                    .map_or(0, |rewrite| {
                        1 + usize::from(rewrite.retired) + usize::from(rewrite.resolved.is_some())
                    })
                    + usize::from(
                        !self.rewrites.contains_key(&reservation.transaction)
                            && self.rewrite_losers.contains(&reservation.transaction),
                    )
            })
            .sum::<usize>();
        let count = create_count
            .checked_add(drop_count)
            .and_then(|count| count.checked_add(rewrite_count))
            .ok_or(corrupt("too many journal records"))?;
        w.u32(u32::try_from(count).map_err(|_| corrupt("too many journal records"))?);
        let mut transactions = self
            .reservations
            .keys()
            .chain(self.drops.keys())
            .chain(self.rewrite_reservations.keys())
            .copied()
            .collect::<Vec<_>>();
        transactions.sort_unstable();
        for txn in transactions {
            if let Some(r) = self.reservations.get(&txn) {
                let mut record = Writer(vec![1]);
                record.u64(r.transaction.0);
                record.u64(r.table.0);
                record.u64(r.storage.0);
                record.u64(r.base_generation.0);
                record.u64(r.base_epoch);
                put_record(&mut w, &record.0)?;
                if let Some(intent) = &r.intent {
                    let mut record = Writer(vec![2]);
                    record.u64(r.transaction.0);
                    record.0.extend_from_slice(&intent.snapshot_digest);
                    record.0.extend_from_slice(&intent.fragment.encode()?);
                    put_record(&mut w, &record.0)?;
                }
                if let Some(committed) = r.resolved {
                    let mut record = Writer(vec![if committed { 4 } else { 3 }]);
                    record.u64(r.transaction.0);
                    put_record(&mut w, &record.0)?;
                }
            } else if let Some(d) = self.drops.get(&txn) {
                let mut record = Writer(vec![5]);
                record.u64(d.transaction.0);
                record.u64(d.target_generation.0);
                record.u64(d.target_epoch);
                record.0.extend_from_slice(&d.snapshot_digest);
                record.0.extend_from_slice(&d.fragment.encode()?);
                put_record(&mut w, &record.0)?;
                if d.retired {
                    let mut record = Writer(vec![6]);
                    record.u64(d.transaction.0);
                    put_record(&mut w, &record.0)?;
                }
                if let Some(committed) = d.resolved {
                    let mut record = Writer(vec![if committed { 8 } else { 7 }]);
                    record.u64(d.transaction.0);
                    put_record(&mut w, &record.0)?;
                }
                if let Some(gc) = &d.gc {
                    let mut record = Writer(vec![9]);
                    record.u64(d.transaction.0);
                    record.u64(gc.coordinator_horizon.0);
                    record.0.extend_from_slice(&gc.manifest_digest);
                    put_record(&mut w, &record.0)?;
                    if gc.complete {
                        let mut record = Writer(vec![10]);
                        record.u64(d.transaction.0);
                        put_record(&mut w, &record.0)?;
                    }
                }
            } else if let Some(reservation) = self.rewrite_reservations.get(&txn) {
                let mut record = Writer(vec![11]);
                record.u64(reservation.transaction.0);
                record.u64(reservation.table.0);
                record.u64(reservation.storage.0);
                record.u32(reservation.column.map_or(0, |column| column.0));
                record.u64(reservation.base_generation.0);
                record.u64(reservation.base_epoch);
                put_record(&mut w, &record.0)?;
                if let Some(rewrite) = self.rewrites.get(&txn) {
                    let mut record = Writer(vec![12]);
                    record.u64(txn.0);
                    record.0.extend_from_slice(&rewrite.snapshot_digest);
                    encode_operation(&mut record, &rewrite.operation)?;
                    let base = rewrite.base.encode()?;
                    record.u32(
                        u32::try_from(base.len()).map_err(|_| corrupt("rewrite base too large"))?,
                    );
                    record.0.extend_from_slice(&base);
                    let target = rewrite.target.encode()?;
                    record.u32(
                        u32::try_from(target.len())
                            .map_err(|_| corrupt("rewrite target too large"))?,
                    );
                    record.0.extend_from_slice(&target);
                    put_record(&mut w, &record.0)?;
                    if rewrite.retired {
                        let mut record = Writer(vec![13]);
                        record.u64(txn.0);
                        put_record(&mut w, &record.0)?;
                    }
                    if let Some(committed) = rewrite.resolved {
                        let mut record = Writer(vec![if committed { 15 } else { 14 }]);
                        record.u64(txn.0);
                        put_record(&mut w, &record.0)?;
                    }
                } else if self.rewrite_losers.contains(&txn) {
                    let mut record = Writer(vec![14]);
                    record.u64(txn.0);
                    put_record(&mut w, &record.0)?;
                }
            }
        }
        let bytes = envelope(b"NBSJ", &w.0)?;
        Self::decode(&bytes)?;
        Ok(bytes)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, SchemaMutationError> {
        let mut reader = Reader(open_envelope(bytes, b"NBSJ")?);
        let incarnation = reader
            .take(16)?
            .try_into()
            .map_err(|_| corrupt("journal incarnation"))?;
        if incarnation == [0; 16] {
            return Err(corrupt("zero journal incarnation"));
        }
        let coordinator = reader.string()?;
        if coordinator.is_empty()
            || Path::new(&coordinator).is_absolute()
            || coordinator.as_bytes().contains(&0)
        {
            return Err(corrupt("invalid journal coordinator locator"));
        }
        let count = reader.count(65536, 29)?;
        let mut reservations: BTreeMap<DatabaseTxnId, Reservation> = BTreeMap::new();
        let mut drops: BTreeMap<DatabaseTxnId, DropIntent> = BTreeMap::new();
        let mut rewrite_reservations: BTreeMap<DatabaseTxnId, RewriteReservation> = BTreeMap::new();
        let mut rewrites: BTreeMap<DatabaseTxnId, RewriteIntent> = BTreeMap::new();
        let mut rewrite_losers = BTreeSet::new();
        let mut last = (0, 0, 0, 0, 0);
        let mut current = None;
        for _ in 0..count {
            let length =
                usize::try_from(reader.u32()?).map_err(|_| corrupt("record length overflow"))?;
            let mut record = Reader(open_envelope(reader.take(length)?, b"NBSR")?);
            let tag = record.u8()?;
            let txn = DatabaseTxnId(record.u64()?);
            if txn.0 == 0 {
                return Err(corrupt("zero mutation transaction"));
            }
            match tag {
                1 => {
                    let table = TableId(record.u64()?);
                    let storage = StorageId(record.u64()?);
                    let generation = SchemaGeneration(record.u64()?);
                    let epoch = record.u64()?;
                    if txn.0 <= last.0
                        || table.0 <= last.1
                        || storage.0 <= last.2
                        || generation.0 == 0
                        || generation.0 < last.3
                        || epoch == 0
                        || epoch < last.4
                        || generation.0.checked_add(1).is_none()
                        || epoch.checked_add(1).is_none()
                    {
                        return Err(corrupt("duplicate, exhausted or nonmonotonic reservation"));
                    }
                    if let Some(previous) = current {
                        if reservations
                            .get(&previous)
                            .is_some_and(|r| r.resolved.is_none())
                            || drops
                                .get(&previous)
                                .is_some_and(|drop| drop.resolved.is_none())
                            || rewrite_reservations.contains_key(&previous)
                                && !rewrite_losers.contains(&previous)
                                && rewrites
                                    .get(&previous)
                                    .is_none_or(|rewrite| rewrite.resolved.is_none())
                        {
                            return Err(corrupt("overlapping schema transactions"));
                        }
                    }
                    last = (txn.0, table.0, storage.0, generation.0, epoch);
                    current = Some(txn);
                    reservations.insert(
                        txn,
                        Reservation {
                            transaction: txn,
                            table,
                            storage,
                            base_generation: generation,
                            base_epoch: epoch,
                            intent: None,
                            resolved: None,
                        },
                    );
                }
                2 => {
                    if current != Some(txn) {
                        return Err(corrupt("intent for unknown transaction"));
                    }
                    let r = reservations
                        .get_mut(&txn)
                        .ok_or(corrupt("intent without reservation"))?;
                    if r.intent.is_some() || r.resolved.is_some() {
                        return Err(corrupt("duplicate or out-of-order intent"));
                    }
                    let snapshot_digest = record
                        .take(32)?
                        .try_into()
                        .map_err(|_| corrupt("intent digest"))?;
                    let fragment = SchemaCatalogSnapshot::decode(record.0)?;
                    record.0 = &[];
                    if fragment.incarnation != incarnation
                        || fragment.epoch != r.base_epoch + 1
                        || fragment.committed.generation.0 != r.base_generation.0 + 1
                        || fragment.committed.schema.tables().len() != 1
                        || fragment.storages.len() != 1
                        || fragment.committed.schema.tables()[0].id != r.table
                        || fragment.storages[0].id != r.storage
                        || fragment.coordinator.as_deref() != Some(&coordinator)
                        || fragment.partition_evidence.is_some()
                        || fragment.committed.tables[0].version.0 != 1
                        || fragment.committed.schema.tables()[0]
                            .columns
                            .iter()
                            .enumerate()
                            .any(|(i, c)| c.id.0 as usize != i + 1 || c.primary_key)
                        || fragment.committed.tables[0]
                            .next_column_id
                            .map(|c| c.0 as usize)
                            != Some(fragment.committed.schema.tables()[0].columns.len() + 1)
                        || !matches!(
                            fragment.storages[0].kind,
                            crate::schema_catalog::CatalogStorageKind::Heap
                        )
                    {
                        return Err(corrupt("create intent identity or schema mismatch"));
                    }
                    if !matches!(
                        fragment.placements.tables[0].placement,
                        crate::registry::TablePlacement::Single { .. }
                    ) || fragment.committed.next_table_id
                        != r.table.0.checked_add(1).map(TableId)
                        || fragment.committed.next_storage_id
                            != r.storage.0.checked_add(1).map(StorageId)
                    {
                        return Err(corrupt("create intent placement or allocator mismatch"));
                    }
                    r.intent = Some(CreateIntent {
                        fragment,
                        snapshot_digest,
                    });
                }
                3 | 4 => {
                    if current != Some(txn) {
                        return Err(corrupt("resolution for unknown transaction"));
                    }
                    let r = reservations
                        .get_mut(&txn)
                        .ok_or(corrupt("resolution without reservation"))?;
                    if r.resolved.is_some() || (tag == 4 && r.intent.is_none()) {
                        return Err(corrupt("duplicate or out-of-order resolution"));
                    }
                    r.resolved = Some(tag == 4);
                }
                5 => {
                    if txn.0 <= last.0 {
                        return Err(corrupt("duplicate or nonmonotonic drop transaction"));
                    }
                    if let Some(previous) = current {
                        let unresolved_create = reservations
                            .get(&previous)
                            .is_some_and(|r| r.resolved.is_none());
                        let unresolved_drop =
                            drops.get(&previous).is_some_and(|d| d.resolved.is_none());
                        let unresolved_rewrite = rewrite_reservations.contains_key(&previous)
                            && !rewrite_losers.contains(&previous)
                            && rewrites
                                .get(&previous)
                                .is_none_or(|rewrite| rewrite.resolved.is_none());
                        if unresolved_create || unresolved_drop || unresolved_rewrite {
                            return Err(corrupt("overlapping schema transactions"));
                        }
                    }
                    let target_generation = SchemaGeneration(record.u64()?);
                    let target_epoch = record.u64()?;
                    let snapshot_digest = record
                        .take(32)?
                        .try_into()
                        .map_err(|_| corrupt("drop intent digest"))?;
                    let fragment = SchemaCatalogSnapshot::decode(record.0)?;
                    record.0 = &[];
                    validate_drop_fragment(
                        &fragment,
                        target_generation,
                        target_epoch,
                        incarnation,
                        &coordinator,
                    )?;
                    if fragment.committed.generation.0 < last.3 || fragment.epoch < last.4 {
                        return Err(corrupt("nonmonotonic drop base"));
                    }
                    last.0 = txn.0;
                    last.1 = last.1.max(allocator_predecessor(
                        fragment.committed.next_table_id.map(|id| id.0),
                    )?);
                    last.2 = last.2.max(allocator_predecessor(
                        fragment.committed.next_storage_id.map(|id| id.0),
                    )?);
                    last.3 = fragment.committed.generation.0;
                    last.4 = fragment.epoch;
                    current = Some(txn);
                    drops.insert(
                        txn,
                        DropIntent {
                            transaction: txn,
                            fragment,
                            target_generation,
                            target_epoch,
                            snapshot_digest,
                            retired: false,
                            resolved: None,
                            gc: None,
                        },
                    );
                }
                6 => {
                    if current != Some(txn) {
                        return Err(corrupt("retirement for unknown transaction"));
                    }
                    let intent = drops
                        .get_mut(&txn)
                        .ok_or(corrupt("retirement without drop intent"))?;
                    if intent.retired || intent.resolved.is_some() {
                        return Err(corrupt("duplicate or out-of-order retirement"));
                    }
                    intent.retired = true;
                }
                7 | 8 => {
                    if current != Some(txn) {
                        return Err(corrupt("drop resolution for unknown transaction"));
                    }
                    let intent = drops
                        .get_mut(&txn)
                        .ok_or(corrupt("drop resolution without intent"))?;
                    if intent.resolved.is_some() || (tag == 8 && !intent.retired) {
                        return Err(corrupt("duplicate or out-of-order drop resolution"));
                    }
                    if tag == 7 && intent.retired {
                        return Err(corrupt("retired drop resolved as loser"));
                    }
                    intent.resolved = Some(tag == 8);
                }
                9 => {
                    if current != Some(txn) {
                        return Err(corrupt("GC intent for unknown transaction"));
                    }
                    let intent = drops
                        .get_mut(&txn)
                        .ok_or(corrupt("GC intent without drop history"))?;
                    let coordinator_horizon = DatabaseTxnId(record.u64()?);
                    let manifest_digest = record
                        .take(32)?
                        .try_into()
                        .map_err(|_| corrupt("GC manifest digest"))?;
                    if !intent.retired
                        || intent.resolved != Some(true)
                        || intent.gc.is_some()
                        || coordinator_horizon.0 < intent.transaction.0
                    {
                        return Err(corrupt("duplicate or out-of-order GC intent"));
                    }
                    intent.gc = Some(RetiredHeapGcRecord {
                        coordinator_horizon,
                        manifest_digest,
                        complete: false,
                    });
                }
                10 => {
                    if current != Some(txn) {
                        return Err(corrupt("GC complete for unknown transaction"));
                    }
                    let gc = drops
                        .get_mut(&txn)
                        .and_then(|drop| drop.gc.as_mut())
                        .ok_or(corrupt("GC complete without intent"))?;
                    if gc.complete {
                        return Err(corrupt("duplicate GC complete"));
                    }
                    gc.complete = true;
                }
                11 => {
                    let table = TableId(record.u64()?);
                    let storage = StorageId(record.u64()?);
                    let raw_column = record.u32()?;
                    let column = (raw_column != 0).then_some(ColumnId(raw_column));
                    let generation = SchemaGeneration(record.u64()?);
                    let epoch = record.u64()?;
                    if txn.0 <= last.0
                        || table.0 == 0
                        || storage.0 <= last.2
                        || generation.0 == 0
                        || generation.0 < last.3
                        || epoch == 0
                        || epoch < last.4
                        || generation.0.checked_add(1).is_none()
                        || epoch.checked_add(1).is_none()
                    {
                        return Err(corrupt("invalid or nonmonotonic rewrite reservation"));
                    }
                    if let Some(previous) = current {
                        let unresolved_create = reservations
                            .get(&previous)
                            .is_some_and(|reservation| reservation.resolved.is_none());
                        let unresolved_drop = drops
                            .get(&previous)
                            .is_some_and(|drop| drop.resolved.is_none());
                        let unresolved_rewrite = rewrite_reservations.contains_key(&previous)
                            && !rewrite_losers.contains(&previous)
                            && rewrites
                                .get(&previous)
                                .is_none_or(|rewrite| rewrite.resolved.is_none());
                        if unresolved_create || unresolved_drop || unresolved_rewrite {
                            return Err(corrupt("overlapping schema transactions"));
                        }
                    }
                    last = (txn.0, last.1.max(table.0), storage.0, generation.0, epoch);
                    current = Some(txn);
                    rewrite_reservations.insert(
                        txn,
                        RewriteReservation {
                            transaction: txn,
                            table,
                            storage,
                            column,
                            base_generation: generation,
                            base_epoch: epoch,
                        },
                    );
                }
                12 => {
                    if current != Some(txn) || rewrites.contains_key(&txn) {
                        return Err(corrupt("rewrite intent without current reservation"));
                    }
                    let reservation = rewrite_reservations
                        .get(&txn)
                        .cloned()
                        .ok_or(corrupt("rewrite intent without reservation"))?;
                    let snapshot_digest = record
                        .take(32)?
                        .try_into()
                        .map_err(|_| corrupt("rewrite intent digest"))?;
                    let operation = decode_operation(&mut record)?;
                    let base_len = usize::try_from(record.u32()?)
                        .map_err(|_| corrupt("rewrite base length overflow"))?;
                    let base = SchemaCatalogSnapshot::decode(record.take(base_len)?)?;
                    let target_len = usize::try_from(record.u32()?)
                        .map_err(|_| corrupt("rewrite target length overflow"))?;
                    let target = SchemaCatalogSnapshot::decode(record.take(target_len)?)?;
                    validate_rewrite_fragments(
                        &reservation,
                        &operation,
                        &base,
                        &target,
                        incarnation,
                        &coordinator,
                    )?;
                    rewrites.insert(
                        txn,
                        RewriteIntent {
                            reservation,
                            operation,
                            base,
                            target,
                            snapshot_digest,
                            retired: false,
                            resolved: None,
                        },
                    );
                }
                13 => {
                    if current != Some(txn) {
                        return Err(corrupt("rewrite retirement for unknown transaction"));
                    }
                    let rewrite = rewrites
                        .get_mut(&txn)
                        .ok_or(corrupt("rewrite retirement without intent"))?;
                    if rewrite.retired || rewrite.resolved.is_some() {
                        return Err(corrupt("duplicate or out-of-order rewrite retirement"));
                    }
                    rewrite.retired = true;
                }
                14 | 15 => {
                    if current != Some(txn) {
                        return Err(corrupt("rewrite resolution for unknown transaction"));
                    }
                    if tag == 14 && !rewrites.contains_key(&txn) {
                        if !rewrite_reservations.contains_key(&txn) || !rewrite_losers.insert(txn) {
                            return Err(corrupt("duplicate rewrite reservation loser"));
                        }
                    } else {
                        let rewrite = rewrites
                            .get_mut(&txn)
                            .ok_or(corrupt("rewrite winner without intent"))?;
                        if rewrite.resolved.is_some() || (tag == 15 && !rewrite.retired) {
                            return Err(corrupt("duplicate or out-of-order rewrite resolution"));
                        }
                        if tag == 14 && rewrite.retired {
                            return Err(corrupt("retired rewrite resolved as loser"));
                        }
                        rewrite.resolved = Some(tag == 15);
                    }
                }
                _ => return Err(corrupt("unknown journal record tag")),
            }
            if !record.0.is_empty() {
                return Err(corrupt("trailing journal record bytes"));
            }
        }
        if !reader.0.is_empty() {
            return Err(corrupt("trailing journal bytes"));
        }
        let mut retired = BTreeMap::new();
        for intent in drops.values().filter(|d| d.retired) {
            if let Some(previous) = retired.insert(intent.storage(), intent.table()) {
                if previous != intent.table() {
                    return Err(corrupt("storage retired by multiple tables"));
                }
                return Err(corrupt("duplicate storage retirement"));
            }
        }
        for rewrite in rewrites.values().filter(|rewrite| rewrite.retired) {
            if retired
                .insert(rewrite.old_storage(), rewrite.table())
                .is_some()
            {
                return Err(corrupt("duplicate storage retirement"));
            }
            if rewrite.old_storage() == rewrite.new_storage() {
                return Err(corrupt("rewrite reuses old StorageId"));
            }
        }
        Ok(Self {
            path: PathBuf::new(),
            incarnation,
            coordinator,
            reservations,
            drops,
            rewrite_reservations,
            rewrites,
            rewrite_losers,
            poisoned: false,
            #[cfg(test)]
            fail_next_sync: false,
        })
    }
}

fn allocator_predecessor(floor: Option<u64>) -> Result<u64, SchemaMutationError> {
    match floor {
        Some(floor) => floor
            .checked_sub(1)
            .ok_or(corrupt("zero allocator floor in drop intent")),
        None => Ok(u64::MAX),
    }
}

fn encode_operation(
    writer: &mut Writer,
    operation: &AlterTableOperation,
) -> Result<(), SchemaMutationError> {
    match operation {
        AlterTableOperation::RenameTable { new_name } => {
            writer.u8(1);
            writer.string(new_name)?;
        }
        AlterTableOperation::RenameColumn {
            column_id,
            new_name,
        } => {
            writer.u8(2);
            writer.u32(column_id.0);
            writer.string(new_name)?;
        }
        AlterTableOperation::AddNullableColumn { name, data_type } => {
            writer.u8(3);
            writer.string(name)?;
            encode_semantic_type(writer, data_type)?;
        }
        AlterTableOperation::DropColumn { column_id } => {
            writer.u8(4);
            writer.u32(column_id.0);
        }
        AlterTableOperation::SetNotNull { column_id } => {
            writer.u8(5);
            writer.u32(column_id.0);
        }
        AlterTableOperation::DropNotNull { column_id } => {
            writer.u8(6);
            writer.u32(column_id.0);
        }
        AlterTableOperation::ChangeNominalType {
            column_id,
            target_type,
        } => {
            writer.u8(7);
            writer.u32(column_id.0);
            encode_semantic_type(writer, target_type)?;
        }
    }
    Ok(())
}

fn decode_operation(reader: &mut Reader<'_>) -> Result<AlterTableOperation, SchemaMutationError> {
    let column = |reader: &mut Reader<'_>| -> Result<ColumnId, SchemaMutationError> {
        let id = ColumnId(reader.u32()?);
        if id.0 == 0 {
            return Err(corrupt("zero rewrite ColumnId"));
        }
        Ok(id)
    };
    Ok(match reader.u8()? {
        1 => AlterTableOperation::RenameTable {
            new_name: reader.string()?,
        },
        2 => AlterTableOperation::RenameColumn {
            column_id: column(reader)?,
            new_name: reader.string()?,
        },
        3 => AlterTableOperation::AddNullableColumn {
            name: reader.string()?,
            data_type: decode_semantic_type(reader)?,
        },
        4 => AlterTableOperation::DropColumn {
            column_id: column(reader)?,
        },
        5 => AlterTableOperation::SetNotNull {
            column_id: column(reader)?,
        },
        6 => AlterTableOperation::DropNotNull {
            column_id: column(reader)?,
        },
        7 => AlterTableOperation::ChangeNominalType {
            column_id: column(reader)?,
            target_type: decode_semantic_type(reader)?,
        },
        _ => return Err(corrupt("unknown rewrite operation tag")),
    })
}

fn encode_semantic_type(
    writer: &mut Writer,
    data_type: &SemanticType,
) -> Result<(), SchemaMutationError> {
    writer.u8(match data_type.physical {
        PhysicalType::Bool => 1,
        PhysicalType::Int64 => 2,
        PhysicalType::UInt64 => 3,
        PhysicalType::Text => 4,
    });
    writer.u8(u8::from(data_type.name.is_some()));
    if let Some(name) = &data_type.name {
        writer.string(name)?;
    }
    Ok(())
}

fn decode_semantic_type(reader: &mut Reader<'_>) -> Result<SemanticType, SchemaMutationError> {
    let physical = match reader.u8()? {
        1 => PhysicalType::Bool,
        2 => PhysicalType::Int64,
        3 => PhysicalType::UInt64,
        4 => PhysicalType::Text,
        _ => return Err(corrupt("unknown rewrite physical type")),
    };
    match reader.u8()? {
        0 => Ok(SemanticType::physical(physical)),
        1 => Ok(SemanticType::named(reader.string()?, physical)),
        _ => Err(corrupt("invalid rewrite semantic type flag")),
    }
}

fn validate_drop_fragment(
    fragment: &SchemaCatalogSnapshot,
    target_generation: SchemaGeneration,
    target_epoch: u64,
    incarnation: [u8; 16],
    coordinator: &str,
) -> Result<(), SchemaMutationError> {
    if fragment.incarnation != incarnation
        || fragment.committed.schema.tables().len() != 1
        || fragment.committed.tables.len() != 1
        || fragment.placements.tables.len() != 1
        || fragment.storages.len() != 1
        || fragment.coordinator.as_deref() != Some(coordinator)
        || fragment.partition_evidence.is_some()
        || target_generation.0
            != fragment
                .committed
                .generation
                .0
                .checked_add(1)
                .ok_or(corrupt("drop generation exhausted"))?
        || target_epoch
            != fragment
                .epoch
                .checked_add(1)
                .ok_or(corrupt("drop epoch exhausted"))?
    {
        return Err(corrupt("drop intent generation or inventory mismatch"));
    }
    let table = &fragment.committed.schema.tables()[0];
    let lineage = &fragment.committed.tables[0];
    let placement = &fragment.placements.tables[0];
    let storage = &fragment.storages[0];
    if table.id != lineage.table_id
        || table.id != placement.table_id
        || table.id != storage.table_id
        || lineage.version.0 == 0
        || table
            .fingerprint()
            .map_err(|_| corrupt("invalid retired table fingerprint"))?
            != placement.schema_fingerprint
        || !matches!(
            storage.kind,
            crate::schema_catalog::CatalogStorageKind::Heap
        )
        || !matches!(
            placement.placement,
            crate::registry::TablePlacement::Single {
                table_id,
                storage_id
            } if table_id == table.id && storage_id == storage.id
        )
    {
        return Err(corrupt("drop intent exact identity mismatch"));
    }
    Ok(())
}

fn validate_rewrite_fragments(
    reservation: &RewriteReservation,
    operation: &AlterTableOperation,
    base: &SchemaCatalogSnapshot,
    target: &SchemaCatalogSnapshot,
    incarnation: [u8; 16],
    coordinator: &str,
) -> Result<(), SchemaMutationError> {
    if base.incarnation != incarnation
        || target.incarnation != incarnation
        || base.epoch != reservation.base_epoch
        || base.committed.generation != reservation.base_generation
        || target.epoch
            != base
                .epoch
                .checked_add(1)
                .ok_or(corrupt("rewrite epoch exhausted"))?
        || target.committed.generation.0
            != base
                .committed
                .generation
                .0
                .checked_add(1)
                .ok_or(corrupt("rewrite generation exhausted"))?
        || base.coordinator.as_deref() != Some(coordinator)
        || target.coordinator.as_deref() != Some(coordinator)
        || base.partition_evidence.is_some()
        || target.partition_evidence.is_some()
        || base.committed.schema.tables().len() != 1
        || target.committed.schema.tables().len() != 1
        || base.committed.tables.len() != 1
        || target.committed.tables.len() != 1
        || base.placements.tables.len() != 1
        || target.placements.tables.len() != 1
        || base.storages.len() != 1
        || target.storages.len() != 1
    {
        return Err(corrupt("rewrite fragment generation or inventory mismatch"));
    }
    let base_table = &base.committed.schema.tables()[0];
    let target_table = &target.committed.schema.tables()[0];
    let base_lineage = &base.committed.tables[0];
    let target_lineage = &target.committed.tables[0];
    let base_storage = &base.storages[0];
    let target_storage = &target.storages[0];
    if base_table.id != reservation.table
        || target_table.id != reservation.table
        || base_lineage.table_id != reservation.table
        || target_lineage.table_id != reservation.table
        || target_lineage.version.0
            != base_lineage
                .version
                .0
                .checked_add(1)
                .ok_or(corrupt("rewrite table version exhausted"))?
        || base_storage.table_id != reservation.table
        || target_storage.table_id != reservation.table
        || base_storage.id == reservation.storage
        || target_storage.id != reservation.storage
        || !matches!(
            base_storage.kind,
            crate::schema_catalog::CatalogStorageKind::Heap
        )
        || !matches!(
            target_storage.kind,
            crate::schema_catalog::CatalogStorageKind::Heap
        )
        || !matches!(
            base.placements.tables[0].placement,
            crate::registry::TablePlacement::Single { table_id, storage_id }
                if table_id == reservation.table && storage_id == base_storage.id
        )
        || !matches!(
            target.placements.tables[0].placement,
            crate::registry::TablePlacement::Single { table_id, storage_id }
                if table_id == reservation.table && storage_id == reservation.storage
        )
        || base_table
            .fingerprint()
            .map_err(|_| corrupt("invalid rewrite base schema"))?
            != base.placements.tables[0].schema_fingerprint
        || target_table
            .fingerprint()
            .map_err(|_| corrupt("invalid rewrite target schema"))?
            != target.placements.tables[0].schema_fingerprint
        || base.placements.tables[0].schema_fingerprint
            == target.placements.tables[0].schema_fingerprint
        || base.committed.next_table_id != target.committed.next_table_id
        || base.committed.next_partition_id != target.committed.next_partition_id
        || !floor_at_least(
            target.committed.next_storage_id.map(|id| id.0),
            reservation.storage.0.checked_add(1),
        )
        || !floor_at_least(
            target_lineage.next_column_id.map(|id| u64::from(id.0)),
            base_lineage.next_column_id.map(|id| u64::from(id.0)),
        )
    {
        return Err(corrupt("rewrite exact identity mismatch"));
    }

    let mut expected = base_table.clone();
    match operation {
        AlterTableOperation::RenameTable { new_name } => {
            if reservation.column.is_some() {
                return Err(corrupt("rename unexpectedly reserves ColumnId"));
            }
            expected.name = new_name.clone();
        }
        AlterTableOperation::RenameColumn {
            column_id,
            new_name,
        } => {
            if reservation.column.is_some() {
                return Err(corrupt("rename unexpectedly reserves ColumnId"));
            }
            expected
                .columns
                .iter_mut()
                .find(|column| column.id == *column_id)
                .ok_or(corrupt("rewrite column absent"))?
                .name = new_name.clone();
        }
        AlterTableOperation::AddNullableColumn { name, data_type } => {
            let id = reservation
                .column
                .ok_or(corrupt("ADD rewrite has no ColumnId reservation"))?;
            let type_spec = match &data_type.name {
                Some(type_name) => TypeSpec::Semantic {
                    physical: data_type.physical,
                    name: type_name.clone(),
                },
                None => TypeSpec::Physical(data_type.physical),
            };
            expected
                .columns
                .push(netbadb_schema::ColumnDef::new(id, name.clone(), type_spec).nullable(true));
            if target_lineage.next_column_id.map(|next| next.0) != id.0.checked_add(1) {
                return Err(corrupt("ADD rewrite ColumnId floor mismatch"));
            }
        }
        AlterTableOperation::DropColumn { column_id } => {
            if reservation.column.is_some() {
                return Err(corrupt("DROP unexpectedly reserves ColumnId"));
            }
            expected.columns.retain(|column| column.id != *column_id);
        }
        AlterTableOperation::SetNotNull { column_id } => {
            if reservation.column.is_some() {
                return Err(corrupt(
                    "nullability rewrite unexpectedly reserves ColumnId",
                ));
            }
            expected
                .columns
                .iter_mut()
                .find(|column| column.id == *column_id)
                .ok_or(corrupt("rewrite column absent"))?
                .nullable = false;
        }
        AlterTableOperation::DropNotNull { column_id } => {
            if reservation.column.is_some() {
                return Err(corrupt(
                    "nullability rewrite unexpectedly reserves ColumnId",
                ));
            }
            expected
                .columns
                .iter_mut()
                .find(|column| column.id == *column_id)
                .ok_or(corrupt("rewrite column absent"))?
                .nullable = true;
        }
        AlterTableOperation::ChangeNominalType {
            column_id,
            target_type,
        } => {
            if reservation.column.is_some() {
                return Err(corrupt("type rewrite unexpectedly reserves ColumnId"));
            }
            let column = expected
                .columns
                .iter_mut()
                .find(|column| column.id == *column_id)
                .ok_or(corrupt("rewrite column absent"))?;
            column.type_spec = match &target_type.name {
                Some(type_name) => TypeSpec::Semantic {
                    physical: target_type.physical,
                    name: type_name.clone(),
                },
                None => TypeSpec::Physical(target_type.physical),
            };
        }
    }
    if expected != *target_table {
        return Err(corrupt("rewrite operation differs from target schema"));
    }
    Ok(())
}
fn floor_at_least(current: Option<u64>, required: Option<u64>) -> bool {
    match (current, required) {
        (None, _) => true,
        (Some(_), None) => false,
        (Some(current), Some(required)) => current >= required,
    }
}
fn put_record(w: &mut Writer, payload: &[u8]) -> Result<(), SchemaMutationError> {
    let record = envelope(b"NBSR", payload)?;
    if w.0
        .len()
        .checked_add(record.len())
        .and_then(|n| n.checked_add(4))
        .is_none_or(|n| n > crate::schema_catalog::MAX_BYTES - 16)
    {
        return Err(corrupt("mutation journal capacity exhausted"));
    }
    w.u32(u32::try_from(record.len()).map_err(|_| corrupt("record too large"))?);
    w.0.extend_from_slice(&record);
    Ok(())
}
