//! Ordered reservation/intention history; NBSC floors + this history are one allocator.
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use netbadb_types::{DatabaseTxnId, SchemaGeneration, StorageId, TableId};

use crate::schema_catalog::{Reader, SchemaCatalogSnapshot, Writer, envelope, open_envelope};
use crate::schema_catalog_file as file;
use crate::schema_mutation::SchemaMutationError;

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
        if activated {
            if open_envelope(&file::read(&witness)?, b"NBSA")? != incarnation {
                return Err(corrupt("mutation activation incarnation mismatch"));
            }
        } else if !journal.reservations.is_empty() {
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
            .max()
            .map_or(Some(floor), |max| {
                max.checked_add(1).map(|n| StorageId(n.max(floor.0)))
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
            .map(|d| 1 + usize::from(d.retired) + usize::from(d.resolved.is_some()))
            .sum::<usize>();
        let count = create_count
            .checked_add(drop_count)
            .ok_or(corrupt("too many journal records"))?;
        w.u32(u32::try_from(count).map_err(|_| corrupt("too many journal records"))?);
        let mut transactions = self
            .reservations
            .keys()
            .chain(self.drops.keys())
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
                        if unresolved_create || unresolved_drop {
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
        Ok(Self {
            path: PathBuf::new(),
            incarnation,
            coordinator,
            reservations,
            drops,
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
