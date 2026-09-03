use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;
use std::rc::Rc;

use netbadb_storage::{
    IndexDefinition, IsolationLevel, StorageError, StorageReadView, StorageTransaction,
    TransactionState as StorageTransactionState,
};
use netbadb_types::{ColumnId, IndexName};
use netbadb_types::{DatabaseTxnId, StorageId};

use crate::coordinator_log::{CoordinatorLog, CoordinatorLogError, CoordinatorParticipant};
use crate::registry::{StorageRegistry, StorageRegistryError};
use crate::schema_composition::SchemaCompositionState;
use crate::schema_mutation::{SchemaMutation, SchemaMutationError};

pub(crate) type SharedCoordinatorLog = Rc<RefCell<CoordinatorLog>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    Active,
    RollbackRequired,
    Preparing,
    DecisionPending,
    CommitDecided,
    ApplyingCommit,
    FinalizePending,
    CommitPending,
    RollbackPending,
    Committed,
    RolledBack,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParticipantMode {
    Read,
    Write,
}

#[derive(Debug)]
struct StorageParticipant {
    mode: ParticipantMode,
    context: StorageTransaction,
}

/// One statement's database-owned collection of engine read views.
///
/// NetbaDB has no database-global commit sequence yet. This object provides
/// database-level ownership and isolation intent while each entry remains the
/// engine adapter for its physical storage's existing MVCC domain.
#[derive(Debug)]
pub struct DatabaseReadView {
    transaction_id: Option<DatabaseTxnId>,
    isolation_level: IsolationLevel,
    views: Vec<(StorageId, StorageReadView)>,
}

impl DatabaseReadView {
    pub(crate) fn autocommit(
        isolation_level: IsolationLevel,
        views: Vec<(StorageId, StorageReadView)>,
    ) -> Self {
        Self {
            transaction_id: None,
            isolation_level,
            views,
        }
    }

    #[must_use]
    pub fn transaction_id(&self) -> Option<DatabaseTxnId> {
        self.transaction_id
    }

    #[must_use]
    pub fn isolation_level(&self) -> IsolationLevel {
        self.isolation_level
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (StorageId, &StorageReadView)> {
        self.views.iter().map(|(id, view)| (*id, view))
    }
}

/// Database-level SQL transaction coordinator.
///
/// Participants are registered lazily. Coordinator-enabled databases prepare
/// two or more write participants before recording one durable global commit
/// decision; legacy databases retain the one-writer boundary.
#[derive(Debug)]
pub struct DatabaseTransaction {
    owner: Rc<()>,
    id: DatabaseTxnId,
    isolation_level: IsolationLevel,
    state: TransactionState,
    participants: BTreeMap<StorageId, StorageParticipant>,
    write_participants: BTreeSet<StorageId>,
    coordinator: Option<SharedCoordinatorLog>,
    pub(crate) schema_mutation: Option<SchemaMutation>,
    pub(crate) schema_composition: SchemaCompositionState,
    pub(crate) preparation_scope: Rc<()>,
    pending_indexes: Vec<(StorageId, IndexDefinition)>,
    pending_index_drops: Vec<(StorageId, netbadb_types::IndexId)>,
}

impl DatabaseTransaction {
    pub(crate) fn new(
        owner: Rc<()>,
        id: DatabaseTxnId,
        isolation_level: IsolationLevel,
        coordinator: Option<SharedCoordinatorLog>,
    ) -> Self {
        Self {
            owner,
            id,
            isolation_level,
            state: TransactionState::Active,
            participants: BTreeMap::new(),
            write_participants: BTreeSet::new(),
            coordinator,
            schema_mutation: None,
            schema_composition: SchemaCompositionState::None,
            preparation_scope: Rc::new(()),
            pending_indexes: Vec::new(),
            pending_index_drops: Vec::new(),
        }
    }

    #[must_use]
    pub fn id(&self) -> DatabaseTxnId {
        self.id
    }

    #[must_use]
    pub fn state(&self) -> TransactionState {
        self.state
    }

    #[must_use]
    pub fn isolation_level(&self) -> IsolationLevel {
        self.isolation_level
    }

    #[must_use]
    pub fn write_participant(&self) -> Option<StorageId> {
        self.write_participants.first().copied()
    }

    #[cfg(test)]
    pub(crate) fn participant_mode(&self, storage_id: StorageId) -> Option<ParticipantMode> {
        self.participants
            .get(&storage_id)
            .map(|participant| participant.mode)
    }

    #[cfg(test)]
    pub(crate) fn participant_count(&self) -> usize {
        self.participants.len()
    }

    #[cfg(test)]
    pub(crate) fn force_participant_rollback_for_prepare_failure(
        &mut self,
        storage_id: StorageId,
    ) -> Result<(), CoordinatorError> {
        let participant = self
            .participants
            .get_mut(&storage_id)
            .ok_or(CoordinatorError::UnknownStorageId { storage_id })?;
        participant
            .context
            .rollback()
            .map_err(CoordinatorError::from)
    }

    pub(crate) fn validate_owner(&self, owner: &Rc<()>) -> Result<(), CoordinatorError> {
        self.validate_commit_owner(owner)?;
        self.ensure_active()
    }

    /// Commit retries must validate ownership without requiring Active again;
    /// the commit state machine owns validation of pending decision states.
    pub(crate) fn validate_commit_owner(&self, owner: &Rc<()>) -> Result<(), CoordinatorError> {
        if !Rc::ptr_eq(&self.owner, owner) {
            return Err(CoordinatorError::ForeignDatabaseTransaction {
                transaction_id: self.id,
            });
        }
        Ok(())
    }

    pub(crate) fn begin_read_view(
        &mut self,
        storage_ids: &[StorageId],
        registry: &mut StorageRegistry,
    ) -> Result<DatabaseReadView, CoordinatorError> {
        self.ensure_active()?;
        let mut unique = BTreeSet::new();
        for storage_id in storage_ids {
            if unique.insert(*storage_id) {
                self.ensure_participant(*storage_id, registry)?;
            }
        }

        let mut views = Vec::with_capacity(unique.len());
        for storage_id in unique {
            let participant = self.participants.get_mut(&storage_id).ok_or(
                CoordinatorError::ParticipantStateViolation {
                    storage_id,
                    reason: "registered read participant is missing",
                },
            )?;
            views.push((storage_id, participant.context.begin_statement()?));
        }
        Ok(DatabaseReadView {
            transaction_id: Some(self.id),
            isolation_level: self.isolation_level,
            views,
        })
    }

    pub(crate) fn write_context(
        &mut self,
        storage_id: StorageId,
        registry: &mut StorageRegistry,
    ) -> Result<&mut StorageTransaction, CoordinatorError> {
        self.ensure_active()?;
        if self.coordinator.is_none() {
            if let Some(existing) = self.write_participants.first().copied() {
                if existing != storage_id {
                    return Err(CoordinatorError::MultipleWriteParticipantsUnsupported {
                        existing,
                        requested: storage_id,
                    });
                }
            }
        }
        self.ensure_participant(storage_id, registry)?;
        let participant = self.participants.get_mut(&storage_id).ok_or(
            CoordinatorError::ParticipantStateViolation {
                storage_id,
                reason: "registered write participant is missing",
            },
        )?;
        participant.mode = ParticipantMode::Write;
        self.write_participants.insert(storage_id);
        Ok(&mut participant.context)
    }

    pub fn commit(&mut self) -> Result<(), CoordinatorError> {
        if self.has_pending_schema_mutations() {
            return Err(CoordinatorError::SchemaMutationRequiresDatabaseCommit);
        }
        self.commit_with_schema_mutations()
    }

    pub(crate) fn commit_with_schema_mutations(&mut self) -> Result<(), CoordinatorError> {
        if self.write_participants.len() > 1
            || self.schema_mutation.is_some()
            || self.schema_composition.materialized().is_some()
            || self.schema_composition.backfill().is_some()
            || self.schema_composition.materialized_index().is_some()
        {
            return self.commit_multi_write();
        }
        match self.state {
            TransactionState::Active => self.state = TransactionState::CommitPending,
            TransactionState::CommitPending => {}
            state => {
                return Err(CoordinatorError::NotActive {
                    transaction_id: self.id,
                    state,
                });
            }
        }

        // Read-only contexts carry no database mutations. Finish them before
        // the unique writer so no later read-context failure can follow the
        // durable data decision.
        for (storage_id, participant) in self
            .participants
            .iter_mut()
            .filter(|(_, participant)| participant.mode == ParticipantMode::Read)
        {
            commit_participant(*storage_id, participant)?;
        }
        if let Some(storage_id) = self.write_participants.first().copied() {
            let participant = self.participants.get_mut(&storage_id).ok_or(
                CoordinatorError::ParticipantStateViolation {
                    storage_id,
                    reason: "write participant identity has no context",
                },
            )?;
            commit_participant(storage_id, participant)?;
        }
        self.state = TransactionState::Committed;
        Ok(())
    }

    pub(crate) fn stage_index(&mut self, storage_id: StorageId, definition: IndexDefinition) {
        self.pending_indexes.push((storage_id, definition));
    }

    pub(crate) fn pending_index_by_name(
        &self,
        name: &IndexName,
    ) -> Option<(StorageId, &IndexDefinition)> {
        self.pending_indexes
            .iter()
            .find(|(_, definition)| definition.name.as_ref() == Some(name))
            .map(|(storage_id, definition)| (*storage_id, definition))
    }

    pub(crate) fn has_pending_index_column(
        &self,
        storage_id: StorageId,
        column_id: ColumnId,
    ) -> bool {
        self.pending_indexes
            .iter()
            .any(|(pending_storage, definition)| {
                *pending_storage == storage_id && definition.column_id == column_id
            })
    }

    pub(crate) fn take_pending_indexes(&mut self) -> Vec<(StorageId, IndexDefinition)> {
        std::mem::take(&mut self.pending_indexes)
    }

    pub(crate) fn has_pending_schema_mutations(&self) -> bool {
        self.schema_mutation.is_some()
            || self.schema_composition.is_started()
            || !self.pending_indexes.is_empty()
            || !self.pending_index_drops.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn is_pristine_for_schema_rewrite(&self) -> bool {
        self.state == TransactionState::Active
            && self.participants.is_empty()
            && self.write_participants.is_empty()
            && !self.has_pending_schema_mutations()
    }

    pub(crate) fn is_pristine_for_schema_composition(&self) -> bool {
        self.state == TransactionState::Active
            && self.participants.is_empty()
            && self.write_participants.is_empty()
            && self.schema_mutation.is_none()
            && self.schema_composition.is_none()
            && self.pending_indexes.is_empty()
            && self.pending_index_drops.is_empty()
    }

    pub(crate) fn has_pending_index_creations(&self) -> bool {
        !self.pending_indexes.is_empty()
    }
    pub(crate) fn has_pending_index_drops(&self) -> bool {
        !self.pending_index_drops.is_empty()
    }
    pub(crate) fn stage_index_drop(&mut self, storage: StorageId, id: netbadb_types::IndexId) {
        self.pending_index_drops.push((storage, id));
    }
    pub(crate) fn take_pending_index_drops(&mut self) -> Vec<(StorageId, netbadb_types::IndexId)> {
        std::mem::take(&mut self.pending_index_drops)
    }

    fn commit_multi_write(&mut self) -> Result<(), CoordinatorError> {
        match self.state {
            TransactionState::Active => self.state = TransactionState::Preparing,
            TransactionState::Preparing
            | TransactionState::DecisionPending
            | TransactionState::CommitDecided
            | TransactionState::ApplyingCommit
            | TransactionState::FinalizePending => {}
            state => {
                return Err(CoordinatorError::NotActive {
                    transaction_id: self.id,
                    state,
                });
            }
        }

        if self.state == TransactionState::Preparing {
            for (storage_id, participant) in self
                .participants
                .iter_mut()
                .filter(|(_, participant)| participant.mode == ParticipantMode::Read)
            {
                commit_participant(*storage_id, participant)?;
            }
            #[cfg(test)]
            crate::coordinator_crash::maybe_crash("before-first-prepare");
            for (position, storage_id) in self.write_participants.iter().copied().enumerate() {
                let participant = self.participants.get_mut(&storage_id).ok_or(
                    CoordinatorError::ParticipantStateViolation {
                        storage_id,
                        reason: "write participant identity has no context",
                    },
                )?;
                if let Err(prepare_error) = prepare_participant(self.id, storage_id, participant) {
                    self.state = TransactionState::RollbackPending;
                    self.rollback_before_decision()?;
                    return Err(CoordinatorError::PrepareFailed {
                        storage_id,
                        source: Box::new(prepare_error),
                    });
                }
                #[cfg(test)]
                crate::coordinator_crash::maybe_crash_indexed("after-prepare", position + 1);
                #[cfg(not(test))]
                let _ = position;
            }
            #[cfg(test)]
            crate::coordinator_crash::maybe_crash("after-all-prepares");
            if let Some(mutation) = &self.schema_mutation {
                crate::schema_mutation::crash("participants-prepared");
                mutation.prepare()?;
                crate::schema_mutation::crash("before-coordinator-decision");
            } else if let Some(composition) = self.schema_composition.backfill() {
                crate::schema_mutation::crash("backfill-participants-prepared");
                composition.prepare()?;
                crate::schema_mutation::crash("backfill-before-coordinator-decision");
                crate::schema_mutation::crash("before-coordinator-decision");
            } else if let Some(composition) = self.schema_composition.materialized() {
                crate::schema_mutation::crash("composition-participants-prepared");
                crate::schema_mutation::crash("participants-prepared");
                composition.prepare()?;
                crate::schema_mutation::crash("composition-before-coordinator-decision");
                crate::schema_mutation::crash("before-coordinator-decision");
            } else if let Some(composition) = self.schema_composition.materialized_index() {
                crate::schema_mutation::crash("composition-participants-prepared");
                crate::schema_mutation::crash("participants-prepared");
                composition.prepare()?;
                crate::schema_mutation::crash("composition-before-coordinator-decision");
                crate::schema_mutation::crash("before-coordinator-decision");
            }
            self.state = TransactionState::DecisionPending;
        }

        if self.state == TransactionState::DecisionPending {
            let decision_participants = self
                .write_participants
                .iter()
                .map(|storage_id| {
                    let participant = self.participants.get(storage_id).ok_or(
                        CoordinatorError::ParticipantStateViolation {
                            storage_id: *storage_id,
                            reason: "prepared participant identity has no context",
                        },
                    )?;
                    Ok(CoordinatorParticipant {
                        storage_id: *storage_id,
                        physical_txn_id: participant.context.id(),
                    })
                })
                .collect::<Result<Vec<_>, CoordinatorError>>()?;
            let mut log = self
                .coordinator
                .as_ref()
                .ok_or(CoordinatorError::DurableCoordinatorRequired)?
                .try_borrow_mut()
                .map_err(|_| CoordinatorError::CoordinatorBusy)?;
            if let Some(mutation) = &self.schema_mutation {
                log.commit_schema_decision(
                    self.id,
                    &decision_participants,
                    Some(&mutation.reference),
                )?;
            } else if let Some(composition) = self.schema_composition.backfill() {
                log.commit_schema_decision(
                    self.id,
                    &decision_participants,
                    Some(&composition.reference),
                )?;
            } else if let Some(composition) = self.schema_composition.materialized() {
                log.commit_schema_decision(
                    self.id,
                    &decision_participants,
                    Some(&composition.reference),
                )?;
            } else if let Some(composition) = self.schema_composition.materialized_index() {
                log.commit_schema_decision(
                    self.id,
                    &decision_participants,
                    composition.reference.as_ref(),
                )?;
            } else {
                log.commit_decision(self.id, &decision_participants)?;
            }
            drop(log);
            self.state = TransactionState::CommitDecided;
            if self.schema_mutation.is_some()
                || self.schema_composition.materialized().is_some()
                || self.schema_composition.backfill().is_some()
                || self.schema_composition.materialized_index().is_some()
            {
                crate::schema_mutation::crash("coordinator-durable");
            }
            #[cfg(test)]
            crate::coordinator_crash::maybe_crash("after-durable-decision");
        }

        if matches!(
            self.state,
            TransactionState::CommitDecided | TransactionState::ApplyingCommit
        ) {
            self.state = TransactionState::ApplyingCommit;
            for (position, storage_id) in self.write_participants.iter().copied().enumerate() {
                let participant = self.participants.get_mut(&storage_id).ok_or(
                    CoordinatorError::ParticipantStateViolation {
                        storage_id,
                        reason: "decided participant identity has no context",
                    },
                )?;
                commit_prepared_participant(self.id, storage_id, participant)?;
                #[cfg(test)]
                crate::coordinator_crash::maybe_crash_indexed("after-commit", position + 1);
                #[cfg(not(test))]
                let _ = position;
            }
            #[cfg(test)]
            crate::coordinator_crash::maybe_crash("after-all-commits");
            self.state = TransactionState::FinalizePending;
            if self.schema_mutation.is_some()
                || self.schema_composition.materialized().is_some()
                || self.schema_composition.materialized_index().is_some()
            {
                crate::schema_mutation::crash("staged-heap-committed");
            }
        }

        if self.schema_mutation.is_some()
            || self.schema_composition.materialized().is_some()
            || self.schema_composition.backfill().is_some()
            || self.schema_composition.materialized_index().is_some()
        {
            return Ok(());
        }
        self.coordinator
            .as_ref()
            .ok_or(CoordinatorError::DurableCoordinatorRequired)?
            .try_borrow_mut()
            .map_err(|_| CoordinatorError::CoordinatorBusy)?
            .complete(self.id)?;
        self.state = TransactionState::Committed;
        #[cfg(test)]
        crate::coordinator_crash::maybe_crash("after-durable-complete");
        Ok(())
    }

    pub fn rollback(&mut self) -> Result<(), CoordinatorError> {
        match self.state {
            TransactionState::Active
            | TransactionState::RollbackRequired
            | TransactionState::Preparing => {
                self.state = TransactionState::RollbackPending;
            }
            TransactionState::RollbackPending => {}
            TransactionState::DecisionPending
            | TransactionState::CommitDecided
            | TransactionState::ApplyingCommit
            | TransactionState::FinalizePending
            | TransactionState::Committed => {
                return Err(CoordinatorError::CommitAlreadyDecided {
                    transaction_id: self.id,
                    state: self.state,
                });
            }
            state => {
                return Err(CoordinatorError::NotActive {
                    transaction_id: self.id,
                    state,
                });
            }
        }

        self.rollback_before_decision()
    }

    pub fn abort(&mut self) -> Result<(), CoordinatorError> {
        self.rollback()
    }

    pub(crate) fn require_schema_rollback(&mut self) {
        self.state = TransactionState::RollbackRequired;
    }
    pub(crate) fn set_coordinator(&mut self, coordinator: SharedCoordinatorLog) {
        self.coordinator = Some(coordinator);
    }
    pub(crate) fn shared_coordinator(&self) -> Option<SharedCoordinatorLog> {
        self.coordinator.as_ref().map(Rc::clone)
    }
    pub(crate) fn coordinator_decisions(
        &self,
    ) -> Result<Vec<crate::CoordinatorDecision>, CoordinatorError> {
        Ok(self
            .coordinator
            .as_ref()
            .ok_or(CoordinatorError::DurableCoordinatorRequired)?
            .try_borrow()
            .map_err(|_| CoordinatorError::CoordinatorBusy)?
            .decisions()
            .cloned()
            .collect())
    }
    #[cfg(test)]
    pub(crate) fn enlist_staged(
        &mut self,
        mut storage: netbadb_storage::TableStorage,
    ) -> Result<(), CoordinatorError> {
        let id = storage.storage_id();
        let context = storage.begin_transaction_with_isolation(self.isolation_level)?;
        let mutation = self
            .schema_mutation
            .as_mut()
            .ok_or(SchemaMutationError::Corrupt(
                "enlist without schema mutation",
            ))?;
        if self.participants.contains_key(&id) {
            return Err(SchemaMutationError::Corrupt("duplicate staged participant").into());
        }
        mutation.staged = Some(storage);
        self.participants.insert(
            id,
            StorageParticipant {
                mode: ParticipantMode::Write,
                context,
            },
        );
        self.write_participants.insert(id);
        Ok(())
    }

    pub(crate) fn enlist_composed_staged(
        &mut self,
        mut storage: netbadb_storage::TableStorage,
    ) -> Result<(), CoordinatorError> {
        let id = storage.storage_id();
        if self.participants.contains_key(&id) {
            return Err(SchemaMutationError::Corrupt("duplicate staged participant").into());
        }
        let context = storage.begin_transaction_with_isolation(self.isolation_level)?;
        let duplicate = if let Some(materialized) = self.schema_composition.materialized_mut() {
            materialized.staged.insert(id, storage).is_some()
        } else if let Some(materialized) = self.schema_composition.materialized_index_mut() {
            materialized.staged.insert(id, storage).is_some()
        } else if let Some(materialized) = self.schema_composition.backfill_mut() {
            materialized.staged.insert(id, storage).is_some()
        } else {
            return Err(
                SchemaMutationError::Corrupt("composed enlist outside materialization").into(),
            );
        };
        if duplicate {
            return Err(SchemaMutationError::Corrupt("duplicate composed Heap").into());
        }
        self.participants.insert(
            id,
            StorageParticipant {
                mode: ParticipantMode::Write,
                context,
            },
        );
        self.write_participants.insert(id);
        Ok(())
    }
    pub(crate) fn execution_staged_storages_mut(
        &mut self,
    ) -> Vec<&mut netbadb_storage::TableStorage> {
        match &mut self.schema_composition {
            SchemaCompositionState::SealingAndMaterializing(materialized)
            | SchemaCompositionState::Materialized(materialized)
            | SchemaCompositionState::RollbackRequiredMaterialized(materialized) => {
                return materialized.staged.values_mut().collect();
            }
            SchemaCompositionState::BackfillMaterializing(materialized)
            | SchemaCompositionState::BackfillOpen(materialized)
            | SchemaCompositionState::Refining(materialized)
            | SchemaCompositionState::Finalizing(materialized)
            | SchemaCompositionState::Finalized(materialized) => {
                return materialized.staged.values_mut().collect();
            }
            SchemaCompositionState::SealingAndMaterializingIndex(materialized)
            | SchemaCompositionState::MaterializedIndex(materialized)
            | SchemaCompositionState::BackfillOpenIndex(materialized)
            | SchemaCompositionState::RefiningIndex(materialized)
            | SchemaCompositionState::FinalizedIndex(materialized)
            | SchemaCompositionState::RollbackRequiredMaterializedIndex(materialized) => {
                return materialized.staged.values_mut().collect();
            }
            _ => {}
        }
        self.schema_mutation
            .as_mut()
            .and_then(|mutation| mutation.staged.as_mut())
            .into_iter()
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn with_staged_write<T>(
        &mut self,
        operation: impl FnOnce(
            &mut netbadb_storage::TableStorage,
            &mut StorageTransaction,
        ) -> Result<T, StorageError>,
    ) -> Result<T, CoordinatorError> {
        self.ensure_active()?;
        let storage_id = self
            .schema_mutation
            .as_ref()
            .and_then(|mutation| mutation.staged.as_ref())
            .map(netbadb_storage::TableStorage::storage_id)
            .ok_or(SchemaMutationError::Corrupt("staged Heap is absent"))?;
        let context = &mut self
            .participants
            .get_mut(&storage_id)
            .ok_or(CoordinatorError::UnknownStorageId { storage_id })?
            .context;
        let storage = self
            .schema_mutation
            .as_mut()
            .and_then(|mutation| mutation.staged.as_mut())
            .ok_or(SchemaMutationError::Corrupt("staged Heap is absent"))?;
        Ok(operation(storage, context)?)
    }
    pub(crate) fn with_composed_staged_write<T>(
        &mut self,
        storage_id: StorageId,
        operation: impl FnOnce(
            &mut netbadb_storage::TableStorage,
            &mut StorageTransaction,
        ) -> Result<T, StorageError>,
    ) -> Result<T, CoordinatorError> {
        self.ensure_active()?;
        let context = &mut self
            .participants
            .get_mut(&storage_id)
            .ok_or(CoordinatorError::UnknownStorageId { storage_id })?
            .context;
        let storage = if let Some(storage) = self
            .schema_composition
            .materialized_mut()
            .and_then(|materialized| materialized.staged.get_mut(&storage_id))
        {
            storage
        } else if let Some(storage) = self
            .schema_composition
            .backfill_mut()
            .and_then(|materialized| materialized.staged.get_mut(&storage_id))
        {
            storage
        } else {
            self.schema_composition
                .materialized_index_mut()
                .and_then(|materialized| materialized.staged.get_mut(&storage_id))
                .ok_or(SchemaMutationError::Corrupt(
                    "composed staged Heap is absent",
                ))?
        };
        Ok(operation(storage, context)?)
    }

    pub(crate) fn with_detached_composed_staged_write<T>(
        &mut self,
        storage_id: StorageId,
        storage: &mut netbadb_storage::TableStorage,
        operation: impl FnOnce(
            &mut netbadb_storage::TableStorage,
            &mut StorageTransaction,
        ) -> Result<T, StorageError>,
    ) -> Result<T, CoordinatorError> {
        self.ensure_active()?;
        let context = &mut self
            .participants
            .get_mut(&storage_id)
            .ok_or(CoordinatorError::UnknownStorageId { storage_id })?
            .context;
        Ok(operation(storage, context)?)
    }
    /// Whether this active transaction privately owns a staged table identity.
    /// This reports lifecycle state only; authorization remains a server policy.
    #[must_use]
    pub fn owns_staged_table(&self, table: netbadb_types::TableId) -> bool {
        self.state() == TransactionState::Active
            && (self.staged_binding(table).is_some()
                || self.schema_composition.plan().is_some_and(|plan| {
                    plan.touched.contains_key(&table)
                        || plan
                            .created
                            .get(&table)
                            .is_some_and(|created| created.present)
                }))
    }

    pub(crate) fn staged_binding(&self, table: netbadb_types::TableId) -> Option<StorageId> {
        self.schema_composition
            .materialized()
            .and_then(|materialized| {
                materialized
                    .intent
                    .tables
                    .iter()
                    .find(|plan| plan.table() == table)
                    .map(|plan| plan.new_storage())
            })
            .or_else(|| {
                self.schema_composition
                    .materialized_index()
                    .and_then(|materialized| {
                        materialized
                            .intent
                            .tables
                            .iter()
                            .find_map(|plan| {
                                match plan {
                            crate::schema_mutation_journal::SchemaIndexTablePlan::CreateHeap {
                                target,
                                ..
                            } if target.committed.schema.tables()[0].id == table => {
                                Some(target.storages[0].id)
                            }
                            crate::schema_mutation_journal::SchemaIndexTablePlan::RewriteHeap {
                                replacement,
                                ..
                            } if replacement.table() == table => Some(replacement.new_storage()),
                            _ => None,
                        }
                            })
                    })
            })
            .or_else(|| {
                self.schema_composition.backfill().and_then(|materialized| {
                    materialized
                        .intent
                        .tables
                        .iter()
                        .find(|plan| plan.table() == table)
                        .map(|plan| plan.new_storage())
                })
            })
            .or_else(|| {
                self.schema_mutation
                    .as_ref()
                    .filter(|m| m.reservation.table == table && m.staged.is_some())
                    .map(|m| m.reservation.storage)
            })
    }
    pub(crate) fn visible_schema<'a>(
        &'a self,
        committed: &'a netbadb_schema::Schema,
    ) -> &'a netbadb_schema::Schema {
        self.schema_composition
            .plan()
            .map(|plan| &plan.overlay.schema)
            .or_else(|| {
                self.schema_mutation
                    .as_ref()
                    .map(|m| &m.target.committed.schema)
            })
            .unwrap_or(committed)
    }
    pub(crate) fn release_staged_context(&mut self, id: StorageId) {
        self.participants.remove(&id);
        self.write_participants.remove(&id);
    }
    pub(crate) fn finish_schema_decision(&mut self) -> Result<(), CoordinatorError> {
        self.coordinator
            .as_ref()
            .ok_or(CoordinatorError::DurableCoordinatorRequired)?
            .try_borrow_mut()
            .map_err(|_| CoordinatorError::CoordinatorBusy)?
            .complete(self.id)?;
        Ok(())
    }
    pub(crate) fn complete_schema_publication(&mut self) {
        self.schema_mutation = None;
        self.schema_composition = SchemaCompositionState::None;
        self.state = TransactionState::Committed;
    }
    pub(crate) fn with_write_storage<T>(
        &mut self,
        storage_id: StorageId,
        registry: &mut StorageRegistry,
        operation: impl FnOnce(
            &mut netbadb_storage::TableStorage,
            &mut StorageTransaction,
        ) -> Result<T, StorageError>,
    ) -> Result<T, CoordinatorError> {
        let _ = self.write_context(storage_id, registry)?;
        let context = &mut self
            .participants
            .get_mut(&storage_id)
            .ok_or(CoordinatorError::UnknownStorageId { storage_id })?
            .context;
        let storage = match self
            .schema_composition
            .materialized_mut()
            .and_then(|materialized| materialized.staged.get_mut(&storage_id))
        {
            Some(storage) => storage,
            None => match self
                .schema_composition
                .materialized_index_mut()
                .and_then(|materialized| materialized.staged.get_mut(&storage_id))
            {
                Some(storage) => storage,
                None => match self
                    .schema_composition
                    .backfill_mut()
                    .and_then(|materialized| materialized.staged.get_mut(&storage_id))
                {
                    Some(storage) => storage,
                    None => match self
                        .schema_mutation
                        .as_mut()
                        .and_then(|m| m.staged.as_mut())
                        .filter(|s| s.storage_id() == storage_id)
                    {
                        Some(storage) => storage,
                        None => registry
                            .get_mut(storage_id)
                            .ok_or(CoordinatorError::UnknownStorageId { storage_id })?,
                    },
                },
            },
        };
        Ok(operation(storage, context)?)
    }

    fn ensure_active(&self) -> Result<(), CoordinatorError> {
        if self.state != TransactionState::Active {
            return Err(CoordinatorError::NotActive {
                transaction_id: self.id,
                state: self.state,
            });
        }
        Ok(())
    }

    fn rollback_before_decision(&mut self) -> Result<(), CoordinatorError> {
        for storage_id in self.write_participants.iter().copied() {
            let participant = self.participants.get_mut(&storage_id).ok_or(
                CoordinatorError::ParticipantStateViolation {
                    storage_id,
                    reason: "write participant identity has no context",
                },
            )?;
            rollback_participant(self.id, storage_id, participant)?;
        }
        for (storage_id, participant) in self
            .participants
            .iter_mut()
            .filter(|(_, participant)| participant.mode == ParticipantMode::Read)
        {
            rollback_participant(self.id, *storage_id, participant)?;
        }
        if let Some(mutation) = &mut self.schema_mutation {
            self.participants.remove(&mutation.reservation.storage);
            self.write_participants
                .remove(&mutation.reservation.storage);
            crate::schema_mutation::crash("rollback-participants-durable");
            mutation.cleanup_loser()?;
        }
        if self.schema_composition.is_started() {
            let staged = self
                .schema_composition
                .materialized()
                .map(|materialized| materialized.staged.keys().copied().collect::<Vec<_>>())
                .or_else(|| {
                    self.schema_composition
                        .materialized_index()
                        .map(|materialized| materialized.staged.keys().copied().collect::<Vec<_>>())
                })
                .unwrap_or_default();
            for storage_id in staged {
                self.participants.remove(&storage_id);
                self.write_participants.remove(&storage_id);
            }
            crate::schema_composition::cleanup_composition_loser(&mut self.schema_composition)?;
        }
        self.schema_mutation = None;
        self.state = TransactionState::RolledBack;
        Ok(())
    }

    fn ensure_participant(
        &mut self,
        storage_id: StorageId,
        registry: &mut StorageRegistry,
    ) -> Result<(), CoordinatorError> {
        if self.participants.contains_key(&storage_id) {
            return Ok(());
        }
        let storage = registry
            .get_mut(storage_id)
            .ok_or(CoordinatorError::UnknownStorageId { storage_id })?;
        let context = storage.begin_transaction_with_isolation(self.isolation_level)?;
        self.participants.insert(
            storage_id,
            StorageParticipant {
                mode: ParticipantMode::Read,
                context,
            },
        );
        Ok(())
    }
}

fn commit_participant(
    storage_id: StorageId,
    participant: &mut StorageParticipant,
) -> Result<(), CoordinatorError> {
    match participant.context.state() {
        StorageTransactionState::Committed => Ok(()),
        StorageTransactionState::Active | StorageTransactionState::CommitPending => {
            participant.context.commit().map_err(CoordinatorError::from)
        }
        state => Err(CoordinatorError::ParticipantStateViolation {
            storage_id,
            reason: storage_commit_state_reason(state),
        }),
    }
}

fn prepare_participant(
    database_txn_id: DatabaseTxnId,
    storage_id: StorageId,
    participant: &mut StorageParticipant,
) -> Result<(), CoordinatorError> {
    match participant.context.state() {
        StorageTransactionState::Active
        | StorageTransactionState::PreparePending
        | StorageTransactionState::Prepared => participant
            .context
            .prepare(database_txn_id)
            .map_err(CoordinatorError::from),
        state => Err(CoordinatorError::ParticipantStateViolation {
            storage_id,
            reason: storage_prepare_state_reason(state),
        }),
    }
}

fn commit_prepared_participant(
    database_txn_id: DatabaseTxnId,
    storage_id: StorageId,
    participant: &mut StorageParticipant,
) -> Result<(), CoordinatorError> {
    match participant.context.state() {
        StorageTransactionState::Prepared
        | StorageTransactionState::CommitPending
        | StorageTransactionState::Committed => participant
            .context
            .commit_prepared(database_txn_id)
            .map_err(CoordinatorError::from),
        state => Err(CoordinatorError::ParticipantStateViolation {
            storage_id,
            reason: storage_commit_state_reason(state),
        }),
    }
}

fn rollback_participant(
    database_txn_id: DatabaseTxnId,
    storage_id: StorageId,
    participant: &mut StorageParticipant,
) -> Result<(), CoordinatorError> {
    match participant.context.state() {
        StorageTransactionState::RolledBack => Ok(()),
        StorageTransactionState::Committed if participant.mode == ParticipantMode::Read => Ok(()),
        StorageTransactionState::PreparePending | StorageTransactionState::Prepared => participant
            .context
            .rollback_prepared(database_txn_id)
            .map_err(CoordinatorError::from),
        StorageTransactionState::Active
        | StorageTransactionState::RollbackRequired
        | StorageTransactionState::RollbackPending => participant
            .context
            .rollback()
            .map_err(CoordinatorError::from),
        _ => Err(CoordinatorError::ParticipantStateViolation {
            storage_id,
            reason: "participant cannot be rolled back from its physical state",
        }),
    }
}

fn storage_prepare_state_reason(state: StorageTransactionState) -> &'static str {
    match state {
        StorageTransactionState::RollbackRequired => "participant requires rollback",
        StorageTransactionState::CommitPending => "participant commit is pending",
        StorageTransactionState::RollbackPending => "participant rollback is pending",
        StorageTransactionState::Committed => "participant is already committed",
        StorageTransactionState::RolledBack => "participant is already rolled back",
        StorageTransactionState::Active
        | StorageTransactionState::PreparePending
        | StorageTransactionState::Prepared => "invalid participant prepare state",
    }
}

fn storage_commit_state_reason(state: StorageTransactionState) -> &'static str {
    match state {
        StorageTransactionState::RollbackRequired => {
            "participant requires rollback and cannot commit"
        }
        StorageTransactionState::RollbackPending => {
            "participant rollback is pending and cannot commit"
        }
        StorageTransactionState::PreparePending => "participant prepare is pending",
        StorageTransactionState::Prepared => "participant requires a coordinator decision",
        StorageTransactionState::RolledBack => "participant is already rolled back",
        StorageTransactionState::Active
        | StorageTransactionState::CommitPending
        | StorageTransactionState::Committed => "invalid participant commit state",
    }
}

#[derive(Debug)]
pub enum CoordinatorError {
    SchemaMutation(SchemaMutationError),
    Storage(StorageError),
    Registry(StorageRegistryError),
    CoordinatorLog(CoordinatorLogError),
    ForeignDatabaseTransaction {
        transaction_id: DatabaseTxnId,
    },
    UnknownStorageId {
        storage_id: StorageId,
    },
    MultipleWriteParticipantsUnsupported {
        existing: StorageId,
        requested: StorageId,
    },
    DurableCoordinatorRequired,
    CoordinatorBusy,
    PrepareFailed {
        storage_id: StorageId,
        source: Box<CoordinatorError>,
    },
    CommitAlreadyDecided {
        transaction_id: DatabaseTxnId,
        state: TransactionState,
    },
    ParticipantStateViolation {
        storage_id: StorageId,
        reason: &'static str,
    },
    NotActive {
        transaction_id: DatabaseTxnId,
        state: TransactionState,
    },
    TransactionIdExhausted,
    SchemaMutationRequiresDatabaseCommit,
}

impl fmt::Display for CoordinatorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SchemaMutation(error) => error.fmt(formatter),
            Self::Storage(error) => error.fmt(formatter),
            Self::Registry(error) => error.fmt(formatter),
            Self::CoordinatorLog(error) => error.fmt(formatter),
            Self::ForeignDatabaseTransaction { transaction_id } => write!(
                formatter,
                "database transaction {} belongs to another opened database",
                transaction_id.0
            ),
            Self::UnknownStorageId { storage_id } => {
                write!(
                    formatter,
                    "physical storage {} is not registered",
                    storage_id.0
                )
            }
            Self::MultipleWriteParticipantsUnsupported {
                existing,
                requested,
            } => write!(
                formatter,
                "database transaction already writes physical storage {}; atomic writes to second storage {} are unsupported",
                existing.0, requested.0
            ),
            Self::DurableCoordinatorRequired => formatter
                .write_str("atomic multi-storage commit requires a durable coordinator log"),
            Self::CoordinatorBusy => {
                formatter.write_str("database coordinator log is already borrowed")
            }
            Self::PrepareFailed { storage_id, source } => write!(
                formatter,
                "physical storage {} failed to prepare and the database transaction was aborted: {source}",
                storage_id.0
            ),
            Self::CommitAlreadyDecided {
                transaction_id,
                state,
            } => write!(
                formatter,
                "database transaction {} has a durable or uncertain commit decision in state {state:?} and cannot roll back",
                transaction_id.0
            ),
            Self::ParticipantStateViolation { storage_id, reason } => write!(
                formatter,
                "physical storage {} participant state is invalid: {reason}",
                storage_id.0
            ),
            Self::NotActive {
                transaction_id,
                state,
            } => write!(
                formatter,
                "database transaction {} is not active: {state:?}",
                transaction_id.0
            ),
            Self::TransactionIdExhausted => {
                formatter.write_str("database transaction identity space is exhausted")
            }
            Self::SchemaMutationRequiresDatabaseCommit => formatter.write_str(
                "transaction contains schema mutations and must be committed through Database",
            ),
        }
    }
}

impl Error for CoordinatorError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::SchemaMutation(error) => Some(error),
            Self::Storage(error) => Some(error),
            Self::Registry(error) => Some(error),
            Self::CoordinatorLog(error) => Some(error),
            Self::PrepareFailed { source, .. } => Some(source.as_ref()),
            Self::ForeignDatabaseTransaction { .. }
            | Self::UnknownStorageId { .. }
            | Self::MultipleWriteParticipantsUnsupported { .. }
            | Self::DurableCoordinatorRequired
            | Self::CoordinatorBusy
            | Self::CommitAlreadyDecided { .. }
            | Self::ParticipantStateViolation { .. }
            | Self::NotActive { .. }
            | Self::TransactionIdExhausted
            | Self::SchemaMutationRequiresDatabaseCommit => None,
        }
    }
}

impl From<StorageError> for CoordinatorError {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
}

impl From<StorageRegistryError> for CoordinatorError {
    fn from(error: StorageRegistryError) -> Self {
        Self::Registry(error)
    }
}

impl From<CoordinatorLogError> for CoordinatorError {
    fn from(error: CoordinatorLogError) -> Self {
        Self::CoordinatorLog(error)
    }
}

impl From<SchemaMutationError> for CoordinatorError {
    fn from(error: SchemaMutationError) -> Self {
        Self::SchemaMutation(error)
    }
}
