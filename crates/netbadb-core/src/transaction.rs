use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;
use std::rc::Rc;

use netbadb_storage::{
    IsolationLevel, StorageError, StorageReadView, StorageTransaction,
    TransactionState as StorageTransactionState,
};
use netbadb_types::{DatabaseTxnId, StorageId};

use crate::coordinator_log::{CoordinatorLog, CoordinatorLogError, CoordinatorParticipant};
use crate::registry::{StorageRegistry, StorageRegistryError};

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

    pub(crate) fn validate_owner(&self, owner: &Rc<()>) -> Result<(), CoordinatorError> {
        if !Rc::ptr_eq(&self.owner, owner) {
            return Err(CoordinatorError::ForeignDatabaseTransaction {
                transaction_id: self.id,
            });
        }
        self.ensure_active()
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
        if self.write_participants.len() > 1 {
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
            self.coordinator
                .as_ref()
                .ok_or(CoordinatorError::DurableCoordinatorRequired)?
                .try_borrow_mut()
                .map_err(|_| CoordinatorError::CoordinatorBusy)?
                .commit_decision(self.id, &decision_participants)?;
            self.state = TransactionState::CommitDecided;
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
}

impl fmt::Display for CoordinatorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
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
        }
    }
}

impl Error for CoordinatorError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
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
            | Self::TransactionIdExhausted => None,
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
