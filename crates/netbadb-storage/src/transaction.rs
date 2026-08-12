use std::cell::RefCell;
use std::rc::Rc;

use netbadb_types::{Lsn, TxnId};

use crate::wal::page_update_kind;
use crate::{Page, StorageError, TransactionError, WalManager, WalRecordKind};

pub(crate) type SharedWal = Rc<RefCell<WalManager>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    Active,
    CommitPending,
    Committed,
    Aborted,
}

/// A synchronous transaction handle. Phase 2A makes commit durable but does
/// not yet provide rollback, isolation, or crash recovery.
#[derive(Debug)]
pub struct Transaction {
    id: TxnId,
    state: TransactionState,
    last_lsn: Lsn,
    wal: SharedWal,
}

impl Transaction {
    #[must_use]
    pub fn id(&self) -> TxnId {
        self.id
    }

    #[must_use]
    pub fn state(&self) -> TransactionState {
        self.state
    }

    #[must_use]
    pub fn last_lsn(&self) -> Lsn {
        self.last_lsn
    }

    pub fn commit(&mut self) -> Result<(), StorageError> {
        let commit_lsn = match self.state {
            TransactionState::Active => {
                let mut wal = self
                    .wal
                    .try_borrow_mut()
                    .map_err(|_| TransactionError::WalBusy)?;
                let lsn = wal.append(self.id, Some(self.last_lsn), WalRecordKind::Commit)?;
                self.last_lsn = lsn;
                self.state = TransactionState::CommitPending;
                lsn
            }
            TransactionState::CommitPending => self.last_lsn,
            state => {
                return Err(TransactionError::NotActive {
                    txn_id: self.id,
                    state,
                }
                .into());
            }
        };

        self.wal
            .try_borrow_mut()
            .map_err(|_| TransactionError::WalBusy)?
            .flush_through(commit_lsn)?;
        self.state = TransactionState::Committed;
        Ok(())
    }

    pub fn abort(&mut self) -> Result<(), StorageError> {
        self.ensure_active()?;
        let lsn = self
            .wal
            .try_borrow_mut()
            .map_err(|_| TransactionError::WalBusy)?
            .append(self.id, Some(self.last_lsn), WalRecordKind::Abort)?;
        self.last_lsn = lsn;
        self.state = TransactionState::Aborted;
        Ok(())
    }

    pub(crate) fn ensure_active(&self) -> Result<(), StorageError> {
        if self.state != TransactionState::Active {
            return Err(TransactionError::NotActive {
                txn_id: self.id,
                state: self.state,
            }
            .into());
        }
        Ok(())
    }

    pub(crate) fn belongs_to(&self, wal: &SharedWal) -> bool {
        Rc::ptr_eq(&self.wal, wal)
    }

    pub(crate) fn log_page_update(
        &mut self,
        before: &Page,
        after: &mut Page,
    ) -> Result<Lsn, StorageError> {
        self.ensure_active()?;
        let mut wal = self
            .wal
            .try_borrow_mut()
            .map_err(|_| TransactionError::WalBusy)?;
        let lsn = wal.next_lsn();
        after.set_page_lsn(lsn);
        let actual = wal.append(
            self.id,
            Some(self.last_lsn),
            page_update_kind(before, after),
        )?;
        debug_assert_eq!(actual, lsn);
        self.last_lsn = actual;
        Ok(actual)
    }

    pub(crate) fn flush_through(&self, lsn: Lsn) -> Result<(), StorageError> {
        self.wal
            .try_borrow_mut()
            .map_err(|_| TransactionError::WalBusy)?
            .flush_through(lsn)?;
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct TransactionManager {
    wal: SharedWal,
    next_txn_id: TxnId,
}

impl TransactionManager {
    pub(crate) fn new(
        wal: SharedWal,
        records_max_txn: Option<TxnId>,
    ) -> Result<Self, StorageError> {
        let next = match records_max_txn {
            Some(maximum) => maximum
                .0
                .checked_add(1)
                .ok_or(TransactionError::IdExhausted)?,
            None => 1,
        };
        Ok(Self {
            wal,
            next_txn_id: TxnId(next),
        })
    }

    pub(crate) fn begin(&mut self) -> Result<Transaction, StorageError> {
        let id = self.next_txn_id;
        let next = id.0.checked_add(1).ok_or(TransactionError::IdExhausted)?;
        let begin_lsn = self
            .wal
            .try_borrow_mut()
            .map_err(|_| TransactionError::WalBusy)?
            .append(id, None, WalRecordKind::Begin)?;
        self.next_txn_id = TxnId(next);
        Ok(Transaction {
            id,
            state: TransactionState::Active,
            last_lsn: begin_lsn,
            wal: Rc::clone(&self.wal),
        })
    }

    pub(crate) fn wal(&self) -> &SharedWal {
        &self.wal
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use super::{TransactionManager, TransactionState};
    use crate::{StorageError, WalManager, WalRecordKind};

    fn test_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("netbadb-{name}-{}-wal", std::process::id()))
    }

    #[test]
    fn commit_record_is_durable_before_state_becomes_committed() {
        let path = test_path("txn-commit");
        let wal = Rc::new(RefCell::new(WalManager::create(&path).expect("create WAL")));
        let mut manager = TransactionManager::new(Rc::clone(&wal), None).expect("manager");
        let mut transaction = manager.begin().expect("begin transaction");
        transaction.commit().expect("commit transaction");

        assert_eq!(transaction.state(), TransactionState::Committed);
        assert_eq!(wal.borrow().durable_lsn(), Some(transaction.last_lsn()));
        let records = wal.borrow_mut().scan().expect("scan WAL");
        assert!(matches!(
            records.last().map(|record| &record.kind),
            Some(WalRecordKind::Commit)
        ));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn failed_commit_flush_remains_pending_and_retry_does_not_duplicate_commit() {
        let path = test_path("txn-commit-failure");
        let wal = Rc::new(RefCell::new(WalManager::create(&path).expect("create WAL")));
        let mut manager = TransactionManager::new(Rc::clone(&wal), None).expect("manager");
        let mut transaction = manager.begin().expect("begin transaction");
        wal.borrow_mut().inject_flush_failure();

        assert!(matches!(transaction.commit(), Err(StorageError::Wal(_))));
        assert_eq!(transaction.state(), TransactionState::CommitPending);
        assert_eq!(wal.borrow_mut().scan().expect("scan WAL").len(), 2);

        transaction.commit().expect("retry commit flush");
        assert_eq!(transaction.state(), TransactionState::Committed);
        assert_eq!(wal.borrow_mut().scan().expect("scan WAL").len(), 2);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn abort_is_logged_and_transitions_state_without_claiming_undo() {
        let path = test_path("txn-abort");
        let wal = Rc::new(RefCell::new(WalManager::create(&path).expect("create WAL")));
        let mut manager = TransactionManager::new(Rc::clone(&wal), None).expect("manager");
        let mut transaction = manager.begin().expect("begin transaction");
        transaction.abort().expect("abort transaction");

        assert_eq!(transaction.state(), TransactionState::Aborted);
        let records = wal.borrow_mut().scan().expect("scan WAL");
        assert!(matches!(records[0].kind, WalRecordKind::Begin));
        assert!(matches!(records[1].kind, WalRecordKind::Abort));
        assert!(transaction.abort().is_err());
        let _ = std::fs::remove_file(path);
    }
}
