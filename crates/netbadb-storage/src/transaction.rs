use std::cell::{Cell, RefCell};
use std::rc::Rc;

use netbadb_types::{Lsn, TxnId};

use crate::wal::page_update_kind;
use crate::{Page, StorageError, TransactionError, WalManager, WalRecordKind};

pub(crate) type SharedWal = Rc<RefCell<WalManager>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WriterState {
    Idle,
    Active(TxnId),
    RecoveryRequired,
}

type SharedWriterState = Rc<Cell<WriterState>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    Active,
    CommitPending,
    Committed,
    Aborted,
}

/// A synchronous transaction handle. Transactions provide durable commit and
/// startup recovery, but do not provide runtime rollback or isolation.
#[derive(Debug)]
pub struct Transaction {
    id: TxnId,
    state: TransactionState,
    last_lsn: Lsn,
    wal: SharedWal,
    writer_state: SharedWriterState,
    has_page_updates: bool,
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
        self.release_writer();
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
        if self.has_page_updates {
            self.writer_state.set(WriterState::RecoveryRequired);
        } else {
            self.release_writer();
        }
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
        self.has_page_updates = true;
        Ok(actual)
    }

    pub(crate) fn flush_through(&self, lsn: Lsn) -> Result<(), StorageError> {
        self.wal
            .try_borrow_mut()
            .map_err(|_| TransactionError::WalBusy)?
            .flush_through(lsn)?;
        Ok(())
    }

    fn release_writer(&self) {
        if self.writer_state.get() == WriterState::Active(self.id) {
            self.writer_state.set(WriterState::Idle);
        }
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if self.state == TransactionState::CommitPending
            || (self.state == TransactionState::Active && self.has_page_updates)
        {
            self.writer_state.set(WriterState::RecoveryRequired);
        } else if self.state == TransactionState::Active {
            self.release_writer();
        }
    }
}

#[derive(Debug)]
pub(crate) struct TransactionManager {
    wal: SharedWal,
    next_txn_id: TxnId,
    writer_state: SharedWriterState,
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
            writer_state: Rc::new(Cell::new(WriterState::Idle)),
        })
    }

    pub(crate) fn begin(&mut self) -> Result<Transaction, StorageError> {
        match self.writer_state.get() {
            WriterState::Idle => {}
            WriterState::Active(txn_id) => {
                return Err(TransactionError::WriterBusy { txn_id }.into());
            }
            WriterState::RecoveryRequired => {
                return Err(TransactionError::RecoveryRequired.into());
            }
        }
        let id = self.next_txn_id;
        let next = id.0.checked_add(1).ok_or(TransactionError::IdExhausted)?;
        let begin_lsn = self
            .wal
            .try_borrow_mut()
            .map_err(|_| TransactionError::WalBusy)?
            .append(id, None, WalRecordKind::Begin)?;
        self.next_txn_id = TxnId(next);
        self.writer_state.set(WriterState::Active(id));
        Ok(Transaction {
            id,
            state: TransactionState::Active,
            last_lsn: begin_lsn,
            wal: Rc::clone(&self.wal),
            writer_state: Rc::clone(&self.writer_state),
            has_page_updates: false,
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
    use netbadb_types::PageId;

    use crate::{Page, PageType, StorageError, TransactionError, WalManager, WalRecordKind};

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

    #[test]
    fn only_one_writer_can_be_active_and_commit_releases_it() {
        let path = test_path("txn-single-writer");
        let wal = Rc::new(RefCell::new(WalManager::create(&path).expect("create WAL")));
        let mut manager = TransactionManager::new(Rc::clone(&wal), None).expect("manager");
        let mut first = manager.begin().expect("begin first transaction");

        assert!(matches!(
            manager.begin(),
            Err(StorageError::Transaction(TransactionError::WriterBusy {
                txn_id
            })) if txn_id == first.id()
        ));
        first.commit().expect("commit first transaction");
        let second = manager.begin().expect("begin after commit");
        drop(second);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn dirty_loser_requires_recovery_before_another_writer() {
        for (name, abort) in [("txn-dirty-abort", true), ("txn-dirty-drop", false)] {
            let path = test_path(name);
            let wal = Rc::new(RefCell::new(WalManager::create(&path).expect("create WAL")));
            let mut manager = TransactionManager::new(Rc::clone(&wal), None).expect("manager");
            let mut transaction = manager.begin().expect("begin transaction");
            let before = Page::new(PageId(1), PageType::Heap);
            let mut after = before.clone();
            after.insert_record(b"dirty").expect("insert row");
            transaction
                .log_page_update(&before, &mut after)
                .expect("log page update");
            if abort {
                transaction.abort().expect("abort transaction");
            }
            drop(transaction);

            assert!(matches!(
                manager.begin(),
                Err(StorageError::Transaction(
                    TransactionError::RecoveryRequired
                ))
            ));
            let _ = std::fs::remove_file(path);
        }
    }

    #[test]
    fn dropped_commit_pending_writer_requires_recovery() {
        let path = test_path("txn-dropped-commit-pending");
        let wal = Rc::new(RefCell::new(WalManager::create(&path).expect("create WAL")));
        let mut manager = TransactionManager::new(Rc::clone(&wal), None).expect("manager");
        let mut transaction = manager.begin().expect("begin transaction");
        wal.borrow_mut().inject_flush_failure();
        assert!(matches!(transaction.commit(), Err(StorageError::Wal(_))));
        assert_eq!(transaction.state(), TransactionState::CommitPending);
        drop(transaction);

        assert!(matches!(
            manager.begin(),
            Err(StorageError::Transaction(
                TransactionError::RecoveryRequired
            ))
        ));
        let _ = std::fs::remove_file(path);
    }
}
