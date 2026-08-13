use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;

use netbadb_types::{Lsn, TxnId};

use crate::wal::page_update_kind;
use crate::{
    BufferPool, Page, StorageError, TransactionError, WalManager, WalRecord, WalRecordKind,
};

pub(crate) type SharedWal = Rc<RefCell<WalManager>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WriterState {
    Idle,
    Active(TxnId),
    RecoveryRequired,
}

#[derive(Debug)]
struct TransactionRuntime {
    writer: Cell<WriterState>,
    outstanding: Cell<u64>,
}

type SharedRuntime = Rc<TransactionRuntime>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    Active,
    /// A compound logical operation logged only part of its physical work.
    /// The transaction may be rolled back but cannot continue or commit.
    RollbackRequired,
    CommitPending,
    RollbackPending,
    Committed,
    RolledBack,
}

/// A synchronous transaction handle with durable commit and physical runtime
/// rollback. The single-writer rule prevents dirty-write dependencies; it is
/// not transaction isolation and reads may observe an active writer's pages.
#[derive(Debug)]
pub struct Transaction {
    id: TxnId,
    state: TransactionState,
    last_lsn: Lsn,
    wal: SharedWal,
    buffer: BufferPool,
    runtime: SharedRuntime,
    registered: bool,
    has_page_updates: bool,
    rollback_start_lsn: Option<Lsn>,
    rollback_complete_lsn: Option<Lsn>,
    #[cfg(test)]
    interrupt_rollback_after: Option<usize>,
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

    /// Makes this transaction's commit record durable. A flush failure leaves
    /// the transaction in `CommitPending`; calling `commit` again retries the
    /// same record without releasing an owned writer.
    pub fn commit(&mut self) -> Result<(), StorageError> {
        let commit_lsn = match self.state {
            TransactionState::Active => {
                let mut wal = self
                    .wal
                    .try_borrow_mut()
                    .map_err(|_| TransactionError::WalBusy)?;
                let lsn = wal.append(self.id, Some(self.last_lsn), WalRecordKind::Commit)?;
                #[cfg(test)]
                crate::crash_test::maybe_crash(
                    crate::crash_test::TestCrashPoint::CommitAfterAppend,
                );
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
        #[cfg(test)]
        crate::crash_test::maybe_crash(crate::crash_test::TestCrashPoint::CommitAfterWalSync);
        self.state = TransactionState::Committed;
        self.release_writer();
        self.unregister();
        Ok(())
    }

    /// Durably records abort intent, physically restores every before-image,
    /// and durably records rollback completion. Success means all physical
    /// effects are synchronized; a failure remains retryable in
    /// `RollbackPending` and retains an owned writer.
    pub fn rollback(&mut self) -> Result<(), StorageError> {
        match self.state {
            TransactionState::Active | TransactionState::RollbackRequired => {
                let rollback_start_lsn = self.last_lsn;
                let abort_lsn = self
                    .wal
                    .try_borrow_mut()
                    .map_err(|_| TransactionError::WalBusy)?
                    .append(self.id, Some(self.last_lsn), WalRecordKind::Abort)?;
                #[cfg(test)]
                crate::crash_test::maybe_crash(
                    crate::crash_test::TestCrashPoint::RollbackAfterAbortAppend,
                );
                self.last_lsn = abort_lsn;
                self.rollback_start_lsn = Some(rollback_start_lsn);
                self.state = TransactionState::RollbackPending;
            }
            TransactionState::RollbackPending => {}
            state => {
                return Err(TransactionError::NotActive {
                    txn_id: self.id,
                    state,
                }
                .into());
            }
        }

        if let Some(complete_lsn) = self.rollback_complete_lsn {
            self.wal
                .try_borrow_mut()
                .map_err(|_| TransactionError::WalBusy)?
                .flush_through(complete_lsn)?;
            self.finish_rollback();
            return Ok(());
        }

        let abort_lsn = self.last_lsn;
        self.wal
            .try_borrow_mut()
            .map_err(|_| TransactionError::WalBusy)?
            .flush_through(abort_lsn)?;
        #[cfg(test)]
        crate::crash_test::maybe_crash(crate::crash_test::TestCrashPoint::RollbackAfterAbortSync);
        let records = self
            .wal
            .try_borrow_mut()
            .map_err(|_| TransactionError::WalBusy)?
            .scan()?;
        let rollback_start_lsn =
            self.rollback_start_lsn
                .ok_or(TransactionError::InvalidRollbackChain {
                    txn_id: self.id,
                    lsn: abort_lsn,
                })?;
        self.undo_records(&records, rollback_start_lsn)?;

        let complete_lsn = self
            .wal
            .try_borrow_mut()
            .map_err(|_| TransactionError::WalBusy)?
            .append(self.id, Some(abort_lsn), WalRecordKind::RollbackComplete)?;
        #[cfg(test)]
        crate::crash_test::maybe_crash(
            crate::crash_test::TestCrashPoint::RollbackAfterCompleteAppend,
        );
        self.last_lsn = complete_lsn;
        self.rollback_complete_lsn = Some(complete_lsn);
        self.wal
            .try_borrow_mut()
            .map_err(|_| TransactionError::WalBusy)?
            .flush_through(complete_lsn)?;
        #[cfg(test)]
        crate::crash_test::maybe_crash(
            crate::crash_test::TestCrashPoint::RollbackAfterCompleteSync,
        );
        self.finish_rollback();
        Ok(())
    }

    /// Alias for [`Self::rollback`]; abort performs physical runtime undo.
    pub fn abort(&mut self) -> Result<(), StorageError> {
        self.rollback()
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

    /// Prevents a transaction with a partially logged compound operation from
    /// committing or performing more writes. Runtime rollback remains allowed.
    pub(crate) fn require_rollback(&mut self) {
        if self.state == TransactionState::Active {
            self.state = TransactionState::RollbackRequired;
        }
    }

    pub(crate) fn belongs_to(&self, wal: &SharedWal) -> bool {
        Rc::ptr_eq(&self.wal, wal)
    }

    pub(crate) fn acquire_writer(&self) -> Result<(), StorageError> {
        self.ensure_active()?;
        match self.runtime.writer.get() {
            WriterState::Idle => {
                self.runtime.writer.set(WriterState::Active(self.id));
                Ok(())
            }
            WriterState::Active(txn_id) if txn_id == self.id => Ok(()),
            WriterState::Active(txn_id) => Err(TransactionError::WriterBusy { txn_id }.into()),
            WriterState::RecoveryRequired => Err(TransactionError::RecoveryRequired.into()),
        }
    }

    pub(crate) fn log_page_update(
        &mut self,
        before: &Page,
        after: &mut Page,
    ) -> Result<Lsn, StorageError> {
        self.acquire_writer()?;
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
        if self.runtime.writer.get() == WriterState::Active(self.id) {
            self.runtime.writer.set(WriterState::Idle);
        }
    }

    fn unregister(&mut self) {
        if self.registered {
            let outstanding = self.runtime.outstanding.get();
            debug_assert!(outstanding > 0);
            self.runtime.outstanding.set(outstanding.saturating_sub(1));
            self.registered = false;
        }
    }

    fn finish_rollback(&mut self) {
        self.state = TransactionState::RolledBack;
        self.release_writer();
        self.unregister();
    }

    fn undo_records(
        &mut self,
        records: &[WalRecord],
        rollback_start_lsn: Lsn,
    ) -> Result<(), StorageError> {
        let record_by_lsn = records
            .iter()
            .map(|record| (record.lsn, record))
            .collect::<HashMap<_, _>>();
        let mut next_lsn = Some(rollback_start_lsn);
        #[cfg(test)]
        let mut operations = 0_usize;
        while let Some(lsn) = next_lsn {
            let record =
                record_by_lsn
                    .get(&lsn)
                    .copied()
                    .ok_or(TransactionError::InvalidRollbackChain {
                        txn_id: self.id,
                        lsn,
                    })?;
            if record.txn_id != self.id {
                return Err(TransactionError::InvalidRollbackChain {
                    txn_id: self.id,
                    lsn,
                }
                .into());
            }
            match &record.kind {
                WalRecordKind::Begin => break,
                WalRecordKind::PageUpdate {
                    page_id, before, ..
                } => {
                    self.buffer.undo_page_update(*page_id, before)?;
                    #[cfg(test)]
                    crate::crash_test::maybe_crash(
                        crate::crash_test::TestCrashPoint::RollbackAfterPageUndo,
                    );
                    #[cfg(test)]
                    {
                        operations += 1;
                        if self
                            .interrupt_rollback_after
                            .is_some_and(|limit| operations >= limit)
                        {
                            self.interrupt_rollback_after = None;
                            return Err(TransactionError::RollbackInterrupted.into());
                        }
                    }
                }
                WalRecordKind::Commit | WalRecordKind::Abort | WalRecordKind::RollbackComplete => {
                    return Err(TransactionError::InvalidRollbackChain {
                        txn_id: self.id,
                        lsn,
                    }
                    .into());
                }
            }
            next_lsn = record.prev_lsn;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn inject_rollback_interruption_after(&mut self, operations: usize) {
        self.interrupt_rollback_after = Some(operations);
    }

    #[cfg(test)]
    pub(crate) fn inject_partial_append_failure(&self, after_bytes: usize) {
        self.wal
            .borrow_mut()
            .inject_partial_append_failure(after_bytes);
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        let owns_writer = self.runtime.writer.get() == WriterState::Active(self.id);
        if owns_writer
            && (matches!(
                self.state,
                TransactionState::RollbackRequired
                    | TransactionState::CommitPending
                    | TransactionState::RollbackPending
            ) || (self.state == TransactionState::Active && self.has_page_updates))
        {
            self.runtime.writer.set(WriterState::RecoveryRequired);
        } else if owns_writer && self.state == TransactionState::Active {
            self.release_writer();
        }
        self.unregister();
    }
}

#[derive(Debug)]
pub(crate) struct TransactionManager {
    wal: SharedWal,
    buffer: BufferPool,
    next_txn_id: TxnId,
    runtime: SharedRuntime,
}

impl TransactionManager {
    pub(crate) fn new(
        wal: SharedWal,
        buffer: BufferPool,
        next_txn_id: TxnId,
    ) -> Result<Self, StorageError> {
        if next_txn_id.0 == 0 {
            return Err(TransactionError::IdExhausted.into());
        }
        Ok(Self {
            wal,
            buffer,
            next_txn_id,
            runtime: Rc::new(TransactionRuntime {
                writer: Cell::new(WriterState::Idle),
                outstanding: Cell::new(0),
            }),
        })
    }

    pub(crate) fn begin(&mut self) -> Result<Transaction, StorageError> {
        let id = self.next_txn_id;
        let next = id.0.checked_add(1).ok_or(TransactionError::IdExhausted)?;
        let outstanding = self
            .runtime
            .outstanding
            .get()
            .checked_add(1)
            .ok_or(TransactionError::OutstandingTransactionCountOverflow)?;
        let begin_lsn = self
            .wal
            .try_borrow_mut()
            .map_err(|_| TransactionError::WalBusy)?
            .append(id, None, WalRecordKind::Begin)?;
        self.runtime.outstanding.set(outstanding);
        self.next_txn_id = TxnId(next);
        Ok(Transaction {
            id,
            state: TransactionState::Active,
            last_lsn: begin_lsn,
            wal: Rc::clone(&self.wal),
            buffer: self.buffer.clone(),
            runtime: Rc::clone(&self.runtime),
            registered: true,
            has_page_updates: false,
            rollback_start_lsn: None,
            rollback_complete_lsn: None,
            #[cfg(test)]
            interrupt_rollback_after: None,
        })
    }

    pub(crate) fn wal(&self) -> &SharedWal {
        &self.wal
    }

    pub(crate) fn next_txn_id(&self) -> TxnId {
        self.next_txn_id
    }

    pub(crate) fn ensure_checkpoint_safe(&self) -> Result<(), crate::CheckpointError> {
        match self.runtime.writer.get() {
            WriterState::Active(txn_id) => {
                return Err(crate::CheckpointError::WriterActive { txn_id });
            }
            WriterState::RecoveryRequired => {
                return Err(crate::CheckpointError::RecoveryRequired);
            }
            WriterState::Idle => {}
        }
        let count = self.runtime.outstanding.get();
        if count != 0 {
            return Err(crate::CheckpointError::OutstandingTransactions { count });
        }
        Ok(())
    }

    pub(crate) fn ensure_clean_close(&self) -> Result<(), StorageError> {
        match self.runtime.writer.get() {
            WriterState::Idle if self.runtime.outstanding.get() == 0 => Ok(()),
            WriterState::Idle => Err(TransactionError::OutstandingTransactions {
                count: self.runtime.outstanding.get(),
            }
            .into()),
            WriterState::Active(txn_id) => {
                Err(TransactionError::UnfinishedWriter { txn_id }.into())
            }
            WriterState::RecoveryRequired => Err(TransactionError::RecoveryRequired.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::path::PathBuf;
    use std::rc::Rc;

    use super::{TransactionManager, TransactionState};

    use crate::{
        BufferPool, PageManager, StorageError, TransactionError, WalManager, WalRecordKind,
    };

    fn test_paths(name: &str) -> (PathBuf, PathBuf) {
        let base = std::env::temp_dir().join(format!(
            "netbadb-{name}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        (base.with_extension("db"), base.with_extension("wal"))
    }

    fn test_manager(
        name: &str,
    ) -> (
        PathBuf,
        PathBuf,
        Rc<RefCell<WalManager>>,
        TransactionManager,
    ) {
        let (page_path, wal_path) = test_paths(name);
        let wal = Rc::new(RefCell::new(
            WalManager::create(&wal_path).expect("create WAL"),
        ));
        let pages = PageManager::create(&page_path).expect("create page file");
        let buffer = BufferPool::with_wal(pages, 2, Rc::clone(&wal)).expect("buffer pool");
        let next_txn_id = wal.borrow().next_txn_id();
        let manager = TransactionManager::new(Rc::clone(&wal), buffer, next_txn_id)
            .expect("transaction manager");
        (page_path, wal_path, wal, manager)
    }

    fn cleanup(page_path: PathBuf, wal_path: PathBuf) {
        let _ = std::fs::remove_file(page_path);
        let _ = std::fs::remove_file(wal_path);
    }

    #[test]
    fn commit_record_is_durable_before_state_becomes_committed() {
        let (page_path, wal_path, wal, mut manager) = test_manager("txn-commit");
        let mut transaction = manager.begin().expect("begin transaction");
        transaction.commit().expect("commit transaction");

        assert_eq!(transaction.state(), TransactionState::Committed);
        assert_eq!(wal.borrow().durable_lsn(), Some(transaction.last_lsn()));
        let records = wal.borrow_mut().scan().expect("scan WAL");
        assert!(matches!(
            records.last().map(|record| &record.kind),
            Some(WalRecordKind::Commit)
        ));
        drop(transaction);
        drop(manager);
        drop(wal);
        cleanup(page_path, wal_path);
    }

    #[test]
    fn failed_commit_flush_remains_pending_and_retry_does_not_duplicate_commit() {
        let (page_path, wal_path, wal, mut manager) = test_manager("txn-commit-failure");
        let mut transaction = manager.begin().expect("begin transaction");
        wal.borrow_mut().inject_flush_failure();

        assert!(matches!(transaction.commit(), Err(StorageError::Wal(_))));
        assert_eq!(transaction.state(), TransactionState::CommitPending);
        assert_eq!(wal.borrow_mut().scan().expect("scan WAL").len(), 2);

        transaction.commit().expect("retry commit flush");
        assert_eq!(transaction.state(), TransactionState::Committed);
        assert_eq!(wal.borrow_mut().scan().expect("scan WAL").len(), 2);
        drop(transaction);
        drop(manager);
        drop(wal);
        cleanup(page_path, wal_path);
    }

    #[test]
    fn rollback_is_durable_and_completed_without_page_updates() {
        let (page_path, wal_path, wal, mut manager) = test_manager("txn-rollback");
        let mut transaction = manager.begin().expect("begin transaction");
        transaction.rollback().expect("rollback transaction");

        assert_eq!(transaction.state(), TransactionState::RolledBack);
        let records = wal.borrow_mut().scan().expect("scan WAL");
        assert!(matches!(records[0].kind, WalRecordKind::Begin));
        assert!(matches!(records[1].kind, WalRecordKind::Abort));
        assert!(matches!(records[2].kind, WalRecordKind::RollbackComplete));
        assert_eq!(wal.borrow().durable_lsn(), Some(transaction.last_lsn()));
        assert!(transaction.rollback().is_err());
        drop(transaction);
        drop(manager);
        drop(wal);
        cleanup(page_path, wal_path);
    }

    #[test]
    fn rollback_required_rejects_commit_and_writes_but_allows_rollback() {
        let (page_path, wal_path, wal, mut manager) = test_manager("txn-rollback-required");
        let mut transaction = manager.begin().expect("begin transaction");
        transaction.acquire_writer().expect("acquire writer");
        transaction.require_rollback();

        assert_eq!(transaction.state(), TransactionState::RollbackRequired);
        assert!(matches!(
            transaction.commit(),
            Err(StorageError::Transaction(TransactionError::NotActive {
                state: TransactionState::RollbackRequired,
                ..
            }))
        ));
        assert!(matches!(
            transaction.acquire_writer(),
            Err(StorageError::Transaction(TransactionError::NotActive {
                state: TransactionState::RollbackRequired,
                ..
            }))
        ));
        transaction
            .rollback()
            .expect("rollback required transaction");
        assert_eq!(transaction.state(), TransactionState::RolledBack);

        drop(transaction);
        drop(manager);
        drop(wal);
        cleanup(page_path, wal_path);
    }

    #[test]
    fn dropping_rollback_required_writer_requires_recovery() {
        let (page_path, wal_path, wal, mut manager) = test_manager("txn-drop-rollback-required");
        let mut transaction = manager.begin().expect("begin transaction");
        transaction.acquire_writer().expect("acquire writer");
        transaction.require_rollback();
        drop(transaction);

        let next = manager.begin().expect("begin read-only transaction");
        assert!(matches!(
            next.acquire_writer(),
            Err(StorageError::Transaction(
                TransactionError::RecoveryRequired
            ))
        ));
        drop(next);
        drop(manager);
        drop(wal);
        cleanup(page_path, wal_path);
    }

    #[test]
    fn transactions_acquire_the_single_writer_lazily() {
        let (page_path, wal_path, wal, mut manager) = test_manager("txn-single-writer");
        let mut first = manager.begin().expect("begin first transaction");
        let second = manager.begin().expect("begin read-only transaction");

        first.acquire_writer().expect("acquire first writer");

        assert!(matches!(
            second.acquire_writer(),
            Err(StorageError::Transaction(TransactionError::WriterBusy {
                txn_id
            })) if txn_id == first.id()
        ));
        first.commit().expect("commit first transaction");
        second.acquire_writer().expect("acquire released writer");
        drop(second);
        drop(first);
        drop(manager);
        drop(wal);
        cleanup(page_path, wal_path);
    }

    #[test]
    fn dropping_an_active_writer_without_updates_releases_it() {
        let (page_path, wal_path, wal, mut manager) = test_manager("txn-clean-drop");
        let transaction = manager.begin().expect("begin transaction");
        transaction.acquire_writer().expect("acquire writer");
        drop(transaction);

        let next = manager.begin().expect("begin next transaction");
        next.acquire_writer().expect("acquire released writer");
        drop(next);
        drop(manager);
        drop(wal);
        cleanup(page_path, wal_path);
    }

    #[test]
    fn dropped_commit_pending_writer_requires_recovery() {
        let (page_path, wal_path, wal, mut manager) = test_manager("txn-dropped-commit-pending");
        let mut transaction = manager.begin().expect("begin transaction");
        transaction.acquire_writer().expect("acquire writer");
        wal.borrow_mut().inject_flush_failure();
        assert!(matches!(transaction.commit(), Err(StorageError::Wal(_))));
        assert_eq!(transaction.state(), TransactionState::CommitPending);
        drop(transaction);

        let next = manager.begin().expect("read-only begin remains available");
        assert!(matches!(
            next.acquire_writer(),
            Err(StorageError::Transaction(
                TransactionError::RecoveryRequired
            ))
        ));
        assert!(matches!(
            manager.ensure_clean_close(),
            Err(StorageError::Transaction(
                TransactionError::RecoveryRequired
            ))
        ));
        drop(next);
        drop(manager);
        drop(wal);
        cleanup(page_path, wal_path);
    }

    #[test]
    fn terminal_transactions_reject_commit_rollback_and_write_acquisition() {
        let (page_path, wal_path, wal, mut manager) = test_manager("txn-terminal-states");
        let mut committed = manager.begin().expect("begin committed transaction");
        committed.commit().expect("commit transaction");
        assert!(matches!(
            committed.rollback(),
            Err(StorageError::Transaction(TransactionError::NotActive {
                state: TransactionState::Committed,
                ..
            }))
        ));
        assert!(matches!(
            committed.acquire_writer(),
            Err(StorageError::Transaction(TransactionError::NotActive {
                state: TransactionState::Committed,
                ..
            }))
        ));

        let mut rolled_back = manager.begin().expect("begin rolled-back transaction");
        rolled_back.rollback().expect("rollback transaction");
        assert!(matches!(
            rolled_back.commit(),
            Err(StorageError::Transaction(TransactionError::NotActive {
                state: TransactionState::RolledBack,
                ..
            }))
        ));
        assert!(matches!(
            rolled_back.acquire_writer(),
            Err(StorageError::Transaction(TransactionError::NotActive {
                state: TransactionState::RolledBack,
                ..
            }))
        ));
        drop(committed);
        drop(rolled_back);
        drop(manager);
        drop(wal);
        cleanup(page_path, wal_path);
    }
}
