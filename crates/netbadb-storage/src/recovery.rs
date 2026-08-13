use std::collections::{BinaryHeap, HashMap};
use std::error::Error;
use std::fmt;

use netbadb_types::{Lsn, PageId, TxnId};

use crate::page::{ValidatedBeforeImage, validate_before_image};
use crate::{Page, PageManager, StorageError, WalError, WalManager, WalRecord, WalRecordKind};

/// Summary of the synchronous recovery work performed while opening storage.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct RecoveryReport {
    pub records_scanned: usize,
    pub committed_transactions: usize,
    pub undone_transactions: usize,
    pub pages_redone: usize,
    pub pages_undone: usize,
    pub truncated_wal_tail: bool,
}

/// Errors raised while reconstructing database pages from the WAL.
#[derive(Debug)]
pub enum RecoveryError {
    Wal(WalError),
    Storage(Box<StorageError>),
    MetadataPageUpdate {
        lsn: Lsn,
    },
    PageGap {
        page_id: PageId,
        page_count: u64,
    },
    NonTrailingPageRemoval {
        page_id: PageId,
        page_count: u64,
    },
    CommittedUpdateDependsOnLoser {
        page_id: PageId,
        loser_txn: TxnId,
        loser_lsn: Lsn,
        winner_txn: TxnId,
        winner_lsn: Lsn,
    },
    MissingRecord {
        txn_id: TxnId,
        lsn: Lsn,
    },
    WrongTransaction {
        expected: TxnId,
        actual: TxnId,
        lsn: Lsn,
    },
    #[cfg(test)]
    InterruptedForTest,
}

impl fmt::Display for RecoveryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Wal(error) => write!(formatter, "recovery WAL error: {error}"),
            Self::Storage(error) => write!(formatter, "recovery page error: {error}"),
            Self::MetadataPageUpdate { lsn } => write!(
                formatter,
                "WAL record at {} attempts to update metadata page 0",
                lsn.0
            ),
            Self::PageGap {
                page_id,
                page_count,
            } => write!(
                formatter,
                "WAL references page {} beyond the {}-page file",
                page_id.0, page_count
            ),
            Self::NonTrailingPageRemoval {
                page_id,
                page_count,
            } => write!(
                formatter,
                "loser-created page {} is not trailing in the {}-page file",
                page_id.0, page_count
            ),
            Self::CommittedUpdateDependsOnLoser {
                page_id,
                loser_txn,
                loser_lsn,
                winner_txn,
                winner_lsn,
            } => write!(
                formatter,
                "committed transaction {} update at {} depends on loser transaction {} update at {} on page {}",
                winner_txn.0, winner_lsn.0, loser_txn.0, loser_lsn.0, page_id.0
            ),
            Self::MissingRecord { txn_id, lsn } => write!(
                formatter,
                "transaction {} references missing WAL record {} during undo",
                txn_id.0, lsn.0
            ),
            Self::WrongTransaction {
                expected,
                actual,
                lsn,
            } => write!(
                formatter,
                "transaction {} undo chain reaches transaction {} record at {}",
                expected.0, actual.0, lsn.0
            ),
            #[cfg(test)]
            Self::InterruptedForTest => formatter.write_str("recovery interrupted for test"),
        }
    }
}

impl Error for RecoveryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Wal(error) => Some(error),
            Self::Storage(error) => Some(error.as_ref()),
            _ => None,
        }
    }
}

impl From<WalError> for RecoveryError {
    fn from(error: WalError) -> Self {
        Self::Wal(error)
    }
}

impl From<StorageError> for RecoveryError {
    fn from(error: StorageError) -> Self {
        Self::Storage(Box::new(error))
    }
}

#[derive(Debug, Clone, Copy)]
struct TransactionAnalysis {
    last_lsn: Lsn,
    committed: bool,
    rolled_back: bool,
}

pub(crate) struct RecoveryManager;

impl RecoveryManager {
    pub(crate) fn recover(
        pages: &mut PageManager,
        wal: &mut WalManager,
        records: &[WalRecord],
        truncated_wal_tail: bool,
    ) -> Result<RecoveryReport, RecoveryError> {
        Self::recover_inner(pages, wal, records, truncated_wal_tail, None)
    }

    fn recover_inner(
        pages: &mut PageManager,
        wal: &mut WalManager,
        records: &[WalRecord],
        truncated_wal_tail: bool,
        operation_limit: Option<usize>,
    ) -> Result<RecoveryReport, RecoveryError> {
        let mut transactions = HashMap::<TxnId, TransactionAnalysis>::new();
        let mut record_by_lsn = HashMap::<Lsn, usize>::with_capacity(records.len());
        for (index, record) in records.iter().enumerate() {
            record_by_lsn.insert(record.lsn, index);
            let transaction = transactions
                .entry(record.txn_id)
                .or_insert(TransactionAnalysis {
                    last_lsn: record.lsn,
                    committed: false,
                    rolled_back: false,
                });
            transaction.last_lsn = record.lsn;
            if matches!(record.kind, WalRecordKind::Commit) {
                transaction.committed = true;
            } else if matches!(record.kind, WalRecordKind::RollbackComplete) {
                transaction.rolled_back = true;
            }
        }

        let committed_transactions = transactions
            .values()
            .filter(|transaction| transaction.committed)
            .count();
        let mut losers = transactions
            .iter()
            .filter(|(_, transaction)| !transaction.committed && !transaction.rolled_back)
            .map(|(txn_id, transaction)| (*txn_id, transaction.last_lsn))
            .collect::<Vec<_>>();
        losers.sort_unstable_by_key(|(txn_id, last_lsn)| (last_lsn.0, txn_id.0));
        Self::validate_no_winner_depends_on_loser(records, &transactions)?;
        Self::validate_page_topology(
            records,
            &record_by_lsn,
            &transactions,
            &losers,
            pages.page_count(),
        )?;
        let mut report = RecoveryReport {
            records_scanned: records.len(),
            committed_transactions,
            undone_transactions: losers.len(),
            truncated_wal_tail,
            ..RecoveryReport::default()
        };

        // Recovery may update the data file only after every retained WAL
        // record is durable. A truncated tail was already durably shortened.
        if let Some(last_lsn) = records.last().map(|record| record.lsn) {
            wal.flush_through(last_lsn)?;
        }

        let mut operations = 0_usize;
        for record in records {
            if transactions
                .get(&record.txn_id)
                .is_some_and(|transaction| transaction.rolled_back)
            {
                continue;
            }
            let WalRecordKind::PageUpdate { page_id, after, .. } = &record.kind else {
                continue;
            };
            Self::reject_metadata_page(*page_id, record.lsn)?;
            let page_count = pages.page_count();
            if page_id.0 > page_count {
                return Err(RecoveryError::PageGap {
                    page_id: *page_id,
                    page_count,
                });
            }
            let current_lsn = if page_id.0 == page_count {
                pages.allocate_page()?;
                None
            } else {
                let current = pages.read_page(*page_id)?;
                if current.bytes().iter().all(|byte| *byte == 0) {
                    None
                } else {
                    current.page_lsn()?
                }
            };
            if current_lsn.is_some_and(|page_lsn| page_lsn >= record.lsn) {
                continue;
            }
            let after_page = Page::from_bytes(*page_id, **after);
            pages.write_page(&after_page)?;
            report.pages_redone += 1;
            operations += 1;
            #[cfg(test)]
            crate::crash_test::maybe_crash(
                crate::crash_test::TestCrashPoint::RecoveryAfterPageOperation,
            );
            Self::maybe_interrupt(operations, operation_limit)?;
        }

        // Each heap entry is the current end of one loser transaction's
        // prevLSN chain. Popping the greatest LSN gives a global reverse-LSN
        // undo order while preserving every per-transaction chain.
        let mut undo = BinaryHeap::<(u64, u64)>::new();
        for (txn_id, last_lsn) in &losers {
            undo.push((last_lsn.0, txn_id.0));
        }
        while let Some((raw_lsn, raw_txn_id)) = undo.pop() {
            let lsn = Lsn(raw_lsn);
            let txn_id = TxnId(raw_txn_id);
            let index = record_by_lsn
                .get(&lsn)
                .copied()
                .ok_or(RecoveryError::MissingRecord { txn_id, lsn })?;
            let record = &records[index];
            if record.txn_id != txn_id {
                return Err(RecoveryError::WrongTransaction {
                    expected: txn_id,
                    actual: record.txn_id,
                    lsn,
                });
            }
            if let WalRecordKind::PageUpdate {
                page_id, before, ..
            } = &record.kind
            {
                Self::reject_metadata_page(*page_id, record.lsn)?;
                let page_count = pages.page_count();
                match validate_before_image(*page_id, before)? {
                    ValidatedBeforeImage::NewPage => {
                        if page_id.0 > page_count {
                            return Err(RecoveryError::PageGap {
                                page_id: *page_id,
                                page_count,
                            });
                        }
                        if pages.remove_trailing_page(*page_id)? {
                            report.pages_undone += 1;
                            operations += 1;
                            #[cfg(test)]
                            crate::crash_test::maybe_crash(
                                crate::crash_test::TestCrashPoint::RecoveryAfterPageOperation,
                            );
                            Self::maybe_interrupt(operations, operation_limit)?;
                        }
                    }
                    ValidatedBeforeImage::Existing(before_page) => {
                        if page_id.0 >= page_count {
                            return Err(RecoveryError::PageGap {
                                page_id: *page_id,
                                page_count,
                            });
                        }
                        pages.write_page(&before_page)?;
                        report.pages_undone += 1;
                        operations += 1;
                        #[cfg(test)]
                        crate::crash_test::maybe_crash(
                            crate::crash_test::TestCrashPoint::RecoveryAfterPageOperation,
                        );
                        Self::maybe_interrupt(operations, operation_limit)?;
                    }
                }
            }
            if let Some(prev_lsn) = record.prev_lsn {
                undo.push((prev_lsn.0, txn_id.0));
            }
        }

        pages.sync()?;

        // Once physical undo is durable, record that startup recovery has
        // completed each loser. Otherwise a later committed update could be
        // rejected—or overwritten by the same old loser—on the next restart.
        let mut completion_lsn = None;
        for (txn_id, last_lsn) in &losers {
            let last_record =
                records
                    .get(record_by_lsn.get(last_lsn).copied().ok_or(
                        RecoveryError::MissingRecord {
                            txn_id: *txn_id,
                            lsn: *last_lsn,
                        },
                    )?)
                    .ok_or(RecoveryError::MissingRecord {
                        txn_id: *txn_id,
                        lsn: *last_lsn,
                    })?;
            let abort_lsn = if matches!(last_record.kind, WalRecordKind::Abort) {
                *last_lsn
            } else {
                wal.append(*txn_id, Some(*last_lsn), WalRecordKind::Abort)?
            };
            completion_lsn =
                Some(wal.append(*txn_id, Some(abort_lsn), WalRecordKind::RollbackComplete)?);
        }
        if let Some(lsn) = completion_lsn {
            wal.flush_through(lsn)?;
        }
        Ok(report)
    }

    fn reject_metadata_page(page_id: PageId, lsn: Lsn) -> Result<(), RecoveryError> {
        if page_id.0 == 0 {
            return Err(RecoveryError::MetadataPageUpdate { lsn });
        }
        Ok(())
    }

    fn validate_page_topology(
        records: &[WalRecord],
        record_by_lsn: &HashMap<Lsn, usize>,
        transactions: &HashMap<TxnId, TransactionAnalysis>,
        losers: &[(TxnId, Lsn)],
        initial_page_count: u64,
    ) -> Result<(), RecoveryError> {
        Self::validate_rolled_back_page_topology(records, transactions, initial_page_count)?;
        let mut simulated_page_count = initial_page_count;
        for record in records {
            if transactions
                .get(&record.txn_id)
                .is_some_and(|transaction| transaction.rolled_back)
            {
                continue;
            }
            let WalRecordKind::PageUpdate { page_id, .. } = &record.kind else {
                continue;
            };
            Self::reject_metadata_page(*page_id, record.lsn)?;
            if page_id.0 > simulated_page_count {
                return Err(RecoveryError::PageGap {
                    page_id: *page_id,
                    page_count: simulated_page_count,
                });
            }
            if page_id.0 == simulated_page_count {
                simulated_page_count =
                    simulated_page_count
                        .checked_add(1)
                        .ok_or(RecoveryError::PageGap {
                            page_id: *page_id,
                            page_count: simulated_page_count,
                        })?;
            }
        }

        let mut undo = BinaryHeap::<(u64, u64)>::new();
        for (txn_id, last_lsn) in losers {
            undo.push((last_lsn.0, txn_id.0));
        }
        while let Some((raw_lsn, raw_txn_id)) = undo.pop() {
            let lsn = Lsn(raw_lsn);
            let txn_id = TxnId(raw_txn_id);
            let index = record_by_lsn
                .get(&lsn)
                .copied()
                .ok_or(RecoveryError::MissingRecord { txn_id, lsn })?;
            let record = &records[index];
            if record.txn_id != txn_id {
                return Err(RecoveryError::WrongTransaction {
                    expected: txn_id,
                    actual: record.txn_id,
                    lsn,
                });
            }
            if let WalRecordKind::PageUpdate {
                page_id, before, ..
            } = &record.kind
            {
                if before.iter().all(|byte| *byte == 0) {
                    let expected_count =
                        page_id
                            .0
                            .checked_add(1)
                            .ok_or(RecoveryError::NonTrailingPageRemoval {
                                page_id: *page_id,
                                page_count: simulated_page_count,
                            })?;
                    if expected_count != simulated_page_count {
                        return Err(RecoveryError::NonTrailingPageRemoval {
                            page_id: *page_id,
                            page_count: simulated_page_count,
                        });
                    }
                    simulated_page_count = page_id.0;
                }
            }
            if let Some(prev_lsn) = record.prev_lsn {
                undo.push((prev_lsn.0, txn_id.0));
            }
        }
        Ok(())
    }

    fn validate_rolled_back_page_topology(
        records: &[WalRecord],
        transactions: &HashMap<TxnId, TransactionAnalysis>,
        initial_page_count: u64,
    ) -> Result<(), RecoveryError> {
        let mut allocated_ranges = HashMap::<TxnId, (u64, u64)>::new();
        for record in records {
            if !transactions
                .get(&record.txn_id)
                .is_some_and(|transaction| transaction.rolled_back)
            {
                continue;
            }
            let WalRecordKind::PageUpdate {
                page_id, before, ..
            } = &record.kind
            else {
                continue;
            };
            Self::reject_metadata_page(*page_id, record.lsn)?;
            if before.iter().all(|byte| *byte == 0) {
                let range = allocated_ranges
                    .entry(record.txn_id)
                    .or_insert((page_id.0, page_id.0));
                if range.0 == range.1 && page_id.0 > initial_page_count {
                    return Err(RecoveryError::PageGap {
                        page_id: *page_id,
                        page_count: initial_page_count,
                    });
                }
                if page_id.0 != range.1 {
                    return Err(RecoveryError::PageGap {
                        page_id: *page_id,
                        page_count: range.1,
                    });
                }
                range.1 = range.1.checked_add(1).ok_or(RecoveryError::PageGap {
                    page_id: *page_id,
                    page_count: range.1,
                })?;
            } else if page_id.0 >= initial_page_count
                && !allocated_ranges
                    .get(&record.txn_id)
                    .is_some_and(|(start, end)| page_id.0 >= *start && page_id.0 < *end)
            {
                return Err(RecoveryError::PageGap {
                    page_id: *page_id,
                    page_count: initial_page_count,
                });
            }
        }
        Ok(())
    }

    fn validate_no_winner_depends_on_loser(
        records: &[WalRecord],
        transactions: &HashMap<TxnId, TransactionAnalysis>,
    ) -> Result<(), RecoveryError> {
        let mut latest_loser_by_page = HashMap::<PageId, (TxnId, Lsn)>::new();
        for record in records {
            let WalRecordKind::PageUpdate { page_id, .. } = &record.kind else {
                continue;
            };
            let Some(transaction) = transactions.get(&record.txn_id) else {
                continue;
            };
            let committed = transaction.committed;
            if committed {
                if let Some((loser_txn, loser_lsn)) = latest_loser_by_page.get(page_id).copied() {
                    return Err(RecoveryError::CommittedUpdateDependsOnLoser {
                        page_id: *page_id,
                        loser_txn,
                        loser_lsn,
                        winner_txn: record.txn_id,
                        winner_lsn: record.lsn,
                    });
                }
            } else if !transaction.rolled_back {
                latest_loser_by_page.insert(*page_id, (record.txn_id, record.lsn));
            }
        }
        Ok(())
    }

    fn maybe_interrupt(
        operations: usize,
        operation_limit: Option<usize>,
    ) -> Result<(), RecoveryError> {
        #[cfg(test)]
        if operation_limit.is_some_and(|limit| operations >= limit) {
            return Err(RecoveryError::InterruptedForTest);
        }
        #[cfg(not(test))]
        let _ = (operations, operation_limit);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn recover_with_operation_limit(
        pages: &mut PageManager,
        wal: &mut WalManager,
        records: &[WalRecord],
        operation_limit: usize,
    ) -> Result<RecoveryReport, RecoveryError> {
        Self::recover_inner(pages, wal, records, false, Some(operation_limit))
    }
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;
    use std::io::{Seek, SeekFrom, Write};
    use std::path::{Path, PathBuf};

    use netbadb_types::{Lsn, PageId, TxnId};

    use super::{RecoveryError, RecoveryManager};
    use crate::wal::page_update_kind;
    use crate::{
        PAGE_SIZE, Page, PageManager, PageType, WalError, WalManager, WalRecordKind, wal_path,
    };

    fn test_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("netbadb-recovery-{name}-{}", std::process::id()))
    }

    fn create_fixture(name: &str) -> (PathBuf, Page) {
        let path = test_path(name);
        cleanup(&path);
        let mut pages = PageManager::create(&path).expect("create page file");
        let page = Page::new(PageId(1), PageType::Heap);
        assert_eq!(pages.allocate_page().expect("allocate page").id, PageId(1));
        pages.write_page(&page).expect("initialize page");
        pages.sync().expect("sync page file");
        WalManager::create(wal_path(&path)).expect("create WAL");
        (path, page)
    }

    fn initial_physical_offset(lsn: Lsn) -> u64 {
        crate::WAL_HEADER_SIZE as u64 + lsn.0 - 1
    }

    fn append_update(
        wal: &mut WalManager,
        txn_id: TxnId,
        prev_lsn: netbadb_types::Lsn,
        before: &Page,
        row: &[u8],
    ) -> (netbadb_types::Lsn, Page) {
        let mut after = before.clone();
        after.insert_record(row).expect("insert test row");
        let lsn = wal.next_lsn();
        after.set_page_lsn(lsn);
        let actual = wal
            .append(txn_id, Some(prev_lsn), page_update_kind(before, &after))
            .expect("append page update");
        assert_eq!(actual, lsn);
        (lsn, after)
    }

    fn recover(path: &Path) -> Result<super::RecoveryReport, RecoveryError> {
        let mut pages = PageManager::open(path)?;
        let (mut wal, records, tail) = WalManager::open_for_recovery(wal_path(path))?;
        RecoveryManager::recover(&mut pages, &mut wal, &records, tail)
    }

    fn read_page(path: &Path, id: PageId) -> Page {
        PageManager::open(path)
            .expect("open pages")
            .read_page(id)
            .expect("read page")
    }

    fn write_page(path: &Path, page: &Page) {
        let mut pages = PageManager::open(path).expect("open pages");
        pages.write_page(page).expect("write page");
        pages.sync().expect("sync page");
    }

    fn cleanup(path: &Path) {
        let _ = std::fs::remove_file(path);
        let wal = wal_path(path);
        let _ = std::fs::remove_file(crate::wal_alternate_path(&wal));
        let _ = std::fs::remove_file(wal);
    }

    #[test]
    fn committed_update_is_redone_and_page_lsn_skips_a_flushed_update() {
        let (path, original) = create_fixture("committed-redo");
        let mut wal = WalManager::open(wal_path(&path)).expect("open WAL");
        let begin = wal
            .append(TxnId(1), None, WalRecordKind::Begin)
            .expect("begin");
        let (update, after) = append_update(&mut wal, TxnId(1), begin, &original, b"winner");
        let commit = wal
            .append(TxnId(1), Some(update), WalRecordKind::Commit)
            .expect("commit");
        wal.flush_through(commit).expect("flush commit");
        drop(wal);

        let report = recover(&path).expect("recover unflushed update");
        assert_eq!(report.pages_redone, 1);
        assert_eq!(read_page(&path, PageId(1)), after);

        let report = recover(&path).expect("recover flushed update");
        assert_eq!(report.pages_redone, 0);
        assert_eq!(read_page(&path, PageId(1)), after);
        cleanup(&path);
    }

    #[test]
    fn active_update_is_undone_whether_or_not_its_page_was_flushed() {
        for (name, flush_page) in [("active-unflushed", false), ("active-flushed", true)] {
            let (path, original) = create_fixture(name);
            let mut wal = WalManager::open(wal_path(&path)).expect("open WAL");
            let begin = wal
                .append(TxnId(2), None, WalRecordKind::Begin)
                .expect("begin");
            let (update, after) = append_update(&mut wal, TxnId(2), begin, &original, b"loser");
            wal.flush_through(update).expect("flush update");
            drop(wal);
            if flush_page {
                write_page(&path, &after);
            }

            let report = recover(&path).expect("recover active transaction");
            assert_eq!(report.undone_transactions, 1);
            assert_eq!(report.pages_undone, 1);
            assert_eq!(read_page(&path, PageId(1)), original);
            cleanup(&path);
        }
    }

    #[test]
    fn explicitly_aborted_flushed_update_is_a_loser() {
        let (path, original) = create_fixture("aborted-flushed");
        let mut wal = WalManager::open(wal_path(&path)).expect("open WAL");
        let begin = wal
            .append(TxnId(3), None, WalRecordKind::Begin)
            .expect("begin");
        let (update, after) = append_update(&mut wal, TxnId(3), begin, &original, b"aborted");
        let abort = wal
            .append(TxnId(3), Some(update), WalRecordKind::Abort)
            .expect("abort");
        wal.flush_through(abort).expect("flush abort");
        drop(wal);
        write_page(&path, &after);

        let report = recover(&path).expect("recover aborted transaction");
        assert_eq!(report.committed_transactions, 0);
        assert_eq!(report.undone_transactions, 1);
        assert_eq!(read_page(&path, PageId(1)), original);
        cleanup(&path);
    }

    #[test]
    fn interleaved_winner_and_loser_restore_the_winner_image() {
        let (path, original) = create_fixture("winner-loser");
        let mut wal = WalManager::open(wal_path(&path)).expect("open WAL");
        let winner_begin = wal
            .append(TxnId(10), None, WalRecordKind::Begin)
            .expect("winner begin");
        let (winner_update, winner_page) =
            append_update(&mut wal, TxnId(10), winner_begin, &original, b"winner");
        let loser_begin = wal
            .append(TxnId(11), None, WalRecordKind::Begin)
            .expect("loser begin");
        let (loser_update, loser_page) =
            append_update(&mut wal, TxnId(11), loser_begin, &winner_page, b"loser-1");
        let (loser_last, loser_last_page) =
            append_update(&mut wal, TxnId(11), loser_update, &loser_page, b"loser-2");
        let commit = wal
            .append(TxnId(10), Some(winner_update), WalRecordKind::Commit)
            .expect("winner commit");
        wal.flush_through(commit.max(loser_last))
            .expect("flush WAL");
        drop(wal);
        write_page(&path, &loser_last_page);

        let report = recover(&path).expect("recover interleaving");
        assert_eq!(report.committed_transactions, 1);
        assert_eq!(report.undone_transactions, 1);
        assert_eq!(read_page(&path, PageId(1)), winner_page);
        cleanup(&path);
    }

    #[test]
    fn recovery_rejects_a_winner_that_observed_an_earlier_loser_update() {
        let (path, original) = create_fixture("loser-before-winner");
        let mut wal = WalManager::open(wal_path(&path)).expect("open WAL");
        let loser_begin = wal
            .append(TxnId(12), None, WalRecordKind::Begin)
            .expect("loser begin");
        let (loser_update, loser_page) =
            append_update(&mut wal, TxnId(12), loser_begin, &original, b"loser");
        let winner_begin = wal
            .append(TxnId(13), None, WalRecordKind::Begin)
            .expect("winner begin");
        let (winner_update, _) =
            append_update(&mut wal, TxnId(13), winner_begin, &loser_page, b"winner");
        let commit = wal
            .append(TxnId(13), Some(winner_update), WalRecordKind::Commit)
            .expect("winner commit");
        wal.flush_through(commit.max(loser_update))
            .expect("flush WAL");
        drop(wal);

        assert!(matches!(
            recover(&path),
            Err(RecoveryError::CommittedUpdateDependsOnLoser {
                page_id: PageId(1),
                loser_txn: TxnId(12),
                winner_txn: TxnId(13),
                ..
            })
        ));
        assert_eq!(read_page(&path, PageId(1)), original);
        cleanup(&path);
    }

    #[test]
    fn multiple_losers_are_undone_in_global_reverse_lsn_order() {
        let (path, original) = create_fixture("global-undo");
        let mut wal = WalManager::open(wal_path(&path)).expect("open WAL");
        let t1_begin = wal
            .append(TxnId(21), None, WalRecordKind::Begin)
            .expect("t1 begin");
        let t2_begin = wal
            .append(TxnId(22), None, WalRecordKind::Begin)
            .expect("t2 begin");
        let (t1_first, page1) = append_update(&mut wal, TxnId(21), t1_begin, &original, b"t1-a");
        let (t2_first, page2) = append_update(&mut wal, TxnId(22), t2_begin, &page1, b"t2-a");
        let (_t1_last, page3) = append_update(&mut wal, TxnId(21), t1_first, &page2, b"t1-b");
        let (last, page4) = append_update(&mut wal, TxnId(22), t2_first, &page3, b"t2-b");
        wal.flush_through(last).expect("flush WAL");
        drop(wal);
        write_page(&path, &page4);

        let report = recover(&path).expect("recover losers");
        assert_eq!(report.undone_transactions, 2);
        assert_eq!(report.pages_undone, 4);
        assert_eq!(read_page(&path, PageId(1)), original);
        cleanup(&path);
    }

    #[test]
    fn multi_page_multi_transaction_recovery_preserves_only_the_winner() {
        let (path, page1) = create_fixture("multi-page-multi-transaction");
        let mut pages = PageManager::open(&path).expect("open pages");
        let page2 = Page::new(PageId(2), PageType::Heap);
        assert_eq!(
            pages.allocate_page().expect("allocate page 2").id,
            PageId(2)
        );
        pages.write_page(&page2).expect("initialize page 2");
        pages.sync().expect("sync pages");
        drop(pages);

        let mut wal = WalManager::open(wal_path(&path)).expect("open WAL");
        let winner_begin = wal
            .append(TxnId(23), None, WalRecordKind::Begin)
            .expect("winner begin");
        let loser_begin = wal
            .append(TxnId(24), None, WalRecordKind::Begin)
            .expect("loser begin");
        let (winner_update, winner_page) =
            append_update(&mut wal, TxnId(23), winner_begin, &page1, b"winner");
        let (loser_update, loser_page) =
            append_update(&mut wal, TxnId(24), loser_begin, &page2, b"loser");
        let commit = wal
            .append(TxnId(23), Some(winner_update), WalRecordKind::Commit)
            .expect("winner commit");
        wal.flush_through(commit.max(loser_update))
            .expect("flush WAL");
        drop(wal);
        write_page(&path, &loser_page);

        let report = recover(&path).expect("recover pages");
        assert_eq!(report.committed_transactions, 1);
        assert_eq!(report.undone_transactions, 1);
        assert_eq!(read_page(&path, PageId(1)), winner_page);
        assert_eq!(read_page(&path, PageId(2)), page2);
        cleanup(&path);
    }

    #[test]
    fn committed_and_loser_created_pages_are_recovered_without_page_gaps() {
        for (name, commit_page, expected_count) in
            [("new-page-winner", true, 3), ("new-page-loser", false, 2)]
        {
            let (path, _original) = create_fixture(name);
            let mut wal = WalManager::open(wal_path(&path)).expect("open WAL");
            let txn_id = TxnId(30);
            let begin = wal
                .append(txn_id, None, WalRecordKind::Begin)
                .expect("begin");
            let before = Page::zero(PageId(2));
            let mut after = Page::new(PageId(2), PageType::Heap);
            after.insert_record(b"new page").expect("insert row");
            let update = wal.next_lsn();
            after.set_page_lsn(update);
            wal.append(txn_id, Some(begin), page_update_kind(&before, &after))
                .expect("append allocation update");
            let last = if commit_page {
                wal.append(txn_id, Some(update), WalRecordKind::Commit)
                    .expect("commit")
            } else {
                update
            };
            wal.flush_through(last).expect("flush WAL");
            drop(wal);

            recover(&path).expect("recover allocated page");
            let mut pages = PageManager::open(&path).expect("open pages");
            assert_eq!(pages.page_count(), expected_count);
            if commit_page {
                assert_eq!(pages.read_page(PageId(2)).expect("read new page"), after);
            }
            cleanup(&path);
        }
    }

    #[test]
    fn incomplete_final_record_is_truncated_but_middle_corruption_is_rejected() {
        let (path, original) = create_fixture("tail");
        let wal_file = wal_path(&path);
        let mut wal = WalManager::open(&wal_file).expect("open WAL");
        let begin = wal
            .append(TxnId(40), None, WalRecordKind::Begin)
            .expect("begin");
        let (update, after) = append_update(&mut wal, TxnId(40), begin, &original, b"tail");
        let commit = wal
            .append(TxnId(40), Some(update), WalRecordKind::Commit)
            .expect("commit");
        wal.flush_through(commit).expect("flush WAL");
        drop(wal);
        write_page(&path, &after);
        OpenOptions::new()
            .write(true)
            .open(&wal_file)
            .expect("open WAL file")
            .set_len(initial_physical_offset(commit) + 36)
            .expect("truncate commit");

        let report = recover(&path).expect("recover partial tail");
        assert!(report.truncated_wal_tail);
        assert_eq!(read_page(&path, PageId(1)), original);
        let mut finalized = WalManager::open(&wal_file).expect("open finalized WAL");
        let finalized_records = finalized.scan().expect("scan finalized WAL");
        assert!(matches!(
            finalized_records[finalized_records.len() - 2].kind,
            WalRecordKind::Abort
        ));
        assert!(matches!(
            finalized_records.last().map(|record| &record.kind),
            Some(WalRecordKind::RollbackComplete)
        ));
        drop(finalized);

        let mut file = OpenOptions::new()
            .write(true)
            .open(&wal_file)
            .expect("open WAL for corruption");
        file.seek(SeekFrom::Start(initial_physical_offset(update)))
            .expect("seek update");
        file.write_all(b"FAIL").expect("corrupt update magic");
        drop(file);
        assert!(matches!(
            recover(&path),
            Err(RecoveryError::Wal(WalError::InvalidRecordMagic { .. }))
        ));
        cleanup(&path);
    }

    #[test]
    fn malformed_tail_metadata_and_page_images_are_not_treated_as_crash_tails() {
        let (path, original) = create_fixture("corruption");
        let wal_file = wal_path(&path);
        let mut wal = WalManager::open(&wal_file).expect("open WAL");
        let begin = wal
            .append(TxnId(50), None, WalRecordKind::Begin)
            .expect("begin");
        let (update, _) = append_update(&mut wal, TxnId(50), begin, &original, b"bad image");
        wal.flush_through(update).expect("flush update");
        drop(wal);
        let after_version = initial_physical_offset(update) + 40 + 8 + PAGE_SIZE as u64 + 4;
        let mut file = OpenOptions::new()
            .write(true)
            .open(&wal_file)
            .expect("open WAL file");
        file.seek(SeekFrom::Start(after_version))
            .expect("seek image version");
        file.write_all(&99_u16.to_le_bytes())
            .expect("corrupt image version");
        drop(file);
        assert!(matches!(
            recover(&path),
            Err(RecoveryError::Wal(WalError::InvalidPageImage {
                image: "after",
                ..
            }))
        ));

        cleanup(&path);
        let (path, _) = create_fixture("bad-tail-version");
        let wal_file = wal_path(&path);
        let mut file = OpenOptions::new()
            .append(true)
            .open(&wal_file)
            .expect("append WAL tail");
        file.write_all(b"WREC").expect("write magic");
        file.write_all(&99_u16.to_le_bytes())
            .expect("write bad version");
        drop(file);
        assert!(matches!(
            recover(&path),
            Err(RecoveryError::Wal(WalError::UnsupportedRecordVersion {
                version: 99,
                ..
            }))
        ));
        cleanup(&path);
    }

    #[test]
    fn recovery_rejects_page_gaps_and_propagates_write_failures() {
        let (path, _original) = create_fixture("page-gap");
        let mut wal = WalManager::open(wal_path(&path)).expect("open WAL");
        let begin = wal
            .append(TxnId(55), None, WalRecordKind::Begin)
            .expect("begin");
        let before = Page::zero(PageId(3));
        let mut after = Page::new(PageId(3), PageType::Heap);
        after.insert_record(b"gap").expect("insert row");
        let update = wal.next_lsn();
        after.set_page_lsn(update);
        wal.append(TxnId(55), Some(begin), page_update_kind(&before, &after))
            .expect("append gap update");
        wal.flush_through(update).expect("flush WAL");
        drop(wal);
        assert!(matches!(
            recover(&path),
            Err(RecoveryError::PageGap {
                page_id: PageId(3),
                page_count: 2
            })
        ));
        cleanup(&path);

        let (path, original) = create_fixture("recovery-write-failure");
        let mut wal = WalManager::open(wal_path(&path)).expect("open WAL");
        let begin = wal
            .append(TxnId(56), None, WalRecordKind::Begin)
            .expect("begin");
        let (update, _) = append_update(&mut wal, TxnId(56), begin, &original, b"winner");
        let commit = wal
            .append(TxnId(56), Some(update), WalRecordKind::Commit)
            .expect("commit");
        wal.flush_through(commit).expect("flush WAL");
        drop(wal);
        let mut pages = PageManager::open(&path).expect("open pages");
        pages.inject_write_failure();
        let (mut wal, records, _) =
            WalManager::open_for_recovery(wal_path(&path)).expect("open recovery WAL");
        assert!(matches!(
            RecoveryManager::recover(&mut pages, &mut wal, &records, false),
            Err(RecoveryError::Storage(_))
        ));
        assert_eq!(read_page(&path, PageId(1)), original);
        cleanup(&path);
    }

    #[test]
    fn rollback_complete_does_not_bypass_page_reference_validation() {
        let (path, _original) = create_fixture("completed-metadata-update");
        let mut wal = WalManager::open(wal_path(&path)).expect("open WAL");
        let txn_id = TxnId(57);
        let begin = wal
            .append(txn_id, None, WalRecordKind::Begin)
            .expect("begin");
        let before = Page::zero(PageId(0));
        let mut after = Page::new(PageId(0), PageType::Heap);
        let update = wal.next_lsn();
        after.set_page_lsn(update);
        wal.append(txn_id, Some(begin), page_update_kind(&before, &after))
            .expect("append metadata update");
        let abort = wal
            .append(txn_id, Some(update), WalRecordKind::Abort)
            .expect("abort");
        let complete = wal
            .append(txn_id, Some(abort), WalRecordKind::RollbackComplete)
            .expect("complete rollback");
        wal.flush_through(complete).expect("flush WAL");
        drop(wal);
        assert!(matches!(
            recover(&path),
            Err(RecoveryError::MetadataPageUpdate { .. })
        ));
        cleanup(&path);

        let (path, _original) = create_fixture("completed-page-gap");
        let mut wal = WalManager::open(wal_path(&path)).expect("open WAL");
        let txn_id = TxnId(58);
        let begin = wal
            .append(txn_id, None, WalRecordKind::Begin)
            .expect("begin");
        let before = Page::zero(PageId(3));
        let mut after = Page::new(PageId(3), PageType::Heap);
        let update = wal.next_lsn();
        after.set_page_lsn(update);
        wal.append(txn_id, Some(begin), page_update_kind(&before, &after))
            .expect("append gap update");
        let abort = wal
            .append(txn_id, Some(update), WalRecordKind::Abort)
            .expect("abort");
        let complete = wal
            .append(txn_id, Some(abort), WalRecordKind::RollbackComplete)
            .expect("complete rollback");
        wal.flush_through(complete).expect("flush WAL");
        drop(wal);
        assert!(matches!(
            recover(&path),
            Err(RecoveryError::PageGap {
                page_id: PageId(3),
                page_count: 2,
            })
        ));
        cleanup(&path);
    }

    #[test]
    fn recovery_is_idempotent_after_interruption_during_redo_or_undo() {
        for (name, flush_latest) in [("redo-interruption", false), ("undo-interruption", true)] {
            let (path, original) = create_fixture(name);
            let mut wal = WalManager::open(wal_path(&path)).expect("open WAL");
            let begin = wal
                .append(TxnId(60), None, WalRecordKind::Begin)
                .expect("begin");
            let (first, page1) = append_update(&mut wal, TxnId(60), begin, &original, b"one");
            let (last, page2) = append_update(&mut wal, TxnId(60), first, &page1, b"two");
            wal.flush_through(last).expect("flush WAL");
            drop(wal);
            if flush_latest {
                write_page(&path, &page2);
            }

            let mut pages = PageManager::open(&path).expect("open pages");
            let (mut wal, records, _) =
                WalManager::open_for_recovery(wal_path(&path)).expect("open recovery WAL");
            assert!(matches!(
                RecoveryManager::recover_with_operation_limit(&mut pages, &mut wal, &records, 1),
                Err(RecoveryError::InterruptedForTest)
            ));
            drop(pages);
            drop(wal);

            recover(&path).expect("restart recovery");
            let first_result = *read_page(&path, PageId(1)).bytes();
            recover(&path).expect("repeat recovery");
            assert_eq!(read_page(&path, PageId(1)).bytes(), &first_result);
            assert_eq!(read_page(&path, PageId(1)), original);
            cleanup(&path);
        }
    }

    #[test]
    fn empty_clean_and_begin_only_wals_reopen_deterministically() {
        let (path, original) = create_fixture("empty-clean");
        let empty = recover(&path).expect("recover empty WAL");
        assert_eq!(empty.records_scanned, 0);
        let mut wal = WalManager::open(wal_path(&path)).expect("open WAL");
        let begin = wal
            .append(TxnId(70), None, WalRecordKind::Begin)
            .expect("begin");
        wal.flush_through(begin).expect("flush begin");
        drop(wal);
        let begin_only = recover(&path).expect("recover begin only");
        assert_eq!(begin_only.undone_transactions, 1);
        assert_eq!(begin_only.pages_undone, 0);
        assert_eq!(read_page(&path, PageId(1)), original);
        recover(&path).expect("clean reopen");
        cleanup(&path);
    }
}
