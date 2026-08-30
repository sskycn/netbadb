fn tail_fixture(
    name: &str,
) -> (
    std::path::PathBuf,
    HeapStorage,
    netbadb_index::IndexDefinition,
) {
    let path = test_path(name);
    cleanup(&path);
    let mut storage = HeapStorage::create(&path, indexed_table()).unwrap();
    storage
        .insert(&[
            ScalarValue::UInt64(1),
            ScalarValue::UInt64(7),
            ScalarValue::Text("kept".into()),
        ])
        .unwrap();
    let index = storage.create_index(ColumnId(2)).unwrap();
    storage.drop_index(index.id).unwrap();
    (path, storage, index)
}

#[test]
fn tail_reclaim_reappend_uses_same_slot_with_fresh_generation_and_lsn() {
    for capacity in [1, 8] {
        let (path, storage, old) = tail_fixture(&format!("round11-reappend-{capacity}"));
        storage.close().unwrap();
        let mut storage =
            HeapStorage::open_with_buffer_pool_size(&path, indexed_table(), capacity).unwrap();
        let old_lsn = storage
            .buffer
            .read_btree_page(old.handle.meta_page)
            .unwrap()
            .page()
            .page_lsn()
            .unwrap()
            .unwrap();
        let report = storage.reclaim_retired_index_tail().unwrap();
        assert_eq!(
            (
                report.page_count_before,
                report.page_count_after,
                report.reclaimed_pages,
                report.reclaimed_indexes
            ),
            (5, 3, 2, 1)
        );
        assert_eq!(report.pending_indexes_remaining, 0);
        assert!(
            storage
                .buffer
                .read_btree_page(old.handle.meta_page)
                .is_err()
        );
        assert!(storage.btree().height(old.handle).is_err());
        let generation = storage.transactions.wal().borrow().generation();
        let bytes = std::fs::read(&path).unwrap();
        let wal = std::fs::read(storage.transactions.wal().borrow().path()).unwrap();
        for _ in 0..3 {
            assert_eq!(
                storage
                    .reclaim_retired_index_tail()
                    .unwrap()
                    .reclaimed_pages,
                0
            );
        }
        assert_eq!(storage.transactions.wal().borrow().generation(), generation);
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
        assert_eq!(
            std::fs::read(storage.transactions.wal().borrow().path()).unwrap(),
            wal
        );
        let new = storage.create_index(ColumnId(2)).unwrap();
        assert_eq!(
            old.handle.meta_page.page_id(),
            new.handle.meta_page.page_id()
        );
        assert!(new.handle.meta_page.generation() > old.handle.meta_page.generation());
        assert!(new.id > old.id);
        let new_lsn = storage
            .buffer
            .read_btree_page(new.handle.meta_page)
            .unwrap()
            .page()
            .page_lsn()
            .unwrap()
            .unwrap();
        assert!(new_lsn > old_lsn);
        assert!(matches!(
            storage.btree().height(old.handle),
            Err(StorageError::Index(IndexError::GenerationMismatch { .. }))
        ));
        assert_eq!(
            storage
                .btree()
                .lookup(new.handle, &ScalarValue::UInt64(7))
                .unwrap()
                .len(),
            1
        );
        eprintln!(
            "ROUND11_REUSE capacity={capacity} old={:?} new={:?} old_lsn={old_lsn:?} new_lsn={new_lsn:?} stale=GenerationMismatch",
            old.handle.meta_page, new.handle.meta_page
        );
        storage.checkpoint().unwrap();
        storage.close().unwrap();
        for _ in 0..3 {
            let mut storage = HeapStorage::open(&path, indexed_table()).unwrap();
            assert_eq!(storage.indexes(), std::slice::from_ref(&new));
            assert_eq!(storage.scan().unwrap().len(), 1);
            assert_eq!(
                storage
                    .inspect_index_reclaim()
                    .unwrap()
                    .pending_reclaim_indexes,
                0
            );
            storage.checkpoint().unwrap();
            storage.close().unwrap();
        }
        cleanup(&path);
    }
}

#[test]
fn tail_multiple_complete_trees_reclaim_together_and_middle_owner_remains() {
    let (path, mut storage, middle) = tail_fixture("round11-multiple");
    storage
        .btree()
        .create(IndexSpec {
            data_type: netbadb_types::SemanticType::physical(PhysicalType::UInt64),
            nullable: false,
        })
        .unwrap();
    let a = storage.create_index(ColumnId(2)).unwrap();
    storage.drop_index(a.id).unwrap();
    let b = storage.create_index(ColumnId(3)).unwrap();
    storage.drop_index(b.id).unwrap();
    storage.compact_index_catalog().unwrap();
    let report = storage.reclaim_retired_index_tail().unwrap();
    assert_eq!(report.reclaimed_pages, 4);
    assert_eq!(report.reclaimed_indexes, 2);
    assert_eq!(report.pending_indexes_remaining, 1);
    let inventory = storage.inspect_index_reclaim().unwrap();
    assert_eq!(
        inventory.pending,
        vec![netbadb_index::RetiredIndexOwnership {
            index_id: middle.id,
            meta_page: middle.handle.meta_page
        }]
    );
    assert_eq!(inventory.retired_owned_pages, 2);
    storage.compact_index_catalog().unwrap();
    storage.close().unwrap();
    let mut storage = HeapStorage::open(&path, indexed_table()).unwrap();
    assert_eq!(
        storage
            .inspect_index_reclaim()
            .unwrap()
            .pending_reclaim_indexes,
        1
    );
    storage.close().unwrap();
    cleanup(&path);
}

#[test]
fn tail_blockers_and_no_candidate_have_no_durable_side_effects() {
    for kind in [PageType::Heap, PageType::IndexCatalog, PageType::BTreeMeta] {
        let (path, mut storage, _) = tail_fixture(&format!("round11-block-{kind:?}"));
        match kind {
            PageType::BTreeMeta => {
                storage.create_index(ColumnId(3)).unwrap();
            }
            _ => {
                let mut page = storage.buffer.new_page().unwrap();
                *page.page_mut() = crate::Page::new(page.page_id(), kind);
                if kind == PageType::IndexCatalog {
                    page.page_mut()
                        .initialize_single_payload(
                            kind,
                            &netbadb_index::encode_index_catalog(
                                &netbadb_index::IndexCatalogNode::empty(),
                            )
                            .unwrap(),
                        )
                        .unwrap();
                }
            }
        }
        storage.checkpoint().unwrap();
        let bytes = std::fs::read(&path).unwrap();
        let before = storage.transactions.wal().borrow().next_lsn();
        let report = storage.reclaim_retired_index_tail().unwrap();
        assert_eq!(report.reclaimed_pages, 0);
        assert_eq!(report.pending_indexes_remaining, 1);
        assert_eq!(storage.transactions.wal().borrow().next_lsn(), before);
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
        storage.close().unwrap();
        cleanup(&path);
    }
}

#[test]
fn tail_pinned_and_retained_handles_block_before_checkpoint() {
    let (path, mut storage, old) = tail_fixture("round11-admission");
    storage.checkpoint().unwrap();
    let bytes = std::fs::read(&path).unwrap();
    let pin = storage
        .buffer
        .read_btree_page(old.handle.meta_page)
        .unwrap();
    assert!(matches!(
        storage.reclaim_retired_index_tail(),
        Err(StorageError::Buffer(BufferError::PagePinned { .. }))
    ));
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
    drop(pin);
    let mut transaction = storage.begin_transaction().unwrap();
    assert!(storage.reclaim_retired_index_tail().is_err());
    transaction.commit().unwrap();
    // Storage terminal handles unregister; Core separately gates retained
    // database handles, including terminal/lazy handles.
    drop(transaction);
    assert_eq!(
        storage
            .reclaim_retired_index_tail()
            .unwrap()
            .reclaimed_pages,
        2
    );
    storage.close().unwrap();
    cleanup(&path);
}

#[test]
fn tail_capacity_rejection_preserves_file_wal_and_pending() {
    let (path, mut storage, _) = tail_fixture("round11-intent-capacity");
    storage.checkpoint().unwrap();
    storage.index_catalog_payload_capacity = Some(104);
    let before = std::fs::read(&path).unwrap();
    let lsn = storage.transactions.wal().borrow().next_lsn();
    assert!(matches!(
        storage.reclaim_retired_index_tail(),
        Err(StorageError::ResourceLimit { .. })
    ));
    assert_eq!(std::fs::read(&path).unwrap(), before);
    assert_eq!(storage.transactions.wal().borrow().next_lsn(), lsn);
    assert_eq!(
        storage
            .inspect_index_reclaim()
            .unwrap()
            .pending_reclaim_indexes,
        1
    );
    storage.index_catalog_payload_capacity = None;
    assert_eq!(
        storage
            .reclaim_retired_index_tail()
            .unwrap()
            .reclaimed_pages,
        2
    );
    storage.close().unwrap();
    cleanup(&path);
}

pub(super) fn tail_crash_child(case: &str, path: &std::path::Path) {
    let mut storage = HeapStorage::open(path, indexed_table()).unwrap();
    if case == "tail-reclaim" {
        storage.reclaim_retired_index_tail().unwrap();
    } else {
        crash_test::without_crash(|| storage.reclaim_retired_index_tail().unwrap());
        let mut tx = storage.begin_transaction().unwrap();
        let new = storage
            .create_named_index_in(&mut tx, IndexName::new("new_tail").unwrap(), ColumnId(2))
            .unwrap();
        assert_eq!(new.handle.meta_page.page_id(), PageId(3));
        crash_test::maybe_crash(TestCrashPoint::GenerationReuseBeforeCommit);
        if crash_test::is_enabled(TestCrashPoint::GenerationReuseAfterFlush) {
            storage.buffer.flush_all().unwrap();
            crash_test::maybe_crash(TestCrashPoint::GenerationReuseAfterFlush);
        }
        tx.commit().unwrap();
        crash_test::maybe_crash(TestCrashPoint::CommittedWithoutDataFlush);
    }
}

#[test]
fn tail_process_crash_matrix_converges_across_three_reopens() {
    for point in [
        TestCrashPoint::TailAfterCheckpoint,
        TestCrashPoint::TailIntentAfterLogs,
        TestCrashPoint::TailIntentDurable,
        TestCrashPoint::TailAfterInvalidation,
        TestCrashPoint::TailAfterSetLen,
        TestCrashPoint::TailAfterFileSync,
        TestCrashPoint::TailFinalizeAfterLogs,
        TestCrashPoint::TailFinalizeDurable,
        TestCrashPoint::TailAfterCompletion,
    ] {
        let (path, storage, _) = tail_fixture(&format!("round11-crash-{point:?}"));
        storage.close().unwrap();
        spawn_crash_child(&path, "tail-reclaim", point);
        let incomplete = matches!(
            point,
            TestCrashPoint::TailAfterCheckpoint | TestCrashPoint::TailIntentAfterLogs
        );
        for reopen in 0..3 {
            let mut storage = HeapStorage::open(&path, indexed_table()).unwrap();
            let report = storage.inspect_index_reclaim().unwrap();
            assert_eq!(report.database_pages, if incomplete { 5 } else { 3 });
            assert_eq!(report.pending_reclaim_indexes, u64::from(incomplete));
            assert_eq!(storage.scan().unwrap().len(), 1);
            assert!(
                storage
                    .read_index_catalog(PageId(1))
                    .unwrap()
                    .reclaim_intent
                    .is_none()
            );
            if reopen == 1 {
                storage.checkpoint().unwrap();
            }
            storage.close().unwrap();
        }
        eprintln!(
            "ROUND11_CRASH {point:?}: pages={} pending={} reopens=3",
            if incomplete { 5 } else { 3 },
            u64::from(incomplete)
        );
        cleanup(&path);
    }
}

#[test]
fn tail_reappend_crash_winner_redo_and_loser_undo() {
    for point in [
        TestCrashPoint::PageGenerationReserved,
        TestCrashPoint::BTreeAfterFirstPageUpdateLog,
        TestCrashPoint::BTreeAfterFirstPagePublish,
        TestCrashPoint::GenerationReuseBeforeCommit,
        TestCrashPoint::GenerationReuseAfterFlush,
        TestCrashPoint::CommitAfterWalSync,
        TestCrashPoint::CommittedWithoutDataFlush,
    ] {
        let (path, storage, old) = tail_fixture(&format!("round11-wal-{point:?}"));
        storage.close().unwrap();
        spawn_crash_child(&path, "tail-reappend", point);
        let winner = matches!(
            point,
            TestCrashPoint::CommitAfterWalSync | TestCrashPoint::CommittedWithoutDataFlush
        );
        if winner {
            let page = PageManager::open(&path)
                .unwrap()
                .read_page(PageId(3))
                .unwrap();
            assert!(
                page.bytes().iter().all(|byte| *byte == 0),
                "winner data must be unflushed"
            );
        }
        for reopen in 0..3 {
            let mut storage = HeapStorage::open(&path, indexed_table()).unwrap();
            assert_eq!(storage.indexes().len(), usize::from(winner));
            assert_eq!(
                storage
                    .inspect_index_reclaim()
                    .unwrap()
                    .pending_reclaim_indexes,
                0
            );
            assert_eq!(storage.scan().unwrap().len(), 1);
            assert_eq!(storage.buffer.page_count(), if winner { 5 } else { 3 });
            if winner {
                let new = storage.indexes()[0].clone();
                assert_eq!(
                    new.handle.meta_page.page_id(),
                    old.handle.meta_page.page_id()
                );
                assert!(new.handle.meta_page.generation() > old.handle.meta_page.generation());
                assert!(matches!(
                    storage.btree().height(old.handle),
                    Err(StorageError::Index(IndexError::GenerationMismatch { .. }))
                ));
                assert_eq!(
                    storage
                        .btree()
                        .lookup(new.handle, &ScalarValue::UInt64(7))
                        .unwrap()
                        .len(),
                    1
                );
            }
            if reopen == 1 {
                storage.checkpoint().unwrap();
            }
            storage.close().unwrap();
        }
        eprintln!("ROUND11_REAPPEND_CRASH {point:?}: winner={winner} reopens=3");
        cleanup(&path);
    }
}

#[test]
fn tail_stress_quantifies_physical_reclamation_and_retained_holes() {
    for interleaved in [false, true] {
        let path = test_path(&format!("round11-stress-{interleaved}"));
        cleanup(&path);
        let mut storage = HeapStorage::create(&path, indexed_table()).unwrap();
        let initial = storage.buffer.page_count();
        let mut peak = initial;
        let mut reclaimed = 0;
        let mut discovered = 0;
        let mut reused = 0;
        let mut previous = None;
        for _ in 0..100 {
            let index = storage.create_index(ColumnId(2)).unwrap();
            if previous == Some(index.handle.meta_page.page_id()) {
                reused += 1;
            }
            previous = Some(index.handle.meta_page.page_id());
            storage.drop_index(index.id).unwrap();
            discovered += 2;
            if interleaved {
                storage
                    .btree()
                    .create(IndexSpec {
                        data_type: netbadb_types::SemanticType::physical(PhysicalType::UInt64),
                        nullable: false,
                    })
                    .unwrap();
            }
            peak = peak.max(storage.buffer.page_count());
            reclaimed += storage
                .reclaim_retired_index_tail()
                .unwrap()
                .reclaimed_pages;
        }
        let inventory = storage.inspect_index_reclaim().unwrap();
        eprintln!(
            "ROUND11_STRESS interleaved={interleaved} cycles=100 initial={initial} peak={peak} final={} discovered={discovered} reclaimed={reclaimed} pending={} reused={reused} middle={}",
            inventory.database_pages,
            inventory.pending_reclaim_indexes,
            inventory.retained_middle_pages
        );
        assert_eq!(reclaimed, if interleaved { 0 } else { 200 });
        assert_eq!(
            inventory.pending_reclaim_indexes,
            if interleaved { 100 } else { 0 }
        );
        assert_eq!(reused, if interleaved { 0 } else { 99 });
        if !interleaved {
            assert_eq!(inventory.database_pages, initial);
        }
        storage.close().unwrap();
        cleanup(&path);
    }
}

#[test]
fn tail_runtime_failures_require_reopen_and_never_admit_mutations() {
    for failure in ["intent-sync", "truncate-sync", "finalize-log"] {
        let (path, mut storage, old) = tail_fixture(&format!("round11-failure-{failure}"));
        storage.flush().unwrap();
        let before = std::fs::read(&path).unwrap();
        storage.fail_tail = Some(failure);
        assert!(storage.reclaim_retired_index_tail().is_err());
        if failure == "intent-sync" {
            assert_eq!(std::fs::read(&path).unwrap(), before);
        }
        assert_eq!(
            storage.buffer.page_count(),
            if failure == "intent-sync" { 5 } else { 3 }
        );
        assert!(matches!(
            storage.begin_transaction(),
            Err(StorageError::Transaction(
                crate::TransactionError::RecoveryRequired
            ))
        ));
        assert!(storage.create_index(ColumnId(3)).is_err());
        assert!(storage.drop_index(old.id).is_err());
        assert!(storage.insert(&indexed_rows()[0]).is_err());
        assert!(storage.checkpoint().is_err());
        assert!(storage.compact_index_catalog().is_err());
        assert!(storage.reclaim_retired_index_tail().is_err());
        // Simulate closing a failed process; startup is the only retry entry.
        storage.skip_drop_flush = true;
        drop(storage);
        for _ in 0..3 {
            let mut storage = HeapStorage::open(&path, indexed_table()).unwrap();
            assert_eq!(storage.buffer.page_count(), 3);
            assert_eq!(
                storage
                    .inspect_index_reclaim()
                    .unwrap()
                    .pending_reclaim_indexes,
                0
            );
            assert_eq!(storage.scan().unwrap().len(), 1);
            storage.close().unwrap();
        }
        cleanup(&path);
    }
}

#[test]
fn tail_intent_reopen_rejects_every_illegal_file_length() {
    for count in [2, 4, 6] {
        let (path, storage, _) = tail_fixture(&format!("round11-length-{count}"));
        storage.close().unwrap();
        spawn_crash_child(&path, "tail-reclaim", TestCrashPoint::TailIntentDurable);
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(count * crate::PAGE_SIZE as u64)
            .unwrap();
        let bytes = std::fs::read(&path).unwrap();
        for _ in 0..3 {
            assert!(HeapStorage::open(&path, indexed_table()).is_err());
            assert_eq!(std::fs::read(&path).unwrap(), bytes);
        }
        cleanup(&path);
    }
}

#[test]
fn tail_corrupt_intent_catalog_identities_never_truncate() {
    for corruption in [
        "unknown",
        "wrong-ref",
        "active",
        "legacy",
        "boundary",
        "duplicate",
    ] {
        let (path, storage, _) = tail_fixture(&format!("round11-intent-{corruption}"));
        storage.close().unwrap();
        spawn_crash_child(&path, "tail-reclaim", TestCrashPoint::TailIntentDurable);
        let mut disk = PageManager::open(&path).unwrap();
        let mut root = disk.read_page(PageId(1)).unwrap();
        let mut payload = root
            .single_payload(PageType::IndexCatalog)
            .unwrap()
            .to_vec();
        // One unnamed full retired entry (56 bytes) then a 32+24-byte intent.
        let intent = 48 + 56;
        match corruption {
            "unknown" => {
                payload[40..48].copy_from_slice(&99_u64.to_le_bytes());
                payload[intent + 32..intent + 40].copy_from_slice(&98_u64.to_le_bytes());
            }
            "wrong-ref" => {
                payload[intent + 48..intent + 56].copy_from_slice(&1_u64.to_le_bytes());
            }
            "active" => {
                payload[48 + 36] = 0;
            }
            "legacy" => {
                payload[48 + 37] = 1;
                payload[48 + 48..48 + 56].fill(0);
            }
            "boundary" => {
                payload[intent + 16..intent + 24].copy_from_slice(&1_u64.to_le_bytes());
            }
            "duplicate" => {
                let record = payload[intent + 32..intent + 56].to_vec();
                payload[intent + 24..intent + 28].copy_from_slice(&2_u32.to_le_bytes());
                payload.extend(record);
            }
            _ => unreachable!(),
        }
        root.replace_single_payload(PageType::IndexCatalog, &payload)
            .unwrap();
        root.refresh_checksum();
        disk.write_page(&root).unwrap();
        disk.sync().unwrap();
        drop(disk);
        let before = std::fs::read(&path).unwrap();
        assert!(
            HeapStorage::open(&path, indexed_table()).is_err(),
            "{corruption}"
        );
        assert_eq!(std::fs::read(&path).unwrap(), before);
        cleanup(&path);
    }
}

#[test]
fn tail_corrupt_owned_page_fails_before_any_checkpoint_or_truncate() {
    let (path, mut storage, old) = tail_fixture("round11-corrupt-page");
    storage.checkpoint().unwrap();
    let lsn = storage.transactions.wal().borrow().next_lsn();
    let page_id = old.handle.meta_page.page_id();
    storage
        .buffer
        .write_page(page_id)
        .unwrap()
        .page_mut()
        .bytes_mut()[300] ^= 1;
    let length = std::fs::metadata(&path).unwrap().len();
    assert!(storage.reclaim_retired_index_tail().is_err());
    assert_eq!(storage.transactions.wal().borrow().next_lsn(), lsn);
    assert_eq!(std::fs::metadata(&path).unwrap().len(), length);
    storage.skip_drop_flush = true;
    drop(storage);
    cleanup(&path);
}

#[test]
fn tail_whole_tree_keeps_split_middle_owner_and_reclaims_only_later_complete_tree() {
    let (path, mut storage, a) = tail_fixture("round11-middle-whole");
    storage
        .btree()
        .create(IndexSpec {
            data_type: netbadb_types::SemanticType::physical(PhysicalType::UInt64),
            nullable: false,
        })
        .unwrap();
    // Add a legitimate self-identifying orphan to the retired owner's inventory,
    // separated from its meta/root by a raw tree. No ownership is guessed.
    let mut tx = storage.begin_transaction().unwrap();
    let generation = tx.reserve_page_generation().unwrap();
    let id = PageId(storage.buffer.page_count());
    let spec = storage.btree().spec(a.handle).unwrap();
    let payload = encode_leaf_generation(
        &spec,
        &netbadb_index::LeafNode::empty(),
        Some(a.id),
        Some(generation),
    )
    .unwrap();
    let before = crate::Page::zero(id);
    let mut after = crate::Page::new(id, PageType::BTreeLeaf);
    after
        .initialize_single_payload(PageType::BTreeLeaf, &payload)
        .unwrap();
    tx.log_page_update(&before, &mut after).unwrap();
    *storage.buffer.new_page().unwrap().page_mut() = after;
    tx.commit().unwrap();
    drop(tx);
    storage.compact_index_catalog().unwrap();
    let no = storage.reclaim_retired_index_tail().unwrap();
    assert_eq!(
        (
            no.eligible_pages,
            no.reclaimed_pages,
            no.pending_indexes_remaining
        ),
        (1, 0, 1)
    );
    let b = storage.create_index(ColumnId(3)).unwrap();
    storage.drop_index(b.id).unwrap();
    let yes = storage.reclaim_retired_index_tail().unwrap();
    assert_eq!(
        (
            yes.eligible_pages,
            yes.reclaimed_pages,
            yes.pending_indexes_remaining
        ),
        (3, 2, 1)
    );
    let inventory = storage.inspect_index_reclaim().unwrap();
    assert_eq!(inventory.retired_owned_pages, 3);
    assert_eq!(inventory.retired_orphan_pages, 1);
    assert_eq!(inventory.pending[0].index_id, a.id);
    storage.close().unwrap();
    cleanup(&path);
}

#[test]
fn tail_complete_merged_tree_reclaims_reachable_and_orphan_pages() {
    let path = test_path("round11-merged-orphans");
    cleanup(&path);
    let mut storage = HeapStorage::create_with_buffer_pool_size(&path, indexed_table(), 1).unwrap();
    let mut rows = Vec::new();
    for id in 0..45 {
        rows.push(
            storage
                .insert(&[
                    ScalarValue::UInt64(id),
                    ScalarValue::UInt64(1),
                    ScalarValue::Text(format!("{id:04}{}", "x".repeat(1000))),
                ])
                .unwrap(),
        );
    }
    let before = storage.buffer.page_count();
    let index = storage.create_index(ColumnId(3)).unwrap();
    let old_refs = storage.inspect_index_reclaim().unwrap().allocations;
    for row in rows {
        storage.delete(row).unwrap();
    }
    storage.vacuum().unwrap();
    let inventory = storage.inspect_index_reclaim().unwrap();
    assert!(inventory.active_orphan_pages > 0);
    storage.drop_index(index.id).unwrap();
    storage.compact_index_catalog().unwrap();
    let reclaimed = storage.reclaim_retired_index_tail().unwrap();
    assert_eq!(reclaimed.reclaimed_pages, inventory.owned_pages);
    assert_eq!(storage.buffer.page_count(), before);
    assert_eq!(reclaimed.pending_indexes_remaining, 0);
    for reference in old_refs {
        assert!(
            storage
                .buffer
                .read_btree_page(BTreePageRef::Allocated(reference.page_ref))
                .is_err()
        );
    }
    storage.close().unwrap();
    let mut storage = HeapStorage::open(&path, indexed_table()).unwrap();
    assert_eq!(storage.inspect_index_reclaim().unwrap().owned_pages, 0);
    assert!(storage.scan().unwrap().is_empty());
    storage.close().unwrap();
    cleanup(&path);
}

#[test]
fn tail_legacy_registered_v1_and_raw_tail_remain_unreclaimed() {
    for legacy in [true, false] {
        let path = test_path(&format!("round11-legacy-{legacy}"));
        cleanup(&path);
        let mut storage = HeapStorage::create(&path, indexed_table()).unwrap();
        let index = storage.create_index(ColumnId(2)).unwrap();
        if !legacy {
            storage
                .btree()
                .create(IndexSpec {
                    data_type: netbadb_types::SemanticType::physical(PhysicalType::UInt64),
                    nullable: false,
                })
                .unwrap();
        }
        storage.checkpoint().unwrap();
        storage.close().unwrap();
        if legacy {
            legacy_catalog(&path, 4);
        }
        let mut storage = HeapStorage::open(&path, indexed_table()).unwrap();
        storage.drop_index(index.id).unwrap();
        let before = storage.buffer.page_count();
        assert_eq!(
            storage
                .reclaim_retired_index_tail()
                .unwrap()
                .reclaimed_pages,
            0
        );
        assert_eq!(storage.buffer.page_count(), before);
        assert_eq!(storage.retired_indexes().len(), 1);
        storage.close().unwrap();
        cleanup(&path);
    }
}

#[test]
fn tail_finalization_across_catalog_pages_recovers_steal_before_root_clear() {
    let path = test_path("round11-chain-finalization");
    cleanup(&path);
    let mut storage = HeapStorage::create(&path, indexed_table()).unwrap();
    // Fixed existing continuation below the future suffix; never allocated by intent.
    let continuation = storage.buffer.new_page().unwrap().page_id();
    {
        let mut guard = storage.buffer.write_page(continuation).unwrap();
        let mut page = crate::Page::new(continuation, PageType::IndexCatalog);
        page.initialize_single_payload(
            PageType::IndexCatalog,
            &encode_index_catalog(&netbadb_index::IndexCatalogNode {
                next_index_id: None,
                ..netbadb_index::IndexCatalogNode::empty()
            })
            .unwrap(),
        )
        .unwrap();
        *guard.page_mut() = page;
    }
    let a = storage.create_index(ColumnId(1)).unwrap();
    let b = storage.create_index(ColumnId(2)).unwrap();
    storage.drop_index(a.id).unwrap();
    storage.drop_index(b.id).unwrap();
    let mut node = decode_index_catalog(
        storage
            .buffer
            .read_page(PageId(1))
            .unwrap()
            .page()
            .single_payload(PageType::IndexCatalog)
            .unwrap(),
    )
    .unwrap();
    let entries = std::mem::take(&mut node.entries);
    node.next_catalog = Some(continuation);
    storage
        .buffer
        .write_page(PageId(1))
        .unwrap()
        .page_mut()
        .replace_single_payload(
            PageType::IndexCatalog,
            &encode_index_catalog(&node).unwrap(),
        )
        .unwrap();
    let tail = netbadb_index::IndexCatalogNode {
        next_index_id: None,
        entries,
        ..netbadb_index::IndexCatalogNode::empty()
    };
    storage
        .buffer
        .write_page(continuation)
        .unwrap()
        .page_mut()
        .replace_single_payload(
            PageType::IndexCatalog,
            &encode_index_catalog(&tail).unwrap(),
        )
        .unwrap();
    storage.checkpoint().unwrap();
    storage.close().unwrap();
    spawn_crash_child(
        &path,
        "tail-reclaim",
        TestCrashPoint::TailFinalizeAfterPagePublish,
    );
    for _ in 0..3 {
        let mut storage = HeapStorage::open(&path, indexed_table()).unwrap();
        assert_eq!(storage.buffer.page_count(), 4);
        let catalog = storage.read_index_catalog(PageId(1)).unwrap();
        assert_eq!(catalog.pages, vec![PageId(1), continuation]);
        assert!(
            catalog.entries.is_empty()
                && catalog.pending.is_empty()
                && catalog.reclaim_intent.is_none()
        );
        assert_eq!(
            storage.inspect_index_reclaim().unwrap().retired_owned_pages,
            0
        );
        storage.close().unwrap();
    }
    cleanup(&path);
}

#[test]
fn tail_admission_rejects_commit_pending_and_rollback_pending_writers() {
    for commit in [true, false] {
        let (path, mut storage, _) = tail_fixture(&format!("round11-pending-writer-{commit}"));
        let mut tx = storage.begin_transaction().unwrap();
        storage.insert_in(&mut tx, &indexed_rows()[0]).unwrap();
        storage
            .transactions
            .wal()
            .borrow_mut()
            .inject_flush_failure();
        if commit {
            assert!(tx.commit().is_err());
            assert_eq!(tx.state(), crate::TransactionState::CommitPending);
        } else {
            assert!(tx.rollback().is_err());
            assert_eq!(tx.state(), crate::TransactionState::RollbackPending);
        }
        let length = std::fs::metadata(&path).unwrap().len();
        assert!(matches!(
            storage.reclaim_retired_index_tail(),
            Err(StorageError::Checkpoint(
                CheckpointError::WriterActive { .. }
            ))
        ));
        assert_eq!(std::fs::metadata(&path).unwrap().len(), length);
        if commit {
            tx.commit().unwrap();
        } else {
            tx.rollback().unwrap();
        }
        drop(tx);
        assert_eq!(
            storage
                .reclaim_retired_index_tail()
                .unwrap()
                .reclaimed_pages,
            2
        );
        storage.close().unwrap();
        cleanup(&path);
    }
}
