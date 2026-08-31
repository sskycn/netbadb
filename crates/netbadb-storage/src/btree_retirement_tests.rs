fn retirement_fixture(
    case: &str,
    capacity: usize,
) -> (std::path::PathBuf, HeapStorage, crate::IndexDefinition) {
    let path = test_path(case);
    cleanup(&path);
    let mut storage =
        HeapStorage::create_with_buffer_pool_size(&path, indexed_table(), capacity).unwrap();
    for id in 0..90 {
        storage.insert(&retirement_values(id)).unwrap();
    }
    let index = storage.create_index(ColumnId(3)).unwrap();
    assert!(storage.btree().height(index.handle).unwrap() >= 4);
    (path, storage, index)
}

fn retirement_values(id: u64) -> Vec<ScalarValue> {
    vec![
        ScalarValue::UInt64(id),
        ScalarValue::UInt64(1),
        ScalarValue::Text(format!("{id:04}{}", "x".repeat(1000))),
    ]
}

fn retirement_images(storage: &mut HeapStorage) -> Vec<Page> {
    storage
        .inspect_index_reclaim()
        .unwrap()
        .allocations
        .iter()
        .map(|p| {
            storage
                .buffer
                .allocation_snapshot(p.page_ref.page_id)
                .unwrap()
        })
        .collect()
}

fn retirement_assert_small(storage: &mut HeapStorage, index: &crate::IndexDefinition) {
    assert_eq!(storage.btree().height(index.handle).unwrap(), 1);
    let report = storage.inspect_index_reclaim().unwrap(); // includes full active traversal
    assert_eq!(report.active_orphan_pages, 0);
    assert_eq!(report.retired_marker_pages, 81);
    assert_eq!(report.owned_pages, 83);
    assert_eq!(storage.vacuum().unwrap(), 0);
    assert!(
        storage
            .btree()
            .lookup(index.handle, &retirement_values(0)[2])
            .unwrap()
            .is_empty()
    );
}

#[test]
fn retirement_p0_runtime_rollback_and_winner() {
    for capacity in [1, 8] {
        let (path, mut storage, index) =
            retirement_fixture(&format!("round14-p0-{capacity}"), capacity);
        storage.buffer.flush_all().unwrap();
        let before = retirement_images(&mut storage);
        let rows = storage.scan().unwrap();
        let mut tx = storage.begin_transaction().unwrap();
        for (row, values) in &rows {
            storage
                .btree()
                .delete_in(&mut tx, index.handle, values[2].clone(), *row)
                .unwrap();
        }
        assert_eq!(tx.retired_btree_pages.len(), 81);
        tx.rollback().unwrap();
        drop(tx);
        for old in &before {
            assert_eq!(
                storage.buffer.allocation_snapshot(old.id).unwrap().bytes(),
                old.bytes()
            );
        }
        assert_eq!(
            storage
                .inspect_index_reclaim()
                .unwrap()
                .retired_marker_pages,
            0
        );
        for (row, values) in rows {
            assert_eq!(
                storage.btree().lookup(index.handle, &values[2]).unwrap(),
                vec![row]
            );
            storage.delete(row).unwrap();
        }
        // DELETE itself leaves all physical index allocations active.
        assert_eq!(
            storage
                .inspect_index_reclaim()
                .unwrap()
                .retired_marker_pages,
            0
        );
        assert_eq!(storage.vacuum().unwrap(), 90);
        retirement_assert_small(&mut storage, &index);
        let after = retirement_images(&mut storage);
        for old in &before {
            let new = after.iter().find(|p| p.id == old.id).unwrap();
            assert_eq!(
                old.allocation_generation().unwrap(),
                new.allocation_generation().unwrap()
            );
        }
        storage.close().unwrap();
        let mut storage = HeapStorage::open(&path, indexed_table()).unwrap();
        retirement_assert_small(&mut storage, &index);
        storage.close().unwrap();
        cleanup(&path);
    }
}

#[test]
fn retirement_uncommitted_rebuild_excluded_and_active_pointer_rejected() {
    let (path, mut storage, index) = retirement_fixture("round14-uncommitted", 8);
    let mut tx = storage.begin_transaction().unwrap();
    for (row, values) in storage.scan().unwrap() {
        storage
            .btree()
            .delete_in(&mut tx, index.handle, values[2].clone(), row)
            .unwrap();
    }
    assert_eq!(tx.retired_btree_pages.len(), 81);
    storage.buffer.flush_all().unwrap(); // uncommitted steal must not authorize reuse
    storage.reusable_btree_pages = None;
    assert!(
        storage
            .claim_reusable_btree_page(&mut tx, index.id)
            .unwrap()
            .is_none()
    );
    let reference = *tx.retired_btree_pages.iter().next().unwrap();
    let meta_before = storage
        .buffer
        .allocation_snapshot(index.handle.meta_page.page_id())
        .unwrap();
    let mut meta = storage.btree().read_meta(index.handle).unwrap();
    meta.root_page = netbadb_index::BTreePageRef::Allocated(reference);
    storage
        .buffer
        .write_btree_page(index.handle.meta_page)
        .unwrap()
        .page_mut()
        .replace_single_payload(
            PageType::BTreeMeta,
            &netbadb_index::encode_meta(&meta).unwrap(),
        )
        .unwrap();
    assert!(
        storage
            .btree()
            .lookup(index.handle, &retirement_values(0)[2])
            .is_err()
    );
    *storage
        .buffer
        .write_btree_page(index.handle.meta_page)
        .unwrap()
        .page_mut() = meta_before;
    tx.rollback().unwrap();
    drop(tx);
    assert_eq!(
        storage
            .inspect_index_reclaim()
            .unwrap()
            .retired_marker_pages,
        0
    );
    assert!(
        storage
            .inspect_reusable_pages()
            .unwrap()
            .candidates
            .is_empty()
    );
    storage.close().unwrap();
    cleanup(&path);
}

fn retirement_shrink(storage: &mut HeapStorage) {
    let mut tx = storage.begin_transaction().unwrap();
    for (row, _) in storage.scan().unwrap() {
        storage.delete_in(&mut tx, row).unwrap();
    }
    tx.commit().unwrap();
    drop(tx);
    assert_eq!(storage.vacuum().unwrap(), 90);
}

fn retirement_grow(storage: &mut HeapStorage, tx: &mut crate::Transaction) {
    for id in 0..90 {
        storage.insert_in(tx, &retirement_values(id)).unwrap();
    }
}

fn retirement_assert_rows(storage: &mut HeapStorage, index: &crate::IndexDefinition) {
    let rows = storage.scan().unwrap();
    assert_eq!(rows.len(), 90);
    for (row, values) in &rows {
        assert_eq!(
            storage.btree().lookup(index.handle, &values[2]).unwrap(),
            vec![*row]
        );
    }
    let range = netbadb_index::IndexRange {
        lower: netbadb_index::IndexBound::Unbounded,
        upper: netbadb_index::IndexBound::Unbounded,
    };
    let ordered = storage.btree().lookup_range(index.handle, &range).unwrap();
    let mut expected = rows;
    expected.sort_by(|a, b| {
        netbadb_index::compare_entry_keys(
            &netbadb_index::IndexEntry {
                key: a.1[2].clone(),
                row_id: a.0,
            },
            &netbadb_index::IndexEntry {
                key: b.1[2].clone(),
                row_id: b.0,
            },
        )
    });
    assert_eq!(
        ordered,
        expected.into_iter().map(|r| r.0).collect::<Vec<_>>()
    );
    storage.inspect_index_reclaim().unwrap();
}

#[test]
fn retirement_same_owner_dml_splits_rollback_exact_then_reuse_all() {
    for capacity in [1, 8] {
        let (path, mut storage, index) =
            retirement_fixture(&format!("round14-regrow-{capacity}"), capacity);
        retirement_shrink(&mut storage);
        retirement_assert_small(&mut storage, &index);
        storage.buffer.flush_all().unwrap();
        let candidates = storage.inspect_reusable_pages().unwrap().candidates;
        assert_eq!(candidates.len(), 81);
        assert!(
            candidates
                .iter()
                .all(|p| p.class == crate::PageReuseClass::RetiredBTreeMarker)
        );
        let markers: Vec<_> = candidates
            .iter()
            .map(|p| {
                storage
                    .buffer
                    .allocation_snapshot(p.page_ref.page_id)
                    .unwrap()
            })
            .collect();
        let count = storage.buffer.page_count();
        let mut burned = std::collections::HashMap::new();
        for winner in [false, false, true] {
            let mut tx = storage.begin_transaction().unwrap();
            for id in 0..90 {
                storage.insert_in(&mut tx, &retirement_values(id)).unwrap();
                if [29, 69, 89].contains(&id) {
                    let catalog = storage.read_index_catalog(PageId(1)).unwrap();
                    let inventory = storage.index_page_inventory(&catalog).unwrap();
                    let remaining = inventory.markers.len();
                    assert!(
                        remaining
                            <= if id == 29 {
                                71
                            } else if id == 69 {
                                31
                            } else {
                                0
                            }
                    );
                    println!(
                        "ROUND14 staged capacity={capacity} rows={} markers_consumed={} remaining={remaining}",
                        id + 1,
                        81 - remaining
                    );
                }
            }
            assert_eq!(storage.buffer.page_count(), count);
            assert!(storage.btree().height(index.handle).unwrap() >= 4);
            for old in &markers {
                let new = storage.buffer.allocation_snapshot(old.id).unwrap();
                assert!(!crate::allocation_transition::is_retired(&new).unwrap());
                let (reference, owner) = crate::allocation_transition::identity(&new).unwrap();
                assert_eq!(owner, index.id);
                assert!(reference.generation > old.allocation_generation().unwrap().unwrap());
                if let Some(previous) = burned.insert(old.id, reference.generation) {
                    assert!(reference.generation > previous);
                }
                assert!(
                    storage
                        .buffer
                        .read_btree_page(netbadb_index::BTreePageRef::Allocated(
                            crate::allocation_transition::identity(old).unwrap().0
                        ))
                        .is_err()
                );
            }
            if winner {
                tx.commit().unwrap();
                drop(tx);
                retirement_assert_rows(&mut storage, &index);
            } else {
                tx.rollback().unwrap();
                drop(tx);
                for old in &markers {
                    assert_eq!(
                        storage.buffer.allocation_snapshot(old.id).unwrap().bytes(),
                        old.bytes()
                    );
                }
                assert!(storage.scan().unwrap().is_empty());
                assert_eq!(
                    storage.inspect_reusable_pages().unwrap().candidates.len(),
                    81
                );
            }
        }
        assert!(
            storage
                .inspect_reusable_pages()
                .unwrap()
                .candidates
                .is_empty()
        );
        storage.analyze().unwrap();
        storage.close().unwrap();
        let mut storage = HeapStorage::open(&path, indexed_table()).unwrap();
        retirement_assert_rows(&mut storage, &index);
        storage.close().unwrap();
        cleanup(&path);
    }
}

#[test]
fn retirement_grow_shrink_100_cycles_bounded() {
    let (path, mut storage, index) = retirement_fixture("round14-churn", 8);
    let initial = storage.buffer.page_count();
    let mut peak = initial;
    let mut retirements = 0;
    let mut transitions = 0;
    let mut appends = 0;
    storage.checkpoint().unwrap();
    for cycle in 0..100 {
        retirement_shrink(&mut storage);
        retirement_assert_small(&mut storage, &index);
        assert_eq!(
            storage.inspect_reusable_pages().unwrap().candidates.len(),
            81
        );
        storage.buffer.flush_all().unwrap();
        let mut tx = storage.begin_transaction().unwrap();
        retirement_grow(&mut storage, &mut tx);
        tx.commit().unwrap();
        drop(tx);
        retirement_assert_rows(&mut storage, &index);
        assert!(
            storage
                .inspect_reusable_pages()
                .unwrap()
                .candidates
                .is_empty()
        );
        for record in storage.wal_records().unwrap() {
            match record.kind {
                crate::WalRecordKind::PageAllocationTransition { .. } => transitions += 1,
                crate::WalRecordKind::PageUpdate {
                    page_id,
                    before,
                    after,
                } => {
                    if before.iter().all(|b| *b == 0) {
                        appends += 1;
                    }
                    let p = Page::from_bytes(page_id, *after);
                    let k = p.header().unwrap().page_type;
                    if matches!(k, PageType::BTreeLeaf | PageType::BTreeInternal)
                        && netbadb_index::retired_btree_page(p.single_payload(k).unwrap())
                            .unwrap()
                            .is_some()
                    {
                        retirements += 1;
                    }
                }
                _ => {}
            }
        }
        peak = peak.max(storage.buffer.page_count());
        assert_eq!(storage.buffer.page_count(), initial, "cycle {cycle}");
        storage.checkpoint().unwrap();
    }
    println!(
        "ROUND14_CHURN cycles=100 initial={initial} peak={peak} final={} marker_retirements={retirements} marker_transitions={transitions} appends={appends} historical_unmarked=0",
        storage.buffer.page_count()
    );
    assert_eq!(retirements, 8100);
    assert_eq!(transitions, 8100);
    assert_eq!(appends, 0);
    storage.close().unwrap();
    cleanup(&path);
}

#[test]
fn retirement_marker_buffer_eligibility_and_backfill() {
    let (path, mut storage, index) = retirement_fixture("round14-marker-buffers", 128);
    retirement_shrink(&mut storage);
    storage.buffer.flush_all().unwrap();
    let candidates = storage.inspect_reusable_pages().unwrap().candidates;
    let first = candidates[0].page_ref;
    let pin = storage
        .buffer
        .read_btree_page(netbadb_index::BTreePageRef::Allocated(first))
        .unwrap();
    let mut tx = storage.begin_transaction().unwrap();
    assert_eq!(
        storage
            .claim_reusable_btree_page(&mut tx, index.id)
            .unwrap()
            .unwrap()
            .id,
        candidates[1].page_ref.page_id
    );
    drop(pin);
    tx.rollback().unwrap();
    drop(tx);
    // An unchanged write guard still makes this frame dirty and ineligible.
    storage
        .buffer
        .write_btree_page(netbadb_index::BTreePageRef::Allocated(first))
        .unwrap()
        .page_mut();
    let mut tx = storage.begin_transaction().unwrap();
    assert_ne!(
        storage
            .claim_reusable_btree_page(&mut tx, index.id)
            .unwrap()
            .unwrap()
            .id,
        first.page_id
    );
    tx.rollback().unwrap();
    drop(tx);
    storage.buffer.flush_all().unwrap();
    let pins: Vec<_> = candidates
        .iter()
        .map(|p| {
            storage
                .buffer
                .read_btree_page(netbadb_index::BTreePageRef::Allocated(p.page_ref))
                .unwrap()
        })
        .collect();
    let count = storage.buffer.page_count();
    let other = storage.create_index(ColumnId(1)).unwrap();
    assert_eq!(storage.buffer.page_count(), count + 2);
    assert_eq!(other.handle.meta_page.page_id().0, count);
    drop(pins);
    // Build heap rows while X is dropped; then real CREATE backfills into its
    // committed marker inventory through the same registered allocator.
    storage.drop_index(index.id).unwrap();
    let mut tx = storage.begin_transaction().unwrap();
    retirement_grow(&mut storage, &mut tx);
    tx.commit().unwrap();
    drop(tx);
    storage.buffer.flush_all().unwrap();
    let count = storage.buffer.page_count();
    let rebuilt = storage.create_index(ColumnId(3)).unwrap();
    assert_eq!(storage.buffer.page_count(), count);
    retirement_assert_rows(&mut storage, &rebuilt);
    storage.analyze().unwrap();
    storage.close().unwrap();
    cleanup(&path);
}

#[test]
fn retirement_crash_child() {
    if std::env::var_os(crate::crash_test::CHILD_ENV).is_none() {
        return;
    }
    let path =
        std::path::PathBuf::from(std::env::var_os(crate::crash_test::DATABASE_PATH_ENV).unwrap());
    let mut storage = HeapStorage::open_with_buffer_pool_size(&path, indexed_table(), 512).unwrap();
    let case = std::env::var_os(crate::crash_test::CASE_ENV).unwrap();
    if case == "reuse" || case == "reuse-undo" {
        let mut tx = storage.begin_transaction().unwrap();
        retirement_grow(&mut storage, &mut tx);
        crate::crash_test::maybe_crash(TestCrashPoint::GenerationReuseBeforeCommit);
        if case == "reuse-undo" {
            tx.rollback().unwrap();
        } else {
            tx.commit().unwrap();
        }
    } else {
        storage.vacuum().unwrap();
    }
    panic!("retirement crash hook not reached");
}

#[test]
fn retirement_same_owner_reuse_process_crash_matrix() {
    for point in [
        TestCrashPoint::TransitionAfterLog,
        TestCrashPoint::TransitionAfterPublish,
        TestCrashPoint::BTreeAfterSiblingUpdate,
        TestCrashPoint::BTreeAfterParentUpdate,
        TestCrashPoint::BTreeAfterInternalSplit,
        TestCrashPoint::GenerationReuseBeforeCommit,
        TestCrashPoint::CommitAfterWalSync,
        TestCrashPoint::RollbackAfterPageUndo,
        TestCrashPoint::TransitionAfterUndo,
    ] {
        let (path, mut storage, index) =
            retirement_fixture(&format!("round14-reuse-crash-{}", point.as_str()), 8);
        retirement_shrink(&mut storage);
        storage.buffer.flush_all().unwrap();
        let before = retirement_images(&mut storage);
        storage.close().unwrap();
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command.args([
            "--exact",
            "heap::tests::maintenance::retirement_crash_child",
            "--nocapture",
        ]);
        let undo = matches!(
            point,
            TestCrashPoint::RollbackAfterPageUndo | TestCrashPoint::TransitionAfterUndo
        );
        crate::crash_test::configure_child(
            &mut command,
            if undo { "reuse-undo" } else { "reuse" },
            &path,
            point,
        );
        let output = command.output().unwrap();
        assert_eq!(
            output.status.code(),
            Some(crate::crash_test::EXIT_CODE),
            "{point:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let winner = point == TestCrashPoint::CommitAfterWalSync;
        if winner {
            let mut disk = PageManager::open(&path).unwrap();
            for old in &before {
                assert_eq!(disk.read_page(old.id).unwrap().bytes(), old.bytes());
            }
        }
        for _ in 0..3 {
            let mut storage = HeapStorage::open(&path, indexed_table()).unwrap();
            if winner {
                retirement_assert_rows(&mut storage, &index);
                assert_eq!(
                    storage
                        .inspect_index_reclaim()
                        .unwrap()
                        .retired_marker_pages,
                    0
                );
            } else {
                retirement_assert_small(&mut storage, &index);
                for old in &before {
                    assert_eq!(
                        storage.buffer.allocation_snapshot(old.id).unwrap().bytes(),
                        old.bytes()
                    );
                }
            }
            storage.close().unwrap();
        }
        println!("ROUND14 reuse crash {point:?}: winner={winner}, three reopens verified");
        cleanup(&path);
    }
}

#[test]
fn retirement_different_owner_drop_pending_cleanup_and_candidate_dedup() {
    let (path, mut storage, index) = retirement_fixture("round14-drop-markers", 8);
    retirement_shrink(&mut storage);
    storage.buffer.flush_all().unwrap();
    let original = storage.inspect_reusable_pages().unwrap().candidates;
    assert_eq!(original.len(), 81);
    // An uncommitted DROP cannot change already committed marker authority.
    let mut tx = storage.begin_transaction().unwrap();
    storage.drop_index_in(&mut tx, index.id).unwrap();
    tx.rollback().unwrap();
    drop(tx);
    assert_eq!(
        storage.inspect_reusable_pages().unwrap().candidates,
        original
    );
    storage.drop_index(index.id).unwrap();
    storage.compact_index_catalog().unwrap();
    let all = storage.inspect_reusable_pages().unwrap().candidates;
    assert_eq!(all.len(), 83);
    assert_eq!(
        all.iter()
            .filter(|p| p.class == crate::PageReuseClass::RetiredBTreeMarker)
            .count(),
        81
    );
    // Lowest IDs are the whole-owner meta/root; consume those first.
    let other = storage.create_index(ColumnId(3)).unwrap();
    storage.compact_index_catalog().unwrap();
    assert!(storage.inspect_index_reclaim().unwrap().pending.is_empty());
    assert_eq!(
        storage.inspect_reusable_pages().unwrap().candidates.len(),
        81
    );
    let mut tx = storage.begin_transaction().unwrap();
    retirement_grow(&mut storage, &mut tx);
    tx.commit().unwrap();
    drop(tx);
    retirement_assert_rows(&mut storage, &other);
    assert!(
        storage
            .inspect_reusable_pages()
            .unwrap()
            .candidates
            .is_empty()
    );
    for old in original {
        let page = storage
            .buffer
            .allocation_snapshot(old.page_ref.page_id)
            .unwrap();
        let (reference, owner) = crate::allocation_transition::identity(&page).unwrap();
        assert_eq!(owner, other.id);
        assert!(reference.generation > old.page_ref.generation);
    }
    storage.close().unwrap();
    cleanup(&path);
}

#[test]
fn retirement_p0_real_vacuum_crash_matrix() {
    for point in [
        TestCrashPoint::RetirementAfterLeafUnlink,
        TestCrashPoint::RetirementAfterParentUnlink,
        TestCrashPoint::RetirementBeforeLog,
        TestCrashPoint::RetirementAfterLog,
        TestCrashPoint::RetirementAfterPublish,
        TestCrashPoint::BTreeAfterUnaryNormalization,
        TestCrashPoint::RetirementBeforeCommit,
        TestCrashPoint::CommitAfterAppend,
        TestCrashPoint::CommitAfterWalSync,
    ] {
        let (path, mut storage, index) =
            retirement_fixture(&format!("round14-crash-{}", point.as_str()), 8);
        for (row, _) in storage.scan().unwrap() {
            storage.delete(row).unwrap();
        }
        storage.checkpoint().unwrap();
        let before = retirement_images(&mut storage);
        storage.close().unwrap();
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command.args([
            "--exact",
            "heap::tests::maintenance::retirement_crash_child",
            "--nocapture",
        ]);
        crate::crash_test::configure_child(&mut command, "retire", &path, point);
        let output = command.output().unwrap();
        assert_eq!(
            output.status.code(),
            Some(crate::crash_test::EXIT_CODE),
            "{point:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        // CommitAfterAppend is not synced, but process exit leaves the complete
        // record readable; this process-loss model treats it as a winner too.
        let winner = matches!(
            point,
            TestCrashPoint::CommitAfterWalSync | TestCrashPoint::CommitAfterAppend
        );
        if winner {
            let mut disk = PageManager::open(&path).unwrap();
            for old in &before {
                assert_eq!(
                    disk.read_page(old.id).unwrap().bytes(),
                    old.bytes(),
                    "winner must leave active disk bytes unflushed"
                );
            }
        }
        for _ in 0..3 {
            let mut storage = HeapStorage::open(&path, indexed_table()).unwrap();
            if winner {
                retirement_assert_small(&mut storage, &index);
            } else {
                assert!(storage.btree().height(index.handle).unwrap() >= 4);
                assert_eq!(
                    storage
                        .inspect_index_reclaim()
                        .unwrap()
                        .retired_marker_pages,
                    0
                );
                for old in &before {
                    assert_eq!(
                        storage.buffer.allocation_snapshot(old.id).unwrap().bytes(),
                        old.bytes()
                    );
                }
            }
            storage.close().unwrap();
        }
        println!("ROUND14 retirement crash {point:?}: winner={winner}, three reopens verified");
        if winner {
            let mut storage = HeapStorage::open(&path, indexed_table()).unwrap();
            assert_eq!(
                storage.inspect_reusable_pages().unwrap().candidates.len(),
                81
            );
            let count = storage.buffer.page_count();
            let mut tx = storage.begin_transaction().unwrap();
            retirement_grow(&mut storage, &mut tx);
            tx.commit().unwrap();
            drop(tx);
            retirement_assert_rows(&mut storage, &index);
            assert_eq!(storage.buffer.page_count(), count);
            assert!(
                storage
                    .inspect_reusable_pages()
                    .unwrap()
                    .candidates
                    .is_empty()
            );
            storage.close().unwrap();
        }
        cleanup(&path);
    }
}
