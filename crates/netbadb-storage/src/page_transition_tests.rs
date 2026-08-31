// P0 uses the typed WAL boundary directly, before enabling the allocator.
fn transition_fixture(
    case: &str,
    capacity: usize,
) -> (
    std::path::PathBuf,
    HeapStorage,
    Page,
    crate::IndexDefinition,
) {
    let path = test_path(case);
    cleanup(&path);
    let mut storage =
        HeapStorage::create_with_buffer_pool_size(&path, indexed_table(), capacity).unwrap();
    let old = storage.create_index(ColumnId(1)).unwrap();
    let active = storage.create_index(ColumnId(2)).unwrap();
    storage.drop_index(old.id).unwrap();
    storage.compact_index_catalog().unwrap();
    storage.buffer.flush_all().unwrap();
    let before = storage
        .buffer
        .allocation_snapshot(old.handle.meta_page.page_id())
        .unwrap();
    (path, storage, before, active)
}

fn transition_publish(
    storage: &mut HeapStorage,
    tx: &mut crate::Transaction,
    before: &Page,
    owner: IndexId,
) -> Page {
    let generation = tx.reserve_page_generation().unwrap();
    let spec = netbadb_index::IndexSpec {
        data_type: netbadb_types::SemanticType::physical(PhysicalType::UInt64),
        nullable: true,
    };
    let payload = netbadb_index::encode_leaf_generation(
        &spec,
        &netbadb_index::LeafNode::empty(),
        Some(owner),
        Some(generation),
    )
    .unwrap();
    let mut after = Page::new(before.id, PageType::BTreeLeaf);
    after
        .initialize_single_payload(PageType::BTreeLeaf, &payload)
        .unwrap();
    tx.log_page_transition(before, &mut after).unwrap();
    storage
        .buffer
        .publish_page_transition(before, &after)
        .unwrap();
    after
}
fn transition_update(storage: &mut HeapStorage, tx: &mut crate::Transaction, page: &mut Page) {
    let before = page.clone();
    tx.log_page_update(&before, page).unwrap();
    let reference = netbadb_index::BTreePageRef::Allocated(
        crate::allocation_transition::identity(page).unwrap().0,
    );
    *storage
        .buffer
        .write_btree_page(reference)
        .unwrap()
        .page_mut() = page.clone();
}

#[test]
fn transition_runtime_rollback_exact_bytes_and_generation_burn() {
    for capacity in [1, 8] {
        let (path, mut storage, before, active) =
            transition_fixture(&format!("round13-runtime-{capacity}"), capacity);
        let old = crate::allocation_transition::identity(&before).unwrap();
        let mut previous = old.0.generation;
        for winner in [false, false, true] {
            let mut tx = storage.begin_transaction().unwrap();
            let mut after = transition_publish(&mut storage, &mut tx, &before, active.id);
            let new = crate::allocation_transition::identity(&after).unwrap();
            assert_eq!(old.0.page_id, new.0.page_id);
            assert!(new.0.generation > previous);
            previous = new.0.generation;
            for _ in 0..3 {
                transition_update(&mut storage, &mut tx, &mut after);
            }
            if winner {
                tx.commit().unwrap();
                assert_eq!(
                    storage
                        .buffer
                        .allocation_snapshot(before.id)
                        .unwrap()
                        .bytes(),
                    after.bytes()
                );
                assert!(
                    storage
                        .buffer
                        .read_btree_page(netbadb_index::BTreePageRef::Allocated(old.0))
                        .is_err()
                );
                assert!(
                    storage
                        .buffer
                        .read_btree_page(netbadb_index::BTreePageRef::Allocated(new.0))
                        .is_ok()
                );
                println!(
                    "transition capacity={capacity}: old={old:?} new={new:?}; old ref stale, new valid"
                );
            } else {
                tx.rollback().unwrap();
                assert_eq!(
                    storage
                        .buffer
                        .allocation_snapshot(before.id)
                        .unwrap()
                        .bytes(),
                    before.bytes()
                );
            }
        }
        assert_eq!(
            storage.inspect_reusable_pages().unwrap().candidates.len(),
            1
        );
        storage.close().unwrap();
        let mut reopened = HeapStorage::open(&path, indexed_table()).unwrap();
        assert_eq!(
            reopened.inspect_reusable_pages().unwrap().candidates.len(),
            1
        );
        reopened.close().unwrap();
        cleanup(&path);
    }
}

#[test]
fn transition_p0_process_child() {
    if std::env::var_os(crate::crash_test::CHILD_ENV).is_none() {
        return;
    }
    let path =
        std::path::PathBuf::from(std::env::var_os(crate::crash_test::DATABASE_PATH_ENV).unwrap());
    let mut storage = HeapStorage::open_with_buffer_pool_size(&path, indexed_table(), 8).unwrap();
    let candidate = storage.inspect_reusable_pages().unwrap().candidates[0].clone();
    let before = storage
        .buffer
        .allocation_snapshot(candidate.page_ref.page_id)
        .unwrap();
    let active = storage.indexes()[0].clone();
    let mut tx = storage.begin_transaction().unwrap();
    let mut after = transition_publish(&mut storage, &mut tx, &before, active.id);
    for _ in 0..3 {
        transition_update(&mut storage, &mut tx, &mut after);
    }
    crate::crash_test::maybe_crash(TestCrashPoint::GenerationReuseBeforeCommit);
    if std::env::var_os(crate::crash_test::CASE_ENV).unwrap() == "undo" {
        tx.rollback().unwrap();
    } else {
        tx.commit().unwrap();
    }
    panic!("required transition crash hook was not reached");
}

#[test]
fn transition_p0_real_crash_matrix_no_checkpoint() {
    for point in [
        TestCrashPoint::TransitionAfterLog,
        TestCrashPoint::TransitionAfterPublish,
        TestCrashPoint::GenerationReuseBeforeCommit,
        TestCrashPoint::CommitAfterWalSync,
        TestCrashPoint::RollbackAfterPageUndo,
        TestCrashPoint::TransitionAfterUndo,
    ] {
        let (path, storage, before, active) =
            transition_fixture(&format!("round13-crash-{}", point.as_str()), 8);
        storage.close().unwrap();
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command.args([
            "--exact",
            "heap::tests::maintenance::transition_p0_process_child",
            "--nocapture",
        ]);
        let undo = matches!(
            point,
            TestCrashPoint::RollbackAfterPageUndo | TestCrashPoint::TransitionAfterUndo
        );
        crate::crash_test::configure_child(
            &mut command,
            if undo { "undo" } else { "commit" },
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
        for _ in 0..3 {
            let mut storage = HeapStorage::open(&path, indexed_table()).unwrap();
            let current = storage.buffer.allocation_snapshot(before.id).unwrap();
            if winner {
                let (new, owner) = crate::allocation_transition::identity(&current).unwrap();
                assert_eq!(owner, active.id);
                assert_ne!(
                    new.generation,
                    before.allocation_generation().unwrap().unwrap()
                );
            } else {
                assert_eq!(current.bytes(), before.bytes());
            }
            assert_eq!(
                storage.inspect_reusable_pages().unwrap().candidates.len(),
                if winner { 1 } else { 2 }
            );
            storage.close().unwrap();
        }
        cleanup(&path);
    }
}

#[test]
fn transition_allocator_create_rollback_commit_without_checkpoint() {
    for capacity in [1, 8] {
        let (path, mut storage, before, active) =
            transition_fixture(&format!("round13-allocator-{capacity}"), capacity);
        let inventory = storage.inspect_reusable_pages().unwrap();
        let images: Vec<_> = inventory
            .candidates
            .iter()
            .map(|c| {
                storage
                    .buffer
                    .allocation_snapshot(c.page_ref.page_id)
                    .unwrap()
            })
            .collect();
        let count = storage.buffer.page_count();
        let mut previous = before.allocation_generation().unwrap().unwrap();
        for commit in [false, false, true] {
            let mut tx = storage.begin_transaction().unwrap();
            let new = storage
                .create_named_index_in(&mut tx, IndexName::new("reuse").unwrap(), ColumnId(1))
                .unwrap();
            assert_eq!(new.handle.meta_page.page_id(), before.id);
            let generation = new.handle.meta_page.generation().unwrap();
            assert!(generation > previous);
            previous = generation;
            assert_eq!(storage.buffer.page_count(), count);
            if commit {
                tx.commit().unwrap();
                storage.publish_committed_index(new.clone());
                assert_eq!(storage.btree().height(new.handle).unwrap(), 1);
                assert!(
                    storage
                        .buffer
                        .read_btree_page(netbadb_index::BTreePageRef::Allocated(
                            inventory.candidates[0].page_ref
                        ))
                        .is_err()
                );
                println!(
                    "production capacity={capacity}: P={:?}, old G={:?} X={:?}, new G={generation:?} Y={:?}; pages={count}",
                    before.id,
                    inventory.candidates[0].page_ref.generation,
                    inventory.candidates[0].retired_index_id,
                    new.id
                );
            } else {
                tx.rollback().unwrap();
                for old in &images {
                    assert_eq!(
                        storage.buffer.allocation_snapshot(old.id).unwrap().bytes(),
                        old.bytes()
                    );
                }
            }
        }
        assert!(
            storage
                .inspect_reusable_pages()
                .unwrap()
                .candidates
                .is_empty()
        );
        assert_eq!(storage.inspect_index_reclaim().unwrap().pending.len(), 1);
        storage.compact_index_catalog().unwrap();
        assert!(storage.inspect_index_reclaim().unwrap().pending.is_empty());
        assert_eq!(storage.btree().height(active.handle).unwrap(), 1);
        storage.close().unwrap();
        for _ in 0..3 {
            let mut reopened = HeapStorage::open(&path, indexed_table()).unwrap();
            assert!(
                reopened
                    .inspect_reusable_pages()
                    .unwrap()
                    .candidates
                    .is_empty()
            );
            reopened.close().unwrap();
        }
        cleanup(&path);
    }
}

#[test]
fn transition_allocator_skips_pinned_and_dirty_but_retries_in_page_order() {
    let (path, mut storage, _, active) = transition_fixture("round13-blocked", 8);
    let candidates = storage.inspect_reusable_pages().unwrap().candidates;
    let mut tx = storage.begin_transaction().unwrap();
    // Build the cache without consuming it, then put controlled pressure on it.
    let old = storage
        .claim_reusable_btree_page(&mut tx, active.id)
        .unwrap()
        .unwrap();
    tx.rollback().unwrap();
    drop(tx);
    let mut tx = storage.begin_transaction().unwrap();
    let pin = storage.buffer.read_page(old.id).unwrap();
    let chosen = storage
        .claim_reusable_btree_page(&mut tx, active.id)
        .unwrap()
        .unwrap();
    assert_eq!(chosen.id, candidates[1].page_ref.page_id);
    assert!(
        storage
            .claim_reusable_btree_page(&mut tx, active.id)
            .unwrap()
            .is_none()
    );
    drop(pin);
    assert_eq!(
        storage
            .claim_reusable_btree_page(&mut tx, active.id)
            .unwrap()
            .unwrap()
            .id,
        old.id
    );
    tx.rollback().unwrap();
    drop(tx);
    let mut tx = storage.begin_transaction().unwrap();
    let guard = storage.buffer.write_page(old.id).unwrap();
    drop(guard);
    // Marking a page mutable preserves bytes but makes it ineligible until flush.
    let mut guard = storage.buffer.write_page(old.id).unwrap();
    let _ = guard.page_mut();
    drop(guard);
    let chosen = storage
        .claim_reusable_btree_page(&mut tx, active.id)
        .unwrap()
        .unwrap();
    assert_eq!(chosen.id, candidates[1].page_ref.page_id);
    assert!(
        storage
            .claim_reusable_btree_page(&mut tx, active.id)
            .unwrap()
            .is_none()
    );
    storage.buffer.flush_all().unwrap();
    assert_eq!(
        storage
            .claim_reusable_btree_page(&mut tx, active.id)
            .unwrap()
            .unwrap()
            .id,
        old.id
    );
    tx.rollback().unwrap();
    drop(tx);
    storage.close().unwrap();
    cleanup(&path);
}

fn transition_large_hole_fixture(
    case: &str,
    capacity: usize,
) -> (std::path::PathBuf, HeapStorage, Vec<Page>) {
    let path = test_path(case);
    cleanup(&path);
    let mut storage =
        HeapStorage::create_with_buffer_pool_size(&path, indexed_table(), capacity).unwrap();
    for id in 0..90 {
        storage
            .insert(&[
                ScalarValue::UInt64(id),
                ScalarValue::UInt64(1),
                ScalarValue::Text(format!("{id:04}{}", "x".repeat(1000))),
            ])
            .unwrap();
    }
    let donor = storage.create_index(ColumnId(3)).unwrap();
    assert!(storage.btree().height(donor.handle).unwrap() >= 3);
    // Raw suffix keeps the donor pages in the middle and never joins inventory.
    storage
        .btree()
        .create(netbadb_index::IndexSpec {
            data_type: netbadb_types::SemanticType::physical(PhysicalType::UInt64),
            nullable: false,
        })
        .unwrap();
    storage.drop_index(donor.id).unwrap();
    storage.buffer.flush_all().unwrap();
    let candidates = storage.inspect_reusable_pages().unwrap().candidates;
    assert!(candidates.len() > 10);
    let images = candidates
        .iter()
        .map(|c| {
            storage
                .buffer
                .allocation_snapshot(c.page_ref.page_id)
                .unwrap()
        })
        .collect();
    (path, storage, images)
}

#[test]
fn transition_backfill_internal_split_rollback_and_exhaustion() {
    for capacity in [1, 8] {
        let (path, mut storage, images) =
            transition_large_hole_fixture(&format!("round13-backfill-{capacity}"), capacity);
        let pages = storage.buffer.page_count();
        for commit in [false, true] {
            let mut tx = storage.begin_transaction().unwrap();
            let new = storage
                .create_named_index_in(&mut tx, IndexName::new("rebuilt").unwrap(), ColumnId(3))
                .unwrap();
            assert!(storage.btree().height(new.handle).unwrap() >= 3);
            assert_eq!(new.handle.meta_page.page_id(), images[0].id);
            assert_eq!(storage.buffer.page_count(), pages);
            if commit {
                tx.commit().unwrap();
                for (row, values) in storage.scan().unwrap() {
                    assert_eq!(
                        storage.btree().lookup(new.handle, &values[2]).unwrap(),
                        vec![row]
                    );
                }
                storage.publish_committed_index(new);
            } else {
                tx.rollback().unwrap();
                for before in &images {
                    assert_eq!(
                        storage
                            .buffer
                            .allocation_snapshot(before.id)
                            .unwrap()
                            .bytes(),
                        before.bytes()
                    );
                }
            }
        }
        assert!(
            storage
                .inspect_reusable_pages()
                .unwrap()
                .candidates
                .is_empty()
        );
        let another = storage.create_index(ColumnId(1)).unwrap();
        assert_eq!(another.handle.meta_page.page_id().0, pages);
        assert!(storage.buffer.page_count() > pages);
        println!(
            "backfill capacity={capacity}: reused={} pages before={pages} after-reuse={pages}; height>=3, append only after exhaustion",
            images.len()
        );
        storage.close().unwrap();
        let mut storage = HeapStorage::open(&path, indexed_table()).unwrap();
        assert_eq!(storage.scan().unwrap().len(), 90);
        storage.inspect_index_reclaim().unwrap();
        storage.close().unwrap();
        cleanup(&path);
    }
}

#[test]
fn transition_real_dml_split_consumes_holes_and_rollback_restores_links() {
    for capacity in [1, 8] {
        let (path, mut storage, _) =
            transition_large_hole_fixture(&format!("round13-dml-{capacity}"), capacity);
        let active = storage.create_index(ColumnId(1)).unwrap();
        storage.buffer.flush_all().unwrap();
        let before = storage.inspect_reusable_pages().unwrap().candidates;
        let snapshots: Vec<_> = before
            .iter()
            .map(|c| {
                storage
                    .buffer
                    .allocation_snapshot(c.page_ref.page_id)
                    .unwrap()
            })
            .collect();
        let original_height = storage.btree().height(active.handle).unwrap();
        let mut tx = storage.begin_transaction().unwrap();
        for id in 90..400 {
            storage
                .insert_in(
                    &mut tx,
                    &[
                        ScalarValue::UInt64(id),
                        ScalarValue::UInt64(1),
                        ScalarValue::Text("small".into()),
                    ],
                )
                .unwrap();
        }
        assert!(storage.btree().height(active.handle).unwrap() > original_height);
        tx.rollback().unwrap();
        drop(tx);
        assert_eq!(storage.scan().unwrap().len(), 90);
        assert_eq!(
            storage.btree().height(active.handle).unwrap(),
            original_height
        );
        for old in snapshots {
            assert_eq!(
                storage.buffer.allocation_snapshot(old.id).unwrap().bytes(),
                old.bytes()
            );
        }
        for id in 90..400 {
            storage
                .insert(&[
                    ScalarValue::UInt64(id),
                    ScalarValue::UInt64(1),
                    ScalarValue::Text("small".into()),
                ])
                .unwrap();
        }
        assert!(storage.inspect_reusable_pages().unwrap().candidates.len() < before.len());
        storage.close().unwrap();
        let mut storage = HeapStorage::open(&path, indexed_table()).unwrap();
        assert_eq!(storage.scan().unwrap().len(), 400);
        storage.inspect_index_reclaim().unwrap();
        storage.close().unwrap();
        cleanup(&path);
    }
}

#[test]
fn transition_production_crash_child() {
    if std::env::var_os(crate::crash_test::CHILD_ENV).is_none() {
        return;
    }
    let path =
        std::path::PathBuf::from(std::env::var_os(crate::crash_test::DATABASE_PATH_ENV).unwrap());
    // All reused pages fit, proving the durable winner without any data flush.
    let mut storage = HeapStorage::open_with_buffer_pool_size(&path, indexed_table(), 512).unwrap();
    let mut tx = storage.begin_transaction().unwrap();
    storage
        .create_named_index_in(&mut tx, IndexName::new("crash-reuse").unwrap(), ColumnId(3))
        .unwrap();
    crate::crash_test::maybe_crash(TestCrashPoint::GenerationReuseBeforeCommit);
    if std::env::var_os(crate::crash_test::CASE_ENV).unwrap() == "undo" {
        tx.rollback().unwrap();
    } else {
        tx.commit().unwrap();
    }
    panic!("production transition hook was not reached");
}

#[test]
fn transition_production_crash_matrix_backfill_leaf_and_internal_split() {
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
        let (path, storage, images) = transition_large_hole_fixture(
            &format!("round13-production-crash-{}", point.as_str()),
            8,
        );
        let pages = storage.buffer.page_count();
        storage.close().unwrap();
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command.args([
            "--exact",
            "heap::tests::maintenance::transition_production_crash_child",
            "--nocapture",
        ]);
        let undo = matches!(
            point,
            TestCrashPoint::RollbackAfterPageUndo | TestCrashPoint::TransitionAfterUndo
        );
        crate::crash_test::configure_child(
            &mut command,
            if undo { "undo" } else { "commit" },
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
            for old in &images {
                assert_eq!(
                    disk.read_page(old.id).unwrap().bytes(),
                    old.bytes(),
                    "winner must have left old data unflushed"
                );
            }
        }
        for _ in 0..3 {
            let mut storage = HeapStorage::open(&path, indexed_table()).unwrap();
            assert_eq!(storage.buffer.page_count(), pages);
            assert_eq!(storage.scan().unwrap().len(), 90);
            let inventory = storage.inspect_reusable_pages().unwrap();
            if winner {
                let new = storage.indexes()[0].clone();
                assert!(storage.btree().height(new.handle).unwrap() >= 3);
                for (row, values) in storage.scan().unwrap() {
                    assert_eq!(
                        storage.btree().lookup(new.handle, &values[2]).unwrap(),
                        vec![row]
                    );
                }
                assert!(inventory.candidates.is_empty());
                for old in &images {
                    let page = storage.buffer.allocation_snapshot(old.id).unwrap();
                    assert_eq!(
                        crate::allocation_transition::identity(&page).unwrap().1,
                        new.id
                    );
                    assert_ne!(
                        page.allocation_generation().unwrap(),
                        old.allocation_generation().unwrap()
                    );
                }
            } else {
                assert!(storage.indexes().is_empty());
                assert_eq!(inventory.candidates.len(), images.len());
                for old in &images {
                    assert_eq!(
                        storage.buffer.allocation_snapshot(old.id).unwrap().bytes(),
                        old.bytes()
                    );
                }
            }
            storage.close().unwrap();
        }
        println!(
            "ROUND13_CRASH {} winner={winner} old_pages={} reopens=3",
            point.as_str(),
            images.len()
        );
        cleanup(&path);
    }
}

#[test]
fn transition_production_meta_first_then_orphan_only_inventory() {
    for capacity in [1, 8] {
        let (path, mut storage, _) =
            transition_large_hole_fixture(&format!("round13-orphan-{capacity}"), capacity);
        // Rebuild the donor, then collapse it while active. Active orphans stay
        // excluded until its subsequent committed DROP.
        let old = storage.create_index(ColumnId(3)).unwrap();
        for (row, _) in storage.scan().unwrap() {
            storage.delete(row).unwrap();
        }
        historical_unmarked_vacuum(&mut storage);
        let active = storage.inspect_index_reclaim().unwrap();
        assert_eq!(active.active_orphan_pages, 81);
        assert!(
            storage
                .inspect_reusable_pages()
                .unwrap()
                .candidates
                .is_empty()
        );
        let meta = storage.btree().read_meta(old.handle).unwrap();
        storage.drop_index(old.id).unwrap();
        storage.compact_index_catalog().unwrap();
        storage.buffer.flush_all().unwrap();
        let new = storage.create_index(ColumnId(3)).unwrap();
        assert_eq!(
            new.handle.meta_page.page_id(),
            old.handle.meta_page.page_id()
        );
        let remaining = storage.inspect_reusable_pages().unwrap();
        assert_eq!(remaining.candidates.len(), 81);
        assert!(remaining.candidates.iter().all(|p| p.page_ref.page_id
            != meta.root_page.page_id()
            && p.retired_index_id == old.id));
        for id in 0..90 {
            storage
                .insert(&[
                    ScalarValue::UInt64(id),
                    ScalarValue::UInt64(1),
                    ScalarValue::Text(format!("{id:04}{}", "x".repeat(1000))),
                ])
                .unwrap();
        }
        assert!(
            storage
                .inspect_reusable_pages()
                .unwrap()
                .candidates
                .is_empty()
        );
        storage.compact_index_catalog().unwrap();
        assert!(storage.inspect_index_reclaim().unwrap().pending.is_empty());
        storage.close().unwrap();
        let mut storage = HeapStorage::open(&path, indexed_table()).unwrap();
        assert_eq!(storage.scan().unwrap().len(), 90);
        storage.inspect_index_reclaim().unwrap();
        storage.close().unwrap();
        cleanup(&path);
    }
}

#[test]
fn transition_rollback_retry_after_boundary_and_commit_retry_are_safe() {
    let (path, mut storage, before, active) = transition_fixture("round13-retry", 8);
    let mut tx = storage.begin_transaction().unwrap();
    let active_before = storage
        .buffer
        .allocation_snapshot(active.handle.meta_page.page_id())
        .unwrap();
    let mut active_after = active_before.clone();
    tx.log_page_update(&active_before, &mut active_after)
        .unwrap();
    *storage
        .buffer
        .write_btree_page(active.handle.meta_page)
        .unwrap()
        .page_mut() = active_after;
    let mut after = transition_publish(&mut storage, &mut tx, &before, active.id);
    for _ in 0..3 {
        transition_update(&mut storage, &mut tx, &mut after);
    }
    let pin = storage
        .buffer
        .read_btree_page(active.handle.meta_page)
        .unwrap();
    assert!(tx.rollback().is_err());
    assert_eq!(tx.state(), crate::TransactionState::RollbackPending);
    assert_eq!(
        storage
            .buffer
            .allocation_snapshot(before.id)
            .unwrap()
            .bytes(),
        before.bytes()
    );
    drop(pin);
    tx.rollback().unwrap();
    drop(tx);
    assert_eq!(
        storage
            .buffer
            .allocation_snapshot(active_before.id)
            .unwrap()
            .bytes(),
        active_before.bytes()
    );
    let mut tx = storage.begin_transaction().unwrap();
    let after = transition_publish(&mut storage, &mut tx, &before, active.id);
    storage
        .transactions
        .wal()
        .borrow_mut()
        .inject_flush_failure();
    assert!(tx.commit().is_err());
    assert_eq!(tx.state(), crate::TransactionState::CommitPending);
    assert!(tx.rollback().is_err());
    assert!(
        storage
            .claim_reusable_btree_page(&mut tx, active.id)
            .is_err()
    );
    tx.commit().unwrap();
    drop(tx);
    assert_eq!(
        storage
            .buffer
            .allocation_snapshot(before.id)
            .unwrap()
            .bytes(),
        after.bytes()
    );
    storage.close().unwrap();
    let mut storage = HeapStorage::open(&path, indexed_table()).unwrap();
    storage.inspect_index_reclaim().unwrap();
    storage.close().unwrap();
    cleanup(&path);
}

#[test]
fn transition_cached_claim_rejects_changed_disk_identity() {
    let (path, mut storage, _, active) = transition_fixture("round13-revalidation", 8);
    let mut tx = storage.begin_transaction().unwrap();
    storage
        .claim_reusable_btree_page(&mut tx, active.id)
        .unwrap()
        .unwrap();
    // The remaining candidate was fully scanned and cached; change only its
    // physical incarnation, with a valid CRC, to exercise local revalidation.
    let id = PageId(4);
    let mut disk = PageManager::open(&path).unwrap();
    let before = disk.read_page(id).unwrap();
    let payload = netbadb_index::encode_leaf_generation(
        &netbadb_index::IndexSpec {
            data_type: netbadb_types::SemanticType::physical(PhysicalType::UInt64),
            nullable: true,
        },
        &netbadb_index::LeafNode::empty(),
        Some(IndexId(1)),
        Some(PageGeneration(
            before.allocation_generation().unwrap().unwrap().0 + 1,
        )),
    )
    .unwrap();
    let mut changed = Page::new(id, PageType::BTreeLeaf);
    changed
        .initialize_single_payload(PageType::BTreeLeaf, &payload)
        .unwrap();
    disk.write_page(&changed).unwrap();
    disk.sync().unwrap();
    assert!(
        storage
            .claim_reusable_btree_page(&mut tx, active.id)
            .is_err()
    );
    disk.write_page(&before).unwrap();
    disk.sync().unwrap();
    drop(disk);
    tx.rollback().unwrap();
    drop(tx);
    storage.close().unwrap();
    cleanup(&path);
}
