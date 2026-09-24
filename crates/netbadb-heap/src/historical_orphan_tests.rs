// Included in heap::tests::maintenance: no production fixture/repair API.
fn adoption_fixture(
    case: &str,
    capacity: usize,
) -> (std::path::PathBuf, HeapStorage, crate::IndexDefinition) {
    let (path, mut storage, index) = retirement_fixture(case, capacity);
    for (row, _) in storage.scan().unwrap() {
        storage.delete(row).unwrap();
    }
    historical_unmarked_vacuum(&mut storage);
    let report = storage.inspect_index_reclaim().unwrap();
    assert_eq!(
        (
            report.owned_pages,
            report.active_orphan_pages,
            report.retired_marker_pages
        ),
        (83, 81, 0)
    );
    assert!(
        storage
            .inspect_reusable_pages()
            .unwrap()
            .candidates
            .is_empty()
    );
    (path, storage, index)
}

fn adoption_assert_historical(storage: &mut HeapStorage, index: &crate::IndexDefinition) {
    let report = storage.inspect_index_reclaim().unwrap();
    assert_eq!(
        (report.active_orphan_pages, report.retired_marker_pages),
        (81, 0)
    );
    assert_eq!(storage.btree().height(index.handle).unwrap(), 1);
    assert!(storage.scan().unwrap().is_empty());
    assert!(
        storage
            .btree()
            .lookup(index.handle, &retirement_values(0)[2])
            .unwrap()
            .is_empty()
    );
}

#[test]
fn adoption_81_identity_idempotency_and_immediate_same_owner_reuse() {
    for capacity in [1, 8] {
        let (path, mut storage, index) =
            adoption_fixture(&format!("round15-81-{capacity}"), capacity);
        let before = retirement_images(&mut storage);
        let pages = storage.buffer.page_count();
        let generation = storage.wal_generation().unwrap();
        let report = storage.adopt_historical_btree_orphans().unwrap();
        assert_eq!(
            (
                report.indexes_scanned,
                report.candidates,
                report.adopted,
                report.historical_remaining
            ),
            (1, 81, 81, 0)
        );
        assert_eq!(report.already_retired_markers, 0);
        assert_eq!(storage.wal_generation().unwrap(), generation + 1);
        assert_eq!(storage.buffer.page_count(), pages);
        assert_eq!(storage.indexes(), std::slice::from_ref(&index));
        retirement_assert_small(&mut storage, &index);
        let candidates = storage.inspect_reusable_pages().unwrap().candidates;
        assert_eq!(candidates.len(), 81);
        for old in &before {
            let new = storage.buffer.allocation_snapshot(old.id).unwrap();
            assert_eq!(
                crate::allocation_transition::identity(old).unwrap(),
                crate::allocation_transition::identity(&new).unwrap()
            );
            if candidates.iter().any(|p| p.page_ref.page_id == old.id) {
                assert!(crate::allocation_transition::is_retired(&new).unwrap());
            } else {
                assert_eq!(new.bytes(), old.bytes(), "meta/root remain byte-identical");
            }
        }
        let records = storage.wal_records().unwrap();
        let updates: Vec<_> = records
            .iter()
            .filter_map(|record| match record.kind {
                crate::WalRecordKind::PageUpdate { page_id, .. } => Some(page_id),
                _ => None,
            })
            .collect();
        assert_eq!(
            updates,
            candidates
                .iter()
                .map(|c| c.page_ref.page_id)
                .collect::<Vec<_>>()
        );
        assert_eq!(records.len(), 83); // Begin + 81 PageUpdates + Commit; no reservations
        assert!(records.iter().all(|r| !matches!(
            r.kind,
            crate::WalRecordKind::PageAllocationTransition { .. }
                | crate::WalRecordKind::PageGenerationReservation
        )));
        let wal_bytes = std::fs::read(storage.current_wal_path().unwrap()).unwrap();
        for _ in 0..2 {
            assert_eq!(storage.adopt_historical_btree_orphans().unwrap().adopted, 0);
            assert_eq!(storage.wal_generation().unwrap(), generation + 1);
            assert_eq!(
                std::fs::read(storage.current_wal_path().unwrap()).unwrap(),
                wal_bytes
            );
        }
        // No caller flush or checkpoint: success makes committed holes usable.
        let mut tx = storage.begin_transaction().unwrap();
        retirement_grow(&mut storage, &mut tx);
        tx.commit().unwrap();
        drop(tx);
        retirement_assert_rows(&mut storage, &index);
        assert_eq!(storage.buffer.page_count(), pages);
        assert!(
            storage
                .inspect_reusable_pages()
                .unwrap()
                .candidates
                .is_empty()
        );
        for candidate in candidates {
            let new = storage
                .buffer
                .allocation_snapshot(candidate.page_ref.page_id)
                .unwrap();
            let (reference, owner) = crate::allocation_transition::identity(&new).unwrap();
            assert_eq!(owner, index.id);
            assert!(reference.generation > candidate.page_ref.generation);
            assert!(
                storage
                    .buffer
                    .read_btree_page(netbadb_index::BTreePageRef::Allocated(candidate.page_ref))
                    .is_err()
            );
        }
        let records = storage.wal_records().unwrap();
        assert_eq!(
            records
                .iter()
                .filter(|r| matches!(
                    r.kind,
                    crate::WalRecordKind::PageAllocationTransition { .. }
                ))
                .count(),
            81
        );
        println!(
            "ROUND15 historical capacity={capacity}: reachable 2->2 historical 81->0 markers 0->81; consumption=81 transitions=81 BTree_appends=0 file={pages}->{pages}"
        );
        storage.close().unwrap();
        for _ in 0..3 {
            let mut storage = HeapStorage::open(&path, indexed_table()).unwrap();
            retirement_assert_rows(&mut storage, &index);
            storage.close().unwrap();
        }
        cleanup(&path);
    }
}

#[test]
fn adoption_rollback_exact_and_uncommitted_marker_cache_exclusion() {
    for capacity in [1, 8] {
        let (path, mut storage, index) =
            adoption_fixture(&format!("round15-undo-{capacity}"), capacity);
        storage.checkpoint().unwrap();
        let before = retirement_images(&mut storage);
        let plan = storage.historical_adoption_plan().unwrap();
        let mut tx = storage.begin_transaction().unwrap();
        tx.acquire_writer().unwrap();
        storage.apply_historical_adoption(&mut tx, &plan).unwrap();
        assert_eq!(tx.retired_btree_pages.len(), 81);
        storage.buffer.flush_all().unwrap(); // all uncommitted marker bytes stolen
        storage.reusable_btree_pages = None;
        assert!(
            storage
                .claim_reusable_btree_page(&mut tx, index.id)
                .unwrap()
                .is_none()
        );
        assert!(storage.adopt_historical_btree_orphans().is_err());
        tx.rollback().unwrap();
        drop(tx);
        for old in &before {
            assert_eq!(
                storage.buffer.allocation_snapshot(old.id).unwrap().bytes(),
                old.bytes(),
                "exact owner/generation/pageLSN/payload/CRC undo"
            );
        }
        adoption_assert_historical(&mut storage, &index);
        storage.close().unwrap();
        for _ in 0..3 {
            let mut storage = HeapStorage::open(&path, indexed_table()).unwrap();
            adoption_assert_historical(&mut storage, &index);
            for old in &before {
                assert_eq!(
                    storage.buffer.allocation_snapshot(old.id).unwrap().bytes(),
                    old.bytes()
                );
            }
            storage.close().unwrap();
        }
        cleanup(&path);
    }
}

#[test]
fn adoption_all_candidate_pin_dirty_and_identity_preconditions() {
    let (path, mut storage, index) = adoption_fixture("round15-preconditions", 256);
    let ownership = storage.inspect_index_reclaim().unwrap();
    let last = ownership
        .allocations
        .iter()
        .rfind(|p| p.reachable == Some(false))
        .unwrap()
        .page_ref;
    let pin = storage
        .buffer
        .read_btree_page(netbadb_index::BTreePageRef::Allocated(last))
        .unwrap();
    let records = storage.wal_records().unwrap().len();
    assert!(matches!(
        storage.adopt_historical_btree_orphans(),
        Err(StorageError::Buffer(crate::BufferError::PagePinned { .. }))
    ));
    assert_eq!(storage.wal_records().unwrap().len(), records);
    drop(pin);
    storage.checkpoint().unwrap();
    let before = retirement_images(&mut storage);
    storage.begin_transaction().unwrap().commit().unwrap();
    storage
        .buffer
        .write_btree_page(netbadb_index::BTreePageRef::Allocated(last))
        .unwrap()
        .page_mut();
    assert!(matches!(
        storage.historical_adoption_plan(),
        Err(StorageError::Buffer(crate::BufferError::PageDirty { .. }))
    ));
    storage.flush().unwrap();
    let plan = storage.historical_adoption_plan().unwrap();
    let mut tx = storage.begin_transaction().unwrap();
    let begin_records = storage.wal_records().unwrap().len();
    // The LAST candidate blocks every adoption, not just its own rewrite.
    storage
        .buffer
        .write_btree_page(netbadb_index::BTreePageRef::Allocated(last))
        .unwrap()
        .page_mut();
    assert!(matches!(
        storage.apply_historical_adoption(&mut tx, &plan),
        Err(StorageError::Buffer(crate::BufferError::PageDirty { .. }))
    ));
    assert_eq!(storage.wal_records().unwrap().len(), begin_records);
    assert!(tx.retired_btree_pages.is_empty());
    tx.rollback().unwrap();
    drop(tx);
    storage.flush().unwrap();
    let mut tx = storage.begin_transaction().unwrap();
    // A clean, valid but byte-changed page is not the proved image.
    let mut changed = storage.buffer.allocation_snapshot(last.page_id).unwrap();
    let original = changed.clone();
    changed.set_page_lsn(tx.last_lsn());
    *storage.buffer.write_page(last.page_id).unwrap().page_mut() = changed;
    storage.flush().unwrap();
    assert!(storage.apply_historical_adoption(&mut tx, &plan).is_err());
    assert!(tx.retired_btree_pages.is_empty());
    *storage.buffer.write_page(last.page_id).unwrap().page_mut() = original;
    tx.rollback().unwrap();
    drop(tx);
    storage.flush().unwrap();
    for old in before {
        assert_eq!(
            storage.buffer.allocation_snapshot(old.id).unwrap().bytes(),
            old.bytes()
        );
    }
    adoption_assert_historical(&mut storage, &index);
    storage.close().unwrap();
    cleanup(&path);
}

#[test]
fn adoption_large_real_fixture_single_transaction_wal_bounds() {
    let path = test_path("round15-large");
    cleanup(&path);
    let mut storage = HeapStorage::create_with_buffer_pool_size(&path, indexed_table(), 8).unwrap();
    let mut tx = storage.begin_transaction().unwrap();
    for id in 0..1200 {
        storage.insert_in(&mut tx, &retirement_values(id)).unwrap();
    }
    tx.commit().unwrap();
    drop(tx);
    storage.create_index(ColumnId(3)).unwrap();
    let mut tx = storage.begin_transaction().unwrap();
    for (row, _) in storage.scan().unwrap() {
        storage.delete_in(&mut tx, row).unwrap();
    }
    tx.commit().unwrap();
    drop(tx);
    historical_unmarked_vacuum(&mut storage);
    let before = storage.inspect_index_reclaim().unwrap();
    assert!(before.active_orphan_pages >= 1000);
    let report = storage.adopt_historical_btree_orphans().unwrap();
    assert_eq!(report.adopted, before.active_orphan_pages);
    let records = storage.wal_records().unwrap();
    assert_eq!(records.len() as u64, report.adopted + 2);
    assert_eq!(
        records
            .iter()
            .map(|r| r.txn_id)
            .collect::<std::collections::HashSet<_>>()
            .len(),
        1
    );
    let wal_bytes = std::fs::metadata(storage.current_wal_path().unwrap())
        .unwrap()
        .len();
    assert!(
        wal_bytes
            <= crate::wal::WAL_HEADER_SIZE as u64
                + (report.adopted + 2) * crate::wal::WAL_MAX_RECORD_SIZE as u64
    );
    assert_eq!(storage.buffer.page_count(), before.database_pages);
    assert_eq!(
        storage.inspect_reusable_pages().unwrap().candidates.len() as u64,
        report.adopted
    );
    println!(
        "ROUND15 large: rows=1200 candidates={} adopted={} WAL_bytes={wal_bytes} transactions=1 file_pages={}",
        report.candidates, report.adopted, before.database_pages
    );
    storage.close().unwrap();
    for _ in 0..3 {
        let mut storage = HeapStorage::open(&path, indexed_table()).unwrap();
        let after = storage.inspect_index_reclaim().unwrap();
        assert_eq!(
            (after.active_orphan_pages, after.retired_marker_pages),
            (0, report.adopted)
        );
        storage.close().unwrap();
    }
    cleanup(&path);
}

#[test]
fn adoption_crash_child() {
    if std::env::var_os(crate::crash_test::CHILD_ENV).is_none() {
        return;
    }
    let path =
        std::path::PathBuf::from(std::env::var_os(crate::crash_test::DATABASE_PATH_ENV).unwrap());
    let case = std::env::var_os(crate::crash_test::CASE_ENV).unwrap();
    let mut storage = HeapStorage::open_with_buffer_pool_size(
        &path,
        indexed_table(),
        if case == "partial" { 8 } else { 512 },
    )
    .unwrap();
    if case == "reuse" {
        let mut tx = storage.begin_transaction().unwrap();
        retirement_grow(&mut storage, &mut tx);
        tx.commit().unwrap();
    } else if case == "different-reuse" {
        storage.create_index(ColumnId(1)).unwrap();
    } else {
        storage.adopt_historical_btree_orphans().unwrap();
    }
    panic!("adoption crash hook not reached");
}

fn adoption_run_child(path: &std::path::Path, case: &str, point: TestCrashPoint) {
    let mut command = std::process::Command::new(std::env::current_exe().unwrap());
    command.args([
        "--exact",
        "heap::tests::maintenance::adoption_crash_child",
        "--nocapture",
    ]);
    crate::crash_test::configure_child(&mut command, case, path, point);
    let output = command.output().unwrap();
    assert_eq!(
        output.status.code(),
        Some(crate::crash_test::EXIT_CODE),
        "{case}/{point:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn adoption_process_crash_matrix_three_reopens() {
    for (case, point) in [
        ("normal", TestCrashPoint::AdoptionBeforeCheckpoint),
        ("normal", TestCrashPoint::AdoptionAfterCheckpoint),
        ("normal", TestCrashPoint::RetirementAfterLog),
        ("normal", TestCrashPoint::RetirementAfterPublish),
        ("normal", TestCrashPoint::AdoptionAfterManyPages),
        ("partial", TestCrashPoint::AdoptionAfterManyPages),
        ("normal", TestCrashPoint::RetirementBeforeCommit),
        ("normal", TestCrashPoint::CommitAfterWalSync),
        ("partial", TestCrashPoint::CommitAfterWalSync),
        ("normal", TestCrashPoint::AdoptionAfterCommit),
    ] {
        let (path, mut storage, index) =
            adoption_fixture(&format!("round15-crash-{case}-{}", point.as_str()), 8);
        let before = retirement_images(&mut storage);
        storage.close().unwrap();
        adoption_run_child(&path, case, point);
        let winner = matches!(
            point,
            TestCrashPoint::CommitAfterWalSync | TestCrashPoint::AdoptionAfterCommit
        );
        let mut disk = PageManager::open(&path).unwrap();
        let changed = before
            .iter()
            .filter(|p| disk.read_page(p.id).unwrap().bytes() != p.bytes())
            .count();
        if case == "normal" {
            assert_eq!(
                changed, 0,
                "NO-FORCE winner/loser must exercise WAL recovery"
            );
        }
        if case == "partial" {
            assert!(
                changed > 0 && changed < 81,
                "must really partially flush markers: {changed}"
            );
        }
        drop(disk);
        for _ in 0..3 {
            let mut storage = HeapStorage::open(&path, indexed_table()).unwrap();
            if winner {
                retirement_assert_small(&mut storage, &index);
                for old in &before {
                    let new = storage.buffer.allocation_snapshot(old.id).unwrap();
                    assert_eq!(
                        crate::allocation_transition::identity(old).unwrap(),
                        crate::allocation_transition::identity(&new).unwrap()
                    );
                }
            } else {
                adoption_assert_historical(&mut storage, &index);
                for old in &before {
                    assert_eq!(
                        storage.buffer.allocation_snapshot(old.id).unwrap().bytes(),
                        old.bytes()
                    );
                }
            }
            storage.close().unwrap();
        }
        println!("ROUND15 crash {case}/{point:?}: winner={winner} flushed={changed} reopen=3");
        cleanup(&path);
    }
}

#[test]
fn adoption_then_same_and_different_owner_reuse_crash() {
    for case in ["reuse", "different-reuse"] {
        let (path, mut storage, index) = adoption_fixture(&format!("round15-crash-{case}"), 8);
        storage.adopt_historical_btree_orphans().unwrap();
        let markers = storage.inspect_reusable_pages().unwrap().candidates;
        let before = retirement_images(&mut storage);
        let count = storage.buffer.page_count();
        storage.close().unwrap(); // retain adoption WAL; do not checkpoint
        adoption_run_child(&path, case, TestCrashPoint::CommitAfterWalSync);
        let mut disk = PageManager::open(&path).unwrap();
        for old in &before {
            assert_eq!(disk.read_page(old.id).unwrap().bytes(), old.bytes());
        }
        drop(disk);
        for _ in 0..3 {
            let mut storage = HeapStorage::open(&path, indexed_table()).unwrap();
            if case == "reuse" {
                retirement_assert_rows(&mut storage, &index);
            }
            let report = storage.inspect_index_reclaim().unwrap();
            let reused = if case == "reuse" { 81 } else { 2 };
            assert_eq!(report.retired_marker_pages, 81 - reused);
            assert_eq!(report.active_orphan_pages, 0);
            assert_eq!(storage.buffer.page_count(), count);
            for marker in markers.iter().take(reused as usize) {
                assert!(
                    storage
                        .buffer
                        .read_btree_page(netbadb_index::BTreePageRef::Allocated(marker.page_ref))
                        .is_err()
                );
                let new = storage
                    .buffer
                    .allocation_snapshot(marker.page_ref.page_id)
                    .unwrap();
                let (reference, owner) = crate::allocation_transition::identity(&new).unwrap();
                assert!(reference.generation > marker.page_ref.generation);
                assert_eq!(owner == index.id, case == "reuse");
            }
            storage.close().unwrap();
        }
        println!(
            "ROUND15 adoption->{case} durable transition winner: reopen=3 file={count}->{count}"
        );
        cleanup(&path);
    }
}

#[test]
fn adoption_quiescent_gate_pending_failures_and_retry_reopen() {
    let (path, mut storage, index) = adoption_fixture("round15-gates", 128);
    let reader = storage.begin_transaction().unwrap();
    assert!(matches!(
        storage.adopt_historical_btree_orphans(),
        Err(StorageError::Checkpoint(
            CheckpointError::OutstandingTransactions { .. }
        ))
    ));
    drop(reader);
    for rollback in [false, true] {
        let mut tx = storage.begin_transaction().unwrap();
        tx.acquire_writer().unwrap();
        assert!(matches!(
            storage.adopt_historical_btree_orphans(),
            Err(StorageError::Checkpoint(
                CheckpointError::WriterActive { .. }
            ))
        ));
        storage
            .transactions
            .wal()
            .borrow_mut()
            .inject_flush_failure();
        assert!(if rollback { tx.rollback() } else { tx.commit() }.is_err());
        assert!(matches!(
            storage.adopt_historical_btree_orphans(),
            Err(StorageError::Checkpoint(
                CheckpointError::WriterActive { .. }
            ))
        ));
        if rollback {
            tx.rollback().unwrap();
        } else {
            tx.commit().unwrap();
        }
        drop(tx);
    }
    // A durable tail intent is never erased/ignored by adoption, even if its
    // runtime retry flag were absent (construct only in this offline test).
    let retired = storage.create_index(ColumnId(1)).unwrap();
    storage.drop_index(retired.id).unwrap();
    let original = storage.buffer.allocation_snapshot(PageId(1)).unwrap();
    let mut catalog =
        decode_index_catalog(original.single_payload(PageType::IndexCatalog).unwrap()).unwrap();
    catalog.reclaim_intent = Some(netbadb_index::TailReclaimIntent {
        old_page_count: storage.buffer.page_count(),
        truncate_from: retired.handle.meta_page.page_id().0,
        checkpoint_lsn: storage.transactions.wal().borrow().next_lsn(),
        covered: vec![netbadb_index::RetiredIndexOwnership {
            index_id: retired.id,
            meta_page: Some(retired.handle.meta_page),
        }],
    });
    {
        let payload = encode_index_catalog(&catalog).unwrap();
        let mut changed = original.clone();
        changed
            .replace_single_payload(PageType::IndexCatalog, &payload)
            .unwrap();
        *storage.buffer.write_page(PageId(1)).unwrap().page_mut() = changed;
        assert!(storage.adopt_historical_btree_orphans().is_err());
        *storage.buffer.write_page(PageId(1)).unwrap().page_mut() = original;
    }
    storage.flush().unwrap();
    // Failed generation creation cannot authorize even one marker log.
    storage.inject_partial_checkpoint_rotation(20).unwrap();
    assert!(storage.adopt_historical_btree_orphans().is_err());
    assert!(matches!(
        storage.adopt_historical_btree_orphans(),
        Err(StorageError::Checkpoint(CheckpointError::RecoveryRequired))
    ));
    storage.simulate_crash();
    let mut storage = HeapStorage::open(&path, indexed_table()).unwrap();
    adoption_assert_historical(&mut storage, &index);
    assert_eq!(
        storage.adopt_historical_btree_orphans().unwrap().adopted,
        81
    );
    storage.close().unwrap();
    cleanup(&path);
}

#[test]
fn adoption_corruption_global_proof_precedes_every_marker_write() {
    let (path, mut storage, index) = adoption_fixture("round15-corrupt", 256);
    let other = storage.create_index(ColumnId(1)).unwrap();
    let before = retirement_images(&mut storage);
    let inventory = storage.inspect_index_reclaim().unwrap();
    let orphan = inventory
        .allocations
        .iter()
        .rfind(|p| p.owner == index.id && p.reachable == Some(false))
        .unwrap()
        .page_ref;
    let meta = storage.btree().read_meta(index.handle).unwrap();
    for case in [
        "crc",
        "payload",
        "zero-generation",
        "future-generation",
        "owner",
        "root-generation",
        "root-owner",
        "leaf-extra",
        "root-marker",
        "leaf-marker",
    ] {
        let target = if case.starts_with("root-") {
            index.handle.meta_page.page_id()
        } else if case.starts_with("leaf-") {
            meta.root_page.page_id()
        } else {
            orphan.page_id
        };
        let original = storage.buffer.allocation_snapshot(target).unwrap();
        let mut bad = original.clone();
        let kind = bad.header().unwrap().page_type;
        if case == "crc" {
            let mut bytes = *bad.bytes();
            bytes[crate::PAGE_SIZE - 1] ^= 1;
            bad = Page::from_bytes(target, bytes);
        } else if case.starts_with("root-") {
            let mut bad_meta = meta.clone();
            bad_meta.root_page = if case == "root-owner" {
                storage.btree().read_meta(other.handle).unwrap().root_page
            } else if case == "root-generation" {
                netbadb_index::BTreePageRef::Allocated(netbadb_types::PageRef {
                    page_id: meta.root_page.page_id(),
                    generation: PageGeneration(u64::MAX),
                })
            } else {
                netbadb_index::BTreePageRef::Allocated(orphan)
            };
            bad.replace_single_payload(kind, &netbadb_index::encode_meta(&bad_meta).unwrap())
                .unwrap();
        } else if case.starts_with("leaf-") {
            let mut leaf = netbadb_index::decode_leaf_owned(
                &meta.spec,
                bad.single_payload(kind).unwrap(),
                meta.owner,
            )
            .unwrap();
            // Extra leaf/marker outside the tree-child traversal is forbidden.
            let leaf_ref = inventory
                .allocations
                .iter()
                .find(|p| {
                    p.reachable == Some(false)
                        && storage
                            .buffer
                            .allocation_snapshot(p.page_ref.page_id)
                            .unwrap()
                            .header()
                            .unwrap()
                            .page_type
                            == PageType::BTreeLeaf
                })
                .unwrap()
                .page_ref;
            leaf.next_leaf = Some(netbadb_index::BTreePageRef::Allocated(leaf_ref));
            bad.replace_single_payload(
                kind,
                &netbadb_index::encode_leaf_generation(
                    &meta.spec,
                    &leaf,
                    meta.owner,
                    meta.root_page.generation(),
                )
                .unwrap(),
            )
            .unwrap();
        } else {
            let mut payload = bad.single_payload(kind).unwrap().to_vec();
            match case {
                "payload" => payload.push(0),
                "zero-generation" => payload[16..24].fill(0),
                "future-generation" => payload[16..24].copy_from_slice(&u64::MAX.to_le_bytes()),
                "owner" => payload[8..16].copy_from_slice(&999_u64.to_le_bytes()),
                _ => unreachable!(),
            }
            bad.replace_single_payload(kind, &payload).unwrap();
        }
        if case.ends_with("marker") {
            let reference = if case == "root-marker" {
                orphan
            } else {
                let next = netbadb_index::decode_leaf_owned(
                    &meta.spec,
                    bad.single_payload(kind).unwrap(),
                    meta.owner,
                )
                .unwrap()
                .next_leaf
                .unwrap();
                netbadb_types::PageRef {
                    page_id: next.page_id(),
                    generation: next.generation().unwrap(),
                }
            };
            let mut marker = storage
                .buffer
                .allocation_snapshot(reference.page_id)
                .unwrap();
            let marker_kind = marker.header().unwrap().page_type;
            marker
                .replace_single_payload(
                    marker_kind,
                    &netbadb_index::encode_retired_btree(netbadb_index::RetiredBTreePage {
                        owner: index.id,
                        page_ref: reference,
                    })
                    .unwrap(),
                )
                .unwrap();
            *storage
                .buffer
                .write_page(reference.page_id)
                .unwrap()
                .page_mut() = marker;
        }
        *storage.buffer.write_page(target).unwrap().page_mut() = bad;
        let wal = storage.wal_records().unwrap().len();
        let generation = storage.wal_generation().unwrap();
        assert!(storage.adopt_historical_btree_orphans().is_err(), "{case}");
        assert_eq!(
            storage.wal_records().unwrap().len(),
            wal,
            "{case}: no adoption writes"
        );
        assert_eq!(
            storage.wal_generation().unwrap(),
            generation,
            "{case}: preflight rejects before checkpoint"
        );
        for original in &before {
            *storage.buffer.write_page(original.id).unwrap().page_mut() = original.clone();
        }
    }
    storage.close().unwrap();
    cleanup(&path);
}

#[test]
fn adoption_checkpoint_horizon_ignores_retained_structural_wal() {
    let (path, mut storage, index) = retirement_fixture("round15-horizon", 128);
    let old_meta = storage
        .buffer
        .allocation_snapshot(index.handle.meta_page.page_id())
        .unwrap();
    for (row, _) in storage.scan().unwrap() {
        storage.delete(row).unwrap();
    }
    historical_unmarked_vacuum(&mut storage);
    // Preserve real former-root structural bytes in a valid committed chain.
    // The final image restores today's root; both updates must stay behind the
    // maintenance horizon, even if the superseded generation reappears on disk.
    let current = storage.buffer.allocation_snapshot(old_meta.id).unwrap();
    let mut tx = storage.begin_transaction().unwrap();
    let mut earlier = old_meta;
    tx.log_page_update(&current, &mut earlier).unwrap();
    let mut restored = current.clone();
    tx.log_page_update(&earlier, &mut restored).unwrap();
    *storage.buffer.write_page(current.id).unwrap().page_mut() = restored;
    tx.commit().unwrap();
    drop(tx);
    let old_path = storage.current_wal_path().unwrap();
    let old_wal = std::fs::read(&old_path).unwrap();
    let old_end = storage.transactions.wal().borrow().next_lsn();
    let old_generation = storage.wal_generation().unwrap();
    assert_eq!(
        storage.adopt_historical_btree_orphans().unwrap().adopted,
        81
    );
    assert!(!old_path.exists());
    assert_eq!(storage.transactions.wal().borrow().base_lsn(), old_end);
    assert_eq!(storage.wal_generation().unwrap(), old_generation + 1);
    let current_path = storage.current_wal_path().unwrap();
    storage.close().unwrap();
    for _ in 0..3 {
        std::fs::write(&old_path, &old_wal).unwrap(); // deliberate stale generation retention
        let mut storage = HeapStorage::open(&path, indexed_table()).unwrap();
        assert_eq!(storage.current_wal_path().unwrap(), current_path);
        assert!(
            storage
                .wal_records()
                .unwrap()
                .iter()
                .all(|r| r.lsn >= old_end)
        );
        assert!(!old_path.exists());
        retirement_assert_small(&mut storage, &index);
        assert_eq!(storage.adopt_historical_btree_orphans().unwrap().adopted, 0);
        storage.close().unwrap();
    }
    println!(
        "ROUND15 horizon: retained structural generation excluded on 3 reopens, base={}",
        old_end.0
    );
    cleanup(&path);
}

#[test]
fn adoption_mixed_markers_multiple_active_indexes_and_different_owner_consumption() {
    let (path, mut storage, index) = adoption_fixture("round15-mixed", 256);
    let other = storage.create_index(ColumnId(1)).unwrap();
    let mut entries = Vec::new();
    let mut tx = storage.begin_transaction().unwrap();
    for n in 0..300 {
        let row = netbadb_types::RowId {
            page: PageId(2),
            slot: n,
            generation: 1,
        };
        storage
            .btree()
            .insert_in(
                &mut tx,
                other.handle,
                ScalarValue::UInt64(u64::from(n)),
                row,
            )
            .unwrap();
        entries.push((n, row));
    }
    tx.commit().unwrap();
    drop(tx);
    let mut tx = storage.begin_transaction().unwrap();
    for (n, row) in entries {
        storage
            .btree()
            .delete_in(
                &mut tx,
                other.handle,
                ScalarValue::UInt64(u64::from(n)),
                row,
            )
            .unwrap();
    }
    tx.commit().unwrap();
    drop(tx);
    storage.flush().unwrap();
    let old_markers: Vec<_> = storage
        .inspect_reusable_pages()
        .unwrap()
        .candidates
        .into_iter()
        .map(|p| {
            storage
                .buffer
                .allocation_snapshot(p.page_ref.page_id)
                .unwrap()
        })
        .collect();
    assert!(!old_markers.is_empty());
    let report = storage.adopt_historical_btree_orphans().unwrap();
    assert_eq!(
        (
            report.indexes_scanned,
            report.adopted,
            report.already_retired_markers
        ),
        (2, 81, old_markers.len() as u64)
    );
    for old in &old_markers {
        assert_eq!(
            storage.buffer.allocation_snapshot(old.id).unwrap().bytes(),
            old.bytes()
        );
    }
    let all = storage.inspect_reusable_pages().unwrap().candidates;
    assert_eq!(all.len(), 81 + old_markers.len());
    assert_eq!(
        all.iter()
            .map(|p| p.page_ref.page_id)
            .collect::<std::collections::HashSet<_>>()
            .len(),
        all.len()
    );
    // DROP uses whole-owner authority for the two remaining ordinary pages;
    // independent historical and future markers must not be counted twice.
    storage.drop_index(index.id).unwrap();
    storage.compact_index_catalog().unwrap();
    let before = storage.inspect_index_reclaim().unwrap();
    assert_eq!(storage.adopt_historical_btree_orphans().unwrap().adopted, 0);
    assert_eq!(storage.inspect_index_reclaim().unwrap(), before);
    let pages = storage.buffer.page_count();
    let replacement = storage.create_index(ColumnId(3)).unwrap();
    let mut tx = storage.begin_transaction().unwrap();
    retirement_grow(&mut storage, &mut tx);
    tx.commit().unwrap();
    drop(tx);
    retirement_assert_rows(&mut storage, &replacement);
    assert_eq!(storage.buffer.page_count(), pages);
    for old in all.iter().take(81) {
        let new = storage
            .buffer
            .allocation_snapshot(old.page_ref.page_id)
            .unwrap();
        let (reference, owner) = crate::allocation_transition::identity(&new).unwrap();
        assert_eq!(owner, replacement.id);
        assert!(reference.generation > old.page_ref.generation);
    }
    storage.compact_index_catalog().unwrap();
    assert!(storage.inspect_index_reclaim().unwrap().pending.is_empty());
    println!(
        "ROUND15 mixed: historical=81 future_markers={} different_owner_consumed=81 file={pages}->{pages}",
        old_markers.len()
    );
    storage.close().unwrap();
    cleanup(&path);
}

#[test]
fn adoption_api_rolls_back_partial_publication_and_poisoned_commit_requires_reopen() {
    let (path, mut storage, index) = adoption_fixture("round15-api-failure", 8);
    let before = retirement_images(&mut storage);
    storage.fail_adoption = Some("after40");
    assert!(storage.adopt_historical_btree_orphans().is_err());
    for old in before {
        assert_eq!(
            storage.buffer.allocation_snapshot(old.id).unwrap().bytes(),
            old.bytes()
        );
    }
    adoption_assert_historical(&mut storage, &index);
    storage.fail_adoption = Some("commit-sync");
    assert!(storage.adopt_historical_btree_orphans().is_err());
    assert!(matches!(
        storage.adopt_historical_btree_orphans(),
        Err(StorageError::Checkpoint(CheckpointError::RecoveryRequired))
    ));
    assert!(storage.begin_transaction().is_err());
    storage.simulate_crash();
    for _ in 0..3 {
        let mut storage = HeapStorage::open(&path, indexed_table()).unwrap();
        // Process-loss model: appended complete COMMIT remains readable despite
        // the injected sync error. API correctly did NOT promise rollback.
        retirement_assert_small(&mut storage, &index);
        storage.close().unwrap();
    }
    cleanup(&path);
}

#[test]
fn adoption_v1_raw_and_historical_abandoned_nodes_are_noops() {
    let (path, storage, _) = adoption_fixture("round15-legacy-noop", 256);
    storage.close().unwrap();
    legacy_catalog(&path, 5);
    let mut storage = HeapStorage::open(&path, indexed_table()).unwrap();
    let spec = IndexSpec {
        data_type: netbadb_types::SemanticType::physical(PhysicalType::UInt64),
        nullable: true,
    };
    storage.btree().create(spec).unwrap();
    storage.checkpoint().unwrap();
    let before = storage.inspect_index_reclaim().unwrap();
    assert_eq!(before.unowned_legacy_pages, 81);
    assert_eq!(before.unregistered_legacy_pages, 2);
    let wal = std::fs::read(storage.current_wal_path().unwrap()).unwrap();
    let heap = std::fs::read(&path).unwrap();
    assert_eq!(storage.adopt_historical_btree_orphans().unwrap().adopted, 0);
    assert_eq!(storage.inspect_index_reclaim().unwrap(), before);
    assert_eq!(
        std::fs::read(storage.current_wal_path().unwrap()).unwrap(),
        wal
    );
    assert_eq!(std::fs::read(&path).unwrap(), heap);
    storage.close().unwrap();
    cleanup(&path);
}

#[test]
fn adoption_internal_marker_duplicate_children_and_leaf_cross_validation() {
    let (path, mut storage, _) = adoption_fixture("round15-live-corruption", 256);
    let other = storage.create_index(ColumnId(1)).unwrap();
    let mut tx = storage.begin_transaction().unwrap();
    for n in 0..300_u16 {
        storage
            .btree()
            .insert_in(
                &mut tx,
                other.handle,
                ScalarValue::UInt64(u64::from(n)),
                netbadb_types::RowId {
                    page: PageId(2),
                    slot: n,
                    generation: 1,
                },
            )
            .unwrap();
    }
    tx.commit().unwrap();
    drop(tx);
    let meta = storage.btree().read_meta(other.handle).unwrap();
    assert_eq!(meta.height, 2);
    let root = storage
        .buffer
        .allocation_snapshot(meta.root_page.page_id())
        .unwrap();
    let internal = netbadb_index::decode_internal_owned(
        &meta.spec,
        root.single_payload(PageType::BTreeInternal).unwrap(),
        meta.owner,
    )
    .unwrap();
    let leaf = storage
        .buffer
        .allocation_snapshot(internal.first_child.page_id())
        .unwrap();
    let originals = retirement_images(&mut storage);
    for case in [
        "internal-marker",
        "duplicate-child",
        "cycle",
        "leaf-missing",
        "leaf-order",
    ] {
        if case == "internal-marker" {
            let mut changed = leaf.clone();
            changed
                .replace_single_payload(
                    PageType::BTreeLeaf,
                    &netbadb_index::encode_retired_btree(netbadb_index::RetiredBTreePage {
                        owner: other.id,
                        page_ref: netbadb_types::PageRef {
                            page_id: leaf.id,
                            generation: leaf.allocation_generation().unwrap().unwrap(),
                        },
                    })
                    .unwrap(),
                )
                .unwrap();
            *storage.buffer.write_page(leaf.id).unwrap().page_mut() = changed;
        } else if case == "duplicate-child" || case == "cycle" {
            let mut node = internal.clone();
            node.first_child = if case == "cycle" {
                meta.root_page
            } else {
                node.separators[0].right_child
            };
            let mut changed = root.clone();
            changed
                .replace_single_payload(
                    PageType::BTreeInternal,
                    &netbadb_index::encode_internal_generation(
                        &meta.spec,
                        &node,
                        meta.owner,
                        meta.root_page.generation(),
                    )
                    .unwrap(),
                )
                .unwrap();
            *storage.buffer.write_page(root.id).unwrap().page_mut() = changed;
        } else {
            let mut node = netbadb_index::decode_leaf_owned(
                &meta.spec,
                leaf.single_payload(PageType::BTreeLeaf).unwrap(),
                meta.owner,
            )
            .unwrap();
            if case == "leaf-missing" {
                node.next_leaf = None;
            } else {
                for entry in &mut node.entries {
                    entry.key = ScalarValue::UInt64(1_000_000 + entry.row_id.slot as u64);
                }
            }
            let mut changed = leaf.clone();
            changed
                .replace_single_payload(
                    PageType::BTreeLeaf,
                    &netbadb_index::encode_leaf_generation(
                        &meta.spec,
                        &node,
                        meta.owner,
                        internal.first_child.generation(),
                    )
                    .unwrap(),
                )
                .unwrap();
            *storage.buffer.write_page(leaf.id).unwrap().page_mut() = changed;
        }
        let lsn = storage.transactions.wal().borrow().next_lsn();
        assert!(storage.adopt_historical_btree_orphans().is_err(), "{case}");
        assert_eq!(storage.transactions.wal().borrow().next_lsn(), lsn);
        for old in &originals {
            *storage.buffer.write_page(old.id).unwrap().page_mut() = old.clone();
        }
    }
    storage.close().unwrap();
    cleanup(&path);
}
