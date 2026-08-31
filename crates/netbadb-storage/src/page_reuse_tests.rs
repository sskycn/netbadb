use crate::Page;
// P0 executable proofs. Offline image replacement below constructs possible
// future committed states; it is deliberately NOT an allocation implementation.

fn install_consumed_page_fixture(
    path: &std::path::Path,
    page_id: PageId,
    owner: IndexId,
    generation: PageGeneration,
) {
    let spec = IndexSpec {
        data_type: netbadb_types::SemanticType::physical(PhysicalType::UInt64),
        nullable: true,
    };
    let payload = encode_leaf_generation(
        &spec,
        &netbadb_index::LeafNode::empty(),
        Some(owner),
        Some(generation),
    )
    .unwrap();
    let mut page = Page::new(page_id, PageType::BTreeLeaf);
    page.initialize_single_payload(PageType::BTreeLeaf, &payload)
        .unwrap();
    let mut disk = PageManager::open(path).unwrap();
    disk.write_page(&page).unwrap();
    disk.sync().unwrap();
}

#[test]
fn reusable_inventory_survives_meta_first_and_orphan_only_remainders() {
    for capacity in [1, 8] {
        let path = test_path(&format!("round12-partial-{capacity}"));
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
        let old = storage.create_index(ColumnId(3)).unwrap();
        let height = storage.btree().height(old.handle).unwrap();
        for (row, _) in storage.scan().unwrap() {
            storage.delete(row).unwrap();
        }
        storage.vacuum().unwrap();
        let old_meta = storage.btree().read_meta(old.handle).unwrap();
        let active_inventory = storage.inspect_index_reclaim().unwrap();
        assert_eq!(old_meta.height, 1);
        assert_eq!(
            active_inventory.active_orphan_pages,
            active_inventory.owned_pages - 2
        );
        assert!(
            storage
                .inspect_reusable_pages()
                .unwrap()
                .candidates
                .is_empty()
        );
        assert_eq!(
            storage
                .inspect_reusable_pages()
                .unwrap()
                .blocked_active_orphans,
            active_inventory.active_orphan_pages
        );
        let active = storage.create_index(ColumnId(1)).unwrap();
        storage.drop_index(old.id).unwrap();
        storage.compact_index_catalog().unwrap();
        let initial = storage.inspect_reusable_pages().unwrap();
        assert!(initial.candidates.len() > 20);
        assert_eq!(initial.middle_candidates as usize, initial.candidates.len());
        assert_eq!(
            initial.candidates[0].page_ref.page_id,
            old.handle.meta_page.page_id()
        );
        assert!(
            initial
                .candidates
                .windows(2)
                .all(|p| p[0].page_ref.page_id < p[1].page_ref.page_id)
        );
        let mut generations = Vec::new();
        let mut tx = storage.begin_transaction().unwrap();
        for _ in &initial.candidates {
            generations.push(tx.reserve_page_generation().unwrap());
        }
        tx.commit().unwrap();
        drop(tx);
        storage.checkpoint().unwrap();
        storage.close().unwrap();
        // Meta first, then the only reachable root, then old merge orphans.
        let mut ordered = initial.candidates.clone();
        ordered.sort_by_key(|p| {
            if p.page_ref.page_id == old.handle.meta_page.page_id() {
                (0, p.page_ref.page_id)
            } else if p.page_ref.page_id == old_meta.root_page.page_id() {
                (1, p.page_ref.page_id)
            } else {
                (2, p.page_ref.page_id)
            }
        });
        for (position, candidate) in ordered.iter().enumerate() {
            install_consumed_page_fixture(
                &path,
                candidate.page_ref.page_id,
                active.id,
                generations[position],
            );
            let mut storage =
                HeapStorage::open_with_buffer_pool_size(&path, indexed_table(), capacity).unwrap();
            let report = storage.inspect_reusable_pages().unwrap();
            assert_eq!(
                report.candidates.len(),
                initial.candidates.len() - position - 1
            );
            assert_eq!(report.file_pages, initial.file_pages);
            assert!(
                report
                    .candidates
                    .iter()
                    .all(|p| p.retired_index_id == old.id)
            );
            assert!(matches!(
                storage
                    .buffer
                    .read_btree_page(BTreePageRef::Allocated(candidate.page_ref)),
                Err(StorageError::Index(IndexError::GenerationMismatch { .. }))
            ));
            let fresh = BTreePageRef::Allocated(PageRef {
                page_id: candidate.page_ref.page_id,
                generation: generations[position],
            });
            storage.buffer.read_btree_page(fresh).unwrap();
            let before = storage.inspect_index_reclaim().unwrap();
            assert_eq!(before.pending.len(), 1); // zero-owner record may safely linger
            assert_eq!(before.pending[0].meta_page, None);
            storage.compact_index_catalog().unwrap();
            let after = storage.inspect_index_reclaim().unwrap();
            assert_eq!(
                after.pending.len(),
                usize::from(!report.candidates.is_empty())
            );
            assert!(after.next_index_id > active.id);
            assert_eq!(storage.btree().height(active.handle).unwrap(), 1);
            storage.checkpoint().unwrap();
            storage.close().unwrap();
        }
        eprintln!(
            "ROUND12_PARTIAL capacity={capacity} height={height}->1 old_pages={} active_orphans={} simulated_consumed={} file_pages={} production_reused=0",
            initial.candidates.len(),
            active_inventory.active_orphan_pages,
            ordered.len(),
            initial.file_pages
        );
        cleanup(&path);
    }
}

#[test]
fn owner_only_partial_remainder_can_be_reclaimed_as_tail() {
    let path = test_path("round12-partial-tail");
    cleanup(&path);
    let mut storage = HeapStorage::create(&path, indexed_table()).unwrap();
    let active = storage.create_index(ColumnId(1)).unwrap();
    let old = storage.create_index(ColumnId(2)).unwrap();
    storage.drop_index(old.id).unwrap();
    storage.compact_index_catalog().unwrap();
    let mut tx = storage.begin_transaction().unwrap();
    let generation = tx.reserve_page_generation().unwrap();
    tx.commit().unwrap();
    drop(tx);
    let pages = storage.buffer.page_count();
    storage.checkpoint().unwrap();
    storage.close().unwrap();
    install_consumed_page_fixture(&path, old.handle.meta_page.page_id(), active.id, generation);
    let mut storage = HeapStorage::open(&path, indexed_table()).unwrap();
    assert_eq!(
        storage.inspect_reusable_pages().unwrap().candidates.len(),
        1
    );
    let report = storage.reclaim_retired_index_tail().unwrap();
    assert_eq!(report.reclaimed_pages, 1);
    assert_eq!(report.pending_indexes_remaining, 0);
    assert_eq!(report.page_count_after, pages - 1);
    storage.close().unwrap();
    for _ in 0..3 {
        let mut storage = HeapStorage::open(&path, indexed_table()).unwrap();
        assert!(
            storage
                .inspect_reusable_pages()
                .unwrap()
                .candidates
                .is_empty()
        );
        assert!(storage.inspect_index_reclaim().unwrap().pending.is_empty());
        assert_eq!(storage.btree().height(active.handle).unwrap(), 1);
        storage.close().unwrap();
    }
    cleanup(&path);
}

#[test]
fn current_wal_rejects_hole_generation_transition_even_after_checkpoint() {
    for checkpoint in [false, true] {
        let path = test_path(&format!("round12-wal-gate-{checkpoint}"));
        cleanup(&path);
        let mut storage = HeapStorage::create(&path, indexed_table()).unwrap();
        let old = storage.create_index(ColumnId(1)).unwrap();
        storage.drop_index(old.id).unwrap();
        storage.compact_index_catalog().unwrap();
        if checkpoint {
            storage.checkpoint().unwrap();
        }
        let before = storage
            .buffer
            .read_btree_page(old.handle.meta_page)
            .unwrap()
            .page()
            .clone();
        let mut tx = storage.begin_transaction().unwrap();
        let generation = tx.reserve_page_generation().unwrap();
        let payload = encode_leaf_generation(
            &IndexSpec {
                data_type: netbadb_types::SemanticType::physical(PhysicalType::UInt64),
                nullable: false,
            },
            &netbadb_index::LeafNode::empty(),
            Some(IndexId(old.id.0 + 1)),
            Some(generation),
        )
        .unwrap();
        let mut after = Page::new(before.id, PageType::BTreeLeaf);
        after
            .initialize_single_payload(PageType::BTreeLeaf, &payload)
            .unwrap();
        assert!(matches!(
            tx.log_page_update(&before, &mut after),
            Err(StorageError::Wal(crate::WalError::InvalidPageImage {
                image: "changed allocation",
                ..
            }))
        ));
        tx.rollback().unwrap();
        drop(tx);
        assert_eq!(
            storage.inspect_reusable_pages().unwrap().candidates.len(),
            2
        );
        assert_eq!(
            storage
                .buffer
                .read_btree_page(old.handle.meta_page)
                .unwrap()
                .page()
                .bytes(),
            before.bytes()
        );
        storage.close().unwrap();
        for _ in 0..3 {
            let mut storage = HeapStorage::open(&path, indexed_table()).unwrap();
            assert_eq!(
                storage.inspect_reusable_pages().unwrap().candidates.len(),
                2
            );
            storage.close().unwrap();
        }
        cleanup(&path);
    }
}

#[test]
fn reusable_scan_excludes_raw_heap_catalog_and_refuses_corruption() {
    for corruption in [
        "crc",
        "owner-zero",
        "generation-zero",
        "kind",
        "extra",
        "duplicate-meta",
        "legacy-under-owner",
        "unknown-owner",
    ] {
        let path = test_path(&format!("round12-corrupt-{corruption}"));
        cleanup(&path);
        let mut storage = HeapStorage::create(&path, indexed_table()).unwrap();
        storage.insert(&indexed_rows()[0]).unwrap();
        let old = storage.create_index(ColumnId(2)).unwrap();
        let active = storage.create_index(ColumnId(1)).unwrap();
        storage.drop_index(old.id).unwrap();
        storage.compact_index_catalog().unwrap();
        let raw = storage
            .btree()
            .create(IndexSpec {
                data_type: netbadb_types::SemanticType::physical(PhysicalType::UInt64),
                nullable: false,
            })
            .unwrap();
        let valid = storage.inspect_reusable_pages().unwrap();
        assert_eq!(valid.candidates.len(), 2);
        assert_eq!(valid.middle_candidates, 2);
        assert!(
            valid
                .candidates
                .iter()
                .all(|p| p.retired_index_id == old.id)
        );
        assert!(
            valid
                .candidates
                .iter()
                .all(|p| p.page_ref.page_id != raw.meta_page.page_id()
                    && p.page_ref.page_id != active.handle.meta_page.page_id())
        );
        storage.checkpoint().unwrap();
        storage.close().unwrap();
        let mut disk = PageManager::open(&path).unwrap();
        let mut page = disk.read_page(old.handle.meta_page.page_id()).unwrap();
        let mut payload = page.single_payload(PageType::BTreeMeta).unwrap().to_vec();
        if corruption == "crc" {
            page.bytes_mut()[100] ^= 1;
        } else {
            match corruption {
                "owner-zero" => payload[8..16].fill(0),
                "generation-zero" => payload[16..24].fill(0),
                "unknown-owner" => payload[8..16].copy_from_slice(&999_u64.to_le_bytes()),
                "extra" => payload.push(0),
                "legacy-under-owner" => {
                    let mut meta = netbadb_index::decode_meta(&payload).unwrap();
                    meta.generation = None;
                    meta.root_page = BTreePageRef::Legacy(meta.root_page.page_id());
                    payload = netbadb_index::encode_meta(&meta).unwrap();
                }
                "duplicate-meta" => {
                    let active_page = disk.read_page(active.handle.meta_page.page_id()).unwrap();
                    payload = active_page
                        .single_payload(PageType::BTreeMeta)
                        .unwrap()
                        .to_vec();
                }
                "kind" => {}
                _ => unreachable!(),
            }
            let kind = if corruption == "kind" {
                PageType::BTreeLeaf
            } else {
                PageType::BTreeMeta
            };
            page = Page::new(page.id, kind);
            page.initialize_single_payload(kind, &payload).unwrap();
        }
        disk.write_page(&page).unwrap();
        disk.sync().unwrap();
        drop(disk);
        if let Ok(mut storage) = HeapStorage::open(&path, indexed_table()) {
            let wal = storage.wal_records().unwrap().len();
            assert!(storage.inspect_reusable_pages().is_err(), "{corruption}");
            assert!(storage.compact_index_catalog().is_err(), "{corruption}");
            assert_eq!(storage.wal_records().unwrap().len(), wal);
            storage.close().unwrap();
        }
        cleanup(&path);
    }
}

#[test]
fn reusable_inspection_is_quiescent_and_cross_kind_allocations_remain_excluded() {
    let path = test_path("round12-no-cross-kind");
    cleanup(&path);
    let mut storage =
        HeapStorage::create_with_buffer_pool_size(&path, indexed_table(), 64).unwrap();
    let old = storage.create_index(ColumnId(1)).unwrap();
    storage.drop_index(old.id).unwrap();
    storage.compact_index_catalog().unwrap();
    let candidates = storage.inspect_reusable_pages().unwrap().candidates;
    let pinned = storage
        .buffer
        .read_btree_page(old.handle.meta_page)
        .unwrap();
    assert!(matches!(
        storage.inspect_reusable_pages(),
        Err(StorageError::Buffer(BufferError::PagePinned { .. }))
    ));
    drop(pinned);
    let mut tx = storage.begin_transaction().unwrap();
    tx.acquire_writer().unwrap();
    assert!(storage.inspect_reusable_pages().is_err());
    tx.rollback().unwrap();
    drop(tx);
    let count = storage.buffer.page_count();
    let raw = storage
        .btree()
        .create(IndexSpec {
            data_type: netbadb_types::SemanticType::physical(PhysicalType::UInt64),
            nullable: false,
        })
        .unwrap();
    assert_eq!(raw.meta_page.page_id().0, count);
    for id in 0..10 {
        let row = storage
            .insert(&[
                ScalarValue::UInt64(id),
                ScalarValue::UInt64(1),
                ScalarValue::Text("z".repeat(3000)),
            ])
            .unwrap();
        assert!(candidates.iter().all(|p| p.page_ref.page_id != row.page));
    }
    storage.index_catalog_payload_capacity = Some(108);
    for _ in 0..5 {
        let index = historical_append_index(&mut storage, ColumnId(1)).unwrap();
        assert!(index.handle.meta_page.page_id().0 >= count);
        storage.drop_index(index.id).unwrap();
        storage.compact_index_catalog().unwrap();
    }
    let catalog = storage.read_index_catalog(PageId(1)).unwrap();
    assert!(catalog.pages.len() > 1);
    assert!(
        catalog
            .pages
            .iter()
            .all(|id| candidates.iter().all(|p| p.page_ref.page_id != *id))
    );
    let current = storage.inspect_reusable_pages().unwrap();
    assert!(candidates.iter().all(|p| current.candidates.contains(p)));
    storage.close().unwrap();
    cleanup(&path);
}

#[test]
fn round13_stress_consumes_round12_holes_without_linear_growth() {
    let path = test_path("round13-stress");
    cleanup(&path);
    // Real pin blockers reproduce the Round 12 404-page layout; release all
    // blockers before measuring the ordinary production allocator.
    let mut storage =
        HeapStorage::create_with_buffer_pool_size(&path, indexed_table(), 512).unwrap();
    for _ in 0..100 {
        let index = historical_append_index(&mut storage, ColumnId(2)).unwrap();
        storage.drop_index(index.id).unwrap();
        storage
            .btree()
            .create(IndexSpec {
                data_type: netbadb_types::SemanticType::physical(PhysicalType::UInt64),
                nullable: false,
            })
            .unwrap();
        assert_eq!(
            storage
                .reclaim_retired_index_tail()
                .unwrap()
                .reclaimed_pages,
            0
        );
    }
    storage.compact_index_catalog().unwrap();
    let initial = storage.inspect_reusable_pages().unwrap();
    assert_eq!(initial.candidates.len(), 200);
    assert_eq!(initial.middle_candidates, 200);
    assert_eq!(initial.pending_owners, 100);
    assert_eq!(initial.file_pages, 404);
    let wal = storage.wal_records().unwrap().len();
    assert_eq!(storage.inspect_reusable_pages().unwrap(), initial);
    assert_eq!(storage.wal_records().unwrap().len(), wal);
    storage.checkpoint().unwrap();
    storage.close().unwrap();
    let mut storage = HeapStorage::open_with_buffer_pool_size(&path, indexed_table(), 8).unwrap();
    let mut hundred = 0;
    for cycle in 1..=500 {
        let index = storage.create_index(ColumnId(2)).unwrap();
        assert!(index.handle.meta_page.page_id().0 < initial.file_pages);
        storage.drop_index(index.id).unwrap();
        storage.compact_index_catalog().unwrap();
        assert_eq!(storage.buffer.page_count(), initial.file_pages);
        if cycle == 100 {
            hundred = storage.buffer.page_count();
        }
    }
    let records = storage.wal_records().unwrap();
    let transitions = records
        .iter()
        .filter(|r| matches!(r.kind, WalRecordKind::PageAllocationTransition { .. }))
        .count();
    let appends = records.iter().filter(|r| matches!(&r.kind, WalRecordKind::PageUpdate { before, after, page_id } if before.iter().all(|b| *b == 0) && matches!(Page::from_bytes(*page_id, **after).header().unwrap().page_type, PageType::BTreeMeta | PageType::BTreeLeaf | PageType::BTreeInternal))).count();
    assert_eq!(transitions, 1000);
    assert_eq!(appends, 0);
    let after = storage.inspect_reusable_pages().unwrap();
    let owners: std::collections::HashSet<_> = after
        .candidates
        .iter()
        .map(|c| c.retired_index_id)
        .collect();
    assert_eq!(after.candidates.len(), 200);
    assert_eq!(owners.len(), 100);
    println!(
        "ROUND13_STRESS initial={} candidates={} pending={} after100={hundred} after500={} transitions={transitions} btree_appends={appends} pending_total={} pending_with_pages={} zero_page_pending={}",
        initial.file_pages,
        initial.candidates.len(),
        initial.pending_owners,
        after.file_pages,
        after.pending_owners,
        owners.len(),
        after.pending_owners - owners.len() as u64
    );
    storage.close().unwrap();
    let mut storage = HeapStorage::open(&path, indexed_table()).unwrap();
    assert_eq!(storage.inspect_reusable_pages().unwrap(), after);
    storage.close().unwrap();
    cleanup(&path);
}

#[test]
fn round12_scan_cost_observations_small_and_large() {
    for raw_trees in [0, 1000] {
        let path = test_path(&format!("round12-scan-cost-{raw_trees}"));
        cleanup(&path);
        let mut storage = HeapStorage::create(&path, indexed_table()).unwrap();
        let old = storage.create_index(ColumnId(1)).unwrap();
        storage.drop_index(old.id).unwrap();
        storage.compact_index_catalog().unwrap();
        for _ in 0..raw_trees {
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
        let started = std::time::Instant::now();
        let mut storage = HeapStorage::open(&path, indexed_table()).unwrap();
        let open_us = started.elapsed().as_micros();
        let started = std::time::Instant::now();
        let report = storage.inspect_reusable_pages().unwrap();
        let scan_us = started.elapsed().as_micros();
        assert_eq!(report.candidates.len(), 2);
        eprintln!(
            "ROUND12_SCAN pages={} candidates=2 open_us={open_us} scan_us={scan_us}",
            report.file_pages
        );
        storage.close().unwrap();
        cleanup(&path);
    }
}

#[test]
fn owner_only_cleanup_crash_matrix_preserves_partial_or_empty_inventory() {
    for consumed in [1, 2] {
        for point in [
            TestCrashPoint::IndexCompactAfterLogs,
            TestCrashPoint::IndexCompactAfterPagesDurable,
            TestCrashPoint::CommitAfterWalSync,
            TestCrashPoint::IndexCompactAfterCommit,
        ] {
            let path = test_path(&format!("round12-cleanup-{consumed}-{}", point.as_str()));
            cleanup(&path);
            let mut storage = HeapStorage::create(&path, indexed_table()).unwrap();
            let old = storage.create_index(ColumnId(1)).unwrap();
            let active = storage.create_index(ColumnId(3)).unwrap();
            let other = storage.create_index(ColumnId(2)).unwrap();
            storage.drop_index(old.id).unwrap();
            storage.compact_index_catalog().unwrap();
            let candidates = storage.inspect_reusable_pages().unwrap().candidates;
            let mut tx = storage.begin_transaction().unwrap();
            let generations = [
                tx.reserve_page_generation().unwrap(),
                tx.reserve_page_generation().unwrap(),
            ];
            tx.commit().unwrap();
            drop(tx);
            storage.checkpoint().unwrap();
            storage.close().unwrap();
            for index in 0..consumed {
                install_consumed_page_fixture(
                    &path,
                    candidates[index].page_ref.page_id,
                    active.id,
                    generations[index],
                );
            }
            if consumed == 1 {
                // Make compaction perform a real metadata transaction while the
                // partial owner remains; retiring a second tree is sufficient.
                let mut storage = HeapStorage::open(&path, indexed_table()).unwrap();
                storage.drop_index(other.id).unwrap();
                storage.checkpoint().unwrap();
                storage.close().unwrap();
            }
            spawn_crash_child(&path, "index-compact", point);
            let winner = matches!(
                point,
                TestCrashPoint::CommitAfterWalSync | TestCrashPoint::IndexCompactAfterCommit
            );
            for _ in 0..3 {
                let mut storage = HeapStorage::open(&path, indexed_table()).unwrap();
                let inventory = storage.inspect_index_reclaim().unwrap();
                let remaining = storage
                    .inspect_reusable_pages()
                    .unwrap()
                    .candidates
                    .into_iter()
                    .filter(|page| page.retired_index_id == old.id)
                    .count();
                assert_eq!(remaining, 2 - consumed);
                assert_eq!(
                    inventory
                        .pending
                        .iter()
                        .any(|pending| pending.index_id == old.id),
                    consumed == 1 || !winner
                );
                assert_eq!(storage.btree().height(active.handle).unwrap(), 1);
                storage.close().unwrap();
            }
            eprintln!(
                "ROUND12_CLEANUP_CRASH consumed={consumed} point={} winner={winner} remaining={} reopens=3",
                point.as_str(),
                2 - consumed
            );
            cleanup(&path);
        }
    }
}

#[test]
fn retired_v2_suffix_does_not_hide_v3_middle_candidates() {
    let path = test_path("round12-legacy-suffix");
    cleanup(&path);
    let mut storage = HeapStorage::create(&path, indexed_table()).unwrap();
    let old = storage.create_index(ColumnId(1)).unwrap();
    let legacy = storage.create_index(ColumnId(2)).unwrap();
    storage.drop_index(old.id).unwrap();
    storage.compact_index_catalog().unwrap();
    let mut meta = storage.btree().read_meta(legacy.handle).unwrap();
    storage.checkpoint().unwrap();
    storage.close().unwrap();
    // Convert only the tail fixture to v2; retain the genuine v3 middle owner.
    let mut disk = PageManager::open(&path).unwrap();
    meta.generation = None;
    meta.root_page = BTreePageRef::Legacy(meta.root_page.page_id());
    let mut page = disk.read_page(legacy.handle.meta_page.page_id()).unwrap();
    page.replace_single_payload(
        PageType::BTreeMeta,
        &netbadb_index::encode_meta(&meta).unwrap(),
    )
    .unwrap();
    disk.write_page(&page).unwrap();
    let mut page = disk.read_page(meta.root_page.page_id()).unwrap();
    page.replace_single_payload(
        PageType::BTreeLeaf,
        &netbadb_index::encode_leaf_owned(
            &meta.spec,
            &netbadb_index::LeafNode::empty(),
            meta.owner,
        )
        .unwrap(),
    )
    .unwrap();
    disk.write_page(&page).unwrap();
    let mut page = disk.read_page(PageId(1)).unwrap();
    let mut node =
        decode_index_catalog(page.single_payload(PageType::IndexCatalog).unwrap()).unwrap();
    node.entries[0].definition.handle.meta_page =
        BTreePageRef::Legacy(legacy.handle.meta_page.page_id());
    page.replace_single_payload(
        PageType::IndexCatalog,
        &encode_index_catalog(&node).unwrap(),
    )
    .unwrap();
    disk.write_page(&page).unwrap();
    disk.sync().unwrap();
    drop(disk);
    let mut storage = HeapStorage::open(&path, indexed_table()).unwrap();
    storage.drop_index(legacy.id).unwrap();
    storage.compact_index_catalog().unwrap();
    assert_eq!(
        storage
            .inspect_index_reclaim()
            .unwrap()
            .retired_suffix_pages,
        4
    );
    let reusable = storage.inspect_reusable_pages().unwrap();
    assert_eq!(reusable.candidates.len(), 2);
    assert_eq!(reusable.middle_candidates, 2);
    assert_eq!(reusable.blocked_legacy_indexes, 1);
    assert!(
        reusable
            .candidates
            .iter()
            .all(|page| page.retired_index_id == old.id)
    );
    assert_eq!(
        storage
            .reclaim_retired_index_tail()
            .unwrap()
            .reclaimed_pages,
        0
    );
    storage.close().unwrap();
    cleanup(&path);
}

#[test]
fn dormant_ref_does_not_claim_heap_appended_after_other_owner_tail_reclaim() {
    let path = test_path("round12-dormant-tail-append");
    cleanup(&path);
    let mut storage = HeapStorage::create(&path, indexed_table()).unwrap();
    let old = storage.create_index(ColumnId(3)).unwrap();
    storage
        .btree()
        .create(IndexSpec {
            data_type: netbadb_types::SemanticType::physical(PhysicalType::UInt64),
            nullable: false,
        })
        .unwrap();
    // Split above a permanent blocker, leaving part of X below any future Y
    // suffix. Otherwise tail maintenance correctly reclaims both whole owners.
    let mut row_count = 0;
    for id in 0..10 {
        storage
            .insert(&[
                ScalarValue::UInt64(id),
                ScalarValue::UInt64(1),
                ScalarValue::Text(format!("{id:04}{}", "x".repeat(1000))),
            ])
            .unwrap();
        row_count += 1;
        if storage.btree().height(old.handle).unwrap() > 1 {
            break;
        }
    }
    assert_eq!(storage.btree().height(old.handle).unwrap(), 2);
    let old_root = storage.btree().read_meta(old.handle).unwrap().root_page;
    assert_eq!(old_root.page_id().0, storage.buffer.page_count() - 1);
    let next = storage.create_index(ColumnId(1)).unwrap();
    storage.drop_index(old.id).unwrap();
    storage.compact_index_catalog().unwrap();
    let old_count = storage.inspect_reusable_pages().unwrap().candidates.len();
    let mut tx = storage.begin_transaction().unwrap();
    let generation = tx.reserve_page_generation().unwrap();
    tx.commit().unwrap();
    drop(tx);
    storage.checkpoint().unwrap();
    storage.close().unwrap();
    install_consumed_page_fixture(&path, old_root.page_id(), next.id, generation);
    let mut storage = HeapStorage::open(&path, indexed_table()).unwrap();
    storage.drop_index(next.id).unwrap();
    assert_eq!(
        storage
            .reclaim_retired_index_tail()
            .unwrap()
            .reclaimed_pages,
        3
    );
    assert_eq!(storage.buffer.page_count(), old_root.page_id().0);
    for id in 100..102 {
        storage
            .insert(&[
                ScalarValue::UInt64(id),
                ScalarValue::UInt64(1),
                ScalarValue::Text("x".repeat(3000)),
            ])
            .unwrap();
    }
    assert_eq!(
        storage
            .buffer
            .read_page(old_root.page_id())
            .unwrap()
            .page()
            .header()
            .unwrap()
            .page_type,
        PageType::Heap
    );
    let candidates = storage.inspect_reusable_pages().unwrap();
    assert_eq!(candidates.candidates.len(), old_count - 1);
    assert!(
        candidates
            .candidates
            .iter()
            .all(|page| page.retired_index_id == old.id
                && page.page_ref.page_id != old_root.page_id())
    );
    assert!(
        candidates
            .candidates
            .iter()
            .any(|page| page.page_ref.page_id == old.handle.meta_page.page_id())
    );
    storage.compact_index_catalog().unwrap();
    storage.checkpoint().unwrap();
    storage.close().unwrap();
    let mut storage = HeapStorage::open(&path, indexed_table()).unwrap();
    assert_eq!(storage.inspect_reusable_pages().unwrap(), candidates);
    assert_eq!(storage.scan().unwrap().len(), row_count + 2);
    storage.close().unwrap();
    cleanup(&path);
}
