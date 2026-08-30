#[test]
fn owned_merge_orphans_remain_discoverable_after_compaction_and_reopen() {
    let path = test_path("round9-merge-orphans");
    cleanup(&path);
    let mut storage = HeapStorage::create_with_buffer_pool_size(&path, indexed_table(), 1).unwrap();
    let mut rows = Vec::new();
    for id in 0..90 {
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
    let index = storage.create_index(ColumnId(3)).unwrap();
    let grown = storage.btree().height(index.handle).unwrap();
    assert!(grown >= 3);
    let owned_before = storage.inspect_index_reclaim().unwrap().owned_pages;
    for row in rows {
        storage.delete(row).unwrap();
    }
    storage.vacuum().unwrap();
    assert_eq!(storage.btree().height(index.handle).unwrap(), 1);
    assert!(storage.scan().unwrap().is_empty());
    let report = storage.inspect_index_reclaim().unwrap();
    assert_eq!(report.owned_pages, owned_before);
    assert_eq!(report.active_orphan_pages, owned_before - 2);
    eprintln!(
        "ROUND9_MERGE height_before={grown} height_after=1 owned={owned_before} orphans={}",
        report.active_orphan_pages
    );
    let snapshot = storage.read_index_catalog(PageId(1)).unwrap();
    let inventory = storage.index_page_inventory(&snapshot).unwrap();
    assert!(
        inventory
            .observations
            .iter()
            .all(|page| page.owner == Some(index.id))
    );
    assert!(
        inventory
            .observations
            .iter()
            .any(|page| !page.reachable && page.kind == PageType::BTreeInternal)
    );
    assert!(
        inventory
            .observations
            .iter()
            .any(|page| !page.reachable && page.kind == PageType::BTreeLeaf)
    );
    storage.drop_index(index.id).unwrap();
    storage.compact_index_catalog().unwrap();
    let snapshot = storage.read_index_catalog(PageId(1)).unwrap();
    assert!(snapshot.entries.is_empty());
    assert_eq!(
        snapshot.pending,
        vec![netbadb_index::RetiredIndexOwnership {
            index_id: index.id,
            meta_page: index.handle.meta_page
        }]
    );
    storage.checkpoint().unwrap();
    storage.close().unwrap();
    let mut storage = HeapStorage::open_with_buffer_pool_size(&path, indexed_table(), 1).unwrap();
    let report = storage.inspect_index_reclaim().unwrap();
    assert_eq!(report.retired_owned_pages, owned_before);
    assert_eq!(report.retired_orphan_pages, owned_before - 2);
    assert_eq!(report.pending_reclaim_indexes, 1);
    assert_eq!(report.pages_reclaimed, 0);
    storage.compact_index_catalog().unwrap();
    assert_eq!(storage.inspect_index_reclaim().unwrap(), report);
    storage.close().unwrap();
    cleanup(&path);
}

#[test]
fn round9_tail_and_interleaved_stress_preserve_every_pending_owner() {
    for interleaved in [false, true] {
        let path = test_path(&format!("round9-stress-{interleaved}"));
        cleanup(&path);
        let mut storage =
            HeapStorage::create_with_buffer_pool_size(&path, indexed_table(), 1).unwrap();
        let before = storage.buffer.page_count();
        let mut last_id = IndexId(0);
        let mut after_drop = before;
        for _ in 0..100 {
            let index = storage.create_index(ColumnId(2)).unwrap();
            assert!(index.id.0 > last_id.0);
            last_id = index.id;
            storage.drop_index(index.id).unwrap();
            if interleaved {
                storage
                    .btree()
                    .create(IndexSpec {
                        data_type: netbadb_types::SemanticType::physical(PhysicalType::UInt64),
                        nullable: false,
                    })
                    .unwrap();
            }
            after_drop = storage.buffer.page_count();
            storage.checkpoint().unwrap();
            storage.compact_index_catalog().unwrap();
            let report = storage.inspect_index_reclaim().unwrap();
            assert_eq!(report.pending_reclaim_indexes, last_id.0);
            assert_eq!(report.retired_owned_pages, 2 * last_id.0);
            assert_eq!(report.next_index_id.0, last_id.0 + 1);
            assert_eq!(report.pages_reclaimed, 0);
        }
        let after_cycle_maintenance = storage.buffer.page_count();
        if interleaved {
            // Active allocations above all retired pages also exclude a suffix.
            storage.create_index(ColumnId(1)).unwrap();
        }
        let report = storage.inspect_index_reclaim().unwrap();
        eprintln!(
            "ROUND9_STRESS interleaved={interleaved} cycles=100 before={before} after_drop={after_drop} after_cycle_maintenance={after_cycle_maintenance} final_pages={} {report:?}",
            storage.buffer.page_count()
        );
        assert_eq!(report.pending_reclaim_indexes, 100);
        assert_eq!(report.retired_owned_pages, 200);
        if interleaved {
            assert_eq!(report.retired_suffix_pages, 0);
            assert_eq!(report.retained_middle_pages, 200);
            assert_eq!(report.unregistered_legacy_pages, 200);
        }
        let wal = storage.wal_records().unwrap().len();
        storage.compact_index_catalog().unwrap();
        assert_eq!(storage.wal_records().unwrap().len(), wal);
        storage.checkpoint().unwrap();
        storage.close().unwrap();
        let mut storage = HeapStorage::open(&path, indexed_table()).unwrap();
        assert_eq!(storage.inspect_index_reclaim().unwrap(), report);
        storage.close().unwrap();
        cleanup(&path);
    }
}

#[test]
fn owned_handles_reject_cross_owner_and_legacy_wildcards_before_wal() {
    let path = test_path("round9-stale-handle");
    cleanup(&path);
    let mut storage = HeapStorage::create(&path, indexed_table()).unwrap();
    let old = storage.create_index(ColumnId(1)).unwrap();
    storage.drop_index(old.id).unwrap();
    storage.compact_index_catalog().unwrap();
    let fresh = storage.create_index(ColumnId(1)).unwrap();
    assert!(fresh.id.0 > old.id.0);
    // Model a stale address after hypothetical cross-owner reuse. No physical
    // truncate is performed or claimed: this isolates the owner guard itself.
    for owner in [old.handle.owner, None] {
        let handle = netbadb_index::BTreeHandle {
            owner,
            meta_page: fresh.handle.meta_page,
        };
        let records = storage.wal_records().unwrap().len();
        assert!(matches!(
            storage.btree().lookup(handle, &ScalarValue::UInt64(1)),
            Err(StorageError::Index(IndexError::OwnerMismatch { .. }))
        ));
        assert_eq!(records, storage.wal_records().unwrap().len());
    }
    storage.close().unwrap();
    cleanup(&path);
}

#[test]
fn scanner_rejects_owned_corruption_and_overlap_without_catalog_writes() {
    for case in [
        "meta-owner",
        "root-owner",
        "child-owner",
        "leaf-owner",
        "zero-owner",
        "duplicate-owner",
        "raw-alias",
        "active-pending",
        "orphan-corrupt",
    ] {
        let path = test_path(&format!("round9-corrupt-{case}"));
        cleanup(&path);
        let mut storage = HeapStorage::create(&path, indexed_table()).unwrap();
        for id in 0..18 {
            storage
                .insert(&[
                    ScalarValue::UInt64(id),
                    ScalarValue::UInt64(1),
                    ScalarValue::Text(format!("{id:04}{}", "x".repeat(1000))),
                ])
                .unwrap();
        }
        let active = storage.create_index(ColumnId(3)).unwrap();
        let other = storage.create_index(ColumnId(1)).unwrap();
        let meta = storage.btree().read_meta(active.handle).unwrap();
        assert!(meta.height >= 3);
        let mut target = meta.root_page;
        if case == "active-pending" {
            let mut catalog = storage.read_index_catalog(PageId(1)).unwrap();
            catalog.pending.push(netbadb_index::RetiredIndexOwnership {
                index_id: active.id,
                meta_page: active.handle.meta_page,
            });
            assert!(matches!(
                storage.index_page_inventory(&catalog),
                Err(StorageError::Index(IndexError::DuplicateIndexId(_)))
            ));
        } else {
            if case == "raw-alias" {
                let raw = storage.btree().create(meta.spec.clone()).unwrap();
                let mut raw_meta = storage.btree().read_meta(raw).unwrap();
                raw_meta.root_page = meta.root_page;
                raw_meta.height = meta.height;
                let payload = netbadb_index::encode_meta(&raw_meta).unwrap();
                storage
                    .buffer
                    .write_page(raw.meta_page)
                    .unwrap()
                    .page_mut()
                    .replace_single_payload(PageType::BTreeMeta, &payload)
                    .unwrap();
            } else {
                if matches!(case, "meta-owner" | "duplicate-owner") {
                    target = active.handle.meta_page;
                }
                if case == "child-owner" {
                    let page = storage.buffer.read_page(meta.root_page).unwrap();
                    let node = netbadb_index::decode_internal_owned(
                        &meta.spec,
                        page.page().single_payload(PageType::BTreeInternal).unwrap(),
                        meta.owner,
                    )
                    .unwrap();
                    target = node.first_child;
                }
                if case == "leaf-owner" {
                    loop {
                        let guard = storage.buffer.read_page(target).unwrap();
                        if guard.page().header().unwrap().page_type == PageType::BTreeLeaf {
                            break;
                        }
                        target = netbadb_index::decode_internal_owned(
                            &meta.spec,
                            guard
                                .page()
                                .single_payload(PageType::BTreeInternal)
                                .unwrap(),
                            meta.owner,
                        )
                        .unwrap()
                        .first_child;
                    }
                }
                if case == "orphan-corrupt" {
                    for (row, _) in storage.scan().unwrap() {
                        storage.delete(row).unwrap();
                    }
                    storage.vacuum().unwrap();
                    let snapshot = storage.read_index_catalog(PageId(1)).unwrap();
                    let inventory = storage.index_page_inventory(&snapshot).unwrap();
                    target = inventory
                        .observations
                        .iter()
                        .find(|page| page.owner == Some(active.id) && !page.reachable)
                        .unwrap()
                        .page_id;
                }
                let mut guard = storage.buffer.write_page(target).unwrap();
                let kind = guard.page().header().unwrap().page_type;
                let mut bytes = guard.page().single_payload(kind).unwrap().to_vec();
                let owner = if case == "zero-owner" {
                    0
                } else if case == "meta-owner" {
                    999
                } else {
                    other.id.0
                };
                if case == "orphan-corrupt" {
                    bytes.push(0);
                } else {
                    bytes[8..16].copy_from_slice(&owner.to_le_bytes());
                }
                guard
                    .page_mut()
                    .replace_single_payload(kind, &bytes)
                    .unwrap();
            }
            let wal = storage.wal_records().unwrap().len();
            assert!(storage.inspect_index_reclaim().is_err(), "{case}");
            assert!(storage.compact_index_catalog().is_err(), "{case}");
            assert_eq!(storage.wal_records().unwrap().len(), wal);
        }
        storage.close().unwrap();
        cleanup(&path);
    }
}

#[test]
fn legacy_and_raw_pages_remain_outside_owned_reclaim_inventory() {
    let path = test_path("round9-mixed-legacy");
    cleanup(&path);
    let mut storage = HeapStorage::create(&path, indexed_table()).unwrap();
    storage.create_index(ColumnId(1)).unwrap();
    storage.checkpoint().unwrap();
    storage.close().unwrap();
    legacy_catalog(&path, 5);
    let mut storage = HeapStorage::open(&path, indexed_table()).unwrap();
    let old = storage.indexes()[0].clone();
    assert_eq!(old.handle.owner, None);
    let row = storage.insert(&indexed_rows()[0]).unwrap();
    assert_eq!(
        storage
            .btree()
            .lookup(old.handle, &ScalarValue::UInt64(1))
            .unwrap(),
        vec![row]
    );
    storage.create_index(ColumnId(2)).unwrap();
    storage.drop_index(old.id).unwrap();
    assert_eq!(
        storage
            .inspect_index_reclaim()
            .unwrap()
            .legacy_unreclaimable_indexes,
        1
    );
    let compact = storage.compact_index_catalog().unwrap();
    assert_eq!(compact.pages_abandoned, 2);
    assert_eq!(compact.pending_reclaim_indexes, 0);
    let fresh = storage.create_index(ColumnId(1)).unwrap();
    assert_eq!(fresh.handle.owner, Some(fresh.id));
    let report = storage.inspect_index_reclaim().unwrap();
    assert_eq!(report.unregistered_legacy_pages, 2);
    assert_eq!(report.retired_owned_pages, 0);
    storage.checkpoint().unwrap();
    storage.close().unwrap();
    let mut storage = HeapStorage::open(&path, indexed_table()).unwrap();
    assert_eq!(storage.inspect_index_reclaim().unwrap(), report);
    storage.close().unwrap();
    cleanup(&path);
}

#[test]
fn process_crash_pending_inventory_survives_checkpoint_and_read_only_maintenance() {
    for point in [
        TestCrashPoint::CheckpointAfterNewGenerationDurable,
        TestCrashPoint::CheckpointAfterOldGenerationRemoved,
        TestCrashPoint::CommittedWithoutDataFlush,
    ] {
        let path = test_path(&format!("round9-crash-pending-{}", point.as_str()));
        cleanup(&path);
        let mut storage = HeapStorage::create(&path, indexed_table()).unwrap();
        storage.insert(&indexed_rows()[0]).unwrap();
        let old = storage.create_index(ColumnId(2)).unwrap();
        storage.drop_index(old.id).unwrap();
        storage.compact_index_catalog().unwrap();
        storage.close().unwrap();
        spawn_crash_child(&path, "index-pending-checkpoint", point);
        for _ in 0..2 {
            let mut storage = HeapStorage::open(&path, indexed_table()).unwrap();
            assert!(storage.indexes().is_empty());
            assert_eq!(storage.scan().unwrap().len(), 1);
            let report = storage.inspect_index_reclaim().unwrap();
            assert_eq!(report.pending_reclaim_indexes, 1);
            assert_eq!(report.retired_owned_pages, 2);
            assert_eq!(report.next_index_id.0, old.id.0 + 1);
            storage.close().unwrap();
        }
        cleanup(&path);
    }
}

#[test]
fn provisional_owner_is_not_a_generation_after_rollback() {
    let path = test_path("round9-provisional-owner");
    cleanup(&path);
    let mut storage = HeapStorage::create(&path, indexed_table()).unwrap();
    let mut tx = storage.begin_transaction().unwrap();
    let provisional = storage
        .create_named_index_in(&mut tx, IndexName::new("temporary").unwrap(), ColumnId(1))
        .unwrap();
    tx.rollback().unwrap();
    drop(tx);
    let committed = storage.create_index(ColumnId(1)).unwrap();
    // Concrete counterexample to Route A as a general generation protocol:
    // unpublished identity and allocation are restored together by rollback.
    // Callers MUST discard provisional handles, as documented by BTreeHandle.
    assert_eq!(provisional.id, committed.id);
    assert_eq!(provisional.handle, committed.handle);
    storage.close().unwrap();
    cleanup(&path);
}

#[test]
fn owned_key_overflow_rolls_back_build_and_dml_without_mutation() {
    let path = test_path("round9-owned-capacity");
    cleanup(&path);
    let schema = TableDef::new(
        TableId(99),
        "texts",
        vec![ColumnDef::new(
            ColumnId(1),
            "body",
            TypeSpec::Physical(PhysicalType::Text),
        )],
    );
    let mut storage = HeapStorage::create(&path, schema).unwrap();
    let row = storage
        .insert(&[ScalarValue::Text("x".repeat(4007))])
        .unwrap();
    let count = storage.buffer.page_count();
    assert!(matches!(
        storage.create_index(ColumnId(1)),
        Err(StorageError::Index(IndexError::KeyTooLarge { .. }))
    ));
    assert!(storage.indexes().is_empty());
    assert_eq!(storage.buffer.page_count(), count);
    assert_eq!(
        storage.read_index_catalog(PageId(1)).unwrap().next_index_id,
        IndexId(1)
    );
    storage.delete(row).unwrap();
    storage.vacuum().unwrap();
    let index = storage.create_index(ColumnId(1)).unwrap();
    let row = storage
        .insert(&[ScalarValue::Text("x".repeat(4005))])
        .unwrap();
    let wal = storage.wal_records().unwrap().len();
    let mut tx = storage.begin_transaction().unwrap();
    let after_begin = storage.wal_records().unwrap().len();
    assert!(matches!(
        storage.insert_in(&mut tx, &[ScalarValue::Text("x".repeat(4006))]),
        Err(StorageError::Index(IndexError::KeyTooLarge { .. }))
    ));
    assert_eq!(storage.wal_records().unwrap().len(), after_begin);
    tx.rollback().unwrap();
    drop(tx);
    assert!(storage.wal_records().unwrap().len() > wal);
    assert_eq!(
        storage
            .btree()
            .lookup(index.handle, &ScalarValue::Text("x".repeat(4005)))
            .unwrap(),
        vec![row]
    );
    storage.close().unwrap();
    cleanup(&path);
}

#[test]
fn process_crash_owned_unary_merge_rolls_back_or_commits_all_pages() {
    for point in [
        TestCrashPoint::BTreeAfterUnaryNormalization,
        TestCrashPoint::CommitAfterWalSync,
    ] {
        let path = test_path(&format!("round9-unary-crash-{}", point.as_str()));
        cleanup(&path);
        let mut storage = HeapStorage::create(&path, indexed_table()).unwrap();
        let mut rows = Vec::new();
        for id in 0..90 {
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
        let index = storage.create_index(ColumnId(3)).unwrap();
        for row in rows {
            storage.delete(row).unwrap();
        }
        let before = storage.inspect_index_reclaim().unwrap();
        storage.checkpoint().unwrap();
        storage.close().unwrap();
        spawn_crash_child(&path, "owned-vacuum", point);
        for _ in 0..2 {
            let mut storage = HeapStorage::open(&path, indexed_table()).unwrap();
            assert!(storage.scan().unwrap().is_empty());
            let report = storage.inspect_index_reclaim().unwrap();
            assert_eq!(report.owned_pages, before.owned_pages);
            if point == TestCrashPoint::CommitAfterWalSync {
                assert_eq!(storage.btree().height(index.handle).unwrap(), 1);
                assert_eq!(report.active_orphan_pages, before.owned_pages - 2);
            } else {
                assert!(storage.btree().height(index.handle).unwrap() >= 3);
                assert_eq!(report.active_orphan_pages, before.active_orphan_pages);
            }
            storage.close().unwrap();
        }
        cleanup(&path);
    }
}

#[test]
fn pending_continuation_ids_are_checked_against_root_high_water() {
    let path = test_path("round9-pending-continuations");
    cleanup(&path);
    let mut storage = HeapStorage::create(&path, indexed_table()).unwrap();
    storage.index_catalog_payload_capacity = Some(100);
    for _ in 0..10 {
        let index = storage.create_index(ColumnId(1)).unwrap();
        storage.drop_index(index.id).unwrap();
        storage.compact_index_catalog().unwrap();
    }
    let snapshot = storage.read_index_catalog(PageId(1)).unwrap();
    assert_eq!(snapshot.pending.len(), 10);
    assert!(snapshot.pages.len() > 1);
    storage.checkpoint().unwrap();
    storage.close().unwrap();
    let mut pages = PageManager::open(&path).unwrap();
    let mut page = pages.read_page(PageId(1)).unwrap();
    let mut node =
        decode_index_catalog(page.single_payload(PageType::IndexCatalog).unwrap()).unwrap();
    node.next_index_id = Some(IndexId(5)); // valid locally, behind continuation IDs
    page.replace_single_payload(
        PageType::IndexCatalog,
        &encode_index_catalog(&node).unwrap(),
    )
    .unwrap();
    page.refresh_checksum();
    pages.write_page(&page).unwrap();
    pages.sync().unwrap();
    drop(pages);
    assert!(matches!(
        HeapStorage::open(&path, indexed_table()),
        Err(StorageError::Index(IndexError::InvalidIndexHighWater(
            IndexId(5)
        )))
    ));
    cleanup(&path);
}

#[test]
fn legacy_merge_orphans_are_validated_but_never_invented_as_owned() {
    let path = test_path("round9-legacy-orphans");
    cleanup(&path);
    let mut storage = HeapStorage::create(&path, indexed_table()).unwrap();
    for id in 0..40 {
        storage
            .insert(&[
                ScalarValue::UInt64(id),
                ScalarValue::UInt64(1),
                ScalarValue::Text(format!("{id:04}{}", "x".repeat(1000))),
            ])
            .unwrap();
    }
    storage.create_index(ColumnId(3)).unwrap();
    storage.checkpoint().unwrap();
    storage.close().unwrap();
    legacy_catalog(&path, 5);
    let mut storage = HeapStorage::open(&path, indexed_table()).unwrap();
    for (row, _) in storage.scan().unwrap() {
        storage.delete(row).unwrap();
    }
    storage.vacuum().unwrap();
    let report = storage.inspect_index_reclaim().unwrap();
    assert_eq!(report.owned_pages, 0);
    assert!(report.unowned_legacy_pages > 0);
    let id = storage.indexes()[0].id;
    storage.drop_index(id).unwrap();
    storage.compact_index_catalog().unwrap();
    let retired = storage.inspect_index_reclaim().unwrap();
    assert_eq!(retired.unowned_legacy_pages, report.unowned_legacy_pages);
    assert_eq!(retired.unregistered_legacy_pages, 2);
    assert_eq!(retired.pending_reclaim_indexes, 0);
    assert_eq!(retired.pages_reclaimed, 0);
    storage.close().unwrap();
    cleanup(&path);
}
