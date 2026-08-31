use netbadb_types::IndexId;

/// Historical fixture only: recreate pre-Round14 unmarked orphans using the
/// exact active before images from retirement WAL, then checkpoint that state.
/// Production never reconstructs these historical bytes; adoption is explicit.
fn historical_unmarked_vacuum(storage: &mut HeapStorage) {
    storage.vacuum().unwrap();
    let mut images = std::collections::BTreeMap::new();
    for record in storage.wal_records().unwrap() {
        if let crate::WalRecordKind::PageUpdate { page_id, before, after } = record.kind {
            let page = Page::from_bytes(page_id, *after);
            let kind = page.header().unwrap().page_type;
            if matches!(kind, PageType::BTreeLeaf | PageType::BTreeInternal)
                && netbadb_index::retired_btree_page(page.single_payload(kind).unwrap()).unwrap().is_some() {
                images.insert(page_id, Page::from_bytes(page_id, *before));
            }
        }
    }
    for (id, original) in images {
        *storage.buffer.write_page(id).unwrap().page_mut() = original;
    }
    storage.checkpoint().unwrap();
    // Subsequent corruption fixtures dirty old-LSN images. Keep a written WAL
    // horizon in this fresh container so those test-only writes can flush.
    storage.begin_transaction().unwrap().commit().unwrap();
    storage.buffer.invalidate_reuse_inventory();
}

// Tests execute inside heap::tests::maintenance to exercise private ownership
// and recovery boundaries without exporting physical maintenance handles.

pub(super) fn legacy_catalog(path: &std::path::Path, version: u16) {
    legacy_btrees(path);
    let mut pages = PageManager::open(path).unwrap();
    let mut id = PageId(1);
    loop {
        let mut page = pages.read_page(id).unwrap();
        let mut node = decode_index_catalog(page.single_payload(PageType::IndexCatalog).unwrap()).unwrap();
        assert!(node.pending.is_empty());
        for entry in &mut node.entries {
            entry.definition.handle.owner = None;
            entry.definition.handle.meta_page = netbadb_index::BTreePageRef::Legacy(
                entry.definition.handle.meta_page.page_id(),
            );
        }
        let payload = encode_index_catalog(&node).unwrap();
        let mut bytes = payload[..48].to_vec();
        bytes[4..6].copy_from_slice(&version.to_le_bytes());
        if version < 5 { bytes[7] = 0; bytes[40..48].fill(0); }
        let mut offset = 48;
        for entry in &node.entries {
            assert!(!entry.retired || version >= 4);
            let name_len = entry
                .definition
                .name
                .as_ref()
                .map_or(0, |name| name.as_str().len());
            if version == 2 {
                assert_eq!(name_len, 0);
            }
            bytes.extend_from_slice(&payload[offset..offset + if version >= 4 { 48 } else { 40 }]);
            bytes.extend_from_slice(&payload[offset + 56..offset + 56 + name_len]);
            offset += 56 + name_len;
        }
        page.replace_single_payload(PageType::IndexCatalog, &bytes)
            .unwrap();
        page.refresh_checksum();
        pages.write_page(&page).unwrap();
        match node.next_catalog {
            Some(next) => id = next,
            None => break,
        }
    }
    pages.sync().unwrap();
}

#[test]
fn compaction_bounds_catalog_growth_and_never_reuses_ids() {
    let path = test_path("round8-stress");
    cleanup(&path);
    let mut storage =
        HeapStorage::create_with_buffer_pool_size(&path, indexed_table(), 512).unwrap();
    let active =
        historical_append_named_index(&mut storage, IndexName::new("keep").unwrap(), ColumnId(1))
            .unwrap();
    let name = IndexName::new("reusable").unwrap();
    let mut last_id = active.id;
    for _ in 0..100 {
        let index = historical_append_named_index(&mut storage, name.clone(), ColumnId(2)).unwrap();
        assert!(index.id.0 > last_id.0);
        last_id = index.id;
        storage.drop_index(index.id).unwrap();
    }
    storage.analyze().unwrap();
    let statistics = storage.table_statistics();
    let index_statistics = storage.index_statistics(ColumnId(1));
    let report = storage.compact_index_catalog().unwrap();
    eprintln!("ROUND8_STRESS {report:?}");
    assert!(report.catalog_pages_before > report.catalog_pages_after);
    assert_eq!(report.catalog_pages_after, 1);
    assert_eq!(report.retired_indexes_removed, 100);
    assert_eq!(report.retired_tree_pages_seen, 200);
    assert_eq!(report.pages_reclaimed, 0);
    assert_eq!(report.file_pages_before, report.file_pages_after);
    assert_eq!(report.pages_abandoned, report.retired_catalog_pages);
    assert_eq!(report.pending_reclaim_indexes, 100);
    assert_eq!(
        storage.inspect_index_reclaim().unwrap().retired_owned_pages,
        200
    );
    assert_eq!(report.next_index_id.0, last_id.0 + 1);
    assert_eq!(storage.indexes(), std::slice::from_ref(&active));
    assert!(storage.retired_indexes().is_empty());
    assert_eq!(statistics, storage.table_statistics());
    assert_eq!(index_statistics, storage.index_statistics(ColumnId(1)));
    let records = storage.wal_records().unwrap().len();
    for _ in 0..3 {
        let again = storage.compact_index_catalog().unwrap();
        assert_eq!(again.retired_indexes_removed, 0);
        assert_eq!(again.pages_abandoned, 0);
        assert_eq!(again.catalog_pages_before, again.catalog_pages_after);
        assert_eq!(again.file_pages_before, report.file_pages_after);
        assert_eq!(records, storage.wal_records().unwrap().len());
    }
    storage.checkpoint().unwrap();
    storage.close().unwrap();
    let mut storage = HeapStorage::open_with_buffer_pool_size(&path, indexed_table(), 512).unwrap();
    let fresh = historical_append_named_index(&mut storage, name, ColumnId(2)).unwrap();
    assert_eq!(fresh.id.0, last_id.0 + 1);
    assert_eq!(storage.index_statistics(ColumnId(2)), None);
    let row = storage.insert(&indexed_rows()[0]).unwrap();
    assert_eq!(
        storage
            .btree()
            .lookup(active.handle, &ScalarValue::UInt64(1))
            .unwrap(),
        vec![row]
    );
    let updated = storage.update(row, &indexed_rows()[1]).unwrap();
    storage.vacuum().unwrap();
    assert!(
        storage
            .btree()
            .lookup(active.handle, &ScalarValue::UInt64(1))
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        storage
            .btree()
            .lookup(active.handle, &ScalarValue::UInt64(2))
            .unwrap(),
        vec![updated]
    );
    storage.delete(updated).unwrap();
    storage.vacuum().unwrap();
    storage.analyze().unwrap();
    assert!(
        storage
            .btree()
            .lookup(active.handle, &ScalarValue::UInt64(2))
            .unwrap()
            .is_empty()
    );
    storage.close().unwrap();
    cleanup(&path);
}

#[test]
fn compaction_upgrades_real_v2_v3_and_v4_preserving_identity() {
    for version in [2_u16, 3, 4] {
        let path = test_path(&format!("round8-upgrade-{version}"));
        cleanup(&path);
        let mut storage =
            HeapStorage::create_with_buffer_pool_size(&path, indexed_table(), 512).unwrap();
        if version == 4 {
            historical_append_index(&mut storage, ColumnId(1)).unwrap(); // active 1
            let retired = historical_append_index(&mut storage, ColumnId(2)).unwrap();
            storage.drop_index(retired.id).unwrap(); // retired 2
            historical_append_index(&mut storage, ColumnId(3)).unwrap(); // active 3
            let retired = historical_append_index(&mut storage, ColumnId(2)).unwrap();
            storage.drop_index(retired.id).unwrap(); // retired 4
        } else {
            for col in 1..=3 {
                if version == 3 {
                    historical_append_named_index(
                        &mut storage,
                        IndexName::new(format!("legacy_{col}")).unwrap(),
                        ColumnId(col),
                    )
                    .unwrap();
                } else {
                    historical_append_index(&mut storage, ColumnId(col)).unwrap();
                }
            }
        }
        storage.analyze().unwrap();
        storage.checkpoint().unwrap();
        storage.close().unwrap();
        legacy_catalog(&path, version);
        let mut storage =
            HeapStorage::open_with_buffer_pool_size(&path, indexed_table(), 1).unwrap();
        let active = storage.indexes().to_vec();
        let stats = storage.table_statistics();
        let index_stats = storage.index_statistics.clone();
        let next = storage.read_index_catalog(PageId(1)).unwrap().next_index_id;
        if version < 4 {
            assert!(
                active
                    .iter()
                    .all(|index| index.id.0 == index.handle.meta_page.page_id().0)
            );
        }
        let report = storage.compact_index_catalog().unwrap();
        assert_eq!(report.next_index_id, next);
        assert_eq!(storage.indexes(), active);
        assert_eq!(storage.index_statistics, index_stats);
        assert_eq!(storage.table_statistics(), stats);
        storage.checkpoint().unwrap();
        storage.close().unwrap();
        let mut storage =
            HeapStorage::open_with_buffer_pool_size(&path, indexed_table(), 512).unwrap();
        assert_eq!(storage.indexes(), active);
        assert_eq!(storage.table_statistics(), stats);
        assert_eq!(storage.index_statistics, index_stats);
        assert!(storage.retired_indexes().is_empty());
        let snapshot = storage.read_index_catalog(PageId(1)).unwrap();
        assert!(snapshot.current_format);
        assert_eq!(snapshot.next_index_id, next);
        if version == 4 {
            assert_eq!(
                active.iter().map(|entry| entry.id.0).collect::<Vec<_>>(),
                vec![1, 3]
            );
            assert_eq!(
                historical_append_index(&mut storage, ColumnId(2))
                    .unwrap()
                    .id
                    .0,
                5
            );
        }
        storage.close().unwrap();
        cleanup(&path);
    }
}

#[test]
fn maintenance_reuses_checkpoint_admission_and_blocks_pending_states() {
    let path = test_path("round8-quiescence");
    cleanup(&path);
    let mut storage = HeapStorage::create(&path, indexed_table()).unwrap();
    let index = storage.create_index(ColumnId(1)).unwrap();
    let reader = storage.begin_transaction().unwrap();
    assert!(matches!(
        storage.compact_index_catalog(),
        Err(StorageError::Checkpoint(
            CheckpointError::OutstandingTransactions { .. }
        ))
    ));
    drop(reader);
    let mut writer = storage.begin_transaction().unwrap();
    storage.drop_index_in(&mut writer, index.id).unwrap();
    assert!(matches!(
        storage.compact_index_catalog(),
        Err(StorageError::Checkpoint(
            CheckpointError::WriterActive { .. }
        ))
    ));
    storage
        .transactions
        .wal()
        .borrow_mut()
        .inject_flush_failure();
    assert!(writer.commit().is_err());
    assert!(matches!(
        storage.compact_index_catalog(),
        Err(StorageError::Checkpoint(
            CheckpointError::WriterActive { .. }
        ))
    ));
    writer.commit().unwrap();
    storage.publish_committed_index_drop(index.id);
    drop(writer);
    storage.compact_index_catalog().unwrap();
    let mut writer = storage.begin_transaction().unwrap();
    storage.insert_in(&mut writer, &indexed_rows()[0]).unwrap();
    drop(writer);
    assert!(matches!(
        storage.compact_index_catalog(),
        Err(StorageError::Checkpoint(CheckpointError::RecoveryRequired))
    ));
    storage.simulate_crash();
    HeapStorage::open(&path, indexed_table())
        .unwrap()
        .close()
        .unwrap();
    cleanup(&path);
}

#[test]
fn high_water_corruption_and_exhaustion_are_typed_errors() {
    for value in [0_u64, 1] {
        let path = test_path(&format!("round8-bad-high-water-{value}"));
        cleanup(&path);
        let mut storage = HeapStorage::create(&path, indexed_table()).unwrap();
        storage.create_index(ColumnId(1)).unwrap();
        storage.checkpoint().unwrap();
        storage.close().unwrap();
        let mut pages = PageManager::open(&path).unwrap();
        let mut page = pages.read_page(PageId(1)).unwrap();
        let mut payload = page
            .single_payload(PageType::IndexCatalog)
            .unwrap()
            .to_vec();
        payload[40..48].copy_from_slice(&value.to_le_bytes());
        page.replace_single_payload(PageType::IndexCatalog, &payload)
            .unwrap();
        page.refresh_checksum();
        pages.write_page(&page).unwrap();
        pages.sync().unwrap();
        drop(pages);
        assert!(matches!(
            HeapStorage::open(&path, indexed_table()),
            Err(StorageError::Index(IndexError::InvalidIndexHighWater(_)))
        ));
        cleanup(&path);
    }
    let path = test_path("round8-exhausted");
    cleanup(&path);
    let mut storage = HeapStorage::create(&path, indexed_table()).unwrap();
    storage.checkpoint().unwrap();
    storage.close().unwrap();
    rewrite_catalog(&path, |node| node.next_index_id = Some(IndexId(u64::MAX)));
    let mut storage = HeapStorage::open(&path, indexed_table()).unwrap();
    assert_eq!(
        storage.compact_index_catalog().unwrap().next_index_id,
        IndexId(u64::MAX)
    );
    let count = storage.buffer.page_count();
    assert!(matches!(
        storage.create_index(ColumnId(1)),
        Err(StorageError::Index(IndexError::IndexIdExhausted))
    ));
    assert_eq!(storage.buffer.page_count(), count);
    storage.close().unwrap();
    cleanup(&path);
}

#[test]
fn process_crash_compaction_preserves_old_or_new_catalog() {
    for (point, winner) in [
        (TestCrashPoint::IndexCompactAfterLogs, false),
        (TestCrashPoint::IndexCompactBeforeRootPublish, false),
        (TestCrashPoint::IndexCompactAfterPagePublish, false),
        (TestCrashPoint::IndexCompactAfterPagesDurable, false),
        (TestCrashPoint::CommitAfterWalSync, true),
        (TestCrashPoint::IndexCompactAfterCommit, true),
    ] {
        let path = test_path(&format!("round8-crash-{}", point.as_str()));
        cleanup(&path);
        let mut storage =
            HeapStorage::create_with_buffer_pool_size(&path, indexed_table(), 512).unwrap();
        let active = historical_append_named_index(
            &mut storage,
            IndexName::new("kept").unwrap(),
            ColumnId(1),
        )
        .unwrap();
        for _ in 0..90 {
            let index = historical_append_named_index(
                &mut storage,
                IndexName::new("again").unwrap(),
                ColumnId(2),
            )
            .unwrap();
            storage.drop_index(index.id).unwrap();
        }
        storage.analyze().unwrap();
        let stats = storage.table_statistics();
        storage.checkpoint().unwrap();
        storage.close().unwrap();
        spawn_crash_child(&path, "index-compact", point);
        for _ in 0..3 {
            let mut storage =
                HeapStorage::open_with_buffer_pool_size(&path, indexed_table(), 512).unwrap();
            assert_eq!(
                storage
                    .inspect_index_reclaim()
                    .unwrap()
                    .pending_reclaim_indexes,
                90
            );
            let snapshot = storage.read_index_catalog(PageId(1)).unwrap();
            assert_eq!(snapshot.pending.len(), if winner { 90 } else { 0 });
            assert_eq!(storage.indexes(), std::slice::from_ref(&active));
            assert_eq!(storage.retired_indexes().len(), if winner { 0 } else { 90 });
            assert_eq!(storage.table_statistics(), stats);
            storage.close().unwrap();
        }
        let mut storage =
            HeapStorage::open_with_buffer_pool_size(&path, indexed_table(), 512).unwrap();
        assert_eq!(
            historical_append_index(&mut storage, ColumnId(2))
                .unwrap()
                .id
                .0,
            92
        );
        storage.close().unwrap();
        cleanup(&path);
    }
}

#[test]
fn process_crash_legacy_compaction_undoes_or_publishes_new_continuation() {
    for point in [
        TestCrashPoint::IndexCompactAfterLogs,
        TestCrashPoint::IndexCompactAfterAllocation,
        TestCrashPoint::IndexCompactAfterPagePublish,
        TestCrashPoint::IndexCompactBeforeRootPublish,
        TestCrashPoint::IndexCompactAfterPagesDurable,
        TestCrashPoint::CommitAfterWalSync,
        TestCrashPoint::IndexCompactAfterCommit,
    ] {
        let path = test_path(&format!("round8-legacy-crash-{}", point.as_str()));
        cleanup(&path);
        let mut storage = HeapStorage::create(&path, indexed_table()).unwrap();
        for col in 1..=3 {
            storage.create_index(ColumnId(col)).unwrap();
        }
        storage.checkpoint().unwrap();
        storage.close().unwrap();
        legacy_catalog(&path, 2);
        let mut storage = HeapStorage::open(&path, indexed_table()).unwrap();
        let active = storage.indexes().to_vec();
        let before_pages = storage.buffer.page_count();
        let next = storage.read_index_catalog(PageId(1)).unwrap().next_index_id;
        storage.close().unwrap();
        spawn_crash_child(&path, "index-compact-legacy", point);
        let winner = matches!(
            point,
            TestCrashPoint::CommitAfterWalSync | TestCrashPoint::IndexCompactAfterCommit
        );
        for _ in 0..3 {
            let mut storage = HeapStorage::open(&path, indexed_table()).unwrap();
            assert_eq!(storage.indexes(), active);
            let snapshot = storage.read_index_catalog(PageId(1)).unwrap();
            assert_eq!(snapshot.next_index_id, next);
            assert_eq!(snapshot.current_format, winner);
            assert_eq!(
                storage.buffer.page_count(),
                before_pages + u64::from(winner)
            );
            storage.close().unwrap();
        }
        cleanup(&path);
    }
}

#[test]
fn ownership_rejects_shared_cycles_wrong_kind_and_leaf_links_without_writes() {
    for case in [
        "alias",
        "heap",
        "catalog",
        "cycle",
        "leaf-link",
        "out-of-range",
    ] {
        let path = test_path(&format!("round8-ownership-{case}"));
        cleanup(&path);
        let mut storage = HeapStorage::create(&path, indexed_table()).unwrap();
        let active = storage.create_index(ColumnId(1)).unwrap();
        let retired = storage.create_index(ColumnId(2)).unwrap();
        storage.drop_index(retired.id).unwrap();
        let active_root = storage.btree().read_meta(active.handle).unwrap().root_page;
        let mut meta = storage.btree().read_meta(retired.handle).unwrap();
        let page_id;
        let payload;
        let kind;
        if case == "leaf-link" {
            page_id = meta.root_page;
            kind = PageType::BTreeLeaf;
            payload = netbadb_index::encode_leaf_generation(
                &meta.spec,
                &netbadb_index::LeafNode {
                    entries: vec![],
                    next_leaf: Some(active_root),
                },
                meta.owner,
                page_id.generation(),
            )
            .unwrap();
        } else {
            meta.root_page = match case {
                "alias" => active_root,
                "heap" => netbadb_index::BTreePageRef::Allocated(netbadb_types::PageRef {
                    page_id: PageId(2),
                    generation: meta.generation.unwrap(),
                }),
                "catalog" => netbadb_index::BTreePageRef::Allocated(netbadb_types::PageRef {
                    page_id: PageId(1),
                    generation: meta.generation.unwrap(),
                }),
                "cycle" => retired.handle.meta_page,
                _ => netbadb_index::BTreePageRef::Allocated(netbadb_types::PageRef {
                    page_id: PageId(storage.buffer.page_count()),
                    generation: meta.generation.unwrap(),
                }),
            };
            page_id = retired.handle.meta_page;
            kind = PageType::BTreeMeta;
            payload = netbadb_index::encode_meta(&meta).unwrap();
        }
        {
            let mut page = storage.buffer.write_page(page_id.page_id()).unwrap();
            page.page_mut()
                .replace_single_payload(kind, &payload)
                .unwrap();
        }
        storage.checkpoint().unwrap();
        let wal_len = storage.wal_records().unwrap().len();
        assert!(storage.compact_index_catalog().is_err(), "{case}");
        assert_eq!(storage.wal_records().unwrap().len(), wal_len);
        assert_eq!(storage.retired_indexes(), &[retired]);
        storage.close().unwrap();
        cleanup(&path);
    }
}

#[test]
fn ownership_enumerates_multilevel_tree_and_detects_duplicate_children() {
    let path = test_path("round8-multilevel");
    cleanup(&path);
    let mut storage = HeapStorage::create_with_buffer_pool_size(&path, indexed_table(), 1).unwrap();
    for id in 0..90 {
        storage
            .insert(&[
                ScalarValue::UInt64(id),
                ScalarValue::UInt64(1),
                ScalarValue::Text(format!("{id:04}{}", "x".repeat(1000))),
            ])
            .unwrap();
    }
    let index = storage.create_index(ColumnId(3)).unwrap();
    let meta = storage.btree().read_meta(index.handle).unwrap();
    assert!(meta.height >= 3);
    let snapshot = storage.read_index_catalog(PageId(1)).unwrap();
    storage.index_page_inventory(&snapshot).unwrap();
    let mut owned = std::collections::HashSet::new();
    let pages = storage
        .btree()
        .collect_owned_pages(index.handle, &mut owned)
        .unwrap();
    assert_eq!(pages, owned);
    assert!(pages.len() > 30);
    storage.drop_index(index.id).unwrap();
    let report = storage.compact_index_catalog().unwrap();
    assert_eq!(report.retired_tree_pages_seen, pages.len() as u64);
    // The raw retained handle still works: physical lifetime has NOT ended.
    assert_eq!(
        storage
            .btree()
            .lookup(
                index.handle,
                &ScalarValue::Text(format!("0000{}", "x".repeat(1000)))
            )
            .unwrap()
            .len(),
        1
    );
    let page = storage.buffer.read_page(meta.root_page.page_id()).unwrap();
    let mut internal = netbadb_index::decode_internal_owned(
        &meta.spec,
        page.page().single_payload(PageType::BTreeInternal).unwrap(),
        meta.owner,
    )
    .unwrap();
    drop(page);
    internal.separators[0].right_child = internal.first_child;
    let payload = netbadb_index::encode_internal_generation(
        &meta.spec,
        &internal,
        meta.owner,
        meta.root_page.generation(),
    )
    .unwrap();
    {
        let mut page = storage.buffer.write_page(meta.root_page.page_id()).unwrap();
        page.page_mut()
            .replace_single_payload(PageType::BTreeInternal, &payload)
            .unwrap();
    }
    assert!(matches!(
        storage.compact_index_catalog(),
        Err(StorageError::Index(IndexError::SharedTreePage(_)))
    ));
    storage.close().unwrap();
    cleanup(&path);
}

#[test]
fn compaction_partial_log_failure_rolls_back_and_pins_block_before_logging() {
    let path = test_path("round8-compaction-failure");
    cleanup(&path);
    let mut storage =
        HeapStorage::create_with_buffer_pool_size(&path, indexed_table(), 512).unwrap();
    storage.index_catalog_payload_capacity = Some(108);
    historical_append_index(&mut storage, ColumnId(1)).unwrap();
    let retired = historical_append_index(&mut storage, ColumnId(2)).unwrap();
    storage.drop_index(retired.id).unwrap();
    historical_append_index(&mut storage, ColumnId(2)).unwrap();
    historical_append_index(&mut storage, ColumnId(3)).unwrap();
    let baseline = storage.read_index_catalog(PageId(1)).unwrap();
    let pinned = storage.buffer.read_page(PageId(1)).unwrap();
    let wal_length = storage.wal_records().unwrap().len();
    assert!(matches!(
        storage.compact_index_catalog(),
        Err(StorageError::Buffer(BufferError::PagePinned { .. }))
    ));
    assert_eq!(storage.wal_records().unwrap().len(), wal_length);
    drop(pinned);
    storage.fail_index_compaction_after_logs = Some(1);
    assert!(storage.compact_index_catalog().is_err());
    let restored = storage.read_index_catalog(PageId(1)).unwrap();
    assert_eq!(restored.entries, baseline.entries);
    assert_eq!(restored.pages, baseline.pages);
    assert_eq!(restored.next_index_id, baseline.next_index_id);
    assert_eq!(storage.retired_indexes(), &[retired]);
    storage.checkpoint().unwrap();
    assert_eq!(
        storage
            .compact_index_catalog()
            .unwrap()
            .retired_indexes_removed,
        1
    );
    storage.close().unwrap();
    let storage = HeapStorage::open_with_buffer_pool_size(&path, indexed_table(), 512).unwrap();
    assert_eq!(storage.indexes().len(), 3);
    assert!(storage.retired_indexes().is_empty());
    storage.close().unwrap();
    cleanup(&path);
}

#[test]
fn open_checks_high_water_over_the_entire_chain_and_rejects_missing_authority() {
    for case in [
        "missing",
        "continuation",
        "behind-continuation",
        "legacy-overflow",
    ] {
        let path = test_path(&format!("round8-chain-high-water-{case}"));
        cleanup(&path);
        let mut storage = HeapStorage::create(&path, indexed_table()).unwrap();
        storage.index_catalog_payload_capacity = Some(108);
        for column in 1..=3 {
            storage.create_index(ColumnId(column)).unwrap();
        }
        let catalog = storage.read_index_catalog(PageId(1)).unwrap();
        assert_eq!(catalog.pages.len(), 3);
        storage.checkpoint().unwrap();
        storage.close().unwrap();
        let target = if case == "continuation" || case == "legacy-overflow" {
            catalog.pages[1]
        } else {
            PageId(1)
        };
        let mut pages = PageManager::open(&path).unwrap();
        let mut page = pages.read_page(target).unwrap();
        let mut node =
            decode_index_catalog(page.single_payload(PageType::IndexCatalog).unwrap()).unwrap();
        match case {
            "missing" => node.next_index_id = None,
            "continuation" => node.next_index_id = Some(IndexId(u64::MAX)),
            "behind-continuation" => node.next_index_id = Some(IndexId(2)),
            _ => { node.entries[0].definition.id = IndexId(u64::MAX); node.entries[0].definition.handle.owner = Some(IndexId(u64::MAX)); },
        }
        page.replace_single_payload(
            PageType::IndexCatalog,
            &encode_index_catalog(&node).unwrap(),
        )
        .unwrap();
        page.refresh_checksum();
        pages.write_page(&page).unwrap();
        pages.sync().unwrap();
        drop(pages);
        if case == "legacy-overflow" {
            legacy_catalog(&path, 4);
        }
        let error = HeapStorage::open(&path, indexed_table()).unwrap_err();
        if case == "legacy-overflow" {
            assert!(matches!(
                error,
                StorageError::Index(IndexError::IndexIdExhausted)
            ));
        } else {
            assert!(matches!(
                error,
                StorageError::Index(IndexError::InvalidIndexHighWater(_))
                    | StorageError::InvalidFormat(_)
            ));
        }
        cleanup(&path);
    }
}

// A compatibility fixture must downgrade the BTree payloads as well as catalog
// records; merely relabeling a v2 tree as a legacy registration is corruption.
pub(super) fn legacy_btrees(path: &std::path::Path) {
    use netbadb_index::{
        BTreePageRef, decode_internal_owned, decode_leaf_owned, decode_meta, encode_internal,
        encode_leaf, encode_meta,
    };
    let mut pages = PageManager::open(path).unwrap();
    let mut specs = std::collections::HashMap::new();
    for number in 1..pages.page_count() {
        let page = pages.read_page(PageId(number)).unwrap();
        if page.header().unwrap().page_type == PageType::BTreeMeta {
            let meta = decode_meta(page.single_payload(PageType::BTreeMeta).unwrap()).unwrap();
            if let Some(owner) = meta.owner {
                specs.insert(owner, meta.spec);
            }
        }
    }
    for number in 1..pages.page_count() {
        let mut page = pages.read_page(PageId(number)).unwrap();
        let kind = page.header().unwrap().page_type;
        if !matches!(
            kind,
            PageType::BTreeMeta | PageType::BTreeLeaf | PageType::BTreeInternal
        ) {
            continue;
        }
        let bytes = page.single_payload(kind).unwrap();
        let owner = netbadb_index::btree_page_owner(bytes).unwrap();
        let Some(owner_id) = owner else {
            continue;
        };
        let spec = &specs[&owner_id];
        let payload = match kind {
            PageType::BTreeMeta => {
                let mut meta = decode_meta(bytes).unwrap();
                meta.owner = None;
                meta.generation = None;
                meta.root_page = BTreePageRef::Legacy(meta.root_page.page_id());
                encode_meta(&meta).unwrap()
            }
            PageType::BTreeLeaf => {
                let mut node = decode_leaf_owned(spec, bytes, owner).unwrap();
                node.next_leaf = node.next_leaf.map(|p| BTreePageRef::Legacy(p.page_id()));
                encode_leaf(spec, &node).unwrap()
            }
            PageType::BTreeInternal => {
                let mut node = decode_internal_owned(spec, bytes, owner).unwrap();
                node.first_child = BTreePageRef::Legacy(node.first_child.page_id());
                for child in &mut node.separators {
                    child.right_child = BTreePageRef::Legacy(child.right_child.page_id());
                }
                encode_internal(spec, &node).unwrap()
            }
            _ => unreachable!(),
        };
        page.replace_single_payload(kind, &payload).unwrap();
        pages.write_page(&page).unwrap();
    }
    pages.sync().unwrap();
}

// Reproduce pre-Round-13 append histories for catalog/legacy-format regression.
// This uses real pins and the production skip policy, never an allocator bypass.
// Fixture builders using this helper need enough frames for the retained pins.
fn historical_append_index(
    storage: &mut HeapStorage,
    column: ColumnId,
) -> Result<crate::IndexDefinition, StorageError> {
    historical_append(storage, None, column)
}
fn historical_append_named_index(
    storage: &mut HeapStorage,
    name: IndexName,
    column: ColumnId,
) -> Result<crate::IndexDefinition, StorageError> {
    historical_append(storage, Some(name), column)
}
fn historical_append(
    storage: &mut HeapStorage,
    name: Option<IndexName>,
    column: ColumnId,
) -> Result<crate::IndexDefinition, StorageError> {
    let candidates = storage.inspect_reusable_pages()?.candidates;
    let pins = candidates
        .iter()
        .map(|p| storage.buffer.read_page(p.page_ref.page_id))
        .collect::<Result<Vec<_>, _>>()?;
    let count = storage.buffer.page_count();
    let result = match name {
        Some(name) => storage.create_named_index(name, column),
        None => storage.create_index(column),
    };
    if let Ok(index) = &result {
        assert!(index.handle.meta_page.page_id().0 >= count);
    }
    drop(pins);
    result
}
