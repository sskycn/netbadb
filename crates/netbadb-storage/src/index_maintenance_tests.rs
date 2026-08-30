use netbadb_types::IndexId;

// Tests execute inside heap::tests::maintenance to exercise private ownership
// and recovery boundaries without exporting physical maintenance handles.

fn legacy_catalog(path: &std::path::Path, version: u16) {
    let mut pages = PageManager::open(path).unwrap();
    let mut id = PageId(1);
    loop {
        let mut page = pages.read_page(id).unwrap();
        let payload = page.single_payload(PageType::IndexCatalog).unwrap();
        let node = decode_index_catalog(payload).unwrap();
        let mut bytes = payload[..48].to_vec();
        bytes[4..6].copy_from_slice(&version.to_le_bytes());
        bytes[7] = 0;
        bytes[40..48].fill(0);
        let mut offset = 48;
        for entry in &node.entries {
            assert!(!entry.retired || version == 4);
            let name_len = entry
                .definition
                .name
                .as_ref()
                .map_or(0, |name| name.as_str().len());
            if version == 2 {
                assert_eq!(name_len, 0);
            }
            bytes.extend_from_slice(&payload[offset..offset + if version == 4 { 48 } else { 40 }]);
            bytes.extend_from_slice(&payload[offset + 48..offset + 48 + name_len]);
            offset += 48 + name_len;
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
    let mut storage = HeapStorage::create_with_buffer_pool_size(&path, indexed_table(), 1).unwrap();
    let active = storage
        .create_named_index(IndexName::new("keep").unwrap(), ColumnId(1))
        .unwrap();
    let name = IndexName::new("reusable").unwrap();
    let mut last_id = active.id;
    for _ in 0..100 {
        let index = storage
            .create_named_index(name.clone(), ColumnId(2))
            .unwrap();
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
    assert_eq!(report.pages_abandoned, 200 + report.retired_catalog_pages);
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
    let mut storage = HeapStorage::open(&path, indexed_table()).unwrap();
    let fresh = storage.create_named_index(name, ColumnId(2)).unwrap();
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
        let mut storage = HeapStorage::create(&path, indexed_table()).unwrap();
        if version == 4 {
            storage.create_index(ColumnId(1)).unwrap(); // active 1
            let retired = storage.create_index(ColumnId(2)).unwrap();
            storage.drop_index(retired.id).unwrap(); // retired 2
            storage.create_index(ColumnId(3)).unwrap(); // active 3
            let retired = storage.create_index(ColumnId(2)).unwrap();
            storage.drop_index(retired.id).unwrap(); // retired 4
        } else {
            for col in 1..=3 {
                if version == 3 {
                    storage
                        .create_named_index(
                            IndexName::new(format!("legacy_{col}")).unwrap(),
                            ColumnId(col),
                        )
                        .unwrap();
                } else {
                    storage.create_index(ColumnId(col)).unwrap();
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
                    .all(|index| index.id.0 == index.handle.meta_page.0)
            );
        }
        let report = storage.compact_index_catalog().unwrap();
        assert_eq!(report.next_index_id, next);
        assert_eq!(storage.indexes(), active);
        assert_eq!(storage.index_statistics, index_stats);
        assert_eq!(storage.table_statistics(), stats);
        storage.checkpoint().unwrap();
        storage.close().unwrap();
        let mut storage = HeapStorage::open(&path, indexed_table()).unwrap();
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
            assert_eq!(storage.create_index(ColumnId(2)).unwrap().id.0, 5);
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
        let mut storage = HeapStorage::create(&path, indexed_table()).unwrap();
        let active = storage
            .create_named_index(IndexName::new("kept").unwrap(), ColumnId(1))
            .unwrap();
        for _ in 0..90 {
            let index = storage
                .create_named_index(IndexName::new("again").unwrap(), ColumnId(2))
                .unwrap();
            storage.drop_index(index.id).unwrap();
        }
        storage.analyze().unwrap();
        let stats = storage.table_statistics();
        storage.checkpoint().unwrap();
        storage.close().unwrap();
        spawn_crash_child(&path, "index-compact", point);
        for _ in 0..3 {
            let storage = HeapStorage::open(&path, indexed_table()).unwrap();
            assert_eq!(storage.indexes(), std::slice::from_ref(&active));
            assert_eq!(storage.retired_indexes().len(), if winner { 0 } else { 90 });
            assert_eq!(storage.table_statistics(), stats);
            storage.close().unwrap();
        }
        let mut storage = HeapStorage::open(&path, indexed_table()).unwrap();
        assert_eq!(storage.create_index(ColumnId(2)).unwrap().id.0, 92);
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
            payload = netbadb_index::encode_leaf(
                &meta.spec,
                &netbadb_index::LeafNode {
                    entries: vec![],
                    next_leaf: Some(active_root),
                },
            )
            .unwrap();
        } else {
            meta.root_page = match case {
                "alias" => active_root,
                "heap" => PageId(2),
                "catalog" => PageId(1),
                "cycle" => retired.handle.meta_page,
                _ => PageId(storage.buffer.page_count()),
            };
            page_id = retired.handle.meta_page;
            kind = PageType::BTreeMeta;
            payload = netbadb_index::encode_meta(&meta).unwrap();
        }
        {
            let mut page = storage.buffer.write_page(page_id).unwrap();
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
    let page = storage.buffer.read_page(meta.root_page).unwrap();
    let mut internal = netbadb_index::decode_internal(
        &meta.spec,
        page.page().single_payload(PageType::BTreeInternal).unwrap(),
    )
    .unwrap();
    drop(page);
    internal.separators[0].right_child = internal.first_child;
    let payload = netbadb_index::encode_internal(&meta.spec, &internal).unwrap();
    {
        let mut page = storage.buffer.write_page(meta.root_page).unwrap();
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
    let mut storage = HeapStorage::create(&path, indexed_table()).unwrap();
    storage.index_catalog_payload_capacity = Some(100);
    storage.create_index(ColumnId(1)).unwrap();
    let retired = storage.create_index(ColumnId(2)).unwrap();
    storage.drop_index(retired.id).unwrap();
    storage.create_index(ColumnId(2)).unwrap();
    storage.create_index(ColumnId(3)).unwrap();
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
    let storage = HeapStorage::open(&path, indexed_table()).unwrap();
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
        storage.index_catalog_payload_capacity = Some(100);
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
            _ => node.entries[0].definition.id = IndexId(u64::MAX),
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
