use netbadb_index::{
    BTreePageRef, decode_internal_owned, decode_leaf_owned, encode_internal_generation,
    encode_leaf_generation,
};
use netbadb_types::{PageGeneration, PageRef};

fn generation_ref(page: BTreePageRef, delta: u64) -> BTreePageRef {
    BTreePageRef::Allocated(PageRef {
        page_id: page.page_id(),
        generation: PageGeneration(page.generation().unwrap().0 + delta),
    })
}

#[test]
fn generation_multi_level_rollback_reuses_slots_but_rejects_every_old_reference() {
    let path = test_path("round10-multipage");
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
    let mut tx = storage.begin_transaction().unwrap();
    let old = storage
        .create_named_index_in(&mut tx, IndexName::new("reused").unwrap(), ColumnId(3))
        .unwrap();
    let old_meta = storage.btree().read_meta(old.handle).unwrap();
    assert!(old_meta.height >= 3);
    let mut old_refs = Vec::new();
    let mut saw_internal = false;
    let mut saw_next = false;
    for number in old.handle.meta_page.page_id().0..storage.buffer.page_count() {
        let guard = storage.buffer.read_page(PageId(number)).unwrap();
        let page = guard.page();
        let kind = page.header().unwrap().page_type;
        let generation = page.allocation_generation().unwrap().unwrap();
        assert!(generation.0 > 0);
        old_refs.push(BTreePageRef::Allocated(PageRef {
            page_id: page.id,
            generation,
        }));
        match kind {
            PageType::BTreeInternal => {
                let node = decode_internal_owned(
                    &old_meta.spec,
                    page.single_payload(kind).unwrap(),
                    Some(old.id),
                )
                .unwrap();
                old_refs.push(node.first_child);
                old_refs.extend(node.separators.iter().map(|s| s.right_child));
                saw_internal = true;
            }
            PageType::BTreeLeaf => {
                let node = decode_leaf_owned(
                    &old_meta.spec,
                    page.single_payload(kind).unwrap(),
                    Some(old.id),
                )
                .unwrap();
                if let Some(next) = node.next_leaf {
                    old_refs.push(next);
                    saw_next = true;
                }
            }
            _ => {}
        }
    }
    assert!(saw_internal && saw_next);
    tx.rollback().unwrap();
    drop(tx);
    let new = storage
        .create_named_index(IndexName::new("reused").unwrap(), ColumnId(3))
        .unwrap();
    assert_eq!(old.id, new.id);
    assert_eq!(
        old.handle.meta_page.page_id(),
        new.handle.meta_page.page_id()
    );
    for reference in old_refs {
        assert!(matches!(
            storage.buffer.read_btree_page(reference),
            Err(StorageError::Index(IndexError::GenerationMismatch { .. }))
        ));
    }
    assert!(matches!(
        storage.btree().height(old.handle),
        Err(StorageError::Index(IndexError::GenerationMismatch { .. }))
    ));
    assert_eq!(storage.btree().height(new.handle).unwrap(), old_meta.height);
    let inventory = storage.inspect_index_reclaim().unwrap();
    assert!(inventory.allocations.len() > 3);
    assert_eq!(inventory.allocations.len() as u64, inventory.owned_pages);
    storage.checkpoint().unwrap();
    storage.close().unwrap();
    let mut storage = HeapStorage::open(&path, indexed_table()).unwrap();
    assert_eq!(storage.indexes()[0], new);
    assert_eq!(storage.inspect_index_reclaim().unwrap(), inventory);
    assert_eq!(storage.scan().unwrap().len(), 90);
    storage.close().unwrap();
    cleanup(&path);
}

#[test]
fn generation_buffer_pins_and_dirty_rollback_cannot_cross_allocations() {
    let path = test_path("round10-buffer");
    cleanup(&path);
    let mut storage = HeapStorage::create(&path, indexed_table()).unwrap();
    let mut tx = storage.begin_transaction().unwrap();
    let old = storage
        .create_named_index_in(&mut tx, IndexName::new("reused").unwrap(), ColumnId(1))
        .unwrap();
    let root = storage.btree().read_meta(old.handle).unwrap().root_page;
    let pin = storage.buffer.read_btree_page(root).unwrap();
    assert!(matches!(
        storage.buffer.read_btree_page(generation_ref(root, 1)),
        Err(StorageError::Buffer(BufferError::PagePinned { .. }))
    ));
    assert!(matches!(
        tx.rollback(),
        Err(StorageError::Buffer(BufferError::PagePinned { .. }))
    ));
    assert!(storage.buffer.page_count() > root.page_id().0);
    drop(pin);
    tx.rollback().unwrap();
    drop(tx);
    let new = storage.create_index(ColumnId(1)).unwrap();
    storage.buffer.flush_all().unwrap();
    assert!(matches!(
        storage.buffer.read_btree_page(root),
        Err(StorageError::Index(IndexError::GenerationMismatch { .. }))
    ));
    assert_eq!(storage.btree().height(new.handle).unwrap(), 1);
    storage.close().unwrap();
    let mut storage = HeapStorage::open(&path, indexed_table()).unwrap();
    assert_eq!(storage.btree().height(new.handle).unwrap(), 1);
    storage.close().unwrap();
    cleanup(&path);
}

#[test]
fn generation_corrupt_root_child_leaf_and_pending_fail_closed() {
    for case in ["root", "child", "leaf", "handle", "pending"] {
        let path = test_path(&format!("round10-corrupt-{case}"));
        cleanup(&path);
        let mut storage = HeapStorage::create(&path, indexed_table()).unwrap();
        for id in 0..15 {
            storage
                .insert(&[
                    ScalarValue::UInt64(id),
                    ScalarValue::UInt64(1),
                    ScalarValue::Text(format!("{id:04}{}", "x".repeat(1000))),
                ])
                .unwrap();
        }
        let index = storage.create_index(ColumnId(3)).unwrap();
        let mut meta = storage.btree().read_meta(index.handle).unwrap();
        if case == "handle" {
            let handle = BTreeHandle {
                meta_page: generation_ref(index.handle.meta_page, 1),
                ..index.handle
            };
            assert!(matches!(
                storage.btree().height(handle),
                Err(StorageError::Index(IndexError::GenerationMismatch { .. }))
            ));
        } else if case == "pending" {
            storage.drop_index(index.id).unwrap();
            storage.compact_index_catalog().unwrap();
            let mut page = storage.buffer.write_page(PageId(1)).unwrap();
            let mut node =
                decode_index_catalog(page.page().single_payload(PageType::IndexCatalog).unwrap())
                    .unwrap();
            node.pending[0].meta_page = generation_ref(node.pending[0].meta_page, 1);
            page.page_mut()
                .replace_single_payload(
                    PageType::IndexCatalog,
                    &encode_index_catalog(&node).unwrap(),
                )
                .unwrap();
            drop(page);
            assert!(matches!(
                storage.inspect_index_reclaim(),
                Err(StorageError::Index(IndexError::GenerationMismatch { .. }))
            ));
        } else {
            let (target, kind, payload) = if case == "root" {
                meta.root_page = generation_ref(meta.root_page, 1);
                (
                    index.handle.meta_page,
                    PageType::BTreeMeta,
                    netbadb_index::encode_meta(&meta).unwrap(),
                )
            } else if case == "child" {
                let page = storage.buffer.read_btree_page(meta.root_page).unwrap();
                let mut node = decode_internal_owned(
                    &meta.spec,
                    page.page().single_payload(PageType::BTreeInternal).unwrap(),
                    meta.owner,
                )
                .unwrap();
                node.first_child = generation_ref(node.first_child, 1);
                (
                    meta.root_page,
                    PageType::BTreeInternal,
                    encode_internal_generation(
                        &meta.spec,
                        &node,
                        meta.owner,
                        meta.root_page.generation(),
                    )
                    .unwrap(),
                )
            } else {
                let mut target = meta.root_page;
                for _ in 1..meta.height {
                    let page = storage.buffer.read_btree_page(target).unwrap();
                    target = decode_internal_owned(
                        &meta.spec,
                        page.page().single_payload(PageType::BTreeInternal).unwrap(),
                        meta.owner,
                    )
                    .unwrap()
                    .first_child;
                }
                let page = storage.buffer.read_btree_page(target).unwrap();
                let mut node = decode_leaf_owned(
                    &meta.spec,
                    page.page().single_payload(PageType::BTreeLeaf).unwrap(),
                    meta.owner,
                )
                .unwrap();
                node.next_leaf = Some(generation_ref(node.next_leaf.unwrap(), 1));
                (
                    target,
                    PageType::BTreeLeaf,
                    encode_leaf_generation(&meta.spec, &node, meta.owner, target.generation())
                        .unwrap(),
                )
            };
            let mut page = storage.buffer.write_btree_page(target).unwrap();
            page.page_mut()
                .replace_single_payload(kind, &payload)
                .unwrap();
            drop(page);
            assert!(matches!(
                storage.inspect_index_reclaim(),
                Err(StorageError::Index(IndexError::GenerationMismatch { .. }))
                    | Err(StorageError::Index(IndexError::InvalidLeafChain(_)))
            ));
            if case != "leaf" {
                assert!(matches!(
                    storage.btree().lookup(
                        index.handle,
                        &ScalarValue::Text("0000".to_owned() + &"x".repeat(1000))
                    ),
                    Err(StorageError::Index(IndexError::GenerationMismatch { .. }))
                ));
            }
        }
        drop(storage);
        cleanup(&path);
    }
}

pub(super) fn generation_crash_child(case: &str, path: &std::path::Path) {
    let mut storage = HeapStorage::open(path, indexed_table()).unwrap();
    let mut tx = storage.begin_transaction().unwrap();
    if case == "page-generation-reserve" {
        tx.reserve_page_generation().unwrap();
        panic!("reservation hook did not terminate");
    }
    if case == "page-generation-rollback" {
        storage
            .create_named_index_in(&mut tx, IndexName::new("reused").unwrap(), ColumnId(1))
            .unwrap();
        tx.rollback().unwrap();
        return;
    }
    let old = crash_test::without_crash(|| {
        let old = storage
            .create_named_index_in(&mut tx, IndexName::new("reused").unwrap(), ColumnId(1))
            .unwrap();
        tx.rollback().unwrap();
        old
    });
    drop(tx);
    let mut tx = storage.begin_transaction().unwrap();
    let new = storage
        .create_named_index_in(&mut tx, IndexName::new("reused").unwrap(), ColumnId(1))
        .unwrap();
    assert_eq!(old.id, new.id);
    assert_eq!(
        old.handle.meta_page.page_id(),
        new.handle.meta_page.page_id()
    );
    assert_ne!(
        old.handle.meta_page.generation(),
        new.handle.meta_page.generation()
    );
    crash_test::maybe_crash(TestCrashPoint::GenerationReuseBeforeCommit);
    if crash_test::is_enabled(TestCrashPoint::GenerationReuseAfterFlush) {
        storage.buffer.flush_all().unwrap();
        crash_test::maybe_crash(TestCrashPoint::GenerationReuseAfterFlush);
    }
    tx.commit().unwrap();
    crash_test::maybe_crash(TestCrashPoint::CommittedWithoutDataFlush);
}

#[test]
fn generation_process_crash_reservation_and_reuse_matrix() {
    for (case, point, winner) in [
        (
            "page-generation-reserve",
            TestCrashPoint::PageGenerationReserved,
            false,
        ),
        (
            "page-generation-rollback",
            TestCrashPoint::RollbackAfterAbortSync,
            false,
        ),
        (
            "page-generation-rollback",
            TestCrashPoint::RollbackAfterPageUndo,
            false,
        ),
        (
            "page-generation-rollback",
            TestCrashPoint::RollbackAfterTrailingRemoval,
            false,
        ),
        (
            "page-generation-reuse",
            TestCrashPoint::BTreeAfterFirstPageUpdateLog,
            false,
        ),
        (
            "page-generation-reuse",
            TestCrashPoint::BTreeAfterFirstPagePublish,
            false,
        ),
        (
            "page-generation-reuse",
            TestCrashPoint::GenerationReuseBeforeCommit,
            false,
        ),
        (
            "page-generation-reuse",
            TestCrashPoint::GenerationReuseAfterFlush,
            false,
        ),
        (
            "page-generation-reuse",
            TestCrashPoint::CommitAfterWalSync,
            true,
        ),
        (
            "page-generation-reuse",
            TestCrashPoint::CommittedWithoutDataFlush,
            true,
        ),
    ] {
        let path = test_path(&format!("round10-crash-{case}-{}", point.as_str()));
        cleanup(&path);
        let mut storage = HeapStorage::create(&path, indexed_table()).unwrap();
        storage.insert(&indexed_rows()[0]).unwrap();
        storage.checkpoint().unwrap();
        storage.close().unwrap();
        spawn_crash_child(&path, case, point);
        let max_reserved = WalManager::open(wal_path(&path))
            .unwrap()
            .scan()
            .unwrap()
            .iter()
            .filter(|r| matches!(r.kind, WalRecordKind::PageGenerationReservation))
            .map(|r| r.lsn.0)
            .max()
            .unwrap();
        for pass in 0..3 {
            let mut storage = HeapStorage::open(&path, indexed_table()).unwrap();
            assert_eq!(storage.indexes().len(), usize::from(winner));
            assert_eq!(storage.scan().unwrap().len(), 1);
            storage.inspect_index_reclaim().unwrap();
            if winner {
                let handle = storage.indexes()[0].handle;
                assert_eq!(
                    storage
                        .btree()
                        .lookup(handle, &ScalarValue::UInt64(1))
                        .unwrap()
                        .len(),
                    1
                );
            }
            let mut tx = storage.begin_transaction().unwrap();
            assert!(tx.reserve_page_generation().unwrap().0 > max_reserved);
            tx.rollback().unwrap();
            drop(tx);
            if pass == 1 {
                storage.checkpoint().unwrap();
            }
            storage.close().unwrap();
        }
        eprintln!(
            "ROUND10_CRASH {case}/{} winner={winner} repeated_reopen=3",
            point.as_str()
        );
        cleanup(&path);
    }
}

#[test]
fn generation_legacy_v2_remains_read_write_and_explicitly_unreclaimable() {
    let path = test_path("round10-legacy-v2");
    cleanup(&path);
    let mut storage = HeapStorage::create(&path, indexed_table()).unwrap();
    let index = storage.create_index(ColumnId(3)).unwrap();
    let mut meta = storage.btree().read_meta(index.handle).unwrap();
    storage.checkpoint().unwrap();
    storage.close().unwrap();
    let mut pages = PageManager::open(&path).unwrap();
    let root = meta.root_page.page_id();
    meta.generation = None;
    meta.root_page = BTreePageRef::Legacy(root);
    let mut page = pages.read_page(index.handle.meta_page.page_id()).unwrap();
    page.replace_single_payload(
        PageType::BTreeMeta,
        &netbadb_index::encode_meta(&meta).unwrap(),
    )
    .unwrap();
    pages.write_page(&page).unwrap();
    let mut page = pages.read_page(root).unwrap();
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
    pages.write_page(&page).unwrap();
    let mut page = pages.read_page(PageId(1)).unwrap();
    let mut node =
        decode_index_catalog(page.single_payload(PageType::IndexCatalog).unwrap()).unwrap();
    node.entries[0].definition.handle.meta_page =
        BTreePageRef::Legacy(index.handle.meta_page.page_id());
    let mut bytes = encode_index_catalog(&node).unwrap();
    bytes.truncate(96);
    bytes[4..6].copy_from_slice(&6_u16.to_le_bytes());
    page.replace_single_payload(PageType::IndexCatalog, &bytes)
        .unwrap();
    pages.write_page(&page).unwrap();
    pages.sync().unwrap();
    drop(pages);
    let mut storage = HeapStorage::open(&path, indexed_table()).unwrap();
    let legacy = storage.indexes()[0].clone();
    assert_eq!(legacy.handle.meta_page.generation(), None);
    assert_eq!(legacy.handle.owner, Some(index.id));
    let mut rows = Vec::new();
    for id in 0..30 {
        rows.push(
            storage
                .insert(&[
                    ScalarValue::UInt64(id),
                    ScalarValue::Null,
                    ScalarValue::Text(format!("{id:04}{}", "x".repeat(1000))),
                ])
                .unwrap(),
        );
    }
    assert!(storage.btree().height(legacy.handle).unwrap() >= 3);
    for row in rows {
        storage.delete(row).unwrap();
    }
    storage.vacuum().unwrap();
    storage.drop_index(index.id).unwrap();
    storage.compact_index_catalog().unwrap();
    let report = storage.inspect_index_reclaim().unwrap();
    assert_eq!(report.legacy_unreclaimable_indexes, 1);
    assert!(report.allocations.is_empty());
    assert_eq!(report.pending[0].meta_page, legacy.handle.meta_page);
    storage.checkpoint().unwrap();
    storage.close().unwrap();
    let mut storage = HeapStorage::open(&path, indexed_table()).unwrap();
    assert_eq!(storage.inspect_index_reclaim().unwrap(), report);
    storage.close().unwrap();
    cleanup(&path);
}

#[test]
fn generation_recovery_rejects_mismatched_disk_even_with_a_newer_page_lsn() {
    for newer_lsn in [false, true] {
        let path = test_path(&format!("round10-redo-mismatch-{newer_lsn}"));
        cleanup(&path);
        let mut storage = HeapStorage::create(&path, indexed_table()).unwrap();
        let index = storage.create_index(ColumnId(1)).unwrap();
        let mut meta = storage.btree().read_meta(index.handle).unwrap();
        storage.close().unwrap();
        let mut pages = PageManager::open(&path).unwrap();
        let mut page = pages.read_page(index.handle.meta_page.page_id()).unwrap();
        meta.generation = Some(PageGeneration(meta.generation.unwrap().0 + 1));
        page.replace_single_payload(
            PageType::BTreeMeta,
            &netbadb_index::encode_meta(&meta).unwrap(),
        )
        .unwrap();
        if newer_lsn {
            page.set_page_lsn(Lsn(u64::MAX));
        }
        pages.write_page(&page).unwrap();
        pages.sync().unwrap();
        drop(pages);
        assert!(matches!(
            HeapStorage::open(&path, indexed_table()),
            Err(StorageError::Index(IndexError::GenerationMismatch { .. }))
        ));
        let mut pages = PageManager::open(&path).unwrap();
        assert_eq!(
            pages
                .read_page(index.handle.meta_page.page_id())
                .unwrap()
                .bytes(),
            page.bytes()
        );
        cleanup(&path);
    }
}

#[test]
fn generation_old_undo_cannot_remove_or_restore_over_new_allocation() {
    let path = test_path("round10-undo-mismatch");
    cleanup(&path);
    let mut storage = HeapStorage::create(&path, indexed_table()).unwrap();
    let mut tx = storage.begin_transaction().unwrap();
    let old = storage
        .create_named_index_in(&mut tx, IndexName::new("reused").unwrap(), ColumnId(1))
        .unwrap();
    let old_page = storage
        .buffer
        .read_btree_page(old.handle.meta_page)
        .unwrap()
        .page()
        .clone();
    tx.rollback().unwrap();
    drop(tx);
    let new = storage.create_index(ColumnId(1)).unwrap();
    for before in [[0; crate::PAGE_SIZE], *old_page.bytes()] {
        assert!(matches!(
            storage.buffer.undo_page_update(
                old.handle.meta_page.page_id(),
                &before,
                old.handle.meta_page.generation()
            ),
            Err(StorageError::Index(IndexError::GenerationMismatch { .. }))
        ));
        assert_eq!(storage.btree().height(new.handle).unwrap(), 1);
    }
    storage.close().unwrap();
    cleanup(&path);
}

#[test]
fn generation_overflow_and_failed_sync_never_publish_a_reference() {
    let path = test_path("round10-generation-errors");
    cleanup(&path);
    let mut storage = HeapStorage::create(&path, indexed_table()).unwrap();
    let mut tx = storage.begin_transaction().unwrap();
    storage
        .transactions
        .wal()
        .borrow_mut()
        .inject_flush_failure();
    assert!(tx.reserve_page_generation().is_err());
    let reserved = storage.wal_records().unwrap().last().unwrap().lsn.0;
    assert!(tx.reserve_page_generation().unwrap().0 > reserved);
    tx.rollback().unwrap();
    drop(tx);
    let mut tx = storage.begin_transaction().unwrap();
    storage
        .transactions
        .wal()
        .borrow_mut()
        .inject_generation_exhaustion();
    assert!(matches!(
        tx.reserve_page_generation(),
        Err(StorageError::Index(IndexError::PageGenerationExhausted))
    ));
    drop(tx);
    drop(storage);
    cleanup(&path);
}

#[test]
fn generation_maximum_key_remains_splittable_at_arbitrary_height() {
    let path = test_path("round10-key-split");
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
    let mut storage = HeapStorage::create_with_buffer_pool_size(&path, schema.clone(), 1).unwrap();
    let index = storage.create_index(ColumnId(1)).unwrap();
    for id in 0..8 {
        storage
            .insert(&[ScalarValue::Text(format!("{id}{}", "x".repeat(3980)))])
            .unwrap();
    }
    assert!(storage.btree().height(index.handle).unwrap() >= 3);
    storage.close().unwrap();
    let mut storage = HeapStorage::open(&path, schema).unwrap();
    assert_eq!(storage.scan().unwrap().len(), 8);
    storage.inspect_index_reclaim().unwrap();
    storage.close().unwrap();
    cleanup(&path);
}
