// Reviewed synthetic historical allocation histories; actual maintenance and
// subsequent same-owner splitting use production APIs. Real pre-Round14 merge
// images and process termination are covered by the storage unit-test fixtures.
fn write_adoption_seeds(output: &Path) -> Result<(), Box<dyn std::error::Error>> {
    for count in [2_u64, 81] {
        let path = std::env::temp_dir().join(format!(
            "netbadb-adoption-seed-{}-{count}",
            std::process::id()
        ));
        let mut storage = HeapStorage::create(&path, fuzz_table())?;
        let index = storage.create_index(ColumnId(1))?;
        storage.close()?;
        let mut heap = std::fs::read(&path)?;
        let mut disk = PageManager::open(&path)?;
        let meta_page = disk.read_page(index.handle.meta_page.page_id())?;
        let meta = netbadb_index::decode_meta(meta_page.read_record(netbadb_types::SlotId(0))?)?;
        let root = disk.read_page(meta.root_page.page_id())?;
        drop(disk);
        let mut wal = WalManager::open(wal_path(&path))?;
        let tx = wal.next_txn_id();
        let mut previous = wal.append(tx, None, WalRecordKind::Begin)?;
        // Fill the existing root to its exact codec capacity. One later public
        // insertion must split and consume both adopted marker allocations.
        let mut leaf = LeafNode::empty();
        loop {
            let n = leaf.entries.len() as u16;
            leaf.entries.push(IndexEntry {
                key: ScalarValue::UInt64(u64::from(n)),
                row_id: RowId {
                    page: PageId(2),
                    slot: n,
                    generation: 1,
                },
            });
            if netbadb_index::encode_leaf_generation(
                &meta.spec,
                &leaf,
                meta.owner,
                meta.root_page.generation(),
            )?
            .len()
                > netbadb_storage::PAGE_SIZE
                    - netbadb_storage::PAGE_HEADER_SIZE
                    - netbadb_storage::SLOT_SIZE
            {
                leaf.entries.pop();
                break;
            }
        }
        let next_key = leaf.entries.len() as u16;
        let mut full_root = adoption_seed_page(
            root.id,
            PageType::BTreeLeaf,
            &netbadb_index::encode_leaf_generation(
                &meta.spec,
                &leaf,
                meta.owner,
                meta.root_page.generation(),
            )?,
        )?;
        adoption_seed_update(&mut wal, tx, &mut previous, &root, &mut full_root)?;
        install_adoption_seed_page(&mut heap, &full_root);
        let mut first_orphan = None;
        for _ in 0..count {
            let reservation =
                wal.append(tx, Some(previous), WalRecordKind::PageGenerationReservation)?;
            previous = reservation;
            let id = PageId((heap.len() / netbadb_storage::PAGE_SIZE) as u64);
            let generation = netbadb_types::PageGeneration(reservation.0);
            first_orphan.get_or_insert(netbadb_index::BTreePageRef::Allocated(
                netbadb_types::PageRef {
                    page_id: id,
                    generation,
                },
            ));
            let mut page = adoption_seed_page(
                id,
                PageType::BTreeLeaf,
                &netbadb_index::encode_leaf_generation(
                    &meta.spec,
                    &LeafNode::empty(),
                    meta.owner,
                    Some(generation),
                )?,
            )?;
            adoption_seed_update(&mut wal, tx, &mut previous, &Page::zero(id), &mut page)?;
            heap.extend_from_slice(page.bytes());
        }
        // Old structural WAL explicitly points at a future adoption candidate,
        // then restores today's root before commit. Retain this whole generation
        // in the horizon envelope to exercise actual two-slot selection.
        let mut earlier_meta = meta.clone();
        earlier_meta.root_page = first_orphan.unwrap();
        let mut earlier = adoption_seed_page(
            meta_page.id,
            PageType::BTreeMeta,
            &netbadb_index::encode_meta(&earlier_meta)?,
        )?;
        adoption_seed_update(&mut wal, tx, &mut previous, &meta_page, &mut earlier)?;
        let mut current = meta_page;
        adoption_seed_update(&mut wal, tx, &mut previous, &earlier, &mut current)?;
        install_adoption_seed_page(&mut heap, &current);
        let commit = wal.append(tx, Some(previous), WalRecordKind::Commit)?;
        wal.flush_through(commit)?;
        let old_wal = std::fs::read(wal.path())?;
        drop(wal);
        std::fs::write(&path, &heap)?;
        let mut storage = HeapStorage::open_with_buffer_pool_size(&path, fuzz_table(), 128)?;
        assert_eq!(storage.inspect_index_reclaim()?.active_orphan_pages, count);
        let before = std::fs::read(&path)?;
        assert_eq!(storage.adopt_historical_btree_orphans()?.adopted, count);
        let mut wal = WalManager::open(wal_path(&path))?;
        let records = wal.scan()?;
        let bytes = std::fs::read(wal.path())?;
        let last = records.last().unwrap();
        let commit_offset =
            (last.lsn.0 - wal.base_lsn().0) as usize + netbadb_storage::WAL_HEADER_SIZE;
        assert!(matches!(last.kind, WalRecordKind::Commit));
        drop(wal);
        if count == 2 {
            write_adoption_snapshot(
                output,
                "round15-adoption-winner",
                &before,
                &bytes,
                None,
                (0, 2),
            )?;
            write_adoption_snapshot(
                output,
                "round15-adoption-loser",
                &before,
                &bytes[..commit_offset],
                None,
                (2, 0),
            )?;
            write_adoption_snapshot(
                output,
                "round15-old-structural-horizon",
                &before,
                &bytes,
                Some(&old_wal),
                (0, 2),
            )?;
            storage.btree().insert(
                index.handle,
                ScalarValue::UInt64(u64::from(next_key)),
                RowId {
                    page: PageId(2),
                    slot: next_key,
                    generation: 1,
                },
            )?;
            let wal = WalManager::open(wal_path(&path))?;
            write_adoption_snapshot(
                output,
                "round15-adoption-reuse-winner",
                &before,
                &std::fs::read(wal.path())?,
                None,
                (0, 0),
            )?;
            assert_eq!(
                storage.inspect_index_reclaim()?.database_pages as usize
                    * netbadb_storage::PAGE_SIZE,
                before.len()
            );
        } else {
            let mut partial = before;
            for record in records
                .iter()
                .filter(|r| matches!(r.kind, WalRecordKind::PageUpdate { .. }))
                .take(40)
            {
                if let WalRecordKind::PageUpdate { page_id, after, .. } = &record.kind {
                    install_adoption_seed_page(&mut partial, &Page::from_bytes(*page_id, **after));
                }
            }
            write_adoption_snapshot(
                output,
                "round15-adoption-81-partial-winner",
                &partial,
                &bytes,
                None,
                (0, 81),
            )?;
        }
        storage.close()?;
        for file in [
            &path,
            &wal_path(&path),
            &wal_alternate_path(wal_path(&path)),
            &netbadb_storage::txn_status_path(&path),
        ] {
            if file.exists() {
                std::fs::remove_file(file)?;
            }
        }
    }
    Ok(())
}

fn adoption_seed_update(
    wal: &mut WalManager,
    tx: TxnId,
    previous: &mut netbadb_types::Lsn,
    before: &Page,
    after: &mut Page,
) -> Result<(), Box<dyn std::error::Error>> {
    adoption_seed_lsn(after, wal.next_lsn());
    *previous = wal.append(
        tx,
        Some(*previous),
        WalRecordKind::PageUpdate {
            page_id: before.id,
            before: Box::new(*before.bytes()),
            after: Box::new(*after.bytes()),
        },
    )?;
    Ok(())
}

fn adoption_seed_page(
    id: PageId,
    kind: PageType,
    payload: &[u8],
) -> Result<Page, Box<dyn std::error::Error>> {
    let mut page = Page::new(id, PageType::Heap);
    page.insert_record(payload)?;
    let mut bytes = *page.bytes();
    bytes[6] = Page::new(id, kind).bytes()[6];
    page = Page::from_bytes(id, bytes);
    adoption_seed_lsn(&mut page, netbadb_types::Lsn(0));
    Ok(page)
}

fn adoption_seed_lsn(page: &mut Page, lsn: netbadb_types::Lsn) {
    // Fixture-only Page v5 envelope, identical to existing transition seeds.
    let mut bytes = *page.bytes();
    bytes[16..24].copy_from_slice(&lsn.0.to_le_bytes());
    bytes[24..28].fill(0);
    let crc = crc32c::crc32c_append(crc32c::crc32c(&page.id.0.to_le_bytes()), &bytes);
    bytes[24..28].copy_from_slice(&crc.to_le_bytes());
    *page = Page::from_bytes(page.id, bytes);
}

fn install_adoption_seed_page(heap: &mut [u8], page: &Page) {
    let start = page.id.0 as usize * netbadb_storage::PAGE_SIZE;
    heap[start..start + netbadb_storage::PAGE_SIZE].copy_from_slice(page.bytes());
}

fn write_adoption_snapshot(
    output: &Path,
    name: &str,
    heap: &[u8],
    wal: &[u8],
    older: Option<&[u8]>,
    expected: (u64, u64),
) -> Result<(), Box<dyn std::error::Error>> {
    // NBRH is a fuzz-only extension: three u32 lengths followed by heap,
    // selected generation and an optional older WAL slot. NBRF stays unchanged.
    let mut bytes = b"NBRH".to_vec();
    for len in [heap.len(), wal.len(), older.map_or(0, <[u8]>::len)] {
        bytes.extend_from_slice(&u32::try_from(len)?.to_le_bytes());
    }
    bytes.extend_from_slice(heap);
    bytes.extend_from_slice(wal);
    bytes.extend_from_slice(older.unwrap_or_default());
    assert!(bytes.len() <= 2 * 1024 * 1024);
    let probe = std::env::temp_dir().join(format!(
        "netbadb-adoption-probe-{}-{name}",
        std::process::id()
    ));
    HeapStorage::create(&probe, fuzz_table())?.close()?;
    std::fs::write(&probe, heap)?;
    std::fs::write(wal_path(&probe), wal)?;
    if let Some(old) = older {
        std::fs::write(wal_alternate_path(wal_path(&probe)), old)?;
    }
    for _ in 0..3 {
        let mut storage = HeapStorage::open(&probe, fuzz_table())?;
        let report = storage.inspect_index_reclaim()?;
        assert_eq!(
            (report.active_orphan_pages, report.retired_marker_pages),
            expected
        );
        assert_eq!(
            storage.inspect_reusable_pages()?.candidates.len() as u64,
            expected.1
        );
        assert!(report.pending.is_empty());
        assert_eq!(
            report
                .allocations
                .iter()
                .filter(|p| p.reachable == Some(true))
                .count(),
            if name.contains("reuse") { 4 } else { 2 }
        );
        storage.close()?;
    }
    for file in [
        &probe,
        &wal_path(&probe),
        &wal_alternate_path(wal_path(&probe)),
        &netbadb_storage::txn_status_path(&probe),
    ] {
        if file.exists() {
            std::fs::remove_file(file)?;
        }
    }
    std::fs::write(output.join(name), bytes)?;
    Ok(())
}
