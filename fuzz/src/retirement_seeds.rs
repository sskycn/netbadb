// Included by generate_wal_corpus. Real registered merge/split histories.
fn write_retirement_seeds(output: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let codec = output.parent().unwrap().join("btree_decode");
    let marker = netbadb_index::RetiredBTreePage {
        owner: netbadb_types::IndexId(1),
        page_ref: netbadb_types::PageRef {
            page_id: PageId(4),
            generation: netbadb_types::PageGeneration(80),
        },
    };
    let payload = netbadb_index::encode_retired_btree(marker)?;
    write_btree_seed(&codec, "round14-valid-retired-marker", 1, &payload)?;
    for (name, offset) in [
        ("zero-owner", 8),
        ("zero-generation", 16),
        ("zero-page", 24),
    ] {
        let mut bad = payload.clone();
        bad[offset..offset + 8].fill(0);
        write_btree_seed(&codec, &format!("round14-marker-{name}"), 1, &bad)?;
    }
    for (name, offset) in [("magic", 0), ("version", 4), ("reserved", 6)] {
        let mut bad = payload.clone();
        bad[offset] ^= 0x80;
        write_btree_seed(&codec, &format!("round14-marker-{name}"), 1, &bad)?;
    }
    write_btree_seed(&codec, "round14-marker-truncated", 1, &payload[..31])?;
    let mut extra = payload.clone();
    extra.push(0);
    write_btree_seed(&codec, "round14-marker-extra", 1, &extra)?;
    for different in [false, true] {
        let path = std::env::temp_dir().join(format!(
            "netbadb-round14-seed-{}-{different}",
            std::process::id()
        ));
        let mut storage = HeapStorage::create_with_buffer_pool_size(&path, fuzz_table(), 512)?;
        let index = storage.create_index(ColumnId(1))?;
        let mut n = 0;
        while storage.btree().height(index.handle)? == 1 {
            storage.btree().insert(
                index.handle,
                ScalarValue::UInt64(n),
                RowId {
                    page: PageId(2),
                    slot: n as u16,
                    generation: 1,
                },
            )?;
            n += 1;
        }
        let original_count = n;
        let active_disk = loop {
            storage.checkpoint()?;
            let disk = std::fs::read(&path)?;
            n -= 1;
            storage.btree().delete(
                index.handle,
                ScalarValue::UInt64(n),
                RowId {
                    page: PageId(2),
                    slot: n as u16,
                    generation: 1,
                },
            )?;
            if storage.btree().height(index.handle)? == 1 {
                break disk;
            }
        };
        assert_eq!(storage.btree().height(index.handle)?, 1);
        assert_eq!(storage.inspect_index_reclaim()?.retired_marker_pages, 2);
        if !different {
            retirement_snapshot_pair(output, "retirement", &active_disk, &path)?;
        }
        // Flush, without checkpoint: retain retirement PageUpdates in the WAL
        // followed by a transition from the same allocation's marker state.
        storage.flush()?;
        let mut marker_disk = std::fs::read(&path)?;
        if different {
            storage.drop_index(index.id)?;
            let other =
                storage.create_named_index(netbadb_types::IndexName::new("other")?, ColumnId(1))?;
            // Y's meta/root consume the two ordinary X pages first. Its next
            // split consumes X's two independent markers, testing other-owner
            // authority after the catalog no longer retains X's root.
            for number in 0..original_count - 1 {
                storage.btree().insert(
                    other.handle,
                    ScalarValue::UInt64(number),
                    RowId {
                        page: PageId(2),
                        slot: number as u16,
                        generation: 1,
                    },
                )?;
            }
            assert_eq!(storage.btree().height(other.handle)?, 1);
            storage.checkpoint()?;
            marker_disk = std::fs::read(&path)?;
            let number = original_count - 1;
            storage.btree().insert(
                other.handle,
                ScalarValue::UInt64(number),
                RowId {
                    page: PageId(2),
                    slot: number as u16,
                    generation: 1,
                },
            )?;
        } else {
            for number in n..original_count {
                storage.btree().insert(
                    index.handle,
                    ScalarValue::UInt64(number),
                    RowId {
                        page: PageId(2),
                        slot: number as u16,
                        generation: 1,
                    },
                )?;
            }
        }
        let name = if different {
            "different-owner"
        } else {
            "same-owner"
        };
        retirement_snapshot_pair(output, name, &marker_disk, &path)?;
        if !different {
            let mut wal = WalManager::open(wal_path(&path))?;
            let records = wal.scan()?;
            let record = records
                .iter()
                .find(|r| matches!(r.kind, WalRecordKind::PageAllocationTransition { .. }))
                .unwrap();
            let start =
                (record.lsn.0 - wal.base_lsn().0) as usize + netbadb_storage::WAL_HEADER_SIZE;
            let mut bad = std::fs::read(wal.path())?;
            bad[start + 48 + 100] ^= 1; // nested marker page CRC; recompute outer CRC
            bad[start + 12..start + 16].fill(0);
            let crc = crc32c::crc32c(&bad[start..start + 8240]);
            bad[start + 12..start + 16].copy_from_slice(&crc.to_le_bytes());
            write_tail_snapshot(output, "round14-corrupt-marker-before", &marker_disk, &bad)?;
            if let WalRecordKind::PageAllocationTransition { page_id, after, .. } = &record.kind {
                let new = Page::from_bytes(*page_id, **after);
                let generation = netbadb_index::btree_page_generation(
                    new.read_record(netbadb_types::SlotId(0))?,
                )?
                .unwrap();
                let unrelated =
                    transition_leaf(*page_id, generation.0 + 1, index.id, record.lsn, 0)?;
                let mut third = marker_disk.clone();
                let offset = page_id.0 as usize * 4096;
                third[offset..offset + 4096].copy_from_slice(unrelated.bytes());
                write_tail_snapshot(
                    output,
                    "round14-marker-third-generation",
                    &third,
                    &std::fs::read(wal.path())?,
                )?;
            }
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

fn retirement_snapshot_pair(
    output: &Path,
    name: &str,
    disk: &[u8],
    path: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut wal = WalManager::open(wal_path(path))?;
    let records = wal.scan()?;
    let last = records.last().unwrap();
    assert!(matches!(last.kind, WalRecordKind::Commit));
    let bytes = std::fs::read(wal.path())?;
    let end = (last.lsn.0 - wal.base_lsn().0) as usize + netbadb_storage::WAL_HEADER_SIZE;
    write_tail_snapshot(
        output,
        &format!("round14-valid-{name}-winner"),
        disk,
        &bytes,
    )?;
    write_tail_snapshot(
        output,
        &format!("round14-valid-{name}-loser"),
        disk,
        &bytes[..end],
    )?;
    Ok(())
}
