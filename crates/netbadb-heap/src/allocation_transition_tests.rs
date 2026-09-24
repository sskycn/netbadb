use super::*;
use crate::recovery::RecoveryManager;
use crate::wal::page_update_kind;
use crate::{PAGE_SIZE, PageManager, WalManager, WalRecordKind, wal_path};
use netbadb_index::{IndexEntry, LeafNode, encode_leaf_generation};
use netbadb_types::{PageGeneration, PageId, RowId, ScalarValue, TxnId};

fn path(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("netbadb-round13-wal-{name}-{}", std::process::id()))
}
fn cleanup(path: &std::path::Path) {
    for file in [
        path.to_owned(),
        wal_path(path),
        crate::wal_alternate_path(wal_path(path)),
    ] {
        let _ = std::fs::remove_file(file);
    }
}
fn leaf(generation: u64, owner: u64, count: u64) -> Page {
    let mut page = Page::new(PageId(1), PageType::BTreeLeaf);
    let payload = encode_leaf_generation(
        &IndexSpec {
            data_type: SemanticType::physical(PhysicalType::UInt64),
            nullable: false,
        },
        &LeafNode {
            next_leaf: None,
            entries: (0..count)
                .map(|n| IndexEntry {
                    key: ScalarValue::UInt64(n),
                    row_id: RowId {
                        page: PageId(99),
                        slot: n as u16,
                        generation: 1,
                    },
                })
                .collect(),
        },
        Some(IndexId(owner)),
        Some(PageGeneration(generation)),
    )
    .unwrap();
    page.initialize_single_payload(PageType::BTreeLeaf, &payload)
        .unwrap();
    page
}
fn transition(before: &Page, after: &Page) -> WalRecordKind {
    WalRecordKind::PageAllocationTransition {
        page_id: after.id,
        before: Box::new(*before.bytes()),
        after: Box::new(*after.bytes()),
    }
}

#[test]
fn retirement_p0_update_identity_and_marker_crc_guards() {
    let path = path("round14-update-guards");
    cleanup(&path);
    let mut wal = WalManager::create(wal_path(&path)).unwrap();
    let begin = wal.append(TxnId(1), None, WalRecordKind::Begin).unwrap();
    let old = leaf(2, 1, 2);
    let mut marker = old.clone();
    let payload = netbadb_index::encode_retired_btree(netbadb_index::RetiredBTreePage {
        owner: IndexId(1),
        page_ref: PageRef {
            page_id: old.id,
            generation: PageGeneration(2),
        },
    })
    .unwrap();
    marker
        .replace_single_payload(PageType::BTreeLeaf, &payload)
        .unwrap();
    marker.set_page_lsn(wal.next_lsn());
    for mut bad in [leaf(3, 1, 2), leaf(2, 2, 2)] {
        bad.set_page_lsn(wal.next_lsn());
        assert!(
            wal.append(TxnId(1), Some(begin), page_update_kind(&old, &bad))
                .is_err()
        );
    }
    let logged = wal
        .append(TxnId(1), Some(begin), page_update_kind(&old, &marker))
        .unwrap();
    let mut repeat = marker.clone();
    repeat.set_page_lsn(wal.next_lsn());
    assert!(
        wal.append(TxnId(1), Some(logged), page_update_kind(&marker, &repeat))
            .is_err()
    );
    let mut wrong_id = marker.clone();
    wrong_id.id = PageId(2);
    assert!(wrong_id.allocation_generation().is_err());
    let mut bytes = *marker.bytes();
    bytes[100] ^= 1;
    assert!(Page::from_bytes(marker.id, bytes).validated().is_err());
    drop(wal);
    cleanup(&path);
}

#[test]
fn retirement_transition_requires_marker_for_same_owner_and_active_destination() {
    let active = leaf(2, 1, 1);
    let mut marker = active.clone();
    let payload = netbadb_index::encode_retired_btree(netbadb_index::RetiredBTreePage {
        owner: IndexId(1),
        page_ref: PageRef {
            page_id: active.id,
            generation: PageGeneration(2),
        },
    })
    .unwrap();
    marker
        .replace_single_payload(PageType::BTreeLeaf, &payload)
        .unwrap();
    for owner in [1, 2] {
        let new = leaf(200, owner, 2);
        validate(&marker, &new).unwrap();
        assert!(undo(&new, &marker, &new).unwrap());
        assert!(!undo(&marker, &marker, &new).unwrap());
        assert!(redo(&leaf(300, owner, 2), &marker, &new, Lsn(250)).is_err());
    }
    assert!(validate(&active, &leaf(200, 1, 1)).is_err());
    assert!(validate(&marker, &active).is_err());
    assert!(validate(&active, &marker).is_err());
    assert!(validate(&marker, &marker).is_err());
}
fn recover(path: &std::path::Path, limit: Option<usize>) -> Result<(), crate::RecoveryError> {
    let mut pages = PageManager::open(path)?;
    let (mut wal, records, tail) = WalManager::open_for_recovery(wal_path(path))?;
    if let Some(limit) = limit {
        RecoveryManager::recover_with_operation_limit(&mut pages, &mut wal, &records, limit)?;
    } else {
        RecoveryManager::recover(&mut pages, &mut wal, &records, tail)?;
    }
    Ok(())
}

#[test]
fn transition_codec_is_distinct_strict_bounded_and_legacy_compatible() {
    let path = path("codec");
    cleanup(&path);
    let mut wal = WalManager::create(wal_path(&path)).unwrap();
    let begin = wal.append(TxnId(1), None, WalRecordKind::Begin).unwrap();
    let reserve = wal
        .append(
            TxnId(1),
            Some(begin),
            WalRecordKind::PageGenerationReservation,
        )
        .unwrap();
    let old = leaf(2, 1, 1);
    let mut new = leaf(reserve.0, 2, 2);
    new.set_page_lsn(wal.next_lsn());
    assert!(
        wal.append(TxnId(1), Some(reserve), transition(&old, &new))
            .is_err()
    );
    wal.flush_through(reserve).unwrap();
    assert!(
        wal.append(TxnId(1), Some(reserve), page_update_kind(&old, &new))
            .is_err()
    );
    for invalid in [
        Page::zero(PageId(1)),
        Page::new(PageId(1), PageType::Heap),
        Page::new(PageId(1), PageType::IndexCatalog),
        leaf(2, 2, 0),
        leaf(reserve.0, 1, 0),
    ] {
        let mut invalid = invalid;
        invalid.set_page_lsn(wal.next_lsn());
        assert!(
            wal.append(TxnId(1), Some(reserve), transition(&old, &invalid))
                .is_err()
        );
    }
    for invalid in [
        Page::zero(PageId(1)),
        Page::new(PageId(1), PageType::Heap),
        Page::new(PageId(1), PageType::IndexCatalog),
    ] {
        assert!(
            wal.append(TxnId(1), Some(reserve), transition(&invalid, &new))
                .is_err()
        );
    }
    // Raw v1 and owner-tagged legacy v2 remain invalid on either side,
    // even though these are valid complete BTree pages with a correct CRC.
    for owner in [None, Some(IndexId(3))] {
        let payload = encode_leaf_generation(
            &IndexSpec {
                data_type: SemanticType::physical(PhysicalType::UInt64),
                nullable: false,
            },
            &LeafNode::empty(),
            owner,
            None,
        )
        .unwrap();
        let mut legacy = Page::new(PageId(1), PageType::BTreeLeaf);
        legacy
            .initialize_single_payload(PageType::BTreeLeaf, &payload)
            .unwrap();
        legacy.set_page_lsn(wal.next_lsn());
        assert!(
            wal.append(TxnId(1), Some(reserve), transition(&old, &legacy))
                .is_err()
        );
        assert!(
            wal.append(TxnId(1), Some(reserve), transition(&legacy, &new))
                .is_err()
        );
    }
    let lsn = wal
        .append(TxnId(1), Some(reserve), transition(&old, &new))
        .unwrap();
    wal.flush_through(lsn).unwrap();
    drop(wal);
    let bytes = std::fs::read(wal_path(&path)).unwrap();
    let offset = crate::WAL_HEADER_SIZE + lsn.0 as usize - 1;
    assert_eq!(&bytes[offset + 4..offset + 7], &[5, 0, 8]);
    let decoded = WalManager::open(wal_path(&path)).unwrap().scan().unwrap();
    assert_eq!(decoded.last().unwrap().kind, transition(&old, &new));
    for end in [offset + 7, offset + 40, offset + 100, bytes.len() - 1] {
        std::fs::write(wal_path(&path), &bytes[..end]).unwrap();
        assert!(WalManager::open(wal_path(&path)).is_err());
    }
    for version in [3_u16, 4, 6] {
        let mut invalid = bytes.clone();
        invalid[offset + 4..offset + 6].copy_from_slice(&version.to_le_bytes());
        invalid[offset + 12..offset + 16].fill(0);
        let crc = crc32c::crc32c(&invalid[offset..]);
        invalid[offset + 12..offset + 16].copy_from_slice(&crc.to_le_bytes());
        std::fs::write(wal_path(&path), &invalid).unwrap();
        assert!(WalManager::open(wal_path(&path)).is_err());
    }
    for at in [offset + 6, offset + 48 + 60, offset + 48 + PAGE_SIZE + 60] {
        let mut corrupt = bytes.clone();
        corrupt[at] ^= 128;
        corrupt[offset + 12..offset + 16].fill(0);
        let crc = crc32c::crc32c(&corrupt[offset..]);
        corrupt[offset + 12..offset + 16].copy_from_slice(&crc.to_le_bytes());
        std::fs::write(wal_path(&path), &corrupt).unwrap();
        assert!(WalManager::open(wal_path(&path)).is_err());
    }
    cleanup(&path);
}

#[test]
fn transition_two_state_redo_undo_is_generation_first() {
    let mut old = leaf(1, 1, 1);
    old.set_page_lsn(Lsn(9999));
    let mut new = leaf(2, 2, 2);
    new.set_page_lsn(Lsn(100));
    assert!(redo(&old, &old, &new, Lsn(100)).unwrap());
    assert!(!redo(&new, &old, &new, Lsn(100)).unwrap());
    assert!(undo(&new, &old, &new).unwrap());
    assert!(!undo(&old, &old, &new).unwrap());
    let third = leaf(3, 3, 0);
    assert!(redo(&third, &old, &new, Lsn(100)).is_err());
    assert!(undo(&third, &old, &new).is_err());
    let mut false_lsn = new.clone();
    false_lsn.set_page_lsn(Lsn(99));
    assert!(redo(&false_lsn, &old, &new, Lsn(100)).is_err());
}

#[test]
fn transition_no_checkpoint_winner_loser_and_interrupted_recovery() {
    for winner in [false, true] {
        for disk_state in 0..3 {
            for interruption in [None, Some(1), Some(3), Some(5)] {
                let path = path(&format!("recovery-{winner}-{disk_state}-{interruption:?}"));
                cleanup(&path);
                let mut pages = PageManager::create(&path).unwrap();
                pages.allocate_page().unwrap();
                let mut wal = WalManager::create(wal_path(&path)).unwrap();
                let b = wal.append(TxnId(1), None, WalRecordKind::Begin).unwrap();
                let r = wal
                    .append(TxnId(1), Some(b), WalRecordKind::PageGenerationReservation)
                    .unwrap();
                let mut old = leaf(r.0, 1, 1);
                old.set_page_lsn(wal.next_lsn());
                let u = wal
                    .append(
                        TxnId(1),
                        Some(r),
                        page_update_kind(&Page::zero(PageId(1)), &old),
                    )
                    .unwrap();
                let c = wal
                    .append(TxnId(1), Some(u), WalRecordKind::Commit)
                    .unwrap();
                wal.flush_through(c).unwrap();
                pages.write_page(&old).unwrap();
                pages.sync().unwrap();
                let b = wal.append(TxnId(2), None, WalRecordKind::Begin).unwrap();
                let r = wal
                    .append(TxnId(2), Some(b), WalRecordKind::PageGenerationReservation)
                    .unwrap();
                wal.flush_through(r).unwrap();
                let mut after = leaf(r.0, 2, 0);
                after.set_page_lsn(wal.next_lsn());
                let mut last = wal
                    .append(TxnId(2), Some(r), transition(&old, &after))
                    .unwrap();
                let initial_new = after.clone();
                for count in 1..=3 {
                    let mut update = leaf(r.0, 2, count);
                    update.set_page_lsn(wal.next_lsn());
                    last = wal
                        .append(TxnId(2), Some(last), page_update_kind(&after, &update))
                        .unwrap();
                    after = update;
                }
                if winner {
                    last = wal
                        .append(TxnId(2), Some(last), WalRecordKind::Commit)
                        .unwrap();
                }
                wal.flush_through(last).unwrap();
                if disk_state != 0 {
                    pages
                        .write_page(if disk_state == 1 {
                            &initial_new
                        } else {
                            &after
                        })
                        .unwrap();
                    pages.sync().unwrap();
                }
                drop(pages);
                drop(wal);
                if let Some(limit) = interruption {
                    let _ = recover(&path, Some(limit));
                }
                for _ in 0..3 {
                    recover(&path, None).unwrap();
                    let current = PageManager::open(&path)
                        .unwrap()
                        .read_page(PageId(1))
                        .unwrap();
                    assert_eq!(
                        current.bytes(),
                        if winner { after.bytes() } else { old.bytes() }
                    );
                }
                cleanup(&path);
            }
        }
    }
}

#[test]
fn transition_retained_multiple_incarnations_require_a_continuous_lineage() {
    for last_wins in [false, true] {
        for disk_owner in 1..=3 {
            let path = path(&format!("chain-{last_wins}-{disk_owner}"));
            cleanup(&path);
            let mut pages = PageManager::create(&path).unwrap();
            pages.allocate_page().unwrap();
            let mut wal = WalManager::create(wal_path(&path)).unwrap();
            let mut images = Vec::new();
            let mut before = Page::zero(PageId(1));
            for owner in 1..=3 {
                let tx = TxnId(owner);
                let begin = wal.append(tx, None, WalRecordKind::Begin).unwrap();
                let r = wal
                    .append(tx, Some(begin), WalRecordKind::PageGenerationReservation)
                    .unwrap();
                wal.flush_through(r).unwrap();
                let mut after = leaf(r.0, owner, owner);
                after.set_page_lsn(wal.next_lsn());
                let kind = if owner == 1 {
                    page_update_kind(&before, &after)
                } else {
                    transition(&before, &after)
                };
                let mut last = wal.append(tx, Some(r), kind).unwrap();
                if owner < 3 || last_wins {
                    last = wal.append(tx, Some(last), WalRecordKind::Commit).unwrap();
                }
                wal.flush_through(last).unwrap();
                before = after.clone();
                images.push(after);
            }
            pages.write_page(&images[disk_owner - 1]).unwrap();
            pages.sync().unwrap();
            drop(pages);
            drop(wal);
            for _ in 0..3 {
                recover(&path, None).unwrap();
                assert_eq!(
                    PageManager::open(&path)
                        .unwrap()
                        .read_page(PageId(1))
                        .unwrap()
                        .bytes(),
                    images[if last_wins { 2 } else { 1 }].bytes()
                );
            }
            cleanup(&path);
        }
    }
}
