use std::path::{Path, PathBuf};

use netbadb_index::{
    BTreeHandle, IndexCatalogEntry, IndexCatalogNode, IndexDefinition, IndexEntry, IndexSpec,
    IndexStatistics, InternalNode, InternalSeparator, LeafNode, MetaNode, TableStatistics,
    encode_index_catalog as encode_current_index_catalog, encode_internal, encode_leaf,
    encode_meta,
};
use netbadb_protocol::{
    ClientMessage, MAX_FRAME_PAYLOAD, ProtocolErrorCode, ServerMessage, WireResultColumn,
    WireTransactionState, encode_client_frame, encode_server_frame,
};
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_storage::{
    HeapStorage, Page, PageManager, PageType, WalManager, WalRecordKind, wal_alternate_path,
    wal_path,
};
use netbadb_types::{
    ColumnId, DatabaseTxnId, PageId, PhysicalType, RowId, ScalarValue, SemanticType, TableId, TxnId,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output = std::env::args_os().nth(1).map_or_else(
        || PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("corpus/wal_recovery"),
        PathBuf::from,
    );
    std::fs::create_dir_all(&output)?;
    std::fs::write(output.join("empty"), [])?;

    write_seed(&output, "valid-header", |_, _| Ok(()))?;
    write_seed(&output, "valid-begin", |wal, _| {
        wal.append(TxnId(1), None, WalRecordKind::Begin)?;
        Ok(())
    })?;
    write_seed(&output, "valid-begin-commit", |wal, _| {
        let begin = wal.append(TxnId(1), None, WalRecordKind::Begin)?;
        wal.append(TxnId(1), Some(begin), WalRecordKind::Commit)?;
        Ok(())
    })?;
    write_seed(&output, "valid-begin-prepare", |wal, _| {
        let begin = wal.append(TxnId(1), None, WalRecordKind::Begin)?;
        wal.append(
            TxnId(1),
            Some(begin),
            WalRecordKind::Prepare {
                database_txn_id: DatabaseTxnId(7),
            },
        )?;
        Ok(())
    })?;
    write_page_update_seed(&output)?;
    write_seed(&output, "truncated-final-record", |wal, path| {
        let begin = wal.append(TxnId(1), None, WalRecordKind::Begin)?;
        let commit = wal.append(TxnId(1), Some(begin), WalRecordKind::Commit)?;
        wal.flush_through(commit)?;
        let truncated_len = std::fs::metadata(path)?.len() - 20;
        std::fs::OpenOptions::new()
            .write(true)
            .open(path)?
            .set_len(truncated_len)?;
        Ok(())
    })?;
    write_page_decode_seed(&output)?;
    write_btree_decode_seeds(&output)?;
    write_index_catalog_decode_seeds(&output)?;
    write_protocol_decode_seeds(&output)?;
    write_generation_seeds(&output)?;
    write_tail_intent_seeds(&output)?;
    write_owner_only_seeds(&output)?;
    Ok(())
}

fn write_protocol_decode_seeds(wal_output: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let output = wal_output
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("protocol_decode");
    std::fs::create_dir_all(&output)?;
    std::fs::write(output.join("empty"), [])?;

    let hello = encode_client_frame(1, &ClientMessage::Hello)?;
    std::fs::write(output.join("valid-hello"), &hello)?;
    std::fs::write(
        output.join("valid-execute"),
        encode_client_frame(
            2,
            &ClientMessage::Execute {
                sql: "SELECT id FROM users".into(),
            },
        )?,
    )?;
    std::fs::write(
        output.join("valid-query-start"),
        encode_server_frame(
            3,
            &ServerMessage::QueryStart {
                columns: vec![WireResultColumn {
                    name: "id".into(),
                    data_type: SemanticType::named("UserId", PhysicalType::UInt64),
                    nullable: false,
                }],
            },
        )?,
    )?;
    std::fs::write(
        output.join("valid-query-row"),
        encode_server_frame(
            4,
            &ServerMessage::QueryRow {
                values: vec![ScalarValue::UInt64(42), ScalarValue::Null],
            },
        )?,
    )?;
    std::fs::write(
        output.join("valid-error"),
        encode_server_frame(
            5,
            &ServerMessage::Error {
                code: ProtocolErrorCode::Compile,
                transaction_state: WireTransactionState::Active,
                message: "invalid SQL".into(),
            },
        )?,
    )?;
    std::fs::write(output.join("truncated-frame"), &hello[..10])?;
    let mut oversized = hello;
    oversized[12..16].copy_from_slice(&(MAX_FRAME_PAYLOAD + 1).to_le_bytes());
    std::fs::write(output.join("oversized-length-header"), oversized)?;
    Ok(())
}

fn write_index_catalog_decode_seeds(wal_output: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let output = wal_output
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("index_catalog_decode");
    std::fs::create_dir_all(&output)?;
    std::fs::write(output.join("empty"), [])?;
    let empty = encode_index_catalog(&IndexCatalogNode::empty())?;
    std::fs::write(output.join("valid-empty-catalog"), &empty)?;
    std::fs::write(
        output.join("valid-analyzed-root"),
        encode_index_catalog(&IndexCatalogNode {
            reclaim_intent: None,
            pending: Vec::new(),
            next_index_id: Some(netbadb_types::IndexId(4)),
            next_catalog: None,
            table_statistics: Some(TableStatistics {
                row_count: 10,
                managed_page_count: 4,
            }),
            entries: vec![],
        })?,
    )?;
    let one = encode_index_catalog(&IndexCatalogNode {
        reclaim_intent: None,
        pending: Vec::new(),
        next_index_id: Some(netbadb_types::IndexId(4)),
        next_catalog: None,
        table_statistics: None,
        entries: vec![IndexCatalogEntry {
            retired: false,
            definition: IndexDefinition {
                id: netbadb_types::IndexId(3),
                name: None,
                column_id: ColumnId(1),
                handle: BTreeHandle {
                    owner: None,
                    meta_page: netbadb_index::BTreePageRef::Legacy(PageId(3)),
                },
            },
            statistics: None,
        }],
    })?;
    let mut named = netbadb_index::decode_index_catalog(&one)?;
    named.entries[0].definition.name = Some(netbadb_types::IndexName::new("named_idx")?);
    let mut named_v5 = encode_index_catalog(&named)?;
    named_v5[4..6].copy_from_slice(&5_u16.to_le_bytes());
    std::fs::write(output.join("valid-named-v5"), &named_v5)?;
    let mut named_v4 = named_v5;
    named_v4[4..6].copy_from_slice(&4_u16.to_le_bytes());
    named_v4[7] = 0;
    named_v4[40..48].fill(0);
    std::fs::write(output.join("valid-named-v4"), &named_v4)?;
    let mut named_v3 = named_v4;
    named_v3[4..6].copy_from_slice(&3_u16.to_le_bytes());
    named_v3.drain(88..96);
    std::fs::write(output.join("valid-named-v3"), named_v3)?;
    named.entries[0].retired = true;
    let mut retired_named = encode_index_catalog(&named)?;
    retired_named[4..6].copy_from_slice(&5_u16.to_le_bytes());
    std::fs::write(output.join("valid-retired-named-v5"), &retired_named)?;
    retired_named[4..6].copy_from_slice(&4_u16.to_le_bytes());
    retired_named[7] = 0;
    retired_named[40..48].fill(0);
    std::fs::write(output.join("valid-retired-named-v4"), retired_named)?;
    std::fs::write(output.join("valid-one-entry"), &one)?;
    let mut v4 = one.clone();
    v4[4..6].copy_from_slice(&4_u16.to_le_bytes());
    v4[7] = 0;
    v4[40..48].fill(0);
    std::fs::write(output.join("valid-legacy-v4"), &v4)?;
    let mut retired = v4.clone();
    retired[84] = 1;
    std::fs::write(output.join("valid-retired-v4"), &retired)?;
    std::fs::write(
        output.join("truncated-retired-v4"),
        &retired[..retired.len() - 1],
    )?;
    retired[84] = 2;
    std::fs::write(output.join("invalid-state-v4"), &retired)?;
    let mut invalid_id = v4.clone();
    invalid_id[88..96].fill(0);
    std::fs::write(output.join("invalid-zero-id-v4"), invalid_id)?;
    let mut duplicate_id = v4.clone();
    duplicate_id[16..20].copy_from_slice(&2_u32.to_le_bytes());
    duplicate_id.extend_from_slice(&one[48..]);
    std::fs::write(output.join("duplicate-ids-v4"), duplicate_id)?;
    for version in [2_u16, 3] {
        let mut legacy = v4.clone();
        legacy[4..6].copy_from_slice(&version.to_le_bytes());
        legacy.drain(88..96);
        std::fs::write(output.join(format!("valid-legacy-v{version}")), legacy)?;
    }

    std::fs::write(output.join("valid-missing-stats-entry"), &one)?;
    let mut continuation = one.clone();
    continuation[7] = 0;
    continuation[40..48].fill(0);
    std::fs::write(output.join("valid-continuation-page"), continuation)?;
    let mut compacted = IndexCatalogNode::empty();
    compacted.next_index_id = Some(netbadb_types::IndexId(101));
    std::fs::write(
        output.join("valid-compacted-high-water-v5"),
        encode_index_catalog(&compacted)?,
    )?;
    for value in [0_u64, 3] {
        let mut invalid = one.clone();
        invalid[40..48].copy_from_slice(&value.to_le_bytes());
        std::fs::write(
            output.join(format!("invalid-high-water-{value}-v5")),
            invalid,
        )?;
    }
    let mut invalid_state = one.clone();
    invalid_state[84] = 2;
    std::fs::write(output.join("invalid-state-v5"), invalid_state)?;
    std::fs::write(
        output.join("valid-analyzed-index-entry"),
        encode_index_catalog(&IndexCatalogNode {
            reclaim_intent: None,
            pending: Vec::new(),
            next_index_id: Some(netbadb_types::IndexId(4)),
            next_catalog: None,
            table_statistics: Some(TableStatistics {
                row_count: 10,
                managed_page_count: 4,
            }),
            entries: vec![IndexCatalogEntry {
                retired: false,
                definition: IndexDefinition {
                    id: netbadb_types::IndexId(3),
                    name: None,
                    column_id: ColumnId(1),
                    handle: BTreeHandle {
                        owner: None,
                        meta_page: netbadb_index::BTreePageRef::Legacy(PageId(3)),
                    },
                },
                statistics: Some(IndexStatistics {
                    distinct_non_null_keys: 8,
                    null_count: 2,
                    tree_height: 2,
                }),
            }],
        })?,
    )?;
    std::fs::write(
        output.join("valid-catalog-with-next"),
        encode_index_catalog(&IndexCatalogNode {
            reclaim_intent: None,
            pending: Vec::new(),
            next_index_id: Some(netbadb_types::IndexId(4)),
            next_catalog: Some(PageId(9)),
            table_statistics: None,
            entries: vec![],
        })?,
    )?;
    std::fs::write(output.join("truncated"), &one[..one.len() - 1])?;
    let mut bad_count = empty;
    bad_count[16..20].copy_from_slice(&u32::MAX.to_le_bytes());
    std::fs::write(output.join("bad-count"), bad_count)?;
    let mut version_one = Vec::new();
    version_one.extend_from_slice(b"NBIC");
    version_one.extend_from_slice(&1_u16.to_le_bytes());
    version_one.extend_from_slice(&0_u16.to_le_bytes());
    version_one.extend_from_slice(&0_u64.to_le_bytes());
    version_one.extend_from_slice(&0_u32.to_le_bytes());
    version_one.extend_from_slice(&0_u32.to_le_bytes());
    version_one.resize(48, 0);
    std::fs::write(output.join("unsupported-v1"), version_one)?;
    let mut pending = IndexCatalogNode::empty();
    pending.next_index_id = Some(netbadb_types::IndexId(9));
    pending.pending.push(netbadb_index::RetiredIndexOwnership {
        index_id: netbadb_types::IndexId(7),
        meta_page: Some(netbadb_index::BTreePageRef::Legacy(PageId(2))),
    });
    let pending_bytes = encode_index_catalog(&pending)?;
    std::fs::write(output.join("valid-pending-v6"), &pending_bytes)?;
    let mut invalid = pending_bytes.clone();
    invalid[48..56].fill(0);
    std::fs::write(output.join("invalid-pending-owner-v6"), invalid)?;
    std::fs::write(output.join("truncated-pending-v6"), &pending_bytes[..63])?;
    let mut owned = netbadb_index::decode_index_catalog(&one)?;
    owned.entries[0].definition.handle.owner = Some(owned.entries[0].definition.id);
    std::fs::write(output.join("valid-owned-v6"), encode_index_catalog(&owned)?)?;
    let mut invalid = encode_index_catalog(&owned)?;
    invalid[85] = 2;
    std::fs::write(output.join("invalid-owner-format-v6"), invalid)?;
    Ok(())
}

fn write_btree_decode_seeds(wal_output: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let output = wal_output
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("btree_decode");
    std::fs::create_dir_all(&output)?;
    std::fs::write(output.join("empty"), [])?;
    let spec = IndexSpec {
        data_type: SemanticType::physical(PhysicalType::UInt64),
        nullable: true,
    };
    let entry = IndexEntry {
        key: ScalarValue::UInt64(42),
        row_id: RowId {
            page: PageId(1),
            slot: 0,
            generation: 1,
        },
    };
    write_btree_seed(
        &output,
        "valid-meta",
        0,
        &encode_meta(&MetaNode {
            generation: None,
            owner: None,
            root_page: netbadb_index::BTreePageRef::Legacy(PageId(2)),
            height: 1,
            spec: spec.clone(),
        })?,
    )?;
    write_btree_seed(
        &output,
        "valid-empty-leaf",
        1,
        &encode_leaf(&spec, &LeafNode::empty())?,
    )?;
    let leaf = encode_leaf(
        &spec,
        &LeafNode {
            entries: vec![entry.clone()],
            next_leaf: None,
        },
    )?;
    write_btree_seed(&output, "valid-leaf-one-entry", 1, &leaf)?;
    write_btree_seed(
        &output,
        "valid-internal-one-separator",
        2,
        &encode_internal(
            &spec,
            &InternalNode {
                first_child: netbadb_index::BTreePageRef::Legacy(PageId(2)),
                separators: vec![InternalSeparator {
                    key: entry,
                    right_child: netbadb_index::BTreePageRef::Legacy(PageId(3)),
                }],
            },
        )?,
    )?;
    write_btree_seed(&output, "truncated-leaf", 1, &leaf[..leaf.len() - 1])?;
    let owner = Some(netbadb_types::IndexId(7));
    let meta = encode_meta(&MetaNode {
        generation: None,
        owner,
        root_page: netbadb_index::BTreePageRef::Legacy(PageId(2)),
        height: 2,
        spec: spec.clone(),
    })?;
    let leaf = netbadb_index::encode_leaf_owned(&spec, &LeafNode::empty(), owner)?;
    let internal = netbadb_index::encode_internal_owned(
        &spec,
        &InternalNode {
            first_child: netbadb_index::BTreePageRef::Legacy(PageId(2)),
            separators: vec![],
        },
        owner,
    )?;
    for (kind, name, payload) in [
        (0, "meta", meta),
        (1, "leaf", leaf),
        (2, "internal", internal),
    ] {
        write_btree_seed(&output, &format!("valid-owned-{name}-v2"), kind, &payload)?;
        let mut zero = payload.clone();
        zero[8..16].fill(0);
        write_btree_seed(&output, &format!("invalid-owner-{name}-v2"), kind, &zero)?;
        write_btree_seed(
            &output,
            &format!("truncated-owner-{name}-v2"),
            kind,
            &payload[..15],
        )?;
    }
    Ok(())
}

fn write_btree_seed(output: &Path, name: &str, kind: u8, payload: &[u8]) -> std::io::Result<()> {
    let mut seed = Vec::with_capacity(payload.len() + 1);
    seed.push(kind);
    seed.extend_from_slice(payload);
    std::fs::write(output.join(name), seed)
}

fn write_page_decode_seed(wal_output: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let output = wal_output
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("page_decode");
    std::fs::create_dir_all(&output)?;
    let mut page = Page::new(PageId(7), PageType::Heap);
    page.insert_record(b"page-v5-generation-fuzz-seed")?;
    let old_seed = output.join("valid-page-v4");
    if old_seed.exists() {
        std::fs::remove_file(old_seed)?;
    }
    std::fs::write(output.join("valid-page-v5"), page.bytes())?;

    let database_path =
        std::env::temp_dir().join(format!("netbadb-page-corpus-{}-heap", std::process::id()));
    let wal_file = wal_path(&database_path);
    let _ = std::fs::remove_file(&database_path);
    let _ = std::fs::remove_file(&wal_file);
    let _ = std::fs::remove_file(wal_alternate_path(&wal_file));
    HeapStorage::create(&database_path, fuzz_table())?.close()?;
    let mut pages = PageManager::open(&database_path)?;
    let catalog = pages.read_page(PageId(1))?;
    std::fs::write(output.join("valid-index-catalog-page-v5"), catalog.bytes())?;
    drop(pages);
    std::fs::remove_file(wal_file)?;
    std::fs::remove_file(database_path)?;
    Ok(())
}

fn write_page_update_seed(output: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let database_path =
        std::env::temp_dir().join(format!("netbadb-wal-corpus-{}-heap", std::process::id()));
    let wal_file = wal_path(&database_path);
    let _ = std::fs::remove_file(&database_path);
    let _ = std::fs::remove_file(&wal_file);
    let mut storage = HeapStorage::create(&database_path, fuzz_table())?;
    storage.insert(&[ScalarValue::UInt64(1)])?;
    storage.flush()?;
    std::fs::copy(&wal_file, output.join("valid-page-update"))?;
    drop(storage);
    // Keep the complete owned lifecycle below the harness's 64 KiB limit.
    // A separate empty heap avoids including unrelated heap INSERT history.
    let owned_path = database_path.with_extension("owned");
    let owned_wal = wal_path(&owned_path);
    let mut storage = HeapStorage::create(&owned_path, fuzz_table())?;
    let index = storage.create_index(ColumnId(1))?;
    storage.flush()?;
    std::fs::copy(&owned_wal, output.join("valid-owned-btree-v3"))?;
    storage.drop_index(index.id)?;
    storage.compact_index_catalog()?;
    storage.flush()?;
    let pending_seed = std::fs::read(&owned_wal)?;
    assert!(pending_seed.len() <= 64 * 1024);
    std::fs::write(output.join("valid-pending-catalog-v7"), pending_seed)?;
    drop(storage);
    std::fs::remove_file(owned_wal)?;
    std::fs::remove_file(&owned_path)?;
    let mut status_path = owned_path.as_os_str().to_os_string();
    status_path.push("-txn-status");
    std::fs::remove_file(status_path)?;
    std::fs::remove_file(wal_file)?;
    std::fs::remove_file(database_path)?;
    Ok(())
}

fn fuzz_table() -> TableDef {
    TableDef::new(
        TableId(1),
        "fuzz_rows",
        vec![
            ColumnDef::new(ColumnId(1), "id", TypeSpec::Physical(PhysicalType::UInt64))
                .primary_key(true),
        ],
    )
}

fn write_seed(
    output: &Path,
    name: &str,
    build: impl FnOnce(&mut WalManager, &Path) -> Result<(), Box<dyn std::error::Error>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let path =
        std::env::temp_dir().join(format!("netbadb-wal-corpus-{}-{name}", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let mut wal = WalManager::create(&path)?;
    build(&mut wal, &path)?;
    drop(wal);
    std::fs::copy(&path, output.join(name))?;
    std::fs::remove_file(path)?;
    Ok(())
}

// Existing corpus fixtures explicitly retain the v6 legacy handle representation.
fn encode_index_catalog(node: &IndexCatalogNode) -> Result<Vec<u8>, netbadb_index::IndexError> {
    let current = encode_current_index_catalog(node)?;
    let mut legacy = current[..48].to_vec();
    legacy[4..6].copy_from_slice(&6_u16.to_le_bytes());
    let mut offset = 48;
    for entry in &node.entries {
        assert!(entry.definition.handle.meta_page.generation().is_none());
        let length = entry
            .definition
            .name
            .as_ref()
            .map_or(0, |name| name.as_str().len());
        legacy.extend_from_slice(&current[offset..offset + 48]);
        legacy.extend_from_slice(&current[offset + 56..offset + 56 + length]);
        offset += 56 + length;
    }
    for pending in &node.pending {
        assert!(
            pending
                .meta_page
                .is_some_and(|page| page.generation().is_none())
        );
        legacy.extend_from_slice(&current[offset..offset + 16]);
        offset += 32;
    }
    Ok(legacy)
}

fn write_generation_seeds(wal_output: &Path) -> Result<(), Box<dyn std::error::Error>> {
    use netbadb_index::{
        BTreePageRef, RetiredIndexOwnership, encode_internal_generation, encode_leaf_generation,
    };
    use netbadb_types::{IndexId, PageGeneration, PageRef};
    let root = wal_output.parent().unwrap_or_else(|| Path::new("."));
    let refs = |page, generation| {
        BTreePageRef::Allocated(PageRef {
            page_id: PageId(page),
            generation: PageGeneration(generation),
        })
    };
    let spec = IndexSpec {
        data_type: SemanticType::physical(PhysicalType::UInt64),
        nullable: true,
    };
    let owner = Some(IndexId(7));
    let generation = Some(PageGeneration(100));
    let meta = MetaNode {
        owner,
        generation,
        root_page: refs(2, 101),
        height: 2,
        spec: spec.clone(),
    };
    let leaf = LeafNode {
        entries: vec![],
        next_leaf: Some(refs(3, 102)),
    };
    let internal = InternalNode {
        first_child: refs(2, 101),
        separators: vec![InternalSeparator {
            key: IndexEntry {
                key: ScalarValue::UInt64(42),
                row_id: RowId {
                    page: PageId(1),
                    slot: 0,
                    generation: 1,
                },
            },
            right_child: refs(3, 102),
        }],
    };
    let output = root.join("btree_decode");
    for (kind, name, bytes, pointer_offset) in [
        (0, "meta", encode_meta(&meta)?, 32),
        (
            1,
            "leaf",
            encode_leaf_generation(&spec, &leaf, owner, generation)?,
            36,
        ),
        (
            2,
            "internal",
            encode_internal_generation(&spec, &internal, owner, generation)?,
            75,
        ),
    ] {
        write_btree_seed(&output, &format!("valid-{name}-v3"), kind, &bytes)?;
        for (field, offset) in [
            ("owner", 8),
            ("generation", 16),
            ("pointer", pointer_offset),
        ] {
            let mut corrupt = bytes.clone();
            corrupt[offset..offset + 8].fill(0);
            write_btree_seed(&output, &format!("zero-{field}-{name}-v3"), kind, &corrupt)?;
        }
        write_btree_seed(
            &output,
            &format!("truncated-ref-{name}-v3"),
            kind,
            &bytes[..pointer_offset + 7],
        )?;
    }
    let mut catalog = IndexCatalogNode::empty();
    catalog.next_index_id = Some(IndexId(9));
    catalog.entries.push(IndexCatalogEntry {
        retired: false,
        statistics: None,
        definition: IndexDefinition {
            id: IndexId(7),
            name: None,
            column_id: ColumnId(1),
            handle: BTreeHandle {
                owner,
                meta_page: refs(2, 100),
            },
        },
    });
    catalog.pending.push(RetiredIndexOwnership {
        index_id: IndexId(8),
        meta_page: Some(refs(3, 102)),
    });
    let bytes = encode_current_index_catalog(&catalog)?;
    let output = root.join("index_catalog_decode");
    std::fs::write(output.join("valid-active-pending-v7"), &bytes)?;
    for (name, offset) in [("active", 96), ("pending", 120)] {
        let mut corrupt = bytes.clone();
        corrupt[offset..offset + 8].fill(0);
        std::fs::write(output.join(format!("zero-generation-{name}-v7")), corrupt)?;
        std::fs::write(
            output.join(format!("truncated-ref-{name}-v7")),
            &bytes[..offset + 7],
        )?;
    }
    write_seed(wal_output, "valid-generation-reservation-v4", |wal, _| {
        let begin = wal.append(TxnId(1), None, WalRecordKind::Begin)?;
        let reservation = wal.append(
            TxnId(1),
            Some(begin),
            WalRecordKind::PageGenerationReservation,
        )?;
        wal.flush_through(reservation)?;
        Ok(())
    })?;
    let path =
        std::env::temp_dir().join(format!("netbadb-generation-corpus-{}", std::process::id()));
    let mut database = netbadb_core::Database::create(&path, fuzz_table())?;
    let prepared = database.prepare_ddl_statement("CREATE INDEX reused ON fuzz_rows (id)")?;
    let mut tx = database.begin_transaction()?;
    database.execute_ddl_in(&mut tx, &prepared)?;
    tx.rollback()?;
    drop(tx);
    database.execute_ddl(&prepared)?;
    database.close()?;
    let bytes = std::fs::read(wal_path(&path))?;
    assert!(bytes.len() <= 128 * 1024);
    std::fs::write(wal_output.join("valid-rollback-reuse-v3"), bytes)?;
    std::fs::remove_file(wal_path(&path))?;
    std::fs::remove_file(netbadb_storage::txn_status_path(&path))?;
    std::fs::remove_file(path)?;
    Ok(())
}

// Synthetic post-checkpoint snapshots exercise open-time intent recovery without
// depending on test-only process hooks. CRCs use the production Page boundary.
fn write_tail_intent_seeds(output: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::temp_dir().join(format!(
        "netbadb-round11-fuzz-generator-{}",
        std::process::id()
    ));
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
    let mut storage = HeapStorage::create(&path, fuzz_table())?;
    let index = storage.create_index(ColumnId(1))?;
    storage.drop_index(index.id)?;
    storage.compact_index_catalog()?;
    storage.checkpoint()?;
    storage.close()?;
    let wal = WalManager::open(wal_path(&path))?;
    let wal_bytes = std::fs::read(wal.path())?;
    let checkpoint_lsn = wal.base_lsn();
    drop(wal);
    let mut disk = PageManager::open(&path)?;
    let root = disk.read_page(PageId(1))?;
    let mut node =
        netbadb_index::decode_index_catalog(root.read_record(netbadb_types::SlotId(0))?)?;
    // Keep Round 11 seeds genuinely v8/root-dependent after v9 compaction.
    node.pending[0].meta_page = Some(index.handle.meta_page);
    node.reclaim_intent = Some(netbadb_index::TailReclaimIntent {
        old_page_count: 5,
        truncate_from: 3,
        checkpoint_lsn,
        covered: node.pending.clone(),
    });
    let mut valid = encode_current_index_catalog(&node)?;
    valid[4..6].copy_from_slice(&8_u16.to_le_bytes());
    let catalog_output = output
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("index_catalog_decode");
    std::fs::create_dir_all(&catalog_output)?;
    let mut payloads = vec![("valid-intent", valid.clone())];
    let mut active = node.clone();
    active.reclaim_intent = None;
    active.pending.clear();
    active.entries.push(IndexCatalogEntry {
        retired: false,
        statistics: None,
        definition: index,
    });
    let mut active_bytes = encode_current_index_catalog(&active)?;
    active_bytes[4..6].copy_from_slice(&8_u16.to_le_bytes());
    active_bytes[7] = 2;
    active_bytes.extend_from_slice(&valid[80..]);
    payloads.push(("active-in-intent", active_bytes));
    let mut legacy = valid.clone();
    legacy[48 + 16..48 + 32].fill(0);
    payloads.push(("legacy-in-intent", legacy));
    payloads.push(("truncated-intent", valid[..valid.len() - 1].to_vec()));
    let mut unknown = valid.clone();
    unknown[40..48].copy_from_slice(&99_u64.to_le_bytes());
    unknown[112..120].copy_from_slice(&98_u64.to_le_bytes());
    payloads.push(("unknown-in-intent", unknown));
    let mut wrong = valid.clone();
    wrong[128..136].copy_from_slice(&1_u64.to_le_bytes());
    payloads.push(("wrong-ref-intent", wrong));
    let heap = std::fs::read(&path)?;
    for (name, payload) in payloads {
        std::fs::write(catalog_output.join(format!("round11-{name}-v8")), &payload)?;
        // A fixture-only Page v5 encoding retains the root's kind and pageLSN;
        // no raw mutation API is exported from storage for corpus generation.
        let mut page = Page::new(PageId(1), PageType::Heap);
        page.insert_record(&payload)?;
        let mut bytes = *page.bytes();
        bytes[6] = root.bytes()[6];
        bytes[16..24].copy_from_slice(&root.bytes()[16..24]);
        bytes[24..28].fill(0);
        let checksum = crc32c::crc32c_append(crc32c::crc32c(&1_u64.to_le_bytes()), &bytes);
        bytes[24..28].copy_from_slice(&checksum.to_le_bytes());
        Page::from_bytes(PageId(1), bytes).header()?;
        let mut image = heap.clone();
        image[4096..8192].copy_from_slice(&bytes);
        write_tail_snapshot(
            output,
            &format!("round11-{name}-old-length"),
            &image,
            &wal_bytes,
        )?;
        if name == "valid-intent" {
            write_tail_snapshot(
                output,
                "round11-valid-intent-new-length",
                &image[..3 * 4096],
                &wal_bytes,
            )?;
            write_tail_snapshot(
                output,
                "round11-invalid-partial-length",
                &image[..4 * 4096],
                &wal_bytes,
            )?;
        }
    }
    drop(disk);
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
    Ok(())
}

fn write_tail_snapshot(
    output: &Path,
    name: &str,
    heap: &[u8],
    wal: &[u8],
) -> Result<(), Box<dyn std::error::Error>> {
    let mut bytes = b"NBRF".to_vec();
    bytes.extend_from_slice(&u32::try_from(heap.len())?.to_le_bytes());
    bytes.extend_from_slice(&u32::try_from(wal.len())?.to_le_bytes());
    bytes.extend_from_slice(heap);
    bytes.extend_from_slice(wal);
    assert!(bytes.len() <= 128 * 1024);
    std::fs::write(output.join(name), bytes)?;
    let probe = std::env::temp_dir().join(format!(
        "netbadb-round11-seed-probe-{}-{name}",
        std::process::id()
    ));
    HeapStorage::create(&probe, fuzz_table())?.close()?;
    std::fs::write(&probe, heap)?;
    std::fs::write(wal_path(&probe), wal)?;
    let reopened = HeapStorage::open(&probe, fuzz_table());
    if name.starts_with("round11-valid-intent") {
        let mut storage = reopened?;
        let report = storage.inspect_index_reclaim()?;
        assert_eq!(report.database_pages, 3);
        assert_eq!(report.pending_reclaim_indexes, 0);
        storage.close()?;
    } else if name.starts_with("round12-valid-") {
        let mut storage = reopened?;
        let ownership = storage.inspect_index_reclaim()?;
        let candidates = storage.inspect_reusable_pages()?;
        assert_eq!(ownership.pending.len(), 1);
        assert!(ownership.pending[0].meta_page.is_none());
        assert_eq!(
            candidates.candidates.len(),
            if name.ends_with("partial") { 1 } else { 2 }
        );
        storage.close()?;
    } else {
        assert!(reopened.is_err(), "malformed fixture must fail: {name}");
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
    Ok(())
}

fn write_owner_only_seeds(output: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::temp_dir().join(format!("netbadb-round12-corpus-{}", std::process::id()));
    let mut storage = HeapStorage::create(&path, fuzz_table())?;
    let old = storage.create_index(ColumnId(1))?;
    storage.drop_index(old.id)?;
    storage.compact_index_catalog()?;
    let active = storage.create_index(ColumnId(1))?;
    storage.checkpoint()?;
    storage.close()?;
    let mut disk = PageManager::open(&path)?;
    let root = disk.read_page(PageId(1))?;
    let node = netbadb_index::decode_index_catalog(root.read_record(netbadb_types::SlotId(0))?)?;
    let valid = encode_current_index_catalog(&node)?;
    let catalog_output = output
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("index_catalog_decode");
    std::fs::write(catalog_output.join("round12-valid-owner-only-v9"), &valid)?;
    // The active entry occupies 56 bytes, followed by one 32-byte pending owner.
    for (name, offset, value) in [
        ("nonzero-root", 112, 3),
        ("nonzero-generation", 120, 9),
        ("unknown-class", 128, 3),
        ("reserved", 129, 1),
    ] {
        let mut bad = valid.clone();
        bad[offset] = value;
        assert!(netbadb_index::decode_index_catalog(&bad).is_err());
        std::fs::write(catalog_output.join(format!("round12-{name}-v9")), bad)?;
    }
    let mut old_format = valid.clone();
    old_format[4..6].copy_from_slice(&8_u16.to_le_bytes());
    assert!(netbadb_index::decode_index_catalog(&old_format).is_err());
    std::fs::write(
        catalog_output.join("round12-owner-only-forbidden-v8"),
        old_format,
    )?;
    std::fs::write(
        catalog_output.join("round12-truncated-owner-only-v9"),
        &valid[..valid.len() - 1],
    )?;
    // Burn a real durable reservation through the ordinary WAL API; no page
    // update is fabricated or admitted through the production allocator.
    let mut wal = WalManager::open(wal_path(&path))?;
    let begin = wal.append(TxnId(9000), None, WalRecordKind::Begin)?;
    let reservation = wal.append(
        TxnId(9000),
        Some(begin),
        WalRecordKind::PageGenerationReservation,
    )?;
    let commit = wal.append(TxnId(9000), Some(reservation), WalRecordKind::Commit)?;
    wal.flush_through(commit)?;
    let generation = netbadb_types::PageGeneration(reservation.0);
    let wal_bytes = std::fs::read(wal.path())?;
    drop(wal);
    let mut heap = std::fs::read(&path)?;
    write_tail_snapshot(output, "round12-valid-owner-only", &heap, &wal_bytes)?;
    let spec = IndexSpec {
        data_type: SemanticType::physical(PhysicalType::UInt64),
        nullable: true,
    };
    let payload = netbadb_index::encode_leaf_generation(
        &spec,
        &LeafNode::empty(),
        Some(active.id),
        Some(generation),
    )?;
    let id = old.handle.meta_page.page_id();
    let mut page = Page::new(id, PageType::Heap);
    page.insert_record(&payload)?;
    let mut bytes = *page.bytes();
    // PageType has explicit persistent tags; obtain the leaf tag from a real
    // leaf page instead of relying on Rust enum representation.
    let active_leaf = disk.read_page(PageId(active.handle.meta_page.page_id().0 + 1))?;
    bytes[6] = active_leaf.bytes()[6];
    bytes[24..28].fill(0);
    let checksum = crc32c::crc32c_append(crc32c::crc32c(&id.0.to_le_bytes()), &bytes);
    bytes[24..28].copy_from_slice(&checksum.to_le_bytes());
    Page::from_bytes(id, bytes).header()?;
    let start = usize::try_from(id.0)? * 4096;
    heap[start..start + 4096].copy_from_slice(&bytes);
    write_tail_snapshot(
        output,
        "round12-valid-owner-only-partial",
        &heap,
        &wal_bytes,
    )?;
    drop(disk);
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
    Ok(())
}
