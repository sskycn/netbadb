use std::path::{Path, PathBuf};

use netbadb_index::{
    BTreeHandle, IndexCatalogEntry, IndexCatalogNode, IndexDefinition, IndexEntry, IndexSpec,
    IndexStatistics, InternalNode, InternalSeparator, LeafNode, MetaNode, TableStatistics,
    encode_index_catalog, encode_internal, encode_leaf, encode_meta,
};
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_storage::{
    HeapStorage, Page, PageManager, PageType, WalManager, WalRecordKind, wal_alternate_path,
    wal_path,
};
use netbadb_types::{
    ColumnId, PageId, PhysicalType, RowId, ScalarValue, SemanticType, TableId, TxnId,
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
            next_catalog: None,
            table_statistics: Some(TableStatistics {
                row_count: 10,
                managed_page_count: 4,
            }),
            entries: vec![],
        })?,
    )?;
    let one = encode_index_catalog(&IndexCatalogNode {
        next_catalog: None,
        table_statistics: None,
        entries: vec![IndexCatalogEntry {
            definition: IndexDefinition {
                column_id: ColumnId(1),
                handle: BTreeHandle {
                    meta_page: PageId(3),
                },
            },
            statistics: None,
        }],
    })?;
    std::fs::write(output.join("valid-one-entry"), &one)?;
    std::fs::write(output.join("valid-missing-stats-entry"), &one)?;
    std::fs::write(output.join("valid-continuation-page"), &one)?;
    std::fs::write(
        output.join("valid-analyzed-index-entry"),
        encode_index_catalog(&IndexCatalogNode {
            next_catalog: None,
            table_statistics: Some(TableStatistics {
                row_count: 10,
                managed_page_count: 4,
            }),
            entries: vec![IndexCatalogEntry {
                definition: IndexDefinition {
                    column_id: ColumnId(1),
                    handle: BTreeHandle {
                        meta_page: PageId(3),
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
    std::fs::write(output.join("unsupported-v1"), version_one)?;
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
            root_page: PageId(2),
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
                first_child: PageId(2),
                separators: vec![InternalSeparator {
                    key: entry,
                    right_child: PageId(3),
                }],
            },
        )?,
    )?;
    write_btree_seed(&output, "truncated-leaf", 1, &leaf[..leaf.len() - 1])?;
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
    drop(storage);
    std::fs::copy(&wal_file, output.join("valid-page-update"))?;
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
