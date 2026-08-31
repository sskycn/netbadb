use std::path::PathBuf;

use netbadb_schema::{ColumnDef, Schema, TableDef, TypeSpec};
use netbadb_storage::{HeapStorage, LsmStorage, StorageError};
use netbadb_types::{ColumnId, PartitionId, PhysicalType, ScalarValue, StorageId, TableId};

use crate::partition_catalog::{CatalogTable, PartitionCatalog};
use crate::registry::{RangePartitionBinding, TablePlacement};
use crate::schema_catalog::{
    CatalogStorage, CatalogStorageKind, CommittedCatalogState, SchemaCatalogSnapshot, envelope,
};
use crate::schema_catalog_file as file;
use crate::{
    CompleteLegacyInventory, Database, DatabaseCoordinatorConfig, DatabaseError,
    LegacyStorageLocation, PartitionCatalogConfig, RangePartitionSpec, SchemaCatalogError,
    SchemaGeneration, TablePlacementSpec, TableSchemaVersion, TableStorageCreateSpec,
};

struct Fixture(PathBuf);
impl Fixture {
    fn new(case: &str) -> Self {
        let root =
            std::env::temp_dir().join(format!("netbadb-round17-{case}-{}", std::process::id()));
        std::fs::create_dir(&root).unwrap();
        Self(root)
    }
    fn path(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
    fn catalog(&self) -> PathBuf {
        self.path("database.schema")
    }
    fn heap_specs(&self) -> Vec<TableStorageCreateSpec> {
        vec![
            TableStorageCreateSpec::heap(self.path("a.db"), table(91, "a")),
            TableStorageCreateSpec::heap(self.path("b.db"), table(7, "b")),
        ]
    }
    fn inventory(&self) -> CompleteLegacyInventory {
        CompleteLegacyInventory::attest_complete(
            vec![
                LegacyStorageLocation::Heap(self.path("a.db")),
                LegacyStorageLocation::Heap(self.path("b.db")),
            ],
            None,
            None,
        )
    }
    fn schema(&self) -> Schema {
        Schema::new(vec![table(91, "a"), table(7, "b")]).unwrap()
    }
    fn legacy(&self) {
        for (index, spec) in self.heap_specs().iter().enumerate() {
            let mut heap = HeapStorage::create_with_storage_id(
                spec.path(),
                spec.table().clone(),
                StorageId(index as u64 + 1),
            )
            .unwrap();
            heap.insert(&[ScalarValue::Int64(42), ScalarValue::Null])
                .unwrap();
            heap.close().unwrap();
        }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn table(id: u64, name: &str) -> TableDef {
    TableDef::new(
        TableId(id),
        name,
        vec![
            ColumnDef::new(
                ColumnId(42),
                "id",
                TypeSpec::Semantic {
                    name: "RecordId".into(),
                    physical: PhysicalType::Int64,
                },
            )
            .primary_key(true),
            ColumnDef::new(ColumnId(7), "name", TypeSpec::Physical(PhysicalType::Text))
                .nullable(true),
        ],
    )
}
fn expect_legacy(result: Result<Database, DatabaseError>) {
    assert!(matches!(
        result,
        Err(DatabaseError::SchemaCatalog(
            SchemaCatalogError::LegacyCatalogRequired
        ))
    ));
}
fn codec_fixture() -> SchemaCatalogSnapshot {
    let schema = Schema::new(vec![table(91, "a"), table(7, "b"), table(22, "events")]).unwrap();
    let placements = PartitionCatalog {
        tables: schema
            .tables()
            .iter()
            .enumerate()
            .map(|(i, table)| CatalogTable {
                table_id: table.id,
                schema_fingerprint: table.fingerprint().unwrap(),
                placement: if i < 2 {
                    TablePlacement::Single {
                        table_id: table.id,
                        storage_id: StorageId(i as u64 + 1),
                    }
                } else {
                    TablePlacement::RangePartitioned {
                        table_id: table.id,
                        partition_key: ColumnId(42),
                        key_type: PhysicalType::Int64,
                        partitions: vec![
                            RangePartitionBinding {
                                partition_id: PartitionId(88),
                                storage_id: StorageId(9),
                                lower: None,
                                upper: Some(ScalarValue::Int64(0)),
                            },
                            RangePartitionBinding {
                                partition_id: PartitionId(5),
                                storage_id: StorageId(11),
                                lower: Some(ScalarValue::Int64(0)),
                                upper: None,
                            },
                        ],
                    }
                },
            })
            .collect(),
    };
    SchemaCatalogSnapshot {
        incarnation: [17; 16],
        epoch: 1,
        committed: CommittedCatalogState::initial(
            schema,
            placements.tables.iter().map(|t| t.placement.clone()),
        ),
        placements,
        storages: vec![
            CatalogStorage {
                id: StorageId(1),
                table_id: TableId(91),
                locator: "a.db".into(),
                kind: CatalogStorageKind::Heap,
            },
            CatalogStorage {
                id: StorageId(2),
                table_id: TableId(7),
                locator: "lsm".into(),
                kind: CatalogStorageKind::Lsm {
                    clustering_column: ColumnId(42),
                },
            },
            CatalogStorage {
                id: StorageId(9),
                table_id: TableId(22),
                locator: "p1.db".into(),
                kind: CatalogStorageKind::Heap,
            },
            CatalogStorage {
                id: StorageId(11),
                table_id: TableId(22),
                locator: "p2.db".into(),
                kind: CatalogStorageKind::Heap,
            },
        ],
        coordinator: Some("coordinator".into()),
        partition_evidence: Some("partitions".into()),
    }
}

#[test]
fn codec_determinism_shapes_bounds_and_exhausted_identity_domains() {
    let snapshot = codec_fixture();
    let bytes = snapshot.encode().unwrap();
    let decoded = SchemaCatalogSnapshot::decode(&bytes).unwrap();
    assert_eq!(snapshot, decoded);
    assert_eq!(decoded.encode().unwrap(), bytes);
    assert_eq!(decoded.committed.next_table_id, Some(TableId(92)));
    assert_eq!(decoded.committed.next_partition_id, Some(PartitionId(89)));
    assert_eq!(decoded.committed.next_storage_id, Some(StorageId(12)));
    assert_eq!(
        decoded.committed.tables[0].next_column_id,
        Some(ColumnId(43))
    );
    let mut maximum = snapshot.clone();
    let mut tables = maximum.committed.schema.tables().to_vec();
    tables[0].name = "x".repeat(crate::schema_catalog::MAX_STRING);
    tables[0].columns[0].name = "界".repeat(1365) + "x";
    tables[0].columns[0].id = ColumnId(u32::MAX);
    tables[0].columns[1].id = ColumnId(0);
    tables[0].id = TableId(u64::MAX);
    maximum.storages[0].table_id = tables[0].id;
    maximum.storages[0].id = StorageId(u64::MAX);
    maximum.placements.tables[0] = CatalogTable {
        table_id: tables[0].id,
        schema_fingerprint: tables[0].fingerprint().unwrap(),
        placement: TablePlacement::Single {
            table_id: tables[0].id,
            storage_id: StorageId(u64::MAX),
        },
    };
    maximum.committed = CommittedCatalogState::initial(
        Schema::new(tables).unwrap(),
        maximum
            .placements
            .tables
            .iter()
            .map(|p| p.placement.clone()),
    );
    let decoded = SchemaCatalogSnapshot::decode(&maximum.encode().unwrap()).unwrap();
    assert_eq!(decoded.committed.next_table_id, None);
    assert_eq!(decoded.committed.tables[0].next_column_id, None);
    assert_eq!(decoded.committed.next_storage_id, None);
    let minimal = SchemaCatalogSnapshot {
        incarnation: [1; 16],
        epoch: 1,
        committed: CommittedCatalogState::initial(Schema::default(), std::iter::empty()),
        placements: PartitionCatalog { tables: Vec::new() },
        storages: Vec::new(),
        coordinator: None,
        partition_evidence: None,
    };
    assert_eq!(
        SchemaCatalogSnapshot::decode(&minimal.encode().unwrap()).unwrap(),
        minimal
    );
}

#[test]
fn codec_rejects_corruption_and_invalid_invariants() {
    let original = codec_fixture();
    let bytes = original.encode().unwrap();
    for length in 0..bytes.len() {
        assert!(
            SchemaCatalogSnapshot::decode(&bytes[..length]).is_err(),
            "length {length}"
        );
    }
    let mut bad = bytes.clone();
    bad[4] = 9;
    assert!(matches!(
        SchemaCatalogSnapshot::decode(&bad),
        Err(SchemaCatalogError::UnsupportedVersion(9))
    ));
    bad = bytes.clone();
    bad[6] = 1;
    assert!(SchemaCatalogSnapshot::decode(&bad).is_err());
    bad = bytes.clone();
    bad[100] ^= 1;
    assert!(SchemaCatalogSnapshot::decode(&bad).is_err());
    // Full CRC repair exercises semantic decoding, not just checksum rejection.
    for offset in 16..bytes.len() {
        let mut payload = bytes[16..].to_vec();
        payload[offset - 16] ^= 0xff;
        let mutated = envelope(b"NBSC", &payload).unwrap();
        if let Ok(decoded) = SchemaCatalogSnapshot::decode(&mutated) {
            assert_eq!(decoded.encode().unwrap(), mutated);
        }
    }
    let mut payload = bytes[16..].to_vec();
    payload[32..40].copy_from_slice(&1_u64.to_le_bytes());
    assert!(matches!(
        SchemaCatalogSnapshot::decode(&envelope(b"NBSC", &payload).unwrap()),
        Err(SchemaCatalogError::SchemaCatalogCorrupt(
            "invalid identity high-water"
        ))
    ));
    let mut payload = bytes[16..].to_vec();
    payload[85] ^= 1;
    assert!(matches!(
        SchemaCatalogSnapshot::decode(&envelope(b"NBSC", &payload).unwrap()),
        Err(SchemaCatalogError::SchemaCatalogCorrupt(
            "logical fingerprint mismatch"
        ))
    ));
    let mut payload = bytes[16..].to_vec();
    payload[84] = 255;
    assert!(matches!(
        SchemaCatalogSnapshot::decode(&envelope(b"NBSC", &payload).unwrap()),
        Err(SchemaCatalogError::SchemaCatalogCorrupt("invalid UTF-8"))
    ));
    let pattern = [
        2_u64.to_le_bytes().as_slice(),
        7_u64.to_le_bytes().as_slice(),
        3_u32.to_le_bytes().as_slice(),
        b"lsm",
    ]
    .concat();
    let mut payload = bytes[16..].to_vec();
    let offset = payload
        .windows(pattern.len())
        .position(|window| window == pattern)
        .unwrap();
    payload[offset..offset + 8].copy_from_slice(&1_u64.to_le_bytes());
    assert!(matches!(
        SchemaCatalogSnapshot::decode(&envelope(b"NBSC", &payload).unwrap()),
        Err(SchemaCatalogError::SchemaCatalogCorrupt(
            "duplicate storage identity or locator"
        ))
    ));
    let mut variants = Vec::new();
    let mut bad = original.clone();
    bad.committed.next_table_id = Some(TableId(91));
    variants.push(bad);
    let mut bad = original.clone();
    bad.committed.next_storage_id = Some(StorageId(9));
    variants.push(bad);
    let mut bad = original.clone();
    bad.committed.next_partition_id = Some(PartitionId(88));
    variants.push(bad);
    let mut bad = original.clone();
    bad.committed.tables[0].next_column_id = Some(ColumnId(42));
    variants.push(bad);
    let mut bad = original.clone();
    bad.storages[1].id = bad.storages[0].id;
    variants.push(bad);
    let mut bad = original.clone();
    bad.committed.tables[0].version = TableSchemaVersion(0);
    variants.push(bad);
    let mut bad = original.clone();
    bad.committed.generation = SchemaGeneration(0);
    variants.push(bad);
    let mut bad = original.clone();
    bad.storages[0].locator = "/absolute".into();
    variants.push(bad);
    let mut bad = original;
    bad.placements.tables[0].schema_fingerprint =
        netbadb_schema::SchemaFingerprint::from_bytes([0; 32]);
    variants.push(bad);
    for invalid in variants {
        assert!(invalid.encode().is_err());
    }
}

#[test]
fn codec_reviewed_seed_export() {
    let Some(destination) = std::env::var_os("NETBADB_SCHEMA_SEED_DIR") else {
        return;
    };
    let destination = PathBuf::from(destination);
    std::fs::create_dir_all(&destination).unwrap();
    let bytes = codec_fixture().encode().unwrap();
    std::fs::write(destination.join("valid-mixed-v1"), &bytes).unwrap();
    std::fs::write(destination.join("truncated-v1"), &bytes[..bytes.len() / 2]).unwrap();
    let mut invalid = bytes.clone();
    invalid[12] ^= 1;
    std::fs::write(destination.join("bad-crc-v1"), invalid).unwrap();
    let mut payload = bytes[16..].to_vec();
    payload[32..40].copy_from_slice(&1_u64.to_le_bytes());
    std::fs::write(
        destination.join("bad-table-high-water-v1"),
        envelope(b"NBSC", &payload).unwrap(),
    )
    .unwrap();
}

#[test]
fn catalog_only_reopen_preserves_schema_data_indexes_versions_and_bytes() {
    let f = Fixture::new("authority");
    let mut db = Database::create_catalog(f.catalog(), f.heap_specs(), None).unwrap();
    db.execute("INSERT INTO a (id, name) VALUES (1, 'Ada')")
        .unwrap();
    db.execute("INSERT INTO b (id, name) VALUES (2, NULL)")
        .unwrap();
    let ddl = db
        .prepare_ddl_statement("CREATE INDEX a_name ON a (name)")
        .unwrap();
    db.execute_ddl(&ddl).unwrap();
    assert_eq!(db.schema_generation(), SchemaGeneration(1));
    assert_eq!(
        db.table_schema_version(TableId(91)),
        Some(TableSchemaVersion(1))
    );
    db.close().unwrap();
    let before = std::fs::read(f.catalog()).unwrap();
    let marker = std::fs::read(file::marker_path(&f.catalog())).unwrap();
    for _ in 0..3 {
        let mut db = Database::open_catalog(f.catalog()).unwrap();
        assert_eq!(db.schema(), &f.schema());
        assert_eq!(db.next_table_id(), Some(TableId(92)));
        assert_eq!(db.next_storage_id(), Some(StorageId(3)));
        assert_eq!(db.next_column_id(TableId(91)), Some(ColumnId(43)));
        assert_eq!(db.schema_generation(), SchemaGeneration(1));
        assert_eq!(
            db.table_schema_version(TableId(91)),
            Some(TableSchemaVersion(1))
        );
        assert_eq!(
            db.query("SELECT name FROM a").unwrap().rows,
            vec![vec![ScalarValue::Text("Ada".into())]]
        );
        assert_eq!(db.inspect_catalog().unwrap().tables[0].indexes.len(), 1);
        let prepared = db.prepare_statement("SELECT id FROM b", &[]).unwrap();
        assert_eq!(prepared.description().columns.len(), 1);
        db.close().unwrap();
        assert_eq!(std::fs::read(f.catalog()).unwrap(), before);
        assert_eq!(
            std::fs::read(file::marker_path(&f.catalog())).unwrap(),
            marker
        );
    }
}

#[test]
fn expectation_subset_is_validation_only_and_cannot_rewrite_schema() {
    let f = Fixture::new("expectation");
    Database::create_catalog(f.catalog(), f.heap_specs(), None)
        .unwrap()
        .close()
        .unwrap();
    let subset = Schema::new(vec![table(7, "b")]).unwrap();
    let db = Database::open_catalog_with_expectation(f.catalog(), Some(&subset)).unwrap();
    assert_eq!(db.schema(), &f.schema());
    assert_eq!(db.inspect_catalog().unwrap().tables.len(), 2);
    db.close().unwrap();
    let bytes = std::fs::read(f.catalog()).unwrap();
    let mut changed = table(91, "a");
    changed.columns.push(ColumnDef::new(
        ColumnId(88),
        "evil_extra",
        TypeSpec::Physical(PhysicalType::Bool),
    ));
    let mut nominal = table(91, "a");
    nominal.columns[0].type_spec = TypeSpec::Semantic {
        name: "OtherId".into(),
        physical: PhysicalType::Int64,
    };
    for expected in [changed, nominal] {
        assert!(matches!(
            Database::open_catalog_with_expectation(
                f.catalog(),
                Some(&Schema::new(vec![expected]).unwrap())
            ),
            Err(DatabaseError::Storage(StorageError::SchemaMismatch { .. }))
        ));
    }
    assert!(matches!(
        Database::open_catalog_with_expectation(
            f.catalog(),
            Some(&Schema::new(vec![table(92, "a")]).unwrap())
        ),
        Err(DatabaseError::Storage(StorageError::TableIdMismatch { .. }))
    ));
    assert!(matches!(
        Database::open_catalog_with_expectation(
            f.catalog(),
            Some(&Schema::new(vec![table(92, "absent")]).unwrap())
        ),
        Err(DatabaseError::SchemaCatalog(
            SchemaCatalogError::ExpectationMissingTable(_)
        ))
    ));
    assert!(matches!(
        Database::open(f.path("b.db"), table(91, "a")),
        Err(DatabaseError::Storage(StorageError::TableIdMismatch {
            expected: TableId(91),
            actual: TableId(7)
        }))
    ));
    assert_eq!(std::fs::read(f.catalog()).unwrap(), bytes);
}

#[test]
fn initialized_missing_corrupt_version_and_marker_never_fall_back() {
    let f = Fixture::new("corrupt");
    Database::create_catalog(f.catalog(), f.heap_specs(), None)
        .unwrap()
        .close()
        .unwrap();
    let good = std::fs::read(f.catalog()).unwrap();
    std::fs::rename(f.catalog(), f.path("saved")).unwrap();
    for result in [
        Database::open_catalog(f.catalog()),
        Database::open(f.path("a.db"), table(91, "a")),
    ] {
        assert!(matches!(
            result,
            Err(DatabaseError::SchemaCatalog(
                SchemaCatalogError::SchemaCatalogMissing
            ))
        ));
    }
    assert!(matches!(
        Database::open_legacy_and_install_catalog(f.catalog(), f.schema(), f.inventory()),
        Err(DatabaseError::SchemaCatalog(
            SchemaCatalogError::AlreadyInitialized
        ))
    ));
    let mut corrupt = good.clone();
    corrupt[20] ^= 1;
    std::fs::write(f.catalog(), corrupt).unwrap();
    assert!(matches!(
        Database::open_catalog_with_expectation(f.catalog(), Some(&f.schema())),
        Err(DatabaseError::SchemaCatalog(
            SchemaCatalogError::SchemaCatalogCorrupt(_)
        ))
    ));
    let mut wrong = good.clone();
    wrong[4] = 2;
    std::fs::write(f.catalog(), wrong).unwrap();
    assert!(matches!(
        Database::open_catalog(f.catalog()),
        Err(DatabaseError::SchemaCatalog(
            SchemaCatalogError::UnsupportedVersion(2)
        ))
    ));
    std::fs::write(f.catalog(), good).unwrap();
    std::fs::write(file::marker_path(&f.catalog()), b"torn").unwrap();
    assert!(matches!(
        Database::open_catalog(f.catalog()),
        Err(DatabaseError::SchemaCatalog(
            SchemaCatalogError::SchemaCatalogCorrupt(_)
        ))
    ));
}

#[test]
fn explicit_legacy_complete_inventory_and_crash_retry_are_distinct_from_expectation() {
    let f = Fixture::new("legacy");
    f.legacy();
    expect_legacy(Database::open_catalog(f.catalog()));
    expect_legacy(Database::open(f.path("a.db"), table(91, "a")));
    let subset = Schema::new(vec![table(91, "a")]).unwrap();
    assert!(matches!(
        Database::open_legacy_and_install_catalog(f.catalog(), subset, f.inventory()),
        Err(DatabaseError::SchemaCatalog(
            SchemaCatalogError::IncompleteLegacyInventory
        ))
    ));
    assert!(!file::marker_path(&f.catalog()).exists());
    assert!(!f.catalog().exists());
    let mut wrong = f.schema().tables().to_vec();
    wrong[0].columns[0].primary_key = false;
    assert!(matches!(
        Database::open_legacy_and_install_catalog(
            f.catalog(),
            Schema::new(wrong).unwrap(),
            f.inventory()
        ),
        Err(DatabaseError::Storage(StorageError::SchemaMismatch { .. }))
    ));
    assert!(!file::marker_path(&f.catalog()).exists());
    Database::open_legacy_and_install_catalog(f.catalog(), f.schema(), f.inventory())
        .unwrap()
        .close()
        .unwrap();
    let mut db = Database::open_catalog(f.catalog()).unwrap();
    assert_eq!(
        db.query("SELECT id FROM b").unwrap().rows,
        vec![vec![ScalarValue::Int64(42)]]
    );
    db.close().unwrap();
    assert!(matches!(
        Database::open_legacy_and_install_catalog(f.catalog(), f.schema(), f.inventory()),
        Err(DatabaseError::SchemaCatalog(
            SchemaCatalogError::AlreadyInitialized
        ))
    ));
}

#[test]
fn lsm_and_partition_catalog_only_reopen_and_legacy_import() {
    let f = Fixture::new("engines");
    let catalog = f.path("mixed.schema");
    let specs = vec![
        TableStorageCreateSpec::heap(f.path("h"), table(91, "a")),
        TableStorageCreateSpec::lsm(f.path("lsm"), table(7, "b"), ColumnId(42)),
    ];
    let mut db = Database::create_catalog(
        &catalog,
        specs,
        Some(DatabaseCoordinatorConfig::new(f.path("coord"))),
    )
    .unwrap();
    db.execute("INSERT INTO b (id, name) VALUES (3, 'LSM')")
        .unwrap();
    db.close().unwrap();
    let mut db = Database::open_catalog(&catalog).unwrap();
    assert_eq!(
        db.query("SELECT name FROM b").unwrap().rows,
        vec![vec![ScalarValue::Text("LSM".into())]]
    );
    db.close().unwrap();
    let legacy_lsm = f.path("legacy-lsm");
    LsmStorage::create_with_storage_id(
        &legacy_lsm,
        table(43, "legacy"),
        ColumnId(42),
        StorageId(51),
    )
    .unwrap()
    .close()
    .unwrap();
    let legacy_catalog = f.path("legacy.schema");
    Database::open_legacy_and_install_catalog(
        &legacy_catalog,
        Schema::new(vec![table(43, "legacy")]).unwrap(),
        CompleteLegacyInventory::attest_complete(
            vec![LegacyStorageLocation::Lsm(legacy_lsm)],
            None,
            None,
        ),
    )
    .unwrap()
    .close()
    .unwrap();
    let db = Database::open_catalog(&legacy_catalog).unwrap();
    assert_eq!(db.next_storage_id(), Some(StorageId(52)));
    db.close().unwrap();
    let partition_catalog = f.path("range.schema");
    let config = PartitionCatalogConfig::new(f.path("ranges"), f.path("range-coord"));
    let specs = vec![TablePlacementSpec::range_partitioned(
        table(13, "events"),
        ColumnId(42),
        vec![
            RangePartitionSpec::new(
                PartitionId(19),
                f.path("p1"),
                None,
                Some(ScalarValue::Int64(0)),
            ),
            RangePartitionSpec::new(
                PartitionId(3),
                f.path("p2"),
                Some(ScalarValue::Int64(0)),
                None,
            ),
        ],
    )];
    let mut db =
        Database::create_catalog_with_placements(&partition_catalog, specs.clone(), config.clone())
            .unwrap();
    db.execute("INSERT INTO events (id, name) VALUES (-1, 'low')")
        .unwrap();
    db.execute("INSERT INTO events (id, name) VALUES (1, 'high')")
        .unwrap();
    db.close().unwrap();
    let mut db = Database::open_catalog(&partition_catalog).unwrap();
    assert_eq!(db.query("SELECT id FROM events").unwrap().rows.len(), 2);
    assert_eq!(db.next_partition_id(), Some(PartitionId(20)));
    db.close().unwrap();
    // Build a genuine legacy partition fixture through the private pre-catalog physical path.
    let legacy_config =
        PartitionCatalogConfig::new(f.path("legacy-ranges"), f.path("legacy-range-coord"));
    let legacy_specs = vec![TablePlacementSpec::range_partitioned(
        table(13, "events"),
        ColumnId(42),
        vec![RangePartitionSpec::new(
            PartitionId(49),
            f.path("legacy-p"),
            None,
            None,
        )],
    )];
    Database::physical_create_with_placements(legacy_specs, legacy_config.clone())
        .unwrap()
        .close()
        .unwrap();
    let legacy_root = f.path("legacy-range.schema");
    Database::open_legacy_and_install_catalog(
        &legacy_root,
        Schema::new(vec![table(13, "events")]).unwrap(),
        CompleteLegacyInventory::attest_complete(
            vec![LegacyStorageLocation::Heap(f.path("legacy-p"))],
            Some(DatabaseCoordinatorConfig::new(
                legacy_config.coordinator_log_path(),
            )),
            Some(legacy_config.catalog_path().to_owned()),
        ),
    )
    .unwrap()
    .close()
    .unwrap();
    let db = Database::open_catalog(&legacy_root).unwrap();
    assert_eq!(db.next_partition_id(), Some(PartitionId(50)));
    db.close().unwrap();
    // Persisted placement and the legacy evidence must agree; no rebuilding from evidence.
    let other = std::fs::read(legacy_config.catalog_path()).unwrap();
    std::fs::write(config.catalog_path(), other).unwrap();
    assert!(matches!(
        Database::open_catalog(&partition_catalog),
        Err(DatabaseError::SchemaCatalog(
            SchemaCatalogError::InventoryMismatch(_)
        ))
    ));
}

#[test]
fn missing_or_swapped_physical_files_never_create_empty_storages() {
    let f = Fixture::new("physical");
    Database::create_catalog(f.catalog(), f.heap_specs(), None)
        .unwrap()
        .close()
        .unwrap();
    std::fs::rename(f.path("a.db"), f.path("saved")).unwrap();
    assert!(Database::open_catalog(f.catalog()).is_err());
    assert!(!f.path("a.db").exists());
    std::fs::copy(f.path("b.db"), f.path("a.db")).unwrap();
    assert!(matches!(
        Database::open_catalog(f.catalog()),
        Err(DatabaseError::SchemaCatalog(
            SchemaCatalogError::InventoryMismatch(_)
        ))
    ));
    std::fs::write(f.path("unknown-staged-file"), b"do not delete").unwrap();
    assert!(Database::open_catalog(f.catalog()).is_err());
    assert_eq!(
        std::fs::read(f.path("unknown-staged-file")).unwrap(),
        b"do not delete"
    );
}

#[test]
fn explicit_empty_root_and_move_of_complete_database_directory() {
    let f = Fixture::new("move");
    let empty = f.path("empty.schema");
    Database::create_catalog(&empty, Vec::new(), None)
        .unwrap()
        .close()
        .unwrap();
    let db = Database::open_catalog(&empty).unwrap();
    assert!(db.schema().tables().is_empty());
    assert_eq!(db.next_table_id(), Some(TableId(1)));
    db.close().unwrap();
    let source = f.path("source");
    std::fs::create_dir(&source).unwrap();
    Database::create_catalog(
        source.join("db.schema"),
        vec![TableStorageCreateSpec::heap(
            source.join("a"),
            table(1, "a"),
        )],
        None,
    )
    .unwrap()
    .close()
    .unwrap();
    let target = f.path("moved");
    std::fs::rename(source, &target).unwrap();
    let db = Database::open_catalog(target.join("db.schema")).unwrap();
    assert_eq!(db.schema().tables(), &[table(1, "a")]);
    db.close().unwrap();
}

#[test]
fn durable_high_water_is_not_rederived_from_active_objects() {
    let f = Fixture::new("high-water");
    Database::create_catalog(f.catalog(), f.heap_specs(), None)
        .unwrap()
        .close()
        .unwrap();
    let mut snapshot = file::load(&f.catalog()).unwrap();
    snapshot.committed.next_table_id = Some(TableId(10001));
    snapshot.committed.next_storage_id = Some(StorageId(9001));
    snapshot.committed.next_partition_id = Some(PartitionId(5001));
    snapshot.committed.tables[0].next_column_id = Some(ColumnId(1001));
    let bytes = snapshot.encode().unwrap();
    let marker = std::fs::read(file::marker_path(&f.catalog())).unwrap();
    let mut payload = marker[16..].to_vec();
    let len = payload.len();
    payload[len - 4..].copy_from_slice(&crc32c::crc32c(&bytes).to_le_bytes());
    std::fs::write(f.catalog(), bytes).unwrap();
    std::fs::write(
        file::marker_path(&f.catalog()),
        envelope(b"NBSM", &payload).unwrap(),
    )
    .unwrap();
    let db = Database::open_catalog(f.catalog()).unwrap();
    assert_eq!(db.next_table_id(), Some(TableId(10001)));
    assert_eq!(db.next_storage_id(), Some(StorageId(9001)));
    assert_eq!(db.next_partition_id(), Some(PartitionId(5001)));
    assert_eq!(db.next_column_id(TableId(91)), Some(ColumnId(1001)));
    db.close().unwrap();
}

#[test]
fn bootstrap_crash_child() {
    let Ok(root) = std::env::var("NETBADB_SCHEMA_CRASH_ROOT") else {
        return;
    };
    let f = Fixture(PathBuf::from(root));
    if std::env::var("NETBADB_SCHEMA_CRASH_MODE").as_deref() == Ok("legacy") {
        Database::open_legacy_and_install_catalog(f.catalog(), f.schema(), f.inventory()).unwrap();
    } else {
        Database::create_catalog(f.catalog(), f.heap_specs(), None).unwrap();
    }
    panic!("crash hook did not fire");
}

#[test]
fn bootstrap_crash_winner_loser_matrix() {
    let points = [
        "before-catalog-write",
        "mid-snapshot-write",
        "after-shadow-sync",
        "after-snapshot-rename",
        "after-snapshot-durable",
        "before-initialized-marker",
        "mid-initialized-marker-write",
        "after-initialized-marker-shadow-sync",
        "after-initialized-marker-rename",
        "after-initialized-marker-durable",
        "before-return",
    ];
    for mode in ["fresh", "legacy"] {
        for point in points {
            let f = Fixture::new(&format!("crash-{mode}-{point}"));
            if mode == "legacy" {
                f.legacy();
            }
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "schema_catalog_tests::bootstrap_crash_child",
                    "--nocapture",
                ])
                .env("NETBADB_SCHEMA_CRASH_ROOT", &f.0)
                .env("NETBADB_SCHEMA_CRASH_MODE", mode)
                .env("NETBADB_SCHEMA_CRASH_POINT", point)
                .status()
                .unwrap();
            assert_eq!(status.code(), Some(89), "{mode}/{point}");
            let winner = matches!(
                point,
                "after-initialized-marker-rename"
                    | "after-initialized-marker-durable"
                    | "before-return"
            );
            if winner {
                Database::open_catalog(f.catalog())
                    .unwrap()
                    .close()
                    .unwrap();
            } else {
                expect_legacy(Database::open_catalog(f.catalog()));
                if point == "before-catalog-write" {
                    let reversed =
                        Schema::new(f.schema().tables().iter().rev().cloned().collect()).unwrap();
                    assert!(matches!(
                        Database::open_legacy_and_install_catalog(
                            f.catalog(),
                            reversed,
                            f.inventory()
                        ),
                        Err(DatabaseError::SchemaCatalog(
                            SchemaCatalogError::InventoryMismatch(_)
                        ))
                    ));
                    assert!(!file::marker(&f.catalog()).unwrap().unwrap().initialized);
                }
                Database::open_legacy_and_install_catalog(f.catalog(), f.schema(), f.inventory())
                    .unwrap()
                    .close()
                    .unwrap();
                Database::open_catalog(f.catalog())
                    .unwrap()
                    .close()
                    .unwrap();
            }
            assert!(file::marker(&f.catalog()).unwrap().unwrap().initialized);
        }
    }
}

#[test]
fn empty_catalog_cannot_hide_retained_coordinator_participants() {
    let f = Fixture::new("empty-coordinator");
    let coordinator = f.path("coordinator");
    Database::create_catalog(
        f.catalog(),
        Vec::new(),
        Some(DatabaseCoordinatorConfig::new(&coordinator)),
    )
    .unwrap()
    .close()
    .unwrap();
    let mut log = crate::CoordinatorLog::open(&coordinator).unwrap();
    log.commit_decision(
        netbadb_types::DatabaseTxnId(1),
        &[
            crate::coordinator_log::CoordinatorParticipant {
                storage_id: StorageId(8),
                physical_txn_id: netbadb_types::TxnId(1),
            },
            crate::coordinator_log::CoordinatorParticipant {
                storage_id: StorageId(9),
                physical_txn_id: netbadb_types::TxnId(1),
            },
        ],
    )
    .unwrap();
    log.complete(netbadb_types::DatabaseTxnId(1)).unwrap();
    drop(log);
    assert!(matches!(
        Database::open_catalog(f.catalog()),
        Err(DatabaseError::MissingCommitParticipant {
            storage_id: StorageId(8),
            ..
        })
    ));
}

#[test]
fn root_and_discovery_consistency_precedes_publication() {
    let f = Fixture::new("root-identity");
    assert!(matches!(
        Database::create_catalog(f.path("a.db"), f.heap_specs(), None),
        Err(DatabaseError::SchemaCatalog(
            SchemaCatalogError::PathConflict(_)
        ))
    ));
    assert!(!f.path("a.db").exists());
    assert!(!file::marker_path(&f.path("a.db")).exists());
    Database::create_catalog(f.catalog(), f.heap_specs(), None)
        .unwrap()
        .close()
        .unwrap();
    // A discovery link is not a per-table schema authority. Explicit root open
    // remains fully defined without it.
    std::fs::remove_file(file::link_path(&f.path("a.db"))).unwrap();
    Database::open_catalog(f.catalog())
        .unwrap()
        .close()
        .unwrap();
    let other = f.path("other.schema");
    Database::create_catalog(&other, Vec::new(), None)
        .unwrap()
        .close()
        .unwrap();
    std::fs::copy(other, f.catalog()).unwrap();
    assert!(matches!(
        Database::open_catalog(f.catalog()),
        Err(DatabaseError::SchemaCatalog(
            SchemaCatalogError::SchemaCatalogCorrupt(_)
        ))
    ));
}

#[cfg(unix)]
#[test]
fn relative_locators_preserve_symlink_parent_semantics() {
    let f = Fixture::new("symlink");
    std::fs::create_dir_all(f.path("actual/child")).unwrap();
    std::os::unix::fs::symlink(f.path("actual/child"), f.path("alias")).unwrap();
    let storage = f.path("alias/../table.db");
    Database::create_catalog(
        f.catalog(),
        vec![TableStorageCreateSpec::heap(storage, table(1, "a"))],
        None,
    )
    .unwrap()
    .close()
    .unwrap();
    assert!(f.path("actual/table.db").exists());
    let db = Database::open_catalog(f.catalog()).unwrap();
    assert_eq!(db.schema().tables(), &[table(1, "a")]);
    db.close().unwrap();
}

#[test]
fn missing_managed_marker_cannot_be_replaced_by_legacy_import() {
    let f = Fixture::new("missing-marker");
    Database::create_catalog(f.catalog(), f.heap_specs(), None)
        .unwrap()
        .close()
        .unwrap();
    let bytes = std::fs::read(f.catalog()).unwrap();
    std::fs::remove_file(file::marker_path(&f.catalog())).unwrap();
    expect_legacy(Database::open_catalog(f.catalog()));
    assert!(matches!(
        Database::open_legacy_and_install_catalog(f.catalog(), f.schema(), f.inventory()),
        Err(DatabaseError::SchemaCatalog(
            SchemaCatalogError::SchemaCatalogCorrupt(
                "catalog discovery link has no installation marker"
            )
        ))
    ));
    assert!(!file::marker_path(&f.catalog()).exists());
    assert_eq!(std::fs::read(f.catalog()).unwrap(), bytes);
}
