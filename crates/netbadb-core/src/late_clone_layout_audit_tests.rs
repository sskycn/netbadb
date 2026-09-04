use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use netbadb_schema::{ColumnDef, SchemaFingerprint, TableDef, TypeSpec};
use netbadb_types::{ColumnId, PhysicalType, ScalarValue, StorageId, TableId};

use super::*;
use crate::schema_mutation_journal::CompositionColumnReservation;

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProjectionTableIdentity {
    table_id: TableId,
    version: TableSchemaVersion,
    fingerprint: SchemaFingerprint,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ProjectionEntry {
    Source {
        column_id: ColumnId,
        source_position: usize,
    },
    SynthesizedNull {
        column_id: ColumnId,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LateCloneProjection {
    source_table: ProjectionTableIdentity,
    target_table: ProjectionTableIdentity,
    target_entries: Vec<ProjectionEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ProjectionError {
    InvalidSource,
    InvalidTarget,
    TableMismatch,
    UnreservedColumn(ColumnId),
    IncompatibleSurvivor(ColumnId),
    SourceIdentityMismatch,
    TargetIdentityMismatch,
    SourceRowWidth,
    NotNullViolation(ColumnId),
}

impl LateCloneProjection {
    fn build(
        source: &TableDef,
        source_version: TableSchemaVersion,
        target: &TableDef,
        target_version: TableSchemaVersion,
        reserved_new_columns: &BTreeSet<ColumnId>,
    ) -> Result<Self, ProjectionError> {
        source
            .validate()
            .map_err(|_| ProjectionError::InvalidSource)?;
        target
            .validate()
            .map_err(|_| ProjectionError::InvalidTarget)?;
        if source.id != target.id {
            return Err(ProjectionError::TableMismatch);
        }
        let mut target_entries = Vec::with_capacity(target.columns.len());
        for target_column in &target.columns {
            if let Some(source_position) = source
                .columns
                .iter()
                .position(|source_column| source_column.id == target_column.id)
            {
                let source_column = &source.columns[source_position];
                if source_column.semantic_type() != target_column.semantic_type() {
                    return Err(ProjectionError::IncompatibleSurvivor(target_column.id));
                }
                target_entries.push(ProjectionEntry::Source {
                    column_id: target_column.id,
                    source_position,
                });
            } else if reserved_new_columns.contains(&target_column.id) {
                target_entries.push(ProjectionEntry::SynthesizedNull {
                    column_id: target_column.id,
                });
            } else {
                return Err(ProjectionError::UnreservedColumn(target_column.id));
            }
        }
        Ok(Self {
            source_table: ProjectionTableIdentity {
                table_id: source.id,
                version: source_version,
                fingerprint: source
                    .fingerprint()
                    .map_err(|_| ProjectionError::InvalidSource)?,
            },
            target_table: ProjectionTableIdentity {
                table_id: target.id,
                version: target_version,
                fingerprint: target
                    .fingerprint()
                    .map_err(|_| ProjectionError::InvalidTarget)?,
            },
            target_entries,
        })
    }

    fn project(
        &self,
        source: &TableDef,
        source_version: TableSchemaVersion,
        target: &TableDef,
        target_version: TableSchemaVersion,
        source_values: &[ScalarValue],
    ) -> Result<Vec<ScalarValue>, ProjectionError> {
        let source_fingerprint = source
            .fingerprint()
            .map_err(|_| ProjectionError::InvalidSource)?;
        if self.source_table
            != (ProjectionTableIdentity {
                table_id: source.id,
                version: source_version,
                fingerprint: source_fingerprint,
            })
        {
            return Err(ProjectionError::SourceIdentityMismatch);
        }
        let target_fingerprint = target
            .fingerprint()
            .map_err(|_| ProjectionError::InvalidTarget)?;
        if self.target_table
            != (ProjectionTableIdentity {
                table_id: target.id,
                version: target_version,
                fingerprint: target_fingerprint,
            })
        {
            return Err(ProjectionError::TargetIdentityMismatch);
        }
        if source_values.len() != source.columns.len() {
            return Err(ProjectionError::SourceRowWidth);
        }
        let mut values = Vec::with_capacity(self.target_entries.len());
        for (target_column, entry) in target.columns.iter().zip(&self.target_entries) {
            let value = match entry {
                ProjectionEntry::Source {
                    column_id,
                    source_position,
                } => {
                    assert_eq!(*column_id, target_column.id);
                    source_values[*source_position].clone()
                }
                ProjectionEntry::SynthesizedNull { column_id } => {
                    assert_eq!(*column_id, target_column.id);
                    ScalarValue::Null
                }
            };
            if !target_column.nullable && matches!(value, ScalarValue::Null) {
                return Err(ProjectionError::NotNullViolation(target_column.id));
            }
            values.push(value);
        }
        Ok(values)
    }
}

fn column(id: u32, name: &str, physical: PhysicalType, nullable: bool) -> ColumnDef {
    ColumnDef::new(ColumnId(id), name, TypeSpec::Physical(physical)).nullable(nullable)
}

fn table(columns: Vec<ColumnDef>) -> TableDef {
    TableDef::new(TableId(40), "audit", columns)
}

fn root(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "netbadb-round40-{name}-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    std::fs::create_dir(&path).unwrap();
    path
}

fn resource_family_bytes(path: &Path) -> u64 {
    let prefix = path.file_name().unwrap().to_string_lossy();
    std::fs::read_dir(path.parent().unwrap())
        .unwrap()
        .map(|entry| entry.unwrap())
        .filter(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with(prefix.as_ref())
        })
        .map(|entry| entry.metadata().unwrap().len())
        .sum()
}

fn seed_source_backfill(root: &Path) -> Database {
    let mut db = Database::create_catalog(
        root.join("catalog"),
        vec![TableStorageCreateSpec::heap(
            root.join("seed.heap"),
            TableDef::new(
                TableId(1),
                "seed",
                vec![column(1, "id", PhysicalType::Int64, false)],
            ),
        )],
        Some(DatabaseCoordinatorConfig::new(root.join("coordinator"))),
    )
    .unwrap();
    db.execute("CREATE TABLE users (id BIGINT NOT NULL, email TEXT)")
        .unwrap();
    db.execute("INSERT INTO users VALUES (1, NULL)").unwrap();
    db.execute("CREATE INDEX users_email_idx ON users(email)")
        .unwrap();
    db
}

#[test]
fn pure_projection_is_column_id_driven_target_ordered_and_streamable() {
    let source = table(vec![
        column(1, "id", PhysicalType::Int64, false),
        column(2, "legacy", PhysicalType::Text, true),
        column(3, "email", PhysicalType::Text, true),
    ]);
    let target = table(vec![
        column(1, "id", PhysicalType::Int64, false),
        column(3, "canonical_email", PhysicalType::Text, true),
        column(4, "legacy", PhysicalType::Text, true),
    ]);
    let projection = LateCloneProjection::build(
        &source,
        TableSchemaVersion(1),
        &target,
        TableSchemaVersion(2),
        &BTreeSet::from([ColumnId(4)]),
    )
    .unwrap();
    assert_eq!(
        projection.target_entries,
        vec![
            ProjectionEntry::Source {
                column_id: ColumnId(1),
                source_position: 0,
            },
            ProjectionEntry::Source {
                column_id: ColumnId(3),
                source_position: 2,
            },
            ProjectionEntry::SynthesizedNull {
                column_id: ColumnId(4),
            },
        ]
    );

    let visible_source_rows = [
        vec![
            ScalarValue::Int64(1),
            ScalarValue::Text("old".into()),
            ScalarValue::Text("filled".into()),
        ],
        vec![
            ScalarValue::Int64(3),
            ScalarValue::Text("secret".into()),
            ScalarValue::Text("three".into()),
        ],
    ];
    let projected = visible_source_rows
        .iter()
        .map(|row| {
            projection
                .project(
                    &source,
                    TableSchemaVersion(1),
                    &target,
                    TableSchemaVersion(2),
                    row,
                )
                .unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        projected,
        vec![
            vec![
                ScalarValue::Int64(1),
                ScalarValue::Text("filled".into()),
                ScalarValue::Null,
            ],
            vec![
                ScalarValue::Int64(3),
                ScalarValue::Text("three".into()),
                ScalarValue::Null,
            ],
        ]
    );
}

#[test]
fn pure_projection_supports_multiple_null_additions_and_rejects_unsafe_identity() {
    let source = table(vec![column(1, "id", PhysicalType::Int64, false)]);
    let target = table(vec![
        column(1, "renamed_id", PhysicalType::Int64, false),
        column(4, "marker", PhysicalType::Text, true),
        column(5, "score", PhysicalType::Int64, true),
    ]);
    let projection = LateCloneProjection::build(
        &source,
        TableSchemaVersion(1),
        &target,
        TableSchemaVersion(2),
        &BTreeSet::from([ColumnId(4), ColumnId(5)]),
    )
    .unwrap();
    assert_eq!(
        projection
            .project(
                &source,
                TableSchemaVersion(1),
                &target,
                TableSchemaVersion(2),
                &[ScalarValue::Int64(7)],
            )
            .unwrap(),
        vec![ScalarValue::Int64(7), ScalarValue::Null, ScalarValue::Null,]
    );
    assert_eq!(
        projection.project(
            &source,
            TableSchemaVersion(9),
            &target,
            TableSchemaVersion(2),
            &[ScalarValue::Int64(7)],
        ),
        Err(ProjectionError::SourceIdentityMismatch)
    );
    assert_eq!(
        projection.project(
            &source,
            TableSchemaVersion(1),
            &target,
            TableSchemaVersion(9),
            &[ScalarValue::Int64(7)],
        ),
        Err(ProjectionError::TargetIdentityMismatch)
    );

    assert_eq!(
        LateCloneProjection::build(
            &source,
            TableSchemaVersion(1),
            &target,
            TableSchemaVersion(2),
            &BTreeSet::new(),
        ),
        Err(ProjectionError::UnreservedColumn(ColumnId(4)))
    );
    let incompatible = table(vec![column(1, "id", PhysicalType::Text, false)]);
    assert_eq!(
        LateCloneProjection::build(
            &source,
            TableSchemaVersion(1),
            &incompatible,
            TableSchemaVersion(2),
            &BTreeSet::new(),
        ),
        Err(ProjectionError::IncompatibleSurvivor(ColumnId(1)))
    );
    let other_table = TableDef::new(TableId(41), "audit", source.columns.clone());
    assert_eq!(
        LateCloneProjection::build(
            &source,
            TableSchemaVersion(1),
            &other_table,
            TableSchemaVersion(2),
            &BTreeSet::new(),
        ),
        Err(ProjectionError::TableMismatch)
    );
}

#[test]
fn projected_new_not_null_column_has_policy_b_empty_nonempty_semantics() {
    let source = table(vec![column(1, "id", PhysicalType::Int64, false)]);
    let target = table(vec![
        column(1, "id", PhysicalType::Int64, false),
        column(2, "marker", PhysicalType::Text, false),
    ]);
    let projection = LateCloneProjection::build(
        &source,
        TableSchemaVersion(1),
        &target,
        TableSchemaVersion(2),
        &BTreeSet::from([ColumnId(2)]),
    )
    .unwrap();

    let empty_rows: [Vec<ScalarValue>; 0] = [];
    assert!(empty_rows.iter().all(|row| {
        projection
            .project(
                &source,
                TableSchemaVersion(1),
                &target,
                TableSchemaVersion(2),
                row,
            )
            .is_ok()
    }));
    assert_eq!(
        projection.project(
            &source,
            TableSchemaVersion(1),
            &target,
            TableSchemaVersion(2),
            &[ScalarValue::Int64(1)],
        ),
        Err(ProjectionError::NotNullViolation(ColumnId(2)))
    );
}

#[test]
fn ordinary_rewrite_uses_one_final_heap_for_drop_add_same_name_and_rename() {
    let root = root("ordinary-physical-projection");
    let mut db = seed_source_backfill(&root);
    db.execute("CREATE TABLE audit_rows (id BIGINT NOT NULL, legacy TEXT, keep TEXT)")
        .unwrap();
    db.execute("INSERT INTO audit_rows VALUES (1, 'secret', 'one')")
        .unwrap();
    db.execute("INSERT INTO audit_rows VALUES (3, 'x', 'three')")
        .unwrap();
    let table_id = db.schema().table("audit_rows").unwrap().id;
    let source_storage = db.bindings.resolve_single(table_id).unwrap();
    let target_storage = db.next_storage_id().unwrap();
    let version = db.table_schema_version(table_id).unwrap();
    let generation = db.schema_generation();
    let revision = db.catalog_generation();
    let first_new_column = db.next_column_id(table_id).unwrap();

    let mut transaction = db.begin_transaction().unwrap();
    for sql in [
        "ALTER TABLE audit_rows DROP COLUMN legacy",
        "ALTER TABLE audit_rows ADD COLUMN legacy TEXT",
        "ALTER TABLE audit_rows RENAME COLUMN keep TO canonical_keep",
        "ALTER TABLE audit_rows ADD COLUMN score BIGINT",
    ] {
        db.execute_in(&mut transaction, sql).unwrap();
    }
    assert_eq!(db.next_storage_id(), Some(target_storage));
    db.commit_transaction(&mut transaction).unwrap();

    let final_table = db.schema().table("audit_rows").unwrap();
    assert_eq!(
        final_table
            .columns
            .iter()
            .map(|column| column.id)
            .collect::<Vec<_>>(),
        vec![
            ColumnId(1),
            ColumnId(3),
            first_new_column,
            ColumnId(first_new_column.0 + 1),
        ]
    );
    assert_eq!(
        db.query("SELECT id, canonical_keep, legacy, score FROM audit_rows ORDER BY id")
            .unwrap()
            .rows,
        vec![
            vec![
                ScalarValue::Int64(1),
                ScalarValue::Text("one".into()),
                ScalarValue::Null,
                ScalarValue::Null,
            ],
            vec![
                ScalarValue::Int64(3),
                ScalarValue::Text("three".into()),
                ScalarValue::Null,
                ScalarValue::Null,
            ],
        ]
    );
    assert_eq!(db.bindings.resolve_single(table_id), Ok(target_storage));
    assert_ne!(db.bindings.resolve_single(table_id), Ok(source_storage));
    assert_eq!(
        db.table_schema_version(table_id),
        Some(TableSchemaVersion(version.0 + 1))
    );
    assert_eq!(db.schema_generation(), SchemaGeneration(generation.0 + 1));
    assert_eq!(db.catalog_generation(), revision + 1);
    assert_eq!(db.next_storage_id(), Some(StorageId(target_storage.0 + 1)));
    assert_eq!(db.inspect_replacement_retired_heaps().len(), 1);

    db.close().unwrap();
    let mut reopened = Database::open_catalog(root.join("catalog")).unwrap();
    assert_eq!(
        reopened.bindings.resolve_single(table_id),
        Ok(target_storage)
    );
    assert_eq!(
        reopened
            .query("SELECT legacy FROM audit_rows ORDER BY id")
            .unwrap()
            .rows,
        vec![vec![ScalarValue::Null], vec![ScalarValue::Null]]
    );
    reopened.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn add_drop_and_combined_projection_each_allocate_one_target_family() {
    for (name, statements) in [
        (
            "add",
            &["ALTER TABLE audit_rows ADD COLUMN marker TEXT"][..],
        ),
        ("drop", &["ALTER TABLE audit_rows DROP COLUMN obsolete"][..]),
        (
            "combined",
            &[
                "ALTER TABLE audit_rows DROP COLUMN obsolete",
                "ALTER TABLE audit_rows ADD COLUMN marker TEXT",
            ][..],
        ),
    ] {
        let root = root(&format!("physical-{name}"));
        let mut db = seed_source_backfill(&root);
        db.execute("CREATE TABLE audit_rows (id BIGINT NOT NULL, obsolete TEXT, keep TEXT)")
            .unwrap();
        db.execute("INSERT INTO audit_rows VALUES (1, 'old', 'one')")
            .unwrap();
        db.execute("INSERT INTO audit_rows VALUES (2, 'x', 'two')")
            .unwrap();
        db.flush().unwrap();
        let table_id = db.schema().table("audit_rows").unwrap().id;
        let catalog_path = root.join("catalog");
        let before = crate::schema_catalog_file::load(&catalog_path).unwrap();
        let source_storage = db.bindings.resolve_single(table_id).unwrap();
        let target_storage = db.next_storage_id().unwrap();
        let source_locator = &before
            .storages
            .iter()
            .find(|storage| storage.id == source_storage)
            .unwrap()
            .locator;
        let source_path = crate::schema_catalog_file::resolve(&catalog_path, source_locator);
        let source_bytes = resource_family_bytes(&source_path);

        let mut transaction = db.begin_transaction().unwrap();
        for statement in statements {
            db.execute_in(&mut transaction, statement).unwrap();
        }
        assert_eq!(db.next_storage_id(), Some(target_storage));
        db.commit_transaction(&mut transaction).unwrap();

        let after = crate::schema_catalog_file::load(&catalog_path).unwrap();
        let target_locator = &after
            .storages
            .iter()
            .find(|storage| storage.id == target_storage)
            .unwrap()
            .locator;
        let target_path = crate::schema_catalog_file::resolve(&catalog_path, target_locator);
        let target_bytes = resource_family_bytes(&target_path);
        assert_eq!(db.bindings.resolve_single(table_id), Ok(target_storage));
        assert_eq!(db.next_storage_id(), Some(StorageId(target_storage.0 + 1)));
        assert_eq!(
            db.inspect_replacement_retired_heaps()
                .iter()
                .filter(|retired| retired.table_id == table_id)
                .count(),
            1
        );
        eprintln!(
            "ROUND40_PHYSICAL_COST case={name} source_storage={} source_bytes={source_bytes} target_storage={} target_bytes={target_bytes} target_ids=1",
            source_storage.0, target_storage.0,
        );
        db.close().unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn current_column_reservation_record_rejects_append_after_source_dml() {
    let root = root("reservation-after-source-dml");
    let mut db = seed_source_backfill(&root);
    let users = db.schema().table("users").unwrap().id;
    let next_column = db.next_column_id(users).unwrap();
    let mut transaction = db.begin_transaction().unwrap();
    db.execute_in(&mut transaction, "DROP INDEX users_email_idx")
        .unwrap();
    db.execute_in(&mut transaction, "SELECT id FROM users")
        .unwrap();
    assert!(matches!(
        db.execute_in(
            &mut transaction,
            "ALTER TABLE users ALTER COLUMN email SET NOT NULL",
        ),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::NotNullViolation(ColumnId(2))
        ))
    ));
    assert!(matches!(
        transaction.schema_composition,
        crate::schema_composition::SchemaCompositionState::SourceBackfillOpen(_)
    ));

    let reservation = CompositionColumnReservation {
        transaction: transaction.id(),
        table: users,
        column: next_column,
        next_column_id: Some(ColumnId(next_column.0 + 1)),
    };
    assert!(matches!(
        db.mutation_journal
            .as_ref()
            .unwrap()
            .borrow_mut()
            .reserve_composition_column(reservation.clone()),
        Err(SchemaMutationError::Corrupt(
            "duplicate or out-of-order composition reservation"
        ))
    ));
    assert_eq!(db.next_column_id(users), Some(next_column));

    // The state-machine API rejects the append today, but the existing NBSJ
    // record shape can canonically encode and decode tag 16 beside the already
    // durable tag 25 intent. Round 41 therefore needs no new record tag.
    let mut representable = db.mutation_journal.as_ref().unwrap().borrow().clone();
    representable
        .compositions
        .get_mut(&transaction.id())
        .unwrap()
        .reservations
        .push(reservation);
    let decoded = crate::schema_mutation_journal::SchemaMutationJournal::decode(
        &representable.encode().unwrap(),
    )
    .unwrap();
    assert_eq!(
        decoded.effective_column(users, Some(next_column)),
        Some(ColumnId(next_column.0 + 1))
    );
    assert!(
        decoded.compositions[&transaction.id()]
            .index_intent
            .is_some()
    );
    transaction.rollback().unwrap();
    drop(transaction);
    db.close().unwrap();
    let reopened = Database::open_catalog(root.join("catalog")).unwrap();
    assert_eq!(reopened.next_column_id(users), Some(next_column));
    reopened.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn existing_indexed_drop_dependency_check_is_column_id_exact() {
    let root = root("drop-dependencies");
    let mut db = seed_source_backfill(&root);
    let users = db.schema().table("users").unwrap().id;
    assert!(matches!(
        db.execute("ALTER TABLE users DROP COLUMN email"),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::IndexedColumn(ColumnId(2))
        ))
    ));
    db.execute("DROP INDEX users_email_idx").unwrap();
    db.execute("ALTER TABLE users DROP COLUMN email").unwrap();
    assert!(
        db.schema()
            .table("users")
            .unwrap()
            .column("email")
            .is_none()
    );
    assert!(db.indexes(users).unwrap().is_empty());
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn semantic_type_changes_are_not_projection_compatible() {
    let source = TableDef::new(
        TableId(40),
        "audit",
        vec![ColumnDef::new(
            ColumnId(1),
            "id",
            TypeSpec::Semantic {
                physical: PhysicalType::Int64,
                name: "UserId".into(),
            },
        )],
    );
    let target = TableDef::new(
        TableId(40),
        "audit",
        vec![ColumnDef::new(
            ColumnId(1),
            "id",
            TypeSpec::Semantic {
                physical: PhysicalType::Int64,
                name: "TeamId".into(),
            },
        )],
    );
    assert_eq!(
        LateCloneProjection::build(
            &source,
            TableSchemaVersion(1),
            &target,
            TableSchemaVersion(2),
            &BTreeSet::new(),
        ),
        Err(ProjectionError::IncompatibleSurvivor(ColumnId(1)))
    );
}
