//! Round 47 architecture evidence promoted to Round 48 production regression tests.
use super::*;
use crate::{
    CompiledDdlStatement, DdlOutcome, DropIndexTarget, PreparedSqlStatement, TypedCreateIndex,
};
use netbadb_types::IndexName;
use std::path::Path;

// Fixture convenience only: every acceptance, phase gate and finalization below
// calls production Core. There is no separate test correctness implementation.
struct FinalIndexFixture {
    transaction: Transaction,
}

impl FinalIndexFixture {
    fn new(transaction: Transaction) -> Self {
        Self { transaction }
    }

    fn logical_mut(&mut self) -> &mut SchemaTransactionPlan {
        &mut self
            .transaction
            .schema_composition
            .adopted_source_mut()
            .unwrap()
            .logical
    }

    fn prepare(&self, db: &Database, sql: &str) -> TypedCreateIndex {
        let PreparedSqlStatement::Ddl(prepared) = db
            .prepare_sql_statement_in(&self.transaction, sql, &[])
            .unwrap()
        else {
            panic!("DDL")
        };
        let CompiledDdlStatement::CreateIndex(statement) = prepared.compiled else {
            panic!("CREATE")
        };
        statement
    }

    fn create(
        &mut self,
        db: &mut Database,
        statement: &TypedCreateIndex,
    ) -> Result<DdlOutcome, DatabaseError> {
        db.compose_create_index_in(&mut self.transaction, statement)
    }

    fn drop_index(
        &mut self,
        db: &mut Database,
        target: DropIndexTarget,
    ) -> Result<DdlOutcome, DatabaseError> {
        db.compose_drop_index_in(&mut self.transaction, target)
    }

    fn execute(&mut self, db: &mut Database, sql: &str) -> Result<(), DatabaseError> {
        db.execute_in(&mut self.transaction, sql).map(|_| ())
    }

    fn finalize(&mut self, db: &mut Database) -> Result<(), DatabaseError> {
        db.finalize_adopted_source(&mut self.transaction)
    }
}

fn root(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "netbadb-round47-{name}-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    std::fs::create_dir(&path).unwrap();
    path
}

fn seed(root: &Path) -> Database {
    let mut db = Database::create_catalog(
        root.join("catalog"),
        vec![crate::TableStorageCreateSpec::heap(
            root.join("seed.heap"),
            TableDef::new(
                TableId(1),
                "seed",
                vec![ColumnDef::new(
                    ColumnId(1),
                    "id",
                    TypeSpec::Physical(netbadb_types::PhysicalType::Int64),
                )],
            ),
        )],
        Some(crate::DatabaseCoordinatorConfig::new(
            root.join("coordinator"),
        )),
    )
    .unwrap();
    db.execute("CREATE TABLE users (id BIGINT NOT NULL, legacy TEXT, email TEXT)")
        .unwrap();
    for sql in [
        "INSERT INTO users VALUES (1, 'old1', 'one')",
        "INSERT INTO users VALUES (2, 'old2', 'two')",
        "INSERT INTO users VALUES (3, 'old3', 'three')",
        "CREATE INDEX users_email_idx ON users(email)",
    ] {
        db.execute(sql).unwrap();
    }
    db
}

fn adopt(db: &mut Database, schema: &str) -> FinalIndexFixture {
    let mut transaction = db.begin_transaction().unwrap();
    for sql in [
        "UPDATE users SET legacy = 'updated1' WHERE id = 1",
        "INSERT INTO users VALUES (4, 'inserted4', 'four')",
        "DELETE FROM users WHERE id = 2",
    ] {
        db.execute_in(&mut transaction, sql).unwrap();
    }
    for sql in schema.split(';').filter(|sql| !sql.trim().is_empty()) {
        db.execute_in(&mut transaction, sql).unwrap();
    }
    FinalIndexFixture::new(transaction)
}

const NOOP: &str =
    "ALTER TABLE users RENAME COLUMN email TO tmp; ALTER TABLE users RENAME COLUMN tmp TO email";
const ADD: &str = "ALTER TABLE users ADD COLUMN marker TEXT";
const CREATE: &str = "CREATE INDEX users_legacy_idx ON users(legacy)";

fn bytes(path: &Path) -> u64 {
    std::fs::read_dir(path)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                bytes(&entry.path())
            } else {
                entry.metadata().unwrap().len()
            }
        })
        .sum()
}

fn journal(db: &Database) -> Vec<u8> {
    db.mutation_journal
        .as_ref()
        .unwrap()
        .borrow()
        .encode()
        .unwrap()
}
fn snapshot(db: &Database) -> SchemaCatalogSnapshot {
    file::load(db.catalog_path.as_ref().unwrap()).unwrap()
}
fn source_digest(db: &mut Database, storage: StorageId) -> [u8; 32] {
    crate::schema_mutation_journal::heap_rewrite_indexes_digest(
        &db.registry
            .get_mut(storage)
            .unwrap()
            .heap_rewrite_indexes()
            .unwrap(),
    )
    .unwrap()
}
fn target(db: &Database, phase: &FinalIndexFixture, name: &str) -> DropIndexTarget {
    db.index_name_bindings(Some(&phase.transaction))
        .into_iter()
        .find(|binding| binding.name.as_str() == name)
        .unwrap()
        .target
}
fn create(db: &mut Database, phase: &mut FinalIndexFixture, sql: &str) {
    let statement = phase.prepare(db, sql);
    assert_eq!(phase.create(db, &statement).unwrap(), DdlOutcome::Created);
}
fn assert_rows(db: &mut Database, winner: bool) {
    let expected = if winner {
        vec![(1, "updated1"), (3, "old3"), (4, "inserted4")]
    } else {
        vec![(1, "old1"), (2, "old2"), (3, "old3")]
    };
    assert_eq!(
        db.query("SELECT id, legacy FROM users ORDER BY id")
            .unwrap()
            .rows,
        expected
            .iter()
            .map(|(id, value)| vec![ScalarValue::Int64(*id), ScalarValue::Text((*value).into())])
            .collect::<Vec<_>>()
    );
    for (id, value) in expected {
        if db
            .indexes(TableId(2))
            .unwrap()
            .iter()
            .any(|index| index.column_id == ColumnId(2))
        {
            let storage = db.bindings.resolve_single(TableId(2)).unwrap();
            assert_physical_keys(
                db,
                storage,
                ColumnId(2),
                &ScalarValue::Text(value.into()),
                &[id],
            );
        }
        assert_eq!(
            db.query(&format!("SELECT id FROM users WHERE legacy = '{value}'"))
                .unwrap()
                .rows,
            [vec![ScalarValue::Int64(id)]]
        );
    }
    if winner {
        if db
            .indexes(TableId(2))
            .unwrap()
            .iter()
            .any(|index| index.column_id == ColumnId(2))
        {
            let storage = db.bindings.resolve_single(TableId(2)).unwrap();
            assert_physical_keys(
                db,
                storage,
                ColumnId(2),
                &ScalarValue::Text("old2".into()),
                &[],
            );
        }
        assert!(
            db.query("SELECT id FROM users WHERE legacy = 'old2'")
                .unwrap()
                .rows
                .is_empty()
        );
    }
}

#[test]
fn hybrid_publication_and_transaction_visible_index_matrix() {
    for (name, schema, ddl, dirty) in [
        (
            "cnew",
            ADD,
            "CREATE INDEX marker_idx ON users(marker)",
            true,
        ),
        (
            "survivor",
            "ALTER TABLE users RENAME COLUMN email TO contact",
            CREATE,
            true,
        ),
        ("rename-back", NOOP, CREATE, false),
        (
            "set-drop",
            "ALTER TABLE users ALTER COLUMN email SET NOT NULL; ALTER TABLE users ALTER COLUMN email DROP NOT NULL",
            CREATE,
            false,
        ),
        (
            "add-drop",
            "ALTER TABLE users ADD COLUMN marker TEXT; ALTER TABLE users DROP COLUMN marker",
            CREATE,
            false,
        ),
    ] {
        let root = root(name);
        let mut db = seed(&root);
        let base = snapshot(&db);
        let revision = db.catalog_generation;
        let source = db.bindings.resolve_single(TableId(2)).unwrap();
        let floor = db.next_storage_id().unwrap();
        let mut phase = adopt(&mut db, schema);
        let digest = source_digest(&mut db, source);
        let physical_txn = phase.transaction.physical_transaction_id(source).unwrap();
        let column_floor = phase.logical_mut().overlay.tables[1].next_column_id;
        let before_prepare = journal(&db);
        let statement = phase.prepare(&db, ddl);
        assert_eq!(journal(&db), before_prepare);
        phase.create(&mut db, &statement).unwrap();
        let reservation = db.mutation_journal.as_ref().unwrap().borrow().compositions
            [&phase.transaction.id()]
            .index_reservations[0]
            .clone();
        assert_eq!(
            reservation.table_version.0,
            base.committed.tables[1].version.0 + u64::from(dirty)
        );
        assert_eq!(reservation.fingerprint, statement.target.fingerprint);
        assert_eq!(
            phase.logical_mut().overlay.tables[1].next_column_id,
            column_floor
        );
        assert_eq!(source_digest(&mut db, source), digest);
        assert_eq!(db.next_storage_id(), Some(floor));
        let before = bytes(&root);
        phase.finalize(&mut db).unwrap();
        let peak = bytes(&root);
        assert_eq!(
            phase.transaction.physical_transaction_id(source),
            Some(physical_txn)
        );
        if dirty {
            let materialized = phase
                .transaction
                .schema_composition
                .source_backfill()
                .unwrap();
            assert_eq!(
                (
                    materialized.source_copy_passes,
                    materialized.source_rows_copied
                ),
                (1, 3)
            );
        } else {
            let materialized = phase
                .transaction
                .schema_composition
                .materialized_index()
                .unwrap();
            assert_eq!(materialized.source_copy_passes, 0);
            assert_eq!(materialized.source_rows_copied, 0);
            assert!(phase.transaction.is_only_participant(source));
            assert!(phase.transaction.is_only_write_participant(source));
            assert!(materialized.target.is_none());
            assert!(materialized.reference.is_none());
            assert!(materialized.staged.is_empty());
            let j = db.mutation_journal.as_ref().unwrap().borrow();
            assert!(
                !j.source_backfill_intents
                    .contains_key(&phase.transaction.id())
            );
            assert!(!j.stage_intents.contains_key(&phase.transaction.id()));
            assert!(
                !j.migration_finalization_intents
                    .contains_key(&phase.transaction.id())
            );
        }
        db.commit_transaction(&mut phase.transaction).unwrap();
        let final_snapshot = snapshot(&db);
        assert_eq!(final_snapshot.epoch, base.epoch + u64::from(dirty));
        assert_eq!(
            final_snapshot.committed.generation.0,
            base.committed.generation.0 + u64::from(dirty)
        );
        assert_eq!(
            final_snapshot.committed.tables[1].version.0,
            base.committed.tables[1].version.0 + u64::from(dirty)
        );
        assert_eq!(db.catalog_generation, revision + 1);
        assert_eq!(
            db.bindings.resolve_single(TableId(2)),
            Ok(if dirty { floor } else { source })
        );
        assert_eq!(
            db.next_storage_id(),
            Some(StorageId(floor.0 + u64::from(dirty)))
        );
        assert_rows(&mut db, true);
        if !dirty {
            let netbadb_inspect::StatementPlanInspection::Query { root } = db
                .inspect_statement("SELECT id FROM users WHERE legacy = 'updated1'")
                .unwrap()
                .plan
            else {
                panic!("query")
            };
            assert!(plan_uses_index(&root));
        }
        if name == "cnew" {
            assert_physical_keys(
                &mut db,
                floor,
                statement.column_id,
                &ScalarValue::Null,
                &[1, 3, 4],
            );
            db.analyze(TableId(2)).unwrap();
            assert_eq!(
                db.registry
                    .get(floor)
                    .unwrap()
                    .index_statistics(statement.column_id)
                    .unwrap()
                    .null_count,
                3
            );
            assert_eq!(
                db.query("SELECT id FROM users WHERE marker IS NULL ORDER BY id")
                    .unwrap()
                    .rows,
                [
                    vec![ScalarValue::Int64(1)],
                    vec![ScalarValue::Int64(3)],
                    vec![ScalarValue::Int64(4)]
                ]
            );
        }
        println!(
            "ROUND47 {name} source={source:?} target={:?} before={before} peak={peak} copy_passes={} rows_copied={}",
            db.bindings.resolve_single(TableId(2)),
            u8::from(dirty),
            if dirty { 3 } else { 0 }
        );
        db.close().unwrap();
        for _ in 0..3 {
            let mut reopened = Database::open_catalog(root.join("catalog")).unwrap();
            assert_rows(&mut reopened, true);
            assert_eq!(reopened.indexes(TableId(2)).unwrap().len(), 2);
            if name == "cnew" {
                assert_physical_keys(
                    &mut reopened,
                    floor,
                    statement.column_id,
                    &ScalarValue::Null,
                    &[1, 3, 4],
                );
            }
            reopened.close().unwrap();
        }
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn terminal_identity_visibility_noop_and_multiple_indexes() {
    for (name, replacement, multiple) in [
        ("global-noop", false, false),
        ("replacement", true, false),
        ("multiple", false, true),
    ] {
        let root = root(name);
        let mut db = seed(&root);
        let base = snapshot(&db);
        let revision = db.catalog_generation;
        let source = db.bindings.resolve_single(TableId(2)).unwrap();
        let floor = db.next_storage_id();
        let mut phase = adopt(&mut db, NOOP);
        let digest = source_digest(&mut db, source);
        let prepared_bytes = journal(&db);
        let PreparedSqlStatement::Ddl(prepared_drop) = db
            .prepare_sql_statement_in(&phase.transaction, "DROP INDEX users_email_idx", &[])
            .unwrap()
        else {
            panic!("DDL")
        };
        let CompiledDdlStatement::DropIndex(prepared_drop) = prepared_drop.compiled else {
            panic!("DROP")
        };
        assert_eq!(journal(&db), prepared_bytes);
        let old = prepared_drop.target.unwrap();
        if replacement {
            phase.drop_index(&mut db, old).unwrap();
            create(
                &mut db,
                &mut phase,
                "CREATE INDEX users_email_idx ON users(email)",
            );
            assert!(matches!(
                phase.drop_index(&mut db, old),
                Err(DatabaseError::UndefinedIndex)
            ));
            assert_ne!(
                target(&db, &phase, "users_email_idx").index_id,
                old.index_id
            );
        } else {
            create(&mut db, &mut phase, CREATE);
            let new = target(&db, &phase, "users_legacy_idx");
            assert!(db.logical_index(&phase.transaction, new).is_some());
            let duplicate = phase.prepare(&db, CREATE);
            assert!(matches!(
                phase.create(&mut db, &duplicate),
                Err(DatabaseError::DuplicateIndexName(_))
            ));
            let unchanged = phase.prepare(
                &db,
                "CREATE INDEX IF NOT EXISTS users_legacy_idx ON users(legacy)",
            );
            assert_eq!(
                phase.create(&mut db, &unchanged).unwrap(),
                DdlOutcome::Unchanged
            );
            if multiple {
                create(
                    &mut db,
                    &mut phase,
                    "CREATE INDEX users_id_idx ON users(id)",
                );
            } else {
                phase.drop_index(&mut db, new).unwrap();
            }
        }
        for sql in [
            ADD,
            "ALTER TABLE users RENAME COLUMN email TO contact",
            "ALTER TABLE users ALTER COLUMN email SET NOT NULL",
            "ALTER TABLE users DROP COLUMN legacy",
            "SELECT id FROM users",
            "UPDATE users SET legacy = legacy",
        ] {
            assert!(matches!(
                phase.execute(&mut db, sql),
                Err(DatabaseError::SchemaMutation(
                    SchemaMutationError::SchemaMutationAfterMaterialization
                        | SchemaMutationError::MigrationDataAccessAfterRefinement
                ))
            ));
        }
        assert_eq!(source_digest(&mut db, source), digest);
        phase.finalize(&mut db).unwrap();
        if !replacement && !multiple {
            assert!(matches!(
                phase.transaction.schema_composition,
                SchemaCompositionState::SealedNoEffectiveChange(_)
            ));
            assert!(
                db.mutation_journal.as_ref().unwrap().borrow().compositions
                    [&phase.transaction.id()]
                    .index_intent
                    .is_none()
            );
        }
        db.commit_transaction(&mut phase.transaction).unwrap();
        assert_eq!(snapshot(&db), base);
        assert_eq!(db.next_storage_id(), floor);
        assert_eq!(
            db.catalog_generation,
            revision + u64::from(replacement || multiple)
        );
        assert_eq!(db.bindings.resolve_single(TableId(2)), Ok(source));
        assert_rows(&mut db, true);
        let count = if multiple { 3 } else { 1 };
        assert_eq!(db.indexes(TableId(2)).unwrap().len(), count);
        let expected_floor = if multiple { IndexId(4) } else { IndexId(3) };
        assert_eq!(
            db.mutation_journal
                .as_ref()
                .unwrap()
                .borrow()
                .effective_index(TableId(2), IndexId(2)),
            Some(expected_floor)
        );
        db.close().unwrap();
        for _ in 0..3 {
            let reopened = Database::open_catalog(root.join("catalog")).unwrap();
            assert_eq!(reopened.indexes(TableId(2)).unwrap().len(), count);
            assert_eq!(
                reopened
                    .mutation_journal
                    .as_ref()
                    .unwrap()
                    .borrow()
                    .effective_index(TableId(2), IndexId(2)),
                Some(expected_floor)
            );
            reopened.close().unwrap();
        }
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn tag33_cannot_precede_aggregate_intent_and_tag24_canonical_lineage_is_strict() {
    let root = root("reservation-theorem");
    let mut db = seed(&root);
    let mut phase = adopt(&mut db, NOOP);
    create(&mut db, &mut phase, CREATE);
    // Decode/encode through actual production validation, including rejected
    // candidate histories. No disk mutation or weakened decoder is involved.
    let encoded = journal(&db);
    let mut candidate = SchemaMutationJournal::decode(&encoded).unwrap();
    let record = candidate
        .compositions
        .get_mut(&phase.transaction.id())
        .unwrap();
    record.migration_index_reservations = std::mem::take(&mut record.index_reservations);
    assert!(matches!(
        candidate.encode(),
        Err(SchemaMutationError::Corrupt(
            "migration IndexId reservation without composition"
        ))
    ));
    phase.finalize(&mut db).unwrap();
    let good = journal(&db);
    assert_eq!(
        SchemaMutationJournal::decode(&good)
            .unwrap()
            .encode()
            .unwrap(),
        good
    );
    for corruption in 0..6 {
        let mut candidate = SchemaMutationJournal::decode(&good).unwrap();
        let record = candidate
            .compositions
            .get_mut(&phase.transaction.id())
            .unwrap();
        match corruption {
            0 => record.index_reservations[0].table_version.0 += 1,
            1 => {
                record.index_reservations[0].fingerprint =
                    netbadb_schema::SchemaFingerprint::from_bytes([0x47; 32])
            }
            2 => record.index_reservations[0].table = TableId(999),
            3 => record
                .index_reservations
                .push(record.index_reservations[0].clone()),
            4 => {
                record.index_reservations[0].next_index_id =
                    Some(record.index_reservations[0].index)
            }
            _ => record.index_reservations.clear(),
        }
        assert!(candidate.encode().is_err(), "corruption {corruption}");
    }
    for length in [0, 1, good.len() / 2, good.len() - 1] {
        assert!(SchemaMutationJournal::decode(&good[..length]).is_err());
    }
    phase.transaction.rollback().unwrap();
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn stale_prepared_create_never_burns_or_rebinds() {
    let root = root("prepared");
    let mut db = seed(&root);
    let mut phase = adopt(&mut db, "ALTER TABLE users RENAME COLUMN email TO contact");
    let old = phase.prepare(&db, CREATE);
    phase
        .execute(&mut db, "ALTER TABLE users RENAME COLUMN contact TO email")
        .unwrap();
    let before = journal(&db);
    assert!(matches!(
        phase.create(&mut db, &old),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::StaleSchemaDependency
        ))
    ));
    assert_eq!(journal(&db), before);
    assert!(matches!(
        phase.transaction.schema_composition,
        SchemaCompositionState::AdoptedSourceRefining(_)
    ));
    create(&mut db, &mut phase, CREATE);
    phase.transaction.rollback().unwrap();
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn drop_existing_and_recreate_renamed_survivor() {
    for (name, schema, recreate) in [
        (
            "drop-effective",
            "ALTER TABLE users RENAME COLUMN legacy TO old_value",
            false,
        ),
        ("drop-noop", NOOP, false),
        (
            "renamed-index",
            "ALTER TABLE users RENAME COLUMN email TO contact",
            true,
        ),
    ] {
        let root = root(name);
        let mut db = seed(&root);
        let base = snapshot(&db);
        let revision = db.catalog_generation;
        let source = db.bindings.resolve_single(TableId(2)).unwrap();
        let floor = db.next_storage_id().unwrap();
        let mut phase = adopt(&mut db, schema);
        let digest = source_digest(&mut db, source);
        let old = target(&db, &phase, "users_email_idx");
        phase.drop_index(&mut db, old).unwrap();
        if recreate {
            create(
                &mut db,
                &mut phase,
                "CREATE INDEX users_contact2_idx ON users(contact)",
            );
            assert_eq!(
                phase.logical_mut().touched[&TableId(2)].indexes.active[0].column_id,
                ColumnId(3)
            );
        }
        assert_eq!(source_digest(&mut db, source), digest);
        phase.finalize(&mut db).unwrap();
        db.commit_transaction(&mut phase.transaction).unwrap();
        let dirty = schema != NOOP;
        assert_eq!(
            db.bindings.resolve_single(TableId(2)),
            Ok(if dirty { floor } else { source })
        );
        assert_eq!(db.catalog_generation, revision + 1);
        assert_eq!(snapshot(&db).epoch, base.epoch + u64::from(dirty));
        db.close().unwrap();
        for _ in 0..3 {
            let reopened = Database::open_catalog(root.join("catalog")).unwrap();
            let indexes = reopened.indexes(TableId(2)).unwrap();
            assert_eq!(indexes.len(), usize::from(recreate));
            if recreate {
                assert_eq!(indexes[0].id, IndexId(2));
                assert_eq!(indexes[0].column_id, ColumnId(3));
            }
            reopened.close().unwrap();
        }
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn rollback_matrix_before_and_after_materialization_burns_only_accepted_ids() {
    for materialize in [false, true] {
        for (name, schema, drops, creates, cancel) in [
            ("dirty-create", ADD, false, 1, false),
            ("noop-create", NOOP, false, 1, false),
            ("noop-drop", NOOP, true, 0, false),
            ("create-drop", NOOP, false, 1, true),
            ("replacement", NOOP, true, 1, false),
            ("multiple", NOOP, false, 2, false),
        ] {
            let root = root(&format!("rollback-{name}-{materialize}"));
            let mut db = seed(&root);
            let base = snapshot(&db);
            let revision = db.catalog_generation;
            let source = db.bindings.resolve_single(TableId(2)).unwrap();
            let floor = db.next_storage_id().unwrap();
            let mut phase = adopt(&mut db, schema);
            if drops {
                let old = target(&db, &phase, "users_email_idx");
                phase.drop_index(&mut db, old).unwrap();
            }
            if creates > 0 {
                create(
                    &mut db,
                    &mut phase,
                    if drops {
                        "CREATE INDEX users_email_idx ON users(email)"
                    } else {
                        CREATE
                    },
                );
            }
            if creates > 1 {
                create(
                    &mut db,
                    &mut phase,
                    "CREATE INDEX users_id_idx ON users(id)",
                );
            }
            if cancel {
                let new = target(&db, &phase, "users_legacy_idx");
                phase.drop_index(&mut db, new).unwrap();
            }
            if materialize {
                phase.finalize(&mut db).unwrap();
            }
            phase.transaction.rollback().unwrap();
            assert!(db.schema_writer.get().is_none());
            assert_eq!(snapshot(&db), base);
            assert_eq!(db.catalog_generation, revision);
            assert_rows(&mut db, false);
            assert_eq!(db.indexes(TableId(2)).unwrap().len(), 1);
            db.close().unwrap();
            for _ in 0..3 {
                let mut reopened = Database::open_catalog(root.join("catalog")).unwrap();
                assert_rows(&mut reopened, false);
                assert_eq!(reopened.bindings.resolve_single(TableId(2)), Ok(source));
                assert_eq!(reopened.indexes(TableId(2)).unwrap()[0].id, IndexId(1));
                assert_eq!(
                    reopened
                        .mutation_journal
                        .as_ref()
                        .unwrap()
                        .borrow()
                        .effective_index(TableId(2), IndexId(2)),
                    Some(IndexId(2 + creates))
                );
                if !materialize || schema == NOOP {
                    assert_eq!(reopened.next_storage_id(), Some(floor));
                }
                reopened.close().unwrap();
            }
            // A staged loser may burn its allocated StorageId, but the stage
            // family itself must not survive rollback or any reopen.
            assert_no_stage(&root);
            std::fs::remove_dir_all(root).unwrap();
        }
    }
}

fn assert_no_stage(root: &Path) {
    for entry in std::fs::read_dir(root).unwrap() {
        let entry = entry.unwrap();
        assert!(
            !entry.file_name().to_string_lossy().contains(".stage"),
            "{:?}",
            entry.path()
        );
        if entry.file_type().unwrap().is_dir() {
            assert_no_stage(&entry.path());
        }
    }
}

#[test]
fn crash_child() {
    let Ok(root) = std::env::var("NETBADB_ROUND47_CRASH_ROOT") else {
        return;
    };
    let mut db = Database::open_catalog(Path::new(&root).join("catalog")).unwrap();
    let dirty = std::env::var_os("NETBADB_ROUND47_DIRTY").is_some();
    let mut phase = adopt(&mut db, if dirty { ADD } else { NOOP });
    create(&mut db, &mut phase, CREATE);
    create(
        &mut db,
        &mut phase,
        "CREATE INDEX users_id_idx ON users(id)",
    );
    phase.finalize(&mut db).unwrap();
    db.commit_transaction(&mut phase.transaction).unwrap();
    panic!("crash point not reached");
}

#[test]
fn predecision_losers_and_exact_one_or_two_participant_winners_reopen_three_times() {
    for dirty in [false, true] {
        let mut cases = vec![
            (
                "adopted-final-index-reservation-durable",
                false,
                false,
                false,
            ),
            ("composition-intent-durable", false, false, false),
            ("after-prepare-1", true, false, false),
            ("after-all-prepares", true, false, false),
            ("after-durable-decision", true, true, false),
            ("after-commit-1", true, true, false),
            ("after-all-commits", true, true, false),
        ];
        if dirty {
            cases.extend([
                ("source-backfill-intent-durable", false, false, false),
                ("source-backfill-stage-intent-durable", false, false, false),
                ("source-backfill-mid-copy", false, false, false),
                ("after-commit-1", true, true, true),
            ]);
        } else {
            cases.extend([
                ("adopted-index-delta-first-tree-built", false, false, false),
                ("composition-after-index-delta-1", false, false, false),
            ]);
        }
        for (point, cord, winner, reverse) in cases {
            let root = root(&format!("crash-{dirty}-{point}-{reverse}"));
            let db = seed(&root);
            let source = db.bindings.resolve_single(TableId(2)).unwrap();
            let floor = db.next_storage_id().unwrap();
            let base = snapshot(&db);
            db.close().unwrap();
            let mut command = std::process::Command::new(std::env::current_exe().unwrap());
            command.args(["--exact", "schema_composition::adopted_source_index_finalization_audit_tests::crash_child", "--nocapture"]).env("NETBADB_ROUND47_CRASH_ROOT", &root);
            if dirty {
                command.env("NETBADB_ROUND47_DIRTY", "1");
            }
            if reverse {
                command.env("NETBADB_REVERSE_PARTICIPANT_COMMIT", "1");
            }
            if cord {
                crate::coordinator_crash::configure_child(&mut command, point, &root, point);
            } else {
                command.env("NETBADB_BACKFILL_CRASH_POINT", point);
            }
            let output = command.output().unwrap();
            assert_eq!(
                output.status.code(),
                Some(if cord { 87 } else { 90 }),
                "{dirty} {point}: {} {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            for _ in 0..3 {
                let mut reopened = Database::open_catalog(root.join("catalog")).unwrap();
                assert_rows(&mut reopened, winner);
                assert_eq!(
                    reopened.bindings.resolve_single(TableId(2)),
                    Ok(if dirty && winner { floor } else { source })
                );
                assert_eq!(
                    snapshot(&reopened).epoch,
                    base.epoch + u64::from(dirty && winner)
                );
                assert_eq!(
                    reopened.indexes(TableId(2)).unwrap().len(),
                    if winner { 3 } else { 1 }
                );
                assert_eq!(
                    reopened
                        .mutation_journal
                        .as_ref()
                        .unwrap()
                        .borrow()
                        .effective_index(TableId(2), IndexId(2)),
                    Some(if point == "adopted-final-index-reservation-durable" {
                        IndexId(3)
                    } else {
                        IndexId(4)
                    })
                );
                if point == "adopted-final-index-reservation-durable" {
                    assert_eq!(reopened.next_storage_id(), Some(floor));
                }
                reopened.close().unwrap();
            }
            assert_no_stage(&root);
            std::fs::remove_dir_all(root).unwrap();
        }
    }
}

#[test]
fn candidate_c_new_column_is_absent_and_early_existing_index_breaks_digest() {
    let root = root("immediate-s1");
    let mut db = seed(&root);
    let mut phase = adopt(&mut db, ADD);
    let source = db.bindings.resolve_single(TableId(2)).unwrap();
    let marker = phase
        .logical_mut()
        .overlay
        .schema
        .table("users")
        .unwrap()
        .column("marker")
        .unwrap()
        .id;
    assert!(
        db.registry
            .get(source)
            .unwrap()
            .table()
            .column_by_id(marker)
            .is_none()
    );
    let bad = phase
        .transaction
        .with_write_storage(source, &mut db.registry, |storage, context| {
            storage.create_named_index_with_reserved_id_in(
                context,
                netbadb_types::IndexName::new("bad").unwrap(),
                marker,
                IndexId(2),
                IndexId(3),
            )
        });
    assert!(bad.is_err());
    db.revalidate_adopted_source_authority(&phase.transaction)
        .unwrap();
    // The pending physical index catalog itself participates in the digest.
    phase
        .transaction
        .with_write_storage(source, &mut db.registry, |storage, context| {
            storage.create_named_index_with_reserved_id_in(
                context,
                netbadb_types::IndexName::new("early").unwrap(),
                ColumnId(2),
                IndexId(2),
                IndexId(3),
            )
        })
        .unwrap();
    assert!(matches!(
        db.revalidate_adopted_source_authority(&phase.transaction),
        Err(DatabaseError::SchemaMutation(SchemaMutationError::Corrupt(
            "adopted source authority drift"
        )))
    ));
    phase.transaction.rollback().unwrap();
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn candidate_b_same_fingerprint_clone_cost_and_publication_rejection() {
    let root = root("force-s2");
    let mut db = seed(&root);
    let mut phase = adopt(&mut db, NOOP);
    create(&mut db, &mut phase, CREATE);
    let source = db.bindings.resolve_single(TableId(2)).unwrap();
    let target_id = db.next_storage_id().unwrap();
    let table = db.schema().table("users").unwrap().clone();
    let before = bytes(&root);
    // Physical feasibility/cost experiment only: no managed identity is burned
    // or published for this deliberately unsupported same-fingerprint copy.
    let mut clone = netbadb_storage::HeapStorage::create_with_storage_id(
        root.join("comparison.heap"),
        table.clone(),
        target_id,
    )
    .unwrap();
    let columns = table
        .columns
        .iter()
        .map(|column| column.id)
        .collect::<Vec<_>>();
    let view = phase
        .transaction
        .begin_read_view(&[source], &mut db.registry)
        .unwrap();
    let mut copied = 0;
    let flow = db
        .registry
        .get_mut(source)
        .unwrap()
        .visit_rows_with_view_control::<DatabaseError, _>(
            &columns,
            view.iter()
                .find_map(|(id, view)| (id == source).then_some(view))
                .unwrap(),
            |_, values| {
                clone.insert(&values)?;
                copied += 1;
                Ok(ControlFlow::Continue(()))
            },
        )
        .unwrap();
    assert!(flow.is_continue());
    assert_eq!(copied, 3);
    clone
        .create_named_index(
            netbadb_types::IndexName::new("users_email_idx").unwrap(),
            ColumnId(3),
        )
        .unwrap();
    clone
        .create_named_index(
            netbadb_types::IndexName::new("users_legacy_idx").unwrap(),
            ColumnId(2),
        )
        .unwrap();
    let peak = bytes(&root);
    clone.close().unwrap();
    println!(
        "ROUND47 FORCE_S2 source={source:?} target={target_id:?} before={before} peak={peak} scans=1 copied={copied} published=false V/G/E=unchanged"
    );
    drop(view);
    phase.transaction.rollback().unwrap();
    drop(phase);
    // Obtain a valid rewrite, then present a canonical no-op table as its
    // target. Existing strict durable publication rejects same F even with +1 V.
    let mut dirty = adopt(&mut db, "ALTER TABLE users RENAME COLUMN email TO contact");
    create(&mut db, &mut dirty, CREATE);
    dirty.finalize(&mut db).unwrap();
    let mut candidate = SchemaMutationJournal::decode(&journal(&db)).unwrap();
    let plan = &mut candidate
        .compositions
        .get_mut(&dirty.transaction.id())
        .unwrap()
        .index_intent
        .as_mut()
        .unwrap()
        .tables[0];
    let SchemaIndexTablePlan::RewriteHeap { replacement, .. } = plan else {
        panic!("rewrite")
    };
    replacement.target.committed.schema = replacement.base.committed.schema.clone();
    replacement.target.placements.tables[0].schema_fingerprint =
        replacement.base.placements.tables[0].schema_fingerprint;
    assert!(candidate.encode().is_err());
    dirty.transaction.rollback().unwrap();
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn production_native_index_entry_points_enter_terminal_phase() {
    for (name, schema, sql) in [
        ("create", ADD, "CREATE INDEX marker_idx ON users(marker)"),
        (
            "drop",
            "ALTER TABLE users RENAME COLUMN email TO contact",
            "DROP INDEX users_email_idx",
        ),
    ] {
        let root = root(name);
        let mut db = seed(&root);
        let mut phase = adopt(&mut db, schema);
        db.execute_in(&mut phase.transaction, sql).unwrap();
        assert!(matches!(
            phase.transaction.schema_composition,
            SchemaCompositionState::AdoptedSourceIndexFinalizing(_)
        ));
        db.commit_transaction(&mut phase.transaction).unwrap();
        db.close().unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}

fn plan_uses_index(node: &netbadb_inspect::PlanNodeInspection) -> bool {
    use netbadb_inspect::PlanNodeInspection as N;
    match node {
        N::IndexScan { .. } | N::RangeIndexScan { .. } => true,
        N::Project { input, .. }
        | N::ScalarProject { input, .. }
        | N::Filter { input, .. }
        | N::Sort { input, .. }
        | N::Limit { input, .. }
        | N::Aggregate { input, .. }
        | N::IndexNestedLoopJoin { left: input, .. } => plan_uses_index(input),
        N::HashJoin { left, right, .. } | N::NestedLoopJoin { left, right, .. } => {
            plan_uses_index(left) || plan_uses_index(right)
        }
        N::OneRow | N::SeqScan { .. } | N::ColumnarScan { .. } | N::PartitionedScan { .. } => false,
    }
}

#[test]
fn multiple_creates_on_effective_table_still_allocate_one_target() {
    let root = root("multiple-effective");
    let mut db = seed(&root);
    let floor = db.next_storage_id().unwrap();
    let mut phase = adopt(&mut db, ADD);
    create(&mut db, &mut phase, CREATE);
    create(
        &mut db,
        &mut phase,
        "CREATE INDEX marker_idx ON users(marker)",
    );
    assert_eq!(db.next_storage_id(), Some(floor));
    let reservations = &db.mutation_journal.as_ref().unwrap().borrow().compositions
        [&phase.transaction.id()]
        .index_reservations
        .clone();
    assert_eq!(
        reservations.iter().map(|r| r.index).collect::<Vec<_>>(),
        [IndexId(2), IndexId(3)]
    );
    assert_noncanonical_reservation_bytes_rejected(&journal(&db), phase.transaction.id());
    phase.finalize(&mut db).unwrap();
    assert_eq!(
        phase
            .transaction
            .schema_composition
            .source_backfill()
            .unwrap()
            .source_copy_passes,
        1
    );
    db.commit_transaction(&mut phase.transaction).unwrap();
    assert_eq!(db.next_storage_id(), Some(StorageId(floor.0 + 1)));
    assert_eq!(db.indexes(TableId(2)).unwrap().len(), 3);
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

// The encoder sorts reservations; corrupt the wire order with valid envelopes
// instead of mistaking encoder canonicalization for decoder acceptance.
fn assert_noncanonical_reservation_bytes_rejected(encoded: &[u8], txn: DatabaseTxnId) {
    use crate::schema_catalog::{Reader, Writer, envelope, open_envelope};
    let mut reader = Reader(open_envelope(encoded, b"NBSJ").unwrap());
    let mut writer = Writer(reader.take(16).unwrap().to_vec());
    writer.string(&reader.string().unwrap()).unwrap();
    let count = reader.u32().unwrap();
    writer.u32(count);
    let mut frames = Vec::new();
    let mut selected = Vec::new();
    for _ in 0..count {
        let len = reader.u32().unwrap();
        let frame = reader.take(len as usize).unwrap().to_vec();
        let mut payload = Reader(open_envelope(&frame, b"NBSR").unwrap());
        if payload.u8().unwrap() == 24 && payload.u64().unwrap() == txn.0 {
            selected.push(frames.len());
        }
        frames.push(frame);
    }
    assert_eq!(selected.len(), 2);
    frames.swap(selected[0], selected[1]);
    for frame in frames {
        writer.u32(u32::try_from(frame.len()).unwrap());
        writer.0.extend_from_slice(&frame);
    }
    let corrupt = envelope(b"NBSJ", &writer.0).unwrap();
    assert!(matches!(
        SchemaMutationJournal::decode(&corrupt),
        Err(SchemaMutationError::Corrupt(
            "invalid composition IndexId reservation"
        ))
    ));
}

#[test]
fn finalization_revalidates_every_captured_source_authority_before_index_mutation() {
    let root = root("authority");
    let mut db = seed(&root);
    let mut phase = adopt(&mut db, NOOP);
    create(&mut db, &mut phase, CREATE);
    let SchemaCompositionState::AdoptedSourceIndexFinalizing(original) =
        &phase.transaction.schema_composition
    else {
        panic!("adopted")
    };
    let original = original.clone();
    let source = original.source_storage;
    let before = journal(&db);
    let digest = source_digest(&mut db, source);
    for mutation in 0..8 {
        let mut candidate = original.clone();
        match mutation {
            0 => candidate.source_storage = StorageId(999),
            1 => candidate.source_physical_txn_id.0 += 1,
            2 => candidate.source_table_version.0 += 1,
            3 => {
                candidate.source_fingerprint =
                    netbadb_schema::SchemaFingerprint::from_bytes([0x47; 32])
            }
            4 => candidate.source_locator.push_str("-wrong"),
            5 => candidate.base_generation.0 += 1,
            6 => candidate.base_epoch += 1,
            _ => candidate.source_index_digest[0] ^= 1,
        }
        phase.transaction.schema_composition =
            SchemaCompositionState::AdoptedSourceIndexFinalizing(candidate);
        assert!(phase.finalize(&mut db).is_err());
        assert_eq!(journal(&db), before);
        assert_eq!(source_digest(&mut db, source), digest);
    }
    phase.transaction.schema_composition =
        SchemaCompositionState::AdoptedSourceIndexFinalizing(original);
    phase.finalize(&mut db).unwrap();
    assert_ne!(source_digest(&mut db, source), digest);
    db.commit_transaction(&mut phase.transaction).unwrap();
    assert!(db.schema_writer.get().is_none());
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn final_index_phase_continues_create_drop_stream_but_never_crosses_tables() {
    let root = root("index-stream");
    let mut db = seed(&root);
    let revision = db.catalog_generation;
    let mut phase = adopt(&mut db, NOOP);
    for expected in [IndexId(2), IndexId(3)] {
        create(&mut db, &mut phase, CREATE);
        let new = target(&db, &phase, "users_legacy_idx");
        assert_eq!(new.index_id, expected);
        phase.drop_index(&mut db, new).unwrap();
    }
    let before = journal(&db);
    let cross = phase.prepare(&db, "CREATE INDEX seed_idx ON seed(id)");
    assert!(matches!(
        phase.create(&mut db, &cross),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::UnsupportedBackfillRefinement(
                crate::schema_mutation::BackfillRefinementReason::CrossTableAccess
            )
        ))
    ));
    assert!(
        phase
            .drop_index(
                &mut db,
                DropIndexTarget {
                    table_id: TableId(1),
                    index_id: IndexId(1)
                }
            )
            .is_err()
    );
    assert_eq!(journal(&db), before);
    phase.finalize(&mut db).unwrap();
    assert!(matches!(
        phase.transaction.schema_composition,
        SchemaCompositionState::SealedNoEffectiveChange(_)
    ));
    db.commit_transaction(&mut phase.transaction).unwrap();
    assert_eq!(db.catalog_generation, revision);
    assert_eq!(
        db.mutation_journal
            .as_ref()
            .unwrap()
            .borrow()
            .effective_index(TableId(2), IndexId(2)),
        Some(IndexId(4))
    );
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

fn assert_physical_keys(
    db: &mut Database,
    storage_id: StorageId,
    column: ColumnId,
    key: &ScalarValue,
    expected: &[i64],
) {
    let storage = db.registry.get_mut(storage_id).unwrap();
    let path = storage
        .access_paths()
        .into_iter()
        .find(|path| path.column_id == column)
        .unwrap()
        .id;
    let view = storage.read_view().unwrap();
    let mut ids = storage
        .point_lookup_columns_with_view(path, key, &[ColumnId(1)], &view)
        .unwrap()
        .into_iter()
        .map(|(_, values)| {
            let ScalarValue::Int64(id) = values[0] else {
                panic!("id")
            };
            id
        })
        .collect::<Vec<_>>();
    ids.sort_unstable();
    assert_eq!(ids, expected);
}

#[test]
fn first_final_index_failures_preserve_overlay_writer_and_allocator_authority() {
    for failure in [
        "stale",
        "stale-if",
        "duplicate",
        "column",
        "indexed-column",
        "identity-limit",
        "cross",
        "actions",
        "reservations",
        "undefined-drop",
    ] {
        let root = root(failure);
        let mut db = seed(&root);
        let mut phase = adopt(&mut db, NOOP);
        let mut statement = phase.prepare(&db, CREATE);
        match failure {
            "stale" | "stale-if" => {
                statement.target.table_version.0 += 1;
                statement.if_not_exists = failure == "stale-if";
                if statement.if_not_exists {
                    statement.name = IndexName::new("users_email_idx").unwrap();
                    statement.column_id = ColumnId(3);
                }
            }
            "duplicate" => statement.name = IndexName::new("users_email_idx").unwrap(),
            "column" => statement.column_id = ColumnId(999),
            "indexed-column" => statement.column_id = ColumnId(3),
            "identity-limit" => {
                phase
                    .logical_mut()
                    .touched
                    .get_mut(&TableId(2))
                    .unwrap()
                    .indexes
                    .next_index_id = IndexId(u64::MAX)
            }
            "cross" => statement = phase.prepare(&db, "CREATE INDEX seed_idx ON seed(id)"),
            "actions" => phase
                .logical_mut()
                .action_evidence
                .resize(MAX_SCHEMA_ACTIONS, [0; 32]),
            "reservations" => phase.logical_mut().index_reservation_count = MAX_INDEX_RESERVATIONS,
            _ => {}
        }
        let before = journal(&db);
        let dependency = phase.logical_mut().dependency(TableId(2)).unwrap();
        let writer = db.schema_writer.get();
        let result = if failure == "undefined-drop" {
            phase.drop_index(
                &mut db,
                DropIndexTarget {
                    table_id: TableId(2),
                    index_id: IndexId(999),
                },
            )
        } else {
            phase.create(&mut db, &statement)
        };
        assert!(result.is_err(), "{failure}");
        assert_eq!(journal(&db), before);
        assert_eq!(
            phase.logical_mut().dependency(TableId(2)).unwrap(),
            dependency
        );
        assert_eq!(db.schema_writer.get(), writer);
        assert!(matches!(
            phase.transaction.schema_composition,
            SchemaCompositionState::AdoptedSourceRefining(_)
        ));
        if failure == "identity-limit" {
            phase
                .logical_mut()
                .touched
                .get_mut(&TableId(2))
                .unwrap()
                .indexes
                .next_index_id = IndexId(2);
        }
        if failure == "actions" {
            phase.logical_mut().action_evidence.truncate(2);
        }
        if failure == "reservations" {
            phase.logical_mut().index_reservation_count = 0;
        }
        phase
            .execute(&mut db, "ALTER TABLE users RENAME COLUMN email TO contact")
            .unwrap();
        phase.transaction.rollback().unwrap();
        db.close().unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn first_unchanged_create_seals_without_burn_and_all_terminal_access_is_closed() {
    let root = root("unchanged-first");
    let mut db = seed(&root);
    let mut phase = adopt(&mut db, NOOP);
    let before = journal(&db);
    let statement = phase.prepare(
        &db,
        "CREATE INDEX IF NOT EXISTS users_email_idx ON users(email)",
    );
    assert_eq!(
        phase.create(&mut db, &statement).unwrap(),
        DdlOutcome::Unchanged
    );
    assert_eq!(journal(&db), before);
    assert!(phase.transaction.schema_composition.is_started());
    assert!(phase.transaction.schema_composition.is_sealed());
    for sql in [
        ADD,
        "ALTER TABLE users DROP COLUMN legacy",
        "ALTER TABLE users RENAME TO people",
        "ALTER TABLE users RENAME COLUMN email TO contact",
        "ALTER TABLE users ALTER COLUMN email SET NOT NULL",
        "ALTER TABLE users ALTER COLUMN email DROP NOT NULL",
    ] {
        assert!(
            matches!(
                phase.execute(&mut db, sql),
                Err(DatabaseError::SchemaMutation(
                    SchemaMutationError::SchemaMutationAfterMaterialization
                ))
            ),
            "{sql}"
        );
    }
    for sql in [
        "SELECT id FROM users",
        "INSERT INTO users VALUES (5, 'five', 'five')",
        "UPDATE users SET legacy = legacy",
        "DELETE FROM users WHERE id = 1",
    ] {
        assert!(
            matches!(
                phase.execute(&mut db, sql),
                Err(DatabaseError::SchemaMutation(
                    SchemaMutationError::MigrationDataAccessAfterRefinement
                ))
            ),
            "{sql}"
        );
    }
    db.commit_transaction(&mut phase.transaction).unwrap();
    assert_rows(&mut db, true);
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn uncertain_final_index_reservation_requires_recovery_instead_of_refining_retry() {
    let root = root("uncertain-reservation");
    let mut db = seed(&root);
    let mut phase = adopt(&mut db, NOOP);
    let statement = phase.prepare(&db, CREATE);
    db.mutation_journal
        .as_ref()
        .unwrap()
        .borrow_mut()
        .inject_sync_failure();
    assert!(matches!(
        phase.create(&mut db, &statement),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::RecoveryRequired
        ))
    ));
    assert!(matches!(
        phase.transaction.schema_composition,
        SchemaCompositionState::RollbackRequiredLogical(_)
    ));
    assert!(
        phase
            .execute(&mut db, "ALTER TABLE users RENAME COLUMN email TO contact")
            .is_err()
    );
    assert!(db.commit_transaction(&mut phase.transaction).is_err());
    drop(phase);
    drop(db);
    for _ in 0..3 {
        let mut reopened = Database::open_catalog(root.join("catalog")).unwrap();
        assert_rows(&mut reopened, false);
        assert_eq!(reopened.indexes(TableId(2)).unwrap().len(), 1);
        assert_eq!(
            reopened
                .mutation_journal
                .as_ref()
                .unwrap()
                .borrow()
                .effective_index(TableId(2), IndexId(2)),
            Some(IndexId(3))
        );
        reopened.close().unwrap();
    }
    std::fs::remove_dir_all(root).unwrap();
}
